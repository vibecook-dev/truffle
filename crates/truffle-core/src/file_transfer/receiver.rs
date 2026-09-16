//! Receive handler — background task that handles incoming file transfer
//! requests from other peers.
//!
//! Instead of auto-accepting, offers are forwarded through an offer channel
//! so the application can decide whether to accept or reject each transfer.
//! Pull requests are served only when the canonicalized requested path lives
//! inside a configured pull root (see [`super::FileTransfer::add_pull_root`]);
//! with no roots configured all incoming PULL_REQUESTs are denied.

use std::sync::Arc;

use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{broadcast, mpsc, oneshot};
use tracing::{error, info, warn};

use crate::network::NetworkProvider;
use crate::node::Node;

use super::types::{
    FileOffer, FileTransferEvent, FtMessage, OfferDecision, OfferResponder, OverwritePolicy,
    TransferDirection, TransferError, TransferProgress,
};
use super::MAX_PENDING_OFFERS_PER_PEER;

/// Reduce a peer-supplied file name to a safe base name.
///
/// A remote peer fully controls `file_name`/`save_path` in an OFFER, so these
/// must never be allowed to escape the receiver's chosen directory. This strips
/// every directory component (both `/` and `\`) and rejects empty, `.`, `..`,
/// or NUL-bearing names. Returns `None` when nothing safe remains.
fn safe_base_name(name: &str) -> Option<String> {
    let last = name.rsplit(['/', '\\']).next().unwrap_or("").trim();
    if last.is_empty() || last == "." || last == ".." || last.contains('\0') {
        return None;
    }
    Some(last.to_string())
}

/// Resolve the final on-disk destination for a received file.
///
/// When `save_path` is a directory (exists as one, or ends with a separator)
/// we append a **sanitized** base name derived from the peer-supplied
/// `file_name`, which prevents path-traversal / absolute-path escapes such as
/// `file_name = "../../../.ssh/authorized_keys"`. When `save_path` is an
/// explicit file path it was chosen by the local application, which owns that
/// decision.
fn resolve_dest_path(save_path: &str, file_name: &str) -> Result<String, TransferError> {
    let p = std::path::Path::new(save_path);
    let treat_as_dir = p.is_dir() || save_path.ends_with('/') || save_path.ends_with('\\');
    if treat_as_dir {
        let safe = safe_base_name(file_name).ok_or_else(|| {
            TransferError::Protocol(format!(
                "Rejected unsafe file name from peer: {file_name:?}"
            ))
        })?;
        Ok(format!(
            "{}/{}",
            save_path.trim_end_matches(['/', '\\']),
            safe
        ))
    } else {
        Ok(save_path.to_string())
    }
}

/// Authorize a peer-supplied PULL_REQUEST path against the allowlist.
///
/// Deny-by-default: an empty allowlist rejects everything. The path is
/// canonicalized (resolving symlinks and `..`) and must be a regular file
/// inside one of the canonicalized `roots`, no larger than `max_size`.
///
/// [`std::path::Path::starts_with`] is component-wise, so a root of `/shared`
/// does not match a sibling `/shared-evil`. Missing, non-file, and
/// outside-root cases all return the same "not shared" message to avoid
/// leaking an existence oracle to the requester.
fn authorize_pull_path(
    roots: &[std::path::PathBuf],
    requested: &str,
    max_size: u64,
) -> Result<std::path::PathBuf, TransferError> {
    if roots.is_empty() {
        return Err(TransferError::Rejected(
            "pull serving is not enabled on this peer".into(),
        ));
    }
    let canon = std::fs::canonicalize(requested)
        .map_err(|_| TransferError::Rejected("requested path is not shared".into()))?;
    if !roots.iter().any(|r| canon.starts_with(r)) {
        return Err(TransferError::Rejected(
            "requested path is not shared".into(),
        ));
    }
    let meta = std::fs::metadata(&canon)
        .map_err(|_| TransferError::Rejected("requested path is not shared".into()))?;
    if !meta.is_file() {
        return Err(TransferError::Rejected(
            "requested path is not shared".into(),
        ));
    }
    if meta.len() > max_size {
        return Err(TransferError::Rejected(format!(
            "file size {} exceeds max transfer size {max_size}",
            meta.len()
        )));
    }
    Ok(canon)
}

/// Spawn a background task that listens for incoming file transfer messages.
///
/// - **OFFER**: Creates a [`FileOffer`] + [`OfferResponder`] pair, sends them
///   on the `offer_tx` channel, and waits up to 60 seconds for a decision.
/// - **PULL_REQUEST**: Serves the requested file only if its canonicalized path
///   is inside a configured pull root (see [`super::FileTransfer::add_pull_root`]);
///   denied by default.
/// - **ACCEPT / REJECT**: Ignored (handled by send/pull initiators).
pub fn spawn_receive_handler<N: NetworkProvider + 'static>(
    node: Arc<Node<N>>,
    offer_tx: mpsc::Sender<(FileOffer, OfferResponder)>,
    event_tx: broadcast::Sender<FileTransferEvent>,
) -> tokio::task::JoinHandle<()> {
    let cancel = node.tasks.cancel.clone();
    let tracker = node.tasks.tracker.clone();
    tracker.spawn(async move {
        let mut rx = node.subscribe("ft");
        info!("File transfer receive handler started");

        loop {
            let recv = tokio::select! {
                _ = cancel.cancelled() => {
                    info!("FT receive handler: node stopping, exiting");
                    break;
                }
                result = rx.recv() => result,
            };
            let msg = match recv {
                Ok(m) => m,
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    warn!("FT receive handler lagged, missed {n} messages");
                    continue;
                }
                Err(broadcast::error::RecvError::Closed) => {
                    info!("FT receive handler: channel closed, exiting");
                    break;
                }
            };

            let ft_msg: FtMessage = match serde_json::from_value(msg.payload.clone()) {
                Ok(m) => m,
                Err(e) => {
                    warn!(from = msg.from.as_str(), "Bad FT message: {e}");
                    continue;
                }
            };

            let node = node.clone();
            let from = msg.from.clone();
            let offer_tx = offer_tx.clone();
            let event_tx = event_tx.clone();

            match ft_msg {
                FtMessage::Offer {
                    file_name,
                    size,
                    sha256,
                    save_path,
                    token,
                    tcp_port: _,
                } => {
                    // Global cap: refuse new work instead of queueing it, so
                    // an offer burst cannot pile up unbounded parked tasks.
                    let permit = match node
                        .file_transfer_state
                        .incoming_ops
                        .clone()
                        .try_acquire_owned()
                    {
                        Ok(p) => p,
                        Err(_) => {
                            warn!(
                                from = from.as_str(),
                                "Rejecting offer: incoming-operation limit reached"
                            );
                            spawn_reject(node, from, token, "receiver busy");
                            continue;
                        }
                    };
                    // Per-peer cap on offers parked awaiting a decision
                    // (each waits up to 60s for the application).
                    let pending_guard = match PeerPendingGuard::try_acquire(
                        &node.file_transfer_state.pending_offers_per_peer,
                        &from,
                        MAX_PENDING_OFFERS_PER_PEER,
                    ) {
                        Some(g) => g,
                        None => {
                            warn!(
                                from = from.as_str(),
                                "Rejecting offer: per-peer pending-offer limit reached"
                            );
                            spawn_reject(node, from, token, "receiver busy");
                            continue;
                        }
                    };
                    // Handle incoming OFFER (someone wants to push a file to us).
                    // Tracked + cancellable: stop() drains these instead of
                    // leaving up-to-60s parked tasks behind.
                    let cancel = node.tasks.cancel.clone();
                    let tracker = node.tasks.tracker.clone();
                    tracker.spawn(async move {
                        let _permit = permit;
                        let _pending = pending_guard;
                        tokio::select! {
                        _ = cancel.cancelled() => {}
                        _ = async {
                        if let Err(e) = handle_incoming_offer(
                            &node, &from, &file_name, size, &sha256, &save_path, &token, &offer_tx,
                            &event_tx,
                        )
                        .await
                        {
                            // Distinguish rejection from actual failures
                            match &e {
                                TransferError::Rejected(reason) => {
                                    info!(
                                        from = from.as_str(),
                                        file = file_name.as_str(),
                                        "File offer rejected: {reason}"
                                    );
                                    let _ = event_tx.send(FileTransferEvent::Rejected {
                                        token,
                                        file_name,
                                        reason: reason.clone(),
                                    });
                                }
                                _ => {
                                    error!(
                                        from = from.as_str(),
                                        file = file_name.as_str(),
                                        "Failed to receive file: {e}"
                                    );
                                    let _ = event_tx.send(FileTransferEvent::Failed {
                                        token,
                                        direction: TransferDirection::Receive,
                                        file_name,
                                        reason: e.to_string(),
                                    });
                                }
                            }
                        }
                        } => {}
                        }
                    });
                }
                FtMessage::PullRequest {
                    path,
                    requester_id: _,
                    token,
                } => {
                    // Global cap applies to pull serving too — each served
                    // pull streams a file and holds a TCP connection.
                    let permit = match node
                        .file_transfer_state
                        .incoming_ops
                        .clone()
                        .try_acquire_owned()
                    {
                        Ok(p) => p,
                        Err(_) => {
                            warn!(
                                from = from.as_str(),
                                "Rejecting pull: incoming-operation limit reached"
                            );
                            spawn_reject(node, from, token, "receiver busy");
                            continue;
                        }
                    };
                    // Handle incoming PULL_REQUEST (someone wants to download
                    // from us). Tracked + cancellable like offers.
                    let cancel = node.tasks.cancel.clone();
                    let tracker = node.tasks.tracker.clone();
                    tracker.spawn(async move {
                        let _permit = permit;
                        tokio::select! {
                        _ = cancel.cancelled() => {}
                        _ = async {
                        if let Err(e) =
                            handle_pull_request(&node, &from, &path, &token, &event_tx).await
                        {
                            error!(
                                from = from.as_str(),
                                path = path.as_str(),
                                "Failed to serve file: {e}"
                            );
                        }
                        } => {}
                        }
                    });
                }
                _ => {
                    // ACCEPT / REJECT are handled by the upload/download initiators
                }
            }
        }
    })
}

/// Handle an incoming OFFER: forward to offer channel, wait for decision,
/// then accept/reject accordingly.
#[allow(clippy::too_many_arguments)]
async fn handle_incoming_offer<N: NetworkProvider + 'static>(
    node: &Node<N>,
    from: &str,
    file_name: &str,
    size: u64,
    sha256: &str,
    save_path: &str,
    token: &str,
    offer_tx: &mpsc::Sender<(FileOffer, OfferResponder)>,
    event_tx: &broadcast::Sender<FileTransferEvent>,
) -> Result<(), TransferError> {
    info!(
        from = from,
        file = file_name,
        size = size,
        "Received incoming file offer"
    );

    // M1: reject over-size offers before doing any work. The peer controls
    // `size`, so an unbounded value would otherwise let an auto-accepting node
    // be driven to disk exhaustion.
    let max_size = node
        .file_transfer_state
        .max_transfer_size
        .load(std::sync::atomic::Ordering::Relaxed);
    if size > max_size {
        return Err(TransferError::Protocol(format!(
            "Offered file size {size} exceeds max transfer size {max_size}"
        )));
    }

    // Build the FileOffer. `suggested_path` is peer-controlled, so we surface it
    // only as a sanitized base-name hint — never a path a naive integrator could
    // pass straight to `accept()` and have it escape their download directory.
    let offer = FileOffer {
        from_peer: from.to_string(),
        from_name: from.to_string(), // Best we have — the peer ID
        file_name: file_name.to_string(),
        size,
        sha256: sha256.to_string(),
        suggested_path: safe_base_name(save_path).unwrap_or_default(),
        token: token.to_string(),
    };

    // Emit OfferReceived event (informational)
    let _ = event_tx.send(FileTransferEvent::OfferReceived(offer.clone()));

    // Create oneshot channel for the decision
    let (decision_tx, decision_rx) = oneshot::channel::<OfferDecision>();
    let responder = OfferResponder::new(decision_tx);

    // Send offer + responder to the offer channel. The channel is bounded:
    // when the application stops draining it, further offers are rejected
    // fail-fast instead of queueing without limit.
    if let Err(e) = offer_tx.try_send((offer, responder)) {
        let reason = match e {
            mpsc::error::TrySendError::Full(_) => "receiver busy: offer queue full",
            mpsc::error::TrySendError::Closed(_) => "receiver has no offer channel",
        };
        send_reject(node, from, token, reason).await;
        return Err(TransferError::Rejected(reason.to_string()));
    }

    // Wait for decision with 60s timeout
    let decision = tokio::time::timeout(tokio::time::Duration::from_secs(60), decision_rx)
        .await
        .map_err(|_| TransferError::Timeout)?
        .map_err(|_| {
            TransferError::Protocol("Offer responder dropped without decision".to_string())
        })?;

    match decision {
        OfferDecision::Accept { save_path: dest } => {
            accept_and_receive(node, from, file_name, size, sha256, token, &dest, event_tx).await
        }
        OfferDecision::Reject { reason } => {
            // Send REJECT message to sender
            let reject = FtMessage::Reject {
                token: token.to_string(),
                reason: reason.clone(),
            };
            let reject_payload = serde_json::to_value(&reject)
                .map_err(|e| TransferError::Protocol(format!("Serialize error: {e}")))?;
            node.send_typed(from, "ft", "reject", &reject_payload)
                .await
                .map_err(|e| TransferError::Node(format!("Failed to send REJECT: {e}")))?;

            info!(
                from = from,
                file = file_name,
                reason = reason.as_str(),
                "Rejected file offer"
            );

            Err(TransferError::Rejected(reason))
        }
    }
}

/// Accept an incoming offer: open TCP listener, receive file streaming to
/// disk, verify SHA-256, and send ACK.
#[allow(clippy::too_many_arguments)]
async fn accept_and_receive<N: NetworkProvider + 'static>(
    node: &Node<N>,
    from: &str,
    file_name: &str,
    size: u64,
    sha256: &str,
    token: &str,
    save_path: &str,
    event_tx: &broadcast::Sender<FileTransferEvent>,
) -> Result<(), TransferError> {
    let start = std::time::Instant::now();

    // F6: resolve the final destination SAFELY up front. When `save_path` is a
    // directory we append a sanitized base name from the peer's `file_name`,
    // blocking path-traversal / absolute-path escapes.
    let final_path = resolve_dest_path(save_path, file_name)?;

    // Fail fast before receiving any bytes when the destination exists and
    // the policy forbids replacing it. Racy against concurrent transfers,
    // so `finalize_received_file` re-enforces the policy atomically.
    let policy = node.file_transfer_state.overwrite_policy();
    if policy == OverwritePolicy::Reject
        && tokio::fs::try_exists(&final_path).await.unwrap_or(false)
    {
        let reason = "destination already exists";
        send_reject(node, from, token, reason).await;
        return Err(TransferError::Rejected(format!("{reason}: {final_path}")));
    }

    // Create parent directories for the resolved destination.
    if let Some(parent) = std::path::Path::new(&final_path).parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    // Start TCP listener
    let mut listener = node
        .listen_tcp(0)
        .await
        .map_err(|e| TransferError::Node(format!("Failed to listen TCP: {e}")))?;

    // Send ACCEPT
    let accept = FtMessage::Accept {
        token: token.to_string(),
        tcp_port: listener.port,
    };
    let accept_payload = serde_json::to_value(&accept)
        .map_err(|e| TransferError::Protocol(format!("Serialize error: {e}")))?;
    node.send_typed(from, "ft", "accept", &accept_payload)
        .await
        .map_err(|e| TransferError::Node(format!("Failed to send ACCEPT: {e}")))?;

    info!(
        port = listener.port,
        "Sent ACCEPT, listening for TCP connection"
    );

    // Wait for TCP connection with 30s timeout
    let incoming = tokio::time::timeout(tokio::time::Duration::from_secs(30), listener.accept())
        .await
        .map_err(|_| TransferError::Timeout)?
        .ok_or_else(|| TransferError::Protocol("Listener closed before accepting".to_string()))?;

    let mut stream = incoming.stream;

    // Read header: [8-byte size][64-byte sha256_hex]
    let mut size_buf = [0u8; 8];
    stream.read_exact(&mut size_buf).await?;
    let file_size = u64::from_be_bytes(size_buf);

    let mut sha_buf = [0u8; 64];
    stream.read_exact(&mut sha_buf).await?;
    let received_sha = String::from_utf8_lossy(&sha_buf).to_string();

    // Verify the metadata matches the OFFER
    if received_sha != sha256 {
        return Err(TransferError::IntegrityError {
            expected: sha256.to_string(),
            actual: received_sha,
        });
    }

    if file_size != size {
        return Err(TransferError::Protocol(format!(
            "Size mismatch: offer said {size}, stream header says {file_size}"
        )));
    }

    // M1: defense-in-depth — enforce the max size against the stream header too.
    let max_size = node
        .file_transfer_state
        .max_transfer_size
        .load(std::sync::atomic::Ordering::Relaxed);
    if file_size > max_size {
        return Err(TransferError::Protocol(format!(
            "File size {file_size} exceeds max transfer size {max_size}"
        )));
    }

    // Stream file data to disk (instead of memory buffer). The temp name is
    // uniqued locally (never from the peer-controlled token) + create_new,
    // so concurrent transfers to the same destination each get their own
    // temp file instead of truncating and interleaving with each other.
    let temp_path = format!("{final_path}.{}.truffle-tmp", uuid::Uuid::new_v4());
    let mut temp_file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp_path)
        .await?;
    let mut hasher = Sha256::new();
    let mut bytes_received: u64 = 0;
    let progress_start = std::time::Instant::now();
    let mut last_progress = std::time::Instant::now();
    let mut buf = vec![0u8; 64 * 1024];

    while bytes_received < file_size {
        let to_read = ((file_size - bytes_received) as usize).min(buf.len());
        let n = stream.read(&mut buf[..to_read]).await?;
        if n == 0 {
            tokio::fs::remove_file(&temp_path).await.ok();
            return Err(TransferError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!("Connection closed after {bytes_received}/{file_size} bytes"),
            )));
        }
        hasher.update(&buf[..n]);
        tokio::io::AsyncWriteExt::write_all(&mut temp_file, &buf[..n]).await?;
        bytes_received += n as u64;

        // Throttle progress events to max 4/sec
        if last_progress.elapsed() >= std::time::Duration::from_millis(250) {
            let elapsed = progress_start.elapsed().as_secs_f64();
            let speed = if elapsed > 0.0 {
                bytes_received as f64 / elapsed
            } else {
                0.0
            };
            let _ = event_tx.send(FileTransferEvent::Progress(TransferProgress {
                token: token.to_string(),
                direction: TransferDirection::Receive,
                file_name: file_name.to_string(),
                bytes_transferred: bytes_received,
                total_bytes: file_size,
                speed_bps: speed,
            }));
            last_progress = std::time::Instant::now();
        }
    }

    // Flush temp file
    tokio::io::AsyncWriteExt::flush(&mut temp_file).await?;

    // Verify SHA-256
    let actual_sha = hex::encode(hasher.finalize());

    if actual_sha != sha256 {
        // Send NACK
        stream.write_all(&[0x00]).await?;
        // Clean up temp file
        tokio::fs::remove_file(&temp_path).await.ok();
        return Err(TransferError::IntegrityError {
            expected: sha256.to_string(),
            actual: actual_sha,
        });
    }

    // (final_path was resolved and its parent created before streaming.)

    // Publish the temp file to the final path per the overwrite policy.
    info!(
        temp = temp_path.as_str(),
        final_path = final_path.as_str(),
        "Moving temp file to final destination"
    );
    let placed_path = finalize_received_file(&temp_path, &final_path, policy).await?;
    info!(final_path = placed_path.as_str(), "File save completed");

    // Send ACK and flush
    stream.write_all(&[0x01]).await?;
    tokio::io::AsyncWriteExt::flush(&mut stream).await?;

    // Wait briefly for the ACK to propagate through the Go bridge.
    // The bridge uses bidirectional io.Copy — when we drop the stream,
    // the bridge may close both directions before the ACK byte has been
    // forwarded to the sender. This small delay ensures the ACK is delivered.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let elapsed = start.elapsed().as_secs_f64();
    info!(
        file = placed_path.as_str(),
        bytes = file_size,
        elapsed_ms = (elapsed * 1000.0) as u64,
        "File received and verified"
    );

    // Emit completed event
    let _ = event_tx.send(FileTransferEvent::Completed {
        token: token.to_string(),
        direction: TransferDirection::Receive,
        file_name: file_name.to_string(),
        bytes_transferred: file_size,
        sha256: actual_sha,
        elapsed_secs: elapsed,
    });

    Ok(())
}

/// Handle a PULL_REQUEST: read file, send OFFER, wait for ACCEPT, stream via TCP.
async fn handle_pull_request<N: NetworkProvider + 'static>(
    node: &Node<N>,
    from: &str,
    path: &str,
    token: &str,
    event_tx: &broadcast::Sender<FileTransferEvent>,
) -> Result<(), TransferError> {
    info!(from = from, path = path, "Processing PULL_REQUEST");

    // Authorize the peer-supplied path against the deny-by-default pull-root
    // allowlist BEFORE any filesystem I/O. This blocks arbitrary absolute-path
    // reads (e.g. `/etc/hosts`) and enforces the max transfer size + regular-file
    // check on the serving side.
    let max_size = node
        .file_transfer_state
        .max_transfer_size
        .load(std::sync::atomic::Ordering::Relaxed);
    let roots = node.file_transfer_state.pull_roots.read().unwrap().clone();
    let serve_path = match authorize_pull_path(&roots, path, max_size) {
        Ok(p) => p,
        Err(e) => {
            warn!(from = from, path = path, "Denying PULL_REQUEST: {e}");
            // Best-effort Reject so the requester fails fast instead of timing out.
            let reject = FtMessage::Reject {
                token: token.to_string(),
                reason: "pull denied by peer".to_string(),
            };
            if let Ok(payload) = serde_json::to_value(&reject) {
                if let Err(send_err) = node.send_typed(from, "ft", "reject", &payload).await {
                    warn!(from = from, "Failed to send pull REJECT: {send_err}");
                }
            }
            return Err(e);
        }
    };

    // Stream-hash the file in 64KB chunks (constant memory, mirrors
    // sender::send_file). Reading the whole file into RAM let one permitted
    // pull allocate up to max_transfer_size bytes.
    let meta = tokio::fs::metadata(&serve_path)
        .await
        .map_err(TransferError::Io)?;
    let size = meta.len();
    let sha256 = {
        let mut hasher = Sha256::new();
        let mut file = tokio::fs::File::open(&serve_path)
            .await
            .map_err(TransferError::Io)?;
        let mut buf = vec![0u8; 64 * 1024];
        loop {
            let n = file.read(&mut buf).await.map_err(TransferError::Io)?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
        }
        hex::encode(hasher.finalize())
    };

    let file_name = std::path::Path::new(path)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("file")
        .to_string();

    let offer_token = uuid::Uuid::new_v4().to_string();

    // Send OFFER and wait for ACCEPT
    let offer = FtMessage::Offer {
        file_name: file_name.clone(),
        size,
        sha256: sha256.clone(),
        save_path: String::new(),
        token: offer_token.clone(),
        tcp_port: 0,
    };
    let offer_payload = serde_json::to_value(&offer)
        .map_err(|e| TransferError::Protocol(format!("Serialize error: {e}")))?;

    let _accept_port = crate::request_reply::send_and_wait(
        node,
        from,
        "ft",
        "offer",
        &offer_payload,
        std::time::Duration::from_secs(30),
        |msg| {
            if msg.from != from {
                return None;
            }
            let ft_msg: FtMessage = serde_json::from_value(msg.payload.clone()).ok()?;
            match ft_msg {
                FtMessage::Accept {
                    token: ref t,
                    tcp_port,
                } if *t == offer_token => Some(Ok(tcp_port)),
                FtMessage::Reject {
                    token: ref t,
                    reason,
                } if *t == offer_token => Some(Err(TransferError::Rejected(format!(
                    "Peer rejected: {reason}"
                )))),
                _ => None,
            }
        },
    )
    .await
    .map_err(|e| match e {
        crate::request_reply::RequestError::Timeout => TransferError::Timeout,
        crate::request_reply::RequestError::Send(e) => {
            TransferError::Node(format!("Failed to send OFFER: {e}"))
        }
        crate::request_reply::RequestError::ChannelClosed => {
            TransferError::Protocol("Channel closed".into())
        }
    })??;

    // Open TCP to peer and stream the file
    let mut stream = node.open_tcp(from, _accept_port).await.map_err(|e| {
        TransferError::Node(format!("Failed to open TCP to {from}:{_accept_port}: {e}"))
    })?;

    let start = std::time::Instant::now();

    // Write [size][sha256_hex] then stream the file in 64KB chunks
    // (constant memory). If the file changes between the hash pass and
    // this read, the requester's integrity check fails the transfer.
    stream.write_all(&size.to_be_bytes()).await?;
    stream.write_all(sha256.as_bytes()).await?;

    let mut file = tokio::fs::File::open(&serve_path)
        .await
        .map_err(TransferError::Io)?;
    let mut buf = vec![0u8; 64 * 1024];
    let mut bytes_sent: u64 = 0;
    let mut last_progress = std::time::Instant::now();

    loop {
        let n = file.read(&mut buf).await.map_err(TransferError::Io)?;
        if n == 0 {
            break;
        }
        stream.write_all(&buf[..n]).await?;
        bytes_sent += n as u64;

        // Throttle progress events to max 4/sec
        if last_progress.elapsed() >= std::time::Duration::from_millis(250) {
            let elapsed = start.elapsed().as_secs_f64();
            let speed = if elapsed > 0.0 {
                bytes_sent as f64 / elapsed
            } else {
                0.0
            };
            let _ = event_tx.send(FileTransferEvent::Progress(TransferProgress {
                token: offer_token.clone(),
                direction: TransferDirection::Send,
                file_name: file_name.clone(),
                bytes_transferred: bytes_sent,
                total_bytes: size,
                speed_bps: speed,
            }));
            last_progress = std::time::Instant::now();
        }
    }

    stream.flush().await?;

    // Read ACK
    let mut ack = [0u8; 1];
    stream.read_exact(&mut ack).await?;

    if ack[0] != 0x01 {
        return Err(TransferError::IntegrityError {
            expected: sha256,
            actual: "peer reported integrity failure".to_string(),
        });
    }

    let elapsed = start.elapsed().as_secs_f64();
    info!(path = path, bytes = size, "File served successfully");

    // Emit completed event
    let _ = event_tx.send(FileTransferEvent::Completed {
        token: offer_token,
        direction: TransferDirection::Send,
        file_name,
        bytes_transferred: size,
        sha256,
        elapsed_secs: elapsed,
    });

    Ok(())
}

// ---------------------------------------------------------------------------
// Limits & finalize helpers
// ---------------------------------------------------------------------------

/// Send a best-effort REJECT for `token` so the sender fails fast instead of
/// waiting out its accept timeout.
async fn send_reject<N: NetworkProvider + 'static>(
    node: &Node<N>,
    from: &str,
    token: &str,
    reason: &str,
) {
    let reject = FtMessage::Reject {
        token: token.to_string(),
        reason: reason.to_string(),
    };
    if let Ok(payload) = serde_json::to_value(&reject) {
        if let Err(e) = node.send_typed(from, "ft", "reject", &payload).await {
            warn!(from = from, "Failed to send REJECT: {e}");
        }
    }
}

/// Fire-and-forget [`send_reject`] from the dispatch loop (which must not
/// block on network sends).
fn spawn_reject<N: NetworkProvider + 'static>(
    node: Arc<Node<N>>,
    from: String,
    token: String,
    reason: &'static str,
) {
    let tracker = node.tasks.tracker.clone();
    tracker.spawn(async move {
        send_reject(&node, &from, &token, reason).await;
    });
}

/// RAII count of offers a peer has awaiting an accept/reject decision.
/// Dropping the guard (offer handled, rejected, or timed out) releases the
/// slot; the map entry is removed at zero so it cannot grow unbounded.
struct PeerPendingGuard {
    map: Arc<std::sync::Mutex<std::collections::HashMap<String, usize>>>,
    peer: String,
}

impl PeerPendingGuard {
    fn try_acquire(
        map: &Arc<std::sync::Mutex<std::collections::HashMap<String, usize>>>,
        peer: &str,
        max: usize,
    ) -> Option<Self> {
        let mut m = map.lock().unwrap();
        let count = m.entry(peer.to_string()).or_insert(0);
        if *count >= max {
            return None;
        }
        *count += 1;
        Some(Self {
            map: map.clone(),
            peer: peer.to_string(),
        })
    }
}

impl Drop for PeerPendingGuard {
    fn drop(&mut self) {
        let mut m = self.map.lock().unwrap();
        if let Some(c) = m.get_mut(&self.peer) {
            *c = c.saturating_sub(1);
            if *c == 0 {
                m.remove(&self.peer);
            }
        }
    }
}

/// Error shape for [`place_no_clobber`] so callers can tell "destination
/// exists" apart from real I/O failures.
enum PlaceError {
    Exists,
    Io(std::io::Error),
}

/// Publish `temp_path` at `dest` without replacing an existing file.
///
/// `hard_link` is an atomic no-clobber publish; the temp file is always a
/// sibling of `dest`, so same-filesystem is guaranteed. Filesystems without
/// hard-link support fall back to an exists-check + rename — a small race,
/// but unique temp names already prevent concurrent transfers from
/// corrupting each other's data.
async fn place_no_clobber(temp_path: &str, dest: &str) -> Result<(), PlaceError> {
    match tokio::fs::hard_link(temp_path, dest).await {
        Ok(()) => {
            tokio::fs::remove_file(temp_path).await.ok();
            Ok(())
        }
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Err(PlaceError::Exists),
        Err(_) => {
            if tokio::fs::try_exists(dest).await.unwrap_or(false) {
                return Err(PlaceError::Exists);
            }
            tokio::fs::rename(temp_path, dest)
                .await
                .map_err(PlaceError::Io)
        }
    }
}

/// Deduplicated sibling name for [`OverwritePolicy::Rename`]:
/// `report.pdf` → `report (1).pdf`, `Makefile` → `Makefile (1)`.
fn dedup_candidate(path: &str, i: u32) -> String {
    let p = std::path::Path::new(path);
    let stem = p.file_stem().and_then(|s| s.to_str()).unwrap_or("file");
    let new_name = match p.extension().and_then(|e| e.to_str()) {
        Some(ext) => format!("{stem} ({i}).{ext}"),
        None => format!("{stem} ({i})"),
    };
    p.with_file_name(new_name).to_string_lossy().into_owned()
}

/// Publish a fully received temp file to `final_path` per the overwrite
/// policy. Returns the path actually used (differs from `final_path` only
/// under [`OverwritePolicy::Rename`]). The temp file is consumed on success
/// and removed on failure.
pub(crate) async fn finalize_received_file(
    temp_path: &str,
    final_path: &str,
    policy: OverwritePolicy,
) -> Result<String, TransferError> {
    match policy {
        OverwritePolicy::Replace => {
            // Rename first (atomic replace on POSIX), fall back to
            // copy+delete — the pre-policy behavior.
            if let Err(rename_err) = tokio::fs::rename(temp_path, final_path).await {
                info!(err = %rename_err, "Rename failed, trying copy+delete fallback");
                if let Err(e) = tokio::fs::copy(temp_path, final_path).await {
                    tokio::fs::remove_file(temp_path).await.ok();
                    return Err(TransferError::Io(e));
                }
                tokio::fs::remove_file(temp_path).await.ok();
            }
            Ok(final_path.to_string())
        }
        OverwritePolicy::Reject => match place_no_clobber(temp_path, final_path).await {
            Ok(()) => Ok(final_path.to_string()),
            Err(PlaceError::Exists) => {
                tokio::fs::remove_file(temp_path).await.ok();
                Err(TransferError::Protocol(format!(
                    "destination already exists: {final_path} (OverwritePolicy::Reject)"
                )))
            }
            Err(PlaceError::Io(e)) => {
                tokio::fs::remove_file(temp_path).await.ok();
                Err(TransferError::Io(e))
            }
        },
        OverwritePolicy::Rename => {
            match place_no_clobber(temp_path, final_path).await {
                Ok(()) => return Ok(final_path.to_string()),
                Err(PlaceError::Exists) => {}
                Err(PlaceError::Io(e)) => {
                    tokio::fs::remove_file(temp_path).await.ok();
                    return Err(TransferError::Io(e));
                }
            }
            for i in 1..=999u32 {
                let candidate = dedup_candidate(final_path, i);
                match place_no_clobber(temp_path, &candidate).await {
                    Ok(()) => return Ok(candidate),
                    Err(PlaceError::Exists) => continue,
                    Err(PlaceError::Io(e)) => {
                        tokio::fs::remove_file(temp_path).await.ok();
                        return Err(TransferError::Io(e));
                    }
                }
            }
            tokio::fs::remove_file(temp_path).await.ok();
            Err(TransferError::Protocol(format!(
                "no free deduplicated name found for {final_path}"
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Extra imports for the behavioral mock-node test (`handle_pull_request_*`).
    use crate::envelope::codec::JsonCodec;
    use crate::envelope::EnvelopeCodec;
    use crate::network::*;
    use crate::session::PeerRegistry;
    use crate::transport::websocket::WebSocketTransport;
    use crate::transport::WsConfig;
    use std::time::Duration;

    #[test]
    fn safe_base_name_strips_directories() {
        assert_eq!(safe_base_name("file.txt").as_deref(), Some("file.txt"));
        assert_eq!(
            safe_base_name("../../etc/passwd").as_deref(),
            Some("passwd")
        );
        assert_eq!(safe_base_name("/abs/path/x").as_deref(), Some("x"));
        assert_eq!(
            safe_base_name("..\\..\\win.exe").as_deref(),
            Some("win.exe")
        );
    }

    #[test]
    fn safe_base_name_rejects_unsafe() {
        assert_eq!(safe_base_name(""), None);
        assert_eq!(safe_base_name("."), None);
        assert_eq!(safe_base_name(".."), None);
        assert_eq!(safe_base_name("../.."), None); // final component is ".."
        assert_eq!(safe_base_name("dir/"), None); // trailing separator => empty base
    }

    #[test]
    fn resolve_dest_contains_traversal_into_directory() {
        // A directory destination + a malicious peer file_name must stay inside
        // the directory (F6): the "../.." escape is stripped to a base name.
        let got = resolve_dest_path("/downloads/", "../../../.ssh/authorized_keys").unwrap();
        assert_eq!(got, "/downloads/authorized_keys");
        assert!(!got.contains(".."));
    }

    #[test]
    fn resolve_dest_rejects_dotdot_filename() {
        assert!(resolve_dest_path("/downloads/", "..").is_err());
    }

    #[test]
    fn resolve_dest_passes_through_explicit_file() {
        // A non-directory explicit path chosen by the local app is used as-is.
        assert_eq!(
            resolve_dest_path("/tmp/truffle-explicit-file.bin", "ignored").unwrap(),
            "/tmp/truffle-explicit-file.bin"
        );
    }

    // ── PULL_REQUEST authorization (pull-arbitrary-read fix) ──────────

    #[test]
    fn pull_denied_when_no_roots() {
        // Deny-by-default: with no roots registered, even a real file is denied.
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("f.txt");
        std::fs::write(&file, b"hi").unwrap();
        let err = authorize_pull_path(&[], file.to_str().unwrap(), u64::MAX).unwrap_err();
        assert!(matches!(err, TransferError::Rejected(_)));
    }

    #[test]
    fn pull_allowlisted_file_authorized() {
        // A regular file inside a registered root is served (returns its canonical path).
        let dir = tempfile::tempdir().unwrap();
        let root = std::fs::canonicalize(dir.path()).unwrap();
        let file = root.join("f.txt");
        std::fs::write(&file, b"hi").unwrap();
        let got = authorize_pull_path(&[root], file.to_str().unwrap(), u64::MAX).unwrap();
        assert_eq!(got, std::fs::canonicalize(&file).unwrap());
    }

    #[test]
    fn pull_outside_root_rejected() {
        let dir_a = tempfile::tempdir().unwrap();
        let dir_b = tempfile::tempdir().unwrap();
        let root_a = std::fs::canonicalize(dir_a.path()).unwrap();
        let file_b = dir_b.path().join("secret.txt");
        std::fs::write(&file_b, b"secret").unwrap();

        // (1) A file that lives entirely outside the root is rejected.
        let err = authorize_pull_path(
            std::slice::from_ref(&root_a),
            file_b.to_str().unwrap(),
            u64::MAX,
        )
        .unwrap_err();
        assert!(matches!(err, TransferError::Rejected(_)));

        // (2) A `..` escape that climbs out of the root is rejected: canonicalize
        //     resolves the `..`, so the resulting path fails the starts_with check.
        let dir_b_name = dir_b.path().file_name().unwrap().to_str().unwrap();
        let dotdot = format!("{}/../{}/secret.txt", dir_a.path().display(), dir_b_name);
        let err = authorize_pull_path(&[root_a], &dotdot, u64::MAX).unwrap_err();
        assert!(matches!(err, TransferError::Rejected(_)));
    }

    #[test]
    fn pull_prefix_collision_rejected() {
        // "shared-evil" shares a string prefix with the "shared" root but is a
        // different path component — Path::starts_with must not be fooled.
        let base = tempfile::tempdir().unwrap();
        let shared = base.path().join("shared");
        let evil = base.path().join("shared-evil");
        std::fs::create_dir(&shared).unwrap();
        std::fs::create_dir(&evil).unwrap();
        let evil_file = evil.join("f.txt");
        std::fs::write(&evil_file, b"x").unwrap();

        let root = std::fs::canonicalize(&shared).unwrap();
        let err = authorize_pull_path(&[root], evil_file.to_str().unwrap(), u64::MAX).unwrap_err();
        assert!(matches!(err, TransferError::Rejected(_)));
    }

    #[cfg(unix)]
    #[test]
    fn pull_symlink_escape_rejected() {
        // A symlink inside the root pointing at a file outside it must not leak
        // the target: canonicalize resolves the link to its out-of-root target.
        let root_dir = tempfile::tempdir().unwrap();
        let outside_dir = tempfile::tempdir().unwrap();
        let root = std::fs::canonicalize(root_dir.path()).unwrap();

        let secret = outside_dir.path().join("secret.txt");
        std::fs::write(&secret, b"secret").unwrap();

        let link = root.join("link.txt");
        std::os::unix::fs::symlink(&secret, &link).unwrap();

        let err = authorize_pull_path(&[root], link.to_str().unwrap(), u64::MAX).unwrap_err();
        assert!(matches!(err, TransferError::Rejected(_)));
    }

    #[test]
    fn pull_oversize_file_rejected() {
        // A 4-byte file under the root is rejected when max_size is 3.
        let dir = tempfile::tempdir().unwrap();
        let root = std::fs::canonicalize(dir.path()).unwrap();
        let file = root.join("big.bin");
        std::fs::write(&file, b"1234").unwrap();
        let err = authorize_pull_path(&[root], file.to_str().unwrap(), 3).unwrap_err();
        assert!(matches!(err, TransferError::Rejected(_)));
    }

    // ── Mock network provider (per-module copy, mirrors request_reply.rs) ──

    struct MockNetworkProvider {
        identity: NodeIdentity,
        local_addr: PeerAddr,
        peer_event_tx: tokio::sync::broadcast::Sender<NetworkPeerEvent>,
        mock_peers: Arc<tokio::sync::RwLock<Vec<NetworkPeer>>>,
    }

    impl MockNetworkProvider {
        fn new(id: &str) -> Self {
            let (peer_event_tx, _) = tokio::sync::broadcast::channel(64);
            Self {
                identity: NodeIdentity {
                    app_id: "test".to_string(),
                    device_id: id.to_string(),
                    device_name: format!("Test Node {id}"),
                    tailscale_hostname: format!("truffle-test-{id}"),
                    tailscale_id: id.to_string(),
                    dns_name: None,
                    ip: Some("127.0.0.1".parse().unwrap()),
                    login_name: None,
                },
                local_addr: PeerAddr {
                    ip: Some("127.0.0.1".parse().unwrap()),
                    hostname: format!("truffle-test-{id}"),
                    dns_name: None,
                },
                peer_event_tx,
                mock_peers: Arc::new(tokio::sync::RwLock::new(Vec::new())),
            }
        }

        fn event_sender(&self) -> tokio::sync::broadcast::Sender<NetworkPeerEvent> {
            self.peer_event_tx.clone()
        }
    }

    impl NetworkProvider for MockNetworkProvider {
        fn local_identity(&self) -> NodeIdentity {
            self.identity.clone()
        }
        fn local_addr(&self) -> PeerAddr {
            self.local_addr.clone()
        }
        fn peer_events(&self) -> tokio::sync::broadcast::Receiver<NetworkPeerEvent> {
            self.peer_event_tx.subscribe()
        }

        async fn start(&mut self) -> Result<(), NetworkError> {
            Ok(())
        }
        async fn stop(&self) -> Result<(), NetworkError> {
            Ok(())
        }
        async fn peers(&self) -> Vec<NetworkPeer> {
            self.mock_peers.read().await.clone()
        }
        async fn dial_tcp(
            &self,
            _addr: &str,
            _port: u16,
        ) -> Result<tokio::net::TcpStream, NetworkError> {
            Err(NetworkError::DialFailed("mock".into()))
        }
        async fn listen_tcp(&self, _port: u16) -> Result<NetworkTcpListener, NetworkError> {
            Err(NetworkError::ListenFailed("mock".into()))
        }
        async fn unlisten_tcp(&self, _port: u16) -> Result<(), NetworkError> {
            Ok(())
        }
        async fn bind_udp(&self, _port: u16) -> Result<NetworkUdpSocket, NetworkError> {
            Err(NetworkError::NotRunning)
        }
        async fn ping(&self, _addr: &str) -> Result<PingResult, NetworkError> {
            Ok(PingResult {
                latency: Duration::from_millis(1),
                connection: "direct".to_string(),
                peer_addr: None,
            })
        }
        async fn health(&self) -> HealthInfo {
            HealthInfo {
                state: "running".to_string(),
                healthy: true,
                ..Default::default()
            }
        }
    }

    fn ws_config(port: u16) -> WsConfig {
        WsConfig {
            port,
            ping_interval: Duration::from_secs(300),
            pong_timeout: Duration::from_secs(300),
            ..Default::default()
        }
    }

    async fn random_port() -> u16 {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        l.local_addr().unwrap().port()
    }

    async fn make_test_node(
        id: &str,
        ws_port: u16,
    ) -> (
        Node<MockNetworkProvider>,
        tokio::sync::broadcast::Sender<NetworkPeerEvent>,
    ) {
        let provider = MockNetworkProvider::new(id);
        let event_tx = provider.event_sender();
        let network = Arc::new(provider);
        let ws_transport = Arc::new(WebSocketTransport::new(network.clone(), ws_config(ws_port)));
        let session = Arc::new(PeerRegistry::new(network.clone(), ws_transport));
        session.start().await;

        let codec: Arc<dyn EnvelopeCodec> = Arc::new(JsonCodec);
        let node = Node::from_parts(network, session, codec);
        (node, event_tx)
    }

    /// Regression test for the pull-arbitrary-read finding: a PULL_REQUEST for a
    /// real, readable file is denied when the node has registered no pull roots,
    /// and the denial happens WITHOUT any network/file transfer taking place.
    ///
    /// Before the fix this returned `TransferError::Node(...)` (the file was read,
    /// then the OFFER send to the unknown peer failed); after the fix it is a
    /// `Rejected` produced before any file I/O.
    #[tokio::test]
    async fn handle_pull_request_denied_by_default() {
        let (node, _event_tx) = make_test_node("node-a", random_port().await).await;

        // A real, readable file the attacker would love to exfiltrate.
        let dir = tempfile::tempdir().unwrap();
        let secret = dir.path().join("secret.txt");
        std::fs::write(&secret, b"top secret").unwrap();
        let secret_path = secret.to_str().unwrap();

        let (event_tx, _rx) = tokio::sync::broadcast::channel(16);
        let err = handle_pull_request(&node, "peer-x", secret_path, "tok-1", &event_tx)
            .await
            .unwrap_err();

        assert!(
            matches!(err, TransferError::Rejected(_)),
            "expected Rejected, got {err}"
        );
    }

    // ── Overwrite policy / finalize helpers ────────────────────────────

    #[test]
    fn dedup_candidate_naming() {
        // Assert on the file name only: `Path::with_file_name` joins with
        // the platform separator, so exact-path equality fails on Windows.
        let got = dedup_candidate("/d/report.pdf", 1);
        assert!(got.ends_with("report (1).pdf"), "{got}");
        let got = dedup_candidate("/d/Makefile", 2);
        assert!(got.ends_with("Makefile (2)"), "{got}");
    }

    async fn write_file(dir: &std::path::Path, name: &str, content: &[u8]) -> String {
        let p = dir.join(name);
        tokio::fs::write(&p, content).await.unwrap();
        p.to_str().unwrap().to_string()
    }

    #[tokio::test]
    async fn finalize_reject_errors_when_dest_exists() {
        let dir = tempfile::tempdir().unwrap();
        let dest = write_file(dir.path(), "f.txt", b"old").await;
        let temp = write_file(dir.path(), "f.txt.abc.truffle-tmp", b"new").await;

        let err = finalize_received_file(&temp, &dest, OverwritePolicy::Reject)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("already exists"), "{err}");
        // Destination untouched, temp cleaned up.
        assert_eq!(tokio::fs::read(&dest).await.unwrap(), b"old");
        assert!(!tokio::fs::try_exists(&temp).await.unwrap());
    }

    #[tokio::test]
    async fn finalize_reject_places_when_dest_missing() {
        let dir = tempfile::tempdir().unwrap();
        let temp = write_file(dir.path(), "g.txt.abc.truffle-tmp", b"data").await;
        let dest = dir.path().join("g.txt").to_str().unwrap().to_string();

        let placed = finalize_received_file(&temp, &dest, OverwritePolicy::Reject)
            .await
            .unwrap();
        assert_eq!(placed, dest);
        assert_eq!(tokio::fs::read(&dest).await.unwrap(), b"data");
        assert!(!tokio::fs::try_exists(&temp).await.unwrap());
    }

    #[tokio::test]
    async fn finalize_replace_overwrites() {
        let dir = tempfile::tempdir().unwrap();
        let dest = write_file(dir.path(), "h.txt", b"old").await;
        let temp = write_file(dir.path(), "h.txt.abc.truffle-tmp", b"new").await;

        let placed = finalize_received_file(&temp, &dest, OverwritePolicy::Replace)
            .await
            .unwrap();
        assert_eq!(placed, dest);
        assert_eq!(tokio::fs::read(&dest).await.unwrap(), b"new");
    }

    #[tokio::test]
    async fn finalize_rename_dedupes() {
        let dir = tempfile::tempdir().unwrap();
        let dest = write_file(dir.path(), "i.txt", b"old").await;
        let temp = write_file(dir.path(), "i.txt.abc.truffle-tmp", b"new").await;

        let placed = finalize_received_file(&temp, &dest, OverwritePolicy::Rename)
            .await
            .unwrap();
        assert!(placed.ends_with("i (1).txt"), "{placed}");
        assert_eq!(tokio::fs::read(&dest).await.unwrap(), b"old");
        assert_eq!(tokio::fs::read(&placed).await.unwrap(), b"new");
    }

    #[test]
    fn peer_pending_guard_caps_and_releases() {
        let map = Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));
        let g1 = PeerPendingGuard::try_acquire(&map, "p", 2).unwrap();
        let _g2 = PeerPendingGuard::try_acquire(&map, "p", 2).unwrap();
        assert!(PeerPendingGuard::try_acquire(&map, "p", 2).is_none());
        // A different peer is unaffected by p's slots.
        assert!(PeerPendingGuard::try_acquire(&map, "q", 2).is_some());
        drop(g1);
        assert!(PeerPendingGuard::try_acquire(&map, "p", 2).is_some());
    }

    #[tokio::test]
    async fn offer_rejected_when_queue_full() {
        let (node, _peer_events) = make_test_node("node-b", random_port().await).await;

        // A capacity-1 offer channel with its only slot pre-filled.
        let (offer_tx, _offer_rx) = mpsc::channel(1);
        let (dtx, _drx) = tokio::sync::oneshot::channel();
        offer_tx
            .try_send((
                FileOffer {
                    from_peer: "x".into(),
                    from_name: "x".into(),
                    file_name: "a".into(),
                    size: 1,
                    sha256: "0".repeat(64),
                    suggested_path: String::new(),
                    token: "t0".into(),
                },
                OfferResponder::new(dtx),
            ))
            .unwrap();

        let (event_tx, _rx) = tokio::sync::broadcast::channel(16);
        let err = handle_incoming_offer(
            &node,
            "peer-x",
            "b.txt",
            1,
            &"0".repeat(64),
            "",
            "t1",
            &offer_tx,
            &event_tx,
        )
        .await
        .unwrap_err();
        assert!(matches!(err, TransferError::Rejected(_)), "{err}");
    }

    // ── Property + concurrency tests (review: fuzzing / exhaustion) ────

    use proptest::prelude::*;

    proptest! {
        /// Whatever a peer sends as a file name, the sanitized base name
        /// must never be able to escape the destination directory.
        #[test]
        fn safe_base_name_never_escapes(name in ".{0,256}") {
            if let Some(safe) = safe_base_name(&name) {
                prop_assert!(!safe.contains('/'));
                prop_assert!(!safe.contains('\\'));
                prop_assert!(!safe.contains('\0'));
                prop_assert!(safe != "." && safe != "..");
                prop_assert!(!safe.is_empty());
            }
        }
    }

    #[tokio::test]
    async fn concurrent_finalize_same_dest_reject_single_winner() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("race.txt").to_str().unwrap().to_string();
        let t1 = write_file(dir.path(), "race.txt.aaa.truffle-tmp", b"one").await;
        let t2 = write_file(dir.path(), "race.txt.bbb.truffle-tmp", b"two").await;

        let (r1, r2) = tokio::join!(
            finalize_received_file(&t1, &dest, OverwritePolicy::Reject),
            finalize_received_file(&t2, &dest, OverwritePolicy::Reject),
        );
        assert!(
            r1.is_ok() ^ r2.is_ok(),
            "exactly one writer should win: {r1:?} / {r2:?}"
        );
        let winner: &[u8] = if r1.is_ok() { b"one" } else { b"two" };
        assert_eq!(tokio::fs::read(&dest).await.unwrap(), winner);
        // Both temp files consumed (winner) or cleaned up (loser).
        assert!(!tokio::fs::try_exists(&t1).await.unwrap());
        assert!(!tokio::fs::try_exists(&t2).await.unwrap());
    }

    #[tokio::test]
    async fn concurrent_finalize_same_dest_rename_both_win() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("both.txt").to_str().unwrap().to_string();
        let t1 = write_file(dir.path(), "both.txt.aaa.truffle-tmp", b"one").await;
        let t2 = write_file(dir.path(), "both.txt.bbb.truffle-tmp", b"two").await;

        let (r1, r2) = tokio::join!(
            finalize_received_file(&t1, &dest, OverwritePolicy::Rename),
            finalize_received_file(&t2, &dest, OverwritePolicy::Rename),
        );
        let (p1, p2) = (r1.unwrap(), r2.unwrap());
        assert_ne!(p1, p2, "rename policy must dedupe concurrent writers");
        let mut contents = vec![
            tokio::fs::read(&p1).await.unwrap(),
            tokio::fs::read(&p2).await.unwrap(),
        ];
        contents.sort();
        assert_eq!(contents, vec![b"one".to_vec(), b"two".to_vec()]);
    }
}
