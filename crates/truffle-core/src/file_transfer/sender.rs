//! Send (push) — send a local file to a remote peer.
//!
//! 1. Stream-hash the file (constant memory)
//! 2. Send OFFER via WS
//! 3. Wait for ACCEPT (with tcp_port)
//! 4. Open raw TCP stream to the port advertised in ACCEPT
//! 5. Stream `[size][sha256][file_bytes]` in 64KB chunks
//! 6. Read ACK
//!
//! Steps 1, 2–4 and 5–6 are timed separately into the result's
//! [`TransferTimings`].

use std::time::Instant;

use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::broadcast;
use tracing::{info, warn};

use crate::network::NetworkProvider;
use crate::node::Node;

use super::types::{
    FileTransferEvent, FtMessage, TransferDirection, TransferError, TransferProgress,
    TransferResult, TransferTimings,
};

// @atlas step file-push.offer
// @atlas step file-push.stream
/// Send a local file to a remote peer.
///
/// Uses constant memory — the file is streamed in 64KB chunks for both
/// hashing and TCP transfer. Never loads the entire file into memory.
///
/// Emits [`FileTransferEvent::Progress`], [`FileTransferEvent::Completed`],
/// or [`FileTransferEvent::Failed`] on the provided `event_tx` channel.
///
/// The result's [`TransferTimings`] split the call into hashing,
/// negotiation and the data transfer. A missing ACK does not fail the
/// transfer (every byte was written); it is reported as
/// [`TransferTimings::ack_ok`] `== false`.
pub async fn send_file<N: NetworkProvider + 'static>(
    node: &Node<N>,
    peer_id: &str,
    local_path: &str,
    remote_path: &str,
    max_transfer_size: u64,
    event_tx: &broadcast::Sender<FileTransferEvent>,
) -> Result<TransferResult, TransferError> {
    let start = Instant::now();

    // 0. Resolve peer_id to the Tailscale routing key. Reply correlation
    // below compares against `IncomingMessage.from`, which carries the
    // connection's WhoIs-verified Tailscale id (RFC 022 §7.5) — never the
    // ULID, so resolving to the ULID here would reject every reply.
    let peer_id = node
        .resolve_peer(peer_id)
        .await
        .map_err(|e| TransferError::Node(e.to_string()))?
        .id;
    let peer_id = peer_id.as_str();

    // 1. Get file metadata and check size limit
    let metadata = tokio::fs::metadata(local_path)
        .await
        .map_err(TransferError::Io)?;
    let file_size = metadata.len();

    if file_size > max_transfer_size {
        return Err(TransferError::Protocol(format!(
            "File size ({} bytes) exceeds max transfer size ({} bytes)",
            file_size, max_transfer_size
        )));
    }

    // 2. Stream-hash the file in chunks (constant memory)
    info!(
        path = local_path,
        size = file_size,
        "Hashing file (streaming)"
    );
    let token = uuid::Uuid::new_v4().to_string();
    let hash_start = Instant::now();
    let sha256 = {
        let mut hasher = Sha256::new();
        let mut file = tokio::fs::File::open(local_path)
            .await
            .map_err(TransferError::Io)?;
        let mut buf = vec![0u8; 64 * 1024];
        let mut bytes_hashed: u64 = 0;
        let mut last_event = Instant::now();
        loop {
            let n = file.read(&mut buf).await.map_err(TransferError::Io)?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
            bytes_hashed += n as u64;

            // Emit hashing progress at most 4 times/sec
            if last_event.elapsed() >= std::time::Duration::from_millis(250) {
                let _ = event_tx.send(FileTransferEvent::Hashing {
                    token: token.clone(),
                    file_name: std::path::Path::new(local_path)
                        .file_name()
                        .and_then(|n| n.to_str())
                        .unwrap_or("file")
                        .to_string(),
                    bytes_hashed,
                    total_bytes: file_size,
                });
                last_event = Instant::now();
            }
        }
        hex::encode(hasher.finalize())
    };
    let hash_secs = hash_start.elapsed().as_secs_f64();

    let file_name = std::path::Path::new(local_path)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("file")
        .to_string();

    // Emit final hashing event at 100%
    let _ = event_tx.send(FileTransferEvent::Hashing {
        token: token.clone(),
        file_name: file_name.clone(),
        bytes_hashed: file_size,
        total_bytes: file_size,
    });

    // 3. Send OFFER and wait for ACCEPT/REJECT
    let offer = FtMessage::Offer {
        file_name: file_name.clone(),
        size: file_size,
        sha256: sha256.clone(),
        save_path: remote_path.to_string(),
        token: token.clone(),
        tcp_port: 0,
    };

    let offer_payload = serde_json::to_value(&offer)
        .map_err(|e| TransferError::Protocol(format!("Failed to serialize offer: {e}")))?;

    info!(
        peer = peer_id,
        file = file_name.as_str(),
        size = file_size,
        "Sending OFFER"
    );

    // Notify UI that we're waiting for the receiver's decision
    let _ = event_tx.send(FileTransferEvent::WaitingForAccept {
        token: token.clone(),
        file_name: file_name.clone(),
    });

    // Negotiation: OFFER sent → TCP data stream open.
    let negotiate_start = Instant::now();
    let accept_port = crate::request_reply::send_and_wait(
        node,
        peer_id,
        "ft",
        "offer",
        &offer_payload,
        std::time::Duration::from_secs(60),
        |msg| {
            if msg.from != peer_id {
                return None;
            }
            let ft_msg: FtMessage = serde_json::from_value(msg.payload.clone()).ok()?;
            match ft_msg {
                FtMessage::Accept {
                    token: ref t,
                    tcp_port,
                } if *t == token => Some(Ok(tcp_port)),
                FtMessage::Reject {
                    token: ref t,
                    reason,
                } if *t == token => Some(Err(TransferError::Rejected(reason))),
                _ => None,
            }
        },
    )
    .await
    .map_err(|e| match e {
        crate::request_reply::RequestError::Timeout => TransferError::Timeout,
        crate::request_reply::RequestError::Send(e) => TransferError::Node(e.to_string()),
        crate::request_reply::RequestError::ChannelClosed => {
            TransferError::Protocol("Channel closed".into())
        }
    })??;

    info!(tcp_port = accept_port, "Received ACCEPT from peer");

    // 5. Open raw TCP stream to the port advertised in ACCEPT
    info!(
        accept_port = accept_port,
        peer = peer_id,
        "Opening TCP stream to peer"
    );
    let mut stream = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        node.open_tcp(peer_id, accept_port),
    )
    .await
    .map_err(|_| TransferError::Timeout)?
    .map_err(|e| TransferError::Node(format!("Failed to open TCP to port {accept_port}: {e}")))?;
    let negotiate_secs = negotiate_start.elapsed().as_secs_f64();

    info!(port = accept_port, "TCP stream opened to peer");

    // Transfer: first write to the data stream → ACK read (or the ACK wait
    // ending), so the receiver's verify-and-place is inside the window. So is
    // the receiving sidecar's per-connection accept handoff (WhoIs + bridge):
    // `open_tcp` returns once our own dial completes, before the peer has the
    // connection (see `TransferTimings`).
    let transfer_start = Instant::now();

    // 6. Write header: [8-byte size][64-byte sha256_hex]
    stream.write_all(&file_size.to_be_bytes()).await?;
    stream.write_all(sha256.as_bytes()).await?;

    // 7. Stream the file in 64KB chunks (constant memory)
    let mut file = tokio::fs::File::open(local_path)
        .await
        .map_err(TransferError::Io)?;
    let chunk_size = 64 * 1024;
    let mut buf = vec![0u8; chunk_size];
    let mut bytes_sent: u64 = 0;
    let progress_start = Instant::now();
    let mut last_progress = Instant::now();

    loop {
        let n = file.read(&mut buf).await.map_err(TransferError::Io)?;
        if n == 0 {
            break;
        }
        stream.write_all(&buf[..n]).await?;
        bytes_sent += n as u64;

        // Throttle progress events to max 4/sec (250ms interval)
        if last_progress.elapsed() >= std::time::Duration::from_millis(250) {
            let elapsed = progress_start.elapsed().as_secs_f64();
            let speed = if elapsed > 0.0 {
                bytes_sent as f64 / elapsed
            } else {
                0.0
            };

            let _ = event_tx.send(FileTransferEvent::Progress(TransferProgress {
                token: token.clone(),
                direction: TransferDirection::Send,
                file_name: file_name.clone(),
                bytes_transferred: bytes_sent,
                total_bytes: file_size,
                speed_bps: speed,
            }));
            last_progress = Instant::now();
        }
    }

    // Final progress event at 100%
    let elapsed = progress_start.elapsed().as_secs_f64();
    let speed = if elapsed > 0.0 {
        bytes_sent as f64 / elapsed
    } else {
        0.0
    };
    let _ = event_tx.send(FileTransferEvent::Progress(TransferProgress {
        token: token.clone(),
        direction: TransferDirection::Send,
        file_name: file_name.clone(),
        bytes_transferred: bytes_sent,
        total_bytes: file_size,
        speed_bps: speed,
    }));

    stream.flush().await?;

    // 8. Read ACK (1 byte: 0x01 = OK, 0x00 = error)
    // Use a timeout because the Go bridge may close the connection before
    // the ACK byte propagates. If we time out after the full file was sent,
    // treat it as success but report it via `TransferTimings::ack_ok`: the
    // receiver may have verified the data, but we never heard so.
    let mut ack = [0u8; 1];
    let ack_result = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        stream.read_exact(&mut ack),
    )
    .await;
    let transfer_secs = transfer_start.elapsed().as_secs_f64();

    let ack_ok = match ack_result {
        Ok(Ok(_)) => {
            if ack[0] != 0x01 {
                return Err(TransferError::IntegrityError {
                    expected: sha256.clone(),
                    actual: "receiver reported integrity failure".to_string(),
                });
            }
            true
        }
        Ok(Err(e)) => {
            // Connection closed before ACK — the Go bridge may have torn down
            // the connection before the ACK byte propagated. Since we sent all
            // data successfully, log the issue but don't fail the transfer.
            warn!("ACK read failed (bridge may have closed early): {e}");
            false
        }
        Err(_timeout) => {
            warn!("ACK read timed out after 10s (bridge may have closed early)");
            false
        }
    };

    let elapsed = start.elapsed().as_secs_f64();
    info!(
        bytes = file_size,
        elapsed_ms = (elapsed * 1000.0) as u64,
        "Upload complete"
    );

    let _ = event_tx.send(FileTransferEvent::Completed {
        token,
        direction: TransferDirection::Send,
        file_name,
        bytes_transferred: file_size,
        sha256: sha256.clone(),
        elapsed_secs: elapsed,
    });

    Ok(TransferResult {
        bytes_transferred: file_size,
        sha256,
        elapsed_secs: elapsed,
        timings: TransferTimings {
            hash_secs,
            negotiate_secs,
            transfer_secs,
            ack_ok,
        },
    })
}
