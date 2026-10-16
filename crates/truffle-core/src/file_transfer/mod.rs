//! File transfer subsystem for truffle-core.
//!
//! Provides a first-class API for sending and receiving files between peers
//! in the truffle mesh. The [`FileTransfer`] manager handles:
//!
//! - **Sending**: Push a local file to a remote peer.
//! - **Receiving**: Accept/reject incoming file offers via an offer channel.
//! - **Pulling**: Request a file from a remote peer. On the serving side, pulls
//!   are served only from roots registered via [`FileTransfer::add_pull_root`];
//!   with no roots configured all incoming PULL_REQUESTs are rejected.
//! - **Events**: Subscribe to transfer lifecycle events (progress, completed, failed).
//!
//! # Architecture
//!
//! The file transfer module sits on top of the Node API, using:
//! - WS namespace `"ft"` for signaling (offers, accepts, rejects, pull requests)
//! - Raw TCP streams for bulk data transfer
//! - SHA-256 for integrity verification
//!
//! # Example
//!
//! ```ignore
//! let ft = node.file_transfer();
//!
//! // Subscribe to events
//! let mut events = ft.subscribe();
//!
//! // Send a file
//! let result = ft.send_file("peer-id", "/path/to/file.txt", "file.txt").await?;
//!
//! // Receive with manual accept/reject
//! let mut offers = ft.offer_channel().await;
//! while let Some((offer, responder)) = offers.recv().await {
//!     responder.accept("/tmp/received-file.txt");
//! }
//! ```

pub mod receiver;
pub mod sender;
pub mod types;

pub use types::*;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::sync::{broadcast, mpsc, Mutex};
use tokio::task::JoinHandle;
use tracing::info;

use crate::network::NetworkProvider;
use crate::node::Node;

/// Default maximum transfer size: 1 GB.
const DEFAULT_MAX_TRANSFER_SIZE: u64 = 1_073_741_824;

/// Maximum incoming operations (offer handling + pull serving) in flight at
/// once. Work beyond this is rejected immediately instead of queued, so a
/// burst of offers from the tailnet cannot pile up unbounded tasks.
pub(crate) const MAX_CONCURRENT_INCOMING: usize = 32;

/// Maximum offers from a single peer that may sit awaiting an accept/reject
/// decision (each parks a task for up to 60 s).
pub(crate) const MAX_PENDING_OFFERS_PER_PEER: usize = 4;

/// Capacity of the offer channel handed to the application. When the app
/// falls behind, further offers are rejected (fail-fast) instead of queued.
const OFFER_CHANNEL_CAPACITY: usize = 32;

/// File transfer manager (internal state).
///
/// This struct holds the channels and configuration for the file transfer
/// subsystem. It is stored inside [`Node`] and accessed via
/// [`Node::file_transfer()`], which returns a [`FileTransferHandle`]
/// that has access to both this state and the Node's networking capabilities.
pub(crate) struct FileTransferState {
    /// Broadcast sender for file transfer events.
    pub(crate) event_tx: broadcast::Sender<FileTransferEvent>,
    /// Sender side of the offer channel (held to clone for new receivers).
    pub(crate) offer_tx: Mutex<Option<mpsc::Sender<(FileOffer, OfferResponder)>>>,
    /// Maximum allowed transfer size in bytes.
    pub(crate) max_transfer_size: AtomicU64,
    /// Canonicalized roots that PULL_REQUESTs may be served from. Empty = deny all pulls.
    pub(crate) pull_roots: std::sync::RwLock<Vec<std::path::PathBuf>>,
    /// Handle to the background receiver task (lazy-started).
    pub(crate) receiver_handle: Mutex<Option<JoinHandle<()>>>,
    /// What to do when a received file's destination already exists.
    pub(crate) overwrite_policy: std::sync::RwLock<OverwritePolicy>,
    /// Global cap on in-flight incoming operations (offers + pull serving).
    pub(crate) incoming_ops: Arc<tokio::sync::Semaphore>,
    /// Offers per peer currently awaiting an accept/reject decision.
    pub(crate) pending_offers_per_peer:
        Arc<std::sync::Mutex<std::collections::HashMap<String, usize>>>,
}

impl FileTransferState {
    /// Create a new file transfer state.
    pub(crate) fn new() -> Self {
        let (event_tx, _) = broadcast::channel(256);
        Self {
            event_tx,
            offer_tx: Mutex::new(None),
            max_transfer_size: AtomicU64::new(DEFAULT_MAX_TRANSFER_SIZE),
            pull_roots: std::sync::RwLock::new(Vec::new()),
            receiver_handle: Mutex::new(None),
            overwrite_policy: std::sync::RwLock::new(OverwritePolicy::default()),
            incoming_ops: Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_INCOMING)),
            pending_offers_per_peer: Arc::new(std::sync::Mutex::new(
                std::collections::HashMap::new(),
            )),
        }
    }

    /// Current overwrite policy.
    pub(crate) fn overwrite_policy(&self) -> OverwritePolicy {
        *self.overwrite_policy.read().unwrap()
    }
}

/// File transfer handle — the public API for file transfers.
///
/// This is a borrowed reference to a [`Node`] that provides file transfer
/// operations. Obtained via [`Node::file_transfer()`].
///
/// Generic over `N: NetworkProvider` to match the [`Node`] it wraps.
pub struct FileTransfer<'a, N: NetworkProvider + 'static> {
    /// Reference to the owning node.
    node: &'a Node<N>,
}

impl<'a, N: NetworkProvider + 'static> FileTransfer<'a, N> {
    /// Create a new file transfer handle for the given node.
    pub(crate) fn new(node: &'a Node<N>) -> Self {
        Self { node }
    }

    /// Access the internal file transfer state.
    fn state(&self) -> &FileTransferState {
        &self.node.file_transfer_state
    }

    /// Subscribe to file transfer events.
    ///
    /// Returns a broadcast receiver that yields [`FileTransferEvent`]s.
    /// Multiple subscribers are supported.
    pub fn subscribe(&self) -> broadcast::Receiver<FileTransferEvent> {
        self.state().event_tx.subscribe()
    }

    /// Set the maximum allowed transfer size in bytes.
    ///
    /// Transfers exceeding this size will be rejected. Default is 1 GB.
    pub fn set_max_transfer_size(&self, bytes: u64) {
        self.state()
            .max_transfer_size
            .store(bytes, Ordering::Relaxed);
    }

    /// Get the current maximum transfer size in bytes.
    pub fn max_transfer_size(&self) -> u64 {
        self.state().max_transfer_size.load(Ordering::Relaxed)
    }

    /// Set the policy for destinations that already exist.
    ///
    /// Default is [`OverwritePolicy::Reject`]: a transfer to an existing
    /// path fails instead of silently replacing the file.
    pub fn set_overwrite_policy(&self, policy: OverwritePolicy) {
        *self.state().overwrite_policy.write().unwrap() = policy;
    }

    /// Get the current overwrite policy.
    pub fn overwrite_policy(&self) -> OverwritePolicy {
        self.state().overwrite_policy()
    }

    /// Register a directory that incoming PULL_REQUESTs may be served from.
    ///
    /// Pull serving is **deny-by-default**: with no roots registered every
    /// incoming PULL_REQUEST is rejected. The `root` is canonicalized at
    /// registration time (so it must already exist, and symlinked roots
    /// compare correctly against canonicalized request paths); a requested
    /// file is only served if its canonical path lives inside one of these
    /// roots.
    pub fn add_pull_root(&self, root: &str) -> std::io::Result<()> {
        let canon = std::fs::canonicalize(root)?;
        self.state().pull_roots.write().unwrap().push(canon);
        Ok(())
    }

    /// Return the currently registered (canonicalized) pull roots.
    ///
    /// An empty list means pull serving is disabled (deny-by-default).
    pub fn pull_roots(&self) -> Vec<std::path::PathBuf> {
        self.state().pull_roots.read().unwrap().clone()
    }

    /// Remove all registered pull roots, disabling pull serving entirely
    /// (returns to the deny-by-default state).
    pub fn clear_pull_roots(&self) {
        self.state().pull_roots.write().unwrap().clear();
    }

    /// Get a channel for receiving incoming file offers.
    ///
    /// Each offer comes with an [`OfferResponder`] that must be used to
    /// accept or reject the transfer. Calling this starts the background
    /// receiver listener (lazy start, idempotent).
    ///
    /// Only one offer channel can be active at a time. Calling this again
    /// replaces the previous channel (the old receiver will stop getting offers).
    ///
    /// The channel is bounded: when the application stops polling and the
    /// queue fills, further incoming offers are rejected (the sender gets a
    /// fail-fast REJECT) rather than buffered without limit.
    ///
    /// **Important**: The returned receiver must be polled. The `node` reference
    /// used here must remain valid — store the `Node` in an `Arc` and pass it
    /// to spawned tasks as needed.
    pub async fn offer_channel(
        &self,
        node: Arc<Node<N>>,
    ) -> mpsc::Receiver<(FileOffer, OfferResponder)> {
        let (tx, rx) = mpsc::channel(OFFER_CHANNEL_CAPACITY);
        {
            let mut offer_tx = self.state().offer_tx.lock().await;
            *offer_tx = Some(tx.clone());
        }
        self.ensure_receiver_started(node, tx).await;
        rx
    }

    /// Convenience: auto-accept all incoming offers, saving files to
    /// `output_dir`.
    ///
    /// This starts the receiver listener (lazy start) and spawns a task
    /// that automatically accepts every offer with the save path set to
    /// `{output_dir}/{file_name}`.
    pub async fn auto_accept(&self, node: Arc<Node<N>>, output_dir: &str) {
        // Tracked + cancellable so stop() drains this loop too.
        let cancel = node.tasks.cancel.clone();
        let tracker = node.tasks.tracker.clone();
        let mut rx = self.offer_channel(node).await;
        // Normalize to a directory path (trailing separator) so the receiver
        // treats it as a directory and appends a *sanitized* base name derived
        // from the peer's `file_name`. We deliberately IGNORE the peer-supplied
        // `suggested_path`: honoring it would let a remote peer choose an
        // arbitrary destination (path traversal / absolute path → arbitrary file
        // overwrite). See `receiver::resolve_dest_path`.
        let output_dir = format!("{}/", output_dir.trim_end_matches(['/', '\\']));

        tracker.spawn(async move {
            loop {
                let recv = tokio::select! {
                    _ = cancel.cancelled() => break,
                    r = rx.recv() => r,
                };
                let Some((offer, responder)) = recv else {
                    break;
                };
                info!(
                    file = offer.file_name.as_str(),
                    dir = output_dir.as_str(),
                    "Auto-accepting file offer"
                );
                responder.accept(&output_dir);
            }
        });
    }

    /// Convenience: auto-reject all incoming offers.
    ///
    /// This starts the receiver listener (lazy start) and spawns a task
    /// that automatically rejects every offer.
    pub async fn auto_reject(&self, node: Arc<Node<N>>) {
        let cancel = node.tasks.cancel.clone();
        let tracker = node.tasks.tracker.clone();
        let mut rx = self.offer_channel(node).await;

        tracker.spawn(async move {
            loop {
                let recv = tokio::select! {
                    _ = cancel.cancelled() => break,
                    r = rx.recv() => r,
                };
                let Some((_offer, responder)) = recv else {
                    break;
                };
                responder.reject("auto-rejected");
            }
        });
    }

    /// Send a file to a peer.
    ///
    /// Hashes the local file, sends an OFFER via the `"ft"` namespace,
    /// waits for ACCEPT, then streams the file over TCP.
    ///
    /// # Errors
    ///
    /// Returns [`TransferError`] on I/O failure, rejection, timeout, or
    /// integrity check failure.
    pub async fn send_file(
        &self,
        peer_id: &str,
        local_path: &str,
        remote_path: &str,
    ) -> Result<TransferResult, TransferError> {
        let max_size = self.state().max_transfer_size.load(Ordering::Relaxed);
        sender::send_file(
            self.node,
            peer_id,
            local_path,
            remote_path,
            max_size,
            &self.state().event_tx,
        )
        .await
    }

    /// Pull a file from a remote peer.
    ///
    /// Sends a PULL_REQUEST, waits for the peer's OFFER, accepts it, then
    /// receives the file over TCP.
    ///
    /// # Errors
    ///
    /// Returns [`TransferError`] on I/O failure, rejection, timeout, or
    /// integrity check failure.
    pub async fn pull_file(
        &self,
        peer_id: &str,
        remote_path: &str,
        local_path: &str,
    ) -> Result<TransferResult, TransferError> {
        pull_file(
            self.node,
            peer_id,
            remote_path,
            local_path,
            &self.state().event_tx,
        )
        .await
    }

    /// Ensure the background receiver task is running.
    ///
    /// The receiver is lazy-started on the first call to `offer_channel()`,
    /// `auto_accept()`, or `auto_reject()`.
    async fn ensure_receiver_started(
        &self,
        node: Arc<Node<N>>,
        offer_tx: mpsc::Sender<(FileOffer, OfferResponder)>,
    ) {
        let mut handle = self.state().receiver_handle.lock().await;
        // If there's an existing handle, abort it (we're replacing the offer channel)
        if let Some(h) = handle.take() {
            h.abort();
        }
        let h = receiver::spawn_receive_handler(node, offer_tx, self.state().event_tx.clone());
        *handle = Some(h);
    }
}

// ---------------------------------------------------------------------------
// Pull file — download from a remote peer
// ---------------------------------------------------------------------------

/// Pull (download) a file from a remote peer.
///
/// 1. Send PULL_REQUEST via WS
/// 2. Wait for OFFER from peer
/// 3. Send ACCEPT
/// 4. Listen for TCP connection from peer
/// 5. Read [size][sha256][file_bytes], streaming to disk
/// 6. Verify SHA-256, rename temp file
/// 7. Send ACK
async fn pull_file<N: NetworkProvider + 'static>(
    node: &Node<N>,
    peer_id: &str,
    remote_path: &str,
    local_path: &str,
    event_tx: &broadcast::Sender<FileTransferEvent>,
) -> Result<TransferResult, TransferError> {
    use sha2::{Digest, Sha256};
    use std::time::Instant;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let start = Instant::now();
    let token = uuid::Uuid::new_v4().to_string();

    // Fail fast before any network round-trip when the destination exists
    // and the policy forbids replacing it.
    let policy = node.file_transfer_state.overwrite_policy();
    if policy == OverwritePolicy::Reject && tokio::fs::try_exists(local_path).await.unwrap_or(false)
    {
        return Err(TransferError::Protocol(format!(
            "destination already exists: {local_path} (OverwritePolicy::Reject)"
        )));
    }

    // RFC 017 §8: use the stable `device_id` (ULID) as the requester
    // identifier. The hello handshake now carries this between peers so
    // the receiving side can match requester to sender.
    let requester_id = node.local_info().device_id;

    // 0. Resolve peer_id to the Tailscale routing key. The OFFER filter
    // below compares against `IncomingMessage.from`, which carries the
    // connection's WhoIs-verified Tailscale id (RFC 022 §7.5) — never the
    // ULID, so resolving to the ULID here would reject every reply.
    let peer_id = node
        .resolve_peer(peer_id)
        .await
        .map_err(|e| TransferError::Node(e.to_string()))?
        .id;
    let peer_id = peer_id.as_str();

    // 1. Send PULL_REQUEST and wait for OFFER from peer
    let pull_req = FtMessage::PullRequest {
        path: remote_path.to_string(),
        requester_id,
        token: token.clone(),
    };

    let pull_payload = serde_json::to_value(&pull_req)
        .map_err(|e| TransferError::Protocol(format!("Failed to serialize pull request: {e}")))?;

    tracing::info!(peer = peer_id, path = remote_path, "Sending PULL_REQUEST");

    // Negotiation: PULL_REQUEST sent → the serving peer's TCP connection
    // accepted (includes that peer hashing the file before its OFFER, and our
    // own sidecar's per-connection accept handoff, which a push counts in
    // `transfer_secs` instead; see `TransferTimings`).
    let negotiate_start = Instant::now();
    let (offer_sha256, offer_size, offer_token, file_name) = crate::request_reply::send_and_wait(
        node,
        peer_id,
        "ft",
        "pull_request",
        &pull_payload,
        std::time::Duration::from_secs(30),
        |msg| {
            if msg.from != peer_id {
                return None;
            }
            let ft_msg: FtMessage = serde_json::from_value(msg.payload.clone()).ok()?;
            match ft_msg {
                FtMessage::Offer {
                    file_name,
                    sha256,
                    size,
                    token: offer_token,
                    ..
                } => {
                    tracing::info!(size = size, "Received OFFER from peer");
                    Some(Ok((sha256, size, offer_token, file_name)))
                }
                FtMessage::Reject { reason, .. } => Some(Err(TransferError::Rejected(reason))),
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

    // M1: reject an over-size OFFER before accepting the pull.
    let max_size = node
        .file_transfer_state
        .max_transfer_size
        .load(Ordering::Relaxed);
    if offer_size > max_size {
        return Err(TransferError::Protocol(format!(
            "Offered file size {offer_size} exceeds max transfer size {max_size}"
        )));
    }

    // 3. Start TCP listener, then send ACCEPT with our port
    let listener = receiver::DataListener::open(node).await?;

    let accept = FtMessage::Accept {
        token: offer_token,
        tcp_port: listener.port(),
    };

    let sent = match serde_json::to_value(&accept) {
        Ok(accept_payload) => node
            .send_typed(peer_id, "ft", "accept", &accept_payload)
            .await
            .map_err(|e| TransferError::Node(e.to_string())),
        Err(e) => Err(TransferError::Protocol(format!(
            "Failed to serialize accept: {e}"
        ))),
    };
    if let Err(e) = sent {
        listener.release().await;
        return Err(e);
    }

    tracing::info!(
        port = listener.port(),
        "Sent ACCEPT, listening for TCP connection"
    );

    // 4. Accept the serving peer's TCP connection with timeout (connections
    // from any other node are dropped; the listener's tsnet port is released
    // whatever the outcome, even if this future is dropped mid-wait)
    let incoming =
        receiver::accept_data_connection(listener, peer_id, receiver::DATA_CONNECT_TIMEOUT).await?;
    let negotiate_secs = negotiate_start.elapsed().as_secs_f64();
    let transfer_start = Instant::now();

    let mut stream = incoming.stream;
    tracing::info!("TCP connection accepted from peer");

    // 5. Read [8-byte size][64-byte sha256_hex][file_bytes]
    let mut size_buf = [0u8; 8];
    stream.read_exact(&mut size_buf).await?;
    let file_size = u64::from_be_bytes(size_buf);

    let mut sha_buf = [0u8; 64]; // hex-encoded SHA-256
    stream.read_exact(&mut sha_buf).await?;
    let received_sha = String::from_utf8_lossy(&sha_buf).to_string();

    // Verify the metadata matches the OFFER
    if received_sha != offer_sha256 {
        return Err(TransferError::IntegrityError {
            expected: offer_sha256,
            actual: received_sha,
        });
    }

    if file_size != offer_size {
        return Err(TransferError::Protocol(format!(
            "Size mismatch: offer said {offer_size}, stream header says {file_size}"
        )));
    }

    // Stream file data to disk (instead of memory buffer)
    // Create parent directories if needed
    if let Some(parent) = std::path::Path::new(local_path).parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    // Token-suffixed temp name + create_new: concurrent transfers to the
    // same destination each get their own temp file instead of truncating
    // and interleaving with each other.
    let temp_path = format!("{local_path}.{token}.truffle-tmp");
    let mut temp_file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp_path)
        .await?;
    let mut hasher = Sha256::new();
    let mut bytes_received: u64 = 0;
    let progress_start = Instant::now();
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
        AsyncWriteExt::write_all(&mut temp_file, &buf[..n]).await?;
        bytes_received += n as u64;

        let elapsed = progress_start.elapsed().as_secs_f64();
        let speed = if elapsed > 0.0 {
            bytes_received as f64 / elapsed
        } else {
            0.0
        };

        // Emit progress event (best-effort)
        let _ = event_tx.send(FileTransferEvent::Progress(TransferProgress {
            token: token.clone(),
            direction: TransferDirection::Receive,
            file_name: file_name.clone(),
            bytes_transferred: bytes_received,
            total_bytes: file_size,
            speed_bps: speed,
        }));
    }

    // Flush temp file
    AsyncWriteExt::flush(&mut temp_file).await?;

    // 6. Verify SHA-256
    let actual_sha = hex::encode(hasher.finalize());

    if actual_sha != offer_sha256 {
        // Send NACK
        stream.write_all(&[0x00]).await?;
        tokio::fs::remove_file(&temp_path).await.ok();
        return Err(TransferError::IntegrityError {
            expected: offer_sha256,
            actual: actual_sha,
        });
    }

    // Publish temp to the final path per the overwrite policy.
    let placed_path = receiver::finalize_received_file(&temp_path, local_path, policy).await?;

    // 7. Send ACK
    stream.write_all(&[0x01]).await?;
    let transfer_secs = transfer_start.elapsed().as_secs_f64();

    let elapsed = start.elapsed().as_secs_f64();
    tracing::info!(
        bytes = file_size,
        elapsed_ms = (elapsed * 1000.0) as u64,
        path = placed_path.as_str(),
        "Download complete"
    );

    // Emit completed event
    let _ = event_tx.send(FileTransferEvent::Completed {
        token,
        direction: TransferDirection::Receive,
        file_name,
        bytes_transferred: file_size,
        sha256: actual_sha.clone(),
        elapsed_secs: elapsed,
    });

    Ok(TransferResult {
        bytes_transferred: file_size,
        sha256: actual_sha,
        elapsed_secs: elapsed,
        // The puller hashes inline while receiving, so nothing is hashed up
        // front; the ACK is ours to write, and writing it succeeded.
        timings: TransferTimings {
            hash_secs: 0.0,
            negotiate_secs,
            transfer_secs,
            ack_ok: true,
        },
    })
}
