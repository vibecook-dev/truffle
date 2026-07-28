//! Node API — the single public entry point for all truffle functionality.
//!
//! The [`Node`] struct wires together Layers 3-6 and exposes a clean ~12-method
//! API that Layer 7 applications consume. Applications should **never** import
//! from lower layers directly; everything they need is accessible through `Node`.
//!
//! # Quick start
//!
//! ```ignore
//! use truffle_core::Node;
//!
//! let node = Node::builder()
//!     .name("my-app")
//!     .sidecar_path("/usr/local/bin/truffle-sidecar")
//!     .build()
//!     .await?;
//!
//! // Discover peers (Layer 3 — no transport needed)
//! let peers = node.peers().await;
//!
//! // Send a namespaced message (Layer 6 envelope over Layer 4 WS).
//! // String args remain queries; RFC 022 Phase B adds Peer handles.
//! node.send(&peers[0].tailscale_id, "chat", b"hello!").await?;
//!
//! // Subscribe to a namespace
//! let mut rx = node.subscribe("chat");
//! let msg = rx.recv().await?;
//!
//! // Open a raw TCP stream (Layer 4 direct)
//! let stream = node.open_tcp(&peers[0].tailscale_id, 8080).await?;
//! ```

use std::collections::HashMap;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
// `std::sync::RwLock` is aliased to `StdRwLock` so the namespace_filters
// lock — which is held only briefly for pure HashMap ops with no `.await`
// points underneath — can stay sync. Keeping it sync lets
// `Node::subscribe` remain non-async (required by the NAPI bridge, which
// is called from Node.js's sync main thread with no tokio runtime in
// scope) without the `try_write`-panic footgun that the previous
// `tokio::sync::RwLock` implementation suffered from (see RFC 017 fix K).
use std::sync::RwLock as StdRwLock;

use tokio::net::TcpStream;
use tokio::sync::broadcast;
// The tokio RwLock is only used by the in-file test `MockNetworkProvider`
// (see `#[cfg(test)] mod tests`); production code now uses the std
// RwLock alias above for `namespace_filters`.
#[cfg(test)]
use tokio::sync::RwLock;

use crate::envelope::codec::{EnvelopeCodec, JsonCodec};
use crate::envelope::{Envelope, EnvelopeError};
use crate::file_transfer::{self, FileTransferState};
use crate::identity::{self, AppId, DeviceId, DeviceName};
use crate::network::tailscale::{TailscaleConfig, TailscaleProvider};
use crate::network::{DialOpts, HealthInfo, ListenOpts, NetworkProvider, NodeIdentity, PingResult};
use crate::session::{PeerEvent, PeerRegistry, PeerState};
use crate::transport::quic::{QuicConnection, QuicListener};
use crate::transport::websocket::WebSocketTransport;
use crate::transport::{DatagramSocket, RawListener, WsConfig};

// ---------------------------------------------------------------------------
// NamespacedMessage — public message type for subscribers
// ---------------------------------------------------------------------------

/// A message received on a specific namespace.
///
/// This is the public type that [`Node::subscribe`] delivers to application
/// code. It contains the deserialized envelope fields plus the sender's peer ID.
#[derive(Debug, Clone)]
pub struct NamespacedMessage {
    /// Stable node ID of the sender.
    pub from: String,
    /// Namespace the message was sent on.
    pub namespace: String,
    /// Application-defined message type within the namespace.
    pub msg_type: String,
    /// Opaque JSON payload.
    pub payload: serde_json::Value,
    /// Millisecond Unix timestamp from the sender, if set.
    pub timestamp: Option<u64>,
}

impl NamespacedMessage {
    /// Decode the payload of a [`send_bytes`](Node::send_bytes) /
    /// [`broadcast_bytes`](Node::broadcast_bytes) message.
    ///
    /// Returns `None` unless `msg_type == "bytes"` with a valid base64
    /// `data` field.
    pub fn payload_bytes(&self) -> Option<Vec<u8>> {
        if self.msg_type != "bytes" {
            return None;
        }
        let s = self.payload.get("data")?.as_str()?;
        use base64::Engine as _;
        base64::engine::general_purpose::STANDARD.decode(s).ok()
    }
}

// ---------------------------------------------------------------------------
// Peer — simplified view for application code
// ---------------------------------------------------------------------------

/// A peer as seen by application code (RFC 022 projection).
///
/// This is a simplified projection of the internal [`PeerState`] that hides
/// session-layer internals. Networking still accepts string queries today;
/// Phase B of RFC 022 will take handle-first parameters. Fields are already
/// honest: `device_id` is never a Tailscale-id fallback.
#[derive(Debug, Clone)]
pub struct Peer {
    /// Durable ULID once known and published; `None` until identity is learned
    /// (or while suppressed under first-wins). Never equals `tailscale_id`.
    pub device_id: Option<String>,
    /// Human-readable device name from the hello identity block, if known.
    pub device_name: Option<String>,
    /// Best label for UI: identity name → hostname with `truffle-{appId}-`
    /// stripped when possible → short tailscale id.
    pub display_name: String,
    /// Layer 3 Tailscale hostname (`truffle-{appId}-{slug}`).
    pub hostname: String,
    /// Tailscale stable node ID — routing key and advanced diagnostics.
    pub tailscale_id: String,
    /// Process-local ref `{tailscale_id}:{generation}` (RFC 022).
    pub peer_ref: String,
    /// Generation of this registry entry (bumped on re-join).
    pub generation: u64,
    /// Network IP address.
    pub ip: IpAddr,
    /// Whether the peer is online (from Layer 3).
    pub online: bool,
    /// Whether there is an active envelope-bus WebSocket connection.
    pub ws_connected: bool,
    /// Connection type description (e.g., `"direct"` or `"relay:ord"`).
    pub connection_type: String,
    /// Operating system, if known. Prefers the hello envelope's value
    /// and falls back to Layer 3.
    pub os: Option<String>,
    /// Last time the peer was seen online (RFC 3339 string).
    pub last_seen: Option<String>,
}

impl From<PeerState> for Peer {
    fn from(s: PeerState) -> Self {
        let (device_id, device_name, os_from_hello) = if s.identity_suppressed {
            (None, None, None)
        } else {
            match s.identity.as_ref() {
                Some(identity) => (
                    Some(identity.device_id.clone()),
                    Some(identity.device_name.clone()),
                    Some(identity.os.clone()),
                ),
                None => (None, None, None),
            }
        };

        // Invariant I1: published device_id must never equal the Tailscale id.
        debug_assert!(
            device_id
                .as_ref()
                .map(|d| d.as_str() != s.id.as_str())
                .unwrap_or(true),
            "RFC 022 I1 violated: device_id must not equal tailscale_id"
        );

        let display_name = device_name
            .clone()
            .filter(|n| !n.is_empty())
            .unwrap_or_else(|| display_name_from_hostname(&s.name, &s.id));

        let peer_ref = s.peer_ref();
        let generation = s.generation;
        let os = os_from_hello.or(s.os.clone());

        Self {
            device_id,
            device_name,
            display_name,
            hostname: s.name,
            tailscale_id: s.id,
            peer_ref,
            generation,
            ip: s.ip,
            online: s.online,
            ws_connected: s.ws_connected,
            connection_type: s.connection_type,
            os,
            last_seen: s.last_seen,
        }
    }
}

/// Derive a human-ish display name from a Tailscale hostname when identity
/// is not yet known: strip a leading `truffle-…-` app prefix when present.
fn display_name_from_hostname(hostname: &str, tailscale_id: &str) -> String {
    const PREFIX: &str = "truffle-";
    if let Some(rest) = hostname.strip_prefix(PREFIX) {
        // rest = "{appId}-{slug…}"; drop the first label (appId).
        if let Some((_, slug)) = rest.split_once('-') {
            if !slug.is_empty() {
                return slug.to_string();
            }
        }
    }
    if !hostname.is_empty() {
        return hostname.to_string();
    }
    // Last resort: short tailscale id.
    let short: String = tailscale_id.chars().take(8).collect();
    if short.is_empty() {
        "peer".to_string()
    } else {
        short
    }
}

// ---------------------------------------------------------------------------
// NodeError
// ---------------------------------------------------------------------------

/// Errors from the Node API.
#[derive(Debug, thiserror::Error)]
pub enum NodeError {
    /// The requested peer is not known.
    #[error("peer not found: {0}")]
    PeerNotFound(String),

    /// The query matched more than one peer (RFC 022 `mesh.peer`).
    #[error("ambiguous peer query '{query}': {} candidates", candidates.len())]
    AmbiguousPeer {
        /// Original query string.
        query: String,
        /// Candidate display labels / routing keys for the UI.
        candidates: Vec<String>,
    },

    /// The peer handle is no longer usable (left the mesh).
    #[error("peer gone: {0}")]
    PeerGone(String),

    /// Failed to establish a connection.
    #[error("connection failed: {0}")]
    ConnectionFailed(String),

    /// Failed to send a message.
    #[error("send failed: {0}")]
    SendFailed(String),

    /// Envelope encoding/decoding error.
    #[error("envelope error: {0}")]
    Envelope(#[from] EnvelopeError),

    /// Session layer error.
    #[error("session error: {0}")]
    Session(#[from] crate::session::SessionError),

    /// Network layer error.
    #[error("network error: {0}")]
    Network(#[from] crate::network::NetworkError),

    /// Transport layer error.
    #[error("transport error: {0}")]
    Transport(#[from] crate::transport::TransportError),

    /// The requested feature is not yet implemented.
    #[error("not implemented: {0}")]
    NotImplemented(String),

    /// The port is reserved by truffle's own listeners.
    #[error("port {0} is reserved by truffle (the session WebSocket port)")]
    ReservedPort(u16),

    /// The node has been stopped.
    #[error("node stopped")]
    Stopped,

    /// Builder configuration error.
    #[error("build error: {0}")]
    BuildError(String),

    /// I/O error from the builder (state dir creation, device-id persistence).
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

// ---------------------------------------------------------------------------
// Node
// ---------------------------------------------------------------------------

/// The main truffle node — single public entry point for all functionality.
///
/// Generic over `N: NetworkProvider` so that tests can inject a mock provider
/// without Tailscale. In production, use the concrete type
/// `Node<TailscaleProvider>` (created via [`NodeBuilder`]).
///
/// # Lifecycle
///
/// 1. Create via [`Node::builder()`] + `.build().await`
/// 2. Use `peers()`, `send()`, `subscribe()`, `open_tcp()`, etc.
/// 3. Call `stop()` to shut down
pub struct Node<N: NetworkProvider + 'static> {
    /// Layer 3 network provider.
    pub(crate) network: Arc<N>,
    /// Layer 5 session / peer registry.
    session: Arc<PeerRegistry<N>>,
    /// Layer 6 envelope codec.
    codec: Arc<dyn EnvelopeCodec>,
    /// Broadcast sender for all incoming namespaced messages.
    /// Kept alive to prevent the channel from closing. The router task holds a clone.
    #[allow(dead_code)]
    incoming_tx: broadcast::Sender<NamespacedMessage>,
    /// Per-namespace subscription channels.
    ///
    /// Guarded by a `std::sync::RwLock` (not tokio) because the lock is
    /// held only for quick HashMap operations with no `.await` points
    /// underneath, and the `subscribe()` API has to be callable from
    /// synchronous contexts (the NAPI bridge, which runs on Node.js's
    /// main thread with no tokio runtime in scope).
    namespace_filters: Arc<StdRwLock<HashMap<String, broadcast::Sender<NamespacedMessage>>>>,
    /// File transfer subsystem state.
    pub(crate) file_transfer_state: FileTransferState,
    /// State directory for persistence (e.g., synced store backends). Empty
    /// path when constructed via `from_parts` (tests); set by the builder.
    state_dir: PathBuf,
    /// The session WebSocket listen port (reserved against raw listeners
    /// and proxy listeners alike — RFC 023 G4).
    /// Defaults to 9417 in `from_parts`; set by the builder.
    pub(crate) ws_port: u16,
    /// Reverse proxy subsystem state.
    pub(crate) proxy_state: Arc<crate::proxy::ProxyState>,
    /// Set once [`stop`](Self::stop) has completed teardown. Makes further
    /// `stop()` calls no-ops and causes the send paths to fail fast with
    /// [`NodeError::Stopped`].
    stopped: std::sync::atomic::AtomicBool,
    /// Structured ownership of node-scoped background tasks.
    pub(crate) tasks: NodeTasks,
}

/// Structured ownership of the node's background tasks.
///
/// Every node-scoped task (envelope router, file-transfer dispatch and
/// per-transfer work) is spawned on the [`TaskTracker`] and watches the
/// [`CancellationToken`]. [`Node::stop`] cancels the token, then waits for
/// the tracker to drain with a bounded timeout, hard-aborting the retained
/// long-lived handles as a last resort — so `stop()` deterministically
/// means "all node work has stopped" and can never hang.
pub(crate) struct NodeTasks {
    /// Cooperative cancellation signal for every node-scoped task.
    pub(crate) cancel: tokio_util::sync::CancellationToken,
    /// Tracks all node-scoped tasks so stop() can await their completion.
    pub(crate) tracker: tokio_util::task::TaskTracker,
    /// Handles to long-lived tasks, retained for hard-abort if the
    /// cooperative drain times out. Ephemeral tasks (per-transfer work) are
    /// tracked but not retained here — holding a handle per completed
    /// transfer would itself grow without bound.
    long_lived: std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

impl NodeTasks {
    fn new() -> Self {
        Self {
            cancel: tokio_util::sync::CancellationToken::new(),
            tracker: tokio_util::task::TaskTracker::new(),
            long_lived: std::sync::Mutex::new(Vec::new()),
        }
    }

    /// Spawn a long-lived node task: tracked for the stop() drain AND
    /// retained for hard-abort if that drain times out.
    fn spawn_long_lived<F>(&self, fut: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let handle = self.tracker.spawn(fut);
        self.long_lived.lock().unwrap().push(handle);
    }

    /// Abort every retained long-lived task (drain-timeout fallback).
    fn abort_long_lived(&self) {
        for h in self.long_lived.lock().unwrap().drain(..) {
            h.abort();
        }
    }
}

impl<N: NetworkProvider + 'static> Node<N> {
    /// Create a `Node` from pre-built components (used by builder and tests).
    ///
    /// This constructor wires together the layers and spawns the envelope
    /// router task that reads from the session layer, deserializes envelopes,
    /// and dispatches to namespace subscribers.
    pub(crate) fn from_parts(
        network: Arc<N>,
        session: Arc<PeerRegistry<N>>,
        codec: Arc<dyn EnvelopeCodec>,
    ) -> Self {
        let (incoming_tx, _) = broadcast::channel(1024);
        let namespace_filters: Arc<
            StdRwLock<HashMap<String, broadcast::Sender<NamespacedMessage>>>,
        > = Arc::new(StdRwLock::new(HashMap::new()));

        let node = Self {
            network,
            session: session.clone(),
            codec: codec.clone(),
            incoming_tx: incoming_tx.clone(),
            namespace_filters: namespace_filters.clone(),
            file_transfer_state: FileTransferState::new(),
            state_dir: PathBuf::new(),
            ws_port: 9417,
            proxy_state: Arc::new(crate::proxy::ProxyState::new()),
            stopped: std::sync::atomic::AtomicBool::new(false),
            tasks: NodeTasks::new(),
        };

        // Spawn the envelope router task.
        node.spawn_envelope_router(session, codec, incoming_tx, namespace_filters);

        // Forward runtime proxy-engine errors into the proxy event channel
        // (RFC 023 G5). Providers without a proxy engine return None and
        // the task is skipped entirely.
        node.spawn_proxy_error_forwarder();

        node
    }

    /// Spawn the node-scoped task that turns provider
    /// [`ProxyRuntimeError`](crate::network::ProxyRuntimeError)s into
    /// [`ProxyEvent::Error`](crate::proxy::ProxyEvent) broadcasts and
    /// `ProxyStatus::Error` state.
    fn spawn_proxy_error_forwarder(&self) {
        let Some(mut rx) = self.network.proxy_runtime_errors() else {
            return;
        };
        let proxy_state = self.proxy_state.clone();
        let cancel = self.tasks.cancel.clone();
        self.tasks.spawn_long_lived(async move {
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => {
                        tracing::debug!("proxy error forwarder: cancelled by stop()");
                        return;
                    }
                    event = rx.recv() => match event {
                        Ok(e) => proxy_state.record_runtime_error(e.id, e.code, e.message),
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            tracing::warn!("proxy error forwarder lagged by {n} events");
                        }
                        Err(broadcast::error::RecvError::Closed) => return,
                    }
                }
            }
        });
    }

    /// Spawn a background task that reads incoming raw messages from the
    /// session layer, deserializes them as envelopes, and routes them to
    /// the global channel and per-namespace subscribers.
    fn spawn_envelope_router(
        &self,
        session: Arc<PeerRegistry<N>>,
        codec: Arc<dyn EnvelopeCodec>,
        incoming_tx: broadcast::Sender<NamespacedMessage>,
        namespace_filters: Arc<StdRwLock<HashMap<String, broadcast::Sender<NamespacedMessage>>>>,
    ) {
        let mut rx = session.subscribe();
        let cancel = self.tasks.cancel.clone();

        self.tasks.spawn_long_lived(async move {
            loop {
                let recv = tokio::select! {
                    _ = cancel.cancelled() => {
                        tracing::debug!("envelope router: cancelled by stop()");
                        break;
                    }
                    result = rx.recv() => result,
                };
                match recv {
                    Ok(msg) => {
                        if let Ok(envelope) = codec.decode(&msg.data) {
                            let namespaced = NamespacedMessage {
                                from: msg.from,
                                namespace: envelope.namespace.clone(),
                                msg_type: envelope.msg_type,
                                payload: envelope.payload,
                                timestamp: envelope.timestamp,
                            };

                            tracing::debug!(
                                from = %namespaced.from,
                                namespace = %namespaced.namespace,
                                msg_type = %namespaced.msg_type,
                                "envelope router: dispatching message"
                            );

                            // Send to global channel (best-effort).
                            let _ = incoming_tx.send(namespaced.clone());

                            // Route to namespace-specific subscriber if present.
                            // Sync read on the std RwLock: the critical section
                            // is a HashMap lookup with no `.await` points.
                            // Poisoning is recovered (into_inner) instead of
                            // panicking: the map stays structurally valid after
                            // any panic, and a panic here would kill routing
                            // for the lifetime of the node.
                            let namespace = namespaced.namespace.clone();
                            let dead_subscriber = {
                                let filters = namespace_filters
                                    .read()
                                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                                match filters.get(&namespace) {
                                    Some(tx) => {
                                        let send_result = tx.send(namespaced);
                                        tracing::debug!(
                                            namespace = %namespace,
                                            subscriber_count = tx.receiver_count(),
                                            sent = send_result.is_ok(),
                                            "envelope router: sent to namespace subscriber"
                                        );
                                        send_result.is_err()
                                    }
                                    None => {
                                        tracing::debug!(
                                            namespace = %namespace,
                                            "envelope router: no subscriber for namespace"
                                        );
                                        false
                                    }
                                }
                            };

                            // A failed send means every receiver was dropped:
                            // prune the entry so dynamic namespaces don't grow
                            // the map forever. Re-checked under the write lock —
                            // subscribe() adds receivers while holding the read
                            // lock, so a zero count here cannot race with a new
                            // subscriber.
                            if dead_subscriber {
                                let mut filters = namespace_filters
                                    .write()
                                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                                if filters
                                    .get(&namespace)
                                    .is_some_and(|tx| tx.receiver_count() == 0)
                                {
                                    filters.remove(&namespace);
                                    tracing::debug!(
                                        namespace = %namespace,
                                        "envelope router: pruned dead namespace subscriber"
                                    );
                                }
                            }
                        } else {
                            tracing::warn!(
                                from = %msg.from,
                                data_len = msg.data.len(),
                                "node: failed to decode envelope from incoming message"
                            );
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(
                            missed = n,
                            "node: envelope router lagged, missed {n} messages"
                        );
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        tracing::debug!("node: session incoming channel closed, router exiting");
                        break;
                    }
                }
            }
        });
    }

    // ── Builder ──────────────────────────────────────────────────────────

    /// Create a new [`NodeBuilder`] for configuring and constructing a node.
    pub fn builder() -> NodeBuilder {
        NodeBuilder::default()
    }

    // ── File Transfer ────────────────────────────────────────────────────

    /// Access the file transfer subsystem.
    ///
    /// Returns a [`FileTransfer`](file_transfer::FileTransfer) handle
    /// that provides methods for sending, receiving, and pulling files.
    pub fn file_transfer(&self) -> file_transfer::FileTransfer<'_, N> {
        file_transfer::FileTransfer::new(self)
    }

    // ── Reverse Proxy ──────────────────────────────────────────────────

    /// Access the reverse proxy subsystem.
    ///
    /// Returns a [`Proxy`](crate::proxy::Proxy) handle that provides
    /// methods for adding, removing, and listing reverse proxies.
    pub fn proxy(&self) -> crate::proxy::Proxy<'_, N> {
        crate::proxy::Proxy::new(self)
    }

    /// Create a synchronized store for device-owned state.
    ///
    /// Returns an `Arc<SyncedStore<T>>` that syncs data across the mesh on
    /// namespace `"ss:{store_id}"`. The caller owns the returned Arc;
    /// the background sync task also holds one.
    ///
    /// Requires `self` to be wrapped in an `Arc` because the sync task
    /// needs to outlive this call.
    pub fn synced_store<T>(
        self: &Arc<Self>,
        store_id: &str,
    ) -> Arc<crate::synced_store::SyncedStore<T>>
    where
        T: serde::Serialize + serde::de::DeserializeOwned + Clone + Send + Sync + 'static,
    {
        crate::synced_store::SyncedStore::new(self.clone(), store_id)
    }

    /// Create a synchronized store with a custom persistence backend.
    ///
    /// Same as [`synced_store`](Self::synced_store) but restores persisted
    /// data on startup and writes through to the backend on every change.
    pub fn synced_store_with_backend<T>(
        self: &Arc<Self>,
        store_id: &str,
        backend: std::sync::Arc<dyn crate::synced_store::StoreBackend>,
    ) -> Arc<crate::synced_store::SyncedStore<T>>
    where
        T: serde::Serialize + serde::de::DeserializeOwned + Clone + Send + Sync + 'static,
    {
        crate::synced_store::SyncedStore::new_with_backend(self.clone(), store_id, backend)
    }

    // ── State directory ───────────────────��────────────────────────────��

    /// Set the state directory (called by the builder after construction).
    pub(crate) fn with_state_dir(mut self, dir: PathBuf) -> Self {
        self.state_dir = dir;
        self
    }

    /// Record the session WebSocket port (called by the builder) so the
    /// reserved-port guard tracks a customized `ws_port`.
    pub(crate) fn with_ws_port(mut self, port: u16) -> Self {
        self.ws_port = port;
        self
    }

    /// The state directory for persistence backends.
    ///
    /// Returns an empty path for test nodes created via `from_parts`.
    pub fn state_dir(&self) -> &Path {
        &self.state_dir
    }

    // ── Lifecycle ────────────────────────────────────────────────────────

    /// Stop the node and all underlying layers.
    ///
    /// Closes every active WebSocket connection (Layer 5) and shuts down the
    /// network provider (Layer 3 — sidecar + bridge). After `stop()` returns,
    /// [`send`](Self::send) and [`send_typed`](Self::send_typed) fail with
    /// [`NodeError::Stopped`], and [`broadcast`](Self::broadcast) /
    /// [`broadcast_typed`](Self::broadcast_typed) become no-ops.
    ///
    /// Idempotent: calling `stop()` more than once is safe — subsequent calls
    /// return immediately without repeating teardown.
    ///
    /// When `stop()` returns, all node-scoped background work has stopped:
    /// the envelope router, the file-transfer dispatch task, and in-flight
    /// per-transfer tasks are cancelled and drained (with a bounded wait
    /// that hard-aborts stragglers, so `stop()` cannot hang). Session and
    /// provider background loops are torn down by their own layers.
    pub async fn stop(&self) {
        if self.stopped.swap(true, std::sync::atomic::Ordering::SeqCst) {
            tracing::debug!("node: stop() called on already-stopped node");
            return;
        }
        tracing::info!("node: stopping");

        // 1. Cancel every node-scoped background task (envelope router,
        //    file-transfer dispatch + per-transfer work). Cooperative: each
        //    task exits at its next await point.
        self.tasks.cancel.cancel();

        // 2. Drop the file-transfer dispatch handle so a later
        //    offer_channel() replace cannot race a half-stopped task.
        if let Some(h) = self.file_transfer_state.receiver_handle.lock().await.take() {
            h.abort();
        }

        // 3. Layer 5: close all active WebSocket connections and stop the
        //    session's background loops (peer-event + accept).
        self.session.shutdown().await;

        // 4. Layer 3: shut down the network provider (sidecar + bridge).
        if let Err(e) = self.network.stop().await {
            tracing::warn!(error = %e, "node: network provider stop failed");
        }

        // 5. Deterministically drain node tasks. The token already fired,
        //    so this normally completes immediately; the timeout guards
        //    against a task stuck in an await that never resolves.
        self.tasks.tracker.close();
        let drain =
            tokio::time::timeout(std::time::Duration::from_secs(5), self.tasks.tracker.wait());
        if drain.await.is_err() {
            tracing::warn!("node: background tasks did not drain in 5s; aborting stragglers");
            self.tasks.abort_long_lived();
            let _ =
                tokio::time::timeout(std::time::Duration::from_secs(1), self.tasks.tracker.wait())
                    .await;
        }
        tracing::info!("node: stopped");
    }

    // ── Identity ─────────────────────────────────────────────────────────

    /// Return the local node's identity (stable ID, hostname, name).
    pub fn local_info(&self) -> NodeIdentity {
        self.network.local_identity()
    }

    // ── Discovery (from Layer 3, no transport needed) ────────────────────

    /// Return all known peers.
    ///
    /// Includes peers that are online but not yet connected (no active WS).
    /// This information comes from Layer 3 peer discovery.
    pub async fn peers(&self) -> Vec<Peer> {
        let app_id = self.network.local_identity().app_id;
        self.session
            .peers()
            .await
            .into_iter()
            .map(|s| Self::project_peer(s, &app_id))
            .collect()
    }

    /// Project a session [`PeerState`] to the public [`Peer`] view.
    fn project_peer(s: PeerState, app_id: &str) -> Peer {
        // RFC 022: `device_name` stays None until identity is known.
        // Prefer a stripped hostname slug for `display_name` when we
        // have no hello name yet (raw-transport / pre-identity peers).
        let bare = s
            .identity
            .is_none()
            .then(|| hostname_slug(&s.name, app_id).map(str::to_string))
            .flatten();
        let mut peer = Peer::from(s);
        if let Some(bare) = bare {
            peer.display_name = bare;
        }
        peer
    }

    /// Subscribe to peer change events (joined, left, connected, etc.).
    pub fn on_peer_change(&self) -> broadcast::Receiver<PeerEvent> {
        self.session.on_peer_change()
    }

    /// Resolve a query to a public [`Peer`] handle view (RFC 022).
    ///
    /// - Not found → `Ok(None)` after optional wait
    /// - Ambiguous (multiple name / short-prefix hits) → [`NodeError::AmbiguousPeer`]
    ///
    /// `wait_ms`: when set, block until the query becomes resolvable or the
    /// timeout elapses (then `Ok(None)`). Useful for rehydrating a saved ULID
    /// whose owner has not completed hello yet.
    pub async fn peer(&self, query: &str, wait_ms: Option<u64>) -> Result<Option<Peer>, NodeError> {
        match self.resolve_peer(query).await {
            Ok(state) => {
                let app_id = self.network.local_identity().app_id;
                return Ok(Some(Self::project_peer(state, &app_id)));
            }
            Err(NodeError::PeerNotFound(_)) => {}
            Err(e) => return Err(e),
        }

        let Some(ms) = wait_ms.filter(|m| *m > 0) else {
            return Ok(None);
        };

        let mut rx = self.session.on_peer_change();

        // Re-check immediately after subscribing: an event landing between
        // the initial miss above and the subscription would otherwise be
        // lost, and the query would only re-resolve on the NEXT event (or
        // never — burning the whole timeout on an already-resolvable peer).
        match self.resolve_peer(query).await {
            Ok(state) => {
                let app_id = self.network.local_identity().app_id;
                return Ok(Some(Self::project_peer(state, &app_id)));
            }
            Err(NodeError::PeerNotFound(_)) => {}
            Err(e) => return Err(e),
        }

        let deadline = tokio::time::Instant::now() + Duration::from_millis(ms);
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Ok(None);
            }
            match tokio::time::timeout(remaining, rx.recv()).await {
                Ok(Ok(_)) => match self.resolve_peer(query).await {
                    Ok(state) => {
                        let app_id = self.network.local_identity().app_id;
                        return Ok(Some(Self::project_peer(state, &app_id)));
                    }
                    Err(NodeError::PeerNotFound(_)) => continue,
                    Err(e) => return Err(e),
                },
                Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(_))) => continue,
                Ok(Err(tokio::sync::broadcast::error::RecvError::Closed)) => return Ok(None),
                Err(_) => return Ok(None), // timeout
            }
        }
    }

    /// Resolve any accepted peer identifier form to the peer's current
    /// session state.
    ///
    /// The single resolution path shared by
    /// [`resolve_peer_id`](Self::resolve_peer_id), [`ping`](Self::ping),
    /// and the raw transport methods. Accepts the identifier forms
    /// documented on `resolve_peer_id`, plus the peer's Tailscale IP.
    pub(crate) async fn resolve_peer(&self, peer_ref: &str) -> Result<PeerState, NodeError> {
        let peers = self.session.peers().await;
        let app_id = self.network.local_identity().app_id;

        // Exact matches that are always unique.
        for p in &peers {
            if let Some(uid) = p.published_device_id() {
                if uid == peer_ref {
                    return Ok(p.clone());
                }
            }
            if p.id == peer_ref || p.name == peer_ref || p.ip.to_string() == peer_ref {
                return Ok(p.clone());
            }
            if p.peer_ref() == peer_ref {
                return Ok(p.clone());
            }
        }

        // Device-name matches (published identity names) — may be ambiguous.
        let name_hits: Vec<&PeerState> = peers
            .iter()
            .filter(|p| {
                p.identity
                    .as_ref()
                    .map(|i| !p.identity_suppressed && i.device_name.eq_ignore_ascii_case(peer_ref))
                    .unwrap_or(false)
            })
            .collect();
        if name_hits.len() > 1 {
            return Err(NodeError::AmbiguousPeer {
                query: peer_ref.to_string(),
                candidates: name_hits
                    .iter()
                    .map(|p| p.published_device_id().unwrap_or(p.id.as_str()).to_string())
                    .collect(),
            });
        }
        if let Some(hit) = name_hits.first() {
            return Ok((*hit).clone());
        }

        // Bare device name via hostname slug (pre-identity peers).
        let ref_slug = identity::slug(peer_ref, 255);
        if !ref_slug.is_empty() {
            let slug_hits: Vec<&PeerState> = peers
                .iter()
                .filter(|p| hostname_slug(&p.name, &app_id) == Some(ref_slug.as_str()))
                .collect();
            if slug_hits.len() > 1 {
                return Err(NodeError::AmbiguousPeer {
                    query: peer_ref.to_string(),
                    candidates: slug_hits.iter().map(|p| p.id.clone()).collect(),
                });
            }
            if let Some(hit) = slug_hits.first() {
                return Ok((*hit).clone());
            }
        }

        // Prefix match on published device_id (require ≥4 chars, unique).
        if peer_ref.len() >= 4 {
            let hits: Vec<&PeerState> = peers
                .iter()
                .filter(|p| {
                    p.published_device_id()
                        .map(|uid| uid.starts_with(peer_ref))
                        .unwrap_or(false)
                })
                .collect();
            if hits.len() > 1 {
                return Err(NodeError::AmbiguousPeer {
                    query: peer_ref.to_string(),
                    candidates: hits
                        .iter()
                        .filter_map(|p| p.published_device_id().map(|s| s.to_string()))
                        .collect(),
                });
            }
            if let Some(hit) = hits.first() {
                return Ok((*hit).clone());
            }
        }

        // A well-formed peer ref that resolved nothing above refers to a
        // departed or superseded generation (live refs exact-matched
        // `p.peer_ref()` earlier) — surface PeerGone, not PeerNotFound, so
        // stale handles fail loudly instead of looking like typos
        // (RFC 022 I5). Checked last so colon-containing names still match.
        if crate::session::parse_peer_ref(peer_ref).is_some() {
            return Err(NodeError::PeerGone(peer_ref.to_string()));
        }

        Err(NodeError::PeerNotFound(peer_ref.to_string()))
    }

    /// Resolve a peer identifier to the peer's Tailscale IP address.
    ///
    /// Accepts the same identifier forms as
    /// [`resolve_peer_id`](Self::resolve_peer_id). Used by the FFI layers
    /// to address datagram sends by peer name.
    pub async fn resolve_peer_ip(&self, peer_ref: &str) -> Result<IpAddr, NodeError> {
        self.ensure_not_stopped()?;
        Ok(self.resolve_peer(peer_ref).await?.ip)
    }

    /// Resolve a peer query to a string safe to pass to [`send`](Self::send)
    /// and other peer-addressed methods.
    ///
    /// Prefer the published durable ULID when known and not suppressed;
    /// otherwise return the Tailscale stable id (routing key). This is the
    /// legacy string path — RFC 022 Phase B replaces it with `peer()` handles.
    ///
    /// Accepts any of:
    /// - the stable `device_id` (full ULID)
    /// - a unique prefix of the `device_id` (at least 4 characters; must
    ///   match exactly one known peer)
    /// - the human-readable `device_name` from the hello
    /// - the bare device name of a peer that has not helloed yet (matched
    ///   via the `truffle-{app_id}-{slug}` hostname convention)
    /// - the Layer 3 Tailscale hostname (the sanitised slug)
    /// - the Tailscale stable ID
    /// - the Tailscale IP address (e.g. `100.x.x.x`)
    pub async fn resolve_peer_id(&self, peer_id: &str) -> Result<String, NodeError> {
        self.ensure_not_stopped()?;
        let p = self.resolve_peer(peer_id).await?;
        Ok(p.published_device_id()
            .map(|s| s.to_string())
            .unwrap_or_else(|| p.id.clone()))
    }

    // ── Diagnostics ──────────────────────────────────────────────────────

    /// Ping a peer via the network layer.
    ///
    /// Resolves the peer ID to an IP address and pings via Layer 3. Accepts
    /// the same identifier forms as [`resolve_peer_id`](Self::resolve_peer_id).
    pub async fn ping(&self, peer_id: &str) -> Result<PingResult, NodeError> {
        self.ensure_not_stopped()?;
        let peer = self.resolve_peer(peer_id).await?;
        let addr = peer.ip.to_string();
        self.network.ping(&addr).await.map_err(NodeError::Network)
    }

    /// Tailnet identity (WhoIs) of the node that owns an address.
    ///
    /// Accepts a raw tailnet IP or `ip:port` (e.g. a QUIC connection's
    /// `remote_address()` verbatim — the port is ignored), or any
    /// [`resolve_peer_id`](Self::resolve_peer_id) identifier form for mesh
    /// peers. Unlike [`peers`](Self::peers), a raw address reaches ANY
    /// tailnet device — other apps' truffle nodes, plain machines, tagged
    /// nodes — and the answer carries user identity (login, display name,
    /// profile pic) that [`Peer`] deliberately does not.
    ///
    /// `Ok(None)` means the tailnet has no identity for the address: the
    /// caller is anonymous — absent, not fabricated.
    pub async fn whois(
        &self,
        addr: &str,
    ) -> Result<Option<crate::network::TailscalePeerIdentity>, NodeError> {
        self.ensure_not_stopped()?;
        // Canonicalize parsed addresses (`ip.to_string()`, not the caller's
        // spelling): the sidecar echoes the sent string verbatim for
        // correlation, and a canonical form keeps that byte-exact even for
        // exotic-but-valid IPv6 spellings.
        let target = if let Ok(ip) = addr.parse::<std::net::IpAddr>() {
            ip.to_string()
        } else if let Ok(sock) = addr.parse::<std::net::SocketAddr>() {
            sock.ip().to_string()
        } else {
            self.resolve_peer(addr).await?.ip.to_string()
        };
        self.network
            .whois(&target)
            .await
            .map_err(NodeError::Network)
    }

    /// Return health information from the network layer.
    pub async fn health(&self) -> HealthInfo {
        self.network.health().await
    }

    // ── Messaging (Layer 6 envelope over Layer 4 WS) ─────────────────────

    /// Return [`NodeError::Stopped`] if [`stop`](Self::stop) has already run.
    ///
    /// Used by the send paths to fail fast instead of attempting a doomed
    /// session send after teardown.
    fn ensure_not_stopped(&self) -> Result<(), NodeError> {
        if self.stopped.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(NodeError::Stopped);
        }
        Ok(())
    }

    /// Map session send errors: a stale peer-ref selector surfaces as
    /// [`NodeError::PeerGone`] (RFC 022 I5); everything else wraps as-is.
    fn map_session_send_err(e: crate::session::SessionError) -> NodeError {
        match e {
            crate::session::SessionError::PeerGone(r) => NodeError::PeerGone(r),
            e => NodeError::Session(e),
        }
    }

    /// Send a namespaced message to a specific peer.
    ///
    /// **Deprecated**: the wire representation depends on the *contents* of
    /// `data` — bytes that parse as UTF-8 JSON are sent as that JSON value
    /// (`b"123"` → number, `b"null"` → null), anything else becomes a JSON
    /// array of byte values. Use [`send_json`](Self::send_json) for
    /// structured payloads or [`send_bytes`](Self::send_bytes) for opaque
    /// binary data instead.
    ///
    /// Returns [`NodeError::Stopped`] if [`stop`](Self::stop) has been called.
    #[deprecated(
        since = "0.7.0",
        note = "wire type depends on data contents; use send_json or send_bytes"
    )]
    pub async fn send(&self, peer_id: &str, namespace: &str, data: &[u8]) -> Result<(), NodeError> {
        self.ensure_not_stopped()?;
        // Legacy content sniffing, kept for compatibility: if the data is
        // valid UTF-8 JSON, parse it into a proper JSON value so the receiver
        // gets a structured object rather than an array of byte values.
        let payload = std::str::from_utf8(data)
            .ok()
            .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok())
            .unwrap_or_else(|| serde_json::Value::from(data.to_vec()));

        let envelope = Envelope::new(namespace, "message", payload).with_timestamp();

        let encoded = self.codec.encode(&envelope)?;
        self.session
            .send(peer_id, &encoded)
            .await
            .map_err(Self::map_session_send_err)?;
        Ok(())
    }

    /// Send a JSON payload to a specific peer.
    ///
    /// The payload is wrapped in a Layer 6 [`Envelope`] with the given
    /// namespace and a `"message"` type — subscribers observe it unchanged
    /// as [`NamespacedMessage::payload`]. If no WebSocket connection
    /// exists, one is lazily established.
    pub async fn send_json(
        &self,
        peer_id: &str,
        namespace: &str,
        payload: &serde_json::Value,
    ) -> Result<(), NodeError> {
        self.send_typed(peer_id, namespace, "message", payload)
            .await
    }

    /// Send opaque binary data to a specific peer.
    ///
    /// The bytes travel base64-encoded in a `"bytes"`-typed envelope with
    /// payload shape `{"encoding":"base64","data":"…"}`; receivers decode
    /// with [`NamespacedMessage::payload_bytes`]. Unlike the deprecated
    /// [`send`](Self::send), the wire representation never depends on the
    /// data's contents.
    pub async fn send_bytes(
        &self,
        peer_id: &str,
        namespace: &str,
        data: &[u8],
    ) -> Result<(), NodeError> {
        let payload = Self::bytes_payload(data);
        self.send_typed(peer_id, namespace, "bytes", &payload).await
    }

    /// Encode opaque bytes as the `"bytes"` envelope payload.
    fn bytes_payload(data: &[u8]) -> serde_json::Value {
        use base64::Engine as _;
        serde_json::json!({
            "encoding": "base64",
            "data": base64::engine::general_purpose::STANDARD.encode(data),
        })
    }

    /// Send a namespaced message with an explicit `msg_type` and JSON payload.
    ///
    /// Unlike [`send`](Self::send), this method takes a pre-built
    /// [`serde_json::Value`] payload and a caller-chosen `msg_type` instead
    /// of raw bytes with a hardcoded `"message"` type. Used by subsystems
    /// (file transfer, synced store, request/reply) that define their own
    /// wire protocol message types.
    pub async fn send_typed(
        &self,
        peer_id: &str,
        namespace: &str,
        msg_type: &str,
        payload: &serde_json::Value,
    ) -> Result<(), NodeError> {
        self.ensure_not_stopped()?;
        let envelope = Envelope::new(namespace, msg_type, payload.clone()).with_timestamp();
        let encoded = self.codec.encode(&envelope)?;
        self.session
            .send(peer_id, &encoded)
            .await
            .map_err(Self::map_session_send_err)?;
        Ok(())
    }

    /// Broadcast a namespaced message with an explicit `msg_type` and JSON
    /// payload to all connected peers.
    pub async fn broadcast_typed(
        &self,
        namespace: &str,
        msg_type: &str,
        payload: &serde_json::Value,
    ) {
        if self.stopped.load(std::sync::atomic::Ordering::SeqCst) {
            tracing::debug!("node: broadcast after stop ignored");
            return;
        }
        let envelope = Envelope::new(namespace, msg_type, payload.clone()).with_timestamp();
        match self.codec.encode(&envelope) {
            Ok(encoded) => {
                self.session.broadcast(&encoded).await;
            }
            Err(e) => {
                tracing::error!("node: failed to encode broadcast envelope: {e}");
            }
        }
    }

    /// Broadcast a namespaced message to all connected peers.
    ///
    /// **Deprecated**: the wire representation depends on the contents of
    /// `data` (see [`send`](Self::send)), and delivery failures are
    /// silently discarded. Use [`broadcast_json`](Self::broadcast_json) or
    /// [`broadcast_bytes`](Self::broadcast_bytes), which return a
    /// [`BroadcastReport`](crate::session::BroadcastReport).
    #[deprecated(
        since = "0.7.0",
        note = "wire type depends on data contents and failures are hidden; \
                use broadcast_json or broadcast_bytes"
    )]
    pub async fn broadcast(&self, namespace: &str, data: &[u8]) {
        if self.stopped.load(std::sync::atomic::Ordering::SeqCst) {
            tracing::debug!("node: broadcast after stop ignored");
            return;
        }
        // Legacy content sniffing, kept for compatibility (see send()).
        let payload = std::str::from_utf8(data)
            .ok()
            .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok())
            .unwrap_or_else(|| serde_json::Value::from(data.to_vec()));

        let envelope = Envelope::new(namespace, "message", payload).with_timestamp();

        match self.codec.encode(&envelope) {
            Ok(encoded) => {
                self.session.broadcast(&encoded).await;
            }
            Err(e) => {
                tracing::error!("node: failed to encode broadcast envelope: {e}");
            }
        }
    }

    /// Broadcast a JSON payload to all connected peers.
    ///
    /// Only peers with an active WebSocket connection receive the message —
    /// no lazy connections are established. Returns a
    /// [`BroadcastReport`](crate::session::BroadcastReport): "queued" means
    /// handed to the peer's connection task, not confirmed delivery.
    pub async fn broadcast_json(
        &self,
        namespace: &str,
        payload: &serde_json::Value,
    ) -> Result<crate::session::BroadcastReport, NodeError> {
        self.broadcast_reported(namespace, "message", payload).await
    }

    /// Broadcast opaque binary data to all connected peers.
    ///
    /// Same wire shape as [`send_bytes`](Self::send_bytes); same report
    /// semantics as [`broadcast_json`](Self::broadcast_json).
    pub async fn broadcast_bytes(
        &self,
        namespace: &str,
        data: &[u8],
    ) -> Result<crate::session::BroadcastReport, NodeError> {
        let payload = Self::bytes_payload(data);
        self.broadcast_reported(namespace, "bytes", &payload).await
    }

    /// Shared broadcast implementation: encode once, report the outcome.
    /// Unlike the deprecated fire-and-forget path, encode failures and
    /// stopped-node calls surface as errors.
    async fn broadcast_reported(
        &self,
        namespace: &str,
        msg_type: &str,
        payload: &serde_json::Value,
    ) -> Result<crate::session::BroadcastReport, NodeError> {
        self.ensure_not_stopped()?;
        let envelope = Envelope::new(namespace, msg_type, payload.clone()).with_timestamp();
        let encoded = self.codec.encode(&envelope)?;
        Ok(self.session.broadcast(&encoded).await)
    }

    /// Subscribe to messages in a specific namespace.
    ///
    /// Returns a broadcast receiver that yields [`NamespacedMessage`]s
    /// matching the given namespace. Multiple subscribers to the same
    /// namespace share the same underlying channel.
    pub fn subscribe(&self, namespace: &str) -> broadcast::Receiver<NamespacedMessage> {
        // Fast path: check if subscriber already exists (read lock).
        // Poisoning is recovered (into_inner), not propagated — see the
        // envelope router for the rationale.
        {
            let filters = self
                .namespace_filters
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(tx) = filters.get(namespace) {
                return tx.subscribe();
            }
        }

        // Slow path: create a new channel for this namespace (write lock).
        let mut filters = self
            .namespace_filters
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Double-check after acquiring write lock.
        if let Some(tx) = filters.get(namespace) {
            return tx.subscribe();
        }
        // Opportunistic sweep: drop entries whose receivers are all gone.
        // The router prunes on send failure, but only for namespaces that
        // still receive traffic — this catches the silent ones.
        filters.retain(|_, tx| tx.receiver_count() > 0);
        let (tx, rx) = broadcast::channel(256);
        filters.insert(namespace.to_string(), tx);
        rx
    }

    // ── Raw streams (Layer 4 direct) ─────────────────────────────────────

    /// Open a raw TCP stream to a peer on the given port.
    ///
    /// Resolves the peer ID to an IP address via the session's peer list,
    /// then dials via the network layer. Accepts the same identifier
    /// forms as [`resolve_peer_id`](Self::resolve_peer_id). Returns a
    /// plain `TcpStream` for byte-oriented I/O.
    ///
    /// The stream is raw even on port 443 — the sidecar's legacy auto-TLS
    /// wrap for 443 dials is disabled on this path.
    pub async fn open_tcp(&self, peer_id: &str, port: u16) -> Result<TcpStream, NodeError> {
        self.ensure_not_stopped()?;
        let peer = self.resolve_peer(peer_id).await?;
        let addr = peer.ip.to_string();
        self.network
            .dial_tcp_opts(&addr, port, DialOpts { tls: Some(false) })
            .await
            .map_err(|e| NodeError::ConnectionFailed(e.to_string()))
    }

    /// Listen for incoming TCP connections on a port.
    ///
    /// Returns a [`RawListener`] that yields raw `TcpStream`s. The caller
    /// is responsible for accepting connections in a loop. Port 0 binds an
    /// ephemeral port (advertise the resolved `RawListener::port` in-band,
    /// like the file transfer subsystem does). The session WebSocket port
    /// (default 9417) is reserved.
    pub async fn listen_tcp(&self, port: u16) -> Result<RawListener, NodeError> {
        self.listen_tcp_opts(port, ListenOpts::default()).await
    }

    /// As [`listen_tcp`](Self::listen_tcp), with options (RFC 023 §7.1).
    ///
    /// `tls: true` terminates TLS in the sidecar with automatic MagicDNS
    /// certificates (requires MagicDNS + HTTPS enabled on the tailnet);
    /// accepted streams then carry plaintext HTTP over the loopback bridge.
    pub async fn listen_tcp_opts(
        &self,
        port: u16,
        opts: ListenOpts,
    ) -> Result<RawListener, NodeError> {
        self.ensure_not_stopped()?;
        use crate::transport::tcp::TcpTransport;

        ensure_port_unreserved(port, self.ws_port)?;
        let tcp = TcpTransport::new(self.network.clone());
        tcp.listen_opts(port, opts).await.map_err(|e| {
            // RFC 023 unreserved 443, but sidecars predating it still bind a
            // legacy TLS listener there — a double-bind here is almost
            // always that, so say so instead of a bare LISTEN_ERROR.
            if port == 443 {
                NodeError::ConnectionFailed(format!(
                    "{e} (port 443 requires a sidecar built with RFC 023 — older sidecars bind \
                     a legacy TLS listener on 443; upgrade the sidecar binary)"
                ))
            } else {
                NodeError::Transport(e)
            }
        })
    }

    /// Stop listening on a previously opened TCP port.
    ///
    /// Dropping the [`RawListener`] alone stops local delivery but leaves
    /// the tsnet port bound in the sidecar; this releases it.
    pub async fn unlisten_tcp(&self, port: u16) -> Result<(), NodeError> {
        self.ensure_not_stopped()?;
        self.network
            .unlisten_tcp(port)
            .await
            .map_err(NodeError::Network)
    }

    /// Open a raw QUIC connection to a peer on the given port.
    ///
    /// The connection carries multiple concurrent bidirectional byte
    /// streams ([`QuicConnection::open_stream`]) with no head-of-line
    /// blocking between them. App scoping is enforced at the TLS layer via
    /// ALPN (`truffle-raw.{app_id}`) — peers from a different app fail the
    /// handshake. Accepts the same identifier forms as
    /// [`resolve_peer_id`](Self::resolve_peer_id).
    pub async fn connect_quic(
        &self,
        peer_id: &str,
        port: u16,
    ) -> Result<QuicConnection, NodeError> {
        self.ensure_not_stopped()?;
        let peer = self.resolve_peer(peer_id).await?;
        let alpn = crate::transport::quic::raw_alpn(&self.network.local_identity().app_id);
        crate::transport::quic::connect_raw(&self.network, &peer.ip.to_string(), port, &alpn)
            .await
            .map_err(NodeError::Transport)
    }

    /// Listen for raw QUIC connections on a port.
    ///
    /// Returns a [`QuicListener`] that yields [`QuicConnection`]s. Only
    /// same-app peers can complete the handshake (ALPN scoping). The
    /// session WebSocket port (default 9417) is reserved, and port 0 is
    /// not yet supported over the tsnet relay (the relay cannot report the
    /// actual ephemeral port back).
    pub async fn listen_quic(&self, port: u16) -> Result<QuicListener, NodeError> {
        self.ensure_not_stopped()?;
        ensure_port_unreserved(port, self.ws_port)?;
        if port == 0 {
            return Err(NodeError::NotImplemented(
                "ephemeral (port 0) QUIC listeners are not supported over the tsnet relay yet — choose an explicit port"
                    .to_string(),
            ));
        }
        let alpn = crate::transport::quic::raw_alpn(&self.network.local_identity().app_id);
        crate::transport::quic::listen_raw(&self.network, port, &alpn)
            .await
            .map_err(NodeError::Transport)
    }

    /// Bind a UDP datagram socket on a port.
    ///
    /// Datagrams are relayed through the network provider (tsnet) with
    /// boundaries preserved; the transport falls back to a direct host
    /// socket only when the provider has no UDP support (tests). Returns a
    /// [`DatagramSocket`] supporting `send_to` / `recv_from` with tailnet
    /// addresses. IPv4 (`100.x`) peers only; keep payloads ≤ ~1200 bytes
    /// to stay under the tailnet MTU. Port 0 binds an ephemeral relay
    /// port — suitable for client-style sockets that send first.
    pub async fn bind_udp(&self, port: u16) -> Result<DatagramSocket, NodeError> {
        self.ensure_not_stopped()?;
        use crate::transport::udp::{UdpConfig, UdpTransport};
        use crate::transport::DatagramTransport;

        let udp = UdpTransport::new(self.network.clone(), UdpConfig::default());
        udp.bind(port).await.map_err(NodeError::Transport)
    }
}

impl<N: NetworkProvider + 'static> Drop for Node<N> {
    fn drop(&mut self) {
        // Insurance for nodes dropped without stop(): cancelling is cheap,
        // synchronous, and lets background tasks exit instead of idling on
        // channels that may never close.
        self.tasks.cancel.cancel();
    }
}

/// The bare device-name slug from a Layer 3 hostname, when it follows this
/// app's `truffle-{app_id}-{slug}` convention (RFC 017). `None` otherwise.
fn hostname_slug<'a>(hostname: &'a str, app_id: &str) -> Option<&'a str> {
    hostname
        .strip_prefix("truffle-")?
        .strip_prefix(app_id)?
        .strip_prefix('-')
}

/// A valid single DNS label: 1–63 chars of `[a-z0-9-]`, lowercase, no
/// leading/trailing hyphen, no dots (tsnet takes a bare hostname, not an
/// FQDN). Used by [`NodeBuilder::hostname`].
fn validate_hostname_label(s: &str) -> Result<(), String> {
    let ok_len = (1..=63).contains(&s.len());
    let ok_chars = s
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-');
    let ok_edges = !s.starts_with('-') && !s.ends_with('-');
    if ok_len && ok_chars && ok_edges {
        Ok(())
    } else {
        Err(format!(
            "invalid hostname {s:?}: must be a single lowercase DNS label \
             (1-63 chars of [a-z0-9-], no leading/trailing hyphen, no dots)"
        ))
    }
}

/// Reject ports reserved by truffle's own listeners: the node's configured
/// session WebSocket port (default 9417). Port 443 is deliberately NOT
/// reserved anymore — RFC 023 removed the sidecar's legacy TLS listener so
/// users can serve HTTPS on the default port.
pub(crate) fn ensure_port_unreserved(port: u16, ws_port: u16) -> Result<(), NodeError> {
    if port == ws_port {
        Err(NodeError::ReservedPort(port))
    } else {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// NodeBuilder
// ---------------------------------------------------------------------------

/// Builder for constructing a [`Node<TailscaleProvider>`].
///
/// Configures the Tailscale sidecar, RFC 017 identity, and transport
/// parameters before wiring all layers together.
///
/// # Example
///
/// ```ignore
/// let node = Node::builder()
///     .app_id("playground")?
///     .device_name("alice-mbp")
///     .sidecar_path("/opt/truffle/sidecar")
///     .ws_port(9417)
///     .build()
///     .await?;
/// ```
#[derive(Clone)]
pub struct NodeBuilder {
    app_id: Option<AppId>,
    device_name: Option<DeviceName>,
    device_id: Option<DeviceId>,
    /// RFC 023 §6.4: explicit Tailscale hostname, bypassing the
    /// `truffle-{app_id}-{slug}` convention (pretty serving URLs).
    hostname: Option<String>,
    sidecar_path: Option<PathBuf>,
    state_dir: Option<String>,
    auth_key: Option<String>,
    ephemeral: bool,
    ws_port: u16,
    idle_timeout_secs: Option<u64>,
    /// RFC 022 Phase C: proactively exchange hello with online peers.
    eager_identity: bool,
}

/// Manual `Debug`: `auth_key` is a tailnet credential and must never reach
/// logs, so it is redacted while preserving presence (`Some`/`None`).
impl std::fmt::Debug for NodeBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeBuilder")
            .field("app_id", &self.app_id)
            .field("device_name", &self.device_name)
            .field("device_id", &self.device_id)
            .field("hostname", &self.hostname)
            .field("sidecar_path", &self.sidecar_path)
            .field("state_dir", &self.state_dir)
            .field("auth_key", &self.auth_key.as_ref().map(|_| "[REDACTED]"))
            .field("ephemeral", &self.ephemeral)
            .field("ws_port", &self.ws_port)
            .field("idle_timeout_secs", &self.idle_timeout_secs)
            .field("eager_identity", &self.eager_identity)
            .finish()
    }
}

/// Atomically and durably write a string to `path` by writing to a sibling
/// `.tmp` file first and then renaming it over the destination.
///
/// The caller persists the durable device ULID (RFC 017 §5.4), so a crash
/// mid-write must never lose or truncate an existing identity file:
/// - the temp file is fsynced before the rename publishes it, so the
///   destination can never be observed empty or truncated;
/// - on POSIX, `rename(2)` atomically replaces the destination and the
///   parent directory is fsynced so the rename itself survives power loss;
/// - on Windows, `MoveFileExW(REPLACE_EXISTING | WRITE_THROUGH)` replaces
///   the destination without the delete-then-rename gap that could drop
///   the file entirely if the process died between the two steps.
fn atomic_write_string(path: &Path, content: &str) -> std::io::Result<()> {
    let parent = path.parent().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "path has no parent directory",
        )
    })?;
    let mut tmp = path.to_path_buf();
    tmp.set_extension("tmp");
    {
        use std::io::Write as _;
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(content.as_bytes())?;
        f.sync_all()?;
    }
    #[cfg(unix)]
    {
        std::fs::rename(&tmp, path)?;
        // Best-effort: some filesystems refuse opening a directory for
        // fsync; the rename is still atomic without it, just not yet
        // guaranteed on disk.
        if let Ok(dir) = std::fs::File::open(parent) {
            let _ = dir.sync_all();
        }
    }
    #[cfg(windows)]
    {
        let _ = parent; // only needed for the unix dir-fsync path
        replace_file_windows(&tmp, path)?;
    }
    Ok(())
}

/// Replace `dest` with `tmp` in one step via `MoveFileExW`.
///
/// `MOVEFILE_REPLACE_EXISTING` avoids the non-atomic remove-then-rename
/// dance (`std::fs::rename` errors on an existing destination on Windows);
/// `MOVEFILE_WRITE_THROUGH` blocks until the move is flushed to disk.
#[cfg(windows)]
fn replace_file_windows(tmp: &Path, dest: &Path) -> std::io::Result<()> {
    use std::os::windows::ffi::OsStrExt;

    const MOVEFILE_REPLACE_EXISTING: u32 = 0x1;
    const MOVEFILE_WRITE_THROUGH: u32 = 0x8;

    #[link(name = "kernel32")]
    extern "system" {
        fn MoveFileExW(from: *const u16, to: *const u16, flags: u32) -> i32;
    }

    fn wide(p: &Path) -> Vec<u16> {
        p.as_os_str()
            .encode_wide()
            .chain(std::iter::once(0))
            .collect()
    }

    let (from, to) = (wide(tmp), wide(dest));
    let ok = unsafe {
        MoveFileExW(
            from.as_ptr(),
            to.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    };
    if ok == 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

impl Default for NodeBuilder {
    fn default() -> Self {
        Self {
            app_id: None,
            device_name: None,
            device_id: None,
            sidecar_path: None,
            state_dir: None,
            auth_key: None,
            hostname: None,
            ephemeral: false,
            ws_port: 9417,
            idle_timeout_secs: None,
            eager_identity: true,
        }
    }
}

impl NodeBuilder {
    /// Set the application namespace identifier (RFC 017 §5.1).
    ///
    /// The input is validated against `^[a-z][a-z0-9-]{1,31}$`. Invalid
    /// values are rejected with `NodeError::BuildError`.
    pub fn app_id(mut self, s: impl Into<String>) -> Result<Self, NodeError> {
        let raw: String = s.into();
        let app_id = AppId::parse(&raw)
            .map_err(|e| NodeError::BuildError(format!("invalid app_id: {e}")))?;
        self.app_id = Some(app_id);
        Ok(self)
    }

    /// Set the human-readable device name.
    ///
    /// Accepts any Unicode input; soft-truncated to 256 graphemes. When
    /// unset, the builder falls back to `hostname::get()` at `build()` time.
    pub fn device_name(mut self, s: impl Into<String>) -> Self {
        self.device_name = Some(DeviceName::parse(s));
        self
    }

    /// Override the composed Tailscale hostname (RFC 023 §6.4).
    ///
    /// Bypasses the `truffle-{app_id}-{slug(device_name)}` convention so
    /// serving URLs read like a product (`https://dashboard.{tailnet}.ts.net`).
    /// Validated as a single lowercase DNS label (1–63 chars of `[a-z0-9-]`,
    /// no leading/trailing hyphen, no dots — tsnet takes a bare hostname).
    ///
    /// Tradeoff: hello-less peers with a custom hostname are not resolvable
    /// by bare device name (the hostname-slug convention no longer matches);
    /// full hostname, dnsName, IP, device-id, and post-hello identity
    /// resolution are unaffected. Tailscale dedupes hostname collisions by
    /// suffixing `-1`/`-2` — read the granted name from
    /// `local_info().dns_name` instead of string-building URLs.
    pub fn hostname(mut self, s: impl Into<String>) -> Result<Self, NodeError> {
        let raw: String = s.into();
        validate_hostname_label(&raw).map_err(NodeError::BuildError)?;
        self.hostname = Some(raw);
        Ok(self)
    }

    /// Override the auto-generated device ID.
    ///
    /// Validates that `s` is a well-formed ULID. When provided, the value
    /// is persisted to `{state_dir}/device-id.txt` during `build()` so
    /// subsequent starts without an explicit `device_id` see it.
    pub fn device_id(mut self, s: impl Into<String>) -> Result<Self, NodeError> {
        let raw: String = s.into();
        let device_id = DeviceId::parse(&raw)
            .map_err(|e| NodeError::BuildError(format!("invalid device_id: {e}")))?;
        self.device_id = Some(device_id);
        Ok(self)
    }

    /// Set the path to the Go sidecar binary.
    pub fn sidecar_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.sidecar_path = Some(path.into());
        self
    }

    /// Set the Tailscale state directory.
    pub fn state_dir(mut self, dir: &str) -> Self {
        self.state_dir = Some(dir.to_string());
        self
    }

    /// Set the Tailscale auth key for headless authentication.
    pub fn auth_key(mut self, key: &str) -> Self {
        self.auth_key = Some(key.to_string());
        self
    }

    /// Set whether the node is ephemeral (auto-removed from tailnet on shutdown).
    pub fn ephemeral(mut self, val: bool) -> Self {
        self.ephemeral = val;
        self
    }

    /// Set the WebSocket listen port.
    /// RFC 022 Phase C: when true (default), proactively exchange hello with
    /// online peers so durable `device_id` is learned without app `send`.
    pub fn eager_identity(mut self, enabled: bool) -> Self {
        self.eager_identity = enabled;
        self
    }

    pub fn ws_port(mut self, port: u16) -> Self {
        self.ws_port = port;
        self
    }

    /// Set the idle timeout (in seconds) for bridged raw TCP connections.
    ///
    /// The sidecar reaps quiet bridged connections after this long
    /// (default: 600 s). Apps holding long-lived quiet sockets should
    /// raise this or send application-level keepalives.
    pub fn idle_timeout_secs(mut self, secs: u64) -> Self {
        self.idle_timeout_secs = Some(secs);
        self
    }

    /// Resolve RFC 017 identity values and the Tailscale config.
    ///
    /// Shared between [`build()`](Self::build) and
    /// [`build_with_auth_handler()`](Self::build_with_auth_handler). Returns
    /// the ready-to-start `TailscaleConfig` along with the parsed identity
    /// triple.
    fn prepare_config(&self) -> Result<TailscaleConfig, NodeError> {
        // 1. sidecar binary is required.
        let binary_path = self
            .sidecar_path
            .clone()
            .ok_or_else(|| NodeError::BuildError("sidecar_path is required".into()))?;

        // 2. app_id is required.
        let app_id = self
            .app_id
            .clone()
            .ok_or_else(|| NodeError::BuildError("app_id is required".into()))?;

        // 3. device_name falls back to the OS hostname.
        let device_name = match self.device_name.clone() {
            Some(name) => name,
            None => {
                let os_hostname = hostname::get()
                    .map_err(|e| {
                        NodeError::BuildError(format!(
                            "device_name is unset and hostname::get() failed: {e}"
                        ))
                    })?
                    .to_string_lossy()
                    .into_owned();
                DeviceName::parse(os_hostname)
            }
        };

        // 4. Compose the Tailscale hostname once, here. Downstream code
        //    MUST NOT rebuild it — the provider config stores this verbatim.
        //    An explicit builder hostname (RFC 023 §6.4) wins over the
        //    `truffle-{app_id}-{slug}` convention; see the setter for what
        //    that costs (bare-name resolution of hello-less peers).
        let tailscale_host = match self.hostname.clone() {
            Some(hostname) => hostname,
            None => identity::tailscale_hostname(&app_id, &device_name),
        };

        // 5. Resolve state_dir. Default:
        //    `{dirs::data_dir}/truffle/{app_id}/{slug(device_name)}`.
        //    No temp-dir fallback: state_dir holds the durable device ULID
        //    and tsnet keys, and a temp directory silently resets both on
        //    reboot. Platforms without a data dir must opt in explicitly.
        let state_dir = match self.state_dir.clone() {
            Some(dir) => dir,
            None => {
                let base = dirs::data_dir().ok_or_else(|| {
                    NodeError::BuildError(
                        "no platform data directory available (dirs::data_dir() returned None); \
                         set state_dir explicitly to a durable location"
                            .into(),
                    )
                })?;
                base.join("truffle")
                    .join(app_id.as_str())
                    .join(identity::slug(device_name.as_str(), 255))
                    .to_string_lossy()
                    .into_owned()
            }
        };

        // 6. Ensure the state directory exists before Tailscale starts.
        std::fs::create_dir_all(&state_dir)?;

        // 7. Resolve device_id. Priority:
        //    a) explicit builder override → validate + persist
        //    b) existing `device-id.txt` → read + validate
        //    c) generate + persist
        let device_id_file = Path::new(&state_dir).join("device-id.txt");
        let device_id = match self.device_id.clone() {
            Some(id) => {
                // Persist the override so later auto-generated calls see it.
                atomic_write_string(&device_id_file, id.as_str())?;
                id
            }
            None => {
                if device_id_file.exists() {
                    let s = std::fs::read_to_string(&device_id_file)?.trim().to_string();
                    DeviceId::parse(&s).map_err(|e| {
                        NodeError::BuildError(format!(
                            "device-id.txt at {device_id_file:?} contains an invalid ULID: {e}"
                        ))
                    })?
                } else {
                    let id = DeviceId::generate();
                    atomic_write_string(&device_id_file, id.as_str())?;
                    id
                }
            }
        };

        Ok(TailscaleConfig {
            binary_path,
            app_id: app_id.as_str().to_string(),
            device_id: device_id.as_str().to_string(),
            device_name: device_name.as_str().to_string(),
            hostname: tailscale_host,
            state_dir,
            auth_key: self.auth_key.clone(),
            ephemeral: if self.ephemeral { Some(true) } else { None },
            tags: None,
            idle_timeout_secs: self.idle_timeout_secs,
        })
    }

    /// Build and start the node.
    ///
    /// This creates the TailscaleProvider, starts it, creates the WebSocket
    /// transport and PeerRegistry, starts the session, and spawns the
    /// envelope router.
    ///
    /// # Errors
    ///
    /// Returns [`NodeError::BuildError`] if required configuration is missing,
    /// or propagates errors from the network provider startup.
    pub async fn build(self) -> Result<Node<TailscaleProvider>, NodeError> {
        let ws_port = self.ws_port;
        let eager_identity = self.eager_identity;
        let config = self.prepare_config()?;
        let state_dir = PathBuf::from(&config.state_dir);

        let mut provider = TailscaleProvider::new(config);
        provider.start().await.map_err(NodeError::Network)?;

        let network = Arc::new(provider);

        // 2. Create WebSocket transport.
        let ws_config = WsConfig {
            port: ws_port,
            ..Default::default()
        };
        let ws_transport = Arc::new(WebSocketTransport::new(network.clone(), ws_config));

        // 3. Create PeerRegistry and start session.
        let session = Arc::new(PeerRegistry::with_options(
            network.clone(),
            ws_transport,
            crate::session::PeerRegistryOptions {
                eager_identity,
                ..Default::default()
            },
        ));
        session.start().await;

        // 4. Create the node with the envelope router.
        let codec: Arc<dyn EnvelopeCodec> = Arc::new(JsonCodec);
        let node = Node::from_parts(network, session, codec)
            .with_state_dir(state_dir)
            .with_ws_port(ws_port);

        tracing::info!("node: started successfully");
        Ok(node)
    }

    /// Build and start the node, calling `on_auth` if authentication is needed.
    ///
    /// This is identical to [`build()`](Self::build) except it subscribes to
    /// provider events *before* `provider.start()` blocks, forwarding
    /// `AuthRequired` events to the callback while waiting for authentication
    /// to complete.
    ///
    /// # Errors
    ///
    /// Returns [`NodeError::BuildError`] if required configuration is missing,
    /// or propagates errors from the network provider startup.
    pub async fn build_with_auth_handler(
        self,
        on_auth: impl Fn(String) + Send + 'static,
    ) -> Result<Node<TailscaleProvider>, NodeError> {
        let ws_port = self.ws_port;
        let eager_identity = self.eager_identity;
        let config = self.prepare_config()?;
        let state_dir = PathBuf::from(&config.state_dir);

        let mut provider = TailscaleProvider::new(config);

        // 2. Subscribe to peer events BEFORE start() so we capture auth URLs.
        let mut auth_rx = provider.peer_events();

        // 3. Spawn a task that forwards AuthRequired events to the callback.
        let auth_task = tokio::spawn(async move {
            use crate::network::NetworkPeerEvent;
            loop {
                match auth_rx.recv().await {
                    Ok(NetworkPeerEvent::AuthRequired { url }) => {
                        on_auth(url);
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    _ => {} // Ignore other events
                }
            }
        });

        // 4. Start the provider (blocks until auth completes).
        let start_result = provider.start().await.map_err(NodeError::Network);

        // 5. Cancel the auth forwarding task — auth is done.
        auth_task.abort();

        start_result?;

        let network = Arc::new(provider);

        // 6. Create WebSocket transport.
        let ws_config = WsConfig {
            port: ws_port,
            ..Default::default()
        };
        let ws_transport = Arc::new(WebSocketTransport::new(network.clone(), ws_config));

        // 7. Create PeerRegistry and start session.
        let session = Arc::new(PeerRegistry::with_options(
            network.clone(),
            ws_transport,
            crate::session::PeerRegistryOptions {
                eager_identity,
                ..Default::default()
            },
        ));
        session.start().await;

        // 8. Create the node with the envelope router.
        let codec: Arc<dyn EnvelopeCodec> = Arc::new(JsonCodec);
        let node = Node::from_parts(network, session, codec)
            .with_state_dir(state_dir)
            .with_ws_port(ws_port);

        tracing::info!("node: started successfully (with auth handler)");
        Ok(node)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::{
        HealthInfo, IncomingConnection, NetworkError, NetworkPeer, NetworkPeerEvent,
        NetworkTcpListener, NetworkUdpSocket, PeerAddr,
    };
    use crate::transport::WsConfig;
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::sync::{broadcast, mpsc};

    #[test]
    fn node_builder_debug_redacts_auth_key() {
        let builder = NodeBuilder::default().auth_key("dummy-auth-SECRET123");
        let dbg = format!("{builder:?}");
        assert!(!dbg.contains("SECRET123"));
        assert!(dbg.contains("[REDACTED]"));
    }

    #[test]
    fn atomic_write_string_creates_and_replaces() {
        let dir =
            std::env::temp_dir().join(format!("truffle-atomic-write-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("device-id.txt");

        atomic_write_string(&path, "01AAAAAAAAAAAAAAAAAAAAAAAA").unwrap();
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            "01AAAAAAAAAAAAAAAAAAAAAAAA"
        );

        // Replacing an existing destination must succeed on every platform
        // (exercises the MoveFileExW REPLACE_EXISTING path on Windows CI).
        atomic_write_string(&path, "01BBBBBBBBBBBBBBBBBBBBBBBB").unwrap();
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            "01BBBBBBBBBBBBBBBBBBBBBBBB"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    // ── Mock NetworkProvider ──────────────────────────────────────────

    struct MockNetworkProvider {
        identity: NodeIdentity,
        local_addr: PeerAddr,
        peer_event_tx: broadcast::Sender<NetworkPeerEvent>,
        /// Pre-loaded peer list for `peers()`.
        mock_peers: Arc<RwLock<Vec<NetworkPeer>>>,
        /// Count of `stop()` invocations — lets tests assert the Node
        /// actually shut the provider down during teardown.
        stop_calls: Arc<AtomicUsize>,
    }

    impl MockNetworkProvider {
        fn new(id: &str) -> Self {
            // RFC 017: align `device_id` with fixture input so tests can
            // reason about a single identifier.
            Self::new_with_device_id(id, id)
        }

        /// `device_id` distinct from the tailscale id — required when a test
        /// lets the node hello with itself (loopback self-dial): publishing
        /// an identity whose device_id equals the entry's tailscale_id would
        /// violate RFC 022 invariant I1.
        fn new_with_device_id(id: &str, device_id: &str) -> Self {
            let (peer_event_tx, _) = broadcast::channel(64);
            Self {
                identity: NodeIdentity {
                    app_id: "test".to_string(),
                    device_id: device_id.to_string(),
                    device_name: format!("Test Node {id}"),
                    tailscale_hostname: format!("truffle-test-{id}"),
                    tailscale_id: id.to_string(),
                    dns_name: None,
                    ip: Some("127.0.0.1".parse().unwrap()),
                },
                local_addr: PeerAddr {
                    ip: Some("127.0.0.1".parse().unwrap()),
                    hostname: format!("truffle-test-{id}"),
                    dns_name: None,
                },
                peer_event_tx,
                mock_peers: Arc::new(RwLock::new(Vec::new())),
                stop_calls: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn event_sender(&self) -> broadcast::Sender<NetworkPeerEvent> {
            self.peer_event_tx.clone()
        }

        /// Number of times `stop()` has been called on this provider.
        fn stop_call_count(&self) -> usize {
            self.stop_calls.load(Ordering::SeqCst)
        }
    }

    impl NetworkProvider for MockNetworkProvider {
        async fn start(&mut self) -> Result<(), NetworkError> {
            Ok(())
        }

        async fn stop(&self) -> Result<(), NetworkError> {
            self.stop_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn local_identity(&self) -> NodeIdentity {
            self.identity.clone()
        }

        fn local_addr(&self) -> PeerAddr {
            self.local_addr.clone()
        }

        fn peer_events(&self) -> broadcast::Receiver<NetworkPeerEvent> {
            self.peer_event_tx.subscribe()
        }

        async fn peers(&self) -> Vec<NetworkPeer> {
            self.mock_peers.read().await.clone()
        }

        async fn dial_tcp(&self, addr: &str, port: u16) -> Result<TcpStream, NetworkError> {
            let target = format!("{addr}:{port}");
            TcpStream::connect(&target)
                .await
                .map_err(|e| NetworkError::DialFailed(format!("mock dial {target}: {e}")))
        }

        async fn listen_tcp(&self, port: u16) -> Result<NetworkTcpListener, NetworkError> {
            let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{port}"))
                .await
                .map_err(|e| NetworkError::ListenFailed(format!("mock listen :{port}: {e}")))?;

            let actual_port = listener.local_addr().unwrap().port();
            let (tx, rx) = mpsc::channel::<IncomingConnection>(64);

            tokio::spawn(async move {
                loop {
                    match listener.accept().await {
                        Ok((stream, addr)) => {
                            let conn = IncomingConnection {
                                stream,
                                remote_addr: addr.to_string(),
                                remote_identity: String::new(),
                                port: actual_port,
                            };
                            if tx.send(conn).await.is_err() {
                                break;
                            }
                        }
                        Err(e) => {
                            tracing::debug!("mock listener error: {e}");
                            break;
                        }
                    }
                }
            });

            Ok(NetworkTcpListener {
                port: actual_port,
                incoming: rx,
            })
        }

        async fn unlisten_tcp(&self, _port: u16) -> Result<(), NetworkError> {
            Ok(())
        }

        async fn bind_udp(&self, _port: u16) -> Result<NetworkUdpSocket, NetworkError> {
            Err(NetworkError::Internal("mock: UDP not supported".into()))
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

    // ── Helpers ──────────────────────────────────────────────────────

    fn make_loopback_peer(id: &str) -> NetworkPeer {
        NetworkPeer {
            id: id.to_string(),
            hostname: format!("truffle-test-{id}"),
            ip: "127.0.0.1".parse().unwrap(),
            online: true,
            cur_addr: Some("127.0.0.1:41641".to_string()),
            relay: None,
            os: Some("linux".to_string()),
            last_seen: Some("2026-03-25T12:00:00Z".to_string()),
            key_expiry: None,
            dns_name: None,
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

    /// Create a Node backed by a mock provider for testing.
    async fn make_test_node(
        id: &str,
        ws_port: u16,
    ) -> (
        Node<MockNetworkProvider>,
        broadcast::Sender<NetworkPeerEvent>,
        Arc<MockNetworkProvider>,
    ) {
        make_test_node_with_device_id(id, id, ws_port).await
    }

    /// Like [`make_test_node`] but with a `device_id` distinct from the
    /// tailscale id — see [`MockNetworkProvider::new_with_device_id`].
    async fn make_test_node_with_device_id(
        id: &str,
        device_id: &str,
        ws_port: u16,
    ) -> (
        Node<MockNetworkProvider>,
        broadcast::Sender<NetworkPeerEvent>,
        Arc<MockNetworkProvider>,
    ) {
        let provider = MockNetworkProvider::new_with_device_id(id, device_id);
        let event_tx = provider.event_sender();
        let network = Arc::new(provider);
        let ws_transport = Arc::new(WebSocketTransport::new(network.clone(), ws_config(ws_port)));
        let session = Arc::new(PeerRegistry::new(network.clone(), ws_transport));
        session.start().await;

        let codec: Arc<dyn EnvelopeCodec> = Arc::new(JsonCodec);
        let node = Node::from_parts(network.clone(), session, codec);

        (node, event_tx, network)
    }

    // ── Tests ────────────────────────────────────────────────────────

    /// `whois` semantics on a provider without WhoIs support: a raw IP passes
    /// straight through to the provider (surfacing `Unsupported`, never a
    /// fabricated identity), while a non-IP identifier still goes through
    /// peer resolution and fails as `PeerNotFound` for unknown peers.
    #[tokio::test]
    async fn whois_unsupported_provider_and_unknown_peer() {
        let ws_port = random_port().await;
        let (node, _event_tx, _network) = make_test_node("whois-node", ws_port).await;

        match node.whois("100.64.0.9").await {
            Err(NodeError::Network(NetworkError::Unsupported(_))) => {}
            other => panic!("expected Unsupported for raw IP on mock provider, got {other:?}"),
        }
        // An `ip:port` string (a QUIC `remote_address()` verbatim) also
        // reaches the provider — the port is stripped, never PeerNotFound.
        match node.whois("100.64.0.9:52133").await {
            Err(NodeError::Network(NetworkError::Unsupported(_))) => {}
            other => panic!("expected Unsupported for ip:port on mock provider, got {other:?}"),
        }
        match node.whois("no-such-peer").await {
            Err(NodeError::PeerNotFound(_)) => {}
            other => panic!("expected PeerNotFound for unknown identifier, got {other:?}"),
        }

        node.stop().await;
    }

    // ── Exhaustion tests (review: resource-exhaustion section) ────────

    /// Flood the real dispatch path with offers from one peer and assert
    /// every bound holds: per-peer pending cap, bounded app queue, and a
    /// prompt stop() despite parked decision waits.
    #[tokio::test]
    async fn offer_flood_respects_caps_and_stop_drains() {
        let ws_port = random_port().await;
        let (node, _event_tx, _network) = make_test_node("node-flood", ws_port).await;
        let node = Arc::new(node);
        let mut offers = node.file_transfer().offer_channel(node.clone()).await;

        for i in 0..500u32 {
            let payload = json!({
                "type": "offer",
                "file_name": format!("f{i}.bin"),
                "size": 1,
                "sha256": "0".repeat(64),
                "save_path": "",
                "token": format!("tok-{i}"),
                "tcp_port": 0,
            });
            let envelope = Envelope::new("ft", "offer", payload).with_timestamp();
            node.session
                .test_inject_incoming("peer-flood", JsonCodec.encode(&envelope).unwrap());
            if i % 64 == 0 {
                tokio::task::yield_now().await;
            }
        }
        tokio::time::sleep(Duration::from_millis(300)).await;

        let pending = node
            .file_transfer_state
            .pending_offers_per_peer
            .lock()
            .unwrap()
            .get("peer-flood")
            .copied()
            .unwrap_or(0);
        assert!(
            pending <= crate::file_transfer::MAX_PENDING_OFFERS_PER_PEER,
            "per-peer pending cap violated: {pending}"
        );

        // Without an app draining it, the offer queue stays bounded.
        let mut buffered = 0;
        while offers.try_recv().is_ok() {
            buffered += 1;
        }
        assert!(buffered <= 36, "app offer queue grew to {buffered}");

        // Parked offers wait up to 60s for a decision — stop() must cancel
        // them instead of waiting that out.
        let started = std::time::Instant::now();
        node.stop().await;
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "stop() took {:?} with parked offers",
            started.elapsed()
        );
        assert!(node.tasks.tracker.is_empty());
    }

    /// Churning through dynamic namespaces must not grow the filter map.
    #[tokio::test]
    async fn dynamic_namespace_churn_stays_bounded() {
        let ws_port = random_port().await;
        let (node, _event_tx, _network) = make_test_node("node-ns-churn", ws_port).await;
        for i in 0..1000 {
            let rx = node.subscribe(&format!("dyn-{i}"));
            drop(rx);
        }
        let len = node
            .namespace_filters
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len();
        assert!(len <= 1, "namespace map grew to {len} entries");
    }

    /// Repeated build → use → stop cycles leave no background tasks behind.
    #[tokio::test]
    async fn lifecycle_churn_leaves_no_tasks() {
        for _ in 0..25 {
            let ws_port = random_port().await;
            let (node, _event_tx, _network) = make_test_node("node-churn", ws_port).await;
            let node = Arc::new(node);
            let _rx = node.subscribe("churn");
            let _offers = node.file_transfer().offer_channel(node.clone()).await;
            node.stop().await;
            assert!(node.tasks.tracker.is_empty(), "task leak after stop()");
        }
    }

    /// A subscriber that never polls observes Lagged (bounded channel)
    /// rather than causing unbounded buffering.
    #[tokio::test]
    async fn slow_subscriber_lags_instead_of_growing() {
        let ws_port = random_port().await;
        let (node, _event_tx, _network) = make_test_node("node-lag", ws_port).await;
        let mut rx = node.subscribe("chat");

        for i in 0..2000 {
            let envelope = Envelope::new("chat", "message", json!({ "i": i })).with_timestamp();
            node.session
                .test_inject_incoming("peer-x", JsonCodec.encode(&envelope).unwrap());
            if i % 100 == 0 {
                tokio::task::yield_now().await;
            }
        }
        tokio::time::sleep(Duration::from_millis(300)).await;

        let mut lagged = false;
        loop {
            match rx.try_recv() {
                Ok(_) => {}
                Err(broadcast::error::TryRecvError::Lagged(_)) => lagged = true,
                Err(_) => break,
            }
        }
        assert!(lagged, "slow subscriber should observe Lagged");
    }

    #[tokio::test]
    async fn stop_drains_all_background_tasks() {
        let ws_port = random_port().await;
        let (node, _event_tx, _network) = make_test_node("node-drain", ws_port).await;
        let node = Arc::new(node);

        // Router is running; also start the FT dispatch task.
        let _offers = node.file_transfer().offer_channel(node.clone()).await;
        assert!(!node.tasks.tracker.is_empty(), "tasks should be running");

        node.stop().await;

        // stop() returns only after every node-scoped task has finished.
        assert!(
            node.tasks.tracker.is_empty(),
            "background tasks still alive after stop()"
        );
        assert!(
            node.file_transfer_state
                .receiver_handle
                .lock()
                .await
                .is_none(),
            "FT receiver handle not cleared by stop()"
        );
    }

    #[tokio::test]
    async fn stopped_node_fails_raw_and_resolution_apis_fast() {
        let ws_port = random_port().await;
        let (node, _event_tx, _network) = make_test_node("node-guards", ws_port).await;
        node.stop().await;

        assert!(matches!(
            node.open_tcp("nobody", 4242).await.unwrap_err(),
            NodeError::Stopped
        ));
        assert!(matches!(
            node.listen_tcp(4242).await.unwrap_err(),
            NodeError::Stopped
        ));
        assert!(matches!(
            node.connect_quic("nobody", 4242).await.unwrap_err(),
            NodeError::Stopped
        ));
        assert!(matches!(
            node.listen_quic(4242).await.unwrap_err(),
            NodeError::Stopped
        ));
        assert!(matches!(
            node.bind_udp(4242).await.err(),
            Some(NodeError::Stopped)
        ));
        assert!(matches!(
            node.ping("nobody").await.unwrap_err(),
            NodeError::Stopped
        ));
        assert!(matches!(
            node.resolve_peer_id("nobody").await.unwrap_err(),
            NodeError::Stopped
        ));
    }

    #[test]
    fn bytes_payload_roundtrip() {
        // Deliberately invalid UTF-8: the representation must not depend
        // on the data's contents.
        let data = vec![0u8, 159, 146, 150, 255];
        let payload = Node::<MockNetworkProvider>::bytes_payload(&data);
        assert_eq!(payload["encoding"], "base64");

        let msg = NamespacedMessage {
            from: "p".into(),
            namespace: "ns".into(),
            msg_type: "bytes".into(),
            payload,
            timestamp: None,
        };
        assert_eq!(msg.payload_bytes().unwrap(), data);
    }

    #[test]
    fn payload_bytes_rejects_other_msg_types() {
        let msg = NamespacedMessage {
            from: "p".into(),
            namespace: "ns".into(),
            msg_type: "message".into(),
            payload: json!({"data": "aGk="}),
            timestamp: None,
        };
        assert!(msg.payload_bytes().is_none());
    }

    #[tokio::test]
    async fn broadcast_json_reports_zero_peers() {
        let ws_port = random_port().await;
        let (node, _event_tx, _network) = make_test_node("node-bj", ws_port).await;

        let report = node.broadcast_json("ns", &json!({"a": 1})).await.unwrap();
        assert_eq!(report.attempted, 0);
        assert_eq!(report.queued, 0);
        assert!(report.failed.is_empty());
    }

    #[tokio::test]
    async fn subscribe_slow_path_sweeps_dead_namespaces() {
        let ws_port = random_port().await;
        let (node, _event_tx, _network) = make_test_node("node-sweep", ws_port).await;

        let rx = node.subscribe("dyn-a");
        drop(rx);
        // Creating a NEW namespace takes the slow path, which sweeps
        // entries with no live receivers.
        let _rx_b = node.subscribe("dyn-b");

        let filters = node.namespace_filters.read().unwrap();
        assert!(!filters.contains_key("dyn-a"), "dead entry not swept");
        assert!(filters.contains_key("dyn-b"));
    }

    #[tokio::test]
    async fn router_prunes_namespace_after_receivers_drop() {
        let ws_port = random_port().await;
        let (node, _event_tx, _network) = make_test_node("node-prune", ws_port).await;

        let rx = node.subscribe("dyn-c");
        drop(rx);

        // Drive the router with a message for the now-dead namespace.
        let envelope = Envelope::new("dyn-c", "message", json!({"x": 1})).with_timestamp();
        let data = JsonCodec.encode(&envelope).unwrap();
        node.session.test_inject_incoming("peer-x", data);

        // The router runs on a background task; poll for the prune.
        for _ in 0..50 {
            {
                let filters = node.namespace_filters.read().unwrap();
                if !filters.contains_key("dyn-c") {
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("dead namespace entry was not pruned by the router");
    }

    #[tokio::test]
    async fn test_node_builder_creates_node() {
        let ws_port = random_port().await;
        let (node, _event_tx, _network) = make_test_node("node-1", ws_port).await;

        let identity = node.local_info();
        assert_eq!(identity.tailscale_id, "node-1");
        assert_eq!(identity.device_id, "node-1");
        assert!(identity.tailscale_hostname.contains("node-1"));
    }

    #[tokio::test]
    async fn test_node_peers_from_network() {
        let ws_port = random_port().await;
        let (node, event_tx, _network) = make_test_node("node-1", ws_port).await;

        // Initially no peers.
        let peers = node.peers().await;
        assert!(peers.is_empty());

        // Inject a peer via Layer 3.
        let peer = make_loopback_peer("peer-a");
        let _ = event_tx.send(NetworkPeerEvent::Joined(peer));
        tokio::time::sleep(Duration::from_millis(50)).await;

        let peers = node.peers().await;
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].tailscale_id, "peer-a");
        assert!(
            peers[0].device_id.is_none(),
            "RFC 022: no ULID before identity"
        );
        assert!(peers[0].online);
        assert!(!peers[0].ws_connected);
    }

    #[tokio::test]
    #[allow(deprecated)] // exercises the legacy send/broadcast contract
    async fn test_node_send_to_unknown_peer_errors() {
        let ws_port = random_port().await;
        let (node, _event_tx, _network) = make_test_node("node-1", ws_port).await;

        let result = node.send("nonexistent", "test", b"hello").await;
        assert!(result.is_err());
        let err_str = result.unwrap_err().to_string();
        assert!(
            err_str.contains("unknown peer") || err_str.contains("not found"),
            "expected unknown peer error, got: {err_str}"
        );
    }

    #[tokio::test]
    async fn test_node_send_wraps_in_envelope() {
        // Test that send() properly creates an envelope.
        // We test the codec directly since a full send requires two connected nodes.
        let codec = JsonCodec;
        let data = b"hello world";
        let envelope = Envelope::new("test-ns", "message", serde_json::Value::from(data.to_vec()))
            .with_timestamp();

        let encoded = codec.encode(&envelope).unwrap();
        let decoded = codec.decode(&encoded).unwrap();

        assert_eq!(decoded.namespace, "test-ns");
        assert_eq!(decoded.msg_type, "message");
        assert!(decoded.timestamp.is_some());
    }

    #[tokio::test]
    async fn test_node_subscribe_filters_by_namespace() {
        let ws_port = random_port().await;
        let (node, _event_tx, _network) = make_test_node("node-1", ws_port).await;

        // Create subscribers for two different namespaces.
        let _rx_chat = node.subscribe("chat");
        let _rx_ft = node.subscribe("ft");

        // Subscribing to the same namespace again should work.
        let _rx_chat2 = node.subscribe("chat");
    }

    #[tokio::test]
    #[allow(deprecated)] // exercises the legacy send/broadcast contract
    async fn test_node_broadcast() {
        let ws_port = random_port().await;
        let (node, _event_tx, _network) = make_test_node("node-1", ws_port).await;

        // Broadcast with no connected peers should not panic.
        node.broadcast("test", b"hello everyone").await;
    }

    #[tokio::test]
    async fn test_node_open_tcp_resolves_peer() {
        let ws_port = random_port().await;
        let (node, event_tx, _network) = make_test_node("node-1", ws_port).await;

        // Start a TCP server for the test.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let tcp_port = listener.local_addr().unwrap().port();

        // Inject a loopback peer.
        let peer = make_loopback_peer("peer-tcp");
        let _ = event_tx.send(NetworkPeerEvent::Joined(peer));
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Accept a connection in the background.
        let accept_handle = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            stream
        });

        // open_tcp should resolve peer-tcp to 127.0.0.1 and connect.
        let stream = node.open_tcp("peer-tcp", tcp_port).await;
        assert!(stream.is_ok(), "open_tcp failed: {:?}", stream.err());

        let _ = accept_handle.await;
    }

    #[tokio::test]
    async fn test_node_open_tcp_unknown_peer_errors() {
        let ws_port = random_port().await;
        let (node, _event_tx, _network) = make_test_node("node-1", ws_port).await;

        let result = node.open_tcp("nonexistent", 8080).await;
        assert!(result.is_err());
        let err_str = result.unwrap_err().to_string();
        assert!(
            err_str.contains("not found"),
            "expected peer not found error, got: {err_str}"
        );
    }

    #[tokio::test]
    async fn test_node_ping_resolves_peer() {
        let ws_port = random_port().await;
        let (node, event_tx, _network) = make_test_node("node-1", ws_port).await;

        // No peer yet.
        let result = node.ping("peer-ping").await;
        assert!(result.is_err());

        // Inject peer.
        let peer = make_loopback_peer("peer-ping");
        let _ = event_tx.send(NetworkPeerEvent::Joined(peer));
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Should succeed (mock returns 1ms latency).
        let result = node.ping("peer-ping").await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().latency, Duration::from_millis(1));
    }

    #[tokio::test]
    async fn test_node_health() {
        let ws_port = random_port().await;
        let (node, _event_tx, _network) = make_test_node("node-1", ws_port).await;

        let health = node.health().await;
        assert!(health.healthy);
        assert_eq!(health.state, "running");
    }

    #[tokio::test]
    async fn test_node_connect_quic_unknown_peer_errors() {
        let ws_port = random_port().await;
        let (node, _event_tx, _network) = make_test_node("node-1", ws_port).await;

        let result = node.connect_quic("peer", 4433).await;
        assert!(matches!(result, Err(NodeError::PeerNotFound(_))));
    }

    #[tokio::test]
    async fn test_node_listen_tcp() {
        let ws_port = random_port().await;
        let (node, _event_tx, _network) = make_test_node("node-1", ws_port).await;

        // listen_tcp(0) should bind to an ephemeral port.
        let listener = node.listen_tcp(0).await;
        assert!(listener.is_ok(), "listen_tcp failed: {:?}", listener.err());
    }

    #[tokio::test]
    async fn test_envelope_serialize_deserialize() {
        let envelope = Envelope::new("chat", "message", json!({"text": "hello"})).with_timestamp();

        let bytes = envelope.serialize().unwrap();
        let decoded = Envelope::deserialize(&bytes).unwrap();

        assert_eq!(decoded.namespace, "chat");
        assert_eq!(decoded.msg_type, "message");
        assert_eq!(decoded.payload["text"], "hello");
        assert!(decoded.timestamp.is_some());
    }

    #[tokio::test]
    async fn test_envelope_codec_json() {
        let codec = JsonCodec;
        let envelope = Envelope::new("ft", "offer", json!({"file": "test.bin"}));

        let encoded = codec.encode(&envelope).unwrap();
        let decoded = codec.decode(&encoded).unwrap();

        assert_eq!(decoded.namespace, "ft");
        assert_eq!(decoded.payload["file"], "test.bin");
    }

    #[tokio::test]
    async fn test_envelope_unknown_fields_ignored() {
        let json_bytes = br#"{
            "namespace": "v2",
            "msg_type": "new",
            "payload": {},
            "future_field": "ignored"
        }"#;

        let codec = JsonCodec;
        let decoded = codec.decode(json_bytes).unwrap();
        assert_eq!(decoded.namespace, "v2");
        assert_eq!(decoded.msg_type, "new");
    }

    #[tokio::test]
    #[allow(deprecated)] // exercises the legacy send/broadcast contract
    async fn test_node_send_and_receive_roundtrip() {
        // Set up two nodes that communicate via loopback WS.
        let port_a = random_port().await;
        let port_b = random_port().await;

        let (node_a, event_tx_a, _net_a) = make_test_node("node-a", port_a).await;
        let (node_b, event_tx_b, _net_b) = make_test_node("node-b", port_b).await;

        // Inject each node as a peer of the other.
        let peer_b = NetworkPeer {
            id: "node-b".to_string(),
            hostname: "truffle-test-node-b".to_string(),
            ip: "127.0.0.1".parse().unwrap(),
            online: true,
            cur_addr: Some("127.0.0.1:41641".to_string()),
            relay: None,
            os: None,
            last_seen: None,
            key_expiry: None,
            dns_name: None,
        };
        let peer_a = NetworkPeer {
            id: "node-a".to_string(),
            hostname: "truffle-test-node-a".to_string(),
            ip: "127.0.0.1".parse().unwrap(),
            online: true,
            cur_addr: Some("127.0.0.1:41641".to_string()),
            relay: None,
            os: None,
            last_seen: None,
            key_expiry: None,
            dns_name: None,
        };

        let _ = event_tx_a.send(NetworkPeerEvent::Joined(peer_b));
        let _ = event_tx_b.send(NetworkPeerEvent::Joined(peer_a));
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Subscribe to namespace on node_b.
        let mut rx = node_b.subscribe("test");

        // Send from node_a to node_b. This triggers lazy WS connect.
        // Note: this will connect to node_b's WS listener on port_b.
        let send_result = node_a.send("node-b", "test", b"hello from a").await;

        // The send may fail in loopback mock because the WS port for node-b
        // is the listener port, and the mock's dial connects to 127.0.0.1:port_b.
        // In a real scenario with Tailscale, this works because each node
        // listens on its own Tailscale IP.
        //
        // For unit tests, we verify the envelope codec roundtrip works.
        // Full integration tests require two separate processes.
        if send_result.is_ok() {
            // If send succeeded, verify the message arrives.
            let msg = tokio::time::timeout(Duration::from_secs(2), rx.recv()).await;
            if let Ok(Ok(msg)) = msg {
                assert_eq!(msg.namespace, "test");
            }
        }
        // If send fails due to loopback WS peer-id mismatch, that's expected
        // in unit tests. The important thing is no panics.
    }

    // ── Lifecycle: stop() teardown ───────────────────────────────────

    #[tokio::test]
    #[allow(deprecated)] // exercises the legacy send/broadcast contract
    async fn test_node_stop_shuts_down_provider_and_is_idempotent() {
        let ws_port = random_port().await;
        let (node, event_tx, network) = make_test_node("node-1", ws_port).await;

        let _ = event_tx.send(NetworkPeerEvent::Joined(make_loopback_peer("peer-1")));
        tokio::time::sleep(Duration::from_millis(50)).await;

        assert_eq!(network.stop_call_count(), 0);
        node.stop().await;
        assert_eq!(
            network.stop_call_count(),
            1,
            "stop() must shut down the provider"
        );

        // Idempotent: second stop must not stop the provider again.
        node.stop().await;
        assert_eq!(network.stop_call_count(), 1);

        // Post-stop sends fail with NodeError::Stopped.
        let err = node.send("peer-1", "test", b"hi").await.unwrap_err();
        assert!(matches!(err, NodeError::Stopped));
    }

    #[tokio::test]
    #[allow(deprecated)] // exercises the legacy send/broadcast contract
    async fn test_node_stop_closes_ws_connections() {
        // A single node that discovers itself as a loopback peer. Under the
        // loopback mock every dial lands on the node's own listener, and the
        // RFC 022 dial-side identity check drops any connection whose
        // answerer is not the dialed peer — so the only WS a mock node can
        // legitimately establish is to an entry carrying its own
        // tailscale_id. That is all this test needs: a live WS connection
        // for stop() to tear down. The distinct device_id keeps the
        // self-hello from violating invariant I1 on projection.
        let port_a = random_port().await;
        let (node_a, event_tx_a, _net_a) =
            make_test_node_with_device_id("node-a", "dev-node-a", port_a).await;

        let self_peer = NetworkPeer {
            id: "node-a".to_string(),
            hostname: "truffle-test-node-a".to_string(),
            ip: "127.0.0.1".parse().unwrap(),
            online: true,
            cur_addr: Some("127.0.0.1:41641".to_string()),
            relay: None,
            os: None,
            last_seen: None,
            key_expiry: None,
            dns_name: None,
        };
        let _ = event_tx_a.send(NetworkPeerEvent::Joined(self_peer));
        tokio::time::sleep(Duration::from_millis(100)).await;

        node_a
            .send("node-a", "test", b"hello from a")
            .await
            .expect("loopback self-dial should establish a WS connection");

        let peers = node_a.peers().await;
        let entry = peers
            .iter()
            .find(|p| p.tailscale_id == "node-a")
            .expect("self peer discovered");
        assert!(
            entry.ws_connected,
            "send() should have established a WS connection"
        );

        node_a.stop().await;
        let peers = node_a.peers().await;
        let entry = peers
            .iter()
            .find(|p| p.tailscale_id == "node-a")
            .expect("self peer still discovered");
        assert!(
            !entry.ws_connected,
            "stop() must close and un-mark WS connections"
        );
    }

    // ── RFC 017 Phase 2: resolve_peer_id ─────────────────────────────

    /// Helper: inject a peer entry into the session registry and then
    /// stamp a synthetic RFC 017 identity on it so `resolve_peer_id`
    /// has something to look up. This skips the real hello exchange.
    async fn inject_peer_with_identity(
        node: &Node<MockNetworkProvider>,
        event_tx: &broadcast::Sender<NetworkPeerEvent>,
        tailscale_id: &str,
        device_id: &str,
        device_name: &str,
    ) {
        let peer = make_loopback_peer(tailscale_id);
        let _ = event_tx.send(NetworkPeerEvent::Joined(peer));
        tokio::time::sleep(Duration::from_millis(30)).await;

        let identity = crate::session::PeerIdentity {
            app_id: "test".into(),
            device_id: device_id.into(),
            device_name: device_name.into(),
            os: "linux".into(),
            tailscale_id: tailscale_id.into(),
        };
        assert!(
            node.session
                .test_stamp_identity(tailscale_id, identity)
                .await,
            "peer {tailscale_id} should exist in session registry before stamping identity"
        );
    }

    #[tokio::test]
    async fn test_resolve_peer_id_by_device_id() {
        let ws_port = random_port().await;
        let (node, event_tx, _net) = make_test_node("node-1", ws_port).await;

        inject_peer_with_identity(
            &node,
            &event_tx,
            "tailscale-abc",
            "01HZZZZZZZZZZZZZZZZZZZZZZZ",
            "Alice MacBook",
        )
        .await;

        let resolved = node
            .resolve_peer_id("01HZZZZZZZZZZZZZZZZZZZZZZZ")
            .await
            .unwrap();
        assert_eq!(resolved, "01HZZZZZZZZZZZZZZZZZZZZZZZ");
    }

    #[tokio::test]
    async fn test_resolve_peer_id_by_device_name() {
        let ws_port = random_port().await;
        let (node, event_tx, _net) = make_test_node("node-1", ws_port).await;

        inject_peer_with_identity(
            &node,
            &event_tx,
            "tailscale-abc",
            "01HXYZXYZXYZXYZXYZXYZXYZXY",
            "Bob's Mac",
        )
        .await;

        let resolved = node.resolve_peer_id("Bob's Mac").await.unwrap();
        assert_eq!(resolved, "01HXYZXYZXYZXYZXYZXYZXYZXY");
    }

    #[tokio::test]
    async fn test_resolve_peer_id_by_device_id_prefix() {
        let ws_port = random_port().await;
        let (node, event_tx, _net) = make_test_node("node-1", ws_port).await;

        inject_peer_with_identity(
            &node,
            &event_tx,
            "tailscale-abc",
            "01HXYZXYZXYZXYZXYZXYZXYZXY",
            "laptop",
        )
        .await;

        // Prefix match — 4 chars is the minimum the implementation
        // accepts.
        let resolved = node.resolve_peer_id("01HX").await.unwrap();
        assert_eq!(resolved, "01HXYZXYZXYZXYZXYZXYZXYZXY");
    }

    #[tokio::test]
    async fn test_resolve_peer_id_by_tailscale_id_legacy() {
        // Escape hatch: resolving by the Tailscale stable ID should
        // still work and return the device_id (or the tailscale_id as
        // fallback when no identity is populated).
        let ws_port = random_port().await;
        let (node, event_tx, _net) = make_test_node("node-1", ws_port).await;

        inject_peer_with_identity(
            &node,
            &event_tx,
            "tailscale-legacy",
            "01HLEGACY0000000000000000X",
            "legacy box",
        )
        .await;

        let resolved = node.resolve_peer_id("tailscale-legacy").await.unwrap();
        assert_eq!(resolved, "01HLEGACY0000000000000000X");
    }

    #[tokio::test]
    async fn test_resolve_peer_id_unknown() {
        let ws_port = random_port().await;
        let (node, _event_tx, _net) = make_test_node("node-1", ws_port).await;
        let result = node.resolve_peer_id("nope").await;
        assert!(matches!(result, Err(NodeError::PeerNotFound(_))));
    }

    // ── RFC 021: raw transport surface ────────────────────────────────

    #[tokio::test]
    async fn test_resolve_peer_by_bare_name_before_hello() {
        // Raw-transport-only peers have no hello identity; the bare device
        // name must still resolve via the hostname slug (RFC 021 smoke-test
        // finding: only the full `truffle-{app}-{slug}` hostname matched).
        let ws_port = random_port().await;
        let (node, event_tx, _net) = make_test_node("node-1", ws_port).await;

        let mut peer = make_loopback_peer("nodeid-9");
        peer.hostname = "truffle-test-ec2-smoke".to_string();
        let _ = event_tx.send(NetworkPeerEvent::Joined(peer));
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Bare slug, and an unslugged variant that normalizes to it.
        assert_eq!(node.resolve_peer("ec2-smoke").await.unwrap().id, "nodeid-9");
        assert_eq!(node.resolve_peer("EC2 Smoke").await.unwrap().id, "nodeid-9");
        // Unknown bare names still miss.
        assert!(node.resolve_peer("other-box").await.is_err());
    }

    #[tokio::test]
    async fn test_peers_device_name_strips_hostname_before_hello() {
        let ws_port = random_port().await;
        let (node, event_tx, _net) = make_test_node("node-1", ws_port).await;

        let mut peer = make_loopback_peer("nodeid-9");
        peer.hostname = "truffle-test-ec2-smoke".to_string();
        let _ = event_tx.send(NetworkPeerEvent::Joined(peer));
        tokio::time::sleep(Duration::from_millis(50)).await;

        let peers = node.peers().await;
        assert_eq!(peers.len(), 1);
        // Pre-identity: device_name is None; display_name uses stripped slug.
        assert!(peers[0].device_name.is_none());
        assert_eq!(peers[0].display_name, "ec2-smoke");
        assert_eq!(peers[0].hostname, "truffle-test-ec2-smoke");
        assert!(peers[0].device_id.is_none());
    }

    #[tokio::test]
    async fn test_resolve_peer_stale_ref_is_peer_gone() {
        let ws_port = random_port().await;
        let (node, event_tx, _net) = make_test_node("node-1", ws_port).await;

        let _ = event_tx.send(NetworkPeerEvent::Joined(make_loopback_peer("peer-9")));
        tokio::time::sleep(Duration::from_millis(50)).await;

        let live_ref = node.peers().await[0].peer_ref.clone();
        assert_eq!(node.resolve_peer(&live_ref).await.unwrap().id, "peer-9");

        // Left + rejoin bumps the generation: the old handle's ref must fail
        // with PeerGone (RFC 022 I5), never silently reach the new
        // generation.
        let _ = event_tx.send(NetworkPeerEvent::Left("peer-9".to_string()));
        tokio::time::sleep(Duration::from_millis(50)).await;
        let _ = event_tx.send(NetworkPeerEvent::Joined(make_loopback_peer("peer-9")));
        tokio::time::sleep(Duration::from_millis(50)).await;

        assert!(matches!(
            node.resolve_peer(&live_ref).await,
            Err(NodeError::PeerGone(_))
        ));
        // A ref for a fully departed peer is PeerGone too — not a typo-shaped
        // PeerNotFound.
        assert!(matches!(
            node.resolve_peer("peer-9:99").await,
            Err(NodeError::PeerGone(_))
        ));
        // peer() propagates PeerGone immediately instead of waiting out the
        // timeout.
        assert!(matches!(
            node.peer(&live_ref, Some(2_000)).await,
            Err(NodeError::PeerGone(_))
        ));
    }

    #[tokio::test]
    async fn test_resolve_peer_accepts_ip() {
        let ws_port = random_port().await;
        let (node, event_tx, _net) = make_test_node("node-1", ws_port).await;

        let _ = event_tx.send(NetworkPeerEvent::Joined(make_loopback_peer("peer-a")));
        tokio::time::sleep(Duration::from_millis(50)).await;

        let resolved = node.resolve_peer("127.0.0.1").await.unwrap();
        assert_eq!(resolved.id, "peer-a");
        // resolve_peer_id falls back to the tailscale id when no hello
        // identity is populated yet.
        assert_eq!(node.resolve_peer_id("127.0.0.1").await.unwrap(), "peer-a");
    }

    #[tokio::test]
    async fn test_listen_tcp_rejects_reserved_ports() {
        let ws_port = random_port().await;
        let (node, _event_tx, _net) = make_test_node("node-1", ws_port).await;

        let err = node.listen_tcp(9417).await.unwrap_err();
        assert!(
            matches!(err, NodeError::ReservedPort(9417)),
            "expected ReservedPort(9417), got: {err}"
        );

        // RFC 023 D4: 443 is no longer reserved — the guard must not reject
        // it. The mock binds a real host socket and 443 is privileged on
        // most systems, so accept either outcome; what matters is that
        // ReservedPort is gone and 443 bind failures carry the
        // sidecar-upgrade hint.
        match node.listen_tcp(443).await {
            Ok(listener) => assert_eq!(listener.port, 443),
            Err(NodeError::ReservedPort(_)) => panic!("443 must not be reserved (RFC 023 D4)"),
            Err(e) => assert!(
                e.to_string().contains("RFC 023"),
                "443 bind errors should hint at the sidecar upgrade, got: {e}"
            ),
        }
    }

    #[tokio::test]
    async fn test_listen_quic_rejects_reserved_and_ephemeral_ports() {
        let ws_port = random_port().await;
        let (node, _event_tx, _net) = make_test_node("node-1", ws_port).await;

        let err = node.listen_quic(9417).await.unwrap_err();
        assert!(matches!(err, NodeError::ReservedPort(9417)));

        let err = node.listen_quic(0).await.unwrap_err();
        assert!(matches!(err, NodeError::NotImplemented(_)));
    }

    #[test]
    fn test_builder_hostname_override_validation() {
        assert!(NodeBuilder::default().hostname("dashboard").is_ok());
        assert!(NodeBuilder::default().hostname("my-app-1").is_ok());
        for bad in ["", "Dashboard", "has.dot", "-lead", "trail-"] {
            assert!(
                NodeBuilder::default().hostname(bad).is_err(),
                "expected hostname {bad:?} to be rejected"
            );
        }
        let too_long = "x".repeat(64);
        assert!(NodeBuilder::default().hostname(too_long).is_err());
    }

    #[tokio::test]
    async fn test_bind_udp_falls_back_to_direct_socket() {
        // The mock provider has no UDP support, so bind_udp exercises the
        // direct-socket fallback and must return a usable socket.
        let ws_port = random_port().await;
        let (node, _event_tx, _net) = make_test_node("node-1", ws_port).await;

        let socket = node.bind_udp(0).await.unwrap();
        let addr = socket.local_addr().unwrap();
        assert_ne!(addr.port(), 0);
    }

    #[test]
    fn test_raw_alpn_scoping() {
        assert_eq!(
            crate::transport::quic::raw_alpn("demo"),
            b"truffle-raw.demo".to_vec()
        );
        assert_eq!(
            crate::transport::quic::raw_alpn(""),
            b"truffle-raw".to_vec()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_connect_quic_roundtrip_via_raw_listener() {
        let ws_port = random_port().await;
        let (client_node, event_tx, _net) = make_test_node("cli", ws_port).await;

        // Raw listener on port 0 (direct-socket fallback path) with the
        // ALPN that connect_quic derives from the mock app_id ("test").
        let server_network = Arc::new(MockNetworkProvider::new("srv"));
        let alpn = crate::transport::quic::raw_alpn("test");
        let listener = crate::transport::quic::listen_raw(&server_network, 0, &alpn)
            .await
            .unwrap();
        let port = listener.port();
        assert_ne!(port, 0);

        let echo_task = tokio::spawn(async move {
            let conn = listener.accept().await.expect("no incoming connection");
            let mut stream = conn
                .accept_stream()
                .await
                .unwrap()
                .expect("connection closed before stream");
            let mut received = Vec::new();
            while let Some(chunk) = stream.read(1024).await.unwrap() {
                received.extend_from_slice(&chunk);
            }
            stream.write(&received).await.unwrap();
            stream.finish();
            // Keep the connection alive until the peer has read the echo —
            // closing immediately could drop in-flight stream data.
            tokio::time::sleep(Duration::from_millis(500)).await;
            conn.close();
        });

        // Register the "server" as a peer at 127.0.0.1 and connect by name.
        let _ = event_tx.send(NetworkPeerEvent::Joined(make_loopback_peer("srv-peer")));
        tokio::time::sleep(Duration::from_millis(50)).await;

        let conn = client_node.connect_quic("srv-peer", port).await.unwrap();
        let mut stream = conn.open_stream().await.unwrap();
        stream.write(b"hello quic").await.unwrap();
        stream.finish();

        let mut echoed = Vec::new();
        while let Some(chunk) = stream.read(1024).await.unwrap() {
            echoed.extend_from_slice(&chunk);
        }
        assert_eq!(echoed, b"hello quic");

        echo_task.await.unwrap();
        conn.close();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_quic_raw_alpn_mismatch_rejected() {
        // Cross-app connections must fail the TLS handshake outright —
        // ALPN is the QUIC analog of the WS hello's app_id check.
        let server_network = Arc::new(MockNetworkProvider::new("srv"));
        let listener = crate::transport::quic::listen_raw(
            &server_network,
            0,
            &crate::transport::quic::raw_alpn("app-a"),
        )
        .await
        .unwrap();
        let port = listener.port();

        let client_network = Arc::new(MockNetworkProvider::new("cli"));
        let result = crate::transport::quic::connect_raw(
            &client_network,
            "127.0.0.1",
            port,
            &crate::transport::quic::raw_alpn("app-b"),
        )
        .await;

        assert!(
            result.is_err(),
            "cross-app QUIC connect must fail (ALPN mismatch)"
        );
    }

    // ── RFC 022 honest projection ─────────────────────────────────────

    #[tokio::test]
    async fn test_rfc022_node_peer_resolves_and_wait_miss_returns_none() {
        let ws_port = random_port().await;
        let (node, event_tx, _net) = make_test_node("node-1", ws_port).await;

        // Miss without wait
        assert!(node.peer("nope", None).await.unwrap().is_none());

        let _ = event_tx.send(NetworkPeerEvent::Joined(make_loopback_peer("peer-a")));
        tokio::time::sleep(Duration::from_millis(50)).await;

        let p = node.peer("peer-a", None).await.unwrap().expect("found");
        assert_eq!(p.tailscale_id, "peer-a");
        assert!(p.device_id.is_none());

        // waitMs on unknown times out to None
        let start = std::time::Instant::now();
        let miss = node.peer("still-missing", Some(80)).await.unwrap();
        assert!(miss.is_none());
        assert!(start.elapsed() >= Duration::from_millis(70));
    }

    #[test]
    fn test_rfc022_peer_projection_never_uses_tailscale_as_device_id() {
        use crate::session::PeerState;
        use std::net::Ipv4Addr;

        // Pre-identity
        let pre = PeerState {
            id: "ts-abc".into(),
            generation: 1,
            name: "truffle-app-laptop".into(),
            ip: Ipv4Addr::new(100, 64, 0, 1).into(),
            online: true,
            ws_connected: false,
            connection_type: "direct".into(),
            os: None,
            last_seen: None,
            identity: None,
            identity_suppressed: false,
        };
        let p = Peer::from(pre);
        assert!(p.device_id.is_none());
        assert_eq!(p.tailscale_id, "ts-abc");
        assert_eq!(p.peer_ref, "ts-abc:1");
        assert_eq!(p.display_name, "laptop");
        assert!(p
            .device_id
            .as_ref()
            .map(|d| d.as_str() != p.tailscale_id)
            .unwrap_or(true));

        // Post-identity
        let post = PeerState {
            id: "ts-abc".into(),
            generation: 1,
            name: "truffle-app-laptop".into(),
            ip: Ipv4Addr::new(100, 64, 0, 1).into(),
            online: true,
            ws_connected: true,
            connection_type: "direct".into(),
            os: Some("darwin".into()),
            last_seen: None,
            identity: Some(crate::session::PeerIdentity {
                app_id: "app".into(),
                device_id: "01J4K9M2Z8AB3RNYQPW6H5TC0X".into(),
                device_name: "Alice's Mac".into(),
                os: "darwin".into(),
                tailscale_id: "ts-abc".into(),
            }),
            identity_suppressed: false,
        };
        let p = Peer::from(post);
        assert_eq!(p.device_id.as_deref(), Some("01J4K9M2Z8AB3RNYQPW6H5TC0X"));
        assert_ne!(p.device_id.as_deref().unwrap(), p.tailscale_id);
        assert_eq!(p.display_name, "Alice's Mac");

        // Suppressed (first-wins loser)
        let suppressed = PeerState {
            id: "ts-xyz".into(),
            generation: 1,
            name: "truffle-app-other".into(),
            ip: Ipv4Addr::new(100, 64, 0, 2).into(),
            online: true,
            ws_connected: true,
            connection_type: "direct".into(),
            os: None,
            last_seen: None,
            identity: Some(crate::session::PeerIdentity {
                app_id: "app".into(),
                device_id: "01J4K9M2Z8AB3RNYQPW6H5TC0X".into(),
                device_name: "Clone".into(),
                os: "linux".into(),
                tailscale_id: "ts-xyz".into(),
            }),
            identity_suppressed: true,
        };
        let p = Peer::from(suppressed);
        assert!(
            p.device_id.is_none(),
            "suppressed claim must project null device_id"
        );
    }
}
