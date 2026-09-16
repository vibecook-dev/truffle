//! Layer 5: Session — Peer identity, connection lifecycle, message routing.
//!
//! The [`PeerRegistry`] is the central component. It consumes peer discovery
//! events from Layer 3 ([`NetworkProvider`]) and manages transport connections
//! from Layer 4 ([`StreamTransport`], [`RawTransport`](crate::transport::RawTransport)).
//!
//! # Layer rules
//!
//! - Layer 5 does NOT know what the data means (no namespaces, no envelopes)
//! - Layer 5 does NOT inspect payloads
//! - Layer 5 does NOT do peer discovery — it consumes Layer 3 events
//! - Peers exist because Layer 3 says they exist, NOT because of connections
//! - Connections are lazy — established on first `send()`
//! - Layer 5 does NOT implement any transport protocol — it delegates to Layer 4

pub mod hello;
pub mod reconnect;

#[cfg(test)]
mod tests;

use std::collections::{HashMap, HashSet};
use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{broadcast, mpsc, Mutex as AsyncMutex, RwLock, Semaphore};

pub use self::hello::{
    HelloEnvelope, HelloKind, PeerIdentity, CLOSE_APP_MISMATCH, CLOSE_HELLO_PROTOCOL,
    CLOSE_IDENTITY_MISMATCH, CLOSE_LOGIN_REFUSED, HELLO_TIMEOUT,
};
use self::reconnect::ReconnectBackoff;

use crate::network::{NetworkPeer, NetworkPeerEvent, NetworkProvider, PeerAddr};
use crate::transport::websocket::{WebSocketTransport, WsFramedStream};
use crate::transport::{FramedStream, StreamTransport};

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// A peer's state in the session registry.
///
/// Combines Layer 3 network information (discovery, addressing) with
/// Layer 5 session state (connection status). Peers are added to the
/// registry when Layer 3 reports them, NOT when transport connections
/// are established.
///
/// RFC 022: `id` is always the Tailscale stable node id (routing key).
/// Application-facing durable identity lives in [`Self::identity`] and is
/// projected as an honest `Option` (never filled with the Tailscale id).
#[derive(Debug, Clone)]
pub struct PeerState {
    /// Tailscale stable node ID from the network provider. Used as the
    /// primary key for routing inside the session layer.
    pub id: String,
    /// Generation counter for this `id` within this process (RFC 022 §7.7).
    /// Bumped each time the same Tailscale node re-joins after `Left`.
    /// Combined with `id` to form [`Self::peer_ref`].
    pub generation: u64,
    /// Tailscale hostname (as seen by Layer 3). This is the slugged form,
    /// NOT the user-facing `device_name`.
    pub name: String,
    /// Network IP address.
    pub ip: IpAddr,
    /// Whether the peer is currently online (from Layer 3).
    pub online: bool,
    /// Whether the peer has an active WebSocket connection.
    pub ws_connected: bool,
    /// Connection type description (e.g., "direct" or "relay:ord").
    pub connection_type: String,
    /// Operating system of the peer, if known (from Layer 3).
    pub os: Option<String>,
    /// Last time the peer was seen online (RFC 3339 string).
    pub last_seen: Option<String>,
    /// Peer identity advertised in the remote's hello envelope (RFC 017 §8).
    ///
    /// `None` until identity is learned (hello / future hostinfo). This is
    /// the source of truth for the durable ULID — never filled with the
    /// Tailscale id as a fallback (RFC 022).
    pub identity: Option<PeerIdentity>,
    /// When true, `identity` is stored but **not published** as `device_id`
    /// because another live peer already owns that ULID in `by_device`
    /// (first-wins, RFC 022 §7.7).
    pub identity_suppressed: bool,
    /// The peer's Tailscale login (owner) name as Layer 3 reported it
    /// (RFC 025 §3.3). `None` when the provider does not know it. Refreshed
    /// on every Layer 3 `Updated`; never taken from the hello.
    pub login_name: Option<String>,
}

impl PeerState {
    /// Process-local peer ref: `{tailscale_id}:{generation}` (RFC 022).
    pub fn peer_ref(&self) -> String {
        format_peer_ref(&self.id, self.generation)
    }

    /// Published durable device id, if any (respects first-wins suppression).
    pub fn published_device_id(&self) -> Option<&str> {
        if self.identity_suppressed {
            return None;
        }
        self.identity.as_ref().map(|i| i.device_id.as_str())
    }
}

/// Format a [`PeerState::peer_ref`] / user-facing `Peer.peer_ref`.
pub fn format_peer_ref(tailscale_id: &str, generation: u64) -> String {
    format!("{tailscale_id}:{generation}")
}

/// Parse a `{tailscale_id}:{generation}` peer ref (RFC 022).
///
/// Returns `None` for anything else: exactly one `:`, non-empty id, all-digit
/// generation. IPv6 literals (multiple colons) and colon-free identifiers
/// never qualify, so query strings fall through to normal resolution.
pub(crate) fn parse_peer_ref(s: &str) -> Option<(&str, u64)> {
    let (ts, generation) = s.rsplit_once(':')?;
    if ts.is_empty() || ts.contains(':') || generation.is_empty() {
        return None;
    }
    if !generation.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    Some((ts, generation.parse().ok()?))
}

/// Events emitted by the session layer when peer state changes.
///
/// Subscribers receive these via [`PeerRegistry::on_peer_change`].
/// Events cover both Layer 3 discovery changes and Layer 5 connection
/// lifecycle changes.
#[derive(Debug, Clone)]
pub enum PeerEvent {
    /// A new peer appeared on the network (from Layer 3).
    Joined(PeerState),
    /// A peer left the network (from Layer 3). Carries the entry's **final
    /// state** (marked offline, WS down) — the registry entry is already
    /// gone when this fires, so consumers get a usable last view for
    /// cleanup (RFC 022 §7.4 / §16.4) instead of a bare id.
    Left(PeerState),
    /// A peer's metadata changed (IP, relay, online status, from Layer 3).
    Updated(PeerState),
    /// Durable identity was first set or rotated on a peer (RFC 022).
    /// Carries the full peer snapshot after the change.
    Identity(PeerState),
    /// A WebSocket connection was established to a peer (Layer 5 — WS transport).
    /// Payload is the Tailscale stable id.
    WsConnected(String),
    /// A WebSocket connection was lost to a peer (Layer 5 — WS transport).
    /// Payload is the Tailscale stable id. Does **not** clear learned identity.
    WsDisconnected(String),
    /// Authentication is required — the URL should be shown to the user.
    AuthRequired { url: String },
}

/// Outcome of a broadcast.
///
/// "Queued" means the bytes were handed to a peer's connection task —
/// delivery is not confirmed at this layer. Broadcasts reach only peers
/// with an active WebSocket connection; no lazy connections are made.
#[derive(Debug, Clone, Default)]
pub struct BroadcastReport {
    /// Peers with an active WS connection at broadcast time.
    pub attempted: usize,
    /// Messages successfully queued to a connection task.
    pub queued: usize,
    /// Tailscale ids of peers whose connection task was already closed.
    pub failed: Vec<String>,
}

/// An incoming message received from a peer via WebSocket.
///
/// Layer 5 does not inspect or interpret the data — it simply delivers
/// raw bytes along with the sender's identity and a timestamp.
#[derive(Debug, Clone)]
pub struct IncomingMessage {
    /// Sender's **WhoIs-verified Tailscale stable id** (the connection's
    /// routing key). RFC 022 §7.5: attribution never uses the self-declared
    /// ULID from the hello envelope.
    pub from: String,
    /// Raw bytes received (Layer 6 will interpret this).
    pub data: Vec<u8>,
    /// When this message was received.
    pub received_at: Instant,
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors from Layer 5 session operations.
#[derive(Debug, thiserror::Error)]
pub enum SessionError {
    /// The specified peer is not known to the registry.
    #[error("unknown peer: {0}")]
    UnknownPeer(String),

    /// A peer-ref selector referenced a departed or superseded registry
    /// entry generation (RFC 022 I5) — the handle it came from is stale.
    #[error("peer gone: {0}")]
    PeerGone(String),

    /// The specified peer is offline (Layer 3 reports not online).
    #[error("peer offline: {0}")]
    PeerOffline(String),

    /// Failed to establish a transport connection.
    #[error("connect failed: {0}")]
    ConnectFailed(String),

    /// Failed to send data on a transport connection.
    #[error("send failed: {0}")]
    SendFailed(String),

    /// Reconnect backoff is active — wait before retrying.
    #[error("reconnect backoff: retry after {retry_after:?}")]
    ReconnectBackoff {
        /// How long the caller must wait before retrying.
        retry_after: Duration,
    },

    /// RFC 025 §3.3: this node is login-gated and Layer 3 cannot currently
    /// state the peer's owner, so a NEW connection is refused.
    ///
    /// The peer keeps its place in the registry — a status that could not
    /// name an owner must not empty a gated mesh — and an EXISTING
    /// connection stands. Only opening a new one is refused, so our hello is
    /// never revealed to a node whose login we cannot check. The inbound
    /// direction already agrees: a caller whose WhoIs carries no login is
    /// refused at the hello with 4004.
    #[error("login unknown for peer: {0}")]
    LoginUnknown(String),

    /// A transport layer error.
    #[error("transport error: {0}")]
    Transport(#[from] crate::transport::TransportError),
}

// ---------------------------------------------------------------------------
// WsConnectionHandle — channel-based connection control
// ---------------------------------------------------------------------------

/// A handle to an active WebSocket connection.
///
/// Instead of sharing a `Mutex<WsFramedStream>` (which would deadlock
/// because recv holds the lock across awaits), we use a channel pair:
/// - `send_tx`: Send data to the connection task, which writes to the WS
/// - `close_tx`: Signal the connection task to close and exit
///
/// The connection task exclusively owns the `WsFramedStream` and uses
/// `tokio::select!` to multiplex between sending, receiving, and close
/// signals. This avoids lock contention entirely.
struct WsConnectionHandle {
    /// Channel to send outgoing data to the connection task.
    send_tx: mpsc::Sender<Vec<u8>>,
    /// One-shot close signal. Dropping this also signals close.
    close_tx: mpsc::Sender<()>,
    /// Stable node ID of the connected peer.
    #[allow(dead_code)]
    peer_id: String,
    /// When this connection was established.
    #[allow(dead_code)]
    connected_at: Instant,
    /// Whether this node dialed or accepted the connection.
    direction: ConnectionDirection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConnectionDirection {
    Inbound,
    Outbound,
}

// ---------------------------------------------------------------------------
// PeerRegistry
// ---------------------------------------------------------------------------

/// Manages peer state and WebSocket connections.
///
/// The `PeerRegistry` is the heart of Layer 5. It:
///
/// 1. **Tracks peers** from Layer 3 discovery events — peers exist in the
///    registry even with zero transport connections.
/// 2. **Manages lazy connections** — the first [`send()`](Self::send) to a
///    peer triggers a WebSocket connection via Layer 4. Subsequent sends
///    reuse the cached connection.
/// 3. **Routes messages** — incoming messages from any peer are forwarded
///    to subscribers via a broadcast channel.
/// 4. **Emits lifecycle events** — [`PeerEvent`]s for peer discovery changes
///    and connection state changes.
///
/// # Example
///
/// ```ignore
/// use std::sync::Arc;
/// use truffle_core::session::PeerRegistry;
///
/// let registry = PeerRegistry::new(network, ws_transport);
/// registry.start().await;
///
/// // Peers appear from Layer 3 discovery
/// let peers = registry.peers().await;
///
/// // First send lazily connects
/// registry.send("peer-id", b"hello").await?;
/// ```
pub struct PeerRegistry<N: NetworkProvider + 'static> {
    /// Layer 3 network provider (for peer events and addressing).
    network: Arc<N>,
    /// Layer 4 WebSocket transport (for framed connections).
    ws_transport: Arc<WebSocketTransport<N>>,

    /// All known peers from Layer 3, keyed by Tailscale stable id.
    /// Peers exist here even with zero connections.
    peers: Arc<RwLock<HashMap<String, PeerState>>>,

    /// Durable ULID → Tailscale id for at most one **published** live entry
    /// (RFC 022 §7.7). Used for queries only — never for message attribution.
    by_device: Arc<RwLock<HashMap<String, String>>>,

    /// Next generation number per Tailscale id (incremented on each join).
    next_generation: Arc<RwLock<HashMap<String, u64>>>,

    /// Active WebSocket connection handles indexed by peer_id (Tailscale id).
    ws_connections: Arc<RwLock<HashMap<String, WsConnectionHandle>>>,

    /// Hello identities accepted before Layer 3 has announced the peer.
    /// Reconciled into `peers` on the subsequent Joined/Updated event.
    pending_identities: Arc<RwLock<HashMap<String, PeerIdentity>>>,

    /// Reconnect backoff trackers per peer.
    peer_backoffs: Arc<RwLock<HashMap<String, ReconnectBackoff>>>,

    /// Set of peer IDs currently being connected to (prevents duplicate dials).
    connecting: Arc<RwLock<HashSet<String>>>,

    /// Event channel for peer changes (discovery + connection lifecycle).
    event_tx: broadcast::Sender<PeerEvent>,

    /// Channel for incoming messages from any connected peer.
    incoming_tx: broadcast::Sender<IncomingMessage>,

    /// Handles to the registry's background loops (peer-event + WS accept),
    /// retained so [`Self::shutdown`] can abort them instead of leaving
    /// them running until every Arc clone drops.
    background_tasks: Arc<std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>>,

    /// RFC 022 Phase C: proactively exchange hello with online peers.
    eager_identity: bool,

    /// Cap concurrent eager-identity dials (default 4).
    eager_identity_sem: Arc<Semaphore>,

    /// Per-peer jitter window (ms) applied before an eager dial (RFC 022 §8.1).
    eager_identity_jitter_ms: u64,

    /// RFC 025 §3.3: refuse a NEW dial to a peer whose login is unresolved.
    login_gated: bool,

    /// Peer ids with an in-flight ensure_identity task (dedupe).
    identity_inflight: Arc<AsyncMutex<HashSet<String>>>,
}

/// Options for [`PeerRegistry::with_options`].
#[derive(Debug, Clone)]
pub struct PeerRegistryOptions {
    /// When true (default), dial the envelope WS once per online peer to
    /// learn durable identity without waiting for app `send` (RFC 022 §8).
    pub eager_identity: bool,
    /// Max concurrent eager-identity dials. Default: 4.
    pub eager_identity_concurrency: usize,
    /// Max per-peer delay (ms) applied *before* each EAGER identity dial to
    /// stagger the first burst of hellos when a node joins a large mesh
    /// (RFC 022 §8.1). The delay is a deterministic hash of the peer id in
    /// `0..eager_identity_jitter_ms` (truffle-core has no `rand` dependency).
    /// `0` disables jitter — tests set it for deterministic timing. Only the
    /// eager path is delayed; app `send` / `ensure_ws_connected` never wait.
    /// Default: 250.
    pub eager_identity_jitter_ms: u64,
    /// RFC 025 §3.3: is this node login-gated (a non-empty `login_allow`)?
    ///
    /// When true, the registry refuses to open a NEW connection to a peer
    /// whose `login_name` Layer 3 cannot state, with
    /// [`SessionError::LoginUnknown`]. Existing connections stand. Default
    /// `false` — an ungated node behaves exactly as before RFC 025.
    pub login_gated: bool,
}

impl Default for PeerRegistryOptions {
    fn default() -> Self {
        Self {
            eager_identity: true,
            eager_identity_concurrency: 4,
            eager_identity_jitter_ms: 250,
            login_gated: false,
        }
    }
}

impl<N: NetworkProvider + 'static> PeerRegistry<N> {
    /// Create a new peer registry with default options (eager identity on).
    ///
    /// - `network`: The Layer 3 network provider for peer discovery.
    /// - `ws_transport`: The Layer 4 WebSocket transport for connections.
    ///
    /// Call [`start()`](Self::start) after creation to begin processing
    /// peer events and accepting incoming connections.
    pub fn new(network: Arc<N>, ws_transport: Arc<WebSocketTransport<N>>) -> Self {
        Self::with_options(network, ws_transport, PeerRegistryOptions::default())
    }

    /// Create a peer registry with explicit options (RFC 022 Phase C).
    pub fn with_options(
        network: Arc<N>,
        ws_transport: Arc<WebSocketTransport<N>>,
        options: PeerRegistryOptions,
    ) -> Self {
        let (event_tx, _) = broadcast::channel(256);
        let (incoming_tx, _) = broadcast::channel(1024);
        let concurrency = options.eager_identity_concurrency.max(1);

        Self {
            network,
            ws_transport,
            peers: Arc::new(RwLock::new(HashMap::new())),
            by_device: Arc::new(RwLock::new(HashMap::new())),
            next_generation: Arc::new(RwLock::new(HashMap::new())),
            ws_connections: Arc::new(RwLock::new(HashMap::new())),
            pending_identities: Arc::new(RwLock::new(HashMap::new())),
            peer_backoffs: Arc::new(RwLock::new(HashMap::new())),
            connecting: Arc::new(RwLock::new(HashSet::new())),
            event_tx,
            incoming_tx,
            background_tasks: Arc::new(std::sync::Mutex::new(Vec::new())),
            login_gated: options.login_gated,
            eager_identity: options.eager_identity,
            eager_identity_sem: Arc::new(Semaphore::new(concurrency)),
            eager_identity_jitter_ms: options.eager_identity_jitter_ms,
            identity_inflight: Arc::new(AsyncMutex::new(HashSet::new())),
        }
    }

    /// Start the peer registry.
    ///
    /// This spawns two background tasks:
    /// 1. A task that subscribes to Layer 3 peer events and maintains the
    ///    peer list (Joined/Left/Updated).
    /// 2. A task that listens for incoming WebSocket connections from peers
    ///    and spawns connection tasks for each.
    ///
    /// Call this once after constructing the registry.
    pub async fn start(&self) {
        // Task 1: Subscribe to Layer 3 peer events
        self.spawn_peer_event_loop();

        // Task 2: Accept incoming WS connections
        self.spawn_accept_loop().await;
    }

    /// Spawn a task that subscribes to Layer 3 peer events and updates the
    /// internal peer list.
    fn spawn_peer_event_loop(&self) {
        let mut events = self.network.peer_events();
        let peers = self.peers.clone();
        let by_device = self.by_device.clone();
        let next_generation = self.next_generation.clone();
        let ws_connections = self.ws_connections.clone();
        let pending_identities = self.pending_identities.clone();
        let event_tx = self.event_tx.clone();
        // Clones for scheduling eager identity from the event loop.
        let schedule_ctx = EagerScheduleCtx {
            eager_identity: self.eager_identity,
            login_gated: self.login_gated,
            peers: self.peers.clone(),
            by_device: self.by_device.clone(),
            ws_connections: self.ws_connections.clone(),
            peer_backoffs: self.peer_backoffs.clone(),
            connecting: self.connecting.clone(),
            event_tx: self.event_tx.clone(),
            incoming_tx: self.incoming_tx.clone(),
            ws_transport: self.ws_transport.clone(),
            network: self.network.clone(),
            eager_identity_sem: self.eager_identity_sem.clone(),
            eager_identity_jitter_ms: self.eager_identity_jitter_ms,
            identity_inflight: self.identity_inflight.clone(),
            background_tasks: self.background_tasks.clone(),
        };

        let handle = tokio::spawn(async move {
            loop {
                match events.recv().await {
                    Ok(NetworkPeerEvent::Joined(network_peer)) => {
                        let generation = {
                            let mut gens = next_generation.write().await;
                            let e = gens.entry(network_peer.id.clone()).or_insert(0);
                            *e += 1;
                            *e
                        };
                        let mut state = network_peer_to_state(&network_peer, generation);
                        state.ws_connected =
                            ws_connections.read().await.contains_key(&network_peer.id);
                        let peer_id = network_peer.id.clone();
                        let peer_event = PeerEvent::Joined(state.clone());
                        let pending_identity = {
                            let mut pending = pending_identities.write().await;
                            pending.remove(&peer_id)
                        };

                        let identity_outcomes = {
                            let mut map = peers.write().await;
                            map.insert(network_peer.id.clone(), state);
                            if let Some(identity) = pending_identity {
                                let mut by_dev = by_device.write().await;
                                apply_identity(&mut map, &mut by_dev, &peer_id, identity)
                            } else {
                                Vec::new()
                            }
                        };

                        let _ = event_tx.send(peer_event);
                        emit_identity_outcomes(&event_tx, identity_outcomes);
                        tracing::info!(
                            peer_id = %network_peer.id,
                            generation,
                            peer_name = %network_peer.hostname,
                            "session: peer joined"
                        );

                        let needs_identity = {
                            let map = peers.read().await;
                            map.get(&peer_id)
                                .map(|state| state.online && state.published_device_id().is_none())
                                .unwrap_or(false)
                        };
                        if needs_identity {
                            schedule_ctx.schedule(peer_id);
                        }
                    }
                    Ok(NetworkPeerEvent::Left(peer_id)) => {
                        pending_identities.write().await.remove(&peer_id);
                        // Close any active WS connection for this peer
                        let handle = {
                            let mut conns = ws_connections.write().await;
                            conns.remove(&peer_id)
                        };
                        if let Some(handle) = handle {
                            let _ = handle.close_tx.send(()).await;
                            // Emit Disconnected before Left
                            let _ = event_tx.send(PeerEvent::WsDisconnected(peer_id.clone()));
                            tracing::info!(
                                peer_id = %peer_id,
                                "session: closed WS connection for departing peer"
                            );
                        }

                        let removed = {
                            let mut map = peers.write().await;
                            map.remove(&peer_id)
                        };

                        // Drop by_device mapping if it pointed at this peer; promote
                        // a suppressed claimant if one exists (RFC 022 §7.7).
                        if let Some(mut removed) = removed {
                            let promote = {
                                let mut by_dev = by_device.write().await;
                                if let Some(uid) = removed.published_device_id() {
                                    if by_dev.get(uid).map(|s| s.as_str()) == Some(peer_id.as_str())
                                    {
                                        by_dev.remove(uid);
                                        Some(uid.to_string())
                                    } else {
                                        None
                                    }
                                } else if let Some(ref ident) = removed.identity {
                                    // Suppressed holder leaving — nothing published
                                    let _ = ident;
                                    None
                                } else {
                                    None
                                }
                            };

                            if let Some(uid) = promote {
                                let mut map = peers.write().await;
                                let mut by_dev = by_device.write().await;
                                if let Some(promoted) = map.values_mut().find(|p| {
                                    p.id != peer_id
                                        && p.online
                                        && p.identity
                                            .as_ref()
                                            .map(|i| i.device_id == uid)
                                            .unwrap_or(false)
                                        && p.identity_suppressed
                                }) {
                                    promoted.identity_suppressed = false;
                                    by_dev.insert(uid.clone(), promoted.id.clone());
                                    let snap = promoted.clone();
                                    drop(map);
                                    drop(by_dev);
                                    let _ = event_tx.send(PeerEvent::Identity(snap));
                                    tracing::info!(
                                        device_id = %uid,
                                        "session: promoted suppressed ULID claimant after holder left"
                                    );
                                }
                            }

                            // Emit `Left` with the entry's final view: the map
                            // entry is already gone, and consumers need a
                            // usable last state for cleanup (RFC 022 §16.4).
                            removed.online = false;
                            removed.ws_connected = false;
                            let _ = event_tx.send(PeerEvent::Left(removed));
                            tracing::info!(peer_id = %peer_id, "session: peer left");
                        } else {
                            // Never announced via `joined` — nothing to retire,
                            // so no `left` is emitted either.
                            tracing::debug!(
                                peer_id = %peer_id,
                                "session: left for unknown peer; no event"
                            );
                        }
                    }
                    Ok(NetworkPeerEvent::Updated(network_peer)) => {
                        let mut state = network_peer_to_state(&network_peer, 0);
                        // Both branches below assign this before it is read;
                        // no initializer keeps the unused-assignment lint quiet.
                        let mut became_online_without_identity;

                        // Preserve Layer 5 state (ws_connected, identity,
                        // generation, suppression) from the existing entry —
                        // Layer 3 Updated events only carry discovery metadata.
                        {
                            let pending_identity = {
                                let mut pending = pending_identities.write().await;
                                pending.remove(&network_peer.id)
                            };
                            let mut map = peers.write().await;
                            if let Some(existing) = map.get(&network_peer.id) {
                                state.generation = existing.generation;
                                state.ws_connected = existing.ws_connected;
                                state.identity = existing.identity.clone();
                                state.identity_suppressed = existing.identity_suppressed;
                                became_online_without_identity = !existing.online
                                    && state.online
                                    && existing.published_device_id().is_none();
                            } else {
                                // Unknown peer Updated without Joined — treat as gen 1.
                                let mut gens = next_generation.write().await;
                                let e = gens.entry(network_peer.id.clone()).or_insert(0);
                                *e += 1;
                                state.generation = *e;
                                became_online_without_identity =
                                    state.online && state.published_device_id().is_none();
                            }
                            state.ws_connected =
                                ws_connections.read().await.contains_key(&network_peer.id);
                            map.insert(network_peer.id.clone(), state.clone());
                            if let Some(identity) = pending_identity {
                                let mut by_dev = by_device.write().await;
                                let outcomes = apply_identity(
                                    &mut map,
                                    &mut by_dev,
                                    &network_peer.id,
                                    identity,
                                );
                                emit_identity_outcomes(&event_tx, outcomes);
                                became_online_without_identity = map
                                    .get(&network_peer.id)
                                    .map(|state| {
                                        state.online && state.published_device_id().is_none()
                                    })
                                    .unwrap_or(false);
                                state = map
                                    .get(&network_peer.id)
                                    .cloned()
                                    .expect("peer inserted immediately above");
                            }
                        }

                        let peer_id = network_peer.id.clone();
                        let _ = event_tx.send(PeerEvent::Updated(state));
                        tracing::debug!(
                            peer_id = %network_peer.id,
                            "session: peer updated"
                        );

                        if became_online_without_identity {
                            schedule_ctx.schedule(peer_id);
                        }
                    }
                    Ok(NetworkPeerEvent::AuthRequired { url }) => {
                        let _ = event_tx.send(PeerEvent::AuthRequired { url });
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(
                            missed = n,
                            "session: peer event receiver lagged, missed {n} events"
                        );
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        tracing::debug!("session: peer event channel closed");
                        break;
                    }
                }
            }
        });
        self.background_tasks.lock().unwrap().push(handle);
    }

    /// Spawn a task that accepts incoming WebSocket connections from peers.
    async fn spawn_accept_loop(&self) {
        let ws_transport = self.ws_transport.clone();
        let ws_connections = self.ws_connections.clone();
        let peers = self.peers.clone();
        let by_device = self.by_device.clone();
        let pending_identities = self.pending_identities.clone();
        let event_tx = self.event_tx.clone();
        let incoming_tx = self.incoming_tx.clone();
        let local_tailscale_id = self.network.local_identity().tailscale_id;

        // Try to start the WS listener. If it fails, log and return.
        let mut listener = match ws_transport.listen().await {
            Ok(l) => l,
            Err(e) => {
                tracing::error!("session: failed to start WS listener: {e}");
                return;
            }
        };

        let handle = tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Some(stream) => {
                        let peer_id = stream.remote_peer_id().to_string();
                        let remote_identity = stream.remote_identity().cloned();
                        tracing::info!(
                            peer_id = %peer_id,
                            device_id = remote_identity.as_ref().map(|i| i.device_id.as_str()),
                            "session: accepted incoming WS connection"
                        );

                        // Create connection handle and spawn connection task.
                        // Attribution uses WhoIs-verified Tailscale id (peer_id).
                        let handle = spawn_connection_task(
                            stream,
                            peer_id.clone(),
                            ConnectionDirection::Inbound,
                            ws_connections.clone(),
                            peers.clone(),
                            event_tx.clone(),
                            incoming_tx.clone(),
                        );

                        if !install_connection(
                            &ws_connections,
                            &local_tailscale_id,
                            &peer_id,
                            handle,
                        )
                        .await
                        {
                            tracing::debug!(
                                peer_id = %peer_id,
                                "session: rejected duplicate incoming WS connection"
                            );
                            continue;
                        }

                        // Mark peer as connected and apply identity from hello.
                        // An inbound hello can win the race with Layer 3
                        // discovery; retain it until Joined/Updated creates the
                        // peer entry instead of silently discarding identity.
                        {
                            let mut pending = pending_identities.write().await;
                            let mut map = peers.write().await;
                            let mut by_dev = by_device.write().await;
                            if let Some(state) = map.get_mut(&peer_id) {
                                state.ws_connected = true;
                                if let Some(identity) = remote_identity {
                                    let outcomes =
                                        apply_identity(&mut map, &mut by_dev, &peer_id, identity);
                                    emit_identity_outcomes(&event_tx, outcomes);
                                }
                            } else if let Some(identity) = remote_identity {
                                pending.insert(peer_id.clone(), identity);
                            }
                        }

                        let _ = event_tx.send(PeerEvent::WsConnected(peer_id));
                    }
                    None => {
                        tracing::debug!("session: WS listener closed");
                        break;
                    }
                }
            }
        });
        self.background_tasks.lock().unwrap().push(handle);
    }

    /// Return all known peers.
    ///
    /// This returns peers discovered by Layer 3, including those with
    /// no active transport connections (`ws_connected: false`).
    pub async fn peers(&self) -> Vec<PeerState> {
        let map = self.peers.read().await;
        map.values().cloned().collect()
    }

    /// Subscribe to peer change events.
    ///
    /// Returns a broadcast receiver that yields [`PeerEvent`]s for peer
    /// discovery changes (Joined/Left/Updated) and connection lifecycle
    /// changes (Connected/Disconnected).
    pub fn on_peer_change(&self) -> broadcast::Receiver<PeerEvent> {
        self.event_tx.subscribe()
    }

    /// Send data to a specific peer.
    ///
    /// If no WebSocket connection exists to the peer, one is lazily
    /// established via Layer 4. The connection is cached for subsequent
    /// sends. If the peer is unknown or offline, an error is returned.
    ///
    /// # Errors
    ///
    /// - [`SessionError::UnknownPeer`] if the peer is not in the registry
    /// - [`SessionError::PeerOffline`] if Layer 3 reports the peer as offline
    /// - [`SessionError::ConnectFailed`] if the WS connection cannot be established
    /// - [`SessionError::SendFailed`] if the send operation fails
    pub async fn send(&self, peer_id: &str, data: &[u8]) -> Result<(), SessionError> {
        let peer_id = self.resolve_routing_key(peer_id).await?;
        self.ensure_ws_connected(&peer_id).await?;

        let conns = self.ws_connections.read().await;
        let handle = conns
            .get(&peer_id)
            .ok_or_else(|| SessionError::SendFailed("connection missing after connect".into()))?;
        handle
            .send_tx
            .send(data.to_vec())
            .await
            .map_err(|_| SessionError::SendFailed("connection task closed".to_string()))
    }

    /// Ensure a WS session exists to `peer_id` (Tailscale routing key).
    /// Completes the RFC 017 hello and applies identity (RFC 022).
    ///
    /// Used by app `send` and by eager-identity (Phase C). Idempotent when
    /// already connected.
    pub async fn ensure_ws_connected(&self, peer_id: &str) -> Result<(), SessionError> {
        // Already connected?
        {
            let conns = self.ws_connections.read().await;
            if conns.contains_key(peer_id) {
                return Ok(());
            }
        }

        let peer_addr = {
            let map = self.peers.read().await;
            let state = map
                .get(peer_id)
                .ok_or_else(|| SessionError::UnknownPeer(peer_id.to_string()))?;
            if !state.online {
                return Err(SessionError::PeerOffline(peer_id.to_string()));
            }
            // RFC 025 §3.3: a gated node never opens a NEW connection to a
            // peer whose owner Layer 3 cannot state. The entry survives an
            // unknown login so the mesh is not emptied by a transient
            // omission, but our hello is not revealed to it and its slices
            // are not invited. An ALREADY-OPEN connection returned above.
            if self.login_gated && state.login_name.is_none() {
                return Err(SessionError::LoginUnknown(peer_id.to_string()));
            }
            PeerAddr {
                ip: Some(state.ip),
                hostname: state.name.clone(),
                dns_name: None,
            }
        };

        // Backoff
        {
            let backoffs = self.peer_backoffs.read().await;
            if let Some(backoff) = backoffs.get(peer_id) {
                if backoff.should_retry().is_none() {
                    let retry_after = backoff.retry_after();
                    return Err(SessionError::ReconnectBackoff { retry_after });
                }
            }
        }

        // Dedupe concurrent dials
        {
            let already = {
                let connecting = self.connecting.read().await;
                connecting.contains(peer_id)
            };
            if already {
                // Wait briefly for the other dial to finish, then re-check.
                for _ in 0..50 {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    let conns = self.ws_connections.read().await;
                    if conns.contains_key(peer_id) {
                        return Ok(());
                    }
                    let connecting = self.connecting.read().await;
                    if !connecting.contains(peer_id) {
                        break;
                    }
                }
                let conns = self.ws_connections.read().await;
                if conns.contains_key(peer_id) {
                    return Ok(());
                }
            }
            let mut connecting = self.connecting.write().await;
            if connecting.contains(peer_id) {
                return Err(SessionError::ConnectFailed(
                    "connection already in progress".to_string(),
                ));
            }
            connecting.insert(peer_id.to_string());
        }

        tracing::info!(peer_id = %peer_id, "session: connecting WS");

        let connect_result = self.ws_transport.connect(&peer_addr).await;

        {
            let mut connecting = self.connecting.write().await;
            connecting.remove(peer_id);
        }

        let ws_stream = match connect_result {
            Ok(stream) => {
                let mut backoffs = self.peer_backoffs.write().await;
                backoffs
                    .entry(peer_id.to_string())
                    .or_insert_with(ReconnectBackoff::new)
                    .success();
                stream
            }
            Err(e) => {
                let mut backoffs = self.peer_backoffs.write().await;
                backoffs
                    .entry(peer_id.to_string())
                    .or_insert_with(ReconnectBackoff::new)
                    .failure();
                return Err(SessionError::ConnectFailed(e.to_string()));
            }
        };

        // RFC 022 §7.5, dial side: the answerer's claimed tailscale_id must
        // match the peer this connection was dialed for. A mismatch means we
        // reached something other than `peer_id` (port collision, loopback
        // test rig, or a lying hello) — registering it would poison the
        // entry's identity and route this peer's traffic to the answerer.
        if let Some(claimed) = ws_stream.remote_identity() {
            if claimed.tailscale_id != peer_id {
                tracing::warn!(
                    peer_id = %peer_id,
                    claimed = %claimed.tailscale_id,
                    "session: dialed peer answered as a different tailscale_id; dropping connection"
                );
                return Err(SessionError::ConnectFailed(format!(
                    "hello identity mismatch: dialed {peer_id}, answerer claims {}",
                    claimed.tailscale_id
                )));
            }
        }

        let remote_identity = ws_stream.remote_identity().cloned();

        let handle = spawn_connection_task(
            ws_stream,
            peer_id.to_string(),
            ConnectionDirection::Outbound,
            self.ws_connections.clone(),
            self.peers.clone(),
            self.event_tx.clone(),
            self.incoming_tx.clone(),
        );

        if !install_connection(
            &self.ws_connections,
            &self.network.local_identity().tailscale_id,
            peer_id,
            handle,
        )
        .await
        {
            // A simultaneous inbound connection won the deterministic
            // tie-break. Its hello already established the same peer session.
            return Ok(());
        }

        {
            let mut map = self.peers.write().await;
            let mut by_dev = self.by_device.write().await;
            if let Some(state) = map.get_mut(peer_id) {
                state.ws_connected = true;
            }
            if let Some(identity) = remote_identity {
                let outcomes = apply_identity(&mut map, &mut by_dev, peer_id, identity);
                emit_identity_outcomes(&self.event_tx, outcomes);
            }
        }

        let _ = self
            .event_tx
            .send(PeerEvent::WsConnected(peer_id.to_string()));

        Ok(())
    }

    /// One-shot identity exchange for an online peer (RFC 022 Phase C).
    ///
    /// No-ops if identity is already published or the peer is offline.
    pub async fn ensure_identity(&self, peer_id: &str) -> Result<(), SessionError> {
        {
            let map = self.peers.read().await;
            match map.get(peer_id) {
                Some(s) if s.published_device_id().is_some() => return Ok(()),
                Some(s) if !s.online => {
                    return Err(SessionError::PeerOffline(peer_id.to_string()));
                }
                None => return Err(SessionError::UnknownPeer(peer_id.to_string())),
                _ => {}
            }
        }
        self.ensure_ws_connected(peer_id).await
    }

    async fn resolve_routing_key(&self, peer_id: &str) -> Result<String, SessionError> {
        let map = self.peers.read().await;
        if map.contains_key(peer_id) {
            return Ok(peer_id.to_string());
        }

        // Published-ULID lookup goes through `by_device` — the first-wins
        // authoritative index — so a suppressed duplicate claimant can never
        // capture ULID-addressed traffic (RFC 022 §7.7). Never match a raw
        // `identity.device_id` here: suppressed identities are unpublished.
        {
            let by_dev = self.by_device.read().await;
            if let Some(ts) = by_dev.get(peer_id) {
                return Ok(ts.clone());
            }
        }

        if let Some(found) = map.values().find(|p| {
            p.name == peer_id
                || (!p.identity_suppressed
                    && p.identity
                        .as_ref()
                        .map(|i| i.device_name == peer_id)
                        .unwrap_or(false))
        }) {
            return Ok(found.id.clone());
        }

        // Peer-ref selector `{tailscale_id}:{generation}` — generation-checked
        // handle routing (RFC 022 I5). Checked last so identifiers that merely
        // look ref-shaped can still resolve above; a real ref that reaches
        // here is live (generation match), superseded, or departed — the
        // latter two must fail with PeerGone, never silently reach a
        // rejoined peer.
        if let Some((ts, generation)) = parse_peer_ref(peer_id) {
            return match map.get(ts) {
                Some(p) if p.generation == generation => Ok(ts.to_string()),
                _ => Err(SessionError::PeerGone(peer_id.to_string())),
            };
        }

        Err(SessionError::UnknownPeer(peer_id.to_string()))
    }

    /// Broadcast data to all peers with active WebSocket connections.
    ///
    /// Sends to all currently connected peers. Peers with no active
    /// connection are skipped (no lazy connect on broadcast).
    /// Errors from individual sends are logged but do not fail the broadcast.
    pub async fn broadcast(&self, data: &[u8]) -> BroadcastReport {
        let conns = self.ws_connections.read().await;
        let mut report = BroadcastReport {
            attempted: conns.len(),
            ..Default::default()
        };

        for (peer_id, handle) in conns.iter() {
            if handle.send_tx.send(data.to_vec()).await.is_err() {
                tracing::warn!(
                    peer_id = %peer_id,
                    "session: broadcast send failed (connection task closed)"
                );
                report.failed.push(peer_id.clone());
            } else {
                report.queued += 1;
            }
        }
        report
    }

    /// Subscribe to incoming messages from any connected peer.
    ///
    /// Returns a broadcast receiver that yields [`IncomingMessage`]s.
    /// Messages include the sender's peer ID and raw bytes — Layer 5
    /// does not interpret the payload.
    pub fn subscribe(&self) -> broadcast::Receiver<IncomingMessage> {
        self.incoming_tx.subscribe()
    }

    /// Test-only: inject an incoming message as if received from a peer,
    /// so tests can drive the envelope router without a real transport.
    #[cfg(test)]
    pub(crate) fn test_inject_incoming(&self, from: &str, data: Vec<u8>) {
        let _ = self.incoming_tx.send(IncomingMessage {
            from: from.to_string(),
            data,
            received_at: Instant::now(),
        });
    }

    /// Test-only: stamp a synthetic [`PeerIdentity`] onto an existing
    /// peer in the registry, simulating the effect of a completed hello
    /// exchange without running a real WebSocket handshake. Returns
    /// `true` if the peer was found and updated (including suppressed).
    #[doc(hidden)]
    pub async fn test_stamp_identity(&self, peer_id: &str, identity: PeerIdentity) -> bool {
        let mut map = self.peers.write().await;
        if !map.contains_key(peer_id) {
            return false;
        }
        let mut by_dev = self.by_device.write().await;
        let outcomes = apply_identity(&mut map, &mut by_dev, peer_id, identity);
        emit_identity_outcomes(&self.event_tx, outcomes);
        true
    }

    /// Look up the published Tailscale id for a durable device ULID, if any.
    #[doc(hidden)]
    pub async fn test_by_device(&self, device_id: &str) -> Option<String> {
        self.by_device.read().await.get(device_id).cloned()
    }

    /// The **published** durable device id of the peer routed by
    /// `tailscale_id`, if the registry knows the peer, it has completed a
    /// hello, and its ULID is not suppressed under first-wins (RFC 022
    /// §7.7). Subsystems that key state by device id (the synced store)
    /// bind an inbound message to its sender through this — never through
    /// a device id the payload names (RFC 025 §3.5).
    pub async fn published_device_id(&self, tailscale_id: &str) -> Option<String> {
        self.peers
            .read()
            .await
            .get(tailscale_id)
            .and_then(|state| state.published_device_id().map(str::to_string))
    }

    /// Disconnect a specific peer's WebSocket connection.
    ///
    /// Removes the cached connection and marks the peer as disconnected.
    /// Does not remove the peer from the registry (that only happens when
    /// Layer 3 emits a `Left` event).
    pub async fn disconnect(&self, peer_id: &str) {
        let handle = {
            let mut conns = self.ws_connections.write().await;
            conns.remove(peer_id)
        };

        if let Some(handle) = handle {
            // Signal the connection task to close. If the channel is already
            // closed (task exited), that's fine.
            let _ = handle.close_tx.send(()).await;
        }

        // Mark peer as disconnected
        {
            let mut map = self.peers.write().await;
            if let Some(state) = map.get_mut(peer_id) {
                state.ws_connected = false;
            }
        }

        let _ = self
            .event_tx
            .send(PeerEvent::WsDisconnected(peer_id.to_string()));
    }

    /// Close all active WebSocket connections and mark every peer as
    /// disconnected. Called by `Node::stop()` during teardown. Safe to call
    /// multiple times — with no active connections it is a no-op.
    pub async fn shutdown(&self) {
        // Stop the background loops first (peer-event + WS accept) so no
        // new connections or peer updates arrive mid-teardown. Abort is
        // safe: both loops only await channel/accept operations.
        for handle in self.background_tasks.lock().unwrap().drain(..) {
            handle.abort();
        }

        let handles: Vec<(String, WsConnectionHandle)> = {
            let mut conns = self.ws_connections.write().await;
            conns.drain().collect()
        };
        for (peer_id, handle) in handles {
            let _ = handle.close_tx.send(()).await;
            let _ = self.event_tx.send(PeerEvent::WsDisconnected(peer_id));
        }
        let mut map = self.peers.write().await;
        for state in map.values_mut() {
            state.ws_connected = false;
        }
    }
}

// ---------------------------------------------------------------------------
// Connection task — exclusively owns the WsFramedStream
// ---------------------------------------------------------------------------

/// Install one connection per peer. During simultaneous cross-dials, both
/// nodes deterministically keep the same physical connection: the
/// lexicographically smaller Tailscale id keeps its outbound side and the
/// larger id keeps the matching inbound side. A one-sided/non-preferred
/// connection is still accepted when no connection exists.
async fn install_connection(
    ws_connections: &Arc<RwLock<HashMap<String, WsConnectionHandle>>>,
    local_tailscale_id: &str,
    peer_id: &str,
    handle: WsConnectionHandle,
) -> bool {
    let preferred = preferred_connection_direction(local_tailscale_id, peer_id);

    let (installed, to_close) = {
        let mut conns = ws_connections.write().await;
        let replace = match conns.get(peer_id) {
            None => true,
            Some(existing) => {
                should_replace_connection(existing.direction, handle.direction, preferred)
            }
        };
        if replace {
            (true, conns.insert(peer_id.to_string(), handle))
        } else {
            (false, Some(handle))
        }
    };

    if let Some(old) = to_close {
        let _ = old.close_tx.try_send(());
    }
    installed
}

fn preferred_connection_direction(
    local_tailscale_id: &str,
    peer_id: &str,
) -> Option<ConnectionDirection> {
    if local_tailscale_id.is_empty() || local_tailscale_id == peer_id {
        None
    } else if local_tailscale_id < peer_id {
        Some(ConnectionDirection::Outbound)
    } else {
        Some(ConnectionDirection::Inbound)
    }
}

fn should_replace_connection(
    existing: ConnectionDirection,
    candidate: ConnectionDirection,
    preferred: Option<ConnectionDirection>,
) -> bool {
    preferred == Some(candidate) && preferred != Some(existing)
}

/// Spawn a background task that exclusively owns a `WsFramedStream`.
///
/// The task uses `tokio::select!` to multiplex between:
/// - Receiving outgoing data from the `send_rx` channel and writing to the WS
/// - Reading incoming data from the WS and forwarding to `incoming_tx`
/// - Receiving a close signal from `close_rx`
///
/// When the task exits (stream closed, error, or close signal), it cleans up
/// the connection from the registry and emits a `Disconnected` event.
///
/// Returns a [`WsConnectionHandle`] for the caller to send data and close.
fn spawn_connection_task(
    stream: WsFramedStream,
    peer_id: String,
    direction: ConnectionDirection,
    ws_connections: Arc<RwLock<HashMap<String, WsConnectionHandle>>>,
    peers: Arc<RwLock<HashMap<String, PeerState>>>,
    event_tx: broadcast::Sender<PeerEvent>,
    incoming_tx: broadcast::Sender<IncomingMessage>,
) -> WsConnectionHandle {
    let (send_tx, mut send_rx) = mpsc::channel::<Vec<u8>>(256);
    let (close_tx, mut close_rx) = mpsc::channel::<()>(1);

    let handle = WsConnectionHandle {
        send_tx: send_tx.clone(),
        close_tx: close_tx.clone(),
        peer_id: peer_id.clone(),
        connected_at: Instant::now(),
        direction,
    };
    let task_send_tx = send_tx.clone();

    // RFC 022 §7.5: attribute inbound traffic by the WhoIs-verified
    // Tailscale stable id of this connection — never the self-declared ULID.
    let from_tailscale_id = peer_id.clone();

    tokio::spawn(async move {
        let mut stream = stream;
        let mut closed = false;

        loop {
            tokio::select! {
                // Outgoing: data from send channel → write to WS
                Some(data) = send_rx.recv() => {
                    if let Err(e) = stream.send(&data).await {
                        tracing::warn!(
                            peer_id = %peer_id,
                            error = %e,
                            "session: WS send error"
                        );
                        break;
                    }
                }

                // Incoming: data from WS → forward to incoming channel
                result = stream.recv() => {
                    match result {
                        Ok(Some(data)) => {
                            let msg = IncomingMessage {
                                from: from_tailscale_id.clone(),
                                data,
                                received_at: Instant::now(),
                            };
                            let _ = incoming_tx.send(msg);
                        }
                        Ok(None) => {
                            tracing::info!(
                                peer_id = %peer_id,
                                "session: WS stream closed"
                            );
                            break;
                        }
                        Err(e) => {
                            tracing::warn!(
                                peer_id = %peer_id,
                                error = %e,
                                "session: WS recv error"
                            );
                            break;
                        }
                    }
                }

                // Close signal
                _ = close_rx.recv() => {
                    tracing::info!(
                        peer_id = %peer_id,
                        "session: connection close requested"
                    );
                    closed = true;
                    let _ = stream.close().await;
                    break;
                }
            }
        }

        // Clean up: remove connection from registry, mark peer as disconnected
        // Only clean up if we weren't explicitly closed (disconnect() handles
        // its own cleanup to avoid racing).
        if !closed {
            let owned_slot = {
                let mut conns = ws_connections.write().await;
                let owns_slot = conns
                    .get(&peer_id)
                    .map(|handle| handle.send_tx.same_channel(&task_send_tx))
                    .unwrap_or(false);
                if owns_slot {
                    conns.remove(&peer_id);
                }
                owns_slot
            };
            if owned_slot {
                let mut map = peers.write().await;
                if let Some(state) = map.get_mut(&peer_id) {
                    state.ws_connected = false;
                }
                let _ = event_tx.send(PeerEvent::WsDisconnected(peer_id));
            }
        }
    });

    handle
}

// ---------------------------------------------------------------------------
// Eager identity scheduling (RFC 022 Phase C)
// ---------------------------------------------------------------------------

/// Deterministic per-peer delay in `0..window_ms` for staggering eager dials
/// (RFC 022 §8.1).
///
/// truffle-core carries no `rand` dependency, so the stagger is a hash of the
/// peer id (`DefaultHasher`, fixed-seed SipHash) folded into the window rather
/// than a random draw. The properties the eager scheduler relies on:
///
/// - **Bounded:** always `< window_ms`, and exactly `Duration::ZERO` when
///   `window_ms == 0` — which both disables jitter (tests use it to keep eager
///   timing deterministic) and avoids a `% 0` panic.
/// - **Stable per peer:** one peer id always maps to the same delay for the
///   life of the process, so a re-scheduled peer does not thrash.
/// - **Spread across peers:** distinct ids land on different offsets, which is
///   the whole point — it breaks up the synchronized first-dial burst when a
///   node joins a large mesh. Decorrelating the *same* peer across different
///   local nodes is out of scope (the herd this targets is one node's own
///   outbound burst); the semaphore bounds concurrency regardless.
fn eager_jitter_delay(peer_id: &str, window_ms: u64) -> Duration {
    if window_ms == 0 {
        return Duration::ZERO;
    }
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut h = DefaultHasher::new();
    peer_id.hash(&mut h);
    Duration::from_millis(h.finish() % window_ms)
}

/// Arcs needed to dial for identity without holding `&PeerRegistry`.
struct EagerScheduleCtx<N: NetworkProvider + 'static> {
    eager_identity: bool,
    /// RFC 025 §3.3: mirrors `PeerRegistry::login_gated`.
    login_gated: bool,
    peers: Arc<RwLock<HashMap<String, PeerState>>>,
    by_device: Arc<RwLock<HashMap<String, String>>>,
    ws_connections: Arc<RwLock<HashMap<String, WsConnectionHandle>>>,
    peer_backoffs: Arc<RwLock<HashMap<String, ReconnectBackoff>>>,
    connecting: Arc<RwLock<HashSet<String>>>,
    event_tx: broadcast::Sender<PeerEvent>,
    incoming_tx: broadcast::Sender<IncomingMessage>,
    ws_transport: Arc<WebSocketTransport<N>>,
    network: Arc<N>,
    eager_identity_sem: Arc<Semaphore>,
    eager_identity_jitter_ms: u64,
    identity_inflight: Arc<AsyncMutex<HashSet<String>>>,
    background_tasks: Arc<std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>>,
}

impl<N: NetworkProvider + 'static> EagerScheduleCtx<N> {
    fn schedule(&self, peer_id: String) {
        if !self.eager_identity {
            return;
        }

        let peers = self.peers.clone();
        let by_device = self.by_device.clone();
        let ws_connections = self.ws_connections.clone();
        let peer_backoffs = self.peer_backoffs.clone();
        let connecting = self.connecting.clone();
        let event_tx = self.event_tx.clone();
        let incoming_tx = self.incoming_tx.clone();
        let ws_transport = self.ws_transport.clone();
        let network = self.network.clone();
        let sem = self.eager_identity_sem.clone();
        let jitter_ms = self.eager_identity_jitter_ms;
        let inflight = self.identity_inflight.clone();
        let connect_ctx = EagerConnectCtx {
            peers: peers.clone(),
            by_device,
            ws_connections,
            peer_backoffs: peer_backoffs.clone(),
            connecting,
            event_tx,
            incoming_tx,
            ws_transport,
            network,
            login_gated: self.login_gated,
        };

        let handle = tokio::spawn(async move {
            // Dedupe inside the task so a contended try-lock can never create
            // two active retry loops for the same peer.
            {
                let mut set = inflight.lock().await;
                if !set.insert(peer_id.clone()) {
                    return;
                }
            }

            // RFC 022 §8.1: stagger the first burst of eager hellos with a
            // bounded per-peer delay applied *before* acquiring the dial
            // permit, so a node joining a large mesh does not fire its first
            // `eager_identity_concurrency` hellos simultaneously. Waiting
            // before the semaphore (not after) means we never hold a dial slot
            // just to idle, and it spreads arrival at the semaphore so even the
            // very first dials are staggered. The delay is derived from the
            // peer id (no `rand` dependency); only this eager path waits — app
            // `send` never does.
            let delay = eager_jitter_delay(&peer_id, jitter_ms);
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }

            loop {
                // Stop retrying once identity is learned or discovery says the
                // peer is gone/offline.
                let needs = {
                    let map = peers.read().await;
                    match map.get(&peer_id) {
                        Some(s) => s.online && s.published_device_id().is_none(),
                        None => false,
                    }
                };
                if !needs {
                    break;
                }

                let permit = match sem.acquire().await {
                    Ok(p) => p,
                    Err(_) => break,
                };

                // Build a temporary view with the same connection logic as
                // PeerRegistry::ensure_ws_connected (duplicated fields).
                let result = eager_connect_ws(&peer_id, &connect_ctx).await;
                drop(permit);

                match result {
                    Ok(()) => break,
                    Err(SessionError::UnknownPeer(_) | SessionError::PeerOffline(_)) => break,
                    Err(e) => {
                        tracing::debug!(
                        peer_id = %peer_id,
                        error = %e,
                        "session: eager identity dial failed; retrying"
                        );
                    }
                }

                let retry_after = {
                    let backoffs = peer_backoffs.read().await;
                    backoffs
                        .get(&peer_id)
                        .map(ReconnectBackoff::retry_after)
                        .unwrap_or_else(|| Duration::from_millis(100))
                        .max(Duration::from_millis(25))
                };
                tokio::time::sleep(retry_after).await;
            }

            let mut set = inflight.lock().await;
            set.remove(&peer_id);
        });
        let mut tasks = self.background_tasks.lock().unwrap();
        tasks.retain(|task| !task.is_finished());
        tasks.push(handle);
    }
}

struct EagerConnectCtx<N: NetworkProvider + 'static> {
    peers: Arc<RwLock<HashMap<String, PeerState>>>,
    by_device: Arc<RwLock<HashMap<String, String>>>,
    ws_connections: Arc<RwLock<HashMap<String, WsConnectionHandle>>>,
    peer_backoffs: Arc<RwLock<HashMap<String, ReconnectBackoff>>>,
    connecting: Arc<RwLock<HashSet<String>>>,
    event_tx: broadcast::Sender<PeerEvent>,
    incoming_tx: broadcast::Sender<IncomingMessage>,
    ws_transport: Arc<WebSocketTransport<N>>,
    network: Arc<N>,
    /// RFC 025 §3.3: mirrors `PeerRegistry::login_gated`.
    login_gated: bool,
}

/// Connection path shared by eager identity (no send payload).
async fn eager_connect_ws<N: NetworkProvider + 'static>(
    peer_id: &str,
    ctx: &EagerConnectCtx<N>,
) -> Result<(), SessionError> {
    {
        let conns = ctx.ws_connections.read().await;
        if conns.contains_key(peer_id) {
            return Ok(());
        }
    }

    let peer_addr = {
        let map = ctx.peers.read().await;
        let state = map
            .get(peer_id)
            .ok_or_else(|| SessionError::UnknownPeer(peer_id.to_string()))?;
        if !state.online {
            return Err(SessionError::PeerOffline(peer_id.to_string()));
        }
        if state.published_device_id().is_some() {
            return Ok(());
        }
        // RFC 025 §3.3: the eager dial inherits the gate — an unresolved
        // login is exactly the case where we must not volunteer a hello.
        if ctx.login_gated && state.login_name.is_none() {
            return Err(SessionError::LoginUnknown(peer_id.to_string()));
        }
        PeerAddr {
            ip: Some(state.ip),
            hostname: state.name.clone(),
            dns_name: None,
        }
    };

    {
        let backoffs = ctx.peer_backoffs.read().await;
        if let Some(backoff) = backoffs.get(peer_id) {
            if backoff.should_retry().is_none() {
                return Err(SessionError::ReconnectBackoff {
                    retry_after: backoff.retry_after(),
                });
            }
        }
    }

    {
        let mut connecting_g = ctx.connecting.write().await;
        if connecting_g.contains(peer_id) {
            return Err(SessionError::ConnectFailed(
                "connection already in progress".to_string(),
            ));
        }
        connecting_g.insert(peer_id.to_string());
    }

    tracing::info!(peer_id = %peer_id, "session: eager identity connecting WS");

    let connect_result = ctx.ws_transport.connect(&peer_addr).await;

    {
        let mut connecting_g = ctx.connecting.write().await;
        connecting_g.remove(peer_id);
    }

    let ws_stream = match connect_result {
        Ok(stream) => {
            let mut backoffs = ctx.peer_backoffs.write().await;
            backoffs
                .entry(peer_id.to_string())
                .or_insert_with(ReconnectBackoff::new)
                .success();
            stream
        }
        Err(e) => {
            let mut backoffs = ctx.peer_backoffs.write().await;
            backoffs
                .entry(peer_id.to_string())
                .or_insert_with(ReconnectBackoff::new)
                .failure();
            return Err(SessionError::ConnectFailed(e.to_string()));
        }
    };

    // RFC 022 §7.5, dial side: same claimed-vs-dialed check as
    // `ensure_ws_connected` — an eager hello must never adopt an identity
    // from an answerer that is not the peer it dialed.
    if let Some(claimed) = ws_stream.remote_identity() {
        if claimed.tailscale_id != peer_id {
            tracing::warn!(
                peer_id = %peer_id,
                claimed = %claimed.tailscale_id,
                "session: eager dial answered as a different tailscale_id; dropping connection"
            );
            return Err(SessionError::ConnectFailed(format!(
                "hello identity mismatch: dialed {peer_id}, answerer claims {}",
                claimed.tailscale_id
            )));
        }
    }

    let remote_identity = ws_stream.remote_identity().cloned();

    let handle = spawn_connection_task(
        ws_stream,
        peer_id.to_string(),
        ConnectionDirection::Outbound,
        ctx.ws_connections.clone(),
        ctx.peers.clone(),
        ctx.event_tx.clone(),
        ctx.incoming_tx.clone(),
    );

    if !install_connection(
        &ctx.ws_connections,
        &ctx.network.local_identity().tailscale_id,
        peer_id,
        handle,
    )
    .await
    {
        // The matching inbound connection won the cross-dial tie-break.
        return Ok(());
    }

    {
        let mut map = ctx.peers.write().await;
        let mut by_dev = ctx.by_device.write().await;
        if let Some(state) = map.get_mut(peer_id) {
            state.ws_connected = true;
        }
        if let Some(identity) = remote_identity {
            let outcomes = apply_identity(&mut map, &mut by_dev, peer_id, identity);
            emit_identity_outcomes(&ctx.event_tx, outcomes);
        }
    }

    let _ = ctx
        .event_tx
        .send(PeerEvent::WsConnected(peer_id.to_string()));
    Ok(())
}

// ---------------------------------------------------------------------------
// Helper: convert NetworkPeer to PeerState
// ---------------------------------------------------------------------------

/// Convert a Layer 3 `NetworkPeer` to a Layer 5 `PeerState`.
///
/// Sets `ws_connected: false` by default — connections are managed by Layer 5,
/// not by Layer 3 discovery. `generation` must be supplied by the registry
/// (bumped per re-join of the same Tailscale id).
fn network_peer_to_state(peer: &NetworkPeer, generation: u64) -> PeerState {
    let connection_type = if let Some(ref relay) = peer.relay {
        format!("relay:{relay}")
    } else if peer.cur_addr.is_some() {
        "direct".to_string()
    } else {
        "unknown".to_string()
    };

    PeerState {
        id: peer.id.clone(),
        generation,
        name: peer.hostname.clone(),
        ip: peer.ip,
        online: peer.online,
        ws_connected: false,
        connection_type,
        os: peer.os.clone(),
        last_seen: peer.last_seen.clone(),
        identity: None,
        identity_suppressed: false,
        login_name: peer.login_name.clone(),
    }
}

// ---------------------------------------------------------------------------
// Identity application (RFC 022 §7.7)
// ---------------------------------------------------------------------------

/// Side effects from applying a hello identity to a registry entry.
#[derive(Debug)]
enum IdentityOutcome {
    /// Emit `PeerEvent::Identity` for this snapshot.
    Identity(PeerState),
    /// Retire a ghost entry (same ULID, offline holder) before the new claim.
    /// Carries the ghost's final state for the synthesized `Left` event.
    GhostLeft(PeerState),
    /// Emit `PeerEvent::Updated` — identity metadata (name/os) changed on a
    /// re-hello without the ULID changing (no second `identity`, RFC 022 §8).
    Updated(PeerState),
}

/// Apply `identity` to the peer at `ts_id`, updating `by_device` under
/// first-wins / ghost-retire / rotation rules.
fn apply_identity(
    peers: &mut HashMap<String, PeerState>,
    by_device: &mut HashMap<String, String>,
    ts_id: &str,
    identity: PeerIdentity,
) -> Vec<IdentityOutcome> {
    let mut outcomes = Vec::new();
    let uid = identity.device_id.clone();

    let Some(state) = peers.get(ts_id) else {
        return outcomes;
    };

    // Rotation: same entry, different ULID already published.
    if let Some(prev) = state.published_device_id() {
        if prev != uid.as_str() {
            by_device.remove(prev);
        } else if !state.identity_suppressed {
            // Same ULID already published — refresh metadata silently:
            // RFC 022 §8 says later confirmations emit no second `identity`
            // (every WS reconnect re-hellos). A changed name/os surfaces as
            // `updated` instead.
            if let Some(s) = peers.get_mut(ts_id) {
                let changed = s.identity.as_ref() != Some(&identity);
                s.identity = Some(identity);
                s.identity_suppressed = false;
                if changed {
                    outcomes.push(IdentityOutcome::Updated(s.clone()));
                }
            }
            return outcomes;
        }
    }

    match by_device.get(&uid).cloned() {
        Some(holder) if holder == ts_id => {
            // Already the published owner — update identity block.
            if let Some(s) = peers.get_mut(ts_id) {
                s.identity = Some(identity);
                s.identity_suppressed = false;
                outcomes.push(IdentityOutcome::Identity(s.clone()));
            }
        }
        Some(holder) => {
            let holder_online = peers.get(&holder).map(|p| p.online).unwrap_or(false);
            if holder_online {
                // First-wins: store identity but suppress publication.
                tracing::warn!(
                    device_id = %uid,
                    holder = %holder,
                    claimant = %ts_id,
                    "session: duplicate-device-id — first-wins, suppressing claimant"
                );
                if let Some(s) = peers.get_mut(ts_id) {
                    s.identity = Some(identity);
                    s.identity_suppressed = true;
                    // No Identity event for suppressed claim (deviceId stays null).
                }
            } else {
                // Ghost retire: offline holder loses the ULID.
                if let Some(mut ghost) = peers.remove(&holder) {
                    ghost.online = false;
                    ghost.ws_connected = false;
                    outcomes.push(IdentityOutcome::GhostLeft(ghost));
                }
                by_device.insert(uid.clone(), ts_id.to_string());
                if let Some(s) = peers.get_mut(ts_id) {
                    s.identity = Some(identity);
                    s.identity_suppressed = false;
                    outcomes.push(IdentityOutcome::Identity(s.clone()));
                }
            }
        }
        None => {
            by_device.insert(uid, ts_id.to_string());
            if let Some(s) = peers.get_mut(ts_id) {
                s.identity = Some(identity);
                s.identity_suppressed = false;
                outcomes.push(IdentityOutcome::Identity(s.clone()));
            }
        }
    }

    outcomes
}

fn emit_identity_outcomes(event_tx: &broadcast::Sender<PeerEvent>, outcomes: Vec<IdentityOutcome>) {
    for o in outcomes {
        match o {
            IdentityOutcome::GhostLeft(state) => {
                let _ = event_tx.send(PeerEvent::Left(state));
            }
            IdentityOutcome::Identity(state) => {
                let _ = event_tx.send(PeerEvent::Identity(state));
            }
            IdentityOutcome::Updated(state) => {
                let _ = event_tx.send(PeerEvent::Updated(state));
            }
        }
    }
}
