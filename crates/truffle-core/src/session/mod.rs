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
mod connection_tests;
#[cfg(test)]
mod tests;

use std::collections::{HashMap, HashSet};
use std::net::IpAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, PoisonError};
use std::time::{Duration, Instant};

use tokio::sync::{broadcast, mpsc, watch, Mutex as AsyncMutex, RwLock, Semaphore};

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

/// The exact [`SessionError::ConnectFailed`] message a caller gets when the
/// dial it waited on was cancelled because the session is shutting down.
///
/// No retry can succeed after it. Compare the whole message: other
/// `ConnectFailed` messages embed strings the remote peer chooses (its
/// claimed id, its app id), so a substring match could be spoofed — but
/// none of them can equal this one, since each starts with a fixed prefix
/// of its own.
pub const DIAL_CANCELLED_SHUTTING_DOWN: &str = "dial cancelled: the session is shutting down";

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
    connected_at: Instant,
    /// Whether this node dialed or accepted the connection.
    direction: ConnectionDirection,
    /// The hello identity the remote presented on this connection. Applied
    /// to the registry when the connection becomes the peer's primary.
    identity: Option<PeerIdentity>,
    /// When the peer was last known to be admitted for this connection:
    /// the start of the dial (outbound: the registry entry was checked just
    /// before) or when the listener took it with its WhoIs identity
    /// (inbound). A Layer 3 departure at or after this instant means the
    /// connection must not be installed (see [`ConnCtx::departed`]).
    admitted_at: Instant,
    /// State shared with the connection task.
    flags: Arc<ConnectionFlags>,
}

/// Per-connection flags shared between a [`WsConnectionHandle`] and its
/// connection task.
///
/// `standby` and `seen_data` form a Dekker pair (both `SeqCst`): the task
/// stores `seen_data` and then loads `standby`; `install_connection` stores
/// `standby` and then loads `seen_data`. Whichever runs second sees the
/// other's write, so a connection that carries data is never left parked
/// (see [`install_connection`]).
///
/// `ended` closes a second race: a connection whose task finishes before
/// `install_connection` runs (the peer closed it at once) must not be
/// installed. The task stores `ended` before it takes the table lock to
/// retire itself, and `install_connection` checks it under that lock, so
/// either the retire finds the installed handle or the install sees the
/// flag.
///
/// `parkings` counts the times the connection was put in the standby slot.
/// A grace timer acts only on the parking it was armed for, so a timer left
/// over from an earlier parking (the connection was promoted, then demoted
/// again) never promotes or closes it.
#[derive(Debug, Default)]
struct ConnectionFlags {
    /// True while the connection is parked in the standby slot.
    standby: AtomicBool,
    /// True once the remote has sent at least one data frame on it.
    seen_data: AtomicBool,
    /// True once the connection task has left its loop.
    ended: AtomicBool,
    /// How many times the connection has been parked.
    parkings: AtomicU64,
}

/// Why a connection sits in the standby slot: decides what its grace timer
/// does (see [`install_connection`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParkedAs {
    /// The peer's new connection, parked behind the primary it may replace.
    /// The timer promotes it.
    Candidate,
    /// A primary displaced by a promoted standby. The timer closes it; it is
    /// kept until then because, in a cross-dial, the peer may still hold it.
    Demoted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConnectionDirection {
    Inbound,
    Outbound,
}

/// A peer's connection table: at most one entry per Tailscale id.
type ConnMap = Arc<RwLock<HashMap<String, WsConnectionHandle>>>;

// ---------------------------------------------------------------------------
// Connection-management tunables
// ---------------------------------------------------------------------------

/// How long a parked standby connection (see [`install_connection`]) waits
/// for the peer to settle a cross-dial before it is promoted over the
/// connection it shadows — and how long a primary that a promotion demoted
/// is kept before it is closed.
///
/// In a genuine cross-dial the peer refuses the standby's twin within about
/// one round trip, which closes the standby long before this fires. A
/// standby still open after the grace period belongs to a peer that does
/// not hold the shadowed connection any more.
const STANDBY_GRACE: Duration = Duration::from_secs(3);

/// The oldest a connection can be and still have a simultaneous cross-dial
/// twin.
///
/// A node dials only when it holds no connection to the peer, so a twin
/// must have been started before the peer installed the existing
/// connection — and a dial cannot outlive the provider's TCP dial timeout
/// (30 s for the Tailscale provider) plus the accepting side's handshake
/// timeout (10 s by default). An inbound connection that arrives while the
/// existing one is older than this cannot be a twin: the peer has lost (or
/// restarted out from under) the existing connection, which is replaced at
/// once instead of waiting out [`STANDBY_GRACE`].
const CROSS_DIAL_WINDOW: Duration = Duration::from_secs(45);

/// How long a connection task waits for its close frame to go out.
const CLOSE_TIMEOUT: Duration = Duration::from_secs(2);

/// Upper bound on one session-driven dial (TCP dial, WS upgrade and hello).
///
/// Every caller that joins a dial waits for its outcome, so the dial itself
/// must end: the client side of the WS upgrade has no timeout of its own.
const SESSION_DIAL_TIMEOUT: Duration = Duration::from_secs(45);

/// How long a Layer 3 departure is remembered (see [`ConnCtx::departed`]).
/// Comfortably longer than any connection can take from admission to
/// install: a dial ends within [`SESSION_DIAL_TIMEOUT`], and an accepted
/// stream waits only for the session's accept loop.
const DEPARTURE_MEMORY: Duration = Duration::from_secs(120);

// ---------------------------------------------------------------------------
// Shared connection state
// ---------------------------------------------------------------------------

/// The registry state a connection task (and anything that installs,
/// promotes or retires connections) works on.
#[derive(Clone)]
struct ConnCtx {
    /// The primary connection per peer: the one sends use.
    ws_connections: ConnMap,
    /// A parked inbound connection per peer, waiting for a cross-dial to
    /// settle (see [`install_connection`]). Never used for sends.
    ///
    /// Lock order: `ws_connections` before `standby`, and neither is held
    /// while taking `pending_identities`, `peers` or `by_device`. The other
    /// way round is fine: the connected/disconnected bookkeeping and the
    /// Layer 3 handlers read `ws_connections` while holding `peers` (and
    /// `pending_identities` / `by_device`), so that state is decided under
    /// the `peers` lock.
    standby: ConnMap,
    peers: Arc<RwLock<HashMap<String, PeerState>>>,
    by_device: Arc<RwLock<HashMap<String, String>>>,
    pending_identities: Arc<RwLock<HashMap<String, PeerIdentity>>>,
    /// When Layer 3 last said each peer `Left` (kept for
    /// [`DEPARTURE_MEMORY`]). A connection admitted before that instant —
    /// a dial already in flight, or a hello verified against a WhoIs that
    /// predates the departure — is refused at install, so a peer Layer 3
    /// evicted (a refused login, say) cannot slip back in through it.
    ///
    /// Written by the `Left` handler while it holds `ws_connections`, and
    /// read by [`install_connection`] under the same lock, so a connection
    /// is either swept by the departure or refused by it. A std mutex:
    /// never held across an await.
    departed: Arc<std::sync::Mutex<HashMap<String, Instant>>>,
    event_tx: broadcast::Sender<PeerEvent>,
    incoming_tx: broadcast::Sender<IncomingMessage>,
    /// [`STANDBY_GRACE`], shortened by tests.
    standby_grace: Duration,
}

impl ConnCtx {
    /// Did Layer 3 report `peer_id` gone at or after `admitted_at`?
    fn departed_since(&self, peer_id: &str, admitted_at: Instant) -> bool {
        self.departed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(peer_id)
            .is_some_and(|left| *left >= admitted_at)
    }

    /// Record a Layer 3 departure of `peer_id` now (and forget old ones).
    fn record_departure(&self, peer_id: &str) {
        let now = Instant::now();
        let mut departed = self.departed.lock().unwrap_or_else(PoisonError::into_inner);
        departed.retain(|_, left| now.duration_since(*left) < DEPARTURE_MEMORY);
        departed.insert(peer_id.to_string(), now);
    }
}

/// Outcome of a dial, shared with every caller waiting on it.
#[derive(Debug, Clone)]
enum DialOutcome {
    /// A connection to the peer is in place (this dial's, or one that won
    /// the tie-break against it).
    Connected,
    /// The dial failed; carries the [`SessionError::ConnectFailed`] message.
    Failed(String),
}

/// A dial in flight. Callers that find one wait on `rx` for its outcome
/// instead of dialing again.
struct InflightDial {
    /// Distinguishes this dial from a later one for the same peer.
    id: u64,
    rx: watch::Receiver<Option<DialOutcome>>,
}

/// In-flight dials by Tailscale id. A std mutex: it is never held across
/// an await.
type DialMap = Arc<std::sync::Mutex<HashMap<String, InflightDial>>>;

/// Source of [`InflightDial::id`].
static NEXT_DIAL_ID: AtomicU64 = AtomicU64::new(1);

/// Why the session is dialing (for logs).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DialReason {
    /// An application `send` / `ensure_ws_connected`.
    App,
    /// RFC 022 Phase C eager identity exchange.
    EagerIdentity,
}

/// Everything a dial needs, detached from `&PeerRegistry` so a dial can run
/// as its own task (and the eager-identity scheduler can start one).
struct DialCtx<N: NetworkProvider + 'static> {
    conn: ConnCtx,
    network: Arc<N>,
    ws_transport: Arc<WebSocketTransport<N>>,
    peer_backoffs: Arc<RwLock<HashMap<String, ReconnectBackoff>>>,
    dials: DialMap,
    background_tasks: Arc<std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>>,
    /// RFC 025 §3.3: mirrors `PeerRegistry::login_gated`.
    login_gated: bool,
}

impl<N: NetworkProvider + 'static> Clone for DialCtx<N> {
    fn clone(&self) -> Self {
        Self {
            conn: self.conn.clone(),
            network: self.network.clone(),
            ws_transport: self.ws_transport.clone(),
            peer_backoffs: self.peer_backoffs.clone(),
            dials: self.dials.clone(),
            background_tasks: self.background_tasks.clone(),
            login_gated: self.login_gated,
        }
    }
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

    /// Parked standby connections (see [`install_connection`]).
    standby_connections: ConnMap,

    /// Recent Layer 3 departures (see [`ConnCtx::departed`]).
    departed: Arc<std::sync::Mutex<HashMap<String, Instant>>>,

    /// Dials in flight, by peer. Concurrent callers join the dial already
    /// running instead of starting (or refusing) another.
    dials: DialMap,

    /// [`STANDBY_GRACE`]; tests shorten it before `start()`.
    standby_grace: Duration,

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
            standby_connections: Arc::new(RwLock::new(HashMap::new())),
            departed: Arc::new(std::sync::Mutex::new(HashMap::new())),
            dials: Arc::new(std::sync::Mutex::new(HashMap::new())),
            standby_grace: STANDBY_GRACE,
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

    /// Test-only: shorten [`STANDBY_GRACE`]. Call before [`Self::start`].
    #[cfg(test)]
    pub(crate) fn set_standby_grace(&mut self, grace: Duration) {
        self.standby_grace = grace;
    }

    /// The connection-table state shared with connection tasks.
    fn conn_ctx(&self) -> ConnCtx {
        ConnCtx {
            ws_connections: self.ws_connections.clone(),
            standby: self.standby_connections.clone(),
            peers: self.peers.clone(),
            by_device: self.by_device.clone(),
            pending_identities: self.pending_identities.clone(),
            departed: self.departed.clone(),
            event_tx: self.event_tx.clone(),
            incoming_tx: self.incoming_tx.clone(),
            standby_grace: self.standby_grace,
        }
    }

    /// Everything a (possibly detached) dial needs.
    fn dial_ctx(&self) -> DialCtx<N> {
        DialCtx {
            conn: self.conn_ctx(),
            network: self.network.clone(),
            ws_transport: self.ws_transport.clone(),
            peer_backoffs: self.peer_backoffs.clone(),
            dials: self.dials.clone(),
            background_tasks: self.background_tasks.clone(),
            login_gated: self.login_gated,
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

    // @atlas step discovery.registry
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
        let standby_connections = self.standby_connections.clone();
        let conn = self.conn_ctx();
        // Clones for scheduling eager identity from the event loop.
        let schedule_ctx = EagerScheduleCtx {
            eager_identity: self.eager_identity,
            dial: self.dial_ctx(),
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
                        let peer_id = network_peer.id.clone();
                        let pending_identity = {
                            let mut pending = pending_identities.write().await;
                            pending.remove(&peer_id)
                        };

                        let (peer_event, identity_outcomes) = {
                            let mut map = peers.write().await;
                            // Read under `peers`, which every connected /
                            // disconnected write holds, so a connection
                            // installed or retired meanwhile is not missed.
                            state.ws_connected = ws_connections.read().await.contains_key(&peer_id);
                            let peer_event = PeerEvent::Joined(state.clone());
                            map.insert(peer_id.clone(), state);
                            let outcomes = if let Some(identity) = pending_identity {
                                let mut by_dev = by_device.write().await;
                                apply_identity(&mut map, &mut by_dev, &peer_id, identity)
                            } else {
                                Vec::new()
                            };
                            (peer_event, outcomes)
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
                        // Order matters. The entry goes first, so no dial can
                        // start for the peer after the departure is stamped;
                        // then the connections are swept and the departure
                        // stamped under the same lock, so a connection
                        // admitted before it (a dial in flight, a hello
                        // verified against an older WhoIs) is either swept
                        // here or refused when it tries to install; the
                        // pending identity goes last, after anything that
                        // could still have stashed one.
                        let removed = {
                            let mut map = peers.write().await;
                            map.remove(&peer_id)
                        };
                        let (handle, parked) = {
                            let mut conns = ws_connections.write().await;
                            let mut standby = standby_connections.write().await;
                            conn.record_departure(&peer_id);
                            (conns.remove(&peer_id), standby.remove(&peer_id))
                        };
                        pending_identities.write().await.remove(&peer_id);
                        if let Some(parked) = parked {
                            let _ = parked.close_tx.try_send(());
                        }
                        if let Some(handle) = handle {
                            let _ = handle.close_tx.send(()).await;
                            // Emit Disconnected before Left
                            let _ = event_tx.send(PeerEvent::WsDisconnected(peer_id.clone()));
                            tracing::info!(
                                peer_id = %peer_id,
                                "session: closed WS connection for departing peer"
                            );
                        }

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
        let ctx = self.conn_ctx();
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
                        let admitted_at = stream.admitted_at();
                        let handle = spawn_connection_task(
                            stream,
                            peer_id.clone(),
                            ConnectionDirection::Inbound,
                            admitted_at,
                            ctx.clone(),
                        );
                        let send_tx = handle.send_tx.clone();

                        match install_connection(&ctx, &local_tailscale_id, &peer_id, handle).await
                        {
                            // Mark the peer connected and apply the hello's
                            // identity (or keep it pending until discovery).
                            Installed::Primary => {
                                on_primary_installed(
                                    &ctx,
                                    &peer_id,
                                    &send_tx,
                                    ConnectionDirection::Inbound,
                                    remote_identity,
                                )
                                .await;
                            }
                            Installed::Departed => {
                                tracing::info!(
                                    peer_id = %peer_id,
                                    "session: refused incoming WS connection admitted \
                                     before Layer 3 reported the peer gone"
                                );
                            }
                            Installed::Standby => {
                                tracing::info!(
                                    peer_id = %peer_id,
                                    "session: parked incoming WS connection as standby \
                                     (existing connection is the preferred cross-dial side)"
                                );
                            }
                            Installed::Refused => {
                                tracing::debug!(
                                    peer_id = %peer_id,
                                    "session: rejected duplicate incoming WS connection"
                                );
                            }
                        }
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

    // @atlas step send.route
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
        let mut data = data.to_vec();

        // A connection can vanish between `ensure_ws_connected` and the send
        // (the peer closed it, or it lost a cross-dial tie-break and was
        // replaced). Nothing was written in that case, so connect once more
        // before giving up.
        let mut last_err = SessionError::SendFailed("connection missing after connect".into());
        for _ in 0..2 {
            self.ensure_ws_connected(&peer_id).await?;
            // Clone the sender out: never hold the connection table across a
            // send that can wait on a full queue.
            let send_tx = self
                .ws_connections
                .read()
                .await
                .get(&peer_id)
                .map(|handle| handle.send_tx.clone());
            let Some(send_tx) = send_tx else {
                last_err = SessionError::SendFailed("connection missing after connect".into());
                continue;
            };
            match send_tx.send(data).await {
                Ok(()) => return Ok(()),
                Err(mpsc::error::SendError(unsent)) => {
                    data = unsent;
                    last_err = SessionError::SendFailed("connection task closed".to_string());
                }
            }
        }
        Err(last_err)
    }

    /// Ensure a WS session exists to `peer_id` (Tailscale routing key).
    /// Completes the RFC 017 hello and applies identity (RFC 022).
    ///
    /// Used by app `send` and by eager-identity (Phase C). Idempotent when
    /// already connected.
    ///
    /// Concurrent callers share one dial: a caller that finds a dial to the
    /// peer already in flight waits for it and returns its outcome (`Ok`, or
    /// the same [`SessionError::ConnectFailed`]). The wait is bounded by the
    /// dial itself (TCP dial, WS upgrade and hello, at most
    /// `SESSION_DIAL_TIMEOUT`); a caller that needs a shorter deadline wraps
    /// the call in its own timeout. Dropping the future does not cancel the
    /// dial — it completes in the background and installs the connection
    /// for the next caller.
    pub async fn ensure_ws_connected(&self, peer_id: &str) -> Result<(), SessionError> {
        // Fast path: no context to build when already connected.
        if self.ws_connections.read().await.contains_key(peer_id) {
            return Ok(());
        }
        self.dial_ctx()
            .ensure_connected(peer_id, DialReason::App)
            .await
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

        // A peer Layer 3 has not announced (yet) but that holds a live
        // connection to us under exactly this Tailscale id. Only a connection
        // the PEER opened qualifies: its hello was verified against WhoIs
        // (and the RFC 025 login gate) when this node accepted it, after any
        // Layer 3 departure (an install admitted before one is refused), so
        // replying on it is sound. A connection this node dialed proves
        // nothing of the kind — the answerer's hello is self-declared — and
        // an entry-less peer can only hold one if it left mid-dial. This is
        // how a reply reaches a peer that just restarted as a new node — its
        // hello can arrive seconds before the netmap announces it — or a
        // hello-only peer Layer 3 never lists (RFC 023 §6.4). Nothing is
        // dialed for an unknown id.
        let inbound = self
            .ws_connections
            .read()
            .await
            .get(peer_id)
            .is_some_and(|handle| handle.direction == ConnectionDirection::Inbound);
        if inbound {
            return Ok(peer_id.to_string());
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

    // @atlas step store-update.fan
    /// Broadcast data to all peers with active WebSocket connections.
    ///
    /// Sends to all currently connected peers. Peers with no active
    /// connection are skipped (no lazy connect on broadcast).
    /// Errors from individual sends are logged but do not fail the broadcast.
    pub async fn broadcast(&self, data: &[u8]) -> BroadcastReport {
        // Snapshot the senders: a full queue must not hold the connection
        // table (and with it every install, removal and send) hostage.
        let targets: Vec<(String, mpsc::Sender<Vec<u8>>)> = self
            .ws_connections
            .read()
            .await
            .iter()
            .map(|(peer_id, handle)| (peer_id.clone(), handle.send_tx.clone()))
            .collect();
        let mut report = BroadcastReport {
            attempted: targets.len(),
            ..Default::default()
        };

        for (peer_id, send_tx) in targets {
            if send_tx.send(data.to_vec()).await.is_err() {
                tracing::warn!(
                    peer_id = %peer_id,
                    "session: broadcast send failed (connection task closed)"
                );
                report.failed.push(peer_id);
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
    ///
    /// # Hello-only peers
    ///
    /// A registry entry is created by a Layer 3 `Joined`, which a node with a
    /// custom hostname (RFC 023 §6.4) never gets: it fails the
    /// `truffle-{appId}-` prefix heuristic, so it is not discovered, only
    /// *accepted*. Its hello identity waits in `pending_identities` for a
    /// `Joined` that will never arrive, and without a fallback every store
    /// slice it sends is dropped as "no published identity" — a regression
    /// against the pre-RFC-025 behaviour, which applied the slice under the
    /// id the payload claimed.
    ///
    /// So when there is NO entry, the pending identity answers, under two
    /// conditions that keep it as sound a binding as the registry path:
    ///
    /// 1. **A connection for that id must still be installed.** The identity
    ///    is the hello THIS NODE'S TRANSPORT VERIFIED for that connection
    ///    (`verify_authenticated_identity` against the bridge's WhoIs): it
    ///    is stashed only for an INBOUND connection, and only while that
    ///    connection is the peer's primary — a dialed answerer's hello is
    ///    self-declared and never stashed, and an install admitted before a
    ///    Layer 3 departure is refused. Once the connection is gone the
    ///    identity speaks for nothing, and `pending_identities` is only
    ///    swept on a Layer 3 `Left` — which, for a peer that was never
    ///    discovered, never comes.
    /// 2. **`by_device` must not map that ULID to a DIFFERENT live peer.**
    ///    First-wins (RFC 022 §7.7): a hello-only claimant must never be able
    ///    to speak for — and so overwrite the store slice of — a ULID a
    ///    published peer already owns.
    ///
    /// An entry that EXISTS answers definitively, `Some` or `None`. It never
    /// falls through to `pending_identities`, because a `None` there means
    /// "no hello yet" or "suppressed under first-wins", and both must stay
    /// `None`.
    pub async fn published_device_id(&self, tailscale_id: &str) -> Option<String> {
        {
            let map = self.peers.read().await;
            if let Some(state) = map.get(tailscale_id) {
                return state.published_device_id().map(str::to_string);
            }
        }

        // No entry: the hello-only path. Condition 1 — a live connection.
        if !self.ws_connections.read().await.contains_key(tailscale_id) {
            return None;
        }

        let device_id = self
            .pending_identities
            .read()
            .await
            .get(tailscale_id)
            .map(|identity| identity.device_id.clone())?;

        // Condition 2 — first-wins.
        match self.by_device.read().await.get(&device_id) {
            Some(owner) if owner != tailscale_id => {
                tracing::warn!(
                    peer_id = %tailscale_id,
                    device = device_id.as_str(),
                    owner = owner.as_str(),
                    "session: hello-only peer claims a ULID another live peer \
                     published; not binding it (RFC 022 §7.7)"
                );
                None
            }
            _ => Some(device_id),
        }
    }

    /// Disconnect a specific peer's WebSocket connection.
    ///
    /// Removes the cached connection and marks the peer as disconnected.
    /// Does not remove the peer from the registry (that only happens when
    /// Layer 3 emits a `Left` event).
    pub async fn disconnect(&self, peer_id: &str) {
        let (handle, parked) = {
            let mut conns = self.ws_connections.write().await;
            let mut standby = self.standby_connections.write().await;
            (conns.remove(peer_id), standby.remove(peer_id))
        };

        if let Some(parked) = parked {
            let _ = parked.close_tx.try_send(());
        }
        if let Some(handle) = handle {
            // Signal the connection task to close. If the channel is already
            // closed (task exited), that's fine.
            let _ = handle.close_tx.send(()).await;
        }

        // Mark the peer disconnected — unless a send already dialed a new
        // connection in the meantime, whose own bookkeeping reports it.
        mark_disconnected_unless_replaced(&self.conn_ctx(), peer_id).await;
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

        let (handles, parked): (Vec<(String, WsConnectionHandle)>, Vec<WsConnectionHandle>) = {
            let mut conns = self.ws_connections.write().await;
            let mut standby = self.standby_connections.write().await;
            (
                conns.drain().collect(),
                standby.drain().map(|(_, handle)| handle).collect(),
            )
        };
        for handle in parked {
            let _ = handle.close_tx.try_send(());
        }
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
// Connection installation — one primary connection per peer
// ---------------------------------------------------------------------------

/// What [`install_connection`] did with a candidate connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Installed {
    /// It is now the peer's primary connection. The previous primary was
    /// closed — or demoted to the standby slot, when the candidate won the
    /// way a promoted standby does — and any other parked standby was
    /// closed.
    Primary,
    /// It is parked in the standby slot behind the existing primary.
    Standby,
    /// It was closed; the existing primary stays.
    Refused,
    /// It was closed because Layer 3 reported the peer gone after the
    /// connection was admitted (see [`ConnCtx::departed`]).
    Departed,
}

/// [`install_decision`]'s verdict for a candidate connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InstallDecision {
    Install,
    Standby,
    Refuse,
}

/// Install one connection per peer.
///
/// During simultaneous cross-dials both nodes must keep the same physical
/// connection: the lexicographically smaller Tailscale id keeps its outbound
/// side and the larger id keeps the matching inbound side. But the existing
/// connection can also be stale — the peer restarted (same Tailscale id, new
/// process) or lost it without this side noticing — and then the peer's new
/// connection must win, or it is refused until a heartbeat clears the dead
/// one. [`install_decision`] tells the cases apart:
///
/// - **No connection, or the same dialer again** → install. A node dials
///   only when it holds no connection to the peer, so a second connection
///   from the same side means that side abandoned the first.
/// - **The candidate is the preferred side** → install (cross-dial
///   tie-break).
/// - **Outbound candidate, the existing one is preferred** → refuse: the
///   peer dialed the existing connection while ours was in flight, and it
///   keeps that one too.
/// - **Inbound candidate, the existing one is preferred** → either a
///   cross-dial twin (the peer will refuse it within a round trip) or a
///   peer that no longer holds the existing connection. Install at once if
///   the existing connection is older than [`CROSS_DIAL_WINDOW`] (no twin
///   can be); otherwise park the candidate as a **standby**. It is dropped
///   when the peer closes it (the twin case), and promoted when the
///   existing connection dies, when the peer sends data on it, or after
///   [`STANDBY_GRACE`].
///
/// Data on the standby does not prove that the peer dropped the existing
/// connection. In a cross-dial whose installs land crossed, the larger id
/// installs its own dial first (it held nothing), sends on it at once, and
/// only later replaces it with the preferred inbound twin — closing the
/// very connection this side promoted. So a promotion never closes the
/// primary it displaces: that connection is **demoted** to the standby
/// slot. If the peer kept it, the peer's close of the promoted connection
/// hands the slot back (and the peer's data on it promotes it back at
/// once); if not (the peer restarted), it is closed after
/// [`STANDBY_GRACE`].
///
/// A candidate admitted before Layer 3 last reported the peer gone is
/// refused ([`Installed::Departed`]), whatever the tie-break says.
async fn install_connection(
    ctx: &ConnCtx,
    local_tailscale_id: &str,
    peer_id: &str,
    handle: WsConnectionHandle,
) -> Installed {
    let preferred = preferred_connection_direction(local_tailscale_id, peer_id);
    let mut to_close = Vec::new();

    let installed = {
        let mut conns = ctx.ws_connections.write().await;
        let mut standby = ctx.standby.write().await;
        if ctx.departed_since(peer_id, handle.admitted_at) {
            // Checked under the lock the `Left` handler stamps under: the
            // departure either swept this connection or refuses it here.
            to_close.push(handle);
            Installed::Departed
        } else {
            let existing = conns
                .get(peer_id)
                .map(|existing| (existing.direction, existing.connected_at.elapsed()));
            let mut decision = install_decision(existing, handle.direction, preferred);
            if handle.flags.ended.load(Ordering::SeqCst) {
                // Already gone (see `ConnectionFlags`): never install a corpse.
                decision = InstallDecision::Refuse;
            }
            // The candidate carried data before it could be parked: it wins
            // the way a promoted standby does, demoting the primary.
            let mut promoted = false;
            if decision == InstallDecision::Standby {
                // Dekker pair with the connection task (see `ConnectionFlags`):
                // publish `standby` first, then look for data that already
                // arrived — data the task saw before this store would otherwise
                // never trigger a promotion.
                handle.flags.standby.store(true, Ordering::SeqCst);
                if handle.flags.seen_data.load(Ordering::SeqCst) {
                    handle.flags.standby.store(false, Ordering::SeqCst);
                    decision = InstallDecision::Install;
                    promoted = true;
                }
            }
            match decision {
                InstallDecision::Install => {
                    let displaced = conns.insert(peer_id.to_string(), handle);
                    to_close.extend(standby.remove(peer_id));
                    if let Some(old) = displaced {
                        if promoted {
                            to_close.extend(demote(ctx, &mut standby, peer_id, old));
                        } else {
                            to_close.push(old);
                        }
                    }
                    Installed::Primary
                }
                InstallDecision::Standby => {
                    to_close.extend(park(
                        ctx,
                        &mut standby,
                        peer_id,
                        handle,
                        ParkedAs::Candidate,
                    ));
                    Installed::Standby
                }
                InstallDecision::Refuse => {
                    to_close.push(handle);
                    Installed::Refused
                }
            }
        }
    };

    for old in to_close {
        let _ = old.close_tx.try_send(());
    }
    installed
}

/// The pure tie-break behind [`install_connection`]. `existing` is the
/// current primary's direction and age.
fn install_decision(
    existing: Option<(ConnectionDirection, Duration)>,
    candidate: ConnectionDirection,
    preferred: Option<ConnectionDirection>,
) -> InstallDecision {
    let Some((existing, age)) = existing else {
        return InstallDecision::Install;
    };
    if existing == candidate || preferred == Some(candidate) {
        return InstallDecision::Install;
    }
    match candidate {
        ConnectionDirection::Outbound => InstallDecision::Refuse,
        ConnectionDirection::Inbound if age >= CROSS_DIAL_WINDOW => InstallDecision::Install,
        ConnectionDirection::Inbound => InstallDecision::Standby,
    }
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

/// Put `handle` in the standby slot (caller holds both table locks) and arm
/// its grace timer. Returns the connection it pushed out of the slot, for
/// the caller to close.
fn park(
    ctx: &ConnCtx,
    standby: &mut HashMap<String, WsConnectionHandle>,
    peer_id: &str,
    handle: WsConnectionHandle,
    kind: ParkedAs,
) -> Option<WsConnectionHandle> {
    handle.flags.standby.store(true, Ordering::SeqCst);
    let parking = handle.flags.parkings.fetch_add(1, Ordering::SeqCst) + 1;
    arm_standby_timer(ctx, peer_id, &handle.send_tx, parking, kind);
    standby.insert(peer_id.to_string(), handle)
}

/// Demote a primary that a promotion displaced (see [`install_connection`])
/// into the standby slot, which the caller has just emptied. A connection
/// whose task already ended is not parked — it is about to retire itself —
/// and comes back for closing instead.
///
/// No Dekker pair is needed here: data the peer sent on the demoted
/// connection just before its `standby` flag was set does not promote it
/// back, but in the cross-dial case the peer's close of the promoted
/// connection then hands the slot back anyway.
fn demote(
    ctx: &ConnCtx,
    standby: &mut HashMap<String, WsConnectionHandle>,
    peer_id: &str,
    old: WsConnectionHandle,
) -> Option<WsConnectionHandle> {
    if old.flags.ended.load(Ordering::SeqCst) {
        return Some(old);
    }
    tracing::debug!(
        peer_id = %peer_id,
        "session: demoted the displaced WS connection to standby"
    );
    park(ctx, standby, peer_id, old, ParkedAs::Demoted)
}

/// After [`ConnCtx::standby_grace`], promote (a candidate) or close (a
/// demoted primary) the connection owning `send_tx` — if it is still in the
/// standby slot from the same parking by then.
fn arm_standby_timer(
    ctx: &ConnCtx,
    peer_id: &str,
    send_tx: &mpsc::Sender<Vec<u8>>,
    parking: u64,
    kind: ParkedAs,
) {
    let ctx = ctx.clone();
    let peer_id = peer_id.to_string();
    let send_tx = send_tx.clone();
    tokio::spawn(async move {
        tokio::time::sleep(ctx.standby_grace).await;
        match kind {
            ParkedAs::Candidate => {
                promote_standby(
                    &ctx,
                    &peer_id,
                    &send_tx,
                    Some(parking),
                    "the peer kept it open past the grace period",
                )
                .await;
            }
            ParkedAs::Demoted => close_demoted(&ctx, &peer_id, &send_tx, parking).await,
        }
    });
}

/// Close a demoted connection that neither came back into use nor was
/// handed the slot back within the grace period: the peer no longer holds
/// it.
async fn close_demoted(
    ctx: &ConnCtx,
    peer_id: &str,
    send_tx: &mpsc::Sender<Vec<u8>>,
    parking: u64,
) {
    let expired = {
        let mut standby = ctx.standby.write().await;
        let still_parked = standby.get(peer_id).is_some_and(|handle| {
            handle.send_tx.same_channel(send_tx)
                && handle.flags.parkings.load(Ordering::SeqCst) == parking
        });
        if still_parked {
            standby.remove(peer_id)
        } else {
            None
        }
    };
    if let Some(old) = expired {
        let _ = old.close_tx.try_send(());
        tracing::debug!(
            peer_id = %peer_id,
            "session: closed the demoted WS connection after the grace period"
        );
    }
}

/// Move the standby owning `send_tx` into the primary slot, demoting the
/// primary it displaces (see [`install_connection`]). With `parking` set
/// (a grace timer), only that parking of the connection qualifies. Returns
/// `false` (and does nothing) when that connection is no longer parked.
async fn promote_standby(
    ctx: &ConnCtx,
    peer_id: &str,
    send_tx: &mpsc::Sender<Vec<u8>>,
    parking: Option<u64>,
    why: &str,
) -> bool {
    let (identity, direction, to_close) = {
        let mut conns = ctx.ws_connections.write().await;
        let mut standby = ctx.standby.write().await;
        // A standby whose task already ended is about to retire itself;
        // promoting it would only trade a dead primary for another.
        let parked_here = standby.get(peer_id).is_some_and(|handle| {
            handle.send_tx.same_channel(send_tx)
                && !handle.flags.ended.load(Ordering::SeqCst)
                && parking.is_none_or(|p| handle.flags.parkings.load(Ordering::SeqCst) == p)
        });
        if !parked_here {
            return false;
        }
        let Some(handle) = standby.remove(peer_id) else {
            return false;
        };
        handle.flags.standby.store(false, Ordering::SeqCst);
        let identity = handle.identity.clone();
        let direction = handle.direction;
        let to_close = conns
            .insert(peer_id.to_string(), handle)
            .and_then(|old| demote(ctx, &mut standby, peer_id, old));
        (identity, direction, to_close)
    };
    if let Some(old) = to_close {
        let _ = old.close_tx.try_send(());
    }
    tracing::info!(
        peer_id = %peer_id,
        reason = why,
        "session: promoted standby WS connection over the existing one"
    );
    on_primary_installed(ctx, peer_id, send_tx, direction, identity).await;
    true
}

/// Bookkeeping once a connection (`send_tx`) becomes a peer's primary: mark
/// the peer connected, apply the hello identity, emit `WsConnected`.
///
/// It runs after the table lock was released, so the connection may be gone
/// again by now — retired by its own task, displaced, or swept by a
/// departure — and whoever removed it has reported that. So the connected
/// state and the event are applied only if it is STILL the primary, checked
/// under the `peers` lock every connected/disconnected write takes, and the
/// event is sent under that lock too, so events come out in the order of
/// the state changes.
///
/// With no registry entry, an inbound hello is held in
/// `pending_identities` until Layer 3 announces the peer (an inbound hello
/// can win that race); a dialed answerer's hello is self-declared and never
/// held — an entry-less outbound connection means the peer left mid-dial.
async fn on_primary_installed(
    ctx: &ConnCtx,
    peer_id: &str,
    send_tx: &mpsc::Sender<Vec<u8>>,
    direction: ConnectionDirection,
    identity: Option<PeerIdentity>,
) {
    let mut pending = ctx.pending_identities.write().await;
    let mut map = ctx.peers.write().await;
    let mut by_dev = ctx.by_device.write().await;
    let still_primary = ctx
        .ws_connections
        .read()
        .await
        .get(peer_id)
        .is_some_and(|handle| handle.send_tx.same_channel(send_tx));
    if let Some(state) = map.get_mut(peer_id) {
        if still_primary {
            state.ws_connected = true;
        }
        if let Some(identity) = identity {
            let outcomes = apply_identity(&mut map, &mut by_dev, peer_id, identity);
            emit_identity_outcomes(&ctx.event_tx, outcomes);
        }
    } else if let Some(identity) = identity {
        if still_primary && direction == ConnectionDirection::Inbound {
            pending.insert(peer_id.to_string(), identity);
        }
    }
    if still_primary {
        let _ = ctx
            .event_tx
            .send(PeerEvent::WsConnected(peer_id.to_string()));
    }
}

/// Mark `peer_id` disconnected and emit `WsDisconnected`, after its primary
/// was removed — unless a new primary was installed since, whose
/// [`on_primary_installed`] reports it. Decided and emitted under the
/// `peers` lock, like the connected side.
async fn mark_disconnected_unless_replaced(ctx: &ConnCtx, peer_id: &str) {
    let mut map = ctx.peers.write().await;
    if ctx.ws_connections.read().await.contains_key(peer_id) {
        return;
    }
    if let Some(state) = map.get_mut(peer_id) {
        state.ws_connected = false;
    }
    let _ = ctx
        .event_tx
        .send(PeerEvent::WsDisconnected(peer_id.to_string()));
}

// ---------------------------------------------------------------------------
// Connection task — exclusively owns the WsFramedStream
// ---------------------------------------------------------------------------

// @atlas step send.receive
/// Spawn a background task that exclusively owns a `WsFramedStream`.
///
/// The task uses `tokio::select!` to multiplex between:
/// - Receiving outgoing data from the `send_rx` channel and writing to the WS
/// - Reading incoming data from the WS and forwarding to `incoming_tx`
/// - Receiving a close signal from `close_rx`
///
/// Data arriving on a parked standby (a candidate, or a demoted primary)
/// promotes it first: the peer is using it now. When the task exits on its
/// own (stream closed or error), it removes its connection from the
/// registry: a primary hands its slot to a parked standby if there is one,
/// else the peer is marked disconnected (`WsDisconnected`); a standby just
/// leaves.
///
/// `admitted_at` is [`WsConnectionHandle::admitted_at`].
///
/// Returns a [`WsConnectionHandle`] for the caller to send data and close.
fn spawn_connection_task(
    stream: WsFramedStream,
    peer_id: String,
    direction: ConnectionDirection,
    admitted_at: Instant,
    ctx: ConnCtx,
) -> WsConnectionHandle {
    let (send_tx, mut send_rx) = mpsc::channel::<Vec<u8>>(256);
    let (close_tx, mut close_rx) = mpsc::channel::<()>(1);
    let flags = Arc::new(ConnectionFlags::default());

    let handle = WsConnectionHandle {
        send_tx: send_tx.clone(),
        close_tx: close_tx.clone(),
        peer_id: peer_id.clone(),
        connected_at: Instant::now(),
        direction,
        identity: stream.remote_identity().cloned(),
        admitted_at,
        flags: flags.clone(),
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
                            // Dekker pair with `install_connection`.
                            if !flags.seen_data.load(Ordering::Relaxed) {
                                flags.seen_data.store(true, Ordering::SeqCst);
                            }
                            if flags.standby.load(Ordering::SeqCst) {
                                // Promote before delivering, so a reply the
                                // application sends goes out on this socket.
                                promote_standby(
                                    &ctx,
                                    &peer_id,
                                    &task_send_tx,
                                    None,
                                    "the peer sent data on it",
                                )
                                .await;
                            }
                            let msg = IncomingMessage {
                                from: from_tailscale_id.clone(),
                                data,
                                received_at: Instant::now(),
                            };
                            let _ = ctx.incoming_tx.send(msg);
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
                    // Bounded: on a dead link the close frame can sit behind
                    // a full send buffer forever. Dropping the stream then
                    // closes the socket regardless.
                    let _ = tokio::time::timeout(CLOSE_TIMEOUT, stream.close()).await;
                    break;
                }
            }
        }
        flags.ended.store(true, Ordering::SeqCst);

        // Clean up only if we weren't explicitly closed: whoever closed us
        // (disconnect, replacement, Left, shutdown) already took us out of
        // the table, and must not race this cleanup.
        if !closed {
            retire_connection(&ctx, &peer_id, &task_send_tx).await;
        }
    });

    handle
}

/// What a connection that ended on its own still owned.
enum Retired {
    /// It was the primary; nothing replaces it.
    Primary,
    /// It was the primary and a parked standby took its slot.
    HandedOver {
        send_tx: mpsc::Sender<Vec<u8>>,
        direction: ConnectionDirection,
        identity: Option<PeerIdentity>,
    },
    /// It was the parked standby (typically: the peer refused our side of
    /// a cross-dial and kept the primary).
    Standby,
    /// It no longer owned a slot.
    Nothing,
}

async fn retire_connection(ctx: &ConnCtx, peer_id: &str, send_tx: &mpsc::Sender<Vec<u8>>) {
    let retired = {
        let mut conns = ctx.ws_connections.write().await;
        let mut standby = ctx.standby.write().await;
        let owns = |map: &HashMap<String, WsConnectionHandle>| {
            map.get(peer_id)
                .is_some_and(|handle| handle.send_tx.same_channel(send_tx))
        };
        if owns(&conns) {
            conns.remove(peer_id);
            match standby.remove(peer_id) {
                // A standby that has itself ended is retiring too: it gets
                // nothing to take over.
                Some(next) if !next.flags.ended.load(Ordering::SeqCst) => {
                    next.flags.standby.store(false, Ordering::SeqCst);
                    let handed_over = Retired::HandedOver {
                        send_tx: next.send_tx.clone(),
                        direction: next.direction,
                        identity: next.identity.clone(),
                    };
                    conns.insert(peer_id.to_string(), next);
                    handed_over
                }
                Some(_) | None => Retired::Primary,
            }
        } else if owns(&standby) {
            standby.remove(peer_id);
            Retired::Standby
        } else {
            Retired::Nothing
        }
    };

    match retired {
        Retired::Primary => mark_disconnected_unless_replaced(ctx, peer_id).await,
        Retired::HandedOver {
            send_tx,
            direction,
            identity,
        } => {
            tracing::info!(
                peer_id = %peer_id,
                "session: WS connection ended; its standby took over"
            );
            on_primary_installed(ctx, peer_id, &send_tx, direction, identity).await;
        }
        Retired::Standby => {
            tracing::debug!(
                peer_id = %peer_id,
                "session: standby WS connection closed by the peer (cross-dial settled)"
            );
        }
        Retired::Nothing => {}
    }
}

// ---------------------------------------------------------------------------
// Dialing — one dial per peer, shared by every concurrent caller
// ---------------------------------------------------------------------------

impl<N: NetworkProvider + 'static> DialCtx<N> {
    /// Ensure `peer_id` has a primary connection, dialing — or joining the
    /// dial already in flight — when it does not.
    async fn ensure_connected(
        &self,
        peer_id: &str,
        reason: DialReason,
    ) -> Result<(), SessionError> {
        if self.conn.ws_connections.read().await.contains_key(peer_id) {
            return Ok(());
        }

        // Taken before the entry is read: a Layer 3 departure stamped at or
        // after this instant refuses the dial's install (see
        // `ConnCtx::departed`).
        let admitted_at = Instant::now();
        let peer_addr = {
            let map = self.conn.peers.read().await;
            let state = map
                .get(peer_id)
                .ok_or_else(|| SessionError::UnknownPeer(peer_id.to_string()))?;
            if !state.online {
                return Err(SessionError::PeerOffline(peer_id.to_string()));
            }
            if reason == DialReason::EagerIdentity && state.published_device_id().is_some() {
                return Ok(());
            }
            // RFC 025 §3.3: a gated node never opens a NEW connection to a
            // peer whose owner Layer 3 cannot state. The entry survives an
            // unknown login so the mesh is not emptied by a transient
            // omission, but our hello is not revealed to it and its slices
            // are not invited. An ALREADY-OPEN connection returned above.
            // The eager dial inherits the gate: an unresolved login is
            // exactly the case where we must not volunteer a hello.
            if self.login_gated && state.login_name.is_none() {
                return Err(SessionError::LoginUnknown(peer_id.to_string()));
            }
            PeerAddr {
                ip: Some(state.ip),
                hostname: state.name.clone(),
                dns_name: None,
            }
        };

        // A dial already in flight answers for this caller too.
        if let Some(rx) = self.inflight(peer_id) {
            return wait_for_dial(rx).await;
        }

        // Backoff gates starting a NEW dial only.
        {
            let backoffs = self.peer_backoffs.read().await;
            if let Some(backoff) = backoffs.get(peer_id) {
                if backoff.should_retry().is_none() {
                    return Err(SessionError::ReconnectBackoff {
                        retry_after: backoff.retry_after(),
                    });
                }
            }
        }

        let (rx, task) = {
            let mut dials = self.dials.lock().unwrap_or_else(PoisonError::into_inner);
            match dials.get(peer_id) {
                Some(inflight) => (inflight.rx.clone(), None),
                None => {
                    let id = NEXT_DIAL_ID.fetch_add(1, Ordering::Relaxed);
                    let (tx, rx) = watch::channel(None);
                    dials.insert(peer_id.to_string(), InflightDial { id, rx: rx.clone() });
                    let ctx = self.clone();
                    let peer = peer_id.to_string();
                    let task = tokio::spawn(async move {
                        ctx.run_dial(id, peer, peer_addr, admitted_at, reason, tx)
                            .await;
                    });
                    (rx, Some(task))
                }
            }
        };
        if let Some(task) = task {
            // Tracked so `shutdown` aborts a dial in flight; never pushed
            // while `dials` is held.
            let mut tasks = self
                .background_tasks
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            tasks.retain(|task| !task.is_finished());
            tasks.push(task);
        }
        wait_for_dial(rx).await
    }

    fn inflight(&self, peer_id: &str) -> Option<watch::Receiver<Option<DialOutcome>>> {
        self.dials
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(peer_id)
            .map(|inflight| inflight.rx.clone())
    }

    /// The body of a dial task: dial, install, publish the outcome.
    async fn run_dial(
        self,
        id: u64,
        peer_id: String,
        peer_addr: PeerAddr,
        admitted_at: Instant,
        reason: DialReason,
        tx: watch::Sender<Option<DialOutcome>>,
    ) {
        // Unregisters the dial however this task ends — including an abort
        // from `shutdown`, which drops `tx` unsent and so wakes every waiter
        // with "cancelled".
        let _guard = DialGuard {
            dials: self.dials.clone(),
            peer_id: peer_id.clone(),
            id,
        };
        let outcome = match self
            .dial_and_install(&peer_id, &peer_addr, admitted_at, reason)
            .await
        {
            Ok(()) => DialOutcome::Connected,
            Err(message) => DialOutcome::Failed(message),
        };
        // Publish before unregistering: a caller that joins in between
        // still gets this outcome.
        tx.send_replace(Some(outcome));
    }

    /// Dial `peer_id`, check the answerer, install the connection. The
    /// error is a [`SessionError::ConnectFailed`] message. `admitted_at` is
    /// when the caller found the peer's registry entry dialable.
    async fn dial_and_install(
        &self,
        peer_id: &str,
        peer_addr: &PeerAddr,
        admitted_at: Instant,
        reason: DialReason,
    ) -> Result<(), String> {
        // A dial that finished between the caller's check and this dial's
        // registration may already have installed a connection.
        if self.conn.ws_connections.read().await.contains_key(peer_id) {
            return Ok(());
        }

        match reason {
            DialReason::App => tracing::info!(peer_id = %peer_id, "session: connecting WS"),
            DialReason::EagerIdentity => {
                tracing::info!(peer_id = %peer_id, "session: eager identity connecting WS")
            }
        }
        let started = Instant::now();

        let connect_result =
            match tokio::time::timeout(SESSION_DIAL_TIMEOUT, self.ws_transport.connect(peer_addr))
                .await
            {
                Ok(result) => result.map_err(|e| e.to_string()),
                Err(_) => Err(format!(
                    "WS dial timed out after {}s",
                    SESSION_DIAL_TIMEOUT.as_secs()
                )),
            };

        {
            let mut backoffs = self.peer_backoffs.write().await;
            let backoff = backoffs
                .entry(peer_id.to_string())
                .or_insert_with(ReconnectBackoff::new);
            if connect_result.is_ok() {
                backoff.success();
            } else {
                backoff.failure();
            }
        }
        let ws_stream = connect_result?;

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
                return Err(format!(
                    "hello identity mismatch: dialed {peer_id}, answerer claims {}",
                    claimed.tailscale_id
                ));
            }
        }

        // The dial can outlive what made it dialable (it runs on after an
        // impatient caller gives up): a peer Layer 3 dropped, or whose owner
        // a gated node can no longer state, gets no new connection. The
        // install re-checks the departure under the table lock, which also
        // covers a `Left` that lands between here and there.
        {
            let map = self.conn.peers.read().await;
            match map.get(peer_id) {
                None => return Err(format!("peer left during dial: {peer_id}")),
                Some(state) if self.login_gated && state.login_name.is_none() => {
                    return Err(format!("login unknown for peer after dial: {peer_id}"));
                }
                Some(_) => {}
            }
        }

        let remote_identity = ws_stream.remote_identity().cloned();
        let handle = spawn_connection_task(
            ws_stream,
            peer_id.to_string(),
            ConnectionDirection::Outbound,
            admitted_at,
            self.conn.clone(),
        );
        let send_tx = handle.send_tx.clone();
        let local_tailscale_id = self.network.local_identity().tailscale_id;
        match install_connection(&self.conn, &local_tailscale_id, peer_id, handle).await {
            Installed::Primary => {
                on_primary_installed(
                    &self.conn,
                    peer_id,
                    &send_tx,
                    ConnectionDirection::Outbound,
                    remote_identity,
                )
                .await;
            }
            Installed::Departed => {
                return Err(format!("peer left during dial: {peer_id}"));
            }
            // A simultaneous inbound connection won the deterministic
            // tie-break. Its hello already established the same peer
            // session.
            Installed::Refused | Installed::Standby => {
                tracing::debug!(
                    peer_id = %peer_id,
                    "session: kept the peer's connection over our dial (cross-dial tie-break)"
                );
            }
        }
        tracing::debug!(
            peer_id = %peer_id,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "session: WS dial complete"
        );
        Ok(())
    }
}

/// Wait for a dial's outcome.
async fn wait_for_dial(mut rx: watch::Receiver<Option<DialOutcome>>) -> Result<(), SessionError> {
    let outcome = match rx.wait_for(Option::is_some).await {
        Ok(outcome) => outcome.clone(),
        Err(_) => None,
    };
    match outcome {
        Some(DialOutcome::Connected) => Ok(()),
        Some(DialOutcome::Failed(message)) => Err(SessionError::ConnectFailed(message)),
        None => Err(SessionError::ConnectFailed(
            DIAL_CANCELLED_SHUTTING_DOWN.to_string(),
        )),
    }
}

/// Removes a dial from [`DialMap`] when its task ends, however it ends.
struct DialGuard {
    dials: DialMap,
    peer_id: String,
    id: u64,
}

impl Drop for DialGuard {
    fn drop(&mut self) {
        let mut dials = self.dials.lock().unwrap_or_else(PoisonError::into_inner);
        if dials
            .get(&self.peer_id)
            .is_some_and(|inflight| inflight.id == self.id)
        {
            dials.remove(&self.peer_id);
        }
    }
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
    /// The same dial machinery app sends use (shared in-flight dials).
    dial: DialCtx<N>,
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

        let dial = self.dial.clone();
        let sem = self.eager_identity_sem.clone();
        let jitter_ms = self.eager_identity_jitter_ms;
        let inflight = self.identity_inflight.clone();

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
                    let map = dial.conn.peers.read().await;
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

                // Same path as an app send: joins a dial already in flight.
                let result = dial
                    .ensure_connected(&peer_id, DialReason::EagerIdentity)
                    .await;
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
                    let backoffs = dial.peer_backoffs.read().await;
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
        let mut tasks = self
            .background_tasks
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        tasks.retain(|task| !task.is_finished());
        tasks.push(handle);
    }
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
    let connection_type = if !peer.active {
        "unknown".to_string()
    } else if peer.cur_addr.as_ref().is_some_and(|s| !s.is_empty()) {
        "direct".to_string()
    } else if let Some(ref relay) = peer.peer_relay {
        format!("peer-relay:{relay}")
    } else if let Some(ref relay) = peer.relay {
        format!("relay:{relay}")
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
