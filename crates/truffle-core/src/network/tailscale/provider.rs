//! TailscaleProvider — the public NetworkProvider implementation.
//!
//! Orchestrates the Go sidecar (Layer 1) and bridge (Layer 2) to provide
//! peer discovery, raw TCP connectivity, and diagnostics via the Tailscale
//! network.

use std::collections::HashMap;
use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpStream;
use tokio::sync::{broadcast, mpsc, oneshot, Mutex, RwLock};

use super::bridge::{Bridge, BridgeCounters, BridgeStats, DIAL_TIMEOUT};
use super::protocol::{
    command_type, EgressAddCommandData, EgressListCommandData, EgressProxyCommandData,
    EgressRemoveCommandData, LoopbackCommandData, PingResultEventData, ProxyAddCommandData,
    ProxyInfoEventData, WhoisResultEventData,
};
use super::sidecar::{GoSidecar, ReplyGuard, SidecarConfig, SidecarInternalEvent};
use crate::egress::{BoundEgress, EgressRoute, EgressRouteInfo, HttpProxyConfig, LoopbackConfig};
use crate::network::login_allow::login_allowed;
use crate::network::{
    BackendState, ControlServer, DialOpts, HealthInfo, IncomingConnection, KeyExpiry, ListenOpts,
    NetworkError, NetworkPeer, NetworkPeerEvent, NetworkStatusEvent, NetworkTcpListener,
    NodeIdentity, PeerAddr, PingResult, ProxyAddParams, ProxyAddResult, ProxyListEntry,
    ProxyRuntimeError, TailscaleVersion,
};

/// How long [`TailscaleProvider::start`](crate::network::NetworkProvider::start)
/// waits for the node to reach Running unless
/// [`TailscaleConfig::start_timeout`] says otherwise: long enough for an
/// interactive browser login or a device approval.
pub const DEFAULT_START_TIMEOUT: Duration = Duration::from_secs(300);

/// Configuration for creating a TailscaleProvider.
#[derive(Clone)]
pub struct TailscaleConfig {
    /// Path to the Go sidecar binary.
    pub binary_path: PathBuf,
    /// Application identifier (RFC 017 §5.1). Stored as a plain `String`
    /// because validation happens in `NodeBuilder::app_id`; by the time the
    /// config is constructed the value is already a valid `AppId`.
    pub app_id: String,
    /// Stable per-device ULID (RFC 017 §5.4).
    pub device_id: String,
    /// Original (unsanitised) device name — retained for display and for
    /// building the `NodeIdentity` returned from `local_identity()`.
    pub device_name: String,
    /// Final Tailscale hostname, already composed by the caller as
    /// `truffle-{app_id}-{slug(device_name)}`. The provider does NOT rebuild
    /// this — it trusts the builder has applied the RFC 017 derivation once.
    pub hostname: String,
    /// State directory for tsnet persistent state.
    pub state_dir: String,
    /// Optional Tailscale auth key for headless authentication.
    pub auth_key: Option<String>,
    /// Whether the node is ephemeral (removed when offline).
    pub ephemeral: Option<bool>,
    /// ACL tags to advertise (e.g., ["tag:truffle"]).
    pub tags: Option<Vec<String>>,
    /// Idle timeout for bridged connections in seconds (RFC 021 §6.5).
    /// `None` → the sidecar's 600s default.
    pub idle_timeout_secs: Option<u64>,
    /// How long `start()` waits for the node to come up — through an
    /// interactive login (`AuthRequired`) or a device approval — before
    /// failing. `None` waits indefinitely, for a headless first run that
    /// shows the login URL elsewhere (a LAN setup page, a log line) and may
    /// sit there for hours; cancel by dropping the `start()` future. The
    /// usual value is [`DEFAULT_START_TIMEOUT`].
    pub start_timeout: Option<Duration>,
    /// Login allow-list (RFC 025 §3.1): `loginName` globs of the tailnet
    /// users whose nodes may be peers. Empty = the whole tailnet (the v1
    /// behaviour). Non-empty = only peers whose login matches are reported,
    /// and the provider refuses to start on a sidecar that cannot report
    /// logins (protocol < 5).
    pub login_allow: Vec<String>,
    /// Whether the embedded Tailscale client uploads its client logs to
    /// Tailscale (`log.tailscale.com`), as Tailscale's own clients do by
    /// default. `true` is that default. `false` opts out: the node then needs
    /// a sidecar speaking protocol 11 (`start()` refuses an older one before
    /// starting anything), and a tailnet with network flow logs on refuses it
    /// ([`NetworkStatusEvent::LoggingRequired`]).
    pub client_log_upload: bool,
}

/// Manual `Debug`: `auth_key` is a tailnet credential and must never reach
/// logs, so it is redacted while preserving presence (`Some`/`None`).
impl std::fmt::Debug for TailscaleConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TailscaleConfig")
            .field("binary_path", &self.binary_path)
            .field("app_id", &self.app_id)
            .field("device_id", &self.device_id)
            .field("device_name", &self.device_name)
            .field("hostname", &self.hostname)
            .field("state_dir", &self.state_dir)
            .field("auth_key", &self.auth_key.as_ref().map(|_| "[REDACTED]"))
            .field("ephemeral", &self.ephemeral)
            .field("tags", &self.tags)
            .field("idle_timeout_secs", &self.idle_timeout_secs)
            .field("start_timeout", &self.start_timeout)
            .field("login_allow", &self.login_allow)
            .field("client_log_upload", &self.client_log_upload)
            .finish()
    }
}

/// State of the provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProviderState {
    Stopped,
    Starting,
    Running,
    Stopping,
}

/// Tailscale network provider implementing [`NetworkProvider`](crate::network::NetworkProvider).
///
/// Wraps the Go sidecar (tsnet) and local TCP bridge to provide:
/// - Peer discovery via WatchIPNBus events
/// - Raw TCP dial/listen over encrypted Tailscale tunnels
/// - Network-level ping and health monitoring
///
/// All bridge internals (pending_dials, session tokens, binary headers) are
/// completely hidden. Callers interact only with plain `TcpStream`s and
/// high-level types.
pub struct TailscaleProvider {
    config: TailscaleConfig,
    state: Arc<RwLock<ProviderState>>,

    /// Local node identity (populated after start).
    ///
    /// Uses `std::sync::RwLock` (not tokio) so the sync trait methods
    /// `local_identity()` and `local_addr()` can read without `.await`.
    identity: Arc<std::sync::RwLock<NodeIdentity>>,
    /// Local node address (populated after start).
    ///
    /// Uses `std::sync::RwLock` (not tokio) so the sync trait method
    /// `local_addr()` can read without `.await`.
    local_addr: Arc<std::sync::RwLock<PeerAddr>>,

    /// Cached peer list.
    peers: Arc<RwLock<HashMap<String, NetworkPeer>>>,

    /// Broadcast channel for peer events.
    peer_event_tx: broadcast::Sender<NetworkPeerEvent>,

    /// The node's own status changes (approval pending, login learned).
    status: StatusReporting,

    /// Health info cache.
    health: Arc<RwLock<HealthInfo>>,

    /// Handle to the Go sidecar (set during start).
    sidecar: Arc<Mutex<Option<GoSidecar>>>,

    /// Handle to the bridge (set during start).
    bridge: Arc<Mutex<Option<Arc<Bridge>>>>,

    /// Bridge delivery/drop counters; outlive any one bridge so the totals
    /// cover restarts.
    bridge_counters: Arc<BridgeCounters>,

    /// Bridge shutdown sender.
    bridge_shutdown_tx: Arc<Mutex<Option<tokio::sync::watch::Sender<bool>>>>,

    /// Session token (32 bytes, generated on start).
    session_token: Arc<RwLock<[u8; 32]>>,

    /// Local Tailscale stable ID, captured from the `tsnet:status` event
    /// (netmap `self` entry). Used for self-filtering in the peer event
    /// chain — we must filter by this, NOT by hostname, because hostname
    /// collisions from crashed/restarted dev runs can cause the local
    /// node to appear as its own peer under a different Tailscale ID.
    local_tailscale_id: Arc<std::sync::RwLock<Option<String>>>,

    /// Runtime proxy-engine errors, forwarded from sidecar `proxy:error`
    /// events (RFC 023 G5). Node subscribes via `proxy_runtime_errors()`.
    proxy_error_tx: broadcast::Sender<ProxyRuntimeError>,

    /// Sidecar control-protocol version from `tsnet:status` (0 = v1 /
    /// unknown). Gates RFC 023 v2 proxy features so they fail loudly on
    /// old sidecars instead of being silently ignored on the wire.
    sidecar_protocol_version: Arc<std::sync::atomic::AtomicU32>,

    /// OS process id of the running sidecar child (0 = none). Written by
    /// [`GoSidecar`] at spawn and reset when the child exits or is killed,
    /// so [`sidecar_pid`](Self::sidecar_pid) can read it without locking.
    sidecar_pid: Arc<std::sync::atomic::AtomicU32>,
}

/// Where the event processor reports the node's own status changes.
#[derive(Clone)]
pub(crate) struct StatusReporting {
    /// [`NetworkStatusEvent`] fan-out (`status_events()`).
    pub(crate) tx: broadcast::Sender<NetworkStatusEvent>,
    /// Set while the device waits for admin approval, so a start timeout
    /// can name the approval instead of "authentication".
    pub(crate) needs_approval: Arc<std::sync::atomic::AtomicBool>,
    /// The node's key expiry, once a protocol 10 sidecar reported it.
    pub(crate) key_expiry: Arc<std::sync::RwLock<Option<KeyExpiry>>>,
    /// The coordination server, once a protocol 10 sidecar reported it.
    pub(crate) control_server: Arc<std::sync::RwLock<Option<ControlServer>>>,
    /// The embedded Tailscale client, once a protocol 11 sidecar named it.
    pub(crate) tailscale_version: Arc<std::sync::RwLock<Option<TailscaleVersion>>>,
    /// Whether Tailnet Lock has this node locked out, once a protocol 11
    /// sidecar reported its lock state.
    pub(crate) locked_out: Arc<std::sync::RwLock<Option<bool>>>,
}

impl StatusReporting {
    pub(crate) fn new(tx: broadcast::Sender<NetworkStatusEvent>) -> Self {
        Self {
            tx,
            needs_approval: Arc::default(),
            key_expiry: Arc::default(),
            control_server: Arc::default(),
            tailscale_version: Arc::default(),
            locked_out: Arc::default(),
        }
    }

    /// Record the embedded Tailscale client a status or version answer
    /// named. A property of the binary, so nothing is announced.
    fn learn_tailscale_version(&self, version: Option<TailscaleVersion>) {
        if let Some(version) = version {
            *self
                .tailscale_version
                .write()
                .unwrap_or_else(|p| p.into_inner()) = Some(version);
        }
    }

    /// Record a Tailnet Lock state report. Becoming locked out raises
    /// [`NetworkStatusEvent::NeedsSignature`]; leaving it raises
    /// [`NetworkStatusEvent::Signed`]; an unchanged report raises nothing.
    fn learn_lock(&self, enabled: bool, signed: bool) {
        let locked_out = enabled && !signed;
        let mut slot = self.locked_out.write().unwrap_or_else(|p| p.into_inner());
        let was_locked_out = slot.replace(locked_out) == Some(true);
        drop(slot);
        if locked_out && !was_locked_out {
            tracing::warn!(
                "tailscale: Tailnet Lock has this node locked out until a signing node signs it"
            );
            let _ = self.tx.send(NetworkStatusEvent::NeedsSignature);
        } else if !locked_out && was_locked_out {
            let _ = self.tx.send(NetworkStatusEvent::Signed);
        }
    }

    /// Record the key expiry a running status reported; announce it when it
    /// is new or moved.
    fn learn_key_expiry(&self, key_expiry: KeyExpiry) {
        let mut slot = self.key_expiry.write().unwrap_or_else(|p| p.into_inner());
        if *slot != Some(key_expiry) {
            *slot = Some(key_expiry);
            drop(slot);
            let _ = self
                .tx
                .send(NetworkStatusEvent::KeyExpiryKnown { key_expiry });
        }
    }

    /// Record the control URL a status reported; announce the server when it
    /// is new or changed.
    fn learn_control_server(&self, url: &str) {
        let control_server = ControlServer::from_url(url);
        let mut slot = self
            .control_server
            .write()
            .unwrap_or_else(|p| p.into_inner());
        if slot.as_ref() != Some(&control_server) {
            *slot = Some(control_server.clone());
            drop(slot);
            let _ = self
                .tx
                .send(NetworkStatusEvent::ControlServerKnown { control_server });
        }
    }
}

/// The sidecar protocol whose running statuses carry the key expiry (and
/// whose statuses carry the control URL).
pub(crate) const KEY_EXPIRY_PROTOCOL_VERSION: u32 = 10;

/// The sidecar protocol that answers `tsnet:logout` and `tsnet:version`,
/// honours `clientLogUpload: false` at start, and reports Tailnet Lock and the
/// embedded Tailscale version.
pub(crate) const LOGOUT_PROTOCOL_VERSION: u32 = 11;

/// How long [`TailscaleProvider::logout`] waits for the control server to
/// take the logout before it stops the node anyway (the sidecar's own bound
/// is 10 s).
pub const LOGOUT_TIMEOUT: Duration = Duration::from_secs(12);

/// How long `start()` waits for a sidecar to say which protocol it speaks
/// when a start option depends on it.
const VERSION_PROBE_TIMEOUT: Duration = Duration::from_secs(5);

impl TailscaleProvider {
    /// Create a new TailscaleProvider with the given configuration.
    ///
    /// Does not start the provider — call [`start()`](crate::network::NetworkProvider::start) to begin.
    pub fn new(config: TailscaleConfig) -> Self {
        let (peer_event_tx, _) = broadcast::channel(256);
        let (proxy_error_tx, _) = broadcast::channel(64);
        let (status_tx, _) = broadcast::channel(16);

        // Seed the identity with the RFC 017 fields we already know from
        // the config. `tailscale_id`, `dns_name`, and `ip` are filled in
        // later when the sidecar reports `tsnet:status`.
        let initial_identity = NodeIdentity {
            app_id: config.app_id.clone(),
            device_id: config.device_id.clone(),
            device_name: config.device_name.clone(),
            tailscale_hostname: config.hostname.clone(),
            tailscale_id: String::new(),
            dns_name: None,
            ip: None,
            login_name: None,
        };

        Self {
            config,
            state: Arc::new(RwLock::new(ProviderState::Stopped)),
            identity: Arc::new(std::sync::RwLock::new(initial_identity)),
            local_addr: Arc::new(std::sync::RwLock::new(PeerAddr::default())),
            peers: Arc::new(RwLock::new(HashMap::new())),
            peer_event_tx,
            status: StatusReporting::new(status_tx),
            health: Arc::new(RwLock::new(HealthInfo {
                state: "stopped".to_string(),
                healthy: false,
                ..Default::default()
            })),
            sidecar: Arc::new(Mutex::new(None)),
            bridge: Arc::new(Mutex::new(None)),
            bridge_counters: Arc::default(),
            bridge_shutdown_tx: Arc::new(Mutex::new(None)),
            session_token: Arc::new(RwLock::new([0u8; 32])),
            local_tailscale_id: Arc::new(std::sync::RwLock::new(None)),
            proxy_error_tx,
            sidecar_protocol_version: Arc::new(std::sync::atomic::AtomicU32::new(0)),
            sidecar_pid: Arc::new(std::sync::atomic::AtomicU32::new(0)),
        }
    }

    /// The OS process id of the running Go sidecar.
    ///
    /// `None` before [`start`](crate::network::NetworkProvider::start)
    /// spawns it, after [`stop`](crate::network::NetworkProvider::stop),
    /// once the process has exited on its own (noticed within about a
    /// second), and once its status can no longer be checked (a few failed
    /// checks in a row, for example after it was reaped elsewhere). Lock-free
    /// and synchronous, so it is cheap to poll.
    pub fn sidecar_pid(&self) -> Option<u32> {
        match self.sidecar_pid.load(std::sync::atomic::Ordering::Relaxed) {
            0 => None,
            pid => Some(pid),
        }
    }

    /// Connections the data-plane bridge delivered or dropped, and why
    /// (cumulative since this provider was created). Lock-free and
    /// synchronous. Non-zero drop counts mean incoming connections were
    /// lost: e.g. `dropped_listener_full` when the app accepts too slowly.
    pub fn bridge_stats(&self) -> BridgeStats {
        self.bridge_counters.snapshot()
    }

    /// When this node's key expires, as the sidecar last reported it: when
    /// the node came up, and again whenever it changed (each change is also
    /// a [`NetworkStatusEvent::KeyExpiryKnown`]). `None` until the node runs,
    /// and always with a sidecar older than protocol 10. Lock-free to the
    /// caller and synchronous.
    pub fn key_expiry(&self) -> Option<KeyExpiry> {
        *self
            .status
            .key_expiry
            .read()
            .unwrap_or_else(|p| p.into_inner())
    }

    /// The coordination server this node logs in to: Tailscale's hosted
    /// service or a custom one such as Headscale (set through
    /// `TS_CONTROL_URL` in the environment the sidecar inherits). Known from
    /// the sidecar's first status, before any login or approval (also a
    /// [`NetworkStatusEvent::ControlServerKnown`]); `None` before
    /// `start()` and always with a sidecar older than protocol 10.
    pub fn control_server(&self) -> Option<ControlServer> {
        self.status
            .control_server
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    /// The Tailscale client this node's sidecar embeds. Known from the
    /// sidecar's first status, before any login; `None` before `start()`
    /// and always with a sidecar older than protocol 11. Lock-free to the
    /// caller and synchronous.
    pub fn tailscale_version(&self) -> Option<TailscaleVersion> {
        self.status
            .tailscale_version
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    /// Whether Tailnet Lock has this node locked out (it runs, but reaches no
    /// peer until a signing node signs its key), as the sidecar last reported
    /// it; each change is also a [`NetworkStatusEvent::NeedsSignature`] or
    /// [`NetworkStatusEvent::Signed`]. `Some(false)` on a tailnet without
    /// Tailnet Lock. `None` until the node runs, and always with a sidecar
    /// older than protocol 11.
    pub fn needs_signature(&self) -> Option<bool> {
        *self
            .status
            .locked_out
            .read()
            .unwrap_or_else(|p| p.into_inner())
    }

    /// Generate a random 32-byte session token.
    fn generate_session_token() -> Result<[u8; 32], NetworkError> {
        let mut token = [0u8; 32];
        getrandom::getrandom(&mut token).map_err(|e| {
            NetworkError::Internal(format!("failed to generate session token: {e}"))
        })?;
        Ok(token)
    }

    /// Convert a SidecarPeer to a NetworkPeer.
    fn sidecar_peer_to_network_peer(peer: &super::protocol::SidecarPeer) -> NetworkPeer {
        let ip = peer
            .tailscale_ips
            .first()
            .and_then(|s| s.parse::<IpAddr>().ok())
            .unwrap_or(IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED));

        NetworkPeer {
            id: peer.id.clone(),
            hostname: peer.hostname.clone(),
            ip,
            online: peer.online,
            active: peer.active,
            peer_relay: (!peer.peer_relay.is_empty()).then(|| peer.peer_relay.clone()),
            cur_addr: if peer.cur_addr.is_empty() {
                None
            } else {
                Some(peer.cur_addr.clone())
            },
            relay: if peer.relay.is_empty() {
                None
            } else {
                Some(peer.relay.clone())
            },
            os: if peer.os.is_empty() {
                None
            } else {
                Some(peer.os.clone())
            },
            last_seen: peer.last_seen.clone(),
            key_expiry: peer.key_expiry.clone(),
            dns_name: Some(peer.dns_name.clone()),
            // RFC 025 §3.3: absent, never fabricated. The sidecar omits the
            // key for an owner it cannot name; an empty string is the same
            // fact and must not become `Some("")` (the gate fails closed on
            // both, but callers read `None` as "unknown").
            login_name: peer.login_name.as_ref().filter(|l| !l.is_empty()).cloned(),
        }
    }

    /// RFC 025 §3.3: what one sidecar row means for the peer map.
    ///
    /// Admission and continued membership are NOT the same question, which is
    /// why this is three-valued rather than a boolean. A stranger with no
    /// login must stay out (fail closed); a peer we already hold must not be
    /// dropped because one status could not name its owner.
    pub(crate) fn classify_row(
        peer: &super::protocol::SidecarPeer,
        app_id: &str,
        login_allow: &[String],
    ) -> RowVerdict {
        if !is_app_peer(&peer.hostname, app_id) {
            return RowVerdict::NotOurApp;
        }
        if login_allow.is_empty() {
            // No gate: exactly the behaviour before RFC 025.
            return RowVerdict::Allowed;
        }
        match peer.login_name.as_deref().filter(|l| !l.is_empty()) {
            None => RowVerdict::LoginUnknown,
            Some(login) => {
                if login_allowed(login_allow, Some(login)) {
                    RowVerdict::Allowed
                } else {
                    RowVerdict::LoginRefused
                }
            }
        }
    }

    /// RFC 025 §3.3: may this row **become** a peer?
    ///
    /// Admission only. `login_allow` empty is today's behaviour exactly (no
    /// gate); under a gate a row with no login is not a peer, so a legacy
    /// sidecar cannot quietly widen the mesh (`start()` refuses that pairing
    /// outright — see [`login_gate_supported`]). For a row about a peer we
    /// ALREADY hold, ask [`Self::row_keeps_peer`] instead.
    pub(crate) fn admit_peer(
        peer: &super::protocol::SidecarPeer,
        app_id: &str,
        login_allow: &[String],
    ) -> bool {
        let verdict = Self::classify_row(peer, app_id, login_allow);
        if verdict != RowVerdict::Allowed {
            // Peer id and hostname only: the login itself is a tailnet
            // identity and does not belong in our logs.
            tracing::debug!(
                peer_id = %peer.id,
                hostname = %peer.hostname,
                ?verdict,
                "row not admitted (RFC 025 §3.3)"
            );
        }
        verdict == RowVerdict::Allowed
    }

    /// RFC 025 §3.3: does this row let a peer we ALREADY hold keep its place?
    ///
    /// A present login the gate refuses evicts — a device that changes hands
    /// keeps its stable node id, so a re-owned node arrives as an update of an
    /// admitted row and must leave the mesh, not linger. A row the sidecar
    /// could not name an owner for keeps the peer: a transient omission must
    /// not empty a gated mesh.
    pub(crate) fn row_keeps_peer(
        peer: &super::protocol::SidecarPeer,
        app_id: &str,
        login_allow: &[String],
    ) -> bool {
        matches!(
            Self::classify_row(peer, app_id, login_allow),
            RowVerdict::Allowed | RowVerdict::LoginUnknown
        )
    }

    /// Apply one `peerChanged` row to the map and emit the event RFC 025
    /// §3.3 requires.
    ///
    /// A row that newly passes **Joins**; a passing row for a peer we already
    /// hold **Updates**; a row whose PRESENT login the gate refuses **evicts**
    /// the peer with `Left` (the session then closes its connection). A row
    /// with no login never admits a stranger and never evicts a peer we hold.
    /// Nothing is ever silently kept-and-not-updated.
    fn apply_peer_row(
        peer_map: &mut HashMap<String, NetworkPeer>,
        peer_event_tx: &broadcast::Sender<NetworkPeerEvent>,
        peer: &super::protocol::SidecarPeer,
        app_id: &str,
        login_allow: &[String],
    ) {
        let already_held = peer_map.contains_key(&peer.id);
        let verdict = Self::classify_row(peer, app_id, login_allow);

        let keeps = match verdict {
            RowVerdict::Allowed => true,
            RowVerdict::LoginUnknown => already_held,
            RowVerdict::LoginRefused | RowVerdict::NotOurApp => false,
        };

        if keeps {
            // RFC 022 honesty: the row names no owner, so neither do we. The
            // ENTRY survives an unknown login (below); the FIELD does not lie
            // about one. Layer 5 refuses to open a new connection to a peer
            // whose login it cannot state on a gated node.
            let np = Self::sidecar_peer_to_network_peer(peer);
            peer_map.insert(np.id.clone(), np.clone());
            let _ = peer_event_tx.send(if already_held {
                NetworkPeerEvent::Updated(np)
            } else {
                NetworkPeerEvent::Joined(np)
            });
            return;
        }

        if peer_map.remove(&peer.id).is_some() {
            // Operator-visible: a peer leaving the mesh because its owner
            // changed is a fact someone will look for in a log. Peer id and
            // hostname only — the login is a tailnet identity and never
            // reaches our logs.
            tracing::info!(
                peer_id = %peer.id,
                hostname = %peer.hostname,
                ?verdict,
                "peer evicted — no longer admitted (RFC 025 §3.3)"
            );
            let _ = peer_event_tx.send(NetworkPeerEvent::Left(peer.id.clone()));
        } else {
            tracing::debug!(
                peer_id = %peer.id,
                hostname = %peer.hostname,
                ?verdict,
                "row not admitted (RFC 025 §3.3)"
            );
        }
    }

    /// Spawn the background event processing loop that maps sidecar events
    /// to peer events and updates cached state.
    #[allow(clippy::too_many_arguments)]
    fn spawn_event_processor(
        mut sidecar_rx: broadcast::Receiver<SidecarInternalEvent>,
        peers: Arc<RwLock<HashMap<String, NetworkPeer>>>,
        peer_event_tx: broadcast::Sender<NetworkPeerEvent>,
        health: Arc<RwLock<HealthInfo>>,
        identity: Arc<std::sync::RwLock<NodeIdentity>>,
        local_addr: Arc<std::sync::RwLock<PeerAddr>>,
        local_tailscale_id: Arc<std::sync::RwLock<Option<String>>>,
        state: Arc<RwLock<ProviderState>>,
        started_tx: Option<oneshot::Sender<Result<(), NetworkError>>>,
        app_id: String,
        login_allow: Vec<String>,
        proxy_error_tx: broadcast::Sender<ProxyRuntimeError>,
        sidecar_protocol_version: Arc<std::sync::atomic::AtomicU32>,
        status: StatusReporting,
    ) {
        tokio::spawn(async move {
            let mut started_tx = started_tx;

            loop {
                match sidecar_rx.recv().await {
                    Ok(event) => {
                        match event {
                            SidecarInternalEvent::Started {
                                hostname,
                                dns_name,
                                tailscale_ip,
                                node_id,
                                login_name,
                                protocol_version,
                                key_expiry,
                                control_url,
                                tailscale_version,
                            } => {
                                status.learn_tailscale_version(tailscale_version);
                                // 0 = v1/unknown; gates RFC 023 v2 proxy features.
                                sidecar_protocol_version.store(
                                    protocol_version.unwrap_or(0),
                                    std::sync::atomic::Ordering::Relaxed,
                                );
                                let ip: Option<IpAddr> = tailscale_ip.parse().ok();

                                let mut login_learned = None;
                                {
                                    let mut id = identity.write().unwrap();
                                    // `tailscale_hostname` is already populated
                                    // from the config at construction time. We
                                    // overwrite it with whatever the sidecar
                                    // actually registered (Tailscale may append
                                    // `-2`, `-3`, … for hostname collisions).
                                    id.tailscale_hostname = hostname.clone();
                                    id.dns_name = Some(dns_name.clone());
                                    id.ip = ip;
                                    if !node_id.is_empty() {
                                        id.tailscale_id = node_id.clone();
                                    }
                                    // RFC 025 §3.6: our own login, from the
                                    // same status event. `None` stays `None`
                                    // — never fabricated, and never cleared
                                    // by a later status that omits it.
                                    if login_name.is_some() && id.login_name != login_name {
                                        id.login_name = login_name.clone();
                                        login_learned = login_name.clone();
                                    }
                                }
                                // Approved (or never needed approval): the
                                // start-timeout message no longer names it.
                                status
                                    .needs_approval
                                    .store(false, std::sync::atomic::Ordering::Relaxed);
                                if let Some(login_name) = login_learned {
                                    // Usually during start; after a fast
                                    // restart (sidecar protocol 8) it can be
                                    // a later status, after start() returned.
                                    let _ = status
                                        .tx
                                        .send(NetworkStatusEvent::LoginKnown { login_name });
                                }

                                // Capture the local Tailscale stable ID for
                                // self-filtering in the peer event chain.
                                if !node_id.is_empty() {
                                    *local_tailscale_id.write().unwrap() = Some(node_id);
                                }

                                {
                                    let mut addr = local_addr.write().unwrap();
                                    addr.hostname = hostname;
                                    addr.dns_name = Some(dns_name);
                                    addr.ip = ip;
                                }

                                // Protocol 10: the coordination server, and
                                // our key expiry (absent = never expires).
                                if let Some(url) = &control_url {
                                    status.learn_control_server(url);
                                }
                                let reports_expiry =
                                    protocol_version.unwrap_or(0) >= KEY_EXPIRY_PROTOCOL_VERSION;
                                let parsed_expiry = if reports_expiry {
                                    let parsed = KeyExpiry::from_wire(key_expiry.as_deref());
                                    if parsed.is_none() {
                                        tracing::warn!(
                                            ?key_expiry,
                                            "sidecar reported an unreadable key expiry; ignoring it"
                                        );
                                    }
                                    parsed
                                } else {
                                    None
                                };

                                {
                                    let mut h = health.write().await;
                                    h.state = "running".to_string();
                                    h.healthy = true;
                                    if parsed_expiry.is_some() {
                                        h.key_expiry = key_expiry.filter(|k| !k.is_empty());
                                    }
                                }
                                if let Some(expiry) = parsed_expiry {
                                    status.learn_key_expiry(expiry);
                                }

                                *state.write().await = ProviderState::Running;

                                // Signal start() that we're ready
                                if let Some(tx) = started_tx.take() {
                                    let _ = tx.send(Ok(()));
                                }
                            }
                            SidecarInternalEvent::AuthRequired { auth_url } => {
                                tracing::info!("tailscale auth required: {auth_url}");
                                // Emit auth URL via peer events so callers can display it.
                                // Do NOT consume started_tx — keep waiting for Running state.
                                let _ = peer_event_tx
                                    .send(NetworkPeerEvent::AuthRequired { url: auth_url });
                            }
                            SidecarInternalEvent::NeedsApproval => {
                                // Device approval is on for this tailnet and
                                // an admin has not approved this device yet.
                                // start() keeps waiting (approval completes
                                // it); callers learn why via status_events().
                                tracing::warn!(
                                    "tailscale: this device is waiting for approval \
                                     in the admin console"
                                );
                                status
                                    .needs_approval
                                    .store(true, std::sync::atomic::Ordering::Relaxed);
                                let _ = status.tx.send(NetworkStatusEvent::NeedsApproval);
                            }
                            SidecarInternalEvent::Stopped => {
                                *state.write().await = ProviderState::Stopped;
                                let mut h = health.write().await;
                                h.state = "stopped".to_string();
                                h.healthy = false;
                                tracing::info!("tailscale provider stopped");
                                return;
                            }
                            SidecarInternalEvent::StateChange {
                                state: new_state,
                                control_url,
                                tailscale_version,
                            } => {
                                {
                                    let mut h = health.write().await;
                                    h.state = new_state;
                                }
                                // The "starting" status names the server
                                // before any login URL or approval event,
                                // and (protocol 11) the Tailscale client.
                                if let Some(url) = &control_url {
                                    status.learn_control_server(url);
                                }
                                status.learn_tailscale_version(tailscale_version);
                            }
                            SidecarInternalEvent::BackendStateChanged { state: new_state } => {
                                {
                                    let mut h = health.write().await;
                                    h.state = new_state.clone();
                                }
                                let _ = status.tx.send(NetworkStatusEvent::BackendStateChanged {
                                    state: BackendState::from_wire(&new_state),
                                });
                            }
                            SidecarInternalEvent::LockStatus { enabled, signed } => {
                                status.learn_lock(enabled, signed);
                            }
                            SidecarInternalEvent::LoggingRequired => {
                                // Only an opted-out node hears this (the
                                // sidecar checks); tsnet has stopped it.
                                tracing::error!(
                                    "tailscale: this tailnet requires client logging (network \
                                     flow logs are on), and the node opted out of it"
                                );
                                let _ = status.tx.send(NetworkStatusEvent::LoggingRequired);
                                if let Some(tx) = started_tx.take() {
                                    let _ = tx.send(Err(NetworkError::StartFailed(
                                        "this tailnet requires client logging (network flow \
                                         logs are on), and the node opted out of it with \
                                         client_log_upload(false); allow uploads, or turn the \
                                         tailnet's flow logs off"
                                            .into(),
                                    )));
                                }
                            }
                            SidecarInternalEvent::KeyExpiring { expires_at } => {
                                let mut h = health.write().await;
                                h.key_expiry = Some(expires_at);
                            }
                            SidecarInternalEvent::HealthWarning { warnings } => {
                                let mut h = health.write().await;
                                h.warnings = warnings;
                                h.healthy = h.warnings.is_empty();
                            }
                            SidecarInternalEvent::PeersReceived(sidecar_peers) => {
                                let mut peer_map = peers.write().await;
                                // Self-filter by Tailscale stable ID, not by
                                // hostname — hostname collisions from crashed/
                                // restarted dev runs can cause the local node
                                // to appear as its own peer under a different
                                // Tailscale ID.
                                let self_id = local_tailscale_id.read().unwrap().clone();
                                // Filter to peers that belong to our app AND
                                // are not ourselves.
                                // RFC 025 §3.3: a stranger must pass the gate
                                // to be admitted, while a peer we already hold
                                // keeps its place through a row the sidecar
                                // could not name an owner for — and is evicted
                                // (below, as `Left`) by a login that is PRESENT
                                // and refused.
                                let new_peers: HashMap<String, NetworkPeer> = sidecar_peers
                                    .iter()
                                    .filter(|p| {
                                        if let Some(ref me) = self_id {
                                            if p.id == *me {
                                                return false;
                                            }
                                        }
                                        if peer_map.contains_key(&p.id) {
                                            Self::row_keeps_peer(p, &app_id, &login_allow)
                                        } else {
                                            Self::admit_peer(p, &app_id, &login_allow)
                                        }
                                    })
                                    .map(|p| {
                                        // Absent, never fabricated — see
                                        // `apply_peer_row`.
                                        let np = Self::sidecar_peer_to_network_peer(p);
                                        (np.id.clone(), np)
                                    })
                                    .collect();

                                // Detect joins, leaves, and updates
                                for (id, new_peer) in &new_peers {
                                    if let Some(_existing) = peer_map.get(id) {
                                        let _ = peer_event_tx
                                            .send(NetworkPeerEvent::Updated(new_peer.clone()));
                                    } else {
                                        let _ = peer_event_tx
                                            .send(NetworkPeerEvent::Joined(new_peer.clone()));
                                    }
                                }
                                for id in peer_map.keys() {
                                    if !new_peers.contains_key(id) {
                                        let _ =
                                            peer_event_tx.send(NetworkPeerEvent::Left(id.clone()));
                                    }
                                }

                                *peer_map = new_peers;
                            }
                            SidecarInternalEvent::PeerChanged(change) => {
                                let mut peer_map = peers.write().await;
                                // Self-filter by Tailscale stable ID, not
                                // by hostname — see comment in PeersReceived.
                                let self_id = local_tailscale_id.read().unwrap().clone();
                                match change.change_type.as_str() {
                                    "joined" => {
                                        if let Some(p) = change.peer {
                                            if let Some(ref me) = self_id {
                                                if p.id == *me {
                                                    continue;
                                                }
                                            }
                                            Self::apply_peer_row(
                                                &mut peer_map,
                                                &peer_event_tx,
                                                &p,
                                                &app_id,
                                                &login_allow,
                                            );
                                        }
                                    }
                                    "left" => {
                                        if peer_map.remove(&change.peer_id).is_some() {
                                            let _ = peer_event_tx
                                                .send(NetworkPeerEvent::Left(change.peer_id));
                                        }
                                    }
                                    "updated" => {
                                        if let Some(p) = change.peer {
                                            if let Some(ref me) = self_id {
                                                if p.id == *me {
                                                    continue;
                                                }
                                            }
                                            Self::apply_peer_row(
                                                &mut peer_map,
                                                &peer_event_tx,
                                                &p,
                                                &app_id,
                                                &login_allow,
                                            );
                                        }
                                    }
                                    other => {
                                        tracing::warn!("unknown peer change type: {other}");
                                    }
                                }
                            }
                            SidecarInternalEvent::Error { code, message } => {
                                tracing::error!("sidecar error [{code}]: {message}");
                                // If start() is still waiting and this is a fatal error
                                if let Some(tx) = started_tx.take() {
                                    let _ = tx.send(Err(NetworkError::SidecarError(format!(
                                        "[{code}] {message}"
                                    ))));
                                }
                            }
                            SidecarInternalEvent::ProcessExited { exit_code } => {
                                tracing::error!("sidecar process exited: {exit_code:?}");
                                *state.write().await = ProviderState::Stopped;
                                {
                                    let mut h = health.write().await;
                                    h.state = "crashed".to_string();
                                    h.healthy = false;
                                }
                                if let Some(tx) = started_tx.take() {
                                    let _ = tx.send(Err(NetworkError::SidecarError(format!(
                                        "process exited with code {exit_code:?}"
                                    ))));
                                }
                                // The node is gone for good (no restart):
                                // tell the app, which otherwise keeps showing
                                // a running node that serves nothing.
                                let _ = status
                                    .tx
                                    .send(NetworkStatusEvent::SidecarExited { exit_code });
                                return;
                            }
                            SidecarInternalEvent::ProxyError { id, code, message } => {
                                // Runtime engine errors (RFC 023 G5). Add-time
                                // failures are also seen (and returned) by the
                                // proxy_add wait loop; the Node-side forwarder
                                // drops events for ids it never saw start.
                                tracing::warn!("proxy runtime error [{code}] for {id}: {message}");
                                let _ =
                                    proxy_error_tx.send(ProxyRuntimeError { id, code, message });
                            }
                            SidecarInternalEvent::EgressError { id, code, message }
                                if !id.is_empty() =>
                            {
                                // RFC 026: a route refused (or could not dial) an
                                // accepted connection — or an egress command failed;
                                // the broker tees command answers here too. Neither
                                // is a node fault, so a trace, not a warning.
                                tracing::debug!("egress [{code}] for {id}: {message}");
                            }
                            // Dial/Listen/Ping results are handled by the caller,
                            // not the background event processor
                            _ => {}
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!("event processor lagged by {n} events");
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        tracing::info!("sidecar event channel closed, stopping event processor");
                        return;
                    }
                }
            }
        });
    }
}

impl TailscaleProvider {
    /// Ask a freshly spawned sidecar which protocol it speaks, before any
    /// start (`tsnet:version`, protocol 11), and learn the Tailscale client
    /// it embeds. `None`: a sidecar older than 11, which does not know the
    /// command and answers `UNKNOWN_CMD` without a `requestId`.
    async fn probe_protocol(&self, sidecar: &GoSidecar) -> Result<Option<u32>, NetworkError> {
        let request_id = uuid::Uuid::new_v4().to_string();
        let mut events = sidecar.subscribe();
        let mut reply = sidecar.register_reply(&request_id);
        sidecar
            .send_request(command_type::VERSION, &request_id)
            .await?;
        let answer = async {
            loop {
                tokio::select! {
                    routed = reply.recv() => return match routed? {
                        SidecarInternalEvent::VersionResult { protocol_version, tailscale_version } => {
                            self.status.learn_tailscale_version(tailscale_version);
                            Ok(Some(protocol_version))
                        }
                        other => Err(Self::unexpected_reply("tsnet:version", other)),
                    },
                    event = events.recv() => match event {
                        Ok(SidecarInternalEvent::Error { code, .. }) if code == "UNKNOWN_CMD" => {
                            return Ok(None);
                        }
                        Ok(SidecarInternalEvent::ProcessExited { exit_code }) => {
                            return Err(NetworkError::SidecarError(format!(
                                "process exited with code {exit_code:?} before saying its version"
                            )));
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            return Err(NetworkError::SidecarError(
                                "sidecar events closed before it said its version".into(),
                            ));
                        }
                        _ => {}
                    },
                }
            }
        };
        tokio::time::timeout(VERSION_PROBE_TIMEOUT, answer)
            .await
            .map_err(|_| {
                NetworkError::SidecarError(format!(
                    "the sidecar did not say its version within {VERSION_PROBE_TIMEOUT:?}"
                ))
            })?
    }

    /// Sign this node out of its tailnet, then stop it.
    ///
    /// The control server expires the node's key: the device stops counting
    /// as signed in on the admin console's Machines page, and an ephemeral
    /// node is removed. Then the provider stops, as
    /// [`stop`](crate::network::NetworkProvider::stop) does. Logging in again
    /// takes a new login (or auth key); the state directory keeps nothing
    /// usable.
    ///
    /// Bounded like `stop`: if the control server does not take the logout
    /// within [`LOGOUT_TIMEOUT`] (offline, say), the node is stopped anyway
    /// and the error says the logout did not happen — the key then stays
    /// valid until it expires or an admin removes the device. Needs sidecar
    /// protocol 11; on an older sidecar it fails with
    /// [`NetworkError::Unsupported`] and stops nothing.
    pub async fn logout(&self) -> Result<(), NetworkError> {
        if *self.state.read().await != ProviderState::Running {
            return Err(NetworkError::NotRunning);
        }
        let version = self.sidecar_version();
        if version < LOGOUT_PROTOCOL_VERSION {
            return Err(NetworkError::Unsupported(format!(
                "logout requires sidecar protocol {LOGOUT_PROTOCOL_VERSION}; this sidecar \
                 speaks {version}"
            )));
        }
        let outcome = self.logout_rpc().await;
        if let Err(e) = &outcome {
            tracing::warn!("tailscale: logout failed, stopping the node anyway: {e}");
        }
        if let Err(e) = super::super::NetworkProvider::stop(self).await {
            tracing::warn!("tailscale: stop after logout failed: {e}");
        }
        outcome
    }

    async fn logout_rpc(&self) -> Result<(), NetworkError> {
        let request_id = uuid::Uuid::new_v4().to_string();
        let mut reply = {
            let sidecar_guard = self.sidecar.lock().await;
            let sidecar = sidecar_guard.as_ref().ok_or(NetworkError::NotRunning)?;
            let reply = sidecar.register_reply(&request_id);
            sidecar
                .send_request(command_type::LOGOUT, &request_id)
                .await?;
            reply
        };
        let answer = tokio::time::timeout(LOGOUT_TIMEOUT, reply.recv())
            .await
            .map_err(|_| {
                NetworkError::SidecarError(format!(
                    "logout: the control server did not answer within {LOGOUT_TIMEOUT:?}"
                ))
            })??;
        match answer {
            SidecarInternalEvent::LoggedOut => Ok(()),
            SidecarInternalEvent::Error { code, message } if code == "NOT_RUNNING" => {
                tracing::debug!("logout: {message}");
                Err(NetworkError::NotRunning)
            }
            SidecarInternalEvent::Error { code, message } => Err(NetworkError::SidecarError(
                format!("logout failed [{code}]: {message}"),
            )),
            other => Err(Self::unexpected_reply("tsnet:logout", other)),
        }
    }
}

/// Check if a hostname belongs to a truffle node in the given app.
///
/// RFC 017 §4: every truffle-managed Tailscale hostname has the shape
/// `truffle-{app_id}-{slug(device_name)}`. The prefix `truffle-{app_id}-`
/// is used to admit peers from our own application and reject peers from
/// other apps on the same tailnet. A hostname that matches the prefix but
/// has no trailing slug is rejected — we require at least one character
/// after the separator so that `truffle-playground-` (empty slug edge)
/// cannot masquerade as a real peer.
pub(crate) fn is_app_peer(hostname: &str, app_id: &str) -> bool {
    let prefix = format!("truffle-{app_id}-");
    hostname.len() > prefix.len() && hostname.starts_with(&prefix)
}

/// What one sidecar peer row means under the node's login gate (RFC 025 §3.3).
///
/// Three-valued on purpose: "may this row be admitted?" and "may a peer we
/// already hold stay?" have different answers when the sidecar reports no
/// login at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RowVerdict {
    /// Ours, and the login passes (or there is no gate).
    Allowed,
    /// Ours, gated, and the row carries a login the globs refuse. Admission
    /// is denied and an already-admitted peer is EVICTED — a device that
    /// changes hands keeps its node id, so this is how a re-owned node leaves.
    LoginRefused,
    /// Ours, gated, and the row carries no login at all. Never admits; never
    /// evicts a peer we already hold.
    LoginUnknown,
    /// Not this app's hostname prefix.
    NotOurApp,
}

/// The first sidecar protocol that reports `loginName` on peers and status
/// (RFC 025 §4). Below it a node cannot know any login.
pub(crate) const LOGIN_NAME_PROTOCOL_VERSION: u32 = 5;

/// RFC 025 D3: may a node with this `login_allow` run against a sidecar
/// speaking `protocol_version`?
///
/// An ungated node is unaffected — every version qualifies. A gated one needs
/// protocol 5: below it every row arrives login-less, the gate fails closed on
/// all of them, and the node would run silently peerless. `start()` turns the
/// `Err` into a refusal rather than that. `protocol_version` is `0` for a
/// sidecar that reported none (v1/unknown).
pub(crate) fn login_gate_supported(
    login_allow: &[String],
    protocol_version: u32,
) -> Result<(), NetworkError> {
    if login_allow.is_empty() || protocol_version >= LOGIN_NAME_PROTOCOL_VERSION {
        return Ok(());
    }
    Err(NetworkError::StartFailed(format!(
        "login_allow requires sidecar protocol {LOGIN_NAME_PROTOCOL_VERSION} \
         (loginName on peers); this sidecar speaks {protocol_version}"
    )))
}

impl super::super::NetworkProvider for TailscaleProvider {
    async fn start(&mut self) -> Result<(), NetworkError> {
        {
            let current_state = *self.state.read().await;
            if current_state != ProviderState::Stopped {
                return Err(NetworkError::AlreadyRunning);
            }
        }
        *self.state.write().await = ProviderState::Starting;

        // Generate session token
        let token = Self::generate_session_token()?;
        let token_hex = hex::encode(token);
        *self.session_token.write().await = token;

        // Start the bridge
        let bridge = Bridge::bind_with_counters(token, self.bridge_counters.clone()).await?;
        let bridge_port = bridge.local_port()?;
        let bridge = Arc::new(bridge);

        // Create bridge shutdown channel
        let (bridge_shutdown_tx, bridge_shutdown_rx) = tokio::sync::watch::channel(false);

        // Run bridge accept loop
        {
            let bridge_clone = bridge.clone();
            tokio::spawn(async move {
                bridge_clone.run(bridge_shutdown_rx).await;
            });
        }

        *self.bridge.lock().await = Some(bridge.clone());
        *self.bridge_shutdown_tx.lock().await = Some(bridge_shutdown_tx);

        // Build sidecar config
        let sidecar_config = SidecarConfig {
            binary_path: self.config.binary_path.clone(),
            hostname: self.config.hostname.clone(),
            state_dir: self.config.state_dir.clone(),
            auth_key: self.config.auth_key.clone(),
            bridge_port,
            session_token_hex: token_hex,
            ephemeral: self.config.ephemeral,
            tags: self.config.tags.clone(),
            idle_timeout_secs: self.config.idle_timeout_secs,
            client_log_upload: (!self.config.client_log_upload).then_some(false),
        };

        // Spawn the sidecar
        let (sidecar, sidecar_rx) =
            GoSidecar::spawn(sidecar_config.clone(), self.sidecar_pid.clone()).await?;

        // An older sidecar would ignore `clientLogUpload: false` and upload
        // the logs it was asked not to, so the opt-out is checked before the
        // start command, not after (the way `login_allow` is): nothing has
        // started yet when an older sidecar is refused.
        if !self.config.client_log_upload {
            let refusal = match self.probe_protocol(&sidecar).await {
                Ok(Some(version)) if version >= LOGOUT_PROTOCOL_VERSION => None,
                Ok(version) => Some(NetworkError::StartFailed(format!(
                    "client_log_upload(false) requires sidecar protocol \
                     {LOGOUT_PROTOCOL_VERSION}; this sidecar speaks {}",
                    version.map_or_else(|| "an older one".to_string(), |v| v.to_string())
                ))),
                Err(e) => Some(e),
            };
            if let Some(e) = refusal {
                tracing::error!("{e}");
                sidecar.shutdown().await;
                if let Some(tx) = self.bridge_shutdown_tx.lock().await.take() {
                    let _ = tx.send(true);
                }
                *self.bridge.lock().await = None;
                *self.state.write().await = ProviderState::Stopped;
                return Err(e);
            }
        }

        // Create a channel for the event processor to signal when we're running
        let (started_tx, started_rx) = oneshot::channel();

        // Start event processor
        Self::spawn_event_processor(
            sidecar_rx,
            self.peers.clone(),
            self.peer_event_tx.clone(),
            self.health.clone(),
            self.identity.clone(),
            self.local_addr.clone(),
            self.local_tailscale_id.clone(),
            self.state.clone(),
            Some(started_tx),
            self.config.app_id.clone(),
            self.config.login_allow.clone(),
            self.proxy_error_tx.clone(),
            self.sidecar_protocol_version.clone(),
            self.status.clone(),
        );

        // Send start command to sidecar
        sidecar.send_start(&sidecar_config).await?;

        *self.sidecar.lock().await = Some(sidecar);

        // Wait for the sidecar to reach "running" state, bounded by the
        // configured start timeout (default 5 min, because browser auth may
        // take a while; `None` = wait until running or until this future is
        // dropped). Auth URLs are emitted via peer_events() — again each time
        // the sidecar reports a new one — so the caller can display them.
        let needs_approval = self.status.needs_approval.clone();
        let started = match self.config.start_timeout {
            Some(limit) => tokio::time::timeout(limit, started_rx).await.map_err(|_| {
                NetworkError::StartFailed(
                    if needs_approval.load(std::sync::atomic::Ordering::Relaxed) {
                        format!(
                            "timed out waiting for device approval ({limit:?}): approve \
                             this device in the Tailscale admin console, or use a \
                             pre-approved auth key. Subscribe to status_events() to show \
                             this while waiting."
                        )
                    } else {
                        format!(
                            "timed out waiting for authentication ({limit:?}). Subscribe \
                             to peer_events() to display auth URLs, or set \
                             start_timeout(None) to wait indefinitely."
                        )
                    },
                )
            })?,
            None => started_rx.await,
        };
        let result = started
            .map_err(|_| NetworkError::StartFailed("start signal channel dropped".into()))?;

        match result {
            Ok(()) => {
                // RFC 025 D3: a gated node whose sidecar cannot report logins
                // would run peerless (every row fails the gate closed) or,
                // read carelessly, ungated. Refuse loudly instead, and tear
                // the pair down rather than leave a half-started node behind.
                if let Err(e) = login_gate_supported(
                    &self.config.login_allow,
                    self.sidecar_protocol_version
                        .load(std::sync::atomic::Ordering::Relaxed),
                ) {
                    tracing::error!("{e}");
                    let _ = self.stop().await;
                    return Err(e);
                }

                // Fetch initial peer list
                if let Some(ref sidecar) = *self.sidecar.lock().await {
                    let _ = sidecar.send_get_peers().await;
                    // Also start WatchIPNBus for real-time peer events
                    let _ = sidecar.send_watch_peers().await;
                }
                tracing::info!("tailscale provider started successfully");
                Ok(())
            }
            Err(e) => {
                *self.state.write().await = ProviderState::Stopped;
                Err(e)
            }
        }
    }

    async fn stop(&self) -> Result<(), NetworkError> {
        *self.state.write().await = ProviderState::Stopping;

        // Shut down sidecar
        if let Some(sidecar) = self.sidecar.lock().await.take() {
            sidecar.shutdown().await;
        }

        // Shut down bridge
        if let Some(tx) = self.bridge_shutdown_tx.lock().await.take() {
            let _ = tx.send(true);
        }
        *self.bridge.lock().await = None;

        // Clear state
        self.peers.write().await.clear();
        *self.state.write().await = ProviderState::Stopped;
        let mut h = self.health.write().await;
        h.state = "stopped".to_string();
        h.healthy = false;

        tracing::info!("tailscale provider stopped");
        Ok(())
    }

    fn local_identity(&self) -> NodeIdentity {
        self.identity.read().unwrap().clone()
    }

    fn local_addr(&self) -> PeerAddr {
        self.local_addr.read().unwrap().clone()
    }

    fn peer_events(&self) -> broadcast::Receiver<NetworkPeerEvent> {
        self.peer_event_tx.subscribe()
    }

    fn status_events(&self) -> broadcast::Receiver<NetworkStatusEvent> {
        self.status.tx.subscribe()
    }

    async fn peers(&self) -> Vec<NetworkPeer> {
        self.peers.read().await.values().cloned().collect()
    }

    async fn dial_tcp(&self, addr: &str, port: u16) -> Result<TcpStream, NetworkError> {
        self.dial_tcp_opts(addr, port, DialOpts::default()).await
    }

    async fn dial_tcp_opts(
        &self,
        addr: &str,
        port: u16,
        opts: DialOpts,
    ) -> Result<TcpStream, NetworkError> {
        if *self.state.read().await != ProviderState::Running {
            return Err(NetworkError::NotRunning);
        }

        let bridge = self
            .bridge
            .lock()
            .await
            .clone()
            .ok_or(NetworkError::NotRunning)?;

        // Generate a unique request ID
        let request_id = uuid::Uuid::new_v4().to_string();

        // Register the pending dial before sending the command
        let dial_rx = bridge.register_dial(request_id.clone()).await;

        // Scope the sidecar lock: register the reply slot + send, then release
        let reply = {
            let sidecar_guard = self.sidecar.lock().await;
            let sidecar = sidecar_guard.as_ref().ok_or(NetworkError::NotRunning)?;

            let reply = sidecar.register_reply(&request_id);

            sidecar
                .send_dial(request_id.clone(), addr.to_string(), port, opts.tls)
                .await?;

            reply
        };

        // Wait for either:
        // 1. Bridge delivers the TcpStream (success path)
        // 2. Sidecar reports the dial result via the broker (error path)
        // 3. Timeout
        Self::await_dial_result(&bridge, &request_id, dial_rx, reply, DIAL_TIMEOUT).await
    }

    async fn listen_tcp(&self, port: u16) -> Result<NetworkTcpListener, NetworkError> {
        self.listen_tcp_opts(port, ListenOpts::default()).await
    }

    async fn listen_tcp_opts(
        &self,
        port: u16,
        opts: ListenOpts,
    ) -> Result<NetworkTcpListener, NetworkError> {
        if *self.state.read().await != ProviderState::Running {
            return Err(NetworkError::NotRunning);
        }

        let bridge = self
            .bridge
            .lock()
            .await
            .clone()
            .ok_or(NetworkError::NotRunning)?;

        // Create channel for incoming connections
        let (tx, rx) = mpsc::channel::<IncomingConnection>(64);

        // `Some(true)` → tsnet ListenTLS with MagicDNS certs (RFC 023
        // §7.1). Plain listeners send None so the field is omitted on
        // the wire — sidecars predating the flag parse the command
        // unchanged.
        let tls = if opts.tls { Some(true) } else { None };

        // v4 sidecars echo a correlation id on the Listening event, so the
        // reply routes through the broker: immune to broadcast lag, and to
        // the port-0 ambiguity where two concurrent ephemeral listens could
        // steal each other's confirmations. Older sidecars fall back to
        // port-matched value correlation.
        let actual_port = if self.sidecar_version() >= Self::SIDECAR_V4_REPLY_ROUTING {
            let request_id = uuid::Uuid::new_v4().to_string();
            let mut reply = {
                let sidecar_guard = self.sidecar.lock().await;
                let sidecar = sidecar_guard.as_ref().ok_or(NetworkError::NotRunning)?;
                let reply = sidecar.register_reply(&request_id);
                sidecar
                    .send_listen(port, tls, Some(request_id.clone()))
                    .await?;
                reply
            };
            match tokio::time::timeout(Duration::from_secs(10), reply.recv())
                .await
                .map_err(|_| NetworkError::ListenFailed("listen confirmation timed out".into()))??
            {
                // When port is 0, the sidecar assigns an ephemeral port and
                // reports the actual port here.
                SidecarInternalEvent::Listening { port: p } => p,
                SidecarInternalEvent::Error { code, message } => {
                    return Err(NetworkError::ListenFailed(format!("[{code}] {message}")));
                }
                other => return Err(Self::unexpected_reply("listen", other)),
            }
        } else {
            // Legacy pre-v4 path: value correlation over broadcast.
            let mut event_rx = {
                let sidecar_guard = self.sidecar.lock().await;
                let sidecar = sidecar_guard.as_ref().ok_or(NetworkError::NotRunning)?;

                let event_rx = sidecar.subscribe();
                sidecar.send_listen(port, tls, None).await?;
                event_rx
            };

            tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    match event_rx.recv().await {
                        Ok(SidecarInternalEvent::Listening { port: p })
                            if port == 0 || p == port =>
                        {
                            return Ok(p);
                        }
                        Ok(SidecarInternalEvent::Error { code, message }) => {
                            return Err(NetworkError::ListenFailed(format!("[{code}] {message}")));
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            return Err(NetworkError::SidecarError("event channel closed".into()));
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => {
                            return Err(NetworkError::SidecarError(
                                "event channel lagged: listen confirmation may have been lost"
                                    .into(),
                            ));
                        }
                        _ => continue,
                    }
                }
            })
            .await
            .map_err(|_| NetworkError::ListenFailed("listen confirmation timed out".into()))??
        };

        // Register the channel with the bridge using the actual port
        bridge.register_listener(actual_port, tx).await;

        Ok(NetworkTcpListener {
            port: actual_port,
            incoming: rx,
        })
    }

    async fn unlisten_tcp(&self, port: u16) -> Result<(), NetworkError> {
        if *self.state.read().await != ProviderState::Running {
            return Err(NetworkError::NotRunning);
        }

        let bridge = self
            .bridge
            .lock()
            .await
            .clone()
            .ok_or(NetworkError::NotRunning)?;

        // Remove bridge listener
        bridge.remove_listener(port).await;

        // Tell sidecar to stop listening
        {
            let sidecar_guard = self.sidecar.lock().await;
            let sidecar = sidecar_guard.as_ref().ok_or(NetworkError::NotRunning)?;
            sidecar.send_unlisten(port).await?;
        }

        Ok(())
    }

    async fn ping(&self, addr: &str) -> Result<PingResult, NetworkError> {
        if *self.state.read().await != ProviderState::Running {
            return Err(NetworkError::NotRunning);
        }

        let target = addr.to_string();

        // v2+ sidecars echo a correlation id on the ping result (P12), so
        // the reply routes through the broker — immune to broadcast lag.
        // Older sidecars fall back to target-matched value correlation.
        if self.sidecar_version() >= Self::SIDECAR_V2_PING_ECHO {
            let request_id = uuid::Uuid::new_v4().to_string();
            let mut reply = {
                let sidecar_guard = self.sidecar.lock().await;
                let sidecar = sidecar_guard.as_ref().ok_or(NetworkError::NotRunning)?;
                let reply = sidecar.register_reply(&request_id);
                sidecar
                    .send_ping(target, Some("disco".into()), Some(request_id.clone()))
                    .await?;
                reply
            };
            return match tokio::time::timeout(Duration::from_secs(15), reply.recv())
                .await
                .map_err(|_| NetworkError::PingFailed("ping timed out".into()))??
            {
                SidecarInternalEvent::PingResult(data) => Self::map_ping_result(data),
                SidecarInternalEvent::Error { code, message } => {
                    Err(NetworkError::PingFailed(format!("[{code}] {message}")))
                }
                other => Err(Self::unexpected_reply("ping", other)),
            };
        }

        // Legacy pre-v2 path: value correlation over broadcast.
        let mut event_rx = {
            let sidecar_guard = self.sidecar.lock().await;
            let sidecar = sidecar_guard.as_ref().ok_or(NetworkError::NotRunning)?;

            let event_rx = sidecar.subscribe();
            sidecar
                .send_ping(target.clone(), Some("disco".into()), None)
                .await?;
            event_rx
        };

        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                match event_rx.recv().await {
                    Ok(SidecarInternalEvent::PingResult(data)) if data.target == target => {
                        return Self::map_ping_result(data);
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        return Err(NetworkError::SidecarError("event channel closed".into()));
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        return Err(NetworkError::SidecarError(
                            "event channel lagged: ping result may have been lost".into(),
                        ));
                    }
                    _ => continue,
                }
            }
        })
        .await
        .map_err(|_| NetworkError::PingFailed("ping timed out".into()))?
    }

    async fn whois(
        &self,
        addr: &str,
    ) -> Result<Option<super::super::TailscalePeerIdentity>, NetworkError> {
        if *self.state.read().await != ProviderState::Running {
            return Err(NetworkError::NotRunning);
        }

        // tsnet:whois shipped with protocol v3 — an older sidecar would
        // silently swallow the command and this call would only time out, so
        // fail fast with an actionable error instead.
        let version = self
            .sidecar_protocol_version
            .load(std::sync::atomic::Ordering::Relaxed);
        if version < 3 {
            return Err(NetworkError::Unsupported(format!(
                "sidecar protocol v{version} predates tsnet:whois; upgrade the \
                 sidecar binary (needs v3)"
            )));
        }

        let target = addr.to_string();
        let request_id = uuid::Uuid::new_v4().to_string();

        // Broker-routed reply: every whois-capable sidecar echoes the id,
        // so this path needs no gate beyond the v3 check above. Register
        // BEFORE sending, under the same lock scope, so the answer can
        // never race the registration.
        let mut reply = {
            let sidecar_guard = self.sidecar.lock().await;
            let sidecar = sidecar_guard.as_ref().ok_or(NetworkError::NotRunning)?;
            let reply = sidecar.register_reply(&request_id);
            sidecar.send_whois(target, Some(request_id.clone())).await?;
            reply
        };

        match tokio::time::timeout(Duration::from_secs(10), reply.recv())
            .await
            .map_err(|_| NetworkError::SidecarError("whois timed out".into()))??
        {
            SidecarInternalEvent::WhoisResult(data) => Self::map_whois_result(data),
            SidecarInternalEvent::Error { code, message } => {
                Err(NetworkError::SidecarError(format!("[{code}] {message}")))
            }
            other => Err(Self::unexpected_reply("whois", other)),
        }
    }

    async fn bind_udp(&self, port: u16) -> Result<super::super::NetworkUdpSocket, NetworkError> {
        if *self.state.read().await != ProviderState::Running {
            return Err(NetworkError::NotRunning);
        }

        if self.sidecar_version() < 6 {
            return Err(NetworkError::Unsupported(
                "authenticated UDP requires sidecar protocol v6; upgrade the sidecar".into(),
            ));
        }
        let request_id = uuid::Uuid::new_v4().to_string();
        let local = tokio::net::UdpSocket::bind("127.0.0.1:0")
            .await
            .map_err(NetworkError::Io)?;
        // Install cleanup before any command can be sent. This covers errors,
        // dropped bind futures, registration timeouts, and normal socket Drop.
        let (mut socket, mut reply) = {
            let guard = self.sidecar.lock().await;
            let sidecar = guard.as_ref().ok_or(NetworkError::NotRunning)?;
            let socket = super::super::NetworkUdpSocket::new(local, 0)
                .with_cleanup(sidecar.udp_cleanup(&request_id));
            let reply = sidecar.register_reply(&request_id);
            sidecar
                .send_listen_packet(port, Some(request_id.clone()))
                .await?;
            (socket, reply)
        };
        let (actual_port, local_port) =
            match tokio::time::timeout(Duration::from_secs(10), reply.recv())
                .await
                .map_err(|_| {
                    NetworkError::ListenFailed("UDP bind confirmation timed out".into())
                })?? {
                SidecarInternalEvent::ListeningPacket { port, local_port }
                    if port != 0 && local_port != 0 =>
                {
                    (port, local_port)
                }
                SidecarInternalEvent::Error { code, message } => {
                    return Err(NetworkError::ListenFailed(format!(
                        "UDP bind failed [{code}] {message}"
                    )))
                }
                other => return Err(Self::unexpected_reply("listenPacket", other)),
            };
        socket.set_tsnet_port(actual_port);
        socket
            .inner()
            .connect((std::net::Ipv4Addr::LOCALHOST, local_port))
            .await
            .map_err(NetworkError::Io)?;
        let token = udp_relay_token(&*self.session_token.read().await, &request_id);
        let mut registration = b"TRUFFLE_UDP_REGISTER".to_vec();
        registration.extend_from_slice(&token);
        socket
            .inner()
            .send(&registration)
            .await
            .map_err(NetworkError::Io)?;
        let mut ack = [0; 64];
        let n = tokio::time::timeout(Duration::from_secs(5), socket.inner().recv(&mut ack))
            .await
            .map_err(|_| NetworkError::ListenFailed("UDP relay authentication timed out".into()))?
            .map_err(NetworkError::Io)?;
        if &ack[..n] != b"TRUFFLE_UDP_READY" {
            return Err(NetworkError::ListenFailed(
                "invalid UDP relay authentication acknowledgement".into(),
            ));
        }
        Ok(socket)
    }

    async fn health(&self) -> HealthInfo {
        self.health.read().await.clone()
    }

    fn proxy_runtime_errors(&self) -> Option<broadcast::Receiver<ProxyRuntimeError>> {
        Some(self.proxy_error_tx.subscribe())
    }

    // ── Reverse proxy ─────────────────────────────────────────────────

    async fn proxy_add(&self, config: ProxyAddParams) -> Result<ProxyAddResult, NetworkError> {
        if *self.state.read().await != ProviderState::Running {
            return Err(NetworkError::NotRunning);
        }

        // RFC 023 §8.1: v2-only features must fail loudly on old sidecars.
        // The wire silently drops unknown JSON fields — for `allow` that
        // would be an access gate the user believes exists and doesn't.
        let uses_v2 = !config.tls
            || config.allow_non_loopback
            || !config.allow.is_empty()
            || !config.routes.is_empty();
        let version = self
            .sidecar_protocol_version
            .load(std::sync::atomic::Ordering::Relaxed);
        if uses_v2 && version < 2 {
            return Err(NetworkError::ProxyError(format!(
                "sidecar protocol v{version} predates RFC 023 — routes, allow lists, \
                 tls: false, and non-loopback targets need a v2 sidecar; upgrade the \
                 sidecar binary"
            )));
        }
        // Same rule for the capability allow-list: an old sidecar would drop
        // the field and the backend would silently never see capabilities.
        if !config.accept_app_capabilities.is_empty() && version < Self::SIDECAR_V9_APP_CAPS {
            return Err(NetworkError::ProxyError(format!(
                "sidecar protocol v{version} cannot forward app capabilities \
                 (accept_app_capabilities needs v{}); upgrade the sidecar binary",
                Self::SIDECAR_V9_APP_CAPS
            )));
        }

        let command = ProxyAddCommandData {
            id: config.id.clone(),
            name: config.name.clone(),
            listen_port: config.listen_port,
            target_host: config.target_host.clone(),
            target_port: config.target_port,
            target_scheme: config.target_scheme.clone(),
            tls: config.tls,
            allow_non_loopback: config.allow_non_loopback,
            allow: config.allow.clone(),
            routes: config.routes.clone(),
            accept_app_caps: config.accept_app_capabilities.clone(),
            request_id: None,
        };

        // v4 sidecars echo a correlation id on proxy:added / proxy:error,
        // routing the reply through the broker; older ones fall back to
        // id-matched value correlation.
        if self.sidecar_version() >= Self::SIDECAR_V4_REPLY_ROUTING {
            let request_id = uuid::Uuid::new_v4().to_string();
            let mut reply = {
                let sidecar_guard = self.sidecar.lock().await;
                let sidecar = sidecar_guard.as_ref().ok_or(NetworkError::NotRunning)?;
                let reply = sidecar.register_reply(&request_id);
                sidecar
                    .send_proxy_add(ProxyAddCommandData {
                        request_id: Some(request_id.clone()),
                        ..command
                    })
                    .await?;
                reply
            };
            return match tokio::time::timeout(Duration::from_secs(10), reply.recv())
                .await
                .map_err(|_| NetworkError::ProxyError("proxy add timed out".into()))??
            {
                SidecarInternalEvent::ProxyAdded {
                    id,
                    listen_port,
                    url,
                } => Ok(ProxyAddResult {
                    id,
                    listen_port,
                    url,
                }),
                SidecarInternalEvent::ProxyError { code, message, .. }
                | SidecarInternalEvent::Error { code, message } => {
                    Err(NetworkError::ProxyError(format!("[{code}] {message}")))
                }
                other => Err(Self::unexpected_reply("proxy add", other)),
            };
        }

        // Legacy pre-v4 path: value correlation over broadcast.
        let mut event_rx = {
            let sidecar_guard = self.sidecar.lock().await;
            let sidecar = sidecar_guard.as_ref().ok_or(NetworkError::NotRunning)?;
            let event_rx = sidecar.subscribe();
            sidecar.send_proxy_add(command).await?;
            event_rx
        };

        // Wait for confirmation or error
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                match event_rx.recv().await {
                    Ok(SidecarInternalEvent::ProxyAdded {
                        id,
                        listen_port,
                        url,
                    }) if id == config.id => {
                        return Ok(ProxyAddResult {
                            id,
                            listen_port,
                            url,
                        });
                    }
                    Ok(SidecarInternalEvent::ProxyError { id, code, message })
                        if id == config.id =>
                    {
                        return Err(NetworkError::ProxyError(format!("[{code}] {message}")));
                    }
                    Ok(SidecarInternalEvent::Error { code, message }) => {
                        return Err(NetworkError::ProxyError(format!("[{code}] {message}")));
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        return Err(NetworkError::SidecarError("event channel closed".into()));
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        return Err(NetworkError::SidecarError(
                            "event channel lagged: proxy confirmation may have been lost".into(),
                        ));
                    }
                    Ok(_) => continue,
                }
            }
        })
        .await
        .map_err(|_| NetworkError::ProxyError("proxy add timed out".into()))?
    }

    async fn proxy_remove(&self, id: &str) -> Result<(), NetworkError> {
        if *self.state.read().await != ProviderState::Running {
            return Err(NetworkError::NotRunning);
        }

        let target_id = id.to_string();

        // v4 sidecars echo a correlation id, routing the reply through the
        // broker; older ones fall back to id-matched value correlation.
        if self.sidecar_version() >= Self::SIDECAR_V4_REPLY_ROUTING {
            let request_id = uuid::Uuid::new_v4().to_string();
            let mut reply = {
                let sidecar_guard = self.sidecar.lock().await;
                let sidecar = sidecar_guard.as_ref().ok_or(NetworkError::NotRunning)?;
                let reply = sidecar.register_reply(&request_id);
                sidecar
                    .send_proxy_remove(id, Some(request_id.clone()))
                    .await?;
                reply
            };
            return match tokio::time::timeout(Duration::from_secs(10), reply.recv())
                .await
                .map_err(|_| NetworkError::ProxyError("proxy remove timed out".into()))??
            {
                SidecarInternalEvent::ProxyRemoved { .. } => Ok(()),
                SidecarInternalEvent::ProxyError { code, message, .. }
                | SidecarInternalEvent::Error { code, message } => {
                    Err(NetworkError::ProxyError(format!("[{code}] {message}")))
                }
                other => Err(Self::unexpected_reply("proxy remove", other)),
            };
        }

        // Legacy pre-v4 path: value correlation over broadcast.
        let mut event_rx = {
            let sidecar_guard = self.sidecar.lock().await;
            let sidecar = sidecar_guard.as_ref().ok_or(NetworkError::NotRunning)?;
            let event_rx = sidecar.subscribe();
            sidecar.send_proxy_remove(id, None).await?;
            event_rx
        };

        // Wait for confirmation or error
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                match event_rx.recv().await {
                    Ok(SidecarInternalEvent::ProxyRemoved { id }) if id == target_id => {
                        return Ok(());
                    }
                    Ok(SidecarInternalEvent::ProxyError { id, code, message })
                        if id == target_id =>
                    {
                        return Err(NetworkError::ProxyError(format!("[{code}] {message}")));
                    }
                    Ok(SidecarInternalEvent::Error { code, message }) => {
                        return Err(NetworkError::ProxyError(format!("[{code}] {message}")));
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        return Err(NetworkError::SidecarError("event channel closed".into()));
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        return Err(NetworkError::SidecarError(
                            "event channel lagged: proxy confirmation may have been lost".into(),
                        ));
                    }
                    Ok(_) => continue,
                }
            }
        })
        .await
        .map_err(|_| NetworkError::ProxyError("proxy remove timed out".into()))?
    }

    async fn proxy_list(&self) -> Result<Vec<ProxyListEntry>, NetworkError> {
        if *self.state.read().await != ProviderState::Running {
            return Err(NetworkError::NotRunning);
        }

        // v4 sidecars echo a correlation id on the list result, routing the
        // reply through the broker; older ones fall back to the historical
        // assumption that one list call is in flight at a time.
        if self.sidecar_version() >= Self::SIDECAR_V4_REPLY_ROUTING {
            let request_id = uuid::Uuid::new_v4().to_string();
            let mut reply = {
                let sidecar_guard = self.sidecar.lock().await;
                let sidecar = sidecar_guard.as_ref().ok_or(NetworkError::NotRunning)?;
                let reply = sidecar.register_reply(&request_id);
                sidecar.send_proxy_list(Some(request_id.clone())).await?;
                reply
            };
            return match tokio::time::timeout(Duration::from_secs(10), reply.recv())
                .await
                .map_err(|_| NetworkError::ProxyError("proxy list timed out".into()))??
            {
                SidecarInternalEvent::ProxyList { proxies } => Ok(Self::proxy_entries(proxies)),
                SidecarInternalEvent::ProxyError { code, message, .. }
                | SidecarInternalEvent::Error { code, message } => {
                    Err(NetworkError::ProxyError(format!("[{code}] {message}")))
                }
                other => Err(Self::unexpected_reply("proxy list", other)),
            };
        }

        // Legacy pre-v4 path: value correlation over broadcast.
        let mut event_rx = {
            let sidecar_guard = self.sidecar.lock().await;
            let sidecar = sidecar_guard.as_ref().ok_or(NetworkError::NotRunning)?;
            let event_rx = sidecar.subscribe();
            sidecar.send_proxy_list(None).await?;
            event_rx
        };

        // Wait for the list response
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                match event_rx.recv().await {
                    Ok(SidecarInternalEvent::ProxyList { proxies }) => {
                        return Ok(Self::proxy_entries(proxies));
                    }
                    Ok(SidecarInternalEvent::Error { code, message }) => {
                        return Err(NetworkError::ProxyError(format!("[{code}] {message}")));
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        return Err(NetworkError::SidecarError("event channel closed".into()));
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        return Err(NetworkError::SidecarError(
                            "event channel lagged: proxy confirmation may have been lost".into(),
                        ));
                    }
                    Ok(_) => continue,
                }
            }
        })
        .await
        .map_err(|_| NetworkError::ProxyError("proxy list timed out".into()))?
    }

    // ── Egress (RFC 026) ──────────────────────────────────────────────

    async fn loopback(&self, local_api: bool) -> Result<LoopbackConfig, NetworkError> {
        let reply = self
            .egress_rpc("tsnet:loopback", command_type::LOOPBACK, |request_id| {
                serde_json::to_value(LoopbackCommandData {
                    local_api,
                    request_id: Some(request_id),
                })
            })
            .await?;
        Self::map_loopback_reply(reply, local_api)
    }

    async fn egress_add(&self, route: EgressRoute) -> Result<BoundEgress, NetworkError> {
        let command = |request_id| serde_json::to_value(self.egress_add_command(route, request_id));
        let reply = self
            .egress_rpc("egress:add", command_type::EGRESS_ADD, command)
            .await?;
        Self::map_egress_added(reply)
    }

    async fn egress_remove(&self, id: &str) -> Result<(), NetworkError> {
        let reply = self
            .egress_rpc("egress:remove", command_type::EGRESS_REMOVE, |request_id| {
                serde_json::to_value(EgressRemoveCommandData {
                    id: id.to_string(),
                    request_id: Some(request_id),
                })
            })
            .await?;
        Self::map_egress_removed(reply, id)
    }

    async fn egress_list(&self) -> Result<Vec<EgressRouteInfo>, NetworkError> {
        let reply = self
            .egress_rpc("egress:list", command_type::EGRESS_LIST, |request_id| {
                serde_json::to_value(EgressListCommandData {
                    request_id: Some(request_id),
                })
            })
            .await?;
        Self::map_egress_list(reply)
    }

    async fn egress_http_proxy(&self) -> Result<HttpProxyConfig, NetworkError> {
        let command = |request_id| serde_json::to_value(self.egress_proxy_command(request_id));
        let reply = self
            .egress_rpc("egress:proxy", command_type::EGRESS_PROXY, command)
            .await?;
        Self::map_egress_proxy(reply)
    }
}

impl TailscaleProvider {
    /// Earliest sidecar protocol that speaks the RFC 026 egress commands.
    pub(crate) const SIDECAR_V7_EGRESS: u32 = 7;

    /// How long an egress command may take. `egress:add` reads the netmap to
    /// admit its target (the sidecar bounds that read at 10 s), so the core
    /// waits past the sidecar's own budget rather than racing it.
    const EGRESS_REPLY_TIMEOUT: Duration = Duration::from_secs(20);

    /// Refuse an RFC 026 command this provider cannot carry: not running, or
    /// a sidecar older than protocol 7. An older sidecar answers the unknown
    /// verb only with an UNKNOWN_CMD error that carries no `requestId`, so a
    /// blind send could only time out — refuse locally instead, loudly.
    async fn egress_ready(&self, verb: &str) -> Result<(), NetworkError> {
        if *self.state.read().await != ProviderState::Running {
            return Err(NetworkError::NotRunning);
        }
        let version = self.sidecar_version();
        if version < Self::SIDECAR_V7_EGRESS {
            return Err(NetworkError::Unsupported(format!(
                "sidecar protocol v{version} predates RFC 026 egress ({verb}); upgrade the \
                 sidecar binary (needs v{})",
                Self::SIDECAR_V7_EGRESS
            )));
        }
        Ok(())
    }

    /// One correlated RFC 026 request: build the payload around a fresh
    /// `requestId`, gate, register the reply slot BEFORE sending (the answer
    /// can never race the registration), send, and await the routed answer.
    async fn egress_rpc<F>(
        &self,
        verb: &str,
        command: &'static str,
        build: F,
    ) -> Result<SidecarInternalEvent, NetworkError>
    where
        F: FnOnce(String) -> Result<serde_json::Value, serde_json::Error>,
    {
        let request_id = uuid::Uuid::new_v4().to_string();
        let data = build(request_id.clone())?;
        self.egress_ready(verb).await?;
        let mut reply = {
            let sidecar_guard = self.sidecar.lock().await;
            let sidecar = sidecar_guard.as_ref().ok_or(NetworkError::NotRunning)?;
            let reply = sidecar.register_reply(&request_id);
            sidecar.send_egress(command, &data).await?;
            reply
        };
        tokio::time::timeout(Self::EGRESS_REPLY_TIMEOUT, reply.recv())
            .await
            .map_err(|_| NetworkError::SidecarError(format!("{verb} timed out")))?
    }

    /// The `egress:add` payload. The node's RFC 025 gate rides along as
    /// `nodeLoginAllow`: the core owns `login_allow`, and the sidecar needs
    /// it to re-check every accepted connection.
    pub(super) fn egress_add_command(
        &self,
        route: EgressRoute,
        request_id: String,
    ) -> EgressAddCommandData {
        EgressAddCommandData {
            id: route.id,
            target: route.target,
            login: route.login.filter(|l| !l.is_empty()),
            node_login_allow: self.config.login_allow.clone(),
            request_id: Some(request_id),
        }
    }

    /// The `egress:proxy` payload, carrying the node's gate as `egress:add`
    /// does.
    pub(super) fn egress_proxy_command(&self, request_id: String) -> EgressProxyCommandData {
        EgressProxyCommandData {
            node_login_allow: self.config.login_allow.clone(),
            request_id: Some(request_id),
        }
    }

    /// A failed egress answer as a typed error. `NOT_RUNNING` keeps its own
    /// variant; every other code rides [`NetworkError::EgressError`].
    fn egress_failure(code: String, message: String) -> NetworkError {
        if code == "NOT_RUNNING" {
            NetworkError::NotRunning
        } else {
            NetworkError::EgressError { code, message }
        }
    }

    /// Map a `tsnet:loopback` answer. A LocalAPI credential the caller did
    /// not ask for is dropped here too: the contract holds on both sides of
    /// the bridge.
    pub(super) fn map_loopback_reply(
        event: SidecarInternalEvent,
        local_api: bool,
    ) -> Result<LoopbackConfig, NetworkError> {
        match event {
            SidecarInternalEvent::Loopback(data) => Ok(LoopbackConfig {
                address: data.address,
                proxy_credential: data.proxy_credential,
                local_api_credential: data
                    .local_api_credential
                    .filter(|credential| local_api && !credential.is_empty()),
            }),
            SidecarInternalEvent::Error { code, message }
            | SidecarInternalEvent::EgressError { code, message, .. } => {
                Err(Self::egress_failure(code, message))
            }
            other => Err(Self::unexpected_reply("tsnet:loopback", other)),
        }
    }

    pub(super) fn map_egress_added(
        event: SidecarInternalEvent,
    ) -> Result<BoundEgress, NetworkError> {
        match event {
            SidecarInternalEvent::EgressAdded { id, listen } => Ok(BoundEgress { id, listen }),
            SidecarInternalEvent::Error { code, message }
            | SidecarInternalEvent::EgressError { code, message, .. } => {
                Err(Self::egress_failure(code, message))
            }
            other => Err(Self::unexpected_reply("egress:add", other)),
        }
    }

    /// Map an `egress:remove` answer; it must name the route that was asked
    /// for, or the reply was misrouted.
    pub(super) fn map_egress_removed(
        event: SidecarInternalEvent,
        requested: &str,
    ) -> Result<(), NetworkError> {
        match event {
            SidecarInternalEvent::EgressRemoved { id } if id == requested => Ok(()),
            SidecarInternalEvent::Error { code, message }
            | SidecarInternalEvent::EgressError { code, message, .. } => {
                Err(Self::egress_failure(code, message))
            }
            other => Err(Self::unexpected_reply("egress:remove", other)),
        }
    }

    pub(super) fn map_egress_list(
        event: SidecarInternalEvent,
    ) -> Result<Vec<EgressRouteInfo>, NetworkError> {
        match event {
            SidecarInternalEvent::EgressList { routes } => Ok(routes
                .into_iter()
                .map(|route| EgressRouteInfo {
                    id: route.id,
                    target: route.target,
                    listen: route.listen,
                    login: route.login.filter(|l| !l.is_empty()),
                })
                .collect()),
            SidecarInternalEvent::Error { code, message }
            | SidecarInternalEvent::EgressError { code, message, .. } => {
                Err(Self::egress_failure(code, message))
            }
            other => Err(Self::unexpected_reply("egress:list", other)),
        }
    }

    pub(super) fn map_egress_proxy(
        event: SidecarInternalEvent,
    ) -> Result<HttpProxyConfig, NetworkError> {
        match event {
            SidecarInternalEvent::EgressProxy(data) => Ok(HttpProxyConfig {
                address: data.address,
                credential: data.credential,
            }),
            SidecarInternalEvent::Error { code, message }
            | SidecarInternalEvent::EgressError { code, message, .. } => {
                Err(Self::egress_failure(code, message))
            }
            other => Err(Self::unexpected_reply("egress:proxy", other)),
        }
    }
}

impl TailscaleProvider {
    /// Get the local identity (convenience alias — same as the trait method).
    ///
    /// Retained for backwards compatibility with existing callers that used
    /// the old async version.
    pub async fn local_identity_async(&self) -> NodeIdentity {
        self.identity.read().unwrap().clone()
    }

    /// Get the local address (convenience alias — same as the trait method).
    ///
    /// Retained for backwards compatibility with existing callers that used
    /// the old async version.
    pub async fn local_addr_async(&self) -> PeerAddr {
        self.local_addr.read().unwrap().clone()
    }

    /// Wait for a registered dial to resolve: the bridge delivers the
    /// `TcpStream`, the sidecar reports a dial failure, or the timeout
    /// elapses.
    ///
    /// Cleanup is guaranteed on every exit path — including timeout — so a
    /// failed dial never leaks its `pending_dials` entry or the fail-watcher
    /// task (which would otherwise hold a broadcast receiver forever).
    ///
    /// `pub(super)` so the module tests can exercise the timeout path.
    /// Earliest sidecar protocol that echoes `requestId` on
    /// `tsnet:pingResult` (P12, shipped before v2 was minted — v2 is the
    /// earliest version we can address).
    const SIDECAR_V2_PING_ECHO: u32 = 2;

    /// Earliest sidecar protocol that echoes `requestId` on every RPC
    /// event: listening, listeningPacket, proxy:added/removed/list,
    /// proxy:error, and correlated tsnet:error.
    const SIDECAR_V4_REPLY_ROUTING: u32 = 4;

    /// Protocol 9 sidecars put `userId`, `tags` and `capMap` in WhoIs
    /// identities and honor `acceptAppCaps` on `proxy:add`.
    const SIDECAR_V9_APP_CAPS: u32 = 9;

    fn sidecar_version(&self) -> u32 {
        self.sidecar_protocol_version
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Broker replies are typed by the sidecar, not by us — a reply of an
    /// unexpected variant is a protocol bug worth naming, not a hang.
    fn unexpected_reply(what: &str, event: SidecarInternalEvent) -> NetworkError {
        NetworkError::SidecarError(format!("unexpected {what} reply: {event:?}"))
    }

    /// Map a `tsnet:pingResult` payload to the public result. Shared by the
    /// broker path and the pre-v2 legacy wait loop.
    fn map_ping_result(data: PingResultEventData) -> Result<PingResult, NetworkError> {
        if !data.error.is_empty() {
            return Err(NetworkError::PingFailed(data.error));
        }
        let connection = if data.direct {
            "direct".to_string()
        } else if !data.peer_relay.is_empty() {
            format!("peer-relay:{}", data.peer_relay)
        } else if !data.relay.is_empty() {
            format!("relay:{}", data.relay)
        } else {
            "unknown".to_string()
        };
        Ok(PingResult {
            latency: Duration::from_secs_f64(data.latency_ms / 1000.0),
            connection,
            peer_addr: if data.peer_addr.is_empty() {
                None
            } else {
                Some(data.peer_addr)
            },
        })
    }

    /// Map a `tsnet:whoisResult` payload to the public identity.
    ///
    /// Belt-and-braces for the "absent, not fabricated" contract: the wire
    /// omits empty fields, but don't let that depend on the serializer —
    /// drop present-but-empty fields, and fold an identity with no
    /// information at all into `None`.
    fn map_whois_result(
        data: WhoisResultEventData,
    ) -> Result<Option<super::super::TailscalePeerIdentity>, NetworkError> {
        if !data.error.is_empty() {
            return Err(NetworkError::SidecarError(data.error));
        }
        Ok(data
            .identity
            .map(super::super::TailscalePeerIdentity::normalized)
            .filter(|identity| !identity.is_empty()))
    }

    /// Map `proxy:list` payload entries to the public list entries.
    fn proxy_entries(proxies: Vec<ProxyInfoEventData>) -> Vec<ProxyListEntry> {
        proxies
            .into_iter()
            .map(|p| ProxyListEntry {
                id: p.id,
                name: p.name,
                listen_port: p.listen_port,
                target_host: p.target_host,
                target_port: p.target_port,
                target_scheme: p.target_scheme,
                url: p.url,
            })
            .collect()
    }

    /// Wait for a dial to complete: the bridge delivers the `TcpStream` on
    /// success, while the broker-routed `bridge:dialResult` reply reports
    /// failures (`dialResult` has carried the request id since the bridge
    /// protocol's first version, so this path needs no version gate). A
    /// success reply only confirms the socket is coming — keep waiting for
    /// the bridge to deliver it.
    pub(super) async fn await_dial_result(
        bridge: &Bridge,
        request_id: &str,
        mut dial_rx: oneshot::Receiver<TcpStream>,
        mut reply: ReplyGuard,
        timeout: Duration,
    ) -> Result<TcpStream, NetworkError> {
        let result = tokio::time::timeout(timeout, async {
            let mut reply_pending = true;
            loop {
                tokio::select! {
                    stream_result = &mut dial_rx => {
                        return stream_result
                            .map_err(|_| NetworkError::DialFailed("dial cancelled".into()));
                    }
                    reply_result = reply.recv(), if reply_pending => {
                        match reply_result {
                            Ok(SidecarInternalEvent::DialFailed { error, .. }) => {
                                return Err(NetworkError::DialFailed(error));
                            }
                            Ok(SidecarInternalEvent::Error { code, message }) => {
                                return Err(NetworkError::DialFailed(format!(
                                    "[{code}] {message}"
                                )));
                            }
                            // DialSucceeded — or a dropped slot during
                            // shutdown: no failure to report; the socket,
                            // or the timeout, decides from here.
                            _ => reply_pending = false,
                        }
                    }
                }
            }
        })
        .await
        .unwrap_or(Err(NetworkError::DialTimeout(timeout)));

        // Clean up the pending dial on any error — including timeout.
        if result.is_err() {
            bridge.remove_dial(request_id).await;
        }

        result
    }
}

#[cfg(test)]
mod config_debug_tests {
    use super::*;

    #[test]
    fn tailscale_config_debug_redacts_auth_key() {
        let config = TailscaleConfig {
            binary_path: PathBuf::from("/opt/sidecar"),
            app_id: "demo".to_string(),
            device_id: "01JZZZZZZZZZZZZZZZZZZZZZZZ".to_string(),
            device_name: "dev".to_string(),
            hostname: "truffle-demo-dev".to_string(),
            state_dir: "/tmp/state".to_string(),
            auth_key: Some("dummy-auth-SECRET123".to_string()),
            ephemeral: None,
            tags: None,
            idle_timeout_secs: None,
            start_timeout: Some(crate::network::tailscale::DEFAULT_START_TIMEOUT),
            login_allow: Vec::new(),
            client_log_upload: true,
        };
        let dbg = format!("{config:?}");
        assert!(!dbg.contains("SECRET123"));
        assert!(dbg.contains("[REDACTED]"));
    }
}

pub(super) fn udp_relay_token(session: &[u8; 32], request_id: &str) -> [u8; 32] {
    use hmac::{Hmac, KeyInit, Mac};
    let mut mac =
        Hmac::<sha2::Sha256>::new_from_slice(session).expect("HMAC accepts a 32-byte key");
    mac.update(b"truffle-udp-v6\0");
    mac.update(request_id.as_bytes());
    mac.finalize().into_bytes().into()
}

#[cfg(test)]
mod diagnostic_tests {
    use super::*;
    use crate::network::NetworkProvider;

    #[tokio::test]
    async fn authenticated_udp_rejects_login_only_protocol_v5() {
        let provider = TailscaleProvider::new(TailscaleConfig {
            binary_path: "/nonexistent/sidecar".into(),
            app_id: "test".into(),
            device_id: "device".into(),
            device_name: "test".into(),
            hostname: "truffle-test-device".into(),
            state_dir: "unused".into(),
            auth_key: None,
            ephemeral: Some(true),
            tags: None,
            idle_timeout_secs: None,
            start_timeout: Some(crate::network::tailscale::DEFAULT_START_TIMEOUT),
            login_allow: Vec::new(),
            client_log_upload: true,
        });
        *provider.state.write().await = ProviderState::Running;
        provider
            .sidecar_protocol_version
            .store(5, std::sync::atomic::Ordering::Relaxed);
        // Protocol 5 advertises loginName, but has no authenticated UDP relay.
        // Reject it before allocating a socket or sending a legacy command.
        assert!(matches!(provider.bind_udp(0).await,
            Err(NetworkError::Unsupported(message)) if message.contains("v6")));
    }

    #[tokio::test]
    async fn proxy_app_capabilities_require_protocol_v9() {
        let provider = TailscaleProvider::new(TailscaleConfig {
            binary_path: "/nonexistent/sidecar".into(),
            app_id: "test".into(),
            device_id: "device".into(),
            device_name: "test".into(),
            hostname: "truffle-test-device".into(),
            state_dir: "unused".into(),
            auth_key: None,
            ephemeral: Some(true),
            tags: None,
            idle_timeout_secs: None,
            start_timeout: Some(crate::network::tailscale::DEFAULT_START_TIMEOUT),
            login_allow: Vec::new(),
            client_log_upload: true,
        });
        *provider.state.write().await = ProviderState::Running;
        provider
            .sidecar_protocol_version
            .store(8, std::sync::atomic::Ordering::Relaxed);
        let params = crate::network::ProxyAddParams {
            id: "flux".into(),
            name: "flux".into(),
            listen_port: 8443,
            target_host: "127.0.0.1".into(),
            target_port: 8096,
            target_scheme: "http".into(),
            tls: true,
            allow_non_loopback: false,
            allow: Vec::new(),
            routes: Vec::new(),
            accept_app_capabilities: vec!["vibecook.dev/cap/flux".into()],
        };
        // A v8 sidecar would drop acceptAppCaps on the floor; refuse before
        // sending anything (no sidecar is running in this test).
        assert!(matches!(provider.proxy_add(params).await,
            Err(NetworkError::ProxyError(message)) if message.contains("v9")));
    }

    #[test]
    fn sidecar_pid_is_none_before_start() {
        let provider = TailscaleProvider::new(TailscaleConfig {
            binary_path: "/nonexistent/sidecar".into(),
            app_id: "test".into(),
            device_id: "device".into(),
            device_name: "test".into(),
            hostname: "truffle-test-device".into(),
            state_dir: "unused".into(),
            auth_key: None,
            ephemeral: Some(true),
            tags: None,
            idle_timeout_secs: None,
            start_timeout: Some(crate::network::tailscale::DEFAULT_START_TIMEOUT),
            login_allow: Vec::new(),
            client_log_upload: true,
        });
        assert_eq!(provider.sidecar_pid(), None);
        // The slot is what a spawned sidecar writes; 0 stays "no process".
        provider
            .sidecar_pid
            .store(4242, std::sync::atomic::Ordering::Relaxed);
        assert_eq!(provider.sidecar_pid(), Some(4242));
    }

    #[test]
    fn ping_paths_survive_the_json_boundary() {
        for (value, expected) in [
            (
                serde_json::json!({"direct":true,"peerAddr":"192.0.2.1:1234"}),
                "direct",
            ),
            (serde_json::json!({"relay":"ord"}), "relay:ord"),
            (
                serde_json::json!({"peerRelay":"100.64.2.1:1234:vni:4","relay":"ord"}),
                "peer-relay:100.64.2.1:1234:vni:4",
            ),
            (serde_json::json!({}), "unknown"),
        ] {
            let data: PingResultEventData = serde_json::from_value(value).unwrap();
            assert_eq!(
                TailscaleProvider::map_ping_result(data).unwrap().connection,
                expected
            );
        }
    }
}

/// RFC 025 §3.3 (D3): the login gate as the provider actually applies it —
/// these drive the real `spawn_event_processor`, not a re-derivation of its
/// predicate, so a filter that stops being called fails here.
#[cfg(test)]
mod login_gate_tests {
    use super::*;
    use crate::network::tailscale::protocol::{PeerChangedEventData, SidecarPeer};
    use std::sync::atomic::AtomicU32;
    use std::time::Duration;

    fn peer_row(id: &str, hostname: &str, login: Option<&str>) -> SidecarPeer {
        SidecarPeer {
            id: id.into(),
            hostname: hostname.into(),
            dns_name: format!("{hostname}.tailnet.ts.net"),
            tailscale_ips: vec!["100.64.0.2".into()],
            online: true,
            os: "linux".into(),
            cur_addr: String::new(),
            relay: String::new(),
            active: true,
            peer_relay: String::new(),
            last_seen: None,
            key_expiry: None,
            expired: false,
            login_name: login.map(Into::into),
        }
    }

    struct Harness {
        sidecar_tx: broadcast::Sender<SidecarInternalEvent>,
        events: broadcast::Receiver<NetworkPeerEvent>,
        status: broadcast::Receiver<NetworkStatusEvent>,
        needs_approval: Arc<std::sync::atomic::AtomicBool>,
        identity: Arc<std::sync::RwLock<NodeIdentity>>,
        peers: Arc<RwLock<HashMap<String, NetworkPeer>>>,
        reporting: StatusReporting,
        health: Arc<RwLock<HealthInfo>>,
    }

    /// Spawn the real event processor for `app_id` under `login_allow`.
    fn harness(app_id: &str, login_allow: Vec<String>) -> Harness {
        let (sidecar_tx, sidecar_rx) = broadcast::channel(64);
        let (peer_event_tx, events) = broadcast::channel(64);
        let (proxy_error_tx, _) = broadcast::channel(8);
        let (status_tx, status) = broadcast::channel(8);
        let needs_approval = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let reporting = StatusReporting {
            needs_approval: needs_approval.clone(),
            ..StatusReporting::new(status_tx)
        };
        let health = Arc::new(RwLock::new(HealthInfo::default()));
        let peers = Arc::new(RwLock::new(HashMap::new()));
        let identity = Arc::new(std::sync::RwLock::new(NodeIdentity {
            app_id: app_id.to_string(),
            device_id: "01JZZZZZZZZZZZZZZZZZZZZZZZ".to_string(),
            device_name: "dev".to_string(),
            tailscale_hostname: format!("truffle-{app_id}-dev"),
            tailscale_id: String::new(),
            dns_name: None,
            ip: None,
            login_name: None,
        }));
        TailscaleProvider::spawn_event_processor(
            sidecar_rx,
            peers.clone(),
            peer_event_tx,
            health.clone(),
            identity.clone(),
            Arc::new(std::sync::RwLock::new(PeerAddr::default())),
            Arc::new(std::sync::RwLock::new(None)),
            Arc::new(RwLock::new(ProviderState::Starting)),
            None,
            app_id.to_string(),
            login_allow,
            proxy_error_tx,
            Arc::new(AtomicU32::new(LOGIN_NAME_PROTOCOL_VERSION)),
            reporting.clone(),
        );
        Harness {
            sidecar_tx,
            events,
            status,
            needs_approval,
            identity,
            peers,
            reporting,
            health,
        }
    }

    /// Collect the peer ids the processor reported as joined, settling first
    /// so a row the gate dropped shows up as an absence rather than a race.
    async fn joined_ids(events: &mut broadcast::Receiver<NetworkPeerEvent>) -> Vec<String> {
        tokio::time::sleep(Duration::from_millis(120)).await;
        let mut ids = Vec::new();
        while let Ok(ev) = events.try_recv() {
            if let NetworkPeerEvent::Joined(p) = ev {
                ids.push(p.id);
            }
        }
        ids.sort();
        ids
    }

    /// Every peer event the processor emitted, as `("joined"|"updated"|"left",
    /// id)` in order. Settles first so an absent event is an absence, not a
    /// race.
    async fn drain(
        events: &mut broadcast::Receiver<NetworkPeerEvent>,
    ) -> Vec<(&'static str, String)> {
        tokio::time::sleep(Duration::from_millis(120)).await;
        let mut out = Vec::new();
        while let Ok(ev) = events.try_recv() {
            match ev {
                NetworkPeerEvent::Joined(p) => out.push(("joined", p.id)),
                NetworkPeerEvent::Updated(p) => out.push(("updated", p.id)),
                NetworkPeerEvent::Left(id) => out.push(("left", id)),
                _ => {}
            }
        }
        out
    }

    /// Send one `peerChanged` row of the given type.
    fn send_change(h: &Harness, change_type: &str, row: SidecarPeer) {
        h.sidecar_tx
            .send(SidecarInternalEvent::PeerChanged(PeerChangedEventData {
                change_type: change_type.to_string(),
                peer_id: row.id.clone(),
                peer: Some(row),
            }))
            .unwrap();
    }

    #[tokio::test]
    async fn peers_received_reports_only_matching_logins() {
        let mut h = harness("playground", vec!["alice@example.com".to_string()]);
        h.sidecar_tx
            .send(SidecarInternalEvent::PeersReceived(vec![
                peer_row("n1", "truffle-playground-alice", Some("alice@example.com")),
                peer_row("n2", "truffle-playground-bob", Some("bob@example.com")),
                // No login at all: fails the gate closed (§3.2).
                peer_row("n3", "truffle-playground-ghost", None),
            ]))
            .unwrap();

        assert_eq!(joined_ids(&mut h.events).await, ["n1"]);
        let cached = h.peers.read().await;
        assert_eq!(cached.len(), 1);
        assert_eq!(
            cached["n1"].login_name.as_deref(),
            Some("alice@example.com"),
            "the admitted peer must carry the login it was admitted on"
        );
    }

    #[tokio::test]
    async fn ungated_peers_received_keeps_login_less_rows() {
        let mut h = harness("playground", Vec::new());
        h.sidecar_tx
            .send(SidecarInternalEvent::PeersReceived(vec![
                peer_row("n1", "truffle-playground-alice", Some("alice@example.com")),
                peer_row("n3", "truffle-playground-ghost", None),
            ]))
            .unwrap();

        assert_eq!(joined_ids(&mut h.events).await, ["n1", "n3"]);
        assert_eq!(h.peers.read().await["n3"].login_name, None);
    }

    #[tokio::test]
    async fn peer_changed_add_honours_the_gate() {
        let mut h = harness("playground", vec!["*@example.com".to_string()]);
        for (change, row) in [
            (
                "joined",
                peer_row("n1", "truffle-playground-alice", Some("alice@example.com")),
            ),
            (
                "joined",
                peer_row("n2", "truffle-playground-eve", Some("eve@stranger.test")),
            ),
            ("joined", peer_row("n3", "truffle-playground-ghost", None)),
        ] {
            h.sidecar_tx
                .send(SidecarInternalEvent::PeerChanged(PeerChangedEventData {
                    change_type: change.to_string(),
                    peer_id: row.id.clone(),
                    peer: Some(row),
                }))
                .unwrap();
        }

        assert_eq!(joined_ids(&mut h.events).await, ["n1"]);
        let cached = h.peers.read().await;
        assert!(cached.contains_key("n1"));
        assert!(
            !cached.contains_key("n2"),
            "a foreign login must not be cached as a peer"
        );
        assert!(
            !cached.contains_key("n3"),
            "a login-less row must not be cached under a gate"
        );
    }

    #[tokio::test]
    async fn started_fills_the_nodes_own_login() {
        let h = harness("playground", Vec::new());
        h.sidecar_tx
            .send(SidecarInternalEvent::Started {
                hostname: "truffle-playground-dev".into(),
                dns_name: "truffle-playground-dev.tailnet.ts.net".into(),
                tailscale_ip: "100.64.0.1".into(),
                node_id: "nodeSELF".into(),
                login_name: Some("alice@example.com".into()),
                protocol_version: Some(LOGIN_NAME_PROTOCOL_VERSION),
                key_expiry: None,
                control_url: None,
                tailscale_version: None,
            })
            .unwrap();
        tokio::time::sleep(Duration::from_millis(120)).await;
        assert_eq!(
            h.identity.read().unwrap().login_name.as_deref(),
            Some("alice@example.com")
        );

        // A legacy sidecar reports none — the identity stays honest.
        let h = harness("playground", Vec::new());
        h.sidecar_tx
            .send(SidecarInternalEvent::Started {
                hostname: "truffle-playground-dev".into(),
                dns_name: "truffle-playground-dev.tailnet.ts.net".into(),
                tailscale_ip: "100.64.0.1".into(),
                node_id: "nodeSELF".into(),
                login_name: None,
                protocol_version: Some(4),
                key_expiry: None,
                control_url: None,
                tailscale_version: None,
            })
            .unwrap();
        tokio::time::sleep(Duration::from_millis(120)).await;
        assert_eq!(h.identity.read().unwrap().login_name, None);
    }

    fn started(login: Option<&str>) -> SidecarInternalEvent {
        SidecarInternalEvent::Started {
            hostname: "truffle-playground-dev".into(),
            dns_name: "truffle-playground-dev.tailnet.ts.net".into(),
            tailscale_ip: "100.64.0.1".into(),
            node_id: "nodeSELF".into(),
            login_name: login.map(Into::into),
            protocol_version: Some(8),
            key_expiry: None,
            control_url: None,
            tailscale_version: None,
        }
    }

    /// A fast restart reaches "running" before the netmap names our user; the
    /// sidecar (protocol 8) re-sends a running status once it does. The later
    /// status must fill the identity and announce it — and only once.
    #[tokio::test]
    async fn a_later_running_status_fills_a_login_that_was_unknown_at_start() {
        let mut h = harness("playground", Vec::new());
        h.sidecar_tx.send(started(None)).unwrap();
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert_eq!(h.identity.read().unwrap().login_name, None);
        assert!(
            h.status.try_recv().is_err(),
            "no status event while the login is unknown"
        );

        h.sidecar_tx
            .send(started(Some("alice@example.com")))
            .unwrap();
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert_eq!(
            h.identity.read().unwrap().login_name.as_deref(),
            Some("alice@example.com"),
            "local_identity().login_name must pick up the follow-up status"
        );
        assert_eq!(
            h.status.try_recv().ok(),
            Some(NetworkStatusEvent::LoginKnown {
                login_name: "alice@example.com".into()
            })
        );

        // Repeats of the same login, and a later status that omits it, change
        // nothing and announce nothing.
        h.sidecar_tx
            .send(started(Some("alice@example.com")))
            .unwrap();
        h.sidecar_tx.send(started(None)).unwrap();
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert!(h.status.try_recv().is_err());
        assert_eq!(
            h.identity.read().unwrap().login_name.as_deref(),
            Some("alice@example.com")
        );
    }

    /// `tsnet:needsApproval` used to be parsed and dropped, leaving a caller
    /// to time out after 5 minutes with an "authentication" error. It is now
    /// forwarded, and marks the provider so a timeout names the approval;
    /// reaching "running" clears the mark.
    #[tokio::test]
    async fn needs_approval_is_forwarded_and_cleared_by_running() {
        let mut h = harness("playground", Vec::new());
        h.sidecar_tx
            .send(SidecarInternalEvent::NeedsApproval)
            .unwrap();
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert_eq!(
            h.status.try_recv().ok(),
            Some(NetworkStatusEvent::NeedsApproval)
        );
        assert!(h.needs_approval.load(std::sync::atomic::Ordering::Relaxed));
        assert!(
            h.events.try_recv().is_err(),
            "approval is a node status, not a peer event"
        );

        h.sidecar_tx
            .send(started(Some("alice@example.com")))
            .unwrap();
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert!(!h.needs_approval.load(std::sync::atomic::Ordering::Relaxed));
        assert!(matches!(
            h.status.try_recv(),
            Ok(NetworkStatusEvent::LoginKnown { .. })
        ));
    }

    /// RFC 025 §3.3: an ADMITTED peer whose later row carries a login the
    /// gate refuses is EVICTED — `Left` emitted and the entry removed, so the
    /// session closes its connection. A device that changes hands keeps its
    /// stable node id, so this is the shape a re-owned node arrives in. It
    /// must never be silently kept, and never silently not-updated.
    #[tokio::test]
    async fn an_admitted_peer_whose_login_changes_to_a_foreign_one_is_evicted() {
        let mut h = harness("playground", vec!["*@example.com".to_string()]);

        send_change(
            &h,
            "joined",
            peer_row("n1", "truffle-playground-alice", Some("alice@example.com")),
        );
        assert_eq!(drain(&mut h.events).await, [("joined", "n1".to_string())]);
        assert!(h.peers.read().await.contains_key("n1"));

        // Same node id, new owner: the device was transferred.
        send_change(
            &h,
            "updated",
            peer_row("n1", "truffle-playground-alice", Some("eve@stranger.test")),
        );
        assert_eq!(
            drain(&mut h.events).await,
            [("left", "n1".to_string())],
            "a re-owned node must leave the mesh, not linger with a stale row"
        );
        assert!(
            !h.peers.read().await.contains_key("n1"),
            "the evicted peer must be gone from the map, not merely un-updated"
        );
    }

    /// RFC 025 §3.3, the reverse: a row the gate refused that later carries an
    /// allowed login is a **Join** — it was never a peer before, so it cannot
    /// arrive as an Update.
    #[tokio::test]
    async fn a_foreign_peer_whose_login_becomes_allowed_joins() {
        let mut h = harness("playground", vec!["*@example.com".to_string()]);

        send_change(
            &h,
            "joined",
            peer_row("n1", "truffle-playground-eve", Some("eve@stranger.test")),
        );
        assert_eq!(
            drain(&mut h.events).await,
            [],
            "a foreign row must produce no event at all"
        );
        assert!(!h.peers.read().await.contains_key("n1"));

        send_change(
            &h,
            "updated",
            peer_row("n1", "truffle-playground-eve", Some("alice@example.com")),
        );
        assert_eq!(
            drain(&mut h.events).await,
            [("joined", "n1".to_string())],
            "a newly-allowed row Joins; it was never a peer to Update"
        );
        assert_eq!(
            h.peers.read().await["n1"].login_name.as_deref(),
            Some("alice@example.com")
        );
    }

    /// RFC 025 §3.3: a row the sidecar could not name an owner for must NOT
    /// evict a peer we already hold — a transient omission cannot be allowed
    /// to empty a gated mesh — and must still deliver the rest of the row.
    /// The last login we were told is carried forward, so a gated node can
    /// always state the login every one of its peers passed on.
    #[tokio::test]
    async fn a_login_less_row_keeps_an_admitted_peer_but_never_admits_a_stranger() {
        let mut h = harness("playground", vec!["*@example.com".to_string()]);

        send_change(
            &h,
            "joined",
            peer_row("n1", "truffle-playground-alice", Some("alice@example.com")),
        );
        assert_eq!(drain(&mut h.events).await, [("joined", "n1".to_string())]);

        // The same peer, now offline, on a row that omits the owner.
        let mut quiet = peer_row("n1", "truffle-playground-alice", None);
        quiet.online = false;
        send_change(&h, "updated", quiet);
        assert_eq!(
            drain(&mut h.events).await,
            [("updated", "n1".to_string())],
            "an omitted login must update the peer, never evict it and never \
             be silently swallowed"
        );
        // ... and the peer is still a peer, reported with no login.
        {
            let cached = h.peers.read().await;
            assert!(cached.contains_key("n1"));
            assert!(!cached["n1"].online, "the rest of the row must still land");
            assert_eq!(
                cached["n1"].login_name, None,
                "the ENTRY survives an unknown login; the FIELD does not lie \
                 about one. Layer 5 is what stops a new dial to such a peer."
            );
        }

        // A STRANGER with no login still stays out: admission fails closed.
        send_change(
            &h,
            "joined",
            peer_row("n2", "truffle-playground-ghost", None),
        );
        assert_eq!(drain(&mut h.events).await, []);
        assert!(!h.peers.read().await.contains_key("n2"));
    }

    /// The same two rules on the snapshot path (`tsnet:peers`), which the
    /// sidecar re-sends every 30 s: a refused login evicts, an omitted one
    /// keeps.
    #[tokio::test]
    async fn a_snapshot_evicts_on_a_refused_login_and_keeps_on_an_absent_one() {
        let mut h = harness("playground", vec!["*@example.com".to_string()]);

        h.sidecar_tx
            .send(SidecarInternalEvent::PeersReceived(vec![
                peer_row("n1", "truffle-playground-alice", Some("alice@example.com")),
                peer_row("n2", "truffle-playground-bob", Some("bob@example.com")),
            ]))
            .unwrap();
        assert_eq!(joined_ids(&mut h.events).await, ["n1", "n2"]);

        // n1 re-owned, n2's owner simply not named this time.
        h.sidecar_tx
            .send(SidecarInternalEvent::PeersReceived(vec![
                peer_row("n1", "truffle-playground-alice", Some("eve@stranger.test")),
                peer_row("n2", "truffle-playground-bob", None),
            ]))
            .unwrap();
        let events = drain(&mut h.events).await;
        assert!(
            events.contains(&("left", "n1".to_string())),
            "a re-owned node must be evicted from a snapshot too, saw: {events:?}"
        );
        assert!(
            !events.contains(&("left", "n2".to_string())),
            "an omitted login must not evict, saw: {events:?}"
        );

        let cached = h.peers.read().await;
        assert!(!cached.contains_key("n1"));
        assert_eq!(
            cached["n2"].login_name, None,
            "the kept peer reports the login the row carried — none"
        );
    }

    /// An UNGATED node is untouched by any of this: a `peerChanged updated`
    /// row updates the peer whatever its login says, or does not say.
    #[tokio::test]
    async fn ungated_peer_changed_updated_always_updates() {
        let mut h = harness("playground", Vec::new());

        send_change(
            &h,
            "joined",
            peer_row("n1", "truffle-playground-alice", Some("alice@example.com")),
        );
        assert_eq!(drain(&mut h.events).await, [("joined", "n1".to_string())]);

        // A login no gate would ever admit, an absent one, and an empty one:
        // ungated, all three are ordinary updates and none evicts.
        for login in [Some("eve@stranger.test"), None, Some("")] {
            let mut row = peer_row("n1", "truffle-playground-alice", login);
            row.online = false;
            send_change(&h, "updated", row);
            assert_eq!(
                drain(&mut h.events).await,
                [("updated", "n1".to_string())],
                "ungated, a {login:?} login must update, never evict"
            );
            assert!(h.peers.read().await.contains_key("n1"));
        }
        assert!(!h.peers.read().await["n1"].online);
    }

    /// The verdict table itself, so the three-way distinction is pinned
    /// independently of the two call sites.
    #[test]
    fn classify_row_separates_refusal_from_ignorance() {
        let gate = vec!["*@example.com".to_string()];
        let cases = [
            ("alice@example.com", RowVerdict::Allowed),
            ("eve@stranger.test", RowVerdict::LoginRefused),
        ];
        for (login, want) in cases {
            assert_eq!(
                TailscaleProvider::classify_row(
                    &peer_row("n", "truffle-playground-x", Some(login)),
                    "playground",
                    &gate
                ),
                want
            );
        }
        // No login, and an empty one, are the same fact: ignorance.
        for login in [None, Some("")] {
            assert_eq!(
                TailscaleProvider::classify_row(
                    &peer_row("n", "truffle-playground-x", login),
                    "playground",
                    &gate
                ),
                RowVerdict::LoginUnknown
            );
        }
        // Wrong app wins over any login.
        assert_eq!(
            TailscaleProvider::classify_row(
                &peer_row("n", "truffle-chat-x", Some("alice@example.com")),
                "playground",
                &gate
            ),
            RowVerdict::NotOurApp
        );
        // Ungated, ignorance does not exist — every row of ours is allowed.
        for login in [None, Some(""), Some("anyone@anywhere.test")] {
            assert_eq!(
                TailscaleProvider::classify_row(
                    &peer_row("n", "truffle-playground-x", login),
                    "playground",
                    &[]
                ),
                RowVerdict::Allowed
            );
        }
        // Admission is Allowed-only; continuity also accepts ignorance.
        let unknown = peer_row("n", "truffle-playground-x", None);
        assert!(!TailscaleProvider::admit_peer(
            &unknown,
            "playground",
            &gate
        ));
        assert!(TailscaleProvider::row_keeps_peer(
            &unknown,
            "playground",
            &gate
        ));
        let refused = peer_row("n", "truffle-playground-x", Some("eve@stranger.test"));
        assert!(!TailscaleProvider::admit_peer(
            &refused,
            "playground",
            &gate
        ));
        assert!(!TailscaleProvider::row_keeps_peer(
            &refused,
            "playground",
            &gate
        ));
    }

    /// RFC 025 D3: the start-time decision, as a table.
    #[test]
    fn login_gate_requires_protocol_five() {
        let gate = vec!["alice@example.com".to_string()];
        // Ungated: every version qualifies, including an unknown (v1) one.
        for v in [0, 1, 4, 5, 6] {
            assert!(login_gate_supported(&[], v).is_ok(), "ungated at v{v}");
        }
        // Gated: below 5 refuses, at or above 5 passes.
        for v in [0, 1, 2, 3, 4] {
            let Err(err) = login_gate_supported(&gate, v) else {
                panic!("a gated node must refuse sidecar protocol {v}");
            };
            let msg = err.to_string();
            assert!(
                matches!(err, NetworkError::StartFailed(_)),
                "the refusal must be a StartFailed, not a silent degrade: {msg}"
            );
            assert!(
                msg.contains("login_allow requires sidecar protocol 5"),
                "unexpected message: {msg}"
            );
            assert!(
                msg.contains(&format!("speaks {v}")),
                "unexpected message: {msg}"
            );
        }
        for v in [5, 6, 99] {
            assert!(login_gate_supported(&gate, v).is_ok(), "gated at v{v}");
        }
    }

    /// RFC 025 §3.3: an empty `loginName` on the wire is the same fact as an
    /// absent one — `NetworkPeer.login_name` is `None`, never `Some("")`.
    #[test]
    fn network_peer_login_is_absent_never_empty() {
        let np = TailscaleProvider::sidecar_peer_to_network_peer(&peer_row(
            "n1",
            "truffle-playground-blank",
            Some(""),
        ));
        assert_eq!(np.login_name, None);

        let np = TailscaleProvider::sidecar_peer_to_network_peer(&peer_row(
            "n2",
            "truffle-playground-alice",
            Some("alice@example.com"),
        ));
        assert_eq!(np.login_name.as_deref(), Some("alice@example.com"));

        let np = TailscaleProvider::sidecar_peer_to_network_peer(&peer_row(
            "n3",
            "truffle-playground-x",
            None,
        ));
        assert_eq!(np.login_name, None);
    }

    fn started_v10(key_expiry: Option<&str>, protocol: u32) -> SidecarInternalEvent {
        SidecarInternalEvent::Started {
            hostname: "truffle-playground-dev".into(),
            dns_name: "truffle-playground-dev.tailnet.ts.net".into(),
            tailscale_ip: "100.64.0.1".into(),
            node_id: "nodeSELF".into(),
            login_name: None,
            protocol_version: Some(protocol),
            key_expiry: key_expiry.map(Into::into),
            control_url: Some("https://controlplane.tailscale.com".into()),
            tailscale_version: None,
        }
    }

    fn status_events_so_far(
        status: &mut broadcast::Receiver<NetworkStatusEvent>,
    ) -> Vec<NetworkStatusEvent> {
        let mut seen = Vec::new();
        while let Ok(event) = status.try_recv() {
            seen.push(event);
        }
        seen
    }

    /// Protocol 10 through the real event processor: the control server
    /// from the "starting" status, the key expiry from running statuses
    /// (absent = never), each announced once and again only on a change; an
    /// older sidecar or an unreadable value never fabricates one.
    #[tokio::test]
    async fn key_expiry_and_control_server_are_learned_and_announced_on_change() {
        let mut h = harness("playground", Vec::new());
        h.sidecar_tx
            .send(SidecarInternalEvent::StateChange {
                state: "starting".into(),
                control_url: Some("https://login.tailscale.com".into()),
                tailscale_version: None,
            })
            .unwrap();
        h.sidecar_tx
            .send(started_v10(Some("2026-06-24T10:00:00Z"), 10))
            .unwrap();
        // The same running facts again (the login follow-up, a monitor tick).
        h.sidecar_tx
            .send(started_v10(Some("2026-06-24T10:00:00Z"), 10))
            .unwrap();
        tokio::time::sleep(Duration::from_millis(120)).await;
        let at = KeyExpiry::At(std::time::UNIX_EPOCH + Duration::from_secs(1_782_295_200));
        assert_eq!(
            status_events_so_far(&mut h.status),
            vec![
                NetworkStatusEvent::ControlServerKnown {
                    control_server: ControlServer::Tailscale
                },
                NetworkStatusEvent::KeyExpiryKnown { key_expiry: at },
            ],
            "login.tailscale.com and controlplane.tailscale.com are one server, announced once"
        );
        assert_eq!(*h.reporting.key_expiry.read().unwrap(), Some(at));
        assert_eq!(
            h.health.read().await.key_expiry.as_deref(),
            Some("2026-06-24T10:00:00Z")
        );

        // An admin disables the expiry: the next running status omits it.
        h.sidecar_tx.send(started_v10(None, 10)).unwrap();
        // A garbled value and a pre-10 sidecar's value are both ignored.
        h.sidecar_tx.send(started_v10(Some("soon"), 10)).unwrap();
        h.sidecar_tx
            .send(started_v10(Some("2030-01-01T00:00:00Z"), 9))
            .unwrap();
        tokio::time::sleep(Duration::from_millis(120)).await;
        assert_eq!(
            status_events_so_far(&mut h.status),
            vec![NetworkStatusEvent::KeyExpiryKnown {
                key_expiry: KeyExpiry::Never
            }]
        );
        assert_eq!(
            *h.reporting.key_expiry.read().unwrap(),
            Some(KeyExpiry::Never)
        );
        assert_eq!(h.health.read().await.key_expiry, None);
    }

    /// Protocol 11 through the real event processor: Tailnet Lock reports
    /// become NeedsSignature on entering the locked-out state and Signed on
    /// leaving it (signed, or Lock turned off), and nothing on a repeat.
    #[tokio::test]
    async fn lock_reports_announce_entering_and_leaving_the_locked_out_state() {
        let mut h = harness("playground", Vec::new());
        assert_eq!(*h.reporting.locked_out.read().unwrap(), None);
        let lock = |enabled, signed| SidecarInternalEvent::LockStatus { enabled, signed };
        for (event, want, slot) in [
            (lock(false, false), vec![], Some(false)),
            (
                lock(true, false),
                vec![NetworkStatusEvent::NeedsSignature],
                Some(true),
            ),
            (lock(true, false), vec![], Some(true)),
            (
                lock(true, true),
                vec![NetworkStatusEvent::Signed],
                Some(false),
            ),
            (
                lock(true, false),
                vec![NetworkStatusEvent::NeedsSignature],
                Some(true),
            ),
            (
                lock(false, false),
                vec![NetworkStatusEvent::Signed],
                Some(false),
            ),
        ] {
            let sent = format!("{event:?}");
            h.sidecar_tx.send(event).unwrap();
            tokio::time::sleep(Duration::from_millis(60)).await;
            assert_eq!(status_events_so_far(&mut h.status), want, "after {sent}");
            assert_eq!(
                *h.reporting.locked_out.read().unwrap(),
                slot,
                "after {sent}"
            );
        }
    }

    /// Protocol 11: the monitor's backend state reaches status_events()
    /// typed, and health keeps Tailscale's name for it; the starting status
    /// names the Tailscale client.
    #[tokio::test]
    async fn backend_states_and_the_tailscale_client_are_reported() {
        let mut h = harness("playground", Vec::new());
        h.sidecar_tx
            .send(SidecarInternalEvent::StateChange {
                state: "starting".into(),
                control_url: None,
                tailscale_version: TailscaleVersion::from_wire(
                    Some("1.104.1"),
                    Some("1.104.1-dev20261020-g0123abcd"),
                ),
            })
            .unwrap();
        for state in ["NeedsLogin", "InUseOtherUser"] {
            h.sidecar_tx
                .send(SidecarInternalEvent::BackendStateChanged {
                    state: state.into(),
                })
                .unwrap();
        }
        tokio::time::sleep(Duration::from_millis(120)).await;
        assert_eq!(
            status_events_so_far(&mut h.status),
            vec![
                NetworkStatusEvent::BackendStateChanged {
                    state: BackendState::NeedsLogin
                },
                NetworkStatusEvent::BackendStateChanged {
                    state: BackendState::Other("InUseOtherUser".into())
                },
            ]
        );
        assert_eq!(h.health.read().await.state, "InUseOtherUser");
        let version = h.reporting.tailscale_version.read().unwrap().clone();
        assert_eq!(
            version.map(|v| (v.version, v.long)),
            Some(("1.104.1".into(), "1.104.1-dev20261020-g0123abcd".into()))
        );
    }

    /// A pre-10 sidecar reports neither: nothing is announced or invented.
    #[tokio::test]
    async fn an_older_sidecar_reports_no_key_expiry_or_control_server() {
        let mut h = harness("playground", Vec::new());
        h.sidecar_tx
            .send(SidecarInternalEvent::StateChange {
                state: "starting".into(),
                control_url: None,
                tailscale_version: None,
            })
            .unwrap();
        h.sidecar_tx
            .send(started(Some("alice@example.com")))
            .unwrap();
        tokio::time::sleep(Duration::from_millis(120)).await;
        assert_eq!(
            status_events_so_far(&mut h.status),
            vec![NetworkStatusEvent::LoginKnown {
                login_name: "alice@example.com".into()
            }]
        );
        assert_eq!(*h.reporting.key_expiry.read().unwrap(), None);
        assert_eq!(*h.reporting.control_server.read().unwrap(), None);
    }
}

/// RFC 026: the egress verbs as the provider carries them — the protocol-7
/// gate, the node's login gate on the wire, and every answer mapped through
/// the production broker (`GoSidecar::dispatch_event`) the way the stdout
/// reader delivers it.
#[cfg(test)]
mod egress_tests {
    use super::*;
    use crate::network::tailscale::protocol::SidecarEvent;
    use crate::network::tailscale::sidecar::test_reply_slot;
    use crate::network::NetworkProvider;
    use std::sync::atomic::Ordering;

    fn config(login_allow: Vec<String>) -> TailscaleConfig {
        TailscaleConfig {
            binary_path: PathBuf::from("/opt/sidecar"),
            app_id: "demo".to_string(),
            device_id: "01JZZZZZZZZZZZZZZZZZZZZZZZ".to_string(),
            device_name: "dev".to_string(),
            hostname: "truffle-demo-dev".to_string(),
            state_dir: "/tmp/state".to_string(),
            auth_key: None,
            ephemeral: None,
            tags: None,
            idle_timeout_secs: None,
            start_timeout: Some(crate::network::tailscale::DEFAULT_START_TIMEOUT),
            login_allow,
            client_log_upload: true,
        }
    }

    fn route() -> EgressRoute {
        EgressRoute {
            id: "cell".into(),
            target: "beta.tail-scale.ts.net:9417".into(),
            login: None,
        }
    }

    /// A provider that believes it is running against a sidecar of
    /// `version`, with no sidecar process attached.
    async fn running_at(version: u32) -> TailscaleProvider {
        let provider = TailscaleProvider::new(config(Vec::new()));
        *provider.state.write().await = ProviderState::Running;
        provider
            .sidecar_protocol_version
            .store(version, Ordering::Relaxed);
        provider
    }

    /// Protocol 6 supplies authenticated UDP, but has no RFC 026 commands.
    /// Reject it before looking for a process or waiting for an unknown verb.
    #[tokio::test]
    async fn a_pre_v7_sidecar_is_refused_locally() {
        for version in [6, LOGIN_NAME_PROTOCOL_VERSION, 1] {
            let provider = running_at(version).await;
            let refusals = [
                ("loopback", provider.loopback(false).await.err()),
                (
                    "loopback_with_local_api",
                    provider.loopback(true).await.err(),
                ),
                ("egress_add", provider.egress_add(route()).await.err()),
                ("egress_remove", provider.egress_remove("cell").await.err()),
                ("egress_list", provider.egress_list().await.err()),
                (
                    "egress_http_proxy",
                    provider.egress_http_proxy().await.err(),
                ),
            ];
            for (verb, err) in refusals {
                match err {
                    Some(NetworkError::Unsupported(message)) => assert!(
                        message.contains(&format!("v{version}"))
                            && message.contains("RFC 026")
                            && message.contains("v7"),
                        "{verb}: the refusal must name both versions and the RFC: {message}"
                    ),
                    other => panic!(
                        "{verb} on v{version}: want Unsupported before any send, got {other:?}"
                    ),
                }
            }
        }
    }

    #[tokio::test]
    async fn a_v7_sidecar_passes_the_gate() {
        // With no sidecar attached, a request the gate lets through fails at
        // the sidecar handle — NotRunning, never Unsupported.
        let provider = running_at(TailscaleProvider::SIDECAR_V7_EGRESS).await;
        assert!(matches!(
            provider.loopback(true).await,
            Err(NetworkError::NotRunning)
        ));
        assert!(matches!(
            provider.egress_add(route()).await,
            Err(NetworkError::NotRunning)
        ));
        // A stopped provider refuses before the version is consulted.
        let stopped = TailscaleProvider::new(config(Vec::new()));
        assert!(matches!(
            stopped.egress_list().await,
            Err(NetworkError::NotRunning)
        ));
    }

    #[test]
    fn egress_commands_carry_the_nodes_login_gate() {
        let gated = TailscaleProvider::new(config(vec!["*@example.com".into()]));
        let add = gated.egress_add_command(
            EgressRoute {
                login: Some(String::new()),
                ..route()
            },
            "r1".into(),
        );
        assert_eq!(add.node_login_allow, vec!["*@example.com".to_string()]);
        assert_eq!(add.login, None, "an empty glob is no glob");
        assert_eq!(add.request_id.as_deref(), Some("r1"));
        assert_eq!(
            gated.egress_proxy_command("r2".into()).node_login_allow,
            vec!["*@example.com".to_string()]
        );

        let ungated = TailscaleProvider::new(config(Vec::new()));
        assert!(ungated
            .egress_add_command(route(), "r3".into())
            .node_login_allow
            .is_empty());
        assert!(ungated
            .egress_proxy_command("r4".into())
            .node_login_allow
            .is_empty());
    }

    /// Dispatch one wire line through the production broker into a slot
    /// registered for `request_id`, and return what the waiter receives.
    async fn routed(line: &str, request_id: &str) -> SidecarInternalEvent {
        let (pending, mut reply) = test_reply_slot(request_id);
        let (event_tx, _event_rx) = broadcast::channel(8);
        let event: SidecarEvent = serde_json::from_str(line).unwrap();
        GoSidecar::dispatch_event(event, &pending, &event_tx);
        reply
            .recv()
            .await
            .expect("the answer must route to its slot")
    }

    #[tokio::test]
    async fn answers_map_to_typed_results() {
        let loopback = r#"{"event":"tsnet:loopbackResult","data":{"address":"127.0.0.1:41641","proxyCredential":"p","localAPICredential":"l","requestId":"r1"}}"#;
        assert_eq!(
            TailscaleProvider::map_loopback_reply(routed(loopback, "r1").await, false).unwrap(),
            LoopbackConfig {
                address: "127.0.0.1:41641".into(),
                proxy_credential: "p".into(),
                local_api_credential: None,
            },
            "a LocalAPI credential nobody asked for is dropped"
        );
        assert_eq!(
            TailscaleProvider::map_loopback_reply(routed(loopback, "r1").await, true)
                .unwrap()
                .local_api_credential
                .as_deref(),
            Some("l")
        );

        assert_eq!(
            TailscaleProvider::map_egress_added(
                routed(
                    r#"{"event":"egress:added","data":{"id":"cell","listen":"127.0.0.1:50001","requestId":"r2"}}"#,
                    "r2"
                )
                .await
            )
            .unwrap(),
            BoundEgress {
                id: "cell".into(),
                listen: "127.0.0.1:50001".into(),
            }
        );

        let removed = r#"{"event":"egress:removed","data":{"id":"cell","requestId":"r3"}}"#;
        assert!(TailscaleProvider::map_egress_removed(routed(removed, "r3").await, "cell").is_ok());
        assert!(
            matches!(
                TailscaleProvider::map_egress_removed(routed(removed, "r3").await, "other"),
                Err(NetworkError::SidecarError(_))
            ),
            "an answer naming another route is a misrouted reply"
        );

        assert_eq!(
            TailscaleProvider::map_egress_list(
                routed(
                    r#"{"event":"egress:list","data":{"routes":[{"id":"a","target":"t:1","listen":"127.0.0.1:2"},{"id":"b","target":"u:2","listen":"127.0.0.1:3","login":"bob@*"}],"requestId":"r4"}}"#,
                    "r4"
                )
                .await
            )
            .unwrap(),
            vec![
                EgressRouteInfo {
                    id: "a".into(),
                    target: "t:1".into(),
                    listen: "127.0.0.1:2".into(),
                    login: None,
                },
                EgressRouteInfo {
                    id: "b".into(),
                    target: "u:2".into(),
                    listen: "127.0.0.1:3".into(),
                    login: Some("bob@*".into()),
                },
            ]
        );

        assert_eq!(
            TailscaleProvider::map_egress_proxy(
                routed(
                    r#"{"event":"egress:proxyResult","data":{"address":"127.0.0.1:41642","credential":"c","requestId":"r5"}}"#,
                    "r5"
                )
                .await
            )
            .unwrap(),
            HttpProxyConfig {
                address: "127.0.0.1:41642".into(),
                credential: "c".into(),
            }
        );

        match TailscaleProvider::map_egress_added(
            routed(
                r#"{"event":"egress:error","data":{"id":"cell","code":"TARGET_NOT_PEER","message":"192.0.2.1 is not a node of this node's tailnet","requestId":"r6"}}"#,
                "r6",
            )
            .await,
        ) {
            Err(NetworkError::EgressError { code, message }) => {
                assert_eq!(code, "TARGET_NOT_PEER");
                assert!(message.contains("192.0.2.1"));
            }
            other => panic!("a refusal must stay typed, got {other:?}"),
        }
        assert!(matches!(
            TailscaleProvider::map_loopback_reply(
                routed(
                    r#"{"event":"tsnet:error","data":{"code":"NOT_RUNNING","message":"node not running","requestId":"r7"}}"#,
                    "r7"
                )
                .await,
                false
            ),
            Err(NetworkError::NotRunning)
        ));

        // An answer of the wrong kind is named, not swallowed — and naming it
        // does not print the credential it carries.
        match TailscaleProvider::map_egress_added(
            routed(
                r#"{"event":"egress:proxyResult","data":{"address":"127.0.0.1:41642","credential":"CONNECT-SECRET","requestId":"r8"}}"#,
                "r8",
            )
            .await,
        ) {
            Err(NetworkError::SidecarError(message)) => {
                assert!(message.contains("unexpected egress:add reply"), "{message}");
                assert!(!message.contains("CONNECT-SECRET"), "{message}");
            }
            other => panic!("want an unexpected-reply error, got {other:?}"),
        }
    }
}

/// `status_events()` end to end through a real child process: a scripted
/// stand-in for the sidecar writes the wire lines a device-approval start
/// produces, and the provider must report them while `start()` waits.
#[cfg(all(test, unix))]
mod status_event_process_tests {
    use super::*;
    use crate::network::NetworkProvider;
    use std::os::unix::fs::PermissionsExt;

    fn scripted_sidecar(dir: &std::path::Path, body: &str) -> PathBuf {
        let path = dir.join("fake-sidecar.sh");
        std::fs::write(&path, format!("#!/bin/sh\n{body}\n")).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        path
    }

    fn config(binary_path: PathBuf, state_dir: &std::path::Path) -> TailscaleConfig {
        TailscaleConfig {
            binary_path,
            app_id: "playground".into(),
            device_id: "01JZZZZZZZZZZZZZZZZZZZZZZZ".into(),
            device_name: "dev".into(),
            hostname: "truffle-playground-dev".into(),
            state_dir: state_dir.to_string_lossy().into_owned(),
            auth_key: None,
            ephemeral: None,
            tags: None,
            idle_timeout_secs: None,
            start_timeout: Some(crate::network::tailscale::DEFAULT_START_TIMEOUT),
            login_allow: Vec::new(),
            client_log_upload: true,
        }
    }

    #[tokio::test]
    async fn start_reports_needs_approval_while_it_waits() {
        let dir = tempfile::tempdir().unwrap();
        // Read tsnet:start, report the approval wait, then (once "approved")
        // the running status, and idle until stdin closes.
        let script = scripted_sidecar(
            dir.path(),
            r#"read start
echo '{"event":"tsnet:status","data":{"state":"starting","protocolVersion":8}}'
echo '{"event":"tsnet:needsApproval"}'
sleep 0.3
echo '{"event":"tsnet:status","data":{"state":"running","hostname":"truffle-playground-dev","dnsName":"truffle-playground-dev.tailnet.ts.net","tailscaleIP":"100.64.0.9","loginName":"alice@example.com","protocolVersion":8}}'
cat > /dev/null"#,
        );
        let mut provider = TailscaleProvider::new(config(script, dir.path()));
        let mut status = provider.status_events();

        tokio::time::timeout(Duration::from_secs(10), provider.start())
            .await
            .expect("start must complete once the device is approved")
            .expect("start");

        assert_eq!(
            status.try_recv().ok(),
            Some(NetworkStatusEvent::NeedsApproval)
        );
        assert_eq!(
            status.try_recv().ok(),
            Some(NetworkStatusEvent::LoginKnown {
                login_name: "alice@example.com".into()
            })
        );
        assert_eq!(
            provider.local_identity().login_name.as_deref(),
            Some("alice@example.com")
        );
        provider.stop().await.unwrap();
    }

    #[tokio::test]
    async fn start_timeout_bounds_the_login_wait() {
        let dir = tempfile::tempdir().unwrap();
        // Ask for a login and never come up.
        let script = scripted_sidecar(
            dir.path(),
            r#"read start
echo '{"event":"tsnet:status","data":{"state":"starting","protocolVersion":9}}'
echo '{"event":"tsnet:authRequired","data":{"authUrl":"https://login.tailscale.com/a/abc"}}'
cat > /dev/null"#,
        );
        let mut cfg = config(script, dir.path());
        cfg.start_timeout = Some(Duration::from_millis(300));
        let mut provider = TailscaleProvider::new(cfg);
        let started = std::time::Instant::now();
        let err = tokio::time::timeout(Duration::from_secs(10), provider.start())
            .await
            .expect("a 300 ms start timeout must not wait for the 5-minute default")
            .expect_err("start must fail when the login never completes");
        assert!(started.elapsed() < Duration::from_secs(5));
        assert!(
            matches!(&err, NetworkError::StartFailed(m) if m.contains("300ms") && m.contains("start_timeout(None)")),
            "unexpected error: {err:?}"
        );
        let _ = provider.stop().await;
    }

    #[tokio::test]
    async fn every_new_login_url_is_reported_while_start_waits_indefinitely() {
        let dir = tempfile::tempdir().unwrap();
        // A protocol 9 sidecar re-sends authRequired when tsnet rotates the
        // URL; start(None) keeps waiting through both and succeeds once the
        // login completes.
        let script = scripted_sidecar(
            dir.path(),
            r#"read start
echo '{"event":"tsnet:status","data":{"state":"starting","protocolVersion":9}}'
echo '{"event":"tsnet:authRequired","data":{"authUrl":"https://login.tailscale.com/a/first"}}'
sleep 0.2
echo '{"event":"tsnet:authRequired","data":{"authUrl":"https://login.tailscale.com/a/second"}}'
sleep 0.2
echo '{"event":"tsnet:status","data":{"state":"running","hostname":"truffle-playground-dev","dnsName":"truffle-playground-dev.tailnet.ts.net","tailscaleIP":"100.64.0.9","loginName":"alice@example.com","protocolVersion":9}}'
cat > /dev/null"#,
        );
        let mut cfg = config(script, dir.path());
        cfg.start_timeout = None;
        let mut provider = TailscaleProvider::new(cfg);
        let mut events = provider.peer_events();
        tokio::time::timeout(Duration::from_secs(10), provider.start())
            .await
            .expect("start must finish once the login completes")
            .expect("start");
        let mut urls = Vec::new();
        while let Ok(event) = events.try_recv() {
            if let NetworkPeerEvent::AuthRequired { url } = event {
                urls.push(url);
            }
        }
        assert_eq!(
            urls,
            vec![
                "https://login.tailscale.com/a/first".to_string(),
                "https://login.tailscale.com/a/second".to_string()
            ]
        );
        provider.stop().await.unwrap();
    }

    #[tokio::test]
    async fn default_status_events_is_closed_for_providers_without_any() {
        struct Silent;
        // Only the default method is under test; the rest is never called.
        #[allow(clippy::manual_async_fn)]
        impl NetworkProvider for Silent {
            fn start(
                &mut self,
            ) -> impl std::future::Future<Output = Result<(), NetworkError>> + Send {
                async { Ok(()) }
            }
            fn stop(&self) -> impl std::future::Future<Output = Result<(), NetworkError>> + Send {
                async { Ok(()) }
            }
            fn local_identity(&self) -> NodeIdentity {
                unreachable!()
            }
            fn local_addr(&self) -> PeerAddr {
                unreachable!()
            }
            fn peer_events(&self) -> broadcast::Receiver<NetworkPeerEvent> {
                unreachable!()
            }
            fn peers(&self) -> impl std::future::Future<Output = Vec<NetworkPeer>> + Send {
                async { Vec::new() }
            }
            fn dial_tcp(
                &self,
                _addr: &str,
                _port: u16,
            ) -> impl std::future::Future<Output = Result<TcpStream, NetworkError>> + Send
            {
                async { unreachable!() }
            }
            fn listen_tcp(
                &self,
                _port: u16,
            ) -> impl std::future::Future<Output = Result<NetworkTcpListener, NetworkError>> + Send
            {
                async { unreachable!() }
            }
            fn unlisten_tcp(
                &self,
                _port: u16,
            ) -> impl std::future::Future<Output = Result<(), NetworkError>> + Send {
                async { Ok(()) }
            }
            fn bind_udp(
                &self,
                _port: u16,
            ) -> impl std::future::Future<
                Output = Result<crate::network::NetworkUdpSocket, NetworkError>,
            > + Send {
                async { unreachable!() }
            }
            fn ping(
                &self,
                _addr: &str,
            ) -> impl std::future::Future<Output = Result<PingResult, NetworkError>> + Send
            {
                async { unreachable!() }
            }
            fn health(&self) -> impl std::future::Future<Output = HealthInfo> + Send {
                async { unreachable!() }
            }
        }
        let mut rx = Silent.status_events();
        assert!(matches!(
            rx.recv().await,
            Err(broadcast::error::RecvError::Closed)
        ));
    }

    /// A hang guard, not a latency bound: the first run of a freshly written
    /// script waits for macOS's executable assessment, seconds under load.
    const START_GUARD: Duration = Duration::from_secs(60);

    /// Protocol 10 end to end through a real child process: the "starting"
    /// status names the control server before the approval wait, and the
    /// running status carries the key expiry. Both reach status_events() in
    /// that order and stay readable from the provider afterwards.
    #[tokio::test]
    async fn start_reports_the_control_server_first_and_the_key_expiry_when_running() {
        let dir = tempfile::tempdir().unwrap();
        let script = scripted_sidecar(
            dir.path(),
            r#"read start
echo '{"event":"tsnet:status","data":{"state":"starting","hostname":"truffle-playground-dev","protocolVersion":10,"controlUrl":"https://hs.example.com"}}'
echo '{"event":"tsnet:needsApproval"}'
sleep 0.2
echo '{"event":"tsnet:status","data":{"state":"running","hostname":"truffle-playground-dev","dnsName":"truffle-playground-dev.tailnet.ts.net","tailscaleIP":"100.64.0.9","loginName":"alice@example.com","protocolVersion":10,"controlUrl":"https://hs.example.com","keyExpiry":"2026-06-24T10:00:00Z"}}'
cat > /dev/null"#,
        );
        let mut provider = TailscaleProvider::new(config(script, dir.path()));
        assert_eq!(provider.control_server(), None);
        assert_eq!(provider.key_expiry(), None);
        let mut status = provider.status_events();
        tokio::time::timeout(START_GUARD, provider.start())
            .await
            .expect("start must complete")
            .expect("start");

        let headscale = ControlServer::Custom {
            url: "https://hs.example.com".into(),
        };
        let expiry = KeyExpiry::At(std::time::UNIX_EPOCH + Duration::from_secs(1_782_295_200));
        let mut seen = Vec::new();
        while let Ok(event) = status.try_recv() {
            seen.push(event);
        }
        assert_eq!(
            seen,
            vec![
                NetworkStatusEvent::ControlServerKnown {
                    control_server: headscale.clone()
                },
                NetworkStatusEvent::NeedsApproval,
                NetworkStatusEvent::LoginKnown {
                    login_name: "alice@example.com".into()
                },
                NetworkStatusEvent::KeyExpiryKnown { key_expiry: expiry },
            ],
            "the server is known before the approval wait, announced once"
        );
        assert_eq!(provider.control_server(), Some(headscale));
        assert_eq!(provider.key_expiry(), Some(expiry));
        assert_eq!(
            provider.health().await.key_expiry.as_deref(),
            Some("2026-06-24T10:00:00Z")
        );
        provider.stop().await.unwrap();
    }

    /// A sidecar that exits on its own after start must be reported: before,
    /// the provider only flipped its internal state, so an app kept showing
    /// a running node that served nothing.
    #[tokio::test]
    async fn a_sidecar_exit_after_start_is_reported_with_its_code() {
        let dir = tempfile::tempdir().unwrap();
        let script = scripted_sidecar(
            dir.path(),
            r#"read start
echo '{"event":"tsnet:status","data":{"state":"running","hostname":"truffle-playground-dev","dnsName":"truffle-playground-dev.tailnet.ts.net","tailscaleIP":"100.64.0.9","protocolVersion":10}}'
sleep 0.3
exit 3"#,
        );
        let mut provider = TailscaleProvider::new(config(script, dir.path()));
        let mut status = provider.status_events();
        tokio::time::timeout(START_GUARD, provider.start())
            .await
            .expect("start must complete")
            .expect("start");
        let exited = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match status.recv().await {
                    Ok(NetworkStatusEvent::SidecarExited { exit_code }) => break exit_code,
                    Ok(_) => continue,
                    Err(e) => panic!("status events ended without the exit: {e}"),
                }
            }
        })
        .await
        .expect("the exit must be reported");
        assert_eq!(exited, Some(3));
        assert_eq!(provider.health().await.state, "crashed");
        assert_eq!(provider.sidecar_pid(), None);
        let _ = provider.stop().await;
    }

    /// Detection latency: the closed stdout makes the watcher look at once
    /// instead of at its next one-second poll (which put the median near
    /// 500 ms). Five kills, median well under that.
    #[tokio::test]
    async fn a_killed_sidecar_is_reported_within_milliseconds() {
        // One script for every round: macOS assesses a freshly written
        // executable on its first run, which can take seconds on a loaded
        // machine and has nothing to do with what is measured here.
        let dir = tempfile::tempdir().unwrap();
        let script = scripted_sidecar(
            dir.path(),
            r#"read start
echo '{"event":"tsnet:status","data":{"state":"running","hostname":"truffle-playground-dev","dnsName":"truffle-playground-dev.tailnet.ts.net","tailscaleIP":"100.64.0.9","protocolVersion":10}}'
while read line; do :; done"#,
        );
        let mut latencies = Vec::new();
        for _ in 0..5 {
            let mut provider = TailscaleProvider::new(config(script.clone(), dir.path()));
            let mut status = provider.status_events();
            tokio::time::timeout(START_GUARD, provider.start())
                .await
                .expect("start must complete")
                .expect("start");
            let pid = provider.sidecar_pid().expect("a running sidecar has a pid");
            let killed = std::process::Command::new("kill")
                .args(["-9", &pid.to_string()])
                .status()
                .expect("run kill");
            assert!(killed.success());
            let t0 = std::time::Instant::now();
            let exit_code = tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    if let Ok(NetworkStatusEvent::SidecarExited { exit_code }) = status.recv().await
                    {
                        break exit_code;
                    }
                }
            })
            .await
            .expect("the kill must be reported");
            latencies.push(t0.elapsed());
            assert_eq!(exit_code, None, "a signal leaves no exit code");
            let _ = provider.stop().await;
        }
        latencies.sort();
        eprintln!("sidecar exit -> SidecarExited latencies: {latencies:?}");
        assert!(
            latencies[2] < Duration::from_millis(300),
            "median detection latency {:?} (all: {latencies:?})",
            latencies[2]
        );
    }

    fn drain_status(
        status: &mut broadcast::Receiver<NetworkStatusEvent>,
    ) -> Vec<NetworkStatusEvent> {
        let mut seen = Vec::new();
        while let Ok(event) = status.try_recv() {
            seen.push(event);
        }
        seen
    }

    /// The shell half of a protocol 11 sidecar: logs every command line to
    /// `log`, answers tsnet:version, and runs `on_start` / `on_logout` for
    /// those commands (answers carry the command's requestId as `$id`).
    fn v11_script(
        dir: &std::path::Path,
        log: &std::path::Path,
        on_start: &str,
        on_logout: &str,
    ) -> PathBuf {
        scripted_sidecar(
            dir,
            &format!(
                r#"log='{log}'
while IFS= read -r line; do
  printf '%s\n' "$line" >> "$log"
  id=$(printf '%s' "$line" | sed -n 's/.*"requestId":"\([^"]*\)".*/\1/p')
  case "$line" in
    *'"tsnet:version"'*)
      echo "{{\"event\":\"tsnet:versionResult\",\"data\":{{\"requestId\":\"$id\",\"protocolVersion\":11,\"tailscaleVersion\":\"1.104.1\",\"tailscaleVersionLong\":\"1.104.1-dev20261020-g0123abcd\"}}}}" ;;
    *'"tsnet:start"'*)
{on_start} ;;
    *'"tsnet:logout"'*)
{on_logout} ;;
  esac
done"#,
                log = log.display()
            ),
        )
    }

    const RUNNING_V11: &str = r#"      echo '{"event":"tsnet:status","data":{"state":"starting","protocolVersion":11,"tailscaleVersion":"1.104.1","tailscaleVersionLong":"1.104.1-dev20261020-g0123abcd"}}'
      echo '{"event":"tsnet:lockStatus","data":{"enabled":true,"signed":false}}'
      echo '{"event":"tsnet:status","data":{"state":"running","hostname":"truffle-playground-dev","dnsName":"truffle-playground-dev.tailnet.ts.net","tailscaleIP":"100.64.0.9","protocolVersion":11,"tailscaleVersion":"1.104.1","tailscaleVersionLong":"1.104.1-dev20261020-g0123abcd"}}'"#;

    fn commands(log: &std::path::Path) -> Vec<String> {
        std::fs::read_to_string(log)
            .unwrap_or_default()
            .lines()
            .map(|line| {
                serde_json::from_str::<serde_json::Value>(line).expect("a JSON command")["command"]
                    .as_str()
                    .expect("a command name")
                    .to_string()
            })
            .collect()
    }

    /// Protocol 11 end to end: an opted-out node asks the sidecar's version
    /// before it starts anything, sends the opt-out with the start, learns
    /// the Tailscale client, hears it is locked out before start() returns,
    /// and logout signs out first and stops after.
    #[tokio::test]
    async fn an_opted_out_node_asks_first_and_logout_signs_out_before_it_stops() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("commands.log");
        let script = v11_script(
            dir.path(),
            &log,
            RUNNING_V11,
            r#"      echo "{\"event\":\"tsnet:loggedOut\",\"data\":{\"requestId\":\"$id\"}}""#,
        );
        let mut cfg = config(script, dir.path());
        cfg.client_log_upload = false;
        let mut provider = TailscaleProvider::new(cfg);
        let mut status = provider.status_events();
        tokio::time::timeout(START_GUARD, provider.start())
            .await
            .expect("start must complete")
            .expect("start");

        assert_eq!(
            drain_status(&mut status),
            vec![
                NetworkStatusEvent::NeedsSignature,
                NetworkStatusEvent::KeyExpiryKnown {
                    key_expiry: KeyExpiry::Never
                },
            ],
            "the lock state arrives before start() returns"
        );
        assert_eq!(provider.needs_signature(), Some(true));
        assert_eq!(
            provider.tailscale_version().map(|v| v.version),
            Some("1.104.1".to_string())
        );
        let start_line = std::fs::read_to_string(&log).unwrap();
        assert!(
            start_line.contains(r#""clientLogUpload":false"#),
            "the opt-out rides tsnet:start"
        );

        provider.logout().await.expect("logout");
        // Stopped once the answer came: no sidecar left, nothing running.
        // (Whether the tsnet:stop line itself reaches the sidecar before
        // its stdin closes is GoSidecar::shutdown's race; either ends it.)
        assert_eq!(provider.health().await.state, "stopped");
        assert_eq!(provider.sidecar_pid(), None);
        let sent = commands(&log);
        assert_eq!(sent[..2], ["tsnet:version", "tsnet:start"]);
        assert!(sent.iter().any(|c| c == "tsnet:logout"), "{sent:?}");
    }

    /// An older sidecar would ignore the opt-out and upload anyway, so it is
    /// refused before tsnet:start is ever sent.
    #[tokio::test]
    async fn an_opt_out_refuses_an_older_sidecar_before_anything_starts() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("commands.log");
        let script = scripted_sidecar(
            dir.path(),
            &format!(
                r#"while IFS= read -r line; do
  printf '%s\n' "$line" >> '{}'
  case "$line" in
    *'"tsnet:version"'*)
      echo '{{"event":"tsnet:error","data":{{"code":"UNKNOWN_CMD","message":"unknown command: tsnet:version"}}}}' ;;
  esac
done"#,
                log.display()
            ),
        );
        let mut cfg = config(script, dir.path());
        cfg.client_log_upload = false;
        let mut provider = TailscaleProvider::new(cfg);
        let err = tokio::time::timeout(START_GUARD, provider.start())
            .await
            .expect("the refusal must not wait for a start timeout")
            .expect_err("an older sidecar must be refused");
        assert!(
            matches!(&err, NetworkError::StartFailed(m) if m.contains("protocol 11") && m.contains("an older one")),
            "unexpected error: {err:?}"
        );
        let sent = commands(&log);
        assert!(
            !sent.iter().any(|c| c == "tsnet:start"),
            "nothing may start on an older sidecar: {sent:?}"
        );
        assert_eq!(provider.sidecar_pid(), None);
    }

    /// An opted-out node on a tailnet that requires client logs never runs;
    /// start() fails at once, naming why, instead of waiting out its timeout.
    #[tokio::test]
    async fn logging_required_fails_the_start_at_once() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("commands.log");
        let script = v11_script(
            dir.path(),
            &log,
            r#"      echo '{"event":"tsnet:status","data":{"state":"starting","protocolVersion":11}}'
      echo '{"event":"tsnet:loggingRequired"}'"#,
            ":",
        );
        let mut cfg = config(script, dir.path());
        cfg.client_log_upload = false;
        let mut provider = TailscaleProvider::new(cfg);
        let mut status = provider.status_events();
        let err = tokio::time::timeout(START_GUARD, provider.start())
            .await
            .expect("the start must not wait out its 5-minute timeout")
            .expect_err("the node can never run");
        assert!(
            matches!(&err, NetworkError::StartFailed(m) if m.contains("requires client logging") && m.contains("client_log_upload(false)")),
            "unexpected error: {err:?}"
        );
        assert!(drain_status(&mut status).contains(&NetworkStatusEvent::LoggingRequired));
        let _ = provider.stop().await;
    }

    /// A logout the control server does not take still stops the node, and
    /// the error says the device stayed signed in.
    #[tokio::test]
    async fn a_refused_logout_still_stops_the_node() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("commands.log");
        let script = v11_script(
            dir.path(),
            &log,
            RUNNING_V11,
            r#"      echo "{\"event\":\"tsnet:error\",\"data\":{\"code\":\"LOGOUT_ERROR\",\"message\":\"control server unreachable\",\"requestId\":\"$id\"}}""#,
        );
        let mut provider = TailscaleProvider::new(config(script, dir.path()));
        tokio::time::timeout(START_GUARD, provider.start())
            .await
            .expect("start must complete")
            .expect("start");
        let err = provider.logout().await.expect_err("the logout was refused");
        assert!(
            matches!(&err, NetworkError::SidecarError(m) if m.contains("LOGOUT_ERROR") && m.contains("unreachable")),
            "unexpected error: {err:?}"
        );
        assert_eq!(provider.health().await.state, "stopped");
        assert_eq!(provider.sidecar_pid(), None);
        // The default start asks nothing first.
        assert_eq!(commands(&log)[0], "tsnet:start");
    }

    /// On an older sidecar logout is refused locally, and the node keeps
    /// running: the caller decides what to do instead.
    #[tokio::test]
    async fn logout_on_an_older_sidecar_is_unsupported_and_stops_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let script = scripted_sidecar(
            dir.path(),
            r#"read start
echo '{"event":"tsnet:status","data":{"state":"running","hostname":"truffle-playground-dev","dnsName":"truffle-playground-dev.tailnet.ts.net","tailscaleIP":"100.64.0.9","protocolVersion":10}}'
cat > /dev/null"#,
        );
        let mut provider = TailscaleProvider::new(config(script, dir.path()));
        tokio::time::timeout(START_GUARD, provider.start())
            .await
            .expect("start must complete")
            .expect("start");
        let err = provider.logout().await.expect_err("v10 cannot log out");
        assert!(
            matches!(&err, NetworkError::Unsupported(m) if m.contains("protocol 11") && m.contains("speaks 10")),
            "unexpected error: {err:?}"
        );
        assert_eq!(provider.health().await.state, "running");
        assert_eq!(provider.tailscale_version(), None);
        assert_eq!(provider.needs_signature(), None);
        provider.stop().await.unwrap();
    }
}
