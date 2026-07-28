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
use tokio::task::JoinHandle;

use super::bridge::{Bridge, DIAL_TIMEOUT};
use super::protocol::ProxyAddCommandData;
use super::sidecar::{GoSidecar, SidecarConfig, SidecarInternalEvent};
use crate::network::{
    DialOpts, HealthInfo, IncomingConnection, ListenOpts, NetworkError, NetworkPeer,
    NetworkPeerEvent, NetworkTcpListener, NodeIdentity, PeerAddr, PingResult, ProxyAddParams,
    ProxyAddResult, ProxyListEntry, ProxyRuntimeError,
};

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

    /// Health info cache.
    health: Arc<RwLock<HealthInfo>>,

    /// Handle to the Go sidecar (set during start).
    sidecar: Arc<Mutex<Option<GoSidecar>>>,

    /// Handle to the bridge (set during start).
    bridge: Arc<Mutex<Option<Arc<Bridge>>>>,

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
}

impl TailscaleProvider {
    /// Create a new TailscaleProvider with the given configuration.
    ///
    /// Does not start the provider — call [`start()`](crate::network::NetworkProvider::start) to begin.
    pub fn new(config: TailscaleConfig) -> Self {
        let (peer_event_tx, _) = broadcast::channel(256);
        let (proxy_error_tx, _) = broadcast::channel(64);

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
        };

        Self {
            config,
            state: Arc::new(RwLock::new(ProviderState::Stopped)),
            identity: Arc::new(std::sync::RwLock::new(initial_identity)),
            local_addr: Arc::new(std::sync::RwLock::new(PeerAddr::default())),
            peers: Arc::new(RwLock::new(HashMap::new())),
            peer_event_tx,
            health: Arc::new(RwLock::new(HealthInfo {
                state: "stopped".to_string(),
                healthy: false,
                ..Default::default()
            })),
            sidecar: Arc::new(Mutex::new(None)),
            bridge: Arc::new(Mutex::new(None)),
            bridge_shutdown_tx: Arc::new(Mutex::new(None)),
            session_token: Arc::new(RwLock::new([0u8; 32])),
            local_tailscale_id: Arc::new(std::sync::RwLock::new(None)),
            proxy_error_tx,
            sidecar_protocol_version: Arc::new(std::sync::atomic::AtomicU32::new(0)),
        }
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
        proxy_error_tx: broadcast::Sender<ProxyRuntimeError>,
        sidecar_protocol_version: Arc<std::sync::atomic::AtomicU32>,
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
                                protocol_version,
                            } => {
                                // 0 = v1/unknown; gates RFC 023 v2 proxy features.
                                sidecar_protocol_version.store(
                                    protocol_version.unwrap_or(0),
                                    std::sync::atomic::Ordering::Relaxed,
                                );
                                let ip: Option<IpAddr> = tailscale_ip.parse().ok();

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

                                {
                                    let mut h = health.write().await;
                                    h.state = "running".to_string();
                                    h.healthy = true;
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
                            SidecarInternalEvent::Stopped => {
                                *state.write().await = ProviderState::Stopped;
                                let mut h = health.write().await;
                                h.state = "stopped".to_string();
                                h.healthy = false;
                                tracing::info!("tailscale provider stopped");
                                return;
                            }
                            SidecarInternalEvent::StateChange { state: new_state } => {
                                let mut h = health.write().await;
                                h.state = new_state;
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
                                let new_peers: HashMap<String, NetworkPeer> = sidecar_peers
                                    .iter()
                                    .filter(|p| {
                                        if let Some(ref me) = self_id {
                                            if p.id == *me {
                                                return false;
                                            }
                                        }
                                        is_app_peer(&p.hostname, &app_id)
                                    })
                                    .map(|p| {
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
                                            if is_app_peer(&p.hostname, &app_id) {
                                                let np = Self::sidecar_peer_to_network_peer(&p);
                                                peer_map.insert(np.id.clone(), np.clone());
                                                let _ = peer_event_tx
                                                    .send(NetworkPeerEvent::Joined(np));
                                            }
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
                                            if is_app_peer(&p.hostname, &app_id) {
                                                let np = Self::sidecar_peer_to_network_peer(&p);
                                                peer_map.insert(np.id.clone(), np.clone());
                                                let _ = peer_event_tx
                                                    .send(NetworkPeerEvent::Updated(np));
                                            }
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
                                let mut h = health.write().await;
                                h.state = "crashed".to_string();
                                h.healthy = false;
                                if let Some(tx) = started_tx.take() {
                                    let _ = tx.send(Err(NetworkError::SidecarError(format!(
                                        "process exited with code {exit_code:?}"
                                    ))));
                                }
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
        let bridge = Bridge::bind(token).await?;
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
        };

        // Spawn the sidecar
        let (sidecar, sidecar_rx) = GoSidecar::spawn(sidecar_config.clone()).await?;

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
            self.proxy_error_tx.clone(),
            self.sidecar_protocol_version.clone(),
        );

        // Send start command to sidecar
        sidecar.send_start(&sidecar_config).await?;

        *self.sidecar.lock().await = Some(sidecar);

        // Wait for the sidecar to reach "running" state.
        // Use a generous timeout (5 min) because browser auth may take a while.
        // Auth URLs are emitted via peer_events() so the caller can display them.
        let auth_timeout = Duration::from_secs(300);
        let result = tokio::time::timeout(auth_timeout, started_rx)
            .await
            .map_err(|_| {
                NetworkError::StartFailed(
                    "timed out waiting for authentication (5 min). \
                 Subscribe to peer_events() to display auth URLs."
                        .into(),
                )
            })?
            .map_err(|_| NetworkError::StartFailed("start signal channel dropped".into()))?;

        match result {
            Ok(()) => {
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

        // Scope the sidecar lock: subscribe + send, then release
        let event_rx = {
            let sidecar_guard = self.sidecar.lock().await;
            let sidecar = sidecar_guard.as_ref().ok_or(NetworkError::NotRunning)?;

            let event_rx = sidecar.subscribe();

            sidecar
                .send_dial(request_id.clone(), addr.to_string(), port, opts.tls)
                .await?;

            event_rx
        };

        // Wait for either:
        // 1. Bridge delivers the TcpStream (success path)
        // 2. Sidecar reports dial failure via event (error path)
        // 3. Timeout
        Self::await_dial_result(&bridge, &request_id, dial_rx, event_rx, DIAL_TIMEOUT).await
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

        // Scope the sidecar lock: subscribe + send listen, then release
        let mut event_rx = {
            let sidecar_guard = self.sidecar.lock().await;
            let sidecar = sidecar_guard.as_ref().ok_or(NetworkError::NotRunning)?;

            let event_rx = sidecar.subscribe();
            // `Some(true)` → tsnet ListenTLS with MagicDNS certs (RFC 023
            // §7.1). Plain listeners send None so the field is omitted on
            // the wire — sidecars predating the flag parse the command
            // unchanged.
            let tls = if opts.tls { Some(true) } else { None };
            sidecar.send_listen(port, tls).await?;
            event_rx
        };

        // Wait for confirmation or error.
        // When port is 0, the sidecar assigns an ephemeral port and reports
        // the actual port in the Listening event.
        let actual_port = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                match event_rx.recv().await {
                    Ok(SidecarInternalEvent::Listening { port: p }) if port == 0 || p == port => {
                        return Ok(p);
                    }
                    Ok(SidecarInternalEvent::Error { code, message }) => {
                        return Err(NetworkError::ListenFailed(format!("[{code}] {message}")));
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        return Err(NetworkError::SidecarError("event channel closed".into()));
                    }
                    _ => continue,
                }
            }
        })
        .await
        .map_err(|_| NetworkError::ListenFailed("listen confirmation timed out".into()))??;

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

        // Scope the sidecar lock: subscribe + send ping, then release
        let mut event_rx = {
            let sidecar_guard = self.sidecar.lock().await;
            let sidecar = sidecar_guard.as_ref().ok_or(NetworkError::NotRunning)?;

            let event_rx = sidecar.subscribe();
            sidecar.send_ping(target.clone(), None).await?;
            event_rx
        };

        // Wait for result
        let result = tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                match event_rx.recv().await {
                    Ok(SidecarInternalEvent::PingResult(data)) if data.target == target => {
                        if !data.error.is_empty() {
                            return Err(NetworkError::PingFailed(data.error));
                        }
                        let connection = if data.direct {
                            "direct".to_string()
                        } else if !data.relay.is_empty() {
                            format!("relay:{}", data.relay)
                        } else {
                            "unknown".to_string()
                        };
                        return Ok(PingResult {
                            latency: Duration::from_secs_f64(data.latency_ms / 1000.0),
                            connection,
                            peer_addr: if data.peer_addr.is_empty() {
                                None
                            } else {
                                Some(data.peer_addr)
                            },
                        });
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        return Err(NetworkError::SidecarError("event channel closed".into()));
                    }
                    _ => continue,
                }
            }
        })
        .await
        .map_err(|_| NetworkError::PingFailed("ping timed out".into()))?;

        result
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

        // Scope the sidecar lock: subscribe + send whois, then release
        let mut event_rx = {
            let sidecar_guard = self.sidecar.lock().await;
            let sidecar = sidecar_guard.as_ref().ok_or(NetworkError::NotRunning)?;

            let event_rx = sidecar.subscribe();
            sidecar.send_whois(target.clone()).await?;
            event_rx
        };

        // Wait for the result, correlated by address (as ping does by
        // target). Safe as string equality: the sidecar echoes the sent
        // addr verbatim, never a re-canonicalized form.
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                match event_rx.recv().await {
                    Ok(SidecarInternalEvent::WhoisResult(data)) if data.addr == target => {
                        if !data.error.is_empty() {
                            return Err(NetworkError::SidecarError(data.error));
                        }
                        // Belt-and-braces for the "absent, not fabricated"
                        // contract: the wire omits empty fields, but don't
                        // let that depend on the serializer — drop
                        // present-but-empty fields, and fold an identity
                        // with no information at all into None.
                        return Ok(data
                            .identity
                            .map(super::super::TailscalePeerIdentity::normalized)
                            .filter(|identity| !identity.is_empty()));
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        return Err(NetworkError::SidecarError("event channel closed".into()));
                    }
                    _ => continue,
                }
            }
        })
        .await
        .map_err(|_| NetworkError::SidecarError("whois timed out".into()))?
    }

    async fn bind_udp(&self, port: u16) -> Result<super::super::NetworkUdpSocket, NetworkError> {
        if *self.state.read().await != ProviderState::Running {
            return Err(NetworkError::NotRunning);
        }

        // Scope the sidecar lock: subscribe + send listenPacket, then release
        let mut event_rx = {
            let sidecar_guard = self.sidecar.lock().await;
            let sidecar = sidecar_guard.as_ref().ok_or(NetworkError::NotRunning)?;

            let event_rx = sidecar.subscribe();
            sidecar.send_listen_packet(port).await?;
            event_rx
        };

        // Wait for the sidecar to report the local relay port
        let local_port = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                match event_rx.recv().await {
                    Ok(SidecarInternalEvent::ListeningPacket {
                        port: p,
                        local_port,
                    }) if p == port => {
                        return Ok(local_port);
                    }
                    Ok(SidecarInternalEvent::Error { code, message }) => {
                        return Err(NetworkError::ListenFailed(format!(
                            "UDP bind failed [{code}] {message}"
                        )));
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        return Err(NetworkError::SidecarError("event channel closed".into()));
                    }
                    _ => continue,
                }
            }
        })
        .await
        .map_err(|_| {
            NetworkError::ListenFailed("UDP listenPacket confirmation timed out".into())
        })??;

        // Bind a local UDP socket and connect it to the relay
        let local_socket = tokio::net::UdpSocket::bind("127.0.0.1:0")
            .await
            .map_err(|e| NetworkError::Internal(format!("failed to bind local UDP socket: {e}")))?;

        local_socket
            .connect(format!("127.0.0.1:{local_port}"))
            .await
            .map_err(|e| {
                NetworkError::Internal(format!("failed to connect local UDP socket to relay: {e}"))
            })?;

        let rust_local_addr = local_socket
            .local_addr()
            .map_err(|e| NetworkError::Internal(format!("failed to get local UDP addr: {e}")))?;

        // Send a registration packet so the relay learns our address.
        // Without this, the relay drops inbound packets because it doesn't
        // know where to forward them (it learns the Rust peer address from
        // the first outbound packet).
        local_socket
            .send(b"TRUFFLE_UDP_REGISTER")
            .await
            .map_err(|e| NetworkError::Internal(format!("failed to send UDP registration: {e}")))?;

        tracing::info!(
            tsnet_port = port,
            relay_port = local_port,
            rust_local_addr = %rust_local_addr,
            "UDP socket bound via tsnet relay (registered)"
        );

        Ok(super::super::NetworkUdpSocket::new(local_socket, port))
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

        // Scope the sidecar lock: subscribe + send command, then release
        let mut event_rx = {
            let sidecar_guard = self.sidecar.lock().await;
            let sidecar = sidecar_guard.as_ref().ok_or(NetworkError::NotRunning)?;
            let event_rx = sidecar.subscribe();
            sidecar
                .send_proxy_add(ProxyAddCommandData {
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
                })
                .await?;
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

        // Scope the sidecar lock: subscribe + send command, then release
        let mut event_rx = {
            let sidecar_guard = self.sidecar.lock().await;
            let sidecar = sidecar_guard.as_ref().ok_or(NetworkError::NotRunning)?;
            let event_rx = sidecar.subscribe();
            sidecar.send_proxy_remove(id).await?;
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

        // Scope the sidecar lock: subscribe + send command, then release
        let mut event_rx = {
            let sidecar_guard = self.sidecar.lock().await;
            let sidecar = sidecar_guard.as_ref().ok_or(NetworkError::NotRunning)?;
            let event_rx = sidecar.subscribe();
            sidecar.send_proxy_list().await?;
            event_rx
        };

        // Wait for the list response
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                match event_rx.recv().await {
                    Ok(SidecarInternalEvent::ProxyList { proxies }) => {
                        return Ok(proxies
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
                            .collect());
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
    pub(super) async fn await_dial_result(
        bridge: &Bridge,
        request_id: &str,
        dial_rx: oneshot::Receiver<TcpStream>,
        mut event_rx: broadcast::Receiver<SidecarInternalEvent>,
        timeout: Duration,
    ) -> Result<TcpStream, NetworkError> {
        // Spawn a task to watch for dial failure events. It lives OUTSIDE
        // the timeout future so its JoinHandle survives a timeout and we can
        // always abort it — a dropped handle would detach the task, leaking
        // a broadcast receiver that loops forever.
        let fail_request_id = request_id.to_string();
        let (fail_tx, fail_rx) = oneshot::channel::<String>();
        let fail_watcher: JoinHandle<()> = tokio::spawn(async move {
            loop {
                match event_rx.recv().await {
                    Ok(SidecarInternalEvent::DialFailed {
                        request_id: rid,
                        error,
                    }) if rid == fail_request_id => {
                        let _ = fail_tx.send(error);
                        return;
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        let _ = fail_tx.send("event channel closed".to_string());
                        return;
                    }
                    _ => continue,
                }
            }
        });

        let result = match tokio::time::timeout(timeout, async {
            tokio::select! {
                stream_result = dial_rx => {
                    stream_result.map_err(|_| NetworkError::DialFailed("dial cancelled".into()))
                }
                fail_result = fail_rx => {
                    let error = fail_result.unwrap_or_else(|_| "dial watcher dropped".to_string());
                    Err(NetworkError::DialFailed(error))
                }
            }
        })
        .await
        {
            Ok(result) => result,
            Err(_) => Err(NetworkError::DialTimeout(timeout)),
        };

        // Cancel the fail-watcher task so it doesn't leak — including on timeout.
        fail_watcher.abort();

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
        };
        let dbg = format!("{config:?}");
        assert!(!dbg.contains("SECRET123"));
        assert!(dbg.contains("[REDACTED]"));
    }
}
