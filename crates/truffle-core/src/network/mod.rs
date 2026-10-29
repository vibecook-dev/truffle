//! Layer 3: Network — Peer discovery, addressing, encrypted tunnels.
//!
//! This module defines the [`NetworkProvider`] trait, the public API for Layer 3.
//! The trait is generic — no Tailscale-specific types leak through.
//!
//! The [`tailscale`] submodule contains the [`TailscaleProvider`](tailscale::TailscaleProvider) implementation
//! that wraps the Go sidecar (tsnet) and bridge.

pub mod login_allow;
pub mod tailscale;

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::net::TcpStream;
use tokio::sync::broadcast;

// ---------------------------------------------------------------------------
// NetworkProvider trait — the public API of Layer 3
// ---------------------------------------------------------------------------

/// The trust boundary a provider supplies to transports.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum NetworkTrust {
    /// Network operations and peer identity must come from the provider.
    #[default]
    Authenticated,
    /// Explicit opt-in for test providers. Unverified connections and direct
    /// datagram sockets are restricted to loopback, never wildcard interfaces.
    UnverifiedLoopback,
}

// @atlas contract network-provider "Layer 3: the swappable network provider" {
//   implemented-by crate:truffle-core
//   note sym:truffle_core::network::tailscale::TailscaleProvider "The only production implementor; every MockNetworkProvider is test code."
//   see "docs/rfcs/012-layered-architecture-redesign.md#35-layer-3-network"
// }
/// Provides network-level peer discovery and raw connectivity.
///
/// The primary implementation is [`TailscaleProvider`](tailscale::TailscaleProvider)
/// which uses tsnet via a Go sidecar. The trait is designed to be swappable —
/// future providers could use mDNS (LAN), STUN/TURN (internet), or Bluetooth.
///
/// # Layer rules
///
/// - Layer 3 does NOT know about WebSocket, QUIC, or any Layer 4 protocol
/// - Layer 3 does NOT know about envelopes, namespaces, or messages
/// - Layer 3 provides raw `TcpStream` — not framed connections
/// - `peer_events()` is the ONLY source of peer events — no polling, no announce
///
/// All async methods return `Send` futures so that `Node<N>` can be used
/// inside `tokio::spawn` tasks (required by the file transfer subsystem
/// and any other code that needs to spawn tasks with node access).
pub trait NetworkProvider: Send + Sync {
    /// Production providers fail closed. Only loopback test providers opt out.
    fn trust_mode(&self) -> NetworkTrust {
        NetworkTrust::Authenticated
    }

    /// Start the network provider.
    ///
    /// This spawns child processes, binds ports, and performs authentication.
    /// If authentication is required (e.g., Tailscale browser auth), the provider
    /// emits `NetworkPeerEvent::AuthRequired { url }` via `peer_events()` and
    /// **keeps waiting** until auth completes or the timeout is reached.
    ///
    /// Callers should subscribe to `peer_events()` BEFORE calling `start()` to
    /// receive auth URLs and display them to the user.
    ///
    /// Returns `Ok(())` when the provider is fully online, or `Err` if auth
    /// times out or a fatal error occurs.
    fn start(&mut self) -> impl std::future::Future<Output = Result<(), NetworkError>> + Send;

    /// Stop the network provider and clean up all resources.
    fn stop(&self) -> impl std::future::Future<Output = Result<(), NetworkError>> + Send;

    /// Local node's identity (stable ID, hostname, display name).
    ///
    /// Returns a clone of the current cached identity. The identity is
    /// populated after [`start()`](Self::start) completes and may be
    /// updated when the sidecar reports `tsnet:status`.
    fn local_identity(&self) -> NodeIdentity;

    /// Local node's network address.
    ///
    /// Returns a clone of the current cached address. The address is
    /// populated after [`start()`](Self::start) completes and may be
    /// updated when the sidecar reports `tsnet:status`.
    fn local_addr(&self) -> PeerAddr;

    // ── Discovery (event-driven, NOT polling) ──

    /// Subscribe to peer events. Fires immediately when peers join/leave/update.
    ///
    /// Uses `WatchIPNBus` for real-time notifications instead of polling.
    fn peer_events(&self) -> broadcast::Receiver<NetworkPeerEvent>;

    /// Subscribe to changes in this node's own network status (device
    /// approval pending, own login learned, …).
    ///
    /// Subscribe before [`start()`](Self::start) to see the conditions that
    /// keep `start()` waiting. The default implementation, for providers
    /// that report none, returns a receiver that is already closed.
    fn status_events(&self) -> broadcast::Receiver<NetworkStatusEvent> {
        broadcast::channel(1).1
    }

    /// Snapshot of all currently known peers.
    fn peers(&self) -> impl std::future::Future<Output = Vec<NetworkPeer>> + Send;

    // ── Connectivity primitives for Layer 4 ──

    /// Dial a TCP connection to a peer via the encrypted Tailscale tunnel.
    ///
    /// Returns a plain `TcpStream` — all bridge internals (pending_dials,
    /// session token, binary headers) are hidden inside the provider.
    fn dial_tcp(
        &self,
        addr: &str,
        port: u16,
    ) -> impl std::future::Future<Output = Result<TcpStream, NetworkError>> + Send;

    /// Dial a TCP connection with explicit options (e.g. a TLS override).
    ///
    /// The default implementation ignores the options and delegates to
    /// [`dial_tcp`](Self::dial_tcp), so providers that don't support the
    /// options keep working unchanged.
    fn dial_tcp_opts(
        &self,
        addr: &str,
        port: u16,
        opts: DialOpts,
    ) -> impl std::future::Future<Output = Result<TcpStream, NetworkError>> + Send {
        let _ = opts;
        self.dial_tcp(addr, port)
    }

    /// Listen for incoming TCP connections on a port via the Tailscale tunnel.
    ///
    /// The returned receiver yields `TcpStream`s for each accepted connection.
    fn listen_tcp(
        &self,
        port: u16,
    ) -> impl std::future::Future<Output = Result<NetworkTcpListener, NetworkError>> + Send;

    /// As [`listen_tcp`](Self::listen_tcp), with options (RFC 023 §7.1).
    ///
    /// Providers without TLS listener support reject `tls: true` rather than
    /// silently serving plaintext.
    fn listen_tcp_opts(
        &self,
        port: u16,
        opts: ListenOpts,
    ) -> impl std::future::Future<Output = Result<NetworkTcpListener, NetworkError>> + Send {
        async move {
            if opts.tls {
                return Err(NetworkError::Unsupported(
                    "TLS listeners are not supported by this provider".into(),
                ));
            }
            self.listen_tcp(port).await
        }
    }

    /// Stop listening on a previously opened port.
    fn unlisten_tcp(
        &self,
        port: u16,
    ) -> impl std::future::Future<Output = Result<(), NetworkError>> + Send;

    /// Bind a UDP socket on a port via the network tunnel.
    ///
    /// Returns a [`NetworkUdpSocket`] that transparently relays datagrams
    /// through the network provider. The socket supports `send_to` / `recv_from`
    /// with full remote address information.
    ///
    /// Not all providers support UDP. Returns [`NetworkError::Internal`] if
    /// the provider has not implemented UDP transport yet.
    fn bind_udp(
        &self,
        port: u16,
    ) -> impl std::future::Future<Output = Result<NetworkUdpSocket, NetworkError>> + Send;

    // ── Diagnostics ──

    /// Ping a peer via the network layer (Tailscale TSMP).
    fn ping(
        &self,
        addr: &str,
    ) -> impl std::future::Future<Output = Result<PingResult, NetworkError>> + Send;

    /// Node health info (key expiry, connection quality, warnings).
    fn health(&self) -> impl std::future::Future<Output = HealthInfo> + Send;

    /// Tailnet identity of the node that owns `addr` (Tailscale WhoIs).
    ///
    /// Unlike [`peers`](Self::peers), this reaches ANY tailnet device — other
    /// apps' nodes, plain machines, tagged nodes — not just app-filtered mesh
    /// peers. `Ok(None)` means the lookup succeeded but the tailnet has no
    /// identity for the address (anonymous — absent, not fabricated).
    ///
    /// The default implementation reports the capability as unsupported, so
    /// providers without WhoIs (e.g. mocks) keep compiling unchanged.
    fn whois(
        &self,
        _addr: &str,
    ) -> impl std::future::Future<Output = Result<Option<TailscalePeerIdentity>, NetworkError>> + Send
    {
        std::future::ready(Err(NetworkError::Unsupported(
            "whois not supported by this provider".into(),
        )))
    }

    // ── Reverse proxy (optional, requires sidecar) ──

    /// Start a reverse proxy. Only supported by providers with sidecar integration.
    fn proxy_add(
        &self,
        _config: ProxyAddParams,
    ) -> impl std::future::Future<Output = Result<ProxyAddResult, NetworkError>> + Send {
        std::future::ready(Err(NetworkError::Unsupported(
            "proxy_add not supported by this provider".into(),
        )))
    }

    /// Stop a reverse proxy.
    fn proxy_remove(
        &self,
        _id: &str,
    ) -> impl std::future::Future<Output = Result<(), NetworkError>> + Send {
        std::future::ready(Err(NetworkError::Unsupported(
            "proxy_remove not supported by this provider".into(),
        )))
    }

    /// List active reverse proxies.
    fn proxy_list(
        &self,
    ) -> impl std::future::Future<Output = Result<Vec<ProxyListEntry>, NetworkError>> + Send {
        std::future::ready(Err(NetworkError::Unsupported(
            "proxy_list not supported by this provider".into(),
        )))
    }

    /// Subscribe to runtime proxy-engine errors (RFC 023 G5). `None` for
    /// providers without a proxy engine — the caller then skips spawning a
    /// forwarding task.
    fn proxy_runtime_errors(&self) -> Option<broadcast::Receiver<ProxyRuntimeError>> {
        None
    }

    // ── Egress (RFC 026, optional, requires sidecar protocol 7) ──

    /// The provider's loopback SOCKS5 proxy onto the network (RFC 026 E1).
    /// `local_api` asks for the LocalAPI credential as well.
    fn loopback(
        &self,
        _local_api: bool,
    ) -> impl std::future::Future<Output = Result<crate::egress::LoopbackConfig, NetworkError>> + Send
    {
        std::future::ready(Err(NetworkError::Unsupported(
            "loopback not supported by this provider".into(),
        )))
    }

    /// Bind a loopback egress route to a fixed tailnet target (RFC 026 E2).
    fn egress_add(
        &self,
        _route: crate::egress::EgressRoute,
    ) -> impl std::future::Future<Output = Result<crate::egress::BoundEgress, NetworkError>> + Send
    {
        std::future::ready(Err(NetworkError::Unsupported(
            "egress_add not supported by this provider".into(),
        )))
    }

    /// Remove an egress route, closing its live connections (RFC 026 E2).
    fn egress_remove(
        &self,
        _id: &str,
    ) -> impl std::future::Future<Output = Result<(), NetworkError>> + Send {
        std::future::ready(Err(NetworkError::Unsupported(
            "egress_remove not supported by this provider".into(),
        )))
    }

    /// List the live egress routes (RFC 026 E2).
    fn egress_list(
        &self,
    ) -> impl std::future::Future<Output = Result<Vec<crate::egress::EgressRouteInfo>, NetworkError>>
           + Send {
        std::future::ready(Err(NetworkError::Unsupported(
            "egress_list not supported by this provider".into(),
        )))
    }

    /// The provider's loopback HTTP CONNECT proxy (RFC 026 E3).
    fn egress_http_proxy(
        &self,
    ) -> impl std::future::Future<Output = Result<crate::egress::HttpProxyConfig, NetworkError>> + Send
    {
        std::future::ready(Err(NetworkError::Unsupported(
            "egress_http_proxy not supported by this provider".into(),
        )))
    }
}

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Options for [`NetworkProvider::dial_tcp_opts`].
#[derive(Debug, Clone, Copy, Default)]
pub struct DialOpts {
    /// Override TLS wrapping of the dial. `None` = no wrap on current
    /// sidecars (RFC 023 D4 removed the legacy wrap-iff-port-443 rule);
    /// `Some(true)` / `Some(false)` force it on / off (RFC 021 §6.4).
    pub tls: Option<bool>,
}

/// Options for [`NetworkProvider::listen_tcp_opts`].
#[derive(Debug, Clone, Copy, Default)]
pub struct ListenOpts {
    /// Terminate TLS at the provider using its platform certificates
    /// (Tailscale: tsnet `ListenTLS` with automatic MagicDNS / Let's
    /// Encrypt certs, RFC 023 §7.1 — requires MagicDNS + HTTPS enabled on
    /// the tailnet). `false` = plain TCP listener.
    pub tls: bool,
}

/// A peer as seen by the network layer (Layer 3).
///
/// Contains only information available from the network provider itself
/// (e.g., Tailscale status). No transport or session state.
#[derive(Debug, Clone)]
pub struct NetworkPeer {
    /// Stable node ID from the network provider.
    pub id: String,
    /// Hostname on the network (e.g., "truffle-cli-abc123").
    pub hostname: String,
    /// Network IP address (e.g., 100.x.x.x for Tailscale).
    pub ip: IpAddr,
    /// Whether the peer is currently online.
    pub online: bool,
    /// Direct endpoint address, if connected directly.
    pub cur_addr: Option<String>,
    /// Active peer relay endpoint, if any.
    pub peer_relay: Option<String>,
    /// Whether the peer recently exchanged traffic.
    pub active: bool,
    /// Home DERP region; its presence alone does not imply relay traffic.
    pub relay: Option<String>,
    /// Operating system of the peer.
    pub os: Option<String>,
    /// Last time the peer was seen online (RFC 3339 string).
    pub last_seen: Option<String>,
    /// Key expiry timestamp (RFC 3339 string).
    pub key_expiry: Option<String>,
    /// DNS name on the tailnet (e.g., "truffle-cli-abc123.tailnet.ts.net").
    pub dns_name: Option<String>,
    /// Tailscale login (owner) name of the peer, e.g. "alice@example.com"
    /// (RFC 025 §3.3). `None` when the provider cannot know it — a sidecar
    /// older than protocol 5, or a node with no user profile. Tagged nodes
    /// report Tailscale's `tagged-devices` pseudo-login.
    pub login_name: Option<String>,
}

/// Events emitted when network peers change state.
#[derive(Debug, Clone)]
pub enum NetworkPeerEvent {
    /// A new peer appeared on the network.
    Joined(NetworkPeer),
    /// A peer left the network (by stable node ID).
    Left(String),
    /// A peer's metadata changed (IP, relay, online status, etc.).
    Updated(NetworkPeer),
    /// Authentication is required. The URL should be shown to the user.
    /// Emitted during `start()` — the provider keeps waiting for auth to complete.
    /// Emitted again whenever the URL changes (tsnet replaces an expired
    /// login URL while the node waits); the latest event is the live link.
    /// Sidecars before protocol 9 report only the first URL.
    AuthRequired {
        /// URL the user should open in a browser.
        url: String,
    },
}

/// Changes in the local node's own network status, as opposed to a peer's.
///
/// Delivered by [`NetworkProvider::status_events`] (and on a node by
/// [`Node::on_network_status`](crate::Node::on_network_status) or the
/// [`NodeBuilder::on_network_status`](crate::NodeBuilder::on_network_status)
/// hook, which also sees the events raised while `build()` is still
/// waiting for the node to come up).
///
/// The enum is `#[non_exhaustive]`: match it with a wildcard arm, since later
/// versions will report more conditions here rather than in
/// [`NetworkPeerEvent`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum NetworkStatusEvent {
    /// The tailnet has device approval turned on and this device is waiting
    /// for an admin to approve it (Tailscale's `NeedsMachineAuth` state).
    ///
    /// Raised once each time the device enters that state, typically while
    /// `start()` is waiting. `start()` keeps waiting: it succeeds as soon as
    /// the device is approved, and fails with an error naming the approval
    /// if its timeout passes first. Show the user something like "approve
    /// this device in the Tailscale admin console (Machines)". An auth key
    /// created as *pre-approved* skips this step.
    NeedsApproval,
    /// The node's own tailnet login became known, or changed.
    ///
    /// Normally raised once, during start, with the same value
    /// [`NodeIdentity::login_name`] then carries. After a fast restart from
    /// saved state the sidecar can report "running" before its network map
    /// names this node's user; the login then arrives in a later status
    /// (sidecar protocol 8), after `start()` returned, and this event marks
    /// the moment [`local_identity`](NetworkProvider::local_identity) starts
    /// carrying it.
    LoginKnown {
        /// The login, e.g. `alice@example.com` (or `tagged-devices` for a
        /// tagged node).
        login_name: String,
    },
    /// The coordination server this node logs in to became known (sidecar
    /// protocol 10).
    ///
    /// Raised once per start, before any login URL or approval request, so
    /// a UI can decide right away whether pointing the user at Tailscale's
    /// admin console makes sense ([`ControlServer::machines_admin_url`]).
    /// Also available afterwards from
    /// [`TailscaleProvider::control_server`](tailscale::TailscaleProvider::control_server).
    ControlServerKnown {
        /// Tailscale's hosted service, or a custom server such as Headscale.
        control_server: ControlServer,
    },
    /// This node's key expiry became known, or changed (sidecar protocol 10).
    ///
    /// Raised when the node comes up, and again when the expiry moves while
    /// it runs: the node re-authenticated, or an admin extended or disabled
    /// the expiry (noticed within about a minute). Also available afterwards
    /// from
    /// [`TailscaleProvider::key_expiry`](tailscale::TailscaleProvider::key_expiry).
    KeyExpiryKnown {
        /// When the key expires, or [`KeyExpiry::Never`].
        key_expiry: KeyExpiry,
    },
    /// The provider's sidecar process exited on its own: the node is off
    /// the tailnet and stays off (it is not restarted). Every bridged
    /// connection ended with it.
    ///
    /// Raised within milliseconds of the exit, both while `start()` is
    /// waiting (which then fails) and after the node came up. A clean
    /// `stop()` does not raise it. Show the node as failed, and build a new
    /// node to come back.
    SidecarExited {
        /// The process exit code; `None` when a signal ended it.
        exit_code: Option<i32>,
    },
    /// The node's Tailscale backend changed state after it came up.
    ///
    /// The one to act on is [`BackendState::NeedsLogin`]: the node's key
    /// expired, or an admin removed or logged out the device in the console,
    /// and it is off the tailnet until someone logs it in again. Since
    /// sidecar protocol 11 a change arrives within about a second; older
    /// sidecars notice it at their next once-a-minute check.
    BackendStateChanged {
        /// The new state.
        state: BackendState,
    },
    /// The tailnet requires client logging, and this node opted out of it
    /// with [`NodeBuilder::client_log_upload(false)`](crate::NodeBuilder::client_log_upload)
    /// (sidecar protocol 11).
    ///
    /// A tailnet with network flow logs on refuses a node that does not send
    /// Tailscale its client logs: Tailscale stops the node rather than run it
    /// unlogged, and records that it should stay down. While `start()` waits,
    /// it fails right away; a node already running when an admin turns flow
    /// logs on stops. Either way it stays down until the node is built again:
    /// with uploads allowed, or after the tailnet's flow logs are turned off.
    /// Tell the user which.
    LoggingRequired,
    /// Tailnet Lock is on and this node's key is not signed, so the node is
    /// locked out (sidecar protocol 11).
    ///
    /// Unlike [`NeedsApproval`](Self::NeedsApproval), a locked-out node does
    /// come up — `start()` returns — but it reaches no peer, and no peer
    /// reaches it, until a signing node signs its key (`tailscale lock sign`).
    /// Raised before `start()` returns when the node starts unsigned, and
    /// whenever it becomes unsigned later. [`Signed`](Self::Signed) follows
    /// once it is signed.
    NeedsSignature,
    /// A node that [needed a signature](Self::NeedsSignature) is no longer
    /// locked out: a signing node signed it, or Tailnet Lock was turned off
    /// (sidecar protocol 11).
    Signed,
}

/// A Tailscale backend state (Tailscale's `ipn.State`), as
/// [`NetworkStatusEvent::BackendStateChanged`] reports it.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum BackendState {
    /// The backend has not started.
    NoState,
    /// The node must log in: its key expired, or it was logged out or
    /// removed in the admin console.
    NeedsLogin,
    /// The node waits for an admin to approve it (device approval).
    NeedsMachineAuth,
    /// The node is stopped. A node that opted out of client logging on a
    /// tailnet that requires them is stopped (see
    /// [`NetworkStatusEvent::LoggingRequired`]).
    Stopped,
    /// The node is connecting.
    Starting,
    /// The node is on the tailnet.
    Running,
    /// A state this version of truffle does not know, as Tailscale named it.
    Other(String),
}

impl BackendState {
    /// Read Tailscale's name for a state (`ipn.State.String()`).
    pub fn from_wire(state: &str) -> BackendState {
        match state {
            "NoState" => BackendState::NoState,
            "NeedsLogin" => BackendState::NeedsLogin,
            "NeedsMachineAuth" => BackendState::NeedsMachineAuth,
            "Stopped" => BackendState::Stopped,
            "Starting" => BackendState::Starting,
            "Running" => BackendState::Running,
            other => BackendState::Other(other.to_string()),
        }
    }

    /// Tailscale's name for the state.
    pub fn as_str(&self) -> &str {
        match self {
            BackendState::NoState => "NoState",
            BackendState::NeedsLogin => "NeedsLogin",
            BackendState::NeedsMachineAuth => "NeedsMachineAuth",
            BackendState::Stopped => "Stopped",
            BackendState::Starting => "Starting",
            BackendState::Running => "Running",
            BackendState::Other(other) => other,
        }
    }
}

/// The Tailscale client a node's sidecar embeds (sidecar protocol 11).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct TailscaleVersion {
    /// The Tailscale release, e.g. `1.104.1`: what to show a person, and
    /// what to compare when Tailscale's admin console says a client is out
    /// of date.
    pub version: String,
    /// The full version string the node reports to its control server, as
    /// the admin console lists it, e.g. `1.104.1-dev20261020-g0123abcd`
    /// (the suffix names the embedding build, not a Tailscale build).
    pub long: String,
}

impl TailscaleVersion {
    /// Read the sidecar's pair; `None` when it did not name a release.
    pub(crate) fn from_wire(version: Option<&str>, long: Option<&str>) -> Option<TailscaleVersion> {
        let version = version.map(str::trim).filter(|v| !v.is_empty())?;
        Some(TailscaleVersion {
            version: version.to_string(),
            long: long.map(str::trim).unwrap_or_default().to_string(),
        })
    }
}

/// When a node's key expires.
///
/// After that moment the node drops off the tailnet until someone logs it in
/// again, so a server that should stay reachable wants to warn its owner
/// ahead of time (or have an admin disable the expiry for it).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum KeyExpiry {
    /// Key expiry is disabled for this node: the admin turned it off, or the
    /// node is tagged (tagged nodes don't expire by default).
    Never,
    /// The key expires (or expired) at this moment.
    At(std::time::SystemTime),
}

impl KeyExpiry {
    /// The expiry moment; `None` for [`KeyExpiry::Never`].
    pub fn at(self) -> Option<std::time::SystemTime> {
        match self {
            KeyExpiry::Never => None,
            KeyExpiry::At(t) => Some(t),
        }
    }

    /// The expiry as milliseconds since the Unix epoch (negative before
    /// 1970), the shape a JSON or JavaScript timestamp takes; `None` for
    /// [`KeyExpiry::Never`].
    pub fn unix_millis(self) -> Option<i64> {
        let t = self.at()?;
        Some(match t.duration_since(std::time::UNIX_EPOCH) {
            Ok(after) => i64::try_from(after.as_millis()).unwrap_or(i64::MAX),
            Err(before) => -i64::try_from(before.duration().as_millis()).unwrap_or(i64::MAX),
        })
    }

    /// Read the sidecar's wire form: an RFC 3339 time, where an absent or
    /// empty value means expiry is disabled. `None` for a value that does not
    /// parse (logged by the caller, never guessed).
    pub(crate) fn from_wire(value: Option<&str>) -> Option<KeyExpiry> {
        match value.map(str::trim).filter(|s| !s.is_empty()) {
            None => Some(KeyExpiry::Never),
            Some(s) => parse_rfc3339(s).map(KeyExpiry::At),
        }
    }
}

/// The coordination ("control") server a node logs in to and receives its
/// network map from.
///
/// Tailscale's hosted service has an admin console where devices are
/// approved and their key expiry managed; a self-hosted server such as
/// Headscale has none (it is managed from its own CLI), so a UI should only
/// link to the console for [`ControlServer::Tailscale`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ControlServer {
    /// Tailscale's hosted service (`https://controlplane.tailscale.com`,
    /// also reachable as `https://login.tailscale.com`).
    Tailscale,
    /// Any other control server, such as Headscale, by URL.
    Custom {
        /// The control URL as configured, e.g. `https://hs.example.com`.
        url: String,
    },
}

impl ControlServer {
    /// Tailscale's default control URL (tailscale's `ipn.DefaultControlURL`).
    pub const TAILSCALE_URL: &'static str = "https://controlplane.tailscale.com";

    /// Classify a control URL the way Tailscale does
    /// (`ipn.IsLoginServerSynonym`): `https://controlplane.tailscale.com` and
    /// `https://login.tailscale.com` are the hosted service, and so is an
    /// empty URL (tsnet's default). The comparison also tolerates letter case
    /// and a trailing slash. Anything else is [`ControlServer::Custom`].
    pub fn from_url(url: &str) -> ControlServer {
        let trimmed = url.trim();
        let canonical = trimmed.trim_end_matches('/').to_ascii_lowercase();
        match canonical.as_str() {
            "" | "https://controlplane.tailscale.com" | "https://login.tailscale.com" => {
                ControlServer::Tailscale
            }
            _ => ControlServer::Custom {
                url: trimmed.to_string(),
            },
        }
    }

    /// True for Tailscale's hosted service.
    pub fn is_tailscale(&self) -> bool {
        matches!(self, ControlServer::Tailscale)
    }

    /// The control URL ([`Self::TAILSCALE_URL`] for the hosted service).
    pub fn url(&self) -> &str {
        match self {
            ControlServer::Tailscale => Self::TAILSCALE_URL,
            ControlServer::Custom { url } => url,
        }
    }

    /// The admin console's Machines page, where an admin approves a device
    /// waiting for approval and manages its key expiry. `None` for a custom
    /// control server, which has no such page.
    pub fn machines_admin_url(&self) -> Option<&'static str> {
        match self {
            ControlServer::Tailscale => Some("https://login.tailscale.com/admin/machines"),
            ControlServer::Custom { .. } => None,
        }
    }
}

/// Parse an RFC 3339 timestamp (`2026-06-24T10:00:00Z`,
/// `2026-06-24T12:00:00.5+02:00`) — the format Go's `time.RFC3339` writes.
/// Returns `None` for anything else, including impossible dates, rather than
/// guessing.
pub(crate) fn parse_rfc3339(s: &str) -> Option<std::time::SystemTime> {
    fn num(b: &[u8]) -> Option<i64> {
        if b.is_empty() || !b.iter().all(u8::is_ascii_digit) {
            return None;
        }
        b.iter().try_fold(0i64, |acc, d| {
            acc.checked_mul(10)?.checked_add(i64::from(d - b'0'))
        })
    }
    let b = s.as_bytes();
    if b.len() < 20
        || b[4] != b'-'
        || b[7] != b'-'
        || !matches!(b[10], b'T' | b't' | b' ')
        || b[13] != b':'
        || b[16] != b':'
    {
        return None;
    }
    let (year, month, day) = (num(&b[0..4])?, num(&b[5..7])?, num(&b[8..10])?);
    let (hour, min, sec) = (num(&b[11..13])?, num(&b[14..16])?, num(&b[17..19])?);
    let leap = (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
    let month_days = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap => 29,
        2 => 28,
        _ => return None,
    };
    if !(1..=month_days).contains(&day) || hour > 23 || min > 59 || sec > 60 {
        return None;
    }
    // Optional fraction, then Z or ±HH:MM.
    let mut rest = &b[19..];
    let mut nanos: u32 = 0;
    if rest.first() == Some(&b'.') {
        let digits = rest[1..].iter().take_while(|c| c.is_ascii_digit()).count();
        if digits == 0 {
            return None;
        }
        for (i, d) in rest[1..=digits].iter().enumerate().take(9) {
            nanos += u32::from(d - b'0') * 10u32.pow(8 - i as u32);
        }
        rest = &rest[1 + digits..];
    }
    let offset_secs = match rest {
        [b'Z' | b'z'] => 0,
        [sign @ (b'+' | b'-'), h1, h2, b':', m1, m2] => {
            let (oh, om) = (num(&[*h1, *h2])?, num(&[*m1, *m2])?);
            if oh > 23 || om > 59 {
                return None;
            }
            let off = oh * 3600 + om * 60;
            if *sign == b'+' {
                off
            } else {
                -off
            }
        }
        _ => return None,
    };
    // Days since 1970-01-01 of a proleptic Gregorian date (Hinnant's
    // days_from_civil).
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (month + 9) % 12;
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    // A leap second (:60) reads as :59.
    let secs = days * 86_400 + hour * 3600 + min * 60 + sec.min(59) - offset_secs;
    let epoch = std::time::UNIX_EPOCH;
    if secs >= 0 {
        epoch.checked_add(Duration::new(secs.unsigned_abs(), nanos))
    } else {
        epoch
            .checked_sub(Duration::from_secs(secs.unsigned_abs()))?
            .checked_add(Duration::from_nanos(u64::from(nanos)))
    }
}

/// Network address of a peer.
#[derive(Debug, Clone, Default)]
pub struct PeerAddr {
    /// IP address (100.x.x.x for Tailscale).
    pub ip: Option<IpAddr>,
    /// Hostname on the network.
    pub hostname: String,
    /// DNS name on the tailnet.
    pub dns_name: Option<String>,
}

/// Identity of the local node on the network.
///
/// Carries the RFC 017 identity triple: `app_id` (namespace), `device_id`
/// (stable per-device ULID), and `device_name` (human-readable). The
/// Tailscale hostname and stable ID are kept alongside as escape hatches
/// and for internal filtering.
#[derive(Debug, Clone, Default)]
pub struct NodeIdentity {
    /// Application namespace identifier (RFC 017 §5.1).
    pub app_id: String,
    /// Stable per-device ULID (RFC 017 §5.4).
    pub device_id: String,
    /// Human-readable device name, original (unsanitised) string.
    pub device_name: String,
    /// Tailscale hostname — `truffle-{app_id}-{slug(device_name)}`.
    pub tailscale_hostname: String,
    /// Tailscale stable node ID (populated after the sidecar reaches Running).
    pub tailscale_id: String,
    /// DNS name on the tailnet.
    pub dns_name: Option<String>,
    /// Tailscale IP address.
    pub ip: Option<IpAddr>,
    /// The node's own Tailscale login (owner) name, e.g. "alice@example.com"
    /// (RFC 025 §3.6). `None` until the sidecar reports it, or when it cannot.
    pub login_name: Option<String>,
}

/// Result of a network-level ping.
#[derive(Debug, Clone)]
pub struct PingResult {
    /// Round-trip latency.
    pub latency: Duration,
    /// Connection type description (e.g., "direct" or "relay:sfo").
    pub connection: String,
    /// Direct peer endpoint address, if available.
    pub peer_addr: Option<String>,
}

/// Health information from the network provider.
#[derive(Debug, Clone, Default)]
pub struct HealthInfo {
    /// Current backend state (e.g., "Running", "NeedsLogin").
    pub state: String,
    /// The node's key expiry (RFC 3339). Sidecars since protocol 10 report
    /// it as soon as the node runs (`None` then means expiry is disabled;
    /// [`KeyExpiry`] says so explicitly); older ones only within 24 hours of
    /// the expiry.
    pub key_expiry: Option<String>,
    /// Active health warnings.
    pub warnings: Vec<String>,
    /// Whether the network is fully operational.
    pub healthy: bool,
}

/// A listener for incoming TCP connections via the network provider.
///
/// Wraps a channel that receives `TcpStream`s from the bridge. The bridge
/// internals (binary headers, session tokens) are completely hidden.
pub struct NetworkTcpListener {
    /// Port this listener is bound to.
    pub port: u16,
    /// Receiver for incoming connections.
    pub incoming: tokio::sync::mpsc::Receiver<IncomingConnection>,
}

/// An incoming TCP connection with metadata.
#[derive(Debug)]
pub struct IncomingConnection {
    /// The raw TCP stream (bridge headers already consumed).
    pub stream: TcpStream,
    /// Remote address of the connecting peer.
    pub remote_addr: String,
    /// Remote DNS name or peer identity JSON.
    pub remote_identity: String,
    /// Port the connection arrived on.
    pub port: u16,
}

/// A remote peer's Tailscale-authenticated identity, parsed from the WhoIs
/// JSON the sidecar attaches to inbound bridge connections (the
/// [`remote_identity`](IncomingConnection::remote_identity) field of
/// [`IncomingConnection`]).
///
/// Produced by Layer 3 — the Go sidecar's `resolvePeerIdentity` writes this
/// JSON into the bridge header — and consumed by Layer 4+ transports and the
/// bindings. Every field is optional: the sidecar omits empty ones, WhoIs may
/// return no Node, and legacy sidecars send a bare DNS name that does not
/// parse as this struct at all. Sidecars before protocol 9 never send
/// `user_id`, `tags` or `cap_map`, so those read as absent/empty.
#[derive(Debug, Clone, Default, PartialEq, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TailscalePeerIdentity {
    /// Tailnet DNS name (e.g., "kitchen.tailnet.ts.net"), trailing dot stripped.
    pub dns_name: Option<String>,
    /// Tailscale login (owner) name, e.g., "alice@example.com".
    pub login_name: Option<String>,
    /// Human-readable display name from the identity provider.
    pub display_name: Option<String>,
    /// URL of the peer owner's profile picture.
    pub profile_pic_url: Option<String>,
    /// Stable Tailscale node ID (WhoIs `Node.StableID`).
    pub node_id: Option<String>,
    /// Numeric Tailscale user ID of the owner (WhoIs `UserProfile.ID`).
    /// Unlike the login name it survives a rename. Sidecar protocol 9+.
    #[serde(default)]
    pub user_id: Option<i64>,
    /// ACL tags of the peer node (e.g. `["tag:tv"]`). A tagged node has no
    /// human owner — its login is Tailscale's `tagged-devices` pseudo-user —
    /// so tags and `node_id` are what distinguish shared devices. Sidecar
    /// protocol 9+.
    #[serde(default)]
    pub tags: Vec<String>,
    /// Application capabilities the tailnet policy grants this peer on THIS
    /// node (WhoIs `CapMap`: the `app` section of policy grants, see
    /// <https://tailscale.com/kb/1537/grants-app-capabilities>), keyed by
    /// capability name (`example.com/cap/media`), one JSON value per matching
    /// grant. Empty when no grant applies, on sidecars before protocol 9, or
    /// when the sidecar had to drop it — see
    /// [`cap_map_dropped`](Self::cap_map_dropped).
    #[serde(default)]
    pub cap_map: HashMap<String, Vec<serde_json::Value>>,
    /// The sidecar dropped `cap_map` because it did not fit the bridge
    /// header (> 16 KiB). An app deriving permissions from capabilities must
    /// treat this as "unknown", not as "no capabilities granted".
    #[serde(default)]
    pub cap_map_dropped: bool,
}

impl TailscalePeerIdentity {
    /// Map present-but-empty fields to `None`. The wire contract says empty
    /// fields are omitted, but the "absent, not fabricated" guarantee must
    /// not depend on the peer's serializer honoring that.
    pub(crate) fn normalized(mut self) -> Self {
        fn drop_empty(field: &mut Option<String>) {
            if field.as_deref().is_some_and(str::is_empty) {
                *field = None;
            }
        }
        drop_empty(&mut self.dns_name);
        drop_empty(&mut self.login_name);
        drop_empty(&mut self.display_name);
        drop_empty(&mut self.profile_pic_url);
        drop_empty(&mut self.node_id);
        if self.user_id == Some(0) {
            self.user_id = None;
        }
        self.tags.retain(|tag| !tag.is_empty());
        self
    }

    /// True when no field carries any information.
    pub(crate) fn is_empty(&self) -> bool {
        self.dns_name.is_none()
            && self.login_name.is_none()
            && self.display_name.is_none()
            && self.profile_pic_url.is_none()
            && self.node_id.is_none()
            && self.user_id.is_none()
            && self.tags.is_empty()
            && self.cap_map.is_empty()
            && !self.cap_map_dropped
    }

    /// True when the peer node is tag-owned (no human user behind it).
    pub fn is_tagged(&self) -> bool {
        !self.tags.is_empty()
    }

    /// The raw grant values for application capability `name`, if the
    /// tailnet policy grants it to this peer.
    pub fn capability_values(&self, name: &str) -> Option<&[serde_json::Value]> {
        self.cap_map.get(name).map(Vec::as_slice)
    }

    /// Decode every grant value of application capability `name` as `T`
    /// (the Rust counterpart of Go's `tailcfg.UnmarshalCapJSON`). An absent
    /// capability yields an empty vector; a value that does not decode as
    /// `T` is an error, so a malformed policy fails closed instead of being
    /// skipped silently.
    pub fn capability<T: serde::de::DeserializeOwned>(
        &self,
        name: &str,
    ) -> Result<Vec<T>, serde_json::Error> {
        self.capability_values(name)
            .unwrap_or_default()
            .iter()
            .map(|value| T::deserialize(value))
            .collect()
    }
}

// ---------------------------------------------------------------------------
// NetworkUdpSocket — address-framed UDP relay wrapper
// ---------------------------------------------------------------------------

/// A UDP socket that relays datagrams through the network provider.
///
/// Under the hood, the Rust side talks to a local relay socket. Each outbound
/// datagram is prefixed with a 6-byte address header (`[4-byte IPv4][2-byte port BE]`)
/// so the relay (Go sidecar) knows where to forward the packet on the tsnet
/// network. Inbound datagrams arrive with the same header prepended by the relay.
///
/// This struct hides the framing — callers use `send_to` / `recv_from` with
/// normal `SocketAddr` values.
pub struct NetworkUdpSocket {
    /// The underlying tokio UDP socket connected to the local relay.
    inner: tokio::net::UdpSocket,
    /// The tsnet-bound port (the logical port on the Tailscale network).
    tsnet_port: u16,
    on_drop: Option<Box<dyn FnOnce() + Send + Sync>>,
}

/// Address header size: 4 bytes IPv4 + 2 bytes port (big-endian).
const UDP_ADDR_HEADER_SIZE: usize = 6;

impl NetworkUdpSocket {
    /// Create a new `NetworkUdpSocket` from a tokio UdpSocket and the tsnet port.
    pub(crate) fn new(inner: tokio::net::UdpSocket, tsnet_port: u16) -> Self {
        Self {
            inner,
            tsnet_port,
            on_drop: None,
        }
    }

    pub(crate) fn with_cleanup(mut self, cleanup: impl FnOnce() + Send + Sync + 'static) -> Self {
        self.on_drop = Some(Box::new(cleanup));
        self
    }

    pub(crate) fn set_tsnet_port(&mut self, port: u16) {
        self.tsnet_port = port;
    }

    /// Send a datagram to the specified address via the relay.
    ///
    /// The relay will forward the datagram to the target on the tsnet network.
    pub async fn send_to(&self, data: &[u8], addr: SocketAddr) -> Result<usize, NetworkError> {
        let ip = match addr.ip() {
            IpAddr::V4(v4) => v4,
            IpAddr::V6(_) => {
                return Err(NetworkError::Internal(
                    "NetworkUdpSocket: IPv6 not supported in relay framing".into(),
                ));
            }
        };
        let port = addr.port();

        // Build framed packet: [4-byte IPv4][2-byte port BE][payload]
        let mut framed = Vec::with_capacity(UDP_ADDR_HEADER_SIZE + data.len());
        framed.extend_from_slice(&ip.octets());
        framed.extend_from_slice(&port.to_be_bytes());
        framed.extend_from_slice(data);

        tracing::debug!(
            target_addr = %addr,
            payload_len = data.len(),
            framed_len = framed.len(),
            relay_addr = ?self.inner.peer_addr().ok(),
            "NetworkUdpSocket: sending framed datagram to relay"
        );

        let n = self.inner.send(&framed).await.map_err(NetworkError::Io)?;

        tracing::debug!(
            bytes_sent = n,
            "NetworkUdpSocket: framed datagram sent to relay"
        );

        // Return the number of payload bytes sent (subtract header)
        Ok(n.saturating_sub(UDP_ADDR_HEADER_SIZE))
    }

    /// Receive a datagram from the relay, returning the payload and sender address.
    ///
    /// The relay prepends a 6-byte address header to each inbound datagram.
    pub async fn recv_from(&self, buf: &mut [u8]) -> Result<(usize, SocketAddr), NetworkError> {
        tracing::debug!("NetworkUdpSocket: waiting for inbound datagram from relay...");

        // Read into a temporary buffer that includes space for the header
        let mut tmp = vec![0u8; UDP_ADDR_HEADER_SIZE + buf.len()];
        let n = self.inner.recv(&mut tmp).await.map_err(NetworkError::Io)?;

        if n < UDP_ADDR_HEADER_SIZE {
            return Err(NetworkError::Internal(
                "NetworkUdpSocket: received packet too short for address header".into(),
            ));
        }

        // Parse address header
        let ip = Ipv4Addr::new(tmp[0], tmp[1], tmp[2], tmp[3]);
        let port = u16::from_be_bytes([tmp[4], tmp[5]]);
        let addr = SocketAddr::new(IpAddr::V4(ip), port);

        // Copy payload to caller's buffer
        let payload_len = n - UDP_ADDR_HEADER_SIZE;
        buf[..payload_len].copy_from_slice(&tmp[UDP_ADDR_HEADER_SIZE..n]);

        tracing::debug!(
            raw_bytes = n,
            payload_len = payload_len,
            sender_addr = %addr,
            "NetworkUdpSocket: received inbound datagram from relay"
        );

        Ok((payload_len, addr))
    }

    /// Return the local address of the underlying relay socket.
    pub fn local_addr(&self) -> Result<SocketAddr, NetworkError> {
        self.inner.local_addr().map_err(NetworkError::Io)
    }

    /// Return the tsnet-bound port (the logical port on the Tailscale network).
    pub fn tsnet_port(&self) -> u16 {
        self.tsnet_port
    }

    /// Get a reference to the inner tokio UdpSocket.
    ///
    /// This is the socket connected to the local relay. Direct reads/writes
    /// bypass the address framing — prefer `send_to` / `recv_from` instead.
    pub fn inner(&self) -> &tokio::net::UdpSocket {
        &self.inner
    }
}

impl Drop for NetworkUdpSocket {
    fn drop(&mut self) {
        if let Some(cleanup) = self.on_drop.take() {
            cleanup();
        }
    }
}

impl std::fmt::Debug for NetworkUdpSocket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NetworkUdpSocket")
            .field("tsnet_port", &self.tsnet_port)
            .field("local_addr", &self.inner.local_addr().ok())
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Reverse proxy types (used by NetworkProvider trait methods)
// ---------------------------------------------------------------------------

/// Parameters for starting a reverse proxy via the network provider.
#[derive(Debug, Clone)]
pub struct ProxyAddParams {
    /// Unique identifier for this proxy.
    pub id: String,
    /// Human-readable name.
    pub name: String,
    /// Port on which the proxy listens on the tailnet.
    pub listen_port: u16,
    /// Target host to forward to (e.g., "localhost").
    pub target_host: String,
    /// Target port to forward to.
    pub target_port: u16,
    /// Target scheme ("http" or "https").
    pub target_scheme: String,
    /// Terminate TLS on the tailnet listener (RFC 023; `true` = the v1
    /// always-TLS behavior, `false` = plain HTTP listener).
    pub tls: bool,
    /// Permit non-loopback targets (RFC 023 §9.3; default deny).
    pub allow_non_loopback: bool,
    /// loginName allow globs; empty = whole tailnet (RFC 023 §9.7).
    pub allow: Vec<String>,
    /// Path-prefix routes; empty = the single-target v1 shape.
    pub routes: Vec<ProxyRoute>,
    /// Application-capability names forwarded to the backend in the
    /// `Tailscale-App-Capabilities` header (sidecar protocol 9).
    pub accept_app_capabilities: Vec<String>,
}

/// One path-prefix route of a v2 proxy (RFC 023 §7). Wire-shaped: exactly
/// one of `target_url` / `dir` must be set. Longest prefix wins (D11).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProxyRoute {
    /// Path prefix to match (must start with `/`).
    pub prefix: String,
    /// Proxy target URL (e.g. `http://localhost:8000`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_url: Option<String>,
    /// Static directory to serve (absolute path on the serving machine).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dir: Option<String>,
    /// SPA fallback rewritten on static misses (e.g. `/index.html`);
    /// only meaningful with `dir`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fallback: Option<String>,
    /// Strip the matched prefix before proxying (default false — D11;
    /// only meaningful with `target_url`).
    #[serde(default)]
    pub strip_prefix: bool,
    /// Per-route loginName globs; overrides the config-level `allow`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allow: Vec<String>,
}

/// A runtime error from the provider's proxy engine after a successful add
/// (RFC 023 G5 fix — e.g. sidecar `SERVE_ERROR` / `CONNECTION_REFUSED`).
#[derive(Debug, Clone)]
pub struct ProxyRuntimeError {
    /// Proxy id the error belongs to.
    pub id: String,
    /// Machine-readable error code.
    pub code: String,
    /// Human-readable detail.
    pub message: String,
}

/// Result of successfully starting a reverse proxy.
#[derive(Debug, Clone)]
pub struct ProxyAddResult {
    /// Proxy ID (echoed back from sidecar).
    pub id: String,
    /// Actual listen port (may differ from requested if 0 was passed).
    pub listen_port: u16,
    /// Fully qualified URL (e.g., "<https://hostname.ts.net:3001>").
    pub url: String,
}

/// Entry in the proxy list from the network provider.
#[derive(Debug, Clone)]
pub struct ProxyListEntry {
    pub id: String,
    pub name: String,
    pub listen_port: u16,
    pub target_host: String,
    pub target_port: u16,
    pub target_scheme: String,
    pub url: String,
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors from Layer 3 network operations.
#[derive(Debug, thiserror::Error)]
pub enum NetworkError {
    /// The network provider is not running.
    #[error("network provider not running")]
    NotRunning,

    /// The network provider is already running.
    #[error("network provider already running")]
    AlreadyRunning,

    /// Failed to start the network provider.
    #[error("start failed: {0}")]
    StartFailed(String),

    /// Failed to stop the network provider.
    #[error("stop failed: {0}")]
    StopFailed(String),

    /// Authentication is required.
    #[error("authentication required: {url}")]
    AuthRequired { url: String },

    /// A dial operation failed.
    #[error("dial failed: {0}")]
    DialFailed(String),

    /// A dial operation timed out.
    #[error("dial timed out after {0:?}")]
    DialTimeout(Duration),

    /// A listen operation failed.
    #[error("listen failed: {0}")]
    ListenFailed(String),

    /// A ping operation failed.
    #[error("ping failed: {0}")]
    PingFailed(String),

    /// The sidecar process crashed or is unavailable.
    #[error("sidecar error: {0}")]
    SidecarError(String),

    /// Bridge communication error.
    #[error("bridge error: {0}")]
    BridgeError(String),

    /// I/O error.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// Serialization error.
    #[error("serialization error: {0}")]
    Serialize(#[from] serde_json::Error),

    /// Generic internal error.
    #[error("internal error: {0}")]
    Internal(String),

    /// The operation is not supported by this provider.
    #[error("unsupported: {0}")]
    Unsupported(String),

    /// A reverse-proxy operation failed.
    #[error("proxy error: {0}")]
    ProxyError(String),

    /// An egress operation was refused or failed (RFC 026). `code` is the
    /// sidecar's stable reason — `TARGET_NOT_PEER`, `LOGIN_REFUSED`,
    /// `LOGIN_UNKNOWN`, `INVALID_TARGET`, `EGRESS_EXISTS`, `NOT_FOUND`, … —
    /// and `message` says why for a human.
    #[error("egress error [{code}]: {message}")]
    EgressError {
        /// The sidecar's stable failure code.
        code: String,
        /// Human-readable detail.
        message: String,
    },
}

#[cfg(test)]
mod status_type_tests {
    use super::*;
    use std::time::{Duration, UNIX_EPOCH};

    #[test]
    fn rfc3339_parses_what_go_writes() {
        // Go's time.RFC3339 of a UTC time, as the sidecar sends it.
        let t = parse_rfc3339("2026-06-24T10:00:00Z").unwrap();
        assert_eq!(t, UNIX_EPOCH + Duration::from_secs(1_782_295_200));
        assert_eq!(parse_rfc3339("1970-01-01T00:00:00Z"), Some(UNIX_EPOCH));
        // Offsets, fractions and a leap day.
        assert_eq!(parse_rfc3339("2026-06-24T12:00:00+02:00"), Some(t));
        assert_eq!(parse_rfc3339("2026-06-24T05:30:00-04:30"), Some(t));
        assert_eq!(
            parse_rfc3339("2026-06-24T10:00:00.25Z"),
            Some(t + Duration::from_millis(250))
        );
        assert_eq!(
            parse_rfc3339("2028-02-29T00:00:00Z"),
            Some(UNIX_EPOCH + Duration::from_secs(1_835_395_200))
        );
        assert_eq!(
            parse_rfc3339("1969-12-31T23:59:59Z"),
            UNIX_EPOCH.checked_sub(Duration::from_secs(1))
        );
    }

    #[test]
    fn rfc3339_rejects_rather_than_guesses() {
        for bad in [
            "",
            "2026-06-24",
            "2026-06-24T10:00:00",        // no zone
            "2026-06-24T10:00:00+0200",   // offset without colon
            "2026-02-30T10:00:00Z",       // no such day
            "2027-02-29T10:00:00Z",       // not a leap year
            "2026-13-01T10:00:00Z",       // no such month
            "2026-06-24T24:00:00Z",       // no such hour
            "2026-06-24T10:00:00.Z",      // empty fraction
            "2026-06-24T10:00:00Zjunk",   // trailing bytes
            "twenty-six-06-24T10:00:00Z", // not digits
        ] {
            assert_eq!(parse_rfc3339(bad), None, "{bad:?} must not parse");
        }
    }

    #[test]
    fn key_expiry_wire_form_and_millis() {
        assert_eq!(KeyExpiry::from_wire(None), Some(KeyExpiry::Never));
        assert_eq!(KeyExpiry::from_wire(Some("")), Some(KeyExpiry::Never));
        assert_eq!(KeyExpiry::from_wire(Some("soon")), None);
        let at = KeyExpiry::from_wire(Some("2026-06-24T10:00:00Z")).unwrap();
        assert_eq!(at.unix_millis(), Some(1_782_295_200_000));
        assert_eq!(
            at.at(),
            Some(UNIX_EPOCH + Duration::from_secs(1_782_295_200))
        );
        assert_eq!(KeyExpiry::Never.unix_millis(), None);
        assert_eq!(KeyExpiry::Never.at(), None);
        let before = KeyExpiry::At(UNIX_EPOCH - Duration::from_millis(1500));
        assert_eq!(before.unix_millis(), Some(-1500));
    }

    #[test]
    fn control_server_classification_matches_tailscale() {
        for hosted in [
            "",
            "https://controlplane.tailscale.com",
            "https://login.tailscale.com",
            "https://controlplane.tailscale.com/",
            "HTTPS://Login.Tailscale.com",
        ] {
            let cs = ControlServer::from_url(hosted);
            assert!(cs.is_tailscale(), "{hosted:?}");
            assert_eq!(cs.url(), ControlServer::TAILSCALE_URL);
            assert_eq!(
                cs.machines_admin_url(),
                Some("https://login.tailscale.com/admin/machines")
            );
        }
        let hs = ControlServer::from_url(" https://hs.example.com ");
        assert_eq!(
            hs,
            ControlServer::Custom {
                url: "https://hs.example.com".into()
            }
        );
        assert!(!hs.is_tailscale());
        assert_eq!(hs.url(), "https://hs.example.com");
        assert_eq!(
            hs.machines_admin_url(),
            None,
            "Headscale has no admin console"
        );
        // Look-alikes are not the hosted service.
        for custom in [
            "http://controlplane.tailscale.com",
            "https://controlplane.tailscale.com.evil.example",
            "https://tailscale.com",
        ] {
            assert!(
                !ControlServer::from_url(custom).is_tailscale(),
                "{custom:?}"
            );
        }
    }
}
