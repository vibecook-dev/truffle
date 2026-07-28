//! Layer 3: Network — Peer discovery, addressing, encrypted tunnels.
//!
//! This module defines the [`NetworkProvider`] trait, the public API for Layer 3.
//! The trait is generic — no Tailscale-specific types leak through.
//!
//! The [`tailscale`] submodule contains the [`TailscaleProvider`](tailscale::TailscaleProvider) implementation
//! that wraps the Go sidecar (tsnet) and bridge.

pub mod tailscale;

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::net::TcpStream;
use tokio::sync::broadcast;

// ---------------------------------------------------------------------------
// NetworkProvider trait — the public API of Layer 3
// ---------------------------------------------------------------------------

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
    /// DERP relay name if connection is relayed.
    pub relay: Option<String>,
    /// Operating system of the peer.
    pub os: Option<String>,
    /// Last time the peer was seen online (RFC 3339 string).
    pub last_seen: Option<String>,
    /// Key expiry timestamp (RFC 3339 string).
    pub key_expiry: Option<String>,
    /// DNS name on the tailnet (e.g., "truffle-cli-abc123.tailnet.ts.net").
    pub dns_name: Option<String>,
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
    /// May be emitted multiple times if the URL expires and is refreshed.
    AuthRequired {
        /// URL the user should open in a browser.
        url: String,
    },
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
    /// Key expiry timestamp (RFC 3339), if applicable.
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
/// parse as this struct at all.
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
        self
    }

    /// True when no field carries any information.
    pub(crate) fn is_empty(&self) -> bool {
        self.dns_name.is_none()
            && self.login_name.is_none()
            && self.display_name.is_none()
            && self.profile_pic_url.is_none()
            && self.node_id.is_none()
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
}

/// Address header size: 4 bytes IPv4 + 2 bytes port (big-endian).
const UDP_ADDR_HEADER_SIZE: usize = 6;

impl NetworkUdpSocket {
    /// Create a new `NetworkUdpSocket` from a tokio UdpSocket and the tsnet port.
    pub(crate) fn new(inner: tokio::net::UdpSocket, tsnet_port: u16) -> Self {
        Self { inner, tsnet_port }
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
}
