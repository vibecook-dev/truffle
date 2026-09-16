//! Layer 4: Transport — Protocol-specific connection management.
//!
//! This module defines three transport trait families:
//!
//! - [`StreamTransport`] + [`FramedStream`]: Persistent, framed, bidirectional
//!   connections used for messaging, pub/sub, and signaling. Implemented by
//!   [`WebSocketTransport`](websocket::WebSocketTransport) and [`QuicTransport`](quic::QuicTransport).
//!
//! - [`RawTransport`] + [`RawListener`]: Raw byte streams used for file transfer,
//!   TCP proxy, and HTTP. Implemented by [`TcpTransport`](tcp::TcpTransport).
//!
//! - [`DatagramTransport`] + [`DatagramSocket`]: Unreliable datagrams for
//!   real-time video/audio and game state. Implemented by [`UdpTransport`](udp::UdpTransport).
//!
//! # Layer rules
//!
//! - Layer 4 does NOT know about peers, sessions, or envelopes
//! - Layer 4 works with [`PeerAddr`] (IP + hostname),
//!   not peer IDs
//! - All transports delegate raw networking to Layer 3's [`NetworkProvider`](crate::network::NetworkProvider)
//! - The [`FramedStream`] trait is what Layer 5 (Session) will consume

pub mod quic;
pub mod quic_socket;
pub mod tcp;
pub mod udp;
pub mod websocket;

#[cfg(test)]
mod tests;

use std::time::Duration;

use tokio::net::TcpStream;

use crate::network::PeerAddr;

// ---------------------------------------------------------------------------
// Transport traits
// ---------------------------------------------------------------------------

/// A persistent, framed, bidirectional connection.
///
/// Used for: messaging, pub/sub, signaling.
/// Implementations: WebSocket, QUIC streams (future).
///
/// The transport takes an `Arc<dyn NetworkProvider>` at construction time and
/// uses Layer 3 to establish raw TCP connections before performing protocol
/// upgrades (e.g., WebSocket handshake).
#[allow(async_fn_in_trait)]
pub trait StreamTransport: Send + Sync {
    /// The framed stream type produced by this transport.
    type Stream: FramedStream;

    /// Connect to a peer address and return a framed bidirectional stream.
    ///
    /// The implementation should:
    /// 1. Dial the peer via Layer 3 (`NetworkProvider::dial_tcp`)
    /// 2. Perform any protocol upgrade (e.g., WebSocket handshake)
    /// 3. Exchange a transport-level handshake message
    /// 4. Return the resulting framed stream
    async fn connect(&self, addr: &PeerAddr) -> Result<Self::Stream, TransportError>;

    /// Start listening for incoming framed connections.
    ///
    /// Returns a [`StreamListener`] that yields framed streams as peers connect.
    async fn listen(&self) -> Result<StreamListener<Self::Stream>, TransportError>;
}

/// A framed bidirectional stream (message-oriented, not byte-oriented).
///
/// Each `send()` call transmits one discrete message; each `recv()` call
/// returns one discrete message. The transport handles framing internally
/// (e.g., WebSocket frames, length-prefixed frames).
///
/// This is the primary interface that Layer 5 (Session) uses to exchange
/// data with peers.
#[allow(async_fn_in_trait)]
pub trait FramedStream: Send + Sync + 'static {
    /// Send a binary message to the peer.
    async fn send(&mut self, data: &[u8]) -> Result<(), TransportError>;

    /// Receive the next binary message from the peer.
    ///
    /// Returns `Ok(None)` when the stream is cleanly closed by the remote side.
    async fn recv(&mut self) -> Result<Option<Vec<u8>>, TransportError>;

    /// Gracefully close the stream.
    async fn close(&mut self) -> Result<(), TransportError>;

    /// The remote peer's address as a human-readable string.
    fn peer_addr(&self) -> String;
}

/// A raw byte stream transport (not framed).
///
/// Used for: file transfer, TCP proxy, HTTP forwarding.
/// Implementations: TCP, QUIC unidirectional streams (future).
///
/// Returns plain `TcpStream`s that Layer 7 applications use directly
/// for byte-oriented I/O.
#[allow(async_fn_in_trait)]
pub trait RawTransport: Send + Sync {
    /// Open a raw byte stream to a peer on the given port.
    ///
    /// Delegates to `NetworkProvider::dial_tcp` and returns the resulting
    /// `TcpStream` without any protocol upgrade.
    async fn open(&self, addr: &PeerAddr, port: u16) -> Result<TcpStream, TransportError>;

    /// Listen for incoming raw byte streams on a port.
    ///
    /// Returns a [`RawListener`] that yields `TcpStream`s as peers connect.
    async fn listen(&self, port: u16) -> Result<RawListener, TransportError>;
}

/// Unreliable datagram transport.
///
/// Used for: real-time video/audio, game state synchronization.
/// Implementations: UDP, QUIC datagrams (future).
///
/// When backed by a [`NetworkProvider`](crate::network::NetworkProvider) that
/// supports UDP (e.g., `TailscaleProvider`), the transport delegates to
/// `NetworkProvider::bind_udp` and wraps the returned [`NetworkUdpSocket`](crate::network::NetworkUdpSocket)
/// in a [`DatagramSocket`].
#[allow(async_fn_in_trait)]
pub trait DatagramTransport: Send + Sync {
    /// Bind a datagram socket to a peer address.
    ///
    /// Returns a [`DatagramSocket`] that can send and receive datagrams.
    async fn bind(&self, port: u16) -> Result<DatagramSocket, TransportError>;
}

// ---------------------------------------------------------------------------
// Supporting types
// ---------------------------------------------------------------------------

/// A listener that yields framed streams as peers connect.
///
/// Wraps a `tokio::sync::mpsc::Receiver` internally. Call [`accept()`](Self::accept)
/// in a loop to handle incoming connections.
pub struct StreamListener<S: FramedStream> {
    /// Receiver for incoming framed streams.
    rx: tokio::sync::mpsc::Receiver<S>,
    /// Port this listener is bound to.
    pub port: u16,
}

impl<S: FramedStream> StreamListener<S> {
    /// Create a new stream listener from a receiver and port.
    pub fn new(rx: tokio::sync::mpsc::Receiver<S>, port: u16) -> Self {
        Self { rx, port }
    }

    /// Accept the next incoming framed stream.
    ///
    /// Returns `None` when the listener has been shut down.
    pub async fn accept(&mut self) -> Option<S> {
        self.rx.recv().await
    }
}

/// A listener that yields raw TCP streams as peers connect.
///
/// Wraps a `tokio::sync::mpsc::Receiver` internally. Call [`accept()`](Self::accept)
/// in a loop to handle incoming connections.
#[derive(Debug)]
pub struct RawListener {
    /// Receiver for incoming connections.
    rx: tokio::sync::mpsc::Receiver<RawIncoming>,
    /// Port this listener is bound to.
    pub port: u16,
}

impl RawListener {
    /// Create a new raw listener from a receiver and port.
    pub fn new(rx: tokio::sync::mpsc::Receiver<RawIncoming>, port: u16) -> Self {
        Self { rx, port }
    }

    /// Accept the next incoming raw connection.
    ///
    /// Returns `None` when the listener has been shut down.
    pub async fn accept(&mut self) -> Option<RawIncoming> {
        self.rx.recv().await
    }
}

/// An incoming raw TCP connection with metadata.
pub struct RawIncoming {
    /// The raw TCP stream.
    pub stream: TcpStream,
    /// Remote address of the connecting peer.
    pub remote_addr: String,
    /// The connecting peer's Tailscale-authenticated identity (WhoIs), parsed
    /// from the bridge header. `None` when the network provider supplies no
    /// identity (e.g., the mock provider) or the header value did not parse as
    /// identity JSON (a legacy bare DNS name).
    pub remote_identity: Option<crate::network::TailscalePeerIdentity>,
}

/// A datagram socket for sending and receiving unreliable packets.
///
/// Supports two modes:
/// - **Direct**: Wraps a `tokio::net::UdpSocket` for loopback/LAN use.
/// - **Network**: Wraps a [`NetworkUdpSocket`](crate::network::NetworkUdpSocket)
///   for Tailscale relay-based UDP.
pub enum DatagramSocket {
    /// A direct UDP socket (loopback / LAN).
    Direct {
        /// The underlying UDP socket.
        socket: tokio::net::UdpSocket,
    },
    /// A network-relayed UDP socket (Tailscale tsnet).
    Network {
        /// The network UDP socket with address-framed relay.
        socket: crate::network::NetworkUdpSocket,
    },
}

impl DatagramSocket {
    /// Create a direct datagram socket from a tokio UdpSocket.
    pub fn direct(socket: tokio::net::UdpSocket) -> Self {
        Self::Direct { socket }
    }

    /// Create a network-relayed datagram socket from a NetworkUdpSocket.
    pub fn network(socket: crate::network::NetworkUdpSocket) -> Self {
        Self::Network { socket }
    }

    /// Send a datagram to the specified address.
    pub async fn send_to(&self, data: &[u8], addr: &str) -> Result<usize, TransportError> {
        match self {
            Self::Direct { socket } => socket.send_to(data, addr).await.map_err(TransportError::Io),
            Self::Network { socket } => {
                let sock_addr: std::net::SocketAddr = addr.parse().map_err(|e| {
                    TransportError::ConnectFailed(format!("invalid address '{addr}': {e}"))
                })?;
                socket
                    .send_to(data, sock_addr)
                    .await
                    .map_err(|e| TransportError::Io(std::io::Error::other(e.to_string())))
            }
        }
    }

    /// Receive a datagram, returning the data and sender address.
    pub async fn recv_from(&self, buf: &mut [u8]) -> Result<(usize, String), TransportError> {
        match self {
            Self::Direct { socket } => {
                let (n, addr) = socket.recv_from(buf).await.map_err(TransportError::Io)?;
                Ok((n, addr.to_string()))
            }
            Self::Network { socket } => {
                let (n, addr) = socket
                    .recv_from(buf)
                    .await
                    .map_err(|e| TransportError::Io(std::io::Error::other(e.to_string())))?;
                Ok((n, addr.to_string()))
            }
        }
    }

    /// Return the local address this socket is bound to.
    pub fn local_addr(&self) -> Result<std::net::SocketAddr, TransportError> {
        match self {
            Self::Direct { socket } => socket.local_addr().map_err(TransportError::Io),
            Self::Network { socket } => socket
                .local_addr()
                .map_err(|e| TransportError::Io(std::io::Error::other(e.to_string()))),
        }
    }
}

// ---------------------------------------------------------------------------
// Handshake types (used by WebSocketTransport)
// ---------------------------------------------------------------------------

/// Transport-level handshake message exchanged during connection setup.
///
/// This is a Layer 4 concern — it identifies the transport endpoint,
/// not the application. Layer 5 (Session) will use this to associate
/// the connection with a known peer.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct Handshake {
    /// Identifier of the connecting peer (e.g., Tailscale stable node ID).
    pub peer_id: String,
    /// Capabilities supported by this peer (e.g., ["ws", "binary", "compress"]).
    pub capabilities: Vec<String>,
    /// Protocol version for compatibility negotiation.
    pub protocol_version: u32,
}

/// Current transport protocol version.
pub const PROTOCOL_VERSION: u32 = 1;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

// Re-export transport configurations from submodules.
pub use quic::QuicConfig;
pub use udp::UdpConfig;

/// Configuration for WebSocket transport.
#[derive(Debug, Clone)]
pub struct WsConfig {
    /// Port to listen on for incoming WebSocket connections.
    pub port: u16,
    /// Interval between ping frames sent to the peer.
    pub ping_interval: Duration,
    /// Maximum time to wait for a pong response before considering
    /// the connection dead.
    pub pong_timeout: Duration,
    /// Maximum WebSocket message size in bytes.
    pub max_message_size: usize,
    /// Maximum number of concurrent in-flight incoming handshakes (WS
    /// upgrade + hello exchange). Connections beyond the cap are dropped
    /// before the upgrade; established connections are not counted.
    pub max_pending_handshakes: usize,
    /// Maximum time an incoming connection may take to complete the WS
    /// upgrade + hello exchange before it is dropped. Must exceed
    /// HELLO_TIMEOUT (5s) so hello timeouts keep their specific
    /// classification.
    pub handshake_timeout: Duration,
    /// Login allow-list (RFC 025 §3.4): `loginName` globs of the tailnet
    /// users whose nodes may complete the hello with us. Empty = no gate
    /// (every caller that passes the app and identity checks is admitted,
    /// and a connection with no authenticated identity is admitted
    /// unverified — the v1 behaviour). Non-empty = the accepting side
    /// refuses, BEFORE revealing its own hello, every caller whose
    /// WhoIs login matches no glob (close code 4004) and every caller the
    /// bridge attached no authenticated identity to (close code 4003).
    pub login_allow: Vec<String>,
}

impl Default for WsConfig {
    fn default() -> Self {
        Self {
            port: 9417,
            ping_interval: Duration::from_secs(10),
            pong_timeout: Duration::from_secs(30),
            max_message_size: 16 * 1024 * 1024, // 16 MiB
            max_pending_handshakes: 256,
            handshake_timeout: Duration::from_secs(10),
            login_allow: Vec::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Resolve the best dial address from a [`PeerAddr`].
///
/// Prefers IP address (most reliable for Tailscale), falls back to DNS name,
/// then hostname.
pub(crate) fn resolve_dial_addr(addr: &PeerAddr) -> String {
    if let Some(ip) = &addr.ip {
        ip.to_string()
    } else if let Some(dns) = &addr.dns_name {
        dns.clone()
    } else {
        addr.hostname.clone()
    }
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors from Layer 4 transport operations.
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    /// The requested transport or feature is not implemented.
    #[error("not implemented: {0}")]
    NotImplemented(String),

    /// Failed to connect to the peer.
    #[error("connect failed: {0}")]
    ConnectFailed(String),

    /// Failed to start listening.
    #[error("listen failed: {0}")]
    ListenFailed(String),

    /// The transport-level handshake failed.
    #[error("handshake failed: {0}")]
    HandshakeFailed(String),

    /// Protocol version mismatch between peers.
    #[error("protocol version mismatch: local={local}, remote={remote}")]
    VersionMismatch {
        /// Local protocol version.
        local: u32,
        /// Remote protocol version.
        remote: u32,
    },

    /// The remote peer did not produce a hello envelope within the timeout
    /// (RFC 017 §8).
    #[error("hello timeout: no envelope received within 5s")]
    HelloTimeout,

    /// The remote peer sent a hello envelope that did not parse as a valid
    /// [`HelloEnvelope`](crate::session::HelloEnvelope), or used a version
    /// we do not support (RFC 017 §8, close code 4002).
    #[error("hello protocol error: {0}")]
    HelloMalformed(String),

    /// The remote peer advertised a different `app_id` in its hello envelope
    /// than we did (RFC 017 §8, close code 4001).
    #[error("app mismatch: local={local}, remote={remote}")]
    AppMismatch {
        /// Our local `app_id`.
        local: String,
        /// The `app_id` advertised by the remote peer.
        remote: String,
    },

    /// The hello envelope's claimed `tailscale_id` did not match the
    /// Tailscale-authenticated identity of the connection (RFC 017 §8,
    /// close code 4003).
    #[error("identity mismatch: hello claimed tailscale_id={claimed}, authenticated nodeId={authenticated}")]
    IdentityMismatch {
        /// The `tailscale_id` the hello envelope claimed.
        claimed: String,
        /// The Tailscale-authenticated node ID (WhoIs) of the connection.
        authenticated: String,
    },

    /// This node admits only listed logins (RFC 025 §3.4) and the bridge
    /// attached no authenticated identity to the incoming connection — a
    /// mock provider, a failed WhoIs, or a legacy header. A gated node
    /// never falls open (close code 4003).
    #[error("identity unavailable: this node admits only listed logins and the connection carried no authenticated identity")]
    IdentityUnavailable,

    /// The caller's Tailscale-authenticated login is not on this node's
    /// allow-list (RFC 025 §3.4, close code 4004). `login` is the WhoIs
    /// login the bridge reported, or `None` when it reported none (a tagged
    /// node with no user profile, or a sidecar that omits the field).
    #[error("login refused: {} is not on this node's allow-list", login.as_deref().unwrap_or("<no login>"))]
    LoginRefused {
        /// The authenticated login that was refused, if the bridge reported one.
        login: Option<String>,
    },

    /// The remote closed the socket with an application close code before
    /// completing the hello — it refused us (RFC 017 §8 / RFC 025 §4:
    /// 4001 app mismatch, 4002 hello protocol, 4003 identity mismatch or
    /// unavailable, 4004 login refused). The dialing side surfaces the code
    /// so a consumer can tell "not my app" from "not my login".
    #[error("hello refused by remote: close code {code} ({reason})")]
    HelloRefused {
        /// The WebSocket close code the remote sent.
        code: u16,
        /// The close reason the remote sent (may be empty).
        reason: String,
    },

    /// The connection was closed unexpectedly.
    #[error("connection closed: {0}")]
    ConnectionClosed(String),

    /// A send or receive operation timed out.
    #[error("timeout: {0}")]
    Timeout(String),

    /// Heartbeat (ping/pong) failure — the peer stopped responding.
    #[error("heartbeat timeout after {0:?}")]
    HeartbeatTimeout(Duration),

    /// WebSocket protocol error.
    #[error("websocket error: {0}")]
    WebSocket(String),

    /// I/O error from the underlying stream.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// Serialization/deserialization error (e.g., handshake JSON).
    #[error("serialization error: {0}")]
    Serialize(String),

    /// Error from Layer 3 (network).
    #[error("network error: {0}")]
    Network(#[from] crate::network::NetworkError),
}
