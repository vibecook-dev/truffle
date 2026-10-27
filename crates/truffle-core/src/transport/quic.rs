//! QUIC transport — [`StreamTransport`] implementation over QUIC, plus the
//! raw QUIC API (RFC 021: [`QuicConnection`] / [`QuicStream`] /
//! [`QuicListener`], reached via `Node::connect_quic` / `Node::listen_quic`).
//!
//! Uses the `quinn` crate for the QUIC protocol, with self-signed certificates
//! generated at runtime via `rcgen`. Since truffle runs over Tailscale
//! (which already provides WireGuard encryption), the QUIC TLS layer is
//! redundant but required by the QUIC specification.
//!
//! # Connection flow
//!
//! **Client (connect)**:
//! 1. Create a QUIC client endpoint with a permissive TLS verifier
//! 2. `endpoint.connect(addr, "truffle")` to establish a QUIC connection
//! 3. Open a bidirectional stream for the handshake
//! 4. Exchange [`Handshake`] messages (JSON, same as WebSocket transport)
//! 5. Return a [`QuicFramedStream`] wrapping the QUIC bidirectional stream
//!
//! **Server (listen)**:
//! 1. Create a QUIC server endpoint with a self-signed certificate
//! 2. Accept incoming QUIC connections
//! 3. For each: accept the first bidirectional stream, perform handshake
//! 4. Yield [`QuicFramedStream`]s via [`StreamListener`]
//!
//! # Framing
//!
//! [`QuicFramedStream`] uses length-prefixed framing on top of QUIC's
//! reliable ordered stream:
//! - **send**: 4-byte big-endian length prefix + payload
//! - **recv**: read 4-byte length prefix, then read that many bytes
//!
//! This is simpler than WebSocket framing and well-suited for binary data.
//!
//! # tsnet support
//!
//! When connected to a [`NetworkProvider`] that supports UDP (e.g.,
//! [`TailscaleProvider`](crate::network::tailscale::TailscaleProvider)),
//! the transport uses [`TsnetUdpSocket`]
//! to route QUIC datagrams through the tsnet relay instead of creating
//! a direct host-network socket. This enables QUIC over userspace Tailscale
//! (tsnet) where the host UDP socket cannot reach peers.
//!
//! When no network provider UDP support is available (e.g., loopback tests
//! with a mock provider), the transport falls back to quinn's default
//! host-network sockets.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use quinn::{RecvStream, SendStream};

use crate::network::{NetworkProvider, PeerAddr};

use super::quic_socket::TsnetUdpSocket;
use super::{
    resolve_dial_addr, FramedStream, Handshake, StreamListener, StreamTransport, TransportError,
    PROTOCOL_VERSION,
};

/// Handshake timeout — maximum time to wait for QUIC handshake exchange.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// Maximum framed message size (64 MiB). Protects against malicious/corrupt length prefixes.
const MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024;

/// ALPN protocol id for the internal framed transport (session use).
const INTERNAL_ALPN: &[u8] = b"truffle";

/// SNI placeholder for QUIC dials. The verifier ignores it (WireGuard
/// authenticates the path), but rustls requires a well-formed server name.
const SERVER_NAME: &str = "truffle";

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for QUIC transport.
#[derive(Debug, Clone)]
pub struct QuicConfig {
    /// Port to listen on for incoming QUIC connections. Default: 9420.
    pub port: u16,
    /// Maximum number of concurrent bidirectional streams per connection.
    /// Default: 100.
    pub max_streams: u32,
}

impl Default for QuicConfig {
    fn default() -> Self {
        Self {
            port: 9420,
            max_streams: 100,
        }
    }
}

// ---------------------------------------------------------------------------
// TLS helpers
// ---------------------------------------------------------------------------

/// Generate a self-signed certificate and private key for QUIC TLS.
///
/// Since Tailscale already encrypts all traffic via WireGuard, this TLS layer
/// is redundant but required by the QUIC protocol specification.
fn generate_self_signed_cert() -> Result<
    (
        rustls::pki_types::CertificateDer<'static>,
        rustls::pki_types::PrivateKeyDer<'static>,
    ),
    TransportError,
> {
    let rcgen::CertifiedKey { cert, signing_key } =
        rcgen::generate_simple_self_signed(vec!["truffle".to_string(), "localhost".to_string()])
            .map_err(|e| {
                TransportError::ConnectFailed(format!("generate self-signed cert: {e}"))
            })?;

    let cert_der = rustls::pki_types::CertificateDer::from(cert.der().to_vec());
    let key_der = rustls::pki_types::PrivateKeyDer::try_from(signing_key.serialize_der())
        .map_err(|e| TransportError::ConnectFailed(format!("serialize private key: {e}")))?;

    Ok((cert_der, key_der))
}

/// Build a `quinn::ServerConfig` with a self-signed certificate.
fn build_server_config(
    alpn: &[u8],
    max_streams: u32,
) -> Result<quinn::ServerConfig, TransportError> {
    let (cert_der, key_der) = generate_self_signed_cert()?;

    let mut tls_config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|e| TransportError::ListenFailed(format!("rustls protocol versions: {e}")))?
    .with_no_client_auth()
    .with_single_cert(vec![cert_der], key_der)
    .map_err(|e| TransportError::ListenFailed(format!("rustls server config: {e}")))?;

    tls_config.alpn_protocols = vec![alpn.to_vec()];

    let mut transport_config = quinn::TransportConfig::default();
    transport_config.max_concurrent_bidi_streams(max_streams.into());
    transport_config.max_concurrent_uni_streams(0u32.into());

    let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(
        quinn::crypto::rustls::QuicServerConfig::try_from(tls_config)
            .map_err(|e| TransportError::ListenFailed(format!("quic server config: {e}")))?,
    ));
    server_config.transport_config(Arc::new(transport_config));

    Ok(server_config)
}

/// Build a `quinn::ClientConfig` that skips server certificate verification.
///
/// This is safe because Tailscale already provides mutual authentication
/// and encryption via WireGuard. The QUIC TLS is purely for protocol compliance.
fn build_client_config(alpn: &[u8]) -> Result<quinn::ClientConfig, TransportError> {
    let mut tls_config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|e| TransportError::ConnectFailed(format!("rustls protocol versions: {e}")))?
    .dangerous()
    .with_custom_certificate_verifier(Arc::new(SkipServerVerification))
    .with_no_client_auth();

    // Must match the server's ALPN protocol, otherwise the TLS handshake fails.
    tls_config.alpn_protocols = vec![alpn.to_vec()];

    let mut client_config = quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(tls_config)
            .map_err(|e| TransportError::ConnectFailed(format!("quic client config: {e}")))?,
    ));

    let mut transport_config = quinn::TransportConfig::default();
    transport_config.max_concurrent_bidi_streams(100u32.into());
    transport_config.max_concurrent_uni_streams(0u32.into());
    // Keep-alive to prevent idle timeouts during tests and real usage
    transport_config.keep_alive_interval(Some(Duration::from_secs(5)));
    client_config.transport_config(Arc::new(transport_config));

    Ok(client_config)
}

/// A `rustls` certificate verifier that accepts any server certificate.
///
/// QUIC mandates TLS 1.3, but since Tailscale already provides encryption
/// and authentication, we skip verification here.
#[derive(Debug)]
struct SkipServerVerification;

impl rustls::client::danger::ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

// ---------------------------------------------------------------------------
// QuicTransport
// ---------------------------------------------------------------------------

/// QUIC-based [`StreamTransport`] implementation.
///
/// Generic over the [`NetworkProvider`] type `N`. Provides multiplexed
/// bidirectional streams over a single QUIC connection.
///
/// QUIC is ideal for scenarios requiring multiple concurrent streams
/// without head-of-line blocking (e.g., video streaming via fondue).
///
/// # Example
///
/// ```ignore
/// use std::sync::Arc;
/// use truffle_core::transport::quic::{QuicTransport, QuicConfig};
///
/// let quic = QuicTransport::new(Arc::new(provider), QuicConfig::default());
/// let stream = quic.connect(&peer_addr).await?;
/// ```
pub struct QuicTransport<N: NetworkProvider> {
    /// Layer 3 network provider (used for identity in handshake).
    network: Arc<N>,
    /// QUIC configuration.
    config: QuicConfig,
}

impl<N: NetworkProvider + 'static> QuicTransport<N> {
    /// Create a new QUIC transport.
    ///
    /// - `network`: An `Arc<N>` where `N: NetworkProvider`.
    /// - `config`: QUIC configuration (port, max streams, etc.).
    pub fn new(network: Arc<N>, config: QuicConfig) -> Self {
        Self { network, config }
    }

    /// Build the local handshake message using the network provider's identity.
    ///
    /// RFC 017 Phase 2: the QUIC handshake now identifies the local endpoint
    /// by its stable `device_id` (ULID) rather than the Tailscale stable ID.
    /// The WebSocket transport carries a richer [`HelloEnvelope`] — QUIC is
    /// not yet plumbed through the session layer, so it keeps the old
    /// [`Handshake`] shape and just swaps the ID.
    fn local_handshake(&self) -> Handshake {
        let identity = self.network.local_identity();
        Handshake {
            peer_id: identity.device_id,
            capabilities: vec!["quic".to_string(), "binary".to_string()],
            protocol_version: PROTOCOL_VERSION,
        }
    }
}

// ---------------------------------------------------------------------------
// Endpoint helpers (shared by the internal transport and the raw QUIC API)
// ---------------------------------------------------------------------------

/// Try to create a QUIC client endpoint backed by a [`TsnetUdpSocket`].
///
/// Binds a UDP socket through the network provider and wraps it in a
/// `TsnetUdpSocket` for quinn. Returns the configured `Endpoint`.
///
/// Returns `Err` if the network provider does not support UDP
/// (e.g., mock providers in tests).
async fn create_tsnet_client_endpoint<N: NetworkProvider + 'static>(
    network: &Arc<N>,
    client_config: &quinn::ClientConfig,
) -> Result<quinn::Endpoint, TransportError> {
    // Bind on port 0 (ephemeral) for the client
    let net_socket = network
        .bind_udp(0)
        .await
        .map_err(|e| TransportError::ConnectFailed(format!("bind_udp for tsnet client: {e}")))?;

    let tsnet_port = net_socket.tsnet_port();
    let local_ip = network
        .local_addr()
        .ip
        .unwrap_or(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED));
    let tsnet_addr = SocketAddr::new(local_ip, tsnet_port);

    let tsnet_socket = Arc::new(TsnetUdpSocket::new(net_socket, tsnet_addr));

    let runtime = quinn::default_runtime().ok_or_else(|| {
        TransportError::ConnectFailed("no async runtime available for quinn".to_string())
    })?;

    let mut endpoint = quinn::Endpoint::new_with_abstract_socket(
        quinn::EndpointConfig::default(),
        None,
        tsnet_socket,
        runtime,
    )
    .map_err(|e| TransportError::ConnectFailed(format!("tsnet quic endpoint: {e}")))?;

    endpoint.set_default_client_config(client_config.clone());

    tracing::debug!(
        tsnet_port,
        tsnet_addr = %tsnet_addr,
        "quic: created tsnet client endpoint"
    );

    Ok(endpoint)
}

/// Try to create a QUIC server endpoint backed by a [`TsnetUdpSocket`].
///
/// Binds a UDP socket on the given port through the network provider
/// and wraps it in a `TsnetUdpSocket` for quinn.
///
/// Returns `(Endpoint, actual_port)` or `Err` if the provider does not
/// support UDP.
async fn create_tsnet_server_endpoint<N: NetworkProvider + 'static>(
    network: &Arc<N>,
    port: u16,
    server_config: &quinn::ServerConfig,
) -> Result<(quinn::Endpoint, u16), TransportError> {
    let net_socket = network
        .bind_udp(port)
        .await
        .map_err(|e| TransportError::ListenFailed(format!("bind_udp for tsnet server: {e}")))?;

    let tsnet_port = net_socket.tsnet_port();
    let local_ip = network
        .local_addr()
        .ip
        .unwrap_or(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED));
    let tsnet_addr = SocketAddr::new(local_ip, tsnet_port);

    let tsnet_socket = Arc::new(TsnetUdpSocket::new(net_socket, tsnet_addr));

    let runtime = quinn::default_runtime().ok_or_else(|| {
        TransportError::ListenFailed("no async runtime available for quinn".to_string())
    })?;

    let endpoint = quinn::Endpoint::new_with_abstract_socket(
        quinn::EndpointConfig::default(),
        Some(server_config.clone()),
        tsnet_socket,
        runtime,
    )
    .map_err(|e| TransportError::ListenFailed(format!("tsnet quic endpoint: {e}")))?;

    tracing::debug!(
        tsnet_port,
        tsnet_addr = %tsnet_addr,
        "quic: created tsnet server endpoint"
    );

    Ok((endpoint, tsnet_port))
}

/// Create a client endpoint via tsnet, falling back to a direct host socket
/// only for providers explicitly configured for unverified loopback tests.
async fn client_endpoint_with_fallback<N: NetworkProvider + 'static>(
    network: &Arc<N>,
    client_config: quinn::ClientConfig,
) -> Result<quinn::Endpoint, TransportError> {
    match create_tsnet_client_endpoint(network, &client_config).await {
        Ok(ep) => {
            tracing::info!("quic: using TsnetUdpSocket for client endpoint");
            Ok(ep)
        }
        Err(tsnet_err)
            if network.trust_mode() == crate::network::NetworkTrust::UnverifiedLoopback =>
        {
            tracing::debug!(
                err = %tsnet_err,
                "quic: tsnet client endpoint unavailable, using explicitly enabled loopback test socket"
            );
            let mut ep = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap())
                .map_err(|e| TransportError::ConnectFailed(format!("create quic endpoint: {e}")))?;
            ep.set_default_client_config(client_config);
            Ok(ep)
        }
        Err(e) => Err(e),
    }
}

/// Create a server endpoint via tsnet, falling back to a direct host socket
/// only for providers explicitly configured for unverified loopback tests.
///
/// Returns `(Endpoint, actual_port)`. On the tsnet path the "actual" port is
/// the actual assigned tsnet port;
/// on the fallback path it is the real local socket port.
async fn server_endpoint_with_fallback<N: NetworkProvider + 'static>(
    network: &Arc<N>,
    port: u16,
    server_config: quinn::ServerConfig,
) -> Result<(quinn::Endpoint, u16), TransportError> {
    match create_tsnet_server_endpoint(network, port, &server_config).await {
        Ok((ep, p)) => {
            tracing::info!(port = p, "quic: using TsnetUdpSocket for server endpoint");
            Ok((ep, p))
        }
        Err(tsnet_err)
            if network.trust_mode() == crate::network::NetworkTrust::UnverifiedLoopback =>
        {
            tracing::debug!(
                err = %tsnet_err,
                "quic: tsnet server endpoint unavailable, using explicitly enabled loopback test socket"
            );
            let bind_addr: SocketAddr = format!("127.0.0.1:{port}")
                .parse()
                .map_err(|e| TransportError::ListenFailed(format!("parse bind address: {e}")))?;

            let ep = quinn::Endpoint::server(server_config, bind_addr)
                .map_err(|e| TransportError::ListenFailed(format!("quic listen: {e}")))?;

            let actual = ep
                .local_addr()
                .map_err(|e| TransportError::ListenFailed(format!("local addr: {e}")))?
                .port();

            Ok((ep, actual))
        }
        Err(e) => Err(e),
    }
}

impl<N: NetworkProvider + 'static> StreamTransport for QuicTransport<N> {
    type Stream = QuicFramedStream;

    async fn connect(&self, addr: &PeerAddr) -> Result<Self::Stream, TransportError> {
        let dial_addr = resolve_dial_addr(addr);
        tracing::debug!(addr = %dial_addr, port = self.config.port, "quic: dialing peer");

        // Step 1: Create a QUIC client endpoint.
        // Try tsnet first (network provider UDP), fall back to direct socket.
        let client_config = build_client_config(INTERNAL_ALPN)?;
        let endpoint = client_endpoint_with_fallback(&self.network, client_config).await?;

        // Step 2: Connect to the peer
        let remote_addr: SocketAddr = format!("{dial_addr}:{}", self.config.port)
            .parse()
            .map_err(|e| TransportError::ConnectFailed(format!("parse address: {e}")))?;

        let connection = endpoint
            .connect(remote_addr, SERVER_NAME)
            .map_err(|e| TransportError::ConnectFailed(format!("quic connect: {e}")))?
            .await
            .map_err(|e| TransportError::ConnectFailed(format!("quic handshake: {e}")))?;

        tracing::debug!(
            remote = %connection.remote_address(),
            "quic: connection established"
        );

        // Step 3: Open a bidirectional stream for the transport handshake
        let (send, recv) = connection
            .open_bi()
            .await
            .map_err(|e| TransportError::ConnectFailed(format!("open bi stream: {e}")))?;

        let mut stream = QuicFramedStream::new(send, recv, connection.remote_address().to_string());

        // Step 4: Exchange handshake (with timeout)
        let local_hs = self.local_handshake();
        let remote_hs =
            tokio::time::timeout(HANDSHAKE_TIMEOUT, client_handshake(&mut stream, &local_hs))
                .await
                .map_err(|_| TransportError::Timeout("quic handshake timed out".to_string()))??;

        // Store remote peer info
        stream.remote_peer_id = remote_hs.peer_id.clone();

        tracing::info!(
            remote_peer = %remote_hs.peer_id,
            remote_version = remote_hs.protocol_version,
            "quic: connected"
        );

        Ok(stream)
    }

    async fn listen(&self) -> Result<StreamListener<Self::Stream>, TransportError> {
        let port = self.config.port;
        tracing::debug!(port, "quic: starting listener");

        // Step 1: Build server config with self-signed cert
        let server_config = build_server_config(INTERNAL_ALPN, self.config.max_streams)?;

        // Step 2: Create server endpoint.
        // Try tsnet first (network provider UDP), fall back to direct socket.
        let (endpoint, actual_port) =
            server_endpoint_with_fallback(&self.network, port, server_config).await?;

        tracing::debug!(actual_port, "quic: listener bound");

        // Step 3: Spawn accept loop
        let (tx, rx) = tokio::sync::mpsc::channel::<QuicFramedStream>(64);
        let local_hs = self.local_handshake();

        tokio::spawn(async move {
            while let Some(incoming) = endpoint.accept().await {
                let tx = tx.clone();
                let local_hs = local_hs.clone();

                tokio::spawn(async move {
                    // Accept the QUIC connection
                    let connection = match incoming.await {
                        Ok(conn) => conn,
                        Err(e) => {
                            tracing::warn!("quic: accept connection failed: {e}");
                            return;
                        }
                    };

                    let remote_addr = connection.remote_address().to_string();
                    tracing::debug!(remote = %remote_addr, "quic: accepted connection");

                    // Accept the first bidirectional stream (handshake stream)
                    let (send, recv) = match connection.accept_bi().await {
                        Ok(streams) => streams,
                        Err(e) => {
                            tracing::warn!(
                                remote = %remote_addr,
                                "quic: accept bi stream failed: {e}"
                            );
                            return;
                        }
                    };

                    let mut stream = QuicFramedStream::new(send, recv, remote_addr.clone());

                    // Server-side handshake (with timeout)
                    let remote_hs = match tokio::time::timeout(
                        HANDSHAKE_TIMEOUT,
                        server_handshake(&mut stream, &local_hs),
                    )
                    .await
                    {
                        Ok(Ok(hs)) => hs,
                        Ok(Err(e)) => {
                            tracing::warn!(
                                remote = %remote_addr,
                                "quic: handshake failed: {e}"
                            );
                            return;
                        }
                        Err(_) => {
                            tracing::warn!(
                                remote = %remote_addr,
                                "quic: handshake timed out"
                            );
                            return;
                        }
                    };

                    stream.remote_peer_id = remote_hs.peer_id.clone();

                    tracing::info!(
                        remote_peer = %remote_hs.peer_id,
                        remote_addr = %remote_addr,
                        "quic: accepted connection"
                    );

                    if tx.send(stream).await.is_err() {
                        tracing::debug!("quic: listener channel closed");
                    }
                });
            }

            tracing::debug!("quic: endpoint accept loop ended");
        });

        Ok(StreamListener::new(rx, actual_port))
    }
}

// ---------------------------------------------------------------------------
// Handshake helpers
// ---------------------------------------------------------------------------

/// Client-side handshake: send ours, receive theirs.
async fn client_handshake(
    stream: &mut QuicFramedStream,
    local_hs: &Handshake,
) -> Result<Handshake, TransportError> {
    // Send our handshake
    let hs_json =
        serde_json::to_vec(local_hs).map_err(|e| TransportError::Serialize(e.to_string()))?;
    stream.send(&hs_json).await?;

    // Receive peer's handshake
    let remote_data = stream.recv().await?.ok_or_else(|| {
        TransportError::HandshakeFailed("connection closed before handshake".to_string())
    })?;

    let remote_hs: Handshake = serde_json::from_slice(&remote_data)
        .map_err(|e| TransportError::HandshakeFailed(format!("parse handshake: {e}")))?;

    // Validate protocol version
    if remote_hs.protocol_version != PROTOCOL_VERSION {
        return Err(TransportError::VersionMismatch {
            local: PROTOCOL_VERSION,
            remote: remote_hs.protocol_version,
        });
    }

    Ok(remote_hs)
}

/// Server-side handshake: receive theirs, send ours.
async fn server_handshake(
    stream: &mut QuicFramedStream,
    local_hs: &Handshake,
) -> Result<Handshake, TransportError> {
    // Receive peer's handshake first
    let remote_data = stream.recv().await?.ok_or_else(|| {
        TransportError::HandshakeFailed("connection closed before handshake".to_string())
    })?;

    let remote_hs: Handshake = serde_json::from_slice(&remote_data)
        .map_err(|e| TransportError::HandshakeFailed(format!("parse handshake: {e}")))?;

    // Validate protocol version
    if remote_hs.protocol_version != PROTOCOL_VERSION {
        return Err(TransportError::VersionMismatch {
            local: PROTOCOL_VERSION,
            remote: remote_hs.protocol_version,
        });
    }

    // Send our handshake
    let hs_json =
        serde_json::to_vec(local_hs).map_err(|e| TransportError::Serialize(e.to_string()))?;
    stream.send(&hs_json).await?;

    Ok(remote_hs)
}

// ---------------------------------------------------------------------------
// QuicFramedStream
// ---------------------------------------------------------------------------

/// A QUIC-backed [`FramedStream`].
///
/// Uses length-prefixed framing (4-byte big-endian length + payload) on top
/// of a QUIC bidirectional stream. Each `send()` writes one framed message;
/// each `recv()` reads one framed message.
///
/// Unlike the WebSocket transport, there is no heartbeat — QUIC handles
/// keep-alive and congestion control natively.
pub struct QuicFramedStream {
    /// QUIC send stream (write half).
    send: SendStream,
    /// QUIC receive stream (read half).
    recv: RecvStream,
    /// Remote peer ID (from handshake). Empty until handshake completes.
    remote_peer_id: String,
    /// Remote address string.
    remote_addr: String,
    /// Flag indicating the stream has been closed.
    closed: Arc<AtomicBool>,
}

impl std::fmt::Debug for QuicFramedStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuicFramedStream")
            .field("remote_peer_id", &self.remote_peer_id)
            .field("remote_addr", &self.remote_addr)
            .field("closed", &self.closed.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

// SAFETY: QuicFramedStream is only accessed via &mut self (exclusive reference).
// SendStream and RecvStream are Send but not Sync; since we never share
// references across threads (only &mut self access), this is safe.
#[allow(unsafe_code)]
unsafe impl Sync for QuicFramedStream {}

impl QuicFramedStream {
    /// Create a new QUIC framed stream from quinn stream halves.
    fn new(send: SendStream, recv: RecvStream, remote_addr: String) -> Self {
        Self {
            send,
            recv,
            remote_peer_id: String::new(),
            remote_addr,
            closed: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Get the remote peer ID (from the transport handshake).
    pub fn remote_peer_id(&self) -> &str {
        &self.remote_peer_id
    }
}

impl FramedStream for QuicFramedStream {
    async fn send(&mut self, data: &[u8]) -> Result<(), TransportError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(TransportError::ConnectionClosed(
                "quic stream already closed".to_string(),
            ));
        }

        // Length-prefix framing: 4-byte BE length + payload
        let len = data.len() as u32;
        let len_bytes = len.to_be_bytes();

        self.send
            .write_all(&len_bytes)
            .await
            .map_err(|e| TransportError::Io(e.into()))?;

        self.send
            .write_all(data)
            .await
            .map_err(|e| TransportError::Io(e.into()))?;

        Ok(())
    }

    async fn recv(&mut self) -> Result<Option<Vec<u8>>, TransportError> {
        if self.closed.load(Ordering::Acquire) {
            return Ok(None);
        }

        // Read 4-byte length prefix
        let mut len_buf = [0u8; 4];
        match self.recv.read_exact(&mut len_buf).await {
            Ok(()) => {}
            Err(quinn::ReadExactError::FinishedEarly(_)) => {
                // Clean stream finish — the peer closed the send side
                self.closed.store(true, Ordering::Release);
                return Ok(None);
            }
            Err(quinn::ReadExactError::ReadError(e)) => {
                self.closed.store(true, Ordering::Release);
                return Err(TransportError::ConnectionClosed(format!("quic read: {e}")));
            }
        }

        let len = u32::from_be_bytes(len_buf) as usize;

        // Sanity check: reject absurdly large messages
        if len > MAX_MESSAGE_SIZE {
            self.closed.store(true, Ordering::Release);
            return Err(TransportError::ConnectionClosed(format!(
                "message too large: {len} bytes (max {MAX_MESSAGE_SIZE})"
            )));
        }

        // Read the payload
        let mut buf = vec![0u8; len];
        match self.recv.read_exact(&mut buf).await {
            Ok(()) => Ok(Some(buf)),
            Err(quinn::ReadExactError::FinishedEarly(_)) => {
                self.closed.store(true, Ordering::Release);
                Ok(None)
            }
            Err(quinn::ReadExactError::ReadError(e)) => {
                self.closed.store(true, Ordering::Release);
                Err(TransportError::ConnectionClosed(format!("quic read: {e}")))
            }
        }
    }

    async fn close(&mut self) -> Result<(), TransportError> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(()); // Already closed
        }

        // Finish the send stream (signals clean close to the peer)
        self.send
            .finish()
            .map_err(|e| TransportError::ConnectionClosed(format!("quic finish: {e}")))?;

        Ok(())
    }

    fn peer_addr(&self) -> String {
        self.remote_addr.clone()
    }
}

// ---------------------------------------------------------------------------
// Raw QUIC API (RFC 021) — user-facing connections, streams, listeners
// ---------------------------------------------------------------------------
//
// Unlike the `StreamTransport` impl above (which frames messages and performs
// the legacy `Handshake` exchange on a single bi-stream for session use), the
// raw API exposes plain QUIC connections and byte-oriented bidirectional
// streams — no framing, no in-stream handshake. App scoping is enforced at
// the TLS layer via ALPN (`truffle-raw.{app_id}`): peers from a different
// app fail the TLS handshake outright. Peer identity is the
// WireGuard-authenticated source address (map it to a peer via Layer 3).

/// ALPN protocol id for raw (user-facing) QUIC connections, scoped by app.
///
/// Both sides derive this from their own `app_id`, so only same-app peers
/// can complete the TLS handshake. An empty `app_id` (mock providers,
/// `from_parts` test nodes) yields the unscoped `truffle-raw`.
pub(crate) fn raw_alpn(app_id: &str) -> Vec<u8> {
    if app_id.is_empty() {
        b"truffle-raw".to_vec()
    } else {
        format!("truffle-raw.{app_id}").into_bytes()
    }
}

/// Open a raw QUIC connection to `dial_addr:port` (no handshake, no framing).
pub(crate) async fn connect_raw<N: NetworkProvider + 'static>(
    network: &Arc<N>,
    dial_addr: &str,
    port: u16,
    alpn: &[u8],
) -> Result<QuicConnection, TransportError> {
    let client_config = build_client_config(alpn)?;
    let endpoint = client_endpoint_with_fallback(network, client_config).await?;

    let remote_addr: SocketAddr = format!("{dial_addr}:{port}")
        .parse()
        .map_err(|e| TransportError::ConnectFailed(format!("parse address: {e}")))?;

    let conn = endpoint
        .connect(remote_addr, SERVER_NAME)
        .map_err(|e| TransportError::ConnectFailed(format!("quic connect: {e}")))?
        .await
        .map_err(|e| TransportError::ConnectFailed(format!("quic handshake: {e}")))?;

    tracing::info!(remote = %conn.remote_address(), "quic(raw): connected");

    Ok(QuicConnection {
        conn,
        _endpoint: endpoint,
    })
}

/// Listen for raw QUIC connections on `port`.
///
/// Port 0 is only meaningful on the direct-socket fallback path (tests) —
/// over the tsnet relay the actual ephemeral port cannot be reported back,
/// so `Node::listen_quic` rejects port 0 before reaching here.
pub(crate) async fn listen_raw<N: NetworkProvider + 'static>(
    network: &Arc<N>,
    port: u16,
    alpn: &[u8],
) -> Result<QuicListener, TransportError> {
    let server_config = build_server_config(alpn, QuicConfig::default().max_streams)?;
    let (endpoint, actual_port) =
        server_endpoint_with_fallback(network, port, server_config).await?;

    tracing::info!(port = actual_port, "quic(raw): listening");

    Ok(QuicListener {
        endpoint,
        port: actual_port,
    })
}

/// A raw QUIC connection to a peer.
///
/// Obtained from `Node::connect_quic` (client side) or
/// [`QuicListener::accept`] (server side). Supports opening and accepting
/// multiple concurrent bidirectional byte streams. Streams are lazy: the
/// peer's `accept_stream()` does not fire until the opener writes bytes.
pub struct QuicConnection {
    conn: quinn::Connection,
    /// Keeps the endpoint driver alive for the lifetime of the connection.
    _endpoint: quinn::Endpoint,
}

impl QuicConnection {
    /// Open a new bidirectional byte stream on this connection.
    pub async fn open_stream(&self) -> Result<QuicStream, TransportError> {
        let (send, recv) = self
            .conn
            .open_bi()
            .await
            .map_err(|e| TransportError::ConnectionClosed(format!("quic open stream: {e}")))?;
        Ok(QuicStream { send, recv })
    }

    /// Accept the next stream opened by the peer.
    ///
    /// Returns `Ok(None)` once the connection has closed (either side).
    pub async fn accept_stream(&self) -> Result<Option<QuicStream>, TransportError> {
        match self.conn.accept_bi().await {
            Ok((send, recv)) => Ok(Some(QuicStream { send, recv })),
            Err(quinn::ConnectionError::ApplicationClosed(_))
            | Err(quinn::ConnectionError::LocallyClosed)
            | Err(quinn::ConnectionError::ConnectionClosed(_)) => Ok(None),
            Err(e) => Err(TransportError::ConnectionClosed(format!(
                "quic accept stream: {e}"
            ))),
        }
    }

    /// Remote address. Over the tsnet relay this is the peer's
    /// WireGuard-authenticated tailnet address (`100.x.x.x:port`).
    pub fn remote_address(&self) -> SocketAddr {
        self.conn.remote_address()
    }

    /// Close the connection and all its streams (application error code 0).
    pub fn close(&self) {
        self.conn.close(quinn::VarInt::from_u32(0), b"");
    }
}

impl std::fmt::Debug for QuicConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuicConnection")
            .field("remote", &self.conn.remote_address())
            .finish_non_exhaustive()
    }
}

/// A bidirectional byte stream on a [`QuicConnection`].
///
/// Plain bytes — no length-prefix framing. One `QuicStream` behaves like an
/// independent TCP connection (ordered, reliable, flow-controlled) without
/// head-of-line blocking against sibling streams.
pub struct QuicStream {
    send: SendStream,
    recv: RecvStream,
}

impl QuicStream {
    /// Read up to `max_len` bytes. Returns `Ok(None)` when the peer has
    /// finished the stream (clean EOF).
    pub async fn read(&mut self, max_len: usize) -> Result<Option<Vec<u8>>, TransportError> {
        let mut buf = vec![0u8; max_len];
        match self.recv.read(&mut buf).await {
            Ok(Some(n)) => {
                buf.truncate(n);
                Ok(Some(buf))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(TransportError::ConnectionClosed(format!("quic read: {e}"))),
        }
    }

    /// Write all of `data` to the stream (respects QUIC flow control).
    pub async fn write(&mut self, data: &[u8]) -> Result<(), TransportError> {
        self.send
            .write_all(data)
            .await
            .map_err(|e| TransportError::ConnectionClosed(format!("quic write: {e}")))
    }

    /// Half-close: finish the write side, signalling clean EOF to the peer.
    /// Reading remains possible. Idempotent.
    pub fn finish(&mut self) {
        let _ = self.send.finish();
    }

    /// Fully close the stream: finish the write side and stop the read side.
    /// Idempotent.
    pub fn close(&mut self) {
        let _ = self.send.finish();
        let _ = self.recv.stop(quinn::VarInt::from_u32(0));
    }

    /// Split into quinn send/receive halves (used by the FFI layer to give
    /// reads and writes independent locks).
    pub fn into_split(self) -> (SendStream, RecvStream) {
        (self.send, self.recv)
    }
}

/// A listener for raw QUIC connections, obtained from `Node::listen_quic`.
pub struct QuicListener {
    endpoint: quinn::Endpoint,
    port: u16,
}

impl QuicListener {
    /// Accept the next incoming connection.
    ///
    /// Returns `None` once the listener has been closed. Individual failed
    /// handshakes (e.g., ALPN mismatch from a different app) are logged and
    /// skipped, not surfaced.
    pub async fn accept(&self) -> Option<QuicConnection> {
        loop {
            let incoming = self.endpoint.accept().await?;
            match incoming.await {
                Ok(conn) => {
                    tracing::debug!(remote = %conn.remote_address(), "quic(raw): accepted connection");
                    return Some(QuicConnection {
                        conn,
                        _endpoint: self.endpoint.clone(),
                    });
                }
                Err(e) => {
                    tracing::debug!("quic(raw): incoming connection failed: {e}");
                    continue;
                }
            }
        }
    }

    /// The port this listener is bound to (the tsnet port over the relay,
    /// or the local socket port on the direct fallback path).
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Close the listener. Also closes connections previously accepted from
    /// it — they share the underlying endpoint.
    pub fn close(&self) {
        self.endpoint.close(quinn::VarInt::from_u32(0), b"");
    }
}

impl std::fmt::Debug for QuicListener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuicListener")
            .field("port", &self.port)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod security_regressions {
    use super::*;
    use crate::network::tailscale::{TailscaleConfig, TailscaleProvider};
    fn stopped_provider() -> Arc<TailscaleProvider> {
        Arc::new(TailscaleProvider::new(TailscaleConfig {
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
        }))
    }
    #[tokio::test]
    async fn stopped_tailscale_cannot_create_host_quic_client() {
        let result = client_endpoint_with_fallback(
            &stopped_provider(),
            build_client_config(b"test").unwrap(),
        )
        .await;
        assert!(
            result.is_err(),
            "stopped Tailscale silently created a host QUIC client"
        );
    }
    #[tokio::test]
    async fn stopped_tailscale_cannot_expose_host_quic_listener() {
        let result = listen_raw(&stopped_provider(), 0, b"test").await;
        assert!(
            result.is_err(),
            "stopped Tailscale silently exposed a host QUIC listener"
        );
    }
    #[tokio::test]
    async fn stopped_tailscale_cannot_expose_host_udp_socket() {
        use crate::transport::{
            udp::{UdpConfig, UdpTransport},
            DatagramTransport,
        };
        let result = UdpTransport::new(stopped_provider(), UdpConfig::default())
            .bind(0)
            .await;
        assert!(
            result.is_err(),
            "stopped Tailscale silently exposed a host UDP socket"
        );
    }
}
