//! UDP transport — [`DatagramTransport`] implementation.
//!
//! Provides unreliable datagram delivery for real-time use cases: video/audio
//! streaming, game state synchronization, and any application where low latency
//! matters more than guaranteed delivery.
//!
//! # Binding
//!
//! `UdpTransport::bind(port)` first attempts to delegate to
//! [`NetworkProvider::bind_udp`] for relay-based UDP (e.g., tsnet). If the
//! provider does not support UDP, it falls back to binding directly via
//! `tokio::net::UdpSocket` on `0.0.0.0:{port}`.

use std::sync::Arc;

use crate::network::NetworkProvider;

use super::{DatagramSocket, DatagramTransport, TransportError};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for UDP transport.
#[derive(Debug, Clone, Default)]
pub struct UdpConfig {
    /// Port to bind on. Default: 0 (OS-assigned ephemeral port).
    pub port: u16,
}

// ---------------------------------------------------------------------------
// UdpTransport
// ---------------------------------------------------------------------------

/// UDP [`DatagramTransport`] implementation.
///
/// Generic over the [`NetworkProvider`] type `N`. Attempts to use the provider's
/// `bind_udp` for network-relayed UDP (Tailscale tsnet). Falls back to direct
/// `tokio::net::UdpSocket` binding only when the provider explicitly enables unverified loopback tests.
///
/// # Example
///
/// ```ignore
/// use std::sync::Arc;
/// use truffle_core::transport::udp::{UdpTransport, UdpConfig};
///
/// let udp = UdpTransport::new(Arc::new(provider), UdpConfig::default());
/// let socket = udp.bind(0).await?;
/// socket.send_to(b"ping", "127.0.0.1:9000").await?;
/// ```
pub struct UdpTransport<N: NetworkProvider> {
    /// Layer 3 network provider.
    network: Arc<N>,
    /// UDP configuration.
    #[allow(dead_code)]
    config: UdpConfig,
}

impl<N: NetworkProvider + 'static> UdpTransport<N> {
    /// Create a new UDP transport backed by the given network provider.
    pub fn new(network: Arc<N>, config: UdpConfig) -> Self {
        Self { network, config }
    }
}

impl<N: NetworkProvider + 'static> DatagramTransport for UdpTransport<N> {
    async fn bind(&self, port: u16) -> Result<DatagramSocket, TransportError> {
        // Try network provider first (e.g., Tailscale tsnet relay)
        match self.network.bind_udp(port).await {
            Ok(network_socket) => {
                tracing::debug!(
                    port = port,
                    tsnet_port = network_socket.tsnet_port(),
                    "udp: bound via network provider"
                );
                return Ok(DatagramSocket::network(network_socket));
            }
            Err(_)
                if self.network.trust_mode()
                    == crate::network::NetworkTrust::UnverifiedLoopback => {}
            Err(e) => {
                return Err(TransportError::ListenFailed(format!(
                    "udp bind via network provider: {e}"
                )));
            }
        }

        // Explicit test mode: bind only loopback.
        let bind_addr = format!("127.0.0.1:{port}");
        tracing::debug!(addr = %bind_addr, "udp: binding socket directly");

        let socket = tokio::net::UdpSocket::bind(&bind_addr)
            .await
            .map_err(|e| TransportError::ListenFailed(format!("udp bind {bind_addr}: {e}")))?;

        let local_addr = socket.local_addr().map_err(TransportError::Io)?;
        tracing::debug!(local_addr = %local_addr, "udp: socket bound directly");

        Ok(DatagramSocket::direct(socket))
    }
}
