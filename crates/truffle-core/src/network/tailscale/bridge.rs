//! Local TCP bridge for Go sidecar ↔ Rust data plane connections.
//!
//! The bridge listens on a local TCP port. When the Go sidecar needs to deliver
//! a Tailscale TCP stream to Rust (either from a dial or an incoming connection),
//! it connects to this port and sends a binary header identifying the connection.
//!
//! ## Outgoing dials (Rust → peer)
//!
//! 1. Rust sends `bridge:dial` command to sidecar via stdin
//! 2. Go calls `tsnet.Dial(addr, port)`
//! 3. Go connects to bridge, writes header with direction=Outgoing + requestId
//! 4. Bridge reads header, matches requestId to pending dial, delivers TcpStream
//!
//! ## Incoming connections (peer → Rust)
//!
//! 1. Go accepts connection on a listened port
//! 2. Go connects to bridge, writes header with direction=Incoming + port
//! 3. Bridge reads header, delivers connection to the listener channel for that port
//!
//! This is Layer 2 in the architecture. Everything here is private.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use subtle::ConstantTimeEq;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot, Mutex};

use super::header::{BridgeHeader, Direction};
use crate::network::{IncomingConnection, NetworkError};

/// Maximum concurrent bridge connections.
const MAX_CONCURRENT_CONNECTIONS: usize = 256;

/// Timeout for reading the bridge header after accepting a connection.
const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(2);

/// Timeout for outgoing dial correlation.
pub(crate) const DIAL_TIMEOUT: Duration = Duration::from_secs(30);

/// The bridge TCP listener and connection router.
///
/// Accepts connections from the Go sidecar, reads and validates the binary header,
/// verifies the session token, and routes:
/// - Outgoing connections to pending dial oneshots
/// - Incoming connections to port-specific listener channels
pub(crate) struct Bridge {
    /// The local TCP listener.
    listener: TcpListener,
    /// Session token for authentication (32 bytes).
    session_token: [u8; 32],
    /// Pending outgoing dials: requestId -> sender for delivering the TcpStream.
    pending_dials: Arc<Mutex<HashMap<String, oneshot::Sender<TcpStream>>>>,
    /// Listener channels for incoming connections, keyed by port.
    incoming_channels: Arc<Mutex<HashMap<u16, mpsc::Sender<IncomingConnection>>>>,
    /// Semaphore to cap concurrent connections.
    semaphore: Arc<tokio::sync::Semaphore>,
}

impl Bridge {
    /// Create a new bridge listening on an ephemeral local port.
    pub async fn bind(session_token: [u8; 32]) -> Result<Self, NetworkError> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        Ok(Self {
            listener,
            session_token,
            pending_dials: Arc::new(Mutex::new(HashMap::new())),
            incoming_channels: Arc::new(Mutex::new(HashMap::new())),
            semaphore: Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_CONNECTIONS)),
        })
    }

    /// Returns the local port the bridge is listening on.
    pub fn local_port(&self) -> Result<u16, NetworkError> {
        Ok(self
            .listener
            .local_addr()
            .map_err(|e| NetworkError::Internal(format!("bridge listener local_addr: {e}")))?
            .port())
    }

    /// Register a pending outgoing dial. Returns a receiver that will deliver
    /// the TcpStream when the Go sidecar bridges the connection back.
    pub async fn register_dial(&self, request_id: String) -> oneshot::Receiver<TcpStream> {
        let (tx, rx) = oneshot::channel();
        self.pending_dials.lock().await.insert(request_id, tx);
        rx
    }

    /// Remove a pending dial (e.g., on timeout or cancellation).
    pub async fn remove_dial(&self, request_id: &str) {
        self.pending_dials.lock().await.remove(request_id);
    }

    /// Number of pending outgoing dials (test-only observability).
    #[cfg(test)]
    pub(crate) async fn pending_dial_count(&self) -> usize {
        self.pending_dials.lock().await.len()
    }

    /// Register a channel for incoming connections on a specific port.
    pub async fn register_listener(&self, port: u16, tx: mpsc::Sender<IncomingConnection>) {
        self.incoming_channels.lock().await.insert(port, tx);
    }

    /// Remove the listener channel for a port.
    pub async fn remove_listener(&self, port: u16) {
        self.incoming_channels.lock().await.remove(&port);
    }

    /// Run the bridge accept loop. This should be spawned as a task.
    ///
    /// Accepts connections, reads headers, validates tokens, and routes
    /// to the appropriate destination (pending dial or listener channel).
    pub async fn run(&self, shutdown: tokio::sync::watch::Receiver<bool>) {
        let mut shutdown = shutdown;

        loop {
            tokio::select! {
                result = self.listener.accept() => {
                    match result {
                        Ok((stream, _addr)) => {
                            let permit = match self.semaphore.clone().try_acquire_owned() {
                                Ok(p) => p,
                                Err(_) => {
                                    tracing::warn!("bridge: max connections reached, dropping");
                                    drop(stream);
                                    continue;
                                }
                            };

                            let session_token = self.session_token;
                            let pending_dials = self.pending_dials.clone();
                            let incoming_channels = self.incoming_channels.clone();

                            tokio::spawn(async move {
                                let _permit = permit;
                                if let Err(e) = Self::handle_connection(
                                    stream,
                                    session_token,
                                    pending_dials,
                                    incoming_channels,
                                ).await {
                                    tracing::debug!("bridge connection error: {e}");
                                }
                            });
                        }
                        Err(e) => {
                            tracing::error!("bridge accept error: {e}");
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
                    }
                }
                _ = shutdown.changed() => {
                    tracing::info!("bridge shutting down");
                    break;
                }
            }
        }
    }

    // @atlas step send.handoff
    /// Handle a single incoming bridge connection.
    async fn handle_connection(
        mut stream: TcpStream,
        expected_token: [u8; 32],
        pending_dials: Arc<Mutex<HashMap<String, oneshot::Sender<TcpStream>>>>,
        incoming_channels: Arc<Mutex<HashMap<u16, mpsc::Sender<IncomingConnection>>>>,
    ) -> Result<(), NetworkError> {
        // Read header with timeout
        let header =
            tokio::time::timeout(HEADER_READ_TIMEOUT, BridgeHeader::read_from(&mut stream))
                .await
                .map_err(|_| NetworkError::BridgeError("header read timeout".into()))?
                .map_err(|e| NetworkError::BridgeError(format!("header parse error: {e}")))?;

        // Validate session token (constant-time comparison)
        if header.session_token.ct_eq(&expected_token).unwrap_u8() != 1 {
            tracing::warn!("bridge: rejected connection with invalid session token");
            return Err(NetworkError::BridgeError("invalid session token".into()));
        }

        match header.direction {
            Direction::Outgoing => {
                // This is a dial response — deliver to the pending dial
                let request_id = &header.request_id;
                if request_id.is_empty() {
                    return Err(NetworkError::BridgeError(
                        "outgoing connection has no request_id".into(),
                    ));
                }

                let sender = pending_dials.lock().await.remove(request_id);
                match sender {
                    Some(tx) => {
                        tracing::debug!("bridge: delivering dial result for {request_id}");
                        let _ = tx.send(stream);
                    }
                    None => {
                        tracing::warn!(
                            "bridge: no pending dial for request_id={request_id}, dropping"
                        );
                    }
                }
            }
            Direction::Incoming => {
                // This is an incoming connection — deliver to the listener channel
                let port = header.service_port;
                let channels = incoming_channels.lock().await;
                if let Some(tx) = channels.get(&port) {
                    let conn = IncomingConnection {
                        stream,
                        remote_addr: header.remote_addr,
                        remote_identity: header.remote_dns_name,
                        port,
                    };
                    if tx.send(conn).await.is_err() {
                        tracing::warn!("bridge: listener channel for port {port} closed");
                    }
                } else {
                    tracing::warn!(
                        "bridge: no listener registered for port {port}, dropping connection"
                    );
                }
            }
        }

        Ok(())
    }
}
