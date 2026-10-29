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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use subtle::ConstantTimeEq;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::error::SendTimeoutError;
use tokio::sync::{mpsc, oneshot, Mutex};

use super::header::{BridgeHeader, Direction};
use crate::network::{IncomingConnection, NetworkError};

/// Maximum concurrent bridge connections.
const MAX_CONCURRENT_CONNECTIONS: usize = 256;

/// Timeout for reading the bridge header after accepting a connection.
const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(2);

/// Timeout for outgoing dial correlation.
pub(crate) const DIAL_TIMEOUT: Duration = Duration::from_secs(30);

/// How long an incoming connection may wait for room in its listener's
/// accept queue before it is dropped (and counted). The wait holds one of the
/// [`MAX_CONCURRENT_CONNECTIONS`] handshake permits, so it must stay bounded:
/// an app that stopped accepting would otherwise pin all of them.
const DELIVERY_TIMEOUT: Duration = Duration::from_secs(5);

/// Connections the bridge handled, and every way it can drop one.
///
/// Each drop used to be at most a log line; these counters make bursts that
/// overrun the bridge (hundreds of thumbnail requests) or an app that is slow
/// to `accept()` visible. Cumulative over the provider's lifetime; read a
/// snapshot with `TailscaleProvider::bridge_stats` or `Node::bridge_stats`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BridgeStats {
    /// Incoming connections handed to a listener.
    pub incoming_delivered: u64,
    /// Outgoing dials matched to their waiter.
    pub outgoing_delivered: u64,
    /// Dropped on accept: more than 256 header handshakes in flight.
    pub dropped_over_capacity: u64,
    /// Dropped: the header did not arrive in time or did not parse.
    pub dropped_bad_header: u64,
    /// Dropped: wrong session token (not our sidecar).
    pub dropped_bad_token: u64,
    /// Dropped: no listener registered for the connection's port.
    pub dropped_no_listener: u64,
    /// Dropped: the listener's accept queue stayed full for 5 s.
    pub dropped_listener_full: u64,
    /// Dropped: the listener was closed while the connection waited.
    pub dropped_listener_closed: u64,
    /// Dropped: an outgoing connection whose dial had already given up.
    pub dropped_no_pending_dial: u64,
}

impl BridgeStats {
    /// Total connections dropped for any reason.
    pub fn dropped(&self) -> u64 {
        self.dropped_over_capacity
            + self.dropped_bad_header
            + self.dropped_bad_token
            + self.dropped_no_listener
            + self.dropped_listener_full
            + self.dropped_listener_closed
            + self.dropped_no_pending_dial
    }
}

/// Lock-free counters behind [`BridgeStats`], shared by the provider (which
/// outlives any one bridge) and the bridge's connection tasks.
#[derive(Debug, Default)]
pub(crate) struct BridgeCounters {
    incoming_delivered: AtomicU64,
    outgoing_delivered: AtomicU64,
    dropped_over_capacity: AtomicU64,
    dropped_bad_header: AtomicU64,
    dropped_bad_token: AtomicU64,
    dropped_no_listener: AtomicU64,
    dropped_listener_full: AtomicU64,
    dropped_listener_closed: AtomicU64,
    dropped_no_pending_dial: AtomicU64,
}

impl BridgeCounters {
    fn bump(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    /// A consistent-enough snapshot (each field is read atomically).
    pub(crate) fn snapshot(&self) -> BridgeStats {
        let get = |c: &AtomicU64| c.load(Ordering::Relaxed);
        BridgeStats {
            incoming_delivered: get(&self.incoming_delivered),
            outgoing_delivered: get(&self.outgoing_delivered),
            dropped_over_capacity: get(&self.dropped_over_capacity),
            dropped_bad_header: get(&self.dropped_bad_header),
            dropped_bad_token: get(&self.dropped_bad_token),
            dropped_no_listener: get(&self.dropped_no_listener),
            dropped_listener_full: get(&self.dropped_listener_full),
            dropped_listener_closed: get(&self.dropped_listener_closed),
            dropped_no_pending_dial: get(&self.dropped_no_pending_dial),
        }
    }
}

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
    /// Delivery and drop accounting.
    counters: Arc<BridgeCounters>,
    /// Bound on waiting for room in a listener's accept queue.
    delivery_timeout: Duration,
}

impl Bridge {
    /// Create a new bridge listening on an ephemeral local port, with its
    /// own counters (the provider uses [`bind_with_counters`](Self::bind_with_counters)).
    #[cfg(test)]
    pub async fn bind(session_token: [u8; 32]) -> Result<Self, NetworkError> {
        Self::bind_with_counters(session_token, Arc::default()).await
    }

    /// Like [`bind`](Self::bind), accounting into `counters` (the provider's,
    /// so the totals survive a restart's new bridge).
    pub(crate) async fn bind_with_counters(
        session_token: [u8; 32],
        counters: Arc<BridgeCounters>,
    ) -> Result<Self, NetworkError> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        Ok(Self {
            listener,
            session_token,
            pending_dials: Arc::new(Mutex::new(HashMap::new())),
            incoming_channels: Arc::new(Mutex::new(HashMap::new())),
            semaphore: Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_CONNECTIONS)),
            counters,
            delivery_timeout: DELIVERY_TIMEOUT,
        })
    }

    /// Snapshot of this bridge's delivery and drop counters.
    #[cfg(test)]
    pub(crate) fn stats(&self) -> BridgeStats {
        self.counters.snapshot()
    }

    /// Override the accept-queue wait (tests use milliseconds).
    #[cfg(test)]
    pub(crate) fn with_delivery_timeout(mut self, timeout: Duration) -> Self {
        self.delivery_timeout = timeout;
        self
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
                                    BridgeCounters::bump(&self.counters.dropped_over_capacity);
                                    tracing::warn!(
                                        "bridge: {MAX_CONCURRENT_CONNECTIONS} handshakes in flight, dropping connection"
                                    );
                                    drop(stream);
                                    continue;
                                }
                            };

                            let session_token = self.session_token;
                            let pending_dials = self.pending_dials.clone();
                            let incoming_channels = self.incoming_channels.clone();
                            let counters = self.counters.clone();
                            let delivery_timeout = self.delivery_timeout;

                            tokio::spawn(async move {
                                let _permit = permit;
                                if let Err(e) = Self::handle_connection(
                                    stream,
                                    session_token,
                                    pending_dials,
                                    incoming_channels,
                                    &counters,
                                    delivery_timeout,
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
        counters: &BridgeCounters,
        delivery_timeout: Duration,
    ) -> Result<(), NetworkError> {
        // Read header with timeout
        let header =
            match tokio::time::timeout(HEADER_READ_TIMEOUT, BridgeHeader::read_from(&mut stream))
                .await
            {
                Ok(Ok(header)) => header,
                Ok(Err(e)) => {
                    BridgeCounters::bump(&counters.dropped_bad_header);
                    return Err(NetworkError::BridgeError(format!(
                        "header parse error: {e}"
                    )));
                }
                Err(_) => {
                    BridgeCounters::bump(&counters.dropped_bad_header);
                    return Err(NetworkError::BridgeError("header read timeout".into()));
                }
            };

        // Validate session token (constant-time comparison)
        if header.session_token.ct_eq(&expected_token).unwrap_u8() != 1 {
            BridgeCounters::bump(&counters.dropped_bad_token);
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
                        if tx.send(stream).is_ok() {
                            BridgeCounters::bump(&counters.outgoing_delivered);
                        } else {
                            // The waiter gave up between remove() and send().
                            BridgeCounters::bump(&counters.dropped_no_pending_dial);
                        }
                    }
                    None => {
                        BridgeCounters::bump(&counters.dropped_no_pending_dial);
                        tracing::warn!(
                            "bridge: no pending dial for request_id={request_id}, dropping"
                        );
                    }
                }
            }
            Direction::Incoming => {
                // This is an incoming connection — deliver to the listener
                // channel. Clone the sender and release the map lock BEFORE
                // waiting for queue room: holding it across `send().await`
                // let one listener whose app was slow to accept stall every
                // other port's incoming connections (head-of-line blocking).
                let port = header.service_port;
                let tx = incoming_channels.lock().await.get(&port).cloned();
                let Some(tx) = tx else {
                    BridgeCounters::bump(&counters.dropped_no_listener);
                    tracing::warn!(
                        "bridge: no listener registered for port {port}, dropping connection"
                    );
                    return Ok(());
                };
                let conn = IncomingConnection {
                    stream,
                    remote_addr: header.remote_addr,
                    remote_identity: header.remote_dns_name,
                    port,
                };
                match tx.send_timeout(conn, delivery_timeout).await {
                    Ok(()) => BridgeCounters::bump(&counters.incoming_delivered),
                    Err(SendTimeoutError::Timeout(_)) => {
                        BridgeCounters::bump(&counters.dropped_listener_full);
                        tracing::warn!(
                            "bridge: listener on port {port} did not accept within \
                             {delivery_timeout:?}; dropping connection"
                        );
                    }
                    Err(SendTimeoutError::Closed(_)) => {
                        BridgeCounters::bump(&counters.dropped_listener_closed);
                        tracing::warn!("bridge: listener channel for port {port} closed");
                    }
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    const TOKEN: [u8; 32] = [0x5A; 32];

    /// Start `bridge` and return its port plus the shutdown sender.
    fn run(bridge: Arc<Bridge>) -> (u16, tokio::sync::watch::Sender<bool>) {
        let port = bridge.local_port().unwrap();
        let (tx, rx) = tokio::sync::watch::channel(false);
        tokio::spawn(async move { bridge.run(rx).await });
        (port, tx)
    }

    /// Play the sidecar: connect to the bridge and write an incoming header
    /// for `service_port`.
    async fn sidecar_conn(bridge_port: u16, service_port: u16, token: [u8; 32]) -> TcpStream {
        let mut stream = TcpStream::connect(("127.0.0.1", bridge_port))
            .await
            .unwrap();
        BridgeHeader {
            session_token: token,
            direction: Direction::Incoming,
            service_port,
            request_id: String::new(),
            remote_addr: "100.64.0.2:50000".into(),
            remote_dns_name: String::new(),
        }
        .write_to(&mut stream)
        .await
        .unwrap();
        stream
    }

    async fn wait_for(bridge: &Bridge, pred: impl Fn(&BridgeStats) -> bool) -> BridgeStats {
        for _ in 0..200 {
            let stats = bridge.stats();
            if pred(&stats) {
                return stats;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("bridge stats never matched: {:?}", bridge.stats());
    }

    /// The P1 regression: a listener whose app stopped accepting must not
    /// stall another port's connections. The old code held the channel-map
    /// lock across `send().await`, so port B waited behind port A.
    #[tokio::test]
    async fn a_full_listener_does_not_block_other_ports() {
        let bridge = Arc::new(
            Bridge::bind(TOKEN)
                .await
                .unwrap()
                .with_delivery_timeout(Duration::from_secs(5)),
        );
        let (slow_tx, _slow_rx) = mpsc::channel(1); // never drained
        let (fast_tx, mut fast_rx) = mpsc::channel(8);
        bridge.register_listener(1001, slow_tx).await;
        bridge.register_listener(1002, fast_tx).await;
        let (port, _shutdown) = run(bridge.clone());

        // Fill port 1001's queue, then park a second connection on it.
        let _a1 = sidecar_conn(port, 1001, TOKEN).await;
        wait_for(&bridge, |s| s.incoming_delivered == 1).await;
        let _a2 = sidecar_conn(port, 1001, TOKEN).await;
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Port 1002 must still be served promptly.
        let _b = sidecar_conn(port, 1002, TOKEN).await;
        let delivered = tokio::time::timeout(Duration::from_secs(1), fast_rx.recv())
            .await
            .expect("port 1002 stalled behind port 1001's full queue (head-of-line lock)")
            .expect("listener channel open");
        assert_eq!(delivered.port, 1002);
    }

    /// A connection that cannot be queued within the delivery timeout is
    /// dropped, closed, and counted instead of waiting forever.
    #[tokio::test]
    async fn listener_full_drops_are_counted_and_closed() {
        let bridge = Arc::new(
            Bridge::bind(TOKEN)
                .await
                .unwrap()
                .with_delivery_timeout(Duration::from_millis(100)),
        );
        let (slow_tx, _slow_rx) = mpsc::channel(1);
        bridge.register_listener(1001, slow_tx).await;
        let (port, _shutdown) = run(bridge.clone());

        let _a1 = sidecar_conn(port, 1001, TOKEN).await;
        let mut a2 = sidecar_conn(port, 1001, TOKEN).await;
        let stats = wait_for(&bridge, |s| s.dropped_listener_full == 1).await;
        assert_eq!(stats.incoming_delivered, 1);
        assert_eq!(stats.dropped(), 1);
        let mut buf = [0u8; 1];
        let n = tokio::time::timeout(Duration::from_secs(2), a2.read(&mut buf))
            .await
            .expect("dropped connection must be closed")
            .unwrap_or(0);
        assert_eq!(n, 0, "dropped connection must read EOF");
    }

    #[tokio::test]
    async fn unroutable_connections_are_counted() {
        let bridge = Arc::new(Bridge::bind(TOKEN).await.unwrap());
        let (closed_tx, closed_rx) = mpsc::channel(1);
        drop(closed_rx);
        bridge.register_listener(1003, closed_tx).await;
        let (port, _shutdown) = run(bridge.clone());

        let _no_listener = sidecar_conn(port, 4242, TOKEN).await;
        let _bad_token = sidecar_conn(port, 1003, [0u8; 32]).await;
        let _closed = sidecar_conn(port, 1003, TOKEN).await;
        let mut garbage = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        tokio::io::AsyncWriteExt::write_all(&mut garbage, b"GET / HTTP/1.1\r\n\r\n")
            .await
            .unwrap();

        let stats = wait_for(&bridge, |s| s.dropped() == 4).await;
        assert_eq!(stats.dropped_no_listener, 1);
        assert_eq!(stats.dropped_bad_token, 1);
        assert_eq!(stats.dropped_listener_closed, 1);
        assert_eq!(stats.dropped_bad_header, 1);
        assert_eq!(stats.incoming_delivered, 0);
    }
}
