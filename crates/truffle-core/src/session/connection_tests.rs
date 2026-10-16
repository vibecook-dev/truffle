//! Two-way connection tests for Layer 5: peer restarts, cross-dials and
//! shared dials.
//!
//! The mock in `tests.rs` gives every node the same port on 127.0.0.1, so
//! only one side of a pair can listen. These tests need both sides to
//! accept, so they run on a [`Fabric`]: each node listens on its own
//! ephemeral port, peers dial each other by a fake tailnet IP, and every
//! dial goes through a relay that can be **frozen** — both sockets stay open
//! but nothing more crosses, in either direction, not even a FIN. That is
//! what the surviving side sees when a peer behind tsnet is killed with
//! `kill -9`: the process and its netstack vanish without closing anything.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc};

use crate::network::{
    HealthInfo, IncomingConnection, NetworkError, NetworkPeer, NetworkPeerEvent, NetworkProvider,
    NetworkTcpListener, NetworkUdpSocket, NodeIdentity, PeerAddr, PingResult,
};
use crate::transport::websocket::WebSocketTransport;
use crate::transport::WsConfig;

use super::{
    install_decision, ConnectionDirection, InstallDecision, PeerEvent, PeerRegistry,
    PeerRegistryOptions, SessionError, CROSS_DIAL_WINDOW,
};

// ---------------------------------------------------------------------------
// Fabric: a loopback tailnet with freezable links
// ---------------------------------------------------------------------------

/// Shared by every node of a test.
#[derive(Default)]
struct Fabric {
    /// Fake tailnet IP → the port that node's WS listener is bound to.
    listeners: Mutex<HashMap<String, u16>>,
    /// Every relay ever opened: (dialer IP, target IP, frozen flag).
    links: Mutex<Vec<(String, String, Arc<AtomicBool>)>>,
}

impl Fabric {
    /// Freeze every existing link that touches `ip` (a host dying).
    fn freeze(&self, ip: &str) {
        for (from, to, frozen) in self.links.lock().unwrap().iter() {
            if from == ip || to == ip {
                frozen.store(true, Ordering::SeqCst);
            }
        }
    }
}

/// One node's view of the fabric.
struct FabricProvider {
    identity: NodeIdentity,
    ip: String,
    fabric: Arc<Fabric>,
    peer_event_tx: broadcast::Sender<NetworkPeerEvent>,
    /// Delay before each dial reaches the target (a slow first WireGuard
    /// handshake, say).
    dial_delay: Duration,
    /// Dials made so far.
    dials: AtomicUsize,
    /// When set, each dial fails (after `dial_delay`) with this message.
    fail_dials: Option<String>,
}

impl FabricProvider {
    fn new(fabric: &Arc<Fabric>, id: &str, ip: &str) -> Self {
        let (peer_event_tx, _) = broadcast::channel(64);
        Self {
            identity: NodeIdentity {
                app_id: "test".to_string(),
                device_id: format!("dev-{id}"),
                device_name: format!("Fabric Node {id}"),
                tailscale_hostname: format!("truffle-test-{id}"),
                tailscale_id: id.to_string(),
                dns_name: None,
                ip: Some(ip.parse().unwrap()),
                login_name: None,
            },
            ip: ip.to_string(),
            fabric: fabric.clone(),
            peer_event_tx,
            dial_delay: Duration::ZERO,
            dials: AtomicUsize::new(0),
            fail_dials: None,
        }
    }
}

/// Connect to `target_port` through a relay that honours `frozen`.
async fn relay_connect(target_port: u16, frozen: Arc<AtomicBool>) -> std::io::Result<TcpStream> {
    let upstream = TcpStream::connect(("127.0.0.1", target_port)).await?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let (client, accepted) = tokio::join!(TcpStream::connect(addr), listener.accept());
    let client = client?;
    let (inner, _) = accepted?;
    let (inner_read, inner_write) = inner.into_split();
    let (up_read, up_write) = upstream.into_split();
    tokio::spawn(pump(inner_read, up_write, frozen.clone()));
    tokio::spawn(pump(up_read, inner_write, frozen));
    Ok(client)
}

async fn pump(mut from: OwnedReadHalf, mut to: OwnedWriteHalf, frozen: Arc<AtomicBool>) {
    let mut buf = vec![0u8; 16 * 1024];
    loop {
        let n = from.read(&mut buf).await.unwrap_or(0);
        if frozen.load(Ordering::SeqCst) {
            // A dead host: hold both sockets open and never say anything
            // again — no data, no FIN, no RST.
            let _held = (from, to);
            std::future::pending::<()>().await;
            return;
        }
        if n == 0 {
            let _ = to.shutdown().await;
            return;
        }
        if to.write_all(&buf[..n]).await.is_err() {
            return;
        }
    }
}

impl NetworkProvider for FabricProvider {
    fn trust_mode(&self) -> crate::network::NetworkTrust {
        crate::network::NetworkTrust::UnverifiedLoopback
    }

    async fn start(&mut self) -> Result<(), NetworkError> {
        Ok(())
    }

    async fn stop(&self) -> Result<(), NetworkError> {
        Ok(())
    }

    fn local_identity(&self) -> NodeIdentity {
        self.identity.clone()
    }

    fn local_addr(&self) -> PeerAddr {
        PeerAddr {
            ip: Some(self.ip.parse().unwrap()),
            hostname: self.identity.tailscale_hostname.clone(),
            dns_name: None,
        }
    }

    fn peer_events(&self) -> broadcast::Receiver<NetworkPeerEvent> {
        self.peer_event_tx.subscribe()
    }

    async fn peers(&self) -> Vec<NetworkPeer> {
        vec![]
    }

    async fn dial_tcp(&self, addr: &str, _port: u16) -> Result<TcpStream, NetworkError> {
        self.dials.fetch_add(1, Ordering::SeqCst);
        if !self.dial_delay.is_zero() {
            tokio::time::sleep(self.dial_delay).await;
        }
        if let Some(message) = &self.fail_dials {
            return Err(NetworkError::DialFailed(message.clone()));
        }
        let port = self
            .fabric
            .listeners
            .lock()
            .unwrap()
            .get(addr)
            .copied()
            .ok_or_else(|| NetworkError::DialFailed(format!("fabric: nothing at {addr}")))?;
        let frozen = Arc::new(AtomicBool::new(false));
        self.fabric
            .links
            .lock()
            .unwrap()
            .push((self.ip.clone(), addr.to_string(), frozen.clone()));
        relay_connect(port, frozen)
            .await
            .map_err(|e| NetworkError::DialFailed(format!("fabric dial {addr}: {e}")))
    }

    async fn listen_tcp(&self, _port: u16) -> Result<NetworkTcpListener, NetworkError> {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|e| NetworkError::ListenFailed(format!("fabric listen: {e}")))?;
        let port = listener.local_addr().unwrap().port();
        self.fabric
            .listeners
            .lock()
            .unwrap()
            .insert(self.ip.clone(), port);
        let (tx, rx) = mpsc::channel::<IncomingConnection>(64);
        tokio::spawn(async move {
            while let Ok((stream, addr)) = listener.accept().await {
                let conn = IncomingConnection {
                    stream,
                    remote_addr: addr.to_string(),
                    remote_identity: String::new(),
                    port,
                };
                if tx.send(conn).await.is_err() {
                    break;
                }
            }
        });
        Ok(NetworkTcpListener { port, incoming: rx })
    }

    async fn unlisten_tcp(&self, _port: u16) -> Result<(), NetworkError> {
        Ok(())
    }

    async fn bind_udp(&self, _port: u16) -> Result<NetworkUdpSocket, NetworkError> {
        Err(NetworkError::Internal("fabric: UDP not supported".into()))
    }

    async fn ping(&self, _addr: &str) -> Result<PingResult, NetworkError> {
        Ok(PingResult {
            latency: Duration::from_millis(1),
            connection: "direct".to_string(),
            peer_addr: None,
        })
    }

    async fn health(&self) -> HealthInfo {
        HealthInfo {
            state: "running".to_string(),
            healthy: true,
            ..Default::default()
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// The fake tailnet IP of each test node.
fn ip_of(id: &str) -> &'static str {
    match id {
        "alpha" => "10.0.0.1",
        "beta" => "10.0.0.2",
        "gamma" => "10.0.0.3",
        other => panic!("no fabric IP for {other}"),
    }
}

fn network_peer(id: &str) -> NetworkPeer {
    NetworkPeer {
        id: id.to_string(),
        hostname: format!("truffle-test-{id}"),
        ip: ip_of(id).parse().unwrap(),
        online: true,
        cur_addr: Some(format!("{}:41641", ip_of(id))),
        active: true,
        peer_relay: None,
        relay: None,
        os: None,
        last_seen: None,
        key_expiry: None,
        dns_name: None,
        login_name: None,
    }
}

struct TestNode {
    registry: PeerRegistry<FabricProvider>,
    network: Arc<FabricProvider>,
}

impl TestNode {
    fn dials(&self) -> usize {
        self.network.dials.load(Ordering::SeqCst)
    }

    /// Direction of the peer's primary connection, if any.
    async fn primary(&self, peer: &str) -> Option<ConnectionDirection> {
        self.registry
            .ws_connections
            .read()
            .await
            .get(peer)
            .map(|handle| handle.direction)
    }

    async fn has_standby(&self, peer: &str) -> bool {
        self.registry
            .standby_connections
            .read()
            .await
            .contains_key(peer)
    }

    /// Direction of the peer's parked standby, if any.
    async fn standby(&self, peer: &str) -> Option<ConnectionDirection> {
        self.registry
            .standby_connections
            .read()
            .await
            .get(peer)
            .map(|handle| handle.direction)
    }

    /// Wait until the standby slot for `peer` is empty.
    async fn wait_no_standby(&self, peer: &str, within: Duration) {
        let deadline = Instant::now() + within;
        while self.has_standby(peer).await {
            assert!(
                Instant::now() < deadline,
                "the standby for {peer} was still parked after {within:?}"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    async fn ws_connected(&self, peer: &str) -> Option<bool> {
        self.registry
            .peers
            .read()
            .await
            .get(peer)
            .map(|state| state.ws_connected)
    }
}

/// The `WsConnected` / `WsDisconnected` events for `peer` received so far.
fn connection_events(rx: &mut broadcast::Receiver<PeerEvent>, peer: &str) -> Vec<&'static str> {
    let mut seen = Vec::new();
    while let Ok(event) = rx.try_recv() {
        match event {
            PeerEvent::WsConnected(id) if id == peer => seen.push("connected"),
            PeerEvent::WsDisconnected(id) if id == peer => seen.push("disconnected"),
            _ => {}
        }
    }
    seen
}

struct NodeOpts {
    standby_grace: Duration,
    dial_delay: Duration,
    fail_dials: Option<String>,
}

impl Default for NodeOpts {
    fn default() -> Self {
        Self {
            standby_grace: Duration::from_millis(400),
            dial_delay: Duration::ZERO,
            fail_dials: None,
        }
    }
}

/// Start a node on `fabric` that already knows `peers` (Layer 3 Joined).
async fn start_node(fabric: &Arc<Fabric>, id: &str, peers: &[&str], opts: NodeOpts) -> TestNode {
    let mut provider = FabricProvider::new(fabric, id, ip_of(id));
    provider.dial_delay = opts.dial_delay;
    provider.fail_dials = opts.fail_dials;
    let events = provider.peer_event_tx.clone();
    let network = Arc::new(provider);
    let ws_config = WsConfig {
        port: 9417,
        // No heartbeat during a test: a dead link must stay undetected
        // unless the session itself works it out.
        ping_interval: Duration::from_secs(300),
        pong_timeout: Duration::from_secs(300),
        ..Default::default()
    };
    let ws_transport = Arc::new(WebSocketTransport::new(network.clone(), ws_config));
    let mut registry = PeerRegistry::with_options(
        network.clone(),
        ws_transport,
        PeerRegistryOptions {
            eager_identity: false,
            eager_identity_jitter_ms: 0,
            ..Default::default()
        },
    );
    registry.set_standby_grace(opts.standby_grace);
    registry.start().await;
    for peer in peers {
        events
            .send(NetworkPeerEvent::Joined(network_peer(peer)))
            .unwrap();
    }
    // Let the event loop register the peers.
    let deadline = Instant::now() + Duration::from_secs(2);
    while registry.peers().await.len() < peers.len() {
        assert!(Instant::now() < deadline, "peers never registered");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    TestNode { registry, network }
}

/// Wait for the next message on `rx` whose payload is `want`.
async fn expect_message(
    rx: &mut broadcast::Receiver<super::IncomingMessage>,
    want: &[u8],
    within: Duration,
) -> Duration {
    let started = Instant::now();
    tokio::time::timeout(within, async {
        loop {
            let msg = rx.recv().await.expect("incoming channel open");
            if msg.data == want {
                return;
            }
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "message {:?} not delivered within {within:?}",
            String::from_utf8_lossy(want)
        )
    });
    started.elapsed()
}

/// Kill `id` the way `kill -9` does behind tsnet: freeze its links (the
/// survivor's sockets stay open and silent), then drop its session.
async fn kill_dash_nine(fabric: &Fabric, node: TestNode, id: &str) {
    fabric.freeze(ip_of(id));
    node.registry.shutdown().await;
}

// ---------------------------------------------------------------------------
// install_decision — the pure tie-break
// ---------------------------------------------------------------------------

#[test]
fn install_decision_covers_every_case() {
    use ConnectionDirection::{Inbound, Outbound};
    let young = Duration::from_millis(20);
    let old = CROSS_DIAL_WINDOW + Duration::from_secs(1);

    // Nothing installed: anything goes in.
    for candidate in [Inbound, Outbound] {
        for preferred in [Some(Inbound), Some(Outbound), None] {
            assert_eq!(
                install_decision(None, candidate, preferred),
                InstallDecision::Install
            );
        }
    }

    // Same dialer again: it abandoned the first connection — the new one
    // wins whatever the preference (this used to be refused).
    for dir in [Inbound, Outbound] {
        for preferred in [Some(Inbound), Some(Outbound), None] {
            assert_eq!(
                install_decision(Some((dir, young)), dir, preferred),
                InstallDecision::Install,
                "same-direction {dir:?}, preferred {preferred:?}"
            );
        }
    }

    // Cross-dial, candidate preferred: replace.
    assert_eq!(
        install_decision(Some((Outbound, young)), Inbound, Some(Inbound)),
        InstallDecision::Install
    );
    assert_eq!(
        install_decision(Some((Inbound, young)), Outbound, Some(Outbound)),
        InstallDecision::Install
    );

    // Cross-dial, existing preferred, our own dial lost: refuse.
    assert_eq!(
        install_decision(Some((Inbound, young)), Outbound, Some(Inbound)),
        InstallDecision::Refuse
    );
    assert_eq!(
        install_decision(Some((Inbound, old)), Outbound, Some(Inbound)),
        InstallDecision::Refuse
    );

    // Cross-dial, existing preferred, the peer's dial: a young existing
    // connection may have a twin — park; an old one cannot — replace.
    assert_eq!(
        install_decision(Some((Outbound, young)), Inbound, Some(Outbound)),
        InstallDecision::Standby
    );
    assert_eq!(
        install_decision(Some((Outbound, old)), Inbound, Some(Outbound)),
        InstallDecision::Install
    );
    assert_eq!(
        install_decision(Some((Outbound, young)), Inbound, None),
        InstallDecision::Standby
    );
}

// ---------------------------------------------------------------------------
// Issue 1 — a peer restart must not leave the survivor on a dead connection
// ---------------------------------------------------------------------------

/// Survivor alpha dialed (alpha < beta, so alpha's outbound is the
/// preferred cross-dial side). beta is killed and restarts with the same
/// Tailscale id; its first send must get through at once, and alpha's
/// replies must reach the new instance — not the dead socket.
///
/// Before the fix alpha refused beta's new connection as a "duplicate" of
/// the dead preferred one, beta's send failed with "connection missing
/// after connect", and alpha kept writing into the void until a heartbeat
/// (10–40 s) cleared the dead link.
#[tokio::test]
async fn restarted_peer_replaces_preferred_stale_connection() {
    let fabric = Arc::new(Fabric::default());
    let alpha = start_node(&fabric, "alpha", &["beta"], NodeOpts::default()).await;
    let beta = start_node(&fabric, "beta", &["alpha"], NodeOpts::default()).await;

    let mut beta_rx = beta.registry.subscribe();
    alpha.registry.send("beta", b"hello-1").await.unwrap();
    expect_message(&mut beta_rx, b"hello-1", Duration::from_secs(2)).await;
    assert_eq!(
        alpha.primary("beta").await,
        Some(ConnectionDirection::Outbound)
    );

    kill_dash_nine(&fabric, beta, "beta").await;
    let beta2 = start_node(&fabric, "beta", &["alpha"], NodeOpts::default()).await;

    let mut alpha_rx = alpha.registry.subscribe();
    let mut beta2_rx = beta2.registry.subscribe();
    let started = Instant::now();
    beta2
        .registry
        .send("alpha", b"i-restarted")
        .await
        .expect("the restarted peer's first send must not be refused");
    let delivered = expect_message(&mut alpha_rx, b"i-restarted", Duration::from_secs(2)).await;

    // alpha's reply reaches the NEW instance: the stale link is gone.
    alpha.registry.send("beta", b"welcome-back").await.unwrap();
    expect_message(&mut beta2_rx, b"welcome-back", Duration::from_secs(2)).await;
    let elapsed = started.elapsed();
    eprintln!("[restart/preferred-stale] first message {delivered:?}, round trip {elapsed:?}");
    assert!(
        elapsed < Duration::from_millis(400),
        "recovery took {elapsed:?}; it must not wait for the standby grace period"
    );
    assert_eq!(
        alpha.primary("beta").await,
        Some(ConnectionDirection::Inbound)
    );
    // The stale connection was demoted, not closed (in a crossed cross-dial
    // the peer may still hold it); nobody hands it back, so it is closed
    // once the grace period runs out.
    assert_eq!(
        alpha.standby("beta").await,
        Some(ConnectionDirection::Outbound)
    );
    alpha
        .wait_no_standby("beta", NodeOpts::default().standby_grace * 3)
        .await;
    assert_eq!(
        alpha.primary("beta").await,
        Some(ConnectionDirection::Inbound)
    );
}

/// Same, but the stale connection was dialed by beta's OLD instance (same
/// direction as the new one). Before the fix the survivor kept the old one
/// whenever it was the preferred direction.
#[tokio::test]
async fn restarted_peer_replaces_same_direction_stale_connection() {
    let fabric = Arc::new(Fabric::default());
    // beta survives: the larger id prefers INBOUND, so the old code kept the
    // stale inbound connection (preferred == existing) and refused the new
    // one.
    let beta = start_node(&fabric, "beta", &["alpha"], NodeOpts::default()).await;
    let alpha = start_node(&fabric, "alpha", &["beta"], NodeOpts::default()).await;

    let mut beta_rx = beta.registry.subscribe();
    alpha.registry.send("beta", b"hello-1").await.unwrap();
    expect_message(&mut beta_rx, b"hello-1", Duration::from_secs(2)).await;
    assert_eq!(
        beta.primary("alpha").await,
        Some(ConnectionDirection::Inbound)
    );

    kill_dash_nine(&fabric, alpha, "alpha").await;
    let alpha2 = start_node(&fabric, "alpha", &["beta"], NodeOpts::default()).await;

    let mut alpha2_rx = alpha2.registry.subscribe();
    let started = Instant::now();
    alpha2
        .registry
        .send("beta", b"i-restarted")
        .await
        .expect("the restarted peer's first send must not be refused");
    expect_message(&mut beta_rx, b"i-restarted", Duration::from_secs(2)).await;
    beta.registry.send("alpha", b"welcome-back").await.unwrap();
    expect_message(&mut alpha2_rx, b"welcome-back", Duration::from_secs(2)).await;
    let elapsed = started.elapsed();
    eprintln!("[restart/same-direction] round trip {elapsed:?}");
    assert!(elapsed < Duration::from_millis(400), "took {elapsed:?}");
    assert!(!beta.has_standby("alpha").await);
}

/// The restarted peer only connects (an eager identity hello) and sends
/// nothing, so nothing proves it dropped the old connection — the standby
/// is promoted after the grace period, and the survivor's sends then reach
/// the new instance.
#[tokio::test]
async fn silent_restarted_peer_is_promoted_after_the_grace_period() {
    let fabric = Arc::new(Fabric::default());
    let grace = Duration::from_millis(300);
    let opts = || NodeOpts {
        standby_grace: grace,
        ..NodeOpts::default()
    };
    let alpha = start_node(&fabric, "alpha", &["beta"], opts()).await;
    let beta = start_node(&fabric, "beta", &["alpha"], opts()).await;

    alpha.registry.ensure_ws_connected("beta").await.unwrap();
    kill_dash_nine(&fabric, beta, "beta").await;
    let beta2 = start_node(&fabric, "beta", &["alpha"], opts()).await;

    let mut events = alpha.registry.on_peer_change();
    let started = Instant::now();
    beta2.registry.ensure_ws_connected("alpha").await.unwrap();

    // Parked first (the old link is young: it could be a cross-dial)...
    let deadline = Instant::now() + Duration::from_secs(1);
    while !alpha.has_standby("beta").await {
        assert!(
            Instant::now() < deadline,
            "the new connection was not parked"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    // ...then promoted, announced with WsConnected.
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Ok(PeerEvent::WsConnected(id)) = events.recv().await {
                if id == "beta" {
                    return;
                }
            }
        }
    })
    .await
    .expect("promotion must emit WsConnected");
    let promoted_after = started.elapsed();
    assert!(
        promoted_after >= grace,
        "promoted too early: {promoted_after:?}"
    );
    // The stale primary is demoted, then closed after one more grace period.
    alpha.wait_no_standby("beta", grace * 3).await;

    let mut beta2_rx = beta2.registry.subscribe();
    alpha
        .registry
        .send("beta", b"after-promotion")
        .await
        .unwrap();
    expect_message(&mut beta2_rx, b"after-promotion", Duration::from_secs(2)).await;
    eprintln!("[restart/silent] promoted after {promoted_after:?} (grace {grace:?})");
}

/// When the stale connection dies (here: the peer's RST finally arrives)
/// while a standby is parked, the standby takes its slot instead of the
/// peer being reported disconnected.
#[tokio::test]
async fn standby_takes_over_when_the_primary_dies() {
    let fabric = Arc::new(Fabric::default());
    let opts = || NodeOpts {
        standby_grace: Duration::from_secs(30),
        ..NodeOpts::default()
    };
    let alpha = start_node(&fabric, "alpha", &["beta"], opts()).await;
    let beta = start_node(&fabric, "beta", &["alpha"], opts()).await;
    alpha.registry.ensure_ws_connected("beta").await.unwrap();

    // Make the old link closable later: freeze nothing, just park a second
    // connection from beta as a standby by having beta "forget" the first.
    let first = beta
        .registry
        .ws_connections
        .write()
        .await
        .remove("alpha")
        .expect("beta holds the inbound side");
    beta.registry.ensure_ws_connected("alpha").await.unwrap();
    let deadline = Instant::now() + Duration::from_secs(1);
    while !alpha.has_standby("beta").await {
        assert!(
            Instant::now() < deadline,
            "the second connection was not parked"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let mut events = alpha.registry.on_peer_change();
    // The old connection now dies (beta closes the socket it forgot).
    let _ = first.close_tx.send(()).await;
    let event = tokio::time::timeout(Duration::from_secs(2), events.recv())
        .await
        .expect("an event")
        .expect("open");
    assert!(
        matches!(&event, PeerEvent::WsConnected(id) if id == "beta"),
        "the standby must take over (WsConnected), got {event:?}"
    );
    assert_eq!(
        alpha.primary("beta").await,
        Some(ConnectionDirection::Inbound)
    );
    assert!(alpha
        .registry
        .peers()
        .await
        .iter()
        .any(|p| p.id == "beta" && p.ws_connected));

    let mut beta_rx = beta.registry.subscribe();
    alpha.registry.send("beta", b"via-standby").await.unwrap();
    expect_message(&mut beta_rx, b"via-standby", Duration::from_secs(2)).await;
}

/// The tie-break still holds for genuine cross-dials: both sides dial at
/// once, and every time they settle on ONE physical connection (dialed by
/// the smaller id), with no standby left and traffic flowing both ways.
#[tokio::test]
async fn simultaneous_cross_dials_settle_on_one_connection() {
    for round in 0..25 {
        let fabric = Arc::new(Fabric::default());
        let opts = || NodeOpts {
            standby_grace: Duration::from_millis(500),
            ..NodeOpts::default()
        };
        let alpha = start_node(&fabric, "alpha", &["beta"], opts()).await;
        let beta = start_node(&fabric, "beta", &["alpha"], opts()).await;

        let (a, b) = tokio::join!(
            alpha.registry.ensure_ws_connected("beta"),
            beta.registry.ensure_ws_connected("alpha"),
        );
        a.unwrap();
        b.unwrap();

        // Let refusals land (well inside the grace period).
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            alpha.primary("beta").await,
            Some(ConnectionDirection::Outbound),
            "round {round}: alpha (smaller id) keeps its outbound side"
        );
        assert_eq!(
            beta.primary("alpha").await,
            Some(ConnectionDirection::Inbound),
            "round {round}: beta keeps the matching inbound side"
        );
        assert!(!alpha.has_standby("beta").await, "round {round}");
        assert!(!beta.has_standby("alpha").await, "round {round}");

        // Past the grace period nothing changes (no late promotion).
        tokio::time::sleep(Duration::from_millis(600)).await;
        assert_eq!(
            alpha.primary("beta").await,
            Some(ConnectionDirection::Outbound),
            "round {round}: no promotion after the grace period"
        );

        let mut alpha_rx = alpha.registry.subscribe();
        let mut beta_rx = beta.registry.subscribe();
        alpha.registry.send("beta", b"a->b").await.unwrap();
        beta.registry.send("alpha", b"b->a").await.unwrap();
        expect_message(&mut beta_rx, b"a->b", Duration::from_secs(2)).await;
        expect_message(&mut alpha_rx, b"b->a", Duration::from_secs(2)).await;
        alpha.registry.shutdown().await;
        beta.registry.shutdown().await;
    }
}

// ---------------------------------------------------------------------------
// Issue 2 — concurrent callers share the dial in flight
// ---------------------------------------------------------------------------

/// Concurrent sends while a slow dial (1.5 s, like a first WireGuard
/// handshake) is in flight all wait for it and succeed — one dial, every
/// message delivered. Before the fix every caller but the first gave up
/// after ~1 s with ConnectFailed("connection already in progress").
#[tokio::test]
async fn concurrent_sends_share_a_slow_dial() {
    let fabric = Arc::new(Fabric::default());
    let beta = start_node(&fabric, "beta", &["alpha"], NodeOpts::default()).await;
    let alpha = start_node(
        &fabric,
        "alpha",
        &["beta"],
        NodeOpts {
            dial_delay: Duration::from_millis(1500),
            ..NodeOpts::default()
        },
    )
    .await;

    let mut beta_rx = beta.registry.subscribe();
    let started = Instant::now();
    let sends = (0..8).map(|i| {
        let registry = &alpha.registry;
        async move {
            let payload = format!("msg-{i}");
            registry.send("beta", payload.as_bytes()).await
        }
    });
    let results = futures_util::future::join_all(sends).await;
    let elapsed = started.elapsed();
    for (i, result) in results.into_iter().enumerate() {
        result.unwrap_or_else(|e| panic!("send {i} failed: {e}"));
    }
    eprintln!("[shared-dial] 8 concurrent sends over one 1.5 s dial took {elapsed:?}");
    assert_eq!(alpha.dials(), 1, "one dial serves every caller");
    assert!(elapsed < Duration::from_millis(2500), "took {elapsed:?}");

    let mut seen = std::collections::HashSet::new();
    tokio::time::timeout(Duration::from_secs(2), async {
        while seen.len() < 8 {
            let msg = beta_rx.recv().await.unwrap();
            seen.insert(msg.data);
        }
    })
    .await
    .expect("all 8 messages delivered");
}

/// A failed dial's error reaches every caller waiting on it — the dial's
/// own error, not "already in progress".
#[tokio::test]
async fn waiters_get_the_dials_own_error() {
    let fabric = Arc::new(Fabric::default());
    let alpha = start_node(
        &fabric,
        "alpha",
        &["beta"],
        NodeOpts {
            dial_delay: Duration::from_millis(1200),
            fail_dials: Some("no route to beta".into()),
            ..NodeOpts::default()
        },
    )
    .await;

    let results =
        futures_util::future::join_all((0..4).map(|_| alpha.registry.ensure_ws_connected("beta")))
            .await;
    assert_eq!(alpha.dials(), 1);
    for result in results {
        match result {
            Err(SessionError::ConnectFailed(message)) => assert!(
                message.contains("no route to beta"),
                "expected the dial's error, got {message:?}"
            ),
            other => panic!("expected ConnectFailed, got {other:?}"),
        }
    }
    // The failure arms the backoff for the NEXT dial as before.
    assert!(matches!(
        alpha.registry.ensure_ws_connected("beta").await,
        Err(SessionError::ReconnectBackoff { .. })
    ));
}

/// A caller that gives up (its own timeout) does not cancel the dial: it
/// completes in the background and the next send reuses it.
#[tokio::test]
async fn an_abandoned_dial_still_completes() {
    let fabric = Arc::new(Fabric::default());
    let beta = start_node(&fabric, "beta", &["alpha"], NodeOpts::default()).await;
    let alpha = start_node(
        &fabric,
        "alpha",
        &["beta"],
        NodeOpts {
            dial_delay: Duration::from_millis(500),
            ..NodeOpts::default()
        },
    )
    .await;

    let gave_up = tokio::time::timeout(
        Duration::from_millis(100),
        alpha.registry.send("beta", b"impatient"),
    )
    .await;
    assert!(gave_up.is_err(), "the caller's own deadline fires first");

    let deadline = Instant::now() + Duration::from_secs(2);
    while alpha.primary("beta").await.is_none() {
        assert!(Instant::now() < deadline, "the abandoned dial never landed");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let mut beta_rx = beta.registry.subscribe();
    alpha.registry.send("beta", b"patient").await.unwrap();
    expect_message(&mut beta_rx, b"patient", Duration::from_secs(1)).await;
    assert_eq!(alpha.dials(), 1, "the second send reused the first dial");
}

/// Shutdown aborts a dial in flight and wakes its waiters at once.
#[tokio::test]
async fn shutdown_cancels_waiters_of_an_inflight_dial() {
    let fabric = Arc::new(Fabric::default());
    let alpha = Arc::new(
        start_node(
            &fabric,
            "alpha",
            &["beta"],
            NodeOpts {
                dial_delay: Duration::from_secs(30),
                ..NodeOpts::default()
            },
        )
        .await,
    );
    let waiter = {
        let alpha = alpha.clone();
        tokio::spawn(async move { alpha.registry.ensure_ws_connected("beta").await })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    alpha.registry.shutdown().await;
    let result = tokio::time::timeout(Duration::from_secs(1), waiter)
        .await
        .expect("waiter woke")
        .expect("joined");
    match result {
        Err(SessionError::ConnectFailed(message)) => assert!(message.contains("cancelled")),
        other => panic!("expected a cancelled dial, got {other:?}"),
    }
    assert!(alpha.registry.dials.lock().unwrap().is_empty());
}

/// A standby replaced by a newer one (the peer re-dialed again) must not
/// take the newer one down when its own grace timer fires: only the
/// connection a timer was armed for may be promoted.
#[tokio::test]
async fn a_replaced_standbys_timer_leaves_the_newer_standby_alone() {
    let fabric = Arc::new(Fabric::default());
    let grace = Duration::from_millis(400);
    let opts = || NodeOpts {
        standby_grace: grace,
        ..NodeOpts::default()
    };
    let alpha = start_node(&fabric, "alpha", &["beta"], opts()).await;
    let beta = start_node(&fabric, "beta", &["alpha"], opts()).await;
    alpha.registry.ensure_ws_connected("beta").await.unwrap();

    // beta "loses" its connections without closing them (the handles are
    // held, so the sockets stay open) and re-dials twice.
    let forget = |node: &TestNode| {
        let conns = node.registry.ws_connections.clone();
        async move { conns.write().await.remove("alpha").expect("beta holds one") }
    };
    let _first = forget(&beta).await;
    beta.registry.ensure_ws_connected("alpha").await.unwrap();
    let parked_at = Instant::now();
    let deadline = parked_at + Duration::from_secs(1);
    while !alpha.has_standby("beta").await {
        assert!(
            Instant::now() < deadline,
            "the first re-dial was not parked"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    tokio::time::sleep(Duration::from_millis(250)).await;
    let _second = forget(&beta).await;
    beta.registry.ensure_ws_connected("alpha").await.unwrap();
    let newer = {
        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            let standby = alpha.registry.standby_connections.read().await;
            if let Some(handle) = standby.get("beta") {
                if handle.connected_at > parked_at + Duration::from_millis(200) {
                    break handle.send_tx.clone();
                }
            }
            drop(standby);
            assert!(
                Instant::now() < deadline,
                "the second re-dial was not parked"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    };

    // Past the FIRST standby's timer, before the second's: the newer
    // standby is still parked (the old timer used to evict and close it).
    let first_timer = parked_at + grace + Duration::from_millis(60);
    tokio::time::sleep_until(first_timer.into()).await;
    {
        let standby = alpha.registry.standby_connections.read().await;
        assert!(
            standby
                .get("beta")
                .is_some_and(|handle| handle.send_tx.same_channel(&newer)),
            "the first standby's timer evicted the newer standby"
        );
    }

    // Its own timer then promotes it.
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let promoted = alpha
            .registry
            .ws_connections
            .read()
            .await
            .get("beta")
            .is_some_and(|handle| handle.send_tx.same_channel(&newer));
        if promoted {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the newer standby was never promoted"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// A peer Layer 3 has not announced yet (a restarted ephemeral node comes
/// back under a new Tailscale id, and its hello can beat the netmap by
/// seconds) can be answered on the connection it opened. Before, the reply
/// failed with UnknownPeer until discovery caught up — the hub's "hello
/// timeout" after a server restart.
#[tokio::test]
async fn a_reply_reaches_a_peer_layer3_has_not_announced() {
    let fabric = Arc::new(Fabric::default());
    // beta's Layer 3 has not announced alpha.
    let beta = start_node(&fabric, "beta", &[], NodeOpts::default()).await;
    let alpha = start_node(&fabric, "alpha", &["beta"], NodeOpts::default()).await;

    let mut beta_rx = beta.registry.subscribe();
    let mut alpha_rx = alpha.registry.subscribe();
    alpha.registry.send("beta", b"hello").await.unwrap();
    let msg = tokio::time::timeout(Duration::from_secs(2), beta_rx.recv())
        .await
        .expect("delivered")
        .expect("open");
    assert_eq!(msg.from, "alpha");
    assert!(beta.registry.peers().await.is_empty(), "still undiscovered");

    beta.registry
        .send(&msg.from, b"hello.ack")
        .await
        .expect("the reply rides the peer's own connection");
    expect_message(&mut alpha_rx, b"hello.ack", Duration::from_secs(2)).await;

    // Nothing is dialed for an id without a connection.
    assert!(matches!(
        beta.registry.send("gamma", b"x").await,
        Err(SessionError::UnknownPeer(_))
    ));
}

// ---------------------------------------------------------------------------
// Review findings — crossed cross-dials, late bookkeeping, departed peers
// ---------------------------------------------------------------------------

/// A cross-dial whose installs land CROSSED: each side installs its own
/// dial first (each accept loop lags), and the larger id (beta) sends on
/// its own dial Y before the preferred twin X reaches its accept loop. alpha
/// parks Y behind X and then sees beta's data on it.
///
/// Promote-on-data used to CLOSE X there, while beta, installing the
/// preferred X, closed Y: both sockets torn down, `WsDisconnected` on both
/// sides, sends lost. Now alpha demotes X instead, and beta's close of Y
/// hands it back: both sides end on X with no disconnect.
///
/// The accept lag is forced by stalling each node's serial accept loop
/// behind a gamma connection whose bookkeeping waits on
/// `pending_identities`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crossed_cross_dial_with_data_on_the_twin_keeps_one_connection() {
    for round in 0..3 {
        let fabric = Arc::new(Fabric::default());
        let slow = || NodeOpts {
            standby_grace: Duration::from_secs(10),
            dial_delay: Duration::from_millis(300),
            ..NodeOpts::default()
        };
        let alpha = start_node(&fabric, "alpha", &["beta", "gamma"], slow()).await;
        let beta = start_node(&fabric, "beta", &["alpha", "gamma"], slow()).await;
        let gamma = start_node(&fabric, "gamma", &["alpha", "beta"], NodeOpts::default()).await;
        let mut a_events = alpha.registry.on_peer_change();
        let mut b_events = beta.registry.on_peer_change();

        // Both dial each other (each dial waits 300 ms before connecting).
        let dials = async {
            tokio::join!(
                alpha.registry.ensure_ws_connected("beta"),
                beta.registry.ensure_ws_connected("alpha")
            )
        };
        let driver = async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            // Stall both accept loops behind gamma.
            let a_stall = alpha.registry.pending_identities.write().await;
            let b_stall = beta.registry.pending_identities.write().await;
            gamma.registry.ensure_ws_connected("alpha").await.unwrap();
            gamma.registry.ensure_ws_connected("beta").await.unwrap();
            // The crossed state: each side's own dial is its primary.
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                let a = alpha.primary("beta").await;
                let b = beta.primary("alpha").await;
                if a == Some(ConnectionDirection::Outbound)
                    && b == Some(ConnectionDirection::Outbound)
                {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "round {round}: no crossed state: alpha={a:?} beta={b:?}"
                );
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            // beta sends on its primary — its own dial, Y.
            let y_tx = beta
                .registry
                .ws_connections
                .read()
                .await
                .get("alpha")
                .expect("beta's own dial is installed")
                .send_tx
                .clone();
            y_tx.send(b"b-on-Y".to_vec()).await.unwrap();
            tokio::time::sleep(Duration::from_millis(20)).await;
            drop(a_stall);
            drop(b_stall);
        };
        let ((a, b), ()) = tokio::join!(dials, driver);
        a.unwrap();
        b.unwrap();

        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(
            alpha.primary("beta").await,
            Some(ConnectionDirection::Outbound),
            "round {round}: alpha must end on its own dial X"
        );
        assert_eq!(
            beta.primary("alpha").await,
            Some(ConnectionDirection::Inbound),
            "round {round}: beta must end on X too"
        );
        assert!(!alpha.has_standby("beta").await, "round {round}");
        assert!(!beta.has_standby("alpha").await, "round {round}");
        let a_seen = connection_events(&mut a_events, "beta");
        let b_seen = connection_events(&mut b_events, "alpha");
        assert!(
            !a_seen.contains(&"disconnected") && !b_seen.contains(&"disconnected"),
            "round {round}: a connection was torn down: alpha {a_seen:?}, beta {b_seen:?}"
        );
        assert_eq!(
            alpha.ws_connected("beta").await,
            Some(true),
            "round {round}"
        );
        assert_eq!(
            beta.ws_connected("alpha").await,
            Some(true),
            "round {round}"
        );

        let mut alpha_rx = alpha.registry.subscribe();
        let mut beta_rx = beta.registry.subscribe();
        alpha.registry.send("beta", b"a->b").await.unwrap();
        beta.registry.send("alpha", b"b->a").await.unwrap();
        expect_message(&mut beta_rx, b"a->b", Duration::from_secs(2)).await;
        expect_message(&mut alpha_rx, b"b->a", Duration::from_secs(2)).await;
        assert_eq!(alpha.dials(), 1, "round {round}: nothing redialed");
        assert_eq!(beta.dials(), 1, "round {round}: nothing redialed");
        for node in [&alpha, &beta, &gamma] {
            node.registry.shutdown().await;
        }
    }
}

/// A hand-over (the primary X ended, the standby Y took its slot) whose
/// bookkeeping runs only after Y has died too must not mark the peer
/// connected with nothing in the table. Before, `on_primary_installed` set
/// `ws_connected = true` and emitted `WsConnected` after Y's retire had
/// already reported the disconnect.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn late_handover_bookkeeping_does_not_resurrect_a_dead_connection() {
    use crate::transport::StreamTransport;
    let fabric = Arc::new(Fabric::default());
    let opts = || NodeOpts {
        standby_grace: Duration::from_secs(10),
        ..NodeOpts::default()
    };
    let alpha = start_node(&fabric, "alpha", &["beta"], opts()).await;
    let beta = start_node(&fabric, "beta", &["alpha"], opts()).await;

    // X: alpha's outbound primary. Y: a raw second connection from beta,
    // outside beta's session, that alpha parks as a standby.
    alpha.registry.ensure_ws_connected("beta").await.unwrap();
    let y = beta
        .registry
        .ws_transport
        .connect(&PeerAddr {
            ip: Some(ip_of("alpha").parse().unwrap()),
            hostname: "truffle-test-alpha".into(),
            dns_name: None,
        })
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    while !alpha.has_standby("beta").await {
        assert!(Instant::now() < deadline, "Y never parked");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let mut events = alpha.registry.on_peer_change();
    let stall = alpha.registry.pending_identities.write().await;
    // X ends: Y takes the slot, its bookkeeping waits on the stall.
    beta.registry.disconnect("alpha").await;
    let deadline = Instant::now() + Duration::from_secs(2);
    while alpha.primary("beta").await != Some(ConnectionDirection::Inbound)
        || alpha.has_standby("beta").await
    {
        assert!(Instant::now() < deadline, "Y never took over");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    // Y ends too, before that bookkeeping runs.
    drop(y);
    let deadline = Instant::now() + Duration::from_secs(2);
    while alpha.primary("beta").await.is_some() {
        assert!(Instant::now() < deadline, "Y never retired");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    drop(stall);
    tokio::time::sleep(Duration::from_millis(150)).await;

    assert_eq!(alpha.primary("beta").await, None);
    assert_eq!(alpha.ws_connected("beta").await, Some(false));
    assert_eq!(connection_events(&mut events, "beta"), ["disconnected"]);
}

/// The grace-timer variant: the timer promotes the standby Y (demoting X),
/// and both connections die before the promotion's bookkeeping runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn late_timer_promotion_bookkeeping_does_not_resurrect_a_dead_connection() {
    use crate::transport::StreamTransport;
    let fabric = Arc::new(Fabric::default());
    let grace = Duration::from_millis(300);
    let opts = || NodeOpts {
        standby_grace: grace,
        ..NodeOpts::default()
    };
    let alpha = start_node(&fabric, "alpha", &["beta"], opts()).await;
    let beta = start_node(&fabric, "beta", &["alpha"], opts()).await;

    alpha.registry.ensure_ws_connected("beta").await.unwrap();
    let mut events = alpha.registry.on_peer_change();
    let y = beta
        .registry
        .ws_transport
        .connect(&PeerAddr {
            ip: Some(ip_of("alpha").parse().unwrap()),
            hostname: "truffle-test-alpha".into(),
            dns_name: None,
        })
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    while !alpha.has_standby("beta").await {
        assert!(Instant::now() < deadline, "Y never parked");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let stall = alpha.registry.pending_identities.write().await;

    // The grace timer promotes Y and demotes X; the bookkeeping waits.
    let deadline = Instant::now() + grace * 4;
    while alpha.primary("beta").await != Some(ConnectionDirection::Inbound) {
        assert!(Instant::now() < deadline, "Y was never promoted");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(
        alpha.standby("beta").await,
        Some(ConnectionDirection::Outbound),
        "X is demoted, not closed"
    );
    // Both die before the bookkeeping runs.
    beta.registry.disconnect("alpha").await;
    drop(y);
    let deadline = Instant::now() + Duration::from_secs(2);
    while alpha.primary("beta").await.is_some() || alpha.has_standby("beta").await {
        assert!(Instant::now() < deadline, "the connections never retired");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    drop(stall);
    tokio::time::sleep(Duration::from_millis(150)).await;

    assert_eq!(alpha.primary("beta").await, None);
    assert_eq!(alpha.ws_connected("beta").await, Some(false));
    assert_eq!(connection_events(&mut events, "beta"), ["disconnected"]);
}

/// A dial still in flight when Layer 3 reports the peer gone (evicted for a
/// refused login, say) must not be installed: no connection, no identity
/// bound from the answerer's self-declared hello, nothing routable.
#[tokio::test]
async fn a_dial_that_outlives_a_departure_is_not_installed() {
    let fabric = Arc::new(Fabric::default());
    let beta = start_node(&fabric, "beta", &["alpha"], NodeOpts::default()).await;
    let alpha = start_node(
        &fabric,
        "alpha",
        &["beta"],
        NodeOpts {
            dial_delay: Duration::from_millis(300),
            ..NodeOpts::default()
        },
    )
    .await;

    let (result, ()) = tokio::join!(alpha.registry.ensure_ws_connected("beta"), async {
        tokio::time::sleep(Duration::from_millis(50)).await;
        alpha
            .network
            .peer_event_tx
            .send(NetworkPeerEvent::Left("beta".into()))
            .unwrap();
    });
    match result {
        Err(SessionError::ConnectFailed(message)) => assert!(
            message.contains("left during dial"),
            "unexpected dial error {message:?}"
        ),
        other => panic!("the dial must fail, got {other:?}"),
    }
    assert_eq!(alpha.primary("beta").await, None);
    assert_eq!(alpha.registry.published_device_id("beta").await, None);
    assert!(matches!(
        alpha.registry.send("beta", b"x").await,
        Err(SessionError::UnknownPeer(_))
    ));
    // The answerer sees its side closed.
    let deadline = Instant::now() + Duration::from_secs(2);
    while beta.primary("alpha").await.is_some() {
        assert!(
            Instant::now() < deadline,
            "beta still holds the dropped dial"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// An inbound hello verified before Layer 3 reported the peer gone, but
/// installed after (the accept loop was busy), is refused the same way.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_inbound_connection_admitted_before_a_departure_is_refused() {
    let fabric = Arc::new(Fabric::default());
    let alpha = start_node(&fabric, "alpha", &["beta", "gamma"], NodeOpts::default()).await;
    let beta = start_node(&fabric, "beta", &["alpha"], NodeOpts::default()).await;
    let gamma = start_node(&fabric, "gamma", &["alpha"], NodeOpts::default()).await;

    // Stall alpha's accept loop behind gamma's bookkeeping.
    let stall = alpha.registry.pending_identities.write().await;
    gamma.registry.ensure_ws_connected("alpha").await.unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    while alpha.primary("gamma").await.is_none() {
        assert!(Instant::now() < deadline, "gamma never installed");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    // beta's hello is verified by alpha's transport and queued.
    beta.registry.ensure_ws_connected("alpha").await.unwrap();
    // Then Layer 3 says beta left.
    alpha
        .network
        .peer_event_tx
        .send(NetworkPeerEvent::Left("beta".into()))
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    while !alpha.registry.departed.lock().unwrap().contains_key("beta") {
        assert!(Instant::now() < deadline, "the departure was never stamped");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    drop(stall);

    // beta's connection is refused and closed.
    let deadline = Instant::now() + Duration::from_secs(2);
    while beta.primary("alpha").await.is_some() {
        assert!(Instant::now() < deadline, "beta's connection was installed");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(alpha.primary("beta").await, None);
    assert_eq!(alpha.registry.published_device_id("beta").await, None);
    assert!(matches!(
        alpha.registry.send("beta", b"x").await,
        Err(SessionError::UnknownPeer(_))
    ));
    // gamma is unaffected.
    assert_eq!(
        alpha.primary("gamma").await,
        Some(ConnectionDirection::Inbound)
    );
}
