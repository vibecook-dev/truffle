//! Unit tests for Layer 5: Session.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpStream;
use tokio::sync::{broadcast, mpsc};

use crate::network::NetworkProvider;
use crate::network::{
    HealthInfo, IncomingConnection, NetworkError, NetworkPeer, NetworkPeerEvent,
    NetworkTcpListener, NetworkUdpSocket, NodeIdentity, PeerAddr, PingResult,
};
use crate::transport::websocket::WebSocketTransport;
use crate::transport::WsConfig;

use super::reconnect::ReconnectBackoff;
use super::{
    format_peer_ref, parse_peer_ref, preferred_connection_direction, should_replace_connection,
    ConnectionDirection, PeerEvent, PeerIdentity, PeerRegistry, PeerRegistryOptions, SessionError,
};

// ---------------------------------------------------------------------------
// Mock NetworkProvider for session tests
// ---------------------------------------------------------------------------

/// A mock network provider that uses local TCP for testing.
///
/// Provides a `peer_event_tx` handle so tests can inject peer events
/// to simulate Layer 3 discovery.
struct MockNetworkProvider {
    identity: NodeIdentity,
    local_addr: PeerAddr,
    peer_event_tx: broadcast::Sender<NetworkPeerEvent>,
    dial_failures_remaining: AtomicUsize,
}

impl MockNetworkProvider {
    #[allow(dead_code)]
    fn new(id: &str) -> Self {
        Self::new_with_app("test", id)
    }

    fn new_with_app(app_id: &str, id: &str) -> Self {
        let (peer_event_tx, _) = broadcast::channel(64);
        Self {
            identity: NodeIdentity {
                app_id: app_id.to_string(),
                // RFC 022 I1: `device_id` must differ from `tailscale_id` or
                // validate_hello rejects the exchange at the trust boundary,
                // so fixtures prefix it with `dev-`.
                device_id: format!("dev-{id}"),
                device_name: format!("Test Node {id}"),
                tailscale_hostname: format!("truffle-{app_id}-{id}"),
                tailscale_id: id.to_string(),
                dns_name: None,
                ip: Some("127.0.0.1".parse().unwrap()),
                login_name: None,
            },
            local_addr: PeerAddr {
                ip: Some("127.0.0.1".parse().unwrap()),
                hostname: format!("truffle-{app_id}-{id}"),
                dns_name: None,
            },
            peer_event_tx,
            dial_failures_remaining: AtomicUsize::new(0),
        }
    }

    fn with_dial_failures(app_id: &str, id: &str, failures: usize) -> Self {
        let provider = Self::new_with_app(app_id, id);
        provider
            .dial_failures_remaining
            .store(failures, Ordering::Relaxed);
        provider
    }

    /// Get a sender to inject peer events for testing.
    fn event_sender(&self) -> broadcast::Sender<NetworkPeerEvent> {
        self.peer_event_tx.clone()
    }
}

impl NetworkProvider for MockNetworkProvider {
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
        self.local_addr.clone()
    }

    fn peer_events(&self) -> broadcast::Receiver<NetworkPeerEvent> {
        self.peer_event_tx.subscribe()
    }

    async fn peers(&self) -> Vec<NetworkPeer> {
        vec![]
    }

    async fn dial_tcp(&self, addr: &str, port: u16) -> Result<TcpStream, NetworkError> {
        if self
            .dial_failures_remaining
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            return Err(NetworkError::DialFailed(
                "mock transient dial failure".to_string(),
            ));
        }
        let target = format!("{addr}:{port}");
        TcpStream::connect(&target)
            .await
            .map_err(|e| NetworkError::DialFailed(format!("mock dial {target}: {e}")))
    }

    async fn listen_tcp(&self, port: u16) -> Result<NetworkTcpListener, NetworkError> {
        let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{port}"))
            .await
            .map_err(|e| NetworkError::ListenFailed(format!("mock listen :{port}: {e}")))?;

        let actual_port = listener.local_addr().unwrap().port();
        let (tx, rx) = mpsc::channel::<IncomingConnection>(64);

        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, addr)) => {
                        let conn = IncomingConnection {
                            stream,
                            remote_addr: addr.to_string(),
                            remote_identity: String::new(),
                            port: actual_port,
                        };
                        if tx.send(conn).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        tracing::debug!("mock listener error: {e}");
                        break;
                    }
                }
            }
        });

        Ok(NetworkTcpListener {
            port: actual_port,
            incoming: rx,
        })
    }

    async fn unlisten_tcp(&self, _port: u16) -> Result<(), NetworkError> {
        Ok(())
    }

    async fn bind_udp(&self, _port: u16) -> Result<NetworkUdpSocket, NetworkError> {
        Err(NetworkError::Internal("mock: UDP not supported".into()))
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

fn make_network_peer(id: &str, ip: &str) -> NetworkPeer {
    NetworkPeer {
        id: id.to_string(),
        hostname: format!("host-{id}"),
        ip: ip.parse().unwrap(),
        online: true,
        cur_addr: Some(format!("{ip}:41641")),
        relay: None,
        os: Some("linux".to_string()),
        last_seen: Some("2026-03-25T12:00:00Z".to_string()),
        key_expiry: None,
        dns_name: None,
        login_name: None,
    }
}

/// Pick a random available port on localhost.
async fn random_port() -> u16 {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    l.local_addr().unwrap().port()
}

fn ws_config(port: u16) -> WsConfig {
    WsConfig {
        port,
        ping_interval: Duration::from_secs(300), // long for tests
        pong_timeout: Duration::from_secs(300),
        ..Default::default()
    }
}

fn make_loopback_peer(id: &str) -> NetworkPeer {
    NetworkPeer {
        id: id.to_string(),
        hostname: format!("truffle-test-{id}"),
        ip: "127.0.0.1".parse().unwrap(),
        online: true,
        cur_addr: Some("127.0.0.1:41641".to_string()),
        relay: None,
        os: None,
        last_seen: None,
        key_expiry: None,
        dns_name: None,
        login_name: None,
    }
}

/// A loopback peer carrying (or withholding) a Layer 3 login — RFC 025 §3.3.
fn make_loopback_peer_with_login(id: &str, login: Option<&str>) -> NetworkPeer {
    NetworkPeer {
        login_name: login.map(str::to_string),
        ..make_loopback_peer(id)
    }
}

/// A registry that is login-gated (a node built with a non-empty allow-list).
fn build_gated_registry(
    id: &str,
    port: u16,
) -> (
    PeerRegistry<MockNetworkProvider>,
    broadcast::Sender<NetworkPeerEvent>,
) {
    build_registry_with_options(
        "test",
        id,
        port,
        PeerRegistryOptions {
            login_gated: true,
            eager_identity: false,
            eager_identity_jitter_ms: 0,
            ..Default::default()
        },
    )
}

/// A login-gated registry with EAGER identity on, to witness that the
/// background dial inherits the gate (RFC 025 §3.3).
fn build_gated_eager_registry(
    id: &str,
    port: u16,
) -> (
    PeerRegistry<MockNetworkProvider>,
    broadcast::Sender<NetworkPeerEvent>,
) {
    build_registry_with_options(
        "test",
        id,
        port,
        PeerRegistryOptions {
            login_gated: true,
            eager_identity: true,
            eager_identity_jitter_ms: 0,
            ..Default::default()
        },
    )
}

/// Build a PeerRegistry. Returns (registry, event_sender).
/// The registry uses the given port for its WS transport.
fn build_registry(
    id: &str,
    port: u16,
) -> (
    PeerRegistry<MockNetworkProvider>,
    broadcast::Sender<NetworkPeerEvent>,
) {
    build_registry_with_app("test", id, port)
}

/// Variant of [`build_registry`] that lets the caller pick the `app_id`.
/// Used by RFC 017 Phase 2 hello-exchange tests to build nodes that
/// disagree on application namespace.
fn build_registry_with_app(
    app_id: &str,
    id: &str,
    port: u16,
) -> (
    PeerRegistry<MockNetworkProvider>,
    broadcast::Sender<NetworkPeerEvent>,
) {
    build_registry_with_options(app_id, id, port, PeerRegistryOptions::default())
}

fn build_registry_with_options(
    app_id: &str,
    id: &str,
    port: u16,
    options: PeerRegistryOptions,
) -> (
    PeerRegistry<MockNetworkProvider>,
    broadcast::Sender<NetworkPeerEvent>,
) {
    let provider = MockNetworkProvider::new_with_app(app_id, id);
    let event_sender = provider.event_sender();
    let network = Arc::new(provider);
    let ws_transport = Arc::new(WebSocketTransport::new(network.clone(), ws_config(port)));
    let registry = PeerRegistry::with_options(network, ws_transport, options);
    (registry, event_sender)
}

fn build_registry_with_dial_failures(
    id: &str,
    port: u16,
    failures: usize,
) -> (
    PeerRegistry<MockNetworkProvider>,
    broadcast::Sender<NetworkPeerEvent>,
) {
    let provider = MockNetworkProvider::with_dial_failures("test", id, failures);
    let event_sender = provider.event_sender();
    let network = Arc::new(provider);
    let ws_transport = Arc::new(WebSocketTransport::new(network.clone(), ws_config(port)));
    let registry = PeerRegistry::with_options(
        network,
        ws_transport,
        PeerRegistryOptions {
            eager_identity_jitter_ms: 0,
            ..Default::default()
        },
    );
    (registry, event_sender)
}

// ===========================================================================
// Tests: Peer discovery from Layer 3 events
// ===========================================================================

#[tokio::test]
async fn test_peers_from_network_events() {
    let port = random_port().await;
    let (registry, event_sender) = build_registry("node-a", port);
    registry.start().await;

    // Inject peer joined events
    let peer1 = make_network_peer("peer-1", "100.64.0.1");
    let peer2 = make_network_peer("peer-2", "100.64.0.2");
    event_sender.send(NetworkPeerEvent::Joined(peer1)).unwrap();
    event_sender.send(NetworkPeerEvent::Joined(peer2)).unwrap();

    // Give the event loop time to process
    tokio::time::sleep(Duration::from_millis(50)).await;

    let peers = registry.peers().await;
    assert_eq!(peers.len(), 2, "should have 2 peers");

    let ids: Vec<String> = peers.iter().map(|p| p.id.clone()).collect();
    assert!(ids.contains(&"peer-1".to_string()));
    assert!(ids.contains(&"peer-2".to_string()));
}

#[tokio::test]
async fn test_peer_left_removes() {
    let port = random_port().await;
    let (registry, event_sender) = build_registry("node-a", port);
    registry.start().await;

    let peer = make_network_peer("peer-1", "100.64.0.1");
    event_sender.send(NetworkPeerEvent::Joined(peer)).unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(registry.peers().await.len(), 1);

    event_sender
        .send(NetworkPeerEvent::Left("peer-1".to_string()))
        .unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(registry.peers().await.len(), 0, "peer should be removed");
}

#[tokio::test]
async fn test_peers_online_without_connection() {
    let port = random_port().await;
    let (registry, event_sender) = build_registry("node-a", port);
    registry.start().await;

    let peer = make_network_peer("peer-1", "100.64.0.1");
    event_sender.send(NetworkPeerEvent::Joined(peer)).unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    let peers = registry.peers().await;
    assert_eq!(peers.len(), 1);

    let p = &peers[0];
    assert_eq!(p.id, "peer-1");
    assert!(p.online, "peer should be online");
    assert!(
        !p.ws_connected,
        "peer should NOT be ws_connected (no WS yet)"
    );
}

#[tokio::test]
async fn test_peer_updated_preserves_connected() {
    let port = random_port().await;
    let (registry, event_sender) = build_registry("node-a", port);
    registry.start().await;

    let peer = make_network_peer("peer-1", "100.64.0.1");
    event_sender.send(NetworkPeerEvent::Joined(peer)).unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut updated_peer = make_network_peer("peer-1", "100.64.0.1");
    updated_peer.relay = Some("sfo".to_string());
    updated_peer.cur_addr = None;
    event_sender
        .send(NetworkPeerEvent::Updated(updated_peer))
        .unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    let peers = registry.peers().await;
    assert_eq!(peers.len(), 1);
    let p = &peers[0];
    assert_eq!(p.connection_type, "relay:sfo");
    assert!(!p.ws_connected, "ws_connected should be preserved as false");
}

#[tokio::test]
async fn test_peer_event_subscription() {
    let port = random_port().await;
    let (registry, event_sender) = build_registry("node-a", port);
    let mut rx = registry.on_peer_change();
    registry.start().await;

    let peer = make_network_peer("peer-1", "100.64.0.1");
    event_sender.send(NetworkPeerEvent::Joined(peer)).unwrap();

    let event = tokio::time::timeout(Duration::from_millis(200), rx.recv())
        .await
        .expect("should receive event within timeout")
        .expect("should not be a recv error");

    match event {
        PeerEvent::Joined(state) => {
            assert_eq!(state.id, "peer-1");
            assert!(state.online);
        }
        other => panic!("expected PeerEvent::Joined, got: {other:?}"),
    }
}

// ===========================================================================
// Tests: Send and lazy connect
// ===========================================================================

#[tokio::test]
async fn test_send_unknown_peer_errors() {
    let port = random_port().await;
    let (registry, _event_sender) = build_registry("node-a", port);
    registry.start().await;

    let result = registry.send("nonexistent-peer", b"hello").await;
    assert!(result.is_err(), "send to unknown peer should fail");

    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("unknown peer"),
        "error should mention unknown peer: {err}"
    );
}

#[tokio::test]
async fn test_send_offline_peer_errors() {
    let port = random_port().await;
    let (registry, event_sender) = build_registry("node-a", port);
    registry.start().await;

    let mut peer = make_network_peer("peer-1", "100.64.0.1");
    peer.online = false;
    event_sender.send(NetworkPeerEvent::Joined(peer)).unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    let result = registry.send("peer-1", b"hello").await;
    assert!(result.is_err(), "send to offline peer should fail");

    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("offline"),
        "error should mention offline: {err}"
    );
}

#[tokio::test]
async fn test_send_lazy_connects() {
    let server_port = random_port().await;

    // Server setup
    let (server_registry, _server_es) = build_registry("server", server_port);
    let mut server_incoming = server_registry.subscribe();
    server_registry.start().await;

    // Client setup — uses server's port so it dials the server
    let (client_registry, client_es) = build_registry("client", server_port);
    client_registry.start().await;

    // Inject server as a known peer on the client
    client_es
        .send(NetworkPeerEvent::Joined(make_loopback_peer("server")))
        .unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Verify peer is known but not connected
    let peers = client_registry.peers().await;
    assert_eq!(peers.len(), 1);
    assert!(
        !peers[0].ws_connected,
        "should not be connected before send"
    );

    // Send — triggers lazy connect
    let msg = b"hello from lazy connect";
    client_registry.send("server", msg).await.unwrap();

    // Verify peer is now connected
    let peers = client_registry.peers().await;
    assert!(peers[0].ws_connected, "should be connected after send");

    // Server should receive the message
    let incoming = tokio::time::timeout(Duration::from_millis(500), server_incoming.recv())
        .await
        .expect("should receive message within timeout")
        .expect("should not be a recv error");

    assert_eq!(incoming.from, "client");
    assert_eq!(incoming.data, msg);
}

#[tokio::test]
async fn test_send_reuses_connection() {
    let server_port = random_port().await;

    let (server_registry, _) = build_registry("server", server_port);
    let mut server_incoming = server_registry.subscribe();
    server_registry.start().await;

    let (client_registry, client_es) = build_registry("client", server_port);
    let mut client_events = client_registry.on_peer_change();
    client_registry.start().await;

    client_es
        .send(NetworkPeerEvent::Joined(make_loopback_peer("server")))
        .unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    // First send — creates connection
    client_registry.send("server", b"msg-1").await.unwrap();

    // Wait for Connected event
    let event = tokio::time::timeout(Duration::from_millis(200), async {
        loop {
            match client_events.recv().await {
                Ok(PeerEvent::WsConnected(_)) => return,
                _ => continue,
            }
        }
    })
    .await;
    assert!(event.is_ok(), "should receive Connected event");

    // Second send — reuses connection (no second Connected event)
    client_registry.send("server", b"msg-2").await.unwrap();

    // Server should receive both messages
    let msg1 = tokio::time::timeout(Duration::from_millis(200), server_incoming.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(msg1.data, b"msg-1");

    let msg2 = tokio::time::timeout(Duration::from_millis(200), server_incoming.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(msg2.data, b"msg-2");
}

#[tokio::test]
async fn test_broadcast_sends_to_all() {
    // The broadcaster has two clients connect TO it (via lazy send).
    // Then the broadcaster broadcasts back to both.
    let bcast_port = random_port().await;

    let (bcast_reg, _) = build_registry("broadcaster", bcast_port);
    bcast_reg.start().await;

    // Client 1
    let (client1_reg, client1_es) = build_registry("client1", bcast_port);
    let mut client1_incoming = client1_reg.subscribe();
    client1_reg.start().await;

    // Client 2
    let (client2_reg, client2_es) = build_registry("client2", bcast_port);
    let mut client2_incoming = client2_reg.subscribe();
    client2_reg.start().await;

    // Inject broadcaster as peer on both clients
    client1_es
        .send(NetworkPeerEvent::Joined(make_loopback_peer("broadcaster")))
        .unwrap();
    client2_es
        .send(NetworkPeerEvent::Joined(make_loopback_peer("broadcaster")))
        .unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Both clients send to broadcaster to establish connections
    // (the broadcaster's accept loop caches these connections)
    client1_reg.send("broadcaster", b"hello1").await.unwrap();
    client2_reg.send("broadcaster", b"hello2").await.unwrap();

    // Give the broadcaster time to accept and cache both connections
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Now broadcast from broadcaster
    bcast_reg.broadcast(b"broadcast-msg").await;

    // Both clients should receive the broadcast
    let msg1 = tokio::time::timeout(Duration::from_millis(500), client1_incoming.recv())
        .await
        .expect("client1 should receive broadcast")
        .expect("should not error");
    assert_eq!(msg1.data, b"broadcast-msg");

    let msg2 = tokio::time::timeout(Duration::from_millis(500), client2_incoming.recv())
        .await
        .expect("client2 should receive broadcast")
        .expect("should not error");
    assert_eq!(msg2.data, b"broadcast-msg");
}

#[tokio::test]
async fn test_incoming_message() {
    let server_port = random_port().await;

    let (server_registry, _) = build_registry("server", server_port);
    let mut server_incoming = server_registry.subscribe();
    server_registry.start().await;

    let (client_registry, client_es) = build_registry("sender", server_port);
    client_registry.start().await;

    client_es
        .send(NetworkPeerEvent::Joined(make_loopback_peer("server")))
        .unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    let payload = b"test payload 12345";
    client_registry.send("server", payload).await.unwrap();

    let msg = tokio::time::timeout(Duration::from_millis(500), server_incoming.recv())
        .await
        .expect("should receive within timeout")
        .expect("should not error");

    assert_eq!(msg.from, "sender");
    assert_eq!(msg.data, payload);
}

#[tokio::test]
async fn test_disconnect_reconnect() {
    let server_port = random_port().await;

    let (server_registry, _) = build_registry("server", server_port);
    let mut server_incoming = server_registry.subscribe();
    server_registry.start().await;

    let (client_registry, client_es) = build_registry("client", server_port);
    client_registry.start().await;

    client_es
        .send(NetworkPeerEvent::Joined(make_loopback_peer("server")))
        .unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    // First send — establishes connection
    client_registry.send("server", b"msg-1").await.unwrap();

    let msg1 = tokio::time::timeout(Duration::from_millis(500), server_incoming.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(msg1.data, b"msg-1");

    // Disconnect
    client_registry.disconnect("server").await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify disconnected state
    let peers = client_registry.peers().await;
    let server_peer = peers.iter().find(|p| p.id == "server").unwrap();
    assert!(
        !server_peer.ws_connected,
        "should be disconnected after disconnect()"
    );

    // Send again — should reconnect via lazy connect
    client_registry.send("server", b"msg-2").await.unwrap();

    // Verify reconnected
    let peers = client_registry.peers().await;
    let server_peer = peers.iter().find(|p| p.id == "server").unwrap();
    assert!(
        server_peer.ws_connected,
        "should be reconnected after second send"
    );

    // Server should receive the second message
    let msg2 = tokio::time::timeout(Duration::from_millis(500), server_incoming.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(msg2.data, b"msg-2");
}

// ===========================================================================
// Tests: PeerEvent::Left closes WS connection
// ===========================================================================

#[tokio::test]
async fn test_peer_left_closes_ws_connection() {
    let server_port = random_port().await;

    // Server that the client will connect to
    let (server_registry, _) = build_registry("server", server_port);
    server_registry.start().await;

    // Client with event sender
    let (client_registry, client_es) = build_registry("client", server_port);
    let mut client_events = client_registry.on_peer_change();
    client_registry.start().await;

    // Inject server as known peer
    client_es
        .send(NetworkPeerEvent::Joined(make_loopback_peer("server")))
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send to establish a WS connection
    client_registry.send("server", b"hello").await.unwrap();

    // Wait for Connected event
    tokio::time::timeout(Duration::from_millis(200), async {
        loop {
            if let Ok(PeerEvent::WsConnected(id)) = client_events.recv().await {
                if id == "server" {
                    return;
                }
            }
        }
    })
    .await
    .expect("should receive Connected event");

    // Verify connected
    let peers = client_registry.peers().await;
    assert!(peers[0].ws_connected, "should be connected before Left");

    // Emit Left event — should close WS and emit Disconnected then Left
    client_es
        .send(NetworkPeerEvent::Left("server".to_string()))
        .unwrap();

    // Collect events: should see Disconnected then Left
    let mut got_disconnected = false;
    let mut got_left = false;

    let _ = tokio::time::timeout(Duration::from_millis(500), async {
        loop {
            match client_events.recv().await {
                Ok(PeerEvent::WsDisconnected(id)) if id == "server" => {
                    got_disconnected = true;
                }
                Ok(PeerEvent::Left(state)) if state.id == "server" => {
                    // Final state arrives offline with WS down (RFC 022 §16.4).
                    assert!(!state.online && !state.ws_connected);
                    got_left = true;
                    return;
                }
                _ => continue,
            }
        }
    })
    .await;

    assert!(got_disconnected, "should emit Disconnected before Left");
    assert!(got_left, "should emit Left");

    // Peer should be removed from registry
    let peers = client_registry.peers().await;
    assert!(
        peers.iter().all(|p| p.id != "server"),
        "peer should be removed after Left"
    );
}

// ===========================================================================
// Tests: Reconnect backoff
// ===========================================================================

#[test]
fn test_reconnect_backoff_basic() {
    let mut backoff = ReconnectBackoff::new();

    // Initially, retry is allowed
    assert!(backoff.should_retry().is_some());

    // After first failure, should_retry returns None (backoff active)
    backoff.failure();
    assert!(backoff.should_retry().is_none());

    // retry_after should be > 0
    let wait = backoff.retry_after();
    assert!(wait > Duration::ZERO, "should have non-zero retry_after");
    assert!(
        wait <= Duration::from_millis(100),
        "first backoff should be <= 100ms"
    );

    // After second failure (simulated after delay elapsed)
    // Manually reset last_attempt to simulate time passing
    backoff.failure();
    let wait2 = backoff.retry_after();
    // Second failure should have a longer delay than the first
    // (though since last_attempt was just set, both are relative to "now")
    assert!(wait2 > Duration::ZERO);
}

#[test]
fn test_reconnect_backoff_resets_on_success() {
    let mut backoff = ReconnectBackoff::new();

    // Fail 3 times
    backoff.failure();
    backoff.failure();
    backoff.failure();

    // Backoff should be active
    assert!(
        backoff.should_retry().is_none(),
        "should be in backoff after 3 failures"
    );

    // Success resets
    backoff.success();
    assert!(
        backoff.should_retry().is_some(),
        "should allow retry after success"
    );
    assert_eq!(
        backoff.retry_after(),
        Duration::ZERO,
        "retry_after should be zero after success"
    );
}

// ===========================================================================
// Tests: Broadcast with zero connections
// ===========================================================================

#[tokio::test]
async fn test_broadcast_with_zero_connections() {
    let port = random_port().await;
    let (registry, _) = build_registry("node-a", port);
    registry.start().await;

    // Broadcast with no connected peers — should not error or panic
    registry.broadcast(b"hello nobody").await;

    // Verify no peers and no connections
    let peers = registry.peers().await;
    assert!(peers.is_empty(), "should have no peers");
}

// ===========================================================================
// Tests: RFC 017 Phase 2 — hello exchange
// ===========================================================================

/// Two nodes with matching `app_id` complete the hello exchange and each
/// side stamps the other's `device_id` / `device_name` / `os` onto its
/// session peer registry.
#[tokio::test]
async fn test_hello_exchange_populates_identity() {
    let server_port = random_port().await;

    let (server_registry, server_es) = build_registry("server", server_port);
    let mut server_incoming = server_registry.subscribe();
    server_registry.start().await;

    let (client_registry, client_es) = build_registry("client", server_port);
    client_registry.start().await;

    // Register each side in the other's Layer 3 peer map by Tailscale ID.
    server_es
        .send(NetworkPeerEvent::Joined(make_loopback_peer("client")))
        .unwrap();
    client_es
        .send(NetworkPeerEvent::Joined(make_loopback_peer("server")))
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Trigger the lazy connect from client → server.
    client_registry.send("server", b"hello").await.unwrap();

    // Give the accept loop time to populate the server-side PeerState.
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Server receives the first frame (verifies the data channel is up).
    let _msg = tokio::time::timeout(Duration::from_millis(500), server_incoming.recv())
        .await
        .expect("server should receive the inbound frame")
        .expect("broadcast channel should not error");

    // Client side: identity for "server" should be populated from the
    // remote hello.
    let client_peers = client_registry.peers().await;
    let server_view = client_peers
        .iter()
        .find(|p| p.id == "server")
        .expect("client should know about server");
    let server_identity = server_view
        .identity
        .as_ref()
        .expect("hello should have landed on the client side");
    assert_eq!(server_identity.app_id, "test");
    assert_eq!(server_identity.device_id, "dev-server");
    assert_eq!(server_identity.device_name, "Test Node server");
    assert_eq!(server_identity.tailscale_id, "server");
    assert!(!server_identity.os.is_empty(), "os should be populated");

    // Server side: identity for "client" should be populated too.
    let server_peers = server_registry.peers().await;
    let client_view = server_peers
        .iter()
        .find(|p| p.id == "client")
        .expect("server should know about client");
    let client_identity = client_view
        .identity
        .as_ref()
        .expect("hello should have landed on the server side");
    assert_eq!(client_identity.app_id, "test");
    assert_eq!(client_identity.device_id, "dev-client");
    assert_eq!(client_identity.device_name, "Test Node client");
    assert_eq!(client_identity.tailscale_id, "client");
}

/// Two nodes that disagree on `app_id` fail the hello exchange: neither
/// side registers a usable WebSocket, and `send()` on the dialer's side
/// surfaces a connect error.
#[tokio::test]
async fn test_hello_exchange_rejects_app_mismatch() {
    let server_port = random_port().await;

    // Server runs under `app_id = "chat"`.
    let (server_registry, _server_es) = build_registry_with_app("chat", "server", server_port);
    server_registry.start().await;

    // Client runs under `app_id = "playground"`.
    let (client_registry, client_es) = build_registry_with_app("playground", "client", server_port);
    client_registry.start().await;

    // Inject the server as a loopback peer on the client.
    client_es
        .send(NetworkPeerEvent::Joined(make_loopback_peer("server")))
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Lazy-send fails because the hello exchange rejects the app mismatch.
    let result = client_registry.send("server", b"unauthorised").await;
    assert!(
        result.is_err(),
        "sending across an app mismatch should fail, got: {result:?}"
    );
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("app mismatch") || err.contains("connect failed"),
        "error should indicate the hello failure, got: {err}"
    );

    // Neither side should have populated identity for the other.
    let client_peers = client_registry.peers().await;
    let server_entry = client_peers
        .iter()
        .find(|p| p.id == "server")
        .expect("peer still known at Layer 3");
    assert!(
        server_entry.identity.is_none(),
        "app mismatch must leave identity unpopulated"
    );
}

/// A remote that sends a hello with an unknown `kind` value is rejected
/// with [`TransportError::HelloMalformed`] and the close frame carries
/// [`CLOSE_HELLO_PROTOCOL`].
#[tokio::test]
async fn test_hello_exchange_rejects_malformed_hello() {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;

    let server_port = random_port().await;

    // Spin up a real PeerRegistry on the server side so the hello
    // exchange actually runs.
    let (server_registry, _server_es) = build_registry("server", server_port);
    server_registry.start().await;

    // Give the accept loop time to bind.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Manually dial the server as a WebSocket client and send a bogus
    // hello frame. This bypasses the library's client path so we can
    // observe the server's rejection behaviour.
    let (mut ws, _resp) =
        tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{server_port}/ws"))
            .await
            .expect("ws upgrade should succeed");

    // Send a malformed hello (wrong `kind`).
    ws.send(Message::Text(
        r#"{"kind":"not_hello","version":2,"identity":{"app_id":"test","device_id":"x","device_name":"y","os":"darwin","tailscale_id":"z"}}"#
            .to_string()
            .into(),
    ))
    .await
    .expect("send should succeed before server close");

    // The server should respond with a close frame carrying
    // `CLOSE_HELLO_PROTOCOL` (4002).
    let mut saw_close = false;
    for _ in 0..10 {
        match ws.next().await {
            Some(Ok(Message::Close(Some(frame)))) => {
                assert_eq!(u16::from(frame.code), super::CLOSE_HELLO_PROTOCOL);
                saw_close = true;
                break;
            }
            Some(Ok(_)) => continue,
            Some(Err(_)) | None => break,
        }
    }
    assert!(
        saw_close,
        "server should close with CLOSE_HELLO_PROTOCOL after bogus hello"
    );
}

/// A remote that never sends any hello frame is dropped after the
/// 5-second hello timeout elapses. Verifies the timeout path sends a
/// [`CLOSE_HELLO_PROTOCOL`] close frame to the caller.
#[tokio::test]
async fn test_hello_exchange_hello_timeout() {
    use futures_util::StreamExt;
    use tokio_tungstenite::tungstenite::Message;

    let server_port = random_port().await;

    let (server_registry, _server_es) = build_registry("server", server_port);
    server_registry.start().await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Dial the server but never send a hello.
    let (mut ws, _resp) =
        tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{server_port}/ws"))
            .await
            .expect("ws upgrade should succeed");

    // Wait slightly longer than the hello timeout (5s). To keep the
    // test fast, we use `tokio::time::pause`-style patience and drain
    // frames until the server closes us out.
    let mut saw_close = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(7);
    while tokio::time::Instant::now() < deadline {
        let remaining = deadline.duration_since(tokio::time::Instant::now());
        match tokio::time::timeout(remaining, ws.next()).await {
            Ok(Some(Ok(Message::Close(Some(frame))))) => {
                assert_eq!(u16::from(frame.code), super::CLOSE_HELLO_PROTOCOL);
                saw_close = true;
                break;
            }
            Ok(Some(Ok(_))) => continue,
            Ok(Some(Err(_))) | Ok(None) => break,
            Err(_) => break,
        }
    }
    assert!(
        saw_close,
        "server should close with CLOSE_HELLO_PROTOCOL after hello timeout"
    );
}

// ===========================================================================
// RFC 022 identity index (first-wins, ghost retire, generation)
// ===========================================================================

#[tokio::test]
async fn test_rfc022_generation_bumps_on_rejoin() {
    let port = random_port().await;
    let (registry, es) = build_registry("local", port);
    registry.start().await;

    es.send(NetworkPeerEvent::Joined(make_loopback_peer("peer-a")))
        .unwrap();
    tokio::time::sleep(Duration::from_millis(40)).await;
    let gen1 = registry
        .peers()
        .await
        .into_iter()
        .find(|p| p.id == "peer-a")
        .unwrap()
        .generation;
    assert_eq!(gen1, 1);

    es.send(NetworkPeerEvent::Left("peer-a".into())).unwrap();
    tokio::time::sleep(Duration::from_millis(40)).await;

    es.send(NetworkPeerEvent::Joined(make_loopback_peer("peer-a")))
        .unwrap();
    tokio::time::sleep(Duration::from_millis(40)).await;
    let gen2 = registry
        .peers()
        .await
        .into_iter()
        .find(|p| p.id == "peer-a")
        .unwrap()
        .generation;
    assert_eq!(gen2, 2);
    assert_ne!(
        format_peer_ref("peer-a", gen1),
        format_peer_ref("peer-a", gen2)
    );
}

#[tokio::test]
async fn test_rfc022_first_wins_duplicate_ulid() {
    let port = random_port().await;
    let (registry, es) = build_registry("local", port);
    registry.start().await;

    es.send(NetworkPeerEvent::Joined(make_loopback_peer("a")))
        .unwrap();
    es.send(NetworkPeerEvent::Joined(make_loopback_peer("b")))
        .unwrap();
    tokio::time::sleep(Duration::from_millis(40)).await;

    let uid = "01J4K9M2Z8AB3RNYQPW6H5TC0X";
    let ident = |ts: &str| PeerIdentity {
        app_id: "test".into(),
        device_id: uid.into(),
        device_name: format!("Node {ts}"),
        os: "linux".into(),
        tailscale_id: ts.into(),
    };

    assert!(registry.test_stamp_identity("a", ident("a")).await);
    assert!(registry.test_stamp_identity("b", ident("b")).await);

    let peers = registry.peers().await;
    let a = peers.iter().find(|p| p.id == "a").unwrap();
    let b = peers.iter().find(|p| p.id == "b").unwrap();
    assert!(!a.identity_suppressed);
    assert_eq!(a.published_device_id(), Some(uid));
    assert!(b.identity_suppressed);
    assert!(b.published_device_id().is_none());
    assert_eq!(registry.test_by_device(uid).await.as_deref(), Some("a"));

    // Projection honesty
    let pa: crate::node::Peer = a.clone().into();
    let pb: crate::node::Peer = b.clone().into();
    assert_eq!(pa.device_id.as_deref(), Some(uid));
    assert!(pb.device_id.is_none());
}

#[tokio::test]
async fn test_rfc022_ghost_retire_on_offline_holder() {
    let port = random_port().await;
    let (registry, es) = build_registry("local", port);
    registry.start().await;

    es.send(NetworkPeerEvent::Joined(make_loopback_peer("old")))
        .unwrap();
    es.send(NetworkPeerEvent::Joined(make_loopback_peer("new")))
        .unwrap();
    tokio::time::sleep(Duration::from_millis(40)).await;

    let uid = "01J4K9M2Z8AB3RNYQPW6H5TC0Y";
    let ident = |ts: &str| PeerIdentity {
        app_id: "test".into(),
        device_id: uid.into(),
        device_name: format!("Node {ts}"),
        os: "linux".into(),
        tailscale_id: ts.into(),
    };

    assert!(registry.test_stamp_identity("old", ident("old")).await);
    // Mark old offline without removing (simulate stale ghost still in map)
    {
        // Left removes; for ghost we need offline but still present.
        // Use Updated with online=false via NetworkPeer.
        let mut p = make_loopback_peer("old");
        p.online = false;
        es.send(NetworkPeerEvent::Updated(p)).unwrap();
    }
    tokio::time::sleep(Duration::from_millis(40)).await;

    assert!(registry.test_stamp_identity("new", ident("new")).await);

    let peers = registry.peers().await;
    assert!(
        peers.iter().all(|p| p.id != "old"),
        "ghost should be retired"
    );
    let n = peers.iter().find(|p| p.id == "new").unwrap();
    assert_eq!(n.published_device_id(), Some(uid));
    assert_eq!(registry.test_by_device(uid).await.as_deref(), Some("new"));
}

#[tokio::test]
async fn test_rfc022_inbound_from_is_tailscale_id() {
    let server_port = random_port().await;

    let (server_registry, server_es) = build_registry("server", server_port);
    let mut server_incoming = server_registry.subscribe();
    server_registry.start().await;

    let (client_registry, client_es) = build_registry("client", server_port);
    client_registry.start().await;

    server_es
        .send(NetworkPeerEvent::Joined(make_loopback_peer("client")))
        .unwrap();
    client_es
        .send(NetworkPeerEvent::Joined(make_loopback_peer("server")))
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    client_registry.send("server", b"ping").await.unwrap();

    let msg = tokio::time::timeout(Duration::from_millis(800), server_incoming.recv())
        .await
        .expect("timeout")
        .expect("recv");
    // RFC 022 §7.5: from is the connection's Tailscale id, not the ULID.
    // In the test harness identity.device_id == tailscale id ("client"), so
    // check that from matches the routing key used for the connection.
    assert_eq!(msg.from, "client");
}

#[tokio::test]
async fn test_rfc022_eager_identity_without_app_send() {
    let server_port = random_port().await;

    let (server_registry, server_es) = build_registry("server", server_port);
    server_registry.start().await;

    let (client_registry, client_es) = build_registry("client", server_port);
    client_registry.start().await;

    server_es
        .send(NetworkPeerEvent::Joined(make_loopback_peer("client")))
        .unwrap();
    client_es
        .send(NetworkPeerEvent::Joined(make_loopback_peer("server")))
        .unwrap();

    // No client_registry.send — rely on eager identity (default on).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    let mut saw = false;
    while tokio::time::Instant::now() < deadline {
        let peers = client_registry.peers().await;
        if let Some(p) = peers.iter().find(|p| p.id == "server") {
            if p.published_device_id().is_some() {
                saw = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        saw,
        "eager identity should populate device_id without app send"
    );

    // Symmetric: server should also learn client identity (accept path or eager dial).
    let peers = server_registry.peers().await;
    let client = peers.iter().find(|p| p.id == "client");
    // May take a moment if only accept path fires after client dials.
    if client.and_then(|p| p.published_device_id()).is_none() {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            let peers = server_registry.peers().await;
            if peers
                .iter()
                .find(|p| p.id == "client")
                .and_then(|p| p.published_device_id())
                .is_some()
            {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    let peers = server_registry.peers().await;
    let client = peers.iter().find(|p| p.id == "client").unwrap();
    assert!(
        client.published_device_id().is_some(),
        "server should see client identity after eager exchange"
    );
}

#[test]
fn test_rfc022_cross_dials_choose_the_same_physical_connection() {
    assert_eq!(
        preferred_connection_direction("alpha", "beta"),
        Some(ConnectionDirection::Outbound)
    );
    assert_eq!(
        preferred_connection_direction("beta", "alpha"),
        Some(ConnectionDirection::Inbound)
    );
    assert!(should_replace_connection(
        ConnectionDirection::Inbound,
        ConnectionDirection::Outbound,
        Some(ConnectionDirection::Outbound),
    ));
    assert!(!should_replace_connection(
        ConnectionDirection::Inbound,
        ConnectionDirection::Outbound,
        Some(ConnectionDirection::Inbound),
    ));
}

#[tokio::test]
async fn test_rfc022_inbound_identity_is_reconciled_after_discovery() {
    let server_port = random_port().await;

    let (server_registry, server_es) = build_registry("server", server_port);
    server_registry.start().await;

    let (client_registry, client_es) = build_registry("client", server_port);
    client_registry.start().await;

    // Client discovers and dials first. The server has not received its
    // Layer-3 Joined event yet, so the inbound hello must be retained.
    client_es
        .send(NetworkPeerEvent::Joined(make_loopback_peer("server")))
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        server_registry
            .peers()
            .await
            .iter()
            .all(|peer| peer.id != "client"),
        "server should not synthesize a Layer-3 peer from the hello"
    );

    server_es
        .send(NetworkPeerEvent::Joined(make_loopback_peer("client")))
        .unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let peers = server_registry.peers().await;
        if peers
            .iter()
            .find(|peer| peer.id == "client")
            .is_some_and(|peer| peer.published_device_id() == Some("dev-client"))
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "pending inbound identity was not reconciled after discovery"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[tokio::test]
async fn test_rfc022_eager_identity_retries_transient_dial_failure() {
    let server_port = random_port().await;

    let (server_registry, _server_es) = build_registry("server", server_port);
    server_registry.start().await;

    let (client_registry, client_es) = build_registry_with_dial_failures("client", server_port, 1);
    client_registry.start().await;

    client_es
        .send(NetworkPeerEvent::Joined(make_loopback_peer("server")))
        .unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        let peers = client_registry.peers().await;
        if peers
            .iter()
            .find(|peer| peer.id == "server")
            .is_some_and(|peer| peer.published_device_id() == Some("dev-server"))
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "eager identity did not recover after a transient dial failure"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[tokio::test]
async fn test_rfc022_eager_identity_disabled_no_auto_hello() {
    let server_port = random_port().await;
    let opts = PeerRegistryOptions {
        eager_identity: false,
        ..Default::default()
    };
    let (server_registry, _server_es) =
        build_registry_with_options("test", "server", server_port, opts.clone());
    server_registry.start().await;

    let (client_registry, client_es) =
        build_registry_with_options("test", "client", server_port, opts);
    client_registry.start().await;

    client_es
        .send(NetworkPeerEvent::Joined(make_loopback_peer("server")))
        .unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;

    let peers = client_registry.peers().await;
    let server = peers.iter().find(|p| p.id == "server").unwrap();
    assert!(
        server.published_device_id().is_none(),
        "with eager_identity=false, no auto hello"
    );
    assert!(!server.ws_connected);
}

#[test]
fn test_parse_peer_ref_forms() {
    assert_eq!(parse_peer_ref("nABC123:3"), Some(("nABC123", 3)));
    assert_eq!(parse_peer_ref("nABC123"), None); // no generation
    assert_eq!(parse_peer_ref("fd7a:115c::1"), None); // IPv6 — multiple colons
    assert_eq!(parse_peer_ref(":3"), None); // empty id
    assert_eq!(parse_peer_ref("nABC:"), None); // empty generation
    assert_eq!(parse_peer_ref("nABC:3a"), None); // non-digit generation
}

/// RFC 022 §7.7 + I5: ULID routing goes through the first-wins `by_device`
/// index (a suppressed duplicate never captures traffic), suppressed names do
/// not resolve, and peer-ref selectors are generation-checked.
#[tokio::test]
async fn test_rfc022_routing_ignores_suppressed_and_checks_generation() {
    let port = random_port().await;
    let (registry, es) = build_registry("local", port);
    registry.start().await;

    es.send(NetworkPeerEvent::Joined(make_loopback_peer("a")))
        .unwrap();
    es.send(NetworkPeerEvent::Joined(make_loopback_peer("b")))
        .unwrap();
    tokio::time::sleep(Duration::from_millis(40)).await;

    let uid = "01J4K9M2Z8AB3RNYQPW6H5TC0X";
    let ident = |ts: &str, name: &str| PeerIdentity {
        app_id: "test".into(),
        device_id: uid.into(),
        device_name: name.into(),
        os: "linux".into(),
        tailscale_id: ts.into(),
    };
    assert!(
        registry
            .test_stamp_identity("a", ident("a", "Holder"))
            .await
    );
    assert!(
        registry
            .test_stamp_identity("b", ident("b", "Claimant"))
            .await
    );

    // ULID-addressed traffic routes to the first-wins holder, never the
    // suppressed claimant — regardless of map iteration order.
    assert_eq!(registry.resolve_routing_key(uid).await.unwrap(), "a");
    // The suppressed claimant's identity is unpublished; its name must not
    // resolve either.
    assert!(matches!(
        registry.resolve_routing_key("Claimant").await,
        Err(SessionError::UnknownPeer(_))
    ));
    // Peer refs: live generation resolves; stale or departed → PeerGone.
    assert_eq!(registry.resolve_routing_key("a:1").await.unwrap(), "a");
    assert!(matches!(
        registry.resolve_routing_key("a:9").await,
        Err(SessionError::PeerGone(_))
    ));
    assert!(matches!(
        registry.resolve_routing_key("gone:1").await,
        Err(SessionError::PeerGone(_))
    ));
}

/// RFC 022 §8: a re-hello with the identical identity emits no second
/// `identity`; a changed device_name surfaces as `updated` instead.
#[tokio::test]
async fn test_rfc022_rehello_emits_no_duplicate_identity() {
    let port = random_port().await;
    let (registry, es) = build_registry("local", port);
    let mut events = registry.on_peer_change();
    registry.start().await;

    es.send(NetworkPeerEvent::Joined(make_loopback_peer("a")))
        .unwrap();
    tokio::time::sleep(Duration::from_millis(40)).await;

    let ident = |name: &str| PeerIdentity {
        app_id: "test".into(),
        device_id: "01J4K9M2Z8AB3RNYQPW6H5TC0X".into(),
        device_name: name.into(),
        os: "linux".into(),
        tailscale_id: "a".into(),
    };

    assert!(registry.test_stamp_identity("a", ident("Alice")).await);
    // Reconnect re-hello with the identical identity: must be silent.
    assert!(registry.test_stamp_identity("a", ident("Alice")).await);
    // Renamed device on re-hello: surfaces as `updated`, not `identity`.
    assert!(registry.test_stamp_identity("a", ident("Alice II")).await);

    let mut identity_events = 0;
    let mut updated_with_identity = 0;
    while let Ok(ev) = events.try_recv() {
        match ev {
            PeerEvent::Identity(_) => identity_events += 1,
            PeerEvent::Updated(s) if s.identity.is_some() => updated_with_identity += 1,
            _ => {}
        }
    }
    assert_eq!(identity_events, 1, "re-hello must not re-emit identity");
    assert_eq!(updated_with_identity, 1, "rename surfaces as updated");
}

/// RFC 022 §8.1: the eager-dial stagger is bounded within the window, stable
/// per peer, disabled at window 0, and spread across peers — all without a
/// `rand` dependency (see `super::eager_jitter_delay`).
#[test]
fn test_eager_jitter_delay_bounded_and_stable() {
    use super::eager_jitter_delay;

    // Window 0 → no delay. Tests rely on this to disable eager jitter, and it
    // guards the `% 0` panic.
    assert_eq!(eager_jitter_delay("server", 0), Duration::ZERO);
    assert_eq!(eager_jitter_delay("anything", 0), Duration::ZERO);

    // Bounded strictly within the window, and stable for a given peer id.
    let window = 250;
    for id in ["server", "client", "peer-a", "peer-b", "01J4K9M2Z8AB"] {
        let d = eager_jitter_delay(id, window);
        assert!(
            d < Duration::from_millis(window),
            "delay {d:?} for {id} must be < {window}ms window"
        );
        assert_eq!(
            d,
            eager_jitter_delay(id, window),
            "delay must be stable per peer id"
        );
    }

    // Spread: across many ids the delays are not all identical — a degenerate
    // constant jitter would defeat the purpose. Deterministic (DefaultHasher is
    // fixed-seed), so this can never flake.
    let distinct: std::collections::HashSet<Duration> = (0..32)
        .map(|i| eager_jitter_delay(&format!("peer-{i}"), window))
        .collect();
    assert!(
        distinct.len() > 1,
        "jitter should spread peers across the window, got {} distinct value(s)",
        distinct.len()
    );
}

// ── Property tests (review: protocol fuzzing) ───────────────────────────

use proptest::prelude::*;

proptest! {
    /// Arbitrary strings must never panic the peer-ref parser.
    #[test]
    fn parse_peer_ref_never_panics(s in ".{0,128}") {
        let _ = super::parse_peer_ref(&s);
    }

    /// Every formatted peer ref parses back to its parts.
    #[test]
    fn peer_ref_roundtrip(id in "[A-Za-z0-9_.-]{1,32}", generation in 0u64..1_000_000) {
        let formatted = super::format_peer_ref(&id, generation);
        prop_assert_eq!(super::parse_peer_ref(&formatted), Some((id.as_str(), generation)));
    }
}

// ---------------------------------------------------------------------------
// RFC 025 §3.3 — a gated node does not dial a peer whose owner it cannot state
// ---------------------------------------------------------------------------

/// A gated registry refuses to OPEN a connection to a peer Layer 3 kept but
/// could not name an owner for. The peer stays in the registry (a transient
/// omission must not empty a gated mesh) but our hello is never volunteered
/// to it, so the dial side now agrees with the inbound gate, which refuses
/// such a caller's hello with 4004.
#[tokio::test]
async fn test_gated_registry_refuses_to_dial_a_peer_with_no_login() {
    let port = random_port().await;
    let (registry, es) = build_gated_registry("client", port);
    registry.start().await;

    es.send(NetworkPeerEvent::Joined(make_loopback_peer_with_login(
        "server", None,
    )))
    .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Precondition: the peer IS in the registry and online. Without this the
    // refusal below could just be an unknown or offline peer.
    let peers = registry.peers().await;
    let peer = peers
        .iter()
        .find(|p| p.id == "server")
        .expect("a login-less row must still be a peer on a gated node");
    assert!(peer.online);
    assert!(!peer.ws_connected);

    let err = registry
        .ensure_ws_connected("server")
        .await
        .expect_err("a gated node must not dial a peer whose login is unknown");
    assert!(
        matches!(&err, SessionError::LoginUnknown(id) if id == "server"),
        "expected LoginUnknown, got {err:?}"
    );

    // No connection was opened.
    assert!(
        !registry
            .peers()
            .await
            .iter()
            .any(|p| p.id == "server" && p.ws_connected),
        "the refusal must leave the connection map untouched"
    );
}

/// The same gated registry dials normally once Layer 3 can state the login:
/// the gate is about ignorance, not about being gated. A real loopback hello
/// so this proves a CONNECTION, not merely a different error.
#[tokio::test]
async fn test_gated_registry_dials_a_peer_whose_login_is_known() {
    let server_port = random_port().await;

    let (server_registry, _server_es) = build_registry("server", server_port);
    server_registry.start().await;

    let (client_registry, client_es) = build_gated_registry("client", server_port);
    client_registry.start().await;

    client_es
        .send(NetworkPeerEvent::Joined(make_loopback_peer_with_login(
            "server",
            Some("alice@example.com"),
        )))
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    client_registry
        .ensure_ws_connected("server")
        .await
        .expect("a known login must dial exactly as before RFC 025");

    let peers = client_registry.peers().await;
    let peer = peers.iter().find(|p| p.id == "server").expect("server");
    assert!(
        peer.ws_connected,
        "the connection must actually be up, not merely un-refused"
    );
}

/// An UNGATED registry ignores the field entirely — this is the control that
/// keeps the change invisible to every node that has not opted in.
#[tokio::test]
async fn test_ungated_registry_ignores_an_unknown_login() {
    let server_port = random_port().await;

    let (server_registry, _server_es) = build_registry("server", server_port);
    server_registry.start().await;

    let (client_registry, client_es) = build_registry("client", server_port);
    client_registry.start().await;

    client_es
        .send(NetworkPeerEvent::Joined(make_loopback_peer_with_login(
            "server", None,
        )))
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    client_registry
        .ensure_ws_connected("server")
        .await
        .expect("an ungated node must dial a login-less peer as it always did");

    let peers = client_registry.peers().await;
    assert!(
        peers
            .iter()
            .find(|p| p.id == "server")
            .expect("server")
            .ws_connected
    );
}

/// An EXISTING connection is left standing when the login later goes unknown:
/// the gate refuses to OPEN one, it does not tear one down. Tearing down on a
/// transient omission is exactly the mesh-emptying the keep rule forbids.
#[tokio::test]
async fn test_gated_registry_leaves_an_existing_connection_standing() {
    let server_port = random_port().await;

    let (server_registry, _server_es) = build_registry("server", server_port);
    server_registry.start().await;

    let (client_registry, client_es) = build_gated_registry("client", server_port);
    client_registry.start().await;

    client_es
        .send(NetworkPeerEvent::Joined(make_loopback_peer_with_login(
            "server",
            Some("alice@example.com"),
        )))
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    client_registry.ensure_ws_connected("server").await.unwrap();

    // Layer 3 now reports the same peer with no owner named.
    client_es
        .send(NetworkPeerEvent::Updated(make_loopback_peer_with_login(
            "server", None,
        )))
        .unwrap();
    tokio::time::sleep(Duration::from_millis(80)).await;

    client_registry
        .ensure_ws_connected("server")
        .await
        .expect("an already-open connection must be returned, not refused");
    let peers = client_registry.peers().await;
    assert!(
        peers
            .iter()
            .find(|p| p.id == "server")
            .expect("server")
            .ws_connected,
        "the standing connection must survive the login going unknown"
    );
}

/// The EAGER dial inherits the gate. This is the path that volunteers a hello
/// with no application traffic at all, so it is the one that would quietly
/// reveal us to a node whose owner we cannot state.
///
/// A/B in one test: the same gated registry, the same loopback server, the
/// same wait — only the row's login differs. Without the control, "it did not
/// connect" would just mean eager identity never ran.
#[tokio::test]
async fn test_gated_eager_dial_skips_a_peer_with_no_login() {
    async fn learned_identity_within(
        registry: &PeerRegistry<MockNetworkProvider>,
        peer: &str,
        window: Duration,
    ) -> bool {
        let deadline = tokio::time::Instant::now() + window;
        while tokio::time::Instant::now() < deadline {
            if registry
                .peers()
                .await
                .iter()
                .any(|p| p.id == peer && p.published_device_id().is_some())
            {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        false
    }

    let server_port = random_port().await;
    let (server_registry, _server_es) = build_registry("server", server_port);
    server_registry.start().await;

    // Control: a row WITH a login is dialed eagerly, so the mechanism works
    // and the window below is long enough.
    let (allowed, allowed_es) = build_gated_eager_registry("allowed", server_port);
    allowed.start().await;
    allowed_es
        .send(NetworkPeerEvent::Joined(make_loopback_peer_with_login(
            "server",
            Some("alice@example.com"),
        )))
        .unwrap();
    assert!(
        learned_identity_within(&allowed, "server", Duration::from_secs(3)).await,
        "the control must dial eagerly, or the negative below proves nothing"
    );

    // The assertion: a row with NO login is never dialed.
    let (blocked, blocked_es) = build_gated_eager_registry("blocked", server_port);
    blocked.start().await;
    blocked_es
        .send(NetworkPeerEvent::Joined(make_loopback_peer_with_login(
            "server", None,
        )))
        .unwrap();
    assert!(
        !learned_identity_within(&blocked, "server", Duration::from_secs(3)).await,
        "a gated node must not volunteer an eager hello to a peer whose owner \
         Layer 3 cannot state"
    );

    // ... and the peer is still a peer. The gate refuses the DIAL, not the row.
    assert!(
        blocked.peers().await.iter().any(|p| p.id == "server"),
        "the login-less peer must keep its place in the registry"
    );
}

// ---------------------------------------------------------------------------
// M1 — a hello-only peer's slices must still bind to their sender
// ---------------------------------------------------------------------------

/// A custom-hostname node (RFC 023 §6.4) fails the Layer 3 app-prefix
/// heuristic, so the accepting side never gets a `Joined` for it and its
/// registry entry is never created. Its hello identity waits in
/// `pending_identities` forever, and without the fallback every synced-store
/// slice it sends is dropped as "no published identity" — a regression
/// against the pre-RFC-025 behaviour.
///
/// This mirrors `test_hello_exchange_populates_identity`'s real loopback WS,
/// with the one difference that makes it the hello-only case: the SERVER is
/// never told about the client at Layer 3.
#[tokio::test]
async fn test_hello_only_peer_publishes_its_device_id_while_connected() {
    let server_port = random_port().await;

    let (server_registry, _server_es) = build_registry("server", server_port);
    let mut server_incoming = server_registry.subscribe();
    server_registry.start().await;

    let (client_registry, client_es) = build_registry("client", server_port);
    client_registry.start().await;

    // ONLY the client learns about the server. The server never gets a
    // Joined for "client", so it has no registry entry for it.
    client_es
        .send(NetworkPeerEvent::Joined(make_loopback_peer("server")))
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Pin the precondition BEFORE the assertion: with no entry and no
    // connection, the answer must be None. Otherwise a later Some could be
    // coming from a registry entry that quietly appeared.
    assert_eq!(
        server_registry.published_device_id("client").await,
        None,
        "nothing is published before the hello"
    );

    client_registry.send("server", b"hello").await.unwrap();
    let _msg = tokio::time::timeout(Duration::from_millis(500), server_incoming.recv())
        .await
        .expect("server should receive the inbound frame")
        .expect("broadcast channel should not error");
    tokio::time::sleep(Duration::from_millis(100)).await;

    // The server still has NO registry entry for the client — this is what
    // makes it the hello-only case rather than an ordinary peer.
    assert!(
        !server_registry
            .peers()
            .await
            .iter()
            .any(|p| p.id == "client"),
        "a hello-only peer must have no Layer 3 entry, or this test is \
         exercising the ordinary path"
    );

    // ... and yet its slices can be bound to it, because the transport
    // verified that hello for the connection that is still standing.
    assert_eq!(
        server_registry
            .published_device_id("client")
            .await
            .as_deref(),
        Some("dev-client"),
        "a hello-only peer's verified identity must bind its store slices"
    );

    // Condition 1: once the connection is gone, the identity speaks for
    // nothing — even though `pending_identities` still holds it (only a
    // Layer 3 `Left` sweeps it, and for this peer none will ever come).
    server_registry.disconnect("client").await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        server_registry.published_device_id("client").await,
        None,
        "a departed connection must stop publishing the identity"
    );
}

/// Condition 2, first-wins (RFC 022 §7.7): a hello-only claimant must never
/// speak for a ULID a live published peer already owns, or it could overwrite
/// that device's store slice.
#[tokio::test]
async fn test_hello_only_peer_never_steals_a_published_ulid() {
    let server_port = random_port().await;

    let (server_registry, server_es) = build_registry("server", server_port);
    let mut server_incoming = server_registry.subscribe();
    server_registry.start().await;

    // A discovered peer publishes the ULID the client will also claim.
    server_es
        .send(NetworkPeerEvent::Joined(make_network_peer(
            "holder",
            "100.64.0.9",
        )))
        .unwrap();
    tokio::time::sleep(Duration::from_millis(40)).await;
    assert!(
        server_registry
            .test_stamp_identity(
                "holder",
                PeerIdentity {
                    app_id: "test".into(),
                    device_id: "dev-client".into(),
                    device_name: "Holder".into(),
                    os: "linux".into(),
                    tailscale_id: "holder".into(),
                },
            )
            .await
    );
    assert_eq!(
        server_registry
            .test_by_device("dev-client")
            .await
            .as_deref(),
        Some("holder"),
        "the holder must own the ULID before the claimant connects"
    );

    // Now the hello-only client connects, claiming the same ULID.
    let (client_registry, client_es) = build_registry("client", server_port);
    client_registry.start().await;
    client_es
        .send(NetworkPeerEvent::Joined(make_loopback_peer("server")))
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    client_registry.send("server", b"hello").await.unwrap();
    let _msg = tokio::time::timeout(Duration::from_millis(500), server_incoming.recv())
        .await
        .expect("server should receive the inbound frame")
        .expect("broadcast channel should not error");
    tokio::time::sleep(Duration::from_millis(100)).await;

    assert_eq!(
        server_registry.published_device_id("client").await,
        None,
        "a hello-only claimant must not speak for a ULID another live peer \
         already published"
    );
    // The holder is untouched.
    assert_eq!(
        server_registry
            .published_device_id("holder")
            .await
            .as_deref(),
        Some("dev-client")
    );
}

/// An entry that EXISTS answers definitively and never falls through to the
/// pending map — a suppressed or hello-less peer must stay unpublished.
#[tokio::test]
async fn test_an_existing_entry_never_falls_back_to_pending() {
    let port = random_port().await;
    let (registry, es) = build_registry("me", port);
    registry.start().await;

    es.send(NetworkPeerEvent::Joined(make_network_peer(
        "peer",
        "100.64.0.4",
    )))
    .unwrap();
    tokio::time::sleep(Duration::from_millis(40)).await;

    assert!(
        registry.peers().await.iter().any(|p| p.id == "peer"),
        "the entry must exist for this test to mean anything"
    );
    assert_eq!(
        registry.published_device_id("peer").await,
        None,
        "an entry with no hello stays unpublished"
    );
}
