//! Unit tests for Layer 4: Transport.

use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpStream;
use tokio::sync::{broadcast, mpsc};

use crate::network::NetworkProvider;
use crate::network::{
    HealthInfo, IncomingConnection, NetworkError, NetworkPeer, NetworkPeerEvent,
    NetworkTcpListener, NodeIdentity, PeerAddr, PingResult,
};
use crate::transport::{
    DatagramTransport, FramedStream, Handshake, RawTransport, StreamTransport, TransportError,
    WsConfig, PROTOCOL_VERSION,
};

// ---------------------------------------------------------------------------
// Mock NetworkProvider for unit tests
// ---------------------------------------------------------------------------

/// A mock network provider that uses local TCP for testing.
///
/// `dial_tcp` connects to `127.0.0.1:{port}` directly.
/// `listen_tcp` binds a local TCP listener and forwards connections.
struct MockNetworkProvider {
    identity: NodeIdentity,
    local_addr: PeerAddr,
    peer_event_tx: broadcast::Sender<NetworkPeerEvent>,
    /// WhoIs identity JSON to attach to every `IncomingConnection` this mock
    /// yields. Empty by default so existing tests take the warn-and-allow
    /// path in `verify_authenticated_identity`.
    incoming_remote_identity: String,
}

impl MockNetworkProvider {
    fn new(id: &str) -> Self {
        let (peer_event_tx, _) = broadcast::channel(16);
        Self {
            identity: NodeIdentity {
                app_id: "test".to_string(),
                // RFC 022 I1: durable device_id must be non-empty and distinct
                // from the Tailscale routing id. Hello validation rejects
                // device_id == tailscale_id at the trust boundary; using the
                // same string for both hangs loopback WS tests on accept()
                // after the peer is closed for a malformed hello.
                device_id: format!("dev-{id}"),
                device_name: format!("Test Node {id}"),
                tailscale_hostname: format!("truffle-test-{id}"),
                // Routing / remote_peer_id still use the short fixture id.
                tailscale_id: id.to_string(),
                dns_name: None,
                ip: Some("127.0.0.1".parse().unwrap()),
                login_name: None,
            },
            local_addr: PeerAddr {
                ip: Some("127.0.0.1".parse().unwrap()),
                hostname: format!("truffle-test-{id}"),
                dns_name: None,
            },
            peer_event_tx,
            incoming_remote_identity: String::new(),
        }
    }

    /// Attach a fixed WhoIs identity JSON to every incoming connection, so
    /// the server-side hello exchange can verify a claimed `tailscale_id`
    /// against it. Used only by the identity-verification tests.
    fn with_incoming_identity(mut self, json: &str) -> Self {
        self.incoming_remote_identity = json.to_string();
        self
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

        let remote_identity = self.incoming_remote_identity.clone();

        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, addr)) => {
                        let conn = IncomingConnection {
                            stream,
                            remote_addr: addr.to_string(),
                            remote_identity: remote_identity.clone(),
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

    async fn bind_udp(&self, _port: u16) -> Result<crate::network::NetworkUdpSocket, NetworkError> {
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

// ===========================================================================
// Handshake serialization tests
// ===========================================================================

#[test]
fn handshake_serialize_roundtrip() {
    let hs = Handshake {
        peer_id: "node-abc123".to_string(),
        capabilities: vec!["ws".to_string(), "binary".to_string()],
        protocol_version: PROTOCOL_VERSION,
    };

    let json = serde_json::to_string(&hs).unwrap();
    let parsed: Handshake = serde_json::from_str(&json).unwrap();

    assert_eq!(hs, parsed);
}

#[test]
fn handshake_json_structure() {
    let hs = Handshake {
        peer_id: "test-peer".to_string(),
        capabilities: vec!["ws".to_string()],
        protocol_version: 1,
    };

    let json = serde_json::to_string(&hs).unwrap();
    let value: serde_json::Value = serde_json::from_str(&json).unwrap();

    assert_eq!(value["peer_id"], "test-peer");
    assert_eq!(value["protocol_version"], 1);
    assert!(value["capabilities"].is_array());
    assert_eq!(value["capabilities"][0], "ws");
}

#[test]
fn handshake_deserialize_unknown_fields_ignored() {
    let json = r#"{
        "peer_id": "node-x",
        "capabilities": ["ws"],
        "protocol_version": 1,
        "extra_field": "should be ignored"
    }"#;

    // serde default behavior is to ignore unknown fields
    let hs: Handshake = serde_json::from_str(json).unwrap();
    assert_eq!(hs.peer_id, "node-x");
    assert_eq!(hs.protocol_version, 1);
}

#[test]
fn handshake_empty_capabilities() {
    let hs = Handshake {
        peer_id: "minimal".to_string(),
        capabilities: vec![],
        protocol_version: 1,
    };

    let json = serde_json::to_string(&hs).unwrap();
    let parsed: Handshake = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed.capabilities.len(), 0);
}

// ===========================================================================
// WsConfig defaults
// ===========================================================================

#[test]
fn ws_config_defaults() {
    let config = WsConfig::default();
    assert_eq!(config.port, 9417);
    assert_eq!(config.ping_interval, Duration::from_secs(10));
    assert_eq!(config.pong_timeout, Duration::from_secs(30));
    assert_eq!(config.max_message_size, 16 * 1024 * 1024);
    assert_eq!(config.max_pending_handshakes, 256);
    assert_eq!(config.handshake_timeout, Duration::from_secs(10));
}

// ===========================================================================
// TransportError formatting
// ===========================================================================

#[test]
fn transport_error_display() {
    let err = TransportError::NotImplemented("QUIC".to_string());
    assert_eq!(err.to_string(), "not implemented: QUIC");

    let err = TransportError::VersionMismatch {
        local: 1,
        remote: 2,
    };
    assert_eq!(
        err.to_string(),
        "protocol version mismatch: local=1, remote=2"
    );

    let err = TransportError::HeartbeatTimeout(Duration::from_secs(30));
    assert_eq!(err.to_string(), "heartbeat timeout after 30s");
}

// ===========================================================================
// WebSocket transport integration tests (loopback)
// ===========================================================================

#[tokio::test]
async fn ws_connect_and_exchange_messages() {
    use crate::transport::websocket::WebSocketTransport;

    // Create two mock providers (server and client)
    let server_provider = Arc::new(MockNetworkProvider::new("server"));
    let client_provider = Arc::new(MockNetworkProvider::new("client"));

    // Pick a random high port
    let port = {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        l.local_addr().unwrap().port()
    };

    let config = WsConfig {
        port,
        ping_interval: Duration::from_secs(60), // long interval for test
        pong_timeout: Duration::from_secs(60),
        ..Default::default()
    };

    let server_ws = WebSocketTransport::new(server_provider, config.clone());
    let mut listener = server_ws.listen().await.unwrap();
    assert_eq!(listener.port, port);

    // Client: connect
    let client_ws = WebSocketTransport::new(client_provider, config);

    let peer_addr = PeerAddr {
        ip: Some("127.0.0.1".parse().unwrap()),
        hostname: "localhost".to_string(),
        dns_name: None,
    };

    // Connect client and accept on server concurrently
    let (client_result, server_stream) = tokio::join!(client_ws.connect(&peer_addr), async {
        listener.accept().await
    });

    let mut client_stream = client_result.expect("client connect should succeed");
    let mut server_stream = server_stream.expect("server should accept a connection");

    // Verify peer addresses
    assert!(!client_stream.peer_addr().is_empty());
    assert!(!server_stream.peer_addr().is_empty());

    // Verify remote peer IDs from handshake
    assert_eq!(client_stream.remote_peer_id(), "server");
    assert_eq!(server_stream.remote_peer_id(), "client");

    // Client sends, server receives
    let msg = b"hello from client";
    client_stream.send(msg).await.unwrap();

    let received = server_stream
        .recv()
        .await
        .unwrap()
        .expect("should receive message");
    assert_eq!(received, msg);

    // Server sends, client receives
    let reply = b"hello from server";
    server_stream.send(reply).await.unwrap();

    let received = client_stream
        .recv()
        .await
        .unwrap()
        .expect("should receive reply");
    assert_eq!(received, reply);

    // Clean close
    client_stream.close().await.unwrap();
    server_stream.close().await.unwrap();
}

#[tokio::test]
async fn ws_rejects_hello_with_mismatched_authenticated_identity() {
    use crate::transport::websocket::WebSocketTransport;

    // The server's bridge reports a WhoIs-authenticated nodeId of
    // "real-node-id" for the incoming connection, but the client's hello
    // will claim tailscale_id "client" — an impersonation attempt.
    let server_provider = Arc::new(
        MockNetworkProvider::new("server")
            .with_incoming_identity(r#"{"dnsName":"mallory.ts.net","nodeId":"real-node-id"}"#),
    );
    let client_provider = Arc::new(MockNetworkProvider::new("client"));

    let port = {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        l.local_addr().unwrap().port()
    };

    let config = WsConfig {
        port,
        ping_interval: Duration::from_secs(60),
        pong_timeout: Duration::from_secs(60),
        ..Default::default()
    };

    let server_ws = WebSocketTransport::new(server_provider, config.clone());
    let mut listener = server_ws.listen().await.unwrap();

    let client_ws = WebSocketTransport::new(client_provider, config);
    let peer_addr = PeerAddr {
        ip: Some("127.0.0.1".parse().unwrap()),
        hostname: "localhost".to_string(),
        dns_name: None,
    };

    // The server must close with code 4003 before replying, so the client's
    // hello exchange errors and the listener never yields a stream.
    let (client_result, server_accept) = tokio::join!(
        client_ws.connect(&peer_addr),
        tokio::time::timeout(Duration::from_secs(2), listener.accept())
    );

    assert!(
        client_result.is_err(),
        "impersonating client must not complete the hello exchange"
    );
    assert!(
        server_accept.is_err() || server_accept.unwrap().is_none(),
        "server must not yield a stream for a mismatched identity"
    );
}

#[tokio::test]
async fn ws_accepts_hello_with_matching_authenticated_identity() {
    use crate::transport::websocket::WebSocketTransport;

    // The bridge-authenticated nodeId matches the tailscale_id the client
    // advertises in its hello ("client"), so verification passes and the
    // honest peer connects and routes by its real peer id.
    let server_provider = Arc::new(
        MockNetworkProvider::new("server")
            .with_incoming_identity(r#"{"dnsName":"client.ts.net","nodeId":"client"}"#),
    );
    let client_provider = Arc::new(MockNetworkProvider::new("client"));

    let port = {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        l.local_addr().unwrap().port()
    };

    let config = WsConfig {
        port,
        ping_interval: Duration::from_secs(60),
        pong_timeout: Duration::from_secs(60),
        ..Default::default()
    };

    let server_ws = WebSocketTransport::new(server_provider, config.clone());
    let mut listener = server_ws.listen().await.unwrap();

    let client_ws = WebSocketTransport::new(client_provider, config);
    let peer_addr = PeerAddr {
        ip: Some("127.0.0.1".parse().unwrap()),
        hostname: "localhost".to_string(),
        dns_name: None,
    };

    let (client_result, server_stream) = tokio::join!(client_ws.connect(&peer_addr), async {
        listener.accept().await
    });

    let mut client_stream = client_result.expect("honest client connect should succeed");
    let mut server_stream = server_stream.expect("server should accept the matching connection");

    assert_eq!(server_stream.remote_peer_id(), "client");

    client_stream.close().await.unwrap();
    server_stream.close().await.unwrap();
}

#[tokio::test]
async fn ws_handshake_version_mismatch() {
    // This tests the handshake parsing logic directly since we can't easily
    // inject a version mismatch through the full transport flow without
    // another WS implementation.
    let hs_v1 = Handshake {
        peer_id: "node-a".to_string(),
        capabilities: vec!["ws".to_string()],
        protocol_version: 1,
    };
    let hs_v99 = Handshake {
        peer_id: "node-b".to_string(),
        capabilities: vec!["ws".to_string()],
        protocol_version: 99,
    };

    // Serialization works for both
    let json_v1 = serde_json::to_string(&hs_v1).unwrap();
    let json_v99 = serde_json::to_string(&hs_v99).unwrap();

    let parsed_v1: Handshake = serde_json::from_str(&json_v1).unwrap();
    let parsed_v99: Handshake = serde_json::from_str(&json_v99).unwrap();

    // Version comparison logic
    assert_eq!(parsed_v1.protocol_version, PROTOCOL_VERSION);
    assert_ne!(parsed_v99.protocol_version, PROTOCOL_VERSION);
}

#[tokio::test]
async fn ws_binary_frame_roundtrip() {
    use crate::transport::websocket::WebSocketTransport;

    let server_provider = Arc::new(MockNetworkProvider::new("server"));
    let client_provider = Arc::new(MockNetworkProvider::new("client"));

    let port = {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        l.local_addr().unwrap().port()
    };

    let config = WsConfig {
        port,
        ping_interval: Duration::from_secs(60),
        pong_timeout: Duration::from_secs(60),
        ..Default::default()
    };

    let server_ws = WebSocketTransport::new(server_provider, config.clone());
    let mut listener = server_ws.listen().await.unwrap();

    let client_ws = WebSocketTransport::new(client_provider, config);
    let peer_addr = PeerAddr {
        ip: Some("127.0.0.1".parse().unwrap()),
        hostname: "localhost".to_string(),
        dns_name: None,
    };

    let (client_result, server_stream) = tokio::join!(client_ws.connect(&peer_addr), async {
        listener.accept().await
    });

    let mut client_stream = client_result.unwrap();
    let mut server_stream = server_stream.unwrap();

    // Test various binary payloads
    let test_cases: Vec<Vec<u8>> = vec![
        vec![],                               // empty
        vec![0x00],                           // single null byte
        vec![0xFF; 1024],                     // 1KB of 0xFF
        (0..=255).map(|b| b as u8).collect(), // all byte values
        b"utf8 text as binary".to_vec(),
    ];

    for payload in &test_cases {
        client_stream.send(payload).await.unwrap();
        let received = server_stream.recv().await.unwrap().expect("should receive");
        assert_eq!(
            &received,
            payload,
            "binary roundtrip failed for payload of len {}",
            payload.len()
        );
    }

    client_stream.close().await.unwrap();
    server_stream.close().await.unwrap();
}

// ===========================================================================
// WS handshake DoS bounds (max_pending_handshakes + handshake_timeout)
// ===========================================================================

#[tokio::test]
async fn ws_drops_stalled_pre_hello_connection_after_handshake_timeout() {
    use crate::transport::websocket::WebSocketTransport;
    use tokio::io::AsyncReadExt;

    let server_provider = Arc::new(MockNetworkProvider::new("server"));

    let port = {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        l.local_addr().unwrap().port()
    };

    // Short handshake timeout so the test resolves quickly.
    let config = WsConfig {
        port,
        ping_interval: Duration::from_secs(60),
        pong_timeout: Duration::from_secs(60),
        handshake_timeout: Duration::from_millis(300),
        ..Default::default()
    };

    let server_ws = WebSocketTransport::new(server_provider, config);
    // Keep the listener alive so the accept loop keeps running.
    let _listener = server_ws.listen().await.unwrap();

    // Open a raw TCP connection and send nothing — this stalls before the
    // WebSocket upgrade even begins, modeling a pre-hello DoS attacker.
    let mut stalled = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

    // With the 300ms handshake timeout the server must drop the socket, so a
    // read resolves to EOF (Ok(0)) or a reset (Err) well within 3s. Before the
    // fix, accept_async_with_config has no timeout and the read hangs the full
    // 3s, failing the test.
    let mut buf = [0u8; 16];
    match tokio::time::timeout(Duration::from_secs(3), stalled.read(&mut buf)).await {
        Ok(Ok(0)) => { /* EOF — connection dropped as expected */ }
        Ok(Err(_)) => { /* reset — also an acceptable drop */ }
        Ok(Ok(n)) => panic!("stalled pre-hello connection unexpectedly received {n} bytes"),
        Err(_) => panic!("stalled pre-hello connection was not dropped"),
    }
}

#[tokio::test]
async fn ws_caps_concurrent_pending_handshakes() {
    use crate::transport::websocket::WebSocketTransport;
    use tokio::io::AsyncReadExt;

    let server_provider = Arc::new(MockNetworkProvider::new("server"));

    let port = {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        l.local_addr().unwrap().port()
    };

    // Cap at 2 in-flight handshakes with a long timeout, so the two stalled
    // connections hold their permits for the whole test.
    let config = WsConfig {
        port,
        ping_interval: Duration::from_secs(60),
        pong_timeout: Duration::from_secs(60),
        max_pending_handshakes: 2,
        handshake_timeout: Duration::from_secs(60),
        ..Default::default()
    };

    let server_ws = WebSocketTransport::new(server_provider, config);
    let _listener = server_ws.listen().await.unwrap();

    // Two silent connections claim both handshake permits.
    let _first = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let _second = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

    // Give the accept loop time to pick both up and acquire their permits.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // The third connection finds no permit left, so the accept loop must drop
    // it immediately (pre-upgrade). Before the fix there is no cap, so it stays
    // open and the read times out, failing the test.
    let mut third = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let mut buf = [0u8; 16];
    match tokio::time::timeout(Duration::from_secs(2), third.read(&mut buf)).await {
        Ok(Ok(0)) => { /* EOF — third connection dropped as expected */ }
        Ok(Err(_)) => { /* reset — also an acceptable drop */ }
        Ok(Ok(n)) => panic!("third pending connection unexpectedly received {n} bytes"),
        Err(_) => panic!("third pending connection was not dropped despite the handshake cap"),
    }
}

// ===========================================================================
// TCP transport tests
// ===========================================================================

#[tokio::test]
async fn tcp_open_and_transfer() {
    use crate::transport::tcp::TcpTransport;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let provider = Arc::new(MockNetworkProvider::new("tcp-test"));

    // Start a raw TCP server
    let tcp_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = tcp_listener.local_addr().unwrap().port();

    let server_handle = tokio::spawn(async move {
        let (mut stream, _addr) = tcp_listener.accept().await.unwrap();
        let mut buf = [0u8; 64];
        let n = stream.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"hello tcp");
        stream.write_all(b"goodbye tcp").await.unwrap();
    });

    // Client: open via TcpTransport
    let tcp = TcpTransport::new(provider);
    let peer_addr = PeerAddr {
        ip: Some("127.0.0.1".parse().unwrap()),
        hostname: "localhost".to_string(),
        dns_name: None,
    };

    let mut stream = tcp.open(&peer_addr, port).await.unwrap();

    stream.write_all(b"hello tcp").await.unwrap();

    let mut buf = [0u8; 64];
    let n = stream.read(&mut buf).await.unwrap();
    assert_eq!(&buf[..n], b"goodbye tcp");

    server_handle.await.unwrap();
}

#[tokio::test]
async fn tcp_listen_and_accept() {
    use crate::transport::tcp::TcpTransport;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let provider = Arc::new(MockNetworkProvider::new("tcp-listen"));
    let tcp = TcpTransport::new(provider);

    // Listen on ephemeral port
    let mut listener = tcp.listen(0).await.unwrap();
    let port = listener.port;

    // Client connects directly
    let client_handle = tokio::spawn(async move {
        let mut stream = TcpStream::connect(format!("127.0.0.1:{port}"))
            .await
            .unwrap();
        stream.write_all(b"from client").await.unwrap();
        stream.shutdown().await.unwrap();
    });

    // Accept on the RawListener
    let incoming = listener.accept().await.expect("should accept connection");
    // The mock provider attaches no WhoIs identity, so the raw path surfaces
    // None (parsed from the empty bridge-header identity string).
    assert!(incoming.remote_identity.is_none());
    let mut stream = incoming.stream;

    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.unwrap();
    assert_eq!(buf, b"from client");

    client_handle.await.unwrap();
}

#[tokio::test]
async fn tcp_transport_wraps_network_provider() {
    use crate::transport::tcp::TcpTransport;

    let provider = Arc::new(MockNetworkProvider::new("wrapper-test"));
    let tcp = TcpTransport::new(provider);

    // Attempting to connect to a non-existent address should fail with
    // ConnectFailed, proving the transport delegates to NetworkProvider.
    let addr = PeerAddr {
        ip: Some("127.0.0.1".parse().unwrap()),
        hostname: "localhost".to_string(),
        dns_name: None,
    };

    let result = tcp.open(&addr, 1).await; // port 1 should be refused
    assert!(result.is_err());
    let err = result.unwrap_err();
    match err {
        TransportError::ConnectFailed(msg) => {
            assert!(
                msg.contains("tcp dial"),
                "error should mention tcp dial: {msg}"
            );
        }
        other => panic!("expected ConnectFailed, got: {other}"),
    }
}

// ===========================================================================
// UDP transport tests
// ===========================================================================

#[tokio::test]
async fn udp_send_recv() {
    use crate::transport::udp::{UdpConfig, UdpTransport};

    let provider = Arc::new(MockNetworkProvider::new("udp-test"));
    let udp = UdpTransport::new(provider, UdpConfig::default());

    // Bind two sockets on ephemeral ports
    let socket_a = udp.bind(0).await.unwrap();
    let socket_b = udp.bind(0).await.unwrap();

    // Use 127.0.0.1 with the assigned port (local_addr may return 0.0.0.0)
    let port_a = socket_a.local_addr().unwrap().port();
    let port_b = socket_b.local_addr().unwrap().port();
    let addr_a = format!("127.0.0.1:{port_a}");
    let addr_b = format!("127.0.0.1:{port_b}");

    // Send from A to B
    let msg = b"hello udp";
    let sent = socket_a.send_to(msg, &addr_b).await.unwrap();
    assert_eq!(sent, msg.len());

    // Receive on B
    let mut buf = [0u8; 1024];
    let (n, sender) = socket_b.recv_from(&mut buf).await.unwrap();
    assert_eq!(&buf[..n], msg);
    assert_eq!(sender, addr_a);
}

#[tokio::test]
async fn udp_large_datagram() {
    use crate::transport::udp::{UdpConfig, UdpTransport};

    let provider = Arc::new(MockNetworkProvider::new("udp-large"));
    let udp = UdpTransport::new(provider, UdpConfig::default());

    let socket_a = udp.bind(0).await.unwrap();
    let socket_b = udp.bind(0).await.unwrap();

    let port_b = socket_b.local_addr().unwrap().port();
    let addr_b = format!("127.0.0.1:{port_b}");

    // MTU-sized payload (~1400 bytes, typical for Ethernet minus headers)
    let payload: Vec<u8> = (0..1400).map(|i| (i % 256) as u8).collect();

    let sent = socket_a.send_to(&payload, &addr_b).await.unwrap();
    assert_eq!(sent, payload.len());

    let mut buf = [0u8; 2048];
    let (n, _sender) = socket_b.recv_from(&mut buf).await.unwrap();
    assert_eq!(&buf[..n], &payload[..]);
}

// ===========================================================================
// QUIC transport tests
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn quic_connect_and_handshake() {
    use crate::transport::quic::{QuicConfig, QuicTransport};

    let server_provider = Arc::new(MockNetworkProvider::new("quic-server"));
    let client_provider = Arc::new(MockNetworkProvider::new("quic-client"));

    // Use port 0 so the OS assigns an ephemeral port
    let server_config = QuicConfig {
        port: 0,
        max_streams: 100,
    };

    let server_quic = QuicTransport::new(server_provider, server_config);
    let mut listener = server_quic.listen().await.unwrap();
    let actual_port = listener.port;
    assert_ne!(actual_port, 0, "should have been assigned a real port");

    // Client config uses the actual port from the server listener
    let client_config = QuicConfig {
        port: actual_port,
        max_streams: 100,
    };

    let client_quic = QuicTransport::new(client_provider, client_config);
    let peer_addr = PeerAddr {
        ip: Some("127.0.0.1".parse().unwrap()),
        hostname: "localhost".to_string(),
        dns_name: None,
    };

    // Connect client and accept on server concurrently
    let (client_result, server_stream) = tokio::join!(client_quic.connect(&peer_addr), async {
        listener.accept().await
    });

    let client_stream = client_result.expect("client connect should succeed");
    let server_stream = server_stream.expect("server should accept a connection");

    // Verify peer IDs from handshake — the QUIC hello carries the durable
    // device_id, which the mock prefixes with `dev-` (RFC 022 I1).
    assert_eq!(client_stream.remote_peer_id(), "dev-quic-server");
    assert_eq!(server_stream.remote_peer_id(), "dev-quic-client");

    // Verify peer addresses are non-empty
    assert!(!client_stream.peer_addr().is_empty());
    assert!(!server_stream.peer_addr().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn quic_send_recv() {
    use crate::transport::quic::{QuicConfig, QuicTransport};

    let server_provider = Arc::new(MockNetworkProvider::new("quic-send-server"));
    let client_provider = Arc::new(MockNetworkProvider::new("quic-send-client"));

    let server_config = QuicConfig {
        port: 0,
        max_streams: 100,
    };

    let server_quic = QuicTransport::new(server_provider, server_config);
    let mut listener = server_quic.listen().await.unwrap();
    let actual_port = listener.port;

    let client_config = QuicConfig {
        port: actual_port,
        max_streams: 100,
    };

    let client_quic = QuicTransport::new(client_provider, client_config);
    let peer_addr = PeerAddr {
        ip: Some("127.0.0.1".parse().unwrap()),
        hostname: "localhost".to_string(),
        dns_name: None,
    };

    let (client_result, server_stream) = tokio::join!(client_quic.connect(&peer_addr), async {
        listener.accept().await
    });

    let mut client_stream = client_result.unwrap();
    let mut server_stream = server_stream.unwrap();

    // Client sends, server receives
    let msg = b"hello from quic client";
    client_stream.send(msg).await.unwrap();

    let received = server_stream
        .recv()
        .await
        .unwrap()
        .expect("should receive message");
    assert_eq!(received, msg);

    // Server sends, client receives
    let reply = b"hello from quic server";
    server_stream.send(reply).await.unwrap();

    let received = client_stream
        .recv()
        .await
        .unwrap()
        .expect("should receive reply");
    assert_eq!(received, reply);

    // Clean close
    client_stream.close().await.unwrap();
    server_stream.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn quic_bidirectional() {
    use crate::transport::quic::{QuicConfig, QuicTransport};

    let server_provider = Arc::new(MockNetworkProvider::new("quic-bidi-server"));
    let client_provider = Arc::new(MockNetworkProvider::new("quic-bidi-client"));

    let server_config = QuicConfig {
        port: 0,
        max_streams: 100,
    };

    let server_quic = QuicTransport::new(server_provider, server_config);
    let mut listener = server_quic.listen().await.unwrap();
    let actual_port = listener.port;

    let client_config = QuicConfig {
        port: actual_port,
        max_streams: 100,
    };

    let client_quic = QuicTransport::new(client_provider, client_config);
    let peer_addr = PeerAddr {
        ip: Some("127.0.0.1".parse().unwrap()),
        hostname: "localhost".to_string(),
        dns_name: None,
    };

    let (client_result, server_stream) = tokio::join!(client_quic.connect(&peer_addr), async {
        listener.accept().await
    });

    let mut client_stream = client_result.unwrap();
    let mut server_stream = server_stream.unwrap();

    // Test various binary payloads bidirectionally
    let test_cases: Vec<Vec<u8>> = vec![
        vec![],                               // empty
        vec![0x00],                           // single null byte
        vec![0xFF; 1024],                     // 1KB of 0xFF
        (0..=255).map(|b| b as u8).collect(), // all byte values
        b"utf8 text as binary".to_vec(),
    ];

    for payload in &test_cases {
        // Client -> Server
        client_stream.send(payload).await.unwrap();
        let received = server_stream.recv().await.unwrap().expect("should receive");
        assert_eq!(
            &received,
            payload,
            "client->server roundtrip failed for payload of len {}",
            payload.len()
        );

        // Server -> Client
        server_stream.send(payload).await.unwrap();
        let received = client_stream.recv().await.unwrap().expect("should receive");
        assert_eq!(
            &received,
            payload,
            "server->client roundtrip failed for payload of len {}",
            payload.len()
        );
    }

    client_stream.close().await.unwrap();
    server_stream.close().await.unwrap();
}

#[tokio::test]
async fn quic_multiple_sequential_messages() {
    use crate::transport::quic::{QuicConfig, QuicTransport};

    let server_provider = Arc::new(MockNetworkProvider::new("quic-multi-server"));
    let client_provider = Arc::new(MockNetworkProvider::new("quic-multi-client"));

    let server_config = QuicConfig {
        port: 0,
        max_streams: 100,
    };

    let server_quic = QuicTransport::new(server_provider, server_config);
    let mut listener = server_quic.listen().await.unwrap();
    let actual_port = listener.port;

    let client_config = QuicConfig {
        port: actual_port,
        max_streams: 100,
    };

    let client_quic = QuicTransport::new(client_provider, client_config);
    let peer_addr = PeerAddr {
        ip: Some("127.0.0.1".parse().unwrap()),
        hostname: "localhost".to_string(),
        dns_name: None,
    };

    let (client_result, server_stream) = tokio::join!(client_quic.connect(&peer_addr), async {
        listener.accept().await
    });

    let mut client_stream = client_result.unwrap();
    let mut server_stream = server_stream.unwrap();

    // Send 50 messages rapidly and verify ordering
    let count = 50;
    for i in 0..count {
        let msg = format!("message-{i}");
        client_stream.send(msg.as_bytes()).await.unwrap();
    }

    for i in 0..count {
        let received = server_stream.recv().await.unwrap().expect("should receive");
        let expected = format!("message-{i}");
        assert_eq!(
            received,
            expected.as_bytes(),
            "message ordering broken at index {i}"
        );
    }

    client_stream.close().await.unwrap();
    server_stream.close().await.unwrap();
}

// ===========================================================================
// Heartbeat logic tests
// ===========================================================================

#[tokio::test]
async fn ws_heartbeat_keeps_connection_alive() {
    use crate::transport::websocket::WebSocketTransport;

    let server_provider = Arc::new(MockNetworkProvider::new("hb-server"));
    let client_provider = Arc::new(MockNetworkProvider::new("hb-client"));

    let port = {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        l.local_addr().unwrap().port()
    };

    // Short heartbeat interval for testing
    let config = WsConfig {
        port,
        ping_interval: Duration::from_millis(100),
        pong_timeout: Duration::from_secs(5),
        ..Default::default()
    };

    let server_ws = WebSocketTransport::new(server_provider, config.clone());
    let mut listener = server_ws.listen().await.unwrap();

    let client_ws = WebSocketTransport::new(client_provider, config);
    let peer_addr = PeerAddr {
        ip: Some("127.0.0.1".parse().unwrap()),
        hostname: "localhost".to_string(),
        dns_name: None,
    };

    let (client_result, server_stream) = tokio::join!(client_ws.connect(&peer_addr), async {
        listener.accept().await
    });

    let mut client_stream = client_result.unwrap();
    let mut server_stream = server_stream.unwrap();

    // Wait long enough for several heartbeats to fire
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Connection should still be alive — send/receive should work
    client_stream.send(b"still alive").await.unwrap();
    let received = server_stream
        .recv()
        .await
        .unwrap()
        .expect("should receive after heartbeats");
    assert_eq!(received, b"still alive");

    client_stream.close().await.unwrap();
    server_stream.close().await.unwrap();
}

// ===========================================================================
// StreamListener and RawListener tests
// ===========================================================================

#[tokio::test]
async fn stream_listener_returns_none_on_channel_close() {
    use crate::transport::websocket::WsFramedStream;
    use crate::transport::StreamListener;

    let (tx, rx) = tokio::sync::mpsc::channel::<WsFramedStream>(1);
    let mut listener = StreamListener::new(rx, 9999);

    // Drop the sender
    drop(tx);

    // accept should return None
    assert!(listener.accept().await.is_none());
}

#[tokio::test]
async fn raw_listener_returns_none_on_channel_close() {
    use crate::transport::{RawIncoming, RawListener};

    let (tx, rx) = tokio::sync::mpsc::channel::<RawIncoming>(1);
    let mut listener = RawListener::new(rx, 9999);

    drop(tx);
    assert!(listener.accept().await.is_none());
}

// ===========================================================================
// Heartbeat timeout detection test
// ===========================================================================

#[tokio::test]
async fn ws_heartbeat_timeout_detects_dead_peer() {
    use crate::transport::websocket::WebSocketTransport;

    let server_provider = Arc::new(MockNetworkProvider::new("hb-timeout-server"));
    let client_provider = Arc::new(MockNetworkProvider::new("hb-timeout-client"));

    let port = {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        l.local_addr().unwrap().port()
    };

    // Very short intervals so the test runs quickly
    let config = WsConfig {
        port,
        ping_interval: Duration::from_millis(50),
        pong_timeout: Duration::from_millis(150),
        ..Default::default()
    };

    let server_ws = WebSocketTransport::new(server_provider, config.clone());
    let mut listener = server_ws.listen().await.unwrap();

    let client_ws = WebSocketTransport::new(client_provider, config);
    let peer_addr = PeerAddr {
        ip: Some("127.0.0.1".parse().unwrap()),
        hostname: "localhost".to_string(),
        dns_name: None,
    };

    let (client_result, server_stream) = tokio::join!(client_ws.connect(&peer_addr), async {
        listener.accept().await
    });

    let mut client_stream = client_result.unwrap();
    let mut server_stream = server_stream.unwrap();

    // Verify connection works initially
    client_stream.send(b"hello").await.unwrap();
    let msg = server_stream.recv().await.unwrap().expect("should receive");
    assert_eq!(msg, b"hello");

    // Now stop reading from the server side (simulating dead peer from client's perspective).
    // The client's heartbeat will send Pings, but since the server is not reading,
    // the Pong responses won't flow back, and eventually the server's heartbeat
    // will detect the client is not responding either.
    //
    // We drop the server_stream to simulate the peer going away.
    drop(server_stream);

    // Wait for the heartbeat timeout to fire
    tokio::time::sleep(Duration::from_millis(400)).await;

    // The client should detect the broken connection: recv returns None or error
    let result = tokio::time::timeout(Duration::from_secs(2), client_stream.recv()).await;
    match result {
        Ok(Ok(None)) => { /* Clean close detected — expected */ }
        Ok(Err(_)) => { /* Transport error — also acceptable for dead peer */ }
        Ok(Ok(Some(_))) => panic!("should not receive data from a dead peer"),
        Err(_) => panic!("recv should not hang forever after heartbeat timeout"),
    }
}

// ===========================================================================
// WS remote close test
// ===========================================================================

#[tokio::test]
async fn ws_remote_close_returns_none() {
    use crate::transport::websocket::WebSocketTransport;

    let server_provider = Arc::new(MockNetworkProvider::new("close-server"));
    let client_provider = Arc::new(MockNetworkProvider::new("close-client"));

    let port = {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        l.local_addr().unwrap().port()
    };

    let config = WsConfig {
        port,
        ping_interval: Duration::from_secs(60),
        pong_timeout: Duration::from_secs(60),
        ..Default::default()
    };

    let server_ws = WebSocketTransport::new(server_provider, config.clone());
    let mut listener = server_ws.listen().await.unwrap();

    let client_ws = WebSocketTransport::new(client_provider, config);
    let peer_addr = PeerAddr {
        ip: Some("127.0.0.1".parse().unwrap()),
        hostname: "localhost".to_string(),
        dns_name: None,
    };

    let (client_result, server_stream) = tokio::join!(client_ws.connect(&peer_addr), async {
        listener.accept().await
    });

    let mut client_stream = client_result.unwrap();
    let mut server_stream = server_stream.unwrap();

    // Server gracefully closes
    server_stream.close().await.unwrap();

    // Client's recv should return Ok(None) indicating clean close
    let result = tokio::time::timeout(Duration::from_secs(2), client_stream.recv()).await;
    match result {
        Ok(Ok(None)) => { /* Expected: clean close signaled */ }
        Ok(Err(_)) => { /* Also acceptable: connection closed error */ }
        Ok(Ok(Some(data))) => panic!("unexpected data after close: {:?}", data),
        Err(_) => panic!("recv should not hang after remote close"),
    }

    client_stream.close().await.ok();
}

// ===========================================================================
// WS connection failure test
// ===========================================================================

#[tokio::test]
async fn ws_connect_to_unreachable_returns_connect_failed() {
    use crate::transport::websocket::WebSocketTransport;

    let provider = Arc::new(MockNetworkProvider::new("fail-client"));

    let config = WsConfig {
        port: 1, // port 1 should be unreachable / refused
        ping_interval: Duration::from_secs(60),
        pong_timeout: Duration::from_secs(60),
        ..Default::default()
    };

    let ws = WebSocketTransport::new(provider, config);
    let peer_addr = PeerAddr {
        ip: Some("127.0.0.1".parse().unwrap()),
        hostname: "localhost".to_string(),
        dns_name: None,
    };

    let result = ws.connect(&peer_addr).await;
    assert!(
        result.is_err(),
        "should fail to connect to unreachable port"
    );

    match result.unwrap_err() {
        TransportError::ConnectFailed(msg) => {
            assert!(
                msg.contains("dial tcp"),
                "error should mention dial tcp: {msg}"
            );
        }
        other => panic!("expected ConnectFailed, got: {other}"),
    }
}
