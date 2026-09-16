//! Integration tests for truffle-core Layer 4 (Transport).
//!
//! These tests exercise WebSocket, TCP, UDP, and QUIC transports under
//! stress conditions and cross-transport scenarios. They use loopback
//! networking via a [`MockNetworkProvider`] — no real Tailscale or external
//! infrastructure is required.
//!
//! All tests are marked `#[ignore]` so `cargo test` doesn't run them by
//! default. They can be heavy on resources (large transfers, many
//! concurrent connections).
//!
//! ## Running
//!
//! ```bash
//! cargo test -p truffle-core --test integration_transport -- --ignored --nocapture
//! ```

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{broadcast, mpsc};
use tokio::time::timeout;

use truffle_core::network::NetworkProvider;
use truffle_core::network::{
    HealthInfo, IncomingConnection, NetworkError, NetworkPeer, NetworkPeerEvent,
    NetworkTcpListener, NodeIdentity, PeerAddr, PingResult,
};
use truffle_core::transport::{
    DatagramTransport, FramedStream, RawTransport, StreamTransport, WsConfig,
};

// ============================================================================
// MockNetworkProvider (duplicated from unit tests for integration test crate)
// ============================================================================

/// A mock network provider that uses local TCP for testing.
///
/// `dial_tcp` connects to `127.0.0.1:{port}` directly.
/// `listen_tcp` binds a local TCP listener and forwards connections.
struct MockNetworkProvider {
    identity: NodeIdentity,
    local_addr: PeerAddr,
    peer_event_tx: broadcast::Sender<NetworkPeerEvent>,
}

impl MockNetworkProvider {
    fn new(id: &str) -> Self {
        let (peer_event_tx, _) = broadcast::channel(16);
        Self {
            identity: NodeIdentity {
                app_id: "test".to_string(),
                device_id: format!("device-{id}"),
                device_name: format!("Test Node {id}"),
                tailscale_hostname: format!("truffle-test-{id}"),
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
        }
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

    async fn bind_udp(
        &self,
        _port: u16,
    ) -> Result<truffle_core::network::NetworkUdpSocket, NetworkError> {
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

// ============================================================================
// Test helpers
// ============================================================================

/// Allocate an ephemeral port by briefly binding and releasing.
async fn ephemeral_port() -> u16 {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    l.local_addr().unwrap().port()
}

/// Build a loopback PeerAddr targeting 127.0.0.1.
fn loopback_peer_addr() -> PeerAddr {
    PeerAddr {
        ip: Some("127.0.0.1".parse().unwrap()),
        hostname: "localhost".to_string(),
        dns_name: None,
    }
}

/// Build a WsConfig on the given port with long heartbeat (avoids interference).
fn ws_config(port: u16) -> WsConfig {
    WsConfig {
        port,
        ping_interval: Duration::from_secs(60),
        pong_timeout: Duration::from_secs(60),
        ..Default::default()
    }
}

/// Default test timeout for the entire test.
const TEST_TIMEOUT: Duration = Duration::from_secs(30);

// ============================================================================
// WebSocket stress tests
// ============================================================================

/// Both sides send 100 messages concurrently, verify all received.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_ws_concurrent_bidirectional() {
    let _ = tracing_subscriber::fmt::try_init();

    timeout(TEST_TIMEOUT, async {
        use truffle_core::transport::websocket::WebSocketTransport;

        let port = ephemeral_port().await;
        let server_provider = Arc::new(MockNetworkProvider::new("bidi-server"));
        let client_provider = Arc::new(MockNetworkProvider::new("bidi-client"));
        let config = ws_config(port);

        let server_ws = WebSocketTransport::new(server_provider, config.clone());
        let mut listener = server_ws.listen().await.unwrap();

        let client_ws = WebSocketTransport::new(client_provider, config);
        let peer_addr = loopback_peer_addr();

        let (client_result, server_stream) = tokio::join!(client_ws.connect(&peer_addr), async {
            listener.accept().await
        });

        let mut client_stream = client_result.expect("client connect should succeed");
        let mut server_stream = server_stream.expect("server should accept");

        let msg_count = 100;

        // Split: client sends on one task, receives on another.
        // Server does the mirror.

        // Client send task
        let client_send = tokio::spawn(async move {
            for i in 0..msg_count {
                let msg = format!("client-{i}");
                client_stream.send(msg.as_bytes()).await.unwrap();
            }
            // Now receive server's messages
            let mut received = Vec::new();
            for _ in 0..msg_count {
                let data = client_stream.recv().await.unwrap().expect("should receive");
                received.push(String::from_utf8(data).unwrap());
            }
            (client_stream, received)
        });

        // Server send + recv task
        let server_task = tokio::spawn(async move {
            // Send server's messages
            for i in 0..msg_count {
                let msg = format!("server-{i}");
                server_stream.send(msg.as_bytes()).await.unwrap();
            }
            // Receive client's messages
            let mut received = Vec::new();
            for _ in 0..msg_count {
                let data = server_stream.recv().await.unwrap().expect("should receive");
                received.push(String::from_utf8(data).unwrap());
            }
            (server_stream, received)
        });

        let (client_result, server_result) = tokio::join!(client_send, server_task);
        let (mut client_stream, client_received) = client_result.unwrap();
        let (mut server_stream, server_received) = server_result.unwrap();

        // Verify client received all server messages
        assert_eq!(client_received.len(), msg_count);
        for (i, message) in client_received.iter().enumerate() {
            assert_eq!(message, &format!("server-{i}"));
        }

        // Verify server received all client messages
        assert_eq!(server_received.len(), msg_count);
        for (i, message) in server_received.iter().enumerate() {
            assert_eq!(message, &format!("client-{i}"));
        }

        client_stream.close().await.unwrap();
        server_stream.close().await.unwrap();
    })
    .await
    .expect("test timed out");
}

/// Send a 1MB message over WebSocket, verify integrity.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_ws_large_message() {
    let _ = tracing_subscriber::fmt::try_init();

    timeout(TEST_TIMEOUT, async {
        use truffle_core::transport::websocket::WebSocketTransport;

        let port = ephemeral_port().await;
        let server_provider = Arc::new(MockNetworkProvider::new("large-server"));
        let client_provider = Arc::new(MockNetworkProvider::new("large-client"));
        // Default max is 16 MiB, so 1 MiB is fine
        let config = ws_config(port);

        let server_ws = WebSocketTransport::new(server_provider, config.clone());
        let mut listener = server_ws.listen().await.unwrap();

        let client_ws = WebSocketTransport::new(client_provider, config);
        let peer_addr = loopback_peer_addr();

        let (client_result, server_stream) = tokio::join!(client_ws.connect(&peer_addr), async {
            listener.accept().await
        });

        let mut client_stream = client_result.unwrap();
        let mut server_stream = server_stream.unwrap();

        // Create a 1 MB payload with a recognizable pattern
        let payload_size = 1024 * 1024; // 1 MB
        let payload: Vec<u8> = (0..payload_size).map(|i| (i % 251) as u8).collect();

        // Client -> Server
        client_stream.send(&payload).await.unwrap();
        let received = server_stream
            .recv()
            .await
            .unwrap()
            .expect("should receive 1MB");
        assert_eq!(received.len(), payload_size);
        assert_eq!(received, payload, "1MB payload integrity check failed");

        // Server -> Client (reverse direction)
        server_stream.send(&payload).await.unwrap();
        let received = client_stream
            .recv()
            .await
            .unwrap()
            .expect("should receive 1MB reply");
        assert_eq!(received.len(), payload_size);
        assert_eq!(
            received, payload,
            "1MB reverse payload integrity check failed"
        );

        client_stream.close().await.unwrap();
        server_stream.close().await.unwrap();
    })
    .await
    .expect("test timed out");
}

/// Connect, send, close, reconnect, send — verify both messages delivered.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_ws_rapid_reconnect() {
    let _ = tracing_subscriber::fmt::try_init();

    timeout(TEST_TIMEOUT, async {
        use truffle_core::transport::websocket::WebSocketTransport;

        let port = ephemeral_port().await;
        let server_provider = Arc::new(MockNetworkProvider::new("reconnect-server"));
        let client_provider = Arc::new(MockNetworkProvider::new("reconnect-client"));
        let config = ws_config(port);

        let server_ws = WebSocketTransport::new(server_provider, config.clone());
        let mut listener = server_ws.listen().await.unwrap();

        let client_ws = WebSocketTransport::new(client_provider, config);
        let peer_addr = loopback_peer_addr();

        // --- First connection ---
        let (client_result, server_stream) = tokio::join!(client_ws.connect(&peer_addr), async {
            listener.accept().await
        });

        let mut client_stream = client_result.unwrap();
        let mut server_stream = server_stream.unwrap();

        // Send first message
        client_stream.send(b"message-1").await.unwrap();
        let received = server_stream
            .recv()
            .await
            .unwrap()
            .expect("should receive msg 1");
        assert_eq!(received, b"message-1");

        // Close both sides
        client_stream.close().await.unwrap();
        server_stream.close().await.unwrap();

        // --- Second connection (rapid reconnect) ---
        let (client_result, server_stream) = tokio::join!(client_ws.connect(&peer_addr), async {
            listener.accept().await
        });

        let mut client_stream = client_result.unwrap();
        let mut server_stream = server_stream.unwrap();

        // Send second message
        client_stream.send(b"message-2").await.unwrap();
        let received = server_stream
            .recv()
            .await
            .unwrap()
            .expect("should receive msg 2");
        assert_eq!(received, b"message-2");

        client_stream.close().await.unwrap();
        server_stream.close().await.unwrap();
    })
    .await
    .expect("test timed out");
}

/// Accept 5 connections from the same listener, send on each.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_ws_multiple_connections_same_listener() {
    let _ = tracing_subscriber::fmt::try_init();

    timeout(TEST_TIMEOUT, async {
        use truffle_core::transport::websocket::WebSocketTransport;

        let port = ephemeral_port().await;
        let server_provider = Arc::new(MockNetworkProvider::new("multi-server"));
        let config = ws_config(port);

        let server_ws = WebSocketTransport::new(server_provider, config.clone());
        let mut listener = server_ws.listen().await.unwrap();

        let peer_addr = loopback_peer_addr();
        let num_connections = 5;

        // Spawn 5 client tasks that each connect, send, and receive
        let mut client_handles = Vec::new();
        for i in 0..num_connections {
            let client_provider = Arc::new(MockNetworkProvider::new(&format!("multi-client-{i}")));
            let client_ws = WebSocketTransport::new(client_provider, config.clone());
            let peer_addr = peer_addr.clone();

            let handle = tokio::spawn(async move {
                let mut stream = client_ws.connect(&peer_addr).await.unwrap();
                let msg = format!("hello from client {i}");
                stream.send(msg.as_bytes()).await.unwrap();
                // Wait for echo
                let reply = stream.recv().await.unwrap().expect("should receive echo");
                stream.close().await.unwrap();
                (i, String::from_utf8(reply).unwrap())
            });
            client_handles.push(handle);
        }

        // Server: accept 5 connections and echo back
        let mut server_handles = Vec::new();
        for _ in 0..num_connections {
            let mut server_stream = listener.accept().await.expect("should accept");
            let handle = tokio::spawn(async move {
                let data = server_stream.recv().await.unwrap().expect("should receive");
                server_stream.send(&data).await.unwrap();
                // Wait for client close
                let _ = server_stream.recv().await;
                server_stream.close().await.ok();
                String::from_utf8(data).unwrap()
            });
            server_handles.push(handle);
        }

        // Collect client results
        let mut client_results: Vec<(usize, String)> = Vec::new();
        for handle in client_handles {
            client_results.push(handle.await.unwrap());
        }

        // Collect server results
        let mut server_results: Vec<String> = Vec::new();
        for handle in server_handles {
            server_results.push(handle.await.unwrap());
        }

        // Verify each client got its own echo back
        for (i, reply) in &client_results {
            let expected = format!("hello from client {i}");
            assert_eq!(reply, &expected, "client {i} should get its own echo back");
        }

        // Verify server saw all 5 messages
        assert_eq!(server_results.len(), num_connections);
        server_results.sort();
        for i in 0..num_connections {
            assert!(
                server_results
                    .iter()
                    .any(|m| m == &format!("hello from client {i}")),
                "server should have seen message from client {i}"
            );
        }
    })
    .await
    .expect("test timed out");
}

/// Send 1000 numbered messages, verify they arrive in order.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_ws_message_ordering() {
    let _ = tracing_subscriber::fmt::try_init();

    timeout(TEST_TIMEOUT, async {
        use truffle_core::transport::websocket::WebSocketTransport;

        let port = ephemeral_port().await;
        let server_provider = Arc::new(MockNetworkProvider::new("order-server"));
        let client_provider = Arc::new(MockNetworkProvider::new("order-client"));
        let config = ws_config(port);

        let server_ws = WebSocketTransport::new(server_provider, config.clone());
        let mut listener = server_ws.listen().await.unwrap();

        let client_ws = WebSocketTransport::new(client_provider, config);
        let peer_addr = loopback_peer_addr();

        let (client_result, server_stream) = tokio::join!(client_ws.connect(&peer_addr), async {
            listener.accept().await
        });

        let mut client_stream = client_result.unwrap();
        let mut server_stream = server_stream.unwrap();

        let msg_count: usize = 1000;

        // Send all messages from client
        let send_task = tokio::spawn(async move {
            for i in 0..msg_count {
                // Encode the sequence number as 4-byte big-endian
                let seq_bytes = (i as u32).to_be_bytes();
                client_stream.send(&seq_bytes).await.unwrap();
            }
            client_stream
        });

        // Receive all messages on server
        let recv_task = tokio::spawn(async move {
            let mut received_seqs = Vec::with_capacity(msg_count);
            for _ in 0..msg_count {
                let data = server_stream.recv().await.unwrap().expect("should receive");
                assert_eq!(data.len(), 4, "each message should be 4 bytes");
                let seq = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
                received_seqs.push(seq);
            }
            (server_stream, received_seqs)
        });

        let (client_result, server_result) = tokio::join!(send_task, recv_task);
        let mut client_stream = client_result.unwrap();
        let (mut server_stream, received_seqs) = server_result.unwrap();

        // Verify strict ordering
        for (expected, actual) in received_seqs.iter().enumerate() {
            assert_eq!(
                *actual, expected as u32,
                "message ordering broken at index {expected}: got {actual}"
            );
        }
        assert_eq!(received_seqs.len(), msg_count);

        client_stream.close().await.unwrap();
        server_stream.close().await.unwrap();
    })
    .await
    .expect("test timed out");
}

// ============================================================================
// TCP stress tests
// ============================================================================

/// Transfer 10MB of data over TCP, verify SHA-256 integrity.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_tcp_large_transfer() {
    let _ = tracing_subscriber::fmt::try_init();

    timeout(Duration::from_secs(60), async {
        use truffle_core::transport::tcp::TcpTransport;

        let provider = Arc::new(MockNetworkProvider::new("tcp-large"));
        let tcp = TcpTransport::new(provider);

        // Listen on ephemeral port
        let mut listener = tcp.listen(0).await.unwrap();
        let port = listener.port;

        let data_size: usize = 10 * 1024 * 1024; // 10 MB

        // Build a deterministic 10MB payload
        let payload: Vec<u8> = (0..data_size).map(|i| ((i * 7 + 13) % 256) as u8).collect();
        let expected_payload = payload.clone();

        // Client: send the full payload
        let peer_addr = loopback_peer_addr();
        let send_task = tokio::spawn(async move {
            let mut stream = tcp.open(&peer_addr, port).await.unwrap();
            // Write in chunks to exercise buffering
            let chunk_size = 64 * 1024; // 64 KB chunks
            for chunk in payload.chunks(chunk_size) {
                stream.write_all(chunk).await.unwrap();
            }
            stream.shutdown().await.unwrap();
        });

        // Server: accept and read the full payload
        let recv_task = tokio::spawn(async move {
            let incoming = listener.accept().await.expect("should accept");
            let mut stream = incoming.stream;
            let mut buf = Vec::with_capacity(data_size);
            stream.read_to_end(&mut buf).await.unwrap();
            buf
        });

        let (send_result, recv_result) = tokio::join!(send_task, recv_task);
        send_result.unwrap();
        let received = recv_result.unwrap();

        assert_eq!(
            received.len(),
            data_size,
            "should receive exactly {} bytes, got {}",
            data_size,
            received.len()
        );
        assert_eq!(
            received, expected_payload,
            "10MB transfer data integrity check failed"
        );
    })
    .await
    .expect("test timed out");
}

/// Open 5 TCP connections to same peer, transfer data on each simultaneously.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_tcp_concurrent_streams() {
    let _ = tracing_subscriber::fmt::try_init();

    timeout(TEST_TIMEOUT, async {
        use truffle_core::transport::tcp::TcpTransport;

        let provider = Arc::new(MockNetworkProvider::new("tcp-concurrent"));
        let tcp = TcpTransport::new(provider.clone());

        let mut listener = tcp.listen(0).await.unwrap();
        let port = listener.port;

        let num_streams = 5;
        let bytes_per_stream = 100_000; // 100 KB per stream

        // Spawn server accept loop in background
        let server_handle = tokio::spawn(async move {
            let mut results: Vec<(usize, Vec<u8>)> = Vec::new();
            for _ in 0..num_streams {
                let incoming = listener.accept().await.expect("should accept");
                let mut stream = incoming.stream;
                // Each client sends a u32 stream ID first, then the data
                let mut id_buf = [0u8; 4];
                stream.read_exact(&mut id_buf).await.unwrap();
                let stream_id = u32::from_be_bytes(id_buf) as usize;
                let mut data = Vec::new();
                stream.read_to_end(&mut data).await.unwrap();
                results.push((stream_id, data));
            }
            results
        });

        // Spawn 5 client connections concurrently
        let mut client_handles = Vec::new();
        for i in 0..num_streams {
            let tcp = TcpTransport::new(provider.clone());
            let peer_addr = loopback_peer_addr();

            let handle = tokio::spawn(async move {
                let mut stream = tcp.open(&peer_addr, port).await.unwrap();
                // Send stream ID
                let id_bytes = (i as u32).to_be_bytes();
                stream.write_all(&id_bytes).await.unwrap();
                // Send data (deterministic pattern based on stream ID)
                let data: Vec<u8> = (0..bytes_per_stream)
                    .map(|j| ((j + i * 37) % 256) as u8)
                    .collect();
                stream.write_all(&data).await.unwrap();
                stream.shutdown().await.unwrap();
                (i, data)
            });
            client_handles.push(handle);
        }

        // Gather all client results
        let mut sent: Vec<(usize, Vec<u8>)> = Vec::new();
        for handle in client_handles {
            sent.push(handle.await.unwrap());
        }

        // Gather server results
        let mut received = server_handle.await.unwrap();

        // Sort both by stream ID for comparison
        sent.sort_by_key(|&(id, _)| id);
        received.sort_by_key(|&(id, _)| id);

        assert_eq!(sent.len(), received.len());
        for ((sent_id, sent_data), (recv_id, recv_data)) in sent.iter().zip(received.iter()) {
            assert_eq!(sent_id, recv_id, "stream ID mismatch");
            assert_eq!(
                sent_data, recv_data,
                "data integrity check failed for stream {sent_id}"
            );
        }
    })
    .await
    .expect("test timed out");
}

/// Close write side, verify read side still works (TCP half-close).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_tcp_half_close() {
    let _ = tracing_subscriber::fmt::try_init();

    timeout(TEST_TIMEOUT, async {
        use truffle_core::transport::tcp::TcpTransport;

        let provider = Arc::new(MockNetworkProvider::new("tcp-halfclose"));
        let tcp = TcpTransport::new(provider);

        let mut listener = tcp.listen(0).await.unwrap();
        let port = listener.port;
        let peer_addr = loopback_peer_addr();

        // Client: connect, write, shutdown write, then read the reply
        let client_task = tokio::spawn(async move {
            let mut stream = tcp.open(&peer_addr, port).await.unwrap();

            // Send data
            stream.write_all(b"request").await.unwrap();
            // Half-close: shutdown write direction
            stream.shutdown().await.unwrap();

            // Read side should still work
            let mut buf = Vec::new();
            stream.read_to_end(&mut buf).await.unwrap();
            String::from_utf8(buf).unwrap()
        });

        // Server: accept, read request (should see EOF from client write shutdown),
        // then send response
        let server_task = tokio::spawn(async move {
            let incoming = listener.accept().await.expect("should accept");
            let mut stream = incoming.stream;

            // Read the request until client's write EOF
            let mut request = Vec::new();
            stream.read_to_end(&mut request).await.unwrap();
            assert_eq!(request, b"request", "server should read client's request");

            // Send the response (client's read side is still open)
            stream.write_all(b"response").await.unwrap();
            stream.shutdown().await.unwrap();
        });

        let (client_result, server_result) = tokio::join!(client_task, server_task);
        let response = client_result.unwrap();
        server_result.unwrap();

        assert_eq!(
            response, "response",
            "client should receive server's response after half-close"
        );
    })
    .await
    .expect("test timed out");
}

// ============================================================================
// UDP tests
// ============================================================================

/// Send 100 datagrams rapidly, verify most arrive (UDP allows some loss).
#[tokio::test]
#[ignore]
async fn test_udp_rapid_fire() {
    let _ = tracing_subscriber::fmt::try_init();

    timeout(TEST_TIMEOUT, async {
        use truffle_core::transport::udp::{UdpConfig, UdpTransport};

        let provider = Arc::new(MockNetworkProvider::new("udp-rapid"));
        let udp = UdpTransport::new(provider, UdpConfig::default());

        let sender_socket = udp.bind(0).await.unwrap();
        let receiver_socket = udp.bind(0).await.unwrap();

        let recv_port = receiver_socket.local_addr().unwrap().port();
        let recv_addr = format!("127.0.0.1:{recv_port}");

        let send_count: usize = 100;
        let payload_size = 64; // Small datagrams

        // Send 100 datagrams as fast as possible
        for i in 0..send_count {
            let mut data = vec![0u8; payload_size];
            // Encode sequence number at the start
            data[0..4].copy_from_slice(&(i as u32).to_be_bytes());
            // Fill rest with recognizable pattern
            for (j, byte) in data.iter_mut().enumerate().skip(4) {
                *byte = ((i + j) % 256) as u8;
            }
            sender_socket.send_to(&data, &recv_addr).await.unwrap();
        }

        // Receive with a short timeout — UDP may drop some
        let mut received_seqs = Vec::new();
        let mut buf = [0u8; 1024];
        let recv_timeout = Duration::from_secs(2);

        loop {
            match timeout(recv_timeout, receiver_socket.recv_from(&mut buf)).await {
                Ok(Ok((n, _sender))) => {
                    assert_eq!(n, payload_size);
                    let seq = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
                    received_seqs.push(seq);
                }
                Ok(Err(e)) => {
                    panic!("recv error: {e}");
                }
                Err(_) => {
                    // Timeout — no more datagrams
                    break;
                }
            }
        }

        let received_count = received_seqs.len();
        eprintln!(
            "UDP rapid fire: sent {send_count}, received {received_count} ({:.0}%)",
            (received_count as f64 / send_count as f64) * 100.0
        );

        // On loopback, we should get nearly all of them (allow 10% loss)
        assert!(
            received_count >= send_count * 90 / 100,
            "expected at least 90% delivery on loopback, got {received_count}/{send_count}"
        );

        // Verify no duplicates
        let unique_count = {
            let mut sorted = received_seqs.clone();
            sorted.sort();
            sorted.dedup();
            sorted.len()
        };
        assert_eq!(
            unique_count, received_count,
            "should have no duplicate sequence numbers"
        );
    })
    .await
    .expect("test timed out");
}

/// Test near MTU boundary datagram sizes.
#[tokio::test]
#[ignore]
async fn test_udp_max_datagram_size() {
    let _ = tracing_subscriber::fmt::try_init();

    timeout(TEST_TIMEOUT, async {
        use truffle_core::transport::udp::{UdpConfig, UdpTransport};

        let provider = Arc::new(MockNetworkProvider::new("udp-mtu"));
        let udp = UdpTransport::new(provider, UdpConfig::default());

        let socket_a = udp.bind(0).await.unwrap();
        let socket_b = udp.bind(0).await.unwrap();

        let port_b = socket_b.local_addr().unwrap().port();
        let addr_b = format!("127.0.0.1:{port_b}");

        // Test various sizes around typical MTU boundaries
        // Loopback MTU is typically 65535, but standard Ethernet MTU is ~1500.
        // UDP over loopback can handle much larger datagrams.
        // Note: macOS loopback max UDP payload is ~9216 bytes (not the
        // theoretical 65507). We test up to 9000 which works cross-platform.
        let test_sizes: Vec<usize> = vec![
            1,    // minimum
            508,  // safe minimum (576 IP - 60 IP header - 8 UDP header)
            1472, // standard Ethernet MTU - IP/UDP headers
            1473, // just over standard Ethernet MTU limit
            8192, // larger, still within typical loopback limits
            9000, // near macOS loopback ceiling
        ];

        for size in &test_sizes {
            let payload: Vec<u8> = (0..*size).map(|i| (i % 251) as u8).collect();

            let sent = socket_a.send_to(&payload, &addr_b).await.unwrap();
            assert_eq!(sent, *size, "should send {size} bytes");

            let mut buf = vec![0u8; 65536];
            let (n, _sender) = timeout(Duration::from_secs(5), socket_b.recv_from(&mut buf))
                .await
                .expect("recv should not time out")
                .unwrap();

            assert_eq!(n, *size, "should receive {size} bytes");
            assert_eq!(
                &buf[..n],
                &payload[..],
                "datagram integrity check failed for size {size}"
            );

            eprintln!("UDP datagram size {size}: OK");
        }
    })
    .await
    .expect("test timed out");
}

// ============================================================================
// QUIC stress tests
// ============================================================================

/// QUIC: concurrent bidirectional messages (mirrors the WS test).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_quic_concurrent_bidirectional() {
    let _ = tracing_subscriber::fmt::try_init();

    timeout(TEST_TIMEOUT, async {
        use truffle_core::transport::quic::{QuicConfig, QuicTransport};

        let server_provider = Arc::new(MockNetworkProvider::new("quic-bidi-integ-server"));
        let client_provider = Arc::new(MockNetworkProvider::new("quic-bidi-integ-client"));

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
        let peer_addr = loopback_peer_addr();

        let (client_result, server_stream) = tokio::join!(client_quic.connect(&peer_addr), async {
            listener.accept().await
        });

        let mut client_stream = client_result.unwrap();
        let mut server_stream = server_stream.unwrap();

        let msg_count = 100;

        // Client: send then receive
        let client_task = tokio::spawn(async move {
            for i in 0..msg_count {
                let msg = format!("quic-client-{i}");
                client_stream.send(msg.as_bytes()).await.unwrap();
            }
            let mut received = Vec::new();
            for _ in 0..msg_count {
                let data = client_stream.recv().await.unwrap().expect("should receive");
                received.push(String::from_utf8(data).unwrap());
            }
            (client_stream, received)
        });

        // Server: send then receive
        let server_task = tokio::spawn(async move {
            for i in 0..msg_count {
                let msg = format!("quic-server-{i}");
                server_stream.send(msg.as_bytes()).await.unwrap();
            }
            let mut received = Vec::new();
            for _ in 0..msg_count {
                let data = server_stream.recv().await.unwrap().expect("should receive");
                received.push(String::from_utf8(data).unwrap());
            }
            (server_stream, received)
        });

        let (client_result, server_result) = tokio::join!(client_task, server_task);
        let (mut client_stream, client_received) = client_result.unwrap();
        let (mut server_stream, server_received) = server_result.unwrap();

        // Verify all messages received in order
        assert_eq!(client_received.len(), msg_count);
        for (i, message) in client_received.iter().enumerate() {
            assert_eq!(message, &format!("quic-server-{i}"));
        }

        assert_eq!(server_received.len(), msg_count);
        for (i, message) in server_received.iter().enumerate() {
            assert_eq!(message, &format!("quic-client-{i}"));
        }

        client_stream.close().await.unwrap();
        server_stream.close().await.unwrap();
    })
    .await
    .expect("test timed out");
}

/// QUIC: large message (1MB) transfer with length-prefix framing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_quic_large_message() {
    let _ = tracing_subscriber::fmt::try_init();

    timeout(TEST_TIMEOUT, async {
        use truffle_core::transport::quic::{QuicConfig, QuicTransport};

        let server_provider = Arc::new(MockNetworkProvider::new("quic-large-server"));
        let client_provider = Arc::new(MockNetworkProvider::new("quic-large-client"));

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
        let peer_addr = loopback_peer_addr();

        let (client_result, server_stream) = tokio::join!(client_quic.connect(&peer_addr), async {
            listener.accept().await
        });

        let mut client_stream = client_result.unwrap();
        let mut server_stream = server_stream.unwrap();

        let payload_size = 1024 * 1024; // 1 MB
        let payload: Vec<u8> = (0..payload_size).map(|i| (i % 251) as u8).collect();

        // Client -> Server
        client_stream.send(&payload).await.unwrap();
        let received = server_stream
            .recv()
            .await
            .unwrap()
            .expect("should receive 1MB");
        assert_eq!(received.len(), payload_size);
        assert_eq!(received, payload);

        client_stream.close().await.unwrap();
        server_stream.close().await.unwrap();
    })
    .await
    .expect("test timed out");
}

/// QUIC: message ordering with 1000 messages.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn test_quic_message_ordering() {
    let _ = tracing_subscriber::fmt::try_init();

    timeout(TEST_TIMEOUT, async {
        use truffle_core::transport::quic::{QuicConfig, QuicTransport};

        let server_provider = Arc::new(MockNetworkProvider::new("quic-order-server"));
        let client_provider = Arc::new(MockNetworkProvider::new("quic-order-client"));

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
        let peer_addr = loopback_peer_addr();

        let (client_result, server_stream) = tokio::join!(client_quic.connect(&peer_addr), async {
            listener.accept().await
        });

        let mut client_stream = client_result.unwrap();
        let mut server_stream = server_stream.unwrap();

        let msg_count: usize = 1000;

        // Send all messages
        let send_task = tokio::spawn(async move {
            for i in 0..msg_count {
                let seq_bytes = (i as u32).to_be_bytes();
                client_stream.send(&seq_bytes).await.unwrap();
            }
            client_stream
        });

        // Receive all messages
        let recv_task = tokio::spawn(async move {
            let mut received_seqs = Vec::with_capacity(msg_count);
            for _ in 0..msg_count {
                let data = server_stream.recv().await.unwrap().expect("should receive");
                let seq = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
                received_seqs.push(seq);
            }
            (server_stream, received_seqs)
        });

        let (client_result, server_result) = tokio::join!(send_task, recv_task);
        let mut client_stream = client_result.unwrap();
        let (mut server_stream, received_seqs) = server_result.unwrap();

        for (expected, actual) in received_seqs.iter().enumerate() {
            assert_eq!(
                *actual, expected as u32,
                "QUIC message ordering broken at index {expected}: got {actual}"
            );
        }
        assert_eq!(received_seqs.len(), msg_count);

        client_stream.close().await.unwrap();
        server_stream.close().await.unwrap();
    })
    .await
    .expect("test timed out");
}

// ============================================================================
// Cross-transport tests
// ============================================================================

/// WS messaging + TCP stream running at the same time to same peer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_ws_and_tcp_simultaneous() {
    let _ = tracing_subscriber::fmt::try_init();

    timeout(TEST_TIMEOUT, async {
        use truffle_core::transport::tcp::TcpTransport;
        use truffle_core::transport::websocket::WebSocketTransport;

        // Shared provider (simulates "same peer")
        let provider = Arc::new(MockNetworkProvider::new("cross-transport-peer"));

        // --- WS setup ---
        let ws_port = ephemeral_port().await;
        let ws_config = ws_config(ws_port);
        let server_ws = WebSocketTransport::new(provider.clone(), ws_config.clone());
        let mut ws_listener = server_ws.listen().await.unwrap();

        // --- TCP setup ---
        let tcp = TcpTransport::new(provider.clone());
        let mut tcp_listener = tcp.listen(0).await.unwrap();
        let tcp_port = tcp_listener.port;

        // --- WS client connects ---
        let ws_client_provider = Arc::new(MockNetworkProvider::new("ws-client"));
        let client_ws = WebSocketTransport::new(ws_client_provider, ws_config);
        let peer_addr = loopback_peer_addr();

        let (ws_client_result, ws_server_stream) =
            tokio::join!(client_ws.connect(&peer_addr), async {
                ws_listener.accept().await
            });

        let mut ws_client_stream = ws_client_result.unwrap();
        let mut ws_server_stream = ws_server_stream.unwrap();

        // --- TCP client connects ---
        let tcp_client = TcpTransport::new(provider.clone());
        let tcp_connect_task = tokio::spawn(async move {
            tcp_client
                .open(&loopback_peer_addr(), tcp_port)
                .await
                .unwrap()
        });
        let tcp_accept_task =
            tokio::spawn(async move { tcp_listener.accept().await.expect("should accept TCP") });

        let (tcp_client_stream, tcp_server_incoming) =
            tokio::join!(tcp_connect_task, tcp_accept_task);
        let mut tcp_client_stream = tcp_client_stream.unwrap();
        let mut tcp_server_stream = tcp_server_incoming.unwrap().stream;

        // --- Run WS and TCP traffic concurrently ---
        let ws_task = tokio::spawn(async move {
            // WS: exchange 50 messages each way
            for i in 0..50 {
                ws_client_stream
                    .send(format!("ws-{i}").as_bytes())
                    .await
                    .unwrap();
                let data = ws_server_stream.recv().await.unwrap().expect("ws recv");
                assert_eq!(data, format!("ws-{i}").as_bytes());

                ws_server_stream
                    .send(format!("ws-reply-{i}").as_bytes())
                    .await
                    .unwrap();
                let reply = ws_client_stream
                    .recv()
                    .await
                    .unwrap()
                    .expect("ws reply recv");
                assert_eq!(reply, format!("ws-reply-{i}").as_bytes());
            }
            ws_client_stream.close().await.unwrap();
            ws_server_stream.close().await.unwrap();
        });

        let tcp_task = tokio::spawn(async move {
            // TCP: transfer 1 MB
            let tcp_data: Vec<u8> = (0..1_000_000).map(|i| (i % 256) as u8).collect();
            let expected = tcp_data.clone();

            tcp_client_stream.write_all(&tcp_data).await.unwrap();
            tcp_client_stream.shutdown().await.unwrap();

            let mut received = Vec::new();
            tcp_server_stream.read_to_end(&mut received).await.unwrap();
            assert_eq!(
                received, expected,
                "TCP data integrity during concurrent WS"
            );
        });

        let (ws_result, tcp_result) = tokio::join!(ws_task, tcp_task);
        ws_result.unwrap();
        tcp_result.unwrap();
    })
    .await
    .expect("test timed out");
}

/// Verify WS, TCP, UDP all work through the same NetworkProvider instance.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_all_transports_use_same_network_provider() {
    let _ = tracing_subscriber::fmt::try_init();

    timeout(TEST_TIMEOUT, async {
        use truffle_core::transport::tcp::TcpTransport;
        use truffle_core::transport::udp::{UdpConfig, UdpTransport};
        use truffle_core::transport::websocket::WebSocketTransport;

        // Single shared provider for all transports
        let provider = Arc::new(MockNetworkProvider::new("shared-provider"));

        // --- WS transport ---
        let ws_port = ephemeral_port().await;
        let ws_config = ws_config(ws_port);
        let ws_transport = WebSocketTransport::new(provider.clone(), ws_config.clone());
        let mut ws_listener = ws_transport.listen().await.unwrap();

        // WS client uses a different provider (to get a different peer_id in handshake)
        let ws_client_provider = Arc::new(MockNetworkProvider::new("ws-client-shared"));
        let ws_client = WebSocketTransport::new(ws_client_provider, ws_config);

        // --- TCP transport ---
        let tcp_transport = TcpTransport::new(provider.clone());
        let mut tcp_listener = tcp_transport.listen(0).await.unwrap();
        let tcp_port = tcp_listener.port;

        // --- UDP transport ---
        let udp_transport = UdpTransport::new(provider.clone(), UdpConfig::default());
        let udp_socket_a = udp_transport.bind(0).await.unwrap();
        let udp_socket_b = udp_transport.bind(0).await.unwrap();
        let udp_port_b = udp_socket_b.local_addr().unwrap().port();
        let udp_addr_b = format!("127.0.0.1:{udp_port_b}");

        let peer_addr = loopback_peer_addr();

        // Run all three transports concurrently
        let ws_task = tokio::spawn(async move {
            let (client_result, server_stream) =
                tokio::join!(ws_client.connect(&peer_addr), async {
                    ws_listener.accept().await
                });
            let mut cs = client_result.unwrap();
            let mut ss = server_stream.unwrap();

            cs.send(b"ws-test").await.unwrap();
            let data = ss.recv().await.unwrap().expect("ws recv");
            assert_eq!(data, b"ws-test");

            cs.close().await.unwrap();
            ss.close().await.unwrap();
            "ws-ok"
        });

        let tcp_task = tokio::spawn(async move {
            // TCP client
            let tcp_client =
                TcpTransport::new(Arc::new(MockNetworkProvider::new("tcp-client-shared")));
            let connect_task = tokio::spawn(async move {
                tcp_client
                    .open(&loopback_peer_addr(), tcp_port)
                    .await
                    .unwrap()
            });
            let accept_task =
                tokio::spawn(
                    async move { tcp_listener.accept().await.expect("should accept TCP") },
                );

            let (client_result, server_result) = tokio::join!(connect_task, accept_task);
            let mut client_stream = client_result.unwrap();
            let mut server_stream = server_result.unwrap().stream;

            client_stream.write_all(b"tcp-test").await.unwrap();
            client_stream.shutdown().await.unwrap();

            let mut buf = Vec::new();
            server_stream.read_to_end(&mut buf).await.unwrap();
            assert_eq!(buf, b"tcp-test");
            "tcp-ok"
        });

        let udp_task = tokio::spawn(async move {
            udp_socket_a
                .send_to(b"udp-test", &udp_addr_b)
                .await
                .unwrap();
            let mut buf = [0u8; 1024];
            let (n, _sender) = timeout(Duration::from_secs(5), udp_socket_b.recv_from(&mut buf))
                .await
                .expect("udp recv timeout")
                .unwrap();
            assert_eq!(&buf[..n], b"udp-test");
            "udp-ok"
        });

        let (ws_result, tcp_result, udp_result) = tokio::join!(ws_task, tcp_task, udp_task);
        assert_eq!(ws_result.unwrap(), "ws-ok");
        assert_eq!(tcp_result.unwrap(), "tcp-ok");
        assert_eq!(udp_result.unwrap(), "udp-ok");

        // All three transports worked through instances sharing the same provider
        eprintln!("All transports (WS, TCP, UDP) verified with same NetworkProvider");
    })
    .await
    .expect("test timed out");
}

// ============================================================================
// QUIC + WS cross-transport test
// ============================================================================

/// QUIC and WS running simultaneously, verifying both StreamTransport
/// implementations work in parallel.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_quic_and_ws_simultaneous() {
    let _ = tracing_subscriber::fmt::try_init();

    timeout(TEST_TIMEOUT, async {
        use truffle_core::transport::quic::{QuicConfig, QuicTransport};
        use truffle_core::transport::websocket::WebSocketTransport;

        let provider = Arc::new(MockNetworkProvider::new("quic-ws-server"));

        // --- WS setup ---
        let ws_port = ephemeral_port().await;
        let ws_config = ws_config(ws_port);
        let ws_server = WebSocketTransport::new(provider.clone(), ws_config.clone());
        let mut ws_listener = ws_server.listen().await.unwrap();

        // --- QUIC setup ---
        let quic_server_config = QuicConfig {
            port: 0,
            max_streams: 100,
        };
        let quic_server = QuicTransport::new(provider.clone(), quic_server_config);
        let mut quic_listener = quic_server.listen().await.unwrap();
        let quic_port = quic_listener.port;

        let peer_addr = loopback_peer_addr();

        // WS traffic task
        let ws_task = tokio::spawn(async move {
            let ws_client_provider = Arc::new(MockNetworkProvider::new("ws-client-x"));
            let ws_client = WebSocketTransport::new(ws_client_provider, ws_config);

            let (client_result, server_stream) =
                tokio::join!(ws_client.connect(&peer_addr), async {
                    ws_listener.accept().await
                });

            let mut cs = client_result.unwrap();
            let mut ss = server_stream.unwrap();

            for i in 0..20 {
                let msg = format!("ws-msg-{i}");
                cs.send(msg.as_bytes()).await.unwrap();
                let data = ss.recv().await.unwrap().expect("ws recv");
                assert_eq!(data, msg.as_bytes());
            }

            cs.close().await.unwrap();
            ss.close().await.unwrap();
        });

        // QUIC traffic task
        let quic_task = tokio::spawn(async move {
            let quic_client_provider = Arc::new(MockNetworkProvider::new("quic-client-x"));
            let quic_client_config = QuicConfig {
                port: quic_port,
                max_streams: 100,
            };
            let quic_client = QuicTransport::new(quic_client_provider, quic_client_config);

            let quic_peer_addr = loopback_peer_addr();
            let (client_result, server_stream) =
                tokio::join!(quic_client.connect(&quic_peer_addr), async {
                    quic_listener.accept().await
                });

            let mut cs = client_result.unwrap();
            let mut ss = server_stream.unwrap();

            for i in 0..20 {
                let msg = format!("quic-msg-{i}");
                cs.send(msg.as_bytes()).await.unwrap();
                let data = ss.recv().await.unwrap().expect("quic recv");
                assert_eq!(data, msg.as_bytes());
            }

            cs.close().await.unwrap();
            ss.close().await.unwrap();
        });

        let (ws_result, quic_result) = tokio::join!(ws_task, quic_task);
        ws_result.unwrap();
        quic_result.unwrap();
    })
    .await
    .expect("test timed out");
}
