//! Cross-machine transport test for all Layer 4 protocols.
//!
//! Tests TCP, WebSocket, QUIC, and UDP transports between two machines
//! on the same Tailscale network.
//!
//! # Usage
//!
//! ```bash
//! # Server mode (e.g., on EC2):
//! SIDECAR_PATH=/tmp/sidecar-slim-linux ./transport_peer_test server
//!
//! # Client mode (e.g., on macOS):
//! SIDECAR_PATH=./test-sidecar ./transport_peer_test client <server-tailscale-ip>
//! ```
//!
//! # Ports
//!
//! - TCP:  19417
//! - WS:   19418
//! - QUIC: 19419
//! - UDP:  19420

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::Instant;

use truffle_core::network::tailscale::{TailscaleConfig, TailscaleProvider};
use truffle_core::network::{NetworkPeerEvent, NetworkProvider, PeerAddr};
use truffle_core::transport::quic::{QuicConfig, QuicTransport};
use truffle_core::transport::tcp::TcpTransport;
use truffle_core::transport::udp::{UdpConfig, UdpTransport};
use truffle_core::transport::websocket::WebSocketTransport;
use truffle_core::transport::{
    DatagramTransport, FramedStream, RawTransport, StreamTransport, WsConfig,
};

const TCP_PORT: u16 = 19417;
const WS_PORT: u16 = 19418;
const QUIC_PORT: u16 = 19419;
const UDP_PORT: u16 = 19420;

const TEST_TIMEOUT: Duration = Duration::from_secs(10);

#[tokio::main]
async fn main() {
    // Initialize tracing so debug logs from NetworkUdpSocket are visible.
    // Set RUST_LOG=debug (or =trace) to see detailed relay logging.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();

    // Parse args
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <server|client> [server-tailscale-ip]", args[0]);
        eprintln!();
        eprintln!("  server  — Listen on all transports (run on EC2)");
        eprintln!("  client  — Connect to server on all transports (run on macOS)");
        std::process::exit(1);
    }

    let mode = args[1].as_str();
    match mode {
        "server" => run_server().await,
        "client" => {
            if args.len() < 3 {
                eprintln!("Client mode requires server Tailscale IP:");
                eprintln!("  {} client <server-tailscale-ip>", args[0]);
                std::process::exit(1);
            }
            run_client(&args[2]).await;
        }
        _ => {
            eprintln!("Unknown mode: {mode}. Use 'server' or 'client'.");
            std::process::exit(1);
        }
    }
}

// ---------------------------------------------------------------------------
// Provider setup (shared)
// ---------------------------------------------------------------------------

async fn create_provider(role: &str) -> TailscaleProvider {
    let sidecar = std::env::var("SIDECAR_PATH").unwrap_or_else(|_| "./sidecar-slim".to_string());
    let state_dir = std::env::var("STATE_DIR")
        .unwrap_or_else(|_| format!("/tmp/truffle-transport-test-{role}"));
    let hostname =
        std::env::var("HOSTNAME_OVERRIDE").unwrap_or_else(|_| format!("transport-test-{role}"));

    eprintln!("[{role}] Starting TailscaleProvider");
    eprintln!("[{role}]   sidecar:   {sidecar}");
    eprintln!("[{role}]   state_dir: {state_dir}");
    eprintln!("[{role}]   hostname:  {hostname}");

    let config = TailscaleConfig {
        binary_path: PathBuf::from(&sidecar),
        app_id: "transport-test".to_string(),
        device_id: "01J4K9M2Z8AB3RNYQPW6H5TC0X".to_string(),
        device_name: hostname.clone(),
        state_dir,
        hostname: format!("truffle-transport-test-{hostname}"),
        auth_key: std::env::var("TS_AUTHKEY").ok(),
        ephemeral: None,
        tags: None,
        idle_timeout_secs: None,
        login_allow: Vec::new(),
    };

    let mut provider = TailscaleProvider::new(config);

    // Subscribe to events BEFORE start() so we see auth URLs
    let mut events = provider.peer_events();
    let role_owned = role.to_string();
    tokio::spawn(async move {
        loop {
            match events.recv().await {
                Ok(NetworkPeerEvent::AuthRequired { url }) => {
                    eprintln!();
                    eprintln!("  =============================================");
                    eprintln!("  Authentication required. Open this URL:");
                    eprintln!("  {url}");
                    eprintln!("  Waiting for approval...");
                    eprintln!("  =============================================");
                    eprintln!();
                }
                Ok(NetworkPeerEvent::Joined(peer)) => {
                    eprintln!(
                        "[{role_owned}] Peer joined: {} ({})",
                        peer.hostname, peer.ip
                    );
                }
                Ok(NetworkPeerEvent::Left(id)) => {
                    eprintln!("[{role_owned}] Peer left: {id}");
                }
                Ok(NetworkPeerEvent::Updated(peer)) => {
                    eprintln!(
                        "[{role_owned}] Peer updated: {} ({})",
                        peer.hostname, peer.ip
                    );
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    });

    if let Err(e) = provider.start().await {
        eprintln!("[{role}] Start failed: {e}");
        std::process::exit(1);
    }

    let identity = provider.local_identity();
    let addr = provider.local_addr();
    eprintln!(
        "[{role}] Online! tailscale_id={}, ip={:?}, hostname={}",
        identity.tailscale_id, addr.ip, addr.hostname
    );

    provider
}

// ---------------------------------------------------------------------------
// SERVER MODE
// ---------------------------------------------------------------------------

async fn run_server() {
    let provider = create_provider("server").await;
    let provider = Arc::new(provider);

    eprintln!("[server] Starting all transport listeners...");

    // --- TCP echo server ---
    let tcp_transport = TcpTransport::new(provider.clone());
    let mut tcp_listener = tcp_transport
        .listen(TCP_PORT)
        .await
        .expect("TCP listen failed");
    eprintln!("[server] TCP  listening on port {TCP_PORT}");

    tokio::spawn(async move {
        loop {
            match tcp_listener.accept().await {
                Some(incoming) => {
                    eprintln!(
                        "[server/tcp] Accepted connection from {}",
                        incoming.remote_addr
                    );
                    tokio::spawn(async move {
                        let mut stream = incoming.stream;
                        let mut buf = [0u8; 4096];
                        loop {
                            match stream.read(&mut buf).await {
                                Ok(0) => {
                                    eprintln!("[server/tcp] Client EOF");
                                    break;
                                }
                                Ok(n) => {
                                    let data = &buf[..n];
                                    eprintln!(
                                        "[server/tcp] Received {} bytes: {:?}",
                                        n,
                                        String::from_utf8_lossy(data)
                                    );
                                    if let Err(e) = stream.write_all(data).await {
                                        eprintln!("[server/tcp] Write error: {e}");
                                        break;
                                    }
                                    if let Err(e) = stream.flush().await {
                                        eprintln!("[server/tcp] Flush error: {e}");
                                        break;
                                    }
                                    eprintln!("[server/tcp] Echoed {} bytes", n);
                                }
                                Err(e) => {
                                    eprintln!("[server/tcp] Read error: {e}");
                                    break;
                                }
                            }
                        }
                        eprintln!("[server/tcp] Connection closed");
                    });
                }
                None => {
                    eprintln!("[server/tcp] Listener closed");
                    break;
                }
            }
        }
    });

    // --- WebSocket echo server ---
    let ws_config = WsConfig {
        port: WS_PORT,
        ping_interval: Duration::from_secs(30),
        pong_timeout: Duration::from_secs(60),
        ..WsConfig::default()
    };
    let ws_transport = WebSocketTransport::new(provider.clone(), ws_config);
    let mut ws_listener = ws_transport.listen().await.expect("WS listen failed");
    eprintln!("[server] WS   listening on port {WS_PORT}");

    tokio::spawn(async move {
        loop {
            match ws_listener.accept().await {
                Some(mut stream) => {
                    let peer = stream.peer_addr();
                    eprintln!("[server/ws] Accepted connection from {peer}");
                    tokio::spawn(async move {
                        loop {
                            match stream.recv().await {
                                Ok(Some(data)) => {
                                    eprintln!(
                                        "[server/ws] Received {} bytes: {:?}",
                                        data.len(),
                                        String::from_utf8_lossy(&data)
                                    );
                                    if let Err(e) = stream.send(&data).await {
                                        eprintln!("[server/ws] Send error: {e}");
                                        break;
                                    }
                                }
                                Ok(None) => {
                                    eprintln!("[server/ws] Stream closed");
                                    break;
                                }
                                Err(e) => {
                                    eprintln!("[server/ws] Recv error: {e}");
                                    break;
                                }
                            }
                        }
                    });
                }
                None => {
                    eprintln!("[server/ws] Listener closed");
                    break;
                }
            }
        }
    });

    // --- QUIC echo server ---
    // QUIC now uses TsnetUdpSocket when a network provider is available.
    // The QuicTransport automatically binds via NetworkProvider::bind_udp()
    // and wraps the socket in TsnetUdpSocket so that datagrams route through
    // the tsnet relay instead of the host network.
    match QuicTransport::new(
        provider.clone(),
        QuicConfig {
            port: QUIC_PORT,
            ..QuicConfig::default()
        },
    )
    .listen()
    .await
    {
        Ok(mut quic_listener) => {
            eprintln!("[server] QUIC listening on port {QUIC_PORT} (via TsnetUdpSocket)");
            tokio::spawn(async move {
                loop {
                    match quic_listener.accept().await {
                        Some(mut stream) => {
                            let peer = stream.peer_addr();
                            eprintln!("[server/quic] Accepted connection from {peer}");
                            tokio::spawn(async move {
                                loop {
                                    match stream.recv().await {
                                        Ok(Some(data)) => {
                                            eprintln!(
                                                "[server/quic] Received {} bytes: {:?}",
                                                data.len(),
                                                String::from_utf8_lossy(&data)
                                            );
                                            if let Err(e) = stream.send(&data).await {
                                                eprintln!("[server/quic] Send error: {e}");
                                                break;
                                            }
                                        }
                                        Ok(None) => {
                                            eprintln!("[server/quic] Stream closed");
                                            break;
                                        }
                                        Err(e) => {
                                            eprintln!("[server/quic] Recv error: {e}");
                                            break;
                                        }
                                    }
                                }
                            });
                        }
                        None => {
                            eprintln!("[server/quic] Listener closed");
                            break;
                        }
                    }
                }
            });
        }
        Err(e) => {
            eprintln!("[server] QUIC listen failed (non-fatal): {e}");
            eprintln!("[server] QUIC will be unavailable — this is expected on some platforms");
        }
    }

    // --- UDP echo server ---
    // UDP binds directly on 0.0.0.0 — tsnet may not route UDP between nodes.
    // If this test fails, it's a known limitation (tsnet routes TCP, not UDP).
    let udp_transport = UdpTransport::new(provider.clone(), UdpConfig::default());
    let udp_socket = udp_transport.bind(UDP_PORT).await.expect("UDP bind failed");
    let local_udp = udp_socket.local_addr().expect("UDP local_addr");
    eprintln!("[server] UDP  listening on {local_udp}");

    tokio::spawn(async move {
        let mut buf = [0u8; 4096];
        loop {
            match udp_socket.recv_from(&mut buf).await {
                Ok((n, sender)) => {
                    let data = &buf[..n];
                    eprintln!(
                        "[server/udp] Received {} bytes from {}: {:?}",
                        n,
                        sender,
                        String::from_utf8_lossy(data)
                    );
                    // Echo back to sender
                    if let Err(e) = udp_socket.send_to(data, &sender).await {
                        eprintln!("[server/udp] Send error: {e}");
                    }
                }
                Err(e) => {
                    eprintln!("[server/udp] Recv error: {e}");
                    break;
                }
            }
        }
    });

    eprintln!();
    eprintln!("=========================================================");
    eprintln!("  All transports listening. Waiting for client...");
    eprintln!("  TCP:  {TCP_PORT}");
    eprintln!("  WS:   {WS_PORT}");
    eprintln!("  QUIC: {QUIC_PORT}");
    eprintln!("  UDP:  {UDP_PORT}");
    eprintln!("=========================================================");
    eprintln!();
    eprintln!("[server] Will shut down after 5 minutes of idle.");

    // Keep running until timeout
    tokio::time::sleep(Duration::from_secs(300)).await;
    eprintln!("[server] Timeout reached. Shutting down.");
}

// ---------------------------------------------------------------------------
// CLIENT MODE
// ---------------------------------------------------------------------------

async fn run_client(server_ip: &str) {
    let provider = create_provider("client").await;
    let provider = Arc::new(provider);

    let peer_addr = PeerAddr {
        ip: Some(server_ip.parse().expect("Invalid server IP")),
        hostname: "transport-test-server".to_string(),
        dns_name: None,
    };

    eprintln!();
    eprintln!("=========================================================");
    eprintln!("  Running transport tests against {server_ip}");
    eprintln!("=========================================================");
    eprintln!();

    let mut results: Vec<(&str, bool, String)> = Vec::new();

    // --- Test A: TCP ---
    eprintln!("[test/tcp] Dialing {server_ip}:{TCP_PORT}...");
    let tcp_result = tokio::time::timeout(TEST_TIMEOUT, async {
        let tcp_transport = TcpTransport::new(provider.clone());
        let mut stream = tcp_transport
            .open(&peer_addr, TCP_PORT)
            .await
            .map_err(|e| format!("dial failed: {e}"))?;

        let msg = b"hello tcp";
        let expected_len = msg.len();
        stream
            .write_all(msg)
            .await
            .map_err(|e| format!("write failed: {e}"))?;
        stream
            .flush()
            .await
            .map_err(|e| format!("flush failed: {e}"))?;
        eprintln!("[test/tcp] Sent {expected_len} bytes, reading echo...");

        // Read with a loop — the bridge adds latency so the echo may not
        // arrive in a single read. Accumulate bytes until we have enough
        // or hit a deadline.
        let mut response = Vec::new();
        let mut buf = [0u8; 256];
        let deadline = Instant::now() + Duration::from_secs(5);
        while response.len() < expected_len && Instant::now() < deadline {
            match tokio::time::timeout(Duration::from_secs(2), stream.read(&mut buf)).await {
                Ok(Ok(0)) => {
                    eprintln!("[test/tcp] EOF after {} bytes", response.len());
                    break;
                }
                Ok(Ok(n)) => {
                    eprintln!("[test/tcp] Read chunk: {} bytes", n);
                    response.extend_from_slice(&buf[..n]);
                }
                Ok(Err(e)) => {
                    eprintln!("[test/tcp] Read error: {e}");
                    break;
                }
                Err(_) => {
                    eprintln!("[test/tcp] Read timed out after {} bytes", response.len());
                    break;
                }
            }
        }

        let reply = String::from_utf8_lossy(&response).to_string();
        // Clean shutdown: signal we're done writing so the server's read
        // returns EOF instead of "Connection reset by peer".
        let _ = stream.shutdown().await;

        if reply == "hello tcp" {
            Ok(format!("echoed {} bytes correctly", response.len()))
        } else {
            Err(format!(
                "expected 'hello tcp', got '{}' ({} bytes)",
                reply,
                response.len()
            ))
        }
    })
    .await;

    match tcp_result {
        Ok(Ok(detail)) => {
            eprintln!("[test/tcp] PASS - {detail}");
            results.push(("TCP", true, detail));
        }
        Ok(Err(e)) => {
            eprintln!("[test/tcp] FAIL - {e}");
            results.push(("TCP", false, e));
        }
        Err(_) => {
            eprintln!("[test/tcp] FAIL - timeout after {TEST_TIMEOUT:?}");
            results.push(("TCP", false, format!("timeout after {TEST_TIMEOUT:?}")));
        }
    }

    // --- Test B: WebSocket ---
    eprintln!("[test/ws] Connecting to {server_ip}:{WS_PORT}...");
    let ws_result = tokio::time::timeout(TEST_TIMEOUT, async {
        let ws_config = WsConfig {
            port: WS_PORT,
            ping_interval: Duration::from_secs(30),
            pong_timeout: Duration::from_secs(60),
            ..WsConfig::default()
        };
        let ws_transport = WebSocketTransport::new(provider.clone(), ws_config);
        let mut stream = ws_transport
            .connect(&peer_addr)
            .await
            .map_err(|e| format!("connect failed: {e}"))?;

        let msg = b"hello ws";
        stream
            .send(msg)
            .await
            .map_err(|e| format!("send failed: {e}"))?;

        let reply = stream
            .recv()
            .await
            .map_err(|e| format!("recv failed: {e}"))?
            .ok_or_else(|| "stream closed before reply".to_string())?;

        let reply_str = String::from_utf8_lossy(&reply).to_string();
        stream.close().await.ok(); // best-effort close

        if reply_str == "hello ws" {
            Ok(format!("echoed {} bytes correctly", reply.len()))
        } else {
            Err(format!("expected 'hello ws', got '{reply_str}'"))
        }
    })
    .await;

    match ws_result {
        Ok(Ok(detail)) => {
            eprintln!("[test/ws] PASS - {detail}");
            results.push(("WebSocket", true, detail));
        }
        Ok(Err(e)) => {
            eprintln!("[test/ws] FAIL - {e}");
            results.push(("WebSocket", false, e));
        }
        Err(_) => {
            eprintln!("[test/ws] FAIL - timeout after {TEST_TIMEOUT:?}");
            results.push((
                "WebSocket",
                false,
                format!("timeout after {TEST_TIMEOUT:?}"),
            ));
        }
    }

    // --- Test C: QUIC ---
    // QUIC now uses TsnetUdpSocket which routes datagrams through the tsnet
    // relay. The QuicTransport automatically creates a TsnetUdpSocket-backed
    // endpoint when NetworkProvider::bind_udp() is available.
    eprintln!("[test/quic] Connecting to {server_ip}:{QUIC_PORT} via TsnetUdpSocket...");
    let quic_result = tokio::time::timeout(TEST_TIMEOUT, async {
        let quic_config = QuicConfig {
            port: QUIC_PORT,
            ..QuicConfig::default()
        };
        let quic_transport = QuicTransport::new(provider.clone(), quic_config);
        let mut stream = quic_transport
            .connect(&peer_addr)
            .await
            .map_err(|e| format!("connect failed: {e}"))?;

        let msg = b"hello quic";
        stream
            .send(msg)
            .await
            .map_err(|e| format!("send failed: {e}"))?;

        let reply = stream
            .recv()
            .await
            .map_err(|e| format!("recv failed: {e}"))?
            .ok_or_else(|| "stream closed before reply".to_string())?;

        let reply_str = String::from_utf8_lossy(&reply).to_string();
        stream.close().await.ok(); // best-effort close

        if reply_str == "hello quic" {
            Ok(format!("echoed {} bytes correctly", reply.len()))
        } else {
            Err(format!("expected 'hello quic', got '{reply_str}'"))
        }
    })
    .await;

    match quic_result {
        Ok(Ok(detail)) => {
            eprintln!("[test/quic] PASS - {detail}");
            results.push(("QUIC", true, detail));
        }
        Ok(Err(e)) => {
            eprintln!("[test/quic] FAIL - {e}");
            results.push(("QUIC", false, e));
        }
        Err(_) => {
            eprintln!("[test/quic] FAIL - timeout after {TEST_TIMEOUT:?}");
            results.push(("QUIC", false, format!("timeout after {TEST_TIMEOUT:?}")));
        }
    }

    // --- Test D: UDP ---
    // UDP is bound through the tsnet relay (NetworkProvider::bind_udp) so that
    // datagrams are routed over the Tailscale network, not the host network.
    // If bind_udp fails, UdpTransport falls back to direct tokio sockets which
    // won't reach the peer over tsnet.
    eprintln!("[test/udp] Binding UDP socket via tsnet relay...");
    let udp_result = tokio::time::timeout(TEST_TIMEOUT, async {
        let udp_transport = UdpTransport::new(provider.clone(), UdpConfig::default());
        let socket = udp_transport
            .bind(0)
            .await
            .map_err(|e| format!("bind failed: {e}"))?;

        let local = socket
            .local_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|_| "?".into());
        eprintln!("[test/udp] Bound socket at {local}");

        // Detect which socket variant we got. If it's Direct (bound to 0.0.0.0),
        // warn that it won't route through tsnet.
        let is_relay = local.starts_with("127.0.0.1:");
        if is_relay {
            eprintln!("[test/udp] Socket is relay-backed (tsnet) -- good");
        } else {
            eprintln!("[test/udp] WARNING: Socket is direct (0.0.0.0) -- NOT routed through tsnet");
            eprintln!("[test/udp] This will likely fail. Check sidecar listenPacket support.");
        }

        let msg = b"hello udp";
        let target = format!("{server_ip}:{UDP_PORT}");
        eprintln!("[test/udp] Sending {} bytes to {target}...", msg.len());
        socket
            .send_to(msg, &target)
            .await
            .map_err(|e| format!("send_to failed: {e}"))?;

        eprintln!("[test/udp] Waiting for echo...");
        let mut buf = [0u8; 1024];
        let (n, from) = socket
            .recv_from(&mut buf)
            .await
            .map_err(|e| format!("recv_from failed: {e}"))?;

        let reply = String::from_utf8_lossy(&buf[..n]).to_string();

        if reply == "hello udp" {
            Ok(format!("echoed {n} bytes from {from}"))
        } else {
            Err(format!("expected 'hello udp', got '{reply}'"))
        }
    })
    .await;

    match udp_result {
        Ok(Ok(detail)) => {
            eprintln!("[test/udp] PASS - {detail}");
            results.push(("UDP", true, detail));
        }
        Ok(Err(e)) => {
            eprintln!("[test/udp] FAIL - {e}");
            results.push(("UDP", false, e));
        }
        Err(_) => {
            let note = "timeout after 10s (check sidecar listenPacket and relay routing)";
            eprintln!("[test/udp] FAIL - {note}");
            results.push(("UDP", false, note.to_string()));
        }
    }

    // --- Summary ---
    eprintln!();
    eprintln!("=========================================================");
    eprintln!("  TRANSPORT TEST RESULTS");
    eprintln!("=========================================================");
    let mut pass_count = 0;
    let mut fail_count = 0;
    for (name, passed, detail) in &results {
        let status = if *passed {
            pass_count += 1;
            "PASS"
        } else {
            fail_count += 1;
            "FAIL"
        };
        eprintln!("  [{status}] {name:10} — {detail}");
    }
    eprintln!("=========================================================");
    eprintln!(
        "  {pass_count} passed, {fail_count} failed out of {} tests",
        results.len()
    );
    eprintln!("=========================================================");

    if fail_count > 0 {
        std::process::exit(1);
    }
}
