//! Raw transport commands — Tauri IPC parity with the NAPI raw surface.
//!
//! Mirrors the NAPI classes from RFC 021 §7 (`truffle-napi/src/raw_socket.rs`,
//! `udp_socket.rs`, `quic.rs`) one-to-one, so a single TypeScript layer can wrap
//! either binding. The semantics are identical to NAPI:
//!
//! - **Pull-model bytes.** `*_read` / `*_recv` / `*_accept` resolve one chunk
//!   (or `null` on clean EOF / close). An awaiting frontend *is* the
//!   backpressure — no data is ever pushed as an event.
//! - **Independent read/write locks.** TCP sockets and QUIC streams split into
//!   owned halves behind separate `Mutex<Option<..>>` guards, so a `*_read`
//!   blocked awaiting bytes never blocks a concurrent `*_write` on the same
//!   handle (full-duplex, like the NAPI handles).
//!
//! Live socket/stream/listener handles cannot cross the Tauri IPC boundary, so
//! each is parked in an id-keyed registry in [`crate::TruffleState`] (the same
//! pattern as `pending_offers`) and every command takes/returns a
//! short string id (`uuid` v4 with a kind prefix, matching the file-transfer
//! token scheme).
//!
//! The core listener APIs reserve the configured session WebSocket port
//! (default 9417). Their errors surface as `Result<_, String>`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use serde::Serialize;
use tauri::{command, State};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::{Mutex, Notify};

use truffle_core::transport::quic::{QuicConnection, QuicListener};
use truffle_core::transport::{DatagramSocket, RawListener};

use crate::commands::get_node;
use crate::TruffleState;

/// Default read size when the caller does not specify one (64 KiB — matches the
/// file-transfer chunk size and the NAPI default).
const DEFAULT_READ_BYTES: u32 = 64 * 1024;

/// Upper bound on a single read allocation (4 MiB).
const MAX_READ_BYTES: u32 = 4 * 1024 * 1024;

/// Receive buffer size: one datagram can be at most 64 KiB.
const RECV_BUF_BYTES: usize = 64 * 1024;

/// Mint a short registry id, e.g. `tcp-<uuid-v4>`. Prefixed by handle kind so
/// ids are self-describing in logs; the prefix is cosmetic (each kind lives in
/// its own map).
fn new_id(prefix: &str) -> String {
    format!("{prefix}-{}", uuid::Uuid::new_v4())
}

// ---------------------------------------------------------------------------
// Registry entries (parked in TruffleState; never serialized)
// ---------------------------------------------------------------------------

/// A parked raw TCP socket. Read and write halves hold independent locks so a
/// `tcp_read` blocked awaiting bytes never blocks a `tcp_write` on the same
/// socket — matching `NapiTcpSocket`. `close_notify` unblocks a pending
/// `tcp_read` on close (it holds the read lock across its await, so
/// `tcp_close` would otherwise block until the peer sent data).
pub struct TcpSocketEntry {
    read: Arc<Mutex<Option<OwnedReadHalf>>>,
    write: Arc<Mutex<Option<OwnedWriteHalf>>>,
    closed: Arc<AtomicBool>,
    close_notify: Arc<Notify>,
}

/// A parked raw TCP listener plus its resolved port (ephemeral `0` → real).
pub struct TcpListenerEntry {
    listener: Arc<Mutex<Option<RawListener>>>,
    port: u16,
}

/// A parked UDP socket. `close_notify` unblocks an in-flight `udp_recv` on
/// close (a datagram socket has no read half to shut down), mirroring
/// `NapiUdpSocket`.
pub struct UdpSocketEntry {
    socket: Arc<DatagramSocket>,
    closed: Arc<AtomicBool>,
    close_notify: Arc<Notify>,
}

/// A parked QUIC connection.
pub struct QuicConnectionEntry {
    conn: Arc<QuicConnection>,
}

/// A parked QUIC stream (independent send/recv locks, like the NAPI stream).
/// `connection_id` lets `quic_close` sweep this stream's registry entry when
/// its parent connection is closed. `close_notify` unblocks a pending
/// `quic_stream_read` on close (it holds the recv lock across its await).
pub struct QuicStreamEntry {
    send: Arc<Mutex<Option<quinn::SendStream>>>,
    recv: Arc<Mutex<Option<quinn::RecvStream>>>,
    closed: Arc<AtomicBool>,
    close_notify: Arc<Notify>,
    connection_id: String,
}

/// A parked QUIC listener.
pub struct QuicListenerEntry {
    listener: Arc<QuicListener>,
}

// ---------------------------------------------------------------------------
// Result DTOs (returned across IPC; camelCase for the frontend)
// ---------------------------------------------------------------------------

/// Result of `tcp_open` / metadata for an outbound socket.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TcpOpenResult {
    pub socket_id: String,
    pub remote_address: String,
    pub remote_peer_id: Option<String>,
}

/// Result of `tcp_listen`.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TcpListenResult {
    pub listener_id: String,
    pub port: u16,
}

/// Result of `tcp_accept` — a freshly registered inbound socket.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TcpAcceptResult {
    pub socket_id: String,
    pub remote_address: String,
    pub remote_peer_id: Option<String>,
    pub remote_peer_name: Option<String>,
}

/// Result of `udp_bind`.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UdpBindResult {
    pub socket_id: String,
    pub port: u16,
}

/// A datagram received via `udp_recv`.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UdpDatagram {
    pub data: Vec<u8>,
    /// Sender's tailnet IP (`100.x.x.x`) — WireGuard-authenticated.
    pub address: String,
    /// Sender's source port.
    pub port: u16,
}

/// Result of `quic_connect` / `quic_accept`.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QuicConnectResult {
    pub connection_id: String,
    pub remote_address: String,
    pub remote_peer_id: Option<String>,
}

/// Result of `quic_open_stream` / `quic_accept_stream`.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QuicStreamResult {
    pub stream_id: String,
}

/// Result of `quic_listen`.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QuicListenResult {
    pub listener_id: String,
    pub port: u16,
}

// ---------------------------------------------------------------------------
// TCP
// ---------------------------------------------------------------------------

/// Open a raw TCP connection to a peer on the given port (RFC 021).
///
/// `host` accepts a device id (or a unique ≥4-char prefix), device name,
/// Tailscale hostname, or Tailscale IP. Returns a `socketId` to use with
/// `tcp_read` / `tcp_write` / `tcp_end` / `tcp_close`.
#[command]
pub async fn tcp_open(
    state: State<'_, TruffleState>,
    host: String,
    port: u16,
) -> Result<TcpOpenResult, String> {
    let node = get_node(&state).await?;
    // Best-effort canonical id for metadata; open_tcp re-resolves internally.
    let remote_peer_id = node.resolve_peer_id(&host).await.ok();
    let stream = node
        .open_tcp(&host, port)
        .await
        .map_err(|e| e.to_string())?;
    let (read, write) = stream.into_split();
    let socket_id = new_id("tcp");
    state.tcp_sockets.lock().await.insert(
        socket_id.clone(),
        TcpSocketEntry {
            read: Arc::new(Mutex::new(Some(read))),
            write: Arc::new(Mutex::new(Some(write))),
            closed: Arc::new(AtomicBool::new(false)),
            close_notify: Arc::new(Notify::new()),
        },
    );
    Ok(TcpOpenResult {
        socket_id,
        remote_address: format!("{host}:{port}"),
        remote_peer_id,
    })
}

/// Read up to `max_bytes` (default 64 KiB) from a TCP socket.
///
/// Resolves the next chunk, or `null` on clean EOF (peer finished writing) and
/// after `tcp_close`. Pull-model — awaiting is the backpressure.
#[command]
pub async fn tcp_read(
    state: State<'_, TruffleState>,
    socket_id: String,
    max_bytes: Option<u32>,
) -> Result<Option<Vec<u8>>, String> {
    // Clone the read half's Arc out under the map lock, then release the map
    // lock before awaiting the read (so other sockets are not serialized).
    let (read, closed, close_notify) = {
        let map = state.tcp_sockets.lock().await;
        let entry = map
            .get(&socket_id)
            .ok_or_else(|| format!("No TCP socket with id: {socket_id}"))?;
        (
            entry.read.clone(),
            entry.closed.clone(),
            entry.close_notify.clone(),
        )
    };

    // Register for the close signal BEFORE checking the flag, so a concurrent
    // tcp_close can never slip between check and select.
    let notified = close_notify.notified();
    tokio::pin!(notified);
    notified.as_mut().enable();
    if closed.load(Ordering::Acquire) {
        return Ok(None);
    }

    let cap = max_bytes
        .unwrap_or(DEFAULT_READ_BYTES)
        .clamp(1, MAX_READ_BYTES) as usize;
    let mut guard = read.lock().await;
    let half = match guard.as_mut() {
        Some(half) => half,
        None => return Ok(None), // closed locally
    };
    let mut buf = vec![0u8; cap];
    tokio::select! {
        biased;
        _ = &mut notified => Ok(None),
        result = half.read(&mut buf) => {
            let n = result.map_err(|e| format!("tcp read: {e}"))?;
            if n == 0 {
                return Ok(None); // clean EOF
            }
            buf.truncate(n);
            Ok(Some(buf))
        }
    }
}

/// Write all of `data` to a TCP socket (respects backpressure).
#[command]
pub async fn tcp_write(
    state: State<'_, TruffleState>,
    socket_id: String,
    data: Vec<u8>,
) -> Result<(), String> {
    let write = {
        let map = state.tcp_sockets.lock().await;
        map.get(&socket_id)
            .ok_or_else(|| format!("No TCP socket with id: {socket_id}"))?
            .write
            .clone()
    };
    let mut guard = write.lock().await;
    let half = guard
        .as_mut()
        .ok_or_else(|| "tcp write: socket closed".to_string())?;
    half.write_all(&data)
        .await
        .map_err(|e| format!("tcp write: {e}"))
}

/// Half-close the write side (send FIN), like `socket.end()`. Reading remains
/// possible until the peer closes its side. Idempotent.
#[command]
pub async fn tcp_end(state: State<'_, TruffleState>, socket_id: String) -> Result<(), String> {
    let write = {
        let map = state.tcp_sockets.lock().await;
        map.get(&socket_id)
            .ok_or_else(|| format!("No TCP socket with id: {socket_id}"))?
            .write
            .clone()
    };
    if let Some(mut half) = write.lock().await.take() {
        let _ = half.shutdown().await;
    }
    Ok(())
}

/// Fully close a TCP socket (both directions) and drop it from the registry.
/// Idempotent — closing an unknown/already-closed id is a no-op.
#[command]
pub async fn tcp_close(state: State<'_, TruffleState>, socket_id: String) -> Result<(), String> {
    let entry = state.tcp_sockets.lock().await.remove(&socket_id);
    if let Some(entry) = entry {
        // Signal first: a pending tcp_read exits with None and releases the
        // read-half lock, so the take below cannot block on it.
        entry.closed.store(true, Ordering::Release);
        entry.close_notify.notify_waiters();
        if let Some(mut half) = entry.write.lock().await.take() {
            let _ = half.shutdown().await;
        }
        let _ = entry.read.lock().await.take();
    }
    Ok(())
}

/// Listen for raw TCP connections on a port (RFC 021).
///
/// Port 0 binds an ephemeral port — read the resolved value from the returned
/// `port`. Only the session WebSocket port (default 9417) is reserved.
#[command]
pub async fn tcp_listen(
    state: State<'_, TruffleState>,
    port: u16,
) -> Result<TcpListenResult, String> {
    let node = get_node(&state).await?;
    let listener = node.listen_tcp(port).await.map_err(|e| e.to_string())?;
    let resolved_port = listener.port;
    let listener_id = new_id("tcpl");
    state.tcp_listeners.lock().await.insert(
        listener_id.clone(),
        TcpListenerEntry {
            listener: Arc::new(Mutex::new(Some(listener))),
            port: resolved_port,
        },
    );
    Ok(TcpListenResult {
        listener_id,
        port: resolved_port,
    })
}

/// Accept the next incoming connection on a listener.
///
/// Resolves `null` once the listener has been closed (or the id is unknown).
/// The accepted socket is registered and its `socketId` returned, along with
/// the WhoIs identity from the bridge header when present.
#[command]
pub async fn tcp_accept(
    state: State<'_, TruffleState>,
    listener_id: String,
) -> Result<Option<TcpAcceptResult>, String> {
    let listener = {
        let map = state.tcp_listeners.lock().await;
        match map.get(&listener_id) {
            Some(entry) => entry.listener.clone(),
            None => return Ok(None),
        }
    };

    let incoming = {
        let mut guard = listener.lock().await;
        match guard.as_mut() {
            Some(l) => l.accept().await,
            None => return Ok(None),
        }
    };

    let Some(incoming) = incoming else {
        return Ok(None);
    };

    let remote_peer_id = incoming
        .remote_identity
        .as_ref()
        .and_then(|i| i.node_id.clone());
    let remote_peer_name = incoming.remote_identity.as_ref().and_then(|i| {
        i.display_name
            .clone()
            .or_else(|| i.login_name.clone())
            .or_else(|| i.dns_name.clone())
    });
    let remote_address = incoming.remote_addr.clone();
    let (read, write) = incoming.stream.into_split();
    let socket_id = new_id("tcp");
    state.tcp_sockets.lock().await.insert(
        socket_id.clone(),
        TcpSocketEntry {
            read: Arc::new(Mutex::new(Some(read))),
            write: Arc::new(Mutex::new(Some(write))),
            closed: Arc::new(AtomicBool::new(false)),
            close_notify: Arc::new(Notify::new()),
        },
    );
    Ok(Some(TcpAcceptResult {
        socket_id,
        remote_address,
        remote_peer_id,
        remote_peer_name,
    }))
}

/// Stop listening and release the tsnet port. Pending `tcp_accept` calls
/// resolve `null`. Idempotent.
#[command]
pub async fn tcp_unlisten(
    state: State<'_, TruffleState>,
    listener_id: String,
) -> Result<(), String> {
    let entry = state.tcp_listeners.lock().await.remove(&listener_id);
    if let Some(entry) = entry {
        // Release the tsnet port first. That closes the listener's channel, so
        // an in-flight `tcp_accept` unblocks and returns null; the RawListener
        // then drops once that accept (and this `entry`) release their `Arc`.
        // We intentionally do NOT take the listener lock here: a blocked accept
        // holds it across `accept().await`, so taking it would deadlock.
        if let Ok(node) = get_node(&state).await {
            if let Err(e) = node.unlisten_tcp(entry.port).await {
                tracing::debug!(port = entry.port, "unlisten_tcp on close: {e}");
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// UDP
// ---------------------------------------------------------------------------

/// Bind a UDP datagram socket on a port (RFC 021).
///
/// Datagram boundaries are preserved; keep payloads ≤ ~1,200 bytes (tailnet
/// MTU). Port 0 binds an ephemeral relay port. The returned `port` echoes the
/// requested value (0 for ephemeral) — parity with `NapiUdpSocket.port()`,
/// whose tsnet port is not reported back by the relay.
#[command]
pub async fn udp_bind(state: State<'_, TruffleState>, port: u16) -> Result<UdpBindResult, String> {
    let node = get_node(&state).await?;
    let socket = node.bind_udp(port).await.map_err(|e| e.to_string())?;
    let socket_id = new_id("udp");
    state.udp_sockets.lock().await.insert(
        socket_id.clone(),
        UdpSocketEntry {
            socket: Arc::new(socket),
            closed: Arc::new(AtomicBool::new(false)),
            close_notify: Arc::new(Notify::new()),
        },
    );
    Ok(UdpBindResult { socket_id, port })
}

/// Send a datagram to `host:port`. `host` accepts a peer ref or literal IP.
/// Resolves with the number of payload bytes sent.
#[command]
pub async fn udp_send(
    state: State<'_, TruffleState>,
    socket_id: String,
    data: Vec<u8>,
    host: String,
    port: u16,
) -> Result<u32, String> {
    let (socket, closed) = {
        let map = state.udp_sockets.lock().await;
        let entry = map
            .get(&socket_id)
            .ok_or_else(|| format!("No UDP socket with id: {socket_id}"))?;
        (entry.socket.clone(), entry.closed.clone())
    };
    if closed.load(Ordering::Acquire) {
        return Err("udp send: socket closed".to_string());
    }

    // A literal IP is used verbatim; anything else resolves as a peer ref.
    let node = get_node(&state).await?;
    let ip = match host.parse::<std::net::IpAddr>() {
        Ok(ip) => ip,
        Err(_) => node
            .resolve_peer_ip(&host)
            .await
            .map_err(|e| e.to_string())?,
    };

    let n = socket
        .send_to(&data, &format!("{ip}:{port}"))
        .await
        .map_err(|e| format!("udp send: {e}"))?;
    Ok(n as u32)
}

/// Receive the next datagram. Resolves `null` once the socket has been closed
/// (a pending call unblocks via the close notify). UDP is unreliable —
/// datagrams may be dropped or reordered.
#[command]
pub async fn udp_recv(
    state: State<'_, TruffleState>,
    socket_id: String,
) -> Result<Option<UdpDatagram>, String> {
    let (socket, closed, close_notify) = {
        let map = state.udp_sockets.lock().await;
        let entry = match map.get(&socket_id) {
            Some(e) => e,
            None => return Ok(None),
        };
        (
            entry.socket.clone(),
            entry.closed.clone(),
            entry.close_notify.clone(),
        )
    };
    // Register for the close signal BEFORE checking the flag, so a concurrent
    // udp_close can never slip between check and select (notify_waiters wakes
    // only already-enabled waiters).
    let notified = close_notify.notified();
    tokio::pin!(notified);
    notified.as_mut().enable();
    if closed.load(Ordering::Acquire) {
        return Ok(None);
    }

    let mut buf = vec![0u8; RECV_BUF_BYTES];
    tokio::select! {
        biased;
        _ = &mut notified => Ok(None),
        result = socket.recv_from(&mut buf) => {
            let (n, addr) = result.map_err(|e| format!("udp recv: {e}"))?;
            buf.truncate(n);
            // DatagramSocket reports the sender as an "ip:port" string.
            let (address, port) = match addr.parse::<std::net::SocketAddr>() {
                Ok(sock_addr) => (sock_addr.ip().to_string(), sock_addr.port()),
                Err(_) => (addr, 0),
            };
            Ok(Some(UdpDatagram { data: buf, address, port }))
        }
    }
}

/// Close a UDP socket and drop it from the registry. Any in-flight `udp_recv`
/// resolves `null`. Idempotent.
#[command]
pub async fn udp_close(state: State<'_, TruffleState>, socket_id: String) -> Result<(), String> {
    if let Some(entry) = state.udp_sockets.lock().await.remove(&socket_id) {
        entry.closed.store(true, Ordering::Release);
        entry.close_notify.notify_waiters();
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// QUIC
// ---------------------------------------------------------------------------

/// Best-effort reverse lookup of a tailnet IP to a known peer's device id.
async fn peer_id_for_ip(state: &TruffleState, ip: &str) -> Option<String> {
    let node = get_node(state).await.ok()?;
    node.peers()
        .await
        .into_iter()
        .find(|p| p.ip.to_string() == ip)
        .map(|p| p.device_id.unwrap_or(p.tailscale_id))
}

/// Open a raw QUIC connection to a peer on the given port (RFC 021).
///
/// The connection carries multiple concurrent bidirectional byte streams with
/// no head-of-line blocking. Only same-app peers can complete the handshake
/// (ALPN scoping). `host` accepts the same identifier forms as `tcp_open`.
#[command]
pub async fn quic_connect(
    state: State<'_, TruffleState>,
    host: String,
    port: u16,
) -> Result<QuicConnectResult, String> {
    let node = get_node(&state).await?;
    let remote_peer_id = node.resolve_peer_id(&host).await.ok();
    let conn = node
        .connect_quic(&host, port)
        .await
        .map_err(|e| e.to_string())?;
    let remote_address = conn.remote_address().to_string();
    let connection_id = new_id("quic");
    state.quic_connections.lock().await.insert(
        connection_id.clone(),
        QuicConnectionEntry {
            conn: Arc::new(conn),
        },
    );
    Ok(QuicConnectResult {
        connection_id,
        remote_address,
        remote_peer_id,
    })
}

/// Open a new bidirectional byte stream on a QUIC connection. Returns a
/// `streamId` for the `quic_stream_*` commands.
#[command]
pub async fn quic_open_stream(
    state: State<'_, TruffleState>,
    connection_id: String,
) -> Result<QuicStreamResult, String> {
    let conn = {
        let map = state.quic_connections.lock().await;
        map.get(&connection_id)
            .ok_or_else(|| format!("No QUIC connection with id: {connection_id}"))?
            .conn
            .clone()
    };
    let stream = conn.open_stream().await.map_err(|e| e.to_string())?;
    let stream_id = register_quic_stream(&state, stream, connection_id).await;
    Ok(QuicStreamResult { stream_id })
}

/// Accept the next stream the peer opens on a QUIC connection. Resolves `null`
/// once the connection has closed (either side). Streams are lazy — the peer's
/// accept does not fire until the opener writes.
#[command]
pub async fn quic_accept_stream(
    state: State<'_, TruffleState>,
    connection_id: String,
) -> Result<Option<QuicStreamResult>, String> {
    let conn = {
        let map = state.quic_connections.lock().await;
        match map.get(&connection_id) {
            Some(e) => e.conn.clone(),
            None => return Ok(None),
        }
    };
    match conn.accept_stream().await.map_err(|e| e.to_string())? {
        Some(stream) => {
            let stream_id = register_quic_stream(&state, stream, connection_id).await;
            Ok(Some(QuicStreamResult { stream_id }))
        }
        None => Ok(None),
    }
}

/// Split a QUIC stream into halves and park it in the registry, tagged with its
/// parent connection so `quic_close` can sweep it.
async fn register_quic_stream(
    state: &TruffleState,
    stream: truffle_core::transport::quic::QuicStream,
    connection_id: String,
) -> String {
    let (send, recv) = stream.into_split();
    let stream_id = new_id("quics");
    state.quic_streams.lock().await.insert(
        stream_id.clone(),
        QuicStreamEntry {
            send: Arc::new(Mutex::new(Some(send))),
            recv: Arc::new(Mutex::new(Some(recv))),
            closed: Arc::new(AtomicBool::new(false)),
            close_notify: Arc::new(Notify::new()),
            connection_id,
        },
    );
    stream_id
}

/// Read up to `max_bytes` (default 64 KiB) from a QUIC stream. Resolves the
/// next chunk, or `null` on clean EOF and after close.
#[command]
pub async fn quic_stream_read(
    state: State<'_, TruffleState>,
    stream_id: String,
    max_bytes: Option<u32>,
) -> Result<Option<Vec<u8>>, String> {
    let (recv, closed, close_notify) = {
        let map = state.quic_streams.lock().await;
        let entry = map
            .get(&stream_id)
            .ok_or_else(|| format!("No QUIC stream with id: {stream_id}"))?;
        (
            entry.recv.clone(),
            entry.closed.clone(),
            entry.close_notify.clone(),
        )
    };

    // Register for the close signal BEFORE checking the flag, so a concurrent
    // quic_stream_close can never slip between check and select.
    let notified = close_notify.notified();
    tokio::pin!(notified);
    notified.as_mut().enable();
    if closed.load(Ordering::Acquire) {
        return Ok(None);
    }

    let cap = max_bytes
        .unwrap_or(DEFAULT_READ_BYTES)
        .clamp(1, MAX_READ_BYTES) as usize;
    let mut guard = recv.lock().await;
    let half = match guard.as_mut() {
        Some(half) => half,
        None => return Ok(None),
    };
    let mut buf = vec![0u8; cap];
    tokio::select! {
        biased;
        _ = &mut notified => Ok(None),
        result = half.read(&mut buf) => match result {
            Ok(Some(n)) => {
                buf.truncate(n);
                Ok(Some(buf))
            }
            Ok(None) => Ok(None), // clean EOF
            Err(e) => Err(format!("quic read: {e}")),
        }
    }
}

/// Write all of `data` to a QUIC stream (respects QUIC flow control).
#[command]
pub async fn quic_stream_write(
    state: State<'_, TruffleState>,
    stream_id: String,
    data: Vec<u8>,
) -> Result<(), String> {
    let send = {
        let map = state.quic_streams.lock().await;
        map.get(&stream_id)
            .ok_or_else(|| format!("No QUIC stream with id: {stream_id}"))?
            .send
            .clone()
    };
    let mut guard = send.lock().await;
    let half = guard
        .as_mut()
        .ok_or_else(|| "quic write: stream closed".to_string())?;
    half.write_all(&data)
        .await
        .map_err(|e| format!("quic write: {e}"))
}

/// Half-close: finish the write side, signalling clean EOF to the peer.
/// Reading remains possible. Idempotent.
#[command]
pub async fn quic_stream_finish(
    state: State<'_, TruffleState>,
    stream_id: String,
) -> Result<(), String> {
    let send = {
        let map = state.quic_streams.lock().await;
        map.get(&stream_id)
            .ok_or_else(|| format!("No QUIC stream with id: {stream_id}"))?
            .send
            .clone()
    };
    if let Some(mut half) = send.lock().await.take() {
        let _ = half.finish();
    }
    Ok(())
}

/// Fully close a QUIC stream (finish writes, stop reads) and drop it from the
/// registry. Idempotent.
#[command]
pub async fn quic_stream_close(
    state: State<'_, TruffleState>,
    stream_id: String,
) -> Result<(), String> {
    if let Some(entry) = state.quic_streams.lock().await.remove(&stream_id) {
        // Signal first: a pending quic_stream_read exits with None and
        // releases the recv-half lock, so the take below cannot block on it.
        entry.closed.store(true, Ordering::Release);
        entry.close_notify.notify_waiters();
        if let Some(mut half) = entry.send.lock().await.take() {
            let _ = half.finish();
        }
        if let Some(mut half) = entry.recv.lock().await.take() {
            let _ = half.stop(quinn::VarInt::from_u32(0));
        }
    }
    Ok(())
}

/// Close a QUIC connection and all its streams, dropping the connection and any
/// of its stream entries from the registries. Idempotent.
#[command]
pub async fn quic_close(
    state: State<'_, TruffleState>,
    connection_id: String,
) -> Result<(), String> {
    if let Some(entry) = state.quic_connections.lock().await.remove(&connection_id) {
        entry.conn.close();
    }
    // The QUIC layer already closed the streams above; sweep their registry
    // entries so they do not leak. An in-flight read that cloned the half's Arc
    // still completes (with an error/EOF from the closed connection).
    state
        .quic_streams
        .lock()
        .await
        .retain(|_, s| s.connection_id != connection_id);
    Ok(())
}

/// Listen for raw QUIC connections on a port (RFC 021).
///
/// Pass 0 to choose an ephemeral tailnet port. The result contains the
/// assigned port. Only the session WebSocket port (default 9417) is reserved.
#[command]
pub async fn quic_listen(
    state: State<'_, TruffleState>,
    port: u16,
) -> Result<QuicListenResult, String> {
    let node = get_node(&state).await?;
    let listener = node.listen_quic(port).await.map_err(|e| e.to_string())?;
    let resolved_port = listener.port();
    let listener_id = new_id("quicl");
    state.quic_listeners.lock().await.insert(
        listener_id.clone(),
        QuicListenerEntry {
            listener: Arc::new(listener),
        },
    );
    Ok(QuicListenResult {
        listener_id,
        port: resolved_port,
    })
}

/// Accept the next incoming QUIC connection. Resolves `null` once the listener
/// has been closed. Cross-app attempts fail the TLS handshake (ALPN scoping)
/// and are skipped, never surfaced.
#[command]
pub async fn quic_accept(
    state: State<'_, TruffleState>,
    listener_id: String,
) -> Result<Option<QuicConnectResult>, String> {
    let listener = {
        let map = state.quic_listeners.lock().await;
        match map.get(&listener_id) {
            Some(e) => e.listener.clone(),
            None => return Ok(None),
        }
    };
    match listener.accept().await {
        Some(conn) => {
            let remote_address = conn.remote_address().to_string();
            let remote_ip = conn.remote_address().ip().to_string();
            let remote_peer_id = peer_id_for_ip(&state, &remote_ip).await;
            let connection_id = new_id("quic");
            state.quic_connections.lock().await.insert(
                connection_id.clone(),
                QuicConnectionEntry {
                    conn: Arc::new(conn),
                },
            );
            Ok(Some(QuicConnectResult {
                connection_id,
                remote_address,
                remote_peer_id,
            }))
        }
        None => Ok(None),
    }
}

/// Close a QUIC listener (and connections accepted from it — they share the
/// endpoint). Pending `quic_accept` calls resolve `null`. Idempotent.
#[command]
pub async fn quic_listener_close(
    state: State<'_, TruffleState>,
    listener_id: String,
) -> Result<(), String> {
    if let Some(entry) = state.quic_listeners.lock().await.remove(&listener_id) {
        entry.listener.close();
    }
    Ok(())
}
