//! Raw QUIC bindings (RFC 021 Phase 3).
//!
//! Same pull-model philosophy as `raw_socket.rs`: byte data crosses the
//! NAPI boundary only as the resolution of a JS-initiated promise. A QUIC
//! connection carries many concurrent bidirectional byte streams with no
//! head-of-line blocking between them; each stream's read and write halves
//! hold independent locks so full-duplex use never self-blocks.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use napi::bindgen_prelude::*;
use napi_derive::napi;
use tokio::sync::{Mutex, Notify, OnceCell};

use truffle_core::network::tailscale::TailscaleProvider;
use truffle_core::transport::quic::{QuicConnection, QuicListener};
use truffle_core::Node;

use crate::types::NapiPeerIdentity;

/// Default read size when the caller does not specify one (64 KiB).
const DEFAULT_READ_BYTES: u32 = 64 * 1024;

/// Upper bound on a single read allocation (4 MiB).
const MAX_READ_BYTES: u32 = 4 * 1024 * 1024;

/// Best-effort reverse lookup of a tailnet IP to a known peer's device id.
async fn peer_id_for_ip(node: &Arc<Node<TailscaleProvider>>, ip: &str) -> Option<String> {
    node.peers()
        .await
        .into_iter()
        .find(|p| p.ip.to_string() == ip)
        .map(|p| p.device_id.unwrap_or(p.tailscale_id))
}

/// A raw QUIC connection to a peer over the mesh.
///
/// Open streams with `openStream()`; accept peer-opened streams with
/// `acceptStream()` (resolves `null` once the connection closes). Streams
/// are lazy — the peer's accept does not fire until the opener writes.
#[napi]
pub struct NapiQuicConnection {
    conn: Arc<QuicConnection>,
    node: Arc<Node<TailscaleProvider>>,
    remote_peer_id: Option<String>,
    /// Lazily-resolved WhoIs identity: successful answers (including a
    /// genuine anonymous `None`) cache for the connection's lifetime;
    /// lookup errors are NOT cached, so the next call retries.
    remote_identity: OnceCell<Option<NapiPeerIdentity>>,
}

impl NapiQuicConnection {
    pub(crate) fn new(
        conn: QuicConnection,
        remote_peer_id: Option<String>,
        node: Arc<Node<TailscaleProvider>>,
    ) -> Self {
        Self {
            conn: Arc::new(conn),
            node,
            remote_peer_id,
            remote_identity: OnceCell::new(),
        }
    }
}

#[napi]
impl NapiQuicConnection {
    /// Open a new bidirectional byte stream on this connection.
    #[napi]
    pub async fn open_stream(&self) -> Result<NapiQuicStream> {
        let stream = self
            .conn
            .open_stream()
            .await
            .map_err(|e| Error::from_reason(e.to_string()))?;
        Ok(NapiQuicStream::from_stream(stream))
    }

    /// Accept the next stream the peer opens.
    ///
    /// Resolves `null` once the connection has closed (either side).
    #[napi]
    pub async fn accept_stream(&self) -> Result<Option<NapiQuicStream>> {
        match self
            .conn
            .accept_stream()
            .await
            .map_err(|e| Error::from_reason(e.to_string()))?
        {
            Some(stream) => Ok(Some(NapiQuicStream::from_stream(stream))),
            None => Ok(None),
        }
    }

    /// The peer's tailnet address (`100.x.x.x:port` over the relay).
    #[napi]
    pub fn remote_address(&self) -> String {
        self.conn.remote_address().to_string()
    }

    /// The peer's stable device id when known. `null` means "anonymous but
    /// tailnet-authenticated".
    #[napi]
    pub fn remote_peer_id(&self) -> Option<String> {
        self.remote_peer_id.clone()
    }

    /// The peer's full WhoIs identity — the tailnet's answer about the
    /// connection's WireGuard-authenticated remote address, never anything
    /// the stream claims about itself. Reaches ANY tailnet caller, not just
    /// mesh peers, and works on outbound connections too.
    ///
    /// Resolved lazily on first call — `accept()` never blocks on identity —
    /// and cached for the connection's lifetime. `null` for anonymous
    /// callers, on pre-v3 sidecars, and when the lookup fails; failures are
    /// not cached, so calling again retries (use `node.whois()` to
    /// distinguish "anonymous" from "lookup failed").
    #[napi]
    pub async fn remote_identity(&self) -> Option<NapiPeerIdentity> {
        let ip = self.conn.remote_address().ip().to_string();
        self.remote_identity
            .get_or_try_init(|| async {
                self.node
                    .whois(&ip)
                    .await
                    .map(|identity| identity.map(NapiPeerIdentity::from))
            })
            .await
            .ok()
            .cloned()
            .flatten()
    }

    /// Close the connection and all its streams. Idempotent.
    #[napi]
    pub fn close(&self) {
        self.conn.close();
    }
}

/// A bidirectional byte stream on a QUIC connection.
///
/// Behaves like an independent TCP connection (ordered, reliable,
/// flow-controlled) without head-of-line blocking against sibling streams.
/// Pull-model reads, like `NapiTcpSocket`.
#[napi]
pub struct NapiQuicStream {
    send: Arc<Mutex<Option<quinn::SendStream>>>,
    recv: Arc<Mutex<Option<quinn::RecvStream>>>,
    /// Set by `close()`; `close_notify` unblocks a pending `read()` so the
    /// recv-half lock is never held past close.
    closed: Arc<AtomicBool>,
    close_notify: Arc<Notify>,
}

impl NapiQuicStream {
    fn from_stream(stream: truffle_core::transport::quic::QuicStream) -> Self {
        let (send, recv) = stream.into_split();
        Self {
            send: Arc::new(Mutex::new(Some(send))),
            recv: Arc::new(Mutex::new(Some(recv))),
            closed: Arc::new(AtomicBool::new(false)),
            close_notify: Arc::new(Notify::new()),
        }
    }
}

#[napi]
impl NapiQuicStream {
    /// Read up to `maxBytes` (default 64 KiB) from the stream.
    ///
    /// Resolves with the next chunk, or `null` on clean EOF (the peer
    /// finished the stream) and after `close()`.
    #[napi]
    pub async fn read(&self, max_bytes: Option<u32>) -> Result<Option<Buffer>> {
        let cap = max_bytes
            .unwrap_or(DEFAULT_READ_BYTES)
            .clamp(1, MAX_READ_BYTES) as usize;

        // Register for the close signal BEFORE checking the flag, so a
        // concurrent close() can never slip between check and select.
        let notified = self.close_notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if self.closed.load(Ordering::Acquire) {
            return Ok(None);
        }

        let mut guard = self.recv.lock().await;
        let half = match guard.as_mut() {
            Some(half) => half,
            None => return Ok(None), // closed locally
        };

        let mut buf = vec![0u8; cap];
        tokio::select! {
            biased;
            _ = &mut notified => Ok(None),
            result = half.read(&mut buf) => match result {
                Ok(Some(n)) => {
                    buf.truncate(n);
                    Ok(Some(buf.into()))
                }
                Ok(None) => Ok(None), // clean EOF
                Err(e) => Err(Error::from_reason(format!("quic read: {e}"))),
            }
        }
    }

    /// Write all of `data` to the stream (respects QUIC flow control).
    #[napi]
    pub async fn write(&self, data: Buffer) -> Result<()> {
        let mut guard = self.send.lock().await;
        let half = guard
            .as_mut()
            .ok_or_else(|| Error::from_reason("quic write: stream closed"))?;
        half.write_all(data.as_ref())
            .await
            .map_err(|e| Error::from_reason(format!("quic write: {e}")))
    }

    /// Half-close: finish the write side, signalling clean EOF to the
    /// peer. Reading remains possible. Idempotent.
    #[napi]
    pub async fn finish(&self) -> Result<()> {
        let mut guard = self.send.lock().await;
        if let Some(mut half) = guard.take() {
            let _ = half.finish();
        }
        Ok(())
    }

    /// Fully close the stream (finish writes, stop reads). Unblocks any
    /// pending `read()` (it resolves `null`). Idempotent.
    #[napi]
    pub async fn close(&self) -> Result<()> {
        // Signal first: a pending read() exits with None and releases the
        // recv-half lock, so the take below cannot block on it.
        self.closed.store(true, Ordering::Release);
        self.close_notify.notify_waiters();
        {
            let mut guard = self.send.lock().await;
            if let Some(mut half) = guard.take() {
                let _ = half.finish();
            }
        }
        {
            let mut guard = self.recv.lock().await;
            if let Some(mut half) = guard.take() {
                let _ = half.stop(quinn::VarInt::from_u32(0));
            }
        }
        Ok(())
    }
}

/// A listener for raw QUIC connections on a mesh port.
#[napi]
pub struct NapiQuicListener {
    listener: Arc<QuicListener>,
    node: Arc<Node<TailscaleProvider>>,
}

impl NapiQuicListener {
    pub(crate) fn new(listener: QuicListener, node: Arc<Node<TailscaleProvider>>) -> Self {
        Self {
            listener: Arc::new(listener),
            node,
        }
    }
}

#[napi]
impl NapiQuicListener {
    /// Accept the next incoming connection.
    ///
    /// Resolves `null` once the listener has been closed. Cross-app
    /// connection attempts fail the TLS handshake (ALPN scoping) and are
    /// skipped, never surfaced.
    #[napi]
    pub async fn accept(&self) -> Result<Option<NapiQuicConnection>> {
        match self.listener.accept().await {
            Some(conn) => {
                let remote_ip = conn.remote_address().ip().to_string();
                let peer_id = peer_id_for_ip(&self.node, &remote_ip).await;
                // No WhoIs here: identity resolves lazily on the first
                // remoteIdentity() call, so a slow sidecar answer can never
                // stall the accept loop for connections queued behind it.
                Ok(Some(NapiQuicConnection::new(
                    conn,
                    peer_id,
                    self.node.clone(),
                )))
            }
            None => Ok(None),
        }
    }

    /// The port this listener is bound to.
    #[napi]
    pub fn port(&self) -> u16 {
        self.listener.port()
    }

    /// Close the listener and connections accepted from it. Pending
    /// `accept()` calls resolve with `null`. Idempotent.
    #[napi]
    pub fn close(&self) {
        self.listener.close();
    }
}
