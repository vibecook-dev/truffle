//! NapiNode — Node.js wrapper for `Node<TailscaleProvider>`.

use std::sync::Arc;

use napi::bindgen_prelude::*;
use napi::threadsafe_function::{ThreadsafeFunction, ThreadsafeFunctionCallMode};
use napi::{Status, Unknown};
use napi_derive::napi;
use tokio::task::AbortHandle;

use truffle_core::network::tailscale::TailscaleProvider;
use truffle_core::Node;

use crate::file_transfer::NapiFileTransfer;
use crate::proxy::NapiProxy;
use crate::quic::{NapiQuicConnection, NapiQuicListener};
use crate::raw_socket::{NapiTcpListener, NapiTcpSocket};
use crate::subscription::NapiSubscription;
use crate::synced_store::NapiSyncedStore;
use crate::types::{
    NapiHealthInfo, NapiNamespacedMessage, NapiNodeConfig, NapiNodeIdentity, NapiPeer,
    NapiPeerEvent, NapiPeerIdentity, NapiPingResult,
};
use crate::udp_socket::NapiUdpSocket;

/// Helper: convert a truffle-core `Peer` to a `NapiPeer` (RFC 022).
fn peer_to_napi(p: &truffle_core::node::Peer) -> NapiPeer {
    NapiPeer {
        device_id: p.device_id.clone(),
        device_name: p.device_name.clone(),
        display_name: p.display_name.clone(),
        hostname: p.hostname.clone(),
        ip: p.ip.to_string(),
        online: p.online,
        ws_connected: p.ws_connected,
        connection_type: p.connection_type.clone(),
        os: p.os.clone(),
        last_seen: p.last_seen.clone(),
        tailscale_id: p.tailscale_id.clone(),
        peer_ref: p.peer_ref.clone(),
        generation: p.generation as u32,
    }
}

/// The main truffle node exposed to JavaScript.
///
/// Lifecycle:
/// 1. `new NapiNode()` — creates an empty wrapper.
/// 2. `await node.start(config)` — builds and starts the underlying `Node<TailscaleProvider>`.
/// 3. Use `getPeers()`, `send()`, `onPeerChange()`, etc.
/// 4. `await node.stop()` — shuts down the node.
#[napi]
pub struct NapiNode {
    node: Option<Arc<Node<TailscaleProvider>>>,
    /// Handles to spawned event-forwarding tasks (cancelled on stop).
    task_handles: Vec<AbortHandle>,
    /// Pre-start auth-required callback. Installed via `on_auth_required()`
    /// and consumed by `start()`, which wires it into
    /// `NodeBuilder::build_with_auth_handler`. Necessary because auth fires
    /// during `start()` — before a post-start `on_peer_change` subscription
    /// could possibly exist.
    ///
    /// `CalleeHandled = false` so the JS callback receives `(url)` directly
    /// rather than `(err, url)` — there's no async error path here.
    pending_auth_callback:
        Option<ThreadsafeFunction<String, Unknown<'static>, String, Status, false>>,
}

#[napi]
impl NapiNode {
    /// Create a new (unstarted) NapiNode.
    #[napi(constructor)]
    pub fn new() -> Self {
        Self {
            node: None,
            task_handles: Vec::new(),
            pending_auth_callback: None,
        }
    }

    /// Install a callback to receive the Tailscale auth URL.
    ///
    /// Must be called BEFORE `start()`. During startup the Tailscale
    /// provider may require interactive authentication; if so, this
    /// callback is invoked with the auth URL while `start()` is still
    /// waiting. If no callback is installed, `start()` uses the plain
    /// build path and may reject with "timed out waiting for
    /// authentication" after 5 minutes.
    #[napi(ts_args_type = "callback: (url: string) => void")]
    pub fn on_auth_required(
        &mut self,
        callback: ThreadsafeFunction<String, Unknown<'static>, String, Status, false>,
    ) -> Result<()> {
        self.pending_auth_callback = Some(callback);
        Ok(())
    }

    /// Start the node with the given configuration.
    ///
    /// Resolves when the Tailscale sidecar is connected and the node is ready.
    ///
    /// # Safety
    /// This takes `&mut self` in an async context. The caller must ensure that
    /// no other calls are made on this NapiNode while `start()` is in progress.
    #[napi]
    pub async unsafe fn start(&mut self, config: NapiNodeConfig) -> Result<()> {
        // RFC 017: app_id is required and validated; device_name /
        // device_id are optional overrides that the builder defaults when
        // absent (OS hostname / auto-generated ULID).
        let mut builder = Node::<TailscaleProvider>::builder()
            .app_id(&config.app_id)
            .map_err(|e| Error::from_reason(e.to_string()))?
            .sidecar_path(&config.sidecar_path);

        if let Some(ref device_name) = config.device_name {
            builder = builder.device_name(device_name);
        }
        if let Some(ref device_id) = config.device_id {
            builder = builder
                .device_id(device_id)
                .map_err(|e| Error::from_reason(e.to_string()))?;
        }
        if let Some(ref hostname) = config.hostname {
            builder = builder
                .hostname(hostname)
                .map_err(|e| Error::from_reason(e.to_string()))?;
        }
        if let Some(ref dir) = config.state_dir {
            builder = builder.state_dir(dir);
        }
        if let Some(ref key) = config.auth_key {
            builder = builder.auth_key(key);
        }
        if let Some(ephemeral) = config.ephemeral {
            builder = builder.ephemeral(ephemeral);
        }
        if let Some(port) = config.ws_port {
            builder = builder.ws_port(port);
        }
        if let Some(eager) = config.eager_identity {
            builder = builder.eager_identity(eager);
        }

        // If the caller installed a pre-start auth handler via
        // `on_auth_required()`, use `build_with_auth_handler` so the auth
        // URL is forwarded to JS while the provider is still waiting for
        // login. Otherwise fall back to the plain build path.
        let node = match self.pending_auth_callback.take() {
            Some(cb) => builder
                .build_with_auth_handler(move |url| {
                    // Fatal error strategy: pass the value directly.
                    cb.call(url, ThreadsafeFunctionCallMode::NonBlocking);
                })
                .await
                .map_err(|e| Error::from_reason(format!("Failed to start node: {e}")))?,
            None => builder
                .build()
                .await
                .map_err(|e| Error::from_reason(format!("Failed to start node: {e}")))?,
        };

        self.node = Some(Arc::new(node));
        Ok(())
    }

    /// Stop the node and release all resources.
    ///
    /// # Safety
    /// This takes `&mut self` in an async context. The caller must ensure that
    /// no other calls are made on this NapiNode while `stop()` is in progress.
    #[napi]
    pub async unsafe fn stop(&mut self) -> Result<()> {
        // Cancel all event-forwarding tasks
        for handle in self.task_handles.drain(..) {
            handle.abort();
        }
        if let Some(node) = self.node.take() {
            node.stop().await;
        }
        Ok(())
    }

    /// Get the local node's identity.
    #[napi]
    pub fn get_local_info(&self) -> Result<NapiNodeIdentity> {
        let node = self.require_node()?;
        let info = node.local_info();
        Ok(NapiNodeIdentity {
            app_id: info.app_id,
            device_id: info.device_id,
            device_name: info.device_name,
            tailscale_hostname: info.tailscale_hostname,
            tailscale_id: info.tailscale_id,
            dns_name: info.dns_name,
            ip: info.ip.map(|ip| ip.to_string()),
        })
    }

    /// Get all known peers as plain snapshots (RFC 022 honest fields).
    ///
    /// Prefer {@link createMeshNode}'s wrapped `getPeers()` which returns
    /// interned `Peer` handles with `===` identity. This native method
    /// returns plain objects for low-level callers.
    #[napi]
    pub async fn get_peers(&self) -> Result<Vec<NapiPeer>> {
        let node = self.require_node()?;
        let peers = node.peers().await;
        Ok(peers.iter().map(peer_to_napi).collect())
    }

    /// Resolve a query to a peer snapshot (RFC 022).
    ///
    /// Accepts display name, ULID, ULID prefix (≥4 unique chars), hostname,
    /// Tailscale id, or tailnet IP. Not found → `null`. Ambiguous → throws.
    ///
    /// `waitMs`: block until resolvable or timeout (then `null`) — for
    /// rehydrating a saved ULID before hello completes.
    #[napi]
    pub async fn peer(&self, query: String, wait_ms: Option<u32>) -> Result<Option<NapiPeer>> {
        let node = self.require_node()?;
        let found = node
            .peer(&query, wait_ms.map(|m| m as u64))
            .await
            .map_err(|e| Error::from_reason(e.to_string()))?;
        Ok(found.as_ref().map(peer_to_napi))
    }

    /// Resolve a peer identifier (name or ID) to a routing string.
    ///
    /// @deprecated Prefer {@link peer} or Peer handles from `createMeshNode`.
    #[napi]
    pub async fn resolve_peer_id(&self, peer_id: String) -> Result<String> {
        let node = self.require_node()?;
        node.resolve_peer_id(&peer_id)
            .await
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Ping a peer and return latency info.
    ///
    /// `peer_id` may be a ULID, name, Tailscale id, or IP (query string).
    #[napi]
    pub async fn ping(&self, peer_id: String) -> Result<NapiPingResult> {
        let node = self.require_node()?;
        let result = node
            .ping(&peer_id)
            .await
            .map_err(|e| Error::from_reason(e.to_string()))?;
        Ok(NapiPingResult {
            latency_ms: result.latency.as_secs_f64() * 1000.0,
            connection: result.connection,
            peer_addr: result.peer_addr,
        })
    }

    /// Tailnet identity (WhoIs) of the node that owns an address.
    ///
    /// `addr` accepts a raw tailnet IP or `ip:port` (e.g. a QUIC
    /// connection's `remoteAddress()` verbatim — the port is ignored), or
    /// any `resolvePeerId` identifier form for mesh peers. Unlike `peers()`,
    /// a raw address reaches ANY tailnet device — other apps' truffle nodes,
    /// plain machines, tagged nodes — and the answer carries user identity
    /// (login, display name, profile pic) that peer entries deliberately do
    /// not.
    ///
    /// Resolves `null` when the tailnet has no identity for the address:
    /// absent, not fabricated. Requires a v3 sidecar (rejects on older ones).
    #[napi]
    pub async fn whois(&self, addr: String) -> Result<Option<NapiPeerIdentity>> {
        let node = self.require_node()?;
        node.whois(&addr)
            .await
            .map(|identity| identity.map(NapiPeerIdentity::from))
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Get health information from the network layer.
    #[napi]
    pub async fn health(&self) -> Result<NapiHealthInfo> {
        let node = self.require_node()?;
        let info = node.health().await;
        Ok(NapiHealthInfo {
            state: info.state,
            key_expiry: info.key_expiry,
            warnings: info.warnings,
            healthy: info.healthy,
        })
    }

    /// Send a namespaced message to a specific peer.
    ///
    /// Legacy content-sniffing behavior: bytes that parse as UTF-8 JSON go
    /// on the wire as that JSON value, anything else as a byte array.
    /// Prefer `sendJson` / `sendBytes`, whose wire type never depends on
    /// the data's contents.
    #[napi]
    pub async fn send(&self, peer_id: String, namespace: String, data: Buffer) -> Result<()> {
        let node = self.require_node()?;
        // Sniffing is deprecated in core but kept here: it is the documented
        // contract of the JS send() API.
        #[allow(deprecated)]
        node.send(&peer_id, &namespace, data.as_ref())
            .await
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Send a JSON payload to a specific peer (explicit wire type).
    #[napi]
    pub async fn send_json(
        &self,
        peer_id: String,
        namespace: String,
        payload: serde_json::Value,
    ) -> Result<()> {
        let node = self.require_node()?;
        node.send_json(&peer_id, &namespace, &payload)
            .await
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Send opaque binary data to a specific peer (explicit wire type).
    ///
    /// The bytes travel base64-encoded under the `"bytes"` msg_type — the
    /// wire representation never depends on the data's contents.
    #[napi]
    pub async fn send_bytes(&self, peer_id: String, namespace: String, data: Buffer) -> Result<()> {
        let node = self.require_node()?;
        node.send_bytes(&peer_id, &namespace, data.as_ref())
            .await
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Broadcast a namespaced message to all connected peers.
    ///
    /// Legacy content-sniffing behavior — prefer `broadcastJson` /
    /// `broadcastBytes`, which also report queueing failures.
    #[napi]
    pub async fn broadcast(&self, namespace: String, data: Buffer) -> Result<()> {
        let node = self.require_node()?;
        // See send(): sniffing is the documented JS contract.
        #[allow(deprecated)]
        node.broadcast(&namespace, data.as_ref()).await;
        Ok(())
    }

    /// Broadcast a JSON payload to all connected peers, reporting how many
    /// peers it was queued to.
    #[napi]
    pub async fn broadcast_json(
        &self,
        namespace: String,
        payload: serde_json::Value,
    ) -> Result<crate::types::NapiBroadcastReport> {
        let node = self.require_node()?;
        let report = node
            .broadcast_json(&namespace, &payload)
            .await
            .map_err(|e| Error::from_reason(e.to_string()))?;
        Ok(report.into())
    }

    /// Broadcast opaque binary data to all connected peers, reporting how
    /// many peers it was queued to.
    #[napi]
    pub async fn broadcast_bytes(
        &self,
        namespace: String,
        data: Buffer,
    ) -> Result<crate::types::NapiBroadcastReport> {
        let node = self.require_node()?;
        let report = node
            .broadcast_bytes(&namespace, data.as_ref())
            .await
            .map_err(|e| Error::from_reason(e.to_string()))?;
        Ok(report.into())
    }

    /// Open a raw TCP connection to a peer on the given port (RFC 021).
    ///
    /// `host` accepts a device id (or a unique ≥4-char prefix), device
    /// name, Tailscale hostname, or Tailscale IP. The returned socket is
    /// pull-model — see `NapiTcpSocket`; the `@vibecook/truffle` package
    /// wraps it in a `stream.Duplex`.
    #[napi]
    pub async fn open_tcp(&self, host: String, port: u16) -> Result<NapiTcpSocket> {
        let node = self.require_node()?;
        // Best-effort canonical id for socket metadata; open_tcp re-resolves
        // the same identifier forms internally.
        let peer_id = node.resolve_peer_id(&host).await.ok();
        let stream = node
            .open_tcp(&host, port)
            .await
            .map_err(|e| Error::from_reason(e.to_string()))?;
        Ok(NapiTcpSocket::from_stream(
            stream,
            format!("{host}:{port}"),
            peer_id,
            None,
            None,
        ))
    }

    /// Listen for raw TCP connections on a port (RFC 021).
    ///
    /// Port 0 binds an ephemeral port — read the resolved port from the
    /// returned listener. The session WebSocket port (default 9417) is
    /// reserved. Pass `tls: true` to terminate TLS in the sidecar with
    /// automatic MagicDNS certificates (RFC 023 §7.1 — requires MagicDNS +
    /// HTTPS enabled on the tailnet, and a sidecar built with RFC 023 for
    /// port 443).
    #[napi]
    pub async fn listen_tcp(&self, port: u16, tls: Option<bool>) -> Result<NapiTcpListener> {
        let node = self.require_node()?;
        let listener = node
            .listen_tcp_opts(
                port,
                truffle_core::network::ListenOpts {
                    tls: tls.unwrap_or(false),
                },
            )
            .await
            .map_err(|e| Error::from_reason(e.to_string()))?;
        Ok(NapiTcpListener::new(listener, node))
    }

    /// Bind a UDP datagram socket on a port (RFC 021).
    ///
    /// Datagram boundaries are preserved; keep payloads ≤ ~1,200 bytes
    /// (tailnet MTU). Port 0 binds an ephemeral relay port — suitable for
    /// client-style sockets that send first.
    #[napi]
    pub async fn bind_udp(&self, port: u16) -> Result<NapiUdpSocket> {
        let node = self.require_node()?;
        let socket = node
            .bind_udp(port)
            .await
            .map_err(|e| Error::from_reason(e.to_string()))?;
        Ok(NapiUdpSocket::new(socket, node, port))
    }

    /// Open a raw QUIC connection to a peer on the given port (RFC 021).
    ///
    /// The connection carries multiple concurrent bidirectional byte
    /// streams with no head-of-line blocking. Only same-app peers can
    /// complete the handshake (ALPN scoping). `host` accepts the same
    /// identifier forms as `openTcp`.
    #[napi]
    pub async fn connect_quic(&self, host: String, port: u16) -> Result<NapiQuicConnection> {
        let node = self.require_node()?;
        let peer_id = node.resolve_peer_id(&host).await.ok();
        let conn = node
            .connect_quic(&host, port)
            .await
            .map_err(|e| Error::from_reason(e.to_string()))?;
        Ok(NapiQuicConnection::new(conn, peer_id, node))
    }

    /// Listen for raw QUIC connections on a port (RFC 021).
    ///
    /// Ports 443 and 9417 are reserved; port 0 is not supported over the
    /// tsnet relay — choose an explicit port.
    #[napi]
    pub async fn listen_quic(&self, port: u16) -> Result<NapiQuicListener> {
        let node = self.require_node()?;
        let listener = node
            .listen_quic(port)
            .await
            .map_err(|e| Error::from_reason(e.to_string()))?;
        Ok(NapiQuicListener::new(listener, node))
    }

    /// Subscribe to peer change events.
    ///
    /// The callback receives `NapiPeerEvent` objects whenever peers
    /// join, leave, connect, disconnect, or update. Call `close()` on the
    /// returned subscription to stop receiving events.
    #[napi(ts_args_type = "callback: (event: PeerEvent) => void")]
    pub fn on_peer_change(
        &mut self,
        callback: ThreadsafeFunction<NapiPeerEvent, Unknown<'static>, NapiPeerEvent, Status, false>,
    ) -> Result<NapiSubscription> {
        let node = self.require_node()?;
        let mut rx = node.on_peer_change();
        let node_for_lookup = node.clone();

        // Use napi-rs's managed Tokio runtime. Bare `tokio::spawn` panics
        // with "there is no reactor running" because this is a sync NAPI
        // method invoked on the Node.js main thread, which has no Tokio
        // runtime in context.
        let handle = napi::bindgen_prelude::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(event) => {
                        let napi_event = convert_peer_event(&event, Some(&node_for_lookup)).await;
                        let status =
                            callback.call(napi_event, ThreadsafeFunctionCallMode::NonBlocking);
                        if status != Status::Ok {
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(missed = n, "on_peer_change lagged");
                        continue;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        });
        let subscription = NapiSubscription::from_task(handle);
        self.task_handles.retain(|handle| !handle.is_finished());
        self.task_handles.push(subscription.abort_handle());

        Ok(subscription)
    }

    /// Subscribe to messages on a specific namespace.
    ///
    /// The callback receives `NapiNamespacedMessage` objects. Call `close()`
    /// on the returned subscription to stop receiving messages.
    #[napi(ts_args_type = "namespace: string, callback: (msg: NamespacedMessage) => void")]
    pub fn on_message(
        &mut self,
        namespace: String,
        callback: ThreadsafeFunction<
            NapiNamespacedMessage,
            Unknown<'static>,
            NapiNamespacedMessage,
            Status,
            false,
        >,
    ) -> Result<NapiSubscription> {
        let node = self.require_node()?;
        let mut rx = node.subscribe(&namespace);

        // Use napi-rs's managed Tokio runtime. Bare `tokio::spawn` panics
        // with "there is no reactor running" because this is a sync NAPI
        // method invoked on the Node.js main thread, which has no Tokio
        // runtime in context.
        let handle = napi::bindgen_prelude::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(msg) => {
                        let napi_msg = NapiNamespacedMessage {
                            from: msg.from,
                            namespace: msg.namespace,
                            msg_type: msg.msg_type,
                            payload: msg.payload,
                            timestamp: msg.timestamp.map(|t| t as f64),
                        };
                        let status =
                            callback.call(napi_msg, ThreadsafeFunctionCallMode::NonBlocking);
                        if status != Status::Ok {
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(missed = n, "on_message lagged");
                        continue;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        });
        let subscription = NapiSubscription::from_task(handle);
        self.task_handles.retain(|handle| !handle.is_finished());
        self.task_handles.push(subscription.abort_handle());

        Ok(subscription)
    }

    /// Get a `NapiFileTransfer` handle for file transfer operations.
    #[napi]
    pub fn file_transfer(&self) -> Result<NapiFileTransfer> {
        let node = self.require_node()?;
        Ok(NapiFileTransfer::new(node))
    }

    /// Get a `NapiProxy` handle for reverse proxy operations.
    #[napi]
    pub fn proxy(&self) -> Result<NapiProxy> {
        let node = self.require_node()?;
        Ok(NapiProxy::new(node))
    }

    /// Get a `NapiSyncedStore` handle for synchronized state operations.
    ///
    /// Each call creates a new store instance with the given `store_id`.
    /// Multiple stores can coexist with different IDs.
    #[napi]
    pub fn synced_store(&self, store_id: String) -> Result<NapiSyncedStore> {
        let node = self.require_node()?;
        Ok(NapiSyncedStore::new(node, &store_id))
    }

    // -- Internal helpers ----------------------------------------------------

    fn require_node(&self) -> Result<Arc<Node<TailscaleProvider>>> {
        self.node
            .as_ref()
            .cloned()
            .ok_or_else(|| Error::from_reason("Node not started. Call start() first."))
    }
}

// ---------------------------------------------------------------------------
// PeerEvent conversion
// ---------------------------------------------------------------------------

/// Convert a core `PeerEvent` into its NAPI shape (RFC 022).
///
/// `peer_id` is always the Tailscale routing key when a peer is involved.
/// Prefer the nested `peer` object for display and durable identity.
async fn convert_peer_event(
    event: &truffle_core::session::PeerEvent,
    node: Option<&Arc<Node<TailscaleProvider>>>,
) -> NapiPeerEvent {
    use truffle_core::session::PeerEvent;

    let from_state = |state: &truffle_core::session::PeerState| -> NapiPeer {
        let peer: truffle_core::node::Peer = state.clone().into();
        peer_to_napi(&peer)
    };

    async fn peer_snapshot(
        node: Option<&Arc<Node<TailscaleProvider>>>,
        tailscale_id: &str,
    ) -> Option<NapiPeer> {
        if let Some(node) = node {
            for p in node.peers().await {
                if p.tailscale_id == tailscale_id {
                    return Some(peer_to_napi(&p));
                }
            }
        }
        None
    }

    match event {
        PeerEvent::Joined(state) => {
            let peer = from_state(state);
            NapiPeerEvent {
                event_type: "joined".to_string(),
                peer_id: peer.tailscale_id.clone(),
                peer: Some(peer),
                auth_url: None,
            }
        }
        PeerEvent::Left(state) => {
            // Core emits `Left` with the entry's final (offline) state — the
            // registry no longer holds it, so a snapshot lookup would miss.
            let peer = from_state(state);
            NapiPeerEvent {
                event_type: "left".to_string(),
                peer_id: peer.tailscale_id.clone(),
                peer: Some(peer),
                auth_url: None,
            }
        }
        PeerEvent::Updated(state) => {
            let peer = from_state(state);
            NapiPeerEvent {
                event_type: "updated".to_string(),
                peer_id: peer.tailscale_id.clone(),
                peer: Some(peer),
                auth_url: None,
            }
        }
        PeerEvent::Identity(state) => {
            let peer = from_state(state);
            NapiPeerEvent {
                event_type: "identity".to_string(),
                peer_id: peer.tailscale_id.clone(),
                peer: Some(peer),
                auth_url: None,
            }
        }
        PeerEvent::WsConnected(id) => NapiPeerEvent {
            event_type: "ws_connected".to_string(),
            peer_id: id.clone(),
            peer: peer_snapshot(node, id).await,
            auth_url: None,
        },
        PeerEvent::WsDisconnected(id) => NapiPeerEvent {
            event_type: "ws_disconnected".to_string(),
            peer_id: id.clone(),
            peer: peer_snapshot(node, id).await,
            auth_url: None,
        },
        PeerEvent::AuthRequired { url } => NapiPeerEvent {
            event_type: "auth_required".to_string(),
            peer_id: String::new(),
            peer: None,
            auth_url: Some(url.clone()),
        },
    }
}
