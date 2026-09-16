//! Request handler: dispatches JSON-RPC methods to the Node API.
//!
//! Each method maps directly to Node methods. No GoShim, no BridgeManager,
//! no pending_dials -- pure Node API.

use std::sync::Arc;
use std::time::Instant;

use tokio::sync::Notify;
use tracing::{debug, info};
use truffle_core::file_transfer::types::FileTransferEvent;
use truffle_core::network::tailscale::TailscaleProvider;
use truffle_core::node::Node;
use truffle_core::session::PeerEvent;

use super::protocol::{
    error_code, method, notification, DaemonNotification, DaemonRequest, DaemonResponse,
};

/// Context bundling all daemon-owned resources needed by request handlers.
pub struct DaemonContext {
    pub node: Arc<Node<TailscaleProvider>>,
    pub shutdown_signal: Arc<Notify>,
    pub started_at: Instant,
}

/// Result of dispatching a request: either a single response or a streaming subscription.
pub enum DispatchResult {
    /// A normal one-shot response.
    Response(DaemonResponse),
    /// A streaming subscription — the handler will push notifications until cancelled.
    /// Contains the parsed subscription parameters.
    Subscribe(SubscribeParams),
}

/// Parsed parameters for a subscribe request.
pub struct SubscribeParams {
    /// Which event types to subscribe to.
    pub events: Vec<SubscribeEventType>,
    /// Optional peer name filter (case-insensitive).
    pub peer_filter: Option<String>,
}

/// Event types that can be subscribed to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubscribeEventType {
    Peer,
    Message,
    Transfer,
}

/// Dispatch a JSON-RPC request to the appropriate handler.
pub async fn dispatch(
    request: &DaemonRequest,
    ctx: &DaemonContext,
    notification_tx: tokio::sync::mpsc::UnboundedSender<DaemonNotification>,
) -> DispatchResult {
    let node = &ctx.node;
    let started_at = ctx.started_at;
    let shutdown_signal = &ctx.shutdown_signal;

    let response = match request.method.as_str() {
        method::STATUS => handle_status(request.id, node, started_at).await,
        method::PEERS => handle_peers(request.id, node).await,
        method::PING => handle_ping(request.id, &request.params, node).await,
        method::SEND_MESSAGE => handle_send_message(request.id, &request.params, node).await,
        method::SHUTDOWN => handle_shutdown(request.id, shutdown_signal),
        method::TCP_CONNECT => handle_tcp_connect(request.id, &request.params, node).await,
        method::PUSH_FILE => {
            handle_push_file(request.id, &request.params, node, notification_tx).await
        }
        method::GET_FILE => {
            handle_get_file(request.id, &request.params, node, notification_tx).await
        }
        method::DOCTOR => handle_doctor(request.id, node).await,
        method::PROXY_ADD => handle_proxy_add(request.id, &request.params, node).await,
        method::PROXY_REMOVE => handle_proxy_remove(request.id, &request.params, node).await,
        method::PROXY_LIST => handle_proxy_list(request.id, node).await,
        method::SUBSCRIBE => {
            return match parse_subscribe_params(&request.params) {
                Ok(params) => DispatchResult::Subscribe(params),
                Err(resp) => DispatchResult::Response(resp),
            };
        }
        _ => DaemonResponse::error(
            request.id,
            error_code::METHOD_NOT_FOUND,
            format!("Method '{}' not found", request.method),
        ),
    };

    DispatchResult::Response(response)
}

/// Parse subscribe request params, returning `SubscribeParams` or an error response.
fn parse_subscribe_params(params: &serde_json::Value) -> Result<SubscribeParams, DaemonResponse> {
    let events_arr = params["events"].as_array().cloned().unwrap_or_default();

    let mut events = Vec::new();
    for v in &events_arr {
        match v.as_str() {
            Some("peer") => events.push(SubscribeEventType::Peer),
            Some("message") => events.push(SubscribeEventType::Message),
            Some("transfer") => events.push(SubscribeEventType::Transfer),
            Some(other) => {
                return Err(DaemonResponse::error(
                    0,
                    error_code::INVALID_PARAMS,
                    format!("Unknown event type: '{other}'. Valid types: peer, message, transfer"),
                ));
            }
            None => {}
        }
    }

    // Default to all events if none specified
    if events.is_empty() {
        events = vec![
            SubscribeEventType::Peer,
            SubscribeEventType::Message,
            SubscribeEventType::Transfer,
        ];
    }

    let peer_filter = params["filter"]["peer"].as_str().map(|s| s.to_lowercase());

    Ok(SubscribeParams {
        events,
        peer_filter,
    })
}

/// Run the streaming subscribe loop. Subscribes to node channels and forwards
/// matching events as notifications until the sender is closed (client disconnected).
pub async fn run_subscribe(
    params: &SubscribeParams,
    ctx: &DaemonContext,
    notification_tx: tokio::sync::mpsc::UnboundedSender<DaemonNotification>,
) {
    let node = &ctx.node;

    // Subscribe to the channels we need.
    let mut peer_rx = if params.events.contains(&SubscribeEventType::Peer) {
        Some(node.on_peer_change())
    } else {
        None
    };

    let mut message_rx = if params.events.contains(&SubscribeEventType::Message) {
        Some(node.subscribe("chat"))
    } else {
        None
    };

    let mut transfer_rx = if params.events.contains(&SubscribeEventType::Transfer) {
        Some(node.file_transfer().subscribe())
    } else {
        None
    };

    debug!(
        events = ?params.events,
        peer_filter = ?params.peer_filter,
        "Subscribe loop started"
    );

    loop {
        tokio::select! {
            // Peer events
            peer_event = async {
                match peer_rx.as_mut() {
                    Some(rx) => rx.recv().await,
                    None => std::future::pending().await,
                }
            } => {
                match peer_event {
                    Ok(event) => {
                        if let Some(notif) = peer_event_to_notification(&event, &params.peer_filter) {
                            if notification_tx.send(notif).is_err() {
                                break; // Client disconnected
                            }
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        debug!("Peer event subscriber lagged by {n} messages");
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        break;
                    }
                }
            }

            // Chat messages
            msg_event = async {
                match message_rx.as_mut() {
                    Some(rx) => rx.recv().await,
                    None => std::future::pending().await,
                }
            } => {
                match msg_event {
                    Ok(msg) => {
                        if let Some(notif) = message_to_notification(&msg, &params.peer_filter) {
                            if notification_tx.send(notif).is_err() {
                                break;
                            }
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        debug!("Message subscriber lagged by {n} messages");
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        break;
                    }
                }
            }

            // File transfer events (from core's FileTransfer::subscribe)
            ft_event = async {
                match transfer_rx.as_mut() {
                    Some(rx) => rx.recv().await,
                    None => std::future::pending().await,
                }
            } => {
                match ft_event {
                    Ok(event) => {
                        if let Some(notif) = ft_event_to_notification(&event) {
                            if notification_tx.send(notif).is_err() {
                                break;
                            }
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        debug!("Transfer subscriber lagged by {n} messages");
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        break;
                    }
                }
            }
        }
    }

    debug!("Subscribe loop ended");
}

/// Convert a PeerEvent into a DaemonNotification, applying the optional peer filter.
fn peer_event_to_notification(
    event: &PeerEvent,
    peer_filter: &Option<String>,
) -> Option<DaemonNotification> {
    // Extract a user-facing display name from a PeerState. Prefer the
    // identity advertised in the hello envelope (RFC 017 §8); fall back to
    // the Layer 3 Tailscale hostname when the hello has not yet landed.
    let display_name = |state: &truffle_core::session::PeerState| -> String {
        state
            .identity
            .as_ref()
            .map(|i| i.device_name.clone())
            .unwrap_or_else(|| state.name.clone())
    };
    let peer_device_id = |state: &truffle_core::session::PeerState| -> Option<String> {
        state.published_device_id().map(|s| s.to_string())
    };

    let (method_name, params) = match event {
        PeerEvent::Joined(state) => {
            let name = display_name(state);
            if !matches_peer_filter(&name, peer_filter) {
                return None;
            }
            (
                notification::PEER_JOINED,
                serde_json::json!({
                    "type": notification::PEER_JOINED,
                    "peer": name,
                    "device_id": peer_device_id(state),
                    "device_name": name,
                    "tailscale_id": state.id,
                    "ip": state.ip.to_string(),
                    "os": state.os,
                    "time": chrono::Utc::now().to_rfc3339(),
                }),
            )
        }
        PeerEvent::Left(state) => {
            // Keep the daemon protocol's field values unchanged: both fields
            // carried the Tailscale stable id before Left gained full state.
            let id = &state.id;
            if !matches_peer_filter(id, peer_filter) {
                return None;
            }
            (
                notification::PEER_LEFT,
                serde_json::json!({
                    "type": notification::PEER_LEFT,
                    "peer": id,
                    "tailscale_id": id,
                    "time": chrono::Utc::now().to_rfc3339(),
                }),
            )
        }
        PeerEvent::Updated(state) | PeerEvent::Identity(state) => {
            let name = display_name(state);
            if !matches_peer_filter(&name, peer_filter) {
                return None;
            }
            (
                notification::PEER_UPDATED,
                serde_json::json!({
                    "type": notification::PEER_UPDATED,
                    "peer": name,
                    "device_id": peer_device_id(state),
                    "device_name": name,
                    "tailscale_id": state.id,
                    "ip": state.ip.to_string(),
                    "online": state.online,
                    "ws_connected": state.ws_connected,
                    "time": chrono::Utc::now().to_rfc3339(),
                }),
            )
        }
        PeerEvent::WsConnected(id) => {
            if !matches_peer_filter(id, peer_filter) {
                return None;
            }
            (
                notification::PEER_WS_CONNECTED,
                serde_json::json!({
                    "type": notification::PEER_WS_CONNECTED,
                    "peer": id,
                    "time": chrono::Utc::now().to_rfc3339(),
                }),
            )
        }
        PeerEvent::WsDisconnected(id) => {
            if !matches_peer_filter(id, peer_filter) {
                return None;
            }
            (
                notification::PEER_WS_DISCONNECTED,
                serde_json::json!({
                    "type": notification::PEER_WS_DISCONNECTED,
                    "peer": id,
                    "time": chrono::Utc::now().to_rfc3339(),
                }),
            )
        }
        PeerEvent::AuthRequired { .. } => return None,
    };

    Some(DaemonNotification::new(method_name, params))
}

/// Convert a NamespacedMessage from the "chat" namespace into a notification.
fn message_to_notification(
    msg: &truffle_core::node::NamespacedMessage,
    peer_filter: &Option<String>,
) -> Option<DaemonNotification> {
    if !matches_peer_filter(&msg.from, peer_filter) {
        return None;
    }
    Some(DaemonNotification::new(
        notification::MESSAGE_RECEIVED,
        serde_json::json!({
            "type": notification::MESSAGE_RECEIVED,
            "from": msg.from,
            "namespace": msg.namespace,
            "msg_type": msg.msg_type,
            "payload": msg.payload,
            "time": chrono::Utc::now().to_rfc3339(),
        }),
    ))
}

/// Convert a core `FileTransferEvent` into a daemon notification.
fn ft_event_to_notification(event: &FileTransferEvent) -> Option<DaemonNotification> {
    let (event_type, params) = match event {
        FileTransferEvent::OfferReceived(offer) => (
            "transfer.offer_received",
            serde_json::json!({
                "type": "transfer.offer_received",
                "from": offer.from_peer,
                "from_name": offer.from_name,
                "file_name": offer.file_name,
                "size": offer.size,
                "sha256": offer.sha256,
                "token": offer.token,
                "time": chrono::Utc::now().to_rfc3339(),
            }),
        ),
        FileTransferEvent::Hashing {
            token,
            file_name,
            bytes_hashed,
            total_bytes,
        } => (
            "transfer.hashing",
            serde_json::json!({
                "type": "transfer.hashing",
                "token": token,
                "file_name": file_name,
                "bytes_hashed": bytes_hashed,
                "total_bytes": total_bytes,
                "percent": if *total_bytes > 0 { *bytes_hashed as f64 / *total_bytes as f64 * 100.0 } else { 0.0 },
                "time": chrono::Utc::now().to_rfc3339(),
            }),
        ),
        FileTransferEvent::WaitingForAccept { token, file_name } => (
            "transfer.waiting",
            serde_json::json!({
                "type": "transfer.waiting",
                "token": token,
                "file_name": file_name,
                "time": chrono::Utc::now().to_rfc3339(),
            }),
        ),
        FileTransferEvent::Progress(p) => (
            "transfer.progress",
            serde_json::json!({
                "type": "transfer.progress",
                "token": p.token,
                "direction": format!("{:?}", p.direction),
                "file_name": p.file_name,
                "bytes_transferred": p.bytes_transferred,
                "total_bytes": p.total_bytes,
                "speed_bps": p.speed_bps,
                "percent": if p.total_bytes > 0 {
                    p.bytes_transferred as f64 / p.total_bytes as f64 * 100.0
                } else { 0.0 },
                "time": chrono::Utc::now().to_rfc3339(),
            }),
        ),
        FileTransferEvent::Completed {
            token,
            direction,
            file_name,
            bytes_transferred,
            sha256,
            elapsed_secs,
        } => (
            "transfer.completed",
            serde_json::json!({
                "type": "transfer.completed",
                "token": token,
                "direction": format!("{:?}", direction),
                "file_name": file_name,
                "bytes_transferred": bytes_transferred,
                "sha256": sha256,
                "elapsed_secs": elapsed_secs,
                "time": chrono::Utc::now().to_rfc3339(),
            }),
        ),
        FileTransferEvent::Rejected {
            token,
            file_name,
            reason,
        } => (
            "transfer.rejected",
            serde_json::json!({
                "type": "transfer.rejected",
                "token": token,
                "file_name": file_name,
                "reason": reason,
                "time": chrono::Utc::now().to_rfc3339(),
            }),
        ),
        FileTransferEvent::Failed {
            token,
            direction,
            file_name,
            reason,
        } => (
            "transfer.failed",
            serde_json::json!({
                "type": "transfer.failed",
                "token": token,
                "direction": format!("{:?}", direction),
                "file_name": file_name,
                "reason": reason,
                "time": chrono::Utc::now().to_rfc3339(),
            }),
        ),
    };

    Some(DaemonNotification::new(event_type, params))
}

/// Check if a peer name/id matches the optional filter (case-insensitive).
fn matches_peer_filter(name: &str, filter: &Option<String>) -> bool {
    match filter {
        Some(f) => name.to_lowercase().contains(f),
        None => true,
    }
}

// ==========================================================================
// Status
// ==========================================================================

async fn handle_status(
    id: u64,
    node: &Arc<Node<TailscaleProvider>>,
    started_at: Instant,
) -> DaemonResponse {
    let info = node.local_info();
    let peers = node.peers().await;
    let health = node.health().await;
    let uptime_secs = started_at.elapsed().as_secs();

    let status = if health.healthy {
        "online"
    } else {
        &health.state
    };
    let ip_str = info.ip.map(|ip| ip.to_string()).unwrap_or_default();

    DaemonResponse::success(
        id,
        serde_json::json!({
            "app_id": info.app_id,
            "device_id": info.device_id,
            "device_name": info.device_name,
            "tailscale_hostname": info.tailscale_hostname,
            "tailscale_id": info.tailscale_id,
            "ip": ip_str,
            "dns_name": info.dns_name.unwrap_or_default(),
            // RFC 025 §3.6: this node's own tailnet login; null when unknown.
            "login_name": info.login_name,
            "status": status,
            "uptime_secs": uptime_secs,
            "peer_count": peers.len(),
            "health": {
                "state": health.state,
                "healthy": health.healthy,
                "key_expiry": health.key_expiry,
                "warnings": health.warnings,
            },
        }),
    )
}

// ==========================================================================
// Peers
// ==========================================================================

async fn handle_peers(id: u64, node: &Arc<Node<TailscaleProvider>>) -> DaemonResponse {
    let peers = node.peers().await;

    let peers_json: Vec<serde_json::Value> = peers
        .iter()
        .map(|p| {
            serde_json::json!({
                "device_id": p.device_id, // Option — null until identity (RFC 022)
                "device_name": p.device_name,
                "display_name": p.display_name,
                "hostname": p.hostname,
                "tailscale_id": p.tailscale_id,
                "peer_ref": p.peer_ref,
                "generation": p.generation,
                "ip": p.ip.to_string(),
                "online": p.online,
                "ws_connected": p.ws_connected,
                "connection_type": p.connection_type,
                "os": p.os,
                "last_seen": p.last_seen,
                // RFC 025 §3.6: the peer owner's tailnet login. null when
                // Layer 3 cannot know it (sidecar below protocol 5, or no
                // profile for the owner) — absent, never fabricated.
                "login_name": p.login_name,
            })
        })
        .collect();

    DaemonResponse::success(id, serde_json::json!({ "peers": peers_json }))
}

// ==========================================================================
// Ping
// ==========================================================================

async fn handle_ping(
    id: u64,
    params: &serde_json::Value,
    node: &Arc<Node<TailscaleProvider>>,
) -> DaemonResponse {
    let peer_id = match params["node"].as_str() {
        Some(n) => n,
        None => {
            return DaemonResponse::error(
                id,
                error_code::INVALID_PARAMS,
                "Missing 'node' parameter",
            )
        }
    };

    match node.ping(peer_id).await {
        Ok(result) => DaemonResponse::success(
            id,
            serde_json::json!({
                "latency_ms": result.latency.as_secs_f64() * 1000.0,
                "connection": result.connection,
                "peer_addr": result.peer_addr,
            }),
        ),
        Err(e) => DaemonResponse::error(id, error_code::NODE_NOT_FOUND, e.to_string()),
    }
}

// ==========================================================================
// Send message
// ==========================================================================

async fn handle_send_message(
    id: u64,
    params: &serde_json::Value,
    node: &Arc<Node<TailscaleProvider>>,
) -> DaemonResponse {
    let peer_id = match params["peer_id"].as_str().or(params["device_id"].as_str()) {
        Some(n) => n,
        None => {
            return DaemonResponse::error(
                id,
                error_code::INVALID_PARAMS,
                "Missing 'peer_id' parameter",
            )
        }
    };

    let namespace = params["namespace"].as_str().unwrap_or("chat");
    let message = params["message"]
        .as_str()
        .or(params["payload"].as_str())
        .unwrap_or("");

    // Daemon wire contract (pre-0.7 sniffing, now explicit): a message
    // string that parses as JSON is sent as that JSON value; anything else
    // keeps the legacy byte-array representation.
    let payload = serde_json::from_str::<serde_json::Value>(message)
        .unwrap_or_else(|_| serde_json::Value::from(message.as_bytes().to_vec()));

    match node.send_json(peer_id, namespace, &payload).await {
        Ok(()) => DaemonResponse::success(id, serde_json::json!({ "sent": true })),
        Err(e) => DaemonResponse::error(id, error_code::INTERNAL_ERROR, e.to_string()),
    }
}

// ==========================================================================
// Shutdown
// ==========================================================================

fn handle_shutdown(id: u64, shutdown_signal: &Arc<Notify>) -> DaemonResponse {
    info!("Received shutdown request");
    shutdown_signal.notify_one();
    DaemonResponse::success(id, serde_json::json!({ "shutting_down": true }))
}

// ==========================================================================
// TCP connect
// ==========================================================================

async fn handle_tcp_connect(
    id: u64,
    params: &serde_json::Value,
    node: &Arc<Node<TailscaleProvider>>,
) -> DaemonResponse {
    let peer_id = match params["node"].as_str() {
        Some(n) => n,
        None => {
            return DaemonResponse::error(
                id,
                error_code::INVALID_PARAMS,
                "Missing 'node' parameter",
            )
        }
    };

    let port = params["port"].as_u64().unwrap_or(0) as u16;
    if port == 0 {
        return DaemonResponse::error(
            id,
            error_code::INVALID_PARAMS,
            "Missing or invalid 'port' parameter",
        );
    }

    match node.open_tcp(peer_id, port).await {
        Ok(_stream) => {
            // For check mode, we just verify connectivity
            DaemonResponse::success(
                id,
                serde_json::json!({
                    "connected": true,
                    "peer": peer_id,
                    "port": port,
                }),
            )
        }
        Err(e) => DaemonResponse::error(id, error_code::INTERNAL_ERROR, e.to_string()),
    }
}

// ==========================================================================
// File transfer (push)
// ==========================================================================

async fn handle_push_file(
    id: u64,
    params: &serde_json::Value,
    node: &Arc<Node<TailscaleProvider>>,
    notification_tx: tokio::sync::mpsc::UnboundedSender<DaemonNotification>,
) -> DaemonResponse {
    let peer_id = match params["peer_id"].as_str() {
        Some(n) => n,
        None => {
            return DaemonResponse::error(
                id,
                error_code::INVALID_PARAMS,
                "Missing 'peer_id' parameter",
            )
        }
    };

    let local_path = match params["local_path"].as_str() {
        Some(p) => p,
        None => {
            return DaemonResponse::error(
                id,
                error_code::INVALID_PARAMS,
                "Missing 'local_path' parameter",
            )
        }
    };

    let remote_path = match params["remote_path"].as_str() {
        Some(p) => p,
        None => {
            return DaemonResponse::error(
                id,
                error_code::INVALID_PARAMS,
                "Missing 'remote_path' parameter",
            )
        }
    };

    // Subscribe to core file transfer events and forward progress as daemon notifications
    let ft = node.file_transfer();
    let mut rx = ft.subscribe();

    let progress_handle = tokio::spawn(async move {
        loop {
            match rx.recv().await {
                Ok(FileTransferEvent::Hashing {
                    bytes_hashed,
                    total_bytes,
                    ..
                }) => {
                    let notif = DaemonNotification::new(
                        super::protocol::notification::CP_PROGRESS,
                        serde_json::json!({
                            "bytes_sent": 0,
                            "total_bytes": total_bytes,
                            "bytes_per_second": 0.0,
                            "percent": 0.0,
                            "phase": "hashing",
                            "hash_percent": if total_bytes > 0 {
                                bytes_hashed as f64 / total_bytes as f64 * 100.0
                            } else { 0.0 },
                        }),
                    );
                    let _ = notification_tx.send(notif);
                }
                Ok(FileTransferEvent::WaitingForAccept { .. }) => {
                    let notif = DaemonNotification::new(
                        super::protocol::notification::CP_PROGRESS,
                        serde_json::json!({
                            "bytes_sent": 0,
                            "total_bytes": 0,
                            "bytes_per_second": 0.0,
                            "percent": 0.0,
                            "phase": "waiting",
                        }),
                    );
                    let _ = notification_tx.send(notif);
                }
                Ok(FileTransferEvent::Progress(p)) => {
                    let notif = DaemonNotification::new(
                        super::protocol::notification::CP_PROGRESS,
                        serde_json::json!({
                            "bytes_sent": p.bytes_transferred,
                            "total_bytes": p.total_bytes,
                            "bytes_per_second": p.speed_bps,
                            "percent": if p.total_bytes > 0 {
                                p.bytes_transferred as f64 / p.total_bytes as f64 * 100.0
                            } else { 0.0 },
                        }),
                    );
                    let _ = notification_tx.send(notif);
                }
                Ok(_) => continue,
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    });

    let result = ft.send_file(peer_id, local_path, remote_path).await;
    progress_handle.abort();

    match result {
        Ok(result) => DaemonResponse::success(
            id,
            serde_json::json!({
                "success": true,
                "bytes_transferred": result.bytes_transferred,
                "sha256": result.sha256,
                "elapsed_secs": result.elapsed_secs,
            }),
        ),
        Err(e) => DaemonResponse::error(id, error_code::INTERNAL_ERROR, e.to_string()),
    }
}

// ==========================================================================
// File transfer (get/download)
// ==========================================================================

async fn handle_get_file(
    id: u64,
    params: &serde_json::Value,
    node: &Arc<Node<TailscaleProvider>>,
    notification_tx: tokio::sync::mpsc::UnboundedSender<DaemonNotification>,
) -> DaemonResponse {
    let peer_id = match params["peer_id"].as_str() {
        Some(n) => n,
        None => {
            return DaemonResponse::error(
                id,
                error_code::INVALID_PARAMS,
                "Missing 'peer_id' parameter",
            )
        }
    };

    let remote_path = match params["remote_path"].as_str() {
        Some(p) => p,
        None => {
            return DaemonResponse::error(
                id,
                error_code::INVALID_PARAMS,
                "Missing 'remote_path' parameter",
            )
        }
    };

    let local_path = match params["local_path"].as_str() {
        Some(p) => p,
        None => {
            return DaemonResponse::error(
                id,
                error_code::INVALID_PARAMS,
                "Missing 'local_path' parameter",
            )
        }
    };

    // Subscribe to core file transfer events and forward progress as daemon notifications
    let ft = node.file_transfer();
    let mut rx = ft.subscribe();

    let progress_handle = tokio::spawn(async move {
        loop {
            match rx.recv().await {
                Ok(FileTransferEvent::Progress(p)) => {
                    let notif = DaemonNotification::new(
                        super::protocol::notification::CP_PROGRESS,
                        serde_json::json!({
                            "bytes_received": p.bytes_transferred,
                            "total_bytes": p.total_bytes,
                            "bytes_per_second": p.speed_bps,
                            "percent": if p.total_bytes > 0 {
                                p.bytes_transferred as f64 / p.total_bytes as f64 * 100.0
                            } else { 0.0 },
                        }),
                    );
                    let _ = notification_tx.send(notif);
                }
                Ok(_) => continue,
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    });

    let result = ft.pull_file(peer_id, remote_path, local_path).await;
    progress_handle.abort();

    match result {
        Ok(result) => DaemonResponse::success(
            id,
            serde_json::json!({
                "success": true,
                "bytes_transferred": result.bytes_transferred,
                "sha256": result.sha256,
                "elapsed_secs": result.elapsed_secs,
            }),
        ),
        Err(e) => DaemonResponse::error(id, error_code::INTERNAL_ERROR, e.to_string()),
    }
}

// ==========================================================================
// Doctor
// ==========================================================================

// ==========================================================================
// Reverse Proxy
// ==========================================================================

/// Snake_case shape of one `proxy_add` route on the CLI <-> daemon JSON-RPC
/// wire. The whole daemon protocol is snake_case; core's
/// [`ProxyRoute`](truffle_core::proxy::ProxyRoute) is camelCase, so this
/// mirror exists purely to translate the casing at the boundary (RFC 023 P3).
#[derive(serde::Deserialize)]
struct ProxyRouteParams {
    prefix: String,
    #[serde(default)]
    target_url: Option<String>,
    #[serde(default)]
    dir: Option<String>,
    #[serde(default)]
    fallback: Option<String>,
    #[serde(default)]
    strip_prefix: bool,
    #[serde(default)]
    allow: Vec<String>,
}

impl From<ProxyRouteParams> for truffle_core::proxy::ProxyRoute {
    fn from(r: ProxyRouteParams) -> Self {
        Self {
            prefix: r.prefix,
            target_url: r.target_url,
            dir: r.dir,
            fallback: r.fallback,
            strip_prefix: r.strip_prefix,
            allow: r.allow,
        }
    }
}

async fn handle_proxy_add(
    id: u64,
    params: &serde_json::Value,
    node: &Arc<Node<TailscaleProvider>>,
) -> DaemonResponse {
    let proxy_id = params["id"].as_str().unwrap_or("").to_string();
    let name = params["name"].as_str().unwrap_or(&proxy_id).to_string();
    let listen_port = params["listen_port"].as_u64().unwrap_or(0) as u16;
    let target_host = params["target_host"]
        .as_str()
        .unwrap_or("localhost")
        .to_string();
    let target_port = params["target_port"].as_u64().unwrap_or(0) as u16;
    let target_scheme = params["target_scheme"]
        .as_str()
        .unwrap_or("http")
        .to_string();
    let announce = params["announce"].as_bool().unwrap_or(true);

    // RFC 023 v2 fields. Absent fields reproduce v1 behavior exactly, so old
    // CLIs (which omit them) keep the shipped single-target, always-TLS,
    // whole-tailnet semantics via serde defaults.
    let tls = params["tls"].as_bool().unwrap_or(true);
    let allow_non_loopback = params["allow_non_loopback"].as_bool().unwrap_or(false);
    let allow: Vec<String> = params["allow"]
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();
    let routes: Vec<truffle_core::proxy::ProxyRoute> = match params.get("routes") {
        Some(v) if !v.is_null() => {
            match serde_json::from_value::<Vec<ProxyRouteParams>>(v.clone()) {
                Ok(rs) => rs.into_iter().map(Into::into).collect(),
                Err(e) => {
                    return DaemonResponse::error(
                        id,
                        error_code::INVALID_PARAMS,
                        format!("Invalid 'routes' parameter: {e}"),
                    )
                }
            }
        }
        _ => vec![],
    };

    if proxy_id.is_empty() {
        return DaemonResponse::error(id, error_code::INVALID_PARAMS, "Missing 'id' parameter");
    }
    if listen_port == 0 {
        return DaemonResponse::error(
            id,
            error_code::INVALID_PARAMS,
            "Missing or invalid 'listen_port' parameter",
        );
    }
    // A single-target (v1) proxy needs a real target port; a routes proxy
    // carries its backends in `routes` and ignores the `target` field.
    if routes.is_empty() && target_port == 0 {
        return DaemonResponse::error(
            id,
            error_code::INVALID_PARAMS,
            "Missing or invalid 'target_port' parameter",
        );
    }

    let config = truffle_core::proxy::ProxyConfig {
        id: proxy_id,
        name,
        listen_port,
        target: truffle_core::proxy::ProxyTarget {
            host: target_host,
            port: target_port,
            scheme: target_scheme,
        },
        announce,
        tls,
        allow_non_loopback,
        allow,
        routes,
    };

    match node.proxy().add(config).await {
        Ok(info) => {
            let status_str = match &info.status {
                truffle_core::proxy::ProxyStatus::Starting => "starting".to_string(),
                truffle_core::proxy::ProxyStatus::Running => "running".to_string(),
                truffle_core::proxy::ProxyStatus::Stopped => "stopped".to_string(),
                truffle_core::proxy::ProxyStatus::Error(ref msg) => format!("error: {msg}"),
            };
            DaemonResponse::success(
                id,
                serde_json::json!({
                    "id": info.id,
                    "name": info.name,
                    "listen_port": info.listen_port,
                    "target_host": info.target.host,
                    "target_port": info.target.port,
                    "target_scheme": info.target.scheme,
                    "url": info.url,
                    "status": status_str,
                }),
            )
        }
        Err(e) => DaemonResponse::error(id, error_code::INTERNAL_ERROR, e.to_string()),
    }
}

async fn handle_proxy_remove(
    id: u64,
    params: &serde_json::Value,
    node: &Arc<Node<TailscaleProvider>>,
) -> DaemonResponse {
    let proxy_id = match params["id"].as_str() {
        Some(p) => p,
        None => {
            return DaemonResponse::error(id, error_code::INVALID_PARAMS, "Missing 'id' parameter")
        }
    };

    match node.proxy().remove(proxy_id).await {
        Ok(()) => DaemonResponse::success(id, serde_json::json!({ "removed": true })),
        Err(e) => DaemonResponse::error(id, error_code::INTERNAL_ERROR, e.to_string()),
    }
}

async fn handle_proxy_list(id: u64, node: &Arc<Node<TailscaleProvider>>) -> DaemonResponse {
    let proxies = node.proxy().list();
    let proxies_json: Vec<serde_json::Value> = proxies
        .into_iter()
        .map(|info| {
            let status_str = match &info.status {
                truffle_core::proxy::ProxyStatus::Starting => "starting".to_string(),
                truffle_core::proxy::ProxyStatus::Running => "running".to_string(),
                truffle_core::proxy::ProxyStatus::Stopped => "stopped".to_string(),
                truffle_core::proxy::ProxyStatus::Error(ref msg) => format!("error: {msg}"),
            };
            serde_json::json!({
                "id": info.id,
                "name": info.name,
                "listen_port": info.listen_port,
                "target_host": info.target.host,
                "target_port": info.target.port,
                "target_scheme": info.target.scheme,
                "url": info.url,
                "status": status_str,
            })
        })
        .collect();

    DaemonResponse::success(id, serde_json::json!({ "proxies": proxies_json }))
}

// ==========================================================================
// Doctor
// ==========================================================================

async fn handle_doctor(id: u64, node: &Arc<Node<TailscaleProvider>>) -> DaemonResponse {
    let health = node.health().await;
    let peers = node.peers().await;
    let info = node.local_info();

    DaemonResponse::success(
        id,
        serde_json::json!({
            "checks": {
                "node_online": health.healthy,
                "tailscale_state": health.state,
                "peer_count": peers.len(),
                "key_expiry": health.key_expiry,
                "warnings": health.warnings,
                "device_id": info.device_id,
                "device_name": info.device_name,
                "tailscale_id": info.tailscale_id,
                "tailscale_hostname": info.tailscale_hostname,
            }
        }),
    )
}
