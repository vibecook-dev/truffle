//! Truffle Tauri v2 plugin.
//!
//! Provides a Tauri plugin that wraps `truffle-core`'s `Node<TailscaleProvider>`
//! API, exposing mesh networking, messaging, and file transfer to the Tauri
//! frontend via commands and events.
//!
//! # Usage
//!
//! ```text
//! fn main() {
//!     tauri::Builder::default()
//!         .plugin(truffle_tauri_plugin::init())
//!         .run(tauri::generate_context!())
//!         .expect("error while running tauri application");
//! }
//! ```
//!
//! # Commands
//!
//! - `start(config)` — Start the truffle node
//! - `stop()` — Stop the node
//! - `get_local_info()` — Get local node identity
//! - `get_peers()` — List all known peers
//! - `ping(peer_id)` — Ping a peer
//! - `health()` — Get network health info
//! - `send_message(peer_id, namespace, data)` — Send a message (legacy content-sniffing)
//! - `broadcast(namespace, data)` — Broadcast to all peers (legacy content-sniffing)
//! - `send_json(peer_id, namespace, payload)` — Send a JSON payload (explicit wire type)
//! - `send_bytes(peer_id, namespace, data)` — Send opaque bytes (explicit wire type)
//! - `broadcast_json(namespace, payload)` — Broadcast JSON, returns a queueing report
//! - `broadcast_bytes(namespace, data)` — Broadcast bytes, returns a queueing report
//! - `send_file(peer_id, local_path, remote_path)` — Send a file
//! - `pull_file(peer_id, remote_path, local_path)` — Download a file
//! - `auto_accept(output_dir)` — Auto-accept incoming file offers
//! - `accept_offer(token, save_path)` — Accept a pending file offer
//! - `reject_offer(token, reason)` — Reject a pending file offer
//! - `add_pull_root(root)` — Register a directory whose files may be pull-served
//! - `pull_roots()` — List registered pull roots
//! - `clear_pull_roots()` — Clear all registered pull roots
//!
//! # Raw Transport Commands (RFC 021 §7)
//!
//! Id-keyed parity with the NAPI raw surface — see [`raw_transport`]. Handles
//! live in [`TruffleState`] registries; commands take/return string ids.
//!
//! - TCP: `tcp_open`, `tcp_read`, `tcp_write`, `tcp_end`, `tcp_close`,
//!   `tcp_listen`, `tcp_accept`, `tcp_unlisten`
//! - UDP: `udp_bind`, `udp_send`, `udp_recv`, `udp_close`
//! - QUIC: `quic_connect`, `quic_open_stream`, `quic_accept_stream`,
//!   `quic_stream_read`, `quic_stream_write`, `quic_stream_finish`,
//!   `quic_stream_close`, `quic_close`, `quic_listen`, `quic_accept`,
//!   `quic_listener_close`
//!
//! # Events
//!
//! - `truffle://peer-event` — Peer state changes
//! - `truffle://file-transfer-event` — File transfer progress/completion
//! - `truffle://file-offer` — Incoming file offers
//! - `truffle://auth-required` — Authentication URL when login is needed
//!
//! @atlas layer bindings · domain bindings

use std::collections::HashMap;
use std::sync::Arc;

use tauri::plugin::{Builder, TauriPlugin};
use tauri::{Manager, Runtime};
use tokio::sync::{Mutex, RwLock};

use truffle_core::network::tailscale::TailscaleProvider;
use truffle_core::{Node, OfferResponder};

pub mod commands;
pub mod events;
pub mod raw_transport;
pub mod types;

use raw_transport::{
    QuicConnectionEntry, QuicListenerEntry, QuicStreamEntry, TcpListenerEntry, TcpSocketEntry,
    UdpSocketEntry,
};

/// Shared state managed by the Tauri plugin.
///
/// Holds the running `Node<TailscaleProvider>` and any pending file offer
/// responders that the frontend has not yet accepted or rejected.
pub struct TruffleState {
    /// The running truffle node, or `None` if not yet started.
    pub node: RwLock<Option<Arc<Node<TailscaleProvider>>>>,
    /// Pending file offer responders, keyed by transfer token.
    /// Removed on accept or reject.
    pub pending_offers: Arc<RwLock<HashMap<String, OfferResponder>>>,

    // ── Raw transport registries (RFC 021 §7, Phase 4) ───────────────────
    // Live socket/stream/listener handles cannot cross Tauri IPC, so each is
    // parked here keyed by a short string id; the `raw_transport` commands
    // take/return those ids. See `raw_transport.rs`.
    /// Raw TCP sockets, keyed by socket id.
    pub tcp_sockets: Mutex<HashMap<String, TcpSocketEntry>>,
    /// Raw TCP listeners, keyed by listener id.
    pub tcp_listeners: Mutex<HashMap<String, TcpListenerEntry>>,
    /// UDP datagram sockets, keyed by socket id.
    pub udp_sockets: Mutex<HashMap<String, UdpSocketEntry>>,
    /// QUIC connections, keyed by connection id.
    pub quic_connections: Mutex<HashMap<String, QuicConnectionEntry>>,
    /// QUIC streams, keyed by stream id (tagged with their connection id).
    pub quic_streams: Mutex<HashMap<String, QuicStreamEntry>>,
    /// QUIC listeners, keyed by listener id.
    pub quic_listeners: Mutex<HashMap<String, QuicListenerEntry>>,
}

/// Initialize the Truffle Tauri v2 plugin.
///
/// Register this in your Tauri app builder:
///
/// ```text
/// tauri::Builder::default()
///     .plugin(truffle_tauri_plugin::init())
/// ```
pub fn init<R: Runtime>() -> TauriPlugin<R> {
    Builder::new("truffle")
        .invoke_handler(tauri::generate_handler![
            commands::start,
            commands::stop,
            commands::get_local_info,
            commands::get_peers,
            commands::ping,
            commands::health,
            commands::send_message,
            commands::broadcast,
            commands::send_json,
            commands::send_bytes,
            commands::broadcast_json,
            commands::broadcast_bytes,
            commands::send_file,
            commands::pull_file,
            commands::auto_accept,
            commands::auto_reject,
            commands::accept_offer,
            commands::reject_offer,
            commands::add_pull_root,
            commands::pull_roots,
            commands::clear_pull_roots,
            commands::proxy_add,
            commands::proxy_remove,
            commands::proxy_list,
            // Raw transport (RFC 021 §7, Phase 4) — NAPI parity.
            raw_transport::tcp_open,
            raw_transport::tcp_read,
            raw_transport::tcp_write,
            raw_transport::tcp_end,
            raw_transport::tcp_close,
            raw_transport::tcp_listen,
            raw_transport::tcp_accept,
            raw_transport::tcp_unlisten,
            raw_transport::udp_bind,
            raw_transport::udp_send,
            raw_transport::udp_recv,
            raw_transport::udp_close,
            raw_transport::quic_connect,
            raw_transport::quic_open_stream,
            raw_transport::quic_accept_stream,
            raw_transport::quic_stream_read,
            raw_transport::quic_stream_write,
            raw_transport::quic_stream_finish,
            raw_transport::quic_stream_close,
            raw_transport::quic_close,
            raw_transport::quic_listen,
            raw_transport::quic_accept,
            raw_transport::quic_listener_close,
        ])
        .setup(|app, _| {
            app.manage(TruffleState {
                node: RwLock::new(None),
                pending_offers: Arc::new(RwLock::new(HashMap::new())),
                tcp_sockets: Mutex::new(HashMap::new()),
                tcp_listeners: Mutex::new(HashMap::new()),
                udp_sockets: Mutex::new(HashMap::new()),
                quic_connections: Mutex::new(HashMap::new()),
                quic_streams: Mutex::new(HashMap::new()),
                quic_listeners: Mutex::new(HashMap::new()),
            });
            Ok(())
        })
        .build()
}
