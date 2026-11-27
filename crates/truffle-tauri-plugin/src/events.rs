//! Event forwarding from truffle-core to the Tauri frontend.
//!
//! Spawns background tasks that subscribe to core event channels and emit
//! them as Tauri events that the frontend can listen to via `listen()`.

use std::collections::HashMap;
use std::sync::Arc;

use tauri::{AppHandle, Emitter, Runtime};
use tokio::sync::RwLock;

use truffle_core::network::tailscale::TailscaleProvider;
use truffle_core::Node;

use crate::types::{FileOfferJs, FileTransferEventJs, PeerEventJs, ProxyEventJs};

/// Spawn background tasks that forward truffle-core events to the Tauri frontend.
///
/// This starts three forwarding tasks:
/// 1. Peer events (joined, left, ws_connected, ws_disconnected, auth required)
/// 2. File transfer events (progress, completed, failed, etc.)
/// 3. File offer channel (stores OfferResponders for accept/reject commands)
pub fn start_event_forwarding<R: Runtime>(
    app: &AppHandle<R>,
    node: &Arc<Node<TailscaleProvider>>,
    pending_offers: &Arc<RwLock<HashMap<String, truffle_core::OfferResponder>>>,
) {
    // Forward PeerEvents
    let mut peer_rx = node.on_peer_change();
    let app_clone = app.clone();
    tokio::spawn(async move {
        loop {
            match peer_rx.recv().await {
                Ok(event) => {
                    let js_event: PeerEventJs = event.into();
                    if let Err(e) = app_clone.emit("truffle://peer-event", &js_event) {
                        tracing::warn!("Failed to emit peer event: {e}");
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!("Peer event listener lagged, missed {n} events");
                    continue;
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    tracing::debug!("Peer event channel closed");
                    break;
                }
            }
        }
    });

    // Forward FileTransferEvents
    let ft = node.file_transfer();
    let mut ft_rx = ft.subscribe();
    let app_clone = app.clone();
    tokio::spawn(async move {
        loop {
            match ft_rx.recv().await {
                Ok(event) => {
                    let js_event: FileTransferEventJs = event.into();
                    if let Err(e) = app_clone.emit("truffle://file-transfer-event", &js_event) {
                        tracing::warn!("Failed to emit file transfer event: {e}");
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!("File transfer event listener lagged, missed {n} events");
                    continue;
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    tracing::debug!("File transfer event channel closed");
                    break;
                }
            }
        }
    });

    let mut grants = node.funnel().subscribe();
    let funnel_app = app.clone();
    tokio::spawn(async move {
        loop {
            match grants.recv().await {
                Ok(status) => {
                    let _ = funnel_app.emit("truffle://funnel-changed", status);
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    });

    // Forward ProxyEvents
    let mut proxy_rx = node.proxy().subscribe();
    let proxy_app = app.clone();
    tokio::spawn(async move {
        loop {
            match proxy_rx.recv().await {
                Ok(event) => {
                    let js_event: ProxyEventJs = event.into();
                    if let Err(e) = proxy_app.emit("truffle://proxy-event", &js_event) {
                        tracing::warn!("Failed to emit proxy event: {e}");
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!("Proxy event listener lagged, missed {n} events");
                    continue;
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    tracing::debug!("Proxy event channel closed");
                    break;
                }
            }
        }
    });

    // Forward file offers — store OfferResponders so the frontend can accept/reject
    let node_clone = node.clone();
    let pending = pending_offers.clone();
    let app_clone = app.clone();
    tokio::spawn(async move {
        let ft = node_clone.file_transfer();
        let mut offer_rx = ft.offer_channel(node_clone.clone()).await;
        while let Some((offer, responder)) = offer_rx.recv().await {
            let token = offer.token.clone();
            let js_offer: FileOfferJs = offer.into();

            // Store the responder so it can be used by accept_offer/reject_offer commands
            pending.write().await.insert(token, responder);

            // Emit the offer to the frontend
            if let Err(e) = app_clone.emit("truffle://file-offer", &js_offer) {
                tracing::warn!("Failed to emit file offer: {e}");
            }
        }
    });
}
