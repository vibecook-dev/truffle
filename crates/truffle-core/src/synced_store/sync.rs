//! Background sync task for SyncedStore.
//!
//! Handles three event sources via `tokio::select!`:
//! 1. Incoming sync messages from peers (namespace `"ss:{store_id}"`)
//! 2. Peer join/leave events (for sync-on-join and cleanup-on-leave)
//! 3. Outbound broadcast requests from `SyncedStore::set()`

use std::sync::Arc;

use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::sync::{broadcast, watch};

use crate::network::NetworkProvider;
use crate::node::Node;
use crate::session::PeerEvent;

use super::types::{Slice, StoreEvent, SyncMessage};
use super::StoreInner;

/// Spawn the background sync task.
///
/// Returns a `JoinHandle` that can be aborted to stop the task.
pub(super) fn spawn_sync_task<N, T>(
    node: Arc<Node<N>>,
    inner: Arc<StoreInner<T>>,
    mut broadcast_rx: watch::Receiver<Option<SyncMessage>>,
) -> tokio::task::JoinHandle<()>
where
    N: NetworkProvider + 'static,
    T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    let namespace = format!("ss:{}", inner.store_id);

    tokio::spawn(async move {
        let mut msg_rx = node.subscribe(&namespace);
        let mut peer_rx = node.on_peer_change();

        tracing::info!(
            store = inner.store_id.as_str(),
            "synced_store: sync task started"
        );

        loop {
            tokio::select! {
                // ── Source 1: incoming sync messages from peers ──
                result = msg_rx.recv() => {
                    match result {
                        Ok(msg) => {
                            handle_incoming_message(&node, &inner, &namespace, &msg.from, msg.payload).await;
                        }
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            tracing::warn!(
                                store = inner.store_id.as_str(),
                                missed = n,
                                "synced_store: message receiver lagged"
                            );
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            tracing::debug!(store = inner.store_id.as_str(), "synced_store: message channel closed");
                            break;
                        }
                    }
                }

                // ── Source 2: peer join/leave events ──
                result = peer_rx.recv() => {
                    match result {
                        Ok(PeerEvent::Joined(state)) => {
                            handle_peer_joined(&node, &inner, &namespace, &state.id).await;
                        }
                        Ok(PeerEvent::Left(state)) => {
                            // Slices are keyed by the departed peer's PUBLISHED
                            // device id, which the event's final state carries
                            // (RFC 022 §16.4) — never by its Tailscale id.
                            handle_peer_left(&inner, &state.id, state.published_device_id()).await;
                        }
                        Ok(_) => {} // Updated, WsConnected, WsDisconnected, AuthRequired — ignore
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            tracing::warn!(
                                store = inner.store_id.as_str(),
                                missed = n,
                                "synced_store: peer event receiver lagged"
                            );
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            tracing::debug!(store = inner.store_id.as_str(), "synced_store: peer event channel closed");
                            break;
                        }
                    }
                }

                // ── Source 3: outbound broadcast requests from set() ──
                // The watch channel coalesces: if set() ran several times
                // while a broadcast was in flight, only the newest pending
                // update is observed here — intermediate versions are
                // superseded and never hit the wire.
                result = broadcast_rx.changed() => {
                    match result {
                        Ok(()) => {
                            let pending = broadcast_rx.borrow_and_update().clone();
                            if let Some(sync_msg) = pending {
                                let msg_type = match &sync_msg {
                                    SyncMessage::Update { .. } => "update",
                                    SyncMessage::Full { .. } => "full",
                                    SyncMessage::Request { .. } => "request",
                                    SyncMessage::Clear { .. } => "clear",
                                };
                                if let Ok(payload) = serde_json::to_value(&sync_msg) {
                                    node.broadcast_typed(&namespace, msg_type, &payload).await;
                                }
                            }
                        }
                        Err(_) => {
                            // The store (sender side) was dropped — no more
                            // outbound updates will ever arrive.
                            tracing::debug!(
                                store = inner.store_id.as_str(),
                                "synced_store: broadcast channel closed"
                            );
                            break;
                        }
                    }
                }
            }
        }

        tracing::info!(
            store = inner.store_id.as_str(),
            "synced_store: sync task stopped"
        );
    })
}

/// Handle an incoming sync message from a peer.
///
/// `from` is the sender's WhoIs-verified Tailscale id (the session's
/// routing key). A slice or a `Clear` is honoured only under the SENDER'S
/// published device id (RFC 025 §3.5): the `device_id` a payload names is
/// a claim, and a claim that does not match the authenticated sender is
/// dropped — a peer can neither write another device's slice nor clear it.
pub(super) async fn handle_incoming_message<N, T>(
    node: &Node<N>,
    inner: &StoreInner<T>,
    namespace: &str,
    from: &str,
    payload: serde_json::Value,
) where
    N: NetworkProvider + 'static,
    T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    let sync_msg: SyncMessage = match serde_json::from_value(payload) {
        Ok(m) => m,
        Err(e) => {
            tracing::warn!(
                store = inner.store_id.as_str(),
                from = from,
                "synced_store: failed to parse sync message: {e}"
            );
            return;
        }
    };

    match sync_msg {
        SyncMessage::Update {
            device_id,
            data,
            version,
            updated_at,
        }
        | SyncMessage::Full {
            device_id,
            data,
            version,
            updated_at,
        } => {
            if !sender_owns(node, inner, from, &device_id, "slice").await {
                return;
            }
            apply_remote_slice(inner, &device_id, data, version, updated_at).await;
        }

        SyncMessage::Request {} => {
            // Peer is requesting our current slice.
            let local = inner.local.read().await;
            if let Some(slice) = local.as_ref() {
                let full = SyncMessage::Full {
                    device_id: inner.device_id.clone(),
                    data: match serde_json::to_value(&slice.data) {
                        Ok(v) => v,
                        Err(e) => {
                            tracing::error!(
                                store = inner.store_id.as_str(),
                                "synced_store: failed to serialize local data: {e}"
                            );
                            return;
                        }
                    },
                    version: slice.version,
                    updated_at: slice.updated_at,
                };
                if let Ok(payload) = serde_json::to_value(&full) {
                    if let Err(e) = node.send_typed(from, namespace, "full", &payload).await {
                        tracing::warn!(
                            store = inner.store_id.as_str(),
                            peer = from,
                            "synced_store: failed to send Full response: {e}"
                        );
                    }
                }
            }
        }

        SyncMessage::Clear { device_id } => {
            if !sender_owns(node, inner, from, &device_id, "clear").await {
                return;
            }
            let mut remotes = inner.remotes.write().await;
            if remotes.remove(&device_id).is_some() {
                let _ = inner.event_tx.send(StoreEvent::PeerRemoved {
                    device_id: device_id.clone(),
                });
                tracing::debug!(
                    store = inner.store_id.as_str(),
                    device = device_id.as_str(),
                    "synced_store: cleared remote slice (Clear message)"
                );
            }
        }
    }
}

/// Bind a payload's `device_id` to the sender (RFC 025 §3.5): true only when
/// the session has a PUBLISHED device id for the sender's Tailscale id and
/// it equals the id the payload names. A sender with no published identity
/// (no hello yet, or suppressed under RFC 022 first-wins) owns nothing.
async fn sender_owns<N, T>(
    node: &Node<N>,
    inner: &StoreInner<T>,
    from: &str,
    claimed_device_id: &str,
    what: &str,
) -> bool
where
    N: NetworkProvider + 'static,
{
    match node.peer_device_id(from).await {
        Some(published) if published == claimed_device_id => true,
        Some(published) => {
            tracing::warn!(
                store = inner.store_id.as_str(),
                from = from,
                sender_device = published.as_str(),
                claimed_device = claimed_device_id,
                "synced_store: {what} names a device the sender is not; dropped"
            );
            false
        }
        None => {
            tracing::warn!(
                store = inner.store_id.as_str(),
                from = from,
                claimed_device = claimed_device_id,
                "synced_store: {what} from a sender with no published identity; dropped"
            );
            false
        }
    }
}

/// Apply a remote slice if the version is newer.
async fn apply_remote_slice<T>(
    inner: &StoreInner<T>,
    device_id: &str,
    data: serde_json::Value,
    version: u64,
    updated_at: u64,
) where
    T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    // Don't apply our own data back.
    if device_id == inner.device_id {
        return;
    }

    let typed_data: T = match serde_json::from_value(data) {
        Ok(d) => d,
        Err(e) => {
            tracing::warn!(
                store = inner.store_id.as_str(),
                device = device_id,
                "synced_store: failed to deserialize remote data: {e}"
            );
            return;
        }
    };

    let mut remotes = inner.remotes.write().await;

    // Check version — only accept if newer.
    if let Some(existing) = remotes.get(device_id) {
        if version <= existing.version {
            tracing::debug!(
                store = inner.store_id.as_str(),
                device = device_id,
                existing_v = existing.version,
                incoming_v = version,
                "synced_store: stale update rejected"
            );
            return;
        }
    }

    let slice = Slice {
        device_id: device_id.to_string(),
        data: typed_data.clone(),
        version,
        updated_at,
    };

    remotes.insert(device_id.to_string(), slice);

    // Persist remote slice to backend.
    if let Ok(serialized) = serde_json::to_vec(&typed_data) {
        inner
            .backend
            .save(&inner.store_id, device_id, &serialized, version);
    }

    let _ = inner.event_tx.send(StoreEvent::PeerUpdated {
        device_id: device_id.to_string(),
        data: typed_data,
        version,
    });

    tracing::debug!(
        store = inner.store_id.as_str(),
        device = device_id,
        version = version,
        "synced_store: applied remote slice"
    );
}

/// Handle a peer joining: send them a Request for their data.
async fn handle_peer_joined<N, T>(
    node: &Node<N>,
    inner: &StoreInner<T>,
    namespace: &str,
    peer_id: &str,
) where
    N: NetworkProvider + 'static,
    T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    // Send Request to the new peer.
    let request = SyncMessage::Request {};
    if let Ok(payload) = serde_json::to_value(&request) {
        if let Err(e) = node
            .send_typed(peer_id, namespace, "request", &payload)
            .await
        {
            tracing::warn!(
                store = inner.store_id.as_str(),
                peer = peer_id,
                "synced_store: failed to send Request to new peer: {e}"
            );
        }
    }

    // Also send our current data to the new peer (proactive full sync).
    let local = inner.local.read().await;
    if let Some(slice) = local.as_ref() {
        let full = SyncMessage::Full {
            device_id: inner.device_id.clone(),
            data: match serde_json::to_value(&slice.data) {
                Ok(v) => v,
                Err(_) => return,
            },
            version: slice.version,
            updated_at: slice.updated_at,
        };
        if let Ok(payload) = serde_json::to_value(&full) {
            let _ = node.send_typed(peer_id, namespace, "full", &payload).await;
        }
    }
}

/// Handle a peer leaving: remove its slice (keyed by its PUBLISHED device
/// id, never its Tailscale id — RFC 022 I1 keeps the two distinct) and emit
/// `PeerRemoved`. A peer that never published an identity owns no slice:
/// nothing to remove.
async fn handle_peer_left<T>(inner: &StoreInner<T>, peer_id: &str, device_id: Option<&str>)
where
    T: Clone + Send + Sync + 'static,
{
    let Some(device_id) = device_id else {
        tracing::debug!(
            store = inner.store_id.as_str(),
            peer = peer_id,
            "synced_store: departed peer had no published device id; no slice to remove"
        );
        return;
    };
    let mut remotes = inner.remotes.write().await;
    if remotes.remove(device_id).is_some() {
        // Remove persisted slice for departed peer.
        inner.backend.remove(&inner.store_id, device_id);

        let _ = inner.event_tx.send(StoreEvent::PeerRemoved {
            device_id: device_id.to_string(),
        });
        tracing::debug!(
            store = inner.store_id.as_str(),
            peer = peer_id,
            device = device_id,
            "synced_store: removed peer slice (peer left)"
        );
    }
}
