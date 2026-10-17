//! SyncedStore — device-owned state synchronization across the mesh.
//!
//! Each device owns exactly one slice of type `T` per store. Writes update
//! the local slice and broadcast the change. Remote slices are received and
//! cached automatically via a background sync task.
//!
//! # Quick start
//!
//! ```ignore
//! use std::sync::Arc;
//! use truffle_core::synced_store::SyncedStore;
//!
//! let node = Arc::new(node);
//! let store: Arc<SyncedStore<MyState>> = SyncedStore::new(node, "my-state");
//!
//! store.set(MyState { value: 42 }).await;
//! let my_data = store.local();
//! let all_data = store.all().await;
//! ```

pub mod backend;
pub mod sync;
pub mod types;

#[cfg(test)]
mod tests;

pub use backend::{FileBackend, MemoryBackend, StoreBackend};
pub use types::{Slice, StoreEvent, SyncMessage};

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::sync::{broadcast, watch, RwLock};
use tokio::task::JoinHandle;

use crate::network::NetworkProvider;
use crate::node::Node;

// ---------------------------------------------------------------------------
// StoreInner — shared state between public API and sync task
// ---------------------------------------------------------------------------

/// Internal shared state for a SyncedStore. Accessed by both the public API
/// and the background sync task.
pub(crate) struct StoreInner<T> {
    /// Store identifier (e.g., "sessions", "config").
    pub(crate) store_id: String,
    /// This device's stable node ID.
    pub(crate) device_id: String,
    /// Local slice (our data).
    pub(crate) local: RwLock<Option<Slice<T>>>,
    /// Remote peers' slices, keyed by device_id.
    pub(crate) remotes: RwLock<HashMap<String, Slice<T>>>,
    /// Current local version counter.
    pub(crate) version: AtomicU64,
    /// Event broadcast channel.
    pub(crate) event_tx: broadcast::Sender<StoreEvent<T>>,
    /// Persistence backend.
    pub(crate) backend: Arc<dyn StoreBackend>,
}

// ---------------------------------------------------------------------------
// SyncedStore — public API
// ---------------------------------------------------------------------------

/// A synchronized key-value store with device-owned slices.
///
/// Each device owns one slice of type `T`. Writes update only the local
/// slice and broadcast the change. Remote slices are received and cached
/// automatically.
///
/// Created via [`SyncedStore::new`] or [`Node::synced_store`].
pub struct SyncedStore<T> {
    pub(crate) inner: Arc<StoreInner<T>>,
    /// Latest pending outbound update for the sync task. A `watch` channel
    /// coalesces: the store has last-write-wins state semantics, so when
    /// `set()` outpaces the network only the newest version needs to go on
    /// the wire — intermediate versions carry no information and an
    /// unbounded queue of them would just grow memory.
    broadcast_tx: watch::Sender<Option<SyncMessage>>,
    /// Handle to the background sync task.
    task_handle: tokio::sync::Mutex<Option<JoinHandle<()>>>,
}

impl<T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static> SyncedStore<T> {
    /// Create a new SyncedStore and start the background sync task.
    ///
    /// Uses the default [`MemoryBackend`] (no persistence). For durable
    /// storage across restarts, use [`new_with_backend`](Self::new_with_backend).
    ///
    /// The store syncs on namespace `"ss:{store_id}"`. The caller must hold
    /// an `Arc<Node<N>>` because the background task needs to outlive this
    /// constructor call.
    pub fn new<N: NetworkProvider + 'static>(node: Arc<Node<N>>, store_id: &str) -> Arc<Self> {
        Self::new_with_backend(node, store_id, Arc::new(MemoryBackend))
    }

    /// Create a new SyncedStore with a custom persistence backend.
    ///
    /// On startup, attempts to load the local device's persisted slice. If
    /// found, the in-memory state and version counter are restored so the
    /// store picks up where it left off.
    pub fn new_with_backend<N: NetworkProvider + 'static>(
        node: Arc<Node<N>>,
        store_id: &str,
        backend: Arc<dyn StoreBackend>,
    ) -> Arc<Self> {
        // RFC 017 §8: synced store slices are keyed by the stable
        // `device_id` (ULID), which is propagated between peers via the
        // WebSocket hello handshake.
        let device_id = node.local_info().device_id;
        let (event_tx, _) = broadcast::channel(256);
        let (broadcast_tx, broadcast_rx) = watch::channel(None);

        // Attempt to restore persisted local slice.
        let (local, initial_version) = match backend.load(store_id, &device_id) {
            Some((data, version)) => match serde_json::from_slice::<T>(&data) {
                Ok(typed) => {
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64;
                    let slice = Slice {
                        device_id: device_id.clone(),
                        data: typed,
                        version,
                        updated_at: now,
                    };
                    tracing::info!(
                        store = store_id,
                        version = version,
                        "synced_store: restored local slice from backend"
                    );
                    (Some(slice), version)
                }
                Err(e) => {
                    tracing::warn!(
                        store = store_id,
                        "synced_store: failed to deserialize persisted data, starting fresh: {e}"
                    );
                    (None, 0)
                }
            },
            None => (None, 0),
        };

        let inner = Arc::new(StoreInner {
            store_id: store_id.to_string(),
            device_id,
            local: RwLock::new(local),
            remotes: RwLock::new(HashMap::new()),
            version: AtomicU64::new(initial_version),
            event_tx,
            backend,
        });

        let task_handle = sync::spawn_sync_task(node, inner.clone(), broadcast_rx);

        Arc::new(Self {
            inner,
            broadcast_tx,
            task_handle: tokio::sync::Mutex::new(Some(task_handle)),
        })
    }

    // ── Write ───────────────────────────────────────────────────────

    // @atlas step store-update.set
    /// Update this device's data. Increments version and broadcasts.
    ///
    /// Rapid successive calls coalesce on the wire: peers are guaranteed to
    /// observe the newest version, not every intermediate one (the store is
    /// last-write-wins state, not an event log).
    pub async fn set(&self, data: T) {
        let version = self.inner.version.fetch_add(1, Ordering::SeqCst) + 1;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let slice = Slice {
            device_id: self.inner.device_id.clone(),
            data: data.clone(),
            version,
            updated_at: now,
        };

        {
            let mut local = self.inner.local.write().await;
            *local = Some(slice);
        }

        // Persist to backend.
        if let Ok(serialized) = serde_json::to_vec(&data) {
            self.inner.backend.save(
                &self.inner.store_id,
                &self.inner.device_id,
                &serialized,
                version,
            );
        }

        // Request the sync task to broadcast. `send_replace` drops any
        // not-yet-sent older version — only the newest matters.
        if let Ok(serialized) = serde_json::to_value(&data) {
            let msg = SyncMessage::Update {
                device_id: self.inner.device_id.clone(),
                data: serialized,
                version,
                updated_at: now,
            };
            self.broadcast_tx.send_replace(Some(msg));
        }

        let _ = self.inner.event_tx.send(StoreEvent::LocalChanged(data));
    }

    // ── Read ────────────────────────────────────────────────────────

    /// This device's current data, or `None` if `set()` hasn't been called.
    pub async fn local(&self) -> Option<T> {
        let local = self.inner.local.read().await;
        local.as_ref().map(|s| s.data.clone())
    }

    /// A specific peer's slice, or `None` if we haven't received their data.
    pub async fn get(&self, device_id: &str) -> Option<Slice<T>> {
        let remotes = self.inner.remotes.read().await;
        remotes.get(device_id).cloned()
    }

    /// All slices (local + remote).
    pub async fn all(&self) -> HashMap<String, Slice<T>> {
        let mut result = HashMap::new();

        {
            let local = self.inner.local.read().await;
            if let Some(slice) = local.as_ref() {
                result.insert(slice.device_id.clone(), slice.clone());
            }
        }

        {
            let remotes = self.inner.remotes.read().await;
            for (id, slice) in remotes.iter() {
                result.insert(id.clone(), slice.clone());
            }
        }

        result
    }

    /// Device IDs that have data in this store (local + remote).
    pub async fn device_ids(&self) -> Vec<String> {
        let mut ids = Vec::new();

        {
            let local = self.inner.local.read().await;
            if let Some(slice) = local.as_ref() {
                ids.push(slice.device_id.clone());
            }
        }

        {
            let remotes = self.inner.remotes.read().await;
            ids.extend(remotes.keys().cloned());
        }

        ids
    }

    /// Subscribe to store change events.
    pub fn subscribe(&self) -> broadcast::Receiver<StoreEvent<T>> {
        self.inner.event_tx.subscribe()
    }

    /// The store identifier.
    pub fn store_id(&self) -> &str {
        &self.inner.store_id
    }

    /// This device's ID.
    pub fn device_id(&self) -> &str {
        &self.inner.device_id
    }

    /// Current local version number.
    pub fn version(&self) -> u64 {
        self.inner.version.load(Ordering::SeqCst)
    }

    // ── Lifecycle ───────────────────────────────────────────────────

    /// Stop the background sync task.
    pub async fn stop(&self) {
        let mut handle = self.task_handle.lock().await;
        if let Some(h) = handle.take() {
            h.abort();
            tracing::info!(
                store = self.inner.store_id.as_str(),
                "synced_store: stopped"
            );
        }
    }
}

impl<T> Drop for SyncedStore<T> {
    fn drop(&mut self) {
        // Best-effort abort on drop.
        if let Ok(mut handle) = self.task_handle.try_lock() {
            if let Some(h) = handle.take() {
                h.abort();
            }
        }
    }
}
