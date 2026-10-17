//! Persistence backends for SyncedStore.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

// @atlas contract store-backend "SyncedStore persistence seam" {
//   implemented-by crate:truffle-core
//   see "docs/rfcs/016-synced-store-and-request-reply.md#persistence-optional"
// }
/// Backend for persisting store data across restarts.
///
/// Methods are synchronous — file I/O for small JSON payloads is fast enough
/// that async overhead isn't warranted. Apps needing async persistence (e.g.,
/// database-backed) can spawn a blocking task internally.
pub trait StoreBackend: Send + Sync + 'static {
    /// Load a device's slice. Returns `(serialized_data, version)` or `None`.
    fn load(&self, store_id: &str, device_id: &str) -> Option<(Vec<u8>, u64)>;

    /// Save a device's slice.
    fn save(&self, store_id: &str, device_id: &str, data: &[u8], version: u64);

    /// Remove a device's slice.
    fn remove(&self, store_id: &str, device_id: &str);
}

/// In-memory backend (no persistence). Default.
///
/// Data lives only in the `SyncedStore`'s in-memory state and is lost on
/// process exit. Suitable for ephemeral stores like presence or typing
/// indicators.
#[derive(Debug, Default)]
pub struct MemoryBackend;

impl StoreBackend for MemoryBackend {
    fn load(&self, _store_id: &str, _device_id: &str) -> Option<(Vec<u8>, u64)> {
        None
    }

    fn save(&self, _store_id: &str, _device_id: &str, _data: &[u8], _version: u64) {}

    fn remove(&self, _store_id: &str, _device_id: &str) {}
}

// ---------------------------------------------------------------------------
// FileBackend — JSON file persistence
// ---------------------------------------------------------------------------

/// On-disk JSON file for a single device slice.
#[derive(Serialize, Deserialize)]
struct StoredSlice {
    data: Vec<u8>,
    version: u64,
}

/// File-backed persistence. Each device slice is stored as a JSON file at
/// `{base_dir}/{store_id}/{device_id}.json`.
///
/// Writes are atomic (write to `.tmp`, then rename) so a crash mid-write
/// never corrupts the persisted data.
pub struct FileBackend {
    base_dir: PathBuf,
}

impl FileBackend {
    /// Create a new `FileBackend` rooted at `base_dir`.
    pub fn new(base_dir: impl Into<PathBuf>) -> Self {
        Self {
            base_dir: base_dir.into(),
        }
    }

    /// Sanitized path: `{base_dir}/{store_id}/{device_id}.json`.
    ///
    /// Replaces `/`, `\`, and `..` segments with `_` to prevent path traversal.
    fn path(&self, store_id: &str, device_id: &str) -> PathBuf {
        let safe_store = sanitize(store_id);
        let safe_device = sanitize(device_id);
        self.base_dir
            .join(safe_store)
            .join(format!("{safe_device}.json"))
    }
}

/// Replace characters that could cause path traversal or filesystem issues.
fn sanitize(s: &str) -> String {
    s.replace(['/', '\\'], "_").replace("..", "_")
}

impl StoreBackend for FileBackend {
    fn load(&self, store_id: &str, device_id: &str) -> Option<(Vec<u8>, u64)> {
        let path = self.path(store_id, device_id);
        let contents = std::fs::read_to_string(&path).ok()?;
        let stored: StoredSlice = serde_json::from_str(&contents).ok()?;
        Some((stored.data, stored.version))
    }

    fn save(&self, store_id: &str, device_id: &str, data: &[u8], version: u64) {
        let path = self.path(store_id, device_id);

        // Ensure parent directory exists.
        if let Some(parent) = path.parent() {
            if let Err(e) = std::fs::create_dir_all(parent) {
                tracing::error!(
                    path = %parent.display(),
                    "file_backend: failed to create directory: {e}"
                );
                return;
            }
        }

        let stored = StoredSlice {
            data: data.to_vec(),
            version,
        };

        let json = match serde_json::to_string(&stored) {
            Ok(j) => j,
            Err(e) => {
                tracing::error!("file_backend: failed to serialize: {e}");
                return;
            }
        };

        // Atomic write: write to .tmp then rename.
        let tmp_path = path.with_extension("json.tmp");
        if let Err(e) = std::fs::write(&tmp_path, json.as_bytes()) {
            tracing::error!(
                path = %tmp_path.display(),
                "file_backend: failed to write tmp file: {e}"
            );
            return;
        }
        if let Err(e) = std::fs::rename(&tmp_path, &path) {
            tracing::error!(
                from = %tmp_path.display(),
                to = %path.display(),
                "file_backend: failed to rename tmp file: {e}"
            );
        }
    }

    fn remove(&self, store_id: &str, device_id: &str) {
        let path = self.path(store_id, device_id);
        let _ = std::fs::remove_file(&path);
    }
}
