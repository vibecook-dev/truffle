//! Types for the SyncedStore subsystem.

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Slice — a versioned device-owned data unit
// ---------------------------------------------------------------------------

/// A versioned slice of data owned by a single device.
///
/// Each device owns exactly one slice per store. Writes only touch the local
/// slice; remote slices are received via the sync protocol.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Slice<T> {
    /// Device that owns this slice (stable node ID).
    pub device_id: String,
    /// The data.
    pub data: T,
    /// Monotonically increasing version (per-device).
    pub version: u64,
    /// When this version was created (Unix milliseconds).
    pub updated_at: u64,
}

// ---------------------------------------------------------------------------
// SyncMessage — wire protocol for store synchronization
// ---------------------------------------------------------------------------

// @atlas contract sync-wire "SyncedStore wire messages on namespace \"ss:{id}\"" {
//   produced-by crate:truffle-core each-variant
//   see "docs/rfcs/016-synced-store-and-request-reply.md#wire-protocol"
//   see "docs/rfcs/025-login-scoped-nodes.md#35-sender-bound-slices-tf-2"
// }
/// Wire protocol messages sent on namespace `"ss:{store_id}"`.
///
/// Uses `serde_json::Value` for the data field to keep this type non-generic.
/// Deserialization to `T` happens inside `SyncedStore<T>`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum SyncMessage {
    /// Broadcast when local data changes.
    #[serde(rename = "update")]
    Update {
        device_id: String,
        data: serde_json::Value,
        version: u64,
        updated_at: u64,
    },

    /// Targeted response to a `Request`, or sent on peer join.
    #[serde(rename = "full")]
    Full {
        device_id: String,
        data: serde_json::Value,
        version: u64,
        updated_at: u64,
    },

    /// Request a peer's current slice.
    #[serde(rename = "request")]
    Request {},

    /// Informational: a peer went offline, its data should be removed.
    #[serde(rename = "clear")]
    Clear { device_id: String },
}

// ---------------------------------------------------------------------------
// StoreEvent — change events emitted to subscribers
// ---------------------------------------------------------------------------

/// Events emitted by a [`SyncedStore`](super::SyncedStore).
#[derive(Debug, Clone)]
pub enum StoreEvent<T> {
    /// Local data was updated via `set()`.
    LocalChanged(T),

    /// A remote peer's data was received or updated.
    PeerUpdated {
        device_id: String,
        data: T,
        version: u64,
    },

    /// A remote peer's data was removed (peer went offline).
    PeerRemoved { device_id: String },
}
