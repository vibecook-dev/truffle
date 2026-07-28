//! JS-facing types for the NAPI bindings.
//!
//! All structs here use `#[napi(object)]` so they are passed as plain JS
//! objects (not class instances). They mirror the public types from
//! truffle-core but flatten/simplify for JS ergonomics.
//!
//! RFC 017 identity model: application code sees peers by `device_id`
//! (the remote node's ULID) and `device_name` (human-readable Unicode).
//!
//! IMPORTANT as-built caveat (v0.5.x): a peer's `device_id` / `device_name`
//! only carry the ULID / real name once that peer's hello has arrived over the
//! envelope-bus WebSocket (i.e. once `ws_connected == true`). Before then — and
//! permanently for raw-transport-only apps (QUIC/TCP/UDP) that never open the
//! bus — they fall back to the peer's Tailscale stable node id / hostname. A
//! peer's `device_id` therefore CHANGES from the Tailscale id to the ULID the
//! moment the WS session connects; it is not stable within a session. Only the
//! *local* node's `device_id` (see [`NapiNodeIdentity`]) is always the real
//! ULID. The Tailscale stable id and hostname are also available as explicit
//! escape hatches for diagnostics. See `NapiPeer` for the full contract.

use napi_derive::napi;

// ---------------------------------------------------------------------------
// Node configuration
// ---------------------------------------------------------------------------

/// Configuration for starting a Node (RFC 017 §7 shape).
#[napi(object)]
pub struct NapiNodeConfig {
    /// Application namespace identifier. Required. Must match
    /// `^[a-z][a-z0-9-]{1,31}$`; validated on the Rust side.
    pub app_id: String,
    /// Optional human-readable device name. Defaults to the OS hostname.
    /// May contain any Unicode characters; the Tailscale hostname is
    /// derived automatically.
    pub device_name: Option<String>,
    /// Optional stable ULID override. Auto-generated (and persisted) when
    /// omitted.
    pub device_id: Option<String>,
    /// Optional explicit Tailscale hostname (RFC 023 §6.4), bypassing the
    /// `truffle-{appId}-{slug}` convention for pretty serving URLs. Must be
    /// a single lowercase DNS label (1–63 chars of `[a-z0-9-]`, no dots).
    /// Tradeoff: hello-less peers with a custom hostname lose bare
    /// device-name resolution; read the granted name from
    /// `getLocalInfo().dnsName` (Tailscale dedupes collisions with `-1`/`-2`).
    pub hostname: Option<String>,
    /// Tailscale state directory. Defaults to
    /// `{userDataDir}/truffle/{app_id}/{slug(device_name)}`.
    pub state_dir: Option<String>,
    /// Path to the Go sidecar binary.
    pub sidecar_path: String,
    /// Tailscale auth key for headless authentication.
    pub auth_key: Option<String>,
    /// Whether the node is ephemeral (auto-removed from tailnet on shutdown).
    pub ephemeral: Option<bool>,
    /// WebSocket listen port. Defaults to 9417.
    pub ws_port: Option<u16>,
    /// RFC 022 Phase C: proactively exchange hello with online peers so
    /// durable deviceId is learned without app send. Defaults to true.
    pub eager_identity: Option<bool>,
}

// ---------------------------------------------------------------------------
// Node identity
// ---------------------------------------------------------------------------

/// Identity of the local node (RFC 017 §7.2).
#[napi(object)]
pub struct NapiNodeIdentity {
    /// Application namespace identifier.
    pub app_id: String,
    /// This node's own stable ULID (RFC 017 §5.4). Primary key for device
    /// identity. Always the real ULID — unlike a *peer's* `deviceId` (see
    /// `NapiPeer`), which falls back to a Tailscale id until that peer's
    /// hello is seen. Persists across restarts, including with
    /// `ephemeral: true`.
    pub device_id: String,
    /// Original (unsanitised) device name, as passed by the application.
    pub device_name: String,
    /// The Tailscale hostname (`truffle-{appId}-{slug}`). Debug use only.
    pub tailscale_hostname: String,
    /// This node's Tailscale stable node ID. Escape hatch for diagnostics;
    /// with `ephemeral: true` it rotates on each restart while `deviceId`
    /// persists.
    pub tailscale_id: String,
    /// DNS name on the tailnet, if available.
    pub dns_name: Option<String>,
    /// Tailscale IP address as a string, if available.
    pub ip: Option<String>,
}

// ---------------------------------------------------------------------------
// Peer
// ---------------------------------------------------------------------------

/// A peer as seen by application code (RFC 022 honest projection).
#[napi(object)]
pub struct NapiPeer {
    /// Durable ULID once known; `null` until identity is learned.
    /// Never equals `tailscaleId` and never silently falls back to it.
    pub device_id: Option<String>,
    /// Human-readable device name from the hello identity block, if known.
    pub device_name: Option<String>,
    /// Best label for UI (identity name → hostname slug → short id).
    pub display_name: String,
    /// Layer 3 Tailscale hostname (`truffle-{appId}-{slug}`).
    pub hostname: String,
    /// Network IP address as a string.
    pub ip: String,
    /// Whether the peer is online (from Layer 3).
    pub online: bool,
    /// Whether an envelope-bus WebSocket session is currently open.
    /// Advanced: do not gate `send` on this field.
    pub ws_connected: bool,
    /// Connection type description (e.g., "direct" or "relay:ord").
    pub connection_type: String,
    /// Operating system, if known.
    pub os: Option<String>,
    /// Last time the peer was seen online (RFC 3339 string).
    pub last_seen: Option<String>,
    /// Tailscale stable node id — routing key / diagnostics (advanced).
    pub tailscale_id: String,
    /// Process-local ref `{tailscaleId}:{generation}` (RFC 022).
    pub peer_ref: String,
    /// Registry entry generation (bumped on re-join).
    pub generation: u32,
}

// ---------------------------------------------------------------------------
// Ping result
// ---------------------------------------------------------------------------

/// Result of a network-level ping.
#[napi(object)]
pub struct NapiPingResult {
    /// Round-trip latency in milliseconds.
    pub latency_ms: f64,
    /// Connection type description (e.g., "direct" or "relay:sfo").
    pub connection: String,
    /// Direct peer endpoint address, if available.
    pub peer_addr: Option<String>,
}

// ---------------------------------------------------------------------------
// WhoIs identity
// ---------------------------------------------------------------------------

/// A tailnet WhoIs identity: the control plane's answer about who owns an
/// address. This is transport-derived — never a peer's claim about itself.
///
/// Every field is optional: tagged nodes carry node identity but no user
/// profile, and legacy sidecars may omit fields. A fully anonymous caller has
/// no identity at all (`whois` resolves `null` instead).
#[napi(object)]
#[derive(Clone)]
pub struct NapiPeerIdentity {
    /// MagicDNS name (e.g. "kitchen.tail1234.ts.net"), trailing dot stripped.
    pub dns_name: Option<String>,
    /// Tailscale login (owner) name, e.g. "alice@example.com".
    pub login_name: Option<String>,
    /// Human-readable display name from the identity provider.
    pub display_name: Option<String>,
    /// URL of the owner's profile picture.
    pub profile_pic_url: Option<String>,
    /// Stable Tailscale node id (WhoIs `Node.StableID`).
    pub node_id: Option<String>,
}

impl From<truffle_core::network::TailscalePeerIdentity> for NapiPeerIdentity {
    fn from(identity: truffle_core::network::TailscalePeerIdentity) -> Self {
        Self {
            dns_name: identity.dns_name,
            login_name: identity.login_name,
            display_name: identity.display_name,
            profile_pic_url: identity.profile_pic_url,
            node_id: identity.node_id,
        }
    }
}

// ---------------------------------------------------------------------------
// Health info
// ---------------------------------------------------------------------------

/// Health information from the network layer.
#[napi(object)]
pub struct NapiHealthInfo {
    /// Current backend state (e.g., "Running", "NeedsLogin").
    pub state: String,
    /// Key expiry timestamp (RFC 3339), if applicable.
    pub key_expiry: Option<String>,
    /// Active health warnings.
    pub warnings: Vec<String>,
    /// Whether the network is fully operational.
    pub healthy: bool,
}

// ---------------------------------------------------------------------------
// Peer event
// ---------------------------------------------------------------------------

/// A peer change event delivered to JS.
#[napi(object)]
pub struct NapiPeerEvent {
    /// Event type: "joined", "left", "updated", "identity", "ws_connected",
    /// "ws_disconnected", "auth_required".
    pub event_type: String,
    /// Routing key of the affected peer (Tailscale stable id), or empty for
    /// `auth_required`. Prefer `peer` when present (RFC 022).
    pub peer_id: String,
    /// Full peer info when available (joined/updated/identity; also preferred
    /// on ws_* when resolvable).
    pub peer: Option<NapiPeer>,
    /// Auth URL (present only for auth_required events).
    pub auth_url: Option<String>,
}

// ---------------------------------------------------------------------------
// Namespaced message
// ---------------------------------------------------------------------------

/// A message received on a specific namespace.
#[napi(object)]
pub struct NapiNamespacedMessage {
    /// Sender's WhoIs-verified **Tailscale stable id** — the connection's
    /// authenticated routing key (RFC 022 §7.5). NOT the durable ULID:
    /// compare against `peer.tailscaleId`, or use `createMeshNode`'s
    /// `onMessage`, whose `msg.from` is an interned `Peer` handle.
    pub from: String,
    /// Namespace the message was sent on.
    pub namespace: String,
    /// Application-defined message type.
    pub msg_type: String,
    /// Opaque JSON payload (as a JS value via serde).
    pub payload: serde_json::Value,
    /// Millisecond Unix timestamp from the sender, if set.
    pub timestamp: Option<f64>,
}

/// Outcome of a `broadcastJson` / `broadcastBytes` call.
///
/// "Queued" means handed to a peer's connection task — not confirmed
/// delivery. Broadcasts reach only currently connected peers.
#[napi(object)]
pub struct NapiBroadcastReport {
    /// Peers with an active WS connection at broadcast time.
    pub attempted: u32,
    /// Messages successfully queued to a connection task.
    pub queued: u32,
    /// Tailscale ids of peers whose connection task was already closed.
    pub failed: Vec<String>,
}

impl From<truffle_core::BroadcastReport> for NapiBroadcastReport {
    fn from(r: truffle_core::BroadcastReport) -> Self {
        Self {
            attempted: r.attempted as u32,
            queued: r.queued as u32,
            failed: r.failed,
        }
    }
}

// ---------------------------------------------------------------------------
// File transfer types
// ---------------------------------------------------------------------------

/// An incoming file offer from a remote peer.
#[napi(object)]
pub struct NapiFileOffer {
    /// Sender's WhoIs-verified **Tailscale stable id** (RFC 022 §7.5
    /// attribution). NOT the durable ULID: allowlists must compare against
    /// `peer.tailscaleId`, not `peer.deviceId`.
    pub from_peer: String,
    /// Human-readable device name of the sending peer.
    pub from_name: String,
    /// File name being offered.
    pub file_name: String,
    /// File size in bytes.
    pub size: f64,
    /// Expected SHA-256 hash (hex).
    pub sha256: String,
    /// Suggested save path from the sender.
    pub suggested_path: String,
    /// Unique token for this transfer.
    pub token: String,
}

/// Result of a completed file transfer.
#[napi(object)]
pub struct NapiTransferResult {
    /// Number of bytes transferred.
    pub bytes_transferred: f64,
    /// SHA-256 hash of the transferred file.
    pub sha256: String,
    /// Elapsed time in seconds.
    pub elapsed_secs: f64,
}

/// Progress update for an in-flight file transfer.
#[napi(object)]
pub struct NapiTransferProgress {
    /// Unique token for this transfer.
    pub token: String,
    /// Direction: "send" or "receive".
    pub direction: String,
    /// File name being transferred.
    pub file_name: String,
    /// Bytes transferred so far.
    pub bytes_transferred: f64,
    /// Total file size in bytes.
    pub total_bytes: f64,
    /// Current transfer speed in bytes per second.
    pub speed_bps: f64,
}

// ---------------------------------------------------------------------------
// Synced store types
// ---------------------------------------------------------------------------

/// A versioned slice of data owned by a single device.
#[napi(object)]
pub struct NapiSlice {
    /// Owning device's stable `device_id` (ULID) from the RFC 017 hello
    /// envelope.
    pub device_id: String,
    /// The data (JSON value).
    pub data: serde_json::Value,
    /// Monotonically increasing version (per-device).
    pub version: f64,
    /// When this version was created (Unix milliseconds).
    pub updated_at: f64,
}

/// A store change event delivered to JS.
#[napi(object)]
pub struct NapiStoreEvent {
    /// Event type: "local_changed", "peer_updated", "peer_removed".
    pub event_type: String,
    /// Stable `device_id` (ULID) of the affected device (present for
    /// peer_updated / peer_removed events).
    pub device_id: Option<String>,
    /// Data payload (present for local_changed/peer_updated events).
    pub data: Option<serde_json::Value>,
    /// Version number (present for peer_updated events).
    pub version: Option<f64>,
}

// ---------------------------------------------------------------------------
// File transfer types
// ---------------------------------------------------------------------------

/// Events emitted by the file transfer subsystem.
#[napi(object)]
pub struct NapiFileTransferEvent {
    /// Event type: "offer_received", "hashing", "waiting_for_accept",
    /// "progress", "completed", "rejected", "failed".
    pub event_type: String,
    /// Transfer token (if applicable).
    pub token: Option<String>,
    /// File name (if applicable).
    pub file_name: Option<String>,
    /// Direction: "send" or "receive" (if applicable).
    pub direction: Option<String>,
    /// Progress info (present for "progress" events).
    pub progress: Option<NapiTransferProgress>,
    /// Offer info (present for "offer_received" events).
    pub offer: Option<NapiFileOffer>,
    /// Bytes transferred (present for "completed" events).
    pub bytes_transferred: Option<f64>,
    /// SHA-256 hash (present for "completed" events).
    pub sha256: Option<String>,
    /// Elapsed seconds (present for "completed" events).
    pub elapsed_secs: Option<f64>,
    /// Reason for rejection or failure.
    pub reason: Option<String>,
    /// Bytes hashed so far (present for "hashing" events).
    pub bytes_hashed: Option<f64>,
    /// Total bytes to hash (present for "hashing" events).
    pub total_bytes: Option<f64>,
}
