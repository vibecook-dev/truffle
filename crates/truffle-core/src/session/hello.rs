//! Hello envelope — Layer 5 identity handshake (RFC 017 §8).
//!
//! When two truffle nodes establish a WebSocket link, each side sends a
//! [`HelloEnvelope`] as the very first message over the socket and expects
//! one back before any application traffic flows. The envelope advertises
//! the sender's `app_id`, stable `device_id`, human-readable `device_name`,
//! operating system, and (as an escape hatch) its Tailscale stable ID.
//!
//! # Version
//!
//! `HelloEnvelope::version` starts at `2`. Version 1 is implicit in
//! pre-RFC-017 code and is explicitly rejected — RFC 017 is a clean break
//! and older peers cannot talk to newer ones.
//!
//! # Deviation from RFC §8
//!
//! RFC 017 §8 lists four fields inside the `identity` block: `app_id`,
//! `device_id`, `device_name`, `os`. This implementation adds a fifth
//! field, `tailscale_id`, because the session layer still routes
//! WebSocket connections by Tailscale stable ID (the primary key of the
//! session's peer map). Carrying the remote's Tailscale ID in the hello
//! lets the session correlate the freshly-accepted WS link with the
//! Layer 3 peer entry that already exists. A future phase can remove
//! this field once the session layer re-keys its connection map by
//! `device_id`.

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// WebSocket close codes (RFC 017 §8)
// ---------------------------------------------------------------------------

/// Remote peer's `app_id` did not match ours. The link is closed immediately
/// with this code so the remote sees a specific reason.
pub const CLOSE_APP_MISMATCH: u16 = 4001;

/// Remote peer sent a malformed hello envelope, or no hello at all within the
/// timeout.
pub const CLOSE_HELLO_PROTOCOL: u16 = 4002;

/// The hello envelope claimed a `tailscale_id` that does not match the
/// Tailscale-authenticated identity (WhoIs) of the underlying connection —
/// or, on a node that admits only listed logins (RFC 025), the connection
/// carried no authenticated identity at all.
pub const CLOSE_IDENTITY_MISMATCH: u16 = 4003;

/// The caller's Tailscale-authenticated login is not on the accepting
/// node's login allow-list (RFC 025 §3.4). Sent before the accepting side
/// reveals its own hello.
pub const CLOSE_LOGIN_REFUSED: u16 = 4004;

/// Timeout applied to the first hello envelope read.
pub const HELLO_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

// ---------------------------------------------------------------------------
// HelloEnvelope
// ---------------------------------------------------------------------------

/// Identity block sent once in each direction when a WebSocket link comes up
/// (RFC 017 §8). Version 2 is the first with a structured `identity` field;
/// version 1 is implicit in pre-RFC-017 code.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HelloEnvelope {
    /// Wire tag discriminating this frame from other possible envelopes.
    pub kind: HelloKind,
    /// Version of the hello envelope schema. Must be >= 2.
    pub version: u32,
    /// The sender's identity block.
    pub identity: PeerIdentity,
}

/// Tag identifying a [`HelloEnvelope`] on the wire.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HelloKind {
    /// The sole variant — a hello envelope.
    Hello,
}

/// Peer identity metadata advertised by a truffle node in its hello envelope.
///
/// The primary key for peer identity is `device_id`, which is stable across
/// Tailscale re-auths and ephemeral-mode rotations. `tailscale_id` is an
/// escape hatch kept so the session layer can correlate the hello with its
/// Layer 3 peer entry — see the module-level note on the RFC §8 deviation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PeerIdentity {
    /// The application namespace. Peers with different `app_id` close the
    /// connection immediately (RFC §8, close code 4001).
    pub app_id: String,
    /// Stable per-device ULID.
    pub device_id: String,
    /// Human-readable device name (original form, NOT the slug).
    pub device_name: String,
    /// Operating system identifier (e.g., "darwin", "linux", "windows").
    pub os: String,
    /// Tailscale stable node ID — escape hatch used by the session layer to
    /// cross-reference the hello with its Layer 3 peer registry.
    #[serde(default)]
    pub tailscale_id: String,
}

impl HelloEnvelope {
    /// The hello envelope schema version this build emits.
    pub const CURRENT_VERSION: u32 = 2;
    /// The minimum hello envelope version this build will accept from a peer.
    pub const MIN_SUPPORTED_VERSION: u32 = 2;

    /// Construct a new hello envelope at [`Self::CURRENT_VERSION`] carrying
    /// `identity`.
    pub fn new(identity: PeerIdentity) -> Self {
        Self {
            kind: HelloKind::Hello,
            version: Self::CURRENT_VERSION,
            identity,
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hello_envelope_round_trips_json() {
        let hello = HelloEnvelope::new(PeerIdentity {
            app_id: "playground".into(),
            device_id: "01J4K9M2Z8AB3RNYQPW6H5TC0X".into(),
            device_name: "Alice's MacBook".into(),
            os: "darwin".into(),
            tailscale_id: "node-abc".into(),
        });

        let json = serde_json::to_string(&hello).unwrap();
        let parsed: HelloEnvelope = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.version, HelloEnvelope::CURRENT_VERSION);
        assert_eq!(parsed.identity.app_id, "playground");
        assert_eq!(parsed.identity.device_id, "01J4K9M2Z8AB3RNYQPW6H5TC0X");
        assert_eq!(parsed.identity.device_name, "Alice's MacBook");
        assert_eq!(parsed.identity.os, "darwin");
        assert_eq!(parsed.identity.tailscale_id, "node-abc");
    }

    #[test]
    fn hello_envelope_json_layout_matches_rfc_shape() {
        let hello = HelloEnvelope::new(PeerIdentity {
            app_id: "chat".into(),
            device_id: "01HZZZZZZZZZZZZZZZZZZZZZZZ".into(),
            device_name: "laptop".into(),
            os: "linux".into(),
            tailscale_id: "tsnode-1".into(),
        });
        let value: serde_json::Value = serde_json::to_value(&hello).unwrap();
        assert_eq!(value["kind"], "hello");
        assert_eq!(value["version"], 2);
        assert_eq!(value["identity"]["app_id"], "chat");
        assert_eq!(value["identity"]["device_id"], "01HZZZZZZZZZZZZZZZZZZZZZZZ");
        assert_eq!(value["identity"]["device_name"], "laptop");
        assert_eq!(value["identity"]["os"], "linux");
    }

    #[test]
    fn hello_envelope_rejects_unknown_kind() {
        let raw = r#"{"kind":"not_hello","version":2,"identity":{"app_id":"a","device_id":"x","device_name":"y","os":"darwin","tailscale_id":"z"}}"#;
        let parsed: Result<HelloEnvelope, _> = serde_json::from_str(raw);
        assert!(
            parsed.is_err(),
            "unknown kind should fail to deserialize as HelloEnvelope"
        );
    }
}
