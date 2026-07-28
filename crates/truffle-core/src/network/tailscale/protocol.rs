//! JSON command/event protocol types for Go sidecar communication.
//!
//! Commands are sent Rust -> Go via stdin (JSON lines).
//! Events are received Go -> Rust via stdout (JSON lines).
//!
//! These types match the wire format defined in `packages/sidecar-slim/main.go`.

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Commands: Rust → Go sidecar (JSON lines on stdin)
// ---------------------------------------------------------------------------

/// Envelope for all commands sent to the Go sidecar.
#[derive(Clone, Serialize)]
pub(crate) struct SidecarCommand {
    pub command: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

/// Manual `Debug`: `data` can embed secrets (`tsnet:start` carries the auth
/// key and session token as a JSON value), so its contents are redacted.
impl std::fmt::Debug for SidecarCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SidecarCommand")
            .field("command", &self.command)
            .field("data", &self.data.as_ref().map(|_| "[REDACTED]"))
            .finish()
    }
}

/// Data payload for `tsnet:start`.
///
/// `Serialize` intentionally emits the real `auth_key` and `session_token` —
/// this is the wire payload the sidecar needs. Only `Debug` redacts.
#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct StartCommandData {
    pub hostname: String,
    pub state_dir: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_key: Option<String>,
    pub bridge_port: u16,
    pub session_token: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ephemeral: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<Vec<String>>,
    /// Override the bridged-connection idle-reap deadline (seconds). Omitted
    /// when None so old sidecars ignore it (RFC 021 §6.5).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idle_timeout_secs: Option<u64>,
    // NOTE: keep the manual Debug impl below in sync when adding fields.
}

/// Manual `Debug`: `auth_key` (tailnet credential) and `session_token`
/// (bridge auth secret) must never reach logs, so both are redacted.
impl std::fmt::Debug for StartCommandData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StartCommandData")
            .field("hostname", &self.hostname)
            .field("state_dir", &self.state_dir)
            .field("auth_key", &self.auth_key.as_ref().map(|_| "[REDACTED]"))
            .field("bridge_port", &self.bridge_port)
            .field("session_token", &"[REDACTED]")
            .field("ephemeral", &self.ephemeral)
            .field("tags", &self.tags)
            .field("idle_timeout_secs", &self.idle_timeout_secs)
            .finish()
    }
}

/// Data payload for `bridge:dial`.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DialCommandData {
    pub request_id: String,
    pub target: String,
    pub port: u16,
    /// Override TLS wrapping of the dial. Omitted when None so old sidecars fall
    /// back to their legacy port==443 behavior (RFC 021 §6.4).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tls: Option<bool>,
}

/// Data payload for `tsnet:listen`.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ListenCommandData {
    pub port: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tls: Option<bool>,
    /// Correlation id echoed on the terminal event (v4 sidecars; older ones
    /// ignore the field, which is why broker use is version-gated).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
}

/// Data payload for `tsnet:unlisten`.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct UnlistenCommandData {
    pub port: u16,
}

/// Data payload for `tsnet:ping`.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PingCommandData {
    pub target: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ping_type: Option<String>,
    /// Correlation id echoed on `tsnet:pingResult` (P12; sidecars ≥ v2).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
}

/// Data payload for `tsnet:whois`.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WhoisCommandData {
    /// Tailnet IP or ip:port to look up.
    pub addr: String,
    /// Correlation id echoed on `tsnet:whoisResult` (all whois-capable
    /// sidecars echo it, so the broker path needs no extra gate).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
}

/// Data payload for `tsnet:watchPeers`.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WatchPeersCommandData {
    /// If true, also include non-truffle peers in events.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include_all: Option<bool>,
}

/// Data payload for `tsnet:listenPacket`.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ListenPacketCommandData {
    pub port: u16,
    /// Correlation id echoed on the terminal event (v4 sidecars).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
}

/// Data payload for `proxy:add`.
///
/// v2 fields (RFC 023) ride the same command; sidecars predating them
/// ignore unknown JSON fields, which is why the provider version-gates
/// v2-only requests instead of trusting the wire (§8.1).
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ProxyAddCommandData {
    pub id: String,
    pub name: String,
    pub listen_port: u16,
    pub target_host: String,
    pub target_port: u16,
    pub target_scheme: String,
    /// TLS on the tailnet listener. Serialized always so a v2 sidecar never
    /// guesses; v1 sidecars ignore it (and are gated off `false`).
    pub tls: bool,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub allow_non_loopback: bool,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub allow: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub routes: Vec<crate::network::ProxyRoute>,
    /// Correlation id echoed on `proxy:added` / `proxy:error` (v4 sidecars).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
}

/// Data payload for `proxy:remove`.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ProxyRemoveCommandData {
    pub id: String,
    /// Correlation id echoed on `proxy:removed` / `proxy:error` (v4 sidecars).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
}

/// Data payload for `proxy:list` (optional — pre-v4 callers send no data).
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ProxyListCommandData {
    /// Correlation id echoed on the `proxy:list` result event (v4 sidecars).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
}

// ---------------------------------------------------------------------------
// Well-known command type strings
// ---------------------------------------------------------------------------

pub(crate) mod command_type {
    pub const START: &str = "tsnet:start";
    pub const STOP: &str = "tsnet:stop";
    pub const GET_PEERS: &str = "tsnet:getPeers";
    pub const DIAL: &str = "bridge:dial";
    pub const LISTEN: &str = "tsnet:listen";
    pub const UNLISTEN: &str = "tsnet:unlisten";
    pub const PING: &str = "tsnet:ping";
    pub const WHOIS: &str = "tsnet:whois";
    pub const WATCH_PEERS: &str = "tsnet:watchPeers";
    pub const LISTEN_PACKET: &str = "tsnet:listenPacket";
    pub const PROXY_ADD: &str = "proxy:add";
    pub const PROXY_REMOVE: &str = "proxy:remove";
    pub const PROXY_LIST: &str = "proxy:list";
}

// ---------------------------------------------------------------------------
// Events: Go sidecar → Rust (JSON lines on stdout)
// ---------------------------------------------------------------------------

/// Envelope for all events received from the Go sidecar.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct SidecarEvent {
    pub event: String,
    #[serde(default)]
    pub data: serde_json::Value,
}

// ---------------------------------------------------------------------------
// Well-known event type strings
// ---------------------------------------------------------------------------

pub(crate) mod event_type {
    pub const STARTED: &str = "tsnet:started";
    pub const STOPPED: &str = "tsnet:stopped";
    pub const STATUS: &str = "tsnet:status";
    pub const AUTH_REQUIRED: &str = "tsnet:authRequired";
    pub const NEEDS_APPROVAL: &str = "tsnet:needsApproval";
    pub const STATE_CHANGE: &str = "tsnet:stateChange";
    pub const KEY_EXPIRING: &str = "tsnet:keyExpiring";
    pub const HEALTH_WARNING: &str = "tsnet:healthWarning";
    pub const PEERS: &str = "tsnet:peers";
    pub const DIAL_RESULT: &str = "bridge:dialResult";
    pub const ERROR: &str = "tsnet:error";
    pub const LISTENING: &str = "tsnet:listening";
    pub const UNLISTENED: &str = "tsnet:unlistened";
    pub const PING_RESULT: &str = "tsnet:pingResult";
    pub const WHOIS_RESULT: &str = "tsnet:whoisResult";
    pub const PEER_CHANGED: &str = "tsnet:peerChanged";
    pub const LISTENING_PACKET: &str = "tsnet:listeningPacket";
    pub const PROXY_ADDED: &str = "proxy:added";
    pub const PROXY_REMOVED: &str = "proxy:removed";
    pub const PROXY_LIST_RESULT: &str = "proxy:list";
    pub const PROXY_ERROR: &str = "proxy:error";
}

// ---------------------------------------------------------------------------
// Event data payloads
// ---------------------------------------------------------------------------

/// Data from `tsnet:status` event.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct StatusEventData {
    pub state: String,
    #[serde(default)]
    pub hostname: String,
    #[serde(default)]
    pub dns_name: String,
    #[serde(default, alias = "tailscaleIP")]
    pub tailscale_ip: String,
    /// Tailscale stable node ID (e.g. "nv8m2uw7se11CNTRL").
    #[serde(default)]
    pub node_id: String,
    #[serde(default)]
    pub error: String,
    /// Sidecar control-protocol version (RFC 023: `2` = proxy engine v2 +
    /// 443-free). Absent on older sidecars → treated as v1; the provider
    /// rejects v2-only features rather than let them be silently ignored.
    #[serde(default)]
    pub protocol_version: Option<u32>,
}

/// Data from `tsnet:authRequired` event.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AuthRequiredEventData {
    pub auth_url: String,
}

/// Data from `tsnet:stateChange` event.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct StateChangeEventData {
    pub state: String,
}

/// Data from `tsnet:keyExpiring` event.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct KeyExpiringEventData {
    pub expires_at: String,
}

/// Data from `tsnet:healthWarning` event.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct HealthWarningEventData {
    pub warnings: Vec<String>,
}

/// A peer as reported by the Go sidecar.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SidecarPeer {
    pub id: String,
    pub hostname: String,
    pub dns_name: String,
    #[serde(rename = "tailscaleIPs")]
    pub tailscale_ips: Vec<String>,
    pub online: bool,
    #[serde(default)]
    pub os: String,
    #[serde(default)]
    pub cur_addr: String,
    #[serde(default)]
    pub relay: String,
    #[serde(default)]
    pub last_seen: Option<String>,
    #[serde(default)]
    pub key_expiry: Option<String>,
    #[serde(default)]
    pub expired: bool,
}

/// Data from `tsnet:peers` event.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PeersEventData {
    pub peers: Vec<SidecarPeer>,
}

/// Data from `bridge:dialResult` event.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DialResultEventData {
    /// The dial's correlation id, echoed back. Correlation now happens at
    /// the JSON level in the reply broker, so nothing reads this field —
    /// it stays declared because it IS the wire contract (and the codec
    /// tests pin it).
    #[allow(dead_code)]
    pub request_id: String,
    pub success: bool,
    #[serde(default)]
    pub error: String,
}

/// Data from `tsnet:error` event.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ErrorEventData {
    pub code: String,
    pub message: String,
}

/// Data from `tsnet:listening` event.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ListeningEventData {
    pub port: u16,
}

/// Data from `tsnet:unlistened` event.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct UnlistenedEventData {
    pub port: u16,
}

/// Data from `tsnet:listeningPacket` event.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ListeningPacketEventData {
    /// The tsnet-bound port (the one requested by Rust).
    pub port: u16,
    /// The local relay port that Rust should send/recv datagrams to/from.
    pub local_port: u16,
}

/// Data from `tsnet:pingResult` event.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PingResultEventData {
    #[serde(default)]
    pub target: String,
    #[serde(default)]
    pub latency_ms: f64,
    #[serde(default)]
    pub direct: bool,
    #[serde(default)]
    pub relay: String,
    #[serde(default)]
    pub peer_addr: String,
    #[serde(default)]
    pub error: String,
}

/// Data from `tsnet:whoisResult` event.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WhoisResultEventData {
    /// The queried address, echoed back. Correlation now happens at the
    /// JSON level in the reply broker, so nothing reads this field — it
    /// stays declared because it IS the wire contract (and the codec tests
    /// pin it).
    #[allow(dead_code)]
    #[serde(default)]
    pub addr: String,
    /// `None` with an empty `error` = the lookup found nothing: the address
    /// maps to no known tailnet node (anonymous — absent, not fabricated).
    #[serde(default)]
    pub identity: Option<crate::network::TailscalePeerIdentity>,
    #[serde(default)]
    pub error: String,
}

/// Data from `tsnet:peerChanged` event (from WatchIPNBus).
///
/// Represents a single peer change notification.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PeerChangedEventData {
    /// "joined", "left", or "updated"
    pub change_type: String,
    /// The peer data (absent for "left" events).
    pub peer: Option<SidecarPeer>,
    /// Peer ID (always present, used for "left" events).
    pub peer_id: String,
}

/// Data from `proxy:added` event.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ProxyAddedEventData {
    pub id: String,
    pub listen_port: u16,
    pub url: String,
}

/// Data from `proxy:removed` event.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ProxyRemovedEventData {
    pub id: String,
}

/// Data from `proxy:list` event.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ProxyListEventData {
    pub proxies: Vec<ProxyInfoEventData>,
}

/// Per-proxy info in list response.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ProxyInfoEventData {
    pub id: String,
    pub name: String,
    pub listen_port: u16,
    pub target_host: String,
    pub target_port: u16,
    pub target_scheme: String,
    pub url: String,
}

/// Data from `proxy:error` event.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ProxyErrorEventData {
    pub id: String,
    pub code: String,
    pub message: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serialize_start_command() {
        let data = StartCommandData {
            hostname: "my-node".to_string(),
            state_dir: "/tmp/tsnet".to_string(),
            auth_key: None,
            bridge_port: 12345,
            session_token: "aa".repeat(32),
            ephemeral: None,
            tags: None,
            idle_timeout_secs: None,
        };
        let cmd = SidecarCommand {
            command: command_type::START,
            data: Some(serde_json::to_value(&data).unwrap()),
        };
        let json = serde_json::to_string(&cmd).unwrap();
        assert!(json.contains("\"command\":\"tsnet:start\""));
        assert!(json.contains("\"hostname\":\"my-node\""));
        assert!(json.contains("\"bridgePort\":12345"));
        // auth_key should be absent (None -> skip)
        assert!(!json.contains("authKey"));
        // idle_timeout_secs should be absent (None -> skip)
        assert!(!json.contains("idleTimeoutSecs"));
    }

    #[test]
    fn serialize_start_command_with_idle_timeout() {
        let data = StartCommandData {
            hostname: "my-node".to_string(),
            state_dir: "/tmp/tsnet".to_string(),
            auth_key: None,
            bridge_port: 12345,
            session_token: "aa".repeat(32),
            ephemeral: None,
            tags: None,
            idle_timeout_secs: Some(300),
        };
        let json = serde_json::to_string(&data).unwrap();
        assert!(json.contains("\"idleTimeoutSecs\":300"));
    }

    #[test]
    fn debug_redacts_start_command_secrets() {
        let data = StartCommandData {
            hostname: "my-node".to_string(),
            state_dir: "/tmp/tsnet".to_string(),
            auth_key: Some("dummy-auth-SECRET123".to_string()),
            bridge_port: 12345,
            session_token: "deadbeef".repeat(8),
            ephemeral: None,
            tags: None,
            idle_timeout_secs: None,
        };
        let dbg = format!("{data:?}");
        assert!(!dbg.contains("SECRET123"));
        assert!(!dbg.contains("deadbeef"));
        assert!(dbg.contains("[REDACTED]"));

        // Serialization must still carry the real values — that's the wire
        // payload the sidecar authenticates with.
        let json = serde_json::to_string(&data).unwrap();
        assert!(json.contains("dummy-auth-SECRET123"));
        assert!(json.contains("deadbeef"));

        // The command envelope embeds the payload as a JSON value; Debug on
        // the envelope must not leak it either.
        let cmd = SidecarCommand {
            command: command_type::START,
            data: Some(serde_json::to_value(&data).unwrap()),
        };
        let dbg = format!("{cmd:?}");
        assert!(!dbg.contains("SECRET123"));
        assert!(!dbg.contains("deadbeef"));
    }

    #[test]
    fn serialize_dial_command() {
        let data = DialCommandData {
            request_id: "req-123".to_string(),
            target: "peer.tailnet.ts.net".to_string(),
            port: 9417,
            tls: None,
        };
        let cmd = SidecarCommand {
            command: command_type::DIAL,
            data: Some(serde_json::to_value(&data).unwrap()),
        };
        let json = serde_json::to_string(&cmd).unwrap();
        assert!(json.contains("\"command\":\"bridge:dial\""));
        assert!(json.contains("\"requestId\":\"req-123\""));
        assert!(json.contains("\"port\":9417"));
        // tls should be absent (None -> skip)
        assert!(!json.contains("tls"));
    }

    #[test]
    fn serialize_dial_command_with_tls() {
        // Explicit tls=false must be present on the wire so the sidecar can
        // suppress its legacy port==443 auto-TLS behavior.
        let data = DialCommandData {
            request_id: "req-123".to_string(),
            target: "peer.tailnet.ts.net".to_string(),
            port: 443,
            tls: Some(false),
        };
        let json = serde_json::to_string(&data).unwrap();
        assert!(json.contains("\"tls\":false"));

        let data = DialCommandData {
            request_id: "req-124".to_string(),
            target: "peer.tailnet.ts.net".to_string(),
            port: 8080,
            tls: Some(true),
        };
        let json = serde_json::to_string(&data).unwrap();
        assert!(json.contains("\"tls\":true"));
    }

    #[test]
    fn serialize_listen_command() {
        let data = ListenCommandData {
            port: 8080,
            tls: None,
            request_id: None,
        };
        let cmd = SidecarCommand {
            command: command_type::LISTEN,
            data: Some(serde_json::to_value(&data).unwrap()),
        };
        let json = serde_json::to_string(&cmd).unwrap();
        assert!(json.contains("\"command\":\"tsnet:listen\""));
        assert!(json.contains("\"port\":8080"));
        assert!(!json.contains("tls"));
    }

    #[test]
    fn serialize_ping_command() {
        let data = PingCommandData {
            target: "100.64.0.2".to_string(),
            ping_type: Some("TSMP".to_string()),
            request_id: None,
        };
        let cmd = SidecarCommand {
            command: command_type::PING,
            data: Some(serde_json::to_value(&data).unwrap()),
        };
        let json = serde_json::to_string(&cmd).unwrap();
        assert!(json.contains("\"command\":\"tsnet:ping\""));
        assert!(json.contains("\"target\":\"100.64.0.2\""));
        assert!(json.contains("\"pingType\":\"TSMP\""));
    }

    #[test]
    fn serialize_watch_peers_command() {
        let data = WatchPeersCommandData { include_all: None };
        let cmd = SidecarCommand {
            command: command_type::WATCH_PEERS,
            data: Some(serde_json::to_value(&data).unwrap()),
        };
        let json = serde_json::to_string(&cmd).unwrap();
        assert!(json.contains("\"command\":\"tsnet:watchPeers\""));
    }

    #[test]
    fn deserialize_status_event() {
        let json = r#"{"event":"tsnet:status","data":{"state":"running","hostname":"my-node","dnsName":"my-node.tailnet.ts.net","tailscaleIP":"100.64.0.1"}}"#;
        let event: SidecarEvent = serde_json::from_str(json).unwrap();
        assert_eq!(event.event, "tsnet:status");
        let data: StatusEventData = serde_json::from_value(event.data).unwrap();
        assert_eq!(data.state, "running");
        assert_eq!(data.hostname, "my-node");
        assert_eq!(data.tailscale_ip, "100.64.0.1");
    }

    #[test]
    fn deserialize_peers_event() {
        let json = r#"{"event":"tsnet:peers","data":{"peers":[{"id":"node123","hostname":"truffle-cli-abc","dnsName":"truffle-cli-abc.tailnet.ts.net","tailscaleIPs":["100.64.0.2"],"online":true,"os":"linux","curAddr":"192.168.1.5:41641","relay":"","lastSeen":"2026-03-24T10:00:00Z","keyExpiry":"2026-06-24T10:00:00Z","expired":false}]}}"#;
        let event: SidecarEvent = serde_json::from_str(json).unwrap();
        assert_eq!(event.event, "tsnet:peers");
        let data: PeersEventData = serde_json::from_value(event.data).unwrap();
        assert_eq!(data.peers.len(), 1);
        assert_eq!(data.peers[0].id, "node123");
        assert_eq!(data.peers[0].hostname, "truffle-cli-abc");
        assert!(data.peers[0].online);
    }

    #[test]
    fn deserialize_dial_result_event() {
        let json = r#"{"event":"bridge:dialResult","data":{"requestId":"req-456","success":true}}"#;
        let event: SidecarEvent = serde_json::from_str(json).unwrap();
        let data: DialResultEventData = serde_json::from_value(event.data).unwrap();
        assert_eq!(data.request_id, "req-456");
        assert!(data.success);
        assert!(data.error.is_empty());
    }

    #[test]
    fn deserialize_dial_result_failure() {
        let json = r#"{"event":"bridge:dialResult","data":{"requestId":"req-789","success":false,"error":"connection refused"}}"#;
        let event: SidecarEvent = serde_json::from_str(json).unwrap();
        let data: DialResultEventData = serde_json::from_value(event.data).unwrap();
        assert_eq!(data.request_id, "req-789");
        assert!(!data.success);
        assert_eq!(data.error, "connection refused");
    }

    #[test]
    fn deserialize_ping_result_event() {
        let json = r#"{"event":"tsnet:pingResult","data":{"target":"100.64.0.2","latencyMs":12.5,"direct":true,"relay":"","peerAddr":"192.168.1.5:41641"}}"#;
        let event: SidecarEvent = serde_json::from_str(json).unwrap();
        let data: PingResultEventData = serde_json::from_value(event.data).unwrap();
        assert_eq!(data.target, "100.64.0.2");
        assert!((data.latency_ms - 12.5).abs() < f64::EPSILON);
        assert!(data.direct);
    }

    #[test]
    fn deserialize_peer_changed_event() {
        let json = r#"{"event":"tsnet:peerChanged","data":{"changeType":"joined","peerId":"node123","peer":{"id":"node123","hostname":"truffle-cli-abc","dnsName":"truffle-cli-abc.tailnet.ts.net","tailscaleIPs":["100.64.0.2"],"online":true}}}"#;
        let event: SidecarEvent = serde_json::from_str(json).unwrap();
        let data: PeerChangedEventData = serde_json::from_value(event.data).unwrap();
        assert_eq!(data.change_type, "joined");
        assert_eq!(data.peer_id, "node123");
        assert!(data.peer.is_some());
    }

    #[test]
    fn deserialize_peer_left_event() {
        let json =
            r#"{"event":"tsnet:peerChanged","data":{"changeType":"left","peerId":"node456"}}"#;
        let event: SidecarEvent = serde_json::from_str(json).unwrap();
        let data: PeerChangedEventData = serde_json::from_value(event.data).unwrap();
        assert_eq!(data.change_type, "left");
        assert_eq!(data.peer_id, "node456");
        assert!(data.peer.is_none());
    }

    #[test]
    fn deserialize_error_event() {
        let json =
            r#"{"event":"tsnet:error","data":{"code":"NOT_RUNNING","message":"node not running"}}"#;
        let event: SidecarEvent = serde_json::from_str(json).unwrap();
        let data: ErrorEventData = serde_json::from_value(event.data).unwrap();
        assert_eq!(data.code, "NOT_RUNNING");
        assert_eq!(data.message, "node not running");
    }

    #[test]
    fn deserialize_auth_required_event() {
        let json = r#"{"event":"tsnet:authRequired","data":{"authUrl":"https://login.tailscale.com/a/abc123"}}"#;
        let event: SidecarEvent = serde_json::from_str(json).unwrap();
        let data: AuthRequiredEventData = serde_json::from_value(event.data).unwrap();
        assert_eq!(data.auth_url, "https://login.tailscale.com/a/abc123");
    }

    #[test]
    fn deserialize_listening_event() {
        let json = r#"{"event":"tsnet:listening","data":{"port":8080}}"#;
        let event: SidecarEvent = serde_json::from_str(json).unwrap();
        let data: ListeningEventData = serde_json::from_value(event.data).unwrap();
        assert_eq!(data.port, 8080);
    }

    #[test]
    fn deserialize_health_warning_event() {
        let json = r#"{"event":"tsnet:healthWarning","data":{"warnings":["DNS is failing","MagicDNS not working"]}}"#;
        let event: SidecarEvent = serde_json::from_str(json).unwrap();
        let data: HealthWarningEventData = serde_json::from_value(event.data).unwrap();
        assert_eq!(data.warnings.len(), 2);
    }

    #[test]
    fn serialize_listen_packet_command() {
        let data = ListenPacketCommandData {
            port: 19420,
            request_id: None,
        };
        let cmd = SidecarCommand {
            command: command_type::LISTEN_PACKET,
            data: Some(serde_json::to_value(&data).unwrap()),
        };
        let json = serde_json::to_string(&cmd).unwrap();
        assert!(json.contains("\"command\":\"tsnet:listenPacket\""));
        assert!(json.contains("\"port\":19420"));
    }

    #[test]
    fn deserialize_listening_packet_event() {
        let json = r#"{"event":"tsnet:listeningPacket","data":{"port":19420,"localPort":54321}}"#;
        let event: SidecarEvent = serde_json::from_str(json).unwrap();
        assert_eq!(event.event, "tsnet:listeningPacket");
        let data: ListeningPacketEventData = serde_json::from_value(event.data).unwrap();
        assert_eq!(data.port, 19420);
        assert_eq!(data.local_port, 54321);
    }

    #[test]
    fn serialize_proxy_add_command() {
        let data = ProxyAddCommandData {
            id: "dev-server".to_string(),
            name: "Dev Server".to_string(),
            listen_port: 3001,
            target_host: "localhost".to_string(),
            target_port: 3000,
            target_scheme: "http".to_string(),
            tls: true,
            allow_non_loopback: false,
            allow: vec![],
            routes: vec![],
            request_id: None,
        };
        let cmd = SidecarCommand {
            command: command_type::PROXY_ADD,
            data: Some(serde_json::to_value(&data).unwrap()),
        };
        let json = serde_json::to_string(&cmd).unwrap();
        assert!(json.contains("\"command\":\"proxy:add\""));
        assert!(json.contains("\"listenPort\":3001"));
        assert!(json.contains("\"targetHost\":\"localhost\""));
        assert!(json.contains("\"targetPort\":3000"));
        // v1-shaped configs stay v1-shaped on the wire: tls is explicit,
        // the v2-only fields are omitted entirely.
        assert!(json.contains("\"tls\":true"));
        assert!(!json.contains("allowNonLoopback"));
        assert!(!json.contains("\"allow\""));
        assert!(!json.contains("\"routes\""));
    }

    #[test]
    fn serialize_proxy_add_command_v2_routes() {
        let data = ProxyAddCommandData {
            id: "web".to_string(),
            name: "web".to_string(),
            listen_port: 443,
            target_host: "localhost".to_string(),
            target_port: 0,
            target_scheme: "http".to_string(),
            tls: true,
            allow_non_loopback: false,
            allow: vec!["*@corp.com".to_string()],
            routes: vec![
                crate::network::ProxyRoute {
                    prefix: "/api".to_string(),
                    target_url: Some("http://localhost:8000".to_string()),
                    dir: None,
                    fallback: None,
                    strip_prefix: true,
                    allow: vec!["ops@corp.com".to_string()],
                },
                crate::network::ProxyRoute {
                    prefix: "/".to_string(),
                    target_url: None,
                    dir: Some("/srv/public".to_string()),
                    fallback: Some("/index.html".to_string()),
                    strip_prefix: false,
                    allow: vec![],
                },
            ],
            request_id: None,
        };
        let json = serde_json::to_string(&data).unwrap();
        assert!(json.contains("\"allow\":[\"*@corp.com\"]"));
        assert!(json.contains("\"prefix\":\"/api\""));
        assert!(json.contains("\"targetUrl\":\"http://localhost:8000\""));
        assert!(json.contains("\"stripPrefix\":true"));
        assert!(json.contains("\"dir\":\"/srv/public\""));
        assert!(json.contains("\"fallback\":\"/index.html\""));
        // Per-route empty allow and absent backends are omitted.
        assert!(!json.contains("\"targetUrl\":null"));
    }

    #[test]
    fn status_event_parses_protocol_version() {
        let json = r#"{"event":"tsnet:status","data":{"state":"running","hostname":"h","dnsName":"h.t.ts.net","tailscaleIP":"100.64.0.1","protocolVersion":2}}"#;
        let event: SidecarEvent = serde_json::from_str(json).unwrap();
        let data: StatusEventData = serde_json::from_value(event.data).unwrap();
        assert_eq!(data.protocol_version, Some(2));

        // Older sidecars omit the field → None (v1), not an error.
        let legacy = r#"{"event":"tsnet:status","data":{"state":"running","hostname":"h","dnsName":"h.t.ts.net","tailscaleIP":"100.64.0.1"}}"#;
        let event: SidecarEvent = serde_json::from_str(legacy).unwrap();
        let data: StatusEventData = serde_json::from_value(event.data).unwrap();
        assert_eq!(data.protocol_version, None);
    }

    #[test]
    fn serialize_proxy_remove_command() {
        let data = ProxyRemoveCommandData {
            id: "dev-server".to_string(),
            request_id: None,
        };
        let cmd = SidecarCommand {
            command: command_type::PROXY_REMOVE,
            data: Some(serde_json::to_value(&data).unwrap()),
        };
        let json = serde_json::to_string(&cmd).unwrap();
        assert!(json.contains("\"command\":\"proxy:remove\""));
        assert!(json.contains("\"id\":\"dev-server\""));
    }

    #[test]
    fn deserialize_proxy_added_event() {
        let json = r#"{"event":"proxy:added","data":{"id":"dev-server","listenPort":3001,"url":"https://myhost.ts.net:3001"}}"#;
        let event: SidecarEvent = serde_json::from_str(json).unwrap();
        assert_eq!(event.event, "proxy:added");
        let data: ProxyAddedEventData = serde_json::from_value(event.data).unwrap();
        assert_eq!(data.id, "dev-server");
        assert_eq!(data.listen_port, 3001);
        assert_eq!(data.url, "https://myhost.ts.net:3001");
    }

    #[test]
    fn deserialize_proxy_removed_event() {
        let json = r#"{"event":"proxy:removed","data":{"id":"dev-server"}}"#;
        let event: SidecarEvent = serde_json::from_str(json).unwrap();
        let data: ProxyRemovedEventData = serde_json::from_value(event.data).unwrap();
        assert_eq!(data.id, "dev-server");
    }

    #[test]
    fn deserialize_proxy_error_event() {
        let json = r#"{"event":"proxy:error","data":{"id":"dev-server","code":"CONNECTION_REFUSED","message":"target localhost:3000 not reachable"}}"#;
        let event: SidecarEvent = serde_json::from_str(json).unwrap();
        let data: ProxyErrorEventData = serde_json::from_value(event.data).unwrap();
        assert_eq!(data.id, "dev-server");
        assert_eq!(data.code, "CONNECTION_REFUSED");
    }

    #[test]
    fn deserialize_proxy_list_event() {
        let json = r#"{"event":"proxy:list","data":{"proxies":[{"id":"dev-server","name":"Dev Server","listenPort":3001,"targetHost":"localhost","targetPort":3000,"targetScheme":"http","url":"https://myhost.ts.net:3001"}]}}"#;
        let event: SidecarEvent = serde_json::from_str(json).unwrap();
        let data: ProxyListEventData = serde_json::from_value(event.data).unwrap();
        assert_eq!(data.proxies.len(), 1);
        assert_eq!(data.proxies[0].id, "dev-server");
        assert_eq!(data.proxies[0].listen_port, 3001);
    }
}
