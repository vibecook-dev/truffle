//! Serializable types for the Tauri frontend.
//!
//! These types wrap truffle-core's internal types with `#[derive(Serialize)]`
//! so they can be returned from Tauri commands as JSON. Core types intentionally
//! do not derive Serialize, so we map them here at the plugin boundary.
//!
//! RFC 017: identity is exposed as `appId` / `deviceId` / `deviceName`.
//! The Tailscale stable ID and hostname remain available as escape hatches
//! (`tailscaleId`, `tailscaleHostname`) for diagnostics.

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// StartConfig — input from frontend
// ---------------------------------------------------------------------------

/// Configuration for starting a truffle node, received from the frontend.
#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StartConfig {
    /// Application namespace identifier. Required. Matches
    /// `^[a-z][a-z0-9-]{1,31}$`.
    pub app_id: String,
    /// Optional human-readable device name. Defaults to OS hostname.
    pub device_name: Option<String>,
    /// Optional ULID override for the stable `device_id`.
    pub device_id: Option<String>,
    /// Path to the Go sidecar binary.
    pub sidecar_path: String,
    /// Optional Tailscale state directory.
    pub state_dir: Option<String>,
    /// Optional Tailscale auth key for headless authentication.
    pub auth_key: Option<String>,
    /// Whether the node is ephemeral (auto-removed on shutdown).
    #[serde(default)]
    pub ephemeral: bool,
    /// WebSocket listen port (defaults to 9417 if not set).
    pub ws_port: Option<u16>,
    /// RFC 025 §3.1: `loginName` globs of the tailnet users whose nodes may
    /// be peers, e.g. `["*@corp.com"]`. Empty or absent = the whole tailnet
    /// (the behaviour before RFC 025). Non-empty and the node is gated: only
    /// peers whose login matches are discovered, a peer whose login cannot be
    /// known is not a peer at all, and starting against a sidecar older than
    /// protocol 5 fails loudly instead of running peerless.
    pub login_allow: Option<Vec<String>>,
}

/// Manual `Debug`: `auth_key` is a tailnet credential and must never reach
/// logs, so it is redacted while preserving presence (`Some`/`None`).
impl std::fmt::Debug for StartConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StartConfig")
            .field("app_id", &self.app_id)
            .field("device_name", &self.device_name)
            .field("device_id", &self.device_id)
            .field("sidecar_path", &self.sidecar_path)
            .field("state_dir", &self.state_dir)
            .field("auth_key", &self.auth_key.as_ref().map(|_| "[REDACTED]"))
            .field("ephemeral", &self.ephemeral)
            .field("ws_port", &self.ws_port)
            .field("login_allow", &self.login_allow)
            .finish()
    }
}

/// Outcome of a `broadcastJson` / `broadcastBytes` call, serialized for the
/// frontend. "Queued" means handed to a peer's connection task — delivery
/// is not confirmed. Broadcasts reach only currently connected peers.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BroadcastReportJs {
    /// Peers with an active WS connection at broadcast time.
    pub attempted: u32,
    /// Messages successfully queued to a connection task.
    pub queued: u32,
    /// Tailscale ids of peers whose connection task was already closed.
    pub failed: Vec<String>,
}

impl From<truffle_core::BroadcastReport> for BroadcastReportJs {
    fn from(r: truffle_core::BroadcastReport) -> Self {
        Self {
            attempted: r.attempted as u32,
            queued: r.queued as u32,
            failed: r.failed,
        }
    }
}

// ---------------------------------------------------------------------------
// NodeIdentityJs
// ---------------------------------------------------------------------------

/// Local node identity, serialized for the frontend.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeIdentityJs {
    pub app_id: String,
    pub device_id: String,
    pub device_name: String,
    pub tailscale_hostname: String,
    pub tailscale_id: String,
    pub dns_name: Option<String>,
    pub ip: Option<String>,
    /// This node's own tailnet login (RFC 025 §3.6); `null` when unknown.
    pub login_name: Option<String>,
}

impl From<truffle_core::network::NodeIdentity> for NodeIdentityJs {
    fn from(i: truffle_core::network::NodeIdentity) -> Self {
        Self {
            app_id: i.app_id,
            device_id: i.device_id,
            device_name: i.device_name,
            tailscale_hostname: i.tailscale_hostname,
            tailscale_id: i.tailscale_id,
            dns_name: i.dns_name,
            ip: i.ip.map(|a| a.to_string()),
            login_name: i.login_name,
        }
    }
}

// ---------------------------------------------------------------------------
// PeerJs
// ---------------------------------------------------------------------------

/// A peer as seen by the frontend (RFC 022).
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PeerJs {
    pub device_id: Option<String>,
    pub device_name: Option<String>,
    pub display_name: String,
    pub hostname: String,
    pub ip: String,
    pub online: bool,
    pub ws_connected: bool,
    pub connection_type: String,
    pub os: Option<String>,
    pub last_seen: Option<String>,
    pub tailscale_id: String,
    pub peer_ref: String,
    pub generation: u64,
    /// The peer owner's tailnet login (RFC 025 §3.6); `null` when unknown.
    pub login_name: Option<String>,
}

impl From<truffle_core::Peer> for PeerJs {
    fn from(p: truffle_core::Peer) -> Self {
        Self {
            device_id: p.device_id,
            device_name: p.device_name,
            display_name: p.display_name,
            hostname: p.hostname,
            ip: p.ip.to_string(),
            online: p.online,
            ws_connected: p.ws_connected,
            connection_type: p.connection_type,
            os: p.os,
            last_seen: p.last_seen,
            tailscale_id: p.tailscale_id,
            peer_ref: p.peer_ref,
            generation: p.generation,
            login_name: p.login_name,
        }
    }
}

// ---------------------------------------------------------------------------
// PingResultJs
// ---------------------------------------------------------------------------

/// Ping result, serialized for the frontend.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PingResultJs {
    pub latency_ms: f64,
    pub connection: String,
    pub peer_addr: Option<String>,
}

impl From<truffle_core::network::PingResult> for PingResultJs {
    fn from(r: truffle_core::network::PingResult) -> Self {
        Self {
            latency_ms: r.latency.as_secs_f64() * 1000.0,
            connection: r.connection,
            peer_addr: r.peer_addr,
        }
    }
}

// ---------------------------------------------------------------------------
// HealthInfoJs
// ---------------------------------------------------------------------------

/// Health info, serialized for the frontend.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HealthInfoJs {
    pub state: String,
    pub key_expiry: Option<String>,
    pub warnings: Vec<String>,
    pub healthy: bool,
}

impl From<truffle_core::network::HealthInfo> for HealthInfoJs {
    fn from(h: truffle_core::network::HealthInfo) -> Self {
        Self {
            state: h.state,
            key_expiry: h.key_expiry,
            warnings: h.warnings,
            healthy: h.healthy,
        }
    }
}

// ---------------------------------------------------------------------------
// TransferResultJs
// ---------------------------------------------------------------------------

/// File transfer result, serialized for the frontend.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TransferResultJs {
    pub bytes_transferred: u64,
    pub sha256: String,
    pub elapsed_secs: f64,
}

impl From<truffle_core::TransferResult> for TransferResultJs {
    fn from(r: truffle_core::TransferResult) -> Self {
        Self {
            bytes_transferred: r.bytes_transferred,
            sha256: r.sha256,
            elapsed_secs: r.elapsed_secs,
        }
    }
}

// ---------------------------------------------------------------------------
// FileOfferJs
// ---------------------------------------------------------------------------

/// An incoming file offer, serialized for the frontend.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FileOfferJs {
    pub from_peer: String,
    pub from_name: String,
    pub file_name: String,
    pub size: u64,
    pub sha256: String,
    pub suggested_path: String,
    pub token: String,
}

impl From<truffle_core::FileOffer> for FileOfferJs {
    fn from(o: truffle_core::FileOffer) -> Self {
        Self {
            from_peer: o.from_peer,
            from_name: o.from_name,
            file_name: o.file_name,
            size: o.size,
            sha256: o.sha256,
            suggested_path: o.suggested_path,
            token: o.token,
        }
    }
}

// ---------------------------------------------------------------------------
// PeerEventJs
// ---------------------------------------------------------------------------

/// Peer event, serialized for the frontend.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum PeerEventJs {
    Joined { peer: PeerStateJs },
    Left { id: String },
    Updated { peer: PeerStateJs },
    Identity { peer: PeerStateJs },
    WsConnected { id: String },
    WsDisconnected { id: String },
    AuthRequired { url: String },
}

/// Internal peer state, serialized for the frontend (used in PeerEventJs).
///
/// Matches the `PeerJs` shape so the frontend only deals with one peer
/// type shape. Built via the core `Peer::from(PeerState)` conversion
/// (RFC 022 honest projection).
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PeerStateJs {
    pub device_id: Option<String>,
    pub device_name: Option<String>,
    pub display_name: String,
    pub hostname: String,
    pub ip: String,
    pub online: bool,
    pub ws_connected: bool,
    pub connection_type: String,
    pub os: Option<String>,
    pub last_seen: Option<String>,
    pub tailscale_id: String,
    pub peer_ref: String,
    pub generation: u64,
    /// The peer owner's tailnet login (RFC 025 §3.6); `null` when unknown.
    pub login_name: Option<String>,
}

impl From<truffle_core::session::PeerState> for PeerStateJs {
    fn from(s: truffle_core::session::PeerState) -> Self {
        // Delegate to the core `Peer` conversion so the identity projection
        // logic lives in one place.
        let peer: truffle_core::Peer = s.into();
        Self {
            device_id: peer.device_id,
            device_name: peer.device_name,
            display_name: peer.display_name,
            hostname: peer.hostname,
            ip: peer.ip.to_string(),
            online: peer.online,
            ws_connected: peer.ws_connected,
            connection_type: peer.connection_type,
            os: peer.os,
            last_seen: peer.last_seen,
            tailscale_id: peer.tailscale_id,
            peer_ref: peer.peer_ref,
            generation: peer.generation,
            login_name: peer.login_name,
        }
    }
}

impl From<truffle_core::session::PeerEvent> for PeerEventJs {
    fn from(e: truffle_core::session::PeerEvent) -> Self {
        use truffle_core::session::PeerEvent;
        match e {
            PeerEvent::Joined(state) => PeerEventJs::Joined { peer: state.into() },
            PeerEvent::Left(state) => PeerEventJs::Left { id: state.id },
            PeerEvent::Updated(state) => PeerEventJs::Updated { peer: state.into() },
            PeerEvent::Identity(state) => PeerEventJs::Identity { peer: state.into() },
            PeerEvent::WsConnected(id) => PeerEventJs::WsConnected { id },
            PeerEvent::WsDisconnected(id) => PeerEventJs::WsDisconnected { id },
            PeerEvent::AuthRequired { url } => PeerEventJs::AuthRequired { url },
        }
    }
}

// ---------------------------------------------------------------------------
// FileTransferEventJs
// ---------------------------------------------------------------------------

/// File transfer event, serialized for the frontend.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum FileTransferEventJs {
    OfferReceived {
        offer: FileOfferJs,
    },
    Hashing {
        token: String,
        file_name: String,
        bytes_hashed: u64,
        total_bytes: u64,
    },
    WaitingForAccept {
        token: String,
        file_name: String,
    },
    Progress {
        token: String,
        direction: String,
        file_name: String,
        bytes_transferred: u64,
        total_bytes: u64,
        speed_bps: f64,
    },
    Completed {
        token: String,
        direction: String,
        file_name: String,
        bytes_transferred: u64,
        sha256: String,
        elapsed_secs: f64,
    },
    Rejected {
        token: String,
        file_name: String,
        reason: String,
    },
    Failed {
        token: String,
        direction: String,
        file_name: String,
        reason: String,
    },
}

fn direction_str(d: truffle_core::TransferDirection) -> String {
    match d {
        truffle_core::TransferDirection::Send => "send".to_string(),
        truffle_core::TransferDirection::Receive => "receive".to_string(),
    }
}

// ---------------------------------------------------------------------------
// Proxy types
// ---------------------------------------------------------------------------

/// Configuration for adding a reverse proxy, received from the frontend.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProxyConfigJs {
    /// Unique identifier (user-chosen or auto-generated).
    pub id: String,
    /// Human-readable name for the proxy.
    pub name: String,
    /// Port on which the proxy listens on the tailnet.
    pub listen_port: u16,
    /// Target host (default: "localhost").
    pub target_host: Option<String>,
    /// Target port (the local service port).
    pub target_port: u16,
    /// Target scheme: "http" or "https" (default: "http").
    pub target_scheme: Option<String>,
    /// Whether to announce this proxy on the mesh for discovery (default: true).
    pub announce: Option<bool>,
    /// Terminate TLS on the tailnet listener (default: true — the v1
    /// always-TLS behavior). `false` = plain HTTP; requires a v2 sidecar.
    pub tls: Option<bool>,
    /// Permit non-loopback targets (default: false — deny). A LAN target
    /// turns this node into a pivot into its network (RFC 023 §9.3).
    pub allow_non_loopback: Option<bool>,
    /// loginName allow globs, e.g. `["*@corp.com"]` (default: none = the
    /// whole tailnet). Non-matching callers get a bare 403 (RFC 023 §9.7).
    pub allow: Option<Vec<String>>,
    /// Path-prefix routes (RFC 023 §7). When non-empty they replace the
    /// single `targetHost`/`targetPort`/`targetScheme` target.
    pub routes: Option<Vec<ProxyRouteJs>>,
}

/// One path-prefix route of a v2 proxy (RFC 023 §7). Exactly one of
/// `targetUrl` / `dir` is set; longest prefix wins. Validation lives in
/// core `validate_config`, not here.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProxyRouteJs {
    /// Path prefix to match (must start with "/").
    pub prefix: String,
    /// Proxy target URL, e.g. "http://localhost:8000". Mutually exclusive
    /// with `dir`.
    pub target_url: Option<String>,
    /// Static directory to serve (absolute path on the serving machine).
    /// Mutually exclusive with `targetUrl`.
    pub dir: Option<String>,
    /// SPA fallback rewritten on static misses, e.g. "/index.html". Only
    /// meaningful with `dir`.
    pub fallback: Option<String>,
    /// Strip the matched prefix before proxying (default: false). Only
    /// meaningful with `targetUrl`.
    pub strip_prefix: Option<bool>,
    /// Per-route loginName globs; overrides the config-level `allow`
    /// (default: none = inherit).
    pub allow: Option<Vec<String>>,
}

impl From<ProxyRouteJs> for truffle_core::network::ProxyRoute {
    fn from(r: ProxyRouteJs) -> Self {
        Self {
            prefix: r.prefix,
            target_url: r.target_url,
            dir: r.dir,
            fallback: r.fallback,
            strip_prefix: r.strip_prefix.unwrap_or(false),
            allow: r.allow.unwrap_or_default(),
        }
    }
}

/// Information about a running or configured proxy, serialized for the frontend.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProxyInfoJs {
    pub id: String,
    pub name: String,
    pub listen_port: u16,
    pub target_host: String,
    pub target_port: u16,
    pub target_scheme: String,
    pub url: String,
    pub status: String,
}

impl From<truffle_core::proxy::ProxyInfo> for ProxyInfoJs {
    fn from(info: truffle_core::proxy::ProxyInfo) -> Self {
        Self {
            id: info.id,
            name: info.name,
            listen_port: info.listen_port,
            target_host: info.target.host,
            target_port: info.target.port,
            target_scheme: info.target.scheme,
            url: info.url,
            status: match info.status {
                truffle_core::proxy::ProxyStatus::Starting => "starting".to_string(),
                truffle_core::proxy::ProxyStatus::Running => "running".to_string(),
                truffle_core::proxy::ProxyStatus::Stopped => "stopped".to_string(),
                truffle_core::proxy::ProxyStatus::Error(msg) => format!("error: {msg}"),
            },
        }
    }
}

/// Proxy lifecycle event, serialized for the frontend.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum ProxyEventJs {
    Started {
        id: String,
        url: String,
        listen_port: u16,
    },
    Stopped {
        id: String,
    },
    Error {
        id: String,
        code: String,
        message: String,
    },
}

impl From<truffle_core::proxy::ProxyEvent> for ProxyEventJs {
    fn from(e: truffle_core::proxy::ProxyEvent) -> Self {
        use truffle_core::proxy::ProxyEvent;
        match e {
            ProxyEvent::Started {
                id,
                url,
                listen_port,
            } => ProxyEventJs::Started {
                id,
                url,
                listen_port,
            },
            ProxyEvent::Stopped { id } => ProxyEventJs::Stopped { id },
            ProxyEvent::Error { id, code, message } => ProxyEventJs::Error { id, code, message },
        }
    }
}

impl From<truffle_core::FileTransferEvent> for FileTransferEventJs {
    fn from(e: truffle_core::FileTransferEvent) -> Self {
        use truffle_core::FileTransferEvent;
        match e {
            FileTransferEvent::OfferReceived(offer) => FileTransferEventJs::OfferReceived {
                offer: offer.into(),
            },
            FileTransferEvent::Hashing {
                token,
                file_name,
                bytes_hashed,
                total_bytes,
            } => FileTransferEventJs::Hashing {
                token,
                file_name,
                bytes_hashed,
                total_bytes,
            },
            FileTransferEvent::WaitingForAccept { token, file_name } => {
                FileTransferEventJs::WaitingForAccept { token, file_name }
            }
            FileTransferEvent::Progress(p) => FileTransferEventJs::Progress {
                token: p.token,
                direction: direction_str(p.direction),
                file_name: p.file_name,
                bytes_transferred: p.bytes_transferred,
                total_bytes: p.total_bytes,
                speed_bps: p.speed_bps,
            },
            FileTransferEvent::Completed {
                token,
                direction,
                file_name,
                bytes_transferred,
                sha256,
                elapsed_secs,
            } => FileTransferEventJs::Completed {
                token,
                direction: direction_str(direction),
                file_name,
                bytes_transferred,
                sha256,
                elapsed_secs,
            },
            FileTransferEvent::Rejected {
                token,
                file_name,
                reason,
            } => FileTransferEventJs::Rejected {
                token,
                file_name,
                reason,
            },
            FileTransferEvent::Failed {
                token,
                direction,
                file_name,
                reason,
            } => FileTransferEventJs::Failed {
                token,
                direction: direction_str(direction),
                file_name,
                reason,
            },
        }
    }
}

#[cfg(test)]
mod start_config_debug_tests {
    use super::*;

    #[test]
    fn start_config_debug_redacts_auth_key() {
        let config = StartConfig {
            app_id: "demo".to_string(),
            device_name: None,
            device_id: None,
            sidecar_path: "/opt/sidecar".to_string(),
            state_dir: None,
            auth_key: Some("dummy-auth-SECRET123".to_string()),
            ephemeral: false,
            ws_port: None,
            login_allow: Some(vec!["*@corp.com".to_string()]),
        };
        let dbg = format!("{config:?}");
        assert!(!dbg.contains("SECRET123"));
        assert!(dbg.contains("[REDACTED]"));
        // The gate is not a credential — it stays readable in the debug line.
        assert!(dbg.contains("*@corp.com"));
    }
}

#[cfg(test)]
mod proxy_route_mapping_tests {
    use super::*;

    #[test]
    fn absent_strip_prefix_and_allow_map_to_core_defaults() {
        // A dir route with stripPrefix / allow omitted must reach core as
        // `strip_prefix: false` / `allow: []` (the v1-equivalent defaults),
        // not as anything the engine would treat as set.
        let route = ProxyRouteJs {
            prefix: "/".to_string(),
            target_url: None,
            dir: Some("/srv/public".to_string()),
            fallback: Some("/index.html".to_string()),
            strip_prefix: None,
            allow: None,
        };
        let mapped: truffle_core::network::ProxyRoute = route.into();
        assert_eq!(
            mapped,
            truffle_core::network::ProxyRoute {
                prefix: "/".to_string(),
                target_url: None,
                dir: Some("/srv/public".to_string()),
                fallback: Some("/index.html".to_string()),
                strip_prefix: false,
                allow: vec![],
            }
        );
    }

    #[test]
    fn camel_case_wire_keys_deserialize_and_set_values_pass_through() {
        // The frontend speaks camelCase (targetUrl / stripPrefix); confirm
        // serde renames them and every set value survives the mapping.
        let route: ProxyRouteJs = serde_json::from_value(serde_json::json!({
            "prefix": "/api",
            "targetUrl": "http://localhost:8000",
            "stripPrefix": true,
            "allow": ["ops@corp.com"],
        }))
        .unwrap();
        let mapped: truffle_core::network::ProxyRoute = route.into();
        assert_eq!(
            mapped,
            truffle_core::network::ProxyRoute {
                prefix: "/api".to_string(),
                target_url: Some("http://localhost:8000".to_string()),
                dir: None,
                fallback: None,
                strip_prefix: true,
                allow: vec!["ops@corp.com".to_string()],
            }
        );
    }
}
