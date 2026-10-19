//! Public types for the reverse proxy subsystem.

use serde::{Deserialize, Serialize};

/// Re-exported from the network layer: the wire-shaped path route
/// (RFC 023 §7). Defined there because the provider command payloads embed
/// it verbatim.
pub use crate::network::ProxyRoute;

/// Target backend for the reverse proxy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProxyTarget {
    /// Target host (e.g. "localhost", "127.0.0.1")
    pub host: String,
    /// Target port (e.g. 3000)
    pub port: u16,
    /// Target scheme ("http" or "https")
    pub scheme: String,
}

impl Default for ProxyTarget {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 0,
            scheme: "http".to_string(),
        }
    }
}

/// Configuration for adding a reverse proxy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProxyConfig {
    /// Unique identifier (user-chosen or auto-generated).
    pub id: String,
    /// Human-readable name.
    pub name: String,
    /// Port on which the proxy listens on the tailnet.
    pub listen_port: u16,
    /// Backend target to forward requests to. Ignored when `routes` is
    /// non-empty (routes replace the single-target shape).
    #[serde(default)]
    pub target: ProxyTarget,
    /// Whether to announce this proxy on the mesh for discovery.
    ///
    /// Currently a no-op — discovery ships in RFC 023 Phase 5, at which
    /// point the default flips to `false` (D15: no surprise broadcasts).
    #[serde(default = "default_true")]
    pub announce: bool,
    /// Terminate TLS on the tailnet listener with automatic MagicDNS
    /// certificates. Default `true` preserves the shipped v1 behavior (the
    /// audience is browsers); disabling requires a v2 sidecar (RFC 023).
    #[serde(default = "default_true")]
    pub tls: bool,
    /// Permit non-loopback targets (RFC 023 §9.3). Default deny: a proxy
    /// with a LAN target turns this node into a pivot into its network.
    #[serde(default)]
    pub allow_non_loopback: bool,
    /// loginName allow globs (RFC 023 §9.7), e.g. `["*@corp.com"]`. Empty =
    /// the whole tailnet (subject to tailnet ACLs). Evaluated by the sidecar
    /// against the connection's WhoIs identity; non-matching requests get a
    /// bare 403, on the proxy and WebSocket-hijack paths alike.
    #[serde(default)]
    pub allow: Vec<String>,
    /// Path-prefix routes (RFC 023 §7). Empty = single-target v1 shape.
    /// Longest prefix wins, order-independent (D11).
    #[serde(default)]
    pub routes: Vec<ProxyRoute>,
    /// Application capabilities (tailnet policy grants, `app` section) to
    /// forward to the backend, by name (e.g. `example.com/cap/media`). For
    /// each request the sidecar sends the caller's grants for these names as
    /// JSON in a `Tailscale-App-Capabilities` header — the same header and
    /// shape as `tailscale serve --accept-app-caps` — after stripping any
    /// inbound `Tailscale-*` header. Empty (the default) sends no header.
    /// Needs sidecar protocol 9; `add` fails on an older sidecar rather than
    /// silently forwarding nothing.
    #[serde(default)]
    pub accept_app_capabilities: Vec<String>,
}

fn default_true() -> bool {
    true
}

/// Information about a running or configured proxy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProxyInfo {
    pub id: String,
    pub name: String,
    pub listen_port: u16,
    pub target: ProxyTarget,
    /// Fully qualified URL (e.g. "<https://hostname.ts.net:3001>")
    pub url: String,
    pub status: ProxyStatus,
}

/// Status of a proxy.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ProxyStatus {
    Starting,
    Running,
    Stopped,
    Error(String),
}

/// Events emitted by the proxy subsystem.
#[derive(Debug, Clone)]
pub enum ProxyEvent {
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

/// A proxy announced by a remote peer (used for mesh discovery).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProxyAnnouncement {
    pub id: String,
    pub name: String,
    pub listen_port: u16,
    pub url: String,
}

/// A remote proxy discovered via the mesh.
#[derive(Debug, Clone)]
pub struct RemoteProxy {
    /// Which peer is hosting this proxy.
    pub peer_id: String,
    /// Human-readable peer name.
    pub peer_name: String,
    /// Proxy ID on the remote peer.
    pub id: String,
    /// Human-readable proxy name.
    pub name: String,
    /// Fully qualified URL.
    pub url: String,
    /// Listen port on the remote peer's tailnet address.
    pub listen_port: u16,
}
