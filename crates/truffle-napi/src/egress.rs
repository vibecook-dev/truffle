//! RFC 026 egress — the plain-object shapes behind `NapiNode.loopback()`,
//! `loopbackWithLocalApi()`, `egressAdd()`, `egressRemove()`,
//! `egressList()` and `egressHttpProxy()`.
//!
//! Every address is `127.0.0.1:<port>`. The credentials are secrets: hand
//! them only to what may act as this node.

use napi_derive::napi;

use truffle_core::egress::{
    BoundEgress, EgressRoute, EgressRouteInfo, HttpProxyConfig, LoopbackConfig,
    LOOPBACK_SOCKS5_USERNAME,
};

/// The node's loopback proxy (RFC 026 E1): SOCKS5 onto the tailnet, and
/// tsnet's LocalAPI, on one listener.
#[napi(object)]
pub struct NapiLoopbackConfig {
    /// `127.0.0.1:<port>`.
    pub address: String,
    /// The SOCKS5 username — always `"tsnet"` (tsnet's own constant).
    pub proxy_username: String,
    /// The SOCKS5 password.
    pub proxy_credential: String,
    /// The LocalAPI Basic password; present only from
    /// `loopbackWithLocalApi()` (that LocalAPI is read-write).
    pub local_api_credential: Option<String>,
}

impl From<LoopbackConfig> for NapiLoopbackConfig {
    fn from(c: LoopbackConfig) -> Self {
        Self {
            address: c.address,
            proxy_username: LOOPBACK_SOCKS5_USERNAME.to_string(),
            proxy_credential: c.proxy_credential,
            local_api_credential: c.local_api_credential,
        }
    }
}

/// An egress route to bind (RFC 026 E2).
#[napi(object)]
pub struct NapiEgressRoute {
    /// Caller-chosen id, unique among the node's routes.
    pub id: String,
    /// `<peer-fqdn-or-tailnet-ip>:<port>`, fixed for the route. The host
    /// must name a node of this node's tailnet; a bare `.ts.net` suffix is
    /// not enough.
    pub target: String,
    /// Optional owner-login glob narrowing the node's own `loginAllow` gate.
    pub login: Option<String>,
}

impl From<NapiEgressRoute> for EgressRoute {
    fn from(r: NapiEgressRoute) -> Self {
        Self {
            id: r.id,
            target: r.target,
            login: r.login,
        }
    }
}

/// A bound egress route: connect to `listen` to reach the route's target.
#[napi(object)]
pub struct NapiBoundEgress {
    pub id: String,
    /// `127.0.0.1:<port>`.
    pub listen: String,
}

impl From<BoundEgress> for NapiBoundEgress {
    fn from(b: BoundEgress) -> Self {
        Self {
            id: b.id,
            listen: b.listen,
        }
    }
}

/// One live egress route, as `egressList()` reports it.
#[napi(object)]
pub struct NapiEgressRouteInfo {
    pub id: String,
    pub target: String,
    /// `127.0.0.1:<port>`.
    pub listen: String,
    pub login: Option<String>,
}

impl From<EgressRouteInfo> for NapiEgressRouteInfo {
    fn from(r: EgressRouteInfo) -> Self {
        Self {
            id: r.id,
            target: r.target,
            listen: r.listen,
            login: r.login,
        }
    }
}

/// The node's HTTP CONNECT proxy (RFC 026 E3): CONNECT only, to nodes of
/// this tailnet only, `Proxy-Authorization: Basic` with any username and
/// `credential` as the password.
#[napi(object)]
pub struct NapiHttpProxyConfig {
    /// `127.0.0.1:<port>`.
    pub address: String,
    /// The Basic password.
    pub credential: String,
}

impl From<HttpProxyConfig> for NapiHttpProxyConfig {
    fn from(c: HttpProxyConfig) -> Self {
        Self {
            address: c.address,
            credential: c.credential,
        }
    }
}
