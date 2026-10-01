//! Egress for the node's own host processes (RFC 026).
//!
//! A node serves IN — [`Node::proxy`](crate::Node::proxy) publishes a
//! loopback service onto the tailnet — and, with RFC 026, lets the other
//! processes on its host reach OUT through it, as this node:
//!
//! - **E1** [`Node::loopback`](crate::Node::loopback): tsnet's own loopback
//!   SOCKS5 proxy onto the tailnet, password-authenticated
//!   ([`LoopbackConfig`]).
//! - **E2** [`Node::egress_add`](crate::Node::egress_add): a loopback listener
//!   with a FIXED tailnet target; every connection to it is admitted, dialled
//!   through the node, and spliced raw — a WebSocket upgrade passes intact
//!   ([`EgressRoute`] → [`BoundEgress`]).
//! - **E3** [`Node::egress_http_proxy`](crate::Node::egress_http_proxy): a
//!   loopback HTTP CONNECT proxy for clients that cannot speak SOCKS5 — a
//!   browser answers its Basic challenge ([`HttpProxyConfig`]).
//!
//! Every listener binds `127.0.0.1`. E2 and E3 reach only nodes of this
//! node's netmap whose owner login passes the node's
//! [`login_allow`](crate::NodeBuilder::login_allow) gate (RFC 025), and they
//! re-check at every connection. E1 is tsnet's own server: with its
//! credential it dials anything the node can, so the credential is its whole
//! gate. Everything here needs sidecar protocol 6; an older sidecar is
//! refused locally with [`NetworkError::Unsupported`](crate::network::NetworkError::Unsupported).

/// The SOCKS5 username tsnet's loopback proxy requires. The password is
/// [`LoopbackConfig::proxy_credential`].
pub const LOOPBACK_SOCKS5_USERNAME: &str = "tsnet";

/// The node's loopback proxy (RFC 026 E1).
///
/// One `127.0.0.1` listener serves both SOCKS5 (username
/// [`LOOPBACK_SOCKS5_USERNAME`], password `proxy_credential`) and tsnet's
/// LocalAPI. It lives exactly as long as the node's sidecar lifecycle: every
/// call answers the same values until the node stops, and a restart mints
/// new ones.
#[derive(Clone, PartialEq, Eq)]
pub struct LoopbackConfig {
    /// `127.0.0.1:<port>`.
    pub address: String,
    /// The SOCKS5 password.
    pub proxy_credential: String,
    /// The LocalAPI Basic password. `None` unless asked for with
    /// [`Node::loopback_with_local_api`](crate::Node::loopback_with_local_api):
    /// that LocalAPI is read-write (preferences, logout), so its credential
    /// leaves the sidecar only on request.
    pub local_api_credential: Option<String>,
}

/// Manual `Debug`: both credentials are secrets and must never reach logs.
impl std::fmt::Debug for LoopbackConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoopbackConfig")
            .field("address", &self.address)
            .field("proxy_credential", &"[REDACTED]")
            .field(
                "local_api_credential",
                &self.local_api_credential.as_ref().map(|_| "[REDACTED]"),
            )
            .finish()
    }
}

/// An egress route to bind (RFC 026 E2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EgressRoute {
    /// Caller-chosen id, unique among the node's routes.
    pub id: String,
    /// `<peer-fqdn-or-tailnet-ip>:<port>`, fixed for the route's life. The
    /// host must name a node of this node's netmap — by MagicDNS name or
    /// tailnet IP; a name that merely ends in `.ts.net` is refused.
    pub target: String,
    /// Optional owner-login glob (RFC 025 §3.2 grammar) narrowing the node's
    /// own gate: the target's owner must pass both.
    pub login: Option<String>,
}

/// A bound egress route (RFC 026 E2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundEgress {
    /// The route's id.
    pub id: String,
    /// `127.0.0.1:<port>` — connect here to reach the route's target.
    pub listen: String,
}

/// One live route, as [`Node::egress_list`](crate::Node::egress_list)
/// reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EgressRouteInfo {
    /// The route's id.
    pub id: String,
    /// The fixed target, as given to `egress_add`.
    pub target: String,
    /// `127.0.0.1:<port>`.
    pub listen: String,
    /// The route's own owner glob, if it has one.
    pub login: Option<String>,
}

/// The node's HTTP CONNECT proxy (RFC 026 E3).
///
/// CONNECT only, to nodes of this tailnet only; `Proxy-Authorization: Basic`
/// with any username and `credential` as the password is required (a
/// missing or wrong one is answered `407` with a Basic challenge). One per
/// sidecar lifecycle: every call answers the same values until the node
/// stops.
#[derive(Clone, PartialEq, Eq)]
pub struct HttpProxyConfig {
    /// `127.0.0.1:<port>`.
    pub address: String,
    /// The Basic password.
    pub credential: String,
}

/// Manual `Debug`: the credential is a secret and must never reach logs.
impl std::fmt::Debug for HttpProxyConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpProxyConfig")
            .field("address", &self.address)
            .field("credential", &"[REDACTED]")
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_redacts_every_credential() {
        let loopback = LoopbackConfig {
            address: "127.0.0.1:41641".into(),
            proxy_credential: "PROXY-SECRET".into(),
            local_api_credential: Some("LOCALAPI-SECRET".into()),
        };
        let proxy = HttpProxyConfig {
            address: "127.0.0.1:41642".into(),
            credential: "CONNECT-SECRET".into(),
        };
        let dbg = format!("{loopback:?} {proxy:?}");
        for secret in ["PROXY-SECRET", "LOCALAPI-SECRET", "CONNECT-SECRET"] {
            assert!(!dbg.contains(secret), "{secret} leaked into Debug: {dbg}");
        }
        assert!(dbg.contains("127.0.0.1:41641") && dbg.contains("127.0.0.1:41642"));
        assert!(format!(
            "{:?}",
            LoopbackConfig {
                local_api_credential: None,
                ..loopback
            }
        )
        .contains("local_api_credential: None"));
    }
}
