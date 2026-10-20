//! TCP transport — [`RawTransport`] implementation over plain TCP.
//!
//! This is a thin wrapper around Layer 3's [`NetworkProvider`] that provides
//! the [`RawTransport`] trait boundary so that Layer 5 and Layer 7 code
//! does not depend on `NetworkProvider` directly.
//!
//! # Connection flow
//!
//! - `open(addr, port)` → delegates to `NetworkProvider::dial_tcp(addr, port)`
//! - `listen(port)` → delegates to `NetworkProvider::listen_tcp(port)` and
//!   wraps incoming connections as a [`RawListener`]
//!
//! No protocol upgrade, no framing, no handshake. The caller gets a raw
//! `TcpStream` for byte-oriented I/O.

use std::sync::Arc;

use crate::network::{ListenOpts, NetworkProvider, PeerAddr, TailscalePeerIdentity};

use super::{resolve_dial_addr, RawIncoming, RawListener, RawTransport, TransportError};

// ---------------------------------------------------------------------------
// TcpTransport
// ---------------------------------------------------------------------------

/// Plain TCP [`RawTransport`] implementation.
///
/// Generic over the [`NetworkProvider`] type `N`. Delegates all connectivity
/// to Layer 3. This struct exists to provide the trait boundary — Layer 5/7
/// code depends on `RawTransport`, not `NetworkProvider`.
///
/// # Example
///
/// ```ignore
/// use std::sync::Arc;
/// use truffle_core::transport::tcp::TcpTransport;
///
/// let tcp = TcpTransport::new(Arc::new(provider));
/// let stream = tcp.open(&peer_addr, 8080).await?;
/// ```
pub struct TcpTransport<N: NetworkProvider> {
    /// Layer 3 network provider.
    network: Arc<N>,
}

impl<N: NetworkProvider + 'static> TcpTransport<N> {
    /// Create a new TCP transport backed by the given network provider.
    pub fn new(network: Arc<N>) -> Self {
        Self { network }
    }

    /// As [`RawTransport::listen`], with listener options (RFC 023 §7.1) —
    /// an inherent method because the trait keeps the plain shape.
    pub async fn listen_opts(
        &self,
        port: u16,
        opts: ListenOpts,
    ) -> Result<RawListener, TransportError> {
        tracing::debug!(port, tls = opts.tls, "tcp: starting listener");

        let mut tcp_listener = self
            .network
            .listen_tcp_opts(port, opts)
            .await
            .map_err(|e| TransportError::ListenFailed(format!("tcp listen: {e}")))?;

        // Use the actual port from the NetworkTcpListener (important when
        // the requested port is 0 and the OS assigns an ephemeral port).
        let actual_port = tcp_listener.port;

        // Spawn a task that forwards incoming connections from the
        // NetworkTcpListener channel to the RawListener channel.
        let (tx, rx) = tokio::sync::mpsc::channel::<RawIncoming>(64);

        tokio::spawn(async move {
            loop {
                match tcp_listener.incoming.recv().await {
                    Some(incoming) => {
                        let raw = RawIncoming {
                            stream: incoming.stream,
                            remote_addr: incoming.remote_addr,
                            remote_identity: parse_remote_identity(&incoming.remote_identity),
                        };
                        if tx.send(raw).await.is_err() {
                            tracing::debug!("tcp: listener channel closed");
                            break;
                        }
                    }
                    None => {
                        tracing::debug!("tcp: network listener channel closed");
                        break;
                    }
                }
            }
        });

        Ok(RawListener::new(rx, actual_port))
    }
}

impl<N: NetworkProvider + 'static> RawTransport for TcpTransport<N> {
    async fn open(
        &self,
        addr: &PeerAddr,
        port: u16,
    ) -> Result<tokio::net::TcpStream, TransportError> {
        let dial_addr = resolve_dial_addr(addr);
        tracing::debug!(addr = %dial_addr, port, "tcp: dialing peer");

        let stream = self
            .network
            .dial_tcp(&dial_addr, port)
            .await
            .map_err(|e| TransportError::ConnectFailed(format!("tcp dial: {e}")))?;

        tracing::debug!(addr = %dial_addr, port, "tcp: connected");
        Ok(stream)
    }

    async fn listen(&self, port: u16) -> Result<RawListener, TransportError> {
        self.listen_opts(port, ListenOpts::default()).await
    }
}

/// Parse the bridge header's WhoIs identity into a [`TailscalePeerIdentity`].
///
/// Layer 3 delivers either a WhoIs identity JSON blob or, from legacy sidecars,
/// a bare DNS name. Empty, whitespace-only, or unparseable input yields `None`:
/// a missing or malformed identity is never an error on the raw path, only an
/// absence of authenticated metadata.
fn parse_remote_identity(raw: &str) -> Option<TailscalePeerIdentity> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    match serde_json::from_str::<TailscalePeerIdentity>(trimmed) {
        Ok(identity) => Some(identity),
        Err(e) => {
            tracing::debug!(error = %e, "tcp: unparseable remote identity header; treating as anonymous");
            None
        }
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn parse_remote_identity_round_trips_whois_json() {
        // A realistic WhoIs blob as the sidecar's resolvePeerIdentity writes it
        // into the bridge header.
        let json = r#"{"dnsName":"kitchen.tailnet.ts.net","loginName":"alice@example.com","displayName":"Alice","profilePicUrl":"https://p.example/a.png","nodeId":"nABC123.ts-node"}"#;
        let identity = parse_remote_identity(json).expect("well-formed WhoIs JSON must parse");
        assert_eq!(
            identity,
            TailscalePeerIdentity {
                dns_name: Some("kitchen.tailnet.ts.net".to_string()),
                login_name: Some("alice@example.com".to_string()),
                display_name: Some("Alice".to_string()),
                profile_pic_url: Some("https://p.example/a.png".to_string()),
                node_id: Some("nABC123.ts-node".to_string()),
                ..Default::default()
            }
        );
    }

    #[test]
    fn parse_remote_identity_carries_cap_map_tags_and_user_id() {
        // Protocol 9 sidecars add userId, tags and the app-capability map.
        let json = r#"{"dnsName":"tv.tailnet.ts.net","loginName":"tagged-devices","nodeId":"nTV","userId":4242,"tags":["tag:tv","tag:kids"],"capMap":{"vibecook.dev/cap/flux":[{"role":"kid"},{"libraries":["Cartoons"]}]}}"#;
        let identity = parse_remote_identity(json).expect("v9 WhoIs JSON must parse");
        assert_eq!(identity.user_id, Some(4242));
        assert_eq!(identity.tags, vec!["tag:tv", "tag:kids"]);
        assert!(identity.is_tagged());
        assert!(!identity.cap_map_dropped);
        let values = identity
            .capability_values("vibecook.dev/cap/flux")
            .expect("capability present");
        assert_eq!(values.len(), 2);
        assert_eq!(values[0]["role"], "kid");

        #[derive(serde::Deserialize, Debug, PartialEq)]
        struct Rule {
            role: Option<String>,
            #[serde(default)]
            libraries: Vec<String>,
        }
        let rules: Vec<Rule> = identity.capability("vibecook.dev/cap/flux").unwrap();
        assert_eq!(rules[0].role.as_deref(), Some("kid"));
        assert_eq!(rules[1].libraries, vec!["Cartoons"]);
        let none: Vec<Rule> = identity.capability("example.com/cap/absent").unwrap();
        assert!(none.is_empty());
        // A value that does not fit the type fails closed instead of vanishing.
        assert!(identity.capability::<u32>("vibecook.dev/cap/flux").is_err());
    }

    #[test]
    fn parse_remote_identity_reports_a_dropped_cap_map() {
        let identity = parse_remote_identity(
            r#"{"dnsName":"peer.ts.net","loginName":"alice@example.com","capMapDropped":true}"#,
        )
        .expect("must parse");
        assert!(identity.cap_map.is_empty());
        assert!(identity.cap_map_dropped);
    }

    #[test]
    fn parse_remote_identity_omits_absent_optional_fields() {
        // The sidecar drops empty optional fields (omitempty); absent fields
        // deserialize to None rather than failing the whole parse.
        let identity = parse_remote_identity(r#"{"dnsName":"peer.ts.net"}"#)
            .expect("partial WhoIs JSON must parse");
        assert_eq!(identity.dns_name.as_deref(), Some("peer.ts.net"));
        assert_eq!(identity.login_name, None);
        assert_eq!(identity.node_id, None);
        // Pre-v9 sidecars send none of the protocol 9 fields.
        assert_eq!(identity.user_id, None);
        assert!(identity.tags.is_empty());
        assert!(identity.cap_map.is_empty());
        assert!(!identity.cap_map_dropped);
    }

    #[test]
    fn parse_remote_identity_returns_none_for_malformed_json() {
        // A legacy bare DNS name (or any non-JSON) is not an error — it just
        // means no authenticated identity metadata is available.
        assert_eq!(parse_remote_identity("peer.tailnet.ts.net"), None);
        assert_eq!(parse_remote_identity("{not json"), None);
    }

    #[test]
    fn parse_remote_identity_returns_none_for_empty_input() {
        // MockNetworkProvider and WhoIs failures deliver "".
        assert_eq!(parse_remote_identity(""), None);
        assert_eq!(parse_remote_identity("   "), None);
    }
}
