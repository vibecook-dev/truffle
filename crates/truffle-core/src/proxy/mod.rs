//! Reverse proxy subsystem.
//!
//! Exposes local HTTP/HTTPS ports to the Tailscale mesh network.
//! The Go sidecar handles the actual HTTP reverse proxying (TLS listener,
//! `httputil.ReverseProxy`, WebSocket hijack). This module provides the
//! Rust API surface and mesh discovery.

pub mod discovery;
pub mod types;

use std::collections::HashMap;
use std::sync::Mutex;
use tokio::sync::broadcast;

pub use types::*;

use crate::network::{NetworkProvider, ProxyAddParams};
use crate::node::{Node, NodeError};

/// Internal state for the proxy subsystem, stored in [`Node`].
///
/// Runtime sidecar errors (`SERVE_ERROR`, `CONNECTION_REFUSED`, …) reach
/// `event_tx` through a node-scoped forwarding task that subscribes to the
/// provider's [`proxy_runtime_errors`](crate::network::NetworkProvider::proxy_runtime_errors)
/// stream (RFC 023 G5 fix) — see `Node::spawn_proxy_error_forwarder`.
pub(crate) struct ProxyState {
    /// Broadcast channel for proxy lifecycle events.
    pub(crate) event_tx: broadcast::Sender<ProxyEvent>,
    /// Currently active proxies (local to this node).
    pub(crate) proxies: Mutex<HashMap<String, ProxyInfo>>,
}

impl ProxyState {
    pub(crate) fn new() -> Self {
        let (event_tx, _) = broadcast::channel(64);
        Self {
            event_tx,
            proxies: Mutex::new(HashMap::new()),
        }
    }

    /// Record a runtime engine error: mark the proxy `Error` and emit
    /// [`ProxyEvent::Error`]. Errors for ids that never started are dropped
    /// — add-time failures are returned by [`Proxy::add`] itself, and
    /// re-emitting them here would announce a proxy that never existed.
    pub(crate) fn record_runtime_error(&self, id: String, code: String, message: String) {
        {
            let mut proxies = self.proxies.lock().expect("proxy state poisoned");
            match proxies.get_mut(&id) {
                Some(info) => info.status = ProxyStatus::Error(format!("[{code}] {message}")),
                None => return,
            }
        }
        let _ = self.event_tx.send(ProxyEvent::Error { id, code, message });
    }
}

/// Handle for interacting with the reverse proxy subsystem.
///
/// Obtained via [`Node::proxy()`]. Follows the same pattern as
/// [`FileTransfer`](crate::file_transfer::FileTransfer).
pub struct Proxy<'a, N: NetworkProvider + 'static> {
    node: &'a Node<N>,
}

impl<'a, N: NetworkProvider + 'static> Proxy<'a, N> {
    pub(crate) fn new(node: &'a Node<N>) -> Self {
        Self { node }
    }

    fn state(&self) -> &ProxyState {
        &self.node.proxy_state
    }

    /// Subscribe to proxy lifecycle events.
    pub fn subscribe(&self) -> broadcast::Receiver<ProxyEvent> {
        self.state().event_tx.subscribe()
    }

    /// Get a snapshot of all locally active proxies.
    pub fn list(&self) -> Vec<ProxyInfo> {
        self.state()
            .proxies
            .lock()
            .expect("proxy state poisoned")
            .values()
            .cloned()
            .collect()
    }

    /// Add a reverse proxy that forwards traffic from a port on the mesh to
    /// a local target — or, with `routes`, to several targets and static
    /// directories by path prefix (RFC 023 §7).
    ///
    /// Sends the `proxy:add` command to the sidecar and waits for confirmation.
    /// On success, stores the proxy in local state and emits a
    /// [`ProxyEvent::Started`] event.
    pub async fn add(&self, config: ProxyConfig) -> Result<ProxyInfo, NodeError> {
        // RFC 023 G4: proxy listeners honor the same reserved-port rule as
        // raw listeners instead of failing at the sidecar OS bind.
        crate::node::ensure_port_unreserved(config.listen_port, self.node.ws_port)?;
        validate_config(&config).map_err(NodeError::ConnectionFailed)?;

        let params = ProxyAddParams {
            id: config.id.clone(),
            name: config.name.clone(),
            listen_port: config.listen_port,
            target_host: config.target.host.clone(),
            target_port: config.target.port,
            target_scheme: config.target.scheme.clone(),
            tls: config.tls,
            allow_non_loopback: config.allow_non_loopback,
            allow: config.allow.clone(),
            routes: config.routes.clone(),
            accept_app_capabilities: config.accept_app_capabilities.clone(),
        };

        let result = self.node.network.proxy_add(params).await?;

        let info = ProxyInfo {
            id: result.id.clone(),
            name: config.name,
            listen_port: result.listen_port,
            target: config.target,
            url: result.url.clone(),
            status: ProxyStatus::Running,
        };

        // Store in local state
        self.state()
            .proxies
            .lock()
            .expect("proxy state poisoned")
            .insert(info.id.clone(), info.clone());

        // Emit event
        let _ = self.state().event_tx.send(ProxyEvent::Started {
            id: result.id,
            url: result.url,
            listen_port: result.listen_port,
        });

        Ok(info)
    }

    /// Remove a reverse proxy by ID.
    ///
    /// Sends the `proxy:remove` command to the sidecar and waits for
    /// confirmation. On success, removes the proxy from local state and
    /// emits a [`ProxyEvent::Stopped`] event.
    pub async fn remove(&self, id: &str) -> Result<(), NodeError> {
        self.node.network.proxy_remove(id).await?;

        // Remove from local state
        self.state()
            .proxies
            .lock()
            .expect("proxy state poisoned")
            .remove(id);

        // Emit event
        let _ = self
            .state()
            .event_tx
            .send(ProxyEvent::Stopped { id: id.to_string() });

        Ok(())
    }
}

/// Config-shape validation shared by every binding (RFC 023 §7): route
/// prefixes are absolute, each route names exactly one backend, and option
/// combinations that would silently do nothing are rejected here rather
/// than half-applied by the engine.
fn validate_config(config: &ProxyConfig) -> Result<(), String> {
    for route in &config.routes {
        if !route.prefix.starts_with('/') {
            return Err(format!(
                "route prefix {:?} must start with '/'",
                route.prefix
            ));
        }
        match (&route.target_url, &route.dir) {
            (Some(_), Some(_)) => {
                return Err(format!(
                    "route {:?} sets both targetUrl and dir — pick one",
                    route.prefix
                ));
            }
            (None, None) => {
                return Err(format!(
                    "route {:?} needs a targetUrl or a dir",
                    route.prefix
                ));
            }
            _ => {}
        }
        if route.fallback.is_some() && route.dir.is_none() {
            return Err(format!(
                "route {:?}: fallback only applies to dir routes",
                route.prefix
            ));
        }
        if route.strip_prefix && route.target_url.is_none() {
            return Err(format!(
                "route {:?}: stripPrefix only applies to targetUrl routes",
                route.prefix
            ));
        }
    }
    if config.allow.iter().any(|glob| glob.trim().is_empty()) {
        return Err("allow globs must be non-empty strings".into());
    }
    if config
        .routes
        .iter()
        .flat_map(|r| r.allow.iter())
        .any(|glob| glob.trim().is_empty())
    {
        return Err("route allow globs must be non-empty strings".into());
    }
    if let Some(bad) = config
        .accept_app_capabilities
        .iter()
        .find(|name| !is_app_capability_name(name))
    {
        return Err(format!(
            "accept_app_capabilities entry {bad:?} is not a capability name ({{domain}}/{{path}})"
        ));
    }
    Ok(())
}

/// Tailscale application-capability names are `{domain}/{path}` (KB 1537).
/// Mirrors the sidecar's `validateAcceptAppCaps`: a name without a slash or
/// with whitespace/control characters cannot match a grant, so it is a typo
/// that would forward nothing.
fn is_app_capability_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 256
        && name.contains('/')
        && !name.chars().any(|c| c.is_whitespace() || c.is_control())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_config(routes: Vec<ProxyRoute>) -> ProxyConfig {
        ProxyConfig {
            id: "p".into(),
            name: "p".into(),
            listen_port: 8443,
            target: ProxyTarget::default(),
            announce: true,
            tls: true,
            allow_non_loopback: false,
            allow: vec![],
            routes,
            accept_app_capabilities: vec![],
        }
    }

    #[test]
    fn validate_accept_app_capabilities() {
        let mut config = base_config(vec![]);
        config.accept_app_capabilities = vec!["vibecook.dev/cap/flux".into()];
        assert!(validate_config(&config).is_ok());
        for bad in ["", "noslash", "has space/x", "tab\t/x"] {
            config.accept_app_capabilities = vec![bad.into()];
            assert!(validate_config(&config).is_err(), "{bad:?} accepted");
        }
    }

    fn url_route(prefix: &str) -> ProxyRoute {
        ProxyRoute {
            prefix: prefix.into(),
            target_url: Some("http://localhost:3000".into()),
            dir: None,
            fallback: None,
            strip_prefix: false,
            allow: vec![],
        }
    }

    #[test]
    fn validate_accepts_v1_shape_and_mixed_routes() {
        assert!(validate_config(&base_config(vec![])).is_ok());
        let dir_route = ProxyRoute {
            prefix: "/".into(),
            target_url: None,
            dir: Some("/srv/public".into()),
            fallback: Some("/index.html".into()),
            strip_prefix: false,
            allow: vec![],
        };
        assert!(validate_config(&base_config(vec![url_route("/api"), dir_route])).is_ok());
    }

    #[test]
    fn validate_rejects_bad_routes() {
        // Relative prefix.
        assert!(validate_config(&base_config(vec![url_route("api")])).is_err());
        // Both backends.
        let mut both = url_route("/x");
        both.dir = Some("/srv".into());
        assert!(validate_config(&base_config(vec![both])).is_err());
        // Neither backend.
        let mut neither = url_route("/x");
        neither.target_url = None;
        assert!(validate_config(&base_config(vec![neither])).is_err());
        // fallback without dir.
        let mut fb = url_route("/x");
        fb.fallback = Some("/index.html".into());
        assert!(validate_config(&base_config(vec![fb])).is_err());
        // stripPrefix on a dir route.
        let strip_dir = ProxyRoute {
            prefix: "/x".into(),
            target_url: None,
            dir: Some("/srv".into()),
            fallback: None,
            strip_prefix: true,
            allow: vec![],
        };
        assert!(validate_config(&base_config(vec![strip_dir])).is_err());
    }

    #[test]
    fn validate_rejects_empty_allow_globs() {
        let mut config = base_config(vec![]);
        config.allow = vec!["*@corp.com".into(), "  ".into()];
        assert!(validate_config(&config).is_err());

        let mut route = url_route("/api");
        route.allow = vec![String::new()];
        assert!(validate_config(&base_config(vec![route])).is_err());
    }

    #[test]
    fn runtime_errors_only_mark_known_proxies() {
        let state = ProxyState::new();
        let mut rx = state.event_tx.subscribe();

        // Unknown id: dropped (add-time failures are returned by add()).
        state.record_runtime_error("ghost".into(), "SERVE_ERROR".into(), "boom".into());
        assert!(rx.try_recv().is_err());

        state.proxies.lock().unwrap().insert(
            "web".into(),
            ProxyInfo {
                id: "web".into(),
                name: "web".into(),
                listen_port: 8443,
                target: ProxyTarget::default(),
                url: "https://x.ts.net:8443".into(),
                status: ProxyStatus::Running,
            },
        );
        state.record_runtime_error("web".into(), "CONNECTION_REFUSED".into(), "down".into());
        match rx.try_recv() {
            Ok(ProxyEvent::Error { id, code, .. }) => {
                assert_eq!(id, "web");
                assert_eq!(code, "CONNECTION_REFUSED");
            }
            other => panic!("expected Error event, got {other:?}"),
        }
        let proxies = state.proxies.lock().unwrap();
        assert!(matches!(
            proxies.get("web").unwrap().status,
            ProxyStatus::Error(_)
        ));
    }
}
