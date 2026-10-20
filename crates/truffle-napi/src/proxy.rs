//! NapiProxy — Node.js wrapper for reverse proxy operations.

use std::sync::Arc;

use napi::bindgen_prelude::*;
use napi::threadsafe_function::{ThreadsafeFunction, ThreadsafeFunctionCallMode};
use napi::{Status, Unknown};
use napi_derive::napi;

use truffle_core::network::tailscale::TailscaleProvider;
use truffle_core::proxy::{ProxyEvent, ProxyStatus};
use truffle_core::Node;

use crate::subscription::NapiSubscription;

// ---------------------------------------------------------------------------
// NAPI types
// ---------------------------------------------------------------------------

/// Configuration for adding a reverse proxy, received from JavaScript.
#[napi(object)]
pub struct NapiProxyConfig {
    /// Unique identifier (user-chosen or auto-generated).
    pub id: String,
    /// Human-readable name for the proxy.
    pub name: String,
    /// Port on which the proxy listens on the tailnet (NAPI uses i32).
    pub listen_port: i32,
    /// Target host (default: "localhost").
    pub target_host: Option<String>,
    /// Target port (the local service port).
    pub target_port: i32,
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
    pub routes: Option<Vec<NapiProxyRoute>>,
}

/// One path-prefix route of a v2 proxy (RFC 023 §7). Exactly one of
/// `targetUrl` / `dir` is set; longest prefix wins. Validation lives in
/// core `validate_config`, not here.
#[napi(object)]
pub struct NapiProxyRoute {
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

impl From<NapiProxyRoute> for truffle_core::network::ProxyRoute {
    fn from(r: NapiProxyRoute) -> Self {
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

/// Information about a running or configured proxy, returned to JavaScript.
#[napi(object)]
pub struct NapiProxyInfo {
    pub id: String,
    pub name: String,
    pub listen_port: i32,
    pub target_host: String,
    pub target_port: i32,
    pub target_scheme: String,
    pub url: String,
    /// Status: "starting", "running", "stopped", or "error: <message>".
    pub status: String,
}

impl From<truffle_core::proxy::ProxyInfo> for NapiProxyInfo {
    fn from(info: truffle_core::proxy::ProxyInfo) -> Self {
        Self {
            id: info.id,
            name: info.name,
            listen_port: info.listen_port as i32,
            target_host: info.target.host,
            target_port: info.target.port as i32,
            target_scheme: info.target.scheme,
            url: info.url,
            status: match info.status {
                ProxyStatus::Starting => "starting".to_string(),
                ProxyStatus::Running => "running".to_string(),
                ProxyStatus::Stopped => "stopped".to_string(),
                ProxyStatus::Error(msg) => format!("error: {msg}"),
            },
        }
    }
}

/// A proxy lifecycle event delivered to JavaScript.
#[napi(object)]
pub struct NapiProxyEvent {
    /// Event type: "started", "stopped", "error".
    pub event_type: String,
    /// Proxy ID.
    pub id: String,
    /// Fully qualified URL (present for "started" events).
    pub url: Option<String>,
    /// Listen port (present for "started" events).
    pub listen_port: Option<i32>,
    /// Error code (present for "error" events).
    pub code: Option<String>,
    /// Error message (present for "error" events).
    pub message: Option<String>,
}

impl From<ProxyEvent> for NapiProxyEvent {
    fn from(event: ProxyEvent) -> Self {
        match event {
            ProxyEvent::Started {
                id,
                url,
                listen_port,
            } => Self {
                event_type: "started".to_string(),
                id,
                url: Some(url),
                listen_port: Some(listen_port as i32),
                code: None,
                message: None,
            },
            ProxyEvent::Stopped { id } => Self {
                event_type: "stopped".to_string(),
                id,
                url: None,
                listen_port: None,
                code: None,
                message: None,
            },
            ProxyEvent::Error { id, code, message } => Self {
                event_type: "error".to_string(),
                id,
                url: None,
                listen_port: None,
                code: Some(code),
                message: Some(message),
            },
        }
    }
}

// ---------------------------------------------------------------------------
// NapiProxy handle
// ---------------------------------------------------------------------------

/// Reverse proxy handle exposed to JavaScript.
///
/// Obtained via `NapiNode.proxy()`. Holds an `Arc<Node>` to create
/// fresh `Proxy` handles on each method call (the Rust `Proxy` borrows
/// `&Node`, so we can't store it across JS boundaries).
#[napi]
pub struct NapiProxy {
    node: Arc<Node<TailscaleProvider>>,
}

impl NapiProxy {
    pub(crate) fn new(node: Arc<Node<TailscaleProvider>>) -> Self {
        Self { node }
    }
}

#[napi]
impl NapiProxy {
    /// Add a reverse proxy that forwards traffic from a TLS port on the mesh
    /// to a local target.
    #[napi]
    pub async fn add(&self, config: NapiProxyConfig) -> Result<NapiProxyInfo> {
        let listen_port = u16::try_from(config.listen_port).map_err(|_| {
            Error::from_reason(format!("listen_port out of range: {}", config.listen_port))
        })?;
        let target_port = u16::try_from(config.target_port).map_err(|_| {
            Error::from_reason(format!("target_port out of range: {}", config.target_port))
        })?;

        let proxy = self.node.proxy();
        let core_config = truffle_core::proxy::ProxyConfig {
            id: config.id,
            name: config.name,
            listen_port,
            target: truffle_core::proxy::ProxyTarget {
                host: config
                    .target_host
                    .unwrap_or_else(|| "localhost".to_string()),
                port: target_port,
                scheme: config.target_scheme.unwrap_or_else(|| "http".to_string()),
            },
            announce: config.announce.unwrap_or(true),
            // RFC 023 v2 fields. Core `validate_config` (called in
            // `Proxy::add`) owns the shape rules — this only maps.
            tls: config.tls.unwrap_or(true),
            allow_non_loopback: config.allow_non_loopback.unwrap_or(false),
            allow: config.allow.unwrap_or_default(),
            routes: config
                .routes
                .unwrap_or_default()
                .into_iter()
                .map(Into::into)
                .collect(),
            // Not exposed to JS yet (protocol 9 capability forwarding).
            accept_app_capabilities: Vec::new(),
        };
        let info = proxy
            .add(core_config)
            .await
            .map_err(|e| Error::from_reason(e.to_string()))?;
        Ok(NapiProxyInfo::from(info))
    }

    /// Remove a reverse proxy by ID.
    #[napi]
    pub async fn remove(&self, id: String) -> Result<()> {
        let proxy = self.node.proxy();
        proxy
            .remove(&id)
            .await
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// List all active proxies on this node.
    #[napi]
    pub async fn list(&self) -> Result<Vec<NapiProxyInfo>> {
        let proxy = self.node.proxy();
        Ok(proxy.list().into_iter().map(NapiProxyInfo::from).collect())
    }

    /// Subscribe to proxy lifecycle events.
    ///
    /// The callback receives `NapiProxyEvent` objects whenever a proxy
    /// starts, stops, or encounters an error. Call `close()` on the returned
    /// subscription when the listener is no longer needed.
    #[napi(ts_args_type = "callback: (event: NapiProxyEvent) => void")]
    pub fn on_event(
        &self,
        callback: ThreadsafeFunction<
            NapiProxyEvent,
            Unknown<'static>,
            NapiProxyEvent,
            Status,
            false,
        >,
    ) -> Result<NapiSubscription> {
        let mut rx = self.node.proxy().subscribe();

        let handle = napi::bindgen_prelude::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(event) => {
                        let napi_event = NapiProxyEvent::from(event);
                        let status =
                            callback.call(napi_event, ThreadsafeFunctionCallMode::NonBlocking);
                        if status != Status::Ok {
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(missed = n, "proxy on_event lagged");
                        continue;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        });

        Ok(NapiSubscription::from_task(handle))
    }
}
