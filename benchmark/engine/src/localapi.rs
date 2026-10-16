//! A tiny client for the node's tsnet LocalAPI (RFC 027 §8.3, §11.2 sign-out).
//!
//! The sidecar serves the LocalAPI on its loopback listener
//! ([`Node::loopback_with_local_api`]). tsnet requires both a
//! `Sec-Tailscale: localapi` header and HTTP Basic auth whose password is the
//! LocalAPI credential (`tsnet.Server.Loopback`); the LocalAPI handler also
//! rejects requests that carry `Referer` or `Origin` and checks the `Host`.
//! That credential is read-write (it can log the node out), so it stays in
//! Rust: [`LocalApi`]'s `Debug` redacts it and nothing here logs it.
//!
//! Three endpoints are used: `GET /localapi/v0/usermetrics` (Prometheus
//! text; per-path byte and packet counters, [`UserMetrics`]),
//! `GET /localapi/v0/status?peers=false` (this node's own login, when the
//! sidecar's start event did not carry it; [`parse_self_login`]) and
//! `POST /localapi/v0/logout`.

use std::fmt;
use std::time::Duration;

use base64::Engine as _;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use truffle_core::network::NetworkProvider;
use truffle_core::Node;

use crate::error::{AppError, ErrorCode};
use crate::types::PathBytes;

/// Whole-request timeout.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
/// Largest response body accepted.
const MAX_RESPONSE: usize = 8 * 1024 * 1024;

/// LocalAPI endpoint and credential. `Debug` never shows the credential.
#[derive(Clone)]
pub struct LocalApi {
    address: String,
    credential: String,
}

impl fmt::Debug for LocalApi {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalApi")
            .field("address", &self.address)
            .field("credential", &"[REDACTED]")
            .finish()
    }
}

/// A raw LocalAPI response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Response {
    /// HTTP status.
    pub status: u16,
    /// Body (de-chunked).
    pub body: Vec<u8>,
}

impl LocalApi {
    /// An endpoint (`127.0.0.1:<port>`) and its LocalAPI credential.
    pub fn new(address: impl Into<String>, credential: impl Into<String>) -> Self {
        Self {
            address: address.into(),
            credential: credential.into(),
        }
    }

    /// The node's LocalAPI, via `loopback_with_local_api()`.
    pub async fn from_node<N: NetworkProvider + 'static>(node: &Node<N>) -> Result<Self, AppError> {
        let lb = node.loopback_with_local_api().await?;
        let credential = lb.local_api_credential.ok_or_else(|| {
            AppError::internal("the sidecar did not return a LocalAPI credential")
        })?;
        Ok(Self::new(lb.address, credential))
    }

    /// The loopback address.
    pub fn address(&self) -> &str {
        &self.address
    }

    /// One request; `Connection: close`, [`REQUEST_TIMEOUT`] overall.
    pub async fn request(&self, method: &str, path: &str) -> Result<Response, AppError> {
        tokio::time::timeout(REQUEST_TIMEOUT, self.request_inner(method, path))
            .await
            .map_err(|_| AppError::timeout(format!("LocalAPI {method} {path} timed out")))?
    }

    async fn request_inner(&self, method: &str, path: &str) -> Result<Response, AppError> {
        let auth =
            base64::engine::general_purpose::STANDARD.encode(format!(":{}", self.credential));
        let req = format!(
            "{method} {path} HTTP/1.1\r\nHost: {host}\r\nAuthorization: Basic {auth}\r\n\
             Sec-Tailscale: localapi\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            host = self.address,
        );
        let mut stream = TcpStream::connect(&self.address)
            .await
            .map_err(|e| AppError::io("LocalAPI connect", &e))?;
        stream
            .write_all(req.as_bytes())
            .await
            .map_err(|e| AppError::io("LocalAPI write", &e))?;
        let mut raw = Vec::new();
        (&mut stream)
            .take(MAX_RESPONSE as u64 + 64 * 1024)
            .read_to_end(&mut raw)
            .await
            .map_err(|e| AppError::io("LocalAPI read", &e))?;
        parse_response(&raw).map_err(|e| AppError::internal(format!("LocalAPI {path}: {e}")))
    }

    /// `GET /localapi/v0/usermetrics`, parsed.
    pub async fn usermetrics(&self) -> Result<UserMetrics, AppError> {
        let resp = self.request("GET", "/localapi/v0/usermetrics").await?;
        if resp.status != 200 {
            return Err(AppError::internal(format!(
                "LocalAPI usermetrics answered HTTP {}",
                resp.status
            )));
        }
        Ok(parse_usermetrics(&String::from_utf8_lossy(&resp.body)))
    }

    /// This node's login from `GET /localapi/v0/status?peers=false`: the
    /// `LoginName` of `Self.UserID` in the `User` map (`tagged-devices` for
    /// a tagged node). `Ok(None)` while the netmap does not name it yet.
    pub async fn self_login(&self) -> Result<Option<String>, AppError> {
        let resp = self
            .request("GET", "/localapi/v0/status?peers=false")
            .await?;
        if resp.status != 200 {
            return Err(AppError::internal(format!(
                "LocalAPI status answered HTTP {}",
                resp.status
            )));
        }
        let status: serde_json::Value = serde_json::from_slice(&resp.body)
            .map_err(|e| AppError::internal(format!("LocalAPI status is not JSON: {e}")))?;
        Ok(parse_self_login(&status))
    }

    /// `POST /localapi/v0/logout`: removes this node's login from the tailnet.
    pub async fn logout(&self) -> Result<(), AppError> {
        let resp = self.request("POST", "/localapi/v0/logout").await?;
        if resp.status == 200 || resp.status == 204 {
            return Ok(());
        }
        Err(AppError::new(
            ErrorCode::Internal,
            format!(
                "LocalAPI logout answered HTTP {}: {}",
                resp.status,
                String::from_utf8_lossy(&resp.body).trim()
            ),
        ))
    }
}

/// Parse an HTTP/1.x response read to EOF (handles `Content-Length` and
/// `Transfer-Encoding: chunked`).
pub fn parse_response(raw: &[u8]) -> Result<Response, String> {
    let head_end = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or("no end of headers")?;
    let head = std::str::from_utf8(&raw[..head_end]).map_err(|_| "headers are not UTF-8")?;
    let mut lines = head.split("\r\n");
    let status_line = lines.next().ok_or("empty response")?;
    let mut parts = status_line.split_whitespace();
    let version = parts.next().unwrap_or_default();
    if !version.starts_with("HTTP/1.") {
        return Err(format!("not an HTTP/1 response: {status_line}"));
    }
    let status: u16 = parts
        .next()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| format!("bad status line: {status_line}"))?;
    let mut chunked = false;
    let mut length = None;
    for line in lines {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        let value = value.trim();
        if name.eq_ignore_ascii_case("transfer-encoding") && value.eq_ignore_ascii_case("chunked") {
            chunked = true;
        } else if name.eq_ignore_ascii_case("content-length") {
            length = value.parse::<usize>().ok();
        }
    }
    let rest = &raw[head_end + 4..];
    let body = if chunked {
        dechunk(rest)?
    } else if let Some(n) = length {
        if rest.len() < n {
            return Err(format!("body truncated ({} of {n} bytes)", rest.len()));
        }
        rest[..n].to_vec()
    } else {
        rest.to_vec()
    };
    if body.len() > MAX_RESPONSE {
        return Err("response too large".into());
    }
    Ok(Response { status, body })
}

fn dechunk(mut data: &[u8]) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    loop {
        let line_end = data
            .windows(2)
            .position(|w| w == b"\r\n")
            .ok_or("truncated chunk header")?;
        let size_str = std::str::from_utf8(&data[..line_end]).map_err(|_| "bad chunk header")?;
        let size_hex = size_str.split(';').next().unwrap_or_default().trim();
        let size = usize::from_str_radix(size_hex, 16).map_err(|_| "bad chunk size")?;
        data = &data[line_end + 2..];
        if size == 0 {
            return Ok(out);
        }
        if data.len() < size + 2 {
            return Err("truncated chunk".into());
        }
        out.extend_from_slice(&data[..size]);
        data = &data[size + 2..];
    }
}

/// This node's login in an `ipnstate.Status` (`/localapi/v0/status`):
/// `User[Self.UserID].LoginName`, checked against tailscale v1.104.1
/// (`ipn/ipnstate.Status.Self`/`.User`, `tailcfg.UserProfile.LoginName`;
/// the self profile is present even with `peers=false`, tailscale#19894).
/// The `User` map's keys are the decimal user ids. `None` when the status
/// has no self node, its user is 0 (not logged in), or the profile is
/// missing or empty.
pub fn parse_self_login(status: &serde_json::Value) -> Option<String> {
    let id = status.get("Self")?.get("UserID")?.as_i64()?;
    if id == 0 {
        return None;
    }
    let login = status
        .get("User")?
        .get(id.to_string())?
        .get("LoginName")?
        .as_str()?
        .trim();
    (!login.is_empty()).then(|| login.to_string())
}

/// Counters per path class.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PathCounters {
    /// `direct_ipv4` + `direct_ipv6`.
    pub direct: u64,
    /// `derp`.
    pub derp: u64,
    /// `peer_relay_ipv4` + `peer_relay_ipv6`.
    pub peer_relay: u64,
}

impl PathCounters {
    fn add(&mut self, path: &str, v: u64) {
        match path {
            "direct_ipv4" | "direct_ipv6" => self.direct += v,
            "derp" => self.derp += v,
            "peer_relay_ipv4" | "peer_relay_ipv6" => self.peer_relay += v,
            _ => {}
        }
    }

    /// `self − earlier`, saturating at 0 (counters reset when a node restarts).
    #[must_use]
    pub fn delta(&self, earlier: &PathCounters) -> PathCounters {
        PathCounters {
            direct: self.direct.saturating_sub(earlier.direct),
            derp: self.derp.saturating_sub(earlier.derp),
            peer_relay: self.peer_relay.saturating_sub(earlier.peer_relay),
        }
    }

    /// Sum of all paths.
    pub fn total(&self) -> u64 {
        self.direct + self.derp + self.peer_relay
    }
}

/// `tailscaled_{inbound,outbound}_{bytes,packets}_total` by path.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct UserMetrics {
    /// Bytes received from peers.
    pub inbound_bytes: PathCounters,
    /// Bytes sent to peers.
    pub outbound_bytes: PathCounters,
    /// Packets received from peers.
    pub inbound_packets: PathCounters,
    /// Packets sent to peers.
    pub outbound_packets: PathCounters,
}

impl UserMetrics {
    /// `self − earlier` for every counter.
    #[must_use]
    pub fn delta(&self, earlier: &UserMetrics) -> UserMetrics {
        UserMetrics {
            inbound_bytes: self.inbound_bytes.delta(&earlier.inbound_bytes),
            outbound_bytes: self.outbound_bytes.delta(&earlier.outbound_bytes),
            inbound_packets: self.inbound_packets.delta(&earlier.inbound_packets),
            outbound_packets: self.outbound_packets.delta(&earlier.outbound_packets),
        }
    }

    /// Bytes in + out per path: one side's contribution to `network.path_bytes`
    /// (the report sums both sides' deltas).
    pub fn path_bytes(&self) -> PathBytes {
        PathBytes {
            direct: self.inbound_bytes.direct + self.outbound_bytes.direct,
            derp: self.inbound_bytes.derp + self.outbound_bytes.derp,
            peer_relay: self.inbound_bytes.peer_relay + self.outbound_bytes.peer_relay,
        }
    }
}

/// Parse the Prometheus text exposition of `/localapi/v0/usermetrics`.
/// Unknown metrics, comments and malformed lines are ignored.
pub fn parse_usermetrics(text: &str) -> UserMetrics {
    let mut m = UserMetrics::default();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((name_labels, value)) = split_sample(line) else {
            continue;
        };
        let (name, labels) = match name_labels.split_once('{') {
            Some((n, rest)) => (n, rest.trim_end_matches('}')),
            None => (name_labels, ""),
        };
        let target = match name {
            "tailscaled_inbound_bytes_total" => &mut m.inbound_bytes,
            "tailscaled_outbound_bytes_total" => &mut m.outbound_bytes,
            "tailscaled_inbound_packets_total" => &mut m.inbound_packets,
            "tailscaled_outbound_packets_total" => &mut m.outbound_packets,
            _ => continue,
        };
        let Some(path) = label(labels, "path") else {
            continue;
        };
        let Ok(v) = value.parse::<f64>() else {
            continue;
        };
        if v.is_finite() && v >= 0.0 {
            target.add(path, v as u64);
        }
    }
    m
}

/// Split `name{labels} value [timestamp]` into `(name{labels}, value)`.
fn split_sample(line: &str) -> Option<(&str, &str)> {
    let close = line.rfind('}');
    let (head, tail) = match close {
        Some(i) => (&line[..=i], &line[i + 1..]),
        None => {
            let i = line.find(char::is_whitespace)?;
            (&line[..i], &line[i..])
        }
    };
    let value = tail.split_whitespace().next()?;
    Some((head, value))
}

/// The value of `key="…"` in a label set.
fn label<'a>(labels: &'a str, key: &str) -> Option<&'a str> {
    for pair in labels.split(',') {
        let (k, v) = pair.split_once('=')?;
        if k.trim() == key {
            return Some(v.trim().trim_matches('"'));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"# HELP tailscaled_inbound_bytes_total Counts the number of bytes received from other peers
# TYPE tailscaled_inbound_bytes_total counter
tailscaled_inbound_bytes_total{path="derp"} 1200
tailscaled_inbound_bytes_total{path="direct_ipv4"} 1000000
tailscaled_inbound_bytes_total{path="direct_ipv6"} 500
tailscaled_inbound_bytes_total{path="peer_relay_ipv4"} 7
tailscaled_inbound_bytes_total{path="peer_relay_ipv6"} 3
# TYPE tailscaled_inbound_packets_total counter
tailscaled_inbound_packets_total{path="direct_ipv4"} 800
tailscaled_inbound_packets_total{path="derp"} 2
tailscaled_outbound_bytes_total{path="direct_ipv4"} 2000000
tailscaled_outbound_bytes_total{path="derp"} 300
tailscaled_outbound_packets_total{path="direct_ipv4"} 1600
tailscaled_outbound_packets_total{path="derp"} 1
tailscaled_outbound_packets_dropped_errors_total{reason="error"} 4
tailscaled_health_messages{type="warning"} 0
tailscaled_home_derp_region_id 1
garbage line
"#;

    #[test]
    fn parses_path_counters() {
        let m = parse_usermetrics(SAMPLE);
        assert_eq!(
            m.inbound_bytes,
            PathCounters {
                direct: 1_000_500,
                derp: 1200,
                peer_relay: 10
            }
        );
        assert_eq!(m.outbound_bytes.direct, 2_000_000);
        assert_eq!(m.outbound_bytes.derp, 300);
        assert_eq!(m.inbound_packets.direct, 800);
        assert_eq!(m.outbound_packets.derp, 1);
        let pb = m.path_bytes();
        assert_eq!(pb.direct, 3_000_500);
        assert_eq!(pb.derp, 1500);
        assert_eq!(pb.peer_relay, 10);
        assert_eq!(parse_usermetrics(""), UserMetrics::default());
    }

    #[test]
    fn deltas_saturate() {
        let before = parse_usermetrics(SAMPLE);
        let mut after = before;
        after.outbound_bytes.direct += 5000;
        after.inbound_bytes.derp = 0; // counter reset
        let d = after.delta(&before);
        assert_eq!(d.outbound_bytes.direct, 5000);
        assert_eq!(d.inbound_bytes.derp, 0);
        assert_eq!(d.inbound_bytes.total(), 0);
    }

    #[test]
    fn parses_scientific_and_timestamped_values() {
        let m = parse_usermetrics(
            "tailscaled_outbound_bytes_total{path=\"direct_ipv4\"} 1.5e+06 1700000000000\n",
        );
        assert_eq!(m.outbound_bytes.direct, 1_500_000);
    }

    #[test]
    fn http_parsing() {
        let r = parse_response(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhelloEXTRA").unwrap();
        assert_eq!((r.status, r.body.as_slice()), (200, &b"hello"[..]));
        let r = parse_response(
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n6;x=y\r\n world\r\n0\r\n\r\n",
        )
        .unwrap();
        assert_eq!(r.body, b"hello world");
        let r = parse_response(b"HTTP/1.1 204 No Content\r\n\r\n").unwrap();
        assert_eq!((r.status, r.body.len()), (204, 0));
        assert!(parse_response(b"garbage").is_err());
        assert!(parse_response(b"HTTP/1.1 200 OK\r\nContent-Length: 9\r\n\r\nshort").is_err());
    }

    #[test]
    fn self_login_from_status() {
        let status = serde_json::json!({
            "BackendState": "Running",
            "Self": {"ID": "nSELF", "UserID": 12345, "DNSName": "a.tail1.ts.net."},
            "User": {
                "12345": {"ID": 12345, "LoginName": "alice@example.com", "DisplayName": "Alice"},
                "99": {"ID": 99, "LoginName": "bob@example.com"}
            }
        });
        assert_eq!(
            parse_self_login(&status).as_deref(),
            Some("alice@example.com")
        );
        let tagged = serde_json::json!({
            "Self": {"UserID": 7},
            "User": {"7": {"ID": 7, "LoginName": "tagged-devices"}}
        });
        assert_eq!(parse_self_login(&tagged).as_deref(), Some("tagged-devices"));
        // Not known yet: no profile, user 0, no self, an empty login.
        for v in [
            serde_json::json!({"Self": {"UserID": 12345}, "User": {}}),
            serde_json::json!({"Self": {"UserID": 12345}}),
            serde_json::json!({"Self": {"UserID": 0}, "User": {"0": {"LoginName": "x"}}}),
            serde_json::json!({"BackendState": "NeedsLogin"}),
            serde_json::json!({"Self": {"UserID": 3}, "User": {"3": {"LoginName": " "}}}),
        ] {
            assert_eq!(parse_self_login(&v), None, "{v}");
        }
    }

    #[test]
    fn debug_redacts_the_credential() {
        let api = LocalApi::new("127.0.0.1:1234", "s3cr3t-credential");
        let s = format!("{api:?}");
        assert!(!s.contains("s3cr3t"), "{s}");
        assert!(s.contains("127.0.0.1:1234"));
    }

    #[tokio::test]
    async fn sends_the_required_headers() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 4096];
            let mut req = Vec::new();
            loop {
                let n = sock.read(&mut buf).await.unwrap();
                req.extend_from_slice(&buf[..n]);
                if n == 0 || req.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }
            let body = "tailscaled_inbound_bytes_total{path=\"derp\"} 42\n";
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{body}",
                body.len()
            );
            sock.write_all(resp.as_bytes()).await.unwrap();
            String::from_utf8(req).unwrap()
        });
        let api = LocalApi::new(addr, "cred");
        let m = api.usermetrics().await.unwrap();
        assert_eq!(m.inbound_bytes.derp, 42);
        let req = server.await.unwrap();
        assert!(
            req.starts_with("GET /localapi/v0/usermetrics HTTP/1.1\r\n"),
            "{req}"
        );
        assert!(req.contains("\r\nSec-Tailscale: localapi\r\n"));
        // base64(":cred")
        assert!(
            req.contains("\r\nAuthorization: Basic OmNyZWQ=\r\n"),
            "{req}"
        );
        assert!(!req.to_ascii_lowercase().contains("origin:"));
        assert!(!req.to_ascii_lowercase().contains("referer:"));
    }
}
