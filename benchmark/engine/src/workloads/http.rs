//! `http` (RFC 027 §8.2 http; §6 rule 5, replaced by the `Tailscale-Node-Id`
//! check on the proxy path).
//!
//! - **Responder server:** a tiny HTTP/1.1 server. It serves only
//!   `GET /r/<run_tag_hex>/bytes/<n>` with `n ≤ response_bytes` (400 for a
//!   larger or malformed `n`, 404 for every other path or method), always
//!   with `Content-Length` (never chunked), and keeps connections alive
//!   unless the request says `Connection: close`.
//! - **`proxy` target:** the server listens on `127.0.0.1:0` and the
//!   sidecar's reverse proxy fronts it: `proxy().add(ProxyConfig { id:
//!   "bench-<run_tag_hex>", listen_port: P, target: 127.0.0.1:<server>, tls:
//!   false, allow_non_loopback: false, allow: [initiator login] unless it is
//!   unknown or tagged, routes: [] })`. `P = 20000 + (run_tag % 10000)`; on
//!   `PORT_IN_USE`/`LISTEN_ERROR` the next port is tried, up to 8 attempts.
//!   Port 0 is never used: the sidecar reports the requested port, not the
//!   bound one. The server answers 403 unless the proxy-injected
//!   `Tailscale-Node-Id` is the initiator's tailscale id.
//! - **`direct` target:** the same handler on `listen_tcp(0)`, behind rule
//!   5's identity check ([`ResponderCtx::admit_tcp`]).
//! - **Initiator:** `concurrency` workers over `open_tcp`, started together.
//!   `keepalive` reuses one connection per worker (reconnecting after an
//!   error); `new` opens one per request and asks the server to close it.
//!   Latency runs from the start of writing the request to the last body
//!   byte; in `new` mode from the `open_tcp` call, with `connect_ms`
//!   reported apart. A request is measured when it starts after the warmup
//!   and before the end; `error_pct = (non-2xx + I/O errors + 10 s
//!   timeouts) / attempts`.
//! - **Teardown** (rule 7): `unlisten_tcp` (direct) or `proxy().remove`
//!   (proxy), then the server tasks.
//!
//! Summary keys: `req_per_sec`, `latency_p50_ms`, `latency_p99_ms`, `error_pct`.
//! Series: `req_per_sec`. Live samples: `req_per_sec`, `latency_p50_ms`,
//! `error_pct`.

use std::collections::BTreeMap;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use futures::future::BoxFuture;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use truffle_core::proxy::{ProxyConfig, ProxyTarget};
use truffle_core::transport::RawListener;

use crate::clock::{instant_ns, now_ns};
use crate::dataplane::payload_chunk;
use crate::error::{AppError, ErrorCode};
use crate::hub::BenchNode;
use crate::params::{HttpConnection, HttpParams, HttpTarget, WorkloadParams};
use crate::stats;
use crate::types::{is_tagged_login, Better, Distribution, Phase, Series, SummaryItem};
use crate::workloads::common::Tasks;
use crate::workloads::{ResponderCtx, ResponderRun, RunCtx, WorkloadOutput};

/// `http` is implemented.
pub const IMPLEMENTED: bool = true;

/// A request (connect included in `new` mode) that takes longer is a timeout.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
/// First proxy port: `P = 20000 + (run_tag % 10000)`.
pub const PROXY_PORT_BASE: u16 = 20_000;
/// Proxy ports wrap within `20000..30000`.
const PROXY_PORT_SPAN: u64 = 10_000;
/// Proxy ports tried before giving up.
pub const PROXY_ATTEMPTS: u32 = 8;
/// The header the sidecar's proxy injects with the caller's stable node id.
pub const NODE_ID_HEADER: &str = "Tailscale-Node-Id";
/// Request and response heads larger than this are refused.
const MAX_HEAD: usize = 16 * 1024;
/// Raw latency samples kept in `results.samples_ms` (order statistics).
const MAX_RAW_SAMPLES: usize = 4000;
/// Series bucket.
const BUCKET_NS: u64 = 250_000_000;
/// After a failed connect in keep-alive mode, wait this long before retrying.
const RECONNECT_PAUSE: Duration = Duration::from_millis(20);

/// §8.2 http note for proxy keep-alive above two workers.
pub const NOTE_PROXY_KEEPALIVE: &str =
    "proxy keepalive above concurrency 2 includes upstream reconnects (Go MaxIdleConnsPerHost = 2)";
/// §8.2 http note for `new` connections.
pub const NOTE_NEW: &str = "new: one WhoIs per request on both paths";

/// Proxy listen port for `attempt` (0-based): `20000 + ((run_tag % 10000) + attempt) % 10000`.
pub fn proxy_port(run_tag: u64, attempt: u32) -> u16 {
    let off = (run_tag % PROXY_PORT_SPAN + u64::from(attempt)) % PROXY_PORT_SPAN;
    PROXY_PORT_BASE + off as u16
}

/// The only path the server answers: `/r/<run_tag_hex>/bytes/<n>`.
pub fn request_path(run_tag_hex: &str, n: u64) -> String {
    format!("/r/{run_tag_hex}/bytes/{n}")
}

/// Whether a proxy error is worth trying the next port for.
fn port_retryable(message: &str) -> bool {
    message.contains("PORT_IN_USE") || message.contains("LISTEN_ERROR")
}

/// Whether a proxy error is the sidecar's own refusal (`[CODE] message`), so
/// nothing was registered. Anything else (core's 10 s timeout, a lost
/// reply) may still end with the proxy registered once the sidecar catches
/// up.
fn sidecar_refused(message: &str) -> bool {
    message.split('[').skip(1).any(|rest| {
        rest.split_once(']').is_some_and(|(code, _)| {
            !code.is_empty() && code.chars().all(|c| c.is_ascii_uppercase() || c == '_')
        })
    })
}

/// How long a failed proxy add is watched for a late registration.
const PROXY_CLEANUP_TRIES: u32 = 20;

/// A proxy add that failed without a refusal may still complete in the
/// sidecar, leaving a tailnet listener aimed at a loopback port this run no
/// longer owns. Keep removing it (a pending add answers "not found") for
/// about 20 s.
fn spawn_proxy_cleanup(node: Arc<crate::hub::BenchNode>, id: String) {
    tokio::spawn(async move {
        for _ in 0..PROXY_CLEANUP_TRIES {
            tokio::time::sleep(Duration::from_secs(1)).await;
            match node.proxy().remove(&id).await {
                Ok(()) => {
                    tracing::debug!(%id, "http: removed a proxy whose add had failed");
                    return;
                }
                Err(e) => tracing::trace!(%id, error = %e, "http: proxy cleanup"),
            }
        }
    });
}

// ---------------------------------------------------------------------------
// Wire and report types
// ---------------------------------------------------------------------------

/// `prepare.client`: nothing beyond the params.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "HttpClient")]
pub struct Client {}

/// `prepare.ack.setup`: where to send requests.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "HttpSetup")]
pub struct Setup {
    /// The tailnet port: the proxy's listen port, or the `listen_tcp` port.
    pub port: u16,
    /// Proxy ports tried (1 when the first was free; 0 for `direct`).
    pub ports_tried: u32,
}

/// `finish.info`: the initiator's request count.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "HttpInfo")]
pub struct Info {
    /// Requests attempted, warmup included.
    pub requests: u64,
}

/// `finish.ack.stats`: what the responder's server saw (warmup included).
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "HttpServerStats")]
pub struct Stats {
    /// Requests parsed.
    pub requests: u64,
    /// 200 answers.
    pub ok: u64,
    /// 403 answers (`Tailscale-Node-Id` missing or not the initiator's).
    pub forbidden: u64,
    /// 400 answers (size above `response_bytes`, or a malformed request).
    pub bad_request: u64,
    /// 404 answers.
    pub not_found: u64,
    /// Body bytes of 200 answers.
    pub body_bytes: u64,
    /// Connections accepted (from the proxy on loopback, or from the tailnet).
    pub connections: u64,
    /// Tailnet connections dropped by rule 5 (direct target).
    pub foreign_dropped: u64,
}

/// Failed requests by cause.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "HttpErrors")]
pub struct Errors {
    /// Answers outside 2xx.
    pub non_2xx: u64,
    /// Connect, read or write failures.
    pub io: u64,
    /// Requests over the 10 s limit.
    pub timeout: u64,
}

/// `RunReport.results` of a `http` run.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "HttpResults")]
pub struct Results {
    /// The tailnet port requests went to.
    pub port: u16,
    /// Measured requests (started after the warmup, before the end).
    pub attempts: u64,
    /// Measured 2xx answers.
    pub ok: u64,
    /// Measured failures by cause.
    pub errors: Errors,
    /// Measured I/O errors by kind (`connect`, `closed`, `reset`, `invalid-response`, `io`).
    pub error_kinds: BTreeMap<String, u64>,
    /// Measured answers by status code.
    pub status_counts: BTreeMap<String, u64>,
    /// `(non-2xx + io + timeouts) / attempts · 100`.
    pub error_pct: Option<f64>,
    /// Measured 2xx answers ÷ `duration_secs`.
    pub req_per_sec: Option<f64>,
    /// Latency of measured 2xx answers, ms (timeouts ranked as lost).
    pub latency_ms: Distribution,
    /// Raw 2xx latencies, ms, ascending; at most 4000 evenly spaced order
    /// statistics when there were more.
    pub samples_ms: Vec<f64>,
    /// Measured 2xx answers behind `samples_ms`.
    pub samples_total: u64,
    /// Median `open_tcp` time, ms (every connection opened).
    pub connect_ms: Option<f64>,
    /// `open_tcp` times, ms.
    pub connect: Distribution,
    /// Connections opened (successful `open_tcp` calls), warmup included.
    pub connections_opened: u64,
    /// Body bytes of measured 2xx answers.
    pub body_bytes: u64,
    /// Body goodput over the measured window, Mb/s.
    pub goodput_mbps: Option<f64>,
    /// Requests started during the warmup (not counted).
    pub warmup_requests: u64,
    /// What the responder's server saw (warmup included).
    pub server: Option<Stats>,
}

// ---------------------------------------------------------------------------
// HTTP/1.1 parsing (just enough for this benchmark)
// ---------------------------------------------------------------------------

/// A parsed request head.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Request {
    /// `GET`, ….
    pub method: String,
    /// The request target, query included.
    pub target: String,
    /// Close after answering (`Connection: close`, or HTTP/1.0 without keep-alive).
    pub close: bool,
    /// `Tailscale-Node-Id`, if present.
    pub node_id: Option<String>,
    /// `Content-Length` of a request body (discarded).
    pub content_length: u64,
}

/// A parsed response head.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ResponseHead {
    /// Status code.
    pub status: u16,
    /// `Content-Length`, if present.
    pub content_length: Option<u64>,
    /// `Transfer-Encoding: chunked`.
    pub chunked: bool,
    /// The server will close the connection.
    pub close: bool,
}

fn header_lines(head: &[u8]) -> Result<(String, Vec<(String, String)>), String> {
    let text = std::str::from_utf8(head).map_err(|_| "head is not UTF-8".to_string())?;
    let mut lines = text.split("\r\n");
    let first = lines.next().unwrap_or_default().to_string();
    let mut headers = Vec::new();
    for line in lines {
        if line.is_empty() {
            continue;
        }
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| format!("malformed header line {line:?}"))?;
        headers.push((name.trim().to_ascii_lowercase(), value.trim().to_string()));
    }
    Ok((first, headers))
}

fn connection_tokens(headers: &[(String, String)]) -> (bool, bool) {
    let mut close = false;
    let mut keep_alive = false;
    for (_, v) in headers.iter().filter(|(n, _)| n == "connection") {
        for t in v.split(',') {
            let t = t.trim();
            close |= t.eq_ignore_ascii_case("close");
            keep_alive |= t.eq_ignore_ascii_case("keep-alive");
        }
    }
    (close, keep_alive)
}

fn header<'h>(headers: &'h [(String, String)], name: &str) -> Option<&'h str> {
    headers
        .iter()
        .find(|(n, _)| n.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.as_str())
}

/// Parse a request head (`"GET /path HTTP/1.1\r\nName: value\r\n…\r\n\r\n"`).
pub fn parse_request(head: &[u8]) -> Result<Request, String> {
    let (first, headers) = header_lines(head)?;
    let mut parts = first.split(' ');
    let (Some(method), Some(target), Some(version), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(format!("malformed request line {first:?}"));
    };
    let http10 = match version {
        "HTTP/1.1" => false,
        "HTTP/1.0" => true,
        other => return Err(format!("unsupported version {other:?}")),
    };
    if method.is_empty() || !target.starts_with('/') {
        return Err(format!("malformed request line {first:?}"));
    }
    let (close, keep_alive) = connection_tokens(&headers);
    let content_length = match header(&headers, "content-length") {
        Some(v) => v
            .parse::<u64>()
            .map_err(|_| format!("bad Content-Length {v:?}"))?,
        None => 0,
    };
    if header(&headers, "transfer-encoding").is_some() {
        return Err("request bodies with Transfer-Encoding are not supported".into());
    }
    Ok(Request {
        method: method.to_string(),
        target: target.to_string(),
        close: close || (http10 && !keep_alive),
        node_id: header(&headers, NODE_ID_HEADER).map(str::to_string),
        content_length,
    })
}

/// Parse a response head.
pub fn parse_response(head: &[u8]) -> Result<ResponseHead, String> {
    let (first, headers) = header_lines(head)?;
    let mut parts = first.splitn(3, ' ');
    let version = parts.next().unwrap_or_default();
    let status = parts
        .next()
        .and_then(|s| s.parse::<u16>().ok())
        .filter(|s| (100..1000).contains(s))
        .ok_or_else(|| format!("malformed status line {first:?}"))?;
    let http10 = match version {
        "HTTP/1.1" => false,
        "HTTP/1.0" => true,
        other => return Err(format!("unsupported version {other:?}")),
    };
    let (close, keep_alive) = connection_tokens(&headers);
    let chunked = header(&headers, "transfer-encoding")
        .is_some_and(|v| v.to_ascii_lowercase().contains("chunked"));
    let content_length = match header(&headers, "content-length") {
        Some(v) if !chunked => Some(
            v.parse::<u64>()
                .map_err(|_| format!("bad Content-Length {v:?}"))?,
        ),
        _ => None,
    };
    Ok(ResponseHead {
        status,
        content_length,
        chunked,
        close: close || (http10 && !keep_alive),
    })
}

/// The server's routing decision: status and body length.
pub fn route(
    req: &Request,
    run_tag_hex: &str,
    max_bytes: u64,
    node_id: Option<&str>,
) -> (u16, u64) {
    if let Some(id) = node_id {
        if req.node_id.as_deref() != Some(id) {
            return (403, 0);
        }
    }
    let path = req.target.split('?').next().unwrap_or_default();
    let prefix = format!("/r/{run_tag_hex}/bytes/");
    let Some(n) = path.strip_prefix(prefix.as_str()) else {
        return (404, 0);
    };
    if req.method != "GET" {
        return (404, 0);
    }
    if n.is_empty() || n.len() > 20 || !n.bytes().all(|b| b.is_ascii_digit()) {
        return (400, 0);
    }
    match n.parse::<u64>() {
        Ok(n) if n <= max_bytes => (200, n),
        _ => (400, 0),
    }
}

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        400 => "Bad Request",
        403 => "Forbidden",
        404 => "Not Found",
        _ => "Error",
    }
}

/// A response head with `Content-Length` (and `Connection: close` when closing).
pub fn response_head(status: u16, len: u64, close: bool) -> String {
    format!(
        "HTTP/1.1 {status} {}\r\nContent-Length: {len}\r\nContent-Type: application/octet-stream\r\n{}\r\n",
        reason(status),
        if close { "Connection: close\r\n" } else { "" }
    )
}

/// A GET request head for the bench path.
pub fn request_head(run_tag_hex: &str, n: u64, close: bool) -> Vec<u8> {
    format!(
        "GET {} HTTP/1.1\r\nHost: truffle-bench\r\nAccept: */*\r\n{}\r\n",
        request_path(run_tag_hex, n),
        if close { "Connection: close\r\n" } else { "" }
    )
    .into_bytes()
}

fn head_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n").map(|i| i + 4)
}

/// Read until a whole head is buffered and take it out of `buf`. `Ok(None)`
/// on a clean EOF before any byte of a new head.
async fn read_head<R: AsyncRead + Unpin>(
    r: &mut R,
    buf: &mut Vec<u8>,
) -> io::Result<Option<Vec<u8>>> {
    let mut tmp = [0u8; 4096];
    loop {
        if let Some(end) = head_end(buf) {
            return Ok(Some(buf.drain(..end).collect()));
        }
        if buf.len() > MAX_HEAD {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "head too large"));
        }
        let n = r.read(&mut tmp).await?;
        if n == 0 {
            return if buf.is_empty() {
                Ok(None)
            } else {
                Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "connection closed inside a head",
                ))
            };
        }
        buf.extend_from_slice(&tmp[..n]);
    }
}

/// Discard `n` body bytes, taking buffered ones first.
async fn discard<R: AsyncRead + Unpin>(
    r: &mut R,
    buf: &mut Vec<u8>,
    scratch: &mut [u8],
    mut n: u64,
) -> io::Result<()> {
    let take = (buf.len() as u64).min(n) as usize;
    buf.drain(..take);
    n -= take as u64;
    while n > 0 {
        let want = n.min(scratch.len() as u64) as usize;
        let got = r.read(&mut scratch[..want]).await?;
        if got == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "connection closed inside a body",
            ));
        }
        n -= got as u64;
    }
    Ok(())
}

/// Read one CRLF-terminated line (without the CRLF).
async fn read_line<R: AsyncRead + Unpin>(r: &mut R, buf: &mut Vec<u8>) -> io::Result<Vec<u8>> {
    let mut tmp = [0u8; 512];
    loop {
        if let Some(i) = buf.windows(2).position(|w| w == b"\r\n") {
            let line: Vec<u8> = buf.drain(..i + 2).take(i).collect();
            return Ok(line);
        }
        if buf.len() > MAX_HEAD {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "line too long"));
        }
        let n = r.read(&mut tmp).await?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "connection closed inside a chunked body",
            ));
        }
        buf.extend_from_slice(&tmp[..n]);
    }
}

/// Read (and discard) one response; returns its status, body bytes and
/// whether the connection must close.
async fn read_response<R: AsyncRead + Unpin>(
    r: &mut R,
    buf: &mut Vec<u8>,
    scratch: &mut [u8],
) -> io::Result<(u16, u64, bool)> {
    let head = read_head(r, buf).await?.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "the server closed the connection before answering",
        )
    })?;
    let h = parse_response(&head).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    if h.chunked {
        let mut total = 0u64;
        loop {
            let line = read_line(r, buf).await?;
            let text = String::from_utf8_lossy(&line);
            let size = text.split(';').next().unwrap_or_default().trim();
            let size = u64::from_str_radix(size, 16)
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad chunk size"))?;
            if size == 0 {
                // Trailers, up to the empty line.
                while !read_line(r, buf).await?.is_empty() {}
                return Ok((h.status, total, h.close));
            }
            discard(r, buf, scratch, size).await?;
            total += size;
            if !read_line(r, buf).await?.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "chunk without CRLF",
                ));
            }
        }
    }
    match h.content_length {
        Some(n) => {
            discard(r, buf, scratch, n).await?;
            Ok((h.status, n, h.close))
        }
        None => {
            // Delimited by EOF.
            let mut total = buf.len() as u64;
            buf.clear();
            loop {
                let n = r.read(scratch).await?;
                if n == 0 {
                    return Ok((h.status, total, true));
                }
                total += n as u64;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Responder
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct Counters {
    requests: AtomicU64,
    ok: AtomicU64,
    forbidden: AtomicU64,
    bad_request: AtomicU64,
    not_found: AtomicU64,
    body_bytes: AtomicU64,
    connections: AtomicU64,
    first_ns: AtomicU64,
    last_ns: AtomicU64,
    measured_bytes: AtomicU64,
}

struct Server {
    run_tag_hex: String,
    max_bytes: u64,
    /// `Some(initiator tailscale id)` on the proxy path.
    node_id: Option<String>,
    warmup_ns: u64,
    counters: Counters,
}

impl Server {
    fn record(&self, status: u16, body: u64, at_ns: u64) {
        let c = &self.counters;
        c.requests.fetch_add(1, Ordering::Relaxed);
        let first =
            match c
                .first_ns
                .compare_exchange(0, at_ns, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => at_ns,
                Err(prev) => prev,
            };
        c.last_ns.fetch_max(at_ns, Ordering::Relaxed);
        match status {
            200 => {
                c.ok.fetch_add(1, Ordering::Relaxed);
                c.body_bytes.fetch_add(body, Ordering::Relaxed);
                if at_ns >= first + self.warmup_ns {
                    c.measured_bytes.fetch_add(body, Ordering::Relaxed);
                }
            }
            403 => {
                c.forbidden.fetch_add(1, Ordering::Relaxed);
            }
            404 => {
                c.not_found.fetch_add(1, Ordering::Relaxed);
            }
            _ => {
                c.bad_request.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn stats(&self, foreign_dropped: u64) -> Stats {
        let c = &self.counters;
        Stats {
            requests: c.requests.load(Ordering::Relaxed),
            ok: c.ok.load(Ordering::Relaxed),
            forbidden: c.forbidden.load(Ordering::Relaxed),
            bad_request: c.bad_request.load(Ordering::Relaxed),
            not_found: c.not_found.load(Ordering::Relaxed),
            body_bytes: c.body_bytes.load(Ordering::Relaxed),
            connections: c.connections.load(Ordering::Relaxed),
            foreign_dropped,
        }
    }

    /// Serve one connection until it closes.
    async fn serve<S: AsyncRead + AsyncWrite + Unpin>(&self, mut s: S) {
        self.counters.connections.fetch_add(1, Ordering::Relaxed);
        let mut buf = Vec::with_capacity(1024);
        let mut scratch = vec![0u8; 16 * 1024];
        loop {
            let head = match read_head(&mut s, &mut buf).await {
                Ok(Some(h)) => h,
                _ => return,
            };
            let at_ns = now_ns();
            let (status, len, close) = match parse_request(&head) {
                Ok(req) => {
                    if req.content_length > 0
                        && discard(&mut s, &mut buf, &mut scratch, req.content_length)
                            .await
                            .is_err()
                    {
                        return;
                    }
                    let (status, len) = route(
                        &req,
                        &self.run_tag_hex,
                        self.max_bytes,
                        self.node_id.as_deref(),
                    );
                    (status, len, req.close)
                }
                Err(_) => (400, 0, true),
            };
            self.record(status, len, at_ns);
            if write_response(&mut s, status, len, close).await.is_err() {
                return;
            }
            if close {
                let _ = s.shutdown().await;
                return;
            }
        }
    }
}

/// Write a response with a `len`-byte pattern body (one write when small).
async fn write_response<W: AsyncWrite + Unpin>(
    w: &mut W,
    status: u16,
    len: u64,
    close: bool,
) -> io::Result<()> {
    let head = response_head(status, len, close);
    let chunk = payload_chunk();
    if len <= chunk.len() as u64 {
        let mut out = Vec::with_capacity(head.len() + len as usize);
        out.extend_from_slice(head.as_bytes());
        out.extend_from_slice(&chunk[..len as usize]);
        w.write_all(&out).await?;
    } else {
        w.write_all(head.as_bytes()).await?;
        let mut left = len;
        while left > 0 {
            let n = left.min(chunk.len() as u64) as usize;
            w.write_all(&chunk[..n]).await?;
            left -= n as u64;
        }
    }
    w.flush().await
}

/// Proxy path: accept the sidecar's upstream connections on loopback.
async fn accept_loopback(listener: TcpListener, server: Arc<Server>, cancel: CancellationToken) {
    let mut conns = JoinSet::new();
    loop {
        let accepted = tokio::select! {
            _ = cancel.cancelled() => break,
            a = listener.accept() => a,
            Some(_) = conns.join_next(), if !conns.is_empty() => continue,
        };
        let Ok((stream, _)) = accepted else {
            tokio::time::sleep(Duration::from_millis(5)).await;
            continue;
        };
        let _ = stream.set_nodelay(true);
        let s = server.clone();
        conns.spawn(async move { s.serve(stream).await });
    }
}

/// Direct path: accept tailnet connections, keeping only the initiator's (rule 5).
async fn accept_tailnet(mut listener: RawListener, r: ResponderCtx, server: Arc<Server>) {
    let cancel = r.cancel_token();
    let mut conns = JoinSet::new();
    loop {
        let incoming = tokio::select! {
            _ = cancel.cancelled() => break,
            i = listener.accept() => match i {
                Some(i) => i,
                None => break,
            },
            Some(_) = conns.join_next(), if !conns.is_empty() => continue,
        };
        if !r.admit_tcp(&incoming) {
            continue;
        }
        let stream = incoming.stream;
        let _ = stream.set_nodelay(true);
        let s = server.clone();
        conns.spawn(async move { s.serve(stream).await });
    }
}

struct HttpResponder {
    r: ResponderCtx,
    server: Arc<Server>,
    tasks: Tasks<()>,
    proxy_id: Option<String>,
    tcp_port: Option<u16>,
}

impl HttpResponder {
    async fn teardown(self) -> Stats {
        let HttpResponder {
            r,
            server,
            tasks,
            proxy_id,
            tcp_port,
        } = self;
        let mut t = r.teardown();
        if let Some(port) = tcp_port {
            t.tcp_port(port);
        }
        if let Some(id) = proxy_id {
            t.proxy(id);
        }
        t.hold(tasks);
        t.run().await;
        server.stats(r.foreign_dropped())
    }
}

/// Responder: start the server and put the proxy (or a tailnet listener) in front of it.
pub async fn prepare(
    r: &ResponderCtx,
    p: &HttpParams,
    _client: Client,
) -> Result<(Setup, Box<dyn ResponderRun>), AppError> {
    let hex = r.run_tag_hex();
    let server = Arc::new(Server {
        run_tag_hex: hex.clone(),
        max_bytes: p.response_bytes,
        node_id: (p.target == HttpTarget::Proxy).then(|| r.initiator.tailscale_id.clone()),
        warmup_ns: u64::from(p.warmup_secs) * 1_000_000_000,
        counters: Counters::default(),
    });
    match p.target {
        HttpTarget::Proxy => {
            let listener = TcpListener::bind("127.0.0.1:0")
                .await
                .map_err(|e| AppError::io("bind the loopback HTTP server", &e))?;
            let server_port = listener
                .local_addr()
                .map_err(|e| AppError::io("loopback HTTP server address", &e))?
                .port();
            let tasks = Tasks::from(vec![tokio::spawn(accept_loopback(
                listener,
                server.clone(),
                r.cancel_token(),
            ))]);
            let allow = match r.initiator.login_name.as_deref() {
                Some(login) if !is_tagged_login(Some(login)) => vec![login.to_string()],
                _ => Vec::new(),
            };
            let id = format!("bench-{hex}");
            let mut last = String::new();
            for attempt in 0..PROXY_ATTEMPTS {
                let listen_port = proxy_port(r.run_tag, attempt);
                let cfg = ProxyConfig {
                    id: id.clone(),
                    name: id.clone(),
                    listen_port,
                    target: ProxyTarget {
                        host: "127.0.0.1".into(),
                        port: server_port,
                        scheme: "http".into(),
                    },
                    announce: false,
                    tls: false,
                    allow_non_loopback: false,
                    allow: allow.clone(),
                    routes: Vec::new(),
                };
                match r.node.proxy().add(cfg).await {
                    Ok(_) => {
                        let run = HttpResponder {
                            r: r.clone(),
                            server,
                            tasks,
                            proxy_id: Some(id),
                            tcp_port: None,
                        };
                        return Ok((
                            Setup {
                                port: listen_port,
                                ports_tried: attempt + 1,
                            },
                            Box::new(run),
                        ));
                    }
                    Err(e) => {
                        let msg = e.to_string();
                        if !port_retryable(&msg) {
                            if !sidecar_refused(&msg) {
                                spawn_proxy_cleanup(r.node.clone(), id.clone());
                            }
                            return Err(AppError::from(e).context("proxy add"));
                        }
                        last = msg;
                    }
                }
            }
            Err(AppError::new(
                ErrorCode::Io,
                format!(
                    "no free proxy port in {PROXY_ATTEMPTS} attempts from {}: {last}",
                    proxy_port(r.run_tag, 0)
                ),
            ))
        }
        HttpTarget::Direct => {
            let listener = r.node.listen_tcp(0).await?;
            let port = listener.port;
            let tasks = Tasks::from(vec![tokio::spawn(accept_tailnet(
                listener,
                r.clone(),
                server.clone(),
            ))]);
            let run = HttpResponder {
                r: r.clone(),
                server,
                tasks,
                proxy_id: None,
                tcp_port: Some(port),
            };
            Ok((
                Setup {
                    port,
                    ports_tried: 0,
                },
                Box::new(run),
            ))
        }
    }
}

impl ResponderRun for HttpResponder {
    fn start(&mut self) -> BoxFuture<'_, Result<(), AppError>> {
        Box::pin(async { Ok(()) })
    }

    fn finish(self: Box<Self>, _info: Value) -> BoxFuture<'static, Result<Value, AppError>> {
        Box::pin(async move {
            let c = &self.server.counters;
            let first = c.first_ns.load(Ordering::Relaxed);
            let last = c.last_ns.load(Ordering::Relaxed);
            let start = first + self.server.warmup_ns;
            if first > 0 && last > start {
                self.r.set_resource_window(
                    start,
                    last,
                    Some(c.measured_bytes.load(Ordering::Relaxed)),
                );
            }
            let stats = self.teardown().await;
            serde_json::to_value(stats).map_err(|e| AppError::internal(format!("http stats: {e}")))
        })
    }

    fn cancel(self: Box<Self>) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            self.teardown().await;
        })
    }
}

// ---------------------------------------------------------------------------
// Initiator
// ---------------------------------------------------------------------------

/// How a request ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// A 2xx answer.
    Ok(u16),
    /// Any other answer.
    Status(u16),
    /// An I/O failure, by kind.
    Io(&'static str),
    /// Over [`REQUEST_TIMEOUT`].
    Timeout,
}

/// One request.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Record {
    /// When it started (`now_ns()`): the write, or `open_tcp` in `new` mode.
    pub start_ns: u64,
    /// When it ended.
    pub end_ns: u64,
    /// How.
    pub outcome: Outcome,
    /// Start → last body byte (or failure), ms.
    pub latency_ms: f64,
    /// Body bytes.
    pub body: u64,
}

fn io_kind(e: &io::Error) -> &'static str {
    match e.kind() {
        io::ErrorKind::UnexpectedEof => "closed",
        io::ErrorKind::ConnectionReset
        | io::ErrorKind::ConnectionAborted
        | io::ErrorKind::BrokenPipe => "reset",
        io::ErrorKind::InvalidData => "invalid-response",
        _ => "io",
    }
}

fn outcome_of(status: u16) -> Outcome {
    if (200..300).contains(&status) {
        Outcome::Ok(status)
    } else {
        Outcome::Status(status)
    }
}

/// Live aggregates the workers feed and a 4 Hz task reads.
#[derive(Debug, Default)]
struct Live {
    ok_since: u64,
    measured_latencies: Vec<f64>,
    warmup_latencies: Vec<f64>,
    measured_attempts: u64,
    measured_errors: u64,
}

impl Live {
    fn record(&mut self, measured: bool, outcome: Outcome, latency_ms: f64) {
        let ok = matches!(outcome, Outcome::Ok(_));
        if ok {
            self.ok_since += 1;
            if measured {
                self.measured_latencies.push(latency_ms);
            } else {
                self.warmup_latencies.push(latency_ms);
            }
        }
        if measured {
            self.measured_attempts += 1;
            if !ok {
                self.measured_errors += 1;
            }
        }
    }
}

fn median_of(v: &[f64]) -> Option<f64> {
    if v.is_empty() {
        return None;
    }
    let mut c = v.to_vec();
    let mid = c.len() / 2;
    let (_, m, _) = c.select_nth_unstable_by(mid, f64::total_cmp);
    Some(*m)
}

#[derive(Clone)]
struct Worker {
    node: Arc<BenchNode>,
    peer_id: String,
    port: u16,
    head: Arc<Vec<u8>>,
    mode: HttpConnection,
    warm_end: Instant,
    end: Instant,
    live: Arc<Mutex<Live>>,
    cancel: CancellationToken,
}

#[derive(Debug, Default)]
struct WorkerOut {
    records: Vec<Record>,
    connects_ms: Vec<f64>,
    connections_opened: u64,
}

impl Worker {
    fn push(
        &self,
        out: &mut WorkerOut,
        started: Instant,
        start_ns: u64,
        outcome: Outcome,
        body: u64,
    ) {
        let latency_ms = started.elapsed().as_secs_f64() * 1000.0;
        let measured = started >= self.warm_end && started < self.end;
        self.live
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .record(measured, outcome, latency_ms);
        out.records.push(Record {
            start_ns,
            end_ns: now_ns(),
            outcome,
            latency_ms,
            body,
        });
    }

    async fn connect(&self, out: &mut WorkerOut) -> Result<tokio::net::TcpStream, Outcome> {
        let t = Instant::now();
        match tokio::time::timeout(
            REQUEST_TIMEOUT,
            self.node.open_tcp(&self.peer_id, self.port),
        )
        .await
        {
            Ok(Ok(s)) => {
                let _ = s.set_nodelay(true);
                out.connections_opened += 1;
                out.connects_ms.push(t.elapsed().as_secs_f64() * 1000.0);
                Ok(s)
            }
            Ok(Err(_)) => Err(Outcome::Io("connect")),
            Err(_) => Err(Outcome::Timeout),
        }
    }

    async fn run(self) -> WorkerOut {
        let mut out = WorkerOut::default();
        let mut buf = Vec::with_capacity(4096);
        let mut scratch = vec![0u8; 64 * 1024];
        let mut conn: Option<tokio::net::TcpStream> = None;
        while Instant::now() < self.end && !self.cancel.is_cancelled() {
            let started = Instant::now();
            let start_ns = now_ns();
            match self.mode {
                HttpConnection::Keepalive => {
                    let mut s = match conn.take() {
                        Some(s) => s,
                        None => match self.connect(&mut out).await {
                            Ok(s) => {
                                buf.clear();
                                s
                            }
                            Err(o) => {
                                self.push(&mut out, started, start_ns, o, 0);
                                tokio::time::sleep(RECONNECT_PAUSE).await;
                                continue;
                            }
                        },
                    };
                    // Keep-alive latency starts at the write, not the connect.
                    let started = Instant::now();
                    let start_ns = now_ns();
                    let res = tokio::select! {
                        _ = self.cancel.cancelled() => break,
                        r = tokio::time::timeout(REQUEST_TIMEOUT, async {
                            s.write_all(&self.head).await?;
                            read_response(&mut s, &mut buf, &mut scratch).await
                        }) => r,
                    };
                    match res {
                        Ok(Ok((status, body, close))) => {
                            self.push(&mut out, started, start_ns, outcome_of(status), body);
                            if !close {
                                conn = Some(s);
                            }
                        }
                        Ok(Err(e)) => {
                            self.push(&mut out, started, start_ns, Outcome::Io(io_kind(&e)), 0)
                        }
                        Err(_) => self.push(&mut out, started, start_ns, Outcome::Timeout, 0),
                    }
                }
                HttpConnection::New => {
                    let res = tokio::select! {
                        _ = self.cancel.cancelled() => break,
                        r = tokio::time::timeout(REQUEST_TIMEOUT, async {
                            let mut s = self.connect(&mut out).await.map_err(Err)?;
                            buf.clear();
                            s.write_all(&self.head).await.map_err(Ok)?;
                            read_response(&mut s, &mut buf, &mut scratch).await.map_err(Ok)
                        }) => r,
                    };
                    let (outcome, body) = match res {
                        Ok(Ok((status, body, _))) => (outcome_of(status), body),
                        Ok(Err(Ok(e))) => (Outcome::Io(io_kind(&e)), 0),
                        Ok(Err(Err(o))) => (o, 0),
                        Err(_) => (Outcome::Timeout, 0),
                    };
                    self.push(&mut out, started, start_ns, outcome, body);
                }
            }
        }
        out
    }
}

/// The measurement's times, `now_ns()`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Window {
    /// Run origin (`t_ms` 0).
    pub origin_ns: u64,
    /// Workers started.
    pub data_start_ns: u64,
    /// Warmup ended: requests starting from here are measured.
    pub warm_end_ns: u64,
    /// Measurement ended: no request starts after this.
    pub end_ns: u64,
}

impl Window {
    fn rel_ms(&self, t: u64) -> f64 {
        (t as f64 - self.origin_ns as f64) / 1e6
    }
}

/// Order statistics of an ascending slice, at most `max` evenly spaced.
fn downsample_sorted(sorted: &[f64], max: usize) -> Vec<f64> {
    if sorted.len() <= max || max == 0 {
        return sorted.to_vec();
    }
    (0..max)
        .map(|i| sorted[i * (sorted.len() - 1) / (max - 1)])
        .collect()
}

/// 2xx completions per second in 250 ms buckets from `data_start` to `end`.
fn rate_series(records: &[Record], w: &Window, warmup_secs: u32) -> Series {
    let span = w.end_ns.saturating_sub(w.data_start_ns);
    let n = span.div_ceil(BUCKET_NS) as usize;
    let mut counts = vec![0u64; n];
    for r in records {
        if matches!(r.outcome, Outcome::Ok(_)) && r.end_ns >= w.data_start_ns && r.end_ns < w.end_ns
        {
            counts[((r.end_ns - w.data_start_ns) / BUCKET_NS) as usize] += 1;
        }
    }
    let points = counts
        .iter()
        .enumerate()
        .filter_map(|(i, c)| {
            let start = w.data_start_ns + BUCKET_NS * i as u64;
            let end = (start + BUCKET_NS).min(w.end_ns);
            let len = end.saturating_sub(start);
            (len >= BUCKET_NS / 10).then(|| (w.rel_ms(end), *c as f64 / (len as f64 / 1e9)))
        })
        .collect();
    Series {
        key: "req_per_sec".into(),
        label: "Requests/s".into(),
        unit: "req/s".into(),
        points,
        warmup_ms: (warmup_secs > 0).then(|| w.rel_ms(w.warm_end_ns)),
    }
}

/// Turn the records into the workload output.
fn assemble(
    p: &HttpParams,
    port: u16,
    records: &[Record],
    connects_ms: &[f64],
    connections_opened: u64,
    w: &Window,
    server: Option<Stats>,
) -> WorkloadOutput {
    let measured: Vec<&Record> = records
        .iter()
        .filter(|r| r.start_ns >= w.warm_end_ns && r.start_ns < w.end_ns)
        .collect();
    let attempts = measured.len() as u64;
    let mut errors = Errors::default();
    let mut error_kinds: BTreeMap<String, u64> = BTreeMap::new();
    let mut status_counts: BTreeMap<String, u64> = BTreeMap::new();
    let mut latencies: Vec<Option<f64>> = Vec::new();
    let mut ok_lat: Vec<f64> = Vec::new();
    let mut body_bytes = 0u64;
    for r in &measured {
        match r.outcome {
            Outcome::Ok(s) => {
                *status_counts.entry(s.to_string()).or_default() += 1;
                latencies.push(Some(r.latency_ms));
                ok_lat.push(r.latency_ms);
                body_bytes += r.body;
            }
            Outcome::Status(s) => {
                *status_counts.entry(s.to_string()).or_default() += 1;
                errors.non_2xx += 1;
            }
            Outcome::Io(kind) => {
                *error_kinds.entry(kind.to_string()).or_default() += 1;
                errors.io += 1;
            }
            Outcome::Timeout => {
                errors.timeout += 1;
                latencies.push(None);
            }
        }
    }
    let ok = ok_lat.len() as u64;
    let latency = Distribution::from_samples(&latencies);
    ok_lat.sort_by(f64::total_cmp);
    let duration_secs = f64::from(p.duration_secs);
    let req_per_sec = (attempts > 0).then(|| ok as f64 / duration_secs);
    let error_pct = stats::pct(
        (errors.non_2xx + errors.io + errors.timeout) as f64,
        attempts as f64,
    );
    let window_ns = w.end_ns.saturating_sub(w.warm_end_ns);
    let goodput_mbps = stats::mbps(body_bytes as f64, window_ns);
    let summary = vec![
        SummaryItem::new(
            "req_per_sec",
            "Requests/s",
            req_per_sec,
            "req/s",
            Better::Higher,
        ),
        SummaryItem::new(
            "latency_p50_ms",
            "Latency p50",
            latency.p50,
            "ms",
            Better::Lower,
        ),
        SummaryItem::new(
            "latency_p99_ms",
            "Latency p99",
            latency.p99,
            "ms",
            Better::Lower,
        ),
        SummaryItem::new("error_pct", "Errors", error_pct, "%", Better::Lower),
    ];
    let mut notes = Vec::new();
    if p.target == HttpTarget::Proxy
        && p.connection == HttpConnection::Keepalive
        && p.concurrency > 2
    {
        notes.push(NOTE_PROXY_KEEPALIVE.to_string());
    }
    if p.connection == HttpConnection::New {
        notes.push(NOTE_NEW.to_string());
    }
    if let Some(s) = server.as_ref().filter(|s| s.forbidden > 0) {
        notes.push(format!(
            "the peer's server refused {} request{} with 403: {NODE_ID_HEADER} was missing or not this device's",
            s.forbidden,
            if s.forbidden == 1 { "" } else { "s" }
        ));
    }
    let warmup_requests = records
        .iter()
        .filter(|r| r.start_ns < w.warm_end_ns)
        .count() as u64;
    let results = Results {
        port,
        attempts,
        ok,
        errors,
        error_kinds,
        status_counts,
        error_pct,
        req_per_sec,
        latency_ms: latency,
        samples_ms: downsample_sorted(&ok_lat, MAX_RAW_SAMPLES),
        samples_total: ok,
        connect_ms: stats::median(connects_ms),
        connect: Distribution::from_values(connects_ms),
        connections_opened,
        body_bytes,
        goodput_mbps,
        warmup_requests,
        server,
    };
    WorkloadOutput {
        summary,
        results: serde_json::to_value(&results).unwrap_or_default(),
        series: vec![rate_series(records, w, p.warmup_secs)],
        notes,
        window_ns: Some((w.warm_end_ns, w.end_ns)),
        goodput_bytes: Some(body_bytes),
    }
}

/// Initiator: run the workers for warmup + duration and tabulate.
pub async fn run(ctx: &mut RunCtx<'_>, p: &HttpParams) -> Result<WorkloadOutput, AppError> {
    let setup: Setup = ctx.prepare(&Client {}).await?;
    let p = match ctx.params() {
        WorkloadParams::Http(used) => used.clone(),
        _ => p.clone(),
    };
    let hex = ctx.run_tag_hex()?;
    let head = Arc::new(request_head(
        &hex,
        p.response_bytes,
        p.connection == HttpConnection::New,
    ));
    ctx.start().await?;
    let data_start = Instant::now();
    let warm_end = data_start + Duration::from_secs(u64::from(p.warmup_secs));
    let end = warm_end + Duration::from_secs(u64::from(p.duration_secs));
    let w = Window {
        origin_ns: ctx.origin_ns(),
        data_start_ns: instant_ns(data_start.into_std()),
        warm_end_ns: instant_ns(warm_end.into_std()),
        end_ns: instant_ns(end.into_std()),
    };
    ctx.phase(if p.warmup_secs > 0 {
        Phase::Warmup
    } else {
        Phase::Running
    });
    let cancel = ctx.cancel_token();
    let live = Arc::new(Mutex::new(Live::default()));
    let worker = Worker {
        node: ctx.node.clone(),
        peer_id: ctx.peer_id().to_string(),
        port: setup.port,
        head,
        mode: p.connection,
        warm_end,
        end,
        live: live.clone(),
        cancel: cancel.clone(),
    };
    let handles: Vec<JoinHandle<WorkerOut>> = (0..p.concurrency)
        .map(|_| tokio::spawn(worker.clone().run()))
        .collect();
    let live_task = {
        let samples = ctx.live();
        let live = live.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_millis(250));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            tick.tick().await;
            let mut last = Instant::now();
            loop {
                tick.tick().await;
                let now = Instant::now();
                let (rate, p50, err) = {
                    let mut l = live.lock().unwrap_or_else(PoisonError::into_inner);
                    let rate = l.ok_since as f64 / now.duration_since(last).as_secs_f64();
                    l.ok_since = 0;
                    let lat = if l.measured_latencies.is_empty() {
                        &l.warmup_latencies
                    } else {
                        &l.measured_latencies
                    };
                    let p50 = median_of(lat);
                    let err = stats::pct(l.measured_errors as f64, l.measured_attempts as f64);
                    (rate, p50, err)
                };
                last = now;
                samples.set("req_per_sec", rate);
                if let Some(v) = p50 {
                    samples.set("latency_p50_ms", v);
                }
                if let Some(v) = err {
                    samples.set("error_pct", v);
                }
            }
        })
    };
    let joined = futures::future::join_all(handles);
    tokio::pin!(joined);
    let mut running = p.warmup_secs == 0;
    let mut draining = false;
    let outs = loop {
        tokio::select! {
            outs = &mut joined => break outs,
            _ = tokio::time::sleep_until(warm_end), if !running => {
                ctx.phase(Phase::Running);
                running = true;
            }
            _ = tokio::time::sleep_until(end), if !draining => {
                ctx.phase(Phase::Draining);
                draining = true;
            }
        }
    };
    live_task.abort();
    ctx.check_cancelled()?;
    let mut records = Vec::new();
    let mut connects = Vec::new();
    let mut opened = 0u64;
    for o in outs {
        let o = o.map_err(|e| AppError::internal(format!("http worker: {e}")))?;
        records.extend(o.records);
        connects.extend(o.connects_ms);
        opened += o.connections_opened;
    }
    records.sort_by_key(|r| r.start_ns);
    let partial = assemble(&p, setup.port, &records, &connects, opened, &w, None);
    ctx.set_partial(partial);
    let stats: Stats = ctx
        .finish(
            &Info {
                requests: records.len() as u64,
            },
            Duration::ZERO,
        )
        .await?;
    let out = assemble(&p, setup.port, &records, &connects, opened, &w, Some(stats));
    let ok = records
        .iter()
        .filter(|r| r.start_ns >= w.warm_end_ns && matches!(r.outcome, Outcome::Ok(_)))
        .count();
    if ok == 0 {
        let r: Results = serde_json::from_value(out.results.clone()).unwrap_or_default();
        ctx.set_partial(out);
        return Err(AppError::new(
            ErrorCode::Io,
            format!(
                "no request succeeded: {} attempts, {} non-2xx {:?}, {} I/O errors {:?}, {} timeouts",
                r.attempts,
                r.errors.non_2xx,
                r.status_counts,
                r.errors.io,
                r.error_kinds,
                r.errors.timeout
            ),
        ));
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

/// A canonical example output for the schema fixtures (proxy, keep-alive, 4 workers, 1 KiB).
pub fn example_output() -> WorkloadOutput {
    let p = HttpParams::default();
    let base = 1_760_000_000_000_000_000u64;
    let w = Window {
        origin_ns: base,
        data_start_ns: base + 400_000_000,
        warm_end_ns: base + 1_400_000_000,
        end_ns: base + 11_400_000_000,
    };
    // 4 workers, one request every ~1.9 ms each, deterministic latencies.
    let mut records = Vec::new();
    let mut t = w.data_start_ns;
    let mut i = 0u64;
    while t < w.end_ns {
        let lat_us = 1_500 + (i * 7919 % 1_400) + if i.is_multiple_of(211) { 3_000 } else { 0 };
        let outcome = if i % 997 == 500 {
            Outcome::Status(502)
        } else {
            Outcome::Ok(200)
        };
        records.push(Record {
            start_ns: t,
            end_ns: t + lat_us * 1000,
            outcome,
            latency_ms: lat_us as f64 / 1000.0,
            body: if matches!(outcome, Outcome::Ok(_)) {
                1024
            } else {
                0
            },
        });
        t += 470_000;
        i += 1;
    }
    let server = Stats {
        requests: records.len() as u64,
        ok: records
            .iter()
            .filter(|r| matches!(r.outcome, Outcome::Ok(_)))
            .count() as u64,
        forbidden: 0,
        bad_request: 0,
        not_found: 0,
        body_bytes: records.iter().map(|r| r.body).sum(),
        connections: 9,
        foreign_dropped: 0,
    };
    let mut out = assemble(
        &p,
        21_337,
        &records,
        &[11.8, 12.4, 12.9, 13.6],
        4,
        &w,
        Some(server),
    );
    // Keep the fixture small: a 40-sample CDF.
    if let Some(obj) = out.results.as_object_mut() {
        let raw: Vec<f64> = obj
            .get("samples_ms")
            .and_then(Value::as_array)
            .map(|a| a.iter().filter_map(Value::as_f64).collect())
            .unwrap_or_default();
        obj.insert(
            "samples_ms".into(),
            serde_json::to_value(downsample_sorted(&raw, 40)).unwrap_or_default(),
        );
    }
    out
}

/// Direct target, a new connection per request, 4 workers: each new
/// tailnet connection waits its turn (~40 ms each), so ~24 req/s at ~160 ms.
fn example_direct_new() -> WorkloadOutput {
    let p = HttpParams {
        target: HttpTarget::Direct,
        connection: HttpConnection::New,
        ..HttpParams::default()
    };
    let base = 1_760_000_000_000_000_000u64;
    let w = Window {
        origin_ns: base,
        data_start_ns: base + 380_000_000,
        warm_end_ns: base + 1_380_000_000,
        end_ns: base + 11_380_000_000,
    };
    let mut records = Vec::new();
    let mut connects = Vec::new();
    for worker in 0..u64::from(p.concurrency) {
        let mut t = w.data_start_ns + worker * 41_000_000;
        let mut i = worker;
        while t < w.end_ns {
            let connect_us = 118_000 + (i * 7919 % 41_000);
            let lat_us = connect_us + 1_100 + (i * 104_729 % 900);
            records.push(Record {
                start_ns: t,
                end_ns: t + lat_us * 1000,
                outcome: Outcome::Ok(200),
                latency_ms: lat_us as f64 / 1000.0,
                body: p.response_bytes,
            });
            connects.push(connect_us as f64 / 1000.0);
            t += lat_us * 1000 + 250_000;
            i += 4;
        }
    }
    records.sort_by_key(|r| r.start_ns);
    let ok = records.len() as u64;
    let server = Stats {
        requests: ok,
        ok,
        forbidden: 0,
        bad_request: 0,
        not_found: 0,
        body_bytes: records.iter().map(|r| r.body).sum(),
        connections: ok,
        foreign_dropped: 0,
    };
    let mut out = assemble(&p, 52_114, &records, &connects, ok, &w, Some(server));
    // Keep the fixture small: a 40-sample CDF.
    if let Some(obj) = out.results.as_object_mut() {
        let raw: Vec<f64> = obj
            .get("samples_ms")
            .and_then(Value::as_array)
            .map(|a| a.iter().filter_map(Value::as_f64).collect())
            .unwrap_or_default();
        obj.insert(
            "samples_ms".into(),
            serde_json::to_value(downsample_sorted(&raw, 40)).unwrap_or_default(),
        );
    }
    out
}

/// One example per http mode for the schema fixtures.
pub fn examples() -> Vec<super::ModeExample> {
    vec![
        super::ModeExample {
            mode: "http-proxy-keepalive",
            params: WorkloadParams::Http(HttpParams::default()),
            output: example_output(),
            clock: false,
        },
        super::ModeExample {
            mode: "http-direct-new",
            params: WorkloadParams::Http(HttpParams {
                target: HttpTarget::Direct,
                connection: HttpConnection::New,
                ..HttpParams::default()
            }),
            output: example_direct_new(),
            clock: false,
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proxy_ports_follow_the_run_tag_and_wrap() {
        assert_eq!(proxy_port(0, 0), 20_000);
        assert_eq!(proxy_port(12_345, 0), 22_345);
        assert_eq!(proxy_port(12_345, 7), 22_352);
        assert_eq!(proxy_port(9_999, 1), 20_000);
        assert_eq!(proxy_port(u64::MAX, 0), 20_000 + (u64::MAX % 10_000) as u16);
        for a in 0..PROXY_ATTEMPTS {
            let p = proxy_port(9_996, a);
            assert!((20_000..30_000).contains(&p) && p != 0);
        }
        assert!(port_retryable(
            "[PORT_IN_USE] port 20001 already used by proxy x"
        ));
        assert!(port_retryable(
            "proxy error: [LISTEN_ERROR] listen tcp :20001: in use"
        ));
        assert!(!port_retryable("[NOT_READY] node lifecycle is not running"));
        // A refusal registered nothing; a timeout may have registered it.
        assert!(sidecar_refused(
            "proxy error: [ID_IN_USE] proxy bench-1 exists"
        ));
        assert!(sidecar_refused("[NOT_READY] node lifecycle is not running"));
        assert!(!sidecar_refused("proxy error: proxy add timed out"));
        assert!(!sidecar_refused("sidecar error: event channel closed"));
        assert!(!sidecar_refused("a [bracketed] note"));
    }

    #[test]
    fn requests_parse_and_route() {
        let head =
            b"GET /r/00ab/bytes/512?x=1 HTTP/1.1\r\nHost: a\r\ntailscale-node-id: nABC\r\n\r\n";
        let req = parse_request(head).unwrap();
        assert_eq!(req.method, "GET");
        assert_eq!(req.node_id.as_deref(), Some("nABC"));
        assert!(!req.close);
        assert_eq!(route(&req, "00ab", 1024, Some("nABC")), (200, 512));
        assert_eq!(route(&req, "00ab", 1024, None), (200, 512));
        assert_eq!(route(&req, "00ab", 100, None), (400, 0));
        assert_eq!(route(&req, "00ab", 1024, Some("nOTHER")), (403, 0));
        assert_eq!(route(&req, "ffff", 1024, None), (404, 0));
        let bad_n = parse_request(b"GET /r/00ab/bytes/12x HTTP/1.1\r\n\r\n").unwrap();
        assert_eq!(route(&bad_n, "00ab", 1024, None), (400, 0));
        let empty = parse_request(b"GET /r/00ab/bytes/ HTTP/1.1\r\n\r\n").unwrap();
        assert_eq!(route(&empty, "00ab", 1024, None), (400, 0));
        let post =
            parse_request(b"POST /r/00ab/bytes/1 HTTP/1.1\r\nContent-Length: 3\r\n\r\n").unwrap();
        assert_eq!(post.content_length, 3);
        assert_eq!(route(&post, "00ab", 1024, None), (404, 0));
        let other = parse_request(b"GET / HTTP/1.1\r\n\r\n").unwrap();
        assert_eq!(route(&other, "00ab", 1024, None), (404, 0));
        // A missing node id is refused on the proxy path.
        let anon = parse_request(b"GET /r/00ab/bytes/1 HTTP/1.1\r\n\r\n").unwrap();
        assert_eq!(route(&anon, "00ab", 1024, Some("nABC")), (403, 0));
        // Connection handling.
        let close = parse_request(b"GET / HTTP/1.1\r\nConnection: close\r\n\r\n").unwrap();
        assert!(close.close);
        let old = parse_request(b"GET / HTTP/1.0\r\n\r\n").unwrap();
        assert!(old.close);
        let old_ka = parse_request(b"GET / HTTP/1.0\r\nConnection: Keep-Alive\r\n\r\n").unwrap();
        assert!(!old_ka.close);
        assert!(parse_request(b"GET\r\n\r\n").is_err());
        assert!(parse_request(b"GET / HTTP/2\r\n\r\n").is_err());
        assert!(parse_request(b"GET / HTTP/1.1\r\nbroken\r\n\r\n").is_err());
        assert!(parse_request(b"GET / HTTP/1.1\r\nContent-Length: x\r\n\r\n").is_err());
    }

    #[test]
    fn responses_parse() {
        let h = parse_response(b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\n").unwrap();
        assert_eq!(
            (h.status, h.content_length, h.chunked, h.close),
            (200, Some(10), false, false)
        );
        let h = parse_response(b"HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n").unwrap();
        assert_eq!((h.status, h.content_length, h.close), (403, None, true));
        let h = parse_response(
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nContent-Length: 4\r\n\r\n",
        )
        .unwrap();
        assert!(h.chunked && h.content_length.is_none());
        assert!(parse_response(b"HTTP/1.1 abc\r\n\r\n").is_err());
        let ours = response_head(200, 5, true);
        let h = parse_response(ours.as_bytes()).unwrap();
        assert_eq!((h.status, h.content_length, h.close), (200, Some(5), true));
        let req = request_head("00ab", 7, true);
        let r = parse_request(&req).unwrap();
        assert_eq!(r.target, "/r/00ab/bytes/7");
        assert!(r.close);
    }

    #[tokio::test]
    async fn server_and_client_round_trip_bodies() {
        let server = Arc::new(Server {
            run_tag_hex: "00ab".into(),
            max_bytes: 200_000,
            node_id: None,
            warmup_ns: 0,
            counters: Counters::default(),
        });
        let (mut client, srv) = tokio::io::duplex(64 * 1024);
        let s = server.clone();
        let task = tokio::spawn(async move { s.serve(srv).await });
        let mut buf = Vec::new();
        let mut scratch = vec![0u8; 8192];
        for (n, want) in [(0u64, 200u16), (1024, 200), (150_000, 200), (200_001, 400)] {
            client
                .write_all(&request_head("00ab", n, false))
                .await
                .unwrap();
            let (status, body, close) = read_response(&mut client, &mut buf, &mut scratch)
                .await
                .unwrap();
            assert_eq!(status, want, "n={n}");
            assert_eq!(body, if want == 200 { n } else { 0 });
            assert!(!close);
        }
        // A wrong path, then a closing request.
        client
            .write_all(b"GET /nope HTTP/1.1\r\n\r\n")
            .await
            .unwrap();
        let (status, _, _) = read_response(&mut client, &mut buf, &mut scratch)
            .await
            .unwrap();
        assert_eq!(status, 404);
        client
            .write_all(&request_head("00ab", 3, true))
            .await
            .unwrap();
        let (status, body, close) = read_response(&mut client, &mut buf, &mut scratch)
            .await
            .unwrap();
        assert_eq!((status, body, close), (200, 3, true));
        task.await.unwrap();
        let st = server.stats(0);
        assert_eq!(
            (st.requests, st.ok, st.bad_request, st.not_found),
            (6, 4, 1, 1)
        );
        assert_eq!(st.body_bytes, 1024 + 150_000 + 3);
        assert_eq!(st.connections, 1);
    }

    #[tokio::test]
    async fn chunked_and_eof_bodies_are_read() {
        let (mut a, mut b) = tokio::io::duplex(4096);
        a.write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n4\r\nabcd\r\n3;x=y\r\nefg\r\n0\r\nTrailer: 1\r\n\r\nHTTP/1.0 200 OK\r\n\r\nrest-of-body")
            .await
            .unwrap();
        drop(a);
        let mut buf = Vec::new();
        let mut scratch = vec![0u8; 16];
        let (s, n, close) = read_response(&mut b, &mut buf, &mut scratch).await.unwrap();
        assert_eq!((s, n, close), (200, 7, false));
        let (s, n, close) = read_response(&mut b, &mut buf, &mut scratch).await.unwrap();
        assert_eq!((s, n, close), (200, 12, true));
        let err = read_response(&mut b, &mut buf, &mut scratch)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn assembly_counts_only_the_measured_window() {
        let p = HttpParams {
            duration_secs: 2,
            warmup_secs: 1,
            concurrency: 1,
            ..HttpParams::default()
        };
        let s = 1_000_000_000u64;
        let w = Window {
            origin_ns: 0,
            data_start_ns: s,
            warm_end_ns: 2 * s,
            end_ns: 4 * s,
        };
        let rec = |start: u64, outcome: Outcome, ms: f64| Record {
            start_ns: start,
            end_ns: start + (ms * 1e6) as u64,
            outcome,
            latency_ms: ms,
            body: if matches!(outcome, Outcome::Ok(_)) {
                100
            } else {
                0
            },
        };
        let records = vec![
            rec(s + 1, Outcome::Ok(200), 5.0), // warmup
            rec(2 * s, Outcome::Ok(200), 2.0), // measured
            rec(2 * s + 10, Outcome::Ok(200), 4.0),
            rec(3 * s, Outcome::Status(503), 1.0),
            rec(3 * s + 5, Outcome::Io("reset"), 1.0),
            rec(3 * s + 9, Outcome::Timeout, 10_000.0),
            rec(4 * s, Outcome::Ok(200), 1.0), // after the end: not measured
        ];
        let out = assemble(&p, 20_001, &records, &[3.0, 5.0], 2, &w, None);
        let r: Results = serde_json::from_value(out.results).unwrap();
        assert_eq!(r.attempts, 5);
        assert_eq!(r.ok, 2);
        assert_eq!(
            r.errors,
            Errors {
                non_2xx: 1,
                io: 1,
                timeout: 1
            }
        );
        assert_eq!(r.error_pct, Some(60.0));
        assert_eq!(r.req_per_sec, Some(1.0));
        assert_eq!(r.latency_ms.samples, 3);
        assert_eq!(r.latency_ms.lost, 1);
        assert_eq!(r.latency_ms.p50, Some(4.0));
        assert_eq!(r.samples_ms, vec![2.0, 4.0]);
        assert_eq!(r.warmup_requests, 1);
        assert_eq!(r.body_bytes, 200);
        assert_eq!(r.connect_ms, Some(4.0));
        assert_eq!(r.error_kinds.get("reset"), Some(&1));
        assert_eq!(r.status_counts.get("503"), Some(&1));
        assert_eq!(out.goodput_bytes, Some(200));
        assert_eq!(out.window_ns, Some((2 * s, 4 * s)));
        let keys: Vec<&str> = out.summary.iter().map(|s| s.key.as_str()).collect();
        assert_eq!(
            keys,
            [
                "req_per_sec",
                "latency_p50_ms",
                "latency_p99_ms",
                "error_pct"
            ]
        );
        // Series: 12 buckets of 250 ms over 3 s, warmup flagged at 2000 ms.
        assert_eq!(out.series[0].points.len(), 12);
        assert_eq!(out.series[0].warmup_ms, Some(2000.0));
        assert_eq!(out.series[0].points[0], (1250.0, 4.0));
        // Notes follow the params.
        assert!(out.notes.is_empty());
        let new = HttpParams {
            connection: HttpConnection::New,
            ..p.clone()
        };
        let out = assemble(&new, 1, &records, &[], 0, &w, None);
        assert_eq!(out.notes, vec![NOTE_NEW.to_string()]);
        let ka = HttpParams {
            concurrency: 3,
            ..p
        };
        let out = assemble(&ka, 1, &records, &[], 0, &w, None);
        assert_eq!(out.notes, vec![NOTE_PROXY_KEEPALIVE.to_string()]);
    }

    #[test]
    fn helpers() {
        assert_eq!(downsample_sorted(&[1.0, 2.0, 3.0], 5), vec![1.0, 2.0, 3.0]);
        let v: Vec<f64> = (0..101).map(f64::from).collect();
        let d = downsample_sorted(&v, 11);
        assert_eq!(d.len(), 11);
        assert_eq!((d[0], d[5], d[10]), (0.0, 50.0, 100.0));
        assert_eq!(median_of(&[3.0, 1.0, 2.0]), Some(2.0));
        assert_eq!(median_of(&[]), None);
        assert_eq!(outcome_of(204), Outcome::Ok(204));
        assert_eq!(outcome_of(302), Outcome::Status(302));
        let e = io::Error::new(io::ErrorKind::ConnectionReset, "x");
        assert_eq!(io_kind(&e), "reset");
    }

    #[test]
    fn example_output_matches_the_results_type() {
        let out = example_output();
        let r: Results = serde_json::from_value(out.results.clone()).unwrap();
        assert!(r.attempts > 1000);
        assert!(r.samples_ms.len() <= 40);
        assert!(out.summary.iter().all(|s| s.value.is_some()));
        assert_eq!(out.notes, vec![NOTE_PROXY_KEEPALIVE.to_string()]);
    }
}
