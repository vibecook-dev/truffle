//! `tcp` (RFC 027 §8.1 receiver accounting, goodput, series, sender checks,
//! bidirectional and latency modes; §8.2 tcp throughput and tcp latency).
//!
//! **Throughput.** The initiator opens `streams` connections per direction
//! with `open_tcp` (each call timed: `connect_ms`), applies `nodelay` to the
//! Rust `TcpStream`, writes the 16-byte TBN1 header and then:
//!
//! - upload (`kind = 0`): writes 64 KiB chunks for warmup + duration, shuts
//!   its write half down and reads the receiver's `[u64 BE stream_bytes]`.
//!   The responder counts every stream into one run-level
//!   [`ReceiverAccounting`] and returns
//!   its [`ReceiverStats`] in `finish.ack.stats`;
//! - download (`kind = 1`): the responder writes for warmup + duration from
//!   the first download header it sees; the initiator counts locally;
//! - bidirectional: both at once (`streams` each way), each direction
//!   counted by its own receiver. Each direction's goodput is taken over the
//!   overlap window `[max(epochs) + warmup, min(lasts)]`, with the upload
//!   receiver's times mapped into the initiator's timebase by the before and
//!   after clock estimates; the headline is their sum.
//!
//! Every stream's ack is a barrier and a cross-check: the run fails if
//! Σ buckets ≠ bytes_total, if a stream received more than its sender wrote,
//! or if an ack disagrees with the receiver's count. Responder-side series
//! (upload) are mapped into the initiator's timebase with `offset(t)`.
//!
//! **Latency.** One stream (`kind = 2`): `payload_bytes` written with one
//! `write_all`, echoed back by the responder, `count` times after
//! `warmup_count` discarded ones. RTT runs from the write call to the last
//! reply byte, on one clock.
//!
//! The responder side (the "TCP sink": `listen_tcp(0)`, rule 5 identity
//! check, then per-connection header dispatch) and the throughput initiator
//! are shared with [`egress`](super::egress), which reaches the same sink
//! through the sidecar's egress listener instead of `open_tcp`.
//!
//! Summary keys: throughput `goodput_mbps` (+ `upload_mbps`, `download_mbps`
//! when bidirectional), `connect_ms`, `cpu_ms_per_mb`, `loaded_rtt_p50_ms`;
//! latency `rtt_p50_ms`, `rtt_p99_ms`, `rtt_max_ms`.
//! Series: `goodput_mbps` (+ `upload_mbps`, `download_mbps` when
//! bidirectional) for throughput, `rtt_ms` for latency.
//! Live samples: `goodput_mbps` (responder ticks for upload, local for
//! download), `upload_mbps`/`download_mbps` when bidirectional, sender-side
//! `sent_mbps`; latency `rtt_ms` (median of the last 250 ms) and
//! `rtt_p50_ms` (running).

use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use futures::future::BoxFuture;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use truffle_core::transport::RawListener;

use crate::clock::now_ns;
use crate::control::Teardown;
use crate::dataplane::{
    max_data_span_ns, payload_chunk, write_header, ReceiverAccounting, ReceiverStats,
    SharedAccounting, StreamHeader, StreamKind, BUCKET_NS, MAX_SERIES_SPAN_NS, MIN_FINAL_BUCKET_NS,
};
use crate::error::{AppError, ErrorCode};
use crate::hub::BenchNode;
use crate::params::{Direction, OneWayDirection, StreamMode, TcpParams, WorkloadParams};
use crate::stats;
use crate::types::{Better, Distribution, Phase, Series, SummaryItem};
use crate::workloads::common::{
    cpu_per_mb_item, dur_ms, loaded_rtt_item, receive_stream, send_stream, spawn_live_goodput,
    spawn_live_rate, spawn_tick_goodput, LiveRtt, Tasks, Tbn1Acceptor,
};
use crate::workloads::{ResponderCtx, ResponderRun, RunCtx, WorkloadOutput};

/// Whether this build offers `tcp` (drives `PeerHello.workloads`).
pub const IMPLEMENTED: bool = true;

/// The Progress sample keys a run with these params can set: the
/// initiator's own and the responder's `tick`s, which the hub merges in.
/// [`live_keys`](super::live_keys) adds the path sampler's `loaded_rtt_ms`
/// for time-based runs; `live-keys.json` in the schema fixtures is built
/// from it.
pub fn live_keys(p: &TcpParams) -> Vec<&'static str> {
    match p.mode {
        StreamMode::Latency => vec!["rtt_ms", "rtt_p50_ms"],
        StreamMode::Throughput => throughput_live_keys(p.direction),
    }
}

/// [`run_throughput`]'s live keys (tcp, quic throughput and egress share
/// them): the sender's `sent_mbps`; `goodput_mbps` from the responder's
/// ticks on upload and from this side on download; both ways, the ticked
/// `upload_mbps`, this side's `download_mbps` and their sum as
/// `goodput_mbps`.
pub(crate) fn throughput_live_keys(direction: Direction) -> Vec<&'static str> {
    match direction {
        Direction::Upload => vec!["sent_mbps", "goodput_mbps"],
        Direction::Download => vec!["goodput_mbps"],
        Direction::Bidirectional => {
            vec!["sent_mbps", "upload_mbps", "download_mbps", "goodput_mbps"]
        }
    }
}

/// How long one `open_tcp` (or egress connect) may take.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
/// How long streams may run past their data period before the run fails.
const STREAM_GRACE: Duration = Duration::from_secs(15);
/// How long one latency round trip may take before the run fails (TCP
/// never loses a message, so a timeout means a stuck connection).
const ECHO_TIMEOUT: Duration = Duration::from_secs(10);
/// How long the sink waits for its connection tasks at `finish`.
const SINK_FINISH_WAIT: Duration = Duration::from_secs(5);
/// The drain allowance added to the `finish` timeout.
const FINISH_DRAIN: Duration = Duration::from_secs(5);

/// `prepare.client`: nothing beyond the params (direction, streams and sizes
/// all come from them).
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "TcpClient")]
pub struct Client {}

/// `prepare.ack.setup`: the sink's tailnet port.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "TcpSetup")]
pub struct Setup {
    /// The port `listen_tcp(0)` bound.
    pub port: u16,
}

/// One stream as its sender saw it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "TcpSentStream")]
pub struct SentStreamInfo {
    /// Stream index.
    pub stream_idx: u16,
    /// Payload bytes written.
    pub sent_bytes: u64,
    /// The receiver's `[u64 BE stream_bytes]` ack (0 when none arrived).
    pub acked_bytes: u64,
}

/// `finish.info`: the initiator's sender-side counts (§8.1 "Sender checks").
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "TcpInfo")]
pub struct Info {
    /// Every stream the initiator wrote (upload and latency streams).
    pub streams: Vec<SentStreamInfo>,
}

/// `finish.ack.stats`: the sink's numbers.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "TcpStats")]
pub struct Stats {
    /// The upload receiver's accounting (responder timebase), when the
    /// responder received.
    pub upload: Option<ReceiverStats>,
    /// Every download stream the responder wrote.
    pub download: Vec<SentStreamInfo>,
    /// Latency messages echoed.
    pub echoed: u64,
    /// Foreign connections and bad headers dropped (§6 rule 5).
    pub foreign_dropped: u64,
    /// Stream errors the sink saw.
    pub errors: Vec<String>,
}

/// One stream of a throughput run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "TcpStreamResult")]
pub struct StreamResult {
    /// Stream index (unique within the run).
    pub stream_idx: u16,
    /// Which way its payload flowed.
    pub direction: OneWayDirection,
    /// `open_tcp` call → return (egress: the loopback connect), ms.
    pub connect_ms: f64,
    /// Payload bytes its sender wrote.
    pub sent_bytes: u64,
    /// Payload bytes its receiver counted.
    pub received_bytes: u64,
    /// The receiver's ack, as the sender read it.
    pub acked_bytes: u64,
}

/// One direction of a bidirectional run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "TcpDirectionResult")]
pub struct DirectionResult {
    /// Goodput over the overlap window, Mb/s.
    pub goodput_mbps: f64,
    /// Payload bytes in the overlap window (partial buckets prorated).
    pub window_bytes: f64,
    /// This direction's receiver accounting, in the receiver's own timebase.
    pub receiver: ReceiverStats,
}

/// `results` of a throughput run (tcp and egress).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "TcpThroughputResults")]
pub struct ThroughputResults {
    /// Upload, download or bidirectional.
    pub direction: Direction,
    /// Every stream, both directions.
    pub streams: Vec<StreamResult>,
    /// Payload bytes received, all directions.
    pub bytes_total: u64,
    /// Same as `bytes_total` (the §12 `received ≤ sent` check reads it).
    pub received_bytes: u64,
    /// Payload bytes written, all directions.
    pub sent_bytes: u64,
    /// Payload bytes inside the measurement window.
    pub goodput_bytes: u64,
    /// Length of the measurement window, ms.
    pub window_ms: u64,
    /// `connect_ms` over every stream.
    pub connect_ms: Distribution,
    /// The receiver's accounting (receiver timebase) for one-way runs.
    pub receiver: Option<ReceiverStats>,
    /// Bidirectional: the upload direction.
    pub upload: Option<DirectionResult>,
    /// Bidirectional: the download direction.
    pub download: Option<DirectionResult>,
    /// Foreign connections and bad headers the sink dropped.
    pub foreign_dropped: u64,
    /// `TCP_NODELAY` on both Rust sockets.
    pub nodelay: bool,
}

/// `results` of a latency run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "TcpLatencyResults")]
pub struct LatencyResults {
    /// Message size.
    pub payload_bytes: u32,
    /// Measured round trips requested.
    pub count: u32,
    /// Discarded round trips before them.
    pub warmup_count: u32,
    /// `TCP_NODELAY` on both Rust sockets.
    pub nodelay: bool,
    /// `open_tcp` call → return, ms.
    pub connect_ms: f64,
    /// Round trips after the warmup.
    pub rtt_ms: Distribution,
    /// The raw round trips after the warmup, in order (for the CDF).
    pub samples_ms: Vec<f64>,
    /// Round trips completed after the warmup.
    pub completed: u32,
    /// Payload bytes written (warmup included).
    pub sent_bytes: u64,
    /// Payload bytes read back.
    pub received_bytes: u64,
    /// Messages the responder echoed (warmup included).
    pub echoed: u64,
    /// Foreign connections and bad headers the sink dropped.
    pub foreign_dropped: u64,
}

/// `RunReport.results` of a `tcp` run, tagged by `mode`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "mode", rename_all = "lowercase")]
#[schemars(rename = "TcpResults")]
pub enum Results {
    /// `mode: "throughput"`.
    Throughput(Box<ThroughputResults>),
    /// `mode: "latency"`.
    Latency(Box<LatencyResults>),
}

// ---------------------------------------------------------------------------
// Small shared pieces (also used by udp.rs and egress.rs)
// ---------------------------------------------------------------------------

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

fn secs(s: u32) -> Duration {
    Duration::from_secs(u64::from(s))
}

/// A summary item.
pub(crate) fn item(
    key: &str,
    label: &str,
    value: Option<f64>,
    unit: &str,
    better: Better,
) -> SummaryItem {
    SummaryItem::new(key, label, value, unit, better)
}

/// The params as the responder echoed them (falls back to `p`).
fn echoed_params(ctx: &RunCtx<'_>, p: &TcpParams) -> TcpParams {
    match ctx.params() {
        WorkloadParams::Tcp(e) => e.clone(),
        _ => p.clone(),
    }
}

/// How the initiator reaches the sink.
#[derive(Debug, Clone)]
pub(crate) enum Dial {
    /// `node.open_tcp(peer_id, port)` (two Rust bridges).
    Tailnet {
        /// The peer's tailscale id.
        peer_id: String,
        /// The sink's port.
        port: u16,
    },
    /// A plain loopback connect to the sidecar's egress listener (one bridge).
    Loopback {
        /// `127.0.0.1:<port>`.
        addr: String,
    },
}

impl Dial {
    async fn open(&self, node: &BenchNode) -> Result<TcpStream, AppError> {
        let fut = async {
            match self {
                Self::Tailnet { peer_id, port } => node
                    .open_tcp(peer_id, *port)
                    .await
                    .map_err(|e| AppError::from(e).context("open_tcp")),
                Self::Loopback { addr } => TcpStream::connect(addr)
                    .await
                    .map_err(|e| AppError::io(format!("connect to {addr}"), &e)),
            }
        };
        tokio::time::timeout(CONNECT_TIMEOUT, fut)
            .await
            .map_err(|_| {
                AppError::timeout(format!(
                    "the connection did not open within {} s",
                    CONNECT_TIMEOUT.as_secs()
                ))
            })?
    }
}

// ---------------------------------------------------------------------------
// The sink (responder side, shared with egress)
// ---------------------------------------------------------------------------

/// What the sink serves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SinkConfig {
    /// Throughput direction (ignored for latency).
    pub direction: Direction,
    /// Streams per direction.
    pub streams: u32,
    /// Warmup seconds.
    pub warmup_secs: u32,
    /// Measured seconds.
    pub duration_secs: u32,
    /// `TCP_NODELAY` on accepted streams.
    pub nodelay: bool,
    /// `Some(payload_bytes)` for a latency (echo) run.
    pub echo_payload: Option<u32>,
}

impl SinkConfig {
    fn accepts_upload(&self) -> bool {
        self.echo_payload.is_none() && self.direction != Direction::Download
    }

    fn accepts_download(&self) -> bool {
        self.echo_payload.is_none() && self.direction != Direction::Upload
    }

    /// The live key the sink ticks for its upload receiver.
    fn tick_key(&self) -> Option<&'static str> {
        if !self.accepts_upload() {
            None
        } else if self.direction == Direction::Bidirectional {
            Some("upload_mbps")
        } else {
            Some("goodput_mbps")
        }
    }
}

struct SinkShared {
    cfg: SinkConfig,
    upload: SharedAccounting,
    download: Mutex<Vec<SentStreamInfo>>,
    /// `(until, start_ns)` of the download writers, fixed by the first
    /// download header.
    download_window: Mutex<Option<(Instant, u64)>>,
    uploads_seen: AtomicU32,
    downloads_seen: AtomicU32,
    echoes_seen: AtomicU32,
    echoed: AtomicU64,
    errors: Mutex<Vec<String>>,
}

impl SinkShared {
    /// Take a slot for a stream of `kind`; `false` when the run does not
    /// expect (more of) it.
    fn admit_kind(&self, kind: StreamKind) -> bool {
        let take = |seen: &AtomicU32, limit: u32| {
            seen.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                (n < limit).then_some(n + 1)
            })
            .is_ok()
        };
        match kind {
            StreamKind::Upload => {
                self.cfg.accepts_upload() && take(&self.uploads_seen, self.cfg.streams)
            }
            StreamKind::Download => {
                self.cfg.accepts_download() && take(&self.downloads_seen, self.cfg.streams)
            }
            StreamKind::Echo => self.cfg.echo_payload.is_some() && take(&self.echoes_seen, 1),
            StreamKind::Frames => false,
        }
    }

    fn download_until(&self) -> Instant {
        lock(&self.download_window)
            .get_or_insert_with(|| {
                let data = secs(self.cfg.warmup_secs + self.cfg.duration_secs);
                (Instant::now() + data, now_ns())
            })
            .0
    }
}

/// Bind the sink (`listen_tcp(0)`) and start accepting: every connection
/// from the initiator is identity-checked (rule 5), its TBN1 header read
/// (wrong magic or tag: dropped and counted) and served by kind. Returns
/// the bound port and the run.
pub(crate) async fn prepare_sink(
    r: &ResponderCtx,
    cfg: SinkConfig,
) -> Result<(u16, Box<dyn ResponderRun>), AppError> {
    let listener = r
        .node
        .listen_tcp(0)
        .await
        .map_err(|e| AppError::from(e).context("listen_tcp"))?;
    let port = listener.port;
    let mut teardown = r.teardown();
    teardown.tcp_port(port);
    let shared = Arc::new(SinkShared {
        cfg,
        upload: ReceiverAccounting::shared(),
        download: Mutex::new(Vec::new()),
        download_window: Mutex::new(None),
        uploads_seen: AtomicU32::new(0),
        downloads_seen: AtomicU32::new(0),
        echoes_seen: AtomicU32::new(0),
        echoed: AtomicU64::new(0),
        errors: Mutex::new(Vec::new()),
    });
    let conns: Arc<Mutex<Vec<JoinHandle<()>>>> = Arc::new(Mutex::new(Vec::new()));
    let accept = tokio::spawn(sink_accept(
        r.clone(),
        listener,
        shared.clone(),
        conns.clone(),
    ));
    Ok((
        port,
        Box::new(SinkRun {
            r: r.clone(),
            shared,
            accept: Some(accept),
            conns,
            ticker: None,
            teardown: Some(teardown),
        }),
    ))
}

async fn sink_accept(
    r: ResponderCtx,
    listener: RawListener,
    shared: Arc<SinkShared>,
    conns: Arc<Mutex<Vec<JoinHandle<()>>>>,
) {
    let mut acceptor = Tbn1Acceptor::new(&r, listener);
    while let Some((stream, header)) = acceptor.next().await {
        let task = tokio::spawn(sink_conn(r.clone(), stream, header, shared.clone()));
        lock(&conns).push(task);
    }
}

async fn sink_conn(
    r: ResponderCtx,
    mut stream: TcpStream,
    header: StreamHeader,
    sh: Arc<SinkShared>,
) {
    let _ = stream.set_nodelay(sh.cfg.nodelay);
    if !sh.admit_kind(header.kind) {
        r.count_foreign();
        return;
    }
    let idx = header.stream_idx;
    let res: io::Result<()> = match header.kind {
        StreamKind::Upload => receive_stream(&mut stream, idx, &sh.upload)
            .await
            .map(|_| ()),
        StreamKind::Download => {
            let until = sh.download_until();
            let sent = AtomicU64::new(0);
            let res = send_stream(&mut stream, until, &sent).await;
            let info = match &res {
                Ok(s) => SentStreamInfo {
                    stream_idx: idx,
                    sent_bytes: s.sent,
                    acked_bytes: s.acked,
                },
                Err(_) => SentStreamInfo {
                    stream_idx: idx,
                    sent_bytes: sent.load(Ordering::Relaxed),
                    acked_bytes: 0,
                },
            };
            lock(&sh.download).push(info);
            res.map(|_| ())
        }
        StreamKind::Echo => {
            echo_loop(&mut stream, sh.cfg.echo_payload.unwrap_or(1), &sh.echoed).await
        }
        StreamKind::Frames => Ok(()),
    };
    if let Err(e) = res {
        if !r.is_cancelled() {
            lock(&sh.errors).push(format!("stream {idx}: {e}"));
        }
    }
}

/// Echo `payload`-byte messages until EOF.
async fn echo_loop(stream: &mut TcpStream, payload: u32, echoed: &AtomicU64) -> io::Result<()> {
    let mut buf = vec![0u8; payload.max(1) as usize];
    loop {
        match stream.read_exact(&mut buf).await {
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(e) => return Err(e),
        }
        stream.write_all(&buf).await?;
        echoed.fetch_add(1, Ordering::Relaxed);
    }
}

struct SinkRun {
    r: ResponderCtx,
    shared: Arc<SinkShared>,
    accept: Option<JoinHandle<()>>,
    conns: Arc<Mutex<Vec<JoinHandle<()>>>>,
    ticker: Option<JoinHandle<()>>,
    teardown: Option<Teardown>,
}

impl SinkRun {
    fn abort_background(&mut self) {
        if let Some(t) = self.ticker.take() {
            t.abort();
        }
        if let Some(a) = self.accept.take() {
            a.abort();
        }
    }

    fn abort_conns(&self) {
        for h in lock(&self.conns).drain(..) {
            h.abort();
        }
    }
}

/// Wait (bounded) for every connection task; abort and count leftovers.
async fn settle_conns(conns: Arc<Mutex<Vec<JoinHandle<()>>>>) -> usize {
    let handles: Vec<JoinHandle<()>> = std::mem::take(&mut *lock(&conns));
    let until = Instant::now() + SINK_FINISH_WAIT;
    let mut left = 0;
    for mut h in handles {
        if tokio::time::timeout_at(until, &mut h).await.is_err() {
            h.abort();
            left += 1;
        }
    }
    left
}

impl ResponderRun for SinkRun {
    fn start(&mut self) -> BoxFuture<'_, Result<(), AppError>> {
        Box::pin(async move {
            // Once only (the hub also drops duplicate starts).
            if self.ticker.is_some() {
                return Ok(());
            }
            if let Some(key) = self.shared.cfg.tick_key() {
                self.ticker = Some(spawn_tick_goodput(
                    self.r.clone(),
                    self.shared.upload.clone(),
                    key,
                ));
            }
            Ok(())
        })
    }

    fn finish(mut self: Box<Self>, info: Value) -> BoxFuture<'static, Result<Value, AppError>> {
        Box::pin(async move {
            if let Some(a) = self.accept.take() {
                a.abort();
            }
            let left = settle_conns(self.conns.clone()).await;
            self.abort_background();
            let sh = &self.shared;
            let mut errors = std::mem::take(&mut *lock(&sh.errors));
            if left > 0 {
                errors.push(format!("{left} stream(s) were still open at finish"));
            }
            let upload = sh.cfg.accepts_upload().then(|| lock(&sh.upload).stats());
            // Cross-check the sender's counts (§8.1 "Sender checks").
            if let (Some(up), Ok(info)) = (&upload, serde_json::from_value::<Info>(info)) {
                for s in &info.streams {
                    let got = up.stream_bytes(s.stream_idx);
                    if got > s.sent_bytes {
                        errors.push(format!(
                            "stream {}: received {got} B but the sender wrote {} B",
                            s.stream_idx, s.sent_bytes
                        ));
                    }
                }
            }
            let mut download = std::mem::take(&mut *lock(&sh.download));
            download.sort_by_key(|s| s.stream_idx);
            // The resource window matches the goodput window where this side
            // knows it.
            let warmup_ns = u64::from(sh.cfg.warmup_secs) * 1_000_000_000;
            match (&upload, *lock(&sh.download_window)) {
                (Some(up), _) if sh.cfg.direction == Direction::Upload => {
                    if let Ok(g) = up.goodput(sh.cfg.warmup_secs) {
                        self.r
                            .set_resource_window(g.start_ns, g.end_ns, Some(g.bytes));
                    }
                }
                (Some(up), _) if up.bytes_total > 0 => {
                    self.r
                        .set_resource_window(up.epoch_ns + warmup_ns, up.last_ns, None);
                }
                (None, Some((_, start_ns))) => {
                    let data_ns =
                        u64::from(sh.cfg.warmup_secs + sh.cfg.duration_secs) * 1_000_000_000;
                    let end = (start_ns + data_ns).min(now_ns());
                    if end > start_ns + warmup_ns {
                        self.r.set_resource_window(start_ns + warmup_ns, end, None);
                    }
                }
                _ => {}
            }
            let stats = Stats {
                upload,
                download,
                echoed: sh.echoed.load(Ordering::Relaxed),
                foreign_dropped: self.r.foreign_dropped(),
                errors,
            };
            if let Some(td) = self.teardown.take() {
                td.run().await;
            }
            serde_json::to_value(stats)
                .map_err(|e| AppError::internal(format!("serialise the tcp stats: {e}")))
        })
    }

    fn cancel(mut self: Box<Self>) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            self.abort_background();
            self.abort_conns();
            if let Some(td) = self.teardown.take() {
                td.run().await;
            }
        })
    }
}

// ---------------------------------------------------------------------------
// The throughput initiator (shared with egress)
// ---------------------------------------------------------------------------

/// What a throughput run does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ThroughputPlan {
    /// Upload, download or both.
    pub direction: Direction,
    /// Streams per direction.
    pub streams: u32,
    /// Warmup seconds.
    pub warmup_secs: u32,
    /// Measured seconds.
    pub duration_secs: u32,
    /// `TCP_NODELAY` on this side's sockets.
    pub nodelay: bool,
}

/// What a throughput run measured.
#[derive(Debug, Clone)]
pub(crate) struct Measured {
    /// The `results` block.
    pub results: ThroughputResults,
    /// Headline goodput (the sum when bidirectional), Mb/s.
    pub goodput_mbps: f64,
    /// Bidirectional: upload over the overlap window.
    pub upload_mbps: Option<f64>,
    /// Bidirectional: download over the overlap window.
    pub download_mbps: Option<f64>,
    /// Median connect time over streams, ms.
    pub connect_ms: Option<f64>,
    /// Goodput series (initiator timebase).
    pub series: Vec<Series>,
    /// Measurement window, local `now_ns()`.
    pub window_ns: (u64, u64),
    /// Payload bytes in that window.
    pub goodput_bytes: u64,
}

/// One opened stream before its data phase.
struct Opened {
    idx: u16,
    kind: StreamKind,
    stream: TcpStream,
    connect_ms: f64,
}

/// The stream plan: `(stream_idx, kind)` for every stream. Upload streams
/// take 0..n, download streams follow them when bidirectional.
fn stream_plan(direction: Direction, streams: u32) -> Vec<(u16, StreamKind)> {
    let n = streams.min(u32::from(u16::MAX / 2)) as u16;
    let mut plan = Vec::new();
    if direction != Direction::Download {
        plan.extend((0..n).map(|i| (i, StreamKind::Upload)));
    }
    if direction != Direction::Upload {
        let base = if direction == Direction::Bidirectional {
            n
        } else {
            0
        };
        plan.extend((0..n).map(|i| (base + i, StreamKind::Download)));
    }
    plan
}

/// Prepare must have run. Takes the clock before (when the responder
/// receives), sends `start`, opens every stream, runs the data phase,
/// sends `finish` and computes goodput per §8.1.
pub(crate) async fn run_throughput(
    ctx: &mut RunCtx<'_>,
    plan: &ThroughputPlan,
    dial: &Dial,
) -> Result<Measured, AppError> {
    let run_tag = ctx.run_tag()?;
    let bidi = plan.direction == Direction::Bidirectional;
    // Responder-produced numbers (the upload receiver) are mapped into this
    // side's timebase, so estimate the clock before and after.
    let needs_clock = plan.direction != Direction::Download;
    if needs_clock {
        ctx.clock().await?;
    }
    ctx.start().await?;

    // Open every stream first, timing each `open_tcp`.
    let node = ctx.node.clone();
    let opened =
        futures::future::join_all(stream_plan(plan.direction, plan.streams).into_iter().map(
            |(idx, kind)| {
                let node = node.clone();
                async move {
                    let t = Instant::now();
                    let stream = dial
                        .open(&node)
                        .await
                        .map_err(|e| e.context(format!("stream {idx}")))?;
                    Ok::<_, AppError>(Opened {
                        idx,
                        kind,
                        stream,
                        connect_ms: dur_ms(t.elapsed()),
                    })
                }
            },
        ))
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    let connects: Vec<(u16, StreamKind, f64)> = opened
        .iter()
        .map(|o| (o.idx, o.kind, o.connect_ms))
        .collect();

    // The data phase: every stream starts together.
    let data = secs(plan.warmup_secs + plan.duration_secs);
    let data_start = Instant::now();
    let until = data_start + data;
    let sent_ctr = Arc::new(AtomicU64::new(0));
    let acc_down = ReceiverAccounting::shared();
    let mut uploads: Tasks<(u16, io::Result<crate::workloads::common::SentStream>)> = Tasks::new();
    let mut downloads: Tasks<(u16, io::Result<u64>)> = Tasks::new();
    for o in opened {
        let Opened {
            idx,
            kind,
            mut stream,
            ..
        } = o;
        let _ = stream.set_nodelay(plan.nodelay);
        let header = StreamHeader {
            run_tag,
            stream_idx: idx,
            kind,
        };
        if kind == StreamKind::Upload {
            let ctr = sent_ctr.clone();
            uploads.push(tokio::spawn(async move {
                let res = async {
                    write_header(&mut stream, &header).await?;
                    send_stream(&mut stream, until, &ctr).await
                }
                .await;
                (idx, res)
            }));
        } else {
            let acc = acc_down.clone();
            downloads.push(tokio::spawn(async move {
                let res = async {
                    write_header(&mut stream, &header).await?;
                    receive_stream(&mut stream, idx, &acc).await
                }
                .await;
                (idx, res)
            }));
        }
    }
    let mut live_tasks: Tasks<()> = Tasks::new();
    if plan.direction != Direction::Download {
        live_tasks.push(spawn_live_rate(ctx.live(), sent_ctr.clone(), "sent_mbps"));
    }
    if plan.direction == Direction::Download {
        live_tasks.push(spawn_live_goodput(
            ctx.live(),
            acc_down.clone(),
            "goodput_mbps",
        ));
    }
    ctx.set_partial(partial_output(plan, &connects, None));
    let warm_end = data_start + secs(plan.warmup_secs);
    let mut in_warmup = plan.warmup_secs > 0;
    ctx.phase(if in_warmup {
        Phase::Warmup
    } else {
        Phase::Running
    });
    let hard_end = until + STREAM_GRACE;
    let mut tick = tokio::time::interval(Duration::from_millis(250));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut last_down = (Instant::now(), 0u64);
    let mut up_live: Option<f64> = None;
    let (ups, downs) = {
        let wait = async { (uploads.join().await, downloads.join().await) };
        tokio::pin!(wait);
        loop {
            tokio::select! {
                r = &mut wait => break r,
                _ = tokio::time::sleep_until(warm_end), if in_warmup => {
                    in_warmup = false;
                    ctx.phase(Phase::Running);
                }
                _ = tokio::time::sleep_until(hard_end) => {
                    return Err(AppError::timeout(format!(
                        "the streams did not finish within {} s of the end of the data period",
                        STREAM_GRACE.as_secs()
                    )));
                }
                _ = tick.tick(), if bidi => {
                    while let Some(t) = ctx.try_tick() {
                        if let Some(v) = t.values.get("upload_mbps") {
                            up_live = Some(*v);
                        }
                    }
                    let now = (Instant::now(), lock(&acc_down).bytes_total());
                    let dt = now.0.saturating_duration_since(last_down.0).as_secs_f64();
                    if dt > 0.0 && now.1 > 0 {
                        let down = (now.1 - last_down.1) as f64 * 8.0 / dt / 1e6;
                        ctx.sample("download_mbps", down);
                        ctx.sample("goodput_mbps", down + up_live.unwrap_or(0.0));
                    }
                    last_down = now;
                }
            }
        }
    };
    drop(live_tasks);

    let mut errors = Vec::new();
    let mut sent_up: Vec<SentStreamInfo> = Vec::new();
    for r in ups {
        match r {
            Ok((idx, Ok(s))) => sent_up.push(SentStreamInfo {
                stream_idx: idx,
                sent_bytes: s.sent,
                acked_bytes: s.acked,
            }),
            Ok((idx, Err(e))) => errors.push(format!("upload stream {idx}: {e}")),
            Err(e) => errors.push(format!("an upload stream task failed: {e}")),
        }
    }
    let mut recv_down: Vec<(u16, u64)> = Vec::new();
    for r in downs {
        match r {
            Ok((idx, Ok(n))) => recv_down.push((idx, n)),
            Ok((idx, Err(e))) => errors.push(format!("download stream {idx}: {e}")),
            Err(e) => errors.push(format!("a download stream task failed: {e}")),
        }
    }
    let down_local = (plan.direction != Direction::Upload).then(|| lock(&acc_down).stats());
    let origin = ctx.origin_ns();
    let local_series = down_local
        .as_ref()
        .map(|d| d.series(origin, plan.warmup_secs, "goodput_mbps", "Goodput"));
    ctx.set_partial(partial_output(plan, &connects, local_series));
    if !errors.is_empty() {
        return Err(AppError::new(ErrorCode::Io, errors.join("; ")));
    }

    ctx.phase(Phase::Finishing);
    let info = Info {
        streams: sent_up.clone(),
    };
    let stats: Stats = ctx.finish(&info, FINISH_DRAIN).await?;
    if needs_clock {
        ctx.clock().await?;
    }
    let map = ctx.clock_map();
    let to_local = |t: u64| map.map_or(t, |m| m.to_local(t));
    if !stats.errors.is_empty() {
        return Err(AppError::new(
            ErrorCode::Io,
            format!("the responder's sink: {}", stats.errors.join("; ")),
        ));
    }

    // Per-stream checks: received ≤ sent, ack == received, Σ buckets == total.
    let up_stats = if plan.direction == Direction::Download {
        None
    } else {
        Some(stats.upload.clone().ok_or_else(|| {
            AppError::new(
                ErrorCode::ProtocolMismatch,
                "finish: the responder returned no upload accounting",
            )
        })?)
    };
    let mut streams = Vec::new();
    let mut checks = Vec::new();
    let connect_of = |idx: u16| connects.iter().find(|c| c.0 == idx).map_or(0.0, |c| c.2);
    if let Some(up) = &up_stats {
        if let Err(e) = up.check_invariants() {
            checks.push(format!("upload: {e}"));
        }
        let max_span = max_data_span_ns(plan.warmup_secs, plan.duration_secs, STREAM_GRACE);
        up.check_span(max_span).map_err(|e| {
            AppError::new(
                ErrorCode::ProtocolMismatch,
                format!("finish: the responder's upload accounting: {e}"),
            )
        })?;
        for s in &sent_up {
            let got = up.stream_bytes(s.stream_idx);
            check_stream(&mut checks, "upload", s, got);
            streams.push(StreamResult {
                stream_idx: s.stream_idx,
                direction: OneWayDirection::Upload,
                connect_ms: connect_of(s.stream_idx),
                sent_bytes: s.sent_bytes,
                received_bytes: got,
                acked_bytes: s.acked_bytes,
            });
        }
    }
    if let Some(down) = &down_local {
        if let Err(e) = down.check_invariants() {
            checks.push(format!("download: {e}"));
        }
        for (idx, n) in &recv_down {
            let sender = stats
                .download
                .iter()
                .find(|s| s.stream_idx == *idx)
                .copied()
                .unwrap_or(SentStreamInfo {
                    stream_idx: *idx,
                    ..Default::default()
                });
            let got = down.stream_bytes(*idx);
            if got != *n {
                checks.push(format!(
                    "download stream {idx}: counted {got} B but read {n} B"
                ));
            }
            check_stream(&mut checks, "download", &sender, got);
            streams.push(StreamResult {
                stream_idx: *idx,
                direction: OneWayDirection::Download,
                connect_ms: connect_of(*idx),
                sent_bytes: sender.sent_bytes,
                received_bytes: got,
                acked_bytes: sender.acked_bytes,
            });
        }
    }
    streams.sort_by_key(|s| s.stream_idx);
    if !checks.is_empty() {
        return Err(AppError::internal(format!(
            "accounting check failed: {}",
            checks.join("; ")
        )));
    }

    // Goodput (§8.1).
    let w = plan.warmup_secs;
    let warmup_ns = u64::from(w) * 1_000_000_000;
    let mut series = Vec::new();
    let (mut upload, mut download, mut receiver) = (None, None, None);
    // (headline, upload, download, window in local time, window bytes)
    let (goodput_mbps, upload_mbps, download_mbps, window_ns, goodput_bytes) =
        match (&up_stats, &down_local) {
            (Some(one), None) | (None, Some(one)) => {
                let g = one.goodput(w)?;
                let local = up_stats.is_none();
                let mapped = if local {
                    one.clone()
                } else {
                    one.map_time(to_local)
                };
                series.push(mapped.series(origin, w, "goodput_mbps", "Goodput"));
                receiver = Some(one.clone());
                let window = if local {
                    (g.start_ns, g.end_ns)
                } else {
                    (to_local(g.start_ns), to_local(g.end_ns))
                };
                (g.mbps, None, None, window, g.bytes)
            }
            (Some(up), Some(down)) => {
                let up_l = up.map_time(to_local);
                let (start, end) = overlap_window(&up_l, down, warmup_ns)?;
                let up_bytes = up_l.bytes_between(start, end);
                let down_bytes = down.bytes_between(start, end);
                let u = stats::mbps(up_bytes, end - start).unwrap_or(0.0);
                let d = stats::mbps(down_bytes, end - start).unwrap_or(0.0);
                series.push(up_l.series(origin, w, "upload_mbps", "Upload"));
                series.push(down.series(origin, w, "download_mbps", "Download"));
                series.push(sum_series(
                    &[&up_l, down],
                    origin,
                    warmup_ns,
                    "goodput_mbps",
                    "Goodput, sum of both directions",
                ));
                upload = Some(DirectionResult {
                    goodput_mbps: u,
                    window_bytes: up_bytes,
                    receiver: up.clone(),
                });
                download = Some(DirectionResult {
                    goodput_mbps: d,
                    window_bytes: down_bytes,
                    receiver: down.clone(),
                });
                (
                    u + d,
                    Some(u),
                    Some(d),
                    (start, end),
                    (up_bytes + down_bytes).round() as u64,
                )
            }
            (None, None) => return Err(AppError::internal("a throughput run with no direction")),
        };
    let window_len_ns = window_ns.1.saturating_sub(window_ns.0);
    let received: u64 = streams.iter().map(|s| s.received_bytes).sum();
    let sent: u64 = streams.iter().map(|s| s.sent_bytes).sum();
    let connect_values: Vec<f64> = streams.iter().map(|s| s.connect_ms).collect();
    Ok(Measured {
        results: ThroughputResults {
            direction: plan.direction,
            streams,
            bytes_total: received,
            received_bytes: received,
            sent_bytes: sent,
            goodput_bytes,
            window_ms: window_len_ns / 1_000_000,
            connect_ms: Distribution::from_values(&connect_values),
            receiver,
            upload,
            download,
            foreign_dropped: stats.foreign_dropped,
            nodelay: plan.nodelay,
        },
        goodput_mbps,
        upload_mbps,
        download_mbps,
        connect_ms: stats::median(&connect_values),
        series,
        window_ns,
        goodput_bytes,
    })
}

fn check_stream(checks: &mut Vec<String>, dir: &str, sender: &SentStreamInfo, received: u64) {
    if received > sender.sent_bytes {
        checks.push(format!(
            "{dir} stream {}: received {received} B but the sender wrote {} B",
            sender.stream_idx, sender.sent_bytes
        ));
    }
    if sender.acked_bytes != received {
        checks.push(format!(
            "{dir} stream {}: the ack says {} B but the receiver counted {received} B",
            sender.stream_idx, sender.acked_bytes
        ));
    }
}

/// The bidirectional overlap window `[max(epochs) + warmup, min(lasts)]`
/// (both stats in one timebase); fails with "window too short" under 1 s.
fn overlap_window(
    a: &ReceiverStats,
    b: &ReceiverStats,
    warmup_ns: u64,
) -> Result<(u64, u64), AppError> {
    if a.bytes_total == 0 || b.bytes_total == 0 {
        return Err(AppError::internal(
            "no payload was received in one of the directions",
        ));
    }
    let start = a.epoch_ns.max(b.epoch_ns) + warmup_ns;
    let end = a.last_ns.min(b.last_ns);
    if end < start + 1_000_000_000 {
        return Err(AppError::internal(format!(
            "window too short: the two directions overlapped for {:.3} s after the warmup, under 1 s",
            end.saturating_sub(start) as f64 / 1e9
        )));
    }
    Ok((start, end))
}

/// Σ of several receivers' goodput on a common 250 ms grid from their
/// earliest epoch, plotted at each grid bucket's end.
fn sum_series(
    parts: &[&ReceiverStats],
    origin_ns: u64,
    warmup_ns: u64,
    key: &str,
    label: &str,
) -> Series {
    let live: Vec<&&ReceiverStats> = parts.iter().filter(|s| s.bytes_total > 0).collect();
    let rel_ms = |t: u64| (t as f64 - origin_ns as f64) / 1e6;
    let (Some(start), Some(end)) = (
        live.iter().map(|s| s.epoch_ns).min(),
        live.iter().map(|s| s.last_ns).max(),
    ) else {
        return Series {
            key: key.into(),
            label: label.into(),
            unit: "Mb/s".into(),
            points: Vec::new(),
            warmup_ms: None,
        };
    };
    // Backstop: never walk more than an hour of grid, whatever the inputs say.
    let end = end.min(start.saturating_add(MAX_SERIES_SPAN_NS));
    let mut points = Vec::new();
    let mut t = start;
    while t < end {
        let next = t.saturating_add(BUCKET_NS).min(end);
        if next - t >= MIN_FINAL_BUCKET_NS || next < end {
            let bytes: f64 = live.iter().map(|s| s.bytes_between(t, next)).sum();
            if let Some(v) = stats::mbps(bytes, next - t) {
                points.push((rel_ms(next), v));
            }
        }
        t = next;
    }
    let warm_start = live.iter().map(|s| s.epoch_ns).max().unwrap_or(start);
    Series {
        key: key.into(),
        label: label.into(),
        unit: "Mb/s".into(),
        points,
        warmup_ms: Some(rel_ms(warm_start + warmup_ns)),
    }
}

/// What a failed throughput run keeps: connect times and any local series.
fn partial_output(
    plan: &ThroughputPlan,
    connects: &[(u16, StreamKind, f64)],
    series: Option<Series>,
) -> WorkloadOutput {
    let connect: Vec<f64> = connects.iter().map(|c| c.2).collect();
    WorkloadOutput {
        summary: vec![item(
            "connect_ms",
            "Connect",
            stats::median(&connect),
            "ms",
            Better::Lower,
        )],
        results: serde_json::json!({
            "mode": "throughput",
            "direction": plan.direction,
            "connect_ms": Distribution::from_values(&connect),
            "nodelay": plan.nodelay,
        }),
        series: series.into_iter().collect(),
        notes: Vec::new(),
        window_ns: None,
        goodput_bytes: None,
    }
}

// ---------------------------------------------------------------------------
// Latency (initiator)
// ---------------------------------------------------------------------------

async fn run_latency(ctx: &mut RunCtx<'_>, p: &TcpParams) -> Result<WorkloadOutput, AppError> {
    let setup: Setup = ctx.prepare(&Client {}).await?;
    let p = echoed_params(ctx, p);
    let run_tag = ctx.run_tag()?;
    ctx.start().await?;
    let dial = Dial::Tailnet {
        peer_id: ctx.peer_id().to_string(),
        port: setup.port,
    };
    let t = Instant::now();
    let mut stream = dial.open(&ctx.node).await?;
    let connect_ms = dur_ms(t.elapsed());
    let _ = stream.set_nodelay(p.nodelay);
    write_header(
        &mut stream,
        &StreamHeader {
            run_tag,
            stream_idx: 0,
            kind: StreamKind::Echo,
        },
    )
    .await
    .map_err(|e| AppError::io("write the stream header", &e))?;

    let size = p.payload_bytes.max(1) as usize;
    let chunk = payload_chunk();
    let msg: Vec<u8> = chunk.iter().copied().cycle().take(size).collect();
    let mut buf = vec![0u8; size];
    let total = p.warmup_count + p.count;
    let mut points: Vec<(f64, f64)> = Vec::with_capacity(total as usize);
    let mut measured: Vec<f64> = Vec::with_capacity(p.count as usize);
    let mut warmup_ms = None;
    let mut live = LiveRtt::default();
    let mut last_emit = Instant::now();
    let (mut sent, mut received) = (0u64, 0u64);
    ctx.phase(if p.warmup_count > 0 {
        Phase::Warmup
    } else {
        Phase::Running
    });
    for i in 0..total {
        ctx.check_cancelled()?;
        let sent_at = now_ns();
        let t0 = Instant::now();
        let round = async {
            stream.write_all(&msg).await?;
            stream.read_exact(&mut buf).await?;
            Ok::<_, io::Error>(())
        };
        match tokio::time::timeout(ECHO_TIMEOUT, round).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                ctx.set_partial(latency_partial(&points, warmup_ms, &measured));
                return Err(AppError::io(format!("round trip {i}"), &e));
            }
            Err(_) => {
                ctx.set_partial(latency_partial(&points, warmup_ms, &measured));
                return Err(AppError::timeout(format!(
                    "round trip {i} took longer than {} s",
                    ECHO_TIMEOUT.as_secs()
                )));
            }
        }
        let rtt = dur_ms(t0.elapsed());
        sent += size as u64;
        received += size as u64;
        let t_ms = ctx.rel_ms(sent_at);
        points.push((t_ms, rtt));
        if i < p.warmup_count {
            warmup_ms = Some(t_ms);
            if i + 1 == p.warmup_count {
                ctx.phase(Phase::Running);
            }
        } else {
            measured.push(rtt);
            live.push(rtt, false);
        }
        if last_emit.elapsed() >= Duration::from_millis(250) {
            last_emit = Instant::now();
            let (interval, p50) = live.take();
            if let Some(v) = interval {
                ctx.sample("rtt_ms", v);
            }
            if let Some(v) = p50 {
                ctx.sample("rtt_p50_ms", v);
            }
            ctx.fraction((i + 1) as f32 / total as f32);
        }
    }
    let _ = stream.shutdown().await;
    drop(stream);
    ctx.set_partial(latency_partial(&points, warmup_ms, &measured));
    ctx.phase(Phase::Finishing);
    let stats: Stats = ctx
        .finish(
            &Info {
                streams: vec![SentStreamInfo {
                    stream_idx: 0,
                    sent_bytes: sent,
                    acked_bytes: 0,
                }],
            },
            FINISH_DRAIN,
        )
        .await?;
    latency_output(
        &p,
        &LatencyMeasured {
            connect_ms,
            points,
            warmup_ms,
            measured,
            sent,
            received,
        },
        &stats,
    )
}

/// What a latency run measured on the initiator.
struct LatencyMeasured {
    connect_ms: f64,
    /// `(t_ms, rtt_ms)` of every round trip, warmup included.
    points: Vec<(f64, f64)>,
    /// The last warmup round trip's `t_ms`.
    warmup_ms: Option<f64>,
    /// Round trips after the warmup, in order.
    measured: Vec<f64>,
    sent: u64,
    received: u64,
}

fn latency_output(
    p: &TcpParams,
    m: &LatencyMeasured,
    stats: &Stats,
) -> Result<WorkloadOutput, AppError> {
    let dist = Distribution::from_values(&m.measured);
    let results = Results::Latency(Box::new(LatencyResults {
        payload_bytes: p.payload_bytes,
        count: p.count,
        warmup_count: p.warmup_count,
        nodelay: p.nodelay,
        connect_ms: m.connect_ms,
        rtt_ms: dist,
        samples_ms: m.measured.clone(),
        completed: m.measured.len() as u32,
        sent_bytes: m.sent,
        received_bytes: m.received,
        echoed: stats.echoed,
        foreign_dropped: stats.foreign_dropped,
    }));
    Ok(WorkloadOutput {
        summary: latency_summary(&dist),
        results: to_value(&results)?,
        series: vec![rtt_series(m.points.clone(), m.warmup_ms)],
        notes: vec![NAGLE_NOTE.into()],
        window_ns: None,
        goodput_bytes: None,
    })
}

/// The tcp latency note (§8.2).
const NAGLE_NOTE: &str = "Nagle exists only on the two Rust loopback sockets. In full, 32 KiB \
                          payloads cross the macOS lo0 MTU, so the nodelay A/B is visible there.";

fn latency_summary(d: &Distribution) -> Vec<SummaryItem> {
    vec![
        item("rtt_p50_ms", "RTT p50", d.p50, "ms", Better::Lower),
        item("rtt_p99_ms", "RTT p99", d.p99, "ms", Better::Lower),
        item("rtt_max_ms", "RTT max", d.max, "ms", Better::Lower),
    ]
}

fn rtt_series(points: Vec<(f64, f64)>, warmup_ms: Option<f64>) -> Series {
    Series {
        key: "rtt_ms".into(),
        label: "RTT".into(),
        unit: "ms".into(),
        points,
        warmup_ms,
    }
}

fn latency_partial(
    points: &[(f64, f64)],
    warmup_ms: Option<f64>,
    measured: &[f64],
) -> WorkloadOutput {
    let d = Distribution::from_values(measured);
    WorkloadOutput {
        summary: latency_summary(&d),
        results: serde_json::json!({
            "mode": "latency",
            "rtt_ms": d,
            "samples_ms": measured,
            "completed": measured.len(),
        }),
        series: vec![rtt_series(points.to_vec(), warmup_ms)],
        notes: Vec::new(),
        window_ns: None,
        goodput_bytes: None,
    }
}

fn to_value<T: Serialize>(v: &T) -> Result<Value, AppError> {
    serde_json::to_value(v).map_err(|e| AppError::internal(format!("serialise the results: {e}")))
}

// ---------------------------------------------------------------------------
// Entry points
// ---------------------------------------------------------------------------

/// Initiator side (§6 "Initiator flow" step 7).
pub async fn run(ctx: &mut RunCtx<'_>, p: &TcpParams) -> Result<WorkloadOutput, AppError> {
    if p.mode == StreamMode::Latency {
        return run_latency(ctx, p).await;
    }
    let setup: Setup = ctx.prepare(&Client {}).await?;
    let p = echoed_params(ctx, p);
    let plan = ThroughputPlan {
        direction: p.direction,
        streams: p.streams,
        warmup_secs: p.warmup_secs,
        duration_secs: p.duration_secs,
        nodelay: p.nodelay,
    };
    let dial = Dial::Tailnet {
        peer_id: ctx.peer_id().to_string(),
        port: setup.port,
    };
    let m = run_throughput(ctx, &plan, &dial).await?;
    throughput_output(&plan, m)
}

/// The tcp throughput output from what [`run_throughput`] measured.
fn throughput_output(plan: &ThroughputPlan, m: Measured) -> Result<WorkloadOutput, AppError> {
    let bidi = plan.direction == Direction::Bidirectional;
    let mut summary = vec![item(
        "goodput_mbps",
        if bidi {
            "Goodput, sum of both directions"
        } else {
            "Goodput"
        },
        Some(m.goodput_mbps),
        "Mb/s",
        Better::Higher,
    )];
    if bidi {
        summary.push(item(
            "upload_mbps",
            "Upload",
            m.upload_mbps,
            "Mb/s",
            Better::Higher,
        ));
        summary.push(item(
            "download_mbps",
            "Download",
            m.download_mbps,
            "Mb/s",
            Better::Higher,
        ));
    }
    summary.push(item(
        "connect_ms",
        "Connect",
        m.connect_ms,
        "ms",
        Better::Lower,
    ));
    summary.push(cpu_per_mb_item());
    summary.push(loaded_rtt_item());
    let mut notes = Vec::new();
    if bidi {
        notes.push("goodput is the sum of both directions".to_string());
    }
    Ok(WorkloadOutput {
        summary,
        results: to_value(&Results::Throughput(Box::new(m.results)))?,
        series: m.series,
        notes,
        window_ns: Some(m.window_ns),
        goodput_bytes: Some(m.goodput_bytes),
    })
}

/// Responder side: bind the sink and return its port.
pub async fn prepare(
    r: &ResponderCtx,
    p: &TcpParams,
    _client: Client,
) -> Result<(Setup, Box<dyn ResponderRun>), AppError> {
    let cfg = SinkConfig {
        direction: p.direction,
        streams: p.streams,
        warmup_secs: p.warmup_secs,
        duration_secs: p.duration_secs,
        nodelay: p.nodelay,
        echo_payload: (p.mode == StreamMode::Latency).then_some(p.payload_bytes),
    };
    let (port, run) = prepare_sink(r, cfg).await?;
    Ok((Setup { port }, run))
}

/// A canonical example output for the schema fixtures (upload, 1 stream, 10 s).
pub fn example_output() -> WorkloadOutput {
    let mut cpu = cpu_per_mb_item();
    cpu.value = Some(4.1);
    let mut rtt = loaded_rtt_item();
    rtt.value = Some(6.8);
    let epoch = 1_760_601_600_900_000_000u64;
    let buckets: Vec<u64> = [
        19_062_500u64,
        27_500_000,
        28_281_250,
        28_781_250,
        28_093_750,
        28_593_750,
        29_062_500,
        28_375_000,
    ]
    .iter()
    .copied()
    .cycle()
    .take(44)
    .collect();
    let total: u64 = buckets.iter().sum();
    let receiver = ReceiverStats {
        epoch_ns: epoch,
        bucket_ms: 250,
        buckets,
        last_ns: epoch + 10_998_000_000,
        bytes_total: total,
        streams: vec![crate::dataplane::StreamStats {
            stream_idx: 0,
            bytes: total,
            first_ns: epoch,
            last_ns: epoch + 10_998_000_000,
        }],
    };
    let goodput = receiver
        .goodput(1)
        .map(|g| (g.mbps, g.bytes))
        .unwrap_or((0.0, 0));
    let results = Results::Throughput(Box::new(ThroughputResults {
        direction: Direction::Upload,
        streams: vec![StreamResult {
            stream_idx: 0,
            direction: OneWayDirection::Upload,
            connect_ms: 38.2,
            sent_bytes: total + 2_621_440,
            received_bytes: total,
            acked_bytes: total,
        }],
        bytes_total: total,
        received_bytes: total,
        sent_bytes: total + 2_621_440,
        goodput_bytes: goodput.1,
        window_ms: 9_998,
        connect_ms: Distribution::from_values(&[38.2]),
        receiver: Some(receiver.clone()),
        upload: None,
        download: None,
        foreign_dropped: 0,
        nodelay: true,
    }));
    let series = receiver.series(epoch - 400_000_000, 1, "goodput_mbps", "Goodput");
    WorkloadOutput {
        summary: vec![
            item(
                "goodput_mbps",
                "Goodput",
                Some((goodput.0 * 10.0).round() / 10.0),
                "Mb/s",
                Better::Higher,
            ),
            item("connect_ms", "Connect", Some(38.2), "ms", Better::Lower),
            cpu,
            rtt,
        ],
        results: serde_json::to_value(results).unwrap_or_default(),
        series: vec![series],
        notes: vec![],
        window_ns: Some((epoch + 1_000_000_000, epoch + 10_998_000_000)),
        goodput_bytes: Some(goodput.1),
    }
}

/// A receiver's accounting for the examples: `buckets` 250 ms buckets of
/// about `per_bucket` bytes on one stream from `epoch_ns` (the first a
/// little slower, a deterministic wobble after).
fn example_receiver(
    epoch_ns: u64,
    stream_idx: u16,
    per_bucket: u64,
    buckets: u64,
) -> ReceiverStats {
    let mut acc = ReceiverAccounting::new();
    for b in 0..buckets {
        let scale = match b {
            0 => 0.71,
            _ => 1.0 + 0.011 * (((b * 7 + u64::from(stream_idx)) % 5) as f64 - 2.0),
        };
        // Two reads per bucket, the last one 2 ms before the bucket ends.
        let bytes = (per_bucket as f64 * scale) as u64;
        let at = epoch_ns + b * BUCKET_NS;
        acc.record(stream_idx, bytes / 2, at + 1_000_000);
        acc.record(stream_idx, bytes - bytes / 2, at + BUCKET_NS - 2_000_000);
    }
    acc.stats()
}

/// The examples' clock: the responder runs 1.284 ms ahead.
fn example_to_local(t: u64) -> u64 {
    t.saturating_sub(1_284_310)
}

/// Bidirectional, 1 stream each way, 1 + 10 s (sum ≈ 362 Mb/s on one Mac).
fn example_bidirectional() -> WorkloadOutput {
    let p = TcpParams {
        direction: Direction::Bidirectional,
        ..TcpParams::default()
    };
    let plan = ThroughputPlan {
        direction: p.direction,
        streams: p.streams,
        warmup_secs: p.warmup_secs,
        duration_secs: p.duration_secs,
        nodelay: p.nodelay,
    };
    let origin = 1_760_601_600_500_000_000u64;
    let w = p.warmup_secs;
    let warmup_ns = u64::from(w) * 1_000_000_000;
    // Upload: counted by the responder (its clock); download: here.
    let up = example_receiver(origin + 401_284_310, 0, 5_603_125, 44);
    let down = example_receiver(origin + 402_600_000, 1, 5_712_500, 44);
    let up_l = up.map_time(example_to_local);
    let (start, end) = overlap_window(&up_l, &down, warmup_ns).unwrap_or((origin, origin + 1));
    let up_bytes = up_l.bytes_between(start, end);
    let down_bytes = down.bytes_between(start, end);
    let u = stats::mbps(up_bytes, end - start).unwrap_or(0.0);
    let d = stats::mbps(down_bytes, end - start).unwrap_or(0.0);
    let series = vec![
        up_l.series(origin, w, "upload_mbps", "Upload"),
        down.series(origin, w, "download_mbps", "Download"),
        sum_series(
            &[&up_l, &down],
            origin,
            warmup_ns,
            "goodput_mbps",
            "Goodput, sum of both directions",
        ),
    ];
    let streams = vec![
        StreamResult {
            stream_idx: 0,
            direction: OneWayDirection::Upload,
            connect_ms: 0.93,
            sent_bytes: up.bytes_total + 1_441_792,
            received_bytes: up.bytes_total,
            acked_bytes: up.bytes_total,
        },
        StreamResult {
            stream_idx: 1,
            direction: OneWayDirection::Download,
            connect_ms: 1.07,
            sent_bytes: down.bytes_total + 1_179_648,
            received_bytes: down.bytes_total,
            acked_bytes: down.bytes_total,
        },
    ];
    let connects: Vec<f64> = streams.iter().map(|s| s.connect_ms).collect();
    let received = up.bytes_total + down.bytes_total;
    let sent: u64 = streams.iter().map(|s| s.sent_bytes).sum();
    let goodput_bytes = (up_bytes + down_bytes).round() as u64;
    let m = Measured {
        results: ThroughputResults {
            direction: p.direction,
            streams,
            bytes_total: received,
            received_bytes: received,
            sent_bytes: sent,
            goodput_bytes,
            window_ms: (end - start) / 1_000_000,
            connect_ms: Distribution::from_values(&connects),
            receiver: None,
            upload: Some(DirectionResult {
                goodput_mbps: u,
                window_bytes: up_bytes,
                receiver: up,
            }),
            download: Some(DirectionResult {
                goodput_mbps: d,
                window_bytes: down_bytes,
                receiver: down,
            }),
            foreign_dropped: 0,
            nodelay: p.nodelay,
        },
        goodput_mbps: u + d,
        upload_mbps: Some(u),
        download_mbps: Some(d),
        connect_ms: stats::median(&connects),
        series,
        window_ns: (start, end),
        goodput_bytes,
    };
    let mut out = throughput_output(&plan, m).unwrap_or_default();
    for s in &mut out.summary {
        match s.key.as_str() {
            "cpu_ms_per_mb" => s.value = Some(15.6),
            "loaded_rtt_p50_ms" => s.value = Some(0.84),
            _ => {}
        }
    }
    out
}

/// Latency, 64 B × 200 after 10 warmup round trips (p50 ≈ 0.25 ms).
fn example_latency() -> WorkloadOutput {
    let p = TcpParams {
        mode: StreamMode::Latency,
        ..TcpParams::default()
    };
    let total = p.warmup_count + p.count;
    let mut t_ms = 412.0;
    let mut points = Vec::new();
    let mut measured = Vec::new();
    let mut warmup_ms = None;
    for i in 0..total {
        let rtt = 0.231
            + f64::from((i * 37) % 23) * 0.0013
            + if i % 61 == 7 { 0.41 } else { 0.0 }
            + if i < p.warmup_count { 0.35 } else { 0.0 };
        points.push((t_ms, rtt));
        if i < p.warmup_count {
            warmup_ms = Some(t_ms);
        } else {
            measured.push(rtt);
        }
        t_ms += rtt + 0.012;
    }
    let bytes = u64::from(total) * u64::from(p.payload_bytes);
    let stats = Stats {
        echoed: u64::from(total),
        ..Stats::default()
    };
    latency_output(
        &p,
        &LatencyMeasured {
            connect_ms: 0.88,
            points,
            warmup_ms,
            measured,
            sent: bytes,
            received: bytes,
        },
        &stats,
    )
    .unwrap_or_default()
}

/// One example per tcp mode for the schema fixtures.
pub fn examples() -> Vec<super::ModeExample> {
    let latency = TcpParams {
        mode: StreamMode::Latency,
        ..TcpParams::default()
    };
    let bidi = TcpParams {
        direction: Direction::Bidirectional,
        ..TcpParams::default()
    };
    vec![
        super::ModeExample {
            mode: "tcp-throughput-upload",
            params: WorkloadParams::Tcp(TcpParams::default()),
            output: example_output(),
            clock: true,
        },
        super::ModeExample {
            mode: "tcp-throughput-bidirectional",
            params: WorkloadParams::Tcp(bidi),
            output: example_bidirectional(),
            clock: true,
        },
        super::ModeExample {
            mode: "tcp-latency",
            params: WorkloadParams::Tcp(latency),
            output: example_latency(),
            clock: false,
        },
    ]
}

/// The resolved egress/tcp target `ip:port` (brackets for IPv6).
pub(crate) fn socket_addr(ip: std::net::IpAddr, port: u16) -> String {
    SocketAddr::new(ip, port).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    const S: u64 = 1_000_000_000;
    const MS: u64 = 1_000_000;

    fn steady(t0: u64, secs: u64, per_10ms: u64) -> ReceiverStats {
        let mut acc = ReceiverAccounting::new();
        let mut t = t0;
        while t < t0 + secs * S {
            acc.record(0, per_10ms, t);
            t += 10 * MS;
        }
        acc.stats()
    }

    #[test]
    fn stream_plans_number_streams_per_direction() {
        let up = stream_plan(Direction::Upload, 3);
        assert_eq!(
            up,
            vec![
                (0, StreamKind::Upload),
                (1, StreamKind::Upload),
                (2, StreamKind::Upload)
            ]
        );
        let down = stream_plan(Direction::Download, 2);
        assert_eq!(
            down,
            vec![(0, StreamKind::Download), (1, StreamKind::Download)]
        );
        let both = stream_plan(Direction::Bidirectional, 2);
        assert_eq!(
            both,
            vec![
                (0, StreamKind::Upload),
                (1, StreamKind::Upload),
                (2, StreamKind::Download),
                (3, StreamKind::Download)
            ]
        );
    }

    #[test]
    fn sink_admits_only_expected_kinds_and_counts() {
        let mk = |direction, echo| SinkShared {
            cfg: SinkConfig {
                direction,
                streams: 2,
                warmup_secs: 1,
                duration_secs: 2,
                nodelay: true,
                echo_payload: echo,
            },
            upload: ReceiverAccounting::shared(),
            download: Mutex::new(Vec::new()),
            download_window: Mutex::new(None),
            uploads_seen: AtomicU32::new(0),
            downloads_seen: AtomicU32::new(0),
            echoes_seen: AtomicU32::new(0),
            echoed: AtomicU64::new(0),
            errors: Mutex::new(Vec::new()),
        };
        let up = mk(Direction::Upload, None);
        assert!(up.admit_kind(StreamKind::Upload));
        assert!(up.admit_kind(StreamKind::Upload));
        assert!(!up.admit_kind(StreamKind::Upload), "only `streams` uploads");
        assert!(!up.admit_kind(StreamKind::Download));
        assert!(!up.admit_kind(StreamKind::Echo));
        assert!(!up.admit_kind(StreamKind::Frames));
        assert_eq!(up.cfg.tick_key(), Some("goodput_mbps"));
        let both = mk(Direction::Bidirectional, None);
        assert!(both.admit_kind(StreamKind::Upload));
        assert!(both.admit_kind(StreamKind::Download));
        assert_eq!(both.cfg.tick_key(), Some("upload_mbps"));
        let down = mk(Direction::Download, None);
        assert!(!down.admit_kind(StreamKind::Upload));
        assert_eq!(down.cfg.tick_key(), None);
        let echo = mk(Direction::Upload, Some(64));
        assert!(echo.admit_kind(StreamKind::Echo));
        assert!(!echo.admit_kind(StreamKind::Echo), "one echo stream");
        assert!(!echo.admit_kind(StreamKind::Upload));
        assert_eq!(echo.cfg.tick_key(), None);
    }

    #[test]
    fn overlap_window_and_sum_series() {
        // Up from 0 s to 4 s at 1 MB/s; down from 0.5 s to 4.5 s at 2 MB/s.
        let up = steady(0, 4, 10_000);
        let down = steady(500 * MS, 4, 20_000);
        let (start, end) = overlap_window(&up, &down, S).unwrap();
        assert_eq!(start, 1_500 * MS);
        assert_eq!(end, up.last_ns);
        let u = up.goodput_between(start, end).unwrap();
        let d = down.goodput_between(start, end).unwrap();
        assert!((u - 8.0).abs() < 0.1, "{u}");
        assert!((d - 16.0).abs() < 0.2, "{d}");
        // Too short an overlap fails like any short window.
        let late = steady(3 * S, 2, 10_000);
        let err = overlap_window(&up, &late, S).unwrap_err();
        assert!(err.message.contains("window too short"), "{}", err.message);
        assert!(overlap_window(&up, &ReceiverStats::default(), 0).is_err());
        // The sum series on a common grid is the sum of the two rates.
        let s = sum_series(&[&up, &down], 0, S, "goodput_mbps", "Goodput");
        assert_eq!(s.warmup_ms, Some(1500.0));
        let mid: Vec<f64> = s
            .points
            .iter()
            .filter(|(t, _)| *t > 1000.0 && *t < 3900.0)
            .map(|p| p.1)
            .collect();
        assert!(!mid.is_empty());
        for v in mid {
            assert!((v - 24.0).abs() < 0.5, "{v}");
        }
        let empty = sum_series(&[&ReceiverStats::default()], 0, 0, "k", "l");
        assert!(empty.points.is_empty());
    }

    #[test]
    fn sender_checks_catch_mismatches() {
        let mut checks = Vec::new();
        let ok = SentStreamInfo {
            stream_idx: 0,
            sent_bytes: 100,
            acked_bytes: 100,
        };
        check_stream(&mut checks, "upload", &ok, 100);
        assert!(checks.is_empty());
        check_stream(&mut checks, "upload", &ok, 120);
        assert_eq!(checks.len(), 2, "{checks:?}");
        let mut checks = Vec::new();
        check_stream(
            &mut checks,
            "download",
            &SentStreamInfo {
                acked_bytes: 90,
                ..ok
            },
            100,
        );
        assert_eq!(checks.len(), 1);
    }

    #[test]
    fn results_are_tagged_by_mode_and_round_trip() {
        let out = example_output();
        assert_eq!(out.results["mode"], "throughput");
        let r: Results = serde_json::from_value(out.results.clone()).unwrap();
        let Results::Throughput(t) = r else {
            panic!("not throughput")
        };
        t.receiver.as_ref().unwrap().check_invariants().unwrap();
        assert!(t.received_bytes <= t.sent_bytes);
        assert_eq!(out.summary[0].key, "goodput_mbps");
        assert!(out.summary.iter().all(|s| s.value.is_some()));
        assert!(!out.series.is_empty());
        let lat = Results::Latency(Box::new(LatencyResults {
            payload_bytes: 64,
            count: 2,
            warmup_count: 0,
            nodelay: true,
            connect_ms: 1.0,
            rtt_ms: Distribution::from_values(&[1.0, 2.0]),
            samples_ms: vec![1.0, 2.0],
            completed: 2,
            sent_bytes: 128,
            received_bytes: 128,
            echoed: 2,
            foreign_dropped: 0,
        }));
        let v = serde_json::to_value(&lat).unwrap();
        assert_eq!(v["mode"], "latency");
        assert_eq!(serde_json::from_value::<Results>(v).unwrap(), lat);
        assert!(NAGLE_NOTE.starts_with("Nagle exists only on the two Rust loopback sockets."));
    }

    #[tokio::test]
    async fn echo_loop_echoes_until_eof() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let echoed = Arc::new(AtomicU64::new(0));
        let e2 = echoed.clone();
        let server = tokio::spawn(async move {
            let (mut s, _) = listener.accept().await.unwrap();
            echo_loop(&mut s, 5, &e2).await
        });
        let mut c = TcpStream::connect(addr).await.unwrap();
        let mut buf = [0u8; 5];
        for i in 0..3u8 {
            c.write_all(&[i; 5]).await.unwrap();
            c.read_exact(&mut buf).await.unwrap();
            assert_eq!(buf, [i; 5]);
        }
        c.shutdown().await.unwrap();
        server.await.unwrap().unwrap();
        assert_eq!(echoed.load(Ordering::Relaxed), 3);
        assert_eq!(
            socket_addr("100.64.0.1".parse().unwrap(), 7),
            "100.64.0.1:7"
        );
        assert_eq!(socket_addr("fd7a::1".parse().unwrap(), 7), "[fd7a::1]:7");
    }
}
