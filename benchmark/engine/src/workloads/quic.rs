//! `quic` (RFC 027 §8.1 receiver accounting; §8.2 quic throughput / latency).
//!
//! One `connect_quic` per run. Every stream is `open_stream()` +
//! `into_split()` (the halves are rejoined with `tokio::io::join`, so the
//! shared TBN1 helpers apply) and starts with the 16-byte TBN1 header.
//!
//! **Throughput** opens `streams` streams per direction (bidirectional: both).
//! - upload: the initiator writes 64 KiB chunks for warmup + duration, then
//!   `finish()`es; the responder counts every read into one run-level
//!   [`ReceiverAccounting`] and, once it sees `finish()` (EOF), writes the
//!   per-stream ack `[u64 BE bytes]` and finishes its half.
//! - download: header `kind = 1`; the responder writes for warmup + duration
//!   from the header's arrival, the initiator counts and acks.
//! - goodput, series and the warmup exclusion are §8.1's (250 ms bucket
//!   edges). The upload receiver's times are mapped into the initiator's
//!   timebase with the before/after clock estimates; bidirectional goodput
//!   is computed per direction over the overlap window and summed.
//! - `connect_ms` is `connect_quic()` call → return (local UDP relay setup
//!   plus the QUIC/TLS handshake).
//! - `flow_control_ceiling_mbps = min(streams·1.25 MB, 10 MB)·8 / before.rtt
//!   / 1e6`; at ≥ 80 % of it the run notes "bounded by quinn default
//!   flow-control windows".
//!
//! **Latency** uses one stream (`kind = 2`): `warmup_count + count` messages
//! of `payload_bytes`, each one `write_all`, echoed back; RTT is send call →
//! last echoed byte on one clock, and the first `warmup_count` are discarded.
//!
//! The responder `listen_quic(0)`s in `prepare`, keeps only connections
//! whose `remote_address().ip()` is the initiator's (rule 5; others are
//! closed and counted in `foreign_dropped`), and tears down in rule 7 order:
//! the stream tasks (their acks) first, then `QuicListener::close()`.
//!
//! Summary keys: throughput `goodput_mbps` (+ `upload_mbps`, `download_mbps`
//! when bidirectional), `connect_ms`, `cpu_ms_per_mb`, `loaded_rtt_p50_ms`;
//! latency `rtt_p50_ms`, `rtt_p99_ms`, `rtt_max_ms`, `connect_ms`.
//! Series: `goodput_mbps` (+ `upload_mbps`, `download_mbps`) or `rtt_ms`.
//! Live: `goodput_mbps` (responder ticks for upload; the initiator's own
//! receiver for download; bidirectional `upload_mbps` ticks + local
//! `download_mbps`), `sent_mbps`; latency `rtt_ms`, `rtt_p50_ms`.

use std::io;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use futures::future::BoxFuture;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use truffle_core::transport::quic::{QuicConnection, QuicListener, QuicStream};

use crate::clock::{now_ns, ClockMap};
use crate::dataplane::{
    max_data_span_ns, payload_chunk, read_counting, read_stream_ack, write_header,
    write_stream_ack, ReceiverAccounting, ReceiverStats, SharedAccounting, StreamHeader,
    StreamKind, BUCKET_MS, BUCKET_NS, MAX_SERIES_SPAN_NS, MIN_FINAL_BUCKET_NS,
};
use crate::error::{AppError, ErrorCode};
use crate::params::{Direction, QuicParams, StreamMode, WorkloadParams};
use crate::stats;
use crate::types::{Better, Distribution, Phase, Series, SummaryItem};
use crate::workloads::common::{
    cpu_per_mb_item, dur_ms, loaded_rtt_item, measured, raw_ms, read_tbn1, rtt_series,
    sample_window, spawn_live_rate, spawn_tick_goodput, LiveRtt, RttSample,
};
use crate::workloads::{ResponderCtx, ResponderRun, RunCtx, WorkloadOutput};

/// Whether this build offers `quic` (drives `PeerHello.workloads`).
pub const IMPLEMENTED: bool = true;

/// The Progress sample keys a run with these params can set: the
/// initiator's own and the responder's `tick`s, which the hub merges in.
/// [`live_keys`](super::live_keys) adds the path sampler's `loaded_rtt_ms`
/// for time-based runs; `live-keys.json` in the schema fixtures is built
/// from it.
pub fn live_keys(p: &QuicParams) -> Vec<&'static str> {
    match p.mode {
        StreamMode::Latency => vec!["rtt_ms", "rtt_p50_ms"],
        StreamMode::Throughput => super::tcp::throughput_live_keys(p.direction),
    }
}

/// Note on every quic run (§8.2 quic).
pub const NOTE_PACKETS: &str = "about 1200 B packets, because MTU discovery is off";
/// Note on every quic run (§8.2 quic).
pub const NOTE_NO_GSO: &str = "no GSO";
/// Note on every quic run (§8.2 quic).
pub const NOTE_DOUBLE_ENCRYPTION: &str = "double encryption";
/// Note when goodput reaches 80 % of [`flow_control_ceiling_mbps`].
pub const NOTE_FLOW_CONTROL: &str = "bounded by quinn default flow-control windows";

/// quinn's default per-stream receive window, bytes.
const STREAM_WINDOW_BYTES: f64 = 1.25e6;
/// quinn's default connection receive window, bytes.
const CONNECTION_WINDOW_BYTES: f64 = 10e6;
/// Share of the flow-control ceiling at which the run says it was the limit.
const CEILING_SHARE: f64 = 0.8;
/// How long a sender waits for the receiver's per-stream ack after `finish()`.
const ACK_TIMEOUT: Duration = Duration::from_secs(30);
/// How long one latency round trip may take before the stream counts as broken.
const ECHO_TIMEOUT: Duration = Duration::from_secs(10);
/// Drain allowance added to the `finish` timeout; also how long the
/// responder waits for its stream tasks (acks) before closing.
const DRAIN: Duration = Duration::from_secs(10);
/// How long the initiator waits for the echo stream's EOF after finishing it.
const ECHO_CLOSE_WAIT: Duration = Duration::from_secs(2);
/// A throughput run whose payload stops moving this long before the senders
/// are due to stop fails as stalled (instead of at its deadline).
const STALL: Duration = Duration::from_secs(10);

// ---------------------------------------------------------------------------
// Control blocks and results
// ---------------------------------------------------------------------------

/// `prepare.client`: nothing beyond the params (mode, direction and stream
/// count are in them).
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "QuicClient")]
pub struct Client {}

/// `prepare.ack.setup`: the listener's tailnet port.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "QuicSetup")]
pub struct Setup {
    /// `QuicListener::port()` of the responder's `listen_quic(0)`.
    pub port: u16,
}

/// One sender-side stream: what was written and what the receiver acked.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "QuicStreamSent")]
pub struct StreamSent {
    /// Stream index.
    pub stream_idx: u16,
    /// Payload bytes handed to the stream.
    pub sent_bytes: u64,
    /// The receiver's `[u64 BE stream_bytes]` ack, when it arrived.
    pub acked_bytes: Option<u64>,
    /// Why the stream failed.
    pub error: Option<String>,
}

/// `finish.info`: the initiator's sender-side counts.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "QuicInfo")]
pub struct Info {
    /// Upload streams (the initiator sent them).
    pub upload: Vec<StreamSent>,
}

/// `finish.ack.stats`: the responder's numbers.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "QuicStats")]
pub struct Stats {
    /// Upload receiver accounting (responder clock), when the run uploads.
    pub upload: Option<ReceiverStats>,
    /// Download streams (the responder sent them).
    pub download: Vec<StreamSent>,
    /// Messages echoed (latency).
    pub echoes: u64,
    /// Connections accepted from the initiator.
    pub connections: u32,
    /// Streams accepted on them.
    pub streams: u32,
    /// Foreign connections and streams dropped (rule 5, bad headers).
    pub foreign_dropped: u64,
    /// Whether every stream task ended before teardown.
    pub drained: bool,
    /// Stream errors on the responder.
    pub errors: Vec<String>,
}

/// One stream of a throughput run, both ends' counts.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "QuicStreamResult")]
pub struct StreamResult {
    /// `upload` or `download`.
    pub kind: StreamKind,
    /// Stream index.
    pub stream_idx: u16,
    /// Payload bytes the sender handed to the stream.
    pub sent_bytes: u64,
    /// Payload bytes the receiver counted.
    pub received_bytes: u64,
    /// The receiver's per-stream ack.
    pub acked_bytes: Option<u64>,
}

/// `results` of a quic throughput run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "QuicThroughputResults")]
pub struct ThroughputResults {
    /// Upload, download or bidirectional.
    pub direction: Direction,
    /// Streams per direction.
    pub streams: u32,
    /// `connect_quic()` call → return, ms.
    pub connect_ms: f64,
    /// Goodput over the measurement window (bidirectional: the sum), Mb/s.
    pub goodput_mbps: f64,
    /// Upload goodput over the overlap window (bidirectional only).
    pub upload_mbps: Option<f64>,
    /// Download goodput over the overlap window (bidirectional only).
    pub download_mbps: Option<f64>,
    /// Payload bytes inside the measurement window.
    pub window_bytes: u64,
    /// Length of the measurement window, ms.
    pub window_ms: f64,
    /// Every payload byte received, warmup included.
    pub bytes_total: u64,
    /// Same as `bytes_total` (the cross-check against `sent_bytes`).
    pub received_bytes: u64,
    /// Every payload byte the senders handed to their streams.
    pub sent_bytes: u64,
    /// Foreign connections and streams the responder dropped.
    pub foreign_dropped: u64,
    /// What quinn's default windows allow at the settled RTT, Mb/s.
    pub flow_control_ceiling_mbps: Option<f64>,
    /// The receiver of the measured direction (bidirectional: upload), in
    /// the receiver's own clock.
    pub receiver: ReceiverStats,
    /// The download receiver of a bidirectional run (initiator clock).
    pub receiver_download: Option<ReceiverStats>,
    /// Per stream.
    pub stream_results: Vec<StreamResult>,
}

/// `results` of a quic latency run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "QuicLatencyResults")]
pub struct LatencyResults {
    /// `connect_quic()` call → return, ms.
    pub connect_ms: f64,
    /// Message size.
    pub payload_bytes: u32,
    /// Measured messages.
    pub count: u32,
    /// Discarded messages before them.
    pub warmup_count: u32,
    /// Measured messages that came back.
    pub completed: u32,
    /// Round trips of the measured messages.
    pub rtt_ms: Distribution,
    /// Raw measured round trips in send order, ms (evenly thinned to at
    /// most 5000).
    pub samples_ms: Vec<f64>,
    /// Messages the responder echoed (warmup included).
    pub echoes: u64,
    /// Foreign connections and streams the responder dropped.
    pub foreign_dropped: u64,
}

/// `RunReport.results` of a `quic` run, tagged by `mode`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "mode", rename_all = "lowercase")]
#[schemars(rename = "QuicResults")]
pub enum Results {
    /// Throughput mode.
    Throughput(ThroughputResults),
    /// Latency mode.
    Latency(LatencyResults),
}

/// `min(streams·1.25 MB, 10 MB)·8 / rtt / 1e6` (§8.2 quic): the goodput
/// quinn's default receive windows allow per direction at `rtt`. `None`
/// without a positive RTT.
pub fn flow_control_ceiling_mbps(streams: u32, rtt: Option<Duration>) -> Option<f64> {
    let rtt = rtt.filter(|r| !r.is_zero())?.as_secs_f64();
    let window = (f64::from(streams) * STREAM_WINDOW_BYTES).min(CONNECTION_WINDOW_BYTES);
    Some(window * 8.0 / rtt / 1e6)
}

// ---------------------------------------------------------------------------
// Stream helpers
// ---------------------------------------------------------------------------

/// `into_split()` the stream and rejoin the halves as one duplex, so the
/// shared stream helpers (header, counting, acks) apply.
fn split_join(stream: QuicStream) -> impl AsyncRead + AsyncWrite + Unpin + Send {
    let (send, recv) = stream.into_split();
    tokio::io::join(recv, send)
}

/// Open a stream on `conn` and write its TBN1 header.
async fn open_with_header(
    conn: &QuicConnection,
    header: StreamHeader,
) -> Result<impl AsyncRead + AsyncWrite + Unpin + Send, AppError> {
    let stream = conn.open_stream().await.map_err(|e| {
        AppError::new(
            ErrorCode::Io,
            format!("open QUIC stream {}: {e}", header.stream_idx),
        )
    })?;
    let mut io = split_join(stream);
    write_header(&mut io, &header).await.map_err(|e| {
        AppError::io(
            format!("write the header of stream {}", header.stream_idx),
            &e,
        )
    })?;
    Ok(io)
}

/// Write 64 KiB chunks until `until`, counting each chunk into `stream` and
/// `total` once `write_all` accepted it. Does not finish the stream.
async fn pump<W: AsyncWrite + Unpin>(
    w: &mut W,
    until: Instant,
    stream: &AtomicU64,
    total: &AtomicU64,
) -> io::Result<()> {
    let chunk = payload_chunk();
    while Instant::now() < until {
        w.write_all(chunk).await?;
        stream.fetch_add(chunk.len() as u64, Ordering::Relaxed);
        total.fetch_add(chunk.len() as u64, Ordering::Relaxed);
    }
    w.flush().await
}

/// Send one payload stream: pump until `until`, `finish()` (shutdown), then
/// read the receiver's ack. Returns the ack.
async fn send_payload<S: AsyncRead + AsyncWrite + Unpin>(
    io: &mut S,
    until: Instant,
    stream: &AtomicU64,
    total: &AtomicU64,
) -> io::Result<u64> {
    pump(io, until, stream, total).await?;
    io.shutdown().await?;
    tokio::time::timeout(ACK_TIMEOUT, read_stream_ack(io))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "no per-stream ack within 30 s"))?
}

/// Receive one payload stream: count to EOF (the sender's `finish()`), then
/// write the per-stream ack and finish this half. Returns the bytes counted.
async fn receive_payload<S: AsyncRead + AsyncWrite + Unpin>(
    io: &mut S,
    stream_idx: u16,
    acc: &SharedAccounting,
) -> io::Result<u64> {
    let n = read_counting(io, stream_idx, acc).await?;
    write_stream_ack(io, n).await?;
    io.shutdown().await?;
    Ok(n)
}

/// The responder's echo loop: read `len` bytes, write them back, until EOF;
/// then finish. Returns the messages echoed.
async fn echo_loop<S: AsyncRead + AsyncWrite + Unpin>(
    io: &mut S,
    len: usize,
    echoes: &AtomicU64,
) -> io::Result<u64> {
    let mut buf = vec![0u8; len.max(1)];
    let buf = &mut buf[..len];
    let mut n = 0;
    loop {
        match io.read_exact(buf).await {
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e),
        }
        io.write_all(buf).await?;
        n += 1;
        echoes.fetch_add(1, Ordering::Relaxed);
    }
    io.shutdown().await?;
    Ok(n)
}

/// One latency round trip: `write_all` the message, read the echo in full.
async fn round_trip<S: AsyncRead + AsyncWrite + Unpin>(
    io: &mut S,
    msg: &[u8],
    buf: &mut [u8],
) -> io::Result<()> {
    io.write_all(msg).await?;
    io.read_exact(buf).await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Initiator
// ---------------------------------------------------------------------------

/// The params as validated and used by the responder (after `prepare`).
fn used_params(ctx: &RunCtx<'_>, requested: &QuicParams) -> QuicParams {
    match ctx.params() {
        WorkloadParams::Quic(p) => p.clone(),
        _ => requested.clone(),
    }
}

/// Initiator side (§6 "Initiator flow" step 7).
pub async fn run(ctx: &mut RunCtx<'_>, p: &QuicParams) -> Result<WorkloadOutput, AppError> {
    let setup: Setup = ctx.prepare(&Client {}).await?;
    let p = used_params(ctx, p);
    match p.mode {
        StreamMode::Throughput => run_throughput(ctx, &p, &setup).await,
        StreamMode::Latency => run_latency(ctx, &p, &setup).await,
    }
}

/// `connect_quic` to the responder's listener; returns the connection and
/// `connect_ms`.
async fn connect(ctx: &RunCtx<'_>, port: u16) -> Result<(QuicConnection, f64), AppError> {
    let cancel = ctx.cancel_token();
    let t0 = Instant::now();
    let conn = tokio::select! {
        _ = cancel.cancelled() => return Err(AppError::cancelled()),
        c = ctx.node.connect_quic(ctx.peer_id(), port) => {
            c.map_err(|e| AppError::from(e).context("connect_quic"))?
        }
    };
    Ok((conn, dur_ms(t0.elapsed())))
}

/// What one initiator-side stream task did.
#[derive(Debug, Clone, PartialEq)]
struct Outcome {
    kind: StreamKind,
    stream_idx: u16,
    sent: u64,
    received: u64,
    acked: Option<u64>,
    error: Option<String>,
}

impl Outcome {
    fn new(kind: StreamKind, stream_idx: u16) -> Self {
        Self {
            kind,
            stream_idx,
            sent: 0,
            received: 0,
            acked: None,
            error: None,
        }
    }

    fn sent_block(&self) -> StreamSent {
        StreamSent {
            stream_idx: self.stream_idx,
            sent_bytes: self.sent,
            acked_bytes: self.acked,
            error: self.error.clone(),
        }
    }
}

async fn upload_task(
    conn: Arc<QuicConnection>,
    run_tag: u64,
    idx: u16,
    until: Instant,
    total: Arc<AtomicU64>,
) -> Outcome {
    let mut o = Outcome::new(StreamKind::Upload, idx);
    let sent = AtomicU64::new(0);
    let header = StreamHeader {
        run_tag,
        stream_idx: idx,
        kind: StreamKind::Upload,
    };
    let res = async {
        let mut io = open_with_header(&conn, header).await?;
        send_payload(&mut io, until, &sent, &total)
            .await
            .map_err(|e| AppError::io(format!("upload stream {idx}"), &e))
    }
    .await;
    o.sent = sent.load(Ordering::Relaxed);
    match res {
        Ok(acked) => o.acked = Some(acked),
        Err(e) => o.error = Some(e.message),
    }
    o
}

async fn download_task(
    conn: Arc<QuicConnection>,
    run_tag: u64,
    idx: u16,
    acc: SharedAccounting,
    deadline: Instant,
) -> Outcome {
    let mut o = Outcome::new(StreamKind::Download, idx);
    let header = StreamHeader {
        run_tag,
        stream_idx: idx,
        kind: StreamKind::Download,
    };
    let res = async {
        let mut io = open_with_header(&conn, header).await?;
        tokio::time::timeout_at(deadline, receive_payload(&mut io, idx, &acc))
            .await
            .map_err(|_| {
                AppError::timeout(format!(
                    "download stream {idx}: the sender did not finish the stream in time"
                ))
            })?
            .map_err(|e| AppError::io(format!("download stream {idx}"), &e))
    }
    .await;
    match res {
        Ok(n) => o.received = n,
        Err(e) => o.error = Some(e.message),
    }
    o
}

/// `(upload, download)` for a direction.
fn directions(d: Direction) -> (bool, bool) {
    match d {
        Direction::Upload => (true, false),
        Direction::Download => (false, true),
        Direction::Bidirectional => (true, true),
    }
}

fn bytes_of(acc: &SharedAccounting) -> u64 {
    acc.lock()
        .unwrap_or_else(PoisonError::into_inner)
        .bytes_total()
}

fn stats_of(acc: &SharedAccounting) -> ReceiverStats {
    acc.lock().unwrap_or_else(PoisonError::into_inner).stats()
}

async fn run_throughput(
    ctx: &mut RunCtx<'_>,
    p: &QuicParams,
    setup: &Setup,
) -> Result<WorkloadOutput, AppError> {
    let (up, down) = directions(p.direction);
    let bidi = up && down;
    let mut notes = Vec::new();
    // The upload receiver's times are mapped into this timebase.
    if up {
        ctx.clock().await?;
    }
    let (conn, connect_ms) = connect(ctx, setup.port).await?;
    let conn = Arc::new(conn);
    ctx.start().await?;
    let run_tag = ctx.run_tag()?;
    let warmup = Duration::from_secs(u64::from(p.warmup_secs));
    let t0 = Instant::now();
    let until = t0 + warmup + Duration::from_secs(u64::from(p.duration_secs));
    let n = u16::try_from(p.streams).unwrap_or(u16::MAX);
    let total_sent = Arc::new(AtomicU64::new(0));
    let acc_down = ReceiverAccounting::shared();
    let mut set = JoinSet::new();
    if up {
        for idx in 0..n {
            set.spawn(upload_task(
                conn.clone(),
                run_tag,
                idx,
                until,
                total_sent.clone(),
            ));
        }
    }
    if down {
        let base = if up { n } else { 0 };
        for i in 0..n {
            set.spawn(download_task(
                conn.clone(),
                run_tag,
                base + i,
                acc_down.clone(),
                until + ACK_TIMEOUT,
            ));
        }
    }

    // Live values: responder ticks bring `goodput_mbps` (upload) or
    // `upload_mbps` (bidirectional); this side adds what it measures.
    let live = ctx.live();
    let mut bg: Vec<JoinHandle<()>> = Vec::new();
    if up {
        bg.push(spawn_live_rate(
            live.clone(),
            total_sent.clone(),
            "sent_mbps",
        ));
    }
    if down && !bidi {
        bg.push(crate::workloads::common::spawn_live_goodput(
            live.clone(),
            acc_down.clone(),
            "goodput_mbps",
        ));
    }
    ctx.phase(if warmup.is_zero() {
        Phase::Running
    } else {
        Phase::Warmup
    });
    let cancel = ctx.cancel_token();
    let mut warm_done = warmup.is_zero();
    let mut draining = false;
    let mut every = tokio::time::interval(Duration::from_millis(BUCKET_MS));
    every.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut last_up_mbps = 0.0;
    let mut last_down = (Instant::now(), 0u64);
    let mut progress = (Instant::now(), 0u64);
    let mut outcomes = Vec::new();
    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                bg.iter().for_each(JoinHandle::abort);
                return Err(AppError::cancelled());
            }
            _ = tokio::time::sleep_until(t0 + warmup), if !warm_done => {
                warm_done = true;
                ctx.phase(Phase::Running);
            }
            _ = tokio::time::sleep_until(until), if !draining => {
                draining = true;
                ctx.phase(Phase::Draining);
            }
            _ = every.tick() => {
                let now = (Instant::now(), bytes_of(&acc_down));
                if bidi {
                    while let Some(t) = ctx.try_tick() {
                        if let Some(v) = t.values.get("upload_mbps") {
                            last_up_mbps = *v;
                        }
                    }
                    let secs = now.0.duration_since(last_down.0).as_secs_f64();
                    if secs > 0.0 && now.1 > 0 {
                        let mbps = (now.1 - last_down.1) as f64 * 8.0 / secs / 1e6;
                        live.set("download_mbps", mbps);
                        live.set("goodput_mbps", last_up_mbps + mbps);
                    }
                }
                last_down = now;
                // A connection that moves nothing for STALL while senders
                // should be writing is stuck: fail now, not at the deadline.
                let moved = total_sent.load(Ordering::Relaxed) + now.1;
                if moved != progress.1 {
                    progress = (now.0, moved);
                } else if !draining && now.0.duration_since(progress.0) >= STALL {
                    bg.iter().for_each(JoinHandle::abort);
                    let down = down.then(|| stats_of(&acc_down)).filter(|d| d.bytes_total > 0);
                    ctx.set_partial(partial_output(ctx.origin_ns(), p, None, down.as_ref(), None));
                    return Err(AppError::new(
                        ErrorCode::Io,
                        format!(
                            "the QUIC connection stalled: no payload moved for {} s",
                            STALL.as_secs()
                        ),
                    ));
                }
            }
            j = set.join_next() => match j {
                Some(Ok(o)) => outcomes.push(o),
                Some(Err(e)) => {
                    let mut o = Outcome::new(StreamKind::Upload, u16::MAX);
                    o.error = Some(format!("a stream task ended abnormally: {e}"));
                    outcomes.push(o);
                }
                None => break,
            },
        }
    }
    bg.iter().for_each(JoinHandle::abort);
    if !draining {
        ctx.phase(Phase::Draining);
    }
    outcomes.sort_by_key(|o| (o.kind as u8, o.stream_idx));
    let info = Info {
        upload: outcomes
            .iter()
            .filter(|o| o.kind == StreamKind::Upload && o.stream_idx != u16::MAX)
            .map(Outcome::sent_block)
            .collect(),
    };
    let finished: Result<Stats, AppError> = ctx.finish(&info, DRAIN).await;
    // Rule 7 on this side: every ack has been exchanged; close now.
    conn.close();
    let down_stats = down.then(|| stats_of(&acc_down));
    let stats = match finished {
        Ok(s) => s,
        Err(e) => {
            if let Some(d) = down_stats.filter(|d| d.bytes_total > 0) {
                ctx.set_partial(partial_output(ctx.origin_ns(), p, None, Some(&d), None));
            }
            return Err(e);
        }
    };
    if up {
        if let Err(e) = ctx.clock().await {
            notes.push(format!(
                "the after-run clock estimate failed ({}); the upload receiver's times use the before estimate only",
                e.message
            ));
        }
    }
    let inputs = ThroughputInputs {
        p: p.clone(),
        origin_ns: ctx.origin_ns(),
        connect_ms,
        up: stats.upload.clone(),
        down: down_stats,
        map: ctx.clock_map(),
        outcomes,
        stats,
        before_rtt: ctx.before_rtt(),
        notes,
    };
    let stream_error = inputs
        .outcomes
        .iter()
        .filter_map(|o| o.error.clone())
        .chain(inputs.stats.errors.iter().cloned())
        .next();
    match (throughput_output(&inputs), stream_error) {
        (Ok(out), None) => Ok(out),
        (Ok(out), Some(e)) => {
            ctx.set_partial(out);
            Err(AppError::new(ErrorCode::Io, e))
        }
        (Err(e), err) => {
            ctx.set_partial(partial_output(
                inputs.origin_ns,
                p,
                inputs.up.as_ref(),
                inputs.down.as_ref(),
                inputs.map,
            ));
            Err(err.map_or(e, |m| AppError::new(ErrorCode::Io, m)))
        }
    }
}

/// Everything a throughput run measured, for [`throughput_output`].
#[derive(Debug, Clone)]
struct ThroughputInputs {
    p: QuicParams,
    origin_ns: u64,
    connect_ms: f64,
    /// Upload receiver, responder clock.
    up: Option<ReceiverStats>,
    /// Download receiver, local clock.
    down: Option<ReceiverStats>,
    map: Option<ClockMap>,
    /// Initiator-side stream tasks.
    outcomes: Vec<Outcome>,
    /// The responder's stats.
    stats: Stats,
    before_rtt: Option<Duration>,
    notes: Vec<String>,
}

/// Map a responder time into this side's timebase.
fn to_local(map: Option<&ClockMap>, t: u64) -> u64 {
    map.map_or(t, |m| m.to_local(t))
}

fn rel_ms(origin_ns: u64, t: u64) -> f64 {
    (t as f64 - origin_ns as f64) / 1e6
}

/// A goodput series summing several receivers on one 250 ms grid over
/// `[from, to]` (prorating partial buckets), plotted at each cell's end.
fn sum_series(
    parts: &[&ReceiverStats],
    from_ns: u64,
    to_ns: u64,
    origin_ns: u64,
    warmup_ms: Option<f64>,
    key: &str,
    label: &str,
) -> Series {
    // Backstop: never walk more than an hour of grid, whatever the inputs say.
    let to_ns = to_ns.min(from_ns.saturating_add(MAX_SERIES_SPAN_NS));
    let mut points = Vec::new();
    let mut a = from_ns;
    while a < to_ns {
        let b = a.saturating_add(BUCKET_NS).min(to_ns);
        if b - a >= MIN_FINAL_BUCKET_NS {
            let bytes: f64 = parts.iter().map(|s| s.bytes_between(a, b)).sum();
            if let Some(v) = stats::mbps(bytes, b - a) {
                points.push((rel_ms(origin_ns, b), v));
            }
        }
        a = b;
    }
    Series {
        key: key.to_string(),
        label: label.to_string(),
        unit: "Mb/s".to_string(),
        points,
        warmup_ms,
    }
}

/// The quic notes every run carries.
fn base_notes() -> Vec<String> {
    vec![
        NOTE_PACKETS.to_string(),
        NOTE_NO_GSO.to_string(),
        NOTE_DOUBLE_ENCRYPTION.to_string(),
    ]
}

/// Summary, results and series of a throughput run (§8.1 goodput, §8.2 quic).
/// Fails on broken invariants (Σ buckets ≠ bytes_total, received > sent) and
/// on a window that is too short.
fn throughput_output(inp: &ThroughputInputs) -> Result<WorkloadOutput, AppError> {
    let p = &inp.p;
    let map = inp.map.as_ref();
    let warmup_ns = u64::from(p.warmup_secs) * 1_000_000_000;
    let bidi = p.direction == Direction::Bidirectional;
    for (st, what) in [(inp.up.as_ref(), "upload"), (inp.down.as_ref(), "download")] {
        if let Some(st) = st {
            st.check_invariants()
                .map_err(|e| AppError::internal(format!("{what} receiver: {e}")))?;
        }
    }
    // The upload accounting is the responder's: its timeline is checked
    // before any series or window math walks it.
    if let Some(up) = inp.up.as_ref() {
        up.check_span(max_data_span_ns(
            p.warmup_secs,
            p.duration_secs,
            ACK_TIMEOUT,
        ))
        .map_err(|e| {
            AppError::new(
                ErrorCode::ProtocolMismatch,
                format!("finish: the responder's upload accounting: {e}"),
            )
        })?;
    }
    let mut notes = base_notes();
    notes.extend(inp.notes.iter().cloned());

    // Per stream: received ≤ sent, ack == received.
    let mut stream_results = Vec::new();
    let mut sent_bytes = 0u64;
    let up_streams = inp
        .outcomes
        .iter()
        .filter(|o| o.kind == StreamKind::Upload && o.stream_idx != u16::MAX)
        .map(|o| (StreamKind::Upload, o.stream_idx, o.sent, o.acked));
    let down_streams = inp.stats.download.iter().map(|s| {
        (
            StreamKind::Download,
            s.stream_idx,
            s.sent_bytes,
            s.acked_bytes,
        )
    });
    for (kind, idx, sent, acked) in up_streams.chain(down_streams) {
        let receiver = match kind {
            StreamKind::Upload => inp.up.as_ref(),
            _ => inp.down.as_ref(),
        };
        let received = receiver.map_or(0, |r| r.stream_bytes(idx));
        if received > sent {
            return Err(AppError::internal(format!(
                "stream {idx}: the receiver counted {received} bytes but the sender sent {sent}"
            )));
        }
        if let Some(a) = acked.filter(|a| *a != received) {
            notes.push(format!(
                "stream {idx}: the receiver acknowledged {a} bytes but counted {received}"
            ));
        }
        sent_bytes += sent;
        stream_results.push(StreamResult {
            kind,
            stream_idx: idx,
            sent_bytes: sent,
            received_bytes: received,
            acked_bytes: acked,
        });
    }

    let origin = inp.origin_ns;
    let mut series = Vec::new();
    let mut summary = Vec::new();
    let (goodput, window, window_bytes, up_mbps, down_mbps) = match (
        inp.up.as_ref(),
        inp.down.as_ref(),
    ) {
        (Some(u), None) => {
            let g = u.goodput(p.warmup_secs)?;
            let local = u.map_time(|t| to_local(map, t));
            series.push(local.series(origin, p.warmup_secs, "goodput_mbps", "Goodput"));
            let w = (to_local(map, g.start_ns), to_local(map, g.end_ns));
            (g.mbps, w, g.bytes, None, None)
        }
        (None, Some(d)) => {
            let g = d.goodput(p.warmup_secs)?;
            series.push(d.series(origin, p.warmup_secs, "goodput_mbps", "Goodput"));
            (g.mbps, (g.start_ns, g.end_ns), g.bytes, None, None)
        }
        (Some(u), Some(d)) => {
            let ul = u.map_time(|t| to_local(map, t));
            if ul.bytes_total == 0 || d.bytes_total == 0 {
                return Err(AppError::internal(format!(
                    "no payload was received {}",
                    if ul.bytes_total == 0 {
                        "upstream"
                    } else {
                        "downstream"
                    }
                )));
            }
            let start = ul.epoch_ns.max(d.epoch_ns) + warmup_ns;
            let end = ul.last_ns.min(d.last_ns);
            if end < start + 1_000_000_000 {
                let overlap = end.saturating_sub(start.saturating_sub(warmup_ns));
                return Err(AppError::internal(format!(
                        "window too short: the two directions overlapped for {:.3} s, which is under warmup + 1 s ({} s)",
                        overlap as f64 / 1e9,
                        p.warmup_secs + 1
                    )));
            }
            let ub = ul.bytes_between(start, end);
            let db = d.bytes_between(start, end);
            let upm = stats::mbps(ub, end - start).unwrap_or(0.0);
            let dpm = stats::mbps(db, end - start).unwrap_or(0.0);
            let warm_ms = Some(rel_ms(origin, start));
            series.push(sum_series(
                &[&ul, d],
                ul.epoch_ns.min(d.epoch_ns),
                ul.last_ns.max(d.last_ns),
                origin,
                warm_ms,
                "goodput_mbps",
                "Goodput, sum of both directions",
            ));
            let mut us = ul.series(origin, p.warmup_secs, "upload_mbps", "Upload");
            us.warmup_ms = warm_ms;
            let mut ds = d.series(origin, p.warmup_secs, "download_mbps", "Download");
            ds.warmup_ms = warm_ms;
            series.push(us);
            series.push(ds);
            (
                upm + dpm,
                (start, end),
                (ub + db).round() as u64,
                Some(upm),
                Some(dpm),
            )
        }
        (None, None) => {
            return Err(AppError::internal("no receiver accounting came back"));
        }
    };

    summary.push(SummaryItem::new(
        "goodput_mbps",
        if bidi {
            "Goodput (sum of both directions)"
        } else {
            "Goodput"
        },
        Some(goodput),
        "Mb/s",
        Better::Higher,
    ));
    if bidi {
        summary.push(SummaryItem::new(
            "upload_mbps",
            "Upload",
            up_mbps,
            "Mb/s",
            Better::Higher,
        ));
        summary.push(SummaryItem::new(
            "download_mbps",
            "Download",
            down_mbps,
            "Mb/s",
            Better::Higher,
        ));
    }
    summary.push(SummaryItem::new(
        "connect_ms",
        "Connect",
        Some(inp.connect_ms),
        "ms",
        Better::Lower,
    ));
    summary.push(cpu_per_mb_item());
    summary.push(loaded_rtt_item());

    let ceiling = flow_control_ceiling_mbps(p.streams, inp.before_rtt);
    let per_direction = if bidi {
        up_mbps.unwrap_or(0.0).max(down_mbps.unwrap_or(0.0))
    } else {
        goodput
    };
    if ceiling.is_some_and(|c| per_direction >= CEILING_SHARE * c) {
        notes.push(NOTE_FLOW_CONTROL.to_string());
    }

    let receivers_total = inp.up.as_ref().map_or(0, |s| s.bytes_total)
        + inp.down.as_ref().map_or(0, |s| s.bytes_total);
    let receiver = inp
        .up
        .clone()
        .or_else(|| inp.down.clone())
        .unwrap_or_default();
    let results = Results::Throughput(ThroughputResults {
        direction: p.direction,
        streams: p.streams,
        connect_ms: inp.connect_ms,
        goodput_mbps: goodput,
        upload_mbps: up_mbps,
        download_mbps: down_mbps,
        window_bytes,
        window_ms: window.1.saturating_sub(window.0) as f64 / 1e6,
        bytes_total: receivers_total,
        received_bytes: receivers_total,
        sent_bytes,
        foreign_dropped: inp.stats.foreign_dropped,
        flow_control_ceiling_mbps: ceiling,
        receiver,
        receiver_download: if bidi { inp.down.clone() } else { None },
        stream_results,
    });
    Ok(WorkloadOutput {
        summary,
        results: serde_json::to_value(&results)
            .map_err(|e| AppError::internal(format!("serialise the quic results: {e}")))?,
        series,
        notes,
        window_ns: Some(window),
        goodput_bytes: Some(window_bytes),
    })
}

/// What a failed throughput run still shows: the receivers' series.
fn partial_output(
    origin_ns: u64,
    p: &QuicParams,
    up: Option<&ReceiverStats>,
    down: Option<&ReceiverStats>,
    map: Option<ClockMap>,
) -> WorkloadOutput {
    let (up_key, down_key) = if p.direction == Direction::Bidirectional {
        (("upload_mbps", "Upload"), ("download_mbps", "Download"))
    } else {
        (("goodput_mbps", "Goodput"), ("goodput_mbps", "Goodput"))
    };
    let mut series = Vec::new();
    if let Some(u) = up.filter(|u| u.bytes_total > 0) {
        let local = u.map_time(|t| to_local(map.as_ref(), t));
        series.push(local.series(origin_ns, p.warmup_secs, up_key.0, up_key.1));
    }
    if let Some(d) = down.filter(|d| d.bytes_total > 0) {
        series.push(d.series(origin_ns, p.warmup_secs, down_key.0, down_key.1));
    }
    WorkloadOutput {
        series,
        notes: base_notes(),
        results: Value::Object(Default::default()),
        ..Default::default()
    }
}

async fn run_latency(
    ctx: &mut RunCtx<'_>,
    p: &QuicParams,
    setup: &Setup,
) -> Result<WorkloadOutput, AppError> {
    let (conn, connect_ms) = connect(ctx, setup.port).await?;
    ctx.start().await?;
    let header = StreamHeader {
        run_tag: ctx.run_tag()?,
        stream_idx: 0,
        kind: StreamKind::Echo,
    };
    let mut io = open_with_header(&conn, header).await?;
    let len = p.payload_bytes as usize;
    let chunk = payload_chunk();
    let msg: Vec<u8> = (0..len).map(|i| chunk[i % chunk.len()]).collect();
    let mut buf = vec![0u8; len];
    let total = p.warmup_count + p.count;
    let live = ctx.live();
    let mut live_rtt = LiveRtt::new();
    let mut samples: Vec<RttSample> = Vec::with_capacity(total as usize);
    ctx.phase(if p.warmup_count > 0 {
        Phase::Warmup
    } else {
        Phase::Running
    });
    let mut failure = None;
    for i in 0..total {
        if i == p.warmup_count && i > 0 {
            ctx.phase(Phase::Running);
        }
        if ctx.is_cancelled() {
            return Err(AppError::cancelled());
        }
        let warmup = i < p.warmup_count;
        let send_ns = now_ns();
        let t = Instant::now();
        match tokio::time::timeout(ECHO_TIMEOUT, round_trip(&mut io, &msg, &mut buf)).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                failure = Some(AppError::io(format!("round trip {i}"), &e));
                break;
            }
            Err(_) => {
                failure = Some(AppError::timeout(format!(
                    "message {i} was not echoed within {} s",
                    ECHO_TIMEOUT.as_secs()
                )));
                break;
            }
        }
        let rtt = dur_ms(t.elapsed());
        samples.push(RttSample {
            send_ns,
            rtt_ms: Some(rtt),
            warmup,
        });
        live_rtt.push(rtt, warmup);
        live_rtt.emit_due(&live, "rtt_ms", "rtt_p50_ms");
        ctx.fraction((i + 1) as f32 / total as f32);
    }
    live_rtt.emit(&live, "rtt_ms", "rtt_p50_ms");
    if failure.is_none() {
        // Finish our half and wait for the responder's EOF, so its echo
        // task has ended before `finish`.
        if io.shutdown().await.is_ok() {
            let _ = tokio::time::timeout(ECHO_CLOSE_WAIT, io.read(&mut [0u8; 1])).await;
        }
    }
    ctx.phase(Phase::Draining);
    // Dropping the halves finishes/stops the stream, so even after a failed
    // round trip the responder's echo task ends before `finish` waits on it.
    drop(io);
    let finished: Result<Stats, AppError> = ctx.finish(&Info::default(), DRAIN).await;
    conn.close();
    if let Some(e) = failure {
        ctx.set_partial(latency_output(
            p,
            ctx.origin_ns(),
            connect_ms,
            &samples,
            &finished.unwrap_or_default(),
        ));
        return Err(e);
    }
    let stats = finished?;
    Ok(latency_output(
        p,
        ctx.origin_ns(),
        connect_ms,
        &samples,
        &stats,
    ))
}

/// Summary, results and series of a latency run.
fn latency_output(
    p: &QuicParams,
    origin_ns: u64,
    connect_ms: f64,
    samples: &[RttSample],
    stats: &Stats,
) -> WorkloadOutput {
    let rtt = Distribution::from_samples(&measured(samples, 1.0));
    let completed = samples
        .iter()
        .filter(|s| !s.warmup && s.rtt_ms.is_some())
        .count() as u32;
    let summary = vec![
        SummaryItem::new("rtt_p50_ms", "RTT p50", rtt.p50, "ms", Better::Lower),
        SummaryItem::new("rtt_p99_ms", "RTT p99", rtt.p99, "ms", Better::Lower),
        SummaryItem::new("rtt_max_ms", "RTT max", rtt.max, "ms", Better::Lower),
        SummaryItem::new(
            "connect_ms",
            "Connect",
            Some(connect_ms),
            "ms",
            Better::Lower,
        ),
    ];
    let results = Results::Latency(LatencyResults {
        connect_ms,
        payload_bytes: p.payload_bytes,
        count: p.count,
        warmup_count: p.warmup_count,
        completed,
        rtt_ms: rtt,
        samples_ms: raw_ms(samples, 1.0),
        echoes: stats.echoes,
        foreign_dropped: stats.foreign_dropped,
    });
    WorkloadOutput {
        summary,
        results: serde_json::to_value(&results).unwrap_or_default(),
        series: vec![rtt_series(samples, origin_ns, "rtt_ms", "RTT", 1.0)],
        notes: base_notes(),
        window_ns: sample_window(samples),
        goodput_bytes: None,
    }
}

// ---------------------------------------------------------------------------
// Responder
// ---------------------------------------------------------------------------

/// State the responder's tasks share.
#[derive(Debug, Default)]
struct Shared {
    acc_up: SharedAccounting,
    download: Mutex<Vec<StreamSent>>,
    errors: Mutex<Vec<String>>,
    echoes: AtomicU64,
    connections: AtomicU32,
    streams: AtomicU32,
    /// `(kind, stream_idx)` of every stream admitted (rule 2).
    admitted: Mutex<std::collections::HashSet<(u8, u16)>>,
    /// The download deadline, set by the first download stream: later ones
    /// never extend it.
    download_until: Mutex<Option<Instant>>,
}

/// Connections the responder accepts per run (the initiator opens one).
const MAX_CONNECTIONS: u32 = 2;

impl Shared {
    fn error(&self, e: String) {
        self.errors
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(e);
    }

    /// Admit a stream the validated params ask for (§6 rule 2: the
    /// responder never trusts the initiator's bounds): throughput takes up
    /// to `streams` uploads and `streams` downloads in the run's
    /// direction(s), each index once; latency takes one echo stream.
    fn admit_stream(&self, p: &QuicParams, kind: StreamKind, idx: u16) -> bool {
        let (up, down) = directions(p.direction);
        let (wanted, limit) = match (p.mode, kind) {
            (StreamMode::Throughput, StreamKind::Upload) => (up, p.streams),
            (StreamMode::Throughput, StreamKind::Download) => (down, p.streams),
            (StreamMode::Latency, StreamKind::Echo) => (true, 1),
            _ => (false, 0),
        };
        if !wanted {
            return false;
        }
        let mut seen = self.admitted.lock().unwrap_or_else(PoisonError::into_inner);
        let of_kind = seen.iter().filter(|(k, _)| *k == kind as u8).count();
        of_kind < limit as usize && seen.insert((kind as u8, idx))
    }

    fn download_until(&self, p: &QuicParams) -> Instant {
        *self
            .download_until
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get_or_insert_with(|| {
                Instant::now()
                    + Duration::from_secs(u64::from(p.warmup_secs) + u64::from(p.duration_secs))
            })
    }
}

/// The responder side of one quic run.
struct QuicResponder {
    r: ResponderCtx,
    p: QuicParams,
    stop: CancellationToken,
    accept: Option<JoinHandle<QuicListener>>,
    tracker: TaskTracker,
    shared: Arc<Shared>,
    ticker: Option<JoinHandle<()>>,
}

/// Responder side: `listen_quic(0)` and accept the initiator's streams.
pub async fn prepare(
    r: &ResponderCtx,
    p: &QuicParams,
    _client: Client,
) -> Result<(Setup, Box<dyn ResponderRun>), AppError> {
    let listener = r
        .node
        .listen_quic(0)
        .await
        .map_err(|e| AppError::from(e).context("listen_quic"))?;
    let port = listener.port();
    let stop = r.cancel_token().child_token();
    let shared = Arc::new(Shared::default());
    let tracker = TaskTracker::new();
    let accept = tokio::spawn(accept_loop(
        r.clone(),
        p.clone(),
        listener,
        stop.clone(),
        tracker.clone(),
        shared.clone(),
    ));
    Ok((
        Setup { port },
        Box::new(QuicResponder {
            r: r.clone(),
            p: p.clone(),
            stop,
            accept: Some(accept),
            tracker,
            shared,
            ticker: None,
        }),
    ))
}

/// Accept connections until stopped; returns the listener for teardown.
async fn accept_loop(
    r: ResponderCtx,
    p: QuicParams,
    listener: QuicListener,
    stop: CancellationToken,
    tracker: TaskTracker,
    shared: Arc<Shared>,
) -> QuicListener {
    loop {
        let conn = tokio::select! {
            _ = stop.cancelled() => break,
            c = listener.accept() => match c {
                Some(c) => c,
                None => break,
            },
        };
        // Rule 5: QUIC checks the WireGuard-authenticated source address.
        if !r.admit_ip(conn.remote_address().ip()) {
            conn.close();
            continue;
        }
        // Rule 2: the run's connections, no more.
        let admitted = shared
            .connections
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                (n < MAX_CONNECTIONS).then_some(n + 1)
            })
            .is_ok();
        if !admitted {
            r.count_foreign();
            conn.close();
            continue;
        }
        tokio::spawn(serve_connection(
            r.clone(),
            p.clone(),
            conn,
            stop.clone(),
            tracker.clone(),
            shared.clone(),
        ));
    }
    listener
}

/// Accept the streams of one connection until it closes or the run stops.
async fn serve_connection(
    r: ResponderCtx,
    p: QuicParams,
    conn: QuicConnection,
    stop: CancellationToken,
    tracker: TaskTracker,
    shared: Arc<Shared>,
) {
    loop {
        let stream = tokio::select! {
            _ = stop.cancelled() => break,
            s = conn.accept_stream() => match s {
                Ok(Some(s)) => s,
                Ok(None) => break,
                Err(e) => {
                    if !stop.is_cancelled() {
                        shared.error(format!("accept a stream: {e}"));
                    }
                    break;
                }
            },
        };
        shared.streams.fetch_add(1, Ordering::Relaxed);
        tracker.spawn(handle_stream(r.clone(), p.clone(), stream, shared.clone()));
    }
}

/// One accepted stream: check its header, then receive, send or echo.
async fn handle_stream(r: ResponderCtx, p: QuicParams, stream: QuicStream, shared: Arc<Shared>) {
    let mut io = split_join(stream);
    let Some(header) = read_tbn1(&r, &mut io).await else {
        return;
    };
    let idx = header.stream_idx;
    if !shared.admit_stream(&p, header.kind, idx) {
        r.count_foreign();
        return;
    }
    match header.kind {
        StreamKind::Upload => {
            if let Err(e) = receive_payload(&mut io, idx, &shared.acc_up).await {
                shared.error(format!("upload stream {idx}: {e}"));
            }
        }
        StreamKind::Download => {
            let until = shared.download_until(&p);
            let sent = AtomicU64::new(0);
            let total = AtomicU64::new(0);
            let res = send_payload(&mut io, until, &sent, &total).await;
            let mut block = StreamSent {
                stream_idx: idx,
                sent_bytes: sent.load(Ordering::Relaxed),
                acked_bytes: None,
                error: None,
            };
            match res {
                Ok(acked) => block.acked_bytes = Some(acked),
                Err(e) => {
                    block.error = Some(e.to_string());
                    shared.error(format!("download stream {idx}: {e}"));
                }
            }
            shared
                .download
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(block);
        }
        StreamKind::Echo => {
            if let Err(e) = echo_loop(&mut io, p.payload_bytes as usize, &shared.echoes).await {
                shared.error(format!("echo stream: {e}"));
            }
        }
        StreamKind::Frames => r.count_foreign(),
    }
}

impl QuicResponder {
    /// Rule 7: stream tasks (acks) first, then `QuicListener::close()`.
    /// Returns whether every stream task ended in time.
    async fn teardown(&mut self, wait: bool) -> bool {
        self.tracker.close();
        let drained = if wait {
            tokio::time::timeout(DRAIN, self.tracker.wait())
                .await
                .is_ok()
        } else {
            false
        };
        if let Some(t) = self.ticker.take() {
            t.abort();
        }
        self.stop.cancel();
        let listener = match self.accept.take() {
            Some(h) => h.await.ok(),
            None => None,
        };
        let mut td = self.r.teardown();
        if let Some(l) = listener {
            td.quic_listener(l);
        }
        td.run().await;
        drained
    }
}

impl ResponderRun for QuicResponder {
    fn start(&mut self) -> BoxFuture<'_, Result<(), AppError>> {
        Box::pin(async move {
            // Once only (the hub also drops duplicate starts).
            if self.ticker.is_some() {
                return Ok(());
            }
            if self.p.mode == StreamMode::Throughput {
                let key = match self.p.direction {
                    Direction::Upload => Some("goodput_mbps"),
                    Direction::Bidirectional => Some("upload_mbps"),
                    Direction::Download => None,
                };
                if let Some(key) = key {
                    self.ticker = Some(spawn_tick_goodput(
                        self.r.clone(),
                        self.shared.acc_up.clone(),
                        key,
                    ));
                }
            }
            Ok(())
        })
    }

    fn finish(mut self: Box<Self>, _info: Value) -> BoxFuture<'static, Result<Value, AppError>> {
        Box::pin(async move {
            let drained = self.teardown(true).await;
            let p = &self.p;
            let (up, _) = directions(p.direction);
            let upload =
                (p.mode == StreamMode::Throughput && up).then(|| stats_of(&self.shared.acc_up));
            if let Some(g) = upload.as_ref().and_then(|u| u.goodput(p.warmup_secs).ok()) {
                self.r
                    .set_resource_window(g.start_ns, g.end_ns, Some(g.bytes));
            }
            let mut download = std::mem::take(
                &mut *self
                    .shared
                    .download
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner),
            );
            download.sort_by_key(|s| s.stream_idx);
            let stats = Stats {
                upload,
                download,
                echoes: self.shared.echoes.load(Ordering::Relaxed),
                connections: self.shared.connections.load(Ordering::Relaxed),
                streams: self.shared.streams.load(Ordering::Relaxed),
                foreign_dropped: self.r.foreign_dropped(),
                drained,
                errors: std::mem::take(
                    &mut *self
                        .shared
                        .errors
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner),
                ),
            };
            serde_json::to_value(&stats)
                .map_err(|e| AppError::internal(format!("serialise the quic stats: {e}")))
        })
    }

    fn cancel(mut self: Box<Self>) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            self.teardown(false).await;
        })
    }
}

// ---------------------------------------------------------------------------
// Example
// ---------------------------------------------------------------------------

/// A synthetic receiver: `streams` streams, `per_bucket` bytes per 250 ms
/// bucket per stream after a ramp, for `secs` seconds from `epoch_ns`.
fn example_receiver(epoch_ns: u64, streams: u16, secs: u64, per_bucket: u64) -> ReceiverStats {
    let mut acc = ReceiverAccounting::new();
    let buckets = secs * 4;
    for b in 0..buckets {
        let ramp = match b {
            0 => 0.62,
            1 => 0.9,
            _ => 1.0 + 0.012 * (((b * 7) % 5) as f64 - 2.0),
        };
        for s in 0..streams {
            let at = epoch_ns + b * BUCKET_NS + u64::from(s) * 1_000_000;
            acc.record(s, (per_bucket as f64 * ramp) as u64, at);
        }
    }
    acc.stats()
}

/// A canonical example output for the schema fixtures (upload, 4 streams,
/// 10 s plus 1 s warmup, LAN).
pub fn example_output() -> WorkloadOutput {
    let origin = 1_760_000_000_000_000_000;
    let p = QuicParams::default();
    let up = example_receiver(origin + 400_000_000, 4, 11, 2_240_000);
    let outcomes = (0..4u16)
        .map(|i| Outcome {
            kind: StreamKind::Upload,
            stream_idx: i,
            sent: up.stream_bytes(i) + 65_536,
            received: 0,
            acked: Some(up.stream_bytes(i)),
            error: None,
        })
        .collect();
    let inputs = ThroughputInputs {
        p,
        origin_ns: origin,
        connect_ms: 61.5,
        up: Some(up),
        down: None,
        map: None,
        outcomes,
        stats: Stats {
            connections: 1,
            streams: 4,
            drained: true,
            ..Default::default()
        },
        before_rtt: Some(Duration::from_micros(2_400)),
        notes: Vec::new(),
    };
    let mut out = throughput_output(&inputs).unwrap_or_default();
    for item in &mut out.summary {
        match item.key.as_str() {
            "cpu_ms_per_mb" => item.value = Some(9.7),
            "loaded_rtt_p50_ms" => item.value = Some(4.4),
            _ => {}
        }
    }
    out
}

/// Latency, 64 B × 200 after 10 warmup round trips on one stream (p50 ≈ 0.3 ms).
fn example_latency() -> WorkloadOutput {
    let p = QuicParams {
        mode: StreamMode::Latency,
        ..QuicParams::default()
    };
    let origin = 1_760_000_000_000_000_000u64;
    let total = u64::from(p.warmup_count + p.count);
    let mut t = origin + 470_000_000;
    let samples: Vec<RttSample> = (0..total)
        .map(|i| {
            let warmup = i < u64::from(p.warmup_count);
            let rtt = 0.274
                + ((i * 31) % 19) as f64 * 0.0021
                + if i % 53 == 11 { 0.38 } else { 0.0 }
                + if warmup { 0.6 } else { 0.0 };
            let s = RttSample {
                send_ns: t,
                rtt_ms: Some(rtt),
                warmup,
            };
            t += (rtt * 1e6) as u64 + 15_000;
            s
        })
        .collect();
    let stats = Stats {
        echoes: total,
        connections: 1,
        streams: 1,
        drained: true,
        ..Stats::default()
    };
    latency_output(&p, origin, 44.8, &samples, &stats)
}

/// One example per quic mode for the schema fixtures.
pub fn examples() -> Vec<super::ModeExample> {
    vec![
        super::ModeExample {
            mode: "quic-throughput",
            params: WorkloadParams::Quic(QuicParams::default()),
            output: example_output(),
            clock: true,
        },
        super::ModeExample {
            mode: "quic-latency",
            params: WorkloadParams::Quic(QuicParams {
                mode: StreamMode::Latency,
                ..QuicParams::default()
            }),
            output: example_latency(),
            clock: false,
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ClockOffset;

    const S: u64 = 1_000_000_000;

    fn upload_inputs(up: ReceiverStats, sent_extra: u64) -> ThroughputInputs {
        let outcomes = up
            .streams
            .iter()
            .map(|s| Outcome {
                kind: StreamKind::Upload,
                stream_idx: s.stream_idx,
                sent: s.bytes + sent_extra,
                received: 0,
                acked: Some(s.bytes),
                error: None,
            })
            .collect();
        ThroughputInputs {
            p: QuicParams {
                streams: up.streams.len() as u32,
                ..QuicParams::default()
            },
            origin_ns: 0,
            connect_ms: 12.0,
            up: Some(up),
            down: None,
            map: None,
            outcomes,
            stats: Stats::default(),
            before_rtt: Some(Duration::from_millis(10)),
            notes: Vec::new(),
        }
    }

    #[test]
    fn responder_admits_only_the_streams_the_params_ask_for() {
        let bidi = QuicParams {
            direction: Direction::Bidirectional,
            streams: 2,
            ..QuicParams::default()
        };
        let sh = Shared::default();
        // Uploads 0, 1 and downloads 2, 3 (the initiator's numbering).
        assert!(sh.admit_stream(&bidi, StreamKind::Upload, 0));
        assert!(
            !sh.admit_stream(&bidi, StreamKind::Upload, 0),
            "duplicate index"
        );
        assert!(sh.admit_stream(&bidi, StreamKind::Upload, 1));
        assert!(
            !sh.admit_stream(&bidi, StreamKind::Upload, 7),
            "more than streams"
        );
        assert!(sh.admit_stream(&bidi, StreamKind::Download, 2));
        assert!(sh.admit_stream(&bidi, StreamKind::Download, 3));
        assert!(!sh.admit_stream(&bidi, StreamKind::Download, 4));
        assert!(!sh.admit_stream(&bidi, StreamKind::Echo, 0));
        assert!(!sh.admit_stream(&bidi, StreamKind::Frames, 0));
        let up = QuicParams::default();
        let sh = Shared::default();
        assert!(
            !sh.admit_stream(&up, StreamKind::Download, 0),
            "upload-only run"
        );
        let latency = QuicParams {
            mode: StreamMode::Latency,
            ..QuicParams::default()
        };
        let sh = Shared::default();
        assert!(sh.admit_stream(&latency, StreamKind::Echo, 0));
        assert!(!sh.admit_stream(&latency, StreamKind::Echo, 1));
        assert!(!sh.admit_stream(&latency, StreamKind::Upload, 0));
        // Every download stream shares the first one's deadline.
        let first = sh.download_until(&up);
        assert_eq!(sh.download_until(&up), first);
    }

    #[test]
    fn ceiling_follows_quinn_defaults() {
        let rtt = Some(Duration::from_millis(10));
        // 4 streams: min(5 MB, 10 MB)·8 / 0.01 s = 4000 Mb/s.
        assert!((flow_control_ceiling_mbps(4, rtt).unwrap() - 4000.0).abs() < 1e-6);
        // 16 streams hit the 10 MB connection window: 8000 Mb/s.
        assert!((flow_control_ceiling_mbps(16, rtt).unwrap() - 8000.0).abs() < 1e-6);
        assert_eq!(flow_control_ceiling_mbps(4, None), None);
        assert_eq!(flow_control_ceiling_mbps(4, Some(Duration::ZERO)), None);
    }

    #[test]
    fn upload_goodput_summary_and_notes() {
        // 2 streams × 1 MB per bucket for 11 s → 64 Mb/s after a 1 s warmup.
        let up = example_receiver(5 * S, 2, 11, 1_000_000);
        let out = throughput_output(&upload_inputs(up.clone(), 0)).unwrap();
        let keys: Vec<&str> = out.summary.iter().map(|s| s.key.as_str()).collect();
        assert_eq!(
            keys,
            [
                "goodput_mbps",
                "connect_ms",
                "cpu_ms_per_mb",
                "loaded_rtt_p50_ms"
            ]
        );
        let g = out.summary[0].value.unwrap();
        let expected = up.goodput(1).unwrap().mbps;
        assert!((g - expected).abs() < 1e-9);
        assert!((g - 64.0).abs() < 2.0, "{g}");
        assert_eq!(out.series[0].key, "goodput_mbps");
        assert_eq!(out.series[0].warmup_ms, Some(6000.0));
        assert!(out.notes.iter().any(|n| n == NOTE_PACKETS));
        assert!(!out.notes.iter().any(|n| n == NOTE_FLOW_CONTROL));
        let Results::Throughput(r) = serde_json::from_value(out.results.clone()).unwrap() else {
            panic!("throughput results expected");
        };
        assert_eq!(r.bytes_total, up.bytes_total);
        assert_eq!(r.received_bytes, r.sent_bytes);
        assert_eq!(r.stream_results.len(), 2);
        assert_eq!(out.goodput_bytes, Some(up.goodput(1).unwrap().bytes));
        assert_eq!(out.results["mode"], "throughput");
        // A slow path: the same goodput is ≥ 80 % of a 300 ms ceiling (2 streams:
        // 2.5 MB·8 / 0.3 s ≈ 66.7 Mb/s).
        let mut slow = upload_inputs(up, 0);
        slow.before_rtt = Some(Duration::from_millis(300));
        let out = throughput_output(&slow).unwrap();
        assert!(out.notes.iter().any(|n| n == NOTE_FLOW_CONTROL));
    }

    #[test]
    fn received_beyond_sent_fails() {
        let up = example_receiver(0, 2, 4, 100_000);
        let mut inp = upload_inputs(up, 0);
        inp.outcomes[1].sent -= 1;
        let err = throughput_output(&inp).unwrap_err();
        assert!(err.message.contains("stream 1"), "{}", err.message);
        // A short window fails too.
        let short = example_receiver(0, 1, 1, 100_000);
        let err = throughput_output(&upload_inputs(short, 0)).unwrap_err();
        assert!(err.message.contains("window too short"), "{}", err.message);
    }

    #[test]
    fn bidirectional_overlap_in_the_initiator_timebase() {
        // The responder's clock runs 5 s ahead; its upload receiver starts
        // 100 ms (initiator time) after the download receiver.
        let offset = 5 * S as i64;
        let down = example_receiver(10 * S, 2, 11, 500_000);
        let up = example_receiver(10 * S + 100_000_000 + 5 * S, 2, 11, 1_000_000);
        let before = ClockOffset {
            offset_ns: offset,
            delay_ns: 200_000,
            uncertainty_ns: 100_000,
            at_ns: 9 * S,
            samples: 16,
        };
        let map = ClockMap::new(before, None);
        let mut inp = upload_inputs(up.clone(), 0);
        inp.p.direction = Direction::Bidirectional;
        inp.down = Some(down.clone());
        inp.map = Some(map);
        inp.stats.download = down
            .streams
            .iter()
            .map(|s| StreamSent {
                stream_idx: s.stream_idx,
                sent_bytes: s.bytes,
                acked_bytes: Some(s.bytes),
                error: None,
            })
            .collect();
        let out = throughput_output(&inp).unwrap();
        let v = |k: &str| {
            out.summary
                .iter()
                .find(|s| s.key == k)
                .unwrap()
                .value
                .unwrap()
        };
        let (g, u, d) = (v("goodput_mbps"), v("upload_mbps"), v("download_mbps"));
        assert!((g - (u + d)).abs() < 1e-9);
        assert!((u - 64.0).abs() < 2.0, "upload {u}");
        assert!((d - 32.0).abs() < 1.0, "download {d}");
        // The window starts at the later epoch (mapped) plus the warmup.
        let (ws, we) = out.window_ns.unwrap();
        assert_eq!(ws, 10 * S + 100_000_000 + S);
        assert_eq!(we, down.last_ns.min(up.last_ns - 5 * S));
        let keys: Vec<&str> = out.series.iter().map(|s| s.key.as_str()).collect();
        assert_eq!(keys, ["goodput_mbps", "upload_mbps", "download_mbps"]);
        // Upload points sit in the initiator timebase, not 5 s later.
        let first_up = out.series[1].points[0].0;
        assert!((first_up - (10_350.0)).abs() < 1.0, "{first_up}");
    }

    #[test]
    fn latency_output_discards_warmup() {
        let samples: Vec<RttSample> = (0..110)
            .map(|i| RttSample {
                send_ns: i * 1_000_000,
                rtt_ms: if i == 50 {
                    None
                } else {
                    Some(1.0 + (i % 7) as f64 * 0.1)
                },
                warmup: i < 10,
            })
            .collect();
        let p = QuicParams {
            mode: StreamMode::Latency,
            count: 100,
            warmup_count: 10,
            ..QuicParams::default()
        };
        let out = latency_output(&p, 0, 30.0, &samples, &Stats::default());
        let keys: Vec<&str> = out.summary.iter().map(|s| s.key.as_str()).collect();
        assert_eq!(
            keys,
            ["rtt_p50_ms", "rtt_p99_ms", "rtt_max_ms", "connect_ms"]
        );
        let Results::Latency(r) = serde_json::from_value(out.results).unwrap() else {
            panic!("latency results expected");
        };
        assert_eq!((r.rtt_ms.samples, r.rtt_ms.lost, r.completed), (100, 1, 99));
        assert_eq!(r.samples_ms.len(), 99);
        // The lost sample ranks last (+∞): p99 (rank 99 of 100) is still
        // finite, p99.9 needs 1000 samples.
        assert!(r.rtt_ms.p99.is_some());
        assert_eq!(r.rtt_ms.p999, None);
        assert_eq!(out.series[0].warmup_ms, Some(9.0));
        assert_eq!(out.series[0].points.len(), 109);
        assert_eq!(out.window_ns.map(|w| w.0), Some(10_000_000));
    }

    #[tokio::test]
    async fn payload_and_echo_streams() {
        // Upload: pump + finish on one end, count + ack on the other.
        let (mut a, mut b) = tokio::io::duplex(256 * 1024);
        let acc = ReceiverAccounting::shared();
        let (stream, total) = (AtomicU64::new(0), AtomicU64::new(0));
        let until = Instant::now() + Duration::from_millis(30);
        let (acked, received) = tokio::join!(
            send_payload(&mut a, until, &stream, &total),
            receive_payload(&mut b, 3, &acc)
        );
        let (acked, received) = (acked.unwrap(), received.unwrap());
        assert_eq!(acked, received);
        assert_eq!(stream.load(Ordering::Relaxed), received);
        assert_eq!(total.load(Ordering::Relaxed), received);
        let st = stats_of(&acc);
        st.check_invariants().unwrap();
        assert_eq!(st.stream_bytes(3), received);

        // Echo: three round trips, then EOF ends the loop.
        let (mut a, mut b) = tokio::io::duplex(1024);
        let echoes = AtomicU64::new(0);
        let client = async {
            let mut buf = [0u8; 5];
            for _ in 0..3 {
                round_trip(&mut a, b"hello", &mut buf).await.unwrap();
                assert_eq!(&buf, b"hello");
            }
            a.shutdown().await.unwrap();
        };
        let (_, n) = tokio::join!(client, echo_loop(&mut b, 5, &echoes));
        assert_eq!(n.unwrap(), 3);
        assert_eq!(echoes.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn example_is_complete() {
        let out = example_output();
        assert!(out.summary.iter().all(|s| s.value.is_some()));
        let r: Results = serde_json::from_value(out.results).unwrap();
        let Results::Throughput(r) = r else {
            panic!("throughput example expected");
        };
        assert!(
            r.goodput_mbps > 250.0 && r.goodput_mbps < 320.0,
            "{}",
            r.goodput_mbps
        );
        assert_eq!(r.stream_results.len(), 4);
        r.receiver.check_invariants().unwrap();
    }
}
