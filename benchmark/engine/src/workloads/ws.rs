//! `ws` (RFC 027 §8.2 ws rr and ws stream): typed messages over the
//! WebSocket session every truffle app uses, on namespace `bench.ws`.
//!
//! **rr** (replaces the old `request-reply` workload):
//! - a request is `send_typed(peer, "bench.ws", "rr.req", {id, data:
//!   base64(payload)})`; the responder answers `rr.resp` with the same
//!   `{id, data}`;
//! - the initiator takes ONE `subscribe("bench.ws")` per run (before
//!   anything is sent), keeps an `id → Instant` map and a semaphore of
//!   `concurrency`; it never calls `send_and_wait` per request;
//! - the first `warmup_count` requests are discarded; a request unanswered
//!   after 10 s counts as lost (+∞ in the distribution);
//! - `msgs_per_sec` = answered measured requests ÷ (first measured send →
//!   last measured answer).
//!
//! **stream** (duration-based):
//! - each message is `send_typed(peer, "bench.ws", "bytes", {"encoding":
//!   "base64", "data": …, "seq": n})`, so `payload_bytes()` decodes it; as
//!   fast as backpressure allows, or paced at `rate_per_sec`;
//! - the responder drains on a dedicated task (subscribed in `prepare`),
//!   decoding every payload, counting unique seqs, `Lagged(n)` as
//!   `lag_dropped`, and unique messages per 250 ms;
//! - `missing = sent − unique`, `router_dropped = missing − lag_dropped`,
//!   `drop_pct = missing ÷ sent · 100`;
//! - finish: the responder answers once it has seen `last_seq`, or after
//!   2 s without messages, and only when its receiver is empty (`len() ==
//!   0`); the initiator's finish drain is 10 s + queued bytes ÷ its observed
//!   send rate, and control requests are retried up to 3×;
//! - `goodput_mbps` counts decoded payload over the receiver's first → last
//!   message; `wire_bytes_per_msg` is one envelope encoded with
//!   `truffle_core::envelope::JsonCodec`, and `wire_mbps` follows from it.
//!
//! Every `bench.ws` message not from the run's peer is ignored (rule 1).
//!
//! Summary keys: rr `rtt_p50_ms`, `rtt_p99_ms`, `msgs_per_sec`; stream
//! `msgs_per_sec`, `goodput_mbps`, `drop_pct`. Series: rr `rtt_ms`; stream
//! `msgs_per_sec`. Live: rr `rtt_ms`, `rtt_p50_ms`, `msgs_per_sec`; stream
//! (responder ticks) `msgs_per_sec`, `goodput_mbps`, `drop_pct`,
//! `received_msgs`, plus the sender's `sent_msgs_per_sec`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use base64::Engine as _;
use futures::future::BoxFuture;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::{oneshot, Semaphore};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use truffle_core::envelope::{Envelope, EnvelopeCodec, JsonCodec};
use truffle_core::NamespacedMessage;

use crate::clock::{now_ns, ClockMap};
use crate::dataplane::{payload_chunk, BUCKET_MS, BUCKET_NS, MIN_FINAL_BUCKET_NS};
use crate::error::{AppError, ErrorCode};
use crate::params::{WorkloadParams, WsMode, WsParams};
use crate::stats::{self, SeqTracker};
use crate::types::{Better, Distribution, Phase, Series, SummaryItem};
use crate::workloads::common::{
    dur_ms, measured, p99_defined, raw_ms, rtt_series, sample_window, LiveRtt, RttSample,
};
use crate::workloads::{ResponderCtx, ResponderRun, RunCtx, WorkloadOutput};
use crate::WS_NS;

/// Whether this build offers `ws` (drives `PeerHello.workloads`).
pub const IMPLEMENTED: bool = true;

/// The Progress sample keys a run with these params can set: the
/// initiator's own and the responder's `tick`s, which the hub merges in.
/// [`live_keys`](super::live_keys) adds the path sampler's `loaded_rtt_ms`
/// for time-based runs; `live-keys.json` in the schema fixtures is built
/// from it.
pub fn live_keys(p: &WsParams) -> Vec<&'static str> {
    match p.mode {
        WsMode::Rr => vec!["rtt_ms", "rtt_p50_ms", "msgs_per_sec"],
        // the sender's rate; the rest are the responder's ticks
        WsMode::Stream => vec![
            "sent_msgs_per_sec",
            "msgs_per_sec",
            "goodput_mbps",
            "drop_pct",
            "received_msgs",
        ],
    }
}

/// Note on every ws stream run (§8.2 ws stream).
pub const NOTE_LOSSLESS: &str =
    "the WebSocket itself is lossless, so every drop happens at a receiver";

/// rr request type.
pub const MSG_RR_REQ: &str = "rr.req";
/// rr reply type.
pub const MSG_RR_RESP: &str = "rr.resp";
/// stream message type (what `send_bytes` uses, so `payload_bytes()` decodes it).
pub const MSG_BYTES: &str = "bytes";

/// An rr request unanswered this long counts as lost.
const RR_TIMEOUT: Duration = Duration::from_secs(10);
/// The responder finishes a stream after this long without messages.
const STREAM_IDLE: Duration = Duration::from_secs(2);
/// Base of the initiator's finish drain for a stream (plus queued ÷ rate).
const STREAM_DRAIN_BASE: Duration = Duration::from_secs(10);
/// The drain allowance never exceeds this.
pub(crate) const STREAM_DRAIN_CAP: Duration = Duration::from_secs(120);
/// Messages core's per-peer outbound queue holds: the backlog estimate when
/// no receiver count arrived.
const SESSION_QUEUE: u64 = 256;
/// Control requests (`start`, `finish`) after ws traffic are re-sent (same
/// id) this many times within their timeout: router drops under load can
/// hit control messages too, and the responder answers a duplicate with the
/// first one's outcome.
const CONTROL_RETRIES: u32 = 3;
/// Drain allowance of an rr `finish` (nothing is queued once every reply came).
pub(crate) const RR_DRAIN: Duration = Duration::from_secs(5);
/// The responder gives up waiting for its drain task after this long.
const RESPONDER_FINISH_CAP: Duration = Duration::from_secs(150);
/// SeqTracker bound: generously above any rate the session can carry.
const MAX_MSGS_PER_SEC: u64 = 2_000_000;

// ---------------------------------------------------------------------------
// Control blocks and results
// ---------------------------------------------------------------------------

/// `prepare.client`: nothing beyond the params.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "WsClient")]
pub struct Client {}

/// `prepare.ack.setup`: nothing; the responder has subscribed to `bench.ws`
/// before it answers.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "WsSetup")]
pub struct Setup {}

/// `finish.info`: the sender's counts (stream).
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "WsInfo")]
pub struct Info {
    /// Messages whose `send_typed` returned Ok.
    pub sent: u64,
    /// Highest seq sent (`sent − 1`), if any.
    pub last_seq: Option<u64>,
}

/// `finish.ack.stats`: the responder's counts.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "WsStats")]
pub struct Stats {
    /// rr: requests answered.
    pub served: u64,
    /// rr: replies whose `send_typed` failed.
    pub send_errors: u64,
    /// stream: distinct seqs received.
    pub unique: u64,
    /// stream: repeated seqs.
    pub dup: u64,
    /// stream: unique seqs below an earlier maximum.
    pub reordered: u64,
    /// stream: highest seq received.
    pub max_seq: Option<u64>,
    /// Σ `Lagged(n)` on the responder's `bench.ws` receiver.
    pub lag_dropped: u64,
    /// stream: decoded payload bytes of unique messages.
    pub received_bytes: u64,
    /// stream: messages whose seq or payload did not decode.
    pub bad_payload: u64,
    /// stream: responder `now_ns()` of the first unique message (0 when none).
    pub first_ns: u64,
    /// stream: responder `now_ns()` of the last unique message.
    pub last_ns: u64,
    /// stream: unique messages per 250 ms from `first_ns`.
    pub buckets: Vec<u64>,
    /// stream: the drain ended on `last_seq` (false: on 2 s without messages).
    pub saw_last_seq: bool,
    /// `bench.ws` messages from anyone but the run's initiator, ignored.
    pub foreign_ignored: u64,
}

/// `results` of a ws rr run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "WsRrResults")]
pub struct RrResults {
    /// Decoded payload per request.
    pub payload_bytes: u32,
    /// Measured requests.
    pub count: u32,
    /// Discarded requests before them.
    pub warmup_count: u32,
    /// Requests in flight.
    pub concurrency: u32,
    /// Requests sent, warmup included.
    pub sent: u64,
    /// Measured requests answered.
    pub completed: u32,
    /// Measured requests not answered within 10 s.
    pub timeouts: u32,
    /// Round trips of the measured requests.
    pub rtt_ms: Distribution,
    /// Raw measured round trips in send order, ms (thinned to at most 5000).
    pub samples_ms: Vec<f64>,
    /// Answered measured requests per second.
    pub msgs_per_sec: Option<f64>,
    /// First measured send → last measured answer, ms.
    pub window_ms: f64,
    /// One `rr.req` envelope as `JsonCodec` encodes it, bytes.
    pub wire_bytes_per_msg: u64,
    /// Σ `Lagged(n)` on this side's `bench.ws` receiver.
    pub lag_dropped: u64,
    /// Σ `Lagged(n)` on the responder's receiver.
    pub peer_lag_dropped: u64,
    /// Replies with an unknown id or a payload of the wrong size.
    pub bad_replies: u64,
}

/// `results` of a ws stream run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "WsStreamResults")]
pub struct StreamResults {
    /// Decoded payload per message.
    pub payload_bytes: u32,
    /// Sending time, s.
    pub duration_secs: u32,
    /// Target rate; `None` = as fast as backpressure allows.
    pub rate_per_sec: Option<u32>,
    /// Messages whose `send_typed` returned Ok.
    pub sent: u64,
    /// Distinct seqs the receiver saw.
    pub unique: u64,
    /// `sent − unique`.
    pub missing: u64,
    /// Repeated seqs.
    pub dup: u64,
    /// Unique seqs below an earlier maximum.
    pub reordered: u64,
    /// Σ `Lagged(n)` on the receiver's subscription.
    pub lag_dropped: u64,
    /// `missing − lag_dropped`: lost before the subscription (the envelope
    /// router's own lag).
    pub router_dropped: u64,
    /// `missing ÷ sent · 100`.
    pub drop_pct: Option<f64>,
    /// Unique messages per second over the receiver's window.
    pub msgs_per_sec: f64,
    /// Decoded payload over the receiver's window, Mb/s.
    pub goodput_mbps: f64,
    /// Messages handed to `send_typed` per second while sending.
    pub sent_per_sec: f64,
    /// One envelope as `JsonCodec` encodes it, bytes.
    pub wire_bytes_per_msg: u64,
    /// `msgs_per_sec × wire_bytes_per_msg × 8 / 1e6`.
    pub wire_mbps: f64,
    /// The receiver's first → last message, ms.
    pub window_ms: f64,
    /// Payload bytes sent (`sent × payload_bytes`).
    pub sent_bytes: u64,
    /// Decoded payload bytes received (unique messages).
    pub received_bytes: u64,
    /// Messages whose seq or payload did not decode.
    pub bad_payload: u64,
    /// The receiver finished on `last_seq` (false: on 2 s without messages).
    pub saw_last_seq: bool,
}

/// `RunReport.results` of a `ws` run, tagged by `mode`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "mode", rename_all = "lowercase")]
#[schemars(rename = "WsResults")]
pub enum Results {
    /// Request/reply.
    Rr(RrResults),
    /// One-way stream.
    Stream(StreamResults),
}

// ---------------------------------------------------------------------------
// Shared pieces
// ---------------------------------------------------------------------------

/// `payload_bytes` of the deterministic pattern, base64-encoded.
fn payload_b64(len: usize) -> String {
    let chunk = payload_chunk();
    let bytes: Vec<u8> = (0..len).map(|i| chunk[i % chunk.len()]).collect();
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

/// Length of one envelope as the session puts it on the wire
/// (`truffle_core::envelope::JsonCodec`, with a timestamp).
pub fn wire_bytes(msg_type: &str, payload: &Value) -> u64 {
    let env = Envelope::new(WS_NS, msg_type, payload.clone()).with_timestamp();
    JsonCodec.encode(&env).map_or(0, |b| b.len() as u64)
}

/// The params as validated and used by the responder (after `prepare`).
fn used_params(ctx: &RunCtx<'_>, requested: &WsParams) -> WsParams {
    match ctx.params() {
        WorkloadParams::Ws(p) => p.clone(),
        _ => requested.clone(),
    }
}

/// Initiator side (§6 "Initiator flow" step 7).
pub async fn run(ctx: &mut RunCtx<'_>, p: &WsParams) -> Result<WorkloadOutput, AppError> {
    match p.mode {
        WsMode::Rr => run_rr(ctx, p).await,
        WsMode::Stream => run_stream(ctx, p).await,
    }
}

// ---------------------------------------------------------------------------
// rr, initiator
// ---------------------------------------------------------------------------

type Inflight = Arc<Mutex<HashMap<u64, (Instant, u64)>>>;

fn inflight_lock(m: &Inflight) -> std::sync::MutexGuard<'_, HashMap<u64, (Instant, u64)>> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

async fn run_rr(ctx: &mut RunCtx<'_>, p: &WsParams) -> Result<WorkloadOutput, AppError> {
    // ONE subscription per run, taken before any request can be answered.
    let mut rx = ctx.node.subscribe(WS_NS);
    let _: Setup = ctx.prepare(&Client {}).await?;
    let p = used_params(ctx, p);
    ctx.set_control_retries(CONTROL_RETRIES);
    ctx.start().await?;

    let warmup = u64::from(p.warmup_count);
    let total = warmup + u64::from(p.count);
    let data: Arc<str> = payload_b64(p.payload_bytes as usize).into();
    let sem = Arc::new(Semaphore::new(p.concurrency.max(1) as usize));
    let inflight: Inflight = Arc::new(Mutex::new(HashMap::new()));
    let sent = Arc::new(AtomicU64::new(0));
    let mut sender = {
        let (node, peer) = (ctx.node.clone(), ctx.peer_id().to_string());
        let (data, sem, inflight, sent) =
            (data.clone(), sem.clone(), inflight.clone(), sent.clone());
        tokio::spawn(async move {
            for id in 0..total {
                match sem.acquire().await {
                    Ok(permit) => permit.forget(),
                    Err(_) => break,
                }
                let payload = json!({ "id": id, "data": &*data });
                inflight_lock(&inflight).insert(id, (Instant::now(), now_ns()));
                node.send_typed(&peer, WS_NS, MSG_RR_REQ, &payload)
                    .await
                    .map_err(|e| AppError::from(e).context(format!("send request {id}")))?;
                sent.fetch_add(1, Ordering::Relaxed);
            }
            Ok::<(), AppError>(())
        })
    };

    let peer_id = ctx.peer_id().to_string();
    let live = ctx.live();
    let cancel = ctx.cancel_token();
    let mut live_rtt = LiveRtt::new();
    let mut samples: Vec<Option<RttSample>> = vec![None; total as usize];
    let (mut done, mut timeouts, mut lag, mut bad) = (0u64, 0u64, 0u64, 0u64);
    let mut answered_since = 0u64;
    let mut since = Instant::now();
    let mut sender_running = true;
    let mut running = warmup == 0;
    let mut failure = None;
    ctx.phase(if running {
        Phase::Running
    } else {
        Phase::Warmup
    });
    let mut every = tokio::time::interval(Duration::from_millis(BUCKET_MS));
    every.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    while done < total {
        tokio::select! {
            _ = cancel.cancelled() => {
                sender.abort();
                return Err(AppError::cancelled());
            }
            r = &mut sender, if sender_running => {
                sender_running = false;
                match r {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => { failure = Some(e); break; }
                    Err(e) => {
                        failure = Some(AppError::internal(format!("the request sender ended abnormally: {e}")));
                        break;
                    }
                }
            }
            m = rx.recv() => match m {
                Ok(msg) => {
                    if msg.from != peer_id || msg.msg_type != MSG_RR_RESP {
                        continue;
                    }
                    let Some(id) = msg.payload.get("id").and_then(Value::as_u64) else {
                        bad += 1;
                        continue;
                    };
                    let Some((t0, send_ns)) = inflight_lock(&inflight).remove(&id) else {
                        bad += 1;
                        continue;
                    };
                    let rtt = dur_ms(t0.elapsed());
                    sem.add_permits(1);
                    if msg.payload.get("data").and_then(Value::as_str).map(str::len) != Some(data.len()) {
                        bad += 1;
                    }
                    let is_warmup = id < warmup;
                    samples[id as usize] = Some(RttSample { send_ns, rtt_ms: Some(rtt), warmup: is_warmup });
                    live_rtt.push(rtt, is_warmup);
                    done += 1;
                    answered_since += 1;
                }
                Err(RecvError::Lagged(n)) => lag += n,
                Err(RecvError::Closed) => {
                    failure = Some(AppError::not_running());
                    break;
                }
            },
            _ = every.tick() => {
                let mut expired = Vec::new();
                inflight_lock(&inflight).retain(|id, (t0, ns)| {
                    let keep = t0.elapsed() < RR_TIMEOUT;
                    if !keep {
                        expired.push((*id, *ns));
                    }
                    keep
                });
                for (id, send_ns) in expired {
                    samples[id as usize] = Some(RttSample { send_ns, rtt_ms: None, warmup: id < warmup });
                    sem.add_permits(1);
                    done += 1;
                    if id >= warmup {
                        timeouts += 1;
                    }
                }
                if !running && sent.load(Ordering::Relaxed) > warmup {
                    running = true;
                    ctx.phase(Phase::Running);
                }
                let secs = since.elapsed().as_secs_f64();
                if secs > 0.0 && done > 0 {
                    live.set("msgs_per_sec", answered_since as f64 / secs);
                }
                (answered_since, since) = (0, Instant::now());
                live_rtt.emit(&live, "rtt_ms", "rtt_p50_ms");
                ctx.fraction(done as f32 / total as f32);
            }
        }
    }
    sender.abort();
    live_rtt.emit(&live, "rtt_ms", "rtt_p50_ms");
    drop(rx);
    let samples: Vec<RttSample> = samples.into_iter().flatten().collect();
    let finished: Result<Stats, AppError> = ctx.finish(&Info::default(), RR_DRAIN).await;
    let counts = RrCounts {
        sent: sent.load(Ordering::Relaxed),
        timeouts: timeouts as u32,
        lag,
        bad,
        wire: wire_bytes(MSG_RR_REQ, &json!({ "id": total / 2, "data": &*data })),
    };
    if let Some(e) = failure {
        let stats = finished.unwrap_or_default();
        ctx.set_partial(rr_output(&p, ctx.origin_ns(), &samples, &counts, &stats));
        return Err(e);
    }
    let stats = finished?;
    let out = rr_output(&p, ctx.origin_ns(), &samples, &counts, &stats);
    if out.summary.iter().all(|s| s.value.is_none()) {
        ctx.set_partial(out);
        return Err(AppError::timeout(format!(
            "none of the {} measured requests was answered within {} s",
            p.count,
            RR_TIMEOUT.as_secs()
        )));
    }
    Ok(out)
}

/// The initiator's rr counts besides the samples.
#[derive(Debug, Clone, Copy, PartialEq)]
struct RrCounts {
    sent: u64,
    timeouts: u32,
    lag: u64,
    bad: u64,
    wire: u64,
}

/// Summary, results and series of an rr run.
fn rr_output(
    p: &WsParams,
    origin_ns: u64,
    samples: &[RttSample],
    c: &RrCounts,
    stats: &Stats,
) -> WorkloadOutput {
    let rtt = Distribution::from_samples(&measured(samples, 1.0));
    let completed = samples
        .iter()
        .filter(|s| !s.warmup && s.rtt_ms.is_some())
        .count() as u32;
    let window = sample_window(samples);
    let window_ns = window.map_or(0, |(a, b)| b - a);
    let msgs_per_sec = stats::per_sec(f64::from(completed), window_ns);
    let mut summary = vec![SummaryItem::new(
        "rtt_p50_ms",
        "RTT p50",
        rtt.p50,
        "ms",
        Better::Lower,
    )];
    if p99_defined(p.count) {
        summary.push(SummaryItem::new(
            "rtt_p99_ms",
            "RTT p99",
            rtt.p99,
            "ms",
            Better::Lower,
        ));
    }
    summary.push(SummaryItem::new(
        "msgs_per_sec",
        "Requests per second",
        msgs_per_sec,
        "msg/s",
        Better::Higher,
    ));
    let mut notes = Vec::new();
    if c.lag + stats.lag_dropped > 0 {
        notes.push(format!(
            "{} messages were dropped by a lagging subscriber; their requests count as lost",
            c.lag + stats.lag_dropped
        ));
    }
    if c.timeouts > 0 {
        notes.push(format!(
            "{} requests were not answered within {} s and count as lost",
            c.timeouts,
            RR_TIMEOUT.as_secs()
        ));
    }
    let results = Results::Rr(RrResults {
        payload_bytes: p.payload_bytes,
        count: p.count,
        warmup_count: p.warmup_count,
        concurrency: p.concurrency,
        sent: c.sent,
        completed,
        timeouts: c.timeouts,
        rtt_ms: rtt,
        samples_ms: raw_ms(samples, 1.0),
        msgs_per_sec,
        window_ms: window_ns as f64 / 1e6,
        wire_bytes_per_msg: c.wire,
        lag_dropped: c.lag,
        peer_lag_dropped: stats.lag_dropped,
        bad_replies: c.bad,
    });
    WorkloadOutput {
        summary,
        results: serde_json::to_value(&results).unwrap_or_default(),
        series: vec![rtt_series(samples, origin_ns, "rtt_ms", "RTT", 1.0)],
        notes,
        window_ns: window,
        goodput_bytes: None,
    }
}

// ---------------------------------------------------------------------------
// stream, initiator
// ---------------------------------------------------------------------------

/// Messages due by `elapsed` at `rate` per second (the first one at once).
fn due(elapsed: Duration, rate: u32) -> u64 {
    (elapsed.as_secs_f64() * f64::from(rate)) as u64 + 1
}

/// The initiator's finish drain: 10 s + queued bytes ÷ the observed send rate,
/// capped at 2 min. `queued_msgs` is what the receiver had not counted yet.
fn drain_allowance(queued_msgs: u64, wire_bytes: u64, sent_bytes_per_sec: f64) -> Duration {
    let queued = queued_msgs.saturating_mul(wire_bytes) as f64;
    let extra = if sent_bytes_per_sec > 0.0 {
        Duration::from_secs_f64((queued / sent_bytes_per_sec).min(STREAM_DRAIN_CAP.as_secs_f64()))
    } else {
        Duration::ZERO
    };
    (STREAM_DRAIN_BASE + extra).min(STREAM_DRAIN_CAP)
}

/// What the sender task did.
#[derive(Debug, Clone, Default)]
struct Sent {
    sent: u64,
    elapsed: Duration,
    error: Option<AppError>,
}

async fn run_stream(ctx: &mut RunCtx<'_>, p: &WsParams) -> Result<WorkloadOutput, AppError> {
    let _: Setup = ctx.prepare(&Client {}).await?;
    let p = used_params(ctx, p);
    ctx.set_control_retries(CONTROL_RETRIES);
    // The receiver's series are mapped into this timebase.
    ctx.clock().await?;
    ctx.start().await?;
    ctx.phase(Phase::Running);
    let data: Arc<str> = payload_b64(p.payload_bytes as usize).into();
    let sent_ctr = Arc::new(AtomicU64::new(0));
    let duration = Duration::from_secs(u64::from(p.duration_secs));
    let mut sender = {
        let (node, peer) = (ctx.node.clone(), ctx.peer_id().to_string());
        let (data, sent_ctr, rate) = (data.clone(), sent_ctr.clone(), p.rate_per_sec);
        tokio::spawn(async move {
            let t0 = Instant::now();
            let until = t0 + duration;
            let mut out = Sent::default();
            let mut seq = 0u64;
            while Instant::now() < until {
                if let Some(rate) = rate {
                    if seq >= due(t0.elapsed(), rate) {
                        let next = t0 + Duration::from_secs_f64(seq as f64 / f64::from(rate));
                        tokio::time::sleep_until(next.min(until)).await;
                        continue;
                    }
                }
                let payload = json!({ "encoding": "base64", "data": &*data, "seq": seq });
                if let Err(e) = node.send_typed(&peer, WS_NS, MSG_BYTES, &payload).await {
                    out.error = Some(AppError::from(e).context(format!("send message {seq}")));
                    break;
                }
                seq += 1;
                sent_ctr.store(seq, Ordering::Relaxed);
            }
            out.sent = seq;
            out.elapsed = t0.elapsed();
            out
        })
    };

    let live = ctx.live();
    let cancel = ctx.cancel_token();
    let mut every = tokio::time::interval(Duration::from_millis(BUCKET_MS));
    every.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut last = (Instant::now(), 0u64);
    let mut received_by_peer: Option<u64> = None;
    let sent = loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                sender.abort();
                return Err(AppError::cancelled());
            }
            r = &mut sender => {
                break r.unwrap_or_else(|e| Sent {
                    error: Some(AppError::internal(format!("the sender ended abnormally: {e}"))),
                    sent: sent_ctr.load(Ordering::Relaxed),
                    ..Sent::default()
                });
            }
            _ = every.tick() => {
                let now = (Instant::now(), sent_ctr.load(Ordering::Relaxed));
                let secs = now.0.duration_since(last.0).as_secs_f64();
                if secs > 0.0 {
                    live.set("sent_msgs_per_sec", (now.1 - last.1) as f64 / secs);
                }
                last = now;
                while let Some(t) = ctx.try_tick() {
                    if let Some(v) = t.values.get("received_msgs") {
                        received_by_peer = Some(*v as u64);
                    }
                }
            }
        }
    };
    ctx.phase(Phase::Draining);
    while let Some(t) = ctx.try_tick() {
        if let Some(v) = t.values.get("received_msgs") {
            received_by_peer = Some(*v as u64);
        }
    }
    let wire = wire_bytes(
        MSG_BYTES,
        &json!({ "encoding": "base64", "data": &*data, "seq": sent.sent / 2 }),
    );
    let queued = received_by_peer.map_or(SESSION_QUEUE.min(sent.sent), |r| {
        sent.sent.saturating_sub(r)
    });
    let send_rate = if sent.elapsed.is_zero() {
        0.0
    } else {
        (sent.sent * wire) as f64 / sent.elapsed.as_secs_f64()
    };
    let drain = drain_allowance(queued, wire, send_rate);
    let info = Info {
        sent: sent.sent,
        last_seq: sent.sent.checked_sub(1),
    };
    let finished: Result<Stats, AppError> = ctx.finish(&info, drain).await;
    let mut notes = Vec::new();
    if finished.is_ok() {
        if let Err(e) = ctx.clock().await {
            notes.push(format!(
                "the after-run clock estimate failed ({}); the receiver's times use the before estimate only",
                e.message
            ));
        }
    }
    let inputs = StreamInputs {
        p: p.clone(),
        origin_ns: ctx.origin_ns(),
        map: ctx.clock_map(),
        sent: sent.sent,
        send_elapsed: sent.elapsed,
        wire,
        notes,
    };
    if let Some(e) = sent.error {
        if let Ok(stats) = &finished {
            if let Ok(out) = stream_output(&inputs, stats) {
                ctx.set_partial(out);
            }
        }
        return Err(e);
    }
    let stats = finished?;
    match stream_output(&inputs, &stats) {
        Ok(out) => Ok(out),
        Err(e) => {
            ctx.set_partial(WorkloadOutput {
                series: vec![msgs_series(&stats, inputs.origin_ns, inputs.map)],
                notes: vec![NOTE_LOSSLESS.to_string()],
                results: Value::Object(Default::default()),
                ..WorkloadOutput::default()
            });
            Err(e)
        }
    }
}

/// What a stream run measured besides the responder's stats.
#[derive(Debug, Clone)]
struct StreamInputs {
    p: WsParams,
    origin_ns: u64,
    map: Option<ClockMap>,
    sent: u64,
    send_elapsed: Duration,
    wire: u64,
    notes: Vec<String>,
}

/// The receiver's unique messages per second, one point per 250 ms bucket
/// at its end in the initiator's timebase; the final partial bucket is
/// divided by its true length.
fn msgs_series(stats: &Stats, origin_ns: u64, map: Option<ClockMap>) -> Series {
    let to_local = |t: u64| map.map_or(t, |m| m.to_local(t));
    let n = stats.buckets.len();
    let mut points = Vec::with_capacity(n);
    for (i, &count) in stats.buckets.iter().enumerate() {
        let start = stats.first_ns + BUCKET_NS * i as u64;
        let end = if i + 1 == n {
            stats.last_ns.max(start)
        } else {
            start + BUCKET_NS
        };
        let len = end - start;
        if i + 1 == n && len < MIN_FINAL_BUCKET_NS {
            continue;
        }
        if let Some(v) = stats::per_sec(count as f64, len) {
            points.push(((to_local(end) as f64 - origin_ns as f64) / 1e6, v));
        }
    }
    Series {
        key: "msgs_per_sec".into(),
        label: "Received".into(),
        unit: "msg/s".into(),
        points,
        warmup_ms: None,
    }
}

/// Summary, results and series of a stream run (§8.2 ws stream).
fn stream_output(inp: &StreamInputs, stats: &Stats) -> Result<WorkloadOutput, AppError> {
    let p = &inp.p;
    let sent = inp.sent;
    let unique = stats.unique.min(sent);
    let missing = sent - unique;
    let lag = stats.lag_dropped;
    let router = missing.saturating_sub(lag);
    let window_ns = stats.last_ns.saturating_sub(stats.first_ns);
    if unique < 2 || window_ns == 0 {
        return Err(AppError::internal(format!(
            "too few messages arrived to measure a rate ({unique} of {sent})"
        )));
    }
    let msgs_per_sec = stats::per_sec(unique as f64, window_ns).unwrap_or(0.0);
    let goodput = stats::mbps(stats.received_bytes as f64, window_ns).unwrap_or(0.0);
    let drop_pct = stats::pct(missing as f64, sent as f64);
    let wire_mbps = msgs_per_sec * inp.wire as f64 * 8.0 / 1e6;
    let sent_per_sec = if inp.send_elapsed.is_zero() {
        0.0
    } else {
        sent as f64 / inp.send_elapsed.as_secs_f64()
    };
    let summary = vec![
        SummaryItem::new(
            "msgs_per_sec",
            "Messages per second",
            Some(msgs_per_sec),
            "msg/s",
            Better::Higher,
        ),
        SummaryItem::new(
            "goodput_mbps",
            "Goodput (decoded payload)",
            Some(goodput),
            "Mb/s",
            Better::Higher,
        ),
        SummaryItem::new("drop_pct", "Dropped", drop_pct, "%", Better::Lower),
    ];
    let mut notes = vec![NOTE_LOSSLESS.to_string()];
    notes.extend(inp.notes.iter().cloned());
    if lag > 0 {
        notes.push(format!(
            "{lag} messages were dropped by the receiver's lagging subscription"
        ));
    }
    if router > 0 {
        notes.push(format!(
            "{router} messages were lost before the subscription (the envelope router lagged)"
        ));
    }
    if !stats.saw_last_seq && sent > 0 {
        notes.push(format!(
            "the receiver never saw the last message; it finished after {} s without messages",
            STREAM_IDLE.as_secs()
        ));
    }
    let to_local = |t: u64| inp.map.map_or(t, |m| m.to_local(t));
    let results = Results::Stream(StreamResults {
        payload_bytes: p.payload_bytes,
        duration_secs: p.duration_secs,
        rate_per_sec: p.rate_per_sec,
        sent,
        unique,
        missing,
        dup: stats.dup,
        reordered: stats.reordered,
        lag_dropped: lag,
        router_dropped: router,
        drop_pct,
        msgs_per_sec,
        goodput_mbps: goodput,
        sent_per_sec,
        wire_bytes_per_msg: inp.wire,
        wire_mbps,
        window_ms: window_ns as f64 / 1e6,
        sent_bytes: sent * u64::from(p.payload_bytes),
        received_bytes: stats.received_bytes,
        bad_payload: stats.bad_payload,
        saw_last_seq: stats.saw_last_seq,
    });
    Ok(WorkloadOutput {
        summary,
        results: serde_json::to_value(&results)
            .map_err(|e| AppError::internal(format!("serialise the ws results: {e}")))?,
        series: vec![msgs_series(stats, inp.origin_ns, inp.map)],
        notes,
        window_ns: Some((to_local(stats.first_ns), to_local(stats.last_ns))),
        goodput_bytes: Some(stats.received_bytes),
    })
}

// ---------------------------------------------------------------------------
// Responder
// ---------------------------------------------------------------------------

/// Responder side: subscribe to `bench.ws` now (so nothing is missed) and
/// start the rr echo or the stream drain task.
pub async fn prepare(
    r: &ResponderCtx,
    p: &WsParams,
    _client: Client,
) -> Result<(Setup, Box<dyn ResponderRun>), AppError> {
    let rx = r.node.subscribe(WS_NS);
    let stop = r.cancel_token().child_token();
    let (finish_tx, finish_rx) = oneshot::channel();
    let task = match p.mode {
        WsMode::Rr => tokio::spawn(rr_responder(r.clone(), rx, stop.clone())),
        WsMode::Stream => tokio::spawn(stream_drain(
            r.clone(),
            p.clone(),
            rx,
            stop.clone(),
            finish_rx,
        )),
    };
    Ok((
        Setup {},
        Box::new(WsResponder {
            r: r.clone(),
            mode: p.mode,
            stop,
            task: Some(task),
            finish_tx: Some(finish_tx),
        }),
    ))
}

/// Answer every `rr.req` from the initiator with `rr.resp` carrying the same
/// `{id, data}`.
async fn rr_responder(
    r: ResponderCtx,
    mut rx: tokio::sync::broadcast::Receiver<NamespacedMessage>,
    stop: CancellationToken,
) -> Stats {
    let mut st = Stats::default();
    let peer = r.initiator.tailscale_id.clone();
    loop {
        let msg = tokio::select! {
            _ = stop.cancelled() => break,
            m = rx.recv() => m,
        };
        match msg {
            Ok(msg) => {
                if msg.from != peer {
                    st.foreign_ignored += 1;
                    continue;
                }
                if msg.msg_type != MSG_RR_REQ {
                    continue;
                }
                match r
                    .node
                    .send_typed(&peer, WS_NS, MSG_RR_RESP, &msg.payload)
                    .await
                {
                    Ok(()) => st.served += 1,
                    Err(_) => st.send_errors += 1,
                }
            }
            Err(RecvError::Lagged(n)) => st.lag_dropped += n,
            Err(RecvError::Closed) => break,
        }
    }
    st
}

/// The stream receiver's counters.
#[derive(Debug)]
struct StreamCounter {
    seqs: SeqTracker,
    payload_len: u64,
    received_bytes: u64,
    bad: u64,
    first_ns: Option<u64>,
    last_ns: u64,
    buckets: Vec<u64>,
}

impl StreamCounter {
    fn new(limit: u64, payload_len: u64) -> Self {
        Self {
            seqs: SeqTracker::new(limit),
            payload_len,
            received_bytes: 0,
            bad: 0,
            first_ns: None,
            last_ns: 0,
            buckets: Vec::new(),
        }
    }

    /// Count one message (its seq and decoded payload length) received at
    /// `at_ns`. A message without a seq or a decodable payload is `bad`
    /// and not counted; one of the wrong size is counted and also `bad`.
    fn record(&mut self, seq: Option<u64>, decoded_len: Option<usize>, at_ns: u64) {
        let (Some(seq), Some(len)) = (seq, decoded_len) else {
            self.bad += 1;
            return;
        };
        if len as u64 != self.payload_len {
            self.bad += 1;
        }
        match self.seqs.record(seq) {
            stats::SeqOutcome::New | stats::SeqOutcome::Reordered => {}
            stats::SeqOutcome::Duplicate | stats::SeqOutcome::OutOfRange => return,
        }
        self.received_bytes += len as u64;
        let epoch = *self.first_ns.get_or_insert(at_ns);
        let idx = (at_ns.saturating_sub(epoch) / BUCKET_NS) as usize;
        if idx >= self.buckets.len() {
            self.buckets.resize(idx + 1, 0);
        }
        self.buckets[idx] += 1;
        self.last_ns = self.last_ns.max(at_ns);
    }

    /// Distinct seqs so far.
    fn unique(&self) -> u64 {
        self.seqs.unique()
    }

    /// Whether `seq` arrived.
    fn has(&self, seq: u64) -> bool {
        self.seqs.contains(seq)
    }

    /// Receiver-side drop estimate so far: seqs up to the highest seen that
    /// never arrived, as a share of them.
    fn drop_pct_so_far(&self) -> Option<f64> {
        let span = self.seqs.max_seen()? + 1;
        stats::pct(span.saturating_sub(self.unique()) as f64, span as f64)
    }

    /// The stats block (without the lag and foreign counts).
    fn stats(&self) -> Stats {
        Stats {
            unique: self.seqs.unique(),
            dup: self.seqs.dup(),
            reordered: self.seqs.reordered(),
            max_seq: self.seqs.max_seen(),
            received_bytes: self.received_bytes,
            bad_payload: self.bad,
            first_ns: self.first_ns.unwrap_or(0),
            last_ns: self.last_ns,
            buckets: self.buckets.clone(),
            ..Stats::default()
        }
    }
}

/// Whether a finishing stream receiver may answer: it has seen `last_seq`,
/// or `idle` passed without messages, and either way its receiver is empty.
fn drained(saw_last: bool, idle: Duration, queued: usize) -> bool {
    queued == 0 && (saw_last || idle >= STREAM_IDLE)
}

/// Drain the stream on a dedicated task until finish (seen `last_seq`, or
/// 2 s without messages; then only with an empty receiver) or stop.
async fn stream_drain(
    r: ResponderCtx,
    p: WsParams,
    mut rx: tokio::sync::broadcast::Receiver<NamespacedMessage>,
    stop: CancellationToken,
    mut finish_rx: oneshot::Receiver<Option<u64>>,
) -> Stats {
    let peer = r.initiator.tailscale_id.clone();
    let limit = u64::from(p.duration_secs + 10) * MAX_MSGS_PER_SEC;
    let mut c = StreamCounter::new(limit, u64::from(p.payload_bytes));
    let (mut lag, mut foreign) = (0u64, 0u64);
    let mut finishing: Option<Option<u64>> = None;
    let mut last_activity = Instant::now();
    let mut tick = tokio::time::interval(Duration::from_millis(BUCKET_MS));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut last_tick = (Instant::now(), 0u64, 0u64);
    let saw_last =
        |c: &StreamCounter, f: Option<Option<u64>>| matches!(f, Some(Some(seq)) if c.has(seq));
    loop {
        tokio::select! {
            _ = stop.cancelled() => break,
            f = &mut finish_rx, if finishing.is_none() => {
                finishing = Some(f.unwrap_or(None));
            }
            m = rx.recv() => match m {
                Ok(msg) => {
                    if msg.from != peer {
                        foreign += 1;
                        continue;
                    }
                    if msg.msg_type != MSG_BYTES {
                        continue;
                    }
                    let at = now_ns();
                    last_activity = Instant::now();
                    let seq = msg.payload.get("seq").and_then(Value::as_u64);
                    let len = msg.payload_bytes().map(|b| b.len());
                    c.record(seq, len, at);
                }
                Err(RecvError::Lagged(n)) => lag += n,
                Err(RecvError::Closed) => break,
            },
            _ = tick.tick() => {
                if finishing.is_none() {
                    let now = (Instant::now(), c.unique(), c.received_bytes);
                    let secs = now.0.duration_since(last_tick.0).as_secs_f64();
                    if secs > 0.0 && now.1 > 0 {
                        let values = [
                            ("msgs_per_sec", (now.1 - last_tick.1) as f64 / secs),
                            ("goodput_mbps", (now.2 - last_tick.2) as f64 * 8.0 / secs / 1e6),
                            ("drop_pct", c.drop_pct_so_far().unwrap_or(0.0)),
                            ("received_msgs", now.1 as f64),
                        ];
                        let r2 = r.clone();
                        tokio::spawn(async move { r2.tick(values).await });
                    }
                    last_tick = now;
                }
            }
        }
        if let Some(f) = finishing {
            if drained(saw_last(&c, finishing), last_activity.elapsed(), rx.len()) {
                let mut st = c.stats();
                st.saw_last_seq = saw_last(&c, Some(f));
                st.lag_dropped = lag;
                st.foreign_ignored = foreign;
                return st;
            }
        }
    }
    let mut st = c.stats();
    st.lag_dropped = lag;
    st.foreign_ignored = foreign;
    st
}

/// The responder side of one ws run.
struct WsResponder {
    r: ResponderCtx,
    mode: WsMode,
    stop: CancellationToken,
    task: Option<JoinHandle<Stats>>,
    finish_tx: Option<oneshot::Sender<Option<u64>>>,
}

impl ResponderRun for WsResponder {
    fn start(&mut self) -> BoxFuture<'_, Result<(), AppError>> {
        Box::pin(async { Ok(()) })
    }

    fn finish(mut self: Box<Self>, info: Value) -> BoxFuture<'static, Result<Value, AppError>> {
        Box::pin(async move {
            let info: Info = serde_json::from_value(info).unwrap_or_default();
            let Some(mut task) = self.task.take() else {
                return Err(AppError::internal("the ws responder task is gone"));
            };
            match self.mode {
                WsMode::Rr => self.stop.cancel(),
                WsMode::Stream => {
                    if let Some(tx) = self.finish_tx.take() {
                        let _ = tx.send(info.last_seq);
                    }
                }
            }
            let stats = match tokio::time::timeout(RESPONDER_FINISH_CAP, &mut task).await {
                Ok(Ok(s)) => s,
                Ok(Err(e)) => {
                    return Err(AppError::internal(format!(
                        "the ws responder task ended abnormally: {e}"
                    )))
                }
                Err(_) => {
                    self.stop.cancel();
                    task.abort();
                    return Err(AppError::new(
                        ErrorCode::Timeout,
                        format!(
                            "the receiver did not drain within {} s",
                            RESPONDER_FINISH_CAP.as_secs()
                        ),
                    ));
                }
            };
            self.stop.cancel();
            if self.mode == WsMode::Stream && stats.unique > 0 {
                self.r.set_resource_window(
                    stats.first_ns,
                    stats.last_ns,
                    Some(stats.received_bytes),
                );
            }
            // Nothing to release in rule 7's order: the subscription went with the task.
            self.r.teardown().run().await;
            serde_json::to_value(&stats)
                .map_err(|e| AppError::internal(format!("serialise the ws stats: {e}")))
        })
    }

    fn cancel(mut self: Box<Self>) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            self.stop.cancel();
            if let Some(t) = self.task.take() {
                t.abort();
            }
            self.r.teardown().run().await;
        })
    }
}

/// A canonical example output for the schema fixtures (rr, 64 B, 500 requests).
pub fn example_output() -> WorkloadOutput {
    let p = WsParams::default();
    let origin = 1_760_000_000_000_000_000u64;
    let samples: Vec<RttSample> = (0..u64::from(p.warmup_count + p.count))
        .map(|i| RttSample {
            send_ns: origin + 600_000_000 + i * 1_900_000,
            rtt_ms: Some(
                1.62 + ((i * 37) % 23) as f64 * 0.021 + if i % 97 == 0 { 1.9 } else { 0.0 },
            ),
            warmup: i < u64::from(p.warmup_count),
        })
        .collect();
    let counts = RrCounts {
        sent: samples.len() as u64,
        timeouts: 0,
        lag: 0,
        bad: 0,
        wire: wire_bytes(MSG_RR_REQ, &json!({ "id": 255, "data": payload_b64(64) })),
    };
    let stats = Stats {
        served: samples.len() as u64,
        ..Stats::default()
    };
    rr_output(&p, origin, &samples, &counts, &stats)
}

/// Stream, 64 B as fast as backpressure allows for 10 s: about 100k msg/s
/// with a little loss at the receiver's subscription.
fn example_stream() -> WorkloadOutput {
    let p = WsParams {
        mode: WsMode::Stream,
        ..WsParams::default()
    };
    let origin = 1_760_000_000_000_000_000u64;
    let data = payload_b64(p.payload_bytes as usize);
    let wire = wire_bytes(
        MSG_BYTES,
        &json!({ "encoding": "base64", "data": data, "seq": 500_000u64 }),
    );
    // 40 buckets of ~25k messages, the first one ramping up.
    let buckets: Vec<u64> = (0..40u64)
        .map(|b| match b {
            0 => 17_250,
            _ => 25_100 + (b * 7919) % 900,
        })
        .collect();
    let unique: u64 = buckets.iter().sum();
    let sent = unique + 9_412;
    let first_ns = origin + 531_284_310;
    let stats = Stats {
        unique,
        dup: 0,
        reordered: 0,
        max_seq: Some(sent - 1),
        lag_dropped: 1_104,
        received_bytes: unique * u64::from(p.payload_bytes),
        first_ns,
        last_ns: first_ns + 39 * BUCKET_NS + 241_000_000,
        buckets,
        saw_last_seq: true,
        ..Stats::default()
    };
    let before = crate::types::ClockOffset {
        offset_ns: 1_284_310,
        delay_ns: 610_000,
        uncertainty_ns: 305_000,
        at_ns: origin + 420_000_000,
        samples: 16,
    };
    let after = crate::types::ClockOffset {
        at_ns: origin + 11_900_000_000,
        offset_ns: 1_285_100,
        ..before
    };
    let inputs = StreamInputs {
        p,
        origin_ns: origin,
        map: Some(ClockMap::new(before, Some(after))),
        sent,
        send_elapsed: Duration::from_millis(10_003),
        wire,
        notes: Vec::new(),
    };
    stream_output(&inputs, &stats).unwrap_or_default()
}

/// One example per ws mode for the schema fixtures.
pub fn examples() -> Vec<super::ModeExample> {
    vec![
        super::ModeExample {
            mode: "ws-rr",
            params: WorkloadParams::Ws(WsParams::default()),
            output: example_output(),
            clock: false,
        },
        super::ModeExample {
            mode: "ws-stream",
            params: WorkloadParams::Ws(WsParams {
                mode: WsMode::Stream,
                ..WsParams::default()
            }),
            output: example_stream(),
            clock: true,
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    const MS: u64 = 1_000_000;

    #[test]
    fn stream_counter_counts_unique_bytes_and_buckets() {
        let mut c = StreamCounter::new(1_000, 4);
        c.record(Some(0), Some(4), 1_000 * MS);
        c.record(Some(2), Some(4), 1_100 * MS);
        c.record(Some(1), Some(4), 1_300 * MS); // reordered, next bucket
        c.record(Some(2), Some(4), 1_310 * MS); // duplicate: not counted again
        c.record(None, Some(4), 1_320 * MS); // no seq: bad
        c.record(Some(3), None, 1_330 * MS); // undecodable: bad
        c.record(Some(4), Some(3), 1_340 * MS); // wrong size: counted, also bad
        let st = c.stats();
        assert_eq!((st.unique, st.dup, st.reordered), (4, 1, 1));
        assert_eq!(st.bad_payload, 3);
        assert_eq!(st.received_bytes, 15);
        assert_eq!(st.buckets, vec![2, 2]);
        assert_eq!((st.first_ns, st.last_ns), (1_000 * MS, 1_340 * MS));
        assert_eq!(st.max_seq, Some(4));
        // Seqs 0..=4 seen except 3: one of five missing so far.
        assert_eq!(c.drop_pct_so_far(), Some(20.0));
        assert!(c.has(4) && !c.has(3));
    }

    #[test]
    fn drain_rule() {
        assert!(drained(true, Duration::ZERO, 0));
        assert!(!drained(true, Duration::ZERO, 3), "queue must be empty");
        assert!(!drained(false, Duration::from_millis(1999), 0));
        assert!(drained(false, STREAM_IDLE, 0));
    }

    #[test]
    fn pacing_and_drain_allowance() {
        // 1000 msg/s: one due at once, 501 after half a second.
        assert_eq!(due(Duration::ZERO, 1000), 1);
        assert_eq!(due(Duration::from_millis(500), 1000), 501);
        // 256 queued × 1000 B at 100 kB/s → 10 s + 2.56 s.
        let d = drain_allowance(256, 1000, 100_000.0);
        assert!((d.as_secs_f64() - 12.56).abs() < 1e-9, "{d:?}");
        assert_eq!(drain_allowance(0, 1000, 0.0), STREAM_DRAIN_BASE);
        assert_eq!(
            drain_allowance(u64::MAX / 1_000_000, 1000, 1.0),
            STREAM_DRAIN_CAP
        );
    }

    #[test]
    fn wire_size_is_the_json_envelope() {
        let b64 = payload_b64(64);
        assert_eq!(b64.len(), 88, "base64 of 64 B");
        let payload = json!({ "encoding": "base64", "data": b64, "seq": 12345 });
        let n = wire_bytes(MSG_BYTES, &payload);
        let env = Envelope::new(WS_NS, MSG_BYTES, payload.clone()).with_timestamp();
        assert_eq!(n, serde_json::to_vec(&env).unwrap().len() as u64);
        assert!(n > 88 + 40, "{n}");
        // The payload decodes the way NamespacedMessage::payload_bytes does.
        let msg = NamespacedMessage {
            from: "p".into(),
            namespace: WS_NS.into(),
            msg_type: MSG_BYTES.into(),
            payload,
            timestamp: None,
        };
        assert_eq!(msg.payload_bytes().unwrap().len(), 64);
    }

    fn stream_inputs(sent: u64) -> StreamInputs {
        StreamInputs {
            p: WsParams {
                mode: WsMode::Stream,
                payload_bytes: 1000,
                ..WsParams::default()
            },
            origin_ns: 0,
            map: None,
            sent,
            send_elapsed: Duration::from_secs(10),
            wire: 1400,
            notes: Vec::new(),
        }
    }

    #[test]
    fn stream_drop_accounting() {
        // 1000 sent, 990 unique over 2 s, 4 lost to subscriber lag.
        let stats = Stats {
            unique: 990,
            lag_dropped: 4,
            received_bytes: 990_000,
            first_ns: 1_000 * MS,
            last_ns: 3_000 * MS,
            buckets: vec![124; 8],
            saw_last_seq: true,
            ..Stats::default()
        };
        let out = stream_output(&stream_inputs(1000), &stats).unwrap();
        let Results::Stream(r) = serde_json::from_value(out.results.clone()).unwrap() else {
            panic!("stream results expected");
        };
        assert_eq!(r.unique + r.missing, r.sent);
        assert_eq!((r.missing, r.lag_dropped, r.router_dropped), (10, 4, 6));
        assert_eq!(r.drop_pct, Some(1.0));
        assert!((r.msgs_per_sec - 495.0).abs() < 1e-9);
        assert!((r.goodput_mbps - 3.96).abs() < 1e-9);
        assert!((r.wire_mbps - 495.0 * 1400.0 * 8.0 / 1e6).abs() < 1e-9);
        assert!(r.received_bytes <= r.sent_bytes);
        let keys: Vec<&str> = out.summary.iter().map(|s| s.key.as_str()).collect();
        assert_eq!(keys, ["msgs_per_sec", "goodput_mbps", "drop_pct"]);
        assert_eq!(out.notes[0], NOTE_LOSSLESS);
        assert!(out.notes.iter().any(|n| n.contains("router")));
        // 8 full buckets of 124 messages: 496 msg/s, plotted at bucket ends.
        let s = &out.series[0];
        assert_eq!(s.key, "msgs_per_sec");
        assert_eq!(s.points[0], (1_250.0, 496.0));
        assert_eq!(s.points.len(), 8);
        // Too few messages fail.
        let none = Stats::default();
        assert!(stream_output(&stream_inputs(10), &none).is_err());
    }

    #[test]
    fn rr_output_discards_warmup_and_counts_rate() {
        let p = WsParams {
            count: 100,
            warmup_count: 10,
            ..WsParams::default()
        };
        // 110 requests sent 1 ms apart, each answered after 2 ms; #60 lost.
        let samples: Vec<RttSample> = (0..110u64)
            .map(|i| RttSample {
                send_ns: i * MS,
                rtt_ms: (i != 60).then_some(2.0),
                warmup: i < 10,
            })
            .collect();
        let c = RrCounts {
            sent: 110,
            timeouts: 1,
            lag: 0,
            bad: 0,
            wire: 160,
        };
        let out = rr_output(&p, 0, &samples, &c, &Stats::default());
        let Results::Rr(r) = serde_json::from_value(out.results.clone()).unwrap() else {
            panic!("rr results expected");
        };
        assert_eq!((r.completed, r.timeouts), (99, 1));
        assert_eq!((r.rtt_ms.samples, r.rtt_ms.lost), (100, 1));
        // First measured send at 10 ms, last answer at 109 + 2 ms: 99 in 101 ms.
        assert_eq!(out.window_ns, Some((10 * MS, 111 * MS)));
        assert!((r.msgs_per_sec.unwrap() - 99.0 / 0.101).abs() < 1e-6);
        assert_eq!(out.series[0].warmup_ms, Some(9.0));
        let keys: Vec<&str> = out.summary.iter().map(|s| s.key.as_str()).collect();
        assert_eq!(keys, ["rtt_p50_ms", "rtt_p99_ms", "msgs_per_sec"]);
        assert_eq!(out.results["mode"], "rr");
        assert!(out.notes.iter().any(|n| n.contains("not answered")));
        // Under 100 measured requests a p99 is undefined: the item is left out.
        let small = WsParams { count: 50, ..p };
        let out = rr_output(&small, 0, &samples[..60], &c, &Stats::default());
        let keys: Vec<&str> = out.summary.iter().map(|s| s.key.as_str()).collect();
        assert_eq!(keys, ["rtt_p50_ms", "msgs_per_sec"]);
    }

    #[tokio::test(start_paused = true)]
    async fn drain_task_finishes_on_last_seq_or_idle() {
        // The drain decision only, driven through a real broadcast channel.
        let (tx, mut rx) = tokio::sync::broadcast::channel::<u64>(4);
        for i in 0..3 {
            tx.send(i).unwrap();
        }
        assert_eq!(rx.len(), 3);
        let mut c = StreamCounter::new(100, 0);
        while let Ok(seq) = rx.try_recv() {
            c.record(Some(seq), Some(0), seq * MS);
        }
        assert!(drained(c.has(2), Duration::ZERO, rx.len()));
        // Lag: four more than the channel holds.
        for i in 3..9 {
            tx.send(i).unwrap();
        }
        assert!(matches!(
            rx.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Lagged(2))
        ));
    }

    #[test]
    fn example_is_complete() {
        let out = example_output();
        assert!(out.summary.iter().all(|s| s.value.is_some()));
        let Results::Rr(r) = serde_json::from_value(out.results).unwrap() else {
            panic!("rr example expected");
        };
        assert_eq!(r.completed, 500);
        assert!(r.wire_bytes_per_msg > 88);
    }
}
