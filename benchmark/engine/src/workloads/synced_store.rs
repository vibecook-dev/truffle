//! `synced-store` (RFC 027 §8.2 synced-store; §8.5: last-write-wins state
//! sync, not a CRDT).
//!
//! - **Setup:** the responder creates the store `bench-<run_tag_hex>` in
//!   `prepare` and never calls `set()` on its own.
//! - **Readiness:** after `prepare.ack` the initiator creates the same store
//!   and calls `set({seq: 0})` every 500 ms, at most 20 times, until a
//!   `PeerUpdated` from the responder's device carries `{echo: 0}`. Only
//!   seq ≥ 1 is timed.
//! - **Pingpong:** sample i: A `set({seq: i, pad})`; B, on seeing seq i,
//!   `set({echo: i, pad})`. `rtt_i` runs from A's `set()` to A's
//!   `PeerUpdated` carrying echo i; `convergence_i = rtt_i / 2`, labelled
//!   "RTT/2 estimate". Each sample has 5 s; a timeout counts as lost. The
//!   first `warmup_count` samples are discarded.
//! - **Burst:** A calls `set()` `count` times back to back; B echoes the
//!   highest seq it has observed whenever that changes. `final_echo_ms` runs
//!   from A's last `set()` to A seeing the echo of `count`;
//!   `burst_write_ms` is the time A spent writing; `coalescing_pct = (1 −
//!   distinct versions B observed / count) · 100`.
//! - `ws_peers` (WebSocket-connected peers) is recorded at the start and the
//!   end on both sides, because `set()` broadcasts to every one of them.
//! - **Identity (rule 1):** each side accepts `PeerUpdated` only from the
//!   other's device id: the session's published id for the peer's
//!   tailscale id (`node.peer_device_id`), which is what the store itself
//!   verified the slice against. The responder falls back to
//!   `Initiator.device_id` until the session publishes one.
//! - **Teardown:** both stores `stop()` (rule 7 step 6).
//!
//! Summary keys: pingpong `convergence_p50_ms`, `convergence_p99_ms`; burst
//! `final_echo_ms`, `coalescing_pct`, `burst_write_ms`. Series: pingpong
//! `convergence_ms`. Live: pingpong `convergence_ms`, `convergence_p50_ms`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use truffle_core::{StoreEvent, SyncedStore};

use crate::clock::now_ns;
use crate::error::{AppError, ErrorCode};
use crate::hub::BenchNode;
use crate::params::{StoreMode, StoreParams, WorkloadParams};
use crate::types::{Better, Distribution, Phase, SummaryItem};
use crate::workloads::common::{
    dur_ms, measured, p99_defined, raw_ms, rtt_series, sample_window, LiveRtt, RttSample,
};
use crate::workloads::{ResponderCtx, ResponderRun, RunCtx, WorkloadOutput};

/// Whether this build offers `synced-store` (drives `PeerHello.workloads`).
pub const IMPLEMENTED: bool = true;

/// The Progress sample keys a run with these params can set: the
/// initiator's own and the responder's `tick`s, which the hub merges in.
/// [`live_keys`](super::live_keys) adds the path sampler's `loaded_rtt_ms`
/// for time-based runs; `live-keys.json` in the schema fixtures is built
/// from it.
pub fn live_keys(p: &StoreParams) -> Vec<&'static str> {
    match p.mode {
        StoreMode::Pingpong => vec!["convergence_ms", "convergence_p50_ms"],
        // a burst is over in well under a second: nothing live
        StoreMode::Burst => Vec::new(),
    }
}

/// How convergence is labelled (§8.2 synced-store).
pub const LABEL_ESTIMATE: &str = "RTT/2 estimate";

/// A pingpong sample unanswered this long counts as lost.
const SAMPLE_TIMEOUT: Duration = Duration::from_secs(5);
/// Readiness writes are this far apart …
const READY_INTERVAL: Duration = Duration::from_millis(500);
/// … and there are at most this many.
const READY_ATTEMPTS: u32 = 20;
/// How long A waits for the echo of the last burst write.
const BURST_ECHO_TIMEOUT: Duration = Duration::from_secs(30);
/// Drain allowance of `finish` (the stores hold nothing in flight by then).
const FINISH_DRAIN: Duration = Duration::from_secs(5);

// ---------------------------------------------------------------------------
// Control blocks and results
// ---------------------------------------------------------------------------

/// What both stores hold: A writes `seq`, B writes `echo`; `pad` is
/// `payload_bytes` of padding.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
struct Doc {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    seq: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    echo: Option<u64>,
    #[serde(default)]
    pad: String,
}

/// `prepare.client`: nothing. The initiator's device id is not taken from
/// the wire: the responder binds echoes to the id the session verified.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "SyncedStoreClient")]
pub struct Client {}

/// `prepare.ack.setup`: the store the responder created.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "SyncedStoreSetup")]
pub struct Setup {
    /// `bench-<run_tag_hex>`.
    pub store_id: String,
    /// The responder's store device id (for diagnostics; echoes are bound
    /// to the session's published id).
    pub device_id: String,
    /// WebSocket-connected peers on the responder at prepare.
    pub ws_peers: u32,
}

/// `finish.info`: what the initiator wrote.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "SyncedStoreInfo")]
pub struct Info {
    /// `set()` calls, readiness included.
    pub writes: u64,
    /// Readiness writes (`seq: 0`).
    pub readiness_writes: u32,
    /// Highest seq written.
    pub last_seq: u64,
}

/// `finish.ack.stats`: what the responder saw and did.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "SyncedStoreStats")]
pub struct Stats {
    /// Echo `set()` calls.
    pub echoes: u64,
    /// `PeerUpdated` from the initiator carrying seq ≥ 1: the distinct
    /// versions observed (the store applies only newer versions).
    pub versions_observed: u64,
    /// Highest seq observed.
    pub highest_seq: Option<u64>,
    /// Σ `Lagged(n)` on the responder's store subscription.
    pub lagged: u64,
    /// `PeerUpdated` from other devices, ignored.
    pub foreign_ignored: u64,
    /// WebSocket-connected peers on the responder at prepare.
    pub ws_peers_start: u32,
    /// … and at finish.
    pub ws_peers_end: u32,
}

/// `results` of a pingpong run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "SyncedStorePingpongResults")]
pub struct PingpongResults {
    /// Measured samples.
    pub count: u32,
    /// Discarded samples before them.
    pub warmup_count: u32,
    /// Padding per write.
    pub payload_bytes: u32,
    /// `rtt / 2` of the measured samples ("RTT/2 estimate").
    pub convergence_ms: Distribution,
    /// Round trips of the measured samples.
    pub rtt_ms: Distribution,
    /// Raw measured convergence values in order, ms (thinned to at most 5000).
    pub samples_ms: Vec<f64>,
    /// Measured samples echoed in time.
    pub completed: u32,
    /// Measured samples not echoed within 5 s.
    pub timeouts: u32,
    /// First readiness write → the echo of seq 0, ms.
    pub ready_ms: f64,
    /// Readiness writes needed.
    pub readiness_writes: u32,
    /// WebSocket-connected peers here at the start.
    pub ws_peers_start: u32,
    /// … and at the end.
    pub ws_peers_end: u32,
    /// The same on the peer.
    pub peer_ws_peers_start: u32,
    /// … and at its end.
    pub peer_ws_peers_end: u32,
    /// Echoes the peer wrote (readiness included).
    pub echoes: u64,
    /// Σ `Lagged(n)` on this side's store subscription.
    pub lagged: u64,
    /// The same on the peer.
    pub peer_lagged: u64,
    /// The store.
    pub store_id: String,
}

/// `results` of a burst run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "SyncedStoreBurstResults")]
pub struct BurstResults {
    /// Writes in the burst.
    pub count: u32,
    /// Padding per write.
    pub payload_bytes: u32,
    /// Time spent calling `set()` `count` times, ms.
    pub burst_write_ms: f64,
    /// Last `set()` → seeing the echo of `count`, ms (`None` when it
    /// never came).
    pub final_echo_ms: Option<f64>,
    /// `(1 − distinct_versions_seen / count) · 100`.
    pub coalescing_pct: f64,
    /// Distinct versions (seq ≥ 1) the peer observed.
    pub distinct_versions_seen: u64,
    /// Echoes (seq ≥ 1) this side saw arrive.
    pub echoes_seen: u64,
    /// Echoes the peer wrote (readiness included).
    pub echoes: u64,
    /// First readiness write → the echo of seq 0, ms.
    pub ready_ms: f64,
    /// Readiness writes needed.
    pub readiness_writes: u32,
    /// WebSocket-connected peers here at the start.
    pub ws_peers_start: u32,
    /// … and at the end.
    pub ws_peers_end: u32,
    /// The same on the peer.
    pub peer_ws_peers_start: u32,
    /// … and at its end.
    pub peer_ws_peers_end: u32,
    /// Σ `Lagged(n)` on this side's store subscription.
    pub lagged: u64,
    /// The same on the peer.
    pub peer_lagged: u64,
    /// The store.
    pub store_id: String,
}

/// `RunReport.results` of a `synced-store` run, tagged by `mode`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "mode", rename_all = "lowercase")]
#[schemars(rename = "SyncedStoreResults")]
pub enum Results {
    /// One write, one echo, repeat.
    Pingpong(Box<PingpongResults>),
    /// `count` writes back to back.
    Burst(BurstResults),
}

/// `(1 − distinct / count) · 100`, never below 0.
pub fn coalescing_pct(distinct: u64, count: u32) -> f64 {
    if count == 0 {
        return 0.0;
    }
    ((1.0 - distinct as f64 / f64::from(count)) * 100.0).max(0.0)
}

/// The responder's echo decision: echo seq 0 every time (readiness may
/// repeat), and any seq above the highest seen so far.
fn echo_for(seq: u64, highest: Option<u64>) -> Option<u64> {
    (seq == 0 || highest.is_none_or(|h| seq > h)).then_some(seq)
}

/// WebSocket-connected peers right now.
async fn ws_peers(node: &BenchNode) -> u32 {
    node.peers().await.iter().filter(|p| p.ws_connected).count() as u32
}

/// Whether `device_id` is the peer's: the cached id, or (on a miss) the
/// session's published id for `tailscale_id`, which then becomes the cache.
async fn from_peer(
    node: &BenchNode,
    tailscale_id: &str,
    cached: &mut Option<String>,
    device_id: &str,
) -> bool {
    if cached.as_deref() == Some(device_id) {
        return true;
    }
    match node.peer_device_id(tailscale_id).await {
        Some(published) => {
            let ok = published == device_id;
            *cached = Some(published);
            ok
        }
        None => false,
    }
}

// ---------------------------------------------------------------------------
// Initiator
// ---------------------------------------------------------------------------

/// An echo seen by the initiator's store, stamped on arrival.
#[derive(Debug, Clone, Copy)]
struct Echo {
    echo: u64,
    at: Instant,
}

/// Forward the responder's echoes from the store's event stream (decoupled,
/// so a burst of local `LocalChanged` events cannot lag them away).
async fn forward_echoes(
    node: Arc<BenchNode>,
    peer: String,
    mut events: broadcast::Receiver<StoreEvent<Doc>>,
    tx: mpsc::UnboundedSender<Echo>,
    lagged: Arc<AtomicU64>,
) {
    let mut dev: Option<String> = None;
    loop {
        match events.recv().await {
            Ok(StoreEvent::PeerUpdated {
                device_id, data, ..
            }) => {
                let at = Instant::now();
                if !from_peer(&node, &peer, &mut dev, &device_id).await {
                    continue;
                }
                if let Some(echo) = data.echo {
                    if tx.send(Echo { echo, at }).is_err() {
                        break;
                    }
                }
            }
            Ok(_) => {}
            Err(RecvError::Lagged(n)) => {
                lagged.fetch_add(n, Ordering::Relaxed);
            }
            Err(RecvError::Closed) => break,
        }
    }
}

/// The params as validated and used by the responder (after `prepare`).
fn used_params(ctx: &RunCtx<'_>, requested: &StoreParams) -> StoreParams {
    match ctx.params() {
        WorkloadParams::SyncedStore(p) => p.clone(),
        _ => requested.clone(),
    }
}

/// Initiator side (§6 "Initiator flow" step 7).
pub async fn run(ctx: &mut RunCtx<'_>, p: &StoreParams) -> Result<WorkloadOutput, AppError> {
    let ws_start = ws_peers(&ctx.node).await;
    let setup: Setup = ctx.prepare(&Client {}).await?;
    let p = used_params(ctx, p);
    let store_id = format!("bench-{}", ctx.run_tag_hex()?);
    if setup.store_id != store_id {
        return Err(AppError::new(
            ErrorCode::ProtocolMismatch,
            format!(
                "the peer created store {} for run tag {store_id}",
                setup.store_id
            ),
        ));
    }
    let store = ctx.node.synced_store::<Doc>(&store_id);
    let result = drive(ctx, &p, &store, &setup, ws_start).await;
    // Rule 7 step 6, on every path (a dropped run future aborts the sync
    // task through the store's Drop instead).
    store.stop().await;
    result
}

/// What the initiator measured, for [`pingpong_output`] / [`burst_output`].
#[derive(Debug, Clone, Default)]
struct Measured {
    ready_ms: f64,
    readiness_writes: u32,
    ws_peers_start: u32,
    ws_peers_end: u32,
    lagged: u64,
    store_id: String,
    samples: Vec<RttSample>,
    burst_write_ms: f64,
    final_echo_ms: Option<f64>,
    echoes_seen: u64,
    window_ns: Option<(u64, u64)>,
}

async fn drive(
    ctx: &mut RunCtx<'_>,
    p: &StoreParams,
    store: &Arc<SyncedStore<Doc>>,
    setup: &Setup,
    ws_start: u32,
) -> Result<WorkloadOutput, AppError> {
    let lagged = Arc::new(AtomicU64::new(0));
    let (tx, mut echoes) = mpsc::unbounded_channel();
    let forwarder = tokio::spawn(forward_echoes(
        ctx.node.clone(),
        ctx.peer_id().to_string(),
        store.subscribe(),
        tx,
        lagged.clone(),
    ));
    let res = drive_inner(ctx, p, store, setup, ws_start, &mut echoes, &lagged).await;
    forwarder.abort();
    res
}

/// Wait until `pred` matches an echo or `deadline` passes.
async fn wait_echo(
    echoes: &mut mpsc::UnboundedReceiver<Echo>,
    deadline: Instant,
    mut pred: impl FnMut(&Echo) -> bool,
) -> Result<Option<Echo>, AppError> {
    loop {
        match tokio::time::timeout_at(deadline, echoes.recv()).await {
            Ok(Some(e)) if pred(&e) => return Ok(Some(e)),
            Ok(Some(_)) => {}
            Ok(None) => {
                return Err(AppError::internal(
                    "the store's event stream ended during the run",
                ))
            }
            Err(_) => return Ok(None),
        }
    }
}

async fn drive_inner(
    ctx: &mut RunCtx<'_>,
    p: &StoreParams,
    store: &Arc<SyncedStore<Doc>>,
    setup: &Setup,
    ws_start: u32,
    echoes: &mut mpsc::UnboundedReceiver<Echo>,
    lagged: &AtomicU64,
) -> Result<WorkloadOutput, AppError> {
    let pad = "x".repeat(p.payload_bytes as usize);
    let doc = |seq: u64| Doc {
        seq: Some(seq),
        echo: None,
        pad: pad.clone(),
    };
    let mut m = Measured {
        ws_peers_start: ws_start,
        store_id: setup.store_id.clone(),
        ..Measured::default()
    };

    // Readiness: seq 0 every 500 ms until the peer's echo 0 comes back.
    ctx.phase_with(Phase::Preparing, "waiting for the peer's store");
    let t0 = Instant::now();
    let mut ready = false;
    for _ in 0..READY_ATTEMPTS {
        ctx.check_cancelled()?;
        store.set(doc(0)).await;
        m.readiness_writes += 1;
        if wait_echo(echoes, Instant::now() + READY_INTERVAL, |e| e.echo == 0)
            .await?
            .is_some()
        {
            ready = true;
            break;
        }
    }
    m.ready_ms = dur_ms(t0.elapsed());
    if !ready {
        return Err(AppError::timeout(format!(
            "the peer's store did not echo the readiness write within {} s ({} writes)",
            (READY_INTERVAL * READY_ATTEMPTS).as_secs(),
            READY_ATTEMPTS
        )));
    }
    ctx.start().await?;

    let live = ctx.live();
    let mut last_seq = 0;
    let mut failure = None;
    match p.mode {
        StoreMode::Pingpong => {
            let total = u64::from(p.warmup_count) + u64::from(p.count);
            let mut live_conv = LiveRtt::new();
            ctx.phase(if p.warmup_count > 0 {
                Phase::Warmup
            } else {
                Phase::Running
            });
            for i in 1..=total {
                if ctx.is_cancelled() {
                    return Err(AppError::cancelled());
                }
                let warmup = i <= u64::from(p.warmup_count);
                if i == u64::from(p.warmup_count) + 1 && p.warmup_count > 0 {
                    ctx.phase(Phase::Running);
                }
                let send_ns = now_ns();
                let t = Instant::now();
                store.set(doc(i)).await;
                last_seq = i;
                let got = match wait_echo(echoes, t + SAMPLE_TIMEOUT, |e| e.echo == i).await {
                    Ok(g) => g,
                    Err(e) => {
                        failure = Some(e);
                        break;
                    }
                };
                let rtt = got.map(|e| dur_ms(e.at.saturating_duration_since(t)));
                if let Some(r) = rtt {
                    live_conv.push(r / 2.0, warmup);
                }
                live_conv.emit_due(&live, "convergence_ms", "convergence_p50_ms");
                m.samples.push(RttSample {
                    send_ns,
                    rtt_ms: rtt,
                    warmup,
                });
                ctx.fraction(i as f32 / total as f32);
            }
            live_conv.emit(&live, "convergence_ms", "convergence_p50_ms");
            m.window_ns = sample_window(&m.samples);
        }
        StoreMode::Burst => {
            ctx.phase(Phase::Running);
            let start_ns = now_ns();
            let t0 = Instant::now();
            let mut last_set = t0;
            for i in 1..=u64::from(p.count) {
                last_set = Instant::now();
                store.set(doc(i)).await;
            }
            last_seq = u64::from(p.count);
            m.burst_write_ms = dur_ms(t0.elapsed());
            ctx.phase(Phase::Draining);
            let mut seen = 0;
            let got = wait_echo(echoes, Instant::now() + BURST_ECHO_TIMEOUT, |e| {
                if e.echo >= 1 {
                    seen += 1;
                }
                e.echo == last_seq
            })
            .await;
            m.echoes_seen = seen;
            match got {
                Ok(Some(e)) => {
                    m.final_echo_ms = Some(dur_ms(e.at.saturating_duration_since(last_set)));
                    m.window_ns = Some((start_ns, now_ns()));
                }
                Ok(None) => {
                    failure = Some(AppError::timeout(format!(
                        "the peer never echoed write {last_seq} within {} s",
                        BURST_ECHO_TIMEOUT.as_secs()
                    )))
                }
                Err(e) => failure = Some(e),
            }
        }
    }
    m.ws_peers_end = ws_peers(&ctx.node).await;
    m.lagged = lagged.load(Ordering::Relaxed);
    let info = Info {
        writes: u64::from(m.readiness_writes) + last_seq,
        readiness_writes: m.readiness_writes,
        last_seq,
    };
    let finished: Result<Stats, AppError> = ctx.finish(&info, FINISH_DRAIN).await;
    let origin_ns = ctx.origin_ns();
    let build = |stats: &Stats| match p.mode {
        StoreMode::Pingpong => pingpong_output(p, origin_ns, &m, stats),
        StoreMode::Burst => burst_output(p, &m, stats),
    };
    if let Some(e) = failure {
        let stats = finished.unwrap_or_default();
        let out = build(&stats);
        ctx.set_partial(out);
        return Err(e);
    }
    let stats = finished?;
    let out = build(&stats);
    if p.mode == StoreMode::Pingpong && out.summary.iter().all(|s| s.value.is_none()) {
        ctx.set_partial(out);
        return Err(AppError::timeout(format!(
            "none of the {} measured writes was echoed within {} s",
            p.count,
            SAMPLE_TIMEOUT.as_secs()
        )));
    }
    Ok(out)
}

/// Notes shared by both modes.
fn notes(m: &Measured, stats: &Stats) -> Vec<String> {
    let mut notes = Vec::new();
    let here = m.ws_peers_start.max(m.ws_peers_end);
    let there = stats.ws_peers_start.max(stats.ws_peers_end);
    if here > 1 || there > 1 {
        notes.push(format!(
            "set() broadcasts to every WebSocket-connected peer: up to {here} here and {there} on the peer were connected"
        ));
    }
    if m.lagged + stats.lagged > 0 {
        notes.push(format!(
            "{} store events were dropped by a lagging subscriber",
            m.lagged + stats.lagged
        ));
    }
    notes
}

/// Summary, results and series of a pingpong run.
fn pingpong_output(p: &StoreParams, origin_ns: u64, m: &Measured, stats: &Stats) -> WorkloadOutput {
    let conv = Distribution::from_samples(&measured(&m.samples, 0.5));
    let rtt = Distribution::from_samples(&measured(&m.samples, 1.0));
    let completed = m
        .samples
        .iter()
        .filter(|s| !s.warmup && s.rtt_ms.is_some())
        .count() as u32;
    let timeouts = m
        .samples
        .iter()
        .filter(|s| !s.warmup && s.rtt_ms.is_none())
        .count() as u32;
    let mut summary = vec![SummaryItem::new(
        "convergence_p50_ms",
        format!("Convergence p50 ({LABEL_ESTIMATE})"),
        conv.p50,
        "ms",
        Better::Lower,
    )];
    if p99_defined(p.count) {
        summary.push(SummaryItem::new(
            "convergence_p99_ms",
            format!("Convergence p99 ({LABEL_ESTIMATE})"),
            conv.p99,
            "ms",
            Better::Lower,
        ));
    }
    let mut notes = notes(m, stats);
    if timeouts > 0 {
        notes.push(format!(
            "{timeouts} writes were not echoed within {} s and count as lost",
            SAMPLE_TIMEOUT.as_secs()
        ));
    }
    let results = Results::Pingpong(Box::new(PingpongResults {
        count: p.count,
        warmup_count: p.warmup_count,
        payload_bytes: p.payload_bytes,
        convergence_ms: conv,
        rtt_ms: rtt,
        samples_ms: raw_ms(&m.samples, 0.5),
        completed,
        timeouts,
        ready_ms: m.ready_ms,
        readiness_writes: m.readiness_writes,
        ws_peers_start: m.ws_peers_start,
        ws_peers_end: m.ws_peers_end,
        peer_ws_peers_start: stats.ws_peers_start,
        peer_ws_peers_end: stats.ws_peers_end,
        echoes: stats.echoes,
        lagged: m.lagged,
        peer_lagged: stats.lagged,
        store_id: m.store_id.clone(),
    }));
    WorkloadOutput {
        summary,
        results: serde_json::to_value(&results).unwrap_or_default(),
        series: vec![rtt_series(
            &m.samples,
            origin_ns,
            "convergence_ms",
            &format!("Convergence ({LABEL_ESTIMATE})"),
            0.5,
        )],
        notes,
        window_ns: m.window_ns,
        goodput_bytes: None,
    }
}

/// Summary and results of a burst run.
fn burst_output(p: &StoreParams, m: &Measured, stats: &Stats) -> WorkloadOutput {
    let coalescing = coalescing_pct(stats.versions_observed, p.count);
    let summary = vec![
        SummaryItem::new(
            "final_echo_ms",
            "Last write echoed",
            m.final_echo_ms,
            "ms",
            Better::Lower,
        ),
        SummaryItem::new(
            "coalescing_pct",
            "Coalesced",
            Some(coalescing),
            "%",
            Better::Lower,
        ),
        SummaryItem::new(
            "burst_write_ms",
            "Writing",
            Some(m.burst_write_ms),
            "ms",
            Better::Lower,
        ),
    ];
    let results = Results::Burst(BurstResults {
        count: p.count,
        payload_bytes: p.payload_bytes,
        burst_write_ms: m.burst_write_ms,
        final_echo_ms: m.final_echo_ms,
        coalescing_pct: coalescing,
        distinct_versions_seen: stats.versions_observed,
        echoes_seen: m.echoes_seen,
        echoes: stats.echoes,
        ready_ms: m.ready_ms,
        readiness_writes: m.readiness_writes,
        ws_peers_start: m.ws_peers_start,
        ws_peers_end: m.ws_peers_end,
        peer_ws_peers_start: stats.ws_peers_start,
        peer_ws_peers_end: stats.ws_peers_end,
        lagged: m.lagged,
        peer_lagged: stats.lagged,
        store_id: m.store_id.clone(),
    });
    WorkloadOutput {
        summary,
        results: serde_json::to_value(&results).unwrap_or_default(),
        series: Vec::new(),
        notes: notes(m, stats),
        window_ns: m.window_ns,
        goodput_bytes: None,
    }
}

// ---------------------------------------------------------------------------
// Responder
// ---------------------------------------------------------------------------

/// Responder side: create `bench-<run_tag_hex>` (no `set()`), and echo the
/// initiator's writes from a task subscribed before `prepare.ack`.
pub async fn prepare(
    r: &ResponderCtx,
    p: &StoreParams,
    _client: Client,
) -> Result<(Setup, Box<dyn ResponderRun>), AppError> {
    let store_id = format!("bench-{}", r.run_tag_hex());
    let store = r.node.synced_store::<Doc>(&store_id);
    let events = store.subscribe();
    let ws_start = ws_peers(&r.node).await;
    let stop = r.cancel_token().child_token();
    let task = tokio::spawn(echo_task(
        r.clone(),
        p.payload_bytes as usize,
        store.clone(),
        events,
        stop.clone(),
    ));
    Ok((
        Setup {
            store_id,
            device_id: store.device_id().to_string(),
            ws_peers: ws_start,
        },
        Box::new(StoreResponder {
            r: r.clone(),
            store: Some(store),
            task: Some(task),
            stop,
            ws_start,
        }),
    ))
}

/// Echo `{echo: seq}` for every new highest seq from the initiator (and for
/// every readiness write), until stopped.
async fn echo_task(
    r: ResponderCtx,
    pad_len: usize,
    store: Arc<SyncedStore<Doc>>,
    mut events: broadcast::Receiver<StoreEvent<Doc>>,
    stop: CancellationToken,
) -> Stats {
    let mut st = Stats::default();
    let mut dev = r.initiator.device_id.clone();
    let pad = "x".repeat(pad_len);
    let mut highest: Option<u64> = None;
    loop {
        let ev = tokio::select! {
            _ = stop.cancelled() => break,
            e = events.recv() => e,
        };
        match ev {
            Ok(StoreEvent::PeerUpdated {
                device_id, data, ..
            }) => {
                if !from_peer(&r.node, &r.initiator.tailscale_id, &mut dev, &device_id).await {
                    st.foreign_ignored += 1;
                    continue;
                }
                let Some(seq) = data.seq else {
                    continue;
                };
                if seq >= 1 {
                    st.versions_observed += 1;
                }
                if let Some(echo) = echo_for(seq, highest) {
                    highest = Some(highest.map_or(echo, |h| h.max(echo)));
                    store
                        .set(Doc {
                            seq: None,
                            echo: Some(echo),
                            pad: pad.clone(),
                        })
                        .await;
                    st.echoes += 1;
                }
            }
            Ok(_) => {}
            Err(RecvError::Lagged(n)) => st.lagged += n,
            Err(RecvError::Closed) => break,
        }
    }
    st.highest_seq = highest;
    st
}

/// The responder side of one synced-store run.
struct StoreResponder {
    r: ResponderCtx,
    store: Option<Arc<SyncedStore<Doc>>>,
    task: Option<JoinHandle<Stats>>,
    stop: CancellationToken,
    ws_start: u32,
}

impl StoreResponder {
    /// Stop the echo task and the store (rule 7 step 6).
    async fn teardown(&mut self) -> Option<Stats> {
        self.stop.cancel();
        let stats = match self.task.take() {
            Some(t) => t.await.ok(),
            None => None,
        };
        let mut td = self.r.teardown();
        if let Some(store) = self.store.take() {
            td.store(store);
        }
        td.run().await;
        stats
    }
}

impl ResponderRun for StoreResponder {
    fn start(&mut self) -> BoxFuture<'_, Result<(), AppError>> {
        Box::pin(async { Ok(()) })
    }

    fn finish(mut self: Box<Self>, _info: Value) -> BoxFuture<'static, Result<Value, AppError>> {
        Box::pin(async move {
            let ws_end = ws_peers(&self.r.node).await;
            let mut stats = self
                .teardown()
                .await
                .ok_or_else(|| AppError::internal("the synced-store echo task ended abnormally"))?;
            stats.ws_peers_start = self.ws_start;
            stats.ws_peers_end = ws_end;
            serde_json::to_value(&stats)
                .map_err(|e| AppError::internal(format!("serialise the synced-store stats: {e}")))
        })
    }

    fn cancel(mut self: Box<Self>) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            self.teardown().await;
        })
    }
}

/// A canonical example output for the schema fixtures (pingpong, 100 writes
/// after 5 warmup writes, LAN).
pub fn example_output() -> WorkloadOutput {
    let p = StoreParams::default();
    let origin = 1_760_000_000_000_000_000u64;
    let total = u64::from(p.warmup_count + p.count);
    let samples: Vec<RttSample> = (1..=total)
        .map(|i| RttSample {
            send_ns: origin + 900_000_000 + i * 3_400_000,
            rtt_ms: Some(3.0 + ((i * 29) % 17) as f64 * 0.04 + if i % 41 == 0 { 2.6 } else { 0.0 }),
            warmup: i <= u64::from(p.warmup_count),
        })
        .collect();
    let m = Measured {
        ready_ms: 412.0,
        readiness_writes: 1,
        ws_peers_start: 1,
        ws_peers_end: 1,
        lagged: 0,
        store_id: "bench-5c1d2e3f4a5b6c7d".into(),
        window_ns: sample_window(&samples),
        samples,
        ..Measured::default()
    };
    let stats = Stats {
        echoes: total + 1,
        versions_observed: total,
        highest_seq: Some(total),
        ws_peers_start: 1,
        ws_peers_end: 1,
        ..Stats::default()
    };
    pingpong_output(&p, origin, &m, &stats)
}

/// A burst of `count` (100) writes; the responder saw 31 distinct versions.
fn example_burst() -> WorkloadOutput {
    let p = StoreParams {
        mode: StoreMode::Burst,
        ..StoreParams::default()
    };
    let origin = 1_760_000_000_000_000_000u64;
    let m = Measured {
        ready_ms: 386.0,
        readiness_writes: 1,
        ws_peers_start: 1,
        ws_peers_end: 1,
        lagged: 0,
        store_id: "bench-5c1d2e3f4a5b6c7d".into(),
        burst_write_ms: 0.31,
        final_echo_ms: Some(1.42),
        echoes_seen: 9,
        window_ns: Some((origin + 900_000_000, origin + 901_730_000)),
        ..Measured::default()
    };
    let stats = Stats {
        echoes: 11,
        versions_observed: 31,
        highest_seq: Some(u64::from(p.count)),
        ws_peers_start: 1,
        ws_peers_end: 1,
        ..Stats::default()
    };
    burst_output(&p, &m, &stats)
}

/// One example per synced-store mode for the schema fixtures.
pub fn examples() -> Vec<super::ModeExample> {
    vec![
        super::ModeExample {
            mode: "synced-store-pingpong",
            params: WorkloadParams::SyncedStore(StoreParams::default()),
            output: example_output(),
            clock: false,
        },
        super::ModeExample {
            mode: "synced-store-burst",
            params: WorkloadParams::SyncedStore(StoreParams {
                mode: StoreMode::Burst,
                ..StoreParams::default()
            }),
            output: example_burst(),
            clock: false,
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(i: u64, rtt: Option<f64>, warmup: bool) -> RttSample {
        RttSample {
            send_ns: i * 10_000_000,
            rtt_ms: rtt,
            warmup,
        }
    }

    #[test]
    fn doc_wire_shape() {
        let d = Doc {
            seq: Some(3),
            echo: None,
            pad: "xx".into(),
        };
        assert_eq!(
            serde_json::to_value(&d).unwrap(),
            serde_json::json!({"seq": 3, "pad": "xx"})
        );
        let e: Doc = serde_json::from_value(serde_json::json!({"echo": 0})).unwrap();
        assert_eq!((e.seq, e.echo, e.pad.as_str()), (None, Some(0), ""));
    }

    #[test]
    fn echo_rule() {
        assert_eq!(echo_for(0, None), Some(0));
        assert_eq!(
            echo_for(0, Some(0)),
            Some(0),
            "readiness repeats are echoed"
        );
        assert_eq!(echo_for(1, Some(0)), Some(1));
        assert_eq!(echo_for(5, Some(7)), None, "never echo below the highest");
        assert_eq!(echo_for(7, Some(7)), None);
        assert_eq!(echo_for(9, Some(7)), Some(9));
    }

    #[test]
    fn coalescing() {
        assert_eq!(coalescing_pct(100, 100), 0.0);
        assert_eq!(coalescing_pct(25, 100), 75.0);
        assert_eq!(coalescing_pct(120, 100), 0.0, "never negative");
        assert_eq!(coalescing_pct(0, 0), 0.0);
    }

    #[test]
    fn pingpong_halves_rtt_and_counts_timeouts() {
        let p = StoreParams {
            count: 100,
            warmup_count: 5,
            ..StoreParams::default()
        };
        let mut samples: Vec<RttSample> = (1..=5).map(|i| sample(i, Some(9.0), true)).collect();
        samples.extend((6..=105).map(|i| sample(i, (i != 50).then_some(4.0), false)));
        let m = Measured {
            samples,
            ws_peers_start: 2,
            ws_peers_end: 1,
            ..Measured::default()
        };
        let out = pingpong_output(&p, 0, &m, &Stats::default());
        let keys: Vec<&str> = out.summary.iter().map(|s| s.key.as_str()).collect();
        assert_eq!(keys, ["convergence_p50_ms", "convergence_p99_ms"]);
        assert_eq!(out.summary[0].value, Some(2.0));
        assert!(out.summary[0].label.contains(LABEL_ESTIMATE));
        let Results::Pingpong(r) = serde_json::from_value(out.results.clone()).unwrap() else {
            panic!("pingpong results expected");
        };
        assert_eq!((r.completed, r.timeouts), (99, 1));
        assert_eq!((r.convergence_ms.samples, r.convergence_ms.lost), (100, 1));
        assert_eq!(r.rtt_ms.p50, Some(4.0));
        assert_eq!(r.samples_ms.len(), 99);
        assert!(r.samples_ms.iter().all(|v| *v == 2.0));
        assert_eq!(out.series[0].key, "convergence_ms");
        assert_eq!(out.series[0].warmup_ms, Some(50.0));
        assert_eq!(out.series[0].points[0], (10.0, 4.5));
        assert!(out
            .notes
            .iter()
            .any(|n| n.contains("every WebSocket-connected peer")));
        assert!(out.notes.iter().any(|n| n.contains("not echoed")));
        assert_eq!(out.results["mode"], "pingpong");
        // Under 100 measured writes a p99 is undefined: the item is left out.
        let small = StoreParams { count: 10, ..p };
        let m10 = Measured {
            samples: m.samples[..15].to_vec(),
            ..Measured::default()
        };
        let out = pingpong_output(&small, 0, &m10, &Stats::default());
        let keys: Vec<&str> = out.summary.iter().map(|s| s.key.as_str()).collect();
        assert_eq!(keys, ["convergence_p50_ms"]);
        assert!(out.summary[0].value.is_some());
    }

    #[test]
    fn burst_summary() {
        let p = StoreParams {
            mode: StoreMode::Burst,
            count: 1000,
            ..StoreParams::default()
        };
        let m = Measured {
            burst_write_ms: 14.5,
            final_echo_ms: Some(6.25),
            echoes_seen: 40,
            ws_peers_start: 1,
            ws_peers_end: 1,
            ..Measured::default()
        };
        let stats = Stats {
            versions_observed: 280,
            ws_peers_start: 1,
            ws_peers_end: 1,
            ..Stats::default()
        };
        let out = burst_output(&p, &m, &stats);
        let v = |k: &str| out.summary.iter().find(|s| s.key == k).unwrap().value;
        assert_eq!(v("final_echo_ms"), Some(6.25));
        assert_eq!(v("coalescing_pct"), Some(72.0));
        assert_eq!(v("burst_write_ms"), Some(14.5));
        assert_eq!(out.summary[0].key, "final_echo_ms");
        let Results::Burst(r) = serde_json::from_value(out.results).unwrap() else {
            panic!("burst results expected");
        };
        assert_eq!(r.distinct_versions_seen, 280);
        assert!(out.notes.is_empty());
    }

    #[test]
    fn example_is_complete() {
        let out = example_output();
        assert!(out.summary.iter().all(|s| s.value.is_some()));
        let Results::Pingpong(r) = serde_json::from_value(out.results).unwrap() else {
            panic!("pingpong example expected");
        };
        assert_eq!(r.completed, 100);
        assert!(r.convergence_ms.p99.is_some());
    }
}
