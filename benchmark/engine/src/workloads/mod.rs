//! The workload framework (RFC 027 §8 intro) and its dispatch.
//!
//! Each workload module defines its `Client`, `Setup`, `Info`, `Stats` and
//! `Results` structs plus:
//!
//! ```text
//! pub const IMPLEMENTED: bool;                       // drives PeerHello.workloads
//! pub async fn run(ctx: &mut RunCtx<'_>, p: &XParams) -> Result<WorkloadOutput, AppError>;   // initiator
//! pub async fn prepare(r: &ResponderCtx, p: &XParams, client: Client)
//!     -> Result<(Setup, Box<dyn ResponderRun>), AppError>;                                 // responder
//! pub fn example_output() -> WorkloadOutput;          // schema fixtures
//! pub fn live_keys(p: &XParams) -> Vec<&'static str>; // Progress sample keys
//! ```
//!
//! The hub owns everything around a run (peer resolution, hello, path
//! settle, usermetrics, resources, report assembly); a workload only drives
//! its data plane through [`RunCtx`] (initiator) and [`ResponderCtx`]
//! (responder). Shared data-plane helpers live in [`common`].
//!
//! # The initiator's contract
//!
//! - Call [`RunCtx::prepare`] (if the workload needs the responder), then
//!   [`RunCtx::clock`] if responder times must be mapped, [`RunCtx::start`],
//!   the data phase, and [`RunCtx::finish`]. Any error after `prepare` makes
//!   the hub send `cancel`, so the responder always tears down.
//! - Announce phases with [`RunCtx::phase`] (`warmup`, `running`,
//!   `draining`, …). Live values go in with [`RunCtx::sample`] / a
//!   [`LiveSamples`] handle; during `warmup`/`running` the hub emits them at
//!   ≤ 4 Hz. Receiver-side values from the responder arrive as `tick`s and
//!   are merged automatically. Sender-side keys must be distinct
//!   (`sent_mbps`), never `goodput_mbps`.
//! - Time-based workloads get the 2 Hz path sampler automatically from
//!   [`RunCtx::start`] (or the first `warmup`/`running` phase) until
//!   [`RunCtx::finish`]; it publishes its latest RTT live as
//!   `loaded_rtt_ms` ([`common::KEY_LOADED_RTT`]).
//! - Summary items for `cpu_ms_per_mb` and `loaded_rtt_p50_ms` may be left
//!   `None` (see [`common::cpu_per_mb_item`], [`common::loaded_rtt_item`]):
//!   the hub fills them once resources and the path timeline are known.
//! - Honour [`RunCtx::cancel_token`]; the hub also drops the run future on
//!   cancel and at [`WorkloadParams::deadline`].
//!
//! # The responder's contract
//!
//! `prepare` validates nothing the hub already validated, binds what it
//! needs (listeners on port 0, sockets, stores) and returns its `Setup` plus
//! a [`ResponderRun`]. `start` must return promptly (spawn the work).
//! `finish` and `cancel` release everything in the order of §6 rule 7 (see
//! [`Teardown`]); the hub removes the run's
//! `in-…` directory and frees the slot.

pub mod common;
pub mod egress;
pub mod file_transfer;
pub mod http;
pub mod path;
pub mod quic;
pub mod synced_store;
pub mod tcp;
pub mod udp;
pub mod video;
pub mod ws;

use std::collections::BTreeMap;
use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use futures::future::BoxFuture;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use truffle_core::transport::RawIncoming;
use truffle_core::Peer;

use crate::clock::{now_ns, ClockMap};
use crate::control::{
    self, in_dir, msg_type, new_request_id, out_dir, ControlRequest, FinishReq, FtDoneMsg,
    Initiator, PrepareReq, StartReq, Teardown, TickMsg, FINISH_TIMEOUT, PREPARE_TIMEOUT,
    START_TIMEOUT, TICK_INTERVAL,
};
use crate::error::{AppError, ErrorCode};
use crate::hub::{BenchNode, OfferRouter, ProgressSink};
use crate::params::{Workload, WorkloadParams};
use crate::pathwatch::PathSampler;
use crate::types::{
    ClockOffset, Distribution, PathBytes, PathSnapshot, PathTimelinePoint, Phase, Progress,
    ResourceStats, Sample, Series, SummaryItem,
};
use crate::CONTROL_NS;

/// Pause before re-sending a `prepare` the responder refused with
/// `peer-not-found` (its Layer 3 did not list this node yet).
const PREPARE_PEER_RETRY: Duration = Duration::from_secs(1);

/// Whether `prepare` asks again after `failure`, with `time_left` for one
/// more try. Only for the RESPONDER's `peer-not-found` refusal: its Layer 3
/// does not list this node yet (it can miss a peer for a while after a fast
/// restart) and it kept nothing, so asking again is what the refusal says
/// to do. This node failing to reach a peer its own registry dropped
/// carries the same code, and ends the run at once.
fn prepare_retries(failure: &control::RequestFailure, time_left: bool) -> bool {
    time_left
        && matches!(
            failure,
            control::RequestFailure::Refused(e) if e.code == ErrorCode::PeerNotFound
        )
}

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

/// Whether this build can run (and respond to) a workload.
pub fn implemented(w: Workload) -> bool {
    #[cfg(test)]
    if test_double::get(w).is_some() {
        return true;
    }
    match w {
        Workload::Path => path::IMPLEMENTED,
        Workload::Tcp => tcp::IMPLEMENTED,
        Workload::Udp => udp::IMPLEMENTED,
        Workload::Quic => quic::IMPLEMENTED,
        Workload::Ws => ws::IMPLEMENTED,
        Workload::FileTransfer => file_transfer::IMPLEMENTED,
        Workload::SyncedStore => synced_store::IMPLEMENTED,
        Workload::Http => http::IMPLEMENTED,
        Workload::Video => video::IMPLEMENTED,
        Workload::Egress => egress::IMPLEMENTED,
    }
}

/// The implemented workloads' names, in rail order (`PeerHello.workloads`).
pub fn implemented_names() -> Vec<String> {
    Workload::ALL
        .iter()
        .filter(|w| implemented(**w))
        .map(|w| w.as_str().to_string())
        .collect()
}

/// A canonical example of a workload's output, for the schema fixtures.
pub fn example_output(w: Workload) -> WorkloadOutput {
    match w {
        Workload::Path => path::example_output(),
        Workload::Tcp => tcp::example_output(),
        Workload::Udp => udp::example_output(),
        Workload::Quic => quic::example_output(),
        Workload::Ws => ws::example_output(),
        Workload::FileTransfer => file_transfer::example_output(),
        Workload::SyncedStore => synced_store::example_output(),
        Workload::Http => http::example_output(),
        Workload::Video => video::example_output(),
        Workload::Egress => egress::example_output(),
    }
}

/// A canonical example of one workload mode's output, for the schema
/// fixtures (`report-<mode>.json`): built by the module's own output code
/// from realistic synthetic measurements, so it carries every summary key,
/// series and `results` field the module emits for that mode.
#[derive(Debug, Clone, PartialEq)]
pub struct ModeExample {
    /// `tcp-throughput-upload`, `udp-echo`, …
    pub mode: &'static str,
    /// The params the output belongs to.
    pub params: WorkloadParams,
    /// What the module returns for them.
    pub output: WorkloadOutput,
    /// The mode estimates the clock before and after (`network.clock`).
    pub clock: bool,
}

/// Every mode's example, in rail order (§7 fixtures).
pub fn mode_examples() -> Vec<ModeExample> {
    [
        path::examples(),
        tcp::examples(),
        udp::examples(),
        quic::examples(),
        ws::examples(),
        file_transfer::examples(),
        synced_store::examples(),
        http::examples(),
        video::examples(),
        egress::examples(),
    ]
    .concat()
}

/// Every Progress sample key a run with these params can set, sorted: the
/// module's own [`live_keys`](tcp::live_keys) (initiator samples and the
/// responder's merged `tick`s), plus the path sampler's `loaded_rtt_ms`
/// ([`common::KEY_LOADED_RTT`]) when the run is time-based. The schema
/// fixture `live-keys.json` lists them per mode for the app, whose live KPIs
/// and charts may only read these keys.
pub fn live_keys(params: &WorkloadParams) -> Vec<&'static str> {
    let mut keys = match params {
        WorkloadParams::Path(p) => path::live_keys(p),
        WorkloadParams::Tcp(p) => tcp::live_keys(p),
        WorkloadParams::Udp(p) => udp::live_keys(p),
        WorkloadParams::Quic(p) => quic::live_keys(p),
        WorkloadParams::Ws(p) => ws::live_keys(p),
        WorkloadParams::FileTransfer(p) => file_transfer::live_keys(p),
        WorkloadParams::SyncedStore(p) => synced_store::live_keys(p),
        WorkloadParams::Http(p) => http::live_keys(p),
        WorkloadParams::Video(p) => video::live_keys(p),
        WorkloadParams::Egress(p) => egress::live_keys(p),
    };
    if params.time_based() {
        keys.push(common::KEY_LOADED_RTT);
    }
    keys.sort_unstable();
    keys.dedup();
    keys
}

/// Run the initiator side of `params` (the hub calls this).
pub async fn run_initiator(
    ctx: &mut RunCtx<'_>,
    params: &WorkloadParams,
) -> Result<WorkloadOutput, AppError> {
    #[cfg(test)]
    if let Some((run, _)) = test_double::get(params.workload()) {
        return run(ctx).await;
    }
    match params {
        WorkloadParams::Path(p) => path::run(ctx, p).await,
        WorkloadParams::Tcp(p) => tcp::run(ctx, p).await,
        WorkloadParams::Udp(p) => udp::run(ctx, p).await,
        WorkloadParams::Quic(p) => quic::run(ctx, p).await,
        WorkloadParams::Ws(p) => ws::run(ctx, p).await,
        WorkloadParams::FileTransfer(p) => file_transfer::run(ctx, p).await,
        WorkloadParams::SyncedStore(p) => synced_store::run(ctx, p).await,
        WorkloadParams::Http(p) => http::run(ctx, p).await,
        WorkloadParams::Video(p) => video::run(ctx, p).await,
        WorkloadParams::Egress(p) => egress::run(ctx, p).await,
    }
}

fn client_of<C: DeserializeOwned>(w: Workload, client: Value) -> Result<C, AppError> {
    let client = if client.is_null() {
        Value::Object(Default::default())
    } else {
        client
    };
    serde_json::from_value(client)
        .map_err(|e| AppError::invalid_params(format!("invalid {w} client block: {e}")))
}

fn setup_of<S: Serialize>(w: Workload, setup: &S) -> Result<Value, AppError> {
    serde_json::to_value(setup)
        .map_err(|e| AppError::internal(format!("serialise the {w} setup block: {e}")))
}

macro_rules! prepare_with {
    ($module:ident, $r:expr, $p:expr, $client:expr, $w:expr) => {{
        let client = client_of($w, $client)?;
        let (setup, run) = $module::prepare($r, $p, client).await?;
        Ok((setup_of($w, &setup)?, run))
    }};
}

/// Run the responder side's `prepare` for `params` (the responder calls this
/// after admission, validation and taking the slot). Returns the `Setup`
/// block as JSON plus the run.
pub async fn prepare_responder(
    r: &ResponderCtx,
    params: &WorkloadParams,
    client: Value,
) -> Result<(Value, Box<dyn ResponderRun>), AppError> {
    let w = params.workload();
    #[cfg(test)]
    if let Some((_, prepare)) = test_double::get(w) {
        return prepare(r.clone(), client).await;
    }
    match params {
        WorkloadParams::Path(_) => Err(AppError::invalid_params(
            "path runs only on the initiator; there is nothing to prepare",
        )),
        WorkloadParams::Tcp(p) => prepare_with!(tcp, r, p, client, w),
        WorkloadParams::Udp(p) => prepare_with!(udp, r, p, client, w),
        WorkloadParams::Quic(p) => prepare_with!(quic, r, p, client, w),
        WorkloadParams::Ws(p) => prepare_with!(ws, r, p, client, w),
        WorkloadParams::FileTransfer(p) => prepare_with!(file_transfer, r, p, client, w),
        WorkloadParams::SyncedStore(p) => prepare_with!(synced_store, r, p, client, w),
        WorkloadParams::Http(p) => prepare_with!(http, r, p, client, w),
        WorkloadParams::Video(p) => prepare_with!(video, r, p, client, w),
        WorkloadParams::Egress(p) => prepare_with!(egress, r, p, client, w),
    }
}

/// A test double that replaces one workload's initiator and responder, so
/// the hub and the control protocol can be exercised end to end before (and
/// independently of) the real workloads.
#[cfg(test)]
pub(crate) mod test_double {
    use super::*;

    /// The initiator side of a double.
    pub(crate) type RunFn =
        for<'a, 'b> fn(&'a mut RunCtx<'b>) -> BoxFuture<'a, Result<WorkloadOutput, AppError>>;
    /// What a double's responder side returns.
    pub(crate) type PrepareResult = Result<(Value, Box<dyn ResponderRun>), AppError>;
    /// The responder side of a double.
    pub(crate) type PrepareFn = fn(ResponderCtx, Value) -> BoxFuture<'static, PrepareResult>;

    static DOUBLE: Mutex<Option<(Workload, RunFn, PrepareFn)>> = Mutex::new(None);

    /// Held by every live pair test in this crate's lib tests that installs
    /// a double or runs the workload a double replaces, so `cargo test`'s
    /// parallel threads never mix the two.
    pub(crate) static LIVE_PAIR_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    /// Replace `w` until [`clear`].
    pub(crate) fn install(w: Workload, run: RunFn, prepare: PrepareFn) {
        *DOUBLE.lock().unwrap_or_else(PoisonError::into_inner) = Some((w, run, prepare));
    }

    /// Remove the double.
    pub(crate) fn clear() {
        *DOUBLE.lock().unwrap_or_else(PoisonError::into_inner) = None;
    }

    pub(crate) fn get(w: Workload) -> Option<(RunFn, PrepareFn)> {
        DOUBLE
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .filter(|d| d.0 == w)
            .map(|d| (d.1, d.2))
    }
}

/// `unsupported-workload` for a workload this build does not implement yet.
pub fn unsupported(w: Workload) -> AppError {
    AppError::new(
        ErrorCode::UnsupportedWorkload,
        format!("the {w} workload is not implemented in this build yet"),
    )
}

// ---------------------------------------------------------------------------
// Outputs
// ---------------------------------------------------------------------------

/// What a workload's initiator returns; the hub turns it into a report.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct WorkloadOutput {
    /// Headline numbers (`RunReport.summary`).
    pub summary: Vec<SummaryItem>,
    /// Workload-specific results (`RunReport.results`), the module's `Results`.
    pub results: Value,
    /// Charts, in the initiator's run timebase (`t_ms` since [`RunCtx::origin_ns`]).
    pub series: Vec<Series>,
    /// Notes for people.
    pub notes: Vec<String>,
    /// Measurement window in local `now_ns()`, for resources; `None` = the
    /// whole sampled span.
    pub window_ns: Option<(u64, u64)>,
    /// Payload bytes delivered in that window, for `cpu_ms_per_mb`.
    pub goodput_bytes: Option<u64>,
}

/// The responder side of one run.
///
/// `start` must return promptly; spawn long work. `finish` returns the
/// workload's `Stats` block (sent in `finish.ack.stats`); both `finish` and
/// `cancel` release everything the run holds, in the order of §6 rule 7.
pub trait ResponderRun: Send {
    /// The initiator sent `start` (e.g. begin sending in download mode).
    fn start(&mut self) -> BoxFuture<'_, Result<(), AppError>>;
    /// The initiator sent `finish` with its `Info` block: drain, tear down,
    /// return `Stats`.
    fn finish(self: Box<Self>, info: Value) -> BoxFuture<'static, Result<Value, AppError>>;
    /// Cancel, watchdog or hub stop: tear down without results.
    fn cancel(self: Box<Self>) -> BoxFuture<'static, ()>;
}

// ---------------------------------------------------------------------------
// Progress plumbing
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct ProgressState {
    phase: Option<Phase>,
    samples: BTreeMap<String, f64>,
    fraction: Option<f32>,
    explicit_fraction: bool,
    data_start: Option<Instant>,
    message: Option<String>,
    last: Option<Progress>,
}

/// One run's live progress: phase, samples, the sinks that receive it and
/// the last event (replayed on attach).
pub(crate) struct RunProgress {
    run_id: String,
    suite_id: Option<String>,
    origin: Instant,
    data_total: Option<Duration>,
    state: Mutex<ProgressState>,
    sinks: Mutex<Vec<ProgressSink>>,
}

impl RunProgress {
    pub(crate) fn new(
        run_id: &str,
        suite_id: Option<String>,
        data_total: Option<Duration>,
        sink: ProgressSink,
    ) -> Arc<Self> {
        Arc::new(Self {
            run_id: run_id.to_string(),
            suite_id,
            origin: Instant::now(),
            data_total,
            state: Mutex::new(ProgressState::default()),
            sinks: Mutex::new(vec![sink]),
        })
    }

    fn state(&self) -> std::sync::MutexGuard<'_, ProgressState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn t_ms(&self) -> u64 {
        self.origin.elapsed().as_millis() as u64
    }

    pub(crate) fn origin(&self) -> Instant {
        self.origin
    }

    fn build(&self, st: &ProgressState) -> Progress {
        let fraction = if st.explicit_fraction {
            st.fraction
        } else {
            match (st.data_start, self.data_total) {
                (Some(start), Some(total)) if !total.is_zero() => {
                    Some((start.elapsed().as_secs_f32() / total.as_secs_f32()).min(1.0))
                }
                _ => st.fraction,
            }
        };
        let phase = st.phase.unwrap_or(Phase::Preparing);
        Progress {
            run_id: self.run_id.clone(),
            suite_id: self.suite_id.clone(),
            t_ms: self.t_ms(),
            phase,
            fraction: if phase == Phase::Done {
                Some(1.0)
            } else {
                fraction
            },
            samples: st
                .samples
                .iter()
                .map(|(k, v)| Sample::new(k.clone(), *v))
                .collect(),
            message: st.message.clone(),
        }
    }

    fn deliver(&self, p: Progress) {
        let sinks: Vec<ProgressSink> = self
            .sinks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        for sink in sinks {
            let p = p.clone();
            // A sink that panics must not take the run down with it.
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| sink(p)));
        }
    }

    /// Emit the current state now.
    pub(crate) fn emit(&self) {
        let p = {
            let mut st = self.state();
            let p = self.build(&st);
            st.last = Some(p.clone());
            p
        };
        self.deliver(p);
    }

    /// Change phase (emits on change, or whenever a message is given).
    pub(crate) fn set_phase(&self, phase: Phase, message: Option<String>) {
        let changed = {
            let mut st = self.state();
            let changed = st.phase != Some(phase) || message.is_some();
            if st.phase != Some(phase) {
                st.message = None;
            }
            st.phase = Some(phase);
            if matches!(phase, Phase::Warmup | Phase::Running) && st.data_start.is_none() {
                st.data_start = Some(Instant::now());
            }
            if message.is_some() {
                st.message = message;
            }
            changed
        };
        if changed {
            self.emit();
        }
    }

    pub(crate) fn phase(&self) -> Option<Phase> {
        self.state().phase
    }

    pub(crate) fn set_sample(&self, key: &str, value: f64) {
        if value.is_finite() {
            self.state().samples.insert(key.to_string(), value);
        }
    }

    pub(crate) fn set_samples(&self, values: &BTreeMap<String, f64>) {
        let mut st = self.state();
        for (k, v) in values {
            if v.is_finite() {
                st.samples.insert(k.clone(), *v);
            }
        }
    }

    pub(crate) fn set_fraction(&self, f: f32) {
        let mut st = self.state();
        st.fraction = Some(f.clamp(0.0, 1.0));
        st.explicit_fraction = true;
    }

    pub(crate) fn set_message(&self, message: Option<String>) {
        self.state().message = message;
        self.emit();
    }

    /// Whether the run is in a data phase (`warmup`/`running`), where
    /// progress is emitted on the 250 ms clock instead of on change.
    pub(crate) fn in_data_phase(&self) -> bool {
        matches!(self.state().phase, Some(Phase::Warmup | Phase::Running))
    }

    pub(crate) fn last(&self) -> Option<Progress> {
        self.state().last.clone()
    }

    /// Add a sink and replay the last event to it.
    pub(crate) fn attach(&self, sink: ProgressSink) {
        if let Some(last) = self.last() {
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| sink(last)));
        }
        self.sinks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(sink);
    }

    /// The ≤ 4 Hz emitter for data phases, aligned to 250 ms since the run's
    /// origin. Abort the handle when the run ends.
    pub(crate) fn spawn_ticker(self: &Arc<Self>) -> JoinHandle<()> {
        let me = self.clone();
        tokio::spawn(async move {
            let period = Duration::from_millis(250);
            let mut tick = tokio::time::interval_at(me.origin + period, period);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;
                if me.in_data_phase() {
                    me.emit();
                }
            }
        })
    }
}

/// A cloneable handle for setting live values from spawned tasks.
#[derive(Clone)]
pub struct LiveSamples {
    progress: Arc<RunProgress>,
}

impl std::fmt::Debug for LiveSamples {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LiveSamples")
            .field("run_id", &self.progress.run_id)
            .finish()
    }
}

impl LiveSamples {
    /// Set a live value (shown at the next ≤ 4 Hz emission).
    pub fn set(&self, key: &str, value: f64) {
        self.progress.set_sample(key, value);
    }

    /// Set several live values.
    pub fn set_many<'k>(&self, values: impl IntoIterator<Item = (&'k str, f64)>) {
        for (k, v) in values {
            self.progress.set_sample(k, v);
        }
    }
}

// ---------------------------------------------------------------------------
// RunCtx (initiator)
// ---------------------------------------------------------------------------

/// Lifecycle timings the path workload reports (§8.2 path).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PeerTimings {
    /// Hub start → Running, ms.
    pub hub_start_ms: Option<u64>,
    /// Running → this peer first listed, ms.
    pub discovery_ms: Option<u64>,
    /// First `hello` to this peer while it was not ws_connected → `hello.ack`, ms.
    pub ws_connect_ms: Option<u64>,
}

/// What the responder said in `finish.ack` besides `stats`.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub(crate) struct PeerFinish {
    pub(crate) resources: Option<ResourceStats>,
    pub(crate) path_bytes: Option<PathBytes>,
    pub(crate) max_overlapping_runs: u32,
}

#[derive(Default)]
struct SamplerSlot {
    running: Option<PathSampler>,
    points: Vec<PathTimelinePoint>,
    loaded: Option<Distribution>,
    ran: bool,
}

/// `prepare.ack.params` must be the request, unchanged: the responder
/// validates what it receives and echoes it (§6 rule 2), and every initiator
/// sizes its work (files, buffers, workers) from these params. An echo that
/// differs could only come from a buggy or hostile peer.
fn check_echoed_params(
    requested: &WorkloadParams,
    echoed: Option<&WorkloadParams>,
) -> Result<(), AppError> {
    match echoed {
        Some(p) if p != requested => Err(AppError::new(
            ErrorCode::ProtocolMismatch,
            "prepare: the responder echoed params that differ from the request",
        )),
        _ => Ok(()),
    }
}

/// A `tick` with more keys than this is dropped (no workload sends more
/// than a handful; the keys reach every progress event).
const MAX_TICK_KEYS: usize = 32;
/// A `tick` with a key longer than this is dropped.
const MAX_TICK_KEY_LEN: usize = 64;
/// `ft.done` messages buffered for a run (file-transfer has ≤ 20 iterations).
const MAX_FT_DONE: usize = 64;
/// `ft.done` error text is cut to this many characters.
const MAX_FT_DONE_ERROR: usize = 1024;

/// Whether a responder's `tick` is small enough to merge into progress.
fn tick_is_sane(t: &TickMsg) -> bool {
    t.values.len() <= MAX_TICK_KEYS && t.values.keys().all(|k| k.len() <= MAX_TICK_KEY_LEN)
}

/// Everything the hub hands a new [`RunCtx`].
pub(crate) struct RunCtxInit<'a> {
    pub(crate) node: Arc<BenchNode>,
    pub(crate) peer: Peer,
    pub(crate) hello: crate::types::PeerHello,
    pub(crate) run_id: String,
    pub(crate) suite_id: Option<String>,
    pub(crate) params: WorkloadParams,
    pub(crate) progress: &'a Arc<RunProgress>,
    pub(crate) cancel: CancellationToken,
    pub(crate) deadline: Instant,
    pub(crate) data_dir: PathBuf,
    pub(crate) before: PathSnapshot,
    pub(crate) timings: PeerTimings,
    pub(crate) same_process: bool,
}

/// The initiator's view of one run.
///
/// Created by the hub after the peer is resolved, hello is checked and the
/// path settled; handed to the workload's `run`.
pub struct RunCtx<'a> {
    /// This hub's node.
    pub node: Arc<BenchNode>,
    /// The target peer. `peer.tailscale_id` is the only peer key (§5.4).
    pub peer: Peer,
    /// The peer's hello.
    pub hello: crate::types::PeerHello,
    /// This run's id.
    pub run_id: String,
    /// Suite id, when part of a suite.
    pub suite_id: Option<String>,
    params: WorkloadParams,
    progress: &'a Arc<RunProgress>,
    cancel: CancellationToken,
    deadline: Instant,
    data_dir: PathBuf,
    before: PathSnapshot,
    timings: PeerTimings,
    same_process: bool,
    origin_ns: u64,
    run_tag: Option<u64>,
    pub(crate) prepare_sent: bool,
    started: bool,
    pub(crate) finished: bool,
    pub(crate) peer_prepare_overlap: u32,
    pub(crate) peer_finish: Option<PeerFinish>,
    pub(crate) clock_before: Option<ClockOffset>,
    pub(crate) clock_after: Option<ClockOffset>,
    sampler: Mutex<SamplerSlot>,
    pub(crate) extra_timeline: Vec<PathTimelinePoint>,
    pub(crate) out_dir: Option<PathBuf>,
    pub(crate) partial: Option<WorkloadOutput>,
    control_retries: u32,
    ticks: mpsc::Receiver<TickMsg>,
    ft_done: mpsc::Receiver<FtDoneMsg>,
    ft_pending: Vec<FtDoneMsg>,
    inbox: JoinHandle<()>,
}

impl Drop for RunCtx<'_> {
    fn drop(&mut self) {
        self.inbox.abort();
    }
}

impl<'a> RunCtx<'a> {
    /// Build the context and start routing this run's `tick` / `ft.done`
    /// messages (subscribed before `prepare`, so none are missed).
    pub(crate) fn new(init: RunCtxInit<'a>) -> Self {
        let (tick_tx, ticks) = mpsc::channel::<TickMsg>(64);
        let (ft_tx, ft_done) = mpsc::channel::<FtDoneMsg>(MAX_FT_DONE);
        let mut rx = init.node.subscribe(CONTROL_NS);
        let peer_id = init.peer.tailscale_id.clone();
        let run_id = init.run_id.clone();
        let progress = init.progress.clone();
        let inbox = tokio::spawn(async move {
            use tokio::sync::broadcast::error::RecvError;
            loop {
                let msg = match rx.recv().await {
                    Ok(m) => m,
                    Err(RecvError::Lagged(_)) => continue,
                    Err(RecvError::Closed) => break,
                };
                if msg.from != peer_id
                    || msg.payload.get("run_id").and_then(Value::as_str) != Some(run_id.as_str())
                {
                    continue;
                }
                match msg.msg_type.as_str() {
                    msg_type::TICK => {
                        if let Ok(t) = serde_json::from_value::<TickMsg>(msg.payload) {
                            if !tick_is_sane(&t) {
                                tracing::debug!(keys = t.values.len(), "dropped an oversized tick");
                                continue;
                            }
                            progress.set_samples(&t.values);
                            let _ = tick_tx.try_send(t);
                        }
                    }
                    msg_type::FT_DONE => {
                        if let Ok(mut d) = serde_json::from_value::<FtDoneMsg>(msg.payload) {
                            if let Some(e) = d.error.as_mut() {
                                crate::types::truncate_chars(e, MAX_FT_DONE_ERROR);
                            }
                            let _ = ft_tx.try_send(d);
                        }
                    }
                    _ => {}
                }
            }
        });
        Self {
            node: init.node,
            peer: init.peer,
            hello: init.hello,
            run_id: init.run_id,
            suite_id: init.suite_id,
            params: init.params,
            progress: init.progress,
            cancel: init.cancel,
            deadline: init.deadline,
            data_dir: init.data_dir,
            before: init.before,
            timings: init.timings,
            same_process: init.same_process,
            origin_ns: crate::clock::instant_ns(init.progress.origin().into_std()),
            run_tag: None,
            prepare_sent: false,
            started: false,
            finished: false,
            peer_prepare_overlap: 0,
            peer_finish: None,
            clock_before: None,
            clock_after: None,
            sampler: Mutex::new(SamplerSlot::default()),
            extra_timeline: Vec::new(),
            out_dir: None,
            partial: None,
            control_retries: 0,
            ticks,
            ft_done,
            ft_pending: Vec::new(),
            inbox,
        }
    }

    // ── facts ──────────────────────────────────────────────────────────

    /// The params: as requested until `prepare`, then as validated and used
    /// by the responder.
    pub fn params(&self) -> &WorkloadParams {
        &self.params
    }

    /// The peer's tailscale id (the key for every send and check).
    pub fn peer_id(&self) -> &str {
        &self.peer.tailscale_id
    }

    /// The peer's tailnet IP.
    pub fn peer_ip(&self) -> IpAddr {
        self.peer.ip
    }

    /// The peer for data-plane identity checks on this side (e.g. UDP
    /// download: drop datagrams from any other IP and count them).
    pub fn peer_identity(&self) -> Initiator {
        Initiator::from_peer(&self.peer)
    }

    /// The settled path before the run (`network.before`).
    pub fn before(&self) -> &PathSnapshot {
        &self.before
    }

    /// The settled RTT, if the settle pings answered.
    pub fn before_rtt(&self) -> Option<Duration> {
        self.before
            .rtt_ms
            .filter(|r| r.is_finite() && *r >= 0.0)
            .map(|ms| Duration::from_secs_f64(ms / 1000.0))
    }

    /// `now_ns()` at the run's start: `t_ms` origin of series and progress.
    pub fn origin_ns(&self) -> u64 {
        self.origin_ns
    }

    /// Ms since the run started.
    pub fn t_ms(&self) -> u64 {
        self.progress.t_ms()
    }

    /// `t` (`now_ns()`, local) relative to the run origin, in ms.
    pub fn rel_ms(&self, t_ns: u64) -> f64 {
        (t_ns as f64 - self.origin_ns as f64) / 1e6
    }

    /// When the hub gives up on this run.
    pub fn deadline(&self) -> Instant {
        self.deadline
    }

    /// Lifecycle timings for `path`.
    pub fn timings(&self) -> PeerTimings {
        self.timings
    }

    /// Both hubs live in this process (`pair`).
    pub fn same_process(&self) -> bool {
        self.same_process
    }

    /// The run's cancellation token (cancel, hub stop).
    pub fn cancel_token(&self) -> CancellationToken {
        self.cancel.clone()
    }

    /// Whether the run was cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.cancel.is_cancelled()
    }

    /// Fail with `cancelled` if the run was cancelled.
    pub fn check_cancelled(&self) -> Result<(), AppError> {
        if self.is_cancelled() {
            Err(AppError::cancelled())
        } else {
            Ok(())
        }
    }

    // ── progress ───────────────────────────────────────────────────────

    /// Announce a phase (emitted at once). Entering `warmup`/`running`
    /// starts the path sampler for time-based workloads.
    pub fn phase(&self, phase: Phase) {
        if matches!(phase, Phase::Warmup | Phase::Running) {
            self.start_path_sampler();
        }
        self.progress.set_phase(phase, None);
    }

    /// Announce a phase with a one-line message.
    pub fn phase_with(&self, phase: Phase, message: impl Into<String>) {
        if matches!(phase, Phase::Warmup | Phase::Running) {
            self.start_path_sampler();
        }
        self.progress.set_phase(phase, Some(message.into()));
    }

    /// Replace the message line (emitted at once).
    pub fn message(&self, message: impl Into<String>) {
        self.progress.set_message(Some(message.into()));
    }

    /// Set the 0–1 fraction explicitly (otherwise time-based runs get
    /// elapsed ÷ (warmup + duration) from the first data phase).
    pub fn fraction(&self, f: f32) {
        self.progress.set_fraction(f);
    }

    /// Set a live value. In `warmup`/`running` it goes out with the next
    /// ≤ 4 Hz event; in other phases with the next phase change, or call
    /// [`emit`](Self::emit).
    pub fn sample(&self, key: &str, value: f64) {
        self.progress.set_sample(key, value);
    }

    /// Emit progress now (use sparingly outside data phases, e.g. once per
    /// file-transfer iteration).
    pub fn emit(&self) {
        self.progress.emit();
    }

    /// A handle for setting live values from spawned tasks.
    pub fn live(&self) -> LiveSamples {
        LiveSamples {
            progress: self.progress.clone(),
        }
    }

    // ── control ────────────────────────────────────────────────────────

    /// Send `req` and wait up to `timeout` for its reply. With control
    /// retries set, the same request (same id) is re-sent `retries` times,
    /// evenly within `timeout`: the responder answers a duplicate `start` or
    /// `finish` with the first one's outcome, so a lost request or reply is
    /// recovered without a second run of the handler, and the exchange never
    /// takes longer than `timeout` (which the run's deadline covers).
    async fn control<R: ControlRequest>(
        &self,
        req: &R,
        timeout: Duration,
    ) -> Result<control::Received<R::Ack>, AppError> {
        if self.control_retries == 0 {
            return control::request(&self.node, &self.peer.tailscale_id, req, timeout).await;
        }
        let every = timeout / (self.control_retries + 1);
        control::request_resending(&self.node, &self.peer.tailscale_id, req, timeout, every).await
    }

    /// Re-send `start`/`finish` (same id) up to `n` times within their
    /// timeout (ws runs: their traffic shares the WebSocket and core's
    /// router with control messages, which can be dropped under load).
    /// `clock` is never re-sent: a lost sample is simply one fewer sample.
    pub fn set_control_retries(&mut self, n: u32) {
        self.control_retries = n;
    }

    /// Whether `prepare` succeeded.
    pub fn is_prepared(&self) -> bool {
        self.run_tag.is_some()
    }

    /// The responder's run tag (after `prepare`).
    pub fn run_tag(&self) -> Result<u64, AppError> {
        self.run_tag
            .ok_or_else(|| AppError::internal("the run is not prepared yet (no run tag)"))
    }

    /// The run tag as 16 hex digits.
    pub fn run_tag_hex(&self) -> Result<String, AppError> {
        self.run_tag().map(crate::dataplane::run_tag_hex)
    }

    /// `prepare` with the workload's `Client` block; returns its `Setup`
    /// block. Afterwards [`run_tag`](Self::run_tag) is known. The
    /// responder's echo of the params must equal the request (the honest
    /// responder echoes them unchanged after validating them); anything else
    /// is a `protocol-mismatch`, so a peer can never resize this side's work.
    pub async fn prepare<C: Serialize, S: DeserializeOwned>(
        &mut self,
        client: &C,
    ) -> Result<S, AppError> {
        self.progress.set_phase(Phase::Preparing, None);
        let client = serde_json::to_value(client)
            .map_err(|e| AppError::internal(format!("serialise the client block: {e}")))?;
        // From here on the responder may hold state: the hub sends `cancel`
        // unless `finish` completes.
        self.prepare_sent = true;
        let deadline = Instant::now() + PREPARE_TIMEOUT;
        let got = loop {
            let req = PrepareReq {
                id: new_request_id(),
                run_id: self.run_id.clone(),
                params: self.params.clone(),
                client: client.clone(),
            };
            let left = deadline.saturating_duration_since(Instant::now());
            match control::request_or_refusal(&self.node, &self.peer.tailscale_id, &req, left).await
            {
                Err(failure)
                    if prepare_retries(
                        &failure,
                        Instant::now() + PREPARE_PEER_RETRY < deadline,
                    ) =>
                {
                    let e = AppError::from(failure);
                    tracing::debug!(error = %e, "prepare: the peer does not list us yet; retrying");
                    tokio::time::sleep(PREPARE_PEER_RETRY).await;
                }
                got => break got.map_err(|e| AppError::from(e).context("prepare"))?,
            }
        };
        check_echoed_params(&self.params, got.ack.params.as_ref())?;
        self.run_tag = Some(got.ack.run_tag);
        self.peer_prepare_overlap = got.ack.overlapping_runs;
        let setup = if got.ack.setup.is_null() {
            Value::Object(Default::default())
        } else {
            got.ack.setup
        };
        serde_json::from_value(setup).map_err(|e| {
            AppError::new(
                ErrorCode::ProtocolMismatch,
                format!("prepare: the responder's setup block did not parse: {e}"),
            )
        })
    }

    /// `start`: the data phase begins (the path sampler starts for
    /// time-based workloads).
    pub async fn start(&mut self) -> Result<(), AppError> {
        let req = StartReq {
            id: new_request_id(),
            run_id: self.run_id.clone(),
        };
        self.control(&req, START_TIMEOUT)
            .await
            .map_err(|e| e.context("start"))?;
        self.started = true;
        self.start_path_sampler();
        Ok(())
    }

    /// `finish` with the workload's `Info` block; returns the responder's
    /// `Stats` block. `drain` is added to the 30 s finish timeout. Stops
    /// the path sampler first.
    pub async fn finish<I: Serialize, S: DeserializeOwned>(
        &mut self,
        info: &I,
        drain: Duration,
    ) -> Result<S, AppError> {
        self.stop_path_sampler().await;
        let info = serde_json::to_value(info)
            .map_err(|e| AppError::internal(format!("serialise the info block: {e}")))?;
        // One request (one id) for every send: a re-sent finish is a
        // duplicate the responder answers with the first one's outcome.
        let req = FinishReq {
            id: new_request_id(),
            run_id: self.run_id.clone(),
            info,
        };
        let got = self
            .control(&req, FINISH_TIMEOUT + drain)
            .await
            .map_err(|e| e.context("finish"))?;
        self.finished = true;
        self.peer_finish = Some(PeerFinish {
            resources: got.ack.resources,
            path_bytes: got.ack.path_bytes,
            max_overlapping_runs: got.ack.max_overlapping_runs,
        });
        let stats = if got.ack.stats.is_null() {
            Value::Object(Default::default())
        } else {
            got.ack.stats
        };
        serde_json::from_value(stats).map_err(|e| {
            AppError::new(
                ErrorCode::ProtocolMismatch,
                format!("finish: the responder's stats block did not parse: {e}"),
            )
        })
    }

    /// Estimate the clock offset (16 samples, 10 ms apart). The first call
    /// is the `before` estimate, later calls the `after` one. Not allowed
    /// between `start` and `finish` (no clock traffic during the data
    /// phase). Announces the `clock` phase.
    pub async fn clock(&mut self) -> Result<ClockOffset, AppError> {
        if self.started && !self.finished {
            return Err(AppError::internal(
                "clock estimates are not allowed during the data phase",
            ));
        }
        self.progress.set_phase(Phase::Clock, None);
        let off = control::estimate_clock(&self.node, &self.peer.tailscale_id).await?;
        if self.clock_before.is_none() {
            self.clock_before = Some(off);
        } else {
            self.clock_after = Some(off);
        }
        Ok(off)
    }

    /// The peer→local time map from the estimates so far.
    pub fn clock_map(&self) -> Option<ClockMap> {
        self.clock_before
            .map(|before| ClockMap::new(before, self.clock_after))
    }

    /// The next `tick` from the responder (also merged into live samples
    /// automatically). `None` once the run's inbox is closed.
    pub async fn next_tick(&mut self) -> Option<TickMsg> {
        self.ticks.recv().await
    }

    /// A buffered `tick`, if any.
    pub fn try_tick(&mut self) -> Option<TickMsg> {
        self.ticks.try_recv().ok()
    }

    /// Wait for the responder's `ft.done` for `iteration`.
    pub async fn wait_ft_done(
        &mut self,
        iteration: u32,
        timeout: Duration,
    ) -> Result<FtDoneMsg, AppError> {
        if let Some(i) = self
            .ft_pending
            .iter()
            .position(|d| d.iteration == iteration)
        {
            return Ok(self.ft_pending.remove(i));
        }
        let until = Instant::now() + timeout;
        loop {
            let got = tokio::select! {
                _ = self.cancel.cancelled() => return Err(AppError::cancelled()),
                r = tokio::time::timeout_at(until, self.ft_done.recv()) => r,
            };
            match got {
                Err(_) => {
                    return Err(AppError::timeout(format!(
                        "no ft.done for iteration {iteration} within {} s",
                        timeout.as_secs()
                    )))
                }
                Ok(None) => return Err(AppError::not_running()),
                Ok(Some(d)) if d.iteration == iteration => return Ok(d),
                // An iteration this run never waits for again is dropped.
                Ok(Some(d)) if d.iteration < iteration || self.ft_pending.len() >= MAX_FT_DONE => {}
                Ok(Some(d)) => self.ft_pending.push(d),
            }
        }
    }

    /// Keep what was measured so far: if the run then fails, its report
    /// still carries this summary, series and results (the app shows the
    /// partial series next to the error).
    pub fn set_partial(&mut self, output: WorkloadOutput) {
        self.partial = Some(output);
    }

    // ── files and path ─────────────────────────────────────────────────

    /// `<data_dir>/tmp/out-<run_tag_hex>/` for source files (created; the
    /// hub removes it when the run ends).
    pub fn out_dir(&mut self) -> Result<PathBuf, AppError> {
        let dir = out_dir(&self.data_dir, self.run_tag()?);
        control::create_run_dir(&dir)?;
        self.out_dir = Some(dir.clone());
        Ok(dir)
    }

    /// Start the 2 Hz path sampler (idempotent; only for time-based
    /// workloads, and once per run).
    pub fn start_path_sampler(&self) {
        if !self.params.time_based() {
            return;
        }
        let mut slot = self.sampler.lock().unwrap_or_else(PoisonError::into_inner);
        if slot.ran {
            return;
        }
        slot.ran = true;
        // Latency under load, live: the sampler's latest RTT (the report's
        // `loaded_rtt_p50_ms` is the p50 over the whole timeline).
        let live = self.live();
        slot.running = Some(PathSampler::for_node(
            self.node.clone(),
            self.peer.tailscale_id.clone(),
            self.origin_ns,
            Box::new(move |point| {
                if let Some(rtt) = point.rtt_ms {
                    live.set(common::KEY_LOADED_RTT, rtt);
                }
            }),
        ));
    }

    /// Stop the path sampler (idempotent) and keep its timeline.
    pub async fn stop_path_sampler(&mut self) {
        let running = self
            .sampler
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .running
            .take();
        if let Some(s) = running {
            let tl = s.stop().await;
            let mut slot = self.sampler.lock().unwrap_or_else(PoisonError::into_inner);
            slot.points.extend(tl.points);
            slot.loaded = tl.loaded_rtt_ms;
        }
    }

    /// Add a path point outside the sampler (the `path` workload records its
    /// own pings so `path_label` sees them).
    pub fn add_timeline_point(&mut self, point: PathTimelinePoint) {
        self.extra_timeline.push(point);
    }

    /// The sampler's points and loaded-RTT distribution (after stopping).
    pub(crate) fn take_timeline(&mut self) -> (Vec<PathTimelinePoint>, Option<Distribution>) {
        let mut slot = self.sampler.lock().unwrap_or_else(PoisonError::into_inner);
        let mut points = std::mem::take(&mut slot.points);
        points.extend(std::mem::take(&mut self.extra_timeline));
        points.sort_by_key(|p| p.t_ms);
        (points, slot.loaded.take())
    }
}

// ---------------------------------------------------------------------------
// ResponderCtx
// ---------------------------------------------------------------------------

/// The measurement window a responder workload reports for its resources.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResourceWindow {
    /// Window start, responder `now_ns()`.
    pub start_ns: u64,
    /// Window end.
    pub end_ns: u64,
    /// Payload bytes in the window (for per-MB figures), if this side knows.
    pub goodput_bytes: Option<u64>,
}

/// The responder's view of one prepared run. Cheap to clone; move clones
/// into spawned tasks.
#[derive(Clone)]
pub struct ResponderCtx {
    /// This hub's node.
    pub node: Arc<BenchNode>,
    /// The peer that prepared the run: only it may use the run's data plane.
    pub initiator: Initiator,
    /// The initiator's run id.
    pub run_id: String,
    /// 64 random bits chosen here.
    pub run_tag: u64,
    /// Params as validated here.
    pub params: WorkloadParams,
    data_dir: PathBuf,
    offers: Arc<OfferRouter>,
    cancel: CancellationToken,
    foreign: Arc<AtomicU64>,
    window: Arc<Mutex<Option<ResourceWindow>>>,
    last_tick: Arc<Mutex<Option<Instant>>>,
    watchdog_at: Arc<Mutex<Instant>>,
}

impl std::fmt::Debug for ResponderCtx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResponderCtx")
            .field("initiator", &self.initiator)
            .field("run_id", &self.run_id)
            .field("run_tag", &crate::dataplane::run_tag_hex(self.run_tag))
            .field("workload", &self.params.workload())
            .finish()
    }
}

impl ResponderCtx {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        node: Arc<BenchNode>,
        initiator: Initiator,
        run_id: String,
        run_tag: u64,
        params: WorkloadParams,
        data_dir: PathBuf,
        offers: Arc<OfferRouter>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            node,
            initiator,
            run_id,
            run_tag,
            data_dir,
            offers,
            cancel,
            foreign: Arc::new(AtomicU64::new(0)),
            window: Arc::new(Mutex::new(None)),
            last_tick: Arc::new(Mutex::new(None)),
            watchdog_at: Arc::new(Mutex::new(Instant::now() + params.responder_watchdog())),
            params,
        }
    }

    /// When the run's watchdog tears it down (§6 rule 4), unless the run
    /// ends first.
    pub(crate) fn watchdog_at(&self) -> Instant {
        *self
            .watchdog_at
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Progress: the watchdog fires no earlier than `after` from now.
    pub fn extend_watchdog(&self, after: Duration) {
        let mut at = self
            .watchdog_at
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        *at = (*at).max(Instant::now() + after);
    }

    /// Tests: the watchdog fires `after` from now.
    #[cfg(test)]
    pub(crate) fn set_watchdog_in(&self, after: Duration) {
        *self
            .watchdog_at
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Instant::now() + after;
    }

    /// The run tag as 16 hex digits (names: `tb-<hex>-<iter>.bin`, `bench-<hex>`).
    pub fn run_tag_hex(&self) -> String {
        crate::dataplane::run_tag_hex(self.run_tag)
    }

    /// `<data_dir>/tmp/in-<run_tag_hex>/` (not created).
    pub fn in_dir_path(&self) -> PathBuf {
        in_dir(&self.data_dir, self.run_tag)
    }

    /// `<data_dir>/tmp/in-<run_tag_hex>/`, created. The hub removes it when
    /// the run ends (finish, cancel or watchdog).
    pub fn in_dir(&self) -> Result<PathBuf, AppError> {
        let dir = self.in_dir_path();
        control::create_run_dir(&dir)?;
        Ok(dir)
    }

    /// Fires on cancel, watchdog and hub stop.
    pub fn cancel_token(&self) -> CancellationToken {
        self.cancel.clone()
    }

    /// Whether the run was torn down.
    pub fn is_cancelled(&self) -> bool {
        self.cancel.is_cancelled()
    }

    /// Rule 5 for an accepted TCP connection; anything foreign is counted.
    pub fn admit_tcp(&self, incoming: &RawIncoming) -> bool {
        let ok = self.initiator.accepts_tcp(incoming);
        if !ok {
            self.count_foreign();
        }
        ok
    }

    /// Rule 5 for a datagram source or QUIC remote address; foreign ones are counted.
    pub fn admit_ip(&self, ip: IpAddr) -> bool {
        let ok = self.initiator.is_ip(ip);
        if !ok {
            self.count_foreign();
        }
        ok
    }

    /// Rule 5 for a `"ip:port"` string (UDP `recv_from`).
    pub fn admit_addr(&self, addr: &str) -> bool {
        let ok = self.initiator.is_addr(addr);
        if !ok {
            self.count_foreign();
        }
        ok
    }

    /// Count one dropped foreign connection, datagram or stream (bad magic/tag too).
    pub fn count_foreign(&self) {
        self.foreign.fetch_add(1, Ordering::Relaxed);
    }

    /// Foreign connections/datagrams dropped so far (`foreign_dropped`).
    pub fn foreign_dropped(&self) -> u64 {
        self.foreign.load(Ordering::Relaxed)
    }

    /// Report the measurement window (responder time) and its payload bytes,
    /// so `finish.ack.resources` covers the same window as goodput. Without
    /// it the hub uses `start` → `finish`.
    pub fn set_resource_window(&self, start_ns: u64, end_ns: u64, goodput_bytes: Option<u64>) {
        *self.window.lock().unwrap_or_else(PoisonError::into_inner) = Some(ResourceWindow {
            start_ns,
            end_ns,
            goodput_bytes,
        });
    }

    pub(crate) fn resource_window(&self) -> Option<ResourceWindow> {
        *self.window.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Send a `tick` of live receiver-side values to the initiator.
    /// Rate-limited to ≤ 4 Hz (extra calls are dropped); errors are ignored.
    pub async fn tick<'k>(&self, values: impl IntoIterator<Item = (&'k str, f64)>) {
        {
            let mut last = self
                .last_tick
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let now = Instant::now();
            if last
                .is_some_and(|t| now.duration_since(t) < TICK_INTERVAL - Duration::from_millis(10))
            {
                return;
            }
            *last = Some(now);
        }
        let msg = TickMsg {
            run_id: self.run_id.clone(),
            t_ns: now_ns(),
            values: values
                .into_iter()
                .filter(|(_, v)| v.is_finite())
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
        };
        let _ = control::notify(
            &self.node,
            &self.initiator.tailscale_id,
            msg_type::TICK,
            &msg,
            Duration::from_secs(1),
        )
        .await;
    }

    /// Expect a file from the initiator in this run's `in-…` directory (the
    /// hub's offer router accepts only expected offers). Await the returned
    /// handle for `Completed`/`Failed`, then send [`send_ft_done`](Self::send_ft_done).
    pub fn expect_file(
        &self,
        file_name: &str,
        size: u64,
    ) -> Result<crate::hub::ExpectedOffer, AppError> {
        let dir = self.in_dir()?;
        Ok(self
            .offers
            .expect(&self.initiator.tailscale_id, file_name, size, &dir))
    }

    /// Tell the initiator a file-transfer iteration ended.
    pub async fn send_ft_done(&self, done: &FtDoneMsg) -> Result<(), AppError> {
        control::notify(
            &self.node,
            &self.initiator.tailscale_id,
            msg_type::FT_DONE,
            done,
            Duration::from_secs(10),
        )
        .await
    }

    /// An empty [`Teardown`] for this node.
    pub fn teardown(&self) -> Teardown {
        Teardown::new(self.node.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn collector() -> (ProgressSink, Arc<Mutex<Vec<Progress>>>) {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let s2 = seen.clone();
        (Arc::new(move |p| s2.lock().unwrap().push(p)), seen)
    }

    #[test]
    fn prepare_retries_only_the_responders_peer_not_found() {
        use control::RequestFailure::{Failed, Refused};
        let not_found = || {
            AppError::new(
                ErrorCode::PeerNotFound,
                "this device does not know you yet; retry in a moment",
            )
        };
        assert!(prepare_retries(&Refused(not_found()), true));
        assert!(
            !prepare_retries(&Refused(not_found()), false),
            "no time left"
        );
        // This node's own send failing (its registry dropped the peer).
        let local: AppError = truffle_core::NodeError::Session(
            truffle_core::session::SessionError::UnknownPeer("p".into()),
        )
        .into();
        assert_eq!(local.code, ErrorCode::PeerNotFound);
        assert!(!prepare_retries(&Failed(local), true));
        assert!(!prepare_retries(&Refused(AppError::internal("boom")), true));
    }

    #[tokio::test(start_paused = true)]
    async fn progress_is_throttled_in_data_phases_and_immediate_otherwise() {
        let (sink, seen) = collector();
        let p = RunProgress::new("r", None, Some(Duration::from_secs(4)), sink);
        let ticker = p.spawn_ticker();
        p.set_phase(Phase::Preparing, None);
        p.set_phase(Phase::Preparing, None); // no change, no event
        assert_eq!(seen.lock().unwrap().len(), 1);
        p.set_phase(Phase::Running, None);
        for i in 0..100 {
            p.set_sample("sent_mbps", f64::from(i));
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        // 2 s of running: 1 phase event + ≤ 8 ticks at 250 ms.
        let n = seen.lock().unwrap().len();
        assert!((8..=10).contains(&n), "{n} events");
        let last = p.last().unwrap();
        assert_eq!(last.phase, Phase::Running);
        assert_eq!(last.samples[0].key, "sent_mbps");
        let f = last.fraction.unwrap();
        assert!((0.4..=0.55).contains(&f), "auto fraction {f}");
        p.set_phase(Phase::Done, None);
        ticker.abort();
        let done = p.last().unwrap();
        assert_eq!((done.phase, done.fraction), (Phase::Done, Some(1.0)));
        // Attach replays the last event.
        let (sink2, seen2) = collector();
        p.attach(sink2);
        assert_eq!(seen2.lock().unwrap()[0].phase, Phase::Done);
        // A panicking sink does not break delivery.
        p.attach(Arc::new(|_| panic!("sink panicked")));
        p.emit();
        assert_eq!(seen2.lock().unwrap().len(), 2);
    }

    #[test]
    fn registry_matches_modules() {
        assert!(implemented(Workload::Path));
        assert!(implemented_names().contains(&"path".to_string()));
        for w in Workload::ALL {
            let out = example_output(w);
            assert!(!out.summary.is_empty(), "{w}");
        }
        assert_eq!(
            unsupported(Workload::Tcp).code,
            ErrorCode::UnsupportedWorkload
        );
    }

    #[test]
    fn every_mode_has_an_example_with_values() {
        let modes: Vec<&str> = mode_examples().iter().map(|e| e.mode).collect();
        assert_eq!(
            modes,
            [
                "path",
                "tcp-throughput-upload",
                "tcp-throughput-bidirectional",
                "tcp-latency",
                "udp-stream",
                "udp-echo",
                "udp-ramp",
                "quic-throughput",
                "quic-latency",
                "ws-rr",
                "ws-stream",
                "file-transfer",
                "synced-store-pingpong",
                "synced-store-burst",
                "http-proxy-keepalive",
                "http-direct-new",
                "video-udp",
                "video-quic",
                "egress",
            ]
        );
        for e in mode_examples() {
            e.params.validate().unwrap();
            assert!(!e.output.summary.is_empty(), "{}", e.mode);
            assert!(
                e.output.summary.iter().all(|s| s.value.is_some()),
                "{}: {:?}",
                e.mode,
                e.output.summary
            );
            // A burst is one write burst and one echo: no series.
            if e.mode != "synced-store-burst" {
                assert!(!e.output.series.is_empty(), "{}", e.mode);
            }
            assert!(
                e.output.series.iter().all(|s| !s.points.is_empty()),
                "{}",
                e.mode
            );
            assert!(e.output.results.is_object(), "{}", e.mode);
        }
    }

    #[test]
    fn echoed_params_must_equal_the_request() {
        use crate::params::FileTransferParams;
        let asked = WorkloadParams::FileTransfer(FileTransferParams {
            size_bytes: 1024 * 1024,
            iterations: 2,
        });
        check_echoed_params(&asked, Some(&asked.clone())).unwrap();
        check_echoed_params(&asked, None).unwrap();
        // A responder may not resize this side's work, in bounds or not.
        for echoed in [
            WorkloadParams::FileTransfer(FileTransferParams {
                size_bytes: 10_995_116_277_760,
                iterations: 2,
            }),
            WorkloadParams::FileTransfer(FileTransferParams {
                size_bytes: 1024 * 1024,
                iterations: u32::MAX,
            }),
            WorkloadParams::FileTransfer(FileTransferParams {
                size_bytes: 2 * 1024 * 1024,
                iterations: 2,
            }),
            WorkloadParams::Tcp(Default::default()),
        ] {
            let e = check_echoed_params(&asked, Some(&echoed)).unwrap_err();
            assert_eq!(e.code, ErrorCode::ProtocolMismatch, "{echoed:?}");
        }
    }

    #[test]
    fn oversized_ticks_are_refused() {
        let tick = |n: usize, len: usize| TickMsg {
            run_id: "r".into(),
            t_ns: 0,
            values: (0..n).map(|i| (format!("{i:0len$}"), 1.0)).collect(),
        };
        assert!(tick_is_sane(&tick(8, 12)));
        assert!(tick_is_sane(&tick(MAX_TICK_KEYS, MAX_TICK_KEY_LEN)));
        assert!(!tick_is_sane(&tick(MAX_TICK_KEYS + 1, 4)));
        assert!(!tick_is_sane(&tick(1, MAX_TICK_KEY_LEN + 1)));
    }

    #[test]
    fn client_blocks_default_to_empty_objects() {
        #[derive(serde::Deserialize)]
        struct C {}
        let _: C = client_of(Workload::Tcp, Value::Null).unwrap();
        assert!(client_of::<C>(Workload::Tcp, Value::from(3)).is_err());
    }
}
