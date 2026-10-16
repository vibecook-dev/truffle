//! `file-transfer` (RFC 027 §8.2 file-transfer; §6 rules 1 and 6).
//!
//! - **Responder:** `prepare` registers every iteration's file,
//!   `tb-<run_tag_hex>-<iter>.bin` of exactly `size_bytes`, with the hub's
//!   single offer router ([`ResponderCtx::expect_file`]; it checks the
//!   sender, name, size and free disk and accepts into `in-<run_tag_hex>/`).
//!   Workloads never call `offer_channel`/`auto_accept`. When an iteration
//!   ends (`Completed` or `Failed`) the responder sends `ft.done` and deletes
//!   the received file, so the disk holds at most one copy.
//! - **Initiator:** writes one deterministic-pattern source file into
//!   `out-<run_tag_hex>/` (renamed per iteration, so the name carries the
//!   iteration and the page cache stays warm), then per iteration calls
//!   `send_file` and waits up to 60 s for that iteration's `ft.done` before
//!   the next (this keeps the `MAX_PENDING_OFFERS_PER_PEER = 4` regression
//!   the old crate's live test guarded covered).
//! - **Metrics** come from `TransferResult` (never from progress
//!   `speed_bps`): `e2e_mbps = size·8 / elapsed_secs / 1e6`,
//!   `wire_mbps = size·8 / timings.transfer_secs / 1e6`,
//!   `hash_ms = timings.hash_secs · 1000` ("SHA-256 of page-cached data"),
//!   `negotiate_ms` (OFFER sent → data stream open).
//! - **ACK check:** an iteration whose ACK was not read (`ack_ok == false`)
//!   is valid only when the responder's received SHA-256 matches, and it is
//!   left out of the timing statistics.
//! - **Summary:** the median over the timed iterations, leaving out
//!   iteration 1 when there are 3 or more.
//!
//! `transfer_secs` carries a fixed cost of about 40 ms at every file size
//! that core has not explained yet (RFC 027 work log); when the transfers
//! are short enough for it to dominate, the run says so in a note.
//!
//! Summary keys: `wire_mbps`, `e2e_mbps`, `hash_ms`, `negotiate_ms`.
//! Series: `wire_mbps` per iteration (at each iteration's end).
//! Live samples (once per finished file): `wire_mbps`, `e2e_mbps`,
//! `hash_ms`, `negotiate_ms`, `transfer_ms`, `iterations_done`.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use futures::future::BoxFuture;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::time::Instant;

use crate::clock::now_ns;
use crate::control::FtDoneMsg;
use crate::error::{AppError, ErrorCode};
use crate::hub::OfferOutcome;
use crate::params::{fmt_bytes, FileTransferParams};
use crate::stats;
use crate::types::{Better, Phase, Series, SummaryItem};
use crate::workloads::common::Tasks;
use crate::workloads::{ResponderCtx, ResponderRun, RunCtx, WorkloadOutput};

/// `file-transfer` is implemented.
pub const IMPLEMENTED: bool = true;

/// The Progress sample keys a run with these params can set: the
/// initiator's own and the responder's `tick`s, which the hub merges in.
/// [`live_keys`](super::live_keys) adds the path sampler's `loaded_rtt_ms`
/// for time-based runs; `live-keys.json` in the schema fixtures is built
/// from it.
pub fn live_keys(_p: &FileTransferParams) -> Vec<&'static str> {
    vec![
        "wire_mbps",
        "e2e_mbps",
        "hash_ms",
        "negotiate_ms",
        "transfer_ms",
        "iterations_done",
    ]
}

/// How long the initiator waits for an iteration's `ft.done` (§8.2).
pub const FT_DONE_TIMEOUT: Duration = Duration::from_secs(60);
/// After a failed `send_file`, how long to wait for the responder's view.
const FAILED_DONE_TIMEOUT: Duration = Duration::from_secs(10);
/// The fixed cost seen in `transfer_secs` at every size (unexplained so far).
pub const TRANSFER_FLOOR_MS: f64 = 40.0;
/// Below this median transfer time the floor dominates `wire_mbps`.
pub const FLOOR_NOTE_BELOW_MS: f64 = 100.0;
/// Source files are written in blocks of this size.
const WRITE_BLOCK: usize = 1 << 20;
/// Seed of the source pattern (fixed: every run sends the same bytes).
const PATTERN_SEED: u64 = 0x7472_7566_666c_6521;

/// `tb-<run_tag_hex>-<iteration>.bin`, iterations from 1.
pub fn file_name(run_tag_hex: &str, iteration: u32) -> String {
    format!("tb-{run_tag_hex}-{iteration}.bin")
}

/// Every iteration's file name, in order.
pub fn file_names(run_tag_hex: &str, iterations: u32) -> Vec<String> {
    (1..=iterations)
        .map(|i| file_name(run_tag_hex, i))
        .collect()
}

// ---------------------------------------------------------------------------
// Wire and report types
// ---------------------------------------------------------------------------

/// `prepare.client`: nothing beyond the params (the file names depend on the
/// responder's run tag).
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "FileTransferClient")]
pub struct Client {}

/// `prepare.ack.setup`: the file names the responder expects, in order.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "FileTransferSetup")]
pub struct Setup {
    /// `tb-<run_tag_hex>-<iter>.bin` for iterations 1….
    pub file_names: Vec<String>,
}

/// One iteration as the sender saw it.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "FileTransferSent")]
pub struct SentIteration {
    /// Iteration, from 1.
    pub iteration: u32,
    /// `send_file` returned Ok.
    pub ok: bool,
    /// Bytes sent.
    pub bytes: u64,
    /// SHA-256 the sender computed (hex).
    pub sha256: Option<String>,
    /// Whether the receiver's ACK byte was read.
    pub ack_ok: bool,
}

/// `finish.info`: the sender's per-iteration outcome.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "FileTransferInfo")]
pub struct Info {
    /// Every iteration the initiator attempted.
    pub iterations: Vec<SentIteration>,
}

/// One iteration as the receiver saw it (also sent as `ft.done`).
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "FileTransferReceived")]
pub struct ReceivedIteration {
    /// Iteration, from 1.
    pub iteration: u32,
    /// `Completed` (true) or `Failed` (false).
    pub ok: bool,
    /// Bytes received.
    pub bytes: u64,
    /// SHA-256 of the received file (hex).
    pub sha256: Option<String>,
    /// Why it failed.
    pub error: Option<String>,
    /// Responder `now_ns()` when the outcome arrived.
    pub at_ns: u64,
}

/// `finish.ack.stats`: the receiver's per-iteration outcome.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "FileTransferStats")]
pub struct Stats {
    /// Iterations that ended, by iteration.
    pub iterations: Vec<ReceivedIteration>,
    /// Σ bytes of completed iterations.
    pub received_bytes: u64,
}

/// One iteration in the report.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "FileTransferIteration")]
pub struct Iteration {
    /// Iteration, from 1.
    pub index: u32,
    /// `tb-<run_tag_hex>-<index>.bin`.
    pub file_name: String,
    /// File size.
    pub bytes: u64,
    /// Sent, received and verified (a missing ACK is fine when the
    /// receiver's SHA-256 matches).
    pub ok: bool,
    /// The sender read the receiver's ACK byte.
    pub ack_ok: bool,
    /// The receiver's SHA-256 equals the sender's; `None` when unknown.
    pub sha256_match: Option<bool>,
    /// `timings.hash_secs` in ms ("SHA-256 of page-cached data").
    pub hash_ms: Option<f64>,
    /// OFFER sent → data stream open, ms.
    pub negotiate_ms: Option<f64>,
    /// First write to the data stream → ACK read, ms.
    pub transfer_ms: Option<f64>,
    /// `send_file` end to end, ms.
    pub elapsed_ms: Option<f64>,
    /// `size·8 / transfer_secs / 1e6`.
    pub wire_mbps: Option<f64>,
    /// `size·8 / elapsed_secs / 1e6`.
    pub e2e_mbps: Option<f64>,
    /// `send_file` returned → `ft.done` arrived, ms.
    pub done_wait_ms: Option<f64>,
    /// Left out of the summary.
    pub excluded: bool,
    /// Why it was left out.
    pub excluded_reason: Option<String>,
    /// What went wrong.
    pub error: Option<String>,
    /// When it ended, ms since the run started.
    pub t_ms: f64,
}

/// `RunReport.results` of a `file-transfer` run.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "FileTransferResults")]
pub struct Results {
    /// File size.
    pub size_bytes: u64,
    /// Every iteration attempted, in order.
    pub iterations: Vec<Iteration>,
    /// Iterations the summary medians cover.
    pub timed_iterations: u32,
    /// Iterations whose ACK was not read (valid by SHA-256, untimed).
    pub ack_failures: u32,
    /// Σ bytes of iterations `send_file` completed.
    pub sent_bytes: u64,
    /// Σ bytes the receiver completed.
    pub received_bytes: u64,
    /// Writing the source file before the first iteration, ms.
    pub source_write_ms: Option<f64>,
}

// ---------------------------------------------------------------------------
// Source files
// ---------------------------------------------------------------------------

/// A deterministic byte stream (xorshift64*): incompressible-looking, the
/// same for every run.
#[derive(Debug, Clone)]
pub struct Pattern {
    state: u64,
}

impl Pattern {
    /// A stream from `seed` (0 is replaced by a fixed non-zero value).
    pub fn new(seed: u64) -> Self {
        Self {
            state: if seed == 0 { PATTERN_SEED } else { seed },
        }
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.state = x;
        x.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    /// Fill `buf` with the next bytes of the stream.
    pub fn fill(&mut self, buf: &mut [u8]) {
        for chunk in buf.chunks_mut(8) {
            let v = self.next_u64().to_le_bytes();
            chunk.copy_from_slice(&v[..chunk.len()]);
        }
    }
}

/// Write `size` bytes of the pattern to `path` (blocking).
pub fn write_pattern_file(path: &Path, size: u64) -> std::io::Result<()> {
    let file = std::fs::File::create(path)?;
    let mut out = std::io::BufWriter::with_capacity(WRITE_BLOCK, file);
    let mut pattern = Pattern::new(PATTERN_SEED);
    let mut block = vec![0u8; WRITE_BLOCK];
    let mut left = size;
    while left > 0 {
        let n = left.min(WRITE_BLOCK as u64) as usize;
        pattern.fill(&mut block[..n]);
        out.write_all(&block[..n])?;
        left -= n as u64;
    }
    out.flush()
}

// ---------------------------------------------------------------------------
// Summary
// ---------------------------------------------------------------------------

/// The summary medians over the timed iterations.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct Medians {
    /// Median `wire_mbps`.
    pub wire_mbps: Option<f64>,
    /// Median `e2e_mbps`.
    pub e2e_mbps: Option<f64>,
    /// Median `hash_ms`.
    pub hash_ms: Option<f64>,
    /// Median `negotiate_ms`.
    pub negotiate_ms: Option<f64>,
    /// Median `transfer_ms`.
    pub transfer_ms: Option<f64>,
    /// Iterations used.
    pub timed: u32,
}

/// With this many iterations or more, iteration 1 (cold caches, a fresh
/// connection) is left out of the medians; with fewer it is included.
pub const EXCLUDE_FIRST_FROM: u32 = 3;

/// Mark which iterations the summary leaves out (iteration 1 when there are
/// [`EXCLUDE_FIRST_FROM`] or more planned; every iteration without an ACK or
/// that failed) and compute the medians over the rest.
pub fn summarise(iterations: &mut [Iteration], planned: u32) -> Medians {
    for it in iterations.iter_mut() {
        let reason = if !it.ok {
            Some("failed")
        } else if planned >= EXCLUDE_FIRST_FROM && it.index == 1 {
            Some("first of 3 or more (warms caches and the connection)")
        } else if !it.ack_ok {
            Some("no ACK read; verified by the receiver's SHA-256 instead")
        } else {
            None
        };
        it.excluded = reason.is_some();
        it.excluded_reason = reason.map(str::to_string);
    }
    let timed: Vec<&Iteration> = iterations.iter().filter(|i| !i.excluded).collect();
    let med = |f: fn(&Iteration) -> Option<f64>| {
        stats::median(&timed.iter().copied().filter_map(f).collect::<Vec<f64>>())
    };
    Medians {
        wire_mbps: med(|i| i.wire_mbps),
        e2e_mbps: med(|i| i.e2e_mbps),
        hash_ms: med(|i| i.hash_ms),
        negotiate_ms: med(|i| i.negotiate_ms),
        transfer_ms: med(|i| i.transfer_ms),
        timed: timed.len() as u32,
    }
}

/// Mb/s of `bytes` over `secs`.
fn mbps_over(bytes: u64, secs: f64) -> Option<f64> {
    (secs > 0.0 && secs.is_finite()).then(|| bytes as f64 * 8.0 / secs / 1e6)
}

/// The note when the transfer floor dominates `wire_mbps`.
pub fn floor_note(median_transfer_ms: Option<f64>) -> Option<String> {
    let ms = median_transfer_ms?;
    (ms < FLOOR_NOTE_BELOW_MS).then(|| {
        format!(
            "transfers took {ms:.0} ms on the wire (median), so the fixed ~{TRANSFER_FLOOR_MS:.0} ms \
             that core's transfer timing shows at every file size (not yet explained) dominates \
             wire_mbps; use larger files for a link figure"
        )
    })
}

/// Turn the iterations into the workload output.
fn assemble(
    p: &FileTransferParams,
    mut iterations: Vec<Iteration>,
    received_bytes: u64,
    source_write_ms: Option<f64>,
    window_ns: Option<(u64, u64)>,
) -> WorkloadOutput {
    let m = summarise(&mut iterations, p.iterations);
    let sent_bytes = iterations
        .iter()
        .filter(|i| i.elapsed_ms.is_some())
        .map(|i| i.bytes)
        .sum();
    let ack_failures = iterations.iter().filter(|i| i.ok && !i.ack_ok).count() as u32;
    let summary = vec![
        SummaryItem::new("wire_mbps", "Wire", m.wire_mbps, "Mb/s", Better::Higher),
        SummaryItem::new("e2e_mbps", "End to end", m.e2e_mbps, "Mb/s", Better::Higher),
        SummaryItem::new(
            "hash_ms",
            "SHA-256 of page-cached data",
            m.hash_ms,
            "ms",
            Better::Lower,
        ),
        SummaryItem::new(
            "negotiate_ms",
            "Offer to stream",
            m.negotiate_ms,
            "ms",
            Better::Lower,
        ),
    ];
    let first_excluded = iterations
        .iter()
        .find(|i| i.index == 1 && p.iterations >= EXCLUDE_FIRST_FROM)
        .map(|i| i.t_ms);
    let series = vec![Series {
        key: "wire_mbps".into(),
        label: "Wire".into(),
        unit: "Mb/s".into(),
        points: iterations
            .iter()
            .filter_map(|i| i.wire_mbps.map(|w| (i.t_ms, w)))
            .collect(),
        warmup_ms: first_excluded,
    }];
    let mut notes = Vec::new();
    if let Some(n) = floor_note(m.transfer_ms) {
        notes.push(n);
    }
    if ack_failures > 0 {
        notes.push(format!(
            "{ack_failures} iteration{} had no ACK; the receiver's SHA-256 matched, so {} \
             counted as delivered but left out of the timings",
            if ack_failures == 1 { "" } else { "s" },
            if ack_failures == 1 {
                "it is"
            } else {
                "they are"
            }
        ));
    }
    // The resource window spans every iteration, so per-MB figures divide by
    // every delivered byte (excluded iterations included).
    let goodput_bytes = Some(iterations.iter().filter(|i| i.ok).map(|i| i.bytes).sum());
    let results = Results {
        size_bytes: p.size_bytes,
        timed_iterations: m.timed,
        ack_failures,
        sent_bytes,
        received_bytes,
        source_write_ms,
        iterations,
    };
    WorkloadOutput {
        summary,
        results: serde_json::to_value(&results).unwrap_or_default(),
        series,
        notes,
        window_ns,
        goodput_bytes,
    }
}

// ---------------------------------------------------------------------------
// Initiator
// ---------------------------------------------------------------------------

/// Initiator: send `iterations` files of `size_bytes`, one at a time.
pub async fn run(ctx: &mut RunCtx<'_>, p: &FileTransferParams) -> Result<WorkloadOutput, AppError> {
    let setup: Setup = ctx.prepare(&Client {}).await?;
    let p = match ctx.params() {
        crate::params::WorkloadParams::FileTransfer(used) => used.clone(),
        _ => p.clone(),
    };
    let hex = ctx.run_tag_hex()?;
    let names = file_names(&hex, p.iterations);
    if setup.file_names != names {
        return Err(AppError::new(
            ErrorCode::ProtocolMismatch,
            "prepare: the responder expects different file names",
        ));
    }

    // The source file (written once; renamed per iteration).
    let dir = ctx.out_dir()?;
    ctx.message(format!("writing a {} source file", fmt_bytes(p.size_bytes)));
    let mut current: PathBuf = dir.join(&names[0]);
    let write_started = Instant::now();
    {
        let path = current.clone();
        let size = p.size_bytes;
        tokio::task::spawn_blocking(move || write_pattern_file(&path, size))
            .await
            .map_err(|e| AppError::internal(format!("source file writer: {e}")))?
            .map_err(|e| AppError::io("write the source file", &e))?;
    }
    let source_write_ms = Some(write_started.elapsed().as_secs_f64() * 1000.0);

    ctx.start().await?;
    ctx.phase(Phase::Running);
    let window_start = now_ns();
    let mut iterations: Vec<Iteration> = Vec::with_capacity(p.iterations as usize);
    let mut sent: Vec<SentIteration> = Vec::with_capacity(p.iterations as usize);
    let peer_id = ctx.peer_id().to_string();
    for (i, name) in (1..=p.iterations).zip(&names) {
        ctx.check_cancelled()?;
        if i > 1 {
            let next = dir.join(name);
            tokio::fs::rename(&current, &next)
                .await
                .map_err(|e| AppError::io("rename the source file", &e))?;
            current = next;
        }
        let local = current
            .to_str()
            .ok_or_else(|| AppError::internal("the data directory is not valid UTF-8"))?
            .to_string();
        ctx.message(format!("file {i} of {}", p.iterations));
        let sent_result = {
            let node = ctx.node.clone();
            let ft = node.file_transfer();
            let cancel = ctx.cancel_token();
            tokio::select! {
                _ = cancel.cancelled() => return Err(AppError::cancelled()),
                r = ft.send_file(&peer_id, &local, name) => r,
            }
        };
        let returned = Instant::now();
        let mut it = Iteration {
            index: i,
            file_name: name.clone(),
            bytes: p.size_bytes,
            ..Iteration::default()
        };
        let mut s = SentIteration {
            iteration: i,
            bytes: p.size_bytes,
            ..SentIteration::default()
        };
        match sent_result {
            Ok(tr) => {
                let t = tr.timings;
                s.ok = true;
                s.sha256 = Some(tr.sha256.clone());
                s.ack_ok = t.ack_ok;
                it.ack_ok = t.ack_ok;
                it.hash_ms = Some(t.hash_secs * 1000.0);
                it.negotiate_ms = Some(t.negotiate_secs * 1000.0);
                it.transfer_ms = Some(t.transfer_secs * 1000.0);
                it.elapsed_ms = Some(tr.elapsed_secs * 1000.0);
                it.wire_mbps = mbps_over(tr.bytes_transferred, t.transfer_secs);
                it.e2e_mbps = mbps_over(tr.bytes_transferred, tr.elapsed_secs);
                match ctx.wait_ft_done(i, FT_DONE_TIMEOUT).await {
                    Ok(done) => {
                        it.done_wait_ms = Some(returned.elapsed().as_secs_f64() * 1000.0);
                        let matched = done.sha256.as_deref() == Some(tr.sha256.as_str());
                        it.sha256_match = Some(matched);
                        if !done.ok {
                            it.error = Some(format!(
                                "the receiver reported a failure: {}",
                                done.error.unwrap_or_else(|| "no reason given".into())
                            ));
                        } else if !matched || done.bytes != p.size_bytes {
                            it.error = Some(format!(
                                "the receiver got {} bytes with a different SHA-256",
                                done.bytes
                            ));
                        } else {
                            it.ok = true;
                        }
                    }
                    Err(e) => it.error = Some(e.message),
                }
            }
            Err(e) => {
                it.error = Some(AppError::from(e).message);
                if let Ok(done) = ctx.wait_ft_done(i, FAILED_DONE_TIMEOUT).await {
                    it.done_wait_ms = Some(returned.elapsed().as_secs_f64() * 1000.0);
                    if let Some(why) = done.error {
                        it.error = Some(format!(
                            "{}; the receiver says: {why}",
                            it.error.take().unwrap_or_default()
                        ));
                    }
                }
            }
        }
        it.t_ms = ctx.rel_ms(now_ns());
        if it.ok {
            let live = ctx.live();
            live.set_many(
                [
                    ("wire_mbps", it.wire_mbps),
                    ("e2e_mbps", it.e2e_mbps),
                    ("hash_ms", it.hash_ms),
                    ("negotiate_ms", it.negotiate_ms),
                    ("transfer_ms", it.transfer_ms),
                ]
                .into_iter()
                .filter_map(|(k, v)| v.map(|v| (k, v))),
            );
        }
        ctx.sample("iterations_done", f64::from(i));
        ctx.fraction(i as f32 / p.iterations as f32);
        ctx.emit();
        let failed = !it.ok;
        let error = it.error.clone();
        iterations.push(it);
        sent.push(s);
        if failed {
            ctx.set_partial(assemble(&p, iterations, 0, source_write_ms, None));
            return Err(AppError::new(
                ErrorCode::Io,
                format!(
                    "file {i} of {} failed: {}",
                    p.iterations,
                    error.unwrap_or_else(|| "unknown error".into())
                ),
            ));
        }
    }
    let window = Some((window_start, now_ns()));
    ctx.set_partial(assemble(&p, iterations.clone(), 0, source_write_ms, window));
    let stats: Stats = ctx
        .finish(&Info { iterations: sent }, Duration::ZERO)
        .await?;
    // Cross-check the responder's view of every iteration.
    for it in &mut iterations {
        let theirs = stats.iterations.iter().find(|r| r.iteration == it.index);
        if !theirs.is_some_and(|r| r.ok && r.bytes == it.bytes) {
            return Err(AppError::internal(format!(
                "the responder has no completed record of file {}",
                it.index
            )));
        }
    }
    Ok(assemble(
        &p,
        iterations,
        stats.received_bytes,
        source_write_ms,
        window,
    ))
}

// ---------------------------------------------------------------------------
// Responder
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct RxState {
    iterations: Vec<ReceivedIteration>,
    received_bytes: u64,
    last_ns: u64,
}

struct FtResponder {
    r: ResponderCtx,
    tasks: Tasks<()>,
    state: Arc<Mutex<RxState>>,
    started_ns: u64,
}

/// Responder: register every iteration's file with the offer router and
/// report each outcome as `ft.done`.
pub async fn prepare(
    r: &ResponderCtx,
    p: &FileTransferParams,
    _client: Client,
) -> Result<(Setup, Box<dyn ResponderRun>), AppError> {
    let dir = r.in_dir()?;
    let hex = r.run_tag_hex();
    let names = file_names(&hex, p.iterations);
    let state = Arc::new(Mutex::new(RxState::default()));
    let wait = r.params.deadline();
    let mut tasks = Vec::with_capacity(names.len());
    for (i, name) in (1..=p.iterations).zip(&names) {
        let expected = r.expect_file(name, p.size_bytes)?;
        let r2 = r.clone();
        let st = state.clone();
        let path = dir.join(name);
        tasks.push(tokio::spawn(async move {
            let cancel = r2.cancel_token();
            let outcome = tokio::select! {
                _ = cancel.cancelled() => return,
                o = expected.outcome(wait) => o,
            };
            let at_ns = now_ns();
            let rec = match outcome {
                OfferOutcome::Completed { bytes, sha256 } => ReceivedIteration {
                    iteration: i,
                    ok: true,
                    bytes,
                    sha256: Some(sha256),
                    error: None,
                    at_ns,
                },
                OfferOutcome::Failed { reason } => ReceivedIteration {
                    iteration: i,
                    ok: false,
                    bytes: 0,
                    sha256: None,
                    error: Some(reason),
                    at_ns,
                },
            };
            let done = {
                let mut s = st.lock().unwrap_or_else(PoisonError::into_inner);
                if rec.ok {
                    s.received_bytes += rec.bytes;
                }
                s.last_ns = s.last_ns.max(at_ns);
                s.iterations.push(rec.clone());
                s.iterations.len() as u32
            };
            // Progress: the watchdog's 30 min cap bounds each file, not
            // the whole run (§6 rule 4; up to 4 GiB may take longer).
            if let Some(after) = r2.params.responder_watchdog_after(done) {
                r2.extend_watchdog(after);
            }
            let done = FtDoneMsg {
                run_id: r2.run_id.clone(),
                iteration: i,
                ok: rec.ok,
                bytes: rec.bytes,
                sha256: rec.sha256,
                error: rec.error,
            };
            if let Err(e) = r2.send_ft_done(&done).await {
                tracing::debug!(error = %e, "file-transfer: ft.done not sent");
            }
            // Keep at most one received copy on disk.
            let _ = tokio::fs::remove_file(&path).await;
        }));
    }
    Ok((
        Setup { file_names: names },
        Box::new(FtResponder {
            r: r.clone(),
            tasks: Tasks::from(tasks),
            state,
            started_ns: 0,
        }),
    ))
}

impl ResponderRun for FtResponder {
    fn start(&mut self) -> BoxFuture<'_, Result<(), AppError>> {
        self.started_ns = now_ns();
        Box::pin(async { Ok(()) })
    }

    fn finish(self: Box<Self>, info: Value) -> BoxFuture<'static, Result<Value, AppError>> {
        Box::pin(async move {
            let info: Info = serde_json::from_value(info)
                .map_err(|e| AppError::invalid_params(format!("file-transfer info: {e}")))?;
            // Every iteration the sender completed has normally been
            // reported already (it waited for ft.done); give a straggler a
            // moment before snapshotting.
            let expected = info.iterations.iter().filter(|s| s.ok).count();
            let until = Instant::now() + Duration::from_secs(2);
            while Instant::now() < until {
                let have = self
                    .state
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .iterations
                    .len();
                if have >= expected {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            let (mut iterations, received_bytes, last_ns) = {
                let s = self.state.lock().unwrap_or_else(PoisonError::into_inner);
                (s.iterations.clone(), s.received_bytes, s.last_ns)
            };
            iterations.sort_by_key(|i| i.iteration);
            let start = if self.started_ns > 0 {
                self.started_ns
            } else {
                last_ns
            };
            if last_ns > start {
                self.r
                    .set_resource_window(start, last_ns, Some(received_bytes));
            }
            let FtResponder { r, tasks, .. } = *self;
            let mut teardown = r.teardown();
            teardown.hold(tasks);
            teardown.run().await;
            serde_json::to_value(Stats {
                iterations,
                received_bytes,
            })
            .map_err(|e| AppError::internal(format!("file-transfer stats: {e}")))
        })
    }

    fn cancel(self: Box<Self>) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            let FtResponder { r, tasks, .. } = *self;
            let mut teardown = r.teardown();
            teardown.hold(tasks);
            teardown.run().await;
        })
    }
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

/// A canonical example output for the schema fixtures (10 MiB × 3).
pub fn example_output() -> WorkloadOutput {
    let size = 10 * 1024 * 1024u64;
    let it = |index: u32, hash: f64, neg: f64, transfer: f64, t_ms: f64| {
        let elapsed = hash + neg + transfer + 0.6;
        Iteration {
            index,
            file_name: file_name("8f3a5c7e9b1d2f40", index),
            bytes: size,
            ok: true,
            ack_ok: true,
            sha256_match: Some(true),
            hash_ms: Some(hash),
            negotiate_ms: Some(neg),
            transfer_ms: Some(transfer),
            elapsed_ms: Some(elapsed),
            wire_mbps: mbps_over(size, transfer / 1000.0),
            e2e_mbps: mbps_over(size, elapsed / 1000.0),
            done_wait_ms: Some(1.9),
            excluded: false,
            excluded_reason: None,
            error: None,
            t_ms,
        }
    };
    let p = FileTransferParams::default();
    assemble(
        &p,
        vec![
            it(1, 27.9, 14.2, 236.3, 1_320.0),
            it(2, 21.4, 4.1, 208.3, 1_560.0),
            it(3, 21.0, 3.9, 205.1, 1_795.0),
        ],
        3 * size,
        Some(48.2),
        Some((1_760_000_001_000_000_000, 1_760_000_001_800_000_000)),
    )
}

/// The file-transfer example for the schema fixtures.
pub fn examples() -> Vec<super::ModeExample> {
    vec![super::ModeExample {
        mode: "file-transfer",
        params: crate::params::WorkloadParams::FileTransfer(FileTransferParams::default()),
        output: example_output(),
        clock: false,
    }]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn timed(index: u32, wire: f64, ack_ok: bool) -> Iteration {
        Iteration {
            index,
            ok: true,
            ack_ok,
            bytes: 1000,
            wire_mbps: Some(wire),
            e2e_mbps: Some(wire / 2.0),
            hash_ms: Some(1.0),
            negotiate_ms: Some(2.0),
            transfer_ms: Some(50.0),
            ..Iteration::default()
        }
    }

    #[test]
    fn names_carry_the_tag_and_iteration() {
        assert_eq!(
            file_name("00000000000000ab", 3),
            "tb-00000000000000ab-3.bin"
        );
        let names = file_names("ff", 12);
        assert_eq!(names.len(), 12);
        assert_eq!(names[0], "tb-ff-1.bin");
        assert_eq!(names[11], "tb-ff-12.bin");
    }

    #[test]
    fn summary_leaves_out_the_first_of_three_and_unacked() {
        let mut its = vec![
            timed(1, 10.0, true),
            timed(2, 100.0, true),
            timed(3, 200.0, false),
            timed(4, 300.0, true),
        ];
        let m = summarise(&mut its, 4);
        assert!(its[0].excluded && its[2].excluded);
        assert!(!its[1].excluded && !its[3].excluded);
        assert_eq!(m.timed, 2);
        assert_eq!(m.wire_mbps, Some(200.0)); // median of 100 and 300
        assert_eq!(m.e2e_mbps, Some(100.0));
        // With fewer than 3 planned, iteration 1 counts.
        let mut two = vec![timed(1, 10.0, true), timed(2, 30.0, true)];
        let m = summarise(&mut two, 2);
        assert_eq!((m.timed, m.wire_mbps), (2, Some(20.0)));
        // A failed iteration is excluded too.
        let mut failed = vec![Iteration {
            ok: false,
            ..timed(1, 1.0, true)
        }];
        let m = summarise(&mut failed, 1);
        assert_eq!((m.timed, m.wire_mbps), (0, None));
        assert_eq!(failed[0].excluded_reason.as_deref(), Some("failed"));
    }

    #[test]
    fn rates_and_floor_note() {
        assert_eq!(mbps_over(1_000_000, 1.0), Some(8.0));
        assert_eq!(mbps_over(1, 0.0), None);
        assert!(floor_note(Some(45.0)).unwrap().contains("~40 ms"));
        assert!(floor_note(Some(250.0)).is_none());
        assert!(floor_note(None).is_none());
    }

    #[test]
    fn pattern_is_deterministic_and_varied() {
        let mut a = vec![0u8; 4099];
        let mut b = vec![0u8; 4099];
        Pattern::new(PATTERN_SEED).fill(&mut a);
        Pattern::new(PATTERN_SEED).fill(&mut b);
        assert_eq!(a, b);
        let distinct: std::collections::HashSet<u8> = a.iter().copied().collect();
        assert!(distinct.len() > 200, "{} distinct bytes", distinct.len());
        assert_ne!(Pattern::new(0).next_u64(), 0);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("src.bin");
        write_pattern_file(&path, 3 * WRITE_BLOCK as u64 + 17).unwrap();
        let data = std::fs::read(&path).unwrap();
        assert_eq!(data.len(), 3 * WRITE_BLOCK + 17);
        assert_eq!(&data[..4099], &a[..]);
        write_pattern_file(&path, 1024).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), a[..1024].to_vec());
    }

    #[test]
    fn example_output_matches_the_results_type() {
        let out = example_output();
        let r: Results = serde_json::from_value(out.results.clone()).unwrap();
        assert_eq!(r.iterations.len(), 3);
        assert!(r.iterations[0].excluded);
        assert_eq!(r.timed_iterations, 2);
        assert!(r.received_bytes <= r.sent_bytes);
        assert!(out.summary.iter().all(|s| s.value.is_some()));
        assert_eq!(out.series[0].points.len(), 3);
        assert!(out.notes.is_empty(), "{:?}", out.notes);
        let keys: Vec<&str> = out.summary.iter().map(|s| s.key.as_str()).collect();
        assert_eq!(keys, ["wire_mbps", "e2e_mbps", "hash_ms", "negotiate_ms"]);
    }
}
