//! The v3 run report (RFC 027 §7.4), its comparability key, the run store,
//! and normalisation of older reports (§7.5).
//!
//! A [`RunReport`] is one run's complete record. [`write_report`] stores it
//! as `<ts>-<workload>-<run_id>.json` (pretty, never overwriting).
//! [`RunStore`] keeps a directory of them for History and imports files,
//! directories and `.zip`s (GitHub artifact downloads) through
//! [`normalise`], which turns v1, Rust v2 and tsnet-bench reports into v3
//! with `tool: "imported"`.

use std::collections::{HashMap, HashSet};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, PoisonError};
use std::time::SystemTime;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::error::{AppError, ErrorCode};
use crate::params::{
    FileTransferParams, QuicParams, StoreMode, StoreParams, TcpParams, UdpMode, Workload,
    WorkloadParams, WsMode, WsParams,
};
use crate::pathwatch::path_label;
use crate::stats;
use crate::types::{
    Better, BuildProfile, ClockInfo, Distribution, ImportResult, NodeEnv, PathBytes, PathLabel,
    PathSnapshot, PathTimelinePoint, ReportTool, ResourceStats, RunStatus, RunSummary, Series,
    SummaryItem, Topology,
};

/// `schema_version` of reports this engine writes.
pub const SCHEMA_VERSION: u32 = 3;
/// `methodology` of reports this engine writes.
pub const METHODOLOGY: &str = "v3";
/// Imported files (and zip entries) larger than this are skipped: over 100×
/// the largest real report (series are thinned to 5000 points).
const MAX_IMPORT_BYTES: u64 = 16 * 1024 * 1024;
/// A `.zip` larger than this is skipped (its directory is read whole).
const MAX_IMPORT_ZIP_BYTES: u64 = 64 * 1024 * 1024;
/// One import reads at most this many candidate files and zip entries…
const MAX_IMPORT_ENTRIES: usize = 10_000;
/// …and at most this many bytes of them in all (decompressed).
const MAX_IMPORT_TOTAL_BYTES: u64 = 1 << 30;
/// An imported report with more series than this is refused.
const MAX_REPORT_SERIES: usize = 64;
/// An imported report with more points than this (series and path
/// timeline together) is refused.
const MAX_REPORT_POINTS: usize = 64 * crate::workloads::common::MAX_POINTS;
/// How deep [`RunStore::import`] descends into directories.
const MAX_IMPORT_DEPTH: usize = 6;
/// Params fields `compare_key` ignores as run length (§7.4), unless
/// [`unkeyed_params`] keeps one because it defines the measurement.
const COMPARE_KEY_EXCLUDED: [&str; 6] = [
    "workload",
    "duration_secs",
    "count",
    "iterations",
    "warmup_secs",
    "warmup_count",
];

/// The params fields `compare_key` leaves out for these params: the
/// run-length fields, except where they define what is measured:
///
/// - synced-store `burst`: `count` is the burst size, which the echo and
///   write times scale with;
/// - udp `ramp`: `duration_secs` decides how many ×1.5 steps fit, so it caps
///   the result (the warmup comes before the ramp and stays excluded);
/// - file-transfer: `iterations` is left out, but whether iteration 1 is
///   excluded from the medians (≥ 3 iterations) is keyed instead.
pub fn unkeyed_params(params: &WorkloadParams) -> Vec<&'static str> {
    let keep: &[&str] = match params {
        WorkloadParams::SyncedStore(s) if s.mode == StoreMode::Burst => &["count"],
        WorkloadParams::Udp(u) if u.mode == UdpMode::Ramp => &["duration_secs"],
        _ => &[],
    };
    COMPARE_KEY_EXCLUDED
        .into_iter()
        .filter(|k| !keep.contains(k))
        .collect()
}
/// Note for runs where both ends share a machine.
pub const NOTE_NOT_A_LINK: &str = "measures the bridges and netstack, not a link";
/// Note for runs with a debug build on either side.
pub const NOTE_DEBUG_BUILD: &str = "debug build: not comparable with release";
/// Note for runs whose sender could not offer its target rate (§8.2 udp);
/// History excludes them from trends.
pub const NOTE_RATE_NOT_ACHIEVED: &str = "sender could not sustain target";

/// [`RunSummary::flags`] values, in the order [`RunReport::flags`] lists them.
pub mod flag {
    /// Either side ran a debug build.
    pub const DEBUG: &str = "debug";
    /// The run shared a machine with other runs (`overlapping_runs > 0`).
    pub const OVERLAP: &str = "overlap";
    /// The sender could not sustain its target rate.
    pub const RATE_NOT_ACHIEVED: &str = "rate-not-achieved";
    /// Imported from an older report or another tool.
    pub const IMPORTED: &str = "imported";
    /// Both ends on one machine, in two processes.
    pub const SAME_HOST: &str = "same-host";
    /// Both ends in one process (`pair`).
    pub const SAME_PROCESS: &str = "same-process";
    /// The path changed or was partly relayed.
    pub const MIXED_PATH: &str = "mixed-path";
    /// A failed or cancelled run that kept partial results.
    pub const PARTIAL: &str = "partial";
}

// ---------------------------------------------------------------------------
// Report types
// ---------------------------------------------------------------------------

/// `network` of a report: where the bytes went.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct NetworkInfo {
    /// Same process, same host, or separate hosts.
    pub topology: Topology,
    /// Path settle time before the run.
    pub settle_ms: u64,
    /// Path before the run (after settling).
    pub before: PathSnapshot,
    /// Path after the run.
    pub after: PathSnapshot,
    /// Clock offsets, for workloads that map peer time.
    pub clock: Option<ClockInfo>,
    /// 2 Hz path samples during time-based runs.
    pub path_timeline: Vec<PathTimelinePoint>,
    /// RTT under load, from the path sampler.
    pub loaded_rtt_ms: Option<Distribution>,
    /// Sum of both sides' usermetrics byte deltas.
    pub path_bytes: Option<PathBytes>,
    /// DERP bytes ÷ all bytes.
    pub derp_share: Option<f64>,
    /// The verdict (History filters on this, never on `before.connection`).
    pub path_label: PathLabel,
}

impl NetworkInfo {
    /// Network info with nothing measured (a run that failed early):
    /// snapshots record why.
    pub fn unmeasured(topology: Topology, why: &str) -> Self {
        let snap = PathSnapshot {
            at: crate::clock::now_rfc3339(),
            connection: None,
            rtt_ms: None,
            endpoint: None,
            error: Some(why.to_string()),
        };
        Self {
            topology,
            settle_ms: 0,
            before: snap.clone(),
            after: snap,
            clock: None,
            path_timeline: Vec::new(),
            loaded_rtt_ms: None,
            path_bytes: None,
            derp_share: None,
            path_label: PathLabel::Unknown,
        }
    }
}

/// `resources` of a report.
#[derive(Debug, Clone, Copy, PartialEq, Default, Serialize, Deserialize, JsonSchema)]
pub struct Resources {
    /// This side (separate processes).
    pub local: Option<ResourceStats>,
    /// The peer (from `finish.ack.resources`).
    pub peer: Option<ResourceStats>,
    /// Same-process topology: the process plus both sidecars.
    pub combined: Option<ResourceStats>,
}

/// One run, schema v3 (§7.4).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct RunReport {
    /// Always 3.
    pub schema_version: u32,
    /// `^[A-Za-z0-9-]{1,64}$`.
    pub run_id: String,
    /// Suite id when part of a suite.
    pub suite_id: Option<String>,
    /// Producing tool.
    pub tool: ReportTool,
    /// Engine version (`unknown` for imports that did not record one).
    pub tool_version: String,
    /// truffle-core version.
    pub truffle_version: String,
    /// `v3`, or an imported method tag (§7.5).
    pub methodology: String,
    /// Workload.
    pub workload: Workload,
    /// Params label.
    pub label: String,
    /// Outcome.
    pub status: RunStatus,
    /// Why it failed.
    pub error: Option<String>,
    /// Error code when failed.
    pub error_code: Option<crate::ErrorCode>,
    /// RFC 3339.
    pub started_at: String,
    /// RFC 3339.
    pub finished_at: String,
    /// Wall time of the run.
    pub duration_ms: u64,
    /// Copy of `local.git_sha`.
    pub git_sha: Option<String>,
    /// Copy of `local.git_dirty`.
    pub git_dirty: Option<bool>,
    /// 16 hex: runs are trended and compared only within one key.
    pub compare_key: String,
    /// This side.
    pub local: NodeEnv,
    /// The other side (`None` for tsnet-bench imports and early failures).
    pub peer: Option<NodeEnv>,
    /// Path and clock.
    pub network: NetworkInfo,
    /// Params as validated and used by the responder.
    pub params: WorkloadParams,
    /// Headline numbers.
    pub summary: Vec<SummaryItem>,
    /// Workload-specific results (§8).
    pub results: Value,
    /// Charts.
    pub series: Vec<Series>,
    /// CPU and memory.
    pub resources: Resources,
    /// Exempt runs (path, probe) that overlapped this one.
    pub overlapping_runs: u32,
    /// Honesty notes and anything else people should know.
    pub notes: Vec<String>,
}

/// `debug` if either side is debug, `unknown` if either is unknown, else `release`.
pub fn combined_profile(local: BuildProfile, peer: Option<BuildProfile>) -> BuildProfile {
    let all = std::iter::once(local).chain(peer);
    let all: Vec<BuildProfile> = all.collect();
    if all.contains(&BuildProfile::Debug) {
        BuildProfile::Debug
    } else if all.contains(&BuildProfile::Unknown) {
        BuildProfile::Unknown
    } else {
        BuildProfile::Release
    }
}

impl RunReport {
    /// The headline value of `key`, if present.
    pub fn summary_value(&self, key: &str) -> Option<f64> {
        self.summary
            .iter()
            .find(|s| s.key == key)
            .and_then(|s| s.value)
    }

    /// Recompute [`compare_key`] from this report's fields.
    pub fn compute_compare_key(&self) -> String {
        compare_key(
            &self.params,
            self.network.topology,
            self.network.path_label,
            self.local.build_profile,
            self.peer.as_ref().map(|p| p.build_profile),
            &self.methodology,
            self.peer
                .as_ref()
                .and_then(|p| p.host_fingerprint.as_deref()),
        )
    }

    /// The History flags this report calls for ([`flag`], in that order):
    /// a debug build on either side, overlapping runs, a sender that missed
    /// its target ([`NOTE_RATE_NOT_ACHIEVED`]), an import, a same-host or
    /// same-process topology, a mixed path, and a failed or cancelled run
    /// that still carries a measured summary value or series.
    pub fn flags(&self) -> Vec<String> {
        let partial = self.status != RunStatus::Ok
            && (self.summary.iter().any(|s| s.value.is_some())
                || self.series.iter().any(|s| !s.points.is_empty()));
        [
            (flag::DEBUG, self.build_profile() == BuildProfile::Debug),
            (flag::OVERLAP, self.overlapping_runs > 0),
            (
                flag::RATE_NOT_ACHIEVED,
                self.notes.iter().any(|n| n == NOTE_RATE_NOT_ACHIEVED),
            ),
            (flag::IMPORTED, self.tool == ReportTool::Imported),
            (flag::SAME_HOST, self.network.topology == Topology::SameHost),
            (
                flag::SAME_PROCESS,
                self.network.topology == Topology::SameProcess,
            ),
            (
                flag::MIXED_PATH,
                self.network.path_label == PathLabel::Mixed,
            ),
            (flag::PARTIAL, partial),
        ]
        .into_iter()
        .filter(|(_, on)| *on)
        .map(|(f, _)| f.to_string())
        .collect()
    }

    /// Both sides' build profiles combined ([`combined_profile`]).
    pub fn build_profile(&self) -> BuildProfile {
        combined_profile(
            self.local.build_profile,
            self.peer.as_ref().map(|p| p.build_profile),
        )
    }

    /// The honesty notes this report's facts call for (§7.4): not a link,
    /// overlapping runs, debug builds.
    pub fn honesty_notes(&self) -> Vec<String> {
        let mut notes = Vec::new();
        if matches!(
            self.network.topology,
            Topology::SameHost | Topology::SameProcess
        ) {
            notes.push(NOTE_NOT_A_LINK.to_string());
        }
        if self.overlapping_runs > 0 {
            notes.push(format!(
                "shared the machine with {} other run{}",
                self.overlapping_runs,
                if self.overlapping_runs == 1 { "" } else { "s" }
            ));
        }
        if self.build_profile() == BuildProfile::Debug {
            notes.push(NOTE_DEBUG_BUILD.to_string());
        }
        notes
    }

    /// Append a note unless it is already there.
    pub fn add_note(&mut self, note: impl Into<String>) {
        let note = note.into();
        if !self.notes.contains(&note) {
            self.notes.push(note);
        }
    }

    /// Add every [`honesty_note`](Self::honesty_notes) not yet present.
    pub fn apply_honesty_notes(&mut self) {
        for n in self.honesty_notes() {
            self.add_note(n);
        }
    }

    /// The History row.
    pub fn to_summary(&self) -> RunSummary {
        RunSummary {
            flags: self.flags(),
            run_id: self.run_id.clone(),
            suite_id: self.suite_id.clone(),
            workload: self.workload,
            label: self.label.clone(),
            status: self.status,
            error_code: self.error_code,
            started_at: self.started_at.clone(),
            peer_name: self.peer.as_ref().map(|p| p.device_name.clone()),
            path_label: self.network.path_label,
            topology: self.network.topology,
            build_profile: self.build_profile(),
            compare_key: self.compare_key.clone(),
            summary: self.summary.clone(),
            overlapping_runs: self.overlapping_runs,
            tool: self.tool,
        }
    }

    /// `<ts>-<workload>-<run_id>.json`, ts = `started_at` as `%Y%m%dT%H%M%SZ`.
    pub fn file_name(&self) -> String {
        format!(
            "{}-{}-{}.json",
            file_timestamp(&self.started_at),
            self.workload.as_str(),
            self.run_id
        )
    }
}

fn parse_time(s: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|t| t.with_timezone(&chrono::Utc))
}

fn file_timestamp(started_at: &str) -> String {
    parse_time(started_at)
        .unwrap_or_else(chrono::Utc::now)
        .format("%Y%m%dT%H%M%SZ")
        .to_string()
}

/// Validate a run id: `^[A-Za-z0-9-]{1,64}$` (§6 rule 2). It never reaches a
/// path otherwise.
pub fn validate_run_id(id: &str) -> Result<(), AppError> {
    let ok =
        (1..=64).contains(&id.len()) && id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-');
    if ok {
        Ok(())
    } else {
        Err(AppError::invalid_params(format!(
            "run id must be 1–64 letters, digits or hyphens (got “{}”)",
            id.chars().take(80).collect::<String>()
        )))
    }
}

/// Split a report file name into `(ts, workload, run_id)`.
pub fn parse_file_name(name: &str) -> Option<(&str, Workload, &str)> {
    let stem = name.strip_suffix(".json")?;
    if stem.len() < 18 || stem.as_bytes()[16] != b'-' {
        return None;
    }
    let (ts, rest) = (&stem[..16], &stem[17..]);
    for w in Workload::ALL {
        if let Some(id) = rest
            .strip_prefix(w.as_str())
            .and_then(|r| r.strip_prefix('-'))
        {
            if validate_run_id(id).is_ok() {
                return Some((ts, w, id));
            }
        }
    }
    None
}

// ---------------------------------------------------------------------------
// compare_key
// ---------------------------------------------------------------------------

/// Serialise JSON canonically: object keys sorted at every level, no
/// whitespace. Independent of serde_json's `preserve_order` feature.
pub fn canonical_json(v: &Value) -> String {
    let mut out = String::new();
    write_canonical(v, &mut out);
    out
}

fn write_canonical(v: &Value, out: &mut String) {
    match v {
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            out.push('{');
            for (i, k) in keys.into_iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&Value::String(k.clone()).to_string());
                out.push(':');
                write_canonical(&map[k], out);
            }
            out.push('}');
        }
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_canonical(item, out);
            }
            out.push(']');
        }
        other => out.push_str(&other.to_string()),
    }
}

/// §7.4 comparability key: the first 16 hex characters of the SHA-256 of a
/// canonical JSON object holding the workload, the params minus the
/// run-length fields ([`unkeyed_params`]: `duration_secs`, `count`,
/// `iterations` and the warmups, except where one defines the measurement),
/// the topology, the path label, both build profiles, the methodology and
/// the peer's host fingerprint.
pub fn compare_key(
    params: &WorkloadParams,
    topology: Topology,
    path_label: PathLabel,
    local_profile: BuildProfile,
    peer_profile: Option<BuildProfile>,
    methodology: &str,
    peer_fingerprint: Option<&str>,
) -> String {
    let mut p = serde_json::to_value(params).unwrap_or(Value::Null);
    if let Value::Object(map) = &mut p {
        for k in unkeyed_params(params) {
            map.remove(k);
        }
        if let WorkloadParams::FileTransfer(f) = params {
            map.insert(
                "first_iteration_excluded".into(),
                Value::Bool(f.iterations >= crate::workloads::file_transfer::EXCLUDE_FIRST_FROM),
            );
        }
    }
    let obj = json!({
        "workload": params.workload().as_str(),
        "params": p,
        "topology": topology,
        "path_label": path_label,
        "build_profiles": [local_profile, peer_profile],
        "methodology": methodology,
        "peer_host_fingerprint": peer_fingerprint,
    });
    let digest = Sha256::digest(canonical_json(&obj).as_bytes());
    let mut hex = hex::encode(digest);
    hex.truncate(16);
    hex
}

// ---------------------------------------------------------------------------
// Writing
// ---------------------------------------------------------------------------

/// Write `report` into `dir` as [`RunReport::file_name`]: pretty JSON plus a
/// newline, never overwriting (the file appears atomically).
pub fn write_report(dir: &Path, report: &RunReport) -> Result<PathBuf, AppError> {
    validate_run_id(&report.run_id)?;
    std::fs::create_dir_all(dir)
        .map_err(|e| AppError::io(format!("create {}", dir.display()), &e))?;
    let path = dir.join(report.file_name());
    let mut tmp = tempfile::Builder::new()
        .prefix(".tmp-report-")
        .tempfile_in(dir)
        .map_err(|e| AppError::io("create a temporary report file", &e))?;
    serde_json::to_writer_pretty(&mut tmp, report)
        .map_err(|e| AppError::internal(format!("serialise report: {e}")))?;
    tmp.write_all(b"\n")
        .map_err(|e| AppError::io("write report", &e))?;
    tmp.persist_noclobber(&path).map_err(|e| {
        if e.error.kind() == std::io::ErrorKind::AlreadyExists {
            AppError::new(
                ErrorCode::Io,
                format!("a report already exists at {}", path.display()),
            )
        } else {
            AppError::io(format!("write {}", path.display()), &e.error)
        }
    })?;
    Ok(path)
}

// ---------------------------------------------------------------------------
// Run store
// ---------------------------------------------------------------------------

type CacheKey = (u64, Option<SystemTime>);

/// The run history: one file per run in a directory (`<data_dir>/runs/`).
#[derive(Debug)]
pub struct RunStore {
    dir: PathBuf,
    cache: Mutex<HashMap<PathBuf, (CacheKey, RunSummary)>>,
}

impl RunStore {
    /// A store over `dir` (created on first save).
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            cache: Mutex::new(HashMap::new()),
        }
    }

    /// The directory.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    fn report_files(&self) -> Vec<(PathBuf, String)> {
        let Ok(entries) = std::fs::read_dir(&self.dir) else {
            return Vec::new();
        };
        entries
            .flatten()
            .filter_map(|e| {
                let name = e.file_name().to_string_lossy().to_string();
                let run_id = parse_file_name(&name)?.2.to_string();
                Some((e.path(), run_id))
            })
            .collect()
    }

    fn find(&self, run_id: &str) -> Option<PathBuf> {
        self.report_files()
            .into_iter()
            .find(|(_, id)| id == run_id)
            .map(|(p, _)| p)
    }

    /// Whether a run with this id is stored.
    pub fn contains(&self, run_id: &str) -> bool {
        validate_run_id(run_id).is_ok() && self.find(run_id).is_some()
    }

    /// Save a report; fails if the run id is already stored.
    pub fn save(&self, report: &RunReport) -> Result<PathBuf, AppError> {
        validate_run_id(&report.run_id)?;
        if self.find(&report.run_id).is_some() {
            return Err(AppError::invalid_params(format!(
                "run {} is already stored",
                report.run_id
            )));
        }
        write_report(&self.dir, report)
    }

    /// Every stored run, newest first. Unreadable files are skipped.
    pub fn list(&self) -> Vec<RunSummary> {
        let files = self.report_files();
        let mut cache = self.cache.lock().unwrap_or_else(PoisonError::into_inner);
        cache.retain(|path, _| files.iter().any(|(p, _)| p == path));
        let mut out = Vec::new();
        for (path, _) in files {
            let Ok(meta) = std::fs::metadata(&path) else {
                continue;
            };
            let key: CacheKey = (meta.len(), meta.modified().ok());
            if let Some((k, summary)) = cache.get(&path) {
                if *k == key {
                    out.push(summary.clone());
                    continue;
                }
            }
            let Ok(report) = read_report_file(&path) else {
                continue;
            };
            let summary = report.to_summary();
            cache.insert(path, (key, summary.clone()));
            out.push(summary);
        }
        out.sort_by(|a, b| {
            let ta = parse_time(&a.started_at);
            let tb = parse_time(&b.started_at);
            tb.cmp(&ta)
                .then_with(|| b.started_at.cmp(&a.started_at))
                .then_with(|| a.run_id.cmp(&b.run_id))
        });
        out
    }

    /// One stored report.
    pub fn get(&self, run_id: &str) -> Result<RunReport, AppError> {
        validate_run_id(run_id)?;
        let path = self
            .find(run_id)
            .ok_or_else(|| AppError::invalid_params(format!("no stored run {run_id}")))?;
        read_report_file(&path).map_err(AppError::internal)
    }

    /// Delete one stored report.
    pub fn delete(&self, run_id: &str) -> Result<(), AppError> {
        validate_run_id(run_id)?;
        let path = self
            .find(run_id)
            .ok_or_else(|| AppError::invalid_params(format!("no stored run {run_id}")))?;
        std::fs::remove_file(&path)
            .map_err(|e| AppError::io(format!("delete {}", path.display()), &e))?;
        self.cache
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&path);
        Ok(())
    }

    /// Import a file, a directory (recursively) or a `.zip`: every report in
    /// it is normalised to v3 and stored. Already-stored runs, non-reports
    /// and anything that fails to normalise count as skipped.
    ///
    /// Reports are stored one by one as they are read (memory holds one
    /// report at a time), within the import limits (files and entries,
    /// total bytes; once reached, the rest is not read and counts as one
    /// skipped item).
    pub fn import(&self, path: &Path) -> ImportResult {
        let mut result = ImportResult::default();
        let mut stored: HashSet<String> =
            self.report_files().into_iter().map(|(_, id)| id).collect();
        for_each_report(path, &mut |source, outcome| {
            match outcome.and_then(|r| {
                if stored.contains(&r.run_id) {
                    return Err(format!("run {} is already stored", r.run_id));
                }
                // `write_report` never overwrites, so a report saved by
                // someone else meanwhile is not clobbered.
                write_report(&self.dir, &r).map_err(|e| e.message)?;
                stored.insert(r.run_id);
                Ok(())
            }) {
                Ok(()) => result.imported += 1,
                Err(why) => {
                    tracing::debug!(source = %source, %why, "skipped on import");
                    result.skipped += 1;
                }
            }
        });
        result
    }
}

fn read_report_file(path: &Path) -> Result<RunReport, String> {
    let bytes = std::fs::read(path).map_err(|e| format!("{}: {e}", path.display()))?;
    serde_json::from_slice(&bytes).map_err(|e| format!("{}: {e}", path.display()))
}

/// Read every report under `path` (a JSON file, a directory searched
/// recursively for `.json` and `.zip`, or a `.zip`) and normalise each to v3.
/// Returns `(source, result)` per candidate file; the CLI's `report` command
/// prints these. See [`for_each_report`] for the limits.
pub fn read_reports(path: &Path) -> Vec<(String, Result<RunReport, String>)> {
    let mut out = Vec::new();
    for_each_report(path, &mut |source, result| out.push((source, result)));
    out
}

/// [`read_reports`], handing each `(source, result)` to `sink` as soon as
/// it is read instead of collecting them ([`RunStore::import`] stores each
/// and drops it). One call reads at most 10 000 files and zip entries and
/// 1 GiB of them in all, each at most 16 MiB; past a limit the rest is not
/// read and `sink` gets one "import limit reached" error.
pub fn for_each_report(path: &Path, sink: &mut dyn FnMut(String, Result<RunReport, String>)) {
    let mut budget = ImportBudget::default();
    collect(path, 0, &mut budget, sink);
}

/// What one import may still read.
#[derive(Debug, Default)]
struct ImportBudget {
    entries: usize,
    bytes: u64,
    exhausted: bool,
}

impl ImportBudget {
    /// Count one candidate; false (and reported once) past the entry limit.
    fn take_entry(&mut self, sink: &mut dyn FnMut(String, Result<RunReport, String>)) -> bool {
        if self.exhausted {
            return false;
        }
        if self.entries >= MAX_IMPORT_ENTRIES {
            self.exhaust(
                sink,
                format!("more than {MAX_IMPORT_ENTRIES} files and zip entries"),
            );
            return false;
        }
        self.entries += 1;
        true
    }

    /// Bytes still allowed.
    fn left(&self) -> u64 {
        MAX_IMPORT_TOTAL_BYTES.saturating_sub(self.bytes)
    }

    /// Charge `n` bytes read; false (and reported once) past the byte limit.
    fn charge(&mut self, n: u64, sink: &mut dyn FnMut(String, Result<RunReport, String>)) -> bool {
        self.bytes = self.bytes.saturating_add(n);
        if self.bytes > MAX_IMPORT_TOTAL_BYTES {
            self.exhaust(
                sink,
                format!("more than {} MiB in all", MAX_IMPORT_TOTAL_BYTES >> 20),
            );
            return false;
        }
        true
    }

    fn exhaust(&mut self, sink: &mut dyn FnMut(String, Result<RunReport, String>), why: String) {
        if !self.exhausted {
            self.exhausted = true;
            sink(
                "import".into(),
                Err(format!(
                    "import limit reached ({why}); the rest was not read"
                )),
            );
        }
    }
}

fn has_ext(path: &Path, ext: &str) -> bool {
    path.extension()
        .is_some_and(|e| e.to_string_lossy().eq_ignore_ascii_case(ext))
}

fn collect(
    path: &Path,
    depth: usize,
    budget: &mut ImportBudget,
    out: &mut dyn FnMut(String, Result<RunReport, String>),
) {
    if budget.exhausted {
        return;
    }
    let source = path.display().to_string();
    let meta = match std::fs::symlink_metadata(path) {
        Ok(m) => m,
        Err(e) => {
            out(source, Err(e.to_string()));
            return;
        }
    };
    if meta.is_dir() {
        if depth >= MAX_IMPORT_DEPTH {
            return;
        }
        let Ok(entries) = std::fs::read_dir(path) else {
            return;
        };
        let mut children: Vec<PathBuf> = entries.flatten().map(|e| e.path()).collect();
        children.sort();
        for child in children {
            let is_dir = std::fs::symlink_metadata(&child).is_ok_and(|m| m.is_dir());
            if is_dir || has_ext(&child, "json") || has_ext(&child, "zip") {
                collect(&child, depth + 1, budget, out);
            }
        }
        return;
    }
    if !meta.is_file() {
        return;
    }
    if has_ext(path, "zip") {
        if meta.len() > MAX_IMPORT_ZIP_BYTES {
            out(source, Err("file too large".into()));
            return;
        }
        // A zip itself is not charged; its entries are, as read.
        collect_zip(path, budget, out);
        return;
    }
    if !budget.take_entry(out) {
        return;
    }
    if meta.len() > MAX_IMPORT_BYTES {
        out(source, Err("file too large".into()));
        return;
    }
    if !budget.charge(meta.len(), out) {
        return;
    }
    let result = std::fs::read(path)
        .map_err(|e| e.to_string())
        .and_then(|bytes| normalise_bytes(&bytes, &file_name_of(path)));
    out(source, result);
}

fn file_name_of(path: &Path) -> String {
    path.file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_default()
}

fn collect_zip(
    path: &Path,
    budget: &mut ImportBudget,
    out: &mut dyn FnMut(String, Result<RunReport, String>),
) {
    let source = path.display().to_string();
    let archive = std::fs::File::open(path)
        .map_err(|e| e.to_string())
        .and_then(|f| zip::ZipArchive::new(f).map_err(|e| e.to_string()));
    let mut archive = match archive {
        Ok(a) => a,
        Err(e) => {
            out(source, Err(e));
            return;
        }
    };
    for i in 0..archive.len() {
        if budget.exhausted {
            return;
        }
        let mut entry = match archive.by_index(i) {
            Ok(e) => e,
            Err(e) => {
                out(format!("{source}#{i}"), Err(e.to_string()));
                continue;
            }
        };
        let name = entry.name().to_string();
        if entry.is_dir() || !name.to_ascii_lowercase().ends_with(".json") {
            continue;
        }
        if !budget.take_entry(out) {
            return;
        }
        let entry_source = format!("{source}!{name}");
        if entry.size() > MAX_IMPORT_BYTES {
            out(entry_source, Err("entry too large".into()));
            continue;
        }
        // The declared size may lie: read at most what is allowed, plus one
        // byte to tell "too large" from "exactly at the limit".
        let cap = MAX_IMPORT_BYTES.min(budget.left()) + 1;
        let mut bytes = Vec::new();
        let read = (&mut entry).take(cap).read_to_end(&mut bytes);
        if !budget.charge(bytes.len() as u64, out) {
            return;
        }
        let result = read.map_err(|e| e.to_string()).and_then(|_| {
            if bytes.len() as u64 > MAX_IMPORT_BYTES {
                return Err("entry too large".into());
            }
            let base = name.rsplit('/').next().unwrap_or(&name).to_string();
            normalise_bytes(&bytes, &base)
        });
        out(entry_source, result);
    }
}

// ---------------------------------------------------------------------------
// Normalisation (§7.5)
// ---------------------------------------------------------------------------

/// Parse and normalise one report file's bytes. `source_name` is recorded in
/// imported results. A v3 report is parsed straight into [`RunReport`] (no
/// JSON tree first); every report must stay within the series and point
/// limits (real reports thin each series to 5000 points).
pub fn normalise_bytes(bytes: &[u8], source_name: &str) -> Result<RunReport, String> {
    /// Just the version: other fields are skipped without being built.
    #[derive(Deserialize)]
    struct Peek {
        #[serde(default)]
        schema_version: Option<Value>,
    }
    let v3 = serde_json::from_slice::<Peek>(bytes)
        .ok()
        .and_then(|p| p.schema_version)
        .and_then(|v| v.as_u64())
        == Some(3);
    let report = if v3 {
        let report: RunReport =
            serde_json::from_slice(bytes).map_err(|e| format!("invalid v3 report: {e}"))?;
        validate_run_id(&report.run_id).map_err(|e| e.message)?;
        report
    } else {
        let value: Value = serde_json::from_slice(bytes).map_err(|e| format!("not JSON: {e}"))?;
        normalise(value, bytes, source_name)?
    };
    check_report_size(&report)?;
    Ok(report)
}

/// Refuse a report with more series or points than any run produces.
fn check_report_size(r: &RunReport) -> Result<(), String> {
    if r.series.len() > MAX_REPORT_SERIES {
        return Err(format!(
            "{} series (at most {MAX_REPORT_SERIES})",
            r.series.len()
        ));
    }
    let points = r
        .series
        .iter()
        .map(|s| s.points.len())
        .sum::<usize>()
        .saturating_add(r.network.path_timeline.len());
    if points > MAX_REPORT_POINTS {
        return Err(format!("{points} points (at most {MAX_REPORT_POINTS})"));
    }
    Ok(())
}

/// Normalise any known report to v3:
///
/// - `schema_version: 3`: parsed as is;
/// - no `schema_version`: v1;
/// - `schema_version: 2` with `workload: "native-tsnet-tcp"`: tsnet-bench;
/// - `schema_version: 2` otherwise: Rust v2.
///
/// Imported reports get `tool: "imported"`, a methodology tag
/// (`v2-transport`, `v2-file-transfer`, `v2-request-reply`,
/// `v2-synced-store`, `native-tsnet`), MB/s converted to Mb/s, and the run id
/// `imported-<first 16 hex of sha256(raw bytes)>`, so importing the same
/// file twice is detected.
pub fn normalise(value: Value, raw: &[u8], source_name: &str) -> Result<RunReport, String> {
    let obj = value.as_object().ok_or("not a JSON object")?;
    match obj.get("schema_version").and_then(Value::as_u64) {
        Some(3) => {
            let report: RunReport =
                serde_json::from_value(value).map_err(|e| format!("invalid v3 report: {e}"))?;
            validate_run_id(&report.run_id).map_err(|e| e.message)?;
            Ok(report)
        }
        None if obj.contains_key("schema_version") => Err("schema_version is not a number".into()),
        None => legacy(&value, raw, source_name, Source::V1),
        Some(2) if obj.get("workload").and_then(Value::as_str) == Some("native-tsnet-tcp") => {
            legacy(&value, raw, source_name, Source::TsnetBench)
        }
        Some(2) => legacy(&value, raw, source_name, Source::V2),
        Some(v) => Err(format!("unsupported schema_version {v}")),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Source {
    V1,
    V2,
    TsnetBench,
}

impl Source {
    fn as_str(self) -> &'static str {
        match self {
            Self::V1 => "v1",
            Self::V2 => "v2",
            Self::TsnetBench => "tsnet-bench",
        }
    }
}

fn f(v: &Value, key: &str) -> Option<f64> {
    v.get(key).and_then(Value::as_f64)
}

fn u(v: &Value, key: &str) -> Option<u64> {
    v.get(key).and_then(Value::as_u64)
}

fn s<'a>(v: &'a Value, key: &str) -> Option<&'a str> {
    v.get(key).and_then(Value::as_str)
}

fn u32_or(v: &Value, key: &str, default: u32) -> u32 {
    u(v, key)
        .and_then(|n| u32::try_from(n).ok())
        .unwrap_or(default)
}

fn item(key: &str, label: &str, value: Option<f64>, unit: &str, better: Better) -> SummaryItem {
    SummaryItem::new(key, label, value, unit, better)
}

/// A v1/v2 distribution's percentile, nulled below the §7.4 sample threshold
/// when the sample count is known.
fn legacy_pct(dist: &Value, key: &str, n: Option<u64>, (num, den): (u64, u64)) -> Option<f64> {
    let n = u(dist, "samples").or(n);
    if n.is_some_and(|n| n < stats::min_samples(num, den)) {
        return None;
    }
    f(dist, key)
}

/// A legacy run claiming to have taken longer than this (about 115 days)
/// keeps no elapsed time.
const MAX_LEGACY_ELAPSED_MS: u64 = 10_000_000_000;

struct Mapped {
    workload: Workload,
    params: WorkloadParams,
    methodology: &'static str,
    label: String,
    summary: Vec<SummaryItem>,
    valid: Option<bool>,
    elapsed_ms: Option<u64>,
    notes: Vec<String>,
}

fn map_legacy(
    source: Source,
    name: &str,
    params: &Value,
    results: &Value,
) -> Result<Mapped, String> {
    let fmt = crate::params::fmt_bytes;
    match (source, name) {
        (Source::TsnetBench, _) => {
            let samples = results
                .get("samples")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            let bytes: f64 = samples.iter().filter_map(|x| f(x, "bytes")).sum();
            let ms: f64 = samples.iter().filter_map(|x| f(x, "transfer_ms")).sum();
            let goodput = (ms > 0.0).then(|| bytes * 8.0 / (ms / 1000.0) / 1e6);
            let connect: Vec<f64> = samples.iter().filter_map(|x| f(x, "connect_ms")).collect();
            let size = u(params, "bytes").unwrap_or(0);
            Ok(Mapped {
                workload: Workload::Tcp,
                params: WorkloadParams::Tcp(TcpParams::default()),
                methodology: "native-tsnet",
                label: format!(
                    "Native tsnet TCP · {} × {}",
                    fmt(size),
                    u(params, "iterations").unwrap_or(samples.len() as u64)
                ),
                summary: vec![
                    item("goodput_mbps", "Goodput", goodput, "Mb/s", Better::Higher),
                    item(
                        "connect_ms",
                        "Connect",
                        stats::median(&connect),
                        "ms",
                        Better::Lower,
                    ),
                ],
                valid: results.get("valid").and_then(Value::as_bool),
                elapsed_ms: (ms > 0.0).then_some(ms as u64),
                notes: vec!["native Go/tsnet baseline".into()],
            })
        }
        (_, "transport") => {
            let protocol = s(params, "protocol").unwrap_or("tcp");
            let concurrency = u32_or(params, "concurrency", 1);
            let size = u(params, "bytes").unwrap_or(0);
            let (workload, wparams, title) = if protocol == "quic" {
                (
                    Workload::Quic,
                    WorkloadParams::Quic(QuicParams {
                        streams: concurrency,
                        ..QuicParams::default()
                    }),
                    "QUIC",
                )
            } else {
                (
                    Workload::Tcp,
                    WorkloadParams::Tcp(TcpParams {
                        streams: concurrency,
                        ..TcpParams::default()
                    }),
                    "TCP",
                )
            };
            let fixed = f(results, "aggregate_mb_per_sec").map(|m| m * 8.0);
            let connect = results
                .get("connect_ms")
                .and_then(|d| legacy_pct(d, "p50", None, stats::P50));
            Ok(Mapped {
                workload,
                params: wparams,
                methodology: "v2-transport",
                label: format!(
                    "{title} fixed-size transfers (v2) · {} × {} · concurrency {concurrency}",
                    fmt(size),
                    u(params, "iterations").unwrap_or(0)
                ),
                summary: vec![
                    item(
                        "fixed_size_mbps",
                        "Fixed-size transfer rate (v2 method)",
                        fixed,
                        "Mb/s",
                        Better::Higher,
                    ),
                    item("connect_ms", "Connect", connect, "ms", Better::Lower),
                ],
                valid: results.get("valid").and_then(Value::as_bool),
                elapsed_ms: f(results, "elapsed_secs").map(|s| (s * 1000.0) as u64),
                notes: Vec::new(),
            })
        }
        (_, "file-transfer") => {
            let size = u(params, "size_bytes").unwrap_or(0);
            let iterations = u32_or(params, "iterations", 1);
            let mbs = results
                .get("throughput_mb_per_sec")
                .and_then(|d| f(d, "p50"))
                .or_else(|| f(results, "mean_throughput_mb_per_sec"));
            Ok(Mapped {
                workload: Workload::FileTransfer,
                params: WorkloadParams::FileTransfer(FileTransferParams {
                    size_bytes: size,
                    iterations,
                }),
                methodology: "v2-file-transfer",
                label: format!("File transfer (v2) · {} × {iterations}", fmt(size)),
                summary: vec![item(
                    "e2e_mbps",
                    "End to end",
                    mbs.map(|m| m * 8.0),
                    "Mb/s",
                    Better::Higher,
                )],
                valid: results.get("valid").and_then(Value::as_bool),
                elapsed_ms: None,
                notes: vec!["v2 throughput includes hashing and the accept wait; it maps to e2e_mbps, not wire_mbps".into()],
            })
        }
        (_, "request-reply") => {
            let count = u32_or(params, "count", 0);
            let payload = u32_or(params, "payload_bytes", 0);
            let successes = u(results, "successes");
            let rtt = results.get("rtt_ms").cloned().unwrap_or(Value::Null);
            let elapsed = f(results, "total_elapsed_secs");
            let rate = match (successes, elapsed) {
                (Some(n), Some(s)) if s > 0.0 => Some(n as f64 / s),
                _ => None,
            };
            Ok(Mapped {
                workload: Workload::Ws,
                params: WorkloadParams::Ws(WsParams {
                    mode: WsMode::Rr,
                    payload_bytes: payload,
                    count,
                    concurrency: 1,
                    warmup_count: u32_or(params, "warmup", 0),
                    ..WsParams::default()
                }),
                methodology: "v2-request-reply",
                label: format!(
                    "WebSocket request/reply (v2) · {} · {count} requests",
                    fmt(u64::from(payload))
                ),
                summary: vec![
                    item(
                        "rtt_p50_ms",
                        "RTT p50",
                        legacy_pct(&rtt, "p50", successes, stats::P50),
                        "ms",
                        Better::Lower,
                    ),
                    item(
                        "rtt_p99_ms",
                        "RTT p99",
                        legacy_pct(&rtt, "p99", successes, stats::P99),
                        "ms",
                        Better::Lower,
                    ),
                    item("msgs_per_sec", "Requests", rate, "msg/s", Better::Higher),
                ],
                valid: results
                    .get("valid")
                    .and_then(Value::as_bool)
                    .or_else(|| u(results, "timeouts").map(|t| t == 0)),
                elapsed_ms: elapsed.map(|s| (s * 1000.0) as u64),
                notes: Vec::new(),
            })
        }
        (_, "synced-store") => {
            let ops = u32_or(params, "ops", 0);
            let scenario = s(params, "scenario").unwrap_or("unknown");
            let converged = match (
                u(results, "final_local_count"),
                u(results, "observed_final_count_on_peer"),
            ) {
                (Some(a), Some(b)) => Some(a == b),
                _ => None,
            };
            Ok(Mapped {
                workload: Workload::SyncedStore,
                params: WorkloadParams::SyncedStore(StoreParams {
                    mode: StoreMode::Burst,
                    count: ops,
                    ..StoreParams::default()
                }),
                methodology: "v2-synced-store",
                label: format!("SyncedStore {scenario} (v2) · {ops} writes"),
                summary: vec![item(
                    "convergence_ms",
                    "Convergence (v2 method: timed after the local writes)",
                    f(results, "convergence_ms"),
                    "ms",
                    Better::Lower,
                )],
                valid: results.get("valid").and_then(Value::as_bool).or(converged),
                elapsed_ms: None,
                notes: Vec::new(),
            })
        }
        (_, other) => Err(format!("unknown legacy workload “{other}”")),
    }
}

/// A v2 `diagnostics()` object (`{"topology", "ping": {"path","rtt_ms","endpoint"} | {"error"}}`).
fn v2_snapshot(diag: Option<&Value>, at: &str) -> PathSnapshot {
    let ping = diag.and_then(|d| d.get("ping"));
    PathSnapshot {
        at: at.to_string(),
        connection: ping.and_then(|p| s(p, "path")).map(str::to_string),
        rtt_ms: ping.and_then(|p| f(p, "rtt_ms")),
        endpoint: ping.and_then(|p| s(p, "endpoint")).map(str::to_string),
        error: ping.and_then(|p| s(p, "error")).map(str::to_string),
    }
}

/// A tsnet-bench probe: an `ipnstate.PingResult` object or an error string.
fn tsnet_snapshot(probe: Option<&Value>, at: &str) -> PathSnapshot {
    let mut snap = PathSnapshot {
        at: at.to_string(),
        connection: None,
        rtt_ms: None,
        endpoint: None,
        error: None,
    };
    match probe {
        Some(Value::String(e)) => snap.error = Some(e.clone()),
        Some(p @ Value::Object(_)) => {
            if let Some(err) = s(p, "Err").filter(|e| !e.is_empty()) {
                snap.error = Some(err.to_string());
                return snap;
            }
            snap.rtt_ms = f(p, "LatencySeconds").map(|s| s * 1000.0);
            let endpoint = s(p, "Endpoint").filter(|e| !e.is_empty());
            let relay = s(p, "PeerRelay").filter(|e| !e.is_empty());
            let region = s(p, "DERPRegionCode").filter(|e| !e.is_empty());
            snap.endpoint = endpoint.map(str::to_string);
            snap.connection = Some(if endpoint.is_some() {
                "direct".to_string()
            } else if let Some(r) = relay {
                format!("peer-relay:{r}")
            } else if let Some(r) = region {
                format!("relay:{r}")
            } else if u(p, "DERPRegionID").is_some_and(|id| id != 0) {
                "relay".to_string()
            } else {
                "unknown".to_string()
            });
        }
        _ => {}
    }
    snap
}

fn normalise_time(ts: &str) -> String {
    parse_time(ts)
        .map(|t| t.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
        .unwrap_or_else(|| ts.to_string())
}

fn legacy(
    value: &Value,
    raw: &[u8],
    source_name: &str,
    source: Source,
) -> Result<RunReport, String> {
    let name = s(value, "workload").ok_or("no workload")?;
    let params = value.get("params").cloned().unwrap_or(Value::Null);
    let results = value.get("results").cloned().unwrap_or(Value::Null);
    let mapped = map_legacy(source, name, &params, &results)?;
    let started_at = normalise_time(s(value, "timestamp_utc").unwrap_or_default());
    if parse_time(&started_at).is_none() {
        return Err("no valid timestamp_utc".into());
    }
    // An absurd elapsed time (a corrupt or crafted file) is dropped, never
    // trusted: the date arithmetic below must not overflow.
    let elapsed_ms = mapped.elapsed_ms.filter(|ms| *ms <= MAX_LEGACY_ELAPSED_MS);
    let finished_at = parse_time(&started_at)
        .zip(elapsed_ms)
        .and_then(|(t, ms)| {
            let delta = chrono::TimeDelta::try_milliseconds(i64::try_from(ms).ok()?)?;
            t.checked_add_signed(delta)
        })
        .map_or_else(
            || started_at.clone(),
            |t| t.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        );

    let mut git_sha = s(value, "git_sha").map(str::to_string);
    let mut git_dirty = value.get("git_dirty").and_then(Value::as_bool);
    let mut notes = vec![format!(
        "imported from a {} report ({source_name}); methodology {}",
        match source {
            Source::V1 => "v1 (no schema_version)",
            Source::V2 => "Rust v2",
            Source::TsnetBench => "tsnet-bench",
        },
        mapped.methodology
    )];
    notes.extend(mapped.notes.iter().cloned());

    let (local, topology, before, after) = match source {
        Source::V1 => {
            let unknown = PathSnapshot {
                at: started_at.clone(),
                connection: None,
                rtt_ms: None,
                endpoint: None,
                error: None,
            };
            notes.push("v1 reports came from the in-process pair harness".into());
            (
                imported_env(
                    "truffle-bench v1",
                    None,
                    None,
                    None,
                    BuildProfile::Unknown,
                    None,
                ),
                Topology::SameProcess,
                unknown.clone(),
                unknown,
            )
        }
        Source::V2 => {
            let env = value.get("environment").cloned().unwrap_or(Value::Null);
            let network = value.get("network").cloned().unwrap_or(Value::Null);
            let profile = match env.get("debug_assertions").and_then(Value::as_bool) {
                Some(true) => BuildProfile::Debug,
                Some(false) => BuildProfile::Release,
                None => BuildProfile::Unknown,
            };
            let recorded = s(&network, "topology").unwrap_or("unknown");
            let topology = if recorded == "same-host" {
                notes.push("v2 \"same-host\" runs came from the in-process pair harness".into());
                Topology::SameProcess
            } else {
                notes.push(format!(
                    "v2 recorded topology “{recorded}”; the two processes may have shared a host"
                ));
                Topology::SeparateHosts
            };
            let mut local = imported_env(
                "truffle-bench v2",
                s(&env, "os"),
                s(&env, "arch"),
                u(&env, "logical_cpus").and_then(|n| u32::try_from(n).ok()),
                profile,
                s(&env, "rustc").map(str::to_string),
            );
            local.sidecar_sha256 = s(&env, "sidecar_sha256").map(str::to_string);
            (
                local,
                topology,
                v2_snapshot(network.get("before"), &started_at),
                v2_snapshot(network.get("after"), &finished_at),
            )
        }
        Source::TsnetBench => {
            let settings = value
                .pointer("/build/Settings")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            let setting = |key: &str| {
                settings
                    .iter()
                    .find(|kv| s(kv, "Key") == Some(key))
                    .and_then(|kv| s(kv, "Value"))
                    .map(str::to_string)
            };
            git_sha = git_sha.or_else(|| setting("vcs.revision"));
            git_dirty = git_dirty.or_else(|| setting("vcs.modified").map(|v| v == "true"));
            if let Some(go) = s(value, "go") {
                notes.push(format!("built with {go}"));
            }
            let recorded = s(value, "topology").unwrap_or("unknown");
            notes.push(format!(
                "tsnet-bench recorded topology “{recorded}”; the two processes may have shared a host"
            ));
            let mut env = imported_env(
                "tsnet-bench",
                s(value, "os"),
                s(value, "arch"),
                None,
                BuildProfile::Release,
                None,
            );
            env.tool_version = s(value, "go").unwrap_or("unknown").to_string();
            (
                env,
                Topology::SeparateHosts,
                tsnet_snapshot(value.get("before"), &started_at),
                tsnet_snapshot(value.get("after"), &finished_at),
            )
        }
    };
    let mut local = local;
    local.git_sha = git_sha.clone();
    local.git_dirty = git_dirty;

    let verdict = path_label(None, &[], &before, &after);
    notes.extend(verdict.notes);

    let digest = hex::encode(Sha256::digest(raw));
    let run_id = format!("imported-{}", &digest[..16]);
    let (status, error) = match mapped.valid {
        Some(false) => (
            RunStatus::Failed,
            Some("the imported run was marked invalid".to_string()),
        ),
        _ => (RunStatus::Ok, None),
    };
    let network = NetworkInfo {
        topology,
        settle_ms: 0,
        before,
        after,
        clock: None,
        path_timeline: Vec::new(),
        loaded_rtt_ms: None,
        path_bytes: None,
        derp_share: None,
        path_label: verdict.label,
    };
    let mut report = RunReport {
        schema_version: SCHEMA_VERSION,
        run_id,
        suite_id: None,
        tool: ReportTool::Imported,
        tool_version: "unknown".into(),
        truffle_version: "unknown".into(),
        methodology: mapped.methodology.to_string(),
        workload: mapped.workload,
        label: mapped.label,
        status,
        error,
        error_code: None,
        started_at,
        finished_at,
        duration_ms: elapsed_ms.unwrap_or(0),
        git_sha,
        git_dirty,
        compare_key: String::new(),
        local,
        peer: None,
        network,
        params: mapped.params,
        summary: mapped.summary,
        results: json!({
            "source": source.as_str(),
            "source_file": source_name,
            "params": params,
            "results": results,
        }),
        series: Vec::new(),
        resources: Resources::default(),
        overlapping_runs: 0,
        notes,
    };
    report.compare_key = report.compute_compare_key();
    report.apply_honesty_notes();
    Ok(report)
}

fn imported_env(
    tool: &str,
    os: Option<&str>,
    arch: Option<&str>,
    logical_cpus: Option<u32>,
    build_profile: BuildProfile,
    rustc: Option<String>,
) -> NodeEnv {
    NodeEnv {
        device_name: "unknown".into(),
        os: os.unwrap_or("unknown").to_string(),
        arch: arch.unwrap_or("unknown").to_string(),
        logical_cpus,
        tool: tool.to_string(),
        tool_version: "unknown".into(),
        truffle_version: "unknown".into(),
        build_profile,
        rustc,
        git_sha: None,
        git_dirty: None,
        sidecar_path: None,
        sidecar_sha256: None,
        tailscale_ip: None,
        host_fingerprint: None,
        host_fingerprint_weak: false,
        on_battery: None,
        app_foreground_pct: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::params::{Direction, HttpParams, PathParams, StreamMode};
    use crate::types::Better;

    const V1_STORE: &str = include_str!("../tests/fixtures/v1/20260913T222009Z-synced-store.json");
    const V1_RR: &str = include_str!("../tests/fixtures/v1/20260913T222014Z-request-reply.json");
    const V2_TRANSPORT: &str = include_str!(
        "../tests/fixtures/v2/20261012T101500Z-transport-6f1c2a9e-0b7d-4c55-9a8e-2d3f4b5c6d7e.json"
    );
    const TSNET: &str = include_str!("../tests/fixtures/v2/tsnet-bench-20261012T103000Z.json");

    fn env(profile: BuildProfile, fp: Option<&str>) -> NodeEnv {
        let mut e = imported_env(
            "truffle-bench-cli",
            Some("macos"),
            Some("aarch64"),
            Some(10),
            profile,
            None,
        );
        e.device_name = "studio".into();
        e.tool_version = crate::TOOL_VERSION.into();
        e.truffle_version = truffle_core::VERSION.into();
        e.host_fingerprint = fp.map(str::to_string);
        e
    }

    fn sample_report(run_id: &str, started_at: &str, params: WorkloadParams) -> RunReport {
        let snap = PathSnapshot {
            at: started_at.into(),
            connection: Some("direct".into()),
            rtt_ms: Some(1.2),
            endpoint: Some("192.0.2.10:41641".into()),
            error: None,
        };
        let mut r = RunReport {
            schema_version: SCHEMA_VERSION,
            run_id: run_id.into(),
            suite_id: None,
            tool: ReportTool::TruffleBenchCli,
            tool_version: crate::TOOL_VERSION.into(),
            truffle_version: truffle_core::VERSION.into(),
            methodology: METHODOLOGY.into(),
            workload: params.workload(),
            label: params.label(),
            status: RunStatus::Ok,
            error: None,
            error_code: None,
            started_at: started_at.into(),
            finished_at: started_at.into(),
            duration_ms: 12_000,
            git_sha: None,
            git_dirty: None,
            compare_key: String::new(),
            local: env(BuildProfile::Release, Some("aaaaaaaaaaaaaaaa")),
            peer: Some(env(BuildProfile::Release, Some("bbbbbbbbbbbbbbbb"))),
            network: NetworkInfo {
                topology: Topology::SeparateHosts,
                settle_ms: 412,
                before: snap.clone(),
                after: snap,
                clock: None,
                path_timeline: vec![],
                loaded_rtt_ms: None,
                path_bytes: Some(PathBytes {
                    direct: 1000,
                    derp: 0,
                    peer_relay: 0,
                }),
                derp_share: Some(0.0),
                path_label: PathLabel::Direct,
            },
            params,
            summary: vec![SummaryItem::new(
                "goodput_mbps",
                "Goodput",
                Some(912.4),
                "Mb/s",
                Better::Higher,
            )],
            results: json!({}),
            series: vec![],
            resources: Resources::default(),
            overlapping_runs: 0,
            notes: vec![],
        };
        r.compare_key = r.compute_compare_key();
        r
    }

    #[test]
    fn report_round_trips_with_rfc_field_spellings() {
        let r = sample_report(
            "r-1",
            "2026-10-16T08:00:00.000Z",
            WorkloadParams::Tcp(TcpParams::default()),
        );
        let v = serde_json::to_value(&r).unwrap();
        assert_eq!(v["schema_version"], 3);
        assert_eq!(v["tool"], "truffle-bench-cli");
        assert_eq!(v["workload"], "tcp");
        assert_eq!(v["status"], "ok");
        assert_eq!(v["params"]["workload"], "tcp");
        assert_eq!(v["network"]["topology"], "separate-hosts");
        assert_eq!(v["network"]["path_label"], "direct");
        assert_eq!(
            v["resources"],
            json!({"local": null, "peer": null, "combined": null})
        );
        assert_eq!(v["compare_key"].as_str().unwrap().len(), 16);
        let back: RunReport = serde_json::from_value(v).unwrap();
        assert_eq!(back, r);
        assert_eq!(r.file_name(), "20261016T080000Z-tcp-r-1.json");
        assert_eq!(
            parse_file_name(&r.file_name()),
            Some(("20261016T080000Z", Workload::Tcp, "r-1"))
        );
        assert_eq!(
            parse_file_name("20261016T080000Z-file-transfer-x-abc.json"),
            Some(("20261016T080000Z", Workload::FileTransfer, "x-abc"))
        );
        assert_eq!(parse_file_name("notes.json"), None);
    }

    #[test]
    fn compare_key_is_stable_and_ignores_run_length() {
        let base = TcpParams::default();
        let key = |p: TcpParams| {
            compare_key(
                &WorkloadParams::Tcp(p),
                Topology::SeparateHosts,
                PathLabel::Direct,
                BuildProfile::Release,
                Some(BuildProfile::Release),
                METHODOLOGY,
                Some("bbbbbbbbbbbbbbbb"),
            )
        };
        let k = key(base.clone());
        assert_eq!(k.len(), 16);
        assert!(k.chars().all(|c| c.is_ascii_hexdigit()));
        // Pinned (computed independently from the canonical JSON): changing
        // the canonical form would silently split History.
        assert_eq!(k, "e80c75981062420c");
        assert_eq!(k, key(base.clone()));
        assert_eq!(
            k,
            key(TcpParams {
                duration_secs: 30,
                warmup_secs: 5,
                count: 999,
                warmup_count: 3,
                ..base.clone()
            })
        );
        assert_ne!(
            k,
            key(TcpParams {
                streams: 4,
                ..base.clone()
            })
        );
        assert_ne!(
            k,
            key(TcpParams {
                direction: Direction::Download,
                ..base.clone()
            })
        );
        let other = |topology, label, local, peer, m: &str, fp: Option<&str>| {
            compare_key(
                &WorkloadParams::Tcp(base.clone()),
                topology,
                label,
                local,
                peer,
                m,
                fp,
            )
        };
        let release = BuildProfile::Release;
        assert_ne!(
            k,
            other(
                Topology::SameHost,
                PathLabel::Direct,
                release,
                Some(release),
                METHODOLOGY,
                Some("bbbbbbbbbbbbbbbb")
            )
        );
        assert_ne!(
            k,
            other(
                Topology::SeparateHosts,
                PathLabel::Relay,
                release,
                Some(release),
                METHODOLOGY,
                Some("bbbbbbbbbbbbbbbb")
            )
        );
        assert_ne!(
            k,
            other(
                Topology::SeparateHosts,
                PathLabel::Direct,
                BuildProfile::Debug,
                Some(release),
                METHODOLOGY,
                Some("bbbbbbbbbbbbbbbb")
            )
        );
        assert_ne!(
            k,
            other(
                Topology::SeparateHosts,
                PathLabel::Direct,
                release,
                Some(BuildProfile::Debug),
                METHODOLOGY,
                Some("bbbbbbbbbbbbbbbb")
            )
        );
        assert_ne!(
            k,
            other(
                Topology::SeparateHosts,
                PathLabel::Direct,
                release,
                Some(release),
                "v2-transport",
                Some("bbbbbbbbbbbbbbbb")
            )
        );
        assert_ne!(
            k,
            other(
                Topology::SeparateHosts,
                PathLabel::Direct,
                release,
                Some(release),
                METHODOLOGY,
                Some("cccccccccccccccc")
            )
        );
        // Other workloads: iterations and count are excluded too.
        let ft = |iterations| {
            compare_key(
                &WorkloadParams::FileTransfer(FileTransferParams {
                    size_bytes: 1 << 20,
                    iterations,
                }),
                Topology::SameProcess,
                PathLabel::Direct,
                release,
                Some(release),
                METHODOLOGY,
                None,
            )
        };
        // …but whether iteration 1 is in the medians is part of the key.
        assert_eq!(ft(3), ft(12));
        assert_eq!(ft(1), ft(2));
        assert_ne!(ft(2), ft(3));
        let path = |count| {
            compare_key(
                &WorkloadParams::Path(PathParams {
                    count,
                    interval_ms: 250,
                }),
                Topology::SameProcess,
                PathLabel::Direct,
                release,
                None,
                METHODOLOGY,
                None,
            )
        };
        assert_eq!(path(5), path(20));
        // Where a run-length field defines the measurement, it is keyed.
        let key_of = |p: WorkloadParams| {
            compare_key(
                &p,
                Topology::SameProcess,
                PathLabel::Direct,
                release,
                Some(release),
                METHODOLOGY,
                None,
            )
        };
        let store = |mode, count| {
            key_of(WorkloadParams::SyncedStore(StoreParams {
                mode,
                count,
                ..StoreParams::default()
            }))
        };
        assert_ne!(store(StoreMode::Burst, 10), store(StoreMode::Burst, 5000));
        assert_eq!(
            store(StoreMode::Pingpong, 10),
            store(StoreMode::Pingpong, 5000)
        );
        let udp = |mode, duration_secs, warmup_secs| {
            key_of(WorkloadParams::Udp(crate::params::UdpParams {
                mode,
                duration_secs,
                warmup_secs,
                ..Default::default()
            }))
        };
        assert_ne!(udp(UdpMode::Ramp, 10, 1), udp(UdpMode::Ramp, 60, 1));
        assert_eq!(udp(UdpMode::Ramp, 10, 1), udp(UdpMode::Ramp, 10, 3));
        assert_eq!(udp(UdpMode::Stream, 10, 1), udp(UdpMode::Stream, 60, 1));
        assert_eq!(
            unkeyed_params(&WorkloadParams::Tcp(TcpParams::default())),
            COMPARE_KEY_EXCLUDED.to_vec()
        );
    }

    #[test]
    fn canonical_json_sorts_keys() {
        let v = json!({"b": 1, "a": {"d": [true, null, 1.5], "c": "x\"y"}});
        assert_eq!(
            canonical_json(&v),
            r#"{"a":{"c":"x\"y","d":[true,null,1.5]},"b":1}"#
        );
    }

    #[test]
    fn honesty_notes_and_summary() {
        let mut r = sample_report(
            "r-2",
            "2026-10-16T08:00:00Z",
            WorkloadParams::Http(HttpParams::default()),
        );
        assert!(r.honesty_notes().is_empty());
        r.network.topology = Topology::SameProcess;
        r.overlapping_runs = 2;
        r.peer.as_mut().unwrap().build_profile = BuildProfile::Debug;
        r.apply_honesty_notes();
        r.apply_honesty_notes();
        assert_eq!(
            r.notes,
            vec![
                NOTE_NOT_A_LINK.to_string(),
                "shared the machine with 2 other runs".to_string(),
                NOTE_DEBUG_BUILD.to_string()
            ]
        );
        let s = r.to_summary();
        assert_eq!(s.build_profile, BuildProfile::Debug);
        assert_eq!(s.peer_name.as_deref(), Some("studio"));
        assert_eq!(s.workload, Workload::Http);
        assert_eq!(r.summary_value("goodput_mbps"), Some(912.4));
        assert_eq!(
            combined_profile(BuildProfile::Release, None),
            BuildProfile::Release
        );
        assert_eq!(
            combined_profile(BuildProfile::Unknown, Some(BuildProfile::Release)),
            BuildProfile::Unknown
        );
    }

    #[test]
    fn run_ids_are_validated() {
        assert!(validate_run_id("2026-10-16-abc-123").is_ok());
        assert!(validate_run_id("").is_err());
        assert!(validate_run_id("../etc").is_err());
        assert!(validate_run_id("a/b").is_err());
        assert!(validate_run_id(&"a".repeat(65)).is_err());
        assert!(validate_run_id(&"a".repeat(64)).is_ok());
    }

    #[test]
    fn store_saves_lists_gets_and_deletes() {
        let dir = tempfile::tempdir().unwrap();
        let store = RunStore::new(dir.path().join("runs"));
        assert!(store.list().is_empty());
        let a = sample_report(
            "run-a",
            "2026-10-16T08:00:00.000Z",
            WorkloadParams::Tcp(TcpParams::default()),
        );
        let b = sample_report(
            "run-b",
            "2026-10-16T09:00:00.000Z",
            WorkloadParams::Path(PathParams::default()),
        );
        let path = store.save(&a).unwrap();
        assert!(path.ends_with("20261016T080000Z-tcp-run-a.json"));
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(
            text.starts_with("{\n") && text.ends_with("}\n"),
            "pretty with a newline"
        );
        store.save(&b).unwrap();
        assert!(store.save(&a).is_err(), "never overwrites");
        let list = store.list();
        assert_eq!(
            list.iter().map(|s| s.run_id.as_str()).collect::<Vec<_>>(),
            ["run-b", "run-a"]
        );
        assert_eq!(store.list(), list, "cached");
        assert_eq!(store.get("run-a").unwrap(), a);
        assert_eq!(
            store.get("nope").unwrap_err().code,
            ErrorCode::InvalidParams
        );
        assert_eq!(
            store.get("../x").unwrap_err().code,
            ErrorCode::InvalidParams
        );
        store.delete("run-a").unwrap();
        assert!(!store.contains("run-a"));
        assert_eq!(store.list().len(), 1);
        // Stray files are ignored.
        std::fs::write(dir.path().join("runs/notes.json"), b"{}").unwrap();
        std::fs::write(
            dir.path().join("runs/20261016T080000Z-tcp-broken.json"),
            b"{",
        )
        .unwrap();
        assert_eq!(store.list().len(), 1);
    }

    #[test]
    fn v1_reports_normalise() {
        let r = normalise_bytes(V1_STORE.as_bytes(), "20260913T222009Z-synced-store.json").unwrap();
        assert_eq!(r.schema_version, 3);
        assert_eq!(r.tool, ReportTool::Imported);
        assert_eq!(r.methodology, "v2-synced-store");
        assert_eq!(r.workload, Workload::SyncedStore);
        assert_eq!(r.status, RunStatus::Ok);
        assert_eq!(r.started_at, "2026-09-13T22:20:09.839Z");
        assert_eq!(r.git_sha.as_deref(), Some("d327825"));
        assert_eq!(r.local.build_profile, BuildProfile::Unknown);
        assert_eq!(r.network.topology, Topology::SameProcess);
        assert_eq!(r.network.path_label, PathLabel::Unknown);
        assert_eq!(r.summary_value("convergence_ms"), Some(28.0));
        assert!(r.run_id.starts_with("imported-"));
        assert_eq!(r.compare_key, r.compute_compare_key());
        assert!(r.notes.iter().any(|n| n.contains("v1")));
        assert!(r.notes.iter().any(|n| n == NOTE_NOT_A_LINK));
        assert_eq!(r.results["params"]["scenario"], "b1-sequential");

        let r = normalise_bytes(V1_RR.as_bytes(), "rr.json").unwrap();
        assert_eq!(r.workload, Workload::Ws);
        assert_eq!(r.methodology, "v2-request-reply");
        assert!(matches!(
            r.params,
            WorkloadParams::Ws(WsParams {
                mode: WsMode::Rr,
                payload_bytes: 1024,
                count: 200,
                ..
            })
        ));
        assert_eq!(r.summary_value("rtt_p50_ms"), Some(0.293));
        assert_eq!(
            r.summary_value("rtt_p99_ms"),
            Some(0.56),
            "200 successes ≥ 100"
        );
        let rate = r.summary_value("msgs_per_sec").unwrap();
        assert!((rate - 200.0 / 0.065324666).abs() < 1e-6);
        assert_eq!(r.duration_ms, 65);
        // Same bytes → same run id.
        assert_eq!(
            normalise_bytes(V1_RR.as_bytes(), "x").unwrap().run_id,
            r.run_id
        );
    }

    #[test]
    fn rust_v2_transport_normalises() {
        let r = normalise_bytes(V2_TRANSPORT.as_bytes(), "t.json").unwrap();
        assert_eq!(r.workload, Workload::Tcp);
        assert_eq!(r.methodology, "v2-transport");
        assert!(matches!(
            r.params,
            WorkloadParams::Tcp(TcpParams {
                mode: StreamMode::Throughput,
                streams: 1,
                ..
            })
        ));
        // MB/s → Mb/s, under its own key.
        assert_eq!(r.summary_value("fixed_size_mbps"), Some(16.644 * 8.0));
        assert_eq!(r.summary_value("goodput_mbps"), None);
        assert_eq!(r.summary_value("connect_ms"), Some(11.902));
        assert_eq!(r.local.build_profile, BuildProfile::Release);
        assert_eq!(
            r.local.rustc.as_deref(),
            Some("rustc 1.97.1 (8bab26f4f 2026-07-14)")
        );
        assert_eq!(
            r.local.sidecar_sha256.as_deref(),
            Some("9f2c4e6a8b0d1f3e5a7c9e1b3d5f7a9c0e2b4d6f8a1c3e5b7d9f0a2c4e6b8d0f")
        );
        assert_eq!(r.local.os, "macos");
        assert_eq!(r.git_dirty, Some(false));
        assert_eq!(r.network.topology, Topology::SameProcess);
        assert_eq!(r.network.before.connection.as_deref(), Some("direct"));
        assert_eq!(
            r.network.before.endpoint.as_deref(),
            Some("192.0.2.10:41641")
        );
        assert_eq!(r.network.path_label, PathLabel::Direct);
        assert_eq!(r.duration_ms, 126);
        assert_eq!(r.status, RunStatus::Ok);
        assert!(r.label.contains("1 MiB × 2"), "{}", r.label);
        // Debug builds carry across and get the note.
        let debug =
            V2_TRANSPORT.replace("\"debug_assertions\": false", "\"debug_assertions\": true");
        let r = normalise_bytes(debug.as_bytes(), "t.json").unwrap();
        assert_eq!(r.local.build_profile, BuildProfile::Debug);
        assert!(r.notes.iter().any(|n| n == NOTE_DEBUG_BUILD));
        // Invalid runs import as failed.
        let invalid = V2_TRANSPORT.replace("\"valid\": true", "\"valid\": false");
        assert_eq!(
            normalise_bytes(invalid.as_bytes(), "t.json")
                .unwrap()
                .status,
            RunStatus::Failed
        );
        // QUIC maps to the quic workload.
        let quic = V2_TRANSPORT.replace("\"protocol\": \"tcp\"", "\"protocol\": \"quic\"");
        assert_eq!(
            normalise_bytes(quic.as_bytes(), "t.json").unwrap().workload,
            Workload::Quic
        );
    }

    #[test]
    fn rust_v2_file_transfer_maps_to_e2e() {
        let v = json!({
            "schema_version": 2,
            "workload": "file-transfer",
            "timestamp_utc": "2026-10-01T00:00:00+00:00",
            "git_sha": null, "git_dirty": null,
            "environment": {"os": "linux", "arch": "x86_64", "logical_cpus": 4, "rustc": null,
                            "debug_assertions": false, "sidecar_sha256": null, "sidecar_build_info": null},
            "network": {"topology": "same-host", "before": {"topology": "same-host", "ping": {"error": "path probe timed out"}},
                        "after": {"topology": "same-host", "ping": {"path": "relay:nyc", "rtt_ms": 30.0, "endpoint": null}}},
            "params": {"size_bytes": 10485760, "iterations": 5, "warmup": 1},
            "results": {"valid": true, "iterations": [], "latency_ms": null,
                        "throughput_mb_per_sec": {"samples": 5, "p50": 50.0, "p95": 60.0, "p99": 60.0, "p999": 60.0, "min": 40.0, "max": 60.0, "mean": 50.0},
                        "mean_throughput_mb_per_sec": 50.0}
        });
        let raw = serde_json::to_vec(&v).unwrap();
        let r = normalise(v, &raw, "ft.json").unwrap();
        assert_eq!(r.workload, Workload::FileTransfer);
        assert_eq!(r.methodology, "v2-file-transfer");
        assert_eq!(r.summary_value("e2e_mbps"), Some(400.0));
        assert_eq!(r.summary_value("wire_mbps"), None);
        assert_eq!(
            r.network.before.error.as_deref(),
            Some("path probe timed out")
        );
        assert_eq!(r.network.path_label, PathLabel::Relay);
        assert!(matches!(
            r.params,
            WorkloadParams::FileTransfer(FileTransferParams {
                size_bytes: 10485760,
                iterations: 5
            })
        ));
    }

    #[test]
    fn tsnet_bench_normalises() {
        let r = normalise_bytes(TSNET.as_bytes(), "tsnet.json").unwrap();
        assert_eq!(r.workload, Workload::Tcp);
        assert_eq!(r.methodology, "native-tsnet");
        assert!(r.peer.is_none());
        let expected = 3.0 * 10485760.0 * 8.0 / ((171.835 + 157.893 + 161.799) / 1000.0) / 1e6;
        let got = r.summary_value("goodput_mbps").unwrap();
        assert!((got - expected).abs() < 1e-9, "{got} vs {expected}");
        assert_eq!(r.summary_value("connect_ms"), Some(10.115));
        assert!(r.notes.iter().any(|n| n == "native Go/tsnet baseline"));
        assert_eq!(
            r.git_sha.as_deref(),
            Some("3cdfbaa1e2d3c4b5a69788796a5b4c3d2e1f0a9b")
        );
        assert_eq!(r.git_dirty, Some(true));
        assert_eq!(r.local.tool, "tsnet-bench");
        assert_eq!(r.local.os, "darwin");
        assert_eq!(r.network.before.connection.as_deref(), Some("relay:nyc"));
        assert_eq!(r.network.after.connection.as_deref(), Some("direct"));
        assert_eq!(
            r.network.after.endpoint.as_deref(),
            Some("192.0.2.20:41641")
        );
        assert!((r.network.before.rtt_ms.unwrap() - 31.208).abs() < 1e-9);
        assert_eq!(r.network.path_label, PathLabel::Mixed);
        assert_eq!(r.network.topology, Topology::SeparateHosts);
    }

    #[test]
    fn unknown_inputs_are_rejected() {
        assert!(normalise_bytes(b"[1,2]", "x").is_err());
        assert!(normalise_bytes(b"not json", "x").is_err());
        assert!(normalise_bytes(br#"{"schema_version": 9}"#, "x").is_err());
        assert!(normalise_bytes(
            br#"{"workload": "mystery", "timestamp_utc": "2026-01-01T00:00:00Z"}"#,
            "x"
        )
        .is_err());
        assert!(normalise_bytes(br#"{"schema_version": 3, "run_id": "x"}"#, "x").is_err());
        // An absurd elapsed time neither panics nor survives.
        for elapsed in ["9e15", "1e30", "-5", "8.3e12"] {
            let raw = format!(
                r#"{{"schema_version":2,"workload":"transport","timestamp_utc":"2026-01-01T00:00:00Z","params":{{}},"results":{{"elapsed_secs":{elapsed}}}}}"#
            );
            let r = normalise_bytes(raw.as_bytes(), "x.json").unwrap();
            assert_eq!(r.finished_at, r.started_at, "{elapsed}");
            assert_eq!(r.duration_ms, 0, "{elapsed}");
        }
    }

    #[test]
    fn import_files_dirs_and_zips() {
        let dir = tempfile::tempdir().unwrap();
        let store = RunStore::new(dir.path().join("runs"));
        let inbox = dir.path().join("inbox");
        std::fs::create_dir_all(inbox.join("nested")).unwrap();
        std::fs::write(inbox.join("a.json"), V1_STORE).unwrap();
        std::fs::write(inbox.join("nested/b.json"), V2_TRANSPORT).unwrap();
        std::fs::write(inbox.join("junk.json"), b"{}").unwrap();
        std::fs::write(inbox.join("readme.txt"), b"ignored").unwrap();
        let v3 = sample_report(
            "cli-run-1",
            "2026-10-16T10:00:00.000Z",
            WorkloadParams::Tcp(TcpParams::default()),
        );
        std::fs::write(
            inbox.join("v3.json"),
            serde_json::to_vec_pretty(&v3).unwrap(),
        )
        .unwrap();

        let res = store.import(&inbox);
        assert_eq!(
            res,
            ImportResult {
                imported: 3,
                skipped: 1
            }
        );
        // Importing again skips everything already stored.
        let res = store.import(&inbox);
        assert_eq!(
            res,
            ImportResult {
                imported: 0,
                skipped: 4
            }
        );
        let v3_back = store.get("cli-run-1").unwrap();
        assert_eq!(
            v3_back.tool,
            ReportTool::TruffleBenchCli,
            "v3 keeps its tool"
        );

        // A zip, as GitHub artifact downloads come.
        let zip_path = dir.path().join("bench-results-123.zip");
        {
            let file = std::fs::File::create(&zip_path).unwrap();
            let mut z = zip::ZipWriter::new(file);
            let opts = zip::write::SimpleFileOptions::default();
            z.start_file("bench-results/c.json", opts).unwrap();
            z.write_all(V1_RR.as_bytes()).unwrap();
            z.start_file("bench-results/tsnet.json", opts).unwrap();
            z.write_all(TSNET.as_bytes()).unwrap();
            z.start_file("bench-results/notes.md", opts).unwrap();
            z.write_all(b"# notes").unwrap();
            z.finish().unwrap();
        }
        let res = store.import(&zip_path);
        assert_eq!(
            res,
            ImportResult {
                imported: 2,
                skipped: 0
            }
        );
        assert_eq!(store.list().len(), 5);
        let missing = store.import(&dir.path().join("missing.json"));
        assert_eq!(
            missing,
            ImportResult {
                imported: 0,
                skipped: 1
            }
        );
    }

    #[test]
    fn imports_are_bounded() {
        let dir = tempfile::tempdir().unwrap();
        let store = RunStore::new(dir.path().join("runs"));
        // More entries than one import reads: small, and compressing well.
        let zip_path = dir.path().join("many.zip");
        {
            let file = std::fs::File::create(&zip_path).unwrap();
            let mut z = zip::ZipWriter::new(file);
            let opts = zip::write::SimpleFileOptions::default();
            for i in 0..MAX_IMPORT_ENTRIES + 50 {
                z.start_file(format!("r/{i}.json"), opts).unwrap();
                z.write_all(b"{}").unwrap();
            }
            z.finish().unwrap();
        }
        let mut limit_errors = 0;
        let mut seen = 0;
        for_each_report(&zip_path, &mut |_, r| {
            seen += 1;
            if r.is_err_and(|e| e.contains("import limit reached")) {
                limit_errors += 1;
            }
        });
        assert_eq!(seen, MAX_IMPORT_ENTRIES + 1, "the rest is not read");
        assert_eq!(limit_errors, 1);
        let res = store.import(&zip_path);
        assert_eq!(res.imported, 0);
        assert_eq!(res.skipped as usize, MAX_IMPORT_ENTRIES + 1);

        // A report with more points than any run produces is refused.
        let mut big = sample_report(
            "big-run",
            "2026-10-16T10:00:00.000Z",
            WorkloadParams::Tcp(TcpParams::default()),
        );
        big.series = vec![Series {
            key: "goodput_mbps".into(),
            label: "Goodput".into(),
            unit: "Mb/s".into(),
            points: vec![(0.0, 0.0); MAX_REPORT_POINTS + 1],
            warmup_ms: None,
        }];
        let err = normalise_bytes(&serde_json::to_vec(&big).unwrap(), "big.json").unwrap_err();
        assert!(err.contains("points"), "{err}");
        big.series = (0..=MAX_REPORT_SERIES)
            .map(|i| Series {
                key: format!("s{i}"),
                label: "S".into(),
                unit: "Mb/s".into(),
                points: vec![],
                warmup_ms: None,
            })
            .collect();
        let err = normalise_bytes(&serde_json::to_vec(&big).unwrap(), "big.json").unwrap_err();
        assert!(err.contains("series"), "{err}");
        big.series.truncate(2);
        normalise_bytes(&serde_json::to_vec(&big).unwrap(), "ok.json").unwrap();
    }

    #[test]
    fn summary_flags_follow_the_report() {
        let tcp = WorkloadParams::Tcp(Default::default());
        let mut r = sample_report("r-flags", "2026-10-16T08:00:00.000Z", tcp);
        assert!(r.flags().is_empty());
        assert!(r.to_summary().flags.is_empty());
        r.local.build_profile = BuildProfile::Debug;
        r.overlapping_runs = 2;
        r.notes.push(NOTE_RATE_NOT_ACHIEVED.into());
        r.tool = ReportTool::Imported;
        r.network.topology = Topology::SameHost;
        r.network.path_label = PathLabel::Mixed;
        r.status = RunStatus::Failed;
        assert_eq!(
            r.to_summary().flags,
            [
                flag::DEBUG,
                flag::OVERLAP,
                flag::RATE_NOT_ACHIEVED,
                flag::IMPORTED,
                flag::SAME_HOST,
                flag::MIXED_PATH,
                flag::PARTIAL,
            ]
        );
        // A failed run with nothing measured is not partial.
        r.summary[0].value = None;
        r.network.topology = Topology::SameProcess;
        let flags = r.flags();
        assert!(!flags.iter().any(|f| f == flag::PARTIAL));
        assert!(flags.iter().any(|f| f == flag::SAME_PROCESS));
    }
}
