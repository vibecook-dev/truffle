//! The `truffle-bench` commands over the engine (RFC 027 §10).
//!
//! `serve`, `peers`, `run`, `pair`, `suite` and `report`. The legacy
//! `transport` lives in [`crate::legacy`].
//!
//! Auth keys are never taken from argv, logged, written to reports or
//! persisted. Lookup order: `--auth-key-file` (first line),
//! `TRUFFLE_BENCH_AUTH_KEY`, `TRUFFLE_TEST_AUTHKEY`, then `TS_AUTHKEY` with a
//! warning. Without a key, `serve`, `peers` and `run` print the login URL;
//! `pair` and `suite --pair` require one.

use std::borrow::Cow;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use clap::{Args, Parser, Subcommand};
use serde_json::Value;
use truffle_bench::config::cli_data_dir;
use truffle_bench::hub::{Hub, Pair, RunRequest, SuiteRequest};
use truffle_bench::report::{read_reports, write_report};
use truffle_bench::types::{
    BuildProfile, HubEvent, NodeStatus, Phase, Progress, RemoteRunState, RunStatus, SuiteProgress,
};
use truffle_bench::{suites, AppError, ErrorCode, HubConfig, RunReport, Tool, WorkloadParams};

use crate::legacy;

/// Default `--out-dir`.
pub const DEFAULT_OUT_DIR: &str = "target/bench-results";

#[derive(Parser)]
#[command(
    name = "truffle-bench",
    about = "Benchmarks for truffle: every workload, between any two hubs (RFC 027)",
    version
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,

    /// Where to write report JSON (default `target/bench-results`).
    #[arg(long, global = true)]
    pub out_dir: Option<PathBuf>,
}

#[derive(Subcommand)]
pub enum Command {
    /// Answer peers until Ctrl-C (a responder for machines without the app).
    Serve(HubArgs),
    /// List same-app peers (waits 5 s for discovery).
    Peers(HubArgs),
    /// Run one workload against a peer.
    Run(RunArgs),
    /// Run one workload between two ephemeral hubs in this process (needs an auth key).
    Pair(PairArgs),
    /// Run a suite profile against a peer or an in-process pair.
    Suite(SuiteArgs),
    /// Print summaries of v1/v2/v3 report files, directories and zips.
    Report(ReportArgs),
    /// Legacy raw TCP/QUIC throughput with pair, server and client modes
    /// (tsnet-bench compatible; writes schema v2).
    Transport(legacy::transport::Args),
}

/// How to build this process's hub.
#[derive(Args, Clone, Debug, Default)]
pub struct NodeArgs {
    /// Device name (default `<hostname>-cli`, plus `-<profile>`).
    #[arg(long)]
    pub device_name: Option<String>,
    /// tsnet state directory (default `<data-dir>/state/<device>`).
    #[arg(long)]
    pub state_dir: Option<PathBuf>,
    /// Data directory: runs and temp files (default: `<OS data dir>/truffle-bench-cli`,
    /// e.g. ~/Library/Application Support/truffle-bench-cli on macOS).
    #[arg(long)]
    pub data_dir: Option<PathBuf>,
    /// Read the auth key from the first line of this file (keys are never accepted as arguments).
    #[arg(long)]
    pub auth_key_file: Option<PathBuf>,
    /// Remove this device from the tailnet when it goes offline.
    #[arg(long)]
    pub ephemeral: bool,
    /// Accept runs from peers signed in with other accounts.
    #[arg(long)]
    pub allow_other_logins: bool,
    /// Accept runs from tagged peers (on by default when this hub signs in with a key).
    #[arg(long)]
    pub allow_tagged: bool,
    /// Sidecar binary (default: TRUFFLE_TEST_SIDECAR_PATH, TRUFFLE_SIDECAR_PATH, then the dev build).
    #[arg(long)]
    pub sidecar: Option<PathBuf>,
}

/// Node flags plus `--profile` (`serve`, `peers`, `run`).
#[derive(Args, Clone, Debug, Default)]
pub struct HubArgs {
    #[command(flatten)]
    pub node: NodeArgs,
    /// Profile name, appended to the device name (`-<profile>`), so two
    /// hubs on one machine keep separate state.
    #[arg(long)]
    pub profile: Option<String>,
}

/// `run`.
#[derive(Args, Clone, Debug)]
pub struct RunArgs {
    /// The peer: device name, ULID, tailscale id, hostname or IP.
    #[arg(long)]
    pub peer: String,
    /// The workload (`path`, `tcp`, `udp`, `quic`, `ws`, `file-transfer`, …).
    pub workload: String,
    #[command(flatten)]
    pub params: ParamArgs,
    #[command(flatten)]
    pub hub: HubArgs,
}

/// `pair`.
#[derive(Args, Clone, Debug)]
pub struct PairArgs {
    /// The workload.
    pub workload: String,
    #[command(flatten)]
    pub params: ParamArgs,
    /// Accepted for symmetry with `suite`; only `suite --profile nightly` refuses debug builds.
    #[arg(long)]
    pub allow_debug: bool,
    /// Read the auth key from the first line of this file.
    #[arg(long)]
    pub auth_key_file: Option<PathBuf>,
    /// Sidecar binary.
    #[arg(long)]
    pub sidecar: Option<PathBuf>,
}

/// `--param k=v` and `--params-json`.
#[derive(Args, Clone, Debug, Default)]
pub struct ParamArgs {
    /// A param override, `field=value` (value parsed as JSON when it can be).
    #[arg(long = "param", value_name = "K=V")]
    pub param: Vec<String>,
    /// Params as a JSON object, merged under `--param`.
    #[arg(long, value_name = "JSON")]
    pub params_json: Option<String>,
}

/// `suite`.
#[derive(Args, Clone, Debug)]
pub struct SuiteArgs {
    /// `smoke`, `standard`, `full` or `nightly`.
    #[arg(long)]
    pub profile: String,
    /// Run against an in-process pair (needs an auth key).
    #[arg(long, conflicts_with = "peer", required_unless_present = "peer")]
    pub pair: bool,
    /// Run against this peer.
    #[arg(long)]
    pub peer: Option<String>,
    /// Override, `<workload>.<field>=value`.
    #[arg(long = "param", value_name = "W.K=V")]
    pub param: Vec<String>,
    /// Allow a debug build to run the nightly profile.
    #[arg(long)]
    pub allow_debug: bool,
    #[command(flatten)]
    pub node: NodeArgs,
    /// With `--peer`: this hub's profile name (`--profile` names the suite here).
    #[arg(long)]
    pub hub_profile: Option<String>,
}

/// `report`.
#[derive(Args, Clone, Debug)]
pub struct ReportArgs {
    /// Report files, directories or zips.
    #[arg(required = true)]
    pub paths: Vec<PathBuf>,
}

// ---------------------------------------------------------------------------
// Shared plumbing
// ---------------------------------------------------------------------------

fn eprintln_err(e: &AppError) {
    eprintln!("error ({}): {}", e.code, term_safe(&e.message));
}

/// `s` with every control character (C0, DEL, C1) replaced by U+FFFD, for
/// text a peer or an imported file supplied: printed raw, an escape
/// sequence could move the cursor, rewrite lines or set the clipboard.
fn term_safe(s: &str) -> Cow<'_, str> {
    if s.chars().any(char::is_control) {
        Cow::Owned(
            s.chars()
                .map(|c| if c.is_control() { '\u{FFFD}' } else { c })
                .collect(),
        )
    } else {
        Cow::Borrowed(s)
    }
}

/// Print the debug-build banner (numbers are not comparable with release).
pub fn debug_banner() {
    if cfg!(debug_assertions) {
        eprintln!(
            "[debug build] numbers are not comparable with release builds; \
             build with --release for real measurements"
        );
    }
}

/// The auth key, in §10 order. `Ok(None)` when none is configured.
pub fn auth_key(file: Option<&Path>) -> Result<Option<String>, AppError> {
    if let Some(path) = file {
        let text = std::fs::read_to_string(path)
            .map_err(|e| AppError::io(format!("read {}", path.display()), &e))?;
        let key = text.lines().next().unwrap_or_default().trim().to_string();
        if key.is_empty() {
            return Err(AppError::invalid_params(format!(
                "{} is empty; its first line must be the auth key",
                path.display()
            )));
        }
        return Ok(Some(key));
    }
    for var in ["TRUFFLE_BENCH_AUTH_KEY", "TRUFFLE_TEST_AUTHKEY"] {
        if let Ok(k) = std::env::var(var) {
            if !k.trim().is_empty() {
                return Ok(Some(k.trim().to_string()));
            }
        }
    }
    if let Ok(k) = std::env::var("TS_AUTHKEY") {
        if !k.trim().is_empty() {
            eprintln!("[warn] using TS_AUTHKEY; TRUFFLE_BENCH_AUTH_KEY is preferred");
            return Ok(Some(k.trim().to_string()));
        }
    }
    Ok(None)
}

fn hub_config(args: &NodeArgs, profile: Option<&str>) -> Result<HubConfig, AppError> {
    let data_dir = args.data_dir.clone().unwrap_or_else(cli_data_dir);
    let mut cfg = HubConfig::defaults(Tool::Cli, profile, data_dir);
    if let Some(name) = &args.device_name {
        let name = match profile {
            Some(p) if !p.is_empty() => format!("{name}-{p}"),
            _ => name.clone(),
        };
        cfg = cfg.with_device_name(name);
    }
    if let Some(dir) = &args.state_dir {
        cfg.state_dir = dir.clone();
    }
    if let Some(key) = auth_key(args.auth_key_file.as_deref())? {
        cfg = cfg.with_auth_key(key);
    }
    cfg.ephemeral = args.ephemeral;
    cfg.allow_other_logins = args.allow_other_logins;
    if args.allow_tagged {
        cfg.allow_tagged = true;
    }
    cfg.sidecar_path = args.sidecar.clone();
    Ok(cfg)
}

/// Parse `--param k=v` values: JSON when possible, otherwise a string.
pub fn parse_param(raw: &str) -> Result<(String, Value), AppError> {
    let (k, v) = raw.split_once('=').ok_or_else(|| {
        AppError::invalid_params(format!("--param “{raw}” must look like field=value"))
    })?;
    let k = k.trim();
    if k.is_empty() {
        return Err(AppError::invalid_params(format!(
            "--param “{raw}” has no field name"
        )));
    }
    let value = serde_json::from_str(v.trim()).unwrap_or_else(|_| Value::String(v.to_string()));
    Ok((k.to_string(), value))
}

/// Defaults of `workload`, then `--params-json`, then every `--param`, validated.
pub fn build_params(workload: &str, args: &ParamArgs) -> Result<WorkloadParams, AppError> {
    let w: truffle_bench::Workload = workload.parse()?;
    let mut params = w.default_params();
    let mut fields: Vec<(String, Value)> = Vec::new();
    if let Some(json) = &args.params_json {
        let v: Value = serde_json::from_str(json)
            .map_err(|e| AppError::invalid_params(format!("--params-json: {e}")))?;
        let Value::Object(map) = v else {
            return Err(AppError::invalid_params(
                "--params-json must be a JSON object",
            ));
        };
        for (k, v) in map {
            if k == "workload" {
                if v.as_str() != Some(w.as_str()) {
                    return Err(AppError::invalid_params(format!(
                        "--params-json names workload {v}, but the command runs {w}"
                    )));
                }
                continue;
            }
            fields.push((k, v));
        }
    }
    for raw in &args.param {
        fields.push(parse_param(raw)?);
    }
    params.apply_overrides(fields.iter().map(|(k, v)| (k.as_str(), v)))?;
    params.validate().map_err(AppError::invalid_params)?;
    Ok(params)
}

fn out_dir(cli_out: &Option<PathBuf>) -> PathBuf {
    cli_out
        .clone()
        .unwrap_or_else(|| PathBuf::from(DEFAULT_OUT_DIR))
}

/// Print node lifecycle events (login URL, errors) and remote runs.
fn spawn_event_printer(hub: &Arc<Hub>, verbose_peers: bool) -> tokio::task::JoinHandle<()> {
    let mut rx = hub.subscribe();
    tokio::spawn(async move {
        use tokio::sync::broadcast::error::RecvError;
        let mut last_peer_count = None;
        loop {
            match rx.recv().await {
                Ok(HubEvent::Node(st)) => match st.state {
                    NodeStatus::Starting => eprintln!(
                        "[node] starting as {}",
                        st.tailscale_hostname.as_deref().unwrap_or("?")
                    ),
                    NodeStatus::NeedsLogin => {
                        if let Some(url) = &st.auth_url {
                            eprintln!(
                                "[node] sign in to Tailscale to continue:\n\n    {url}\n\n\
                                 (this adds {} to your tailnet; with device approval on, approve it in the admin console)",
                                st.tailscale_hostname.as_deref().unwrap_or("this device")
                            );
                        }
                    }
                    NodeStatus::Running => {
                        if let Some(l) = &st.local {
                            eprintln!(
                                "[node] running as {} ({}) · login {} · tailnet {}",
                                l.device_name,
                                l.tailscale_ip.as_deref().unwrap_or("no ip"),
                                l.login_name.as_deref().unwrap_or("unknown"),
                                l.tailnet.as_deref().unwrap_or("unknown")
                            );
                        }
                    }
                    NodeStatus::Error => {
                        if let Some(e) = &st.error {
                            eprintln!("[node] error ({}): {}", e.code, e.message);
                        }
                    }
                    NodeStatus::Stopping => eprintln!("[node] stopping"),
                    NodeStatus::Stopped => eprintln!("[node] stopped"),
                    NodeStatus::Idle => {}
                },
                Ok(HubEvent::RemoteRun {
                    run_id,
                    peer,
                    workload,
                    state,
                    ..
                }) => {
                    let what = match state {
                        RemoteRunState::Prepared => "being measured by",
                        RemoteRunState::Finished => "finished a run for",
                        RemoteRunState::Cancelled => "cancelled a run for",
                        RemoteRunState::Expired => "watchdog ended a run for",
                    };
                    eprintln!(
                        "[serve] {what} {} · {} · {}",
                        term_safe(&peer),
                        term_safe(&workload),
                        term_safe(&run_id)
                    );
                }
                Ok(HubEvent::Peers { peers }) => {
                    let online = peers.iter().filter(|p| p.online).count();
                    if verbose_peers && last_peer_count != Some(online) {
                        eprintln!("[peers] {online} online of {}", peers.len());
                        last_peer_count = Some(online);
                    }
                }
                Err(RecvError::Lagged(_)) => continue,
                Err(RecvError::Closed) => break,
            }
        }
    })
}

/// Start a hub, or stop it if Ctrl-C comes first.
async fn start_hub(hub: &Arc<Hub>) -> Result<(), AppError> {
    let outcome = tokio::select! {
        r = hub.start() => Some(r),
        _ = tokio::signal::ctrl_c() => None,
    };
    match outcome {
        Some(r) => r,
        None => {
            let _force = ForceQuit::spawn();
            hub.stop().await;
            Err(AppError::cancelled())
        }
    }
}

/// Exit at once (status 130) on Ctrl-C while this lives. Installing a
/// Ctrl-C listener replaces the default action for the whole process, so
/// teardown after a first Ctrl-C (or a run's end) keeps one in place.
struct ForceQuit(tokio::task::JoinHandle<()>);

impl ForceQuit {
    fn spawn() -> Self {
        Self(tokio::spawn(async {
            if tokio::signal::ctrl_c().await.is_ok() {
                force_quit();
            }
        }))
    }
}

impl Drop for ForceQuit {
    fn drop(&mut self) {
        self.0.abort();
    }
}

fn force_quit() -> ! {
    eprintln!("\n[quit] Ctrl-C again: quitting without cleaning up");
    std::process::exit(130);
}

/// Console progress: phase changes, live samples at most once a second.
fn progress_printer(prefix: String) -> truffle_bench::ProgressSink {
    let state: Mutex<(Option<Phase>, u64)> = Mutex::new((None, 0));
    Arc::new(move |p: Progress| {
        let mut st = state.lock().unwrap_or_else(PoisonError::into_inner);
        let changed = st.0 != Some(p.phase);
        if changed {
            st.0 = Some(p.phase);
            if !p.phase.is_terminal() {
                match &p.message {
                    Some(m) => eprintln!("{prefix}[{}] {}", phase_str(p.phase), term_safe(m)),
                    None => eprintln!("{prefix}[{}]", phase_str(p.phase)),
                }
            }
        }
        let live = matches!(p.phase, Phase::Warmup | Phase::Running);
        if live && !p.samples.is_empty() && (changed || p.t_ms >= st.1 + 1000) {
            st.1 = p.t_ms;
            let values: Vec<String> = p
                .samples
                .iter()
                .map(|s| format!("{} {}", term_safe(&s.key), fmt_num(s.value)))
                .collect();
            let frac = p
                .fraction
                .map(|f| format!("  ({:.0} %)", f * 100.0))
                .unwrap_or_default();
            eprintln!(
                "{prefix}  {:>6.1} s  {}{frac}",
                p.t_ms as f64 / 1000.0,
                values.join("  ")
            );
        }
    })
}

fn phase_str(p: Phase) -> &'static str {
    match p {
        Phase::Preparing => "preparing",
        Phase::Settling => "settling",
        Phase::Clock => "clock",
        Phase::Warmup => "warmup",
        Phase::Running => "running",
        Phase::Draining => "draining",
        Phase::Finishing => "finishing",
        Phase::Done => "done",
        Phase::Failed => "failed",
        Phase::Cancelled => "cancelled",
    }
}

fn fmt_num(v: f64) -> String {
    if v.abs() >= 100.0 {
        format!("{v:.1}")
    } else if v.abs() >= 1.0 {
        format!("{v:.2}")
    } else {
        format!("{v:.3}")
    }
}

fn status_str(s: RunStatus) -> &'static str {
    match s {
        RunStatus::Ok => "ok",
        RunStatus::Failed => "failed",
        RunStatus::Cancelled => "cancelled",
    }
}

/// A closed-set value as the report spells it (`same-process`, `direct`).
fn wire_name<T: serde::Serialize + std::fmt::Debug>(v: &T) -> String {
    serde_json::to_value(v)
        .ok()
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_else(|| format!("{v:?}"))
}

/// The final summary table of a report. Every string in it may come from
/// a peer or an imported file, so all of it goes through [`term_safe`].
pub fn print_summary(r: &RunReport, written: Option<&Path>) {
    let mut out = std::io::stdout().lock();
    let _ = writeln!(out);
    let _ = writeln!(out, "  {} · {}", r.workload, term_safe(&r.label));
    match (&r.error, r.error_code) {
        (Some(e), Some(code)) => {
            let _ = writeln!(
                out,
                "  status    {} ({code}): {}",
                status_str(r.status),
                term_safe(e)
            );
        }
        _ => {
            let _ = writeln!(out, "  status    {}", status_str(r.status));
        }
    }
    let peer = r.peer.as_ref().map_or_else(
        || "—".to_string(),
        |p| format!("{} ({})", term_safe(&p.device_name), term_safe(&p.os)),
    );
    let _ = writeln!(
        out,
        "  peer      {peer} · {} · path {}{}",
        wire_name(&r.network.topology),
        wire_name(&r.network.path_label),
        r.network
            .before
            .rtt_ms
            .map(|ms| format!(" · settled {} ms, rtt {ms:.2} ms", r.network.settle_ms))
            .unwrap_or_default()
    );
    let width = r.summary.iter().map(|s| s.label.len()).max().unwrap_or(0);
    for s in &r.summary {
        let value = s.value.map_or_else(|| "—".to_string(), fmt_num);
        let unc = s
            .uncertainty
            .map(|u| format!(" ± {}", fmt_num(u)))
            .unwrap_or_default();
        let _ = writeln!(
            out,
            "  {:<width$}  {:>10}{unc} {}",
            term_safe(&s.label),
            value,
            term_safe(&s.unit)
        );
    }
    for n in &r.notes {
        let _ = writeln!(out, "  note: {}", term_safe(n));
    }
    if let Some(path) = written {
        let _ = writeln!(out, "  report: {}", path.display());
    }
}

fn write_out(dir: &Path, report: &RunReport) -> Option<PathBuf> {
    match write_report(dir, report) {
        Ok(p) => Some(p),
        Err(e) => {
            eprintln_err(&e.context("write the report"));
            None
        }
    }
}

/// Success only for an `ok` run whose report was written: CI relies on the
/// files under `--out-dir`.
fn exit_for(report: &RunReport, written: bool) -> ExitCode {
    if report.status == RunStatus::Ok && written {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

/// `serve`: answer peers until Ctrl-C.
pub async fn serve(args: HubArgs) -> ExitCode {
    let cfg = match hub_config(&args.node, args.profile.as_deref()) {
        Ok(c) => c,
        Err(e) => {
            eprintln_err(&e);
            return ExitCode::FAILURE;
        }
    };
    debug_banner();
    let hub = Hub::new(cfg);
    let printer = spawn_event_printer(&hub, true);
    if let Err(e) = start_hub(&hub).await {
        if e.code != ErrorCode::Cancelled {
            eprintln_err(&e);
        }
        printer.abort();
        return ExitCode::FAILURE;
    }
    eprintln!("[serve] answering peers; Ctrl-C to stop");
    let _ = tokio::signal::ctrl_c().await;
    eprintln!("[serve] stopping; Ctrl-C again to quit at once");
    let _force = ForceQuit::spawn();
    hub.stop().await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    printer.abort();
    ExitCode::SUCCESS
}

/// `peers`: list same-app peers after 5 s of discovery.
pub async fn peers(args: HubArgs) -> ExitCode {
    let cfg = match hub_config(&args.node, args.profile.as_deref()) {
        Ok(c) => c,
        Err(e) => {
            eprintln_err(&e);
            return ExitCode::FAILURE;
        }
    };
    let hub = Hub::new(cfg);
    let printer = spawn_event_printer(&hub, false);
    if let Err(e) = start_hub(&hub).await {
        if e.code != ErrorCode::Cancelled {
            eprintln_err(&e);
        }
        printer.abort();
        return ExitCode::FAILURE;
    }
    tokio::select! {
        _ = tokio::time::sleep(Duration::from_secs(5)) => {}
        _ = tokio::signal::ctrl_c() => {}
    }
    let _force = ForceQuit::spawn();
    let peers = hub.peers().await;
    let mut out = std::io::stdout().lock();
    if peers.is_empty() {
        let _ = writeln!(out, "no other truffle bench peers on this tailnet yet");
    }
    for p in &peers {
        let state = if p.online { "online" } else { "offline" };
        let hello = match (&p.hello, &p.hello_error) {
            (Some(h), _) => format!(
                "{} {} {}{}{}",
                term_safe(&h.app),
                term_safe(&h.tool_version),
                h.build_profile.as_str(),
                if h.accepts_runs_from_you {
                    " · accepts runs from you"
                } else {
                    " · does not accept runs from you"
                },
                h.busy
                    .as_deref()
                    .map(|b| format!(" · busy: {}", term_safe(b)))
                    .unwrap_or_default()
            ),
            (None, Some(e)) => format!("hello failed: {}", term_safe(&e.message)),
            (None, None) => "no hello yet".into(),
        };
        let _ = writeln!(
            out,
            "{:<24} {:<7} {:<12} {:<16} {:<28} {}",
            term_safe(&p.device_name),
            state,
            term_safe(&p.connection),
            p.ip,
            term_safe(p.login_name.as_deref().unwrap_or("?")),
            hello
        );
        let _ = writeln!(out, "    id {}", p.id);
    }
    drop(out);
    hub.stop().await;
    printer.abort();
    ExitCode::SUCCESS
}

/// `run --peer <ref> <workload>`.
pub async fn run(args: RunArgs, cli_out: &Option<PathBuf>) -> ExitCode {
    let params = match build_params(&args.workload, &args.params) {
        Ok(p) => p,
        Err(e) => {
            eprintln_err(&e);
            return ExitCode::FAILURE;
        }
    };
    let cfg = match hub_config(&args.hub.node, args.hub.profile.as_deref()) {
        Ok(c) => c,
        Err(e) => {
            eprintln_err(&e);
            return ExitCode::FAILURE;
        }
    };
    debug_banner();
    let hub = Hub::new(cfg);
    let printer = spawn_event_printer(&hub, false);
    if let Err(e) = start_hub(&hub).await {
        if e.code != ErrorCode::Cancelled {
            eprintln_err(&e);
        }
        printer.abort();
        return ExitCode::FAILURE;
    }
    let run_id = uuid::Uuid::new_v4().to_string();
    let canceller = spawn_canceller(&hub, run_id.clone());
    eprintln!("[run] {} → {}", params.label(), args.peer);
    let report = hub
        .run(
            RunRequest {
                run_id,
                peer: args.peer.clone(),
                params,
                suite_id: None,
            },
            progress_printer(String::new()),
        )
        .await;
    let written = write_out(&out_dir(cli_out), &report);
    print_summary(&report, written.as_deref());
    hub.stop().await;
    canceller.abort();
    printer.abort();
    exit_for(&report, written.is_some())
}

/// Ctrl-C cancels the run or suite `id` (or, once it has ended, lets the
/// teardown finish); a second Ctrl-C quits at once. Abort it only after the
/// hub has stopped, so Ctrl-C is never swallowed during teardown.
fn spawn_canceller(hub: &Arc<Hub>, id: String) -> tokio::task::JoinHandle<()> {
    let hub = hub.clone();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            if hub.cancel(&id) {
                eprintln!("[cancel] cancelling {id}; Ctrl-C again to quit at once");
            } else {
                eprintln!("[cancel] stopping; Ctrl-C again to quit at once");
            }
            if tokio::signal::ctrl_c().await.is_ok() {
                force_quit();
            }
        }
    })
}

async fn start_pair(key_file: Option<&Path>, sidecar: Option<PathBuf>) -> Result<Pair, AppError> {
    let key = auth_key(key_file)?.ok_or_else(|| {
        AppError::invalid_params(
            "pair needs an auth key: set TRUFFLE_BENCH_AUTH_KEY (or TRUFFLE_TEST_AUTHKEY) in the \
             environment or .env, or pass --auth-key-file",
        )
    })?;
    eprintln!("[pair] starting two ephemeral hubs in this process…");
    let started = tokio::select! {
        p = Pair::start(&key, Tool::Cli, sidecar) => Some(p),
        _ = tokio::signal::ctrl_c() => None,
    };
    let pair = started.ok_or_else(AppError::cancelled)??;
    eprintln!("[pair] ready · app id {}", pair.app_id);
    Ok(pair)
}

/// `pair <workload>`.
pub async fn pair(args: PairArgs, cli_out: &Option<PathBuf>) -> ExitCode {
    let params = match build_params(&args.workload, &args.params) {
        Ok(p) => p,
        Err(e) => {
            eprintln_err(&e);
            return ExitCode::FAILURE;
        }
    };
    debug_banner();
    let pair = match start_pair(args.auth_key_file.as_deref(), args.sidecar.clone()).await {
        Ok(p) => p,
        Err(e) => {
            eprintln_err(&e);
            return ExitCode::FAILURE;
        }
    };
    let run_id = uuid::Uuid::new_v4().to_string();
    let canceller = spawn_canceller(&pair.initiator, run_id.clone());
    eprintln!("[run] {}", params.label());
    let report = pair
        .run(run_id, params, progress_printer(String::new()))
        .await;
    let written = write_out(&out_dir(cli_out), &report);
    print_summary(&report, written.as_deref());
    pair.stop().await;
    canceller.abort();
    exit_for(&report, written.is_some())
}

/// `suite --profile <p> (--pair | --peer <ref>)`.
pub async fn suite(args: SuiteArgs, cli_out: &Option<PathBuf>) -> ExitCode {
    let overrides: Vec<(String, Value)> = match args
        .param
        .iter()
        .map(|p| parse_param(p))
        .collect::<Result<_, _>>()
    {
        Ok(o) => o,
        Err(e) => {
            eprintln_err(&e);
            return ExitCode::FAILURE;
        }
    };
    let items = match suites::resolve(&args.profile, &overrides) {
        Ok(items) => items,
        Err(e) => {
            eprintln_err(&e);
            return ExitCode::FAILURE;
        }
    };
    debug_banner();
    if args.profile == "nightly"
        && BuildProfile::current() == BuildProfile::Debug
        && !args.allow_debug
    {
        eprintln!(
            "error: the nightly profile refuses debug builds (numbers would not be comparable); \
             build with --release or pass --allow-debug"
        );
        return ExitCode::FAILURE;
    }
    // Keep the pair alive for the whole suite.
    let mut pair_holder = None;
    let (hub, peer) = if args.pair {
        match start_pair(
            args.node.auth_key_file.as_deref(),
            args.node.sidecar.clone(),
        )
        .await
        {
            Ok(p) => {
                let hub = p.initiator.clone();
                let peer = p.responder_ref().to_string();
                pair_holder = Some(p);
                (hub, peer)
            }
            Err(e) => {
                eprintln_err(&e);
                return ExitCode::FAILURE;
            }
        }
    } else {
        let cfg = match hub_config(&args.node, args.hub_profile.as_deref()) {
            Ok(c) => c,
            Err(e) => {
                eprintln_err(&e);
                return ExitCode::FAILURE;
            }
        };
        let hub = Hub::new(cfg);
        if let Err(e) = start_hub(&hub).await {
            if e.code != ErrorCode::Cancelled {
                eprintln_err(&e);
            }
            return ExitCode::FAILURE;
        }
        let peer = args.peer.clone().unwrap_or_default();
        // A debug build on either side is not comparable (§7.4): with
        // --peer the other side is a separate binary, so ask it.
        if args.profile == "nightly" && !args.allow_debug {
            if let Err(e) = refuse_debug_peer(&hub, &peer).await {
                eprintln_err(&e);
                hub.stop().await;
                return ExitCode::FAILURE;
            }
        }
        (hub, peer)
    };
    let printer = (pair_holder.is_none()).then(|| spawn_event_printer(&hub, false));
    let suite_id = uuid::Uuid::new_v4().to_string();
    let canceller = spawn_canceller(&hub, suite_id.clone());
    let dir = out_dir(cli_out);
    eprintln!(
        "[suite] {} · {} items · about {:.0} s of workload time",
        args.profile,
        items.len(),
        suites::total_nominal_secs(&items)
    );
    let item_printer: Mutex<(Option<u32>, Option<truffle_bench::ProgressSink>)> =
        Mutex::new((None, None));
    let sink: truffle_bench::SuiteProgressSink = Arc::new(move |sp: SuiteProgress| {
        let mut st = item_printer.lock().unwrap_or_else(PoisonError::into_inner);
        if st.0 != Some(sp.index) {
            eprintln!("\n[suite {}/{}] {}", sp.index + 1, sp.total, sp.item_label);
            st.0 = Some(sp.index);
            st.1 = Some(progress_printer("  ".into()));
        }
        if let Some(p) = &st.1 {
            p(sp.run);
        }
    });
    let mut written_count = 0usize;
    let result = hub
        .suite_with(
            SuiteRequest {
                suite_id,
                profile: args.profile.clone(),
                peer,
                overrides,
            },
            sink,
            |report| {
                let written = write_out(&dir, report);
                written_count += usize::from(written.is_some());
                print_summary(report, written.as_deref());
            },
        )
        .await;
    match pair_holder {
        Some(p) => p.stop().await,
        None => hub.stop().await,
    }
    canceller.abort();
    if let Some(p) = printer {
        p.abort();
    }
    let unwritten = result.run_ids.len().saturating_sub(written_count);
    eprintln!(
        "\n[suite] {} item{} run, {} failed{}; {written_count} report{} in {}{}",
        result.run_ids.len(),
        if result.run_ids.len() == 1 { "" } else { "s" },
        result.failed,
        if result.cancelled { ", cancelled" } else { "" },
        if written_count == 1 { "" } else { "s" },
        dir.display(),
        if unwritten > 0 {
            format!(" ({unwritten} could not be written)")
        } else {
            String::new()
        }
    );
    // Exactly one report per item (§10): a report that could not be
    // written fails the suite, as CI relies on the files.
    if result.failed == 0
        && !result.cancelled
        && !result.run_ids.is_empty()
        && written_count == result.run_ids.len()
    {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

/// `nightly` refuses a debug (or unknown) build on the peer, as on this side.
async fn refuse_debug_peer(hub: &Hub, peer: &str) -> Result<(), AppError> {
    let p = hub.resolve_peer(peer, 5_000).await?;
    let hello = hub.hello_peer(&p.tailscale_id).await?;
    if hello.build_profile != BuildProfile::Release {
        return Err(AppError::invalid_params(format!(
            "the nightly profile refuses debug builds, and the peer {} is a {} build \
             (numbers would not be comparable); run it from a release build or pass --allow-debug",
            term_safe(&hello.device_name),
            hello.build_profile.as_str()
        )));
    }
    Ok(())
}

/// `report <path>…`: print v1/v2/v3 summaries.
pub fn report(args: ReportArgs) -> ExitCode {
    let mut ok = 0usize;
    let mut bad = 0usize;
    for path in &args.paths {
        for (source, result) in read_reports(path) {
            match result {
                Ok(r) => {
                    ok += 1;
                    println!("\n{}", term_safe(&source));
                    println!(
                        "  tool {} · methodology {} · {}",
                        wire_name(&r.tool),
                        term_safe(&r.methodology),
                        term_safe(&r.started_at)
                    );
                    print_summary(&r, None);
                }
                Err(e) => {
                    bad += 1;
                    eprintln!(
                        "\n{}\n  not a report: {}",
                        term_safe(&source),
                        term_safe(&e)
                    );
                }
            }
        }
    }
    eprintln!(
        "\n{ok} report{} read, {bad} skipped",
        if ok == 1 { "" } else { "s" }
    );
    if ok > 0 {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn params_merge_json_then_flags() {
        let args = ParamArgs {
            param: vec!["streams=4".into(), "direction=download".into()],
            params_json: Some(r#"{"duration_secs": 5, "streams": 2}"#.into()),
        };
        let p = build_params("tcp", &args).unwrap();
        let v = serde_json::to_value(&p).unwrap();
        assert_eq!(v["streams"], 4);
        assert_eq!(v["direction"], "download");
        assert_eq!(v["duration_secs"], 5);
        assert!(build_params(
            "tcp",
            &ParamArgs {
                param: vec!["streams=99".into()],
                params_json: None
            }
        )
        .is_err());
        assert!(build_params("nope", &ParamArgs::default()).is_err());
        assert!(build_params(
            "path",
            &ParamArgs {
                param: vec![],
                params_json: Some(r#"{"workload": "tcp"}"#.into())
            }
        )
        .is_err());
    }

    #[test]
    fn param_values_are_json_when_possible() {
        assert_eq!(parse_param("count=5").unwrap().1, Value::from(5));
        assert_eq!(parse_param("nodelay=false").unwrap().1, Value::from(false));
        assert_eq!(
            parse_param("profile=1080p30").unwrap().1,
            Value::from("1080p30")
        );
        assert_eq!(
            parse_param("tcp.streams=2").unwrap().0,
            "tcp.streams".to_string()
        );
        assert!(parse_param("novalue").is_err());
        assert!(parse_param("=3").is_err());
    }

    #[test]
    fn peer_text_cannot_drive_the_terminal() {
        assert!(matches!(term_safe("studio · tcp"), Cow::Borrowed(_)));
        assert_eq!(
            term_safe("evil\u{1b}]52;c;aGk=\u{7}name\r\n\u{9b}2J\u{7f}"),
            "evil\u{FFFD}]52;c;aGk=\u{FFFD}name\u{FFFD}\u{FFFD}\u{FFFD}2J\u{FFFD}"
        );
    }

    #[test]
    fn a_report_that_was_not_written_fails_the_command() {
        let report: RunReport = serde_json::from_str(include_str!(
            "../../schema/fixtures/report-tcp-throughput-upload.json"
        ))
        .unwrap();
        assert_eq!(report.status, RunStatus::Ok);
        assert_eq!(exit_for(&report, true), ExitCode::SUCCESS);
        assert_eq!(exit_for(&report, false), ExitCode::FAILURE);
    }

    #[test]
    fn key_file_first_line_wins() {
        let dir = tempfile::tempdir().unwrap();
        let f = dir.path().join("key");
        std::fs::write(&f, "tskey-auth-…\nsecond line\n").unwrap();
        assert_eq!(auth_key(Some(&f)).unwrap().as_deref(), Some("tskey-auth-…"));
        std::fs::write(&f, "\n").unwrap();
        assert!(auth_key(Some(&f)).is_err());
        assert!(auth_key(Some(&dir.path().join("missing"))).is_err());
    }

    #[test]
    fn cli_parses() {
        use clap::CommandFactory;
        Cli::command().debug_assert();
        let cli = Cli::try_parse_from([
            "truffle-bench",
            "run",
            "--peer",
            "studio",
            "tcp",
            "--param",
            "streams=2",
            "--out-dir",
            "/tmp/x",
        ])
        .unwrap();
        assert_eq!(cli.out_dir, Some(PathBuf::from("/tmp/x")));
        assert!(Cli::try_parse_from(["truffle-bench", "suite", "--profile", "smoke"]).is_err());
        assert!(Cli::try_parse_from([
            "truffle-bench",
            "suite",
            "--profile",
            "smoke",
            "--pair",
            "--peer",
            "x"
        ])
        .is_err());
        assert!(
            Cli::try_parse_from(["truffle-bench", "suite", "--profile", "smoke", "--pair"]).is_ok()
        );
        assert!(Cli::try_parse_from(["truffle-bench", "serve", "--auth-key", "x"]).is_err());
        // The data dir default reads as a path under the OS data dir.
        let help = Cli::command()
            .find_subcommand_mut("serve")
            .unwrap()
            .render_long_help()
            .to_string();
        assert!(help.contains("<OS data dir>/truffle-bench-cli"), "{help}");
    }
}
