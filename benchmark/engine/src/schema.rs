//! The JSON Schema and canonical fixtures the app is checked against
//! (RFC 027 §7).
//!
//! Rust is the source of truth. [`report_schema`] generates
//! `benchmark/schema/report.v3.schema.json` (root: [`RunReport`], with every
//! other type shared with the app under `$defs`), and [`fixtures`] builds one
//! canonical JSON example per type and variant for
//! `benchmark/schema/fixtures/`: every [`HubEvent`] variant, [`NodeState`]
//! per state, every [`WorkloadParams`] variant (plus the mode variants),
//! [`Progress`], a [`RunReport`] per workload mode (`report-<mode>.json`,
//! from each module's `examples()`) and per workload (`report-<workload>-ok`
//! / `-failed`, its default mode), and the app command types.
//!
//! The `schema_is_current` test regenerates both and fails on drift;
//! `UPDATE_SCHEMA=1 cargo test -p truffle-bench schema_is_current` rewrites
//! them. Files are written with sorted keys, so the bytes do not depend on
//! serde_json's `preserve_order` feature.

use std::path::{Path, PathBuf};

use schemars::generate::SchemaSettings;
use serde::Serialize;
use serde_json::{json, Value};

use crate::control::{
    ClockAck, ClockReq, FinishAck, FinishReq, FtDoneMsg, HelloAck, HelloReq, PrepareAck,
    PrepareReq, StartAck, StartReq, TickMsg,
};
use crate::error::{AppError, ErrorCode};
use crate::hub::{RunRequest, SuiteRequest};
use crate::params::{
    Direction, EgressParams, HttpConnection, HttpParams, HttpTarget, OneWayDirection, QuicParams,
    StoreMode, StoreParams, StreamMode, TcpParams, UdpMode, UdpParams, VideoParams, VideoTransport,
    Workload, WorkloadParams, WsMode, WsParams,
};
use crate::report::{
    NetworkInfo, Resources, RunReport, METHODOLOGY, NOTE_NOT_A_LINK, SCHEMA_VERSION,
};
use crate::types::{
    ActiveRun, AppInfo, BenchPeer, BuildProfile, ClockInfo, ClockOffset, Distribution,
    HealthSummary, HubEvent, ImportResult, LocalInfo, NodeEnv, NodeState, NodeStatus, PathBytes,
    PathLabel, PathSnapshot, PathTimelinePoint, PeerHello, PeerProbe, Phase, Progress,
    RemoteRunState, ReportTool, ResourceStats, RunStatus, RunSummary, Sample, Settings,
    SuiteProfile, SuiteProgress, SuiteResult, Theme, Topology,
};
use crate::workloads;

/// `benchmark/schema/` in this checkout.
pub fn schema_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../schema")
}

/// File name of the generated schema.
pub const SCHEMA_FILE: &str = "report.v3.schema.json";

/// The v3 report schema: root [`RunReport`], every app-facing type in `$defs`.
pub fn report_schema() -> Value {
    let mut gen = SchemaSettings::draft2020_12().into_generator();
    macro_rules! defs {
        ($($t:ty),* $(,)?) => { $( let _ = gen.subschema_for::<$t>(); )* };
    }
    defs!(
        HubEvent,
        NodeState,
        LocalInfo,
        BenchPeer,
        PeerProbe,
        PeerHello,
        ActiveRun,
        Progress,
        SuiteProgress,
        SuiteResult,
        SuiteProfile,
        RunSummary,
        AppInfo,
        Settings,
        ImportResult,
        AppError,
        WorkloadParams,
        RunRequest,
        SuiteRequest,
        HelloReq,
        HelloAck,
        ClockReq,
        ClockAck,
        PrepareReq,
        PrepareAck,
        StartReq,
        StartAck,
        FinishReq,
        FinishAck,
        crate::control::CancelMsg,
        TickMsg,
        FtDoneMsg,
        crate::dataplane::ReceiverStats,
        // Every workload's `results` (named `<Workload>Results`).
        workloads::path::Results,
        workloads::tcp::Results,
        workloads::udp::Results,
        workloads::quic::Results,
        workloads::ws::Results,
        workloads::file_transfer::Results,
        workloads::synced_store::Results,
        workloads::http::Results,
        workloads::video::Results,
        workloads::egress::Results,
    );
    let mut schema =
        serde_json::to_value(gen.into_root_schema_for::<RunReport>()).unwrap_or(Value::Null);
    if let Some(obj) = schema.as_object_mut() {
        obj.insert(
            "description".into(),
            Value::String(
                "truffle bench run report, schema v3 (RFC 027 §7.4). GENERATED from the Rust \
                 types in benchmark/engine; do not edit. `$defs` also holds every other type \
                 the app exchanges with the engine."
                    .into(),
            ),
        );
    }
    schema
}

/// JSON with object keys sorted at every level, 2-space indented, with a
/// trailing newline.
pub fn pretty(v: &Value) -> String {
    let mut out = String::new();
    write_pretty(v, 0, &mut out);
    out.push('\n');
    out
}

fn write_pretty(v: &Value, indent: usize, out: &mut String) {
    let pad = |n: usize, out: &mut String| out.extend(std::iter::repeat_n(' ', n));
    match v {
        Value::Object(map) if !map.is_empty() => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            out.push_str("{\n");
            for (i, k) in keys.iter().enumerate() {
                pad(indent + 2, out);
                out.push_str(&Value::String((*k).clone()).to_string());
                out.push_str(": ");
                write_pretty(&map[*k], indent + 2, out);
                if i + 1 < keys.len() {
                    out.push(',');
                }
                out.push('\n');
            }
            pad(indent, out);
            out.push('}');
        }
        Value::Array(items) if !items.is_empty() => {
            out.push_str("[\n");
            for (i, item) in items.iter().enumerate() {
                pad(indent + 2, out);
                write_pretty(item, indent + 2, out);
                if i + 1 < items.len() {
                    out.push(',');
                }
                out.push('\n');
            }
            pad(indent, out);
            out.push(']');
        }
        other => out.push_str(&other.to_string()),
    }
}

fn val<T: Serialize>(t: &T) -> Value {
    serde_json::to_value(t).unwrap_or(Value::Null)
}

// ---------------------------------------------------------------------------
// Example values (fixed, so fixtures never drift on their own)
// ---------------------------------------------------------------------------

const AT: &str = "2026-10-16T08:00:00.000Z";
const AT_END: &str = "2026-10-16T08:00:14.212Z";
const SINCE_MS: u64 = 1_760_601_600_000;
const TOOL_VERSION: &str = "0.1.0";
const TRUFFLE_VERSION: &str = "0.7.17";

fn example_health() -> HealthSummary {
    HealthSummary {
        state: "Running".into(),
        healthy: true,
        warnings: vec![],
        key_expiry: Some("2027-04-14T08:00:00Z".into()),
    }
}

fn example_local() -> LocalInfo {
    LocalInfo {
        device_name: "studio".into(),
        device_id: "01JA7Q8Z3V5X2K9M4N6P8R0T1W".into(),
        tailscale_id: "nSTUDIO1CNTRL".into(),
        tailscale_hostname: "truffle-truffle-bench-studio".into(),
        tailscale_ip: Some("100.64.0.1".into()),
        dns_name: Some("truffle-truffle-bench-studio.tail1234.ts.net.".into()),
        tailnet: Some("tail1234.ts.net".into()),
        login_name: Some("james@example.com".into()),
        app_id: "truffle-bench".into(),
        health: example_health(),
        sidecar_path: "/Applications/truffle bench.app/Contents/MacOS/sidecar-slim".into(),
        hub_start_ms: Some(2_310),
    }
}

fn example_hello(device: &str, os: &str, accepts: bool) -> PeerHello {
    PeerHello {
        protocol: crate::PROTOCOL,
        app: "truffle-bench-app".into(),
        tool_version: TOOL_VERSION.into(),
        truffle_version: TRUFFLE_VERSION.into(),
        build_profile: BuildProfile::Release,
        git_sha: Some("3cdfbaa0c1d2e3f4a5b6c7d8e9f0a1b2c3d4e5f6".into()),
        sidecar_sha256: Some(
            "9f2c1e7a4b8d3f60c5a1e2d4b6f8a0c2e4d6f8a1b3c5e7f9a2b4c6d8e0f1a3b5".into(),
        ),
        device_name: device.into(),
        device_id: Some("01JA7QB2M8H4D6F9K1N3Q5S7U9".into()),
        os: os.into(),
        arch: "aarch64".into(),
        logical_cpus: Some(12),
        login_name: Some("james@example.com".into()),
        host_fingerprint: accepts.then(|| "a1b2c3d4e5f60718".into()),
        host_fingerprint_weak: false,
        workloads: Workload::ALL
            .iter()
            .map(|w| w.as_str().to_string())
            .collect(),
        accepts_runs_from_you: accepts,
        busy: None,
    }
}

fn example_peer() -> BenchPeer {
    BenchPeer {
        id: "nLAPTOP2CNTRL".into(),
        device_id: Some("01JA7QB2M8H4D6F9K1N3Q5S7U9".into()),
        device_name: "laptop".into(),
        hostname: "truffle-truffle-bench-laptop".into(),
        ip: "100.64.0.2".into(),
        os: Some("macOS".into()),
        online: true,
        ws_connected: true,
        connection: "direct".into(),
        login_name: Some("james@example.com".into()),
        same_login: true,
        same_host: Some(false),
        tagged: false,
        last_seen: Some(AT.into()),
        hello: Some(example_hello("laptop", "macos", true)),
        hello_error: None,
        rtt_ms: Some(1.2),
    }
}

fn example_snapshot(conn: &str, rtt: f64) -> PathSnapshot {
    PathSnapshot {
        at: AT.into(),
        connection: Some(conn.into()),
        rtt_ms: Some(rtt),
        endpoint: Some("192.0.2.10:41641".into()),
        error: None,
    }
}

fn example_env(device: &str, os: &str, fp: &str, cli: bool) -> NodeEnv {
    NodeEnv {
        device_name: device.into(),
        os: os.into(),
        arch: "aarch64".into(),
        logical_cpus: Some(12),
        tool: if cli {
            "truffle-bench-cli".into()
        } else {
            "truffle-bench-app".into()
        },
        tool_version: TOOL_VERSION.into(),
        truffle_version: TRUFFLE_VERSION.into(),
        build_profile: BuildProfile::Release,
        rustc: cli.then(|| "rustc 1.91.0 (f8297e351 2025-10-28)".into()),
        git_sha: Some("3cdfbaa0c1d2e3f4a5b6c7d8e9f0a1b2c3d4e5f6".into()),
        git_dirty: cli.then_some(false),
        sidecar_path: cli.then(|| "crates/truffle-core/tests/test-sidecar".into()),
        sidecar_sha256: Some(
            "9f2c1e7a4b8d3f60c5a1e2d4b6f8a0c2e4d6f8a1b3c5e7f9a2b4c6d8e0f1a3b5".into(),
        ),
        tailscale_ip: Some(if cli { "100.64.0.1" } else { "100.64.0.2" }.into()),
        host_fingerprint: Some(fp.into()),
        host_fingerprint_weak: false,
        on_battery: cli.then_some(false),
        app_foreground_pct: None,
    }
}

fn example_resources(cpu_ms: f64, per_mb: Option<f64>) -> ResourceStats {
    ResourceStats {
        cpu_ms,
        cpu_pct_avg: cpu_ms / 100.0,
        cpu_pct_max: cpu_ms / 80.0,
        rss_mb_max: 48.2,
        cpu_ms_per_mb: per_mb,
        sidecar_cpu_ms: Some(cpu_ms * 1.6),
        sidecar_cpu_pct_avg: Some(cpu_ms * 1.6 / 100.0),
        sidecar_cpu_pct_max: Some(cpu_ms * 1.6 / 80.0),
        sidecar_rss_mb_max: Some(61.9),
        sidecar_cpu_ms_per_mb: per_mb.map(|p| p * 1.6),
    }
}

fn example_clock() -> ClockInfo {
    let before = ClockOffset {
        offset_ns: -1_284_310,
        delay_ns: 824_000,
        uncertainty_ns: 412_000,
        at_ns: 1_760_601_600_400_000_000,
        samples: 16,
    };
    let after = ClockOffset {
        offset_ns: -1_253_300,
        at_ns: 1_760_601_610_400_000_000,
        ..before
    };
    crate::clock::ClockMap::new(before, Some(after)).info()
}

/// A full, deterministic report around a workload's example output (its
/// default mode).
pub fn example_report(w: Workload, status: RunStatus) -> RunReport {
    let failed = status != RunStatus::Ok;
    let run_id = format!(
        "example-{}-{}",
        w.as_str(),
        if failed { "failed" } else { "ok" }
    );
    let example = workloads::mode_examples()
        .into_iter()
        .find(|e| e.params.workload() == w)
        .unwrap_or_else(|| workloads::ModeExample {
            mode: w.as_str(),
            params: w.default_params(),
            output: workloads::example_output(w),
            clock: false,
        });
    example_report_for(&example, status, run_id)
}

/// A full, deterministic report around one mode's example
/// (`report-<mode>.json`).
pub fn example_report_for(
    example: &workloads::ModeExample,
    status: RunStatus,
    run_id: String,
) -> RunReport {
    let out = example.output.clone();
    let params = example.params.clone();
    let w = params.workload();
    let time_based = params.time_based();
    let timeline: Vec<PathTimelinePoint> = if time_based {
        (1..=6)
            .map(|i| PathTimelinePoint {
                t_ms: 500 * i,
                connection: Some("direct".into()),
                rtt_ms: Some(3.0 + i as f64 * 0.4),
            })
            .collect()
    } else {
        Vec::new()
    };
    let failed = status != RunStatus::Ok;
    let mut report = RunReport {
        schema_version: SCHEMA_VERSION,
        run_id,
        suite_id: None,
        tool: ReportTool::TruffleBenchCli,
        tool_version: TOOL_VERSION.into(),
        truffle_version: TRUFFLE_VERSION.into(),
        methodology: METHODOLOGY.into(),
        workload: w,
        label: params.label(),
        status,
        error: failed.then(|| "finish: no finish.ack from the peer within 41 s".into()),
        error_code: failed.then_some(ErrorCode::Timeout),
        started_at: AT.into(),
        finished_at: AT_END.into(),
        duration_ms: 14_212,
        git_sha: Some("3cdfbaa0c1d2e3f4a5b6c7d8e9f0a1b2c3d4e5f6".into()),
        git_dirty: Some(false),
        compare_key: String::new(),
        local: example_env("studio-cli", "macos", "0f1e2d3c4b5a6978", true),
        peer: Some(example_env("laptop", "macos", "a1b2c3d4e5f60718", false)),
        network: NetworkInfo {
            topology: Topology::SeparateHosts,
            settle_ms: 412,
            before: example_snapshot("direct", 3.1),
            after: example_snapshot("direct", 3.3),
            clock: example.clock.then(example_clock),
            path_timeline: timeline.clone(),
            loaded_rtt_ms: time_based.then(|| {
                Distribution::from_values(
                    &timeline.iter().filter_map(|p| p.rtt_ms).collect::<Vec<_>>(),
                )
            }),
            path_bytes: Some(PathBytes {
                direct: 1_160_412_880,
                derp: 1_240,
                peer_relay: 0,
            }),
            derp_share: Some(1_240.0 / 1_160_414_120.0),
            path_label: PathLabel::Direct,
        },
        params,
        summary: if failed { Vec::new() } else { out.summary },
        results: if failed { json!({}) } else { out.results },
        series: out.series,
        resources: Resources {
            local: Some(example_resources(812.0, Some(0.71))),
            peer: Some(example_resources(1_044.0, Some(0.92))),
            combined: None,
        },
        overlapping_runs: 0,
        notes: out.notes,
    };
    if failed {
        report.add_note("ended during the running phase");
    }
    report.apply_honesty_notes();
    report.compare_key = report.compute_compare_key();
    report
}

fn node_state(status: NodeStatus) -> NodeState {
    let mut st = NodeState {
        state: status,
        auth_url: None,
        error: None,
        local: None,
        tailscale_hostname: Some("truffle-truffle-bench-studio".into()),
        since_ms: SINCE_MS,
    };
    match status {
        NodeStatus::NeedsLogin => {
            st.auth_url = Some("https://login.tailscale.com/a/0123456789abcdef".into())
        }
        NodeStatus::Running => st.local = Some(example_local()),
        NodeStatus::Error => {
            st.error = Some(AppError::new(
                ErrorCode::StateInUse,
                "Another truffle bench is using this profile. Quit it, or start this one with --profile <name>.",
            ))
        }
        _ => {}
    }
    st
}

fn example_progress(phase: Phase) -> Progress {
    let mut p = Progress::new("2f0c7a1e-5b3d-4c8a-9e6f-1a2b3c4d5e6f", None, 5_250, phase);
    match phase {
        Phase::Running => {
            p.fraction = Some(0.43);
            p.samples = vec![
                Sample::new("goodput_mbps", 912.4),
                Sample::new("sent_mbps", 930.2),
            ];
        }
        Phase::Failed => {
            p.message = Some("finish: no finish.ack from the peer within 41 s".into());
        }
        Phase::Done => p.fraction = Some(1.0),
        _ => {}
    }
    p
}

/// Every fixture: `(file name, canonical JSON)`.
pub fn fixtures() -> Vec<(String, Value)> {
    let mut out: Vec<(String, Value)> = Vec::new();
    let mut add = |name: String, v: Value| out.push((format!("{name}.json"), v));

    // HubEvent ×3.
    add(
        "hub-event-node".into(),
        val(&HubEvent::Node(Box::new(node_state(NodeStatus::Running)))),
    );
    add(
        "hub-event-peers".into(),
        val(&HubEvent::Peers {
            peers: vec![example_peer()],
        }),
    );
    add(
        "hub-event-remote-run".into(),
        val(&HubEvent::RemoteRun {
            run_id: "2f0c7a1e-5b3d-4c8a-9e6f-1a2b3c4d5e6f".into(),
            peer: "laptop".into(),
            peer_id: "nLAPTOP2CNTRL".into(),
            workload: "tcp".into(),
            state: RemoteRunState::Prepared,
        }),
    );
    // NodeState per state.
    for (name, st) in [
        ("idle", NodeStatus::Idle),
        ("starting", NodeStatus::Starting),
        ("needs-login", NodeStatus::NeedsLogin),
        ("needs-approval", NodeStatus::NeedsApproval),
        ("running", NodeStatus::Running),
        ("stopping", NodeStatus::Stopping),
        ("stopped", NodeStatus::Stopped),
        ("error", NodeStatus::Error),
    ] {
        add(format!("node-state-{name}"), val(&node_state(st)));
    }
    // WorkloadParams: every variant, plus the mode variants.
    for p in WorkloadParams::defaults() {
        add(format!("params-{}", p.workload().as_str()), val(&p));
    }
    let modes: Vec<(&str, WorkloadParams)> = vec![
        (
            "tcp-latency",
            WorkloadParams::Tcp(TcpParams {
                mode: StreamMode::Latency,
                ..TcpParams::default()
            }),
        ),
        (
            "tcp-bidirectional",
            WorkloadParams::Tcp(TcpParams {
                direction: Direction::Bidirectional,
                streams: 4,
                ..TcpParams::default()
            }),
        ),
        (
            "udp-echo",
            WorkloadParams::Udp(UdpParams {
                mode: UdpMode::Echo,
                ..UdpParams::default()
            }),
        ),
        (
            "udp-ramp",
            WorkloadParams::Udp(UdpParams {
                mode: UdpMode::Ramp,
                direction: OneWayDirection::Download,
                ..UdpParams::default()
            }),
        ),
        (
            "quic-latency",
            WorkloadParams::Quic(QuicParams {
                mode: StreamMode::Latency,
                ..QuicParams::default()
            }),
        ),
        (
            "ws-stream",
            WorkloadParams::Ws(WsParams {
                mode: WsMode::Stream,
                rate_per_sec: Some(1000),
                ..WsParams::default()
            }),
        ),
        (
            "synced-store-burst",
            WorkloadParams::SyncedStore(StoreParams {
                mode: StoreMode::Burst,
                ..StoreParams::default()
            }),
        ),
        (
            "http-direct-new",
            WorkloadParams::Http(HttpParams {
                target: HttpTarget::Direct,
                connection: HttpConnection::New,
                ..HttpParams::default()
            }),
        ),
        (
            "video-quic",
            WorkloadParams::Video(VideoParams {
                transport: VideoTransport::Quic,
                ..VideoParams::default()
            }),
        ),
        (
            "egress-download",
            WorkloadParams::Egress(EgressParams {
                direction: OneWayDirection::Download,
                ..EgressParams::default()
            }),
        ),
    ];
    for (name, p) in modes {
        add(format!("params-{name}"), val(&p));
    }
    // Progress.
    for (name, phase) in [
        ("running", Phase::Running),
        ("done", Phase::Done),
        ("failed", Phase::Failed),
    ] {
        add(format!("progress-{name}"), val(&example_progress(phase)));
    }
    // RunReport per workload mode (the module's own output code on
    // realistic synthetic measurements).
    for example in workloads::mode_examples() {
        add(
            format!("report-{}", example.mode),
            val(&example_report_for(
                &example,
                RunStatus::Ok,
                format!("example-{}", example.mode),
            )),
        );
    }
    // The Progress sample keys each mode can set (`workloads::live_keys`),
    // by example name: the app's live KPIs and charts read only these.
    add(
        "live-keys".into(),
        Value::Object(
            workloads::mode_examples()
                .iter()
                .map(|e| (e.mode.to_string(), json!(workloads::live_keys(&e.params))))
                .collect(),
        ),
    );
    // RunReport per workload (its default mode), ok and failed.
    for w in Workload::ALL {
        add(
            format!("report-{}-ok", w.as_str()),
            val(&example_report(w, RunStatus::Ok)),
        );
        add(
            format!("report-{}-failed", w.as_str()),
            val(&example_report(w, RunStatus::Failed)),
        );
    }
    let same_process = {
        let mut r = example_report(Workload::Tcp, RunStatus::Ok);
        r.run_id = "example-tcp-pair".into();
        r.network.topology = Topology::SameProcess;
        r.resources = Resources {
            local: None,
            peer: None,
            combined: Some(example_resources(2_390.0, Some(2.1))),
        };
        r.notes.retain(|n| n != NOTE_NOT_A_LINK);
        r.apply_honesty_notes();
        r.compare_key = r.compute_compare_key();
        r
    };
    add("report-tcp-same-process".into(), val(&same_process));
    // App command types. The History row is the in-process tcp run's, so
    // `flags` is not empty (`same-process`).
    add("run-summary".into(), val(&same_process.to_summary()));
    add(
        "app-info".into(),
        val(&AppInfo {
            app_version: TOOL_VERSION.into(),
            truffle_version: TRUFFLE_VERSION.into(),
            data_dir: "/Users/james/Library/Application Support/com.vibecook.truffle-bench".into(),
            profile: None,
            os: "macos".into(),
            build_profile: BuildProfile::Release,
            sidecar_path: Some(
                "/Applications/truffle bench.app/Contents/MacOS/sidecar-slim".into(),
            ),
            sidecar_sha256: Some(
                "9f2c1e7a4b8d3f60c5a1e2d4b6f8a0c2e4d6f8a1b3c5e7f9a2b4c6d8e0f1a3b5".into(),
            ),
        }),
    );
    add(
        "settings".into(),
        val(&Settings {
            device_name: "studio".into(),
            allow_other_logins: false,
            theme: Theme::System,
            target_peer: Some("01JA7QB2M8H4D6F9K1N3Q5S7U9".into()),
        }),
    );
    add("bench-peer".into(), val(&example_peer()));
    add(
        "peer-probe".into(),
        val(&PeerProbe {
            peer: example_peer(),
            hello: example_hello("laptop", "macos", true),
            path: example_snapshot("direct", 1.2),
        }),
    );
    for profile in crate::suites::profiles() {
        add(format!("suite-profile-{}", profile.id), val(&profile));
    }
    add(
        "app-error".into(),
        val(&AppError::new(
            ErrorCode::BeingMeasured,
            "this device is being measured by laptop (tcp)",
        )),
    );
    add(
        "suite-progress".into(),
        val(&SuiteProgress {
            suite_id: "6a1d2c3b-4e5f-4a6b-8c7d-9e0f1a2b3c4d".into(),
            index: 1,
            total: 10,
            item_label: "TCP throughput · upload · 1 stream · 3 s".into(),
            run: Progress {
                suite_id: Some("6a1d2c3b-4e5f-4a6b-8c7d-9e0f1a2b3c4d".into()),
                ..example_progress(Phase::Running)
            },
        }),
    );
    add(
        "suite-result".into(),
        val(&SuiteResult {
            suite_id: "6a1d2c3b-4e5f-4a6b-8c7d-9e0f1a2b3c4d".into(),
            run_ids: vec![
                "2f0c7a1e-5b3d-4c8a-9e6f-1a2b3c4d5e6f".into(),
                "8b9c0d1e-2f3a-4b5c-8d6e-7f8091a2b3c4".into(),
            ],
            failed: 0,
            cancelled: false,
        }),
    );
    add(
        "active-run".into(),
        val(&ActiveRun {
            run_id: "2f0c7a1e-5b3d-4c8a-9e6f-1a2b3c4d5e6f".into(),
            suite_id: Some("6a1d2c3b-4e5f-4a6b-8c7d-9e0f1a2b3c4d".into()),
            suite_profile: Some("smoke".into()),
            suite_index: Some(1),
            suite_total: Some(10),
            workload: "tcp".into(),
            peer_name: "laptop".into(),
            started_at: AT.into(),
            last: Some(Progress {
                suite_id: Some("6a1d2c3b-4e5f-4a6b-8c7d-9e0f1a2b3c4d".into()),
                ..example_progress(Phase::Running)
            }),
        }),
    );
    add(
        "import-result".into(),
        val(&ImportResult {
            imported: 12,
            skipped: 1,
        }),
    );
    out
}

/// Compare (or, with `update`, rewrite) the schema and fixtures under `dir`.
/// Returns the paths that drifted (empty when everything is current).
pub fn sync(dir: &Path, update: bool) -> Result<Vec<String>, AppError> {
    let mut drift = Vec::new();
    let fixtures_dir = dir.join("fixtures");
    let mut wanted: Vec<(PathBuf, String)> =
        vec![(dir.join(SCHEMA_FILE), pretty(&report_schema()))];
    for (name, v) in fixtures() {
        wanted.push((fixtures_dir.join(name), pretty(&v)));
    }
    let names: std::collections::HashSet<PathBuf> = wanted.iter().map(|w| w.0.clone()).collect();
    for (path, text) in &wanted {
        // A checkout with `core.autocrlf` (Windows) has CRLF line endings;
        // the content is what must match. Updates still write LF.
        let current = std::fs::read_to_string(path)
            .ok()
            .map(|s| s.replace("\r\n", "\n"));
        if current.as_deref() != Some(text.as_str()) {
            drift.push(path.display().to_string());
            if update {
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent)
                        .map_err(|e| AppError::io(format!("create {}", parent.display()), &e))?;
                }
                std::fs::write(path, text)
                    .map_err(|e| AppError::io(format!("write {}", path.display()), &e))?;
            }
        }
    }
    // Stale fixtures (a renamed or removed example) are drift too.
    if let Ok(entries) = std::fs::read_dir(&fixtures_dir) {
        for e in entries.flatten() {
            let path = e.path();
            if path.extension().is_some_and(|x| x == "json") && !names.contains(&path) {
                drift.push(path.display().to_string());
                if update {
                    std::fs::remove_file(&path)
                        .map_err(|err| AppError::io(format!("remove {}", path.display()), &err))?;
                }
            }
        }
    }
    Ok(drift)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pretty_sorts_keys_and_round_trips() {
        let v = json!({"b": [1, {"d": null, "c": "x"}], "a": {}, "e": []});
        let text = pretty(&v);
        assert!(text.find("\"a\"").unwrap() < text.find("\"b\"").unwrap());
        assert!(text.find("\"c\"").unwrap() < text.find("\"d\"").unwrap());
        assert!(text.ends_with("}\n"));
        let back: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(back, v);
    }

    #[test]
    fn fixtures_parse_back_into_their_types() {
        for (name, v) in fixtures() {
            let ok = if name.starts_with("hub-event-") {
                serde_json::from_value::<HubEvent>(v).is_ok()
            } else if name.starts_with("node-state-") {
                serde_json::from_value::<NodeState>(v).is_ok()
            } else if name.starts_with("params-") {
                WorkloadParams::from_value(v).is_ok()
            } else if name.starts_with("progress-") {
                serde_json::from_value::<Progress>(v).is_ok()
            } else if name.starts_with("report-") {
                serde_json::from_value::<RunReport>(v).is_ok_and(|r| {
                    r.compare_key == r.compute_compare_key() && r.schema_version == 3
                })
            } else if name.starts_with("suite-profile-") {
                serde_json::from_value::<SuiteProfile>(v).is_ok()
            } else {
                true
            };
            assert!(ok, "{name} does not parse back");
        }
    }

    #[test]
    fn every_workload_has_ok_and_failed_reports() {
        let names: Vec<String> = fixtures().into_iter().map(|f| f.0).collect();
        for e in workloads::mode_examples() {
            let n = format!("report-{}.json", e.mode);
            assert!(names.contains(&n), "{n}");
        }
        for w in Workload::ALL {
            for s in ["ok", "failed"] {
                let n = format!("report-{}-{s}.json", w.as_str());
                assert!(names.contains(&n), "{n}");
            }
        }
        for st in [
            "idle",
            "starting",
            "needs-login",
            "needs-approval",
            "running",
            "stopping",
            "stopped",
            "error",
        ] {
            assert!(names.contains(&format!("node-state-{st}.json")));
        }
    }

    #[test]
    fn live_keys_cover_every_mode_example() {
        let (_, v) = fixtures()
            .into_iter()
            .find(|(n, _)| n == "live-keys.json")
            .expect("live-keys.json");
        let map = v.as_object().expect("an object");
        let examples = workloads::mode_examples();
        assert_eq!(map.len(), examples.len());
        for e in &examples {
            let keys: Vec<&str> = map[e.mode]
                .as_array()
                .unwrap_or_else(|| panic!("{}: not a list", e.mode))
                .iter()
                .map(|k| k.as_str().expect("a string"))
                .collect();
            // a synced-store burst is over too fast for live values
            assert_eq!(
                keys.is_empty(),
                e.mode == "synced-store-burst",
                "{}",
                e.mode
            );
            let mut sorted = keys.clone();
            sorted.sort_unstable();
            sorted.dedup();
            assert_eq!(keys, sorted, "{}: sorted and unique", e.mode);
            assert_eq!(
                keys.contains(&workloads::common::KEY_LOADED_RTT),
                e.params.time_based(),
                "{}: the path sampler's key exactly when time-based",
                e.mode
            );
        }
    }

    /// RFC 027 §7: the generated schema and fixtures are committed and must
    /// match the Rust types. `UPDATE_SCHEMA=1` rewrites them.
    #[test]
    fn crlf_checkouts_are_not_drift() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(!sync(tmp.path(), true).unwrap().is_empty(), "first write");
        assert!(sync(tmp.path(), false).unwrap().is_empty());
        // What `core.autocrlf=true` makes of the files on Windows.
        let schema = tmp.path().join(SCHEMA_FILE);
        let lf = std::fs::read_to_string(&schema).unwrap();
        std::fs::write(&schema, lf.replace('\n', "\r\n")).unwrap();
        assert!(sync(tmp.path(), false).unwrap().is_empty());
        // A real change still is drift.
        std::fs::write(&schema, lf.replace("\"", "'")).unwrap();
        assert_eq!(sync(tmp.path(), false).unwrap().len(), 1);
    }

    #[test]
    fn schema_is_current() {
        let update = std::env::var("UPDATE_SCHEMA").is_ok_and(|v| v == "1");
        let drift = sync(&schema_dir(), update).unwrap();
        if update {
            if !drift.is_empty() {
                eprintln!("[schema] rewrote {} file(s)", drift.len());
            }
            return;
        }
        assert!(
            drift.is_empty(),
            "benchmark/schema is out of date ({} file(s): {}).\n\
             Regenerate with: UPDATE_SCHEMA=1 cargo test -p truffle-bench schema_is_current",
            drift.len(),
            drift.join(", ")
        );
    }
}
