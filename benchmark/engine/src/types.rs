//! Types shared with the app and across the control protocol (RFC 027 §6, §7.1–§7.4).
//!
//! Rust is the source of truth: every type derives `serde` (snake_case
//! fields) and [`schemars::JsonSchema`], and `benchmark/app/src/api/types.ts`
//! mirrors them by hand. Closed string sets from the RFC (`"needs-login"`,
//! `"ok"`, `"same-host"`, …) are Rust enums that serialise to exactly those
//! strings. The report itself ([`RunReport`](crate::report::RunReport)) lives
//! in [`crate::report`].

use std::path::Path;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use truffle_core::network::{HealthInfo, NodeIdentity};
use truffle_core::Peer;

use crate::error::{AppError, ErrorCode};
use crate::params::Workload;

/// The pseudo-login Tailscale reports for tagged devices.
pub const TAGGED_LOGIN: &str = "tagged-devices";

/// Unix time now, in milliseconds.
pub fn unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Closed string sets
// ---------------------------------------------------------------------------

/// Lifecycle state of a hub (`NodeState.state`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum NodeStatus {
    /// Created, never started.
    Idle,
    /// `Hub::start` is building the node.
    Starting,
    /// The node waits for an interactive login (`auth_url` is set).
    NeedsLogin,
    /// The node is up; `local` is set.
    Running,
    /// `Hub::stop` is tearing down.
    Stopping,
    /// Stopped after running.
    Stopped,
    /// Start failed; `error` is set.
    Error,
}

/// Phase of a run (`Progress.phase`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum Phase {
    /// Resolving the peer, hello, prepare.
    Preparing,
    /// Settling the path (§6 flow step 4).
    Settling,
    /// Estimating the clock offset.
    Clock,
    /// Data is flowing but not yet measured.
    Warmup,
    /// The measurement window.
    Running,
    /// Waiting for in-flight data after the senders stop.
    Draining,
    /// `finish`, after-snapshots, report assembly.
    Finishing,
    /// Terminal: the report has `status: ok`.
    Done,
    /// Terminal: the report has `status: failed`.
    Failed,
    /// Terminal: the report has `status: cancelled`.
    Cancelled,
}

impl Phase {
    /// `done`, `failed` or `cancelled`.
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Done | Self::Failed | Self::Cancelled)
    }
}

/// Outcome of a run (`RunReport.status`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum RunStatus {
    /// Completed and measured.
    Ok,
    /// Failed; `error`/`error_code` say why.
    Failed,
    /// Cancelled by the user or a peer.
    Cancelled,
}

/// Which tool produced a report (`RunReport.tool`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum ReportTool {
    /// The desktop app.
    TruffleBenchApp,
    /// The `truffle-bench` CLI.
    TruffleBenchCli,
    /// Normalised from a v1/v2/tsnet-bench report (§7.5).
    Imported,
}

/// Where the two ends of a run lived (§7.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum Topology {
    /// Both hubs in one process (`pair`).
    SameProcess,
    /// Matching, non-weak host fingerprints.
    SameHost,
    /// Anything else.
    SeparateHosts,
}

/// Which path carried the bytes (§7.4 `path_label`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum PathLabel {
    /// Direct UDP the whole run.
    Direct,
    /// Relayed (DERP or peer relay) the whole run.
    Relay,
    /// Both, or a transition during the run.
    Mixed,
    /// No usermetrics and no usable path samples.
    Unknown,
}

/// Rust build profile of one side.
///
/// `unknown` only appears in imported v1 reports, which recorded nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum BuildProfile {
    /// `debug_assertions` on.
    Debug,
    /// An optimised build.
    Release,
    /// Not recorded (imported v1 reports).
    Unknown,
}

impl BuildProfile {
    /// The profile this crate was compiled with.
    pub fn current() -> Self {
        if cfg!(debug_assertions) {
            Self::Debug
        } else {
            Self::Release
        }
    }

    /// The wire spelling.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Debug => "debug",
            Self::Release => "release",
            Self::Unknown => "unknown",
        }
    }
}

/// Theme preference (`Settings.theme`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum Theme {
    /// Follow the OS.
    #[default]
    System,
    /// Always light.
    Light,
    /// Always dark.
    Dark,
}

/// Which direction of a summary value is better.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum Better {
    /// Bigger is better (throughput, rates).
    Higher,
    /// Smaller is better (latency, loss, CPU).
    Lower,
}

/// State of a run another peer drives against this hub (`HubEvent::RemoteRun`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum RemoteRunState {
    /// `prepare` accepted; this device is being measured.
    Prepared,
    /// `finish` answered.
    Finished,
    /// The initiator cancelled.
    Cancelled,
    /// The watchdog tore it down.
    Expired,
}

// ---------------------------------------------------------------------------
// §7.1 Node, peers, events
// ---------------------------------------------------------------------------

/// Health of the local node (from `Node::health()`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct HealthSummary {
    /// Tailscale backend state, e.g. `"Running"`.
    pub state: String,
    /// Whether the network is fully operational.
    pub healthy: bool,
    /// Active health warnings.
    pub warnings: Vec<String>,
    /// Node key expiry (RFC 3339), if any.
    pub key_expiry: Option<String>,
}

impl From<HealthInfo> for HealthSummary {
    fn from(h: HealthInfo) -> Self {
        Self {
            state: h.state,
            healthy: h.healthy,
            warnings: h.warnings,
            key_expiry: h.key_expiry,
        }
    }
}

/// What this hub knows about itself once running.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct LocalInfo {
    /// Human device name.
    pub device_name: String,
    /// Durable ULID.
    pub device_id: String,
    /// Tailscale stable node id.
    pub tailscale_id: String,
    /// `truffle-truffle-bench-<slug>`.
    pub tailscale_hostname: String,
    /// Tailnet IPv4, if known.
    pub tailscale_ip: Option<String>,
    /// MagicDNS name, if known.
    pub dns_name: Option<String>,
    /// Tailnet name, derived from `dns_name` (everything after the first label).
    pub tailnet: Option<String>,
    /// Signed-in login, or `tagged-devices`.
    pub login_name: Option<String>,
    /// The truffle app id (`truffle-bench`, or a per-run id in `pair`).
    pub app_id: String,
    /// Node health.
    pub health: HealthSummary,
    /// The sidecar binary in use.
    pub sidecar_path: String,
    /// Hub start → Running, in ms.
    pub hub_start_ms: Option<u64>,
}

impl LocalInfo {
    /// Assemble from the node's identity and health.
    pub fn from_identity(
        identity: &NodeIdentity,
        health: HealthSummary,
        sidecar_path: &Path,
        hub_start_ms: Option<u64>,
    ) -> Self {
        Self {
            device_name: identity.device_name.clone(),
            device_id: identity.device_id.clone(),
            tailscale_id: identity.tailscale_id.clone(),
            tailscale_hostname: identity.tailscale_hostname.clone(),
            tailscale_ip: identity.ip.map(|ip| ip.to_string()),
            dns_name: identity.dns_name.clone(),
            tailnet: identity.dns_name.as_deref().and_then(tailnet_from_dns_name),
            login_name: identity.login_name.clone(),
            app_id: identity.app_id.clone(),
            health,
            sidecar_path: sidecar_path.display().to_string(),
            hub_start_ms,
        }
    }
}

/// The tailnet part of a MagicDNS name: `host.tail1234.ts.net.` → `tail1234.ts.net`.
pub fn tailnet_from_dns_name(dns_name: &str) -> Option<String> {
    let trimmed = dns_name.trim().trim_end_matches('.');
    let (_, rest) = trimmed.split_once('.')?;
    (!rest.is_empty()).then(|| rest.to_string())
}

/// Whether a login is Tailscale's tagged-device pseudo-login.
pub fn is_tagged_login(login: Option<&str>) -> bool {
    login.is_some_and(|l| l.eq_ignore_ascii_case(TAGGED_LOGIN))
}

/// Same account: both logins known, equal ignoring case, and not the tagged
/// pseudo-login (§6 rule 1, second bullet).
pub fn same_login(local: Option<&str>, peer: Option<&str>) -> bool {
    match (local, peer) {
        (Some(a), Some(b)) => {
            !is_tagged_login(Some(a))
                && !is_tagged_login(Some(b))
                && a.to_lowercase() == b.to_lowercase()
        }
        _ => false,
    }
}

/// The responder's admission rule (§6 rule 1).
///
/// A peer is admitted iff `allow_other_logins`, or both logins are the same
/// real account, or both are tagged and `allow_tagged` is set. `None` on
/// either side is refused unless `allow_other_logins`.
pub fn is_admitted(
    local_login: Option<&str>,
    peer_login: Option<&str>,
    allow_other_logins: bool,
    allow_tagged: bool,
) -> bool {
    if allow_other_logins {
        return true;
    }
    if same_login(local_login, peer_login) {
        return true;
    }
    allow_tagged && is_tagged_login(local_login) && is_tagged_login(peer_login)
}

/// A same-app peer as the app shows it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct BenchPeer {
    /// Tailscale stable node id: the only peer key used for runs.
    pub id: String,
    /// Durable ULID once hello completed.
    pub device_id: Option<String>,
    /// Device name (falls back to the display name).
    pub device_name: String,
    /// Tailscale hostname.
    pub hostname: String,
    /// Tailnet IP.
    pub ip: String,
    /// OS, if known.
    pub os: Option<String>,
    /// Layer 3 online.
    pub online: bool,
    /// Session WebSocket connected.
    pub ws_connected: bool,
    /// Tailscale's connection label (`direct`, `relay:<region>`, `peer-relay:…`, `unknown`).
    pub connection: String,
    /// Owner login, or `tagged-devices`.
    pub login_name: Option<String>,
    /// Same real account as this hub ([`same_login`]).
    pub same_login: bool,
    /// Same machine as this hub, from the host fingerprint in the peer's
    /// hello (§7.4; [`same_host_verdict`](crate::env::same_host_verdict)):
    /// `None` when unknown (no hello yet, a peer that does not admit us and
    /// so withholds its fingerprint, or weak fingerprints that cannot tell).
    pub same_host: Option<bool>,
    /// The peer is a tagged device.
    pub tagged: bool,
    /// Last seen (RFC 3339).
    pub last_seen: Option<String>,
    /// Cached `hello.ack`, once received.
    pub hello: Option<PeerHello>,
    /// Why the last hello failed, if it did.
    pub hello_error: Option<AppError>,
    /// Last measured path RTT.
    pub rtt_ms: Option<f64>,
}

impl BenchPeer {
    /// Build from a core [`Peer`]; `same_host`, `hello`, `hello_error` and
    /// `rtt_ms` start empty.
    pub fn from_peer(peer: &Peer, local_login: Option<&str>) -> Self {
        Self {
            id: peer.tailscale_id.clone(),
            device_id: peer.device_id.clone(),
            device_name: peer
                .device_name
                .clone()
                .unwrap_or_else(|| peer.display_name.clone()),
            hostname: peer.hostname.clone(),
            ip: peer.ip.to_string(),
            os: peer.os.clone(),
            online: peer.online,
            ws_connected: peer.ws_connected,
            connection: peer.connection_type.clone(),
            login_name: peer.login_name.clone(),
            same_login: same_login(local_login, peer.login_name.as_deref()),
            same_host: None,
            tagged: is_tagged_login(peer.login_name.as_deref()),
            last_seen: peer.last_seen.clone(),
            hello: None,
            hello_error: None,
            rtt_ms: None,
        }
    }
}

/// `peer_probe`: a peer, its fresh hello and a path snapshot.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct PeerProbe {
    /// The peer.
    pub peer: BenchPeer,
    /// Its hello.
    pub hello: PeerHello,
    /// One ping.
    pub path: PathSnapshot,
}

/// One ping's view of the path.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct PathSnapshot {
    /// When (RFC 3339).
    pub at: String,
    /// `direct`, `relay:<region>`, `peer-relay:…`; `None` when the ping failed.
    pub connection: Option<String>,
    /// Ping RTT.
    pub rtt_ms: Option<f64>,
    /// Direct endpoint, if any.
    pub endpoint: Option<String>,
    /// Why the ping failed.
    pub error: Option<String>,
}

/// Events a hub broadcasts (`bench://event` in the app).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum HubEvent {
    /// The node's lifecycle state changed (boxed: it is by far the largest
    /// variant; serialises flat, `{"type": "node", ...NodeState}`).
    Node(Box<NodeState>),
    /// The peer list changed (debounced to 250 ms).
    Peers {
        /// Every same-app peer.
        peers: Vec<BenchPeer>,
    },
    /// A peer's run against this hub changed state.
    RemoteRun {
        /// The initiator's run id.
        run_id: String,
        /// The initiator's device name.
        peer: String,
        /// The initiator's tailscale id.
        peer_id: String,
        /// Workload name.
        workload: String,
        /// New state.
        state: RemoteRunState,
    },
}

/// The hub's lifecycle state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct NodeState {
    /// Lifecycle state.
    pub state: NodeStatus,
    /// Login URL while `needs-login`.
    pub auth_url: Option<String>,
    /// Why start failed, in `error`.
    pub error: Option<AppError>,
    /// Set while `running`.
    pub local: Option<LocalInfo>,
    /// The Tailscale hostname this hub registers as (known before login).
    pub tailscale_hostname: Option<String>,
    /// When the hub entered this state, Unix ms.
    pub since_ms: u64,
}

impl NodeState {
    /// A state entered now, with every optional field empty.
    pub fn new(state: NodeStatus) -> Self {
        Self {
            state,
            auth_url: None,
            error: None,
            local: None,
            tailscale_hostname: None,
            since_ms: unix_ms(),
        }
    }
}

/// A run in flight on this hub (initiator side).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ActiveRun {
    /// Run id.
    pub run_id: String,
    /// Suite id when part of a suite.
    pub suite_id: Option<String>,
    /// The suite's profile (`smoke`, `standard`, `full`, `nightly`) when
    /// part of a suite, so a reloaded UI can follow it.
    pub suite_profile: Option<String>,
    /// This item's 0-based index in its suite.
    pub suite_index: Option<u32>,
    /// Items in its suite.
    pub suite_total: Option<u32>,
    /// Workload name.
    pub workload: String,
    /// Target peer's device name.
    pub peer_name: String,
    /// RFC 3339.
    pub started_at: String,
    /// The latest progress event.
    pub last: Option<Progress>,
}

// ---------------------------------------------------------------------------
// §7.2 Progress
// ---------------------------------------------------------------------------

/// Live progress of one run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Progress {
    /// Run id.
    pub run_id: String,
    /// Suite id when part of a suite.
    pub suite_id: Option<String>,
    /// Ms since the run started.
    pub t_ms: u64,
    /// Phase.
    pub phase: Phase,
    /// 0–1 when known.
    pub fraction: Option<f32>,
    /// Live values. Receiver-side keys come only from `tick`s; sender-side
    /// keys are distinct (`sent_mbps`).
    pub samples: Vec<Sample>,
    /// One line for people.
    pub message: Option<String>,
}

impl Progress {
    /// A progress event with no fraction, samples or message.
    pub fn new(
        run_id: impl Into<String>,
        suite_id: Option<String>,
        t_ms: u64,
        phase: Phase,
    ) -> Self {
        Self {
            run_id: run_id.into(),
            suite_id,
            t_ms,
            phase,
            fraction: None,
            samples: Vec::new(),
            message: None,
        }
    }
}

/// One live value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Sample {
    /// Key, e.g. `goodput_mbps`.
    pub key: String,
    /// Value.
    pub value: f64,
}

impl Sample {
    /// Build a sample.
    pub fn new(key: impl Into<String>, value: f64) -> Self {
        Self {
            key: key.into(),
            value,
        }
    }
}

/// Live progress of a suite: which item, plus that item's run progress.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SuiteProgress {
    /// Suite id.
    pub suite_id: String,
    /// 0-based item index.
    pub index: u32,
    /// Item count.
    pub total: u32,
    /// The item's params label.
    pub item_label: String,
    /// The item's run progress.
    pub run: Progress,
}

/// Outcome of a suite.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SuiteResult {
    /// Suite id.
    pub suite_id: String,
    /// One run per item that started, in order.
    pub run_ids: Vec<String>,
    /// Items whose run did not end `ok`.
    pub failed: u32,
    /// The suite was cancelled.
    pub cancelled: bool,
}

// ---------------------------------------------------------------------------
// §6 hello
// ---------------------------------------------------------------------------

/// What a hub says about itself in `hello` / `hello.ack`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct PeerHello {
    /// Control protocol version ([`PROTOCOL`](crate::PROTOCOL)).
    pub protocol: u32,
    /// `truffle-bench-app` or `truffle-bench-cli`.
    pub app: String,
    /// Engine version.
    pub tool_version: String,
    /// truffle-core version.
    pub truffle_version: String,
    /// `debug` or `release`.
    pub build_profile: BuildProfile,
    /// Git commit, if known.
    pub git_sha: Option<String>,
    /// SHA-256 of the sidecar binary.
    pub sidecar_sha256: Option<String>,
    /// Device name.
    pub device_name: String,
    /// Durable ULID.
    pub device_id: Option<String>,
    /// `std::env::consts::OS`.
    pub os: String,
    /// `std::env::consts::ARCH`.
    pub arch: String,
    /// Logical CPUs.
    pub logical_cpus: Option<u32>,
    /// Signed-in login.
    pub login_name: Option<String>,
    /// §7.4 host fingerprint; omitted for peers that are not admitted.
    pub host_fingerprint: Option<String>,
    /// The fingerprint fell back to the hostname.
    pub host_fingerprint_weak: bool,
    /// Workloads this hub can respond to.
    pub workloads: Vec<String>,
    /// Whether the asking peer passes this hub's admission rule.
    pub accepts_runs_from_you: bool,
    /// Set while this hub is in a data-plane run, e.g. "measuring studio · tcp".
    pub busy: Option<String>,
}

/// Cut `s` to at most `max` characters (on a char boundary).
pub fn truncate_chars(s: &mut String, max: usize) {
    if let Some((i, _)) = s.char_indices().nth(max) {
        s.truncate(i);
    }
}

impl PeerHello {
    /// The longest text field kept from a peer's hello, in characters.
    pub const MAX_TEXT: usize = 256;
    /// The longest login kept (an email address is at most 320).
    pub const MAX_LOGIN: usize = 320;
    /// The most workload names kept.
    pub const MAX_WORKLOADS: usize = 32;
    /// The longest workload name kept.
    pub const MAX_WORKLOAD_NAME: usize = 32;

    /// A peer's hello cut to sane sizes. Hellos come from any same-app peer,
    /// are cached, re-sent in every `peers` event and copied into reports,
    /// so nothing a peer says may grow without bound.
    #[must_use]
    pub fn bounded(mut self) -> Self {
        for s in [
            &mut self.app,
            &mut self.tool_version,
            &mut self.truffle_version,
            &mut self.device_name,
            &mut self.os,
            &mut self.arch,
        ] {
            truncate_chars(s, Self::MAX_TEXT);
        }
        for s in [
            self.git_sha.as_mut(),
            self.sidecar_sha256.as_mut(),
            self.device_id.as_mut(),
            self.host_fingerprint.as_mut(),
            self.busy.as_mut(),
        ]
        .into_iter()
        .flatten()
        {
            truncate_chars(s, Self::MAX_TEXT);
        }
        if let Some(l) = self.login_name.as_mut() {
            truncate_chars(l, Self::MAX_LOGIN);
        }
        self.workloads.truncate(Self::MAX_WORKLOADS);
        self.workloads
            .retain(|w| w.chars().count() <= Self::MAX_WORKLOAD_NAME);
        self
    }
}

// ---------------------------------------------------------------------------
// §7.4 report building blocks
// ---------------------------------------------------------------------------

/// The environment of one side of a run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct NodeEnv {
    /// Device name.
    pub device_name: String,
    /// OS.
    pub os: String,
    /// CPU architecture.
    pub arch: String,
    /// Logical CPUs.
    pub logical_cpus: Option<u32>,
    /// `truffle-bench-app`, `truffle-bench-cli`, or for imports the source
    /// tool (`truffle-bench v1`, `truffle-bench v2`, `tsnet-bench`).
    pub tool: String,
    /// Engine version.
    pub tool_version: String,
    /// truffle-core version.
    pub truffle_version: String,
    /// Build profile.
    pub build_profile: BuildProfile,
    /// `rustc -V` at build time.
    pub rustc: Option<String>,
    /// Git commit.
    pub git_sha: Option<String>,
    /// Uncommitted tracked changes.
    pub git_dirty: Option<bool>,
    /// Sidecar binary path.
    pub sidecar_path: Option<String>,
    /// SHA-256 of the sidecar binary.
    pub sidecar_sha256: Option<String>,
    /// Tailnet IP.
    pub tailscale_ip: Option<String>,
    /// §7.4 host fingerprint.
    pub host_fingerprint: Option<String>,
    /// The fingerprint fell back to the hostname.
    pub host_fingerprint_weak: bool,
    /// macOS only: running on battery.
    pub on_battery: Option<bool>,
    /// App only: share of the run with the window in front, 0–100.
    pub app_foreground_pct: Option<f64>,
}

impl NodeEnv {
    /// The peer's environment as far as its hello tells (no rustc, git
    /// dirtiness, sidecar path or power state).
    pub fn from_hello(hello: &PeerHello, tailscale_ip: Option<String>) -> Self {
        Self {
            device_name: hello.device_name.clone(),
            os: hello.os.clone(),
            arch: hello.arch.clone(),
            logical_cpus: hello.logical_cpus,
            tool: hello.app.clone(),
            tool_version: hello.tool_version.clone(),
            truffle_version: hello.truffle_version.clone(),
            build_profile: hello.build_profile,
            rustc: None,
            git_sha: hello.git_sha.clone(),
            git_dirty: None,
            sidecar_path: None,
            sidecar_sha256: hello.sidecar_sha256.clone(),
            tailscale_ip,
            host_fingerprint: hello.host_fingerprint.clone(),
            host_fingerprint_weak: hello.host_fingerprint_weak,
            on_battery: None,
            app_foreground_pct: None,
        }
    }
}

/// NTP-style offset estimate (§6 Clock).
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ClockOffset {
    /// θ: peer − local, ns.
    pub offset_ns: i64,
    /// δ: round-trip delay of the chosen sample, ns.
    pub delay_ns: i64,
    /// ⌈δ/2⌉, ns.
    pub uncertainty_ns: u64,
    /// t4 of the chosen sample (local `now_ns()`).
    pub at_ns: u64,
    /// Samples that answered.
    pub samples: u32,
}

/// Clock offsets taken before and after a run (`network.clock`).
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ClockInfo {
    /// Estimate before the data phase.
    pub before: ClockOffset,
    /// Estimate after it.
    pub after: Option<ClockOffset>,
    /// Drift between the two, ppm.
    pub drift_ppm: Option<f64>,
    /// The larger uncertainty of the two, ns.
    pub uncertainty_ns: u64,
}

/// Summary statistics of a sample set (§7.4).
///
/// `samples` counts every sample including lost ones. Percentiles are
/// nearest-rank over all samples with lost ones ranked as +∞; a percentile is
/// `None` below its minimum sample count or when it lands on a lost sample.
/// `min`, `mean`, `max` and `stddev` cover the received samples only.
#[derive(Debug, Clone, Copy, PartialEq, Default, Serialize, Deserialize, JsonSchema)]
pub struct Distribution {
    /// n, lost included.
    pub samples: u32,
    /// Lost or timed-out samples.
    pub lost: u32,
    /// Smallest received value.
    pub min: Option<f64>,
    /// Mean of received values.
    pub mean: Option<f64>,
    /// Median (n ≥ 2).
    pub p50: Option<f64>,
    /// n ≥ 10.
    pub p90: Option<f64>,
    /// n ≥ 20.
    pub p95: Option<f64>,
    /// n ≥ 100.
    pub p99: Option<f64>,
    /// n ≥ 1000.
    pub p999: Option<f64>,
    /// Largest received value.
    pub max: Option<f64>,
    /// Sample standard deviation (n − 1) of received values; n ≥ 2.
    pub stddev: Option<f64>,
}

/// One headline number.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SummaryItem {
    /// Stable key, e.g. `goodput_mbps`.
    pub key: String,
    /// Label for people.
    pub label: String,
    /// Value; `None` when it could not be measured.
    pub value: Option<f64>,
    /// Unit, e.g. `Mb/s`.
    pub unit: String,
    /// Which direction is better.
    pub better: Better,
    /// ± uncertainty in the same unit.
    pub uncertainty: Option<f64>,
}

impl SummaryItem {
    /// Build an item with no uncertainty.
    pub fn new(
        key: impl Into<String>,
        label: impl Into<String>,
        value: Option<f64>,
        unit: impl Into<String>,
        better: Better,
    ) -> Self {
        Self {
            key: key.into(),
            label: label.into(),
            value,
            unit: unit.into(),
            better,
            uncertainty: None,
        }
    }

    /// Attach a ± uncertainty.
    #[must_use]
    pub fn with_uncertainty(mut self, uncertainty: Option<f64>) -> Self {
        self.uncertainty = uncertainty;
        self
    }
}

/// A time series for a chart.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Series {
    /// Stable key.
    pub key: String,
    /// Label for people.
    pub label: String,
    /// Unit.
    pub unit: String,
    /// `[t_ms, value]` pairs, t in the initiator's run timebase.
    pub points: Vec<(f64, f64)>,
    /// Points with `t_ms ≤ warmup_ms` are warmup.
    pub warmup_ms: Option<f64>,
}

/// One path-sampler point.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct PathTimelinePoint {
    /// Ms since the sampler's origin.
    pub t_ms: u64,
    /// Connection label; `None` when the ping failed or timed out.
    pub connection: Option<String>,
    /// RTT; `None` when lost.
    pub rtt_ms: Option<f64>,
}

/// Bytes per path class, from usermetrics deltas.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
pub struct PathBytes {
    /// `direct_ipv4` + `direct_ipv6`.
    pub direct: u64,
    /// `derp`.
    pub derp: u64,
    /// `peer_relay_ipv4` + `peer_relay_ipv6`.
    pub peer_relay: u64,
}

impl PathBytes {
    /// All bytes.
    pub fn total(&self) -> u64 {
        self.direct
            .saturating_add(self.derp)
            .saturating_add(self.peer_relay)
    }

    /// Component-wise sum (both sides of a run).
    #[must_use]
    pub fn plus(&self, other: &PathBytes) -> PathBytes {
        PathBytes {
            direct: self.direct.saturating_add(other.direct),
            derp: self.derp.saturating_add(other.derp),
            peer_relay: self.peer_relay.saturating_add(other.peer_relay),
        }
    }
}

/// CPU and memory of one side over the measurement window (§7.4).
///
/// CPU percentages use 100 = one logical CPU, so they can exceed 100.
#[derive(Debug, Clone, Copy, PartialEq, Default, Serialize, Deserialize, JsonSchema)]
pub struct ResourceStats {
    /// Process CPU time (user + sys) in the window, ms.
    pub cpu_ms: f64,
    /// `cpu_ms / window_ms × 100`.
    pub cpu_pct_avg: f64,
    /// Max over 500 ms intervals (priming interval discarded).
    pub cpu_pct_max: f64,
    /// Max RSS, MB (10⁶ bytes).
    pub rss_mb_max: f64,
    /// `cpu_ms / (goodput bytes / 10⁶)`.
    pub cpu_ms_per_mb: Option<f64>,
    /// Sidecar CPU time, ms.
    pub sidecar_cpu_ms: Option<f64>,
    /// Sidecar average CPU %.
    pub sidecar_cpu_pct_avg: Option<f64>,
    /// Sidecar max CPU % over 500 ms intervals.
    pub sidecar_cpu_pct_max: Option<f64>,
    /// Sidecar max RSS, MB.
    pub sidecar_rss_mb_max: Option<f64>,
    /// Sidecar `cpu_ms` per MB of goodput.
    pub sidecar_cpu_ms_per_mb: Option<f64>,
}

// ---------------------------------------------------------------------------
// §11.2 app command types
// ---------------------------------------------------------------------------

/// `app_info`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct AppInfo {
    /// App (engine) version.
    pub app_version: String,
    /// truffle-core version.
    pub truffle_version: String,
    /// Data directory.
    pub data_dir: String,
    /// Profile name, if any.
    pub profile: Option<String>,
    /// OS.
    pub os: String,
    /// Build profile.
    pub build_profile: BuildProfile,
    /// Resolved sidecar path.
    pub sidecar_path: Option<String>,
    /// Its SHA-256.
    pub sidecar_sha256: Option<String>,
}

/// Persisted app settings.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize, JsonSchema)]
pub struct Settings {
    /// Device name.
    pub device_name: String,
    /// Accept runs from other accounts.
    pub allow_other_logins: bool,
    /// Theme.
    pub theme: Theme,
    /// Remembered target: device_id, else tailscale_id.
    pub target_peer: Option<String>,
}

/// One History row.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct RunSummary {
    /// Run id.
    pub run_id: String,
    /// Suite id.
    pub suite_id: Option<String>,
    /// Workload.
    pub workload: Workload,
    /// Params label.
    pub label: String,
    /// Outcome.
    pub status: RunStatus,
    /// Error code when failed.
    pub error_code: Option<ErrorCode>,
    /// RFC 3339.
    pub started_at: String,
    /// Peer device name.
    pub peer_name: Option<String>,
    /// Path label.
    pub path_label: PathLabel,
    /// Topology.
    pub topology: Topology,
    /// `debug` if either side is debug, `unknown` if not recorded, else `release`.
    pub build_profile: BuildProfile,
    /// Comparability key.
    pub compare_key: String,
    /// Headline numbers.
    pub summary: Vec<SummaryItem>,
    /// Other runs sharing the machine.
    pub overlapping_runs: u32,
    /// Producing tool.
    pub tool: ReportTool,
    /// History flags derived from the report, in this order when present:
    /// `debug` (either side a debug build), `overlap` (shared the machine
    /// with other runs), `rate-not-achieved` (the sender could not sustain
    /// its target), `imported`, `same-host`, `same-process`, `mixed-path`,
    /// `partial` (a failed or cancelled run that kept partial results). See
    /// [`RunReport::flags`](crate::report::RunReport::flags).
    pub flags: Vec<String>,
}

/// A suite profile (§8.4): what `suite_profiles` returns.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SuiteProfile {
    /// `smoke`, `standard`, `full` or `nightly`.
    pub id: String,
    /// Name people read.
    pub label: String,
    /// One sentence.
    pub description: String,
    /// Items, run in order.
    pub items: Vec<crate::params::WorkloadParams>,
}

/// `runs_import` result.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
pub struct ImportResult {
    /// Reports added to the store.
    pub imported: u32,
    /// Files that were not reports, could not be normalised, or were already stored.
    pub skipped: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tailnet_is_everything_after_the_first_label() {
        assert_eq!(
            tailnet_from_dns_name("truffle-truffle-bench-mac.tail1234.ts.net."),
            Some("tail1234.ts.net".into())
        );
        assert_eq!(tailnet_from_dns_name("host"), None);
        assert_eq!(tailnet_from_dns_name("host."), None);
    }

    #[test]
    fn admission_rule() {
        let me = Some("Alice@example.com");
        assert!(is_admitted(me, Some("alice@example.com"), false, false));
        assert!(!is_admitted(me, Some("bob@example.com"), false, false));
        assert!(is_admitted(me, Some("bob@example.com"), true, false));
        // None on either side is refused.
        assert!(!is_admitted(None, None, false, true));
        assert!(!is_admitted(me, None, false, true));
        // Tagged devices are never "the same login"; they need allow_tagged.
        let tagged = Some(TAGGED_LOGIN);
        assert!(!same_login(tagged, tagged));
        assert!(!is_admitted(tagged, tagged, false, false));
        assert!(is_admitted(tagged, tagged, false, true));
        assert!(!is_admitted(me, tagged, false, true));
    }

    #[test]
    fn peer_hellos_are_bounded() {
        let mut s = "héllo".to_string();
        truncate_chars(&mut s, 2);
        assert_eq!(s, "hé");
        truncate_chars(&mut s, 10);
        assert_eq!(s, "hé");
        let big = "x".repeat(10_000);
        let h = PeerHello {
            protocol: 1,
            app: big.clone(),
            tool_version: "0.1.0".into(),
            truffle_version: "0.7.17".into(),
            build_profile: BuildProfile::Release,
            git_sha: Some(big.clone()),
            sidecar_sha256: None,
            device_name: big.clone(),
            device_id: None,
            os: "macos".into(),
            arch: "aarch64".into(),
            logical_cpus: Some(8),
            login_name: Some(big.clone()),
            host_fingerprint: Some("abcd".into()),
            host_fingerprint_weak: false,
            workloads: (0..1000)
                .map(|i| format!("w{i}"))
                .chain([big.clone()])
                .collect(),
            accepts_runs_from_you: true,
            busy: Some(big),
        }
        .bounded();
        assert_eq!(h.app.len(), PeerHello::MAX_TEXT);
        assert_eq!(h.device_name.len(), PeerHello::MAX_TEXT);
        assert_eq!(h.git_sha.unwrap().len(), PeerHello::MAX_TEXT);
        assert_eq!(h.busy.unwrap().len(), PeerHello::MAX_TEXT);
        assert_eq!(h.login_name.unwrap().len(), PeerHello::MAX_LOGIN);
        assert_eq!(h.workloads.len(), PeerHello::MAX_WORKLOADS);
        assert_eq!(h.host_fingerprint.as_deref(), Some("abcd"));
        assert_eq!(h.os, "macos");
    }

    #[test]
    fn hub_event_is_tagged_kebab_case() {
        let ev = HubEvent::Node(Box::new(NodeState::new(NodeStatus::NeedsLogin)));
        let v = serde_json::to_value(&ev).unwrap();
        assert_eq!(v["type"], "node");
        assert_eq!(v["state"], "needs-login");
        let ev = HubEvent::RemoteRun {
            run_id: "r1".into(),
            peer: "studio".into(),
            peer_id: "nX".into(),
            workload: "tcp".into(),
            state: RemoteRunState::Prepared,
        };
        let v = serde_json::to_value(&ev).unwrap();
        assert_eq!(v["type"], "remote-run");
        assert_eq!(v["state"], "prepared");
        let back: HubEvent = serde_json::from_value(v).unwrap();
        assert_eq!(back, ev);
        let v = serde_json::to_value(HubEvent::Peers { peers: vec![] }).unwrap();
        assert_eq!(v, serde_json::json!({"type": "peers", "peers": []}));
    }

    #[test]
    fn closed_sets_use_rfc_spellings() {
        let s = |v: serde_json::Value| v.as_str().unwrap().to_string();
        assert_eq!(s(serde_json::to_value(Phase::Warmup).unwrap()), "warmup");
        assert_eq!(s(serde_json::to_value(RunStatus::Ok).unwrap()), "ok");
        assert_eq!(
            s(serde_json::to_value(ReportTool::TruffleBenchCli).unwrap()),
            "truffle-bench-cli"
        );
        assert_eq!(
            s(serde_json::to_value(Topology::SameProcess).unwrap()),
            "same-process"
        );
        assert_eq!(s(serde_json::to_value(PathLabel::Mixed).unwrap()), "mixed");
        assert_eq!(
            s(serde_json::to_value(BuildProfile::Release).unwrap()),
            "release"
        );
        assert_eq!(s(serde_json::to_value(Better::Lower).unwrap()), "lower");
        assert!(Phase::Cancelled.is_terminal() && !Phase::Running.is_terminal());
    }

    #[test]
    fn series_points_are_pairs() {
        let s = Series {
            key: "goodput_mbps".into(),
            label: "Goodput".into(),
            unit: "Mb/s".into(),
            points: vec![(250.0, 912.5)],
            warmup_ms: Some(1000.0),
        };
        let v = serde_json::to_value(&s).unwrap();
        assert_eq!(v["points"], serde_json::json!([[250.0, 912.5]]));
    }
}
