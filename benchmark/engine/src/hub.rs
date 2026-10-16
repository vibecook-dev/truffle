//! The Hub (RFC 027 §5.4): one node, its responder, its peers, its runs.
//!
//! A [`Hub`] is created idle ([`Hub::new`]; [`Hub::subscribe`] works at
//! once), started with [`Hub::start`] (which emits `starting`,
//! `needs-login`, `running` or `error`), and stopped with [`Hub::stop`] in
//! any state. While running it:
//!
//! - holds an exclusive lock on `<state_dir>/hub.lock` (`state-in-use`);
//! - answers peers through the responder ([`crate::control`]);
//! - routes file offers to file-transfer runs ([`OfferRouter`], installed once);
//! - says `hello` to every online same-app peer when it appears or comes
//!   back, caches the reply, and publishes [`HubEvent::Peers`] (debounced to
//!   250 ms);
//! - re-emits `needs-login` when the node asks for re-authentication.
//!
//! [`Hub::run`] executes the §6 initiator flow and always returns a
//! [`RunReport`] (failures are `status: failed` with an `error_code`);
//! [`Hub::suite`] runs a §8.4 profile item by item. [`Pair`] is two hubs in
//! one process with a per-run app id, as `truffle-bench pair` and the live
//! PR test use.
//!
//! ```no_run
//! # async fn demo() -> Result<(), truffle_bench::AppError> {
//! use std::sync::Arc;
//! use truffle_bench::hub::{Hub, RunRequest};
//! use truffle_bench::{HubConfig, Tool, WorkloadParams};
//!
//! let cfg = HubConfig::defaults(Tool::Cli, None, truffle_bench::config::cli_data_dir());
//! let hub = Hub::new(cfg);
//! hub.start().await?;
//! let report = hub
//!     .run(
//!         RunRequest {
//!             run_id: "demo-1".into(),
//!             peer: "studio".into(),
//!             params: WorkloadParams::from_value(serde_json::json!({"workload": "path"}))?,
//!             suite_id: None,
//!         },
//!         Arc::new(|p| println!("{:?} {:?}", p.phase, p.samples)),
//!     )
//!     .await;
//! println!("{:?} {:?}", report.status, report.summary);
//! hub.stop().await;
//! # Ok(()) }
//! ```

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError, RwLock, Weak};
use std::time::Duration;

use futures::FutureExt;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{broadcast, oneshot, Notify};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use truffle_core::file_transfer::OverwritePolicy;
use truffle_core::network::tailscale::TailscaleProvider;
use truffle_core::session::PeerEvent;
use truffle_core::{FileTransferEvent, Node, Peer, TransferDirection};

use crate::clock::{self, rfc3339, ClockMap};
use crate::config::{resolve_sidecar, HubConfig, Tool};
use crate::control::{self, RemoteRuns, RunToken, SlotHolder, SlotRole, Slots};
use crate::env;
use crate::error::{AppError, ErrorCode};
use crate::localapi::{LocalApi, UserMetrics};
use crate::params::{Workload, WorkloadParams};
use crate::pathwatch;
use crate::report::{
    validate_run_id, NetworkInfo, Resources, RunReport, RunStore, METHODOLOGY, SCHEMA_VERSION,
};
use crate::resources::{ResourceSampler, ResourceTargets};
use crate::suites;
use crate::types::{
    ActiveRun, BenchPeer, HealthSummary, HubEvent, LocalInfo, NodeEnv, NodeState, NodeStatus,
    PathSnapshot, PeerHello, PeerProbe, Phase, Progress, RemoteRunState, RunStatus, SuiteProgress,
    SuiteResult, Topology,
};
use crate::workloads::common::{fill_per_mb, KEY_CPU_PER_MB, KEY_LOADED_RTT_P50};
use crate::workloads::{self, PeerTimings, RunCtx, RunCtxInit, RunProgress, WorkloadOutput};
use crate::PROTOCOL;

/// The node type every hub runs.
pub type BenchNode = Node<TailscaleProvider>;
/// Receives a run's [`Progress`] events (CLI printer, Tauri channel).
pub type ProgressSink = Arc<dyn Fn(Progress) + Send + Sync>;
/// Receives a suite's [`SuiteProgress`] events.
pub type SuiteProgressSink = Arc<dyn Fn(SuiteProgress) + Send + Sync>;
/// Sees every run's report just before it is saved and may amend it (the
/// app sets `local.app_foreground_pct`). See [`Hub::set_report_hook`].
pub type ReportHook = Arc<dyn Fn(&mut RunReport) + Send + Sync>;

/// How long `Hub::run` waits for Layer 3 to list the peer.
pub const PEER_WAIT_MS: u64 = 30_000;
/// Peers events are debounced to this.
pub const PEERS_DEBOUNCE: Duration = Duration::from_millis(250);
/// `stop` waits this long for in-flight runs to wind down.
const STOP_DRAIN: Duration = Duration::from_secs(15);
/// A failed hello is retried no sooner than this, unless the peer reconnects.
const HELLO_RETRY: Duration = Duration::from_secs(30);
/// `pair`: each hub waits this long for its sibling.
pub const PAIR_SIBLING_WAIT_MS: u64 = 60_000;
/// `pair`: per-run app ids are this plus 6 hex digits (see [`Pair`]).
pub const PAIR_APP_PREFIX: &str = "truffle-benchpair-";
/// `pair`: each direction's hello is tried this many times.
const PAIR_HELLO_ATTEMPTS: u32 = 3;
/// `pair`: pause between hello attempts.
const PAIR_HELLO_RETRY: Duration = Duration::from_secs(1);

/// One run to execute (`bench_run`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct RunRequest {
    /// Caller-chosen id, `^[A-Za-z0-9-]{1,64}$`.
    pub run_id: String,
    /// Peer reference: device name, ULID, tailscale id, hostname or IP.
    pub peer: String,
    /// What to run.
    pub params: WorkloadParams,
    /// Set when the run belongs to a suite.
    pub suite_id: Option<String>,
}

/// One suite to execute (`suite_run`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SuiteRequest {
    /// Caller-chosen id, `^[A-Za-z0-9-]{1,64}$`.
    pub suite_id: String,
    /// `smoke`, `standard`, `full` or `nightly`.
    pub profile: String,
    /// Peer reference.
    pub peer: String,
    /// `("<workload>.<field>", value)` overrides.
    #[serde(default)]
    pub overrides: Vec<(String, Value)>,
}

// ---------------------------------------------------------------------------
// File-offer router (§8.2 file-transfer)
// ---------------------------------------------------------------------------

/// How an expected file transfer ended on the receiving side.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OfferOutcome {
    /// Received and verified.
    Completed {
        /// Bytes received.
        bytes: u64,
        /// SHA-256 of the received file (hex).
        sha256: String,
    },
    /// Refused, failed or never arrived.
    Failed {
        /// Why.
        reason: String,
    },
}

struct Expected {
    id: u64,
    size: u64,
    dir: PathBuf,
    tx: oneshot::Sender<OfferOutcome>,
}

/// An accepted offer until its transfer ends.
struct InFlight {
    id: u64,
    tx: oneshot::Sender<OfferOutcome>,
    /// Bytes reserved for it.
    size: u64,
    /// Its run's `in-…` directory.
    dir: PathBuf,
}

#[derive(Default)]
struct RouterState {
    next: u64,
    expected: HashMap<(String, String), Expected>,
    tokens: HashMap<String, InFlight>,
    /// Bytes of accepted offers still being received.
    reserved: u64,
}

impl RouterState {
    /// End an in-flight transfer: its reservation is released.
    fn finish(&mut self, token: &str) -> Option<InFlight> {
        let t = self.tokens.remove(token)?;
        self.reserved = self.reserved.saturating_sub(t.size);
        Some(t)
    }

    /// Whether an offer of `size` into `dir` may be accepted now: one file
    /// at a time per run (the initiator waits for `ft.done` between them),
    /// and twice the size free beyond what accepted offers still need.
    fn admit(&self, size: u64, dir: &Path, free: Option<u64>) -> Result<(), String> {
        if self.tokens.values().any(|t| t.dir == dir) {
            return Err("another file of this run is still being received".into());
        }
        if let Some(free) = free {
            let left = free.saturating_sub(self.reserved);
            if left < size.saturating_mul(2) {
                return Err(format!(
                    "only {left} bytes free (after {} reserved); a {size}-byte file needs twice that",
                    self.reserved
                ));
            }
        }
        Ok(())
    }
}

/// The hub's single file-offer router.
///
/// Installed once at hub start (`offer_channel` is called exactly once per
/// node; workloads never call it or `auto_accept`). An offer is accepted iff
/// a run registered it with [`expect`](Self::expect): same sender
/// (tailscale id), same file name, same size, and at least twice the size
/// free on disk. It is accepted into the run's `in-<run_tag_hex>/`
/// directory; every other offer is rejected. The overwrite policy stays
/// `Reject`: names are unique per run tag and iteration, so a clash means
/// something is wrong.
pub struct OfferRouter {
    state: Mutex<RouterState>,
}

impl std::fmt::Debug for OfferRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let st = self.lock();
        f.debug_struct("OfferRouter")
            .field("expected", &st.expected.len())
            .field("in_flight", &st.tokens.len())
            .finish()
    }
}

/// A registered expectation; await [`outcome`](Self::outcome). Dropping it
/// withdraws the registration.
pub struct ExpectedOffer {
    router: Arc<OfferRouter>,
    id: u64,
    rx: oneshot::Receiver<OfferOutcome>,
}

impl std::fmt::Debug for ExpectedOffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExpectedOffer")
            .field("id", &self.id)
            .finish()
    }
}

impl ExpectedOffer {
    /// Wait for the transfer to complete or fail (offer wait included).
    pub async fn outcome(mut self, timeout: Duration) -> OfferOutcome {
        match tokio::time::timeout(timeout, &mut self.rx).await {
            Ok(Ok(o)) => o,
            Ok(Err(_)) => OfferOutcome::Failed {
                reason: "the offer router stopped".into(),
            },
            Err(_) => OfferOutcome::Failed {
                reason: format!("the file did not arrive within {} s", timeout.as_secs()),
            },
        }
    }
}

impl Drop for ExpectedOffer {
    fn drop(&mut self) {
        self.router.forget(self.id);
    }
}

impl OfferRouter {
    fn lock(&self) -> std::sync::MutexGuard<'_, RouterState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Expect `file_name` of `size` bytes from `from_tailscale_id`, saved
    /// into `dir` (which must exist). A second registration for the same
    /// sender and name replaces the first.
    pub fn expect(
        self: &Arc<Self>,
        from_tailscale_id: &str,
        file_name: &str,
        size: u64,
        dir: &Path,
    ) -> ExpectedOffer {
        let (tx, rx) = oneshot::channel();
        let mut st = self.lock();
        st.next += 1;
        let id = st.next;
        st.expected.insert(
            (from_tailscale_id.to_string(), file_name.to_string()),
            Expected {
                id,
                size,
                dir: dir.to_path_buf(),
                tx,
            },
        );
        ExpectedOffer {
            router: self.clone(),
            id,
            rx,
        }
    }

    fn forget(&self, id: u64) {
        let mut st = self.lock();
        st.expected.retain(|_, e| e.id != id);
        let gone: Vec<String> = st
            .tokens
            .iter()
            .filter(|(_, t)| t.id == id)
            .map(|(k, _)| k.clone())
            .collect();
        for token in gone {
            st.finish(&token);
        }
    }

    /// Call `offer_channel` once and run the router and its event watcher
    /// until `cancel`.
    async fn install(
        node: Arc<BenchNode>,
        cancel: CancellationToken,
        tracker: &TaskTracker,
    ) -> Arc<Self> {
        let router = Arc::new(Self {
            state: Mutex::new(RouterState::default()),
        });
        let ft = node.file_transfer();
        ft.set_overwrite_policy(OverwritePolicy::Reject);
        let mut events = ft.subscribe();
        let mut offers = ft.offer_channel(node.clone()).await;

        let r = router.clone();
        let c = cancel.clone();
        tracker.spawn(async move {
            loop {
                let next = tokio::select! {
                    _ = c.cancelled() => break,
                    o = offers.recv() => o,
                };
                let Some((offer, responder)) = next else {
                    break;
                };
                let key = (offer.from_peer.clone(), offer.file_name.clone());
                let expected = r.lock().expected.remove(&key);
                let Some(e) = expected else {
                    responder.reject("not expected by any benchmark run");
                    continue;
                };
                if offer.size != e.size {
                    let reason = format!("offered {} bytes, expected {}", offer.size, e.size);
                    responder.reject(&reason);
                    let _ = e.tx.send(OfferOutcome::Failed { reason });
                    continue;
                }
                let dir = e.dir.clone();
                let free = tokio::task::spawn_blocking(move || free_space(&dir))
                    .await
                    .ok()
                    .flatten();
                if free.is_none() {
                    tracing::debug!(dir = %e.dir.display(), "offer router: free space unknown");
                }
                // Checked and reserved under one lock, so concurrent offers
                // cannot all pass against the same free space.
                let admitted = {
                    let mut st = r.lock();
                    let ok = st.admit(offer.size, &e.dir, free);
                    if ok.is_ok() {
                        st.reserved = st.reserved.saturating_add(offer.size);
                    }
                    ok
                };
                if let Err(reason) = admitted {
                    responder.reject(&reason);
                    let _ = e.tx.send(OfferOutcome::Failed { reason });
                    continue;
                }
                let accept_dir = format!("{}/", e.dir.display());
                r.lock().tokens.insert(
                    offer.token.clone(),
                    InFlight {
                        id: e.id,
                        tx: e.tx,
                        size: offer.size,
                        dir: e.dir,
                    },
                );
                responder.accept(&accept_dir);
            }
        });

        let r = router.clone();
        tracker.spawn(async move {
            use tokio::sync::broadcast::error::RecvError;
            loop {
                let ev = tokio::select! {
                    _ = cancel.cancelled() => break,
                    e = events.recv() => e,
                };
                let (token, outcome) = match ev {
                    Ok(FileTransferEvent::Completed {
                        token,
                        direction: TransferDirection::Receive,
                        bytes_transferred,
                        sha256,
                        ..
                    }) => (
                        token,
                        OfferOutcome::Completed {
                            bytes: bytes_transferred,
                            sha256,
                        },
                    ),
                    Ok(FileTransferEvent::Failed {
                        token,
                        direction: TransferDirection::Receive,
                        reason,
                        ..
                    })
                    | Ok(FileTransferEvent::Rejected { token, reason, .. }) => {
                        (token, OfferOutcome::Failed { reason })
                    }
                    Ok(_) => continue,
                    Err(RecvError::Lagged(n)) => {
                        tracing::warn!(missed = n, "offer router: file-transfer events lagged");
                        continue;
                    }
                    Err(RecvError::Closed) => break,
                };
                let done = r.lock().finish(&token);
                if let Some(t) = done {
                    let _ = t.tx.send(outcome);
                }
            }
        });
        router
    }
}

/// Free bytes on the volume holding `dir`, if known.
fn free_space(dir: &Path) -> Option<u64> {
    let dir = std::fs::canonicalize(dir).ok()?;
    // Windows canonicalizes to a verbatim path (`\\?\C:\…`); mount points
    // are plain (`C:\`).
    #[cfg(windows)]
    let dir = match dir.to_str().and_then(|s| s.strip_prefix(r"\\?\")) {
        Some(plain) => PathBuf::from(plain),
        None => dir,
    };
    let disks = sysinfo::Disks::new_with_refreshed_list();
    disks
        .list()
        .iter()
        .filter(|d| dir.starts_with(d.mount_point()))
        .max_by_key(|d| d.mount_point().as_os_str().len())
        .map(|d| d.available_space())
}

// ---------------------------------------------------------------------------
// Shared state
// ---------------------------------------------------------------------------

/// What the hub knows about each peer beyond Layer 3.
#[derive(Default)]
struct PeerBook {
    hello: HashMap<String, PeerHello>,
    hello_error: HashMap<String, AppError>,
    in_flight: HashSet<String>,
    last_attempt: HashMap<String, Instant>,
    online: HashMap<String, bool>,
    discovery_ms: HashMap<String, u64>,
    ws_connect_ms: HashMap<String, u64>,
    rtt_ms: HashMap<String, f64>,
}

/// State that exists only while the node runs.
pub(crate) struct Live {
    pub(crate) node: Arc<BenchNode>,
    /// This hub's hello template (`accepts_runs_from_you`/`busy` set per
    /// reply); its login is filled in late by [`resolve_login`] when needed.
    hello: RwLock<PeerHello>,
    /// The login read from the LocalAPI when the sidecar's start event did
    /// not carry one (see [`resolve_login`]).
    login_fallback: RwLock<Option<String>>,
    pub(crate) offers: Arc<OfferRouter>,
    /// Fires on `stop`: every background task and responder run.
    pub(crate) cancel: CancellationToken,
    pub(crate) tracker: TaskTracker,
    localapi: tokio::sync::Mutex<Option<LocalApi>>,
    local: Mutex<LocalInfo>,
    running_at: Instant,
    sidecar_path: PathBuf,
    hub_start_ms: u64,
    /// The responder's bounds on concurrent requests.
    pub(crate) limits: control::ResponderLimits,
    /// Set first thing in [`Hub::stop`]: no new runs, either role.
    pub(crate) stopping: AtomicBool,
}

impl Live {
    /// The signed-in login: as the sidecar reported it, or else as the
    /// LocalAPI did ([`resolve_login`]).
    pub(crate) fn local_login(&self) -> Option<String> {
        self.node.local_info().login_name.or_else(|| {
            self.login_fallback
                .read()
                .unwrap_or_else(PoisonError::into_inner)
                .clone()
        })
    }

    /// This hub's hello template.
    pub(crate) fn hello(&self) -> PeerHello {
        self.hello
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    async fn localapi(&self) -> Result<LocalApi, AppError> {
        let mut slot = self.localapi.lock().await;
        if let Some(api) = slot.as_ref() {
            return Ok(api.clone());
        }
        let api = LocalApi::from_node(&self.node).await?;
        *slot = Some(api.clone());
        Ok(api)
    }

    /// Usermetrics now, or `None` when the LocalAPI cannot be read.
    pub(crate) async fn usermetrics(&self) -> Option<UserMetrics> {
        match self.localapi().await {
            Ok(api) => match api.usermetrics().await {
                Ok(m) => Some(m),
                Err(e) => {
                    tracing::debug!(error = %e, "usermetrics unavailable");
                    None
                }
            },
            Err(e) => {
                tracing::debug!(error = %e, "LocalAPI unavailable");
                None
            }
        }
    }

    fn local(&self) -> LocalInfo {
        self.local
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

struct ActiveEntry {
    info: ActiveRun,
    progress: Arc<RunProgress>,
    cancel: CancellationToken,
}

#[derive(Default)]
struct Lifecycle {
    lock: Option<fd_lock::RwLock<File>>,
}

/// Everything a hub shares with its background tasks and the responder.
pub(crate) struct Core {
    pub(crate) cfg: HubConfig,
    allow_other: AtomicBool,
    events: broadcast::Sender<HubEvent>,
    state: Mutex<NodeState>,
    live: RwLock<Option<Arc<Live>>>,
    pub(crate) slots: Arc<Slots>,
    pub(crate) remote: Mutex<RemoteRuns>,
    book: Mutex<PeerBook>,
    dirty: Notify,
    active: Mutex<HashMap<String, ActiveEntry>>,
    active_changed: Notify,
    suites: Mutex<HashMap<String, CancellationToken>>,
    store: RunStore,
    lifecycle: tokio::sync::Mutex<Lifecycle>,
    start_abort: Mutex<Option<tokio::task::AbortHandle>>,
    same_process: AtomicBool,
    sibling: Mutex<Option<Weak<Core>>>,
    reauth_epoch: AtomicU64,
    report_hook: RwLock<Option<ReportHook>>,
    /// Workloads this hub's responder refuses as unsupported although the
    /// build implements them (tests of the `unsupported-workload` refusal).
    #[cfg(test)]
    withheld: Mutex<HashSet<Workload>>,
    /// Tests: every run prepared here gets this watchdog instead.
    #[cfg(test)]
    watchdog: Mutex<Option<Duration>>,
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Core {
    pub(crate) fn emit(&self, ev: HubEvent) {
        let _ = self.events.send(ev);
    }

    pub(crate) fn allow_other_logins(&self) -> bool {
        self.allow_other.load(Ordering::Relaxed)
    }

    fn live(&self) -> Option<Arc<Live>> {
        self.live
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    fn tailscale_hostname(&self) -> Option<String> {
        let app = truffle_core::AppId::parse(&self.cfg.app_id).ok()?;
        let name = truffle_core::DeviceName::parse(self.cfg.device_name.clone());
        Some(truffle_core::tailscale_hostname(&app, &name))
    }

    fn set_state(&self, mut st: NodeState) {
        if st.tailscale_hostname.is_none() {
            st.tailscale_hostname = self.tailscale_hostname();
        }
        *lock(&self.state) = st.clone();
        self.emit(HubEvent::Node(Box::new(st)));
    }

    fn state(&self) -> NodeState {
        lock(&self.state).clone()
    }

    fn mark_peers_dirty(&self) {
        self.dirty.notify_one();
    }

    /// A peer said hello to us; say hello back if we have no hello from it
    /// yet, or if what it says about itself changed since its last
    /// `hello.ack` (its login became known after a fast restart, it is a
    /// new instance under the same node, or it restarted with another build
    /// or sidecar), so our cached view (and its answer to whether it
    /// accepts runs from us) is refreshed before it describes the peer in a
    /// report.
    pub(crate) fn remember_inbound_hello(self: &Arc<Self>, from: &str, theirs: PeerHello) {
        let Some(live) = self.live() else {
            return;
        };
        let stale = lock(&self.book)
            .hello
            .get(from)
            .is_none_or(|known| hello_changed(known, &theirs));
        if stale {
            let core = self.clone();
            let from = from.to_string();
            live.tracker.clone().spawn(async move {
                if let Ok(Some(peer)) = live.node.peer(&from, Some(2_000)).await {
                    spawn_hello(&core, &live, &peer);
                }
            });
        }
    }

    /// `ours`: this hub's hello (its login and host fingerprint).
    fn bench_peer(&self, peer: &Peer, local_login: Option<&str>, ours: &PeerHello) -> BenchPeer {
        let mut bp = BenchPeer::from_peer(peer, local_login);
        let book = lock(&self.book);
        bp.hello = book.hello.get(&peer.tailscale_id).cloned();
        bp.hello_error = book.hello_error.get(&peer.tailscale_id).cloned();
        bp.rtt_ms = book.rtt_ms.get(&peer.tailscale_id).copied();
        bp.same_host = bp.hello.as_ref().and_then(|h| {
            env::same_host_verdict(
                ours.host_fingerprint.as_deref(),
                ours.host_fingerprint_weak,
                h.host_fingerprint.as_deref(),
                h.host_fingerprint_weak,
            )
        });
        bp
    }

    async fn peer_list(&self, live: &Live) -> Vec<BenchPeer> {
        let login = live.local_login();
        let ours = live.hello();
        let mut peers: Vec<BenchPeer> = live
            .node
            .peers()
            .await
            .iter()
            .map(|p| self.bench_peer(p, login.as_deref(), &ours))
            .collect();
        peers.sort_by(|a, b| {
            b.online.cmp(&a.online).then_with(|| {
                a.device_name
                    .to_lowercase()
                    .cmp(&b.device_name.to_lowercase())
            })
        });
        peers
    }

    fn timings_for(&self, live: &Live, peer_id: &str) -> PeerTimings {
        let book = lock(&self.book);
        PeerTimings {
            hub_start_ms: Some(live.hub_start_ms),
            discovery_ms: book.discovery_ms.get(peer_id).copied(),
            ws_connect_ms: book.ws_connect_ms.get(peer_id).copied(),
        }
    }

    /// Whether this hub's responder accepts `prepare` for `w`.
    pub(crate) fn offers_workload(&self, w: Workload) -> bool {
        #[cfg(test)]
        if lock(&self.withheld).contains(&w) {
            return false;
        }
        workloads::implemented(w)
    }

    /// Tests: the watchdog every run prepared here gets, if overridden.
    #[cfg(test)]
    pub(crate) fn watchdog_override(&self) -> Option<Duration> {
        *lock(&self.watchdog)
    }

    fn sibling_sidecar_pid(&self) -> Option<u32> {
        let sib = lock(&self.sibling).as_ref().and_then(Weak::upgrade)?;
        sib.live().and_then(|l| l.node.sidecar_pid())
    }
}

/// Whether a peer's inbound hello says something about itself that our
/// cached `hello.ack` from it does not. Per-asker and volatile fields
/// (`accepts_runs_from_you`, `busy`) are ignored, and the host fingerprint
/// counts only when the cached one has it (it is withheld from peers that
/// are not admitted).
fn hello_changed(known: &PeerHello, theirs: &PeerHello) -> bool {
    known.login_name != theirs.login_name
        || known.device_id != theirs.device_id
        || known.protocol != theirs.protocol
        || known.app != theirs.app
        || known.tool_version != theirs.tool_version
        || known.truffle_version != theirs.truffle_version
        || known.build_profile != theirs.build_profile
        || known.git_sha != theirs.git_sha
        || known.sidecar_sha256 != theirs.sidecar_sha256
        || known.device_name != theirs.device_name
        || known.os != theirs.os
        || known.arch != theirs.arch
        || known.logical_cpus != theirs.logical_cpus
        || known.workloads != theirs.workloads
        || (known.host_fingerprint.is_some()
            && (known.host_fingerprint != theirs.host_fingerprint
                || known.host_fingerprint_weak != theirs.host_fingerprint_weak))
}

/// The hello this hub sends `peer`: the host fingerprint goes only to peers
/// it admits (§6 rule 1), as in its `hello.ack`; `busy` goes to everyone.
fn outgoing_hello(core: &Core, live: &Live, peer: Option<&Peer>) -> PeerHello {
    let mut ours = live.hello();
    ours.busy = core.slots.busy();
    let admitted = peer.is_some_and(|p| control::admit_on(core, live, p).is_ok());
    if !admitted {
        ours.host_fingerprint = None;
        ours.host_fingerprint_weak = false;
    }
    ours
}

// ---------------------------------------------------------------------------
// Background tasks: hello cache, peers publisher, re-auth
// ---------------------------------------------------------------------------

fn spawn_hello(core: &Arc<Core>, live: &Arc<Live>, peer: &Peer) {
    let id = peer.tailscale_id.clone();
    {
        let mut book = lock(&core.book);
        if !book.in_flight.insert(id.clone()) {
            return;
        }
        book.last_attempt.insert(id.clone(), Instant::now());
    }
    let ws_was_connected = peer.ws_connected;
    let ours = outgoing_hello(core, live, Some(peer));
    let core = core.clone();
    let live2 = live.clone();
    live.tracker.spawn(async move {
        let t0 = Instant::now();
        let res = tokio::select! {
            _ = live2.cancel.cancelled() => {
                lock(&core.book).in_flight.remove(&id);
                return;
            }
            r = control::hello(&live2.node, &id, &ours) => r,
        };
        {
            let mut book = lock(&core.book);
            book.in_flight.remove(&id);
            match res {
                Ok(h) => {
                    if !ws_was_connected && !book.ws_connect_ms.contains_key(&id) {
                        book.ws_connect_ms
                            .insert(id.clone(), t0.elapsed().as_millis() as u64);
                    }
                    book.hello.insert(id.clone(), h);
                    book.hello_error.remove(&id);
                }
                Err(e) => {
                    tracing::debug!(peer = %id, error = %e, "hello failed");
                    book.hello_error.insert(id.clone(), e);
                }
            }
        }
        core.mark_peers_dirty();
    });
}

/// Note a peer: discovery time, online transitions, hello when needed.
fn observe(core: &Arc<Core>, live: &Arc<Live>, peer: &Peer) {
    let id = &peer.tailscale_id;
    let want_hello = {
        let mut book = lock(&core.book);
        if !book.discovery_ms.contains_key(id) {
            book.discovery_ms
                .insert(id.clone(), live.running_at.elapsed().as_millis() as u64);
        }
        let was_online = book.online.insert(id.clone(), peer.online) == Some(true);
        if !peer.online {
            false
        } else if !was_online {
            true
        } else {
            !book.hello.contains_key(id)
                && book
                    .last_attempt
                    .get(id)
                    .is_none_or(|t| t.elapsed() >= HELLO_RETRY)
        }
    };
    if want_hello {
        spawn_hello(core, live, peer);
    }
}

async fn peer_watch(core: Arc<Core>, live: Arc<Live>) {
    use tokio::sync::broadcast::error::RecvError;
    let mut rx = live.node.on_peer_change();
    for p in live.node.peers().await {
        observe(&core, &live, &p);
    }
    core.mark_peers_dirty();
    loop {
        let ev = tokio::select! {
            biased;
            _ = live.cancel.cancelled() => break,
            e = rx.recv() => e,
        };
        match ev {
            Ok(PeerEvent::Joined(s) | PeerEvent::Updated(s) | PeerEvent::Identity(s)) => {
                if let Ok(Some(p)) = live.node.peer(&s.id, None).await {
                    observe(&core, &live, &p);
                }
            }
            Ok(PeerEvent::Left(s)) => {
                lock(&core.book).online.insert(s.id.clone(), false);
            }
            Ok(PeerEvent::WsConnected(id)) => {
                let has = lock(&core.book).hello.contains_key(&id);
                if !has {
                    if let Ok(Some(p)) = live.node.peer(&id, None).await {
                        spawn_hello(&core, &live, &p);
                    }
                }
            }
            Ok(PeerEvent::WsDisconnected(_)) => {}
            Ok(PeerEvent::AuthRequired { url }) => {
                reauth(&core, &live, url);
            }
            Err(RecvError::Lagged(_)) => {
                for p in live.node.peers().await {
                    observe(&core, &live, &p);
                }
            }
            Err(RecvError::Closed) => break,
        }
        core.mark_peers_dirty();
    }
}

/// The node asked for re-authentication: show `needs-login`, then poll
/// health until it is running again.
fn reauth(core: &Arc<Core>, live: &Arc<Live>, url: String) {
    let mut st = NodeState::new(NodeStatus::NeedsLogin);
    st.auth_url = Some(url);
    core.set_state(st);
    let epoch = core.reauth_epoch.fetch_add(1, Ordering::SeqCst) + 1;
    let core = core.clone();
    let live2 = live.clone();
    live.tracker.spawn(async move {
        loop {
            tokio::select! {
                _ = live2.cancel.cancelled() => return,
                _ = tokio::time::sleep(Duration::from_secs(2)) => {}
            }
            if core.reauth_epoch.load(Ordering::SeqCst) != epoch {
                return;
            }
            let health = live2.node.health().await;
            if health.state == "Running" {
                let mut local = live2.local();
                local.health = HealthSummary::from(health);
                local.login_name = live2.local_login();
                *lock(&live2.local) = local.clone();
                let mut st = NodeState::new(NodeStatus::Running);
                st.local = Some(local);
                core.set_state(st);
                return;
            }
        }
    });
}

/// When the LocalAPI is asked for the login after a start without one.
const LOGIN_RETRY: [Duration; 8] = [
    Duration::from_millis(250),
    Duration::from_millis(500),
    Duration::from_secs(1),
    Duration::from_secs(2),
    Duration::from_secs(3),
    Duration::from_secs(5),
    Duration::from_secs(8),
    Duration::from_secs(13),
];

/// The sidecar's start event carried no login (seen after a fast restart
/// from saved state: the node is Running before its netmap names the self
/// user, and core takes the login only from that event, so it stays
/// unknown). Admission refuses every peer while the login is unknown
/// (§6 rule 1), so read it from the LocalAPI (`/localapi/v0/status`) a few
/// times in the background, about 33 s in all; once known, update the local
/// info and hello, emit `running` again, re-hello every online peer (their
/// cached view of us said "does not accept runs from you") and republish
/// the peers (`same_login`).
async fn resolve_login(core: Arc<Core>, live: Arc<Live>) {
    let mut found = None;
    let started = Instant::now();
    for wait in LOGIN_RETRY {
        tokio::select! {
            _ = live.cancel.cancelled() => return,
            _ = tokio::time::sleep(wait) => {}
        }
        if let Some(login) = live.node.local_info().login_name {
            tracing::info!(
                after_ms = started.elapsed().as_millis() as u64,
                "login known from the sidecar"
            );
            found = Some(login);
            break;
        }
        match live.localapi().await {
            Ok(api) => match api.self_login().await {
                Ok(Some(login)) => {
                    tracing::info!(
                        after_ms = started.elapsed().as_millis() as u64,
                        "login known from the LocalAPI status"
                    );
                    *live
                        .login_fallback
                        .write()
                        .unwrap_or_else(PoisonError::into_inner) = Some(login.clone());
                    found = Some(login);
                    break;
                }
                Ok(None) => {}
                Err(e) => tracing::debug!(error = %e, "login lookup: LocalAPI status failed"),
            },
            Err(e) => tracing::debug!(error = %e, "login lookup: LocalAPI unavailable"),
        }
    }
    let Some(login) = found else {
        tracing::warn!(
            "this device's login is still unknown; runs from peers are refused until it is known"
        );
        return;
    };
    live.hello
        .write()
        .unwrap_or_else(PoisonError::into_inner)
        .login_name = Some(login.clone());
    let local = {
        let mut local = lock(&live.local);
        local.login_name = Some(login);
        local.clone()
    };
    if core.state().state == NodeStatus::Running {
        let mut st = NodeState::new(NodeStatus::Running);
        st.local = Some(local);
        core.set_state(st);
    }
    for p in live.node.peers().await {
        if p.online {
            spawn_hello(&core, &live, &p);
        }
    }
    core.mark_peers_dirty();
}

async fn peers_publisher(core: Arc<Core>, live: Arc<Live>) {
    loop {
        tokio::select! {
            _ = live.cancel.cancelled() => break,
            _ = core.dirty.notified() => {}
        }
        tokio::select! {
            _ = live.cancel.cancelled() => break,
            _ = tokio::time::sleep(PEERS_DEBOUNCE) => {}
        }
        let peers = core.peer_list(&live).await;
        core.emit(HubEvent::Peers { peers });
    }
}

// ---------------------------------------------------------------------------
// Hub
// ---------------------------------------------------------------------------

/// One benchmark hub (§5.4).
pub struct Hub {
    core: Arc<Core>,
}

impl std::fmt::Debug for Hub {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Hub")
            .field("cfg", &self.core.cfg)
            .field("state", &self.core.state().state)
            .finish()
    }
}

impl Drop for Hub {
    fn drop(&mut self) {
        // Without stop(): end the background tasks so the node (and its
        // sidecar, kill_on_drop) goes away with the last reference.
        if let Some(live) = self.core.live() {
            live.cancel.cancel();
        }
        if let Some(h) = lock(&self.core.start_abort).take() {
            h.abort();
        }
    }
}

/// Where a run sits in its suite (`ActiveRun.suite_*`).
struct SuiteSlot {
    profile: String,
    index: u32,
    total: u32,
}

/// What `run_inner` learned, for the report.
#[derive(Default)]
struct Flow {
    peer: Option<Peer>,
    hello: Option<PeerHello>,
    settle_ms: u64,
    before: Option<PathSnapshot>,
    after: Option<PathSnapshot>,
    metrics_before: Option<UserMetrics>,
    metrics_after: Option<UserMetrics>,
    samples: Option<crate::resources::ResourceSamples>,
    output: Option<WorkloadOutput>,
    params_used: Option<WorkloadParams>,
    clock_before: Option<crate::types::ClockOffset>,
    clock_after: Option<crate::types::ClockOffset>,
    timeline: Vec<crate::types::PathTimelinePoint>,
    loaded: Option<crate::types::Distribution>,
    peer_finish: Option<workloads::PeerFinish>,
    peer_overlap: u32,
    token: Option<RunToken>,
    failed_phase: Option<Phase>,
    notes: Vec<String>,
}

impl Hub {
    /// A hub in state `idle`. [`subscribe`](Self::subscribe) works at once.
    pub fn new(cfg: HubConfig) -> Arc<Hub> {
        let (events, _) = broadcast::channel(256);
        let store = RunStore::new(cfg.runs_dir());
        let allow_other = AtomicBool::new(cfg.allow_other_logins);
        let mut st = NodeState::new(NodeStatus::Idle);
        let core = Arc::new(Core {
            cfg,
            allow_other,
            events,
            state: Mutex::new(NodeState::new(NodeStatus::Idle)),
            live: RwLock::new(None),
            slots: Slots::new(),
            remote: Mutex::new(RemoteRuns::default()),
            book: Mutex::new(PeerBook::default()),
            dirty: Notify::new(),
            active: Mutex::new(HashMap::new()),
            active_changed: Notify::new(),
            suites: Mutex::new(HashMap::new()),
            store,
            lifecycle: tokio::sync::Mutex::new(Lifecycle::default()),
            start_abort: Mutex::new(None),
            same_process: AtomicBool::new(false),
            sibling: Mutex::new(None),
            reauth_epoch: AtomicU64::new(0),
            report_hook: RwLock::new(None),
            #[cfg(test)]
            withheld: Mutex::new(HashSet::new()),
            #[cfg(test)]
            watchdog: Mutex::new(None),
        });
        st.tailscale_hostname = core.tailscale_hostname();
        *lock(&core.state) = st;
        Arc::new(Hub { core })
    }

    /// The configuration this hub was created with.
    pub fn config(&self) -> &HubConfig {
        &self.core.cfg
    }

    /// Start the node: lock the state dir, resolve the sidecar, build the
    /// node (emitting `needs-login` for every login URL), then install the
    /// offer router and start the responder, the hello cache and the peers
    /// publisher. Emits `running` on success and `error` on failure.
    /// Returns once running (or failed); [`stop`](Self::stop) aborts it.
    pub async fn start(self: &Arc<Self>) -> Result<(), AppError> {
        let core = &self.core;
        let mut lc = core.lifecycle.lock().await;
        if let Some(live) = core.live() {
            if !live.stopping.load(Ordering::SeqCst) {
                return Ok(());
            }
            // A stop that was interrupted (its future dropped): finish it.
            live.cancel.cancel();
            live.node.stop().await;
            core.live
                .write()
                .unwrap_or_else(PoisonError::into_inner)
                .take();
            lc.lock = None;
        }
        // What we knew about peers belongs to the previous session.
        *lock(&core.book) = PeerBook::default();
        clock::init();
        let t0 = Instant::now();
        let fail = |core: &Core, e: AppError| {
            let mut st = NodeState::new(NodeStatus::Error);
            st.error = Some(e.clone());
            core.set_state(st);
            e
        };
        // State lock first: two hubs must never share a tsnet state dir.
        let lock_file = match acquire_state_lock(&core.cfg.state_dir).await {
            Ok(l) => l,
            Err(e) => return Err(fail(core, e)),
        };
        lc.lock = Some(lock_file);
        let sidecar = match resolve_sidecar(&core.cfg) {
            Ok(p) => p,
            Err(e) => {
                lc.lock = None;
                return Err(fail(core, e));
            }
        };
        core.set_state(NodeState::new(NodeStatus::Starting));

        let builder = match self.builder(&sidecar) {
            Ok(b) => b,
            Err(e) => {
                lc.lock = None;
                return Err(fail(core, e));
            }
        };
        let weak = Arc::downgrade(core);
        let on_auth = move |url: String| {
            if let Some(core) = weak.upgrade() {
                let mut st = NodeState::new(NodeStatus::NeedsLogin);
                st.auth_url = Some(url);
                core.set_state(st);
            }
        };
        let build = tokio::spawn(builder.build_with_auth_handler(on_auth));
        *lock(&core.start_abort) = Some(build.abort_handle());
        let built = build.await;
        lock(&core.start_abort).take();
        let node = match built {
            Ok(Ok(node)) => Arc::new(node),
            Ok(Err(e)) => {
                lc.lock = None;
                return Err(fail(core, AppError::from(e).context("start")));
            }
            Err(_) => {
                lc.lock = None;
                core.set_state(NodeState::new(NodeStatus::Stopped));
                return Err(AppError::cancelled());
            }
        };
        let hub_start_ms = t0.elapsed().as_millis() as u64;
        let identity = node.local_info();
        let health = HealthSummary::from(node.health().await);
        let local = LocalInfo::from_identity(&identity, health, &sidecar, Some(hub_start_ms));
        let (tool, name, device_id, login, path) = (
            core.cfg.tool,
            core.cfg.device_name.clone(),
            Some(identity.device_id.clone()),
            identity.login_name.clone(),
            sidecar.clone(),
        );
        let hello = tokio::task::spawn_blocking(move || {
            env::local_hello(
                tool,
                &name,
                device_id,
                login,
                Some(&path),
                workloads::implemented_names(),
            )
        })
        .await
        .map_err(|e| AppError::internal(format!("build the local hello: {e}")))?;
        let cancel = CancellationToken::new();
        let tracker = TaskTracker::new();
        let offers = OfferRouter::install(node.clone(), cancel.clone(), &tracker).await;
        let login_known = identity.login_name.is_some();
        let live = Arc::new(Live {
            node,
            hello: RwLock::new(hello),
            login_fallback: RwLock::new(None),
            offers,
            cancel,
            tracker,
            localapi: tokio::sync::Mutex::new(None),
            local: Mutex::new(local.clone()),
            running_at: Instant::now(),
            sidecar_path: sidecar,
            hub_start_ms,
            limits: control::ResponderLimits::default(),
            stopping: AtomicBool::new(false),
        });
        *core.live.write().unwrap_or_else(PoisonError::into_inner) = Some(live.clone());
        live.tracker
            .spawn(control::responder_loop(core.clone(), live.clone()));
        live.tracker.spawn(peer_watch(core.clone(), live.clone()));
        live.tracker
            .spawn(peers_publisher(core.clone(), live.clone()));
        if !login_known {
            live.tracker
                .spawn(resolve_login(core.clone(), live.clone()));
        }
        let mut st = NodeState::new(NodeStatus::Running);
        st.local = Some(local);
        core.set_state(st);
        Ok(())
    }

    fn builder(&self, sidecar: &Path) -> Result<truffle_core::NodeBuilder, AppError> {
        let cfg = &self.core.cfg;
        let state_dir = cfg.state_dir.to_str().ok_or_else(|| {
            AppError::invalid_params(format!(
                "the state directory {} is not valid UTF-8",
                cfg.state_dir.display()
            ))
        })?;
        let mut b = BenchNode::builder()
            .app_id(cfg.app_id.clone())
            .map_err(|e| AppError::invalid_params(format!("app id: {e}")))?
            .device_name(cfg.device_name.clone())
            .state_dir(state_dir)
            .sidecar_path(sidecar.to_path_buf())
            .ephemeral(cfg.ephemeral);
        if let Some(key) = cfg.auth_key.as_deref() {
            b = b.auth_key(key);
        }
        Ok(b)
    }

    /// Stop, valid in every state: aborts a start in progress (dropping the
    /// build kills the sidecar), cancels this hub's runs and suites, tears
    /// down every run peers prepared here, stops background tasks, then the
    /// node, and releases the state lock.
    pub async fn stop(&self) {
        let core = &self.core;
        if let Some(h) = lock(&core.start_abort).take() {
            h.abort();
        }
        let mut lc = core.lifecycle.lock().await;
        // `live` stays in place until the node has stopped, so the reports
        // of runs cancelled below still carry this side's environment; the
        // stopping flag refuses new runs in either role meanwhile.
        let live = core.live();
        let Some(live) = live else {
            lc.lock = None;
            let st = core.state().state;
            if matches!(st, NodeStatus::Starting | NodeStatus::NeedsLogin) {
                core.set_state(NodeState::new(NodeStatus::Stopped));
            }
            return;
        };
        live.stopping.store(true, Ordering::SeqCst);
        core.set_state(NodeState::new(NodeStatus::Stopping));
        // 1. This hub's own runs and suites.
        for t in lock(&core.suites).values() {
            t.cancel();
        }
        for e in lock(&core.active).values() {
            e.cancel.cancel();
        }
        let drained = tokio::time::timeout(STOP_DRAIN, async {
            loop {
                let notified = core.active_changed.notified();
                if lock(&core.active).is_empty() {
                    break;
                }
                notified.await;
            }
        })
        .await;
        if drained.is_err() {
            tracing::warn!("stop: runs did not wind down in time");
        }
        // 2. Runs peers prepared here (rule 7: before Node::stop). A prepare
        // still in flight checks the stopping flag under the same lock as
        // this listing, so it is either listed here or refused.
        let keys = lock(&core.remote).keys();
        for key in keys {
            control::abort_remote(core, &key, RemoteRunState::Cancelled).await;
        }
        // 3. Background tasks (watchdogs end with their runs' tokens).
        live.cancel.cancel();
        live.tracker.close();
        if tokio::time::timeout(Duration::from_secs(5), live.tracker.wait())
            .await
            .is_err()
        {
            tracing::warn!("stop: background tasks did not finish in 5 s");
        }
        // 4. The node.
        live.node.stop().await;
        core.live
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        lc.lock = None;
        core.set_state(NodeState::new(NodeStatus::Stopped));
    }

    /// The current lifecycle state.
    pub fn state(&self) -> NodeState {
        self.core.state()
    }

    /// Hub events: node state, peers, remote runs.
    pub fn subscribe(&self) -> broadcast::Receiver<HubEvent> {
        self.core.events.subscribe()
    }

    /// This hub's identity while running.
    pub fn local(&self) -> Option<LocalInfo> {
        self.core.live().map(|l| l.local())
    }

    /// The node while running.
    pub fn node(&self) -> Option<Arc<BenchNode>> {
        self.core.live().map(|l| l.node.clone())
    }

    /// The run history.
    pub fn store(&self) -> &RunStore {
        &self.core.store
    }

    /// Accept (or stop accepting) runs from peers signed in with other accounts.
    pub fn set_allow_other_logins(&self, on: bool) {
        self.core.allow_other.store(on, Ordering::Relaxed);
    }

    /// Call `hook` with every run's report just before it is saved to the
    /// [`store`](Self::store) (and returned), replacing any earlier hook: the
    /// app sets `local.app_foreground_pct` here instead of rewriting the
    /// stored file. `compare_key` is recomputed afterwards; a hook that
    /// panics leaves the report as it was before the call.
    pub fn set_report_hook(&self, hook: ReportHook) {
        *self
            .core
            .report_hook
            .write()
            .unwrap_or_else(PoisonError::into_inner) = Some(hook);
    }

    /// Remove the [`set_report_hook`](Self::set_report_hook) hook.
    pub fn clear_report_hook(&self) {
        *self
            .core
            .report_hook
            .write()
            .unwrap_or_else(PoisonError::into_inner) = None;
    }

    fn apply_report_hook(&self, report: &mut RunReport) {
        let hook = self
            .core
            .report_hook
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let Some(hook) = hook else {
            return;
        };
        let mut amended = report.clone();
        let ran = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| hook(&mut amended)));
        match ran {
            Ok(()) => {
                amended.compare_key = amended.compute_compare_key();
                *report = amended;
            }
            Err(_) => tracing::warn!(run_id = %report.run_id, "the report hook panicked"),
        }
    }

    /// Mark this hub as one half of an in-process pair: reports say
    /// `same-process` and resources become `combined` (this process plus
    /// both sidecars).
    pub fn set_same_process_sibling(&self, sibling: &Hub) {
        self.core.same_process.store(true, Ordering::Relaxed);
        *lock(&self.core.sibling) = Some(Arc::downgrade(&sibling.core));
    }

    fn require_live(&self) -> Result<Arc<Live>, AppError> {
        self.core
            .live()
            .filter(|l| !l.stopping.load(Ordering::SeqCst))
            .ok_or_else(AppError::not_running)
    }

    /// Every same-app peer, enriched with hello, admission and RTT.
    pub async fn peers(&self) -> Vec<BenchPeer> {
        match self.core.live() {
            Some(live) => self.core.peer_list(&live).await,
            None => Vec::new(),
        }
    }

    /// Resolve a peer reference, waiting up to `wait_ms` for discovery.
    pub async fn resolve_peer(&self, query: &str, wait_ms: u64) -> Result<Peer, AppError> {
        let live = self.require_live()?;
        resolve(&live, query, wait_ms).await
    }

    /// A fresh `hello` with a peer (by tailscale id); updates the cache.
    pub async fn hello_peer(&self, peer_id: &str) -> Result<PeerHello, AppError> {
        let live = self.require_live()?;
        let peer = live
            .node
            .peer(peer_id, Some(control::PEER_LOOKUP_MS))
            .await
            .ok()
            .flatten();
        let ours = outgoing_hello(&self.core, &live, peer.as_ref());
        let res = control::hello(&live.node, peer_id, &ours).await;
        {
            let mut book = lock(&self.core.book);
            match &res {
                Ok(h) => {
                    book.hello.insert(peer_id.to_string(), h.clone());
                    book.hello_error.remove(peer_id);
                }
                Err(e) => {
                    book.hello_error.insert(peer_id.to_string(), e.clone());
                }
            }
        }
        self.core.mark_peers_dirty();
        res
    }

    /// Resolve a peer, refresh its hello and take one path snapshot. Counts
    /// as an exempt run for overlap accounting.
    pub async fn probe(&self, peer: &str) -> Result<PeerProbe, AppError> {
        let live = self.require_live()?;
        let _token = self.core.slots.acquire_exempt();
        let p = resolve(&live, peer, 5_000).await?;
        let hello = self.hello_peer(&p.tailscale_id).await?;
        let path = pathwatch::snapshot(&live.node, &p.tailscale_id).await;
        if let Some(rtt) = path.rtt_ms {
            lock(&self.core.book)
                .rtt_ms
                .insert(p.tailscale_id.clone(), rtt);
        }
        let login = live.local_login();
        Ok(PeerProbe {
            peer: self.core.bench_peer(&p, login.as_deref(), &live.hello()),
            hello,
            path,
        })
    }

    /// Runs this hub is initiating, with their latest progress.
    pub fn active_runs(&self) -> Vec<ActiveRun> {
        let active = lock(&self.core.active);
        let mut runs: Vec<ActiveRun> = active
            .values()
            .map(|e| ActiveRun {
                last: e.progress.last(),
                ..e.info.clone()
            })
            .collect();
        runs.sort_by(|a, b| a.started_at.cmp(&b.started_at));
        runs
    }

    /// Stream an active run's progress to another sink, replaying the last
    /// event first. `false` if no such run is active.
    pub fn attach(&self, run_id: &str, progress: ProgressSink) -> bool {
        let entry = lock(&self.core.active)
            .get(run_id)
            .map(|e| e.progress.clone());
        match entry {
            Some(p) => {
                p.attach(progress);
                true
            }
            None => false,
        }
    }

    /// Cancel a run or a suite by id. `false` if neither is active.
    pub fn cancel(&self, id: &str) -> bool {
        if let Some(t) = lock(&self.core.suites).get(id) {
            t.cancel();
            return true;
        }
        if let Some(e) = lock(&self.core.active).get(id) {
            e.cancel.cancel();
            return true;
        }
        false
    }

    /// Sign out: LocalAPI logout (removes the device from the tailnet), stop,
    /// then delete the state directory. If the logout fails the hub still
    /// stops and forgets its state, and the error says to remove the device
    /// in the admin console.
    pub async fn sign_out(&self) -> Result<(), AppError> {
        let logout = match self.core.live() {
            Some(live) => match live.localapi().await {
                Ok(api) => api.logout().await,
                Err(e) => Err(e),
            },
            None => Ok(()),
        };
        self.stop().await;
        let dir = &self.core.cfg.state_dir;
        match std::fs::remove_dir_all(dir) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(AppError::io(format!("delete {}", dir.display()), &e)),
        }
        logout.map_err(|e| {
            AppError::new(
                e.code,
                format!(
                    "signed out on this device, but the tailnet logout failed ({}); \
                     remove {} in the Tailscale admin console",
                    e.message, self.core.cfg.device_name
                ),
            )
        })
    }

    // ── runs ───────────────────────────────────────────────────────────

    /// Run one workload against a peer (§6 "Initiator flow"). Never panics:
    /// every failure is a report with `status: failed` (or `cancelled`) and
    /// an `error_code`. The report is saved to [`store`](Self::store).
    pub async fn run(&self, req: RunRequest, progress: ProgressSink) -> RunReport {
        self.run_with(req, progress, None).await
    }

    /// `suite`: the suite token (the run's cancel token is its child) and
    /// where the run sits in its suite.
    async fn run_with(
        &self,
        req: RunRequest,
        sink: ProgressSink,
        suite: Option<(&CancellationToken, SuiteSlot)>,
    ) -> RunReport {
        let (parent, slot) = match suite {
            Some((t, s)) => (Some(t), Some(s)),
            None => (None, None),
        };
        let core = &self.core;
        // Calendar time for the report and ActiveRun (now_ns lags the wall
        // clock after a sleep); the monotonic instant for durations.
        let started_wall = clock::wall_ns();
        let started_at = Instant::now();
        // u64 before adding: the params are not validated yet.
        let data_total = req.params.time_based().then(|| {
            Duration::from_secs(
                u64::from(req.params.warmup_secs().unwrap_or(0))
                    + u64::from(req.params.duration_secs().unwrap_or(0)),
            )
        });
        let progress = RunProgress::new(&req.run_id, req.suite_id.clone(), data_total, sink);
        let cancel = parent.map_or_else(CancellationToken::new, CancellationToken::child_token);
        let mut flow = Flow::default();

        // Register (or fail before anything else happens).
        let registered = validate_run_id(&req.run_id).and_then(|()| {
            let mut active = lock(&core.active);
            if active.contains_key(&req.run_id) {
                return Err(AppError::invalid_params(format!(
                    "run {} is already running",
                    req.run_id
                )));
            }
            active.insert(
                req.run_id.clone(),
                ActiveEntry {
                    info: ActiveRun {
                        run_id: req.run_id.clone(),
                        suite_id: req.suite_id.clone(),
                        suite_profile: slot.as_ref().map(|s| s.profile.clone()),
                        suite_index: slot.as_ref().map(|s| s.index),
                        suite_total: slot.as_ref().map(|s| s.total),
                        workload: req.params.workload().as_str().to_string(),
                        peer_name: req.peer.clone(),
                        started_at: rfc3339(started_wall),
                        last: None,
                    },
                    progress: progress.clone(),
                    cancel: cancel.clone(),
                },
            );
            Ok(())
        });
        let was_registered = registered.is_ok();
        let outcome = match registered {
            Err(e) => Err(e),
            Ok(()) => {
                let ticker = progress.spawn_ticker();
                let r = self.run_inner(&req, &progress, &cancel, &mut flow).await;
                ticker.abort();
                r
            }
        };
        let outcome = match outcome {
            Err(e) if cancel.is_cancelled() && e.code != ErrorCode::Cancelled => {
                Err(AppError::cancelled().context(e.message))
            }
            other => other,
        };
        if outcome.is_err() && flow.failed_phase.is_none() {
            flow.failed_phase = progress.phase();
        }
        let mut report = self
            .assemble(&req, started_wall, started_at, flow, &outcome)
            .await;
        // A request that never registered (bad or duplicate run id) never ran:
        // its report is returned but not stored.
        if was_registered {
            self.apply_report_hook(&mut report);
            if let Err(e) = core.store.save(&report) {
                tracing::warn!(error = %e, "could not save the report");
                report.add_note(format!("could not save the report: {}", e.message));
            }
        }
        let (phase, message) = match report.status {
            RunStatus::Ok => (Phase::Done, None),
            RunStatus::Failed => (Phase::Failed, report.error.clone()),
            RunStatus::Cancelled => (Phase::Cancelled, report.error.clone()),
        };
        progress.set_phase(phase, message);
        if lock(&core.active).remove(&req.run_id).is_some() {
            core.active_changed.notify_waiters();
        }
        report
    }

    async fn run_inner(
        &self,
        req: &RunRequest,
        progress: &Arc<RunProgress>,
        cancel: &CancellationToken,
        flow: &mut Flow,
    ) -> Result<(), AppError> {
        let core = &self.core;
        progress.set_phase(Phase::Preparing, None);
        let live = self.require_live()?;
        let params = req.params.clone();
        params.validate().map_err(AppError::invalid_params)?;
        let workload = params.workload();
        if !workloads::implemented(workload) {
            return Err(workloads::unsupported(workload));
        }
        // 1. Resolve the peer once; its tailscale id is the only key from here on.
        let peer = tokio::select! {
            _ = cancel.cancelled() => return Err(AppError::cancelled()),
            p = resolve(&live, &req.peer, PEER_WAIT_MS) => p?,
        };
        let peer_name = peer
            .device_name
            .clone()
            .unwrap_or_else(|| peer.display_name.clone());
        if let Some(e) = lock(&core.active).get_mut(&req.run_id) {
            e.info.peer_name = peer_name.clone();
        }
        flow.peer = Some(peer.clone());
        // Rule 3: one data-plane run per hub (path is exempt).
        flow.token = Some(if workload == Workload::Path {
            core.slots.acquire_exempt()
        } else {
            core.slots.acquire_data(SlotHolder {
                role: SlotRole::Initiating,
                run_id: req.run_id.clone(),
                peer_name: peer_name.clone(),
                workload,
            })?
        });
        // 2–3. Hello (cached), protocol, workload.
        let cached = lock(&core.book).hello.get(&peer.tailscale_id).cloned();
        let hello = match cached {
            Some(h) => h,
            None => tokio::select! {
                _ = cancel.cancelled() => return Err(AppError::cancelled()),
                h = self.hello_peer(&peer.tailscale_id) => h?,
            },
        };
        flow.hello = Some(hello.clone());
        if hello.protocol != PROTOCOL {
            return Err(AppError::new(
                ErrorCode::ProtocolMismatch,
                format!(
                    "{peer_name} speaks control protocol {}, this hub speaks {PROTOCOL}; update one of them",
                    hello.protocol
                ),
            ));
        }
        if !hello.workloads.iter().any(|w| w == workload.as_str()) {
            return Err(AppError::new(
                ErrorCode::UnsupportedWorkload,
                format!(
                    "{peer_name} does not offer {workload} (it offers {})",
                    hello.workloads.join(", ")
                ),
            ));
        }
        // 4. Settle the path; take the before snapshot.
        progress.set_phase(Phase::Settling, None);
        let settled = tokio::select! {
            _ = cancel.cancelled() => return Err(AppError::cancelled()),
            s = pathwatch::settle(&live.node, &peer.tailscale_id) => s,
        };
        flow.settle_ms = settled.settle_ms;
        if let Some(rtt) = settled.snapshot.rtt_ms {
            lock(&core.book)
                .rtt_ms
                .insert(peer.tailscale_id.clone(), rtt);
        }
        if !settled.settled {
            flow.notes.push(format!(
                "the path did not settle within 3 s ({} pings)",
                settled.pings
            ));
        }
        flow.before = Some(settled.snapshot.clone());
        // 5. Usermetrics; 6. resources.
        flow.metrics_before = live.usermetrics().await;
        let same_process = core.same_process.load(Ordering::Relaxed);
        let mut pids = vec![live.node.sidecar_pid()];
        if same_process {
            pids.push(core.sibling_sidecar_pid());
        }
        let sampler = ResourceSampler::start(ResourceTargets::this_process(pids));
        // 7. The workload.
        let deadline = Instant::now() + params.deadline();
        let mut ctx = RunCtx::new(RunCtxInit {
            node: live.node.clone(),
            peer: peer.clone(),
            hello,
            run_id: req.run_id.clone(),
            suite_id: req.suite_id.clone(),
            params: params.clone(),
            progress,
            cancel: cancel.clone(),
            deadline,
            data_dir: core.cfg.data_dir.clone(),
            before: settled.snapshot,
            timings: core.timings_for(&live, &peer.tailscale_id),
            same_process,
        });
        let result = {
            let fut = std::panic::AssertUnwindSafe(workloads::run_initiator(&mut ctx, &params))
                .catch_unwind();
            tokio::select! {
                biased;
                _ = cancel.cancelled() => Err(AppError::cancelled()),
                _ = tokio::time::sleep_until(deadline) => Err(AppError::timeout(format!(
                    "the run passed its {} s deadline",
                    params.deadline().as_secs()
                ))),
                r = fut => r.unwrap_or_else(|panic| {
                    let what = panic
                        .downcast_ref::<String>()
                        .cloned()
                        .or_else(|| panic.downcast_ref::<&str>().map(|s| s.to_string()))
                        .unwrap_or_else(|| "unknown panic".into());
                    Err(AppError::internal(format!("the {workload} workload panicked: {what}")))
                }),
            }
        };
        if result.is_err() {
            flow.failed_phase = progress.phase();
        }
        ctx.stop_path_sampler().await;
        // Any error after prepare still tears the responder down.
        if ctx.prepare_sent && !ctx.finished {
            let msg = control::CancelMsg {
                run_id: req.run_id.clone(),
            };
            if let Err(e) = control::notify(
                &live.node,
                &peer.tailscale_id,
                control::msg_type::CANCEL,
                &msg,
                Duration::from_secs(5),
            )
            .await
            {
                tracing::debug!(error = %e, "could not send cancel to the responder");
            }
        }
        if result.is_err() {
            flow.output = ctx.partial.take();
        }
        flow.params_used = Some(ctx.params().clone());
        flow.clock_before = ctx.clock_before;
        flow.clock_after = ctx.clock_after;
        let (timeline, loaded) = ctx.take_timeline();
        flow.timeline = timeline;
        flow.loaded = loaded;
        flow.peer_finish = ctx.peer_finish;
        flow.peer_overlap = ctx.peer_finish.map_or(ctx.peer_prepare_overlap, |f| {
            f.max_overlapping_runs.max(ctx.peer_prepare_overlap)
        });
        if let Some(dir) = ctx.out_dir.take() {
            control::remove_run_dir(&dir);
        }
        drop(ctx);
        // 8. After snapshot, usermetrics, resources.
        progress.set_phase(Phase::Finishing, None);
        if !cancel.is_cancelled() {
            flow.after = Some(pathwatch::snapshot(&live.node, &peer.tailscale_id).await);
        }
        flow.metrics_after = live.usermetrics().await;
        flow.samples = Some(sampler.stop().await);
        flow.output = Some(result?);
        Ok(())
    }

    /// `started_wall`: the wall clock ([`clock::wall_ns`]) at the start;
    /// `started_at`: the same moment, monotonic.
    async fn assemble(
        &self,
        req: &RunRequest,
        started_wall: u64,
        started_at: Instant,
        mut flow: Flow,
        outcome: &Result<(), AppError>,
    ) -> RunReport {
        let core = &self.core;
        let live = core.live();
        let (tool, name) = (core.cfg.tool, core.cfg.device_name.clone());
        let sidecar = live.as_ref().map(|l| l.sidecar_path.clone());
        let ip = live.as_ref().and_then(|l| l.local().tailscale_ip);
        let local =
            tokio::task::spawn_blocking(move || env::node_env(tool, &name, sidecar.as_deref(), ip))
                .await
                .unwrap_or_else(|_| env_fallback(tool, &core.cfg.device_name));
        let peer_env = flow
            .hello
            .as_ref()
            .map(|h| NodeEnv::from_hello(h, flow.peer.as_ref().map(|p| p.ip.to_string())));
        let same_process = core.same_process.load(Ordering::Relaxed);
        let topology = if same_process {
            Topology::SameProcess
        } else if peer_env.as_ref().is_some_and(|p| {
            env::same_host(
                local.host_fingerprint.as_deref(),
                local.host_fingerprint_weak,
                p.host_fingerprint.as_deref(),
                p.host_fingerprint_weak,
            )
        }) {
            Topology::SameHost
        } else {
            Topology::SeparateHosts
        };
        let output = flow.output.take();
        let mut notes = std::mem::take(&mut flow.notes);

        // Network.
        let mut network = match &flow.before {
            Some(before) => NetworkInfo {
                topology,
                settle_ms: flow.settle_ms,
                before: before.clone(),
                after: flow.after.clone().unwrap_or_else(|| PathSnapshot {
                    at: clock::now_rfc3339(),
                    connection: None,
                    rtt_ms: None,
                    endpoint: None,
                    error: Some("not measured".into()),
                }),
                clock: flow
                    .clock_before
                    .map(|b| ClockMap::new(b, flow.clock_after).info()),
                path_timeline: flow.timeline.clone(),
                loaded_rtt_ms: flow.loaded,
                path_bytes: None,
                derp_share: None,
                path_label: crate::types::PathLabel::Unknown,
            },
            None => NetworkInfo::unmeasured(
                topology,
                outcome
                    .as_ref()
                    .err()
                    .map_or("not measured", |e| e.message.as_str()),
            ),
        };
        let local_bytes = match (&flow.metrics_before, &flow.metrics_after) {
            (Some(b), Some(a)) => Some(a.delta(b).path_bytes()),
            _ => None,
        };
        let peer_bytes = flow.peer_finish.and_then(|f| f.path_bytes);
        let needs_peer_bytes = flow.peer_finish.is_some();
        network.path_bytes = match (local_bytes, peer_bytes) {
            (Some(l), Some(p)) => Some(l.plus(&p)),
            (Some(l), None) => {
                if needs_peer_bytes {
                    notes.push("path bytes cover this side only".into());
                }
                Some(l)
            }
            (None, Some(p)) => {
                notes.push("path bytes cover the peer only".into());
                Some(p)
            }
            (None, None) => None,
        };
        if flow.before.is_some() {
            let verdict = pathwatch::path_label(
                network.path_bytes.as_ref(),
                &network.path_timeline,
                &network.before,
                &network.after,
            );
            network.path_label = verdict.label;
            network.derp_share = verdict.derp_share;
            notes.extend(verdict.notes);
        }

        // Resources.
        let goodput_bytes = output.as_ref().and_then(|o| o.goodput_bytes);
        let window = output.as_ref().and_then(|o| o.window_ns);
        let local_stats = flow
            .samples
            .as_ref()
            .and_then(|s| s.stats(window, goodput_bytes));
        let mut resources = Resources::default();
        if same_process {
            resources.combined = local_stats;
        } else {
            resources.local = local_stats;
            resources.peer = flow.peer_finish.and_then(|f| f.resources).map(|mut r| {
                fill_per_mb(&mut r, goodput_bytes);
                r
            });
        }
        let local_overlap = flow.token.as_ref().map_or(0, RunToken::overlapping);
        let overlapping_runs = local_overlap + flow.peer_overlap;

        // Summary placeholders the hub fills.
        let (mut summary, results, series, out_notes) = match output {
            Some(o) => (o.summary, o.results, o.series, o.notes),
            None => (
                Vec::new(),
                Value::Object(Default::default()),
                Vec::new(),
                Vec::new(),
            ),
        };
        for item in summary.iter_mut().filter(|s| s.value.is_none()) {
            match item.key.as_str() {
                KEY_CPU_PER_MB => {
                    item.value = cpu_per_mb(&resources, goodput_bytes, &mut notes);
                }
                KEY_LOADED_RTT_P50 => {
                    item.value = network.loaded_rtt_ms.and_then(|d| d.p50);
                }
                _ => {}
            }
        }
        let mut all_notes = out_notes;
        all_notes.extend(notes);

        let (status, error, error_code) = match outcome {
            Ok(()) => (RunStatus::Ok, None, None),
            Err(e) => {
                let status = if e.code == ErrorCode::Cancelled {
                    RunStatus::Cancelled
                } else {
                    RunStatus::Failed
                };
                if let Some(phase) = flow.failed_phase {
                    all_notes.push(format!("ended during the {} phase", phase_name(phase)));
                }
                (status, Some(e.message.clone()), Some(e.code))
            }
        };
        let params = flow
            .params_used
            .take()
            .unwrap_or_else(|| req.params.clone());
        // finished_at − started_at is exactly the run's (monotonic) length.
        let duration = started_at.elapsed();
        let mut report = RunReport {
            schema_version: SCHEMA_VERSION,
            run_id: if validate_run_id(&req.run_id).is_ok() {
                req.run_id.clone()
            } else {
                uuid::Uuid::new_v4().to_string()
            },
            suite_id: req.suite_id.clone(),
            tool: core.cfg.tool.report_tool(),
            tool_version: crate::TOOL_VERSION.to_string(),
            truffle_version: truffle_core::VERSION.to_string(),
            methodology: METHODOLOGY.to_string(),
            workload: params.workload(),
            label: params.label(),
            status,
            error,
            error_code,
            started_at: rfc3339(started_wall),
            finished_at: rfc3339(started_wall.saturating_add(duration.as_nanos() as u64)),
            duration_ms: duration.as_millis() as u64,
            git_sha: local.git_sha.clone(),
            git_dirty: local.git_dirty,
            compare_key: String::new(),
            local,
            peer: peer_env,
            network,
            params,
            summary,
            results,
            series,
            resources,
            overlapping_runs,
            notes: Vec::new(),
        };
        for n in all_notes {
            report.add_note(n);
        }
        report.apply_honesty_notes();
        report.compare_key = report.compute_compare_key();
        report
    }

    /// Run a suite profile item by item on this hub (§8.4), with a cooldown
    /// between items. One report per item is saved to the store.
    pub async fn suite(&self, req: SuiteRequest, progress: SuiteProgressSink) -> SuiteResult {
        self.suite_with(req, progress, |_| {}).await
    }

    /// [`suite`](Self::suite), calling `on_report` with each item's report
    /// as it completes (the CLI writes them to `--out-dir`).
    pub async fn suite_with(
        &self,
        req: SuiteRequest,
        progress: SuiteProgressSink,
        mut on_report: impl FnMut(&RunReport) + Send,
    ) -> SuiteResult {
        let mut result = SuiteResult {
            suite_id: req.suite_id.clone(),
            run_ids: Vec::new(),
            failed: 0,
            cancelled: false,
        };
        let fail_early = |e: AppError| {
            let mut run = Progress::new(
                req.suite_id.clone(),
                Some(req.suite_id.clone()),
                0,
                Phase::Failed,
            );
            run.message = Some(e.message);
            progress(SuiteProgress {
                suite_id: req.suite_id.clone(),
                index: 0,
                total: 0,
                item_label: req.profile.clone(),
                run,
            });
        };
        if let Err(e) = validate_run_id(&req.suite_id) {
            fail_early(e);
            result.failed = 1;
            return result;
        }
        let items = match suites::resolve(&req.profile, &req.overrides) {
            Ok(items) => items,
            Err(e) => {
                fail_early(e);
                result.failed = 1;
                return result;
            }
        };
        let cancel = {
            let mut suites = lock(&self.core.suites);
            if suites.contains_key(&req.suite_id) {
                drop(suites);
                fail_early(AppError::invalid_params(format!(
                    "suite {} is already running",
                    req.suite_id
                )));
                result.failed = 1;
                return result;
            }
            let t = CancellationToken::new();
            suites.insert(req.suite_id.clone(), t.clone());
            t
        };
        let total = items.len() as u32;
        for (i, params) in items.into_iter().enumerate() {
            if cancel.is_cancelled() {
                result.cancelled = true;
                break;
            }
            if i > 0 {
                tokio::select! {
                    _ = cancel.cancelled() => {
                        result.cancelled = true;
                        break;
                    }
                    _ = tokio::time::sleep(Duration::from_millis(suites::COOLDOWN_MS)) => {}
                }
            }
            let label = params.label();
            let sink: ProgressSink = {
                let progress = progress.clone();
                let suite_id = req.suite_id.clone();
                let label = label.clone();
                Arc::new(move |run: Progress| {
                    progress(SuiteProgress {
                        suite_id: suite_id.clone(),
                        index: i as u32,
                        total,
                        item_label: label.clone(),
                        run,
                    })
                })
            };
            let run_req = RunRequest {
                run_id: uuid::Uuid::new_v4().to_string(),
                peer: req.peer.clone(),
                params,
                suite_id: Some(req.suite_id.clone()),
            };
            let slot = SuiteSlot {
                profile: req.profile.clone(),
                index: i as u32,
                total,
            };
            let report = self.run_with(run_req, sink, Some((&cancel, slot))).await;
            result.run_ids.push(report.run_id.clone());
            if report.status != RunStatus::Ok {
                result.failed += 1;
            }
            on_report(&report);
            if report.status == RunStatus::Cancelled && cancel.is_cancelled() {
                result.cancelled = true;
                break;
            }
        }
        lock(&self.core.suites).remove(&req.suite_id);
        result
    }
}

fn phase_name(p: Phase) -> &'static str {
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

/// Total CPU (process + sidecar, both ends) per MB of goodput.
fn cpu_per_mb(r: &Resources, goodput_bytes: Option<u64>, notes: &mut Vec<String>) -> Option<f64> {
    let mb = goodput_bytes.filter(|b| *b > 0)? as f64 / 1e6;
    let total = |s: &crate::types::ResourceStats| s.cpu_ms + s.sidecar_cpu_ms.unwrap_or(0.0);
    if let Some(c) = &r.combined {
        return Some(total(c) / mb);
    }
    let local = r.local.as_ref().map(total);
    let peer = r.peer.as_ref().map(total);
    match (local, peer) {
        (Some(l), Some(p)) => Some((l + p) / mb),
        (Some(l), None) => {
            notes.push("CPU per MB covers this side only".into());
            Some(l / mb)
        }
        (None, Some(p)) => {
            notes.push("CPU per MB covers the peer only".into());
            Some(p / mb)
        }
        (None, None) => None,
    }
}

fn env_fallback(tool: Tool, device_name: &str) -> NodeEnv {
    NodeEnv {
        device_name: device_name.to_string(),
        os: std::env::consts::OS.to_string(),
        arch: std::env::consts::ARCH.to_string(),
        logical_cpus: env::logical_cpus(),
        tool: tool.app_name().to_string(),
        tool_version: crate::TOOL_VERSION.to_string(),
        truffle_version: truffle_core::VERSION.to_string(),
        build_profile: crate::types::BuildProfile::current(),
        rustc: env::rustc_version(),
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

async fn resolve(live: &Live, query: &str, wait_ms: u64) -> Result<Peer, AppError> {
    let peer = live
        .node
        .peer(query, Some(wait_ms))
        .await
        .map_err(AppError::from)?
        .ok_or_else(|| {
            AppError::new(
                ErrorCode::PeerNotFound,
                format!(
                    "no truffle bench peer called “{query}” appeared within {} s",
                    wait_ms / 1000
                ),
            )
        })?;
    if !peer.online {
        return Err(AppError::new(
            ErrorCode::PeerOffline,
            format!("{} is offline", peer.display_name),
        ));
    }
    Ok(peer)
}

/// Take the exclusive `<state_dir>/hub.lock` (§5.4): `state-in-use` if held.
///
/// Retries briefly: a child process spawned at the same moment (a sidecar,
/// `git`) can hold a transient reference to a just-closed lock file.
async fn acquire_state_lock(state_dir: &Path) -> Result<fd_lock::RwLock<File>, AppError> {
    let mut attempt = 0;
    loop {
        match try_state_lock(state_dir) {
            Err(e) if e.code == ErrorCode::StateInUse && attempt < 4 => {
                attempt += 1;
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            other => return other,
        }
    }
}

fn try_state_lock(state_dir: &Path) -> Result<fd_lock::RwLock<File>, AppError> {
    std::fs::create_dir_all(state_dir)
        .map_err(|e| AppError::io(format!("create {}", state_dir.display()), &e))?;
    let path = state_dir.join("hub.lock");
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&path)
        .map_err(|e| AppError::io(format!("open {}", path.display()), &e))?;
    let mut lock = fd_lock::RwLock::new(file);
    match lock.try_write() {
        // The lock lives as long as the file stays open: keep the RwLock,
        // forget the guard. Dropping the RwLock closes the file and unlocks.
        Ok(guard) => std::mem::forget(guard),
        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
            return Err(AppError::new(
                ErrorCode::StateInUse,
                format!(
                    "another truffle bench is using {}; quit it, or use another profile",
                    state_dir.display()
                ),
            ))
        }
        Err(e) => return Err(AppError::io(format!("lock {}", path.display()), &e)),
    }
    Ok(lock)
}

// ---------------------------------------------------------------------------
// Pair: two hubs in one process
// ---------------------------------------------------------------------------

/// Two ephemeral hubs in one process (`truffle-bench pair`, the live PR
/// test): a per-run app id `truffle-benchpair-<6 hex>` so concurrent pairs
/// never see each other, each hub with its own temporary state and data dirs
/// (deleted when the pair is dropped), topology `same-process`.
///
/// The app id deliberately does not start with `truffle-bench-`: discovery
/// matches the hostname prefix `truffle-<app id>-`, so an id like
/// `truffle-bench-p…` would make every pair node a peer of every regular
/// `truffle-bench` hub on the tailnet.
pub struct Pair {
    /// The hub that runs workloads.
    pub initiator: Arc<Hub>,
    /// The hub that answers them.
    pub responder: Arc<Hub>,
    /// The per-run app id.
    pub app_id: String,
    responder_id: String,
    _dirs: tempfile::TempDir,
}

impl std::fmt::Debug for Pair {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pair")
            .field("app_id", &self.app_id)
            .field("responder_id", &self.responder_id)
            .finish()
    }
}

/// A fresh hello from `hub` to its pair sibling, tried up to
/// [`PAIR_HELLO_ATTEMPTS`] times. Each attempt already re-sends within its
/// 30 s; a further attempt covers a WebSocket dial that stayed stuck (the
/// session refuses new dials while one is in flight, "connection already in
/// progress") past an attempt's deadline.
async fn pair_hello(hub: &Hub, peer_id: &str) -> Result<PeerHello, AppError> {
    let mut attempt = 1;
    loop {
        match hub.hello_peer(peer_id).await {
            Ok(h) => return Ok(h),
            Err(e) if attempt < PAIR_HELLO_ATTEMPTS && e.code != ErrorCode::NotRunning => {
                tracing::warn!(attempt, error = %e, "pair: hello failed; retrying");
                attempt += 1;
                tokio::time::sleep(PAIR_HELLO_RETRY).await;
            }
            Err(e) => return Err(e),
        }
    }
}

impl Pair {
    /// Start both hubs with `auth_key` (tagged, ephemeral), wait up to 60 s
    /// for each to list the other, and exchange `hello` both ways (each
    /// direction tried up to three times).
    pub async fn start(
        auth_key: &str,
        tool: Tool,
        sidecar: Option<PathBuf>,
    ) -> Result<Pair, AppError> {
        let tag = crate::dataplane::random_run_tag()?;
        let hex6 = format!("{:06x}", tag & 0xff_ffff);
        let app_id = format!("{PAIR_APP_PREFIX}{hex6}");
        let dirs = tempfile::Builder::new()
            .prefix("truffle-bench-pair-")
            .tempdir()
            .map_err(|e| AppError::io("create the pair's temp dir", &e))?;
        let cfg = |side: &str| {
            let base = dirs.path().join(side);
            let mut c = HubConfig::defaults(tool, None, base.join("data"))
                .with_device_name(side)
                .with_auth_key(auth_key);
            c.state_dir = base.join("state");
            c.ephemeral = true;
            c.app_id = app_id.clone();
            c.sidecar_path = sidecar.clone();
            c
        };
        let initiator = Hub::new(cfg("alpha"));
        let responder = Hub::new(cfg("beta"));
        initiator.set_same_process_sibling(&responder);
        responder.set_same_process_sibling(&initiator);
        let (a, b) = tokio::join!(initiator.start(), responder.start());
        if let Err(e) = a.and(b) {
            tokio::join!(initiator.stop(), responder.stop());
            return Err(e.context("pair"));
        }
        let ready = async {
            let a_id = initiator
                .local()
                .map(|l| l.tailscale_id)
                .ok_or_else(AppError::not_running)?;
            let b_id = responder
                .local()
                .map(|l| l.tailscale_id)
                .ok_or_else(AppError::not_running)?;
            let (pa, pb) = tokio::join!(
                initiator.resolve_peer(&b_id, PAIR_SIBLING_WAIT_MS),
                responder.resolve_peer(&a_id, PAIR_SIBLING_WAIT_MS)
            );
            pa.map_err(|e| e.context("the initiator never saw its sibling"))?;
            pb.map_err(|e| e.context("the responder never saw its sibling"))?;
            // One direction at a time: both hubs' hello caches are dialling
            // each other at this moment too, and two more simultaneous dials
            // only add collisions.
            pair_hello(&initiator, &b_id).await?;
            pair_hello(&responder, &a_id).await?;
            Ok::<String, AppError>(b_id)
        }
        .await;
        match ready {
            Ok(responder_id) => Ok(Pair {
                initiator,
                responder,
                app_id,
                responder_id,
                _dirs: dirs,
            }),
            Err(e) => {
                tokio::join!(initiator.stop(), responder.stop());
                Err(e.context("pair"))
            }
        }
    }

    /// The responder's tailscale id (the peer reference for runs).
    pub fn responder_ref(&self) -> &str {
        &self.responder_id
    }

    /// Run one workload from the initiator against the responder.
    pub async fn run(
        &self,
        run_id: String,
        params: WorkloadParams,
        progress: ProgressSink,
    ) -> RunReport {
        self.initiator
            .run(
                RunRequest {
                    run_id,
                    peer: self.responder_id.clone(),
                    params,
                    suite_id: None,
                },
                progress,
            )
            .await
    }

    /// Stop both hubs (the temp dirs go when the pair is dropped).
    pub async fn stop(&self) {
        tokio::join!(self.initiator.stop(), self.responder.stop());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::now_ns;

    fn cfg(dir: &Path) -> HubConfig {
        let mut c =
            HubConfig::defaults(Tool::Cli, None, dir.join("data")).with_device_name("unit-test");
        c.state_dir = dir.join("state");
        c
    }

    /// The app runs these on Tauri's multi-threaded runtime.
    #[allow(dead_code)]
    fn hub_futures_are_send(hub: Arc<Hub>) {
        fn send<T: Send>(_: &T) {}
        send(&hub.start());
        send(&hub.stop());
        send(&hub.run(
            RunRequest {
                run_id: "x".into(),
                peer: "y".into(),
                params: WorkloadParams::default(),
                suite_id: None,
            },
            Arc::new(|_| {}),
        ));
        send(&hub.suite(
            SuiteRequest {
                suite_id: "s".into(),
                profile: "smoke".into(),
                peer: "y".into(),
                overrides: vec![],
            },
            Arc::new(|_| {}),
        ));
        send(&hub.probe("y"));
        send(&hub.sign_out());
        send(&Pair::start("k", Tool::Cli, None));
        fn sync<T: Send + Sync>() {}
        sync::<Hub>();
    }

    #[tokio::test]
    async fn state_lock_is_exclusive() {
        let dir = tempfile::tempdir().unwrap();
        let first = acquire_state_lock(dir.path()).await.unwrap();
        let err = acquire_state_lock(dir.path()).await.unwrap_err();
        assert_eq!(err.code, ErrorCode::StateInUse);
        drop(first);
        acquire_state_lock(dir.path()).await.unwrap();
    }

    #[test]
    fn offers_reserve_space_and_go_one_at_a_time_per_run() {
        let gib = 1u64 << 30;
        let (a, b) = (PathBuf::from("/d/tmp/in-a"), PathBuf::from("/d/tmp/in-b"));
        let mut st = RouterState::default();
        let accept = |st: &mut RouterState, token: &str, size: u64, dir: &Path| {
            st.admit(size, dir, Some(5 * gib))?;
            st.reserved += size;
            let (tx, _rx) = oneshot::channel();
            st.tokens.insert(
                token.into(),
                InFlight {
                    id: 1,
                    tx,
                    size,
                    dir: dir.to_path_buf(),
                },
            );
            Ok::<_, String>(())
        };
        accept(&mut st, "t1", 2 * gib, &a).unwrap();
        // The same run's next file waits for this one.
        assert!(accept(&mut st, "t2", gib, &a).is_err());
        // 5 GiB free, 2 GiB reserved: a 2 GiB file needs 4 GiB, only 3 left.
        assert!(accept(&mut st, "t3", 2 * gib, &b).is_err());
        accept(&mut st, "t4", gib, &b).unwrap();
        assert_eq!(st.reserved, 3 * gib);
        assert!(st.finish("t1").is_some());
        assert!(st.finish("t1").is_none());
        assert_eq!(st.reserved, gib);
        accept(&mut st, "t5", gib, &a).unwrap();
        // Unknown free space: one at a time per run still holds.
        assert!(st.admit(gib, &a, None).is_err());
        assert!(st.admit(gib, Path::new("/d/tmp/in-c"), None).is_ok());
    }

    #[test]
    fn pair_app_ids_are_not_visible_to_regular_hubs() {
        let pair_id = format!("{PAIR_APP_PREFIX}abc123");
        assert!(truffle_core::AppId::parse(&pair_id).is_ok());
        // Regular hubs discover hostnames starting with `truffle-truffle-bench-`.
        let regular = format!("truffle-{}-", crate::APP_ID);
        let pair_host = truffle_core::tailscale_hostname(
            &truffle_core::AppId::parse(&pair_id).unwrap(),
            &truffle_core::DeviceName::parse("alpha"),
        );
        assert!(!pair_host.starts_with(&regular), "{pair_host}");
    }

    #[tokio::test]
    async fn idle_hub_reports_and_stops() {
        let dir = tempfile::tempdir().unwrap();
        let hub = Hub::new(cfg(dir.path()));
        let mut events = hub.subscribe();
        assert_eq!(hub.state().state, NodeStatus::Idle);
        assert!(hub
            .state()
            .tailscale_hostname
            .unwrap()
            .starts_with("truffle-truffle-bench-"));
        assert!(hub.peers().await.is_empty());
        assert!(hub.local().is_none());
        assert!(!hub.cancel("nope"));
        assert!(hub.active_runs().is_empty());
        hub.stop().await;
        let report = hub
            .run(
                RunRequest {
                    run_id: "r-1".into(),
                    peer: "studio".into(),
                    params: WorkloadParams::default(),
                    suite_id: None,
                },
                Arc::new(|_| {}),
            )
            .await;
        assert_eq!(report.status, RunStatus::Failed);
        assert_eq!(report.error_code, Some(ErrorCode::NotRunning));
        assert_eq!(report.compare_key.len(), 16);
        assert_eq!(hub.store().list().len(), 1, "failed runs are stored too");
        assert!(
            events.try_recv().is_err(),
            "stop on an idle hub emits nothing"
        );
        let bad = hub
            .run(
                RunRequest {
                    run_id: "bad id/..".into(),
                    peer: "studio".into(),
                    params: WorkloadParams::default(),
                    suite_id: None,
                },
                Arc::new(|_| {}),
            )
            .await;
        assert_eq!(bad.error_code, Some(ErrorCode::InvalidParams));
        assert_eq!(
            hub.store().list().len(),
            1,
            "malformed requests are not stored"
        );
    }

    #[tokio::test]
    async fn start_without_sidecar_is_an_error_state() {
        let dir = tempfile::tempdir().unwrap();
        let mut c = cfg(dir.path());
        c.sidecar_path = Some(dir.path().join("missing-sidecar"));
        let hub = Hub::new(c);
        let mut events = hub.subscribe();
        let err = hub.start().await.unwrap_err();
        assert_eq!(err.code, ErrorCode::SidecarNotFound);
        assert_eq!(hub.state().state, NodeStatus::Error);
        match events.recv().await.unwrap() {
            HubEvent::Node(st) => assert_eq!(st.state, NodeStatus::Error),
            other => panic!("{other:?}"),
        }
        // The state lock was released: a second hub on the same dir can try.
        let mut c2 = cfg(dir.path());
        c2.sidecar_path = Some(dir.path().join("missing-sidecar"));
        let hub2 = Hub::new(c2);
        assert_eq!(
            hub2.start().await.unwrap_err().code,
            ErrorCode::SidecarNotFound
        );
    }

    #[tokio::test]
    async fn suite_with_unknown_profile_fails_cleanly() {
        let dir = tempfile::tempdir().unwrap();
        let hub = Hub::new(cfg(dir.path()));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let s2 = seen.clone();
        let res = hub
            .suite(
                SuiteRequest {
                    suite_id: "s-1".into(),
                    profile: "nope".into(),
                    peer: "studio".into(),
                    overrides: vec![],
                },
                Arc::new(move |p| s2.lock().unwrap().push(p)),
            )
            .await;
        assert_eq!(res.failed, 1);
        assert!(res.run_ids.is_empty());
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].run.phase, Phase::Failed);
    }

    #[tokio::test(start_paused = true)]
    async fn suite_on_a_stopped_hub_runs_every_item_and_counts_failures() {
        let dir = tempfile::tempdir().unwrap();
        let hub = Hub::new(cfg(dir.path()));
        let mut reports = Vec::new();
        // While an item runs, `active_runs` says where it sits in its suite.
        let seen = Arc::new(Mutex::new(Vec::new()));
        let (h2, s2) = (hub.clone(), seen.clone());
        let sink: SuiteProgressSink = Arc::new(move |_| {
            for a in h2.active_runs() {
                s2.lock()
                    .unwrap()
                    .push((a.suite_profile, a.suite_index, a.suite_total));
            }
        });
        let res = hub
            .suite_with(
                SuiteRequest {
                    suite_id: "s-2".into(),
                    profile: "smoke".into(),
                    peer: "studio".into(),
                    overrides: vec![],
                },
                sink,
                |r| reports.push(r.clone()),
            )
            .await;
        let n = suites::profile("smoke").unwrap().items.len();
        let seen = seen.lock().unwrap();
        assert!(!seen.is_empty());
        assert!(seen
            .iter()
            .all(|(p, _, t)| p.as_deref() == Some("smoke") && *t == Some(n as u32)));
        assert_eq!(seen.last().unwrap().1, Some(n as u32 - 1));
        assert_eq!(res.run_ids.len(), n);
        assert_eq!(res.failed as usize, n);
        assert!(!res.cancelled);
        assert!(reports
            .iter()
            .all(|r| r.suite_id.as_deref() == Some("s-2") && r.status == RunStatus::Failed));
    }

    /// End-to-end control protocol on the real tailnet, with a test double in
    /// place of `egress`: prepare → clock → start → tick → finish, rule 3
    /// (busy), cancel after a failure, the responder's refusals (a workload
    /// it withholds, path, bad run ids, a failing prepare, tombstones,
    /// unknown runs), the report hook and live `loaded_rtt_ms`. Prints
    /// `[skip]` without an auth key.
    mod live {
        use super::*;
        use crate::control::{
            msg_type, new_request_id, request, CancelMsg, FinishReq, PrepareReq, StartReq,
            PREPARE_TIMEOUT,
        };
        use crate::params::{EgressParams, OneWayDirection};
        use crate::types::SummaryItem;
        use crate::workloads::common::{cpu_per_mb_item, loaded_rtt_item};
        use crate::workloads::{test_double, ResponderCtx, ResponderRun};
        use futures::future::BoxFuture;
        use serde_json::json;

        fn key() -> Option<String> {
            let _ = dotenvy::dotenv();
            [
                "TRUFFLE_BENCH_AUTH_KEY",
                "TRUFFLE_TEST_AUTHKEY",
                "TS_AUTHKEY",
            ]
            .iter()
            .find_map(|v| std::env::var(v).ok().filter(|k| !k.trim().is_empty()))
        }

        #[derive(Serialize, Deserialize)]
        struct Client {
            greeting: String,
        }
        #[derive(Serialize, Deserialize)]
        struct Setup {
            echo: String,
            run_tag_hex: String,
            in_dir_exists: bool,
        }
        #[derive(Serialize, Deserialize)]
        struct Info {
            sent: u64,
        }
        #[derive(Serialize, Deserialize)]
        struct Stats {
            sent_echo: u64,
            started: bool,
            starts: u32,
            foreign: u64,
        }

        /// Cancels of the double that ran to completion (past a real await).
        static CANCELS_DONE: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

        struct EchoRun {
            ctx: ResponderCtx,
            started: bool,
            starts: u32,
            ticker: Option<tokio::task::JoinHandle<()>>,
        }

        impl ResponderRun for EchoRun {
            fn start(&mut self) -> BoxFuture<'_, Result<(), AppError>> {
                Box::pin(async move {
                    self.started = true;
                    self.starts += 1;
                    let r = self.ctx.clone();
                    self.ticker = Some(tokio::spawn(async move {
                        loop {
                            r.tick([("goodput_mbps", 42.0)]).await;
                            tokio::time::sleep(Duration::from_millis(250)).await;
                        }
                    }));
                    Ok(())
                })
            }
            fn finish(self: Box<Self>, info: Value) -> BoxFuture<'static, Result<Value, AppError>> {
                Box::pin(async move {
                    if let Some(t) = &self.ticker {
                        t.abort();
                    }
                    let info: Info = serde_json::from_value(info)
                        .map_err(|e| AppError::invalid_params(e.to_string()))?;
                    let now = now_ns();
                    self.ctx
                        .set_resource_window(now - 1_000_000_000, now, Some(1_000_000));
                    Ok(json!(Stats {
                        sent_echo: info.sent,
                        started: self.started,
                        starts: self.starts,
                        foreign: self.ctx.foreign_dropped(),
                    }))
                })
            }
            fn cancel(self: Box<Self>) -> BoxFuture<'static, ()> {
                Box::pin(async move {
                    if let Some(t) = &self.ticker {
                        t.abort();
                    }
                    // Like a real teardown (a QUIC accept join, a proxy
                    // removal's reply): a real await before it completes.
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    CANCELS_DONE.fetch_add(1, Ordering::SeqCst);
                })
            }
        }

        fn prepare_double(
            r: ResponderCtx,
            client: Value,
        ) -> BoxFuture<'static, test_double::PrepareResult> {
            Box::pin(async move {
                let c: Client = serde_json::from_value(client)
                    .map_err(|e| AppError::invalid_params(e.to_string()))?;
                if c.greeting == "fail" {
                    return Err(AppError::invalid_params("told to fail"));
                }
                let dir = r.in_dir()?;
                let setup = Setup {
                    echo: c.greeting,
                    run_tag_hex: r.run_tag_hex(),
                    in_dir_exists: dir.is_dir(),
                };
                let run: Box<dyn ResponderRun> = Box::new(EchoRun {
                    ctx: r,
                    started: false,
                    starts: 0,
                    ticker: None,
                });
                Ok((serde_json::to_value(setup).unwrap(), run))
            })
        }

        fn run_double<'a, 'b>(
            ctx: &'a mut RunCtx<'b>,
        ) -> BoxFuture<'a, Result<WorkloadOutput, AppError>> {
            Box::pin(async move {
                let fail_after_start = matches!(
                    ctx.params(),
                    WorkloadParams::Egress(EgressParams {
                        direction: OneWayDirection::Download,
                        ..
                    })
                );
                let setup: Setup = ctx
                    .prepare(&Client {
                        greeting: "hi".into(),
                    })
                    .await?;
                if setup.echo != "hi"
                    || setup.run_tag_hex != ctx.run_tag_hex()?
                    || !setup.in_dir_exists
                {
                    return Err(AppError::internal("double: bad setup echo"));
                }
                let out = ctx.out_dir()?;
                ctx.clock().await?;
                // Rule 3: the responder already serves this run, so another
                // prepare (even from the same initiator) is busy.
                let busy = request(
                    &ctx.node,
                    ctx.peer_id(),
                    &PrepareReq {
                        id: new_request_id(),
                        run_id: "second-run".into(),
                        params: ctx.params().clone(),
                        client: json!({"greeting": "x"}),
                    },
                    PREPARE_TIMEOUT,
                )
                .await
                .err()
                .map(|e| e.code);
                ctx.start().await?;
                // A duplicate start (another id) is acknowledged without effect.
                request(
                    &ctx.node,
                    ctx.peer_id(),
                    &StartReq {
                        id: new_request_id(),
                        run_id: ctx.run_id.clone(),
                    },
                    PREPARE_TIMEOUT,
                )
                .await
                .map_err(|e| e.context("double: duplicate start"))?;
                ctx.phase(Phase::Running);
                let tick = tokio::time::timeout(Duration::from_secs(5), ctx.next_tick())
                    .await
                    .ok()
                    .flatten()
                    .ok_or_else(|| AppError::timeout("double: no tick"))?;
                let window_start = now_ns();
                tokio::time::sleep(Duration::from_millis(1500)).await;
                if fail_after_start {
                    return Err(AppError::internal("double: failing after start on purpose"));
                }
                let stats: Stats = ctx.finish(&Info { sent: 7 }, Duration::ZERO).await?;
                ctx.clock().await?;
                Ok(WorkloadOutput {
                    summary: vec![
                        SummaryItem::new(
                            "goodput_mbps",
                            "Goodput",
                            tick.values.get("goodput_mbps").copied(),
                            "Mb/s",
                            crate::types::Better::Higher,
                        ),
                        cpu_per_mb_item(),
                        loaded_rtt_item(),
                    ],
                    results: json!({
                        "sent_echo": stats.sent_echo,
                        "started": stats.started,
                        "starts": stats.starts,
                        "foreign": stats.foreign,
                        "out_dir": out.display().to_string(),
                        "out_dir_exists": out.is_dir(),
                        "busy_code": busy,
                    }),
                    series: vec![],
                    notes: vec![],
                    window_ns: Some((window_start, now_ns())),
                    goodput_bytes: Some(1_000_000),
                })
            })
        }

        fn egress(direction: OneWayDirection) -> WorkloadParams {
            WorkloadParams::Egress(EgressParams {
                direction,
                duration_secs: 3,
                warmup_secs: 1,
                ..EgressParams::default()
            })
        }

        async fn expect_refusal<R: crate::control::ControlRequest>(
            pair: &Pair,
            req: &R,
            code: ErrorCode,
        ) {
            let node = pair.initiator.node().unwrap();
            let err = request(&node, pair.responder_ref(), req, PREPARE_TIMEOUT)
                .await
                .err()
                .unwrap_or_else(|| panic!("{} was accepted", R::TYPE));
            assert_eq!(err.code, code, "{}: {}", R::TYPE, err.message);
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn control_protocol_end_to_end() {
            let Some(key) = key() else {
                eprintln!("[skip] control_protocol_end_to_end: no auth key");
                return;
            };
            struct Uninstall;
            impl Drop for Uninstall {
                fn drop(&mut self) {
                    test_double::clear();
                }
            }
            let _serial = test_double::LIVE_PAIR_LOCK.lock().await;
            test_double::install(Workload::Egress, run_double, prepare_double);
            let _uninstall = Uninstall;
            let pair = Pair::start(&key, Tool::Cli, None).await.expect("pair");
            let mut remote = pair.responder.subscribe();
            let node = pair.initiator.node().unwrap();
            let peer = pair.responder_ref().to_string();

            // hello: admitted (same key, same login), fingerprint shared.
            let hello = pair.initiator.hello_peer(&peer).await.unwrap();
            assert!(hello.accepts_runs_from_you);
            assert!(hello.host_fingerprint.is_some());
            assert!(hello.workloads.iter().any(|w| w == "egress"));
            // clock in one process: |offset| ≤ uncertainty.
            let off = control::estimate_clock(&node, &peer).await.unwrap();
            assert!(
                off.offset_ns.unsigned_abs() <= off.uncertainty_ns,
                "{off:?}"
            );

            // The responder is on this machine: same_host from the fingerprints.
            let peers = pair.initiator.peers().await;
            let sibling = peers.iter().find(|p| p.id == peer).expect("listed");
            let expect_same_host = (!env::host_fingerprint().weak).then_some(true);
            assert_eq!(sibling.same_host, expect_same_host, "{sibling:?}");
            assert!(sibling.same_login);

            // A full run through the double, with a report hook and the
            // live samples collected.
            pair.initiator
                .set_report_hook(Arc::new(|r| r.local.app_foreground_pct = Some(42.0)));
            let samples = Arc::new(Mutex::new(Vec::<String>::new()));
            let s2 = samples.clone();
            let report = pair
                .run(
                    "double-ok".into(),
                    egress(OneWayDirection::Upload),
                    Arc::new(move |p| {
                        s2.lock()
                            .unwrap()
                            .extend(p.samples.iter().map(|s| s.key.clone()))
                    }),
                )
                .await;
            pair.initiator.clear_report_hook();
            assert_eq!(report.local.app_foreground_pct, Some(42.0));
            assert_eq!(
                pair.initiator
                    .store()
                    .get("double-ok")
                    .unwrap()
                    .local
                    .app_foreground_pct,
                Some(42.0)
            );
            assert!(
                samples
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|k| k == crate::workloads::common::KEY_LOADED_RTT),
                "no live loaded_rtt_ms in {:?}",
                samples.lock().unwrap()
            );
            assert_eq!(
                report.status,
                RunStatus::Ok,
                "{:?} {:?} {:?}",
                report.error_code,
                report.error,
                report.notes
            );
            assert_eq!(report.results["sent_echo"], 7);
            assert_eq!(report.results["started"], true);
            assert_eq!(report.results["starts"], 1, "start is idempotent");
            assert_eq!(report.results["out_dir_exists"], true);
            assert_eq!(report.results["busy_code"], json!("busy"));
            assert_eq!(report.summary_value("goodput_mbps"), Some(42.0));
            assert!(
                report.summary_value("cpu_ms_per_mb").is_some(),
                "{:?}",
                report.resources
            );
            assert!(report.summary_value("loaded_rtt_p50_ms").is_some());
            assert!(!report.network.path_timeline.is_empty());
            let clock = report.network.clock.expect("clock");
            assert!(clock.after.is_some());
            assert!(clock.before.offset_ns.unsigned_abs() <= clock.before.uncertainty_ns);
            assert!(report.resources.combined.is_some());
            assert!(report.network.path_bytes.is_some());
            assert_eq!(report.params, egress(OneWayDirection::Upload));
            assert!(pair.initiator.store().contains("double-ok"));
            // The out dir is gone; the responder freed its slot.
            let out_dir = report.results["out_dir"].as_str().unwrap();
            assert!(!Path::new(out_dir).exists(), "{out_dir} was left behind");
            assert_eq!(pair.responder.core.slots.busy(), None);
            assert_eq!(pair.initiator.core.slots.busy(), None);
            // Calendar fields are wall-clock times.
            let started = chrono::DateTime::parse_from_rfc3339(&report.started_at).unwrap();
            let age_ms = chrono::Utc::now().timestamp_millis() - started.timestamp_millis();
            assert!(
                (0..120_000).contains(&age_ms),
                "started_at {} is {age_ms} ms ago",
                report.started_at
            );
            // A duplicate finish (another id, e.g. a retry after a lost
            // reply) gets the first finish's outcome, not "no run …".
            let again = request(
                &node,
                &peer,
                &FinishReq {
                    id: new_request_id(),
                    run_id: "double-ok".into(),
                    info: json!({"sent": 9}),
                },
                PREPARE_TIMEOUT,
            )
            .await
            .expect("duplicate finish");
            assert_eq!(again.ack.stats["sent_echo"], 7);

            // A failure after start: the hub sends cancel, the responder tears down.
            let failed = pair
                .run(
                    "double-fail".into(),
                    egress(OneWayDirection::Download),
                    Arc::new(|_| {}),
                )
                .await;
            assert_eq!(failed.status, RunStatus::Failed);
            assert!(
                failed.notes.iter().any(|n| n.contains("running phase")),
                "{:?}",
                failed.notes
            );
            let mut states = Vec::new();
            let deadline = Instant::now() + Duration::from_secs(5);
            while states.len() < 4 && Instant::now() < deadline {
                if let Ok(Ok(HubEvent::RemoteRun { run_id, state, .. })) =
                    tokio::time::timeout(Duration::from_millis(500), remote.recv()).await
                {
                    states.push((run_id, state));
                }
            }
            assert_eq!(
                states,
                vec![
                    ("double-ok".to_string(), RemoteRunState::Prepared),
                    ("double-ok".to_string(), RemoteRunState::Finished),
                    ("double-fail".to_string(), RemoteRunState::Prepared),
                    ("double-fail".to_string(), RemoteRunState::Cancelled),
                ]
            );
            assert_eq!(pair.responder.core.slots.busy(), None);

            // Refusals.
            let prep = |run_id: &str, params: WorkloadParams, client: Value| PrepareReq {
                id: new_request_id(),
                run_id: run_id.into(),
                params,
                client,
            };
            // A workload the responder does not offer (every workload is
            // implemented, so withhold one on this responder only).
            lock(&pair.responder.core.withheld).insert(Workload::Tcp);
            expect_refusal(
                &pair,
                &prep("r-tcp", WorkloadParams::Tcp(Default::default()), json!({})),
                ErrorCode::UnsupportedWorkload,
            )
            .await;
            lock(&pair.responder.core.withheld).clear();
            expect_refusal(
                &pair,
                &prep("r-path", WorkloadParams::default(), json!({})),
                ErrorCode::InvalidParams,
            )
            .await;
            expect_refusal(
                &pair,
                &prep(
                    "bad/id",
                    egress(OneWayDirection::Upload),
                    json!({"greeting": "x"}),
                ),
                ErrorCode::InvalidParams,
            )
            .await;
            expect_refusal(
                &pair,
                &prep(
                    "r-fail",
                    egress(OneWayDirection::Upload),
                    json!({"greeting": "fail"}),
                ),
                ErrorCode::InvalidParams,
            )
            .await;
            assert_eq!(
                pair.responder.core.slots.busy(),
                None,
                "a failed prepare frees the slot"
            );
            // A cancel before prepare leaves a tombstone.
            control::notify(
                &node,
                &peer,
                msg_type::CANCEL,
                &CancelMsg {
                    run_id: "ghost-run".into(),
                },
                Duration::from_secs(5),
            )
            .await
            .unwrap();
            tokio::time::sleep(Duration::from_millis(500)).await;
            expect_refusal(
                &pair,
                &prep(
                    "ghost-run",
                    egress(OneWayDirection::Upload),
                    json!({"greeting": "x"}),
                ),
                ErrorCode::Cancelled,
            )
            .await;
            expect_refusal(
                &pair,
                &FinishReq {
                    id: new_request_id(),
                    run_id: "nope".into(),
                    info: json!({}),
                },
                ErrorCode::InvalidParams,
            )
            .await;
            expect_refusal(
                &pair,
                &StartReq {
                    id: new_request_id(),
                    run_id: "nope".into(),
                },
                ErrorCode::InvalidParams,
            )
            .await;

            // The watchdog: an initiator that prepares and vanishes. The run
            // is torn down completely (the workload's cancel runs past its
            // awaits, the in dir is removed, the slot freed) and announced
            // as expired.
            *lock(&pair.responder.core.watchdog) = Some(Duration::from_secs(2));
            let cancels = CANCELS_DONE.load(Ordering::SeqCst);
            let prepared = request(
                &node,
                &peer,
                &prep(
                    "wd-run",
                    egress(OneWayDirection::Upload),
                    json!({"greeting": "x"}),
                ),
                PREPARE_TIMEOUT,
            )
            .await
            .expect("prepare wd-run");
            let in_dir = control::in_dir(&pair.responder.core.cfg.data_dir, prepared.ack.run_tag);
            assert!(in_dir.is_dir());
            assert!(pair.responder.core.slots.busy().is_some());
            let mut expired = false;
            let deadline = Instant::now() + Duration::from_secs(15);
            while !expired && Instant::now() < deadline {
                if let Ok(Ok(HubEvent::RemoteRun { run_id, state, .. })) =
                    tokio::time::timeout(Duration::from_millis(500), remote.recv()).await
                {
                    expired = run_id == "wd-run" && state == RemoteRunState::Expired;
                }
            }
            assert!(expired, "no expired event for wd-run");
            assert_eq!(CANCELS_DONE.load(Ordering::SeqCst), cancels + 1);
            assert!(!in_dir.exists(), "{} was left behind", in_dir.display());
            assert_eq!(pair.responder.core.slots.busy(), None);
            *lock(&pair.responder.core.watchdog) = None;

            pair.stop().await;
        }
    }
}
