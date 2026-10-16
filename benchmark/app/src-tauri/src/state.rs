//! App state (RFC 027 §11.1, §11.2): the profile and its data directory, the
//! persisted [`Settings`], the one [`Hub`], and the trackers that annotate
//! runs (the macOS process activity and `app_foreground_pct`).
//!
//! [`AppState`] knows nothing about Tauri windows: events leave through an
//! [`Emit`] closure, so the command layer can be tested on the mock runtime.

use std::collections::{HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use truffle_bench::config::{resolve_sidecar, state_dir_for};
use truffle_bench::hub::ReportHook;
use truffle_bench::{
    env, AppError, AppInfo, BuildProfile, ErrorCode, Hub, HubConfig, HubEvent, ImportResult,
    NodeState, NodeStatus, ProgressSink, RemoteRunState, RunReport, RunRequest, Settings,
    SuiteProgressSink, SuiteRequest, SuiteResult, Theme, Tool,
};

/// Delivers a hub event to the UI (`bench://event` in the app).
pub type Emit = Arc<dyn Fn(&HubEvent) + Send + Sync>;

/// The settings file inside the data directory.
pub const SETTINGS_FILE: &str = "settings.json";

/// Debug builds use this profile unless told otherwise, so `tauri dev` never
/// shares state with the installed app (§11.1).
pub const DEBUG_PROFILE: &str = "dev";

/// What §11.1 says when another process holds this profile's state lock.
pub const STATE_IN_USE_MESSAGE: &str =
    "Another truffle bench is using this profile. Quit it, or start this one with --profile <name>.";

/// How long `hub_start` waits for the hub's first state change before it
/// answers, so the reply is `starting` (or `error`) rather than a stale `idle`.
const START_REPLY_WAIT: Duration = Duration::from_millis(500);

/// Focus transitions kept for `app_foreground_pct`.
const FOREGROUND_SPANS: usize = 4096;

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

// ---------------------------------------------------------------------------
// Launch: profile and test sign-in
// ---------------------------------------------------------------------------

/// What the process was started with.
#[derive(Clone, Default)]
pub struct Launch {
    /// `--profile <name>` / `TRUFFLE_BENCH_PROFILE`; `dev` in debug builds.
    pub profile: Option<String>,
    /// `TRUFFLE_BENCH_AUTH_KEY` (dev and test only). Never logged.
    pub auth_key: Option<String>,
    /// Debug builds only: `TRUFFLE_BENCH_SELFTEST=<peer>` runs `path` once.
    pub selftest: Option<String>,
}

impl std::fmt::Debug for Launch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Launch")
            .field("profile", &self.profile)
            .field("auth_key", &self.auth_key.as_ref().map(|_| "[REDACTED]"))
            .field("selftest", &self.selftest)
            .finish()
    }
}

impl Launch {
    /// Read argv and the environment.
    pub fn from_env() -> Result<Self, String> {
        let profile = profile_from(
            std::env::args().skip(1),
            std::env::var("TRUFFLE_BENCH_PROFILE").ok(),
            cfg!(debug_assertions),
        )?;
        let auth_key = std::env::var("TRUFFLE_BENCH_AUTH_KEY")
            .ok()
            .map(|k| k.trim().to_string())
            .filter(|k| !k.is_empty());
        let selftest = if cfg!(debug_assertions) {
            std::env::var("TRUFFLE_BENCH_SELFTEST")
                .ok()
                .map(|p| p.trim().to_string())
                .filter(|p| !p.is_empty())
        } else {
            None
        };
        Ok(Self {
            profile,
            auth_key,
            selftest,
        })
    }
}

/// The profile: `--profile <name>` or `--profile=<name>` on the command line,
/// else the environment value, else `dev` when `debug`.
pub fn profile_from(
    args: impl IntoIterator<Item = String>,
    env_value: Option<String>,
    debug: bool,
) -> Result<Option<String>, String> {
    let mut found = None;
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        if arg == "--profile" {
            found = Some(args.next().ok_or("--profile needs a name")?);
        } else if let Some(v) = arg.strip_prefix("--profile=") {
            found = Some(v.to_string());
        }
    }
    let profile = found
        .or_else(|| {
            env_value
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
        })
        .or_else(|| debug.then(|| DEBUG_PROFILE.to_string()));
    if let Some(p) = &profile {
        validate_profile(p)?;
    }
    Ok(profile)
}

/// A profile names a directory and a device-name suffix: 1–32 letters,
/// digits, `-` or `_`.
pub fn validate_profile(p: &str) -> Result<(), String> {
    let ok = (1..=32).contains(&p.len())
        && p.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_');
    if ok {
        Ok(())
    } else {
        Err(format!(
            "the profile must be 1–32 letters, digits, - or _ (got “{p}”)"
        ))
    }
}

/// The app data directory for a profile: the Tauri app-data dir with
/// `-<profile>` appended (§5.3).
pub fn profile_data_dir(base: &Path, profile: Option<&str>) -> PathBuf {
    match profile {
        None => base.to_path_buf(),
        Some(p) => {
            let mut name = base
                .file_name()
                .map(|n| n.to_os_string())
                .unwrap_or_default();
            name.push(format!("-{p}"));
            base.with_file_name(name)
        }
    }
}

// ---------------------------------------------------------------------------
// Settings
// ---------------------------------------------------------------------------

/// Settings as first launched: the default device name, nothing else set.
pub fn default_settings(profile: Option<&str>, data_dir: &Path) -> Settings {
    Settings {
        device_name: HubConfig::defaults(Tool::App, profile, data_dir.to_path_buf()).device_name,
        allow_other_logins: false,
        theme: Theme::System,
        target_peer: None,
    }
}

/// Trim and check settings from the UI. The device name must be 1–63
/// characters with no control characters; an empty target means none.
pub fn normalise_settings(mut s: Settings) -> Result<Settings, AppError> {
    s.device_name = s.device_name.trim().to_string();
    let len = s.device_name.chars().count();
    if len == 0 || len > 63 || s.device_name.chars().any(char::is_control) {
        return Err(AppError::invalid_params(
            "the device name must be 1–63 characters",
        ));
    }
    s.target_peer = s
        .target_peer
        .map(|t| t.trim().to_string())
        .filter(|t| !t.is_empty());
    Ok(s)
}

/// `settings.json` as the app stores it: the [`Settings`] the UI sees, plus
/// the app's own `state_key`, which never leaves Rust.
#[derive(Serialize, Deserialize)]
struct StoredSettings {
    #[serde(flatten)]
    settings: Settings,
    /// The directory under `<data_dir>/state/` that holds this device's
    /// tsnet state. Chosen from the device name at the first sign-in and
    /// kept across renames, so a new name renames the tailnet device in
    /// place instead of starting a new one (§5.3 names only the first dir).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    state_key: Option<String>,
}

/// The state key for a device name: the directory name §5.3 gives
/// `<data_dir>/state/<slug(device_name)>`.
pub fn state_key_for(data_dir: &Path, device_name: &str) -> String {
    state_dir_for(data_dir, device_name)
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default()
}

/// A stored state key is one plain directory name: letters, digits, `-`
/// and `_` (a slug is `[a-z0-9-]`), never `..` or a path.
fn valid_state_key(key: &str) -> bool {
    (1..=128).contains(&key.len())
        && key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

/// `<data_dir>/state/<state_key>`.
pub fn state_dir_of(data_dir: &Path, state_key: &str) -> PathBuf {
    data_dir.join("state").join(state_key)
}

/// Load `<data_dir>/settings.json`; a missing or unreadable file gives the
/// defaults (an unreadable one is logged).
pub fn load_settings(data_dir: &Path, profile: Option<&str>) -> Settings {
    load_stored(data_dir, profile).0
}

/// [`load_settings`] and the state key (from the device name when the file
/// has none: installs from before the key existed keep their directory).
pub fn load_stored(data_dir: &Path, profile: Option<&str>) -> (Settings, String) {
    let path = data_dir.join(SETTINGS_FILE);
    let defaults = || {
        let s = default_settings(profile, data_dir);
        let key = state_key_for(data_dir, &s.device_name);
        (s, key)
    };
    let bytes = match std::fs::read(&path) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return defaults(),
        Err(e) => {
            tracing::warn!(path = %path.display(), error = %e, "could not read the settings; using defaults");
            return defaults();
        }
    };
    match serde_json::from_slice::<StoredSettings>(&bytes).map_err(|e| e.to_string()) {
        Ok(stored) => match normalise_settings(stored.settings) {
            Ok(s) => {
                let key = stored
                    .state_key
                    .filter(|k| valid_state_key(k))
                    .unwrap_or_else(|| state_key_for(data_dir, &s.device_name));
                (s, key)
            }
            Err(_) => defaults(),
        },
        Err(e) => {
            tracing::warn!(path = %path.display(), error = %e, "the settings file is not valid; using defaults");
            defaults()
        }
    }
}

/// Write `<data_dir>/settings.json` (pretty JSON, replaced atomically).
pub fn save_settings(data_dir: &Path, s: &Settings, state_key: &str) -> Result<(), AppError> {
    std::fs::create_dir_all(data_dir)
        .map_err(|e| AppError::io(format!("create {}", data_dir.display()), &e))?;
    let stored = StoredSettings {
        settings: s.clone(),
        state_key: Some(state_key.to_string()),
    };
    let mut json = serde_json::to_vec_pretty(&stored)
        .map_err(|e| AppError::internal(format!("serialise the settings: {e}")))?;
    json.push(b'\n');
    let path = data_dir.join(SETTINGS_FILE);
    replace_file(&path, &json)
}

/// Written into the state directory when the hub first runs: a directory
/// without it only saw a start that never signed in (cancelled at the login
/// link, or a failed start), so it is no reason to start on our own.
pub const SIGNED_IN_MARKER: &str = "signed-in";

/// Whether `state_dir` holds a sign-in that reached `running`.
pub fn signed_in_at(state_dir: &Path) -> bool {
    state_dir.join(SIGNED_IN_MARKER).is_file()
}

fn mark_signed_in(state_dir: &Path) {
    let path = state_dir.join(SIGNED_IN_MARKER);
    if path.is_file() {
        return;
    }
    if let Err(e) = std::fs::write(&path, b"") {
        tracing::warn!(path = %path.display(), error = %e, "could not record the sign-in");
    }
}

/// Write `bytes` to a dot-file beside `path`, then rename it over `path`.
fn replace_file(path: &Path, bytes: &[u8]) -> Result<(), AppError> {
    let name = path
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_default();
    let tmp = path.with_file_name(format!(".tmp-app-{name}"));
    std::fs::write(&tmp, bytes)
        .map_err(|e| AppError::io(format!("write {}", tmp.display()), &e))?;
    std::fs::rename(&tmp, path).map_err(|e| {
        let _ = std::fs::remove_file(&tmp);
        AppError::io(format!("replace {}", path.display()), &e)
    })
}

// ---------------------------------------------------------------------------
// Foreground share (`NodeEnv.app_foreground_pct`)
// ---------------------------------------------------------------------------

/// Records when the main window is key, so a run can say what share of it
/// the app spent in front (§7.4 `app_foreground_pct`).
#[derive(Debug, Default)]
pub struct Foreground {
    log: Mutex<FocusLog>,
}

#[derive(Debug, Default)]
struct FocusLog {
    /// Start of the current focused span (Unix ms), if focused.
    since: Option<u64>,
    /// Closed focused spans `[start, end)` in Unix ms, oldest first.
    spans: VecDeque<(u64, u64)>,
}

impl Foreground {
    /// The window gained (`true`) or lost focus now.
    pub fn set_focused(&self, focused: bool) {
        self.set_focused_at(focused, truffle_bench::unix_ms());
    }

    /// [`set_focused`](Self::set_focused) at a given time (tests).
    pub fn set_focused_at(&self, focused: bool, now_ms: u64) {
        let mut log = lock(&self.log);
        match (focused, log.since) {
            (true, None) => log.since = Some(now_ms),
            (false, Some(start)) => {
                log.since = None;
                log.spans.push_back((start, now_ms));
                while log.spans.len() > FOREGROUND_SPANS {
                    log.spans.pop_front();
                }
            }
            _ => {}
        }
    }

    /// Percentage of `[start_ms, end_ms)` with the window focused, to 0.1. A
    /// run that took no whole millisecond gets 100 or 0: focused then or not.
    /// `None` for a window that ends before it starts.
    pub fn pct(&self, start_ms: u64, end_ms: u64) -> Option<f64> {
        self.pct_at(start_ms, end_ms, truffle_bench::unix_ms())
    }

    /// [`pct`](Self::pct) with an explicit "now" (tests).
    pub fn pct_at(&self, start_ms: u64, end_ms: u64, now_ms: u64) -> Option<f64> {
        if end_ms < start_ms {
            return None;
        }
        let end_ms = end_ms.max(start_ms + 1);
        let log = lock(&self.log);
        let open = log.since.map(|s| (s, now_ms.max(s).max(end_ms)));
        let focused: u64 = log
            .spans
            .iter()
            .copied()
            .chain(open)
            .map(|(a, b)| b.min(end_ms).saturating_sub(a.max(start_ms)))
            .sum();
        let pct = focused as f64 * 100.0 / (end_ms - start_ms) as f64;
        Some((pct.clamp(0.0, 100.0) * 10.0).round() / 10.0)
    }
}

/// The hub's report hook (§7.4 `app_foreground_pct`): set the share of the
/// run the window spent focused on every report before the hub saves it.
pub fn foreground_hook(foreground: Arc<Foreground>) -> ReportHook {
    Arc::new(move |report: &mut RunReport| {
        let (Some(start), Some(end)) = (
            rfc3339_ms(&report.started_at),
            rfc3339_ms(&report.finished_at),
        ) else {
            return;
        };
        if let Some(pct) = foreground.pct(start, end) {
            report.local.app_foreground_pct = Some(pct);
        }
    })
}

fn rfc3339_ms(s: &str) -> Option<u64> {
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .and_then(|t| u64::try_from(t.timestamp_millis()).ok())
}

// ---------------------------------------------------------------------------
// macOS process activity while any run is active (§11.1)
// ---------------------------------------------------------------------------

#[cfg(target_os = "macos")]
type Hold = crate::macos::Activity;

#[cfg(not(target_os = "macos"))]
struct Hold;

#[cfg(not(target_os = "macos"))]
impl Hold {
    fn begin(_reason: &str) -> Self {
        Hold
    }
}

/// Counts runs in either role and holds the process activity while any is
/// active: runs this app initiates, and runs peers prepared against it.
#[derive(Default)]
pub struct Activity {
    initiating: AtomicUsize,
    remote: Mutex<HashSet<String>>,
    held: Mutex<Option<Hold>>,
}

impl Activity {
    /// Whether the activity is held now.
    pub fn is_held(&self) -> bool {
        lock(&self.held).is_some()
    }

    fn begin_initiator(&self) {
        self.initiating.fetch_add(1, Ordering::SeqCst);
        self.refresh();
    }

    fn end_initiator(&self) {
        let _ = self
            .initiating
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1));
        self.refresh();
    }

    fn remote_event(&self, run_id: &str, state: RemoteRunState) {
        {
            let mut remote = lock(&self.remote);
            match state {
                RemoteRunState::Prepared => {
                    remote.insert(run_id.to_string());
                }
                _ => {
                    remote.remove(run_id);
                }
            }
        }
        self.refresh();
    }

    fn clear_remote(&self) {
        lock(&self.remote).clear();
        self.refresh();
    }

    fn refresh(&self) {
        let active = self.initiating.load(Ordering::SeqCst) > 0 || !lock(&self.remote).is_empty();
        let mut held = lock(&self.held);
        match (active, held.is_some()) {
            (true, false) => {
                tracing::debug!("a run is active: holding the process activity");
                *held = Some(Hold::begin("truffle bench is measuring"));
            }
            (false, true) => {
                tracing::debug!("no run is active: releasing the process activity");
                *held = None;
            }
            _ => {}
        }
    }
}

/// Counts one initiated run for as long as it lives.
struct InitiatorGuard(Arc<Inner>);

impl InitiatorGuard {
    fn new(inner: &Arc<Inner>) -> Self {
        inner.activity.begin_initiator();
        Self(inner.clone())
    }
}

impl Drop for InitiatorGuard {
    fn drop(&mut self) {
        self.0.activity.end_initiator();
    }
}

// ---------------------------------------------------------------------------
// AppState
// ---------------------------------------------------------------------------

/// How [`AppState`] is created.
pub struct StateInit {
    /// The profile, if any.
    pub profile: Option<String>,
    /// The profile's data directory (runs, settings, tsnet state).
    pub data_dir: PathBuf,
    /// True inside a packaged app (sidecar next to the executable).
    pub bundled: bool,
    /// `TRUFFLE_BENCH_AUTH_KEY`, used when `hub_start` brings none.
    pub env_auth_key: Option<String>,
    /// Where hub events go.
    pub emit: Emit,
}

/// Everything the commands share. Cheap to clone.
#[derive(Clone)]
pub struct AppState {
    inner: Arc<Inner>,
}

struct Inner {
    profile: Option<String>,
    data_dir: PathBuf,
    bundled: bool,
    env_auth_key: Option<String>,
    settings: Mutex<Settings>,
    /// The tsnet state directory's name under `<data_dir>/state/`
    /// ([`StoredSettings::state_key`]).
    state_key: Mutex<String>,
    hub: Mutex<Arc<Hub>>,
    forwarder: Mutex<Option<tauri::async_runtime::JoinHandle<()>>>,
    emit: Emit,
    activity: Activity,
    foreground: Arc<Foreground>,
    exiting: AtomicBool,
    shut_down: AtomicBool,
    ui_ready: AtomicBool,
}

impl Drop for Inner {
    fn drop(&mut self) {
        if let Some(task) = lock(&self.forwarder).take() {
            task.abort();
        }
    }
}

/// Starting, needing login or approval, running or stopping: the hub owns a
/// node (or is building one), so its config cannot change.
pub fn is_live(status: NodeStatus) -> bool {
    matches!(
        status,
        NodeStatus::Starting
            | NodeStatus::NeedsLogin
            | NodeStatus::NeedsApproval
            | NodeStatus::Running
            | NodeStatus::Stopping
    )
}

/// A node state as the app shows it: the state-lock error gets §11.1's words.
pub fn present(mut state: NodeState) -> NodeState {
    if let Some(e) = state.error.as_mut() {
        if e.code == ErrorCode::StateInUse {
            e.message = STATE_IN_USE_MESSAGE.to_string();
        }
    }
    state
}

fn present_event(event: HubEvent) -> HubEvent {
    match event {
        HubEvent::Node(state) => HubEvent::Node(Box::new(present(*state))),
        other => other,
    }
}

impl AppState {
    /// Load the settings and create an idle hub for them.
    pub fn new(init: StateInit) -> Self {
        let (settings, state_key) = load_stored(&init.data_dir, init.profile.as_deref());
        let cfg = hub_config(
            init.profile.as_deref(),
            &init.data_dir,
            init.bundled,
            &settings,
            &state_key,
            None,
        );
        let inner = Arc::new(Inner {
            profile: init.profile,
            data_dir: init.data_dir,
            bundled: init.bundled,
            env_auth_key: init.env_auth_key,
            settings: Mutex::new(settings),
            state_key: Mutex::new(state_key),
            hub: Mutex::new(Hub::new(cfg)),
            forwarder: Mutex::new(None),
            emit: init.emit,
            activity: Activity::default(),
            foreground: Arc::new(Foreground::default()),
            exiting: AtomicBool::new(false),
            shut_down: AtomicBool::new(false),
            ui_ready: AtomicBool::new(false),
        });
        let state = Self { inner };
        let hub = state.hub();
        state.adopt(&hub);
        state
    }

    /// The profile, if any.
    pub fn profile(&self) -> Option<&str> {
        self.inner.profile.as_deref()
    }

    /// The profile's data directory.
    pub fn data_dir(&self) -> &Path {
        &self.inner.data_dir
    }

    /// The current hub.
    pub fn hub(&self) -> Arc<Hub> {
        lock(&self.inner.hub).clone()
    }

    /// The current settings.
    pub fn settings(&self) -> Settings {
        lock(&self.inner.settings).clone()
    }

    /// The tsnet state directory a hub started now would use.
    pub fn state_dir(&self) -> PathBuf {
        state_dir_of(self.data_dir(), &lock(&self.inner.state_key))
    }

    /// The state key for a device about to be called `name`: the current one
    /// once this device has signed in (a rename keeps the sign-in), else the
    /// one §5.3 derives from `name`.
    fn state_key_for_name(&self, name: &str) -> String {
        if self.has_saved_state() {
            lock(&self.inner.state_key).clone()
        } else {
            state_key_for(self.data_dir(), name)
        }
    }

    /// Persist `settings` with `state_key`, then make both current.
    fn commit_settings(&self, settings: &Settings, state_key: &str) -> Result<(), AppError> {
        save_settings(self.data_dir(), settings, state_key)?;
        *lock(&self.inner.settings) = settings.clone();
        *lock(&self.inner.state_key) = state_key.to_string();
        Ok(())
    }

    /// The run trackers.
    pub fn activity(&self) -> &Activity {
        &self.inner.activity
    }

    /// The focus tracker.
    pub fn foreground(&self) -> &Foreground {
        &self.inner.foreground
    }

    /// The hub's state as the UI shows it.
    pub fn node_state(&self) -> NodeState {
        present(self.hub().state())
    }

    /// Record that the UI painted (the window may be shown).
    pub fn mark_ui_ready(&self) {
        self.inner.ui_ready.store(true, Ordering::SeqCst);
    }

    /// Whether `ui_ready` has been called.
    pub fn ui_is_ready(&self) -> bool {
        self.inner.ui_ready.load(Ordering::SeqCst)
    }

    /// Mark the app as quitting; `true` the first time only.
    pub fn begin_exit(&self) -> bool {
        !self.inner.exiting.swap(true, Ordering::SeqCst)
    }

    /// Whether quit has begun.
    pub fn is_exiting(&self) -> bool {
        self.inner.exiting.load(Ordering::SeqCst)
    }

    fn config_for(&self, settings: &Settings, auth_key: Option<String>) -> HubConfig {
        let key = lock(&self.inner.state_key).clone();
        hub_config(
            self.profile(),
            self.data_dir(),
            self.inner.bundled,
            settings,
            &key,
            auth_key,
        )
    }

    /// Replace the hub (only while it is not live) and adopt it.
    fn install(&self, hub: Arc<Hub>) {
        let old = std::mem::replace(&mut *lock(&self.inner.hub), hub.clone());
        self.adopt(&hub);
        drop(old);
    }

    /// Forward a hub's events and give it the foreground report hook.
    fn adopt(&self, hub: &Arc<Hub>) {
        hub.set_report_hook(foreground_hook(self.inner.foreground.clone()));
        self.forward(hub);
    }

    /// Forward `hub`'s events to the UI, replacing any previous forwarder.
    fn forward(&self, hub: &Arc<Hub>) {
        let rx = hub.subscribe();
        let task = tauri::async_runtime::spawn(forward_events(
            Arc::downgrade(&self.inner),
            Arc::downgrade(hub),
            rx,
        ));
        if let Some(prev) = lock(&self.inner.forwarder).replace(task) {
            prev.abort();
        }
    }

    /// Whether this device has signed in before: its state directory holds
    /// [`SIGNED_IN_MARKER`]. A directory left by a start that never reached
    /// `running` (cancelled at the login link, a missing sidecar) does not
    /// count, so the next launch shows the form rather than the login link.
    pub fn has_saved_state(&self) -> bool {
        signed_in_at(&self.state_dir())
    }

    /// At launch: start on our own when there is something to sign in with
    /// (a saved state directory or `TRUFFLE_BENCH_AUTH_KEY`); otherwise the
    /// UI shows the sign-in form.
    pub fn autostart(&self) {
        let saved = self.has_saved_state();
        let key = self.inner.env_auth_key.is_some();
        if !saved && !key {
            tracing::info!("no saved sign-in: waiting for the sign-in form");
            return;
        }
        tracing::info!(
            saved_state = saved,
            env_auth_key = key,
            "starting the hub on launch"
        );
        let state = self.clone();
        tauri::async_runtime::spawn(async move {
            if let Err(e) = state.start_hub(None, None).await {
                tracing::warn!(code = %e.code, error = %e.message, "could not start the hub");
            }
        });
    }

    /// `hub_start`: save a new device name, then build a fresh hub (device
    /// name, auth key) and spawn [`Hub::start`]. Returns at once with the
    /// first state the hub reports. A hub that is already live is left alone.
    ///
    /// A device that has signed in keeps its state directory under a new
    /// name (the tailnet device is renamed in place); one that has not gets
    /// the directory §5.3 derives from the new name. After a `state-in-use`
    /// failure the name is not changed: another instance holds this profile,
    /// and a new name would only start a second node beside it (§11.1).
    pub async fn start_hub(
        &self,
        device_name: Option<String>,
        auth_key: Option<String>,
    ) -> Result<NodeState, AppError> {
        let current = self.hub();
        let now = current.state();
        if is_live(now.state) {
            return Ok(present(now));
        }
        let in_use = now
            .error
            .as_ref()
            .is_some_and(|e| e.code == ErrorCode::StateInUse);
        let name = device_name.filter(|n| !n.trim().is_empty());
        if in_use && name.is_some() {
            tracing::info!("another instance holds this profile: keeping the device name");
        }
        let mut settings = self.settings();
        if let Some(name) = name.filter(|_| !in_use) {
            settings.device_name = name;
            settings = normalise_settings(settings)?;
            let key = self.state_key_for_name(&settings.device_name);
            self.commit_settings(&settings, &key)?;
        }
        let key = auth_key
            .map(|k| k.trim().to_string())
            .filter(|k| !k.is_empty())
            .or_else(|| self.inner.env_auth_key.clone());
        let hub = Hub::new(self.config_for(&settings, key));
        let mut rx = hub.subscribe();
        self.install(hub.clone());
        let starter = hub.clone();
        tauri::async_runtime::spawn(async move {
            // Failures are reported through the `error` node state.
            let _ = starter.start().await;
        });
        let _ = tokio::time::timeout(START_REPLY_WAIT, async {
            loop {
                match rx.recv().await {
                    Ok(HubEvent::Node(_)) | Err(broadcast::error::RecvError::Closed) => break,
                    _ => {}
                }
            }
        })
        .await;
        let st = hub.state();
        if st.state == NodeStatus::Idle {
            // Still resolving the sidecar: say what is happening.
            let mut starting = NodeState::new(NodeStatus::Starting);
            starting.tailscale_hostname = st.tailscale_hostname;
            return Ok(starting);
        }
        Ok(present(st))
    }

    /// `hub_stop`.
    pub async fn stop_hub(&self) -> NodeState {
        let hub = self.hub();
        hub.stop().await;
        self.inner.activity.clear_remote();
        present(hub.state())
    }

    /// `hub_sign_out`: LocalAPI logout, stop, delete the state directory.
    /// The next sign-in starts from the directory the current name gives.
    pub async fn sign_out(&self) -> Result<NodeState, AppError> {
        let hub = self.hub();
        let res = hub.sign_out().await;
        self.inner.activity.clear_remote();
        if res.is_ok() {
            let settings = self.settings();
            let key = state_key_for(self.data_dir(), &settings.device_name);
            if let Err(e) = self.commit_settings(&settings, &key) {
                tracing::warn!(error = %e.message, "could not save the settings after signing out");
            }
        }
        res.map(|()| present(hub.state()))
    }

    /// Quit (§11.1): cancel this hub's runs and suites, then stop it within
    /// `budget`. Runs once; later calls return at once.
    pub async fn shutdown(&self, budget: Duration) {
        if self.inner.shut_down.swap(true, Ordering::SeqCst) {
            return;
        }
        self.inner.exiting.store(true, Ordering::SeqCst);
        let hub = self.hub();
        let active = hub.active_runs();
        tracing::info!(
            active_runs = active.len(),
            "quitting: cancelling runs and stopping the hub"
        );
        for run in active {
            if let Some(suite) = &run.suite_id {
                hub.cancel(suite);
            }
            hub.cancel(&run.run_id);
        }
        let t0 = std::time::Instant::now();
        if tokio::time::timeout(budget, hub.stop()).await.is_err() {
            tracing::warn!(
                "quit: the hub did not stop within {} s",
                budget.as_secs_f64()
            );
        } else {
            tracing::info!(
                ms = t0.elapsed().as_millis() as u64,
                "quit: the hub stopped"
            );
        }
    }

    /// `settings_set` (theme is applied by the caller, which has the
    /// window): persist, admit or refuse other accounts at once, and give an
    /// idle hub the new device name. A device that has signed in keeps its
    /// state directory under a new name, so it stays signed in (the tailnet
    /// device is renamed at its next start); one that has not follows §5.3.
    pub fn update_settings(&self, new: Settings) -> Result<Settings, AppError> {
        let new = normalise_settings(new)?;
        let old = self.settings();
        let key = if old.device_name == new.device_name {
            lock(&self.inner.state_key).clone()
        } else {
            self.state_key_for_name(&new.device_name)
        };
        self.commit_settings(&new, &key)?;
        let hub = self.hub();
        hub.set_allow_other_logins(new.allow_other_logins);
        if old.device_name != new.device_name && !is_live(hub.state().state) {
            let hub = Hub::new(self.config_for(&new, None));
            self.install(hub.clone());
            (self.inner.emit)(&HubEvent::Node(Box::new(present(hub.state()))));
        }
        Ok(new)
    }

    /// `app_info`.
    pub async fn app_info(&self) -> AppInfo {
        let hub = self.hub();
        let path = match hub.local() {
            Some(local) => Some(PathBuf::from(local.sidecar_path)),
            None => resolve_sidecar(hub.config()).ok(),
        };
        let sha = match path.clone() {
            Some(p) => tauri::async_runtime::spawn_blocking(move || env::sidecar_sha256(&p))
                .await
                .ok()
                .flatten(),
            None => None,
        };
        AppInfo {
            app_version: truffle_bench::TOOL_VERSION.to_string(),
            truffle_version: truffle_core::VERSION.to_string(),
            data_dir: self.data_dir().display().to_string(),
            profile: self.inner.profile.clone(),
            os: std::env::consts::OS.to_string(),
            build_profile: BuildProfile::current(),
            sidecar_path: path.map(|p| p.display().to_string()),
            sidecar_sha256: sha,
        }
    }

    /// `bench_run`: one run on the hub, holding the process activity. The
    /// hub's report hook records the foreground share before it saves.
    pub async fn run(&self, req: RunRequest, progress: ProgressSink) -> RunReport {
        let hub = self.hub();
        let _guard = InitiatorGuard::new(&self.inner);
        hub.run(req, progress).await
    }

    /// `suite_run`: like [`run`](Self::run), per item.
    pub async fn suite(&self, req: SuiteRequest, progress: SuiteProgressSink) -> SuiteResult {
        let hub = self.hub();
        let _guard = InitiatorGuard::new(&self.inner);
        hub.suite(req, progress).await
    }

    /// `runs_import` / `runs_import_pick`: import each path into the store.
    pub async fn import(&self, paths: Vec<PathBuf>) -> Result<ImportResult, AppError> {
        let hub = self.hub();
        tauri::async_runtime::spawn_blocking(move || {
            let mut total = ImportResult::default();
            for path in paths {
                let r = hub.store().import(&path);
                total.imported += r.imported;
                total.skipped += r.skipped;
            }
            total
        })
        .await
        .map_err(|e| AppError::internal(format!("import: {e}")))
    }
}

/// The hub configuration for a device name, its state directory and an
/// optional auth key.
fn hub_config(
    profile: Option<&str>,
    data_dir: &Path,
    bundled: bool,
    settings: &Settings,
    state_key: &str,
    auth_key: Option<String>,
) -> HubConfig {
    let mut cfg = HubConfig::defaults(Tool::App, profile, data_dir.to_path_buf())
        .with_device_name(settings.device_name.clone());
    cfg.state_dir = state_dir_of(data_dir, state_key);
    if let Some(key) = auth_key {
        cfg = cfg.with_auth_key(key);
    }
    cfg.allow_other_logins = settings.allow_other_logins;
    cfg.bundled = bundled;
    cfg
}

/// Relay one hub's events to the UI, tracking remote runs and logging. The
/// first `running` records the sign-in ([`SIGNED_IN_MARKER`]).
async fn forward_events(state: Weak<Inner>, hub: Weak<Hub>, mut rx: broadcast::Receiver<HubEvent>) {
    loop {
        let events = match rx.recv().await {
            Ok(event) => vec![event],
            Err(broadcast::error::RecvError::Lagged(n)) => {
                // What was missed is replaced by snapshots: the node state and
                // the peer list. (Remote-run ends cannot be recovered: the
                // engine has no query for the runs it serves.)
                tracing::warn!(
                    missed = n,
                    "hub events lagged; re-sending the node state and peers"
                );
                let Some(hub) = hub.upgrade() else {
                    break;
                };
                let node = HubEvent::Node(Box::new(hub.state()));
                if hub.state().state == NodeStatus::Running {
                    vec![
                        node,
                        HubEvent::Peers {
                            peers: hub.peers().await,
                        },
                    ]
                } else {
                    vec![node]
                }
            }
            Err(broadcast::error::RecvError::Closed) => break,
        };
        for event in events {
            let Some(inner) = state.upgrade() else {
                return;
            };
            if let HubEvent::Node(st) = &event {
                if st.state == NodeStatus::Running {
                    if let Some(hub) = hub.upgrade() {
                        mark_signed_in(&hub.config().state_dir);
                    }
                }
            }
            let event = present_event(event);
            observe(&inner, &event);
            (inner.emit)(&event);
        }
    }
}

fn observe(inner: &Inner, event: &HubEvent) {
    match event {
        HubEvent::Node(st) => {
            match (&st.local, &st.error) {
                (Some(local), _) => tracing::info!(
                    state = ?st.state,
                    device = %local.device_name,
                    login = local.login_name.as_deref().unwrap_or("-"),
                    ip = local.tailscale_ip.as_deref().unwrap_or("-"),
                    hub_start_ms = local.hub_start_ms,
                    "hub"
                ),
                (None, Some(e)) => {
                    tracing::warn!(state = ?st.state, code = %e.code, error = %e.message, "hub")
                }
                (None, None) => tracing::info!(state = ?st.state, "hub"),
            }
            if let Some(url) = &st.auth_url {
                tracing::debug!(%url, "login URL");
            }
            if st.state != NodeStatus::Running {
                inner.activity.clear_remote();
            }
        }
        HubEvent::Peers { peers } => {
            tracing::debug!(count = peers.len(), "peers");
            for p in peers {
                tracing::debug!(
                    name = %p.device_name,
                    id = %p.id,
                    online = p.online,
                    connection = %p.connection,
                    same_login = p.same_login,
                    hello = p.hello.as_ref().map(|h| h.app.as_str()).unwrap_or("-"),
                    hello_error = p.hello_error.as_ref().map(|e| e.message.as_str()).unwrap_or("-"),
                    "peer"
                );
            }
        }
        HubEvent::RemoteRun {
            run_id,
            peer,
            workload,
            state,
            ..
        } => {
            tracing::info!(%peer, %workload, %run_id, state = ?state, "remote run");
            inner.activity.remote_event(run_id, *state);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profile_comes_from_argv_then_env_then_debug_default() {
        let args = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        assert_eq!(
            profile_from(args(&["--profile", "b"]), Some("env".into()), true).unwrap(),
            Some("b".into())
        );
        assert_eq!(
            profile_from(args(&["--profile=c"]), None, false).unwrap(),
            Some("c".into())
        );
        assert_eq!(
            profile_from(args(&[]), Some("env".into()), true).unwrap(),
            Some("env".into())
        );
        assert_eq!(
            profile_from(args(&[]), Some(" ".into()), true).unwrap(),
            Some("dev".into())
        );
        assert_eq!(profile_from(args(&[]), None, false).unwrap(), None);
        assert!(profile_from(args(&["--profile"]), None, false).is_err());
        assert!(profile_from(args(&["--profile", "../x"]), None, false).is_err());
        assert!(profile_from(args(&["--profile", ""]), None, false).is_err());
    }

    #[test]
    fn profile_dirs_append_the_profile() {
        let base = Path::new("/data/com.vibecook.truffle-bench");
        assert_eq!(profile_data_dir(base, None), base);
        assert_eq!(
            profile_data_dir(base, Some("dev")),
            Path::new("/data/com.vibecook.truffle-bench-dev")
        );
    }

    #[test]
    fn foreground_share_counts_focused_overlap() {
        let f = Foreground::default();
        f.set_focused_at(true, 1_000);
        f.set_focused_at(false, 3_000);
        f.set_focused_at(true, 5_000);
        // [0, 10 000): focused 1–3 s and 5–10 s (still focused at "now").
        assert_eq!(f.pct_at(0, 10_000, 10_000), Some(70.0));
        // A window entirely before any focus.
        assert_eq!(f.pct_at(0, 1_000, 10_000), Some(0.0));
        // Repeated focus events do not double count.
        f.set_focused_at(true, 6_000);
        assert_eq!(f.pct_at(5_000, 7_000, 7_000), Some(100.0));
        // A run shorter than a millisecond: focused then, or not.
        assert_eq!(f.pct_at(5_000, 5_000, 7_000), Some(100.0));
        assert_eq!(f.pct_at(4_000, 4_000, 7_000), Some(0.0));
        assert_eq!(f.pct_at(5, 4, 7_000), None);
    }

    #[test]
    fn settings_are_trimmed_and_checked() {
        let s = Settings {
            device_name: "  studio ".into(),
            allow_other_logins: false,
            theme: Theme::Dark,
            target_peer: Some(" ".into()),
        };
        let n = normalise_settings(s).unwrap();
        assert_eq!(n.device_name, "studio");
        assert_eq!(n.target_peer, None);
        let empty = Settings {
            device_name: " ".into(),
            ..n.clone()
        };
        assert_eq!(
            normalise_settings(empty).unwrap_err().code,
            ErrorCode::InvalidParams
        );
    }

    fn idle_state(dir: &Path) -> AppState {
        AppState::new(StateInit {
            profile: Some("t".into()),
            data_dir: dir.to_path_buf(),
            bundled: false,
            env_auth_key: None,
            emit: Arc::new(|_| {}),
        })
    }

    fn rename(state: &AppState, name: &str) {
        let mut s = state.settings();
        s.device_name = name.into();
        state.update_settings(s).unwrap();
    }

    #[test]
    fn only_a_start_that_ran_is_a_saved_sign_in() {
        let dir = tempfile::tempdir().unwrap();
        let state = idle_state(dir.path());
        assert!(!state.has_saved_state());
        // A start cancelled at the login link leaves the lock and tsnet's
        // machine key, but never ran: not a sign-in.
        let sd = state.state_dir();
        std::fs::create_dir_all(&sd).unwrap();
        std::fs::write(sd.join("hub.lock"), b"").unwrap();
        std::fs::write(sd.join("tailscaled.state"), b"{}").unwrap();
        assert!(!state.has_saved_state());
        mark_signed_in(&sd);
        assert!(state.has_saved_state());
        assert_eq!(state.hub().config().state_dir, sd);
    }

    #[test]
    fn a_rename_after_signing_in_keeps_the_state_dir() {
        let dir = tempfile::tempdir().unwrap();
        let state = idle_state(dir.path());
        // Before any sign-in a rename follows §5.3: state/<slug(name)>.
        rename(&state, "Studio");
        let studio = dir.path().join("state").join("studio");
        assert_eq!(state.state_dir(), studio);
        std::fs::create_dir_all(&studio).unwrap();
        mark_signed_in(&studio);

        // Signed in: a new name keeps the directory, and with it the sign-in
        // and the device id.
        rename(&state, "Studio 2");
        assert_eq!(state.settings().device_name, "Studio 2");
        assert!(state.has_saved_state());
        assert_eq!(state.state_dir(), studio);
        let cfg = state.hub().config().clone();
        assert_eq!(cfg.device_name, "Studio 2");
        assert_eq!(cfg.state_dir, studio);

        // …and so does the next launch.
        let again = idle_state(dir.path());
        assert_eq!(again.settings().device_name, "Studio 2");
        assert!(again.has_saved_state());
        assert_eq!(again.hub().config().state_dir, studio);
    }

    #[test]
    fn settings_files_without_a_state_key_keep_their_directory() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join(SETTINGS_FILE),
            br#"{"device_name":"Old Mac","allow_other_logins":false,"theme":"system","target_peer":null}"#,
        )
        .unwrap();
        let (s, key) = load_stored(dir.path(), Some("t"));
        assert_eq!(s.device_name, "Old Mac");
        assert_eq!(key, "old-mac");
        // A key that is not one plain directory name is ignored.
        std::fs::write(
            dir.path().join(SETTINGS_FILE),
            br#"{"device_name":"Old Mac","allow_other_logins":false,"theme":"system","target_peer":null,"state_key":"../x"}"#,
        )
        .unwrap();
        assert_eq!(load_stored(dir.path(), Some("t")).1, "old-mac");
        // Saved keys round-trip, and the UI's Settings never carry them.
        save_settings(dir.path(), &s, "first-name").unwrap();
        assert_eq!(load_stored(dir.path(), Some("t")).1, "first-name");
        assert!(!serde_json::to_value(load_settings(dir.path(), Some("t")))
            .unwrap()
            .as_object()
            .unwrap()
            .contains_key("state_key"));
    }

    #[test]
    fn state_in_use_gets_the_app_wording() {
        let mut st = NodeState::new(NodeStatus::Error);
        st.error = Some(AppError::new(ErrorCode::StateInUse, "engine words"));
        assert_eq!(present(st).error.unwrap().message, STATE_IN_USE_MESSAGE);
    }

    #[test]
    fn the_report_hook_records_the_foreground_share() {
        let fixture =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../schema/fixtures/report-tcp-ok.json");
        let mut report: RunReport =
            serde_json::from_slice(&std::fs::read(fixture).unwrap()).unwrap();
        let start = rfc3339_ms(&report.started_at).unwrap();
        let end = rfc3339_ms(&report.finished_at).unwrap();
        let fg = Arc::new(Foreground::default());
        fg.set_focused_at(true, start);
        fg.set_focused_at(false, start + (end - start) / 4);
        foreground_hook(fg)(&mut report);
        let pct = report.local.app_foreground_pct.unwrap();
        assert!((pct - 25.0).abs() <= 0.1, "{pct}");

        // A report with unreadable times is left alone.
        report.local.app_foreground_pct = None;
        report.started_at = "not a time".into();
        foreground_hook(Arc::new(Foreground::default()))(&mut report);
        assert_eq!(report.local.app_foreground_pct, None);
    }

    #[tokio::test]
    async fn every_saved_report_carries_the_foreground_share() {
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::new(StateInit {
            profile: Some("t".into()),
            data_dir: dir.path().to_path_buf(),
            bundled: false,
            env_auth_key: None,
            emit: Arc::new(|_| {}),
        });
        state
            .foreground()
            .set_focused_at(true, truffle_bench::unix_ms() - 1_000);
        // The hub isn't running, so the run fails at once, but it registered: the
        // hub runs its hook and saves the report.
        let req = RunRequest {
            run_id: "fg-1".into(),
            peer: "nobody".into(),
            params: truffle_bench::Workload::Path.default_params(),
            suite_id: None,
        };
        let report = state.run(req, Arc::new(|_| {})).await;
        assert_eq!(report.local.app_foreground_pct, Some(100.0));
        let stored = state.hub().store().get("fg-1").unwrap();
        assert_eq!(stored.local.app_foreground_pct, Some(100.0));

        // A hub built later (a new device name while idle) gets the hook too.
        let mut s = state.settings();
        s.device_name = "renamed".into();
        state.update_settings(s).unwrap();
        state
            .foreground()
            .set_focused_at(false, truffle_bench::unix_ms());
        let req = RunRequest {
            run_id: "fg-2".into(),
            peer: "nobody".into(),
            params: truffle_bench::Workload::Path.default_params(),
            suite_id: None,
        };
        let report = state.run(req, Arc::new(|_| {})).await;
        assert_eq!(report.local.app_foreground_pct, Some(0.0));
    }
}
