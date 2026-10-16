//! The app commands (RFC 027 §11.2).
//!
//! JS calls `invoke(cmd, { req })`; streaming commands also take
//! `onProgress`, a `Channel`. Every command returns `Result<T, AppError>`,
//! so a rejection is always `{ code, message }`: requests arrive as JSON and
//! are parsed here, so a malformed one is `invalid-params` rather than
//! Tauri's own argument error.

use std::path::PathBuf;
use std::sync::Arc;

use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::Value;
use tauri::ipc::{Channel, Invoke};
use tauri::{AppHandle, Runtime, State};
use tauri_plugin_dialog::DialogExt;
use truffle_bench::{
    suites, ActiveRun, AppError, AppInfo, BenchPeer, ImportResult, NodeState, PeerProbe, Progress,
    ProgressSink, RunReport, RunRequest, RunSummary, Settings, SuiteProfile, SuiteProgress,
    SuiteProgressSink, SuiteRequest, SuiteResult, WorkloadParams,
};

use crate::state::AppState;
use crate::window;

/// The event every [`HubEvent`](truffle_bench::HubEvent) is emitted as.
pub const EVENT: &str = "bench://event";

/// The invoke handler with every §11.2 command.
pub fn handler<R: Runtime>() -> impl Fn(Invoke<R>) -> bool + Send + Sync + 'static {
    tauri::generate_handler![
        app_info,
        ui_ready,
        hub_state,
        hub_start,
        hub_stop,
        hub_sign_out,
        open_url,
        open_data_dir,
        peers_list,
        peer_probe,
        bench_run,
        bench_cancel,
        runs_active,
        run_attach,
        suite_profiles,
        suite_run,
        workload_defaults,
        runs_list,
        run_get,
        run_delete,
        runs_import,
        runs_import_pick,
        settings_get,
        settings_set,
    ]
}

/// Parse a command's `req`; any mismatch is `invalid-params`.
fn parse<T: DeserializeOwned>(req: Value) -> Result<T, AppError> {
    serde_json::from_value(req)
        .map_err(|e| AppError::invalid_params(format!("invalid request: {e}")))
}

async fn blocking<T: Send + 'static>(
    what: &str,
    f: impl FnOnce() -> Result<T, AppError> + Send + 'static,
) -> Result<T, AppError> {
    tauri::async_runtime::spawn_blocking(f)
        .await
        .map_err(|e| AppError::internal(format!("{what}: {e}")))?
}

// ── requests ───────────────────────────────────────────────────────────

/// `hub_start`.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HubStartReq {
    /// Saved as the device name before starting.
    #[serde(default)]
    pub device_name: Option<String>,
    /// For this start only; never saved.
    #[serde(default)]
    pub auth_key: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UrlReq {
    url: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PeerReq {
    peer: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct IdReq {
    id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RunIdReq {
    run_id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PathsReq {
    paths: Vec<String>,
}

/// `bench_run`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BenchRunReq {
    /// `^[A-Za-z0-9-]{1,64}$`, chosen by the UI.
    pub run_id: String,
    /// A peer reference (the UI sends the tailscale id).
    pub peer: String,
    /// What to run; bounds are checked by the hub.
    pub params: WorkloadParams,
}

/// `suite_run`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SuiteRunReq {
    /// `^[A-Za-z0-9-]{1,64}$`.
    pub suite_id: String,
    /// `smoke`, `standard`, `full` or `nightly`.
    pub profile: String,
    /// A peer reference.
    pub peer: String,
}

// ── app and window ─────────────────────────────────────────────────────

/// `app_info`.
#[tauri::command]
pub async fn app_info(state: State<'_, AppState>) -> Result<AppInfo, AppError> {
    tracing::debug!("command: app_info");
    Ok(state.app_info().await)
}

/// `ui_ready`: the UI painted with its theme; show the window.
#[tauri::command]
pub async fn ui_ready<R: Runtime>(
    app: AppHandle<R>,
    state: State<'_, AppState>,
) -> Result<(), AppError> {
    if !state.ui_is_ready() {
        tracing::info!("the UI is ready: showing the window");
    }
    state.mark_ui_ready();
    window::show_main(&app);
    Ok(())
}

/// `open_url`: https only, in the default browser.
#[tauri::command]
pub async fn open_url(req: Value) -> Result<(), AppError> {
    let req: UrlReq = parse(req)?;
    let url = tauri::Url::parse(req.url.trim())
        .map_err(|e| AppError::invalid_params(format!("not a URL: {e}")))?;
    if url.scheme() != "https" {
        return Err(AppError::invalid_params("only https links open"));
    }
    window::open_external(&url).map_err(|e| AppError::io("open the link", &e))
}

/// `open_data_dir`: show the profile's data folder.
#[tauri::command]
pub async fn open_data_dir(state: State<'_, AppState>) -> Result<(), AppError> {
    let dir = state.data_dir().to_path_buf();
    std::fs::create_dir_all(&dir)
        .map_err(|e| AppError::io(format!("create {}", dir.display()), &e))?;
    open::that_detached(&dir).map_err(|e| AppError::io(format!("open {}", dir.display()), &e))
}

// ── hub ────────────────────────────────────────────────────────────────

/// `hub_state`.
#[tauri::command]
pub async fn hub_state(state: State<'_, AppState>) -> Result<NodeState, AppError> {
    tracing::debug!("command: hub_state");
    Ok(state.node_state())
}

/// `hub_start`: returns at once; progress arrives as `node` events.
#[tauri::command]
pub async fn hub_start(state: State<'_, AppState>, req: Value) -> Result<NodeState, AppError> {
    tracing::debug!("command: hub_start");
    let req: HubStartReq = parse(req)?;
    state.start_hub(req.device_name, req.auth_key).await
}

/// `hub_stop`.
#[tauri::command]
pub async fn hub_stop(state: State<'_, AppState>) -> Result<NodeState, AppError> {
    tracing::debug!("command: hub_stop");
    Ok(state.stop_hub().await)
}

/// `hub_sign_out`: LocalAPI logout, stop, delete the state directory.
#[tauri::command]
pub async fn hub_sign_out(state: State<'_, AppState>) -> Result<NodeState, AppError> {
    tracing::debug!("command: hub_sign_out");
    state.sign_out().await
}

// ── peers ──────────────────────────────────────────────────────────────

/// `peers_list`.
#[tauri::command]
pub async fn peers_list(state: State<'_, AppState>) -> Result<Vec<BenchPeer>, AppError> {
    tracing::debug!("command: peers_list");
    Ok(state.hub().peers().await)
}

/// `peer_probe`.
#[tauri::command]
pub async fn peer_probe(state: State<'_, AppState>, req: Value) -> Result<PeerProbe, AppError> {
    tracing::debug!("command: peer_probe");
    let req: PeerReq = parse(req)?;
    state.hub().probe(&req.peer).await
}

// ── runs ───────────────────────────────────────────────────────────────

/// `bench_run`: the returned report is authoritative.
#[tauri::command]
pub async fn bench_run(
    state: State<'_, AppState>,
    req: Value,
    on_progress: Channel<Progress>,
) -> Result<RunReport, AppError> {
    tracing::debug!("command: bench_run");
    let req: BenchRunReq = parse(req)?;
    let sink: ProgressSink = Arc::new(move |p: Progress| {
        let _ = on_progress.send(p);
    });
    let run = RunRequest {
        run_id: req.run_id,
        peer: req.peer,
        params: req.params,
        suite_id: None,
    };
    Ok(state.run(run, sink).await)
}

/// `bench_cancel`: a run id or a suite id.
#[tauri::command]
pub async fn bench_cancel(state: State<'_, AppState>, req: Value) -> Result<bool, AppError> {
    tracing::debug!("command: bench_cancel");
    let req: IdReq = parse(req)?;
    Ok(state.hub().cancel(&req.id))
}

/// `runs_active`.
#[tauri::command]
pub async fn runs_active(state: State<'_, AppState>) -> Result<Vec<ActiveRun>, AppError> {
    tracing::debug!("command: runs_active");
    Ok(state.hub().active_runs())
}

/// `run_attach`: replays the last Progress, then streams.
#[tauri::command]
pub async fn run_attach(
    state: State<'_, AppState>,
    req: Value,
    on_progress: Channel<Progress>,
) -> Result<bool, AppError> {
    tracing::debug!("command: run_attach");
    let req: RunIdReq = parse(req)?;
    let sink: ProgressSink = Arc::new(move |p: Progress| {
        let _ = on_progress.send(p);
    });
    Ok(state.hub().attach(&req.run_id, sink))
}

// ── suites and params ──────────────────────────────────────────────────

/// `suite_profiles`.
#[tauri::command]
pub async fn suite_profiles() -> Result<Vec<SuiteProfile>, AppError> {
    Ok(suites::profiles())
}

/// `suite_run`.
#[tauri::command]
pub async fn suite_run(
    state: State<'_, AppState>,
    req: Value,
    on_progress: Channel<SuiteProgress>,
) -> Result<SuiteResult, AppError> {
    tracing::debug!("command: suite_run");
    let req: SuiteRunReq = parse(req)?;
    let sink: SuiteProgressSink = Arc::new(move |p: SuiteProgress| {
        let _ = on_progress.send(p);
    });
    let suite = SuiteRequest {
        suite_id: req.suite_id,
        profile: req.profile,
        peer: req.peer,
        overrides: Vec::new(),
    };
    Ok(state.suite(suite, sink).await)
}

/// `workload_defaults`: every workload at its defaults.
#[tauri::command]
pub async fn workload_defaults() -> Result<Vec<WorkloadParams>, AppError> {
    Ok(WorkloadParams::defaults())
}

// ── history ────────────────────────────────────────────────────────────

/// `runs_list`, newest first.
#[tauri::command]
pub async fn runs_list(state: State<'_, AppState>) -> Result<Vec<RunSummary>, AppError> {
    tracing::debug!("command: runs_list");
    let hub = state.hub();
    blocking("list runs", move || Ok(hub.store().list())).await
}

/// `run_get`.
#[tauri::command]
pub async fn run_get(state: State<'_, AppState>, req: Value) -> Result<RunReport, AppError> {
    tracing::debug!("command: run_get");
    let req: RunIdReq = parse(req)?;
    let hub = state.hub();
    blocking("read a run", move || hub.store().get(&req.run_id)).await
}

/// `run_delete`.
#[tauri::command]
pub async fn run_delete(state: State<'_, AppState>, req: Value) -> Result<(), AppError> {
    tracing::debug!("command: run_delete");
    let req: RunIdReq = parse(req)?;
    let hub = state.hub();
    blocking("delete a run", move || hub.store().delete(&req.run_id)).await
}

/// `runs_import`: files, folders or `.zip` archives (History drops).
#[tauri::command]
pub async fn runs_import(state: State<'_, AppState>, req: Value) -> Result<ImportResult, AppError> {
    tracing::debug!("command: runs_import");
    let req: PathsReq = parse(req)?;
    state
        .import(req.paths.into_iter().map(PathBuf::from).collect())
        .await
}

/// `runs_import_pick`: the native file picker, from Rust (no JS permission).
#[tauri::command]
pub async fn runs_import_pick<R: Runtime>(
    app: AppHandle<R>,
    state: State<'_, AppState>,
) -> Result<ImportResult, AppError> {
    tracing::debug!("command: runs_import_pick");
    let (tx, rx) = tokio::sync::oneshot::channel();
    app.dialog()
        .file()
        .set_title("Import reports")
        .add_filter("Reports", &["json", "zip"])
        .pick_files(move |files| {
            let _ = tx.send(files);
        });
    let files = rx.await.ok().flatten().unwrap_or_default();
    let paths: Vec<PathBuf> = files
        .into_iter()
        .filter_map(|f| f.into_path().ok())
        .collect();
    if paths.is_empty() {
        return Ok(ImportResult::default());
    }
    state.import(paths).await
}

// ── settings ───────────────────────────────────────────────────────────

/// `settings_get`.
#[tauri::command]
pub async fn settings_get(state: State<'_, AppState>) -> Result<Settings, AppError> {
    tracing::debug!("command: settings_get");
    Ok(state.settings())
}

/// `settings_set`: persist and apply (theme on the window, other accounts at
/// once, the device name at the next start).
#[tauri::command]
pub async fn settings_set<R: Runtime>(
    app: AppHandle<R>,
    state: State<'_, AppState>,
    req: Value,
) -> Result<Settings, AppError> {
    tracing::debug!("command: settings_set");
    let settings: Settings = parse(req)?;
    let saved = state.update_settings(settings)?;
    window::apply_theme(&app, saved.theme);
    Ok(saved)
}
