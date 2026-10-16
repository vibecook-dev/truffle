//! Command-level tests on Tauri's mock runtime: each command is invoked
//! through the IPC layer exactly as `invoke(cmd, { req })` would, and the
//! answers are checked against the engine's schema fixtures
//! (`benchmark/schema/fixtures/`).

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};
use tauri::ipc::{CallbackFn, InvokeBody};
use tauri::test::{
    get_ipc_response, mock_builder, mock_context, noop_assets, MockRuntime, INVOKE_KEY,
};
use tauri::webview::InvokeRequest;
use tauri::{App, WebviewWindow, WebviewWindowBuilder};
use truffle_bench::{HubEvent, Workload};

use crate::commands;
use crate::state::{AppState, StateInit};

fn fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../schema/fixtures")
}

fn fixture(name: &str) -> Value {
    let path = fixtures_dir().join(name);
    let bytes = std::fs::read(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    serde_json::from_slice(&bytes).unwrap()
}

fn keys(v: &Value) -> BTreeSet<String> {
    v.as_object().expect("an object").keys().cloned().collect()
}

struct TestApp {
    _app: App<MockRuntime>,
    window: WebviewWindow<MockRuntime>,
    state: AppState,
    events: Arc<Mutex<Vec<HubEvent>>>,
}

fn state_in(dir: &Path, events: Arc<Mutex<Vec<HubEvent>>>) -> AppState {
    AppState::new(StateInit {
        profile: Some("test".into()),
        data_dir: dir.to_path_buf(),
        bundled: false,
        env_auth_key: None,
        emit: Arc::new(move |e| events.lock().unwrap().push(e.clone())),
    })
}

fn app_in(dir: &Path) -> TestApp {
    let events = Arc::new(Mutex::new(Vec::new()));
    let state = state_in(dir, events.clone());
    let app = mock_builder()
        .manage(state.clone())
        .invoke_handler(commands::handler())
        .build(mock_context(noop_assets()))
        .expect("mock app");
    let window = WebviewWindowBuilder::new(&app, crate::window::MAIN, Default::default())
        .build()
        .expect("mock window");
    TestApp {
        _app: app,
        window,
        state,
        events,
    }
}

impl TestApp {
    /// Invoke `cmd` with `args` (the object JS passes to `invoke`).
    fn invoke(&self, cmd: &str, args: Value) -> Result<Value, Value> {
        get_ipc_response(
            &self.window,
            InvokeRequest {
                cmd: cmd.into(),
                callback: CallbackFn(0),
                error: CallbackFn(1),
                url: "http://tauri.localhost".parse().unwrap(),
                body: InvokeBody::Json(args),
                headers: Default::default(),
                invoke_key: INVOKE_KEY.to_string(),
            },
        )
        .map(|b| b.deserialize::<Value>().unwrap())
    }

    fn ok(&self, cmd: &str, args: Value) -> Value {
        self.invoke(cmd, args)
            .unwrap_or_else(|e| panic!("{cmd} failed: {e}"))
    }

    fn err(&self, cmd: &str, args: Value) -> Value {
        match self.invoke(cmd, args) {
            Ok(v) => panic!("{cmd} succeeded with {v}"),
            Err(e) => e,
        }
    }
}

/// Every rejection is exactly `{code, message}` with a kebab-case code.
fn assert_app_error(e: &Value, code: &str) {
    assert_eq!(
        keys(e),
        BTreeSet::from(["code".to_string(), "message".to_string()]),
        "{e}"
    );
    assert_eq!(e["code"], code, "{e}");
    assert!(!e["message"].as_str().unwrap().is_empty());
    assert_eq!(keys(&fixture("app-error.json")), keys(e));
}

#[test]
fn app_info_matches_its_fixture() {
    let dir = tempfile::tempdir().unwrap();
    let t = app_in(dir.path());
    let info = t.ok("app_info", json!({}));
    assert_eq!(keys(&info), keys(&fixture("app-info.json")));
    assert_eq!(info["profile"], "test");
    assert_eq!(info["data_dir"], dir.path().display().to_string());
    assert_eq!(info["app_version"], truffle_bench::TOOL_VERSION);
    assert_eq!(info["truffle_version"], truffle_core::VERSION);
    assert_eq!(info["os"], std::env::consts::OS);
    assert_eq!(info["build_profile"], "debug");
}

#[test]
fn hub_state_is_idle_with_a_hostname() {
    let dir = tempfile::tempdir().unwrap();
    let t = app_in(dir.path());
    let st = t.ok("hub_state", json!({}));
    assert_eq!(keys(&st), keys(&fixture("node-state-idle.json")));
    assert_eq!(st["state"], "idle");
    assert!(st["tailscale_hostname"]
        .as_str()
        .unwrap()
        .starts_with("truffle-truffle-bench-"));
    // Not running: no peers, no runs.
    assert_eq!(t.ok("peers_list", json!({})), json!([]));
    assert_eq!(t.ok("runs_active", json!({})), json!([]));
    assert_eq!(
        t.ok("bench_cancel", json!({"req": {"id": "nope"}})),
        json!(false)
    );
    assert_eq!(
        t.ok(
            "run_attach",
            json!({"req": {"run_id": "nope"}, "onProgress": "__CHANNEL__:1"})
        ),
        json!(false)
    );
}

#[test]
fn suite_profiles_and_workload_defaults() {
    let dir = tempfile::tempdir().unwrap();
    let t = app_in(dir.path());
    let profiles = t.ok("suite_profiles", json!({}));
    let ids: Vec<&str> = profiles
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["smoke", "standard", "full", "nightly"]);
    assert_eq!(
        profiles,
        serde_json::to_value(truffle_bench::suites::profiles()).unwrap()
    );
    for p in profiles.as_array().unwrap() {
        assert_eq!(keys(p), keys(&fixture("suite-profile-smoke.json")));
    }

    let defaults = t.ok("workload_defaults", json!({}));
    let names: Vec<&str> = defaults
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["workload"].as_str().unwrap())
        .collect();
    let all: Vec<&str> = Workload::ALL.iter().map(|w| w.as_str()).collect();
    assert_eq!(names, all);
    assert_eq!(defaults[0], fixture("params-path.json"));
}

#[test]
fn settings_round_trip_and_persist() {
    let dir = tempfile::tempdir().unwrap();
    let t = app_in(dir.path());
    let first = t.ok("settings_get", json!({}));
    assert_eq!(keys(&first), keys(&fixture("settings.json")));
    assert!(first["device_name"].as_str().unwrap().ends_with("-test"));
    assert_eq!(first["theme"], "system");
    assert_eq!(first["allow_other_logins"], false);

    let wanted = json!({
        "device_name": "  studio-test ",
        "allow_other_logins": true,
        "theme": "dark",
        "target_peer": "01JA7QB2M8H4D6F9K1N3Q5S7U9",
    });
    let saved = t.ok("settings_set", json!({ "req": wanted }));
    assert_eq!(saved["device_name"], "studio-test");
    assert_eq!(saved["theme"], "dark");
    assert_eq!(t.ok("settings_get", json!({})), saved);

    // The idle hub took the new device name, and the UI heard about it.
    assert_eq!(t.state.hub().config().device_name, "studio-test");
    assert!(t.state.hub().config().allow_other_logins);
    assert!(t
        .events
        .lock()
        .unwrap()
        .iter()
        .any(|e| matches!(e, HubEvent::Node(_))));

    // Persisted: a new state on the same directory loads it.
    let reloaded = state_in(dir.path(), Arc::new(Mutex::new(Vec::new())));
    assert_eq!(serde_json::to_value(reloaded.settings()).unwrap(), saved);
    let mut on_disk: Value =
        serde_json::from_slice(&std::fs::read(dir.path().join("settings.json")).unwrap()).unwrap();
    // The app's own state key rides along; the UI never sees it.
    let key = on_disk.as_object_mut().unwrap().remove("state_key");
    assert!(key.is_some_and(|k| k.as_str().is_some_and(|k| !k.is_empty())));
    assert_eq!(on_disk, saved);

    // Bad settings are refused with an AppError and change nothing.
    let e = t.err(
        "settings_set",
        json!({"req": {"device_name": " ", "allow_other_logins": false, "theme": "light", "target_peer": null}}),
    );
    assert_app_error(&e, "invalid-params");
    let e = t.err(
        "settings_set",
        json!({"req": {"device_name": "x", "allow_other_logins": false, "theme": "purple", "target_peer": null}}),
    );
    assert_app_error(&e, "invalid-params");
    assert_eq!(t.ok("settings_get", json!({})), saved);
}

#[test]
fn runs_import_list_get_delete_against_fixtures() {
    let dir = tempfile::tempdir().unwrap();
    let t = app_in(dir.path());
    let reports: Vec<PathBuf> = std::fs::read_dir(fixtures_dir())
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .is_some_and(|n| n.to_string_lossy().starts_with("report-"))
        })
        .collect();
    assert!(!reports.is_empty());
    let paths: Vec<String> = reports.iter().map(|p| p.display().to_string()).collect();

    let r = t.ok("runs_import", json!({"req": {"paths": paths}}));
    assert_eq!(keys(&r), keys(&fixture("import-result.json")));
    let imported = r["imported"].as_u64().unwrap() as usize;
    assert_eq!(
        imported + r["skipped"].as_u64().unwrap() as usize,
        reports.len()
    );
    assert!(imported > 0, "{r}");

    let list = t.ok("runs_list", json!({}));
    let list = list.as_array().unwrap();
    assert_eq!(list.len(), imported);
    for row in list {
        assert_eq!(keys(row), keys(&fixture("run-summary.json")), "{row}");
    }

    // Importing again skips every report.
    let again = t.ok("runs_import", json!({"req": {"paths": paths}}));
    assert_eq!(again["imported"], 0);

    // Old-format reports import too (a folder, normalised to v3).
    let v1 = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../engine/tests/fixtures/v1");
    let r = t.ok(
        "runs_import",
        json!({"req": {"paths": [v1.display().to_string()]}}),
    );
    assert!(r["imported"].as_u64().unwrap() > 0, "{r}");

    let id = list[0]["run_id"].as_str().unwrap().to_string();
    let report = t.ok("run_get", json!({"req": {"run_id": id}}));
    assert_eq!(report["run_id"], id.as_str());
    assert_eq!(report["schema_version"], 3);
    assert_eq!(keys(&report), keys(&fixture("report-tcp-ok.json")));

    assert_eq!(
        t.ok("run_delete", json!({"req": {"run_id": id}})),
        Value::Null
    );
    let e = t.err("run_get", json!({"req": {"run_id": id}}));
    assert_app_error(&e, "invalid-params");
    let e = t.err("run_get", json!({"req": {"run_id": "../escape"}}));
    assert_app_error(&e, "invalid-params");
}

#[test]
fn rejections_are_app_errors() {
    let dir = tempfile::tempdir().unwrap();
    let t = app_in(dir.path());
    // Not running.
    let e = t.err("peer_probe", json!({"req": {"peer": "laptop"}}));
    assert_app_error(&e, "not-running");
    // Malformed requests.
    let e = t.err(
        "bench_run",
        json!({"req": {"run_id": "r1", "peer": "p", "params": {"workload": "tcp", "bogus": 1}},
               "onProgress": "__CHANNEL__:2"}),
    );
    assert_app_error(&e, "invalid-params");
    let e = t.err("peer_probe", json!({"req": {}}));
    assert_app_error(&e, "invalid-params");
    let e = t.err(
        "hub_start",
        json!({"req": {"device_name": "x", "extra": true}}),
    );
    assert_app_error(&e, "invalid-params");
    // Only https links open.
    let e = t.err("open_url", json!({"req": {"url": "http://example.com"}}));
    assert_app_error(&e, "invalid-params");
    let e = t.err("open_url", json!({"req": {"url": "not a url"}}));
    assert_app_error(&e, "invalid-params");
}

#[test]
fn a_run_without_a_hub_is_a_failed_report() {
    let dir = tempfile::tempdir().unwrap();
    let t = app_in(dir.path());
    let report = t.ok(
        "bench_run",
        json!({"req": {"run_id": "r-idle", "peer": "laptop", "params": {"workload": "path"}},
               "onProgress": "__CHANNEL__:3"}),
    );
    assert_eq!(report["status"], "failed");
    assert_eq!(report["error_code"], "not-running");
    assert_eq!(report["tool"], "truffle-bench-app");
    assert_eq!(keys(&report), keys(&fixture("report-path-failed.json")));
    // The activity was held only while the run lasted.
    assert!(!t.state.activity().is_held());
}
