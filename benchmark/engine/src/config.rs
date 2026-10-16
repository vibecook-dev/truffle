//! Hub configuration and sidecar resolution (RFC 027 §5.3).

use std::ffi::OsString;
use std::fmt;
use std::path::{Path, PathBuf};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::error::{AppError, ErrorCode};
use crate::types::ReportTool;
use crate::APP_ID;

/// The dev sidecar `crates/truffle-core/build.rs` builds, as a compile-time
/// path. Used only if it exists (§5.3 step 5).
pub const DEV_SIDECAR_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../crates/truffle-core/tests/test-sidecar"
);

/// Env var naming a test sidecar (§5.3 step 2).
pub const ENV_TEST_SIDECAR_PATH: &str = "TRUFFLE_TEST_SIDECAR_PATH";
/// Env var naming a sidecar (§5.3 step 3).
pub const ENV_SIDECAR_PATH: &str = "TRUFFLE_SIDECAR_PATH";
/// File name of the sidecar bundled next to the app executable, before
/// [`std::env::consts::EXE_SUFFIX`].
pub const BUNDLED_SIDECAR_NAME: &str = "sidecar-slim";
/// Budget for the state-directory slug.
const STATE_SLUG_BUDGET: usize = 63;

/// Which front end drives the engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum Tool {
    /// The desktop app.
    App,
    /// The `truffle-bench` CLI.
    Cli,
}

impl Tool {
    /// `"truffle-bench-app"` or `"truffle-bench-cli"` (`PeerHello.app`, `NodeEnv.tool`).
    pub fn app_name(self) -> &'static str {
        match self {
            Self::App => "truffle-bench-app",
            Self::Cli => "truffle-bench-cli",
        }
    }

    /// The report's `tool`.
    pub fn report_tool(self) -> ReportTool {
        match self {
            Self::App => ReportTool::TruffleBenchApp,
            Self::Cli => ReportTool::TruffleBenchCli,
        }
    }
}

/// Everything a hub needs to build its node.
///
/// `Debug` redacts `auth_key`.
#[derive(Clone)]
pub struct HubConfig {
    /// Human device name; the Tailscale hostname is derived from it.
    pub device_name: String,
    /// tsnet state (keys, device id). Stable across runs, so login happens once.
    pub state_dir: PathBuf,
    /// Runs, temp files.
    pub data_dir: PathBuf,
    /// Explicit sidecar binary (§5.3 step 1).
    pub sidecar_path: Option<PathBuf>,
    /// Headless login key. Never logged, persisted or written to reports.
    pub auth_key: Option<String>,
    /// Remove the node from the tailnet when it goes offline (`pair` only).
    pub ephemeral: bool,
    /// truffle app id; [`APP_ID`] except per-run ids in `pair`.
    pub app_id: String,
    /// Admit peers signed in with other accounts (§6 rule 1).
    pub allow_other_logins: bool,
    /// Admit tagged peers when this hub is tagged too.
    pub allow_tagged: bool,
    /// App or CLI.
    pub tool: Tool,
    /// True only inside a packaged app (Tauri `cfg(not(dev))`).
    pub bundled: bool,
}

impl fmt::Debug for HubConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HubConfig")
            .field("device_name", &self.device_name)
            .field("state_dir", &self.state_dir)
            .field("data_dir", &self.data_dir)
            .field("sidecar_path", &self.sidecar_path)
            .field("auth_key", &self.auth_key.as_ref().map(|_| "[REDACTED]"))
            .field("ephemeral", &self.ephemeral)
            .field("app_id", &self.app_id)
            .field("allow_other_logins", &self.allow_other_logins)
            .field("allow_tagged", &self.allow_tagged)
            .field("tool", &self.tool)
            .field("bundled", &self.bundled)
            .finish()
    }
}

impl HubConfig {
    /// Defaults (§5.3):
    ///
    /// - `device_name`: the OS hostname (app) or `<hostname>-cli` (CLI), plus
    ///   `-<profile>` when a profile is set;
    /// - `state_dir`: `<data_dir>/state/<slug(device_name)>`;
    /// - no auth key, so `allow_tagged` is false; not ephemeral; [`APP_ID`].
    ///
    /// The CLI passes [`cli_data_dir`]; the app passes its Tauri app-data dir
    /// with `-<profile>` already appended.
    pub fn defaults(tool: Tool, profile: Option<&str>, data_dir: PathBuf) -> Self {
        let host = os_hostname();
        let mut device_name = match tool {
            Tool::App => host,
            Tool::Cli => format!("{host}-cli"),
        };
        if let Some(profile) = profile.filter(|p| !p.is_empty()) {
            device_name.push('-');
            device_name.push_str(profile);
        }
        let state_dir = state_dir_for(&data_dir, &device_name);
        Self {
            device_name,
            state_dir,
            data_dir,
            sidecar_path: None,
            auth_key: None,
            ephemeral: false,
            app_id: APP_ID.to_string(),
            allow_other_logins: false,
            allow_tagged: false,
            tool,
            bundled: false,
        }
    }

    /// Set the device name and move `state_dir` to its default for that name.
    /// Set a custom `state_dir` after this, not before.
    #[must_use]
    pub fn with_device_name(mut self, name: impl Into<String>) -> Self {
        self.device_name = name.into();
        self.state_dir = state_dir_for(&self.data_dir, &self.device_name);
        self
    }

    /// Set the auth key; a key makes this a tagged device, so `allow_tagged`
    /// becomes true (override it afterwards if needed).
    #[must_use]
    pub fn with_auth_key(mut self, key: impl Into<String>) -> Self {
        self.auth_key = Some(key.into());
        self.allow_tagged = true;
        self
    }

    /// `<data_dir>/runs`, where [`RunStore`](crate::report::RunStore) keeps reports.
    pub fn runs_dir(&self) -> PathBuf {
        self.data_dir.join("runs")
    }

    /// `<data_dir>/tmp`, the parent of per-run `in-…`/`out-…` directories.
    pub fn tmp_dir(&self) -> PathBuf {
        self.data_dir.join("tmp")
    }
}

/// `<data_dir>/state/<slug(device_name)>`.
pub fn state_dir_for(data_dir: &Path, device_name: &str) -> PathBuf {
    data_dir
        .join("state")
        .join(truffle_core::slug(device_name, STATE_SLUG_BUDGET))
}

/// The CLI's data directory: `dirs::data_dir()/truffle-bench-cli`
/// (the system temp dir if the platform has no data dir).
pub fn cli_data_dir() -> PathBuf {
    dirs::data_dir()
        .unwrap_or_else(std::env::temp_dir)
        .join("truffle-bench-cli")
}

/// The OS hostname, or `truffle-bench` if it cannot be read.
pub fn os_hostname() -> String {
    hostname::get()
        .ok()
        .map(|h| h.to_string_lossy().trim().to_string())
        .filter(|h| !h.is_empty())
        .unwrap_or_else(|| "truffle-bench".to_string())
}

/// Resolve the sidecar for a hub (§5.3), reading the process environment.
pub fn resolve_sidecar(cfg: &HubConfig) -> Result<PathBuf, AppError> {
    let exe_dir = std::env::current_exe()
        .ok()
        .and_then(|exe| exe.parent().map(Path::to_path_buf));
    resolve_sidecar_with(
        cfg.sidecar_path.as_deref(),
        &|key| std::env::var_os(key),
        exe_dir.as_deref(),
        cfg.tool == Tool::App && cfg.bundled,
        Path::new(DEV_SIDECAR_PATH),
    )
}

/// Pure sidecar resolution (§5.3), in order:
///
/// 1. `explicit`;
/// 2. `TRUFFLE_TEST_SIDECAR_PATH`;
/// 3. `TRUFFLE_SIDECAR_PATH`;
/// 4. only when `bundled` (a packaged app): `<exe_dir>/sidecar-slim{EXE_SUFFIX}`;
/// 5. `dev_path`, if it exists;
/// 6. otherwise `sidecar-not-found`, listing every path tried.
///
/// A path given explicitly or through an env var must be a file; the search
/// does not fall past it, so a stale setting is reported rather than silently
/// replaced. The CLI never looks next to its own executable: a shared cargo
/// target dir can hold foreign copies. `~/.config/truffle/bin` is never used.
pub fn resolve_sidecar_with(
    explicit: Option<&Path>,
    env: &dyn Fn(&str) -> Option<OsString>,
    exe_dir: Option<&Path>,
    bundled: bool,
    dev_path: &Path,
) -> Result<PathBuf, AppError> {
    let not_a_file = |what: &str, path: &Path| {
        AppError::new(
            ErrorCode::SidecarNotFound,
            format!("{what} {} is not a file", path.display()),
        )
    };
    if let Some(path) = explicit {
        if path.is_file() {
            return Ok(path.to_path_buf());
        }
        return Err(not_a_file("the sidecar path", path));
    }
    for var in [ENV_TEST_SIDECAR_PATH, ENV_SIDECAR_PATH] {
        if let Some(value) = env(var).filter(|v| !v.is_empty()) {
            let path = PathBuf::from(value);
            if path.is_file() {
                return Ok(path);
            }
            return Err(not_a_file(&format!("{var} is set, but"), &path));
        }
    }
    let mut tried = Vec::new();
    if bundled {
        if let Some(dir) = exe_dir {
            let path = dir.join(format!(
                "{BUNDLED_SIDECAR_NAME}{}",
                std::env::consts::EXE_SUFFIX
            ));
            if path.is_file() {
                return Ok(path);
            }
            tried.push(path);
        }
    }
    if dev_path.is_file() {
        return Ok(dev_path.to_path_buf());
    }
    tried.push(dev_path.to_path_buf());
    let tried: Vec<String> = tried.iter().map(|p| p.display().to_string()).collect();
    Err(AppError::new(
        ErrorCode::SidecarNotFound,
        format!(
            "no sidecar found: {ENV_TEST_SIDECAR_PATH} and {ENV_SIDECAR_PATH} are unset, and none of these exist: {}. \
             Run `cargo build -p truffle-core` to build the dev sidecar, or set {ENV_SIDECAR_PATH}.",
            tried.join(", ")
        ),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    struct Fixture {
        dir: tempfile::TempDir,
    }

    impl Fixture {
        fn new() -> Self {
            Self {
                dir: tempfile::tempdir().unwrap(),
            }
        }
        fn file(&self, name: &str) -> PathBuf {
            let path = self.dir.path().join(name);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, b"#!/bin/sh\n").unwrap();
            path
        }
        fn missing(&self, name: &str) -> PathBuf {
            self.dir.path().join(name)
        }
    }

    fn env_of(vars: &[(&str, &Path)]) -> impl Fn(&str) -> Option<OsString> {
        let map: HashMap<String, OsString> = vars
            .iter()
            .map(|(k, v)| (k.to_string(), v.as_os_str().to_owned()))
            .collect();
        move |k| map.get(k).cloned()
    }

    fn no_env(_: &str) -> Option<OsString> {
        None
    }

    #[test]
    fn explicit_wins_over_everything() {
        let fx = Fixture::new();
        let explicit = fx.file("explicit/sidecar");
        let test = fx.file("test/sidecar");
        let env = env_of(&[(ENV_TEST_SIDECAR_PATH, &test)]);
        let dev = fx.file("dev/test-sidecar");
        let got = resolve_sidecar_with(Some(&explicit), &env, None, false, &dev).unwrap();
        assert_eq!(got, explicit);
        let err =
            resolve_sidecar_with(Some(&fx.missing("nope")), &env, None, false, &dev).unwrap_err();
        assert_eq!(err.code, ErrorCode::SidecarNotFound);
    }

    #[test]
    fn test_env_before_sidecar_env_before_dev() {
        let fx = Fixture::new();
        let test = fx.file("test/sidecar");
        let general = fx.file("general/sidecar");
        let dev = fx.file("dev/test-sidecar");
        let both = env_of(&[(ENV_TEST_SIDECAR_PATH, &test), (ENV_SIDECAR_PATH, &general)]);
        assert_eq!(
            resolve_sidecar_with(None, &both, None, false, &dev).unwrap(),
            test
        );
        let general_only = env_of(&[(ENV_SIDECAR_PATH, &general)]);
        assert_eq!(
            resolve_sidecar_with(None, &general_only, None, false, &dev).unwrap(),
            general
        );
        assert_eq!(
            resolve_sidecar_with(None, &no_env, None, false, &dev).unwrap(),
            dev
        );
    }

    #[test]
    fn env_pointing_nowhere_is_an_error_not_a_fallthrough() {
        let fx = Fixture::new();
        let dev = fx.file("dev/test-sidecar");
        let env = env_of(&[(ENV_SIDECAR_PATH, &fx.missing("gone"))]);
        let err = resolve_sidecar_with(None, &env, None, false, &dev).unwrap_err();
        assert_eq!(err.code, ErrorCode::SidecarNotFound);
        assert!(err.message.contains(ENV_SIDECAR_PATH));
        // An empty value counts as unset.
        let empty = |k: &str| (k == ENV_SIDECAR_PATH).then(OsString::new);
        assert_eq!(
            resolve_sidecar_with(None, &empty, None, false, &dev).unwrap(),
            dev
        );
    }

    #[test]
    fn exe_dir_only_when_bundled() {
        let fx = Fixture::new();
        let exe_dir = fx.dir.path().join("app");
        let bundled = fx.file(&format!(
            "app/{BUNDLED_SIDECAR_NAME}{}",
            std::env::consts::EXE_SUFFIX
        ));
        let dev = fx.file("dev/test-sidecar");
        assert_eq!(
            resolve_sidecar_with(None, &no_env, Some(&exe_dir), true, &dev).unwrap(),
            bundled
        );
        // The CLI (not bundled) never looks next to its executable.
        assert_eq!(
            resolve_sidecar_with(None, &no_env, Some(&exe_dir), false, &dev).unwrap(),
            dev
        );
    }

    #[test]
    fn nothing_found_lists_every_path_tried() {
        let fx = Fixture::new();
        let exe_dir = fx.dir.path().join("app");
        let dev = fx.missing("dev/test-sidecar");
        let err = resolve_sidecar_with(None, &no_env, Some(&exe_dir), true, &dev).unwrap_err();
        assert_eq!(err.code, ErrorCode::SidecarNotFound);
        assert!(
            err.message.contains(&dev.display().to_string()),
            "{}",
            err.message
        );
        assert!(
            err.message.contains(&exe_dir.display().to_string()),
            "{}",
            err.message
        );
    }

    #[test]
    fn defaults_follow_the_rfc() {
        let data = PathBuf::from("/data");
        let cli = HubConfig::defaults(Tool::Cli, None, data.clone());
        assert!(cli.device_name.ends_with("-cli"));
        assert_eq!(cli.state_dir, state_dir_for(&data, &cli.device_name));
        assert!(cli.state_dir.starts_with("/data/state"));
        assert_eq!(cli.app_id, APP_ID);
        assert!(!cli.ephemeral && !cli.allow_tagged && !cli.allow_other_logins);
        let app = HubConfig::defaults(Tool::App, Some("b"), data.clone());
        assert!(app.device_name.ends_with("-b"));
        assert!(!app.device_name.ends_with("-cli-b"));
        let keyed = app.with_auth_key("tskey-auth-…");
        assert!(keyed.allow_tagged);
        let debug = format!("{keyed:?}");
        assert!(!debug.contains("tskey"), "{debug}");
        let renamed = cli.with_device_name("Studio Mac");
        assert_eq!(renamed.state_dir, data.join("state").join("studio-mac"));
        assert_eq!(Tool::App.app_name(), "truffle-bench-app");
    }
}
