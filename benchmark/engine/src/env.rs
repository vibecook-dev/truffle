//! Environment capture (RFC 027 §7.4 `NodeEnv`, §6 `PeerHello`).
//!
//! Expensive facts (the sidecar's SHA-256, the host fingerprint, git state)
//! are computed once per process and cached.

use std::collections::HashMap;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Mutex, OnceLock, PoisonError};
use std::time::SystemTime;

use sha2::{Digest, Sha256};

use crate::config::{os_hostname, Tool};
use crate::types::{BuildProfile, NodeEnv, PeerHello};
use crate::{PROTOCOL, TOOL_VERSION};

/// `rustc -V` of the compiler that built this crate (from `build.rs`), or
/// `"unknown"`.
pub const RUSTC: &str = env!("TRUFFLE_BENCH_RUSTC");

/// [`RUSTC`], `None` when unknown.
pub fn rustc_version() -> Option<String> {
    (RUSTC != "unknown" && !RUSTC.is_empty()).then(|| RUSTC.to_string())
}

/// Logical CPUs available to this process.
pub fn logical_cpus() -> Option<u32> {
    std::thread::available_parallelism()
        .ok()
        .map(|n| n.get() as u32)
}

/// Git provenance of the running build.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GitInfo {
    /// `HEAD` commit.
    pub sha: Option<String>,
    /// Uncommitted changes to tracked files.
    pub dirty: Option<bool>,
}

/// Git provenance, cached per process (§7.4):
///
/// - CLI: `git -C <this crate's source dir>` at run time, never the current
///   directory, so a run started elsewhere still names the engine's commit;
/// - App: the build-time `TRUFFLE_BENCH_GIT_SHA` / `TRUFFLE_BENCH_GIT_DIRTY`
///   that `app:build` sets, otherwise unknown.
pub fn git_info(tool: Tool) -> GitInfo {
    static CLI: OnceLock<GitInfo> = OnceLock::new();
    match tool {
        Tool::Cli => CLI
            .get_or_init(|| {
                let dir = env!("CARGO_MANIFEST_DIR");
                GitInfo {
                    sha: git(dir, &["rev-parse", "HEAD"]).filter(|s| !s.is_empty()),
                    dirty: git(dir, &["status", "--porcelain", "--untracked-files=no"])
                        .map(|s| !s.is_empty()),
                }
            })
            .clone(),
        Tool::App => GitInfo {
            sha: option_env!("TRUFFLE_BENCH_GIT_SHA")
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string),
            dirty: option_env!("TRUFFLE_BENCH_GIT_DIRTY").and_then(parse_bool),
        },
    }
}

fn parse_bool(s: &str) -> Option<bool> {
    match s.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" => Some(true),
        "0" | "false" | "no" => Some(false),
        _ => None,
    }
}

fn git(dir: &str, args: &[&str]) -> Option<String> {
    let out = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .output()
        .ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).trim().to_string())
}

/// §7.4 host fingerprint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostFingerprint {
    /// First 16 hex characters of the hash.
    pub fingerprint: String,
    /// No machine id was available; only the hostname was hashed.
    pub weak: bool,
}

/// `sha256(machine_id ‖ hostname)`, first 16 hex; without a machine id,
/// `sha256(hostname)` and `weak = true`.
pub fn fingerprint_from(machine_id: Option<&str>, hostname: &str) -> HostFingerprint {
    let machine_id = machine_id.map(str::trim).filter(|m| !m.is_empty());
    let mut h = Sha256::new();
    if let Some(id) = machine_id {
        h.update(id.as_bytes());
    }
    h.update(hostname.as_bytes());
    let mut hex = hex::encode(h.finalize());
    hex.truncate(16);
    HostFingerprint {
        fingerprint: hex,
        weak: machine_id.is_none(),
    }
}

/// This machine's fingerprint (via `machine-uid`), cached.
pub fn host_fingerprint() -> &'static HostFingerprint {
    static FP: OnceLock<HostFingerprint> = OnceLock::new();
    FP.get_or_init(|| {
        let id = machine_uid::get().ok();
        fingerprint_from(id.as_deref(), &os_hostname())
    })
}

/// Same host iff both fingerprints are known, equal, and neither is weak
/// (§7.4 topology).
pub fn same_host(a: Option<&str>, a_weak: bool, b: Option<&str>, b_weak: bool) -> bool {
    match (a, b) {
        (Some(a), Some(b)) => !a_weak && !b_weak && a == b,
        _ => false,
    }
}

/// What two hosts' fingerprints say about sharing a machine (§7.4, the
/// `BenchPeer.same_host` flag): `Some(true)` iff both are strong and equal;
/// `Some(false)` iff both are strong and differ, or both are weak (hostname
/// only) and differ, since a weak one is a hash of the hostname and one
/// host has one hostname; `None` when either is unknown, both are weak and
/// equal (two hosts can share a hostname), or one is weak and the other
/// strong (hashes of different inputs never match).
pub fn same_host_verdict(
    a: Option<&str>,
    a_weak: bool,
    b: Option<&str>,
    b_weak: bool,
) -> Option<bool> {
    let (a, b) = (a?, b?);
    match (a_weak, b_weak) {
        (false, false) => Some(a == b),
        (true, true) => (a != b).then_some(false),
        _ => None,
    }
}

/// Whether the machine runs on battery: macOS `pmset -g batt`; `None`
/// elsewhere or when it cannot be read.
pub fn on_battery() -> Option<bool> {
    if !cfg!(target_os = "macos") {
        return None;
    }
    let out = Command::new("pmset").args(["-g", "batt"]).output().ok()?;
    if !out.status.success() {
        return None;
    }
    parse_pmset(&String::from_utf8_lossy(&out.stdout))
}

/// Parse `pmset -g batt`: "Now drawing from 'AC Power'" → false,
/// 'Battery Power' or 'UPS Power' → true.
pub fn parse_pmset(output: &str) -> Option<bool> {
    if output.contains("'AC Power'") {
        Some(false)
    } else if output.contains("'Battery Power'") || output.contains("'UPS Power'") {
        Some(true)
    } else {
        None
    }
}

/// SHA-256 (hex) of the sidecar binary, cached per path and (size, mtime).
/// `None` when the file cannot be read. Blocking on first use per file.
pub fn sidecar_sha256(path: &Path) -> Option<String> {
    type Key = (u64, Option<SystemTime>);
    static CACHE: OnceLock<Mutex<HashMap<PathBuf, (Key, String)>>> = OnceLock::new();
    let meta = std::fs::metadata(path).ok()?;
    let key: Key = (meta.len(), meta.modified().ok());
    let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    if let Some((k, hash)) = cache
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .get(path)
    {
        if *k == key {
            return Some(hash.clone());
        }
    }
    let hash = sha256_file(path)?;
    cache
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .insert(path.to_path_buf(), (key, hash.clone()));
    Some(hash)
}

fn sha256_file(path: &Path) -> Option<String> {
    let mut file = std::fs::File::open(path).ok()?;
    let mut h = Sha256::new();
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = file.read(&mut buf).ok()?;
        if n == 0 {
            break;
        }
        h.update(&buf[..n]);
    }
    Some(hex::encode(h.finalize()))
}

/// This side's [`NodeEnv`]. `app_foreground_pct` is left `None` (the app
/// fills it).
///
/// Blocking: it may run `git` (CLI, once per process) and `pmset` (macOS),
/// and hashes the sidecar on first use; call it from `spawn_blocking` in
/// async code.
pub fn node_env(
    tool: Tool,
    device_name: &str,
    sidecar_path: Option<&Path>,
    tailscale_ip: Option<String>,
) -> NodeEnv {
    let git = git_info(tool);
    let fp = host_fingerprint();
    NodeEnv {
        device_name: device_name.to_string(),
        os: std::env::consts::OS.to_string(),
        arch: std::env::consts::ARCH.to_string(),
        logical_cpus: logical_cpus(),
        tool: tool.app_name().to_string(),
        tool_version: TOOL_VERSION.to_string(),
        truffle_version: truffle_core::VERSION.to_string(),
        build_profile: BuildProfile::current(),
        rustc: rustc_version(),
        git_sha: git.sha,
        git_dirty: git.dirty,
        sidecar_path: sidecar_path.map(|p| p.display().to_string()),
        sidecar_sha256: sidecar_path.and_then(sidecar_sha256),
        tailscale_ip,
        host_fingerprint: Some(fp.fingerprint.clone()),
        host_fingerprint_weak: fp.weak,
        on_battery: on_battery(),
        app_foreground_pct: None,
    }
}

/// This hub's hello, minus what only the hub knows per asking peer:
/// `accepts_runs_from_you` starts false and `busy` empty, and
/// `host_fingerprint` is filled in — the responder must clear it for peers
/// it does not admit (§6 `hello`).
///
/// Blocking on first use, like [`node_env`]; build it once at hub start and
/// clone it per reply.
pub fn local_hello(
    tool: Tool,
    device_name: &str,
    device_id: Option<String>,
    login_name: Option<String>,
    sidecar_path: Option<&Path>,
    workloads: Vec<String>,
) -> PeerHello {
    let fp = host_fingerprint();
    PeerHello {
        protocol: PROTOCOL,
        app: tool.app_name().to_string(),
        tool_version: TOOL_VERSION.to_string(),
        truffle_version: truffle_core::VERSION.to_string(),
        build_profile: BuildProfile::current(),
        git_sha: git_info(tool).sha,
        sidecar_sha256: sidecar_path.and_then(sidecar_sha256),
        device_name: device_name.to_string(),
        device_id,
        os: std::env::consts::OS.to_string(),
        arch: std::env::consts::ARCH.to_string(),
        logical_cpus: logical_cpus(),
        login_name,
        host_fingerprint: Some(fp.fingerprint.clone()),
        host_fingerprint_weak: fp.weak,
        workloads,
        accepts_runs_from_you: false,
        busy: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_host_verdicts() {
        let v = same_host_verdict;
        assert_eq!(v(Some("a"), false, Some("a"), false), Some(true));
        assert_eq!(v(Some("a"), false, Some("b"), false), Some(false));
        assert_eq!(v(Some("a"), true, Some("b"), true), Some(false));
        assert_eq!(v(Some("a"), true, Some("a"), true), None);
        assert_eq!(v(Some("a"), true, Some("a"), false), None);
        assert_eq!(v(None, false, Some("a"), false), None);
        assert_eq!(v(Some("a"), false, None, false), None);
    }

    #[test]
    fn fingerprint_mixes_machine_id_and_hostname() {
        let a = fingerprint_from(Some("UUID-1"), "mac");
        assert_eq!(a.fingerprint.len(), 16);
        assert!(!a.weak);
        assert_ne!(a, fingerprint_from(Some("UUID-2"), "mac"));
        assert_ne!(a, fingerprint_from(Some("UUID-1"), "mac2"));
        let expected = {
            let mut h = Sha256::new();
            h.update(b"UUID-1mac");
            hex::encode(h.finalize())[..16].to_string()
        };
        assert_eq!(a.fingerprint, expected);
        let weak = fingerprint_from(None, "mac");
        assert!(weak.weak);
        assert_eq!(weak, fingerprint_from(Some("  "), "mac"));
    }

    #[test]
    fn same_host_needs_strong_equal_fingerprints() {
        assert!(same_host(Some("ab"), false, Some("ab"), false));
        assert!(!same_host(Some("ab"), true, Some("ab"), false));
        assert!(!same_host(Some("ab"), false, Some("cd"), false));
        assert!(!same_host(None, false, Some("ab"), false));
    }

    #[test]
    fn pmset_parsing() {
        let ac = "Now drawing from 'AC Power'\n -InternalBattery-0 (id=1)\t100%; charged; present: true\n";
        let batt =
            "Now drawing from 'Battery Power'\n -InternalBattery-0 (id=1)\t80%; discharging;\n";
        assert_eq!(parse_pmset(ac), Some(false));
        assert_eq!(parse_pmset(batt), Some(true));
        assert_eq!(parse_pmset("Now drawing from 'UPS Power'"), Some(true));
        assert_eq!(parse_pmset(""), None);
        assert_eq!(parse_bool(" TRUE "), Some(true));
        assert_eq!(parse_bool("0"), Some(false));
        assert_eq!(parse_bool("maybe"), None);
    }

    #[test]
    fn sidecar_hash_is_cached_and_tracks_changes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sidecar");
        std::fs::write(&path, b"abc").unwrap();
        let h = sidecar_sha256(&path).unwrap();
        assert_eq!(
            h,
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(sidecar_sha256(&path).unwrap(), h);
        std::fs::write(&path, b"abcd").unwrap();
        assert_ne!(sidecar_sha256(&path).unwrap(), h);
        assert_eq!(sidecar_sha256(&dir.path().join("missing")), None);
    }

    #[test]
    fn env_and_hello_describe_this_build() {
        let env = node_env(Tool::Cli, "mac-cli", None, Some("100.64.0.1".into()));
        assert_eq!(env.tool, "truffle-bench-cli");
        assert_eq!(env.tool_version, TOOL_VERSION);
        assert_eq!(env.truffle_version, truffle_core::VERSION);
        assert_eq!(env.build_profile, BuildProfile::current());
        assert_eq!(env.os, std::env::consts::OS);
        assert!(env.host_fingerprint.is_some());
        assert_eq!(env.sidecar_sha256, None);
        if !cfg!(target_os = "macos") {
            assert_eq!(env.on_battery, None);
        }
        let hello = local_hello(Tool::App, "mac", None, None, None, vec!["tcp".into()]);
        assert_eq!(hello.protocol, PROTOCOL);
        assert_eq!(hello.app, "truffle-bench-app");
        assert!(!hello.accepts_runs_from_you);
        assert_eq!(hello.workloads, vec!["tcp".to_string()]);
        let peer = NodeEnv::from_hello(&hello, None);
        assert_eq!(peer.tool, "truffle-bench-app");
        assert_eq!(peer.host_fingerprint, hello.host_fingerprint);
        assert!(!RUSTC.is_empty());
    }
}
