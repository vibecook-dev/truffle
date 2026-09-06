//! Build script for truffle-core.
//!
//! Compiles the Go test-sidecar (`packages/sidecar-slim/`) into
//! `tests/test-sidecar` when the source tree is present and newer than the
//! output. Used by the real-Tailscale integration tests.
//!
//! Design notes (RFC 019 §8):
//!
//! - **Unconditional but smart.** Runs on every build, but detects whether
//!   work is needed via mtime. Fast no-op when the binary is fresh.
//! - **Source-absent = skip silently.** When truffle-core is consumed from
//!   crates.io, the `packages/sidecar-slim/` directory doesn't exist; we
//!   just return without error.
//! - **Go-missing = warn, don't fail.** If Go isn't installed, we emit a
//!   `cargo:warning` and let integration tests surface the missing binary
//!   with a clearer message. Library builds stay green.
//! - **No cargo feature gate.** Adding one would force every downstream
//!   consumer to opt in and break `cargo test` ergonomics. The source-absent
//!   check is enough to keep published crates untouched.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::SystemTime;

fn main() {
    let crate_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());
    let sidecar_src = crate_dir.join("../../packages/sidecar-slim");
    let output = crate_dir.join("tests/test-sidecar");

    // If the sidecar source tree isn't here (e.g. a published-crate build),
    // do nothing and watch for it to appear.
    if !sidecar_src.join("main.go").exists() {
        println!(
            "cargo:rerun-if-changed={}",
            sidecar_src.join("main.go").display()
        );
        return;
    }

    // Watch every input that can invalidate the output.
    for file in go_sources(&sidecar_src) {
        println!("cargo:rerun-if-changed={}", file.display());
    }
    for extra in ["go.mod", "go.sum"] {
        let p = sidecar_src.join(extra);
        if p.exists() {
            println!("cargo:rerun-if-changed={}", p.display());
        }
    }

    if !needs_rebuild(&output, &sidecar_src) {
        return;
    }

    if !has_go() {
        println!(
            "cargo:warning=truffle-core: test-sidecar is stale or missing and `go` \
             is not on PATH. Real-network integration tests will fail with a clear \
             message. Install the Go version in packages/sidecar-slim/go.mod to auto-build the sidecar."
        );
        return;
    }

    println!(
        "cargo:warning=truffle-core: building test-sidecar from {} (this takes ~10-30s the first time)",
        sidecar_src.display()
    );

    let status = Command::new("go")
        .arg("build")
        .arg("-o")
        .arg(&output)
        .arg(".")
        .current_dir(&sidecar_src)
        .status();

    match status {
        Ok(s) if s.success() => {
            println!(
                "cargo:warning=truffle-core: test-sidecar built at {}",
                output.display()
            );
        }
        Ok(s) => {
            println!(
                "cargo:warning=truffle-core: `go build` for test-sidecar failed (exit {}). \
                 Rebuild manually: cd packages/sidecar-slim && go build -o {} .",
                s,
                output.display()
            );
        }
        Err(e) => {
            println!(
                "cargo:warning=truffle-core: failed to invoke `go build`: {e}. \
                 Install the Go version in packages/sidecar-slim/go.mod or build the sidecar manually."
            );
        }
    }
}

fn go_sources(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return out;
    };
    for e in entries.flatten() {
        let p = e.path();
        if p.is_file() && p.extension().and_then(|s| s.to_str()) == Some("go") {
            out.push(p);
        }
    }
    out
}

fn mtime(path: &Path) -> Option<SystemTime> {
    std::fs::metadata(path).ok().and_then(|m| m.modified().ok())
}

fn newest_source_mtime(dir: &Path) -> Option<SystemTime> {
    let mut newest: Option<SystemTime> = None;
    let candidates = go_sources(dir).into_iter().chain(
        ["go.mod", "go.sum"]
            .iter()
            .map(|n| dir.join(n))
            .filter(|p| p.exists()),
    );
    for p in candidates {
        if let Some(t) = mtime(&p) {
            newest = Some(newest.map(|n| n.max(t)).unwrap_or(t));
        }
    }
    newest
}

fn needs_rebuild(output: &Path, sidecar_src: &Path) -> bool {
    match (mtime(output), newest_source_mtime(sidecar_src)) {
        (Some(out), Some(src)) => src > out,
        (None, _) => true,        // no output yet
        (Some(_), None) => false, // no sources? shouldn't happen, leave alone
    }
}

fn has_go() -> bool {
    Command::new("go")
        .arg("version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}
