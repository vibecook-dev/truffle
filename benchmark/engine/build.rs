//! Embeds `rustc -V` as `TRUFFLE_BENCH_RUSTC` (RFC 027 §4) for `NodeEnv.rustc`.
//! Falls back to `"unknown"` when the compiler cannot be asked.

use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=RUSTC");
    // The app's `app:build` sets these for `env::git_info(Tool::App)`.
    println!("cargo:rerun-if-env-changed=TRUFFLE_BENCH_GIT_SHA");
    println!("cargo:rerun-if-env-changed=TRUFFLE_BENCH_GIT_DIRTY");

    let rustc = std::env::var_os("RUSTC").unwrap_or_else(|| "rustc".into());
    let version = Command::new(rustc)
        .arg("-V")
        .output()
        .ok()
        .filter(|out| out.status.success())
        .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_string())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=TRUFFLE_BENCH_RUSTC={version}");
}
