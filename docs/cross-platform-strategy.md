I now have a thorough understanding of the entire codebase. Here is the complete architecture report.

---

# Truffle CLI: Cross-Platform & Zero-Config Distribution Architecture

## Summary of Existing State

After reviewing all the referenced code, here is what exists today:

**Already working:**
- TS project has full cross-platform IPC (Unix sockets on macOS/Linux, named pipes on Windows) via `ipc-path-resolver.ts` and `ipc-service.ts`
- Go sidecar cross-compilation CI in `release-sidecar.yml` (5 platforms: darwin-arm64, darwin-x64, linux-x64, linux-arm64, win32-x64)
- NAPI build CI in `napi-build.yml` (same 5 targets plus tests)
- Platform-specific npm sidecar packages already exist under `/Users/jamesyong/Projects/project100/p008/truffle/npm/`
- `helpers.js` has working `resolveSidecarPath()` with platform resolution
- Rust CLI `sidecar.rs` has 7-location auto-discovery (already has `#[cfg(not(unix))]` for `is_executable`)
- `pid.rs` uses `libc::kill(pid, 0)` for process checking (Unix-only)
- `server.rs` uses `tokio::net::UnixListener` (Unix-only)
- `client.rs` uses `tokio::net::UnixStream` (Unix-only)

**Not yet cross-platform in Rust:**
- Daemon IPC is hard-coded to Unix sockets
- PID checking uses `libc::kill` (no Windows equivalent)
- Signal handling in server uses `tokio::signal::unix::signal(SIGTERM)`
- `which` binary lookup in `sidecar.rs` calls the `which` command (not available on Windows natively)
- Sidecar binary names don't account for `.exe` suffix on Windows

---

## 1. Recommended Architecture for Cross-Platform IPC

### Recommendation: Conditional compilation with a thin module, NOT a trait

A trait-based abstraction is over-engineering. The TS project proves the key insight: Unix sockets and Windows named pipes have **identical stream semantics** -- the only differences are (a) the path format and (b) the listener/stream types. Conditional compilation at the module level is cleaner and avoids dynamic dispatch.

The `tokio` crate already provides `tokio::net::windows::named_pipe` on Windows. This is the direct analog of `tokio::net::UnixListener`/`UnixStream`.

### Proposed Rust Code: `crates/truffle-cli/src/daemon/ipc.rs`

```rust
//! Cross-platform IPC transport for daemon <-> CLI communication.
//!
//! - Unix (macOS/Linux): Unix Domain Sockets via `tokio::net::UnixListener`
//! - Windows: Named Pipes via `tokio::net::windows::named_pipe`
//!
//! The module exposes a unified `IpcListener` and `IpcStream` that work
//! identically on all platforms. The wire protocol (newline-delimited JSON-RPC)
//! is transport-agnostic.

use std::path::PathBuf;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

// ═══════════════════════════════════════════════════════════════════════════
// Path resolution (mirrors ipc-path-resolver.ts from the TS project)
// ═══════════════════════════════════════════════════════════════════════════

const PIPE_NAME: &str = "truffle-daemon";

/// Get the IPC endpoint path for this platform.
///
/// - Unix:    `~/.config/truffle/truffle.sock`
/// - Windows: `\\.\pipe\truffle-daemon`
pub fn ipc_path() -> PathBuf {
    #[cfg(unix)]
    {
        crate::config::TruffleConfig::socket_path()
    }
    #[cfg(windows)]
    {
        PathBuf::from(format!(r"\\.\pipe\{PIPE_NAME}"))
    }
}

/// Returns true if the current platform uses named pipes (Windows).
#[allow(dead_code)]
pub fn is_named_pipe() -> bool {
    cfg!(windows)
}

// ═══════════════════════════════════════════════════════════════════════════
// Unix implementation
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(unix)]
mod platform {
    use super::*;
    use tokio::net::{UnixListener, UnixStream};

    pub struct IpcListener {
        inner: UnixListener,
    }

    impl IpcListener {
        pub fn bind(path: &std::path::Path) -> Result<Self, std::io::Error> {
            // Clean up stale socket file
            if path.exists() {
                std::fs::remove_file(path)?;
            }
            // Ensure parent directory exists
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let inner = UnixListener::bind(path)?;
            Ok(Self { inner })
        }

        pub async fn accept(&self) -> Result<IpcStream, std::io::Error> {
            let (stream, _addr) = self.inner.accept().await?;
            Ok(IpcStream { inner: stream })
        }
    }

    pub struct IpcStream {
        inner: UnixStream,
    }

    impl IpcStream {
        pub async fn connect(path: &std::path::Path) -> Result<Self, std::io::Error> {
            let inner = UnixStream::connect(path).await?;
            Ok(Self { inner })
        }

        pub fn into_split(self) -> (IpcReadHalf, IpcWriteHalf) {
            let (r, w) = self.inner.into_split();
            (
                IpcReadHalf { inner: Box::new(BufReader::new(r)) },
                IpcWriteHalf { inner: Box::new(w) },
            )
        }
    }

    // Type-erased read/write halves so the caller doesn't see platform types
    pub struct IpcReadHalf {
        inner: Box<BufReader<tokio::net::unix::OwnedReadHalf>>,
    }

    impl IpcReadHalf {
        pub async fn next_line(&mut self) -> Result<Option<String>, std::io::Error> {
            let mut line = String::new();
            match self.inner.read_line(&mut line).await? {
                0 => Ok(None),
                _ => {
                    // Remove trailing newline
                    if line.ends_with('\n') { line.pop(); }
                    if line.ends_with('\r') { line.pop(); }
                    Ok(Some(line))
                }
            }
        }
    }

    pub struct IpcWriteHalf {
        inner: Box<tokio::net::unix::OwnedWriteHalf>,
    }

    impl IpcWriteHalf {
        pub async fn write_line(&mut self, data: &str) -> Result<(), std::io::Error> {
            self.inner.write_all(data.as_bytes()).await?;
            self.inner.write_all(b"\n").await?;
            self.inner.flush().await
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Windows implementation
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(windows)]
mod platform {
    use super::*;
    use tokio::net::windows::named_pipe::{
        ClientOptions, ServerOptions, NamedPipeServer, NamedPipeClient,
    };
    use std::io;

    pub struct IpcListener {
        pipe_name: String,
    }

    impl IpcListener {
        pub fn bind(path: &std::path::Path) -> Result<Self, io::Error> {
            let pipe_name = path.to_string_lossy().to_string();
            // Pre-create the first pipe instance to verify the name works
            let _server = ServerOptions::new()
                .first_pipe_instance(true)
                .create(&pipe_name)?;
            Ok(Self { pipe_name })
        }

        pub async fn accept(&self) -> Result<IpcStream, io::Error> {
            let server = ServerOptions::new().create(&self.pipe_name)?;
            server.connect().await?;
            Ok(IpcStream::Server(server))
        }
    }

    pub enum IpcStream {
        Server(NamedPipeServer),
        Client(NamedPipeClient),
    }

    impl IpcStream {
        pub async fn connect(path: &std::path::Path) -> Result<Self, io::Error> {
            let pipe_name = path.to_string_lossy().to_string();
            let client = ClientOptions::new().open(&pipe_name)?;
            Ok(Self::Client(client))
        }

        pub fn into_split(self) -> (IpcReadHalf, IpcWriteHalf) {
            match self {
                Self::Server(s) => {
                    let (r, w) = tokio::io::split(s);
                    (
                        IpcReadHalf { inner: BufReader::new(r) },
                        IpcWriteHalf { inner: w },
                    )
                }
                Self::Client(c) => {
                    let (r, w) = tokio::io::split(c);
                    (
                        IpcReadHalf { inner: BufReader::new(r) },
                        IpcWriteHalf { inner: w },
                    )
                }
            }
        }
    }

    // Windows uses tokio::io::split which produces generic halves.
    // We wrap them to match the Unix API surface.
    pub struct IpcReadHalf {
        inner: BufReader<tokio::io::ReadHalf</* ... */>>,
        // In practice, use Box<dyn AsyncBufRead + Unpin + Send>
    }

    pub struct IpcWriteHalf {
        inner: tokio::io::WriteHalf</* ... */>,
        // In practice, use Box<dyn AsyncWrite + Unpin + Send>
    }

    // ... same next_line() / write_line() API as Unix
}

// Re-export the platform-specific types uniformly
pub use platform::{IpcListener, IpcStream, IpcReadHalf, IpcWriteHalf};
```

### Key design decisions and why

1. **No trait, no dynamic dispatch.** The `#[cfg]` approach means the compiler only sees one implementation per platform. This is what `tokio` itself does internally. Zero runtime cost.

2. **The read/write halves are type-erased.** The server and client code never sees `UnixStream` or `NamedPipeServer` directly -- they just call `next_line()` and `write_line()`. This is the lesson from the TS project: Node.js `net.createServer` transparently handles both because the `Socket` API is identical. We replicate that here.

3. **Path resolution mirrors the TS project exactly.** The TS `ipc-path-resolver.ts` uses `\\.\pipe\<name>` on Windows and `~/.dir/file.sock` on Unix. We do the same.

4. **PID file management needs a parallel fix.** The `libc::kill(pid, 0)` in `pid.rs` must be replaced:

```rust
// pid.rs - cross-platform process liveness check

#[cfg(unix)]
pub fn is_process_running(pid: u32) -> bool {
    unsafe { libc::kill(pid as libc::pid_t, 0) == 0 }
}

#[cfg(windows)]
pub fn is_process_running(pid: u32) -> bool {
    use windows_sys::Win32::System::Threading::{
        OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
    };
    use windows_sys::Win32::Foundation::CloseHandle;
    unsafe {
        let handle = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid);
        if handle.is_null() {
            false
        } else {
            CloseHandle(handle);
            true
        }
    }
}
```

5. **Signal handling (server.rs SIGTERM block)** already has `#[cfg(unix)]`. On Windows, `ctrl_c()` is sufficient -- Windows services receive `CTRL_CLOSE_EVENT` which `tokio::signal::ctrl_c()` handles.

### What changes in `server.rs` and `client.rs`

The changes are minimal. Replace direct `UnixListener`/`UnixStream` usage with `IpcListener`/`IpcStream`:

```rust
// server.rs -- change this line:
//   let listener = UnixListener::bind(&self.socket_path)...
// to:
    let listener = ipc::IpcListener::bind(&self.socket_path)
        .map_err(|e| format!("Failed to bind IPC at {}: {e}", self.socket_path.display()))?;

// And in the accept loop:
//   let (stream, _addr) = listener.accept().await?;
// becomes:
    let stream = listener.accept().await?;
```

```rust
// client.rs -- change this line:
//   UnixStream::connect(&self.socket_path).await
// to:
    ipc::IpcStream::connect(&self.socket_path).await
```

The rest of `handle_connection` stays identical because it operates on `lines()` and `write_all()` -- the same primitives exposed by both backends.

---

## 2. Recommended Sidecar Distribution Strategy

### Recommendation: **Adjacent binary (option b)** as the primary model, with download-on-first-run as fallback

After evaluating all five options against your existing infrastructure:

| Option | Verdict | Rationale |
|--------|---------|-----------|
| (a) `include_bytes!()` | **Reject** | The Go sidecar is ~15-20MB compressed. Embedding it doubles the CLI binary size, makes every Rust recompile re-link the blob, and requires a different Rust binary per platform anyway (you already need cross-compilation). No win. |
| (b) Adjacent binary | **Primary** | This is what Tailscale does (`tailscale` + `tailscaled`). Your `find_sidecar()` search location #3 already handles this. GitHub releases and Homebrew naturally bundle two files in a tarball. |
| (c) npm optionalDependencies | **Keep for NAPI** | Already works for the `truffle-napi` Node.js binding use case. Not relevant for standalone CLI distribution. |
| (d) Download on first run | **Fallback** | For the `cargo install` channel where we cannot bundle Go binaries. On first `truffle up`, if no sidecar found, download from GitHub releases. |
| (e) Go cross-compilation in build.rs | **Reject** | Requiring Go on the user's machine to `cargo install` defeats "zero-config". Good for CI, bad for end users. |

### Updated `find_sidecar()` that works on all platforms

```rust
//! Auto-discovery of the Go sidecar binary.

use std::path::{Path, PathBuf};
use tracing::debug;

/// Binary name (platform-aware).
fn sidecar_bin_name() -> &'static str {
    #[cfg(windows)]
    { "sidecar-slim.exe" }
    #[cfg(not(windows))]
    { "sidecar-slim" }
}

/// All names we search for in PATH.
fn sidecar_search_names() -> &'static [&'static str] {
    #[cfg(windows)]
    { &["truffle-sidecar.exe", "sidecar-slim.exe"] }
    #[cfg(not(windows))]
    { &["truffle-sidecar", "sidecar-slim"] }
}

/// Locate the Go sidecar binary using a prioritized search.
///
/// Search order:
/// 1. Explicit config file override (`config_path`)
/// 2. `TRUFFLE_SIDECAR_PATH` env var
/// 3. Next to the CLI binary (same directory)
/// 4. `~/.config/truffle/bin/` (installed via download-on-first-run)
/// 5. Workspace dev locations (walk up from binary / cwd)
/// 6. npm global modules (`npm root -g` / platform-specific npm package)
/// 7. Homebrew prefix (macOS/Linux only)
/// 8. In `PATH` via `which` (Unix) or `where` (Windows)
pub fn find_sidecar(config_path: Option<&str>) -> Result<PathBuf, String> {
    let bin_name = sidecar_bin_name();

    // 1. Explicit config override
    if let Some(p) = config_path {
        if !p.is_empty() {
            let path = PathBuf::from(p);
            if path.exists() {
                debug!(path = %path.display(), "sidecar: found via config");
                return Ok(path);
            }
            return Err(format!(
                "Sidecar path from config does not exist: {}", path.display()
            ));
        }
    }

    // 2. TRUFFLE_SIDECAR_PATH env var
    if let Ok(env_path) = std::env::var("TRUFFLE_SIDECAR_PATH") {
        if !env_path.is_empty() {
            let path = PathBuf::from(&env_path);
            if path.exists() {
                debug!(path = %path.display(), "sidecar: found via TRUFFLE_SIDECAR_PATH");
                return Ok(path);
            }
            return Err(format!("TRUFFLE_SIDECAR_PATH does not exist: {env_path}"));
        }
    }

    // 3. Next to the CLI binary (same directory)
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            for name in sidecar_search_names() {
                let candidate = dir.join(name);
                if candidate.exists() && is_executable(&candidate) {
                    debug!(path = %candidate.display(), "sidecar: found next to CLI");
                    return Ok(candidate);
                }
            }
        }
    }

    // 4. ~/.config/truffle/bin/ (user-local install)
    if let Some(config_dir) = dirs::config_dir() {
        let candidate = config_dir.join("truffle").join("bin").join(bin_name);
        if candidate.exists() && is_executable(&candidate) {
            debug!(path = %candidate.display(), "sidecar: found in config bin dir");
            return Ok(candidate);
        }
    }

    // 5. Workspace dev locations
    if let Ok(exe) = std::env::current_exe() {
        if let Some(start) = exe.parent() {
            if let Some(path) = walk_up_for_workspace(start, bin_name) {
                debug!(path = %path.display(), "sidecar: found in workspace (from exe)");
                return Ok(path);
            }
        }
    }
    if let Ok(cwd) = std::env::current_dir() {
        if let Some(path) = walk_up_for_workspace(&cwd, bin_name) {
            debug!(path = %path.display(), "sidecar: found in workspace (from cwd)");
            return Ok(path);
        }
    }

    // 6. npm global modules
    if let Some(path) = find_in_npm_global(bin_name) {
        debug!(path = %path.display(), "sidecar: found in npm global");
        return Ok(path);
    }

    // 7. Homebrew prefix (macOS/Linux only)
    #[cfg(unix)]
    if let Some(path) = find_in_homebrew(bin_name) {
        debug!(path = %path.display(), "sidecar: found in Homebrew");
        return Ok(path);
    }

    // 8. In PATH
    for name in sidecar_search_names() {
        if let Some(path) = which_binary(name) {
            debug!(path = %path.display(), "sidecar: found in PATH");
            return Ok(path);
        }
    }

    Err(
        "Could not find sidecar binary. Searched:\n\
         \x20 - next to CLI binary\n\
         \x20 - ~/.config/truffle/bin/\n\
         \x20 - workspace packages/sidecar-slim/bin/\n\
         \x20 - npm global modules\n\
         \x20 - PATH (truffle-sidecar, sidecar-slim)\n\n\
         Fix: run 'truffle install-sidecar' to download,\n\
         \x20    or set sidecar_path in ~/.config/truffle/config.toml"
            .to_string(),
    )
}

/// Check npm global modules for the platform-specific sidecar package.
fn find_in_npm_global(bin_name: &str) -> Option<PathBuf> {
    let output = std::process::Command::new("npm")
        .args(["root", "-g"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let npm_root = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if npm_root.is_empty() {
        return None;
    }

    // Map current platform to npm package name
    let pkg_name = platform_npm_package()?;
    let candidate = PathBuf::from(&npm_root)
        .join(pkg_name)
        .join("bin")
        .join(bin_name);
    if candidate.exists() && is_executable(&candidate) {
        return Some(candidate);
    }
    None
}

/// Get the npm package name for the current platform.
fn platform_npm_package() -> Option<&'static str> {
    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    { Some("@vibecook/truffle-sidecar-darwin-arm64") }
    #[cfg(all(target_os = "macos", target_arch = "x86_64"))]
    { Some("@vibecook/truffle-sidecar-darwin-x64") }
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    { Some("@vibecook/truffle-sidecar-linux-x64") }
    #[cfg(all(target_os = "linux", target_arch = "aarch64"))]
    { Some("@vibecook/truffle-sidecar-linux-arm64") }
    #[cfg(all(target_os = "windows", target_arch = "x86_64"))]
    { Some("@vibecook/truffle-sidecar-win32-x64") }
    #[cfg(not(any(
        all(target_os = "macos", target_arch = "aarch64"),
        all(target_os = "macos", target_arch = "x86_64"),
        all(target_os = "linux", target_arch = "x86_64"),
        all(target_os = "linux", target_arch = "aarch64"),
        all(target_os = "windows", target_arch = "x86_64"),
    )))]
    { None }
}

/// Check Homebrew prefix for the sidecar binary.
#[cfg(unix)]
fn find_in_homebrew(bin_name: &str) -> Option<PathBuf> {
    let output = std::process::Command::new("brew")
        .args(["--prefix", "truffle"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let prefix = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let candidate = PathBuf::from(&prefix).join("bin").join(bin_name);
    if candidate.exists() && is_executable(&candidate) {
        Some(candidate)
    } else {
        None
    }
}

/// Find a binary in PATH.
fn which_binary(name: &str) -> Option<PathBuf> {
    #[cfg(unix)]
    let cmd = "which";
    #[cfg(windows)]
    let cmd = "where";

    std::process::Command::new(cmd)
        .arg(name)
        .output()
        .ok()
        .and_then(|output| {
            if output.status.success() {
                let path_str = String::from_utf8_lossy(&output.stdout)
                    .lines()
                    .next()?  // `where` on Windows may return multiple lines
                    .trim()
                    .to_string();
                if !path_str.is_empty() {
                    Some(PathBuf::from(path_str))
                } else {
                    None
                }
            } else {
                None
            }
        })
}

#[cfg(unix)]
fn is_executable(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    std::fs::metadata(path)
        .map(|m| m.permissions().mode() & 0o111 != 0)
        .unwrap_or(false)
}

#[cfg(windows)]
fn is_executable(path: &Path) -> bool {
    // On Windows, existence + .exe extension is sufficient
    path.exists()
}

fn walk_up_for_workspace(start: &Path, bin_name: &str) -> Option<PathBuf> {
    let mut dir = start.to_path_buf();
    for _ in 0..10 {
        let candidate = dir.join("packages").join("sidecar-slim").join("bin").join(bin_name);
        if candidate.exists() && is_executable(&candidate) {
            return Some(candidate);
        }
        let legacy = dir.join("packages").join("core").join("bin").join(bin_name);
        if legacy.exists() && is_executable(&legacy) {
            return Some(legacy);
        }
        if !dir.pop() { break; }
    }
    None
}
```

### Download-on-first-run fallback (`truffle install-sidecar` subcommand)

For the `cargo install` channel, add a `truffle install-sidecar` command that:

1. Determines the current platform/arch
2. Downloads the matching binary from GitHub releases: `https://github.com/vibecook-dev/truffle/releases/latest/download/tsnet-sidecar-{os}-{arch}`
3. Installs it to `~/.config/truffle/bin/sidecar-slim`
4. Sets executable permission on Unix

This is exactly what `rustup` does for toolchains. The `truffle up` command should also call this automatically when `find_sidecar()` fails and `config.node.auto_up` is true.

---

## 3. Build Matrix and CI/CD Plan

### Supported platforms (6 targets)

| Platform | Rust Target | Go GOOS/GOARCH | Priority |
|----------|-------------|----------------|----------|
| macOS ARM64 (Apple Silicon) | `aarch64-apple-darwin` | `darwin/arm64` | P0 -- your dev machine |
| macOS x64 (Intel) | `x86_64-apple-darwin` | `darwin/amd64` | P1 |
| Linux x64 | `x86_64-unknown-linux-gnu` | `linux/amd64` | P0 -- servers, CI, WSL |
| Linux ARM64 | `aarch64-unknown-linux-gnu` | `linux/arm64` | P1 -- Raspberry Pi, ARM servers |
| Windows x64 | `x86_64-pc-windows-msvc` | `windows/amd64` | P1 |
| Windows ARM64 | `aarch64-pc-windows-msvc` | `windows/arm64` | P2 -- defer (Surface Pro X, etc.) |

### CI/CD workflow: `release-cli.yml` (new)

The existing `release-sidecar.yml` and `napi-build.yml` already handle the Go and NAPI builds. Add a new workflow for the CLI binary:

```yaml
name: Release CLI

on:
  release:
    types: [created]

permissions:
  contents: write

jobs:
  build:
    strategy:
      matrix:
        include:
          - os: macos-latest
            target: aarch64-apple-darwin
            asset: truffle-darwin-arm64.tar.gz
            sidecar_goos: darwin
            sidecar_goarch: arm64
            sidecar_bin: sidecar-slim

          - os: macos-latest
            target: x86_64-apple-darwin
            asset: truffle-darwin-x64.tar.gz
            sidecar_goos: darwin
            sidecar_goarch: amd64
            sidecar_bin: sidecar-slim

          - os: ubuntu-latest
            target: x86_64-unknown-linux-gnu
            asset: truffle-linux-x64.tar.gz
            sidecar_goos: linux
            sidecar_goarch: amd64
            sidecar_bin: sidecar-slim

          - os: ubuntu-latest
            target: aarch64-unknown-linux-gnu
            asset: truffle-linux-arm64.tar.gz
            sidecar_goos: linux
            sidecar_goarch: arm64
            sidecar_bin: sidecar-slim

          - os: windows-latest
            target: x86_64-pc-windows-msvc
            asset: truffle-win32-x64.zip
            sidecar_goos: windows
            sidecar_goarch: amd64
            sidecar_bin: sidecar-slim.exe

    runs-on: ${{ matrix.os }}
    steps:
      - uses: actions/checkout@v4

      - uses: dtolnay/rust-toolchain@stable
        with:
          targets: ${{ matrix.target }}

      - uses: actions/setup-go@v5
        with:
          go-version: '1.25'
          cache-dependency-path: packages/sidecar-slim/go.sum

      # Cross-compilation tools for Linux ARM64
      - name: Install cross tools (aarch64-linux)
        if: matrix.target == 'aarch64-unknown-linux-gnu'
        run: |
          sudo apt-get update
          sudo apt-get install -y gcc-aarch64-linux-gnu

      # Build Rust CLI
      - name: Build CLI
        env:
          CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER: aarch64-linux-gnu-gcc
        run: |
          cargo build --release --package truffle-cli --target ${{ matrix.target }}

      # Build Go sidecar
      - name: Build sidecar
        working-directory: packages/sidecar-slim
        env:
          GOOS: ${{ matrix.sidecar_goos }}
          GOARCH: ${{ matrix.sidecar_goarch }}
          CGO_ENABLED: '0'
        run: |
          go build -ldflags "-s -w -X main.Version=${{ github.ref_name }}" \
            -o ${{ matrix.sidecar_bin }} .

      # Package both binaries together
      - name: Package (Unix)
        if: runner.os != 'Windows'
        run: |
          mkdir -p dist
          cp target/${{ matrix.target }}/release/truffle dist/
          cp packages/sidecar-slim/${{ matrix.sidecar_bin }} dist/
          strip dist/truffle || true
          cd dist && tar czf ../${{ matrix.asset }} *

      - name: Package (Windows)
        if: runner.os == 'Windows'
        run: |
          mkdir dist
          copy target\${{ matrix.target }}\release\truffle.exe dist\
          copy packages\sidecar-slim\${{ matrix.sidecar_bin }} dist\
          cd dist && 7z a ..\${{ matrix.asset }} *

      - name: Upload release asset
        env:
          GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}
        run: |
          gh release upload "${{ github.ref_name }}" ${{ matrix.asset }} --clobber
```

### What gets built per release

For every tagged release (e.g., `v0.2.0`):

1. **`release-cli.yml`** -- builds 5 tarballs/zips, each containing `truffle` + `sidecar-slim` side by side
2. **`release-sidecar.yml`** (existing) -- builds standalone sidecar binaries + publishes npm platform packages
3. **`napi-build.yml`** (existing) -- builds NAPI `.node` bindings for the Node.js SDK

---

## 4. Distribution Channel Recommendations

### Channel 1: GitHub Releases (primary, day-one)

**How it works:** User downloads a platform tarball containing both `truffle` and `sidecar-slim`.

```bash
# macOS Apple Silicon
curl -fsSL https://github.com/vibecook-dev/truffle/releases/latest/download/truffle-darwin-arm64.tar.gz | tar xz -C /usr/local/bin

# Linux
curl -fsSL https://github.com/vibecook-dev/truffle/releases/latest/download/truffle-linux-x64.tar.gz | tar xz -C /usr/local/bin
```

**Sidecar inclusion:** Bundled in the tarball. `find_sidecar()` location #3 (adjacent to CLI) finds it immediately.

**Priority:** Ship this first. It is the simplest and requires only the CI workflow above.

### Channel 2: Install script (day-one, builds on Channel 1)

```bash
curl -fsSL https://truffle.sh/install | sh
```

The script:
1. Detects OS and arch
2. Downloads the correct tarball from GitHub releases
3. Extracts to `~/.config/truffle/bin/`
4. Adds to PATH (or prints instructions)
5. Runs `truffle doctor` to verify

**Sidecar inclusion:** Bundled in the tarball, both binaries extracted together.

### Channel 3: Homebrew (week 2)

```ruby
class Truffle < Formula
  desc "Mesh networking CLI built on Tailscale"
  homepage "https://github.com/vibecook-dev/truffle"
  version "0.2.0"

  on_macos do
    on_arm do
      url "https://github.com/vibecook-dev/truffle/releases/download/v0.2.0/truffle-darwin-arm64.tar.gz"
      sha256 "..."
    end
    on_intel do
      url "https://github.com/vibecook-dev/truffle/releases/download/v0.2.0/truffle-darwin-x64.tar.gz"
      sha256 "..."
    end
  end

  on_linux do
    on_arm do
      url "https://github.com/vibecook-dev/truffle/releases/download/v0.2.0/truffle-linux-arm64.tar.gz"
      sha256 "..."
    end
    on_intel do
      url "https://github.com/vibecook-dev/truffle/releases/download/v0.2.0/truffle-linux-x64.tar.gz"
      sha256 "..."
    end
  end

  def install
    bin.install "truffle"
    bin.install "sidecar-slim"
  end
end
```

**Sidecar inclusion:** Both binaries installed to `$(brew --prefix)/bin/`. `find_sidecar()` location #3 (adjacent) or #8 (PATH) finds it.

### Channel 4: `cargo install truffle-cli` (nice-to-have, month 2)

**Problem:** `cargo install` builds from source but cannot bundle a Go binary.

**Solution:** On first `truffle up`, if no sidecar is found, the CLI runs the download-on-first-run logic:

```
$ truffle up
  Sidecar binary not found. Downloading for darwin-arm64...
  Downloaded to ~/.config/truffle/bin/sidecar-slim (18.2 MB)
  Daemon started.
```

This requires publishing `truffle-cli` to crates.io (currently `publish = false`). Change the `Cargo.toml` when ready.

**Sidecar inclusion:** Auto-downloaded to `~/.config/truffle/bin/` on first run. `find_sidecar()` location #4 finds it.

### Channel 5: npm global install (leverages existing infra)

```bash
npm install -g @vibecook/truffle-cli
```

This would be a thin npm package that:
- Has `optionalDependencies` on all `@vibecook/truffle-sidecar-*` packages (already exist)
- Has `optionalDependencies` on platform-specific NAPI packages (already exist)
- Ships a shell/batch script in `bin` that invokes the native truffle binary

**Sidecar inclusion:** Via `optionalDependencies`, npm installs the correct platform sidecar. `find_sidecar()` location #6 (npm global) finds it.

**Priority:** This channel is useful for developers who already have Node.js. Your existing npm infrastructure handles it.

---

## 5. Complete Cargo.toml Dependency Changes

For full cross-platform support, `truffle-cli/Cargo.toml` needs:

```toml
[dependencies]
# ... existing deps ...
libc = "0.2"  # already present

[target.'cfg(windows)'.dependencies]
windows-sys = { version = "0.59", features = [
    "Win32_System_Threading",
    "Win32_Foundation",
] }
```

The `tokio` crate's named pipe support is behind the `net` feature, which is already enabled in the existing `Cargo.toml`.

---

## 6. Summary: Prioritized Implementation Plan

| Step | Effort | What |
|------|--------|------|
| 1 | Small | Add `ipc.rs` module with `#[cfg(unix)]`/`#[cfg(windows)]` listener/stream types |
| 2 | Small | Update `server.rs` and `client.rs` to use `ipc::IpcListener`/`IpcStream` |
| 3 | Small | Fix `pid.rs` to use `windows-sys` on Windows for process liveness |
| 4 | Small | Fix `sidecar.rs`: `.exe` suffix, `where` instead of `which`, npm global search |
| 5 | Small | Update `config.rs` `socket_path()` to return named pipe path on Windows |
| 6 | Medium | Add `release-cli.yml` CI workflow (builds both Rust + Go for 5 platforms) |
| 7 | Medium | Add `truffle install-sidecar` subcommand with download-on-first-run |
| 8 | Small | Add install script (`install.sh`) |
| 9 | Small | Add Homebrew formula |

Steps 1-5 are purely mechanical code changes (~200 lines of new/modified Rust). Step 6 is the CI workflow shown above. Steps 7-9 are distribution polish.

The most critical insight from studying the TS codebase: **Node.js `net.createServer` transparently handles both Unix sockets and named pipes because the `Socket` stream API is identical**. In Rust, we achieve the same thing with `#[cfg]` module-level selection. The wire protocol (newline-delimited JSON-RPC) does not change. The IPC transport is a detail hidden below the module boundary.