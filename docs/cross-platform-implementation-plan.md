# Truffle CLI: Cross-Platform Implementation Plan

## Review Date: 2026-03-21
## Based on: `docs/cross-platform-strategy.md` validated against actual codebase

---

## Part 1: Strategy Validation

### 1.1 IPC Abstraction (`ipc.rs` with `#[cfg]` platform selection)

**Verdict: Correct, with caveats.**

The strategy's recommendation to use `#[cfg]` module-level selection instead of a trait is the right call. This mirrors what tokio itself does and avoids dynamic dispatch overhead. The proposed code structure (Unix `UnixListener`/`UnixStream` vs Windows `NamedPipeServer`/`NamedPipeClient`) is architecturally sound.

**Validation of `tokio::net::windows::named_pipe`:**
- Yes, this module exists in tokio (stabilized in tokio 1.x with the `net` feature).
- The types `ServerOptions`, `ClientOptions`, `NamedPipeServer`, `NamedPipeClient` are real.
- The `net` feature is already enabled in `truffle-cli/Cargo.toml` (`tokio = { features = ["net"] }`).
- **Caveat the doc missed:** Named pipes on Windows require a different accept pattern. `NamedPipeServer` is single-use per connection -- you must create a new `ServerOptions::new().create()` instance for each subsequent connection. The strategy doc's `accept()` method does show this correctly (creating a new server instance per accept), but the `bind()` method creates an initial instance with `first_pipe_instance(true)` and then drops it immediately. This is slightly wasteful but functionally correct. A better pattern: keep the first instance and use it for the first connection, then create new instances for subsequent ones. However, this is a minor optimization and the current approach works.

**Issue found in Windows `IpcStream`:** The strategy doc shows `IpcReadHalf` and `IpcWriteHalf` with placeholder types (`/* ... */`) and comments saying "In practice, use `Box<dyn AsyncBufRead + Unpin + Send>`". This is a real gap -- the actual implementation needs concrete types. The correct approach is to use `tokio::io::split()` which produces `ReadHalf<T>` and `WriteHalf<T>`, but since `IpcStream` is an enum (`Server` or `Client`), you need to type-erase. The cleanest solution: use `Box<dyn AsyncRead + Unpin + Send>` and `Box<dyn AsyncWrite + Unpin + Send>` for both platforms, keeping the API uniform.

**Better alternative:** Instead of type-erasing at the read/write half level, type-erase at the stream level. Use `tokio_util::either::Either` or a simple enum wrapper that implements `AsyncRead + AsyncWrite` via delegation. This avoids boxing entirely on Unix (where there's only one variant) and keeps the Windows enum lightweight.

### 1.2 PID file approach on Windows

**Verdict: Correct but incomplete.**

The strategy's `is_process_running()` using `OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, ...)` is the standard Windows approach and is correct.

**Gotchas missed:**
1. **PID reuse on Windows is more aggressive than Unix.** Windows recycles PIDs faster. The current approach (write PID, check PID) can falsely detect a different process as "still running." Mitigation: also store the process start time in the PID file and compare against `GetProcessTimes()`. However, this is an edge case and the current approach matches what most daemon managers do.

2. **File locking on Windows.** The strategy doc does not address this at all. On Unix, if the daemon crashes, the PID file remains and `check_stale_pid()` handles it. On Windows, the PID file may be **locked** by the crashed process's handle (especially if using file-based logging in the same directory). The fix: use `fs::write()` with overwrite semantics (which Rust does by default) and be prepared for `ERROR_SHARING_VIOLATION`. The current `write_pid()` code uses `fs::write()` which is fine -- Windows will overwrite even if another process had the file open for reading.

3. **The `libc` dependency becomes unix-only.** The strategy doc mentions adding `windows-sys` for Windows but doesn't mention making `libc` conditional. Currently `libc = "0.2"` is an unconditional dependency. It should become:
   ```toml
   [target.'cfg(unix)'.dependencies]
   libc = "0.2"
   ```

### 1.3 Signal handling

**Verdict: Correct. Already handled.**

The existing `server.rs` already has `#[cfg(unix)]` around the SIGTERM handler (lines 160-171). `tokio::signal::ctrl_c()` works on both Unix and Windows. On Windows, it handles `CTRL_C_EVENT` and `CTRL_CLOSE_EVENT`. No changes needed here beyond what the strategy already states.

### 1.4 Go sidecar stdin/stdout protocol on Windows

**Verdict: Works, but with an important caveat.**

The Go sidecar uses `bufio.NewScanner(os.Stdin)` for reading and `json.NewEncoder(os.Stdout)` for writing. On Windows:
- **stdin/stdout are valid** when spawned as a child process with `Stdio::piped()`. The `GoShim::spawn_child()` in `shim.rs` uses `Command::new(&config.binary_path).stdin(piped()).stdout(piped()).stderr(piped())`, which works identically on Windows.
- **No TTY issues.** Since the Go sidecar is always spawned as a subprocess (never run interactively), there are no TTY vs pipe behavioral differences.
- **Line ending caveat:** The Go scanner uses `\n` as the default delimiter. On Windows, `os.Stdout` writes with `\n` (not `\r\n`) when writing to a pipe (as opposed to a console). The Rust side reads with `lines()` which strips both. **No issue here.**
- **Buffer size:** The Go sidecar sets `scanner.Buffer(make([]byte, 1024*1024), 1024*1024)` (1MB). This is fine on all platforms.

### 1.5 `GoShim::spawn()` cross-platform behavior

**Verdict: Works on Windows as-is.**

`Command::new(&config.binary_path)` with `.stdin(piped()).stdout(piped()).stderr(piped())` is fully cross-platform in Rust. The `kill_on_drop(true)` on `tokio::process::Command` also works on Windows (it calls `TerminateProcess`).

**Issue found:** The background daemon spawning in `commands/up.rs` uses `libc::setsid()` inside a `#[cfg(unix)]` block (line 238-247). This is already correctly gated. On Windows, the `cmd.spawn()` without `setsid` will still work for background operation since stdin/stdout/stderr are set to `null()`. However, the child process will still be in the same console group. If the parent terminal is closed, the child may receive `CTRL_CLOSE_EVENT`. This is usually acceptable, but for true daemon behavior on Windows, you should add:
```rust
#[cfg(windows)]
{
    use std::os::windows::process::CommandExt;
    cmd.creation_flags(0x00000008); // CREATE_NO_WINDOW
}
```

### 1.6 File path separators

**Verdict: Non-issue for the most part.**

Rust's `PathBuf` and `Path` handle separators correctly on all platforms. The `dirs::config_dir()` function returns platform-appropriate paths:
- macOS: `~/Library/Application Support`
- Linux: `~/.config` (XDG_CONFIG_HOME)
- Windows: `C:\Users\<user>\AppData\Roaming`

**One real issue:** The config uses `dirs::config_dir().join("truffle")`, which means:
- macOS: `~/Library/Application Support/truffle/` (the strategy doc says `~/.config/truffle/` -- this is **wrong** for macOS)
- Linux: `~/.config/truffle/`
- Windows: `C:\Users\<user>\AppData\Roaming\truffle\`

The actual behavior is correct (using `dirs` crate), but the docs and comments reference `~/.config/truffle` everywhere, which is only accurate on Linux. This is cosmetic but worth noting in user-facing docs.

**Named pipe path:** The strategy uses `\\.\pipe\truffle-daemon`. This is correct for Windows named pipes. The `ipc_path()` function correctly returns a `PathBuf` on Windows. However, `PathBuf::from(r"\\.\pipe\truffle-daemon")` works but `path.exists()` will return `false` for named pipe paths -- this is fine because the server creates the pipe, not the filesystem.

### 1.7 `which_binary()` on Windows

**Verdict: Strategy is correct.**

The current code calls `which` (Unix-only). The strategy correctly proposes using `where` on Windows via `#[cfg]`. One note: `where` on Windows can return multiple lines (if the binary is found in multiple PATH locations). The strategy's code handles this correctly by using `.lines().next()`.

**Better alternative:** Use the `which` crate (`which = "7"`) instead of shelling out. It's pure Rust, cross-platform, and avoids the subprocess overhead. This is a minor improvement but removes a potential failure mode (the `which` binary not being installed on minimal Linux containers).

### 1.8 Sidecar binary names (.exe suffix)

**Verdict: Strategy is correct.**

The current `sidecar.rs` hardcodes `"truffle-sidecar"` and `"sidecar-slim"` without `.exe`. The strategy correctly adds platform-aware binary names. The `helpers.js` (NAPI layer) already does this correctly: `const ext = process.platform === 'win32' ? '.exe' : '';`.

### 1.9 Bridge TCP protocol (localhost binding)

**Verdict: Works cross-platform.**

The `BridgeManager` in `truffle-core/src/bridge/manager.rs` binds to `127.0.0.1:0` (ephemeral port). The Go sidecar dials `127.0.0.1:<port>` to connect to the bridge. TCP localhost works identically on all platforms. No IPv6 issues since it explicitly uses `127.0.0.1`.

---

## Part 2: Missed Issues

### 2.1 Windows Defender / Antivirus

**Risk: MEDIUM.** Unknown unsigned binaries downloaded from the internet will trigger Windows Defender SmartScreen. The Go sidecar and Rust CLI will both be flagged on first run.

**Mitigations:**
- **Code signing (recommended for v1.0, not blocking for alpha/beta).** An EV code signing certificate costs ~$200-400/year. Sign both the `.exe` files in CI.
- **For now:** Document that users may need to click "Run anyway" on SmartScreen. This is standard for new developer tools.
- **Long-term:** As the binaries accumulate reputation (downloads), SmartScreen stops flagging them automatically.

### 2.2 macOS Gatekeeper quarantine

**Risk: LOW.** Binaries downloaded via `curl` do NOT get quarantined. Gatekeeper quarantine is applied by browsers and macOS-aware apps (Safari, Chrome, Finder). `curl | tar` bypasses quarantine entirely.

**If users download the tarball via browser:** They will need to `xattr -d com.apple.quarantine truffle sidecar-slim` or right-click -> Open in Finder. Document this in the install guide.

**Homebrew channel:** Not affected -- Homebrew handles quarantine removal automatically.

### 2.3 Linux AppArmor/SELinux

**Risk: LOW for CLI tools.** AppArmor/SELinux primarily restrict server processes and system services. A user-space CLI tool running from `~/.config/truffle/bin/` or `/usr/local/bin/` is unlikely to be restricted.

**Potential issue:** The Go sidecar creates `tsnet.Server` which opens network listeners on Tailscale IPs. On SELinux-enforcing systems (e.g., RHEL/Fedora), this may require the `network_connect` permission. In practice, tsnet uses userspace networking and does not require privileged ports, so this is unlikely to cause issues.

**Mitigation:** None needed initially. Add a `truffle doctor` diagnostic that checks for SELinux denials if users report issues.

### 2.4 File locking on Windows (multiple processes)

**Risk: MEDIUM.** Windows has mandatory file locking by default. If two `truffle` processes try to read/write the config file or PID file simultaneously:
- `fs::read_to_string()` can fail with `ERROR_SHARING_VIOLATION` if another process has the file open for writing.
- `fs::write()` can fail similarly.

**Current exposure:**
- PID file: `write_pid()` and `read_pid()` are called during daemon startup and client connection checks. A race between `truffle up` and `truffle status` could theoretically fail, but the window is tiny.
- Config file: Read-only after startup, no issue.

**Mitigation:** Use `OpenOptions` with `FILE_SHARE_READ | FILE_SHARE_WRITE` on Windows. However, this is over-engineering for now. The current code will work in practice because:
1. The daemon writes the PID file once at startup.
2. Clients only read the PID file (shared read access is allowed).
3. The config file is read-only.

### 2.5 Go sidecar Tailscale behavior on Windows

**Risk: LOW.** The Go sidecar uses `tsnet`, which is Tailscale's userspace networking library. `tsnet` works on Windows -- it's how Tailscale's own Windows client operates. The `tsnet.Server` creates a userspace WireGuard tunnel without requiring a TUN device or admin privileges.

**Key difference on Windows:** `tsnet` stores state in the `Dir` directory. The current code passes `config.node.state_dir` which resolves to `~/.config/truffle/state` (via `dirs::config_dir()`). On Windows, this becomes `C:\Users\<user>\AppData\Roaming\truffle\state`. This is correct.

**Firewall note:** Windows Firewall may prompt the user to allow the sidecar binary on first run. This is unavoidable for any networked application. Document it.

### 2.6 Background daemon on Windows (missed in strategy)

**Risk: MEDIUM.** The `run_background()` function in `commands/up.rs` spawns the daemon as a child process. On Unix, `setsid()` detaches it properly. On Windows, there is no equivalent of `setsid()`. The current code does not add `CREATE_NO_WINDOW` to the creation flags.

**Consequence:** Without `CREATE_NO_WINDOW`, the background daemon process may:
1. Flash a console window briefly on spawn.
2. Be terminated when the parent console is closed.

**Fix required:** Add `CREATE_NO_WINDOW` (0x08000000) creation flag on Windows. See Phase 1 for implementation.

### 2.7 `client.rs` auto-start uses `socket_path.exists()` for readiness check

The `ensure_running()` method in `client.rs` polls `self.socket_path.exists()` to detect when the daemon is ready. On Windows with named pipes, `Path::exists()` returns `false` for pipe paths like `\\.\pipe\truffle-daemon`. The readiness check will time out.

**Fix:** On Windows, skip the `exists()` check and go straight to attempting a connection. The connection will fail with a specific error until the pipe is created. Alternatively, only check `is_daemon_running()` (PID-based) and then attempt to connect.

### 2.8 Socket cleanup on crash (Unix-specific assumption)

In `server.rs`, the `start()` method removes stale socket files:
```rust
if socket_path.exists() {
    std::fs::remove_file(&socket_path)?;
}
```
On Windows, named pipes are kernel objects and do not leave stale files. This code path should be `#[cfg(unix)]`-gated (harmless on Windows since the path won't exist, but cleaner to gate).

---

## Part 3: Implementation Plan

### Phase 1: IPC Abstraction + Platform Fixes

**Goal:** Make the daemon server and client work on macOS, Linux, and Windows.

**Dependencies:** None (foundational phase).

**Estimated effort:** 2-3 hours.

#### 1.1 Create `crates/truffle-cli/src/daemon/ipc.rs`

New file. Cross-platform IPC transport with `#[cfg]` selection.

```rust
//! Cross-platform IPC transport for daemon <-> CLI communication.
//!
//! - Unix (macOS/Linux): Unix Domain Sockets
//! - Windows: Named Pipes (\\.\pipe\truffle-daemon)
//!
//! Exposes `IpcListener` and `IpcStream` that work identically on all
//! platforms. The wire protocol (newline-delimited JSON-RPC) is unchanged.

use std::path::PathBuf;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

const PIPE_NAME: &str = "truffle-daemon";

/// Get the IPC endpoint path for this platform.
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

/// Returns true if the IPC path refers to a named pipe (Windows).
#[allow(dead_code)]
pub fn is_named_pipe() -> bool {
    cfg!(windows)
}

// ---- Unix implementation ----

#[cfg(unix)]
mod platform {
    use std::io;
    use std::path::Path;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::{UnixListener, UnixStream};

    pub struct IpcListener {
        inner: UnixListener,
    }

    impl IpcListener {
        pub fn bind(path: &Path) -> Result<Self, io::Error> {
            if path.exists() {
                std::fs::remove_file(path)?;
            }
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            Ok(Self { inner: UnixListener::bind(path)? })
        }

        pub async fn accept(&self) -> Result<IpcStream, io::Error> {
            let (stream, _) = self.inner.accept().await?;
            Ok(IpcStream { inner: stream })
        }
    }

    pub struct IpcStream {
        inner: UnixStream,
    }

    impl IpcStream {
        pub async fn connect(path: &Path) -> Result<Self, io::Error> {
            Ok(Self { inner: UnixStream::connect(path).await? })
        }

        pub fn into_split(self) -> (IpcReadHalf, IpcWriteHalf) {
            let (r, w) = self.inner.into_split();
            (
                IpcReadHalf { inner: BufReader::new(r) },
                IpcWriteHalf { inner: w },
            )
        }
    }

    pub struct IpcReadHalf {
        inner: BufReader<tokio::net::unix::OwnedReadHalf>,
    }

    impl IpcReadHalf {
        pub async fn next_line(&mut self) -> Result<Option<String>, io::Error> {
            let mut line = String::new();
            match self.inner.read_line(&mut line).await? {
                0 => Ok(None),
                _ => {
                    if line.ends_with('\n') { line.pop(); }
                    if line.ends_with('\r') { line.pop(); }
                    Ok(Some(line))
                }
            }
        }
    }

    pub struct IpcWriteHalf {
        inner: tokio::net::unix::OwnedWriteHalf,
    }

    impl IpcWriteHalf {
        pub async fn write_line(&mut self, data: &str) -> Result<(), io::Error> {
            self.inner.write_all(data.as_bytes()).await?;
            self.inner.write_all(b"\n").await?;
            self.inner.flush().await
        }
    }
}

// ---- Windows implementation ----

#[cfg(windows)]
mod platform {
    use std::io;
    use std::path::Path;
    use tokio::io::{split, AsyncBufReadExt, AsyncWriteExt, BufReader,
                    ReadHalf, WriteHalf};
    use tokio::net::windows::named_pipe::{
        ClientOptions, NamedPipeClient, NamedPipeServer, ServerOptions,
    };

    pub struct IpcListener {
        pipe_name: String,
    }

    impl IpcListener {
        pub fn bind(path: &Path) -> Result<Self, io::Error> {
            let pipe_name = path.to_string_lossy().to_string();
            // Validate the pipe name by creating (and keeping) the first instance.
            // The first accept() will use this.
            let _test = ServerOptions::new()
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
        pub async fn connect(path: &Path) -> Result<Self, io::Error> {
            let pipe_name = path.to_string_lossy().to_string();
            let client = ClientOptions::new().open(&pipe_name)?;
            Ok(Self::Client(client))
        }

        pub fn into_split(self) -> (IpcReadHalf, IpcWriteHalf) {
            match self {
                Self::Server(s) => {
                    let (r, w) = split(s);
                    (
                        IpcReadHalf { inner: BufReader::new(r) },
                        IpcWriteHalf { inner: w },
                    )
                }
                Self::Client(c) => {
                    let (r, w) = split(c);
                    (
                        IpcReadHalf { inner: BufReader::new(r) },
                        IpcWriteHalf { inner: w },
                    )
                }
            }
        }
    }

    // Note: Windows uses tokio::io::split which returns generic halves.
    // These must be type-erased since IpcStream is an enum.
    // For simplicity, use Box<dyn ...> for the inner types.
    pub struct IpcReadHalf {
        inner: BufReader<Box<dyn tokio::io::AsyncRead + Unpin + Send>>,
    }

    pub struct IpcWriteHalf {
        inner: Box<dyn tokio::io::AsyncWrite + Unpin + Send>,
    }

    impl IpcReadHalf {
        pub async fn next_line(&mut self) -> Result<Option<String>, io::Error> {
            let mut line = String::new();
            match self.inner.read_line(&mut line).await? {
                0 => Ok(None),
                _ => {
                    if line.ends_with('\n') { line.pop(); }
                    if line.ends_with('\r') { line.pop(); }
                    Ok(Some(line))
                }
            }
        }
    }

    impl IpcWriteHalf {
        pub async fn write_line(&mut self, data: &str) -> Result<(), io::Error> {
            self.inner.write_all(data.as_bytes()).await?;
            self.inner.write_all(b"\n").await?;
            self.inner.flush().await
        }
    }
}

pub use platform::{IpcListener, IpcStream, IpcReadHalf, IpcWriteHalf};
```

**Important note on Windows type-erasure:** The code above uses `Box<dyn AsyncRead>` for illustration. In practice, the Windows `into_split()` will need to construct the boxed types from `ReadHalf<NamedPipeServer>` or `ReadHalf<NamedPipeClient>`. Both implement `AsyncRead`, so this works. An alternative (and arguably cleaner) approach: define a single-variant wrapper for each platform side (Server vs Client) and implement `AsyncRead`/`AsyncWrite` on the wrapper enum. This avoids boxing. Decide during implementation which feels cleaner.

#### 1.2 Modify `crates/truffle-cli/src/daemon/mod.rs`

Add the new module:

```rust
pub mod ipc;  // Add this line
```

#### 1.3 Modify `crates/truffle-cli/src/daemon/server.rs`

**Changes:**
1. Replace `use tokio::net::UnixListener;` with `use super::ipc;`
2. Replace `UnixListener::bind(&self.socket_path)` with `ipc::IpcListener::bind(&self.socket_path)`
3. Replace `handle_connection(stream: tokio::net::UnixStream, ...)` with the IPC abstraction's split API
4. Gate socket file cleanup with `#[cfg(unix)]`

Key diff for `run()`:
```rust
// BEFORE:
let listener = UnixListener::bind(&self.socket_path)
    .map_err(|e| format!("Failed to bind Unix socket at {}: {e}", self.socket_path.display()))?;
// ...
let (stream, _addr) = listener.accept().await?;
// ...
let (reader, mut writer) = stream.into_split();
let mut lines = BufReader::new(reader).lines();

// AFTER:
let listener = ipc::IpcListener::bind(&self.socket_path)
    .map_err(|e| format!("Failed to bind IPC at {}: {e}", self.socket_path.display()))?;
// ...
let stream = listener.accept().await?;
// ...
let (mut reader, mut writer) = stream.into_split();
// Use reader.next_line() instead of lines.next_line()
// Use writer.write_line(&resp_json) instead of writer.write_all(resp_json.as_bytes())
```

#### 1.4 Modify `crates/truffle-cli/src/daemon/client.rs`

**Changes:**
1. Replace `use tokio::net::UnixStream;` with `use super::ipc;`
2. Replace `UnixStream::connect(&self.socket_path).await` with `ipc::IpcStream::connect(&self.socket_path).await`
3. Fix `ensure_running()` readiness check:

```rust
// BEFORE:
if self.socket_path.exists() && self.is_daemon_running() {
    if UnixStream::connect(&self.socket_path).await.is_ok() {
        return Ok(());
    }
}

// AFTER:
if self.is_daemon_running() {
    // On Windows, named pipe paths don't respond to exists().
    // Try connecting directly.
    if ipc::IpcStream::connect(&self.socket_path).await.is_ok() {
        return Ok(());
    }
}
```

#### 1.5 Modify `crates/truffle-cli/src/daemon/pid.rs`

**Changes:**
1. Gate `libc::kill` behind `#[cfg(unix)]`
2. Add Windows implementation using `windows-sys`

```rust
#[cfg(unix)]
pub fn is_process_running(pid: u32) -> bool {
    unsafe { libc::kill(pid as libc::pid_t, 0) == 0 }
}

#[cfg(windows)]
pub fn is_process_running(pid: u32) -> bool {
    use windows_sys::Win32::Foundation::CloseHandle;
    use windows_sys::Win32::System::Threading::{
        OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
    };
    unsafe {
        let handle = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid);
        if handle == 0 {
            false
        } else {
            CloseHandle(handle);
            true
        }
    }
}
```

**Note:** `windows-sys` uses `isize` (aliased to `HANDLE`) and returns `0` (not null pointer) on failure. The strategy doc uses `.is_null()` which is incorrect for `windows-sys` (it would be correct for `windows` crate). The code above uses `== 0` which is correct for `windows-sys`.

#### 1.6 Modify `crates/truffle-cli/src/config.rs`

**Changes:** Make `socket_path()` return named pipe path on Windows.

```rust
/// The IPC path: Unix socket or Windows named pipe.
pub fn socket_path() -> PathBuf {
    #[cfg(unix)]
    {
        config_dir().join("truffle.sock")
    }
    #[cfg(windows)]
    {
        PathBuf::from(r"\\.\pipe\truffle-daemon")
    }
}
```

#### 1.7 Modify `crates/truffle-cli/src/commands/up.rs`

**Changes:** Add Windows process detachment flag.

```rust
// After the existing #[cfg(unix)] block (line 238-247), add:
#[cfg(windows)]
{
    use std::os::windows::process::CommandExt;
    const CREATE_NO_WINDOW: u32 = 0x08000000;
    cmd.creation_flags(CREATE_NO_WINDOW);
}
```

#### 1.8 Modify `crates/truffle-cli/Cargo.toml`

```toml
[dependencies]
# ... keep all existing ...

[target.'cfg(unix)'.dependencies]
libc = "0.2"

[target.'cfg(windows)'.dependencies]
windows-sys = { version = "0.59", features = [
    "Win32_System_Threading",
    "Win32_Foundation",
] }
```

Remove the unconditional `libc = "0.2"` line from `[dependencies]`.

#### 1.9 Files to modify (summary)

| File | Action |
|------|--------|
| `crates/truffle-cli/src/daemon/ipc.rs` | **CREATE** -- cross-platform IPC abstraction |
| `crates/truffle-cli/src/daemon/mod.rs` | MODIFY -- add `pub mod ipc;` |
| `crates/truffle-cli/src/daemon/server.rs` | MODIFY -- use `ipc::IpcListener`, gate socket cleanup |
| `crates/truffle-cli/src/daemon/client.rs` | MODIFY -- use `ipc::IpcStream`, fix readiness check |
| `crates/truffle-cli/src/daemon/pid.rs` | MODIFY -- `#[cfg]` gate `is_process_running()` |
| `crates/truffle-cli/src/config.rs` | MODIFY -- `socket_path()` returns pipe path on Windows |
| `crates/truffle-cli/src/commands/up.rs` | MODIFY -- add `CREATE_NO_WINDOW` on Windows |
| `crates/truffle-cli/Cargo.toml` | MODIFY -- conditional deps for libc/windows-sys |

#### Verification

1. `cargo build` on macOS/Linux -- must compile without warnings.
2. `cargo build --target x86_64-pc-windows-msvc` -- cross-compile check (if cross-compilation toolchain available). Otherwise verify on CI.
3. Run existing tests: `cargo test -p truffle-cli` -- all existing tests must pass.
4. Add IPC tests (see Phase 5).

---

### Phase 2: Sidecar Discovery

**Goal:** `find_sidecar()` works on all platforms with .exe suffix, `where` command, and npm global search.

**Dependencies:** None (independent of Phase 1).

**Estimated effort:** 1-2 hours.

#### 2.1 Modify `crates/truffle-cli/src/sidecar.rs`

**Changes:**
1. Add `sidecar_bin_name()` and `sidecar_search_names()` with platform-aware names.
2. Add `.exe` suffix awareness to `SIDECAR_NAMES` and all `join()` calls.
3. Replace `which` command with `where` on Windows (or use the `which` crate).
4. Add `find_in_npm_global()` for npm global search.
5. Add `find_in_homebrew()` for Homebrew prefix search (Unix-only).
6. Pass `bin_name` to `walk_up_for_workspace()`.

The strategy doc provides complete code for this. Key changes from current code:

```rust
// Current (broken on Windows):
const SIDECAR_NAMES: &[&str] = &["truffle-sidecar", "sidecar-slim"];

// Fixed:
fn sidecar_bin_name() -> &'static str {
    if cfg!(windows) { "sidecar-slim.exe" } else { "sidecar-slim" }
}

fn sidecar_search_names() -> &'static [&'static str] {
    if cfg!(windows) {
        &["truffle-sidecar.exe", "sidecar-slim.exe"]
    } else {
        &["truffle-sidecar", "sidecar-slim"]
    }
}
```

```rust
// Current (broken on Windows):
fn which_binary(name: &str) -> Option<PathBuf> {
    std::process::Command::new("which")

// Fixed:
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
                // `where` on Windows may return multiple lines; take the first
                String::from_utf8_lossy(&output.stdout)
                    .lines()
                    .next()
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .map(PathBuf::from)
            } else {
                None
            }
        })
}
```

Add npm global search (new function):
```rust
fn find_in_npm_global(bin_name: &str) -> Option<PathBuf> {
    let output = std::process::Command::new("npm")
        .args(["root", "-g"])
        .output()
        .ok()?;
    if !output.status.success() { return None; }
    let npm_root = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if npm_root.is_empty() { return None; }

    let pkg_name = platform_npm_package()?;
    let candidate = PathBuf::from(&npm_root).join(pkg_name).join("bin").join(bin_name);
    if candidate.exists() && is_executable(&candidate) {
        Some(candidate)
    } else {
        None
    }
}

fn platform_npm_package() -> Option<&'static str> {
    if cfg!(all(target_os = "macos", target_arch = "aarch64")) {
        Some("@vibecook/truffle-sidecar-darwin-arm64")
    } else if cfg!(all(target_os = "macos", target_arch = "x86_64")) {
        Some("@vibecook/truffle-sidecar-darwin-x64")
    } else if cfg!(all(target_os = "linux", target_arch = "x86_64")) {
        Some("@vibecook/truffle-sidecar-linux-x64")
    } else if cfg!(all(target_os = "linux", target_arch = "aarch64")) {
        Some("@vibecook/truffle-sidecar-linux-arm64")
    } else if cfg!(all(target_os = "windows", target_arch = "x86_64")) {
        Some("@vibecook/truffle-sidecar-win32-x64")
    } else {
        None
    }
}
```

**Note on `cfg!()` vs `#[cfg()]`:** The strategy doc uses `#[cfg()]` attribute syntax for `platform_npm_package()`, which requires separate function bodies per platform variant. Using `cfg!()` macro (runtime check) is simpler and produces the same result since the compiler optimizes dead branches away. Either approach works; `cfg!()` is easier to read.

#### 2.2 Files to modify

| File | Action |
|------|--------|
| `crates/truffle-cli/src/sidecar.rs` | MODIFY -- platform-aware names, npm global, where vs which |

#### Verification

1. `cargo test -p truffle-cli -- sidecar` -- existing sidecar tests pass.
2. On macOS: verify `find_sidecar()` still finds the workspace sidecar.
3. Manual test: `npm install -g @vibecook/truffle-sidecar-darwin-arm64` then verify `find_in_npm_global()` finds it.

---

### Phase 3: Build Infrastructure (CI/CD)

**Goal:** Produce multi-platform release artifacts containing both the Rust CLI and Go sidecar.

**Dependencies:** Phases 1 and 2 should be complete so the built binary actually works on all platforms.

**Estimated effort:** 2-3 hours.

#### 3.1 Create `.github/workflows/release-cli.yml`

New file. Builds CLI + sidecar for 5 platforms, packages them together as release assets.

The strategy doc provides a complete workflow. Key points to validate/adjust:

1. **Go version:** The strategy says `go-version: '1.25'` matching the existing `release-sidecar.yml`. Verify this matches `go.mod`.
2. **Rust cross-compilation for Linux ARM64:** Requires `gcc-aarch64-linux-gnu` linker. Already handled in `napi-build.yml`, so the pattern is proven.
3. **Windows packaging uses `7z`:** This is available on `windows-latest` runners by default. Correct.
4. **Both binaries in same directory:** The tarball/zip contains `truffle` and `sidecar-slim` at the top level. This is critical because `find_sidecar()` location #3 looks for the sidecar adjacent to the CLI binary.

**Adjustment needed:** The strategy workflow does not include `CGO_ENABLED: '0'` for the Go sidecar build on Windows. The existing `release-sidecar.yml` does set this. Add it to ensure static linking.

**Adjustment needed:** On Windows, the Rust binary is `truffle.exe`, not `truffle`. The `Package (Windows)` step correctly copies `truffle.exe` but verify the binary name in the output of `cargo build`.

#### 3.2 Update CI to test on multiple platforms

Modify `.github/workflows/ci.yml` to add a Rust test matrix:

```yaml
  cargo-test:
    strategy:
      matrix:
        os: [ubuntu-latest, macos-latest, windows-latest]
    runs-on: ${{ matrix.os }}
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - run: cargo test --workspace --exclude truffle-tauri-plugin
```

This validates that the Rust code compiles and tests pass on all three platforms.

#### 3.3 Files to create/modify

| File | Action |
|------|--------|
| `.github/workflows/release-cli.yml` | **CREATE** -- multi-platform CLI release workflow |
| `.github/workflows/ci.yml` | MODIFY -- add multi-platform Rust test matrix |

#### Verification

1. Push to a branch and open a PR. CI should run `cargo test` on all three platforms.
2. Create a test release tag (e.g., `v0.2.0-rc.1`) and verify `release-cli.yml` produces 5 release assets.
3. Download each asset, extract, verify both binaries are present and the CLI can find the sidecar.

---

### Phase 4: Distribution Channels

**Goal:** Users can install truffle with a single command on any platform.

**Dependencies:** Phase 3 (release artifacts must exist).

**Estimated effort:** 3-4 hours total across all channels.

#### 4.1 Install script (`scripts/install.sh`)

Create a shell script for Unix (macOS/Linux):

```bash
#!/bin/sh
set -e

REPO="vibecook-dev/truffle"
INSTALL_DIR="${TRUFFLE_INSTALL_DIR:-$HOME/.config/truffle/bin}"

# Detect platform
OS=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH=$(uname -m)
case "$ARCH" in
    x86_64|amd64) ARCH="x64" ;;
    aarch64|arm64) ARCH="arm64" ;;
    *) echo "Unsupported architecture: $ARCH"; exit 1 ;;
esac

ASSET="truffle-${OS}-${ARCH}.tar.gz"
URL="https://github.com/${REPO}/releases/latest/download/${ASSET}"

echo "Downloading truffle for ${OS}-${ARCH}..."
mkdir -p "$INSTALL_DIR"
curl -fsSL "$URL" | tar xz -C "$INSTALL_DIR"
chmod +x "$INSTALL_DIR/truffle" "$INSTALL_DIR/sidecar-slim"

echo "Installed to $INSTALL_DIR"

# Check if in PATH
case ":$PATH:" in
    *":$INSTALL_DIR:"*) ;;
    *)
        echo ""
        echo "Add to your PATH:"
        echo "  export PATH=\"$INSTALL_DIR:\$PATH\""
        echo ""
        echo "Add this to your ~/.bashrc, ~/.zshrc, or equivalent."
        ;;
esac

echo "Run 'truffle up' to start."
```

#### 4.2 PowerShell install script (`scripts/install.ps1`)

```powershell
$ErrorActionPreference = "Stop"

$repo = "vibecook-dev/truffle"
$installDir = if ($env:TRUFFLE_INSTALL_DIR) { $env:TRUFFLE_INSTALL_DIR }
              else { "$env:LOCALAPPDATA\truffle\bin" }

$asset = "truffle-win32-x64.zip"
$url = "https://github.com/$repo/releases/latest/download/$asset"

Write-Host "Downloading truffle for windows-x64..."
New-Item -ItemType Directory -Force -Path $installDir | Out-Null

$tempZip = Join-Path $env:TEMP "truffle-install.zip"
Invoke-WebRequest -Uri $url -OutFile $tempZip
Expand-Archive -Path $tempZip -DestinationPath $installDir -Force
Remove-Item $tempZip

Write-Host "Installed to $installDir"

# Add to user PATH if not already present
$userPath = [Environment]::GetEnvironmentVariable("Path", "User")
if ($userPath -notlike "*$installDir*") {
    [Environment]::SetEnvironmentVariable("Path", "$installDir;$userPath", "User")
    Write-Host "Added to user PATH. Restart your terminal."
}

Write-Host "Run 'truffle up' to start."
```

#### 4.3 `truffle install-sidecar` subcommand

New subcommand for the `cargo install` channel. Downloads the sidecar binary for the current platform.

**File:** `crates/truffle-cli/src/commands/install_sidecar.rs` (new)

Key logic:
1. Determine `(os, arch)` from `std::env::consts::{OS, ARCH}`.
2. Map to GitHub release asset name.
3. Download to `~/.config/truffle/bin/sidecar-slim[.exe]`.
4. Set executable permission on Unix.

This requires adding an HTTP client dependency. Options:
- `ureq` (minimal, blocking, ~200KB) -- simplest for a one-off download.
- `reqwest` (async, full-featured, larger) -- already indirectly depended on? Check.

**Recommendation:** Use `ureq` since it's a single blocking call and avoids pulling in a large async HTTP stack.

Add to `Cargo.toml`:
```toml
[dependencies]
ureq = "3"
```

Add to CLI command dispatch in `main.rs`.

#### 4.4 Auto-download on `truffle up` (fallback)

Modify `find_sidecar()` error path: when no sidecar is found and `auto_up` is true, invoke the download logic before returning an error.

Alternatively, modify the `DaemonServer::start()` to catch the sidecar-not-found error and run the download. This keeps `find_sidecar()` pure (search only, no side effects).

#### 4.5 Homebrew formula

**File:** `Formula/truffle.rb` or in a separate homebrew-truffle tap repo.

The strategy doc provides a complete formula. The formula downloads pre-built tarballs, so no compilation is needed.

**Recommendation:** Create a `homebrew-truffle` repo at `github.com/vibecook-dev/homebrew-truffle` so users can `brew tap vibecook-dev/truffle && brew install truffle`.

The formula should be auto-updated by CI on each release (use a GitHub Action to update the SHA256 and version in the formula).

#### 4.6 Files to create/modify

| File | Action |
|------|--------|
| `scripts/install.sh` | **CREATE** -- Unix install script |
| `scripts/install.ps1` | **CREATE** -- Windows install script |
| `crates/truffle-cli/src/commands/install_sidecar.rs` | **CREATE** -- download subcommand |
| `crates/truffle-cli/src/commands/mod.rs` | MODIFY -- add `install_sidecar` module |
| `crates/truffle-cli/src/main.rs` | MODIFY -- add `install-sidecar` to CLI commands |
| `crates/truffle-cli/Cargo.toml` | MODIFY -- add `ureq` dependency |
| `Formula/truffle.rb` (or separate repo) | **CREATE** -- Homebrew formula |

#### Verification

1. Run `scripts/install.sh` on macOS and Linux. Verify both binaries are installed and `truffle up` works.
2. Run `scripts/install.ps1` on Windows. Verify installation and PATH.
3. `cargo install --path crates/truffle-cli && truffle install-sidecar` -- verify sidecar download.
4. `truffle up` after `cargo install` (no pre-installed sidecar) -- verify auto-download prompt.
5. `brew install vibecook-dev/truffle/truffle` -- verify Homebrew installation.

---

### Phase 5: Testing

**Goal:** Comprehensive test coverage for cross-platform behavior.

**Dependencies:** Phases 1-4 complete.

**Estimated effort:** 2-3 hours.

#### 5.1 IPC integration tests

**File:** `crates/truffle-cli/src/daemon/ipc.rs` (add `#[cfg(test)]` module)

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_ipc_roundtrip() {
        let path = ipc_path_for_test();

        let listener = IpcListener::bind(&path).unwrap();

        // Spawn a client in a separate task
        let client_path = path.clone();
        let client_task = tokio::spawn(async move {
            // Small delay to ensure server is ready
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
            let stream = IpcStream::connect(&client_path).await.unwrap();
            let (mut reader, mut writer) = stream.into_split();
            writer.write_line(r#"{"id":1,"method":"ping"}"#).await.unwrap();
            let response = reader.next_line().await.unwrap().unwrap();
            assert!(response.contains("pong"));
        });

        // Server accepts and echoes
        let stream = listener.accept().await.unwrap();
        let (mut reader, mut writer) = stream.into_split();
        let line = reader.next_line().await.unwrap().unwrap();
        assert!(line.contains("ping"));
        writer.write_line(r#"{"result":"pong"}"#).await.unwrap();

        client_task.await.unwrap();

        // Cleanup
        #[cfg(unix)]
        let _ = std::fs::remove_file(&path);
    }

    fn ipc_path_for_test() -> PathBuf {
        #[cfg(unix)]
        {
            let dir = tempfile::tempdir().unwrap();
            // Leak the tempdir so it persists for the test
            let path = dir.path().join("test.sock");
            std::mem::forget(dir);
            path
        }
        #[cfg(windows)]
        {
            use std::sync::atomic::{AtomicU32, Ordering};
            static COUNTER: AtomicU32 = AtomicU32::new(0);
            let n = COUNTER.fetch_add(1, Ordering::Relaxed);
            PathBuf::from(format!(r"\\.\pipe\truffle-test-{}-{n}", std::process::id()))
        }
    }
}
```

#### 5.2 PID cross-platform tests

**File:** `crates/truffle-cli/src/daemon/pid.rs` -- existing tests work on all platforms (they use `std::process::id()` which is cross-platform). Verify they pass on Windows CI.

Add one Windows-specific test:
```rust
#[cfg(windows)]
#[test]
fn test_is_process_running_windows() {
    // Current process should be running
    assert!(is_process_running(std::process::id()));
    // Nonexistent PID should not be running
    assert!(!is_process_running(u32::MAX));
}
```

#### 5.3 Sidecar discovery tests

Add tests for the new search locations:
```rust
#[test]
fn test_sidecar_bin_name_has_exe_on_windows() {
    let name = sidecar_bin_name();
    if cfg!(windows) {
        assert!(name.ends_with(".exe"));
    } else {
        assert!(!name.contains(".exe"));
    }
}

#[test]
fn test_platform_npm_package_returns_some() {
    // Should return Some for all supported platforms
    let pkg = platform_npm_package();
    assert!(pkg.is_some(), "platform_npm_package() returned None");
    assert!(pkg.unwrap().starts_with("@vibecook/truffle-sidecar-"));
}
```

#### 5.4 CI multi-platform test matrix

Already added in Phase 3 (section 3.2). Ensure the `cargo-test` job in `ci.yml` runs on `ubuntu-latest`, `macos-latest`, and `windows-latest`.

#### 5.5 Files to create/modify

| File | Action |
|------|--------|
| `crates/truffle-cli/src/daemon/ipc.rs` | MODIFY -- add `#[cfg(test)]` module |
| `crates/truffle-cli/src/daemon/pid.rs` | MODIFY -- add Windows-specific test |
| `crates/truffle-cli/src/sidecar.rs` | MODIFY -- add cross-platform tests |
| `.github/workflows/ci.yml` | Already modified in Phase 3 |

#### Verification

1. `cargo test -p truffle-cli` passes on macOS.
2. CI runs and passes on all three platforms.
3. IPC roundtrip test passes on current platform.

---

## Appendix A: Risk Matrix

| Risk | Severity | Likelihood | Mitigation |
|------|----------|------------|------------|
| Windows named pipe API differences from Unix sockets | Medium | Medium | Thorough integration tests in Phase 5 |
| Windows Defender blocking unsigned binaries | Medium | High | Document workaround; code signing in v1.0 |
| PID reuse on Windows causing false positives | Low | Low | Acceptable for now; add start time check later |
| `setsid` equivalent missing on Windows | Medium | Certain | `CREATE_NO_WINDOW` flag added in Phase 1 |
| `socket_path.exists()` failing for named pipes | High | Certain | Fixed in Phase 1 (client.rs readiness check) |
| Go sidecar Windows firewall prompts | Low | High | Document in install guide |
| `dirs::config_dir()` returning different paths per OS | Low | Certain | Already correct (uses `dirs` crate); fix docs |

## Appendix B: Complete File Change List

| Phase | File | Action | Est. Lines |
|-------|------|--------|------------|
| 1 | `crates/truffle-cli/src/daemon/ipc.rs` | CREATE | ~180 |
| 1 | `crates/truffle-cli/src/daemon/mod.rs` | MODIFY | +1 |
| 1 | `crates/truffle-cli/src/daemon/server.rs` | MODIFY | ~20 changed |
| 1 | `crates/truffle-cli/src/daemon/client.rs` | MODIFY | ~15 changed |
| 1 | `crates/truffle-cli/src/daemon/pid.rs` | MODIFY | +15 |
| 1 | `crates/truffle-cli/src/config.rs` | MODIFY | +8 |
| 1 | `crates/truffle-cli/src/commands/up.rs` | MODIFY | +6 |
| 1 | `crates/truffle-cli/Cargo.toml` | MODIFY | +6 |
| 2 | `crates/truffle-cli/src/sidecar.rs` | MODIFY | ~80 changed |
| 3 | `.github/workflows/release-cli.yml` | CREATE | ~80 |
| 3 | `.github/workflows/ci.yml` | MODIFY | +10 |
| 4 | `scripts/install.sh` | CREATE | ~40 |
| 4 | `scripts/install.ps1` | CREATE | ~25 |
| 4 | `crates/truffle-cli/src/commands/install_sidecar.rs` | CREATE | ~100 |
| 4 | `crates/truffle-cli/Cargo.toml` | MODIFY | +1 |
| 5 | Various test modules | MODIFY | ~80 |

**Total new/changed: ~650 lines** (the strategy doc estimated ~200 for steps 1-5; the actual number is higher due to proper error handling, tests, and the Windows type-erasure complexity in `ipc.rs`).

## Appendix C: Recommended Execution Order

```
Phase 1 ──────────────────┐
Phase 2 ──────────────────┤ (can be done in parallel)
                          ├──> Phase 3 ──> Phase 4
Phase 5 ──────────────────┘ (start tests after Phase 1)
```

Phases 1 and 2 are independent and can be done in parallel or in either order. Phase 3 depends on both. Phase 4 depends on Phase 3. Phase 5 can start as soon as Phase 1 is done (IPC tests) and grows as other phases complete.

**Recommended first PR:** Phases 1 + 2 together (all Rust code changes). This is a single coherent PR titled "feat: cross-platform IPC and sidecar discovery" that makes the codebase compile and work on Windows without changing any CI or distribution.

**Recommended second PR:** Phase 3 (CI workflows). Test in a draft PR to verify all matrix entries build correctly.

**Recommended third PR:** Phase 4 (distribution scripts and install-sidecar command).

Phase 5 testing should be woven into each PR rather than done as a separate pass.
