# RFC 028: Truffle on Android — PICO OS 6 with the Kotlin Spatial SDK

**Status**: Draft
**Author**: James Yong + Claude
**Date**: 2026-10-25
**Depends on**: RFC 012 (layered architecture), RFC 016 (SyncedStore, request/reply; amended by §6 R4), RFC 017 (identity & namespacing), RFC 024 (Truffle Swift; the in-process precedent and its §11.1 revisit condition), RFC 025 (login-scoped nodes), RFC 027 (truffle-bench, used for the device gates)
**Code citations**: checked against `b507208` (truffle 0.7.21). Paths are abbreviated: `node.rs` = `crates/truffle-core/src/node.rs`, `main.go` = `packages/sidecar-slim/main.go`, `sync.rs` = `crates/truffle-core/src/synced_store/sync.rs`, and so on.
**Evidence tags**: **[V]** verified against the repo at that commit or a primary source (URL and date), independently fact-checked · **[V¹]** read from a primary source by one investigation, not re-checked · **[I]** inferred, needs the PICO Emulator, a device or a measurement. All effort figures are estimates [I].
**Scope note**: an earlier internal draft also covered Unity, PICO 4 Ultra (OS 5) and a foreground service. The owner's decisions in §1 removed them; §1 records what was cut.

---

## 1. Summary and decision record

**Plan.** Ship the existing Rust `truffle-core` plus the Go `sidecar-slim` on Android:
- The sidecar is built as a `GOOS=android` c-shared library and driven by a new in-process launcher.
- Kotlin reaches Rust through a **hand-written JNI bridge** of about 8 natives (`jni = 0.22.4`), sitting on a host-testable Rust dispatcher. Kotlin sends commands as JSON; results and events come back through one polled queue; Rust never calls into the JVM.
- **Lifecycle is "mesh while in use":** a clean stop when the app leaves use, and a rebuild from the same `state_dir` when it returns. There is no suspend/resume and no foreground service.
- **Login** is interactive against the owner's personal tailnet: system browser first, then a typeable URL, with QR only for the emulator or casting.
- **Wire contract is unchanged:** hello v2, envelope, close codes. Hello reports `os = "android"` via `std::env::consts::OS` (`transport/websocket.rs:113`).

**Cost.** About **20.5 engineer-weeks (range 16–26)** to GA on the PICO Emulator plus an Android 16 phone. Space Pro qualification is gated on the calendar. That is roughly 11–14 calendar weeks with two engineers. The broader draft (Unity + Kotlin, PICO 4 Ultra, foreground service) was estimated at 23 (19–29). Removing Unity, the FGS and the OS 5 work saves about 6 ew. New auth-lifecycle work, the lifecycle driver and a larger Kotlin binding add about 3.5 ew back.

### Owner decisions and their effect

| # | Decision | Cut from the broader draft | Kept or added |
|---|---|---|---|
| 1 | **Kotlin + PICO Spatial SDK; no Unity** | C ABI, cbindgen/csbindgen, UPM and the editor sidecar package, IL2CPP soak in G2, GameCI, the Unity auth prefab, the "Java 8 zero-dependency runtime" (it only existed for Unity's AGP 7.1.2) | **Kept:** the dispatcher design (handles, op-ids, polled queue, JSON Schema). **Added:** Kotlin facade (coroutines, Flow, kotlinx.serialization); a **no-Compose** AAR, because PICO forks Compose UI. A C ABI can be added later over the same dispatcher (≈0.5–1 ew) [I]. |
| 2 | **PICO OS 6 only** (Space Pro + PICO Emulator) | PICO 4 Ultra/OS 5 gates, Android 14 lifecycle rows, Quest smoke | **Kept:** arm64-v8a release, plus an x86_64 build for CI and the Windows emulator (dev only). **Added:** Android 16 phone and stock emulator as labelled proxies, because Space Pro is not shipping. |
| 3 | **Personal tailnet, interactive login** | Operator mode / KeyProvider, `NodeBuilder::tags()` (`node.rs:2126` is `tags: None`), RFC 025 tag globs, backend-minted keys | **Kept:** `authKey` and `controlUrl` as **test-only** hooks for hermetic testcontrol. **Added:** auth lifecycle: logout, post-start login, URL redaction, expiry UX, `SameAccount` gate. |
| 4 | **China out of scope** | Headscale/private DERP validation, CN store/portal, PIPL review | **Kept:** logtail upload off on Android, because of store privacy declarations, not China. |
| 5 | **No background mesh** | `truffle-service` FGS (connectedDevice/dataSync), FGS permissions and store questions, `Node::suspend/resume`, "suspending" presence, sleep-gap detector, mobile ping/pong knobs, the "1 h reachable" gate | **Added:** stop→rebuild lifecycle; SyncedStore restart fixes C1–C4; heartbeat fix C5; awaitable in-process stop; NeedsLogin exemption. |

---

## 2. Key facts that still matter

| # | Fact | Tag |
|---|---|---|
| F1 | OS 6 runs only on PICO Space Pro (not shipping; "Q4 2026") and the PICO Emulator 6.1 (Apple Silicon M1 Pro+, macOS 14, 16 GB; or Windows x86 with Houdini ARM translation). No GMS. Source: developer.picoxr.com install-pico-emulator and release notes, 2026-10-24. | V |
| F2 | A Spatial SDK app is **one process**. Each container (WindowContainer, Volume, Stage) is one Activity, "typically in a different task". `Application.onCreate` calls `launch(::mainApp)` and only the default container auto-opens. SDK AAR manifests declare no `android:process`. Sources: developer.picoxr.com port-android-apps and register-window-containers; Volcengine Maven 6.1.9 AARs; 2026-10-24. | V |
| F3 | **Container lifecycle:** minimize → PAUSE→STOP; close → PAUSE→STOP→DESTROY. Focus, occlusion, Home and head-turn raise only `SpatialContainerStateEvent` (FOCUSED/STAGED/SIGHTED), never PAUSE. The Stage "close app" action lists **ON_DESTROY only**, possibly a process kill. Bytecode shows the container's LifecycleOwner is the Activity (`SpatialStubActivity` extends `FragmentActivity`). Source: picoxr manage-the-lifecycle docs, 2026-10-24. | V docs and bytecode; runtime not yet observed |
| F4 | **Toolchain baseline:**<br>• compile/targetSdk 35<br>• minSdk 26 in 6 of 8 PICO samples (35 in SpatialUI; the porting guide says ≥35)<br>• AGP 8.8.x (the guide warns about release builds below 8.8.0)<br>• Kotlin 2.0.0/2.0.21, Java 11, Android Studio 2025.1.x only, BOM 6.1.9<br>• ui:* AARs need minCompileSdk 35; core needs 29.<br>Source: 8 PICO sample repos and AAR metadata, 2026-10-24. | V |
| F5 | **PICO forks Compose UI** (`com.pico.spatial.ui:ui:1.2.5` on Compose 1.10.4). Apps must exclude `androidx.compose.ui:ui`, `ui-graphics`, `ui-text` and `foundation`. The SDK pins lifecycle 2.10.0, coroutines-android 1.9.0 and kotlin-stdlib 2.0.21. **The Truffle AAR must not depend on Compose.** Source: Volcengine POMs, 2026-10-24. | V |
| F6 | **Emulator networking** is AOSP-style SLIRP NAT: host at `10.0.2.2`, outbound only, no ICMP. PICO's WebSpatial doc tells developers to use 10.0.2.2. The emulator-manifest repo was last pushed 2025-11-06. The Mac guest ABI is arm64 by inference (HVF, VGICv3, Intel Macs rejected). 6.1.x adds a screen-off simulation. | V / ABI I |
| F7 | **Netlink blocker:** tsnet fails with "netlinkrib: permission denied" under an app UID on Android 11+. Fix: a host-fed `netmon.RegisterInterfaceGetter`, the tailscale-android pattern. `feature/androidbin` is inert in cgo/c-shared builds. | V (brief §C) |
| F8 | A Go c-shared library on android/arm64 starts with an **empty environment** (go1.27.3 `rt0_android_arm64.s`). `TS_CONTROL_URL` (`main.go:946`), `TRUFFLE_DEBUG`, TMPDIR and HOME are therefore unset. With `GOOS=android` the LocalAPI cert route is compiled out (`ipn/localapi/cert.go` is `//go:build !ios && !android && …` in v1.104.1). `ListenTLS` and `CertPair` still compile, but both fetch certificates through that route, so TLS listeners and cert pre-warm fail at runtime. The sidecar calls them at `main.go:1589`, `2336` and `3675`. | V |
| F9 | **Android 15+ background network.**<br>• The BACKGROUND chain blocks a UID 2 s after it becomes cached, and 20 s after it enters TOP_SLEEPING or LAST_ACTIVITY. Both delays are server-flag gated.<br>• ConnectivityService **destroys a blocked or frozen UID's non-loopback TCP sockets** (`isAtLeastV`). That kills tsnet's control long-poll and DERP; UDP and loopback survive.<br>• The freezer debounce is 10 s.<br>Sources: android.googlesource.com NetworkPolicyManagerService (android15-release :566-594, 4407-4413) and ConnectivityService (main), 2026-10-24. Which flags PICO sets is unknown. | V AOSP / I PICO |
| F10 | **OS 5 store rules** PXRC-System-R4001 (app not put to sleep during the 10 s–5 min screen-off after removal) and **R4002 (downloads continue after doff)** are MUSTs. No OS 6 rules are published. Source: developer.picoxr.com app-review-overview, 2026-10-24. | V |
| F11 | **Restart behaviour.**<br>• `Node::stop()` is one-way (`node.rs:769`), so a restart means a rebuild.<br>• A rebuild from the same `state_dir` keeps `device-id.txt` (the ULID), the tsnet keys and the Tailscale ID.<br>• The session layer replaces a restarted peer's stale connection (`session/mod.rs:359-372`, `1498-1647`). This is tested over real tsnet in `tests/integration_session_restart.rs`. | V |
| F12 | **SyncedStore breaks on a rebuild:**<br>• MemoryBackend is the default (`synced_store/mod.rs:100`) and the version restarts at 0.<br>• Peers reject `version <= existing` (`sync.rs:300-311`).<br>• Slices are dropped only on `Left` (`sync.rs:72-77`).<br>• `WsConnected` is ignored (`sync.rs:78`).<br>• `SyncMessage::Clear` exists (`types.rs:60`) but is never sent.<br>• A store created after `build()` never sends `Request`. | V / late-store case I |
| F13 | A Rust WS heartbeat timeout does not wake a `recv()` already blocked in `read.next()` (`transport/websocket.rs:904-1006`). An idle session therefore keeps `ws_connected=true` until the next send. That send returns `Ok` because it is only queued (`session/mod.rs:1139`). The connection task's `WsFramedStream::send` then fails with `ConnectionClosed` because `closed` is set (`websocket.rs:957-961`), so the task logs a warning, drops the frame and retires the connection (`session/mod.rs:1921-1930`), which marks the peer `WsDisconnected`. One message is lost with no error to the caller; a later send redials. Swift ends the session on timeout. | V (code read; not reproduced) |
| F14 | **Auth gaps today:**<br>• The sidecar surfaces login only inside `pollUntilRunning` (`main.go:1200-1272`).<br>• There is no `login` or `logout` command (`main.go:1027-1076`).<br>• After Running, NeedsLogin only updates `health.state` (`provider.rs:689-702`).<br>• The URL is logged in plain text at `provider.rs:661`, `tui/mod.rs:723` and via sidecar stderr (`sidecar.rs:322`; tsnet `printAuthURLLoop` with `UserLogf` unset, `main.go:1149-1156`).<br>• The CLI shows the URL only through that stderr: `DaemonServer` uses `.build()` (`daemon/server.rs:77-92`) and onboarding's `_auth_tx` is dead (`tui/onboarding.rs:208`). | V |
| F15 | **tsnet login behaviour:**<br>• tsnet never asks for a new login URL after Running.<br>• The **pending node key lives only in memory** until the URL is completed (direct.go v1.104.1 :888-895), so tearing down mid-login throws the login away.<br>• Restarting from an expired key does produce a URL (direct.go :861-867). | V¹ |
| F16 | **Login surface:**<br>• Google blocks OAuth in embedded WebViews: developers.googleblog.com, 2021-06, re-checked 2026-10-24.<br>• A QR drawn inside the headset cannot be scanned by a phone.<br>• The OS 6 handler for `ACTION_VIEW https`, Custom Tabs support and WebView presence are undocumented. | V / QR I |
| F17 | **Defaults that fail on Android:** `device_name` falls back to `hostname::get()` → "localhost" (`node.rs:2042`), and `state_dir` falls back to `dirs::data_dir()` → None, a BuildError (`node.rs:2072`). | V |
| F18 | **Binding landscape (2026-10-24):**<br>• UniFFI #2624 is open with no workaround, and one trace hits a **sync** JNA free path. #2953 is an open deadlock.<br>• `uniffi-bindgen-kotlin-jni` is unreleased and not on crates.io; application-services #7607 is unmerged.<br>• BoltFFI was first published 2026-02-17, has 2 reverse deps, and use-after-close #664 is open.<br>• jni 0.22.4 (2026-03-16) has `EnvUnowned::with_env` and `ErrorPolicy`, and libsignal ships the polled/completed-future pattern on it. jni-rs has made no release since, and fixes sit on main (#811, #817).<br>• jni 0.22.4 is already in `Cargo.lock:2472`. | V |
| F19 | **Version pins:** Go 1.27.3 is already pinned for the sidecar (`packages/sidecar-slim/go.mod:3`; all workflows except `apple-tests.yml:89`, which is still 1.27.2, as is the vendored `apple/Vendor/libtailscale/go.mod:3`). `tailscale.com` is still v1.104.1 (`packages/sidecar-slim/go.mod:7`). The Apple graph stays on Go 1.27.2 deliberately until the combined Tailscale 1.106 upgrade (`docs/tailscale-upgrade.md`), because changing it means publishing a new TailscaleKit artifact. | V |
| F20 | tsnet uploads backend logs to log.tailscale.io unless disabled, and the sidecar never disables it. | V (tsnet v1.104.1 source, review) |

---

## 3. Architecture

### 3.1 On-device stack

```
┌──────────── PICO OS 6 app process (arm64-v8a · one ART + one Go runtime) ─────────────┐
│ Spatial SDK app (Kotlin 2.0, PICO Compose fork)                                         │
│   DefaultWindowContainer · Volume · optional Stage  ── ViewModels collect StateFlows   │
│   (sample-only) SignInPanel: ACTION_VIEW · typeable URL · zxing-core QR                │
│ ─────────────────────────────────────────────────────────────────────────────────────── │
│ truffle-android.aar  com.vibecook.truffle  (Kotlin 2.0.21 · Java 11 · NO Compose)       │
│   Truffle.install(app,cfg) → MeshNode facade (stable across native incarnations)        │
│   LifecycleDriver actor: started-Activity counter · SCREEN_OFF/ON · onBlockedStatus     │
│   NetworkFeeder: default NetworkCallback(LinkProperties) → setNetwork(snapshot)         │
│   EventPump thread "truffle-events": poll() → continuations · StateFlow · SharedFlow   │
│   TruffleNative: 8 natives, RegisterNatives in JNI_OnLoad                               │
│ ═══ JNI: create/call/cancel/destroy/setNetwork return in µs (enqueue) · poll blocks ═══ │
│ libtruffle_android.so  (Rust cdylib)                                                    │
│   crates/truffle-android  jni 0.22.4 shim, every native in with_env(..).resolve()      │
│   crates/truffle-ffi      dispatcher (#[serde(tag="m")] Request ≈30 ops) · handle +     │
│                           op registries (generation-checked ids) · event queue ·        │
│                           error codes · dto (serde+schemars) · tokio RT (2 workers) ·   │
│                           tracing→logcat (redacting) · state_dir lock                   │
│   truffle-core            L7 Node API · FT "ft" · SyncedStore "ss:{id}" · rr:{service}  │
│                           L6 envelope · L5 session · L4 WS :9417 /ws, TCP, QUIC         │
│                           L3 TailscaleProvider → SidecarLauncher::InProcess (NEW)       │
│       control: JSON lines over socketpair (protocol v11) · data: 127.0.0.1 TRFF+token   │
│ libtruffle_tsnet.so  (packages/sidecar-slim, GOOS=android c-shared, cgo, NDK r30, 16KB) │
│   run(cmd,evt,log) · Start/Await · host-fed netmon getter + net:update→InjectEvent     │
│   tsnet:login/logout · redacting UserLogf · logtail off · SetCrashOutput · TLS/E2 guard │
└──────────────────────────────────────────┬──────────────────────────────────────────────┘
          WireGuard/UDP over the puck's Wi-Fi (direct LAN or DERP) → Tailscale SaaS control
   Mac/Win/Linux peers: same truffle-core + same sidecar as a child process (unchanged)
   iOS peers: Swift runtime (messaging interop only; unchanged)
```

**How the pieces load:**
- Rust loads the Go library with `dlopen` on the first InProcess start [I]. Host builds and the `Process` launcher therefore never link Go.
- On the host (macOS/Linux), `truffle-android` builds as a dylib that uses the child-process launcher. This enables a **host-JVM test lane** that runs the Kotlin facade tests against the real core and the desktop sidecar.
- The `Process` launcher stays a fallback on Android too: exec `lib*.so` from `nativeLibraryDir`. A foreground-only app no longer needs an FGS for it [I].

### 3.2 Binding mechanism: hand-written JNI, message-shaped

| Option | Verdict | Deciding reason |
|---|---|---|
| UniFFI 0.32.2 + JNA | Rejected | #2624 is open with "no workarounds", and a reported trace dies in a sync `RustBuffer::destroy` via JNA, so sync-only UniFFI does not clearly escape it. #2953 deadlock is open. JNA would need a ≥5.17.0 pin for 16 KB pages plus R8 keep rules. |
| `uniffi-bindgen-kotlin-jni` | Not yet | Unreleased and not on crates.io. PRs #3037–#3042 are opened, not merged. Mozilla's #7607 is unmerged. |
| BoltFFI 0.31.1 | Rejected | Immaturity: crate first published 2026-02-17, 2 reverse deps, use-after-close #664 and no Cleaner (#55). The stream ring size is configurable, so that was not the reason. |
| C ABI + JNI shim (earlier draft) | Rejected for v1 | The C layer only served Unity. Its design (handles, op-ids, polled queue, schema) is kept. |
| **Hand JNI, `jni = 0.22.4`, about 8 natives over a Rust dispatcher** | **Chosen** | A mature crate with panic→exception policy. libsignal ships the same pattern (AGPL, so we copy the pattern only). No upcalls means no AttachCurrentThread, no FindClass from native threads, no re-entrancy. The dispatcher is host-testable and can carry a C ABI later. |

**Revisit gate** (a trigger to re-evaluate, not a task). Regenerate the Kotlin facade with `uniffi-bindgen-kotlin-jni` only when all of these hold:
- it ships in a tagged UniFFI release;
- application-services #7607 is merged and has gone through a Firefox Android cycle;
- #2953 is closed;
- the Rust-thread class-loader failure (isblu repro, 2026-10-20) is filed upstream and fixed.

We keep the dispatcher either way.

**DTOs.** DTOs are written fresh in `truffle-ffi::dto` with serde and schemars. JSON Schema and fixtures go under `conformance/ffi/`. Tauri's `types.rs:69-622` (DTOs plus their `From` conversions) is the starting point, with these limits:
- Its outbound types are Serialize-only.
- It has no SyncedStore, request/reply or error DTOs.
- It is never compiled in CI (`ci.yml:76,81,141,144`).

Moving Tauri onto these DTOs is a follow-up. `truffle-ffi` exposes `dto` with `default-features = false`, so Tauri never pulls in the runtime.

### 3.3 Threading model

- **Calls go down only.** Any Kotlin thread, Main included, may call `create`, `call`, `cancel`, `destroy` or `setNetwork`. Each validates its input, enqueues work to the Rust runtime and returns in microseconds.
- **Async core calls are ops.** `build`, `build_with_auth_handler`, `stop`, `peers` and `health` are async in core (`node.rs:2143`, `2209`, `769`, `825`, `1131`), so they run as ops. There is no `callSync`. Kotlin serves sync-looking getters from its own StateFlows.
- **Events go up through one blocking poll.** One daemon platform thread, `truffle-events`, loops in `poll(timeoutMs = 500, max = 256)`.
  - Rust wakes it on a condvar the moment an event is queued, so latency is microseconds. The timeout only bounds shutdown checks.
  - The thread decodes a length-prefixed batch: a JSON header plus a raw byte body, with no base64.
  - It completes `CancellableContinuation`s keyed by `opId` and `tryEmit`s into buffered flows.
  - It never runs app code. `serve()` handlers run on a `SupervisorJob + Dispatchers.Default` scope.
- **Tokio and Go threads never touch JNI.**
  - Rust runs one process-wide multi-thread runtime with 2 workers and named threads.
  - Go runs its own runtime, with GOMAXPROCS and GOMEMLIMIT capped through v11 start fields.
  - truffle-core makes no JNI calls. Its only JNI-using dependency, `rustls-platform-verifier` (pulled in by quinn's defaults, `Cargo.lock:3932`; it depends on `jni 0.22.4`, `Cargo.lock:4528`), is linked but unused, because QUIC uses `SkipServerVerification` (`transport/quic.rs:168,196`). We remove it in P0.
- **Handles are generation-checked u64 ids, never pointers.** A concurrent `close()` yields `STALE_HANDLE` rather than undefined behaviour, so no `reachabilityFence` is needed. That fence is API 28+, above our minSdk 26.
- **Cancellation.**
  1. `suspendCancellableCoroutine` registers `opId`.
  2. `invokeOnCancellation` removes the entry and then calls `cancel(h, op)`, which calls `AbortHandle::abort()`.
  3. A late completion is dropped.
  4. A cancelled op resumes with `CancellationException`, never a `MeshException`.
- **Panics.** Release keeps `panic = unwind`; the only profile section is `Cargo.toml:143-144`, for dev. Every native runs inside `EnvUnowned::with_env(..).resolve::<ThrowRuntimeExAndDefault>()`.

---

## 4. Kotlin API sketch and FFI surface

### 4.1 Kotlin API (`com.vibecook.truffle`)

Names follow Swift (`apple/Sources/Truffle/Mesh/MeshTypes.swift:4-12`). The Kotlin `MeshPhase` adds `NotSignedIn` and `Paused`, has no counterpart to Swift's `stopped`, and gives `NeedsLogin`, `NeedsMachineAuth` and `Failed` payloads (Swift's is a payload-free String enum).

```kotlin
// Application.onCreate(), BEFORE launch(::mainApp). Registers callbacks only; no native work.
val mesh: MeshNode = Truffle.install(this, MeshConfiguration(
    appId = "spatial-demo",                       // ^[a-z][a-z0-9-]{1,31}$
    deviceName = prefs.name ?: Truffle.defaultDeviceName(this), // Settings.Global.DEVICE_NAME ?: Build.MODEL
    // stateDirectory defaults to File(noBackupFilesDir, "truffle/<appId>")  (never auto-backed-up)
    loginAllow = LoginAllow.SameAccount,          // | WholeTailnet | Globs(listOf(...))
))

class MeshNode internal constructor(/* process-scoped singleton */) {
    val phase: StateFlow<MeshPhase>
    val auth: StateFlow<AuthState>                // loginUrl, reason, loginName, rememberedLogin, keyExpiry, adminUrl
    val peers: StateFlow<List<Peer>>
    val local: StateFlow<LocalNode?>              // deviceId, dnsName, ip, loginName
    val events: SharedFlow<MeshEvent>             // PeerJoined/Left/Updated/WsConnected/…, Lagged, CrashReport
    val hasSavedLogin: Boolean                    // stateDir/tailscaled.state exists

    fun connect()                                 // first-run consent; afterwards starts automatically while in use
    fun renewLogin()                              // tsnet:login while Running
    suspend fun signOut(forgetIfOffline: Boolean = false)
    suspend fun awaitRunning(timeout: Duration = 5.minutes)

    suspend fun sendBytes(to: PeerRef, namespace: String, data: ByteArray)
    suspend fun <T> sendJson(to: PeerRef, namespace: String, value: T, ser: KSerializer<T>, msgType: String = "message")
    suspend fun <T> broadcastJson(namespace: String, value: T, ser: KSerializer<T>, msgType: String = "message"): BroadcastReport
    fun messages(namespace: String): Flow<MeshMessage>   // cold; native subscribe while collected; rejects ft / ss:* / rr:*

    fun <T> syncedStore(id: String, ser: KSerializer<T>): SyncedStore<T>  // usable before Running; rebinds per incarnation
    val files: FileTransfer                       // policy Reject|Ask|AutoAccept(dir); offers; transfers; send; pull
    suspend fun <Q, R> request(to: PeerRef, service: String, body: Q, qs: KSerializer<Q>, rs: KSerializer<R>,
                               timeout: Duration = 10.seconds): R
    fun <Q, R> serve(service: String, qs: KSerializer<Q>, rs: KSerializer<R>,
                     handler: suspend (from: Peer, Q) -> R): Closeable
    suspend fun ping(peer: PeerRef): PingResult
    suspend fun health(): HealthInfo
}

sealed interface MeshPhase {
    data object NotSignedIn : MeshPhase           // no saved login and connect() not called
    data object Paused : MeshPhase                // app not in use; native node stopped
    data object Starting : MeshPhase
    data class NeedsLogin(val url: String?, val reason: LoginReason) : MeshPhase  // FirstRun|KeyExpired|Renew|SignedOut
    data class NeedsMachineAuth(val adminUrl: String?) : MeshPhase
    data object Running : MeshPhase
    data object Stopping : MeshPhase
    data class Failed(val error: MeshException) : MeshPhase
}

@JvmInline value class PeerRef(val value: String)  // "{incarnation}/{tailscaleId}:{generation}"
class SyncedStore<T> : Closeable {               // LWW slices per device
    val local: StateFlow<T?>; val slices: StateFlow<Map<String, Slice<T>>>
    fun set(value: T)                             // non-suspending, coalesced; facade re-applies after each rebuild
}
sealed class MeshException(code: String, msg: String) : Exception(msg)  // PeerNotFound, PeerAmbiguous(candidates),
    // PeerGone, NotRunning, Stopped, ConnectionFailed, SendFailed, Timeout, Rejected, Remote(code), Io, Unsupported, Internal
```

**Notes:**
- **Peer refs across rebuilds.** `PeerRef` carries the facade's incarnation counter. Core refs are process-local `{tailscale_id}:{generation}` (`node.rs:130-133`), and generations restart with each fresh registry [I]. A ref from an earlier incarnation therefore fails with `PeerGone` instead of silently aliasing.
- **SyncedStore across stops.** In-flight ops fail with `Stopped` when the app leaves use. A store's local value lives in the facade for the process lifetime and is pushed again after each rebuild. Durable persistence is deferred.
- **Message overflow.** `messages()` overflow drops the oldest messages and emits `Lagged{missed}` (1024 per subscription). Swift instead cancels the subscription (RFC 024 §6.4); write the divergence into both RFCs.
- **AuthKey.** `MeshAuth.AuthKey` exists only behind `@RequiresOptIn(DevOnly)`, for CI and testcontrol.
- **AAR build settings:**
  - Kotlin 2.0.21, Java 11 bytecode, compileSdk 35.
  - **minSdk 26**, to merge cleanly with PICO's minSdk-26 samples. APIs above 26 are guarded, e.g. `onBlockedStatusChanged` (API 29). We test only on API 35+.
  - Dependencies: `kotlinx-coroutines-android:1.9.0`, `kotlinx-serialization-json` (1.7.x, the Kotlin 2.0 line [I]).
  - **No Compose and no androidx.lifecycle.** We count Activities ourselves, which avoids `ProcessLifecycleOwner`'s 700 ms delay and its dependency on androidx.startup.
  - Manifest: `INTERNET` and `ACCESS_NETWORK_STATE` only.
  - Consumer R8 rule: `-keep class com.vibecook.truffle.internal.TruffleNative { native <methods>; }`.

### 4.2 FFI surface (`internal object TruffleNative`)

```kotlin
external fun init(abiVersion: Int, logLevel: Int): Int             // once; tokio RT, logcat tracing, panic hook, SIGPIPE→SIG_IGN
external fun create(configJson: ByteArray): Long                   // validates sync; build runs as an op → phase/auth events
external fun call(h: Long, op: Long, req: ByteArray, body: ByteArray?): Int  // enqueue; 0 = accepted, <0 = code
external fun cancel(h: Long, op: Long)
external fun destroy(h: Long)                                      // enqueue graceful stop (§5.1); "stopped" event on completion
external fun poll(timeoutMs: Int, maxEvents: Int): ByteArray?      // process-global; events carry h; null only after shutdown
external fun setNetwork(snapshotJson: ByteArray)                   // process-global interface snapshot (getter + net:update)
external fun shutdown()                                            // tests only
```

**Config (`create`):** `appId`, `deviceName` and `stateDir` are required; the FFI refuses the core fallbacks of F17. Also: `loginAllow`, `loginMode`, `logLevel`, plus test-only `authKey` and `controlUrl`.

**Requests (about 30):**
- `node.*`: `peers`, `peer(query, waitMs)`, `ping`, `whois`, `health`, `login`, `logout`
- `msg.*`: `subscribe(ns)`, `unsubscribe(ns)`, `send(peer, ns, msgType, json)`, `sendBytes(peer, ns)` with the body, `broadcast(ns, msgType, json)`, `broadcastBytes`
- `store.*`: `open(id)`, `set(id, json)` (op = 0, fire-and-forget), `all(id)`, `leave(id)`, `close(id)`
- `ft.*`: `setPolicy`, `send(peer, path, remoteName)`, `pull(peer, remotePath, into)`, `accept(token, into)`, `reject(token, reason)`, `addPullRoot`, `clearPullRoots`
- `rr.*`: `request(peer, service, json, timeoutMs)`, `serve(service)`, `unserve`, `reply(reqId, json | {code, message})`

**Events and queue policy:**

| Kind | Policy |
|---|---|
| `done{op,result}`, `failed{op,code,message,details}`, `phase`, `auth{loginRequired, needsApproval, loginKnown, keyExpiry, loggedOut}`, `peer`, `offer`, `offerExpired`, `request`, terminal `transfer`, `stopped`, `crash` | Lossless (low volume) |
| `peers` snapshot; `store{id, device, version, value}` per (id, device); `transfer` progress per token | Coalesced |
| `message{ns, from, msgType, payload, [body]}` | Bounded, 1024 per subscription; drop-oldest plus `lagged{ns, missed}` |

**Error codes:**
- From `NodeError`'s 14 variants (`node.rs:237-298`): `PEER_NOT_FOUND`, `AMBIGUOUS_PEER`, `PEER_GONE`, `CONNECTION_FAILED`, `SEND_FAILED`, `ENVELOPE`, `SESSION`, `NETWORK`, `TRANSPORT`, `NOT_IMPLEMENTED`, `RESERVED_PORT`, `STOPPED`, `BUILD_ERROR`, `IO`.
- FFI-level: `INVALID_ARGUMENT`, `STALE_HANDLE`, `NOT_RUNNING`, `TIMEOUT`, `UNSUPPORTED`, `INTERNAL_PANIC`. `CANCELLED` is internal and never surfaced.
- NAPI and Tauri still flatten errors to strings (`commands.rs:56,90,146`; napi `node.rs:116,162`) and can adopt the codes later.

**Explicitly out of v1:** raw TCP streams (v1.1, via a socketpair fd as `ParcelFileDescriptor`, about +1 ew), QUIC/UDP to Kotlin, HTTP serve/proxy (TLS listeners fail on Android because the LocalAPI cert route is compiled out), egress (E2 is refused on Android).

---

## 5. Lifecycle (foreground-only) and login UX

### 5.1 "In use" and the LifecycleDriver

**In use** means all of the following:
- at least one started Activity, counted with `ActivityLifecycleCallbacks` and ignoring `isChangingConfigurations`;
- the screen is interactive;
- the default network is not blocked;
- the user has a saved login or has called `connect()`.

The mesh belongs to the **process**. It is never started or stopped from `mainApp`, a container Composable, `DisposableEffect` or a per-window ViewModel. `SpatialContainerStateEvent` is used only for UI. All transitions are serialized in one actor and run off the main thread.

| Trigger | Action | Why |
|---|---|---|
| Becomes in use | `create()` from the saved config. Rust waits for the state_dir lock if a previous stop is still draining. | Fresh node, same keys and ULID (F11). |
| Started-Activity count reaches 0 (minimize or close of the last container) | **Stop immediately.** `stopDelay` defaults to 0 ms; G0 may raise it to ≤1 s if Stage or window swaps flap. | The firewall bites 2 s after the app is cached (F9). `ProcessLifecycleOwner`'s 700 ms delay would eat a third of that. |
| `ACTION_SCREEN_OFF` (doff, or the screen-off simulation) | Stop after `screenOffGrace` (10 s, below the 20 s TOP_SLEEPING delay). **Exception:** while a file transfer is active and the network is unblocked, stop when the transfer ends or the network is blocked. | Lets the goodbye reach peers; hedges R4002 (F10). |
| `onBlockedStatusChanged(true)` | Stop immediately with short timeouts. The goodbye is probably lost. | Peers then rely on C5. |
| Phase is `NeedsLogin` or `NeedsMachineAuth` | **Ignore every stop trigger** for up to `loginGrace` (10 min); after that, stop. | The pending node key exists only in memory (F15). |
| Default network changes while Running | `setNetwork` → `net:update` → `InjectEvent`, with no restart. If G2 shows no recovery within 10 s, restart instead. | Android netmon otherwise polls every 10 min. |
| Process killed (LMK, or the Stage's "close app") | Nothing; the next launch cold-starts. A crash report is surfaced from `go-crash.txt` or `ApplicationExitInfo` `REASON_CRASH_NATIVE` (API 30+). | |

**Stop sequence.** One Rust op, budget 3 s, hard cap 5 s [I]:
1. `store.leave()` for each store. This broadcasts `Clear` (C4).
2. `Node::stop()`. This closes every WS (`node.rs:787-794`), then runs `tsnet:stop`, which does `EditPrefs(WantRunning=false)` with a 3 s cap and then `srv.Close` (`main.go:1417-1437`). Today the Process launcher force-kills the sidecar 500 ms after sending `tsnet:stop` (`sidecar.rs:1008-1026`), which can cut that goodbye short; R1 replaces the kill with an await.
3. Await the return of Go's `run()`.
4. Release the state_dir lock.
5. Emit `stopped{h}`.

**What peers see:**
- **Clean stop:** a WS Close within about 1 RTT; `Updated(online=false)` from control "within seconds" (code comment only, not measured); the slice is cleared. The headset stays in the registry as offline, and sends fail fast with PeerOffline (`session/mod.rs:2103-2105`).
- **Unclean exit:** detected within 40 s once C5 lands (ping 10 s, pong 30 s, `transport/mod.rs:341-342`). Today it can take up to the 10-minute bridge idle reap (`main.go:62-65`).

### 5.2 Login UX flow (personal tailnet)

1. **First launch** (no `tailscaled.state`; phase `NotSignedIn`).
   - Screen: "Name this headset", prefilled from `Settings.Global.DEVICE_NAME` or else `Build.MODEL`. Warn that "PICO Space Pro" names will collide; the ULID remains the identity.
   - One button, **[Connect with Tailscale]**, which calls `connect()`. The node never generates a login URL the user did not ask for.
2. **`NeedsLogin(url)`**, with `start_timeout(None)` (`node.rs:1950-1962`). The sample's SignInPanel shows three paths:
   - **Primary:** [Open sign-in page] via `Intent(ACTION_VIEW, url)` with `FLAG_ACTIVITY_NEW_TASK`. Catch `ActivityNotFoundException` and hide the button. No WebView, no Custom Tabs dependency.
   - **Secondary:** "On your phone or computer go to `login.tailscale.com/a/7c1e 9f2b 04d3 a5`", in large monospace, grouped in fours.
   - **Tertiary:** a QR code labelled "Emulator or casting", drawn with zxing-core as a BitMatrix on a Canvas. No Compose QR libraries, because they pull in androidx Compose UI.
   - **Footer:** "This headset will appear as `truffle-{app}-{slug}`".
   - **Rotation:** the panel always shows the latest URL; the sidecar re-sends it when it rotates (`main.go:1225-1228`).
3. **While the user signs in**, our containers should only lose focus, because the browser opens as its own Shared Space window [I]. If they stop instead, `loginGrace` keeps the node alive.
4. **`NeedsMachineAuth`** (rare on personal tailnets): [Open admin console] from `ControlServer::machines_admin_url`.
5. **Running:** "Connected as alice@…". The facade persists `auth.json` `{loginName, controlUrl, keyExpiry}` in stateDir.
6. **Later launches:** the node starts silently from saved state. If `LoginRequired(KeyExpired)` arrives, show "Your sign-in expired — sign in again as alice@…".
7. **Expiry warnings:**
   - **≤14 days left:** a dismissible banner: "Sign-in expires in N days · [Renew]". Renew calls `renewLogin()`, which runs `tsnet:login` and keeps the mesh up [V¹]. Link to "turn off key expiry for this headset" in the admin console.
   - **≤24 h left:** the banner becomes persistent.
   - **Expiry mid-session:** the IPN bus reports NeedsLogin, core emits `LoginRequired`, the FFI auto-calls `login()`, and the panel reappears.
8. **Sign out:**
   - Confirm, then `signOut()`: `tsnet:logout` (`lc.Logout`, 15 s cap), stop, delete `tailscaled.state`, **keep `device-id.txt`**, then return to `NotSignedIn`.
   - **Offline:** "Remove sign-in anyway? The device stays in your admin console until it expires or you remove it."
   - **Switching accounts** is sign out, then Connect.
9. **Invariants:**
   - Never ephemeral (default `ephemeral: false`, `node.rs:1807`).
   - Never log the URL: `adb logcat | grep '/a/'` must be empty.
   - stateDir lives under `noBackupFilesDir`, so the node key is never backed up. The sample manifest sets `allowBackup=false`.
10. **To try in G-auth:** Tailscale's Android-TV "Input code" path (admin console → Add device → Android → Input code; tailscale.com/docs/install/android, 2026-10-24). If it accepts tsnet AuthURL codes, it beats typing a URL.

---

## 6. Core, sidecar and CLI changes

### Sidecar (`packages/sidecar-slim`, protocol v10 → v11, `main.go:145`)

| ID | Change | Where |
|---|---|---|
| S1 | `main()` becomes `run(cmd io.Reader, evt io.Writer, logw io.Writer) int` with a 3-line `main()`. Add `//export TruffleSidecarStart(cmdFd, evtFd, logFd) → runId` and `TruffleSidecarAwait(runId, timeoutMs)`. | `main.go:987-1081` |
| S2 | Host-fed `netmon.RegisterInterfaceGetter` from a v11 start field `interfaces`. New `net:update` command that replaces the snapshot and calls `InjectEvent`. | new case in the dispatch, `main.go:1027-1076` |
| S3 | Empty-environment v11 start fields: `controlUrl` (test-only; sets `srv.ControlURL`, which tsnet and `effectiveControlURL` already prefer over the inherited `TS_CONTROL_URL` read by tsnet and mirrored at `main.go:946`), `debug` (`TRUFFLE_DEBUG` is a package-init var, `main.go:976`), `tmpDir`, `gomaxprocs`, `gomemlimit`, `uploadLogs` (default false, which disables logtail). | `startData` (plain `json.Unmarshal`, so the new fields are backward-compatible) |
| S4 | `GOOS=android` guards: skip `ListenTLS` and the `CertPair` prewarm; refuse the E2 egress listener (unauthenticated `127.0.0.1:0`; on Android, loopback is shared across apps). | `main.go:1589`, `2336`, `3675`; `egress.go:498` |
| S5 | Auth:<br>• `tsnet:login{requestId}`: `StartLoginInteractive` plus an await loop that re-emits `tsnet:authRequired`, without starting a second monitor.<br>• `tsnet:logout{requestId}`: `lc.Logout` with a 15 s cap, then `tsnet:logoutResult`.<br>• Add `NotifyInitialState` to the watcher (today it uses `NotifyInitialStatus|NotifyPeerPatches|NotifyNoNetMap`) and forward `State` and `BrowseToURL` after start. | `main.go:1924-1925`; `pollUntilRunning` at `1200-1272` |
| S6 | Redacting `srv.UserLogf`/`Logf` that rewrites `…/a/<code>` to `[login URL redacted]`. | `main.go:1149-1156` |
| S7 | `debug.SetCrashOutput(stateDir/go-crash.txt)` and `SetTraceback`. In-process stop awaits `srv.Close` rather than relying on a kill. | `main.go:963-969` (`recoverPanic` covers only the sidecar's own goroutines) |
| S8 | Build: android arm64 and amd64 c-shared, cgo, NDK r30, `-Wl,-z,max-page-size=16384`, CI `llvm-readelf` alignment check. No Android-specific Tailscale bump: build against the sidecar's pin (v1.104.1 today) and pick up the planned combined 1.106 upgrade (`docs/tailscale-upgrade.md`), whose netstack fixes and reporting of node removal as NeedsLogin [I] both help here. | `packages/sidecar-slim/go.mod:7` |

### Rust core (`crates/truffle-core`)

| ID | Change | Where |
|---|---|---|
| R1 | `SidecarLauncher{Process, InProcess}`. The in-process launcher uses a socketpair for control and dlopens the Go library. `shutdown` awaits `TruffleSidecarAwait` instead of sleeping 500 ms and killing. Add a state_dir lock so rebuilds are serialized. `TailscaleConfig` gains a launcher field, a 0.8.0 breaking change (its struct literals exist only in core). | `network/tailscale/sidecar.rs:379-387`, `1008-1026`; `provider.rs:37-79` |
| R2 | Android guards: explicit BuildErrors replace the fallbacks (the FFI always passes both values). `truffle-sidecar/build.rs:25-38` becomes a no-op on Android instead of panicking. | `node.rs:2042`, `2072` |
| C1 | Clock-seeded store version, `max(persisted, unix_ms*1000)`. Stays below 2^53, which NAPI's f64 needs (`truffle-napi/src/synced_store.rs:67`). Wire-compatible. Rewrite about a dozen absolute-version test asserts. | `synced_store/mod.rs:121-157`; fixes `sync.rs:300-311` |
| C2 | Initial pull: when the sync task starts, send `Request` (and `Full` if a local slice exists) to every online peer. | `synced_store/sync.rs:35-44` |
| C3 | Treat `PeerEvent::WsConnected` like `Joined` (resync). | `sync.rs:66-78` |
| C4 | New `SyncedStore::leave()` broadcasts `SyncMessage::Clear`, which is defined but never sent today (the only lifecycle method, `stop()`, just aborts the sync task). | `synced_store/mod.rs:296-308`; `types.rs:60` |
| C5 | Heartbeat timeout wakes `recv()`, so the session retires the connection and emits `WsDisconnected`. | `transport/websocket.rs:904-1006` |
| C6 | RAII guard deletes `*.truffle-tmp` on error or cancel. | `file_transfer/receiver.rs:520-576`; `file_transfer/mod.rs:553-600` |
| C7 | Seed the session from `network.peers()` after subscribing, closing the startup race. | `provider.rs:1066-1071` vs `node.rs:2186` |
| R3 | Auth:<br>• `NetworkStatusEvent::LoginRequired{url: Option<String>, reason}` (the enum is `#[non_exhaustive]`, `network/mod.rs:397-398`).<br>• Map a post-start NeedsLogin to it (today it only writes health, `provider.rs:689-702`).<br>• `Node::login()` / `logout()` via `register_reply`, gated on protocol ≥11.<br>• `LoginAllow::SameAccount`: fails closed until `LoginKnown`, then gates on the node's own login, in both the provider filter (static Vec, `provider.rs:540-546`) and `WsConfig`.<br>• Redact `provider.rs:661`. | `network/login_allow.rs:18-21` |
| R4 | `Node::request()` / `serve_requests()` on namespace `rr:{service}`. msg_types `rr.req`, `rr.res`, `rr.err`; payloads `{id: ULID, body}` or `{id, code, message}`. A reply is accepted only from the target's WhoIs-verified `tailscale_id`. Needs an **RFC 016 amendment** (§"Why a function, not a method on Node?" and §"Why a filter function, not a correlation ID?", lines 103-112). | `request_reply.rs:56-64` (filter-closure only today) |
| R5 | Make `broadcast_reported` public (it already takes a msg_type). | `node.rs:1345` (private); `broadcast_typed` at `1259` returns `()` |
| R6 | Builder `control_url()`, test-only. | (`TS_CONTROL_URL` is unavailable, F8) |
| R7 | Hygiene: drop quinn's `platform-verifier` default and keep the others; `SIGPIPE → SIG_IGN` at FFI init. Bench: cfg-gate `machine_uid::get()` for `not(target_os = "android")`. | `Cargo.toml:73`, `Cargo.lock:3932`; `benchmark/engine/src/env.rs:119-123` |

### CLI (`crates/truffle-cli`): must ship with S6

Today the CLI shows the login URL only through log output (the sidecar's stderr and the core's `provider.rs:661` info log), and S6 and R3 redact both. So:
- `DaemonServer::start` uses `build_with_auth_handler` and prints the URL itself (`daemon/server.rs:77-92`).
- Connect onboarding's dead `_auth_tx` (`tui/onboarding.rs:208`).
- Redact `tui/mod.rs:723`.

### New code

- `crates/truffle-ffi`: rlib, in CI.
- `crates/truffle-android`: cdylib, 300–500 LOC.
- `android/`: Gradle; `truffle` AAR plus the `harness` instrumentation app.
- `android/examples/spatial-sample`.
- `examples/spatial-companion`: Rust bin, about 250 LOC.
- `conformance/`: move the 4 Swift fixtures here; a missing fixture fails under `CI=true` (`tests/interop_fixtures.rs:18-37` skips silently today).

**Not in this RFC:**
- Swift adopting the Rust cross-dial tie-break (`MeshNode.swift:790-796` vs `session/mod.rs:1618-1650`). This is a pre-existing iOS↔desktop bug; file it separately.
- Swift logout and expiry parity (about 0.5 ew).

---

## 7. Phased roadmap

**Where each gate is tested:**
- **E** = stock Android 15/16 emulator, x86_64, CI (GitHub-hosted Linux with KVM [I]).
- **Ph** = Android 16 phone.
- **PE** = PICO Emulator 6.1 on an Apple Silicon Mac (manual or self-hosted nightly; not possible on hosted runners [I]).
- **SP** = Space Pro.
- **H** = host: cargo, Go and host-JVM tests.

| Phase | Scope | Effort (ew) | Exit gate · where |
|---|---|---|---|
| **P0 Probe & prerequisites** (wk 1–2) | • R7 hygiene; R2 `build.rs`.<br>• **G0 recon** on PE: `getprop` for `ro.product.cpu.abilist`, `ro.build.version.sdk` and `ro.boot.qemu` (one name per call), `getconf PAGE_SIZE`, `getenforce`, `cmd package resolve-activity -a android.intent.action.VIEW -d https://login.tailscale.com`, `pm list packages \| grep -iE 'webview\|browser'`.<br>• **Exec probe APK:** a plain app with `useLegacyPackaging` that runs today's `truffle-bench serve` plus sidecar variants V1 (`GOOS=linux CGO_ENABLED=0 -tags truffle_android`) and V2 (`GOOS=android CGO_ENABLED=0 -buildmode=pie -tags truffle_android`, which keeps androidbin active).<br>• **Lifecycle logger:** a planar-window app scaffolded with PICO CLI (exact `pico-cli` template syntax to confirm) logging Activity callbacks, `SpatialContainerStateEvent`, SCREEN_OFF/ON, NetworkCallback blocked state and `dumpsys` procState/connectivity.<br>• Matrix: minimize · close window · Stage "close app" · Home · own Stage · another app's Full Space · ACTION_VIEW to the browser · screen-off simulation <20 s / >20 s / >5 min · Wi-Fi toggle.<br>• Settle this RFC's open product defaults (§8). | 1.75 (1.25–2.25) | **G0+G1** · PE, Ph, E: node reaches Running under the app UID; the Mac's `truffle-bench peers` lists it; direct vs DERP explained; matrix rows recorded, including firewall timing and socket destroy. If tsnet fails even with the getter, escalate: it blocks everything. |
| **P1 In-process L3 + JNI spike** (wk 3–6, track A) | • S1–S4, S6–S8; R1, R2, R6.<br>• Spike: `truffle-android` with `init`, `create`, `call`, `poll`, `setNetwork`, `destroy`, plus a minimal Kotlin NetworkFeeder in `harness`. This becomes production code.<br>• CI: arm64 + x86_64 builds, 16 KB check, E smoke against hermetic testcontrol (0.0.0.0, DERP host 10.0.2.2). | 4 (3.25–5) | **G2** · E, Ph, PE:<br>• in-process node passes the G1 checklist;<br>• 1 h soak;<br>• 20 stop→start cycles, each stop under 3 s, with no state_dir contention;<br>• fault injection: recovered Go panic; forced fatal surfaced from `go-crash.txt`; native tombstone via `ApplicationExitInfo`;<br>• desktop `go list` file set unchanged, Go/Rust suites and `live_pair` green.<br>**Decision:** confirm in-process as default and exec as fallback. |
| **P2 Core for foreground-only + auth** (wk 3–7, track B) | • C1–C7 (1.25)<br>• R4 plus the RFC 016 amendment (0.75)<br>• R5 (0.1)<br>• auth: S5, R3, CLI (1.75)<br>• `conformance/` (0.25)<br>• review and integration buffer (0.15) | 4.25 (3.5–5.25) | **G3-core** · H, E:<br>• 20 stop/rebuild cycles against a Mac: `ws_connected=false` within 1 s; `online=false` within X s (X set from P0 data); no rejected slices; a late store pulls; no leftover `*.truffle-tmp`; `kill -9` detected within 40 s.<br>• Auth on testcontrol (`RequireAuth`, `CompleteAuth`, `SetExpireAllNodes`): first login, URL rotation, expiry → auto re-login, renew while Running, logout online and offline.<br>• `grep '/a/'` over all logs is empty. |
| **P3 Kotlin binding** (wk 6–11) | `truffle-ffi`: dispatcher, queue, errors, DTOs, schema (2) · JNI (0.5) · Kotlin facade, LifecycleDriver, NetworkFeeder, auth state (2.25) · tests and packaging: host-JVM lane, E instrumented tests, R8, 16 KB, AAR (1) | 5.75 (4.5–7.25) | **G3-Kotlin** · H, E, PE:<br>• host-JVM and E suites green;<br>• StrictMode clean on Main;<br>• the harness on PE survives 20 in-use / not-in-use cycles with converged stores and zero lost lossless events;<br>• R8 release build works;<br>• permissions are INTERNET + ACCESS_NETWORK_STATE only. |
| **P4 Spatial sample + companion** (wk 10–12) | • DefaultWindowContainer: SignInPanel, peers, chat, offers, sign out.<br>• Volume with 3 `ModelEntity` objects, poses in an LWW store (`SpatialDragValue` → `set`, throttled to 30 Hz).<br>• Optional Stage, to exercise the Full Space transition.<br>• `.glb` received through `truffle cp` from the Mac.<br>• `examples/spatial-companion` serves `rr:scene-snapshot`. | 1.25 (1–1.75) | **G-auth + G-sample** · PE + Mac, Ph:<br>• login via the headset browser, via a URL typed on a phone, and via QR from the emulator window;<br>• backgrounding during login;<br>• kill after the URL was completed;<br>• TV "Input code" tried;<br>• 100 MB file with progress;<br>• 20 minimize/reopen cycles converge;<br>• move round trip measured (under 200 ms on LAN is the target on Ph or SP; PE goes through SLIRP). |
| **P5 Release & hardening** (wk 11–14) | • `release-android.yml` dispatched from `release-please.yml` (GITHUB_TOKEN-created releases fire no events).<br>• Version checks covering Gradle; Maven Central `com.vibecook`.<br>• THIRD_PARTY_NOTICES (go-licenses + cargo-about); unstripped symbols published.<br>• Nightly E live pair with no auth key; PE nightly on a self-hosted Mac or manual.<br>• Power and frame-time measurement on Ph as a proxy.<br>• Dev guide and parity table; PICO store test submission. | 2.75 (2.25–3.5) | A tag reproducibly yields the AAR with pinned checksums; nightly green; store feedback received on permissions and native libs. |
| **P6 Space Pro qualification** (when hardware exists) | Doff and puck sleep; real Wi-Fi direct paths and throughput; power, thermal and frame time during a 100 MB transfer; OS 6 firewall and freezer flags. | 0.75 (0.5–1) + calendar | **G4** · SP: the G2, G3-Kotlin and G-sample matrices pass. |

**Totals.** About **20.5 ew (16–26)**.
- Track A (L3, JNI, lifecycle) and track B (core, auth, facade) run in parallel after week 2. With two engineers that is about 11–14 weeks to GA on PE + Ph (P5 ends in week 14); P6 is gated on the calendar.
- **Cut line (Kotlin MVP):** drop request/reply (R4 and its binding part), pull roots and the Volume sample, for about 18.5 ew.
- **Not included:** raw TCP streams to Kotlin (+1), a QUIC-datagram pose channel (+1.5–2), migrating Tauri onto the DTOs (+0.5), Swift parity (+0.5), iOS on truffle-core (RFC 029, 8–12).

**What each test target can and cannot show:**

| Capability | E | Ph | PE | SP |
|---|---|---|---|---|
| tsnet under the app sandbox (netlink, getter) | ✓ | ✓ | ✓ (OS 6 policy, emulator build) | final |
| Spatial containers, Stage/Full Space lifecycle | — (Spatial app on a stock emulator: try once, F5) | — | ✓ | ✓ |
| Android 15+ firewall, freezer, socket destroy | partial | ✓ AOSP reference | only if PICO enables it | ✓ |
| Doff / puck sleep | — | — | screen-off simulation only | ✓ |
| Browser login (`ACTION_VIEW`) | ✓ (Google APIs image) | ✓ | ✓ (PICO browser) | ✓ |
| Direct LAN path, throughput | ✗ (SLIRP) | ✓ proxy | measure (likely DERP) | ✓ |
| Power, thermal, frame time | — | proxy | — | ✓ |
| JNI/AAR instrumented, R8, 16 KB | ✓ | ✓ | ✓ | — |

---

## 8. Risks and what remains unverified

| # | Risk | L / I | Mitigation |
|---|---|---|---|
| 1 | **tsnet fails or degrades in the OS 6 sandbox** (netlink, SELinux, puck Wi-Fi), or reaches DERP only. | M / H | Host-fed getter (the tailscale-android pattern) plus the exec probe with androidbin as a second interface source. G1 in weeks 1–2 on PE, Ph and E. G4 remains for hardware. |
| 2 | **OS 6 lifecycle differs from AOSP:** doff never stops the Activity, TOP_SLEEPING firewall at 20 s, socket destroy, "close app" kills the process. Effects: lost goodbyes, stale presence, lost logins. | H / M | Counter-based immediate stop; 10 s screen-off grace; 3 s stop budget; C1–C5 so peers converge even after unclean exits; NeedsLogin exemption; G0 matrix on PE's screen-off simulation and on Ph; thresholds are settings, tuned from data. |
| 3 | **Login inside the headset is painful or broken:** OS 6 ACTION_VIEW resolution and SSO/passkeys in the PICO browser are unknown, and the in-headset QR can't be scanned. | M / H | Browser + typeable URL + QR for emulator/casting; Android-TV input-code path; G-auth on PE; ask PICO devrel for a "send URL to phone" API (Meta has `send_auth_url`; no PICO equivalent found, 2026-10-24). |
| 4 | **In-process Go + ART:** no crash isolation; Go fatal traces go to fd 2 (discarded); AT_SECURE forces GOTRACEBACK=none; signal ordering with crash reporters. | M / M | `SetCrashOutput` + `ApplicationExitInfo` tombstones surfaced on next start; published symbols; G2 fault injection; exec launcher kept as fallback (no FGS needed under foreground-only [I]); detect a second Go runtime at load (golang/go#65050). |
| 5 | **No hardware; emulator fidelity unknown:** Mac guest ABI arm64 [I]; SLIRP NAT; emulator manifest last pushed 2025-11; no doff. | H / M | Labelled proxies (PE, Ph, E); Early Access on day 1; G4 is separate and does not block GA; x86_64 build kept for CI and the Windows PE. |
| 6 | **Store and toolchain constraints:** OS 6 store rules unpublished; R4002 (downloads continue after doff) conflicts with foreground-only; logtail privacy; PICO's Compose fork and AS 2025.1.x / AGP 8.8 / Kotlin 2.0 pins. | M / M | Transfer-in-flight exemption at screen-off; logtail off (S3); INTERNET + ACCESS_NETWORK_STATE only; early test submission; devrel questions on day 1; an AAR with no Compose and minimal dependencies matching the SDK's POM pins. |

**Also tracked:**
- **Effort:** all figures are inference; re-estimate at G1 and G2.
- **jni-rs upstream is slow:** no release since 0.22.4; fixes #811 and #817 sit on main. Pin 0.22.4 and watch.
- **Store retention after rebuild:** `leave()`/Clear for durable stores may surprise desktop apps that keep an offline device's last slice. Decide default vs opt-in per store.
- **Wall clock:** a backwards wall-clock step across restarts defeats C1. C4's Clear covers clean stops.

**Unverified (needs PE, a device or a measurement):**
- OS 6 runtime API level; PE guest ABI on Mac.
- Whether the PICO Spatial SDK app actually runs degraded on a stock emulator.
- Whether doff or screen-off yields ON_STOP or only TOP_SLEEPING; PICO's firewall and freezer server-flag values; whether socket destroy happens on OS 6.
- Whether the browser opens as its own Shared Space window while our process stays STARTED; which package handles `ACTION_VIEW https`, whether it supports Custom Tabs, and whether a WebView is present.
- Whether another app's Full Space stops our containers.
- Restart-to-mesh-ready latency (estimated 2–5 s). If it exceeds about 3–5 s, add a stop debounce or a "soft leave".
- How fast control marks a blackholed node offline.
- Whether SLIRP permits a direct UDP path.
- What happens if the node is killed after the URL was completed: a new URL only, or a dangling machine.
- Whether Tailscale TV input codes accept tsnet AuthURLs.
- Whether a Kotlin 2.0.0 app reads metadata from an AAR built with 2.0.21 (expected yes [I]).
- `Settings.Global.DEVICE_NAME` on OS 6.
- Whether `ModelEntity` `FROM_STORAGE` with GLTF accepts app-private paths.
- Whether OS 6 keeps R4001/R4002.

**Product choices with proposed defaults** (the owner should confirm):
- `LoginAllow.SameAccount` by default, with an opt-out, since the Personal plan allows 6 users.
- Sign-out keeps the device ULID and wipes only account-scoped data.
- `AuthKey` stays dev-only.
- Raw streams are deferred to v1.1.
- Maven coordinates `com.vibecook:truffle-android`.

---

## 9. First two weeks

Engineer A owns the probe, L3 and JNI. Engineer B owns prerequisites, core and auth.

1. **Day 1, both:**
   - Confirm the product defaults listed at the end of §8.
   - Apply for PICO Global Early Access (company account).
   - Set up an Apple Silicon Mac (M1 Pro+, 16 GB, macOS 14) with PICO Emulator 6.1 and Android Studio 2025.1.x; buy an Android 16 phone; start Maven Central `com.vibecook` verification.
   - Email PICO devrel:
     - OS 6 doff, screen-off and firewall behaviour, and whether R4001/R4002 apply;
     - the ACTION_VIEW browser, Custom Tabs, and whether it opens as a separate window;
     - any "send URL to companion phone" API, or casting;
     - store rules on bundled native `.so` files and on exec;
     - Space Pro dev-kit timing.
2. **Day 1, A:** G0 recon commands on PE (§7 P0). Decide whether x86_64 is needed beyond CI and the Windows PE.
3. **Day 1–2, B:**
   - `chore(bench)`: cfg-gate `machine_uid` (`benchmark/engine/src/env.rs:119-123`).
   - `truffle-sidecar/build.rs:25-38` no-op on Android.
   - Drop quinn's `platform-verifier` (`Cargo.toml:73`).
   - `cargo ndk -t arm64-v8a -t x86_64 -P 26 build --release -p truffle-bench` (NDK r30, cargo-ndk 4.1.2).
4. **Day 2, A:**
   - Add `packages/sidecar-slim/android_features.go` (`//go:build truffle_android`, blank import of `feature/androidbin`).
   - Build V1 and V2; confirm the desktop `go list` file set is unchanged; `llvm-readelf` LOAD alignment.
5. **Day 3–4, A:**
   - Probe APK in `spikes/android-probe/` (never shipped; targetSdk 35, minSdk 26, `useLegacyPackaging`). The auth key goes in through a `run-as` file, never argv.
   - Lifecycle-logger app scaffolded with PICO CLI (planar template; exact syntax to confirm), with a Volume and a Stage.
6. **Day 4–6, A, G1:**
   - From the Mac: `cargo run --locked -p truffle-bench --release -- peers`, then `run --peer <pico>` with the `path`, `tcp`, `ws` and `file-transfer` workloads, on PE, Ph and E.
   - Run the lifecycle matrix, recording procState, `dumpsys connectivity` blocked reasons, socket destroys and firewall timing.
7. **Day 3–5, B:**
   - Create `conformance/` (move the 4 fixtures; fail under `CI=true`).
   - Draft the RFC 016 amendment for `rr:{service}`.
   - File the Swift tie-break issue separately.
8. **Day 5–8, B:** SyncedStore PR with C1–C4, plus tests: restart mid-sync, late-created store pulls, `Clear` on leave, `WsConnected` resync.
9. **Day 6–9, A:** refactor S1, `main.go:987-1081` → `run(cmd, evt, logw)`. Drive `run()` over `io.Pipe` in Go tests; full Go suite green.
10. **Day 8–10, B:**
    - C5 heartbeat fix with a regression test (idle link + pong timeout → `WsDisconnected`).
    - C7 startup race; C6 tmp guard.
11. **Day 10, both, G0/G1 review:**
    - Lock the LifecycleDriver thresholds (`stopDelay`, `screenOffGrace`, `loginGrace`) from the measurements.
    - Confirm in-process as default and exec as fallback.
    - Confirm the P1–P3 scope and re-estimate against the G1 data.