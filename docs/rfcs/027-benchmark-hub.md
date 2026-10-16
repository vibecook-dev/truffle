# RFC 027: Benchmark hub — one engine, one app, every number truffle can produce

**Status**: Implemented on `feat/benchmark-hub` (not yet merged); r3 records the amendments found while building and reviewing, r4 those that followed the core fixes, see §14
**Author**: James Yong + Claude
**Date**: 2026-10-16
**Depends on**: RFC 012 (layers), RFC 014 (file transfer), RFC 016 (SyncedStore, request/reply), RFC 019 (local testing and benchmarking; this supersedes its §10.2), RFC 021 (raw TCP/UDP/QUIC), RFC 023 (HTTP serving), RFC 025 (login-scoped nodes), RFC 026 (egress)
**Review**: r1 was checked by four adversarial reviews (core API, measurement methodology, app/UI/identity, housekeeping). r2 folds in every accepted finding. Rationale is kept short; the rules are what implementers follow.

---

## 1. Problem

Benchmarking truffle today is scattered and partly broken.

| Artifact | State |
|---|---|
| `crates/truffle-bench` | A binary only, with four workloads. Only `transport` runs across two hosts. Every node is ephemeral and joins with an auth key. The workloads sit in a private module of the binary, so nothing else can link them. |
| `crates/truffle-core/benches/*` | Two criterion benches: `envelope`, plus `chunk_hash`. `chunk_hash` measures the `sha2` crate, which is the hashing ceiling for file transfer, not truffle code. |
| `packages/sidecar-slim/cmd/tsnet-bench` | A Go TCP baseline with no Rust bridge. Same wire format as `transport`. Nothing runs it. |
| `scripts/run-all-tests-and-benches.sh` | Its summaries never match because it greps for `"key":` without the space serde emits. Its criterion path ignores a redirected target dir, and its labels are stale. |
| `justfile`, `package.json`, `run-all…sh`, `benchmarks.yml` | Four different "default" benchmark sets. Only CI runs `transport`. |
| Docs | `TESTING.md` says "all three" criterion benches; there are two. `benchmarks.yml` says "RFC 023 will add a self-hosted runner", but RFC 023 is HTTP serving. RFC 019 lists files and scenarios that were never built. |

Some things are measured nowhere:
- UDP, WebSocket message throughput, HTTP serving, egress, or video.
- Which path (direct or DERP) actually carried the bytes.
- The CPU cost of each workload.
- The link between two of a person's own machines, signed in with their own Tailscale account.

## 2. Goals

1. **One engine.** `truffle-bench` becomes a Rust library plus a binary. It owns every two-node workload, the peer control protocol, the report format and the statistics. The CLI and the desktop app both drive it, and their numbers are comparable when they come from the same build profile.
2. **One app.** "truffle bench" is a Tauri v2 desktop app.
   - It signs in to the user's Tailscale account and finds every other copy of itself on the tailnet under the same account.
   - It runs any workload against any of those copies, with live charts.
   - It keeps a run history and imports reports from the CLI and CI.
3. **Every meaningful metric** for what truffle supports. Planned capabilities appear in the app as planned.
4. **Housekeeping.** All benchmark material lives under `benchmark/` or is linked from it. Each piece is defined once, builds, runs, and is documented truthfully.
5. **The app wears James's identity** (`career/brand`).

## 3. Non-goals

- **A JS binding (NAPI) lane.** It is planned (§8.6).
- **A real camera.** The video workload is synthetic and exact; a camera is planned.
- **Replacing criterion.** It stays in `truffle-core`.
- **Moving `tsnet-bench`.** It shares the sidecar's `go.mod`, `go test` and govulncheck.
- **Publishing.** Both new crates are `publish = false`, and the app package is `private`.
- **Signed or notarized app builds.** Ad-hoc signing only (§11.6).

## 4. Layout and repo plumbing

```
benchmark/
  README.md                 what lives here, how to run CLI and app, methodology, baselines
  schema/
    report.v3.schema.json   GENERATED from the Rust types (schemars); test fails on drift
    fixtures/*.json         GENERATED canonical example of every §7 type/variant
  engine/                   crate `truffle-bench` (git mv of crates/truffle-bench; name kept)
    Cargo.toml              version 0.1.0, publish = false, [lib] truffle_bench + [[bin]] truffle-bench
    build.rs                embeds `rustc -V` (TRUFFLE_BENCH_RUSTC)
    src/
      lib.rs  error.rs  config.rs  hub.rs  control.rs  clock.rs  dataplane.rs  stats.rs
      env.rs  resources.rs  localapi.rs  pathwatch.rs  report.rs  suites.rs  params.rs
      workloads/{mod,path,tcp,udp,quic,ws,file_transfer,synced_store,http,video,egress}.rs
      main.rs  cli.rs                       binary only
      legacy/{mod,transport,harness,report_v2,error}.rs   binary only (old `transport`)
    tests/live_pair.rs      real-network PR regression (§12)
    tests/fixtures/{v1,v2}/ normaliser inputs (copies of real old reports)
  app/                      package `@vibecook/truffle-bench-app` (private, version 0.0.0)
    package.json  index.html  vite.config.ts  tsconfig.json  vitest.config.ts
    scripts/prepare-sidecar.mjs  scripts/check-dist.mjs
    src/
      brand/                vendored from career/brand, SAME folder shape (§11.5)
      api/                  types.ts, tauri.ts (real), mock.ts (fixtures only), index.ts
      components/ charts/ views/ hooks/ styles/
    src-tauri/              crate `truffle-bench-app` (version 0.1.0, publish = false)
      Cargo.toml  build.rs (only tauri_build::build())  Info.plist
      tauri.conf.json         NO bundle.externalBin
      tauri.bundle.conf.json  {"bundle":{"externalBin":["binaries/sidecar-slim"]}}  (app:build only)
      capabilities/default.json  icons/ (committed)  binaries/.gitkeep (contents gitignored)
      src/main.rs lib.rs commands.rs state.rs window.rs macos.rs
```

**Cargo workspace.**
- Both crates are listed explicitly in `members`, never by glob.
- The root gets `default-members = ["crates/truffle-core", "crates/truffle-cli", "crates/truffle-sidecar", "crates/truffle", "benchmark/engine"]`, so a plain `cargo build` skips GUI crates.
- `truffle-bench` is a normal member. It must pass on ubuntu, macOS and Windows:
  - `cargo clippy --all-targets -D warnings` (no `unsafe`)
  - `RUSTDOCFLAGS=-D warnings cargo doc`
  - doctests (examples that start a hub are `no_run`)
  - nextest
- Platform code uses sysinfo and `EXE_SUFFIX`, and nothing is unix-only without a cfg gate.
- `truffle-bench-app` follows the `truffle-tauri-plugin` precedent. `--exclude truffle-bench-app` goes next to the existing excludes in:
  - `ci.yml` (clippy, doc, nextest, doctests) and `deny.toml` `[graph] exclude`
  - `docs/RELEASING.md`, README dev commands and `.github/pull_request_template.md`
  - justfile `test`/`lint`, root `package.json` `test:rust`, and the run-all wrapper
- Benchmarks therefore build against **the same locked dependency versions the library ships with**.
- The version bump and new dependencies are committed together with `Cargo.lock`.
- New engine dependencies (all MIT or Apache): `sysinfo`, `schemars`, `fd-lock`, `machine-uid`, `zip` (already a workspace dep), `getrandom` (already a workspace dep), and `socket2` (for UDP receive buffers).
- The app adds `tauri-plugin-dialog` and nothing else. URLs and Finder use the workspace `open` crate. There is no shell plugin, because cargo-audit scans the whole lock.

**Tauri build without a sidecar.**
- `tauri.conf.json` has `build.devUrl: "http://127.0.0.1:47183"` (Vite `host: 127.0.0.1`, `port: 47183`, `strictPort`), `build.frontendDist: "../dist"`, `beforeDevCommand: "pnpm dev"` and `beforeBuildCommand: "pnpm build"`, and no `externalBin`.
- A plain `cargo build -p truffle-bench-app` and `tauri dev` therefore need neither a sidecar file nor a built frontend: codegen in dev mode with a `devUrl` never reads `frontendDist`.
- Bundling uses `app:build` = `node scripts/prepare-sidecar.mjs --tauri build --config src-tauri/tauri.bundle.conf.json`.
  - Only this form passes `TRUFFLE_BENCH_GIT_SHA`/`_DIRTY` to the build: `prepare-sidecar` sets them in the environment of the Tauri CLI it spawns, and the engine reads them at compile time with `option_env!` (§7.4). `prepare-sidecar && tauri build` yields bundles with `git_sha: null`.
  - `prepare-sidecar` runs `CGO_ENABLED=0 go build -ldflags "-s -w"` in `packages/sidecar-slim`.
  - It writes `src-tauri/binaries/sidecar-slim-<rustc host triple>`, with `.exe` on Windows.
  - Never use `tauri.macos.conf.json` for this, because tauri-build merges it automatically.

**pnpm.**
- `benchmark/app` joins `pnpm-workspace.yaml` in the same change as the regenerated `pnpm-lock.yaml`, and `pnpm install --frozen-lockfile --ignore-scripts` must pass.
- The app's `build` script is frontend-only (`tsc -b && vite build && node scripts/check-dist.mjs`), because CI runs `pnpm -r run build` on ubuntu. `check-dist` fails if `dist/assets` holds no `.woff2` files.
- `test` runs vitest.
- `@tauri-apps/api` and `@tauri-apps/cli` are pinned `~2.10.0` to match the locked tauri 2.10.3; tauri-cli refuses mismatched minors. `@tauri-apps/plugin-dialog` is not needed (dialog is used from Rust only).
- The tsconfig is standalone: lib `["ES2022", "DOM", "DOM.Iterable"]`, `jsx: react-jsx`, `skipLibCheck`. It never extends `tsconfig.base.json`.
- The root `lint`/`format`/`format:check` globs and `.githooks/pre-commit` gain `benchmark/app/src/**/*.{ts,tsx}`. `.prettierignore` gains `benchmark/app/src/brand/`.

**release-please.**
- `packages["."]` gets `"exclude-paths": ["benchmark"]`. The pinned release-please-action v4.4.1 bundles release-please 17.3.0, which supports it.
- That only drops commits whose files are **all** under `benchmark/`. Hub changes that also touch root files must use a non-releasing type (`chore(bench):`, `ci:`, `build:`, `docs:`).
- The truffle-core additions in §5.1 are a real feature, `feat(core): …`, and can ship on their own.
- Neither new crate carries an `x-release-please-version` marker.

**.gitignore** gains:
- `benchmark/app/src-tauri/binaries/*` and `!benchmark/app/src-tauri/binaries/.gitkeep`
- `benchmark/app/src-tauri/gen/`

## 5. Engine API (`truffle_bench`)

### 5.1 Core additions (truffle-core, small, land first)

| Addition | Why |
|---|---|
| `pub const VERSION: &str = env!("CARGO_PKG_VERSION");` in `lib.rs` | Reports record the library version. |
| `impl Node<TailscaleProvider> { pub fn sidecar_pid(&self) -> Option<u32> }`, from the sidecar `Child::id()` | CPU and RSS of the sidecar. In pair mode two identical children share one parent, so they cannot be told apart otherwise. |
| `TransferResult` gains `pub timings: TransferTimings { pub hash_secs: f64, pub negotiate_secs: f64 /* OFFER sent → TCP stream open */, pub transfer_secs: f64 /* first payload write → ACK read */, pub ack_ok: bool }` | An honest wire throughput figure. Progress events are throttled to 4/s and measured at the sender's socket buffer. |
| `receiver::accept_and_receive` calls `node.unlisten_tcp(port)` after accept | Today a long-lived hub leaks one tsnet listener per received file. |

All other core shortcomings stay out of v1 and become notes or planned items: the dropped `tsnet:needsApproval` event, the private sidecar protocol version, disco-only ping, `MaxIdleConnsPerHost`, and QUIC datagrams. (r4: core has since forwarded `needsApproval` and raised the proxy's idle pool, see §14 #20–#22.)

### 5.2 Constants, errors

```rust
pub const APP_ID: &str = "truffle-bench";
pub const CONTROL_NS: &str = "bench";
pub const WS_NS: &str = "bench.ws";
pub const PROTOCOL: u32 = 1;
pub const TOOL_VERSION: &str = env!("CARGO_PKG_VERSION");
pub struct AppError { pub code: ErrorCode, pub message: String }   // serde: {code: "kebab", message}
pub enum ErrorCode { NotRunning, PeerNotFound, PeerAmbiguous, PeerOffline, LoginRefused, Busy,
    BeingMeasured, ProtocolMismatch, UnsupportedWorkload, InvalidParams, Timeout, StateInUse,
    SidecarNotFound, Cancelled, Io, Internal }
```

### 5.3 Configuration and sidecar resolution

```rust
pub enum Tool { App, Cli }
pub struct HubConfig {
    pub device_name: String, pub state_dir: PathBuf, pub data_dir: PathBuf,
    pub sidecar_path: Option<PathBuf>, pub auth_key: Option<String>, pub ephemeral: bool,
    pub app_id: String, pub allow_other_logins: bool, pub allow_tagged: bool, pub tool: Tool,
    pub bundled: bool,   // true only inside a packaged app (Tauri cfg(not(dev)))
}
impl HubConfig { pub fn defaults(tool: Tool, profile: Option<&str>, data_dir: PathBuf) -> Self; }
pub fn resolve_sidecar(cfg: &HubConfig) -> Result<PathBuf, AppError>;
pub fn resolve_sidecar_with(explicit: Option<&Path>, env: &dyn Fn(&str) -> Option<OsString>,
    exe_dir: Option<&Path>, bundled: bool, dev_path: &Path) -> Result<PathBuf, AppError>;  // pure; unit-tested
```

**Defaults.**
- `device_name`: the OS hostname for the app, or `<hostname>-cli` for the CLI. `-<profile>` is appended when a profile is set.
- `state_dir`: `<data_dir>/state/<slug(device_name)>`.
- CLI `data_dir`: `dirs::data_dir()/truffle-bench-cli`.
- App `data_dir`: the Tauri app-data dir, with `-<profile>` appended.
- `allow_tagged`: true when `auth_key` is set, otherwise false.
- `ephemeral`: false, except in `pair`.

**Sidecar resolution order.** The CLI never looks next to its own executable, because the cargo target dir may be shared across projects and hold foreign copies.
1. explicit
2. `TRUFFLE_TEST_SIDECAR_PATH`
3. `TRUFFLE_SIDECAR_PATH`
4. only when `tool == App && bundled`: `<exe dir>/sidecar-slim{EXE_SUFFIX}`
5. the dev binary `crates/truffle-core/tests/test-sidecar`, as a compile-time path from `CARGO_MANIFEST_DIR/../../crates/truffle-core/tests/test-sidecar`, used only if it exists
6. otherwise `SidecarNotFound`, listing every path tried

It never uses `~/.config/truffle/bin`.

**Node build.**
- `Node::builder().app_id(..)?.device_name(..).state_dir(..).sidecar_path(..).ephemeral(..)`, plus `.auth_key(..)` when a key is set.
- Then `build_with_auth_handler`.
- Never call `.hostname()`, which breaks app-scoped discovery.
- Use the default `ws_port`, since both peers must agree on it.

### 5.4 Hub

```rust
pub struct Hub { .. }
impl Hub {
    pub fn new(cfg: HubConfig) -> Arc<Hub>;                         // state "idle"; subscribe() works at once
    pub async fn start(self: &Arc<Self>) -> Result<(), AppError>;   // emits Node events (below)
    pub async fn stop(&self);                                       // valid in every state
    pub fn state(&self) -> NodeState;
    pub fn subscribe(&self) -> broadcast::Receiver<HubEvent>;
    pub fn local(&self) -> Option<LocalInfo>;
    pub fn node(&self) -> Option<Arc<Node<TailscaleProvider>>>;
    pub async fn peers(&self) -> Vec<BenchPeer>;
    pub async fn probe(&self, peer: &str) -> Result<PeerProbe, AppError>;
    pub async fn run(&self, req: RunRequest, progress: ProgressSink) -> RunReport;  // never panics
    pub async fn suite(&self, req: SuiteRequest, progress: SuiteProgressSink) -> SuiteResult;
    pub fn cancel(&self, id: &str) -> bool;                         // run_id or suite_id
    pub fn active_runs(&self) -> Vec<ActiveRun>;
    pub fn attach(&self, run_id: &str, progress: ProgressSink) -> bool;  // replays last Progress, then streams
    pub fn set_allow_other_logins(&self, on: bool);
    pub async fn sign_out(&self) -> Result<(), AppError>;           // LocalAPI logout → stop → delete state_dir
    pub fn store(&self) -> &RunStore;
}
pub type ProgressSink = Arc<dyn Fn(Progress) + Send + Sync>;
pub struct RunRequest { pub run_id: String, pub peer: String, pub params: WorkloadParams, pub suite_id: Option<String> }
pub struct SuiteRequest { pub suite_id: String, pub profile: String, pub peer: String,
    pub overrides: Vec<(String, serde_json::Value)> /* "<workload>.<field>" */ }
```

**Start.**
- `start` takes an exclusive `fd-lock` on `<state_dir>/hub.lock`. If the lock is held it fails with `StateInUse`.
- It emits `HubEvent::Node` at each stage:
  - `starting`
  - `needs-login`, carrying `auth_url` for every URL the auth handler delivers
  - `running`, carrying `local`
  - `error`
- While running, it watches `node.on_peer_change()` for `PeerEvent::AuthRequired` and re-emits `needs-login`.
- `stop()` aborts a start in progress. Dropping the build future kills the sidecar, because the child has `kill_on_drop`.
- At start the hub also:
  - installs the single file-offer router (§8.5)
  - spawns the responder
  - spawns the hello cache: a `hello` to every online same-app peer when it appears or reconnects, cached in `BenchPeer.hello` / `hello_error`
  - spawns the peers publisher, which emits `HubEvent::Peers` on every change, debounced to 250 ms
- It records `hub_start_ms` (start → Running) and, per peer, `discovery_ms` (Running → first listed) for the path workload.

**Peer resolution.**
- `Hub::run` resolves `req.peer` once with `node.peer(&req.peer, Some(30_000))`.
- `None` fails with `PeerNotFound`. `AmbiguousPeer` fails with `PeerAmbiguous` and lists the candidates.
- From then on `Peer.tailscale_id` is **the only peer key**:
  - the argument of every `send_typed` and `send_file`
  - the `msg.from` filter value
  - the busy slot
  - the data-plane identity check
- `resolve_peer_id()` is never used for these, because it returns the ULID once hello is done.

**RunStore** (`report.rs`) keeps one file per run in `<data_dir>/runs/`, named `<ts>-<workload>-<run_id>.json`.
- `list() -> Vec<RunSummary>`, `get`, `delete`, `save`.
- `import(path) -> ImportResult { imported, skipped }` takes a file, a directory or a `.zip` (GitHub artifact downloads).
  - v1 has no `schema_version`.
  - Rust v2 has `environment` and `network` objects.
  - **tsnet-bench** reports `workload == "native-tsnet-tcp"` with top-level `topology`/`before`/`after`.
  - All of these are normalised into v3 with `tool: "imported"` (§7.5).

## 6. Control protocol

All control traffic is `send_typed(peer.tailscale_id, "bench", msg_type, json)`.

**Request ids and matching.**
- Request ids are random UUID v4.
- The requester matches a reply when all three hold: `msg.from == peer.tailscale_id`, `msg.msg_type == "<req>.ack"`, and `payload.id == id`.
- Every `.ack` carries `ok: bool`, `error?: string` and `code?: ErrorCode`.
- The responder ignores `*.ack` messages and handles each request on its own tracked task.
- `SessionError::ReconnectBackoff{retry_after}` is retried after `retry_after` until the request deadline.

**Timeouts:** hello 30 s, prepare 30 s, start 10 s, clock 5 s per sample, and finish 30 s plus the workload's drain allowance.

**Message types:**

| Message → reply | Payload → reply payload |
|---|---|
| `hello` → `hello.ack` | `{id, hello: PeerHello}` → `{id, ok, hello: PeerHello}`. Always answered, across accounts. `host_fingerprint` is omitted for non-admitted peers. |
| `clock` → `clock.ack` | `{id, t1_ns}` → `{id, ok, t1_ns, t2_ns, t3_ns}` |
| `prepare` → `prepare.ack` | `{id, run_id, params, client: Value}` → `{id, ok, run_id, run_tag: u64, params /* as validated & used */, setup: Value, overlapping_runs}` |
| `start` → `start.ack` | `{id, run_id}` → `{id, ok, run_id}` |
| `finish` → `finish.ack` | `{id, run_id, info: Value}` → `{id, ok, run_id, stats: Value, resources: ResourceStats?, path_bytes: PathBytes?, max_overlapping_runs}` |
| `cancel` | `{run_id}`, no reply |
| `tick` (responder → initiator) | `{run_id, t_ns, values: {key: f64}}`, no reply, ≤ 4 Hz. Live receiver-side numbers. |
| `ft.done` (responder → initiator) | `{run_id, iteration, ok, bytes, sha256}`, no reply |

- `client`, `setup`, `info` and `stats` are typed structs defined once per workload in its module, so the same Rust type is used on both sides. On the wire they are JSON objects.
- A params struct uses `#[serde(deny_unknown_fields)]`.
- If `hello.protocol != PROTOCOL`, the run fails with `ProtocolMismatch`.

```rust
pub struct PeerHello {
    pub protocol: u32, pub app: String /* "truffle-bench-app" | "truffle-bench-cli" */,
    pub tool_version: String, pub truffle_version: String, pub build_profile: String /* debug|release */,
    pub git_sha: Option<String>, pub sidecar_sha256: Option<String>,
    pub device_name: String, pub device_id: Option<String>, pub os: String, pub arch: String,
    pub logical_cpus: Option<u32>, pub login_name: Option<String>,
    pub host_fingerprint: Option<String>, pub host_fingerprint_weak: bool,
    pub workloads: Vec<String>, pub accepts_runs_from_you: bool, pub busy: Option<String>,
}
```

**Clock.**
- Every engine timestamp is `now_ns()`: the wall clock read once at hub start, plus monotonic `Instant` elapsed since then. NTP steps cannot move it.
- `clock::estimate(peer, samples = 16, spacing = 10 ms)` computes, per sample:
  - δ = (t4 − t1) − (t3 − t2)
  - θ = ((t2 − t1) + (t3 − t4)) / 2
- It keeps the sample with the smallest δ and returns `ClockOffset { offset_ns: θ /* peer − local */, delay_ns: δ, uncertainty_ns: ⌈δ/2⌉, at_ns: t4, samples }`.
- A workload that maps responder times into initiator time (video one-way latency, responder-produced series, the bidirectional overlap) estimates before **and** after. It maps with `offset(t)` linearly interpolated between the two, and records `drift_ppm`.
- No `clock` request runs while a data workload toward that peer is in its data phase.

**Responder rules.** Every hub and every `serve` node runs a responder.

1. **Admission.**
   - `prepare`, `start`, `clock`, `finish` and `cancel` require admission.
   - The sender is looked up with `node.peer(&msg.from, Some(5_000))`. If not found, the reply is `code: peer-not-found`, which is retryable (the initiator re-sends `prepare` every 1 s within its timeout, §14 #24).
   - A peer is admitted iff one of these holds:
     - `allow_other_logins`
     - both `login_name`s are `Some`, equal ignoring case, and not `"tagged-devices"`
     - both are `"tagged-devices"` and `allow_tagged`
   - `None` on either side is refused, with `code: login-refused`; while the hub still looks for its own login after a fast restart, it first waits up to 10 s for it (§14 #24).
   - `finish`, `cancel`, `start` and ticks are honoured only from the tailscale_id that prepared the run.
   - Every `bench.ws` message, SyncedStore `PeerUpdated` and file offer is ignored unless it comes from that run's initiator.
2. **Validation.** `params.validate()` runs again; the responder never trusts the initiator's bounds. `run_id` must match `^[A-Za-z0-9-]{1,64}$`, otherwise `code: invalid-params`. Run state is keyed by `(peer tailscale_id, run_id)`. `run_id` never appears in a filesystem path. `run_tag` is 64 random bits from the OS RNG, chosen by the responder.
3. **One data-plane run per hub, in either role.**
   - A hub that is initiating answers `prepare` with `code: busy`.
   - A hub that is responding refuses to start a local run with `code: being-measured` ("this device is being measured by <peer>").
   - `path` and `probe` are exempt.
   - `overlapping_runs` records the exempt runs active during the window; History flags any run where it is > 0.
   - The responder frees the slot before it sends `finish.ack`.
4. **Watchdog.** Every prepared run has a watchdog at `min(params.deadline() + 60 s, 30 min)`. It tears the run down if `finish` never arrives, and also on `cancel`. A `cancel` for an unknown run leaves a 5-minute tombstone that a later `prepare` honours.
5. **Data-plane identity.**
   - Listeners bind port 0.
   - An accepted TCP connection is kept iff `remote_identity.node_id == initiator.tailscale_id`, or, when no identity is available, the IP of `remote_addr` equals the initiator's `Peer.ip`.
   - QUIC checks `remote_address().ip()` and UDP checks the IP from `recv_from`.
   - Anything else is dropped and counted (`foreign_dropped`).
   - The HTTP proxy path replaces this check with the `Tailscale-Node-Id` header check in §8.1 http.
6. **Files.**
   - Received files go to `<data_dir>/tmp/in-<run_tag_hex>/`.
   - Initiator sources go to `<data_dir>/tmp/out-<run_tag_hex>/`.
   - Each is removed by its owner on finish, cancel or watchdog.
   - `pair` gives each hub its own `state_dir` and `data_dir`.
7. **Teardown order** on finish, cancel, watchdog or `Hub::stop`:
   1. per-stream acks
   2. `QuicListener::close()`
   3. `unlisten_tcp(port)` for every `listen_tcp`
   4. `egress_remove`
   5. `proxy().remove`
   6. `SyncedStore::stop()`
   7. drop sockets

   `Node::stop()` runs only after every run is torn down.
8. **Visibility.** Every prepared, finished, cancelled or expired run is published as `HubEvent::RemoteRun`, so the UI can say "being measured by <peer>".

**Initiator flow (`Hub::run`).**
1. Resolve the peer.
2. Get `hello` (cached).
3. Check the protocol and that the workload is in `hello.workloads`.
4. Settle the path: ping until two consecutive pings report the same `connection`, at most 3 s, and record `settle_ms`. Take the `before` snapshot.
5. Read usermetrics.
6. Start the resources sampler.
7. Run the workload:
   - it calls `ctx.prepare(client)`, `ctx.clock()`, `ctx.start()` and `ctx.finish(info)` as it needs
   - time-based workloads get a 2 Hz path sampler: `node.ping` with a 5 s timeout, recording `{t_ms, connection, rtt_ms}`
8. Take the `after` snapshot and read usermetrics again.
9. Assemble the `RunReport`, save it, and send `Progress{phase: done | failed | cancelled}`.

Any error after `prepare` still sends `finish` (or `cancel`), so the responder tears down.

**Data plane.**
- **TCP, QUIC and egress streams.** Every stream the initiator opens starts with a 16-byte header:
  - `b"TBN1"`
  - `run_tag: u64 BE`
  - `stream_idx: u16 BE`
  - `kind: u8` (0 upload, 1 download, 2 echo, 3 frames)
  - `reserved: u8`
- **UDP datagrams** start with a 22-byte header:
  - `b"TB"`
  - `run_tag_lo: u32 BE`
  - `seq: u64 BE`
  - `send_ns: u64 BE`
- A wrong magic or tag closes the connection or drops the datagram, and it is counted.

## 7. Types shared with the app

Rust is the source of truth.
- Every type here derives `serde` (snake_case fields) and `schemars::JsonSchema`.
- The test `schema_is_current` regenerates `benchmark/schema/report.v3.schema.json` and `benchmark/schema/fixtures/*.json`, one canonical example per type and variant:
  - `HubEvent` ×3 and `NodeState` per state
  - every `WorkloadParams` variant
  - `Progress`
  - `RunReport` for every workload, ok and failed
  - `RunSummary`, `AppInfo`, `Settings`, `BenchPeer`, `PeerProbe`, `SuiteProfile`, `AppError`
- The test fails on drift; `UPDATE_SCHEMA=1` rewrites the files.
- `app/src/api/types.ts` is hand-written. `app/src/api/fixtures.test.ts` (vitest) imports every fixture with `satisfies <Type>`, so `tsc` and CI catch drift. The mock API serves only fixtures.

### 7.1 Node, peers, events

```rust
pub struct LocalInfo { device_name, device_id, tailscale_id, tailscale_hostname, tailscale_ip: Option<String>,
    dns_name: Option<String>, tailnet: Option<String> /* from dns_name */, login_name: Option<String>,
    app_id, health: HealthSummary, sidecar_path: String, hub_start_ms: Option<u64> }
pub struct HealthSummary { state: String, healthy: bool, warnings: Vec<String>, key_expiry: Option<String> }
pub struct BenchPeer { id /* tailscale_id */, device_id: Option<String>, device_name: String, hostname: String,
    ip: String, os: Option<String>, online: bool, ws_connected: bool, connection: String,
    login_name: Option<String>, same_login: bool, tagged: bool, last_seen: Option<String>,
    hello: Option<PeerHello>, hello_error: Option<AppError>, rtt_ms: Option<f64> }
pub struct PeerProbe { peer: BenchPeer, hello: PeerHello, path: PathSnapshot }
pub struct PathSnapshot { at: String, connection: Option<String>, rtt_ms: Option<f64>,
    endpoint: Option<String>, error: Option<String> }
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum HubEvent { Node(NodeState), Peers { peers: Vec<BenchPeer> },
    RemoteRun { run_id: String, peer: String, peer_id: String, workload: String,
                state: String /* prepared|finished|cancelled|expired */ } }
pub struct NodeState { state: String /* idle|starting|needs-login|needs-approval|running|stopping|stopped|error */,
    auth_url: Option<String>, error: Option<AppError>, local: Option<LocalInfo>,
    tailscale_hostname: Option<String>, since_ms: u64 }
pub struct ActiveRun { run_id, suite_id: Option<String>, workload, peer_name, started_at, last: Option<Progress> }
```

### 7.2 Progress

```rust
pub struct Progress { run_id: String, suite_id: Option<String>, t_ms: u64,
    phase: String /* preparing|settling|clock|warmup|running|draining|finishing|done|failed|cancelled */,
    fraction: Option<f32>, samples: Vec<Sample>, message: Option<String> }
pub struct Sample { key: String, value: f64 }
pub struct SuiteProgress { suite_id, index: u32, total: u32, item_label: String, run: Progress }
pub struct SuiteResult { suite_id, run_ids: Vec<String>, failed: u32, cancelled: bool }
```

**Progress rate and sample keys.**
- During `running`, Progress is throttled to ≤ 4 events/s, aligned to 250 ms bucket ends. Other phases emit on change.
- Live receiver-side keys come only from `tick`s.
- Sender-side live values use distinct keys such as `sent_mbps`, so they are never confused with goodput.

### 7.3 Workload parameters

```rust
#[serde(tag = "workload", rename_all = "kebab-case")]
pub enum WorkloadParams { Path(PathParams), Tcp(TcpParams), Udp(UdpParams), Quic(QuicParams), Ws(WsParams),
    FileTransfer(FileTransferParams), SyncedStore(StoreParams), Http(HttpParams), Video(VideoParams),
    Egress(EgressParams) }
impl WorkloadParams { fn validate(&self) -> Result<(), String>; fn deadline(&self) -> Duration;
    fn label(&self) -> String; fn time_based(&self) -> bool; }
```

Every field has a default. `validate()` rejects out-of-bounds values; it never clamps them.

| Workload | Params: default (bounds) |
|---|---|
| `path` | `count` 20 (2–200); `interval_ms` 250 (50–5000); `count × interval_ms` ≤ 300 s |
| `tcp` | `mode` throughput \| latency (throughput); `direction` upload \| download \| bidirectional (upload); `streams` 1 (1–16); `duration_secs` 10 (2–60); `warmup_secs` 1 (0–10, < duration − 1); `payload_bytes` 64 (1–65536, latency); `count` 200 (10–10000, latency); `warmup_count` 10 (0–1000, < count); `nodelay` true |
| `udp` | `mode` stream \| echo \| ramp (stream); `direction` upload \| download (upload); `payload_bytes` 1200 (30–1400); `rate_mbps` 10 (0.1–1000; for ramp, the start rate); `pps` 50 (1–2000, echo); `duration_secs` 10 (2–60); `warmup_secs` 1; `warmup_count` 10 (echo) |
| `quic` | `mode` throughput \| latency; `direction` (as tcp); `streams` 4 (1–32); `duration_secs` 10 (2–60); `warmup_secs` 1; `payload_bytes` 64 (1–65536); `count` 200 (10–10000); `warmup_count` 10 |
| `ws` | `mode` rr \| stream (rr); `payload_bytes` 64 (rr 0–1048576; stream 0–262144); `count` 500 (rr 10–100000); `concurrency` 1 (1–64, rr); `warmup_count` 10 (rr); `duration_secs` 10 (2–60, stream); `rate_per_sec` null (1–100000, stream; null means as fast as backpressure allows); rr `count × payload_bytes` ≤ 1 GiB |
| `file-transfer` | `size_bytes` 10485760 (1024–1073741824); `iterations` 3 (1–20); `size_bytes × iterations` ≤ 4 GiB |
| `synced-store` | `mode` pingpong \| burst (pingpong); `count` 100 (10–5000); `payload_bytes` 64 (0–65536); `warmup_count` 5 |
| `http` | `target` proxy \| direct (proxy); `connection` keepalive \| new (keepalive); `concurrency` 4 (1–64); `duration_secs` 10 (2–60); `warmup_secs` 1; `response_bytes` 1024 (0–10485760) |
| `video` | `transport` udp \| quic \| tcp (udp); `profile` 720p30 \| 1080p30 \| 1080p60 \| 4k30 (1080p30); `bitrate_mbps` null (0.5–100); `fps` null (1–120); `gop` 60 (1–600, ≤ fps × duration); `keyframe_ratio` 8 (1–20); `duration_secs` 10 (2–60); `warmup_secs` 1; `pacing` paced \| burst (paced); `jitter_buffer_ms` 100 (0–1000) |
| `egress` | `direction` upload \| download (upload); `streams` 1 (1–8); `duration_secs` 10 (2–60); `warmup_secs` 1 |

**Video profiles** (fps, Mb/s): 720p30 (30, 2.5), 1080p30 (30, 6), 1080p60 (60, 12), 4k30 (30, 25).

**`deadline()`** is when the initiator aborts and when the responder watchdog fires (the watchdog adds 60 s and caps at 30 min).

| Workload | Deadline |
|---|---|
| Duration workloads | warmup + duration + 30 s |
| path | count × (interval + 5 s) |
| ws rr | 30 s + count × max(payload, 1024) × 8 / 1 Mb/s |
| synced-store | count × 10 s |
| file-transfer | iterations × (90 s + size × 8 / 1 Mb/s) |

### 7.4 Report (schema v3)

```jsonc
{
  "schema_version": 3, "run_id": "…", "suite_id": null,
  "tool": "truffle-bench-app" | "truffle-bench-cli" | "imported",
  "tool_version": "0.1.0", "truffle_version": "0.7.17", "methodology": "v3",
  "workload": "tcp", "label": "TCP throughput · upload · 4 streams · 10 s",
  "status": "ok" | "failed" | "cancelled", "error": null, "error_code": null,
  "started_at": "RFC 3339", "finished_at": "RFC 3339", "duration_ms": 12345,
  "git_sha": null, "git_dirty": null,              // copies of local's
  "compare_key": "16 hex",
  "local": NodeEnv, "peer": NodeEnv | null,
  "network": {
    "topology": "same-process" | "same-host" | "separate-hosts",
    "settle_ms": 412, "before": PathSnapshot, "after": PathSnapshot,
    "clock": { "before": ClockOffset, "after": ClockOffset | null, "drift_ppm": 3.1, "uncertainty_ns": 412000 } | null,
    "path_timeline": [ { "t_ms": 500, "connection": "direct", "rtt_ms": 3.2 } ],
    "loaded_rtt_ms": Distribution | null,
    "path_bytes": { "direct": 0, "derp": 0, "peer_relay": 0 } | null,   // sum of both sides' usermetrics Δ
    "derp_share": 0.0 | null,
    "path_label": "direct" | "relay" | "mixed" | "unknown"
  },
  "params": { "workload": "tcp", … },             // as validated and used by the responder
  "summary": [ { "key": "goodput_mbps", "label": "Goodput", "value": 912.4, "unit": "Mb/s",
                 "better": "higher" | "lower", "uncertainty": null } ],
  "results": { … },                                // workload-specific, §8
  "series": [ { "key": "goodput_mbps", "label": "Goodput", "unit": "Mb/s",
                "points": [[t_ms, value], …], "warmup_ms": 1000 } ],
  "resources": { "local": ResourceStats | null, "peer": ResourceStats | null, "combined": ResourceStats | null },
  "overlapping_runs": 0,
  "notes": [ "…" ]
}
```

**`NodeEnv`** has these fields:
- `device_name`, `os`, `arch`, `logical_cpus`
- `tool`, `tool_version`, `truffle_version`
- `build_profile` (`debug` | `release`), `rustc`
- `git_sha`, `git_dirty`
- `sidecar_path`, `sidecar_sha256`
- `tailscale_ip`, `host_fingerprint`, `host_fingerprint_weak`
- `on_battery: Option<bool>` (macOS `pmset -g batt`; elsewhere null)
- `app_foreground_pct: Option<f64>` (app only)

Where the git fields come from:
- CLI: `git -C env!("CARGO_MANIFEST_DIR")` at run time, never the current directory.
- App: build-time env (`TRUFFLE_BENCH_GIT_SHA`/`_DIRTY`, set by `app:build`), otherwise null.

**`host_fingerprint`** is the first 16 hex characters of `sha256(machine_id ‖ hostname)`, using the `machine-uid` crate. When no machine id is available it falls back to the hostname and sets `host_fingerprint_weak = true`.

**Topology.**
- `same-process` is set by `pair` itself.
- `same-host` requires matching fingerprints, neither of them weak.
- Otherwise `separate-hosts`.

**`path_label`.**
- `direct` iff `derp_share < 1%` and the timeline shows no transition.
- `relay` iff `derp_share > 99%`.
- Otherwise `mixed`, with a note.
- Without usermetrics it falls back to the timeline, or `unknown`.
- History filters on this, never on `before.connection`.

**`ResourceStats`** has `cpu_ms`, `cpu_pct_avg`, `cpu_pct_max`, `rss_mb_max`, `cpu_ms_per_mb`, `sidecar_cpu_ms`, `sidecar_cpu_pct_avg`, `sidecar_cpu_pct_max`, `sidecar_rss_mb_max` and `sidecar_cpu_ms_per_mb`.
- CPU comes from cumulative CPU time (user + sys) over the measurement window. 100 = one logical CPU, so values can exceed 100.
- `cpu_pct_max` is taken over 500 ms intervals, after discarding the priming interval.
- Only two PIDs are refreshed: this process, and `sidecar_pid()`.
- `*_per_mb` divides by goodput bytes / 1e6.
- In `same-process` topology, `local` and `peer` are null and `combined` holds the process plus both sidecars.

**`Distribution`** has `samples`, `lost`, `min`, `mean`, `p50`, `p90`, `p95`, `p99`, `p999`, `max` and `stddev`.
- Percentiles are nearest-rank: `sorted[⌈p·n⌉ − 1]`.
- A percentile p is null unless n ≥ ⌈1/(1 − p)⌉. So p90 needs 10 samples, p95 20, p99 100, and p999 1000.
- `stddev` is the sample standard deviation (n − 1), null when n < 2.
- Lost or timed-out samples enter as +∞. A percentile that lands on one is null, and losses are counted in `lost`.

**`compare_key`** is the first 16 hex characters of `sha256` over a canonical JSON object containing:
- `workload`
- `params` minus `duration_secs`, `count`, `iterations` and the warmups
- `topology`, `path_label`
- both `build_profile`s
- `methodology`
- the peer's `host_fingerprint`

Sparklines, trends, "latest" cards and comparisons only group runs with the same key. Comparing different keys lists the fields that differ. A debug build on either side adds the note "debug build: not comparable with release". History never trends a debug run with a release one, and `suite --profile nightly` refuses debug builds unless `--allow-debug` is passed.

**Honesty notes.**
- `same-host` and `same-process` runs carry the note "measures the bridges and netstack, not a link".
- A run with `overlapping_runs > 0` carries "shared the machine with N other runs".
- A `mixed` path carries the path transitions.

### 7.5 Normalising old reports (v1, v2, tsnet-bench)

- `methodology` is set to `v2-transport`, `v2-file-transfer`, `v2-request-reply`, `v2-synced-store` or `native-tsnet`.
- MB/s (10⁶ B/s) is converted to Mb/s (× 8).
- v2 file-transfer throughput maps to `e2e_mbps`, never `wire_mbps`.
- v2 transport maps to `tcp`/`quic` with the key `fixed_size_mbps`, not `goodput_mbps`, because it is a different method.
- v2 `debug_assertions` and `sidecar_sha256` carry across.
- tsnet-bench reports get `peer: null`, `goodput_mbps` derived from their samples, and the note "native Go/tsnet baseline".

## 8. Workloads

Each workload module defines its params, `Client`, `Setup`, `Info`, `Stats` and `Results` structs, plus:

```rust
pub async fn run(ctx: &mut RunCtx<'_>, p: &XParams) -> Result<WorkloadOutput, AppError>;          // initiator
pub async fn prepare(r: &ResponderCtx, p: &XParams, client: XClient) -> Result<(XSetup, Box<dyn ResponderRun>), AppError>;
pub trait ResponderRun: Send { fn start(&mut self) -> BoxFuture<'_, Result<(), AppError>>;
    fn finish(self: Box<Self>, info: serde_json::Value) -> BoxFuture<'static, Result<serde_json::Value, AppError>>;
    fn cancel(self: Box<Self>) -> BoxFuture<'static, ()>; }
pub struct WorkloadOutput { summary: Vec<SummaryItem>, results: serde_json::Value, series: Vec<Series>,
    notes: Vec<String>, window_ns: Option<(u64, u64)> /* for resources */ }
```

### 8.1 Common definitions

**Goodput** is payload bits delivered per second over the measurement window, counted **at the receiver**, excluding the warmup. Mb/s means 10⁶ bits/s. Headers do not count as payload.

**Receiver accounting** (tcp, quic, egress, and either direction of video over TCP and QUIC):
- The receiver keeps one run-level counter across all streams of a direction.
- `epoch_ns` is the receiver's `now_ns()` at the first payload byte of any stream.
- Bucket i covers `[epoch + 250·i ms, epoch + 250·(i+1) ms)`, and `buckets[i]` is the payload bytes read in it.
- `last_ns` is the time of the last payload byte of any stream.
- After EOF on a stream, the receiver writes back `[u64 BE stream_bytes]`. This is a barrier and a cross-check only; it carries no timing.
- The result is `ReceiverStats { epoch_ns, bucket_ms: 250, buckets, last_ns, bytes_total, streams: [{stream_idx, bytes, first_ns, last_ns}] }`. When the responder is the receiver it comes back in `finish.ack.stats`; when the initiator is the receiver it is computed locally.

**Goodput formula.**
- `goodput_mbps = (bytes_total − Σ buckets[0 .. 4·warmup_secs]) · 8 / (last_ns − epoch_ns − warmup·1e9) · 1e3`.
- If `last_ns − epoch_ns < warmup + 1 s`, the run fails with "window too short".

**Series.** Point i is `buckets[i] · 8 / 0.25 / 1e6`, plotted at the end of the bucket. The final partial bucket is divided by its true length. Warmup points are kept and flagged with `warmup_ms`. Responder-side series are mapped into the initiator timebase with `offset(t)`.

**Sender checks.** The sender reports `sent_bytes` per stream in `finish.info`, and the test asserts received ≤ sent and Σ buckets == bytes_total.

**Bidirectional** opens `streams` upload streams and `streams` download streams concurrently, each counted by its own receiver.
- Each direction's goodput is computed over the overlap window `[max(epoch_up, epoch_down) + warmup, min(last_up, last_down)]`, mapped to the initiator timebase.
- `goodput_mbps = upload_mbps + download_mbps`, labelled "sum of both directions".

**Latency modes** (tcp, quic, udp echo, ws rr, synced-store pingpong):
- The first `warmup_count` samples are discarded.
- Each message is written with one `write_all`.
- RTT is measured on one clock: send call to reply received.

**Loaded RTT.** Every time-based workload reports `loaded_rtt_ms`, taken from the path sampler. This is latency under load.

**CPU per MB.** Every throughput workload reports `cpu_ms_per_mb`, taken from the resources.

### 8.2 Workload definitions

**path** (initiator only; no prepare)
- Pings `count` times at `interval_ms` with `node.ping`, each wrapped in a 5 s timeout; a timeout counts as lost.
- Sample 0 is reported as `first_rtt_ms` and excluded from the distribution.
- Records `connection` per sample, plus `transitions`.
- Also reports `hub_start_ms`, `discovery_ms` (this peer) and `ws_connect_ms` (the time to the first `hello.ack` when the peer was not ws_connected).
- Summary keys: `rtt_p50_ms`, `rtt_min_ms`, `rtt_max_ms`, `loss_pct`.
- Series: `rtt_ms`.

**tcp throughput**
- Opens N streams with `open_tcp`. Both sides apply `nodelay` to their Rust `TcpStream` (the `open_tcp` result and `RawIncoming.stream`). Writes are 64 KiB.
- **Upload:** the initiator writes for `warmup + duration` and then shuts down its write half. The responder is the receiver.
- **Download:** the initiator sends the header with `kind=1`, and the responder writes for `warmup + duration`.
- `connect_ms` is the time from the `open_tcp` call to its return. That covers local sidecar IPC, the tailnet handshake and the bridge handoff. The responder's per-connection WhoIs falls into the first data instead.
- Summary keys: `goodput_mbps` (plus `upload_mbps` and `download_mbps` for bidirectional), `connect_ms` (median over streams), `cpu_ms_per_mb`, `loaded_rtt_p50_ms`.
- Series: `goodput_mbps`.

**tcp latency**
- One stream, ping-pong of `payload_bytes` × `count`.
- Summary keys: `rtt_p50_ms`, `rtt_p99_ms` (null when n < 100), `rtt_max_ms`.
- Series: `rtt_ms`.
- Note: Nagle exists only on the two Rust loopback sockets. In `full`, 32 KiB payloads cross the macOS lo0 MTU, so the `nodelay` A/B is visible there.

**udp stream** (Client `{udp_port}`; `start` triggers download)
- The socket's tailnet port is the `tsnet_port()` of the `DatagramSocket::Network` variant. `local_addr()` is the loopback relay and is never advertised.
- The initiator binds with `bind_udp(0)` before `prepare`.
- In download mode the responder sends only to `<initiator Peer.ip>:<client.udp_port>`, and only after `start`.
- Receivers raise `SO_RCVBUF` to 4 MiB on `inner()` with `socket2::SockRef`, on a best-effort basis.
- **Pacing:** a token bucket refilled from elapsed time on each 1 ms tick, with depth `max(1, rate × 2 ms)`. `send_ns` is stamped just before `send_to`.
- **Offered load:**
  - The sender reports `offered_mbps`, `max_burst_datagrams` and `rate_achieved = offered ≥ 0.98 · target`.
  - When the target is not achieved, the summary notes "sender could not sustain target" and History excludes the run from trends.
- **Receiver definitions** (computed after drain):
  - `sent`: datagrams whose `send_to` returned Ok. Errors are counted as `send_errors`.
  - `unique`: distinct seqs received. `dup`: repeats.
  - `reordered`: a unique datagram with seq < the max seen before it (RFC 4737).
  - `loss_pct = (sent − unique) / sent · 100`.
  - `max_loss_burst`: the longest run of never-received seqs.
  - `jitter_ms`: RFC 3550 J with gain 1/16 over unique datagrams in arrival order, using D = (Rj − Ri) − (Sj − Si) on raw clocks (offset-invariant), taken at the end of the window.
- **Drain:**
  - `finish.info` carries `last_seq`.
  - The receiver snapshots when it has seen `last_seq`, or after `max(500 ms, 4 × before.rtt)` with no arrivals, capped at 3 s.
  - After its last datagram, the sender waits for the same period before sending `finish`.
- **Loss series:** `finish.info.bucket_first_seq` records the first seq of each 250 ms send bucket. A bucket's loss is the seqs in `[first_b, first_{b+1})` that were never received. This is exact and clock-free.
- **Loss locality** (only when usermetrics is available on both sides; an approximation, because other traffic counts too):
  - `local_send_drops = sent − sender outbound_packets Δ`
  - `network = sender outbound Δ − receiver inbound Δ`
  - `local_recv_drops = receiver inbound Δ − unique`
- Summary keys: `goodput_mbps`, `offered_mbps`, `loss_pct`, `jitter_ms`.
- Series: `goodput_mbps` and `loss_pct`.
- Notes:
  - payload > 1252 B fragments in netstack
  - IPv4 only
  - loss is "end to end, including loopback relay drops"

**udp echo**
- Paced at `pps`. The responder echoes each datagram.
- RTT = `recv_now − send_ns`, on the same clock.
- A datagram not echoed by the drain deadline counts as lost.
- Summary keys: `rtt_p50_ms`, `rtt_p99_ms`, `loss_pct`.

**udp ramp**
- Starts at `rate_mbps` and multiplies by 1.5 every 3 s until bucket loss > 2% for two consecutive buckets, or the sender can no longer achieve the rate.
- Summary key: `max_sustainable_mbps`, the last rate with loss ≤ 2%.
- Series: `offered_mbps` and `loss_pct`.

**quic throughput / latency**
- One `connect_quic`. Each stream gets `into_split`.
- Accounting is the same as tcp, with a per-stream ack written after the receiver sees `finish()`.
- `connect_ms` is the time for `connect_quic()` to return. It includes setting up the local UDP relay and the QUIC/TLS handshake.
- `flow_control_ceiling_mbps = min(streams·1.25 MB, 10 MB) · 8 / before.rtt_s / 1e6`. When goodput ≥ 80% of it, the run notes "bounded by quinn default flow-control windows".
- Summary keys are the same as tcp, with `connect_ms`.
- Notes:
  - about 1200 B packets, because MTU discovery is off
  - no GSO
  - double encryption

**ws rr**
- A request is `send_typed(peer, "bench.ws", "rr.req", {id, data: base64(payload)})`. The responder replies `rr.resp` with the same `{id, data}`.
- The initiator uses ONE `subscribe("bench.ws")` per run, an `id → Instant` map and a semaphore of `concurrency`. It never calls `send_and_wait` per request.
- Summary keys: `rtt_p50_ms`, `rtt_p99_ms`, `msgs_per_sec`.
- Series: `rtt_ms`.
- This replaces the old `request-reply` workload.

**ws stream**
- Duration-based. Each message is `send_typed(peer, "bench.ws", "bytes", {"encoding": "base64", "data": …, "seq": n})`, so `payload_bytes()` still decodes it.
- The responder drains on a dedicated task.
- **Counting:**
  - `lag_dropped` = Σ `Lagged(n)` on its receiver
  - `missing = sent − unique`
  - `router_dropped = missing − lag_dropped`
- **Finish:**
  - The responder answers `finish` once it has seen `last_seq`, or after 2 s with no messages, having waited for `len() == 0`.
  - The initiator's finish timeout is 10 s plus queued bytes ÷ its observed send rate.
  - Control requests after a ws run are retried up to 3×.
- **Wire size:** `goodput_mbps` counts decoded payload. `wire_bytes_per_msg` is the length of one envelope encoded with `truffle_core::envelope` `JsonCodec`, and `wire_mbps` is derived from it.
- Summary keys: `msgs_per_sec`, `goodput_mbps`, `drop_pct`.
- Note: the WebSocket itself is lossless, so every drop happens at a receiver.

**file-transfer**
- **Offer router:** hub start calls `node.file_transfer().offer_channel(node.clone())` exactly once and runs one offer router for the hub's life. Workloads never call `offer_channel` or `auto_accept`.
- **Accepting:** the router accepts an offer iff all of these hold:
  - a prepared file-transfer run exists for `offer.from_peer` (a tailscale_id)
  - `offer.file_name == "tb-<run_tag_hex>-<iter>.bin"`
  - `offer.size == params.size_bytes`
  - free disk space is at least 2 × size

  It accepts into `tmp/in-<run_tag_hex>/` (trailing slash), and rejects every other offer.
- **Source files:** the initiator writes deterministic-pattern source files into `tmp/out-…/`.
- **Iterations:** after each `send_file`, the initiator waits for `ft.done` for that iteration (60 s) before starting the next. The responder sends `ft.done` on `Completed` or `Failed`. This preserves the regression the old crate's live test guarded, `MAX_PENDING_OFFERS_PER_PEER = 4`.
- **Metrics:**
  - `e2e_mbps = size·8 / elapsed_secs / 1e6`
  - `wire_mbps = size·8 / timings.transfer_secs / 1e6`
  - `hash_ms = timings.hash_secs · 1000`, labelled "SHA-256 of page-cached data"
  - `negotiate_ms`
  - Progress `speed_bps` is never used.
- **ACK check:** an iteration with `ack_ok == false` is valid only when the responder's received sha256 matches, and it is excluded from timing stats.
- **Summary:** the median over iterations, excluding iteration 1 when there are 3 or more.
- Summary keys: `wire_mbps`, `e2e_mbps`, `hash_ms`.
- Series: `wire_mbps` per iteration.

**synced-store**
- **Setup:** the responder creates the store `bench-<run_tag_hex>` in prepare and does not call `set()`.
- **Readiness:** after `prepare.ack`, the initiator creates the same store and calls `set({seq: 0})` every 500 ms, at most 20 times, until a `PeerUpdated` from the responder's device_id carries `{echo: 0}`. Only seq ≥ 1 is timed.
- **Pingpong:**
  - Sample i: A calls `set({seq: i, pad})`. B, on seeing seq i, calls `set({echo: i, pad})`.
  - `rtt_i` runs from A's `set()` to A's `PeerUpdated` carrying echo i.
  - `convergence_i = rtt_i / 2`, labelled "RTT/2 estimate".
  - Per-sample timeout is 5 s; a timeout counts as lost.
- **Burst:**
  - A calls `set()` `count` times back to back.
  - B echoes the highest seq it has observed whenever that changes.
  - `final_echo_ms` = from A's last `set()` to A seeing the echo of `count`.
  - `burst_write_ms` is the time A spent writing.
  - `coalescing_pct = (1 − distinct versions B observed / count) · 100`.
- **Recorded:** `ws_peers` at start and end, because `broadcast_typed` fans out to every WS-connected peer. Pair mode is the reference.
- **Teardown:** both stores `stop()` on finish.
- Summary keys: `convergence_p50_ms` and `convergence_p99_ms`, or for burst `final_echo_ms` and `coalescing_pct`.

**http**
- **Responder server:** a tiny HTTP/1.1 server on `127.0.0.1:0`. It serves only `GET /r/<run_tag_hex>/bytes/<n>`, with n ≤ `response_bytes`. It returns 400 for other sizes and 404 for other paths, and always sends `Content-Length` (never chunked).
- **`proxy` target:**
  - Calls `proxy().add(ProxyConfig { id: "bench-<run_tag_hex>", listen_port: P, target: 127.0.0.1:<server>, tls: false, allow_non_loopback: false, allow: [initiator login] when it is Some and not tagged, otherwise empty, routes: [] })`.
  - P is `20000 + (run_tag % 10000)`. On `PORT_IN_USE` or `LISTEN_ERROR` it tries the next port, up to 8 attempts. Port 0 is never used, because the sidecar reports the requested port, not the bound one.
  - The server answers 403 unless the proxy-injected `Tailscale-Node-Id` equals the initiator's tailscale_id.
- **`direct` target:** `listen_tcp(0)` with the same handler written in Rust, plus rule 5's identity check.
- **Initiator:**
  - `concurrency` workers over `open_tcp`.
  - Keepalive reuses one connection per worker; `new` opens a connection per request.
  - Latency runs from the start of writing the request to the last body byte. In `new` mode it starts at the `open_tcp` call and `connect_ms` is reported separately.
  - `error_pct = (non-2xx + I/O errors + 10 s timeouts) / attempts`.
  - Workers start together, and the first `warmup_secs` are excluded.
  - Records `connections_opened` and error kinds.
- Summary keys: `req_per_sec`, `latency_p50_ms`, `latency_p99_ms`, `error_pct`.
- Series: `req_per_sec`.
- Notes:
  - proxy keep-alive, when the server saw more upstream connections than workers: "the sidecar's proxy reconnected upstream: N upstream connections for W keep-alive workers (sidecars before protocol 8 keep only 2 idle connections per host)"; without server stats, above 2 workers: "proxy keepalive above concurrency 2 includes upstream reconnects on sidecars before protocol 8 (Go MaxIdleConnsPerHost = 2)" (§14 #22)
  - "new: one WhoIs per request on both paths"

**video** (the frame model)
- **Frame sizes:**
  - Let G = gop, K = keyframe_ratio, and b = bitrate.
  - `P = round(b/8 · G/fps / (K + G − 1))` bytes, and `I = K·P`. Frame k is I iff `k mod G == 0`.
  - `offered_mbps` = Σ frame bytes · 8 / (frames / fps) / 1e6. Goodput and loss are compared with offered, not the nominal profile.
- **Capture:** `capture_ns_k = t0 + k·1e9/fps` on the sender. This is scheduled time; frames are never skipped, so sender stalls appear as latency.
- **Wire, UDP:** the 22-byte TB header, then an 18-byte frame header, then the payload, ≤ 1200 B in total.
  - Frame header: `frame_no u32 BE · frag_idx u16 · frag_cnt u16 · flags u8 (bit 0 = keyframe) · reserved u8 · capture_ns u64`.
  - Paced mode spreads a frame's fragments over `[capture, capture + 1/fps)` at 1 ms ticks; burst sends them back to back.
- **Wire, TCP and QUIC:** `[u32 BE len][the same 18-byte header][payload]`.
  - TCP uses one stream.
  - QUIC opens one bidi stream per frame, and the receiver finishes its send half immediately on accept.
- **Receiver:** computes nothing that depends on the clock. `finish.ack.stats.frames` holds raw `{frame_no, keyframe, bytes, capture_ns, first_recv_ns, complete_recv_ns | null}`, all in responder time.
- **Initiator** estimates the clock before and after, then computes:
  - `raw_k = complete_k − capture_k` and the relative delay `d_k = raw_k − min_j raw_j`. This needs no clock offset.
  - `late`: `d_k > jitter_buffer_ms`.
  - `dropped`: not complete by the drain deadline.
  - `on_time`: complete and not late.
  - `delivered_fps = on_time / (frames_sent / fps)`.
  - `freezes`: maximal runs of ≥ 2 consecutive frames that are not on time; `freeze_ms` = Σ run length · 1000/fps.
  - `frame_latency_ms = raw_k − offset(complete_k)`, shown ± uncertainty. It is never used for late, dropped or freeze.
  - `frame_spread_ms`: first to last fragment of a frame (clock-free).
  - `keyframe_complete_ms` p50.
- **Frame strip:** `results.frames` keeps every frame when there are ≤ 3600. Otherwise it keeps every ⌈n/3600⌉-th frame plus every frame that was not on time.
- Summary keys: `delivered_fps`, `late_pct`, `dropped_pct`, `freezes`, `frame_latency_p50_ms` (± uncertainty), `goodput_mbps`.
- Series: `frame_latency_ms`, `goodput_mbps`.

**egress**
- The responder runs the tcp sink.
- The initiator calls `egress_add(EgressRoute { id: "bench-<run_tag_hex>", target: "<responder Peer.ip>:<port>", login: None })`, then a plain `tokio::net::TcpStream::connect(bound.listen)` with the TBN1 header and the same accounting.
- `NodeError::Network(NetworkError::Unsupported(_))` fails the run with "sidecar protocol < 7".
- The route is removed at teardown.
- Note: egress removes only the initiator-side Rust bridge, and the responder's sink still uses one. So tcp crosses 2 bridges and egress 1, and egress − tcp is the cost of one bridge.

### 8.3 Path honesty
- Before every workload the path is settled (§6 flow).
- Time-based runs carry a `path_timeline` and `loaded_rtt_ms`.
- Usermetrics byte deltas come from `/localapi/v0/usermetrics`. Both sides read it at prepare/start and at finish through `loopback_with_local_api()`; the credential never leaves Rust.
- From those deltas, `path_label` decides direct, relay or mixed (§7.4).

### 8.4 Suites (`suites.rs`; shared by CLI and app)

Suites run items sequentially on one hub (one pair in `pair` mode), with `cooldown_ms` 1000 between items.

| Suite | Contents |
|---|---|
| `smoke` | path 10; tcp throughput 3 s; udp stream 3 s at 5 Mb/s; quic throughput 3 s; ws rr 100; file-transfer 1 MiB × 2; synced-store pingpong 20; http 3 s; video 720p30 3 s; egress 3 s |
| `standard` | the defaults of every workload |
| `full` | standard, plus: tcp download, bidirectional and 4 streams; tcp latency at 64, 1024 and 32768 B × nodelay {true, false}; udp stream at payload {200, 1200, 1252, 1253, 1400}; udp echo; udp ramp; quic latency; ws stream; synced-store burst; http direct, `new`, and proxy keepalive at concurrency 1 and 2; video over udp, quic and tcp |
| `nightly` | `full`, except video only over udp and quic, every duration capped at 10 s, and no udp payload sweep. Total workload time ≤ 12 min. |

### 8.5 Not a CRDT

RFC 018's CrdtDoc/Loro was removed on 2026-07-07. `synced-store` is last-write-wins state sync; it is what exists and what is measured. The app says so on the SyncedStore view.

### 8.6 Planned (shown in the app as planned)

- QUIC datagrams (RFC 9221)
- `QuicConnection` stats (rtt, cwnd, lost)
- JS binding lane (NAPI)
- Swift plane (RFC 024) peers
- tsnet ceiling: an in-sidecar `bench:*` command (a later sidecar protocol); use `tsnet-bench` meanwhile
- A latency waterfall (TSMP/ICMP ping; core pins disco today)
- HTTP with `tls: true` (handshake time, first-cert time)
- `pull_file`
- SyncedStore `FileBackend`
- Egress via SOCKS5 / HTTP CONNECT
- Broadcast fan-out (needs ≥ 3 nodes)
- Live camera (WebCodecs)
- Taildrop comparison
- Video ramp

## 9. Housekeeping

### 9.1 Moves
- `git mv crates/truffle-bench benchmark/engine`. The package name `truffle-bench` is kept, so `-p truffle-bench` and the path-less `Cargo.lock` entry stay valid. Bump to 0.1.0.
- `transport` is legacy and binary-only: `src/legacy/{transport, harness, report_v2, error}.rs`.
  - The old transport.rs, harness.rs and v2 writer are moved verbatim, and `anyhow_lite` moves into `legacy/error.rs`.
  - It still writes `schema_version: 2`, now with the additive params `mode`, `port` and `app_id`.
  - It stays byte-compatible with tsnet-bench, and never uses the TBN1 header.
  - It still requires an auth key from the environment.
  - Its sidecar path comes from `resolve_sidecar()`.
- The old `file-transfer`, `synced-store` and `request-reply` subcommands are replaced by `pair <workload>`.

### 9.2 Defined once
- `truffle-bench suite --profile <p> --pair` is the single default set. These all call it with `--locked`:
  - `justfile` (`bench-macro`, `bench-one` → `pair`)
  - root `package.json`: `bench:suite` and `bench:pair` replace `bench:macro*`, `bench:file-transfer`, `bench:synced-store` and `bench:request-reply`
  - `benchmarks.yml`
- **benchmarks.yml:**
  - The three dispatch inputs become `profile` (smoke | standard | full | nightly, default nightly).
  - Its summary prints `.workload .status .label` and `.summary[]` for v3 files, and the old lines for anything else.
  - The stale RFC 023 comment goes.
- **Criterion:** every micro-bench entry point runs `cargo bench --locked -p truffle-core` with an absolute, repo-root `CRITERION_HOME`: `CRITERION_HOME="$PWD/target/criterion"` from the repo root (`package.json`), or `{{justfile_directory()}}/target/criterion` (justfile). Cargo runs bench binaries with the package root (`crates/truffle-core`) as the working directory, so a relative `target/criterion` would land in `crates/truffle-core/target/criterion`, which nothing cleans. `bench-clean` / `clean:bench` remove only the repo-relative `target/criterion` and `target/bench-results`, never a `cargo metadata` path, because the target dir may be shared.
- **run-all:** `scripts/run-all-tests-and-benches.sh` becomes a thin wrapper that runs:
  1. build (excluding the app)
  2. unit tests
  3. real-network tests
  4. criterion
  5. `suite --profile standard --pair`

  It has no hand-rolled JSON greps.

### 9.3 Fixes carried over
- The scenario print bug.
- File-transfer failures now write a report.
- Transport params now include mode, port and app_id.
- Convergence timing (§8.2).
- 200 ms sleeps replaced by readiness checks.
- The `check-for-secrets.sh` allowlist path (the line does not match the pattern, so the entry is dropped).

### 9.4 Docs
- **New:** `benchmark/README.md`.
  - Absorbs the methodology from `docs/tailscale-performance.md`; that file keeps a pointer plus the tsnet-bench baseline section.
  - Links `examples/quic-streams` `bench`, `crates/truffle-core/benches`, `packages/sidecar-slim/cmd/tsnet-bench` and this RFC.
- **`docs/TESTING.md`:** the bench section, lines 18 and 180, the 219 filename, and the 259-262 no-key behaviour.
- **RFC status notes:** RFC 019 (§10.2 superseded) and RFC 020 (§3/§6.2 superseded).
- **README.md:** package table and dev commands.
- **`.github/pull_request_template.md`.**
- **`CLAUDE.md`:** layout table and CI excludes.
- **`.env.example`:** documents `TRUFFLE_BENCH_AUTH_KEY`.
  - Placeholders never look like keys: use `tskey-auth-…`.

### 9.5 Repo plumbing
§4 (workspace, excludes, pnpm, lint globs, release-please, .gitignore).

### 9.6 Local artefacts: do not delete
Implementers never remove untracked or ignored files outside `benchmark/`. The following are the user's to decide on:

| Artefact | Note |
|---|---|
| repo-root `sidecar-slim-linux` and `test-sidecar` | Inputs to `crates/truffle-core/examples/transport_peer_test.rs`'s EC2 procedure |
| `packages/sidecar-slim/sidecar-slim{,-linux}` | |
| `crates/truffle-napi/*.node` | |
| `.build` | |
| `~/.config/truffle/bin/sidecar-slim` | |
| `crates/truffle-core/tests/test-sidecar` | Current and required |
| `target/bench-results/*.json` | **Copied, not moved**, into the engine's v1 fixtures |

## 10. CLI

```
truffle-bench serve  [--device-name N] [--state-dir D] [--data-dir D] [--auth-key-file F] [--ephemeral]
                     [--allow-other-logins] [--allow-tagged] [--profile P]
truffle-bench peers  [node flags]                    # waits 5 s for discovery
truffle-bench run    --peer <ref> <workload> [--param k=v]… [--params-json '{…}'] [--out-dir D] [node flags]
truffle-bench pair   <workload> [--param k=v]… [--params-json …] [--out-dir D] [--allow-debug]
truffle-bench suite  --profile smoke|standard|full|nightly (--pair | --peer <ref>) [--param w.k=v]…
                     [--out-dir D] [--allow-debug] [--hub-profile P]
truffle-bench report <file|dir|zip>…                 # print v1/v2/v3 summaries
truffle-bench transport …                            # legacy, unchanged (tsnet-bench compatible)
```

**Auth key lookup**, in order:
1. `--auth-key-file`, first line
2. `TRUFFLE_BENCH_AUTH_KEY`
3. `TRUFFLE_TEST_AUTHKEY`
4. `TS_AUTHKEY`, with a warning

**Key handling.**
- Keys are never accepted as argv, written to reports, logged, or persisted.
- With no key, `serve`, `peers` and `run` print the login URL.
- `pair` and `suite --pair` require a key.
- `dotenvy` loads `.env` from the current directory upward.

**`pair`.**
- Starts two ephemeral hubs in one process with the per-run app id `truffle-benchpair-<6 hex>`, so concurrent pairs never see each other. (Discovery matches the hostname prefix `truffle-<app_id>-`, so an id starting `truffle-bench-` would make pair nodes peers of every real hub.)
- Each hub has its own temp `state_dir` and `data_dir`, deleted on exit.
- Topology is `same-process`.
- Each hub waits up to 60 s for `node.peer(<sibling>)`, and both exchange `hello` before the first workload.

**`suite`.**
- Writes exactly one v3 report per item and nothing else into `--out-dir` (default `target/bench-results`).
- Exits 0 only if every item is `ok`.

**Output.** Debug builds print a banner. `--param` values parse as JSON when possible, otherwise as strings, and are merged over the defaults.

## 11. The app

### 11.1 Shell and process
**Identity and window.**
- Tauri v2, identifier `com.vibecook.truffle-bench`, product name "truffle bench". v1 targets macOS 13+ (arm64 and x86_64); Windows and Linux are best effort.
- **The window is created in Rust** (`WebviewWindowBuilder`, label `main`; `app.windows` is `[]` in the config):
  - `title_bar_style(Overlay)`, `hidden_title(true)`, `traffic_light_position(16, 13)`
  - 1360 × 900, minimum 960 × 640
  - `visible(false)`, with the background colour of the current theme
  - The UI calls `ui_ready` after its first themed paint, and Rust then shows the window.
- **Navigation:**
  - `on_navigation` allows only the app origin.
  - `on_new_window` denies, and forwards https URLs to the opener.
  - All external URLs go through `open_url`.
- **CSP:** `default-src 'self'; img-src 'self' data:; style-src 'self' 'unsafe-inline'; font-src 'self'; connect-src ipc: http://ipc.localhost`.
- **Title strip:** a 40 px `<header data-tauri-drag-region>`. Its title span carries the attribute too. `-webkit-app-region` does nothing in Tauri.
- **Capabilities:** `capabilities/default.json` = `{ identifier: "default", windows: ["main"], permissions: ["core:default", "core:window:allow-start-dragging"] }`. App commands need no entries. `build.rs` declares no app manifest.

**Lifecycle.**
- Closing the window hides it (`CloseRequested` → `prevent_close` + `hide`), and clicking the Dock icon shows it again (`RunEvent::Reopen`). The responder keeps answering peers until ⌘Q.
- Quit (`ExitRequested`) calls `prevent_exit`, cancels active runs, awaits `hub.stop()` for at most 5 s, then calls `exit(0)`.
- **macOS process policy:**
  - `Info.plist` sets `NSAppSleepDisabled = true` and `NSLocalNetworkUsageDescription` ("truffle bench measures the direct path to your other devices on this network.").
  - While any run is active in either role, the app holds an `NSProcessInfo` activity (UserInitiated | LatencyCritical | IdleSystemSleepDisabled) through `objc2-foundation`, which is already in `Cargo.lock`.
  - `app_foreground_pct` is recorded.

**Profiles and data.**
- The profile comes from `--profile <name>` (argv) or `TRUFFLE_BENCH_PROFILE`. Debug builds default to the profile `dev`, so `tauri dev` never shares state with the installed app.
- A second instance on one Mac: `open -n -a "truffle bench" --args --profile b`.
- `TRUFFLE_BENCH_AUTH_KEY` (dev and test only) signs in without a browser.
- The state lock (§5.4) reports a held profile as: "Another truffle bench is using this profile. Quit it, or start this one with --profile <name>."

**Progress.**
- Live progress uses a `tauri::ipc::Channel` per call.
- Hub events go out as `bench://event` (`HubEvent`).
- Runs and suites execute in Rust, so a webview reload loses nothing: the UI calls `runs_active` and `run_attach` on mount.

**Theme.** `settings_set` applies the theme natively with `window.set_theme`. The UI sets or removes `<html data-theme>`, and charts re-read tokens when the theme changes.

### 11.2 Commands

JS calls `invoke(cmd, { req })`. Commands that stream also take `onProgress` (camelCase), a `Channel` from `@tauri-apps/api/core`. Every command returns `Result<T, AppError>`.

| Command | `req` | Returns |
|---|---|---|
| `app_info` | — | `AppInfo { app_version, truffle_version, data_dir, profile, os, build_profile, sidecar_path: Option<String>, sidecar_sha256: Option<String> }` |
| `ui_ready` | — | `()` (shows the window) |
| `hub_state` | — | `NodeState` |
| `hub_start` | `{ device_name?: string, auth_key?: string }` | `NodeState` (returns at once; spawns `Hub::start`) |
| `hub_stop` | — | `NodeState` |
| `hub_sign_out` | — | `NodeState` |
| `open_url` | `{ url }` | `()`, https only, via `open` |
| `open_data_dir` | — | `()` |
| `peers_list` | — | `BenchPeer[]` |
| `peer_probe` | `{ peer }` | `PeerProbe` |
| `bench_run` | `{ run_id, peer, params }` + `onProgress: Channel<Progress>` | `RunReport` (authoritative; Progress that arrives after it for that run is ignored) |
| `bench_cancel` | `{ id }` (run_id or suite_id) | `bool` |
| `runs_active` | — | `ActiveRun[]` |
| `run_attach` | `{ run_id }` + `onProgress` | `bool` |
| `suite_profiles` | — | `SuiteProfile[] { id, label, description, items: WorkloadParams[] }` |
| `suite_run` | `{ suite_id, profile, peer }` + `onProgress: Channel<SuiteProgress>` | `SuiteResult` |
| `workload_defaults` | — | `WorkloadParams[]` |
| `runs_list` | — | `RunSummary[] { run_id, suite_id, workload, label, status, error_code, started_at, peer_name, path_label, topology, build_profile, compare_key, summary, overlapping_runs, tool }` |
| `run_get` | `{ run_id }` | `RunReport` |
| `run_delete` | `{ run_id }` | `()` |
| `runs_import` | `{ paths: string[] }` | `ImportResult { imported, skipped }` (History accepts OS file drops via `onDragDropEvent`) |
| `runs_import_pick` | — | `ImportResult` (native picker through `tauri-plugin-dialog`'s Rust API) |
| `settings_get` / `settings_set` | `Settings { device_name, allow_other_logins, theme: "system" \| "light" \| "dark", target_peer: Option<String> /* device_id, else tailscale_id */ }` | `Settings` |

### 11.3 Journey and views

**Getting the app onto two machines (§11.6).**
- Two Macs, signed in with the same account.
- A machine without the app (Linux, EC2, Windows) joins as a responder with `truffle-bench serve`.

**Sign in.** This is the full window until the node runs.
- **At launch:**
  - With a saved state dir, the app starts on its own and shows "Connecting as <device>".
  - The sign-in form appears only on `needs-login`.
- **Form:** a device name field and the primary button "Sign in with Tailscale".
- **`starting` → `needs-login`:**
  - shows the URL with Open and Copy buttons
  - a 5:00 countdown
  - "This adds a device called <tailscale_hostname> to your tailnet."
  - after 30 s with the URL opened: "If your tailnet requires device approval, approve <tailscale_hostname> in the admin console", with an "Open admin console" button (the fallback for when no approval event arrives: hubs on cores before `NodeBuilder::on_network_status` dropped it)
- **`needs-approval`** (every sidecar sends `tsnet:needsApproval`, since RFC 008 phase 2; core now forwards it as `NetworkStatusEvent::NeedsApproval` through `NodeBuilder::on_network_status`, where it used to drop it):
  - "Approve this device": "Your tailnet requires device approval. Approve <tailscale_hostname> in the Tailscale admin console, under Machines."
  - primary button "Open admin console" (`open_url` `https://login.tailscale.com/admin/machines`)
  - the time waited; the node continues on its own once approved (core's start fails after 5 minutes with an error that names the approval)
  - "An auth key created as pre-approved skips this step", and Cancel
- **`error`:** the error message and "Try again".
- **"Use an auth key"** disclosure: "A key makes this a tagged device. It only matches peers that also use a key, unless both accept other accounts."

**Shell.**
- Title strip.
- Rail (Home's rail: 232 px; compact 60 px, icons only):
  - **Overview**, **Peers**
  - *Benchmarks* group: Path, TCP, UDP, QUIC, Messaging, File transfer, SyncedStore, HTTP, Video, Egress, Planned
  - **Suite**, **History**, **Settings**
- Rail foot: the tailnet state.
- While a `RemoteRun` is prepared, the title strip and rail foot show "Being measured by <peer> · <workload>", and every Run button is disabled with that reason.
- A debug build shows a quiet notice: "Debug build: numbers are not comparable with release builds".

**Overview.**
- **This device:** login, tailnet, IP, DNS name, health.
- **Target strip:** node ■ ——— `JY.Measure` (RTT, path label) ——— node ■, with the peer's name and OS.
- **Latest result** per workload, as fact cards with a sparkline of the last ten runs that share its `compare_key`.
- **Primary button:** "Run the smoke suite".
- **Relayed path:** "This path is relayed through DERP <region>. If both machines share a network, check System Settings → Privacy & Security → Local Network → truffle bench."

**Peers.**
- Header: "Tailnet <tailnet> · signed in as <login>".
- **Rows**, for every same-app peer:
  - status (Home's `.status`: square plus label)
  - name, OS, path, RTT
  - app version and build profile
  - same-host flag
  - whether it accepts runs from you
- **Filtering:**
  - Other-account peers are hidden unless "Show other accounts" is on. This is a local view filter, separate from Settings → "Accept runs from other accounts", which is persisted and off by default.
  - The count of hidden peers is shown.
- **Empty state:** "No other truffle bench is signed in to <tailnet> as <login>. Open truffle bench on another machine and sign in with the same account, or run `truffle-bench serve` there."
- **Target:**
  - When exactly one same-account peer is online, it becomes the target automatically.
  - The target is remembered.
  - Run is disabled, with the reason stated, when the target is offline, on a different protocol, lacks the workload, does not accept runs from you, or is busy.

**Benchmark views.** One per workload, all sharing one frame:
- **Top bar:** the target and the path.
- **Left pane:** parameters (presets plus the §7.3 fields with bounds) and the single primary button "Run" (Cancel while running).
- **Centre:**
  - the live KPI row
  - the main chart
  - a secondary chart: CDF, loss, the video frame strip, or the file-transfer phases
  - the path timeline
- **Right detail pane** (surface type):
  - what is measured
  - this workload's data-path hops
  - notes
  - the distribution table
  - resources
- **A failed run** keeps its partial series and shows the error in a `.notice.error`, with the phase it failed in and "Run again" / "Open report".
- **Holding ⌥** shows the construction layer.

**Suite.**
- A profile picker and a matrix of items with status.
- "Run the suite" runs it in Rust and survives a reload.
- Results are grouped by `suite_id`.

**History.**
- Lists every run: local, CLI-imported and CI-imported.
- Columns: status, workload, label, peer, path label, topology, headline value, date. Plus flags for debug, overlap and rate not achieved.
- **Run detail:** summary, charts, params, environments and notes.
- **Compare:** pick two runs. Compare warns and lists the differing fields when the `compare_key`s differ.
- **Import:** "Import reports" (picker) or a drop.
- **Delete.**

**Settings.**
- device name
- accept runs from other accounts
- theme
- data folder
- sign out (LocalAPI logout, then stop, then delete the state)
- about:
  - versions, sidecar path and SHA
  - the JY lockup
  - licences

**Planned.** One card per item in §8.6, drawn with a dashed outline, each with its reason.

### 11.4 Identity rules

**Colour.**
- Ground, ink, ink-2, ink-3. Cards are ground-raised with a rule edge. There is no accent colour.
- `blueline` is for construction only, `blueline-ink` for construction annotation text and focus, and `blueline-wash` for selection and measured bands.
- `redline` is for errors, loss and failed points, and nothing else.

**Type.**
- Jost for words. DM Mono (`.spec`, `.annotation`, `.code`) only for numbers, specs and code.
- Sentence case. `.label` is the only all-caps style.

**Shape and motion.**
- Square corners; round only for circles.
- 1 px hairlines; 2 px only for the active nav bar.
- No shadows except the focus ring on every focusable control.
- Motion: 120 ms for hover, 240 ms for panels, the settle easing for arrivals. Under reduced motion, nothing moves.

**State.**
- State is drawn with Home's `.status`: a square plus an uppercase label (LIVE, WAITING, RUNNING, PLANNED, FAILED, OFFLINE).
- Filled = live/done, outlined = waiting, half = running, dashed = planned, redline = failed (always with its label), and Home's flat line = offline.

**Charts, surface (always drawn).**
- The plot area is ground-raised with a 1 px rule edge.
- Gridlines are 1 px `rule`; the axis is 1 px `rule-strong`.
- Ticks are ink-3 `.annotation`.
- Series are ink, ink-2 and ink-3, told apart by stroke: solid 1.5, dashed 5 4, dotted 2 4.
- A previous run or baseline is ink-3 dashed with outlined square markers; this run's markers are filled squares. The circle and triangle are never used as markers, because they are the mark.
- Measured bands are blueline-wash.
- Loss and failure points are redline.

**Charts, construction (only while ⌥ is held; fades in over 240 ms).**
- A blueline grid, `JY.Measure` dimension lines, and p50/p99 guides labelled in blueline-ink `.annotation`.
- Every number a guide shows also appears on the surface (KPI row and table).

**⌥ handling.** The ⌥ state is one app-level flag, handled as in Home's `App.jsx:240-289`:
- ignored while typing, and reset on blur
- applied as `.construction` on the root
- announced through an aria-live region

`JY.Blueprint` is used with `hotkey={false}` and `show={construction}`.

**Copy.**
- Plain and first person where the app speaks; numbers with units; verbs on buttons.
- No emoji and no hype words. Typographic quotes.

**Mark and icons.**
- The mark comes only from `JY.Mark` (16 px row in the rail; `layout="micro"` in the compact rail) or the vendored SVGs. It is never redrawn.
- The product name is set in lowercase Jost 500: "truffle bench".
- New interface icons (play, stop, refresh, peer, clock, signal, sync, video, file, globe) follow the brand icon spec: 24 grid, 1.5 stroke, square caps, mitred joins, currentColor.

**Peer-supplied text.** Peer and report strings are rendered as text only. There is no `dangerouslySetInnerHTML`.

### 11.5 Vendored brand files

`src/brand/` mirrors `career/brand` folder shapes, so the `../fonts/` URLs inside `tokens.css` resolve. Everything is copied unedited:
- `tokens/tokens.css`, `tokens/tokens.json`
- `fonts/*.woff2`, `Jost-OFL.txt`, `DMMono-OFL.txt`
- `components/bundle.js`, `bundle.css`, `index.d.ts`
- `icons/*.svg`
- `logos/jy-mark-{ink,paper}.svg`, `jy-mark-wrap-{ink,paper}.svg`, `jy-lockup-inline-{ink,paper}.svg`
- `VERSION` (source date 2026-09-30, tokens v1)
- `NOTICE`: the JY mark and logos are James Yong's, all rights reserved, and not covered by the repo's MIT licence. Jost and DM Mono are OFL 1.1.

**Loading the JY bundle in `main.tsx`:**
1. `import './brand/react-global'` sets `window.React = React`.
2. Then `import './brand/components/bundle.js'` and `bundle.css`.
3. `jy.d.ts` types `window.JY`.

The bundle uses only `createElement` and basic hooks, so it is React 19-safe once the global is set.

Home's chrome selectors (rail, `.status`, controls, `.notice`, `.empty`, `.toast`) are re-implemented in `styles/` from `career/home/styles`.

**App icon.**
- A placeholder set is committed in `src-tauri/icons/`, generated with `tauri icon` from a 1024 px ink tile with the paper micro mark and a brand-blue dimension line beneath it.
- It is drawn from the vendored SVG geometry and differs from James Yong Home's icon.

### 11.6 Getting it onto two machines
- `app:build:universal` builds both darwin sidecars, `lipo`s them into `binaries/sidecar-slim-universal-apple-darwin`, and runs `tauri build --target universal-apple-darwin --config src-tauri/tauri.bundle.conf.json`.
- `bundle.macOS.signingIdentity` is `"-"` (ad-hoc signing).
- `benchmark/README.md` covers copying the `.dmg` to the second Mac and clearing quarantine. Either run `xattr -dr com.apple.quarantine "/Applications/truffle bench.app"`, or use System Settings → Privacy & Security → Open Anyway.

## 12. Verification

**Unit tests:**
- stats: nearest-rank validity thresholds, stddev, and RFC 3550 jitter with a constant clock offset (the result must not depend on the offset)
- params: bounds, deadlines, serde round-trips, `deny_unknown_fields`
- control message serde
- report normalisation from v1, Rust v2 and tsnet-bench fixtures
- the video frame model (mean equals offered over whole GOPs) and reassembly
- UDP loss, reorder and burst accounting, and bucket loss
- `resolve_sidecar_with` order
- `compare_key` stability
- `schema_is_current`

**Invariants asserted on every real-network run:**
- Σ buckets == bytes_total
- received ≤ sent per stream
- udp unique + missing == sent
- summary keys are non-null at defaults
- in same-process runs, |offset| ≤ uncertainty

**Live PR CI.** `benchmark/engine/tests/live_pair.rs` prints `[skip]` and passes when there is no key. With a key, it runs one `pair`:
- path 5
- tcp throughput 2 s
- ws rr 50
- file-transfer 1024 B × 12 (the pending-offer regression)
- synced-store pingpong 10

It asserts `ok` and 12 iterations. `integration-tests.yml` keeps `-p truffle-bench --tests`.

**Manual real-network checks** (key from `.env`):
1. `suite --profile smoke --pair`
2. `serve` in one process, plus `suite --profile smoke --peer …` in another
3. every workload `ok`

**App checks:**
- frontend `tsc` + `vite build` (fonts emitted) + vitest (fixtures)
- `cargo build -p truffle-bench-app`
- every view in a browser on the mock, in light and dark
- the real app with `TRUFFLE_BENCH_AUTH_KEY` and `--profile`, against a `serve` peer started with `--allow-tagged`

## 13. Open questions
- **App icon:** the placeholder is described in §11.5. James decides the final icon.
- **CI job:** whether to add a path-filtered macOS CI job that builds the app.
- **Core follow-ups, done in r4** (§14 #20–#24): `needsApproval` forwarded (`NetworkStatusEvent`), the sidecar proxy's upstream keep-alive pool, the late login (sidecar protocol 8), session restart handling and shared dials, the ~41 ms per new connection (LocalAPI `OmitAuth`) and the 30 s TIME_WAIT dial stall.
- **Core follow-ups, still open:**
  - Layer 3's event processor drops sidecar events on `Lagged` and never resyncs (a 256-slot channel; ~75–95 events lost at start with ~10 peers), so a node can learn of a peer ~30 s late (seen in both builds in the restart scenario; §14 #24 makes the run wait instead of fail);
  - expose the sidecar protocol version (the engine cannot tell an old sidecar, so it waits 5 s on core before its LocalAPI login fallback);
  - TSMP/ICMP ping;
  - the ~5 s first WebSocket dial when both hubs dial at once (crossing WireGuard handshakes; only the lower id should dial at once);
  - `ft.done` ~100 ms late (the sleep in `file_transfer/receiver.rs`) and ~28 ms `negotiate_ms`;
  - the remaining ~1 s stall on a TIME_WAIT collision: 0.8–0.9 % of new connections at ~410–450 per second, each exactly one 1.00 s SYN retransmit (three HTTP new-connection runs, same host, 2026-10-16). About 450 4-tuples sit in a 1 s TIME_WAIT over the 49,536-port source range, so the rate grows linearly with the connection rate (~2 % at 1k/s); at ~450/s the stalls are ~90 % of the mean request time. Removable by accepting through a gVisor listener;
  - `on_network_status` in the NAPI and Tauri bindings;
  - a per-instance id in the session hello (exact restart detection instead of the standby heuristic).

## 14. Amendments found while building (r3, r4)

| # | Amendment | Why |
|---|---|---|
| 1 | `pair` app id is `truffle-benchpair-<6 hex>`. | `truffle-bench-p…` matched the `truffle-truffle-bench-` hostname prefix, so pair nodes were discovered by real hubs (seen live). |
| 2 | `suite` takes `--hub-profile` for its own hub profile. | `--profile` already names the suite profile. |
| 3 | Control sends retry `ConnectFailed("connection already in progress")` every 250 ms until the request deadline. | Simultaneous hellos from both hubs collide on the lazy WebSocket dial. |
| 4 | `WorkloadOutput` carries `goodput_bytes`; responders report their measurement window with `ResponderCtx::set_resource_window`. | Resources need the window and byte count on both sides without changing the `ResponderRun` trait. |
| 5 | The app's dev server is `127.0.0.1:47183` (strict). | 5173 and 5183 were both held by other projects' Vite servers on the build machine; a strict port on an explicit host fails loudly instead of loading the wrong app. |
| 6 | `BuildProfile` also has `unknown` (imported v1 reports). | v1 recorded no build profile. |
| 7 | `TransferTimings.transfer_secs` starts at the 72-byte header written just before the payload; a ~40 ms floor exists for small files and is noted in reports. | Measured live; predates the timing change. |
| 8 | `dataplane::Pacer` is a GCRA (virtual schedule) pacer: depth `max(1, rate × 2 ms)`, bounded catch-up. | A token bucket woken on 1 ms ticks under-delivered on macOS timer slack (9.55 of 10 Mb/s upload, 6.91 download; 370 of 500 pps). |
| 9 | Quit is handled at `RunEvent::Exit` as well as `ExitRequested`; the window also shows 600 ms after page load if `ui_ready` never arrives. | tao 0.34 sends ⌘Q straight to `applicationWillTerminate`; WebKit may suspend rAF in a never-shown window. |
| 10 | `tauri-plugin-dialog` is pinned `~2.7.3`. | 2.8 requires tauri 2.12; the workspace locks 2.10.3. |
| 11 | Control sends also retry `SendFailed` every 250 ms until the request deadline (§6). | The peer kept its WebSocket to an earlier instance of this node that reused the device name, and dropped the new one ("connection missing after connect") until its 10 s ping cleared the dead one. |
| 12 | `hello` is re-sent every 5 s (`HELLO_RESEND`) within its 30 s timeout until answered. | `hello` is idempotent, and a message written into a WebSocket that died with a restarted peer is lost without an error (seen live: "no hello.ack within 30 s"); the re-send goes out on the redialled connection. |
| 13 | `pair` says hello one direction at a time (initiator → responder, then back), each up to 3 attempts 1 s apart. | Both hubs' hello caches are already dialling each other at that moment, so two more simultaneous dials only add collisions; a stuck "connection already in progress" dial can outlast one attempt's deadline. |
| 14 | `path_label` (§7.4) uses the **relayed** share (DERP plus peer relay) for the 1 % / 99 % thresholds; `derp_share` stays DERP only. With fewer than `PATH_BYTES_MIN` (256 KiB) of usermetrics bytes, or without usermetrics, the label comes from the ping samples (`before`, the timeline, `after`) with a "path label from ping samples; …" note and `derp_share: null`. `unknown` means only that no sample named a path. | A peer-relayed run is not direct. Tiny runs move mostly pings and control traffic: a same-host `path` run with 148 B of DERP delta was labelled `relay` or `mixed` while every ping said direct at 0.27 ms. |
| 15 | Hello re-sends with the same id every 5 s inside its 30 s deadline; sends also retry `SendFailed`; `Pair::start` exchanges hellos one direction at a time. | A peer that restarted keeps a stale WebSocket on the other side until its 10 s ping clears it; reused device names hit the same race. |
| 16 | When the sidecar's start event carries no login, the hub reads it from LocalAPI `/localapi/v0/status` (`User[Self.UserID].LoginName`) and re-admits peers when it arrives. | Fast restarts from saved state reported no login, so every same-account peer was refused. Core fixed it in r4 (#21). |
| 17 | Below 256 KiB of usermetrics delta, `path_label` uses the ping samples and `derp_share` is null. | A few hundred stray bytes decided the label of short runs. |
| 18 | `Hub::set_report_hook` runs before a report is saved; `BenchPeer.same_host`, `RunSummary.flags` and `ActiveRun.suite_*` were added. | The app sets `app_foreground_pct` without rewriting files and stops guessing flags. |
| 19 | Per-mode fixtures (`report-<mode>.json`, 19 modes) plus an app registry contract test. | One fixture per workload could not catch drift in mode-specific results. |
| 20 | `NodeState.state` gains `needs-approval` (§7.1). The hub enters it from `NodeBuilder::on_network_status` (`NetworkStatusEvent::NeedsApproval`) while `build()` waits, never over `running` or `stopped`; the app shows the approval step (§11.3), the CLI prints the admin console link. | Core now forwards the `tsnet:needsApproval` event that sidecars have always sent (it used to drop it); before, sign-in could only hint at approval after 30 s, which stays as the fallback when no approval event arrives. |
| 21 | Amendments 3, 11, 12, 13, 15 and 16 stay, as defence in depth for peers on older cores. Core now shares one bounded dial per peer among its callers, installs a re-dialled connection from a restarted peer on the accepting side, and re-sends the login once the netmap names it (sidecar protocol 8, `NetworkStatusEvent::LoginKnown`). The hub waits for that event for 5 s before it asks the LocalAPI, and a send whose dial core cancelled at shutdown is not retried. | The workarounds treated core bugs that are now fixed at the root; against this core they fire only on a genuine failure. |
| 22 | The http proxy keep-alive note follows what the peer's server saw: "the sidecar's proxy reconnected upstream: N upstream connections for W keep-alive workers …" when N > W; without server stats, the old note above 2 workers, now naming sidecars before protocol 8. | Protocol-8 sidecars keep 64 idle upstream connections per host (Go's default was 2), so the unconditional note became wrong. |
| 23 | `RunSummary.mode`; summary labels in one "RTT …" style; `schema/fixtures/live-keys.json` generated from each workload's declared live keys, and `live_pair` fails on an undeclared key; the path summary leads with `rtt_p50_ms`, then min, max and loss (§8.2). | The Benchmark view shows a run in the form's mode, and the app's live keys can no longer drift from the engine's. |
| 24 | Rule 1 waits before it refuses: when admission fails while the hub still looks for its own login, the responder waits up to 10 s (`OWN_LOGIN_WAIT`) for it. The initiator re-sends a `prepare` refused with `peer-not-found` every 1 s within its 30 s timeout. | The restart scenario on this core: a run reached `prepare` ~0.1 s before the restarted responder learned its login ("an unknown account … does not accept runs"), and a responder whose Layer 3 missed the initiator's join refused with "retry in a moment", which nothing did. |
