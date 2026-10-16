# Benchmarks

Every number truffle can produce is measured from here: one engine
(`truffle-bench`) that owns every two-node workload, a CLI and a desktop app
that both drive it, and one report format. The design contract is
[RFC 027](../docs/rfcs/027-benchmark-hub.md); this page is how to use it.

- [What lives here](#what-lives-here)
- [CLI quick start](#cli-quick-start)
- [App quick start](#app-quick-start)
- [Getting it onto two machines](#getting-it-onto-two-machines)
- [Workloads](#workloads)
- [Suites](#suites)
- [Methodology](#methodology)
- [Baselines ladder](#baselines-ladder)
- [Reports](#reports)
- [CI](#ci)
- [Working on the hub](#working-on-the-hub)
- [What we found so far](#what-we-found-so-far)
- [Planned](#planned)

## What lives here

| Path | What it is |
|---|---|
| [`engine/`](engine) | Crate `truffle-bench` (version 0.1.0, not published): the `truffle_bench` library and the `truffle-bench` CLI. Workloads, the peer control protocol, statistics, suites and the v3 report. |
| [`app/`](app) | The **truffle bench** desktop app. Frontend package `@vibecook/truffle-bench-app` (React + Vite, private); Tauri v2 crate `truffle-bench-app` in `app/src-tauri`. |
| [`schema/`](schema) | `report.v3.schema.json` and one fixture per type and variant, **generated** from the engine's Rust types. Do not edit by hand. |

Linked from here, but kept where they are:

| Path | What it is |
|---|---|
| [`crates/truffle-core/benches/`](../crates/truffle-core/benches) | Criterion micro benches: `envelope` (JSON codec encode/decode) and `chunk_hash` (SHA-256 via the `sha2` crate, i.e. the hashing ceiling for file transfer, not truffle code). |
| [`packages/sidecar-slim/cmd/tsnet-bench`](../packages/sidecar-slim/cmd/tsnet-bench) | Native Go/tsnet TCP baseline with no Rust bridge. Same wire format as the legacy `transport` subcommand. It shares the sidecar's `go.mod`, `go test` and govulncheck, so it stays there. |
| [`examples/quic-streams`](../examples/quic-streams) | `bench <peer> <port> [streams]`: an informal JS demo that times N concurrent QUIC streams. Prints to stdout; writes no report. |
| [`docs/tailscale-performance.md`](../docs/tailscale-performance.md) | Release-build commands and the tsnet-bench baseline procedure. |
| [`docs/TESTING.md`](../docs/TESTING.md) | Test setup, auth keys, CI secrets. |

## CLI quick start

Below, `truffle-bench` stands for `cargo run --locked -p truffle-bench --release --`
from the repo root, or the built binary at `<target-dir>/release/truffle-bench`
after `cargo build --locked -p truffle-bench --release`. Building needs Rust
stable, and Go for the dev sidecar (truffle-core's build script compiles it when Go
is installed; otherwise point `TRUFFLE_SIDECAR_PATH` at a sidecar binary).

| Command | What it does | Auth key |
|---|---|---|
| `serve` | Starts a hub that answers runs from peers until Ctrl-C. | Optional |
| `peers` | Lists same-app peers after waiting 5 s for discovery. | Optional |
| `run --peer <ref> <workload>` | Runs one workload against a peer on another machine (or another profile). | Optional |
| `pair <workload>` | Starts two ephemeral hubs in one process and runs one workload between them. | **Required** |
| `suite --profile <p> (--pair \| --peer <ref>)` | Runs a [suite](#suites) item by item. | **Required** with `--pair` |
| `report <file\|dir\|zip>…` | Prints summaries of v1, v2, v3 and tsnet-bench reports. | None |
| `transport …` | Legacy raw TCP/QUIC benchmark, unchanged; schema 2; byte-compatible with tsnet-bench. | Required: `TRUFFLE_TEST_AUTHKEY` or `TS_AUTHKEY` only |

```sh
# Machine B: answer runs. Without a key it prints a login URL; sign in with your account.
truffle-bench serve

# Machine A: find B, then measure.
truffle-bench peers
truffle-bench run --peer mac-b-cli path
truffle-bench run --peer mac-b-cli tcp --param direction=download --param streams=4
truffle-bench run --peer mac-b-cli udp --param mode=ramp
truffle-bench run --peer mac-b-cli video --params-json '{"transport":"quic","profile":"1080p60"}'
truffle-bench suite --profile smoke --peer mac-b-cli

# One machine, two hubs in one process (needs a key)
truffle-bench pair ws --param count=1000
truffle-bench suite --profile standard --pair
truffle-bench suite --profile full --pair --param tcp.duration_secs=5

# Read reports, including a downloaded CI artefact
truffle-bench report target/bench-results ~/Downloads/bench-results-123.zip
```

Shortcuts: `pnpm bench:suite` (standard profile, pair), `pnpm bench:pair <workload> …`,
`just bench-macro [profile]`, `just bench-one <workload> …`, `just bench-serve …`.

**Flags.**
- Node flags, shared by `serve`, `peers`, `run` and `suite --peer`: `--device-name`,
  `--state-dir`, `--data-dir`, `--auth-key-file`, `--ephemeral`, `--allow-other-logins`,
  `--allow-tagged`, `--sidecar`. The hub profile is `--profile` for `serve`, `peers` and
  `run`, and `--hub-profile` for `suite --peer` (there `--profile` is the suite profile).
  `pair` and `suite --pair` use only `--auth-key-file` and `--sidecar`.
- `--param k=v` parses `v` as JSON when it can, otherwise as a string, and merges it over
  the workload's defaults. `--params-json '{…}'` sets several at once. In `suite`,
  overrides are `--param <workload>.<field>=v`. Unknown fields and out-of-bounds values
  are rejected, never clamped.
- `--out-dir D` (default `target/bench-results`, relative to the current directory).
- `--allow-debug` lets `suite --profile nightly` run from a debug build (other profiles
  and `pair` run debug builds with a warning banner; `pair` accepts the flag and ignores
  it). See [comparability](#comparability).

**Auth keys.** Looked up in this order: `--auth-key-file` (first line),
`TRUFFLE_BENCH_AUTH_KEY`, `TRUFFLE_TEST_AUTHKEY`, `TS_AUTHKEY` (with a warning).
`.env` is loaded from the current directory upward. Keys are never accepted as a
command-line value, written to reports, logged or persisted. Without a key, `serve`,
`peers` and `run` print a login URL and wait; `pair` and `suite --pair` refuse to start.
A key from [docs/TESTING.md](../docs/TESTING.md#one-time-setup-for-real-network-tests)
(reusable, ephemeral, `tag:truffle-test`) works for everything. The legacy `transport`
subcommand is the exception to this order: it reads only `TRUFFLE_TEST_AUTHKEY`, then
`TS_AUTHKEY`, and has no `--auth-key-file`.

**Who may run what (admission).** A responder accepts runs from a peer when the peer
is signed in with the same login, or both are tagged devices (`tagged-devices`) and the
responder allows tagged peers (`allow_tagged`, on whenever it started with a key, or
with `--allow-tagged`), or the responder runs with `--allow-other-logins`. Any peer
gets an answer to `hello`; every other request from a peer that is not admitted is
refused with `login-refused`.

**Where things go.**

| | Default |
|---|---|
| Device name | `<hostname>-cli`, plus `-<profile>` with `--profile` |
| Data dir | `<OS data dir>/truffle-bench-cli` (macOS `~/Library/Application Support/truffle-bench-cli`, Linux `~/.local/share/truffle-bench-cli`, Windows `%APPDATA%\truffle-bench-cli`) |
| Node state | `<data dir>/state/<device name>`; one hub per state dir (a lock file), so a second hub on one machine needs `--profile` |
| Reports | `--out-dir` (default `target/bench-results/`), one `<timestamp>-<workload>-<run_id>.json` per run |
| Run history | `<data dir>/runs/<timestamp>-<workload>-<run_id>.json`: every `run` and `suite --peer` report is also kept here (`pair` and `suite --pair` use a temporary data dir). Nothing lists or prunes it; delete by hand or point `--data-dir` elsewhere. |
| Temporary files | `<data dir>/tmp/{in,out}-<run tag>/`, removed at the end of each run |

`serve`, `peers` and `run` register a normal (non-ephemeral) device on your
tailnet, so the next start reuses it; `--ephemeral` changes that. `pair` always
uses two ephemeral devices with temporary state and a per-run app id
(`truffle-benchpair-<6 hex>`), so concurrent pairs never see each other (an id
starting `truffle-bench-` would make pair nodes peers of every real hub, because
discovery matches the hostname prefix `truffle-<app id>-`). `suite` names its own hub's
profile with `--hub-profile`, because `--profile` is the suite profile.

`suite` writes exactly one report per item and nothing else into `--out-dir`, and
exits 0 only if every item is `ok`.

**Sidecar.** The engine finds the Go sidecar in this order: an explicit path,
`TRUFFLE_TEST_SIDECAR_PATH`, `TRUFFLE_SIDECAR_PATH`, the copy bundled next to the
packaged app's executable (app only), then the dev binary
`crates/truffle-core/tests/test-sidecar` that truffle-core's build script compiles
when Go is installed. Otherwise it fails and lists every path it tried. The CLI never
looks next to its own executable (the cargo target dir may be shared with other
projects) and never uses `~/.config/truffle/bin`. Reports record the sidecar's path
and SHA-256.

## App quick start

The app signs in to your Tailscale account, finds every other copy of itself (and
every `truffle-bench serve`) on your tailnet under the same account, runs any
workload against any of them with live charts, and keeps a history that also
imports CLI and CI reports. It targets macOS 13+ (Apple silicon and Intel); Windows
and Linux are best effort.

```sh
pnpm install
pnpm --filter @vibecook/truffle-bench-app app:dev               # Tauri dev build, profile "dev"
pnpm --filter @vibecook/truffle-bench-app app:build             # bundle for this machine
pnpm --filter @vibecook/truffle-bench-app app:build:universal   # macOS universal .app/.dmg
pnpm --filter @vibecook/truffle-bench-app dev                   # frontend only, in a browser, on the mock
pnpm --filter @vibecook/truffle-bench-app build                 # frontend build (what CI runs)
pnpm --filter @vibecook/truffle-bench-app test                  # vitest
```

Or `just app-dev` and `just app-build`.

- **Dev server: `http://127.0.0.1:47183`**, strict port (`vite.config.ts`, and
  `build.devUrl` in `tauri.conf.json`). Not Vite's usual 5173: other projects' dev
  servers held 5173 and 5183 on the build machine, and a strict port on `localhost`
  quietly binds `[::1]` instead, so the app could load the wrong page. On
  `127.0.0.1` a clash fails loudly. Change both files together if you move it.
- `app:dev` needs no sidecar file and no built frontend: it starts that Vite server
  and finds the dev sidecar like the CLI does. It is a **debug build**, so its
  numbers are flagged as not comparable with release builds.
- `dev` alone serves the frontend in a browser on a **mock** of the Rust side. Every
  mock report is built on the engine's own example of the same workload and mode
  (`schema/fixtures/report-<mode>.json`), so it has the engine's summary keys, series
  and results fields with simulated numbers; suite profiles are the engine's too.
  The mock takes URL switches for design work (`?theme=dark`, `?autorun=tcp`,
  `?fail=udp`, `?active_suite=smoke`, … listed at the top of `src/api/mock.ts`).
- **Real reports in the browser (dev only).** Copy report JSONs (from `suite
  --out-dir`, CI artefacts, the app's data folder) into `benchmark/app/dev-reports/`
  (gitignored; subfolders are fine) and open `http://127.0.0.1:47183/?real=1`:
  History, Run detail, Compare and Overview then show those reports instead of the
  mock's. The dev server reads the folder on each load (`/__dev-reports`); a build
  never sees it.
- `app:build` first runs `scripts/prepare-sidecar.mjs`, which builds the Go sidecar
  (`CGO_ENABLED=0 go build -ldflags "-s -w"`) into `src-tauri/binaries/` (gitignored),
  then `tauri build` with `src-tauri/tauri.bundle.conf.json`. The bundle lands in
  `<cargo target dir>/release/bundle/` (`target/` unless your cargo config redirects it).
- `app:build:universal` builds both darwin sidecars, `lipo`s them, and bundles for
  `universal-apple-darwin` (needs `rustup target add aarch64-apple-darwin x86_64-apple-darwin`).
  Output: `<cargo target dir>/universal-apple-darwin/release/bundle/`.

**Profiles.** `--profile <name>` (argv) or `TRUFFLE_BENCH_PROFILE` gives an
instance its own data dir (the app-data dir with `-<name>` appended), device name
and node state. Debug builds default to the profile `dev`, so `app:dev` never
shares state with an installed copy.

**Two instances on one Mac** (each its own device on the tailnet; runs between them
are `same-host`):

```sh
# development: the first instance runs Vite on 127.0.0.1:47183 …
pnpm --filter @vibecook/truffle-bench-app app:dev
# … and a second one is the same debug binary on another profile, using that server
# (a second app:dev would fail on the strict port)
cargo run --locked -p truffle-bench-app -- --profile b
TRUFFLE_BENCH_PROFILE=b cargo run --locked -p truffle-bench-app   # the same, by environment

# a bundled app
open -n -a "truffle bench" --args --profile b
```

Two instances on one profile are refused: "Another truffle bench is using this
profile. Quit it, or start this one with --profile <name>."

**Bundles.** From `benchmark/app`:

```sh
node scripts/prepare-sidecar.mjs                                                       # the Go sidecar into src-tauri/binaries/
node scripts/prepare-sidecar.mjs --tauri build --debug --config src-tauri/tauri.bundle.conf.json   # debug .app + .dmg (fast; debug numbers)
pnpm app:build                                                                         # release: sidecar, git provenance, bundle
pnpm app:build:universal                                                               # release, arm64 + x86_64
```

The debug bundle lands in `<cargo target dir>/debug/bundle/{macos,dmg}/`, the release
ones in `<cargo target dir>/release/bundle/` (or `universal-apple-darwin/release/`).
`--tauri <args…>` builds the sidecar, then runs the app's pinned Tauri CLI
(`@tauri-apps/cli ~2.10`, matching the locked tauri; a global `cargo tauri` of another
minor refuses to build) with `TRUFFLE_BENCH_GIT_SHA`/`_DIRTY` set. `app:build` goes
through it too; a bundle built with plain `pnpm tauri build` records no git sha.

**Self-test (debug builds only).** `TRUFFLE_BENCH_SELFTEST=<peer>` makes a debug app
run `path` against `<peer>` (a device name, tailscale id or hostname) as soon as its
hub is running and the peer has said hello, through the same code path as the Run
button, and log each report (status, summary, `app_foreground_pct`, where it was
stored). `TRUFFLE_BENCH_SELFTEST_PARAMS` replaces `path` with other params: one JSON
object or an array run one after another. It is compiled out of release builds.

```sh
# a responder to measure against
truffle-bench serve --allow-tagged --ephemeral --device-name selftest-serve &
# a debug bundle, signed in with a key, on its own profile
TRUFFLE_BENCH_PROFILE=selftest TRUFFLE_BENCH_AUTH_KEY=… TRUFFLE_BENCH_SELFTEST=selftest-serve \
TRUFFLE_BENCH_SELFTEST_PARAMS='[{"workload":"path"},{"workload":"tcp","duration_secs":3}]' \
  "<cargo target dir>/debug/bundle/macos/truffle bench.app/Contents/MacOS/truffle-bench-app"
```

**Signing in.** "Sign in with Tailscale" shows the login URL (Open, Copy) and adds
a device named after the machine to your tailnet. If your tailnet requires device
approval, approve it in the admin console. For tests, `TRUFFLE_BENCH_AUTH_KEY` in
the app's environment signs in without a browser (dev and test only); a tagged key
makes a tagged device, which pairs with a `truffle-bench serve` that also uses a key.

**Living with it.** Closing the window hides it; the app keeps answering peers
until you quit (⌘Q). While a peer measures this machine, the title strip says
"Being measured by <peer>" and local runs are disabled. Settings holds the device
name, "Accept runs from other accounts" (off by default), theme, data folder and
sign-out. History lists local, CLI and CI runs, compares two runs, and imports a
file, a folder or a `.zip` (also by drag and drop).

## Getting it onto two machines

**Two Macs.**
1. On one Mac: `pnpm install`, then `pnpm --filter @vibecook/truffle-bench-app app:build:universal`.
   The app is ad-hoc signed (`signingIdentity "-"`), not notarized.
2. Copy the `.dmg` to the other Mac (AirDrop, a share, `scp`) and drag
   **truffle bench** into Applications on both.
3. Clear the quarantine flag on the copy, either with
   `xattr -dr com.apple.quarantine "/Applications/truffle bench.app"`, or by opening it
   once and then choosing System Settings → Privacy & Security → **Open Anyway**.
4. Open it on both and sign in with the **same** Tailscale account. Each lists the
   other under Peers; when exactly one same-account peer is online it becomes the
   target.
5. Allow Local Network access when macOS asks. Without it the direct path cannot
   form and every run is relayed through DERP (Overview says so; fix it in System
   Settings → Privacy & Security → Local Network → truffle bench).

**Machines without the app** (Linux, Windows, an EC2 instance, a second shell on this
Mac) join as responders with the CLI:

```sh
cargo build --locked -p truffle-bench --release      # needs Rust and Go
truffle-bench serve                                  # open the printed URL in any browser
```

Signed in with your account, it appears in the app's Peers list like another
copy of the app. A `serve` started with a key is a tagged device: to accept runs
from your signed-in app it needs `--allow-other-logins`.

## Workloads

Every workload has defaults for every parameter; the bounds are in RFC 027 §7.3.
The last column lists every `summary` key each mode reports (a failed run may have
none); `schema/fixtures/report-<mode>.json` holds a generated example of each.

| Workload | Variants (params) | Measures | Summary keys |
|---|---|---|---|
| `path` | `count`, `interval_ms` | Disco ping RTT and which path answered; start-up costs | `rtt_min_ms`, `rtt_p50_ms`, `rtt_max_ms`, `loss_pct` (results also hold `first_rtt_ms`, `hub_start_ms`, `discovery_ms`, `ws_connect_ms`) |
| `tcp` | `mode` throughput \| latency; `direction` upload \| download \| bidirectional; `streams`; `nodelay` | Raw TCP over `open_tcp` / `listen_tcp` | throughput: `goodput_mbps`, `connect_ms`, `cpu_ms_per_mb`, `loaded_rtt_p50_ms`, plus `upload_mbps`, `download_mbps` when bidirectional; latency: `rtt_p50_ms`, `rtt_p99_ms`, `rtt_max_ms` |
| `udp` | `mode` stream \| echo \| ramp; `direction`; `payload_bytes`; `rate_mbps`; `pps` | Datagrams over `bind_udp`: paced load, loss, reorder, jitter | stream: `goodput_mbps`, `offered_mbps`, `loss_pct`, `jitter_ms`, `cpu_ms_per_mb`; echo: `rtt_p50_ms`, `rtt_p99_ms`, `loss_pct`; ramp: `max_sustainable_mbps`, `offered_mbps`, `loss_pct` |
| `quic` | as tcp, `streams` default 4 | QUIC over `connect_quic`, one stream per worker | throughput: as tcp, plus a flow-control ceiling note; latency: `rtt_p50_ms`, `rtt_p99_ms`, `rtt_max_ms`, `connect_ms` |
| `ws` | `mode` rr \| stream; `payload_bytes`; `count`; `concurrency`; `rate_per_sec` | Envelope messages over the mesh WebSocket (replaces the old `request-reply`) | rr: `rtt_p50_ms`, `rtt_p99_ms`, `msgs_per_sec`; stream: `msgs_per_sec`, `goodput_mbps`, `drop_pct` |
| `file-transfer` | `size_bytes`, `iterations` | `send_file` end to end and on the wire | `wire_mbps`, `e2e_mbps`, `hash_ms`, `negotiate_ms` |
| `synced-store` | `mode` pingpong \| burst; `count`; `payload_bytes` | SyncedStore round trips and coalescing | pingpong: `convergence_p50_ms`, `convergence_p99_ms` (RTT/2 estimate); burst: `final_echo_ms`, `coalescing_pct`, `burst_write_ms` |
| `http` | `target` proxy \| direct; `connection` keepalive \| new; `concurrency`; `response_bytes` | HTTP/1.1 through the sidecar reverse proxy (RFC 023) or a direct listener | `req_per_sec`, `latency_p50_ms`, `latency_p99_ms`, `error_pct` |
| `video` | `transport` udp \| quic \| tcp; `profile`; `bitrate_mbps`; `fps`; `gop`; `pacing`; `jitter_buffer_ms` | A synthetic, exact frame stream (I and P frames, paced) | `delivered_fps`, `late_pct`, `dropped_pct`, `freezes`, `frame_latency_p50_ms` (± uncertainty), `goodput_mbps` |
| `egress` | `direction`, `streams` | TCP through an egress route (RFC 026); needs sidecar protocol ≥ 7 | `goodput_mbps` (same accounting as tcp), `cpu_ms_per_mb`, `loaded_rtt_p50_ms` |

Notes that matter when reading the numbers:
- **tcp** crosses two Rust bridges (initiator and responder); **egress** crosses one.
  `egress − tcp` is the cost of one bridge. Nagle exists only on the two Rust loopback
  sockets, so the `nodelay` A/B only shows with large payloads (32 KiB in `full`).
- **udp** payloads above 1252 B fragment in the netstack; datagrams are IPv4 only;
  loss is end to end, including loopback relay drops. When the sender cannot sustain
  the target rate the run says so and History leaves it out of trends.
- **quic** uses about 1200 B packets (MTU discovery is off), no GSO, and double
  encryption (QUIC inside WireGuard). Near quinn's default flow-control ceiling the
  run notes "bounded by quinn default flow-control windows".
- **ws** loses nothing on the wire; every drop happens at a receiver (`lag_dropped`
  vs `router_dropped`).
- **file-transfer** `hash_ms` hashes page-cached data; `wire_mbps` times first payload
  write to ACK. The summary is the median over iterations, without iteration 1 when
  there are three or more.
- **http** proxy keepalive above concurrency 2 includes upstream reconnects (the
  sidecar's `MaxIdleConnsPerHost` is 2); `new` pays one WhoIs per request.
- **video** profiles (fps, Mb/s): `720p30` (30, 2.5), `1080p30` (30, 6), `1080p60`
  (60, 12), `4k30` (30, 25). Late, dropped and freeze counts are clock-free; only
  `frame_latency_ms` depends on the clock estimate.

**Not a CRDT.** RFC 018's CrdtDoc/Loro was removed on 2026-07-07. `synced-store`
measures SyncedStore, which is last-write-wins state sync; it is what exists, so it
is what is measured. Its updates go to every WebSocket-connected peer, so pair mode
is the reference.

## Suites

Suites run their items one after another on one hub (one pair with `--pair`), with
a 1 s cool-down between items. The CLI and the app share the definitions.

| Profile | Contents |
|---|---|
| `smoke` | path 10; tcp throughput 3 s; udp stream 3 s at 5 Mb/s; quic throughput 3 s; ws rr 100; file-transfer 1 MiB × 2; synced-store pingpong 20; http 3 s; video 720p30 3 s; egress 3 s |
| `standard` | Every workload at its defaults |
| `full` | `standard`, plus tcp download, bidirectional and 4 streams; tcp latency at 64, 1024 and 32768 B × nodelay on/off; udp stream at 200, 1200, 1252, 1253 and 1400 B; udp echo; udp ramp; quic latency; ws stream; synced-store burst; http direct, `new`, and proxy keepalive at concurrency 1 and 2; video over udp, quic and tcp |
| `nightly` | `full` without the udp payload sweep or video over tcp, every duration capped at 10 s; at most 12 min of workload time. Refuses debug builds unless `--allow-debug`. |

## Methodology

This is what the engine does for every run, so numbers from the CLI, the app and
CI mean the same thing when they come from the same build profile.

### Units and goodput

- Mb/s means 10⁶ bit/s. Old reports in MB/s (10⁶ B/s) are converted × 8 on import.
- **Goodput** is payload bits delivered per second, counted **at the receiver**,
  over the measurement window, excluding warmup. Headers are not payload.
- The receiver keeps one counter per direction across all streams. Its epoch is the
  first payload byte of any stream; bucket *i* covers
  `[epoch + 250·i ms, epoch + 250·(i+1) ms)`.
- `goodput_mbps = (bytes_total − bytes in the warmup buckets) · 8 / (last byte −
  epoch − warmup)`. A window shorter than warmup + 1 s fails the run ("window too short").
- The series plots each 250 ms bucket at its end; warmup points stay in the chart,
  flagged. Sender-side live values use separate keys (`sent_mbps`) so they are never
  mistaken for goodput.
- The sender reports `sent_bytes` per stream, so every run can be cross-checked; the
  live tests assert Σ buckets == bytes received and received ≤ sent per stream.
- **Bidirectional** runs count each direction at its own receiver over the window
  where both overlap, and report the sum, labelled "sum of both directions".

### Warmup

- Time-based workloads drop the first `warmup_secs` (default 1 s) from goodput.
- Latency modes discard the first `warmup_count` samples (connection set-up, remote
  WhoIs and path warm-up land there).
- file-transfer drops iteration 1 from the summary when there are three or more.

### Latency and percentiles

- RTT is measured on one clock: from the send call to the reply. Each message is one
  `write_all`.
- Percentiles are nearest-rank (`sorted[⌈p·n⌉ − 1]`) and are **null unless there are
  enough samples**: p90 needs 10, p95 20, p99 100, p999 1000. `stddev` uses n − 1.
- Lost and timed-out samples count as +∞: a percentile that lands on one is null,
  and losses are reported in `lost`. Tails are never flattered by dropping timeouts.

### Clocks

- Engine timestamps are a wall-clock reading taken once at hub start plus monotonic
  time since then, so NTP steps cannot move them mid-run.
- When a workload must map the peer's times into ours (video one-way latency,
  responder-side series, the bidirectional overlap), it estimates the offset
  NTP-style before and after the run (16 samples, keeping the one with the smallest
  delay δ), interpolates between the two, and records `drift_ppm` and an uncertainty
  of ⌈δ/2⌉. No clock request runs during a data phase.
- Metrics that can be clock-free are: UDP jitter (RFC 3550, offset-invariant), the
  UDP loss series (by sequence number), video late/dropped/freeze (relative delay).

### Which path carried the bytes

- Before each workload the engine **settles** the path: it pings until two in a row
  report the same connection (at most 3 s) and records `settle_ms`, then takes a
  `before` snapshot. It takes an `after` snapshot at the end.
- Time-based workloads also sample the path at 2 Hz (`path_timeline`), which gives
  **latency under load** (`loaded_rtt_ms`). Before/after pings alone cannot see a
  mid-run direct↔DERP flip: the sidecar re-reads peers only on control-plane updates
  or a 30 s ticker.
- Both sides read Tailscale's usermetrics byte counters (direct, DERP, peer relay)
  at start and finish, and the deltas are summed. When they count at least 256 KiB
  (`PATH_BYTES_MIN`), `path_label` is `direct` if the relayed share (DERP plus peer
  relay) is under 1% and the timeline shows no transition. It is `relay` if the
  relayed share is over 99%, and `mixed` otherwise, with the share and any transitions
  in the notes. `derp_share` is the DERP share only.
- Without counters, or below 256 KiB (a short or path-only run is mostly pings and
  control traffic), the label comes from the ping samples (`before`, the timeline,
  `after`): `direct` if all say direct, `relay` if all are relayed, otherwise `mixed`.
  It carries the note "path label from ping samples; …", and `derp_share` is null.
  `unknown` appears only when no sample named a path.
- History filters on `path_label`, never on the `before` snapshot.

### Topology honesty

| Topology | When |
|---|---|
| `same-process` | `pair`: two hubs in one process |
| `same-host` | Both hosts report the same, non-weak `host_fingerprint` (first 16 hex of sha256(machine id ‖ hostname)) |
| `separate-hosts` | Anything else |

`same-process` and `same-host` runs carry the note "measures the bridges and
netstack, not a link". Never attribute loopback or page-cached throughput to a
physical link.

### Resources

- CPU is the cumulative CPU time (user + sys) of this process and its sidecar over the
  measurement window; 100% is one logical CPU, so values can exceed 100. `cpu_pct_max`
  is over 500 ms intervals. RSS is the maximum seen.
- `cpu_ms_per_mb` divides by goodput megabytes, so efficiency is comparable across
  link speeds. Peer resources come back with the run.
- In `same-process` runs only `combined` is filled (the process plus both sidecars).
- `on_battery` is recorded on macOS; the app also records how much of the run it was
  in the foreground. The app opts out of App Nap and, while any run is active in
  either role, holds a latency-critical activity that also blocks idle sleep.

### One run at a time

A hub takes part in one data-plane run at a time, in either role: an initiating
hub answers `prepare` with `busy`, and a hub being measured refuses local runs.
`path` and probes are exempt; any that overlapped are counted in
`overlapping_runs`, and History flags those runs.

### Comparability

- `compare_key` is a hash of the workload, the params (without durations, counts and
  warmups), topology, `path_label`, both build profiles, the methodology version and
  the peer's host fingerprint. Trends, sparklines and "latest" cards only group runs
  with the same key; comparing different keys lists the fields that differ.
- **Debug builds are not comparable with release builds.** Workspace crates compile
  at opt-level 0 in dev, so `cargo run` without `--release` and `app:dev` produce
  debug numbers. Reports record `build_profile`, `rustc`, git sha/dirty and the
  sidecar SHA-256 for both sides, and debug runs carry a note.

### Run hygiene

- Use release builds, the pinned Go toolchain and idle, powered machines. Run at
  least five trials.
- Keep OS, CPU, toolchains, sidecar hash, parameters and placement constant between
  runs you compare. Keep the exact source diff when benchmarking a dirty tree.
- Exercise direct, peer-relay and DERP paths under controlled conditions, check that
  `path_label` matches what you intended, and write down the network conditions.
- For profiles beyond the built-in resource numbers, use the platform profiler or
  `/usr/bin/time -l` (macOS) / `-v` (Linux), and include both the sidecar and the
  Rust process.

## Baselines ladder

Each rung adds one layer; the difference between two rungs, on the same hosts,
path and build profile, is the cost of that layer.

| Rung | Tool | Isolates |
|---|---|---|
| 0. System Tailscale | `iperf3 -s` on B, `iperf3 -c <B's tailnet IP>` on A, through the OS Tailscale client | What the link and Tailscale's own data path achieve (not tsnet). Manual. |
| 1. tsnet ceiling | [`tsnet-bench`](../packages/sidecar-slim/cmd/tsnet-bench) ([procedure](../docs/tailscale-performance.md#two-hosts-and-a-native-baseline)) | Go userspace netstack, no Rust bridge. An in-sidecar equivalent is planned. |
| 2. truffle raw TCP | legacy `truffle-bench transport` (same wire as rung 1; compare with `--concurrency 1`), then the v3 `tcp` and `egress` workloads | The Rust bridge: rung 2 − rung 1. `tcp` crosses two bridges, `egress` one. |
| 3. Bindings | JS/NAPI lane (planned); `examples/quic-streams bench` meanwhile | The binding layer. |
| 4. Subsystems | `ws`, `file-transfer`, `synced-store`, `http`, `video` | truffle's own protocols on top of rung 2. |

The criterion benches sit beside the ladder: `envelope` is the codec cost per
message, and `chunk_hash` is the SHA-256 ceiling for file-transfer hashing.

## Reports

Every run writes one **schema v3** JSON report. Rust is the source of truth:
[`schema/report.v3.schema.json`](schema) and `schema/fixtures/*.json` are generated
from the engine's types by the test `schema_is_current`, which fails on drift.
Regenerate with:

```sh
UPDATE_SCHEMA=1 cargo test --locked -p truffle-bench schema_is_current
```

The app's TypeScript types are checked against the same fixtures (vitest + `tsc`).

| Field | Holds |
|---|---|
| `schema_version`, `methodology` | `3`, `"v3"` (imports carry `v2-transport`, `native-tsnet`, …) |
| `run_id`, `suite_id`, `tool`, `tool_version`, `truffle_version` | Identity; `tool` is `truffle-bench-cli`, `truffle-bench-app` or `imported` |
| `workload`, `label`, `params` | What ran; `params` as validated and used by the responder |
| `status`, `error`, `error_code` | `ok`, `failed` or `cancelled`; failed runs still write a report |
| `local`, `peer` | Each side's OS, CPU count, build profile, rustc, git, sidecar path and hash, tailnet IP, host fingerprint, battery |
| `network` | `topology`, `settle_ms`, `before`/`after`, `clock`, `path_timeline`, `loaded_rtt_ms`, `path_bytes`, `derp_share`, `path_label` |
| `summary` | Headline values: `key`, `label`, `value`, `unit`, `better`, `uncertainty` |
| `results`, `series` | Workload-specific detail and 250 ms time series |
| `resources`, `overlapping_runs`, `notes`, `compare_key` | See [methodology](#methodology) |

**Old reports.** `truffle-bench report` and the app's History import (file, folder
or `.zip`) read every older format and normalise it to v3 with `tool: "imported"`:

| Format | Recognised by | Becomes |
|---|---|---|
| v1 | No `schema_version` | `v2-file-transfer` / `v2-synced-store` / `v2-request-reply` methodology |
| Rust v2 (`transport`, `file-transfer`, `synced-store`, `request-reply`) | `environment` and `network` objects | MB/s × 8; file-transfer throughput → `e2e_mbps` (never `wire_mbps`); transport → `tcp`/`quic` with `fixed_size_mbps` (a different method from `goodput_mbps`) |
| tsnet-bench | `workload: "native-tsnet-tcp"`, top-level `topology`/`before`/`after` | `peer: null`, `goodput_mbps` from its samples, note "native Go/tsnet baseline" |

## CI

| Workflow | What it runs |
|---|---|
| [`benchmarks.yml`](../.github/workflows/benchmarks.yml) | Nightly 06:00 UTC and on demand (input `profile`: smoke, standard, full, nightly; default nightly). Release build, `suite --profile <p> --pair`, then legacy `transport` TCP and QUIC. The step summary lists each report's status and headline numbers; artefact `bench-results-<run id>` is kept 30 days and imports into the app as a `.zip`. All suite reports are `same-process` and hosted-runner numbers are directional (±20-40%); a dedicated runner is a future option. |
| [`integration-tests.yml`](../.github/workflows/integration-tests.yml) | Every PR and push: `cargo test -p truffle-core -p truffle-bench --tests`. The engine's `tests/live_pair.rs` runs one pair — path 5; tcp throughput 2 s; ws rr 50; file-transfer 1024 B × 12 (the pending-offer regression); synced-store pingpong 10 — and asserts every item is `ok`. It prints `[skip]` and passes without a key (fork PRs). |
| [`ci.yml`](../.github/workflows/ci.yml) | The engine is a normal member: clippy `-D warnings`, rustdoc `-D warnings`, nextest on Ubuntu, macOS and Windows, doctests. The app crate is excluded (GUI toolchains), like `truffle-tauri-plugin`; its frontend is linted, format-checked, built and tested with the other pnpm packages. Criterion benches are compile-checked on Linux. |

The app bundle itself is not built in CI.

## Working on the hub

- **Commit types.** release-please ignores a commit only when every file it touches
  is under `benchmark/`. Hub changes that touch anything else (Cargo.toml, lockfiles,
  workflows, scripts, docs) use `chore(bench):`, `ci:`, `build:` or `docs:`, never
  `feat:` or `fix:`. A truffle-core change the hub needs ships on its own as
  `feat(core):` / `fix(core):`. See [docs/RELEASING.md](../docs/RELEASING.md).
- **Workspace commands.** Add `--exclude truffle-bench-app` next to the existing
  excludes in any new `cargo … --workspace` command. A plain `cargo build` only builds
  the default members, which do not include GUI crates.
- **Placeholders.** Never write anything that looks like a real auth key; use
  `tskey-auth-…`. The pre-commit secret scan blocks `tskey-` followed by eight or more
  key characters.
- **Brand files.** `app/src/brand/` is vendored unedited from `career/brand` and is
  excluded from Prettier. The JY mark and logos are James Yong's, not covered by the
  repo's MIT licence (see `app/src/brand/NOTICE`).
- **Local artefacts.** Do not delete untracked or ignored files outside `benchmark/`
  (old sidecar binaries, `.node` addons, `target/bench-results/*.json`); they belong
  to whoever made them.

## What we found so far

These are **same-host observations on one Mac** (Apple silicon, 10 logical CPUs,
release builds), from the engine's `full` and `nightly` suites in `pair` mode and a
`serve` + `suite --peer` run in two processes, all on **2026-10-16**. Both ends shared
one machine, so they measure the bridges, the netstacks and the sidecars, not a
network link; treat them as leads to check across two machines, not as results.

- **New tailnet connections cost about 41 ms each, one at a time.** HTTP with a new
  connection per request reaches ~26 req/s whatever the worker count: one worker sees
  p50 ≈ 41 ms, four see p50 ≈ 153 ms, while `open_tcp` itself returns in about 1 ms.
  The cost lands on the first bytes, and it is serialised across connections. It very
  likely explains the fixed ~40 ms in core's file-transfer `transfer_secs` at every
  file size (a 1 MiB file "transfers" in 61–64 ms) and part of the ~25 ms
  `negotiate_ms`.
- **`ft.done` arrives about 100 ms after `send_file` returns** (101–105 ms, every file,
  every run). It changes no metric, but adds ~0.1 s per file to a multi-file transfer.
  The cause is deterministic: core's receiver sleeps 100 ms after writing the ACK
  (`file_transfer/receiver.rs`, so the Go bridge forwards the ACK before the stream
  drops) and only then emits `FileTransferEvent::Completed`, which the responder waits
  for before sending `ft.done`. Emitting `Completed` before that sleep, or moving the
  sleep and the stream drop into a spawned task, would remove it.
- **Messages drop in core's session broadcast at high rates.** A `ws stream` of 64 B
  messages ran at 252k msg/s with 1.58% dropped: 32,775 lost before the subscription
  (core's 1024-entry session broadcast overflowing; `router_dropped`) and 8,077 at
  the receiver's lagging subscription. Other runs at ~212k msg/s lost 79–1,521, all
  at the subscription. The WebSocket itself is lossless; the same channel carries
  control traffic.
- **Nagle with 32 KiB messages on lo0: not reproducible yet.** An early run measured
  TCP latency p50 1.44 ms with `TCP_NODELAY` and 10.39 ms without. Three `full` and
  `nightly` suites since show no difference (1.33 vs 1.31, 1.66 vs 1.66, 1.69 vs
  1.69 ms). 64 B and 1 KiB never differ (~0.24 ms).
- **QUIC against TCP.** 4 QUIC streams: 390–467 Mb/s; 1 TCP stream: 351–503 Mb/s;
  4 TCP streams: 316–506 Mb/s. QUIC connects in ~41–46 ms (relay set-up and TLS)
  against ~0.8 ms for TCP, and is slightly faster per round trip (p50 0.20 vs 0.23 ms
  at 64 B). Video frames complete faster over QUIC (p50 2.9–3.1 ms) than over TCP
  (3.6–4.0 ms); paced UDP spreads each frame over its 33 ms slot by design (~34 ms).
- **The sidecars are the ceiling.** At ~350–500 Mb/s the two sidecars together use
  about 6 logical CPUs (590–660% in `pair` runs; ~300% per side in the two-process
  run at 387 Mb/s), while the Rust processes use 25–40%. Throughput is bimodal from
  run to run: the same TCP, egress and bidirectional runs land near 350 or near
  500 Mb/s, sometimes switching mid-run, so compare medians of several runs.
- **One bridge less.** Egress (one Rust bridge) against TCP (two): +12.6% in the
  final `full` suite (397.6 vs 353.0 Mb/s), but within the run-to-run spread above.
- **HTTP.** Through the sidecar proxy with keep-alive: 3.9k, 6.1k and 8.9k req/s at
  1, 2 and 4 workers (p50 0.25–0.43 ms); straight to a tailnet listener 10.4k req/s.
- **UDP.** 10 Mb/s streams lost nothing at 200, 1200, 1252, 1253 and 1400 B (the last
  two fragment in the netstack); echo p50 ~0.76 ms. The default ramp (from 10 Mb/s,
  ×1.5 every 3 s, 10 s) runs out of time before it finds a limit: it reports
  22.5 Mb/s, the last full step, with no loss at 33.75 Mb/s either. Use a higher start
  rate or a longer run to find the ceiling.
- **Starting a pair.** The first WebSocket dial to a just-joined peer sometimes takes
  ~5 s (most likely the WireGuard handshake retry), so a pair start takes 3.5 s or ~8 s.

## Planned

Shown in the app's Planned view, with reasons:

- QUIC datagrams (RFC 9221) and `QuicConnection` stats (RTT, cwnd, loss)
- The JS binding (NAPI) lane, and Swift (RFC 024) peers
- A tsnet ceiling inside the sidecar (`bench:*` command); use `tsnet-bench` meanwhile
- A latency waterfall (TSMP/ICMP ping; core pins disco today)
- HTTP with `tls: true` (handshake time, first-cert time)
- `pull_file`, and SyncedStore `FileBackend`
- Egress via SOCKS5 / HTTP CONNECT
- Broadcast fan-out (needs three or more nodes)
- A live camera (WebCodecs) and a video ramp
- A Taildrop comparison
- `tsnet:needsApproval` surfaced by core
