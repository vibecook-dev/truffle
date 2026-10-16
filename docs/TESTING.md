# Testing and Benchmarking

This guide covers Truffle's local and CI test suites, against either mocks
(no setup) or a real Tailscale network (one-time setup). The local harness is
specified in [RFC 019](rfcs/019-local-testing-and-benchmarking.md), and the CI
automation in [RFC 020](rfcs/020-ci-testing.md). Benchmarks (the
`truffle-bench` engine and CLI, and the truffle bench app) are specified in
[RFC 027](rfcs/027-benchmark-hub.md) and documented in
[benchmark/README.md](../benchmark/README.md).

---

## TL;DR

| You want to... | Command |
|---|---|
| Run all unit tests (no network) | `just test`, or `env TRUFFLE_TEST_AUTHKEY= TS_AUTHKEY= TRUFFLE_BENCH_AUTH_KEY= cargo test --locked --workspace --exclude truffle-bench-app` (empty keys keep `.env`'s key out, so live tests skip) |
| Run real-network integration tests | `TRUFFLE_TEST_AUTHKEY=tskey-... cargo test -p truffle-core` |
| Run the ignored transport stress suite | `cargo test --locked -p truffle-core --test integration_transport -- --ignored --test-threads=1` |
| Run micro benchmarks (no network) | `pnpm bench:micro` (criterion, two benches) |
| Run the benchmark suite against a real tailnet | `cargo run --locked -p truffle-bench --release -- suite --profile smoke --pair` |

Without an auth key, real-network tests print `[skip] ...: TRUFFLE_TEST_AUTHKEY not set` and pass — you can contribute to 95% of the codebase without any Tailscale setup.

---

## One-Time Setup (for real-network tests)

You only need this if you want to run the integration tests that hit real
Tailscale — everything else works without it.

### 1. Create a Tailscale auth key

Go to <https://login.tailscale.com/admin/settings/keys> and generate an auth
key with:

- [x] **Reusable** — one key handles many test runs.
- [x] **Ephemeral** — test nodes auto-expire 30–60 min after shutdown.
- [x] **Tags: `tag:truffle-test`** — scopes the key so a stray leak can only
      mint test nodes, not production devices.
- [x] **Pre-approved** (only if your tailnet has device approval enabled).

The key looks like `tskey-auth-kABCDE1CNTRL-...`.

### 2. Add the tag to your tailnet ACL

If this is the first time using `tag:truffle-test`, your tailnet ACL needs
to declare it as an ownerless tag. Example snippet (add to your `tagOwners`
and `acls` sections):

```jsonc
{
  "tagOwners": {
    "tag:truffle-test": []  // or your admin group
  },
  "acls": [
    // Allow test nodes to reach each other — needed for the pair harness.
    {
      "action": "accept",
      "src":    ["tag:truffle-test"],
      "dst":    ["tag:truffle-test:*"]
    }
  ]
}
```

Apply the ACL, then the first test run will register the device.

### 3. Put the key in `.env`

At the repo root:

```bash
cp .env.example .env
# edit .env — set TRUFFLE_TEST_AUTHKEY=tskey-auth-...
```

`.env` is gitignored and loaded automatically by tests/benchmarks via
`dotenvy`. Don't commit it.

### 4. Install Go (for the test-sidecar build script)

The integration tests spawn a Go-based tsnet sidecar. Install the Go version recorded in `packages/sidecar-slim/go.mod` (currently 1.27.2) from
<https://go.dev/dl/>. truffle-core's `build.rs` auto-detects Go and builds
the `test-sidecar` binary the first time you run integration tests.

If Go is missing, the build prints a clear warning and the tests fail with
instructions. Library builds stay green.

---

## Environment Variables

All variables read by tests and benchmarks. Defaults shown.

| Variable | Default | Purpose |
|---|---|---|
| `TRUFFLE_TEST_AUTHKEY` | _(unset → skip)_ | Tailscale auth key |
| `TS_AUTHKEY` | _(unset)_ | Legacy fallback; prints deprecation warning |
| `TRUFFLE_TEST_TAGS` | `tag:truffle-test` | Comma-separated tags to advertise |
| `TRUFFLE_TEST_EPHEMERAL` | `true` | Register nodes as ephemeral |
| `TRUFFLE_TEST_LOG` | `truffle_core=info` | `tracing_subscriber` filter |
| `TRUFFLE_TEST_STATE_ROOT` | `$TMPDIR` | Root for per-run node state dirs |
| `TRUFFLE_BENCH_AUTH_KEY` | _(unset)_ | Preferred key for `truffle-bench` and the app's test sign-in; `truffle-bench` falls back to `TRUFFLE_TEST_AUTHKEY`, then `TS_AUTHKEY`. The legacy `transport` subcommand ignores it: it reads only `TRUFFLE_TEST_AUTHKEY`, then `TS_AUTHKEY`, and has no `--auth-key-file` |

---

## Tests

### Structure

| Path | Network | Run by `cargo test` |
|---|---|---|
| `crates/*/src/**/*.rs` `#[cfg(test)]` blocks | Mocks / in-process | Yes — always |
| `crates/truffle-core/tests/integration_network.rs` | **Real Tailscale** (pair) | Yes (skip w/o auth) |
| `crates/truffle-core/tests/integration_synced_store.rs` | **Real Tailscale** (pair) | Yes (skip w/o auth) |
| `crates/truffle-core/tests/integration_egress.rs` | **Real Tailscale** (SOCKS5, routes, CONNECT, caller identity) | Yes (skip w/o auth) |
| `crates/truffle-core/tests/integration_request_reply.rs` | **Real Tailscale** (pair) | Yes (skip w/o auth) |
| `crates/truffle-core/tests/integration_file_transfer.rs` | **Real Tailscale** (pair) | Yes (skip w/o auth) |
| `crates/truffle-core/tests/integration_transport.rs` | In-process loopback (heavy) | CI on Linux; locally only with `--ignored` |

### Running

```bash
# Fast: unit + integration (integrations skip without a key).
# truffle-bench-app is the GUI benchmark app; it stays out, as in CI.
cargo test --locked --workspace --exclude truffle-bench-app

# Heavy loopback transport stress suite (serial to reduce flakiness)
cargo test --locked -p truffle-core --test integration_transport \
    -- --ignored --test-threads=1

# Just the integration suite, verbose
TRUFFLE_TEST_LOG="truffle_core=debug,integration_synced_store=debug" \
    cargo test -p truffle-core --tests -- --nocapture

# Single test
cargo test -p truffle-core --test integration_synced_store \
    test_pair_converges_on_simple_write -- --nocapture
```

Or with `just`:

```bash
just test          # workspace unit + integration (skip)
just test-real     # integration with extra logging
```

### How the skip works

Every real-network test opens with:

```rust
let Some(authkey) = common::require_authkey("test_name") else { return; };
```

If `TRUFFLE_TEST_AUTHKEY` (or the legacy `TS_AUTHKEY`) is missing, the
helper prints a single `[skip]` line and the test returns as passing. No
`#[ignore]` attributes — the env var is the only switch.

### How the pair harness works

Each integration test spins up **two ephemeral tsnet nodes** on your
tailnet, waits for them to discover each other, and exercises real traffic
between them — no external EC2 or hand-configured peer required.

- Nodes are named `truffle-test-integ-<short-id>-{alpha,beta}` (Layer 3 pair) or
  `truffle-<app>-{alpha,beta}-<short-id>` (Node pair).
- Per-test `TempDir` for tsnet state — no cross-test contamination.
- `ephemeral=true` — Tailscale removes nodes ~30-60 min after the test ends.
- Child sidecar processes die via tokio's `kill_on_drop(true)`.

If the two nodes can't see each other within 45 s, the test fails with a
diagnostic listing what each node's `peers()` returned.

---

## Benchmarks

[benchmark/README.md](../benchmark/README.md) is the full guide: the CLI, the
desktop app, every workload, the methodology and the report format. This
section covers what you need from a test-focused workflow.

### Micro benchmarks (criterion)

No Tailscale. Two benches in `crates/truffle-core/benches/`:

- `envelope.rs` — JSON codec encode/decode throughput
- `chunk_hash.rs` — SHA-256 throughput (full buffer + streamed). It measures
  the `sha2` crate, i.e. the hashing ceiling for file transfer, not truffle code.

```bash
pnpm bench:micro                     # or: just bench-micro (both benches)
CRITERION_HOME="$PWD/target/criterion" cargo bench --locked -p truffle-core --bench envelope
```

`CRITERION_HOME` keeps output in this repo's `target/criterion/` even when your
cargo target dir is shared or redirected (bench binaries run from
`crates/truffle-core`, so the path must be absolute). Each bench prints lines like:

```
envelope/decode/262144  time: [40.8 µs 40.9 µs 41.0 µs]
                        thrpt: [5.96 GiB/s 5.97 GiB/s 5.98 GiB/s]
```

### Real-network benchmarks (truffle-bench)

One engine, `truffle-bench` (crate at `benchmark/engine`), drives every
two-node workload: `path`, `tcp`, `udp`, `quic`, `ws`, `file-transfer`,
`synced-store`, `http`, `video` and `egress`.

```bash
# Every workload at its defaults, on two hubs in one process (needs a key)
cargo run --locked -p truffle-bench --release -- suite --profile standard --pair

# One workload on a same-process pair, with parameter overrides (needs a key)
cargo run --locked -p truffle-bench --release -- pair tcp --param streams=4

# Two machines, no key needed: a responder on B, then a run from A.
# `serve` names itself <hostname>-cli; `truffle-bench peers` lists what A sees.
cargo run --locked -p truffle-bench --release -- serve                    # on B
cargo run --locked -p truffle-bench --release -- run --peer mac-b-cli tcp # on A

# Print summaries of any reports (v1, v2, v3, tsnet-bench; files, dirs or zips)
cargo run --locked -p truffle-bench --release -- report target/bench-results
```

Or: `pnpm bench:suite`, `pnpm bench:pair <workload>`, `just bench-macro
[profile]`, `just bench-one <workload> …`, `just bench-serve`. Suite profiles
are `smoke`, `standard`, `full` and `nightly`.

Auth: `serve`, `peers` and `run` need no key — without one they print a login
URL and you sign in with your own Tailscale account. `pair` and `suite --pair`
require a key: `--auth-key-file <path>`, else `TRUFFLE_BENCH_AUTH_KEY`, else
`TRUFFLE_TEST_AUTHKEY`, else `TS_AUTHKEY` (with a warning). Keys are never
accepted as command-line values. The legacy `transport` subcommand reads only
`TRUFFLE_TEST_AUTHKEY`, then `TS_AUTHKEY`.

`run`, `pair` and `suite` write one v3 JSON report per run to `--out-dir` (default
`target/bench-results/`, relative to the current directory), named
`<timestamp>-<workload>-<run_id>.json`, so repeated runs never overwrite each
other. Each report records both builds, the path (direct or relayed) and a
`compare_key`; compare only runs with the same key, and never debug builds
against release builds. Same-process numbers measure the bridges and netstack,
not a link.

The legacy `transport` subcommand (schema 2, byte-compatible with
`tsnet-bench`) is unchanged; see
[tailscale-performance.md](tailscale-performance.md).

---

## Pre-commit Hook (recommended)

```bash
git config core.hooksPath .githooks
```

Enables `.githooks/pre-commit`, which runs on every commit:

1. NAPI binding drift check
2. Workflow YAML sanity
3. Rustfmt on staged `.rs` files
4. Prettier on staged TypeScript files
5. `truffle` meta-crate doctest (if relevant files changed)
6. TypeScript build (if packages/ or benchmark/app files changed)
7. **Secret scan** — blocks commits containing `tskey-` patterns

Skip once with `git commit --no-verify`. If the secret scanner flags a
legitimate doc example, add the file to `allowed_files` in
`scripts/check-for-secrets.sh`.

---

## Troubleshooting

**`test-sidecar binary not found`** — Go isn't installed or `go build`
failed. Install the Go version recorded in `packages/sidecar-slim/go.mod` and re-run. Or manually: `cd packages/sidecar-slim
&& go build -o ../../crates/truffle-core/tests/test-sidecar .`

**`pair rendezvous timeout`** — Both nodes registered, but they couldn't
see each other within 45 s. Usual causes:
- Tailnet ACL doesn't allow `tag:truffle-test` → `tag:truffle-test`.
- Auth key was revoked, wasn't reusable, or expired.
- Tailscale control plane is slow (rare); re-run.

**No auth key (from `truffle-bench pair` / `suite --pair`)** — unlike tests,
these commands error out rather than silently skipping, because a pair of
throwaway nodes needs a key. Set `TRUFFLE_BENCH_AUTH_KEY` or
`TRUFFLE_TEST_AUTHKEY` (shell or `.env`), or pass `--auth-key-file`.
`serve`, `peers` and `run` never need a key: without one they print a login URL
and wait for you to sign in.

**Benchmarks are wildly noisy** — expected on a shared dev machine. Close
other apps, plug laptops in, disable background sync, and look at *relative*
changes between runs with the same `compare_key`, not absolute numbers. See
the methodology in [benchmark/README.md](../benchmark/README.md).

**Integration tests leak Tailscale devices** — they shouldn't:
`ephemeral=true` + short test lifetime means Tailscale removes them within
the hour. If you see stale devices in the admin panel, check that
`TRUFFLE_TEST_EPHEMERAL=true` (the default) hasn't been overridden.

**Tailscale free-plan ceiling** — 50 concurrent tagged devices. Each test
run makes 2 ephemeral devices. Running all integration tests serially uses
~20 at peak; parallelism uses more. If you hit the limit, wait ~30 min for
ephemeral cleanup, or tighten `TRUFFLE_TEST_LOG` and re-run.

---

## CI Integration

The harness runs in GitHub Actions too. See
[RFC 020](rfcs/020-ci-testing.md) for the design.

### Workflows

| Workflow | Trigger | What it runs |
|---|---|---|
| `ci.yml` | PR + push to main | locked dependency graph, release metadata, unit tests, warnings-as-errors, fmt, Linux transport stress, bench compile-check |
| `integration-tests.yml` | PR + push to main | real-network tests (truffle-core, plus `truffle-bench`'s `live_pair`); only fork PRs may skip without the secret |
| `benchmarks.yml` | nightly cron (06:00 UTC) + manual | credential preflight, `truffle-bench suite --profile nightly --pair`, legacy `transport` TCP/QUIC, uploads JSON artefacts |

### One-time setup: configure the `TRUFFLE_TEST_AUTHKEY` secret

1. **Generate a Tailscale auth key for CI.** Go to
   <https://login.tailscale.com/admin/settings/keys> → **Generate auth key**.
   Enable:
   - **Reusable** (CI uses it for every run)
   - **Ephemeral** (nodes auto-expire within ~1 hour of job end)
   - **Pre-approved** (skip manual approval per device)
   - Leave **Tags** unset unless your ACL has a matching `tagOwners` entry.

   Expiration is 90 days max — put a calendar reminder at T+80 to rotate.

2. **Install it as a GitHub secret.**
   In the repo: **Settings → Secrets and variables → Actions → New repository secret**.
   - Name: `TRUFFLE_TEST_AUTHKEY`
   - Value: the `tskey-auth-...` string

3. **Verify.** Trigger a run — either push any commit, or from the
   Actions UI, **Integration tests** → **Run workflow**.

### Fork PRs

GitHub deliberately doesn't pass secrets to PRs opened from forks.
Integration tests skip cleanly on fork PRs — `[skip]` messages in the
logs, workflow still passes green. External contributors can open PRs
without needing any secret or special setup.

### Rotating the auth key

Every 80 days (before the 90-day expiry), do:

1. Generate a new auth key (same settings as above).
2. Update the `TRUFFLE_TEST_AUTHKEY` secret in GitHub with the new value.
3. Revoke the old key in the Tailscale admin panel.

An OAuth client that mints short-lived keys on demand remains the preferred
long-term replacement for the reusable repository secret.

### Nightly benchmarks

The `benchmarks.yml` workflow runs every night at 06:00 UTC:
`truffle-bench suite --profile nightly --pair` (the `full` profile without the
UDP payload sweep or video over TCP, every duration capped at 10 s), then the legacy
`transport` TCP and QUIC runs. Every item writes a JSON report; the step
summary lists each one's status and headline numbers, and the reports go to
the workflow's **Artefacts** section (`bench-results-<run id>`, retained 30
days). The truffle bench app imports the downloaded `.zip` directly.

Before compiling and running the suite, CI runs a three-minute real-tailnet
identity test. A missing, expired, revoked, or mis-scoped key therefore fails
quickly with a focused error instead of wasting the whole benchmark job.

To run on demand:
- Actions → **Benchmarks (nightly + on-demand)** → **Run workflow**
- Pick a `profile`: `smoke`, `standard`, `full` or `nightly` (default)

Hosted-runner numbers are directional (±20-40%) and every suite report is
`same-process`. A dedicated runner is a future option.

---

## Design References

- [RFC 019 — Local Testing and Benchmarking Harness](rfcs/019-local-testing-and-benchmarking.md)
- [RFC 020 — CI Testing Harness](rfcs/020-ci-testing.md)
- [RFC 027 — Benchmark hub](rfcs/027-benchmark-hub.md) and [benchmark/README.md](../benchmark/README.md)
- [RFC 012 — Layered architecture](rfcs/012-layered-architecture-redesign.md)
- [RFC 016 — SyncedStore](rfcs/016-synced-store-and-request-reply.md)
