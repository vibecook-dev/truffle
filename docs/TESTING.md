# Testing and Benchmarking

This guide covers Truffle's local and CI test suites, against either mocks
(no setup) or a real Tailscale network (one-time setup). The local harness is
specified in [RFC 019](rfcs/019-local-testing-and-benchmarking.md), and the CI
automation in [RFC 020](rfcs/020-ci-testing.md).

---

## TL;DR

| You want to... | Command |
|---|---|
| Run all unit tests (no network) | `cargo test --locked --workspace` |
| Run real-network integration tests | `TRUFFLE_TEST_AUTHKEY=tskey-... cargo test -p truffle-core` |
| Run the ignored transport stress suite | `cargo test --locked -p truffle-core --test integration_transport -- --ignored --test-threads=1` |
| Run micro benchmarks (no network) | `cargo bench -p truffle-core` |
| Run macro benchmarks against a real tailnet | `cargo run -p truffle-bench --release -- file-transfer` |

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
# Fast: unit + integration (integrations skip without a key)
cargo test --locked --workspace

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

### Tier 1 — Micro benchmarks (criterion)

No Tailscale. Runs on every dev machine, every CI run.

```bash
cargo bench -p truffle-core                 # all three
cargo bench -p truffle-core --bench envelope
```

Files:
- `benches/envelope.rs` — JSON codec encode/decode throughput
- `benches/chunk_hash.rs` — sha2-256 throughput (full buffer + streamed)

Output lands in `target/criterion/`. Each bench run prints percentile
lines like:

```
envelope/decode/262144  time: [40.8 µs 40.9 µs 41.0 µs]
                        thrpt: [5.96 GiB/s 5.97 GiB/s 5.98 GiB/s]
```

### Tier 2 — Macro benchmarks (truffle-bench)

Real Tailscale pair. Requires `TRUFFLE_TEST_AUTHKEY`.

```bash
# File transfer — throughput for various payload sizes
cargo run -p truffle-bench --release -- file-transfer --size 10M --iterations 5

# SyncedStore convergence — B1 sequential (alpha writes, beta catches up)
cargo run -p truffle-bench --release -- synced-store \
    --scenario b1-sequential --ops 1000

# SyncedStore convergence — B2 concurrent (both write, both converge)
cargo run -p truffle-bench --release -- synced-store \
    --scenario b2-concurrent --ops 500

# Request/reply RTT — fires N round trips, reports p50/p95/p99/p999
cargo run -p truffle-bench --release -- request-reply \
    --count 500 --payload-bytes 1024
```

Or: `just bench-macro` runs the default matrix.

Each run writes a JSON report to `target/bench-results/{timestamp}-{workload}.json`
with per-iteration numbers, percentiles, and the current git SHA. Keep
these around to spot regressions on your own machine — absolute numbers
aren't comparable across machines.

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
6. TypeScript build (if packages/ files changed)
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

**`TRUFFLE_TEST_AUTHKEY is not set` (from `truffle-bench`)** — unlike tests,
the benchmark crate errors out rather than silently skipping. Set the key
or add it to `.env`.

**Benchmarks are wildly noisy** — expected on a shared dev machine. Close
other apps, disable background sync, and look at *relative* changes, not
absolute numbers. Dedicated hardware is a Phase-2 concern (RFC 020).

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
| `integration-tests.yml` | PR + push to main | real-network tests; only fork PRs may skip without the secret |
| `benchmarks.yml` | nightly cron (06:00 UTC) + manual | credential preflight, macro bench suite, uploads JSON artefacts |

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

The `benchmarks.yml` workflow runs every night at 06:00 UTC with default
parameters (10MB file transfer × 5, 1000 SyncedStore ops, 500 request/reply
round trips). Results go to the workflow's **Artefacts** section as a
JSON bundle, retained 30 days.

Before compiling and running that matrix, CI runs a three-minute real-tailnet
identity test. A missing, expired, revoked, or mis-scoped key therefore fails
quickly with a focused error instead of wasting the whole benchmark job.

To run on-demand with custom parameters:
- Actions → **Benchmarks (nightly + on-demand)** → **Run workflow**
- Tweak `file_transfer_size` / `synced_store_ops` / `request_reply_count`

For absolute performance numbers or regression alerts on PRs,
[RFC 023](rfcs/020-ci-testing.md#9-follow-ups-deferred) proposes a
dedicated self-hosted runner + `benchmark-action/github-action-benchmark`
integration.

---

## Design References

- [RFC 019 — Local Testing and Benchmarking Harness](rfcs/019-local-testing-and-benchmarking.md)
- [RFC 020 — CI Testing Harness](rfcs/020-ci-testing.md)
- [RFC 012 — Layered architecture](rfcs/012-layered-architecture-redesign.md)
- [RFC 016 — SyncedStore](rfcs/016-synced-store-and-request-reply.md)
