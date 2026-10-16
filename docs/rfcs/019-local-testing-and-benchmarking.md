# RFC 019: Local Testing and Benchmarking Harness — ⚠️ Partially Stale

**Status:** Draft (partially superseded)
**Author:** James + Claude
**Date:** 2026-04-21

> **Stale note (2026-07-07):** The CrdtDoc / Loro feature (RFC 018) was removed.
> References below to `src/crdt_doc/tests.rs`, `benches/crdt_merge.rs`, and "CRDT
> merge/convergence" no longer apply. SyncedStore (RFC 016) remains, and the
> SyncedStore testing/benchmarking sections are still current — it uses
> last-write-wins device slices, not a CRDT.

> **Superseded in part (2026-10-16): §10.2 → [RFC 027](027-benchmark-hub.md).**
> The Tier-2 macro benchmarks are now the RFC 027 benchmark hub. `truffle-bench`
> moved to `benchmark/engine` (package name kept) and became a library plus a
> CLI; its `file-transfer`, `synced-store` and `request-reply` subcommands were
> replaced by `truffle-bench pair <workload>` and `suite --profile <p>`
> (request/reply is now the `ws` workload). `transport` remains as a legacy
> subcommand. The §12 file table and §13.4 below describe the original plan and
> are not updated. Current docs: [benchmark/README.md](../../benchmark/README.md).

---

## 1. Problem Statement

Truffle's test suite is substantial (~199 tests, ~4,253 LOC in test files) but it has two structural gaps that prevent it from providing the assurance a production-grade mesh-networking library needs.

**Gap 1: Real-network coverage is limited to Layers 3 and 4.** Only `tests/integration_network.rs` and `tests/integration_transport.rs` exercise a real Tailscale network. Every test for SyncedStore (`src/synced_store/tests.rs`, 616 LOC), CRDT merge (`src/crdt_doc/tests.rs`, 674 LOC), session, request/reply, and the Node API uses `MockNetworkProvider`, which — per CLAUDE.md — "can't do real WS between nodes." This means Truffle's flagship feature (two peers converging on a shared document over an encrypted mesh) is **not tested end-to-end against a real network today**.

**Gap 2: Running the real-network tests is a manual, friction-heavy process.**
- Integration tests are `#[ignore]`'d, so default `cargo test` skips them silently.
- They require a manually-built Go sidecar at a specific path.
- They require an authenticated Tailscale session, which today means either a pre-set `TS_AUTHKEY` env var (undocumented) or an interactive browser login that blocks automation.
- They hardcode a single known peer IP (`100.126.82.98`, an EC2 server), so running them without that external peer being up will fail in misleading ways.
- There are no benchmarks at all — no `benches/` directory, no criterion setup, no way to track performance regressions.

**This RFC addresses only the local phase.** A follow-up RFC will tackle CI automation (OAuth key minting, multi-runner matrix, Headscale fallback). Getting the local harness right first is a prerequisite: the CI harness will be a thin wrapper around the same primitives.

---

## 2. Goals

1. **Zero-setup default.** `cargo test` and `cargo bench` work on a fresh clone with no Tailscale account. Real-network tests/benches skip gracefully with a single-line notice; all other tests pass normally.

2. **One-variable opt-in.** Setting a single environment variable (`TRUFFLE_TEST_AUTHKEY`) is the only manual step a developer needs to take before real-network tests and benchmarks run end-to-end, automatically, headlessly.

3. **Two-node coverage without external peers.** The test harness spins up two independent tsnet nodes on the developer's machine, gives them distinct identities, and exercises the full stack (Layers 3–7) between them. The EC2 hardcode is eliminated. Any dev can run the full real-network suite on a plane.

4. **CRDT convergence as a flagship integration test.** SyncedStore convergence, request/reply, and file-transfer correctness are tested against a real tailnet — not only against mocks. These become the headline tests that prove Truffle actually works over real networks.

5. **Measurable performance.** A criterion-based micro-benchmark suite runs on every dev machine without Tailscale. A separate macro-benchmark binary drives real two-node workloads and emits structured numbers for throughput, latency tails, and convergence time.

6. **No secret leakage.** Auth keys live only in environment variables and gitignored dotfiles. Secrets never appear in logs, never in committed files, never in error messages.

## 3. Non-Goals

- **CI automation.** Deferred to RFC 020 (Phase 2). This RFC provides the primitives; CI is a consumer.
- **OAuth-based key minting.** Long-lived dev keys are fine for local work. OAuth is for CI, where secret rotation matters.
- **Multi-machine real-network tests.** Two nodes on one machine is the scope here. Running A and B on different physical hosts is Phase 2.
- **Headscale integration.** Useful later for fully-offline CI; adds ops weight that local dev doesn't need.
- **Absolute performance numbers.** Local benchmarks track regressions on your own machine. Cross-machine "Truffle does X MB/s" numbers need dedicated hardware — a later phase.
- **Rewriting existing mock-based unit tests.** The mocks stay. Real-network tests are additive coverage, not a replacement.
- **Chaos/fault injection.** `tc netem`, Toxiproxy, and similar belong in the CI phase where we control the environment.

---

## 4. Design Overview

The harness has four layered primitives, each building on the previous:

```
┌──────────────────────────────────────────────────────────────────┐
│  Layer D: Benchmark binary (truffle-bench crate)                 │
│           Drives workloads, emits structured results             │
├──────────────────────────────────────────────────────────────────┤
│  Layer C: Integration tests (tests/integration_*.rs)             │
│           SyncedStore convergence, file transfer, request/reply │
├──────────────────────────────────────────────────────────────────┤
│  Layer B: Two-node pair harness (common::make_pair_of_nodes)     │
│           Spins up two tsnet providers with distinct identities  │
├──────────────────────────────────────────────────────────────────┤
│  Layer A: Environment + skip plumbing (common::require_authkey)  │
│           Reads TRUFFLE_TEST_AUTHKEY, gates real-network work    │
└──────────────────────────────────────────────────────────────────┘
```

**Single source of authentication.** Every real-network test and benchmark reaches for the same `require_authkey()` helper. There is exactly one way to provide a key (one env var) and one way to skip (helper returns `None`). No `#[ignore]`, no feature flags, no separate test binaries.

**Pair harness is load-bearing.** Both the integration tests (Layer C) and the macro benchmarks (Layer D) use the same two-node setup. This means workload code written for tests can run as a bench and vice versa; fixes to one benefit the other.

---

## 5. Environment Contract

### 5.1 Variables

| Variable | Required? | Purpose | Default |
|----------|-----------|---------|---------|
| `TRUFFLE_TEST_AUTHKEY` | Only for real-network tests/benches | Tailscale auth key used by test/bench nodes | Unset → skip |
| `TS_AUTHKEY` | Legacy fallback only | Read if `TRUFFLE_TEST_AUTHKEY` is missing. Deprecated; prints a warning. | Unset |
| `TRUFFLE_TEST_TAGS` | No | Comma-separated ACL tags for test nodes | `tag:truffle-test` |
| `TRUFFLE_TEST_EPHEMERAL` | No | `true`/`false`; whether to register as ephemeral | `true` |
| `TRUFFLE_TEST_LOG` | No | tracing filter for test/bench output | `truffle_core=info` |
| `TRUFFLE_TEST_STATE_ROOT` | No | Root dir for per-node state (cleaned up after each test) | `$TMPDIR/truffle-test-{uuid}` |

### 5.2 Loading

Tests and benches load environment via `dotenvy::dotenv().ok()` at harness startup. This means a developer can drop a `.env` at the repo root with:

```env
TRUFFLE_TEST_AUTHKEY=tskey-auth-...
```

and `cargo test`/`cargo bench` will pick it up automatically, with no shell export needed.

A `.env.example` is committed to the repo showing the expected variables without any values.

### 5.3 Secret hygiene

- `.env` added to `.gitignore` (verify current state; add if absent).
- Auth key is never logged. Tracing code emits only a boolean `authkey_present=true` or a redacted prefix (`tskey-auth-****`).
- A `scripts/check-for-secrets.sh` script greps staged files for `tskey-` on pre-commit. The existing `.githooks/pre-commit` is extended; opt-in remains `git config core.hooksPath .githooks`.

### 5.4 Auth-key requirements

The key must be:
- **Reusable** — tests may mint many devices per run
- **Ephemeral** — nodes auto-expire on disconnect (30–60 min after last activity)
- **Tagged** — requires the tailnet ACL to include `tag:truffle-test` as an owner-less tag. Documented in `docs/TESTING.md`.
- **Pre-approved** (if device approval is enabled on the tailnet) — avoids manual admin approval for each test node.

`docs/TESTING.md` includes a direct link to `https://login.tailscale.com/admin/settings/keys` with the exact checkbox configuration.

---

## 6. Skip-Gracefully Contract

### 6.1 Removal of `#[ignore]`

All existing `#[ignore]` attributes on real-network tests are **removed**. Replacement: a runtime check at the top of each test that returns early if no auth key is available.

### 6.2 Shared helper

A new file `crates/truffle-core/tests/common/mod.rs`:

```rust
pub fn require_authkey(test_name: &str) -> Option<String> {
    let key = std::env::var("TRUFFLE_TEST_AUTHKEY").ok()
        .filter(|k| !k.is_empty())
        .or_else(|| {
            let legacy = std::env::var("TS_AUTHKEY").ok().filter(|k| !k.is_empty());
            if legacy.is_some() {
                eprintln!("[warn] using TS_AUTHKEY; TRUFFLE_TEST_AUTHKEY is preferred");
            }
            legacy
        });

    match key {
        Some(k) => Some(k),
        None => {
            eprintln!("[skip] {test_name}: TRUFFLE_TEST_AUTHKEY not set");
            None
        }
    }
}

#[macro_export]
macro_rules! skip_without_authkey {
    () => {
        let __test_name = {
            fn f() {}
            fn type_name_of<T>(_: T) -> &'static str { std::any::type_name::<T>() }
            let n = type_name_of(f);
            &n[..n.len() - 3]
        };
        let Some(_authkey) = $crate::common::require_authkey(__test_name) else { return; };
    };
}
```

### 6.3 Usage

Every real-network test becomes:

```rust
#[tokio::test]
async fn test_provider_start_and_auth() {
    skip_without_authkey!();
    let (a, _b) = common::make_pair_of_nodes().await;
    assert!(a.local_identity().await.ip.is_some());
    // No manual cleanup — Drop impl on the pair handles it
}
```

### 6.4 Default experience

- `cargo test` → all tests compile, mock-based tests run, real-network tests print `[skip] ...: TRUFFLE_TEST_AUTHKEY not set` and pass.
- `TRUFFLE_TEST_AUTHKEY=tskey-... cargo test` → same tests, real network.
- No command-line flags, no separate targets, no `--ignored`. The dev's shell environment is the switch.

---

## 7. Two-Node Pair Harness

### 7.1 Primitive

```rust
pub struct NodePair {
    pub alpha: TailscaleProvider,
    pub beta:  TailscaleProvider,
    // Drop impl stops both providers and removes their state dirs
    _guard: PairGuard,
}

pub async fn make_pair_of_nodes() -> NodePair { ... }

pub async fn make_pair_of_nodes_with(opts: PairOpts) -> NodePair { ... }
```

### 7.2 What it does

1. Reads `TRUFFLE_TEST_AUTHKEY` (caller has already passed `skip_without_authkey!()`).
2. Generates a single UUID for the test run (`run_id`).
3. Builds two `TailscaleConfig`s:
   - **Alpha**: hostname `truffle-test-{run_id}-alpha`, state dir `$TRUFFLE_TEST_STATE_ROOT/{run_id}/alpha`.
   - **Beta**: hostname `truffle-test-{run_id}-beta`, state dir `$TRUFFLE_TEST_STATE_ROOT/{run_id}/beta`.
   - Both `ephemeral = true`, same `app_id = "truffle-test"`, tags from env.
4. Starts both providers in parallel with a 30s timeout.
5. Subscribes to `peer_events` on both before starting; after start, waits until each has seen the other (up to 15s rendezvous timeout).
6. Returns the pair. Drop impl:
   - Calls `stop()` on both providers.
   - Removes state dirs (which triggers tsnet to discard identity on next start; combined with `ephemeral = true`, the control plane deregisters within seconds).

### 7.3 Why per-test unique `state_dir` matters

From the research: tsnet's `Dir` is sticky — if `tailscaled.state` exists, the stored identity is reused and `AuthKey` is ignored. For fresh-per-test nodes, each test gets a unique tmpdir. Combined with `Ephemeral: true`, the control plane discards the identity on logout anyway, but the unique dir prevents cross-test contamination locally.

### 7.4 Why the EC2 hardcode goes

Current tests dial `100.126.82.98:22` (the EC2 server's SSH port) to prove TCP works. The pair harness replaces this: alpha spawns a TCP listener on its Tailscale IP, beta dials it, they exchange a nonce. The test now depends on *nothing* external.

### 7.5 Rendezvous strategy

Tailscale netmap propagation is lazy — a newly-registered peer may not appear in the other's `peers()` immediately. The pair harness subscribes to `peer_events()` **before** calling `start()` so it catches the `Joined` event for the other side deterministically. If neither sees the other within 15s, the test fails with `PairRendezvousTimeout` and prints both sides' peer lists for diagnosis.

---

## 8. Test-Sidecar Build Automation

### 8.1 Current friction

Tests expect `crates/truffle-core/tests/test-sidecar` to exist. Today it's built manually with:

```bash
cd packages/sidecar-slim && go build -o ../../crates/truffle-core/tests/test-sidecar .
```

Missed this step → confusing "file not found" errors.

### 8.2 Solution: `build.rs`

Add `crates/truffle-core/build.rs` gated by a cargo feature `test-sidecar`:

```rust
fn main() {
    if std::env::var("CARGO_FEATURE_TEST_SIDECAR").is_err() {
        return;
    }
    // Skip if Go is not installed (print clear warning, let tests fail later with a helpful message)
    // Skip if test-sidecar binary is newer than any .go file in packages/sidecar-slim/
    // Otherwise: shell out to `go build -o tests/test-sidecar ...`
    // Emit cargo:rerun-if-changed for every .go file in packages/sidecar-slim/
}
```

`[dev-dependencies]` includes the `test-sidecar` feature so integration tests automatically trigger the build, but normal library consumers never pay for Go.

### 8.3 Failure mode when Go is missing

`build.rs` prints a clear warning and sets a cargo rerun. A helper `common::assert_sidecar_exists()` at the top of real-network tests fails with:

```
error: test-sidecar binary not found at <path>.
       This build requires Go 1.22+. Install Go and re-run cargo test,
       or run `just build-test-sidecar` manually.
```

Better than a generic "No such file or directory" surfacing from sidecar spawn.

### 8.4 Workspace hygiene

The `test-sidecar` feature is not in the default feature set. Published crates on crates.io don't trigger the build. Only local `cargo test` / `cargo bench` invocations do.

---

## 9. Integration Test Suite

Three new files join the existing two, each backed by the pair harness:

### 9.1 `tests/integration_synced_store.rs`

**This is the headline test file — it closes Gap 1 from §1.**

| Test | What it proves |
|------|----------------|
| `test_pair_converges_on_simple_writes` | A writes `{x: 1}`, B reads `{x: 1}` within 5s. |
| `test_pair_converges_on_concurrent_writes` | A writes `{x: 1}` and B writes `{y: 2}` within ~10ms of each other; both ultimately observe `{x: 1, y: 2}`. |
| `test_pair_survives_peer_reconnect` | A and B sync 100 writes; B disconnects (drop transport); B reconnects; B catches up to A's state. |
| `test_late_join_backfill` | A makes 100 writes alone; B joins; B reaches parity within 10s. |
| `test_pair_converges_under_churn` | A + B exchange 1000 writes while B's WS flips connected/disconnected every 100ms. Both converge once stable. |

### 9.2 `tests/integration_request_reply.rs`

| Test | What it proves |
|------|----------------|
| `test_basic_request_reply` | A→B request with string payload; correct reply returned. |
| `test_large_payload_roundtrip` | 1 MB payload in each direction, byte-exact. |
| `test_request_timeout_on_peer_drop` | B stops mid-request; A's `request()` resolves to `Err(Timeout)` within the configured window. |
| `test_concurrent_requests` | 100 in-flight A→B requests, each gets its correct reply (correlation IDs not crossed). |

### 9.3 `tests/integration_file_transfer.rs`

| Test | What it proves |
|------|----------------|
| `test_transfer_1mb` | Integrity of a 1 MB transfer (sha256 match on both sides). |
| `test_transfer_100mb` | Same for 100 MB. Validates chunking + resumption + hashing pipeline. |
| `test_transfer_cancel_mid_flight` | B cancels at ~50%; A's state is cleaned up; no tempfile leaks on disk. |
| `test_transfer_many_small` | 50 × 10 KB transfers in sequence. Catches per-transfer setup/teardown leaks. |

### 9.4 Existing `integration_network.rs` and `integration_transport.rs`

Rewritten to use the pair harness. The six existing tests (`test_provider_start_and_auth`, `test_peer_discovery`, `test_peer_events`, `test_dial_tcp`, `test_ping`, `test_health`) are preserved semantically but no longer reference the EC2 IP. `test_dial_tcp` and `test_ping` now target the peer node instead of an external host.

---

## 10. Benchmark Suite

### 10.1 Tier 1 — Micro benchmarks (criterion)

Location: `crates/truffle-core/benches/`.

Tooling: criterion 0.5. Run with `cargo bench -p truffle-core`. No Tailscale involvement.

| File | What it measures |
|------|------------------|
| `envelope.rs` | Envelope encode + decode throughput (bytes/sec) across payload sizes 64B / 4KB / 256KB / 4MB. |
| `crdt_merge.rs` | CRDT document merge at 1k / 10k / 100k ops. Catches algorithmic regressions in the merge path. |
| `chunk_hash.rs` | File chunker throughput (MB/s) and chunk sha256 throughput, varying chunk sizes. |
| `request_reply_loopback.rs` | In-process request/reply RTT using the in-memory transport (no Tailscale). Establishes the ceiling for §10.2 macro RTT. |

Criterion output lives in `target/criterion/` and is consumed by `benchmark-action/github-action-benchmark` in the CI phase (RFC 020). Zero cost to the default build.

### 10.2 Tier 2 — Macro benchmarks (`truffle-bench` binary)

Location: new crate `crates/truffle-bench/` with `publish = false`. Built when invoked, never as part of `cargo build --workspace` default output (kept out of default workspace members, or marked `publish = false` with a workspace member entry — decision in §13.4).

Structure: a clap CLI with subcommands, each driving the pair harness through Truffle's public API.

```
truffle-bench file-transfer --size 100MB --iterations 5
truffle-bench synced-store   --ops 10000 --scenario b1-sequential
truffle-bench request-reply  --count 1000 --payload 4KB
truffle-bench all
```

Shared infrastructure:
- Uses the same `make_pair_of_nodes()` from `truffle-core` test common (re-exported or duplicated — see §13.3).
- `hdrhistogram` for latency percentiles.
- Emits JSON to `target/bench-results/{timestamp}-{subcommand}.json`.
- Errors out clearly if `TRUFFLE_TEST_AUTHKEY` is missing (no silent skip — the user explicitly asked to run benchmarks).

#### 10.2.1 File transfer benchmark

Varies payload size (1MB, 10MB, 100MB, 1GB). For each, runs N iterations end-to-end and reports:
- Throughput p50/p95 (MB/s)
- Total wall time
- Memory high-water-mark on sender and receiver (via `/proc/self/statm` on Linux, `task_info` on macOS)

#### 10.2.2 SyncedStore / CRDT convergence benchmark

Scenarios ported from `dmonad/crdt-benchmarks`:
- **B1** — sequential writes: one node makes N writes, measure time for other node to reach parity.
- **B2** — two nodes each make N writes concurrently, measure total convergence time + bytes on wire.
- **B3** — N concurrent writers to the same slice (worst case for LWW conflict), measure how many updates survive.
- **B4** — real-world edit replay: deterministic transcript of a shared-doc session, replayed over the pair.

Metrics reported per scenario:
- Convergence latency (time from last producer op to all consumers quiesced).
- Total message count.
- Total bytes on wire.
- Peak update-queue depth.

#### 10.2.3 Request/reply benchmark

Drives N concurrent request/reply pairs across A↔B, reports p50/p99/p99.9 via hdrhistogram. Parameterized on payload size and concurrency.

### 10.3 What is deliberately excluded from local benchmarks

- **Absolute numbers as claims.** Local benchmarks exist to catch regressions on your own machine. Numbers are directional, not comparable cross-machine.
- **Chaos injection.** `tc netem` is Linux-specific and requires root/`NET_ADMIN`. Belongs in the Phase-2 CI docker-compose harness.
- **Long-running soak tests.** Local runs should complete in minutes, not hours. Soak is a Phase-3 concern.

---

## 11. Developer Workflow

### 11.1 One-time setup

From `docs/TESTING.md`:

1. Go to https://login.tailscale.com/admin/settings/keys → Generate auth key.
   Settings: **Reusable**, **Ephemeral**, **Tags: `tag:truffle-test`**, **Pre-approved** (if device approval is on).
2. Ensure your tailnet ACL grants `tag:truffle-test` as an ownerless tag (snippet in `docs/TESTING.md`).
3. `cp .env.example .env` and paste the key:
   ```
   TRUFFLE_TEST_AUTHKEY=tskey-auth-...
   ```
4. Install Go 1.22+ if not already installed (required for the test-sidecar `build.rs`).

### 11.2 Daily commands

| Command | Effect |
|---------|--------|
| `cargo test` | All tests. Real-network ones skip unless `.env` is set. |
| `cargo test -p truffle-core` | truffle-core tests, same skip rules. |
| `cargo bench -p truffle-core` | Micro benchmarks. No Tailscale needed. |
| `cargo run -p truffle-bench -- all` | Macro benchmarks (requires auth). |
| `just test-real` | Wrapper: extra logging + summary of skipped tests. |
| `just bench-all` | Wrapper: runs tier 1 then tier 2, collates JSON. |
| `just build-test-sidecar` | Force a fresh sidecar build (bypasses build.rs caching). |

### 11.3 Expected friction — and what we accept

- Developers without Tailscale accounts can still contribute to 95% of the codebase; real-network tests just skip.
- Developers who do set up Tailscale pay a one-time 10-minute setup cost. After that, every `cargo test` proves their changes work over real Tailscale.
- Build-time impact: `build.rs` shells out to `go build` on first test run (~5s), then caches via mtime. Subsequent runs are instant.

---

## 12. Affected Files

### 12.1 New files

| Path | Purpose |
|------|---------|
| `.env.example` | Template for local env vars |
| `docs/TESTING.md` | One-time setup + daily commands |
| `justfile` | Task shortcuts |
| `scripts/check-for-secrets.sh` | Pre-commit secret scan |
| `crates/truffle-core/build.rs` | Compiles test-sidecar when `test-sidecar` feature is on |
| `crates/truffle-core/tests/common/mod.rs` | `require_authkey`, `skip_without_authkey!`, `make_pair_of_nodes`, `PairGuard`, env loading |
| `crates/truffle-core/tests/integration_synced_store.rs` | Real-network CRDT convergence tests |
| `crates/truffle-core/tests/integration_request_reply.rs` | Real-network request/reply tests |
| `crates/truffle-core/tests/integration_file_transfer.rs` | Real-network file-transfer tests |
| `crates/truffle-core/benches/envelope.rs` | Criterion micro bench |
| `crates/truffle-core/benches/crdt_merge.rs` | Criterion micro bench |
| `crates/truffle-core/benches/chunk_hash.rs` | Criterion micro bench |
| `crates/truffle-core/benches/request_reply_loopback.rs` | Criterion micro bench |
| `crates/truffle-bench/` | New crate — macro benchmark binary |
| `crates/truffle-bench/Cargo.toml` | `publish = false`, binary target |
| `crates/truffle-bench/src/main.rs` | Clap CLI entry |
| `crates/truffle-bench/src/workloads/file_transfer.rs` | File-transfer workload |
| `crates/truffle-bench/src/workloads/synced_store.rs` | B1–B4 CRDT workloads |
| `crates/truffle-bench/src/workloads/request_reply.rs` | RTT workload |
| `crates/truffle-bench/src/harness.rs` | Thin wrapper over `make_pair_of_nodes` + hdrhistogram + JSON output |

### 12.2 Modified files

| Path | Change |
|------|--------|
| `.gitignore` | Add `.env`, `target/bench-results/`, `crates/truffle-core/tests/test-sidecar` |
| `.githooks/pre-commit` | Add a call to `scripts/check-for-secrets.sh` |
| `Cargo.toml` (workspace) | Add `truffle-bench` to `members`; register `criterion` and `dotenvy` and `hdrhistogram` as workspace deps |
| `crates/truffle-core/Cargo.toml` | Add `[features] test-sidecar = []`, add criterion/dotenvy/uuid as dev-deps, register `[[bench]]` entries, register integration test `required-features = ["test-sidecar"]` |
| `crates/truffle-core/tests/integration_network.rs` | Drop `#[ignore]`, drop EC2 hardcode, adopt pair harness, use `skip_without_authkey!` |
| `crates/truffle-core/tests/integration_transport.rs` | Same treatment |
| `crates/truffle-core/src/network/tailscale/provider.rs` | No API change — config already has `auth_key`, `ephemeral`, `tags`. Possibly relax logging to redact `auth_key` if not already done. |
| `docs/rfcs/README.md` if present | Link to RFC 019 |

---

## 13. Open Design Decisions

### 13.1 Criterion vs divan for micro benches

**Decision: criterion.** Well-established, HTML reports, compatible with `benchmark-action/github-action-benchmark` out of the box. Divan compiles faster and has a nicer API but is newer and less battle-tested. Revisit if criterion compile times become painful.

### 13.2 `build.rs` vs explicit `just build-test-sidecar`

**Decision: build.rs, gated by a cargo feature.** Zero friction once set up. Go installation is the only external prerequisite, and the failure message is clear. `just build-test-sidecar` remains available as an escape hatch.

### 13.3 Sharing `make_pair_of_nodes` between tests and benches

Options:
(a) Duplicate the helper in both `truffle-core/tests/common/` and `truffle-bench/src/harness.rs`.
(b) Extract to a new `truffle-test-harness` crate depended on by both.
(c) Put it in `truffle-core` behind a `test-harness` feature.

**Decision: (c), `truffle-core` with a `test-harness` feature.** It's too tightly coupled to `TailscaleProvider` internals to live elsewhere. Cargo features keep it out of release builds. Both the integration tests (via `dev-dependencies`) and `truffle-bench` (via `dependencies` + `features = ["test-harness"]`) consume the same implementation.

### 13.4 Where does `truffle-bench` live in the workspace?

**Decision: `crates/truffle-bench/` with `publish = false`, included in `[workspace.members]`.** Keeps it buildable with `cargo run -p truffle-bench`. Kept out of default `cargo build --workspace --release` output by `publish = false` and not being depended on by anything. CI can skip it with `--exclude truffle-bench` if needed.

### 13.5 Should `#[ignore]` be retained as belt-and-suspenders?

**Decision: no.** The runtime skip is sufficient, and keeping `#[ignore]` means two places to remember (`--ignored` flag + env var), which re-creates the exact friction we're solving. Dropping it is cleaner.

### 13.6 What about the `TS_AUTHKEY` fallback — how long do we keep it?

**Decision: keep for one minor version, warn on use, remove in the next.** Prevents a sudden break for anyone currently using it. Warning is a single-line `eprintln!` on test startup.

---

## 14. Risks and Mitigations

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Tailscale free-plan device cap (50 tagged devices) hit during heavy local testing | Low | Dev can't run tests until devices expire (30–60 min for ephemeral) | Ephemeral is the default; documented. If a dev hits the cap, they wait a minute. |
| `build.rs` triggers unexpectedly in library consumers' builds | Low | Surprising Go dependency | Feature-gated; not in default features; docs explain. |
| Pair harness rendezvous flakiness under high Tailscale control-plane load | Medium | Intermittent CI failures (Phase 2) | 15s rendezvous timeout, retry once, emit clear diagnostic on failure. |
| Auth key accidentally committed | Low | Public leak | `.env` gitignored, pre-commit scan, tests never log the key. |
| Concurrent test runs on one machine register conflicting hostnames | Low | Flaky tests | UUID per run baked into hostname; unique state dirs. |
| tsnet state dir survives across tests, reuses identity | Medium | Ghost peers in tailnet | Per-test `TempDir`, `ephemeral: true`, explicit cleanup in Drop. |
| Developer's system `tailscaled` picks up `TRUFFLE_TEST_AUTHKEY` from env | Very low | Dev's personal laptop joins test tailnet | Scoped env name avoids this; `TS_AUTHKEY` fallback emits warning. |
| macro benches give wildly variable numbers on busy dev machines | High | Hard to spot small regressions locally | Documented; local benches are for "did this PR 2× or 0.5× it" questions. Cross-run tracking is a Phase-2 concern. |
| Go not installed on dev's machine | Medium | `build.rs` fails with confusing error | Check for `go` binary early in build.rs, emit a specific install message. |

---

## 15. Implementation Plan

Nine steps, total estimate ~7–8 focused days. Each step is independently reviewable and lands with green `cargo test` / `cargo bench`.

| # | Step | Est. | Exit criteria |
|---|------|------|---------------|
| 1 | Env + skip plumbing | 0.5d | `.env.example`, `.gitignore` updates, `common/mod.rs` with `require_authkey` + `skip_without_authkey!`, `dotenvy` wired in. `cargo test` passes with no auth key (skips print correctly). |
| 2 | Two-node pair harness | 1d | `make_pair_of_nodes()` lands with `PairGuard`. Existing 6 tests in `integration_network.rs` + `integration_transport.rs` rewritten against it. EC2 hardcode deleted. With auth key: full pair suite green. |
| 3 | `build.rs` for test-sidecar | 0.5d | Fresh clone → `cargo test --features test-sidecar` builds the sidecar automatically. Missing-Go failure message is clear. |
| 4 | `integration_synced_store.rs` — **the flagship** | 1d | 5 new tests green with auth key. Gap 1 from §1 is closed. |
| 5 | `integration_request_reply.rs` + `integration_file_transfer.rs` | 1d | 8 new tests green with auth key. |
| 6 | Tier-1 micro benches (criterion) | 0.5–1d | `cargo bench -p truffle-core` runs envelope/crdt_merge/chunk_hash/request_reply_loopback, produces criterion HTML. |
| 7 | `truffle-bench` crate scaffold + file-transfer workload | 1d | `cargo run -p truffle-bench -- file-transfer --size 100MB` runs end-to-end, emits JSON. Pair harness re-used via `test-harness` feature. |
| 8 | SyncedStore (B1–B4) + request/reply macro workloads | 1d | `cargo run -p truffle-bench -- synced-store --scenario b1-sequential` and `request-reply` both produce numbers. |
| 9 | `docs/TESTING.md` + `justfile` + pre-commit secret scan | 0.5d | Fresh clone → a new dev can follow `TESTING.md` top-to-bottom and have the full suite running. Pre-commit blocks a staged file containing `tskey-`. |

Steps land in order. Step 4 is the highest-value individual commit because it closes the central coverage gap the project has today.

---

## 16. Follow-ups (Not in Scope)

- **RFC 020: CI Testing Harness.** OAuth-based key minting, `tailscale/github-action@v4`, matrix fan-out, rendezvous over GitHub artifacts or the tailnet itself. Reuses `make_pair_of_nodes` and the integration tests from this RFC without modification.
- **RFC 021: Multi-Runner Real-Network Tests.** Three-plus runner matrices for mesh-of-N scenarios. Builds on RFC 020.
- **RFC 022: Performance Tracking.** `benchmark-action/github-action-benchmark` or Codspeed for regression alerts. Needs dedicated hardware to produce stable absolute numbers.
- **RFC 023: Network Chaos.** `tc netem`, Toxiproxy, deterministic packet-loss / latency / bandwidth matrices. Lives in docker-compose, not on real Tailscale.
- **RFC 024: Headscale Fallback.** Fully-offline CI backend. `ControlURL` override on `TailscaleConfig`. Useful for airgapped development and for removing Tailscale SaaS as a CI dependency.
