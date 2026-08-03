# RFC 020: CI Testing Harness

**Status:** Draft
**Author:** James + Claude
**Date:** 2026-04-21

---

## 1. Problem Statement

[RFC 019](019-local-testing-and-benchmarking.md) landed a working real-Tailscale
test and benchmark harness that a developer can run on their laptop with one
env var set. Within hours of its first run, it caught a user-facing
file-transfer regression (the ephemeral-port bug fixed in
commit `cb88c27`) that would otherwise have shipped. The harness works.

But it only catches regressions when a developer *thinks to run it*. The
regression existed for weeks before being caught because no automated
pipeline exercised the real-network path. Mock-only CI passed clean the
whole time.

This RFC extends the harness to run automatically in GitHub Actions on
every PR and every main-branch push, plus a nightly macro-benchmark job
that tracks performance over time. Phase 2.1 (this RFC) covers
single-runner integration + nightly benchmarks. Multi-runner tests,
OAuth-based key minting, and PR-level benchmark trend tracking are
deferred to later phases.

---

## 2. Goals

1. **Every PR runs the real-network integration suite** — 16 tests, ~40s
   on a clean tailnet, fail the build if any regress.
2. **Benches compile on every PR** — no full bench runs (slow/noisy on
   hosted runners), just `cargo bench --no-run` to catch API drift and
   lint-level issues.
3. **Nightly macro benchmarks** — full `truffle-bench` matrix, results
   uploaded as artifacts for offline inspection. Also available via
   manual `workflow_dispatch` when the user wants numbers on-demand.
4. **Fork PRs still pass** — GitHub doesn't pass secrets to fork PRs.
   The existing env-gate makes real-network tests skip cleanly; the
   rest of the suite runs normally on fork CI.
5. **Setup is one secret** — `TRUFFLE_TEST_AUTHKEY` in GitHub →
   Settings → Secrets. No OAuth flow, no headscale, no runner
   bootstrapping.

## 3. Non-Goals

- **Multi-runner cross-machine tests.** Two GHA runners on the same tailnet
  talking to each other is doable but adds rendezvous and matrix
  coordination complexity. Deferred to RFC 021.
- **OAuth-based just-in-time key minting.** Rotating the static auth key is
  manual for now. Migrating to OAuth client credentials is a pure
  secret-management win with no functional change to the harness.
  Deferred to RFC 022.
- **PR-level benchmark regression alerts.** `benchmark-action/github-action-benchmark`
  can post bench deltas on PR comments, but hosted-runner noise (±20-40%)
  would drown small regressions. Deferred to RFC 023 where we pair it
  with a dedicated self-hosted runner.
- **Absolute performance numbers.** Hosted runners are noisy. Nightly
  numbers are *directional* — "did this commit cause a 2× regression?"
  not "truffle does X MB/s". Phase 3 concern.
- **Running on macOS / Windows runners.** Free GHA minutes are 10×
  costlier on macOS and the current Layer-3/4 tests already cover the
  per-OS path via the existing `cargo-test` matrix. Start with Ubuntu.

---

## 4. Design Overview

```
 ┌──────────────────────────────────────────────────────────────────┐
 │  existing ci.yml (PR + main)                                     │
 │   - check (pnpm lint, format, build, test)                       │
 │   - cargo-lint (fmt, clippy, doc warnings)                       │
 │   - cargo-deny (supply chain)                                    │
 │   - cargo-test matrix (ubuntu, macos, windows)                   │
 │   + bench compile-check (new: `cargo bench --no-run`)            │
 └──────────────────────────────────────────────────────────────────┘

 ┌──────────────────────────────────────────────────────────────────┐
 │  NEW integration-tests.yml (PR + main)                           │
 │   - ubuntu-latest, single job                                    │
 │   - checkout, setup rust+go, rust-cache                          │
 │   - set env TRUFFLE_TEST_AUTHKEY from secret                     │
 │   - cargo test -p truffle-core --tests                           │
 │   - if secret missing (fork PRs): tests skip gracefully          │
 └──────────────────────────────────────────────────────────────────┘

 ┌──────────────────────────────────────────────────────────────────┐
 │  NEW benchmarks.yml (nightly cron + workflow_dispatch)           │
 │   - ubuntu-latest                                                │
 │   - cargo run -p truffle-bench -- file-transfer 10M x5           │
 │   - cargo run -p truffle-bench -- synced-store b1 1000 ops       │
 │   - cargo run -p truffle-bench -- request-reply 500x             │
 │   - upload target/bench-results/ as artefact (30d retention)     │
 └──────────────────────────────────────────────────────────────────┘
```

---

## 5. Secrets

### 5.1 `TRUFFLE_TEST_AUTHKEY` (required)

**What:** A Tailscale auth key that lives in GitHub's repository secrets.

**How to generate:**
1. Go to <https://login.tailscale.com/admin/settings/keys>.
2. Click **Generate auth key**. Configure:
   - ✅ **Reusable** — CI runs use this key hundreds of times.
   - ✅ **Ephemeral** — nodes auto-clean within ~1 hour of job end.
   - ✅ **Pre-approved** — avoids a manual approve step for every new node.
   - ❌ **Tags** — leave unset unless your tailnet ACL has a matching
     `tagOwners` entry for `tag:truffle-test`. The key's own tag applies
     regardless.
   - **Expiration:** 90 days (the maximum). Rotation process below.
3. Copy the `tskey-auth-...` string.

**How to install:**
1. Repo on GitHub → Settings → Secrets and variables → Actions → **New
   repository secret**.
2. Name: `TRUFFLE_TEST_AUTHKEY`.
3. Value: paste the key.

**Rotation:**
- Calendar reminder for 80 days after creation.
- Generate a new key, update the secret, revoke the old key.
- OAuth (RFC 022) will automate this.

### 5.2 Why not `tailscale/github-action@v4`?

The official action is designed to put the *runner* on the tailnet (for
SSH tunneling, accessing tailnet-only resources, etc.). We don't need
that — truffle's own Go sidecar handles tsnet registration directly,
and the pair harness spins up *two* tsnet nodes within one runner, not
one runner itself joining a tailnet.

Using `tailscale/github-action` would add a step with no benefit and a
different OAuth surface than what the pair harness expects. Skip it.

---

## 6. Workflows

### 6.1 `integration-tests.yml`

```yaml
name: Integration tests (real Tailscale)
on:
  push:
    branches: [main]
  pull_request:
    branches: [main]
permissions:
  contents: read
jobs:
  real-network:
    runs-on: ubuntu-latest
    timeout-minutes: 15
    steps:
      - uses: actions/checkout@v5
      - uses: dtolnay/rust-toolchain@stable
      - uses: Swatinem/rust-cache@v2
      - uses: actions/setup-go@v6
        with: { go-version: '1.26.5', cache-dependency-path: packages/sidecar-slim/go.sum }
      - name: Run real-network integration tests
        env:
          TRUFFLE_TEST_AUTHKEY: ${{ secrets.TRUFFLE_TEST_AUTHKEY }}
        run: cargo test -p truffle-core --tests --verbose
```

Behaviour:
- **With secret** (main branch, same-repo PRs): all 16 integration tests
  run for real.
- **Without secret** (fork PRs): tests print `[skip] ... TRUFFLE_TEST_AUTHKEY
  not set` and pass. Fork authors still get a green check from their
  PR without us leaking the secret.

### 6.2 `benchmarks.yml`

```yaml
name: Benchmarks (nightly + on-demand)
on:
  schedule:
    - cron: '0 6 * * *'  # daily 06:00 UTC
  workflow_dispatch:
permissions:
  contents: read
jobs:
  macro:
    runs-on: ubuntu-latest
    timeout-minutes: 30
    steps:
      - uses: actions/checkout@v5
      - uses: dtolnay/rust-toolchain@stable
      - uses: Swatinem/rust-cache@v2
      - uses: actions/setup-go@v6
        with: { go-version: '1.26.5', cache-dependency-path: packages/sidecar-slim/go.sum }
      - name: File transfer (10M x5)
        env: { TRUFFLE_TEST_AUTHKEY: ${{ secrets.TRUFFLE_TEST_AUTHKEY }} }
        run: cargo run -p truffle-bench --release -- file-transfer --size 10M --iterations 5
      - name: SyncedStore B1 (1000 ops)
        env: { TRUFFLE_TEST_AUTHKEY: ${{ secrets.TRUFFLE_TEST_AUTHKEY }} }
        run: cargo run -p truffle-bench --release -- synced-store --scenario b1-sequential --ops 1000
      - name: Request/reply RTT (500 count)
        env: { TRUFFLE_TEST_AUTHKEY: ${{ secrets.TRUFFLE_TEST_AUTHKEY }} }
        run: cargo run -p truffle-bench --release -- request-reply --count 500
      - uses: actions/upload-artifact@v4
        with:
          name: bench-results
          path: target/bench-results/
          retention-days: 30
```

Notes:
- Fails the workflow if any bench subcommand errors (deliberate — macro
  benches hard-fail on missing auth key, unlike tests).
- Artefacts include git SHA in each JSON, so you can correlate numbers
  with commits after the fact.
- No PR comment/trend UI yet; that's RFC 023.

### 6.3 Change to `ci.yml`

Add one step to the existing `cargo-test` job:

```yaml
      - name: Bench compile check
        run: cargo bench -p truffle-core --no-run
```

Catches bench API drift without running the full criterion loops
(~30s extra compile time vs ~2 minutes of actual benchmarking).

---

## 7. Failure Modes and Mitigations

| Failure | Likelihood | Response |
|---|---|---|
| Tailscale control-plane outage during a run | Low | Workflow fails. Re-run from Actions UI. Don't auto-retry (masks real issues). |
| Device limit hit (50 tagged devices, free plan) | Low (ephemeral nodes expire ~1h) | Wait 1h and re-run. If recurring, upgrade plan or migrate to dedicated CI tailnet. |
| Auth key revoked / expired | Medium (90-day expiry) | Generate new key, update GitHub secret. Calendar reminder at 80 days. |
| Fork PR tries to run integration | Expected | Integration tests skip cleanly; other CI passes. No action. |
| Integration test genuinely flaky | Unknown | First offence: open an issue, not a revert. Flaky-test budget is 0. |
| Hosted runner noise breaks macro bench thresholds | High | No thresholds in phase 2.1 — just collecting raw numbers. RFC 023 adds thresholds + self-hosted runners. |
| Secret accidentally logged | Low | GHA auto-redacts secret env vars. Pre-commit scanner catches commits. |

---

## 8. Rollout

1. **Land this RFC + the three workflow files.** Integration workflow
   runs on PRs immediately — but until the secret is configured, every
   run skips all 16 tests. That's expected and safe.
2. **Configure the `TRUFFLE_TEST_AUTHKEY` secret.** From this point,
   main-branch pushes and same-repo PRs get real-network coverage.
3. **Verify one integration run end-to-end.** Merge any trivial PR,
   watch the workflow complete, ensure 16 tests pass.
4. **Trigger `benchmarks.yml` manually.** Confirm the artefact uploads.
5. **Wait for first scheduled nightly run.** Confirm cron fires.
6. **Add a calendar reminder** at T+80 days to rotate the key.

No flag day, no phased rollout — each component is independently useful
and independently toggleable.

---

## 9. Follow-ups (Deferred)

- **RFC 021: Multi-runner cross-machine tests.** Two GHA runners on the
  same tailnet, each running one end of the pair. Catches bugs that
  same-process pair harness can't (real OS networking, routing).
- **RFC 022: OAuth-based key minting.** Replace the static auth key
  with an OAuth client-credentials exchange; mint ephemeral keys
  just-in-time per run. Eliminates the 90-day rotation chore.
- **RFC 023: Benchmark regression detection.** `benchmark-action/github-action-benchmark`
  posting PR comments + dedicated self-hosted runner for stable numbers.
- **RFC 024: Headscale fallback backend.** Fully offline CI using a
  self-hosted control plane. Useful for airgapped contributors and for
  removing the Tailscale SaaS dependency from CI.
