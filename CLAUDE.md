# Truffle — Mesh Networking Library

## What This Is

Mesh networking for local-first apps built on Tailscale. Devices discover each other, exchange messages, sync state, transfer files — no central server.

## Architecture

Clean layered architecture (RFC 012):

```
Layer 7: Applications   — CLI commands, TUI, daemon handler
         Node API       — 16-method public API + FileTransfer + SyncedStore
Layer 6: Envelope       — namespace-routed message framing (JSON codec)
Layer 5: Session        — PeerRegistry, lazy WS connections, event broadcast
Layer 4: Transport      — WebSocket, TCP, UDP, QUIC transports
Layer 3: Network        — TailscaleProvider (peer discovery via WatchIPNBus)
         Go Sidecar     — tsnet integration, WireGuard tunnels
```

Key principles:
- Layer 3 (WatchIPNBus) is the source of truth for peers — NOT transport connections
- `PeerState.ws_connected` tracks WebSocket state; `PeerState.online` tracks Tailscale status
- Subsystems (FileTransfer, SyncedStore) live in core, built on Node primitives, own their namespace
- `send_typed()`/`broadcast_typed()` for explicit msg_type — subsystems use these, not `send()`

## Crates

| Crate | Purpose |
|-------|---------|
| `truffle-core` (~12k LOC) | Layers 3-6, Node API, file transfer, synced store, request/reply |
| `truffle-cli` (~10k LOC) | 13 CLI commands + interactive TUI (ratatui + crossterm) |
| `truffle-sidecar` | Build-time sidecar download helper |
| `truffle` | Thin re-export crate |
| `truffle-napi` | NAPI-RS bridge for Node.js |
| `truffle-tauri-plugin` | Tauri v2 desktop plugin |
| `truffle-bench` (`benchmark/engine`) | Benchmark engine (lib) + CLI (bin): workloads, peer control protocol, v3 reports, suites (RFC 027). `publish = false` |
| `truffle-bench-app` (`benchmark/app/src-tauri`) | Tauri v2 shell of the "truffle bench" desktop app; frontend is `@vibecook/truffle-bench-app` (`benchmark/app`, React + Vite, private). `publish = false` |

Each crate's place in the RFC 012 stack and its domain are declared in `atlas/` or its crate root and graded by Atlas (see "Atlas" below).

## Build & Test

```bash
cargo build                   # default members only (no GUI crates)
cargo test --workspace --exclude truffle-bench-app
```

## Pre-commit Hook (one-time setup per clone)

```bash
git config core.hooksPath .githooks
```

Enables `.githooks/pre-commit`, which catches issues before they reach CI:
- **NAPI binding drift**: if `crates/truffle-napi/src/` changed, auto-regenerates `index.d.ts` / `index.js` and blocks the commit if they're not staged (this is what went wrong in 0.3.17 → shipped stale types).
- **Rustfmt** on staged `.rs` files.

Skip with `git commit --no-verify` when you need to (rare).

## Release Workflow

1. Push `feat:` or `fix:` commit to main
2. release-please creates/updates a release PR
3. Checkout release branch, bump Cargo.toml versions (`sed -i '' 's/^version = "OLD"/version = "NEW"/'`), push
4. Wait for CI, merge PR with `--admin`
5. Binary builds (CLI + sidecar) auto-trigger via `actions: write` permission
6. Use `--ref truffle-v<VERSION>` if manually triggering workflows (NOT `--ref main`)

## Key Conventions

- **Namespaces**: `"ft"` = file transfer, `"ss:{id}"` = synced store, `"chat"` = messaging
- **Wire protocol**: Envelope with namespace + msg_type + JSON payload over WebSocket
- **Windows TUI**: Must filter `KeyEventKind::Press` — crossterm emits Press+Release on Windows
- **Tests**: Use `MockNetworkProvider` + `make_test_node()` pattern; mock can't do real WS between nodes
- **CI excludes**: truffle-tauri-plugin, truffle-napi and truffle-bench-app from clippy, doc, nextest and doctests (need platform-specific tooling); all three are also in `deny.toml` `[graph] exclude`. Add `--exclude truffle-bench-app` to any new `--workspace` command

## Benchmarks

Everything lives under `benchmark/` or is linked from [benchmark/README.md](benchmark/README.md) (RFC 027):
- `benchmark/engine` — `truffle-bench` engine + CLI: `serve`, `peers`, `run`, `pair`, `suite`, `report`, legacy `transport`
- `benchmark/app` — the truffle bench desktop app (Tauri v2); `benchmark/schema` — generated v3 report schema + fixtures (`UPDATE_SCHEMA=1` regenerates)
- Linked, not moved: criterion benches in `crates/truffle-core/benches`, `packages/sidecar-slim/cmd/tsnet-bench`, `examples/quic-streams` `bench`

```bash
pnpm bench:micro                                                          # criterion
cargo run --locked -p truffle-bench --release -- suite --profile smoke --pair   # needs an auth key
cargo run --locked -p truffle-bench --release -- serve                    # responder; login URL without a key
pnpm --filter @vibecook/truffle-bench-app app:dev                         # the app (profile "dev")
```

- Keys: `--auth-key-file`, `TRUFFLE_BENCH_AUTH_KEY`, `TRUFFLE_TEST_AUTHKEY`, `TS_AUTHKEY` — never argv; `pair`/`suite --pair` require one
- **Commit types**: release-please ignores a commit only if *every* file is under `benchmark/` (`exclude-paths`). Hub changes that touch anything else use `chore(bench):`, `ci:`, `build:` or `docs:` — never `feat:`/`fix:`. Library changes the hub needs ship separately as `feat(core):`
- Nightly: `benchmarks.yml` (`suite --profile nightly --pair`); PR: `integration-tests.yml` runs `-p truffle-bench --tests` (`live_pair`)

## Atlas

`atlas/*.atlas` plus `@atlas` comment lines declare what code cannot say about itself: the RFC 012 layer stack, domains, runtime links, contracts, four hot-path flows, dependency rules and RFC decisions. Project Atlas grades each statement against the code. It is not a third living document: every rule and decision cites this file, `docs/`, a README or a comment in the code with a checked `see "file#slug"` (`#L<n>` for a line). Authoring guide: the `atlas` skill.

- **Where**: truffle-napi, truffle-tauri-plugin, truffle-bench and truffle-bench-app end their root `//!` doc (the benchmark crates' `lib.rs`) with their placement; seams and flow steps carry `// @atlas` lines (`grep -rn '@atlas' crates benchmark`). The crates.io crates (truffle-core, truffle, truffle-sidecar) carry no `//!` atlas text, so docs.rs shows none: they, truffle-cli (no crate doc), truffle-core's module tiers, the npm packages (`@vibecook/truffle-bench-app` included) and the directories (`benchmark/schema/` included) are placed in `atlas/layers.atlas` and `atlas/domains.atlas`.
- **New code**: place a new crate, truffle-core module, npm package or top-level directory in a layer and a domain in the same change. A truffle-core module you forget lands in node-api and `mesh` and nothing reports it: `explain` it.
- **Renames**: update every selector that names the code, in `atlas/` and in other code's `@atlas` lines (brace forms like `mod:truffle_core::{node, identity, egress}` hide from grep). `check` must then show 0 DANGLING. Once a baseline exists, moving the code of a known finding also makes it new (the skill's "Known or new").
- **Never weaken a rule to make it pass**: fix the code. Widening an `only` list, adding an `except`, moving a module to another tier or changing what a rule or decision means is the user's call, even when `check` suggests it. `atlas/atlas.baseline.json` is written only by `atlas baseline`, and `atlas baseline` runs only when the user decides to accept today's violations, never to turn the check green.
- **Check** from the repo root after changing crates, modules, packages, dependencies, `atlas/`, or a heading in CLAUDE.md, a README or `docs/` (`see` cites them by slug): `TRUFFLE_SIDECAR_SKIP_DOWNLOAD=1 npm --prefix "${ATLAS:-$HOME/Projects/project100/protos/project-atlas}" run -s atlas -- check "$PWD" --intent "$PWD/atlas"`. It runs `cargo doc` over the workspace (a build, build scripts included); the variable keeps truffle-sidecar's build script offline, as CI's lint job does (`cargo doc` never runs the sidecar). Atlas is a private tool: if `$ATLAS/package.json` is missing (the command exits 254 with no output), skip the check, install nothing, and say so. When builds are off-limits, run the syntax-only check: `ATLAS="${ATLAS:-$HOME/Projects/project100/protos/project-atlas}"; "$ATLAS/node_modules/.bin/tsx" "$ATLAS/packages/engine-intent/src/cli.ts" atlas crates benchmark packages examples`.
- **Reading it**: exit 1 = a new error violation. Until the user runs `atlas baseline`, the check exits 1 on a clean tree with 7 findings kept for the user (listed in the skill). Your change is clean only when VIOLATED shows those same 7 claims with the same counts (the exit code is 1 either way, and a further edge into one of the 7 only raises its count); do not change code, rules or tiers for them unless asked. DANGLING does not fail the check but must be fixed, and a dangling rule, contract or flow named in a decision's `confirmed-by` makes that decision violated (exit 1; `layered-architecture` and `synced-store` are violated already, so for their members only DANGLING and the decision's evidence show it): fix the selector.
- **Commits**: an atlas-only change is `docs:` or `chore:`, never `feat:`/`fix:`, which make Release Please cut a release. Staged `@atlas` lines still trigger the pre-commit hook: a staged truffle-napi `src/*.rs`, `Cargo.toml` or `build.rs` runs a release `napi build` (and fails if pnpm is missing); a staged truffle-core `lib.rs`/`node.rs` or truffle `lib.rs`/`Cargo.toml` runs `cargo test -p truffle --doc`; every staged `.rs` gets rustfmt (the skill lists all 7 checks). Never put `@atlas` in a `///` line: rustdoc renders it, and NAPI-RS copies item docs into `index.d.ts`.

## RFCs

RFCs live in `docs/rfcs/`. Key ones:
- **012**: Layered architecture redesign
- **013**: TUI + agent CLI dual-mode
- **014**: File transfer in core (not Layer 7)
- **015**: NAPI-RS and Tauri plugin rewrite
- **016**: SyncedStore + request/reply (Phase 1-2 done, Phase 3 persistence pending)
- **027**: Benchmark hub (`benchmark/`: engine, CLI, desktop app)
