---
name: atlas
description: Maintain truffle's architecture atlas (atlas/*.atlas plus @atlas comments, graded by Project Atlas) — place a new crate, truffle-core module, npm package or directory in the RFC 012 layer stack and a domain, keep selectors right through renames, write rules/contracts/flow steps/decisions that cite truffle's docs and RFCs, run `atlas check`, and fix dangling or violated claims.
---

# Atlas workflow

The atlas states what truffle's code cannot say about itself, and Project Atlas grades every
statement against the code: `verified`, `violated`, `asserted` (declared, nothing checkable),
`dangling` (a selector or reference matches nothing), `stale` (≥ 10 commits touched the code since
the claim's file last changed) or `ambiguous`. Truffle's own docs stay the authority: the atlas
cites them, never replaces them. Read CLAUDE.md "Atlas" first. The DSL spec is
`$ATLAS/docs/DSL.md` (§3 selectors, §4 statements, §5 in-code, §7 grading, §8 agent guide).

## Where it lives

- `atlas/system.atlas` — system, thesis, `let $subsystems` (truffle-core's file_transfer,
  synced_store, request_reply and proxy) and `let $shim` (the Go shim: sidecar-slim without the
  Go baseline bench in `cmd/`).
- `atlas/layers.atlas` — the RFC 012 stack (position is rank): truffle-core's module tiers
  (node-api, envelope, session, transport, network; `mod:truffle_core` makes node-api the default),
  the L7 sub-tiers (examples, cli, bench, react, js-api, bindings), the Go sidecar, Truffle Swift,
  distribution and tooling. It places the crates.io crates (truffle-core, truffle,
  truffle-sidecar), truffle-cli, the npm packages and the directories. `bench` holds the RFC 027
  hub: truffle-bench and truffle-bench-app place themselves, and this file adds the app's frontend
  `@vibecook/truffle-bench-app` and the generated `benchmark/schema/`.
- `atlas/domains.atlas` — the same crates, plus domain claims below crate level and `note`s for
  the Go files that do a domain's work. Each truffle-bench workload `touches` the domain it
  measures (`workloads::{tcp, udp, quic, video}` raw-transport, `path` mesh, `ws` messaging,
  `http` serving, `egress`, `file_transfer`, `synced_store`); no workload calls `request_reply`
  directly since `ws` replaced it.
- `atlas/components.atlas` — runtime units and how they talk (`via spawn|ipc|ws|port|http|embed`).
- `atlas/contracts.atlas` — the seams with several anchors: `transport-traits`,
  `napi-projection`, `session-wire`, and `bench-app-types` (the 12 types of RFC 027 §7's fixture
  list, each mirrored in `benchmark/app/src/api/types.ts`).
- `atlas/flows.atlas` — `discovery`, `send`, `file-push`, `store-update`: step ids, titles,
  `via`/`direct`, `by`, and the two steps done in Go (`at file:packages/sidecar-slim/main.go`).
- `atlas/rules.atlas`, `atlas/decisions.atlas` — the rules (including the single-crate rules of
  the crates.io crates and truffle-cli, with `crate:` scopes); one decision per significant RFC
  with its real status. Every accepted decision has a `confirmed-by` but `one-release-unit`: the
  comment above it names the release paths that do not fail closed yet. `bridge-is-private` holds
  the one sanctioned `except` (`BridgeStats`, public since 0.7.20).
- In code (`grep -rn '@atlas' crates benchmark`):
  - the roots of the unpublished crates with a crate doc: truffle-napi (its placement and
    `napi-pull-model`), truffle-tauri-plugin, truffle-bench (`benchmark/engine/src/lib.rs`: its
    placement, `bench-never-sets-hostname`, `legacy-transport-without-tbn1`) and truffle-bench-app
    (`benchmark/app/src-tauri/src/lib.rs`: its placement, `bench-app-drives-the-engine`);
  - contracts at their single anchor: `NetworkProvider`, `EnvelopeCodec`, `StoreBackend`,
    `FtMessage`, `SyncMessage`;
  - flow steps at the function that does the step (`// @atlas step send.route`).
- `atlas/atlas.baseline.json` — written only by `atlas baseline`; it holds the 7 findings kept for the user (13 keys), recorded when the atlas landed.

## Running it

Atlas lives at `$ATLAS`, default `$HOME/Projects/project100/protos/project-atlas`
(github.com/vibefield/project-atlas, private; clone it there and `npm install`). From the truffle root:

```sh
ATLAS="${ATLAS:-$HOME/Projects/project100/protos/project-atlas}"
TRUFFLE_SIDECAR_SKIP_DOWNLOAD=1 npm --prefix "$ATLAS" run -s atlas -- check "$PWD" --intent "$PWD/atlas"         # what is wrong
TRUFFLE_SIDECAR_SKIP_DOWNLOAD=1 npm --prefix "$ATLAS" run -s atlas -- check "$PWD" --intent "$PWD/atlas" --all   # also asserted + verified
TRUFFLE_SIDECAR_SKIP_DOWNLOAD=1 npm --prefix "$ATLAS" run -s atlas -- explain "$PWD" --intent "$PWD/atlas" crates/truffle-core/src/egress.rs
TRUFFLE_SIDECAR_SKIP_DOWNLOAD=1 npm --prefix "$ATLAS" run -s atlas -- explain "$PWD" --intent "$PWD/atlas" 'mod:truffle_core::{session, transport}'
"$ATLAS/node_modules/.bin/tsx" "$ATLAS/packages/engine-intent/src/cli.ts" atlas crates benchmark packages examples   # syntax only
```

- Atlas is private. If `$ATLAS/package.json` is missing, `npm --prefix` exits 254 with no output (`-s`):
  skip the check, install nothing, and say so.
- Pass `--intent "$PWD/atlas"`: it pins the DSL root and the baseline
  (`atlas/atlas.baseline.json`, beside `system.atlas`) whatever Atlas version you run.
- Rust facts come from `cargo doc` over the whole workspace, napi and tauri included, with the
  target dir in Atlas's own cache, not `target/`. It runs truffle's build scripts:
  - truffle-core's `build.rs` runs `go build` of the gitignored `crates/truffle-core/tests/test-sidecar`
    when the Go sources are newer than it and `go` is on PATH (without `go` it only warns);
  - truffle-sidecar's `build.rs` downloads the release sidecar unless `TRUFFLE_SIDECAR_SKIP_DOWNLOAD=1`
    is set. Without it a cold cache downloads over the network (Atlas's `CARGO_NET_OFFLINE` does not
    stop a build script), an offline run panics, and on a Release Please branch (version bumped, no
    pinned checksum yet) it panics with "SECURITY: no pinned checksum" (`docs/RELEASING.md`).
    `cargo doc` never runs the sidecar, so every command above sets the variable, as CI's lint job does;
  - truffle-tauri-plugin's `build.rs` runs `tauri_plugin::Builder`, which can rewrite the tracked
    files under `permissions/`: check `git status` after a cold run. truffle-bench-app's runs
    `tauri_build::build()` (its output, `benchmark/app/src-tauri/gen/`, is gitignored), and
    truffle-bench's embeds `rustc -V`;
  - on Linux, truffle-tauri-plugin and truffle-bench-app need Tauri's system libraries (`Cargo.lock`
    has `webkit2gtk-sys` and `gtk-sys`; CI's `cargo doc` excludes both). Without them Atlas reports
    `cargo-doc-failed` for them and grades them from the source scan (not tried on Linux).

  A `cargo-doc-failed` error means that crate's facts come from the source scan only, with no
  rustdoc symbols: fix the build before trusting its `sym:`, contract or flow-step results.
  `check` itself prints only engine errors such as `cargo-doc-failed`; warnings sit in an
  atlas.json's `diagnostics` (`check --out <file>`). Two `rustdoc-mismatch` warnings are expected
  there: truffle-bench and truffle-bench-app name their bin target like their lib, so `cargo doc`
  writes one JSON for both and Atlas models the bin targets (truffle-bench's `cli.rs` and
  `legacy/`, the app's `main.rs`) from source. Their libraries keep rustdoc symbols; select bin
  code with `path:`/`file:`, not `sym:`.

  A cold run took about 3 minutes; after comment-only edits the Rust step took 9-10 s. Uncommitted
  files count.
- The last command parses `atlas/` and every `@atlas` comment without building anything: syntax
  and duplicate ids only, no selector or reference checks. It also parses `.go` and `.swift`
  comments, which a real build ignores. It calls Atlas's own `tsx` directly: truffle has none, and
  `npx` would fetch one from the registry.
- Do not use `--rust-source-only` on truffle: without rustdoc symbols, 81 claims dangle (contract
  anchors, `sym:`-scoped checks, domain `touches`, flow steps and hops) and 14 are violated instead
  of 7 (the `BridgeStats` except needs its symbol too).
- `baseline` (`TRUFFLE_SIDECAR_SKIP_DOWNLOAD=1 npm --prefix "$ATLAS" run -s atlas -- baseline "$PWD" --intent "$PWD/atlas"`)
  writes `atlas/atlas.baseline.json` with every violation of that run, so whatever your change
  added becomes `known` too. The committed one holds the 7 findings kept for the user. It runs
  again only when the user decides to accept that run's violations, never to turn the check green
  or to hide a violation your change caused.

## Reading `check`

An excerpt of the run on 0.7.20 with this atlas and its baseline committed (the header's hash
changes with every commit); `…` marks what is left out here. Claims sort by severity, then by the
file and line that declare them: today the decisions (`atlas/decisions.atlas`), then the rules
(`atlas/rules.atlas`), then the in-code contract:

```
atlas check · Truffle @ 59fab9c · 630 claims
  590 verified · 33 asserted · 7 violated (0 new, 7 known) · 0 dangling · 0 ambiguous · 0 stale
  coverage: layers 100.0% · domains 100.0% of 89,563 prod lines · rules 40 pass, 4 fail, 0 unchecked

VIOLATED · known (in the baseline) (7)
  decision:layered-architecture  decision layered-architecture "Layers 3-6 behind one Node API; applications are Layer 7" code RFC-0…
    at atlas/decisions.atlas:38 · decision · basis none
    rule:layers-point-down  violated
    rule:network-knows-no-upper-layer  violated
    rule:transport-knows-no-session  violated
    rule:apps-use-node-api  violated
    → Accepted, but rule:layers-point-down, rule:network-knows-no-upper-layer, rule:transport-knows-no-session, rule:apps-use-node-api are failing: the code has drifted from this decision.
  decision:synced-store  decision synced-store "SyncedStore and a request/reply helper in core" code RFC-016 status accepted
    at atlas/decisions.atlas:85 · decision · basis none
    contract:sync-wire  violated
    → Accepted, but contract:sync-wire is failing: the code has drifted from this decision.
  rule:layers-point-down  layered
    at atlas/rules.atlas:10 · rule · basis edge
    mod:rs/truffle_core::network → mod:rs/truffle_core::egress
      crates/truffle-core/src/network/mod.rs:261  ) -> impl std::future::Future<Output = Result<crate::egress::LoopbackConfig, NetworkError…
      …
    → 3 production edges go up the layer order; first: truffle_core::network (layer:network) → truffle_core::egress (layer:node-api) …
  … rule:network-knows-no-upper-layer, rule:transport-knows-no-session
  rule:apps-use-node-api  deny layer:apps -> layer:network, layer:transport, layer:session, layer:envelope
    at atlas/rules.atlas:74 · rule · basis edge
    pkg:cargo/truffle-bench → pkg:cargo/truffle-core
      benchmark/engine/src/control.rs:34  use truffle_core::session::{SessionError, DIAL_CANCELLED_SHUTTING_DOWN};
      benchmark/engine/src/control.rs:35  use truffle_core::transport::quic::QuicListener;
    …
    → 66 forbidden dependencies from layer:apps into layer:network, layer:transport, layer:session, layer:envelope (code); …
  … contract:sync-wire

ASSERTED (33): not listed; --all lists them

VERIFIED (590): not listed; --all lists them

ok: no new violations
```

- With a new violation the run ends instead with `N new violations (…): exit 1` and Atlas's generic
  advice, "fix the code, add `except <sel> "reason"`, or record today's state with
  `atlas baseline`". Never take its `except` or `atlas baseline` yourself: an `except` changes the
  design and the baseline is the user's decision (Running it, Fixing).
- This is what a clean tree prints: the 7 findings kept for the user under
  `VIOLATED · known (in the baseline) (7)`, `7 violated (0 new, 7 known)`, `ok: no new violations`,
  exit 0. The baseline holds 13 keys (3 claim ids and 10 rule pairs).
- Sections in order: VIOLATED · new, VIOLATED · known (in the baseline), DANGLING, AMBIGUOUS, STALE.
  ASSERTED and VERIFIED are only counted unless you pass `--all`.
- Each claim: `subject  DSL text [severity]`, then `at file:line · kind · basis`. A claim listed
  under VIOLATED, DANGLING, AMBIGUOUS or STALE then prints each glob's match count (`* → 41`, and
  `→ 0 (matches nothing)` for any selector that matches nothing), the evidence
  (`file:line  source`) and `→` the remedy. `--all` lists verified and asserted claims with those
  first two lines only, no counts, so `explain '<glob>'` (each term prints `→ n`) to see what a
  verified glob matches. A glob that matches nothing dangles.
- Exit 1 only for a violated claim of severity `error` that is not known. DANGLING, STALE, AMBIGUOUS and `warn` rules (none today) do not fail the
  check, but you still leave 0 dangling and 0 ambiguous. One exception: a dangling rule, contract
  or flow named in a decision's `confirmed-by` makes that decision violated (exit 1), so fix the
  selector. `layered-architecture` and `synced-store` are violated already, so a dangling member of
  theirs (any of the 10 rules in layered-architecture's `confirmed-by`, or `sync-wire` and
  `request-reply-over-node`) shows only under DANGLING and in the decision's evidence
  (`rule:bridge-is-private  dangling`), not in the exit code: the decision's only key is its claim
  id, so it stays known and the check exits 0 (Atlas `grade/decisions.ts`, `main.keys = [main.id]`).
- **Known or new** (Atlas `packages/aggregator/src/grade/rules.ts`,
  `grade/claims.ts` `evidenceKey`, `packages/aggregator/src/baseline.ts`). A violated claim is
  known only when every one of its keys is in the baseline. Its keys:
  - a dependency rule (`layered`, `deny`, `only`): one per evidence item, `rule:<id>|<from>|<to>`.
    That is per package pair for an edge between packages
    (`rule:apps-use-node-api|pkg:cargo/truffle-napi|pkg:cargo/truffle-core`); per module pair inside
    one, named by the leaf modules at the two ends of the import
    (`rule:layers-point-down|mod:rs/truffle_core::network::tailscale::provider|mod:rs/truffle_core::egress`);
    and per importing module (Rust) or file (TS) for an `ext:` target
    (`deny layer:network -> ext:tokio-tungstenite, ext:quinn`);
  - `require` (dependency): one per scope root, `rule:<id>|<root>|no code edge into <target>`
    (`transport-knows-no-session`, `session-knows-no-payload`, `request-reply-over-node`,
    `bench-app-drives-the-engine`);
  - `forbid text`: one per file; `require text`: one per missing literal; `exhaustive`: one per
    unplaced file;
  - a decision: its claim id, a hash of its header (id, title, `code`, `status`; not its date,
    `because` or body);
  - a contract, flow claim or relation: its claim id, a hash of its statement line (for a contract
    the check line, such as `produced-by crate:truffle-core each-variant`, not its quoted title),
    salted with the host symbol when the contract is declared in code (`SyncMessage` for
    `sync-wire`).

  So a new pair is new and exits 1: an upward import between two truffle-core modules that had
  none, or a lower-layer import from truffle-bench-app or any other package the baseline lacks.
  Another import between a listed pair stays known: any further truffle-cli, truffle-napi,
  truffle-tauri-plugin or truffle-bench import of a truffle-core lower layer, another egress import
  in `network/mod.rs` or `tailscale/provider.rs` (one in any other `network` module is a new pair),
  and a second unproduced `SyncMessage` variant. The check still exits 0, so compare each known
  claim's evidence and `→` count with the list below.

  Keys name the code. A known finding becomes new with the same count, and the check exits 1, when
  you move code at either end of a known module pair (an egress import leaving `network/mod.rs` or
  `tailscale/provider.rs`, the hello import leaving `transport/websocket.rs`, the egress types
  leaving `egress`), rename a package in a known package pair, or move or rename `SyncMessage`.
  That is not a finding of yours: say so to the user; re-running `atlas baseline` is their call.
  Rewording a decision's header or a contract's check line changes its id and makes it new too; a
  rule's keys use its id, not its text.
- Both coverages are 100%. A new crate, npm package or directory left unplaced drops them (and
  fails `exhaustive`). A new truffle-core module never does: it inherits node-api and `mesh`, so
  `explain` it and place it on purpose.
- Go, Swift and scripts are never production code to Atlas: they count in no coverage, and only
  text rules, witnesses and `see` grade them.
- 0 stale. A claim goes stale (it never fails the check) once 10 commits touched its code since the
  last commit of the file holding the statement (the `.atlas` file, or the source file holding the
  `@atlas` line); an uncommitted file has none, so commit atlas changes with the code they describe.
- The 7 findings kept for the user, in output order, with their counts (each claim's `→` line
  gives its count). Your change is clean only when nothing is new and these 7 are known with these
  counts; any other violated claim, or a higher count, is yours. A further import between modules or packages
  already listed only lengthens a claim's evidence, so the summary line and the exit code stay the
  same: compare the counts. Do not edit a rule, a decision or a module's tier for them, and do not
  change their code either (no deleting `Clear`, no moving the hello gate or the egress types)
  unless the user asks:
  - `decision:layered-architecture`: its 4 failing rules below;
  - `decision:synced-store`: `contract:sync-wire`;
  - `rule:layers-point-down`: 3 module edges, the two egress ones below and the hello one;
  - `rule:network-knows-no-upper-layer`: 2 edges. The `NetworkProvider` egress verbs
    (`network/mod.rs:261`, `:271`, `:272`, `:292`, `:302`) and `tailscale/provider.rs:23` name
    `egress` types, while `egress.rs` documents them as the Node API of RFC 026 and `egress` sits in
    node-api;
  - `rule:transport-knows-no-session`: 1 edge. `transport::websocket` imports `session::hello`
    (`websocket.rs:57`): the RFC 017 §8 hello runs inside the WebSocket handshake. RFC 012 §3.4
    calls that handshake transport-level while `hello.rs` calls itself Layer 5, and RFC 025 §3.4
    ("Layer 4/5 — the hello gate") added the login gate there;
  - `rule:apps-use-node-api`: 66 edges (Atlas's count) in 60 module pairs from 4 packages:
    truffle-bench 24, truffle-napi 16, truffle-tauri-plugin 7, truffle-cli 13. They name
    `TailscaleProvider`, `PeerEvent`/`PeerState`/`PeerIdentity`, `NetworkStatusEvent`,
    `transport::quic`, `RawListener`/`RawIncoming`, `DatagramSocket`, `SessionError`,
    `NetworkError` and `network` value types; Node's own signatures return them
    (`on_network_status` since 0.7.18, and `lib.rs` does not re-export `NetworkStatusEvent`).
    RFC 015 §3 designs the bindings around `Node<TailscaleProvider>`, so naming the Layer 3
    provider follows RFC 015 and breaks RFC 012 §3.1. truffle-bench also names
    `envelope::{Envelope, EnvelopeCodec, JsonCodec}` (the `ws` workload sizes encoded envelopes) and
    `session::DIAL_CANCELLED_SHUTTING_DOWN` (`control.rs`). RFC 027 prescribes these lower-layer
    types for the engine (§5.4 `Node<TailscaleProvider>` and `PeerEvent::AuthRequired`; §6
    `SessionError::ReconnectBackoff` and `QuicListener::close()`; §8.2 `RawIncoming.stream`,
    `DatagramSocket::Network` and `JsonCodec`; §14 #21 the dial cancelled at shutdown), so
    truffle-bench's imports follow RFC 027 and break RFC 012 §3.1. Two of the edges exist only
    through truffle's own re-exports (`truffle_core::BroadcastReport`,
    `truffle_core::proxy::ProxyRoute`): Atlas resolves a re-export to the module that defines the
    type;
  - `contract:sync-wire`: 1 variant, `SyncMessage::Clear`, is never produced. RFC 016's wire table
    broadcasts it when a peer goes offline; RFC 025 §3.5 since drops any `Clear` naming another
    device. The receive path still handles it (`synced_store/sync.rs`, `handle_incoming_message`).

## Fixing

- **Dangling selector.** `explain` the selector: each term prints `→ n` and
  `DANGLING: matches nothing` for the bad one. Find where the code went (`git log --follow`,
  grep) and fix the selector in the same change as the rename. Module paths also sit in brace
  forms (`mod:truffle_core::{node, identity, egress}`, `mod:truffle_napi::{raw_socket, udp_socket, quic}`,
  `mod:truffle_bench::workloads::{tcp, udp, quic}`) that a grep for the full path misses: grep the
  stem. If the code is gone for good, delete the claim with it. Some text checks search one
  function (`sym:…::validate_hello`, `is_app_peer`, `spawn_connection_task`,
  `Node::spawn_envelope_router`, `handle_incoming_message`, `sender_owns`): renaming it dangles
  them, and the decision they confirm then grades violated.
- **Dangling `see`.** The message suggests the nearest heading (`Did you mean #…?`). An RFC
  heading rename dangles every `see` pointing at it: fix them together. A `#L<n>` reference
  (`crates/truffle/src/lib.rs#L38-L41`) dangles only when the file gets shorter; re-check that
  the line still says it.
- **"has no code subject".** An `@atlas` comment sits below the last declaration or in the wrong
  place; move it directly above its item.
- **Dependency rule violated.** The evidence names the edge and its sites. Fix the code (route
  through Node), unless the edge is one of the 7 findings above. The remedy line also offers an `except …` to paste or a wider `only` list: an
  agent never takes either on its own. It changes the design, so the rule and the RFC it cites
  change together, and that is the user's call; propose it instead. A sanctioned exception reads
  `except <selector> "real reason"`; an `except` that excuses nothing goes stale: delete it.
- **Contract violated.** `implemented-by` names the package missing an implementation;
  `produced-by … each-variant` names the enum variant nobody constructs (produce it, or delete
  the dead variant; `SyncMessage::Clear` is one of the 7 findings above, so neither without the user); `mirror` names the missing `Napi…` export in `crates/truffle-napi/index.d.ts`
  (rebuild the addon or rename the type) or, for `bench-app-types`, the type missing from the
  hand-written `benchmark/app/src/api/types.ts` (RFC 027 §7: add it there).
- **Step asserted: "The step has no `at` anchor".** You deleted or renamed a function that
  carried `// @atlas step flow.step`. Put the line above the function that now does the work.
  A misspelt `@atlas step` id produces no claim and no error at all, so after touching a flow
  run `check --all` and confirm all 27 steps of the four flows are verified. Hops `via` a runtime
  mechanism stay asserted (10 today).
- **Stale.** At least 10 commits touched the claim's code since its file (the `.atlas` file, or
  the source file holding the `@atlas` line) last changed. Re-read the code; fix the statement
  if it no longer holds. Staleness is per file in v1, so it clears when that file is next committed.
  Stale never fails the check. Do not touch an `.atlas` file only to reset staleness: re-read the
  code, and commit a statement change only when the statement needed one.
- **Planned element landed.** `expects … [warn]` resolving means the thing was built: drop
  `status planned`/`proposed` and `expects`, then bind it.

## Authoring

1. **Crate roots.** Only an inner doc comment (`//!`) in `lib.rs`/`main.rs` binds the whole
   package; there `this` is the package, manifest edges included. In a lib + bin package the
   lib's `lib.rs` binds the bin's modules too (`explain benchmark/engine/src/legacy/transport.rs`:
   `layer:bench (inherited from pkg:cargo/truffle-bench)`).
   - **Published crates** (no `publish = false` in `Cargo.toml`: truffle-core, truffle,
     truffle-sidecar) get no `//!` atlas lines, because docs.rs would render them. Place them in
     `atlas/layers.atlas` and `atlas/domains.atlas` (`contains crate:x`) and write their rules in
     `atlas/rules.atlas` with `crate:` scopes. A `//` header comment is not a substitute: it binds
     the crate's root module, not the package. Tried on truffle: `explain` showed
     `root mod:rs/truffle · depth 2` with `pkg:cargo/truffle` unclaimed, and
     `only this -> crate:truffle-core on manifest` passed although `Cargo.toml` names truffle-sidecar.
   - **Unpublished crates with a crate doc** end it with one blank `//!` line, then their lines:

     ```rust
     //! …the crate's existing docs, unchanged…
     //!
     //! @atlas layer bindings · domain bindings
     //! @atlas rule napi-pull-model "Raw-transport handles pass bytes only as the answer to a JS read, never a ThreadsafeFunction" {
     //!   forbid text "ThreadsafeFunction" in mod:truffle_napi::{raw_socket, udp_socket, quic}
     //!   see "docs/rfcs/021-raw-transport-js-api.md#12-decided-questions"
     //!   see "docs/rfcs/021-raw-transport-js-api.md#5-design-overview"
     //! }
     ```

     These lines still render into rustdoc, and CI runs `clippy -D warnings`,
     `RUSTDOCFLAGS=-D warnings cargo doc` (every crate but truffle-napi, truffle-tauri-plugin and
     truffle-bench-app, so truffle-bench's lines must be rustdoc-clean) and doctests. So: never
     touch the first doc paragraph; indent continuation lines 2 spaces, never 4+ (an indented code
     block becomes a doctest); no `[…]` (intra-doc link), no `<word` (HTML tag), no bare URL, no
     line starting with `#`, `>`, `-`, `*` or `1.`; when the docs end in a code fence (truffle-bench's
     end in a doctest), insert after the closing fence.
   - **A crate without a crate doc** (truffle-cli's `main.rs`) is placed in `atlas/`. Never
     invent a crate doc to hold the lines.
2. **Everything else in Rust uses `//`**, never `///`: directly above the item, above its `///`
   docs and `#[…]` attributes, at the item's indentation. `///` would render into rustdoc, and in
   `crates/truffle-napi/src` NAPI-RS copies item docs into the committed `index.d.ts`.

   ```rust
   // @atlas contract sync-wire "SyncedStore wire messages on namespace \"ss:{id}\"" {
   //   produced-by crate:truffle-core each-variant
   //   see "docs/rfcs/016-synced-store-and-request-reply.md#wire-protocol"
   //   see "docs/rfcs/025-login-scoped-nodes.md#35-sender-bound-slices-tf-2"
   // }
   /// Wire protocol messages sent on namespace `"ss:{store_id}"`.
   ```
3. **TypeScript uses `//`** (a `*/` in a glob would close a block comment).
   `crates/truffle-tauri-plugin/guest-js/` has no `package.json`, so Atlas sees inventory files
   only: describe it from `atlas/` with `path:`/`file:` selectors.
4. **Go and Swift are described from `atlas/`, never in-code**: `atlas build` reads no `@atlas`
   comment in a `.go` or `.swift` file. Use `path:` for placement, `file:` for text rules and
   relation anchors, and `note file:… "…"` to name a Go file in a domain (a `file:` term in
   `contains` binds the file's whole directory).
5. **Header pitfall.** A `//` annotation before a file's first code token binds the file's module,
   not the declaration below it. Symbol annotations go after the `use` lines. The parse command
   with `--json` shows each annotation's `host.style` (`inner-doc` for crate roots) and
   `host.fileHeader`; `explain <file>` shows what it bound to.
6. **Standalone files** hold what spans crates: the layer structure and truffle-core's tiers,
   domain claims below crate level, components and relations, cross-cutting rules, decisions.
   Declaring an element in two places is a parse error.
7. **Cite truffle.** Every rule and decision carries `see "<file>#<slug>"`, or `#L<n>` on a code
   file or on a Markdown header line under no heading of its own (an RFC's `**Supersedes**` line:
   `docs/rfcs/012-layered-architecture-redesign.md#L6`). Cite CLAUDE.md, README.md,
   `docs/API-STABILITY.md`, `docs/RELEASING.md`, `docs/TESTING.md`, `docs/tailscale-performance.md`,
   `docs/guide/`, `apple/README.md`, `crates/truffle-napi/README.md`, `benchmark/README.md` and RFCs
   003, 010 and 012–027 for decisions in force (RFC 027 superseded RFC 019 §10.2 and RFC 020 §3 and
   §6.2), and a comment in the code that states the rule by line
   (`crates/truffle/src/lib.rs#L38-L41`, `apple/Package.swift#L4-L6`); cite other pre-012 RFCs only
   to record supersession. Never cite `docs/architecture/networking-flows.md` (the archived v1
   architecture). A slug is the heading lowercased, with every character that is not a letter,
   digit, whitespace or `-` dropped, then each whitespace character turned into `-` (no
   collapsing); repeated headings get `-1`, `-2`; headings inside code fences don't count. So
   `### 3.6 Layers 1-2: Bridge + Sidecar (internal to TailscaleProvider)` →
   `36-layers-1-2-bridge--sidecar-internal-to-tailscaleprovider`. That is Atlas's rule; GitHub
   keeps `_`, so a heading with `_` in it (RFC 027's ``## 5. Engine API (`truffle_bench`)``) gets a
   slug Atlas accepts and a link GitHub does not: cite another heading. A non-Markdown file
   (`docs/index.html`) takes no `#slug`. When an RFC header's status disagrees with what shipped,
   the decision records the real status and names the header in `because`; never edit the RFC
   to match (the user's call).
8. **Style** (DSL §8): ids are slugs; the coarsest true selector (crate > mod > path > sym); one
   statement per line; prose only for what code cannot show, at most two sentences; never restate
   a doc comment; do not annotate every module.

## Committing

- An atlas-only change is `docs:` or `chore:`, never `feat:` or `fix:`: Release Please cuts a
  release for those (and for the other types its default changelog shows, such as `perf:`, or a
  `!` breaking change). `docs:` and `chore:` are hidden types and cut none.
- `.githooks/pre-commit` (enabled by `git config core.hooksPath .githooks`) runs, on what is staged:
  1. NAPI drift: a staged `crates/truffle-napi/src/*.rs`, `Cargo.toml` or `build.rs` runs
     `cd crates/truffle-napi && pnpm run build` (`napi build --platform --release`, a cargo build)
     and fails while the regenerated `crates/truffle-napi/index.d.ts` is unstaged; without pnpm it
     exits 1 at once (it does not skip);
  2. a YAML parse of staged `.github/workflows/*.yml` and `*.yaml` (python3 + PyYAML, skipped
     without them);
  3. `rustfmt --edition 2021 --check` on staged `.rs` files;
  4. `pnpm exec prettier --check` on staged `packages/*/src/**/*.ts` and
     `benchmark/app/src/**/*.{ts,tsx}` (not `benchmark/app/src/brand/`), skipped without pnpm;
  5. `cargo test -p truffle --doc` when `crates/truffle/src/lib.rs`, `crates/truffle/Cargo.toml`,
     `crates/truffle-core/src/lib.rs` or `crates/truffle-core/src/node.rs` is staged;
  6. `pnpm run build` (`pnpm -r run build`, the NAPI addon's cargo build included) when
     `packages/*/src/**/*.ts`, a `packages/*/package.json`, `crates/truffle-napi/index.d.ts`, a file
     under `benchmark/app/src/` or `benchmark/schema/`, `benchmark/app/package.json`,
     `benchmark/app/tsconfig*.json` or `benchmark/app/vite.config.ts` is staged (skipped without pnpm);
  7. `scripts/check-for-secrets.sh` on every commit that stages a file: added lines matching
     `tskey-…` outside its allow-list.

  So staging `@atlas` lines in truffle-napi or `node.rs` runs cargo builds; lines in
  `benchmark/engine/src/lib.rs` or `benchmark/app/src-tauri/src/lib.rs` get rustfmt and the secret
  scan only, and `atlas/`, CLAUDE.md or the skill the secret scan only.
  CLAUDE.md allows `--no-verify` rarely; when the hook cannot run because builds are off-limits,
  ask the user before skipping it, or commit later.

## Worked examples

- **New crate** `crates/truffle-foo`: published (no `publish = false`)? Add `contains crate:truffle-foo`
  to a layer in `atlas/layers.atlas` and a domain in `atlas/domains.atlas`. Unpublished with a
  crate doc? End its root `//!` docs with `//! @atlas layer <tier> · domain <domain>`. A consumer
  of the Node API sits in an `apps` sub-tier (as truffle-bench does), and `apps-use-node-api` then
  holds it to the Node API; only code that uses no truffle crate (scripts, CI, docs, the Go
  baseline bench) sits in `tooling`. A new language binding joins `bindings-wrap-core` in
  `atlas/rules.atlas`; truffle-core must not depend on it (`core-depends-on-no-crate`).
- **New truffle-core module** `truffle_core::foo`: add it to a tier in `atlas/layers.atlas` and a
  domain in `atlas/domains.atlas`. Forgotten, it falls to node-api through `contains mod:truffle_core`
  and to `mesh` through `crate:truffle-core`, and nothing reports it:

  ```
  $ TRUFFLE_SIDECAR_SKIP_DOWNLOAD=1 npm --prefix "$ATLAS" run -s atlas -- explain "$PWD" --intent "$PWD/atlas" crates/truffle-core/src/lib.rs
  mod:rs/truffle_core  (module of pkg:cargo/truffle-core · truffle_core · slug truffle_core)
    layer       layer:node-api
    …
     ✓ layer:node-api         contains mod:truffle_core
         atlas/layers.atlas:34 · root mod:rs/truffle_core · depth 2 · literal
       layer:core             contains crate:truffle-core
         atlas/layers.atlas:31 · root pkg:cargo/truffle-core · depth 1 · literal  ← less specific (depth 1 < 2)
  ```

  Rank matters: `layers-point-down` (`layered`) forbids any edge going up the stack, so pick a
  tier at or above every tier the module uses and at or below every tier that uses it; if no such
  tier exists, that is a design question for the user. Then compare `layers-point-down`'s edge
  count with today's 3. A new subsystem joins `$subsystems` in
  `atlas/system.atlas` (node-api tier, `subsystems-send-typed`); give it its own namespace and add
  that namespace to `namespaces-owned`.
- **New transport** `transport::foo`: it lands in the transport tier through
  `mod:truffle_core::transport`. Add it to `transports-independent` and to the `raw-transport`
  domain; keep `tokio-tungstenite` and `quinn` out of `network`.
- **New benchmark workload** `benchmark/engine/src/workloads/foo.rs`: it inherits `bench` and the
  `bench` domain from truffle-bench's root; add `touches mod:truffle_bench::workloads::foo` to the
  domain it measures (`atlas/domains.atlas`). Its truffle-core imports below the Node API raise
  `apps-use-node-api`'s count. That claim is violated already and known under the
  truffle-bench package pair, so the exit code does not show it: report the new count to
  the user and do not call the change clean. A new type joins `bench-app-types` when RFC 027 §7's
  fixture list names it.
- **New NAPI type** for a core type: `napi-projection` checks every core type in its `at` list
  for an `index.d.ts` export named `Napi{name}`. Add the core type when RFC 015's set grows.
- **New npm package or example**: `pkg:@vibecook/example-*` picks up a new example in both
  `atlas/layers.atlas` and `atlas/domains.atlas`; any other package needs a `contains pkg:…` line
  in a layer and in a domain.
- **Splitting a big file** (node.rs, provider.rs, session/mod.rs): `// @atlas step` lines move
  with their functions; `mod:`/`file:`/`sym:` selectors that named the old path, in `atlas/` or in
  other code's `@atlas` lines, dangle. Fix them in the same change and check `--all` that the
  flows' steps are still verified. Moving `provider.rs`'s `use crate::egress`
  into a child module also turns `layers-point-down` and `network-knows-no-upper-layer` new with
  the same counts (exit 1): their keys name the module (Known or new). Tell the user; it is not
  your finding.
