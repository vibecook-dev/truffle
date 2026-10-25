# TailscaleKit on libtailscale main with Go 1.27.3 (October 2026)

The Apple framework now uses the sidecar's Go 1.27.3 (CVE-2026-111943) and
builds from upstream libtailscale `main` (`d1dc45d`). Tailscale stays at
1.104.1 in both graphs.

Upstream's framed fd-passing records replace our remote-address patch, and the
typed-DERP-ID test patch is upstream. Upstream still keys connection cleanup by
the C-side descriptor, whose number is reused almost immediately: in a test,
7 of 8 accepted connections never had their `net.Conn` closed. The new
`libtailscale-conn-cleanup.patch` keys cleanup by the Go-owned descriptor and
adds a regression test that fails without it. The same change is proposed
upstream in [tailscale/libtailscale#69](https://github.com/tailscale/libtailscale/pull/69)
(issue [tailscale/tailscale#21934](https://github.com/tailscale/tailscale/issues/21934)). The peer-notification and
IPN-stream-recovery patches are unchanged. See
[the provenance record](../apple/Vendor/README.md).

The machine preparing this change had no full Xcode, so CI built the artifact:
`apple-tests` now uploads the framework its source build validated, and the
dependency release publishes that archive unchanged. The recorded toolchain is
therefore Xcode 16.4 (Swift 6.1.2) instead of the Xcode 26.6 used for the
previous artifact. The framework ships Swift interfaces, so newer Xcode
versions can consume it.

Validation on October 25, 2026:

- Local (macOS 26.3, Go 1.27.3): all three patches apply to `d1dc45d`,
  `go mod tidy -diff` is clean, and the patched Go packages pass `go vet` and
  `go test -race`. The materializer passes source checks, patch application,
  version gates and the Go tests. `scripts/check-go-security.sh` finds no
  reachable vulnerabilities in either graph.
- [CI source build](https://github.com/vibecook-dev/truffle/actions/runs/40184193955/job/121141398540):
  patched Go race tests, six exact-source Swift stream tests, 107 Swift package
  tests, and iOS device, simulator and example-app builds.
- The published archive's SHA-256 matches the CI log, GitHub's asset digest
  and a fresh public download. Both binaries embed Go 1.27.3, Tailscale
  1.104.1 and `golang.org/x/net` 0.60.0.

Rollback: revert this change. The previous artifact,
`tailscalekit-59d4bb82-ts1.104.1-go1.27.2` (checksum
`f8a61a742e42cbf04983213ea28f8a2370802b17fe420ea9cac7f59249c3238f`), stays
published.

---

# Go 1.27.3 for the sidecar (October 2026)

The sidecar now builds with Go 1.27.3 for CVE-2026-111943 (GO-2026-6809):
`crypto/x509` now rejects a set of compromised intermediate certificates. Under
Go 1.27.2, govulncheck reports it as reachable through the sidecar's TLS dial
(`handleDial` → `tls.Conn.HandshakeContext` → `x509.Certificate.Verify`).
Tailscale stays at 1.104.1: 1.104.2 moves Tailscale's own builds to Go 1.27.3
and otherwise only fixes Windows DNS, which tsnet does not use.

The Apple graph keeps Go 1.27.2 until the next TailscaleKit artifact, so the two
graphs now record different Go versions:

- `apple-tests` keeps Go 1.27.2, because the materializer requires the exact Go
  version in `apple/Vendor/libtailscale/go.mod`.
- The security workflow scans both graphs with Go 1.27.3, so it does not report
  GO-2026-6809 for the published framework, which was built with 1.27.2. On
  macOS and iOS, Go verifies against system roots with Apple's platform
  verifier, so the framework relies on Apple's trust store updates rather than
  Go's denylist.

The materializer's Tailscale version check now also reads a `require` block.
Since gVisor became a direct sidecar dependency (`7ef088e`), the check read an
empty sidecar version and every materialization stopped with "Apple and sidecar
Tailscale module versions must match".

Validation on October 24, 2026: `go mod tidy -diff`, `go mod verify`, `go vet`
and `go test -race` pass for the sidecar. All five release targets build with
`CGO_ENABLED=0`, and each binary embeds Go 1.27.3 and Tailscale 1.104.1.
`scripts/check-go-security.sh` (govulncheck 1.9.0) finds no reachable
vulnerabilities in either graph under Go 1.27.3; the same scan of the sidecar
under Go 1.27.2 reports GO-2026-6809.

---

# Tailscale 1.104.1 upgrade (October 2026)

Both Go module graphs pin Tailscale 1.104.1 and Go 1.27.2, with WireGuard/gVisor
upgraded through Tailscale's dependency graph. `golang.org/x/net` is additionally
pinned to 0.60.0 for the October HTTP/2 security fixes. Go build workflows use
the same toolchain. Dependabot watches both graphs; security CI scans the
sidecar and exact patched libtailscale sources with govulncheck 1.9.0.
The only remaining module-level advisory concerns unused `x/crypto/openpgp`;
there are no reachable vulnerable symbols or imported vulnerable packages.

The sidecar peer watcher no longer refers to the removed `Notify.NetMap`.
Apple retains its reviewed identity/stream patches and adds the typed-DERP-ID
compatibility patch for upstream test utilities. Wrapper source revision
is recorded once in `apple/Vendor/libtailscale/REVISION`.

## Transport regression fixes

New tests first reproduced host-network fallback on stopped Tailscale,
missing WhoIs proof acceptance, unauthorized local UDP injection, leaked and
incorrect ephemeral UDP ports, and direct paths mislabeled as DERP. The fixes:

- Production QUIC/UDP fail closed. Only providers explicitly selecting
  `NetworkTrust::UnverifiedLoopback` may bind direct loopback test sockets.
- Incoming WebSocket hello identity requires a valid WhoIs stable node ID.
  Mock opt-out is explicit and restricted to loopback; concrete mismatches
  are always rejected before returning a hello or yielding a stream.
- UDP control protocol **v6** derives a per-binding HMAC capability from the
  bridge session key and binding ID, pins the authenticated local endpoint,
  and acknowledges registration before data can flow. No secret is added
  to JSON events or logs. Self-test packets no longer enter application IO.
- Binding IDs, actual assigned ports, and cleanup installed before the bind
  request cover normal Drop, failures and cancelled futures. Late release
  cannot close a replacement binding using the same port. Unregistered
  relays expire after 10 seconds. QUIC can now listen on an ephemeral port.
- Diagnostic pings explicitly use Disco. Peer status distinguishes direct,
  peer relay, DERP, and inactive/unknown paths; a home DERP region alone is
  not evidence of active relay traffic.

Upgrade the Rust library and sidecar together. New Rust rejects UDP on a
pre-v6 sidecar with an explicit upgrade error. Distributing the new sidecar
uses the normal versioned release workflow; do not overwrite published binaries.
Protocol v5 remains the minimum for login gating; it does not imply support
for authenticated UDP. Rust and Go regression tests reject that legacy relay
before opening a listener or exchanging traffic.
Public `NetworkPeer` literals in custom Rust providers now include `active`
and `peer_relay`, and loopback mocks must explicitly opt into their trust mode.

## Benchmarking and storage

See [Tailscale performance measurements](tailscale-performance.md) for the
versioned reports, validated application workloads, TCP/QUIC server/client
modes, native Go/tsnet baseline, and two-host comparison procedure. Hosted
same-host results are smoke/overhead measurements, not WAN capacity claims.

Large local builds can use external storage. Set `CARGO_TARGET_DIR`, `GOCACHE`,
`GOTMPDIR`, and `TMPDIR` to that volume. `TRUFFLE_TEST_SIDECAR_PATH` overrides
the build script, integration harness and benchmark sidecar output/input path.
Swift supports `--scratch-path`/`--cache-path`; use Xcode `-derivedDataPath`
and a module-cache path on the same volume. Ignored Apple cache directories
also support symlinks. Rebuild Swift module caches after moving them: cached
compiler paths can be absolute.

## Validation on October 12, 2026

- The sidecar and native benchmark pass Go race tests, `go vet`, and module
  verification. Both final Go graphs pass the reachable-code vulnerability
  scan. Final sidecar builds pass for macOS arm64/amd64, Linux arm64/amd64,
  and Windows amd64; embedded build information confirms all three pins.
- `cargo test --locked --workspace -- --test-threads=1` passes, including
  366 core unit tests and the live tailnet suites. The 16 ignored transport
  stress tests also pass when explicitly enabled. Three pre-existing `todo!`
  integration placeholders and ignored documentation examples remain ignored;
  real startup, discovery, TCP and ping coverage lives in `integration_network`.
- Strict Clippy and rustdoc checks pass; existing Criterion benchmarks compile.
  TypeScript builds, 67 JavaScript tests, and 18 release-tooling tests pass.
- Release-mode smoke runs pass for request/reply, verified file transfer,
  both store scenarios, raw TCP, and raw QUIC with two concurrent clients.
  A native Go/tsnet TCP smoke run also passes. All these measurements use
  this Mac and are validation results, not evidence of WAN capacity.
  A native Go client also verifies 1 MiB transfers against the standalone
  Rust server, confirming the shared benchmark wire protocol.
- The Apple artifact passes all patched Go race tests, six exact-source Swift
  stream tests, 70 Swift package tests at its published source snapshot, and
  iOS device/simulator builds. Integration with main passes 107 Swift tests.
- Hosted benchmark CI exposed overlapping receiver cleanup between small file
  transfers. A 13-transfer regression reproduced the pending-offer rejection;
  the harness now waits for matching receiver completion between iterations,
  outside the sender's timed interval. The live-network workflow includes
  benchmark regressions so this ordering remains covered.

The existing QUIC adapter still uses IPv4 datagram framing without batching,
GSO/GRO or ECN, and allocates framing buffers per datagram. Those optimization
candidates need profiling and comparisons against the native baseline on two
hosts; this change does not claim that maximum stack performance is established.

## Apple artifact publication

The [dependency release](https://github.com/vibecook-dev/truffle/releases/tag/tailscalekit-59d4bb82-ts1.104.1-go1.27.2)
is published from source commit `b1541c5dff71d870640d69f773ec5a2549ca3cd5`.
Archive and source checksums are recorded in
[the provenance record](../apple/Vendor/README.md); the root Swift package now
pins the public URL and checksum.

A fresh clone of the public tag, with fresh SwiftPM/Xcode caches and no local
framework, downloaded the archive, passed all 70 Swift tests, and built the
production runtime for iOS devices and simulators. The downloaded archive and
both framework binaries match the validated hashes. These checks ran locally;
the dependency release was published before GitHub Actions CI ran. All local
consumer checkouts, caches, and build products used the portable SSD.

The subsequent [Apple CI run](https://github.com/vibecook-dev/truffle/actions/runs/38235663650)
checked out that exact source commit and passed source builds, public-download
consumer tests, iOS device/simulator builds, and Rust interop fixtures.
[Live-network CI](https://github.com/vibecook-dev/truffle/actions/runs/38235448715)
and [CodeQL](https://github.com/vibecook-dev/truffle/actions/runs/38235456509)
also passed. The historical source commit does **not** have fully green CI:
[security CI](https://github.com/vibecook-dev/truffle/actions/runs/38235452502)
found RustSec RUSTSEC-2026-0285 in its Rust dependency graph (both Go scans
passed), and [Rust lint](https://github.com/vibecook-dev/truffle/actions/runs/38235445020)
found an atomic-method deprecation on the newer CI compiler. These Rust-only
failures are corrected on the integration branch, which incorporates main's
rustls 0.23.45 update and uses an MSRV-compatible compare/exchange loop.
The published Apple artifact and source tag remain unchanged.

Future dependency releases require committed source and successful source-build
CI before publication. The subsequent root-package URL/checksum change must
pass public-download consumer CI before merge. Manual Apple CI accepts a
`source_ref`, allowing a specific immutable commit or dependency tag to be
validated without moving that tag.

---

# Tailscale 1.102.3 upgrade

## Scope

- Upgrade the Go sidecar from Tailscale 1.100.0 to 1.102.3.
- Upgrade Apple's embedded Tailscale from 1.94.1 to 1.102.3, using
  libtailscale revision `59d4bb82744915815178e0f0776d60026a397ee7`.
- Pin Go 1.26.8 in both module graphs and every Go build workflow.
- Subscribe to peer deltas in both runtimes. Tailscale 1.102 sends ongoing
  full `NetMap` notifications only on Windows. The Go watcher opts into
  narrow patches, refreshes its status snapshot, and preserves its baseline
  across reconnects. Apple requests full peer changes and refreshes status
  for each notification. Both retain their existing polling fallback.
- Use `client/local.Client` in the sidecar instead of the deprecated alias.

## Validation

Use the pinned Go version on `PATH`:

```sh
cd packages/sidecar-slim
go test -race -count=1 ./...
go vet ./...
go mod verify
```

The peer-watcher regression test drives a real LocalAPI streaming client
against a local HTTP server. It sends additions, online-status patches,
stream disconnection/reconnection, and removals without a `NetMap`, and
requires Truffle events before the 30-second polling fallback.

Build the sidecar with `CGO_ENABLED=0` for `darwin/arm64`, `darwin/amd64`,
`linux/amd64`, `linux/arm64`, and `windows/amd64`. Check each artifact with
`go version -m`: it must contain Go 1.26.8 and Tailscale 1.102.3.

With the repository's test-tailnet credentials configured, run:

```sh
cargo test --locked -p truffle-core \
  --test integration_network --test integration_eager_identity -- --test-threads=1
```

This covers startup/authentication, discovery, peer events, ping, WhoIs,
TCP dialing, ephemeral listeners, and identity exchange before app traffic.
The separate `integration_transport` stress suite uses a mock network provider
and is ignored by default; it is not evidence of a live Tailscale transport run.

Validate Apple using the local artifact first:

```sh
TAILSCALE_RUN_TESTS=1 apple/scripts/materialize-tailscalekit.sh
cd apple
swift test
xcodebuild -scheme TruffleTailscale -destination 'generic/platform=iOS' build CODE_SIGNING_ALLOWED=NO
xcodebuild -scheme TruffleTailscale -destination 'generic/platform=iOS Simulator' build CODE_SIGNING_ALLOWED=NO
```

## Publication order

Local validation on September 6, 2026 passed: the Go sidecar race suite,
`go vet`, module verification, all five sidecar release builds, nine live
tailnet networking/identity tests, the patched C-binding race tests, 68 Swift
tests, and iOS device/simulator builds. Archive integrity, embedded
Go/Tailscale versions, privacy manifests, and recorded checksums were verified.

The [dependency release](https://github.com/vibecook-dev/truffle/releases/tag/tailscalekit-59d4bb82-ts1.102.3-go1.26.8)
was published from source commit `1f87fd453bf21ac379a8dc58426f40ae23c1ab46`
with the framework, patched source archive, and build provenance attached.
GitHub's uploaded asset digest matches the checksum in `Package.swift`.
A fresh clone of that tag, with a fresh SwiftPM cache and no local framework,
resolved the public URL, passed all 68 Swift tests, and built the root
`TruffleTailscale` target for iOS devices and simulators. The publication gates
are complete. The upgrade subsequently shipped in `truffle-v0.7.13`.

## Apple review fixes

The post-release review found two existing identity issues and an IPN stream
recovery gap exposed by the upgrade. The corrected patch set:

- Associates remote addresses with unique transfer IDs and retains queued
  socket references until acceptance. Connection cleanup uses the Go-owned
  descriptor and protects shutdown against concurrent close/reuse.
- Reports unexpected successful EOF from the indefinite IPN watch, allowing
  the backend to reconnect and refresh its snapshot. Deliberate cancellation
  stops the URLSession; stale callbacks cannot restart a replacement watch.
- Normalizes IPv4, bare IPv6, and bracketed IPv6 before WhoIs, preserves a
  supplied port, and compares canonical addresses when validating identity.

The C regression suite exercises multiple queued connections over IPv4 and
IPv6, forced sender-descriptor reuse, payload attribution, split transfer
tokens, and cleanup. `TAILSCALE_RUN_TESTS=1` also compiles the exact patched
Swift reader/processor sources and tests terminal notifications, clean EOF,
reconnection, cancellation, stale callbacks, and transport errors through
URLSession. The regular Swift package tests include the production endpoint
parser on macOS, so this part of the iOS integration now has runtime coverage.

Changes to the wrapper patch set require a new immutable dependency artifact,
even when the upstream revision and Go/Tailscale versions stay the same.
The corrected artifact is published as
[`tailscalekit-59d4bb82-ts1.102.3-go1.26.8-r2`](https://github.com/vibecook-dev/truffle/releases/tag/tailscalekit-59d4bb82-ts1.102.3-go1.26.8-r2),
with its source inputs and checksums recorded in `apple/Vendor/README.md`.
A fresh clone of that tag downloaded the public archive, passed all 70
Swift tests, and built the production runtime for iOS devices and simulators.

## Future publication sequence

The sequence used for this upgrade, and required for future artifact changes:

1. Finish the source/build checks and review the pins and patch checksums in
   `apple/Vendor/README.md`.
2. Package the exact validated XCFramework and compute its checksum:

   ```sh
   (cd apple/Vendor && zip -qry /tmp/TailscaleKit.xcframework.zip TailscaleKit.xcframework)
   swift package compute-checksum /tmp/TailscaleKit.xcframework.zip
   ```

3. Publish the archive under the new immutable dependency tag
   (including a new suffix when the patch set changes), recording the source revision,
   module locks, patch checksums, and toolchains in the release notes.
   The checksum must match both `Package.swift` and the provenance record.
   Never replace the previous `tailscalekit-5e89501d` asset.
4. Resolve/build/test the root Swift package from a fresh checkout using the
   public URL, including iOS device and simulator builds. This step requires
   publication; local framework validation does not prove the URL resolves.
5. Merge the dependency update only after that check passes. Use the normal
   Truffle release workflows to distribute new sidecar binaries and update
   their version-specific checksums. Existing published sidecar assets and
   checksum pins must not be overwritten for this dependency upgrade.

## Rollback

Revert the upgrade as one change, including the module locks, Go workflow
pins, peer watcher adaptations, Apple patches, and root binary URL/checksum.
Leave the new dependency artifact available for any consumers already using
it. The previous immutable Apple artifact remains available at
`tailscalekit-5e89501d` with checksum
`25c84847b70f673835e9c0fd75a697fbe76943a0b20314bf56d2f5569c68f494`.
