# TailscaleKit provenance

`TruffleTailscale` consumes a TailscaleKit XCFramework with these inputs:

| Input | Pin |
| --- | --- |
| Upstream | `https://github.com/tailscale/libtailscale.git` |
| Revision | `d1dc45d039edb7759ecbee6ba2aea0c4bca884b8` |
| Embedded Tailscale | `tailscale.com v1.104.1` |
| Go | `1.27.3` |
| Recorded Apple toolchain | Xcode 16.4 (16F6), Apple Swift 6.1.2, GitHub Actions macOS 15 arm64 runner |
| License | BSD-3-Clause; see `TAILSCALE-LICENSE` |

Upstream libtailscale `main` also pins Tailscale 1.104.1, but with Go 1.27.1
and `golang.org/x/net` 0.59.0. The tracked `libtailscale/go.mod` and
`libtailscale/go.sum` replace that dependency graph in a temporary detached
worktree. The materializer requires the same
Tailscale version as the sidecar, checks the Go toolchain, builds with
`-mod=readonly -trimpath -buildvcs=false`, and verifies the Go/Tailscale
versions embedded in the resulting framework binaries.

## Patches

- `libtailscale-conn-cleanup.patch` keys connection cleanup by the Go-owned
  socket. Upstream registers each connection under its C-side descriptor,
  which `TsnetListen` closes right after passing it over `SCM_RIGHTS`. The
  next connection's socketpair reuses that number and overwrites the entry, so
  the earlier connection's cleanup never closes its `net.Conn` (7 of 8
  connections in a test). The Go-owned descriptor stays open until cleanup, so
  its number cannot be reused while the connection is alive. `newConn` returns
  the C descriptor so the added `TestConnCleanupAfterFDReuse` can drive it; the
  test fails without the fix. The same change is proposed upstream in
  [tailscale/libtailscale#69](https://github.com/tailscale/libtailscale/pull/69)
  (issue [tailscale/tailscale#21934](https://github.com/tailscale/tailscale/issues/21934));
  drop this patch once it lands.

  Upstream's framed fd-passing records (`4e3b222`, `258321b9f`) replaced
  truffle's earlier remote-address patch: each connection's remote address
  travels with its descriptor in one fixed-size record and is recorded under
  the received descriptor. The typed-DERP-ID test patch is upstream as well.
- `libtailscale-peer-notifications.patch` exposes the upstream `peerChanges`
  and `noNetMap` watch options to Swift. Truffle subscribes to peer deltas
  and refreshes LocalAPI status on each notification. This keeps discovery
  reactive when Tailscale no longer emits full network maps on iOS.
- `libtailscale-ipn-stream-recovery.patch` reports unexpected successful EOF
  after queued notifications have drained. The consumer can restart the
  indefinite watch even when Tailscale closes it without a transport error.
  Cancellation stops the underlying URLSession, and stale completion
  callbacks cannot interrupt a replacement watch. The materializer runs
  URLSession regression tests against these exact patched Swift sources.

| Input file | SHA-256 |
| --- | --- |
| `libtailscale-conn-cleanup.patch` | `4b473188ad1c7149769b97d19347812802cf07d7a62f98e145b6cb99f2308ace` |
| `libtailscale-peer-notifications.patch` | `23be455e4c2b1637a276c5574d5e976d1c2c1f23ac5e21418e9b6f2db8fe96bf` |
| `libtailscale-ipn-stream-recovery.patch` | `9bc20231b7e8dc81e0a09ddd178e7a6b45396882663ba57d42a0c3bb89c0bb22` |
| `libtailscale/go.mod` | `9969fc1f5b10da3b4f7693b32e576160adcd15432f93528bf9c8c8a4e031ea4b` |
| `libtailscale/go.sum` | `86cc3286bdd486996ef3d012d83de656d605b6e854f8d0a7b8c1e4802424280b` |

## Published artifact

CI built the framework from the inputs above, including `golang.org/x/net
v0.60.0` for the October HTTP/2 security fixes. On October 25, 2026 the
[apple-tests source build](https://github.com/vibecook-dev/truffle/actions/runs/40184193955/job/121141398540)
passed all patched Go packages under the race detector, six exact-source Swift
stream tests, 107 Swift package tests, and iOS device, simulator and example-app
builds, then uploaded the archive. `govulncheck` reports no reachable
vulnerabilities in either Go graph.

The [dependency release](https://github.com/vibecook-dev/truffle/releases/tag/tailscalekit-d1dc45d0-ts1.104.1-go1.27.3)
publishes that uploaded archive unchanged, from source commit
[`25f6e8af4f9b222ea5458f634c916f047bc9c02d`](https://github.com/vibecook-dev/truffle/commit/25f6e8af4f9b222ea5458f634c916f047bc9c02d).
Its SHA-256 matches the checksum the CI run logged, GitHub's stored asset
digest, and a fresh unauthenticated download. Both binaries embed Go 1.27.3,
Tailscale 1.104.1 and `golang.org/x/net` 0.60.0, and both slices carry the
reviewed privacy manifest and Swift interfaces. The root `Package.swift` pins
the URL and checksum, and the published-package CI job resolves it before the
package update merges. The exact patched source archive and `provenance.json`
are attached to the release.

| Field | Value |
| --- | --- |
| Release tag | `tailscalekit-d1dc45d0-ts1.104.1-go1.27.3` |
| Asset | `TailscaleKit.xcframework.zip` (about 24 MiB) |
| SwiftPM checksum | `0895dc67fc6531b4b0183ae63648a8378aacd7a5a04aa35cacfdba5931fe71d3` |
| Patched source archive | `956500d6ccd47e16ea12ffce1d73d3bcca4fd5105f7eb54173de40bcde69026f` |
| Device binary | `7c96a548b97689e86f66e7c24d8e85d80d62f971ff725ea9978ec985efb3fd74` |
| Simulator binary | `ff71b9ca15e62565b864a0f00f96879f374f6cf3cf0310b24b756206fe03c8c4` |

The previous artifact, `tailscalekit-59d4bb82-ts1.104.1-go1.27.2` (checksum
`f8a61a742e42cbf04983213ea28f8a2370802b17fe420ea9cac7f59249c3238f`), stays
published for rollback.

The artifact key includes the wrapper revision, embedded Tailscale version,
and Go toolchain. A wrapper revision alone is insufficient because its Go
dependency is maintained here separately. Use a new tag suffix if the patch
set or Apple build toolchain changes. Never overwrite an existing release
asset: SwiftPM caches and downstream manifests pin those exact bytes.

## Rebuilding

Install the recorded Go version on `PATH`, then from the repository root:

```sh
TAILSCALE_RUN_TESTS=1 apple/scripts/materialize-tailscalekit.sh
```

The script requires full Xcode; set `DEVELOPER_DIR` if it is installed at a
different path. Without a local Xcode, use CI: the `apple-tests` source build
uploads the archive it validated, and a dependency release can publish that
archive unchanged. `TAILSCALE_RUN_TESTS=1` runs the patched C-binding tests with
the race detector against a local test coordination server, followed by the
Swift IPN stream tests in `Tests/TailscaleKitRegression`. The latter compile
the source being built rather than resolving an existing framework. CI
enables both suites. Endpoint normalization and identity address comparisons
are covered by the regular `TruffleTailscaleTests` SwiftPM target on macOS.

The source cache is keyed by revision, so an upgrade does not modify an older
checkout. The source revision and clean-tree checks remain mandatory. The
framework includes the reviewed privacy manifest in both slices and is
ignored by Git. Different toolchains or build paths can change archive bytes;
recompute the checksum for the archive that is actually published.

See [the upgrade procedure](../../docs/tailscale-upgrade.md) for validation,
publication order, and rollback.
