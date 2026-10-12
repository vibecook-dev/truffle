# TailscaleKit provenance

`TruffleTailscale` consumes a TailscaleKit XCFramework with these inputs:

| Input | Pin |
| --- | --- |
| Upstream | `https://github.com/tailscale/libtailscale.git` |
| Revision | `59d4bb82744915815178e0f0776d60026a397ee7` |
| Embedded Tailscale | `tailscale.com v1.104.1` |
| Go | `1.27.2` |
| Recorded Apple toolchain | Xcode 26.6 (17F113), Apple Swift 6.3.3, Apple silicon macOS |
| License | BSD-3-Clause; see `TAILSCALE-LICENSE` |

Upstream libtailscale still pins Tailscale 1.94.1. The tracked
`libtailscale/go.mod` and `libtailscale/go.sum` replace that dependency graph
in a temporary detached worktree. The materializer requires the same
Tailscale version as the sidecar, checks the Go toolchain, builds with
`-mod=readonly -trimpath -buildvcs=false`, and verifies the Go/Tailscale
versions embedded in the resulting framework binaries.

## Patches

- `libtailscale-104-testcontrol.patch` adapts the upstream test server to
  Tailscale's typed DERP region IDs. The materializer now tests all Go packages,
  including these C test utilities.

- `libtailscale-remote-address-fd.patch` preserves the authenticated remote
  address across `SCM_RIGHTS`. The receiving file descriptor can differ from
  the sending descriptor, and the sender's descriptor can be reused before
  accept. The patch carries a unique, fixed-size transfer token in a separate
  pending-address map, associates the address with the received descriptor,
  and consumes it after lookup. A queued connection retains an owned socket
  reference until accept or listener cleanup. Connection cleanup uses the Go-owned
  descriptor, which stays open for the connection's lifetime, and holds a file
  reference during shutdown so close/reuse cannot race it. Regression
  tests cover descriptor reuse, split tokens, failed transfers, and queued
  IPv4/IPv6 connections through the C API.
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
| `libtailscale-remote-address-fd.patch` | `6248c03f1d631c221d26a3f6cf7df19b29db78a2d060eac0ef75f9b938d27f94` |
| `libtailscale-peer-notifications.patch` | `23be455e4c2b1637a276c5574d5e976d1c2c1f23ac5e21418e9b6f2db8fe96bf` |
| `libtailscale-ipn-stream-recovery.patch` | `9bc20231b7e8dc81e0a09ddd178e7a6b45396882663ba57d42a0c3bb89c0bb22` |
| `libtailscale-104-testcontrol.patch` | `478c36c17e35a22455daaa75b4addb1d91db26ae17a875821f6c16a89a1a3cb6` |
| `libtailscale/go.mod` | `131bb9f3a7e7b7f7576e5c30439f1caa34a722ed03d85249e4d41e8700c09855` |
| `libtailscale/go.sum` | `86cc3286bdd486996ef3d012d83de656d605b6e854f8d0a7b8c1e4802424280b` |

## Published artifact

The rebuilt local framework uses the inputs above, including `golang.org/x/net
v0.60.0` for the October HTTP/2 security fixes. On October 12, 2026 it passed
all C-binding Go packages under the race detector, six exact-source Swift
stream tests, 70 Swift package tests, and iOS device/simulator builds.
`govulncheck` reports no reachable vulnerabilities in either Go graph.

The [dependency release](https://github.com/vibecook-dev/truffle/releases/tag/tailscalekit-59d4bb82-ts1.104.1-go1.27.2)
was published from source commit
[`b1541c5dff71d870640d69f773ec5a2549ca3cd5`](https://github.com/vibecook-dev/truffle/commit/b1541c5dff71d870640d69f773ec5a2549ca3cd5).
The root `Package.swift` pins its URL and checksum. A fresh clone of that tag,
with fresh SwiftPM/Xcode caches and no materialized local framework, downloaded
the public archive, passed all 70 Swift tests, and built the production runtime
for iOS devices and simulators. GitHub's uploaded digests and the freshly
downloaded archive/binaries match the validated hashes. The exact patched
source archive and `provenance.json` are attached to the release.

These checks were run locally. Publication occurred before GitHub Actions CI
ran on the source commit; the CI audit is pending. Future dependency releases
must pass source-build CI on their committed inputs before publication, then
pass public-download consumer CI before the package update is merged.

| Field | Value |
| --- | --- |
| Release tag | `tailscalekit-59d4bb82-ts1.104.1-go1.27.2` |
| Asset | `TailscaleKit.xcframework.zip` (about 24 MiB) |
| SwiftPM checksum | `f8a61a742e42cbf04983213ea28f8a2370802b17fe420ea9cac7f59249c3238f` |
| Patched source archive | `463de5953d3f94790922b8cc495958aafd3713f856eb7986ec99461d84db47ca` |
| Device binary | `c9a7d20999b41889958f1c5c8dc3f9d3cc94d0ca82ad8409e114ac0a77bc0b55` |
| Simulator binary | `4d6defab38abd26d6d60430efb59b17ad930edb9fe0a42dd524141ba49ce1d54` |

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
different path. `TAILSCALE_RUN_TESTS=1` runs the patched C-binding tests with
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
