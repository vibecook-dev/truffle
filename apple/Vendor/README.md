# TailscaleKit provenance

`TruffleTailscale` consumes a TailscaleKit XCFramework with these inputs:

| Input | Pin |
| --- | --- |
| Upstream | `https://github.com/tailscale/libtailscale.git` |
| Revision | `59d4bb82744915815178e0f0776d60026a397ee7` |
| Embedded Tailscale | `tailscale.com v1.102.3` |
| Go | `1.26.8` |
| Recorded Apple toolchain | Xcode 26.6 (17F113), Apple Swift 6.3.3, Apple silicon macOS |
| License | BSD-3-Clause; see `TAILSCALE-LICENSE` |

Upstream libtailscale still pins Tailscale 1.94.1. The tracked
`libtailscale/go.mod` and `libtailscale/go.sum` replace that dependency graph
in a temporary detached worktree. The materializer requires the same
Tailscale version as the sidecar, checks the Go toolchain, builds with
`-mod=readonly -trimpath -buildvcs=false`, and verifies the Go/Tailscale
versions embedded in the resulting framework binaries.

## Patches

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
| `libtailscale/go.mod` | `f168460643efe18df86cd898fe58a233fc5a1f12a3cc0ed8ceb4c4c246562bf2` |
| `libtailscale/go.sum` | `9b5aa1f09b761d6b5600e4505794c50e30013e57ec0d9a208c7ac0e1df64c180` |

## Published artifact

The root `Package.swift` pins the archive published in the
[dependency release](https://github.com/vibecook-dev/truffle/releases/tag/tailscalekit-59d4bb82-ts1.102.3-go1.26.8-r2).
The local `apple/Package.swift` uses the materialized framework directly.
On September 7, 2026, the local artifact passed the C-binding race tests,
six exact-source IPN stream tests, 70 Swift package tests, and production
iOS device/simulator builds. A fresh clone of the dependency tag, with a
fresh SwiftPM cache and no local framework, downloaded the public archive,
passed all 70 Swift tests, and built `TruffleTailscale` for iOS devices and
simulators. GitHub's uploaded asset digest matches the pinned checksum.

| Field | Value |
| --- | --- |
| Release tag | `tailscalekit-59d4bb82-ts1.102.3-go1.26.8-r2` |
| Asset | `TailscaleKit.xcframework.zip` (about 24 MiB) |
| SwiftPM checksum | `4d97655a8776c0f76c21fa91ef51c2e00a2e7339100f56987391dcb0c67541d2` |
| Device binary | `50e9f45ecbad36e5c11c401477fb5ca622a6604976cb85befc0843ac72c84bfa` |
| Simulator binary | `b9931c2289c7013aed4ca48104cbc4e4e8fa27495c1d495469b055cd0aaa3973` |

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
