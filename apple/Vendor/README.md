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
  the sending descriptor, so the patch carries the source key, remaps the
  address during accept, and consumes it after lookup. Its C integration
  test checks the accepted peer address and that a second lookup fails.
- `libtailscale-peer-notifications.patch` exposes the upstream `peerChanges`
  and `noNetMap` watch options to Swift. Truffle subscribes to peer deltas
  and refreshes LocalAPI status on each notification. This keeps discovery
  reactive when Tailscale no longer emits full network maps on iOS.

| Input file | SHA-256 |
| --- | --- |
| `libtailscale-remote-address-fd.patch` | `1d4f03330fcae2adcf43514c7d7e5464a629aceb5e5a607c4ae091a73b305417` |
| `libtailscale-peer-notifications.patch` | `23be455e4c2b1637a276c5574d5e976d1c2c1f23ac5e21418e9b6f2db8fe96bf` |
| `libtailscale/go.mod` | `f168460643efe18df86cd898fe58a233fc5a1f12a3cc0ed8ceb4c4c246562bf2` |
| `libtailscale/go.sum` | `9b5aa1f09b761d6b5600e4505794c50e30013e57ec0d9a208c7ac0e1df64c180` |

## Prepared artifact

The root `Package.swift` pins the following archive. **Publish this new
artifact before merging the manifest update.** The local `apple/Package.swift`
uses the materialized framework directly and can be validated before release.

| Field | Value |
| --- | --- |
| Release tag | `tailscalekit-59d4bb82-ts1.102.3-go1.26.8` |
| Asset | `TailscaleKit.xcframework.zip` (about 24 MiB) |
| SwiftPM checksum | `14d224f67360e2ac5b12fb31531401313dc63a0caae4838b1d74879e2ff16964` |
| Device binary | `fac825f527b988d5235b43e62677be4b289b362c3f16b1ba2103f1a2ca1e9aec` |
| Simulator binary | `b7f8582d2a873c193d4edb5fbf4bf4dbaa48a9bcfd5b23411c6dab68a4212786` |

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
the race detector against a local test coordination server. CI enables it.

The source cache is keyed by revision, so an upgrade does not modify an older
checkout. The source revision and clean-tree checks remain mandatory. The
framework includes the reviewed privacy manifest in both slices and is
ignored by Git. Different toolchains or build paths can change archive bytes;
recompute the checksum for the archive that is actually published.

See [the upgrade procedure](../../docs/tailscale-upgrade.md) for validation,
publication order, and rollback.
