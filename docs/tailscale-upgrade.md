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
