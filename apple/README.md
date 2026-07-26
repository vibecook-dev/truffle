# Truffle Swift — Apple-native mesh runtime

Swift implementation of the Truffle product (RFC 024): same mesh model as
`@vibecook/truffle` — Peers, `appId` isolation, namespaced messages — built
for in-process embedding on iOS/macOS via libtailscale/TailscaleKit instead
of the desktop sidecar.

## Package layout

| Target             | Contents                                                                                                                                                                                                                                  |
| ------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Truffle`          | Product core: identity (AppId / ULID / hostname slug), wire codecs (hello v2, envelope, byte payloads), session handshake + close codes, generation-checked peer registry, `MeshNode` actor, `NetworkBackend` seam, loopback test backend |
| `TruffleSwiftUI`   | `MeshModel` (@Observable — RFC 024 §6.7) + `AuthSafariView` for the interactive login sheet                                                                                                                                               |
| `TruffleTailscale` | Production Layer 0–1: pinned TailscaleKit, login/status/IPN supervision, full-duplex sockets, fail-closed LocalAPI WhoIs, SOCKS-backed URLSession, and `MeshNode.startTailscale`                                                          |

An iOS example lives in `Examples/MeshChatDemo/` — a SwiftUI chat app over an
in-process demo mesh (your node + two bot peers on `LoopbackNetwork`); see
its README.

## Adding it to your project

The package is published from the **repository root** — SwiftPM resolves a URL
dependency only against a root manifest, so point at the repository itself, not
at this directory:

```swift
.package(url: "https://github.com/vibecook-dev/truffle.git", from: "0.7.7")
```

The package identity is `truffle`, so products are referenced as:

```swift
.product(name: "Truffle", package: "truffle")
.product(name: "TruffleSwiftUI", package: "truffle")
.product(name: "TruffleTailscale", package: "truffle")   // iOS only
```

Nothing else is required. TailscaleKit arrives as a checksum-verified binary
target, so there is no Go toolchain, no source build, and no materialization
step for consumers.

Pin an exact commit instead when your release process demands it:

```swift
.package(url: "https://github.com/vibecook-dev/truffle.git", revision: "…")
```

> Versions below `0.7.7` have no SemVer tag — SwiftPM cannot parse the
> `truffle-vX.Y.Z` scheme used before then, and the stray `v0.2.x` tags predate
> this package entirely. Use `from: "0.7.7"` or newer, or pin a revision.

## Build & test (working on Truffle itself)

From the repository root, exactly as a consumer resolves it:

```bash
swift build
# Tests need swift-testing, which CommandLineTools does not bundle:
DEVELOPER_DIR=/Applications/Xcode.app/Contents/Developer swift test
```

`apple/Package.swift` is a compatibility shim for consumers still on a
relative-path dependency. It builds TailscaleKit from source, so it needs the
materializer and a Go toolchain:

```bash
cd apple
./scripts/materialize-tailscalekit.sh
swift build
```

Stick to one toolchain per `.build` directory: CommandLineTools and Xcode may
ship different Swift versions, and mixing them yields "module compiled with
Swift X cannot be imported" — `rm -rf .build` and rebuild if you switch.

## Wire compatibility (RFC 024 §8)

The `Truffle` target implements the shipped `truffle-core` contracts
verbatim — hello v2 (`kind`/`version: 2`/`identity`, snake_case, desktop
field bounds, `device_id ≠ tailscale_id`), the JSON message envelope (no `v`,
no `from_device_id`, receiver-stamped attribution), close codes 4001/4002/4003,
fail-closed inbound WhoIs policy, and the `truffle-{appId}-{slug}` hostname
scheme with `is_app_peer` candidate filtering.

Cross-runtime semantic fixtures live in `Tests/TruffleTests/Fixtures/` and are
decoded by BOTH this suite (`WireTests.swift`) and the Rust suite
(`crates/truffle-core/tests/interop_fixtures.rs`).

Two intentional divergences from Rust internals (documented in `Slug.swift`;
safe because a node only derives its _own_ hostname and remote nodes validate
only the prefix): transliteration uses `CFStringTransform` instead of
`deunicode`, and the slug fallback hash is SHA-256 instead of BLAKE3.

## Production startup

Import `TruffleTailscale`, materialize the pinned XCFramework, and start with:

```swift
let node = try await MeshNode.startTailscale(
    MeshConfiguration(
        appId: "ghosttea",
        deviceName: UIDevice.current.name,
        auth: .interactive { url in /* present AuthSafariView(url:) */ }
    )
)
```

The explicit name keeps `Truffle` independent of the binary runtime and avoids
a SwiftPM target cycle. `MeshNode.start(_:backend:frameTransport:)` remains the
dependency-injection entry point for tests and custom backends.

## Status vs RFC 024 phases

The package implementation for Phase 0 and Phase 1 is present and builds for
generic iOS device and simulator. Release qualification still requires the
RFC's live two-device login, long-idle accept, cancellation, and desktop interop
matrix; those are device gates, not alternate code paths.

Done:

- Identity: AppId validation, device ULID (`device-id.txt`, desktop-compatible),
  DeviceName, 10-step hostname slug + `truffle-{appId}-{slug}` composition
- Wire: hello v2 + envelope codecs with all desktop bounds; base64 `"bytes"`
  schema with 11 MiB cap; 15 MiB envelope bound
- Session: role-ordered handshake, 4001/4002/4003 on the wire, ≤16 control
  frames, full-exchange 10s deadline, 256-handshake concurrency cap,
  Ping/Pong heartbeat with pong-timeout closure, token-guarded session
  replacement (no supersede races), fail-closed WhoIs with explicit
  test-only opt-out
- Mesh: `MeshNode` (start contract with backend cleanup on failed
  bootstrap, listener supervision with backoff, awaited shutdown, events
  streams with bounded buffers + single lag notice), generation-checked
  `Peer`/`PeerRef` (peerGone on stale refs), provisional entries preserved
  across snapshots until netmap merge, desktop-parity peer query
  resolution, `send` / `sendBytes` / `sendJSON` (u64-exact JSON, outbound
  15 MiB bound) / `onMessage` with bounded serial subscriptions that
  auto-cancel when the handle is released
- Production runtime: pinned/checksummed build provenance and license,
  full-duplex fd adoption with blocking work off the cooperative executor,
  cancellable listener, RFC 6455 `/ws` client/server roles, LocalAPI WhoIs,
  IPN restart + status polling, interactive auth events, and URLSession proxy

Pending release gates:

- Run login, bidirectional raw stream, WhoIs, >60-second idle accept, and
  cancellation probes on physical devices
- Run Swift↔desktop JSON/bytes/attribution/app-mismatch interop matrix
- Migrate the research prototype and example chat from loopback to the
  production startup API

## Reserved port

TCP **9417** is the session WebSocket port (`truffle-core` default);
`MeshNode.listen` refuses it, matching desktop.
