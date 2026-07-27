# RFC 024: Truffle Swift — Apple-native mesh runtime

**Status:** Phases 0, 1, 1b and 2 implemented and published; Phase 3 not started; Phase 4 partial. Five device gates remain — see the end of §12.  
**Author:** James Yong + Grok  
**Date:** 2026-07-16 (revised same day: wire contract aligned with shipped `truffle-core`; post-review transport, identity, payload, and security requirements incorporated)  
**Depends on:** RFC 008 (vision), RFC 012 (layered architecture), RFC 017 (identity & namespacing), RFC 021 (raw transport JS API — *API shape*), RFC 022 (peer handle API), RFC 023 (HTTP serving — *later phases*)  
**Related research:** `project100/research/tailscale/` (iOS embed prototype with TailscaleKit / libtailscale — lives **outside** this repo, see §15)  
**Does not supersede:** Desktop Node/Rust/sidecar stack remains canonical for Node, CLI, Tauri

---

## 1. Problem Statement

### 1.1 Tailscale alone is too low-level for product apps

Embedding Tailscale (tsnet / libtailscale / LocalAPI) is necessary infrastructure, but product code should not juggle IPN bus timeouts, `BrowseToURL`, SOCKS credentials, MagicDNS, or stringly peer IDs. That is why Truffle exists on TS/Rust:

```ts
const mesh = await createMeshNode({ appId: 'field-tools', deviceName: 'alice-laptop' });
const bob = await mesh.peer('bob-laptop', { waitMs: 5000 });
await bob?.send('chat', Buffer.from('hello'));
```

### 1.2 Desktop Truffle cannot ship as-is on iOS

Current stack:

```text
App (TS/Rust) → NAPI / native → truffle-core → Go sidecar process → tsnet
```

iOS constraints:

| Desktop assumption | iOS reality |
|---|---|
| Long-lived sidecar process | No durable helper processes for ordinary apps |
| NAPI / Node | Not available |
| Bridge: local TCP + binary headers into tokio `TcpStream` | Must use in-process userspace stack |
| System-wide VPN optional | App-scoped mesh preferred; Network Extension is a different product |

Research prototype (`project100/research/tailscale/ios-prototype`, outside this repo) proved the viable L0 path:

```text
Swift app → TailscaleKit → libtailscale.a (userspace tsnet) → tailnet
```

Interactive login via in-app `SFSafariViewController`, IPN bus + `backendStatus` polling with bus restart after URLSession timeouts, peer list, HTTP over embedded SOCKS proxy — **without** the Tailscale App Store app.

### 1.3 Gap

We have:

- A **product API** (Truffle) for TS/Rust/desktop  
- A **working L0** on iOS (embed + auth + status)  

We lack:

- A **Swift-native Truffle** that turns L0 into the same *product* (peers, `appId`, messages, familiar I/O)  
- Explicit **interop rules** so iPhone Truffle can talk to laptop `@vibecook/truffle` when we choose  

---

## 2. Decision

**Ship Truffle Swift as a first-class, Swift-native runtime of the Truffle product.**

- **Same public product model** as `createMeshNode` / Peer / namespaced messages / mesh HTTP.  
- **Different L0/L1 implementation** on Apple: in-process **libtailscale + TailscaleKit**, not the Go sidecar + Rust bridge.  
- **Shared contracts** (identity, `appId`, wire envelopes) so cross-runtime interop is possible without merging codebases day one.  
- **Not** a thin re-export of LocalAPI/IPN types. Callers never see `Ipn.Notify` unless they opt into an escape hatch.

This is option **A** from the architecture discussion: Swift-native Truffle on TailscaleKit. Rust+uniFFI is deferred (see §11).

---

## 3. Goals

1. **Product parity of intent** with `@vibecook/truffle` for the MVP surface (§6), not line-by-line port of every NAPI method.  
2. **Zero Tailscale App Store dependency** for end users on iOS (and optionally macOS embed mode).  
3. **In-app auth UX**: interactive web login (`SFSafariViewController` / `ASWebAuthenticationSession`) or short-lived auth keys from the host app backend.  
4. **Peer objects, not string soup** (RFC 022 spirit): dial/send take `Peer`; ids are fields for display/persistence.  
5. **`appId` isolation** (RFC 017): different apps on one tailnet do not confirm as Truffle peers or exchange application traffic.  
6. **Apple-shaped I/O**: `URLSession` over the mesh, async/await, `AsyncStream` events, SwiftUI-friendly observation.  
7. **Production-grade lifecycle**: bus timeout recovery, status polling, NeedsLogin / NeedsMachineAuth / Running — learned from the prototype.  
8. **SPM-first packaging** for iOS 18.1+ (aligned with current TailscaleKit build) and macOS where feasible.  
9. **Wire-compatible with Rust/TS nodes from Phase 1** — Swift speaks the shipped session contracts (hello v2, JSON envelope, port 9417, §8) from day one; Phase 2 *verifies* live interop rather than designing it.

## 4. Non-Goals (this RFC / v0.1–v0.2)

| Non-goal | Rationale |
|---|---|
| Port Go sidecar + Rust bridge to iOS | Wrong process model; superseded by libtailscale embed |
| Full QUIC / `mesh.ws` / `mesh.serve` Funnel in v0.1 | High value later; not required to prove the product |
| System Network Extension / whole-device VPN | Different product class (RFC-sized separately if ever needed) |
| Android / Windows Swift | Out of scope |
| Bit-identical NAPI surface | Swift gets idiomatic names; semantic parity matters |
| Replace desktop Rust stack | Node/CLI/Tauri stay on current architecture |
| Browser peers | Still out of scope per earlier RFCs |

---

## 5. Architecture

### 5.1 Layering (maps to RFC 012, Apple runtime)

```text
┌─────────────────────────────────────────────────────────────┐
│  Layer 5 (later): Serve / publish / advanced HTTP           │
├─────────────────────────────────────────────────────────────┤
│  Layer 4: App services — send/onMessage, (later) files/store│
├─────────────────────────────────────────────────────────────┤
│  Layer 3: Mesh — Peer registry, appId filter, discovery     │
├─────────────────────────────────────────────────────────────┤
│  Layer 2: Transport — URLSession, dial/listen streams       │
├─────────────────────────────────────────────────────────────┤
│  Layer 1: TruffleTailscale — status, auth, bus, recovery    │
├─────────────────────────────────────────────────────────────┤
│  Layer 0: libtailscale / TailscaleKit (userspace tsnet)     │
└─────────────────────────────────────────────────────────────┘
```

**Layer 0–1** are the hardened form of `project100/research/tailscale/ios-prototype`.  
**Layer 2–4** are the Truffle product.  
**Host apps** only import Layer 3–4 (+ optional SwiftUI helpers).

### 5.2 Runtime split (one API, two engines)

```text
              Truffle product API
         (MeshNode, Peer, messages, …)
                    │
     ┌──────────────┴──────────────┐
     │                             │
 Desktop / Node / Tauri      iOS / macOS (embed)
     │                             │
 truffle-core + sidecar      Truffle Swift + TailscaleKit
```

| Concern | Shared? | Mechanism |
|---|---|---|
| `createMeshNode` / `MeshNode` semantics | Yes | This RFC + docs site parity tables |
| Peer handle model | Yes (semantics) | RFC 022 generation/identity rules; live TS handle vs immutable Swift snapshot (§6.3) |
| Session protocol (hello v2, WS port 9417) | Yes (Phase 1) | RFC 017 §8 as shipped; dual impl (§8) |
| Envelope JSON schema | Yes (Phase 1) | Shipped `truffle-core` schema (§8.3); dual impl |
| Hostname scheme (`truffle-{appId}-{slug}`) | Yes (Phase 1) | RFC 017 §4 — desktop candidate filter depends on it (§7.2) |
| L0 process model | No | Sidecar vs in-process |
| Auth presentation | No | Browser OS vs SFSafariViewController |

### 5.3 Package layout (proposed)

Monorepo options (choose at implementation time):

**Option 1 — inside `p008/truffle` (preferred for API cohesion):**

```text
truffle/
  packages/          # existing JS
  crates/            # existing Rust
  apple/
    Package.swift
    Sources/
      Truffle/                 # public product API
      TruffleTailscale/        # L0–L1 (auth, bus, status)
      TruffleSwiftUI/          # optional observation helpers
    Tests/
    Examples/
      EmbedDemo/               # evolved from research prototype
  vendor/ or binary target     # TailscaleKit.xcframework build story
  docs/rfcs/024-truffle-swift.md
```

**Option 2 — sibling repo `truffle-swift`** linked by git submodule + mirrored RFCs.  
Use only if monorepo SPM + Go/Xcode build friction is intolerable.

### 5.4 Dependency on TailscaleKit

- **Pin:** The package MUST name an exact upstream libtailscale commit (or immutable release tag). Building from the default branch or an unpinned shallow clone is forbidden.  
- **Build:** A reproducible script (from research: `DEVELOPER_DIR`, `make ios-fat`) produces `TailscaleKit.xcframework` with recorded Xcode, Go, and SDK versions.  
- **Link:** A checksummed SPM binary target or provenance-attested CI artifact; do not require every app consumer to compile Go.  
- **Patch set:** Any Truffle-required changes to the Swift wrapper (full-duplex connections, listener lifecycle, WhoIs) live as a documented, reviewable patch series against the pinned commit.  
- **Compliance:** Ship upstream license/notices, artifact checksums, and an upgrade/security-response policy with the package.  
- **Minimum OS:** Match framework (currently iOS 18.1+ / as built).  
- **Escape hatch:** Advanced module `TruffleTailscale` may expose `TailscaleNode` for apps that need raw dial before higher layers exist.

### 5.5 Full-duplex transport prerequisite

The current research TailscaleKit wrapper is not sufficient for the Truffle
session protocol as-is: its public `IncomingConnection` is receive-only, its
`OutgoingConnection` is send-only, and accepted connections expose a remote
address but not a WhoIs-authenticated identity. WebSocket sessions require a
single full-duplex byte stream in both directions.

Phase 0 therefore includes a required adapter/patch that provides:

- Full-duplex `read` + `write` + `close` for both dialed and accepted TCP streams  
- A cancellable listener whose idle accept timeout does **not** permanently close port 9417  
- Remote endpoint metadata on accepted streams and LocalAPI WhoIs lookup  
- Dedicated execution for blocking libtailscale/C calls (`poll`, `accept`, `dial`); an `async` actor method alone is not permission to block Swift's cooperative executor  
- A vetted RFC 6455 implementation that can adopt the connected libtailscale stream for both client and server roles  

The WebSocket implementation choice may be an existing SPM dependency or a
small isolated adapter over a vetted library, but Phase 0 must prove on device
that it can adopt a dialed stream, adopt an accepted stream, exchange data in
both directions, cancel cleanly, and survive more than 60 seconds idle. Writing
a new general-purpose WebSocket stack inside Truffle is out of scope.

---

## 6. Public API (Swift)

Naming is idiomatic Swift; **semantics** track `createMeshNode` / RFC 022.

### 6.1 Configuration

```swift
public struct MeshConfiguration: Sendable {
    /// Required. RFC 017: `^[a-z][a-z0-9-]{1,31}$`
    public var appId: String

    /// Human-readable device label (hostname advertisement strategy: §7).
    public var deviceName: String

    /// Where node + Tailscale state live (Application Support; exclude from backup).
    public var stateDirectory: URL?

    public var controlURL: URL?          // default Tailscale control plane
    public var ephemeral: Bool           // default false
    public var auth: MeshAuth

    public var logger: (any MeshLogger)?
}

public enum MeshLogLevel: Sendable { case debug, info, warning, error }

public protocol MeshLogger: Sendable {
    func log(level: MeshLogLevel, message: String)
}

public enum MeshAuth: Sendable {
    /// Interactive Tailscale login; host presents `url` (in-app Safari recommended).
    case interactive(onAuthRequired: @MainActor @Sendable (URL) async -> Void)

    /// Pre-minted or callback-minted auth key (B2C / automated).
    case authKey(String)
    case authKeyProvider(@Sendable () async throws -> String)

    /// Reuse existing state dir credentials only; fail if NeedsLogin.
    case existingState
}
```

### 6.2 Lifecycle

```swift
public actor MeshNode {
    public static func start(_ config: MeshConfiguration) async throws -> MeshNode
    public func stop() async
    public func refresh() async throws
    public func waitUntilRunning(timeout: Duration) async throws

    public var phase: MeshPhase { get async }
    public var localPeer: Peer { get async }
    public var dnsName: String? { get async }   // MagicDNS FQDN when available
    public var tailnetIPs: [String] { get async }

    /// Each access mints a NEW independently-buffered stream. Events are edges;
    /// callers re-read `phase` / `peers()` after a reported lag or resubscribe.
    public var events: AsyncStream<MeshEvent> { get async }
}

public enum MeshPhase: String, Sendable {
    case starting
    case needsLogin
    case needsMachineAuth
    case running
    case stopping
    case stopped
    case failed
}

public enum MeshEvent: Sendable {
    case phase(MeshPhase)
    case authRequired(URL)
    case peerUpsert(Peer)
    case peerLeft(Peer)
    case message(MeshMessage)
    case health(String)   // non-fatal bus restarts, etc.
}
```

`events` uses a bounded newest-value buffer of 256 events per subscriber. If a subscriber
falls behind, the runtime emits one `.health("event stream lagged; resync")`
when space is available; consumers then re-read the level APIs. Terminating a
stream removes its continuation from the actor. Streams do not replay history,
but the current phase is emitted immediately when a stream is created.

**Start contract (critical — prototype lessons):**

1. Create embedded Tailscale node (`authKey` optional).  
2. Start LocalAPI loopback + IPN bus watch.  
3. **IPN bus uses ~60s URLSession timeout** → library MUST auto-restart bus and must not treat timeout as fatal.  
4. Poll `backendStatus` while not `running` (≈2s).  
5. When a new `BrowseToURL` / `AuthURL` appears, emit `authRequired` first, then invoke `MeshAuth.interactive` once on the main actor. Deduplicate the same URL until it changes or `refresh()` begins a new auth attempt.  
6. On sheet dismiss / login finished / explicit `refresh()`, re-poll and re-`up()` as needed.  
7. `start()` returns after the node is started and watchers are live; it does not wait for `running`. Callers that require connectivity use `waitUntilRunning(timeout:)`. Auth remains event-driven.  
8. Once running, supervise the port-9417 listener independently from the IPN bus. Idle accept timeouts, foreground/background transitions, and transient accept errors recreate the listener with bounded backoff; they do not silently end inbound messaging.  
9. `stop()` is idempotent: stop accepting, cancel handshakes and subscriptions, close sessions, stop bus/poll tasks, then close the embedded node. It returns only after owned tasks have terminated.

For `.existingState`, `start()` performs an initial status read before returning
and throws `MeshError.needsLogin` / `.needsMachineAuth` instead of returning an
unusable node. Other auth modes may return in a non-running phase as described
above.

### 6.3 Peers (RFC 022)

```swift
public struct PeerRef: Hashable, Sendable, CustomStringConvertible {
    /// Opaque process-local `{tailscaleId}:{generation}` token.
    /// Constructed only by Truffle; never persist it across launches.
    public let description: String
}

public struct Peer: Identifiable, Hashable, Sendable {
    /// Stable for this registry generation. A leave + rejoin gets a new ref.
    public let ref: PeerRef
    public var id: PeerRef { ref }

    /// Durable app-level identity (ULID) after hello — nil before hello.
    /// Use for persistence across restarts; never for row identity.
    public let deviceId: String?

    /// Tailscale stable node ID — the routing key; matches `tailscale_id` on
    /// the wire (RFC 017 §8). NOT the rotating WireGuard node key, and never
    /// pretend this is deviceId.
    public let tailscaleId: String
    public let generation: UInt64

    public let displayName: String
    public let hostname: String
    public let tailnetIPs: [String]
    public let online: Bool
    public let appId: String?
    public let isLocal: Bool

    /// Row IDENTITY (`id`/hash) is `ref` — stable per generation, so SwiftUI
    /// rows never churn when hello lands. EQUALITY is full content: snapshots
    /// of the same generation compare unequal once `deviceId`/`online`/
    /// metadata change, so Equatable-based view diffing observes updates.
    /// (Ref-only equality would make SwiftUI skip re-rendering confirmed
    /// peers — found building the example app.)
    public static func == (lhs: Peer, rhs: Peer) -> Bool
    public func hash(into hasher: inout Hasher)
}

extension MeshNode {
    public func peers() async -> [Peer]
    public func peer(_ query: String, waitMs: Int = 0) async throws -> Peer?
}
```

**Query resolution order** (align with Rust `resolve_peer_id` as much as practical):

1. Exact process-local `PeerRef.description`  
2. Exact `deviceId` (ULID)  
3. Exact `tailscaleId`  
4. Exact hostname (case-sensitive) or display/device name (Unicode case-insensitive)  
5. Unique `deviceId` prefix (≥4 characters) if unambiguous  
6. Tailnet IP  

Not found / wait timeout → nil. Ambiguous → `MeshError.peerAmbiguous`; shutdown
and invalid queries also throw. This matches the desktop distinction instead of
collapsing ambiguity, timeout, and absence into nil.

**Stale snapshots:** `Peer` is a value snapshot, not a live handle (RFC 022
desktop handles are live). Every peer-taking call resolves `Peer.ref`, including
its generation. If that generation left the registry, the call throws
`MeshError.peerGone` even if the same Tailscale stable ID has since rejoined.
Fresh snapshots for the same live generation share identity (`id`, hash) but
compare equal only when their content also matches.

### 6.4 Namespaced messages

```swift
public struct MeshMessage: Sendable {
    public let from: Peer
    public let namespace: String
    public let msgType: String
    /// Raw JSON bytes for the envelope's `payload` value (not the whole envelope).
    public let payloadJSON: Data
    public let timestamp: Date?

    public func decodePayload<T: Decodable>(_ type: T.Type) throws -> T
    /// Decodes the normative `msg_type == "bytes"` base64 wrapper; otherwise nil.
    public func payloadBytes() throws -> Data?
}

extension MeshNode {
    /// Convenience alias for `sendBytes`; never content-sniffs the data.
    public func send(
        to: Peer,
        namespace: String,
        data: Data
    ) async throws

    public func sendBytes(
        to: Peer,
        namespace: String,
        data: Data
    ) async throws

    public func sendJSON<T: Encodable>(
        to: Peer,
        namespace: String,
        msgType: String = "message",
        payload: T
    ) async throws

    public func onMessage(
        namespace: String,
        handler: @escaping @Sendable (MeshMessage) async -> Void
    ) async -> MessageSubscription
}

public actor MessageSubscription {
    /// Idempotent. Also called automatically when the subscription is released.
    public func cancel()
}
```

`send` / `sendBytes` use the shipped explicit-byte representation and do not
copy desktop's deprecated content sniffing:

```json
{
  "namespace": "chat",
  "msg_type": "bytes",
  "payload": { "encoding": "base64", "data": "aGVsbG8=" },
  "timestamp": 1752675000000
}
```

`sendJSON` preserves the encoded JSON value under `payload`. Node/TS consumers
inspect `msgType`; a shared helper for `msg_type == "bytes"` decodes the base64
wrapper. Semantic fixtures cover scalars, objects, arrays, null, Unicode, and
opaque bytes. The Swift API intentionally follows `truffle-core.send_bytes`
rather than perpetuating the legacy JS `send(Buffer)` sniffing behavior.
Because base64 expands data, v0.1 caps the raw input to `sendBytes` at **11 MiB**
and rejects a decoded byte payload above the same limit.

**v0.1 transport for messages:** WebSocket over mesh TCP on port **9417** — the exact session protocol desktop `truffle-core` ships today (WS listener on `ws_port` 9417, hello v2 as first frame, `appId` filter, ping keepalive) — see §8. Swift↔Swift v0.1 speaks the real protocol from day one; there is **no** Swift-only interim protocol to migrate off in Phase 2.  
**Not** “send via Tailscale WhoIs magic” without a real app channel.

Each message subscription invokes its handler serially and in receive order.
It has a bounded queue of 256 messages so a slow handler cannot grow memory
without limit. Overflow cancels that subscription and emits a health event;
delivery is at-most-once and there is no replay in v0.1.

### 6.5 HTTP / URLSession (high value, low cost)

```swift
extension MeshNode {
    /// URLSession that proxies through the embedded node (SOCKS / TailscaleKit).
    public func urlSession(
        configuration: URLSessionConfiguration = .default
    ) async throws -> URLSession

    public func httpGet(_ url: URL) async throws -> (Data, URLResponse)
}
```

This covers “phone home to any tailnet HTTP service” without implementing serve in v0.1.

The supplied configuration is copied before Truffle installs its authenticated
SOCKS5 proxy settings; the caller's cache, cookie, timeout, and protocol options
are preserved. v0.1 rejects background-session configurations and conflicting
preconfigured proxies rather than returning a session that bypasses the mesh.

### 6.6 Raw dial / listen (Phase 1b)

This is the public raw-transport surface delivered in Phase 1b. Its internal
full-duplex stream and listener primitives are already mandatory in Phase 0
because the Phase 1 session WebSocket is built on them (§5.5).

```swift
extension MeshNode {
    public func dial(to: Peer, port: UInt16) async throws -> any MeshConnection
    public func listen(port: UInt16) async throws -> any MeshListener
}

public protocol MeshConnection: Sendable {
    func read(_ max: Int) async throws -> Data?
    func write(_ data: Data) async throws
    func close() async
}

public struct MeshAcceptedConnection: Sendable {
    public let connection: any MeshConnection
    /// Numeric remote IP:port from the authenticated tailnet connection.
    public let remoteEndpoint: String
}

public protocol MeshListener: Sendable {
    func accept() async throws -> MeshAcceptedConnection
    func close() async
}
```

Map both dialed and accepted sockets to the same full-duplex abstraction. EOF
returns nil (never an empty-data sentinel); writes either consume all bytes or
throw. The public API never exposes borrowed C file descriptors.

### 6.7 SwiftUI helper (optional package)

```swift
@MainActor
@Observable
public final class MeshModel {
    public private(set) var phase: MeshPhase
    public private(set) var peers: [Peer]
    public private(set) var authURL: URL?
    public private(set) var lastError: String?

    public init(config: MeshConfiguration)
    public func start() async
    public func stop() async
    public func refresh() async          // recover after login
    public func presentAuthInSafari()    // SFSafariViewController bridge
}
```

Host apps should need ~20 lines to Connect → Safari → Connected.

### 6.8 Explicit non-API in v0.1

- `fileTransfer()`, synced store, request/reply framework  
- `mesh.serve` / Funnel / reverse proxy (RFC 023)  
- QUIC / UDP namespaces  
- Multi-profile Tailscale account switching  

---

## 7. Identity, hostname, and `appId`

Follow RFC 017 / 022 with Swift-specific advertisement:

### 7.1 Device ULID

- Generate once; store under `stateDirectory/truffle-device-id`.  
- Survives Tailscale re-auth when file preserved.  
- `ephemeral` controls Tailscale node state only; it does **not** rotate the Truffle device ULID. The ULID rotates only when its state file is explicitly deleted or a future API requests identity rotation.

### 7.2 Tailscale hostname

Adopt the RFC 017 §4 scheme **unchanged**:

```text
Tailscale hostname: truffle-{appId}-{slug(deviceName)}
```

This is not optional for interop: the desktop provider filters peer *candidates*
by the `truffle-{app_id}-` prefix (`provider.rs::is_app_peer`) **before**
any hello happens. A Swift node advertising a bare device name never enters a
desktop node's Layer 3 registry — Phase 2 interop would be dead on arrival.

The prefix is candidate filtering only; identity is confirmed by the hello
(RFC 017 §8). The RFC 017 bug class was *trusting* the prefix alone — the fix
was prefix + non-empty slug + hello verification, not dropping the prefix.

Peer candidacy vs membership:

- Candidates = hostname-prefix matches from the netmap  
- `peers()` returns candidates immediately, including pre-hello snapshots with `deviceId == nil`  
- Confirmed membership = completed hello for the same `appId`; only confirmed peers have a `deviceId` or may deliver application messages  
- An `appId`-mismatched hello closes the session and never confirms the candidate, but the hostname-derived candidate may remain visible until Layer 3 removes it  
- If a valid inbound hello races ahead of the netmap event, create a provisional registry entry keyed by its authenticated Tailscale ID, then merge the later Layer 3 metadata into the same generation; never deliver a message whose `from` cannot be projected as a `Peer`  
- Optional debug flag: `includeAllTailnetPeers` for Phone-Home style tools  

### 7.3 Peer projection (honest fields)

Never set `deviceId = tailscaleId`.  
`Peer.deviceId` is nil until hello; `Peer.tailscaleId` always holds the routing id.

---

## 8. Wire interop (implement in Phase 1, verify in Phase 2)

Goal: Swift phone ↔ TS/Rust laptop on same `appId`.

The wire contract is **not a design problem** — it is already designed, shipped,
and versioned in `truffle-core`. Swift implements the existing contracts in
Phase 1 (they are what Swift↔Swift messaging runs on); Phase 2 is *verification*
(live interop matrix), not protocol work.

### 8.1 Session protocol (as shipped)

1. **Endpoint:** WebSocket over mesh TCP port **9417**, client request path **`/ws`** (`truffle-core` `ws_port` default). The server accepts that path; no subprotocol is negotiated.  
2. **Role ordering:** the dialing/client side completes the RFC 6455 upgrade, sends its hello, then reads the server hello. The accepting/server side upgrades, reads and validates the client hello, performs inbound identity verification, then sends its hello.  
3. **Hello frame:** emitted as a WebSocket **Text** frame containing hello v2 (§8.2). Receivers accept Text or Binary JSON for compatibility. The first application-level frame in each direction must be hello; up to 16 control frames may precede it.  
4. **Timeouts:** hello read timeout **5s**. The complete incoming upgrade + hello exchange is bounded by **10s**.  
5. **Close codes:** `appId` mismatch → **4001**; malformed, invalid, or missing hello → **4002**; claimed `tailscale_id` contradicting authenticated identity → **4003**. A rejected hello never confirms the peer and never enables application traffic.  
6. **Application frames:** compact JSON envelopes (§8.3) are emitted as WebSocket **Binary** frames; receivers also accept Text frames containing JSON.  
7. **Bounds:** maximum WebSocket frame/message size **16 MiB**; maximum **256** simultaneous incoming upgrade/hello handshakes. Envelope field and payload bounds in §8.3 apply within that transport limit.  
8. **Keepalive:** after hello, send Ping every **10s** and require a Pong within **30s**. Peers must answer Ping according to RFC 6455. Ping payload contents are not semantically significant.  

These values describe the current desktop defaults and are the v0.1 Swift
contract. A future configurable limit may be stricter locally, but a release
must document any value that can reject otherwise-valid peers.

#### 8.1.1 Authenticated identity policy

The two connection roles have different evidence:

- **Inbound/server:** resolve the accepted connection's remote address through LocalAPI WhoIs. In production Swift builds, absence of a concrete stable node ID fails closed with 4003 (`identity unavailable`); a concrete ID that differs from the hello also closes with 4003. Loopback/mock tests may opt into unverified identity explicitly.  
- **Outbound/client:** the tailnet route authenticates the dialed endpoint; require the server hello's `tailscale_id` to equal the exact Layer 3 peer that was dialed. A mismatch aborts the connection.  

Desktop `truffle-core` currently fails open when its sidecar supplies no usable
WhoIs identity on an inbound connection. That is shipped behavior, not the
desired Swift security policy. It does not prevent interop when WhoIs succeeds,
and the difference must be covered by the live interop matrix. Swift cannot
claim 4003 support until the Phase 0 backend exposes WhoIs.

### 8.2 Hello envelope (hello v2 — `session/hello.rs`, RFC 017 §8)

```json
{
  "kind": "hello",
  "version": 2,
  "identity": {
    "app_id": "field-tools",
    "device_id": "01H…",
    "device_name": "Alice's iPhone",
    "os": "ios",
    "tailscale_id": "nodeid…"
  }
}
```

- Field names are **snake_case**; `version >= 2` is required (v1 is pre-RFC-017 and rejected).  
- `device_id` must be non-empty and distinct from `tailscale_id`; identity string fields use the desktop maximums (`app_id` 32 bytes, `device_name` 512, `device_id` 64, `tailscale_id` 256, `os` 32).  
- `os`: Swift sends `"ios"`; macOS embed sends `"darwin"` to match desktop convention.  
- **Protocol versioning lives here**, in the hello — not per message envelope.  

### 8.3 Message envelope (as shipped — `envelope/mod.rs`)

```json
{
  "namespace": "chat",
  "msg_type": "message",
  "payload": { },
  "timestamp": 1752675000000
}
```

- There is **no `v` field and no `from_device_id`** — do not invent them; versioning is in the hello (§8.2).  
- The optional wire `from` field is not trusted. The receiver overwrites/ignores it and attributes the message from the connection's authenticated Tailscale ID; a sender-supplied value never controls `MeshMessage.from`.  
- Unknown fields are silently ignored on decode (forward compatibility) — the Swift decoder must match.  
- **JSON is the only interop-compatible encoding.** RFC 009 binary frames were never shipped for the session; MessagePack would be a coordinated cross-runtime change, not a Swift choice.  
- Locally emitted namespace and message-type strings must be non-empty and at most **1,024 UTF-8 bytes** each. Received envelopes are bounded primarily by total encoded size for desktop compatibility. v0.1 rejects an encoded envelope larger than **15 MiB** before JSON payload decoding, leaving headroom beneath the 16 MiB WebSocket limit.  
- Opaque bytes use `msg_type: "bytes"` and payload `{ "encoding": "base64", "data": "…" }`; no alternate array/string encoding is emitted by Swift.  

### 8.4 Semantic fixtures and transcripts

Phase 1 adds shared semantic JSON fixtures (hello + representative envelopes)
decoded and validated by **both** the Rust and Swift test suites. Tests compare
the parsed structure, required omissions, numeric ranges, Unicode, and byte
payload decoding — not incidental object-key order or whitespace.

Separate WebSocket transcript tests assert `/ws`, role-specific hello ordering,
Text/Binary opcodes, timeouts, close codes, maximum sizes, Ping/Pong behavior,
and attribution. Exact byte fixtures are used only if a future RFC defines a
canonical JSON serialization.

### 8.5 Phase 1 vs Phase 2

Because Phase 1 Swift↔Swift already runs on the shipped contracts, Phase 2
reduces to: validating the fail-closed Swift WhoIs path on real devices, auditing
the desktop fail-open compatibility case, the live Node ↔ iOS interop matrix,
and peer-query parity gaps.  
HTTP Phone Home to non-Truffle tailnet services works immediately via `urlSession()`.

---

## 9. Auth UX (normative)

| Mode | Behavior |
|---|---|
| `.interactive` | Emit `authRequired`, then invoke the deduplicated main-actor callback with the URL; recommend `SFSafariViewController`. On dismiss, library `refresh()`. Auto-poll until Running or timeout. |
| `.authKey` / provider | Set before `up`; no browser. |
| `.existingState` | No interactive prompt; fail with `MeshError.needsLogin` if required. |

**Must handle:**

- Bus timeout during long IdP login (restart bus; do not surface as fatal alone)  
- `NeedsMachineAuth` distinct phase  
- Re-login after logout / wiped state  
- State dir excluded from iCloud backup (private keys)

**Must not:**

- Bake long-lived org auth keys into the IPA  
- Require the Tailscale App Store app  

---

## 10. Error model

```swift
public enum MeshError: Error, Sendable {
    case invalidAppId(String)
    case needsLogin
    case needsMachineAuth
    case notRunning
    case peerNotFound(String)
    case peerAmbiguous(query: String, candidates: [String])
    /// A `Peer` snapshot's underlying node re-keyed or left the mesh between
    /// snapshot and use (desktop `PeerGone` equivalent — RFC 022 §7.7).
    case peerGone(String)
    case identityUnavailable(String)
    case identityMismatch(claimed: String, authenticated: String)
    case invalidPayload(String)
    case payloadTooLarge(actual: Int, limit: Int)
    case protocolViolation(String)
    case dialFailed(String)
    case listenFailed(String)
    case transport(String)
    case timeout(String)
    case stopped
}
```

Transient health (bus restart) → `MeshEvent.health`, not throw from `peers()`.

---

## 11. Alternatives considered

### 11.1 Rust core + uniFFI + libtailscale provider

**Pros:** One file-transfer/store implementation.  
**Cons:** `NetworkProvider` today returns bridged tokio `TcpStream`s; months of porting; larger binary; harder iOS debug.  
**Verdict:** Defer until Phase 1–2 prove product value and wire format is stable. Revisit as RFC 02x if duplication of L4 becomes costly.

### 11.2 Ship Go sidecar as Network Extension

**Pros:** Closer to desktop.  
**Cons:** VPN entitlement, UX, battery, App Store scrutiny; overkill for app-scoped mesh.  
**Verdict:** Reject for default Truffle Swift.

### 11.3 Swift only L0, logic on a hub server

**Pros:** Tiny client.  
**Cons:** Not mesh; SPOF; fights Truffle vision.  
**Verdict:** Reject as primary design; hub remains a valid *app* pattern using `urlSession()`.

### 11.4 Depend on system Tailscale app + IPC

**Pros:** Official client.  
**Cons:** Two-app install; bad consumer UX; no control over auth.  
**Verdict:** Reject as default; optional future “use system Tailscale if present” is non-goal for v0.1.

---

## 12. Implementation plan

> **Progress note (2026-07-27).** Checkboxes below are ticked only where the
> claim is backed by something checkable — a shipped symbol, a passing test, a
> CI job, or a recorded physical-device observation. Two evidence sources are
> cited by shorthand:
>
> - **[pkg]** — this repository: `apple/Sources`, the 68-test `TruffleTests`
>   suite, `crates/truffle-core/tests/interop_fixtures.rs`, and the
>   `apple-tests.yml` jobs (`Swift package as published` builds the package the
>   way a consumer does and compiles `TailscaleKitBackend` for both iOS slices).
> - **[dev]** — physical-device evidence recorded **outside this repository**,
>   in Ghosttea's `apple/GhostteaKit/Compatibility/ios-device-evidence.md`
>   (iPhone 14 Pro, iOS 26.5, signed build, live tailnet). That record is the
>   qualification evidence for the device gates; it was taken at Truffle
>   revision `071264b0`, which predates the 0.7.7/0.7.8 packaging releases but
>   contains identical Swift sources.
>
> Unticked items are genuinely open, not merely unrecorded.

### Phase 0 — Foundation (extract prototype)

**Deliverable:** SPM package `TruffleTailscale` + example app.

- [x] Pin libtailscale; reproducibly build and checksum TailscaleKit.xcframework; record licenses, toolchain, provenance, and patch set — `apple/Vendor/README.md`; artifact now also published and checksum-pinned at `tailscalekit-5e89501d` **[pkg]**
- [x] `TailscaleRuntime`: start/stop, state dir, auth modes — `TailscaleKitBackend` **[pkg]**
- [x] IPN bus + auto-restart + `backendStatus` polling — `busConsumer` / `busRestartTask` / `pollTask` **[pkg]**
- [x] Full-duplex dialed + accepted stream adapter (§5.5), with blocking C operations off the cooperative executor — dedicated dispatch queue per blocking call **[pkg]**
- [x] Cancellable listener supervisor: backoff and recreation — bounded exponential backoff, `MeshNode.swift` §6.2 step 8 **[pkg]**; *idle-timeout recreation and foreground/background restart are covered by the separate device gate below*
- [x] LocalAPI WhoIs for accepted remote endpoints; production path fails closed when stable node ID is unavailable — `failClosedIdentityRejectsWhenWhoIsUnavailable`, `allowUnverifiedIsExplicitOptIn` **[pkg]**
- [x] RFC 6455 client/server adoption spike over libtailscale streams (`/ws`, bidirectional hello + data) — `RFC6455FrameTransport`, `upgradesOnWSAndExchangesAllFrameKinds` **[pkg]**
- [x] Auth URL presentation helper (Safari representable) — `AuthSafariView` **[pkg]**
- [x] Phase stream / observable status — `MeshModel` (@Observable), `eventsStreamEmitsPhaseImmediately` **[pkg]**
- [x] Unit tests for state machine (mock LocalAPI where possible) — 68 tests over `LoopbackBackend` **[pkg]**
- [ ] Migrate `project100/research/tailscale/ios-prototype` to consume the package — prototype lives outside this repository (§15); superseded in practice by Ghosttea consuming the published package

**Exit:** Demo app uses package only; Connect → login → Running → peer list →
HTTP get. A transport harness dials and accepts the same TCP connection, reads
and writes in both directions, resolves inbound WhoIs, cancels without leaked
tasks, and still accepts a connection after more than 60 seconds idle.

### Phase 1 — Mesh product MVP (`Truffle`)

- [x] `MeshConfiguration` / `MeshNode.start` — plus `MeshNode.startTailscale` for the production backend **[pkg]**
- [x] Device ULID persistence — `device-id.txt`, desktop-compatible **[pkg]**
- [x] `truffle-{appId}-{slug}` hostname advertisement (§7.2) — 10-step slug **[pkg]**
- [x] Peer registry from Tailscale status (honest fields) — provisional entries preserved across snapshots until netmap merge **[pkg]**
- [x] Hello v2 + `appId` filter — shipped schema, close codes 4001–4003 (§8.1–8.2) — `appMismatchCloses4001AndNeverSendsServerHello`, `malformedHelloCloses4002`, `identityContradictionCloses4003` **[pkg]**
- [x] `/ws` role-specific WebSocket handshake, bounds, heartbeat, and listener supervision (§8.1) — `rejectsMoreThan16ControlFramesBeforeHello`, `heartbeatTimeoutClosesDeadSession`, `helloReadTimesOut` **[pkg]**
- [x] `send` / `sendBytes` / `sendJSON` / `onMessage` over session port 9417, shipped envelope and base64-byte schemas (§6.4, §8.3) — `exchangesJSONBothDirectionsWithAttribution`, `exchangesOpaqueBytes` **[pkg]**
- [x] Semantic fixture + WebSocket transcript tests vs `truffle-core` (§8.4) — fixtures cross-decoded by both suites; `Interop fixtures (Rust side)` runs in CI **[pkg]**
- [x] `urlSession()` / `httpGet` — `MeshNode.swift:512,519` **[pkg]**
- [x] `waitUntilRunning` — `MeshNode.swift:254` **[pkg]**
- [ ] Example: chat between two simulators/devices — `Examples/MeshChatDemo` still runs on `LoopbackNetwork`; the real two-party exercise happens in Ghosttea, not in this repository's example
- [x] Doc: parity table TS → Swift — `apple/README.md` "Status vs RFC 024 phases" **[pkg]**

**Exit:** Two real iOS nodes with the same `appId` exchange JSON and byte
messages in both dial directions and preserve authenticated attribution. A node
with a different `appId` may remain a hostname-derived candidate but never
confirms or delivers application traffic.

### Phase 1b — Raw transport

- [x] `dial` / `listen` — `TailscaleKitBackend.swift:125,139`; `MeshNode.listen` refuses reserved port 9417, matching desktop **[pkg]**
- [ ] Integration example (netcat-style) — no such example ships in this repository

### Phase 2 — Interop verification with Rust/TS

(Wire contracts already implemented in Phase 1 — see §8.5.)

- [x] Validate fail-closed Swift WhoIs on real devices — exercised by the signed-device interop run; `confirmIdentity(of:)` provides an authenticated, generation-checked handshake without application traffic, added because lazy sessions mean peer listing alone may not expose a durable device ID **[dev]**. *The audit of desktop's missing-WhoIs fail-open compatibility case remains open.*
- [x] Manual or CI interop matrix: Node-client → iOS-server and iOS-client → Node-server; JSON + bytes; attribution; stale peer ref — signed iPhone joined the same `ghosttea-terminal` mesh as the desktop, completed the handshake, attached read-write, and exchanged traffic observed at both ends. An automated probe recorded `GHOSTTEA_SHARED_INTEROP_PASS handoff=a,b,a resize=… snapshot=1 selectionBytes=6 reconnect=1` and `GHOSTTEA_SHARED_RESTART_PASS peerGeneration=stable-1 hostInstance=changed stalePeer=not-stale staleSession=rejected` **[dev]**. *App-mismatch rejection is covered by unit tests but not yet by a device run.*
- [x] Fix gaps in peer query parity — desktop-parity peer query resolution **[pkg]**

The device runs above also found two defects that only physical hardware
surfaced, which is the argument for keeping this gate: a cross-runtime codec
mismatch (Swift emitted `requestID`/`sessionID`/`viewID`; Rust serde requires
`requestId`/`sessionId`/`viewId`), and the pinned libtailscale `EBADF` listener
defect that `apple/patches/libtailscale-remote-address-fd.patch` now corrects.
Neither was reachable from loopback tests.

### Phase 3 — Expand surface (separate RFCs may split these)

- [ ] File transfer (subset of RFC 014)  
- [ ] Synced store lite or skip  
- [ ] RFC 023-style serve if MagicDNS HTTPS available from userspace  
- [ ] macOS App Store / Developer ID packaging notes  

### Phase 4 — Hardening

- [ ] Binary size budget, dead-code stripping, dSYM/symbol policy  
- [ ] Privacy nutrition labels guidance — the reviewed `TailscaleKit-PrivacyInfo.xcprivacy` ships stamped into both framework slices, but no guidance document exists  
- [ ] Background execution, reconnection, and battery-budget hardening — background/foreground reconnect is observed **[dev]**, but no battery budget is defined, and note the hard constraint that a background `URLSession` cannot use embedded Tailscale (`TailscaleKitBackend.swift:202`)  
- [ ] Fuzz envelope decoder  
- [x] Public changelog + versioning aligned with truffle releases — the Swift package publishes from the repository root as identity `truffle`, sharing one version with every other artifact; each release is tagged `vX.Y.Z` alongside `truffle-vX.Y.Z` so SwiftPM can resolve it. Verified from 0.7.8: `.package(url:, from: "0.7.7")` resolves and builds **[pkg]**

---

### Remaining device gates

Everything below needs physical hardware and is not satisfied by any recorded
run. This is the honest residue of §12:

- **Long-idle accept.** Phase 0's exit requires accepting a connection after
  **more than 60 seconds idle**. No recorded run covers it, and it is the single
  most likely place for a latent listener or NAT-timeout defect to hide.
- **Cancellation probes.** Dial/accept cancellation without leaked tasks, on
  device rather than over loopback.
- **App-mismatch on device.** Unit-tested (`appIdMismatchNeverConfirms`,
  `appMismatchCloses4001AndNeverSendsServerHello`) but never exercised against a
  real tailnet.
- **Desktop missing-WhoIs fail-open audit.** Swift fails closed; desktop ships
  a fail-open compatibility case. The interaction has not been audited.
- **Re-validation at the current revision.** The **[dev]** evidence was recorded
  at `071264b0`. The Swift sources are unchanged since — 0.7.7 and 0.7.8 altered
  packaging and release automation only — but no run has been recorded against
  the currently published package, and a qualification claim pinned to a
  superseded revision is weak evidence even when it is true.

---

## 13. Testing strategy

| Level | What |
|---|---|
| Unit | Phase machine, generation-checked peer resolve/hash, appId validation, envelope/byte codec, event lag/resync |
| Semantic fixtures | Hello v2 + envelope JSON fixtures cross-decoded by Rust and Swift (§8.4) |
| WS transcripts | `/ws`, role ordering, opcodes, limits, close codes, heartbeat, attribution |
| Integration (sim) | Two processes with mock provider if possible; else dual-sim + real control |
| Integration (device v0.1) | Two real iOS nodes, both dial directions, fail-closed WhoIs, JSON + bytes, app mismatch |
| Integration (device v0.2) | Real desktop TS node ↔ iOS in both client/server roles (Phase 2) |
| Soak | Repeated bus and listener timeouts; background/foreground; assert recovery without user action |
| API freeze | Snapshot of public Swift interface in CI |

LocalAPI is hard to mock; introduce `NetworkBackend` protocol early:

```swift
protocol NetworkBackend: Sendable {
    func start() async throws
    func stop() async throws
    func status() async throws -> BackendStatus
    var events: AsyncStream<BackendEvent> { get }
    func dial(host: String, port: UInt16) async throws -> any MeshConnection
    func listen(port: UInt16) async throws -> any MeshListener
    func whoIs(remoteEndpoint: String) async throws -> AuthenticatedPeer
    func makeURLSession() async throws -> URLSession
}
```

`AuthenticatedPeer` contains at minimum the stable Tailscale node ID and the
normalized remote addresses used for the comparison. `TailscaleKitBackend` is
production; `LoopbackBackend` for tests must make unverified identity an
explicit opt-in rather than silently returning a claimed ID.

---

## 14. Security & privacy

1. State directory: Application Support, `NSURLIsExcludedFromBackupKey`, complete-until-auth data protection as appropriate.  
2. No long-lived auth keys in binary; prefer interactive or server-minted single-use keys.  
3. `appId` is not a security boundary against a malicious peer on the same tailnet who speaks the protocol — **Tailscale ACLs + tailnet membership** remain the trust boundary; `appId` is isolation/UX.  
4. Production inbound sessions fail closed when WhoIs does not produce a stable node ID; a claim/authenticated-ID mismatch closes with 4003 before the local hello is revealed.  
5. Enforce the handshake concurrency cap, field bounds, 16 MiB WebSocket limit, 15 MiB envelope limit, and 11 MiB decoded-byte limit before application delivery. Reject malformed base64.  
6. Treat all envelope fields, including `from`, as attacker-controlled. Attribution comes only from the authenticated connection.  
7. Log redaction: never log auth keys, LocalAPI/SOCKS credentials, complete auth URLs, message payloads, or private state paths at default levels.  
8. Pin and checksum the native artifact; publish its provenance, upstream license/notices, patch set, and supported upgrade window.  

---

## 15. Documentation deliverables

1. This RFC.  
2. `docs/guide/swift.md` — install, first app, auth, byte-vs-JSON messages, and troubleshooting (bus/listener timeout, WhoIs failure).  
3. Parity table: `createMeshNode` / Peer / send / net / http → Swift.  
4. Architecture diagram: desktop vs Apple runtime.  
5. Research notes link: `project100/research/tailscale/ios-embed-without-installing-tailscale.md` (outside this repo — copy or mirror into `docs/` before publishing the docs site, or the link will break).  

---

## 16. Open questions

| # | Question | Default until decided |
|---|---|---|
| Q1 | ~~Exact Truffle session TCP port shared with Rust~~ | **Resolved: 9417** — `truffle-core` `ws_port` default (`node.rs`); single source of truth is the Rust crate |
| Q2 | Monorepo `apple/` vs `truffle-swift` repo | Prefer monorepo `apple/` |
| Q3 | `start()` blocks until Running vs returns early | Return early + `waitUntilRunning` |
| Q4 | ~~JSON-only envelopes vs RFC 009 binary frames on Swift~~ | **Resolved: JSON only** — the session never shipped RFC 009 binary frames; JSON is the sole interop-compatible encoding (§8.3) |
| Q5 | macOS: embed-only vs optional system Tailscale | Embed-only v0.1 |
| Q6 | Minimum iOS version if TailscaleKit lowers deployment target | Track framework |
| Q7 | ~~Should `deviceName` set Tailscale hostname directly?~~ | **Resolved: No** — hostname must be `truffle-{appId}-{slug(deviceName)}` (RFC 017 §4); the desktop candidate filter depends on the prefix (§7.2) |

---

## 17. Success criteria

**RFC accepted when** team agrees on: Swift-native runtime; the pinned
TailscaleKit artifact and patch policy; the full-duplex/listen/WhoIs prerequisite;
generation-checked peer snapshots; explicit byte/JSON semantics; the fail-closed
Swift inbound identity policy; and the complete §8 interop contract.

**v0.1 shipped when:**

1. Reproducible, checksummed SPM package builds an iOS app without Tailscale App Store.  
2. Interactive login completes with bus recovery (no stuck `needsLogin` after successful IdP).  
3. Port 9417 remains available after idle accept timeouts and a foreground/background cycle.  
4. Two devices, same `appId`, exchange JSON and opaque-byte messages through generation-checked `Peer` snapshots; different `appId` never confirms or delivers application traffic.  
5. Inbound WhoIs is fail-closed and claimed-ID mismatches close with 4003.  
6. `urlSession` can GET a known tailnet HTTP service.  
7. Semantic fixtures, WebSocket transcript tests, public docs, and example app pass.  

**v0.2 interop when:** Node `@vibecook/truffle` and Truffle Swift exchange JSON
and byte messages in both client/server directions on a shared tailnet and
`appId`, attribute each sender to the expected peer, reject an `appId` mismatch,
and pass the §8 transcript/fixture suite from both implementations.

---

## 18. Appendix A — TS → Swift parity (MVP)

| TS (`@vibecook/truffle`) | Swift (`Truffle`) | Phase |
|---|---|---|
| `createMeshNode({ appId, deviceName, onAuthRequired })` | `MeshNode.start(MeshConfiguration…)` | 1 |
| `mesh.getPeers()` | `mesh.peers()` | 1 |
| `mesh.peer(q, { waitMs })` | `mesh.peer(_:waitMs:)` | 1 |
| `mesh.send(peer, ns, buf)` (legacy content sniffing) | `mesh.send(to:namespace:data:)` (always explicit `bytes`; §6.4) | 1 |
| `mesh.native.sendJson/sendBytes` | `mesh.sendJSON/sendBytes` | 1 |
| `mesh.onMessage(ns, cb)` | `mesh.onMessage(namespace:handler:)` | 1 |
| `mesh.onPeerChange` | `mesh.events` / peer cases | 1 |
| `mesh.dnsName` | `mesh.dnsName` | 1 |
| fetch via custom agent | `mesh.urlSession()` / `httpGet` | 1 |
| `mesh.net.connect/listen` | `dial` / `listen` | 1b |
| `mesh.http` / Express | Not v0.1 (URLSession client yes) | 3 |
| `mesh.ws` / `mesh.quic` | Later | 3 |
| `mesh.serve` | Later (RFC 023) | 3 |
| `fileTransfer()` | Later | 3 |
| `useMesh` React | `MeshModel` SwiftUI | 1 |

## 19. Appendix B — Prototype → library map

| Prototype (`ios-prototype`) | Library type |
|---|---|
| `TailscaleSession` | `TailscaleRuntime` + `MeshNode` |
| `BusConsumer` + restart logic | Internal bus supervisor |
| `SafariView` | `TruffleSwiftUI.AuthSafariView` |
| `backendStatus` polling | Normative in §6.2 / §9 |
| Phone Home | `urlSession` / `httpGet` |
| Peer rows from netmap | Seed for `Peer` registry (pre-hello) |

## 20. Appendix C — Suggested first PR sequence

1. Pin libtailscale; add reproducible XCFramework build, checksum, provenance, licenses, and patch scaffolding  
2. Full-duplex connection/listener + WhoIs adapter; prove idle recovery and RFC 6455 stream adoption  
3. Move runtime out of research demo  
4. `MeshNode` façade with phase/events only (no messaging yet)  
5. Generation-checked registry + hello + messaging codecs/transcripts  
6. Example chat (JSON + bytes)  
7. Docs + changelog  

---

## 21. Summary

Truffle Swift is the **Apple-native implementation of the Truffle product**, built on a **pinned, checksummed in-process libtailscale/TailscaleKit**, not a port of the desktop sidecar. It prioritizes the same developer experience as `createMeshNode`—**generation-checked Peers, appId, explicit JSON/byte messages, URLSession**—with auth, WhoIs verification, listener supervision, and IPN recovery treated as library responsibilities. Interop with Rust/TS uses the fully specified shipped wire contract (hello v2, JSON envelope, `/ws` on 9417, role ordering, bounds, heartbeat, and `truffle-{appId}-` hostnames), adopted in Phase 1 and verified live in Phase 2 — not a requirement to share one binary core on day one.

**Decision requested:** Accept this RFC (Swift-native runtime, phased plan, API sketch) and proceed with Phase 0 package extraction.
