# RFC 017: Identity and App Namespacing

**Status:** Implemented
**Author:** James + Claude
**Date:** 2026-04-11

---

## 1. Problem Statement

Truffle currently has one identity concept — the Tailscale stable node ID — and one implicit filter — `hostname.starts_with("truffle-")`. Both break down in practice:

### 1.1 The immediate bug: devices disappear from their own peer list

`crates/truffle-core/src/network/tailscale/provider.rs:361`:

```rust
pub(crate) fn is_truffle_peer(hostname: &str) -> bool {
    hostname.starts_with("truffle-")
}
```

This runs on every `PeersReceived` / `PeerChanged` event (lines 259, 290, 306). Peers whose hostname doesn't start with `truffle-` are silently dropped before reaching application code.

The Electron playground passes `name: 'playground'` to `createMeshNode`, which flows verbatim to the Tailscale hostname. The playground therefore registers itself as `playground`, not `truffle-playground`. Every other truffle node on the same tailnet runs `is_truffle_peer("playground")` → `false`, filters it out, and the playground vanishes from its own peer list.

The CLI playground worked because it used hostnames like `truffle-cli-{uuid}`. The new ergonomic `createMeshNode` API dropped that implicit convention and pushed responsibility onto the caller without documenting it.

### 1.2 No application-level isolation

Even if we patch the prefix, `is_truffle_peer` is a *library-wide* filter. It admits every truffle node on the user's tailnet — regardless of which application it belongs to. If a user runs the playground and a chat app and a file-sync tool on the same tailnet, all three show up in each other's peer lists. The WebSocket handshake may reject cross-app messages once a connection is attempted, but the list is polluted and any app that trusts truffle for "are we the same app" gets burned.

### 1.3 Tailscale ID is not a stable app identity

`NapiNodeIdentity.id` is the Tailscale machine stable ID. This is stable per-Tailscale-registration but NOT per-logical-device:

- Running with `ephemeral: true` rotates the ID on every shutdown.
- Wiping the Tailscale state dir rotates the ID on next start.
- Moving to a new machine gives a different ID.
- The same user on two different Tailscale accounts gets two IDs for what is morally the same device.

Applications that want "this is the same logical device as last week" currently have nothing to hang on to.

### 1.4 State directory defaults are not durable

`crates/truffle-core/src/node.rs` builder:

```rust
let state_dir = self
    .state_dir
    .unwrap_or_else(|| format!("/tmp/truffle-{hostname}"));
```

- **macOS**: `/tmp/*` is cleaned after ~3 days of no access.
- **Linux**: most distros mount `/tmp` as tmpfs; it disappears on every reboot.
- **Windows**: `/tmp` isn't a thing at all — this path becomes `C:\tmp\...` which works by accident but is not where per-user app data belongs.
- **Multi-user**: `/tmp/truffle-playground` is shared across every user on the machine, so two users clobber each other's machine keys.

When the state dir is wiped, the machine key goes with it, and the next launch looks like a brand-new device to Tailscale — forcing re-authentication. Users hit this on every reboot.

### 1.5 Device names are unpredictable user input

Callers currently pass a raw `name` string that is interpreted as a DNS hostname. Real device names that users reach for include:

- `Jamess MacBook Pro (6)` (spaces and parentheses)
- `Alice's iMac` (apostrophe)
- `server@office` (at sign)
- `田中's 部屋` (Unicode)
- `test_device_01` (underscore — invalid in DNS labels)
- `very-long-descriptive-device-name-that-definitely-exceeds-the-dns-label-length-budget` (>63 chars)

Any of these break Tailscale silently. The user doesn't find out until peer discovery fails to match their node. Right now truffle performs no sanitization — it hands the raw string to the sidecar and hopes.

---

## 2. Goals

1. **Immediate visibility fix.** Under the new design, a freshly created node reliably shows up in peer lists of other nodes in the same app.
2. **Cleanly scoped peer discovery.** Two apps using truffle on the same tailnet do not see each other as peers, even when they share the user's Tailscale account.
3. **Stable per-device identity that survives Tailscale re-auth.** Apps have a `deviceId` that persists across restarts, state wipes (when recoverable), and ephemeral-mode rotations.
4. **Forgiving input for device names.** Users pass whatever string they want (Unicode, spaces, emoji) and truffle derives a valid DNS hostname from it without surprising the caller.
5. **Durable state directory by default.** First-run auth, last-run auth — same node. Zero re-auth in the steady state.
6. **Cross-platform correctness.** Works the same way on macOS, Linux, Windows without special-casing in application code.
7. **Single, obvious API.** Users don't have to know about Tailscale internals, hostname DNS rules, or stateDir conventions to get correct behaviour.

---

## 3. Non-Goals

- **Federated / cryptographic identity.** `deviceId` is an opaque ULID generated by truffle. If applications want to tie it to their own user system, they provide `deviceId` explicitly at construction. Truffle does not validate or sign anything.
- **Tailscale ACL tags.** Tempting for namespacing but requires the user to configure their Tailscale admin UI — a step our users have not taken and should not have to take for the playground to work.
- **Multiple apps per process.** A single process runs one truffle node for one `appId`. A future RFC can revisit if a concrete use case appears; the API below does not foreclose the option.
- **Reconciliation of multiple devices masquerading as the same `deviceId`.** If the user copies a state dir to two machines, the `deviceId` collides, both register separately with Tailscale, and both appear as peers — by Tailscale ID, not by `deviceId`. Applications that care can detect and warn; truffle does not.
- **Back-compat with `name` on a deprecation timer.** Truffle is young; the only consumers are examples in this monorepo. Clean break in 0.4.0.

---

## 4. Design: Two Namespaces

```
┌──────────────────────────────────────────────────────────────────┐
│  Tailnet (owned by the user's Tailscale account)                 │
│                                                                  │
│  ┌────────────────────────────────────────────────────────┐      │
│  │  Namespace 1: Truffle-managed nodes                    │      │
│  │  Admission rule: tailscaleHostname matches             │      │
│  │     `^truffle-[a-z0-9][a-z0-9-]{0,31}-[a-z0-9-]+$`     │      │
│  │                                                        │      │
│  │  ┌──────────────────┐   ┌──────────────────┐           │      │
│  │  │ Namespace 2a     │   │ Namespace 2b     │           │      │
│  │  │ appId: playground│   │ appId: chat      │           │      │
│  │  │                  │   │                  │           │      │
│  │  │ deviceId: 01J4…A │   │ deviceId: 01J4…X │           │      │
│  │  │ deviceId: 01J4…B │   │ deviceId: 01J4…Y │           │      │
│  │  └──────────────────┘   └──────────────────┘           │      │
│  │      ↑                     ↑                           │      │
│  │      └── mutually invisible to each other ──┘          │      │
│  └────────────────────────────────────────────────────────┘      │
│                                                                  │
│  ┌────────────────────────────────────────────────────────┐      │
│  │  Other tailnet nodes (user's laptop, phone, NAS, …)    │      │
│  │  Filtered out — hostname doesn't match truffle-*       │      │
│  └────────────────────────────────────────────────────────┘      │
└──────────────────────────────────────────────────────────────────┘
```

**Namespace 1 — the `truffle-*` prefix** — is owned by the library. Every Tailscale hostname registered by a truffle node begins with `truffle-`. This lets truffle distinguish its own nodes from everything else on the user's tailnet. Users never choose this prefix; the library always prepends it.

**Namespace 2 — the `appId`** — is owned by the application. Each app declares an `appId` at construction time. Two apps with different `appId`s are invisible to each other at the peer discovery layer — they have different hostname prefixes and truffle's filter rejects the wrong ones before any application code sees them.

Tailscale hostnames become a deterministic projection of both namespaces plus the device name:

```
truffle-{appId}-{slug(deviceName)}
```

Filtering uses the hostname prefix `truffle-{appId}-`. Self-filtering uses the Tailscale stable ID (not hostname, which is susceptible to collisions). Peer identity exposed to application code uses `deviceId`, which is stable across restarts.

---

## 5. Identifier Schemas

### 5.1 `appId`

Format: `^[a-z][a-z0-9-]{1,31}$`

- Must start with a lowercase letter.
- Allowed: lowercase ASCII letters, digits, hyphen.
- Length: 2–32 characters.
- Case is normalised to lowercase on ingest; the user-provided case is discarded.

Validation happens inside `createMeshNode` / `NodeBuilder::app_id`. An invalid `appId` is a hard error, not a warning — the caller gets a thrown exception / `Result::Err` before any Tailscale work begins. The error message includes the exact regex and shows which character caused the rejection.

Rationale: 32 characters leaves enough room in the Tailscale hostname budget (see §5.3) for the device name to carry any reasonable value, while still being long enough for descriptive app identifiers like `production-playground-v2`.

### 5.2 `deviceName` (user-supplied)

**Accepted as-is.** Users pass whatever they want — emoji, Unicode, spaces, punctuation, arbitrary length. Truffle stores the original string verbatim for display and transmits it verbatim to peers. The only constraint is a soft cap of **256 characters**; longer strings are truncated to 256 with a warning.

The only reason truffle cares about `deviceName` at all is that a *sanitised* version is needed to form the Tailscale hostname. That sanitisation is a one-way derivation — the original stays intact for display.

### 5.3 `slug(deviceName)` — the Tailscale-hostname-safe derivation

DNS labels are limited to 63 characters, [a-z0-9-], no leading/trailing hyphen, case-insensitive. The full Tailscale hostname `truffle-{appId}-{slug}` has budget:

```
63                     DNS label limit
-  8                   'truffle-'
-  len(appId) + 1      '{appId}-'
=  54 - len(appId)     available for slug
```

With `appId` capped at 32, the slug budget is at least **22 characters**. With `appId = 'playground'` (10 chars), it's 44.

**Sanitisation algorithm** (deterministic, reversible-enough-for-humans-to-recognise):

```
fn slug(raw: &str, budget: usize) -> String {
    1. NFKC-normalise the input (collapses lookalike Unicode codepoints).
    2. Transliterate Unicode to ASCII where a reasonable mapping exists:
       - diacritics stripped ('é' → 'e', 'ñ' → 'n', 'ü' → 'u')
       - ligatures split ('æ' → 'ae')
       - scripts without a Latin mapping (CJK, Arabic, emoji) are
         replaced with a single hyphen per contiguous run.
    3. Lowercase.
    4. Replace every character not in [a-z0-9] with '-'.
    5. Collapse runs of multiple '-' into a single '-'.
    6. Trim leading and trailing '-'.
    7. If the result is empty (e.g., pure emoji input), replace with
       the first 8 chars of base36(blake3(raw)) — a stable hash fallback
       so two all-emoji device names at least stay distinct per-device.
    8. Truncate to `budget` chars, preserving any deterministic suffix
       if one was added.
    9. Trim trailing '-' again post-truncation.
   10. If the result would be <2 chars, append the first 6 chars of
       base36(blake3(raw)) so every device gets a usable label.
}
```

Examples with `appId = 'playground'` (slug budget 44):

| Input | Slug | Full hostname |
|---|---|---|
| `Alice's MacBook Pro` | `alice-s-macbook-pro` | `truffle-playground-alice-s-macbook-pro` |
| `田中's 部屋` | `s-d2a1f3k8` | `truffle-playground-s-d2a1f3k8` |
| `🚀` | `rocket` | `truffle-playground-rocket` |
| `〓` (U+3013, GETA mark — no Latin transliteration) | deterministic 8-char hash | `truffle-playground-{hash}` |
| `JAMES-mbp-16"` | `james-mbp-16` | `truffle-playground-james-mbp-16` |
| `this-is-a-very-long-device-name-that-blows-past-the-dns-label-budget-and-then-some` | `this-is-a-very-long-device-name-that-blows-p` | `truffle-playground-this-is-a-very-long-device-name-that-blows-p` |
| `` (empty) | rejected at input — defaults to OS hostname instead | — |

**Collision handling.** Two distinct `deviceName`s can sanitise to the same slug (`"Alice's Mac"` and `"Alice Mac"` both → `alice-mac`). Tailscale's control plane resolves hostname collisions by appending `-2`, `-3`, … at the Tailscale layer — we rely on that behaviour and do NOT pre-emptively add our own suffix. The app-level `deviceId` (§5.4) is what guarantees unique identity; the hostname is only for Tailscale's own use.

### 5.4 `deviceId` (stable per-device UUID)

Format: ULID (Crockford base32, 26 characters), e.g., `01J4K9M2Z8AB3RNYQPW6H5TC0X`.

- Generated on first start via `ulid::Generator`, persisted to `{stateDir}/device-id.txt` as a single-line ASCII file.
- On subsequent starts the file is read verbatim. If the file exists but contains an invalid ULID, a clear error is surfaced and truffle refuses to start (better than silently regenerating and losing identity).
- Applications may **override** by passing `deviceId` to `createMeshNode`. When provided, the value is validated (must be a well-formed ULID) and persisted to the file so future auto-generated calls use it too.

**Why ULID and not UUIDv4 or UUIDv7?**
- ULIDs are lexicographically sortable by creation time — useful for debugging ("which device was provisioned first?").
- 26 ASCII characters is shorter than the 36-character UUID dash format and fits cleanly in JSON keys, log lines, and URL segments.
- No hyphens means no parsing ambiguity inside hostname-like contexts where we already use `-` as a separator.

**Why persisted in a separate file, not inside a `truffle.json` config?** Keeping it in its own file means a directory-level copy/move operation preserves it without dragging along the rest of the Tailscale state, and text files are trivial to inspect and edit during debugging.

---

## 6. State Directory Resolution

### 6.1 The default

```
{userDataDir}/truffle/{appId}/{slug(deviceName)}/
```

Where `{userDataDir}` is determined per-platform:

| Platform | Path (via `dirs::data_dir()` in Rust, `app.getPath('userData')` in Electron) |
|---|---|
| macOS   | `~/Library/Application Support/` |
| Linux   | `~/.local/share/` |
| Windows | `%APPDATA%\` |

Concrete example with `appId='playground'` and `deviceName='alice-mbp'` on macOS:

```
~/Library/Application Support/truffle/playground/alice-mbp/
├── device-id.txt         (our own, §5.4)
├── tailscaled.state      (written by tsnet)
├── tailscaled.log        (written by tsnet)
└── derpmap.cached.json   (written by tsnet)
```

**Why scope by `slug(deviceName)` under `appId`?** So that a single user can run a second instance with a different device name without colliding. The monorepo developer running two instances on one laptop for multi-peer testing sets `VITE_TRUFFLE_NAME=bob` for the second instance and its state lives in a sibling directory.

### 6.2 `/tmp` is retired

The `/tmp/truffle-{hostname}` default is removed. `/tmp` remains a last-resort fallback only if `dirs::data_dir()` returns `None` (which happens inside minimal containers with no `HOME`). In that case truffle logs a prominent warning recommending an explicit `stateDir`.

### 6.3 Electron's convenience

In the Electron playground, the main process passes `stateDir` explicitly using `app.getPath('userData')`:

```ts
const stateDir = join(
  app.getPath('userData'),   // Truffle Playground's per-OS user data dir
  'truffle-state',
  appId,
  slug(deviceName),
);
```

This is identical in meaning to the core default but uses Electron's built-in resolution, which is what Electron-based applications already use for their OWN state. Keeping truffle's state adjacent to application state simplifies mental model ("delete this one directory to reset everything").

---

## 7. API Changes

### 7.1 `CreateMeshNodeOptions` (TypeScript, breaking)

```ts
export interface CreateMeshNodeOptions {
  /**
   * Required. Identifies your application within the tailnet. Two apps
   * with different `appId`s cannot see each other as peers.
   *
   * Format: `^[a-z][a-z0-9-]{1,31}$` (lowercase letters, digits, hyphens,
   * 2-32 characters, must start with a letter).
   */
  appId: string;

  /**
   * Optional human-readable device name. Defaults to the OS hostname.
   * May contain any Unicode characters; truffle derives a Tailscale-safe
   * hostname from this automatically.
   */
  deviceName?: string;

  /**
   * Optional stable per-device ULID. If omitted, one is generated on
   * first run and persisted inside `stateDir`. Provide your own if you
   * want to federate identity with an existing user system.
   */
  deviceId?: string;

  /**
   * Optional state directory override. Defaults to
   * `{userDataDir}/truffle/{appId}/{slug(deviceName)}/`.
   */
  stateDir?: string;

  /** Remaining options unchanged from current createMeshNode. */
  ephemeral?: boolean;
  wsPort?: number;
  authKey?: string;
  autoAuth?: boolean;
  openUrl?: (url: string) => void;
  sidecarPath?: string;
  onAuthRequired?: (url: string) => void;
  onPeerChange?: (event: NapiPeerEvent) => void;
}
```

The `name` field is **removed**, not deprecated. Callers must migrate to `appId` + `deviceName`.

### 7.2 `NapiNodeIdentity` (breaking)

```ts
export interface NapiNodeIdentity {
  /** The application identifier. */
  appId: string;
  /** Stable per-device ULID. Primary key for device identity. */
  deviceId: string;
  /** Original (unsanitised) device name, as passed by the application. */
  deviceName: string;
  /** The Tailscale hostname (`truffle-{appId}-{slug}`). Debug use only. */
  tailscaleHostname: string;
  /** The Tailscale stable node ID. Escape hatch; most code should not need this. */
  tailscaleId: string;
  /** Tailscale DNS name, if available. */
  dnsName?: string;
  /** Tailscale IP address as a string. */
  ip?: string;
}
```

Rename of `id` → `deviceId`, addition of `appId`, `deviceName`, `tailscaleHostname`, `tailscaleId`. The old `name` and `hostname` fields are gone.

### 7.3 `NapiPeer` (breaking)

```ts
export interface NapiPeer {
  /** Stable per-device ULID from the remote node. Primary key. */
  deviceId: string;
  /** Human-readable device name from the remote node. */
  deviceName: string;
  /** Tailscale IP address as a string. */
  ip: string;
  /** True if the Tailscale layer reports this peer as online. */
  online: boolean;
  /** True if an active WebSocket connection exists. */
  wsConnected: boolean;
  connectionType: string;
  os?: string;
  lastSeen?: string;
  /** Tailscale stable ID. Escape hatch; most code should use deviceId. */
  tailscaleId: string;
}
```

The previous `id` / `name` fields are renamed to `deviceId` / `deviceName`. The application never needs to know the Tailscale stable ID in normal code paths.

### 7.4 Message routing signatures

`node.send(deviceId, namespace, data)`, `node.resolvePeerId(query)`, `ChatMessage.from`, etc. all take/return `deviceId` rather than the Tailscale ID. The only place a Tailscale ID surfaces is via `Peer.tailscaleId` and `NodeIdentity.tailscaleId` for debugging.

### 7.5 Rust `NodeBuilder`

```rust
impl NodeBuilder {
    /// Required. Validated against the appId regex (§5.1).
    pub fn app_id(mut self, app_id: impl Into<String>) -> Result<Self, NodeError>;

    /// Optional. Defaults to the OS hostname.
    pub fn device_name(mut self, device_name: impl Into<String>) -> Self;

    /// Optional override. Defaults to auto-generated ULID persisted
    /// in `{state_dir}/device-id.txt`.
    pub fn device_id(mut self, device_id: impl Into<String>) -> Result<Self, NodeError>;

    // state_dir, ephemeral, ws_port, auth_key, sidecar_path unchanged.
    // build() / build_with_auth_handler() unchanged.
}
```

`NodeBuilder::name` is **removed**.

---

## 8. Wire Protocol: Peer Metadata Exchange

`deviceId` and `deviceName` must propagate between peers so a node can display `Alice's MacBook` rather than `01J4K9M2Z...`. The Tailscale layer only knows `tailscaleHostname` (which is the sanitised slug, not the original), so we need a separate metadata channel.

**Mechanism: WebSocket hello extension.**

When two truffle nodes open a WebSocket link, the first envelope exchanged in each direction is already a connection hello. Extend the hello payload:

```json
{
  "kind": "hello",
  "version": 2,
  "identity": {
    "appId": "playground",
    "deviceId": "01J4K9M2Z8AB3RNYQPW6H5TC0X",
    "deviceName": "Alice's MacBook Pro",
    "os": "darwin"
  }
}
```

The receiving session layer extracts the identity block and stores it on `PeerState.identity`. This is the source of truth for peer display metadata. `ChatMessage.fromName` in the current design becomes redundant — the session layer can resolve a peer's display name from the local registry using the sender's `deviceId`.

**AppId mismatch on hello.** If the remote peer's hello advertises a different `appId`, close the WebSocket immediately with a specific close code (e.g., 4001 "app mismatch") and record the rejection in telemetry. This is a belt-and-braces check on top of hostname-prefix filtering — in case two apps end up sharing a hostname prefix due to a future Tailscale naming change, the hello handshake catches it.

> **Amended 2026-09-16 (RFC 025).** The close-code table as shipped: **4001** app
> mismatch · **4002** hello protocol (malformed, missing, timed out) · **4003** the
> claimed `tailscale_id` contradicts the WhoIs-authenticated identity — or, on a node
> that admits only listed logins, the connection carried no authenticated identity ·
> **4004** the caller's authenticated login is not on the node's allow-list. 4003 and
> 4004 are sent BEFORE the accepting side reveals its own hello. The dialing side
> surfaces any 4xxx close received before the hello as
> `TransportError::HelloRefused { code, reason }`. The login is never carried in the
> hello; WhoIs is the authority (RFC 025 §3.4, D8).

**Version negotiation.** The `version` field starts at 2 (version 1 being the implicit pre-RFC-017 hello). Newer truffle versions can add fields below `identity` without breaking older peers, as long as the `version` field is monotonically increased and the old required fields stay in place.

---

## 9. Self-Filtering

The current code does not explicitly exclude the local node from its own peer list. Peer filtering happens inside the session and session.rs does NOT test `peer.id == self.local.id`. The accidental self-exclusion comes from the Go sidecar's `WatchIPNBus` not returning the local node in its `peers` map.

**This needs an explicit rule** — relying on sidecar behaviour is fragile, especially since hostname collisions (same-name dev runs crashing and restarting) can cause the local node to appear as its own peer under a different Tailscale ID.

New rule:

```rust
fn is_own_node(&self, peer: &SidecarPeer) -> bool {
    peer.id == self.local_tailscale_id
}
```

Applied in the filter chain BEFORE the hostname prefix check. The local node's `tailscaleId` is captured during `provider.start()` from the netmap's `self` entry and never changes for the lifetime of the process.

---

## 10. Migration: 0.3.x → 0.4.0

Breaking changes are confined to:

- `createMeshNode({ name })` → `createMeshNode({ appId, deviceName? })`
- `NapiNode.getLocalInfo()` return shape (`id` / `name` → `deviceId` / `deviceName` / `appId` / `tailscaleId`)
- `NapiPeer` shape (`id` / `name` → `deviceId` / `deviceName` / `tailscaleId`)
- `node.send()` / `node.resolvePeerId()` / `node.ping()` accept `deviceId` (formerly Tailscale ID)
- `ChatMessage.from` / `fromName` — `from` is now `deviceId`
- Default state dir location (from `/tmp` to OS user data directory)

Consumers in this monorepo and their migration:

1. **`truffle-cli`** — heaviest user of the current API. CLI commands reference `Peer.id` and `Peer.name` in dozens of places; all need a sed-level rename to `deviceId` / `deviceName`. The `truffle up --name foo` flag becomes `truffle up --device-name foo`, with a new `--app-id` flag required (or baked into the CLI as a constant `cli` app ID).
2. **`examples/playground`** — small; `TruffleContext.makeDefaultConfig` changes from `{ name: envName ?? 'playground' }` to `{ appId: 'playground', deviceName: envName }`. Three-line edit.
3. **`examples/chat`, `examples/discovery`, `examples/shared-state`** — all already broken against the current API (they use the pre-RFC-015 API and haven't been updated). RFC 017 migration rolls into the rewrite already planned for those.
4. **`@vibecook/truffle-react` hooks** — need to update peer types and any code that reads `peer.id`. Minor.
5. **External consumers** — there are none yet.

Because the only non-trivial consumer is the monorepo's own CLI, the migration is a single PR and the release can go out as 0.4.0 without a transitional 0.3.x branch.

---

## 11. Implementation Plan

Phases are sequential. Within a phase, bullets are parallelisable.

### Phase 1 — Core identity model

- [ ] `crates/truffle-core/src/identity.rs` — new module with:
  - `AppId` newtype wrapping `String` with `FromStr` that validates the regex
  - `DeviceName` newtype wrapping `String` (accepts anything up to 256 chars)
  - `slug()` function implementing §5.3 exactly, with unit tests covering every row of the examples table
  - `DeviceId` newtype wrapping `String` with ULID validation
  - `tailscale_hostname(app_id, device_name)` helper returning the final hostname
- [ ] Add `ulid` and `dirs` crates to `truffle-core` Cargo.toml
- [ ] `crates/truffle-core/src/node.rs` — extend `NodeBuilder`:
  - Add `app_id`, `device_name`, `device_id` fields
  - Remove `name`
  - `build()` fails with a clear error if `app_id` is missing
  - Default `state_dir` uses `dirs::data_dir().join("truffle").join(app_id).join(slug)` with `/tmp` fallback only for no-home environments
  - `device_id` is loaded from `{state_dir}/device-id.txt` if present, else generated + written
- [ ] `crates/truffle-core/src/network/tailscale/config.rs` — `TailscaleConfig` takes `app_id` + `device_name`, constructs `hostname` internally
- [ ] `crates/truffle-core/src/network/tailscale/provider.rs`:
  - `is_truffle_peer(hostname)` → `is_app_peer(hostname, app_id)`
  - Self-filter by `tailscaleId` before the app filter
  - `local_identity()` returns the new `NodeIdentity` shape

### Phase 2 — Wire protocol extension

- [ ] `crates/truffle-core/src/session/hello.rs` — new hello envelope format (§8)
- [ ] `PeerState` gains `identity: Option<PeerIdentity>` populated from the incoming hello
- [ ] Reject connections whose hello has a mismatched `appId` with close code 4001
- [ ] Update `send_typed` / `broadcast_typed` to stamp the local `deviceId` on outgoing messages
- [ ] Update `ChatMessage.from` in consumers to mean `deviceId`

### Phase 3 — NAPI and TypeScript bindings

- [ ] `crates/truffle-napi/src/types.rs` — new `NapiNodeIdentity`, `NapiPeer`, `NapiNodeConfig`
- [ ] `crates/truffle-napi/src/node.rs` — `start()` takes the new config; all peer-accepting methods are still `peer_id` parameters but now that means `deviceId`
- [ ] `packages/core/src/create-mesh-node.ts` — accept `appId` / `deviceName` / `deviceId`, drop `name`, call into `NapiNode` with the new shape
- [ ] Regenerate `crates/truffle-napi/index.d.ts` via pre-commit hook

### Phase 4 — CLI migration

- [ ] `truffle-cli` commands: every `Peer.id` → `Peer.device_id`, every `Peer.name` → `Peer.device_name`
- [ ] CLI flag rename: `--name` → `--device-name`, new `--app-id` flag (default `cli`)
- [ ] TUI display uses `device_name` verbatim (Unicode OK) and `short(device_id)` for ID columns
- [ ] Update all fixture device IDs in CLI tests from Tailscale-style to ULID

### Phase 5 — Examples and release

- [ ] `examples/playground` — update `TruffleContext.makeDefaultConfig`, `TruffleManager.start` (Electron `app.getPath('userData')` for stateDir)
- [ ] `examples/chat`, `examples/discovery`, `examples/shared-state` — full rewrite to the new API (they're already broken)
- [ ] Update RFC statuses and CHANGELOG
- [ ] Release 0.4.0

---

## 12. Alternatives Considered

### 12.1 Keep `name`, auto-prefix in `createMeshNode`

"Just make `createMeshNode({ name: 'playground' })` construct `truffle-playground` under the hood."

**Rejected.** Fixes the immediate visibility bug but solves none of the structural problems: no app scoping, no stable device identity, no durable state dir, no input sanitisation. It also silently rewrites user input, which is a worse UX than a clear `appId` requirement.

### 12.2 Tailscale ACL tags for scoping

"Use `tag:truffle-playground` on the tailnet instead of hostname prefixes."

**Rejected.** Tags require ACL configuration in the Tailscale admin UI, which is a manual prerequisite for every user before truffle can work. Truffle's selling point is that a developer runs `pnpm dev` and things work. Tags also don't solve state dir or device ID.

### 12.3 UUIDv7 instead of ULID

"UUIDv7 is the newer timestamp-based UUID standard."

**Rejected for now.** UUIDv7 is still settling as a standard, and the `uuid` crate's v7 support is newer than ULID support. ULID has been stable for years, is lexicographically sortable, and is 26 chars of crockford base32 vs UUID's 36-char dash format. Both work; ULID wins on consistency.

### 12.4 Put `deviceId` in the Tailscale hostname itself

`truffle-playground-01j4k9m2z8ab3rnyqpw6h5tc0x`

**Rejected.** 26 chars of ULID plus `truffle-playground-` (20 chars) is 46 chars — well under the 63-char limit but leaves no room for human-readable device names. More importantly, the Tailscale admin UI becomes unreadable (a wall of ULIDs). Keep the hostname human-readable and carry the `deviceId` separately via the WebSocket hello.

### 12.5 Single-level namespace (just `appId`, no `truffle-` prefix)

"Why do we need `truffle-` at all? Just use `playground-alice`."

**Rejected.** Without the `truffle-` prefix, truffle cannot distinguish its own nodes from random tailnet nodes. A user's MacBook is probably named `alice-mbp` — if an app has `appId = alice`, its nodes look identical to the laptop. The `truffle-` prefix is a library-wide invariant that costs 8 chars of hostname budget and buys unambiguous identification of library-managed nodes.

---

## 13. Open Questions

### 13.1 Should `appId` validation reject reserved words?

`test`, `default`, `truffle`, `cli`, `core` — should any be reserved? My instinct is **no** — let users pick whatever works for them, including `test`. If we ever need a reserved word we can add it as a breaking change.

### 13.2 What happens when the state dir is moved across machines?

If a user rsyncs `~/Library/Application Support/truffle/playground/alice-mbp/` from laptop A to laptop B and starts the playground on both, the Tailscale state dir contains the same machine key, so Tailscale will complain ("this node is already registered elsewhere"). `deviceId` collides too. We can't prevent this usefully; detecting it reliably is hard. For 0.4.0 the answer is "don't do that"; a future RFC can consider a `forceRegenerate` flag or a collision detection heuristic.

### 13.3 Should `deviceName` be globally unique within the app?

I think **no**. Two laptops literally named "alice-mbp" should both be able to run the playground and show up correctly; Tailscale handles the hostname suffixing, and `deviceId` disambiguates at the app layer. Forcing uniqueness would require a registration round-trip we don't have.

### 13.4 How does this interact with `ephemeral: true`?

`ephemeral` is a Tailscale construct — the tailnet entry is removed on shutdown, so next start gets a fresh Tailscale ID. But the `device-id.txt` file (our ULID) stays in the state dir. So a node in ephemeral mode has a stable `deviceId` and a rotating `tailscaleId`. This is actually the right behaviour: the app layer sees a stable logical device even though Tailscale sees churn.

### 13.5 What about CLI auth tokens (`TS_AUTHKEY`)?

Unchanged. Tailscale auth keys are orthogonal to `appId` / `deviceId` — they authenticate the device against the tailnet but tell truffle nothing about identity. Users pass them via `authKey: process.env.TS_AUTHKEY` exactly as today.

---

## 14. Acknowledgements

This RFC emerged from a debugging session where the Electron playground rewrite was invisible to itself on the tailnet. The dig through `is_truffle_peer` and the realisation that the default `/tmp` state dir was silently rotating identity on every reboot surfaced both the immediate bug and the structural gap it implied.

---

## 15. Implementation deviation (as-built, v0.5.x)

> **Superseded by [RFC 022: Peer Handle API](./022-peer-handle-api.md)** (proposed for 0.6.0).  
> RFC 022 replaces string-id-first consumption with a single user-facing **Peer** handle, makes `deviceId` an honest `ULID | null`, and moves dual-index / routing complexity into `PeerRegistry`. The guidance below remains the source of truth for **v0.5.x** consumers until 0.6 ships.

The design above presents `deviceId` as *the* peer identity — §7.3 calls `NapiPeer.deviceId` the "Primary key", and §7.4 has `send()` / routing take a `deviceId`. The shipped implementation only realises that once a peer's hello has been exchanged. This section records what `getPeers()` actually returns, why, and how consumers should key identity until the honest fix lands.

### 15.1 What `getPeers()` actually returns

A peer's ULID and human name travel **only over the envelope-bus WebSocket hello** (§8). That hello is exchanged the first time a WS session opens between two nodes — which happens lazily on the first `send()` / `broadcast()` / `on_message` traffic, not at peer discovery. Until it lands, the session layer holds `PeerState.identity == None`, and the projection to the app-facing peer falls back to the Tailscale layer:

```rust
// crates/truffle-core/src/node.rs — impl From<PeerState> for Peer
let (device_id, device_name, os) = match s.identity.as_ref() {
    Some(identity) => (identity.device_id.clone(), identity.device_name.clone(), Some(identity.os.clone())),
    None            => (s.id.clone(),               s.name.clone(),               s.os.clone()),   // s.id == Tailscale stable id
};
```

So **before the hello, `peer.deviceId == peer.tailscaleId`** (the Tailscale stable node id), and `peer.deviceName` is the Tailscale hostname/slug rather than the original name. For an app that uses **only the raw transports** (QUIC/TCP/UDP via RFC 021) and never touches the envelope bus, `wsConnected` stays `false` for the whole session and the ULID is *never* observed through `getPeers()` — the "Primary key" is permanently the Tailscale id.

### 15.2 The flip: `deviceId` is not stable within a session

Two same-host `tsnet` nodes (`appId: strata-collab`, `ephemeral: true`), talking over DERP, probed 2026-07-08 on published `@vibecook/truffle@0.5.0` (ids redacted head…tail):

- Nodes A (`ulid=01KX21…0BGT`, `tsId=ngNqQe…NTRL`) and B (`ulid=01KX21…SEF3`, `tsId=nqw3mv…NTRL`); ULIDs and Tailscale ids all distinct. (The two ULIDs share the leading `01KX21…` timestamp prefix — they were generated seconds apart — but differ in the random tail.)
- **Pre-send** (both mutually visible, no WS session): `A-sees-B → { wsConnected: false, deviceId: nqw3mv…NTRL }` — i.e. `deviceId === B.tailscaleId`, not the ULID. Symmetric for `B-sees-A`.
- A `send()`s to B (which lazily opens the WS session and exchanges hellos).
- **Post-send** (same tick the session comes up): `A-sees-B → { wsConnected: true, deviceId: 01KX21…SEF3 }` — i.e. `deviceId === B.deviceId (ULID)`. The `wsConnected` false→true transition and the `deviceId` tailscale-id→ULID flip are **simultaneous**, and the value does **not** regress to the Tailscale id for the rest of the session.

**Verdict: FLIP, not stable-forever.** A peer's `deviceId` changes exactly once, from the Tailscale stable id to the ULID, at the instant its envelope-bus WS session connects. A consumer that keys a `Map` by `peer.deviceId` and adds entries both before and after that moment ends up with **two keys for one peer** (a Tailscale-id entry and a ULID entry) — this is the concrete failure mode that produced duplicate mesh links and a dead UDP inbound gate in a real integration.

`send()` reference resolution was probed alongside: the **listed `deviceId`** resolves (pre-hello it is the Tailscale id, which `resolvePeerId` accepts); the bare **Tailscale id string** resolves as the same reference; a raw **tailnet IP** (`100.x`) does **not** resolve (`session error: unknown peer`). So there is no IP escape hatch for `send()` — only id / name / hostname / prefix forms.

### 15.3 Guidance for consumers (until the fix lands)

- **Raw-transport-only apps** (mesh built on QUIC/TCP/UDP, no envelope-bus messaging): key all identity — link maps, dial-direction tie-breaks, per-peer state, inbound gates — on **`tailscaleId`**, which is stable for the whole session and never flips. Treat `deviceId` as opaque/unreliable in this mode. (This is what `examples/canvas-desktop` does; see its `mesh.mjs` note.)
- **Envelope-bus apps** (they call `send`/`broadcast`/`on_message`): `deviceId` is the ULID and is the correct portable, cross-restart identity — but only key on it for peers where `wsConnected === true`. Do not persist a `deviceId` captured while `wsConnected` was `false`; it is a Tailscale id in disguise and will rotate on the next `ephemeral` restart.
- The **local** node's identity is not affected: `getLocalInfo().deviceId` is always the real ULID.

### 15.4 Future work — an honest fix

**See [RFC 022](./022-peer-handle-api.md).** The committed direction is:

1. **Peer handle-first API** — networking takes `Peer`, not “the right string id.”
2. **Honest `deviceId: ULID | null`** — never fall back to Tailscale id in that field.
3. **Eager identity (default)** — learn ULID without requiring app `send`.
4. Optional hostinfo advertisement and raw first-frame as later phases.

Until 0.6 ships, `tailscaleId` remains the canonical pre-hello map key for v0.5.x consumers, and this section remains the source of truth over §7.3 for that line.
