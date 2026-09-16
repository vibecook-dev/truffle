# RFC 025: Login-Scoped Nodes — the tailnet login as the mesh boundary

**Status**: Implemented (2026-09-16) on `feat/rfc025-login-scoped-nodes` (core + sidecar) and `feat/rfc025-apple-login` (Apple plane, on top of PR #188)
**Author**: James Yong + Claude
**Date**: 2026-09-16
**Depends on**: RFC 017 (identity & namespacing), RFC 022 (peer handle API), RFC 023 §9.7 (loginName allow globs), RFC 024 (Truffle Swift)
**Origin**: VibeField petitions T4 (the login name on the Apple plane) and T5 (the synced store scoped to a login)

---

## 1. Problem Statement

A truffle node admits every node on its tailnet that carries the app's hostname prefix
or merely *claims* the app id in its hello. On a personal tailnet that is fine. On a
shared tailnet — a family, a company, a tailnet with a colleague's devices on it — the
mesh is the union of every user's devices, and every subsystem built on the session
plane (the synced store, request/reply, file transfer, chat) is shared with strangers by
construction. Read at 0.7.13:

1. **Discovery admits by hostname prefix alone.** `is_app_peer` (`network/tailscale/provider.rs:492`)
   admits any node whose hostname starts with `truffle-{appId}-`; the login of the node's
   owner is never consulted, because the sidecar's peer rows (`peerInfo`, `main.go:143`)
   do not carry it.
2. **The hello admits any claimant.** `validate_hello` (`transport/websocket.rs:216`) checks
   the self-declared `app_id`; `verify_authenticated_identity` (`:292`) compares the claimed
   `tailscale_id` against the bridge's WhoIs `nodeId` and **fails open** when the bridge
   supplies none (mock providers, a failed WhoIs, a legacy header). The WhoIs JSON already
   carries `loginName` (`peerIdentityData`, `main.go:172`; `TailscalePeerIdentity`,
   `network/mod.rs:386`) and the transport parses only `nodeId`.
3. **The store trusts the payload's author.** `apply_remote_slice` (`synced_store/sync.rs:219`)
   keys a slice on the payload's `device_id` and rejects only our own; `Clear { device_id }`
   (`:202`) removes any device's slice on the sender's say-so (TF-2). A departed peer's
   slice is never removed at all: `handle_peer_left` (`:326`) removes by the **Tailscale**
   id while `remotes` is keyed by the **device** ULID — the unit test passes only because
   its mock peer violates RFC 022 I1 (`id == device_id`).
4. **The Apple plane surfaces no login.** `AuthenticatedPeer` carries `tailscaleId` and
   addresses, `BackendStatus`/`BackendPeer` carry `dnsName`/`tailnetIPs`, and the LocalAPI
   WhoIs decoder (`TailscaleKitBackend.swift:397`) keeps `Node.StableID`/`Node.Addresses`
   and drops `UserProfile`. A phone cannot say who is signed in, cannot tell its own
   account's desktop from a colleague's, and cannot scope account-owned state.

VibeField's ruling (2026-09-15): one VibeField user ↔ one tailscale login; a linked node
interacts **only** with nodes under that login; every store is scoped to the login
identity. Until truffle can express that, VibeField publishes nothing content-bearing on
the store bus.

## 2. Decision

A node may declare a **login allow-list** — the same `loginName` globs RFC 023 §9.7
already evaluates for served routes — and when it does:

- **Layer 3 reports only peers whose login matches** (the login is a netmap fact: the
  sidecar's peer rows and self status now carry `loginName`).
- **The hello refuses every caller whose WhoIs login does not match**, before our own
  hello is revealed, with a new close code **4004 `login refused`**; a gated node also
  stops failing open — a caller with no authenticated identity is refused with 4003.
- **A synced-store slice is applied only under its sender's authenticated `device_id`**
  (and `Clear` only for that id), on every node, gated or not.
- **The login is a first-class field on every public identity surface** — the node's own
  (`NodeIdentity`, `BackendStatus`, `MeshNode`), each peer's (`Peer`, `BackendPeer`), and
  each accepted connection's (`TailscalePeerIdentity`, `AuthenticatedPeer`).

An empty list is today's behaviour exactly (the whole tailnet, hostname-prefix
discovery, fail-open on absent identity). Nothing on the wire changes for ungated nodes.

## 3. Semantics (normative)

### 3.1 The gate is per node, not per store

`login_allow` is a `NodeBuilder` / `NodeConfig` / `MeshConfiguration` field, fixed for the
node's lifetime. Layer 5 owns admission; a per-store list would leave the connection open
to a foreign login for every other namespace (`chat`, `ft`, `rr`, raw dial), which is
exactly the class the ruling forbids. "The store scoped to the login" is the corollary:
every store on a gated node is scoped by construction. To change the list, restart the
node (VibeField does: link → start gated, unlink → stop).

### 3.2 Glob grammar — Go `path.Match`, case-insensitive

The three implementations (Go sidecar, Rust core, Swift core) share one grammar and one
test table (`TestAllowedLogin` in `main_test.go` is the reference):

- both the pattern and the login are lowercased first (`strings.ToLower` / `str::to_lowercase`
  / `String.lowercased()`);
- `*` matches any run (including empty) of characters **other than `/`**; `?` matches exactly
  one non-`/` character; `[abc]`, `[a-z]`, `[^abc]` character classes; `\x` escapes `x`;
- a malformed pattern (unterminated class, trailing `\`) never matches and never panics;
- an empty list means no gate; a non-empty list with an empty or absent login **fails closed**.

### 3.3 Layer 3 — the login is a peer fact

- The sidecar's `tsnet:peers` and `tsnet:peerChanged` rows carry `loginName` (from
  `ipnstate.Status.User[peer.UserID].LoginName`; a tagged node reports Tailscale's
  `tagged-devices` pseudo-user; an unknown user omits the field). `tsnet:status` /
  `tsnet:started` carry the node's own `loginName` the same way. Sidecar protocol
  version **5**.
- `SidecarPeer.login_name` → `NetworkPeer.login_name: Option<String>`; the self login →
  `NodeIdentity.login_name: Option<String>`.
- The provider's peer filter becomes `is_app_peer(hostname) && login_allowed(login)`; a
  row without a login on a gated node is **not a peer**. A gated node whose sidecar speaks
  a protocol older than 5 cannot know logins and **refuses to start** (`NetworkError::StartFailed`,
  loud) rather than run peerless or, worse, ungated.
- Swift mirrors it: `BackendPeer.loginName` (from `status.User[String(peer.UserID)]`),
  `BackendStatus.loginName` (self), and `MeshNode.upsertFromLayer3` applies the same predicate.

### 3.4 Layer 4/5 — the hello gate

On an accepted connection, after `validate_hello` and **before** `send_hello` (an impostor
never learns our identity block):

| bridge identity                                     | ungated (today)          | gated                                   |
|-----------------------------------------------------|--------------------------|-----------------------------------------|
| absent / unparseable / no `nodeId`                  | accept unverified        | **refuse 4003** `identity unavailable`  |
| `nodeId ≠ claimed tailscale_id`                     | refuse 4003              | refuse 4003                             |
| `nodeId` ok, `loginName` absent                     | accept                   | **refuse 4004** `login refused`         |
| `nodeId` ok, `loginName` matches no glob            | accept                   | **refuse 4004** `login refused`         |
| `nodeId` ok, `loginName` matches                    | accept                   | accept                                  |

The Swift `Handshake.server` already has the `.failClosed` policy for the first row; it
gains the login rows. The dialing side needs no new check: a gated node only dials peers
its Layer 3 reported, and Layer 3 filtered them (3.3).

The hostname-prefix half of the Layer 3 predicate is **not** enforced at the hello. A
custom-hostname node (RFC 023 §6.4) has no prefix and legitimately joins by hello; the
prefix is a discovery heuristic, the login is the admission rule (RFC 017 §4 already calls
the hello's `app_id` check "belt-and-braces" over the prefix). Petition T5's third ask is
answered by the gate, not by the prefix.

### 3.5 Sender-bound slices (TF-2)

In the store's sync task, an inbound `Update`/`Full` whose `device_id` is not the sender's
**published** device id (`PeerState::published_device_id()` for the sender's Tailscale id) is
dropped with a warning; so is a `Clear` naming any other id; so is any slice from a sender
whose identity is not published (hello-less, or suppressed under RFC 022 first-wins). On
`PeerEvent::Left`, the departed peer's slice is removed by **its published device id**, which
the event's final `PeerState` carries — fixing the Tailscale-id/ULID mismatch. The unit test
that hid it is corrected to a peer whose ids differ (RFC 022 I1).

### 3.6 Public identity surfaces

| surface                       | Rust                                   | NAPI                          | Swift                                         |
|-------------------------------|----------------------------------------|-------------------------------|-----------------------------------------------|
| the node's own login          | `NodeIdentity.login_name`              | `NapiNodeIdentity.loginName`  | `BackendStatus.loginName`, `MeshNode.loginName` |
| a peer's login                | `Peer.login_name`                      | `NapiPeer.loginName`          | `BackendPeer.loginName`, `Peer.loginName`     |
| an accepted caller's identity | `TailscalePeerIdentity` (unchanged)    | (unchanged)                   | `AuthenticatedPeer.loginName`, `.displayName` |
| the gate                      | `NodeBuilder::login_allow(globs)`      | `NodeConfig.loginAllow`       | `MeshConfiguration.loginAllow`                |

Every login field is `Option`/optional — absent, never fabricated (RFC 022's honesty rule).

### 3.7 What is unchanged

- The hello envelope stays at version 2. The login is never self-declared; WhoIs is the authority.
- The QUIC plane (ALPN app check) and raw `listen` (the app reads `IncomingConnection.remote_identity`)
  keep their own admission; the reverse proxy keeps `allow` (RFC 023 §9.7). Gating those
  planes by the node's list is future work (§7).

## 4. Wire changes

- `tsnet:peers`/`tsnet:peerChanged` `peerInfo`: `+ loginName?: string`.
- `tsnet:status`/`tsnet:started` `statusData`: `+ loginName?: string`.
- `sidecarProtocolVersion`: 4 → 5.
- WebSocket close code **4004** "login refused" (RFC 017 §8 table: 4001 app mismatch,
  4002 hello protocol, 4003 identity mismatch / unavailable, 4004 login refused).

## 5. Testing

- **Glob**: the Go table (`TestAllowedLogin`) reproduced verbatim in Rust
  (`network/login_allow.rs`) and Swift (`LoginGlob`).
- **Layer 3**: the sidecar's `peer_watch_test.go` harness gains a `User` map and asserts
  `loginName` on the row and on the self status; the Rust provider tests feed
  `PeersReceived` rows with and without logins under a gate.
- **Hello gate**: `transport/tests.rs` sets the mock bridge identity (`incoming_remote_identity`)
  with a login and drives every row of the 3.4 table, asserting the close code the client
  sees; `session/tests.rs` asserts a refused caller is never installed.
- **Store**: `synced_store/tests.rs` — a spoofed `Update`, a spoofed `Clear`, a hello-less
  sender, and the corrected peer-left row.
- **Swift**: `HandshakeTests` for the gate rows, `IdentityTests` for the glob table,
  `NodeLoopbackTests` for a gated loopback pair, `TailscaleEndpointTests`/backend mapping
  tests for the login fields.
- **Real tailnet** (`TRUFFLE_TEST_AUTHKEY`): a pair on one login — the gated side lists a
  foreign glob and the dial is refused with 4004 and the store never converges; the gated
  side lists the real login and the pair converges as before.

## 6. Consumers

VibeField (petitions T4/T5): the desktop starts its node gated on its one linked login;
the store bus returns, scoped; the phone reads its own login for the account line and
labels a peer self/guest by comparing logins.

## 7. Future work

- Per-plane gates for QUIC accept and raw listen (the node's list as a default for
  `IncomingConnection` consumers).
- Tags, groups and posture: the tailnet ACL layer's job (RFC 023 §9.7's cap stands).
- A per-store *narrowing* list (a subset of the node's) if a consumer ever needs one.

## 8. Decisions

- **D1** per-node gate, not per-store (§3.1).
- **D2** grammar = Go `path.Match`, case-insensitive, fail-closed on absent login (§3.2).
- **D3** the login is a Layer 3 fact carried by the sidecar; a gated node requires protocol 5 (§3.3).
- **D4** the hello gate refuses before revealing our hello; 4003 for no identity when gated, 4004 for login (§3.4).
- **D5** the hostname prefix is not enforced at the hello — RFC 023 §6.4 custom hostnames (§3.4).
- **D6** sender-bound slices and `Clear`, on every node; peer-left removes by published device id (§3.5).
- **D7** the login on every public identity surface, optional and honest (§3.6).
- **D8** hello version unchanged; the login is never self-declared (§3.7).
- **D9** QUIC/raw/proxy planes unchanged in this RFC (§3.7, §7).
