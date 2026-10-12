# RFC 026: Egress — the node's own host processes reach the tailnet through it

**Status**: Implemented (sidecar, core, NAPI); integrated with Tailscale 1.104.1 on 2026-10-12
**Author**: James Yong + Claude
**Date**: 2026-09-30
**Depends on**: RFC 017 (identity & namespacing), RFC 021 (raw transport — the bridge idle reap), RFC 023 (HTTP serving — the engine this mirrors), RFC 024 §6.5 (the Swift plane's loopback), RFC 025 (login-scoped nodes — the gate egress admits by)
**Origin**: VibeField petition T7 (the desktop sync plane, DS-1)

---

## 1. Problem Statement

Truffle serves IN and never OUT. A node's host can publish a loopback service onto the
tailnet (RFC 023: `proxy:add`, `main.go:894` → `handleProxyAdd` `:2350` → `bindProxyRoutes`
`:2393`, targets confined to loopback unless `allowNonLoopback`, `:348`), and the Rust core can
open a raw connection to a peer (`bridge:dial`, `main.go:870` → `handleDial` `:1264` — the
core's own verb over its own pipe, reachable from no other process). Nothing lets an ordinary
process on the same host — a Node daemon, a Chromium renderer — reach a peer THROUGH the
node. The host's other processes reach the tailnet through the operating system's Tailscale
app, or not at all.

tsnet already has the answer and the sidecar did not expose it:
`func (s *Server) Loopback() (addr string, proxyCred, localAPICred string, err error)`
(tailscale.com v1.102.3, `tsnet/tsnet.go:445`) starts a loopback listener that is a SOCKS5
proxy onto the tailnet (username `tsnet`, a generated 16-byte credential) and serves the
LocalAPI behind a second credential; an HTTP proxy on the same listener is a TODO (`:471`). The
Apple plane already exposes that shape — TailscaleKit's `LoopbackConfig`, which RFC 024 §6.5's
`urlSession()` rides — so a phone can dial a desktop through its own node while a desktop
cannot dial another desktop through its own.

**Consumer impact (VibeField).** Desktop-to-desktop federation dials a peer's door over the OS
network, so it routes only where the system Tailscale app is installed and signed in to the
same login — a second identity on every machine. Because that node is not the app's, the peer
cannot attribute the caller to the calling node, and falls back to a claim in the hello. A
remote artifact opens only in the system browser, and a terminal on another desktop cannot be
attached at all. Every one of these is the same missing verb.

## 2. Decision

Three rungs, each additive, on sidecar protocol **7**:

| Rung | Command | For | Authenticated by |
|---|---|---|---|
| **E1** | `tsnet:loopback` | anything that speaks SOCKS5 (a Node `SocksProxyAgent`) | tsnet's SOCKS5 password |
| **E2** | `egress:add` / `egress:remove` / `egress:list` | a client that dials a plain `127.0.0.1:<p>` (a WebSocket URL, a raw socket) | nothing — see §5.1 |
| **E3** | `egress:proxy` | a browser (Chromium answers an HTTP proxy's Basic challenge; it cannot authenticate to SOCKS5) | `Proxy-Authorization: Basic` |

Every listener binds `127.0.0.1`. E2 and E3 reach only nodes of this node's netmap whose owner
passes the node's RFC 025 gate, and re-check at every connection (§3.2). E1 is tsnet's own
server: with its credential it dials anything the node can (§3.1).

Core: `Node::loopback()`, `Node::loopback_with_local_api()`, `Node::egress_add(EgressRoute)`,
`Node::egress_remove(id)`, `Node::egress_list()`, `Node::egress_http_proxy()`
(`crates/truffle-core/src/egress.rs` for the types); NAPI: `NapiNode.loopback()`,
`loopbackWithLocalApi()`, `egressAdd()`, `egressRemove()`, `egressList()`, `egressHttpProxy()`.

## 3. Semantics (normative)

### 3.1 E1 — the node's loopback (tsnet's own)

- `tsnet:loopback {localAPI?, requestId?}` → `tsnet:loopbackResult {address, proxyCredential,
  localAPICredential?, requestId}`. Idempotent: tsnet creates the listener on the first call and
  answers the same address and credentials until the server closes; `tsnet:stop` closes it
  (`srv.Close`), and the next lifecycle mints new credentials.
- SOCKS5 username `tsnet` (tsnet's constant; `truffle_core::LOOPBACK_SOCKS5_USERNAME`),
  password `proxyCredential`.
- **The LocalAPI credential is opt-in.** tsnet serves a read-write LocalAPI on the same
  listener (preferences, logout). `localAPICredential` crosses the bridge only when the command
  says `localAPI: true` (`Node::loopback_with_local_api`); `Node::loopback()` answers `None`, and
  the core drops one it did not ask for.
- **No admission.** tsnet's SOCKS5 server dials with `UserDial`: a tailnet address through
  netstack, anything else through the host's own network (`tsdial` `dialOneUser`,
  `net/tsdial/tsdial.go:587-603`) or an exit node. The credential is therefore the whole gate:
  whoever holds it acts as this node, toward the tailnet and beyond.
- The handler runs on the sidecar's dispatch loop, not a goroutine: tsnet creates the listener
  lazily with no lock of its own and `Server.Close` reads that field unguarded, so a
  `tsnet:loopback` must be serialized with `tsnet:stop`.

### 3.2 Admission (E2, E3)

- **A destination is a node of THIS netmap**, named by its MagicDNS name (any case, trailing
  dot optional) or by one of its tailnet IPs. This node itself counts. A short MagicDNS name
  (`beta`) is refused, and so is a name that merely ends in `.ts.net`: tsnet dials a name outside
  the netmap over the host's own network — another tailnet's Funnel host, say — which is exactly
  the egress these rungs must not offer. The quad-100 service address is not a node.
- **The dial goes to the admitted node's address**: the IP the target named, or the node's first
  IPv4 address when it was named by name. Nothing is re-resolved between admission and dial.
  E2/E3 call the TCP netstack directly, so a peer disappearing between admission and dialing
  cannot make `tsnet.Server.Dial` fall back to the host network. The pinned, unstable subsystem
  API is isolated in `dialEgress` and covered by upgrade regressions.
- **Gates.** The node's RFC 025 `login_allow` lives in the core; the core sends it with
  `egress:add` and `egress:proxy` as `nodeLoginAllow`. A route may add its own `login` glob
  (RFC 025 §3.2 grammar). With either gate present, the destination's owner login must be
  present (`LOGIN_UNKNOWN` otherwise — fail closed) and pass BOTH (`LOGIN_REFUSED`). A tagged
  node's owner is Tailscale's `tagged-devices` pseudo-user, as in RFC 025.
- **Not scoped to the app.** Admission does not require the destination to be a truffle node of
  the same app: the hostname prefix is a discovery filter (anyone can name a node
  `truffle-app-x`), the login is the trust boundary. A gated node reaches its owner's other
  devices; an ungated one reaches its tailnet — what the OS Tailscale app would reach.
- **Per connection.** Every connection a route accepts, and every CONNECT, is admitted afresh
  against the netmap of that moment: a peer that left, or whose owner changed, is refused at the
  next connection, and admitted again if it comes back. An established splice is not re-judged
  mid-stream; a consumer that must cut it on an eviction removes the route (`egress:remove`
  closes every live connection). Cutting live splices on a netmap change is future work (§9).

### 3.3 E2 — routes

- `egress:add {id, target, login?, nodeLoginAllow?, requestId}` → `egress:added {id, listen,
  requestId}`; `egress:remove {id, requestId}` → `egress:removed {id, requestId}`;
  `egress:list {requestId}` → `egress:list {routes: [{id, target, listen, login?}], requestId}`
  (sorted by id, `[]` when empty). Failures answer `egress:error {id?, code, message, requestId}`.
- A route is one `127.0.0.1:0` listener with a FIXED target. Each accepted connection is
  admitted (§3.2), dialled directly through the node's TCP netstack, and spliced raw both ways (the
  sidecar's `bridgeCopy`: half-close on EOF, the node's idle reap). **No HTTP is parsed**:
  with the target fixed there is nothing to route on and nothing to rewrite, so any protocol
  passes byte-for-byte — a WebSocket upgrade's path, escaped segments and query included. The
  far door therefore sees the `Host` header the client wrote (`127.0.0.1:<p>` unless the client
  sets its own).
- The idle reap is the node's `idleTimeoutSecs` (default 10 minutes) in both directions: a
  long-lived connection that can go silent longer needs its own keepalive.
- The id is reserved at once, so a duplicate is refused (`EGRESS_EXISTS`) even while the first
  add is in flight. `tsnet:stop` closes every route and its connections, and an add still in
  flight when the node stops cannot install afterwards.
- A connection the route refuses, or cannot dial, is closed without a byte and reported as a
  runtime `egress:error {id, code, message}` with no `requestId` (the core traces it).

### 3.4 E3 — the CONNECT proxy

- `egress:proxy {nodeLoginAllow?, requestId}` → `egress:proxyResult {address, credential,
  requestId}`. One proxy per lifecycle, started on the first call; later calls answer the same
  address and credential. The gate it admits by is the one it was created with — the node's
  gate is fixed for the node's lifetime (RFC 025 §3.1).
- Each request is answered in this order: no or wrong `Proxy-Authorization` → `407` with
  `Proxy-Authenticate: Basic realm="truffle"` (any username; the password must equal the
  credential, compared in constant time); not `CONNECT` → `405` (`Allow: CONNECT`); a malformed
  authority → `400`; not admitted (§3.2) → a bare `403`, as RFC 023 §9.7's gate answers, with no
  detail about the netmap; dial failure → `502`; else `200 Connection Established` and a raw
  tunnel. Bytes the client pipelines behind the CONNECT (a TLS ClientHello) are forwarded first.
- **CONNECT only.** There is no absolute-form forwarding, so the proxy never parses or rewrites a
  request it relays. Chromium tunnels `https:` (and WebSockets) through CONNECT but sends a
  plain `http:` request in absolute form, so an `http://` tailnet origin is not reachable
  through E3: serve it with TLS (RFC 023 `tls: true`) or reach it through an E2 route.
- Header reads are bounded (10 s, the serve engine's slow-loris bound); tunnels are tracked and
  closed on `tsnet:stop` (`http.Server.Close` does not touch hijacked connections).

## 4. Wire changes

| Command | Data | Answer |
|---|---|---|
| `tsnet:loopback` | `{localAPI?: bool, requestId}` | `tsnet:loopbackResult {address, proxyCredential, localAPICredential?, requestId}` or `tsnet:error {code, message, requestId}` |
| `egress:add` | `{id, target, login?, nodeLoginAllow?: [glob], requestId}` | `egress:added {id, listen, requestId}` or `egress:error` |
| `egress:remove` | `{id, requestId}` | `egress:removed {id, requestId}` or `egress:error` |
| `egress:list` | `{requestId}` | `egress:list {routes: [{id, target, listen, login?}], requestId}` |
| `egress:proxy` | `{nodeLoginAllow?: [glob], requestId}` | `egress:proxyResult {address, credential, requestId}` or `egress:error` |

`egress:error {id?, code, message, requestId?}` codes: `INVALID_COMMAND`, `INVALID_TARGET`,
`INVALID_LOGIN`, `TARGET_NOT_PEER`, `LOGIN_REFUSED`, `LOGIN_UNKNOWN`, `STATUS_UNAVAILABLE`,
`EGRESS_EXISTS`, `NOT_FOUND`, `NOT_RUNNING`, `NOT_READY`, `LISTEN_ERROR`, `DIAL_FAILED`
(runtime only), `SERVE_ERROR`, `INTERNAL`. A `tsnet:loopback` failure rides `tsnet:error` with
`NOT_RUNNING`, `INVALID_COMMAND` or `LOOPBACK_ERROR`. The core maps `NOT_RUNNING` to
`NetworkError::NotRunning` and every other code to the new
`NetworkError::EgressError { code, message }`.

`sidecarProtocolVersion`: 6 → **7**. Protocol 5 added login identity; protocol 6
already identifies authenticated UDP relay support in Truffle 0.7.15. The rule at the constant
is "bump on any addition to the command surface the core must not send blind", and these five are such additions: a v6 sidecar
answers an unknown command only with `tsnet:error {code: UNKNOWN_CMD}` and no `requestId`, which
no waiter can claim, so a blind send could only time out. A core talking to a sidecar below 7
refuses every egress call locally with `NetworkError::Unsupported`, naming both versions.

## 5. Security model

1. **Local processes can connect.** Every listener is loopback-only, so the network cannot
   reach them, but any local process, including another OS user, can connect. E1 and E3 are
   credentialed for that reason. **An E2 route is not**: a raw splice cannot authenticate without parsing the
   protocol it carries, so any local process can use a route to reach that route's one fixed
   target as this node. Truffle cannot decide when that is acceptable; the consumer's rule
   (VibeField DS-L6) is to bind an unauthenticated route only to a far door that demands its own
   grant (a MAC on the first frame), and to reach doors that trust the calling node's identity
   only through E1 or E3.
2. **Credentials** are 16 random bytes, hex. They cross the bridge once per call, on the
   sidecar's stdout (the core's own pipe). Malformed-event diagnostics omit both payloads and
   input-bearing parser messages; the sidecar never logs credentials. The Rust types holding
   them (`LoopbackConfig`, `HttpProxyConfig`, and the internal event payloads, which an
   unexpected-reply error would format) redact them in `Debug`.
3. **Least privilege for the LocalAPI** (§3.1): its credential leaves the sidecar only on request.
4. **E1 dials anything with its credential** (§3.1) — the tailnet, and other destinations through
   the host's network or an exit node. tsnet compares the SOCKS5 password with a plain string
   comparison (`net/socks5/socks5.go:176`), not in constant time; E3's comparison is constant-time.
5. **E2 and E3 are tailnet-only by construction**: admission is netmap membership plus the gates
   (§3.2), and the dial goes to the admitted node's tailnet address, never to a re-resolved name.
6. **Attribution at the far end.** A connection through any rung arrives from this node's
   tailnet address, so the far side's WhoIs — and RFC 023's identity headers on a served door —
   name THIS node and its owner. The two-node rows witness that for all three rungs.

## 6. Compatibility

- Additive: no existing command or event changes shape; protocol-5/6 cores never send the new verbs.
- `NetworkError::EgressError` is a new variant on an exhaustive public enum (as RFC 023's
  `ProxyError` was); no crate in this workspace, and no consumer we know of, matches it
  exhaustively.
- The trait methods on `NetworkProvider` have defaults (`Unsupported`), so mocks and other
  providers compile unchanged.
- The Apple plane is unchanged: TailscaleKit already gives it E1 (`LoopbackConfig`); E2/E3 parity
  is future work.
- The in-process test control plane uses the Tailscale 1.104.1 dependency graph.
  Six additional indirect modules support these tests: `hyperloglog`, `immutable`, `reco`,
  `go-metro`, `hashtriemap`, and `intmap`. No production dependency is downgraded.

## 7. Testing

- **Sidecar, unit** (`egress_test.go`): target parsing; the admission table (24 rows — names,
  case, trailing dot, IPv6, IPv4-mapped, self, a tailnet-range IP no node holds, quad-100, a
  public name, a LAN address, another tailnet's `.ts.net` name, a short name, both gates, a
  tagged owner, an unknown owner gated and ungated, a malformed glob); Basic parsing; the wire
  shapes the core parses; every verb answering with its `requestId` when no node runs; route
  shutdown; `tsnet:stop` teardown.
- **Sidecar, two real tsnet nodes** (`egress_tailnet_test.go`): an in-process tailnet —
  tailscale's `testcontrol` with a local DERP and STUN server, the harness tsnet's own suite
  uses, rebuilt from `derpserver` and `stuntest` — needing no auth key and no network beyond
  loopback. **E1**: a SOCKS5 CONNECT to a peer's tsnet listener by IP and by MagicDNS name
  succeeds with the credential, is refused at authentication with a wrong one and at method
  negotiation with none; a second call answers the same values; the LocalAPI credential appears
  only when asked and opens `/localapi/v0/status`, which the proxy credential does not. **E2**: a
  WebSocket upgrade through a route reaches the peer with its escaped path and query
  byte-for-byte (`/cell/b00t/control%2Fpane?client=2&next=%2Fhome%3Fq`) and the upgraded stream
  flows both ways; a documentation-range address, a public name, quad-100, an unheld tailnet IP,
  another tailnet's name, the node's gate, the route's glob, a malformed glob and a port-less
  target are each refused at `egress:add` with their code, binding nothing; a duplicate id is
  refused; `egress:remove` cuts a live splice; and when the control plane hands the target to
  another owner, the route's next connection is refused (with a runtime `LOGIN_REFUSED`), and
  admitted again once the owner changes back. **E3**: a CONNECT to `<peer>.ts.net:443` with the
  credential tunnels, pipelined bytes first; by tailnet IP too; without or with a wrong credential
  `407` with a Basic challenge; a public name, a documentation-range address, an unheld tailnet
  IP, another tailnet's name and quad-100 are `403`; an absolute-form GET is `405`; a proxy
  created under a gate that excludes the peer's owner answers `403`. In E1, E2 and E3 the far
  side's WhoIs names the egressing node.
- **Mutation probes** (run once, not committed): admit-everything, no per-connection re-check,
  no pipelined forward, the LocalAPI credential always included, no proxy auth, and no
  connection close on shutdown each turned their rows red. The egress rows are clean under
  `-race`.
- **Core**: the command payloads (camelCase, `localAPI`, the gate's omission when ungated); the
  event payloads (the literals the sidecar's wire-shape test pins); `Debug` redaction; every
  verb refused locally on protocol-1/5/6 sidecars, and passed through on 7; the node's
  `login_allow` riding `egress:add` and `egress:proxy`; every answer mapped through the
  production reply broker, including a refusal staying typed, `NOT_RUNNING`, a misrouted
  `egress:removed`, and an unexpected answer named without printing its credential; Node-level
  `Unsupported` on a provider without egress and `Stopped` after stop.
- **Release regressions**: protocol-6 sidecars refuse every egress verb locally; a destination
  disappearing between admission and dial never reaches the host dialer (IPv4 and IPv6);
  malformed stdout, including serde error details, never logs credentials.
- **Self dialing**: E2 and E3 reach the node itself by IP and MagicDNS name under its login gate.
- **Real tailnet** (`integration_egress.rs`): the public Node API exercises all three rungs
  through the real sidecar, checks payloads and far-side WhoIs attribution, and verifies route
  removal closes a live stream. The existing integration workflow runs this with its auth key.

## 8. Consumers

VibeField DS-1: E1 replaces federation's OS-network dial with a SOCKS agent carrying the
credential; E2 gives the routed terminal runtime a `ws://127.0.0.1:<p>/cell/…` for a remote cell
whose door verifies its own grant; E3 backs the artifact partition's PAC rule (`*.ts.net` → the
proxy, everything else direct). With all three, nothing crosses through the OS Tailscale app.

## 9. Future work

- Cut established splices when the netmap evicts their target (an IPN-bus watch per node, not
  per connection).
- A runtime egress-error stream on the Node API, as `proxy_runtime_errors` is for the serve engine.
- `mesh.egress` in `@vibecook/truffle` and the Tauri plugin.
- E2/E3 on the Apple plane.

## 10. Decisions

- **D1** three rungs, additive, all `127.0.0.1` (§2).
- **D2** E1 is tsnet's own server, exposed as-is; its credential is its whole gate (§3.1).
- **D3** the LocalAPI credential is opt-in (§3.1).
- **D4** admission is netmap membership by MagicDNS name or tailnet IP; a `.ts.net` suffix is not
  enough; the dial goes to the admitted address (§3.2).
- **D5** the node's RFC 025 gate travels with each command (`nodeLoginAllow`); a route glob
  narrows it; the owner must be present, fail closed (§3.2).
- **D6** admission is by login, not by app hostname prefix (§3.2).
- **D7** re-admission per connection; established splices are not re-judged (§3.2, §9).
- **D8** E2 splices raw TCP with no HTTP parsing, because the target is fixed (§3.3).
- **D9** E2 routes carry no credential; the consumer decides which far doors may be reached that
  way (§5.1).
- **D10** E3 is CONNECT only, Basic with any username, constant-time; refusals are bare (§3.4).
- **D11** protocol 7, because protocol 6 is authenticated UDP and cannot answer the new verbs (§4).
  The UDP relay frame version and HMAC domain remain at 6 for interoperability.
