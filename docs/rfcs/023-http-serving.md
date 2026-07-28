# RFC 023: HTTP Serving over the Tailnet

**Status**: Phases 1–3 implemented (2026-07-12) on `feat/rfc023-http-serving`; Phases 4 (Funnel) and 5 (service discovery) are future work
**Author**: James Yong + Claude
**Date**: 2026-07-12
**Depends on**: RFC 012 (layered architecture), RFC 015 (bindings rewrite), RFC 017 (identity & namespacing), RFC 021 (raw transport JS API), RFC 022 (peer handle API)
**Adopts**: the reverse-proxy subsystem (PR #36, 2026-04-12) — shipped without an RFC; this document is its retroactive spec and its v2

---

## 1. Problem Statement

RFC 021 gave JS apps the *client* half of HTTP over the mesh (`mesh.http.agent`,
`request`, `fetchText`). The *server* half is fragmented:

1. **Serving your own JS handler** (Express/Koa/Fastify/plain `node:http`) requires
   hand-wiring `node:http` onto `mesh.net.createServer` — possible (that is exactly
   what `mesh.ws.createServer` does internally, `ws.ts:138`) but undocumented, and
   there is no TLS, so browser consumers get no padlock, no secure context, and no
   `wss:`.
2. **Publishing an existing local service** (a dev server, Grafana, a container) has
   a real engine — the proxy subsystem — but it shipped spec-less in PR #36 and has
   sat untouched since: whole-port forwarding only, always-TLS, no path routes, no
   static file serving, dead discovery code, and several security gaps (§4.2).
3. **Naming**: the node's MagicDNS FQDN is exposed (`getLocalInfo().dnsName`) but has
   no ergonomic surface, and the default hostname convention
   (`truffle-{appId}-{device}`) produces URLs nobody wants to screenshot.
4. **Port 443** — the URL that matters (`https://name.ts.net/`, no port suffix) — is
   squatted by a v1-era fossil listener (§4.3).

The audience is bigger than the mesh: dynamic tsnet listeners are raw TCP on the
tailnet wire, so **any Tailscale device — a phone browser, curl, a teammate's laptop —
can reach a truffle HTTP server**, with WhoIs identity attached per connection. This
RFC turns that into a product: *publish an app or a directory to your tailnet in one
call*.

## 2. Goals

1. `mesh.http.createServer(opts?, handler?)` — a drop-in `node:http.createServer`
   replacement that Express/Koa/Fastify/socket.io/`ws` attach to unmodified.
2. `mesh.serve(config)` — one declarative verb for publishing local services and
   static directories, with path routes, mirroring the `tailscale serve` mental model.
3. TLS with automatic MagicDNS certificates on both paths, correctly signalled to
   frameworks (`req.secure === true`).
4. Caller identity on every request: `req.socket.remotePeer` (JS path) and
   `Tailscale-User-*` headers (engine path), spoof-proof.
5. Reclaim port 443; expose `mesh.dnsName`; allow a custom hostname for pretty URLs.
6. Close the shipped engine's gaps: loopback-restricted targets, port guards, runtime
   error propagation, behavioral tests.

## 3. Non-Goals

- **Funnel** (public-internet exposure). Deferred to Phase 4; the config key is
  reserved and its constraints documented (§9.6), but no implementation here.
- **HTTP/3**, and HTTP/2 *termination inside Node* (§12 D12 — the Go engine gets h2
  free; the JS path stays HTTP/1.1).
- **An auth framework.** We expose verified identity; authorization stays app code
  (middleware examples in docs). Route-level allow-lists are future work (§13).
- **Raw TCP/UDP proxying.** The serve engine is HTTP(S)-shaped; raw streams already
  have `mesh.net` (RFC 021).
- **WebTransport / WebRTC** (unchanged from RFC 021 §13).

## 4. Current State (evidence)

### 4.1 What exists and works

- **Proxy engine (Go)**: `proxy:add|remove|list` sidecar ops (`main.go:612-617`);
  per-proxy `srv.ListenTLS(":{port}")` (`main.go:2068`) with
  `httputil.NewSingleHostReverseProxy` (`:2020`); WebSocket upgrade via `Hijack()`
  with bidirectional copy (`:2118-2202`); URL reported as
  `https://{dnsName}:{port}` (`:2109`); `ReadHeaderTimeout` 10 s, `IdleTimeout`
  120 s, detail-free 502 ErrorHandler (`:2045-52`).
- **Rust core**: `Node::proxy()` → `add(ProxyConfig) / remove(id) / list() /
  subscribe()` (`proxy/mod.rs:74-138`); provider trait `proxy_add/remove/list`
  (`network/mod.rs:147-173`) implemented by `TailscaleProvider`.
- **Bindings, all four**: NAPI `NapiProxy` (add/remove/list/onEvent), Tauri commands +
  `truffle://proxy-event`, CLI `truffle proxy add|remove|list` over daemon JSON-RPC.
  TS reaches it only as a raw passthrough — `mesh.proxy()` returns the bare
  `NapiProxy`; no ergonomic namespace, no PeerLike idiom.
- **TLS listen support (unplumbed)**: `tsnet:listen` already honors `tls: true` →
  `srv.ListenTLS` on any port (`main.go` `listenData.TLS`, `handleListen`), and the
  Rust protocol layer has the parameter (`sidecar.rs:542
  send_listen(port, tls: Option<bool>)`) — but the provider hard-codes `None`
  (`provider.rs:682`). Nothing above Layer 3 can request a TLS listener today.
- **Identity per connection**: the TRFF header carries the WhoIs identity JSON
  (`dnsName, loginName, displayName, profilePicUrl, nodeId`) for **any** tailnet
  caller, truffle or not (`header.rs:37-39`); RFC 021 already surfaces
  `remotePeerId/remotePeerName` on inbound `TruffleSocket`s.
- **Self dnsName**: `getLocalInfo().dnsName` exists end-to-end
  (`NodeIdentity.dns_name`, napi `node.rs:179-190`). Missing only sugar.

### 4.2 Engine gaps (inventory, PR #36 as-built)

| # | Gap | Evidence |
|---|-----|----------|
| G1 | `announce` flag + `discovery.rs` are dead code — `add()` never passes `announce`; `build_announcements()`/`extract_remote_proxies()` have no callers. No mesh advertisement happens. | `proxy/mod.rs:96-103`, `proxy/discovery.rs` |
| G2 | Targets are **not** loopback-restricted; `isLoopbackHost()` only gates `InsecureSkipVerify`. Any reachable host/IP is a valid target → the node is a pivot into its LAN. | `main.go:2019,2040,2422` |
| G3 | No identity headers injected; inbound client-supplied `Tailscale-User-*` pass through **unstripped** (spoofable). | only `Header.Set` is `Host` (`main.go:2022-26`) |
| G4 | Proxy `listen_port` bypasses `ensure_port_unreserved` — `add({listenPort: 9417})` is accepted by Rust and fails only at the sidecar OS bind. | no guard call in the proxy chain; guard exists `node.rs:1341-47` |
| G5 | Runtime sidecar errors (`SERVE_ERROR`, `CONNECTION_REFUSED`) are logged, never forwarded — `emit_error` is `#[allow(dead_code)]`. | `proxy/mod.rs:22-27,44-52` |
| G6 | No path routes, no static serving, TLS mandatory (no plain-HTTP option). | `ProxyConfig`/`proxyAddData` field sets |
| G7 | No behavioral tests (Go or Rust) and no docs/guide/examples. | `main_test.go:176`; docs grep |

### 4.3 The port-443 fossil

The sidecar starts `ListenTLS(":443")` unconditionally at `tsnet:start`
(`main.go:787,812`). Accepted connections get a WhoIs lookup and are bridged to Rust
with `service_port = 443` (`main.go:837-840`) — where they are **always dropped**:
the bridge dispatches inbound conns by registered port channel
(`bridge.rs:207-224`), the only `register_listener` call site is
`provider.rs:709` (inside `listen_tcp`), and nothing listens on 443 — the session
layer listens on `ws_port` 9417, `ensure_port_unreserved` forbids user listeners on
443, and truffle-cli has zero 443 references.

History: the listener dates to the original v1 Go shim (`191a107`); sessions moved to
the dynamic ws-port listener in the v2 architecture swap (`2f2f310`, RFC 012 Phase 7)
and the consumer was never re-created. Net effect today: any tailnet device probing
`https://node:443` triggers a **pointless ACME certificate issuance**, a TLS
handshake, a WhoIs, a loopback bridge — and then a dropped connection. Its dial-side
twin (the legacy "auto-TLS-wrap dials to 443", RFC 021 §4) is equally orphaned.

### 4.4 Naming state

Default hostname is composed as `truffle-{app_id}-{slug(device_name)}`
(`node.rs:1620`); there is no override on `NodeBuilder`. Bare-name resolution of
*hello-less* peers depends on that prefix convention (`hostname_slug`,
`node.rs:1332`); once a peer has done the RFC 017 hello, identity-based resolution
works regardless of hostname.

## 5. Design Overview

One rule decides where bytes flow:

> **If the request handler is JavaScript, bytes come to JS** (`mesh.http.createServer`
> over the RFC 021 bridge). **If the target is a directory or another process, bytes
> stay in Go** (`mesh.serve` = the proxy engine, v2).

```
 browser / curl / truffle peer ──tailnet──▶ tsnet listener (Go sidecar)
                                                │
              ┌─────────────────────────────────┴──────────────────────────┐
              │ JS path (handler is your code)   │ engine path (target elsewhere)
              │ tsnet:listen (+tls)              │ proxy:add v2 (+tls, routes)
              │ loopback bridge + TRFF identity  │ httputil.ReverseProxy / FileServer
              ▼                                  │ Tailscale-User-* inject (strip first)
 Rust listen_tcp ─▶ NAPI accept() ─▶ TruffleSocket ▼
              ▼                            localhost:3000 · ./public · (bytes
 patched http.Server ─▶ Express/Fastify/ws   never cross Rust/NAPI/V8)
```

Consequences worth stating: the engine path never crosses the loopback bridge, so the
bridge's 256-conn cap and 10-minute idle reap (RFC 021 §10) don't apply to it; the JS
path does cross it, and inherits them.

**Naming decision (D2)**: the engine keeps its `proxy` identity in Rust
(`Node::proxy()`), the sidecar protocol, the CLI, and Tauri — renaming four binding
layers buys nothing. TypeScript gets **`mesh.serve()`** as the one ergonomic verb
(config shapes cover proxy *and* static *and* mixed routes), because `tailscale
serve` is the mental model users arrive with. The raw `mesh.proxy()` passthrough
remains for advanced use. The docs rule: *"You wrote the handler? `http.createServer`.
It's a directory or another process? `serve`."*

## 6. TypeScript API (`@vibecook/truffle`)

### 6.1 `mesh.http.createServer(options?, requestListener?)`

```ts
const app = express();
const server = mesh.http.createServer({ tls: true }, app);
server.listen(443, () => console.log(`https://${mesh.dnsName}/`));
```

- Returns a **real `node:http.Server`** (constructed, never net-bound) whose
  `listen()` / `close()` / `address()` are instance-patched to drive a mesh
  `TruffleServer`; accepted `TruffleSocket`s are fed via
  `server.emit('connection', socket)`. Because `instanceof http.Server` holds,
  Express, Koa, socket.io, and `ws`'s `WebSocketServer({ server })` ('upgrade'
  event) attach unmodified. Fastify uses its documented `serverFactory` option.
  (`mesh.ws.createServer` already proves this wiring in-repo; Phase 1 refactors it
  onto the shared implementation.)
- `options.tls?: boolean` (default **false**) — TLS terminates in the sidecar
  (tsnet `ListenTLS`, automatic MagicDNS certs). On TLS listeners each inbound
  socket gets `socket.encrypted = true` — the property frameworks sniff — so
  `req.secure` / `req.protocol === 'https'` are correct.
- `listen(port[, callback])`, port 0 supported (ephemeral, resolved by sidecar).
  `server.address()` → `{ port }`.
- **Identity**: `req.socket` is a `TruffleSocket` with `remotePeerId`,
  `remotePeerName`, and (new) `remotePeer: Peer | null` — the RFC 022 handle,
  resolved live through the packages/core registry. `null` is possible and is
  the reject cue: transiently before the caller is interned (registry lag right
  after discovery), always when the namespace is built without registry hooks
  (outside `createMeshNode`), and permanently for future Funnel traffic. The
  WhoIs strings (`remotePeerId`/`remotePeerName`) are set from accept time
  regardless. No headers are injected on this path — there is no proxy hop to
  inject them.

### 6.2 `mesh.serve(config)` → `ServeHandle`

```ts
// A. Expose a local dev server
const h = await mesh.serve({ port: 443, target: 'http://localhost:3000' });
console.log(h.url); // https://myapp.tail1234.ts.net

// B. Static SPA
await mesh.serve({ port: 443, dir: './public', fallback: '/index.html' });

// C. Mixed routes (the SPA + API deployment shape)
await mesh.serve({
  port: 443,
  routes: {
    '/api':     'http://localhost:8000',
    '/grafana': { target: 'http://localhost:3001' },
    '/':        { dir: './public', fallback: '/index.html' },
  },
});
```

- Config shapes: `{ port, target }` (single proxy) · `{ port, dir, fallback? }`
  (static) · `{ port, routes }` (mixed). Common options: `tls?: boolean` (default
  **true** — preserves shipped engine behavior; the audience is browsers),
  `name?`, `allowNonLoopback?: boolean` (default false, §9.3), `allow?: string[]`
  (loginName globs; absent = whole tailnet; also per-route, route wins — §9.7),
  `announce?` (accepted, inert until Phase 5 — documented as such).
- Routes match by **longest prefix**, order-independent. Per-route
  `stripPrefix?: boolean` (default **false** — Grafana-style backends need the
  prefix preserved; make stripping explicit).
- Static serving: Go `http.FileServer`/`ServeContent` semantics — `index.html` at
  directory roots, ETag/Range/mime for free, **no directory listing**, dotfiles
  denied, `fallback` rewrites misses for SPAs.
- Returns `ServeHandle { id, url, port, config, close(): Promise<void> }`, an
  `EventEmitter` for runtime errors (`'error'` → `{ code, message }`, backed by the
  G5 fix). `mesh.serve` is sugar over the proxy engine: handle ↔ proxy `id`.
  `config` is the exact normalized JSON that created the handle — apps wanting
  boot-time re-serving persist it and replay `mesh.serve(saved)`; the library
  itself never persists (there is nothing to resume: the sidecar dies with the
  process, so "rehydration" is always re-creation).
- `funnel?: true` is **reserved and rejected** ("not supported yet, see RFC 023
  §9.6") until Phase 4.

### 6.3 `mesh.dnsName: string | null`

The node's MagicDNS FQDN (`myapp.tail1234.ts.net`), populated after start from
`getLocalInfo().dnsName`; `null` before Running. Read this instead of
string-building URLs — Tailscale dedupes hostname collisions by suffixing `-1`/`-2`,
so the requested name is not always the granted name.

### 6.4 `createMeshNode({ hostname })` — pretty URLs

Optional override of the composed `truffle-{appId}-{device}` hostname; validated as a
single lowercase DNS label (`[a-z0-9-]{1,63}`, no dots). **Documented tradeoff**:
hello-less (raw-transport-only) peers with a custom hostname are not resolvable by
bare device name (the `hostname_slug` convention no longer matches) — they remain
resolvable by full hostname, dnsName, IP, and deviceId, and by display name once
they hello (RFC 022 identity resolution is hostname-independent). Serving-centric
nodes that also participate in the mesh lose nothing in practice.

## 7. Engine v2 (sidecar changes)

`proxy:add` payload additions (all optional — absent fields reproduce v1 behavior
exactly, so old cores work against new sidecars):

```jsonc
{
  "id": "web", "name": "web", "listenPort": 443,
  "tls": true,                        // NEW: false → srv.Listen (plain HTTP)
  "allowNonLoopback": false,          // NEW: gate on isLoopbackHost(target)
  "allow": ["*@corp.com"],            // NEW: loginName globs; absent = whole tailnet
  "routes": [                          // NEW: replaces single target when present
    { "prefix": "/api", "targetUrl": "http://localhost:8000", "stripPrefix": false,
      "allow": ["ops@corp.com"] },     // per-route allow overrides the config-level list
    { "prefix": "/",    "dir": "/abs/path/public", "fallback": "/index.html" }
  ],
  // v1 fields (targetHost/targetPort/targetScheme) remain the no-routes shorthand
}
```

- **Identity headers (G3 fix)**: on every proxied request, first **strip** all
  inbound `Tailscale-*` headers, then set `Tailscale-User-Login`,
  `Tailscale-User-Name`, `Tailscale-User-Profile-Pic` from the connection's WhoIs
  (Tailscale's own header convention). Applies to route targets and the v1
  single-target path; static routes need none. `X-Forwarded-For`/`-Proto` set
  explicitly. *Amended (sidecar v3):* `Tailscale-Node-Id` (WhoIs `Node.StableID`)
  and `Tailscale-Node-Name` (MagicDNS FQDN, trailing dot stripped) are injected
  alongside the user trio — node identity is present even for callers with no
  user profile (tagged nodes), which the login gate structurally cannot see.
- **Target validation (G2 fix)**: reject non-loopback `targetUrl`/`targetHost`
  unless `allowNonLoopback` — at *add* time, with a distinct error code
  (`TARGET_NOT_LOOPBACK`).
- **Allow-lists (minimal, deliberately)**: before proxying/serving, evaluate the
  route's `allow` globs (fall back to config-level) against the connection's WhoIs
  `loginName`; no match → `403`, no body detail. Applies on the ReverseProxy path
  **and the WS hijack path**. Semantics are capped at loginName globs (§9.7);
  richer policy (tags, groups, posture) is the tailnet ACL layer's job.
- **Port guards (G4 fix)**: Rust-side `ensure_port_unreserved(listen_port,
  ws_port)` on the proxy add path (443 becomes legal per §8.1); sidecar keeps its
  `PROXY_EXISTS` / `PORT_IN_USE` checks.
- **Cert pre-warm (always on, no knob)**: every TLS listener creation
  (`tsnet:listen` with `tls`, `proxy:add`) fires a fire-and-forget certificate
  fetch for the node's dnsName. Non-fatal — failure logs and emits a warning-level
  event — but it makes "HTTPS certificates not enabled on this tailnet" surface at
  *listen time* instead of as the first visitor's mystery handshake failure, and it
  hides ACME latency (tsnet caches certs in the state dir, so it is a no-op after
  first issuance).
- **Error propagation (G5 fix)**: sidecar `proxy:error` events forwarded by a
  persistent core task into `ProxyState.event_tx` (un-deadening `emit_error`), and
  surfaced on `ServeHandle`/`NapiProxy.onEvent`.
- WebSocket upgrade (existing hijack path) extended to per-route targets. Go's TLS
  listener + `net/http` give **h2 termination free** on the engine path.
- Static handler: `http.FileServer` behind the route mux with the §6.2 hardening.
- **Behavioral tests (G7 fix)**: Go httptest-level tests for routing, header
  strip/inject, loopback gate, fallback; Rust wire-format tests for new fields.

### 7.1 `tsnet:listen` TLS plumb (JS path)

`NetworkProvider::listen_tcp(port)` grows an options form (mirroring the RFC 021
`dial_tcp_opts` precedent): `listen_tcp_opts(port, ListenOpts { tls: bool })` →
`send_listen(port, Some(tls))` — the sidecar side already works. Exposed as
`Node::listen_tcp_opts`, NAPI `listenTcp(port, tls?)`, consumed by
`mesh.http.createServer` and `mesh.ws.createServer` (`wss:` for browsers).

## 8. Rust core & NAPI changes

### 8.1 Port 443 reclamation

Remove the startup `listenTLS(":443")` and its accept loop from the sidecar, **and
its dial-side twin**: the legacy "`tls: nil` auto-wraps dials to port 443" rule
(`shouldWrapTLS`) becomes `nil = never wrap` (explicit `tls: true` still wraps).
Old cores are unaffected — their session dials target `ws_port`, never 443 (443 was
reserved). Drop the `443` arm from `ensure_port_unreserved` (`ws_port` stays
reserved). Compat: only
v1-era peers (pre-`2f2f310`) ever dialed a session over :443-TLS; every supported
version dials `ws_port`. New-core + old-sidecar: the old sidecar still binds 443, so
a user `listen(443)` fails its OS bind — map that `LISTEN_ERROR` to a "sidecar ≥
{version} required for port 443" NodeError. Release notes carry the min-sidecar
version (same pattern as RFC 021's `tls`/`idleTimeoutSecs`).

### 8.2 Other core changes

- `ProxyConfig` v2 fields (`tls`, `allow_non_loopback`, `routes:
  Vec<ProxyRoute>`), all serde-defaulted → non-breaking; `ProxyRoute { prefix,
  target: RouteTarget (Url | Dir { path, fallback }), strip_prefix }`.
- Proxy add path calls `ensure_port_unreserved`; proxy runtime-error forwarding task
  (G5); `announce` documented inert until Phase 5.
- `NodeBuilder::hostname(label)` override + validation (§6.4); resolver untouched.
- NAPI: `NapiProxyConfig` v2 fields; `listenTcp(port, tls?)`; no new identity
  surface needed (`getLocalInfo().dnsName` exists) — `mesh.dnsName` is TS sugar.

## 9. Security Model

1. **Exposure scope**: a served port is reachable by the **entire tailnet**, gated
   only by Tailscale ACLs — *not* scoped to `app_id` like the QUIC-ALPN or WS-hello
   paths (RFC 021 §9). For HTTP that is usually the point; the guide says it loudly.
2. **Identity is verified, headers are contractual**: WhoIs identity comes from the
   WireGuard tunnel, not from anything the client sent. The engine strips inbound
   `Tailscale-User-*` before injecting (spoof defense); the JS path reads
   `socket.remotePeer`, which no client can forge.
3. **No LAN pivot by default**: targets loopback-only unless `allowNonLoopback`
   (mirrors `tailscale serve`); enforced at add time.
4. **TLS is about browsers, not secrecy**: WireGuard already encrypts and
   authenticates every tailnet byte. `tls: true` buys the padlock, secure-context
   APIs (clipboard, service workers), and `wss:`. Certificates require MagicDNS +
   HTTPS enabled on the tailnet; first handshake per name does ACME issuance
   (seconds); the cert matches only the full `.ts.net` FQDN — `https://short-name/`
   fails by design, `http://short-name/` works.
5. **Key custody**: TLS private keys live in the Go process and tsnet state dir
   only; with sidecar termination they never cross into Rust or JS memory.
6. **Funnel runway (future)**: public exposure will be a separate loud opt-in —
   TLS mandatory, ports 443/8443/10000 only, tailnet policy must grant `funnel`,
   **no caller identity** (WhoIs fails; headers absent; `remotePeer === null`) —
   apps relying on identity must handle anonymous before enabling it.
7. **Allow-lists are a gate, not a framework**: the serve engine is the one place
   where user code isn't running (a directory or a proxied backend cannot do its
   own auth), so `allow` exists there — loginName globs, evaluated against WhoIs,
   403 on miss, on both the proxy and WS-hijack paths. Scope is capped at
   loginName matching: device tags, groups, and posture are the tailnet ACL
   layer's job; request-level policy on the JS path is middleware's job
   (`req.socket.remotePeer`).

## 10. Constraints (documented, user-facing)

| Constraint | Detail |
|---|---|
| Tailnet prerequisites for TLS | MagicDNS + HTTPS certificates enabled in the admin console; clear error otherwise |
| ACME first-hit latency | seconds on the first TLS handshake per name; §13 pre-warm option |
| Cert name match | full `.ts.net` FQDN only; use `mesh.dnsName` in URLs |
| JS path limits | inherits the RFC 021 bridge: 256 concurrent conns, 10-min idle reap; HTTP/1.1 only |
| Engine path limits | its own Go conns (no bridge caps); `IdleTimeout` 120 s; h2 free |
| Route semantics | longest-prefix wins; `stripPrefix` default false |
| `announce` | accepted but inert until Phase 5 |
| Port 443 | requires sidecar ≥ this release; one listener per port per node |

## 11. Implementation Plan

- **Phase 1 — `mesh.http.createServer` (TS only, no native changes). Landed.** Patched-listen
  `http.Server`, `socket.remotePeer`, shared wiring with `mesh.ws.createServer`;
  unit tests on mocked natives (`net.test.cjs` pattern); examples: Express on the
  mesh, Fastify `serverFactory`, `ws` upgrade; phone-browser demo at
  `http://name:8080`. Docs: new guide page `serving-http.md`.
- **Phase 2 — TLS & naming. Landed.** `listen_tcp_opts` plumb; 443 fossil removal (listener
  + dial auto-wrap) + unreserve + version-gated error; cert pre-warm (always-on);
  `socket.encrypted` shim; `mesh.dnsName`; `hostname` override; release notes
  (min sidecar).
- **Phase 3 — engine v2 + `mesh.serve`. Landed.** Sidecar routes/static/headers/loopback
  gate/allow-lists/error forwarding; core `ProxyConfig` v2 + guard + event task;
  NAPI/Tauri field updates; CLI `truffle serve` family (positional URL-or-dir
  target, `serve status`, `serve stop`; `truffle proxy` becomes a hidden
  deprecated alias); `mesh.serve()` + `ServeHandle` (incl. `config`); Go
  behavioral tests; guide + examples (`serve-static-spa`, `expose-dev-server`).
- **Phase 4 — Funnel.** `ListenFunnel`, constraints per §9.6, anonymous-identity
  semantics, its own examples.
- **Phase 5 — service discovery.** Wire `announce` over a SyncedStore
  (`ss:truffle-services`), `mesh.services` browse API; un-deadens `discovery.rs`.
  Gets its own RFC, with three principles pinned now: (1) code-facing API only —
  `list()` / `watch()` returning `{ peer, name, url, port }`; no human-browse UI
  ambitions; (2) liveness rides the RFC 022 `Peer` handle (`service.peer.online`)
  — announcements are declarative store state reaped on peer leave, no separate
  heartbeat protocol; (3) **`announce` flips its default to `false` the moment it
  becomes live** — today's inert `default true` must not start broadcasting
  existing users' proxies on upgrade.

Each phase lands green independently; Phases 1 and 2 are individually releasable.

### Implementation notes (as built, 2026-07-12)

Deviations and refinements from the spec above, all deliberate:

- **ServeHandle events**: runtime engine errors emit `'serveError'` always and
  `'error'` only when a listener is attached — an unlistened EventEmitter
  `'error'` crashes the process, which a fire-and-forget `serve()` must never do.
- **Bare `host:port` targets**: the JS API rejects them with an add-`http://`
  hint (WHATWG `URL` parses `localhost:` as a scheme); the CLI accepts the
  `localhost:3000` shorthand. Deliberate per-surface ergonomics; documented in
  the guide.
- **`announce` is not in `ServeOptions`**: an inert option has no business in
  the public type; it remains (default `true`, inert) at the `NapiProxyConfig`
  layer only, until Phase 5 flips it live-and-false (D15).
- **`id?` option added** (default `serve-{port}`, `name` defaults to the id);
  duplicate live ids fast-fail client-side to protect event routing.
- **protocolVersion gating** landed as designed (§8.1): the sidecar advertises
  `protocolVersion: 2` in every status event; v1 sidecars get a loud error for
  v2-only features instead of silently dropped wire fields.
- **CLI keeps a v1-compatible fast path**: a plain root-mounted URL serve
  (no v2 options) is sent in the v1 single-target wire shape, so `truffle
  serve http://localhost:3000 --port 8443` works against pre-RFC-023 sidecars.

## 12. Decisions

- **D1 — bytes placement**: handler-in-JS → bridge path; target-elsewhere → Go
  engine. No JS byte-pumping proxies.
- **D2 — adopt, don't rename**: PR #36's proxy subsystem *is* the serve engine;
  `proxy` naming stays in Rust/sidecar/CLI/Tauri; TS ergonomic verb is
  `mesh.serve()`; raw `mesh.proxy()` remains.
- **D3 — real `http.Server`**: patch `listen`/`close`/`address` on a genuine
  instance; never subclass or re-implement. Framework compat is the whole point.
- **D4 — 443**: remove the fossil listener **and** the dial-side `tls: nil` →
  auto-wrap-on-443 rule (evidence §4.3), unreserve 443, version-gate the error.
  Ancient (pre-v2-swap) peers lose nothing supported.
- **D5 — TLS defaults differ by path, deliberately**: `serve` defaults `tls: true`
  (browser audience; preserves shipped behavior); `createServer` defaults
  `tls: false` (app↔app default; WireGuard already encrypts). Both explicit in docs.
- **D6 — identity**: engine injects `Tailscale-User-*` (and, since sidecar v3,
  `Tailscale-Node-Id`/`Tailscale-Node-Name`) after stripping the whole inbound
  `Tailscale-*` namespace; JS path exposes `socket.remotePeer` and injects
  nothing.
- **D7 — loopback-only targets** by default; `allowNonLoopback` opt-out at add time.
- **D8 — hostname override** ships with TLS (Phase 2), single-label validation,
  tradeoff documented (§6.4).
- **D9 — Funnel deferred**, key reserved, constraints pre-documented (§9.6).
- **D10 — discovery kept, not culled**: `announce`/`discovery.rs` stay (already in
  shipped binding types), documented inert, implemented in Phase 5.
- **D11 — routes**: longest-prefix, order-independent, `stripPrefix` default
  false for URL targets. Dir mounts always strip — the mount point maps to the
  directory root (`/app/x` on a `/app` dir mount serves `dir/x`), which is why
  `stripPrefix` is rejected on dir routes rather than defaulted.
- **D12 — HTTP/2**: engine path terminates h2 in Go for free; JS path stays h1.1
  (no ALPN into Node; sidecar-terminated TLS). Revisit only with demand.
- **D13 — cert pre-warm is behavior, not an option**: always fire-and-forget on
  TLS-listener creation; non-fatal; doubles as early "HTTPS not enabled" detection
  (§7). A `warmCert: false` knob would only opt out of good error reporting.
- **D14 — minimal allow-lists ship in Phase 3**, not later: the engine path is the
  only place user code can't gate requests itself. Semantics capped at loginName
  globs (§9.7); evaluated where identity headers are injected, hijack path
  included.
- **D15 — `announce` defaults to `false` when discovery goes live** (Phase 5);
  inert `true` until then. No surprise broadcasts on upgrade.
- **D16 — no library persistence**: `ServeHandle.config` (the normalized creating
  JSON) is the entire persistence feature; replay is app code. Daemon-side
  persistence is a future CLI decision, out of scope here.

## 13. Open Questions

1. `mesh.services` browse API detail (beyond the three pinned principles in §11
   Phase 5) — its own RFC once Phase 3 lands and real serve usage exists.
2. Daemon-mode persistence for CLI serves (`truffle serve --bg` writing daemon
   state) — future CLI decision, independent of the library (D16).
