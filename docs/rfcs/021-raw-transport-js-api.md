# RFC 021: Raw Transport Surface for the JS API

**Status**: Implemented (Phases 1–4, 2026-07-07; Phase 5 items remain future work)
**Author**: James Yong + Claude
**Date**: 2026-07-07
**Depends on**: RFC 012 (layered architecture), RFC 015 (bindings rewrite), RFC 017 (identity & namespacing)
**Reverses**: RFC 015's decision to exclude raw TCP/UDP from the JS surface ("too low-level")

> The CrdtDoc / Loro feature (RFC 018) was removed. The original design in
> sections 4 and 6 also predates the release of port 443 for applications.
> TCP and QUIC listeners now reserve only the configured session WebSocket port
> (default 9417). [RFC 023, section 8.1](023-http-serving.md#81-port-443-reclamation)

---

## 1. Problem Statement

Truffle's goal is a nice interface for doing network things easily on top of tsnet. Today the
public docs promise exactly that — `docs/index.md` advertises "Direct WS/TCP/UDP/QUIC connections
between all nodes" and `docs/guide/architecture.md` claims "All four transports verified working
cross-machine over real Tailscale" — but none of it is reachable from TypeScript:

| Protocol | Layer 4 (Rust) | Node API (Rust) | NAPI / TS |
|---|---|---|---|
| WebSocket | ✅ real, production (`websocket.rs`) | internal only (powers `send`/`broadcast`) | hidden pipe under `send()` |
| TCP | ✅ real (`tcp.rs`) | ✅ `open_tcp` / `listen_tcp` work | ❌ not bridged |
| UDP | ✅ real (`udp.rs` + sidecar relay) | ❌ `open_udp` stub (`node.rs:821`) | ❌ not bridged |
| QUIC | ✅ real (`quic.rs` + `quic_socket.rs`) | ❌ `open_quic` stub returning `()` (`node.rs:812`) | ❌ not bridged |
| WebRTC | — | — | — (RFC 008 non-goal) |
| WebTransport | — | — | — |

Two corrections to the common understanding of this table, established by code archaeology:

1. **The foundation is ~85% done, not missing.** RFC 012 Phase 8 shipped (`9362cea`,
   `b404452`, `8008f0f`): UDP rides a sidecar datagram relay (`tsnet:listenPacket` + a 6-byte
   address header over a loopback UDP socket, `packages/sidecar-slim/main.go:1115`,
   `provider.rs:765`), and QUIC rides that relay via a `quinn::AsyncUdpSocket` implementation
   (`transport/quic_socket.rs`) — the same pattern iroh uses for its magic socket. Cross-machine
   integration tests exist (`tests/integration_transport.rs`: `test_quic_concurrent_bidirectional`,
   `test_udp_rapid_fire`, `test_quic_and_ws_simultaneous`, …). The `Node::open_udp`/`open_quic`
   stubs are pre-Phase-8 leftovers that were never rewired.
2. **RFC 015's "raw TCP hard to bridge to JS" is solved.** A pull-model NAPI handle
   (`read(): Promise<Buffer|null>`) wrapped in a `stream.Duplex` gives natural backpressure with
   no ThreadsafeFunction, and Node's ecosystem has documented seams for custom duplexes:
   `http.Agent#createConnection` accepts any `stream.Duplex`, the `ws` package rides a custom
   agent, and undici `fetch()` accepts a custom `connect` dialer. One good Duplex makes
   Express/HTTP/ws/fetch all work over the mesh for free.

The remaining work is therefore mostly **API design + bindings**, not transport engineering.

## 2. Goals

1. **Node-stdlib mimicry** (Principle of Least Surprise): `mesh.net` looks like `node:net`,
   `mesh.dgram` looks like `node:dgram`. Existing Node networking code ports by swapping the
   module. (Source: `docs/node-api-thinking`.)
2. **Ecosystem interop as a feature**: run Express, the `ws` package, and `fetch()` over the mesh
   via the documented custom-duplex seams — instead of reimplementing HTTP/WS in Rust.
3. **Modern promise/async-iterable API for QUIC** (sessions and streams, iroh-shaped:
   `connect(peer) → connection → openStream()/streams()`).
4. **Peer-first addressing everywhere**: `host` accepts peer name, device id, or 100.x IP; one
   resolver, consistent with `resolve_peer_id`.
5. **Uniform lifecycle**: every handle has explicit `close()`; `mesh.stop()` tears down all
   sockets, listeners, and the sidecar.
6. **Tauri parity path**: same capabilities exposable through the id-keyed-registry pattern
   (live handles can't cross Tauri IPC).

## 3. Non-Goals

- **WebRTC.** RFC 008 §9.5's non-goal stands. Over a tailnet, WebRTC's NAT traversal and
  encryption are redundant (double-tunneled), and native↔native data channels are strictly worse
  than QUIC streams. The one real use case — browser↔native — is deferred until a browser-peer
  story exists; when it does, `str0m` with host-only ICE is the implementation path, and truffle's
  envelope layer is the signaling channel. We ship a **recipe doc**, not an API.
- **WebTransport (this RFC).** Deferred to a follow-up RFC (see §11 Phase 5). Two viable modes
  are already scoped: (a) tailnet-resident browsers hitting a tsnet listener with Tailscale's
  Let's Encrypt `ts.net` certs (no cert dance, 90-day validity), and (b) LAN/localhost browsers
  hitting an OS socket via `wtransport` + `serverCertificateHashes` (P-256, ≤14-day, SHA-256,
  auto-rotation — supported by `wtransport::tls::self_signed`). WebTransport is Baseline across
  all four engines as of Safari 26.4, so this is worth doing — as its own RFC once QUIC ships.
- **IPv6 for datagrams (v1).** The relay's 6-byte `[IPv4][port]` header is IPv4-only in three
  synchronized places (`main.go:1345`, `network/mod.rs:329`, `quic_socket.rs:88`). Tailnets
  always assign a 100.x IPv4, so this costs nothing today. Widening to a family-tagged header is
  Phase 5.
- **Media codecs** (RFC 008 §9.5 unchanged) and **browser-as-mesh-peer**.
- **Kernel-grade throughput.** Everything crosses the userspace gVisor netstack plus a loopback
  hop; no GSO/GRO. Fine for control traffic, file transfer, and app data; not line-rate bulk UDP.

## 4. Current State (evidence)

- **Data path**: every byte crosses the Go sidecar. TCP/WS: one loopback TCP connection per
  stream, 46+-byte `TRFF` header with session token + WhoIs identity (`header.rs:26-46`,
  `main.go:2224`). UDP: one loopback UDP socket per bound tsnet port, 6-byte address header,
  datagram boundaries preserved; anti-hijack `TRUFFLE_UDP_REGISTER` handshake (`main.go:1312`).
- **Dial/listen**: outbound `bridge:dial` reaches any `target:port` (`main.go:909`); inbound
  `tsnet:listen` binds arbitrary ports incl. ephemeral `0` (`main.go:972`), routed to Rust by
  destination port (`bridge.rs:56`). **Caveat**: dials to port 443 are silently TLS-wrapped
  (`main.go:951`).
- **Node API**: `open_tcp(peer_id, port) → tokio TcpStream` (real, `node.rs:776`);
  `listen_tcp(port) → RawListener` (real, `node.rs:801`; `RawIncoming` currently drops the
  bridge's WhoIs identity); `open_udp`/`open_quic` are `NotImplemented` stubs.
- **QUIC internals**: per-call self-signed rcgen certs; client uses `SkipServerVerification`
  (accept-any — sound because WireGuard already authenticates the path, `quic.rs:184-219`);
  legacy `Handshake{device_id, protocol_version}` with **no app_id or identity check** (unlike
  WS's RFC 017 hello); ALPN `"truffle"`; 4-byte length-prefix framing on a single bi-stream.
  `QuicTransport` is shaped as a future session transport, not as a user-facing multi-stream API.
- **Transport traits are not unifiable**: `StreamTransport`/`RawTransport`/`DatagramTransport`
  have heterogeneous verbs and return types, use `async_fn_in_trait` (not dyn-compatible), and
  are generic over `N: NetworkProvider` — the FFI layer needs concrete per-protocol handle types
  regardless.
- **NAPI**: no byte ever crosses back to JS today (only `Buffer` in on `send`). Handle-lifecycle
  precedents exist (`Arc` + tracked `JoinHandle`s + explicit `stop()`, `synced_store.rs:150`);
  there are **no `Drop` impls** — GC'd handles leak tasks until `node.stop()`.
- **Operational limits**: bridge caps 256 concurrent TCP conns (`bridge.rs:34`); Go reaps
  bridged conns idle >10 min (`main.go:2336`); WS session port 9417 and sidecar TLS port 443 are
  reserved; tailnet MTU ≈1280 (QUIC's 1200-byte initial fits).

## 5. Design Overview

```
TypeScript   mesh.net (node:net mimic)   mesh.dgram (node:dgram)   mesh.quic (sessions/streams)
             mesh.ws ('ws' pkg over mesh.net)   mesh.http (Agent/fetch/createServer sugar)
                 │ stream.Duplex / EventEmitter / async iterables
NAPI         NapiTcpSocket  NapiTcpListener  NapiUdpSocket  NapiQuicConnection  NapiQuicStream
                 │ pull-model: read()/recv()/accept() return Promises (backpressure = await)
Node (Rust)  open_tcp  listen_tcp  bind_udp  connect_quic  listen_quic     ← one peer resolver
                 │
Layer 4      TcpTransport      UdpTransport        QuicTransport (+ QuicConnection wrapper)
Layer 3      TailscaleProvider ── TCP bridge ── UDP relay ── Go sidecar (tsnet)
```

Three principles:

1. **Pull, don't push, across NAPI.** Byte data crosses only as the resolution of a JS-initiated
   Promise (`read`/`recv`/`accept`). The existing ThreadsafeFunction event pattern
   (`NonBlocking`, unbounded queue) stays reserved for low-rate events; a fast byte stream would
   queue unboundedly. JS awaiting each read *is* the backpressure and mirrors core's
   `FramedStream::recv` shape.
2. **Native shapes in TS, plain handles in NAPI.** The NAPI classes are deliberately minimal and
   un-idiomatic (promise-per-op); the `@vibecook/truffle` TS layer wraps them in the shapes
   Node developers expect (`stream.Duplex`, `dgram`-style `EventEmitter`, async iterables).
3. **WebSocket ships zero new Rust.** `mesh.ws` wraps the battle-tested `ws` npm package over
   mesh TCP (client via custom `http.Agent`, server via `httpServer.emit('connection', duplex)`).
   `websocket.rs` remains the internal envelope pipe on 9417.

## 6. Rust Core Changes (Phase 1)

### 6.1 Unified peer resolver

`open_tcp`, `ping`, and session `send` each hand-roll peer lookup (`node.rs:780-788`, `:606-617`,
`session/mod.rs:468-484`) and none accept the device-id prefixes `resolve_peer_id` supports.
Extract one internal resolver used by all of them and every new method:

```rust
/// Accepts: peer name, full device id, ≥4-char device-id prefix, or 100.x IP.
async fn resolve_peer(&self, peer_ref: &str) -> Result<Peer, NodeError>
```

### 6.2 UDP: replace the stub with the real shape

`open_udp(peer_id)` is the wrong shape for UDP (sockets aren't per-peer). Replace with:

```rust
pub async fn bind_udp(&self, port: u16) -> Result<MeshUdpSocket, NodeError>   // port 0 = ephemeral
```

`MeshUdpSocket` wraps `UdpTransport::bind` (which already routes through the tsnet relay and
falls back to a host socket only for non-tsnet providers) plus a resolver handle:
`send_to(data, target: &str, port: u16)` resolves peer refs; `recv_from` returns
`(bytes, SocketAddr, Option<peer_id>)` — source IPs are WireGuard-authenticated by Tailscale, so
IP↔netmap mapping *is* the identity model for datagrams.

### 6.3 QUIC: expose connections, not the framed pipe

Rework `quic.rs` to separate two consumers:

- **User-facing** (new): `QuicConnection` wrapping `quinn::Connection` + resolved peer identity.
  `open_stream() → QuicStream` (plain byte stream over one bi-stream — no 4-byte framing, no
  legacy `Handshake`), `accept_stream()`, `close()`. `QuicListener` accepts connections.
  ```rust
  pub async fn connect_quic(&self, peer_ref: &str, port: u16) -> Result<QuicConnection, NodeError>
  pub async fn listen_quic(&self, port: u16) -> Result<QuicListener, NodeError>
  ```
- **Internal** (existing): the framed single-stream `StreamTransport` impl stays for future
  session-transport use.

**App scoping via ALPN**: user QUIC uses ALPN `truffle-raw.{app_id}` so cross-app connections
fail at the TLS layer — the QUIC analog of the WS hello's app_id check, at zero runtime cost.
Keep self-signed certs + `SkipServerVerification` (WireGuard authenticates the path; document
this explicitly). Peer identity on accept = source-IP↔netmap lookup, same as UDP.

### 6.4 TCP polish

- Plumb the bridge header's WhoIs identity into `RawIncoming` (it's parsed and then dropped
  today): `remote_identity: Option<PeerIdentity>`.
- Add `tls: Option<bool>` to `bridge:dial` (Go + Rust protocol structs) so raw TCP to a peer's
  443 is possible; default preserves today's auto-TLS behavior. Old sidecars ignore unknown JSON
  fields, so this is version-safe; gate on sidecar version in release notes.
- Reject `listen_tcp`/`listen_quic` on reserved ports 443 and 9417 with a clear error.

### 6.5 Sidecar knob

`tsnet:start` gains optional `idleTimeoutSecs` (default 600, current behavior) so long-lived
quiet user connections aren't silently reaped at 10 minutes. Documented alongside a
recommendation to send app-level keepalives (TCP keepalive on the loopback hop does not cross
the bridge).

## 7. NAPI Surface (Phases 2–3)

New module `crates/truffle-napi/src/raw_socket.rs` (+ `quic.rs`). All classes follow the
`Arc` + tracked-tasks + explicit-`close()` pattern, **plus `Drop` impls that close the underlying
socket** (fixing the leak-until-node-stop precedent). All methods `&self` with `Arc<Mutex<half>>`
interior mutability — no `unsafe fn` surface. Errors: `Error::from_reason` (codebase convention).
`ts_args_type` must reference the real `Napi`-prefixed type names (existing `.d.ts` has dangling
refs to avoid copying).

```ts
export declare class NapiTcpSocket {
  read(maxBytes?: number): Promise<Buffer | null>   // null = clean EOF
  write(data: Buffer): Promise<void>
  close(): Promise<void>
  remoteAddress(): string                            // 100.x
  remotePeerId(): string | null                      // from bridge WhoIs (inbound) / dial target
}
export declare class NapiTcpListener {
  accept(): Promise<NapiTcpSocket | null>            // null = listener closed
  port(): number                                     // resolved (ephemeral 0 → real)
  close(): Promise<void>
}
export declare class NapiUdpSocket {
  send(data: Buffer, host: string, port: number): Promise<number>   // host: peer ref or IP
  recv(): Promise<{ data: Buffer; address: string; port: number; peerId?: string }>
  localPort(): number
  close(): Promise<void>
}
export declare class NapiQuicConnection {
  openStream(): Promise<NapiQuicStream>
  acceptStream(): Promise<NapiQuicStream | null>
  remotePeerId(): string | null
  close(): Promise<void>
}
export declare class NapiQuicStream { /* read/write/close as NapiTcpSocket */ }
export declare class NapiQuicListener {
  accept(): Promise<NapiQuicConnection | null>
  port(): number
  close(): Promise<void>
}
// On NapiNode:
openTcp(host: string, port: number): Promise<NapiTcpSocket>
listenTcp(port: number): Promise<NapiTcpListener>
bindUdp(port: number): Promise<NapiUdpSocket>
connectQuic(host: string, port: number): Promise<NapiQuicConnection>
listenQuic(port: number): Promise<NapiQuicListener>
```

Read-size default 64 KiB (matches file-transfer chunking); `Buffer` in both directions (one copy
on the return path — acceptable; no zero-copy precedent exists and the loopback hop dominates).

## 8. TypeScript API (`@vibecook/truffle`)

`createMeshNode` returns a `MeshNode` wrapper (delegating all existing NapiNode methods, with a
`.native` escape hatch) that adds protocol namespaces:

```ts
const mesh = await createMeshNode({ appId: 'my-app', hostname: 'kitchen' });

// ── TCP: node:net mimic; sockets are real stream.Duplex ──────────────────
const server = mesh.net.createServer((socket) => {
  console.log(`conn from ${socket.remotePeerName}`);   // + remoteAddress, remotePeerId
  socket.pipe(socket);
});
await server.listen(8080);                              // port 0 → server.port has the real one
const client = mesh.net.connect({ host: 'other-machine', port: 8080 });

// ── The payoff: existing ecosystems just work ─────────────────────────────
http.createServer(app);                       // Express over the mesh:
meshServer.on('connection', (sock) => httpServer.emit('connection', sock));
const res = await mesh.http.fetch('other-machine:8080/api/status');   // undici custom connect
// mesh.ws.createServer / mesh.ws.connect — thin wrappers over the 'ws' pkg (optional peer dep)

// ── UDP: node:dgram mimic ────────────────────────────────────────────────
const sock = mesh.dgram.createSocket();
sock.on('message', (msg, rinfo) => { /* rinfo.address, rinfo.port, rinfo.peerId */ });
await sock.bind(8081);
sock.send(payload, 8081, 'other-machine');
// pump loop awaits recv() between emits → slow handlers delay reads and the relay
// drops excess datagrams — correct UDP semantics, bounded memory.

// ── QUIC: modern, iroh-shaped ────────────────────────────────────────────
const quicServer = await mesh.quic.listen(4433);
for await (const conn of quicServer) {                   // async-iterable connections
  for await (const stream of conn.streams()) {           // streams are stream.Duplex too
    stream.pipe(stream);
  }
}
const conn = await mesh.quic.connect('other-machine', 4433);
const stream = await conn.openStream();

await mesh.stop();   // closes every socket/listener/stream, then the sidecar
```

Also in this layer: fix `packages/core/src/index.ts` re-export drift (`NapiProxy`,
`NapiCrdtDoc` are currently unreachable via `@vibecook/truffle`).

## 9. Security Model

- **Inbound raw connections are tailnet-authenticated but app-unauthenticated.** Any device on
  the tailnet (subject to Tailscale ACLs) can reach a raw TCP/UDP listener — unlike the envelope
  layer, whose WS hello enforces app_id + WhoIs identity. Mitigations: QUIC gets app scoping via
  ALPN (§6.3); TCP surfaces WhoIs identity on every inbound socket (§6.4); UDP/QUIC surface
  source-IP↔netmap identity. Users decide; docs say so loudly.
- **QUIC's accept-any cert verifier is intentional** and stays: the WireGuard tunnel provides
  authentication + encryption; QUIC TLS is protocol compliance. Revisit (cert pinning via
  envelope-exchanged per-node certs) only if a non-Tailscale provider ever lands.
- **Loopback hardening is already in place** (constant-time session token on the TCP bridge,
  `TRUFFLE_UDP_REGISTER` on the relay) — new APIs ride the same paths and inherit it.
- **Future (Phase 5)**: per-listener accept-callback ACLs, mirroring RFC 014's `OfferDecision`
  pattern.

## 10. Constraints (documented, user-facing)

| Constraint | Value | Consequence |
|---|---|---|
| Datagram payload | keep ≤ ~1,200 B | tailnet MTU ≈1280; larger fragments in netstack, loss-prone |
| Datagram addressing | IPv4 (100.x) only | v6-only peers unreachable for UDP/QUIC until Phase 5 |
| Bridge concurrency | 256 TCP conns | `MAX_CONCURRENT_CONNECTIONS`, make configurable if hit |
| Idle reap | 10 min default | configurable via §6.5; send keepalives on quiet sockets |
| Reserved port | Configured session WebSocket port (default 9417) | TCP and QUIC reject this port. Pass 0 for an ephemeral port and read the assigned port from the listener. |
| Throughput | userspace netstack | no GSO/GRO; loopback hop per direction; "good, not line-rate" |
| WS envelope cap | 16 MiB/frame | unchanged; raw streams have no frame cap |

## 11. Implementation Plan

**Phase 1 — Core unstub (Rust + tiny Go).** §6 in full: resolver, `bind_udp`, `QuicConnection` +
`connect_quic`/`listen_quic`, `RawIncoming.remote_identity`, dial `tls` flag, reserved-port
guards, `idleTimeoutSecs`. Tests: extend `transport/tests.rs` + cross-machine
`tests/integration_transport.rs` (node-level QUIC/UDP, identity plumbing). Refresh the stale
"Stubs only" doc at `transport/mod.rs:13`.

**Phase 2 — TCP to TypeScript (proves the thesis).** `NapiTcpSocket`/`NapiTcpListener`,
`openTcp`/`listenTcp` on NapiNode, `MeshNode` wrapper + `mesh.net` Duplex/Server, `mesh.http`
Agent + fetch. Examples: `examples/netcat`, `examples/express-over-mesh`. Docs page. (Pre-commit
hook will regenerate `index.d.ts` and re-run the meta-crate doctest — expected.)

**Phase 3 — UDP + QUIC to TypeScript.** `NapiUdpSocket` + `mesh.dgram`; QUIC classes +
`mesh.quic` async iterables. Example: `examples/quic-streams`.

**Phase 4 — Ecosystem + parity.** `mesh.ws` over the `ws` package; Tauri id-keyed-registry
parity (`tcp_open → streamId`, `tcp_read/write/close`, `truffle://tcp-*` events) and guest-js
refresh (it has drifted badly); drive-by fixes: `@vibecook/truffle` re-exports, dangling
`ts_args_type` names, `examples/README.md` stale `NapiStoreSyncAdapter` mention.

**Phase 5 — Future RFCs / follow-ups.** WebTransport gateway RFC (ts.net-cert mode +
`serverCertificateHashes` mode); QUIC datagrams (RFC 9221); IPv6 datagram header; listener ACL
callbacks; WebRTC recipe doc (mesh as signaling plane).

## 12. Decided Questions

| # | Decision | Rationale |
|---|---|---|
| D1 | Pull-model NAPI handles (no TSFN for bytes) | backpressure = awaiting JS; TSFN NonBlocking queues unboundedly |
| D2 | TS wraps handles in `stream.Duplex` / dgram / async iterables | Principle of Least Surprise; unlocks http/ws/fetch seams |
| D3 | No native user-facing WebSocket | `ws` pkg over mesh TCP is strictly better; `websocket.rs` stays internal |
| D4 | QUIC user API = connection + byte streams (iroh shape) | multi-stream is the point of QUIC; framed pipe stays internal |
| D5 | QUIC app scoping via ALPN `truffle-raw.{app_id}` | RFC 017 parity at the TLS layer, no in-stream handshake |
| D6 | Keep `SkipServerVerification` + self-signed certs | WireGuard authenticates; document loudly |
| D7 | Datagrams stay on the UDP loopback relay (never the TCP bridge) | boundary preservation free; no HoL blocking; quinn contract |
| D8 | `bind_udp(port)` replaces `open_udp(peer_id)` | UDP sockets aren't per-peer; peer refs accepted per-send |
| D9 | Defer WebRTC (recipe) and WebTransport (own RFC) | redundant over tailnet / needs cert-mode design respectively |
| D10 | `MeshNode` TS wrapper class over NapiNode | namespaces need a home; `.native` escape hatch keeps compat |

## 13. Open Questions

1. **Error codes**: keep `Error.from_reason` strings (status quo) or introduce stable `code`
   fields (`ERR_PEER_NOT_FOUND`, …) on the new surface? Leaning codes-on-new-surface only.
2. **`mesh.quic.listen` default port**: require explicit port (current lean) or default 9420?
3. **QUIC datagrams in v1?** quinn makes it ~trivial; deferred to keep Phase 3 tight — confirm.
4. **Per-listener ACL callback shape** (Phase 5): `(peerIdentity) => boolean | Promise<boolean>`
   mirroring `OfferDecision`?
5. **Bridge conn cap (256)**: expose as builder knob now or when someone hits it?
