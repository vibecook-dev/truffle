# Truffle

[![CI](https://github.com/vibecook-dev/truffle/actions/workflows/ci.yml/badge.svg)](https://github.com/vibecook-dev/truffle/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/vibecook-dev/truffle?label=latest)](https://github.com/vibecook-dev/truffle/releases/latest)
[![npm](https://img.shields.io/npm/v/@vibecook/truffle?label=%40vibecook%2Ftruffle)](https://www.npmjs.com/package/@vibecook/truffle)
[![MIT License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

**Simple network programming on top of Tailscale.**

Truffle gives your apps familiar networking APIs — TCP, HTTP, UDP, WebSocket, QUIC — that run across devices on the same tailnet. Discover peers, open sockets, send messages, sync state, transfer files, and publish services. No central server. No new protocol to learn.

```ts
import { createMeshNode } from '@vibecook/truffle';

const mesh = await createMeshNode({
  appId: 'field-tools',
  deviceName: 'alice-laptop',
});

const bob = await mesh.peer('bob-laptop', { waitMs: 5000 });
if (bob) await bob.sendJson('chat', { text: 'hello' });

// Node-shaped transports over the private mesh
const server = mesh.net.createServer((socket) => socket.pipe(socket));
server.listen(7000);
```

## What you get

| Layer | Surface |
| --- | --- |
| **Transports** | `mesh.net` · `mesh.http` · `mesh.dgram` · `mesh.ws` · `mesh.quic` |
| **Peers** | Interned `Peer` handles — no id-string soup for dial/send |
| **App APIs** | Namespaced messaging, SHA-256 file transfer, device-owned synced stores |
| **Publish** | `mesh.serve` and `mesh.http.createServer` for HTTP on the tailnet |
| **CLI / TUI** | Interactive mesh view, `truffle cp`, JSONL for agents |

Tailscale owns tunnels, identity, ACLs, and crypto. Truffle owns discovery-aware peers, app isolation (`appId`), and the APIs your code calls.

## Install

> Requires [Tailscale](https://tailscale.com/) installed and authenticated on the devices you want to connect.

### JavaScript / Node.js

```bash
pnpm add @vibecook/truffle
# optional — only if you use mesh.ws
pnpm add ws
```

Node.js 18+. The package includes a prebuilt native addon and platform sidecar.

### CLI

```bash
# macOS / Linux
curl -fsSL https://vibecook-dev.github.io/truffle/install.sh | sh

# Windows (PowerShell)
iwr -useb https://vibecook-dev.github.io/truffle/install.ps1 | iex

# Homebrew
brew install jamesyong-42/tap/truffle
```

Supports macOS (arm64/x64), Linux (x64/arm64), and Windows (x64).

## Quick start

### API

```ts
import { createMeshNode } from '@vibecook/truffle';

const mesh = await createMeshNode({
  appId: 'notes',
  deviceName: 'alice-laptop',
  onAuthRequired: (url) => console.log('Auth URL:', url),
});

// Discovery yields Peer handles (same object from events / messages)
for (const p of await mesh.getPeers()) {
  console.log(p.displayName, p.online ? 'online' : 'offline');
}

mesh.onMessage('chat', async (msg) => {
  if (typeof msg.from === 'string') return;
  console.log(msg.from.displayName, msg.payload);
  await msg.from.sendJson('chat', { text: 'ack' });
});

// Serve a local process or SPA to the whole tailnet
const h = await mesh.serve({
  port: 443,
  target: 'http://localhost:3000',
});
console.log(h.url); // https://….ts.net
```

### CLI

```bash
truffle                           # interactive TUI
truffle up                        # start the background daemon
truffle ls                        # list peers
truffle send server "hello"       # send a message
truffle cp report.pdf server:/tmp/  # send a file
truffle serve http://localhost:3000 --port 443  # publish a local service
truffle watch --json              # stream mesh events as JSONL
```

## Mental model

1. **Start a node** — `createMeshNode({ appId })` boots the native core + tsnet sidecar in-process.
2. **Discover peers** — Tailscale status is the source of truth; you get interned `Peer` objects.
3. **Talk on purpose** — messages, sockets, files, and HTTP are addressed with a Peer (or a query that resolves to one).
4. **Ship features** — chat, agents, shared state, local dashboards — without operating a broker.

`appId` is the isolation boundary: devices with different app IDs do not see each other as Truffle peers, even on the same tailnet.

## Packages

| Package | Role |
| --- | --- |
| [`@vibecook/truffle`](packages/core) | Primary JS API (`createMeshNode`, transports, files, stores, serve) |
| [`@vibecook/truffle-react`](packages/react) | React hooks (`useMesh`, `useAuth`, `useSyncedStore`) |
| [`@vibecook/truffle-native`](crates/truffle-napi) | NAPI-RS native addon |
| [`truffle` CLI](crates/truffle-cli) | Interactive CLI + TUI (`truffle serve`, `cp`, agents JSONL, …) |
| [`truffle-core`](crates/truffle-core) | Rust core (network → transport → session → envelope → Node) |
| [`truffle-tauri-plugin`](crates/truffle-tauri-plugin) | Tauri v2 desktop plugin |
| [`sidecar-slim`](packages/sidecar-slim) | Go sidecar embedding tsnet |
| [Truffle Swift](apple/) | Apple-native SPM package (RFC 024) — wire-compatible mesh core + SwiftUI helpers |

Messaging prefers explicit wire types: `peer.sendJson` / `peer.sendBytes` (and `mesh.broadcastJson` / `broadcastBytes`) over the legacy content-sniffing `send` / `broadcast`.

## Examples

See [`examples/`](examples/) for runnable demos:

- Discovery, chat, shared state
- Netcat-style TCP, QUIC streams
- Express over the mesh, static SPA + API, expose a local dev server
- WebSocket chat over mesh

Swift: [`apple/Examples/MeshChatDemo`](apple/Examples/MeshChatDemo) — iOS SwiftUI chat over an in-process demo mesh (loopback backend today; live TailscaleKit backend still pending).

## Docs

- **Guide:** [vibecook-dev.github.io/truffle](https://vibecook-dev.github.io/truffle/)
- **API reference:** [api.html](https://vibecook-dev.github.io/truffle/api.html)
- **HTTP serving guide:** [docs/guide/serving-http.md](docs/guide/serving-http.md)
- **API stability:** [docs/API-STABILITY.md](docs/API-STABILITY.md)
- **Testing:** [docs/TESTING.md](docs/TESTING.md)
- **Releasing:** [docs/RELEASING.md](docs/RELEASING.md)
- **RFCs:** [docs/rfcs/](docs/rfcs/) · Swift: [RFC 024](docs/rfcs/024-truffle-swift.md)

## Development

```bash
pnpm install
cargo build --locked --workspace --exclude truffle-tauri-plugin --exclude truffle-napi
cargo test --locked --workspace --exclude truffle-tauri-plugin --exclude truffle-napi
pnpm run build
pnpm run test
```

Swift package (macOS / Xcode toolchain):

```bash
cd apple && DEVELOPER_DIR=/Applications/Xcode.app/Contents/Developer swift test
```

Enable the pre-commit hook once per clone:

```bash
git config core.hooksPath .githooks
```

## License

[MIT](LICENSE)
