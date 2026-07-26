# @vibecook/truffle

The primary JavaScript API for Truffle: simple network programming over a
Tailscale mesh.

```bash
pnpm add @vibecook/truffle
```

```ts
import { createMeshNode } from '@vibecook/truffle';

const mesh = await createMeshNode({
  appId: 'notes',
  deviceName: 'alice-laptop',
});

for (const peer of await mesh.getPeers()) {
  console.log(peer.displayName, peer.online);
}
```

Requires Node.js 18 or newer and Tailscale authentication. Documentation,
examples, platform support, and the security policy are in the
[Truffle repository](https://github.com/vibecook-dev/truffle).
