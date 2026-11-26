# quic-streams

Multiplexed QUIC streams over the mesh: one `mesh.quic` connection
([RFC 021](../../docs/rfcs/021-raw-transport-js-api.md) §8) carrying several
independent bidirectional byte streams at once. Opening N plain TCP
connections to get the same concurrency costs N handshakes and N separate
flow-control windows, and congestion on any one of them is purely that
connection's problem to recover from. QUIC instead does a single handshake
for the whole connection, gives each stream its own flow control, and
guarantees no head-of-line blocking — a slow or lossy stream never stalls
its siblings, unlike TCP where one lost segment blocks every byte queued
behind it on that connection. This example proves it by opening several
streams at once and timing them individually: the total wall time tracks
the slowest stream, not the sum of all of them.

## Prerequisites

- Tailscale installed and running on both devices.
- Both devices on the same tailnet, running this example with the **same
  `appId`** (`quic-demo`, hardcoded in `src/main.ts`) — `appId` scopes peer
  visibility, so two different app ids can't see each other as peers.
- `pnpm install` from the repo root (links `@vibecook/truffle` as a
  workspace dependency).

## Usage

Run from the repo root, on each device:

```bash
# Device A — list peers to find B's name
pnpm --filter @vibecook/example-quic-streams exec tsx src/main.ts peers

# Device A — serve (echoes every stream it receives)
pnpm --filter @vibecook/example-quic-streams exec tsx src/main.ts serve 9420

# Device B — bench (opens 4 concurrent streams by default)
pnpm --filter @vibecook/example-quic-streams exec tsx src/main.ts bench device-a-name 9420
```

`bench` prints a per-stream timing line plus a total, e.g.:

```
stream 0: 41.32 ms (32768 bytes, echo verified)
stream 1: 40.98 ms (32768 bytes, echo verified)
stream 2: 42.61 ms (32768 bytes, echo verified)
stream 3: 41.05 ms (32768 bytes, echo verified)
total: 43.87 ms wall time for 4 streams (sum of individual times: 165.96 ms) — wall time tracks the slowest stream, not the sum, because they ran concurrently over one connection.
```

Pass a different stream count as a third argument: `bench device-a-name 9420 16`.

### Commands

| Command | Description |
|---|---|
| `serve <port>` | Start a mesh node and echo every stream on every connection it accepts. Runs until Ctrl+C. |
| `bench <peer> <port> [streams=4]` | Connect to `<peer>` on `<port>`, open `streams` concurrent QUIC streams, write a distinct 32 KiB payload on each, verify the echo byte-for-byte, and print timings. |
| `peers` | List known peers (`deviceName`, `deviceId`, `ip`, `online`) and exit. |

All diagnostics (connection status, errors) print to **stderr**; timing
results print to **stdout**.

Run `serve 0` to let the tailnet assign a port. Use the port printed by the
server for `bench <peer> <port>`. You can also choose a port such as 443 or
9420. Only the configured session WebSocket port (default 9417) is reserved.

## Caveats

- Needs two real devices on the same tailnet — this can't be exercised
  against `localhost` (mesh peers are other tailnet devices, not the local
  node itself).
- `serve` accepts connections until interrupted; run `bench` again for
  another round, even against the same running `serve` process.
- If a stream's echo doesn't match byte-for-byte, `bench` exits non-zero
  with a message naming the stream index and the byte-length mismatch.
