# RFC 033 · Public serves and live routes

## Live route replacement

Protocol 14 adds `proxy:update`. Its payload carries the proxy ID, request ID,
the entire routes array and optional serve-level allow, headers and accepted
app capabilities. Missing policy lists default to empty. A successful update
answers `proxy:updated` with the same IDs. A failure answers `proxy:error` and
leaves the listener, its state and its old table intact.

A request uses one immutable table. The sidecar keeps routes with the same
effective backend, path, headers, allow list and capability list, including
their connection pools and directory handles. Replacing a route cancels its
proxied requests, closes its hijacked sockets and drains active file sends.
The listener, TLS mode, port and loopback restriction are fixed at add.
Updates are applied in wire order. Removing a serve first makes subsequent
updates fail with `NOT_FOUND`.

An explicitly empty table returns 404 on every path. On add, a nonzero
single-target port retains the legacy meaning. Both sides bound a table to
1,024 routes. The core rejects a serialized command that would exceed the
sidecar’s 8 MiB line limit before writing it.

## Public serving

Protocol 15 adds `funnel:status` and `funnel:statusResult` correlated by
`requestId`. The sidecar publishes `funnel:changed` when the status differs
after a self-change on the IPN bus. The status carries a state, the granted
ports, and any enable URL and instructions supplied by control. An unavailable
state includes a reason. Subscribe before reading status to cover an approval
that arrives during the read.

`proxy:add` with `funnel: true` uses a Funnel-only TLS listener on 443, 8443 or
10000. Private and public listeners can share a port using separate IDs.
Public serves refuse login gates, capability forwarding and non-loopback
backends. These rules also apply when a route table changes.

Public visitors are anonymous. The engine strips incoming Tailscale identity
headers, sets `Tailscale-Funnel-Request: ?1` and forwards the visitor address
from the accepted Funnel connection as the sole `X-Forwarded-For` value.
HTTP/2 and HTTP/1.1 are offered. Each public listener allows 256 connections
and configures a 64 KiB header limit. Closing the listener revokes its public
mark. The sidecar clears stale marks for its own node on startup.

The real-tailnet test exercises simultaneous public and private port 443,
public DNS, TLS, HTTP/2, stream continuity, revocation and listener removal.
