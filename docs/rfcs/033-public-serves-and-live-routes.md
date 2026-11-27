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
