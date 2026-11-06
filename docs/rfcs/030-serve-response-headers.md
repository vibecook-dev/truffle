# RFC 030: Response headers per serve — the serve's policy rides every response

**Status**: Implemented (sidecar protocol 13, core, NAPI, `mesh.serve`, the Tauri plugin); ships in the release after 0.7.26
**Author**: James Yong + Claude
**Date**: 2026-11-06
**Depends on**: RFC 023 (HTTP serving — the engine this extends)
**Origin**: VibeField petition T-1 (the Drive, DR-D11: a served page is confined by its serve's headers, never by a rewrite of the page)

---

## 1. Problem Statement

A serve (RFC 023) publishes a folder or a loopback service, but it has no say in how a browser
treats what it serves. It cannot send a `Content-Security-Policy`, a `frame-ancestors` or
`X-Frame-Options` rule, a `Referrer-Policy` or a `Permissions-Policy`.

An app that serves content it did not write has two workarounds today, and both are poor:

1. **Rewrite every file**, for example by injecting a `<meta http-equiv="Content-Security-Policy">`.
   This changes the bytes, and with them any digest the content is stored under. A `<meta>` policy
   also cannot express everything: browsers ignore `frame-ancestors`, `report-uri` and `sandbox` in
   a `<meta>` CSP.
2. **Put its own proxy in front of the serve.** That duplicates the engine, and its TLS, identity
   headers and allow-lists, in every consumer.

The policy belongs to the serve, not to the content. Changing the policy should be a serve change,
and it should leave every stored object as it is.

## 2. Decision

- `proxy:add` carries `headers`, a list of `{name, value}`, on the serve and on each route.
- A route's header replaces the serve's header of the same name. Serve headers the route does not
  set still apply to it.
- The sidecar sets the resulting list on **every HTTP response** of that serve or route: static
  files, proxied responses and the engine's own errors. Each header replaces the value a proxied
  backend sent for the same name.
- The core and the sidecar both check the list against one table, which both test suites read
  (§3.4).
- This is sidecar protocol 13. The core refuses headers with `NetworkError::Unsupported` when the
  sidecar speaks an older protocol (§3.5).

## 3. Semantics (normative)

### 3.1 Shape

| Plane | Serve level | Route level |
| --- | --- | --- |
| Rust | `ProxyConfig.headers: Vec<ProxyHeader>` | `ProxyRoute.headers: Vec<ProxyHeader>` |
| Wire (`proxy:add`) | `"headers": [{"name": …, "value": …}]` | `routes[i].headers`, same shape |
| NAPI | `NapiProxyConfig.headers?: NapiProxyHeader[]` | `NapiProxyRoute.headers?: NapiProxyHeader[]` |
| `mesh.serve` | `ServeOptions.headers?: Record<string, string>` | `ServeRouteValue.headers?: Record<string, string>` |
| Tauri (`proxyAdd`) | `ProxyConfig.headers?: ProxyHeader[]` | `ProxyRoute.headers?: ProxyHeader[]` |

`ProxyHeader { name: String, value: String }` lives in `truffle_core::network` and is re-exported
from `truffle_core::proxy`. An empty list is the default and sets nothing. The wire omits the key
when a list is empty. `mesh.serve` sends a record's entries in insertion order.

### 3.2 The merge rule

A route's **effective list** is the serve's list with each of the route's headers replacing the
serve header of the same name. Names are compared ASCII case-insensitively. The **serve's own
list** is the effective list of a v1 (single-target) serve, and of the engine's 404 for a path no
route matches.

A route cannot remove a serve header. An empty value is refused, so it cannot unset one either
(Q2).

### 3.3 Which responses carry them

| Response | List | How |
| --- | --- | --- |
| Static file: 200, 206, SPA fallback, HEAD, 304; and the static server's own 404 (miss, dotfile, denied path), 405 and 416 | the route's effective list | set on the writer before the file server runs |
| Proxied response, any final status, including a 101 that `ReverseProxy` relays itself | the route's effective list | set in `ModifyResponse` on the backend's response, replacing the backend's values |
| The engine's 502 for a dead or failing backend | the route's effective list | set in the `ErrorHandler` before the existing 502 |
| The WebSocket path's own 502 (dial failed) and 500 (no hijack) | the route's effective list | set before `handleProxyWebSocket` runs |
| The allow-list gate's bare 403 | the route's effective list | set before the 403 |
| The engine's 404 for a path no route matches | the serve's own list | set before the 404 |

**Not carried:**

- A relayed `1xx` response, such as `103 Early Hints`. It goes out as the backend sent it.
- A hijacked WebSocket upgrade. The tunnel is a byte relay, so its `101` and everything after it
  are the backend's own.
- Trailers. They are not headers, and browsers apply no policy from them.

**Why `ModifyResponse`, and not the writer, for proxied routes.** Go's `httputil.ReverseProxy` adds
the backend's headers to the writer's map, so a header set on the writer beforehand would be sent
twice. Two CSPs intersect, and two `X-Frame-Options` values are invalid. After relaying a `1xx`,
`ReverseProxy` also clears the writer's map, so a header set beforehand would be missing from the
final response. `ReverseProxy` removes hop-by-hop headers, including any names the backend lists in
its own `Connection` header, *before* `ModifyResponse` runs. A backend therefore cannot strip the
policy that way. (This was read in Go 1.25.6's `net/http/httputil/reverseproxy.go`. The sidecar
builds with Go 1.27.3, and its tests pin the observable behaviour.)

**What the engine itself rewrites:**

- Go's `http.Error` sets `Content-Type: text/plain; charset=utf-8` and `X-Content-Type-Options:
  nosniff` on the engine's own 403, 404, 405 and 502. A configured `X-Content-Type-Options` is
  replaced there. (`Content-Type` is refused, §3.4.)
- `http.ServeContent`'s error path, such as a 416, drops a configured `Cache-Control`. The other
  names `ServeContent` drops or rewrites (`Content-Type`, `Content-Length`, `Content-Encoding`,
  `ETag`, `Last-Modified`) are refused, §3.4.

### 3.4 Validation

The core (`validate_config`, so every binding) and the sidecar (`validateResponseHeaders`, because
it must not trust the wire) apply the same rules in the same order, with the same message for each
fault. The sidecar refuses the add with `proxy:error` code `INVALID_HEADER`. The core refuses it
with `NodeError::ConnectionFailed`, as it does every other `validate_config` refusal. A message
names the list (`serve headers` or `route "/api" headers`) and the header. It never echoes a value,
or a name longer than the bound.

Each list is checked on its own first:

1. At most 32 headers.
2. For each header:
   - The name is at most 64 bytes, and is an RFC 7230 `token`: one or more of
     ``! # $ % & ' * + - . ^ _ ` | ~``, digits and ASCII letters.
   - The name is not refused (below).
   - The value is not empty, and is at most 8192 bytes.
   - Every byte of the value is visible ASCII (`VCHAR`), a space or a horizontal tab. No CR, LF,
     NUL, other control bytes, DEL or non-ASCII (`obs-text`).
   - The value has no leading or trailing space or tab.
3. No name appears twice in one list (case-insensitive).

Then each list a response can carry is checked: the serve's own, and each route's effective list.
Each must have at most **32 headers** and at most **16 KiB** (16384 bytes, each name's bytes plus
its value's). A route that replaces a serve name is counted and sized once.

**Refused names** (case-insensitive):

- `Connection`, `Keep-Alive`, `TE`, `Trailer`, `Transfer-Encoding`, `Upgrade` and every `Proxy-*`
  name. These are hop-by-hop (RFC 7230 §6.1, and `Proxy-Connection`): they describe one
  connection, not the response.
- `Content-Length`: the engine frames each body.
- `Content-Type`, `Content-Encoding`, `Content-Range`, `ETag` and `Last-Modified`: each describes
  one body. The static server sets the list on the writer before `http.ServeContent` runs, and
  `ServeContent` keeps a `Content-Type` already set, matches a request's `If-None-Match` against
  the `ETag` already set, and sends no `Content-Length` when a `Content-Encoding` is set. One
  serve-wide value would give every file the same MIME type (with `nosniff`, browsers then refuse
  its scripts and styles), answer a 304 to every client holding the configured `ETag` after a
  redeploy, or mislabel the bytes. On a proxied route it would replace the backend's own
  description of its body.
- `Host`: a request header, and the engine's.
- `Set-Cookie`: a serve is not a session. Every caller of the serve would get the same cookie.

**Bounds** (`MAX_RESPONSE_HEADERS` = 32, `MAX_RESPONSE_HEADER_NAME_BYTES` = 64,
`MAX_RESPONSE_HEADER_VALUE_BYTES` = 8192, `MAX_RESPONSE_HEADERS_BYTES` = 16384, as `pub` consts in
`truffle_core::proxy` and the sidecar's `maxResponseHeader*`). All are counted in bytes, and all
bound one list. They fix the cost each response carries, and cap the names and values one list adds
to a `proxy:add` at 16 KiB. They do not bound the `proxy:add` itself: nothing bounds the number of
routes (RFC 023 bounds neither routes nor `allow` globs), so a serve with some hundreds of routes,
each carrying a full list, can pass the sidecar's 8 MiB command line. The sidecar then skips the
line with an untagged `COMMAND_TOO_LARGE`, and the core reports `proxy add timed out` after 10
seconds rather than a validation refusal. That limit, and its untagged error, predate this RFC. A
consumer that composes a policy, such as a CSP naming many hosts, must bound it to fit: 16 hosts of
253 bytes in each of six directives do not fit in 8192 bytes.

**The shared table.** `crates/truffle-core/src/proxy/response_headers_vectors.json` holds the
bounds, the refused names (`proxy-*` for the prefix) and 74 cases. The Rust tests
(`proxy/mod.rs`) and the Go tests (`response_headers_v13_test.go`) both run every case. Both also
assert that their constants and their refused list equal the file, so neither side can change a
rule alone. Each case has the following fields:

- `serve`: the serve's headers.
- `route`: when present, the headers of one `{prefix: "/", dir: "/srv"}` route.
- `serveFill` / `routeFill`: `{prefix, count}`, which appends `prefix1 … prefixN`, each with the
  value `v`.
- `repeat`: on a header, repeats its value unit N times.
- `ok`: whether the case is accepted.
- `error`: for a refused case, a substring both implementations' messages contain.

A consumer that keeps its own validator, such as VibeField's contracts, should copy this file and
hold its own rules to be at least as strict.

### 3.5 Version gate

`RESPONSE_HEADERS_PROTOCOL_VERSION` = 13. When the serve or any route has headers and the sidecar
speaks below 13, `proxy_add` returns, before anything is sent:

```
NetworkError::Unsupported("sidecar protocol v12 cannot set response headers (headers need v13); upgrade the sidecar binary")
```

`Proxy::add` surfaces this as `NodeError::Network(NetworkError::Unsupported(_))`. The sidecar reads
`proxy:add` with a plain `json.Unmarshal`, so an older sidecar would silently drop the field and
serve without the policy. A refusal is the only honest answer.

## 4. Wire changes

`proxy:add`, protocol 13:

```jsonc
{
  "id": "app", "name": "app", "listenPort": 8443, "tls": false,
  "headers": [                                   // NEW: the serve's list
    { "name": "Content-Security-Policy", "value": "default-src 'self'; frame-ancestors 'none'" },
    { "name": "Referrer-Policy", "value": "no-referrer" }
  ],
  "routes": [
    { "prefix": "/", "dir": "/abs/made/app/1f2e…", "fallback": "/index.html",
      "headers": [ { "name": "X-Frame-Options", "value": "DENY" } ] },   // NEW: per route
    { "prefix": "/api", "targetUrl": "http://localhost:8000" }
  ]
}
```

Add-time refusal: `proxy:error {"id": "app", "code": "INVALID_HEADER", "message": "route \"/\"
headers: header \"Set-Cookie\" is hop-by-hop or set by the engine and cannot be configured"}`.

Status events advertise `"protocolVersion": 13`.

## 5. Bindings

- **Rust**: `ProxyConfig.headers`, `ProxyRoute.headers`, `ProxyAddParams.headers`. These are new
  pub fields on types with no `Default` and no `#[non_exhaustive]`, so struct literals must add
  them. This follows the 0.7.20 precedent (`accept_app_capabilities`).
- **NAPI**: `NapiProxyHeader`, plus `headers` on `NapiProxyConfig` and `NapiProxyRoute`.
- **`mesh.serve`**: `headers` records on the options and on route objects. They survive in
  `handle.config`, so a replay reproduces them.
- **CLI daemon**: `proxy_add` reads `headers` on the params and on each route. There is no
  `truffle serve --header` flag yet (Q5).
- **Tauri plugin**: `headers` on `ProxyConfigJs` and `ProxyRouteJs` (guest-js `ProxyHeader[]` on
  `ProxyConfig` and `ProxyRoute`), mapped through to core unchanged. A plugin that took the key
  and dropped it would serve without the policy and report nothing, which §3.5 exists to prevent.
- **Swift**: the Swift package has no serve surface, so nothing changes.

## 6. Security model

1. **The serve owner's policy wins.** Each header replaces the backend's value for its name, so a
   page served through a confined serve cannot loosen its own policy from behind the proxy. The
   cost: a backend's own stricter CSP is replaced, not intersected (Q3).
2. **No response splitting.** Names are tokens, and values hold no CR, LF, NUL or other control
   bytes. Both sides check, because the sidecar must not trust the wire.
3. **No shared session.** `Set-Cookie` is refused (§3.4).
4. **Bounded cost.** The bounds cap the bytes each response carries and the size of each list
   (§3.4). A serve's total grows with its route count, which nothing bounds.
5. **No leaks in errors.** A refusal never echoes a value: a value may carry a nonce or a
   reporting endpoint.
6. **Not a gate.** Headers instruct the browser. Who may reach the serve is still the tailnet's ACLs
   and the serve's `allow` (RFC 023 §9.7).

## 7. Compatibility

| Core | Sidecar | Result |
| --- | --- | --- |
| old | new (13) | Unchanged. An old core never sends `headers`, and a missing field reads as none. |
| new | new (13) | Headers applied. |
| new, no headers | old | Unchanged: the gate passes and the wire omits the keys. |
| new, headers | old (< 13) | Refused with `Unsupported` before sending (§3.5). |

A consumer adopting this moves the core and the sidecar in one step. The Rust struct literals gain
the field.

## 8. Testing

- **Rust** (`cargo test -p truffle-core`):
  - `proxy::tests`: every vector case, plus the bounds and the refused names against the file;
    errors naming their list; no value echoed; a header-less route still checked through the
    serve's list; and the `ProxyAddParams` that `Proxy::add` hands the provider carry the serve's
    and each route's list unchanged (`proxy_add_params`).
  - `protocol::tests`: the v1 shape has no `headers` key; serve and route lists serialize as
    `[{name, value}]` in order; a route without headers omits the key; the field is read back.
  - `provider::diagnostic_tests::proxy_response_headers_require_protocol_v13`: serve-level and
    route-level headers are refused at v12 with both versions named. No headers at v12, and headers
    at v13, pass the gate.
  - `provider::diagnostic_tests::proxy_add_command_carries_the_serve_and_route_headers`: the
    `proxy:add` payload built from those params (`proxy_add_command`) carries both lists unchanged.
  - Not unit-tested: the NAPI mapping (`truffle-napi` is a `cdylib` with no Rust tests). The
    TypeScript tests pin the NAPI shapes it receives.
- **Go** (`go test ./...` in `packages/sidecar-slim`):
  - `response_headers_v13_test.go`: the vector table and its bounds and refused names; the wire
    shape; the merge and replace rules.
  - An end-to-end run through `startProxyAdd` on a loopback listener:
    - static 200, SPA fallback, HEAD, 304, dotfile 404 and 405;
    - a proxied 200 with exactly one CSP and the route's `X-Frame-Options` replacing the backend's;
    - the same after a relayed 103;
    - the dead-backend 502 and the WebSocket-dial 502;
    - the gate 403;
    - the no-route 404 with the serve's list only.
  - Add-time refusals that never open the static root and never install a placeholder.
  - `main_test.go` pins protocol 13.
- **TypeScript** (`packages/core/test/serve.test.cjs`): the records map to the NAPI lists for every
  shape, absence stays absent, shape errors throw, and `serve()` and replay carry them.
- **Tauri plugin** (`cargo test -p truffle-tauri-plugin`, `proxy_route_mapping_tests`): a
  frontend config's serve and route `headers` reach core's `ProxyConfig` in order; absent ones map
  to an empty list.

## 9. Consumers

VibeField's made-app serve (DR-1, the Drive) sets the CSP, frame and referrer policy of every
made app on its serve. The stored page is never rewritten, so a policy change is a serve change.
VibeField copies `response_headers_vectors.json` into its contract vectors, and its own rules must
be at least as strict as these.

## 10. Open questions

- **Q1** (closed for 13, by the review of 2026-11-06). Should `Content-Type`, `Content-Encoding`,
  `Content-Range`, `ETag` and `Last-Modified` also be refused? Yes: each describes one body (§3.4),
  and protocol 13 refuses them (D6). A later protocol may loosen the list, and no caller breaks.
  Tightening it after a release would refuse serves that work today, so it is decided before the
  first release. The maintainer confirms it at review.
- **Q2.** Should a route be able to unset a serve header? In 13 it cannot: values are non-empty.
- **Q3.** Should proxied responses append a policy (so a backend's stricter CSP still intersects)
  rather than replace it? 13 replaces, as one rule for every name.
- **Q4.** The bounds (32, 64, 8192 and 16384 bytes) are first numbers, sized for policy headers. They
  are for the maintainer to confirm.
- **Q5.** Expose headers in `truffle serve --header 'Name: value'`? (The Tauri plugin's guest API
  carries them since the review of 2026-11-06, §5.)

## 11. Decisions

- **D1** headers live on the serve and on routes; a route's header replaces the serve's of the same
  name (§3.2).
- **D2** every response of the serve or route carries them, including the engine's own errors.
  Relayed `1xx` responses and the WebSocket tunnel do not (§3.3).
- **D3** each header replaces the backend's value for its name (§3.3, §6).
- **D4** proxied routes apply them in `ModifyResponse` and the error handler; static files and the
  engine's own responses apply them on the writer (§3.3).
- **D5** one validation table with two implementations, held together by a shared vector file
  (§3.4). The Go token check is written by hand to mirror the Rust one, not taken from
  `httpguts`, so the file is the only reference.
- **D6** hop-by-hop names, `Set-Cookie`, `Host` and the names that describe one body
  (`Content-Length`, `Content-Type`, `Content-Encoding`, `Content-Range`, `ETag`,
  `Last-Modified`) are refused (§3.4, Q1).
- **D7** bounds in bytes: 32 headers, 64-byte names, 8192-byte values and 16 KiB per effective list
  (§3.4).
- **D8** protocol 13. The core refuses with the typed `NetworkError::Unsupported`, as egress (v7)
  and logout (v11) do (§3.5). The older capability gate (v9 `acceptAppCaps`) still answers
  `ProxyError`.
