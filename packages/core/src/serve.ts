// Declarative HTTP serving over the tailnet (RFC 023 §6.2, Phase 3).
//
// `mesh.serve(config)` is the one ergonomic verb for publishing a local
// service or a static directory to the whole tailnet, mirroring the
// `tailscale serve` mental model. It is sugar over the shipped proxy engine
// (`mesh.proxy()` / Rust `Node::proxy()`): a serve handle maps 1:1 to a proxy
// `id`, and every config shape below normalizes to one `NapiProxyConfig`.
//
// The docs rule (RFC 023 §5, D2): you wrote the request handler →
// `mesh.http.createServer`; it's a directory or another process → `serve`.
//
// Three config shapes, one function:
//   • { port, target }                     — forward a whole port to a backend
//   • { port, dir, fallback? }             — serve a static directory (SPA)
//   • { port, routes }                     — mixed path routes (SPA + APIs)
//
// TLS defaults ON here (unlike `createServer`): the audience is browsers, and
// this preserves the shipped engine's always-TLS behavior (RFC 023 D5).

import { EventEmitter } from 'node:events';
import { resolve as resolvePath } from 'node:path';
import type {
  NapiProxy,
  NapiProxyConfig,
  NapiProxyEvent,
  NapiProxyHeader,
  NapiProxyRoute,
  NapiSubscription,
} from '@vibecook/truffle-native';

/** Options common to every {@link ServeConfig} shape. */
export interface ServeOptions {
  /**
   * Terminate TLS in the sidecar with automatic MagicDNS certificates.
   * Default **true** — the serve audience is browsers (padlock, secure
   * context, `wss:`), and it preserves the shipped engine behavior. `false`
   * serves plain HTTP (WireGuard still encrypts the tailnet hop). Requires
   * MagicDNS + HTTPS enabled on the tailnet; the cert matches only the full
   * `.ts.net` FQDN — use {@link ServeHandle.url} / `mesh.dnsName` in links.
   */
  tls?: boolean;
  /** Human-readable label. Defaults to {@link id}. */
  name?: string;
  /**
   * Stable proxy id, unique per node. Defaults to `serve-${port}`, so two
   * serves on the same port collide unless you set distinct ids.
   */
  id?: string;
  /**
   * loginName allow globs, e.g. `["*@corp.com"]`. Absent = the whole tailnet.
   * A non-matching caller gets a bare 403. Per-route `allow` overrides this
   * (RFC 023 §9.7).
   */
  allow?: string[];
  /**
   * Permit non-loopback targets (default false = deny). A LAN target turns
   * this node into a pivot into its network, so it is opt-in (RFC 023 §9.3).
   */
  allowNonLoopback?: boolean;
  /**
   * Response headers set on every response of this serve (RFC 030): static
   * files, proxied responses and the engine's own errors alike, e.g.
   * `{ 'Content-Security-Policy': "default-src 'self'" }`. Each replaces the
   * value a proxied backend sent for the same name; a route's own `headers`
   * replace these by name (case-insensitive). Hop-by-hop names, `Set-Cookie`,
   * `Host` and the names that describe one body (`Content-Length`,
   * `Content-Type`, `Content-Encoding`, `Content-Range`, `ETag`,
   * `Last-Modified`) are refused. Needs sidecar protocol 13
   * (truffle ≥ 0.7.27); an older sidecar refuses the serve rather than
   * serving without them.
   */
  headers?: Record<string, string>;
}

/** Forward a whole tailnet port to one local backend. */
export interface ServeTargetConfig extends ServeOptions {
  /** Tailnet port to listen on (e.g. 443). */
  port: number;
  /** Backend URL, e.g. `http://localhost:3000`. Must be http(s). */
  target: string;
}

/** Serve a static directory (single-page-app shape). */
export interface ServeStaticConfig extends ServeOptions {
  /** Tailnet port to listen on. */
  port: number;
  /** Directory to serve; resolved to an absolute path. */
  dir: string;
  /** Rewrite static misses to this path (SPA fallback), e.g. `/index.html`. */
  fallback?: string;
}

/** One entry of a {@link ServeRoutesConfig} `routes` map. */
export interface ServeRouteValue {
  /** Backend URL for a proxy route. Mutually exclusive with {@link dir}. */
  target?: string;
  /** Directory for a static route. Mutually exclusive with {@link target}. */
  dir?: string;
  /** SPA fallback path; only meaningful with {@link dir}. */
  fallback?: string;
  /** Strip the matched prefix before proxying; only meaningful with {@link target} (default false). */
  stripPrefix?: boolean;
  /** Per-route loginName globs; overrides the config-level {@link ServeOptions.allow}. */
  allow?: string[];
  /**
   * Response headers for this route (RFC 030). Each replaces the config-level
   * {@link ServeOptions.headers} entry of the same name; config-level names
   * this route does not set still apply.
   */
  headers?: Record<string, string>;
}

/**
 * Mixed path routes. Keys are path prefixes matched by longest prefix
 * (order-independent); values are a backend URL string, or a
 * {@link ServeRouteValue} for static/stripPrefix/allow control.
 */
export interface ServeRoutesConfig extends ServeOptions {
  /** Tailnet port to listen on. */
  port: number;
  /** Prefix → backend URL string or {@link ServeRouteValue}. */
  routes: Record<string, string | ServeRouteValue>;
}

/** The union accepted by {@link TruffleServe}. */
export type ServeConfig = ServeTargetConfig | ServeStaticConfig | ServeRoutesConfig;

/** Runtime error surfaced on a {@link ServeHandle} (G5 fix, RFC 023 §7). */
export interface ServeErrorInfo {
  /** Sidecar error code, e.g. `SERVE_ERROR`, `CONNECTION_REFUSED`. */
  code: string;
  /** Human-readable detail. */
  message: string;
}

/** Broad reader over the {@link ServeConfig} union for normalization. */
interface ServeConfigFields extends ServeOptions {
  port: number;
  target?: string;
  dir?: string;
  fallback?: string;
  routes?: Record<string, string | ServeRouteValue>;
  funnel?: unknown;
}

/**
 * A running serve, 1:1 with a proxy `id`. An `EventEmitter` for runtime
 * errors:
 *
 * - `'serveError'` — `(info: ServeErrorInfo)`. Always emitted; safe to leave
 *   unlistened. Prefer this for programmatic handling.
 * - `'error'` — `(info: ServeErrorInfo)`. Emitted **only when a listener is
 *   attached** (an unlistened EventEmitter `'error'` throws and would crash
 *   the process). Same payload as `'serveError'`.
 * - `'close'` — the proxy stopped (via {@link close} or a sidecar-side stop).
 *   Fires once.
 */
export class ServeHandle extends EventEmitter {
  /** The proxy id backing this serve. */
  readonly id: string;
  /**
   * The normalized, frozen config that created this handle — every default
   * filled and every `dir` resolved to an absolute path. Persist it and
   * replay `mesh.serve(handle.config)` to recreate an identical serve
   * (RFC 023 D16); the library never persists on its own.
   */
  readonly config: ServeConfig;

  #url = '';
  #port = 0;
  #proxy: NapiProxy;
  #retire: () => void;
  #closed = false;
  #closeEmitted = false;

  /** @internal Constructed by {@link createServeNamespace}. */
  constructor(id: string, config: ServeConfig, proxy: NapiProxy, retire: () => void) {
    super();
    this.id = id;
    this.config = config;
    this.#proxy = proxy;
    this.#retire = retire;
  }

  /** Public serving URL from the `add()` result (`https://name.tailnet.ts.net[:port]`). */
  get url(): string {
    return this.#url;
  }

  /** Tailnet port the serve is listening on (resolved from the `add()` result). */
  get port(): number {
    return this.#port;
  }

  /** @internal Fill url/port from the `NapiProxyInfo` once `add()` resolves. */
  _start(url: string, port: number): void {
    this.#url = url;
    this.#port = port;
  }

  /** @internal Route a `'error'` proxy event to this handle. */
  _onError(code: string | undefined, message: string | undefined): void {
    const info: ServeErrorInfo = { code: code ?? '', message: message ?? '' };
    // 'serveError' never crashes an unlistened emitter; mirror onto the
    // conventional 'error' only when someone is listening.
    this.emit('serveError', info);
    if (this.listenerCount('error') > 0) this.emit('error', info);
  }

  /** @internal Route a `'stopped'` proxy event to this handle. */
  _onStopped(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#retire();
    this.#emitClose();
  }

  /**
   * Stop serving and release the tailnet port (`proxy.remove(id)`). Emits
   * `'close'` once. Idempotent.
   */
  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    this.#retire();
    try {
      await this.#proxy.remove(this.id);
    } finally {
      this.#emitClose();
    }
  }

  #emitClose(): void {
    if (this.#closeEmitted) return;
    this.#closeEmitted = true;
    this.emit('close');
  }
}

/**
 * The `mesh.serve` verb: publish a local service or directory to the tailnet.
 * Resolves with a {@link ServeHandle} once the proxy is running.
 */
export type TruffleServe = (config: ServeConfig) => Promise<ServeHandle>;

/**
 * Normalize any {@link ServeConfig} shape into the single `NapiProxyConfig`
 * the proxy engine consumes. Pure and deterministic; exported so the shape
 * can be asserted directly. Throws on a `funnel` key (reserved), a malformed
 * shape, or a non-http(s) `target`.
 */
export function normalizeServeConfig(config: ServeConfig): NapiProxyConfig {
  return canonicalToNapi(canonicalServeConfig(config));
}

/**
 * Build the `mesh.serve` verb over a lazy proxy getter. `getProxy` is called
 * per serve (NapiProxy handles are per-call); proxy events are node-global,
 * so one `onEvent` subscription — installed on the first serve — routes
 * runtime errors and stops to every live handle by id.
 */
export function createServeNamespace(getProxy: () => NapiProxy): TruffleServe {
  const handles = new Map<string, ServeHandle>();
  let eventProxy: NapiProxy | null = null;
  let eventSubscription: NapiSubscription | null = null;

  function routeEvent(ev: NapiProxyEvent): void {
    const handle = handles.get(ev.id);
    if (!handle) return;
    if (ev.eventType === 'error') {
      handle._onError(ev.code, ev.message);
    } else if (ev.eventType === 'stopped') {
      handle._onStopped();
    }
    // 'started' carries url/listenPort, but the add() result already gives us
    // those synchronously — nothing to route.
  }

  function ensureSubscribed(proxy: NapiProxy): void {
    if (eventProxy) return;
    // Retain the subscribed handle so the native subscription outlives GC of
    // this NapiProxy. Events are node-global (all handles share one Arc<Node>).
    eventProxy = proxy;
    eventSubscription = proxy.onEvent(routeEvent);
    void eventSubscription;
  }

  return async function serve(config: ServeConfig): Promise<ServeHandle> {
    const canonical = canonicalServeConfig(config); // throws: funnel / bad shape
    const napiConfig = canonicalToNapi(canonical); // throws: non-http(s) target
    const replayConfig = deepFreeze(canonical);
    const id = napiConfig.id;

    if (handles.has(id)) {
      throw new Error(
        `serve: id ${JSON.stringify(id)} is already serving on this node — ` +
          'pass a distinct `id` (the default is `serve-${port}`, so two serves ' +
          'on the same port collide).',
      );
    }

    const proxy = getProxy();
    // Subscribe and register the handle before add() resolves, so a runtime
    // error emitted the instant the listener starts is routed, not dropped.
    ensureSubscribed(proxy);
    const handle = new ServeHandle(id, replayConfig, proxy, () => {
      if (handles.get(id) === handle) handles.delete(id);
    });
    handles.set(id, handle);

    try {
      const info = await proxy.add(napiConfig);
      handle._start(info.url, info.listenPort);
      return handle;
    } catch (err) {
      if (handles.get(id) === handle) handles.delete(id);
      throw err;
    }
  };
}

// ─── normalization ────────────────────────────────────────────────────────

/**
 * Fill defaults, resolve dirs to absolute, and validate — producing a
 * canonical {@link ServeConfig} that is its own replayable form. Idempotent:
 * `canonicalServeConfig(canonicalServeConfig(x))` normalizes identically, so
 * replaying `handle.config` reproduces the original `NapiProxyConfig`.
 */
function canonicalServeConfig(config: ServeConfig): ServeConfig {
  if (!config || typeof config !== 'object') {
    throw new TypeError('serve: config must be an object');
  }
  const c = config as ServeConfigFields;

  if (c.funnel !== undefined) {
    throw new Error('funnel is not supported yet — RFC 023 §9.6 reserves it for a future phase');
  }
  if (!Number.isInteger(c.port) || c.port < 0 || c.port > 65535) {
    throw new TypeError(`serve: invalid port ${String(c.port)} (expected an integer 0–65535)`);
  }

  const shapes =
    Number(c.target !== undefined) + Number(c.dir !== undefined) + Number(c.routes !== undefined);
  if (shapes !== 1) {
    throw new TypeError(
      `serve: config must have exactly one of \`target\`, \`dir\`, or \`routes\` (got ${
        shapes === 0 ? 'none' : 'several'
      })`,
    );
  }

  const id = c.id ?? `serve-${c.port}`;
  const common: ServeOptions & { port: number } = {
    port: c.port,
    id,
    name: c.name ?? id,
    tls: c.tls ?? true,
  };
  if (c.allow !== undefined) common.allow = [...c.allow];
  if (c.allowNonLoopback !== undefined) common.allowNonLoopback = c.allowNonLoopback;
  if (c.headers !== undefined) common.headers = canonicalHeaders(c.headers, 'serve');

  if (c.target !== undefined) {
    return { ...common, target: c.target };
  }
  if (c.dir !== undefined) {
    const out: ServeStaticConfig = { ...common, dir: resolvePath(c.dir) };
    if (c.fallback !== undefined) out.fallback = c.fallback;
    return out;
  }
  const routesIn = c.routes as Record<string, string | ServeRouteValue>;
  if (Object.keys(routesIn).length === 0) {
    throw new TypeError('serve: `routes` must have at least one entry');
  }
  const routes: Record<string, string | ServeRouteValue> = {};
  for (const [prefix, value] of Object.entries(routesIn)) {
    routes[prefix] = canonicalRouteValue(prefix, value);
  }
  return { ...common, routes };
}

/** Canonicalize one route value: string stays a URL; object resolves its dir. */
function canonicalRouteValue(
  prefix: string,
  value: string | ServeRouteValue,
): string | ServeRouteValue {
  if (typeof value === 'string') return value;
  if (!value || typeof value !== 'object') {
    throw new TypeError('serve: a route value must be a URL string or a route object');
  }
  const hasTarget = value.target !== undefined;
  const hasDir = value.dir !== undefined;
  if (hasTarget === hasDir) {
    throw new TypeError('serve: each route object needs exactly one of `target` or `dir`');
  }
  const out: ServeRouteValue = {};
  if (hasTarget) {
    out.target = value.target;
    if (value.stripPrefix !== undefined) out.stripPrefix = value.stripPrefix;
  } else {
    out.dir = resolvePath(value.dir as string);
    if (value.fallback !== undefined) out.fallback = value.fallback;
  }
  if (value.allow !== undefined) out.allow = [...value.allow];
  if (value.headers !== undefined) {
    out.headers = canonicalHeaders(value.headers, `route ${JSON.stringify(prefix)}`);
  }
  return out;
}

/**
 * Copy a name → value headers record, checking only its shape. truffle-core
 * owns the rules (names, values, refused names, bounds; RFC 030) and refuses
 * the serve with its own message.
 */
function canonicalHeaders(headers: Record<string, string>, where: string): Record<string, string> {
  if (!headers || typeof headers !== 'object' || Array.isArray(headers)) {
    throw new TypeError(`serve: ${where} headers must be an object of name → value strings`);
  }
  const out: Record<string, string> = {};
  for (const [name, value] of Object.entries(headers)) {
    if (typeof value !== 'string') {
      throw new TypeError(`serve: ${where} header ${JSON.stringify(name)} must be a string`);
    }
    out[name] = value;
  }
  return out;
}

/** A headers record as the NAPI list, in insertion order. */
function headersToNapi(headers: Record<string, string>): NapiProxyHeader[] {
  return Object.entries(headers).map(([name, value]) => ({ name, value }));
}

/** Convert a canonical config (defaults filled, dirs absolute) to NAPI shape. */
function canonicalToNapi(config: ServeConfig): NapiProxyConfig {
  const c = config as ServeConfigFields;
  const napi: NapiProxyConfig = {
    id: c.id as string,
    name: c.name as string,
    listenPort: c.port,
    // Ignored when `routes` is present; the NAPI field is required (RFC 023).
    targetPort: 0,
    tls: c.tls as boolean,
  };
  if (c.allow !== undefined) napi.allow = [...c.allow];
  if (c.allowNonLoopback !== undefined) napi.allowNonLoopback = c.allowNonLoopback;
  if (c.headers !== undefined) napi.headers = headersToNapi(c.headers);

  if (c.target !== undefined) {
    const { host, port, scheme } = parseTargetUrl(c.target);
    napi.targetHost = host;
    napi.targetPort = port;
    napi.targetScheme = scheme;
    return napi;
  }

  const routes: NapiProxyRoute[] = [];
  if (c.dir !== undefined) {
    const route: NapiProxyRoute = { prefix: '/', dir: c.dir };
    if (c.fallback !== undefined) route.fallback = c.fallback;
    routes.push(route);
  } else {
    for (const [prefix, value] of Object.entries(
      c.routes as Record<string, string | ServeRouteValue>,
    )) {
      routes.push(routeValueToNapi(prefix, value));
    }
  }
  napi.routes = routes;
  return napi;
}

/** Convert a canonical route value to a `NapiProxyRoute`. */
function routeValueToNapi(prefix: string, value: string | ServeRouteValue): NapiProxyRoute {
  const route: NapiProxyRoute = { prefix };
  if (typeof value === 'string') {
    route.targetUrl = value;
    return route;
  }
  if (value.target !== undefined) {
    route.targetUrl = value.target;
    if (value.stripPrefix !== undefined) route.stripPrefix = value.stripPrefix;
  } else {
    route.dir = value.dir as string;
    if (value.fallback !== undefined) route.fallback = value.fallback;
  }
  if (value.allow !== undefined) route.allow = [...value.allow];
  if (value.headers !== undefined) route.headers = headersToNapi(value.headers);
  return route;
}

/**
 * Parse an http(s) target URL into host/port/scheme. A bare `host:port`
 * (no scheme) is rejected: WHATWG URL reads `localhost` as a scheme, so it
 * fails the http(s) check with a pointer to add `http://`.
 */
function parseTargetUrl(target: string): { host: string; port: number; scheme: string } {
  let url: URL;
  try {
    url = new URL(target);
  } catch {
    throw new TypeError(
      `serve: invalid target URL ${JSON.stringify(target)} — pass a full http(s) URL, e.g. "http://localhost:3000"`,
    );
  }
  if (url.protocol !== 'http:' && url.protocol !== 'https:') {
    throw new TypeError(
      `serve: target must be an http:// or https:// URL, got ${JSON.stringify(target)} ` +
        '(a bare "host:port" is read as a URL scheme — include http://)',
    );
  }
  const scheme = url.protocol.slice(0, -1);
  const port = url.port ? Number(url.port) : scheme === 'https' ? 443 : 80;
  return { host: url.hostname, port, scheme };
}

/** Recursively freeze an object graph (config + nested routes/arrays). */
function deepFreeze<T>(value: T): T {
  if (value && typeof value === 'object' && !Object.isFrozen(value)) {
    for (const child of Object.values(value)) deepFreeze(child);
    Object.freeze(value);
  }
  return value;
}
