// Unit tests for mesh.serve (RFC 023 §6.2, Phase 3).
//
// Normalization is tested directly through the pure `normalizeServeConfig`;
// handle behavior (url/port, close, error/stop routing, config replay) runs
// serve() against a mocked NapiProxy — no tailnet, no native addon.
// Run: pnpm --filter @vibecook/truffle test

'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { once } = require('node:events');
const path = require('node:path');

async function load() {
  return import('../dist/serve.js');
}

/** A mock NapiProxy recording add/remove and capturing the onEvent callback. */
function mockProxy() {
  const state = { added: [], removed: [], eventCb: null };
  const proxy = {
    async add(config) {
      state.added.push(config);
      const suffix = config.listenPort === 443 ? '' : `:${config.listenPort}`;
      return {
        id: config.id,
        name: config.name,
        listenPort: config.listenPort,
        targetHost: config.targetHost ?? '',
        targetPort: config.targetPort,
        targetScheme: config.targetScheme ?? '',
        url: `https://myapp.tail1234.ts.net${suffix}`,
        status: 'running',
      };
    },
    async remove(id) {
      state.removed.push(id);
    },
    onEvent(cb) {
      state.eventCb = cb;
    },
  };
  return { proxy, state };
}

// ─── normalization ──────────────────────────────────────────────────────────

test('single target: URL parses to host/port/scheme; no routes', async () => {
  const { normalizeServeConfig } = await load();
  const napi = normalizeServeConfig({ port: 443, target: 'http://localhost:3000' });
  assert.equal(napi.listenPort, 443);
  assert.equal(napi.targetHost, 'localhost');
  assert.equal(napi.targetPort, 3000);
  assert.equal(napi.targetScheme, 'http');
  assert.equal(napi.routes, undefined);
});

test('single target: https default port is 443, explicit port honored', async () => {
  const { normalizeServeConfig } = await load();
  assert.equal(normalizeServeConfig({ port: 8443, target: 'https://localhost' }).targetPort, 443);
  const explicit = normalizeServeConfig({ port: 8443, target: 'https://localhost:3001' });
  assert.equal(explicit.targetScheme, 'https');
  assert.equal(explicit.targetPort, 3001);
});

test('single target: non-http(s) and malformed URLs throw TypeError', async () => {
  const { normalizeServeConfig } = await load();
  // A bare host:port is read as a URL scheme — rejected with a pointer to http://.
  assert.throws(() => normalizeServeConfig({ port: 443, target: 'localhost:3000' }), {
    name: 'TypeError',
    message: /http:\/\//,
  });
  assert.throws(() => normalizeServeConfig({ port: 443, target: 'ftp://host/x' }), {
    name: 'TypeError',
    message: /http/,
  });
  assert.throws(() => normalizeServeConfig({ port: 443, target: 'not a url' }), {
    name: 'TypeError',
    message: /invalid target URL/,
  });
});

test('dir shape → one "/" route with an absolute dir and fallback', async () => {
  const { normalizeServeConfig } = await load();
  const napi = normalizeServeConfig({ port: 443, dir: './public', fallback: '/index.html' });
  assert.equal(napi.targetPort, 0);
  assert.deepEqual(napi.routes, [
    { prefix: '/', dir: path.resolve('./public'), fallback: '/index.html' },
  ]);
  // Without fallback, the field is absent (not undefined).
  const noFallback = normalizeServeConfig({ port: 443, dir: '/abs/pub' });
  assert.deepEqual(noFallback.routes, [{ prefix: '/', dir: '/abs/pub' }]);
});

test('routes object → route array (order-independent) with prefixes preserved', async () => {
  const { normalizeServeConfig } = await load();
  const napi = normalizeServeConfig({
    port: 443,
    routes: {
      '/api': 'http://localhost:8000',
      '/grafana': { target: 'http://localhost:3001' },
      '/': { dir: './public', fallback: '/index.html' },
    },
  });
  assert.equal(napi.targetPort, 0);
  assert.equal(napi.routes.length, 3);
  const byPrefix = Object.fromEntries(napi.routes.map((r) => [r.prefix, r]));
  assert.deepEqual(byPrefix['/api'], { prefix: '/api', targetUrl: 'http://localhost:8000' });
  assert.deepEqual(byPrefix['/grafana'], {
    prefix: '/grafana',
    targetUrl: 'http://localhost:3001',
  });
  assert.deepEqual(byPrefix['/'], {
    prefix: '/',
    dir: path.resolve('./public'),
    fallback: '/index.html',
  });
});

test('routes object: stripPrefix and per-route allow pass through', async () => {
  const { normalizeServeConfig } = await load();
  const napi = normalizeServeConfig({
    port: 443,
    routes: {
      '/api': { target: 'http://localhost:8000', stripPrefix: true, allow: ['ops@corp.com'] },
    },
  });
  assert.deepEqual(napi.routes, [
    {
      prefix: '/api',
      targetUrl: 'http://localhost:8000',
      stripPrefix: true,
      allow: ['ops@corp.com'],
    },
  ]);
});

// ─── response headers (RFC 030) ─────────────────────────────────────────────

test('headers: a serve-level record becomes NapiProxyConfig.headers, in order', async () => {
  const { normalizeServeConfig } = await load();
  const headers = {
    'Content-Security-Policy': "default-src 'self'",
    'Referrer-Policy': 'no-referrer',
  };
  const want = [
    { name: 'Content-Security-Policy', value: "default-src 'self'" },
    { name: 'Referrer-Policy', value: 'no-referrer' },
  ];
  const target = normalizeServeConfig({ port: 443, target: 'http://localhost:3000', headers });
  assert.deepEqual(target.headers, want);
  const dir = normalizeServeConfig({ port: 443, dir: '/abs/pub', headers });
  assert.deepEqual(dir.headers, want);
  // The dir shape's one route carries none of its own: it inherits the serve's.
  assert.deepEqual(dir.routes, [{ prefix: '/', dir: '/abs/pub' }]);
  const routes = normalizeServeConfig({
    port: 443,
    headers,
    routes: { '/api': 'http://localhost:8000' },
  });
  assert.deepEqual(routes.headers, want);
});

test('headers: a route object carries NapiProxyRoute.headers; a string route none', async () => {
  const { normalizeServeConfig } = await load();
  const napi = normalizeServeConfig({
    port: 443,
    routes: {
      '/api': 'http://localhost:8000',
      '/': { dir: '/abs/pub', headers: { 'X-Frame-Options': 'DENY' } },
    },
  });
  const byPrefix = Object.fromEntries(napi.routes.map((r) => [r.prefix, r]));
  assert.deepEqual(byPrefix['/'], {
    prefix: '/',
    dir: '/abs/pub',
    headers: [{ name: 'X-Frame-Options', value: 'DENY' }],
  });
  assert.equal('headers' in byPrefix['/api'], false);
  // No serve-level headers: the field is absent, not an empty list.
  assert.equal('headers' in napi, false);
});

test('headers: absent headers leave the NAPI fields absent', async () => {
  const { normalizeServeConfig } = await load();
  const napi = normalizeServeConfig({
    port: 443,
    routes: { '/': { target: 'http://localhost:8000' } },
  });
  assert.equal('headers' in napi, false);
  assert.equal('headers' in napi.routes[0], false);
});

test('headers: a non-object record or a non-string value is a TypeError', async () => {
  const { normalizeServeConfig } = await load();
  assert.throws(
    () => normalizeServeConfig({ port: 443, target: 'http://localhost:3000', headers: ['x'] }),
    { name: 'TypeError', message: /serve headers must be an object/ },
  );
  assert.throws(
    () =>
      normalizeServeConfig({
        port: 443,
        routes: { '/': { dir: '/abs/pub', headers: { 'X-Frame-Options': 1 } } },
      }),
    { name: 'TypeError', message: /route "\/" header "X-Frame-Options" must be a string/ },
  );
});

test('headers: serve() hands them to add(); handle.config replays them', async () => {
  const { createServeNamespace, normalizeServeConfig } = await load();
  const { proxy, state } = mockProxy();
  const serve = createServeNamespace(() => proxy);

  const h = await serve({
    port: 443,
    headers: { 'Content-Security-Policy': "default-src 'self'" },
    routes: {
      '/': { dir: './public', fallback: '/index.html', headers: { 'X-Frame-Options': 'DENY' } },
    },
  });
  const first = state.added[0];
  assert.deepEqual(first.headers, [
    { name: 'Content-Security-Policy', value: "default-src 'self'" },
  ]);
  assert.deepEqual(first.routes[0].headers, [{ name: 'X-Frame-Options', value: 'DENY' }]);
  // The frozen config keeps the records, and replays to the same NAPI config.
  assert.ok(Object.isFrozen(h.config.headers));
  assert.deepEqual(normalizeServeConfig(h.config), first);
});

test('tls defaults to true and honors an explicit false', async () => {
  const { normalizeServeConfig } = await load();
  assert.equal(normalizeServeConfig({ port: 443, target: 'http://localhost:3000' }).tls, true);
  assert.equal(
    normalizeServeConfig({ port: 443, target: 'http://localhost:3000', tls: false }).tls,
    false,
  );
});

test('allow / allowNonLoopback pass through, and are omitted when absent', async () => {
  const { normalizeServeConfig } = await load();
  const napi = normalizeServeConfig({
    port: 443,
    target: 'http://localhost:3000',
    allow: ['*@corp.com'],
    allowNonLoopback: true,
  });
  assert.deepEqual(napi.allow, ['*@corp.com']);
  assert.equal(napi.allowNonLoopback, true);

  const bare = normalizeServeConfig({ port: 443, target: 'http://localhost:3000' });
  assert.equal('allow' in bare, false);
  assert.equal('allowNonLoopback' in bare, false);
});

test('id defaults to serve-{port}; name defaults to id', async () => {
  const { normalizeServeConfig } = await load();
  const def = normalizeServeConfig({ port: 3000, target: 'http://localhost:8080' });
  assert.equal(def.id, 'serve-3000');
  assert.equal(def.name, 'serve-3000');

  const named = normalizeServeConfig({
    port: 3000,
    target: 'http://localhost:8080',
    id: 'web',
    name: 'My Web',
  });
  assert.equal(named.id, 'web');
  assert.equal(named.name, 'My Web');

  // name falls back to id when only id is set.
  assert.equal(
    normalizeServeConfig({ port: 3000, target: 'http://localhost:8080', id: 'web' }).name,
    'web',
  );
});

test('funnel is rejected explicitly (RFC 023 §9.6)', async () => {
  const { normalizeServeConfig, createServeNamespace } = await load();
  assert.throws(
    () => normalizeServeConfig({ port: 443, target: 'http://localhost:3000', funnel: true }),
    /funnel is not supported yet/,
  );
  // Also via the serve() verb.
  const { proxy } = mockProxy();
  const serve = createServeNamespace(() => proxy);
  await assert.rejects(
    () => serve({ port: 443, target: 'http://localhost:3000', funnel: true }),
    /funnel is not supported yet/,
  );
});

test('config must have exactly one of target / dir / routes', async () => {
  const { normalizeServeConfig } = await load();
  assert.throws(() => normalizeServeConfig({ port: 443 }), /exactly one/);
  assert.throws(
    () => normalizeServeConfig({ port: 443, target: 'http://x', dir: './p' }),
    /exactly one/,
  );
  assert.throws(() => normalizeServeConfig({ port: 443, routes: {} }), /at least one entry/);
});

// ─── handle behavior ─────────────────────────────────────────────────────────

test('serve() returns a handle with id, url, and port from the add() result', async () => {
  const { createServeNamespace } = await load();
  const { proxy, state } = mockProxy();
  const serve = createServeNamespace(() => proxy);

  const h = await serve({ port: 443, target: 'http://localhost:3000' });
  assert.equal(h.id, 'serve-443');
  assert.equal(h.url, 'https://myapp.tail1234.ts.net');
  assert.equal(h.port, 443);
  // add() received the normalized single-target config.
  assert.equal(state.added.length, 1);
  assert.equal(state.added[0].targetHost, 'localhost');
  assert.equal(state.added[0].targetPort, 3000);
});

test('close() calls proxy.remove(id), emits close once, and is idempotent', async () => {
  const { createServeNamespace } = await load();
  const { proxy, state } = mockProxy();
  const serve = createServeNamespace(() => proxy);

  const h = await serve({ port: 8443, target: 'http://localhost:3000', id: 'web' });
  let closeCount = 0;
  h.on('close', () => (closeCount += 1));

  await h.close();
  await h.close();
  assert.deepEqual(state.removed, ['web']);
  assert.equal(closeCount, 1);
});

test('error events route by id; error only fires when listened; no crash unlistened', async () => {
  const { createServeNamespace } = await load();
  const { proxy, state } = mockProxy();
  const serve = createServeNamespace(() => proxy);

  const a = await serve({ port: 443, target: 'http://localhost:3000', id: 'a' });
  const b = await serve({ port: 8443, target: 'http://localhost:3001', id: 'b' });
  assert.equal(typeof state.eventCb, 'function'); // subscribed once, lazily

  const seenA = [];
  const seenB = [];
  a.on('serveError', (e) => seenA.push(e));
  b.on('serveError', (e) => seenB.push(e));

  // An error for 'a' reaches only handle a.
  state.eventCb({ eventType: 'error', id: 'a', code: 'SERVE_ERROR', message: 'boom' });
  assert.deepEqual(seenA, [{ code: 'SERVE_ERROR', message: 'boom' }]);
  assert.deepEqual(seenB, []);

  // 'b' has no 'error' listener — emitting must not throw (only 'serveError').
  assert.doesNotThrow(() =>
    state.eventCb({ eventType: 'error', id: 'b', code: 'CONNECTION_REFUSED', message: 'nope' }),
  );
  assert.deepEqual(seenB, [{ code: 'CONNECTION_REFUSED', message: 'nope' }]);

  // With an 'error' listener attached, the conventional event fires too.
  const errs = [];
  a.on('error', (e) => errs.push(e));
  state.eventCb({ eventType: 'error', id: 'a', code: 'X', message: 'y' });
  assert.deepEqual(errs, [{ code: 'X', message: 'y' }]);

  // Events for an unknown id are ignored.
  assert.doesNotThrow(() =>
    state.eventCb({ eventType: 'error', id: 'ghost', code: 'X', message: 'y' }),
  );
});

test("stopped event emits 'close' and marks the handle closed", async () => {
  const { createServeNamespace } = await load();
  const { proxy, state } = mockProxy();
  const serve = createServeNamespace(() => proxy);

  const h = await serve({ port: 443, target: 'http://localhost:3000', id: 'web' });
  const closed = once(h, 'close');
  state.eventCb({ eventType: 'stopped', id: 'web' });
  await closed;
});

test('duplicate live id is rejected before touching the sidecar', async () => {
  const { createServeNamespace } = await load();
  const { proxy, state } = mockProxy();
  const serve = createServeNamespace(() => proxy);

  const h = await serve({ port: 443, target: 'http://localhost:3000' }); // id serve-443
  await assert.rejects(
    () => serve({ port: 443, target: 'http://localhost:9000' }),
    /already serving/,
  );
  assert.equal(state.added.length, 1); // the duplicate never reached add()

  // After close() frees the id, the same port serves again.
  await h.close();
  const h2 = await serve({ port: 443, target: 'http://localhost:9000' });
  assert.equal(h2.id, 'serve-443');
  assert.equal(state.added.length, 2);
});

// ─── config replay (RFC 023 D16) ─────────────────────────────────────────────

test('handle.config is a frozen, replayable ServeConfig', async () => {
  const { createServeNamespace } = await load();
  const { proxy } = mockProxy();
  const serve = createServeNamespace(() => proxy);

  const h = await serve({ port: 443, dir: './public', fallback: '/index.html' });
  assert.ok(Object.isFrozen(h.config));
  assert.equal(h.config.port, 443);
  assert.equal(h.config.tls, true); // default filled
  assert.equal(h.config.id, 'serve-443');
  assert.equal(h.config.name, 'serve-443');
  assert.equal(path.isAbsolute(h.config.dir), true); // dir resolved
});

test('mesh.serve(handle.config) normalizes to an identical NAPI config', async () => {
  const { createServeNamespace, normalizeServeConfig } = await load();
  const { proxy, state } = mockProxy();
  const serve = createServeNamespace(() => proxy);

  const original = {
    port: 443,
    allow: ['*@corp.com'],
    routes: {
      '/api': 'http://localhost:8000',
      '/': { dir: './public', fallback: '/index.html' },
    },
  };
  const h = await serve(original);
  const first = state.added[0];

  // Pure: re-normalizing the frozen handle.config reproduces the NAPI config.
  assert.deepEqual(normalizeServeConfig(h.config), first);

  // Full replay through the verb (close first to free the id).
  await h.close();
  const h2 = await serve(h.config);
  assert.deepEqual(state.added[1], first);
  assert.equal(h2.id, h.id);
});
