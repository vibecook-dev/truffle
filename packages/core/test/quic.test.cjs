// Unit tests for the mesh.quic wrappers (RFC 021 Phase 3).
//
// Mocked native handles — no tailnet, no addon calls.

'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { once } = require('node:events');

async function loadQuic() {
  return import('../dist/quic.js');
}

function mockNativeStream(chunks = []) {
  const state = { writes: [], finished: false, closed: false, order: [] };
  let readIndex = 0;
  const native = {
    async read(_maxBytes) {
      await new Promise((resolve) => setTimeout(resolve, 1));
      if (readIndex >= chunks.length) return null;
      const chunk = chunks[readIndex];
      readIndex += 1;
      return chunk;
    },
    async write(data) {
      state.writes.push(Buffer.from(data));
    },
    async finish() {
      state.finished = true;
      state.order.push('finish');
    },
    async close() {
      state.closed = true;
      state.order.push('close');
    },
  };
  return { native, state };
}

function mockNativeConnection(streamMocks) {
  const state = { closed: false, opened: 0 };
  let acceptIndex = 0;
  const native = {
    async openStream() {
      state.opened += 1;
      return mockNativeStream([]).native;
    },
    async acceptStream() {
      if (acceptIndex >= streamMocks.length) return null;
      const s = streamMocks[acceptIndex];
      acceptIndex += 1;
      return s;
    },
    remoteAddress: () => '100.64.0.9:4433',
    remotePeerId: () => 'PEER-9',
    close() {
      state.closed = true;
    },
  };
  return { native, state };
}

test('TruffleQuicStream delivers data, end() half-closes via finish()', async () => {
  const { TruffleQuicStream } = await loadQuic();
  const { native, state } = mockNativeStream([Buffer.from('qu'), Buffer.from('ic')]);

  const stream = new TruffleQuicStream(native);
  const received = [];
  stream.on('data', (chunk) => received.push(chunk));

  // Attach all waiters up front — event order is not guaranteed.
  const finished = once(stream, 'finish');
  const ended = once(stream, 'end');
  const closed = once(stream, 'close');

  stream.write('payload');
  stream.end(); // half-close: reads must still complete after this
  await finished;
  await ended;
  await closed; // autoDestroy after both directions complete

  assert.equal(Buffer.concat(received).toString(), 'quic');
  assert.equal(Buffer.concat(state.writes).toString(), 'payload');
  assert.equal(state.finished, true, 'end() must map to native finish()');
  assert.deepEqual(
    state.order,
    ['finish', 'close'],
    'half-close (finish) must precede the autoDestroy close',
  );
});

test('TruffleQuicStream destroy() maps to native close()', async () => {
  const { TruffleQuicStream } = await loadQuic();
  const { native, state } = mockNativeStream([]);

  const stream = new TruffleQuicStream(native);
  stream.destroy();
  await once(stream, 'close');
  assert.equal(state.closed, true);
});

test('TruffleQuicConnection iterates peer streams and exposes metadata', async () => {
  const { TruffleQuicConnection } = await loadQuic();
  const s1 = mockNativeStream([Buffer.from('a')]).native;
  const s2 = mockNativeStream([Buffer.from('b')]).native;
  const { native, state } = mockNativeConnection([s1, s2]);

  const conn = new TruffleQuicConnection(native);
  assert.equal(conn.remoteAddress, '100.64.0.9:4433');
  assert.equal(conn.remotePeerId, 'PEER-9');

  const seen = [];
  for await (const stream of conn.streams()) {
    seen.push(stream);
    stream.destroy();
  }
  assert.equal(seen.length, 2, 'iteration ends when acceptStream returns null');

  await conn.openStream();
  assert.equal(state.opened, 1);

  conn.close();
  assert.equal(state.closed, true);
});

test('listen(0) exposes the assigned port and iterates connections until closed', async () => {
  const { createQuicNamespace } = await loadQuic();
  const connMocks = [mockNativeConnection([]).native, mockNativeConnection([]).native];
  let acceptIndex = 0;
  let closed = false;
  const nativeListener = {
    async accept() {
      if (acceptIndex >= connMocks.length) return null;
      const c = connMocks[acceptIndex];
      acceptIndex += 1;
      return c;
    },
    port: () => 9420,
    close() {
      closed = true;
    },
  };

  const quic = createQuicNamespace({
    async listenQuic(port) {
      assert.equal(port, 0);
      return nativeListener;
    },
  });
  const server = await quic.listen(0);
  assert.equal(server.port, 9420);

  const conns = [];
  for await (const conn of server) {
    conns.push(conn);
  }
  assert.equal(conns.length, 2);

  server.close();
  assert.equal(closed, true);
});
