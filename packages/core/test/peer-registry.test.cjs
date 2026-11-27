// Unit tests for the RFC 022 Peer registry + peer-event conversion:
// same-instance interning, `left` retirement with a usable handle, and
// generation supersession. Mocked snapshots — no tailnet required (the
// dynamic import of create-mesh-node loads the native addon package, so the
// platform .node binary must be built, same as the other dist-based tests).
//
// Run: pnpm --filter @vibecook/truffle test

'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');

async function load() {
  const peer = await import('../dist/peer.js');
  const cmn = await import('../dist/create-mesh-node.js');
  return {
    PeerRegistry: peer.PeerRegistry,
    peerLikeToQuery: peer.peerLikeToQuery,
    toMeshPeerEvent: cmn.toMeshPeerEvent,
  };
}

const fakeNode = {}; // Peer networking sugar (send/ping) is not exercised here.

function snap(overrides = {}) {
  return {
    displayName: 'bob',
    online: true,
    wsConnected: false,
    ip: '100.64.0.7',
    hostname: 'truffle-test-bob',
    connectionType: 'direct',
    deviceId: null,
    deviceName: null,
    tailscaleId: 'ts-1',
    peerRef: 'ts-1:1',
    generation: 1,
    ...overrides,
  };
}

test('upsert interns one instance per ref and refreshes fields', async () => {
  const { PeerRegistry } = await load();
  const reg = new PeerRegistry(fakeNode);
  const a = reg.upsert(snap());
  const b = reg.upsert(snap({ online: false }));
  assert.strictEqual(a, b);
  assert.equal(a.online, false);
  assert.strictEqual(reg.getByTailscaleId('ts-1'), a);
});

test('left without a snapshot still delivers the interned handle and retires it', async () => {
  const { PeerRegistry, toMeshPeerEvent } = await load();
  const reg = new PeerRegistry(fakeNode);
  const joined = toMeshPeerEvent(reg, { eventType: 'joined', peerId: 'ts-1', peer: snap() });
  const left = toMeshPeerEvent(reg, { eventType: 'left', peerId: 'ts-1' }); // no snapshot
  assert.strictEqual(left.peer, joined.peer); // same instance, so Map cleanup works
  assert.equal(left.peer.online, false); // final view is offline
  assert.equal(left.peer.wsConnected, false);
  assert.equal(reg.getByTailscaleId('ts-1'), undefined);
  assert.equal(reg.get('ts-1:1'), undefined);
});

test('left with the final snapshot delivers the same interned instance', async () => {
  const { PeerRegistry, toMeshPeerEvent } = await load();
  const reg = new PeerRegistry(fakeNode);
  const joined = toMeshPeerEvent(reg, { eventType: 'joined', peerId: 'ts-1', peer: snap() });
  const left = toMeshPeerEvent(reg, {
    eventType: 'left',
    peerId: 'ts-1',
    peer: snap({ online: false, wsConnected: false }),
  });
  assert.strictEqual(left.peer, joined.peer);
  assert.equal(left.peer.online, false);
  assert.equal(reg.getByTailscaleId('ts-1'), undefined);
});

test('rejoin with a new generation supersedes the old handle without leaking', async () => {
  const { PeerRegistry } = await load();
  const reg = new PeerRegistry(fakeNode);
  const gen1 = reg.upsert(snap());
  const gen2 = reg.upsert(snap({ peerRef: 'ts-1:2', generation: 2 }));
  assert.notStrictEqual(gen1, gen2);
  assert.equal(gen1.equals(gen2), false);
  assert.equal(gen1.online, false); // superseded handle pinned offline
  assert.equal(reg.get('ts-1:1'), undefined); // no leak across rejoins
  assert.strictEqual(reg.getByTailscaleId('ts-1'), gen2); // attribution → live generation
});

test('handles route by generation-checked ref; strings pass through', async () => {
  const { PeerRegistry, peerLikeToQuery } = await load();
  const reg = new PeerRegistry(fakeNode);
  const peer = reg.upsert(snap({ deviceId: '01HZXK7Q2M' }));
  // Ref (not ULID): the core generation-checks it and PeerGone-rejects
  // stale handles (RFC 022 I5).
  assert.equal(peerLikeToQuery(peer), 'ts-1:1');
  assert.equal(peerLikeToQuery('Bob'), 'Bob');
});

test('left for an unknown peer yields no handle and does not throw', async () => {
  const { PeerRegistry, toMeshPeerEvent } = await load();
  const reg = new PeerRegistry(fakeNode);
  const ev = toMeshPeerEvent(reg, { eventType: 'left', peerId: 'ts-9' });
  assert.equal(ev.peer, undefined);
  assert.equal(ev.peerId, 'ts-9');
});

test('mesh and native callers keep their peer and message contracts', async (t) => {
  const { createMeshNode, NapiNode } = await import('../dist/index.js');
  const snapshot = snap();
  const messageListeners = new Set();
  const rawStop = t.mock.method(NapiNode.prototype, 'stop', async () => {});
  t.mock.method(NapiNode.prototype, 'start', async () => {});
  t.mock.method(NapiNode.prototype, 'onAuthRequired', () => {});
  t.mock.method(NapiNode.prototype, 'onPeerChange', () => ({ close() {} }));
  t.mock.method(NapiNode.prototype, 'peer', async (query, waitMs) => {
    assert.equal(query, 'bob');
    assert.equal(waitMs, 42);
    return snapshot;
  });
  t.mock.method(NapiNode.prototype, 'onMessage', (namespace, callback) => {
    assert.equal(namespace, 'chat');
    messageListeners.add(callback);
    return { close: () => messageListeners.delete(callback) };
  });

  const mesh = await createMeshNode({ appId: 'mesh-test', sidecarPath: '/unused' });
  assert.equal(await mesh.native.peer('bob', 42), snapshot);
  const bob = await mesh.peer('bob', { waitMs: 42 });
  assert.equal(bob.ref, snapshot.peerRef);

  const rawMessages = [];
  const meshMessages = [];
  const sub = mesh.native.onMessage('chat', (message) => rawMessages.push(message));
  const unsubscribe = mesh.onMessage('chat', (message) => meshMessages.push(message));
  const message = {
    from: snapshot.tailscaleId,
    namespace: 'chat',
    msgType: 'text',
    payload: Buffer.from('hi'),
  };
  for (const listener of messageListeners) listener(message);
  assert.equal(rawMessages[0].from, snapshot.tailscaleId);
  assert.equal(meshMessages[0].from, bob);
  sub.close();
  assert.equal(messageListeners.size, 1);
  unsubscribe();
  assert.equal(messageListeners.size, 0);

  mesh.onMessage('chat', () => {});
  await Promise.all([mesh.native.stop(), mesh.stop()]);
  assert.equal(messageListeners.size, 0);
  assert.equal(rawStop.mock.callCount(), 1);
});
