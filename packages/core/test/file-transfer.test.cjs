// Unit tests for the file-transfer wrapper: every peer-taking method routes a
// PeerLike to the native call — a Peer handle by its generation-checked ref,
// a string unchanged — and named pulls (RFC 032) pass the root name and the
// relative path through untouched. A fake native handle records the calls,
// so no tailnet is needed (the dynamic import of create-mesh-node loads the
// native addon package, so the platform .node binary must be built, same as
// the other dist-based tests).
//
// Run: pnpm --filter @vibecook/truffle test

'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');

async function load() {
  const peer = await import('../dist/peer.js');
  const cmn = await import('../dist/create-mesh-node.js');
  return { PeerRegistry: peer.PeerRegistry, wrapFileTransfer: cmn.wrapFileTransfer };
}

const RESULT = { bytesTransferred: 4, sha256: 'ab'.repeat(32), elapsedSecs: 0.01 };

/** A native file-transfer handle that records each call and its arguments. */
function fakeNativeFileTransfer() {
  const calls = [];
  const record =
    (name) =>
    async (...args) => {
      calls.push([name, ...args]);
      return RESULT;
    };
  return {
    calls,
    sendFile: record('sendFile'),
    pullFile: record('pullFile'),
    pullNamedFile: record('pullNamedFile'),
    addNamedPullRoot: (name, root) => calls.push(['addNamedPullRoot', name, root]),
  };
}

function snap() {
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
  };
}

test('pullNamedFile routes a Peer handle by its ref and passes root and path through', async () => {
  const { PeerRegistry, wrapFileTransfer } = await load();
  const peer = new PeerRegistry({}).upsert(snap());
  const native = fakeNativeFileTransfer();
  const ft = wrapFileTransfer(native);

  const result = await ft.pullNamedFile(peer, 'drive-objects', 'ab/ab01.bin', '/tmp/ab01.bin');

  assert.deepEqual(native.calls, [
    ['pullNamedFile', 'ts-1:1', 'drive-objects', 'ab/ab01.bin', '/tmp/ab01.bin'],
  ]);
  assert.strictEqual(result, RESULT);
});

test('pullNamedFile passes a string peer query unchanged', async () => {
  const { wrapFileTransfer } = await load();
  const native = fakeNativeFileTransfer();
  const ft = wrapFileTransfer(native);

  await ft.pullNamedFile('laptop', 'objects', 'f.bin', '/tmp/f.bin');

  assert.deepEqual(native.calls, [['pullNamedFile', 'laptop', 'objects', 'f.bin', '/tmp/f.bin']]);
});

test('sendFile and pullFile still route PeerLike as before', async () => {
  const { PeerRegistry, wrapFileTransfer } = await load();
  const peer = new PeerRegistry({}).upsert(snap());
  const native = fakeNativeFileTransfer();
  const ft = wrapFileTransfer(native);

  await ft.sendFile(peer, '/tmp/a.bin', 'a.bin');
  await ft.pullFile('laptop', '/srv/shared/b.bin', '/tmp/b.bin');

  assert.deepEqual(native.calls, [
    ['sendFile', 'ts-1:1', '/tmp/a.bin', 'a.bin'],
    ['pullFile', 'laptop', '/srv/shared/b.bin', '/tmp/b.bin'],
  ]);
});

test('methods that take no peer are the native ones, untouched', async () => {
  const { wrapFileTransfer } = await load();
  const native = fakeNativeFileTransfer();
  const addNamedPullRoot = native.addNamedPullRoot;
  const ft = wrapFileTransfer(native);

  assert.strictEqual(ft.addNamedPullRoot, addNamedPullRoot);
  ft.addNamedPullRoot('objects', '/srv/objects');
  assert.deepEqual(native.calls, [['addNamedPullRoot', 'objects', '/srv/objects']]);
});
