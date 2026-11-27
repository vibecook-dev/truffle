'use strict';

const assert = require('node:assert/strict');
const { after, test } = require('node:test');
const { JSDOM } = require('jsdom');
const React = require('react');

const dom = new JSDOM('<!doctype html><body></body>');
globalThis.window = dom.window;
globalThis.document = dom.window.document;
globalThis.IS_REACT_ACT_ENVIRONMENT = true;
const { createRoot } = require('react-dom/client');
after(() => dom.window.close());

function peer(id) {
  return {
    tailscaleId: id,
    peerRef: id + ':1',
    generation: 1,
    deviceId: null,
    deviceName: null,
    displayName: id,
    hostname: id,
    ip: '127.0.0.1',
    online: true,
    wsConnected: false,
    connectionType: 'direct',
  };
}

for (const input of ['mesh', 'mesh.native', 'standalone NapiNode']) {
  test(`useMesh and useAuth receive events and unsubscribe with ${input}`, async (t) => {
    const { createMeshNode, NapiNode } = await import('@vibecook/truffle');
    const { useMesh, useAuth } = await import('@vibecook/truffle-react');
    const listeners = new Set();
    const alpha = peer('alpha');
    let started;
    t.mock.method(NapiNode.prototype, 'start', async function () {
      started = this;
    });
    t.mock.method(NapiNode.prototype, 'stop', async () => {});
    t.mock.method(NapiNode.prototype, 'onAuthRequired', () => {});
    t.mock.method(NapiNode.prototype, 'getLocalInfo', function () {
      assert.equal(this, started);
      return { deviceId: 'local' };
    });
    t.mock.method(NapiNode.prototype, 'getPeers', async () => [alpha]);
    t.mock.method(NapiNode.prototype, 'onPeerChange', function (callback) {
      assert.equal(this, started);
      listeners.add(callback);
      return { close: () => listeners.delete(callback) };
    });
    const emit = async (event) => {
      await React.act(async () => {
        for (const listener of listeners) listener(event);
      });
    };

    const config = { appId: 'hook-test', sidecarPath: '/unused', autoAuth: false };
    let node;
    if (input === 'standalone NapiNode') {
      node = new NapiNode();
      await node.start(config);
    } else {
      const mesh = await createMeshNode(config);
      assert.equal(mesh.native, started);
      assert.notEqual(mesh, started);
      assert.equal(mesh.getLocalInfo().deviceId, 'local');
      assert.equal((await mesh.getPeers())[0].ref, alpha.peerRef);
      node = input === 'mesh' ? mesh : mesh.native;
    }
    const initialSubscriptions = listeners.size;
    const root = createRoot(document.createElement('div'));
    let meshState;
    let auth;
    function Probe() {
      meshState = useMesh(node);
      auth = useAuth(node);
      return null;
    }

    try {
      await React.act(async () => {
        root.render(React.createElement(React.StrictMode, null, React.createElement(Probe)));
      });
      assert.deepEqual(meshState.peers, [alpha]);
      assert.equal(listeners.size, initialSubscriptions + 2);

      const authUrl = 'https://example.invalid/login';
      await emit({ eventType: 'auth_required', peerId: '', authUrl });
      assert.equal(auth.status, 'required');
      assert.equal(auth.authUrl, authUrl);

      const beta = peer('beta');
      await emit({ eventType: 'joined', peerId: 'beta', peer: beta });
      assert.deepEqual(meshState.peers, [alpha, beta]);
      assert.equal(auth.status, 'authenticated');
      assert.equal(auth.authUrl, null);

      const renamed = { ...beta, displayName: 'renamed' };
      await emit({ eventType: 'updated', peerId: 'beta', peer: renamed });
      assert.deepEqual(meshState.peers, [alpha, renamed]);

      await emit({ eventType: 'left', peerId: 'alpha' });
      assert.deepEqual(meshState.peers, [renamed]);
    } finally {
      await React.act(async () => {
        root.unmount();
      });
      assert.equal(listeners.size, initialSubscriptions);
      await node.stop();
    }
    assert.equal(listeners.size, 0);
  });
}
