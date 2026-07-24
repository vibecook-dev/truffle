import assert from 'node:assert/strict';
import test from 'node:test';

import {
  getCrateVersionStatus,
  sparseIndexContainsVersion,
  sparseIndexPath,
} from './crates-io-version-status.mjs';

const response = (status, body = '') => ({
  status,
  text: async () => body,
});

test('builds Cargo sparse-index paths for every crate-name length', () => {
  assert.equal(sparseIndexPath('a'), '1/a');
  assert.equal(sparseIndexPath('ab'), '2/ab');
  assert.equal(sparseIndexPath('AbC'), '3/a/abc');
  assert.equal(sparseIndexPath('truffle-core'), 'tr/uf/truffle-core');
});

test('rejects crate names that cannot form a safe sparse-index URL', () => {
  assert.throws(() => sparseIndexPath('../secret'), /invalid crate name/);
});

test('finds only an exact crate version in newline-delimited index JSON', () => {
  const contents = [
    JSON.stringify({ name: 'truffle-core', vers: '0.7.4' }),
    JSON.stringify({ name: 'truffle-core', vers: '0.7.5' }),
  ].join('\n');

  assert.equal(sparseIndexContainsVersion(contents, 'truffle-core', '0.7.5'), true);
  assert.equal(sparseIndexContainsVersion(contents, 'truffle-core', '0.7.6'), false);
});

test('fails closed when a sparse-index record is malformed', () => {
  assert.throws(
    () => sparseIndexContainsVersion('not JSON', 'truffle-core', '0.7.5'),
    /invalid sparse-index JSON/,
  );
});

test('reports a published version from the sparse index', async () => {
  let requestedUrl;
  const status = await getCrateVersionStatus('truffle-core', '0.7.5', {
    fetchImpl: async (url) => {
      requestedUrl = url;
      return response(200, JSON.stringify({ name: 'truffle-core', vers: '0.7.5' }));
    },
  });

  assert.equal(requestedUrl, 'https://index.crates.io/tr/uf/truffle-core');
  assert.equal(status, 'published');
});

test('reports a missing crate or version without treating it as a request failure', async () => {
  assert.equal(
    await getCrateVersionStatus('truffle-core', '0.7.5', {
      fetchImpl: async () => response(404),
    }),
    'missing',
  );
  assert.equal(
    await getCrateVersionStatus('truffle-core', '0.7.5', {
      fetchImpl: async () => response(200, JSON.stringify({ name: 'truffle-core', vers: '0.7.4' })),
    }),
    'missing',
  );
});

test('fails closed on unexpected registry responses', async () => {
  await assert.rejects(
    getCrateVersionStatus('truffle-core', '0.7.5', {
      fetchImpl: async () => response(403),
    }),
    /HTTP 403/,
  );
});
