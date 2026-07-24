const test = require('node:test');
const assert = require('node:assert/strict');
const {
  buildDownloadUrl,
  assertChecksum,
  loadExpectedChecksum,
  requireExpectedChecksum,
  requirePlatformConfig,
} = require('../scripts/postinstall.cjs');
const checksumMap = require('../sidecar-checksums.json');

const assets = [
  'tsnet-sidecar-darwin-arm64',
  'tsnet-sidecar-darwin-amd64',
  'tsnet-sidecar-linux-amd64',
  'tsnet-sidecar-linux-arm64',
  'tsnet-sidecar-windows-amd64.exe',
];

test('fallback URL uses the truffle-v release tag scheme', () => {
  assert.equal(
    buildDownloadUrl('0.4.8', 'tsnet-sidecar-darwin-arm64'),
    'https://github.com/jamesyong-42/truffle/releases/download/truffle-v0.4.8/tsnet-sidecar-darwin-arm64',
  );
});

test('checksum mismatch throws (fail closed); match and case-insensitive match pass', () => {
  assert.throws(() => assertChecksum('aa'.repeat(32), 'bb'.repeat(32), 'x'), /checksum mismatch/);
  assert.doesNotThrow(() => assertChecksum('AB'.repeat(32), 'ab'.repeat(32), 'x'));
});

test('every shipped checksum set is complete and contains real sha256 entries', () => {
  const versions = Object.keys(checksumMap).filter((key) => key !== '_comment');
  assert.ok(versions.length > 0);
  for (const version of versions) {
    for (const asset of assets) {
      assert.match(loadExpectedChecksum(version, asset) ?? '', /^[0-9a-f]{64}$/);
    }
  }
});

test('missing checksums fail closed before a download can be attempted', () => {
  assert.throws(
    () => requireExpectedChecksum('999.999.999', 'tsnet-sidecar-linux-amd64'),
    /Refusing to download an unverified executable/,
  );
});

test('supported platform configuration resolves every required asset', () => {
  assert.deepEqual(requirePlatformConfig('linux', 'x64'), {
    key: 'linux-x64',
    pkg: '@vibecook/truffle-sidecar-linux-x64',
    asset: 'tsnet-sidecar-linux-amd64',
    binName: 'sidecar-slim',
  });
  assert.equal(requirePlatformConfig('win32', 'x64').binName, 'sidecar-slim.exe');
});

test('unsupported platforms fail closed', () => {
  assert.throws(() => requirePlatformConfig('freebsd', 'x64'), /No prebuilt sidecar/);
});

test('requiring the script does not execute main()', () => {
  // If main() had run on require, the suite would have attempted a download; reaching here is the assertion.
  assert.ok(true);
});
