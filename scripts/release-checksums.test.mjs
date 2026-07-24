import assert from 'node:assert/strict';
import test from 'node:test';

import { validateReleaseChecksums } from './release-checksums.mjs';

const version = '1.2.3';
const assets = ['sidecar-linux', 'sidecar-windows.exe'];
const checksumPaths = ['javascript-checksums.json', 'rust-checksums.json'];
const digest = 'a'.repeat(64);

const validate = (checksumMaps, allowMissingChecksums) =>
  validateReleaseChecksums(checksumMaps, version, {
    allowMissingChecksums,
    assets,
    checksumPaths,
  });

test('pre-artifact validation accepts a version absent from both maps', () => {
  assert.deepEqual(validate([{}, {}], true), []);
});

test('strict publishing validation rejects absent checksums', () => {
  assert.equal(validate([{}, {}], false).length, assets.length * checksumPaths.length);
});

test('pre-artifact validation rejects a checksum set present in only one map', () => {
  const errors = validate(
    [{ [version]: Object.fromEntries(assets.map((asset) => [asset, digest])) }, {}],
    true,
  );
  assert.ok(errors.length > 0);
  assert.ok(errors.some((error) => error.includes('rust-checksums.json')));
});

test('pre-artifact validation rejects partial or malformed checksum sets', () => {
  const partial = { [version]: { [assets[0]]: 'not-a-sha256' } };
  const errors = validate([partial, structuredClone(partial)], true);
  assert.ok(errors.some((error) => error.includes(assets[0])));
  assert.ok(errors.some((error) => error.includes(assets[1])));
});

test('pre-artifact validation accepts complete matching checksum sets', () => {
  const complete = { [version]: Object.fromEntries(assets.map((asset) => [asset, digest])) };
  assert.deepEqual(validate([complete, structuredClone(complete)], true), []);
});

test('validation rejects disagreement between complete checksum sets', () => {
  const javascript = { [version]: Object.fromEntries(assets.map((asset) => [asset, digest])) };
  const rust = structuredClone(javascript);
  rust[version][assets[1]] = 'b'.repeat(64);
  assert.ok(
    validate([javascript, rust], true).some((error) =>
      error.includes(`checksum maps disagree for ${version}/${assets[1]}`),
    ),
  );
});
