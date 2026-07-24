import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

import {
  calculateReleaseLockUpdates,
  synchronizeCargoLock,
  synchronizeNapiPackageLock,
} from './sync-release-locks.mjs';

test('synchronizeCargoLock updates each local package exactly once', () => {
  const lock = [
    '[[package]]',
    'name = "truffle"',
    'version = "0.7.3"',
    '',
    '[[package]]',
    'name = "truffle-core"',
    'version = "0.7.3"',
    '',
  ].join('\n');

  const updated = synchronizeCargoLock(
    lock,
    new Map([
      ['truffle', '0.7.4'],
      ['truffle-core', '0.7.4'],
    ]),
  );
  assert.match(updated, /name = "truffle"\nversion = "0\.7\.4"/);
  assert.match(updated, /name = "truffle-core"\nversion = "0\.7\.4"/);
});

test('synchronizeCargoLock fails closed when a workspace package is absent', () => {
  assert.throws(
    () => synchronizeCargoLock('', new Map([['truffle', '0.7.4']])),
    /expected exactly one local package named truffle, found 0/,
  );
});

test('synchronizeCargoLock fails closed when a package name is ambiguous', () => {
  const block = '[[package]]\nname = "truffle"\nversion = "0.7.3"\n';
  assert.throws(
    () => synchronizeCargoLock(`${block}\n${block}`, new Map([['truffle', '0.7.4']])),
    /expected exactly one local package named truffle, found 2/,
  );
});

test('synchronizeNapiPackageLock updates both npm lockfile version fields', () => {
  const updated = JSON.parse(
    synchronizeNapiPackageLock(
      JSON.stringify({ version: '0.7.3', packages: { '': { version: '0.7.3' } } }),
      '0.7.4',
    ),
  );
  assert.equal(updated.version, '0.7.4');
  assert.equal(updated.packages[''].version, '0.7.4');
});

test('repository release manifests produce already-synchronized lockfiles', () => {
  const { files } = calculateReleaseLockUpdates();
  for (const [path, expected] of files) {
    assert.equal(readFileSync(new URL(`../${path}`, import.meta.url), 'utf8'), expected);
  }
});
