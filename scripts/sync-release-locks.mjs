#!/usr/bin/env node

import { readFileSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const root = join(dirname(fileURLToPath(import.meta.url)), '..');

const cargoPackages = new Map([
  ['truffle', 'crates/truffle/Cargo.toml'],
  ['truffle-cli', 'crates/truffle-cli/Cargo.toml'],
  ['truffle-core', 'crates/truffle-core/Cargo.toml'],
  ['truffle-napi', 'crates/truffle-napi/Cargo.toml'],
  ['truffle-sidecar', 'crates/truffle-sidecar/Cargo.toml'],
]);

function escapeRegExp(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function manifestVersion(content, path) {
  const version = content.match(/^version = "([^"]+)"/m)?.[1];
  if (!version) throw new Error(`${path}: package version not found`);
  return version;
}

export function synchronizeCargoLock(content, versions) {
  let updated = content;

  for (const [name, version] of versions) {
    const pattern = new RegExp(
      `(\\[\\[package\\]\\]\\r?\\nname = "${escapeRegExp(name)}"\\r?\\nversion = ")[^"]+(")`,
      'g',
    );
    let matches = 0;
    updated = updated.replace(pattern, (_match, prefix, suffix) => {
      matches += 1;
      return `${prefix}${version}${suffix}`;
    });
    if (matches !== 1) {
      throw new Error(
        `Cargo.lock: expected exactly one local package named ${name}, found ${matches}`,
      );
    }
  }

  return updated;
}

export function synchronizeNapiPackageLock(content, version) {
  const lock = JSON.parse(content);
  if (!lock.packages?.['']) {
    throw new Error('crates/truffle-napi/package-lock.json: root package entry is missing');
  }
  lock.version = version;
  lock.packages[''].version = version;
  return `${JSON.stringify(lock, null, 2)}\n`;
}

export function calculateReleaseLockUpdates(
  read = (path) => readFileSync(join(root, path), 'utf8'),
) {
  const releaseVersion = JSON.parse(read('packages/core/package.json')).version;
  const versions = new Map();

  for (const [name, path] of cargoPackages) {
    const version = manifestVersion(read(path), path);
    if (version !== releaseVersion) {
      throw new Error(`${path}: expected release version ${releaseVersion}, found ${version}`);
    }
    versions.set(name, version);
  }

  const cargoLock = read('Cargo.lock');
  const napiLock = read('crates/truffle-napi/package-lock.json');
  return {
    releaseVersion,
    files: new Map([
      ['Cargo.lock', synchronizeCargoLock(cargoLock, versions)],
      [
        'crates/truffle-napi/package-lock.json',
        synchronizeNapiPackageLock(napiLock, releaseVersion),
      ],
    ]),
  };
}

function main() {
  const checkOnly = process.argv.includes('--check');
  const { releaseVersion, files } = calculateReleaseLockUpdates();
  const changed = [];

  for (const [path, expected] of files) {
    const absolutePath = join(root, path);
    const current = readFileSync(absolutePath, 'utf8');
    if (current === expected) continue;
    changed.push(path);
    if (!checkOnly) writeFileSync(absolutePath, expected);
  }

  if (checkOnly && changed.length > 0) {
    console.error(`[release:locks] generated lockfiles are stale for ${releaseVersion}:`);
    for (const path of changed) console.error(`  - ${path}`);
    console.error('Run `pnpm release:prepare` and commit the result before releasing.');
    process.exit(1);
  }

  if (changed.length === 0) {
    console.log(`[release:locks] generated lockfiles are current for ${releaseVersion}`);
  } else {
    console.log(`[release:locks] synchronized ${changed.join(', ')} for ${releaseVersion}`);
  }
}

const invokedPath = process.argv[1] ? pathToFileURL(process.argv[1]).href : undefined;
if (invokedPath === import.meta.url) main();
