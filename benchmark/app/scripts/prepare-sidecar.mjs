// Builds the sidecar a bundled app ships next to its executable (RFC 027 §4, §11.6).
//
//   node scripts/prepare-sidecar.mjs                  sidecar for the Rust host triple
//   node scripts/prepare-sidecar.mjs --universal      both darwin triples, plus the lipo'd
//                                                     sidecar-slim-universal-apple-darwin
//   … --tauri <args…>                                 then runs `tauri <args…>` with
//                                                     TRUFFLE_BENCH_GIT_SHA / _DIRTY set, so the
//                                                     bundled app's reports name their commit
//
// Each binary is `CGO_ENABLED=0 go build -ldflags "-s -w"` of packages/sidecar-slim, written to
// src-tauri/binaries/sidecar-slim-<target triple>[.exe] (gitignored), which is where
// `bundle.externalBin` in tauri.bundle.conf.json looks. A plain `cargo build` or `tauri dev`
// needs none of this: they use the dev sidecar (crates/truffle-core/tests/test-sidecar).

import { spawnSync } from 'node:child_process';
import { mkdirSync, statSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const appDir = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const repoRoot = resolve(appDir, '..', '..');
const sidecarSrc = join(repoRoot, 'packages', 'sidecar-slim');
const binDir = join(appDir, 'src-tauri', 'binaries');

function fail(message) {
  console.error(`prepare-sidecar: ${message}`);
  process.exit(1);
}

function run(cmd, args, options = {}) {
  const r = spawnSync(cmd, args, { stdio: 'inherit', ...options });
  if (r.error) fail(`${cmd}: ${r.error.message}`);
  if (r.status !== 0) fail(`${cmd} ${args.join(' ')} exited with ${r.status}`);
}

function output(cmd, args, options = {}) {
  const r = spawnSync(cmd, args, { encoding: 'utf8', ...options });
  return r.status === 0 ? r.stdout.trim() : null;
}

/** The Rust host triple: `rustc --print host-tuple`, or the `host:` line of `rustc -vV`. */
function hostTriple() {
  const tuple = output('rustc', ['--print', 'host-tuple']);
  if (tuple) return tuple;
  const verbose = output('rustc', ['-vV']);
  const host = verbose?.split('\n').find((l) => l.startsWith('host: '));
  if (!host) fail('could not ask rustc for the host triple; is Rust installed?');
  return host.slice('host: '.length).trim();
}

/** GOOS / GOARCH for a Rust target triple. */
function goTarget(triple) {
  const [arch] = triple.split('-');
  const GOARCH = { aarch64: 'arm64', x86_64: 'amd64', i686: '386', armv7: 'arm' }[arch];
  const GOOS = triple.includes('apple-darwin')
    ? 'darwin'
    : triple.includes('windows')
      ? 'windows'
      : triple.includes('linux')
        ? 'linux'
        : null;
  if (!GOARCH || !GOOS) fail(`no Go target for ${triple}`);
  return { GOOS, GOARCH };
}

function sidecarPath(triple) {
  return join(binDir, `sidecar-slim-${triple}${triple.includes('windows') ? '.exe' : ''}`);
}

function buildSidecar(triple) {
  const out = sidecarPath(triple);
  const { GOOS, GOARCH } = goTarget(triple);
  console.log(`prepare-sidecar: ${GOOS}/${GOARCH} → ${out}`);
  run('go', ['build', '-ldflags', '-s -w', '-o', out, '.'], {
    cwd: sidecarSrc,
    env: { ...process.env, CGO_ENABLED: '0', GOOS, GOARCH },
  });
  return out;
}

function gitEnv() {
  const sha = output('git', ['-C', repoRoot, 'rev-parse', 'HEAD']);
  const status = output('git', ['-C', repoRoot, 'status', '--porcelain', '--untracked-files=no']);
  const env = {};
  if (sha) env.TRUFFLE_BENCH_GIT_SHA = sha;
  if (status !== null) env.TRUFFLE_BENCH_GIT_DIRTY = status === '' ? 'false' : 'true';
  return env;
}

const argv = process.argv.slice(2);
const tauriAt = argv.indexOf('--tauri');
const own = tauriAt === -1 ? argv : argv.slice(0, tauriAt);
const tauriArgs = tauriAt === -1 ? null : argv.slice(tauriAt + 1);
const unknown = own.filter((a) => a !== '--universal');
if (unknown.length) fail(`unknown option ${unknown[0]} (use --universal and/or --tauri <args…>)`);

mkdirSync(binDir, { recursive: true });
const written = [];
if (own.includes('--universal')) {
  if (process.platform !== 'darwin') fail('--universal needs macOS (lipo)');
  const arm = buildSidecar('aarch64-apple-darwin');
  const intel = buildSidecar('x86_64-apple-darwin');
  const universal = sidecarPath('universal-apple-darwin');
  run('lipo', ['-create', '-output', universal, arm, intel]);
  console.log(`prepare-sidecar: lipo → ${universal}`);
  written.push(arm, intel, universal);
} else {
  written.push(buildSidecar(hostTriple()));
}
for (const f of written) {
  try {
    if (!statSync(f).isFile()) fail(`${f} is not a file`);
  } catch {
    fail(`${f} was not written`);
  }
}

if (tauriArgs) {
  const env = { ...process.env, ...gitEnv() };
  console.log(
    `prepare-sidecar: tauri ${tauriArgs.join(' ')} (git ${env.TRUFFLE_BENCH_GIT_SHA?.slice(0, 12) ?? 'unknown'}${
      env.TRUFFLE_BENCH_GIT_DIRTY === 'true' ? ', dirty' : ''
    })`,
  );
  const bin = join(
    appDir,
    'node_modules',
    '.bin',
    process.platform === 'win32' ? 'tauri.cmd' : 'tauri',
  );
  run(bin, tauriArgs, { cwd: appDir, env, shell: process.platform === 'win32' });
}
