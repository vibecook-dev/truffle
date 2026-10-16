// A browser stand-in for the Rust side, for `vite dev`, design work and screenshots (RFC 027 §7).
// Keep it in this one file.
//
// Everything with a shape comes from the engine's generated fixtures (benchmark/schema/fixtures):
// suite profiles are the engine's, and every report is built on the engine's example of the same
// workload and mode (api/examples.ts), so its summary keys, labels, units, series and results
// fields are the engine's, with simulated numbers laid over them. mock.test.ts checks every mode
// against its example.
//
// It simulates: the sign-in flow (a fake auth URL that "approves" a few seconds after Open);
// two peers on your account, one on another account and one offline; runs of all ten
// workloads that stream the engine's live keys at 4 Hz with plausible numbers; a history with
// failed and cancelled runs, a debug build, a run that couldn't reach its target rate, relayed
// and mixed paths, same-host and same-process runs, CLI and imported v2/tsnet-bench reports,
// compare pairs with the same and different keys, and two suites. Imports and drops add
// imported reports.
//
// Dev-only URL switches (all optional):
//   ?mock=fresh | connecting | needs-login | needs-approval | error
//                        sign-in state at launch (default: signed in)
//   ?mock_elapsed=40     seconds already spent on the login URL (or waiting for approval)
//   ?approval=1          the tailnet requires device approval: sign-in waits ~6 s in needs-approval
//   ?active=tcp          a run of that workload is already going (exercises runs_active/run_attach)
//   ?active_at=6         …and has been for this many seconds
//   ?active_suite=smoke  a suite is already going (exercises reattaching to a suite)
//   ?measured=1          another device is measuring this one
//   ?peers=none | one    no peers, or a single online peer
//   ?build=debug         the app reports a debug build
//   ?peer_build=debug    studio (the default target) runs a debug build
//   ?fail=tcp            the next run of that workload fails part way (the peer stops answering)
//   ?theme=dark | light  initial theme setting
//   ?history=none        start with no runs
//   ?real=1              dev only: History serves the real reports in benchmark/app/dev-reports/
//   ?real_dir=two-machine  …only those in that subfolder

import type { Api, Subscription, Unsubscribe } from './api';
import type {
  ActiveRun,
  AppError,
  AppInfo,
  BenchPeer,
  BuildProfile,
  Direction,
  Distribution,
  EgressResults,
  FileTransferIteration,
  FileTransferResults,
  HttpResults,
  HubEvent,
  NetworkInfo,
  NodeEnv,
  NodeState,
  PathPoint,
  PathResults,
  PeerHello,
  Phase,
  Progress,
  QuicLatencyResults,
  QuicThroughputResults,
  ReceiverStats,
  ResourceStats,
  RunFlag,
  RunReport,
  RunSummary,
  Sample,
  Series,
  Settings,
  SuiteProfile,
  SuiteProgress,
  SuiteResult,
  SummaryItem,
  SyncedStoreBurstResults,
  SyncedStorePingpongResults,
  TcpLatencyResults,
  TcpThroughputResults,
  ThemeSetting,
  Tool,
  Topology,
  UdpEchoResults,
  UdpRampResults,
  UdpRampStep,
  UdpStreamResults,
  VideoFrameState,
  VideoResults,
  WorkloadKind,
  WorkloadParams,
  WsRrResults,
  WsStreamResults,
} from './types';
import { KEY_LOADED_RTT, PROTOCOL } from './types';
import { exampleFor } from './examples';
import suiteFull from '../../../schema/fixtures/suite-profile-full.json';
import suiteNightly from '../../../schema/fixtures/suite-profile-nightly.json';
import suiteSmoke from '../../../schema/fixtures/suite-profile-smoke.json';
import suiteStandard from '../../../schema/fixtures/suite-profile-standard.json';
import {
  REGISTRY,
  WORKLOAD_ORDER,
  modeOf,
  paramsLabel,
  validateParams,
  videoFps,
  videoMbps,
  videoPFrameBytes,
} from '../workloads/registry';
import { keyedParams } from '../lib/compare';

// ---------------------------------------------------------------- switches

const q = new URLSearchParams(
  typeof window !== 'undefined' && window.location ? window.location.search : '',
);
const SCENARIO = q.get('mock') ?? 'signed-in';
const ELAPSED = Number(q.get('mock_elapsed') ?? 0);
const APPROVAL = q.get('approval') === '1';
const ACTIVE = q.get('active') as WorkloadKind | null;
const ACTIVE_AT = Number(q.get('active_at') ?? 6);
const ACTIVE_SUITE = q.get('active_suite');
const MEASURED = q.get('measured') === '1';
const PEERS_MODE = q.get('peers') ?? 'all';
const BUILD: BuildProfile = q.get('build') === 'debug' ? 'debug' : 'release';
const PEER_BUILD: BuildProfile = q.get('peer_build') === 'debug' ? 'debug' : 'release';
const NO_HISTORY = q.get('history') === 'none';
/** Dev only: serve the real reports in benchmark/app/dev-reports/ (see vite.config.ts). */
const REAL = import.meta.env.DEV && q.get('real') === '1';
/** With ?real=1: only the reports in this subfolder of dev-reports/ (e.g. ?real_dir=two-machine). */
const REAL_DIR = q.get('real_dir');
let FAIL = q.get('fail') as WorkloadKind | null;
const THEME = (
  ['light', 'dark'].includes(q.get('theme') ?? '') ? q.get('theme') : 'system'
) as ThemeSetting;

// ---------------------------------------------------------------- randomness

function hashStr(s: string): number {
  let h = 0x811c9dc5;
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return h >>> 0;
}

function rng(seed: string) {
  let a = hashStr(seed) || 1;
  const next = () => {
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
  return {
    next,
    /** roughly normal, mean 0, sd 1 */
    gauss: () => (next() + next() + next() + next() - 2) * 1.732,
    pick: <T>(xs: T[]) => xs[Math.floor(next() * xs.length)],
  };
}
type Rng = ReturnType<typeof rng>;

function hex(seed: string, n: number): string {
  let out = '';
  let h = hashStr(seed);
  while (out.length < n) {
    h = Math.imul(h ^ (h >>> 13), 0x5bd1e995) >>> 0;
    out += h.toString(16).padStart(8, '0');
  }
  return out.slice(0, n);
}

function uuid(seed: string): string {
  const h = hex(seed, 32);
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-4${h.slice(13, 16)}-a${h.slice(17, 20)}-${h.slice(20, 32)}`;
}

const r1 = (v: number) => Math.round(v * 10) / 10;
const r2 = (v: number) => Math.round(v * 100) / 100;
const r3 = (v: number) => Math.round(v * 1000) / 1000;
const sleep = (ms: number) => new Promise((res) => setTimeout(res, ms));
const iso = (ms: number) => new Date(ms).toISOString();
const median = (xs: number[]) => {
  const s = xs.filter(Number.isFinite).sort((a, b) => a - b);
  return s.length ? s[Math.floor((s.length - 1) / 2)] : null;
};
const mean = (xs: number[]) => (xs.length ? xs.reduce((a, b) => a + b, 0) / xs.length : null);

// ---------------------------------------------------------------- the machines

interface Machine {
  key: string;
  device_name: string;
  hostname: string;
  device_id: string;
  tailscale_id: string;
  ip: string;
  os: string;
  arch: string;
  cpus: number;
  login: string | null;
  app: 'truffle-bench-app' | 'truffle-bench-cli';
  build: BuildProfile;
  fingerprint: string;
  /** idle disco RTT, ms */
  rtt: number;
  /** TCP upload goodput, 1 stream, Mb/s */
  tcp: number;
  quic: number;
  egress: number;
  /** UDP rate where loss passes 2%, Mb/s */
  udpCap: number;
  jitter: number;
  connection: string;
}

const LOGIN = 'james@example.com';
const TAILNET = 'tail4a2c1.ts.net';
/** What this Mac's UDP sender can pace before it falls behind, Mb/s. */
const SENDER_CAP = 420;

const ME: Machine = {
  key: 'me',
  device_name: 'james-mbp',
  hostname: 'truffle-truffle-bench-james-mbp',
  device_id: '01JA8ZQ4M7K2V9X3T6R1P0W5NB',
  tailscale_id: 'nQm4b7CNTRL',
  ip: '100.101.12.34',
  os: 'macos',
  arch: 'aarch64',
  cpus: 12,
  login: LOGIN,
  app: 'truffle-bench-app',
  build: BUILD,
  fingerprint: '3f9a0c51e2d47b86',
  rtt: 0,
  tcp: 0,
  quic: 0,
  egress: 0,
  udpCap: 0,
  jitter: 0,
  connection: '',
};

const STUDIO: Machine = {
  key: 'studio',
  device_name: 'studio',
  hostname: 'truffle-truffle-bench-studio',
  device_id: '01JA90B3K1D6F8H2J4L6N8Q0SV',
  tailscale_id: 'nT2s8wCNTRL',
  ip: '100.88.7.21',
  os: 'macos',
  arch: 'aarch64',
  cpus: 24,
  login: LOGIN,
  app: 'truffle-bench-app',
  build: PEER_BUILD,
  fingerprint: 'a17c44e09b3d5f12',
  rtt: 2.1,
  tcp: 612,
  quic: 286,
  egress: 734,
  udpCap: 380,
  jitter: 0.31,
  connection: 'direct',
};

const EC2: Machine = {
  key: 'ec2',
  device_name: 'ec2-bench',
  hostname: 'truffle-truffle-bench-ec2-bench',
  device_id: '01JA91C7P3R5T7V9X1Z3B5D7FG',
  tailscale_id: 'nE9c2kCNTRL',
  ip: '100.72.140.9',
  os: 'linux',
  arch: 'x86_64',
  cpus: 8,
  login: LOGIN,
  app: 'truffle-bench-cli',
  build: 'release',
  fingerprint: '5d2e91f0c3a87b64',
  rtt: 71.4,
  tcp: 184,
  quic: 97,
  egress: 201,
  udpCap: 160,
  jitter: 1.24,
  connection: 'direct',
};

const ALEX: Machine = {
  ...STUDIO,
  key: 'alex',
  device_name: 'alex-air',
  hostname: 'truffle-truffle-bench-alex-air',
  device_id: '01JA92D9S5V7X9Z1C3E5G7J9KM',
  tailscale_id: 'nA5x1pCNTRL',
  ip: '100.64.33.80',
  login: 'alex@example.org',
  cpus: 8,
  fingerprint: 'c08b1a7735e2d9f4',
  rtt: 3.4,
  connection: 'direct',
};

const MINI: Machine = {
  ...STUDIO,
  key: 'mini',
  device_name: 'old-mini',
  hostname: 'truffle-truffle-bench-old-mini',
  device_id: '01JA93F1V7Y9A1D3F5H7K9M1PQ',
  tailscale_id: 'nM3d6yCNTRL',
  ip: '100.99.4.52',
  arch: 'x86_64',
  cpus: 4,
  fingerprint: '91e7d3a5b0c24f68',
  connection: '',
};

/** A second copy of the app on this Mac (open -n … --profile b): same host. */
const ME_B: Machine = {
  ...ME,
  key: 'me-b',
  device_name: 'james-mbp-b',
  hostname: 'truffle-truffle-bench-james-mbp-b',
  device_id: '01JA94G3X9Z1B3D5F7H9K1M3RT',
  tailscale_id: 'nB7f3qCNTRL',
  ip: '100.101.12.35',
  build: 'release',
  rtt: 0.4,
  tcp: 1840,
  quic: 610,
  egress: 2390,
  udpCap: 900,
  jitter: 0.05,
  connection: 'direct',
};

/**
 * The other hub of a CLI `pair` run: same process. Pair nodes use a per-run app id
 * `truffle-benchpair-<6 hex>` and the device names alpha/beta (RFC 027 §14 #1), so they never
 * look like a real hub.
 */
const PAIR: Machine = {
  ...ME_B,
  key: 'pair',
  device_name: 'beta',
  hostname: 'truffle-truffle-benchpair-4c1e9a-beta',
  app: 'truffle-bench-cli',
};

const MACHINES: Record<string, Machine> = { studio: STUDIO, ec2: EC2, alex: ALEX, mini: MINI };

function hello(m: Machine): PeerHello {
  const sameLogin = m.login === LOGIN;
  return {
    protocol: PROTOCOL,
    app: m.app,
    tool_version: '0.1.0',
    truffle_version: '0.7.17',
    build_profile: m.build,
    git_sha: m.app === 'truffle-bench-cli' ? '3cdfbaa' : null,
    sidecar_sha256: hex(`sidecar-${m.key}`, 64),
    device_name: m.device_name,
    device_id: m.device_id,
    os: m.os,
    arch: m.arch,
    logical_cpus: m.cpus,
    login_name: m.login,
    host_fingerprint: sameLogin ? m.fingerprint : null,
    host_fingerprint_weak: false,
    workloads: [...WORKLOAD_ORDER],
    accepts_runs_from_you: sameLogin,
    busy: null,
  };
}

function benchPeer(m: Machine, online: boolean, rtt: number | null): BenchPeer {
  return {
    id: m.tailscale_id,
    device_id: m.device_id,
    device_name: m.device_name,
    hostname: m.hostname,
    ip: m.ip,
    os: m.os,
    online,
    ws_connected: online,
    connection: online ? m.connection : '',
    login_name: m.login,
    same_login: m.login === LOGIN,
    tagged: false,
    last_seen: online ? null : iso(Date.now() - 3 * 86400_000 - 4 * 3600_000),
    hello: online ? hello(m) : null,
    hello_error: null,
    rtt_ms: online ? rtt : null,
    // the engine's verdict needs both fingerprints: none for a peer offline or not admitted
    same_host: online && m.login === LOGIN ? m.fingerprint === ME.fingerprint : null,
  };
}

// ---------------------------------------------------------------- environments

function nodeEnv(m: Machine, tool: Tool | string): NodeEnv {
  const bundled = m.app === 'truffle-bench-app';
  return {
    device_name: m.device_name,
    os: m.os,
    arch: m.arch,
    logical_cpus: m.cpus,
    tool,
    tool_version: '0.1.0',
    truffle_version: '0.7.17',
    build_profile: m.build,
    rustc: 'rustc 1.89.0 (29483883e 2025-08-04)',
    git_sha: bundled ? null : '3cdfbaa',
    git_dirty: bundled ? null : false,
    sidecar_path: bundled
      ? '/Applications/truffle bench.app/Contents/MacOS/sidecar-slim'
      : m.os === 'linux'
        ? '/home/ec2-user/truffle/crates/truffle-core/tests/test-sidecar'
        : '/Users/james/Projects/truffle/crates/truffle-core/tests/test-sidecar',
    sidecar_sha256: hex(`sidecar-${m.key}`, 64),
    tailscale_ip: m.ip,
    host_fingerprint: m.fingerprint,
    host_fingerprint_weak: false,
    on_battery: m.os === 'macos' ? false : null,
    app_foreground_pct: bundled ? 100 : null,
  };
}

function compareKey(
  p: WorkloadParams,
  topology: Topology,
  path: string,
  buildA: string,
  buildB: string,
  peerFp: string | null,
  methodology = 'v3',
): string {
  const rest = keyedParams(p);
  const canon = JSON.stringify(
    Object.keys(rest)
      .sort()
      .map((k) => [k, rest[k]]),
  );
  return hex(
    `${p.workload}|${canon}|${topology}|${path}|${buildA}|${buildB}|${methodology}|${peerFp}`,
    16,
  );
}

function dist(xs: number[], lost = 0): Distribution {
  const s = xs.filter(Number.isFinite).sort((a, b) => a - b);
  const all = [...s, ...new Array(lost).fill(Infinity)];
  const n = all.length;
  const pct = (p: number) => {
    if (n === 0 || n < Math.ceil(1 / (1 - p) - 1e-9)) return null;
    const v = all[Math.max(1, Math.ceil(p * n)) - 1];
    return Number.isFinite(v) ? r3(v) : null;
  };
  const m = mean(s);
  const sd =
    s.length > 1 && m != null
      ? Math.sqrt(s.reduce((a, b) => a + (b - m) ** 2, 0) / (s.length - 1))
      : null;
  return {
    samples: n,
    lost,
    min: s.length ? r3(s[0]) : null,
    mean: m == null ? null : r3(m),
    p50: pct(0.5),
    p90: pct(0.9),
    p95: pct(0.95),
    p99: pct(0.99),
    p999: pct(0.999),
    max: s.length ? r3(s[s.length - 1]) : null,
    stddev: sd == null ? null : r3(sd),
  };
}

function resources(
  R: Rng,
  kind: 'throughput' | 'light',
  megabytes: number,
  secs: number,
): ResourceStats {
  const cpuAvg = kind === 'throughput' ? 38 + R.next() * 14 : 3 + R.next() * 4;
  const sideAvg = kind === 'throughput' ? 84 + R.next() * 22 : 5 + R.next() * 5;
  const cpuMs = (cpuAvg / 100) * secs * 1000;
  const sideMs = (sideAvg / 100) * secs * 1000;
  return {
    cpu_ms: Math.round(cpuMs),
    cpu_pct_avg: r1(cpuAvg),
    cpu_pct_max: r1(cpuAvg * (1.2 + R.next() * 0.2)),
    rss_mb_max: r1(34 + R.next() * 8),
    cpu_ms_per_mb: megabytes > 0 ? r2(cpuMs / megabytes) : null,
    sidecar_cpu_ms: Math.round(sideMs),
    sidecar_cpu_pct_avg: r1(sideAvg),
    sidecar_cpu_pct_max: r1(sideAvg * (1.15 + R.next() * 0.2)),
    sidecar_rss_mb_max: r1(58 + R.next() * 12),
    sidecar_cpu_ms_per_mb: megabytes > 0 ? r2(sideMs / megabytes) : null,
  };
}

// ---------------------------------------------------------------- workload models
//
// The models only simulate numbers. Which summary keys, series and results fields a report has,
// and their labels and units, come from the engine's example of the same mode (api/examples.ts):
// a model's `results` are laid over the example's (`satisfies Partial<…Results>` keeps their
// names honest), so a mock report always has the engine's shape.

/** A run's plan: phase lengths, and a generator for the data phase. */
interface Plan {
  phases: { phase: Phase; ms: number }[];
  /** Length of warmup + running, the data phase. */
  dataMs: number;
  warmupMs: number;
  model: Model;
}

interface Finished {
  /** Summary values by key; the example decides the keys, labels and order. */
  summary: Record<string, number | null>;
  /** The results fields this run measured, laid over the example's. */
  results: object;
  /** Series points by key; the example decides the keys, labels and units. */
  series: Record<string, [number, number][]>;
  /** The warmup to flag on series that have one. */
  warmupMs: number | null;
  /** In the engine's words (see NOTE). */
  notes: string[];
  timeline: PathPoint[];
  megabytes: number;
  kind: 'throughput' | 'light';
}

interface Model {
  /** Samples for the 250 ms bucket ending at `t` ms into the data phase. */
  tick(t: number, phase: Phase): Sample[];
  /** Summary, results, series and notes once the data phase is over (or cut short at `cutMs`). */
  finish(cutMs: number | null): Finished;
}

/** The engine's notes, word for word (its workload modules and report.rs). */
const NOTE = {
  notALink: 'measures the bridges and netstack, not a link',
  debug: 'debug build: not comparable with release',
  rate: 'sender could not sustain target',
  bidi: 'goodput is the sum of both directions',
  nagle:
    'Nagle exists only on the two Rust loopback sockets. In full, 32 KiB payloads cross the macOS lo0 MTU, so the nodelay A/B is visible there.',
  quic: ['about 1200 B packets, because MTU discovery is off', 'no GSO', 'double encryption'],
  flowControl: 'bounded by quinn default flow-control windows',
  ipv4: 'IPv4 only',
  udpLoss: 'loss is end to end, including loopback relay drops',
  locality:
    'loss locality is an approximation from usermetrics packet counts: other traffic on both sidecars counts too',
  fragments: 'payload > 1252 B fragments in netstack',
  rampLowerBound:
    'the ramp ended before reaching a loss limit: max_sustainable_mbps is a lower bound (raise duration_secs or rate_mbps)',
  wsLossless: 'the WebSocket itself is lossless, so every drop happens at a receiver',
  httpWhois: 'new: one WhoIs per request on both paths',
  videoPacing:
    'pacing applies to UDP fragments; over TCP and QUIC each frame is written whole at its capture time',
  egress:
    "egress removes only the initiator-side Rust bridge, and the responder's sink still uses one. So tcp crosses 2 bridges and egress 1, and egress − tcp is the cost of one bridge.",
};

const upTo = (cutMs: number | null) => (pts: [number, number][]) =>
  pts.filter(([t]) => cutMs == null || t <= cutMs);

/** The 2 Hz path sampler: RTT under load, live as `loaded_rtt_ms`. */
function sampler(R: Rng, m: Machine, loadFactor: number, relayAt: number | null) {
  const timeline: PathPoint[] = [];
  return {
    timeline,
    at(t: number): number | null {
      if (t % 500 !== 0) return null;
      const relayed = relayAt != null && t >= relayAt;
      const base = relayed ? m.rtt + 18 : m.rtt;
      const lost = R.next() < 0.004;
      const rtt = lost
        ? null
        : r2(base * (1 + loadFactor * (0.6 + R.next() * 0.8)) + Math.abs(R.gauss()) * 0.4);
      timeline.push({ t_ms: t, connection: relayed ? 'relay:nyc' : 'direct', rtt_ms: rtt });
      return rtt;
    },
    upTo(cutMs: number | null) {
      return timeline.filter((x) => cutMs == null || x.t_ms <= cutMs);
    },
  };
}

/** A loaded-RTT sample for this tick, if the sampler pinged. */
const loadedRtt = (ps: ReturnType<typeof sampler>, t: number): Sample[] => {
  const rtt = ps.at(t);
  return rtt == null ? [] : [{ key: KEY_LOADED_RTT, value: rtt }];
};

const EPOCH_NS = 1_760_601_600_000_000_000;
const bytesOf = (mbps: number, secs: number) => Math.round((mbps * 1e6 * secs) / 8);

/** Receiver accounting (§8.1) of a goodput series: payload bytes per 250 ms bucket. */
function receiverOf(points: [number, number][], streams: number): ReceiverStats {
  const buckets = points.map(([, v]) => bytesOf(v, 0.25));
  const bytes_total = buckets.reduce((a, b) => a + b, 0);
  const last_ns = EPOCH_NS + points.length * 250_000_000;
  return {
    epoch_ns: EPOCH_NS,
    bucket_ms: 250,
    buckets,
    last_ns,
    bytes_total,
    streams: Array.from({ length: streams }, (_, i) => ({
      stream_idx: i,
      bytes: Math.round(bytes_total / streams),
      first_ns: EPOCH_NS,
      last_ns,
    })),
  };
}

function throughputModel(
  p: Extract<WorkloadParams, { workload: 'tcp' | 'quic' | 'egress' }>,
  m: Machine,
  R: Rng,
  opts: { relayAt?: number | null } = {},
): Model {
  const warm = p.warmup_secs * 1000;
  const streams = p.streams;
  const direction = p.direction;
  // QUIC is quoted at its default 4 streams; TCP and egress at 1 stream
  let base =
    p.workload === 'quic'
      ? m.quic * (Math.min(2.2, 1 + 0.3 * (streams - 1)) / 1.9)
      : (p.workload === 'tcp' ? m.tcp : m.egress) * Math.min(1.55, 1 + 0.22 * (streams - 1));
  if (direction === 'download') base *= 0.95;
  const bidi = direction === 'bidirectional';
  const ps = sampler(R, m, 2.4, opts.relayAt ?? null);
  const up: [number, number][] = [];
  const down: [number, number][] = [];
  const good: [number, number][] = [];
  return {
    tick(t) {
      const k = t / 250;
      const ramp = k <= 1 ? 0.38 : k <= 2 ? 0.71 : k <= 3 ? 0.93 : 1;
      const relayed = opts.relayAt != null && t >= opts.relayAt;
      const dip = R.next() < 0.03 ? 0.82 : 1;
      const v = (relayed ? base * 0.31 : base) * ramp * dip * (1 + R.gauss() * 0.035);
      const out: Sample[] = [];
      if (bidi) {
        const u = v * 0.61;
        const d = v * 0.57 * (1 + R.gauss() * 0.03);
        up.push([t, r1(u)]);
        down.push([t, r1(d)]);
        good.push([t, r1(u + d)]);
        out.push(
          { key: 'upload_mbps', value: r1(u) },
          { key: 'download_mbps', value: r1(d) },
          { key: 'goodput_mbps', value: r1(u + d) },
          { key: 'sent_mbps', value: r1(u * 1.004) },
        );
      } else {
        good.push([t, r1(v)]);
        out.push({ key: 'goodput_mbps', value: r1(v) });
        if (direction !== 'download')
          out.push({ key: 'sent_mbps', value: r1(v * (1.004 + R.next() * 0.01)) });
      }
      return [...out, ...loadedRtt(ps, t)];
    },
    finish(cutMs) {
      const keep = upTo(cutMs);
      const g = keep(good);
      const after = (pts: [number, number][]) => pts.filter(([t]) => t > warm).map(([, v]) => v);
      const goodput = mean(after(g));
      const secs = after(g).length * 0.25;
      const megabytes = goodput ? (goodput * secs) / 8 : 0;
      const goodputBytes = Math.round(megabytes * 1e6);
      const bytesTotal = g.reduce((a, [, v]) => a + bytesOf(v, 0.25), 0);
      const n = streams * (bidi ? 2 : 1);
      const connects = Array.from({ length: n }, () =>
        p.workload === 'quic'
          ? r2(m.rtt * 1.5 + 42 + R.next() * 6)
          : r2(m.rtt * 1.5 + 0.6 + R.next()),
      );
      const per = Math.round(bytesTotal / n);
      const upMbps = bidi ? r1(mean(after(keep(up))) ?? 0) : null;
      const downMbps = bidi ? r1(mean(after(keep(down))) ?? 0) : null;
      const summary: Record<string, number | null> = {
        goodput_mbps: goodput == null ? null : r1(goodput),
        connect_ms: median(connects),
        upload_mbps: upMbps,
        download_mbps: downMbps,
      };
      const dirOf = (i: number): Direction =>
        bidi ? (i < streams ? 'upload' : 'download') : (direction as Direction);
      const notes: string[] = bidi ? [NOTE.bidi] : [];
      let results: object;
      if (p.workload === 'quic') {
        const ceiling = r1((Math.min(streams * 1.25, 10) * 8) / Math.max(m.rtt / 1000, 0.0005));
        if (goodput && goodput >= 0.8 * ceiling) notes.push(NOTE.flowControl);
        notes.unshift(...NOTE.quic);
        results = {
          direction,
          streams,
          connect_ms: connects[0],
          goodput_mbps: r1(goodput ?? 0),
          upload_mbps: upMbps,
          download_mbps: downMbps,
          window_bytes: goodputBytes,
          window_ms: Math.round(secs * 1000),
          bytes_total: bytesTotal,
          received_bytes: bytesTotal,
          sent_bytes: bytesTotal,
          foreign_dropped: 0,
          flow_control_ceiling_mbps: ceiling,
          receiver: receiverOf(bidi ? keep(up) : g, bidi ? streams : n),
          receiver_download: bidi ? receiverOf(keep(down), streams) : null,
          stream_results: connects.map((_, i) => ({
            stream_idx: i,
            kind: dirOf(i),
            sent_bytes: per,
            received_bytes: per,
            acked_bytes: per,
          })),
        } satisfies Partial<QuicThroughputResults>;
      } else {
        const tcp = {
          direction,
          streams: connects.map((c, i) => ({
            stream_idx: i,
            direction: dirOf(i),
            connect_ms: c,
            sent_bytes: per,
            received_bytes: per,
            acked_bytes: per,
          })),
          bytes_total: bytesTotal,
          received_bytes: bytesTotal,
          sent_bytes: bytesTotal,
          goodput_bytes: goodputBytes,
          window_ms: Math.round(secs * 1000),
          connect_ms: dist(connects),
          receiver: bidi ? null : receiverOf(g, n),
          upload: bidi
            ? {
                goodput_mbps: upMbps ?? 0,
                window_bytes: bytesOf(upMbps ?? 0, secs),
                receiver: receiverOf(keep(up), streams),
              }
            : null,
          download: bidi
            ? {
                goodput_mbps: downMbps ?? 0,
                window_bytes: bytesOf(downMbps ?? 0, secs),
                receiver: receiverOf(keep(down), streams),
              }
            : null,
          foreign_dropped: 0,
          nodelay: p.workload === 'tcp' ? p.nodelay : true,
        } satisfies Partial<TcpThroughputResults>;
        results =
          p.workload === 'egress'
            ? ({
                ...tcp,
                bridges: 1,
                route_id: `bench-${hex(`route-${R.next()}`, 16)}`,
              } satisfies Partial<EgressResults>)
            : tcp;
        if (p.workload === 'egress') notes.push(NOTE.egress);
      }
      return {
        summary,
        results,
        series: { goodput_mbps: g, upload_mbps: keep(up), download_mbps: keep(down) },
        warmupMs: warm,
        notes,
        timeline: ps.upTo(cutMs),
        megabytes,
        kind: 'throughput',
      };
    },
  };
}

/** What a count-based model hands its workload to build results from. */
interface Counted {
  dist: Distribution;
  /** Measured samples, ms; lost ones are Infinity. */
  all: number[];
  finite: number[];
  lost: number;
  /** Ping 0 (path). */
  first: number | null;
  timeline: PathPoint[];
  windowMs: number;
}

/** Count-based round trips: tcp/quic latency, udp echo, path pings, ws rr, synced-store. */
function latencyModel(
  m: Machine,
  R: Rng,
  base: number,
  opts: {
    /** Series, results and live key of the distribution (rtt_ms; convergence_ms). */
    key?: string;
    perSampleMs: number;
    count: number;
    warmup: number;
    lossP?: number;
    /** Path pings: ping 0 is reported on its own, every ping is a timeline point. */
    pings?: boolean;
    /** The live keys besides `<key>`: the running p50, loss, a request rate. */
    live: ('p50' | 'loss' | 'rate')[];
    /** A time-based mode (udp echo): the path sampler runs. */
    loaded?: boolean;
    results: (c: Counted) => object;
    summary?: (c: Counted) => Record<string, number | null>;
    notes?: (c: Counted) => string[];
  },
): Model {
  const key = opts.key ?? 'rtt_ms';
  const stem = key.replace(/_ms$/, '');
  const samples: [number, number][] = [];
  const all: number[] = [];
  const tl: PathPoint[] = [];
  const ps = sampler(R, m, 0.2, null);
  let next = 0;
  let done = 0;
  let first: number | null = null;
  return {
    tick(t) {
      const out: number[] = [];
      while (next < t && done < opts.count + opts.warmup) {
        const tail = R.next() < 0.03 ? 1.6 + R.next() * 2.2 : 1;
        let v = base * tail * (1 + Math.abs(R.gauss()) * 0.18);
        if (done === 0 && first == null && opts.pings) {
          v = base * 3.4 + 6;
          first = r3(v);
        }
        const lost = opts.lossP != null && R.next() < opts.lossP;
        if (opts.pings)
          tl.push({
            t_ms: Math.round(next),
            connection: m.connection || 'direct',
            rtt_ms: lost ? null : r3(v),
          });
        if (done >= opts.warmup && !(opts.pings && done === 0)) {
          all.push(lost ? Infinity : v);
          if (!lost) {
            samples.push([Math.round(next), r3(v)]);
            out.push(v);
          }
        }
        done += 1;
        next += opts.perSampleMs * (0.9 + R.next() * 0.2);
      }
      const res: Sample[] = [];
      if (out.length) res.push({ key, value: r3(median(out) as number) });
      if (opts.live.includes('p50') && all.length)
        res.push({ key: `${stem}_p50_ms`, value: r3(median(all) as number) });
      const lostN = all.filter((x) => !Number.isFinite(x)).length;
      if (opts.live.includes('loss') && all.length)
        res.push({ key: 'loss_pct', value: r2((lostN / all.length) * 100) });
      if (opts.live.includes('rate') && all.length)
        res.push({ key: 'msgs_per_sec', value: r1(1000 / opts.perSampleMs) });
      return opts.loaded ? [...res, ...loadedRtt(ps, t)] : res;
    },
    finish(cutMs) {
      const kept = all.slice(
        0,
        cutMs == null ? all.length : Math.max(0, Math.floor(cutMs / opts.perSampleMs)),
      );
      const finite = kept.filter(Number.isFinite);
      const c: Counted = {
        dist: dist(finite, kept.length - finite.length),
        all: kept,
        finite,
        lost: kept.length - finite.length,
        first,
        timeline: tl.filter((x) => cutMs == null || x.t_ms <= cutMs),
        windowMs: Math.round(kept.length * opts.perSampleMs),
      };
      return {
        summary: {
          [`${stem}_p50_ms`]: c.dist.p50,
          [`${stem}_p99_ms`]: c.dist.p99,
          [`${stem}_max_ms`]: c.dist.max,
          [`${stem}_min_ms`]: c.dist.min,
          loss_pct: kept.length ? r2((c.lost / kept.length) * 100) : null,
          ...opts.summary?.(c),
        },
        results: opts.results(c),
        series: { [key]: upTo(cutMs)(samples) },
        warmupMs: null,
        notes: opts.notes?.(c) ?? [],
        timeline: opts.pings ? c.timeline : opts.loaded ? ps.upTo(cutMs) : [],
        megabytes: 0,
        kind: 'light',
      };
    },
  };
}

function udpModel(
  p: Extract<WorkloadParams, { workload: 'udp' }>,
  m: Machine,
  R: Rng,
  relayAt: number | null,
): Model {
  const warm = p.warmup_secs * 1000;
  const ps = sampler(R, m, 0.6, relayAt);
  const good: [number, number][] = [];
  const offered: [number, number][] = [];
  const loss: [number, number][] = [];
  const steps: UdpRampStep[] = [];
  let rate = p.rate_mbps;
  let maxOk = 0;
  let over = 0;
  let stopped: string | null = null;
  let stepStart = 0;
  let seq = 0;
  const closeStep = (t: number, complete: boolean) => {
    const at = (pts: [number, number][]) =>
      mean(pts.filter(([x]) => x > stepStart && x <= t).map(([, v]) => v));
    const off = at(offered);
    const lp = at(loss);
    const endSeq = seq;
    steps.push({
      rate_mbps: r2(rate),
      offered_mbps: off == null ? null : r2(off),
      first_seq: steps.length ? steps[steps.length - 1].end_seq : 0,
      end_seq: endSeq,
      start_ms: stepStart,
      end_ms: t,
      complete,
      achieved: off != null && off >= 0.98 * rate,
      loss_pct: lp == null ? null : r3(lp),
      sustained: off != null && off >= 0.98 * rate && (lp ?? 0) <= 2 && t - stepStart >= 1000,
    });
    stepStart = t;
  };
  return {
    tick(t) {
      if (p.mode === 'ramp' && t > warm && (t - warm) % 3000 === 0 && !stopped) {
        closeStep(t, true);
        rate *= 1.5;
      }
      const cap = relayAt != null && t >= relayAt ? m.udpCap * 0.3 : m.udpCap;
      const off = Math.min(rate, SENDER_CAP) * (0.995 + R.next() * 0.01);
      if (p.mode === 'ramp' && rate > SENDER_CAP && !stopped)
        stopped = `the sender could not reach ${r2(rate)} Mb/s`;
      const frag = p.payload_bytes > 1252 ? 1.8 : 1;
      const lp =
        off <= cap * 0.85
          ? Math.max(0, (0.02 + R.gauss() * 0.02) * frag)
          : Math.max(0, ((off - cap * 0.85) / off) * 100 * frag + R.gauss() * 0.4);
      const g = off * (1 - lp / 100);
      seq += Math.round(((off * 1e6) / 8 / p.payload_bytes) * 0.25);
      good.push([t, r2(g)]);
      offered.push([t, r2(off)]);
      loss.push([t, r3(lp)]);
      if (p.mode === 'ramp' && !stopped) {
        if (lp <= 2 && t > warm) maxOk = Math.max(maxOk, Math.min(rate, off));
        over = lp > 2 ? over + 1 : 0;
        if (over >= 2)
          stopped = `loss above 2% in two consecutive 250 ms buckets at ${r2(rate)} Mb/s`;
      }
      const out: Sample[] = [
        { key: 'goodput_mbps', value: r2(g) },
        { key: 'offered_mbps', value: r2(off) },
        { key: 'loss_pct', value: r3(lp) },
        { key: 'jitter_ms', value: r3(m.jitter * (1 + R.gauss() * 0.1)) },
      ];
      // the sender's target: ticked by the peer on download; on upload only a ramp sets it
      if (p.mode === 'ramp' || p.direction === 'download')
        out.push({ key: 'target_mbps', value: r2(rate) });
      if (p.mode === 'ramp') out.push({ key: 'max_sustainable_mbps', value: r1(maxOk) });
      return [...out, ...loadedRtt(ps, t)];
    },
    finish(cutMs) {
      const keep = upTo(cutMs);
      const after = (pts: [number, number][]) => pts.filter(([t]) => t > warm).map(([, v]) => v);
      const gm = mean(after(keep(good))) ?? 0;
      const lp = mean(after(keep(loss))) ?? 0;
      const off = mean(after(keep(offered))) ?? 0;
      const nBuckets = after(keep(good)).length;
      const megabytes = (gm * nBuckets * 0.25) / 8;
      const sent = Math.round(((off * 1e6) / 8 / p.payload_bytes) * nBuckets * 0.25);
      const unique = Math.round(sent * (1 - lp / 100));
      const achieved = p.mode !== 'stream' || off >= 0.98 * p.rate_mbps;
      const missing = sent - unique;
      if (p.mode === 'ramp' && steps.at(-1)?.end_ms !== (cutMs ?? good.at(-1)?.[0]))
        closeStep(cutMs ?? good.at(-1)?.[0] ?? 0, false);
      // as the engine: a limit is a step that lost > 2% or missed its rate, or the sender giving
      // out; without one the run's length capped the ramp and the result is only a lower bound
      const measured = (s: UdpRampStep) => s.complete || s.end_ms - s.start_ms >= 1000;
      const limitFound =
        stopped != null || steps.some((s) => measured(s) && (!s.achieved || (s.loss_pct ?? 0) > 2));
      const sustained = steps.filter((s) => s.sustained).at(-1)?.rate_mbps ?? null;
      const notes = [NOTE.ipv4, NOTE.udpLoss, NOTE.locality];
      if (p.payload_bytes > 1252) notes.push(NOTE.fragments);
      if (!achieved) notes.push(NOTE.rate);
      if (p.mode === 'ramp') {
        notes.push(`ramp stopped: ${stopped ?? 'the run ended first'}`);
        if (sustained == null) notes.push('no step held loss at or under 2%');
        else if (!limitFound) notes.push(NOTE.rampLowerBound);
      }
      const ppd = p.payload_bytes > 1252 ? 2 : 1;
      const results = {
        direction: p.direction,
        payload_bytes: p.payload_bytes,
        target_mbps: p.rate_mbps,
        sent,
        unique,
        missing,
        dup: 0,
        reordered: Math.round(sent * 0.0004),
        out_of_range: 0,
        bad_datagrams: 0,
        send_errors: 0,
        loss_pct: r3(lp),
        max_loss_burst: lp > 0.5 ? 6 : missing ? 1 : 0,
        jitter_ms: r3(m.jitter),
        goodput_mbps: r2(gm),
        offered_mbps: r2(off),
        rate_achieved: achieved,
        max_burst_datagrams: 3,
        pacer_late_ms_mean: r3(0.04 + R.next() * 0.05),
        sent_bytes: sent * p.payload_bytes,
        received_bytes: unique * p.payload_bytes,
        goodput_bytes: Math.round(megabytes * 1e6),
        window_ms: Math.round(nBuckets * 250),
        bucket_loss: keep(loss).map(([, v]) => {
          const s = Math.round(((off * 1e6) / 8 / p.payload_bytes) * 0.25);
          return { sent: s, missing: Math.round((s * v) / 100) };
        }),
        receiver: receiverOf(keep(good), 1),
        loss_locality: {
          local_send_drops: Math.round(missing * 0.08),
          network: Math.round(missing * 0.71),
          local_recv_drops: missing - Math.round(missing * 0.08) - Math.round(missing * 0.71),
          packets_per_datagram: ppd,
        },
        drained_by: 'last_seq',
      } satisfies Partial<UdpStreamResults>;
      return {
        summary: {
          goodput_mbps: r2(gm),
          offered_mbps:
            p.mode === 'ramp' ? r2(Math.max(...keep(offered).map(([, v]) => v), 0)) : r2(off),
          loss_pct: r3(lp),
          jitter_ms: r3(m.jitter),
          max_sustainable_mbps: p.mode === 'ramp' && limitFound ? sustained : null,
        },
        results:
          p.mode === 'ramp'
            ? ({
                ...results,
                steps,
                stop_reason: stopped ?? 'the run ended first',
                max_sustainable_mbps: sustained,
                limit_found: limitFound,
              } satisfies Partial<UdpRampResults>)
            : results,
        series: { goodput_mbps: keep(good), offered_mbps: keep(offered), loss_pct: keep(loss) },
        warmupMs: warm,
        notes,
        timeline: ps.upTo(cutMs),
        megabytes,
        kind: 'throughput',
      };
    },
  };
}

function videoModel(p: Extract<WorkloadParams, { workload: 'video' }>, m: Machine, R: Rng): Model {
  const fps = videoFps(p);
  const warm = p.warmup_secs * 1000;
  const total = (p.warmup_secs + p.duration_secs) * fps;
  const P = videoPFrameBytes(p);
  const frames: {
    frame_no: number;
    keyframe: boolean;
    bytes: number;
    latency: number | null;
    spread: number;
    state: VideoFrameState;
  }[] = [];
  const lat: [number, number][] = [];
  const good: [number, number][] = [];
  const ps = sampler(R, m, 0.4, null);
  const uncertainty = r2(m.rtt / 2 + 0.05);
  const capacity = p.transport === 'quic' ? m.quic : p.transport === 'tcp' ? m.tcp : m.udpCap;
  let nextFrame = 0;
  let minLat = Infinity;
  const offeredOf = (n: number) => {
    let bytes = 0;
    for (let k = 0; k < n; k++) bytes += k % p.gop === 0 ? P * p.keyframe_ratio : P;
    return n ? (bytes * 8) / (n / fps) / 1e6 : null;
  };
  const isWarm = (f: { frame_no: number }) => (f.frame_no * 1000) / fps < warm;
  const counted = () => frames.filter((f) => !isWarm(f));
  const steady = offeredOf(total) ?? videoMbps(p);
  const freezesOf = (fr: typeof frames) => {
    let freezes = 0;
    let run = 0;
    let ms = 0;
    for (const f of fr) {
      if (f.state !== 'on-time') run += 1;
      else {
        if (run >= 2) {
          freezes += 1;
          ms += (run * 1000) / fps;
        }
        run = 0;
      }
    }
    if (run >= 2) {
      freezes += 1;
      ms += (run * 1000) / fps;
    }
    return { freezes, ms };
  };
  return {
    tick(t) {
      const before = frames.length;
      while (nextFrame < total && (nextFrame * 1000) / fps < t) {
        const key = nextFrame % p.gop === 0;
        const bytes = key ? P * p.keyframe_ratio : P;
        const tx = ((bytes * 8) / (capacity * 1e6)) * 1000;
        const spike = R.next() < 0.008 ? 60 + R.next() * 120 : 0;
        const spread =
          p.transport === 'udp' && p.pacing === 'paced' ? 1000 / fps / (key ? 1.05 : 1.2) : tx;
        const v =
          m.rtt / 2 +
          tx +
          Math.abs(R.gauss()) * 0.6 +
          spike +
          (p.transport === 'udp' && p.pacing === 'paced' ? spread : 0) +
          1.3;
        const dropped = p.transport === 'udp' && R.next() < (key ? 0.012 : 0.003);
        frames.push({
          frame_no: nextFrame,
          keyframe: key,
          bytes,
          latency: dropped ? null : r2(v),
          spread: r2(spread),
          state: 'on-time',
        });
        if (!dropped) {
          lat.push([Math.round((nextFrame * 1000) / fps), r2(v)]);
          minLat = Math.min(minLat, v);
        }
        nextFrame += 1;
      }
      for (const f of frames.slice(before)) {
        if (f.latency == null) f.state = 'dropped';
        else if (f.latency - minLat > p.jitter_buffer_ms) f.state = 'late';
      }
      const c = counted();
      const late = c.filter((f) => f.state === 'late').length;
      const dropped = c.filter((f) => f.state === 'dropped').length;
      const n = Math.max(1, c.length);
      const g = steady * (1 - dropped / n) * (1 + R.gauss() * 0.02);
      good.push([t, r2(g)]);
      const out: Sample[] = [
        { key: 'frames_done', value: frames.length },
        { key: 'frames_sent', value: frames.length },
        { key: 'delivered_fps', value: r2((fps * (n - late - dropped)) / n) },
        { key: 'late_pct', value: r2((late / n) * 100) },
        { key: 'dropped_pct', value: r2((dropped / n) * 100) },
        { key: 'freezes', value: freezesOf(c).freezes },
        { key: 'goodput_mbps', value: r2(g) },
        { key: 'offered_mbps', value: r2(steady) },
        { key: 'clock_uncertainty_ms', value: uncertainty },
      ];
      const lastLat = lat[lat.length - 1];
      if (lastLat) {
        out.push({ key: 'frame_latency_ms', value: lastLat[1] });
        out.push({
          key: 'frame_latency_p50_ms',
          value: r2(median(lat.map((x) => x[1])) as number),
        });
      }
      return [...out, ...loadedRtt(ps, t)];
    },
    finish(cutMs) {
      const fr = frames.filter((f) => cutMs == null || (f.frame_no * 1000) / fps <= cutMs);
      const c = fr.filter((f) => !isWarm(f));
      const n = Math.max(1, c.length);
      const late = c.filter((f) => f.state === 'late').length;
      const dropped = c.filter((f) => f.state === 'dropped').length;
      const fz = freezesOf(c);
      const latencies = c.filter((f) => f.latency != null).map((f) => f.latency as number);
      const d = dist(latencies);
      const offered = offeredOf(fr.length) ?? videoMbps(p);
      const keep = upTo(cutMs);
      const frags = Math.ceil((P * 1.3) / 1160);
      const notes: string[] = p.transport === 'udp' ? [] : [NOTE.videoPacing];
      return {
        summary: {
          delivered_fps: r2((fps * (n - late - dropped)) / n),
          late_pct: r2((late / n) * 100),
          dropped_pct: r2((dropped / n) * 100),
          freezes: fz.freezes,
          frame_latency_p50_ms: d.p50,
          goodput_mbps: r2(offered * (1 - dropped / n)),
        },
        results: {
          transport: p.transport,
          pacing: p.pacing,
          fps,
          bitrate_mbps: videoMbps(p),
          p_frame_bytes: P,
          i_frame_bytes: P * p.keyframe_ratio,
          frames_total: total,
          warmup_frames: fr.length - c.length,
          frames_sent: fr.length,
          on_time: c.length - late - dropped,
          late,
          dropped,
          late_pct: r2((late / n) * 100),
          dropped_pct: r2((dropped / n) * 100),
          delivered_fps: r2((fps * (n - late - dropped)) / n),
          freezes: fz.freezes,
          freeze_ms: r1(fz.ms),
          frame_latency_ms: d,
          relative_delay_ms: dist(latencies.map((x) => x - Math.min(...latencies))),
          frame_spread_ms: dist(c.map((f) => f.spread)),
          keyframe_complete_ms: dist(
            c.filter((f) => f.keyframe && f.latency != null).map((f) => f.latency as number),
          ),
          samples_ms: latencies.map(r3),
          latency_uncertainty_ms: uncertainty,
          frame_spread_p50_ms: median(c.map((f) => f.spread)),
          keyframe_complete_p50_ms: median(
            c.filter((f) => f.keyframe && f.latency != null).map((f) => f.latency as number),
          ),
          offered_mbps: r2(offered),
          goodput_mbps: r2(offered * (1 - dropped / n)),
          received_bytes: c.filter((f) => f.state !== 'dropped').reduce((a, f) => a + f.bytes, 0),
          sent_bytes: fr.reduce((a, f) => a + f.bytes, 0),
          receiver: receiverOf(keep(good), 1),
          datagrams:
            p.transport === 'udp'
              ? {
                  sent: fr.length * frags,
                  unique: fr.length * frags - dropped,
                  missing: dropped,
                  dup: 0,
                  reordered: 0,
                  send_errors: 0,
                  loss_pct: r3((dropped / Math.max(1, fr.length * frags)) * 100),
                  max_loss_burst: dropped ? 1 : 0,
                }
              : null,
          connect_ms:
            p.transport === 'udp' ? null : r2(m.rtt * 1.5 + (p.transport === 'quic' ? 42 : 0.8)),
          max_send_lag_ms: r2(0.3 + R.next() * 0.6),
          stream_ack: null,
          foreign_dropped: 0,
          frames: fr.map((f) => ({
            frame_no: f.frame_no,
            keyframe: f.keyframe,
            bytes: f.bytes,
            state: f.state,
            latency_ms: f.latency,
            delay_ms: f.latency == null ? null : r2(f.latency - minLat),
            spread_ms: f.spread,
            warmup: isWarm(f),
          })),
          frames_stride: 1,
        } satisfies Partial<VideoResults>,
        series: { frame_latency_ms: keep(lat), goodput_mbps: keep(good) },
        warmupMs: warm,
        notes,
        timeline: ps.upTo(cutMs),
        megabytes: (offered * p.duration_secs) / 8,
        kind: 'throughput',
      };
    },
  };
}

function fileModel(
  p: Extract<WorkloadParams, { workload: 'file-transfer' }>,
  m: Machine,
  R: Rng,
): Model {
  const wire = m.tcp * 0.88;
  const iters: (FileTransferIteration & { end: number })[] = [];
  const exclude = p.iterations >= 3;
  let t0 = 0;
  for (let i = 0; i < p.iterations; i++) {
    const hash = (p.size_bytes / 1e6) * 0.8 * (1 + R.gauss() * 0.04) + (i === 0 ? 6 : 0);
    const neg = m.rtt * 2.6 + 22 + R.next() * 4 + (i === 0 ? 9 : 0);
    const w = wire * (i === 0 ? 0.36 : 1) * (1 + R.gauss() * 0.03);
    const transfer = ((p.size_bytes * 8) / (w * 1e6)) * 1000 + 40;
    const total = hash + neg + transfer + m.rtt;
    t0 += total + 100;
    iters.push({
      index: i + 1,
      file_name: `tb-${hex(`ft-${R.next()}`, 16)}-${i + 1}.bin`,
      bytes: p.size_bytes,
      ok: true,
      ack_ok: true,
      sha256_match: true,
      hash_ms: r2(hash),
      negotiate_ms: r2(neg),
      transfer_ms: r2(transfer),
      elapsed_ms: r2(total),
      wire_mbps: r1((p.size_bytes * 8) / (transfer / 1000) / 1e6),
      e2e_mbps: r1((p.size_bytes * 8) / (total / 1000) / 1e6),
      done_wait_ms: r2(100 + R.next() * 4),
      excluded: exclude && i === 0,
      excluded_reason:
        exclude && i === 0 ? 'the first of 3 or more warms caches and connections' : null,
      error: null,
      t_ms: Math.round(t0),
      end: Math.round(t0),
    });
  }
  return {
    tick(t) {
      // the engine sets each file's numbers when it finishes and its hub re-sends them at 4 Hz
      const done = iters.filter((x) => x.end <= t);
      const last = done.at(-1);
      const out: Sample[] = [{ key: 'iterations_done', value: done.length }];
      if (last)
        out.push(
          { key: 'wire_mbps', value: last.wire_mbps ?? 0 },
          { key: 'e2e_mbps', value: last.e2e_mbps ?? 0 },
          { key: 'hash_ms', value: last.hash_ms ?? 0 },
          { key: 'negotiate_ms', value: last.negotiate_ms ?? 0 },
          { key: 'transfer_ms', value: last.transfer_ms ?? 0 },
        );
      return out;
    },
    finish(cutMs) {
      const kept = iters.filter((x) => cutMs == null || x.end <= cutMs);
      const used = kept.filter((x) => !x.excluded);
      const med = (f: (x: FileTransferIteration) => number | null) =>
        median(used.map(f).filter((v): v is number => v != null));
      const notes: string[] = [];
      const transfer = med((x) => x.transfer_ms);
      if (transfer != null && transfer < 100)
        notes.push(
          `transfers took ${Math.round(transfer)} ms on the wire (median), so the fixed ~40 ms that core's transfer timing shows at every file size (not yet explained) dominates wire_mbps; use larger files for a link figure`,
        );
      return {
        summary: {
          wire_mbps: med((x) => x.wire_mbps),
          e2e_mbps: med((x) => x.e2e_mbps),
          hash_ms: med((x) => x.hash_ms),
          negotiate_ms: med((x) => x.negotiate_ms),
        },
        results: {
          size_bytes: p.size_bytes,
          timed_iterations: used.length,
          ack_failures: 0,
          sent_bytes: p.size_bytes * kept.length,
          received_bytes: p.size_bytes * kept.length,
          source_write_ms: r2(p.size_bytes / 4e6),
          iterations: kept.map(({ end: _end, ...it }) => it),
        } satisfies Partial<FileTransferResults>,
        series: { wire_mbps: kept.map((x) => [x.t_ms, x.wire_mbps ?? 0]) },
        warmupMs: exclude && kept.length ? kept[0].t_ms : null,
        notes,
        timeline: [],
        megabytes: (p.size_bytes * kept.length) / 1e6,
        kind: 'throughput',
      };
    },
  };
}

function wsStreamModel(p: Extract<WorkloadParams, { workload: 'ws' }>, m: Machine, R: Rng): Model {
  const target = p.rate_per_sec ?? Math.min(250000, 1.4e9 / Math.max(64, p.payload_bytes) / 8 / 10);
  const pts: [number, number][] = [];
  const ps = sampler(R, m, 0.8, null);
  const wire = Math.ceil((p.payload_bytes * 4) / 3) + 118;
  let sent = 0;
  let unique = 0;
  let lag = 0;
  let router = 0;
  return {
    tick(t) {
      const v = target * (t <= 250 ? 0.6 : 1) * (1 + R.gauss() * 0.04);
      const n = Math.round(v / 4);
      const overflow = target > 100000 ? Math.round(n * 0.014) : 0;
      const lagged = target > 15000 && R.next() < 0.2 ? Math.round(n * 0.002) : 0;
      sent += n;
      unique += n - overflow - lagged;
      router += overflow;
      lag += lagged;
      const got = v * (1 - (overflow + lagged) / Math.max(1, n));
      pts.push([t, r1(got)]);
      return [
        { key: 'msgs_per_sec', value: r1(got) },
        { key: 'goodput_mbps', value: r2((got * p.payload_bytes * 8) / 1e6) },
        { key: 'drop_pct', value: r2(sent ? ((sent - unique) / sent) * 100 : 0) },
        { key: 'received_msgs', value: unique },
        { key: 'sent_msgs_per_sec', value: r1(v) },
        ...loadedRtt(ps, t),
      ];
    },
    finish(cutMs) {
      const kept = upTo(cutMs)(pts);
      const rate = mean(kept.map(([, v]) => v)) ?? 0;
      const notes = [NOTE.wsLossless];
      if (lag) notes.push(`${lag} messages were dropped by the receiver's lagging subscription`);
      if (router)
        notes.push(
          `${router} messages were lost before the subscription (the envelope router lagged)`,
        );
      const drop = sent ? ((sent - unique) / sent) * 100 : 0;
      return {
        summary: {
          msgs_per_sec: r1(rate),
          goodput_mbps: r2((rate * p.payload_bytes * 8) / 1e6),
          drop_pct: r3(drop),
        },
        results: {
          payload_bytes: p.payload_bytes,
          duration_secs: p.duration_secs,
          rate_per_sec: p.rate_per_sec,
          sent,
          unique,
          missing: sent - unique,
          dup: 0,
          reordered: 0,
          lag_dropped: lag,
          router_dropped: router,
          drop_pct: r3(drop),
          msgs_per_sec: r1(rate),
          goodput_mbps: r2((rate * p.payload_bytes * 8) / 1e6),
          sent_per_sec: r1(sent / Math.max(1, kept.length * 0.25)),
          wire_bytes_per_msg: wire,
          wire_mbps: r2((rate * wire * 8) / 1e6),
          window_ms: kept.length * 250,
          sent_bytes: sent * p.payload_bytes,
          received_bytes: unique * p.payload_bytes,
          bad_payload: 0,
          saw_last_seq: true,
        } satisfies Partial<WsStreamResults>,
        series: { msgs_per_sec: kept },
        warmupMs: null,
        notes,
        timeline: ps.upTo(cutMs),
        megabytes: (sent * p.payload_bytes) / 1e6,
        kind: 'throughput',
      };
    },
  };
}

function httpModel(p: Extract<WorkloadParams, { workload: 'http' }>, m: Machine, R: Rng): Model {
  const warm = p.warmup_secs * 1000;
  // a new tailnet connection costs ~0.5 ms since sidecar protocol 8 (~41 ms, one at a time
  // across workers, before it: the LocalAPI token lookup on every WhoIs)
  const connect = p.connection === 'new' ? 0.5 : 0;
  const lat =
    m.rtt +
    (p.target === 'proxy' ? 0.3 : 0.25) +
    connect +
    ((p.response_bytes * 8) / (m.tcp * 1e6)) * 1000;
  const rate = (Math.min(p.concurrency, 12) / lat) * 1000;
  const pts: [number, number][] = [];
  const latency: number[] = [];
  const ps = sampler(R, m, 0.8, null);
  let attempts = 0;
  return {
    tick(t) {
      const v = rate * (t <= 250 ? 0.6 : 1) * (1 + R.gauss() * 0.04);
      pts.push([t, r1(v)]);
      const bucket: number[] = [];
      for (let i = 0; i < 12; i++) {
        const tail = R.next() < 0.02 ? 2 + R.next() * 3 : 1;
        bucket.push(lat * tail * (1 + Math.abs(R.gauss()) * 0.08));
      }
      if (t > warm) {
        attempts += Math.round(v / 4);
        if (latency.length < 4000) latency.push(...bucket);
      }
      return [
        { key: 'req_per_sec', value: r1(v) },
        { key: 'latency_p50_ms', value: r2(median(bucket) as number) },
        { key: 'error_pct', value: 0 },
        ...loadedRtt(ps, t),
      ];
    },
    finish(cutMs) {
      const kept = upTo(cutMs)(pts);
      const after = kept.filter(([t]) => t > warm).map(([, v]) => v);
      const d = dist(latency);
      const notes: string[] = [];
      if (p.connection === 'new') notes.push(NOTE.httpWhois);
      const connects = p.connection === 'new' ? latency.map((x) => x * 0.96) : [];
      return {
        summary: {
          req_per_sec: r1(mean(after) ?? 0),
          latency_p50_ms: d.p50,
          latency_p99_ms: d.p99,
          error_pct: 0,
        },
        results: {
          attempts,
          ok: attempts,
          errors: { non_2xx: 0, io: 0, timeout: 0 },
          error_kinds: {},
          status_counts: { '200': attempts },
          error_pct: 0,
          req_per_sec: r1(mean(after) ?? 0),
          latency_ms: d,
          samples_ms: latency.slice(0, 4000).map(r3),
          samples_total: latency.length,
          connect_ms: p.connection === 'new' ? median(connects) : null,
          connect: dist(connects),
          connections_opened: p.connection === 'new' ? attempts : p.concurrency,
          body_bytes: attempts * p.response_bytes,
          goodput_mbps: r2(((mean(after) ?? 0) * p.response_bytes * 8) / 1e6),
          warmup_requests: Math.round(rate * p.warmup_secs),
        } satisfies Partial<HttpResults>,
        series: { req_per_sec: kept },
        warmupMs: warm,
        notes,
        timeline: ps.upTo(cutMs),
        megabytes: (attempts * p.response_bytes) / 1e6,
        kind: 'light',
      };
    },
  };
}

function burstModel(
  p: Extract<WorkloadParams, { workload: 'synced-store' }>,
  m: Machine,
  R: Rng,
): Model {
  const write = r3(p.count * 0.0009 * (1 + p.payload_bytes / 8192));
  const echo = r2(m.rtt * 2 + 0.6 + R.next() * 0.6);
  const distinct = Math.round(p.count * (0.24 + R.next() * 0.08));
  return {
    tick: () => [],
    finish: () => ({
      summary: {
        final_echo_ms: echo,
        coalescing_pct: r1((1 - distinct / p.count) * 100),
        burst_write_ms: write,
      },
      results: {
        count: p.count,
        payload_bytes: p.payload_bytes,
        ready_ms: r2(m.rtt * 2 + 1.2),
        readiness_writes: 1,
        burst_write_ms: write,
        final_echo_ms: echo,
        coalescing_pct: r1((1 - distinct / p.count) * 100),
        distinct_versions_seen: distinct,
        echoes_seen: distinct,
        echoes: distinct,
        lagged: 0,
        peer_lagged: 0,
        ws_peers_start: 1,
        ws_peers_end: 1,
        peer_ws_peers_start: 1,
        peer_ws_peers_end: 1,
      } satisfies Partial<SyncedStoreBurstResults>,
      series: {},
      warmupMs: null,
      notes: [],
      timeline: [],
      megabytes: 0,
      kind: 'light',
    }),
  };
}

function planFor(
  p: WorkloadParams,
  m: Machine,
  seed: string,
  opts: { relayAt?: number | null } = {},
): Plan {
  const R = rng(seed);
  const common = (warmupMs: number, runMs: number, model: Model, clock = false): Plan => ({
    phases: [
      { phase: 'preparing', ms: 250 },
      { phase: 'settling', ms: 500 },
      ...(clock ? [{ phase: 'clock' as Phase, ms: 250 }] : []),
      ...(warmupMs ? [{ phase: 'warmup' as Phase, ms: warmupMs }] : []),
      { phase: 'running', ms: runMs },
      { phase: 'draining', ms: 500 },
      ...(clock ? [{ phase: 'clock' as Phase, ms: 250 }] : []),
      { phase: 'finishing', ms: 250 },
    ],
    dataMs: warmupMs + runMs,
    warmupMs,
    model,
  });
  const latencyRun = (count: number, warmup: number, per: number) =>
    Math.max(500, Math.ceil(((count + warmup) * per) / 250) * 250 + 250);
  switch (p.workload) {
    case 'tcp':
    case 'quic':
      if (p.mode === 'throughput')
        return common(
          p.warmup_secs * 1000,
          p.duration_secs * 1000,
          throughputModel(p, m, R, opts),
          p.direction !== 'download',
        );
      {
        const base =
          (m.rtt + (p.workload === 'tcp' ? 0.24 : 0.2)) *
          (1 + p.payload_bytes / 30000) *
          (p.workload === 'tcp' && !p.nodelay && p.payload_bytes > 16384 ? 1.04 : 1);
        const per = base * 1.05;
        const connect = p.workload === 'quic' ? r2(m.rtt * 1.5 + 41) : r2(m.rtt * 1.5 + 0.7);
        return common(
          0,
          latencyRun(p.count, p.warmup_count, per),
          latencyModel(m, R, base, {
            perSampleMs: per,
            count: p.count,
            warmup: p.warmup_count,
            live: ['p50'],
            summary: () => ({ connect_ms: connect }),
            notes: () => (p.workload === 'tcp' ? [NOTE.nagle] : [...NOTE.quic]),
            results: (c) =>
              p.workload === 'tcp'
                ? ({
                    payload_bytes: p.payload_bytes,
                    count: p.count,
                    warmup_count: p.warmup_count,
                    nodelay: p.nodelay,
                    connect_ms: connect,
                    rtt_ms: c.dist,
                    samples_ms: c.finite.map(r3),
                    completed: c.finite.length,
                    sent_bytes: (c.all.length + p.warmup_count) * p.payload_bytes,
                    received_bytes: (c.finite.length + p.warmup_count) * p.payload_bytes,
                    echoed: c.finite.length + p.warmup_count,
                  } satisfies Partial<TcpLatencyResults>)
                : ({
                    connect_ms: connect,
                    payload_bytes: p.payload_bytes,
                    count: p.count,
                    warmup_count: p.warmup_count,
                    completed: c.finite.length,
                    rtt_ms: c.dist,
                    samples_ms: c.finite.map(r3),
                    echoes: c.finite.length + p.warmup_count,
                  } satisfies Partial<QuicLatencyResults>),
          }),
        );
      }
    case 'egress':
      return common(
        p.warmup_secs * 1000,
        p.duration_secs * 1000,
        throughputModel(p, m, R, opts),
        p.direction === 'upload',
      );
    case 'udp':
      if (p.mode === 'echo') {
        const per = 1000 / p.pps;
        const n = Math.round(p.duration_secs * p.pps);
        return common(
          0,
          p.duration_secs * 1000,
          latencyModel(m, R, m.rtt + 0.5, {
            perSampleMs: per,
            count: n - p.warmup_count,
            warmup: p.warmup_count,
            lossP: 0.002,
            live: ['p50', 'loss'],
            loaded: true,
            notes: () => [NOTE.ipv4, NOTE.udpLoss],
            results: (c) =>
              ({
                payload_bytes: p.payload_bytes,
                pps: p.pps,
                warmup_count: p.warmup_count,
                sent: c.all.length,
                completed: c.finite.length,
                lost: c.lost,
                loss_pct: c.all.length ? r3((c.lost / c.all.length) * 100) : null,
                rtt_ms: c.dist,
                samples_ms: c.finite.map(r3),
                offered_pps: r2(p.pps * (0.998 + R.next() * 0.004)),
                responder: {
                  received: c.finite.length + p.warmup_count,
                  echoed: c.finite.length + p.warmup_count,
                  echo_errors: 0,
                  before_start: 0,
                },
              }) satisfies Partial<UdpEchoResults>,
          }),
        );
      }
      return common(
        p.warmup_secs * 1000,
        p.duration_secs * 1000,
        udpModel(p, m, R, opts.relayAt ?? null),
        true,
      );
    case 'path':
      return common(
        0,
        p.count * p.interval_ms,
        latencyModel(m, R, m.rtt, {
          perSampleMs: p.interval_ms,
          count: p.count,
          warmup: 0,
          lossP: 0.004,
          pings: true,
          live: [],
          results: (c) => {
            const connections: Record<string, number> = {};
            for (const x of c.timeline)
              if (x.connection) connections[x.connection] = (connections[x.connection] ?? 0) + 1;
            return {
              sent: c.timeline.length,
              lost: c.lost,
              loss_pct: c.all.length ? r3((c.lost / c.all.length) * 100) : null,
              first_rtt_ms: c.first,
              rtt_ms: c.dist,
              samples: c.timeline.map((x, i) => ({
                i,
                t_ms: x.t_ms,
                connection: x.connection,
                rtt_ms: x.rtt_ms,
                endpoint: m.key === 'ec2' ? '54.210.33.17:41641' : '192.168.1.42:41641',
                error: x.rtt_ms == null ? 'timed out after 5 s' : null,
              })),
              connections,
              transitions: [],
              hub_start_ms: 1840,
              discovery_ms: Math.round(380 + m.rtt * 4),
              ws_connect_ms: Math.round(m.rtt * 3 + 9),
            } satisfies Partial<PathResults>;
          },
        }),
      );
    case 'ws':
      if (p.mode === 'rr') {
        const base = m.rtt + 0.23 + p.payload_bytes / 9000;
        const per = base / Math.min(p.concurrency, 6);
        return common(
          0,
          latencyRun(p.count, p.warmup_count, per),
          latencyModel(m, R, base, {
            perSampleMs: per,
            count: p.count,
            warmup: p.warmup_count,
            live: ['p50', 'rate'],
            summary: () => ({ msgs_per_sec: r1(1000 / per) }),
            results: (c) =>
              ({
                payload_bytes: p.payload_bytes,
                count: p.count,
                warmup_count: p.warmup_count,
                concurrency: p.concurrency,
                sent: c.all.length + p.warmup_count,
                completed: c.finite.length,
                timeouts: c.lost,
                rtt_ms: c.dist,
                samples_ms: c.finite.map(r3),
                msgs_per_sec: r1(1000 / per),
                window_ms: c.windowMs,
                wire_bytes_per_msg: Math.ceil((p.payload_bytes * 4) / 3) + 132,
                lag_dropped: 0,
                peer_lag_dropped: 0,
                bad_replies: 0,
              }) satisfies Partial<WsRrResults>,
          }),
        );
      }
      return common(0, p.duration_secs * 1000, wsStreamModel(p, m, R), true);
    case 'file-transfer': {
      const perFile =
        ((p.size_bytes * 8) / (m.tcp * 0.88 * 1e6)) * 1000 +
        (p.size_bytes / 1e6) * 0.8 +
        m.rtt * 3.6 +
        200;
      return common(0, Math.ceil((p.iterations * perFile) / 250) * 250 + 500, fileModel(p, m, R));
    }
    case 'synced-store': {
      if (p.mode === 'pingpong') {
        const base = (m.rtt + 0.3) / 2;
        const per = base * 2 + 0.2;
        return common(
          0,
          latencyRun(p.count, p.warmup_count, per),
          latencyModel(m, R, base, {
            key: 'convergence_ms',
            perSampleMs: per,
            count: p.count,
            warmup: p.warmup_count,
            live: ['p50'],
            results: (c) =>
              ({
                count: p.count,
                payload_bytes: p.payload_bytes,
                warmup_count: p.warmup_count,
                store_id: `bench-${hex(`store-${R.next()}`, 16)}`,
                ready_ms: r2(m.rtt * 2 + 1.2),
                readiness_writes: 1,
                convergence_ms: c.dist,
                rtt_ms: dist(c.finite.map((x) => x * 2)),
                samples_ms: c.finite.map(r3),
                completed: c.finite.length,
                timeouts: c.lost,
                echoes: c.finite.length + p.warmup_count,
                lagged: 0,
                peer_lagged: 0,
                ws_peers_start: 1,
                ws_peers_end: 1,
                peer_ws_peers_start: 1,
                peer_ws_peers_end: 1,
              }) satisfies Partial<SyncedStorePingpongResults>,
          }),
        );
      }
      return common(0, 1500, burstModel(p, m, R));
    }
    case 'http':
      return common(p.warmup_secs * 1000, p.duration_secs * 1000, httpModel(p, m, R));
    case 'video':
      return common(p.warmup_secs * 1000, p.duration_secs * 1000, videoModel(p, m, R), true);
  }
}

// ---------------------------------------------------------------- reports

/**
 * The engine's `RunReport::flags()`: a debug build, overlap, a missed target rate, imported,
 * the topology, a mixed path, and a failed or cancelled run that kept something.
 */
function flagsOfReport(r: RunReport): RunFlag[] {
  const debug = r.local.build_profile === 'debug' || r.peer?.build_profile === 'debug';
  const partial =
    r.status !== 'ok' &&
    (r.summary.some((s) => s.value != null) || r.series.some((s) => s.points.length > 0));
  const on: [RunFlag, boolean][] = [
    ['debug', debug],
    ['overlap', r.overlapping_runs > 0],
    ['rate-not-achieved', r.notes.includes(NOTE.rate)],
    ['imported', r.tool === 'imported'],
    ['same-host', r.network.topology === 'same-host'],
    ['same-process', r.network.topology === 'same-process'],
    ['mixed-path', r.network.path_label === 'mixed'],
    ['partial', partial],
  ];
  return on.filter(([, x]) => x).map(([f]) => f);
}

/** A deep copy of an example's JSON. */
const clone = <T>(v: T): T => JSON.parse(JSON.stringify(v)) as T;

function buildReport(opts: {
  runId: string;
  suiteId: string | null;
  params: WorkloadParams;
  peer: Machine;
  startedAt: number;
  plan: Plan;
  status: 'ok' | 'failed' | 'cancelled';
  error?: AppError | null;
  cutMs?: number | null;
  tool?: Tool;
  local?: Machine;
  overlapping?: number;
  topology?: Topology;
}): RunReport {
  const { params, peer, plan, status } = opts;
  const local = opts.local ?? ME;
  const topology = opts.topology ?? 'separate-hosts';
  const meta = REGISTRY[params.workload];
  const example = exampleFor(params);
  const out = plan.model.finish(opts.cutMs ?? null);
  const nothing = status !== 'ok' && opts.cutMs === 0;
  const R = rng(`${opts.runId}-env`);
  const timeBased = meta.timeBased(params);
  const transitions = out.timeline.filter(
    (x, i, a) => i > 0 && x.connection !== a[i - 1].connection,
  ).length;
  const relayed = out.timeline.filter((x) => x.connection?.startsWith('relay')).length;
  const derpShare = out.timeline.length ? relayed / out.timeline.length : 0;
  const pathLabel =
    derpShare > 0.99 ? 'relay' : derpShare >= 0.01 || transitions ? 'mixed' : 'direct';
  const notes = [...out.notes];
  if (local.build === 'debug' || peer.build === 'debug') notes.unshift(NOTE.debug);
  if (topology !== 'separate-hosts') notes.unshift(NOTE.notALink);
  if (pathLabel === 'mixed') {
    const tr = out.timeline.find((x, i, a) => i > 0 && x.connection !== a[i - 1].connection);
    notes.push(
      `path changed during the run: direct → ${tr?.connection?.replace('relay:', 'relay ') ?? 'relay'} at ${((tr?.t_ms ?? 0) / 1000).toFixed(1)} s`,
    );
  }
  if ((opts.overlapping ?? 0) > 0)
    notes.push(
      `shared the machine with ${opts.overlapping} other run${opts.overlapping === 1 ? '' : 's'}`,
    );
  const clockUnc = params.workload === 'video' ? r2(peer.rtt / 2 + 0.05) : null;
  const res = resources(R, out.kind, out.megabytes, plan.dataMs / 1000);
  const tl = out.timeline;
  const finiteTl = tl.map((x) => x.rtt_ms).filter((v): v is number => v != null);
  const loaded = timeBased && finiteTl.length ? dist(finiteTl, tl.length - finiteTl.length) : null;
  // the engine leaves p99 out of ws rr and SyncedStore ping-pong summaries below 100 samples
  const fewSamples =
    (params.workload === 'ws' && params.mode === 'rr' && params.count < 100) ||
    (params.workload === 'synced-store' && params.mode === 'pingpong' && params.count < 100);
  const valueOf = (key: string): number | null =>
    key === 'cpu_ms_per_mb'
      ? res.cpu_ms_per_mb
      : key === 'loaded_rtt_p50_ms'
        ? (loaded?.p50 ?? null)
        : (out.summary[key] ?? null);
  const summary: SummaryItem[] = nothing
    ? []
    : example.summary
        .filter((item) => !(fewSamples && /_p99_ms$/.test(item.key)))
        .map((item) => ({
          ...item,
          value: valueOf(item.key),
          uncertainty: item.key === 'frame_latency_p50_ms' ? clockUnc : null,
        }));
  const series: Series[] = nothing
    ? []
    : example.series.map((s) => ({
        ...s,
        points: out.series[s.key] ?? [],
        warmup_ms: s.warmup_ms == null ? null : out.warmupMs,
      }));
  const results = nothing ? {} : { ...clone(example.results as object), ...out.results };
  const settle = Math.round(380 + R.next() * 500);
  const snap = (ms: number, relay = false) => ({
    at: iso(ms),
    connection: relay ? 'relay:nyc' : peer.connection || 'direct',
    rtt_ms: r2((relay ? peer.rtt + 18 : peer.rtt) * (1 + R.next() * 0.2)),
    endpoint: relay ? null : peer.key === 'ec2' ? '54.210.33.17:41641' : '192.168.1.42:41641',
    error: null,
  });
  const lastRelayed = tl[tl.length - 1]?.connection?.startsWith('relay') ?? false;
  const firstRelayed = tl[0]?.connection?.startsWith('relay') ?? false;
  const network: NetworkInfo = {
    topology,
    settle_ms: settle,
    before: snap(opts.startedAt + 300, firstRelayed),
    after: snap(opts.startedAt + plan.dataMs + 1200, lastRelayed),
    clock:
      clockUnc != null
        ? {
            before: {
              offset_ns: 1_284_113,
              delay_ns: Math.round(peer.rtt * 1e6),
              uncertainty_ns: Math.round(clockUnc * 1e6),
              at_ns: 0,
              samples: 16,
            },
            after: {
              offset_ns: 1_290_844,
              delay_ns: Math.round(peer.rtt * 1e6),
              uncertainty_ns: Math.round(clockUnc * 1e6),
              at_ns: plan.dataMs * 1e6,
              samples: 16,
            },
            drift_ppm: 0.6,
            uncertainty_ns: Math.round(clockUnc * 1e6),
          }
        : null,
    path_timeline: tl,
    loaded_rtt_ms: loaded,
    path_bytes: out.megabytes
      ? {
          direct: Math.round(out.megabytes * 1e6 * 2.06 * (1 - derpShare)),
          derp: Math.round(out.megabytes * 1e6 * 2.06 * derpShare),
          peer_relay: 0,
        }
      : null,
    derp_share: out.megabytes ? r3(derpShare) : null,
    path_label: pathLabel,
  };
  const started = opts.startedAt;
  const duration = plan.phases.reduce((a, x) => a + x.ms, 0);
  const tool = opts.tool ?? 'truffle-bench-app';
  const sameProcess = topology === 'same-process';
  return {
    schema_version: 3,
    run_id: opts.runId,
    suite_id: opts.suiteId,
    tool,
    tool_version: '0.1.0',
    truffle_version: '0.7.17',
    methodology: 'v3',
    workload: params.workload,
    label: paramsLabel(params),
    status,
    error: opts.error?.message ?? null,
    error_code: opts.error?.code ?? null,
    started_at: iso(started),
    finished_at: iso(started + duration),
    duration_ms: duration,
    git_sha: tool === 'truffle-bench-app' ? null : '3cdfbaa',
    git_dirty: tool === 'truffle-bench-app' ? null : false,
    compare_key: compareKey(params, topology, pathLabel, local.build, peer.build, peer.fingerprint),
    local: nodeEnv(local, tool),
    peer: nodeEnv(peer, peer.app),
    network,
    params,
    summary,
    results,
    series,
    resources: sameProcess
      ? {
          local: null,
          peer: null,
          combined: resources(rng(`${opts.runId}-c`), out.kind, out.megabytes, plan.dataMs / 500),
        }
      : {
          local: res,
          peer: resources(rng(`${opts.runId}-peer`), out.kind, out.megabytes, plan.dataMs / 1000),
          combined: null,
        },
    overlapping_runs: opts.overlapping ?? 0,
    notes,
  } as RunReport;
}

/** Run a plan to the end in one go (history). */
function simulate(plan: Plan, cutMs: number | null = null) {
  const end = cutMs ?? plan.dataMs;
  for (let t = 250; t <= end; t += 250)
    plan.model.tick(t, t <= plan.warmupMs ? 'warmup' : 'running');
}

function summaryOf(r: RunReport): RunSummary {
  return {
    run_id: r.run_id,
    suite_id: r.suite_id,
    workload: r.workload,
    mode: modeOf(r.params),
    label: r.label,
    status: r.status,
    error_code: r.error_code,
    started_at: r.started_at,
    peer_name: r.peer?.device_name ?? null,
    path_label: r.network.path_label,
    topology: r.network.topology,
    build_profile:
      r.local.build_profile === 'debug' || r.peer?.build_profile === 'debug' ? 'debug' : 'release',
    compare_key: r.compare_key,
    summary: r.summary,
    overlapping_runs: r.overlapping_runs,
    tool: r.tool,
    flags: flagsOfReport(r),
  };
}

// ---------------------------------------------------------------- history

function withDefaults<K extends WorkloadKind>(
  k: K,
  over: Partial<Extract<WorkloadParams, { workload: K }>> = {},
): WorkloadParams {
  return { ...(REGISTRY[k].defaults as object), ...over } as WorkloadParams;
}

/** An old report normalised to v3 (§7.5): tool imported, a v2 methodology, no series. */
function importedReport(
  seed: string,
  startedMs: number,
  params: WorkloadParams,
  methodology: string,
  label: string,
  key: string,
  value: number,
  unit: string,
  peer: Machine | null,
): RunReport {
  const runId = uuid(`import-${seed}`);
  return {
    schema_version: 3,
    run_id: runId,
    suite_id: null,
    tool: 'imported',
    tool_version: methodology === 'native-tsnet' ? 'tsnet-bench' : '0.0.0',
    truffle_version: '0.7.14',
    methodology,
    workload: params.workload,
    label,
    status: 'ok',
    error: null,
    error_code: null,
    started_at: iso(startedMs),
    finished_at: iso(startedMs + 21000),
    duration_ms: 21000,
    git_sha: 'f3162a2',
    git_dirty: false,
    compare_key: hex(`import-${methodology}-${label}`, 16),
    local: { ...nodeEnv(EC2, 'truffle-bench'), device_name: 'ci-runner', sidecar_path: null },
    peer: peer ? nodeEnv(peer, 'truffle-bench') : null,
    network: {
      topology: 'separate-hosts',
      settle_ms: 0,
      before: {
        at: iso(startedMs),
        connection: 'direct',
        rtt_ms: 0.9,
        endpoint: null,
        error: null,
      },
      after: {
        at: iso(startedMs + 21000),
        connection: 'direct',
        rtt_ms: 1.1,
        endpoint: null,
        error: null,
      },
      clock: null,
      path_timeline: [],
      loaded_rtt_ms: null,
      path_bytes: null,
      derp_share: null,
      path_label: 'unknown',
    },
    params,
    summary: [
      {
        key,
        label:
          key === 'fixed_size_mbps'
            ? 'Fixed-size transfer'
            : key === 'e2e_mbps'
              ? 'End to end'
              : 'Goodput',
        value,
        unit,
        better: 'higher',
        uncertainty: null,
      },
    ],
    results: {
      source: methodology === 'native-tsnet' ? 'tsnet-bench' : 'v2',
      source_file: `${seed}.json`,
      params: { protocol: 'tcp', bytes: 4194304, iterations: 10 },
      results: { throughput_mbps: r1(value / 8) },
    },
    series: [],
    resources: { local: null, peer: null, combined: null },
    overlapping_runs: 0,
    notes:
      methodology === 'native-tsnet'
        ? ['native Go/tsnet baseline']
        : ['imported from a v2 report: MB/s converted to Mb/s'],
  } as RunReport;
}

function seedHistory(): RunReport[] {
  if (NO_HISTORY) return [];
  const now = Date.now();
  const out: RunReport[] = [];
  const add = (
    days: number,
    params: WorkloadParams,
    peer: Machine,
    o: {
      status?: 'ok' | 'failed' | 'cancelled';
      error?: AppError;
      cutMs?: number;
      suite?: string;
      local?: Machine;
      tool?: Tool;
      relayAt?: number;
      overlapping?: number;
      topology?: Topology;
    } = {},
  ) => {
    const runId = uuid(`hist-${out.length}-${params.workload}`);
    const plan = planFor(params, peer, runId, { relayAt: o.relayAt ?? null });
    simulate(plan, o.cutMs ?? null);
    const r = buildReport({
      runId,
      suiteId: o.suite ?? null,
      params,
      peer,
      startedAt: now - days * 86400_000,
      plan,
      status: o.status ?? 'ok',
      error: o.error ?? null,
      cutMs: o.cutMs ?? null,
      local: o.local,
      tool: o.tool,
      overlapping: o.overlapping,
      topology: o.topology,
    });
    if (o.cutMs != null) r.duration_ms = o.cutMs + 1000;
    out.push(r);
  };

  // repeated runs at the defaults against studio, so trends and compare pairs have material
  const days = [9.2, 7.9, 6.1, 5.3, 4.2, 3.1, 2.4, 1.6, 0.9, 0.12];
  days.forEach((d, i) => {
    add(d, withDefaults('tcp'), STUDIO);
    if (i % 2 === 0) add(d + 0.01, withDefaults('path'), STUDIO);
    if (i % 3 === 0) add(d + 0.02, withDefaults('quic'), STUDIO);
    if (i % 3 === 1) add(d + 0.03, withDefaults('ws'), STUDIO);
    if (i % 4 === 0) add(d + 0.04, withDefaults('file-transfer'), STUDIO);
    if (i % 4 === 1) add(d + 0.05, withDefaults('udp'), STUDIO);
    if (i % 5 === 2) add(d + 0.06, withDefaults('http'), STUDIO);
    if (i % 5 === 3) add(d + 0.07, withDefaults('synced-store'), STUDIO);
    if (i % 5 === 4) add(d + 0.08, withDefaults('video'), STUDIO);
    if (i % 4 === 2) add(d + 0.09, withDefaults('egress'), STUDIO);
  });
  // the other modes, once each
  add(3.4, withDefaults('tcp', { mode: 'latency' }), STUDIO);
  add(3.3, withDefaults('tcp', { mode: 'latency', payload_bytes: 32768, nodelay: false }), STUDIO);
  add(3.2, withDefaults('quic', { mode: 'latency' }), STUDIO);
  add(2.9, withDefaults('udp', { mode: 'echo' }), STUDIO);
  add(2.8, withDefaults('udp', { mode: 'ramp', duration_secs: 30 }), STUDIO);
  add(2.7, withDefaults('ws', { mode: 'stream', payload_bytes: 1024 }), STUDIO);
  add(2.65, withDefaults('synced-store', { mode: 'burst', count: 1000 }), STUDIO);
  add(2.6, withDefaults('http', { connection: 'new' }), STUDIO);
  add(2.55, withDefaults('video', { transport: 'quic' }), STUDIO);
  add(2.5, withDefaults('tcp', { direction: 'bidirectional', streams: 2 }), STUDIO);
  // a different compare key against the same peer: 4 streams instead of 1
  add(1.4, withDefaults('tcp', { streams: 4 }), STUDIO);
  add(0.3, withDefaults('tcp', { streams: 4 }), STUDIO);
  // across the country: direct, then a relayed run and one that moved mid-run
  add(2.2, withDefaults('tcp'), EC2);
  add(2.21, withDefaults('path'), EC2);
  add(2.0, withDefaults('udp'), EC2, { relayAt: 6000 });
  add(1.9, withDefaults('tcp'), EC2, { relayAt: 0 });
  // a failed run: the peer stopped answering mid-run
  add(1.1, withDefaults('tcp', { streams: 4 }), EC2, {
    status: 'failed',
    cutMs: 4500,
    error: {
      code: 'timeout',
      message: 'ec2-bench stopped answering during the run (finish timed out after 30 s).',
    },
  });
  // a cancelled ramp
  add(1.0, withDefaults('udp', { mode: 'ramp', duration_secs: 30 }), STUDIO, {
    status: 'cancelled',
    cutMs: 9000,
    error: { code: 'cancelled', message: 'Cancelled.' },
  });
  // a UDP run whose sender couldn't reach the target rate
  add(0.8, withDefaults('udp', { rate_mbps: 1000 }), STUDIO);
  // a debug build of this app
  add(0.7, withDefaults('tcp'), STUDIO, { local: { ...ME, build: 'debug' } });
  // a run that shared the machine with a path probe
  add(0.5, withDefaults('quic', { streams: 1 }), STUDIO, { overlapping: 1 });
  // a second copy of the app on this Mac (same host), and a CLI pair run (same process)
  add(0.45, withDefaults('tcp'), ME_B, { topology: 'same-host' });
  add(0.4, withDefaults('tcp'), PAIR, {
    topology: 'same-process',
    tool: 'truffle-bench-cli',
    local: {
      ...ME,
      device_name: 'alpha',
      hostname: 'truffle-truffle-benchpair-4c1e9a-alpha',
      app: 'truffle-bench-cli',
    },
  });
  // a run from the CLI against studio
  add(0.35, withDefaults('egress'), STUDIO, {
    tool: 'truffle-bench-cli',
    local: { ...ME, device_name: 'james-mbp-cli', app: 'truffle-bench-cli' },
  });

  // the smoke suite two days ago, and the standard suite five days ago with one failure
  const smoke = uuid('suite-smoke-1');
  SUITES[0].items.forEach((p, i) => add(2.6 - i * 0.0005, p, STUDIO, { suite: smoke }));
  const standard = uuid('suite-standard-1');
  SUITES[1].items.forEach((p, i) =>
    add(
      5.05 - i * 0.001,
      p,
      STUDIO,
      p.workload === 'video'
        ? {
            suite: standard,
            status: 'failed',
            cutMs: 3000,
            error: {
              code: 'timeout',
              message: 'studio stopped sending frame stats (finish timed out after 30 s).',
            },
          }
        : { suite: standard },
    ),
  );

  // imported: a Rust v2 transport report from CI, a v2 file transfer and a tsnet-bench baseline
  out.push(
    importedReport(
      'v2-transport',
      now - 8.5 * 86400_000,
      withDefaults('tcp', { streams: 1 }),
      'v2-transport',
      'Transport TCP (v2) · 4 MiB × 10',
      'fixed_size_mbps',
      704.8,
      'Mb/s',
      EC2,
    ),
    importedReport(
      'v2-ft',
      now - 8.4 * 86400_000,
      withDefaults('file-transfer'),
      'v2-file-transfer',
      'File transfer (v2) · 10 MiB × 5',
      'e2e_mbps',
      361.2,
      'Mb/s',
      EC2,
    ),
    importedReport(
      'tsnet',
      now - 8.3 * 86400_000,
      withDefaults('tcp'),
      'native-tsnet',
      'tsnet-bench native TCP · 64 MiB × 5',
      'goodput_mbps',
      941.6,
      'Mb/s',
      null,
    ),
  );

  return out.sort((a, b) => b.started_at.localeCompare(a.started_at));
}

// ---------------------------------------------------------------- suites (§8.4)

// The engine's own profiles (suites.rs), as its schema test wrote them.
const SUITES = [suiteSmoke, suiteStandard, suiteFull, suiteNightly] as unknown as SuiteProfile[];

// ---------------------------------------------------------------- live runs

interface LiveRun {
  id: string;
  suiteId: string | null;
  /** A suite item's profile, index and count (runs_active says them, so a reload can follow). */
  suite: { profile: string; index: number; total: number } | null;
  params: WorkloadParams;
  peer: Machine;
  startedAt: number;
  plan: Plan;
  t: number;
  last: Progress;
  listeners: Set<(p: Progress) => void>;
  timer: number;
  resolve: (r: RunReport) => void;
  promise: Promise<RunReport>;
  cancelled: boolean;
}

// ---------------------------------------------------------------- the API

export function createMockApi(): Api {
  const listeners = new Set<(e: HubEvent) => void>();
  const emit = (e: HubEvent) => listeners.forEach((l) => l(e));
  const history: RunReport[] = REAL ? [] : seedHistory();
  if (REAL)
    void fetch(REAL_DIR ? `/__dev-reports?dir=${encodeURIComponent(REAL_DIR)}` : '/__dev-reports')
      .then((r) => r.json() as Promise<RunReport[]>)
      .then((reports) => {
        history.push(...reports);
        history.sort((a, b) => b.started_at.localeCompare(a.started_at));
      })
      .catch(() => undefined);
  const live = new Map<string, LiveRun>();
  const suiteCancelled = new Set<string>();
  let settings: Settings = {
    device_name: ME.device_name,
    allow_other_logins: false,
    theme: THEME,
    target_peer: PEERS_MODE === 'all' ? STUDIO.device_id : null,
  };

  const visible = (): Machine[] =>
    PEERS_MODE === 'none' ? [] : PEERS_MODE === 'one' ? [STUDIO] : [STUDIO, EC2, ALEX, MINI];
  const online = (m: Machine) => m.key !== 'mini';
  const jitterR = rng('peers');
  const peers = (): BenchPeer[] =>
    visible().map((m) =>
      benchPeer(m, online(m), online(m) ? r2(m.rtt * (0.9 + jitterR.next() * 0.25)) : null),
    );

  const local = () => ({
    device_name: settings.device_name,
    device_id: ME.device_id,
    tailscale_id: ME.tailscale_id,
    tailscale_hostname: `truffle-truffle-bench-${settings.device_name}`,
    tailscale_ip: ME.ip,
    dns_name: `truffle-truffle-bench-${settings.device_name}.${TAILNET}`,
    tailnet: TAILNET,
    login_name: LOGIN,
    app_id: 'truffle-bench',
    health: {
      state: 'Running',
      healthy: true,
      warnings: [],
      key_expiry: iso(Date.now() + 172 * 86400_000),
    },
    sidecar_path: '/Applications/truffle bench.app/Contents/MacOS/sidecar-slim',
    hub_start_ms: 1840,
  });

  let node: NodeState;
  const setNode = (n: Partial<NodeState> & { state: NodeState['state'] }) => {
    node = {
      auth_url: null,
      error: null,
      local: null,
      tailscale_hostname: `truffle-truffle-bench-${settings.device_name}`,
      since_ms: Date.now(),
      ...n,
    };
    emit({ type: 'node', ...node });
  };
  const AUTH_URL = 'https://login.tailscale.com/a/7c1e9f2b04d3a5';

  let peerTimer = 0;
  const goRunning = () => {
    // an already-running run or suite must exist before the UI asks runs_active
    if (ACTIVE && REGISTRY[ACTIVE] && !live.size)
      startRun(uuid(`active-${ACTIVE}`), null, withDefaults(ACTIVE), STUDIO, ACTIVE_AT * 1000);
    if (ACTIVE_SUITE && !live.size) {
      const profile = SUITES.find((x) => x.id === ACTIVE_SUITE);
      if (profile)
        void runSuite(uuid(`active-suite-${ACTIVE_SUITE}`), profile, STUDIO, () => undefined, 3);
    }
    setNode({ state: 'running', local: local() });
    emit({ type: 'peers', peers: peers() });
    window.clearInterval(peerTimer);
    peerTimer = window.setInterval(() => emit({ type: 'peers', peers: peers() }), 2500);
    if (MEASURED) {
      window.setTimeout(
        () =>
          emit({
            type: 'remote-run',
            run_id: uuid('remote'),
            peer: STUDIO.device_name,
            peer_id: STUDIO.tailscale_id,
            workload: 'udp',
            state: 'prepared',
          }),
        400,
      );
    }
  };

  /** ?approval=1: an admin approves the device ~6 s after it starts waiting. */
  const awaitApproval = () => {
    setNode({ state: 'needs-approval' });
    window.setTimeout(() => {
      if (node.state === 'needs-approval') goRunning();
    }, 6000);
  };

  // initial state
  node = {
    state: 'idle',
    auth_url: null,
    error: null,
    local: null,
    tailscale_hostname: null,
    since_ms: Date.now(),
  };
  if (SCENARIO === 'needs-login') {
    node = {
      ...node,
      state: 'needs-login',
      auth_url: AUTH_URL,
      tailscale_hostname: `truffle-truffle-bench-${ME.device_name}`,
      since_ms: Date.now() - ELAPSED * 1000,
    };
  } else if (SCENARIO === 'needs-approval') {
    node = {
      ...node,
      state: 'needs-approval',
      tailscale_hostname: `truffle-truffle-bench-${ME.device_name}`,
      since_ms: Date.now() - ELAPSED * 1000,
    };
  } else if (SCENARIO === 'error') {
    node = {
      ...node,
      state: 'error',
      error: { code: 'timeout', message: 'Tailscale didn’t finish signing in within 5 minutes.' },
      since_ms: Date.now(),
    };
  } else if (SCENARIO === 'connecting') {
    node = {
      ...node,
      state: 'starting',
      tailscale_hostname: `truffle-truffle-bench-${ME.device_name}`,
    };
  } else if (SCENARIO !== 'fresh') {
    // a saved state dir: the app starts on its own
    node = {
      ...node,
      state: 'starting',
      tailscale_hostname: `truffle-truffle-bench-${ME.device_name}`,
    };
    window.setTimeout(goRunning, 900);
  }

  // ---------- runs
  const progress = (
    run: LiveRun,
    phase: Phase,
    samples: Sample[],
    message: string | null = null,
  ): Progress => {
    const total = run.plan.phases.reduce((a, x) => a + x.ms, 0);
    const p: Progress = {
      run_id: run.id,
      suite_id: run.suiteId,
      t_ms: run.t,
      phase,
      fraction: Math.min(1, run.t / total),
      samples,
      message,
    };
    run.last = p;
    run.listeners.forEach((l) => l(p));
    return p;
  };

  const phaseAt = (plan: Plan, t: number): { phase: Phase; start: number } => {
    let acc = 0;
    for (const ph of plan.phases) {
      if (t < acc + ph.ms) return { phase: ph.phase, start: acc };
      acc += ph.ms;
    }
    return { phase: 'done', start: acc };
  };

  const dataStart = (plan: Plan) => {
    let acc = 0;
    for (const ph of plan.phases) {
      if (ph.phase === 'warmup' || ph.phase === 'running') return acc;
      acc += ph.ms;
    }
    return acc;
  };

  function startRun(
    id: string,
    suiteId: string | null,
    params: WorkloadParams,
    peer: Machine,
    fastForward = 0,
    suite: LiveRun['suite'] = null,
  ): LiveRun {
    const plan = planFor(params, peer, id);
    let resolve!: (r: RunReport) => void;
    const promise = new Promise<RunReport>((res) => (resolve = res));
    const run: LiveRun = {
      id,
      suiteId,
      suite,
      params,
      peer,
      startedAt: Date.now() - fastForward,
      plan,
      t: 0,
      last: {
        run_id: id,
        suite_id: suiteId,
        t_ms: 0,
        phase: 'preparing',
        fraction: 0,
        samples: [],
        message: null,
      },
      listeners: new Set(),
      timer: 0,
      resolve,
      promise,
      cancelled: false,
    };
    live.set(id, run);
    const d0 = dataStart(plan);
    let lastPhase: Phase | null = null;
    const failAt =
      FAIL === params.workload ? d0 + Math.round((plan.dataMs * 0.42) / 250) * 250 : null;
    if (failAt != null) FAIL = null;
    const step = () => {
      run.t += 250;
      if (failAt != null && run.t >= failAt) {
        finishRun(run, 'failed', {
          code: 'timeout',
          message: `${peer.device_name} stopped answering during the run (finish timed out after 30 s).`,
        });
        return;
      }
      const { phase } = phaseAt(plan, run.t);
      if (phase === 'done') {
        finishRun(run, 'ok');
        return;
      }
      const inData = phase === 'warmup' || phase === 'running';
      const samples = inData ? plan.model.tick(run.t - d0, phase) : [];
      if (inData || phase !== lastPhase)
        progress(
          run,
          phase,
          samples,
          phase === 'settling' ? 'Waiting for two pings on the same path' : null,
        );
      lastPhase = phase;
    };
    for (let t = 0; t < fastForward; t += 250) step();
    run.timer = window.setInterval(step, 250);
    return run;
  }

  function finishRun(
    run: LiveRun,
    status: 'ok' | 'failed' | 'cancelled',
    error: AppError | null = null,
  ) {
    window.clearInterval(run.timer);
    live.delete(run.id);
    const d0 = dataStart(run.plan);
    const cut = status === 'ok' ? null : Math.max(0, run.t - d0);
    const report = buildReport({
      runId: run.id,
      suiteId: run.suiteId,
      params: run.params,
      peer: run.peer,
      startedAt: run.startedAt,
      plan: run.plan,
      status,
      error,
      cutMs: cut,
    });
    if (status !== 'ok') {
      report.duration_ms = run.t;
      report.finished_at = iso(run.startedAt + run.t);
    }
    history.unshift(report);
    progress(run, status === 'ok' ? 'done' : status, [], error?.message ?? null);
    run.resolve(report);
  }

  /** Run a suite's items one after another, skipping the first `skip` (already done). */
  async function runSuite(
    suiteId: string,
    profile: SuiteProfile,
    m: Machine,
    onProgress: (p: SuiteProgress) => void,
    skip = 0,
  ): Promise<SuiteResult> {
    const runIds: string[] = [];
    let failed = 0;
    for (let i = 0; i < profile.items.length; i++) {
      if (suiteCancelled.has(suiteId)) break;
      const params = profile.items[i];
      const id = uuid(`${suiteId}-${i}-${Date.now()}`);
      runIds.push(id);
      if (i < skip) {
        // items that finished before the window opened
        const plan = planFor(params, m, id);
        simulate(plan);
        history.unshift(
          buildReport({
            runId: id,
            suiteId,
            params,
            peer: m,
            startedAt: Date.now() - (skip - i) * 8000,
            plan,
            status: 'ok',
          }),
        );
        continue;
      }
      const run = startRun(id, suiteId, params, m, 0, {
        profile: profile.id,
        index: i,
        total: profile.items.length,
      });
      run.listeners.add((p) =>
        onProgress({
          suite_id: suiteId,
          index: i,
          total: profile.items.length,
          item_label: paramsLabel(params),
          run: p,
        }),
      );
      const rep = await run.promise;
      if (rep.status === 'failed') failed += 1;
      if (i < profile.items.length - 1 && !suiteCancelled.has(suiteId)) await sleep(1000);
    }
    const cancelled = suiteCancelled.delete(suiteId);
    return { suite_id: suiteId, run_ids: runIds, failed, cancelled };
  }

  const failedReport = (
    req: { run_id: string; params: WorkloadParams },
    peer: Machine,
    error: AppError,
  ): RunReport => {
    const plan = planFor(req.params, peer, req.run_id);
    const report = buildReport({
      runId: req.run_id,
      suiteId: null,
      params: req.params,
      peer,
      startedAt: Date.now(),
      plan,
      status: 'failed',
      error,
      cutMs: 0,
    });
    report.duration_ms = 0;
    history.unshift(report);
    return report;
  };

  const findMachine = (ref: string): Machine | null =>
    Object.values(MACHINES).find((m) =>
      [m.tailscale_id, m.device_id, m.device_name, m.hostname].includes(ref),
    ) ?? null;

  const reject = (code: AppError['code'], message: string) =>
    Promise.reject<never>({ code, message } satisfies AppError);

  /** Imports: each report-like path becomes an imported report; anything else is skipped. */
  const importPaths = (paths: string[]) => {
    let imported = 0;
    let skipped = 0;
    paths.forEach((path, i) => {
      const name = path.split(/[\\/]/).pop() ?? path;
      if (!/\.(json|zip)$/i.test(name) && name.includes('.')) {
        skipped += 1;
        return;
      }
      const tsnet = /tsnet/i.test(name);
      history.unshift(
        importedReport(
          `${name}-${Date.now()}-${i}`,
          Date.now() - (i + 1) * 3600_000,
          withDefaults('tcp'),
          tsnet ? 'native-tsnet' : 'v2-transport',
          tsnet ? `tsnet-bench native TCP · ${name}` : `Transport TCP (v2) · ${name}`,
          tsnet ? 'goodput_mbps' : 'fixed_size_mbps',
          r1(tsnet ? 930 + Math.random() * 30 : 690 + Math.random() * 30),
          'Mb/s',
          tsnet ? null : EC2,
        ),
      );
      imported += 1;
    });
    history.sort((a, b) => b.started_at.localeCompare(a.started_at));
    return { imported, skipped };
  };

  const api: Api = {
    kind: 'mock',
    async appInfo(): Promise<AppInfo> {
      return {
        app_version: '0.1.0',
        truffle_version: '0.7.17',
        data_dir: '/Users/james/Library/Application Support/com.vibecook.truffle-bench',
        profile: null,
        os: 'macos',
        build_profile: BUILD,
        sidecar_path: '/Applications/truffle bench.app/Contents/MacOS/sidecar-slim',
        sidecar_sha256: hex('sidecar-me', 64),
      };
    },
    async uiReady() {},
    async hubState() {
      return node;
    },
    async hubStart(req) {
      if (req.device_name) settings = { ...settings, device_name: req.device_name };
      setNode({ state: 'starting' });
      if (req.auth_key) {
        window.setTimeout(APPROVAL ? awaitApproval : goRunning, 1200);
      } else {
        window.setTimeout(() => {
          if (node.state === 'starting') setNode({ state: 'needs-login', auth_url: AUTH_URL });
        }, 800);
      }
      return node;
    },
    async hubStop() {
      window.clearInterval(peerTimer);
      setNode({ state: 'stopped' });
      return node;
    },
    async hubSignOut() {
      window.clearInterval(peerTimer);
      setNode({ state: 'stopping', local: node.local });
      await sleep(400);
      setNode({ state: 'idle' });
      return node;
    },
    async openUrl(url) {
      if (!url.startsWith('https://')) return reject('invalid-params', 'Only https links open.');
      if (url === AUTH_URL && node.state === 'needs-login') {
        window.setTimeout(() => {
          if (node.state === 'needs-login' && SCENARIO !== 'needs-login')
            (APPROVAL ? awaitApproval : goRunning)();
        }, 4000);
      }
    },
    async openDataDir() {},
    async peersList() {
      return node.state === 'running' ? peers() : [];
    },
    async peerProbe(ref) {
      const m = findMachine(ref);
      if (!m || !online(m)) return reject('peer-offline', `${m?.device_name ?? ref} is offline.`);
      await sleep(350);
      const bp = benchPeer(m, true, r2(m.rtt * (0.95 + Math.random() * 0.15)));
      return {
        peer: bp,
        hello: hello(m),
        path: {
          at: iso(Date.now()),
          connection: m.connection,
          rtt_ms: bp.rtt_ms,
          endpoint: m.key === 'ec2' ? '54.210.33.17:41641' : '192.168.1.42:41641',
          error: null,
        },
      };
    },
    async benchRun(req, onProgress) {
      if (node.state !== 'running') return reject('not-running', 'Sign in first.');
      if (!/^[A-Za-z0-9-]{1,64}$/.test(req.run_id))
        return reject('invalid-params', 'run_id must be 1–64 letters, digits or hyphens.');
      const problem = validateParams(REGISTRY[req.params.workload], req.params);
      if (problem) return reject('invalid-params', problem);
      const m = findMachine(req.peer);
      if (!m)
        return failedReport(req, STUDIO, {
          code: 'peer-not-found',
          message: `No peer called ${req.peer}.`,
        });
      if (!online(m))
        return failedReport(req, m, {
          code: 'peer-offline',
          message: `${m.device_name} is offline.`,
        });
      if (m.login !== LOGIN)
        return failedReport(req, m, {
          code: 'login-refused',
          message: `${m.device_name} only accepts runs from its own account.`,
        });
      if (live.size)
        return failedReport(req, m, {
          code: 'busy',
          message: 'Another run is in progress on this device.',
        });
      const run = startRun(req.run_id, null, req.params, m);
      run.listeners.add(onProgress);
      return run.promise;
    },
    async benchCancel(id) {
      const run = live.get(id) ?? [...live.values()].find((r) => r.suiteId === id);
      if (!run) return false;
      if (run.suiteId === id) suiteCancelled.add(id);
      finishRun(run, 'cancelled', { code: 'cancelled', message: 'Cancelled.' });
      return true;
    },
    async runsActive(): Promise<ActiveRun[]> {
      return [...live.values()].map((r) => ({
        run_id: r.id,
        suite_id: r.suiteId,
        workload: r.params.workload,
        peer_name: r.peer.device_name,
        started_at: iso(r.startedAt),
        last: r.last,
        suite_profile: r.suite?.profile ?? null,
        suite_index: r.suite?.index ?? null,
        suite_total: r.suite?.total ?? null,
      }));
    },
    async runAttach(runId, onProgress) {
      const run = live.get(runId);
      if (!run) return false;
      onProgress(run.last);
      run.listeners.add(onProgress);
      return true;
    },
    async suiteProfiles() {
      return SUITES;
    },
    async suiteRun(req, onProgress) {
      const profile = SUITES.find((s) => s.id === req.profile);
      if (!profile) return reject('invalid-params', `No suite called ${req.profile}.`);
      const m = findMachine(req.peer);
      if (!m || !online(m))
        return reject('peer-offline', `${m?.device_name ?? req.peer} is offline.`);
      if (profile.id === 'nightly' && BUILD === 'debug')
        return reject('invalid-params', 'The nightly suite refuses debug builds.');
      return runSuite(req.suite_id, profile, m, onProgress);
    },
    async workloadDefaults() {
      return WORKLOAD_ORDER.map((k) => REGISTRY[k].defaults);
    },
    async runsList() {
      return history.map(summaryOf);
    },
    async runGet(runId) {
      const r = history.find((x) => x.run_id === runId);
      if (!r) return reject('io', `No report for run ${runId}.`);
      return r;
    },
    async runDelete(runId) {
      const i = history.findIndex((x) => x.run_id === runId);
      if (i >= 0) history.splice(i, 1);
    },
    async runsImport(paths) {
      await sleep(250);
      return importPaths(paths);
    },
    async runsImportPick() {
      await sleep(300);
      // as if two files were picked in the native dialog
      return importPaths([
        '/Users/james/Downloads/bench-results/transport-2026-10-14.json',
        '/Users/james/Downloads/bench-results/tsnet-bench-2026-10-14.json',
      ]);
    },
    async settingsGet() {
      return settings;
    },
    async settingsSet(s) {
      settings = { ...s };
      return settings;
    },
    onEvent(handler): Subscription {
      listeners.add(handler);
      return Object.assign(() => void listeners.delete(handler), { ready: Promise.resolve() });
    },
    onFileDrop(handler, onHover): Unsubscribe {
      // the browser stand-in for Tauri's onDragDropEvent: file names stand in for paths
      if (typeof window === 'undefined' || typeof window.addEventListener !== 'function')
        return () => undefined;
      let depth = 0;
      const enter = (e: DragEvent) => {
        if (!e.dataTransfer?.types.includes('Files')) return;
        e.preventDefault();
        depth += 1;
        onHover?.(true);
      };
      const over = (e: DragEvent) => {
        if (e.dataTransfer?.types.includes('Files')) e.preventDefault();
      };
      const leave = () => {
        depth = Math.max(0, depth - 1);
        if (!depth) onHover?.(false);
      };
      const drop = (e: DragEvent) => {
        e.preventDefault();
        depth = 0;
        onHover?.(false);
        const names = [...(e.dataTransfer?.files ?? [])].map((f) => f.name);
        if (names.length) handler(names);
      };
      window.addEventListener('dragenter', enter);
      window.addEventListener('dragover', over);
      window.addEventListener('dragleave', leave);
      window.addEventListener('drop', drop);
      return () => {
        window.removeEventListener('dragenter', enter);
        window.removeEventListener('dragover', over);
        window.removeEventListener('dragleave', leave);
        window.removeEventListener('drop', drop);
      };
    },
  };
  return api;
}
