// One entry per workload: everything a benchmark view needs to draw it. Parameters, bounds and
// defaults mirror RFC 027 §7.3 and the engine's `WorkloadParams::validate()`, which stays
// authoritative. The defaults are the engine's `Default`s: registry.contract.test.ts checks them
// against `params-<workload>.json`, which the engine regenerates from `WorkloadParams::defaults()`.
// KPI keys are the summary keys of §8.2. `paramsLabel()` is a port of the engine's `label()`.
//
// To change a workload's view, change its entry here; BenchmarkFrame, Run detail and Compare
// read only this registry and the report.
//
// Everything an entry names is something the engine emits: KPI keys are summary keys (same
// units), chart series are series keys, live keys are Progress sample keys, facts are dotted
// paths into `results` (typed in api/types.ts: `TcpThroughputResults`, `VideoResults`, …), and
// notes that restate an engine note use its words, so `mergeNotes` says each once.
// registry.contract.test.ts checks all of it against the engine's per-mode example reports.

import {
  KEY_LOADED_RTT,
  type Distribution,
  type EgressParams,
  type FileTransferParams,
  type HttpParams,
  type ParamsOf,
  type PathParams,
  type PathResults,
  type QuicParams,
  type StoreParams,
  type SummaryItem,
  type TcpParams,
  type UdpParams,
  type VideoParams,
  type VideoProfile,
  type WorkloadKind,
  type WorkloadParams,
  type WsParams,
} from '../api/types';
import type { IconName } from '../components/Icon';
import type { Page } from '../hooks/useRoute';
import { fmtBytes } from '../lib/format';

// ---------------------------------------------------------------- shapes

type Text<T> = string | ((p: T) => string);

interface FieldBase<T> {
  key: keyof T & string;
  label: string;
  hint?: Text<T>;
  /** Show the field only when this holds (e.g. latency-only fields). */
  show?: (p: T) => boolean;
}

export interface NumberFieldMeta<T> extends FieldBase<T> {
  kind: 'number';
  min: number;
  max: number;
  unit?: string;
  integer?: boolean;
  /** null is allowed and means "use the default" (shown as the placeholder). */
  nullable?: boolean;
  nullLabel?: Text<T>;
}

export interface EnumFieldMeta<T> extends FieldBase<T> {
  kind: 'enum';
  options: { value: string; label: string }[];
}

export interface BoolFieldMeta<T> extends FieldBase<T> {
  kind: 'bool';
}

export type FieldMeta<T> = NumberFieldMeta<T> | EnumFieldMeta<T> | BoolFieldMeta<T>;

export interface KpiMeta {
  /** Summary key in the report. */
  key: string;
  label: string;
  unit: string;
  /** Progress sample key to show while running; absent = only after the run. */
  live?: string;
  /** What the live value is, under it (default "live"). */
  liveSub?: string;
  digits?: number;
}

export interface SeriesMeta {
  /** Series key in the report and, while running, the live sample key. */
  key: string;
  label: string;
  /** Fill the measured window under it (blueline-wash). */
  area?: boolean;
  /** Live sample key if it differs from the report's series key. */
  liveKey?: string;
}

export interface MainChartMeta {
  /** line (default), iterations (file-transfer bars), burst (synced-store burst phases). */
  kind?: 'line' | 'iterations' | 'burst';
  title: string;
  unit: string;
  series: SeriesMeta[];
  /** Guides drawn under ⌥, from summary keys (their numbers are in the KPI row too). */
  guides?: { label: string; key: string }[];
  /** ± the clock offset uncertainty around the first series, in blueline-wash (video). */
  band?: boolean;
}

/**
 * The panels under the main chart, in order. Wide panels span the row.
 * - cdf: the workload's distribution (results[distribution.key], raw results.samples_ms)
 * - loaded-rtt: CDF of the path sampler's round trips during the run (latency under load)
 * - loss: loss % per 250 ms, loss points in redline
 * - frames: the video frame strip (wide)
 * - phases: file-transfer phases per file
 * - compare: the headline against earlier runs with the same compare_key
 * - bridge: egress against TCP at the same direction, streams, peer and path
 * - line: a second time series
 */
export type Panel =
  | { kind: 'cdf' }
  | { kind: 'loaded-rtt' }
  | { kind: 'loss' }
  | { kind: 'frames' }
  | { kind: 'phases' }
  | { kind: 'compare' }
  /** egress: goodput against the latest TCP run at the same settings (one bridge less) */
  | { kind: 'bridge' }
  | {
      kind: 'line';
      title: string;
      unit: string;
      series: SeriesMeta[];
      /** A reference level from results (e.g. offered_mbps), drawn ink-3 dashed. */
      ref?: { label: string; resultKey: string };
    };

export interface DistMeta {
  /** results key of the Distribution. */
  key: string;
  title: string;
  unit: string;
  /**
   * The raw samples for the CDF, when they aren't `results.samples_ms` (path keeps its pings in
   * `results.samples`); a lost sample is Infinity.
   */
  raw?: (results: unknown) => number[] | null;
}

export interface FactMeta {
  /** Dotted path into results. */
  key: string;
  label: string;
  unit?: string;
  /** count: the length of an array (transitions, steps). */
  format?: 'int' | 'num' | 'bool' | 'bytes' | 'text' | 'count';
  /** Show the row only when this holds for the run's results. */
  when?: (results: unknown) => boolean;
}

export interface Preset<T> {
  id: string;
  label: string;
  params: Partial<T>;
}

interface TypedMeta<K extends WorkloadKind> {
  id: K;
  page: Page;
  /** Rail label. */
  label: string;
  icon: IconName;
  /** One sentence for the view header and the Overview card. */
  blurb: string;
  defaults: ParamsOf<K>;
  presets: Preset<ParamsOf<K>>[];
  fields: FieldMeta<ParamsOf<K>>[];
  /** Cross-field rules of validate(); single-field bounds come from `fields`. */
  validate: (p: ParamsOf<K>) => string | null;
  kpis: (p: ParamsOf<K>) => KpiMeta[];
  /** The summary key the Overview card and sparklines use. */
  headline: (p: ParamsOf<K>) => string;
  main: (p: ParamsOf<K>) => MainChartMeta;
  panels: (p: ParamsOf<K>) => Panel[];
  /**
   * The distribution the right pane tabulates; null = round trip under load in a time-based mode,
   * and nothing in a count-based one.
   */
  distribution: (p: ParamsOf<K>) => DistMeta | null;
  /** Counts and timings from results, tabulated in the right pane. */
  facts: (p: ParamsOf<K>) => FactMeta[];
  timeBased: (p: ParamsOf<K>) => boolean;
  /** Rough wall time of a run, for the "takes about" line and live x domains. */
  estimateMs: (p: ParamsOf<K>) => number;
  /** What is measured, first person where the app speaks. */
  measured: (p: ParamsOf<K>) => string[];
  hops: (p: ParamsOf<K>) => string[];
  notes: (p: ParamsOf<K>) => string[];
  /** A notice the view always shows (SyncedStore says it is not a CRDT, §8.5). */
  notice?: { title: string; text: string };
}

/** The registry's erased form; narrow `params` with `params.workload` when you need fields. */
export interface WorkloadMeta {
  id: WorkloadKind;
  page: Page;
  label: string;
  icon: IconName;
  blurb: string;
  defaults: WorkloadParams;
  presets: Preset<Record<string, unknown>>[];
  fields: FieldMeta<Record<string, unknown>>[];
  validate: (p: WorkloadParams) => string | null;
  kpis: (p: WorkloadParams) => KpiMeta[];
  headline: (p: WorkloadParams) => string;
  main: (p: WorkloadParams) => MainChartMeta;
  panels: (p: WorkloadParams) => Panel[];
  distribution: (p: WorkloadParams) => DistMeta | null;
  facts: (p: WorkloadParams) => FactMeta[];
  timeBased: (p: WorkloadParams) => boolean;
  estimateMs: (p: WorkloadParams) => number;
  measured: (p: WorkloadParams) => string[];
  hops: (p: WorkloadParams) => string[];
  notes: (p: WorkloadParams) => string[];
  notice?: { title: string; text: string };
}

function define<K extends WorkloadKind>(m: TypedMeta<K>): WorkloadMeta {
  return m as unknown as WorkloadMeta;
}

/** A field's hint or null label, resolved against the params. */
export function textOf(t: Text<Record<string, unknown>> | undefined, p: WorkloadParams) {
  if (t == null) return undefined;
  return typeof t === 'function' ? t(p as unknown as Record<string, unknown>) : t;
}

// ---------------------------------------------------------------- shared pieces

const SETUP_MS = 2500; // prepare, settle, clock, finish: roughly

const DIRECTIONS3 = [
  { value: 'upload', label: 'Upload' },
  { value: 'download', label: 'Download' },
  { value: 'bidirectional', label: 'Both' },
];
const DIRECTIONS = [
  { value: 'upload', label: 'Upload' },
  { value: 'download', label: 'Download' },
];

const plural = (n: number, one: string, many = `${one}s`) => `${n} ${n === 1 ? one : many}`;

const warmupRule = (p: { warmup_secs: number; duration_secs: number }) =>
  p.warmup_secs + 1 < p.duration_secs
    ? null
    : 'Warmup must be at least 1 s shorter than the duration.';

const durationFields = <T extends { duration_secs: number; warmup_secs: number }>(
  show?: (p: T) => boolean,
  warmupShow?: (p: T) => boolean,
): FieldMeta<T>[] => [
  {
    kind: 'number',
    key: 'duration_secs' as keyof T & string,
    label: 'Duration',
    min: 2,
    max: 60,
    unit: 's',
    integer: true,
    show,
  },
  {
    kind: 'number',
    key: 'warmup_secs' as keyof T & string,
    label: 'Warmup',
    min: 0,
    max: 10,
    unit: 's',
    integer: true,
    hint: 'Not counted in the result.',
    show: warmupShow ?? show,
  },
];

/** RTT under load: the run's p50 afterwards, the path sampler's latest ping while it runs. */
const LOADED_RTT_KPI: KpiMeta = {
  key: 'loaded_rtt_p50_ms',
  label: 'RTT p50 under load',
  unit: 'ms',
  live: KEY_LOADED_RTT,
  liveSub: 'last ping',
};

/** The engine's throughput summary, in its order: goodput, (connect), CPU per MB, RTT under load. */
const throughputKpis = (bidirectional: boolean, connect = true): KpiMeta[] => [
  {
    key: 'goodput_mbps',
    label: bidirectional ? 'Goodput, both ways' : 'Goodput',
    unit: 'Mb/s',
    live: 'goodput_mbps',
  },
  ...(bidirectional
    ? [
        { key: 'upload_mbps', label: 'Upload', unit: 'Mb/s', live: 'upload_mbps' },
        { key: 'download_mbps', label: 'Download', unit: 'Mb/s', live: 'download_mbps' },
      ]
    : connect
      ? [{ key: 'connect_ms', label: 'Connect', unit: 'ms' }]
      : []),
  { key: 'cpu_ms_per_mb', label: 'CPU per MB', unit: 'ms/MB' },
  LOADED_RTT_KPI,
];

const latencyKpis: KpiMeta[] = [
  { key: 'rtt_p50_ms', label: 'RTT p50', unit: 'ms', live: 'rtt_p50_ms' },
  { key: 'rtt_p99_ms', label: 'RTT p99', unit: 'ms' },
  { key: 'rtt_max_ms', label: 'RTT max', unit: 'ms' },
];

const goodputMain = (p: { direction?: string }): MainChartMeta =>
  p.direction === 'bidirectional'
    ? {
        title: 'Goodput',
        unit: 'Mb/s',
        series: [
          { key: 'upload_mbps', label: 'Upload' },
          { key: 'download_mbps', label: 'Download' },
        ],
      }
    : {
        title: 'Goodput',
        unit: 'Mb/s',
        series: [{ key: 'goodput_mbps', label: 'Goodput', area: true }],
      };

const rttMain: MainChartMeta = {
  title: 'RTT',
  unit: 'ms',
  series: [{ key: 'rtt_ms', label: 'RTT' }],
  guides: [
    { label: 'p50', key: 'rtt_p50_ms' },
    { label: 'p99', key: 'rtt_p99_ms' },
  ],
};

const RTT_DIST: DistMeta = { key: 'rtt_ms', title: 'RTT', unit: 'ms' };

const throughputFacts: FactMeta[] = [
  { key: 'bytes_total', label: 'Payload received', format: 'bytes' },
  { key: 'window_ms', label: 'Measured window', unit: 'ms', format: 'int' },
  { key: 'sent_bytes', label: 'Payload sent', format: 'bytes' },
  { key: 'foreign_dropped', label: 'Foreign connections dropped', format: 'int' },
];

/** Count-based round trips: how many came back, and the one connect before them. */
const latencyFacts: FactMeta[] = [
  { key: 'completed', label: 'Round trips', format: 'int' },
  { key: 'connect_ms', label: 'Connect', unit: 'ms' },
];

/** The hops below the app for anything on a Rust bridge (RFC 027 §8, sidecar data path B). */
const BRIDGE_DOWN = [
  'Loopback into the sidecar’s bridge: a 32 KiB copy loop in Go',
  'gVisor netstack: TCP segmented at the 1280 B tunnel MTU',
  'WireGuard encrypts (+80 B a packet); magicsock sends it',
  'The tailnet path: direct UDP, a peer relay, or DERP over TLS',
  'The peer’s sidecar decrypts, reassembles and copies out',
];

const BRIDGED_TCP_HOPS = [
  'truffle bench writes 64 KiB at a time to a Rust TcpStream',
  ...BRIDGE_DOWN,
  'Loopback bridge out to the peer’s truffle bench, which counts the payload',
];

const WS_HOPS = [
  'The message becomes a JSON envelope; bytes travel as base64 (+33%)',
  'A WebSocket frame on the session’s Rust TcpStream (core doesn’t set TCP_NODELAY)',
  'Loopback into the sidecar, then netstack and WireGuard',
  'The tailnet path to the peer',
  'The peer’s sidecar and loopback bridge',
  'The peer decodes the envelope and routes it by namespace',
];

const UDP_HOPS = [
  'A datagram with a 22 B header, plus a 6 B relay header, to the sidecar’s loopback UDP relay',
  'The sidecar writes it to a tailnet UDP socket in gVisor; above 1252 B it fragments',
  'WireGuard encrypts; magicsock sends it on the tailnet path',
  'The peer’s sidecar reads it and relays it over loopback UDP to truffle bench',
];

const QUIC_HOPS = [
  'quinn (TLS 1.3) on a tailnet UDP socket: one 1200 B packet per send',
  'The sidecar’s loopback UDP relay',
  'gVisor UDP and WireGuard: the data is encrypted twice',
  'The tailnet path',
  'The peer’s relay and quinn, in reverse',
];

export const PROFILES: Record<VideoProfile, { fps: number; mbps: number; label: string }> = {
  '720p30': { fps: 30, mbps: 2.5, label: '720p30' },
  '1080p30': { fps: 30, mbps: 6, label: '1080p30' },
  '1080p60': { fps: 60, mbps: 12, label: '1080p60' },
  '4k30': { fps: 30, mbps: 25, label: '4K30' },
};

export function videoFps(p: VideoParams): number {
  return p.fps ?? PROFILES[p.profile].fps;
}
export function videoMbps(p: VideoParams): number {
  return p.bitrate_mbps ?? PROFILES[p.profile].mbps;
}
/** P-frame bytes: round(b/8 · G/fps / (K + G − 1)), at least 1 (§8.2 video). */
export function videoPFrameBytes(p: VideoParams): number {
  const b = videoMbps(p) * 1e6;
  return Math.max(1, Math.round(((b / 8) * p.gop) / videoFps(p) / (p.keyframe_ratio + p.gop - 1)));
}

// ---------------------------------------------------------------- entries

const path = define<'path'>({
  id: 'path',
  page: 'path',
  label: 'Path',
  icon: 'signal',
  blurb: 'Which path Tailscale uses to the peer, and how long a round trip takes on it.',
  defaults: { workload: 'path', count: 20, interval_ms: 250 } as ParamsOf<'path'>,
  presets: [
    { id: 'quick', label: 'Quick', params: { count: 10, interval_ms: 250 } },
    { id: 'default', label: 'Default', params: { count: 20, interval_ms: 250 } },
    { id: 'minute', label: 'One minute', params: { count: 60, interval_ms: 1000 } },
  ],
  fields: [
    {
      kind: 'number',
      key: 'count',
      label: 'Pings',
      min: 2,
      max: 200,
      integer: true,
      hint: 'The first is reported on its own.',
    },
    {
      kind: 'number',
      key: 'interval_ms',
      label: 'Interval',
      min: 50,
      max: 5000,
      unit: 'ms',
      integer: true,
    },
  ],
  validate: (p: PathParams) =>
    p.count * p.interval_ms <= 300_000 ? null : 'Pings × interval must be 300 s or less.',
  kpis: () => [
    {
      key: 'rtt_p50_ms',
      label: 'RTT p50',
      unit: 'ms',
      live: 'rtt_ms',
      liveSub: 'last ping',
    },
    { key: 'rtt_min_ms', label: 'RTT min', unit: 'ms' },
    { key: 'rtt_max_ms', label: 'RTT max', unit: 'ms' },
    { key: 'loss_pct', label: 'Loss', unit: '%' },
  ],
  headline: () => 'rtt_p50_ms',
  main: () => ({ ...rttMain, title: 'Pings', guides: [{ label: 'p50', key: 'rtt_p50_ms' }] }),
  panels: () => [{ kind: 'cdf' }, { kind: 'compare' }],
  // ping 0 is reported on its own and kept out of the distribution
  distribution: () => ({
    ...RTT_DIST,
    raw: (r) =>
      (r as PathResults | null)?.samples.slice(1).map((x) => x.rtt_ms ?? Infinity) ?? null,
  }),
  facts: () => [
    { key: 'sent', label: 'Pings', format: 'int' },
    { key: 'lost', label: 'Lost (5 s timeout)', format: 'int' },
    { key: 'first_rtt_ms', label: 'First ping', unit: 'ms' },
    { key: 'transitions', label: 'Path changes', format: 'count' },
    { key: 'hub_start_ms', label: 'Hub start to running', unit: 'ms', format: 'int' },
    { key: 'discovery_ms', label: 'Running to this peer listed', unit: 'ms', format: 'int' },
    { key: 'ws_connect_ms', label: 'First hello on a new WebSocket', unit: 'ms', format: 'int' },
  ],
  timeBased: () => false,
  estimateMs: (p) => p.count * p.interval_ms + 1500,
  measured: (p) => [
    `I ping the peer ${p.count} times, ${p.interval_ms} ms apart, with Tailscale’s disco ping. Each ping has 5 s to come back; one that doesn’t counts as lost.`,
    'The first ping is reported on its own, because it can include setting up the path.',
    'I also record how long the hub took to start, to list this peer, and to say hello over a new WebSocket.',
  ],
  hops: () => [
    'truffle bench asks its sidecar for a disco ping',
    'magicsock sends it: no WireGuard, no netstack, no bridges',
    'The tailnet path: direct UDP, a peer relay, or DERP over TLS',
    'The peer’s magicsock answers; the pong comes back the same way',
  ],
  notes: () => [
    'Disco pings measure the path floor: what WireGuard, the netstack and the bridges add shows up in the other workloads.',
    'The path can change during the run; each ping records the one it took.',
  ],
});

const tcp = define<'tcp'>({
  id: 'tcp',
  page: 'tcp',
  label: 'TCP',
  icon: 'tcp',
  blurb:
    'A raw TCP stream to the peer over the tailnet: goodput, or round trips of small messages.',
  defaults: {
    workload: 'tcp',
    mode: 'throughput',
    direction: 'upload',
    streams: 1,
    duration_secs: 10,
    warmup_secs: 1,
    payload_bytes: 64,
    count: 200,
    warmup_count: 10,
    nodelay: true,
  } as ParamsOf<'tcp'>,
  presets: [
    {
      id: 'quick',
      label: 'LAN quick',
      params: { mode: 'throughput', direction: 'upload', streams: 1, duration_secs: 3 },
    },
    {
      id: 'default',
      label: 'Default',
      params: { mode: 'throughput', direction: 'upload', streams: 1, duration_secs: 10 },
    },
    {
      id: 'parallel',
      label: '4 streams',
      params: { mode: 'throughput', direction: 'upload', streams: 4, duration_secs: 10 },
    },
    {
      id: 'latency',
      label: 'Latency',
      params: { mode: 'latency', payload_bytes: 64, count: 200, warmup_count: 10 },
    },
  ],
  fields: [
    {
      kind: 'enum',
      key: 'mode',
      label: 'Mode',
      options: [
        { value: 'throughput', label: 'Throughput' },
        { value: 'latency', label: 'Latency' },
      ],
    },
    {
      kind: 'enum',
      key: 'direction',
      label: 'Direction',
      options: DIRECTIONS3,
      show: (p) => p.mode === 'throughput',
    },
    {
      kind: 'number',
      key: 'streams',
      label: 'Streams',
      min: 1,
      max: 16,
      integer: true,
      hint: (p) => (p.direction === 'bidirectional' ? 'Each way.' : ''),
      show: (p) => p.mode === 'throughput',
    },
    ...durationFields<TcpParams>((p) => p.mode === 'throughput'),
    {
      kind: 'number',
      key: 'payload_bytes',
      label: 'Message size',
      min: 1,
      max: 65536,
      unit: 'B',
      integer: true,
      hint: (p) => fmtBytes(p.payload_bytes),
      show: (p) => p.mode === 'latency',
    },
    {
      kind: 'number',
      key: 'count',
      label: 'Messages',
      min: 10,
      max: 10000,
      integer: true,
      show: (p) => p.mode === 'latency',
    },
    {
      kind: 'number',
      key: 'warmup_count',
      label: 'Warmup messages',
      min: 0,
      max: 1000,
      integer: true,
      hint: 'Discarded.',
      show: (p) => p.mode === 'latency',
    },
    {
      kind: 'bool',
      key: 'nodelay',
      label: 'TCP_NODELAY on both bridges',
      hint: (p) => (p.nodelay ? '' : 'Nagle is on: small writes can wait.'),
    },
  ],
  validate: (p: TcpParams) => {
    if (p.mode === 'throughput') return warmupRule(p);
    return p.warmup_count < p.count ? null : 'Warmup messages must be fewer than messages.';
  },
  kpis: (p) =>
    p.mode === 'latency' ? latencyKpis : throughputKpis(p.direction === 'bidirectional'),
  headline: (p) => (p.mode === 'latency' ? 'rtt_p50_ms' : 'goodput_mbps'),
  main: (p) => (p.mode === 'latency' ? rttMain : goodputMain(p)),
  panels: (p) =>
    p.mode === 'latency'
      ? [{ kind: 'cdf' }, { kind: 'compare' }]
      : [{ kind: 'loaded-rtt' }, { kind: 'compare' }],
  distribution: (p) => (p.mode === 'latency' ? RTT_DIST : null),
  facts: (p) => (p.mode === 'latency' ? latencyFacts : throughputFacts),
  timeBased: (p) => p.mode === 'throughput',
  estimateMs: (p) =>
    p.mode === 'throughput'
      ? (p.warmup_secs + p.duration_secs) * 1000 + SETUP_MS
      : p.count * 5 + SETUP_MS,
  measured: (p) =>
    p.mode === 'throughput'
      ? [
          p.direction === 'bidirectional'
            ? `I open ${plural(p.streams, 'stream')} each way and both directions run at once, for ${p.warmup_secs + p.duration_secs} s. Each receiver counts its own direction over the time they overlap; the headline is the sum.`
            : `I open ${plural(p.streams, 'TCP stream')} and ${p.direction === 'upload' ? 'write to the peer' : 'the peer writes to me'} for ${p.warmup_secs + p.duration_secs} s. The ${p.direction === 'upload' ? 'peer' : 'app on this device'} is the receiver, so the number is what arrived, not what was sent.`,
          `Goodput is payload bits per second, counted in 250 ms buckets after the ${p.warmup_secs} s warmup. Headers don’t count.`,
          'Connect is how long open_tcp takes: sidecar IPC, the tailnet handshake and the bridge handoff.',
          'RTT under load comes from pinging the peer twice a second while the data flows.',
        ]
      : [
          `I send ${p.count} messages of ${fmtBytes(p.payload_bytes)} on one stream and the peer echoes each one. A round trip runs from my write to the reply, on one clock.`,
          `The first ${p.warmup_count} are discarded.`,
        ],
  hops: () => BRIDGED_TCP_HOPS,
  notes: (p) => [
    'Crosses two loopback bridges, one on each side. Egress removes this side’s.',
    p.nodelay
      ? 'TCP_NODELAY is set on both Rust sockets.'
      : 'Nagle is on for both Rust sockets: small writes can wait for an acknowledgement.',
    ...(p.mode === 'latency'
      ? [
          'Nagle exists only on the two Rust loopback sockets. In full, 32 KiB payloads cross the macOS lo0 MTU, so the nodelay A/B is visible there.',
        ]
      : []),
    'On macOS and Windows the netstack handles every 1280 B segment on its own (no GSO); Linux batches them.',
  ],
});

const udp = define<'udp'>({
  id: 'udp',
  page: 'udp',
  label: 'UDP',
  icon: 'udp',
  blurb: 'Paced datagrams to the peer: goodput, loss, jitter, and the highest rate the path holds.',
  defaults: {
    workload: 'udp',
    mode: 'stream',
    direction: 'upload',
    payload_bytes: 1200,
    rate_mbps: 10,
    pps: 50,
    duration_secs: 10,
    warmup_secs: 1,
    warmup_count: 10,
  } as ParamsOf<'udp'>,
  presets: [
    {
      id: 'default',
      label: 'Default',
      params: { mode: 'stream', rate_mbps: 10, payload_bytes: 1200 },
    },
    {
      id: 'lan',
      label: 'LAN 100 Mb/s',
      params: { mode: 'stream', rate_mbps: 100, payload_bytes: 1200 },
    },
    {
      id: 'derp',
      label: 'DERP-friendly',
      params: { mode: 'stream', rate_mbps: 2, payload_bytes: 1200 },
    },
    { id: 'ramp', label: 'Ramp', params: { mode: 'ramp', rate_mbps: 10, duration_secs: 30 } },
  ],
  fields: [
    {
      kind: 'enum',
      key: 'mode',
      label: 'Mode',
      options: [
        { value: 'stream', label: 'Stream' },
        { value: 'echo', label: 'Echo' },
        { value: 'ramp', label: 'Ramp' },
      ],
    },
    {
      kind: 'enum',
      key: 'direction',
      label: 'Direction',
      options: DIRECTIONS,
      show: (p) => p.mode !== 'echo',
    },
    {
      kind: 'number',
      key: 'payload_bytes',
      label: 'Datagram size',
      min: 30,
      max: 1400,
      unit: 'B',
      integer: true,
      hint: (p) =>
        p.payload_bytes > 1252
          ? 'Above 1252 B: the netstack fragments each datagram.'
          : 'Includes the 22 B header. Above 1252 B the netstack fragments.',
    },
    {
      kind: 'number',
      key: 'rate_mbps',
      label: 'Rate',
      min: 0.1,
      max: 1000,
      unit: 'Mb/s',
      show: (p) => p.mode === 'stream',
    },
    {
      kind: 'number',
      key: 'rate_mbps',
      label: 'Start rate',
      min: 0.1,
      max: 1000,
      unit: 'Mb/s',
      hint: 'Raised 1.5× every 3 s.',
      show: (p) => p.mode === 'ramp',
    },
    {
      kind: 'number',
      key: 'pps',
      label: 'Datagrams per second',
      min: 1,
      max: 2000,
      integer: true,
      show: (p) => p.mode === 'echo',
    },
    ...durationFields<UdpParams>(undefined, (p) => p.mode !== 'echo'),
    {
      kind: 'number',
      key: 'warmup_count',
      label: 'Warmup datagrams',
      min: 0,
      max: 1000,
      integer: true,
      hint: 'Discarded.',
      show: (p) => p.mode === 'echo',
    },
  ],
  validate: (p: UdpParams) =>
    p.mode === 'echo'
      ? p.warmup_count < p.pps * p.duration_secs
        ? null
        : 'Warmup datagrams must be fewer than datagrams per second × duration.'
      : warmupRule(p),
  kpis: (p) =>
    p.mode === 'echo'
      ? [
          { key: 'rtt_p50_ms', label: 'RTT p50', unit: 'ms', live: 'rtt_p50_ms' },
          { key: 'rtt_p99_ms', label: 'RTT p99', unit: 'ms' },
          { key: 'loss_pct', label: 'Loss', unit: '%', live: 'loss_pct' },
        ]
      : p.mode === 'ramp'
        ? [
            {
              key: 'max_sustainable_mbps',
              label: 'Highest rate at ≤ 2% loss',
              unit: 'Mb/s',
              live: 'max_sustainable_mbps',
            },
            { key: 'offered_mbps', label: 'Offered', unit: 'Mb/s', live: 'offered_mbps' },
            { key: 'loss_pct', label: 'Loss', unit: '%', live: 'loss_pct' },
          ]
        : [
            { key: 'goodput_mbps', label: 'Goodput', unit: 'Mb/s', live: 'goodput_mbps' },
            { key: 'offered_mbps', label: 'Offered', unit: 'Mb/s', live: 'offered_mbps' },
            { key: 'loss_pct', label: 'Loss', unit: '%', live: 'loss_pct' },
            { key: 'jitter_ms', label: 'Jitter', unit: 'ms', live: 'jitter_ms' },
          ],
  headline: (p) =>
    p.mode === 'echo' ? 'rtt_p50_ms' : p.mode === 'ramp' ? 'max_sustainable_mbps' : 'goodput_mbps',
  main: (p) =>
    p.mode === 'echo'
      ? rttMain
      : p.mode === 'ramp'
        ? {
            title: 'Offered rate',
            unit: 'Mb/s',
            series: [
              { key: 'offered_mbps', label: 'Offered' },
              { key: 'goodput_mbps', label: 'Goodput' },
            ],
            guides: [{ label: 'highest at ≤ 2%', key: 'max_sustainable_mbps' }],
          }
        : {
            title: 'Goodput',
            unit: 'Mb/s',
            series: [
              { key: 'goodput_mbps', label: 'Goodput', area: true },
              { key: 'offered_mbps', label: 'Offered' },
            ],
          },
  panels: (p) =>
    p.mode === 'echo'
      ? [{ kind: 'cdf' }, { kind: 'compare' }]
      : [{ kind: 'loss' }, { kind: 'compare' }],
  distribution: (p) => (p.mode === 'echo' ? RTT_DIST : null),
  facts: (p) =>
    p.mode === 'echo'
      ? [
          { key: 'sent', label: 'Sent', format: 'int' },
          { key: 'lost', label: 'Not echoed in time', format: 'int' },
        ]
      : [
          { key: 'sent', label: 'Sent', format: 'int' },
          { key: 'unique', label: 'Received', format: 'int' },
          { key: 'dup', label: 'Duplicates', format: 'int' },
          { key: 'reordered', label: 'Reordered', format: 'int' },
          { key: 'max_loss_burst', label: 'Longest loss burst', format: 'int' },
          { key: 'send_errors', label: 'Send errors', format: 'int' },
          { key: 'max_burst_datagrams', label: 'Largest send burst', format: 'int' },
          { key: 'rate_achieved', label: 'Target rate reached', format: 'bool' },
          { key: 'pacer_late_ms_mean', label: 'Pacer lateness, mean', unit: 'ms' },
          // where the loss happened, from usermetrics packet counts: an approximation that
          // other traffic skews (it can go negative), so only when something was lost
          ...(
            [
              ['local_send_drops', 'Lost leaving the sender, about'],
              ['network', 'Lost on the path, about'],
              ['local_recv_drops', 'Lost at the receiver, about'],
            ] as const
          ).map(([k, label]) => ({
            key: `loss_locality.${k}`,
            label,
            format: 'int' as const,
            when: (r: unknown) => ((r as { missing?: number } | null)?.missing ?? 0) > 0,
          })),
          ...(p.mode === 'ramp'
            ? [
                { key: 'steps', label: 'Rate steps', format: 'count' as const },
                { key: 'stop_reason', label: 'Stopped because', format: 'text' as const },
              ]
            : []),
        ],
  timeBased: () => true,
  estimateMs: (p) =>
    (p.mode === 'echo' ? p.duration_secs : p.warmup_secs + p.duration_secs) * 1000 +
    SETUP_MS +
    1500,
  measured: (p) =>
    p.mode === 'stream'
      ? [
          `I send ${p.payload_bytes} B datagrams at ${p.rate_mbps} Mb/s, paced every millisecond, for ${p.warmup_secs + p.duration_secs} s (${p.direction}). The receiver counts what arrived once the last datagram has had time to land.`,
          'Loss is what never arrived, end to end. Jitter is RFC 3550 interarrival jitter, which needs no shared clock.',
          'If the sender can’t keep up with the target rate, the run says so and stays out of trends.',
        ]
      : p.mode === 'echo'
        ? [
            `I send ${p.pps} datagrams a second for ${p.duration_secs} s and the peer echoes each one. A round trip is measured on my clock; anything not back by the drain deadline is lost.`,
          ]
        : [
            `I start at ${p.rate_mbps} Mb/s and raise the rate 1.5× every 3 s, until more than 2% is lost in two 250 ms buckets in a row, or the sender can’t keep up.`,
            'The result is the last rate that lost 2% or less.',
          ],
  hops: () => UDP_HOPS,
  notes: (p) => [
    'IPv4 only.',
    'Loss is end to end, including loopback relay drops.',
    p.payload_bytes > 1252
      ? `Payload > 1252 B fragments in netstack: each ${p.payload_bytes} B datagram leaves as two IP fragments, and losing either loses both.`
      : 'Datagrams above 1252 B would fragment in the netstack; this size doesn’t.',
  ],
});

const quic = define<'quic'>({
  id: 'quic',
  page: 'quic',
  label: 'QUIC',
  icon: 'quic',
  blurb: 'QUIC streams to the peer over a tailnet UDP socket: goodput, or round trips.',
  defaults: {
    workload: 'quic',
    mode: 'throughput',
    direction: 'upload',
    streams: 4,
    duration_secs: 10,
    warmup_secs: 1,
    payload_bytes: 64,
    count: 200,
    warmup_count: 10,
  } as ParamsOf<'quic'>,
  presets: [
    {
      id: 'quick',
      label: 'LAN quick',
      params: { mode: 'throughput', streams: 4, duration_secs: 3 },
    },
    {
      id: 'default',
      label: 'Default',
      params: { mode: 'throughput', streams: 4, duration_secs: 10 },
    },
    { id: 'one', label: '1 stream', params: { mode: 'throughput', streams: 1, duration_secs: 10 } },
    { id: 'latency', label: 'Latency', params: { mode: 'latency', payload_bytes: 64, count: 200 } },
  ],
  fields: [
    {
      kind: 'enum',
      key: 'mode',
      label: 'Mode',
      options: [
        { value: 'throughput', label: 'Throughput' },
        { value: 'latency', label: 'Latency' },
      ],
    },
    {
      kind: 'enum',
      key: 'direction',
      label: 'Direction',
      options: DIRECTIONS3,
      show: (p) => p.mode === 'throughput',
    },
    {
      kind: 'number',
      key: 'streams',
      label: 'Streams',
      min: 1,
      max: 32,
      integer: true,
      hint: 'On one connection.',
      show: (p) => p.mode === 'throughput',
    },
    ...durationFields<QuicParams>((p) => p.mode === 'throughput'),
    {
      kind: 'number',
      key: 'payload_bytes',
      label: 'Message size',
      min: 1,
      max: 65536,
      unit: 'B',
      integer: true,
      hint: (p) => fmtBytes(p.payload_bytes),
      show: (p) => p.mode === 'latency',
    },
    {
      kind: 'number',
      key: 'count',
      label: 'Messages',
      min: 10,
      max: 10000,
      integer: true,
      show: (p) => p.mode === 'latency',
    },
    {
      kind: 'number',
      key: 'warmup_count',
      label: 'Warmup messages',
      min: 0,
      max: 1000,
      integer: true,
      hint: 'Discarded.',
      show: (p) => p.mode === 'latency',
    },
  ],
  validate: (p: QuicParams) =>
    p.mode === 'throughput'
      ? warmupRule(p)
      : p.warmup_count < p.count
        ? null
        : 'Warmup messages must be fewer than messages.',
  kpis: (p) =>
    p.mode === 'latency' ? latencyKpis : throughputKpis(p.direction === 'bidirectional'),
  headline: (p) => (p.mode === 'latency' ? 'rtt_p50_ms' : 'goodput_mbps'),
  main: (p) => (p.mode === 'latency' ? rttMain : goodputMain(p)),
  panels: (p) =>
    p.mode === 'latency'
      ? [{ kind: 'cdf' }, { kind: 'compare' }]
      : [{ kind: 'loaded-rtt' }, { kind: 'compare' }],
  distribution: (p) => (p.mode === 'latency' ? RTT_DIST : null),
  facts: (p) =>
    p.mode === 'latency'
      ? latencyFacts
      : [
          ...throughputFacts.slice(0, 3),
          { key: 'streams', label: 'Streams', format: 'int' },
          {
            key: 'flow_control_ceiling_mbps',
            label: 'Flow-control ceiling at this round trip',
            unit: 'Mb/s',
          },
        ],
  timeBased: (p) => p.mode === 'throughput',
  estimateMs: (p) =>
    p.mode === 'throughput'
      ? (p.warmup_secs + p.duration_secs) * 1000 + SETUP_MS
      : p.count * 5 + SETUP_MS,
  measured: (p) =>
    p.mode === 'throughput'
      ? [
          `I open one QUIC connection with ${plural(p.streams, 'stream')}${p.direction === 'bidirectional' ? ' each way' : ''} and send for ${p.warmup_secs + p.duration_secs} s. The receiver counts payload as for TCP, after the ${p.warmup_secs} s warmup.`,
          'Connect is how long connect_quic takes: the local UDP relay and the QUIC/TLS handshake.',
          'When goodput reaches 80% of what quinn’s default flow-control windows allow at this round trip, the run says so.',
        ]
      : [
          `I send ${p.count} messages of ${fmtBytes(p.payload_bytes)} on one QUIC stream and the peer echoes each one, timed on one clock. The first ${p.warmup_count} are discarded.`,
        ],
  hops: () => QUIC_HOPS,
  notes: () => [
    'About 1200 B packets, because MTU discovery is off: the tailnet socket may fragment.',
    'No GSO: one packet per send.',
    'Double encryption: TLS inside WireGuard.',
  ],
});

const ws = define<'ws'>({
  id: 'ws',
  page: 'messaging',
  label: 'Messaging',
  icon: 'message',
  blurb:
    'Typed messages over the WebSocket session every truffle app uses: round trips and message rate.',
  defaults: {
    workload: 'ws',
    mode: 'rr',
    payload_bytes: 64,
    count: 500,
    concurrency: 1,
    warmup_count: 10,
    duration_secs: 10,
    rate_per_sec: null,
  } as ParamsOf<'ws'>,
  presets: [
    {
      id: 'default',
      label: 'Default',
      params: { mode: 'rr', payload_bytes: 64, count: 500, concurrency: 1 },
    },
    {
      id: 'pipelined',
      label: '8 in flight',
      params: { mode: 'rr', payload_bytes: 64, count: 2000, concurrency: 8 },
    },
    {
      id: 'large',
      label: '64 KiB',
      params: { mode: 'rr', payload_bytes: 65536, count: 200, concurrency: 1 },
    },
    {
      id: 'stream',
      label: 'Stream',
      params: { mode: 'stream', payload_bytes: 1024, duration_secs: 10, rate_per_sec: null },
    },
  ],
  fields: [
    {
      kind: 'enum',
      key: 'mode',
      label: 'Mode',
      options: [
        { value: 'rr', label: 'Request/reply' },
        { value: 'stream', label: 'Stream' },
      ],
    },
    {
      kind: 'number',
      key: 'payload_bytes',
      label: 'Payload',
      min: 0,
      max: 1048576,
      unit: 'B',
      integer: true,
      hint: (p) => fmtBytes(p.payload_bytes),
      show: (p) => p.mode === 'rr',
    },
    {
      kind: 'number',
      key: 'payload_bytes',
      label: 'Payload',
      min: 0,
      max: 262144,
      unit: 'B',
      integer: true,
      hint: (p) => fmtBytes(p.payload_bytes),
      show: (p) => p.mode === 'stream',
    },
    {
      kind: 'number',
      key: 'count',
      label: 'Requests',
      min: 10,
      max: 100000,
      integer: true,
      show: (p) => p.mode === 'rr',
    },
    {
      kind: 'number',
      key: 'concurrency',
      label: 'In flight',
      min: 1,
      max: 64,
      integer: true,
      show: (p) => p.mode === 'rr',
    },
    {
      kind: 'number',
      key: 'warmup_count',
      label: 'Warmup requests',
      min: 0,
      max: 1000,
      integer: true,
      hint: 'Discarded.',
      show: (p) => p.mode === 'rr',
    },
    {
      kind: 'number',
      key: 'duration_secs',
      label: 'Duration',
      min: 2,
      max: 60,
      unit: 's',
      integer: true,
      show: (p) => p.mode === 'stream',
    },
    {
      kind: 'number',
      key: 'rate_per_sec',
      label: 'Rate',
      min: 1,
      max: 100000,
      unit: 'msg/s',
      integer: true,
      nullable: true,
      nullLabel: 'As fast as it goes',
      hint: 'Empty: as fast as backpressure allows.',
      show: (p) => p.mode === 'stream',
    },
  ],
  validate: (p: WsParams) =>
    p.mode === 'rr' && p.count * p.payload_bytes > 1024 ** 3
      ? 'Requests × payload must be 1 GiB or less.'
      : p.mode === 'rr' && p.warmup_count >= p.count
        ? 'Warmup requests must be fewer than requests.'
        : null,
  kpis: (p) =>
    p.mode === 'rr'
      ? [
          { key: 'rtt_p50_ms', label: 'RTT p50', unit: 'ms', live: 'rtt_p50_ms' },
          { key: 'rtt_p99_ms', label: 'RTT p99', unit: 'ms' },
          {
            key: 'msgs_per_sec',
            label: 'Requests per second',
            unit: 'msg/s',
            live: 'msgs_per_sec',
          },
        ]
      : [
          {
            key: 'msgs_per_sec',
            label: 'Messages per second',
            unit: 'msg/s',
            live: 'msgs_per_sec',
          },
          { key: 'goodput_mbps', label: 'Goodput', unit: 'Mb/s', live: 'goodput_mbps' },
          { key: 'drop_pct', label: 'Dropped', unit: '%', live: 'drop_pct' },
        ],
  headline: (p) => (p.mode === 'rr' ? 'rtt_p50_ms' : 'msgs_per_sec'),
  main: (p) =>
    p.mode === 'rr'
      ? rttMain
      : {
          title: 'Message rate',
          unit: 'msg/s',
          series: [{ key: 'msgs_per_sec', label: 'Received', area: true }],
        },
  panels: (p) =>
    p.mode === 'rr'
      ? [{ kind: 'cdf' }, { kind: 'compare' }]
      : [{ kind: 'loaded-rtt' }, { kind: 'compare' }],
  distribution: (p) => (p.mode === 'rr' ? RTT_DIST : null),
  facts: (p) =>
    p.mode === 'rr'
      ? [
          { key: 'completed', label: 'Requests answered', format: 'int' },
          { key: 'timeouts', label: 'Unanswered after 10 s', format: 'int' },
          { key: 'wire_bytes_per_msg', label: 'Envelope on the wire', format: 'bytes' },
        ]
      : [
          { key: 'sent', label: 'Sent', format: 'int' },
          { key: 'unique', label: 'Received', format: 'int' },
          { key: 'lag_dropped', label: 'Dropped by a lagging subscription', format: 'int' },
          { key: 'router_dropped', label: 'Lost before the subscription', format: 'int' },
          { key: 'wire_bytes_per_msg', label: 'Envelope on the wire', format: 'bytes' },
          { key: 'wire_mbps', label: 'Wire rate', unit: 'Mb/s' },
        ],
  timeBased: (p) => p.mode === 'stream',
  estimateMs: (p) =>
    p.mode === 'stream'
      ? p.duration_secs * 1000 + SETUP_MS + 2000
      : (p.count * 4) / Math.min(p.concurrency, 6) + SETUP_MS,
  measured: (p) =>
    p.mode === 'rr'
      ? [
          `I send ${p.count} requests of ${fmtBytes(p.payload_bytes)} with send_typed, ${p.concurrency} in flight, and the peer replies with the same payload.`,
          'Each round trip is timed from the send to the reply, through one subscription and an id map: no per-request waiters.',
        ]
      : [
          `I send ${fmtBytes(p.payload_bytes)} messages one way for ${p.duration_secs} s, ${p.rate_per_sec == null ? 'as fast as backpressure allows' : `at ${p.rate_per_sec} a second`}.`,
          'Goodput counts decoded payload; the wire also carries the JSON envelope and base64. The receiver counts every message it never saw.',
        ],
  hops: () => WS_HOPS,
  notes: (p) => [
    ...(p.mode === 'stream'
      ? [
          'The WebSocket itself is lossless, so every drop happens at a receiver.',
          'At high message rates core’s session broadcast (1024 entries) can overflow: those messages are lost before the subscription.',
        ]
      : []),
    'Wire size includes the JSON envelope and base64.',
    'Core doesn’t set TCP_NODELAY on the session socket.',
  ],
});

const fileTransfer = define<'file-transfer'>({
  id: 'file-transfer',
  page: 'file-transfer',
  label: 'File transfer',
  icon: 'file',
  blurb:
    'Sending files with truffle’s file transfer: wire speed, end-to-end speed, and each phase.',
  defaults: {
    workload: 'file-transfer',
    size_bytes: 10485760,
    iterations: 3,
  } as ParamsOf<'file-transfer'>,
  presets: [
    { id: 'small', label: '1 MiB × 5', params: { size_bytes: 1048576, iterations: 5 } },
    { id: 'default', label: '10 MiB × 3', params: { size_bytes: 10485760, iterations: 3 } },
    { id: 'large', label: '256 MiB × 3', params: { size_bytes: 268435456, iterations: 3 } },
  ],
  fields: [
    {
      kind: 'number',
      key: 'size_bytes',
      label: 'File size',
      min: 1024,
      max: 1073741824,
      unit: 'B',
      integer: true,
      hint: (p) => fmtBytes(p.size_bytes),
    },
    {
      kind: 'number',
      key: 'iterations',
      label: 'Files',
      min: 1,
      max: 20,
      integer: true,
      hint: 'With 3 or more, the first is left out of the summary.',
    },
  ],
  validate: (p: FileTransferParams) =>
    p.size_bytes * p.iterations <= 4 * 1024 ** 3 ? null : 'Size × files must be 4 GiB or less.',
  kpis: () => [
    {
      key: 'wire_mbps',
      label: 'Wire speed',
      unit: 'Mb/s',
      live: 'wire_mbps',
      liveSub: 'last file',
    },
    { key: 'e2e_mbps', label: 'End to end', unit: 'Mb/s', live: 'e2e_mbps', liveSub: 'last file' },
    { key: 'hash_ms', label: 'SHA-256', unit: 'ms', live: 'hash_ms', liveSub: 'last file' },
    {
      key: 'negotiate_ms',
      label: 'Offer to stream',
      unit: 'ms',
      live: 'negotiate_ms',
      liveSub: 'last file',
    },
  ],
  headline: () => 'wire_mbps',
  main: () => ({
    kind: 'iterations',
    title: 'Wire speed per file',
    unit: 'Mb/s',
    series: [{ key: 'wire_mbps', label: 'Wire speed' }],
    guides: [{ label: 'median', key: 'wire_mbps' }],
  }),
  panels: () => [{ kind: 'phases' }, { kind: 'compare' }],
  distribution: () => null,
  facts: () => [
    { key: 'timed_iterations', label: 'Files in the summary', format: 'int' },
    { key: 'ack_failures', label: 'Acknowledgements lost', format: 'int' },
    { key: 'source_write_ms', label: 'Writing the source file', unit: 'ms' },
  ],
  timeBased: () => false,
  estimateMs: (p) => p.iterations * ((p.size_bytes * 8) / 400e6 + 0.6) * 1000 + SETUP_MS,
  measured: (p) => [
    `I send ${plural(p.iterations, 'file')} of ${fmtBytes(p.size_bytes)} with truffle’s file transfer, one after another, and wait for the peer to confirm each before the next.`,
    'Wire speed runs from the first payload byte to the receiver’s acknowledgement. End to end adds hashing the file and the offer and accept.',
    p.iterations >= 3
      ? 'The summary is the median over files, leaving out the first (it warms caches and connections).'
      : 'The summary is the median over files.',
  ],
  hops: () => [
    'SHA-256 of the whole file (a first read from disk)',
    'An offer over the WebSocket session; the peer opens a fresh tailnet port and accepts',
    'open_tcp: a raw TCP stream through both loopback bridges, plus the receiver’s WhoIs',
    '64 KiB chunks (a second read); the receiver writes to disk and hashes as it goes',
    'The receiver checks the hash and acknowledges',
  ],
  notes: () => [
    'SHA-256 of page-cached data: a cold file reads slower.',
    'Progress events are never used for speed: they are throttled and measured at the sender’s socket buffer.',
    'Files land in a temporary folder that is removed after the run.',
  ],
});

const syncedStore = define<'synced-store'>({
  id: 'synced-store',
  page: 'synced-store',
  label: 'SyncedStore',
  icon: 'sync',
  blurb: 'How long a SyncedStore write takes to reach the peer and come back.',
  defaults: {
    workload: 'synced-store',
    mode: 'pingpong',
    count: 100,
    payload_bytes: 64,
    warmup_count: 5,
  } as ParamsOf<'synced-store'>,
  presets: [
    {
      id: 'default',
      label: 'Default',
      params: { mode: 'pingpong', count: 100, payload_bytes: 64 },
    },
    {
      id: 'large',
      label: '4 KiB writes',
      params: { mode: 'pingpong', count: 100, payload_bytes: 4096 },
    },
    {
      id: 'burst',
      label: 'Burst of 1000',
      params: { mode: 'burst', count: 1000, payload_bytes: 64 },
    },
  ],
  fields: [
    {
      kind: 'enum',
      key: 'mode',
      label: 'Mode',
      options: [
        { value: 'pingpong', label: 'Ping-pong' },
        { value: 'burst', label: 'Burst' },
      ],
    },
    { kind: 'number', key: 'count', label: 'Writes', min: 10, max: 5000, integer: true },
    {
      kind: 'number',
      key: 'payload_bytes',
      label: 'Padding per write',
      min: 0,
      max: 65536,
      unit: 'B',
      integer: true,
      hint: (p) => fmtBytes(p.payload_bytes),
    },
    {
      kind: 'number',
      key: 'warmup_count',
      label: 'Warmup writes',
      min: 0,
      max: 1000,
      integer: true,
      hint: 'Discarded.',
      show: (p) => p.mode === 'pingpong',
    },
  ],
  validate: (p: StoreParams) =>
    p.mode === 'burst' || p.warmup_count < p.count
      ? null
      : 'Warmup writes must be fewer than writes.',
  kpis: (p) =>
    p.mode === 'burst'
      ? [
          { key: 'final_echo_ms', label: 'Last write echoed', unit: 'ms' },
          { key: 'coalescing_pct', label: 'Coalesced', unit: '%' },
          { key: 'burst_write_ms', label: 'Writing', unit: 'ms' },
        ]
      : [
          {
            key: 'convergence_p50_ms',
            label: 'Convergence p50 (RTT/2 estimate)',
            unit: 'ms',
            live: 'convergence_p50_ms',
          },
          { key: 'convergence_p99_ms', label: 'Convergence p99 (RTT/2 estimate)', unit: 'ms' },
        ],
  headline: (p) => (p.mode === 'burst' ? 'final_echo_ms' : 'convergence_p50_ms'),
  main: (p) =>
    p.mode === 'burst'
      ? { kind: 'burst', title: 'The burst', unit: 'ms', series: [] }
      : {
          title: 'Convergence per write',
          unit: 'ms',
          series: [{ key: 'convergence_ms', label: 'Convergence (RTT/2 estimate)' }],
          guides: [
            { label: 'p50', key: 'convergence_p50_ms' },
            { label: 'p99', key: 'convergence_p99_ms' },
          ],
        },
  panels: (p) =>
    p.mode === 'burst' ? [{ kind: 'compare' }] : [{ kind: 'cdf' }, { kind: 'compare' }],
  distribution: (p) =>
    p.mode === 'burst'
      ? null
      : { key: 'convergence_ms', title: 'Convergence (RTT/2 estimate)', unit: 'ms' },
  facts: (p) =>
    p.mode === 'burst'
      ? [
          { key: 'distinct_versions_seen', label: 'Versions the peer saw', format: 'int' },
          { key: 'echoes_seen', label: 'Echoes seen', format: 'int' },
          { key: 'ready_ms', label: 'Store ready', unit: 'ms', format: 'int' },
          { key: 'ws_peers_start', label: 'WebSocket peers at the start', format: 'int' },
          { key: 'ws_peers_end', label: 'WebSocket peers at the end', format: 'int' },
        ]
      : [
          { key: 'timeouts', label: 'Writes not echoed in 5 s', format: 'int' },
          { key: 'ready_ms', label: 'Store ready', unit: 'ms', format: 'int' },
          { key: 'ws_peers_start', label: 'WebSocket peers at the start', format: 'int' },
          { key: 'ws_peers_end', label: 'WebSocket peers at the end', format: 'int' },
        ],
  timeBased: () => false,
  estimateMs: (p) => p.count * 12 + SETUP_MS + 1500,
  measured: (p) =>
    p.mode === 'pingpong'
      ? [
          'I write a sequence number to a SyncedStore. The peer’s store sees it and writes an echo back.',
          'Convergence is half that round trip, an estimate rather than a one-way time, so it needs no shared clock.',
          `${p.count} writes after ${plural(p.warmup_count, 'warmup write')}; each has 5 s to come back.`,
        ]
      : [
          `I write ${p.count} times back to back. The peer echoes the highest sequence it has seen whenever that changes.`,
          'Coalescing is the share of writes the peer never saw on their own; the last echo says when the peer caught up.',
        ],
  hops: () => [
    'set() bumps the version and broadcasts the state over the WebSocket session',
    ...WS_HOPS.slice(1),
    'The peer’s store applies it (last write wins) and emits PeerUpdated',
  ],
  notes: () => [
    'Every WebSocket peer gets the broadcast (broadcast_typed), so other connected peers add load.',
    'Last-write-wins: intermediate states can be skipped, which is what coalescing counts.',
  ],
  notice: {
    title: 'SyncedStore is not a CRDT',
    text: 'It is last-write-wins state sync, and that is what this measures. RFC 018’s CrdtDoc was removed on 7 July 2026.',
  },
});

const http = define<'http'>({
  id: 'http',
  page: 'http',
  label: 'HTTP',
  icon: 'globe',
  blurb:
    'HTTP requests to a server on the peer, through the sidecar’s proxy or straight to a tailnet listener.',
  defaults: {
    workload: 'http',
    target: 'proxy',
    connection: 'keepalive',
    concurrency: 4,
    duration_secs: 10,
    warmup_secs: 1,
    response_bytes: 1024,
  } as ParamsOf<'http'>,
  presets: [
    {
      id: 'default',
      label: 'Default',
      params: { target: 'proxy', connection: 'keepalive', concurrency: 4, response_bytes: 1024 },
    },
    {
      id: 'direct',
      label: 'Direct',
      params: { target: 'direct', connection: 'keepalive', concurrency: 4, response_bytes: 1024 },
    },
    {
      id: 'new',
      label: 'New connections',
      params: { target: 'proxy', connection: 'new', concurrency: 4, response_bytes: 1024 },
    },
    {
      id: 'large',
      label: '1 MiB bodies',
      params: { target: 'proxy', connection: 'keepalive', concurrency: 2, response_bytes: 1048576 },
    },
  ],
  fields: [
    {
      kind: 'enum',
      key: 'target',
      label: 'Server',
      options: [
        { value: 'proxy', label: 'Sidecar proxy' },
        { value: 'direct', label: 'Direct' },
      ],
    },
    {
      kind: 'enum',
      key: 'connection',
      label: 'Connections',
      options: [
        { value: 'keepalive', label: 'Keep-alive' },
        { value: 'new', label: 'New each time' },
      ],
    },
    {
      kind: 'number',
      key: 'concurrency',
      label: 'Workers',
      min: 1,
      max: 64,
      integer: true,
      hint: (p) =>
        p.target === 'proxy' && p.connection === 'keepalive' && p.concurrency > 2
          ? 'Sidecars before protocol 8 reconnect upstream above 2.'
          : '',
    },
    ...durationFields<HttpParams>(),
    {
      kind: 'number',
      key: 'response_bytes',
      label: 'Response size',
      min: 0,
      max: 10485760,
      unit: 'B',
      integer: true,
      hint: (p) => fmtBytes(p.response_bytes),
    },
  ],
  validate: (p: HttpParams) => warmupRule(p),
  kpis: () => [
    { key: 'req_per_sec', label: 'Requests per second', unit: 'req/s', live: 'req_per_sec' },
    { key: 'latency_p50_ms', label: 'Latency p50', unit: 'ms', live: 'latency_p50_ms' },
    { key: 'latency_p99_ms', label: 'Latency p99', unit: 'ms' },
    { key: 'error_pct', label: 'Errors', unit: '%', live: 'error_pct' },
  ],
  headline: () => 'req_per_sec',
  main: () => ({
    title: 'Requests per second',
    unit: 'req/s',
    series: [{ key: 'req_per_sec', label: 'Requests', area: true }],
  }),
  panels: () => [{ kind: 'cdf' }, { kind: 'compare' }],
  distribution: () => ({ key: 'latency_ms', title: 'Request latency', unit: 'ms' }),
  facts: (p) => [
    { key: 'attempts', label: 'Requests', format: 'int' },
    { key: 'connections_opened', label: 'Connections opened', format: 'int' },
    { key: 'errors.non_2xx', label: 'Non-2xx answers', format: 'int' },
    { key: 'errors.io', label: 'I/O errors', format: 'int' },
    { key: 'errors.timeout', label: 'Timeouts (10 s)', format: 'int' },
    ...(p.connection === 'new' ? [{ key: 'connect_ms', label: 'Connect p50', unit: 'ms' }] : []),
  ],
  timeBased: () => true,
  estimateMs: (p) => (p.warmup_secs + p.duration_secs) * 1000 + SETUP_MS,
  measured: (p) => [
    `${p.concurrency} workers send GET requests for a ${fmtBytes(p.response_bytes)} body ${p.target === 'proxy' ? 'through the peer’s sidecar proxy' : 'to a tailnet listener on the peer'}, ${p.connection === 'keepalive' ? 'each reusing one connection' : 'opening a new connection for every request'}.`,
    p.connection === 'new'
      ? 'Latency runs from open_tcp to the last body byte; connect time is reported apart.'
      : 'Latency runs from writing the request to the last body byte.',
    `Errors are non-2xx answers, I/O errors and 10 s timeouts, as a share of all requests. The first ${p.warmup_secs} s don’t count.`,
  ],
  hops: (p) =>
    p.target === 'proxy'
      ? [
          'An HTTP/1.1 request on a raw TCP stream, through this side’s loopback bridge',
          'The tailnet path to the peer’s sidecar',
          'The sidecar’s Go server, with no Rust bridge: a WhoIs (cached 60 s) adds the Tailscale identity headers',
          'httputil.ReverseProxy dials the peer’s server on loopback, keeping 2 idle connections',
          'The peer’s tiny HTTP server checks the node id and answers',
        ]
      : [
          'An HTTP/1.1 request on a raw TCP stream, through this side’s loopback bridge',
          'The tailnet path',
          'The peer’s sidecar: a WhoIs per connection, then its loopback bridge',
          'A Rust handler on the peer’s listen_tcp port',
        ],
  // the engine's WhoIs note, under the engine's condition and in its words; its proxy
  // reconnect note depends on what the peer's server saw, so only the report carries it
  notes: (p) => [
    ...(p.connection === 'new' ? ['New: one WhoIs per request on both paths.'] : []),
    p.target === 'proxy'
      ? 'The proxy’s WhoIs is cached for 60 s; the server checks the injected Tailscale-Node-Id.'
      : 'The direct target is a Rust handler behind the peer’s loopback bridge.',
  ],
});

const video = define<'video'>({
  id: 'video',
  page: 'video',
  label: 'Video',
  icon: 'video',
  blurb:
    'A synthetic video stream with exact frame sizes: delivered frame rate, late and dropped frames, freezes.',
  defaults: {
    workload: 'video',
    transport: 'udp',
    profile: '1080p30',
    bitrate_mbps: null,
    fps: null,
    gop: 60,
    keyframe_ratio: 8,
    duration_secs: 10,
    warmup_secs: 1,
    pacing: 'paced',
    jitter_buffer_ms: 100,
  } as ParamsOf<'video'>,
  presets: [
    {
      id: 'call',
      label: '720p call',
      params: {
        transport: 'udp',
        profile: '720p30',
        bitrate_mbps: null,
        fps: null,
        jitter_buffer_ms: 60,
      },
    },
    {
      id: 'default',
      label: 'Default',
      params: { transport: 'udp', profile: '1080p30', bitrate_mbps: null, fps: null },
    },
    {
      id: 'quic4k',
      label: '4K over QUIC',
      params: { transport: 'quic', profile: '4k30', bitrate_mbps: null, fps: null },
    },
    {
      id: 'tcp',
      label: 'Over TCP',
      params: { transport: 'tcp', profile: '1080p30', bitrate_mbps: null, fps: null },
    },
  ],
  fields: [
    {
      kind: 'enum',
      key: 'transport',
      label: 'Transport',
      options: [
        { value: 'udp', label: 'UDP' },
        { value: 'quic', label: 'QUIC' },
        { value: 'tcp', label: 'TCP' },
      ],
    },
    {
      kind: 'enum',
      key: 'profile',
      label: 'Profile',
      options: (Object.keys(PROFILES) as VideoProfile[]).map((k) => ({
        value: k,
        label: PROFILES[k].label,
      })),
    },
    {
      kind: 'number',
      key: 'bitrate_mbps',
      label: 'Bitrate',
      min: 0.5,
      max: 100,
      unit: 'Mb/s',
      nullable: true,
      nullLabel: (p) => `${PROFILES[p.profile].mbps} from the profile`,
      hint: 'Empty: the profile’s.',
    },
    {
      kind: 'number',
      key: 'fps',
      label: 'Frame rate',
      min: 1,
      max: 120,
      unit: 'fps',
      integer: true,
      nullable: true,
      nullLabel: (p) => `${PROFILES[p.profile].fps} from the profile`,
    },
    {
      kind: 'number',
      key: 'gop',
      label: 'Keyframe every',
      min: 1,
      max: 600,
      unit: 'frames',
      integer: true,
    },
    {
      kind: 'number',
      key: 'keyframe_ratio',
      label: 'Keyframe size',
      min: 1,
      max: 20,
      unit: '×',
      integer: true,
      hint: (p) => {
        const P = videoPFrameBytes(p);
        return `Keyframes ${fmtBytes(P * p.keyframe_ratio)}, others ${fmtBytes(P)}.`;
      },
    },
    ...durationFields<VideoParams>(),
    {
      kind: 'enum',
      key: 'pacing',
      label: 'Pacing',
      options: [
        { value: 'paced', label: 'Paced' },
        { value: 'burst', label: 'Burst' },
      ],
    },
    {
      kind: 'number',
      key: 'jitter_buffer_ms',
      label: 'Jitter buffer',
      min: 0,
      max: 1000,
      unit: 'ms',
      integer: true,
      hint: 'Later than this behind the fastest frame is late.',
    },
  ],
  validate: (p: VideoParams) =>
    warmupRule(p) ??
    (p.gop <= videoFps(p) * p.duration_secs
      ? null
      : 'The keyframe interval must fit in the run (frame rate × duration).'),
  kpis: () => [
    { key: 'delivered_fps', label: 'Delivered', unit: 'fps', live: 'delivered_fps' },
    { key: 'late_pct', label: 'Late', unit: '%', live: 'late_pct' },
    { key: 'dropped_pct', label: 'Dropped', unit: '%', live: 'dropped_pct' },
    { key: 'freezes', label: 'Freezes', unit: '', digits: 0, live: 'freezes' },
    { key: 'frame_latency_p50_ms', label: 'Latency p50', unit: 'ms', live: 'frame_latency_p50_ms' },
    { key: 'goodput_mbps', label: 'Goodput', unit: 'Mb/s', live: 'goodput_mbps' },
  ],
  headline: () => 'delivered_fps',
  main: () => ({
    title: 'Frame latency',
    unit: 'ms',
    series: [{ key: 'frame_latency_ms', label: 'Frame latency' }],
    guides: [{ label: 'p50', key: 'frame_latency_p50_ms' }],
    band: true,
  }),
  panels: () => [
    { kind: 'frames' },
    {
      kind: 'line',
      title: 'Goodput',
      unit: 'Mb/s',
      series: [{ key: 'goodput_mbps', label: 'Goodput' }],
      ref: { label: 'Offered', resultKey: 'offered_mbps' },
    },
    { kind: 'compare' },
  ],
  distribution: () => ({ key: 'frame_latency_ms', title: 'Frame latency', unit: 'ms' }),
  facts: (p) => [
    { key: 'frames_sent', label: 'Frames sent', format: 'int' },
    { key: 'on_time', label: 'On time', format: 'int' },
    { key: 'late', label: 'Late', format: 'int' },
    { key: 'dropped', label: 'Dropped', format: 'int' },
    { key: 'freeze_ms', label: 'Frozen for', unit: 'ms', format: 'int' },
    { key: 'frame_spread_p50_ms', label: 'First to last fragment, p50', unit: 'ms' },
    { key: 'keyframe_complete_p50_ms', label: 'Keyframe complete, p50', unit: 'ms' },
    { key: 'latency_uncertainty_ms', label: 'Latency ±', unit: 'ms' },
    { key: 'offered_mbps', label: 'Offered', unit: 'Mb/s' },
    { key: 'max_send_lag_ms', label: 'Sender behind schedule, max', unit: 'ms' },
    ...(p.transport === 'udp'
      ? [
          { key: 'datagrams.sent', label: 'Datagrams sent', format: 'int' as const },
          { key: 'datagrams.loss_pct', label: 'Datagrams lost', unit: '%' },
        ]
      : []),
  ],
  timeBased: () => true,
  estimateMs: (p) => (p.warmup_secs + p.duration_secs) * 1000 + SETUP_MS + 1000,
  measured: (p) => [
    `${videoFps(p)} frames a second at ${videoMbps(p)} Mb/s over ${p.transport.toUpperCase()}, a keyframe every ${p.gop} frames at ${p.keyframe_ratio}× the size of the others. The sizes are exact; there is no camera or codec.`,
    `A frame is late when it completes more than ${p.jitter_buffer_ms} ms after the fastest frame would have, and dropped when it never completes. Neither needs our clocks to agree.`,
    'Frame latency does: I estimate the clock offset before and after the run, and show the latency ± that uncertainty.',
  ],
  hops: (p) =>
    p.transport === 'udp'
      ? [
          `Each frame split into datagrams of at most 1200 B, ${p.pacing === 'paced' ? 'spread across the frame time' : 'sent back to back'}`,
          ...UDP_HOPS.slice(1),
        ]
      : p.transport === 'quic'
        ? ['One QUIC stream per frame, length-prefixed', ...QUIC_HOPS.slice(1)]
        : ['Length-prefixed frames on one TCP stream', ...BRIDGED_TCP_HOPS.slice(1)],
  notes: (p) => [
    'Synthetic and exact: no camera, no codec.',
    p.transport === 'udp'
      ? 'A frame with one lost datagram is dropped; nothing is retransmitted.'
      : p.transport === 'quic'
        ? 'One stream per frame: a lost packet stalls only its own frame.'
        : 'One stream: a lost packet stalls every frame behind it.',
    ...(p.transport === 'udp' && p.pacing === 'paced'
      ? [
          'Paced fragments are spread across the frame interval, so frame latency includes up to one frame time of pacing.',
        ]
      : []),
    'Late, dropped and freezes need no shared clock; frame latency does, and carries its uncertainty.',
  ],
});

const egress = define<'egress'>({
  id: 'egress',
  page: 'egress',
  label: 'Egress',
  icon: 'egress',
  blurb:
    'A plain TCP socket into the sidecar’s egress listener: TCP without the Rust bridge on this side.',
  defaults: {
    workload: 'egress',
    direction: 'upload',
    streams: 1,
    duration_secs: 10,
    warmup_secs: 1,
  } as ParamsOf<'egress'>,
  presets: [
    { id: 'default', label: 'Default', params: { direction: 'upload', streams: 1 } },
    { id: 'parallel', label: '4 streams', params: { direction: 'upload', streams: 4 } },
    { id: 'download', label: 'Download', params: { direction: 'download', streams: 1 } },
  ],
  fields: [
    { kind: 'enum', key: 'direction', label: 'Direction', options: DIRECTIONS },
    { kind: 'number', key: 'streams', label: 'Streams', min: 1, max: 8, integer: true },
    ...durationFields<EgressParams>(),
  ],
  validate: (p: EgressParams) => warmupRule(p),
  kpis: () => throughputKpis(false, false),
  headline: () => 'goodput_mbps',
  main: () => goodputMain({}),
  panels: () => [{ kind: 'loaded-rtt' }, { kind: 'bridge' }, { kind: 'compare' }],
  distribution: () => null,
  facts: () => [
    ...throughputFacts.slice(0, 3),
    { key: 'bridges', label: 'Rust bridges crossed', format: 'int' },
  ],
  timeBased: () => true,
  estimateMs: (p) => (p.warmup_secs + p.duration_secs) * 1000 + SETUP_MS,
  measured: (p) => [
    `I connect a plain socket to the sidecar’s egress listener, which forwards to the peer’s TCP sink, and ${p.direction === 'upload' ? 'write' : 'read'} for ${p.warmup_secs + p.duration_secs} s on ${plural(p.streams, 'stream')}.`,
    'The receiver counts goodput exactly as for TCP. Compare it with a TCP run at the same settings: the difference is one loopback bridge.',
  ],
  hops: () => [
    'A plain TCP socket to the sidecar’s egress listener on loopback: no Rust bridge on this side',
    'The sidecar dials the peer and copies bytes into the netstack',
    'WireGuard and the tailnet path',
    'The peer’s sidecar and its loopback bridge to the TCP sink',
  ],
  notes: () => [
    'Egress removes only the initiator-side Rust bridge, and the responder’s sink still uses one. So tcp crosses 2 bridges and egress 1, and egress − tcp is the cost of one bridge.',
    'Needs sidecar protocol 7 or later.',
  ],
});

export const REGISTRY: Record<WorkloadKind, WorkloadMeta> = {
  path,
  tcp,
  udp,
  quic,
  ws,
  'file-transfer': fileTransfer,
  'synced-store': syncedStore,
  http,
  video,
  egress,
};

/** Rail order (RFC 027 §11.3), the same as the engine's `Workload::ALL`. */
export const WORKLOAD_ORDER: WorkloadKind[] = [
  'path',
  'tcp',
  'udp',
  'quic',
  'ws',
  'file-transfer',
  'synced-store',
  'http',
  'video',
  'egress',
];

export function metaForPage(page: Page): WorkloadMeta | null {
  return WORKLOAD_ORDER.map((k) => REGISTRY[k]).find((m) => m.page === page) ?? null;
}

/** Fields that apply to these params, in order. */
export function visibleFields(meta: WorkloadMeta, p: WorkloadParams) {
  return meta.fields.filter((f) => !f.show || f.show(p as unknown as Record<string, unknown>));
}

/** Single-field bounds then the cross-field rules; the first problem, or null. */
export function validateParams(meta: WorkloadMeta, p: WorkloadParams): string | null {
  const rec = p as unknown as Record<string, unknown>;
  for (const f of visibleFields(meta, p)) {
    if (f.kind !== 'number') continue;
    const v = rec[f.key];
    if (v == null) {
      if (f.nullable) continue;
      return `${f.label} is required.`;
    }
    if (typeof v !== 'number' || !Number.isFinite(v)) return `${f.label} must be a number.`;
    if (f.integer && !Number.isInteger(v)) return `${f.label} must be a whole number.`;
    if (v < f.min || v > f.max)
      return `${f.label} must be ${f.min}–${f.max}${f.unit ? ` ${f.unit}` : ''}.`;
  }
  return meta.validate(p);
}

// ---------------------------------------------------------------- labels (port of the engine's label())

const KIB = 1024;
const MIB = 1024 * KIB;
const GIB = 1024 * MIB;

/** Exact binary multiples as KiB/MiB/GiB, else bytes: the engine's `fmt_bytes`. */
export function labelBytes(n: number): string {
  if (n >= GIB && n % GIB === 0) return `${n / GIB} GiB`;
  if (n >= MIB && n % MIB === 0) return `${n / MIB} MiB`;
  if (n >= KIB && n % KIB === 0) return `${n / KIB} KiB`;
  return `${n} B`;
}

const rate = (mbps: number) => `${mbps} Mb/s`;

function streamLabel(
  name: string,
  p: {
    mode: string;
    direction: string;
    streams: number;
    duration_secs: number;
    payload_bytes: number;
    count: number;
  },
): string[] {
  return p.mode === 'throughput'
    ? [`${name} throughput`, p.direction, plural(p.streams, 'stream'), `${p.duration_secs} s`]
    : [`${name} latency`, labelBytes(p.payload_bytes), plural(p.count, 'sample')];
}

/** A run's label, exactly as the engine's `WorkloadParams::label()` builds it. */
export function paramsLabel(p: WorkloadParams): string {
  let parts: string[];
  switch (p.workload) {
    case 'path':
      parts = ['Path', plural(p.count, 'ping'), `${p.interval_ms} ms apart`];
      break;
    case 'tcp':
      parts = streamLabel('TCP', p);
      if (!p.nodelay) parts.push('Nagle on');
      break;
    case 'quic':
      parts = streamLabel('QUIC', p);
      break;
    case 'udp':
      parts =
        p.mode === 'stream'
          ? [
              'UDP stream',
              p.direction,
              rate(p.rate_mbps),
              `${p.payload_bytes} B`,
              `${p.duration_secs} s`,
            ]
          : p.mode === 'echo'
            ? ['UDP echo', `${p.pps} pps`, `${p.payload_bytes} B`, `${p.duration_secs} s`]
            : [
                'UDP ramp',
                p.direction,
                `from ${rate(p.rate_mbps)}`,
                `${p.payload_bytes} B`,
                `${p.duration_secs} s`,
              ];
      break;
    case 'ws':
      parts =
        p.mode === 'rr'
          ? [
              'WebSocket request/reply',
              labelBytes(p.payload_bytes),
              plural(p.count, 'request'),
              ...(p.concurrency > 1 ? [`${p.concurrency} in flight`] : []),
            ]
          : [
              'WebSocket stream',
              labelBytes(p.payload_bytes),
              ...(p.rate_per_sec != null ? [`${p.rate_per_sec} msg/s`] : []),
              `${p.duration_secs} s`,
            ];
      break;
    case 'file-transfer':
      parts = ['File transfer', `${labelBytes(p.size_bytes)} × ${p.iterations}`];
      break;
    case 'synced-store':
      parts = [
        p.mode === 'pingpong' ? 'SyncedStore ping-pong' : 'SyncedStore burst',
        plural(p.count, 'write'),
        labelBytes(p.payload_bytes),
      ];
      break;
    case 'http':
      parts = [
        'HTTP',
        p.target,
        p.connection === 'keepalive' ? 'keep-alive' : 'new connections',
        plural(p.concurrency, 'worker'),
        labelBytes(p.response_bytes),
        `${p.duration_secs} s`,
      ];
      break;
    case 'video':
      parts = [
        'Video',
        p.profile,
        p.transport.toUpperCase(),
        rate(videoMbps(p)),
        p.pacing,
        `${p.duration_secs} s`,
      ];
      break;
    case 'egress':
      parts = ['Egress', p.direction, plural(p.streams, 'stream'), `${p.duration_secs} s`];
      break;
  }
  return parts.join(' · ');
}

// ---------------------------------------------------------------- reading reports

/**
 * Each workload's headline summary keys, in priority order: the first one a run's summary has is
 * its headline (its mode decides which it has). The engine orders summaries headline first, but
 * path reports from before 2026-10-16 lead with `rtt_min_ms`, and a udp ramp without a limit has
 * a null headline.
 */
const HEADLINE_KEYS: Record<WorkloadKind, string[]> = {
  path: ['rtt_p50_ms'],
  tcp: ['goodput_mbps', 'rtt_p50_ms'],
  udp: ['max_sustainable_mbps', 'goodput_mbps', 'rtt_p50_ms'],
  quic: ['goodput_mbps', 'rtt_p50_ms'],
  ws: ['rtt_p50_ms', 'msgs_per_sec'],
  'file-transfer': ['wire_mbps'],
  'synced-store': ['convergence_p50_ms', 'final_echo_ms'],
  http: ['req_per_sec'],
  video: ['delivered_fps'],
  egress: ['goodput_mbps'],
};

/**
 * The mode of a run's params, as the engine's `RunSummary.mode` has it: `params.mode` for tcp,
 * udp, quic, ws and synced-store; null for the workloads without modes.
 */
export function modeOf(p: WorkloadParams | null | undefined): string | null {
  if (!p) return null;
  const m = (p as unknown as { mode?: unknown }).mode;
  return typeof m === 'string' ? m : null;
}

/** The headline of a run: its workload's headline key, else the summary's first item. */
export function headlineOf(run: { workload: string; summary: SummaryItem[] }): SummaryItem | null {
  for (const k of HEADLINE_KEYS[run.workload as WorkloadKind] ?? []) {
    const item = run.summary.find((x) => x.key === k);
    if (item) return item;
  }
  return run.summary[0] ?? null;
}

/** A mode as the form's mode switch names it, in a sentence: "throughput", "request/reply". */
export function modeWord(meta: WorkloadMeta, mode: string | null): string | null {
  if (mode == null) return null;
  const f = meta.fields.find((x) => x.key === 'mode');
  const label = f?.kind === 'enum' ? f.options.find((o) => o.value === mode)?.label : null;
  return (label ?? mode).toLowerCase();
}

/** "a throughput", "an echo". */
export const withArticle = (w: string) => `${/^[aeiou]/i.test(w) ? 'an' : 'a'} ${w}`;

/**
 * Short names for headline values where the summary's label is a sentence: the Overview card has
 * one line beside its number (the unit says the rest), and Run detail keeps the full label.
 */
const SHORT_LABELS: Record<string, string> = {
  goodput_mbps: 'Goodput',
  max_sustainable_mbps: 'Max at ≤ 2% loss',
  msgs_per_sec: 'Messages',
  req_per_sec: 'Requests',
  convergence_p50_ms: 'Convergence p50',
  final_echo_ms: 'Last echo',
};

/**
 * The engine's names, since 2026-10-16, for summary keys it used to name differently ("Round trip
 * p50", "Slowest round trip", "Requests/s", "Lost"): reports keep the label they were written
 * with, and `labelOf` reads older ones the way new ones say it, so one table never names one
 * measure two ways.
 */
const LABELS: Record<string, string> = {
  rtt_p50_ms: 'RTT p50',
  rtt_p99_ms: 'RTT p99',
  rtt_max_ms: 'RTT max',
  rtt_min_ms: 'RTT min',
  loaded_rtt_p50_ms: 'RTT p50 under load',
  req_per_sec: 'Requests per second',
  loss_pct: 'Loss',
};

/** A summary item's label as the current engine writes it (see LABELS). */
export function labelOf(item: Pick<SummaryItem, 'key' | 'label'>): string {
  return LABELS[item.key] ?? item.label;
}

/**
 * A headline's label for one line beside its value: the short name above, else the summary's
 * label without a trailing parenthesis ("Convergence p50 (RTT/2 estimate)" → "Convergence p50").
 */
export function shortLabelOf(item: Pick<SummaryItem, 'key' | 'label'>): string {
  return SHORT_LABELS[item.key] ?? labelOf(item).replace(/\s*\([^)]*\)\s*$/, '');
}

/** A value at a dotted path in results. */
export function resultAt(results: unknown, path: string): unknown {
  let cur: unknown = results;
  for (const k of path.split('.')) {
    if (cur == null || typeof cur !== 'object') return undefined;
    cur = (cur as Record<string, unknown>)[k];
  }
  return cur;
}

/**
 * One field edited: the new params and which fields hold invalid input. A field that the edit
 * hides (a mode switch) and that held an invalid value goes back to its default: nothing on
 * screen could show or fix it, and the engine checks some hidden fields regardless of mode.
 */
export function editParams(
  meta: WorkloadMeta,
  params: WorkloadParams,
  bad: Record<string, boolean>,
  key: string,
  v: unknown,
  ok: boolean,
): { params: WorkloadParams; bad: Record<string, boolean> } {
  const next = { ...(params as unknown as Record<string, unknown>), [key]: v };
  const shown = new Set(
    visibleFields(meta, next as unknown as WorkloadParams).map((f) => f.key as string),
  );
  const nextBad: Record<string, boolean> = { ...bad, [key]: !ok };
  const defaults = meta.defaults as unknown as Record<string, unknown>;
  for (const k of Object.keys(nextBad)) {
    if (nextBad[k] && !shown.has(k)) {
      next[k] = defaults[k];
      delete nextBad[k];
    }
  }
  return { params: next as unknown as WorkloadParams, bad: nextBad };
}

/**
 * A udp ramp that the run's length capped (`limit_found: false`) has no headline: its
 * `max_sustainable_mbps` is only a lower bound, kept in results (§8.2). That bound, for showing as
 * "≥ X"; null for every other key and run.
 */
export function lowerBoundOf(
  report: { results: unknown } | null | undefined,
  key: string,
): number | null {
  if (!report || key !== 'max_sustainable_mbps') return null;
  if (resultAt(report.results, 'limit_found') !== false) return null;
  const v = resultAt(report.results, 'max_sustainable_mbps');
  return typeof v === 'number' ? v : null;
}

export function isDistribution(v: unknown): v is Distribution {
  return !!v && typeof v === 'object' && 'samples' in v && 'p50' in v;
}
