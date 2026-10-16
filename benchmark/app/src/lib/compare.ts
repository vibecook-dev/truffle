// Comparing runs (RFC 027 §7.4): which compare_key fields differ, how each summary value moved,
// and the flags History shows. Pure functions, unit-tested.

import type {
  BuildProfile,
  RunReport,
  RunSummary,
  SummaryItem,
  WorkloadParams,
} from '../api/types';

/** Run-length fields compare_key leaves out, unless `unkeyedParams` keeps one (§7.4). */
export const UNKEYED_PARAMS = new Set([
  'workload',
  'duration_secs',
  'count',
  'iterations',
  'warmup_secs',
  'warmup_count',
]);

/** file-transfer leaves iteration 1 out of its medians from this many iterations on. */
export const EXCLUDE_FIRST_FROM = 3;

/**
 * The params fields compare_key leaves out for these params: the run-length fields, except where
 * one defines what is measured (a port of the engine's `unkeyed_params`):
 * - synced-store burst keeps `count`, the burst size the times scale with;
 * - udp ramp keeps `duration_secs`, which decides how many ×1.5 steps fit.
 */
export function unkeyedParams(p: WorkloadParams): Set<string> {
  const keep =
    p.workload === 'synced-store' && p.mode === 'burst'
      ? 'count'
      : p.workload === 'udp' && p.mode === 'ramp'
        ? 'duration_secs'
        : null;
  return new Set([...UNKEYED_PARAMS].filter((k) => k !== keep));
}

/**
 * The params as compare_key hashes them: minus `unkeyedParams`, and for file-transfer plus
 * `first_iteration_excluded` (whether iteration 1 is left out of the medians).
 */
export function keyedParams(p: WorkloadParams): Record<string, unknown> {
  const skip = unkeyedParams(p);
  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(p as unknown as Record<string, unknown>))
    if (!skip.has(k)) out[k] = v;
  if (p.workload === 'file-transfer')
    out.first_iteration_excluded = p.iterations >= EXCLUDE_FIRST_FROM;
  return out;
}

/** Whether two params agree on everything compare_key hashes from them. */
export function sameKeyedParams(
  a: WorkloadParams | null | undefined,
  b: WorkloadParams | null | undefined,
): boolean {
  if (!a || !b || a.workload !== b.workload) return false;
  const pa = keyedParams(a);
  const pb = keyedParams(b);
  return [...new Set([...Object.keys(pa), ...Object.keys(pb)])].every(
    (k) => JSON.stringify(pa[k] ?? null) === JSON.stringify(pb[k] ?? null),
  );
}

export interface FieldDiff {
  field: string;
  a: string;
  b: string;
}

const show = (v: unknown): string =>
  v === null || v === undefined ? 'none' : typeof v === 'object' ? JSON.stringify(v) : String(v);

/**
 * The compare_key inputs that differ between two reports: workload, params (`keyedParams`:
 * minus durations, counts and warmups that don't define the measurement), topology, path label,
 * both build profiles, methodology and the peer's host fingerprint.
 */
export function keyDiffs(a: RunReport, b: RunReport): FieldDiff[] {
  const out: FieldDiff[] = [];
  const add = (field: string, x: unknown, y: unknown) => {
    if (show(x) !== show(y)) out.push({ field, a: show(x), b: show(y) });
  };
  add('workload', a.workload, b.workload);
  if (a.workload === b.workload && a.params && b.params) {
    const pa = keyedParams(a.params);
    const pb = keyedParams(b.params);
    const keys = [...new Set([...Object.keys(pa), ...Object.keys(pb)])];
    for (const k of keys) add(`params.${k}`, pa[k], pb[k]);
  }
  add('topology', a.network.topology, b.network.topology);
  add('path label', a.network.path_label, b.network.path_label);
  add('build here', a.local.build_profile, b.local.build_profile);
  add('build on the peer', a.peer?.build_profile, b.peer?.build_profile);
  add('methodology', a.methodology, b.methodology);
  add('peer host', a.peer?.host_fingerprint, b.peer?.host_fingerprint);
  return out;
}

export type Verdict = 'better' | 'worse' | 'same';

export interface Delta {
  key: string;
  label: string;
  unit: string;
  better: 'higher' | 'lower';
  a: number | null;
  b: number | null;
  /** b − a */
  diff: number | null;
  /** (b − a) / |a| · 100 */
  pct: number | null;
  verdict: Verdict | null;
}

/** Changes within this share are "same": run-to-run noise, not a finding. */
export const SAME_WITHIN_PCT = 1;

/** Every summary key of either run, in b's order, with how it moved from a to b. */
export function deltas(a: SummaryItem[], b: SummaryItem[]): Delta[] {
  const keys = [
    ...b.map((x) => x.key),
    ...a.map((x) => x.key).filter((k) => !b.some((y) => y.key === k)),
  ];
  return keys.map((key) => {
    const ia = a.find((x) => x.key === key);
    const ib = b.find((x) => x.key === key);
    const meta = (ib ?? ia) as SummaryItem;
    const va = ia?.value ?? null;
    const vb = ib?.value ?? null;
    const diff = va != null && vb != null ? vb - va : null;
    const pct = diff != null && va ? (diff / Math.abs(va)) * 100 : diff === 0 ? 0 : null;
    let verdict: Verdict | null = null;
    if (diff != null) {
      if (pct != null && Math.abs(pct) < SAME_WITHIN_PCT) verdict = 'same';
      else if (diff === 0) verdict = 'same';
      else verdict = diff > 0 === (meta.better === 'higher') ? 'better' : 'worse';
    }
    return {
      key,
      label: meta.label,
      unit: meta.unit,
      better: meta.better,
      a: va,
      b: vb,
      diff,
      pct,
      verdict,
    };
  });
}

/**
 * The flags History shows: the engine's (`RunSummary.flags`, computed from the report) that the
 * path and topology columns don't already say, plus "CLI" from the tool.
 */
export type Flag = 'debug' | 'overlap' | 'rate-not-achieved' | 'partial' | 'imported' | 'cli';

const SHOWN: Flag[] = ['debug', 'overlap', 'rate-not-achieved', 'partial', 'imported'];

export function flagsOf(r: Pick<RunSummary, 'flags' | 'tool'>): Flag[] {
  const engine = new Set<string>(r.flags);
  const out = SHOWN.filter((f) => engine.has(f));
  if (r.tool === 'truffle-bench-cli') out.push('cli');
  return out;
}

export const FLAG_WORDS: Record<Flag, string> = {
  debug: 'debug',
  overlap: 'overlap',
  'rate-not-achieved': 'rate not reached',
  partial: 'partial',
  imported: 'imported',
  cli: 'CLI',
};

export const FLAG_TIPS: Record<Flag, string> = {
  debug: 'A debug build on either side: not comparable with release.',
  overlap: 'Another run shared the machine.',
  'rate-not-achieved': 'The sender couldn’t sustain the target rate; left out of trends.',
  partial: 'Failed or cancelled, but kept what it measured before it stopped.',
  imported: 'Imported from a report file.',
  cli: 'Run by truffle-bench on the command line.',
};

/** Runs a trend may join: ok, and not flagged as unrepresentative. */
export function trendable(r: Pick<RunSummary, 'status' | 'flags'>): boolean {
  return r.status === 'ok' && !r.flags.includes('rate-not-achieved');
}

/**
 * While a run is live its compare_key isn't known yet (it comes with the report). The latest
 * finished run of the workload may stand in for it only when that run used the same keyed params
 * against the same peer (§7.4); otherwise there is no basis, and no earlier runs are shown as
 * "comparable" until the report lands.
 */
export function liveBasis(
  live: { params: WorkloadParams | null; peer_name: string },
  latest: Pick<RunSummary, 'peer_name'> | null,
  history: RunReport | null,
): RunReport | null {
  return history &&
    latest?.peer_name === live.peer_name &&
    sameKeyedParams(history.params, live.params)
    ? history
    : null;
}

/** Both sides' build profiles as one, as `RunSummary.build_profile` has it (debug wins). */
export function combinedProfile(local: BuildProfile, peer: BuildProfile | null | undefined) {
  const all = [local, ...(peer ? [peer] : [])];
  return all.includes('debug') ? 'debug' : all.includes('unknown') ? 'unknown' : 'release';
}

/**
 * The TCP run an egress run is set against, "one bridge less" (§8.2): the newest trendable TCP
 * throughput run at the same direction and stream count, against the same peer over the same
 * path and topology, and measured the same way: run here (not imported), the same build profiles
 * (a debug build changes the numbers far more than a bridge), and with Nagle off. Durations may
 * differ, as compare_key allows. Null when there is none.
 */
export function bridgeBaseline(egress: RunReport, runs: RunSummary[]): RunSummary | null {
  const p = egress.params as unknown as { direction?: string; streams?: number };
  if (p.direction == null || p.streams == null) return null;
  const prefix = `TCP throughput · ${p.direction} · ${p.streams} stream${p.streams === 1 ? '' : 's'} ·`;
  const build = combinedProfile(egress.local.build_profile, egress.peer?.build_profile);
  return (
    runs.find(
      (x) =>
        x.workload === 'tcp' &&
        x.run_id !== egress.run_id &&
        trendable(x) &&
        x.tool !== 'imported' &&
        !x.flags.includes('imported') &&
        x.build_profile === build &&
        x.label.startsWith(prefix) &&
        !x.label.includes('Nagle on') &&
        x.peer_name === (egress.peer?.device_name ?? null) &&
        x.path_label === egress.network.path_label &&
        x.topology === egress.network.topology,
    ) ?? null
  );
}
