import { describe, expect, it } from 'vitest';
import type { RunReport, RunSummary, SummaryItem, WorkloadParams } from '../api/types';
import { REGISTRY } from '../workloads/registry';
import {
  bridgeBaseline,
  deltas,
  flagsOf,
  keyDiffs,
  keyedParams,
  liveBasis,
  sameKeyedParams,
  shownRunOf,
  trendable,
  unkeyedParams,
} from './compare';
import egressOk from '../../../schema/fixtures/report-egress-ok.json';
import runSummary from '../../../schema/fixtures/run-summary.json';
import { mergeNotes } from './notes';

const item = (
  key: string,
  value: number | null,
  better: 'higher' | 'lower' = 'higher',
): SummaryItem => ({
  key,
  label: key,
  value,
  unit: 'Mb/s',
  better,
  uncertainty: null,
});

function report(over: Record<string, unknown> = {}): RunReport {
  const base = {
    workload: 'tcp',
    methodology: 'v3',
    compare_key: 'k',
    params: { workload: 'tcp', mode: 'throughput', streams: 1, duration_secs: 10 },
    network: { topology: 'separate-hosts', path_label: 'direct' },
    local: { build_profile: 'release' },
    peer: { build_profile: 'release', host_fingerprint: 'aaaa' },
  };
  return { ...base, ...over } as unknown as RunReport;
}

describe('the run a benchmark view shows', () => {
  const run = (id: string, mode: string | null, over: Partial<RunSummary> = {}) =>
    ({ run_id: id, workload: 'tcp', mode, tool: 'truffle-bench-app', ...over }) as RunSummary;
  // newest first, as runs_list returns them
  const runs = [
    run('lat-2', 'latency'),
    run('imported-tp', 'throughput', { tool: 'imported' }),
    run('tp-1', 'throughput'),
    run('lat-1', 'latency'),
    run('udp-1', 'stream', { workload: 'udp' }),
  ];
  it('is the newest run of the workload in the form’s mode, never an imported one', () => {
    expect(shownRunOf(runs, 'tcp', 'throughput')).toEqual({ latest: runs[2], other: null });
    expect(shownRunOf(runs, 'tcp', 'latency')).toEqual({ latest: runs[0], other: null });
  });
  it('links the newest run of another mode when the form’s mode has none', () => {
    expect(shownRunOf(runs, 'udp', 'echo')).toEqual({ latest: null, other: runs[4] });
    expect(shownRunOf(runs, 'quic', 'latency')).toEqual({ latest: null, other: null });
  });
  it('takes the newest run for a workload without modes, or a summary without one', () => {
    const plain = [run('p-2', null, { workload: 'path' }), run('p-1', null, { workload: 'path' })];
    expect(shownRunOf(plain, 'path', null).latest?.run_id).toBe('p-2');
    expect(shownRunOf([run('old', null)], 'tcp', 'latency').latest?.run_id).toBe('old');
  });
});

describe('compare', () => {
  it('lists the compare_key fields that differ, ignoring durations and counts', () => {
    const a = report();
    const b = report({
      params: { workload: 'tcp', mode: 'throughput', streams: 4, duration_secs: 3 },
    });
    expect(keyDiffs(a, b)).toEqual([{ field: 'params.streams', a: '1', b: '4' }]);
    const c = report({
      network: { topology: 'separate-hosts', path_label: 'relay' },
      local: { build_profile: 'debug' },
    });
    expect(keyDiffs(a, c).map((d) => d.field)).toEqual(['path label', 'build here']);
  });

  it('keys the run-length fields that define a measurement, as the engine does (§7.4)', () => {
    const p = (w: keyof typeof REGISTRY, over: Record<string, unknown> = {}) =>
      ({ ...REGISTRY[w].defaults, ...over }) as WorkloadParams;
    // ordinary runs: durations, counts and warmups stay out
    expect(unkeyedParams(p('tcp')).has('duration_secs')).toBe(true);
    expect(sameKeyedParams(p('tcp', { duration_secs: 3 }), p('tcp', { duration_secs: 30 }))).toBe(
      true,
    );
    // a synced-store burst's count is its burst size
    expect(unkeyedParams(p('synced-store', { mode: 'burst' })).has('count')).toBe(false);
    expect(
      sameKeyedParams(
        p('synced-store', { mode: 'burst', count: 100 }),
        p('synced-store', { mode: 'burst', count: 1000 }),
      ),
    ).toBe(false);
    expect(unkeyedParams(p('synced-store', { mode: 'pingpong' })).has('count')).toBe(true);
    // a udp ramp's duration decides how many steps fit; its warmup stays out
    const ramp = unkeyedParams(p('udp', { mode: 'ramp' }));
    expect(ramp.has('duration_secs')).toBe(false);
    expect(ramp.has('warmup_secs')).toBe(true);
    expect(unkeyedParams(p('udp', { mode: 'stream' })).has('duration_secs')).toBe(true);
    // file-transfer: iterations stay out, but whether iteration 1 is excluded is keyed
    expect(keyedParams(p('file-transfer', { iterations: 2 }))).toMatchObject({
      first_iteration_excluded: false,
    });
    expect(keyedParams(p('file-transfer', { iterations: 3 }))).toMatchObject({
      first_iteration_excluded: true,
    });
    expect('iterations' in keyedParams(p('file-transfer'))).toBe(false);
    expect(
      sameKeyedParams(p('file-transfer', { iterations: 3 }), p('file-transfer', { iterations: 9 })),
    ).toBe(true);
    expect(
      sameKeyedParams(p('file-transfer', { iterations: 2 }), p('file-transfer', { iterations: 3 })),
    ).toBe(false);
    // and keyDiffs names the field
    const a = report({ workload: 'udp', params: p('udp', { mode: 'ramp', duration_secs: 10 }) });
    const b = report({ workload: 'udp', params: p('udp', { mode: 'ramp', duration_secs: 30 }) });
    expect(keyDiffs(a, b).map((d) => d.field)).toEqual(['params.duration_secs']);
  });

  it('lets a live run borrow the latest run’s key only for the same measurement on the same peer', () => {
    const tcp = (over: Record<string, unknown>) =>
      ({ ...REGISTRY.tcp.defaults, ...over }) as WorkloadParams;
    const history = report({ params: tcp({ streams: 1, direction: 'upload' }) });
    const latest = { peer_name: 'studio' };
    const live = (params: WorkloadParams | null, peer_name = 'studio') => ({ params, peer_name });
    // same keyed params (a different duration is still the same measurement)
    expect(liveBasis(live(tcp({ streams: 1, duration_secs: 30 })), latest, history)).toBe(history);
    // 4 streams both ways is another measurement: no earlier bars while it runs
    expect(liveBasis(live(tcp({ streams: 4, direction: 'bidirectional' })), latest, history)).toBe(
      null,
    );
    // another peer, or a reattached run whose params aren't known
    expect(liveBasis(live(tcp({ streams: 1 }), 'laptop'), latest, history)).toBeNull();
    expect(liveBasis(live(null), latest, history)).toBeNull();
    expect(liveBasis(live(tcp({ streams: 1 })), latest, null)).toBeNull();
  });

  it('sets egress against a TCP run measured the same way, never a debug, Nagle or imported one', () => {
    const egress = egressOk as unknown as RunReport;
    const tcp = (over: Partial<RunSummary>): RunSummary =>
      ({
        ...(runSummary as unknown as RunSummary),
        flags: [],
        topology: egress.network.topology,
        path_label: egress.network.path_label,
        peer_name: egress.peer?.device_name ?? null,
        tool: 'truffle-bench-app',
        ...over,
      }) as RunSummary;
    const good = tcp({ run_id: 'good', started_at: '2026-10-15T08:00:00.000Z' });
    const newer = (over: Partial<RunSummary>) =>
      tcp({ run_id: `newer-${Object.keys(over).join('-')}`, ...over });
    expect(bridgeBaseline(egress, [good])?.run_id).toBe('good');
    // each of these is newer, and each is skipped for the good one
    const skipped = [
      newer({ build_profile: 'debug', flags: ['debug'] }),
      newer({ label: 'TCP throughput · upload · 1 stream · 10 s · Nagle on' }),
      newer({ tool: 'imported', flags: ['imported'] }),
      newer({ flags: ['rate-not-achieved'] }),
      newer({ status: 'failed' }),
      newer({ label: 'TCP throughput · upload · 4 streams · 10 s' }),
      newer({ label: 'TCP throughput · download · 1 stream · 10 s' }),
      newer({ path_label: 'relay' }),
      newer({ peer_name: 'someone-else' }),
    ];
    expect(bridgeBaseline(egress, [...skipped, good])?.run_id).toBe('good');
    expect(bridgeBaseline(egress, skipped)).toBeNull();
    // a different duration is the same measurement, as compare_key has it
    expect(
      bridgeBaseline(egress, [newer({ label: 'TCP throughput · upload · 1 stream · 30 s' })]),
    ).not.toBeNull();
  });

  it('says whether each value got better, worse or stayed the same', () => {
    const d = deltas(
      [item('goodput_mbps', 600), item('rtt_ms', 2, 'lower'), item('gone', 1)],
      [item('goodput_mbps', 900), item('rtt_ms', 3, 'lower'), item('jitter', 0.3)],
    );
    expect(d.map((x) => [x.key, x.verdict])).toEqual([
      ['goodput_mbps', 'better'],
      ['rtt_ms', 'worse'],
      ['jitter', null],
      ['gone', null],
    ]);
    expect(d[0].pct).toBeCloseTo(50);
    expect(deltas([item('x', 100)], [item('x', 100.5)])[0].verdict).toBe('same');
  });

  it('names each value the way today’s engine does, even in older reports', () => {
    const old = (key: string, label: string): SummaryItem => ({ ...item(key, 1), label });
    const d = deltas(
      [old('rtt_p50_ms', 'Round trip p50'), old('req_per_sec', 'Requests/s')],
      [
        old('loaded_rtt_p50_ms', 'RTT under load (p50)'),
        old('loss_pct', 'Lost'),
        old('goodput_mbps', 'Goodput'),
      ],
    );
    expect(Object.fromEntries(d.map((x) => [x.key, x.label]))).toEqual({
      rtt_p50_ms: 'RTT p50',
      req_per_sec: 'Requests per second',
      loaded_rtt_p50_ms: 'RTT p50 under load',
      loss_pct: 'Loss',
      goodput_mbps: 'Goodput',
    });
  });

  it('shows the engine’s flags the other columns don’t, and keeps rate-limited runs out of trends', () => {
    const run = {
      status: 'ok',
      tool: 'truffle-bench-cli',
      flags: ['rate-not-achieved', 'same-process', 'mixed-path', 'debug', 'from-a-newer-engine'],
    } as unknown as RunSummary;
    expect(flagsOf(run)).toEqual(['debug', 'rate-not-achieved', 'cli']);
    expect(trendable(run)).toBe(false);
    expect(trendable({ ...run, flags: ['same-process'] })).toBe(true);
    expect(trendable({ ...run, status: 'failed', flags: ['partial'] })).toBe(false);
  });
});

describe('notes', () => {
  it('says each note once, keeping the longer wording', () => {
    expect(
      mergeNotes(
        ['loss is end to end, including loopback relay drops', 'SHA-256 of page-cached data'],
        [
          'Loss is end to end, including loopback relay drops.',
          'SHA-256 of page-cached data: a cold file reads slower.',
          'new: one WhoIs per request on both paths',
          'New connections: one WhoIs per request on both paths.',
          'broadcast_typed fans out',
        ],
      ),
    ).toEqual([
      'Loss is end to end, including loopback relay drops.',
      'SHA-256 of page-cached data: a cold file reads slower.',
      'New connections: one WhoIs per request on both paths.',
      'broadcast_typed fans out.',
    ]);
  });

  it('matches the engine’s straight apostrophes and shows typographic ones', () => {
    expect(
      mergeNotes(
        [
          "egress removes only the initiator-side Rust bridge, and the responder's sink still uses one",
        ],
        [
          'Egress removes only the initiator-side Rust bridge, and the responder’s sink still uses one.',
        ],
      ),
    ).toEqual([
      'Egress removes only the initiator-side Rust bridge, and the responder’s sink still uses one.',
    ]);
  });
});
