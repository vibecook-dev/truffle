// The browser mock, driven with fake timers: sign-in, a TCP run streaming at 4 Hz, and the
// history it serves. The mock is temporary (RFC 027 §7); these keep it honest meanwhile.
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { Api } from './api';
import type { HubEvent, Progress, RunReport, WorkloadParams } from './types';
import { REGISTRY, WORKLOAD_ORDER, isDistribution, resultAt } from '../workloads/registry';
import { flagsOf } from '../lib/compare';
import { LIVE_KEYS, MODE_EXAMPLES } from './examples';

/** Records keyed by data (connection names, HTTP statuses, error kinds), not by schema. */
const DATA_KEYED = new Set(['results.connections', 'results.status_counts', 'results.error_kinds']);

/** Where two results trees differ in shape: missing or extra keys, or a different kind of value. */
function shapeDiff(want: unknown, got: unknown, path = 'results'): string[] {
  if (want === null || got === null || DATA_KEYED.has(path)) return [];
  if (Array.isArray(want) || Array.isArray(got)) {
    if (!Array.isArray(want) || !Array.isArray(got)) return [`${path}: array vs not`];
    return want.length && got.length ? shapeDiff(want[0], got[0], `${path}[]`) : [];
  }
  if (typeof want === 'object' && typeof got === 'object') {
    const w = want as Record<string, unknown>;
    const g = got as Record<string, unknown>;
    return [
      ...Object.keys(w)
        .filter((k) => !(k in g))
        .map((k) => `${path}.${k} missing`),
      ...Object.keys(g)
        .filter((k) => !(k in w))
        .map((k) => `${path}.${k} not in the engine's`),
      ...Object.keys(w)
        .filter((k) => k in g)
        .flatMap((k) => shapeDiff(w[k], g[k], `${path}.${k}`)),
    ];
  }
  return typeof want === typeof got
    ? []
    : [`${path}: ${typeof got}, the engine's is ${typeof want}`];
}

async function makeApi(): Promise<Api> {
  (globalThis as unknown as { window: unknown }).window = globalThis;
  vi.resetModules();
  const { createMockApi } = await import('./mock');
  return createMockApi();
}

describe('mock api', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it('starts on its own with a saved state, then lists peers', async () => {
    const api = await makeApi();
    const events: HubEvent[] = [];
    api.onEvent((e) => events.push(e));
    expect((await api.hubState()).state).toBe('starting');
    await vi.advanceTimersByTimeAsync(1000);
    expect((await api.hubState()).state).toBe('running');
    const peers = await api.peersList();
    expect(peers.filter((p) => p.same_login && p.online)).toHaveLength(2);
    expect(peers.some((p) => !p.same_login)).toBe(true);
    expect(peers.some((p) => !p.online)).toBe(true);
    expect(events.some((e) => e.type === 'peers')).toBe(true);
  });

  it('runs TCP with Progress at 4 Hz and returns an ok report', async () => {
    const api = await makeApi();
    await vi.advanceTimersByTimeAsync(1000);
    const peer = (await api.peersList()).find((p) => p.device_name === 'studio');
    const params = { ...REGISTRY.tcp.defaults, duration_secs: 3 } as WorkloadParams;
    const progress: Progress[] = [];
    const done = api.benchRun({ run_id: 'run-1', peer: peer!.id, params }, (p) => progress.push(p));
    await vi.advanceTimersByTimeAsync(10_000);
    const report: RunReport = await done;
    expect(report.status).toBe('ok');
    expect(report.schema_version).toBe(3);
    expect(report.params).toEqual(params);
    const goodput = report.summary.find((s) => s.key === 'goodput_mbps');
    expect(goodput?.value).toBeGreaterThan(100);
    const running = progress.filter((p) => p.phase === 'running');
    expect(running.length).toBeGreaterThanOrEqual(11); // 3 s at 4 Hz
    for (let i = 1; i < running.length; i++)
      expect(running[i].t_ms - running[i - 1].t_ms).toBe(250);
    expect(progress[progress.length - 1].phase).toBe('done');
    const list = await api.runsList();
    expect(list[0].run_id).toBe('run-1');
  });

  it('serves a history with a failed run, a debug build, imports and a suite', async () => {
    const api = await makeApi();
    const runs = await api.runsList();
    expect(runs.some((r) => r.status === 'failed')).toBe(true);
    expect(runs.some((r) => r.build_profile === 'debug')).toBe(true);
    expect(runs.filter((r) => r.tool === 'imported').length).toBeGreaterThanOrEqual(2);
    expect(runs.some((r) => r.suite_id)).toBe(true);
  });

  it('rejects out-of-bounds params with invalid-params', async () => {
    const api = await makeApi();
    await vi.advanceTimersByTimeAsync(1000);
    const params = { ...REGISTRY.tcp.defaults, streams: 99 } as WorkloadParams;
    await expect(
      api.benchRun({ run_id: 'run-2', peer: 'studio', params }, () => undefined),
    ).rejects.toMatchObject({
      code: 'invalid-params',
    });
  });

  it('runs every mode of every workload to a report shaped like the engine’s example', async () => {
    const api = await makeApi();
    await vi.advanceTimersByTimeAsync(1000);
    const peer = (await api.peersList()).find((p) => p.device_name === 'studio')!;
    for (const [name, { workload, report: ex }] of Object.entries(MODE_EXAMPLES)) {
      const m = REGISTRY[workload];
      const params = ex.params;
      const keys = new Set<string>();
      const done = api.benchRun(
        { run_id: `mode-${name.replace(/\W/g, '-')}`, peer: peer.id, params },
        (p) => p.samples.forEach((x) => keys.add(x.key)),
      );
      await vi.advanceTimersByTimeAsync(m.estimateMs(params) + 30_000);
      const r = await done;
      expect(r.status, name).toBe('ok');
      expect(r.label, name).toBe(ex.label);
      // the engine's summary, series and results, with simulated numbers
      const meta = (x: RunReport) =>
        x.summary.map(({ key, label, unit, better }) => ({ key, label, unit, better }));
      expect(meta(r), name).toEqual(meta(ex));
      const headline = r.summary.find((x) => x.key === m.headline(params))?.value;
      const ramp = resultAt(r.results, 'limit_found');
      if (ramp === false) {
        // a ramp the run's length capped: no headline, a lower bound in results, and the note
        expect(headline, name).toBeNull();
        expect(resultAt(r.results, 'max_sustainable_mbps'), name).not.toBeNull();
        expect(
          r.notes.some((n) => n.includes('lower bound')),
          name,
        ).toBe(true);
      } else expect(headline, name).not.toBeNull();
      const series = (x: RunReport) =>
        x.series.map(({ key, label, unit }) => ({ key, label, unit }));
      expect(series(r), name).toEqual(series(ex));
      for (const s of r.series) expect(s.points.length, `${name} ${s.key}`).toBeGreaterThan(0);
      expect(shapeDiff(ex.results, r.results), name).toEqual([]);
      // live: only keys the engine sets in this mode
      for (const k of keys) expect(LIVE_KEYS[name], `${name} live ${k}`).toContain(k);
      // the engine's notes for this mode, the ones that don't quote this run's numbers
      // (a ramp's lower-bound note depends on where its numbers stopped, so it is left out too)
      const fixed = (x: RunReport) =>
        x.notes.filter((n) => !/\d/.test(n) && !n.includes('lower bound')).sort();
      expect(fixed(r), name).toEqual(fixed(ex));
      const dm = m.distribution(params);
      if (dm) expect(isDistribution(resultAt(r.results, dm.key)), name).toBe(true);
      if (m.timeBased(params)) expect(r.network.path_timeline.length, name).toBeGreaterThan(0);
    }
  }, 30_000);

  it('serves runs for every view: cancelled, rate-limited, same host, one process, CLI, suites', async () => {
    const api = await makeApi();
    const runs = await api.runsList();
    expect(runs.some((r) => r.status === 'cancelled')).toBe(true);
    expect(runs.some((r) => flagsOf(r).includes('rate-not-achieved'))).toBe(true);
    expect(runs.some((r) => flagsOf(r).includes('overlap'))).toBe(true);
    expect(runs.some((r) => r.topology === 'same-host')).toBe(true);
    expect(runs.some((r) => r.topology === 'same-process')).toBe(true);
    expect(runs.some((r) => r.tool === 'truffle-bench-cli')).toBe(true);
    expect(runs.some((r) => r.path_label === 'relay')).toBe(true);
    expect(runs.some((r) => r.path_label === 'mixed')).toBe(true);
    expect(new Set(runs.filter((r) => r.suite_id).map((r) => r.suite_id)).size).toBe(2);
    for (const w of WORKLOAD_ORDER)
      expect(
        runs.some((r) => r.workload === w),
        w,
      ).toBe(true);
    // a compare pair with different keys against the same peer
    const tcp = runs.filter(
      (r) => r.workload === 'tcp' && r.peer_name === 'studio' && r.status === 'ok',
    );
    expect(new Set(tcp.map((r) => r.compare_key)).size).toBeGreaterThan(1);
    const same = await api.runGet(runs.find((r) => r.topology === 'same-process')!.run_id);
    expect(same.resources.combined).not.toBeNull();
    expect(same.notes[0]).toMatch(/not a link/);
  });

  it('imports dropped and picked reports, skipping what isn’t one', async () => {
    const api = await makeApi();
    const before = (await api.runsList()).length;
    const pending = api.runsImport([
      '/tmp/a/transport.json',
      '/tmp/a/tsnet-bench.json',
      '/tmp/a/notes.txt',
    ]);
    await vi.advanceTimersByTimeAsync(500);
    expect(await pending).toEqual({ imported: 2, skipped: 1 });
    const picked = api.runsImportPick();
    await vi.advanceTimersByTimeAsync(500);
    expect((await picked).imported).toBe(2);
    const after = await api.runsList();
    expect(after.length).toBe(before + 4);
    expect(after.filter((r) => r.tool === 'imported').length).toBeGreaterThanOrEqual(7);
  });

  it('lists a running suite in runs_active, so a reload can follow it', async () => {
    const api = await makeApi();
    await vi.advanceTimersByTimeAsync(1000);
    const peer = (await api.peersList()).find((p) => p.device_name === 'studio')!;
    const progress: number[] = [];
    const done = api.suiteRun({ suite_id: 'suite-1', profile: 'smoke', peer: peer.id }, (p) =>
      progress.push(p.index),
    );
    await vi.advanceTimersByTimeAsync(2000);
    const active = await api.runsActive();
    expect(active[0]?.suite_id).toBe('suite-1');
    expect(await api.benchCancel('suite-1')).toBe(true);
    const result = await done;
    expect(result.cancelled).toBe(true);
    expect(progress[0]).toBe(0);
  });
});
