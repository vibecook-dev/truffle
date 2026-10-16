// The registry against the engine: for every per-mode example report the engine generates
// (api/examples.ts), the registry entry for that workload and mode may only reference what the
// engine emits. KPI keys are summary keys (with the same units), chart series are series, dotted
// facts paths resolve in results with the type their format expects, the distribution is a
// Distribution, each panel finds its data, live keys are Progress sample keys the engine sets,
// and the registry's notes de-duplicate against the engine's.

import { describe, expect, it } from 'vitest';
import { LIVE_KEYS, MODE_EXAMPLES } from '../api/examples';
import { KEY_LOADED_RTT, resultsOf, type RunReport } from '../api/types';
import { mergeNotes, noteKey } from '../lib/notes';
import {
  REGISTRY,
  WORKLOAD_ORDER,
  headlineOf,
  isDistribution,
  resultAt,
  type FactMeta,
  type Panel,
} from './registry';

/** `params-<workload>.json`: the engine's `WorkloadParams::defaults()`, one per workload. */
const ENGINE_DEFAULTS = import.meta.glob<unknown>('../../../schema/fixtures/params-*.json', {
  eager: true,
  import: 'default',
});

const STOP = new Set(
  'a an and are as at be by for from in is it its of on or so the this to with'.split(' '),
);
const words = (n: string) =>
  new Set(
    n
      .toLowerCase()
      .replace(/[^a-z0-9.%>≤−-]+/g, ' ')
      .split(' ')
      .map((w) => w.replace(/^[.-]+|[.-]+$/g, ''))
      .filter((w) => w.length > 1 && !STOP.has(w)),
  );

/** Two notes that say the same thing in other words: most of the shorter one's words recur. */
function nearDuplicate(a: string, b: string): boolean {
  const wa = words(a);
  const wb = words(b);
  const small = Math.min(wa.size, wb.size);
  if (small < 2) return false;
  const shared = [...wa].filter((w) => wb.has(w)).length;
  return shared / small >= 0.75;
}

function factOk(f: FactMeta, v: unknown): boolean {
  switch (f.format) {
    case 'bool':
      return typeof v === 'boolean';
    case 'text':
      return typeof v === 'string';
    case 'count':
      return Array.isArray(v);
    default:
      // int, num, bytes: a number, or null where the engine has nothing to say
      return v === null || typeof v === 'number';
  }
}

const seriesKeys = (r: RunReport) => r.series.map((s) => s.key);
const summaryKeys = (r: RunReport) => r.summary.map((s) => s.key);

describe('registry defaults', () => {
  it('are the engine’s defaults, value for value', () => {
    for (const w of WORKLOAD_ORDER) {
      const engine = ENGINE_DEFAULTS[`../../../schema/fixtures/params-${w}.json`];
      expect(engine, `params-${w}.json`).toBeDefined();
      expect(REGISTRY[w].defaults, w).toEqual(engine);
    }
  });
});

describe('registry against the engine’s per-mode examples', () => {
  it('covers every example with a list of live keys', () => {
    expect(Object.keys(LIVE_KEYS).sort()).toEqual(Object.keys(MODE_EXAMPLES).sort());
  });

  for (const [name, { workload, report }] of Object.entries(MODE_EXAMPLES)) {
    describe(name, () => {
      const meta = REGISTRY[workload];
      const p = report.params;
      const results = resultsOf(report);
      const live = LIVE_KEYS[name];

      it('has results to read', () => {
        expect(report.status).toBe('ok');
        expect(results).not.toBeNull();
      });

      it('shows KPIs the summary has, in its units, live only from keys the engine sets', () => {
        for (const k of meta.kpis(p)) {
          const item = report.summary.find((s) => s.key === k.key);
          expect(item, `KPI ${k.key} in summary [${summaryKeys(report)}]`).toBeDefined();
          expect(k.unit, `unit of ${k.key}`).toBe(item?.unit);
          if (k.live) expect(live, `live key ${k.live} of KPI ${k.key}`).toContain(k.live);
        }
      });

      it('heads History, Suite and Overview with the view’s headline KPI', () => {
        expect(headlineOf(report)?.key).toBe(meta.headline(p));
        expect(meta.kpis(p)[0]?.key).toBe(meta.headline(p));
      });

      it('draws its main chart from series and summary keys that exist', () => {
        const main = meta.main(p);
        for (const g of main.guides ?? [])
          expect(summaryKeys(report), `guide ${g.key}`).toContain(g.key);
        if (main.kind === 'iterations') {
          expect(resultAt(results, 'iterations')).toBeInstanceOf(Array);
          for (const s of main.series)
            expect(live, `live per-file key ${s.liveKey ?? s.key}`).toContain(s.liveKey ?? s.key);
          expect(live).toContain('iterations_done');
        } else if (main.kind === 'burst') {
          expect(summaryKeys(report)).toEqual(
            expect.arrayContaining(['burst_write_ms', 'final_echo_ms']),
          );
        } else {
          for (const s of main.series) {
            expect(seriesKeys(report), `main series ${s.key}`).toContain(s.key);
            expect(live, `live key of main series ${s.key}`).toContain(s.liveKey ?? s.key);
          }
        }
      });

      it('tabulates a distribution that is one', () => {
        const dm = meta.distribution(p);
        if (dm) {
          expect(isDistribution(resultAt(results, dm.key)), `results.${dm.key}`).toBe(true);
          const raw = dm.raw ? dm.raw(results) : resultAt(results, 'samples_ms');
          expect(raw, 'raw samples for the CDF').toBeInstanceOf(Array);
          expect((raw as number[]).length).toBeGreaterThan(0);
        } else if (meta.timeBased(p)) {
          // the right pane tabulates round trips under load instead
          expect(report.network.loaded_rtt_ms).not.toBeNull();
        }
      });

      it('lists facts whose dotted paths resolve, with the type their format expects', () => {
        for (const f of meta.facts(p)) {
          const v = resultAt(results, f.key);
          expect(v, `results.${f.key}`).not.toBeUndefined();
          expect(
            factOk(f, v),
            `results.${f.key} = ${JSON.stringify(v)} as ${f.format ?? 'num'}`,
          ).toBe(true);
        }
      });

      it('finds every panel’s data', () => {
        for (const panel of meta.panels(p) as Panel[]) {
          switch (panel.kind) {
            case 'cdf': {
              const dm = meta.distribution(p);
              expect(dm, 'cdf needs a distribution').not.toBeNull();
              break;
            }
            case 'loaded-rtt':
              expect(meta.timeBased(p)).toBe(true);
              expect(report.network.path_timeline.length).toBeGreaterThan(0);
              expect(live).toContain(KEY_LOADED_RTT);
              break;
            case 'loss':
              expect(seriesKeys(report)).toContain('loss_pct');
              expect(live).toContain('loss_pct');
              break;
            case 'frames': {
              const frames = resultAt(results, 'frames') as Record<string, unknown>[];
              expect(frames.length).toBeGreaterThan(0);
              for (const k of ['frame_no', 'keyframe', 'bytes', 'state', 'latency_ms'])
                expect(frames[0], `frames[].${k}`).toHaveProperty(k);
              expect(typeof resultAt(results, 'fps')).toBe('number');
              break;
            }
            case 'phases': {
              const its = resultAt(results, 'iterations') as Record<string, unknown>[];
              expect(its.length).toBeGreaterThan(0);
              for (const k of ['index', 'hash_ms', 'negotiate_ms', 'transfer_ms', 'excluded', 'ok'])
                expect(its[0], `iterations[].${k}`).toHaveProperty(k);
              break;
            }
            case 'line':
              for (const s of panel.series) {
                expect(seriesKeys(report), `panel series ${s.key}`).toContain(s.key);
                expect(live, `live key of panel series ${s.key}`).toContain(s.liveKey ?? s.key);
              }
              if (panel.ref) {
                expect(typeof resultAt(results, panel.ref.resultKey)).toBe('number');
                expect(live, `live ${panel.ref.resultKey}`).toContain(panel.ref.resultKey);
              }
              break;
            case 'bridge':
              expect(summaryKeys(report)).toContain('goodput_mbps');
              break;
            case 'compare':
              expect(summaryKeys(report)).toContain(meta.kpis(p)[0].key);
              break;
          }
        }
      });

      it('says each of the engine’s notes once, in the engine’s words', () => {
        const merged = mergeNotes(report.notes, meta.notes(p));
        for (let i = 0; i < merged.length; i++)
          for (let j = i + 1; j < merged.length; j++)
            expect(
              nearDuplicate(merged[i], merged[j]),
              `near-duplicate notes:\n  ${merged[i]}\n  ${merged[j]}`,
            ).toBe(false);
        // the engine's notes all survive (a registry note may only extend one, never replace it)
        for (const n of report.notes)
          expect(
            merged.some((m) => noteKey(m).startsWith(noteKey(n))),
            `engine note kept: ${n}`,
          ).toBe(true);
      });
    });
  }
});
