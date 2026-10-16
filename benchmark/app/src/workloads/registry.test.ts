import { describe, expect, it } from 'vitest';
import { WORKLOADS, type WorkloadParams } from '../api/types';
import {
  REGISTRY,
  WORKLOAD_ORDER,
  editParams,
  lowerBoundOf,
  paramsLabel,
  validateParams,
  visibleFields,
} from './registry';

describe('workload registry', () => {
  it('covers every workload, in the rail order', () => {
    expect([...WORKLOAD_ORDER].sort()).toEqual([...WORKLOADS].sort());
    for (const w of WORKLOADS) expect(REGISTRY[w].id).toBe(w);
  });
  it('has defaults and presets that validate', () => {
    for (const w of WORKLOADS) {
      const m = REGISTRY[w];
      expect(m.defaults.workload).toBe(w);
      expect(validateParams(m, m.defaults), `${w} defaults`).toBeNull();
      for (const p of m.presets) {
        const params = { ...m.defaults, ...p.params } as WorkloadParams;
        expect(validateParams(m, params), `${w} preset ${p.id}`).toBeNull();
      }
    }
  });
  it('rejects out-of-bounds values and never clamps them', () => {
    const m = REGISTRY.tcp;
    const p = { ...m.defaults, streams: 17 } as WorkloadParams;
    expect(validateParams(m, p)).toMatch(/Streams/);
    expect((p as { streams: number }).streams).toBe(17);
  });
  it('applies the cross-field rules of §7.3', () => {
    expect(
      validateParams(REGISTRY.tcp, {
        ...REGISTRY.tcp.defaults,
        duration_secs: 2,
        warmup_secs: 1,
      } as WorkloadParams),
    ).toMatch(/Warmup/);
    expect(
      validateParams(REGISTRY.path, { workload: 'path', count: 200, interval_ms: 5000 }),
    ).toMatch(/300 s/);
    expect(
      validateParams(REGISTRY['file-transfer'], {
        workload: 'file-transfer',
        size_bytes: 1073741824,
        iterations: 5,
      }),
    ).toMatch(/4 GiB/);
  });
  it('resets an invalid value that a mode switch hides, so nothing unseen blocks Run', () => {
    // [workload, field, invalid value, the edit that hides it]
    const cases: [keyof typeof REGISTRY, string, number, [string, string]][] = [
      ['tcp', 'streams', 20, ['mode', 'latency']],
      ['tcp', 'warmup_secs', 15, ['mode', 'latency']],
      ['udp', 'rate_mbps', 5000, ['mode', 'echo']],
    ];
    for (const [w, field, value, [key, v]] of cases) {
      const m = REGISTRY[w];
      const typed = editParams(m, m.defaults, {}, field, value, false);
      expect(typed.bad[field], `${w} ${field}`).toBe(true);
      expect((typed.params as unknown as Record<string, unknown>)[field]).toBe(value);
      const hidden = editParams(m, typed.params, typed.bad, key, v, true);
      const p = hidden.params as unknown as Record<string, unknown>;
      expect(p[key]).toBe(v);
      expect(p[field], `${w} ${field} back to its default`).toBe(
        (m.defaults as unknown as Record<string, unknown>)[field],
      );
      expect(Object.values(hidden.bad).some(Boolean), `${w} ${field}`).toBe(false);
      expect(validateParams(m, hidden.params), `${w} ${field}`).toBeNull();
    }
    // a visible invalid field stays as typed, and stays marked
    const m = REGISTRY.tcp;
    const a = editParams(m, m.defaults, {}, 'streams', 20, false);
    const b = editParams(m, a.params, a.bad, 'direction', 'download', true);
    expect((b.params as { streams: number }).streams).toBe(20);
    expect(b.bad.streams).toBe(true);
  });

  it('reads a capped udp ramp’s lower bound, and nothing else', () => {
    const ramp = (limit_found: boolean) => ({
      results: { mode: 'ramp', max_sustainable_mbps: 22.5, limit_found },
    });
    expect(lowerBoundOf(ramp(false), 'max_sustainable_mbps')).toBe(22.5);
    expect(lowerBoundOf(ramp(true), 'max_sustainable_mbps')).toBeNull();
    expect(lowerBoundOf(ramp(false), 'goodput_mbps')).toBeNull();
    expect(
      lowerBoundOf({ results: { max_sustainable_mbps: 5 } }, 'max_sustainable_mbps'),
    ).toBeNull();
    expect(lowerBoundOf(null, 'max_sustainable_mbps')).toBeNull();
  });

  it('shows only the fields that apply', () => {
    const latency = { ...REGISTRY.tcp.defaults, mode: 'latency' } as WorkloadParams;
    const keys = visibleFields(REGISTRY.tcp, latency).map((f) => f.key);
    expect(keys).toContain('payload_bytes');
    expect(keys).not.toContain('streams');
  });
  it('labels runs exactly as the engine’s label() does', () => {
    const p = (k: keyof typeof REGISTRY, over: Record<string, unknown> = {}) =>
      paramsLabel({ ...REGISTRY[k].defaults, ...over } as WorkloadParams);
    // the cases of benchmark/engine/src/params.rs `labels()`
    expect(p('tcp', { streams: 4 })).toBe('TCP throughput · upload · 4 streams · 10 s');
    expect(p('tcp')).toBe('TCP throughput · upload · 1 stream · 10 s');
    expect(p('tcp', { mode: 'latency', payload_bytes: 32768, nodelay: false })).toBe(
      'TCP latency · 32 KiB · 200 samples · Nagle on',
    );
    expect(p('file-transfer')).toBe('File transfer · 10 MiB × 3');
    expect(p('video')).toBe('Video · 1080p30 · UDP · 6 Mb/s · paced · 10 s');
    expect(p('udp')).toBe('UDP stream · upload · 10 Mb/s · 1200 B · 10 s');
    // and the rest
    expect(p('path')).toBe('Path · 20 pings · 250 ms apart');
    expect(p('udp', { mode: 'ramp' })).toBe('UDP ramp · upload · from 10 Mb/s · 1200 B · 10 s');
    expect(p('ws')).toBe('WebSocket request/reply · 64 B · 500 requests');
    expect(p('ws', { mode: 'stream', rate_per_sec: 2000 })).toBe(
      'WebSocket stream · 64 B · 2000 msg/s · 10 s',
    );
    expect(p('synced-store')).toBe('SyncedStore ping-pong · 100 writes · 64 B');
    expect(p('http')).toBe('HTTP · proxy · keep-alive · 4 workers · 1 KiB · 10 s');
    expect(p('egress', { streams: 2 })).toBe('Egress · upload · 2 streams · 10 s');
  });
  it('mirrors the engine’s mode-dependent rules', () => {
    // udp echo: warmup datagrams < pps × duration; ramp shows a direction and a start rate
    const echo = {
      ...REGISTRY.udp.defaults,
      mode: 'echo',
      pps: 1,
      duration_secs: 5,
      warmup_count: 5,
    };
    expect(validateParams(REGISTRY.udp, echo as WorkloadParams)).toMatch(/Warmup datagrams/);
    const ramp = { ...REGISTRY.udp.defaults, mode: 'ramp' } as WorkloadParams;
    const keys = visibleFields(REGISTRY.udp, ramp).map((f) => f.label);
    expect(keys).toContain('Direction');
    expect(keys).toContain('Start rate');
    // synced-store burst ignores the warmup count
    const burst = {
      ...REGISTRY['synced-store'].defaults,
      mode: 'burst',
      count: 10,
      warmup_count: 50,
    };
    expect(validateParams(REGISTRY['synced-store'], burst as WorkloadParams)).toBeNull();
  });
  it('gives every workload a view: main chart, panels and a data path for every mode', () => {
    for (const w of WORKLOADS) {
      const m = REGISTRY[w];
      const variants = [
        m.defaults,
        ...m.presets.map((p) => ({ ...m.defaults, ...p.params }) as WorkloadParams),
      ];
      for (const v of variants) {
        expect(m.main(v).title, w).toBeTruthy();
        expect(m.panels(v).length, w).toBeGreaterThan(0);
        expect(m.measured(v).length, w).toBeGreaterThan(0);
        expect(m.notes(v).length, w).toBeGreaterThan(0);
        expect(
          m.kpis(v).map((k) => k.key),
          w,
        ).toContain(m.headline(v));
      }
    }
  });
  it('gives every workload a headline among its KPIs', () => {
    for (const w of WORKLOADS) {
      const m = REGISTRY[w];
      const keys = m.kpis(m.defaults).map((k) => k.key);
      expect(keys, w).toContain(m.headline(m.defaults));
      expect(m.hops(m.defaults).length, w).toBeGreaterThan(0);
    }
  });
});
