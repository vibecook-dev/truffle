// The pieces a run is drawn with, shared by the benchmark views (live and finished), Run detail
// and Compare. Everything workload-specific comes from the registry; numbers come from the
// report, or while running from the live Progress samples.

import type { ReactNode } from 'react';
import {
  KEY_LOADED_RTT,
  resultsOf,
  type Distribution,
  type FileTransferIteration,
  type NodeEnv,
  type PathPoint,
  type PathSnapshot,
  type ResourceStats,
  type RunReport,
  type RunSummary,
  type WorkloadParams,
} from '../api';
import type { LiveRun } from '../state/store';
import {
  BarCompare,
  CdfChart,
  FrameStrip,
  IterationBars,
  LineChart,
  PathTimeline,
  PhaseBars,
  quantilesOf,
  type BarItem,
  type FrameCell,
  type IterationBar,
  type LineSeries,
  type PhaseRow,
} from '../charts';
import { Table, num } from '../components/Data';
import { Status } from '../components/Status';
import {
  DASH,
  fmtBytes,
  fmtConnection,
  fmtDuration,
  fmtInt,
  fmtNumber,
  fmtWhen,
  withUnit,
} from '../lib/format';
import {
  isDistribution,
  resultAt,
  textOf,
  videoFps,
  visibleFields,
  type FactMeta,
  type MainChartMeta,
  type Panel,
  type WorkloadMeta,
} from '../workloads/registry';
import { bridgeBaseline } from '../lib/compare';

/** What the charts read: a live run, or a finished report, plus earlier comparable runs. */
export interface RunData {
  meta: WorkloadMeta;
  params: WorkloadParams;
  report: RunReport | null;
  /** The live run while it streams (no report yet). */
  live: LiveRun | null;
  running: boolean;
  /** The latest earlier run with the same compare_key, for baselines. */
  prev: RunReport | null;
  /** Earlier runs with the same compare_key, newest first. */
  comparable: RunSummary[];
  /** The target's connection, for live path samples. */
  connection?: string | null;
  /** Every run in History, for panels that look across workloads (egress against TCP). */
  allRuns?: RunSummary[];
}

const PHASE_WORDS: Record<string, string> = {
  preparing: 'Preparing the peer',
  settling: 'Settling the path',
  clock: 'Estimating the clock offset',
  warmup: 'Warming up',
  running: 'Measuring',
  draining: 'Draining',
  finishing: 'Collecting results',
  done: 'Done',
  failed: 'Failed',
  cancelled: 'Cancelled',
};
export const phaseWords = (p: string) => PHASE_WORDS[p] ?? p;

const isLive = (d: RunData) => d.running || (!!d.live && !d.live.report);

/** The finished run's own results; null while live, for `{}` and for imported reports. */
function results(d: RunData): unknown {
  return d.report ? resultsOf(d.report) : null;
}

/** A series: live samples while streaming, else the report's. */
export function pointsOf(d: RunData, key: string, liveKey?: string): [number, number][] {
  if (isLive(d)) return d.live?.series[liveKey ?? key] ?? [];
  return d.report?.series.find((x) => x.key === key)?.points ?? [];
}

function dataMs(d: RunData): number | null {
  if (!d.meta.timeBased(d.params)) return null;
  const p = d.params as unknown as Record<string, number>;
  return ((p.warmup_secs ?? 0) + (p.duration_secs ?? 0)) * 1000;
}

function warmupMs(d: RunData, key: string | undefined): number | null {
  if (isLive(d)) {
    const p = d.params as unknown as Record<string, number>;
    return dataMs(d) && !d.live?.attached ? (p.warmup_secs ?? 0) * 1000 || null : null;
  }
  return d.report?.series.find((x) => x.key === key)?.warmup_ms ?? null;
}

const summaryValue = (r: RunReport | null, key: string) =>
  r?.summary.find((x) => x.key === key)?.value ?? null;

/** The clock offset uncertainty in ms, for the video latency band. */
function uncertaintyMs(d: RunData): number | null {
  if (isLive(d)) return d.live?.last.clock_uncertainty_ms ?? null;
  const ns = d.report?.network.clock?.uncertainty_ns;
  return ns == null ? null : ns / 1e6;
}

// ---------------------------------------------------------------- main chart

export function MainChart({ data, height = 240 }: { data: RunData; height?: number }) {
  const m: MainChartMeta = data.meta.main(data.params);
  const live = isLive(data);
  const r = data.report;
  const guides = !live
    ? (m.guides ?? [])
        .map((g) => ({ label: g.label, value: summaryValue(r, g.key) }))
        .filter((g): g is { label: string; value: number } => g.value != null)
    : [];
  const meta = live
    ? data.live?.attached
      ? `${phaseWords(data.live.phase)} · reattached, earlier samples arrive with the report`
      : phaseWords(data.live?.phase ?? 'running')
    : r
      ? r.label
      : 'No run yet';

  if (m.kind === 'iterations') {
    const its = iterationsOf(data);
    const items: IterationBar[] = its.map((it) => ({
      key: String(it.index),
      label: `File ${it.index}`,
      value: it.wire_mbps,
      kind: it.failed ? 'failed' : it.excluded ? 'excluded' : 'current',
    }));
    const p = data.params as unknown as { iterations?: number };
    if (live && p.iterations && items.length < p.iterations)
      items.push({
        key: 'next',
        label: `File ${items.length + 1}`,
        value: null,
        kind: 'running',
      });
    const prevMedian = summaryValue(data.prev, 'wire_mbps');
    return (
      <IterationBars
        title={m.title}
        meta={meta}
        items={items}
        unit={m.unit}
        height={height}
        baseline={
          !live && prevMedian != null
            ? {
                label: `Previous run’s median, ${fmtWhen(data.prev?.started_at)}`,
                value: prevMedian,
              }
            : null
        }
        guide={guides[0] ?? null}
        emptyText={live ? 'Waiting for the first file' : 'Run it to see the result here.'}
      />
    );
  }

  if (m.kind === 'burst') {
    const write = live ? null : summaryValue(r, 'burst_write_ms');
    const echo = live ? null : summaryValue(r, 'final_echo_ms');
    const prevWrite = summaryValue(data.prev, 'burst_write_ms');
    const prevEcho = summaryValue(data.prev, 'final_echo_ms');
    const rows: PhaseRow[] = [];
    if (write != null || echo != null)
      rows.push({
        key: 'cur',
        label: 'This run',
        values: { write, echo },
      });
    if (!live && prevWrite != null)
      rows.push({
        key: 'prev',
        label: 'Previous run',
        sub: fmtWhen(data.prev?.started_at),
        values: { write: prevWrite, echo: prevEcho },
        dim: true,
      });
    return (
      <PhaseBars
        title={m.title}
        meta={meta}
        segments={[
          { key: 'write', label: 'Writing every value', fill: 'ink3' },
          { key: 'echo', label: 'Until the last echo', fill: 'ink' },
        ]}
        rows={rows}
        emptyText={
          live
            ? 'The burst is timed as a whole: it arrives with the report.'
            : 'Run it to see the result here.'
        }
      />
    );
  }

  const series: LineSeries[] = m.series.map((sm, i) => ({
    key: sm.key,
    label: sm.label,
    area: sm.area,
    points: pointsOf(data, sm.key, sm.liveKey),
    band: m.band && i === 0 ? uncertaintyMs(data) : null,
  }));
  const prevSeries = data.prev?.series.find((x) => x.key === m.series[0]?.key);
  if (prevSeries && !live && series.length === 1) {
    series.push({
      key: `prev-${prevSeries.key}`,
      label: `Previous run, ${fmtWhen(data.prev?.started_at)}`,
      points: prevSeries.points,
      baseline: true,
    });
  }
  // where a failed run stopped: its last point, in redline
  const first = series.find((x) => !x.baseline)?.points ?? [];
  const failEnd: [number, number][] =
    r?.status === 'failed' && !live && first.length ? [first[first.length - 1]] : [];
  return (
    <LineChart
      title={m.title}
      meta={meta}
      series={series}
      unit={m.unit}
      height={height}
      warmupMs={warmupMs(data, m.series[0]?.key)}
      domainMs={live && !data.live?.attached ? dataMs(data) : null}
      guides={guides}
      marks={failEnd}
      marksLabel={failEnd.length ? 'Where the run stopped' : undefined}
      live={live}
      emptyText={live ? 'Waiting for the first samples' : 'Run it to see the result here.'}
    />
  );
}

// ---------------------------------------------------------------- panels

interface Iteration {
  index: number;
  hash_ms: number | null;
  negotiate_ms: number | null;
  transfer_ms: number | null;
  wire_mbps: number | null;
  e2e_mbps: number | null;
  excluded: boolean;
  failed: boolean;
}

function iterationsOf(d: RunData): Iteration[] {
  if (isLive(d)) {
    // The hub re-sends its live values at 4 Hz, so each file's numbers repeat until the next
    // one: take them once, from the Progress where iterations_done went up.
    const s = d.live?.series ?? {};
    const at = (key: string, t: number) => s[key]?.find(([x]) => x === t)?.[1] ?? null;
    const exclude = ((d.params as unknown as { iterations?: number }).iterations ?? 0) >= 3;
    const out: Iteration[] = [];
    let done = 0;
    for (const [t, n] of s.iterations_done ?? []) {
      if (n <= done) continue;
      done = n;
      out.push({
        index: n,
        wire_mbps: at('wire_mbps', t),
        e2e_mbps: at('e2e_mbps', t),
        hash_ms: at('hash_ms', t),
        negotiate_ms: at('negotiate_ms', t),
        transfer_ms: at('transfer_ms', t),
        excluded: exclude && n === 1,
        failed: false,
      });
    }
    return out;
  }
  const raw = (resultAt(results(d), 'iterations') ?? []) as FileTransferIteration[];
  return raw.map((x) => ({
    index: x.index,
    hash_ms: x.hash_ms,
    negotiate_ms: x.negotiate_ms,
    transfer_ms: x.transfer_ms,
    wire_mbps: x.wire_mbps,
    e2e_mbps: x.e2e_mbps,
    excluded: x.excluded,
    failed: !x.ok,
  }));
}

export interface DistributionData {
  title: string;
  unit: string;
  dist: Distribution | null;
  samples: number[] | null;
}

/**
 * The workload's distribution in this run, and its raw samples when the report has them; in a
 * time-based mode without one, the round trips under load; null for a count-based mode that has
 * neither (file transfer, the SyncedStore burst).
 */
export function distributionOf(
  meta: WorkloadMeta,
  params: WorkloadParams,
  report: RunReport | null,
): DistributionData | null {
  const dm = meta.distribution(params);
  const res = report ? resultsOf(report) : null;
  if (dm) {
    const d = resultAt(res, dm.key);
    const raw = dm.raw ? dm.raw(res) : resultAt(res, 'samples_ms');
    return {
      title: dm.title,
      unit: dm.unit,
      dist: isDistribution(d) ? d : null,
      samples: Array.isArray(raw) && raw.length ? (raw as number[]) : null,
    };
  }
  if (!meta.timeBased(params)) return null;
  const tl = report?.network.path_timeline ?? [];
  return {
    title: 'RTT under load',
    unit: 'ms',
    dist: report?.network.loaded_rtt_ms ?? null,
    samples: tl.length ? tl.map((x) => x.rtt_ms ?? Infinity) : null,
  };
}

/**
 * Path samples: while running, the round trips so far (path's own pings, or the path sampler's
 * `loaded_rtt_ms` in a time-based run), each on the peer's connection when it arrived; after it,
 * the report's timeline.
 */
export function timelineOf(d: RunData): PathPoint[] {
  if (isLive(d)) {
    const key = d.meta.id === 'path' ? 'rtt_ms' : KEY_LOADED_RTT;
    // each sample keeps the connection it arrived on; the target's current one only fills gaps
    const conn = d.live?.conn?.[key] ?? [];
    return (d.live?.series[key] ?? []).map(([t, v], i) => ({
      t_ms: t,
      connection: conn[i] ?? (d.connection || null),
      rtt_ms: v,
    }));
  }
  return d.report?.network.path_timeline ?? [];
}

export function PanelView({ panel, data }: { panel: Panel; data: RunData }): ReactNode {
  const live = isLive(data);
  const head = data.meta.kpis(data.params)[0];
  switch (panel.kind) {
    case 'loaded-rtt': {
      const vals = timelineOf(data).map((x) => x.rtt_ms ?? Infinity);
      const prevVals = data.prev?.network.path_timeline.map((x) => x.rtt_ms ?? Infinity);
      return (
        <CdfChart
          title="RTT under load"
          meta={
            vals.length ? `${vals.length} pings at 2 Hz` : 'Pinged twice a second during the run'
          }
          samples={vals}
          lost={vals.filter((v) => !Number.isFinite(v)).length}
          previous={!live && prevVals?.length ? { label: 'Previous run', samples: prevVals } : null}
          unit="ms"
        />
      );
    }
    case 'cdf': {
      const dm = data.meta.distribution(data.params);
      const dd = distributionOf(data.meta, data.params, data.report);
      if (!dd) return null;
      const { title, unit, dist, samples } = dd;
      const liveVals = live
        ? (
            data.live?.series[dm?.key ?? ''] ??
            data.live?.series[(dm?.key ?? '').replace(/_ms$/, '_p50_ms')] ??
            []
          ).map((x) => x[1])
        : undefined;
      const prevD = data.prev ? distributionOf(data.meta, data.prev.params, data.prev) : null;
      return (
        <CdfChart
          title={title}
          meta={
            !live && dist
              ? `${dist.samples} samples${dist.lost ? `, ${dist.lost} lost` : ''}`
              : live
                ? 'Samples so far'
                : undefined
          }
          samples={live ? liveVals : (samples ?? undefined)}
          quantiles={!live && !samples && dist ? quantilesOf(dist) : undefined}
          lost={live ? 0 : (dist?.lost ?? 0)}
          previous={
            !live && prevD && (prevD.samples || prevD.dist)
              ? {
                  label: 'Previous run',
                  samples: prevD.samples ?? undefined,
                  quantiles: !prevD.samples && prevD.dist ? quantilesOf(prevD.dist) : undefined,
                }
              : null
          }
          unit={unit}
        />
      );
    }
    case 'loss': {
      const pts = pointsOf(data, 'loss_pct');
      return (
        <LineChart
          title="Loss per 250 ms"
          series={[{ key: 'loss_pct', label: 'Loss', points: pts }]}
          unit="%"
          height={200}
          marks={pts.filter(([, v]) => v > 1)}
          marksLabel="Above 1%"
          warmupMs={warmupMs(data, 'loss_pct')}
          domainMs={live && !data.live?.attached ? dataMs(data) : null}
          digits={2}
          emptyText={live ? 'Waiting for the first samples' : 'No loss series.'}
        />
      );
    }
    case 'frames': {
      const res = results(data);
      const fr = (resultAt(res, 'frames') ?? []) as FrameCell[];
      const fps = Number(
        resultAt(res, 'fps') ?? (data.params.workload === 'video' ? videoFps(data.params) : 30),
      );
      if (live) {
        const last = data.live?.last ?? {};
        return (
          <figure className="chart">
            <figcaption className="chart-head">
              <span className="ch-title">
                <span className="ch-name">Frames</span>
                <span className="ch-meta caption">every frame arrives with the report</span>
              </span>
            </figcaption>
            <div className="frames-live caption">
              <span>{fmtInt(last.frames_done)} frames so far</span>
              <span>{withUnit(fmtNumber(last.late_pct ?? null), '%')} late</span>
              <span className={(last.dropped_pct ?? 0) > 0 ? 'bad' : undefined}>
                {withUnit(fmtNumber(last.dropped_pct ?? null), '%')} dropped
              </span>
            </div>
          </figure>
        );
      }
      // the engine thins long strips (frames_stride): the header counts the run, not the cells
      const n = (k: string) => {
        const v = resultAt(res, k);
        return typeof v === 'number' ? v : null;
      };
      const [sent, onTime, late, dropped] = ['frames_sent', 'on_time', 'late', 'dropped'].map(n);
      const totals =
        sent != null && onTime != null && late != null && dropped != null
          ? { sent, onTime, late, dropped }
          : undefined;
      return (
        <FrameStrip
          frames={fr}
          fps={fps}
          title="Frames"
          stride={n('frames_stride') ?? 1}
          totals={totals}
        />
      );
    }
    case 'phases': {
      const its = iterationsOf(data);
      return (
        <PhaseBars
          title="Where each file’s time went"
          meta={its.some((x) => x.excluded) ? 'File 1 is left out of the summary' : undefined}
          segments={[
            { key: 'hash', label: 'SHA-256', fill: 'ink3' },
            { key: 'negotiate', label: 'Offer to stream', fill: 'hatch' },
            { key: 'transfer', label: 'Transfer', fill: 'ink' },
          ]}
          rows={its.map((it) => ({
            key: String(it.index),
            label: `File ${it.index}`,
            sub: it.excluded ? 'left out' : it.failed ? 'failed' : undefined,
            values: { hash: it.hash_ms, negotiate: it.negotiate_ms, transfer: it.transfer_ms },
            dim: it.excluded,
          }))}
          emptyText={live ? 'Waiting for the first file' : 'After a run.'}
        />
      );
    }
    case 'line': {
      const series: LineSeries[] = panel.series.map((sm) => ({
        key: sm.key,
        label: sm.label,
        area: sm.area,
        points: pointsOf(data, sm.key, sm.liveKey),
      }));
      const refVal = panel.ref
        ? live
          ? (data.live?.last[panel.ref.resultKey] ?? null)
          : (resultAt(results(data), panel.ref.resultKey) as number | undefined)
        : null;
      return (
        <LineChart
          title={panel.title}
          series={series}
          unit={panel.unit}
          height={200}
          warmupMs={warmupMs(data, panel.series[0]?.key)}
          domainMs={live && !data.live?.attached ? dataMs(data) : null}
          refs={panel.ref && refVal != null ? [{ label: panel.ref.label, value: refVal }] : []}
          emptyText={live ? 'Waiting for the first samples' : 'No series.'}
        />
      );
    }
    case 'bridge': {
      const r = data.report;
      const tcp = r ? bridgeBaseline(r, data.allRuns ?? []) : null;
      const eg = live ? (data.live?.last.goodput_mbps ?? null) : summaryValue(r, 'goodput_mbps');
      const tv = tcp?.summary.find((x) => x.key === 'goodput_mbps')?.value ?? null;
      const diff = eg != null && tv != null ? eg - tv : null;
      return (
        <BarCompare
          title="Against TCP: one bridge less"
          kindLabels={{ current: 'Egress', previous: 'TCP' }}
          meta={
            diff != null && tv
              ? `egress ${diff >= 0 ? '+' : '−'}${fmtNumber(Math.abs(diff))} Mb/s (${fmtNumber(Math.abs((diff / tv) * 100), 1)}%)`
              : 'No comparable TCP run yet: same streams, direction, peer, path and build'
          }
          items={[
            {
              key: 'egress',
              label: live ? 'Egress, so far' : 'Egress, this run',
              value: eg,
              kind: 'current',
            },
            ...(tcp
              ? [
                  {
                    key: 'tcp',
                    label: `TCP, ${tcp.build_profile}, ${fmtWhen(tcp.started_at)}`,
                    value: tv,
                    kind: 'previous' as const,
                  },
                ]
              : []),
          ]}
          unit="Mb/s"
        />
      );
    }
    case 'compare': {
      if (!head) return null;
      const items: BarItem[] = [];
      const cur = live
        ? head.live
          ? (data.live?.last[head.live] ?? null)
          : null
        : summaryValue(data.report, head.key);
      items.push({
        key: 'cur',
        label: live
          ? 'This run, so far'
          : data.report && data.report.status !== 'ok'
            ? 'This run, partial'
            : 'This run',
        value: cur,
        kind: 'current',
      });
      for (const r of data.comparable.slice(0, 4))
        items.push({
          key: r.run_id,
          label: fmtWhen(r.started_at),
          value: r.summary.find((x) => x.key === head.key)?.value ?? null,
          kind: 'previous',
        });
      return (
        <BarCompare
          title={`${head.label}, against earlier runs`}
          meta={
            data.comparable.length
              ? `${data.comparable.length} comparable`
              : live
                ? 'Comparable runs appear with the report'
                : 'No comparable runs yet'
          }
          items={items}
          unit={head.unit}
          digits={head.digits}
        />
      );
    }
  }
}

/** The panels under the main chart: wide ones span the row, the rest pair up. */
export function Panels({ data }: { data: RunData }) {
  const panels = data.meta.panels(data.params);
  return (
    <div className="panels">
      {panels.map((p, i) => (
        <div
          key={`${p.kind}-${i}`}
          className={p.kind === 'frames' ? 'panel wide' : 'panel'}
          data-measure={p.kind}
        >
          <PanelView panel={p} data={data} />
        </div>
      ))}
    </div>
  );
}

export function PathPanel({
  data,
  title = 'Path during the run',
}: {
  data: RunData;
  title?: string;
}) {
  if (!data.meta.timeBased(data.params) && data.meta.id !== 'path') return null;
  return (
    <PathTimeline
      points={timelineOf(data)}
      durationMs={isLive(data) ? dataMs(data) : null}
      title={title}
    />
  );
}

// ---------------------------------------------------------------- tables

export function DistributionTable({ d, unit = 'ms' }: { d: Distribution; unit?: string }) {
  return (
    <Table
      compact
      caption="Distribution"
      rows={
        [
          ['Samples', d.samples, ''],
          ['Lost', d.lost, ''],
          ['Fastest', d.min, unit],
          ['Mean', d.mean, unit],
          ['p50', d.p50, unit],
          ['p90', d.p90, unit],
          ['p95', d.p95, unit],
          ['p99', d.p99, unit],
          ['p99.9', d.p999, unit],
          ['Slowest', d.max, unit],
          ['Std dev', d.stddev, unit],
        ] as [string, number | null, string][]
      }
      rowKey={(r) => r[0]}
      columns={[
        { key: 'k', label: 'Statistic', render: (r) => r[0] },
        {
          key: 'v',
          label: 'Value',
          num: true,
          render: (r) => (r[2] ? num(r[1], r[2]) : r[1] == null ? DASH : fmtInt(r[1])),
        },
      ]}
    />
  );
}

export function factValue(f: FactMeta, v: unknown): string {
  if (v == null) return DASH;
  if (f.format === 'count') return Array.isArray(v) ? fmtInt(v.length) : DASH;
  if (f.format === 'bool') return v ? 'Yes' : 'No';
  if (f.format === 'text') return String(v);
  if (typeof v !== 'number') return String(v);
  if (f.format === 'bytes') return fmtBytes(v);
  if (f.format === 'int') return withUnit(fmtInt(v), f.unit ?? '');
  return withUnit(fmtNumber(v), f.unit ?? '');
}

/** Counts and timings from results; rows the report doesn't have are left out. */
export function FactsTable({ facts, report }: { facts: FactMeta[]; report: RunReport | null }) {
  const res = report ? resultsOf(report) : null;
  const rows = facts
    .filter((f) => !f.when || f.when(res))
    .map((f) => ({ f, v: resultAt(res, f.key) }))
    .filter((x) => x.v !== undefined);
  if (!rows.length) return null;
  return (
    <Table
      compact
      caption="Counts"
      rows={rows}
      rowKey={(r) => r.f.key}
      columns={[
        { key: 'k', label: 'Count', render: (r) => r.f.label },
        {
          key: 'v',
          label: 'Value',
          num: true,
          render: (r) =>
            r.f.format === 'text' ? (
              <span className="caption wrap">{factValue(r.f, r.v)}</span>
            ) : (
              factValue(r.f, r.v)
            ),
        },
      ]}
    />
  );
}

export function ResourceTable({
  local,
  peer,
  combined,
}: {
  local: ResourceStats | null;
  peer: ResourceStats | null;
  combined: ResourceStats | null;
}) {
  const rows: { k: string; a: (r: ResourceStats) => number | null; u: string }[] = [
    { k: 'CPU, average', a: (r) => r.cpu_pct_avg, u: '%' },
    { k: 'CPU, peak', a: (r) => r.cpu_pct_max, u: '%' },
    { k: 'CPU time', a: (r) => r.cpu_ms, u: 'ms' },
    { k: 'CPU per MB', a: (r) => r.cpu_ms_per_mb, u: 'ms' },
    { k: 'Memory, peak', a: (r) => r.rss_mb_max, u: 'MB' },
    { k: 'Sidecar CPU, average', a: (r) => r.sidecar_cpu_pct_avg, u: '%' },
    { k: 'Sidecar CPU, peak', a: (r) => r.sidecar_cpu_pct_max, u: '%' },
    { k: 'Sidecar CPU per MB', a: (r) => r.sidecar_cpu_ms_per_mb, u: 'ms' },
    { k: 'Sidecar memory, peak', a: (r) => r.sidecar_rss_mb_max, u: 'MB' },
  ];
  const cols: { key: string; label: string; r: ResourceStats | null }[] = combined
    ? [{ key: 'c', label: 'One process', r: combined }]
    : [
        { key: 'l', label: 'Here', r: local },
        { key: 'p', label: 'Peer', r: peer },
      ];
  return (
    <Table
      compact
      caption="Resources"
      rows={rows}
      rowKey={(r) => r.k}
      columns={[
        { key: 'k', label: 'Process', render: (r) => r.k },
        ...cols.map((c) => ({
          key: c.key,
          label: c.label,
          num: true,
          render: (r: (typeof rows)[number]) => (c.r ? num(r.a(c.r), r.u) : DASH),
        })),
      ]}
    />
  );
}

const PATH_WORDS: Record<string, string> = {
  direct: 'Direct',
  relay: 'Relayed',
  mixed: 'Mixed',
  unknown: 'Unknown',
};

export function PathLabel({ label }: { label: string }) {
  return label === 'direct' ? (
    <Status kind="live">{PATH_WORDS[label]}</Status>
  ) : label === 'relay' ? (
    <Status kind="running">{PATH_WORDS[label]}</Status>
  ) : label === 'mixed' ? (
    <Status kind="running">{PATH_WORDS[label]}</Status>
  ) : (
    <Status kind="waiting">{PATH_WORDS[label] ?? label}</Status>
  );
}

const snap = (s: PathSnapshot | null | undefined) =>
  s
    ? s.error
      ? `error: ${s.error}`
      : `${fmtConnection(s.connection)}${s.rtt_ms == null ? '' : `, ${fmtNumber(s.rtt_ms)} ms`}`
    : DASH;

/** Path facts of a finished run: label, DERP share, bytes per path, before and after, clock. */
export function PathFacts({ report }: { report: RunReport }) {
  const n = report.network;
  const pb = n.path_bytes;
  const rows: [string, ReactNode][] = [
    ['Path', <PathLabel key="pl" label={n.path_label} />],
    ['Topology', n.topology.replace('-', ' ')],
    ['Settled in', withUnit(fmtInt(n.settle_ms), 'ms')],
    ['Before', snap(n.before)],
    ['After', snap(n.after)],
  ];
  if (n.derp_share != null)
    rows.push(['Through DERP', withUnit(fmtNumber(n.derp_share * 100, 1), '%')]);
  if (pb) {
    rows.push(['Bytes direct', fmtBytes(pb.direct)]);
    rows.push(['Bytes via DERP', fmtBytes(pb.derp)]);
    if (pb.peer_relay) rows.push(['Bytes via a peer relay', fmtBytes(pb.peer_relay)]);
  }
  if (n.clock) {
    rows.push([
      'Clock offset',
      `${fmtNumber(n.clock.before.offset_ns / 1e6)} ± ${fmtNumber(n.clock.uncertainty_ns / 1e6)} ms`,
    ]);
    if (n.clock.drift_ppm != null)
      rows.push(['Clock drift', `${fmtNumber(n.clock.drift_ppm)} ppm`]);
  }
  return (
    <dl className="facts">
      {rows.map(([k, v]) => (
        <div key={k}>
          <dt>{k}</dt>
          <dd className="num">{v}</dd>
        </div>
      ))}
    </dl>
  );
}

// ---------------------------------------------------------------- environments and params

const ENV_ROWS: { k: string; f: (e: NodeEnv) => ReactNode; long?: boolean }[] = [
  { k: 'Device', f: (e) => e.device_name },
  {
    k: 'System',
    f: (e) => `${e.os} ${e.arch}${e.logical_cpus ? ` · ${e.logical_cpus} CPUs` : ''}`,
  },
  { k: 'Tool', f: (e) => `${e.tool} ${e.tool_version}` },
  { k: 'truffle', f: (e) => e.truffle_version },
  {
    k: 'Build',
    f: (e) =>
      e.build_profile === 'debug' ? <span className="chip strong">debug</span> : 'release',
  },
  { k: 'rustc', f: (e) => e.rustc ?? DASH },
  { k: 'Git', f: (e) => (e.git_sha ? `${e.git_sha}${e.git_dirty ? ' (dirty)' : ''}` : DASH) },
  { k: 'Sidecar', f: (e) => e.sidecar_path ?? DASH, long: true },
  { k: 'Sidecar SHA-256', f: (e) => e.sidecar_sha256 ?? DASH, long: true },
  { k: 'Tailscale IP', f: (e) => e.tailscale_ip ?? DASH },
  {
    k: 'Host fingerprint',
    f: (e) =>
      e.host_fingerprint
        ? `${e.host_fingerprint}${e.host_fingerprint_weak ? ' (weak)' : ''}`
        : DASH,
  },
  { k: 'On battery', f: (e) => (e.on_battery == null ? DASH : e.on_battery ? 'Yes' : 'No') },
  {
    k: 'App in front',
    f: (e) =>
      e.app_foreground_pct == null ? DASH : withUnit(fmtNumber(e.app_foreground_pct, 0), '%'),
  },
];

/** This device and the peer, side by side. */
export function EnvTable({ local, peer }: { local: NodeEnv; peer: NodeEnv | null }) {
  return (
    <Table
      compact
      caption="Environments"
      rows={ENV_ROWS}
      rowKey={(r) => r.k}
      rowClass={(r) => (r.long ? 'long' : '')}
      columns={[
        { key: 'k', label: 'Field', render: (r) => r.k, width: 150 },
        {
          key: 'l',
          label: 'This device',
          render: (r) => <span className="mono wrap">{r.f(local)}</span>,
        },
        {
          key: 'p',
          label: 'Peer',
          render: (r) => <span className="mono wrap">{peer ? r.f(peer) : DASH}</span>,
        },
      ]}
    />
  );
}

/** Field labels from the registry where it has them, raw keys otherwise. */
export function ParamsTable({
  meta,
  params,
}: {
  meta: WorkloadMeta | null;
  params: WorkloadParams;
}) {
  const rec = params as unknown as Record<string, unknown>;
  const label = (k: string) => meta?.fields.find((f) => f.key === k)?.label ?? k;
  const unit = (k: string) => {
    const f = meta?.fields.find((x) => x.key === k);
    return f && f.kind === 'number' ? (f.unit ?? '') : '';
  };
  // the fields that apply to these params (a throughput run has no message size), and any the
  // registry doesn't know
  const shown = new Set(meta ? visibleFields(meta, params).map((f) => f.key) : []);
  const known = new Set(meta?.fields.map((f) => f.key) ?? []);
  const rows = Object.keys(rec).filter((k) => k !== 'workload' && (shown.has(k) || !known.has(k)));
  return (
    <Table
      compact
      caption="Parameters"
      rows={rows}
      rowKey={(k) => k}
      columns={[
        { key: 'k', label: 'Parameter', render: (k) => label(k) },
        { key: 'f', label: 'Field', render: (k) => <span className="mono muted">{k}</span> },
        {
          key: 'v',
          label: 'Value',
          num: true,
          render: (k) => {
            const v = rec[k];
            if (v == null) {
              const f = meta?.fields.find((x) => x.key === k);
              return (f?.kind === 'number' ? textOf(f.nullLabel, params) : null) ?? 'default';
            }
            if (typeof v === 'boolean') return v ? 'on' : 'off';
            if (typeof v === 'number')
              return unit(k) === 'B' ? `${fmtInt(v)} B` : withUnit(String(v), unit(k));
            return String(v);
          },
        },
      ]}
    />
  );
}

export const durationText = (r: RunReport) => fmtDuration(r.duration_ms);

export { mergeNotes } from '../lib/notes';
