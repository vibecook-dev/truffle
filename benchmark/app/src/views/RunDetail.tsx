// Run detail (RFC 027 §11.3): one report in full. Summary, every chart, params, both
// environments, the network (topology, settle, before and after, clock, path timeline, bytes per
// path), resources and notes.

import { useEffect, useMemo, useState, type ReactNode } from 'react';
import { resultsOf, type RunReport, type WorkloadKind } from '../api';
import { useAppState, useAppStore } from '../state/context';
import { Button, ConfirmButton } from '../components/Button';
import { Facts, Kpi, KpiRow } from '../components/Data';
import { Empty, Notice } from '../components/Notice';
import { Loader, Page, Section } from '../components/Misc';
import { Icon } from '../components/Icon';
import { useConstructionFlag } from '../charts/construction';
import { LineChart } from '../charts';
import {
  REGISTRY,
  labelOf,
  lowerBoundOf,
  resultAt,
  type MainChartMeta,
} from '../workloads/registry';
import { DASH, fmtDuration, fmtNumber, fmtWhen, withUnit } from '../lib/format';
import { trendable } from '../lib/compare';
import type { Page as PageId } from '../hooks/useRoute';
import {
  DistributionTable,
  EnvTable,
  FactsTable,
  MainChart,
  Panels,
  ParamsTable,
  PathFacts,
  PathPanel,
  ResourceTable,
  distributionOf,
  mergeNotes,
  type RunData,
} from './RunParts';
import { Flags, StatusOf, workloadLabel } from './History';

/** As the benchmark views: loss, drops and errors above 1% are redline; late frames are not. */
const BAD_WHEN_HIGH = new Set(['loss_pct', 'drop_pct', 'dropped_pct', 'error_pct']);

export function RunDetail({ id, go }: { id: string; go: (p: PageId, id?: string) => void }) {
  const store = useAppStore();
  const s = useAppState();
  const construction = useConstructionFlag();
  const [report, setReport] = useState<RunReport | null | 'missing'>(null);
  useEffect(() => {
    let off = false;
    setReport(null);
    void store.report(id).then((r) => !off && setReport(r ?? 'missing'));
    return () => {
      off = true;
    };
  }, [id, store]);

  const r = report && report !== 'missing' ? report : null;
  const summary = s.runs.find((x) => x.run_id === id) ?? null;
  // earlier runs with the same compare_key, newest first
  const comparable = useMemo(
    () =>
      r
        ? s.runs.filter(
            (x) =>
              x.compare_key === r.compare_key &&
              x.run_id !== r.run_id &&
              x.started_at < r.started_at &&
              trendable(x),
          )
        : [],
    [s.runs, r],
  );
  const [prev, setPrev] = useState<RunReport | null>(null);
  useEffect(() => {
    let off = false;
    const first = comparable[0];
    if (first) void store.report(first.run_id).then((x) => !off && setPrev(x));
    else setPrev(null);
    return () => {
      off = true;
    };
  }, [comparable, store]);

  if (report === null)
    return (
      <div className="splash fill">
        <Loader label="Loading the report" />
      </div>
    );
  if (!r)
    return (
      <Page construction={construction} eyebrow="History" title="Report not found">
        <Empty
          title="This run’s report is gone"
          actions={
            <Button icon="arrow-left" onClick={() => go('history')}>
              Back to History
            </Button>
          }
        >
          It may have been deleted, or the data folder changed.
        </Empty>
      </Page>
    );

  const meta = REGISTRY[r.workload as WorkloadKind] ?? null;
  const data: RunData | null = meta
    ? {
        meta,
        params: r.params,
        report: r,
        live: null,
        running: false,
        prev,
        comparable,
        allRuns: s.runs,
      }
    : null;
  const main: MainChartMeta | null = meta ? meta.main(r.params) : null;
  const shownKeys = new Set<string>([
    ...(main?.series.map((x) => x.key) ?? []),
    'loss_pct',
    ...(meta
      ?.panels(r.params)
      .flatMap((p) => (p.kind === 'line' ? p.series.map((x) => x.key) : [])) ?? []),
  ]);
  const other = r.series.filter((x) => !shownKeys.has(x.key) && x.points.length);
  const dist = meta ? distributionOf(meta, r.params, r) : null;
  const loaded = r.network.loaded_rtt_ms;
  const timeBased = meta ? meta.timeBased(r.params) : false;
  const showLoaded = !!meta?.distribution(r.params) && !!loaded;
  const res = resultsOf(r);
  const hasFacts = !!meta?.facts(r.params).some((f) => resultAt(res, f.key) !== undefined);
  // the tables that have something in them: the distribution, latency under load, counts
  const tables: { key: string; label: string; meta?: string; body: ReactNode }[] = [];
  if (dist?.dist)
    tables.push({
      key: 'dist',
      label: dist.title,
      meta: dist.samples ? `${dist.dist.samples} samples` : undefined,
      body: <DistributionTable d={dist.dist} unit={dist.unit} />,
    });
  if (showLoaded && loaded)
    tables.push({
      key: 'loaded',
      label: 'RTT under load',
      meta: 'pinged at 2 Hz',
      body: <DistributionTable d={loaded} />,
    });
  if (hasFacts && meta)
    tables.push({
      key: 'facts',
      label: 'Counts',
      body: <FactsTable facts={meta.facts(r.params)} report={r} />,
    });

  return (
    <Page
      construction={construction}
      eyebrow={`History · ${workloadLabel(r.workload)}`}
      title={r.label}
      lede={`Against ${r.peer?.device_name ?? 'no peer'} · ${fmtWhen(r.started_at)} · took ${fmtDuration(r.duration_ms)}`}
      actions={
        <>
          <Button variant="quiet" icon="arrow-left" onClick={() => go('history')}>
            History
          </Button>
          {comparable[0] ? (
            <Button onClick={() => go('history', `compare/${comparable[0].run_id}/${r.run_id}`)}>
              Compare with the previous run
            </Button>
          ) : null}
          {meta ? (
            <Button trailingIcon="arrow-right" onClick={() => go(meta.page)}>
              Open {meta.label}
            </Button>
          ) : null}
          <ConfirmButton
            size="md"
            icon="trash"
            confirm="Delete this run"
            onConfirm={() => {
              void store.deleteRuns([r.run_id]).then(() => go('history'));
            }}
          >
            Delete
          </ConfirmButton>
        </>
      }
    >
      <div className="rd-state">
        <StatusOf run={r} />
        {summary ? <Flags run={summary} /> : null}
        <span className="mono muted">{r.run_id}</span>
      </div>

      {r.status === 'failed' ? (
        <Notice
          kind="error"
          title={`Failed${r.error_code ? `: ${r.error_code.replace(/-/g, ' ')}` : ''}`}
        >
          {r.error ?? 'The run failed.'} What it measured before it stopped is kept below.
        </Notice>
      ) : r.status === 'cancelled' ? (
        <Notice kind="quiet" title="Cancelled">
          The run stopped early. What it measured is kept, and not used in trends.
        </Notice>
      ) : null}
      {r.network.topology !== 'separate-hosts' ? (
        <Notice
          kind="quiet"
          title={
            r.network.topology === 'same-process'
              ? 'Both ends ran in one process'
              : 'Both ends ran on this machine'
          }
        >
          This measures the bridges and the netstack, not a link between machines.
        </Notice>
      ) : null}
      {r.tool === 'imported' ? (
        <Notice kind="quiet" title={`Imported, methodology ${r.methodology}`}>
          Normalised from an older report. Its numbers were measured another way, so they only
          compare with reports of the same methodology.
        </Notice>
      ) : null}

      <Section label="Summary" meta={`${r.summary.length} values`}>
        <KpiRow dense={r.summary.length > 5}>
          {r.summary.map((k) => {
            const bound = k.value == null ? lowerBoundOf(r, k.key) : null;
            return (
              <Kpi
                key={k.key}
                label={labelOf(k)}
                value={bound ?? k.value}
                bound={bound != null}
                unit={k.unit}
                bad={BAD_WHEN_HIGH.has(k.key) && (k.value ?? 0) > 1}
                sub={
                  bound != null
                    ? 'lower bound: no loss limit reached'
                    : k.uncertainty != null
                      ? `± ${withUnit(fmtNumber(k.uncertainty), k.unit)}`
                      : `${k.better} is better`
                }
              />
            );
          })}
        </KpiRow>
      </Section>

      {data ? (
        <Section
          label="Charts"
          meta={prev ? `Dashed: the previous run, ${fmtWhen(prev.started_at)}` : undefined}
        >
          <div className="rd-charts">
            <MainChart data={data} />
            <Panels data={data} />
            {timeBased || r.workload === 'path' ? <PathPanel data={data} title="Path" /> : null}
            {other.map((x) => (
              <LineChart
                key={x.key}
                title={x.label}
                series={[{ key: x.key, label: x.label, points: x.points }]}
                unit={x.unit}
                height={180}
                warmupMs={x.warmup_ms}
              />
            ))}
          </div>
        </Section>
      ) : null}

      <div className="rd-two">
        <Section label="Parameters" meta={r.params.workload}>
          <ParamsTable meta={meta} params={r.params} />
        </Section>
        <Section label="Network" meta={r.network.topology.replace('-', ' ')}>
          <article className="card tight">
            <PathFacts report={r} />
          </article>
        </Section>
      </div>

      <Section label="Environments" meta="This device and the peer, as each reported itself">
        <EnvTable local={r.local} peer={r.peer} />
      </Section>

      {tables.length ? (
        <div className="rd-two">
          {tables.map((t) => (
            <Section key={t.key} label={t.label} meta={t.meta}>
              {t.body}
            </Section>
          ))}
        </div>
      ) : null}

      <Section label="Resources" meta="100% is one logical CPU">
        {r.resources.local || r.resources.peer || r.resources.combined ? (
          <ResourceTable
            local={r.resources.local}
            peer={r.resources.peer}
            combined={r.resources.combined}
          />
        ) : (
          <p className="muted">This report has no resource figures.</p>
        )}
      </Section>

      <Section label="Notes">
        {r.notes.length ? (
          <ul className="notes">
            {mergeNotes(r.notes).map((n) => (
              <li key={n}>{n}</li>
            ))}
          </ul>
        ) : (
          <p className="muted">No notes.</p>
        )}
      </Section>

      <Section label="About this report">
        <article className="card tight">
          <Facts
            rows={[
              ['Run id', r.run_id, true],
              ['Suite', r.suite_id ?? DASH, true],
              ['Compare key', r.compare_key, true],
              ['Tool', `${r.tool} ${r.tool_version}`, true],
              ['truffle', r.truffle_version, true],
              ['Methodology', r.methodology, true],
              ['Schema', `v${r.schema_version}`, true],
              ['Git', r.git_sha ? `${r.git_sha}${r.git_dirty ? ' (dirty)' : ''}` : DASH, true],
              ['Started', r.started_at, true],
              ['Finished', r.finished_at, true],
              ['Overlapping runs', String(r.overlapping_runs), true],
            ]}
          />
        </article>
        <details className="disclosure raw">
          <summary>
            <Icon name="chevron-right" size={16} />
            Raw results
          </summary>
          <pre className="code-block">{JSON.stringify(r.results, null, 2)}</pre>
        </details>
      </Section>
    </Page>
  );
}
