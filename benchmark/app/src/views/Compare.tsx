// Compare (RFC 027 §11.3): two runs side by side. Deltas per summary key with their better or
// worse direction; a warning listing the compare_key fields that differ; every series overlaid,
// the earlier run ink-3 dashed.

import { useEffect, useState } from 'react';
import type { NodeEnv, RunReport, WorkloadKind } from '../api';
import { useAppStore } from '../state/context';
import { Button } from '../components/Button';
import { Table } from '../components/Data';
import { Empty, Notice } from '../components/Notice';
import { Loader, Page, Section } from '../components/Misc';
import { Status } from '../components/Status';
import { useConstructionFlag } from '../charts/construction';
import { CdfChart, LineChart, quantilesOf } from '../charts';
import { REGISTRY } from '../workloads/registry';
import { deltas, keyDiffs, type Delta } from '../lib/compare';
import { DASH, fmtNumber, fmtWhen, withUnit } from '../lib/format';
import type { Page as PageId } from '../hooks/useRoute';
import { distributionOf } from './RunParts';
import { StatusOf, workloadLabel } from './History';

const ENV_FIELDS: { k: string; f: (e: NodeEnv | null) => string }[] = [
  { k: 'tool', f: (e) => (e ? `${e.tool} ${e.tool_version}` : DASH) },
  { k: 'truffle', f: (e) => e?.truffle_version ?? DASH },
  { k: 'build', f: (e) => e?.build_profile ?? DASH },
  { k: 'rustc', f: (e) => e?.rustc ?? DASH },
  { k: 'git', f: (e) => e?.git_sha ?? DASH },
  { k: 'sidecar SHA-256', f: (e) => (e?.sidecar_sha256 ? e.sidecar_sha256.slice(0, 16) : DASH) },
  { k: 'system', f: (e) => (e ? `${e.os} ${e.arch}` : DASH) },
  { k: 'on battery', f: (e) => (e?.on_battery == null ? DASH : e.on_battery ? 'yes' : 'no') },
];

function useReport(id: string) {
  const store = useAppStore();
  const [r, setR] = useState<RunReport | null | 'missing'>(null);
  useEffect(() => {
    let off = false;
    setR(null);
    void store.report(id).then((x) => !off && setR(x ?? 'missing'));
    return () => {
      off = true;
    };
  }, [id, store]);
  return r;
}

export function CompareView({
  a: idA,
  b: idB,
  go,
}: {
  a: string;
  b: string;
  go: (p: PageId, id?: string) => void;
}) {
  const construction = useConstructionFlag();
  const ra = useReport(idA);
  const rb = useReport(idB);
  if (ra === null || rb === null)
    return (
      <div className="splash fill">
        <Loader label="Loading both reports" />
      </div>
    );
  if (ra === 'missing' || rb === 'missing')
    return (
      <Page construction={construction} eyebrow="History" title="Compare">
        <Empty
          title="One of these reports is gone"
          actions={
            <Button icon="arrow-left" onClick={() => go('history')}>
              Back to History
            </Button>
          }
        />
      </Page>
    );

  // the earlier run is the baseline
  const [a, b] = ra.started_at <= rb.started_at ? [ra, rb] : [rb, ra];
  const diffs = keyDiffs(a, b);
  const sameKey = a.compare_key === b.compare_key;
  const rows = deltas(a.summary, b.summary);
  const meta = REGISTRY[b.workload as WorkloadKind] ?? null;
  const seriesKeys = [...new Set([...b.series.map((x) => x.key), ...a.series.map((x) => x.key)])];
  const envDiff = (['local', 'peer'] as const).flatMap((side) =>
    ENV_FIELDS.map((f) => ({
      key: `${side}-${f.k}`,
      side: side === 'local' ? 'This device' : 'Peer',
      field: f.k,
      a: f.f(a[side]),
      b: f.f(b[side]),
    })).filter((x) => x.a !== x.b),
  );
  const distA = meta ? distributionOf(meta, a.params, a) : null;
  const distB = meta ? distributionOf(meta, b.params, b) : null;
  const hasDist = !!(distB && (distB.samples || distB.dist));

  return (
    <Page
      construction={construction}
      eyebrow="History · Compare"
      title={sameKey ? `${workloadLabel(b.workload)}: two comparable runs` : 'Two runs'}
      lede={`${fmtWhen(a.started_at)} against ${fmtWhen(b.started_at)}. The earlier run is the baseline, drawn dashed.`}
      actions={
        <Button variant="quiet" icon="arrow-left" onClick={() => go('history')}>
          History
        </Button>
      }
    >
      <div className="cmp-heads">
        {[a, b].map((r, i) => (
          // only the label is the button (the card is its click target): the status, time, peer,
          // path and key stay readable instead of hidden inside a button
          <article key={r.run_id} className="card linked cmp-head">
            <span className="label">{i === 0 ? 'Earlier · baseline' : 'Later'}</span>
            <button
              type="button"
              className="card-link cmp-label"
              onClick={() => go('history', r.run_id)}
            >
              {r.label}
              <span className="sr">, open the report</span>
            </button>
            <span className="cmp-meta">
              <StatusOf run={r} />
              <span className="caption muted">
                {fmtWhen(r.started_at)} · {r.peer?.device_name ?? 'no peer'} ·{' '}
                {r.network.path_label}
              </span>
            </span>
            <span className="cmp-key mono muted">key {r.compare_key}</span>
          </article>
        ))}
      </div>

      {sameKey ? (
        <Notice kind="quiet" title="Same compare key">
          Same workload settings, topology, path, builds, methodology and peer host: the difference
          is the run itself.
        </Notice>
      ) : (
        <Notice
          title={
            a.workload !== b.workload
              ? 'Different workloads: these numbers measure different things'
              : 'These runs differ in more than duration and counts'
          }
        >
          <span>
            Trends never join them. {diffs.length ? 'What differs:' : 'Their compare keys differ.'}
          </span>
          {diffs.length ? (
            <Table
              compact
              caption="Fields that differ"
              rows={diffs}
              rowKey={(d) => d.field}
              columns={[
                {
                  key: 'f',
                  label: 'Field',
                  render: (d) => <span className="mono">{d.field}</span>,
                },
                { key: 'a', label: 'Earlier', render: (d) => <span className="mono">{d.a}</span> },
                { key: 'b', label: 'Later', render: (d) => <span className="mono">{d.b}</span> },
              ]}
            />
          ) : null}
        </Notice>
      )}

      <Section label="Summary" meta={`${rows.length} values · changes under 1% count as the same`}>
        <Table
          caption="Summary deltas"
          rows={rows}
          rowKey={(d) => d.key}
          rowClass={(d) => (d.verdict === 'worse' ? 'worse' : '')}
          columns={[
            { key: 'l', label: 'Value', render: (d) => d.label },
            {
              key: 'a',
              label: 'Earlier',
              num: true,
              render: (d) => withUnit(fmtNumber(d.a), d.unit),
            },
            {
              key: 'b',
              label: 'Later',
              num: true,
              render: (d) => withUnit(fmtNumber(d.b), d.unit),
            },
            { key: 'd', label: 'Change', num: true, render: (d) => change(d) },
            {
              key: 'v',
              label: 'Verdict',
              render: (d) => <Verdict d={d} />,
            },
          ]}
        />
      </Section>

      <Section label="Series" meta="Later solid, earlier dashed">
        <div className="rd-charts">
          {hasDist && distA && distB ? (
            <CdfChart
              title={distB.title}
              samples={distB.samples ?? undefined}
              quantiles={!distB.samples && distB.dist ? quantilesOf(distB.dist) : undefined}
              lost={distB.dist?.lost ?? 0}
              label="Later"
              previous={
                distA.samples || distA.dist
                  ? {
                      label: 'Earlier',
                      samples: distA.samples ?? undefined,
                      quantiles: !distA.samples && distA.dist ? quantilesOf(distA.dist) : undefined,
                    }
                  : null
              }
              unit={distB.unit}
            />
          ) : null}
          {seriesKeys.map((key) => {
            const sb = b.series.find((x) => x.key === key);
            const sa = a.series.find((x) => x.key === key);
            const any = sb ?? sa;
            if (!any) return null;
            return (
              <LineChart
                key={key}
                title={any.label}
                unit={any.unit}
                height={200}
                warmupMs={sb?.warmup_ms ?? null}
                series={[
                  ...(sb
                    ? [
                        {
                          key: `b-${key}`,
                          label: `Later, ${fmtWhen(b.started_at)}`,
                          points: sb.points,
                        },
                      ]
                    : []),
                  ...(sa
                    ? [
                        {
                          key: `a-${key}`,
                          label: `Earlier, ${fmtWhen(a.started_at)}`,
                          points: sa.points,
                          baseline: true,
                        },
                      ]
                    : []),
                ]}
              />
            );
          })}
          {!seriesKeys.length && !hasDist ? (
            <p className="muted">Neither report has series (imported reports often don’t).</p>
          ) : null}
        </div>
      </Section>

      <Section
        label="Environments"
        meta={envDiff.length ? `${envDiff.length} differences` : 'no differences'}
      >
        {envDiff.length ? (
          <Table
            compact
            caption="Environment differences"
            rows={envDiff}
            rowKey={(x) => x.key}
            columns={[
              { key: 's', label: 'Side', render: (x) => x.side },
              { key: 'f', label: 'Field', render: (x) => x.field },
              {
                key: 'a',
                label: 'Earlier',
                render: (x) => <span className="mono wrap">{x.a}</span>,
              },
              { key: 'b', label: 'Later', render: (x) => <span className="mono wrap">{x.b}</span> },
            ]}
          />
        ) : (
          <p className="ink2">Same tools, builds and sidecars on both sides.</p>
        )}
      </Section>
    </Page>
  );
}

function change(d: Delta): string {
  if (d.diff == null) return DASH;
  const sign = d.diff > 0 ? '+' : d.diff < 0 ? '−' : '';
  const abs = withUnit(fmtNumber(Math.abs(d.diff)), d.unit);
  return d.pct == null
    ? `${sign}${abs}`
    : `${sign}${abs} (${sign}${fmtNumber(Math.abs(d.pct), 1)}%)`;
}

function Verdict({ d }: { d: Delta }) {
  if (!d.verdict) return <span className="muted">{DASH}</span>;
  const dir = d.better === 'higher' ? 'higher is better' : 'lower is better';
  return d.verdict === 'better' ? (
    <Status kind="live" title={dir}>
      Better
    </Status>
  ) : d.verdict === 'worse' ? (
    <Status kind="waiting" title={dir}>
      Worse
    </Status>
  ) : (
    <Status kind="off" title={dir}>
      Same
    </Status>
  );
}
