// The frame every benchmark view shares (RFC 027 §11.3): a top bar with the target and path;
// parameters and the one Run button on the left; the live KPI row, the main chart, the panels
// and the path timeline in the centre; what is measured, the data path, notes, the
// distribution, counts, the path and resources on the right. Everything workload-specific
// comes from the registry (workloads/registry.ts); the drawing is shared with Run detail
// (views/RunParts.tsx).

import { useEffect, useMemo, useState, type ReactNode } from 'react';
import type { RunReport, WorkloadParams } from '../api';
import { liveBasis } from '../lib/compare';
import { useAppState, useAppStore } from '../state/context';
import { runBlockReason, type LiveRun } from '../state/store';
import { useConstructionFlag } from '../charts/construction';
import { Button } from '../components/Button';
import { Field, NumberField, Seg, Switch } from '../components/Controls';
import { Kpi, KpiRow, Meter } from '../components/Data';
import { Notice } from '../components/Notice';
import { Icon } from '../components/Icon';
import { KbdHint, Tooltip } from '../components/Misc';
import { Status } from '../components/Status';
import { jy } from '../lib/jy';
import {
  DASH,
  fmtConnection,
  fmtDuration,
  fmtNumber,
  fmtOs,
  fmtWhen,
  withUnit,
} from '../lib/format';
import { MOD, useViewActions } from '../hooks/useShortcuts';
import {
  editParams,
  lowerBoundOf,
  textOf,
  validateParams,
  visibleFields,
  type FieldMeta,
  type WorkloadMeta,
} from '../workloads/registry';
import type { Page } from '../hooks/useRoute';
import {
  DistributionTable,
  FactsTable,
  MainChart,
  Panels,
  PathFacts,
  PathPanel,
  ResourceTable,
  distributionOf,
  mergeNotes,
  phaseWords,
  type RunData,
} from './RunParts';

const PHASE_PLAIN: Partial<Record<string, string>> = {
  preparing: 'preparing the peer',
  settling: 'settling the path',
  clock: 'estimating the clock offset',
  warmup: 'warming up',
  running: 'measuring',
  draining: 'draining',
  finishing: 'collecting results',
};

/**
 * Shares that are bad news above 1%: shown in redline (always with their label). Loss, drops and
 * errors only: a late frame still arrived, so late_pct stays ink, as the frame strip draws it.
 */
const BAD_WHEN_HIGH = new Set(['loss_pct', 'drop_pct', 'dropped_pct', 'error_pct']);

const terminal = (p: string) => p === 'done' || p === 'failed' || p === 'cancelled';

export function BenchmarkFrame({
  meta,
  go,
}: {
  meta: WorkloadMeta;
  go: (p: Page, id?: string) => void;
}) {
  const store = useAppStore();
  const s = useAppState();
  const construction = useConstructionFlag();
  const target = store.target();

  const [params, setParams] = useState<WorkloadParams>(meta.defaults);
  const [preset, setPreset] = useState<string>(
    meta.presets.find((p) => p.id === 'default')?.id ?? meta.presets[0]?.id ?? '',
  );
  const [bad, setBad] = useState<Record<string, boolean>>({});

  // the run on screen: this window's latest run of the workload, else the latest in history
  const liveId = s.current[meta.id];
  const live: LiveRun | null = liveId ? (s.live[liveId] ?? null) : null;
  const latest = useMemo(
    () => s.runs.find((r) => r.workload === meta.id && r.tool !== 'imported') ?? null,
    [s.runs, meta.id],
  );
  const [history, setHistory] = useState<RunReport | null>(null);
  const latestId = latest?.run_id ?? null;
  useEffect(() => {
    let off = false;
    if (latestId) void store.report(latestId).then((r) => !off && setHistory(r));
    else setHistory(null);
    return () => {
      off = true;
    };
  }, [latestId, store]);
  const report: RunReport | null = live ? live.report : history;
  const running = !!live && !live.report && !live.error && !terminal(live.phase);

  // the previous comparable run, for baselines and the comparison bars (a live run borrows the
  // latest run's compare_key only when it is the same measurement: see liveBasis)
  const basis = report ?? (running && live ? liveBasis(live, latest, history) : null);
  const comparable = useMemo(() => {
    if (!basis) return [];
    return s.runs.filter(
      (r) =>
        r.compare_key === basis.compare_key && r.run_id !== report?.run_id && r.status === 'ok',
    );
  }, [s.runs, basis, report]);
  const [prev, setPrev] = useState<RunReport | null>(null);
  useEffect(() => {
    let off = false;
    const first = comparable[0];
    if (first) void store.report(first.run_id).then((r) => !off && setPrev(r));
    else setPrev(null);
    return () => {
      off = true;
    };
  }, [comparable, store]);

  // a run reattached after a reload whose settings aren't known yet (not a suite item): draw its
  // progress only, never the form's settings as if they were the run's
  const unknownParams = running && !!live?.attached && !live.params;
  const shownParams: WorkloadParams = live?.params ?? report?.params ?? params;
  const paneParams: WorkloadParams = running ? shownParams : params;
  const shownKeys = new Set(visibleFields(meta, params).map((f) => f.key));
  const problem =
    validateParams(meta, params) ??
    (Object.entries(bad).some(([k, b]) => b && shownKeys.has(k))
      ? 'Fix the highlighted parameters.'
      : null);
  const block = runBlockReason(s, target, meta.id);
  const reason = running ? null : (block ?? problem);
  const start = () => {
    if (target && !reason) void store.run(params, target);
  };
  const cancel = () => {
    if (live && running) void store.cancel(live.run_id);
  };
  useViewActions({ run: start, canRun: !reason && !running, cancel, canCancel: running });

  const data: RunData = {
    meta,
    params: shownParams,
    report,
    live: live && !live.report ? live : null,
    running,
    prev,
    comparable,
    // the run's own peer (an attached run may not be against the current target)
    connection: live
      ? s.peers.find((x) =>
          live.peer_id ? x.id === live.peer_id : x.device_name === live.peer_name,
        )?.connection
      : target?.connection,
    allRuns: s.runs,
  };

  const kpis = meta.kpis(shownParams);
  const valueOf = (key: string): number | null =>
    report?.summary.find((x) => x.key === key)?.value ?? null;
  /** A summary value, or its lower bound (a capped udp ramp) shown as "≥ X". */
  const shownValue = (key: string): { value: number | null; bound: boolean } => {
    const v = valueOf(key);
    const lb = v == null ? lowerBoundOf(report, key) : null;
    return lb != null ? { value: lb, bound: true } : { value: v, bound: false };
  };

  const failed =
    live?.error || report?.status === 'failed'
      ? (live?.error?.message ?? report?.error ?? 'The run failed.')
      : null;
  const cancelled = report?.status === 'cancelled';
  const failedPhase = live?.last_active_phase ? PHASE_PLAIN[live.last_active_phase] : null;

  // what a screen reader hears: the phase while running, then the outcome
  const head = kpis[0];
  const announce = running
    ? `${meta.label}: ${phaseWords(live?.phase ?? 'running')}`
    : live && (live.report || live.error)
      ? failed
        ? `${meta.label} run failed. ${failed}`
        : cancelled
          ? `${meta.label} run cancelled.`
          : `${meta.label} run done. ${head?.label ?? ''} ${shownValue(head?.key ?? '').bound ? 'at least ' : ''}${withUnit(fmtNumber(shownValue(head?.key ?? '').value), head?.unit ?? '')}`
      : '';

  const J = jy();
  const centre = (
    <div className="bf-centre">
      {meta.notice ? (
        <Notice kind="quiet" title={meta.notice.title}>
          {meta.notice.text}
        </Notice>
      ) : null}
      {failed ? (
        <Notice
          kind="error"
          title={failedPhase ? `The run failed while ${failedPhase}.` : 'The run failed.'}
          actions={
            <>
              <Button
                size="small"
                icon="refresh"
                disabled={!!reason}
                reason={reason}
                onClick={start}
              >
                Run again
              </Button>
              {report ? (
                <Button
                  size="small"
                  variant="quiet"
                  trailingIcon="arrow-right"
                  onClick={() => go('history', report.run_id)}
                >
                  Open report
                </Button>
              ) : null}
            </>
          }
        >
          {failed}{' '}
          {report?.series.some((x) => x.points.length) ? 'The partial series is kept below.' : null}
        </Notice>
      ) : cancelled ? (
        <Notice kind="quiet" title="Cancelled">
          The run stopped early{failedPhase ? ` while ${failedPhase}` : ''}. What it measured so far
          is kept below and not used in trends.
        </Notice>
      ) : null}

      {unknownParams ? (
        <Notice kind="quiet" title="Reattached after a reload">
          This run started before the window reloaded. Its settings, numbers and charts arrive with
          its report; the bar above follows its progress until then.
        </Notice>
      ) : (
        <>
          <div data-measure="kpi row">
            <KpiRow dense={kpis.length > 5}>
              {kpis.map((k) => {
                const shown = running
                  ? { value: k.live ? (live?.last[k.live] ?? null) : null, bound: false }
                  : shownValue(k.key);
                const v = shown.value;
                const item = report?.summary.find((x) => x.key === k.key);
                return (
                  <Kpi
                    key={k.key}
                    label={k.label}
                    value={v}
                    bound={shown.bound}
                    unit={k.unit}
                    digits={k.digits}
                    pending={running && !k.live}
                    bad={!running && BAD_WHEN_HIGH.has(k.key) && (v ?? 0) > 1}
                    sub={
                      running
                        ? k.live
                          ? (k.liveSub ?? 'live')
                          : 'after the run'
                        : report && report.status !== 'ok'
                          ? 'partial'
                          : shown.bound
                            ? 'lower bound: no loss limit reached'
                            : item?.uncertainty != null
                              ? `± ${withUnit(fmtNumber(item.uncertainty), k.unit)}`
                              : report
                                ? undefined
                                : 'no run yet'
                    }
                  />
                );
              })}
            </KpiRow>
          </div>

          <div data-measure="main chart">
            <MainChart data={data} />
          </div>

          <Panels data={data} />

          <div data-measure="path">
            <PathPanel data={data} />
          </div>
        </>
      )}
      <span className="sr" aria-live="polite">
        {announce}
      </span>
    </div>
  );

  return (
    <div className="tool bench">
      {topBar()}
      <div className="main">
        <aside className="pane left" aria-label="Parameters">
          <div className="pane-head">
            <span className="label">Parameters</span>
          </div>
          <div className="pane-body">
            {meta.presets.length ? (
              <Field label="Preset">
                <Seg
                  label="Preset"
                  value={preset}
                  disabled={running}
                  options={meta.presets.map((pr) => ({ value: pr.id, label: pr.label }))}
                  onChange={(id) => {
                    const pr = meta.presets.find((x) => x.id === id);
                    setPreset(id);
                    setBad({});
                    if (pr)
                      setParams({
                        ...(meta.defaults as object),
                        ...(pr.params as object),
                      } as WorkloadParams);
                  }}
                />
              </Field>
            ) : null}
            {unknownParams ? (
              <p className="caption muted">
                The running settings arrive with the report. You can change them once it ends.
              </p>
            ) : (
              visibleFields(meta, paneParams).map((f) => (
                <ParamField
                  key={`${f.key}-${f.kind === 'number' ? `${f.min}-${f.max}` : ''}`}
                  field={f}
                  params={paneParams}
                  disabled={running}
                  onChange={(key, v, ok) => {
                    const next = editParams(meta, params, bad, key, v, ok);
                    setParams(next.params);
                    setBad(next.bad);
                    setPreset('');
                  }}
                />
              ))
            )}
          </div>
          <div className="bf-run">
            <span className="caption muted">
              Takes about {fmtDuration(meta.estimateMs(paneParams))}
            </span>
            {running && live ? (
              <Tooltip block tip={<KbdHint label="Cancel" keys={['Esc']} />}>
                <Button wide icon="stop" onClick={cancel} aria-keyshortcuts="Escape">
                  Cancel
                </Button>
              </Tooltip>
            ) : (
              <Tooltip block tip={reason ? null : <KbdHint label="Run" keys={[MOD, 'R']} />}>
                <Button
                  variant="primary"
                  wide
                  icon="play"
                  disabled={!!reason}
                  reason={reason}
                  onClick={start}
                  aria-keyshortcuts={MOD === '⌘' ? 'Meta+R' : 'Control+R'}
                >
                  Run
                </Button>
              </Tooltip>
            )}
            {reason ? <span className="bf-reason">{reason}</span> : null}
          </div>
        </aside>

        <section className="pane centre" aria-label="Results">
          {J?.Blueprint ? (
            <J.Blueprint
              as="div"
              className="bf-bp"
              show={construction}
              hotkey={false}
              columns={12}
              gutter={16}
              margin={24}
            >
              {centre}
            </J.Blueprint>
          ) : (
            centre
          )}
        </section>

        <aside className="pane right" aria-label="Details">
          {details()}
        </aside>
      </div>
    </div>
  );

  // ---------------------------------------------------------------- pieces

  function topBar() {
    const st: ReactNode =
      running && live ? (
        <span className="bf-state">
          <Status kind="running">{phaseWords(live.phase)}</Status>
          <Meter fraction={live.fraction} label="Run progress" mono>
            {fmtDuration(Date.now() - live.started_at)}
          </Meter>
        </span>
      ) : report ? (
        <span className="bf-state">
          {report.status === 'ok' ? (
            <Status kind="done">Done</Status>
          ) : report.status === 'cancelled' ? (
            <Status kind="waiting">Cancelled</Status>
          ) : (
            <Status kind="failed">Failed</Status>
          )}
          <span className="mono muted">{fmtWhen(report.started_at)}</span>
          <Button
            size="small"
            variant="quiet"
            trailingIcon="arrow-right"
            onClick={() => go('history', report.run_id)}
          >
            Report
          </Button>
        </span>
      ) : (
        <span className="bf-state">
          <Status kind="waiting">No run yet</Status>
        </span>
      );
    return (
      <div className="topbar bf-top">
        <div className="group bf-title">
          <Icon name={meta.icon} />
          <h1>{meta.label}</h1>
        </div>
        <span className="divider" aria-hidden="true" />
        <div className="group bf-target">
          {target ? (
            <>
              <span className="label">Target</span>
              {target.online ? (
                <Status kind="live">{target.device_name}</Status>
              ) : (
                <Status kind="off">{target.device_name}</Status>
              )}
              <span className="caption muted">{fmtOs(target.os)}</span>
              <span className="caption">
                {target.online ? fmtConnection(target.connection) : 'offline'}
              </span>
              <span className="mono">
                {target.rtt_ms == null ? DASH : `${fmtNumber(target.rtt_ms)} ms`}
              </span>
            </>
          ) : (
            <span className="muted">No target</span>
          )}
          <Button size="small" variant="quiet" onClick={() => go('peers')}>
            {target ? 'Change' : 'Choose'}
          </Button>
        </div>
        <span className="spacer" />
        {st}
      </div>
    );
  }

  function details() {
    if (unknownParams)
      return (
        <div className="pane-body bf-details">
          <section className="dt-sec">
            <h2 className="label">What is measured</h2>
            <p className="muted">With the report: this run started before the window reloaded.</p>
          </section>
        </div>
      );
    const notes = mergeNotes(report?.notes ?? [], meta.notes(shownParams));
    const dist = distributionOf(meta, shownParams, running ? null : report);
    const res = running ? null : report?.resources;
    const facts = meta.facts(shownParams);
    return (
      <div className="pane-body bf-details">
        <section className="dt-sec">
          <h2 className="label">What is measured</h2>
          {meta.measured(shownParams).map((t) => (
            <p key={t}>{t}</p>
          ))}
        </section>
        <section className="dt-sec">
          <h2 className="label">Data path</h2>
          <ol className="hops">
            {meta.hops(shownParams).map((h, i) => (
              <li key={h}>
                <span className="hop-n mono">{i + 1}</span>
                <span>{h}</span>
              </li>
            ))}
          </ol>
        </section>
        <section className="dt-sec">
          <h2 className="label">Notes</h2>
          <ul className="notes">
            {notes.map((n) => (
              <li key={n}>{n}</li>
            ))}
          </ul>
        </section>
        {dist ? (
          <section className="dt-sec">
            <h2 className="label">{dist.title}</h2>
            {dist.dist ? (
              <DistributionTable d={dist.dist} unit={dist.unit} />
            ) : (
              <p className="muted">After a run.</p>
            )}
          </section>
        ) : null}
        {facts.length && report && !running ? (
          <section className="dt-sec">
            <h2 className="label">Counts</h2>
            <FactsTable facts={facts} report={report} />
          </section>
        ) : null}
        <section className="dt-sec">
          <h2 className="label">Path</h2>
          {report && !running ? (
            <PathFacts report={report} />
          ) : (
            <p className="muted">After a run.</p>
          )}
        </section>
        <section className="dt-sec">
          <h2 className="label">Resources</h2>
          {res && (res.local || res.peer || res.combined) ? (
            <ResourceTable local={res.local} peer={res.peer} combined={res.combined} />
          ) : (
            <p className="muted">After a run.</p>
          )}
        </section>
      </div>
    );
  }
}

function ParamField({
  field,
  params,
  disabled,
  onChange,
}: {
  field: FieldMeta<Record<string, unknown>>;
  params: WorkloadParams;
  disabled: boolean;
  onChange: (key: string, v: unknown, ok: boolean) => void;
}) {
  const rec = params as unknown as Record<string, unknown>;
  const hint = textOf(field.hint, params) || undefined;
  if (field.kind === 'enum') {
    return (
      <Field label={field.label} hint={hint}>
        <Seg
          label={field.label}
          value={String(rec[field.key])}
          disabled={disabled}
          options={field.options}
          onChange={(v) => onChange(field.key, v, true)}
        />
      </Field>
    );
  }
  if (field.kind === 'bool') {
    return (
      <div className="field">
        <Switch
          checked={!!rec[field.key]}
          disabled={disabled}
          onChange={(v) => onChange(field.key, v, true)}
        >
          {field.label}
        </Switch>
        {hint ? <span className="hint caption">{hint}</span> : null}
      </div>
    );
  }
  return (
    <NumberField
      label={field.label}
      value={(rec[field.key] as number | null) ?? null}
      min={field.min}
      max={field.max}
      unit={field.unit}
      integer={field.integer}
      nullable={field.nullable}
      nullLabel={textOf(field.nullLabel, params)}
      hint={hint}
      disabled={disabled}
      onChange={(v, ok) => onChange(field.key, v, ok)}
    />
  );
}
