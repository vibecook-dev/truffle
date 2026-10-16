// Suite (RFC 027 §11.3, §8.4): a profile picker, the matrix of its items with each one's
// status, and Run the suite. Suites run in Rust, so they survive a reload: the store follows a
// running suite through runs_active and run_attach. Earlier suites are grouped by suite_id.

import { useEffect, useMemo, useState } from 'react';
import type { RunSummary, SuiteProfile, WorkloadKind, WorkloadParams } from '../api';
import { useAppState, useAppStore } from '../state/context';
import { activeRun, runBlockReason } from '../state/store';
import { Button } from '../components/Button';
import { Seg } from '../components/Controls';
import { Meter, Table, type Column } from '../components/Data';
import { Empty, Notice } from '../components/Notice';
import { KbdHint, Loader, Page, Section, Tooltip } from '../components/Misc';
import { Status } from '../components/Status';
import { Icon } from '../components/Icon';
import { useConstructionFlag } from '../charts/construction';
import { REGISTRY, paramsLabel } from '../workloads/registry';
import { fmtDuration, fmtWhen, plural } from '../lib/format';
import { MOD, useViewActions } from '../hooks/useShortcuts';
import type { Page as PageId } from '../hooks/useRoute';
import { Headline, StatusOf, workloadLabel } from './History';
import { FLAG_TIPS } from '../lib/compare';
import { phaseWords } from './RunParts';

const COOLDOWN_MS = 1000;

interface Row {
  index: number;
  params: WorkloadParams | null;
  label: string;
  workload: string;
  run: RunSummary | null;
  /** live phase when this is the item running now */
  phase: string | null;
  fraction: number | null;
}

export function Suite({ go }: { go: (p: PageId, id?: string) => void }) {
  const store = useAppStore();
  const s = useAppState();
  const construction = useConstructionFlag();
  const target = store.target();
  const [profiles, setProfiles] = useState<SuiteProfile[] | null>(null);
  const [chosen, setChosen] = useState<string>('smoke');
  useEffect(() => {
    let off = false;
    void store.api
      .suiteProfiles()
      .then((p) => !off && setProfiles(p))
      .catch(() => !off && setProfiles([]));
    return () => {
      off = true;
    };
  }, [store]);

  const suite = s.suite;
  const running = !!suite && !suite.result && !suite.error;
  // follow a suite this window reattached to, or started elsewhere in the app
  const suiteProfile = suite?.profile ?? null;
  const suiteId = suite?.suite_id ?? null;
  useEffect(() => {
    if (suiteProfile) setChosen(suiteProfile);
  }, [suiteId, suiteProfile]);
  const profileId = running && suiteProfile ? suiteProfile : chosen;
  const profile = profiles?.find((p) => p.id === profileId) ?? null;
  // the matrix shows the current or last suite when it is of this profile (or of one we can't name)
  const showSuite =
    !!suite && (running || !!suite.result) && (suiteProfile == null || suiteProfile === profileId);
  const block = running ? null : runBlockReason(s, target, 'path');
  const debugBlock =
    profileId === 'nightly' && s.appInfo?.build_profile === 'debug'
      ? 'The nightly suite refuses debug builds: their numbers aren’t comparable.'
      : null;
  const reason = block ?? debugBlock;

  const start = () => {
    if (target && !reason && profile) void store.runSuite(profile.id, target);
  };
  const cancel = () => {
    if (suite && running) void store.cancel(suite.suite_id);
  };
  useViewActions({
    run: start,
    canRun: !reason && !running && !!profile,
    cancel,
    canCancel: running,
  });

  // this suite's runs, in the order they ran
  const suiteRuns = useMemo(
    () =>
      suite
        ? s.runs
            .filter((r) => r.suite_id === suite.suite_id)
            .sort((x, y) => x.started_at.localeCompare(y.started_at))
        : [],
    [s.runs, suite],
  );
  const live = activeRun(s);
  const progress = suite?.progress;

  const unknownProfile = showSuite && suiteProfile == null;
  const items: WorkloadParams[] = unknownProfile ? [] : (profile?.items ?? []);
  const liveItem = live && suite && live.suite_id === suite.suite_id ? live : null;
  const base: (WorkloadParams | null)[] = items.length
    ? items
    : [...suiteRuns.map(() => null), ...(liveItem && showSuite ? [null] : [])];
  const rows: Row[] = base.map((p, i) => {
    let run: RunSummary | null = null;
    let phase: string | null = null;
    let fraction: number | null = null;
    if (showSuite && suite) {
      const id = suite.items[i];
      run = (id ? s.runs.find((r) => r.run_id === id) : suiteRuns[i]) ?? null;
      if (!run && progress && progress.index === i) {
        // reattached: the item's live run is fresher than the last polled Progress
        const cur = suite.reattached && liveItem ? liveItem : null;
        phase = cur?.phase ?? progress.run.phase;
        fraction = cur ? cur.fraction : progress.run.fraction;
      } else if (!run && !progress && suite.reattached && liveItem && i === suiteRuns.length) {
        phase = liveItem.phase;
        fraction = liveItem.fraction;
      }
    }
    return {
      index: i,
      params: p,
      label: p
        ? paramsLabel(p)
        : (run?.label ?? (phase && liveItem ? 'Running now' : `Item ${i + 1}`)),
      workload: p?.workload ?? run?.workload ?? (phase && liveItem ? liveItem.workload : ''),
      run,
      phase,
      fraction,
    };
  });
  const doneCount = rows.filter((r) => r.run).length;
  const failedCount = rows.filter((r) => r.run?.status === 'failed').length;
  const cancelledCount = rows.filter((r) => r.run?.status === 'cancelled').length;
  const estimate = items.reduce((a, p) => a + REGISTRY[p.workload].estimateMs(p) + COOLDOWN_MS, 0);

  const columns: Column<Row>[] = [
    { key: 'n', label: '#', width: 44, num: true, render: (r) => r.index + 1 },
    {
      key: 'w',
      label: 'Workload',
      width: 170,
      render: (r) => {
        const m = REGISTRY[r.workload as WorkloadKind];
        return (
          <span className="wl">
            {m ? <Icon name={m.icon} size={16} /> : null}
            <span>{workloadLabel(r.workload)}</span>
          </span>
        );
      },
    },
    { key: 'l', label: 'Item', render: (r) => <span className="run-label">{r.label}</span> },
    {
      key: 's',
      label: 'Status',
      width: 200,
      render: (r) =>
        r.run ? (
          <StatusOf run={r.run} />
        ) : r.phase ? (
          <span className="item-run">
            <Status kind="running">{phaseWords(r.phase)}</Status>
            <Meter fraction={r.fraction} label={`Item ${r.index + 1} progress`} />
          </span>
        ) : running && showSuite ? (
          <Status kind="waiting">Waiting</Status>
        ) : (
          <span className="muted">Not run</span>
        ),
    },
    {
      key: 'h',
      label: 'Result',
      num: true,
      width: 180,
      render: (r) => (r.run ? <Headline run={r.run} /> : null),
    },
  ];

  // earlier suites, grouped by suite_id, newest first
  const groups = useMemo(() => {
    const by = new Map<string, RunSummary[]>();
    for (const r of s.runs) {
      if (!r.suite_id || r.suite_id === suite?.suite_id) continue;
      const g = by.get(r.suite_id) ?? [];
      g.push(r);
      by.set(r.suite_id, g);
    }
    return [...by.entries()]
      .map(([id, runs]) => ({
        id,
        runs: runs.sort((x, y) => x.started_at.localeCompare(y.started_at)),
        started: runs.reduce((m, r) => (r.started_at < m ? r.started_at : m), runs[0].started_at),
      }))
      .sort((x, y) => y.started.localeCompare(x.started))
      .slice(0, 6);
  }, [s.runs, suite?.suite_id]);

  // which profile an earlier suite ran: the one whose item labels are exactly its runs' labels
  const guess = (runs: RunSummary[]) => {
    const labels = runs
      .map((r) => r.label)
      .sort()
      .join('\n');
    return (
      profiles?.find((p) => p.items.map(paramsLabel).sort().join('\n') === labels)?.label ?? 'Suite'
    );
  };

  return (
    <Page
      construction={construction}
      eyebrow="Suite"
      title="Suite"
      lede={`Every item of a profile, one after another against ${target?.device_name ?? 'your target'}, with a 1 s pause between items. It runs in truffle bench’s background process, so it keeps going if this window reloads or closes.`}
      actions={
        running ? (
          <Tooltip tip={<KbdHint label="Cancel" keys={['Esc']} />}>
            <Button icon="stop" onClick={cancel} aria-keyshortcuts="Escape">
              Cancel the suite
            </Button>
          </Tooltip>
        ) : (
          <Tooltip tip={reason ? null : <KbdHint label="Run" keys={[MOD, 'R']} />}>
            <Button
              variant="primary"
              icon="play"
              disabled={!!reason || !profile}
              reason={reason}
              onClick={start}
            >
              Run the suite
            </Button>
          </Tooltip>
        )
      }
    >
      {reason && !running ? (
        <Notice kind="quiet" title="The suite can’t start yet">
          {reason}
        </Notice>
      ) : null}

      <Section
        label="Profile"
        meta={
          unknownProfile
            ? 'not known'
            : profile
              ? `${plural(items.length, 'item')} · about ${fmtDuration(estimate)}`
              : undefined
        }
      >
        {unknownProfile ? (
          <p className="ink2">
            This suite started before this window opened, and the engine didn’t say which profile it
            is. Its items show here as they run.
          </p>
        ) : profiles == null ? (
          <Loader label="Loading the profiles" />
        ) : profiles.length ? (
          <div className="suite-pick">
            <Seg
              label="Profile"
              value={profileId}
              disabled={running}
              options={profiles.map((p) => ({ value: p.id, label: p.label }))}
              onChange={(v) => setChosen(v)}
            />
            <p className="ink2">{profile?.description}</p>
          </div>
        ) : (
          <Empty title="No suite profiles">The engine didn’t list any.</Empty>
        )}
      </Section>

      <Section
        label={
          running ? 'Running' : suite?.result && showSuite ? 'Last run of this profile' : 'Items'
        }
        meta={
          showSuite && suite
            ? `${doneCount} of ${rows.length} done${failedCount ? ` · ${failedCount} failed` : ''}${cancelledCount ? ` · ${cancelledCount} cancelled` : ''} · started ${fmtWhen(new Date(suite.started_at).toISOString())}`
            : undefined
        }
      >
        {running && suite ? (
          <div className="suite-progress" aria-live="polite">
            <Meter
              wide
              label="Suite progress"
              fraction={
                rows.length
                  ? (doneCount + (rows.find((r) => r.phase)?.fraction ?? 0)) / rows.length
                  : null
              }
            >
              {progress
                ? `${progress.index + 1} of ${progress.total} · ${items[progress.index] ? paramsLabel(items[progress.index]) : progress.item_label}`
                : suite.reattached
                  ? 'Reattached after a reload; following it from the background process'
                  : 'Starting'}
            </Meter>
          </div>
        ) : null}
        {suite?.result && showSuite ? (
          <Notice
            kind={suite.result.failed ? 'error' : 'quiet'}
            title={
              suite.result.cancelled
                ? 'Cancelled'
                : suite.result.failed
                  ? `${plural(suite.result.failed, 'item')} failed`
                  : `All ${plural(suite.result.run_ids.length, 'item')} ran`
            }
            actions={
              <Button
                size="small"
                variant="quiet"
                trailingIcon="arrow-right"
                onClick={() => go('history')}
              >
                Open History
              </Button>
            }
          >
            Each item is a run in History, tagged with this suite.
          </Notice>
        ) : null}
        {rows.length ? (
          <Table
            caption="Suite items"
            columns={columns}
            rows={rows}
            rowKey={(r) => String(r.index)}
            onRowClick={(r) => (r.run ? go('history', r.run.run_id) : undefined)}
            rowClass={(r) => (r.phase ? 'current' : r.run ? '' : 'pending')}
          />
        ) : null}
      </Section>

      <Section label="Earlier suites" meta={groups.length ? undefined : 'none yet'}>
        {groups.length ? (
          <div className="suite-groups">
            {groups.map((g) => {
              // redline counts failures only; cancelled runs were stopped on purpose
              const bad = g.runs.filter((r) => r.status === 'failed').length;
              const cancelled = g.runs.filter((r) => r.status === 'cancelled').length;
              return (
                <article key={g.id} className="card suite-group">
                  <div className="card-head">
                    <Icon name="suite" />
                    <h3>{guess(g.runs)}</h3>
                    {bad ? (
                      <Status kind="failed">{`${bad} failed`}</Status>
                    ) : cancelled ? (
                      <Status kind="waiting">{`${cancelled} cancelled`}</Status>
                    ) : (
                      <Status kind="done">All ok</Status>
                    )}
                  </div>
                  <span className="caption muted sg-meta">
                    {fmtWhen(g.started)} · {plural(g.runs.length, 'run')} ·{' '}
                    {g.runs[0]?.peer_name ?? 'no peer'}
                    {g.runs.some((r) => r.flags.includes('debug')) ? (
                      <span className="chip strong" title={FLAG_TIPS.debug}>
                        debug
                      </span>
                    ) : null}
                  </span>
                  <ul className="suite-runs">
                    {g.runs.map((r) => (
                      <li key={r.run_id}>
                        <button
                          type="button"
                          className="sr-row"
                          onClick={() => go('history', r.run_id)}
                        >
                          <span
                            className={`sq ${r.status === 'ok' ? 'ok' : r.status === 'failed' ? 'failed' : 'cancelled'}`}
                            role="img"
                            aria-label={r.status}
                          />
                          <span className="sr-w">{workloadLabel(r.workload)}</span>
                          <span className="sr-h">
                            <Headline run={r} short />
                          </span>
                        </button>
                      </li>
                    ))}
                  </ul>
                </article>
              );
            })}
          </div>
        ) : (
          <p className="muted">Suites you run show up here, grouped.</p>
        )}
      </Section>
    </Page>
  );
}
