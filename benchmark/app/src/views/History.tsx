// History (RFC 027 §11.3): every run, local, CLI-imported and CI-imported. Filters, two picked
// for Compare, imports from the picker or a drop, and delete with a confirmation.

import { useEffect, useMemo, useState, type ReactNode } from 'react';
import type { PathLabel, RunSummary, Tool, WorkloadKind } from '../api';
import { useAppState, useAppStore } from '../state/context';
import { load, save } from '../hooks/storage';
import { Button, ConfirmButton } from '../components/Button';
import { Check, Seg, Select } from '../components/Controls';
import { Table, type Column } from '../components/Data';
import { Empty } from '../components/Notice';
import { Page, Section } from '../components/Misc';
import { Status } from '../components/Status';
import { Icon } from '../components/Icon';
import { useConstructionFlag } from '../charts/construction';
import { REGISTRY, WORKLOAD_ORDER, headlineOf } from '../workloads/registry';
import { FLAG_TIPS, FLAG_WORDS, flagsOf } from '../lib/compare';
import { DASH, fmtNumber, fmtWhen, plural, withUnit } from '../lib/format';
import type { Page as PageId } from '../hooks/useRoute';

interface Filters {
  workload: WorkloadKind | 'all';
  path: PathLabel | 'all';
  peer: string;
  tool: 'all' | 'app' | 'cli' | 'imported';
}

/** Dev only, for screenshots: ?pick=<run_id>,<run_id> picks runs; ?drop=1 shows a drag over. */
const devQ =
  import.meta.env.DEV && typeof window !== 'undefined'
    ? new URLSearchParams(window.location.search)
    : null;
const DEV_PICK = devQ?.get('pick')?.split(',').filter(Boolean).slice(0, 2) ?? [];
const DEV_DROP = devQ?.get('drop') === '1';

export const TOPOLOGY_WORDS: Record<string, string> = {
  'separate-hosts': '2 hosts',
  'same-host': '1 host',
  'same-process': '1 process',
};

const NO_FILTERS: Filters = { workload: 'all', path: 'all', peer: 'all', tool: 'all' };

const TOOL_OF: Record<Filters['tool'], Tool | null> = {
  all: null,
  app: 'truffle-bench-app',
  cli: 'truffle-bench-cli',
  imported: 'imported',
};

export function StatusOf({ run }: { run: Pick<RunSummary, 'status'> }) {
  return run.status === 'ok' ? (
    <Status kind="done">Done</Status>
  ) : run.status === 'cancelled' ? (
    <Status kind="waiting">Cancelled</Status>
  ) : (
    <Status kind="failed">Failed</Status>
  );
}

export function workloadLabel(w: string): string {
  return REGISTRY[w as WorkloadKind]?.label ?? w;
}

export function Headline({
  run,
}: {
  run: Pick<RunSummary, 'workload' | 'summary' | 'status' | 'error_code'>;
}) {
  const h = headlineOf(run);
  if (run.status === 'failed' && (h?.value == null || !h))
    return <span className="bad mono">{run.error_code?.replace(/-/g, ' ') ?? 'failed'}</span>;
  if (!h) return <>{DASH}</>;
  return (
    <span className="hl" title={h.label}>
      <span className="hl-v">{withUnit(fmtNumber(h.value), h.unit)}</span>
      <span className="hl-l">{h.label}</span>
    </span>
  );
}

export function Flags({ run }: { run: RunSummary }) {
  const flags = flagsOf(run);
  if (!flags.length) return null;
  return (
    <span className="flags">
      {flags.map((f) => (
        <span
          key={f}
          className={`chip${f === 'imported' || f === 'cli' || f === 'partial' ? ' quiet' : ' flag'}`}
          title={FLAG_TIPS[f]}
        >
          {f === 'overlap' ? `overlap ${run.overlapping_runs}` : FLAG_WORDS[f]}
        </span>
      ))}
    </span>
  );
}

export function History({ go }: { go: (p: PageId, id?: string) => void }) {
  const store = useAppStore();
  const s = useAppState();
  const construction = useConstructionFlag();
  const [filters, setFilters] = useState<Filters>(() => ({
    ...NO_FILTERS,
    ...load<Partial<Filters>>('history/filters', {}),
  }));
  const [picked, setPicked] = useState<string[]>(() => DEV_PICK);
  const [hover, setHover] = useState(DEV_DROP);
  const [busy, setBusy] = useState(false);

  // OS drops on the window, while this view is open
  useEffect(
    () =>
      store.api.onFileDrop(
        (paths) => {
          setHover(false);
          void store.importPaths(paths);
        },
        (over) => setHover(over),
      ),
    [store],
  );
  useEffect(() => {
    void store.refreshRuns();
  }, [store]);

  const setF = (patch: Partial<Filters>) => {
    const next = { ...filters, ...patch };
    setFilters(next);
    save('history/filters', next);
  };

  const peers = useMemo(
    () => [...new Set(s.runs.map((r) => r.peer_name).filter((x): x is string => !!x))].sort(),
    [s.runs],
  );
  const shown = s.runs.filter(
    (r) =>
      (filters.workload === 'all' || r.workload === filters.workload) &&
      (filters.path === 'all' || r.path_label === filters.path) &&
      (filters.peer === 'all' || r.peer_name === filters.peer) &&
      (filters.tool === 'all' || r.tool === TOOL_OF[filters.tool]),
  );
  const filtered = JSON.stringify(filters) !== JSON.stringify(NO_FILTERS);
  const pickedRuns = picked
    .map((id) => s.runs.find((r) => r.run_id === id))
    .filter((r): r is RunSummary => !!r);

  const toggle = (id: string, on: boolean) =>
    setPicked((cur) => {
      const rest = cur.filter((x) => x !== id);
      if (!on) return rest;
      return [...rest, id].slice(-2);
    });

  const compare = () => {
    if (pickedRuns.length !== 2) return;
    const [a, b] = [...pickedRuns].sort((x, y) => x.started_at.localeCompare(y.started_at));
    go('history', `compare/${a.run_id}/${b.run_id}`);
  };

  const columns: Column<RunSummary>[] = [
    {
      key: 'pick',
      label: <span className="sr">Pick to compare</span>,
      width: 36,
      render: (r) => (
        <span
          className="pick"
          onClick={(e) => e.stopPropagation()}
          onKeyDown={(e) => e.stopPropagation()}
        >
          <Check checked={picked.includes(r.run_id)} onChange={(v) => toggle(r.run_id, v)}>
            <span className="sr">Pick {r.label}</span>
          </Check>
        </span>
      ),
    },
    { key: 'status', label: 'Status', width: 100, render: (r) => <StatusOf run={r} /> },
    {
      key: 'workload',
      label: 'Workload',
      render: (r) => {
        const m = REGISTRY[r.workload];
        return (
          <span className="wl">
            {m ? <Icon name={m.icon} size={16} /> : null}
            <span>{workloadLabel(r.workload)}</span>
          </span>
        );
      },
    },
    {
      key: 'label',
      label: 'Run',
      render: (r) => (
        <span className="run-label">
          <span>{r.label}</span>
          <Flags run={r} />
        </span>
      ),
    },
    {
      key: 'peer',
      label: 'Peer',
      render: (r) =>
        r.peer_name ? (
          <span className="cell-clip" title={r.peer_name}>
            {r.peer_name}
          </span>
        ) : (
          <span className="muted">none</span>
        ),
    },
    {
      key: 'path',
      label: 'Path',
      render: (r) => r.path_label,
    },
    {
      key: 'topo',
      label: 'Topology',
      render: (r) => <span className="muted nowrap">{TOPOLOGY_WORDS[r.topology]}</span>,
    },
    { key: 'head', label: 'Result', num: true, render: (r) => <Headline run={r} /> },
    { key: 'date', label: 'When', num: true, render: (r) => fmtWhen(r.started_at) },
  ];

  const actions: ReactNode = (
    <>
      <Button
        icon="upload"
        disabled={busy}
        onClick={async () => {
          setBusy(true);
          await store.importPick();
          setBusy(false);
        }}
      >
        Import reports
      </Button>
    </>
  );

  return (
    <Page
      construction={construction}
      eyebrow="History"
      title="History"
      lede="Every run on this device, and reports imported from the CLI and CI. Pick two to compare them."
      actions={actions}
    >
      <div
        className={`dropzone${hover ? ' over' : ''}`}
        data-measure="drop zone"
        aria-live="polite"
      >
        <Icon name="upload" />
        <span>
          {hover
            ? 'Drop to import.'
            : 'Drop report files here to import them: JSON reports, folders of them, or a GitHub artifact .zip.'}
        </span>
      </div>

      <Section
        label="Runs"
        meta={
          filtered
            ? `${plural(shown.length, 'run')} of ${s.runs.length}`
            : plural(s.runs.length, 'run')
        }
      >
        <div className="filters" role="group" aria-label="Filters">
          <label className="flt">
            <span className="flt-l">Workload</span>
            <Select
              label="Workload"
              value={filters.workload}
              options={[
                { value: 'all', label: 'All workloads' },
                ...WORKLOAD_ORDER.map((w) => ({ value: w, label: REGISTRY[w].label })),
              ]}
              onChange={(v) => setF({ workload: v as Filters['workload'] })}
            />
          </label>
          <label className="flt">
            <span className="flt-l">Peer</span>
            <Select
              label="Peer"
              value={filters.peer}
              options={[
                { value: 'all', label: 'All peers' },
                ...peers.map((p) => ({ value: p, label: p })),
              ]}
              onChange={(v) => setF({ peer: v })}
            />
          </label>
          <div className="flt">
            <span className="flt-l">Path</span>
            <Seg
              label="Path"
              value={filters.path}
              options={[
                { value: 'all', label: 'All' },
                { value: 'direct', label: 'Direct' },
                { value: 'relay', label: 'Relay' },
                { value: 'mixed', label: 'Mixed' },
                { value: 'unknown', label: 'Unknown' },
              ]}
              onChange={(v) => setF({ path: v as Filters['path'] })}
            />
          </div>
          <div className="flt">
            <span className="flt-l">Tool</span>
            <Seg
              label="Tool"
              value={filters.tool}
              options={[
                { value: 'all', label: 'All' },
                { value: 'app', label: 'App' },
                { value: 'cli', label: 'CLI' },
                { value: 'imported', label: 'Imported' },
              ]}
              onChange={(v) => setF({ tool: v as Filters['tool'] })}
            />
          </div>
          {filtered ? (
            <Button size="small" variant="quiet" icon="close" onClick={() => setF(NO_FILTERS)}>
              Clear filters
            </Button>
          ) : null}
        </div>

        <div className="pickbar" aria-live="polite">
          <span className="ink2">
            {pickedRuns.length === 0
              ? 'Pick two runs to compare them.'
              : pickedRuns.length === 1
                ? `Picked ${pickedRuns[0].label}. Pick one more.`
                : `Picked 2: ${pickedRuns.map((r) => workloadLabel(r.workload)).join(' and ')}${
                    pickedRuns[0].compare_key === pickedRuns[1].compare_key
                      ? ', same settings, path and build.'
                      : '; their settings differ.'
                  }`}
          </span>
          <span className="spacer" />
          <Button
            size="small"
            variant={pickedRuns.length === 2 ? 'primary' : 'secondary'}
            disabled={pickedRuns.length !== 2}
            reason={pickedRuns.length !== 2 ? 'Pick two runs first.' : null}
            onClick={compare}
          >
            Compare
          </Button>
          {pickedRuns.length ? (
            <>
              <ConfirmButton
                icon="trash"
                confirm={pickedRuns.length === 1 ? 'Delete this run' : 'Delete 2 runs'}
                onConfirm={() => {
                  const ids = pickedRuns.map((r) => r.run_id);
                  setPicked([]);
                  void store.deleteRuns(ids);
                }}
              >
                Delete
              </ConfirmButton>
              <Button size="small" variant="quiet" onClick={() => setPicked([])}>
                Clear
              </Button>
            </>
          ) : null}
        </div>

        {shown.length ? (
          <Table
            caption="Runs"
            className="history-table"
            columns={columns}
            rows={shown}
            rowKey={(r) => r.run_id}
            onRowClick={(r) => go('history', r.run_id)}
            rowClass={(r) =>
              [picked.includes(r.run_id) ? 'picked' : '', r.status !== 'ok' ? 'not-ok' : '']
                .filter(Boolean)
                .join(' ')
            }
          />
        ) : s.runs.length ? (
          <Empty
            title="No runs match these filters"
            actions={
              <Button size="small" onClick={() => setF(NO_FILTERS)}>
                Clear filters
              </Button>
            }
          />
        ) : (
          <Empty title="No runs yet">
            Run a benchmark, or import reports from the CLI and CI: drop them above or use Import
            reports.
          </Empty>
        )}
      </Section>
    </Page>
  );
}
