// truffle bench: a rail of places on the left, one view at a time on the right, the title strip
// on top (after James Yong Home's App.jsx). Pages are addressed by hash: #/overview, #/tcp.

import { useCallback, useEffect, useMemo, useRef, useState, type ReactNode } from 'react';
import { useAppState, useAppStore } from './state/context';
import { activeRun, workloadName } from './state/store';
import { useRoute, type Page } from './hooks/useRoute';
import { useConstruction } from './hooks/useConstruction';
import { load, save } from './hooks/storage';
import { ConstructionContext } from './charts/construction';
import { Rail, RailFoot, TitleStrip, type RailEntry, type DotKind } from './components/Chrome';
import { Loader, Mark, Toasts } from './components/Misc';
import { SignIn } from './views/SignIn';
import { Overview } from './views/Overview';
import { Peers } from './views/Peers';
import { Planned } from './views/Planned';
import { BenchmarkFrame } from './views/BenchmarkFrame';
import { Suite } from './views/Suite';
import { History } from './views/History';
import { RunDetail } from './views/RunDetail';
import { CompareView } from './views/Compare';
import { Settings } from './views/Settings';
import { MOD, useShortcuts } from './hooks/useShortcuts';
import { REGISTRY, WORKLOAD_ORDER, metaForPage } from './workloads/registry';
import type { BuildProfile, WorkloadKind } from './api';

/**
 * Above every view while either end of a run would be a debug build: ink on paper, so it reads
 * before the numbers do, and never redline (a debug build is a choice, not an error). An empty
 * div otherwise, so the host's grid keeps its rows.
 */
function BuildBar({
  here,
  target,
  targetName,
}: {
  here: BuildProfile | null;
  target: BuildProfile | null;
  targetName: string | null;
}) {
  const local = here === 'debug';
  const peer = target === 'debug';
  if (!local && !peer) return <div />;
  const who = local && peer ? `This device and ${targetName}` : local ? 'This device' : targetName;
  return (
    <div className="build-bar debug" role="status">
      <span className="label">Debug build</span>
      <span>
        {who} {local && peer ? 'run debug builds' : 'runs a debug build'}: throughput and CPU
        numbers aren’t representative and don’t compare with release builds.
      </span>
    </div>
  );
}

/** The places ⌘1…⌘9 go to, in rail order. */
const railOrder: Page[] = [
  'overview',
  'peers',
  'path',
  'tcp',
  'udp',
  'quic',
  'messaging',
  'file-transfer',
  'synced-store',
];

const TITLES: Record<Page, string> = {
  overview: 'Overview',
  peers: 'Peers',
  path: 'Path',
  tcp: 'TCP',
  udp: 'UDP',
  quic: 'QUIC',
  messaging: 'Messaging',
  'file-transfer': 'File transfer',
  'synced-store': 'SyncedStore',
  http: 'HTTP',
  video: 'Video',
  egress: 'Egress',
  planned: 'Planned',
  suite: 'Suite',
  history: 'History',
  settings: 'Settings',
};

export function App() {
  const store = useAppStore();
  const s = useAppState();
  const [route, go] = useRoute();
  const construction = useConstruction();
  const [railPref, setRailPref] = useState<'auto' | 'compact' | 'expanded'>(() =>
    load('rail/pref', 'auto'),
  );
  const readySent = useRef(false);

  // theme: the setting decides; "system" leaves it to prefers-color-scheme
  const theme = s.settings?.theme;
  useEffect(() => {
    const root = document.documentElement;
    if (theme === 'light' || theme === 'dark') root.setAttribute('data-theme', theme);
    else if (theme === 'system') root.removeAttribute('data-theme');
  }, [theme]);

  // the window is shown by Rust after the first themed paint; a window that was never shown is
  // hidden, and WebKit may not run requestAnimationFrame for it, so then say so at once
  useEffect(() => {
    if (!s.ready || readySent.current) return;
    readySent.current = true;
    const ready = () => void store.api.uiReady().catch(() => undefined);
    if (document.visibilityState === 'hidden') ready();
    else requestAnimationFrame(() => requestAnimationFrame(ready));
  }, [s.ready, store]);

  const signedIn = s.node.state === 'running';
  const navs = useMemo(() => railOrder.map((p) => () => go(p)), [go]);
  const toSettings = useCallback(() => go('settings'), [go]);
  useShortcuts(navs, toSettings, signedIn);

  // dev only: ?autorun=<page> starts a run of that view's workload once signed in (screenshots)
  const autorun = useRef(false);
  useEffect(() => {
    if (!import.meta.env.DEV || autorun.current || !signedIn) return;
    const page = new URLSearchParams(window.location.search).get('autorun');
    const m = page ? metaForPage(page as Page) : null;
    const target = store.target();
    if (!target || (!m && page !== 'suite')) return;
    autorun.current = true;
    if (m) void store.run(m.defaults, target);
    else void store.runSuite('smoke', target);
  }, [signedIn, s.peers, s.settings, store]);
  const title = signedIn ? `${TITLES[route.page]} · truffle bench` : 'truffle bench';
  useEffect(() => {
    document.title = title;
  }, [title]);

  const remote = s.remote[0];
  const measuredBy = remote
    ? `Being measured by ${remote.peer} · ${workloadName(remote.workload)}`
    : null;
  const banner = measuredBy ? (
    <span className="ts-banner">
      <i className="halfsq" aria-hidden="true" />
      {measuredBy}
    </span>
  ) : null;

  if (!s.ready) {
    return (
      <div className="app">
        <TitleStrip title="truffle bench" />
        <div className="splash">
          <Mark size={24} />
          <Loader label="Opening truffle bench" />
        </div>
      </div>
    );
  }

  if (!signedIn) {
    return (
      <div className={`app${construction ? ' construction' : ''}`}>
        <TitleStrip title={title} />
        <ConstructionContext.Provider value={construction}>
          <SignIn />
        </ConstructionContext.Provider>
        <Toasts toast={s.toast} />
        <span className="sr" aria-live="polite">
          {construction ? 'Construction shown' : ''}
        </span>
      </div>
    );
  }

  // the rail shrinks to icons in benchmark views, where the panes need the room (Home App.jsx:223)
  const compact = railPref === 'compact' || (railPref === 'auto' && !!metaForPage(route.page));

  // rail
  const busy = activeRun(s);
  const dotFor = (w: WorkloadKind): { dot: DotKind; label?: string } => {
    if (busy?.workload === w) return { dot: 'running', label: 'running' };
    const last = s.runs.find((r) => r.workload === w && r.tool !== 'imported');
    if (!last) return { dot: null };
    return last.status === 'ok'
      ? { dot: 'live', label: 'has a result' }
      : last.status === 'cancelled'
        ? { dot: 'cancelled', label: 'last run cancelled' }
        : { dot: 'failed', label: 'last run failed' };
  };
  const benchItems: RailEntry[] = WORKLOAD_ORDER.map((w) => {
    const m = REGISTRY[w];
    const d = dotFor(w);
    return { page: m.page, label: m.label, icon: m.icon, dot: d.dot, dotLabel: d.label };
  });
  benchItems.push({
    page: 'planned',
    label: 'Planned',
    icon: 'planned',
    dot: 'planned',
    dotLabel: 'planned',
  });
  const groups: { label?: string; items: RailEntry[] }[] = [
    {
      items: [
        { page: 'overview', label: 'Overview', icon: 'overview' },
        { page: 'peers', label: 'Peers', icon: 'peer' },
      ],
    },
    { label: 'Benchmarks', items: benchItems },
    {
      items: [
        { page: 'suite', label: 'Suite', icon: 'suite' },
        { page: 'history', label: 'History', icon: 'clock' },
        { page: 'settings', label: 'Settings', icon: 'settings', keys: [MOD, ','] },
      ],
    },
  ];
  // ⌘1…⌘9: the first nine places in the rail, in order
  railOrder.forEach((page, i) => {
    const it = groups.flatMap((g) => g.items).find((x) => x.page === page);
    if (it) it.keys = [MOD, String(i + 1)];
  });
  const local = s.node.local;
  const foot = (
    <RailFoot
      compact={compact}
      onToggle={() => {
        const next = compact ? 'expanded' : 'compact';
        setRailPref(next);
        save('rail/pref', next);
      }}
      square={
        <i
          className={measuredBy ? 'halfsq' : 'fullsq'}
          aria-label={measuredBy ?? 'Connected'}
          role="img"
        />
      }
    >
      {measuredBy ? (
        <span className="rf-line rf-measured">
          <i className="halfsq" aria-hidden="true" />
          {measuredBy}
        </span>
      ) : (
        <span className="rf-line">
          <i className="fullsq" aria-hidden="true" />
          Connected
        </span>
      )}
      <span className="rf-line mono muted">{local?.tailnet ?? local?.dns_name ?? 'tailnet'}</span>
    </RailFoot>
  );

  const meta = metaForPage(route.page);
  let view: ReactNode;
  let tool = false;
  if (meta) {
    tool = true;
    view = <BenchmarkFrame key={meta.id} meta={meta} go={go} />;
  } else {
    switch (route.page) {
      case 'peers':
        view = <Peers />;
        break;
      case 'planned':
        view = <Planned />;
        break;
      case 'suite':
        view = <Suite go={go} />;
        break;
      case 'history': {
        const [first, a, b] = route.id.split('/');
        if (first === 'compare' && a && b)
          view = <CompareView key={route.id} a={a} b={b} go={go} />;
        else if (route.id) view = <RunDetail key={route.id} id={route.id} go={go} />;
        else view = <History go={go} />;
        break;
      }
      case 'settings':
        view = <Settings />;
        break;
      default:
        view = <Overview go={go} />;
    }
  }

  return (
    <div className={`app${compact ? ' rail-compact' : ''}${construction ? ' construction' : ''}`}>
      <TitleStrip title={title} banner={banner} />
      <ConstructionContext.Provider value={construction}>
        <div className="app-body">
          <Rail route={route} compact={compact} groups={groups} foot={foot} />
          <main className={`host${tool ? '' : ' scroll'}`}>
            <BuildBar
              here={s.appInfo?.build_profile ?? null}
              target={store.target()?.hello?.build_profile ?? null}
              targetName={store.target()?.device_name ?? null}
            />
            <div className="host-view">{view}</div>
          </main>
        </div>
      </ConstructionContext.Provider>
      <Toasts toast={s.toast} />
      <span className="sr" aria-live="polite">
        {construction ? 'Construction shown' : ''}
      </span>
    </div>
  );
}
