// App state: the hub's node state and peers (from `bench://event`), settings, the run history,
// and live runs. Runs execute in Rust; this keeps what the UI has seen of them, reattaches on
// load (`runs_active` + `run_attach`), and treats the returned RunReport as authoritative.

import { useSyncExternalStore } from 'react';
import {
  KEY_LOADED_RTT,
  PROTOCOL,
  toAppError,
  type ActiveRun,
  type Api,
  type AppError,
  type AppInfo,
  type BenchPeer,
  type HubEvent,
  type ImportResult,
  type NodeState,
  type Phase,
  type Progress,
  type RunReport,
  type RunSummary,
  type Settings,
  type SuiteProfile,
  type SuiteProgress,
  type SuiteResult,
  type WorkloadKind,
  type WorkloadParams,
} from '../api';
import { REGISTRY } from '../workloads/registry';

export interface LiveRun {
  run_id: string;
  suite_id: string | null;
  workload: WorkloadKind;
  /** null for a run reattached after a reload until its report (unless a suite item names it). */
  params: WorkloadParams | null;
  peer_name: string;
  /** The peer's tailscale id, for runs this window started; reattached runs go by peer_name. */
  peer_id: string | null;
  started_at: number;
  phase: Phase;
  /** The phase before failed/cancelled, for "failed while …". */
  last_active_phase: Phase | null;
  fraction: number | null;
  message: string | null;
  t_ms: number;
  /** t_ms of the first warmup/running Progress: series are relative to it. */
  data_start: number | null;
  /** Reattached after a reload: samples before the attach only arrive with the report. */
  attached: boolean;
  /** key → [t_ms since data start, value] */
  series: Record<string, [number, number][]>;
  /**
   * The path samples' series key → the peer's connection when each sample arrived, in step with
   * `series[key]`, so a path change mid-run doesn't recolour the samples before it.
   */
  conn: Record<string, (string | null)[]>;
  last: Record<string, number>;
  report: RunReport | null;
  error: AppError | null;
}

export interface RemoteRun {
  run_id: string;
  peer: string;
  peer_id: string;
  workload: string;
}

export interface SuiteState {
  suite_id: string;
  /** The profile id; null only if the engine didn't say (`ActiveRun.suite_profile`). */
  profile: string | null;
  started_at: number;
  progress: SuiteProgress | null;
  /** item index → run_id, from SuiteProgress */
  items: Record<number, string>;
  result: SuiteResult | null;
  error: AppError | null;
  /**
   * Reattached after a reload. suite_run's channel is gone, so the suite is followed through
   * runs_active (polled; each suite item names its profile, index and total) and run_attach, and
   * its items through runs_list.
   */
  reattached: boolean;
}

export interface State {
  ready: boolean;
  apiKind: 'tauri' | 'mock';
  appInfo: AppInfo | null;
  node: NodeState;
  peers: BenchPeer[];
  settings: Settings | null;
  remote: RemoteRun[];
  runs: RunSummary[];
  /** run_id → live run (current and finished in this session) */
  live: Record<string, LiveRun>;
  /** The latest run started from this window per workload. */
  current: Partial<Record<WorkloadKind, string>>;
  reports: Record<string, RunReport>;
  suite: SuiteState | null;
  /**
   * After a load: looking again for a suite that may be in its 1 s pause between items, when
   * runs_active showed none but the newest stored run is a suite item. Runs wait meanwhile.
   */
  suiteCheck: boolean;
  toast: { id: number; text: string; kind: 'info' | 'error' } | null;
}

type Listener = () => void;

const IDLE: NodeState = {
  state: 'idle',
  auth_url: null,
  error: null,
  local: null,
  tailscale_hostname: null,
  since_ms: Date.now(),
};

/** Reports kept in memory (by last use); views hold the ones they show. */
export const REPORT_CACHE = 24;

const isTerminal = (p: Phase) => p === 'done' || p === 'failed' || p === 'cancelled';

const sleep = (ms: number) => new Promise<void>((r) => window.setTimeout(r, ms));

function runId(): string {
  const c = globalThis.crypto;
  if (c?.randomUUID) return c.randomUUID();
  return `run-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`;
}

export class Store {
  private state: State;
  private listeners = new Set<Listener>();
  private toastTimer = 0;
  /** False while `state.node` is still the IDLE placeholder (its since_ms is this clock's). */
  private nodeKnown = false;
  /** Report ids in `state.reports`, least recently used first. */
  private reportOrder: string[] = [];
  /** Settings saves, one at a time (each patches what Rust holds). */
  private saving: Promise<void> = Promise.resolve();
  private profiles: Promise<SuiteProfile[]> | null = null;

  constructor(readonly api: Api) {
    this.state = {
      ready: false,
      apiKind: api.kind,
      appInfo: null,
      node: IDLE,
      peers: [],
      settings: null,
      remote: [],
      runs: [],
      live: {},
      current: {},
      reports: {},
      suite: null,
      suiteCheck: false,
      toast: null,
    };
  }

  get = (): State => this.state;

  subscribe = (l: Listener): (() => void) => {
    this.listeners.add(l);
    return () => this.listeners.delete(l);
  };

  private set(patch: Partial<State> | ((s: State) => Partial<State>)) {
    const p = typeof patch === 'function' ? patch(this.state) : patch;
    this.state = { ...this.state, ...p };
    this.listeners.forEach((l) => l());
  }

  // ---------------------------------------------------------------- start-up

  async init(): Promise<void> {
    // listen first, then read the snapshot: nothing emitted in between is lost, and the snapshot
    // can't overwrite a newer event (applyNode orders them by since_ms)
    // (bounded: a registration that never settles must not keep the window on its splash)
    await Promise.race([this.api.onEvent((e) => this.onEvent(e)).ready, sleep(2000)]);
    const [node, settings] = await Promise.all([
      this.api.hubState().catch(() => null),
      this.api.settingsGet().catch(() => null),
    ]);
    this.set({ settings });
    if (node) this.applyNode(node, false);
    this.set({ ready: true });
    // app_info hashes the sidecar: it must not hold up the node state
    void this.api.appInfo().then(
      (appInfo) => this.set({ appInfo }),
      () => undefined,
    );
  }

  /**
   * Every NodeState goes through here: an event, a snapshot (`hub_state`) or a command's reply.
   * An older state never replaces a newer one: events win ties, snapshots and replies must be
   * strictly newer. Entering `running` loads peers, runs and active runs, once per transition.
   */
  private applyNode(node: NodeState, fromEvent: boolean) {
    const cur = this.state.node;
    if (
      this.nodeKnown &&
      (fromEvent ? node.since_ms < cur.since_ms : node.since_ms <= cur.since_ms)
    )
      return;
    this.nodeKnown = true;
    const was = cur.state;
    this.set({ node });
    if (node.state === 'running' && was !== 'running') void this.onRunning();
    if (node.state !== 'running' && was === 'running') this.set({ peers: [], remote: [] });
  }

  private async onRunning() {
    const [peers, runs, active] = await Promise.all([
      this.api.peersList().catch(() => [] as BenchPeer[]),
      this.api.runsList().catch(() => [] as RunSummary[]),
      this.api.runsActive().catch(() => [] as ActiveRun[]),
    ]);
    this.set({ peers, runs });
    this.autoTarget(peers);
    for (const a of active) this.attach(a);
    const suiteRun = active.find((a) => a.suite_id);
    if (suiteRun && !this.state.suite) this.reattachSuite(suiteRun);
    else if (!suiteRun && !this.state.suite && runs[0]?.suite_id) void this.lookForSuite();
  }

  /**
   * A suite pauses 1 s between items with nothing in runs_active, so a load in that pause sees no
   * suite. When the newest stored run is a suite item, look again for a moment (Run waits) before
   * deciding none is running.
   */
  private async lookForSuite() {
    this.set({ suiteCheck: true });
    try {
      for (let i = 0; i < 3 && !this.state.suite; i++) {
        await sleep(500);
        if (this.state.node.state !== 'running') return;
        const active = await this.api.runsActive().catch(() => [] as ActiveRun[]);
        const item = active.find((a) => a.suite_id);
        if (!item) continue;
        for (const a of active) this.attach(a);
        if (!this.state.suite) this.reattachSuite(item);
      }
    } finally {
      this.set({ suiteCheck: false });
    }
  }

  private onEvent(e: HubEvent) {
    if (e.type === 'node') {
      const { type: _t, ...node } = e;
      this.applyNode(node, true);
      return;
    }
    if (e.type === 'peers') {
      this.set({ peers: e.peers });
      this.autoTarget(e.peers);
      return;
    }
    if (e.type === 'remote-run') {
      if (e.state === 'prepared') {
        this.set((s) => ({
          remote: [
            ...s.remote.filter((r) => r.run_id !== e.run_id),
            { run_id: e.run_id, peer: e.peer, peer_id: e.peer_id, workload: e.workload },
          ],
        }));
      } else {
        this.set((s) => ({ remote: s.remote.filter((r) => r.run_id !== e.run_id) }));
      }
    }
  }

  // ---------------------------------------------------------------- hub

  async signIn(deviceName: string, authKey?: string): Promise<void> {
    try {
      const node = await this.api.hubStart({
        device_name: deviceName.trim() || undefined,
        auth_key: authKey?.trim() || undefined,
      });
      this.applyNode(node, false);
    } catch (e) {
      this.applyNode(
        { ...this.state.node, state: 'error', error: toAppError(e), since_ms: Date.now() },
        true,
      );
    }
    // hub_start saves the device name in Rust: take its settings, so no later save reverts it
    const settings = await this.api.settingsGet().catch(() => null);
    if (settings) this.set({ settings });
  }

  async stopHub(): Promise<void> {
    const node = await this.api.hubStop().catch(() => this.state.node);
    this.applyNode(node, false);
  }

  async signOut(): Promise<void> {
    const node = await this.api.hubSignOut().catch((e) => {
      this.toast(toAppError(e).message, 'error');
      return this.state.node;
    });
    this.applyNode(node, false);
    this.set({ peers: [] });
  }

  async openDataDir(): Promise<void> {
    try {
      await this.api.openDataDir();
    } catch (e) {
      this.toast(toAppError(e).message, 'error');
    }
  }

  async openUrl(url: string): Promise<void> {
    try {
      await this.api.openUrl(url);
    } catch (e) {
      this.toast(toAppError(e).message, 'error');
    }
  }

  // ---------------------------------------------------------------- settings and target

  /**
   * Change some settings. Shown at once; saved one at a time, each as a patch on the settings
   * Rust holds (they change there too: hub_start saves the device name), never as this window's
   * possibly stale copy.
   */
  saveSettings(patch: Partial<Settings>): Promise<void> {
    const cur = this.state.settings;
    if (!cur) return Promise.resolve();
    this.set({ settings: { ...cur, ...patch } });
    const run = this.saving.then(async () => {
      try {
        const base = await this.api.settingsGet().catch(() => this.state.settings ?? cur);
        const saved = await this.api.settingsSet({ ...base, ...patch });
        this.set({ settings: saved });
      } catch (e) {
        this.toast(toAppError(e).message, 'error');
        const back = await this.api.settingsGet().catch(() => null);
        if (back) this.set({ settings: back });
      }
    });
    this.saving = run;
    return run;
  }

  /** The remembered target, by device_id, else tailscale_id. */
  target(): BenchPeer | null {
    const ref = this.state.settings?.target_peer;
    if (!ref) return null;
    return this.state.peers.find((p) => p.device_id === ref || p.id === ref) ?? null;
  }

  setTarget(peer: BenchPeer) {
    void this.saveSettings({ target_peer: peer.device_id ?? peer.id });
  }

  /** When exactly one same-account peer is online and no target is remembered, it becomes the target. */
  private autoTarget(peers: BenchPeer[]) {
    const s = this.state.settings;
    if (!s || s.target_peer) return;
    const candidates = peers.filter((p) => p.online && p.same_login && p.hello);
    if (candidates.length === 1) this.setTarget(candidates[0]);
  }

  async probe(peer: BenchPeer): Promise<void> {
    try {
      const r = await this.api.peerProbe(peer.id);
      this.set((s) => ({
        peers: s.peers.map((p) => (p.id === peer.id ? { ...p, ...r.peer } : p)),
      }));
      const rtt =
        r.path.rtt_ms == null
          ? 'no answer'
          : `${r.path.rtt_ms.toFixed(r.path.rtt_ms < 10 ? 2 : 1)} ms`;
      this.toast(`${peer.device_name}: ${r.path.connection ?? 'unknown path'}, ${rtt}`);
    } catch (e) {
      this.toast(toAppError(e).message, 'error');
    }
  }

  // ---------------------------------------------------------------- runs

  private applyProgress(p: Progress) {
    this.set((s) => {
      const cur = s.live[p.run_id];
      if (!cur || cur.report) return {}; // the report is authoritative; later Progress is ignored
      const inData = p.phase === 'warmup' || p.phase === 'running';
      const data_start = cur.data_start ?? (inData ? p.t_ms - 250 : null);
      const series = { ...cur.series };
      const last = { ...cur.last };
      const conn = { ...cur.conn };
      const pathKey = cur.workload === 'path' ? 'rtt_ms' : KEY_LOADED_RTT;
      if (data_start != null) {
        for (const smp of p.samples) {
          const t = p.t_ms - data_start;
          series[smp.key] = [...(series[smp.key] ?? []), [t, smp.value]];
          last[smp.key] = smp.value;
          if (smp.key === pathKey) conn[smp.key] = [...(conn[smp.key] ?? []), connectionOf(s, cur)];
        }
      } else {
        for (const smp of p.samples) last[smp.key] = smp.value;
      }
      const terminal = p.phase === 'done' || p.phase === 'failed' || p.phase === 'cancelled';
      const next: LiveRun = {
        ...cur,
        phase: p.phase,
        last_active_phase: terminal ? cur.phase : cur.last_active_phase,
        fraction: p.fraction,
        message: p.message,
        t_ms: p.t_ms,
        data_start,
        series,
        conn,
        last,
      };
      return { live: { ...s.live, [p.run_id]: next } };
    });
  }

  private attach(a: ActiveRun) {
    const have = this.state.live[a.run_id];
    if (have && !have.report && !have.error && !isTerminal(have.phase)) return; // followed already
    const workload = a.workload as WorkloadKind;
    const run: LiveRun = {
      run_id: a.run_id,
      suite_id: a.suite_id,
      workload,
      params: null,
      peer_name: a.peer_name,
      peer_id: null,
      started_at: new Date(a.started_at).getTime(),
      phase: a.last?.phase ?? 'preparing',
      last_active_phase: null,
      fraction: a.last?.fraction ?? null,
      message: null,
      t_ms: a.last?.t_ms ?? 0,
      // the data phase began before we could see it: plot against run time instead
      data_start: a.last && (a.last.phase === 'warmup' || a.last.phase === 'running') ? 0 : null,
      attached: true,
      series: {},
      conn: {},
      last: {},
      report: null,
      error: null,
    };
    this.set((s) => ({
      live: { ...s.live, [a.run_id]: run },
      current: { ...s.current, [workload]: a.run_id },
    }));
    // a suite item's params are its profile's item (the app runs profiles without overrides)
    void this.suiteItemParams(a).then((params) => {
      if (params)
        this.set((s) => {
          const r = s.live[a.run_id];
          return r && !r.params ? { live: { ...s.live, [a.run_id]: { ...r, params } } } : {};
        });
    });
    void this.api
      .runAttach(a.run_id, (p) => this.onAttachedProgress(p))
      .then(
        (ok) => {
          if (!ok) void this.finishAttached(a.run_id);
        },
        () => void this.finishAttached(a.run_id),
      );
  }

  private async suiteItemParams(a: ActiveRun): Promise<WorkloadParams | null> {
    if (a.suite_profile == null || a.suite_index == null) return null;
    if (!this.profiles)
      this.profiles = this.api.suiteProfiles().catch(() => {
        this.profiles = null;
        return [] as SuiteProfile[];
      });
    const item = (await this.profiles).find((p) => p.id === a.suite_profile)?.items[a.suite_index];
    return item && item.workload === a.workload ? item : null;
  }

  private onAttachedProgress(p: Progress) {
    this.applyProgress(p);
    if (p.phase === 'done' || p.phase === 'failed' || p.phase === 'cancelled')
      void this.finishAttached(p.run_id);
  }

  private async finishAttached(id: string) {
    try {
      const report = await this.api.runGet(id);
      this.completeRun(id, report);
    } catch {
      // no report to show: stop counting the run as in progress (it would block Run)
      this.set((s) => {
        const cur = s.live[id];
        if (!cur || cur.report || cur.error) return {};
        const error: AppError = {
          code: 'internal',
          message: 'This window lost track of the run. Its report appears in History when it ends.',
        };
        return {
          live: {
            ...s.live,
            [id]: { ...cur, error, phase: 'failed', last_active_phase: cur.phase },
          },
        };
      });
    }
  }

  private completeRun(id: string, report: RunReport) {
    this.set((s) => {
      const cur = s.live[id];
      const live: Record<string, LiveRun> = {};
      // finished runs that no view shows any more go; the report replaces a run's samples
      const shown = new Set(Object.values(s.current));
      for (const [k, r] of Object.entries(s.live)) {
        const done = !!r.report || !!r.error || isTerminal(r.phase);
        if (k !== id && done && !shown.has(k)) continue;
        live[k] = r;
      }
      if (cur)
        live[id] = {
          ...cur,
          report,
          phase: (report.status === 'ok' ? 'done' : report.status) as Phase,
          params: report.params,
          series: {},
          conn: {},
        };
      return { live, reports: this.cacheReport(s, id, report) };
    });
    void this.refreshRuns();
  }

  /** `reports` with `id` added as the most recently used, past REPORT_CACHE least used dropped. */
  private cacheReport(s: State, id: string, r: RunReport): Record<string, RunReport> {
    this.reportOrder = [...this.reportOrder.filter((x) => x !== id), id];
    const reports = { ...s.reports, [id]: r };
    const keep = new Set(Object.values(s.current));
    while (this.reportOrder.length > REPORT_CACHE) {
      const i = this.reportOrder.findIndex((x) => !keep.has(x));
      if (i < 0) break;
      const [old] = this.reportOrder.splice(i, 1);
      delete reports[old];
    }
    return reports;
  }

  async run(params: WorkloadParams, peer: BenchPeer): Promise<void> {
    const id = runId();
    const run: LiveRun = {
      run_id: id,
      suite_id: null,
      workload: params.workload,
      params,
      peer_name: peer.device_name,
      peer_id: peer.id,
      started_at: Date.now(),
      phase: 'preparing',
      last_active_phase: null,
      fraction: 0,
      message: null,
      t_ms: 0,
      data_start: null,
      attached: false,
      series: {},
      conn: {},
      last: {},
      report: null,
      error: null,
    };
    this.set((s) => ({
      live: { ...s.live, [id]: run },
      current: { ...s.current, [params.workload]: id },
    }));
    try {
      const report = await this.api.benchRun({ run_id: id, peer: peer.id, params }, (p) =>
        this.applyProgress(p),
      );
      this.completeRun(id, report);
    } catch (e) {
      const err = toAppError(e);
      this.set((s) => {
        const cur = s.live[id];
        return cur
          ? {
              live: {
                ...s.live,
                [id]: { ...cur, error: err, phase: 'failed', last_active_phase: cur.phase },
              },
            }
          : {};
      });
    }
  }

  async cancel(id: string): Promise<void> {
    try {
      await this.api.benchCancel(id);
    } catch (e) {
      this.toast(toAppError(e).message, 'error');
    }
  }

  async runSuite(profile: string, peer: BenchPeer): Promise<void> {
    const suite_id = runId();
    const started_at = Date.now();
    this.set({
      suite: {
        suite_id,
        profile,
        started_at,
        progress: null,
        items: {},
        result: null,
        error: null,
        reattached: false,
      },
    });
    try {
      const result = await this.api.suiteRun({ suite_id, profile, peer: peer.id }, (p) => {
        this.set((s) =>
          s.suite?.suite_id === suite_id
            ? {
                suite: {
                  ...s.suite,
                  progress: p,
                  items: { ...s.suite.items, [p.index]: p.run.run_id },
                },
              }
            : {},
        );
        if (p.run.phase === 'done' || p.run.phase === 'failed' || p.run.phase === 'cancelled')
          void this.refreshRuns();
      });
      this.set((s) => (s.suite?.suite_id === suite_id ? { suite: { ...s.suite, result } } : {}));
      this.toast(
        result.cancelled
          ? 'Suite cancelled.'
          : result.failed
            ? `Suite finished: ${result.failed} of ${result.run_ids.length} runs failed.`
            : `Suite finished: ${result.run_ids.length} runs.`,
        result.failed ? 'error' : 'info',
      );
    } catch (e) {
      const err = toAppError(e);
      this.set((s) =>
        s.suite?.suite_id === suite_id ? { suite: { ...s.suite, error: err } } : {},
      );
      this.toast(err.message, 'error');
    }
    void this.refreshRuns();
  }

  /** A suite that was running before a reload: follow it through runs_active until it ends. */
  private reattachSuite(first: ActiveRun) {
    const suite_id = first.suite_id as string;
    const earlier = this.state.runs
      .filter((r) => r.suite_id === suite_id)
      .map((r) => new Date(r.started_at).getTime());
    this.set({
      suite: {
        suite_id,
        profile: first.suite_profile,
        started_at: Math.min(new Date(first.started_at).getTime(), ...earlier),
        progress: suiteProgressOf(first),
        items: {},
        result: null,
        error: null,
        reattached: true,
      },
    });
    let idle = 0;
    const seen = new Set<string>(
      Object.values(this.state.live)
        .filter((r) => r.suite_id === suite_id)
        .map((r) => r.run_id),
    );
    const tick = async () => {
      if (this.state.suite?.suite_id !== suite_id || this.state.suite.result) return;
      const active = await this.api.runsActive().catch(() => [] as ActiveRun[]);
      const mine = active.filter((a) => a.suite_id === suite_id);
      for (const a of mine) {
        if (!seen.has(a.run_id)) {
          seen.add(a.run_id);
          void this.refreshRuns();
          this.attach(a);
        }
        const progress = suiteProgressOf(a);
        this.set((s) =>
          s.suite?.suite_id === suite_id && progress
            ? {
                suite: {
                  ...s.suite,
                  progress,
                  items: { ...s.suite.items, [progress.index]: a.run_id },
                },
              }
            : {},
        );
      }
      // the suite pauses 1 s between items: two empty polls in a row means it ended
      idle = mine.length ? 0 : idle + 1;
      if (idle >= 2) {
        await this.refreshRuns();
        const runs = this.state.runs.filter((r) => r.suite_id === suite_id);
        this.set((s) =>
          s.suite?.suite_id === suite_id
            ? {
                suite: {
                  ...s.suite,
                  result: {
                    suite_id,
                    run_ids: runs.map((r) => r.run_id),
                    failed: runs.filter((r) => r.status === 'failed').length,
                    cancelled: runs.some((r) => r.status === 'cancelled'),
                  },
                },
              }
            : {},
        );
        return;
      }
      window.setTimeout(() => void tick(), 1500);
    };
    window.setTimeout(() => void tick(), 1500);
  }

  /** Import report files, folders or zips (a drop, or paths from elsewhere). */
  async importPaths(paths: string[]): Promise<ImportResult | null> {
    try {
      const r = await this.api.runsImport(paths);
      this.toastImport(r);
      await this.refreshRuns();
      return r;
    } catch (e) {
      this.toast(toAppError(e).message, 'error');
      return null;
    }
  }

  /** The native picker, in Rust. */
  async importPick(): Promise<ImportResult | null> {
    try {
      const r = await this.api.runsImportPick();
      if (r.imported || r.skipped) this.toastImport(r);
      await this.refreshRuns();
      return r;
    } catch (e) {
      this.toast(toAppError(e).message, 'error');
      return null;
    }
  }

  private toastImport(r: ImportResult) {
    const imported = `${r.imported} report${r.imported === 1 ? '' : 's'}`;
    this.toast(
      r.skipped
        ? `Imported ${imported}; skipped ${r.skipped} that weren’t reports or were already here.`
        : `Imported ${imported}.`,
    );
  }

  async deleteRuns(ids: string[]): Promise<void> {
    let done = 0;
    for (const id of ids) {
      try {
        await this.api.runDelete(id);
        done += 1;
      } catch (e) {
        this.toast(toAppError(e).message, 'error');
      }
    }
    this.reportOrder = this.reportOrder.filter((x) => !ids.includes(x));
    this.set((s) => {
      const reports = { ...s.reports };
      for (const id of ids) delete reports[id];
      return { reports };
    });
    await this.refreshRuns();
    if (done) this.toast(done === 1 ? 'Deleted the run.' : `Deleted ${done} runs.`);
  }

  async refreshRuns(): Promise<void> {
    const runs = await this.api.runsList().catch(() => this.state.runs);
    this.set({ runs });
  }

  /** A stored report, cached (the REPORT_CACHE most recently used). */
  async report(id: string): Promise<RunReport | null> {
    const have = this.state.reports[id];
    if (have) {
      this.reportOrder = [...this.reportOrder.filter((x) => x !== id), id];
      return have;
    }
    try {
      const r = await this.api.runGet(id);
      this.set((s) => ({ reports: this.cacheReport(s, id, r) }));
      return r;
    } catch {
      return null;
    }
  }

  // ---------------------------------------------------------------- toasts

  toast(text: string, kind: 'info' | 'error' = 'info') {
    window.clearTimeout(this.toastTimer);
    this.set({ toast: { id: Date.now(), text, kind } });
    this.toastTimer = window.setTimeout(
      () => this.set({ toast: null }),
      kind === 'error' ? 6000 : 3200,
    );
  }
}

/** The run's peer's connection now: by tailscale id when known, else by device name. */
function connectionOf(s: State, run: LiveRun): string | null {
  const peer = run.peer_id
    ? s.peers.find((p) => p.id === run.peer_id)
    : s.peers.find((p) => p.device_name === run.peer_name);
  return peer?.connection ?? null;
}

/** A suite item's place in its suite, from runs_active (what suite_run's channel would say). */
function suiteProgressOf(a: ActiveRun): SuiteProgress | null {
  if (a.suite_id == null || a.suite_index == null || a.suite_total == null) return null;
  return {
    suite_id: a.suite_id,
    index: a.suite_index,
    total: a.suite_total,
    item_label: workloadName(a.workload),
    run: a.last ?? {
      run_id: a.run_id,
      suite_id: a.suite_id,
      t_ms: 0,
      phase: 'preparing',
      fraction: null,
      samples: [],
      message: null,
    },
  };
}

// ---------------------------------------------------------------- selectors

export function activeRun(s: State): LiveRun | null {
  return (
    Object.values(s.live).find(
      (r) => !r.report && !r.error && !['done', 'failed', 'cancelled'].includes(r.phase),
    ) ?? null
  );
}

/** Why a run of `workload` against `target` cannot start now, or null (RFC 027 §11.3). */
export function runBlockReason(
  s: State,
  target: BenchPeer | null,
  workload: WorkloadKind,
): string | null {
  if (s.node.state !== 'running') return 'Sign in to Tailscale first.';
  const remote = s.remote[0];
  if (remote) return `Being measured by ${remote.peer} · ${workloadName(remote.workload)}`;
  const busy = activeRun(s);
  if (busy) return `A ${workloadName(busy.workload)} run is in progress.`;
  if (s.suite && !s.suite.result && !s.suite.error) return 'A suite is running.';
  if (s.suiteCheck) return 'Checking whether a suite is still running…';
  if (!target) return 'Choose a target on the Peers page.';
  if (!target.online) return `${target.device_name} is offline.`;
  if (target.hello_error)
    return `${target.device_name} didn’t answer: ${target.hello_error.message}`;
  const h = target.hello;
  if (!h) return `Waiting for ${target.device_name} to answer.`;
  if (h.protocol !== PROTOCOL)
    return `${target.device_name} speaks protocol ${h.protocol}; this app speaks ${PROTOCOL}. Update one of them.`;
  if (!h.workloads.includes(workload))
    return `${target.device_name} can’t run ${workloadName(workload)}.`;
  if (!h.accepts_runs_from_you)
    return `${target.device_name} doesn’t accept runs from your account.`;
  if (h.busy) return `${target.device_name} is busy: ${h.busy}.`;
  return null;
}

export function workloadName(w: string): string {
  return REGISTRY[w as WorkloadKind]?.label ?? w;
}

export function useStore<T>(store: Store, select: (s: State) => T): T {
  return useSyncExternalStore(store.subscribe, () => select(store.get()));
}
