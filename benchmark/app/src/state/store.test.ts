// The store against a fake Api: the orderings and failures a real hub produces that the mock
// doesn't (a sign-in that renames the device, events racing the launch snapshot, a reload in a
// suite's pause, an attach that fails), and the bounds on what it keeps in memory.
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { Api, Subscription } from '../api/api';
import type {
  ActiveRun,
  BenchPeer,
  HubEvent,
  NodeState,
  Progress,
  RunReport,
  RunSummary,
  Settings,
  SuiteProfile,
  WorkloadParams,
} from '../api/types';
import benchPeer from '../../../schema/fixtures/bench-peer.json';
import nodeRunning from '../../../schema/fixtures/node-state-running.json';
import reportTcp from '../../../schema/fixtures/report-tcp-ok.json';
import { REGISTRY } from '../workloads/registry';
import { REPORT_CACHE, Store, activeRun, runBlockReason } from './store';

const node = (state: NodeState['state'], since_ms: number): NodeState =>
  ({ ...(nodeRunning as unknown as NodeState), state, since_ms }) as NodeState;

const peer = (over: Partial<BenchPeer> = {}): BenchPeer =>
  ({
    ...(benchPeer as unknown as BenchPeer),
    online: true,
    same_login: true,
    ...over,
  }) as BenchPeer;

/** A fake Rust side: settings it owns, events it emits, and overridable commands. */
function fakeApi(over: Partial<Api> = {}) {
  let rust: Settings = {
    device_name: 'default-name',
    allow_other_logins: false,
    theme: 'system',
    target_peer: null,
  };
  const handlers = new Set<(e: HubEvent) => void>();
  const set = vi.fn(async (s: Settings) => {
    rust = { ...s };
    return { ...rust };
  });
  const api: Api = {
    kind: 'mock',
    appInfo: vi.fn(() => new Promise<never>(() => undefined)),
    uiReady: async () => undefined,
    hubState: async () => node('idle', 1),
    hubStart: async (req) => {
      if (req.device_name) rust = { ...rust, device_name: req.device_name };
      return node('starting', 100);
    },
    hubStop: async () => node('stopped', 900),
    hubSignOut: async () => node('idle', 900),
    openUrl: async () => undefined,
    openDataDir: async () => undefined,
    peersList: vi.fn(async () => [] as BenchPeer[]),
    peerProbe: () => Promise.reject(new Error('unused')),
    benchRun: () => new Promise<never>(() => undefined),
    benchCancel: async () => true,
    runsActive: vi.fn(async () => [] as ActiveRun[]),
    runAttach: async () => true,
    suiteProfiles: async () => [] as SuiteProfile[],
    suiteRun: () => Promise.reject(new Error('unused')),
    workloadDefaults: async () => [],
    runsList: async () => [] as RunSummary[],
    runGet: (id) => Promise.reject({ code: 'io', message: `no ${id}` }),
    runDelete: async () => undefined,
    runsImport: async () => ({ imported: 0, skipped: 0 }),
    runsImportPick: async () => ({ imported: 0, skipped: 0 }),
    settingsGet: async () => ({ ...rust }),
    settingsSet: set,
    onEvent: (h): Subscription => {
      handlers.add(h);
      return Object.assign(() => void handlers.delete(h), { ready: Promise.resolve() });
    },
    onFileDrop: () => () => undefined,
    ...over,
  };
  return {
    api,
    emit: (e: HubEvent) => handlers.forEach((h) => h(e)),
    settingsSet: set,
    rust: () => rust,
  };
}

const flush = () => vi.advanceTimersByTimeAsync(0);

describe('store', () => {
  beforeEach(() => {
    (globalThis as unknown as { window: unknown }).window = globalThis;
    vi.useFakeTimers();
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it('keeps the device name typed at sign-in when a later save patches the settings', async () => {
    const f = fakeApi();
    const store = new Store(f.api);
    await store.init();
    await store.signIn('studio');
    expect(store.get().settings?.device_name).toBe('studio');
    // the hub runs and exactly one same-account peer answers: it becomes the target
    f.emit({ type: 'node', ...node('running', 200) });
    await flush();
    f.emit({ type: 'peers', peers: [peer()] });
    await flush();
    expect(f.settingsSet).toHaveBeenCalled();
    for (const [s] of f.settingsSet.mock.calls) expect(s.device_name).toBe('studio');
    expect(f.rust().device_name).toBe('studio');
    expect(f.rust().target_peer).not.toBeNull();
    expect(store.get().settings?.device_name).toBe('studio');
  });

  it('saves settings one at a time, each a patch on what Rust holds', async () => {
    const f = fakeApi();
    const store = new Store(f.api);
    await store.init();
    const a = store.saveSettings({ theme: 'dark' });
    const b = store.saveSettings({ allow_other_logins: true });
    await Promise.all([a, b]);
    expect(f.rust()).toMatchObject({ theme: 'dark', allow_other_logins: true });
    expect(store.get().settings).toMatchObject({ theme: 'dark', allow_other_logins: true });
  });

  it('never lets the launch snapshot replace a newer node event', async () => {
    let answer: (n: NodeState) => void = () => undefined;
    const f = fakeApi({
      hubState: () => new Promise<NodeState>((r) => (answer = r)),
    });
    const store = new Store(f.api);
    const init = store.init();
    await flush();
    // the hub reaches running while hub_state's older answer is still on its way
    f.emit({ type: 'node', ...node('running', 200) });
    answer(node('starting', 150));
    await init;
    await flush();
    expect(store.get().node.state).toBe('running');
    // and entering running loaded peers and runs once
    expect(f.api.peersList).toHaveBeenCalledTimes(1);
    // a reply as old as what is shown changes nothing; a newer event still applies
    f.emit({ type: 'node', ...node('stopping', 300) });
    expect(store.get().node.state).toBe('stopping');
  });

  it('draws a reattached suite item with its profile item’s params', async () => {
    const item = { ...REGISTRY.tcp.defaults, mode: 'latency', payload_bytes: 64 } as WorkloadParams;
    const active: ActiveRun = {
      run_id: 'r-suite',
      suite_id: 's1',
      workload: 'tcp',
      peer_name: 'studio',
      started_at: new Date().toISOString(),
      last: null,
      suite_profile: 'full',
      suite_index: 1,
      suite_total: 3,
    };
    const single: ActiveRun = {
      ...active,
      run_id: 'r-single',
      workload: 'udp',
      suite_id: null,
      suite_profile: null,
      suite_index: null,
      suite_total: null,
    };
    const f = fakeApi({
      hubState: async () => node('running', 200),
      runsActive: vi.fn(async () => [active, single]),
      suiteProfiles: async () => [
        { id: 'full', label: 'Full', description: '', items: [REGISTRY.path.defaults, item] },
      ],
    });
    const store = new Store(f.api);
    await store.init();
    await flush();
    expect(store.get().live['r-suite'].params).toEqual(item);
    // a run that isn't a suite item has no params until its report: never the form's defaults
    expect(store.get().live['r-single'].params).toBeNull();
    expect(store.get().suite?.suite_id).toBe('s1');
  });

  it('finds a suite after a reload in its pause between items', async () => {
    let calls = 0;
    const item: ActiveRun = {
      run_id: 'r2',
      suite_id: 's1',
      workload: 'tcp',
      peer_name: 'studio',
      started_at: new Date().toISOString(),
      last: null,
      suite_profile: 'smoke',
      suite_index: 1,
      suite_total: 10,
    };
    const f = fakeApi({
      hubState: async () => node('running', 200),
      // the suite's first item finished just now, and the next one isn't registered yet
      runsList: async () => [{ ...(reportTcp as unknown as RunSummary), suite_id: 's1' }],
      runsActive: vi.fn(async () => (calls++ === 0 ? [] : [item])),
    });
    const store = new Store(f.api);
    await store.init();
    await flush();
    expect(store.get().suite).toBeNull();
    const target = peer();
    expect(runBlockReason(store.get(), target, 'tcp')).toMatch(/suite/);
    await vi.advanceTimersByTimeAsync(600);
    expect(store.get().suite?.suite_id).toBe('s1');
    expect(store.get().suiteCheck).toBe(false);
    expect(runBlockReason(store.get(), target, 'tcp')).toMatch(/in progress|suite/);
  });

  it('stops counting an attached run as in progress when it can be neither followed nor read', async () => {
    const active: ActiveRun = {
      run_id: 'lost',
      suite_id: null,
      workload: 'tcp',
      peer_name: 'studio',
      started_at: new Date().toISOString(),
      last: null,
      suite_profile: null,
      suite_index: null,
      suite_total: null,
    };
    const f = fakeApi({
      hubState: async () => node('running', 200),
      runsActive: vi.fn(async () => [active]),
      runAttach: () => Promise.reject(new Error('ipc')),
    });
    const store = new Store(f.api);
    await store.init();
    await flush();
    expect(store.get().live.lost.error).not.toBeNull();
    expect(activeRun(store.get())).toBeNull();
  });

  it('toasts when the data folder can’t be opened', async () => {
    const f = fakeApi({ openDataDir: () => Promise.reject({ code: 'io', message: 'no Finder' }) });
    const store = new Store(f.api);
    await store.openDataDir();
    expect(store.get().toast).toMatchObject({ kind: 'error', text: 'no Finder' });
  });

  it('keeps each live path sample on the connection it arrived on', async () => {
    let send: (p: Progress) => void = () => undefined;
    const f = fakeApi({
      hubState: async () => node('running', 200),
      benchRun: (_req, onProgress) => {
        send = onProgress;
        return new Promise<never>(() => undefined);
      },
    });
    const store = new Store(f.api);
    await store.init();
    const target = peer({ id: 'p1', connection: 'relay:fra' });
    f.emit({ type: 'peers', peers: [target] });
    void store.run(REGISTRY.tcp.defaults, target);
    const progress = (t_ms: number, value: number): Progress => ({
      run_id: Object.keys(store.get().live)[0],
      suite_id: null,
      t_ms,
      phase: 'running',
      fraction: 0.5,
      samples: [{ key: 'loaded_rtt_ms', value }],
      message: null,
    });
    send(progress(500, 30));
    f.emit({ type: 'peers', peers: [{ ...target, connection: 'direct' }] });
    send(progress(1000, 4));
    const run = Object.values(store.get().live)[0];
    expect(run.conn.loaded_rtt_ms).toEqual(['relay:fra', 'direct']);
    expect(run.series.loaded_rtt_ms.length).toBe(2);
  });

  it('keeps at most REPORT_CACHE reports in memory', async () => {
    const f = fakeApi({
      runGet: async (id) => ({ ...(reportTcp as unknown as RunReport), run_id: id }),
    });
    const store = new Store(f.api);
    for (let i = 0; i < REPORT_CACHE + 10; i++) await store.report(`r${i}`);
    const kept = Object.keys(store.get().reports);
    expect(kept.length).toBe(REPORT_CACHE);
    expect(kept).toContain(`r${REPORT_CACHE + 9}`);
    expect(kept).not.toContain('r0');
  });
});
