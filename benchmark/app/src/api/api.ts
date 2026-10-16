// The app's view of the Rust side: one method per §11.2 command, plus the `bench://event`
// subscription. `tauri.ts` implements it over `invoke`, `mock.ts` in the browser.
//
// Every method rejects with an `AppError` ({ code, message }); use `toAppError` to read one.

import type {
  ActiveRun,
  AppError,
  AppInfo,
  BenchPeer,
  HubEvent,
  HubStartRequest,
  ImportResult,
  NodeState,
  PeerProbe,
  Progress,
  RunReport,
  RunRequest,
  RunSummary,
  Settings,
  SuiteProfile,
  SuiteProgress,
  SuiteResult,
  SuiteRunRequest,
  WorkloadParams,
} from './types';

export type Unsubscribe = () => void;

/**
 * An event subscription: call it to unsubscribe. `ready` settles once the listener is registered
 * (also if registering failed), so a caller can subscribe before it reads a snapshot and miss
 * nothing in between.
 */
export type Subscription = Unsubscribe & { ready: Promise<void> };

export interface Api {
  /** Which implementation is behind this object. */
  readonly kind: 'tauri' | 'mock';

  /** `app_info` */
  appInfo(): Promise<AppInfo>;
  /** `ui_ready`: call once after the first themed paint; Rust then shows the window. */
  uiReady(): Promise<void>;

  /** `hub_state` */
  hubState(): Promise<NodeState>;
  /** `hub_start`: returns at once; progress arrives as `node` events. */
  hubStart(req: HubStartRequest): Promise<NodeState>;
  /** `hub_stop` */
  hubStop(): Promise<NodeState>;
  /** `hub_sign_out`: LocalAPI logout, stop, then delete the state dir. */
  hubSignOut(): Promise<NodeState>;

  /** `open_url`: https only, opened by the OS. */
  openUrl(url: string): Promise<void>;
  /** `open_data_dir` */
  openDataDir(): Promise<void>;

  /** `peers_list` */
  peersList(): Promise<BenchPeer[]>;
  /** `peer_probe` */
  peerProbe(peer: string): Promise<PeerProbe>;

  /** `bench_run` + `onProgress: Channel<Progress>`. The returned report is authoritative. */
  benchRun(req: RunRequest, onProgress: (p: Progress) => void): Promise<RunReport>;
  /** `bench_cancel`: a run_id or a suite_id. */
  benchCancel(id: string): Promise<boolean>;
  /** `runs_active`: runs live in Rust, so a reload loses nothing. */
  runsActive(): Promise<ActiveRun[]>;
  /** `run_attach` + `onProgress`: replays the last Progress, then streams. */
  runAttach(runId: string, onProgress: (p: Progress) => void): Promise<boolean>;

  /** `suite_profiles` */
  suiteProfiles(): Promise<SuiteProfile[]>;
  /** `suite_run` + `onProgress: Channel<SuiteProgress>` */
  suiteRun(req: SuiteRunRequest, onProgress: (p: SuiteProgress) => void): Promise<SuiteResult>;
  /** `workload_defaults` */
  workloadDefaults(): Promise<WorkloadParams[]>;

  /** `runs_list` */
  runsList(): Promise<RunSummary[]>;
  /** `run_get` */
  runGet(runId: string): Promise<RunReport>;
  /** `run_delete` */
  runDelete(runId: string): Promise<void>;
  /** `runs_import`: files, folders or .zip archives. */
  runsImport(paths: string[]): Promise<ImportResult>;
  /** `runs_import_pick`: the native picker, in Rust. */
  runsImportPick(): Promise<ImportResult>;

  /** `settings_get` */
  settingsGet(): Promise<Settings>;
  /** `settings_set`: also applies the theme to the native window. */
  settingsSet(settings: Settings): Promise<Settings>;

  /** `bench://event` */
  onEvent(handler: (event: HubEvent) => void): Subscription;
  /**
   * OS file drops on the window (History imports them). `onHover` says whether files are being
   * dragged over the window, so the drop zone can show it (Tauri: onDragDropEvent enter/over/
   * leave/drop).
   */
  onFileDrop(handler: (paths: string[]) => void, onHover?: (over: boolean) => void): Unsubscribe;
}

/** The §11.2 command name behind each method, for the Tauri implementation and for logs. */
export const COMMANDS = {
  appInfo: 'app_info',
  uiReady: 'ui_ready',
  hubState: 'hub_state',
  hubStart: 'hub_start',
  hubStop: 'hub_stop',
  hubSignOut: 'hub_sign_out',
  openUrl: 'open_url',
  openDataDir: 'open_data_dir',
  peersList: 'peers_list',
  peerProbe: 'peer_probe',
  benchRun: 'bench_run',
  benchCancel: 'bench_cancel',
  runsActive: 'runs_active',
  runAttach: 'run_attach',
  suiteProfiles: 'suite_profiles',
  suiteRun: 'suite_run',
  workloadDefaults: 'workload_defaults',
  runsList: 'runs_list',
  runGet: 'run_get',
  runDelete: 'run_delete',
  runsImport: 'runs_import',
  runsImportPick: 'runs_import_pick',
  settingsGet: 'settings_get',
  settingsSet: 'settings_set',
} as const;

export const EVENT_NAME = 'bench://event';

const CODES = new Set([
  'not-running',
  'peer-not-found',
  'peer-ambiguous',
  'peer-offline',
  'login-refused',
  'busy',
  'being-measured',
  'protocol-mismatch',
  'unsupported-workload',
  'invalid-params',
  'timeout',
  'state-in-use',
  'sidecar-not-found',
  'cancelled',
  'io',
  'internal',
]);

export function isAppError(e: unknown): e is AppError {
  return (
    !!e &&
    typeof e === 'object' &&
    typeof (e as AppError).message === 'string' &&
    CODES.has((e as AppError).code)
  );
}

/** Whatever a command rejected with, as an AppError. */
export function toAppError(e: unknown): AppError {
  if (isAppError(e)) return e;
  if (e instanceof Error) return { code: 'internal', message: e.message };
  if (typeof e === 'string') return { code: 'internal', message: e };
  return { code: 'internal', message: 'Something went wrong.' };
}
