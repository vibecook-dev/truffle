// The real implementation: §11.2 commands over Tauri IPC.
//
// JS calls `invoke(cmd, { req })`; streaming commands also pass `onProgress`, a Channel
// (Tauri renames the Rust `on_progress` argument to camelCase). Rejections are AppErrors.

import { Channel, invoke } from '@tauri-apps/api/core';
import { listen } from '@tauri-apps/api/event';
import { getCurrentWebview } from '@tauri-apps/api/webview';
import { COMMANDS, EVENT_NAME, type Api, type Subscription } from './api';
import type { HubEvent, Progress, SuiteProgress } from './types';

/**
 * Wraps an async subscription so callers get a synchronous unsubscribe, plus `ready`, which
 * settles once the listener is registered (or failed to be: it never rejects, so a caller that
 * waits on it never hangs).
 */
function deferred(start: () => Promise<() => void>): Subscription {
  let stop: (() => void) | null = null;
  let stopped = false;
  const ready = start().then(
    (fn) => {
      if (stopped) fn();
      else stop = fn;
    },
    () => undefined,
  );
  return Object.assign(
    () => {
      stopped = true;
      if (stop) stop();
    },
    { ready },
  );
}

export function createTauriApi(): Api {
  const call = <T>(cmd: string, args?: Record<string, unknown>) => invoke<T>(cmd, args);
  return {
    kind: 'tauri',
    appInfo: () => call(COMMANDS.appInfo),
    uiReady: () => call(COMMANDS.uiReady),
    hubState: () => call(COMMANDS.hubState),
    hubStart: (req) => call(COMMANDS.hubStart, { req }),
    hubStop: () => call(COMMANDS.hubStop),
    hubSignOut: () => call(COMMANDS.hubSignOut),
    openUrl: (url) => call(COMMANDS.openUrl, { req: { url } }),
    openDataDir: () => call(COMMANDS.openDataDir),
    peersList: () => call(COMMANDS.peersList),
    peerProbe: (peer) => call(COMMANDS.peerProbe, { req: { peer } }),
    benchRun: (req, onProgress) => {
      const channel = new Channel<Progress>();
      channel.onmessage = onProgress;
      return call(COMMANDS.benchRun, { req, onProgress: channel });
    },
    benchCancel: (id) => call(COMMANDS.benchCancel, { req: { id } }),
    runsActive: () => call(COMMANDS.runsActive),
    runAttach: (runId, onProgress) => {
      const channel = new Channel<Progress>();
      channel.onmessage = onProgress;
      return call(COMMANDS.runAttach, { req: { run_id: runId }, onProgress: channel });
    },
    suiteProfiles: () => call(COMMANDS.suiteProfiles),
    suiteRun: (req, onProgress) => {
      const channel = new Channel<SuiteProgress>();
      channel.onmessage = onProgress;
      return call(COMMANDS.suiteRun, { req, onProgress: channel });
    },
    workloadDefaults: () => call(COMMANDS.workloadDefaults),
    runsList: () => call(COMMANDS.runsList),
    runGet: (runId) => call(COMMANDS.runGet, { req: { run_id: runId } }),
    runDelete: (runId) => call(COMMANDS.runDelete, { req: { run_id: runId } }),
    runsImport: (paths) => call(COMMANDS.runsImport, { req: { paths } }),
    runsImportPick: () => call(COMMANDS.runsImportPick),
    settingsGet: () => call(COMMANDS.settingsGet),
    settingsSet: (settings) => call(COMMANDS.settingsSet, { req: settings }),
    onEvent: (handler) => deferred(() => listen<HubEvent>(EVENT_NAME, (e) => handler(e.payload))),
    onFileDrop: (handler, onHover) =>
      deferred(() =>
        getCurrentWebview().onDragDropEvent((e) => {
          const t = e.payload.type;
          if (t === 'enter' || t === 'over') onHover?.(true);
          else onHover?.(false);
          if (t === 'drop' && e.payload.paths.length) handler(e.payload.paths);
        }),
      ),
  };
}
