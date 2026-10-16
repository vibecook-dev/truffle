// Picks the implementation: the Tauri IPC bridge inside the app, the mock in a plain browser
// (vite dev, screenshots, design work). Each loads as its own chunk, so a bundled app never
// evaluates the mock.

import type { Api } from './api';

export type { Api, Subscription, Unsubscribe } from './api';
export { COMMANDS, EVENT_NAME, isAppError, toAppError } from './api';
export * from './types';

export function isTauri(): boolean {
  return typeof window !== 'undefined' && '__TAURI_INTERNALS__' in window;
}

export async function loadApi(): Promise<Api> {
  if (isTauri()) {
    const { createTauriApi } = await import('./tauri');
    return createTauriApi();
  }
  const { createMockApi } = await import('./mock');
  return createMockApi();
}
