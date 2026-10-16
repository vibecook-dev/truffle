// Keyboard: ⌘R runs the current view's benchmark (when it can run), Esc cancels it, ⌘1…⌘9 move
// through the rail and ⌘, opens Settings. The view on screen registers what Run and Cancel mean
// for it; App owns the one keydown listener.

import { useEffect } from 'react';

export interface ViewActions {
  /** Start a run, if the view can run now. */
  run?: () => void;
  canRun?: boolean;
  /** Cancel what this view started, if anything is running. */
  cancel?: () => void;
  canCancel?: boolean;
}

let current: ViewActions = {};

/** The view on screen says what ⌘R and Esc do. Re-registered every render, so it stays current. */
export function useViewActions(actions: ViewActions) {
  useEffect(() => {
    current = actions;
    return () => {
      if (current === actions) current = {};
    };
  });
}

/** True for macOS-style ⌘, Ctrl elsewhere. */
export const isMac =
  typeof navigator !== 'undefined' &&
  /Mac|iPhone|iPad/.test(navigator.platform || navigator.userAgent);
export const MOD = isMac ? '⌘' : 'Ctrl';

const typing = (el: Element | null) =>
  !!el &&
  (el.tagName === 'INPUT' ||
    el.tagName === 'TEXTAREA' ||
    el.tagName === 'SELECT' ||
    (el as HTMLElement).isContentEditable);

/**
 * The app's shortcuts. `nav[i]` is where ⌘(i+1) goes; `settings` is ⌘,.
 * Esc first lets an armed confirmation or an open control handle it (they stop propagation).
 */
export function useShortcuts(nav: (() => void)[], settings: () => void, enabled: boolean) {
  useEffect(() => {
    if (!enabled) return undefined;
    const onKey = (e: KeyboardEvent) => {
      const mod = isMac ? e.metaKey : e.ctrlKey;
      if (mod && !e.altKey && !e.shiftKey && (e.key === 'r' || e.key === 'R')) {
        // never reload the webview: runs live in Rust, but the view would flash
        e.preventDefault();
        if (current.run && current.canRun) current.run();
        return;
      }
      if (mod && !e.altKey && /^[1-9]$/.test(e.key)) {
        const go = nav[Number(e.key) - 1];
        if (go) {
          e.preventDefault();
          go();
        }
        return;
      }
      if (mod && e.key === ',') {
        e.preventDefault();
        settings();
        return;
      }
      if (e.key === 'Escape' && !e.defaultPrevented) {
        if (typing(document.activeElement)) {
          (document.activeElement as HTMLElement).blur();
          return;
        }
        if (current.cancel && current.canCancel) {
          e.preventDefault();
          current.cancel();
        }
      }
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [nav, settings, enabled]);
}
