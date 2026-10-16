// Per-viewer conveniences only (rail width, the "show other accounts" filter). Every access is
// guarded: storage can be missing or throw, and the app works without it.

const PREFIX = 'truffle-bench/';

export function load<T>(key: string, fallback: T): T {
  try {
    const raw = window.localStorage.getItem(PREFIX + key);
    return raw == null ? fallback : (JSON.parse(raw) as T);
  } catch {
    return fallback;
  }
}

export function save(key: string, value: unknown): void {
  try {
    window.localStorage.setItem(PREFIX + key, JSON.stringify(value));
  } catch {
    /* not available */
  }
}
