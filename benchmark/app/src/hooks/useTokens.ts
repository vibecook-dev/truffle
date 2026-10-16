// Brand colour tokens resolved for drawing. Charts read them with getComputedStyle and read
// them again when the system theme or <html data-theme> changes (RFC 027 §11.1, §11.4).

import { useEffect, useState } from 'react';

export interface Tokens {
  ground: string;
  groundRaised: string;
  groundSunken: string;
  rule: string;
  ruleStrong: string;
  ink: string;
  ink2: string;
  ink3: string;
  onInk: string;
  blueline: string;
  bluelineInk: string;
  bluelineWash: string;
  redline: string;
  fontMono: string;
  fontSans: string;
}

const NAMES: Record<keyof Tokens, string> = {
  ground: '--ground',
  groundRaised: '--ground-raised',
  groundSunken: '--ground-sunken',
  rule: '--rule',
  ruleStrong: '--rule-strong',
  ink: '--ink',
  ink2: '--ink-2',
  ink3: '--ink-3',
  onInk: '--on-ink',
  blueline: '--blueline',
  bluelineInk: '--blueline-ink',
  bluelineWash: '--blueline-wash',
  redline: '--redline',
  fontMono: '--font-mono',
  fontSans: '--font-sans',
};

export function readTokens(): Tokens {
  const cs = getComputedStyle(document.documentElement);
  const out = {} as Tokens;
  for (const k of Object.keys(NAMES) as (keyof Tokens)[])
    out[k] = cs.getPropertyValue(NAMES[k]).trim();
  return out;
}

type Listener = () => void;
const listeners = new Set<Listener>();
let current: Tokens | null = null;
let watching = false;

function watch() {
  if (watching || typeof window === 'undefined') return;
  watching = true;
  const refresh = () => {
    current = readTokens();
    listeners.forEach((l) => l());
  };
  window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', refresh);
  new MutationObserver(refresh).observe(document.documentElement, {
    attributes: true,
    attributeFilter: ['data-theme'],
  });
}

/** The current tokens; components re-render when the theme changes. */
export function useTokens(): Tokens {
  watch();
  const [tokens, setTokens] = useState<Tokens>(() => current ?? (current = readTokens()));
  useEffect(() => {
    const l = () => setTokens(current ?? readTokens());
    listeners.add(l);
    l();
    return () => {
      listeners.delete(l);
    };
  }, []);
  return tokens;
}
