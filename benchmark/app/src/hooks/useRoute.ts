// Pages are addressed by a plain hash: #/overview, #/tcp, #/history/<run_id>.

import { useCallback, useEffect, useState } from 'react';

export type Page =
  | 'overview'
  | 'peers'
  | 'path'
  | 'tcp'
  | 'udp'
  | 'quic'
  | 'messaging'
  | 'file-transfer'
  | 'synced-store'
  | 'http'
  | 'video'
  | 'egress'
  | 'planned'
  | 'suite'
  | 'history'
  | 'settings';

export const PAGES: readonly Page[] = [
  'overview',
  'peers',
  'path',
  'tcp',
  'udp',
  'quic',
  'messaging',
  'file-transfer',
  'synced-store',
  'http',
  'video',
  'egress',
  'planned',
  'suite',
  'history',
  'settings',
];

export interface Route {
  page: Page;
  id: string;
}

export function parseHash(hash: string): Route | null {
  let raw = '';
  try {
    raw = decodeURIComponent(hash.replace(/^#\/?/, ''));
  } catch {
    raw = '';
  }
  const [page, ...rest] = raw.split('/');
  if (!(PAGES as readonly string[]).includes(page)) return null;
  return { page: page as Page, id: rest.join('/') };
}

export function hrefOf(page: Page, id = ''): string {
  return `#/${page}${id ? `/${encodeURIComponent(id)}` : ''}`;
}

export function useRoute(): [Route, (page: Page, id?: string) => void] {
  const [route, setRoute] = useState<Route>(
    () => parseHash(window.location.hash) ?? { page: 'overview', id: '' },
  );
  useEffect(() => {
    const on = () => {
      const r = parseHash(window.location.hash);
      if (r) setRoute(r);
    };
    window.addEventListener('hashchange', on);
    return () => window.removeEventListener('hashchange', on);
  }, []);
  const go = useCallback((page: Page, id = '') => {
    const target = hrefOf(page, id);
    if (window.location.hash !== target) window.location.hash = target;
    setRoute({ page, id });
  }, []);
  return [route, go];
}
