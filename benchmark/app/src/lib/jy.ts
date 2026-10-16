// Read the bundle at render time, never at import time, so module order can't bite.
import type { JYApi } from '../brand/jy.d';

export function jy(): JYApi | null {
  return (window as { JY?: JYApi }).JY ?? null;
}

export const JY_ICONS = new Set([
  'arrow-right',
  'arrow-left',
  'arrow-up-right',
  'download',
  'close',
  'menu',
  'plus',
  'minus',
  'check',
  'copy',
  'search',
  'mail',
  'theme',
]);
