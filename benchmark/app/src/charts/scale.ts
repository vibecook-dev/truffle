// Scales and ticks for the SVG charts. Pure functions, unit-tested.

export interface Ticks {
  min: number;
  max: number;
  step: number;
  ticks: number[];
}

/** A step of 1, 2, 2.5 or 5 × 10ⁿ that gives about `count` intervals. */
export function niceStep(span: number, count: number): number {
  if (!(span > 0) || !Number.isFinite(span)) return 1;
  const raw = span / Math.max(1, count);
  const mag = Math.pow(10, Math.floor(Math.log10(raw)));
  const norm = raw / mag;
  const nice = norm <= 1 ? 1 : norm <= 2 ? 2 : norm <= 2.5 ? 2.5 : norm <= 5 ? 5 : 10;
  return nice * mag;
}

/** Ticks covering [lo, hi], with the domain widened to whole steps. */
export function niceTicks(lo: number, hi: number, count = 5): Ticks {
  let a = Number.isFinite(lo) ? lo : 0;
  let b = Number.isFinite(hi) ? hi : 1;
  if (a === b) {
    if (a === 0) b = 1;
    else {
      a = a > 0 ? 0 : a * 1.5;
      b = b > 0 ? b * 1.5 : 0;
    }
  }
  if (a > b) [a, b] = [b, a];
  const step = niceStep(b - a, count);
  const min = Math.floor(a / step + 1e-9) * step;
  const max = Math.ceil(b / step - 1e-9) * step;
  const ticks: number[] = [];
  for (let v = min; v <= max + step * 1e-6; v += step) ticks.push(Number(v.toFixed(10)));
  return { min, max, step, ticks };
}

/** Ticks inside [lo, hi] without widening the domain: for time axes, where the end is the end. */
export function ticksWithin(lo: number, hi: number, count = 5): Ticks {
  const span = hi > lo ? hi - lo : 1;
  const step = niceStep(span, count);
  const ticks: number[] = [];
  const first = Math.ceil(lo / step - 1e-9) * step;
  for (let v = first; v <= hi + step * 1e-6; v += step) ticks.push(Number(v.toFixed(10)));
  return { min: lo, max: hi > lo ? hi : lo + 1, step, ticks };
}

/** Decimal places a tick step needs. */
export function stepDigits(step: number): number {
  if (step >= 1) return Number.isInteger(step) ? 0 : 1;
  return Math.min(6, Math.ceil(-Math.log10(step) - 1e-9) + (String(step).includes('25') ? 1 : 0));
}

export function fmtTick(v: number, step: number): string {
  const d = stepDigits(step);
  return v.toLocaleString('en-US', { minimumFractionDigits: d, maximumFractionDigits: d });
}

export type Scale = ((v: number) => number) & { invert: (px: number) => number };

export function linear(d0: number, d1: number, r0: number, r1: number): Scale {
  const span = d1 - d0 || 1;
  const f = ((v: number) => r0 + ((v - d0) / span) * (r1 - r0)) as Scale;
  f.invert = (px: number) => d0 + ((px - r0) / (r1 - r0 || 1)) * span;
  return f;
}

/** Index of the point whose x is nearest to `x` (points sorted by x). */
export function nearestIndex(xs: number[], x: number): number {
  if (!xs.length) return -1;
  let lo = 0;
  let hi = xs.length - 1;
  while (hi - lo > 1) {
    const mid = (lo + hi) >> 1;
    if (xs[mid] < x) lo = mid;
    else hi = mid;
  }
  return Math.abs(xs[lo] - x) <= Math.abs(xs[hi] - x) ? lo : hi;
}

/** SVG path through points, skipping non-finite values (gaps stay gaps). */
export function linePath(pts: [number, number][]): string {
  let d = '';
  let pen = false;
  for (const [x, y] of pts) {
    if (!Number.isFinite(x) || !Number.isFinite(y)) {
      pen = false;
      continue;
    }
    d += `${pen ? 'L' : 'M'}${round(x)} ${round(y)}`;
    pen = true;
  }
  return d;
}

export function round(v: number): number {
  return Math.round(v * 10) / 10;
}

/** Every k-th index plus the last, so about `max` markers are drawn. */
export function markerIndices(n: number, max = 12): number[] {
  if (n <= 0) return [];
  const k = Math.max(1, Math.ceil(n / max));
  const out: number[] = [];
  for (let i = 0; i < n; i += k) out.push(i);
  if (out[out.length - 1] !== n - 1) out.push(n - 1);
  return out;
}
