// The engine's statistics rules (RFC 027 §7.4), for values the UI derives itself, such as a
// CDF of the path sampler's RTTs. The engine's numbers stay authoritative.

/** Nearest-rank percentile, null unless n ≥ ⌈1/(1 − p)⌉ (p90 needs 10 samples, p99 100). */
export function percentile(sorted: number[], p: number): number | null {
  const n = sorted.length;
  if (n === 0) return null;
  if (p > 0 && p < 1 && n < Math.ceil(1 / (1 - p) - 1e-9)) return null;
  const rank = Math.max(1, Math.ceil(p * n));
  const v = sorted[rank - 1];
  return Number.isFinite(v) ? v : null;
}

export function mean(xs: number[]): number | null {
  const f = xs.filter(Number.isFinite);
  if (!f.length) return null;
  return f.reduce((a, b) => a + b, 0) / f.length;
}

export function sortNumbers(xs: number[]): number[] {
  return xs.filter((x) => !Number.isNaN(x)).sort((a, b) => a - b);
}

/** Empirical CDF as step points [value, fraction 0..1]; +∞ (lost) samples are left out. */
export function ecdf(sorted: number[]): [number, number][] {
  const n = sorted.length;
  const out: [number, number][] = [];
  for (let i = 0; i < n; i++) {
    if (!Number.isFinite(sorted[i])) break;
    out.push([sorted[i], (i + 1) / n]);
  }
  return out;
}

/** Equal-width bins between lo and hi. */
export function histogram(xs: number[], bins: number, lo?: number, hi?: number) {
  const f = xs.filter(Number.isFinite);
  const a = lo ?? Math.min(...f);
  const b = hi ?? Math.max(...f);
  const w = b > a ? (b - a) / bins : 1;
  const counts = new Array<number>(bins).fill(0);
  for (const x of f) {
    const i = Math.min(bins - 1, Math.max(0, Math.floor((x - a) / w)));
    counts[i] += 1;
  }
  return counts.map((c, i) => ({ x0: a + i * w, x1: a + (i + 1) * w, count: c }));
}
