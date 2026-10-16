import { describe, expect, it } from 'vitest';
import { ecdf, histogram, percentile, sortNumbers } from './stats';

describe('stats (RFC 027 §7.4 rules)', () => {
  const xs = Array.from({ length: 100 }, (_, i) => i + 1);
  it('uses nearest rank', () => {
    expect(percentile(xs, 0.5)).toBe(50);
    expect(percentile(xs, 0.99)).toBe(99);
  });
  it('withholds a percentile until n is large enough', () => {
    expect(percentile(xs.slice(0, 9), 0.9)).toBeNull();
    expect(percentile(xs.slice(0, 10), 0.9)).toBe(9);
    expect(percentile(xs.slice(0, 99), 0.99)).toBeNull();
  });
  it('treats a lost sample as +∞ and nulls a percentile that lands on it', () => {
    const s = sortNumbers([1, 2, 3, 4, 5, 6, 7, 8, 9, Infinity]);
    expect(percentile(s, 0.9)).toBe(9);
    expect(percentile([1, Infinity], 0.5 + 1e-9)).toBeNull();
  });
  it('builds an ECDF without lost samples', () => {
    expect(ecdf([1, 2, Infinity, Infinity])).toEqual([
      [1, 0.25],
      [2, 0.5],
    ]);
  });
  it('bins into equal widths', () => {
    const h = histogram([0, 1, 2, 3], 2, 0, 4);
    expect(h.map((b) => b.count)).toEqual([2, 2]);
  });
});
