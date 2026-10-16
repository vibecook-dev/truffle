import { describe, expect, it } from 'vitest';
import {
  fmtTick,
  linear,
  markerIndices,
  nearestIndex,
  niceStep,
  niceTicks,
  ticksWithin,
} from './scale';

describe('scale', () => {
  it('picks 1, 2, 2.5 or 5 steps', () => {
    expect(niceStep(10, 5)).toBe(2);
    expect(niceStep(800, 4)).toBe(200);
    expect(niceStep(1, 4)).toBe(0.25);
  });
  it('widens the domain to whole steps', () => {
    const t = niceTicks(0, 657, 4);
    expect(t.min).toBe(0);
    expect(t.max).toBe(800);
    expect(t.ticks).toEqual([0, 200, 400, 600, 800]);
  });
  it('keeps a time domain exact', () => {
    const t = ticksWithin(0, 11.25, 5);
    expect(t.max).toBe(11.25);
    expect(t.ticks[t.ticks.length - 1]).toBeLessThanOrEqual(11.25);
    expect(t.ticks[0]).toBe(0);
  });
  it('formats ticks with the digits the step needs', () => {
    expect(fmtTick(0.25, 0.25)).toBe('0.25');
    expect(fmtTick(1000, 500)).toBe('1,000');
    expect(fmtTick(0.5, 0.5)).toBe('0.5');
  });
  it('maps and inverts', () => {
    const x = linear(0, 10, 100, 200);
    expect(x(5)).toBe(150);
    expect(x.invert(150)).toBe(5);
  });
  it('finds the nearest point', () => {
    expect(nearestIndex([0, 250, 500, 750], 380)).toBe(2);
    expect(nearestIndex([], 1)).toBe(-1);
  });
  it('spaces markers and always keeps the last', () => {
    const m = markerIndices(44, 12);
    expect(m[0]).toBe(0);
    expect(m[m.length - 1]).toBe(43);
    expect(m.length).toBeLessThanOrEqual(13);
  });
});
