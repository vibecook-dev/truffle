import { describe, expect, it } from 'vitest';
import { frameStripMeta, type FrameCell } from './Strips';

/** A strip as the engine thins it: every `stride`-th on-time frame, plus every other frame. */
function thinned(total: number, warmup: number, late: Set<number>, stride: number): FrameCell[] {
  const out: FrameCell[] = [];
  for (let i = 0; i < total; i++) {
    const state = late.has(i) ? 'late' : 'on-time';
    if (state === 'on-time' && i % stride !== 0) continue;
    out.push({ frame_no: i, keyframe: i % 60 === 0, state, warmup: i < warmup });
  }
  return out;
}

describe('frame strip header', () => {
  it('counts the run, not a thinned strip’s cells', () => {
    // 60 fps for 60 s plus a 1 s warmup: 3660 frames, stride 2, 1% late
    const late = new Set(Array.from({ length: 36 }, (_, k) => 60 + k * 100));
    const frames = thinned(3660, 60, late, 2);
    const totals = { sent: 3600, onTime: 3564, late: 36, dropped: 0 };
    const h = frameStripMeta(frames, 2, totals);
    expect(h.meta).toBe(
      '3,600 frames · 3,564 on time · 36 late · 0 dropped · every 2nd on-time frame shown',
    );
    expect(h.late).toBe(36);
    // without the totals the cells under-count on-time frames (what the header used to say)
    expect(frameStripMeta(frames).meta).toMatch(/^1,8\d\d frames/);
  });

  it('counts the cells of a strip that wasn’t thinned', () => {
    const frames = thinned(90, 30, new Set([40]), 1);
    expect(frameStripMeta(frames).meta).toBe('60 frames · 59 on time · 1 late · 0 dropped');
    expect(frameStripMeta(frames, 3).meta).toMatch(/every 3rd on-time frame shown$/);
    expect(frameStripMeta(frames, 11).meta).toMatch(/every 11th on-time frame shown$/);
  });
});
