import { describe, expect, it } from 'vitest';
import {
  fmtBytes,
  fmtClock,
  fmtConnection,
  fmtDuration,
  fmtNumber,
  fmtValue,
  relayRegion,
  withUnit,
} from './format';

describe('format', () => {
  it('keeps about three significant figures', () => {
    expect(fmtNumber(912.44)).toBe('912.4');
    expect(fmtNumber(2.384)).toBe('2.38');
    expect(fmtNumber(0.0184)).toBe('0.018');
    expect(fmtNumber(1350)).toBe('1,350');
    // places follow the value as shown, so neighbours across a boundary read alike
    expect(fmtNumber(9.9996)).toBe('10.0');
    expect(fmtNumber(999.97)).toBe('1,000');
    expect(fmtNumber(0.09996)).toBe('0.10');
    expect(fmtNumber(null)).toBe('–');
    expect(fmtNumber(Infinity)).toBe('–');
    // too small for its places: never "0" or "-0"
    expect(fmtNumber(0.00016)).toBe('< 0.001');
    expect(fmtNumber(0.0004)).toBe('< 0.001');
    expect(fmtNumber(-0.0001)).toBe('> -0.001');
    expect(fmtNumber(0.0005)).toBe('0.001');
    expect(fmtNumber(0.004, 2)).toBe('< 0.01');
    expect(fmtNumber(0.3, 0)).toBe('< 0.001');
    expect(fmtNumber(0)).toBe('0');
    expect(fmtNumber(-0)).toBe('0');
  });
  it('glues units the way the brand writes them', () => {
    expect(fmtValue(3.14, '%')).toBe('3.14%');
    expect(fmtValue(912.4, 'Mb/s')).toBe('912.4 Mb/s');
    expect(withUnit('–', 'ms')).toBe('–');
  });
  it('writes binary sizes', () => {
    expect(fmtBytes(64)).toBe('64 B');
    expect(fmtBytes(1024)).toBe('1 KiB');
    expect(fmtBytes(10485760)).toBe('10 MiB');
    expect(fmtBytes(1536)).toBe('1.5 KiB');
  });
  it('writes durations and countdowns', () => {
    expect(fmtDuration(412)).toBe('412 ms');
    expect(fmtDuration(11400)).toBe('11 s');
    expect(fmtDuration(4200)).toBe('4.2 s');
    expect(fmtDuration(125000)).toBe('2 min 05 s');
    // rounded before it is split
    expect(fmtDuration(119600)).toBe('2 min 00 s');
    expect(fmtDuration(59600)).toBe('1 min 00 s');
    expect(fmtDuration(59400)).toBe('59 s');
    expect(fmtDuration(9970)).toBe('10 s');
    expect(fmtDuration(999.6)).toBe('1.0 s');
    expect(fmtDuration(60000)).toBe('1 min 00 s');
    expect(fmtClock(300)).toBe('5:00');
    expect(fmtClock(59.2)).toBe('1:00');
    expect(fmtClock(-3)).toBe('0:00');
  });
  it('names connections', () => {
    expect(fmtConnection('direct')).toBe('direct');
    expect(fmtConnection('relay:sfo')).toBe('relay (sfo)');
    expect(fmtConnection('')).toBe('unknown');
    expect(relayRegion('relay:nyc')).toBe('nyc');
    expect(relayRegion('direct')).toBeNull();
  });
});
