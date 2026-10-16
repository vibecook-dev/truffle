// Numbers, units, sizes and times, set the way the identity writes them: numerals with units
// ("912.4 Mb/s", "3.1%"), typographic quotes and dashes.

export const DASH = '–';

/** Decimal places that keep about three significant figures without inventing precision. */
function places(abs: number): number {
  if (abs >= 1000) return 0;
  if (abs >= 100) return 1;
  if (abs >= 10) return 1;
  if (abs >= 1) return 2;
  if (abs >= 0.1) return 2;
  if (abs === 0) return 0;
  return 3;
}

/**
 * A number for display; null, undefined and non-finite values become an en dash. A value too
 * small for the places shown is "< 0.001" (or "> -0.001"), never "0": a run that lost one
 * datagram in 600 000 did lose something. Zero is never "-0".
 */
export function fmtNumber(v: number | null | undefined, digits?: number): string {
  if (v == null || !Number.isFinite(v)) return DASH;
  // places for the value as shown: 9.9996 is "10.0", like 10.0001, not "10.00"
  const first = places(Math.abs(v));
  const d = digits ?? places(Math.abs(Number(v.toFixed(first))));
  if (v !== 0 && Math.abs(v) < 0.5 * 10 ** -d) {
    const q = d > 0 ? d : 3;
    return `${v < 0 ? '> -' : '< '}${(10 ** -q).toFixed(q)}`;
  }
  const shown = v === 0 ? 0 : v; // -0 is "0"
  return shown.toLocaleString('en-US', { minimumFractionDigits: d, maximumFractionDigits: d });
}

/** Unit glued the way the brand writes it: "3.1%", otherwise "912.4 Mb/s". */
export function withUnit(text: string, unit: string): string {
  if (!unit) return text;
  if (text === DASH) return text;
  return unit === '%' ? `${text}%` : `${text} ${unit}`;
}

export function fmtValue(v: number | null | undefined, unit = '', digits?: number): string {
  return withUnit(fmtNumber(v, digits), unit);
}

/** Integers, grouped. */
export function fmtInt(v: number | null | undefined): string {
  if (v == null || !Number.isFinite(v)) return DASH;
  return Math.round(v).toLocaleString('en-US');
}

const BYTE_UNITS = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];

/** Binary sizes: 64 B, 1 KiB, 10 MiB, 1.5 GiB. */
export function fmtBytes(n: number | null | undefined): string {
  if (n == null || !Number.isFinite(n)) return DASH;
  let v = n;
  let i = 0;
  while (Math.abs(v) >= 1024 && i < BYTE_UNITS.length - 1) {
    v /= 1024;
    i += 1;
  }
  const text = Number.isInteger(v) ? String(v) : fmtNumber(v, v >= 100 ? 0 : 1);
  return `${text} ${BYTE_UNITS[i]}`;
}

/** 412 ms, 4.2 s, 11 s, 2 min 05 s. Rounded first, so 59.6 s is "1 min 00 s", never "60 s". */
export function fmtDuration(ms: number | null | undefined): string {
  if (ms == null || !Number.isFinite(ms)) return DASH;
  if (Math.round(ms) < 1000) return `${Math.round(ms)} ms`;
  const s = ms / 1000;
  if (Number(s.toFixed(1)) < 10) return `${fmtNumber(s, 1)} s`;
  const total = Math.round(s);
  if (total < 60) return `${total} s`;
  return `${Math.floor(total / 60)} min ${String(total % 60).padStart(2, '0')} s`;
}

/** 4:59, for countdowns. */
export function fmtClock(secs: number): string {
  const s = Math.max(0, Math.ceil(secs));
  return `${Math.floor(s / 60)}:${String(s % 60).padStart(2, '0')}`;
}

const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

/** 16 Oct 14:02 (local time). */
export function fmtWhen(iso: string | null | undefined): string {
  if (!iso) return DASH;
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return DASH;
  const hh = String(d.getHours()).padStart(2, '0');
  const mm = String(d.getMinutes()).padStart(2, '0');
  return `${d.getDate()} ${MONTHS[d.getMonth()]} ${hh}:${mm}`;
}

/** just now, 4 min ago, 3 h ago, 2 days ago */
export function fmtAgo(iso: string | null | undefined, now = Date.now()): string {
  if (!iso) return DASH;
  const t = new Date(iso).getTime();
  if (Number.isNaN(t)) return DASH;
  const s = Math.max(0, (now - t) / 1000);
  if (s < 45) return 'just now';
  if (s < 3600) return `${Math.round(s / 60)} min ago`;
  if (s < 86400) return `${Math.round(s / 3600)} h ago`;
  const days = Math.round(s / 86400);
  return `${days} day${days === 1 ? '' : 's'} ago`;
}

export function plural(n: number, one: string, many = `${one}s`): string {
  return `${n} ${n === 1 ? one : many}`;
}

/** “text” */
export function quote(s: string): string {
  return `“${s}”`;
}

/** A Tailscale connection string for people: "direct", "relay (sfo)", "unknown". */
export function fmtConnection(c: string | null | undefined): string {
  if (!c) return 'unknown';
  if (c.startsWith('relay:')) return `relay (${c.slice(6)})`;
  return c;
}

/** The DERP region of a relayed connection, or null. */
export function relayRegion(c: string | null | undefined): string | null {
  return c && c.startsWith('relay:') ? c.slice(6) : null;
}

const OS_NAMES: Record<string, string> = {
  macos: 'macOS',
  darwin: 'macOS',
  linux: 'Linux',
  windows: 'Windows',
  ios: 'iOS',
  android: 'Android',
};

export function fmtOs(os: string | null | undefined): string {
  if (!os) return DASH;
  return OS_NAMES[os.toLowerCase()] ?? os;
}
