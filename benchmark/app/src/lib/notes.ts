// Notes from a report and from the workload, said once each (RFC 027 §7.4 honesty notes).

/** A note's identity: case, spacing, quote style and trailing punctuation don't matter. */
export const noteKey = (n: string) =>
  n
    .toLowerCase()
    .replace(/[‘’]/g, "'")
    .replace(/\s+/g, ' ')
    .replace(/[.:;\s]+$/, '');
const norm = noteKey;

/**
 * The report's notes, then the workload's own, each said once: a note that another one starts
 * with is the same note, and the longer wording wins.
 */
export function mergeNotes(...lists: string[][]): string[] {
  // "new: one WhoIs…" and "New connections: one WhoIs…" are the same note
  const body = (k: string) => k.replace(/^[a-z][a-z -]{0,24}: /, '');
  const out: string[] = [];
  for (const n of lists.flat()) {
    const k = norm(n);
    const i = out.findIndex((o) => {
      const ko = norm(o);
      return ko.startsWith(k) || k.startsWith(ko) || body(ko) === body(k);
    });
    if (i < 0) out.push(n);
    else if (k.length > norm(out[i]).length) out[i] = n;
  }
  // sentence case, unless the note starts with an identifier (broadcast_typed, open_tcp), and
  // typographic apostrophes (the engine writes straight ones)
  return out.map((n) => {
    const first = n.split(/\s/)[0];
    const lead = /[_()]/.test(first) ? n : `${n.charAt(0).toUpperCase()}${n.slice(1)}`;
    return `${lead}${/[.:]$/.test(n) ? '' : '.'}`.replace(/(\w)'(\w)/g, '$1’$2');
  });
}
