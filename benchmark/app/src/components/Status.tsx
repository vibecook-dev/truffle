// Home's .status: the mark's square plus an uppercase label (RFC 027 §11.4).
// filled = live/done, outlined = waiting, half = running, dashed = planned,
// redline = failed (always with its label), a flat line = offline.

import type { ReactNode } from 'react';

export type StatusKind = 'live' | 'done' | 'waiting' | 'running' | 'planned' | 'failed' | 'off';

const WORDS: Record<StatusKind, string> = {
  live: 'Live',
  done: 'Done',
  waiting: 'Waiting',
  running: 'Running',
  planned: 'Planned',
  failed: 'Failed',
  off: 'Offline',
};

export function Status({
  kind,
  children,
  title,
}: {
  kind: StatusKind;
  children?: ReactNode;
  title?: string;
}) {
  return (
    <span className={`status s-${kind}`} title={title}>
      <i aria-hidden="true" />
      {children ?? WORDS[kind]}
    </span>
  );
}

/** The bare square, for dense rows (rail dots use .ri-dot). */
export function StatusSquare({ kind, label }: { kind: StatusKind; label: string }) {
  return (
    <span className={`status s-${kind}`} role="img" aria-label={label} title={label}>
      <i aria-hidden="true" />
    </span>
  );
}
