// Tooltip, keyboard key, toast, page scaffolding and the JY identity pieces.

import { useState, type ReactNode } from 'react';
import { jy } from '../lib/jy';

/** A tooltip on hover and focus: ground-raised, rule-strong edge, no shadow. */
export function Tooltip({
  tip,
  children,
  below,
  block,
}: {
  tip: ReactNode;
  children: ReactNode;
  below?: boolean;
  /** Take the full width (around a wide button). */
  block?: boolean;
}) {
  const [open, setOpen] = useState(false);
  return (
    <span
      className={`tip-anchor${block ? ' block' : ''}`}
      onMouseEnter={() => setOpen(true)}
      onMouseLeave={() => setOpen(false)}
      onFocus={() => setOpen(true)}
      onBlur={() => setOpen(false)}
    >
      {children}
      {open && tip ? (
        <span className={`tip${below ? ' below' : ''}`} role="tooltip">
          {tip}
        </span>
      ) : null}
    </span>
  );
}

export function Kbd({ children }: { children: ReactNode }) {
  const J = jy();
  return J?.Kbd ? <J.Kbd>{children}</J.Kbd> : <kbd className="kbd">{children}</kbd>;
}

/** "Run ⌘ R": a label and its keys, for tooltips. */
export function KbdHint({ label, keys }: { label?: ReactNode; keys: string[] }) {
  return (
    <span className="kbd-hint">
      {label ? <span>{label}</span> : null}
      <span className="kbd-keys">
        {keys.map((k) => (
          <Kbd key={k}>{k}</Kbd>
        ))}
      </span>
    </span>
  );
}

/**
 * The toast, and what a screen reader hears of it. The live regions stay mounted while the text
 * changes (a region that mounts already holding its text is often not read), and an error is
 * assertive. The visible toast is keyed per toast so its entrance replays.
 */
export function Toasts({
  toast,
}: {
  toast: { id: number; text: string; kind: 'info' | 'error' } | null;
}) {
  return (
    <>
      <div className="sr" role="status" aria-live="polite">
        {toast?.kind === 'info' ? toast.text : ''}
      </div>
      <div className="sr" role="alert" aria-live="assertive">
        {toast?.kind === 'error' ? toast.text : ''}
      </div>
      {toast ? (
        <div
          key={toast.id}
          className={`toast${toast.kind === 'error' ? ' error' : ''}`}
          aria-hidden="true"
        >
          {toast.text}
        </div>
      ) : null}
    </>
  );
}

/** The row mark from the identity's own geometry. Never redrawn. */
export function Mark({
  size = 16,
  layout = 'row',
  reveal,
  title = '',
}: {
  size?: number;
  layout?: 'row' | 'wrap' | 'micro';
  reveal?: boolean;
  title?: string;
}) {
  const J = jy();
  if (!J?.Mark) return null;
  return (
    <J.Mark
      size={size}
      layout={layout}
      reveal={reveal}
      title={title}
      aria-hidden={title ? undefined : true}
    />
  );
}

export function Loader({ size = 8, label }: { size?: number; label: string }) {
  const J = jy();
  return J?.Loader ? <J.Loader size={size} label={label} /> : <span className="sr">{label}</span>;
}

/**
 * A document page (home.css .page-wrap/.page/.page-head). While ⌥ is held the identity's
 * Blueprint draws the grid and measures every block marked data-measure.
 */
export function Page({
  eyebrow,
  title,
  lede,
  actions,
  construction,
  children,
}: {
  eyebrow?: ReactNode;
  title: ReactNode;
  lede?: ReactNode;
  actions?: ReactNode;
  construction: boolean;
  children: ReactNode;
}) {
  const J = jy();
  const inner = (
    <>
      <header className="page-head" data-measure="header">
        <div className="ph-text">
          {eyebrow ? <span className="label">{eyebrow}</span> : null}
          <h1>{title}</h1>
          {lede ? <p className="lede">{lede}</p> : null}
        </div>
        {actions ? <div className="ph-actions">{actions}</div> : null}
      </header>
      {children}
    </>
  );
  return (
    <div className="page-wrap">
      {J?.Blueprint ? (
        <J.Blueprint
          as="div"
          className="page"
          show={construction}
          hotkey={false}
          columns={12}
          gutter={24}
          margin={0}
        >
          {inner}
        </J.Blueprint>
      ) : (
        <div className="page">{inner}</div>
      )}
    </div>
  );
}

/** home.css .sec: a label, mono meta, actions on the right, a rule under. */
export function Section({
  label,
  meta,
  actions,
  children,
  measure,
  className = '',
}: {
  label: ReactNode;
  meta?: ReactNode;
  actions?: ReactNode;
  children?: ReactNode;
  measure?: string;
  className?: string;
}) {
  return (
    <section
      className={`sec ${className}`}
      data-measure={measure ?? (typeof label === 'string' ? label : undefined)}
    >
      <div className="sec-head">
        <h2 className="label">{label}</h2>
        {meta ? <span className="caption muted sec-meta">{meta}</span> : null}
        {actions ? <span className="sec-actions">{actions}</span> : null}
      </div>
      {children}
    </section>
  );
}
