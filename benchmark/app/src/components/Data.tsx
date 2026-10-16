// Pieces that show numbers: KPI tiles, fact lists, fact cards, meters and tables.
// Numbers are DM Mono with their unit in ink-3; words are Jost.

import type { ReactNode } from 'react';
import { fmtNumber, withUnit, DASH } from '../lib/format';
import { Icon, type IconName } from './Icon';

/** A KPI tile: Jost label, DM Mono 500 28/36 number, unit in ink-3 .annotation. */
export function Kpi({
  label,
  value,
  unit,
  digits,
  sub,
  pending,
  bad,
  large,
  title,
  bound,
}: {
  label: ReactNode;
  value: number | null | undefined;
  unit?: string;
  digits?: number;
  sub?: ReactNode;
  pending?: boolean;
  bad?: boolean;
  large?: boolean;
  title?: string;
  /** The value is only a lower bound: shown as "≥ value". */
  bound?: boolean;
}) {
  const num = fmtNumber(value ?? null, digits);
  const text = bound && num !== DASH ? `≥ ${num}` : num;
  const cls = ['kpi', pending ? 'pending' : '', bad ? 'bad' : '', large ? 'lg' : '']
    .filter(Boolean)
    .join(' ');
  return (
    <div className={cls} title={title}>
      <span className="k-label">{label}</span>
      <span className="k-value">
        <span className="k-num">{text}</span>
        {unit && text !== DASH ? <span className="k-unit annotation">{unit}</span> : null}
      </span>
      {sub ? <span className="k-sub caption">{sub}</span> : null}
    </div>
  );
}

/** `dense` for six or more tiles: a smaller number, so each fits its unit. */
export function KpiRow({ children, dense }: { children: ReactNode; dense?: boolean }) {
  return <div className={`kpi-row${dense ? ' dense' : ''}`}>{children}</div>;
}

/** A number and unit inline, e.g. inside a fact card. */
export function Figure({
  value,
  unit,
  digits,
}: {
  value: number | null | undefined;
  unit?: string;
  digits?: number;
}) {
  const text = fmtNumber(value ?? null, digits);
  return (
    <span className="kpi-inline">
      <span className="k-num">{text}</span>
      {unit && text !== DASH ? <span className="k-unit annotation">{unit}</span> : null}
    </span>
  );
}

export type FactRow = [label: ReactNode, value: ReactNode, mono?: boolean];

/** home.css .tc-facts: label left in ink-3, value right. Pass mono for numbers and specs. */
export function Facts({ rows }: { rows: FactRow[] }) {
  return (
    <dl className="facts">
      {rows.map(([k, v, mono], i) => (
        <div key={i}>
          <dt>{k}</dt>
          <dd className={mono ? 'num' : undefined}>{v ?? DASH}</dd>
        </div>
      ))}
    </dl>
  );
}

/**
 * home.css .tool-card with a .tc-head: icon, title, state. With `onClick` the title is the
 * button and the whole card is its click target (a stretched link): the value, sparkline and
 * facts stay plain content a screen reader reads, instead of a button's hidden children.
 */
export function FactCard({
  icon,
  title,
  state,
  children,
  className = '',
  onClick,
  label,
}: {
  icon?: IconName;
  title: ReactNode;
  state?: ReactNode;
  children?: ReactNode;
  className?: string;
  onClick?: () => void;
  /** What the title button does, for screen readers, e.g. "open"; read after the title. */
  label?: string;
}) {
  return (
    <article className={`card fact-card${onClick ? ' linked' : ''} ${className}`}>
      <div className="card-head">
        {icon ? <Icon name={icon} /> : <span />}
        <h3>
          {onClick ? (
            <button type="button" className="card-link" onClick={onClick}>
              {title}
              {label ? <span className="sr">{`, ${label}`}</span> : null}
            </button>
          ) : (
            title
          )}
        </h3>
        {state ?? null}
      </div>
      {children}
    </article>
  );
}

/**
 * A 4 px bar meter (home.css .charmeter) with what it measures beside it: words in Jost
 * (.caption), or with `mono` a number or spec in DM Mono.
 */
export function Meter({
  fraction,
  children,
  wide,
  bad,
  label,
  mono,
}: {
  fraction: number | null;
  children?: ReactNode;
  wide?: boolean;
  bad?: boolean;
  label?: string;
  mono?: boolean;
}) {
  const f = fraction == null ? 0 : Math.max(0, Math.min(1, fraction));
  return (
    <span
      className={`meter${wide ? ' wide' : ''}${bad ? ' bad' : ''}`}
      role="progressbar"
      aria-label={label}
      aria-valuemin={0}
      aria-valuemax={100}
      aria-valuenow={fraction == null ? undefined : Math.round(f * 100)}
    >
      <span className="bar" aria-hidden="true">
        <i style={{ width: `${f * 100}%` }} />
      </span>
      {children ? <span className={mono ? 'mono' : 'caption'}>{children}</span> : null}
    </span>
  );
}

export interface Column<T> {
  key: string;
  label: ReactNode;
  /** Right-aligned DM Mono. */
  num?: boolean;
  width?: number | string;
  render: (row: T) => ReactNode;
}

/** home.css .token-table */
export function Table<T>({
  columns,
  rows,
  rowKey,
  onRowClick,
  selected,
  rowClass,
  compact,
  caption,
  className = '',
}: {
  columns: Column<T>[];
  rows: T[];
  rowKey: (row: T) => string;
  onRowClick?: (row: T) => void;
  selected?: string | null;
  rowClass?: (row: T) => string;
  compact?: boolean;
  caption?: string;
  className?: string;
}) {
  return (
    <div className="table-wrap">
      <table className={`table${compact ? ' compact' : ''}${className ? ` ${className}` : ''}`}>
        {caption ? <caption className="sr">{caption}</caption> : null}
        <thead>
          <tr>
            {columns.map((c) => (
              <th
                key={c.key}
                className={c.num ? 'num' : undefined}
                style={c.width ? { width: c.width } : undefined}
              >
                {c.label}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => {
            const k = rowKey(r);
            const cls = [
              onRowClick ? 'clickable' : '',
              selected === k ? 'selected' : '',
              rowClass ? rowClass(r) : '',
            ]
              .filter(Boolean)
              .join(' ');
            return (
              <tr
                key={k}
                className={cls || undefined}
                onClick={onRowClick ? () => onRowClick(r) : undefined}
                onKeyDown={
                  onRowClick
                    ? (e) => {
                        if (e.target !== e.currentTarget) return;
                        if (e.key === 'Enter' || e.key === ' ') {
                          e.preventDefault();
                          onRowClick(r);
                        }
                      }
                    : undefined
                }
                tabIndex={onRowClick ? 0 : undefined}
                aria-selected={onRowClick && selected !== undefined ? selected === k : undefined}
              >
                {columns.map((c) => (
                  <td key={c.key} className={c.num ? 'num' : undefined}>
                    {c.render(r)}
                  </td>
                ))}
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

/** A value with its unit as plain text, for table cells. */
export function num(v: number | null | undefined, unit = '', digits?: number): string {
  return withUnit(fmtNumber(v ?? null, digits), unit);
}
