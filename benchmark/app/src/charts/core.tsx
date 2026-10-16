// Shared chart anatomy (RFC 027 §11.4).
//
// Surface, always drawn: the plot area is ground-raised with a 1 px rule edge, gridlines are
// 1 px rule, the axis is 1 px rule-strong, ticks are ink-3 .annotation. Series are ink, ink-2
// and ink-3, told apart by stroke: solid 1.5, dashed 5 4, dotted 2 4. Markers are squares
// only: filled for this run, outlined for a previous run or baseline.
//
// Construction, only while ⌥ is held (fades in over 240 ms): a blueline grid, JY.Measure
// dimension lines and p50/p99 guides labelled in blueline-ink .annotation. Every number a
// guide shows is also on the surface (KPI row and tables).

import type { ReactNode } from 'react';
import type { Tokens } from '../hooks/useTokens';
import { fmtTick, type Scale, type Ticks } from './scale';

export type Tone = 'ink' | 'ink2' | 'ink3';
export type Stroke = 'solid' | 'dashed' | 'dotted';

export const DASHES: Record<Stroke, string | undefined> = {
  solid: undefined,
  dashed: '5 4',
  dotted: '2 4',
};

/** Series i gets the i-th tone and stroke. */
export const SERIES_STYLES: { tone: Tone; stroke: Stroke }[] = [
  { tone: 'ink', stroke: 'solid' },
  { tone: 'ink2', stroke: 'dashed' },
  { tone: 'ink3', stroke: 'dotted' },
];

export function toneColor(t: Tokens, tone: Tone): string {
  return tone === 'ink' ? t.ink : tone === 'ink2' ? t.ink2 : t.ink3;
}

export const MARGIN = { top: 24, right: 14, bottom: 26, left: 48 };

export interface Box {
  x: number;
  y: number;
  w: number;
  h: number;
}

/** The chart card: a title row with the legend, then the plot. */
export function ChartCard({
  title,
  meta,
  legend,
  right,
  children,
  className = '',
  measure,
}: {
  title?: ReactNode;
  meta?: ReactNode;
  legend?: ReactNode;
  right?: ReactNode;
  children: ReactNode;
  className?: string;
  measure?: string;
}) {
  return (
    <figure className={`chart ${className}`} data-measure={measure}>
      {title || legend || right ? (
        <figcaption className="chart-head">
          <span className="ch-title">
            {title ? <span className="ch-name">{title}</span> : null}
            {meta ? <span className="ch-meta caption">{meta}</span> : null}
          </span>
          {legend ? <span className="ch-legend">{legend}</span> : null}
          {right ? <span className="ch-right">{right}</span> : null}
        </figcaption>
      ) : null}
      {children}
    </figure>
  );
}

/** A legend entry: the line key mirrors the mark (a short stroke, with its square). */
export function LegendKey({
  t,
  tone,
  stroke,
  label,
  outlined,
  swatch,
}: {
  t: Tokens;
  tone: Tone;
  stroke: Stroke;
  label: ReactNode;
  outlined?: boolean;
  /** Draw a filled box instead of a line (bars, bands). */
  swatch?: 'fill' | 'outline' | 'dashed' | 'wash' | 'hatch' | 'red' | 'sunken';
}) {
  const c = toneColor(t, tone);
  return (
    <span className="lg-item">
      <svg width={swatch ? 12 : 24} height={12} aria-hidden="true">
        {swatch === 'fill' ? <rect x={0.5} y={0.5} width={11} height={11} fill={c} /> : null}
        {swatch === 'red' ? <rect x={2} y={2} width={8} height={8} fill={t.redline} /> : null}
        {swatch === 'outline' ? (
          <rect
            x={0.75}
            y={0.75}
            width={10.5}
            height={10.5}
            fill="none"
            stroke={c}
            strokeWidth={1.5}
          />
        ) : null}
        {swatch === 'dashed' ? (
          <rect
            x={0.5}
            y={0.5}
            width={11}
            height={11}
            fill="none"
            stroke={t.ruleStrong}
            strokeDasharray="3 2"
          />
        ) : null}
        {swatch === 'sunken' ? (
          <g>
            <rect x={0.5} y={0.5} width={11} height={11} fill={t.groundSunken} stroke={t.rule} />
            <line x1={11.5} x2={11.5} y1={0} y2={12} stroke={t.ruleStrong} strokeDasharray="2 2" />
          </g>
        ) : null}
        {swatch === 'wash' ? (
          <rect x={0.5} y={0.5} width={11} height={11} fill={t.bluelineWash} stroke={t.rule} />
        ) : null}
        {swatch === 'hatch' ? (
          <g>
            <rect x={0.5} y={0.5} width={11} height={11} fill="none" stroke={t.ink2} />
            <path d="M0.5 8.5L8.5 0.5M3.5 11.5L11.5 3.5" stroke={t.ink2} />
          </g>
        ) : null}
        {!swatch ? (
          <>
            <line
              x1={0}
              x2={24}
              y1={6}
              y2={6}
              stroke={c}
              strokeWidth={1.5}
              strokeDasharray={DASHES[stroke]}
            />
            <rect
              x={9}
              y={3.5}
              width={5}
              height={5}
              fill={outlined ? t.groundRaised : c}
              stroke={c}
              strokeWidth={outlined ? 1.2 : 0}
            />
          </>
        ) : null}
      </svg>
      <span>{label}</span>
    </span>
  );
}

/** Plot background and gridlines (surface). */
export function PlotSurface({
  t,
  box,
  y,
  yTicks,
  x,
  xTicks,
}: {
  t: Tokens;
  box: Box;
  y?: Scale;
  yTicks?: Ticks;
  x?: Scale;
  xTicks?: Ticks;
}) {
  return (
    <g>
      <rect
        x={box.x + 0.5}
        y={box.y + 0.5}
        width={box.w - 1}
        height={box.h - 1}
        fill={t.groundRaised}
        stroke={t.rule}
      />
      {y && yTicks
        ? yTicks.ticks.map((v) => (
            <line
              key={`gy${v}`}
              x1={box.x}
              x2={box.x + box.w}
              y1={crisp(y(v))}
              y2={crisp(y(v))}
              stroke={t.rule}
            />
          ))
        : null}
      {x && xTicks
        ? xTicks.ticks.map((v) =>
            v === xTicks.min ? null : (
              <line
                key={`gx${v}`}
                y1={box.y}
                y2={box.y + box.h}
                x1={crisp(x(v))}
                x2={crisp(x(v))}
                stroke={t.rule}
                strokeOpacity={0.6}
              />
            ),
          )
        : null}
    </g>
  );
}

/** Axes in rule-strong with ink-3 .annotation tick labels. */
export function Axes({
  t,
  box,
  x,
  xTicks,
  xFormat,
  y,
  yTicks,
  yFormat,
  xUnit,
  yUnit,
}: {
  t: Tokens;
  box: Box;
  x?: Scale;
  xTicks?: Ticks;
  xFormat?: (v: number) => string;
  y?: Scale;
  yTicks?: Ticks;
  yFormat?: (v: number) => string;
  xUnit?: string;
  yUnit?: string;
}) {
  const bottom = box.y + box.h;
  return (
    <g className="annotation" fill={t.ink3}>
      <line
        x1={box.x}
        x2={box.x + box.w}
        y1={crisp(bottom)}
        y2={crisp(bottom)}
        stroke={t.ruleStrong}
      />
      <line x1={crisp(box.x)} x2={crisp(box.x)} y1={box.y} y2={bottom} stroke={t.ruleStrong} />
      {x && xTicks
        ? xTicks.ticks.map((v, i) => {
            const px = x(v);
            const atStart = px - box.x < 12;
            const atEnd = box.x + box.w - px < 24;
            const last = i === xTicks.ticks.length - 1;
            return (
              <g key={`tx${v}`}>
                <line
                  x1={crisp(px)}
                  x2={crisp(px)}
                  y1={bottom}
                  y2={bottom + 4}
                  stroke={t.ruleStrong}
                />
                <text
                  x={px}
                  y={bottom + 16}
                  textAnchor={atStart ? 'start' : atEnd ? 'end' : 'middle'}
                >
                  {(xFormat ?? ((n: number) => fmtTick(n, xTicks.step)))(v)}
                  {xUnit && last ? ` ${xUnit}` : ''}
                </text>
              </g>
            );
          })
        : null}
      {y && yTicks
        ? yTicks.ticks.map((v) => (
            <text key={`ty${v}`} x={box.x - 6} y={y(v) + 4} textAnchor="end">
              {(yFormat ?? ((n: number) => fmtTick(n, yTicks.step)))(v)}
            </text>
          ))
        : null}
      {yUnit ? (
        <text x={box.x - 6} y={box.y - 12} textAnchor="end">
          {yUnit}
        </text>
      ) : null}
    </g>
  );
}

/** A blueline grid at half-tick spacing, drawn only in the construction layer. */
export function ConstructionGrid({
  t,
  box,
  x,
  xTicks,
  y,
  yTicks,
}: {
  t: Tokens;
  box: Box;
  x?: Scale;
  xTicks?: Ticks;
  y?: Scale;
  yTicks?: Ticks;
}) {
  const lines: ReactNode[] = [];
  if (y && yTicks) {
    for (let v = yTicks.min; v <= yTicks.max + 1e-9; v += yTicks.step / 2) {
      lines.push(
        <line key={`cy${v}`} x1={box.x} x2={box.x + box.w} y1={crisp(y(v))} y2={crisp(y(v))} />,
      );
    }
  }
  if (x && xTicks) {
    for (let v = xTicks.min; v <= xTicks.max + 1e-9; v += xTicks.step / 2) {
      lines.push(
        <line key={`cx${v}`} y1={box.y} y2={box.y + box.h} x1={crisp(x(v))} x2={crisp(x(v))} />,
      );
    }
  }
  return (
    <g stroke={t.blueline} strokeWidth={1} opacity={0.7}>
      {lines}
    </g>
  );
}

/** A guide line with its label, construction layer only. */
export function Guide({
  t,
  box,
  axis,
  at,
  label,
  anchor = 'start',
}: {
  t: Tokens;
  box: Box;
  axis: 'x' | 'y';
  at: number;
  label: string;
  anchor?: 'start' | 'end';
}) {
  if (!Number.isFinite(at)) return null;
  if (axis === 'x') {
    const xx = crisp(at);
    return (
      <g>
        <line x1={xx} x2={xx} y1={box.y} y2={box.y + box.h} stroke={t.blueline} strokeWidth={1} />
        <text
          className="annotation"
          x={anchor === 'start' ? xx + 4 : xx - 4}
          y={box.y + 12}
          textAnchor={anchor}
          fill={t.bluelineInk}
          stroke={t.groundRaised}
          strokeWidth={3}
          paintOrder="stroke"
        >
          {label}
        </text>
      </g>
    );
  }
  const yy = crisp(at);
  return (
    <g>
      <line x1={box.x} x2={box.x + box.w} y1={yy} y2={yy} stroke={t.blueline} strokeWidth={1} />
      <text
        className="annotation"
        x={anchor === 'end' ? box.x + box.w - 4 : box.x + 4}
        y={yy - 4}
        textAnchor={anchor}
        fill={t.bluelineInk}
        stroke={t.groundRaised}
        strokeWidth={3}
        paintOrder="stroke"
      >
        {label}
      </text>
    </g>
  );
}

/** Square markers: filled for this run, outlined for a previous one. Never circles or triangles. */
export function Squares({
  pts,
  color,
  fill,
  size = 5,
}: {
  pts: [number, number][];
  color: string;
  /** Fill colour for outlined markers (the plot ground); omit for filled. */
  fill?: string;
  size?: number;
}) {
  const h = size / 2;
  return (
    <g>
      {pts.map(([x, y], i) =>
        Number.isFinite(x) && Number.isFinite(y) ? (
          <rect
            key={i}
            x={x - h}
            y={y - h}
            width={size}
            height={size}
            fill={fill ?? color}
            stroke={fill ? color : 'none'}
            strokeWidth={fill ? 1.2 : 0}
          />
        ) : null,
      )}
    </g>
  );
}

/** Pixel-crisp 1 px lines. */
export function crisp(v: number): number {
  return Math.round(v) + 0.5;
}

/** A hatch pattern (relay segments), defined once per SVG. */
export function HatchDef({ id, color }: { id: string; color: string }) {
  return (
    <defs>
      <pattern
        id={id}
        width={5}
        height={5}
        patternUnits="userSpaceOnUse"
        patternTransform="rotate(45)"
      >
        <line x1={0} y1={0} x2={0} y2={5} stroke={color} strokeWidth={1.5} />
      </pattern>
    </defs>
  );
}

/** A floating readout over a chart: values lead, labels follow. */
export function ChartTip({
  left,
  top,
  title,
  rows,
  flip,
}: {
  left: number;
  top: number;
  title: ReactNode;
  rows: { key: string; value: ReactNode; label: ReactNode; keyEl?: ReactNode }[];
  flip?: boolean;
}) {
  return (
    <div
      className="chart-tip"
      style={{ left, top, transform: flip ? 'translateX(calc(-100% - 12px))' : 'translateX(12px)' }}
      role="status"
    >
      <div className="ct-title annotation">{title}</div>
      {rows.map((r) => (
        <div key={r.key} className="ct-row">
          {r.keyEl ?? null}
          <span className="ct-value spec">{r.value}</span>
          <span className="ct-label">{r.label}</span>
        </div>
      ))}
    </div>
  );
}
