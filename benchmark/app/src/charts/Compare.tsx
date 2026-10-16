// Comparison bars and sparklines.
//
// BarCompare: this run is an ink fill, a previous run a 1.5 px ink outline (filled versus
// outlined, as the status squares), a planned item a dashed outline. Bars are separated by
// ground. Sparkline: 1 px ink-2, no axes, a 3 px square on the last point.

import { useTokens } from '../hooks/useTokens';
import { useWidth } from '../hooks/useSize';
import { fmtNumber, withUnit } from '../lib/format';
import { ChartCard, LegendKey } from './core';
import { linear, linePath } from './scale';

export interface BarItem {
  key: string;
  label: string;
  sub?: string;
  value: number | null;
  kind: 'current' | 'previous' | 'planned';
}

export function BarCompare({
  title,
  meta,
  items,
  unit,
  digits,
  measure,
  kindLabels,
}: {
  title?: string;
  meta?: string;
  items: BarItem[];
  unit: string;
  digits?: number;
  measure?: string;
  /** Legend words for the fill and the outline, when they aren't "This run" and "Earlier". */
  kindLabels?: { current?: string; previous?: string };
}) {
  const t = useTokens();
  const [ref, width] = useWidth<HTMLDivElement>();
  // each row: the label and its value on one line, the bar under them at full width
  const max = Math.max(1e-9, ...items.map((i) => i.value ?? 0));
  const x = linear(0, max, 0, Math.max(10, width - 1));
  const rowH = 38;
  const height = items.length * rowH;
  const kinds = new Set(items.map((i) => i.kind));
  const legend =
    kinds.size > 1 ? (
      <>
        {kinds.has('current') ? (
          <LegendKey
            t={t}
            tone="ink"
            stroke="solid"
            label={kindLabels?.current ?? 'This run'}
            swatch="fill"
          />
        ) : null}
        {kinds.has('previous') ? (
          <LegendKey
            t={t}
            tone="ink"
            stroke="solid"
            label={kindLabels?.previous ?? 'Earlier'}
            swatch="outline"
          />
        ) : null}
        {kinds.has('planned') ? (
          <LegendKey t={t} tone="ink3" stroke="solid" label="Planned" swatch="dashed" />
        ) : null}
      </>
    ) : null;
  return (
    <ChartCard title={title} meta={meta} legend={legend} measure={measure}>
      <div className="chart-plot bars" ref={ref} style={{ height }}>
        <svg
          width={width}
          height={height}
          role="img"
          aria-label={`${title ?? 'Comparison'} in ${unit}`}
        >
          {items.map((it, i) => {
            const top = i * rowH;
            const w = it.value != null ? Math.max(1, x(it.value)) : 0;
            const by = top + 21;
            const bh = 10;
            const dim = it.kind === 'planned';
            return (
              <g key={it.key}>
                <text
                  x={0}
                  y={top + 13}
                  fill={dim ? t.ink3 : t.ink2}
                  fontSize={13}
                  fontFamily="var(--font-sans)"
                >
                  {it.label}
                  {it.sub ? (
                    <tspan fill={t.ink3} dx={6}>
                      {it.sub}
                    </tspan>
                  ) : null}
                </text>
                <text
                  x={width}
                  y={top + 13}
                  textAnchor="end"
                  className="spec"
                  fill={dim ? t.ink3 : t.ink}
                >
                  {dim ? 'planned' : withUnit(fmtNumber(it.value, digits), unit)}
                </text>
                <rect
                  x={0.5}
                  y={by + 0.5}
                  width={Math.max(0, width - 1)}
                  height={bh - 1}
                  fill="none"
                  stroke={t.rule}
                />
                {it.kind === 'current' ? (
                  <rect x={0} y={by} width={w} height={bh} fill={t.ink} />
                ) : null}
                {it.kind === 'previous' ? (
                  <rect
                    x={0.75}
                    y={by + 0.75}
                    width={Math.max(0, w - 1.5)}
                    height={bh - 1.5}
                    fill={t.groundRaised}
                    stroke={t.ink}
                    strokeWidth={1.5}
                  />
                ) : null}
                {it.kind === 'planned' ? (
                  <rect
                    x={0.5}
                    y={by + 0.5}
                    width={Math.max(0, width * 0.35)}
                    height={bh - 1}
                    fill="none"
                    stroke={t.ruleStrong}
                    strokeDasharray="5 4"
                  />
                ) : null}
              </g>
            );
          })}
        </svg>
      </div>
    </ChartCard>
  );
}

export function Sparkline({
  values,
  width = 96,
  height = 24,
  label,
}: {
  values: number[];
  width?: number;
  height?: number;
  label?: string;
}) {
  const t = useTokens();
  const v = values.filter(Number.isFinite);
  if (v.length === 0) return <svg width={width} height={height} aria-hidden="true" />;
  const lo = Math.min(...v);
  const hi = Math.max(...v);
  const pad = 2;
  const x = linear(0, Math.max(1, v.length - 1), pad, width - pad);
  const y = hi > lo ? linear(lo, hi, height - pad, pad) : () => height / 2;
  const pts = v.map((val, i) => [x(i), y(val)] as [number, number]);
  const last = pts[pts.length - 1];
  return (
    <svg
      className="sparkline"
      width={width}
      height={height}
      role="img"
      aria-label={label ?? `Last ${v.length} runs`}
    >
      <path d={linePath(pts)} fill="none" stroke={t.ink2} strokeWidth={1} strokeLinejoin="miter" />
      <rect x={last[0] - 1.5} y={last[1] - 1.5} width={3} height={3} fill={t.ink} />
    </svg>
  );
}

/** A sparkline that fills its container's width. */
export function FluidSparkline({
  values,
  height = 28,
  label,
}: {
  values: number[];
  height?: number;
  label?: string;
}) {
  const [ref, width] = useWidth<HTMLDivElement>(120);
  return (
    <div ref={ref} className="spark-fluid" style={{ height }}>
      <Sparkline values={values} width={width} height={height} label={label} />
    </div>
  );
}
