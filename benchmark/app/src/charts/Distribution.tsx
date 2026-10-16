// Distributions: a CDF and a histogram. The p50–p99 range is a blueline-wash band on the
// surface; under ⌥ the p50 and p99 guides are drawn and labelled in blueline-ink. Lost samples
// are counted in redline. A previous run is ink-3 dashed with outlined squares.

import { useMemo, useState } from 'react';
import { useTokens } from '../hooks/useTokens';
import { useWidth } from '../hooks/useSize';
import { useConstructionFlag } from './construction';
import { jy } from '../lib/jy';
import { ecdf, histogram, percentile, sortNumbers } from '../lib/stats';
import { fmtNumber, withUnit } from '../lib/format';
import {
  Axes,
  ChartCard,
  ChartTip,
  ConstructionGrid,
  Guide,
  LegendKey,
  MARGIN,
  PlotSurface,
  Squares,
  crisp,
  type Box,
} from './core';
import { linear, linePath, markerIndices, nearestIndex, niceTicks } from './scale';

export interface Quantile {
  /** 0..1 */
  p: number;
  v: number;
}

/** Known percentiles of a Distribution, as plottable points. */
export function quantilesOf(d: {
  min: number | null;
  p50: number | null;
  p90: number | null;
  p95: number | null;
  p99: number | null;
  p999: number | null;
  max: number | null;
}): Quantile[] {
  const out: Quantile[] = [];
  const add = (p: number, v: number | null) => {
    if (v != null && Number.isFinite(v)) out.push({ p, v });
  };
  add(0, d.min);
  add(0.5, d.p50);
  add(0.9, d.p90);
  add(0.95, d.p95);
  add(0.99, d.p99);
  add(0.999, d.p999);
  add(1, d.max);
  return out;
}

function stepPath(pts: [number, number][]): [number, number][] {
  // a staircase: hold each fraction until the next value
  const out: [number, number][] = [];
  for (let i = 0; i < pts.length; i++) {
    if (i > 0) out.push([pts[i][0], pts[i - 1][1]]);
    out.push(pts[i]);
  }
  return out;
}

export function CdfChart({
  title,
  meta,
  samples,
  quantiles,
  previous,
  unit = 'ms',
  lost = 0,
  height = 200,
  label = 'This run',
  measure,
}: {
  title?: string;
  meta?: string;
  /** Raw samples (preferred). +∞ marks a lost sample. */
  samples?: number[];
  /** Known percentiles when raw samples are not available. */
  quantiles?: Quantile[];
  previous?: { label: string; samples?: number[]; quantiles?: Quantile[] } | null;
  unit?: string;
  lost?: number;
  height?: number;
  label?: string;
  measure?: string;
}) {
  const t = useTokens();
  const construction = useConstructionFlag();
  const [ref, width] = useWidth<HTMLDivElement>();
  const [hover, setHover] = useState<number | null>(null);

  const cur = useMemo(() => toPoints(samples, quantiles), [samples, quantiles]);
  const prev = useMemo(
    () => (previous ? toPoints(previous.samples, previous.quantiles) : null),
    [previous],
  );
  const sorted = useMemo(() => (samples ? sortNumbers(samples) : null), [samples]);
  const p50 = sorted ? percentile(sorted, 0.5) : (quantiles?.find((q) => q.p === 0.5)?.v ?? null);
  const p99 = sorted ? percentile(sorted, 0.99) : (quantiles?.find((q) => q.p === 0.99)?.v ?? null);

  const box: Box = {
    x: MARGIN.left,
    y: MARGIN.top,
    w: Math.max(40, width - MARGIN.left - MARGIN.right),
    h: Math.max(40, height - MARGIN.top - MARGIN.bottom),
  };
  const vals = [...cur.pts, ...(prev?.pts ?? [])].map((p) => p[0]);
  const xTicks = niceTicks(
    Math.min(0, ...vals),
    Math.max(...vals, 1),
    Math.max(3, Math.floor(box.w / 80)),
  );
  const yTicks = niceTicks(0, 100, 4);
  const x = linear(xTicks.min, xTicks.max, box.x, box.x + box.w);
  const y = linear(0, 100, box.y + box.h, box.y);
  const px = (p: [number, number]): [number, number] => [x(p[0]), y(p[1] * 100)];
  const J = jy();

  const curPx = (cur.steps ? stepPath(cur.pts) : cur.pts).map(px);
  const prevPx = prev ? (prev.steps ? stepPath(prev.pts) : prev.pts).map(px) : [];
  const curMarks = cur.steps
    ? markerIndices(cur.pts.length, 10).map((i) => px(cur.pts[i]))
    : cur.pts.map(px);
  const prevMarks = prev
    ? prev.steps
      ? markerIndices(prev.pts.length, 10).map((i) => px(prev.pts[i]))
      : prev.pts.map(px)
    : [];

  const legend = prev ? (
    <>
      <LegendKey t={t} tone="ink" stroke="solid" label={label} />
      <LegendKey t={t} tone="ink3" stroke="dashed" label={previous?.label} outlined />
    </>
  ) : null;

  const hv = hover != null ? cur.pts[hover] : null;

  return (
    <ChartCard title={title} meta={meta} legend={legend} measure={measure}>
      <div className="chart-plot" ref={ref} style={{ height }}>
        <svg
          width={width}
          height={height}
          role="img"
          aria-label={`${title ?? 'Distribution'} in ${unit}`}
        >
          <PlotSurface t={t} box={box} y={y} yTicks={yTicks} />
          {p50 != null && p99 != null && p99 > p50 ? (
            <rect
              x={x(p50)}
              y={box.y + 1}
              width={Math.max(1, x(p99) - x(p50))}
              height={box.h - 2}
              fill={t.bluelineWash}
            />
          ) : null}
          <Axes
            t={t}
            box={box}
            x={x}
            xTicks={xTicks}
            xUnit={unit}
            y={y}
            yTicks={yTicks}
            yFormat={(v) => `${v}%`}
          />
          {prev ? (
            <g>
              <path
                d={linePath(prevPx)}
                fill="none"
                stroke={t.ink3}
                strokeWidth={1.2}
                strokeDasharray="5 4"
              />
              <Squares pts={prevMarks} color={t.ink3} fill={t.groundRaised} />
            </g>
          ) : null}
          <path
            d={linePath(curPx)}
            fill="none"
            stroke={t.ink}
            strokeWidth={1.5}
            strokeLinejoin="miter"
          />
          <Squares pts={curMarks} color={t.ink} size={4} />
          {lost > 0 ? (
            <g>
              <Squares pts={[[box.x + box.w - 8, box.y + 8]]} color={t.redline} size={6} />
              <text
                className="annotation"
                x={box.x + box.w - 16}
                y={box.y + 12}
                textAnchor="end"
                fill={t.redline}
              >
                {lost} lost
              </text>
            </g>
          ) : null}
          {hv ? (
            <line
              x1={crisp(px(hv)[0])}
              x2={crisp(px(hv)[0])}
              y1={box.y}
              y2={box.y + box.h}
              stroke={t.ruleStrong}
            />
          ) : null}
          <rect
            x={box.x}
            y={box.y}
            width={box.w}
            height={box.h}
            fill="transparent"
            onPointerMove={(e) => {
              if (!cur.pts.length) return;
              const r = (e.currentTarget.ownerSVGElement as SVGSVGElement).getBoundingClientRect();
              setHover(
                nearestIndex(
                  cur.pts.map((p) => p[0]),
                  x.invert(e.clientX - r.left),
                ),
              );
            }}
            onPointerLeave={() => setHover(null)}
          />
        </svg>
        {construction && cur.pts.length ? (
          <div className="chart-construction" aria-hidden="true">
            <svg width={width} height={height}>
              <ConstructionGrid t={t} box={box} x={x} xTicks={xTicks} y={y} yTicks={yTicks} />
              {p50 != null ? (
                <Guide
                  t={t}
                  box={box}
                  axis="x"
                  at={x(p50)}
                  label={`p50 ${withUnit(fmtNumber(p50), unit)}`}
                  anchor="end"
                />
              ) : null}
              {p99 != null ? (
                <Guide
                  t={t}
                  box={box}
                  axis="x"
                  at={x(p99)}
                  label={`p99 ${withUnit(fmtNumber(p99), unit)}`}
                />
              ) : null}
            </svg>
            {J?.Measure && p50 != null && p99 != null && x(p99) - x(p50) > 24 ? (
              <J.Measure
                style={{ position: 'absolute', left: x(p50), top: box.y + box.h / 2 }}
                length={x(p99) - x(p50)}
                label={withUnit(fmtNumber(p99 - p50), unit)}
                title="p50 to p99"
              />
            ) : null}
          </div>
        ) : null}
        {hv ? (
          <ChartTip
            left={px(hv)[0]}
            top={box.y + 4}
            flip={px(hv)[0] > box.x + box.w * 0.6}
            title={`${fmtNumber(hv[1] * 100, 1)}% of samples`}
            rows={[{ key: 'v', value: `≤ ${withUnit(fmtNumber(hv[0]), unit)}`, label }]}
          />
        ) : null}
      </div>
    </ChartCard>
  );
}

function toPoints(
  samples?: number[],
  quantiles?: Quantile[],
): { pts: [number, number][]; steps: boolean } {
  if (samples && samples.length) return { pts: ecdf(sortNumbers(samples)), steps: true };
  if (quantiles && quantiles.length)
    return { pts: quantiles.map((q) => [q.v, q.p] as [number, number]), steps: false };
  return { pts: [], steps: false };
}

export function Histogram({
  title,
  meta,
  samples,
  unit = 'ms',
  bins = 24,
  height = 180,
  measure,
}: {
  title?: string;
  meta?: string;
  samples: number[];
  unit?: string;
  bins?: number;
  height?: number;
  measure?: string;
}) {
  const t = useTokens();
  const construction = useConstructionFlag();
  const [ref, width] = useWidth<HTMLDivElement>();
  const sorted = useMemo(() => sortNumbers(samples), [samples]);
  const finite = sorted.filter(Number.isFinite);
  const lost = sorted.length - finite.length;
  const box: Box = {
    x: MARGIN.left,
    y: MARGIN.top,
    w: Math.max(40, width - MARGIN.left - MARGIN.right),
    h: Math.max(40, height - MARGIN.top - MARGIN.bottom),
  };
  const lo = finite.length ? finite[0] : 0;
  const hi = finite.length ? finite[finite.length - 1] : 1;
  const xTicks = niceTicks(lo, hi, Math.max(3, Math.floor(box.w / 80)));
  const hist = finite.length ? histogram(finite, bins, xTicks.min, xTicks.max) : [];
  const cMax = Math.max(1, ...hist.map((b) => b.count));
  const yTicks = niceTicks(0, cMax, 3);
  const x = linear(xTicks.min, xTicks.max, box.x, box.x + box.w);
  const y = linear(0, yTicks.max, box.y + box.h, box.y);
  const p50 = percentile(sorted, 0.5);
  const p99 = percentile(sorted, 0.99);
  return (
    <ChartCard title={title} meta={meta} measure={measure}>
      <div className="chart-plot" ref={ref} style={{ height }}>
        <svg
          width={width}
          height={height}
          role="img"
          aria-label={`${title ?? 'Histogram'} in ${unit}`}
        >
          <PlotSurface t={t} box={box} y={y} yTicks={yTicks} />
          {hist.map((b, i) => {
            const x0 = x(b.x0) + 0.5;
            const x1 = x(b.x1) - 0.5;
            return b.count ? (
              <rect
                key={i}
                x={x0}
                y={y(b.count)}
                width={Math.max(1, x1 - x0)}
                height={y(0) - y(b.count)}
                fill={t.ink}
              />
            ) : null;
          })}
          <Axes t={t} box={box} x={x} xTicks={xTicks} xUnit={unit} y={y} yTicks={yTicks} />
          {lost > 0 ? (
            <text
              className="annotation"
              x={box.x + box.w - 4}
              y={box.y + 12}
              textAnchor="end"
              fill={t.redline}
            >
              {lost} lost
            </text>
          ) : null}
        </svg>
        {construction && finite.length ? (
          <div className="chart-construction" aria-hidden="true">
            <svg width={width} height={height}>
              <ConstructionGrid t={t} box={box} x={x} xTicks={xTicks} y={y} yTicks={yTicks} />
              {p50 != null ? (
                <Guide
                  t={t}
                  box={box}
                  axis="x"
                  at={x(p50)}
                  label={`p50 ${withUnit(fmtNumber(p50), unit)}`}
                  anchor="end"
                />
              ) : null}
              {p99 != null ? (
                <Guide
                  t={t}
                  box={box}
                  axis="x"
                  at={x(p99)}
                  label={`p99 ${withUnit(fmtNumber(p99), unit)}`}
                />
              ) : null}
            </svg>
          </div>
        ) : null}
      </div>
    </ChartCard>
  );
}
