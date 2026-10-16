// Bars for count-based workloads.
//
// IterationBars: one bar per file (file-transfer wire speed). This run's bars are ink fills;
// one left out of the median is a 1.5 px ink-2 outline; one still in flight is a dashed
// outline; a failed one is redline. A previous run's median is an ink-3 dashed level.
//
// PhaseBars: where the time went, one stacked bar per row (hash, offer, transfer). Segments
// are told apart without colour: ink fill, ink-2 hatch, ink-3 fill.

import { useId } from 'react';
import { useTokens } from '../hooks/useTokens';
import { useWidth } from '../hooks/useSize';
import { useConstructionFlag } from './construction';
import { fmtNumber, withUnit } from '../lib/format';
import {
  Axes,
  ChartCard,
  ConstructionGrid,
  DASHES,
  Guide,
  HatchDef,
  LegendKey,
  MARGIN,
  PlotSurface,
  crisp,
  type Box,
} from './core';
import { linear, niceTicks } from './scale';

export interface IterationBar {
  key: string;
  label: string;
  value: number | null;
  kind: 'current' | 'excluded' | 'running' | 'failed';
}

export function IterationBars({
  title,
  meta,
  items,
  unit,
  baseline,
  guide,
  height = 240,
  emptyText = 'No files yet.',
}: {
  title: string;
  meta?: string;
  items: IterationBar[];
  unit: string;
  /** A previous run's level, ink-3 dashed. */
  baseline?: { label: string; value: number } | null;
  /** Drawn under ⌥ only; the number is in the KPI row too. */
  guide?: { label: string; value: number } | null;
  height?: number;
  emptyText?: string;
}) {
  const t = useTokens();
  const construction = useConstructionFlag();
  const [ref, width] = useWidth<HTMLDivElement>();
  const box: Box = {
    x: MARGIN.left,
    y: MARGIN.top,
    w: Math.max(40, width - MARGIN.left - MARGIN.right),
    h: Math.max(40, height - MARGIN.top - MARGIN.bottom),
  };
  const vMax = Math.max(
    0,
    ...items.map((i) => i.value ?? 0),
    baseline?.value ?? 0,
    guide?.value ?? 0,
  );
  const yTicks = niceTicks(0, vMax > 0 ? vMax * 1.12 : 1, Math.max(3, Math.floor(box.h / 46)));
  const y = linear(yTicks.min, yTicks.max, box.y + box.h, box.y);
  const n = Math.max(1, items.length);
  const slot = box.w / n;
  const bw = Math.min(56, Math.max(6, slot * 0.56));
  const kinds = new Set(items.map((i) => i.kind));
  const legend = (
    <>
      <LegendKey t={t} tone="ink" stroke="solid" label="This run" swatch="fill" />
      {kinds.has('excluded') ? (
        <LegendKey
          t={t}
          tone="ink2"
          stroke="solid"
          label="Left out of the median"
          swatch="outline"
        />
      ) : null}
      {kinds.has('failed') ? (
        <LegendKey t={t} tone="ink" stroke="solid" label="Failed" swatch="red" />
      ) : null}
      {baseline ? (
        <LegendKey t={t} tone="ink3" stroke="dashed" label={baseline.label} outlined />
      ) : null}
    </>
  );
  return (
    <ChartCard title={title} meta={meta} legend={legend}>
      <div className="chart-plot" ref={ref} style={{ height }}>
        <svg
          width={width}
          height={height}
          role="img"
          aria-label={`${title}: ${items
            .map(
              (i) =>
                `${i.label} ${i.value == null ? 'pending' : withUnit(fmtNumber(i.value), unit)}`,
            )
            .join(', ')}`}
        >
          <PlotSurface t={t} box={box} y={y} yTicks={yTicks} />
          <Axes t={t} box={box} y={y} yTicks={yTicks} yUnit={unit} />
          {items.map((it, i) => {
            const cx = box.x + slot * (i + 0.5);
            const x0 = cx - bw / 2;
            const v = it.value ?? 0;
            const top = y(v);
            const h = Math.max(0, box.y + box.h - top);
            return (
              <g key={it.key}>
                {it.kind === 'current' ? (
                  <rect x={x0} y={top} width={bw} height={h} fill={t.ink} />
                ) : null}
                {it.kind === 'failed' ? (
                  <rect x={x0} y={top} width={bw} height={Math.max(2, h)} fill={t.redline} />
                ) : null}
                {it.kind === 'excluded' ? (
                  <rect
                    x={x0 + 0.75}
                    y={top + 0.75}
                    width={bw - 1.5}
                    height={Math.max(0, h - 1.5)}
                    fill={t.groundRaised}
                    stroke={t.ink2}
                    strokeWidth={1.5}
                  />
                ) : null}
                {it.kind === 'running' ? (
                  <rect
                    x={x0 + 0.5}
                    y={box.y + box.h * 0.55}
                    width={bw - 1}
                    height={box.h * 0.45 - 0.5}
                    fill="none"
                    stroke={t.ruleStrong}
                    strokeDasharray="3 3"
                  />
                ) : null}
                {it.value != null ? (
                  <text
                    className="annotation"
                    x={cx}
                    y={top - 6}
                    textAnchor="middle"
                    fill={it.kind === 'failed' ? t.redline : t.ink2}
                    stroke={t.groundRaised}
                    strokeWidth={3}
                    paintOrder="stroke"
                  >
                    {fmtNumber(it.value)}
                  </text>
                ) : null}
                <text
                  className="annotation"
                  x={cx}
                  y={box.y + box.h + 16}
                  textAnchor="middle"
                  fill={t.ink3}
                >
                  {it.label}
                </text>
              </g>
            );
          })}
          {baseline ? (
            <g>
              <line
                x1={box.x}
                x2={box.x + box.w}
                y1={crisp(y(baseline.value))}
                y2={crisp(y(baseline.value))}
                stroke={t.ink3}
                strokeWidth={1.2}
                strokeDasharray={DASHES.dashed}
              />
              <rect
                x={box.x + box.w - 10}
                y={y(baseline.value) - 2.5}
                width={5}
                height={5}
                fill={t.groundRaised}
                stroke={t.ink3}
                strokeWidth={1.2}
              />
            </g>
          ) : null}
          {!items.length ? (
            <text
              x={box.x + box.w / 2}
              y={box.y + box.h / 2}
              textAnchor="middle"
              fill={t.ink3}
              className="chart-empty"
            >
              {emptyText}
            </text>
          ) : null}
        </svg>
        {construction && items.length ? (
          <div className="chart-construction" aria-hidden="true">
            <svg width={width} height={height}>
              <ConstructionGrid t={t} box={box} y={y} yTicks={yTicks} />
              {guide ? (
                <Guide
                  t={t}
                  box={box}
                  axis="y"
                  at={y(guide.value)}
                  anchor="end"
                  label={`${guide.label} ${withUnit(fmtNumber(guide.value), unit)}`}
                />
              ) : null}
            </svg>
          </div>
        ) : null}
      </div>
    </ChartCard>
  );
}

export interface PhaseSegmentMeta {
  key: string;
  label: string;
  fill: 'ink' | 'hatch' | 'ink3';
}

export interface PhaseRow {
  key: string;
  label: string;
  sub?: string;
  values: Record<string, number | null>;
  /** Draw this row quieter (left out of the summary). */
  dim?: boolean;
}

export function PhaseBars({
  title,
  meta,
  segments,
  rows,
  unit = 'ms',
  emptyText = 'After a run.',
}: {
  title: string;
  meta?: string;
  segments: PhaseSegmentMeta[];
  rows: PhaseRow[];
  unit?: string;
  emptyText?: string;
}) {
  const t = useTokens();
  const [ref, width] = useWidth<HTMLDivElement>();
  const id = `ph-${useId().replace(/[^a-zA-Z0-9-]/g, '')}`;
  const totals = rows.map((r) => segments.reduce((a, s) => a + (r.values[s.key] ?? 0), 0));
  const max = Math.max(1e-9, ...totals);
  const x = linear(0, max, 0, Math.max(10, width - 1));
  const rowH = 38;
  const height = Math.max(rowH, rows.length * rowH);
  const legend = (
    <>
      {segments.map((s) => (
        <LegendKey
          key={s.key}
          t={t}
          tone={s.fill === 'ink3' ? 'ink3' : s.fill === 'hatch' ? 'ink2' : 'ink'}
          stroke="solid"
          label={s.label}
          swatch={s.fill === 'hatch' ? 'hatch' : 'fill'}
        />
      ))}
    </>
  );
  return (
    <ChartCard title={title} meta={meta} legend={legend}>
      <div className="chart-plot bars" ref={ref} style={{ height }}>
        <svg
          width={width}
          height={height}
          role="img"
          aria-label={`${title}: ${rows
            .map(
              (r, i) =>
                `${r.label} ${segments.map((s) => `${s.label} ${withUnit(fmtNumber(r.values[s.key]), unit)}`).join(', ')}, total ${withUnit(fmtNumber(totals[i]), unit)}`,
            )
            .join('; ')}`}
        >
          <HatchDef id={id} color={t.ink2} />
          {rows.map((r, i) => {
            const top = i * rowH;
            const by = top + 21;
            const bh = 10;
            let acc = 0;
            return (
              <g key={r.key} opacity={r.dim ? 0.55 : 1}>
                <text x={0} y={top + 13} fill={t.ink2} fontSize={13} fontFamily="var(--font-sans)">
                  {r.label}
                  {r.sub ? (
                    <tspan fill={t.ink3} dx={6}>
                      {r.sub}
                    </tspan>
                  ) : null}
                </text>
                <text x={width} y={top + 13} textAnchor="end" className="spec" fill={t.ink}>
                  {withUnit(fmtNumber(totals[i]), unit)}
                </text>
                <rect
                  x={0.5}
                  y={by + 0.5}
                  width={Math.max(0, width - 1)}
                  height={bh - 1}
                  fill="none"
                  stroke={t.rule}
                />
                {segments.map((s) => {
                  const v = r.values[s.key] ?? 0;
                  const x0 = x(acc);
                  acc += v;
                  const w = Math.max(v > 0 ? 1 : 0, x(acc) - x0);
                  if (!w) return null;
                  if (s.fill === 'hatch')
                    return (
                      <g key={s.key}>
                        <rect x={x0} y={by} width={w} height={bh} fill={`url(#${id})`} />
                        <rect
                          x={x0 + 0.5}
                          y={by + 0.5}
                          width={Math.max(0, w - 1)}
                          height={bh - 1}
                          fill="none"
                          stroke={t.ink2}
                        />
                      </g>
                    );
                  return (
                    <rect
                      key={s.key}
                      x={x0}
                      y={by}
                      width={w}
                      height={bh}
                      fill={s.fill === 'ink' ? t.ink : t.ink3}
                    />
                  );
                })}
              </g>
            );
          })}
          {!rows.length ? (
            <text x={0} y={22} fill={t.ink3} className="chart-empty">
              {emptyText}
            </text>
          ) : null}
        </svg>
      </div>
    </ChartCard>
  );
}
