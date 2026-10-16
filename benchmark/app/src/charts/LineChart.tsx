// A time series: one or more series over run time (t_ms), the warmup flagged, a previous run
// as an ink-3 dashed baseline with outlined squares, the measured window as a blueline-wash
// band under the main series. Points append live without easing; width follows the container.

import { useMemo, useState, type PointerEvent } from 'react';
import { useTokens } from '../hooks/useTokens';
import { useWidth } from '../hooks/useSize';
import { useConstructionFlag } from './construction';
import { jy } from '../lib/jy';
import { fmtNumber, withUnit } from '../lib/format';
import {
  Axes,
  ChartCard,
  ChartTip,
  ConstructionGrid,
  DASHES,
  Guide,
  LegendKey,
  MARGIN,
  PlotSurface,
  SERIES_STYLES,
  Squares,
  crisp,
  toneColor,
  type Box,
  type Stroke,
  type Tone,
} from './core';
import { linear, linePath, markerIndices, nearestIndex, niceTicks, ticksWithin } from './scale';

export interface LineSeries {
  key: string;
  label: string;
  /** [t_ms, value] sorted by time. */
  points: [number, number][];
  /** Defaults to the series' position: ink solid, ink-2 dashed, ink-3 dotted. */
  tone?: Tone;
  stroke?: Stroke;
  /** A previous run or baseline: ink-3 dashed with outlined squares. */
  baseline?: boolean;
  /** Fill the measured window under this series with blueline-wash. */
  area?: boolean;
  /** ± this much around the series, drawn as a blueline-wash band (an uncertainty). */
  band?: number | null;
}

export interface LineChartProps {
  title?: string;
  meta?: string;
  series: LineSeries[];
  /** Unit of the y values, e.g. "Mb/s". */
  unit: string;
  height?: number;
  /** End of the warmup in ms; the region before it is flagged and excluded from the band. */
  warmupMs?: number | null;
  /** Planned run length in ms, so a live run does not rescale as it grows. */
  domainMs?: number | null;
  /** Construction guides on the y axis, e.g. p50 and p99. Their numbers must be on the surface too. */
  guides?: { label: string; value: number }[];
  /** Points to mark in redline (loss, failures). */
  marks?: [number, number][];
  /** Legend text for the redline marks, e.g. "Loss above 1%". */
  marksLabel?: string;
  /** Reference levels (an offered rate, a ceiling): ink-3 dashed, labelled at the right. */
  refs?: { label: string; value: number }[];
  /** Draw the last point of the first series larger (live). */
  live?: boolean;
  digits?: number;
  emptyText?: string;
  measure?: string;
}

export function LineChart({
  title,
  meta,
  series,
  unit,
  height = 220,
  warmupMs,
  domainMs,
  guides = [],
  marks = [],
  marksLabel,
  refs = [],
  live,
  digits,
  emptyText = 'No data yet.',
  measure,
}: LineChartProps) {
  const t = useTokens();
  const construction = useConstructionFlag();
  const [ref, width] = useWidth<HTMLDivElement>();
  const [hover, setHover] = useState<number | null>(null);

  const styled = useMemo(() => {
    let i = 0;
    return series.map((s) => {
      if (s.baseline) return { ...s, tone: 'ink3' as Tone, stroke: 'dashed' as Stroke };
      const d = SERIES_STYLES[Math.min(i++, SERIES_STYLES.length - 1)];
      return { ...s, tone: s.tone ?? d.tone, stroke: s.stroke ?? d.stroke };
    });
  }, [series]);

  const box: Box = {
    x: MARGIN.left,
    y: MARGIN.top,
    w: Math.max(40, width - MARGIN.left - MARGIN.right),
    h: Math.max(40, height - MARGIN.top - MARGIN.bottom),
  };

  const allPts = styled.flatMap((s) => s.points);
  const empty = allPts.length === 0;
  // a live run keeps its planned length; a finished one ends at its last point
  const tMax = Math.max(
    domainMs ?? 0,
    ...allPts.map((p) => p[0]),
    allPts.length || domainMs ? 250 : 1000,
  );
  // a finished count-based run starts after setup (its first sample at ~0.1 s of a 0.2 s run):
  // start the axis at the first point instead of drawing the empty setup
  const tFirst = allPts.length ? Math.min(...allPts.map((p) => p[0])) : 0;
  const tMin = !domainMs && tFirst > 0.2 * tMax ? tFirst - 0.02 * (tMax - tFirst) : 0;
  // the scale fits the measured points: a warmup outlier (the first round trip pays for the
  // responder's WhoIs) is drawn at the top edge rather than flattening everything after it
  const measured = warmupMs ? allPts.filter((p) => p[0] >= warmupMs) : allPts;
  const scaled = measured.length ? measured : allPts;
  const vMax = Math.max(
    0,
    ...scaled.map((p) => p[1]).filter(Number.isFinite),
    ...guides.map((g) => g.value),
    ...refs.map((r) => r.value),
    ...styled.flatMap((s) =>
      s.band
        ? s.points
            .filter((p) => !warmupMs || p[0] >= warmupMs)
            .map((p) => p[1] + (s.band as number))
        : [],
    ),
  );
  const xTicks = ticksWithin(tMin / 1000, tMax / 1000, Math.max(3, Math.floor(box.w / 90)));
  const yTicks = niceTicks(0, vMax > 0 ? vMax * 1.08 : 1, Math.max(3, Math.floor(box.h / 46)));
  const x = linear(xTicks.min, xTicks.max, box.x, box.x + box.w);
  const y = linear(yTicks.min, yTicks.max, box.y + box.h, box.y);
  const px = (p: [number, number]): [number, number] => [
    x(p[0] / 1000),
    y(Math.min(p[1], yTicks.max)),
  ];

  // the crosshair snaps to the first non-baseline series
  const primary = styled.find((s) => !s.baseline && s.points.length) ?? styled[0];
  const xs = primary ? primary.points.map((p) => p[0]) : [];

  const onMove = (e: PointerEvent<SVGRectElement>) => {
    if (!xs.length) return;
    const r = (e.currentTarget.ownerSVGElement as SVGSVGElement).getBoundingClientRect();
    const tm = x.invert(e.clientX - r.left) * 1000;
    setHover(nearestIndex(xs, tm));
  };

  const warmX = warmupMs && warmupMs > tMin ? x(Math.min(warmupMs, tMax) / 1000) : null;
  const J = jy();
  const hoverT = hover != null && xs[hover] != null ? xs[hover] : null;

  const showWarm = !!warmupMs && !empty;
  const banded = styled.find((s) => s.band && !s.baseline && s.points.length);
  const legend =
    styled.length > 1 || showWarm || banded || refs.length || (marksLabel && marks.length) ? (
      <>
        {styled.length > 1
          ? styled.map((s) => (
              <LegendKey
                key={s.key}
                t={t}
                tone={s.tone}
                stroke={s.stroke}
                label={s.label}
                outlined={s.baseline}
              />
            ))
          : null}
        {banded ? (
          <LegendKey
            t={t}
            tone="ink"
            stroke="solid"
            label={`± ${withUnit(fmtNumber(banded.band), unit)} clock uncertainty`}
            swatch="wash"
          />
        ) : null}
        {refs.map((r) => (
          <LegendKey key={`ref-${r.label}`} t={t} tone="ink3" stroke="dashed" label={r.label} />
        ))}
        {marksLabel && marks.length ? (
          <LegendKey t={t} tone="ink" stroke="solid" label={marksLabel} swatch="red" />
        ) : null}
        {showWarm ? (
          <LegendKey t={t} tone="ink3" stroke="solid" label="Warmup, not counted" swatch="sunken" />
        ) : null}
      </>
    ) : null;

  return (
    <ChartCard title={title} meta={meta} legend={legend} measure={measure}>
      <div className="chart-plot" ref={ref} style={{ height }}>
        <svg
          width={width}
          height={height}
          role="img"
          aria-label={`${title ?? 'Chart'}: ${styled.map((s) => s.label).join(', ')} over time, in ${unit}`}
        >
          <PlotSurface t={t} box={box} y={y} yTicks={yTicks} />
          {warmX != null && warmX > box.x + 1 ? (
            <g>
              <rect
                x={box.x + 1}
                y={box.y + 1}
                width={Math.max(0, warmX - box.x - 1)}
                height={box.h - 2}
                fill={t.groundSunken}
              />
              <line
                x1={crisp(warmX)}
                x2={crisp(warmX)}
                y1={box.y}
                y2={box.y + box.h}
                stroke={t.ruleStrong}
                strokeDasharray="2 4"
              />
              {warmX - box.x >= 64 ? (
                <text className="annotation" x={box.x + 6} y={box.y + 14} fill={t.ink3}>
                  warmup
                </text>
              ) : null}
            </g>
          ) : null}
          {styled
            .filter((s) => s.area && !s.baseline && s.points.length > 1)
            .map((s) => {
              const pts = s.points.filter((p) => !warmupMs || p[0] >= warmupMs).map(px);
              if (pts.length < 2) return null;
              const base = y(0);
              const d = `${linePath(pts)}L${pts[pts.length - 1][0]} ${base}L${pts[0][0]} ${base}Z`;
              return <path key={`area-${s.key}`} d={d} fill={t.bluelineWash} stroke="none" />;
            })}
          {styled
            .filter((s) => s.band && !s.baseline && s.points.length > 1)
            .map((s) => {
              const b = s.band as number;
              const up = s.points.map(
                (p) => [x(p[0] / 1000), y(Math.min(p[1] + b, yTicks.max))] as [number, number],
              );
              const down = s.points
                .map((p) => [x(p[0] / 1000), y(Math.max(0, p[1] - b))] as [number, number])
                .reverse();
              const d = `${linePath(up)}L${linePath(down).slice(1)}Z`;
              return <path key={`band-${s.key}`} d={d} fill={t.bluelineWash} stroke="none" />;
            })}
          <Axes
            t={t}
            box={box}
            x={x}
            xTicks={xTicks}
            xUnit="s"
            y={y}
            yTicks={yTicks}
            yUnit={unit}
          />
          {refs.map((r) => (
            <g key={`ref-${r.label}`}>
              <line
                x1={box.x}
                x2={box.x + box.w}
                y1={crisp(y(r.value))}
                y2={crisp(y(r.value))}
                stroke={t.ink3}
                strokeWidth={1.2}
                strokeDasharray={DASHES.dashed}
              />
              <text
                className="annotation"
                x={box.x + box.w - 4}
                y={y(r.value) - 5}
                textAnchor="end"
                fill={t.ink3}
                stroke={t.groundRaised}
                strokeWidth={3}
                paintOrder="stroke"
              >
                {r.label} {withUnit(fmtNumber(r.value, digits), unit)}
              </text>
            </g>
          ))}
          {styled.map((s) => {
            const c = toneColor(t, s.tone);
            const pts = s.points.map(px);
            const idx = markerIndices(pts.length, 12);
            return (
              <g key={s.key}>
                <path
                  d={linePath(pts)}
                  fill="none"
                  stroke={c}
                  strokeWidth={s.baseline ? 1.2 : 1.5}
                  strokeDasharray={DASHES[s.stroke]}
                  strokeLinejoin="miter"
                  strokeLinecap="square"
                />
                <Squares
                  pts={idx.map((i) => pts[i])}
                  color={c}
                  fill={s.baseline ? t.groundRaised : undefined}
                  size={s.baseline ? 5 : 4}
                />
              </g>
            );
          })}
          {live && primary && primary.points.length ? (
            <Squares pts={[px(primary.points[primary.points.length - 1])]} color={t.ink} size={7} />
          ) : null}
          {marks.length ? <Squares pts={marks.map(px)} color={t.redline} size={6} /> : null}
          {hoverT != null ? (
            <line
              x1={crisp(x(hoverT / 1000))}
              x2={crisp(x(hoverT / 1000))}
              y1={box.y}
              y2={box.y + box.h}
              stroke={t.ruleStrong}
            />
          ) : null}
          {empty ? (
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
          <rect
            x={box.x}
            y={box.y}
            width={box.w}
            height={box.h}
            fill="transparent"
            onPointerMove={onMove}
            onPointerLeave={() => setHover(null)}
          />
        </svg>
        {construction && !empty ? (
          <div className="chart-construction" aria-hidden="true">
            <svg width={width} height={height}>
              <ConstructionGrid t={t} box={box} x={x} xTicks={xTicks} y={y} yTicks={yTicks} />
              {guides.map((g) => (
                <Guide
                  key={g.label}
                  t={t}
                  box={box}
                  axis="y"
                  at={y(g.value)}
                  anchor="end"
                  label={`${g.label} ${withUnit(fmtNumber(g.value, digits), unit)}`}
                />
              ))}
            </svg>
            {J?.Measure && warmX != null && warmupMs ? (
              <J.Measure
                className="cm-measure"
                style={{ position: 'absolute', left: box.x, top: box.y + box.h - 24 }}
                length={Math.max(8, warmX - box.x)}
                label={`${fmtNumber(warmupMs / 1000, 1)} s`}
                title="Warmup"
              />
            ) : null}
            {J?.Measure && warmX != null && primary && primary.points.length > 1 ? (
              <J.Measure
                className="cm-measure"
                style={{ position: 'absolute', left: warmX, top: box.y + box.h - 24 }}
                length={Math.max(8, px(primary.points[primary.points.length - 1])[0] - warmX)}
                label={`window ${fmtNumber((primary.points[primary.points.length - 1][0] - (warmupMs ?? 0)) / 1000, 1)} s`}
                title="Measured window"
              />
            ) : null}
          </div>
        ) : null}
        {hoverT != null ? (
          <ChartTip
            left={x(hoverT / 1000)}
            top={box.y + 4}
            flip={x(hoverT / 1000) > box.x + box.w * 0.6}
            title={`${fmtNumber(hoverT / 1000, 2)} s`}
            rows={styled.map((s) => {
              const i = nearestIndex(
                s.points.map((p) => p[0]),
                hoverT,
              );
              const v = i >= 0 ? s.points[i][1] : null;
              return {
                key: s.key,
                value: withUnit(fmtNumber(v, digits), unit),
                label: s.label,
                keyEl: (
                  <svg width={16} height={8} aria-hidden="true">
                    <line
                      x1={0}
                      x2={16}
                      y1={4}
                      y2={4}
                      stroke={toneColor(t, s.tone)}
                      strokeWidth={1.5}
                      strokeDasharray={DASHES[s.stroke]}
                    />
                  </svg>
                ),
              };
            })}
          />
        ) : null}
      </div>
    </ChartCard>
  );
}
