// Strips over run time.
//
// PathTimeline: which path carried the run, from the 2 Hz path sampler. Monochrome: direct is
// solid ink, a relay is hatched ink-2, unknown is an empty outline; each transition is marked
// with a tick and its new path; a ping that timed out is a redline tick.
//
// FrameStrip: one column per video frame. On time ink, late ink-3, dropped redline; keyframes
// stand taller.

import { useId, useState } from 'react';
import type { PathPoint } from '../api/types';
import { useTokens } from '../hooks/useTokens';
import { useWidth } from '../hooks/useSize';
import { fmtConnection, fmtInt, fmtNumber } from '../lib/format';
import { ChartCard, ChartTip, HatchDef, LegendKey, crisp } from './core';
import { linear, nearestIndex, ticksWithin, fmtTick } from './scale';

type Kind = 'direct' | 'relay' | 'unknown';

function kindOf(c: string | null): Kind {
  if (!c) return 'unknown';
  if (c === 'direct') return 'direct';
  if (c.startsWith('relay') || c.startsWith('peer-relay') || c === 'derp') return 'relay';
  return 'unknown';
}

export function PathTimeline({
  points,
  durationMs,
  title = 'Path',
  measure,
}: {
  points: PathPoint[];
  durationMs?: number | null;
  title?: string;
  measure?: string;
}) {
  const t = useTokens();
  const [ref, width] = useWidth<HTMLDivElement>();
  const [hover, setHover] = useState<number | null>(null);
  const left = 48;
  const right = 14;
  const height = 64;
  const barY = 22;
  const barH = 12;
  const end = Math.max(durationMs ?? 0, ...points.map((p) => p.t_ms), 1000);
  const ticks = ticksWithin(0, end / 1000, Math.max(3, Math.floor((width - left - right) / 90)));
  const x = linear(ticks.min, ticks.max, left, width - right);
  const id = `hatch-${useId().replace(/[^a-zA-Z0-9-]/g, '')}`;

  // one segment per sample, then runs of the same path merged so the strip has no seams
  const raw = points.map((p, i) => {
    const t0 = i === 0 ? 0 : (points[i - 1].t_ms + p.t_ms) / 2;
    const t1 = i === points.length - 1 ? p.t_ms : (p.t_ms + points[i + 1].t_ms) / 2;
    return { t0, t1, kind: kindOf(p.connection) };
  });
  const segs = raw.reduce<typeof raw>((acc, sg) => {
    const last = acc[acc.length - 1];
    if (last && last.kind === sg.kind) last.t1 = sg.t1;
    else acc.push({ ...sg });
    return acc;
  }, []);
  const transitions = points
    .map((p, i) => ({ p, i }))
    .filter(
      ({ p, i }) =>
        i > 0 &&
        p.connection &&
        points[i - 1].connection &&
        p.connection !== points[i - 1].connection,
    );
  const timeouts = points.filter((p) => p.rtt_ms == null);
  const total = (k: Kind) =>
    segs.filter((s) => s.kind === k).reduce((a, s) => a + (s.t1 - s.t0), 0);
  const d = total('direct');
  const r = total('relay');
  const hp = hover != null ? points[hover] : null;

  const meta = points.length
    ? [
        d > 0 ? `direct ${fmtNumber(d / 1000, 1)} s` : null,
        r > 0 ? `relay ${fmtNumber(r / 1000, 1)} s` : null,
        transitions.length
          ? `${transitions.length} transition${transitions.length === 1 ? '' : 's'}`
          : null,
        timeouts.length ? `${timeouts.length} lost ping${timeouts.length === 1 ? '' : 's'}` : null,
      ]
        .filter(Boolean)
        .join(' · ')
    : 'No path samples';

  return (
    <ChartCard
      title={title}
      meta={meta}
      measure={measure}
      legend={
        <>
          <LegendKey t={t} tone="ink" stroke="solid" label="Direct" swatch="fill" />
          <LegendKey t={t} tone="ink2" stroke="solid" label="Relay" swatch="hatch" />
          {timeouts.length ? (
            <LegendKey t={t} tone="ink" stroke="solid" label="Lost ping" swatch="red" />
          ) : null}
        </>
      }
    >
      <div className="chart-plot strip" ref={ref} style={{ height }}>
        <svg width={width} height={height} role="img" aria-label={`Path over the run: ${meta}`}>
          <HatchDef id={id} color={t.ink2} />
          <rect
            x={left + 0.5}
            y={barY + 0.5}
            width={Math.max(0, width - left - right - 1)}
            height={barH - 1}
            fill={t.groundRaised}
            stroke={t.rule}
          />
          {segs.map((s, i) => {
            const x0 = x(s.t0 / 1000);
            const w = Math.max(1, x(s.t1 / 1000) - x0);
            if (s.kind === 'direct')
              return <rect key={i} x={x0} y={barY} width={w} height={barH} fill={t.ink} />;
            if (s.kind === 'relay')
              return (
                <g key={i}>
                  <rect x={x0} y={barY} width={w} height={barH} fill={`url(#${id})`} />
                  <rect
                    x={x0 + 0.5}
                    y={barY + 0.5}
                    width={Math.max(0, w - 1)}
                    height={barH - 1}
                    fill="none"
                    stroke={t.ink2}
                  />
                </g>
              );
            return (
              <rect
                key={i}
                x={x0 + 0.5}
                y={barY + 0.5}
                width={Math.max(0, w - 1)}
                height={barH - 1}
                fill="none"
                stroke={t.ruleStrong}
                strokeDasharray="2 2"
              />
            );
          })}
          {transitions.map(({ p, i }) => {
            const tx = crisp(x((points[i - 1].t_ms + p.t_ms) / 2 / 1000));
            return (
              <g key={`tr${i}`}>
                <line x1={tx} x2={tx} y1={barY - 8} y2={barY + barH + 3} stroke={t.ink} />
                <text className="annotation" x={tx + 4} y={barY - 2} fill={t.ink2}>
                  {fmtConnection(p.connection)}
                </text>
              </g>
            );
          })}
          {timeouts.map((p, i) => (
            <rect
              key={`to${i}`}
              x={x(p.t_ms / 1000) - 1}
              y={barY - 4}
              width={2}
              height={barH + 8}
              fill={t.redline}
            />
          ))}
          <g className="annotation" fill={t.ink3}>
            {ticks.ticks.map((v, i) => (
              <text
                key={v}
                x={x(v)}
                y={height - 6}
                textAnchor={i === 0 ? 'start' : i === ticks.ticks.length - 1 ? 'end' : 'middle'}
              >
                {fmtTick(v, ticks.step)}
                {i === ticks.ticks.length - 1 ? ' s' : ''}
              </text>
            ))}
            <text x={left - 8} y={barY + barH - 2} textAnchor="end">
              path
            </text>
          </g>
          {hp ? (
            <line
              x1={crisp(x(hp.t_ms / 1000))}
              x2={crisp(x(hp.t_ms / 1000))}
              y1={barY - 8}
              y2={barY + barH + 4}
              stroke={t.ruleStrong}
            />
          ) : null}
          <rect
            x={left}
            y={0}
            width={Math.max(0, width - left - right)}
            height={height}
            fill="transparent"
            onPointerMove={(e) => {
              if (!points.length) return;
              const rr = (e.currentTarget.ownerSVGElement as SVGSVGElement).getBoundingClientRect();
              setHover(
                nearestIndex(
                  points.map((p) => p.t_ms),
                  x.invert(e.clientX - rr.left) * 1000,
                ),
              );
            }}
            onPointerLeave={() => setHover(null)}
          />
        </svg>
        {hp ? (
          <ChartTip
            left={x(hp.t_ms / 1000)}
            top={0}
            flip={x(hp.t_ms / 1000) > width * 0.6}
            title={`${fmtNumber(hp.t_ms / 1000, 1)} s`}
            rows={[
              { key: 'c', value: fmtConnection(hp.connection), label: 'path' },
              {
                key: 'r',
                value: hp.rtt_ms == null ? 'lost' : `${fmtNumber(hp.rtt_ms)} ms`,
                label: 'RTT',
              },
            ]}
          />
        ) : null}
      </div>
    </ChartCard>
  );
}

export interface FrameCell {
  frame_no: number;
  keyframe: boolean;
  state: 'on-time' | 'late' | 'dropped';
  /** Captured during the warmup: shown, not counted. */
  warmup?: boolean;
}

/** The run's own frame counts (warmup excluded), when the strip is a thinned sample of them. */
export interface FrameTotals {
  sent: number;
  onTime: number;
  late: number;
  dropped: number;
}

const ordinal = (n: number) =>
  n % 100 >= 11 && n % 100 <= 13
    ? `${n}th`
    : `${n}${n % 10 === 1 ? 'st' : n % 10 === 2 ? 'nd' : n % 10 === 3 ? 'rd' : 'th'}`;

/** The strip's header: the run's counts when given, else the cells', and the thinning. */
export function frameStripMeta(
  frames: FrameCell[],
  stride = 1,
  totals?: FrameTotals,
): { meta: string; late: number; dropped: number } {
  const counted = frames.filter((f) => !f.warmup);
  const count = (s: FrameCell['state']) => counted.filter((f) => f.state === s).length;
  const t = totals ?? {
    sent: counted.length,
    onTime: count('on-time'),
    late: count('late'),
    dropped: count('dropped'),
  };
  const meta =
    `${fmtInt(t.sent)} frames · ${fmtInt(t.onTime)} on time · ${fmtInt(t.late)} late · ` +
    `${fmtInt(t.dropped)} dropped${stride > 1 ? ` · every ${ordinal(stride)} on-time frame shown` : ''}`;
  return { meta, late: t.late, dropped: t.dropped };
}

export function FrameStrip({
  frames,
  fps,
  title = 'Frames',
  measure,
  stride = 1,
  totals,
}: {
  frames: FrameCell[];
  fps: number;
  title?: string;
  measure?: string;
  /** The engine kept every `stride`-th on-time frame (and every late and dropped one). */
  stride?: number;
  /** The run's counts: with a thinned strip, counting cells would undercount on-time frames. */
  totals?: FrameTotals;
}) {
  const t = useTokens();
  const [ref, width] = useWidth<HTMLDivElement>();
  const left = 48;
  const right = 14;
  const height = 76;
  const top = 8;
  const full = 40;
  // placed by frame number, so a strip the engine thinned (frames_stride > 1) keeps its time axis
  const n = frames.length ? frames[frames.length - 1].frame_no + 1 : 0;
  const plotW = Math.max(10, width - left - right);
  const cw = n ? plotW / n : plotW;
  const gap = cw >= 3 ? 1 : 0;
  const paths = { 'on-time': '', late: '', dropped: '' } as Record<FrameCell['state'], string>;
  frames.forEach((f) => {
    const h = f.keyframe ? full : Math.round(full * 0.55);
    const x0 = left + f.frame_no * cw;
    const w = Math.max(0.6, cw - gap);
    paths[f.state] +=
      `M${x0.toFixed(2)} ${top + full - h}h${w.toFixed(2)}v${h}h${(-w).toFixed(2)}Z`;
  });
  const head = frameStripMeta(frames, stride, totals);
  const warm = frames.filter((f) => f.warmup);
  const warmX = warm.length ? left + (warm[warm.length - 1].frame_no + 1) * cw : null;
  const secs = n / Math.max(1, fps);
  const ticks = ticksWithin(0, secs, Math.max(3, Math.floor(plotW / 90)));
  const x = linear(ticks.min, ticks.max, left, left + plotW);
  return (
    <ChartCard
      title={title}
      meta={head.meta}
      measure={measure}
      legend={
        <>
          <LegendKey t={t} tone="ink" stroke="solid" label="On time" swatch="fill" />
          <LegendKey t={t} tone="ink3" stroke="solid" label="Late" swatch="fill" />
          <LegendKey t={t} tone="ink" stroke="solid" label="Dropped" swatch="red" />
          {warmX != null ? (
            <LegendKey
              t={t}
              tone="ink3"
              stroke="solid"
              label="Warmup, not counted"
              swatch="sunken"
            />
          ) : null}
        </>
      }
    >
      <div className="chart-plot strip" ref={ref} style={{ height }}>
        <svg
          width={width}
          height={height}
          role="img"
          aria-label={`Video frames: ${head.late} late, ${head.dropped} dropped`}
        >
          {warmX != null ? (
            <rect x={left} y={top} width={warmX - left} height={full} fill={t.groundSunken} />
          ) : null}
          <line
            x1={left}
            x2={left + plotW}
            y1={crisp(top + full)}
            y2={crisp(top + full)}
            stroke={t.ruleStrong}
          />
          <path d={paths['on-time']} fill={t.ink} />
          <path d={paths.late} fill={t.ink3} />
          <path d={paths.dropped} fill={t.redline} />
          <g className="annotation" fill={t.ink3}>
            <text x={left - 8} y={top + 10} textAnchor="end">
              key
            </text>
            {ticks.ticks.map((v, i) => (
              <text
                key={v}
                x={x(v)}
                y={height - 8}
                textAnchor={i === 0 ? 'start' : i === ticks.ticks.length - 1 ? 'end' : 'middle'}
              >
                {fmtTick(v, ticks.step)}
                {i === ticks.ticks.length - 1 ? ' s' : ''}
              </text>
            ))}
          </g>
        </svg>
      </div>
    </ChartCard>
  );
}
