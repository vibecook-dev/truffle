// App chrome after Home: the title strip (desktop.css), the rail and its foot (home.css .rail*),
// and the two-node topology strip.

import { useState, type ReactNode } from 'react';
import type { BenchPeer, LocalInfo } from '../api';
import { hrefOf, type Page, type Route } from '../hooks/useRoute';
import { jy } from '../lib/jy';
import { fmtConnection, fmtNumber, fmtOs } from '../lib/format';
import { useWidth } from '../hooks/useSize';
import { Icon, type IconName } from './Icon';
import { IconButton } from './Button';
import { Kbd, Mark } from './Misc';
import { Status } from './Status';

/** 40 px, draggable: the strip and its title both carry data-tauri-drag-region. */
export function TitleStrip({ title, banner }: { title: string; banner?: ReactNode }) {
  return (
    <header className="titlestrip" data-tauri-drag-region="">
      <span className="ts-title" data-tauri-drag-region="">
        {title}
      </span>
      {banner ? <span className="ts-right">{banner}</span> : null}
    </header>
  );
}

export type DotKind = 'live' | 'running' | 'failed' | 'cancelled' | 'planned' | null;

export interface RailEntry {
  page: Page;
  label: string;
  icon: IconName;
  dot?: DotKind;
  dotLabel?: string;
  /** Its shortcut, e.g. ['⌘', '3']. */
  keys?: string[];
}

interface RailTip {
  label: string;
  keys?: string[];
  top: number;
  left: number;
}

export function Rail({
  route,
  compact,
  groups,
  foot,
}: {
  route: Route;
  compact: boolean;
  groups: { label?: string; items: RailEntry[] }[];
  foot: ReactNode;
}) {
  // one tooltip for the rail, fixed to the viewport so the scrolling rail doesn't clip it
  const [tip, setTip] = useState<RailTip | null>(null);
  const show = (el: HTMLElement, it: RailEntry) => {
    if (!compact && !it.keys) return;
    const r = el.getBoundingClientRect();
    setTip({ label: it.label, keys: it.keys, top: r.top + r.height / 2, left: r.right + 8 });
  };
  return (
    <nav className={`rail${compact ? ' compact' : ''}`} aria-label="truffle bench">
      <a href={hrefOf('overview')} className="rail-brand" title={compact ? 'Overview' : undefined}>
        {compact ? <Mark layout="micro" size={8.6} /> : <Mark size={16} />}
        <span className="rb-name">truffle bench</span>
      </a>
      <div className="rail-scroll">
        {groups.map((g, gi) => (
          <div key={g.label ?? gi} className={`rail-group${gi === 0 ? ' first' : ''}`}>
            {g.label ? <span className="rg-label label">{g.label}</span> : null}
            {g.items.map((it) => {
              const active = route.page === it.page;
              return (
                <a
                  key={it.page}
                  href={hrefOf(it.page)}
                  className={`rail-item${active ? ' active' : ''}`}
                  aria-current={active ? 'page' : undefined}
                  aria-label={compact ? it.label : undefined}
                  aria-keyshortcuts={
                    it.keys
                      ? it.keys.join('+').replace('⌘', 'Meta').replace('Ctrl', 'Control')
                      : undefined
                  }
                  onMouseEnter={(e) => show(e.currentTarget, it)}
                  onMouseLeave={() => setTip(null)}
                  onFocus={(e) => show(e.currentTarget, it)}
                  onBlur={() => setTip(null)}
                >
                  <span className="ri-icon" aria-hidden="true">
                    <Icon name={it.icon} />
                  </span>
                  <span className="ri-label">{it.label}</span>
                  {it.dot ? (
                    <span
                      className={`ri-dot ${it.dot}`}
                      role="img"
                      aria-label={it.dotLabel ?? it.dot}
                    />
                  ) : null}
                </a>
              );
            })}
          </div>
        ))}
      </div>
      {foot}
      {tip ? (
        <span className="rail-tip" role="tooltip" style={{ top: tip.top, left: tip.left }}>
          <span>{tip.label}</span>
          {tip.keys ? (
            <span className="kbd-keys">
              {tip.keys.map((k) => (
                <Kbd key={k}>{k}</Kbd>
              ))}
            </span>
          ) : null}
        </span>
      ) : null}
    </nav>
  );
}

export function RailFoot({
  compact,
  onToggle,
  children,
  square,
}: {
  compact: boolean;
  onToggle: () => void;
  children: ReactNode;
  /** The state square alone, for the compact rail. */
  square: ReactNode;
}) {
  return (
    <div className="rail-foot">
      <div className="rf-state">{children}</div>
      <span className="rf-square">{square}</span>
      <IconButton
        small
        icon="sidebar"
        label={compact ? 'Show labels' : 'Hide labels'}
        onClick={onToggle}
      />
    </div>
  );
}

/**
 * node ■ ——— JY.Measure (RTT, path) ——— node ■ (RFC 027 §11.3). The numbers in the dimension
 * line are repeated in the facts under it.
 */
export function TopologyStrip({
  local,
  target,
}: {
  local: LocalInfo | null;
  target: BenchPeer | null;
}) {
  const [ref, width] = useWidth<HTMLDivElement>(320);
  const J = jy();
  const rtt = target?.rtt_ms;
  const label = target
    ? target.online
      ? `${rtt == null ? '–' : `${fmtNumber(rtt)} ms`} · ${fmtConnection(target.connection)}`
      : 'offline'
    : 'no target';
  return (
    <div className="topology">
      <div className="tp-node">
        <span className="tp-square live" aria-hidden="true" />
        <span className="tp-name">{local?.device_name ?? 'This device'}</span>
        <span className="tp-os caption muted">This device</span>
      </div>
      <div className="tp-link" ref={ref}>
        {J?.Measure ? (
          <J.Measure length={Math.max(40, width)} label={label} title="Round trip to the target" />
        ) : (
          <span className="mono">{label}</span>
        )}
      </div>
      <div className="tp-node right">
        <span
          className={`tp-square ${target ? (target.online ? 'live' : 'off') : 'none'}`}
          aria-hidden="true"
        />
        <span className="tp-name">{target?.device_name ?? 'No target'}</span>
        <span className="tp-os caption muted">
          {target
            ? `${fmtOs(target.os)}${target.online ? '' : ' · offline'}`
            : 'Choose one on Peers'}
        </span>
      </div>
    </div>
  );
}

export function TargetStatus({ peer }: { peer: BenchPeer }) {
  return peer.online ? <Status kind="live">Online</Status> : <Status kind="off">Offline</Status>;
}
