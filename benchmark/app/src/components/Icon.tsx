// Interface icons. The identity's 13 come from JY.Icon; the rest are drawn here on the same
// spec: 24 grid, 1.5 stroke, square caps, mitred joins, currentColor (RFC 027 §11.4). Some
// paths are taken from Home's set (career/home/src/core/ui.jsx ICONS), marked "Home".

import { jy, JY_ICONS } from '../lib/jy';
import type { JYIconName } from '../brand/jy.d';

const PATHS = {
  // Home
  overview: 'M4 4H10V10H4ZM14 4H20V10H14ZM4 14H10V20H4ZM14 14H20V20H14Z',
  file: 'M6 3H14L19 8V21H6ZM14 3V8H19',
  globe:
    'M21 12A9 9 0 1 1 3 12A9 9 0 1 1 21 12ZM3 12H21M12 3C9.5 6 9.5 18 12 21M12 3C14.5 6 14.5 18 12 21',
  upload: 'M12 15V3M7 8L12 3L17 8M4 20H20',
  play: 'M8 5L19 12L8 19Z',
  sidebar: 'M4 4H20V20H4ZM9 4V20',
  'chevron-down': 'M6 9L12 15L18 9',
  'chevron-right': 'M9 6L15 12L9 18',
  link: 'M10 14L14 10M8.5 11.5L6 14A3 3 0 0 0 10 18L12.5 15.5M15.5 12.5L18 10A3 3 0 0 0 14 6L11.5 8.5',
  trash: 'M5 7H19M10 7V4H14V7M7 7L8 20H16L17 7',
  lock: 'M6 11H18V20H6ZM9 11V8A3 3 0 0 1 15 8V11',
  // new for truffle bench
  stop: 'M6 6H18V18H6Z',
  refresh: 'M19.5 12A7.5 7.5 0 1 1 17.3 6.7M17.5 2.5V7H13',
  peer: 'M3 5H21V16H3ZM8 20H16M12 16V20',
  clock: 'M21 12A9 9 0 1 1 3 12A9 9 0 1 1 21 12ZM12 7V12L15.5 14',
  signal: 'M5 20V16M10 20V12M15 20V8M20 20V4',
  sync: 'M4.5 10A8 8 0 0 1 18 6.5M19 3V7H15M19.5 14A8 8 0 0 1 6 17.5M5 21V17H9',
  video: 'M3 6H16V18H3ZM16 10L21 7V17L16 14',
  folder: 'M3 5H10L12 7H21V19H3Z',
  warning: 'M12 3.5L21.5 20H2.5ZM12 9.5V14M12 16.5V17.5',
  tcp: 'M4 8H19M15 4L19 8L15 12M20 16H5M9 12L5 16L9 20',
  udp: 'M3 12H5M8 12H10M13 12H15M17 8L21 12L17 16',
  quic: 'M4 6H18M4 12H18M4 18H18M15 3L18 6L15 9M15 15L18 18L15 21',
  message: 'M4 5H20V16H11L7 20V16H4Z',
  egress: 'M13 4H20V11M20 4L11 13M17 14V20H4V7H10',
  planned: 'M4 4H8M11 4H13M16 4H20V8M20 11V13M20 16V20H16M13 20H11M8 20H4V16M4 13V11M4 8V4',
  suite: 'M4 6H6M9 6H20M4 12H6M9 12H20M4 18H6M9 18H20',
  settings: 'M4 7H13M17 7H20M4 17H7M11 17H20M15 4.5V9.5M9 14.5V19.5',
  info: 'M21 12A9 9 0 1 1 3 12A9 9 0 1 1 21 12ZM12 11V17M12 7V8',
  dot: 'M11.5 12H12.5',
} as const;

export type IconName = JYIconName | keyof typeof PATHS;

export interface IconProps {
  name: IconName;
  /** px; 20 by default, 16 inline with small text. */
  size?: number;
  /** Accessible name. Omit when a text label sits next to the icon. */
  title?: string;
  className?: string;
}

export function Icon({ name, size = 20, title, className }: IconProps) {
  if (JY_ICONS.has(name)) {
    const J = jy();
    if (J?.Icon)
      return <J.Icon name={name as JYIconName} size={size} title={title} className={className} />;
  }
  const d = (PATHS as Record<string, string>)[name];
  if (!d) return null;
  return (
    <svg
      className={`icon${className ? ` ${className}` : ''}`}
      viewBox="0 0 24 24"
      width={size}
      height={size}
      fill="none"
      stroke="currentColor"
      strokeWidth={1.5}
      strokeLinecap="square"
      strokeLinejoin="miter"
      role={title ? 'img' : undefined}
      aria-label={title || undefined}
      aria-hidden={title ? undefined : true}
      focusable="false"
    >
      <path d={d} />
    </svg>
  );
}
