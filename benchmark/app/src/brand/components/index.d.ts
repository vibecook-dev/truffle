import type * as React from 'react';

/** Arrangement of the three shapes. `row` is the mark; `wrap` breaks it onto two lines for square formats; `micro` is the wrap with wider gaps for 16–48px tiles. */
export type MarkLayout = 'row' | 'wrap' | 'micro';

export interface MarkProps extends Omit<React.SVGProps<SVGSVGElement>, 'color'> {
  /** Default `row`. */
  layout?: MarkLayout;
  /** Side of the square in px; every other dimension scales from it. Default 24. Row mark minimum 16. */
  size?: number;
  /** Fill colour. Default `currentColor`, so the mark takes the text colour (`ink`). */
  color?: string;
  /** Draws the construction layer behind the shapes: baseline, cap line, overshoot lines, boxes, and (at size 64+) the gap dimensions. */
  construction?: boolean;
  /** Resolves the mark from blur on mount (duration-reveal). Once per page. Ignored under prefers-reduced-motion. */
  reveal?: boolean;
  /** Accessible name. Default "James Yong". */
  title?: string;
}
export declare function Mark(props: MarkProps): React.ReactElement;

export interface WordmarkProps extends Omit<React.SVGProps<SVGSVGElement>, 'color'> {
  /** Cap height in px. Default 16. */
  size?: number;
  color?: string;
  title?: string;
}
/** JAMES YONG, outlined from Jost Medium with a baseline J and equal-white spacing. */
export declare function Wordmark(props: WordmarkProps): React.ReactElement;

export interface LockupProps extends Omit<React.SVGProps<SVGSVGElement>, 'color'> {
  /** `inline`: mark and name on one baseline, one square apart. `stacked`: name under the mark, exactly as wide. Default `inline`. */
  layout?: 'inline' | 'stacked';
  /** Side of the mark's square in px (the inline name's cap height). Default 24. */
  size?: number;
  color?: string;
  title?: string;
}
export declare function Lockup(props: LockupProps): React.ReactElement;

export interface LoaderProps {
  /** Side of the square in px. Default 12. */
  size?: number;
  /** Announced to screen readers. Default "Loading". */
  label?: string;
  className?: string;
  style?: React.CSSProperties;
}
/** The three shapes take turns, left to right. Static under prefers-reduced-motion. */
export declare function Loader(props: LoaderProps): React.ReactElement;

export interface BlueprintProps extends React.HTMLAttributes<HTMLElement> {
  /** Controlled visibility. Omit to let Option (Alt) and the toggle drive it. */
  show?: boolean;
  /** Start visible (uncontrolled). */
  defaultShow?: boolean;
  /** Hold Option (Alt) to show. Default true. */
  hotkey?: boolean;
  /** Renders a small "⌥ Construction" toggle button in the top-right corner, for touch and for people who don't know the key. */
  toggle?: boolean;
  /** Toggle text. Default "Construction". */
  label?: string;
  /** Layout grid drawn under the boxes. Defaults 12 columns, 24px gutter, 0 margin. Set the margin to the container's side padding. */
  columns?: number;
  gutter?: number;
  margin?: number;
  /** Element to render. Default 'div'. */
  as?: keyof JSX.IntrinsicElements;
  children?: React.ReactNode;
}
/** Wraps a layout and, when shown, draws its construction: grid columns, a box around each element marked `data-measure` (with its size), and the vertical gaps between them. */
export declare function Blueprint(props: BlueprintProps): React.ReactElement;

export interface MeasureProps {
  /** Length of the dimension line in px. */
  length: number;
  /** Text on the line. Default: the length, rounded. */
  label?: string;
  /** `x` (default) or `y`. */
  axis?: 'x' | 'y';
  title?: string;
  className?: string;
  style?: React.CSSProperties;
}
/** A dimension line in the construction layer: blueline with end ticks and a mono label. */
export declare function Measure(props: MeasureProps): React.ReactElement;

export interface ButtonProps extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  /** `primary` (ink fill, once per view), `secondary` (outlined), `quiet` (underlined text). Default `primary`. */
  variant?: 'primary' | 'secondary' | 'quiet';
  /** `md` 44px (default) or `sm` 32px. */
  size?: 'md' | 'sm';
  /** Renders an anchor with the same look. */
  href?: string;
}
export declare function Button(props: ButtonProps): React.ReactElement;

export interface TextLinkProps extends React.AnchorHTMLAttributes<HTMLAnchorElement> {
  /** Opens in a new tab and adds ↗. */
  external?: boolean;
}
export declare function TextLink(props: TextLinkProps): React.ReactElement;

export interface TagProps extends React.HTMLAttributes<HTMLElement> {
  /** The discipline: `circle` design, `triangle` engineering, `square` building. Drawn with the mark's own proportions. */
  shape?: 'circle' | 'triangle' | 'square';
  as?: keyof JSX.IntrinsicElements;
  children?: React.ReactNode;
}
export declare function Tag(props: TagProps): React.ReactElement;

export interface KbdProps { children?: React.ReactNode; className?: string }
export declare function Kbd(props: KbdProps): React.ReactElement;

export type IconName = 'arrow-right' | 'arrow-left' | 'arrow-up-right' | 'download' | 'close' | 'menu' | 'plus' | 'minus' | 'check' | 'copy' | 'search' | 'mail' | 'theme';
export interface IconProps {
  name: IconName;
  /** px. Default 24; 16 or 20 inline with text. The stroke stays 1.5 units of the 24 grid. */
  size?: number;
  /** Accessible name. Omit when the icon sits next to a text label (it is then hidden from screen readers). */
  title?: string;
  className?: string;
  style?: React.CSSProperties;
}
/** 24px grid, 1.5px stroke, square caps and mitred joins, in currentColor. */
export declare function Icon(props: IconProps): React.ReactElement | null;

export interface ProjectCardProps extends React.HTMLAttributes<HTMLElement> {
  title: React.ReactNode;
  summary?: React.ReactNode;
  /** Shown in mono, e.g. "2026". */
  year?: React.ReactNode;
  tags?: Array<{ label: string; shape?: 'circle' | 'triangle' | 'square' }>;
  /** Makes the whole card a link; hover shows crop marks. */
  href?: string;
  /** An <img>, <video> or any node. Omit for the grid placeholder. */
  media?: React.ReactNode;
  /** CSS aspect-ratio of the media area. Default '16 / 10'. */
  mediaRatio?: string;
  /** Heading level of the title. Default 3. */
  headingLevel?: 2 | 3 | 4;
}
export declare function ProjectCard(props: ProjectCardProps): React.ReactElement;

export interface Geometry {
  d: string; box: [number, number, number, number]; baseline: number; capline: number;
  parts?: Array<{ kind: 'c' | 't' | 's'; d: string; box: [number, number, number, number] }>;
}
declare global {
  interface Window {
    JY: {
      Mark: typeof Mark; Wordmark: typeof Wordmark; Lockup: typeof Lockup; Loader: typeof Loader;
      Blueprint: typeof Blueprint; Measure: typeof Measure; Button: typeof Button; TextLink: typeof TextLink;
      Tag: typeof Tag; Kbd: typeof Kbd; Icon: typeof Icon; ProjectCard: typeof ProjectCard;
      /** Names accepted by Icon. */
      icons: IconName[];
      /** Raw geometry of every logo form, in mark units (square = 1000), SVG y-down. */
      geometry: Record<'row' | 'wrap' | 'wrapMicro' | 'wordmark' | 'inline' | 'stacked', Geometry> & { params: Record<string, unknown> };
    };
  }
}
