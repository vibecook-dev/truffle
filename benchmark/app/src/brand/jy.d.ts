// Types for `window.JY`, the identity's component bundle. The vendored components/index.d.ts
// types the same API but uses the global `JSX` namespace that @types/react 19 removed, so it is
// excluded from the program (tsconfig) and these declarations stand in for it, unchanged in
// meaning. Keep them in step with components/index.d.ts when the bundle is re-vendored.
import type * as React from 'react';

export type MarkLayout = 'row' | 'wrap' | 'micro';

export interface MarkProps extends Omit<React.SVGProps<SVGSVGElement>, 'color'> {
  layout?: MarkLayout;
  /** Side of the square in px. Default 24. Row mark minimum 16. */
  size?: number;
  color?: string;
  construction?: boolean;
  /** Resolve from blur on mount. Once per page. */
  reveal?: boolean;
  title?: string;
}

export interface WordmarkProps extends Omit<React.SVGProps<SVGSVGElement>, 'color'> {
  size?: number;
  color?: string;
  title?: string;
}

export interface LockupProps extends Omit<React.SVGProps<SVGSVGElement>, 'color'> {
  layout?: 'inline' | 'stacked';
  size?: number;
  color?: string;
  title?: string;
}

export interface LoaderProps {
  size?: number;
  label?: string;
  className?: string;
  style?: React.CSSProperties;
}

export interface BlueprintProps extends React.HTMLAttributes<HTMLElement> {
  show?: boolean;
  defaultShow?: boolean;
  hotkey?: boolean;
  toggle?: boolean;
  label?: string;
  columns?: number;
  gutter?: number;
  margin?: number;
  as?: keyof React.JSX.IntrinsicElements;
  children?: React.ReactNode;
}

export interface MeasureProps {
  length: number;
  label?: string;
  axis?: 'x' | 'y';
  title?: string;
  className?: string;
  style?: React.CSSProperties;
}

export interface ButtonProps extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: 'primary' | 'secondary' | 'quiet';
  size?: 'md' | 'sm';
  href?: string;
  ref?: React.Ref<HTMLButtonElement>;
}

export interface TextLinkProps extends React.AnchorHTMLAttributes<HTMLAnchorElement> {
  external?: boolean;
}

export interface TagProps extends React.HTMLAttributes<HTMLElement> {
  shape?: 'circle' | 'triangle' | 'square';
  as?: keyof React.JSX.IntrinsicElements;
  children?: React.ReactNode;
}

export interface KbdProps {
  children?: React.ReactNode;
  className?: string;
}

export type JYIconName =
  | 'arrow-right'
  | 'arrow-left'
  | 'arrow-up-right'
  | 'download'
  | 'close'
  | 'menu'
  | 'plus'
  | 'minus'
  | 'check'
  | 'copy'
  | 'search'
  | 'mail'
  | 'theme';

export interface JYIconProps {
  name: JYIconName;
  size?: number;
  title?: string;
  className?: string;
  style?: React.CSSProperties;
}

export interface Geometry {
  d: string;
  box: [number, number, number, number];
  baseline: number;
  capline: number;
  parts?: Array<{ kind: 'c' | 't' | 's'; d: string; box: [number, number, number, number] }>;
}

type FC<P> = (props: P) => React.ReactElement | null;

export interface JYApi {
  Mark: FC<MarkProps>;
  Wordmark: FC<WordmarkProps>;
  Lockup: FC<LockupProps>;
  Loader: FC<LoaderProps>;
  Blueprint: FC<BlueprintProps>;
  Measure: FC<MeasureProps>;
  Button: FC<ButtonProps>;
  TextLink: FC<TextLinkProps>;
  Tag: FC<TagProps>;
  Kbd: FC<KbdProps>;
  Icon: FC<JYIconProps>;
  icons: JYIconName[];
  geometry: Record<'row' | 'wrap' | 'wrapMicro' | 'wordmark' | 'inline' | 'stacked', Geometry> & {
    params: Record<string, unknown>;
  };
}

declare global {
  interface Window {
    JY: JYApi;
  }
}
