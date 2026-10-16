// base.css .notice / .notice.error. Info notices are drawn with a rule, not blueline: blueline is
// construction only (RFC 027 §11.4). Errors are redline with the redline wash.

import type { ReactNode } from 'react';
import { Icon } from './Icon';

export function Notice({
  kind = 'info',
  title,
  children,
  actions,
  role,
}: {
  kind?: 'info' | 'error' | 'quiet';
  title?: ReactNode;
  children?: ReactNode;
  actions?: ReactNode;
  role?: 'status' | 'alert';
}) {
  return (
    <div
      className={`notice${kind === 'info' ? '' : ` ${kind}`}`}
      role={role ?? (kind === 'error' ? 'alert' : undefined)}
    >
      <Icon name={kind === 'error' ? 'warning' : 'info'} size={18} />
      <div className="n-body">
        {title ? <div className="n-title">{title}</div> : null}
        {children ? <div className="n-text">{children}</div> : null}
        {actions ? <div className="n-actions">{actions}</div> : null}
      </div>
    </div>
  );
}

/** home.css .empty: a dashed box that says what is missing and what to do. */
export function Empty({
  title,
  children,
  actions,
}: {
  title?: ReactNode;
  children?: ReactNode;
  actions?: ReactNode;
}) {
  return (
    <div className="empty">
      {title ? <div className="e-title">{title}</div> : null}
      {children ? <div className="e-text">{children}</div> : null}
      {actions ? <div className="e-actions">{actions}</div> : null}
    </div>
  );
}
