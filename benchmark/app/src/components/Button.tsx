// Home's .btn (base.css :136-158): 34 px, square, ink fill for the one primary action per view.
// `large` is the JY.Button md height (44 px) for the sign-in and hero actions. A disabled button
// can carry its reason, which is shown as its tooltip and announced.

import { useEffect, useState, type ButtonHTMLAttributes, type ReactNode } from 'react';
import { Icon, type IconName } from './Icon';

export interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: 'primary' | 'secondary' | 'quiet' | 'danger';
  size?: 'md' | 'small' | 'large';
  icon?: IconName;
  /** Icon after the label, e.g. arrow-right. */
  trailingIcon?: IconName;
  wide?: boolean;
  /** Why the button is disabled; sets `title` and `aria-description`. */
  reason?: string | null;
  children?: ReactNode;
}

export function Button({
  variant = 'secondary',
  size = 'md',
  icon,
  trailingIcon,
  wide,
  reason,
  className = '',
  children,
  type = 'button',
  ...rest
}: ButtonProps) {
  const cls = [
    'btn',
    variant === 'secondary' ? '' : variant,
    size === 'md' ? '' : size,
    wide ? 'wide' : '',
    className,
  ]
    .filter(Boolean)
    .join(' ');
  const iconSize = size === 'small' ? 16 : 18;
  return (
    <button
      type={type}
      className={cls}
      title={reason ?? rest.title}
      aria-description={reason ?? undefined}
      {...rest}
    >
      {icon ? <Icon name={icon} size={iconSize} /> : null}
      {children}
      {trailingIcon ? <Icon name={trailingIcon} size={iconSize} /> : null}
    </button>
  );
}

export interface IconButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  icon: IconName;
  /** Accessible name and tooltip. */
  label: string;
  small?: boolean;
  pressed?: boolean;
}

/** base.css .iconbtn */
export function IconButton({
  icon,
  label,
  small,
  pressed,
  className = '',
  ...rest
}: IconButtonProps) {
  return (
    <button
      type="button"
      className={`iconbtn${small ? ' small' : ''}${className ? ` ${className}` : ''}`}
      aria-label={label}
      title={label}
      aria-pressed={pressed}
      {...rest}
    >
      <Icon name={icon} size={small ? 16 : 20} />
    </button>
  );
}

/**
 * A two-step button for what can't be undone (Home's ConfirmButton, core/ui.jsx:301): the first
 * press arms it, showing the confirming verb and "Keep". It disarms after 5 s or on Esc.
 */
export function ConfirmButton({
  children,
  confirm,
  onConfirm,
  icon,
  size = 'small',
  variant = 'secondary',
  disabled,
  reason,
}: {
  children: ReactNode;
  /** The confirming verb, e.g. "Delete 2 runs". */
  confirm: string;
  onConfirm: () => void;
  icon?: IconName;
  size?: 'md' | 'small';
  variant?: 'secondary' | 'quiet' | 'danger';
  disabled?: boolean;
  reason?: string | null;
}) {
  const [armed, setArmed] = useState(false);
  useEffect(() => {
    if (!armed) return undefined;
    const t = window.setTimeout(() => setArmed(false), 5000);
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        e.preventDefault();
        setArmed(false);
      }
    };
    document.addEventListener('keydown', onKey);
    return () => {
      window.clearTimeout(t);
      document.removeEventListener('keydown', onKey);
    };
  }, [armed]);
  return armed ? (
    <span className="confirm" role="group" aria-label={confirm}>
      <Button
        variant="danger"
        size={size}
        autoFocus
        onClick={() => {
          setArmed(false);
          onConfirm();
        }}
      >
        {confirm}
      </Button>
      <Button variant="quiet" size={size} onClick={() => setArmed(false)}>
        Keep
      </Button>
    </span>
  ) : (
    <Button
      variant={variant}
      size={size}
      icon={icon}
      disabled={disabled}
      reason={reason}
      onClick={() => setArmed(true)}
    >
      {children}
    </Button>
  );
}
