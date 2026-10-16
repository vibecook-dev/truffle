// Form controls from Home (base.css .seg, .field, .input, .switch, .check; ui.jsx Seg, Field,
// Switch). Labels are Jost, values and bounds DM Mono.

import { useEffect, useId, useState, type ReactNode, type InputHTMLAttributes } from 'react';
import { fmtNumber } from '../lib/format';

export interface SegOption<T extends string> {
  value: T;
  label: string;
  title?: string;
  disabled?: boolean;
}

export function Seg<T extends string>({
  value,
  options,
  onChange,
  label,
  full,
  disabled,
}: {
  value: T;
  options: SegOption<T>[];
  onChange: (v: T) => void;
  label: string;
  full?: boolean;
  disabled?: boolean;
}) {
  return (
    <div className={`seg${full ? ' full' : ''}`} role="group" aria-label={label}>
      {options.map((o) => (
        <button
          key={o.value}
          type="button"
          aria-pressed={value === o.value}
          disabled={disabled || o.disabled}
          title={o.title}
          onClick={() => onChange(o.value)}
        >
          {o.label}
        </button>
      ))}
    </div>
  );
}

/** A label over a control, with the bounds on the right and a hint or error below. */
export function Field({
  label,
  bounds,
  hint,
  error,
  htmlFor,
  children,
}: {
  label: ReactNode;
  bounds?: ReactNode;
  hint?: ReactNode;
  error?: string | null;
  htmlFor?: string;
  children: ReactNode;
}) {
  return (
    <div className="field">
      <label className="f-label" htmlFor={htmlFor}>
        <span>{label}</span>
        {bounds ? <span className="f-bounds mono">{bounds}</span> : null}
      </label>
      {children}
      {error ? (
        <span className="f-error caption" role="alert">
          {error}
        </span>
      ) : hint ? (
        <span className="hint caption">{hint}</span>
      ) : null}
    </div>
  );
}

export function TextInput({
  value,
  onChange,
  mono,
  className = '',
  ...rest
}: Omit<InputHTMLAttributes<HTMLInputElement>, 'onChange' | 'value'> & {
  value: string;
  onChange: (v: string) => void;
  mono?: boolean;
}) {
  return (
    <input
      type="text"
      className={`input${mono ? ' mono' : ''}${className ? ` ${className}` : ''}`}
      value={value}
      spellCheck={false}
      onChange={(e) => onChange(e.target.value)}
      {...rest}
    />
  );
}

/** "1–16", "0.1–1,000" */
export function fmtBounds(min: number, max: number): string {
  const f = (v: number) => (Number.isInteger(v) ? v.toLocaleString('en-US') : fmtNumber(v));
  return `${f(min)}–${f(max)}`;
}

/**
 * A number with bounds. Out-of-bounds values are shown as an error and passed up as typed: the
 * engine rejects them (validate() never clamps), so the UI never clamps either.
 */
export function NumberField({
  label,
  value,
  onChange,
  min,
  max,
  unit,
  integer,
  nullable,
  nullLabel = 'Auto',
  hint,
  error: externalError,
  disabled,
}: {
  label: ReactNode;
  value: number | null;
  onChange: (v: number | null, valid: boolean) => void;
  min: number;
  max: number;
  unit?: string;
  integer?: boolean;
  nullable?: boolean;
  nullLabel?: string;
  hint?: ReactNode;
  error?: string | null;
  disabled?: boolean;
}) {
  const id = useId();
  const [text, setText] = useState(value == null ? '' : String(value));
  useEffect(() => {
    setText((t) => {
      const parsed = t.trim() === '' ? null : Number(t);
      return parsed === value ? t : value == null ? '' : String(value);
    });
  }, [value]);
  const problem = check(text, min, max, integer, nullable);
  const shown = problem ?? externalError ?? null;
  return (
    <Field label={label} bounds={fmtBounds(min, max)} hint={hint} error={shown} htmlFor={id}>
      <div className="numfield">
        <input
          id={id}
          className="input"
          inputMode={integer ? 'numeric' : 'decimal'}
          value={text}
          placeholder={nullable ? nullLabel : undefined}
          aria-invalid={shown ? true : undefined}
          disabled={disabled}
          spellCheck={false}
          onChange={(e) => {
            const t = e.target.value;
            setText(t);
            const p = check(t, min, max, integer, nullable);
            const n = t.trim() === '' ? null : Number(t);
            onChange(n != null && Number.isNaN(n) ? value : n, p == null);
          }}
        />
        {unit ? <span className="nf-unit mono">{unit}</span> : null}
      </div>
    </Field>
  );
}

function check(
  text: string,
  min: number,
  max: number,
  integer?: boolean,
  nullable?: boolean,
): string | null {
  const t = text.trim();
  if (t === '') return nullable ? null : 'Enter a number.';
  const n = Number(t);
  if (!Number.isFinite(n)) return 'Enter a number.';
  if (integer && !Number.isInteger(n)) return 'Enter a whole number.';
  if (n < min || n > max) return `Must be ${fmtBounds(min, max)}.`;
  return null;
}

export function Switch({
  checked,
  onChange,
  children,
  disabled,
}: {
  checked: boolean;
  onChange: (v: boolean) => void;
  children?: ReactNode;
  disabled?: boolean;
}) {
  return (
    <label className="switch">
      <input
        type="checkbox"
        role="switch"
        checked={checked}
        disabled={disabled}
        onChange={(e) => onChange(e.target.checked)}
      />
      <span className="track" aria-hidden="true" />
      {children ? <span>{children}</span> : null}
    </label>
  );
}

export function Check({
  checked,
  onChange,
  children,
}: {
  checked: boolean;
  onChange: (v: boolean) => void;
  children?: ReactNode;
}) {
  return (
    <label className="check-row">
      <input
        type="checkbox"
        className="check"
        checked={checked}
        onChange={(e) => onChange(e.target.checked)}
      />
      {children ? <span>{children}</span> : null}
    </label>
  );
}

export function Select<T extends string>({
  value,
  options,
  onChange,
  id,
  label,
}: {
  value: T;
  options: { value: T; label: string }[];
  onChange: (v: T) => void;
  id?: string;
  label?: string;
}) {
  return (
    <span className="select-wrap">
      <select
        id={id}
        aria-label={label}
        className="select"
        value={value}
        onChange={(e) => onChange(e.target.value as T)}
      >
        {options.map((o) => (
          <option key={o.value} value={o.value}>
            {o.label}
          </option>
        ))}
      </select>
    </span>
  );
}
