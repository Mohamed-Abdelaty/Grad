import React, { useId } from 'react';

interface FormFieldProps {
  label: string;
  error?: string;
  hint?: string;
  required?: boolean;
  children: React.ReactElement;
}

export function FormField({ label, error, hint, required, children }: FormFieldProps) {
  const id = useId();
  const errorId = `${id}-error`;
  const hintId = `${id}-hint`;

  const child = React.cloneElement(children, {
    id,
    'aria-required': required,
    'aria-invalid': !!error,
    'aria-describedby': [error ? errorId : '', hint ? hintId : ''].filter(Boolean).join(' ') || undefined,
  });

  return (
    <div className="space-y-1">
      <label
        htmlFor={id}
        className="block text-sm font-medium text-slate-300"
      >
        {label}
        {required && <span className="ml-1 text-status-error" aria-hidden="true">*</span>}
      </label>
      {child}
      {hint && !error && (
        <p id={hintId} className="text-xs text-slate-500">{hint}</p>
      )}
      {error && (
        <p id={errorId} role="alert" className="text-xs text-status-error">{error}</p>
      )}
    </div>
  );
}
