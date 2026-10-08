import React from 'react';

interface ProgressBarProps {
  value: number | null; // 0–100, null = indeterminate
  label?: string;
  color?: 'brand' | 'success' | 'warning' | 'error';
  showValue?: boolean;
}

const colorClasses = {
  brand:   'bg-brand',
  success: 'bg-status-success',
  warning: 'bg-status-warning',
  error:   'bg-status-error',
};

export function ProgressBar({
  value,
  label,
  color = 'brand',
  showValue = false,
}: ProgressBarProps) {
  const isIndeterminate = value === null;
  const pct = Math.max(0, Math.min(100, value ?? 0));

  return (
    <div className="space-y-1">
      {(label || showValue) && (
        <div className="flex justify-between text-xs text-slate-400">
          {label && <span>{label}</span>}
          {showValue && !isIndeterminate && <span>{pct.toFixed(0)}%</span>}
          {showValue && isIndeterminate && <span>—</span>}
        </div>
      )}
      <div
        role="progressbar"
        aria-valuenow={isIndeterminate ? undefined : pct}
        aria-valuemin={0}
        aria-valuemax={100}
        aria-label={label}
        className="h-2 w-full rounded-full bg-surface-border overflow-hidden"
      >
        <div
          className={[
            'h-full rounded-full transition-all duration-slow',
            colorClasses[color],
            isIndeterminate ? 'w-1/3 animate-shimmer' : '',
          ].join(' ')}
          style={isIndeterminate ? undefined : { width: `${pct}%` }}
        />
      </div>
    </div>
  );
}
