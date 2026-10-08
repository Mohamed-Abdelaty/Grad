import React from 'react';

type StatusVariant = 'success' | 'warning' | 'error' | 'info' | 'neutral' | 'running';

interface StatusPillProps {
  status: StatusVariant | string;
  label?: string;
  dot?: boolean;
}

const styles: Record<string, string> = {
  success:  'bg-status-success/15 text-status-success border-status-success/30',
  warning:  'bg-status-warning/15 text-status-warning border-status-warning/30',
  error:    'bg-status-error/15   text-status-error   border-status-error/30',
  failed:   'bg-status-error/15   text-status-error   border-status-error/30',
  info:     'bg-status-info/15    text-status-info    border-status-info/30',
  running:  'bg-brand/15          text-brand-light    border-brand/30',
  pending:  'bg-status-neutral/10 text-status-neutral border-status-neutral/25',
  neutral:  'bg-status-neutral/10 text-status-neutral border-status-neutral/25',
  cancelled:'bg-status-neutral/10 text-status-neutral border-status-neutral/25',
  succeeded:'bg-status-success/15 text-status-success border-status-success/30',
};

const dotStyles: Record<string, string> = {
  success:   'bg-status-success',
  warning:   'bg-status-warning',
  error:     'bg-status-error',
  failed:    'bg-status-error',
  info:      'bg-status-info',
  running:   'bg-brand-light animate-pulse',
  pending:   'bg-status-neutral',
  neutral:   'bg-status-neutral',
  cancelled: 'bg-status-neutral',
  succeeded: 'bg-status-success',
};

export function StatusPill({ status, label, dot = false }: StatusPillProps) {
  const key = status?.toLowerCase() ?? 'neutral';
  const cls = styles[key] ?? styles.neutral;
  const dotCls = dotStyles[key] ?? dotStyles.neutral;

  return (
    <span
      className={[
        'inline-flex items-center gap-1.5 px-2.5 py-0.5 text-xs font-medium rounded-full border',
        cls,
      ].join(' ')}
    >
      {dot && <span className={['h-1.5 w-1.5 rounded-full', dotCls].join(' ')} />}
      {label ?? status}
    </span>
  );
}
