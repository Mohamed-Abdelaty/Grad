import React from 'react';

interface StatCardProps {
  label: string;
  value: string | number | null;
  sub?: string;
  accent?: 'brand' | 'success' | 'warning' | 'error' | 'neutral';
}

const accentClasses: Record<string, string> = {
  brand:   'text-brand-light',
  success: 'text-status-success',
  warning: 'text-status-warning',
  error:   'text-status-error',
  neutral: 'text-slate-200',
};

export function StatCard({ label, value, sub, accent = 'brand' }: StatCardProps) {
  return (
    <div className="bg-surface-raised border border-surface-border rounded-lg p-5 flex flex-col gap-1">
      <p className="text-xs font-medium text-slate-400 uppercase tracking-wider">{label}</p>
      <p className={['text-3xl font-bold font-mono tabular-nums', accentClasses[accent]].join(' ')}>
        {value ?? '—'}
      </p>
      {sub && <p className="text-xs text-slate-500 mt-0.5">{sub}</p>}
    </div>
  );
}
