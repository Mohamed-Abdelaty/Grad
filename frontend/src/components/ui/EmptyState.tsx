import React from 'react';
import { InboxIcon } from 'lucide-react';

interface EmptyStateProps {
  title?: string;
  description?: string;
  action?: React.ReactNode;
  icon?: React.ReactNode;
}

export function EmptyState({
  title = 'No data yet',
  description,
  action,
  icon,
}: EmptyStateProps) {
  return (
    <div className="flex flex-col items-center justify-center py-16 text-center gap-4 animate-fade-in">
      <div className="rounded-full bg-surface-raised border border-surface-border p-5 text-slate-500">
        {icon ?? <InboxIcon className="h-8 w-8" />}
      </div>
      <div>
        <p className="text-base font-semibold text-slate-300">{title}</p>
        {description && <p className="mt-1 text-sm text-slate-400 max-w-xs">{description}</p>}
      </div>
      {action}
    </div>
  );
}
