import React from 'react';
import { AlertTriangleIcon } from 'lucide-react';
import { Button } from './Button';

interface ErrorStateProps {
  title?: string;
  description?: string;
  onRetry?: () => void;
}

export function ErrorState({
  title = 'Something went wrong',
  description = 'An error occurred while loading this data.',
  onRetry,
}: ErrorStateProps) {
  return (
    <div className="flex flex-col items-center justify-center py-16 text-center gap-4 animate-fade-in">
      <div className="rounded-full bg-status-error/10 border border-status-error/30 p-5 text-status-error">
        <AlertTriangleIcon className="h-8 w-8" />
      </div>
      <div>
        <p className="text-base font-semibold text-slate-200">{title}</p>
        <p className="mt-1 text-sm text-slate-400 max-w-xs">{description}</p>
      </div>
      {onRetry && (
        <Button variant="secondary" onClick={onRetry}>
          Try again
        </Button>
      )}
    </div>
  );
}
