import React from 'react';

interface SkeletonProps {
  className?: string;
  lines?: number;
}

export function Skeleton({ className = '' }: SkeletonProps) {
  return (
    <div
      aria-hidden="true"
      className={['rounded bg-slate-700/60 animate-shimmer', className].join(' ')}
    />
  );
}

export function SkeletonCard() {
  return (
    <div
      role="status"
      aria-busy="true"
      aria-label="Loading…"
      className="bg-surface-raised border border-surface-border rounded-lg p-6 space-y-3"
    >
      <Skeleton className="h-4 w-1/3" />
      <Skeleton className="h-8 w-1/2" />
      <Skeleton className="h-3 w-3/4" />
    </div>
  );
}

export function SkeletonTable({ rows = 4 }: { rows?: number }) {
  return (
    <div
      role="status"
      aria-busy="true"
      aria-label="Loading table…"
      className="space-y-2"
    >
      <Skeleton className="h-9 w-full" />
      {Array.from({ length: rows }).map((_, i) => (
        <Skeleton key={i} className="h-12 w-full" />
      ))}
    </div>
  );
}
