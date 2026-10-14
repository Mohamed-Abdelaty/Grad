import React, { useEffect, useRef } from 'react';
import { AlertTriangleIcon, InfoIcon, XCircleIcon } from 'lucide-react';

export interface LogEntry {
  id?: string | number;
  timestamp?: string;
  level?: 'info' | 'warn' | 'error';
  message: string;
}

export interface LogViewerProps {
  lines?: string[];
  entries?: LogEntry[];
  searchQuery?: string;
  label?: string;
  maxHeight?: string;
  follow?: boolean;
  onScroll?: (e: React.UIEvent<HTMLDivElement>) => void;
  onFollowChange?: (follow: boolean) => void;
  emptyMessage?: string;
  headerControls?: React.ReactNode;
}

function HighlightedText({ text, query }: { text: string; query?: string }) {
  if (!query || !query.trim()) return <>{text}</>;
  const trimmed = query.trim();
  const escaped = trimmed.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const parts = text.split(new RegExp(`(${escaped})`, 'gi'));
  return (
    <>
      {parts.map((part, i) =>
        part.toLowerCase() === trimmed.toLowerCase() ? (
          <mark key={i} className="bg-brand/30 text-brand-light px-0.5 rounded font-bold">
            {part}
          </mark>
        ) : (
          part
        )
      )}
    </>
  );
}

export function LogViewer({
  lines,
  entries,
  searchQuery,
  label = 'Log output',
  maxHeight = '16rem',
  follow = true,
  onScroll,
  onFollowChange,
  emptyMessage = 'No log output yet.',
  headerControls,
}: LogViewerProps) {
  const bottomRef = useRef<HTMLDivElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);

  const itemCount = entries ? entries.length : lines ? lines.length : 0;

  useEffect(() => {
    if (follow) {
      bottomRef.current?.scrollIntoView({ behavior: 'smooth' });
    }
  }, [itemCount, follow]);

  const handleScroll = (e: React.UIEvent<HTMLDivElement>) => {
    const el = e.currentTarget;
    const isAtBottom = el.scrollHeight - el.scrollTop - el.clientHeight < 25;
    if (onFollowChange && follow && !isAtBottom) {
      onFollowChange(false);
    }
    onScroll?.(e);
  };

  return (
    <div className="rounded-lg border border-surface-border overflow-hidden">
      {(label || headerControls) && (
        <div className="px-3 py-1.5 bg-surface-base border-b border-surface-border text-xs font-medium text-slate-400 flex items-center justify-between gap-2 flex-wrap">
          <div className="flex items-center gap-2">
            <span>{label}</span>
            <span className="tabular-nums text-slate-400">{itemCount} lines</span>
          </div>
          {headerControls && <div>{headerControls}</div>}
        </div>
      )}
      <div
        ref={containerRef}
        role="log"
        tabIndex={0}
        aria-label={label}
        aria-live="polite"
        onScroll={handleScroll}
        className="overflow-y-auto bg-slate-950 p-3 space-y-1 font-mono text-xs focus:outline-none focus:ring-1 focus:ring-brand/40"
        style={{ maxHeight }}
      >
        {itemCount === 0 ? (
          <p className="text-xs text-slate-400 italic font-sans">{emptyMessage}</p>
        ) : entries ? (
          entries.map((entry, i) => {
            const level = entry.level || 'info';
            return (
              <div
                key={entry.id ?? i}
                className="flex items-start gap-2 leading-relaxed hover:bg-slate-900/60 rounded px-1.5 py-0.5"
              >
                {entry.timestamp && (
                  <span className="text-slate-400 tabular-nums select-none shrink-0 text-[11px]">
                    {entry.timestamp}
                  </span>
                )}
                <span
                  className={`inline-flex items-center gap-1 px-1.5 py-0.2 rounded text-[10px] font-semibold uppercase tracking-wider shrink-0 select-none ${
                    level === 'error'
                      ? 'bg-status-error/20 text-status-error'
                      : level === 'warn'
                      ? 'bg-status-warning/20 text-status-warning'
                      : 'bg-brand/20 text-brand-light'
                  }`}
                >
                  {level === 'error' ? (
                    <XCircleIcon className="h-2.5 w-2.5" />
                  ) : level === 'warn' ? (
                    <AlertTriangleIcon className="h-2.5 w-2.5" />
                  ) : (
                    <InfoIcon className="h-2.5 w-2.5" />
                  )}
                  {level}
                </span>
                <span className="text-slate-300 whitespace-pre-wrap break-all flex-1">
                  <HighlightedText text={entry.message} query={searchQuery} />
                </span>
              </div>
            );
          })
        ) : (
          lines?.map((line, i) => (
            <p
              key={i}
              className="text-slate-300 leading-relaxed whitespace-pre-wrap break-all"
            >
              <HighlightedText text={line} query={searchQuery} />
            </p>
          ))
        )}
        <div ref={bottomRef} />
      </div>
    </div>
  );
}
