import React, { useEffect, useRef } from 'react';

interface LogViewerProps {
  lines: string[];
  label?: string;
  maxHeight?: string;
  follow?: boolean; // auto-scroll to bottom on new lines
}

export function LogViewer({
  lines,
  label = 'Log output',
  maxHeight = '16rem',
  follow = true,
}: LogViewerProps) {
  const bottomRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (follow) bottomRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [lines, follow]);

  return (
    <div className="rounded-lg border border-surface-border overflow-hidden">
      {label && (
        <div className="px-3 py-1.5 bg-surface-base border-b border-surface-border text-xs font-medium text-slate-400 flex items-center justify-between">
          <span>{label}</span>
          <span className="tabular-nums text-slate-500">{lines.length} lines</span>
        </div>
      )}
      <div
        role="log"
        aria-label={label}
        aria-live="polite"
        className="overflow-y-auto bg-slate-950 p-3"
        style={{ maxHeight }}
      >
        {lines.length === 0 ? (
          <p className="text-xs text-slate-600 italic">No log output yet.</p>
        ) : (
          lines.map((line, i) => (
            <p
              key={i}
              className="font-mono text-xs text-slate-300 leading-relaxed whitespace-pre-wrap break-all"
            >
              {line}
            </p>
          ))
        )}
        <div ref={bottomRef} />
      </div>
    </div>
  );
}
