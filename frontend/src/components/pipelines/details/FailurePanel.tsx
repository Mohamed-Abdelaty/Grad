import React, { useState } from 'react';
import { AlertCircleIcon, ChevronDownIcon, ChevronUpIcon, PlayIcon } from 'lucide-react';
import { RunProgress, RunEvent } from '../../../types/runProgress';
import { stageLabel, formatEventMessage } from '../../../lib/format';
import { Button } from '../../ui/Button';

interface FailurePanelProps {
  progress: RunProgress | null;
  errorMessage?: string | null;
  events: RunEvent[];
  onRerun: () => void;
  canRerun: boolean;
}

export const FailurePanel = React.memo(function FailurePanel({
  progress,
  errorMessage,
  events,
  onRerun,
  canRerun,
}: FailurePanelProps) {
  const [isExpanded, setIsExpanded] = useState(false);

  // Find failing stage
  const failingStage = progress?.stages?.find((s) => s.state === 'error') || null;
  const stageName = failingStage ? stageLabel(failingStage.key) : 'Pipeline Execution';
  const displayError = failingStage?.error || errorMessage || 'An unexpected error occurred during execution.';

  // Filter last 20 warning or error events
  const issueEvents = events
    .map((e) => ({
      event: e,
      formatted: formatEventMessage(e),
    }))
    .filter((item) => item.formatted.level === 'error' || item.formatted.level === 'warn')
    .slice(-20);

  return (
    <div
      id="failure-panel"
      data-testid="failure-panel"
      role="region"
      aria-label="Failure details"
      className="p-5 bg-status-error/10 border border-status-error/40 rounded-xl space-y-4"
    >
      <div className="flex items-start justify-between gap-4 flex-wrap">
        <div className="flex items-start gap-3">
          <div className="p-2 bg-status-error/20 rounded-lg text-status-error mt-0.5">
            <AlertCircleIcon className="h-5 w-5" aria-hidden="true" />
          </div>
          <div>
            <h2 className="text-base font-bold text-slate-100 flex items-center gap-2">
              Failed at {stageName}
            </h2>
            <p className="mt-1 text-sm text-slate-300 font-mono break-all leading-relaxed">
              {displayError}
            </p>
          </div>
        </div>

        <Button
          variant="secondary"
          size="sm"
          onClick={onRerun}
          disabled={!canRerun}
          className="shrink-0"
        >
          <PlayIcon className="h-3.5 w-3.5 mr-1.5" />
          Re-run Pipeline
        </Button>
      </div>

      {issueEvents.length > 0 && (
        <div className="pt-2 border-t border-status-error/20">
          <button
            type="button"
            onClick={() => setIsExpanded(!isExpanded)}
            aria-expanded={isExpanded}
            className="flex items-center gap-1.5 text-xs font-medium text-slate-300 hover:text-white transition-colors"
          >
            {isExpanded ? (
              <ChevronUpIcon className="h-4 w-4" />
            ) : (
              <ChevronDownIcon className="h-4 w-4" />
            )}
            <span>
              {isExpanded ? 'Hide Error & Warning Details' : `Details (${issueEvents.length} events)`}
            </span>
          </button>

          {isExpanded && (
            <div className="mt-3 p-3 bg-slate-950/80 rounded-lg border border-surface-border text-xs font-mono space-y-1.5 max-h-60 overflow-y-auto">
              {issueEvents.map(({ event, formatted }) => (
                <div
                  key={event.seq}
                  className="flex items-start gap-2 text-slate-300 leading-relaxed"
                >
                  <span className="text-slate-500 tabular-nums select-none shrink-0">
                    {new Date(event.ts).toLocaleTimeString()}
                  </span>
                  <span
                    className={`px-1 rounded text-[10px] font-bold uppercase shrink-0 ${
                      formatted.level === 'error'
                        ? 'bg-status-error/20 text-status-error'
                        : 'bg-status-warning/20 text-status-warning'
                    }`}
                  >
                    {formatted.level}
                  </span>
                  <span className="break-all">{formatted.text}</span>
                </div>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  );
});
