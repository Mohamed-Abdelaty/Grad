import React from 'react';
import { RunProgress } from '../../../types/runProgress';
import { activeStageSummary, formatCount } from '../../../lib/format';
import { ProgressBar } from '../../ui/ProgressBar';

interface ActiveProgressProps {
  progress: RunProgress | null;
}

export const ActiveProgress = React.memo(function ActiveProgress({
  progress,
}: ActiveProgressProps) {
  if (!progress) {
    return (
      <div
        data-testid="active-progress"
        className="p-4 bg-surface-card border border-surface-border rounded-xl space-y-2 min-h-[88px]"
      >
        <span className="text-xs text-slate-500">Initializing run...</span>
      </div>
    );
  }

  // Streaming mode handler
  if (progress.mode === 'streaming') {
    const isDrained = progress.kafka.consumerLag === 0;
    return (
      <div
        data-testid="active-progress"
        className="p-4 bg-surface-card border border-surface-border rounded-xl space-y-3 min-h-[88px]"
      >
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-semibold bg-status-success/20 text-status-success border border-status-success/40">
              <span className="h-1.5 w-1.5 rounded-full bg-status-success animate-pulse" />
              Live Streaming
            </span>
            <span className="text-xs text-slate-400">
              {isDrained ? 'Stream drained' : `Draining Kafka: lag ${progress.kafka.consumerLag !== null ? formatCount(progress.kafka.consumerLag) : '—'}`}
            </span>
          </div>
          {progress.kafka.consumerLag !== null && (
            <div className="text-xs font-mono text-slate-300">
              Consumer Lag:{' '}
              <span className="text-brand-light font-bold">
                {formatCount(progress.kafka.consumerLag)}
              </span>
            </div>
          )}
        </div>
      </div>
    );
  }

  const summary = activeStageSummary(progress);
  const activeStage = progress.stages.find((s) => s.state === 'active');

  // Compute n / m text when determinate
  let ratioFractionText: string | null = null;
  if (!summary.indeterminate && summary.ratio !== null && activeStage) {
    if (activeStage.key === 'source') {
      ratioFractionText = `${formatCount(progress.source.rowsRead)} / ${formatCount(
        progress.source.rowsTotal
      )} rows`;
    } else if (activeStage.key === 'kafka') {
      ratioFractionText = `${formatCount(progress.kafka.published)} / ${formatCount(
        progress.source.rowsTotal
      )} records`;
    } else if (activeStage.key === 'spark' && progress.spark.activeStage) {
      ratioFractionText = `${formatCount(
        progress.spark.activeStage.tasksDone
      )} / ${formatCount(progress.spark.activeStage.tasksTotal)} tasks`;
    }
  }

  const progressBarValue = summary.indeterminate
    ? null
    : summary.ratio !== null
    ? Math.min(100, Math.max(0, Math.round(summary.ratio * 100)))
    : null;

  return (
    <div
      data-testid="active-progress"
      className="p-4 bg-surface-card border border-surface-border rounded-xl space-y-2 min-h-[88px]"
    >
      <div className="flex items-center justify-between text-xs">
        <span className="font-semibold text-slate-200">{summary.label}</span>
        {ratioFractionText && (
          <span className="text-slate-400 font-mono tabular-nums">
            {ratioFractionText}
          </span>
        )}
      </div>

      <ProgressBar
        value={progressBarValue}
        label={summary.label}
        showValue={!summary.indeterminate && progressBarValue !== null}
        color={activeStage?.state === 'error' ? 'error' : 'brand'}
      />

      {/* Spark tasks done line */}
      {activeStage?.key === 'spark' && progress.spark.tasksDone !== null && (
        <div className="text-xs text-slate-400 font-mono">
          Tasks done so far: {formatCount(progress.spark.tasksDone)} (total grows as stages start)
        </div>
      )}

      {/* Honest ETA: strictly only rendered when non-null */}
      {progress.etaSeconds !== null && (
        <div className="text-xs text-slate-400 font-mono">
          Estimated time remaining: ~{progress.etaSeconds} s (estimate)
        </div>
      )}
    </div>
  );
});
