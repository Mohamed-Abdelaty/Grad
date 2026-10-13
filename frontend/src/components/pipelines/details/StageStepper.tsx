import React from 'react';
import {
  AlertTriangleIcon,
  CheckCircle2Icon,
  CheckIcon,
  CircleDashedIcon,
  Loader2Icon,
  MinusCircleIcon,
} from 'lucide-react';
import { StageKey, StageProgress } from '../../../types/runProgress';
import {
  stageLabel,
  stageStateLabel,
  formatDuration,
} from '../../../lib/format';

interface StageStepperProps {
  stages: StageProgress[];
  hdfsVerified: boolean | null;
  failingStageKey?: StageKey | null;
}

const STAGE_ORDER: StageKey[] = ['source', 'kafka', 'spark', 'hdfs'];

export const StageStepper = React.memo(function StageStepper({
  stages,
  hdfsVerified,
  failingStageKey,
}: StageStepperProps) {
  const stageMap = new Map<StageKey, StageProgress>();
  for (const st of stages) {
    stageMap.set(st.key, st);
  }

  return (
    <div className="w-full bg-surface-card border border-surface-border rounded-xl p-4 min-h-[96px]">
      <ol
        data-testid="stage-stepper"
        className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-3 list-none m-0 p-0"
      >
        {STAGE_ORDER.map((key, index) => {
          const stage = stageMap.get(key) || {
            key,
            state: 'pending',
            startedAt: null,
            finishedAt: null,
            error: null,
          };
          const state = stage.state;
          const isError = state === 'error' || failingStageKey === key;

          // Duration if finished or running
          let durationStr: string | null = null;
          if (stage.startedAt && stage.finishedAt) {
            const ms = Math.max(
              0,
              new Date(stage.finishedAt).getTime() - new Date(stage.startedAt).getTime()
            );
            durationStr = formatDuration(ms);
          }

          return (
            <li
              key={key}
              data-testid={`stage-${key}`}
              data-state={state}
              aria-describedby={isError ? 'failure-panel' : undefined}
              className={`relative flex items-center gap-3 p-3 rounded-lg border transition-all ${
                isError
                  ? 'bg-status-error/10 border-status-error/40 text-status-error'
                  : state === 'active'
                  ? 'bg-brand/10 border-brand/40 text-brand-light'
                  : state === 'done'
                  ? 'bg-surface-base/80 border-surface-border text-slate-200'
                  : 'bg-surface-base/40 border-surface-border/40 text-slate-400'
              }`}
            >
              <div className="flex items-center justify-center h-8 w-8 rounded-full shrink-0 bg-surface-base border border-surface-border">
                {state === 'done' ? (
                  <CheckCircle2Icon
                    className="h-4 w-4 text-status-success"
                    aria-hidden="true"
                  />
                ) : state === 'active' ? (
                  <Loader2Icon
                    className="h-4 w-4 text-brand-light animate-spin motion-reduce:animate-none"
                    aria-hidden="true"
                  />
                ) : isError ? (
                  <AlertTriangleIcon
                    className="h-4 w-4 text-status-error"
                    aria-hidden="true"
                  />
                ) : state === 'skipped' ? (
                  <MinusCircleIcon
                    className="h-4 w-4 text-slate-500"
                    aria-hidden="true"
                  />
                ) : (
                  <CircleDashedIcon
                    className="h-4 w-4 text-slate-600"
                    aria-hidden="true"
                  />
                )}
              </div>

              <div className="flex-1 min-w-0">
                <div className="flex items-center justify-between gap-1">
                  <span className="text-xs text-slate-500 font-mono">
                    Step {index + 1}
                  </span>
                  {durationStr && (
                    <span className="text-[11px] text-slate-400 font-mono tabular-nums">
                      {durationStr}
                    </span>
                  )}
                </div>

                <div className="flex items-center gap-1.5 font-semibold text-sm truncate">
                  <span>{stageLabel(key)}</span>
                  {key === 'hdfs' && hdfsVerified === true && (
                    <span
                      data-testid="hdfs-verified-badge"
                      className="inline-flex items-center gap-0.5 text-[11px] font-normal text-status-success bg-status-success/10 px-1.5 py-0.2 rounded"
                    >
                      <CheckIcon className="h-3 w-3" aria-hidden="true" />
                      verified
                    </span>
                  )}
                </div>

                <div className="text-xs text-slate-400 capitalize">
                  {stageStateLabel(state)}
                </div>
              </div>
            </li>
          );
        })}
      </ol>
    </div>
  );
});
