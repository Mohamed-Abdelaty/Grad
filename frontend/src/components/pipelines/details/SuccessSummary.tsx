import React, { useState } from 'react';
import { CheckCircle2Icon, CopyIcon, CheckIcon, ArrowRightIcon } from 'lucide-react';
import { RunProgress } from '../../../types/runProgress';
import { formatCount, formatDuration } from '../../../lib/format';
import { IconButton } from '../../ui/IconButton';
import { Button } from '../../ui/Button';
import { Tooltip } from '../../ui/Tooltip';
import { toast } from '../../ui/Toast';

interface SuccessSummaryProps {
  progress: RunProgress | null;
  outputPath?: string | null;
  elapsedMs: number;
  onViewOutput: () => void;
}

export const SuccessSummary = React.memo(function SuccessSummary({
  progress,
  outputPath,
  elapsedMs,
  onViewOutput,
}: SuccessSummaryProps) {
  const [copiedPath, setCopiedPath] = useState(false);

  const finalOutputPath =
    outputPath || progress?.hdfs?.path || '—';

  const handleCopyPath = async () => {
    if (!finalOutputPath || finalOutputPath === '—') return;
    try {
      await navigator.clipboard.writeText(finalOutputPath);
      setCopiedPath(true);
      toast.success('Output path copied');
      setTimeout(() => setCopiedPath(false), 2000);
    } catch {
      toast.error('Failed to copy output path');
    }
  };

  return (
    <div
      data-testid="success-summary"
      className="p-5 bg-status-success/10 border border-status-success/30 rounded-xl space-y-4"
    >
      <div className="flex items-center justify-between gap-4 flex-wrap">
        <div className="flex items-center gap-3">
          <div className="p-2 bg-status-success/20 rounded-lg text-status-success">
            <CheckCircle2Icon className="h-5 w-5" aria-hidden="true" />
          </div>
          <div>
            <h2 className="text-base font-bold text-slate-100">
              Pipeline Run Succeeded
            </h2>
            <p className="text-xs text-slate-400">
              All stages completed and output verified in HDFS.
            </p>
          </div>
        </div>

        <Button variant="primary" size="sm" onClick={onViewOutput}>
          View Output
          <ArrowRightIcon className="h-3.5 w-3.5 ml-1.5" />
        </Button>
      </div>

      <div className="grid grid-cols-2 md:grid-cols-4 gap-3 pt-2 text-xs">
        <div className="bg-surface-base/80 p-2.5 rounded-lg border border-surface-border">
          <span className="text-slate-400 block">Rows Written:</span>
          <span className="font-mono text-sm font-bold text-slate-100 tabular-nums">
            {formatCount(progress?.hdfs?.rowsWritten ?? null)}
          </span>
        </div>

        <div className="bg-surface-base/80 p-2.5 rounded-lg border border-surface-border">
          <span className="text-slate-400 block">Spark Records In / Out:</span>
          <span className="font-mono text-sm font-bold text-slate-100 tabular-nums">
            {formatCount(progress?.spark?.recordsIn ?? null)} / {formatCount(progress?.spark?.recordsOut ?? null)}
          </span>
        </div>

        <div className="bg-surface-base/80 p-2.5 rounded-lg border border-surface-border">
          <span className="text-slate-400 block">Total Duration:</span>
          <span className="font-mono text-sm font-bold text-slate-100 tabular-nums">
            {formatDuration(elapsedMs)}
          </span>
        </div>

        <div className="bg-surface-base/80 p-2.5 rounded-lg border border-surface-border">
          <div className="flex items-center justify-between text-slate-400">
            <span>Destination:</span>
            {finalOutputPath !== '—' && (
              <Tooltip content="Copy output path">
                <IconButton
                  label="Copy path"
                  size="sm"
                  onClick={handleCopyPath}
                  className="text-slate-400 hover:text-slate-200"
                >
                  {copiedPath ? (
                    <CheckIcon className="h-3 w-3 text-status-success" />
                  ) : (
                    <CopyIcon className="h-3 w-3" />
                  )}
                </IconButton>
              </Tooltip>
            )}
          </div>
          <span className="font-mono text-xs font-semibold text-slate-200 truncate block" title={finalOutputPath}>
            {finalOutputPath}
          </span>
        </div>
      </div>
    </div>
  );
});
