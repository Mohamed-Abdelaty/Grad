import React, { useState } from 'react';
import { CopyIcon, CheckIcon, FolderArchiveIcon } from 'lucide-react';
import { RunProgress } from '../../../types/runProgress';
import { EmptyState } from '../../ui/EmptyState';
import { IconButton } from '../../ui/IconButton';
import { Tooltip } from '../../ui/Tooltip';
import { toast } from '../../ui/Toast';
import { formatCount, formatBytes } from '../../../lib/format';

interface OutputTabProps {
  progress: RunProgress | null;
  actualOutputPath?: string | null;
  fallbackOutputPath?: string | null;
}

export const OutputTab = React.memo(function OutputTab({
  progress,
  actualOutputPath,
  fallbackOutputPath,
}: OutputTabProps) {
  const [copied, setCopied] = useState(false);

  const destinationPath =
    actualOutputPath || progress?.hdfs?.path || fallbackOutputPath || '—';

  const handleCopy = async () => {
    if (!destinationPath || destinationPath === '—') return;
    try {
      await navigator.clipboard.writeText(destinationPath);
      setCopied(true);
      toast.success('Output path copied');
      setTimeout(() => setCopied(false), 2000);
    } catch {
      toast.error('Failed to copy output path');
    }
  };

  return (
    <div data-testid="output-tab" className="space-y-4">
      {/* Output Metadata Box */}
      <div className="p-4 bg-surface-card border border-surface-border rounded-xl space-y-3">
        <div className="flex items-center gap-2 text-sm font-semibold text-slate-200">
          <FolderArchiveIcon className="h-4 w-4 text-brand-light" />
          <span>Storage Destination & Output Metadata</span>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-3 gap-3 text-xs">
          <div className="md:col-span-1 bg-surface-base p-3 rounded-lg border border-surface-border">
            <div className="flex items-center justify-between text-slate-400">
              <span>HDFS Path:</span>
              {destinationPath !== '—' && (
                <Tooltip content="Copy HDFS output path">
                  <IconButton
                    label="Copy path"
                    size="sm"
                    onClick={handleCopy}
                    className="text-slate-400 hover:text-slate-200"
                  >
                    {copied ? (
                      <CheckIcon className="h-3.5 w-3.5 text-status-success" />
                    ) : (
                      <CopyIcon className="h-3.5 w-3.5" />
                    )}
                  </IconButton>
                </Tooltip>
              )}
            </div>
            <code className="font-mono text-slate-200 text-xs break-all block mt-1" title={destinationPath}>
              {destinationPath}
            </code>
          </div>

          <div className="bg-surface-base p-3 rounded-lg border border-surface-border">
            <span className="text-slate-400 block">Files Written:</span>
            <span className="font-mono text-base font-bold text-slate-100 tabular-nums block mt-1">
              {formatCount(progress?.hdfs?.filesWritten ?? null)}
            </span>
          </div>

          <div className="bg-surface-base p-3 rounded-lg border border-surface-border">
            <span className="text-slate-400 block">Total Bytes Written:</span>
            <span className="font-mono text-base font-bold text-slate-100 tabular-nums block mt-1">
              {formatBytes(progress?.hdfs?.bytesWritten ?? null)}
            </span>
          </div>
        </div>
      </div>

      {/* EmptyState until UI-6 */}
      <div className="p-8 bg-surface-card border border-surface-border rounded-xl flex items-center justify-center min-h-[220px]">
        <EmptyState
          title="Output preview is added in the next phase"
          description="Data file browsing and tabular preview will be available in UI-6."
        />
      </div>
    </div>
  );
});
