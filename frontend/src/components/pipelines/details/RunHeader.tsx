import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import {
  ArrowLeftIcon,
  CheckIcon,
  ClockIcon,
  CopyIcon,
  DownloadIcon,
  PlayIcon,
  XCircleIcon,
} from 'lucide-react';
import { RunProgress } from '../../../types/runProgress';
import { PipelineRunListItem } from '../../../types/pipeline';
import { StatusPill } from '../../ui/StatusPill';
import { Button } from '../../ui/Button';
import { IconButton } from '../../ui/IconButton';
import { Tooltip } from '../../ui/Tooltip';
import { ConfirmDialog } from '../../ui/ConfirmDialog';
import { toast } from '../../ui/Toast';
import { formatDuration, safeFileName } from '../../../lib/format';
import { cancelRun } from '../../../api/pipelines';

interface RunHeaderProps {
  run: PipelineRunListItem | null;
  progress: RunProgress | null;
  connection: 'connecting' | 'live' | 'reconnecting' | 'polling' | 'closed';
  lastUpdatedAt: number | null;
  elapsedMs: number;
  onRefreshRun?: () => void;
}

export const RunHeader = React.memo(function RunHeader({
  run,
  progress,
  connection,
  lastUpdatedAt,
  elapsedMs,
  onRefreshRun,
}: RunHeaderProps) {
  const navigate = useNavigate();
  const [isCancelConfirmOpen, setIsCancelConfirmOpen] = useState(false);
  const [isCancelling, setIsCancelling] = useState(false);
  const [copiedId, setCopiedId] = useState(false);

  const status = progress?.status || run?.status || 'pending';
  const runId = progress?.runId || run?.id || '';
  const pipelineName = run?.name || progress?.runId || 'Pipeline Run';
  const yamlContent = run?.yamlContent || null;
  const isRunning = status === 'running';

  const handleCopyId = async () => {
    if (!runId) return;
    try {
      await navigator.clipboard.writeText(runId);
      setCopiedId(true);
      toast.success('Run ID copied to clipboard');
      setTimeout(() => setCopiedId(false), 2000);
    } catch {
      toast.error('Failed to copy Run ID');
    }
  };

  const handleCancel = async () => {
    if (!runId) return;
    setIsCancelling(true);
    try {
      const res = await cancelRun(runId);
      if (res.accepted) {
        toast.success('Cancellation requested');
        onRefreshRun?.();
      } else {
        toast.error(res.detail || 'Cancel request not accepted');
      }
    } catch (err) {
      toast.error(err instanceof Error ? err.message : 'Failed to cancel run');
    } finally {
      setIsCancelling(false);
      setIsCancelConfirmOpen(false);
    }
  };

  const handleRerun = () => {
    if (!yamlContent) return;
    navigate('/pipelines', { state: { yaml: yamlContent } });
  };

  const handleDownloadYaml = () => {
    if (!yamlContent) return;
    const blob = new Blob([yamlContent], { type: 'text/yaml;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = safeFileName(pipelineName);
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
    toast.success(`Downloaded ${safeFileName(pipelineName)}`);
  };

  const secondsAgo = lastUpdatedAt
    ? Math.max(0, Math.floor((Date.now() - lastUpdatedAt) / 1000))
    : null;

  const connectionColors = {
    live: 'bg-status-success/15 text-status-success border-status-success/30',
    reconnecting: 'bg-status-warning/15 text-status-warning border-status-warning/30',
    polling: 'bg-blue-500/15 text-blue-400 border-blue-500/30',
    connecting: 'bg-blue-500/15 text-blue-400 border-blue-500/30',
    closed: 'bg-slate-700/30 text-slate-400 border-slate-700/50',
  };

  const connectionLabels = {
    live: 'Live',
    reconnecting: 'Reconnecting',
    polling: 'Polling',
    connecting: 'Connecting',
    closed: 'Closed',
  };

  return (
    <div
      data-testid="run-header"
      className="p-4 bg-surface-card border border-surface-border rounded-xl space-y-3"
    >
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="flex items-center gap-3 flex-wrap">
          <Button
            variant="ghost"
            size="sm"
            onClick={() => navigate('/pipelines')}
            className="text-slate-400 hover:text-slate-200 -ml-2"
          >
            <ArrowLeftIcon className="h-4 w-4 mr-1" />
            Pipelines
          </Button>

          <h1 className="text-xl font-bold text-slate-100">{pipelineName}</h1>

          <StatusPill status={status} label={status} dot={isRunning} />

          {progress?.mode === 'streaming' && (
            <span
              data-testid="streaming-badge"
              className="inline-flex items-center gap-1 px-2.5 py-0.5 rounded-full text-xs font-semibold bg-brand/20 text-brand-light border border-brand/40 uppercase tracking-wide"
            >
              STREAMING
            </span>
          )}

          <div className="flex items-center gap-1.5 text-xs text-slate-400 bg-surface-base px-2.5 py-1 rounded-md border border-surface-border">
            <ClockIcon className="h-3.5 w-3.5 text-slate-500" />
            <span className="tabular-nums font-mono">
              {formatDuration(elapsedMs)}
            </span>
          </div>

          <div
            data-testid="connection-badge"
            className={`inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded-full text-xs font-medium border ${
              connectionColors[connection] || connectionColors.closed
            }`}
          >
            <span
              className={`h-1.5 w-1.5 rounded-full ${
                connection === 'live'
                  ? 'bg-status-success animate-pulse'
                  : connection === 'reconnecting'
                  ? 'bg-status-warning'
                  : connection === 'polling'
                  ? 'bg-blue-400'
                  : 'bg-slate-500'
              }`}
            />
            <span>{connectionLabels[connection] || 'Closed'}</span>
            {secondsAgo !== null && (
              <span className="text-[10px] opacity-75 font-normal">
                updated {secondsAgo}s ago
              </span>
            )}
          </div>
        </div>

        <div className="flex items-center gap-2 flex-wrap">
          {isRunning && (
            <Button
              variant="danger"
              size="sm"
              onClick={() => setIsCancelConfirmOpen(true)}
              disabled={isCancelling}
            >
              <XCircleIcon className="h-4 w-4 mr-1.5" />
              {isCancelling ? 'Cancelling...' : 'Cancel'}
            </Button>
          )}

          {yamlContent ? (
            <Button variant="secondary" size="sm" onClick={handleRerun}>
              <PlayIcon className="h-3.5 w-3.5 mr-1.5" />
              Re-run
            </Button>
          ) : (
            <Tooltip content="The configuration of this run was not stored">
              <span>
                <Button variant="secondary" size="sm" disabled>
                  <PlayIcon className="h-3.5 w-3.5 mr-1.5" />
                  Re-run
                </Button>
              </span>
            </Tooltip>
          )}

          {yamlContent && (
            <Tooltip content="Download pipeline YAML">
              <IconButton
                label="Download YAML"
                size="sm"
                onClick={handleDownloadYaml}
              >
                <DownloadIcon className="h-4 w-4" />
              </IconButton>
            </Tooltip>
          )}
        </div>
      </div>

      <div className="flex items-center gap-2 text-xs text-slate-400 pt-1 border-t border-surface-border/50">
        <span className="text-slate-500">Run ID:</span>
        <code className="font-mono text-slate-300 bg-surface-base px-2 py-0.5 rounded border border-surface-border select-all">
          {runId}
        </code>
        <Tooltip content="Copy Run ID">
          <IconButton
            label="Copy Run ID"
            size="sm"
            onClick={handleCopyId}
            className="text-slate-400 hover:text-slate-200"
          >
            {copiedId ? (
              <CheckIcon className="h-3.5 w-3.5 text-status-success" />
            ) : (
              <CopyIcon className="h-3.5 w-3.5" />
            )}
          </IconButton>
        </Tooltip>
      </div>

      <ConfirmDialog
        open={isCancelConfirmOpen}
        title="Cancel Pipeline Run"
        description="Are you sure you want to stop this pipeline execution? Active tasks will be cancelled."
        confirmLabel={isCancelling ? 'Cancelling...' : 'Yes, Cancel Run'}
        confirmVariant="danger"
        loading={isCancelling}
        onConfirm={handleCancel}
        onClose={() => setIsCancelConfirmOpen(false)}
      />
    </div>
  );
});
