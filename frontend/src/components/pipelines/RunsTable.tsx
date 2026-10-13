import React, { useEffect, useMemo, useState } from 'react';
import { useNavigate, useSearchParams } from 'react-router-dom';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import {
  ArrowUpDownIcon,
  CopyIcon,
  DownloadIcon,
  ExternalLinkIcon,
  RotateCcwIcon,
  SearchIcon,
  SquareIcon,
  XIcon,
} from 'lucide-react';
import { toast } from 'sonner';

import { listRuns, cancelRun } from '../../api/pipelines';
import { PipelineRunListItem, RunStatus } from '../../types/pipeline';
import { safeFileName } from '../../lib/yaml';
import { Card, CardHeader, CardTitle } from '../ui/Card';
import { DataTable, Column } from '../ui/DataTable';
import { StatusPill } from '../ui/StatusPill';
import { Button } from '../ui/Button';
import { IconButton } from '../ui/IconButton';
import { Tooltip } from '../ui/Tooltip';
import { ConfirmDialog } from '../ui/ConfirmDialog';
import { EmptyState } from '../ui/EmptyState';
import { ErrorState } from '../ui/ErrorState';
import { Skeleton } from '../ui/Skeleton';

const ALL_STATUSES: RunStatus[] = ['pending', 'running', 'succeeded', 'failed', 'cancelled'];

export interface RunsTableProps {
  onRerun?: (yamlContent: string) => void;
  onFocusTemplates?: () => void;
}

function formatRelativeTime(dateStr: string | null): string {
  if (!dateStr) return '-';
  try {
    const date = new Date(dateStr);
    const now = new Date();
    const diffSec = Math.round((date.getTime() - now.getTime()) / 1000);
    const rtf = new Intl.RelativeTimeFormat('en', { numeric: 'auto' });
    if (Math.abs(diffSec) < 60) return rtf.format(diffSec, 'second');
    const diffMin = Math.round(diffSec / 60);
    if (Math.abs(diffMin) < 60) return rtf.format(diffMin, 'minute');
    const diffHour = Math.round(diffMin / 60);
    if (Math.abs(diffHour) < 24) return rtf.format(diffHour, 'hour');
    const diffDay = Math.round(diffHour / 24);
    return rtf.format(diffDay, 'day');
  } catch {
    return dateStr;
  }
}

function formatDuration(
  startedAt: string | null,
  finishedAt: string | null,
  nowMs: number
): string {
  if (!startedAt) return '-';
  const start = new Date(startedAt).getTime();
  if (isNaN(start)) return '-';
  const end = finishedAt ? new Date(finishedAt).getTime() : nowMs;
  if (isNaN(end) || end < start) return '-';
  const totalSec = Math.floor((end - start) / 1000);
  const min = Math.floor(totalSec / 60);
  const sec = totalSec % 60;
  if (min === 0) return `${sec}s`;
  return `${min}m ${sec}s`;
}

export function RunsTable({ onRerun, onFocusTemplates }: RunsTableProps) {
  const navigate = useNavigate();
  const [searchParams, setSearchParams] = useSearchParams();
  const queryClient = useQueryClient();

  // Read params from URL query string
  const urlStatus = searchParams.get('status');
  const selectedStatuses = useMemo<RunStatus[]>(() => {
    if (!urlStatus) return [];
    return urlStatus
      .split(',')
      .map((s) => s.trim().toLowerCase())
      .filter((s): s is RunStatus => ALL_STATUSES.includes(s as RunStatus));
  }, [urlStatus]);

  const rawQ = searchParams.get('q') || '';
  const [searchInput, setSearchInput] = useState(rawQ);
  const [debouncedQ, setDebouncedQ] = useState(rawQ);

  useEffect(() => {
    const timer = setTimeout(() => {
      setDebouncedQ(searchInput);
      setSearchParams(
        (prev) => {
          const next = new URLSearchParams(prev);
          if (searchInput.trim()) {
            next.set('q', searchInput.trim());
          } else {
            next.delete('q');
          }
          next.set('page', '1');
          return next;
        },
        { replace: true }
      );
    }, 300);
    return () => clearTimeout(timer);
  }, [searchInput, setSearchParams]);

  const rawSort = searchParams.get('sort');
  const sort = rawSort === 'started_at_asc' ? 'started_at_asc' : 'started_at_desc';

  const rawPage = parseInt(searchParams.get('page') || '1', 10);
  const page = !isNaN(rawPage) && rawPage > 0 ? rawPage : 1;

  const rawLimit = parseInt(searchParams.get('limit') || '25', 10);
  const pageSize = [10, 25, 50].includes(rawLimit) ? rawLimit : 25;

  const offset = (page - 1) * pageSize;

  // Live ticker for running rows
  const [nowMs, setNowMs] = useState(Date.now());

  // Stop confirmation dialog state
  const [stopRunTarget, setStopRunTarget] = useState<PipelineRunListItem | null>(null);

  // Fetch runs
  const { data, isLoading, isError, refetch } = useQuery({
    queryKey: ['pipelineRuns', selectedStatuses, debouncedQ, sort, pageSize, offset],
    queryFn: () =>
      listRuns({
        status: selectedStatuses.length > 0 ? selectedStatuses : undefined,
        q: debouncedQ || undefined,
        sort,
        limit: pageSize,
        offset,
      }),
    refetchInterval: (query) => {
      if (typeof document !== 'undefined' && document.visibilityState !== 'visible') {
        return false;
      }
      const items = query.state.data?.items;
      const hasRunning = items?.some((r) => r.status === 'running');
      return hasRunning ? 3000 : false;
    },
    placeholderData: (prev) => prev,
  });

  const runs = data?.items || [];
  const totalCount = data?.total ?? 0;
  const totalPages = Math.max(1, Math.ceil(totalCount / pageSize));
  const hasRunningRows = runs.some((r) => r.status === 'running');

  // Live ticker effect when running rows exist
  useEffect(() => {
    if (!hasRunningRows) return;
    const interval = setInterval(() => {
      setNowMs(Date.now());
    }, 1000);
    return () => clearInterval(interval);
  }, [hasRunningRows]);

  // Cancel mutation
  const cancelMutation = useMutation({
    mutationFn: (runId: string) => cancelRun(runId),
    onSuccess: (_, runId) => {
      toast.success(`Cancellation requested for run ${runId}`);
      queryClient.invalidateQueries({ queryKey: ['pipelineRuns'] });
      setStopRunTarget(null);
    },
    onError: (err) => {
      toast.error(err instanceof Error ? err.message : 'Failed to stop pipeline run');
      setStopRunTarget(null);
    },
  });

  const handleToggleStatus = (st: RunStatus) => {
    const nextStatuses = selectedStatuses.includes(st)
      ? selectedStatuses.filter((s) => s !== st)
      : [...selectedStatuses, st];

    setSearchParams(
      (prev) => {
        const next = new URLSearchParams(prev);
        if (nextStatuses.length > 0) {
          next.set('status', nextStatuses.join(','));
        } else {
          next.delete('status');
        }
        next.set('page', '1');
        return next;
      },
      { replace: true }
    );
  };

  const handleToggleSort = () => {
    const nextSort = sort === 'started_at_desc' ? 'started_at_asc' : 'started_at_desc';
    setSearchParams(
      (prev) => {
        const next = new URLSearchParams(prev);
        next.set('sort', nextSort);
        return next;
      },
      { replace: true }
    );
  };

  const handlePageChange = (newPage: number) => {
    setSearchParams(
      (prev) => {
        const next = new URLSearchParams(prev);
        next.set('page', String(newPage));
        return next;
      },
      { replace: true }
    );
  };

  const handlePageSizeChange = (newSize: number) => {
    setSearchParams(
      (prev) => {
        const next = new URLSearchParams(prev);
        next.set('limit', String(newSize));
        next.set('page', '1');
        return next;
      },
      { replace: true }
    );
  };

  const handleClearFilters = () => {
    setSearchInput('');
    setDebouncedQ('');
    setSearchParams(
      (prev) => {
        const next = new URLSearchParams();
        next.set('page', '1');
        next.set('limit', String(pageSize));
        return next;
      },
      { replace: true }
    );
  };

  const handleDownloadYaml = (run: PipelineRunListItem) => {
    if (!run.yamlContent) return;
    const blob = new Blob([run.yamlContent], { type: 'text/yaml' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = safeFileName(run.name);
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
    toast.success(`Downloaded ${safeFileName(run.name)}`);
  };

  const handleCopyPath = (path: string | null) => {
    if (!path) return;
    navigator.clipboard.writeText(path);
    toast.success('Output path copied to clipboard');
  };

  const hasFilterActive = selectedStatuses.length > 0 || Boolean(debouncedQ.trim());

  const columns: Column<PipelineRunListItem>[] = [
    {
      key: 'name',
      header: 'Pipeline',
      cell: (run) => (
        <span className="font-medium text-slate-100 flex items-center gap-1.5">
          {run.name}
        </span>
      ),
    },
    {
      key: 'status',
      header: 'Status',
      cell: (run) => (
        <StatusPill
          status={run.status}
          label={run.status}
          dot={run.status === 'running'}
        />
      ),
    },
    {
      key: 'started',
      header: (
        <button
          type="button"
          onClick={handleToggleSort}
          className="inline-flex items-center gap-1 text-xs font-semibold uppercase tracking-wider text-slate-400 hover:text-slate-200 transition-colors"
          title={`Sort by started time (${sort === 'started_at_desc' ? 'descending' : 'ascending'})`}
        >
          Started
          <ArrowUpDownIcon className="h-3.5 w-3.5" />
        </button>
      ) as any,
      cell: (run) => (
        <span
          title={run.startedAt ? new Date(run.startedAt).toLocaleString() : '-'}
          className="text-slate-300 cursor-help"
        >
          {formatRelativeTime(run.startedAt)}
        </span>
      ),
    },
    {
      key: 'duration',
      header: 'Duration',
      cell: (run) => (
        <span className="text-slate-300 font-mono text-xs">
          {formatDuration(run.startedAt, run.finishedAt, nowMs)}
        </span>
      ),
    },
    {
      key: 'records',
      header: 'Records',
      cell: () => <span className="text-slate-400 font-mono text-xs">-</span>,
    },
    {
      key: 'output',
      header: 'Output',
      cell: (run) => {
        const out = run.actualOutputPath || run.outputPath;
        if (!out) return <span className="text-slate-500">-</span>;
        return (
          <div className="flex items-center gap-1.5 max-w-[200px]">
            <span className="truncate font-mono text-xs text-slate-300" title={out}>
              {out}
            </span>
            <IconButton
              aria-label="Copy output path"
              size="sm"
              variant="ghost"
              onClick={() => handleCopyPath(out)}
            >
              <CopyIcon className="h-3 w-3" />
            </IconButton>
          </div>
        );
      },
    },
    {
      key: 'actions',
      header: 'Actions',
      cell: (run) => (
        <div className="flex items-center gap-1">
          <Tooltip content="Open run details">
            <IconButton
              aria-label="Open"
              size="sm"
              variant="ghost"
              onClick={() => navigate(`/pipelines/${run.id}`)}
            >
              <ExternalLinkIcon className="h-3.5 w-3.5" />
            </IconButton>
          </Tooltip>

          {run.yamlContent ? (
            <Tooltip content="Re-run with this configuration">
              <IconButton
                aria-label="Re-run"
                size="sm"
                variant="ghost"
                onClick={() => onRerun?.(run.yamlContent!)}
              >
                <RotateCcwIcon className="h-3.5 w-3.5 text-brand-light" />
              </IconButton>
            </Tooltip>
          ) : (
            <Tooltip content="YAML content not recorded for this run">
              <span>
                <IconButton
                  aria-label="Re-run"
                  size="sm"
                  variant="ghost"
                  disabled
                >
                  <RotateCcwIcon className="h-3.5 w-3.5 text-slate-600" />
                </IconButton>
              </span>
            </Tooltip>
          )}

          {run.yamlContent && (
            <Tooltip content="Download YAML configuration">
              <IconButton
                aria-label="Download YAML"
                size="sm"
                variant="ghost"
                onClick={() => handleDownloadYaml(run)}
              >
                <DownloadIcon className="h-3.5 w-3.5 text-slate-400" />
              </IconButton>
            </Tooltip>
          )}

          {run.status === 'running' && (
            <Tooltip content="Stop running pipeline">
              <IconButton
                aria-label="Stop"
                size="sm"
                variant="danger"
                onClick={() => setStopRunTarget(run)}
              >
                <SquareIcon className="h-3.5 w-3.5 fill-current" />
              </IconButton>
            </Tooltip>
          )}
        </div>
      ),
    },
  ];

  return (
    <Card data-testid="runs-table" padding="none" className="overflow-hidden">
      <CardHeader className="p-4 border-b border-surface-border mb-0">
        <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 w-full">
          <CardTitle>Pipeline runs</CardTitle>

          {/* Search input */}
          <div className="relative w-full sm:w-64">
            <SearchIcon className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-slate-400" />
            <input
              data-testid="runs-search"
              type="text"
              placeholder="Search by pipeline name..."
              value={searchInput}
              onChange={(e) => setSearchInput(e.target.value)}
              className="w-full bg-surface-base border border-surface-border rounded-md pl-9 pr-8 py-1.5 text-sm text-slate-200 placeholder-slate-500 focus:outline-none focus:border-brand focus:ring-1 focus:ring-brand"
            />
            {searchInput && (
              <button
                type="button"
                onClick={() => setSearchInput('')}
                aria-label="Clear search"
                className="absolute right-2.5 top-1/2 -translate-y-1/2 text-slate-500 hover:text-slate-300"
              >
                <XIcon className="h-3.5 w-3.5" />
              </button>
            )}
          </div>
        </div>
      </CardHeader>

      {/* Filter status chips */}
      <div className="flex items-center gap-2 px-4 py-2.5 bg-surface-base/50 border-b border-surface-border overflow-x-auto">
        <span className="text-xs font-medium text-slate-400 shrink-0">Status:</span>
        {ALL_STATUSES.map((st) => {
          const isSelected = selectedStatuses.includes(st);
          return (
            <button
              key={st}
              type="button"
              data-testid={`filter-status-${st}`}
              onClick={() => handleToggleStatus(st)}
              className={[
                'px-2.5 py-1 text-xs font-medium rounded-full border transition-colors shrink-0',
                isSelected
                  ? 'bg-brand text-white border-brand'
                  : 'bg-surface-raised text-slate-300 border-surface-border hover:bg-surface-border',
              ].join(' ')}
            >
              {st}
            </button>
          );
        })}
        {hasFilterActive && (
          <button
            type="button"
            onClick={handleClearFilters}
            className="text-xs text-brand-light hover:underline ml-auto shrink-0"
          >
            Clear filters
          </button>
        )}
      </div>

      {/* Content state */}
      {isLoading ? (
        <div className="p-6 space-y-3">
          <Skeleton className="h-10 w-full rounded" />
          <Skeleton className="h-10 w-full rounded" />
          <Skeleton className="h-10 w-full rounded" />
        </div>
      ) : isError ? (
        <ErrorState
          title="Failed to load pipeline runs"
          description="Could not connect to the server. Please check your network or try again."
          onRetry={() => refetch()}
        />
      ) : runs.length === 0 ? (
        hasFilterActive ? (
          <EmptyState
            title="No matching runs"
            description="No pipeline runs matched your active filters."
            action={
              <Button variant="secondary" size="sm" onClick={handleClearFilters}>
                Clear filters
              </Button>
            }
          />
        ) : (
          <EmptyState
            title="No runs yet"
            description="Create or select a pipeline template above to get started."
            action={
              onFocusTemplates ? (
                <Button variant="primary" size="sm" onClick={onFocusTemplates}>
                  Browse Templates
                </Button>
              ) : undefined
            }
          />
        )
      ) : (
        <>
          <DataTable
            columns={columns}
            rows={runs}
            rowKey={(r) => r.id}
            rowTestId={() => 'runs-row'}
          />

          {/* Pagination controls */}
          <div
            data-testid="runs-pagination"
            className="flex flex-col sm:flex-row items-center justify-between gap-3 px-4 py-3 border-t border-surface-border text-sm text-slate-400 bg-surface-base/30"
          >
            <div className="flex items-center gap-2">
              <span>Rows per page:</span>
              <select
                value={pageSize}
                onChange={(e) => handlePageSizeChange(parseInt(e.target.value, 10))}
                className="bg-surface-raised border border-surface-border text-slate-200 text-xs rounded px-2 py-1 focus:outline-none focus:border-brand"
              >
                <option value={10}>10</option>
                <option value={25}>25</option>
                <option value={50}>50</option>
              </select>
              <span>
                Showing {Math.min(offset + 1, totalCount)} -{' '}
                {Math.min(offset + pageSize, totalCount)} of {totalCount}
              </span>
            </div>

            <div className="flex items-center gap-2">
              <Button
                variant="secondary"
                size="sm"
                disabled={page <= 1}
                onClick={() => handlePageChange(page - 1)}
              >
                Previous
              </Button>
              <span className="text-xs px-2">
                Page {page} of {totalPages}
              </span>
              <Button
                variant="secondary"
                size="sm"
                disabled={page >= totalPages}
                onClick={() => handlePageChange(page + 1)}
              >
                Next
              </Button>
            </div>
          </div>
        </>
      )}

      {/* Stop confirmation dialog */}
      <ConfirmDialog
        open={Boolean(stopRunTarget)}
        onClose={() => setStopRunTarget(null)}
        onConfirm={() => {
          if (stopRunTarget) {
            cancelMutation.mutate(stopRunTarget.id);
          }
        }}
        title="Stop running pipeline?"
        description={`This will send a SIGTERM cancellation request to pipeline run "${stopRunTarget?.name}".`}
        confirmLabel="Stop Run"
        confirmVariant="danger"
        loading={cancelMutation.isPending}
      />
    </Card>
  );
}

export default RunsTable;
