/**
 * frontend/src/components/pipelines/details/OutputTab.tsx
 * -------------------------------------------------------
 * UI-6: Production-quality Output inspection, schema display, bounded tabular preview,
 * and signed single-use CSV/Parquet downloads.
 */

import React, { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import {
  CopyIcon,
  CheckIcon,
  FolderArchiveIcon,
  DownloadIcon,
  RefreshCwIcon,
  FileTextIcon,
  DatabaseIcon,
  ChevronDownIcon,
  ChevronUpIcon,
  ShieldCheckIcon,
} from 'lucide-react';
import { RunProgress } from '../../../types/runProgress';
import { getOutput, getPreview, requestDownloadLink, toOutputError } from '../../../api/output';
import {
  columnAlignment,
  downloadSizeWarning,
  formatCell,
  isNumericType,
  previewToCsvText,
} from '../../../lib/output';
import { formatBytes, formatCount } from '../../../lib/format';
import { Button } from '../../ui/Button';
import { EmptyState } from '../../ui/EmptyState';
import { ErrorState } from '../../ui/ErrorState';
import { Skeleton } from '../../ui/Skeleton';
import { Tooltip } from '../../ui/Tooltip';
import { toast } from '../../ui/Toast';
import { ConfirmDialog } from '../../ui/ConfirmDialog';

interface OutputTabProps {
  runId?: string;
  status?: string;
  progress?: RunProgress | null;
  actualOutputPath?: string | null;
  fallbackOutputPath?: string | null;
}

const PREVIEW_LIMITS = [25, 50, 100, 500] as const;

export const OutputTab = React.memo(function OutputTab({
  runId,
  status: propStatus,
  progress,
  actualOutputPath,
  fallbackOutputPath,
}: OutputTabProps) {
  // Determine effective run status
  const effectiveStatus = propStatus || progress?.status || 'pending';
  const isSucceeded = effectiveStatus === 'succeeded';

  // Preview limit state (default 50)
  const [limit, setLimit] = useState<number>(50);
  const [pathCopied, setPathCopied] = useState(false);
  const [csvCopied, setCsvCopied] = useState(false);
  const [showFileList, setShowFileList] = useState(false);

  // Large download confirmation modal state
  const [pendingDownloadFormat, setPendingDownloadFormat] = useState<'csv' | 'parquet' | null>(null);
  const [isDownloading, setIsDownloading] = useState(false);

  // 1. Fetch Output Metadata
  const {
    data: metadata,
    isLoading: isLoadingMeta,
    isError: isMetaError,
    error: metaError,
    refetch: refetchMeta,
  } = useQuery({
    queryKey: ['outputMetadata', runId],
    queryFn: () => getOutput(runId!),
    enabled: Boolean(runId) && isSucceeded,
    staleTime: 30000,
  });

  // 2. Fetch Bounded Preview
  const {
    data: preview,
    isLoading: isLoadingPreview,
    isError: isPreviewError,
    error: previewError,
    refetch: refetchPreview,
    isFetching: isFetchingPreview,
  } = useQuery({
    queryKey: ['outputPreview', runId, limit],
    queryFn: () => getPreview(runId!, limit),
    enabled: Boolean(runId) && isSucceeded && Boolean(metadata && metadata.fileCount > 0),
    staleTime: 30000,
  });

  // Handle path copy
  const displayPath =
    metadata?.path || actualOutputPath || progress?.hdfs?.path || fallbackOutputPath || '—';

  const handleCopyPath = async () => {
    if (!displayPath || displayPath === '—') return;
    try {
      await navigator.clipboard.writeText(displayPath);
      setPathCopied(true);
      toast.success('Output path copied');
      setTimeout(() => setPathCopied(false), 2000);
    } catch {
      toast.error('Failed to copy output path');
    }
  };

  // Handle Copy as CSV
  const handleCopyCsv = async () => {
    if (!preview) return;
    try {
      const csv = previewToCsvText(preview);
      await navigator.clipboard.writeText(csv);
      setCsvCopied(true);
      toast.success('Copied CSV preview to clipboard');
      setTimeout(() => setCsvCopied(false), 2000);
    } catch {
      toast.error('Failed to copy CSV to clipboard');
    }
  };

  // Perform actual download trigger
  const executeDownload = async (format: 'csv' | 'parquet') => {
    if (!runId) return;
    setIsDownloading(true);
    try {
      const link = await requestDownloadLink(runId, format);
      // Trigger download via hidden anchor element
      const anchor = document.createElement('a');
      anchor.href = link.url;
      const baseName = metadata?.path ? metadata.path.split('/').filter(Boolean).pop() : 'output';
      anchor.download = `${baseName || 'pipeline-output'}.${format}`;
      anchor.style.display = 'none';
      document.body.appendChild(anchor);
      anchor.click();
      document.body.removeChild(anchor);
      toast.success(`Download started (${format.toUpperCase()})`);
    } catch (err) {
      toast.error(toOutputError(err));
    } finally {
      setIsDownloading(false);
      setPendingDownloadFormat(null);
    }
  };

  // Trigger download with large-file check (> 100 MB)
  const handleDownloadClick = (format: 'csv' | 'parquet') => {
    if (metadata && downloadSizeWarning(metadata.totalSizeBytes)) {
      setPendingDownloadFormat(format);
    } else {
      executeDownload(format);
    }
  };

  // ── Render Guard: Not Succeeded ───────────────────────────────────────────
  if (!isSucceeded) {
    return (
      <div data-testid="output-tab" className="space-y-4">
        <div
          data-testid="output-not-ready"
          className="p-8 bg-surface-card border border-surface-border rounded-xl flex items-center justify-center min-h-[260px]"
        >
          <EmptyState
            title={
              effectiveStatus === 'running' || effectiveStatus === 'pending'
                ? 'Pipeline is still running'
                : 'Output not available'
            }
            description={
              effectiveStatus === 'failed' || effectiveStatus === 'cancelled'
                ? 'Output is only available for successfully completed pipeline runs.'
                : 'Output preview and downloads will become available once the pipeline finishes successfully.'
            }
          />
        </div>
      </div>
    );
  }

  // ── Render Guard: Loading Metadata ────────────────────────────────────────
  if (isLoadingMeta) {
    return (
      <div data-testid="output-tab" className="space-y-4">
        <div data-testid="output-loading" className="space-y-4">
          <Skeleton className="h-36 w-full rounded-xl" />
          <Skeleton className="h-20 w-full rounded-xl" />
          <Skeleton className="h-64 w-full rounded-xl" />
        </div>
      </div>
    );
  }

  // ── Render Guard: Metadata Error ──────────────────────────────────────────
  if (isMetaError) {
    return (
      <div data-testid="output-tab" className="space-y-4">
        <div data-testid="output-error">
          <ErrorState
            title="Failed to load output"
            description={toOutputError(metaError)}
            onRetry={() => refetchMeta()}
          />
        </div>
      </div>
    );
  }

  // ── Render Guard: Empty Output ────────────────────────────────────────────
  if (!metadata || metadata.fileCount === 0) {
    return (
      <div data-testid="output-tab" className="space-y-4">
        <div
          data-testid="output-empty"
          className="p-8 bg-surface-card border border-surface-border rounded-xl flex items-center justify-center min-h-[260px]"
        >
          <EmptyState
            title="No output files produced"
            description="The pipeline completed, but no data files were written to the destination."
          />
        </div>
      </div>
    );
  }

  // ── Ready State ───────────────────────────────────────────────────────────
  return (
    <div data-testid="output-tab" className="space-y-6">
      {/* 1. Storage Destination & Output Metadata */}
      <div
        data-testid="output-metadata"
        className="p-5 bg-surface-card border border-surface-border rounded-xl space-y-4 shadow-sm"
      >
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2 text-sm font-semibold text-slate-200">
            <FolderArchiveIcon className="h-4 w-4 text-brand-light" />
            <span>Storage Destination & Output Metadata</span>
          </div>
          <div className="flex items-center gap-2">
            <span
              data-testid="badge-format"
              className="px-2.5 py-0.5 rounded-full text-xs font-semibold bg-brand/20 text-brand-light border border-brand/30 uppercase tracking-wide"
            >
              {metadata.format}
            </span>
            {metadata.verified && (
              <span
                data-testid="badge-verified"
                className="inline-flex items-center gap-1 px-2.5 py-0.5 rounded-full text-xs font-medium bg-emerald-500/10 text-emerald-400 border border-emerald-500/20"
              >
                <ShieldCheckIcon className="h-3 w-3" />
                Verified
              </span>
            )}
          </div>
        </div>

        {/* HDFS Path & Actions */}
        <div className="bg-surface-base p-3 rounded-lg border border-surface-border flex flex-col md:flex-row md:items-center justify-between gap-3">
          <div className="min-w-0 flex-1">
            <span className="text-xs font-medium text-slate-400 block mb-1">HDFS Output Path:</span>
            <code
              data-testid="meta-output-path"
              className="font-mono text-xs text-slate-200 break-all select-all"
              title={displayPath}
            >
              {displayPath}
            </code>
          </div>
          <div className="shrink-0">
            <Tooltip content="Copy HDFS output path">
              <Button
                data-testid="btn-copy-path"
                variant="secondary"
                size="sm"
                onClick={handleCopyPath}
                className="gap-1.5"
              >
                {pathCopied ? (
                  <>
                    <CheckIcon className="h-3.5 w-3.5 text-status-success" />
                    <span>Copied</span>
                  </>
                ) : (
                  <>
                    <CopyIcon className="h-3.5 w-3.5" />
                    <span>Copy Path</span>
                  </>
                )}
              </Button>
            </Tooltip>
          </div>
        </div>

        {/* Summary Metrics Grid */}
        <div className="grid grid-cols-1 sm:grid-cols-3 gap-3 text-xs">
          <div className="bg-surface-base p-3 rounded-lg border border-surface-border">
            <span className="text-slate-400 block mb-1">Part Files:</span>
            <span
              data-testid="meta-file-count"
              className="font-mono text-base font-bold text-slate-100 tabular-nums"
            >
              {formatCount(metadata.fileCount)} {metadata.fileCount === 1 ? 'file' : 'files'}
            </span>
          </div>

          <div className="bg-surface-base p-3 rounded-lg border border-surface-border">
            <span className="text-slate-400 block mb-1">Total Size:</span>
            <span
              data-testid="meta-total-size"
              className="font-mono text-base font-bold text-slate-100 tabular-nums"
            >
              {formatBytes(metadata.totalSizeBytes)}
            </span>
          </div>

          <div className="bg-surface-base p-3 rounded-lg border border-surface-border">
            <span className="text-slate-400 block mb-1">Total Records:</span>
            <span
              data-testid="meta-row-count"
              className="font-mono text-base font-bold text-slate-100 tabular-nums"
            >
              {metadata.rowCount !== null ? formatCount(metadata.rowCount) : '—'}
            </span>
          </div>
        </div>

        {/* Collapsible Part Files List */}
        {metadata.files && metadata.files.length > 0 && (
          <div>
            <button
              data-testid="btn-toggle-files"
              type="button"
              onClick={() => setShowFileList(!showFileList)}
              className="flex items-center gap-1.5 text-xs font-medium text-slate-400 hover:text-slate-200 transition-colors focus:outline-none focus:ring-1 focus:ring-brand"
            >
              {showFileList ? (
                <ChevronUpIcon className="h-3.5 w-3.5" />
              ) : (
                <ChevronDownIcon className="h-3.5 w-3.5" />
              )}
              <span>
                {showFileList ? 'Hide part files' : `View ${metadata.files.length} part files`}
              </span>
            </button>

            {showFileList && (
              <div
                data-testid="output-file-list"
                className="mt-3 bg-surface-base rounded-lg border border-surface-border p-3 max-h-48 overflow-y-auto space-y-1.5 text-xs font-mono"
              >
                {metadata.files.map((file) => (
                  <div
                    key={file.name}
                    className="flex items-center justify-between text-slate-300 py-0.5 border-b border-surface-border/50 last:border-0"
                  >
                    <span className="truncate pr-2" title={file.name}>
                      {file.name}
                    </span>
                    <span className="text-slate-400 shrink-0 tabular-nums">
                      {formatBytes(file.sizeBytes)}
                    </span>
                  </div>
                ))}
              </div>
            )}
          </div>
        )}
      </div>

      {/* 2. Schema Specification */}
      <div
        data-testid="output-schema"
        className="p-5 bg-surface-card border border-surface-border rounded-xl space-y-3 shadow-sm"
      >
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2 text-sm font-semibold text-slate-200">
            <DatabaseIcon className="h-4 w-4 text-brand-light" />
            <span>Output Schema ({metadata.columns.length} columns)</span>
          </div>
        </div>

        <div className="flex flex-wrap gap-2 pt-1">
          {metadata.columns.map((col) => (
            <div
              key={col.name}
              data-testid={`schema-col-${col.name}`}
              className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-surface-base border border-surface-border text-xs"
            >
              <span className="font-medium text-slate-200">{col.name}</span>
              <span
                data-testid={`col-badge-${col.type}`}
                className="font-mono text-[10px] uppercase font-semibold px-1.5 py-0.5 rounded bg-surface-border text-slate-400"
              >
                {col.type}
              </span>
            </div>
          ))}
        </div>
      </div>

      {/* 3. Bounded Preview Table */}
      <div
        data-testid="output-preview"
        className="p-5 bg-surface-card border border-surface-border rounded-xl space-y-4 shadow-sm"
      >
        {/* Preview Toolbar */}
        <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3">
          <div className="flex items-center gap-2">
            <FileTextIcon className="h-4 w-4 text-brand-light" />
            <h3 className="text-sm font-semibold text-slate-200">Data Preview</h3>
            {preview && (
              <span
                data-testid="preview-count"
                className="text-xs text-slate-400 ml-2"
              >
                Showing {preview.rows.length} of{' '}
                {preview.totalRows !== null ? formatCount(preview.totalRows) : preview.rows.length}{' '}
                rows
              </span>
            )}
          </div>

          <div className="flex items-center gap-2">
            {/* Rows Limit Selector */}
            <div className="flex items-center gap-1.5 text-xs text-slate-400">
              <label htmlFor="preview-limit-select">Rows:</label>
              <select
                id="preview-limit-select"
                aria-label="Rows per page"
                data-testid="select-preview-limit"
                value={limit}
                onChange={(e) => setLimit(Number(e.target.value))}
                disabled={isLoadingPreview || isFetchingPreview}
                className="bg-surface-base border border-surface-border text-slate-200 rounded px-2 py-1 text-xs focus:ring-1 focus:ring-brand focus:outline-none"
              >
                {PREVIEW_LIMITS.map((lim) => (
                  <option key={lim} value={lim}>
                    {lim}
                  </option>
                ))}
              </select>
            </div>

            {/* Refresh Button */}
            <Tooltip content="Refresh preview data">
              <Button
                data-testid="btn-refresh-preview"
                variant="secondary"
                size="sm"
                onClick={() => refetchPreview()}
                disabled={isLoadingPreview || isFetchingPreview}
                className="px-2"
                aria-label="Refresh preview"
              >
                <RefreshCwIcon
                  className={`h-3.5 w-3.5 ${isFetchingPreview ? 'animate-spin text-brand-light' : ''}`}
                />
              </Button>
            </Tooltip>

            {/* Copy CSV Button */}
            <Tooltip content="Copy preview table as CSV">
              <Button
                data-testid="btn-copy-csv"
                variant="secondary"
                size="sm"
                onClick={handleCopyCsv}
                disabled={!preview || preview.rows.length === 0}
                className="gap-1 text-xs"
              >
                {csvCopied ? (
                  <>
                    <CheckIcon className="h-3.5 w-3.5 text-status-success" />
                    <span>Copied CSV</span>
                  </>
                ) : (
                  <>
                    <CopyIcon className="h-3.5 w-3.5" />
                    <span>Copy CSV</span>
                  </>
                )}
              </Button>
            </Tooltip>
          </div>
        </div>

        {/* Truncated notice banner */}
        {preview?.truncated && (
          <div className="p-2.5 bg-amber-500/10 border border-amber-500/20 rounded-lg text-xs text-amber-300">
            Preview capped at 500 records. To inspect complete dataset, download the file below.
          </div>
        )}

        {/* Table Container */}
        {isLoadingPreview ? (
          <div className="space-y-2 py-4">
            <Skeleton className="h-8 w-full rounded" />
            <Skeleton className="h-8 w-full rounded" />
            <Skeleton className="h-8 w-full rounded" />
            <Skeleton className="h-8 w-full rounded" />
          </div>
        ) : isPreviewError ? (
          <div className="p-4 bg-surface-base rounded-lg border border-surface-border">
            <ErrorState
              title="Failed to load preview"
              description={toOutputError(previewError)}
              onRetry={() => refetchPreview()}
            />
          </div>
        ) : !preview || preview.rows.length === 0 ? (
          <div className="text-center py-8 text-xs text-slate-400">
            No rows found in output.
          </div>
        ) : (
          <div
            tabIndex={0}
            role="region"
            aria-label="Output preview"
            className="overflow-x-auto rounded-lg border border-surface-border focus:outline-none focus:ring-1 focus:ring-brand max-h-96"
          >
            <table className="w-full text-xs text-left border-collapse">
              <thead className="bg-surface-base/90 sticky top-0 border-b border-surface-border text-slate-400 font-semibold uppercase tracking-wider text-[11px]">
                <tr>
                  <th scope="col" className="px-3 py-2.5 text-center w-12 text-slate-500 border-r border-surface-border">
                    #
                  </th>
                  {preview.columns.map((col) => {
                    const align = columnAlignment(col.type);
                    return (
                      <th
                        key={col.name}
                        scope="col"
                        className={`px-3 py-2.5 whitespace-nowrap ${
                          align === 'right' ? 'text-right' : 'text-left'
                        }`}
                      >
                        {col.name}
                      </th>
                    );
                  })}
                </tr>
              </thead>
              <tbody className="divide-y divide-surface-border/50 bg-surface-card">
                {preview.rows.map((row, rowIdx) => (
                  <tr
                    key={rowIdx}
                    className="hover:bg-surface-base/50 transition-colors"
                  >
                    <td className="px-3 py-2 text-center text-slate-500 font-mono text-[10px] border-r border-surface-border select-none">
                      {rowIdx + 1}
                    </td>
                    {preview.columns.map((col) => {
                      const align = columnAlignment(col.type);
                      const isNum = isNumericType(col.type);
                      const formatted = formatCell(row[col.name], col.type);
                      return (
                        <td
                          key={col.name}
                          className={`px-3 py-2 whitespace-nowrap text-slate-200 ${
                            align === 'right' ? 'text-right' : 'text-left'
                          } ${isNum || col.type === 'timestamp' || col.type === 'date' ? 'font-mono' : ''}`}
                        >
                          {formatted}
                        </td>
                      );
                    })}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      {/* 4. Download Section */}
      <div
        data-testid="output-download"
        className="p-5 bg-surface-card border border-surface-border rounded-xl space-y-3 shadow-sm"
      >
        <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
          <div>
            <h4 className="text-sm font-semibold text-slate-200">Download Complete Output</h4>
            <p className="text-xs text-slate-400 mt-0.5">
              Generates a secure single-use stream link valid for 60 seconds.
            </p>
          </div>

          <div className="flex items-center gap-3">
            <Button
              data-testid="btn-download-csv"
              variant="secondary"
              size="sm"
              onClick={() => handleDownloadClick('csv')}
              disabled={isDownloading}
              loading={isDownloading && pendingDownloadFormat === 'csv'}
              className="gap-2 text-xs"
            >
              <DownloadIcon className="h-3.5 w-3.5" />
              <span>Download CSV</span>
            </Button>

            <Button
              data-testid="btn-download-parquet"
              variant="primary"
              size="sm"
              onClick={() => handleDownloadClick('parquet')}
              disabled={isDownloading}
              loading={isDownloading && pendingDownloadFormat === 'parquet'}
              className="gap-2 text-xs"
            >
              <DownloadIcon className="h-3.5 w-3.5" />
              <span>Download Parquet</span>
            </Button>
          </div>
        </div>
      </div>

      {/* Large Download Confirmation Dialog */}
      <ConfirmDialog
        open={Boolean(pendingDownloadFormat)}
        onClose={() => setPendingDownloadFormat(null)}
        onConfirm={() => {
          if (pendingDownloadFormat) {
            executeDownload(pendingDownloadFormat);
          }
        }}
        title="Large Download Confirmation"
        description={`This output is ${formatBytes(metadata.totalSizeBytes)} (over 100 MB). Merging and streaming large datasets may take extra time and network bandwidth. Do you wish to proceed?`}
        confirmLabel="Proceed to Download"
        confirmVariant="primary"
        loading={isDownloading}
      />
    </div>
  );
});
