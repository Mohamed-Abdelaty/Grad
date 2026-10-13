import React from 'react';
import { Link, useParams } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';
import { ArrowLeftIcon } from 'lucide-react';
import { getPipelineRun } from '../api/pipelines';
import { StatusPill } from '../components/ui/StatusPill';
import { Card, CardHeader, CardTitle } from '../components/ui/Card';
import { ErrorState } from '../components/ui/ErrorState';
import { Skeleton } from '../components/ui/Skeleton';

export default function PipelineDetails() {
  const { runId } = useParams<{ runId: string }>();

  const { data: run, isLoading, isError, refetch } = useQuery({
    queryKey: ['pipelineRun', runId],
    queryFn: () => getPipelineRun(runId!),
    enabled: Boolean(runId),
    refetchInterval: (query) => {
      const status = query.state.data?.status;
      return status === 'running' ? 2000 : false;
    },
  });

  if (isLoading) {
    return (
      <div className="max-w-4xl space-y-6">
        <Skeleton className="h-8 w-48 rounded" />
        <Skeleton className="h-64 w-full rounded" />
      </div>
    );
  }

  if (isError || !run) {
    return (
      <div className="max-w-4xl space-y-6">
        <Link
          to="/pipelines"
          className="inline-flex items-center gap-1.5 text-sm text-brand-light hover:underline"
        >
          <ArrowLeftIcon className="h-4 w-4" />
          Back to Pipelines
        </Link>
        <ErrorState
          title="Run not found"
          description="The requested pipeline run could not be found or failed to load."
          onRetry={() => refetch()}
        />
      </div>
    );
  }

  const outputPath = run.actualOutputPath || run.outputPath;

  return (
    <div className="max-w-4xl space-y-6">
      <div className="flex items-center justify-between">
        <Link
          to="/pipelines"
          className="inline-flex items-center gap-1.5 text-sm text-brand-light hover:underline"
        >
          <ArrowLeftIcon className="h-4 w-4" />
          Back to Pipelines
        </Link>
        <StatusPill status={run.status} label={run.status} dot={run.status === 'running'} />
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Pipeline Run: {run.name}</CardTitle>
        </CardHeader>

        <div className="grid grid-cols-1 md:grid-cols-2 gap-4 text-sm">
          <div>
            <span className="text-slate-400 block">Run ID:</span>
            <span className="font-mono text-slate-200 text-xs break-all">{run.id}</span>
          </div>

          <div>
            <span className="text-slate-400 block">Status:</span>
            <span className="capitalize font-medium text-slate-200">{run.status}</span>
          </div>

          <div>
            <span className="text-slate-400 block">Started at:</span>
            <span className="text-slate-200">
              {run.startedAt ? new Date(run.startedAt).toLocaleString() : '-'}
            </span>
          </div>

          <div>
            <span className="text-slate-400 block">Finished at:</span>
            <span className="text-slate-200">
              {run.finishedAt ? new Date(run.finishedAt).toLocaleString() : '-'}
            </span>
          </div>

          <div className="md:col-span-2">
            <span className="text-slate-400 block">Output Path:</span>
            <span className="font-mono text-slate-200 text-xs break-all">
              {outputPath || '-'}
            </span>
          </div>
        </div>

        {run.errorMessage && (
          <div className="mt-6 p-4 rounded-lg bg-status-error/10 border border-status-error/30">
            <h4 className="text-xs font-semibold text-rose-300 uppercase tracking-wider mb-1">
              Error Details
            </h4>
            <pre className="text-xs text-rose-200 font-mono whitespace-pre-wrap break-words">
              {run.errorMessage}
            </pre>
          </div>
        )}
      </Card>
    </div>
  );
}
