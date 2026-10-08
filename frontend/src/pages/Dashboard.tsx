import React from 'react';
import { useQuery } from '@tanstack/react-query';
import { Link } from 'react-router-dom';
import { getSystemStatus, SystemStatus } from '../api/system';
import { getPipelineRuns } from '../api/pipelines';
import { listExperiments } from '../api/experiments';
import ServiceStatusBadge from '../components/pipelines/ServiceStatusBadge';
import {
  Card,
  CardHeader,
  CardTitle,
  EmptyState,
  ErrorState,
  SkeletonCard,
  StatCard,
  StatusPill,
} from '../components/ui';

type ServiceKey = keyof Omit<SystemStatus, 'status'>;

const SERVICES: Array<[ServiceKey, string]> = [
  ['database', 'Database'],
  ['kafka', 'Kafka'],
  ['spark', 'Spark'],
  ['hdfs', 'HDFS'],
];

export default function Dashboard() {
  const status = useQuery({
    queryKey: ['systemStatus'],
    queryFn: getSystemStatus,
    refetchInterval: 5000,
  });
  const runs = useQuery({
    queryKey: ['pipelineRuns'],
    queryFn: getPipelineRuns,
    refetchInterval: 5000,
  });
  const experiments = useQuery({
    queryKey: ['experiments'],
    queryFn: listExperiments,
    refetchInterval: 5000,
  });

  const runList = runs.data ?? [];
  const experimentList = experiments.data ?? [];
  const runningRuns = runList.filter((r) => r.status === 'running').length;
  const failedRuns = runList.filter((r) => r.status === 'failed').length;
  const runningExperiments = experimentList.filter((e) => e.status === 'running').length;

  return (
    <div className="space-y-6">
      <section aria-labelledby="system-status-heading">
        <h2 id="system-status-heading" className="text-lg font-semibold mb-3">
          System Status
        </h2>
        {status.isLoading && <SkeletonCard />}
        {status.isError && (
          <ErrorState
            title="Could not load system status"
            onRetry={() => void status.refetch()}
          />
        )}
        {status.data && (
          <div className="flex flex-wrap gap-4">
            {SERVICES.map(([key, label]) => (
              <ServiceStatusBadge
                key={key}
                serviceName={label}
                connected={status.data[key].connected}
              />
            ))}
          </div>
        )}
      </section>

      <section aria-label="Key figures" className="grid grid-cols-1 sm:grid-cols-3 gap-4">
        {runs.isLoading || experiments.isLoading ? (
          <>
            <SkeletonCard />
            <SkeletonCard />
            <SkeletonCard />
          </>
        ) : (
          <>
            <StatCard label="Pipeline runs" value={runs.isError ? null : runList.length} sub="Total tracked" />
            <StatCard
              label="Active pipelines"
              value={runs.isError ? null : runningRuns}
              sub={runs.isError ? 'Unavailable' : `${failedRuns} failed`}
              accent={failedRuns > 0 ? 'warning' : 'success'}
            />
            <StatCard
              label="Running experiments"
              value={experiments.isError ? null : runningExperiments}
              sub={experiments.isError ? 'Unavailable' : `${experimentList.length} total`}
            />
          </>
        )}
      </section>

      <Card as="section">
        <CardHeader>
          <CardTitle>Recent Experiments</CardTitle>
          <Link to="/reliability" className="text-sm text-brand-light hover:underline">
            View all
          </Link>
        </CardHeader>
        {experiments.isLoading && <SkeletonCard />}
        {experiments.isError && (
          <ErrorState
            title="Could not load experiments"
            onRetry={() => void experiments.refetch()}
          />
        )}
        {experiments.data && experimentList.length === 0 && (
          <EmptyState
            title="No experiments yet"
            description="Run a fault-injection experiment from Reliability Testing."
          />
        )}
        {experimentList.length > 0 && (
          <ul className="space-y-2">
            {experimentList.slice(0, 5).map((exp) => (
              <li
                key={exp.id}
                className="flex justify-between items-center text-sm border-b border-surface-border pb-2 last:border-0"
              >
                <div className="min-w-0">
                  <span className="font-mono">{exp.name}</span>
                  {exp.target && (
                    <span className="ml-2 text-xs text-slate-500">→ {exp.target}</span>
                  )}
                </div>
                <StatusPill status={exp.status ?? 'pending'} />
              </li>
            ))}
          </ul>
        )}
      </Card>
    </div>
  );
}
