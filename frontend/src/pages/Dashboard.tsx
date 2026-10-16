import React, { Suspense, useMemo } from 'react';
import { useQuery } from '@tanstack/react-query';
import { Link } from 'react-router-dom';
import {
  ActivityIcon,
  ArrowRightIcon,
  CheckCircle2Icon,
  ExternalLinkIcon,
  PlayIcon,
  PlusIcon,
  ShieldAlertIcon,
  XCircleIcon,
} from 'lucide-react';

import { getSystemStatus, SystemStatus } from '../api/system';
import { listExperiments } from '../api/experiments';
import { getDashboardSummary } from '../api/dashboard';
import { RunsTable } from '../components/pipelines/RunsTable';
import {
  Card,
  CardHeader,
  CardTitle,
  EmptyState,
  ErrorBoundary,
  ErrorState,
  Skeleton,
  StatusPill,
} from '../components/ui';

// Lazy-loaded trend charts for bundle code-splitting
const DashboardTrends = React.lazy(() => import('../components/dashboard/DashboardTrends'));

type ServiceKey = keyof Omit<SystemStatus, 'status'>;

const SERVICES: Array<[ServiceKey, string]> = [
  ['database', 'Database'],
  ['kafka', 'Kafka'],
  ['spark', 'Spark'],
  ['hdfs', 'HDFS'],
];

// Lightweight SVG Sparkline (zero axes, no tooltip)
function Sparkline({
  data,
  color = '#6366f1',
}: {
  data: number[];
  color?: string;
}) {
  if (!data || data.length < 2) return null;
  const min = Math.min(...data);
  const max = Math.max(...data);
  const range = max - min || 1;
  const width = 64;
  const height = 20;

  const points = data
    .map((val, idx) => {
      const x = (idx / (data.length - 1)) * width;
      const y = height - ((val - min) / range) * (height - 4) - 2;
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    })
    .join(' ');

  return (
    <svg width={width} height={height} className="overflow-visible shrink-0">
      <polyline fill="none" stroke={color} strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" points={points} />
    </svg>
  );
}

// ── 1. System Health Section ──────────────────────────────────────────────────
function HealthCardsSection() {
  const { data, isLoading, isError, refetch } = useQuery({
    queryKey: ['systemStatus'],
    queryFn: getSystemStatus,
    refetchInterval: () => {
      if (typeof document !== 'undefined' && document.visibilityState !== 'visible') {
        return false;
      }
      return 10000;
    },
  });

  return (
    <Card data-testid="health-cards" className="border-surface-border">
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <ActivityIcon className="h-5 w-5 text-brand-light" />
          System Health
        </CardTitle>
      </CardHeader>

      {isLoading ? (
        <div role="status" aria-busy="true" aria-label="Loading…" className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
          <Skeleton className="h-20 w-full rounded-lg" />
          <Skeleton className="h-20 w-full rounded-lg" />
          <Skeleton className="h-20 w-full rounded-lg" />
          <Skeleton className="h-20 w-full rounded-lg" />
        </div>
      ) : isError ? (
        <ErrorState
          title="Could not load system status"
          description="Failed to reach the backend monitoring endpoint."
          onRetry={() => void refetch()}
        />
      ) : data ? (
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
          {SERVICES.map(([key, label]) => {
            const svc = data[key];
            const isConnected = svc?.connected ?? false;
            return (
              <div
                key={key}
                data-testid={`health-${key}`}
                className={`p-4 rounded-lg border flex flex-col justify-between ${
                  isConnected
                    ? 'bg-surface-base/50 border-surface-border'
                    : 'bg-status-error/10 border-status-error/40'
                }`}
              >
                <div className="flex items-center justify-between">
                  <span className="font-semibold text-slate-200">{label}</span>
                  {isConnected ? (
                    <span className="inline-flex items-center gap-1 text-xs text-status-success font-medium">
                      <CheckCircle2Icon className="h-4 w-4" /> Connected
                    </span>
                  ) : (
                    <span className="inline-flex items-center gap-1 text-xs text-status-error font-medium">
                      <XCircleIcon className="h-4 w-4" /> Disconnected
                    </span>
                  )}
                </div>

                {!isConnected && (
                  <div className="mt-2 text-xs text-status-error">
                    <p>{label} is unreachable</p>
                    <a
                      href="#troubleshooting"
                      className="underline text-brand-light hover:text-white mt-1 inline-block"
                      target="_blank"
                      rel="noreferrer"
                    >
                      Troubleshooting guide &rarr;
                    </a>
                  </div>
                )}
              </div>
            );
          })}
        </div>
      ) : null}
    </Card>
  );
}

// ── 2. KPI Tile Component ──────────────────────────────────────────────────────
interface KPITileProps {
  testId: string;
  label: string;
  value: string | number | null;
  caption: string;
  sparklineData?: number[];
  sparklineColor?: string;
  accent?: 'neutral' | 'success' | 'warning' | 'error';
}

function KPITile({
  testId,
  label,
  value,
  caption,
  sparklineData,
  sparklineColor,
  accent = 'neutral',
}: KPITileProps) {
  const accentClass = {
    neutral: 'text-slate-100',
    success: 'text-status-success',
    warning: 'text-status-warning',
    error: 'text-status-error',
  }[accent];

  return (
    <div
      data-testid={testId}
      className="bg-surface-raised border border-surface-border rounded-lg p-4 flex flex-col justify-between"
    >
      <div className="flex items-center justify-between gap-2">
        <span className="text-xs font-medium text-slate-400 uppercase tracking-wider truncate">
          {label}
        </span>
        {sparklineData && sparklineData.length > 1 && (
          <Sparkline data={sparklineData} color={sparklineColor} />
        )}
      </div>

      <div className="mt-2">
        <p className={`text-2xl lg:text-3xl font-bold font-mono tabular-nums ${accentClass}`}>
          {value !== null && value !== undefined ? value : '—'}
        </p>
        <p className="text-xs text-slate-400 mt-1 truncate">{caption}</p>
      </div>
    </div>
  );
}

// ── 2. KPI Row Section ────────────────────────────────────────────────────────
function KPIRowSection() {
  const { data, isLoading, isError, refetch } = useQuery({
    queryKey: ['dashboardSummary', 24],
    queryFn: () => getDashboardSummary(24),
    refetchInterval: 5000,
  });

  const runsPerHourSpark = useMemo(() => {
    return data?.runsPerHour?.map((b) => b.total) || [];
  }, [data?.runsPerHour]);

  const recoveryTrendSpark = useMemo(() => {
    return data?.recoveryTrend?.map((t) => t.recoverySeconds) || [];
  }, [data?.recoveryTrend]);

  return (
    <section aria-label="Key performance indicators" data-testid="kpis">
      {isLoading ? (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          {Array.from({ length: 8 }).map((_, i) => (
            <Skeleton key={i} className="h-28 w-full rounded-lg" />
          ))}
        </div>
      ) : isError ? (
        <ErrorState
          title="Could not load KPIs"
          description="Failed to load platform performance metrics."
          onRetry={() => void refetch()}
        />
      ) : data ? (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <KPITile
            testId="kpi-active-runs"
            label="Active runs"
            value={data.runs.active}
            caption="Currently running"
            accent={data.runs.active > 0 ? 'success' : 'neutral'}
          />
          <KPITile
            testId="kpi-running-experiments"
            label="Running experiments"
            value={data.experiments.running}
            caption="Currently running"
            accent={data.experiments.running > 0 ? 'warning' : 'neutral'}
          />
          <KPITile
            testId="kpi-runs-24h"
            label="Runs (24 h)"
            value={data.runs.total}
            caption="Last 24 hours"
            sparklineData={runsPerHourSpark}
            sparklineColor="#38bdf8"
          />
          <KPITile
            testId="kpi-failures-24h"
            label="Failures (24 h)"
            value={data.runs.failed}
            caption="Last 24 hours"
            accent={data.runs.failed > 0 ? 'error' : 'neutral'}
          />
          <KPITile
            testId="kpi-avg-recovery-time"
            label="Avg recovery time"
            value={data.kpis.avgRecoverySeconds !== null ? `${data.kpis.avgRecoverySeconds}s` : null}
            caption="last 10 experiments"
            sparklineData={recoveryTrendSpark}
            sparklineColor="#6366f1"
          />
          <KPITile
            testId="kpi-total-data-loss"
            label="Total data loss"
            value={data.kpis.totalDataLoss}
            caption="last 10 experiments"
            accent={data.kpis.totalDataLoss && data.kpis.totalDataLoss > 0 ? 'error' : 'neutral'}
          />
          <KPITile
            testId="kpi-avg-throughput"
            label="Avg throughput"
            value={data.kpis.avgThroughput !== null ? `${data.kpis.avgThroughput} r/s` : null}
            caption="last 10 experiments"
          />
          <KPITile
            testId="kpi-avg-availability"
            label="Avg availability"
            value={data.kpis.avgAvailabilityPercent !== null ? `${data.kpis.avgAvailabilityPercent}%` : null}
            caption="last 10 experiments"
            accent={
              data.kpis.avgAvailabilityPercent !== null && data.kpis.avgAvailabilityPercent >= 99
                ? 'success'
                : 'neutral'
            }
          />
        </div>
      ) : null}
    </section>
  );
}

// ── 4. Recent Experiments Section ─────────────────────────────────────────────
function RecentExperimentsSection() {
  const { data, isLoading, isError, refetch } = useQuery({
    queryKey: ['experiments'],
    queryFn: listExperiments,
    refetchInterval: () => {
      if (typeof document !== 'undefined' && document.visibilityState !== 'visible') {
        return false;
      }
      return 10000;
    },
  });

  const experiments = (data || []).slice(0, 6);

  return (
    <Card data-testid="dash-experiments" className="border-surface-border">
      <CardHeader>
        <CardTitle>Recent experiments</CardTitle>
        <Link to="/reports" className="text-sm text-brand-light hover:underline inline-flex items-center gap-1">
          View all <ExternalLinkIcon className="h-3 w-3" />
        </Link>
      </CardHeader>

      {isLoading ? (
        <div className="space-y-3 p-4">
          <Skeleton className="h-10 w-full rounded" />
          <Skeleton className="h-10 w-full rounded" />
          <Skeleton className="h-10 w-full rounded" />
        </div>
      ) : isError ? (
        <ErrorState
          title="Could not load experiments"
          description="Failed to load recent chaos experiments."
          onRetry={() => void refetch()}
        />
      ) : experiments.length === 0 ? (
        <EmptyState
          title="No experiments yet"
          description="Run a fault-injection experiment from Reliability Testing."
          action={
            <Link
              to="/reliability"
              className="inline-flex items-center justify-center font-medium rounded transition-colors bg-brand-primary text-white hover:bg-brand-primary/90 text-sm px-3 py-1.5 min-h-[40px]"
            >
              Run an experiment
            </Link>
          }
        />
      ) : (
        <ul className="divide-y divide-surface-border">
          {experiments.map((exp) => (
            <li key={exp.id} className="p-3 flex items-center justify-between gap-3 text-sm hover:bg-surface-base/40">
              <div className="min-w-0">
                <p className="font-mono text-xs font-semibold text-slate-200 truncate">{exp.name}</p>
                <p className="text-xs text-slate-400 mt-0.5">
                  Target: <span className="text-slate-300 font-mono">{exp.target || '—'}</span> &bull; Fault:{' '}
                  <span className="text-slate-300 font-mono">{exp.faultType || '—'}</span>
                </p>
              </div>
              <div className="flex items-center gap-2 shrink-0">
                <StatusPill status={exp.status ?? 'pending'} />
              </div>
            </li>
          ))}
        </ul>
      )}
    </Card>
  );
}

// ── 5. Recent Failures Section ────────────────────────────────────────────────
function RecentFailuresSection() {
  const { data, isLoading, isError, refetch } = useQuery({
    queryKey: ['dashboardSummary', 24],
    queryFn: () => getDashboardSummary(24),
    refetchInterval: () => {
      if (typeof document !== 'undefined' && document.visibilityState !== 'visible') {
        return false;
      }
      return 10000;
    },
  });

  const failures = data?.recentFailures || [];

  return (
    <Card data-testid="dash-failures" className="border-surface-border">
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <ShieldAlertIcon className="h-5 w-5 text-status-error" />
          Recent failures (7 days)
        </CardTitle>
      </CardHeader>

      {isLoading ? (
        <div className="space-y-3 p-4">
          <Skeleton className="h-10 w-full rounded" />
          <Skeleton className="h-10 w-full rounded" />
        </div>
      ) : isError ? (
        <ErrorState
          title="Could not load failures"
          description="Failed to load recent failure records."
          onRetry={() => void refetch()}
        />
      ) : failures.length === 0 ? (
        <div className="p-8 text-center text-slate-400 text-sm">
          <CheckCircle2Icon className="h-8 w-8 text-status-success mx-auto mb-2 opacity-80" />
          <p className="font-medium text-slate-300">No failures in the last 7 days</p>
          <p className="text-xs text-slate-400 mt-1">All pipelines and experiments executed successfully.</p>
        </div>
      ) : (
        <ul className="divide-y divide-surface-border">
          {failures.map((f, idx) => {
            const linkHref = f.type === 'run' ? `/pipelines/${f.id}` : `/reports/${f.id}`;
            return (
              <li key={`${f.type}-${f.id}-${idx}`} className="p-3 hover:bg-surface-base/40">
                <Link to={linkHref} className="flex items-start justify-between gap-3 group">
                  <div className="min-w-0">
                    <div className="flex items-center gap-2">
                      <span className="px-1.5 py-0.5 rounded text-[10px] uppercase font-bold tracking-wider bg-status-error/15 text-status-error border border-status-error/30">
                        {f.type}
                      </span>
                      <span className="font-medium text-xs text-slate-200 group-hover:text-brand-light truncate">
                        {f.title}
                      </span>
                    </div>
                    <p className="text-xs text-status-error/90 mt-1 line-clamp-1">{f.message}</p>
                    <p className="text-[10px] text-slate-400 mt-0.5">
                      {f.at ? new Date(f.at).toLocaleString() : '—'}
                    </p>
                  </div>
                  <ArrowRightIcon className="h-4 w-4 text-slate-400 group-hover:text-slate-300 shrink-0 mt-1" />
                </Link>
              </li>
            );
          })}
        </ul>
      )}
    </Card>
  );
}

// ── 6. Trends Section ─────────────────────────────────────────────────────────
function TrendsSection() {
  const { data, isLoading, isError, refetch } = useQuery({
    queryKey: ['dashboardSummary', 24],
    queryFn: () => getDashboardSummary(24),
    refetchInterval: () => {
      if (typeof document !== 'undefined' && document.visibilityState !== 'visible') {
        return false;
      }
      return 10000;
    },
  });

  if (isLoading) {
    return (
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <Skeleton className="h-72 w-full rounded-lg" />
        <Skeleton className="h-72 w-full rounded-lg" />
      </div>
    );
  }

  if (isError) {
    return (
      <ErrorState
        title="Could not load trends"
        description="Failed to load recovery and throughput trends."
        onRetry={() => void refetch()}
      />
    );
  }

  return (
    <Suspense
      fallback={
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <Skeleton className="h-72 w-full rounded-lg" />
          <Skeleton className="h-72 w-full rounded-lg" />
        </div>
      }
    >
      <DashboardTrends
        recoveryTrend={data?.recoveryTrend || []}
        throughputTrend={data?.throughputTrend || []}
      />
    </Suspense>
  );
}

// ── Main Dashboard Page ───────────────────────────────────────────────────────
export default function Dashboard() {
  return (
    <div className="space-y-6">
      {/* Page Header with Single H1 and Quick Actions */}
      <header className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
        <div>
          <h1 className="text-2xl font-bold text-slate-100">Dashboard</h1>
          <p className="text-sm text-slate-400 mt-1">
            Real-time pipeline orchestration, system health, and ChaosLab resilience overview.
          </p>
        </div>

        {/* 7. Quick Actions */}
        <div className="flex items-center gap-3 shrink-0">
          <Link
            to="/pipelines"
            className="inline-flex items-center justify-center font-medium rounded transition-colors bg-surface-border text-slate-200 hover:bg-slate-700 text-sm px-3 py-1.5 min-h-[40px]"
          >
            <PlusIcon className="h-4 w-4 mr-1.5" />
            New pipeline
          </Link>
          <Link
            to="/reliability"
            className="inline-flex items-center justify-center font-medium rounded transition-colors bg-brand-primary text-white hover:bg-brand-primary/90 text-sm px-3 py-1.5 min-h-[40px]"
          >
            <PlayIcon className="h-4 w-4 mr-1.5" />
            Run an experiment
          </Link>
        </div>
      </header>

      {/* 1. System Health */}
      <ErrorBoundary fallback={<ErrorState title="System health section encountered an error" />}>
        <HealthCardsSection />
      </ErrorBoundary>

      {/* 2. Key Performance Indicators Row */}
      <ErrorBoundary fallback={<ErrorState title="KPI section encountered an error" />}>
        <KPIRowSection />
      </ErrorBoundary>

      {/* 3. Recent Pipeline Runs (Compact RunsTable variant) */}
      <ErrorBoundary fallback={<ErrorState title="Recent runs section encountered an error" />}>
        <RunsTable compact />
      </ErrorBoundary>

      {/* 4 & 5. Recent Experiments & Recent Failures */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <ErrorBoundary fallback={<ErrorState title="Recent experiments section encountered an error" />}>
          <RecentExperimentsSection />
        </ErrorBoundary>

        <ErrorBoundary fallback={<ErrorState title="Recent failures section encountered an error" />}>
          <RecentFailuresSection />
        </ErrorBoundary>
      </div>

      {/* 6. Reliability & Performance Trends */}
      <ErrorBoundary fallback={<ErrorState title="Trends section encountered an error" />}>
        <TrendsSection />
      </ErrorBoundary>
    </div>
  );
}
