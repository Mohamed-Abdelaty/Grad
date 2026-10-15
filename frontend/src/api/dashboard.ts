import { apiClient } from './client';

export interface DashboardRuns {
  active: number;
  total: number;
  succeeded: number;
  failed: number;
  cancelled: number;
}

export interface DashboardExperiments {
  running: number;
  total: number;
  passed: number;
  failed: number;
}

export interface DashboardKPIs {
  avgRecoverySeconds: number | null;
  totalDataLoss: number | null;
  avgThroughput: number | null;
  avgAvailabilityPercent: number | null;
}

export interface RunPerHourBucket {
  hour: string;
  total: number;
  failed: number;
}

export interface RecoveryTrendItem {
  experimentId: string;
  createdAt: string;
  recoverySeconds: number;
  result: string;
}

export interface ThroughputTrendItem {
  experimentId: string;
  createdAt: string;
  baseline: number | null;
  during: number | null;
}

export interface RecentFailureItem {
  type: 'run' | 'experiment';
  id: string;
  title: string;
  at: string;
  message: string;
}

export interface DashboardSummary {
  windowHours: number;
  bucketHours: number;
  generatedAt: string;
  runs: DashboardRuns;
  experiments: DashboardExperiments;
  kpis: DashboardKPIs;
  runsPerHour: RunPerHourBucket[];
  recoveryTrend: RecoveryTrendItem[];
  throughputTrend: ThroughputTrendItem[];
  recentFailures: RecentFailureItem[];
}

export async function getDashboardSummary(windowHours: number = 24): Promise<DashboardSummary> {
  const res = await apiClient.get<DashboardSummary>('/api/dashboard/summary', {
    params: { windowHours },
  });
  return res.data;
}
