import { ExperimentDetail } from './experiment';

export interface ReportListItem {
  id: string;
  experimentId: string;
  title: string;
  targetService: string;
  faultType: string;
  result: 'PASS' | 'FAIL' | null;
  createdAt: string;
  recoveryTimeSeconds: number | null;
  dataLoss: number;
  throughputBaseline: number | null;
  throughputDuringFault: number | null;
  availabilityPercentage: number | null;
  errorRate: number | null;
  cpuAvgPercent: number | null;
  memoryAvgBytes: number | null;

  // Compatibility fields
  report_id?: string;
  created_at?: string;
  summary?: Record<string, unknown>;
}

export interface ReportDetail extends ExperimentDetail {
  reportId: string;
  aiAnalysis: Record<string, unknown> | null;
}

export interface ReportFilterParams {
  target?: string;
  fault?: string;
  result?: 'PASS' | 'FAIL';
  from?: string;
  to?: string;
  sort?: 'created_desc' | 'created_asc' | 'recovery_asc' | 'recovery_desc';
  limit?: number;
  offset?: number;
}

// Canonical frontend Report type for compatibility with existing components
export interface Report {
  id: string;
  title: string;
  createdAt: string;
  experimentName: string;
  target: string | null;
  failureType: string | null;
  recoveryTimeSeconds: number | null;
  availabilityPercentage: number | null;
  throughputRecordsPerSec: number | null;
  dataLossPercentage: number | null;
  summary: Record<string, unknown>;
}
