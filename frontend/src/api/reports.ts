import { apiClient } from './client';
import { Report, ReportDetail, ReportFilterParams, ReportListItem } from '../types/report';
import { mapToExperimentDetail } from './experiments';

export function mapToReportListItem(raw: Record<string, unknown>): ReportListItem {
  if (!raw || typeof raw !== 'object') {
    throw new Error('Invalid report list item');
  }
  const id = String(raw.id || raw.report_id || '');
  return {
    id,
    report_id: id,
    experimentId: String(raw.experimentId || raw.experiment_id || ''),
    title: String(raw.title || ''),
    targetService: String(raw.targetService || raw.target_service || ''),
    faultType: String(raw.faultType || raw.fault_type || ''),
    result: raw.result === 'PASS' || raw.result === 'FAIL' ? raw.result : null,
    createdAt: String(raw.createdAt || raw.created_at || ''),
    created_at: String(raw.createdAt || raw.created_at || ''),
    recoveryTimeSeconds: (raw.recoveryTimeSeconds ?? raw.recovery_time_seconds ?? null) as number | null,
    dataLoss: Number(raw.dataLoss ?? raw.data_loss ?? 0),
    throughputBaseline: (raw.throughputBaseline ?? raw.throughput_baseline ?? null) as number | null,
    throughputDuringFault: (raw.throughputDuringFault ?? raw.throughput_during_fault ?? null) as number | null,
    availabilityPercentage: (raw.availabilityPercentage ?? raw.availability_percentage ?? null) as number | null,
    errorRate: (raw.errorRate ?? raw.error_rate ?? null) as number | null,
    cpuAvgPercent: (raw.cpuAvgPercent ?? raw.cpu_avg_percent ?? null) as number | null,
    memoryAvgBytes: (raw.memoryAvgBytes ?? raw.memory_avg_bytes ?? null) as number | null,
    summary: ((raw.summary as Record<string, unknown>) || raw) as Record<string, unknown>,
  };
}

export function mapToReportDetail(raw: Record<string, unknown>): ReportDetail {
  const expDetail = mapToExperimentDetail(raw);
  return {
    ...expDetail,
    reportId: String(raw.reportId || raw.report_id || raw.id || ''),
    aiAnalysis: typeof raw.aiAnalysis === 'object' && raw.aiAnalysis !== null ? (raw.aiAnalysis as ReportDetail['aiAnalysis']) : null,
  };
}

// Converts the raw API report list item to the canonical Report type (backwards compatibility).
export function mapToReport(item: ReportListItem | Record<string, unknown>): Report {
  const itemDict = item as Record<string, unknown>;
  const s = ((itemDict.summary ?? itemDict) as Record<string, unknown>) || {};
  return {
    id: String(itemDict.report_id || itemDict.id || ''),
    title: String(itemDict.title || ''),
    createdAt: String(itemDict.created_at || itemDict.createdAt || ''),
    experimentName: (s.experiment_name as string | undefined) ?? (itemDict.title as string | undefined) ?? '',
    target: (s.target_service as string | undefined) ?? (itemDict.targetService as string | undefined) ?? null,
    failureType: (s.fault_type as string | undefined) ?? (itemDict.faultType as string | undefined) ?? null,
    recoveryTimeSeconds: (s.recovery_time_seconds as number | undefined) ?? (itemDict.recoveryTimeSeconds as number | undefined) ?? null,
    availabilityPercentage: (s.availability_percentage as number | undefined) ?? (itemDict.availabilityPercentage as number | undefined) ?? null,
    throughputRecordsPerSec: (s.throughput_records_per_sec as number | undefined) ?? (itemDict.throughputAfter as number | undefined) ?? null,
    dataLossPercentage: (s.data_loss_percentage as number | undefined) ?? null,
    summary: s,
  };
}

export async function listReports(
  params?: ReportFilterParams
): Promise<{ items: ReportListItem[]; total: number }> {
  const query: Record<string, string | number> = {};
  if (params?.target) query.target = params.target;
  if (params?.fault) query.fault = params.fault;
  if (params?.result) query.result = params.result;
  if (params?.from) query.from = params.from;
  if (params?.to) query.to = params.to;
  if (params?.sort) query.sort = params.sort;
  if (typeof params?.limit === 'number') query.limit = params.limit;
  if (typeof params?.offset === 'number') query.offset = params.offset;

  const res = await apiClient.get<Record<string, unknown>[]>('/api/reports', { params: query });
  const totalHeader = res.headers['x-total-count'];
  const total = totalHeader !== undefined ? parseInt(String(totalHeader), 10) : res.data.length;

  return {
    items: res.data.map(mapToReportListItem),
    total: isNaN(total) ? res.data.length : total,
  };
}

export async function getReport(reportId: string): Promise<ReportDetail> {
  const res = await apiClient.get<Record<string, unknown>>(`/api/reports/${reportId}`);
  return mapToReportDetail(res.data);
}

// Legacy compatibility
export async function getReports(): Promise<Report[]> {
  const { items } = await listReports();
  return items.map(mapToReport);
}
