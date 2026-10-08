import { apiClient } from './client';
import {
  AllowedService,
  Experiment,
  ExperimentDetail,
  ExperimentListItem,
} from '../types/experiment';
import { Report, ReportListItem } from '../types/report';

export interface ReliabilityAnalysis {
  method: string;
  observation_count: number;
  risk_score: number;
  risk_level: 'low' | 'medium' | 'high';
  anomalies: Array<Record<string, unknown>>;
  likely_contributors: string[];
  recommendations: string[];
  limitations: string;
}

// Converts the raw API list item to the canonical Experiment type.
export function mapToExperiment(item: ExperimentListItem): Experiment {
  return {
    id: item.experiment_id,
    name: item.name,
    target: item.target_service,
    faultType: item.fault_type,
    status: item.latest_run_status,
    createdAt: item.created_at,
    latestRunId: item.latest_run_id,
  };
}

// Converts the raw API report list item to the canonical Report type.
export function mapToReport(item: ReportListItem): Report {
  const s = item.summary ?? {};
  return {
    id: item.report_id,
    title: item.title,
    createdAt: item.created_at,
    experimentName: (s.experiment_name as string | undefined) ?? item.title,
    target: (s.target_service as string | undefined) ?? null,
    failureType: (s.fault_type as string | undefined) ?? null,
    recoveryTimeSeconds: (s.recovery_time_seconds as number | undefined) ?? null,
    availabilityPercentage: (s.availability_percentage as number | undefined) ?? null,
    throughputRecordsPerSec: (s.throughput_records_per_sec as number | undefined) ?? null,
    dataLossPercentage: (s.data_loss_percentage as number | undefined) ?? null,
    summary: s,
  };
}

export async function getAllowedTargets(): Promise<AllowedService[]> {
  const res = await apiClient.get<{ allowed_services: AllowedService[] }>(
    '/api/experiments/allowed-targets'
  );
  return res.data.allowed_services;
}

export async function createExperiment(
  yamlContent: string,
  pipelineYaml = ''
): Promise<{ experiment_id: string }> {
  const res = await apiClient.post<{ experiment_id: string }>('/api/experiments', {
    yaml_content: yamlContent,
    pipeline_yaml: pipelineYaml,
  });
  return res.data;
}

export async function runExperiment(experimentId: string): Promise<void> {
  await apiClient.post(`/api/experiments/${experimentId}/run`);
}

export async function getExperiment(experimentId: string): Promise<ExperimentDetail> {
  const res = await apiClient.get<ExperimentDetail>(`/api/experiments/${experimentId}`);
  return res.data;
}

export async function listExperiments(): Promise<Experiment[]> {
  const res = await apiClient.get<ExperimentListItem[]>('/api/experiments');
  return res.data.map(mapToExperiment);
}

export async function cancelExperiment(experimentId: string): Promise<void> {
  await apiClient.post(`/api/experiments/${experimentId}/cancel`);
}

export async function getReports(): Promise<Report[]> {
  const res = await apiClient.get<ReportListItem[]>('/api/reports');
  return res.data.map(mapToReport);
}

export async function getReport(reportId: string): Promise<Report> {
  const res = await apiClient.get<Record<string, unknown>>(`/api/reports/${reportId}`);
  const raw = res.data;
  // GET /api/reports/{id} returns metadata_ directly (the full canonical dict).
  return {
    id: reportId,
    title: (raw.title as string) ?? '',
    createdAt: (raw.created_at as string) ?? '',
    experimentName: (raw.experiment_name as string) ?? '',
    target: (raw.target_service as string) ?? null,
    failureType: (raw.fault_type as string) ?? null,
    recoveryTimeSeconds: (raw.recovery_time_seconds as number) ?? null,
    availabilityPercentage: (raw.availability_percentage as number) ?? null,
    throughputRecordsPerSec: (raw.throughput_records_per_sec as number) ?? null,
    dataLossPercentage: (raw.data_loss_percentage as number) ?? null,
    summary: raw,
  };
}

export async function analyzeReliability(
  observations: Array<Record<string, number>>
): Promise<ReliabilityAnalysis> {
  const res = await apiClient.post<ReliabilityAnalysis>('/api/ai/reliability-analysis', {
    observations,
  });
  return res.data;
}
