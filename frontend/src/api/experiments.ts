import { apiClient } from './client';
import {
  AllowedService,
  Experiment,
  ExperimentDetail,
  ExperimentEvent,
  ExperimentListItem,
  ExperimentOptions,
  FaultPhase,
  FaultType,
  LifecyclePhase,
} from '../types/experiment';

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
export function mapToExperiment(item: any): Experiment {
  if (!item || typeof item !== 'object') {
    return {
      id: '',
      name: '',
      target: null,
      faultType: null,
      status: null,
      createdAt: '',
      latestRunId: null,
    };
  }
  return {
    id: item.experiment_id || item.id || '',
    name: item.name || item.experiment_name || '',
    target: item.target_service || item.target || null,
    faultType: item.fault_type || item.faultType || null,
    status: item.latest_run_status || item.status || null,
    createdAt: item.created_at || item.createdAt || '',
    latestRunId: item.latest_run_id || item.latestRunId || null,
  };
}

const DEFAULT_PHASES = [
  'validating',
  'preparing',
  'baseline',
  'injecting',
  'monitoring',
  'rolling_back',
  'waiting_for_recovery',
  'validating_data',
  'collecting_metrics',
  'analyzing',
  'cleaning_up',
  'generating_report',
];

export function mapToExperimentDetail(raw: any): ExperimentDetail {
  if (!raw || typeof raw !== 'object') {
    throw new Error('Invalid experiment detail payload');
  }

  const rawPhases = Array.isArray(raw.phases) ? raw.phases : [];
  const phaseMap = new Map<string, LifecyclePhase>();
  for (const p of rawPhases) {
    if (p && typeof p.name === 'string') {
      phaseMap.set(p.name, {
        name: p.name,
        state: ['pending', 'active', 'done', 'error', 'skipped'].includes(p.state) ? p.state : 'pending',
        startedAt: p.startedAt || null,
        finishedAt: p.finishedAt || null,
      });
    }
  }

  const phases: LifecyclePhase[] = DEFAULT_PHASES.map((name) => {
    return (
      phaseMap.get(name) || {
        name,
        state: 'pending',
        startedAt: null,
        finishedAt: null,
      }
    );
  });

  const thresholds = raw.thresholds || {};
  const results = raw.results || {};

  const validFaultPhases: FaultPhase[] = ['not_injected', 'injected', 'recovering', 'recovered'];
  const faultPhase: FaultPhase = validFaultPhases.includes(raw.faultPhase)
    ? raw.faultPhase
    : 'not_injected';

  return {
    id: String(raw.id || raw.experiment_id || ''),
    name: String(raw.name || ''),
    targetService: String(raw.targetService || raw.target || ''),
    faultType: String(raw.faultType || raw.fault_type || ''),
    faultParams: typeof raw.faultParams === 'object' && raw.faultParams !== null ? raw.faultParams : {},
    durationSeconds: Number(raw.durationSeconds ?? raw.duration ?? 20),
    thresholds: {
      expectedDataLoss: Number(thresholds.expectedDataLoss ?? thresholds.expected_data_loss ?? 0),
      maxRecoverySeconds: Number(thresholds.maxRecoverySeconds ?? thresholds.max_recovery_time ?? 60),
    },
    status: ['pending', 'running', 'succeeded', 'failed', 'cancelled'].includes(raw.status)
      ? raw.status
      : 'pending',
    currentPhase: String(raw.currentPhase || raw.lifecycleStage || 'pending'),
    phases,
    runId: raw.runId ? String(raw.runId) : null,
    runStartedAt: raw.runStartedAt || null,
    runFinishedAt: raw.runFinishedAt || null,
    faultInjectedAt: raw.faultInjectedAt || null,
    faultRevertedAt: raw.faultRevertedAt || null,
    recoveredAt: raw.recoveredAt || null,
    faultPhase,
    serverTime: raw.serverTime || new Date().toISOString(),
    pipelineRunId: raw.pipelineRunId ? String(raw.pipelineRunId) : null,
    result: raw.result === 'PASS' || raw.result === 'FAIL' ? raw.result : null,
    failedCriteria: Array.isArray(raw.failedCriteria) ? raw.failedCriteria.map(String) : [],
    results: {
      recoveryTimeSeconds: results.recoveryTimeSeconds ?? results.recovery_time_seconds ?? null,
      dataLoss: Number(results.dataLoss ?? results.data_loss ?? 0),
      duplicates: Number(results.duplicates ?? results.duplicate_ids_found ?? 0),
      expectedRecords: Number(results.expectedRecords ?? results.expected_record_count ?? 0),
      processedRecords: Number(results.processedRecords ?? results.actual_records ?? 0),
      throughputBaseline: results.throughputBaseline ?? results.throughput_baseline ?? null,
      throughputDuringFault: results.throughputDuringFault ?? null,
      throughputAfter: results.throughputAfter ?? results.throughput_records_per_sec ?? null,
      throughputDegradationPercent: results.throughputDegradationPercent ?? results.throughput_degradation_percent ?? null,
      availabilityPercentage: results.availabilityPercentage ?? results.availability_percentage ?? null,
      errorRate: results.errorRate ?? null,
      errorCount: results.errorCount ?? null,
      totalOperations: results.totalOperations ?? null,
      availabilitySamples: Array.isArray(results.availabilitySamples) ? results.availabilitySamples : (Array.isArray(results.samples) ? results.samples : null),
      throughputSamples: Array.isArray(results.throughputSamples) ? results.throughputSamples : null,
      cpuSamples: Array.isArray(results.cpuSamples) ? results.cpuSamples : null,
      memorySamples: Array.isArray(results.memorySamples) ? results.memorySamples : null,
      recovery_time_seconds: results.recovery_time_seconds,
      availability_percentage: results.availability_percentage,
      throughput_records_per_sec: results.throughput_records_per_sec,
      data_loss_percentage: results.data_loss_percentage,
      logs: Array.isArray(raw.logs) ? raw.logs : (Array.isArray(results.logs) ? results.logs : []),
    },
    description: raw.description ?? null,
    experimentYaml: raw.experimentYaml || raw.experiment_yaml || '',
    pipelineYaml: raw.pipelineYaml || raw.pipeline_yaml || null,
    target: raw.target || raw.targetService,
    lifecycleStage: raw.lifecycleStage || raw.currentPhase,
    createdAt: raw.createdAt || raw.created_at || '',
    logs: Array.isArray(raw.logs) ? raw.logs : [],
  };
}

export async function getOptions(): Promise<ExperimentOptions> {
  const res = await apiClient.get<ExperimentOptions>('/api/experiments/options');
  return res.data;
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
  const res = await apiClient.get<any>(`/api/experiments/${experimentId}`);
  return mapToExperimentDetail(res.data);
}

export async function listExperiments(): Promise<Experiment[]> {
  const res = await apiClient.get<ExperimentListItem[]>('/api/experiments');
  return res.data.map(mapToExperiment);
}

export async function cancelExperiment(experimentId: string): Promise<void> {
  await apiClient.post(`/api/experiments/${experimentId}/cancel`);
}

export async function getExperimentEvents(
  experimentId: string,
  after = 0
): Promise<ExperimentEvent[]> {
  const res = await apiClient.get<ExperimentEvent[]>(
    `/api/experiments/${experimentId}/events?after=${after}`
  );
  return res.data;
}

export async function analyzeReliability(
  observations: Array<Record<string, number>>
): Promise<ReliabilityAnalysis> {
  const res = await apiClient.post<ReliabilityAnalysis>('/api/ai/reliability-analysis', {
    observations,
  });
  return res.data;
}

export { getReports, getReport, listReports, mapToReport } from './reports';
