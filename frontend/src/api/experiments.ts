import { apiClient } from './client';
import {
  AllowedService,
  ComparisonTableRow,
  Experiment,
  ExperimentDetail,
  ExperimentEvent,
  ExperimentListItem,
  ExperimentOptions,
  ExperimentResults,
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

interface RawExperimentRecord {
  id?: string;
  experiment_id?: string;
  name?: string;
  experiment_name?: string;
  target?: AllowedService;
  target_service?: AllowedService;
  targetService?: AllowedService;
  faultType?: FaultType;
  fault_type?: FaultType;
  faultParams?: Record<string, unknown>;
  durationSeconds?: number;
  duration?: number;
  thresholds?: {
    expectedDataLoss?: number;
    expected_data_loss?: number;
    maxRecoverySeconds?: number;
    max_recovery_time?: number;
  };
  status?: 'pending' | 'running' | 'succeeded' | 'failed' | 'cancelled';
  latest_run_status?: 'pending' | 'running' | 'succeeded' | 'failed' | 'cancelled';
  latest_run_id?: string;
  latestRunId?: string;
  currentPhase?: string;
  lifecycleStage?: string;
  phases?: LifecyclePhase[];
  runId?: string;
  runStartedAt?: string | null;
  runFinishedAt?: string | null;
  faultInjectedAt?: string | null;
  faultRevertedAt?: string | null;
  recoveredAt?: string | null;
  faultPhase?: FaultPhase;
  serverTime?: string;
  pipelineRunId?: string;
  result?: string;
  failedCriteria?: string[];
  results?: Partial<ExperimentResults>;
  description?: string;
  experimentYaml?: string;
  experiment_yaml?: string;
  pipelineYaml?: string;
  pipeline_yaml?: string;
  createdAt?: string;
  created_at?: string;
  logs?: string[];
  comparisonTable?: ComparisonTableRow[] | null;
  comparison_table?: ComparisonTableRow[] | null;
  preFaultAccounting?: ExperimentDetail['preFaultAccounting'];
  pre_fault_accounting?: ExperimentDetail['preFaultAccounting'];
  repetitions?: number;
  totalRepetitions?: number;
  [key: string]: unknown;
}

// Converts the raw API list item to the canonical Experiment type.
export function mapToExperiment(item: ExperimentListItem | Record<string, unknown>): Experiment {
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
  const r = item as RawExperimentRecord;
  return {
    id: String(r.experiment_id || r.id || ''),
    name: String(r.name || r.experiment_name || ''),
    target: r.target_service || r.target || null,
    faultType: r.fault_type || r.faultType || null,
    status: r.latest_run_status || r.status || null,
    createdAt: String(r.created_at || r.createdAt || ''),
    latestRunId: r.latest_run_id || r.latestRunId || null,
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

export function mapToExperimentDetail(raw: Record<string, unknown>): ExperimentDetail {
  if (!raw || typeof raw !== 'object') {
    throw new Error('Invalid experiment detail payload');
  }

  const r = raw as RawExperimentRecord;
  const rawPhases = Array.isArray(r.phases) ? r.phases : [];
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

  const thresholds = r.thresholds || {};
  const results = r.results || {};

  const validFaultPhases: FaultPhase[] = ['not_injected', 'injected', 'recovering', 'recovered'];
  const faultPhase: FaultPhase = (r.faultPhase && validFaultPhases.includes(r.faultPhase))
    ? r.faultPhase
    : 'not_injected';

  return {
    id: String(r.id || r.experiment_id || ''),
    name: String(r.name || ''),
    targetService: String(r.targetService || r.target || ''),
    faultType: String(r.faultType || r.fault_type || ''),
    faultParams: typeof r.faultParams === 'object' && r.faultParams !== null ? r.faultParams : {},
    durationSeconds: Number(r.durationSeconds ?? r.duration ?? 20),
    thresholds: {
      expectedDataLoss: Number(thresholds.expectedDataLoss ?? thresholds.expected_data_loss ?? 0),
      maxRecoverySeconds: Number(thresholds.maxRecoverySeconds ?? thresholds.max_recovery_time ?? 60),
    },
    status: r.status && ['pending', 'running', 'succeeded', 'failed', 'cancelled'].includes(r.status)
      ? r.status
      : 'pending',
    currentPhase: String(r.currentPhase || r.lifecycleStage || 'pending'),
    phases,
    runId: r.runId ? String(r.runId) : null,
    runStartedAt: r.runStartedAt || null,
    runFinishedAt: r.runFinishedAt || null,
    faultInjectedAt: r.faultInjectedAt || null,
    faultRevertedAt: r.faultRevertedAt || null,
    recoveredAt: r.recoveredAt || null,
    faultPhase,
    serverTime: r.serverTime || new Date().toISOString(),
    pipelineRunId: r.pipelineRunId ? String(r.pipelineRunId) : null,
    result: r.result === 'PASS' || r.result === 'FAIL' ? r.result : null,
    failedCriteria: Array.isArray(r.failedCriteria) ? r.failedCriteria.map(String) : [],
    results: {
      recoveryTimeSeconds: results.recoveryTimeSeconds ?? results.recovery_time_seconds ?? null,
      dataLoss: Number(results.dataLoss ?? results.data_loss ?? 0),
      duplicates: Number(results.duplicates ?? (results['duplicate_ids_found'] as number | undefined) ?? 0),
      expectedRecords: Number(results.expectedRecords ?? (results['expected_record_count'] as number | undefined) ?? 0),
      processedRecords: Number(results.processedRecords ?? (results['actual_records'] as number | undefined) ?? 0),
      throughputBaseline: (results.throughputBaseline ?? results.throughput_baseline ?? null) as number | null,
      throughputDuringFault: results.throughputDuringFault ?? null,
      throughputAfter: (results.throughputAfter ?? results.throughput_records_per_sec ?? null) as number | null,
      throughputDegradationPercent: (results.throughputDegradationPercent ?? results.throughput_degradation_percent ?? null) as number | null,
      availabilityPercentage: (results.availabilityPercentage ?? results.availability_percentage ?? null) as number | null,
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
      logs: Array.isArray(r.logs) ? r.logs : (Array.isArray(results.logs) ? results.logs : []),
    },
    description: r.description ?? null,
    experimentYaml: r.experimentYaml || r.experiment_yaml || '',
    pipelineYaml: r.pipelineYaml || r.pipeline_yaml || null,
    target: r.target || (r.targetService as AllowedService),
    lifecycleStage: String(r.lifecycleStage || r.currentPhase || 'pending'),
    createdAt: r.createdAt || r.created_at || '',
    logs: Array.isArray(r.logs) ? r.logs : [],
    comparisonTable: r.comparisonTable || r.comparison_table || (results as Record<string, unknown>).comparisonTable as ComparisonTableRow[] || (results as Record<string, unknown>).comparison_table as ComparisonTableRow[] || undefined,
    comparison_table: r.comparisonTable || r.comparison_table || (results as Record<string, unknown>).comparisonTable as ComparisonTableRow[] || (results as Record<string, unknown>).comparison_table as ComparisonTableRow[] || undefined,
    preFaultAccounting: r.preFaultAccounting || r.pre_fault_accounting || undefined,
    totalRepetitions: r.repetitions ?? r.totalRepetitions ?? undefined,
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
  const res = await apiClient.get<Record<string, unknown>>(`/api/experiments/${experimentId}`);
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
