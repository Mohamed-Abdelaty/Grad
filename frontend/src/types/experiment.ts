export type FaultType =
  | 'container_stop'
  | 'container_restart'
  | 'network_disruption'
  | 'cpu_stress'
  | 'memory_stress'
  | 'disk_pressure'
  | 'service_delay';

export type AllowedService =
  | 'kafka'
  | 'spark-master'
  | 'spark-worker'
  | 'namenode'
  | 'datanode'
  | 'postgres'
  | 'backend';

export interface TargetOption {
  id: string;
  label: string;
  description: string;
  risk: 'low' | 'medium' | 'high';
  service: string;
}

export interface FaultParamOption {
  name: string;
  type: string;
  label?: string;
  default?: unknown;
  min?: number;
  max?: number;
  unit?: string;
}

export interface FaultOption {
  id: string;
  label: string;
  description: string;
  blastRadius: string;
  params: FaultParamOption[];
  icon: string;
}

export interface ExperimentLimits {
  minDurationSeconds: number;
  maxDurationSeconds: number;
}

export interface ExperimentOptions {
  targets: TargetOption[];
  faults: FaultOption[];
  limits: ExperimentLimits;
}

export type PhaseState = 'pending' | 'active' | 'done' | 'error' | 'skipped';

export interface LifecyclePhase {
  name: string;
  state: PhaseState;
  startedAt: string | null;
  finishedAt: string | null;
}

export interface AvailabilitySample {
  t: string | number;
  available: boolean;
  timestamp?: string; // backwards compatibility
}

export interface MetricSample {
  t: string | number;
  v: number;
}

export interface ResourceSample {
  t: string | number;
  service: string;
  percent?: number;
  bytes?: number;
}

export interface ComparisonTableRow {
  metric: string;
  label: string;
  unit: string;
  normal: number | null;
  normal_reason?: string | null;
  normalReason?: string | null;
  during_fault: number | null;
  duringFault?: number | null;
  during_fault_reason?: string | null;
  duringFaultReason?: string | null;
  delta: number | null;
  pct_change: number | null;
  pctChange?: number | null;
  baseline_applicable: boolean;
  baselineApplicable?: boolean;
  measured_directly: boolean;
  measuredDirectly?: boolean;
  definition: string;
  output_duplicates?: number | null;
  outputDuplicates?: number | null;
  source_duplicates?: number | null;
  sourceDuplicates?: number | null;
  fault_induced_duplicates?: number | null;
  faultInducedDuplicates?: number | null;
  aggregate?: {
    mean: number | null;
    std: number | null;
    min: number | null;
    max: number | null;
    n_samples: number;
    nSamples?: number;
  } | null;
}

export interface ExperimentResults {
  recoveryTimeSeconds: number | null;
  dataLoss: number;
  duplicates: number;
  expectedRecords: number;
  processedRecords: number;
  throughputBaseline: number | null;
  throughputDuringFault: number | null;
  throughputAfter: number | null;
  throughputDegradationPercent: number | null;
  availabilityPercentage: number | null;
  errorRate: number | null;
  errorCount: number | null;
  totalOperations: number | null;
  availabilitySamples: AvailabilitySample[] | null;
  throughputSamples: MetricSample[] | null;
  cpuSamples: ResourceSample[] | null;
  comparisonTable?: ComparisonTableRow[] | null;
  comparison_table?: ComparisonTableRow[] | null;
  memorySamples: ResourceSample[] | null;

  // Compatibility fields
  recovery_time_seconds?: number;
  availability_percentage?: number;
  throughput_records_per_sec?: number;
  data_loss_percentage?: number;
  samples?: AvailabilitySample[];
  logs?: string[];
  [key: string]: unknown;
}

export interface ExperimentEvent {
  seq: number;
  level: string;
  phase: string;
  message: string;
  ts: string;
}

export type FaultPhase = 'not_injected' | 'injected' | 'recovering' | 'recovered';

export interface ExperimentListItem {
  experiment_id: string;
  name: string;
  created_at: string;
  latest_run_status: 'pending' | 'running' | 'succeeded' | 'failed' | 'cancelled' | null;
  latest_run_id: string | null;
  target_service: AllowedService | null;
  fault_type: FaultType | null;
}

export interface Experiment {
  id: string;
  name: string;
  target: AllowedService | null;
  faultType: FaultType | null;
  status: 'pending' | 'running' | 'succeeded' | 'failed' | 'cancelled' | null;
  createdAt: string;
  latestRunId: string | null;
}

export interface ExperimentThresholds {
  expectedDataLoss: number;
  maxRecoverySeconds: number;
}

export interface ExperimentDetail {
  id: string;
  name: string;
  targetService: string;
  faultType: string;
  faultParams: Record<string, unknown>;
  durationSeconds: number;
  thresholds: ExperimentThresholds;
  status: 'pending' | 'running' | 'succeeded' | 'failed' | 'cancelled';
  currentPhase: string;
  phases: LifecyclePhase[];
  runId: string | null;
  runStartedAt: string | null;
  runFinishedAt: string | null;
  faultInjectedAt: string | null;
  faultRevertedAt: string | null;
  recoveredAt: string | null;
  faultPhase: FaultPhase;
  serverTime: string;
  pipelineRunId: string | null;
  result: 'PASS' | 'FAIL' | null;
  failedCriteria: string[];
  results: ExperimentResults;
  comparisonTable?: ComparisonTableRow[] | null;
  comparison_table?: ComparisonTableRow[] | null;
  preFaultAccounting?: {
    snapshot_timestamp: string;
    pipeline_run_id: string;
    records_received: number | null;
    records_processed: number | null;
    records_written: number | null;
  } | null;
  totalRepetitions?: number;
  currentRepetition?: number | null;
  aggregatedMetrics?: Record<string, {
    n_samples: number;
    mean: number | null;
    std: number | null;
    min: number | null;
    max: number | null;
  }> | null;
  repetitionResults?: Array<{
    repetition_index: number;
    run_id?: string;
    status: string;
    result: string;
    recovery_time_seconds?: number | null;
    data_loss?: number | null;
    duplicates?: number | null;
    availability_percentage?: number | null;
    throughput_records_per_sec?: number | null;
    error?: string | null;
  }> | null;

  // Compatibility fields
  description?: string | null;
  experimentYaml?: string;
  pipelineYaml?: string | null;
  target?: AllowedService | string;
  lifecycleStage: string;
  createdAt: string;
  logs: string[];
}

export interface ExperimentRun {
  experimentId: string;
  durationSeconds: number;
}
