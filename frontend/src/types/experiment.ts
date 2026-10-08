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

// Shape returned by GET /api/experiments (list endpoint).
// Keys match the backend response exactly; mapToExperiment() in api/experiments.ts
// converts this to the canonical Experiment type used by components.
export interface ExperimentListItem {
  experiment_id: string;
  name: string;
  created_at: string;
  latest_run_status: 'pending' | 'running' | 'succeeded' | 'failed' | 'cancelled' | null;
  latest_run_id: string | null;
  target_service: AllowedService | null;
  fault_type: FaultType | null;
}

// Canonical frontend type — used by all components.
export interface Experiment {
  id: string;
  name: string;
  target: AllowedService | null;
  faultType: FaultType | null;
  status: 'pending' | 'running' | 'succeeded' | 'failed' | 'cancelled' | null;
  createdAt: string;
  latestRunId: string | null;
}

// Per-second availability sample stored in ExperimentRun.results["samples"].
export interface AvailabilitySample {
  timestamp: string;
  available: boolean;
}

// Full results dict as stored by MetricsCollector.capture_during_experiment().
// All fields are optional because not every experiment path populates every field.
export interface ExperimentResults {
  recovery_time_seconds?: number;
  availability_percentage?: number;
  throughput_records_per_sec?: number;
  data_loss_percentage?: number;
  samples?: AvailabilitySample[];
  logs?: string[];
  [key: string]: unknown;
}

// Shape returned by GET /api/experiments/{id} (detail endpoint).
export interface ExperimentDetail {
  id: string;
  name: string;
  description: string | null;
  experimentYaml: string;
  pipelineYaml: string | null;
  target: AllowedService | string;
  faultType: FaultType | string;
  status: 'pending' | 'running' | 'succeeded' | 'failed' | 'cancelled';
  createdAt: string;
  lifecycleStage: string;
  // ISO timestamps from ExperimentRun — null when run has not started yet.
  runStartedAt: string | null;
  runFinishedAt: string | null;
  // Real stage-transition log lines from ExperimentRun.results["logs"].
  // Empty array when no run has started or runner has not emitted logs yet.
  logs: string[];
  results: ExperimentResults;
}

export interface ExperimentRun {
  experimentId: string;
  durationSeconds: number;
}
