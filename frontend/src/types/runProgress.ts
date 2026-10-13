import { RunStatus } from './pipeline';

export type StageKey = 'source' | 'kafka' | 'spark' | 'hdfs';
export type StageState = 'pending' | 'active' | 'done' | 'error' | 'skipped';

export interface StageProgress {
  key: StageKey;
  state: StageState;
  startedAt: string | null;
  finishedAt: string | null;
  error: string | null;
}

export interface RunProgress {
  runId: string;
  status: RunStatus;
  mode: 'batch' | 'streaming';
  startedAt: string;
  updatedAt: string;
  completedAt: string | null;
  stages: StageProgress[];
  source: {
    type: string | null;
    rowsTotal: number | null;
    rowsRead: number | null;
  };
  kafka: {
    topic: string | null;
    partitions: number | null;
    published: number | null;
    failed: number | null;
    endOffsets: number | null;
    consumerLag: number | null;
  };
  spark: {
    appId: string | null;
    tasksTotal: number | null;
    tasksDone: number | null;
    recordsIn: number | null;
    recordsOut: number | null;
    durationMs: number | null;
    activeStage: null | {
      id: number;
      name: string;
      tasksTotal: number;
      tasksDone: number;
    };
  };
  hdfs: {
    path: string | null;
    filesWritten: number | null;
    bytesWritten: number | null;
    rowsWritten: number | null;
    verified: boolean | null;
  };
  throughput: {
    recordsPerSecond: number | null;
    samples: { t: number; v: number }[];
  };
  etaSeconds: number | null;
  errors: number | null;
  fault: null | {
    experimentId: string;
    target: string;
    type: string;
    phase: 'injected' | 'recovering' | 'recovered';
    injectedAt: string;
    durationSeconds: number;
    recoverySeconds: number | null;
  };
}

export interface RunEvent {
  seq: number;
  eventType: string;
  payload: Record<string, unknown>;
  ts: string;
}
