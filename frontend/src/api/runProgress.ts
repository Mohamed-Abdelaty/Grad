import { apiClient } from './client';
import { RunProgress, RunEvent, StageProgress, StageKey, StageState } from '../types/runProgress';
import { RunStatus } from '../types/pipeline';

const VALID_STAGES: StageKey[] = ['source', 'kafka', 'spark', 'hdfs'];
const VALID_STATES: StageState[] = ['pending', 'active', 'done', 'error', 'skipped'];
const VALID_STATUSES: RunStatus[] = ['pending', 'running', 'succeeded', 'failed', 'cancelled'];

export function isRunProgress(data: unknown): data is RunProgress {
  if (!data || typeof data !== 'object') {
    if (import.meta.env.DEV) {
      console.warn('isRunProgress: payload is not an object', data);
    }
    return false;
  }
  const obj = data as Record<string, unknown>;

  const runId = obj.runId ?? obj.run_id;
  if (typeof runId !== 'string' || !runId) {
    if (import.meta.env.DEV) {
      console.warn('isRunProgress: missing or invalid runId', data);
    }
    return false;
  }

  const status = obj.status;
  if (typeof status !== 'string' || !VALID_STATUSES.includes(status as RunStatus)) {
    if (import.meta.env.DEV) {
      console.warn('isRunProgress: invalid status', status);
    }
    return false;
  }

  if (!Array.isArray(obj.stages)) {
    if (import.meta.env.DEV) {
      console.warn('isRunProgress: stages is not an array', obj.stages);
    }
    return false;
  }

  return true;
}

export function mapRawProgress(raw: Record<string, unknown>): RunProgress {
  const runId = String(raw.runId || raw.run_id || '');
  const status = (raw.status as RunStatus) || 'pending';
  const mode = (raw.mode === 'streaming' ? 'streaming' : 'batch') as 'batch' | 'streaming';
  const startedAt = String(raw.startedAt || raw.started_at || new Date().toISOString());
  const updatedAt = String(raw.updatedAt || raw.updated_at || new Date().toISOString());
  const completedAt = raw.completedAt || raw.completed_at ? String(raw.completedAt || raw.completed_at) : null;

  const rawStages = Array.isArray(raw.stages) ? raw.stages : [];
  const stages: StageProgress[] = rawStages.map((st: Record<string, unknown>) => {
    const rawKey = String(st.key || '');
    const key: StageKey = VALID_STAGES.includes(rawKey as StageKey) ? (rawKey as StageKey) : 'source';
    const rawState = String(st.state || '');
    const state: StageState = VALID_STATES.includes(rawState as StageState) ? (rawState as StageState) : 'pending';
    return {
      key,
      state,
      startedAt: st.startedAt ? String(st.startedAt) : st.started_at ? String(st.started_at) : null,
      finishedAt: st.finishedAt ? String(st.finishedAt) : st.finished_at ? String(st.finished_at) : null,
      error: st.error ? String(st.error) : null,
    };
  });

  // Ensure all 4 stages exist
  for (const k of VALID_STAGES) {
    if (!stages.some((s) => s.key === k)) {
      stages.push({
        key: k,
        state: 'pending',
        startedAt: null,
        finishedAt: null,
        error: null,
      });
    }
  }

  const rawSource = (raw.source as Record<string, unknown>) || {};
  const source = {
    type: rawSource.type != null ? String(rawSource.type) : null,
    rowsTotal: typeof rawSource.rowsTotal === 'number' ? rawSource.rowsTotal : (typeof rawSource.rows_total === 'number' ? rawSource.rows_total : null),
    rowsRead: typeof rawSource.rowsRead === 'number' ? rawSource.rowsRead : (typeof rawSource.rows_read === 'number' ? rawSource.rows_read : null),
  };

  const rawKafka = (raw.kafka as Record<string, unknown>) || {};
  const kafka = {
    topic: rawKafka.topic != null ? String(rawKafka.topic) : null,
    partitions: typeof rawKafka.partitions === 'number' ? rawKafka.partitions : null,
    published: typeof rawKafka.published === 'number' ? rawKafka.published : null,
    failed: typeof rawKafka.failed === 'number' ? rawKafka.failed : null,
    endOffsets: typeof rawKafka.endOffsets === 'number' ? rawKafka.endOffsets : (typeof rawKafka.end_offsets === 'number' ? rawKafka.end_offsets : null),
    consumerLag: typeof rawKafka.consumerLag === 'number' ? rawKafka.consumerLag : (typeof rawKafka.consumer_lag === 'number' ? rawKafka.consumer_lag : null),
  };

  const rawSpark = (raw.spark as Record<string, unknown>) || {};
  let activeStage: RunProgress['spark']['activeStage'] = null;
  if (rawSpark.activeStage && typeof rawSpark.activeStage === 'object') {
    const act = rawSpark.activeStage as Record<string, unknown>;
    activeStage = {
      id: typeof act.id === 'number' ? act.id : 0,
      name: String(act.name || 'Stage'),
      tasksTotal: typeof act.tasksTotal === 'number' ? act.tasksTotal : 0,
      tasksDone: typeof act.tasksDone === 'number' ? act.tasksDone : 0,
    };
  }
  const spark = {
    appId: rawSpark.appId != null ? String(rawSpark.appId) : (rawSpark.app_id != null ? String(rawSpark.app_id) : null),
    tasksTotal: typeof rawSpark.tasksTotal === 'number' ? rawSpark.tasksTotal : (typeof rawSpark.tasks_total === 'number' ? rawSpark.tasks_total : null),
    tasksDone: typeof rawSpark.tasksDone === 'number' ? rawSpark.tasksDone : (typeof rawSpark.tasks_done === 'number' ? rawSpark.tasks_done : null),
    recordsIn: typeof rawSpark.recordsIn === 'number' ? rawSpark.recordsIn : (typeof rawSpark.records_in === 'number' ? rawSpark.records_in : null),
    recordsOut: typeof rawSpark.recordsOut === 'number' ? rawSpark.recordsOut : (typeof rawSpark.records_out === 'number' ? rawSpark.records_out : null),
    durationMs: typeof rawSpark.durationMs === 'number' ? rawSpark.durationMs : (typeof rawSpark.duration_ms === 'number' ? rawSpark.duration_ms : null),
    activeStage,
  };

  const rawHdfs = (raw.hdfs as Record<string, unknown>) || {};
  const hdfs = {
    path: rawHdfs.path != null ? String(rawHdfs.path) : null,
    filesWritten: typeof rawHdfs.filesWritten === 'number' ? rawHdfs.filesWritten : (typeof rawHdfs.files_written === 'number' ? rawHdfs.files_written : null),
    bytesWritten: typeof rawHdfs.bytesWritten === 'number' ? rawHdfs.bytesWritten : (typeof rawHdfs.bytes_written === 'number' ? rawHdfs.bytes_written : null),
    rowsWritten: typeof rawHdfs.rowsWritten === 'number' ? rawHdfs.rowsWritten : (typeof rawHdfs.rows_written === 'number' ? rawHdfs.rows_written : null),
    verified: typeof rawHdfs.verified === 'boolean' ? rawHdfs.verified : null,
  };

  const rawThroughput = (raw.throughput as Record<string, unknown>) || {};
  const rawSamples = Array.isArray(rawThroughput.samples) ? rawThroughput.samples : [];
  const samples = rawSamples
    .filter((s) => s && typeof s === 'object')
    .map((s: Record<string, unknown>) => ({
      t: typeof s.t === 'number' ? s.t : 0,
      v: typeof s.v === 'number' ? s.v : 0,
    }));
  const throughput = {
    recordsPerSecond: typeof rawThroughput.recordsPerSecond === 'number' ? rawThroughput.recordsPerSecond : (typeof rawThroughput.records_per_second === 'number' ? rawThroughput.records_per_second : null),
    samples,
  };

  let fault: RunProgress['fault'] = null;
  if (raw.fault && typeof raw.fault === 'object') {
    const rf = raw.fault as Record<string, unknown>;
    fault = {
      experimentId: String(rf.experimentId || rf.experiment_id || ''),
      target: String(rf.target || ''),
      type: String(rf.type || ''),
      phase: (rf.phase as 'injected' | 'recovering' | 'recovered') || 'injected',
      injectedAt: String(rf.injectedAt || rf.injected_at || ''),
      durationSeconds: typeof rf.durationSeconds === 'number' ? rf.durationSeconds : Number(rf.duration_seconds || 0),
      recoverySeconds: typeof rf.recoverySeconds === 'number' ? rf.recoverySeconds : (typeof rf.recovery_seconds === 'number' ? rf.recovery_seconds : null),
    };
  }

  return {
    runId,
    status,
    mode,
    startedAt,
    updatedAt,
    completedAt,
    stages,
    source,
    kafka,
    spark,
    hdfs,
    throughput,
    etaSeconds: typeof raw.etaSeconds === 'number' ? raw.etaSeconds : (typeof raw.eta_seconds === 'number' ? raw.eta_seconds : null),
    errors: typeof raw.errors === 'number' ? raw.errors : (Array.isArray(raw.errors) ? raw.errors.length : null),
    fault,
  };
}

export async function getProgress(runId: string): Promise<RunProgress> {
  const res = await apiClient.get<Record<string, unknown>>(`/api/pipeline-runs/${runId}/progress`);
  return mapRawProgress(res.data);
}

export function mapRawEvent(raw: Record<string, unknown>): RunEvent {
  return {
    seq: typeof raw.seq === 'number' ? raw.seq : 0,
    eventType: String(raw.eventType || raw.event_type || 'message'),
    payload: (raw.payload as Record<string, unknown>) || {},
    ts: String(raw.ts || raw.emitted_at || new Date().toISOString()),
  };
}

export async function getEvents(runId: string, afterSeq: number = 0): Promise<RunEvent[]> {
  const res = await apiClient.get<{ run_id?: string; events: Record<string, unknown>[] }>(
    `/api/pipeline-runs/${runId}/events`,
    { params: { after: afterSeq } }
  );
  const rawList = res.data?.events || [];
  return rawList.map(mapRawEvent);
}
