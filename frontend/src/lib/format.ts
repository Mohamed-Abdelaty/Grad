import { RunProgress, RunEvent, StageKey, StageState } from '../types/runProgress';

export const EM_DASH = '—';

export function formatCount(n: number | null | undefined): string {
  if (n == null || isNaN(n)) return EM_DASH;
  return n.toLocaleString('en-US');
}

export function formatBytes(n: number | null | undefined): string {
  if (n == null || isNaN(n) || n < 0) return EM_DASH;
  if (n < 1024) return `${n} B`;
  const kb = n / 1024;
  if (kb < 1024) return `${Number.isInteger(kb) ? kb : kb.toFixed(1)} KB`;
  const mb = kb / 1024;
  if (mb < 1024) return `${Number.isInteger(mb) ? mb : mb.toFixed(1)} MB`;
  const gb = mb / 1024;
  return `${Number.isInteger(gb) ? gb : gb.toFixed(1)} GB`;
}

export function formatDuration(ms: number | null | undefined): string {
  if (ms == null || isNaN(ms) || ms < 0) return EM_DASH;
  if (ms < 1000) return `${Math.round(ms)} ms`;
  const totalSec = ms / 1000;
  if (totalSec < 60) return `${totalSec.toFixed(1)} s`;
  const minutes = Math.floor(totalSec / 60);
  const seconds = Math.floor(totalSec % 60);
  if (minutes < 60) {
    return `${minutes} m ${seconds < 10 ? '0' : ''}${seconds} s`;
  }
  const hours = Math.floor(minutes / 60);
  const remMinutes = minutes % 60;
  return `${hours} h ${remMinutes < 10 ? '0' : ''}${remMinutes} m`;
}

export function formatRate(n: number | null | undefined): string {
  if (n == null || isNaN(n) || n < 0) return EM_DASH;
  return `${Math.round(n).toLocaleString('en-US')} rec/s`;
}

export function elapsedMs(
  startedAt: string | null | undefined,
  completedAt: string | null | undefined,
  nowMs: number = Date.now()
): number {
  if (!startedAt) return 0;
  const start = new Date(startedAt).getTime();
  if (isNaN(start)) return 0;

  const end = completedAt ? new Date(completedAt).getTime() : nowMs;
  if (isNaN(end)) return 0;

  return Math.max(0, end - start);
}

export function stageLabel(key: StageKey): string {
  switch (key) {
    case 'source':
      return 'Source';
    case 'kafka':
      return 'Kafka';
    case 'spark':
      return 'Spark';
    case 'hdfs':
      return 'HDFS';
  }
}

export function stageStateLabel(state: StageState): string {
  switch (state) {
    case 'pending':
      return 'Pending';
    case 'active':
      return 'Active';
    case 'done':
      return 'Completed';
    case 'error':
      return 'Failed';
    case 'skipped':
      return 'Skipped';
  }
}

export function formatEventMessage(e: RunEvent): { level: 'info' | 'warn' | 'error'; text: string } {
  const payload = e.payload || {};
  const isError =
    e.eventType === 'run_failed' ||
    e.eventType === 'source_failed' ||
    Boolean(payload.error) ||
    Boolean(payload.errorMessage);

  const isWarn =
    !isError &&
    (e.eventType.includes('warning') ||
      Boolean(payload.warning) ||
      (typeof payload.failed === 'number' && payload.failed > 0));

  const level: 'info' | 'warn' | 'error' = isError ? 'error' : isWarn ? 'warn' : 'info';

  switch (e.eventType) {
    case 'source_start':
      return { level, text: 'Source ingestion started.' };
    case 'source_done': {
      const rows = payload.rows ?? payload.records ?? payload.rowsTotal;
      return {
        level,
        text: rows != null ? `Source read finished: ${rows} records.` : 'Source read completed.',
      };
    }
    case 'source_failed':
      return { level: 'error', text: `Source reading failed: ${payload.error || 'Unknown error'}` };

    case 'kafka_start':
      return { level, text: `Kafka publishing started on topic "${payload.topic || 'default'}".` };
    case 'kafka_progress':
      return {
        level,
        text: `Publishing to Kafka: ${payload.published ?? 0} records published${
          payload.failed ? ` (${payload.failed} failed)` : ''
        }.`,
      };
    case 'kafka_done':
      return {
        level,
        text: `Kafka publishing completed: ${payload.published ?? 0} records published${
          payload.failed ? ` (${payload.failed} failed)` : ''
        }.`,
      };

    case 'stage_skipped':
      return { level: 'warn', text: `Stage skipped: ${payload.stage || 'unknown'}.` };

    case 'spark_start':
      return { level, text: 'Spark processing job submitted and started.' };
    case 'spark_progress': {
      const done = payload.tasksDone;
      const total = payload.tasksTotal;
      if (done != null && total != null) {
        return { level, text: `Spark execution progress: ${done} / ${total} tasks completed.` };
      }
      return { level, text: 'Spark job executing...' };
    }
    case 'spark_done': {
      const rIn = payload.recordsIn ?? payload.records_in;
      const rOut = payload.recordsOut ?? payload.records_out;
      const dur = payload.durationMs != null ? formatDuration(Number(payload.durationMs)) : null;
      let text = 'Spark job finished';
      if (rIn != null && rOut != null) {
        text += `: ${rIn} rows in, ${rOut} rows out`;
      }
      if (dur) {
        text += `, ${dur}`;
      }
      text += '.';
      return { level, text };
    }

    case 'hdfs_start':
      return { level, text: `Writing processed records to HDFS storage: ${payload.path || ''}` };
    case 'hdfs_progress':
      return { level, text: 'HDFS verification in progress...' };
    case 'hdfs_done': {
      const rows = payload.rowsWritten ?? payload.rows;
      return {
        level,
        text: `HDFS storage verified: ${rows != null ? `${rows} rows written` : 'verified'}.`,
      };
    }

    case 'throughput_update': {
      const rps = payload.recordsPerSecond ?? payload.rps;
      return { level, text: `Throughput measured at ${formatRate(Number(rps))}.` };
    }

    case 'run_success':
      return { level: 'info', text: 'Pipeline run completed successfully.' };
    case 'run_failed':
      return { level: 'error', text: `Pipeline run failed: ${payload.error || 'Execution error'}` };
    case 'run_cancelled':
      return { level: 'warn', text: 'Pipeline run was cancelled by user.' };

    default: {
      const json = JSON.stringify(payload);
      const truncated = json.length > 300 ? `${json.slice(0, 300)}...` : json;
      return {
        level,
        text: Object.keys(payload).length > 0 ? `${e.eventType}: ${truncated}` : e.eventType,
      };
    }
  }
}

export interface ActiveStageSummary {
  label: string;
  ratio: number | null;
  indeterminate: boolean;
}

export function activeStageSummary(progress: RunProgress | null | undefined): ActiveStageSummary {
  if (!progress || !Array.isArray(progress.stages)) {
    return { label: 'Idle', ratio: null, indeterminate: false };
  }

  const activeStage = progress.stages.find((s) => s.state === 'active');
  if (!activeStage) {
    return { label: 'Idle', ratio: null, indeterminate: false };
  }

  switch (activeStage.key) {
    case 'source': {
      const total = progress.source.rowsTotal;
      const read = progress.source.rowsRead;
      if (typeof total === 'number' && total > 0 && typeof read === 'number') {
        const ratio = Math.max(0, Math.min(1, read / total));
        return { label: 'Source: reading records', ratio, indeterminate: false };
      }
      return { label: 'Source: reading records', ratio: null, indeterminate: true };
    }

    case 'kafka': {
      const total = progress.source.rowsTotal;
      const pub = progress.kafka.published;
      if (typeof total === 'number' && total > 0 && typeof pub === 'number') {
        const ratio = Math.max(0, Math.min(1, pub / total));
        return { label: 'Kafka: publishing records', ratio, indeterminate: false };
      }
      return { label: 'Kafka: publishing records', ratio: null, indeterminate: true };
    }

    case 'spark': {
      const act = progress.spark.activeStage;
      if (act && typeof act.tasksTotal === 'number' && act.tasksTotal > 0 && typeof act.tasksDone === 'number') {
        const ratio = Math.max(0, Math.min(1, act.tasksDone / act.tasksTotal));
        return { label: `Spark: processing stage "${act.name}"`, ratio, indeterminate: false };
      }
      return { label: 'Spark: processing dataset', ratio: null, indeterminate: true };
    }

    case 'hdfs': {
      if (progress.hdfs.verified === true) {
        return { label: 'HDFS: records verified', ratio: 1, indeterminate: false };
      }
      return { label: 'HDFS: storing and verifying records', ratio: null, indeterminate: true };
    }

    default:
      return { label: 'Processing', ratio: null, indeterminate: true };
  }
}
