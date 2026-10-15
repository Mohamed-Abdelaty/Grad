import yaml from 'yaml';
import {
  AvailabilitySample,
  ExperimentDetail,
  ExperimentOptions,
  ExperimentResults,
  ResourceSample,
} from '../types/experiment';
import { ReportListItem } from '../types/report';
import { RunProgress } from '../types/runProgress';

export interface ExperimentFormState {
  name: string;
  targetId: string;
  faultId: string;
  duration: number;
  params: Record<string, unknown>;
  expectedDataLoss: number;
  maxRecoverySeconds: number;
}

export interface CriterionResult {
  name: string;
  threshold: string;
  actual: string;
  passed: boolean | null;
}

/**
 * Builds experiment YAML string from form state using yaml.stringify (safe escaping).
 */
export function buildExperimentYaml(form: ExperimentFormState): string {
  const faultBlock: Record<string, unknown> = {
    type: form.faultId,
    duration: Number(form.duration),
  };

  if (form.params && typeof form.params === 'object') {
    for (const [key, val] of Object.entries(form.params)) {
      if (val !== undefined && val !== null && val !== '') {
        faultBlock[key] = val;
      }
    }
  }

  const doc = {
    experiment: {
      name: form.name.trim(),
    },
    target: {
      service: form.targetId,
    },
    fault: faultBlock,
    validation: {
      expected_data_loss: Number(form.expectedDataLoss ?? 0),
      max_recovery_time: Number(form.maxRecoverySeconds ?? 30),
    },
  };

  return yaml.stringify(doc);
}

/**
 * Generates human-readable review sentence describing the experiment.
 */
export function describeExperiment(
  form: ExperimentFormState,
  options?: ExperimentOptions
): string {
  const target = options?.targets.find((t) => t.id === form.targetId);
  const targetLabel = target?.label || form.targetId || 'Selected service';

  let faultPhrase = `subjected to ${form.faultId}`;
  if (form.faultId === 'container_stop') {
    faultPhrase = `${targetLabel} will be stopped`;
  } else if (form.faultId === 'container_restart') {
    faultPhrase = `${targetLabel} will be restarted`;
  } else if (form.faultId === 'service_delay') {
    const delay = form.params?.delay_ms ? `${form.params.delay_ms} ms latency` : 'network delay';
    faultPhrase = `${targetLabel} will have ${delay} injected`;
  } else if (form.faultId === 'cpu_stress') {
    faultPhrase = `${targetLabel} will be subjected to CPU stress`;
  } else if (form.faultId === 'memory_stress') {
    faultPhrase = `${targetLabel} will be subjected to memory pressure`;
  } else if (form.faultId === 'disk_pressure') {
    faultPhrase = `${targetLabel} will be subjected to disk pressure`;
  } else {
    faultPhrase = `${targetLabel} will be subjected to ${form.faultId}`;
  }

  return `${faultPhrase} for ${form.duration} seconds. The pipeline is expected to lose ${form.expectedDataLoss} records and recover within ${form.maxRecoverySeconds} seconds.`;
}

/**
 * Calculates clock offset in ms between server time and client now.
 */
export function serverClockOffset(
  serverTime?: string | null,
  clientNow: number = Date.now()
): number {
  if (!serverTime) return 0;
  const serverMs = new Date(serverTime).getTime();
  if (isNaN(serverMs)) return 0;
  return serverMs - clientNow;
}

/**
 * Returns elapsed seconds for the currently active phase.
 */
export function phaseElapsedSeconds(
  detail: ExperimentDetail,
  nowAligned: number = Date.now()
): number {
  if (!detail || !detail.phases) return 0;
  const current = detail.phases.find((p) => p.name === detail.currentPhase || p.state === 'active');
  if (!current || !current.startedAt) return 0;

  const startMs = new Date(current.startedAt).getTime();
  if (isNaN(startMs)) return 0;

  if (current.finishedAt) {
    const finishMs = new Date(current.finishedAt).getTime();
    if (!isNaN(finishMs)) {
      return Math.max(0, Math.round((finishMs - startMs) / 1000));
    }
  }

  return Math.max(0, Math.round((nowAligned - startMs) / 1000));
}

/**
 * Calculates countdown of active fault using server-aligned timestamps.
 */
export function faultCountdown(
  detail: ExperimentDetail,
  nowAligned: number = Date.now()
): { elapsed: number; total: number; active: boolean } {
  const total = detail?.durationSeconds || 0;
  if (!detail || detail.faultPhase !== 'injected' || !detail.faultInjectedAt) {
    return { elapsed: 0, total, active: false };
  }

  const injectedMs = new Date(detail.faultInjectedAt).getTime();
  if (isNaN(injectedMs)) {
    return { elapsed: 0, total, active: false };
  }

  const elapsed = Math.max(0, Math.min(total, Math.round((nowAligned - injectedMs) / 1000)));
  return { elapsed, total, active: true };
}

/**
 * Evaluates pass/fail status per criterion.
 */
export function criterionLabels(detail: ExperimentDetail): CriterionResult[] {
  if (!detail) return [];

  const results = detail.results || {};
  const thresholds = detail.thresholds || { expectedDataLoss: 0, maxRecoverySeconds: 30 };

  const resultsDict = results as unknown as Record<string, unknown>;
  const rawLoss = results.dataLoss ?? resultsDict.data_loss;
  const isLossMeasured = rawLoss !== null && rawLoss !== undefined;
  const lossActual = isLossMeasured ? Number(rawLoss) : null;
  const lossPassed = isLossMeasured ? (lossActual! <= thresholds.expectedDataLoss) : null;

  const recActual = results.recoveryTimeSeconds ?? (resultsDict.recovery_time_seconds as number | null | undefined);
  const recPassed = recActual !== null && recActual !== undefined && recActual <= thresholds.maxRecoverySeconds;

  const statusPassed = detail.status !== 'failed' && (detail.result === 'PASS' || detail.result === null);

  return [
    {
      name: 'Recovery Time',
      threshold: `≤ ${thresholds.maxRecoverySeconds} s`,
      actual: recActual !== null && recActual !== undefined ? `${Number(recActual).toFixed(1)} s` : 'N/A',
      passed: recPassed,
    },
    {
      name: 'Data Loss',
      threshold: `≤ ${thresholds.expectedDataLoss} records`,
      actual: isLossMeasured ? `${lossActual} records` : 'Data loss: not measured',
      passed: lossPassed,
    },
    {
      name: 'Execution Errors',
      threshold: '0 unhandled errors',
      actual: detail.status === 'failed' ? 'Errors occurred' : 'None',
      passed: statusPassed,
    },
  ];
}

/**
 * Transforms availability samples into step chart data.
 */
export function availabilityStepData(
  samples: AvailabilitySample[] | null | undefined,
  faultWindow?: { start?: string | null; end?: string | null }
): Array<{ t: string | number; available: number; inFaultWindow: boolean }> | null {
  if (!samples || !Array.isArray(samples) || samples.length === 0) {
    return null;
  }

  const startMs = faultWindow?.start ? new Date(faultWindow.start).getTime() : null;
  const endMs = faultWindow?.end ? new Date(faultWindow.end).getTime() : null;

  return samples.map((s) => {
    const rawT = s.t ?? (s as unknown as Record<string, unknown>).timestamp;
    const sampleMs = typeof rawT === 'number' ? rawT : new Date(String(rawT)).getTime();
    const inWindow =
      startMs !== null && endMs !== null && !isNaN(sampleMs)
        ? sampleMs >= startMs && sampleMs <= endMs
        : false;

    return {
      t: rawT,
      available: s.available ? 1 : 0,
      inFaultWindow: inWindow,
    };
  });
}

/**
 * Builds throughput comparison bar chart data.
 */
export function throughputBars(
  results: ExperimentResults | null | undefined
): Array<{ name: string; throughput: number }> | null {
  if (!results) return null;
  const baseline = results.throughputBaseline;
  const during = results.throughputDuringFault;
  const after = results.throughputAfter;

  if (baseline == null && during == null && after == null) {
    return null;
  }

  return [
    { name: 'Baseline', throughput: baseline ?? 0 },
    { name: 'During Fault', throughput: during ?? 0 },
    { name: 'After Recovery', throughput: after ?? 0 },
  ];
}

/**
 * Builds recovery time trend points across reports.
 */
export function recoveryTrend(
  reports: ReportListItem[] | null | undefined
): Array<{ id: string; date: string; recoveryTime: number; result: string }> | null {
  if (!reports || !Array.isArray(reports) || reports.length === 0) {
    return null;
  }

  const valid = reports.filter((r) => r.recoveryTimeSeconds !== null && r.recoveryTimeSeconds !== undefined);
  if (valid.length === 0) return null;

  return valid.map((r) => ({
    id: r.id,
    date: r.createdAt ? new Date(r.createdAt).toLocaleDateString() : '',
    recoveryTime: r.recoveryTimeSeconds as number,
    result: r.result || 'UNKNOWN',
  }));
}

/**
 * Builds resource utilization series from samples.
 */
export function resourceSeries(
  samples: ResourceSample[] | null | undefined
): Array<{ t: string | number; service: string; percent?: number; bytes?: number }> | null {
  if (!samples || !Array.isArray(samples) || samples.length === 0) {
    return null;
  }

  return samples.map((s) => ({
    t: s.t,
    service: s.service,
    percent: s.percent,
    bytes: s.bytes,
  }));
}

/**
 * Maps ExperimentDetail to a truthful RunProgress snapshot for compact pipeline visualization.
 */
export function experimentToProgress(detail: ExperimentDetail): RunProgress | null {
  if (!detail) return null;

  const faultObj =
    detail.faultPhase && detail.faultPhase !== 'not_injected'
      ? {
          experimentId: detail.id,
          target: detail.targetService,
          type: detail.faultType,
          phase: detail.faultPhase as 'injected' | 'recovering' | 'recovered',
          injectedAt: detail.faultInjectedAt || detail.runStartedAt || new Date().toISOString(),
          durationSeconds: detail.durationSeconds,
          recoverySeconds: detail.results?.recoveryTimeSeconds ?? null,
        }
      : null;

  return {
    runId: detail.pipelineRunId || detail.id,
    status:
      detail.status === 'succeeded'
        ? 'succeeded'
        : detail.status === 'failed'
        ? 'failed'
        : detail.status === 'cancelled'
        ? 'cancelled'
        : 'running',
    mode: 'batch',
    startedAt: detail.runStartedAt || detail.createdAt || new Date().toISOString(),
    updatedAt: detail.serverTime || new Date().toISOString(),
    completedAt: detail.runFinishedAt || null,
    stages: [
      { key: 'source', state: 'pending', startedAt: null, finishedAt: null, error: null },
      { key: 'kafka', state: 'pending', startedAt: null, finishedAt: null, error: null },
      { key: 'spark', state: 'pending', startedAt: null, finishedAt: null, error: null },
      { key: 'hdfs', state: 'pending', startedAt: null, finishedAt: null, error: null },
    ],
    source: { type: 'csv', rowsTotal: null, rowsRead: null },
    kafka: { topic: null, partitions: null, published: null, failed: 0, endOffsets: null, consumerLag: null },
    spark: { appId: null, tasksTotal: null, tasksDone: null, recordsIn: null, recordsOut: null, durationMs: null, activeStage: null },
    hdfs: { path: null, filesWritten: null, bytesWritten: null, rowsWritten: null, verified: null },
    throughput: { recordsPerSecond: null, samples: [] },
    etaSeconds: null,
    errors: null,
    fault: faultObj,
  };
}
