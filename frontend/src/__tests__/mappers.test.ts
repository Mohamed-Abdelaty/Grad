import { describe, it, expect } from 'vitest';
import { mapToExperiment, mapToReport } from '../api/experiments';
import type { ExperimentListItem } from '../types/experiment';
import type { ReportListItem } from '../types/report';

describe('mapToExperiment', () => {
  const item: ExperimentListItem = {
    experiment_id: 'abc-123',
    name: 'Kafka stop test',
    created_at: '2026-01-01T00:00:00Z',
    latest_run_status: 'succeeded',
    latest_run_id: 'run-456',
    target_service: 'kafka',
    fault_type: 'container_stop',
  };

  it('maps API snake_case keys to the canonical Experiment shape', () => {
    expect(mapToExperiment(item)).toEqual({
      id: 'abc-123',
      name: 'Kafka stop test',
      target: 'kafka',
      faultType: 'container_stop',
      status: 'succeeded',
      createdAt: '2026-01-01T00:00:00Z',
      latestRunId: 'run-456',
    });
  });

  it('keeps null target and status as null', () => {
    const mapped = mapToExperiment({ ...item, target_service: null, latest_run_status: null });
    expect(mapped.target).toBeNull();
    expect(mapped.status).toBeNull();
  });
});

describe('mapToReport', () => {
  const item: ReportListItem = {
    report_id: 'rep-999',
    title: 'Kafka resilience report',
    created_at: '2026-06-01T12:00:00Z',
    summary: {
      experiment_name: 'Kafka stop test',
      target_service: 'kafka',
      fault_type: 'container_stop',
      recovery_time_seconds: 45.2,
      availability_percentage: 92.1,
      throughput_records_per_sec: 1200,
      data_loss_percentage: 0.3,
    },
  };

  it('maps summary metrics to canonical fields', () => {
    const r = mapToReport(item);
    expect(r.id).toBe('rep-999');
    expect(r.experimentName).toBe('Kafka stop test');
    expect(r.target).toBe('kafka');
    expect(r.failureType).toBe('container_stop');
    expect(r.recoveryTimeSeconds).toBe(45.2);
    expect(r.availabilityPercentage).toBe(92.1);
    expect(r.throughputRecordsPerSec).toBe(1200);
    expect(r.dataLossPercentage).toBe(0.3);
  });

  it('falls back to the title and null metrics when the summary is empty', () => {
    const r = mapToReport({ ...item, summary: {} });
    expect(r.experimentName).toBe('Kafka resilience report');
    expect(r.recoveryTimeSeconds).toBeNull();
    expect(r.availabilityPercentage).toBeNull();
    expect(r.dataLossPercentage).toBeNull();
  });
});

describe('mapRawProgress', () => {
  it('maps errors array to count', async () => {
    const { mapRawProgress } = await import('../api/runProgress');

    // Case 1: Backend returns an array of strings -> map to array length
    const res1 = mapRawProgress({
      runId: 'r-1',
      status: 'failed',
      errors: ['First error', 'Second error'],
      stages: [],
    });
    expect(res1.errors).toBe(2);

    // Case 2: Backend returns empty array [] -> map to 0
    const res2 = mapRawProgress({
      runId: 'r-2',
      status: 'succeeded',
      errors: [],
      stages: [],
    });
    expect(res2.errors).toBe(0);

    // Case 3: Payload has a number -> preserve number
    const res3 = mapRawProgress({
      runId: 'r-3',
      status: 'failed',
      errors: 5,
      stages: [],
    });
    expect(res3.errors).toBe(5);

    // Case 4: Payload has null/undefined errors -> map to null
    const res4 = mapRawProgress({
      runId: 'r-4',
      status: 'pending',
      errors: null,
      stages: [],
    });
    expect(res4.errors).toBeNull();
  });

  it('maps spark, hdfs, throughput samples and counters', async () => {
    const { mapRawProgress, isRunProgress } = await import('../api/runProgress');

    const raw = {
      runId: 'run-full-1',
      status: 'succeeded',
      mode: 'batch',
      stages: [{ key: 'source', state: 'done' }],
      spark: {
        recordsIn: 1000000,
        recordsOut: 950000,
        tasksTotal: 10,
        tasksDone: 10,
      },
      hdfs: {
        filesWritten: 3,
        bytesWritten: 25000000,
        rowsWritten: 950000,
        verified: true,
      },
      throughput: {
        recordsPerSecond: 5500,
        samples: [{ t: 100, v: 5000 }, { t: 105, v: 6000 }],
      },
    };

    expect(isRunProgress(raw)).toBe(true);

    const mapped = mapRawProgress(raw);
    expect(mapped.spark.recordsIn).toBe(1000000);
    expect(mapped.spark.recordsOut).toBe(950000);
    expect(mapped.hdfs.filesWritten).toBe(3);
    expect(mapped.hdfs.bytesWritten).toBe(25000000);
    expect(mapped.hdfs.rowsWritten).toBe(950000);
    expect(mapped.hdfs.verified).toBe(true);
    expect(mapped.throughput.recordsPerSecond).toBe(5500);
    expect(mapped.throughput.samples).toHaveLength(2);
  });
});
