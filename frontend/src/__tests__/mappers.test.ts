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
