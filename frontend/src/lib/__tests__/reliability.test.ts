import { describe, it, expect } from 'vitest';
import yaml from 'yaml';
import {
  buildExperimentYaml,
  describeExperiment,
  serverClockOffset,
  phaseElapsedSeconds,
  faultCountdown,
  criterionLabels,
  availabilityStepData,
  throughputBars,
  recoveryTrend,
  resourceSeries,
  experimentToProgress,
  ExperimentFormState,
} from '../reliability';
import { ExperimentDetail, ExperimentOptions } from '../../types/experiment';
import { ReportListItem } from '../../types/report';

describe('Reliability Pure Helpers', () => {
  const mockOptions: ExperimentOptions = {
    targets: [
      { id: 'kafka', label: 'Kafka Broker', description: 'Message bus', risk: 'high', service: 'kafka' },
      { id: 'spark-master', label: 'Spark Master', description: 'Compute manager', risk: 'medium', service: 'spark-master' },
    ],
    faults: [
      { id: 'container_stop', label: 'Stop Container', description: 'Stops service', blastRadius: 'Cluster degraded', params: [], icon: 'square' },
      { id: 'service_delay', label: 'Service Delay', description: 'Adds network latency', blastRadius: 'RPC timeouts', params: [{ name: 'delay_ms', type: 'int' }], icon: 'clock' },
    ],
    limits: { minDurationSeconds: 1, maxDurationSeconds: 300 },
  };

  const sampleForm: ExperimentFormState = {
    name: 'test-exp-01',
    targetId: 'kafka',
    faultId: 'container_stop',
    duration: 25,
    params: {},
    expectedDataLoss: 0,
    maxRecoverySeconds: 30,
  };

  describe('buildExperimentYaml', () => {
    it('generates valid parseable YAML matching the specification', () => {
      const y = buildExperimentYaml(sampleForm);
      const parsed = yaml.parse(y);

      expect(parsed.experiment.name).toBe('test-exp-01');
      expect(parsed.target.service).toBe('kafka');
      expect(parsed.fault.type).toBe('container_stop');
      expect(parsed.fault.duration).toBe(25);
      expect(parsed.validation.expected_data_loss).toBe(0);
      expect(parsed.validation.max_recovery_time).toBe(30);
    });

    it('includes fault params like delay_ms when provided', () => {
      const y = buildExperimentYaml({
        ...sampleForm,
        faultId: 'service_delay',
        params: { delay_ms: 500 },
      });
      const parsed = yaml.parse(y);
      expect(parsed.fault.delay_ms).toBe(500);
    });
  });

  describe('describeExperiment', () => {
    it('generates human-readable review sentence for container_stop', () => {
      const sentence = describeExperiment(sampleForm, mockOptions);
      expect(sentence).toContain('Kafka Broker will be stopped for 25 seconds');
      expect(sentence).toContain('expected to lose 0 records');
      expect(sentence).toContain('recover within 30 seconds');
    });

    it('generates review sentence for service_delay', () => {
      const sentence = describeExperiment(
        { ...sampleForm, faultId: 'service_delay', params: { delay_ms: 300 } },
        mockOptions
      );
      expect(sentence).toContain('Kafka Broker will have 300 ms latency injected for 25 seconds');
    });
  });

  describe('serverClockOffset', () => {
    it('returns 0 when serverTime is missing', () => {
      expect(serverClockOffset(null)).toBe(0);
      expect(serverClockOffset(undefined)).toBe(0);
    });

    it('computes positive offset when server clock is ahead', () => {
      const clientNow = 100000;
      const serverIso = new Date(105000).toISOString();
      expect(serverClockOffset(serverIso, clientNow)).toBe(5000);
    });
  });

  describe('phaseElapsedSeconds', () => {
    const detail: ExperimentDetail = {
      id: 'exp-1',
      name: 'test',
      targetService: 'kafka',
      faultType: 'container_stop',
      faultParams: {},
      durationSeconds: 20,
      thresholds: { expectedDataLoss: 0, maxRecoverySeconds: 30 },
      status: 'running',
      currentPhase: 'injecting',
      phases: [
        { name: 'validating', state: 'done', startedAt: '2026-01-01T00:00:00Z', finishedAt: '2026-01-01T00:00:05Z' },
        { name: 'injecting', state: 'active', startedAt: '2026-01-01T00:00:05Z', finishedAt: null },
      ],
      runId: 'r1',
      runStartedAt: '2026-01-01T00:00:00Z',
      runFinishedAt: null,
      faultInjectedAt: '2026-01-01T00:00:05Z',
      faultRevertedAt: null,
      recoveredAt: null,
      faultPhase: 'injected',
      serverTime: '2026-01-01T00:00:15Z',
      pipelineRunId: null,
      result: null,
      failedCriteria: [],
      results: {
        recoveryTimeSeconds: null,
        dataLoss: 0,
        duplicates: 0,
        expectedRecords: 0,
        processedRecords: 0,
        throughputBaseline: null,
        throughputDuringFault: null,
        throughputAfter: null,
        throughputDegradationPercent: null,
        availabilityPercentage: null,
        errorRate: null,
        errorCount: null,
        totalOperations: null,
        availabilitySamples: null,
        throughputSamples: null,
        cpuSamples: null,
        memorySamples: null,
      },
      lifecycleStage: 'injecting',
      createdAt: '2026-01-01T00:00:00Z',
      logs: [],
    };

    it('calculates elapsed time for active phase based on nowAligned', () => {
      const now = new Date('2026-01-01T00:00:15Z').getTime();
      expect(phaseElapsedSeconds(detail, now)).toBe(10);
    });

    it('calculates exact duration for completed phase', () => {
      const finishedDetail: ExperimentDetail = {
        ...detail,
        currentPhase: 'validating',
      };
      expect(phaseElapsedSeconds(finishedDetail)).toBe(5);
    });
  });

  describe('faultCountdown', () => {
    it('returns active countdown while faultPhase is injected', () => {
      const detail = {
        faultPhase: 'injected',
        faultInjectedAt: '2026-01-01T00:00:00Z',
        durationSeconds: 20,
      } as ExperimentDetail;

      const now = new Date('2026-01-01T00:00:08Z').getTime();
      const res = faultCountdown(detail, now);

      expect(res.active).toBe(true);
      expect(res.total).toBe(20);
      expect(res.elapsed).toBe(8);
    });

    it('returns inactive countdown when recovering or not injected', () => {
      const detail = {
        faultPhase: 'recovering',
        faultInjectedAt: '2026-01-01T00:00:00Z',
        durationSeconds: 20,
      } as ExperimentDetail;

      const res = faultCountdown(detail);
      expect(res.active).toBe(false);
      expect(res.elapsed).toBe(0);
    });
  });

  describe('criterionLabels', () => {
    it('correctly evaluates passing criteria', () => {
      const detail = {
        status: 'succeeded',
        result: 'PASS',
        thresholds: { expectedDataLoss: 0, maxRecoverySeconds: 30 },
        results: { dataLoss: 0, recoveryTimeSeconds: 12.4 },
      } as unknown as ExperimentDetail;

      const results = criterionLabels(detail);
      expect(results[0].passed).toBe(true); // Recovery time
      expect(results[1].passed).toBe(true); // Data loss
      expect(results[2].passed).toBe(true); // Execution errors
    });

    it('flags failing criteria when thresholds are exceeded', () => {
      const detail = {
        status: 'failed',
        result: 'FAIL',
        thresholds: { expectedDataLoss: 0, maxRecoverySeconds: 30 },
        results: { dataLoss: 15, recoveryTimeSeconds: 45.2 },
      } as unknown as ExperimentDetail;

      const results = criterionLabels(detail);
      expect(results[0].passed).toBe(false);
      expect(results[1].passed).toBe(false);
      expect(results[2].passed).toBe(false);
    });
  });

  describe('Chart data builders', () => {
    it('availabilityStepData returns null when samples are missing', () => {
      expect(availabilityStepData(null)).toBeNull();
      expect(availabilityStepData([])).toBeNull();
    });

    it('availabilityStepData maps samples and highlights fault window', () => {
      const samples = [
        { t: '2026-01-01T00:00:00Z', available: true },
        { t: '2026-01-01T00:00:10Z', available: false },
        { t: '2026-01-01T00:00:20Z', available: true },
      ];
      const data = availabilityStepData(samples, {
        start: '2026-01-01T00:00:05Z',
        end: '2026-01-01T00:00:15Z',
      });
      expect(data).toHaveLength(3);
      expect(data![0].inFaultWindow).toBe(false);
      expect(data![1].inFaultWindow).toBe(true);
      expect(data![2].inFaultWindow).toBe(false);
    });

    it('throughputBars returns null when all throughput stages are missing', () => {
      expect(throughputBars(null)).toBeNull();
      expect(
        throughputBars({
          throughputBaseline: null,
          throughputDuringFault: null,
          throughputAfter: null,
        } as any)
      ).toBeNull();
    });

    it('recoveryTrend formats reports and handles nulls', () => {
      expect(recoveryTrend(null)).toBeNull();
      expect(recoveryTrend([])).toBeNull();

      const reports: ReportListItem[] = [
        { id: 'r1', experimentId: 'e1', title: 'R1', targetService: 'k', faultType: 's', result: 'PASS', createdAt: '2026-01-01T00:00:00Z', recoveryTimeSeconds: 15.2, dataLoss: 0, throughputBaseline: 100, throughputDuringFault: 0, availabilityPercentage: 90, errorRate: 0, cpuAvgPercent: null, memoryAvgBytes: null },
      ];
      const trend = recoveryTrend(reports);
      expect(trend).toHaveLength(1);
      expect(trend![0].recoveryTime).toBe(15.2);
    });

    it('resourceSeries maps CPU/Memory samples', () => {
      expect(resourceSeries(null)).toBeNull();
      const samples = [{ t: '2026-01-01T00:00:00Z', service: 'kafka', percent: 45.2 }];
      const res = resourceSeries(samples);
      expect(res).toHaveLength(1);
      expect(res![0].percent).toBe(45.2);
    });
  });

  describe('experimentToProgress', () => {
    it('creates truthful RunProgress snapshot for compact pipeline visualization', () => {
      const detail: ExperimentDetail = {
        id: 'exp-viz',
        name: 'test-viz',
        targetService: 'kafka',
        faultType: 'container_stop',
        faultParams: {},
        durationSeconds: 20,
        thresholds: { expectedDataLoss: 0, maxRecoverySeconds: 30 },
        status: 'running',
        currentPhase: 'injecting',
        phases: [],
        runId: 'r1',
        runStartedAt: '2026-01-01T00:00:00Z',
        runFinishedAt: null,
        faultInjectedAt: '2026-01-01T00:00:05Z',
        faultRevertedAt: null,
        recoveredAt: null,
        faultPhase: 'injected',
        serverTime: '2026-01-01T00:00:10Z',
        pipelineRunId: null,
        result: null,
        failedCriteria: [],
        results: {
          recoveryTimeSeconds: 12.0,
          dataLoss: 0,
          duplicates: 0,
          expectedRecords: 0,
          processedRecords: 0,
          throughputBaseline: null,
          throughputDuringFault: null,
          throughputAfter: null,
          throughputDegradationPercent: null,
          availabilityPercentage: null,
          errorRate: null,
          errorCount: null,
          totalOperations: null,
          availabilitySamples: null,
          throughputSamples: null,
          cpuSamples: null,
          memorySamples: null,
        },
        lifecycleStage: 'injecting',
        createdAt: '2026-01-01T00:00:00Z',
        logs: [],
      };

      const prog = experimentToProgress(detail);
      expect(prog).not.toBeNull();
      expect(prog.status).toBe('running');
      expect(prog.fault).not.toBeNull();
      expect(prog.fault.experimentId).toBe('exp-viz');
      expect(prog.fault.target).toBe('kafka');
      expect(prog.fault.phase).toBe('injected');
    });
  });
});
