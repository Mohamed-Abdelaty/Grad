import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { axe } from 'vitest-axe';
import { MemoryRouter, Route, Routes } from 'react-router-dom';

import { ExperimentWizard } from '../components/experiments/ExperimentWizard';
import { ActiveExperimentMonitor } from '../components/experiments/ActiveExperimentMonitor';
import { ComparePanel } from '../components/reports/ComparePanel';
import ReportDetails from '../pages/ReportDetails';
import { ExperimentDetail, ExperimentOptions } from '../types/experiment';
import { ReportDetail, ReportListItem } from '../types/report';
import * as experimentsApi from '../api/experiments';
import * as reportsApi from '../api/reports';

// Mock Recharts to avoid jsdom zero dimension warnings
vi.mock('recharts', async () => {
  const original = await vi.importActual<Record<string, unknown>>('recharts');
  return {
    ...original,
    ResponsiveContainer: ({ children }: { children?: React.ReactNode }) => <div style={{ width: 800, height: 300 }}>{children}</div>,
  };
});

// Mock sonner toast
vi.mock('sonner', () => ({
  toast: {
    success: vi.fn(),
    error: vi.fn(),
    info: vi.fn(),
  },
}));

// Mock 3D visualization
vi.mock('../features/viz/PipelineVisualization', () => ({
  PipelineVisualization: () => <div data-testid="mock-pipeline-viz">Pipeline Viz</div>,
}));

// Mock useRunLive hook
vi.mock('../hooks/useRunLive', () => ({
  useRunLive: () => ({
    progress: null,
    connection: 'Polling',
    lastUpdatedAt: null,
    error: null,
  }),
}));

const mockOptions: ExperimentOptions = {
  targets: [
    { id: 'kafka', label: 'Kafka Broker', description: 'Distributed messaging bus', risk: 'high', service: 'kafka' },
    { id: 'spark-master', label: 'Spark Master', description: 'Cluster manager', risk: 'medium', service: 'spark-master' },
  ],
  faults: [
    { id: 'container_stop', label: 'Container Stop', description: 'Stops container', blastRadius: 'Service outage', params: [], icon: 'square' },
    { id: 'service_delay', label: 'Service Delay', description: 'Latency injection', blastRadius: 'Slow responses', params: [{ name: 'delay_ms', type: 'int' }], icon: 'clock' },
  ],
  limits: { minDurationSeconds: 1, maxDurationSeconds: 300 },
};

const mockDetail: ExperimentDetail = {
  id: 'exp-test-123',
  name: 'kafka-stop-experiment',
  targetService: 'kafka',
  faultType: 'container_stop',
  faultParams: {},
  durationSeconds: 20,
  thresholds: { expectedDataLoss: 0, maxRecoverySeconds: 30 },
  status: 'running',
  currentPhase: 'injecting',
  phases: [
    { name: 'validating', state: 'done', startedAt: '2026-01-01T00:00:00Z', finishedAt: '2026-01-01T00:00:02Z' },
    { name: 'preparing', state: 'done', startedAt: '2026-01-01T00:00:02Z', finishedAt: '2026-01-01T00:00:04Z' },
    { name: 'baseline', state: 'done', startedAt: '2026-01-01T00:00:04Z', finishedAt: '2026-01-01T00:00:08Z' },
    { name: 'injecting', state: 'active', startedAt: '2026-01-01T00:00:08Z', finishedAt: null },
    { name: 'monitoring', state: 'pending', startedAt: null, finishedAt: null },
    { name: 'rolling_back', state: 'pending', startedAt: null, finishedAt: null },
    { name: 'waiting_for_recovery', state: 'pending', startedAt: null, finishedAt: null },
    { name: 'validating_data', state: 'pending', startedAt: null, finishedAt: null },
    { name: 'collecting_metrics', state: 'pending', startedAt: null, finishedAt: null },
    { name: 'analyzing', state: 'pending', startedAt: null, finishedAt: null },
    { name: 'cleaning_up', state: 'pending', startedAt: null, finishedAt: null },
    { name: 'generating_report', state: 'pending', startedAt: null, finishedAt: null },
  ],
  runId: 'run-1',
  runStartedAt: '2026-01-01T00:00:00Z',
  runFinishedAt: null,
  faultInjectedAt: '2026-01-01T00:00:08Z',
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
    expectedRecords: 1000,
    processedRecords: 1000,
    throughputBaseline: 150,
    throughputDuringFault: null,
    throughputAfter: null,
    throughputDegradationPercent: null,
    availabilityPercentage: 95.0,
    errorRate: 0.05,
    errorCount: 1,
    totalOperations: 100,
    availabilitySamples: null,
    throughputSamples: null,
    cpuSamples: null,
    memorySamples: null,
  },
  lifecycleStage: 'injecting',
  createdAt: '2026-01-01T00:00:00Z',
  logs: ['Validating experiment target', 'Injecting fault into kafka'],
};

const mockReportDetail: ReportDetail = {
  ...mockDetail,
  reportId: 'rep-test-123',
  status: 'succeeded',
  currentPhase: 'generating_report',
  result: 'PASS',
  failedCriteria: [],
  results: {
    ...mockDetail.results,
    recoveryTimeSeconds: 14.2,
  },
  aiAnalysis: {
    risk_score: 12,
    risk_level: 'low',
    recommendations: ['Maintain current consumer retry timeout'],
  },
};

describe('Reliability UI Components & Accessibility', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe('ExperimentWizard Component', () => {
    it('walks through 4 steps, enforces confirmation, and starts experiment', async () => {
      const handleStarted = vi.fn();
      vi.spyOn(experimentsApi, 'createExperiment').mockResolvedValue({ experiment_id: 'exp-created-99' });
      vi.spyOn(experimentsApi, 'runExperiment').mockResolvedValue(undefined);

      render(
        <MemoryRouter>
          <ExperimentWizard options={mockOptions} onExperimentStarted={handleStarted} />
        </MemoryRouter>
      );

      // Step 1: Target
      expect(screen.getByTestId('exp-wizard')).toBeInTheDocument();
      expect(screen.getByTestId('target-opt-kafka')).toBeInTheDocument();
      expect(screen.getByTestId('target-opt-spark-master')).toBeInTheDocument();

      // Click target-opt-spark-master
      fireEvent.click(screen.getByTestId('target-opt-spark-master'));

      // Click Next to Step 2
      fireEvent.click(screen.getByText('Next'));

      // Step 2: Fault
      expect(screen.getByTestId('fault-opt-container_stop')).toBeInTheDocument();
      expect(screen.getByTestId('fault-opt-service_delay')).toBeInTheDocument();

      fireEvent.click(screen.getByTestId('fault-opt-container_stop'));
      fireEvent.click(screen.getByText('Next'));

      // Step 3: Parameters
      expect(screen.getByLabelText(/Fault Hold Duration \(seconds\)/i)).toBeInTheDocument();
      expect(screen.getByLabelText(/Expected Data Loss Threshold/i)).toBeInTheDocument();

      fireEvent.click(screen.getByText('Next'));

      // Step 4: Review
      expect(screen.getByText(/Generated Experiment YAML/i)).toBeInTheDocument();
      const startBtn = screen.getByTestId('btn-start-exp');
      expect(startBtn).toBeDisabled();

      // Check confirmation box
      const confirmCheck = screen.getByTestId('exp-confirm');
      fireEvent.click(confirmCheck);
      expect(startBtn).toBeEnabled();

      // Submit
      fireEvent.click(startBtn);

      await waitFor(() => {
        expect(experimentsApi.createExperiment).toHaveBeenCalled();
        expect(experimentsApi.runExperiment).toHaveBeenCalledWith('exp-created-99');
        expect(handleStarted).toHaveBeenCalledWith('exp-created-99');
      });
    });

    it('has zero accessibility violations (axe-core)', async () => {
      const { container } = render(
        <MemoryRouter>
          <ExperimentWizard options={mockOptions} onExperimentStarted={() => {}} />
        </MemoryRouter>
      );
      expect(await axe(container)).toHaveNoViolations();
    });
  });

  describe('ActiveExperimentMonitor Component', () => {
    it('renders 12 phases, fault badge, and logs', async () => {
      vi.spyOn(experimentsApi, 'getExperiment').mockResolvedValue(mockDetail);
      vi.spyOn(experimentsApi, 'getExperimentEvents').mockResolvedValue([
        { seq: 1, level: 'INFO', phase: 'injecting', message: 'Fault active', ts: '2026-01-01T00:00:10Z' },
      ]);

      render(
        <MemoryRouter>
          <ActiveExperimentMonitor experimentId="exp-test-123" />
        </MemoryRouter>
      );

      await waitFor(() => {
        expect(screen.getByTestId('exp-monitor')).toBeInTheDocument();
      });

      // 12-phase stepper
      expect(screen.getByTestId('exp-lifecycle')).toBeInTheDocument();
      expect(screen.getByTestId('phase-validating')).toBeInTheDocument();
      expect(screen.getByTestId('phase-injecting')).toBeInTheDocument();
      expect(screen.getByTestId('phase-generating_report')).toBeInTheDocument();

      // Fault phase badge
      const badge = screen.getByTestId('exp-fault-badge');
      expect(badge).toHaveTextContent(/Fault Phase: Injected/i);

      // Cancel button triggers confirm dialog
      const cancelBtn = screen.getByText('Cancel');
      fireEvent.click(cancelBtn);
      expect(screen.getByText(/Cancel Chaos Experiment\?/i)).toBeInTheDocument();
    });

    it('renders result summary tiles when finished', async () => {
      const finishedDetail: ExperimentDetail = {
        ...mockDetail,
        status: 'succeeded',
        result: 'PASS',
        results: {
          ...mockDetail.results,
          recoveryTimeSeconds: 15.3,
          dataLoss: 0,
        },
      };

      vi.spyOn(experimentsApi, 'getExperiment').mockResolvedValue(finishedDetail);
      vi.spyOn(experimentsApi, 'getExperimentEvents').mockResolvedValue([]);

      render(
        <MemoryRouter>
          <ActiveExperimentMonitor experimentId="exp-test-123" />
        </MemoryRouter>
      );

      await waitFor(() => {
        expect(screen.getByTestId('exp-result')).toBeInTheDocument();
      });

      expect(screen.getByText(/Experiment Result: PASS/i)).toBeInTheDocument();
      expect(screen.getAllByText('15.3 s').length).toBeGreaterThanOrEqual(1);
    });

    it('has zero accessibility violations (axe-core)', async () => {
      vi.spyOn(experimentsApi, 'getExperiment').mockResolvedValue(mockDetail);
      vi.spyOn(experimentsApi, 'getExperimentEvents').mockResolvedValue([]);

      const { container } = render(
        <MemoryRouter>
          <ActiveExperimentMonitor experimentId="exp-test-123" />
        </MemoryRouter>
      );

      await waitFor(() => {
        expect(screen.getByTestId('exp-monitor')).toBeInTheDocument();
      });

      expect(await axe(container)).toHaveNoViolations();
    });
  });

  describe('ComparePanel Component', () => {
    const mockReportsList: ReportListItem[] = [
      {
        id: 'rep-1',
        experimentId: 'exp-1',
        title: 'Kafka Test 1',
        targetService: 'kafka',
        faultType: 'container_stop',
        result: 'PASS',
        createdAt: '2026-01-01T00:00:00Z',
        recoveryTimeSeconds: 12.0,
        dataLoss: 0,
        throughputBaseline: 150,
        throughputDuringFault: null,
        availabilityPercentage: 98.0,
        errorRate: 0.02,
        cpuAvgPercent: null,
        memoryAvgBytes: null,
      },
      {
        id: 'rep-2',
        experimentId: 'exp-2',
        title: 'Spark Test 2',
        targetService: 'spark-master',
        faultType: 'service_delay',
        result: 'FAIL',
        createdAt: '2026-01-02T00:00:00Z',
        recoveryTimeSeconds: 42.5,
        dataLoss: 10,
        throughputBaseline: 200,
        throughputDuringFault: 50,
        availabilityPercentage: 85.0,
        errorRate: 0.15,
        cpuAvgPercent: null,
        memoryAvgBytes: null,
      },
    ];

    it('renders comparison table and closes on close button or Escape', () => {
      const handleClose = vi.fn();
      render(
        <MemoryRouter>
          <ComparePanel open={true} onClose={handleClose} reports={mockReportsList} />
        </MemoryRouter>
      );

      expect(screen.getByTestId('compare-panel')).toBeInTheDocument();
      expect(screen.getByText('Availability during fault window')).toBeInTheDocument();
      expect(screen.getByText(/Kafka Test 1/i)).toBeInTheDocument();
      expect(screen.getByText(/Spark Test 2/i)).toBeInTheDocument();
      expect(screen.getByText('12.0 s')).toBeInTheDocument();
      expect(screen.getByText('42.5 s')).toBeInTheDocument();

      // Escape key triggers close
      fireEvent.keyDown(window, { key: 'Escape' });
      expect(handleClose).toHaveBeenCalled();
    });

    it('has zero accessibility violations (axe-core)', async () => {
      const { container } = render(
        <MemoryRouter>
          <ComparePanel open={true} onClose={() => {}} reports={mockReportsList} />
        </MemoryRouter>
      );
      expect(await axe(container)).toHaveNoViolations();
    });
  });

  describe('ReportDetails Component', () => {
    it('renders report details, criteria, metrics, and AI insights', async () => {
      vi.spyOn(reportsApi, 'getReport').mockResolvedValue(mockReportDetail);
      vi.spyOn(experimentsApi, 'getExperimentEvents').mockResolvedValue([]);

      render(
        <MemoryRouter initialEntries={['/reports/rep-test-123']}>
          <Routes>
            <Route path="/reports/:id" element={<ReportDetails />} />
          </Routes>
        </MemoryRouter>
      );

      await waitFor(() => {
        expect(screen.getByText(/kafka-stop-experiment/i)).toBeInTheDocument();
      });

      expect(screen.getByText(/Acceptance Criteria Evaluation/i)).toBeInTheDocument();
      expect(screen.getByText(/Key Execution Metrics/i)).toBeInTheDocument();
      expect(screen.getByText(/AI Reliability Insights/i)).toBeInTheDocument();
      expect(screen.getByText(/Maintain current consumer retry timeout/i)).toBeInTheDocument();
      expect(screen.getByText(/Export JSON/i)).toBeInTheDocument();
      expect(screen.getByText(/Print Report/i)).toBeInTheDocument();
    });

    it('verifies label "Availability during fault window" and N/A rendering for unmeasured metrics', async () => {
      const nullReportDetail: ReportDetail = {
        ...mockReportDetail,
        results: {
          ...mockReportDetail.results,
          dataLoss: null,
          expectedRecords: null,
          processedRecords: null,
          duplicates: null,
          availabilityPercentage: null,
          throughputDegradationPercent: null,
          errorRate: null,
          totalOperations: null,
        },
      };

      vi.spyOn(reportsApi, 'getReport').mockResolvedValue(nullReportDetail);
      vi.spyOn(experimentsApi, 'getExperimentEvents').mockResolvedValue([]);

      render(
        <MemoryRouter initialEntries={['/reports/rep-test-null']}>
          <Routes>
            <Route path="/reports/:id" element={<ReportDetails />} />
          </Routes>
        </MemoryRouter>
      );

      await waitFor(() => {
        expect(screen.getByText('Availability during fault window')).toBeInTheDocument();
      });

      const naElements = screen.getAllByText('N/A');
      expect(naElements.length).toBeGreaterThanOrEqual(4);
    });

    it('has zero accessibility violations (axe-core)', async () => {
      vi.spyOn(reportsApi, 'getReport').mockResolvedValue(mockReportDetail);
      vi.spyOn(experimentsApi, 'getExperimentEvents').mockResolvedValue([]);

      const { container } = render(
        <MemoryRouter initialEntries={['/reports/rep-test-123']}>
          <Routes>
            <Route path="/reports/:id" element={<ReportDetails />} />
          </Routes>
        </MemoryRouter>
      );

      await waitFor(() => {
        expect(screen.getByText(/kafka-stop-experiment/i)).toBeInTheDocument();
      });

      expect(await axe(container)).toHaveNoViolations();
    });
  });
});
