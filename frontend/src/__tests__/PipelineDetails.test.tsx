/* eslint-disable @typescript-eslint/no-explicit-any */
import React from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import { MemoryRouter, Routes, Route } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { axe } from 'vitest-axe';

import PipelineDetails from '../pages/PipelineDetails';
import * as pipelinesApi from '../api/pipelines';
import * as systemApi from '../api/system';
import * as runLiveHook from '../hooks/useRunLive';
import * as runEventsHook from '../hooks/useRunEvents';
import { RunProgress } from '../types/runProgress';
import { PipelineRunListItem } from '../types/pipeline';

// Mock child components that use heavy third-party libs if needed
vi.mock('../components/pipelines/YamlEditor', () => ({
  default: ({ value, readOnly }: { value: string; readOnly?: boolean }) => (
    <div data-testid="mock-yaml-editor" data-readonly={readOnly}>
      {value}
    </div>
  ),
}));

// Mock Recharts to avoid jsdom zero dimension issues
vi.mock('recharts', async () => {
  const actual = await vi.importActual<any>('recharts');
  return {
    ...actual,
    ResponsiveContainer: ({ children }: any) => (
      <div data-testid="mock-responsive-container" style={{ width: 500, height: 220 }}>
        {children}
      </div>
    ),
  };
});

const mockRunData: PipelineRunListItem = {
  id: 'run-test-123',
  name: 'Sales Aggregation Pipeline',
  pipelineId: 'p-1',
  pipelineVersionId: 'pv-1',
  status: 'running',
  startedAt: '2026-10-13T10:00:00Z',
  finishedAt: null,
  errorMessage: null,
  outputPath: '/data/sales_output',
  actualOutputPath: '/data/sales_output',
  yamlContent: `pipeline:\n  name: sales-pipeline\n  version: "1.0"\nsource:\n  type: csv\n  path: /app/sample-data/sales.csv\nprocessing:\n  operations:\n    - type: filter\nstorage:\n  type: hdfs\n  path: /data/sales_output\n`,
};

const mockRunningProgress: RunProgress = {
  runId: 'run-test-123',
  status: 'running',
  mode: 'batch',
  startedAt: '2026-10-13T10:00:00Z',
  updatedAt: '2026-10-13T10:00:15Z',
  completedAt: null,
  stages: [
    {
      key: 'source',
      state: 'done',
      startedAt: '2026-10-13T10:00:00Z',
      finishedAt: '2026-10-13T10:00:02Z',
      error: null,
    },
    {
      key: 'kafka',
      state: 'active',
      startedAt: '2026-10-13T10:00:02Z',
      finishedAt: null,
      error: null,
    },
    {
      key: 'spark',
      state: 'pending',
      startedAt: null,
      finishedAt: null,
      error: null,
    },
    {
      key: 'hdfs',
      state: 'pending',
      startedAt: null,
      finishedAt: null,
      error: null,
    },
  ],
  source: {
    type: 'csv',
    rowsTotal: 1000000,
    rowsRead: 1000000,
  },
  kafka: {
    topic: 'sales-topic',
    partitions: 3,
    published: 450000,
    failed: 12,
    endOffsets: 450012,
    consumerLag: null,
  },
  spark: {
    appId: 'app-20261013-0001',
    tasksTotal: null,
    tasksDone: null,
    recordsIn: null,
    recordsOut: null,
    durationMs: null,
    activeStage: null,
  },
  hdfs: {
    path: '/data/sales_output',
    filesWritten: null,
    bytesWritten: null,
    rowsWritten: null,
    verified: null,
  },
  throughput: {
    recordsPerSecond: 12500,
    samples: [
      { t: 1760350802, v: 10000 },
      { t: 1760350805, v: 12500 },
    ],
  },
  etaSeconds: 44,
  errors: 0,
  fault: null,
};

const mockSucceededProgress: RunProgress = {
  ...mockRunningProgress,
  status: 'succeeded',
  completedAt: '2026-10-13T10:01:15Z',
  stages: [
    { key: 'source', state: 'done', startedAt: '2026-10-13T10:00:00Z', finishedAt: '2026-10-13T10:00:02Z', error: null },
    { key: 'kafka', state: 'done', startedAt: '2026-10-13T10:00:02Z', finishedAt: '2026-10-13T10:00:30Z', error: null },
    { key: 'spark', state: 'done', startedAt: '2026-10-13T10:00:30Z', finishedAt: '2026-10-13T10:01:00Z', error: null },
    { key: 'hdfs', state: 'done', startedAt: '2026-10-13T10:01:00Z', finishedAt: '2026-10-13T10:01:15Z', error: null },
  ],
  kafka: {
    ...mockRunningProgress.kafka,
    published: 1000000,
    failed: 0,
  },
  spark: {
    appId: 'app-20261013-0001',
    tasksTotal: 8,
    tasksDone: 8,
    recordsIn: 1000000,
    recordsOut: 950000,
    durationMs: 30000,
    activeStage: null,
  },
  hdfs: {
    path: '/data/sales_output',
    filesWritten: 4,
    bytesWritten: 10485760,
    rowsWritten: 950000,
    verified: true,
  },
  etaSeconds: null,
};

const mockFailedProgress: RunProgress = {
  ...mockRunningProgress,
  status: 'failed',
  completedAt: '2026-10-13T10:00:35Z',
  stages: [
    { key: 'source', state: 'done', startedAt: '2026-10-13T10:00:00Z', finishedAt: '2026-10-13T10:00:02Z', error: null },
    { key: 'kafka', state: 'done', startedAt: '2026-10-13T10:00:02Z', finishedAt: '2026-10-13T10:00:25Z', error: null },
    { key: 'spark', state: 'error', startedAt: '2026-10-13T10:00:25Z', finishedAt: '2026-10-13T10:00:35Z', error: 'Spark executor lost: OOM error in worker container' },
    { key: 'hdfs', state: 'skipped', startedAt: null, finishedAt: null, error: null },
  ],
  etaSeconds: null,
  errors: 1,
};

function renderPage(initialEntries = ['/pipelines/run-test-123']) {
  const queryClient = new QueryClient({
    defaultOptions: {
      queries: {
        retry: false,
      },
    },
  });

  return render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter initialEntries={initialEntries}>
        <Routes>
          <Route path="/pipelines/:runId" element={<PipelineDetails />} />
          <Route path="/pipelines" element={<div data-testid="pipelines-page">Pipelines Page</div>} />
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>
  );
}

describe('PipelineDetails Live Operations View', () => {
  beforeEach(() => {
    vi.clearAllMocks();

    vi.spyOn(pipelinesApi, 'getPipelineRun').mockResolvedValue(mockRunData);
    vi.spyOn(systemApi, 'getSystemStatus').mockResolvedValue({
      status: 'ok',
      database: { connected: true, latency_ms: 5, error: null },
      kafka: { connected: true, broker_count: 1, error: null },
      spark: { connected: true, worker_count: 1, error: null },
      hdfs: { connected: false, error: 'Connection refused', num_live_datanodes: 0 },
    });

    vi.spyOn(runLiveHook, 'useRunLive').mockReturnValue({
      progress: mockRunningProgress,
      connection: 'live',
      lastUpdatedAt: Date.now() - 3000,
      error: null,
    });

    vi.spyOn(runEventsHook, 'useRunEvents').mockReturnValue({
      events: [
        { seq: 1, eventType: 'run_start', payload: {}, ts: '2026-10-13T10:00:00Z' },
        { seq: 2, eventType: 'source_done', payload: { rows_read: 1000000 }, ts: '2026-10-13T10:00:02Z' },
        { seq: 3, eventType: 'kafka_progress', payload: { published: 450000 }, ts: '2026-10-13T10:00:10Z' },
      ],
      isLoading: false,
    });
  });

  it('renders header with pipeline name, StatusPill, connection badge, and run ID', async () => {
    renderPage();

    expect(screen.getByTestId('run-header')).toBeInTheDocument();
    await waitFor(() => {
      expect(screen.getByText('Sales Aggregation Pipeline')).toBeInTheDocument();
    });
    expect(screen.getByTestId('connection-badge')).toHaveTextContent('Live');
    expect(screen.getByText('run-test-123')).toBeInTheDocument();

    // Cancel button is visible while running
    expect(screen.getByRole('button', { name: /cancel/i })).toBeInTheDocument();
  });

  it('renders stage stepper with 4 stages, ordered list, states and verified badge', async () => {
    renderPage();

    const stepper = screen.getByTestId('stage-stepper');
    expect(stepper.tagName.toLowerCase()).toBe('ol');

    const sourceStage = screen.getByTestId('stage-source');
    expect(sourceStage).toHaveAttribute('data-state', 'done');

    const kafkaStage = screen.getByTestId('stage-kafka');
    expect(kafkaStage).toHaveAttribute('data-state', 'active');

    const sparkStage = screen.getByTestId('stage-spark');
    expect(sparkStage).toHaveAttribute('data-state', 'pending');

    const hdfsStage = screen.getByTestId('stage-hdfs');
    expect(hdfsStage).toHaveAttribute('data-state', 'pending');
  });

  it('renders verified badge on HDFS stage when verified is true', async () => {
    vi.spyOn(runLiveHook, 'useRunLive').mockReturnValue({
      progress: mockSucceededProgress,
      connection: 'closed',
      lastUpdatedAt: Date.now(),
      error: null,
    });

    renderPage();

    expect(screen.getByTestId('stage-hdfs')).toHaveAttribute('data-state', 'done');
    expect(screen.getByTestId('hdfs-verified-badge')).toBeInTheDocument();
  });

  it('renders honest counters grid: null renders em dash and NEVER 0, numbers format correctly', async () => {
    renderPage();

    const counters = screen.getByTestId('counters');
    expect(counters).toBeInTheDocument();

    // Rows total: 1000000 -> 1,000,000
    expect(screen.getByTestId('counter-rows-total')).toHaveTextContent('1,000,000');

    // Published: 450000 -> 450,000 with failed: 12
    const kafkaTile = screen.getByTestId('counter-published');
    expect(kafkaTile).toHaveTextContent('450,000');
    expect(kafkaTile).toHaveTextContent('failed: 12');

    // Spark records in is null -> MUST render em dash '—' and NOT 0
    const sparkInTile = screen.getByTestId('counter-records-in');
    expect(sparkInTile).toHaveTextContent('—');
    expect(sparkInTile).not.toHaveTextContent('0');

    // Files written is null -> MUST render em dash '—'
    const filesTile = screen.getByTestId('counter-files-written');
    expect(filesTile).toHaveTextContent('—');

    // Rate: 12500 -> 12,500 rec/s
    expect(screen.getByTestId('counter-rate')).toHaveTextContent('12,500 rec/s');
  });

  it('renders all counter tiles with populated numbers on succeeded run and none show em dash', async () => {
    vi.spyOn(runLiveHook, 'useRunLive').mockReturnValue({
      progress: mockSucceededProgress,
      connection: 'closed',
      lastUpdatedAt: Date.now(),
      error: null,
    });

    renderPage();

    expect(screen.getByTestId('counter-rows-total')).toHaveTextContent('1,000,000');
    expect(screen.getByTestId('counter-rows-read')).toHaveTextContent('1,000,000');
    expect(screen.getByTestId('counter-published')).toHaveTextContent('1,000,000');
    expect(screen.getByTestId('counter-records-in')).toHaveTextContent('1,000,000');
    expect(screen.getByTestId('counter-records-out')).toHaveTextContent('950,000');
    expect(screen.getByTestId('counter-rows-written')).toHaveTextContent('950,000');
    expect(screen.getByTestId('counter-files-written')).toHaveTextContent('4');
    expect(screen.getByTestId('counter-bytes-written')).toHaveTextContent('10 MB');
    expect(screen.getByTestId('counter-errors')).toHaveTextContent('0');

    // Prove that none of these populated tiles show '—'
    const tileIds = [
      'counter-rows-total',
      'counter-rows-read',
      'counter-published',
      'counter-records-in',
      'counter-records-out',
      'counter-rows-written',
      'counter-files-written',
      'counter-bytes-written',
      'counter-errors',
    ];
    for (const id of tileIds) {
      expect(screen.getByTestId(id)).not.toHaveTextContent('—');
    }
  });

  it('renders active stage progress with determinate progress bar, tasks and ETA', async () => {
    renderPage();

    const activeProgress = screen.getByTestId('active-progress');
    expect(activeProgress).toBeInTheDocument();
    expect(activeProgress).toHaveTextContent('Kafka: publishing records');
    expect(activeProgress).toHaveTextContent('450,000 / 1,000,000 records');
    expect(activeProgress).toHaveTextContent('Estimated time remaining: ~44 s (estimate)');

    const progressBar = activeProgress.querySelector('[role="progressbar"]');
    expect(progressBar).toBeInTheDocument();
    expect(progressBar).toHaveAttribute('aria-valuenow', '45');
  });

  it('renders failure panel when status is failed, showing failing stage and error message', async () => {
    vi.spyOn(runLiveHook, 'useRunLive').mockReturnValue({
      progress: mockFailedProgress,
      connection: 'closed',
      lastUpdatedAt: Date.now(),
      error: null,
    });
    vi.spyOn(runEventsHook, 'useRunEvents').mockReturnValue({
      events: [
        { seq: 1, eventType: 'run_failed', payload: { error: 'OOM error in worker' }, ts: '2026-10-13T10:00:35Z' },
      ],
      isLoading: false,
    });

    renderPage();

    const failurePanel = screen.getByTestId('failure-panel');
    expect(failurePanel).toBeInTheDocument();
    expect(failurePanel).toHaveTextContent('Failed at Spark');
    expect(failurePanel).toHaveTextContent('Spark executor lost: OOM error in worker container');

    // Failing stage in stepper is highlighted with error state
    expect(screen.getByTestId('stage-spark')).toHaveAttribute('data-state', 'error');
  });

  it('renders success summary when status is succeeded with view output button', async () => {
    vi.spyOn(runLiveHook, 'useRunLive').mockReturnValue({
      progress: mockSucceededProgress,
      connection: 'closed',
      lastUpdatedAt: Date.now(),
      error: null,
    });

    renderPage();

    const successSummary = screen.getByTestId('success-summary');
    expect(successSummary).toBeInTheDocument();
    expect(successSummary).toHaveTextContent('Pipeline Run Succeeded');
    expect(successSummary).toHaveTextContent('950,000'); // rows written
    expect(screen.getByRole('button', { name: /view output/i })).toBeInTheDocument();
  });

  it('renders services panel with boolean connected status pills and Spark UI link', async () => {
    renderPage();

    const servicesPanel = screen.getByTestId('services-panel');
    expect(servicesPanel).toBeInTheDocument();

    // Spark Master is connected: true -> Connected
    const sparkService = screen.getByTestId('service-spark');
    await waitFor(() => {
      expect(sparkService).toHaveTextContent('Connected');
    });

    // HDFS is connected: false -> must show Unavailable
    const hdfsService = screen.getByTestId('service-hdfs');
    expect(hdfsService).toHaveTextContent('Unavailable');

    // Spark UI Link rendered for app id
    const sparkLink = screen.getByRole('link', { name: /open in spark ui/i });
    expect(sparkLink).toBeInTheDocument();
    expect(sparkLink).toHaveAttribute('href', expect.stringContaining('app-20261013-0001'));
    expect(sparkLink).toHaveAttribute('rel', 'noopener noreferrer');
  });

  it('synchronizes tabs with query string and supports navigation', async () => {
    // Deep link directly to ?tab=logs
    renderPage(['/pipelines/run-test-123?tab=logs']);

    expect(screen.getByTestId('logs-tab')).toBeInTheDocument();

    // Deep link directly to ?tab=configuration
    renderPage(['/pipelines/run-test-123?tab=configuration']);
    expect(screen.getByTestId('config-tab')).toBeInTheDocument();
  });

  it('Configuration tab displays parsed summary and CodeMirror editor', async () => {
    renderPage(['/pipelines/run-test-123?tab=configuration']);

    const configTab = screen.getByTestId('config-tab');
    expect(configTab).toBeInTheDocument();
    await waitFor(() => {
      expect(configTab).toHaveTextContent('sales-pipeline');
      expect(screen.getByTestId('mock-yaml-editor')).toBeInTheDocument();
    });
    expect(configTab).toHaveTextContent('/data/sales_output');
  });

  it('Output tab renders not-ready state while pipeline is running', async () => {
    renderPage(['/pipelines/run-test-123?tab=output']);

    const outputTab = screen.getByTestId('output-tab');
    expect(outputTab).toBeInTheDocument();
    expect(screen.getByTestId('output-not-ready')).toBeInTheDocument();
    expect(outputTab).toHaveTextContent('Pipeline is still running');
  });

  it('re-render isolation: updating lastUpdatedAt does not re-render heavy tabs', async () => {
    let currentLastUpdated = 1000;
    const liveHookSpy = vi.spyOn(runLiveHook, 'useRunLive').mockImplementation(() => ({
      progress: mockRunningProgress,
      connection: 'live',
      lastUpdatedAt: currentLastUpdated,
      error: null,
    }));

    const { rerender } = renderPage();
    expect(screen.getByTestId('connection-badge')).toBeInTheDocument();

    // Trigger update where only lastUpdatedAt changes
    currentLastUpdated = 2000;
    liveHookSpy.mockReturnValue({
      progress: mockRunningProgress,
      connection: 'live',
      lastUpdatedAt: currentLastUpdated,
      error: null,
    });

    rerender(
      <QueryClientProvider client={new QueryClient()}>
        <MemoryRouter initialEntries={['/pipelines/run-test-123']}>
          <Routes>
            <Route path="/pipelines/:runId" element={<PipelineDetails />} />
          </Routes>
        </MemoryRouter>
      </QueryClientProvider>
    );

    // Component renders stably
    expect(screen.getByTestId('connection-badge')).toBeInTheDocument();
  });

  it('renders streaming badge and consumer lag counter when mode is streaming', async () => {
    const streamingProgress: RunProgress = {
      ...mockRunningProgress,
      mode: 'streaming',
      kafka: {
        ...mockRunningProgress.kafka,
        consumerLag: 42,
      },
    };
    vi.spyOn(runLiveHook, 'useRunLive').mockReturnValue({
      progress: streamingProgress,
      connection: 'live',
      lastUpdatedAt: 1000,
      error: null,
    });

    renderPage();
    expect(screen.getByTestId('streaming-badge')).toHaveTextContent('STREAMING');
    const lagTile = screen.getByTestId('counter-lag');
    expect(lagTile).toBeInTheDocument();
    expect(lagTile).toHaveTextContent('42');
  });

  it('has zero accessibility violations (axe-core)', async () => {
    const { container } = renderPage();
    const results = await axe(container, {
      rules: { 'color-contrast': { enabled: false } },
    });
    expect(results).toHaveNoViolations();
  });
});
