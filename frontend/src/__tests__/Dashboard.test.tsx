import React from 'react';
import { render, screen } from '@testing-library/react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { MemoryRouter } from 'react-router-dom';
import { axe } from 'vitest-axe';
import Dashboard from '../pages/Dashboard';
import * as system from '../api/system';
import * as pipelines from '../api/pipelines';
import * as experiments from '../api/experiments';
import * as dashboard from '../api/dashboard';

vi.mock('../api/system');
vi.mock('../api/pipelines');
vi.mock('../api/experiments');
vi.mock('../api/dashboard');

const ok = { connected: true, error: null, latency_ms: 3 };
const down = { connected: false, error: 'refused', latency_ms: null };

const defaultSummary = {
  windowHours: 24,
  runs: { total: 0, active: 0, succeeded: 0, failed: 0, cancelled: 0 },
  experiments: { total: 0, running: 0, succeeded: 0, failed: 0, cancelled: 0 },
  kpis: {
    avgRecoverySeconds: null,
    totalDataLoss: 0,
    avgThroughput: null,
    avgAvailabilityPercent: null,
    completedExperimentsConsidered: 0,
  },
  runsPerHour: [],
  recoveryTrend: [],
  throughputTrend: [],
  recentFailures: [],
};

function renderDashboard() {
  const client = new QueryClient({
    defaultOptions: {
      queries: {
        retry: false,
        cacheTime: 0,
      },
    },
  });
  return render(
    <QueryClientProvider client={client}>
      <MemoryRouter>
        <Dashboard />
      </MemoryRouter>
    </QueryClientProvider>
  );
}

describe('Dashboard states', () => {
  beforeEach(() => {
    vi.resetAllMocks();
    vi.mocked(dashboard.getDashboardSummary).mockResolvedValue(defaultSummary);
    vi.mocked(pipelines.listRuns).mockResolvedValue({ items: [], total: 0 } as never);
    vi.mocked(pipelines.getPipelineRuns).mockResolvedValue([]);
    vi.mocked(experiments.listExperiments).mockResolvedValue([]);
    vi.mocked(system.getSystemStatus).mockResolvedValue({
      status: 'ok', database: ok, kafka: ok, spark: ok, hdfs: ok,
    } as never);
  });

  it('shows loading skeletons while queries are pending', () => {
    vi.mocked(system.getSystemStatus).mockReturnValue(new Promise(() => undefined));
    vi.mocked(pipelines.listRuns).mockReturnValue(new Promise(() => undefined));
    vi.mocked(pipelines.getPipelineRuns).mockReturnValue(new Promise(() => undefined));
    vi.mocked(experiments.listExperiments).mockReturnValue(new Promise(() => undefined));
    vi.mocked(dashboard.getDashboardSummary).mockReturnValue(new Promise(() => undefined));
    renderDashboard();
    expect(screen.getAllByLabelText('Loading…').length).toBeGreaterThan(0);
  });

  it('shows empty state when there are no experiments', async () => {
    vi.mocked(experiments.listExperiments).mockResolvedValue([]);
    renderDashboard();
    expect(await screen.findByText('No experiments yet')).toBeInTheDocument();
  });

  it('shows an error state with retry when the experiments request fails', async () => {
    vi.mocked(experiments.listExperiments).mockRejectedValue(new Error('boom'));
    renderDashboard();
    expect(await screen.findByText('Could not load experiments')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /try again/i })).toBeInTheDocument();
  }, 15000);

  it('renders per-service connectivity and experiment rows with correct status', async () => {
    vi.mocked(system.getSystemStatus).mockResolvedValue({
      status: 'degraded', database: ok, kafka: down, spark: ok, hdfs: ok,
    } as never);
    vi.mocked(experiments.listExperiments).mockResolvedValue([
      {
        id: 'e1', name: 'kafka-stop', target: 'kafka', faultType: 'container_stop',
        status: 'succeeded', createdAt: '2026-01-01T00:00:00Z', latestRunId: 'r1',
      },
    ]);
    renderDashboard();
    expect(await screen.findByText('kafka-stop')).toBeInTheDocument();
    expect(screen.getByText('succeeded')).toBeInTheDocument();
    expect(screen.getAllByText('Connected')).toHaveLength(3);
    expect(screen.getAllByText('Disconnected')).toHaveLength(1);
  });

  it('displays KPI numbers equal to dashboard summary API values', async () => {
    vi.mocked(dashboard.getDashboardSummary).mockResolvedValue({
      ...defaultSummary,
      runs: { total: 42, active: 3, succeeded: 35, failed: 4, cancelled: 0 },
      experiments: { total: 12, running: 2, succeeded: 9, failed: 1, cancelled: 0 },
      kpis: {
        avgRecoverySeconds: 14.5,
        totalDataLoss: 0,
        avgThroughput: 1250.0,
        avgAvailabilityPercent: 99.8,
        completedExperimentsConsidered: 10,
      },
    });
    renderDashboard();
    expect(await screen.findByTestId('kpi-active-runs')).toHaveTextContent('3');
    expect(screen.getByTestId('kpi-running-experiments')).toHaveTextContent('2');
    expect(screen.getByTestId('kpi-runs-24h')).toHaveTextContent('42');
    expect(screen.getByTestId('kpi-failures-24h')).toHaveTextContent('4');
    expect(screen.getByTestId('kpi-avg-recovery-time')).toHaveTextContent('14.5s');
  });

  it('has zero accessibility violations (axe-core)', async () => {
    const { container } = renderDashboard();
    await screen.findByText('System Health');
    const results = await axe(container, {
      rules: {
        'color-contrast': { enabled: false },
      },
    });
    expect(results).toHaveNoViolations();
  });
});
