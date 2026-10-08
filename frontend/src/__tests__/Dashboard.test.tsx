import React from 'react';
import { render, screen, waitFor } from '@testing-library/react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { MemoryRouter } from 'react-router-dom';
import Dashboard from '../pages/Dashboard';
import * as system from '../api/system';
import * as pipelines from '../api/pipelines';
import * as experiments from '../api/experiments';

vi.mock('../api/system');
vi.mock('../api/pipelines');
vi.mock('../api/experiments');

const ok = { connected: true, error: null, latency_ms: 3 };
const down = { connected: false, error: 'refused', latency_ms: null };

function renderDashboard() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
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
  });

  it('shows loading skeletons while queries are pending', () => {
    vi.mocked(system.getSystemStatus).mockReturnValue(new Promise(() => undefined));
    vi.mocked(pipelines.getPipelineRuns).mockReturnValue(new Promise(() => undefined));
    vi.mocked(experiments.listExperiments).mockReturnValue(new Promise(() => undefined));
    renderDashboard();
    expect(screen.getAllByLabelText('Loading…').length).toBeGreaterThan(0);
  });

  it('shows empty state when there are no experiments', async () => {
    vi.mocked(system.getSystemStatus).mockResolvedValue({
      status: 'ok', database: ok, kafka: ok, spark: ok, hdfs: ok,
    } as never);
    vi.mocked(pipelines.getPipelineRuns).mockResolvedValue([]);
    vi.mocked(experiments.listExperiments).mockResolvedValue([]);
    renderDashboard();
    expect(await screen.findByText('No experiments yet')).toBeInTheDocument();
  });

  it('shows an error state with retry when the experiments request fails', async () => {
    vi.mocked(system.getSystemStatus).mockResolvedValue({
      status: 'ok', database: ok, kafka: ok, spark: ok, hdfs: ok,
    } as never);
    vi.mocked(pipelines.getPipelineRuns).mockResolvedValue([]);
    vi.mocked(experiments.listExperiments).mockRejectedValue(new Error('boom'));
    renderDashboard();
    expect(await screen.findByText('Could not load experiments')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /try again/i })).toBeInTheDocument();
  });

  it('renders per-service connectivity and experiment rows with correct status', async () => {
    vi.mocked(system.getSystemStatus).mockResolvedValue({
      status: 'degraded', database: ok, kafka: down, spark: ok, hdfs: ok,
    } as never);
    vi.mocked(pipelines.getPipelineRuns).mockResolvedValue([]);
    vi.mocked(experiments.listExperiments).mockResolvedValue([
      {
        id: 'e1', name: 'kafka-stop', target: 'kafka', faultType: 'container_stop',
        status: 'succeeded', createdAt: '2026-01-01T00:00:00Z', latestRunId: 'r1',
      },
    ]);
    renderDashboard();
    await waitFor(() => expect(screen.getByText('kafka-stop')).toBeInTheDocument());
    expect(screen.getByText('succeeded')).toBeInTheDocument();
    expect(screen.getAllByText('Connected')).toHaveLength(3);
    expect(screen.getAllByText('Disconnected')).toHaveLength(1);
  });
});
