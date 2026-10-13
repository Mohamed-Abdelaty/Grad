import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import { axe } from 'vitest-axe';

import RunsTable from '../components/pipelines/RunsTable';
import * as pipelines from '../api/pipelines';
import { PipelineRunListItem } from '../types/pipeline';

vi.mock('../api/pipelines');

function renderRunsTable(initialEntries = ['/pipelines'], onRerun = vi.fn()) {
  const queryClient = new QueryClient({
    defaultOptions: {
      queries: { retry: false },
    },
  });

  return {
    queryClient,
    onRerun,
    ...render(
      <QueryClientProvider client={queryClient}>
        <MemoryRouter initialEntries={initialEntries}>
          <Routes>
            <Route
              path="/pipelines"
              element={<RunsTable onRerun={onRerun} />}
            />
          </Routes>
        </MemoryRouter>
      </QueryClientProvider>
    ),
  };
}

const mockRuns: PipelineRunListItem[] = [
  {
    id: 'run-1',
    name: 'sales-pipeline',
    pipelineId: 'p-1',
    pipelineVersionId: 'pv-1',
    status: 'succeeded',
    startedAt: new Date(Date.now() - 60000).toISOString(),
    finishedAt: new Date().toISOString(),
    errorMessage: null,
    outputPath: '/data/sales',
    actualOutputPath: '/data/sales',
    yamlContent: 'pipeline:\n  name: sales-pipeline\n',
  },
  {
    id: 'run-2',
    name: 'iot-pipeline',
    pipelineId: 'p-2',
    pipelineVersionId: 'pv-2',
    status: 'running',
    startedAt: new Date(Date.now() - 30000).toISOString(),
    finishedAt: null,
    errorMessage: null,
    outputPath: '/data/iot',
    actualOutputPath: '/data/iot',
    yamlContent: 'pipeline:\n  name: iot-pipeline\n',
  },
  {
    id: 'run-3',
    name: 'network-logs',
    pipelineId: 'p-3',
    pipelineVersionId: 'pv-3',
    status: 'failed',
    startedAt: new Date(Date.now() - 120000).toISOString(),
    finishedAt: new Date(Date.now() - 60000).toISOString(),
    errorMessage: 'Job failed on cluster',
    outputPath: null,
    actualOutputPath: null,
    yamlContent: null, // null yamlContent
  },
];

describe('RunsTable Component', () => {
  beforeEach(() => {
    vi.resetAllMocks();
  });

  it('renders loading state initially', () => {
    vi.mocked(pipelines.listRuns).mockReturnValue(new Promise(() => {}));
    renderRunsTable();
    expect(screen.getByTestId('runs-table')).toBeInTheDocument();
  });

  it('renders empty state when there are no runs', async () => {
    vi.mocked(pipelines.listRuns).mockResolvedValue({ items: [], total: 0 });
    renderRunsTable();
    expect(await screen.findByText('No runs yet')).toBeInTheDocument();
  });

  it('renders empty state when filters match nothing', async () => {
    vi.mocked(pipelines.listRuns).mockResolvedValue({ items: [], total: 0 });
    renderRunsTable(['/pipelines?q=nonexistent']);
    expect(await screen.findByText('No matching runs')).toBeInTheDocument();
    expect(screen.getAllByRole('button', { name: /clear filters/i }).length).toBeGreaterThan(0);
  });

  it('renders error state with retry button', async () => {
    vi.mocked(pipelines.listRuns).mockRejectedValue(new Error('Network error'));
    renderRunsTable();
    expect(await screen.findByText('Failed to load pipeline runs')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /try again/i })).toBeInTheDocument();
  });

  it('renders rows with all statuses, stop button on running, and disables re-run when yamlContent is null', async () => {
    vi.mocked(pipelines.listRuns).mockResolvedValue({ items: mockRuns, total: 3 });
    const { onRerun } = renderRunsTable();

    // Verify all 3 rows render
    expect(await screen.findByText('sales-pipeline')).toBeInTheDocument();
    expect(screen.getByText('iot-pipeline')).toBeInTheDocument();
    expect(screen.getByText('network-logs')).toBeInTheDocument();

    const rows = screen.getAllByTestId('runs-row');
    expect(rows.length).toBe(3);

    // Running row has Stop button
    const stopButtons = screen.getAllByRole('button', { name: 'Stop' });
    expect(stopButtons.length).toBe(1);

    // Re-run buttons: run-1 and run-2 enabled, run-3 disabled
    const rerunButtons = screen.getAllByRole('button', { name: 'Re-run' });
    expect(rerunButtons.length).toBe(3);
    expect(rerunButtons[0]).not.toBeDisabled();
    expect(rerunButtons[1]).not.toBeDisabled();
    expect(rerunButtons[2]).toBeDisabled();

    // Click re-run on run-1
    fireEvent.click(rerunButtons[0]);
    expect(onRerun).toHaveBeenCalledWith('pipeline:\n  name: sales-pipeline\n');
  });

  it('ignores invalid URL filter values and updates query string on filter changes', async () => {
    vi.mocked(pipelines.listRuns).mockResolvedValue({ items: mockRuns, total: 3 });
    // URL with invalid status & negative page
    renderRunsTable(['/pipelines?status=invalid_status,succeeded&page=-5']);

    await waitFor(() => {
      // listRuns was called with valid status 'succeeded'
      expect(pipelines.listRuns).toHaveBeenCalledWith(
        expect.objectContaining({
          status: ['succeeded'],
          offset: 0, // invalid page -5 normalized to page 1 (offset 0)
        })
      );
    });

    // Toggle status filter chip
    const runningChip = screen.getByTestId('filter-status-running');
    fireEvent.click(runningChip);

    await waitFor(() => {
      expect(pipelines.listRuns).toHaveBeenCalledWith(
        expect.objectContaining({
          status: ['succeeded', 'running'],
        })
      );
    });
  });

  it('has zero accessibility violations (with color-contrast disabled in jsdom)', async () => {
    vi.mocked(pipelines.listRuns).mockResolvedValue({ items: mockRuns, total: 3 });
    const { container } = renderRunsTable();
    await screen.findByText('sales-pipeline');

    const results = await axe(container, {
      rules: { 'color-contrast': { enabled: false } },
    });
    expect(results).toHaveNoViolations();
  });
});
