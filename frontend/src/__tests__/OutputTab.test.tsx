import React from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor, fireEvent } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { axe } from 'vitest-axe';

import { OutputTab } from '../components/pipelines/details/OutputTab';
import * as outputApi from '../api/output';
import { OutputMetadata, OutputPreview, DownloadLink } from '../types/output';
import { toast } from '../components/ui/Toast';

const mockMetadata: OutputMetadata = {
  format: 'parquet',
  path: 'hdfs://namenode:9000/data/processed/sales',
  fileCount: 2,
  totalSizeBytes: 20480,
  rowCount: 100,
  verified: true,
  columns: [
    { name: 'id', type: 'string' },
    { name: 'amount', type: 'double' },
    { name: 'count', type: 'int32' },
  ],
  files: [
    { name: 'part-00000.parquet', sizeBytes: 10240 },
    { name: 'part-00001.parquet', sizeBytes: 10240 },
  ],
};

const mockPreview: OutputPreview = {
  columns: [
    { name: 'id', type: 'string' },
    { name: 'amount', type: 'double' },
    { name: 'count', type: 'int32' },
  ],
  rows: [
    { id: 'tx-1', amount: 99.5, count: 1 },
    { id: 'tx-2', amount: 150.0, count: 2 },
  ],
  limit: 50,
  totalRows: 100,
  truncated: false,
};

function renderWithClient(ui: React.ReactElement) {
  const queryClient = new QueryClient({
    defaultOptions: {
      queries: {
        retry: false,
        gcTime: 0,
      },
    },
  });
  return render(
    <QueryClientProvider client={queryClient}>
      {ui}
    </QueryClientProvider>
  );
}

describe('OutputTab Component', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('renders output-not-ready state when status is running or pending', () => {
    renderWithClient(
      <OutputTab runId="run-123" status="running" />
    );

    expect(screen.getByTestId('output-tab')).toBeInTheDocument();
    expect(screen.getByTestId('output-not-ready')).toBeInTheDocument();
    expect(screen.getByText('Pipeline is still running')).toBeInTheDocument();
  });

  it('renders output-not-ready state when status is failed', () => {
    renderWithClient(
      <OutputTab runId="run-123" status="failed" />
    );

    expect(screen.getByTestId('output-not-ready')).toBeInTheDocument();
    expect(screen.getByText('Output not available')).toBeInTheDocument();
    expect(
      screen.getByText('Output is only available for successfully completed pipeline runs.')
    ).toBeInTheDocument();
  });

  it('renders loading skeleton while metadata is fetching', () => {
    vi.spyOn(outputApi, 'getOutput').mockImplementation(() => new Promise(() => {}));

    renderWithClient(
      <OutputTab runId="run-123" status="succeeded" />
    );

    expect(screen.getByTestId('output-loading')).toBeInTheDocument();
  });

  it('renders error state when metadata fails to load', async () => {
    vi.spyOn(outputApi, 'getOutput').mockRejectedValue(new Error('Storage unavailable'));

    renderWithClient(
      <OutputTab runId="run-123" status="succeeded" />
    );

    await waitFor(() => {
      expect(screen.getByTestId('output-error')).toBeInTheDocument();
      expect(screen.getByText('Storage unavailable')).toBeInTheDocument();
    });
  });

  it('renders output-empty state when file count is zero', async () => {
    vi.spyOn(outputApi, 'getOutput').mockResolvedValue({
      ...mockMetadata,
      fileCount: 0,
      files: [],
    });

    renderWithClient(
      <OutputTab runId="run-123" status="succeeded" />
    );

    await waitFor(() => {
      expect(screen.getByTestId('output-empty')).toBeInTheDocument();
      expect(screen.getByText('No output files produced')).toBeInTheDocument();
    });
  });

  it('renders ready state with metadata, schema, preview, and download controls', async () => {
    vi.spyOn(outputApi, 'getOutput').mockResolvedValue(mockMetadata);
    vi.spyOn(outputApi, 'getPreview').mockResolvedValue(mockPreview);

    renderWithClient(
      <OutputTab runId="run-123" status="succeeded" />
    );

    // Metadata section
    await waitFor(() => {
      expect(screen.getByTestId('output-metadata')).toBeInTheDocument();
    });
    expect(screen.getByTestId('badge-format')).toHaveTextContent('parquet');
    expect(screen.getByTestId('badge-verified')).toHaveTextContent('Verified');
    expect(screen.getByTestId('meta-output-path')).toHaveTextContent('hdfs://namenode:9000/data/processed/sales');
    expect(screen.getByTestId('meta-file-count')).toHaveTextContent('2 files');
    expect(screen.getByTestId('meta-row-count')).toHaveTextContent('100');

    // Schema section
    expect(screen.getByTestId('output-schema')).toBeInTheDocument();
    expect(screen.getByTestId('schema-col-id')).toHaveTextContent('string');
    expect(screen.getByTestId('schema-col-amount')).toHaveTextContent('double');

    // Preview table
    await waitFor(() => {
      expect(screen.getByTestId('output-preview')).toBeInTheDocument();
    });
    expect(screen.getByTestId('preview-count')).toHaveTextContent('Showing 2 of 100 rows');
    expect(screen.getByText('tx-1')).toBeInTheDocument();
    expect(screen.getByText('tx-2')).toBeInTheDocument();

    // Toggle part files
    const toggleFilesBtn = screen.getByTestId('btn-toggle-files');
    fireEvent.click(toggleFilesBtn);
    expect(screen.getByTestId('output-file-list')).toBeInTheDocument();
    expect(screen.getByText('part-00000.parquet')).toBeInTheDocument();

    // Download controls
    expect(screen.getByTestId('btn-download-csv')).toBeInTheDocument();
    expect(screen.getByTestId('btn-download-parquet')).toBeInTheDocument();
  });

  it('handles path copy and CSV preview copy to clipboard', async () => {
    vi.spyOn(outputApi, 'getOutput').mockResolvedValue(mockMetadata);
    vi.spyOn(outputApi, 'getPreview').mockResolvedValue(mockPreview);
    const toastSpy = vi.spyOn(toast, 'success');

    // Mock navigator.clipboard.writeText
    const writeTextMock = vi.fn().mockResolvedValue(undefined);
    Object.assign(navigator, {
      clipboard: { writeText: writeTextMock },
    });

    renderWithClient(
      <OutputTab runId="run-123" status="succeeded" />
    );

    await waitFor(() => {
      expect(screen.getByTestId('btn-copy-path')).toBeInTheDocument();
    });

    // Copy path
    fireEvent.click(screen.getByTestId('btn-copy-path'));
    await waitFor(() => {
      expect(writeTextMock).toHaveBeenCalledWith('hdfs://namenode:9000/data/processed/sales');
      expect(toastSpy).toHaveBeenCalledWith('Output path copied');
    });

    // Copy CSV
    await waitFor(() => {
      expect(screen.getByTestId('btn-copy-csv')).toBeInTheDocument();
    });
    fireEvent.click(screen.getByTestId('btn-copy-csv'));
    await waitFor(() => {
      expect(writeTextMock).toHaveBeenCalledWith(expect.stringContaining('id,amount,count'));
      expect(toastSpy).toHaveBeenCalledWith('Copied CSV preview to clipboard');
    });
  });

  it('triggers download for normal sized output without warning modal', async () => {
    vi.spyOn(outputApi, 'getOutput').mockResolvedValue(mockMetadata);
    vi.spyOn(outputApi, 'getPreview').mockResolvedValue(mockPreview);
    const downloadMock: DownloadLink = {
      url: '/api/pipeline-runs/run-123/output/download?token=signed-token-123',
      expiresAt: '2026-10-14T12:00:00Z',
      sizeBytes: 20480,
      format: 'csv',
    };
    const reqSpy = vi.spyOn(outputApi, 'requestDownloadLink').mockResolvedValue(downloadMock);
    const toastSpy = vi.spyOn(toast, 'success');

    renderWithClient(
      <OutputTab runId="run-123" status="succeeded" />
    );

    await waitFor(() => {
      expect(screen.getByTestId('btn-download-csv')).toBeInTheDocument();
    });

    fireEvent.click(screen.getByTestId('btn-download-csv'));

    await waitFor(() => {
      expect(reqSpy).toHaveBeenCalledWith('run-123', 'csv');
      expect(toastSpy).toHaveBeenCalledWith('Download started (CSV)');
    });
  });

  it('opens confirmation modal before downloading large output (> 100 MB)', async () => {
    const largeMeta: OutputMetadata = {
      ...mockMetadata,
      totalSizeBytes: 150 * 1024 * 1024, // 150 MB
    };
    vi.spyOn(outputApi, 'getOutput').mockResolvedValue(largeMeta);
    vi.spyOn(outputApi, 'getPreview').mockResolvedValue(mockPreview);
    const reqSpy = vi.spyOn(outputApi, 'requestDownloadLink').mockResolvedValue({
      url: '/download?token=xyz',
      expiresAt: '2026-10-14T12:00:00Z',
      sizeBytes: 150 * 1024 * 1024,
      format: 'parquet',
    });

    renderWithClient(
      <OutputTab runId="run-123" status="succeeded" />
    );

    await waitFor(() => {
      expect(screen.getByTestId('btn-download-parquet')).toBeInTheDocument();
    });

    fireEvent.click(screen.getByTestId('btn-download-parquet'));

    // Confirmation dialog should be visible
    expect(screen.getByText('Large Download Confirmation')).toBeInTheDocument();
    expect(screen.getByText(/over 100 MB/)).toBeInTheDocument();

    // Confirm dialog
    const proceedBtn = screen.getByRole('button', { name: 'Proceed to Download' });
    fireEvent.click(proceedBtn);

    await waitFor(() => {
      expect(reqSpy).toHaveBeenCalledWith('run-123', 'parquet');
    });
  });

  it('shows error toast when download link request fails with 413 or error', async () => {
    vi.spyOn(outputApi, 'getOutput').mockResolvedValue(mockMetadata);
    vi.spyOn(outputApi, 'getPreview').mockResolvedValue(mockPreview);
    vi.spyOn(outputApi, 'requestDownloadLink').mockRejectedValue(
      new Error('Output exceeds the maximum downloadable size (500 MB). Read it from HDFS directly.')
    );
    const toastErrorSpy = vi.spyOn(toast, 'error');

    renderWithClient(
      <OutputTab runId="run-123" status="succeeded" />
    );

    await waitFor(() => {
      expect(screen.getByTestId('btn-download-csv')).toBeInTheDocument();
    });

    fireEvent.click(screen.getByTestId('btn-download-csv'));

    await waitFor(() => {
      expect(toastErrorSpy).toHaveBeenCalledWith(
        'Output exceeds the maximum downloadable size (500 MB). Read it from HDFS directly.'
      );
    });
  });

  it('has zero accessibility violations (axe-core)', async () => {
    vi.spyOn(outputApi, 'getOutput').mockResolvedValue(mockMetadata);
    vi.spyOn(outputApi, 'getPreview').mockResolvedValue(mockPreview);

    const { container } = renderWithClient(
      <OutputTab runId="run-123" status="succeeded" />
    );

    await waitFor(() => {
      expect(screen.getByTestId('output-metadata')).toBeInTheDocument();
    });

    const results = await axe(container);
    expect(results).toHaveNoViolations();
  });
});
