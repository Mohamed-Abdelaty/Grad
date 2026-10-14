/**
 * frontend/src/api/output.ts
 * --------------------------
 * API client methods for UI-6 output inspection, preview, and downloads.
 */

import axios from 'axios';
import { apiClient } from './client';
import { DownloadLink, OutputMetadata, OutputPreview } from '../types/output';

export async function getOutput(runId: string): Promise<OutputMetadata> {
  const response = await apiClient.get<OutputMetadata>(`/api/pipeline-runs/${runId}/output`);
  return response.data;
}

export async function getPreview(runId: string, limit: number = 50): Promise<OutputPreview> {
  const response = await apiClient.get<OutputPreview>(`/api/pipeline-runs/${runId}/output/preview`, {
    params: { limit },
  });
  return response.data;
}

export async function requestDownloadLink(
  runId: string,
  format: 'csv' | 'parquet'
): Promise<DownloadLink> {
  const response = await apiClient.post<DownloadLink>(
    `/api/pipeline-runs/${runId}/output/download-link`,
    { format }
  );
  return response.data;
}

export function toOutputError(error: unknown): string {
  if (axios.isAxiosError(error)) {
    const status = error.response?.status;
    const detail = error.response?.data?.detail;

    if (status === 404) {
      return 'No output found for this run';
    }
    if (status === 409) {
      return 'Output is not available until the run completes successfully';
    }
    if (status === 413) {
      return typeof detail === 'string' && detail.trim().length > 0
        ? detail
        : 'Output exceeds the maximum downloadable size (500 MB). Read it from HDFS directly.';
    }
    if (status === 503) {
      return 'Storage service is temporarily unavailable';
    }
    if (typeof detail === 'string' && detail.trim().length > 0) {
      return detail;
    }
    if (error.message) {
      return error.message;
    }
  }

  if (error instanceof Error) {
    return error.message;
  }
  return 'Failed to load output';
}
