import { apiClient } from './client';
import {
  PipelineRunListItem,
  PipelineTemplate,
  RunStatus,
  ValidationResult,
} from '../types/pipeline';

export interface RawPipelineRun {
  id: string;
  name: string;
  pipeline_id: string;
  pipeline_version_id: string;
  status: RunStatus;
  started_at: string | null;
  finished_at: string | null;
  error_message: string | null;
  output_path: string | null;
  actual_output_path: string | null;
  yaml_content: string | null;
}

export function mapRawPipelineRun(raw: RawPipelineRun): PipelineRunListItem {
  return {
    id: raw.id,
    name: raw.name,
    pipelineId: raw.pipeline_id,
    pipelineVersionId: raw.pipeline_version_id,
    status: raw.status,
    startedAt: raw.started_at,
    finishedAt: raw.finished_at,
    errorMessage: raw.error_message,
    outputPath: raw.output_path,
    actualOutputPath: raw.actual_output_path,
    yamlContent: raw.yaml_content,
  };
}

export async function validatePipeline(yamlContent: string): Promise<ValidationResult> {
  const res = await apiClient.post<ValidationResult>('/api/pipelines/validate', {
    yaml_content: yamlContent,
  });
  return res.data;
}

export interface RunPipelineResponse {
  pipelineRunId: string;
  pipeline_run_id: string;
  status?: string;
  [key: string]: unknown;
}

export async function runPipeline(yamlContent: string, wait: boolean = false): Promise<RunPipelineResponse> {
  const url = wait ? '/api/pipelines/run?wait=true' : '/api/pipelines/run?wait=false';
  const res = await apiClient.post<{ pipeline_run_id: string; status?: string }>(url, {
    yaml_content: yamlContent,
  });
  return {
    pipelineRunId: res.data.pipeline_run_id,
    pipeline_run_id: res.data.pipeline_run_id,
    status: res.data.status,
    ...res.data,
  };
}

export interface ListRunsParams {
  status?: string[];
  q?: string;
  limit?: number;
  offset?: number;
  sort?: string;
}

export interface ListRunsResult {
  items: PipelineRunListItem[];
  total: number;
}

export async function listRuns(params?: ListRunsParams): Promise<ListRunsResult> {
  const queryParams: Record<string, string | number> = {};
  if (params?.status && params.status.length > 0) {
    queryParams.status = params.status.join(',');
  }
  if (params?.q) {
    queryParams.q = params.q;
  }
  if (params?.limit !== undefined) {
    queryParams.limit = params.limit;
  }
  if (params?.offset !== undefined) {
    queryParams.offset = params.offset;
  }
  if (params?.sort) {
    queryParams.sort = params.sort;
  }

  const res = await apiClient.get<RawPipelineRun[]>('/api/pipeline-runs', {
    params: queryParams,
  });
  const totalHeader = res.headers ? res.headers['x-total-count'] : undefined;
  const parsedTotal = totalHeader ? parseInt(totalHeader, 10) : NaN;
  const total = !isNaN(parsedTotal) ? parsedTotal : res.data.length;

  return {
    items: (res.data || []).map(mapRawPipelineRun),
    total,
  };
}

export async function getPipelineRuns(): Promise<PipelineRunListItem[]> {
  const { items } = await listRuns({ limit: 100 });
  return items;
}

export async function getRun(runId: string): Promise<PipelineRunListItem> {
  const res = await apiClient.get<RawPipelineRun>(`/api/pipeline-runs/${runId}`);
  return mapRawPipelineRun(res.data);
}

export const getPipelineRun = getRun;

export async function cancelRun(
  runId: string
): Promise<{ accepted: boolean; runId?: string; detail?: string }> {
  const res = await apiClient.post<{ accepted: boolean; run_id?: string; detail?: string }>(
    `/api/pipeline-runs/${runId}/cancel`
  );
  return {
    accepted: res.data.accepted,
    runId: res.data.run_id,
    detail: res.data.detail,
  };
}

export async function listTemplates(): Promise<PipelineTemplate[]> {
  const res = await apiClient.get<PipelineTemplate[]>('/api/pipeline-templates');
  return res.data;
}
