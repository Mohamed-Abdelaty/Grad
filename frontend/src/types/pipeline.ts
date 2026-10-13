export interface ValidationIssue {
  field: string;
  message: string;
}

export type ValidationError = ValidationIssue;

export interface ValidationResult {
  valid: boolean;
  errors: ValidationIssue[];
}

export interface PipelineTemplate {
  id: string;
  name: string;
  description: string;
  yaml: string;
}

export type RunStatus = 'pending' | 'running' | 'succeeded' | 'failed' | 'cancelled';

export interface PipelineRunListItem {
  id: string;
  name: string;
  pipelineId: string;
  pipelineVersionId: string;
  status: RunStatus;
  startedAt: string | null;
  finishedAt: string | null;
  errorMessage: string | null;
  outputPath: string | null;
  actualOutputPath: string | null;
  yamlContent: string | null;
}

export type PipelineRun = PipelineRunListItem;

export interface PipelineExecutionResult {
  pipeline_run_id: string;
  status: 'success' | 'failed' | 'invalid';
  metrics?: Record<string, number | string | null>;
  logs?: Array<{ level: string; step: string; message: string }>;
}
