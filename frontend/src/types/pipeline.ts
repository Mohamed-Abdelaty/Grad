export interface ValidationError {
  field: string;
  message: string;
}

export interface ValidationResult {
  valid: boolean;
  errors: ValidationError[];
}

// Result returned by POST /api/pipelines/run.
// pipeline_run_id is the DB UUID of the created PipelineRun row.
// run_id (the executor's internal UUID) is not returned by the API and is removed.
export interface PipelineExecutionResult {
  pipeline_run_id: string;
  status: 'success' | 'failed' | 'invalid';
  metrics: Record<string, number | string | null>;
  logs: Array<{ level: string; step: string; message: string }>;
}

// Shape returned by GET /api/pipeline-runs/{id}.
// Matches the PipelineRun SQLAlchemy model fields as serialized by the route.
export interface PipelineRun {
  id: string;
  name: string;
  status: 'pending' | 'running' | 'succeeded' | 'failed' | 'cancelled';
  recordsProcessed: number | null;
  startedAt: string;
  completedAt: string | null;
  error_message: string | null;
  metrics: Record<string, number | null>;
  configuration: {
    source?: { path?: string; type?: string };
    storage?: { path?: string; type?: string };
    processing?: { operations?: Array<{ type: string }> };
  } | null;
}
