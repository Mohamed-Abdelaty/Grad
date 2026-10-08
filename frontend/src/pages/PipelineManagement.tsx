import React, { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import YamlEditor from '../components/pipelines/YamlEditor';
import ValidationErrors from '../components/pipelines/ValidationErrors';
import PipelineList from '../components/pipelines/PipelineList';
import { validatePipeline, runPipeline, getPipelineRuns } from '../api/pipelines';
import { PipelineExecutionResult, ValidationResult } from '../types/pipeline';

export default function PipelineManagement() {
  const [yaml, setYaml] = useState('');
  const [validation, setValidation] = useState<ValidationResult | null>(null);
  const [running, setRunning] = useState(false);
  const [actionError, setActionError] = useState<string | null>(null);
  const [execution, setExecution] = useState<PipelineExecutionResult | null>(null);
  const finalLog = execution?.logs[execution.logs.length - 1];

  const { data: runs, refetch } = useQuery({
    queryKey: ['pipelineRuns'],
    queryFn: getPipelineRuns,
    refetchInterval: 5000
  });

  const handleValidate = async () => {
    setActionError(null);
    try {
      const res = await validatePipeline(yaml);
      setValidation(res);
    } catch (error) {
      setValidation(null);
      setActionError(error instanceof Error ? error.message : 'Validation request failed.');
    }
  };

  const handleRun = async () => {
    if (!validation?.valid) return;
    setRunning(true);
    setActionError(null);
    setExecution(null);
    try {
      const result = await runPipeline(yaml, true); // Use wait=true to keep existing synchronous behavior for UI-2
      setExecution(result as PipelineExecutionResult);
      refetch();
    } catch (error) {
      setActionError(error instanceof Error ? error.message : 'Pipeline execution request failed.');
    } finally {
      setRunning(false);
    }
  };

  return (
    <div className="space-y-6 max-w-5xl">
      <section className="space-y-4">
        <h2 className="text-xl font-semibold">Define Pipeline</h2>
        <YamlEditor value={yaml} onChange={setYaml} placeholder="apiVersion: v1..." />
        
        <div className="flex space-x-4">
          <button onClick={handleValidate} className="px-4 py-2 bg-slate-700 hover:bg-slate-600 rounded">
            Validate
          </button>
          <button 
            onClick={handleRun} 
            disabled={!validation?.valid || running}
            className="px-4 py-2 bg-indigo-600 hover:bg-indigo-500 rounded disabled:opacity-50"
          >
            {running ? 'Submitting...' : 'Run Pipeline'}
          </button>
        </div>

        {actionError && <div className="text-sm text-rose-400">{actionError}</div>}
        {validation && <ValidationErrors errors={validation.errors} />}
        {execution && (
          <div className={`rounded border p-4 text-sm ${execution.status === 'success' ? 'border-emerald-500/40 bg-emerald-500/10 text-emerald-200' : 'border-rose-500/40 bg-rose-500/10 text-rose-200'}`}>
            <p className="font-semibold">Pipeline {execution.status === 'success' ? 'completed successfully.' : 'finished with an error.'}</p>
            <p className="mt-1 font-mono text-xs">Run ID: {execution.pipeline_run_id}</p>
            {execution.status !== 'success' && finalLog && (
              <p className="mt-2">{finalLog.message}</p>
            )}
          </div>
        )}
      </section>

      <section>
        <h2 className="text-xl font-semibold mb-4">Pipeline Runs</h2>
        <PipelineList runs={runs || []} />
      </section>
    </div>
  );
}
