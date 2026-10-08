import React, { useState, useEffect } from 'react';
import { AllowedService, FaultType } from '../../types/experiment';
import { getAllowedTargets, createExperiment, runExperiment } from '../../api/experiments';

interface Props {
  onExperimentStarted: (id: string) => void;
}

export default function ExperimentForm({ onExperimentStarted }: Props) {
  const [targets, setTargets] = useState<AllowedService[]>([]);
  const [selectedTarget, setSelectedTarget] = useState<AllowedService | ''>('');
  const [faultType, setFaultType] = useState<FaultType | ''>('');
  const [duration, setDuration] = useState<number>(60);
  const [delayMs, setDelayMs] = useState<number>(200);
  const [expectedLoss, setExpectedLoss] = useState<number>(0);
  const [maxRecovery, setMaxRecovery] = useState<number>(120);
  const [pipelineYaml, setPipelineYaml] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const faultOptions: FaultType[] = [
    'container_stop', 'container_restart', 'network_disruption', 'cpu_stress',
    'memory_stress', 'disk_pressure', 'service_delay'
  ];
  const faultDescriptions: Record<FaultType, string> = {
    container_stop: 'Stops the selected container for the selected duration, then starts it again.',
    container_restart: 'Restarts the selected container once and measures the recovery.',
    network_disruption: 'Temporarily interrupts the selected container network connection.',
    cpu_stress: 'Applies bounded CPU load to the selected container.',
    memory_stress: 'Applies bounded memory load to the selected container.',
    disk_pressure: 'Creates temporary bounded disk pressure, then removes it.',
    service_delay: 'Adds a temporary response delay, then removes it automatically.'
  };

  useEffect(() => {
    getAllowedTargets().then(setTargets).catch(() => setError('Failed to load targets.'));
  }, []);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!selectedTarget || !faultType || duration < 1 || duration > 300) {
      setError('Please fill out all fields correctly. Duration 1-300.');
      return;
    }
    
    setLoading(true);
    setError(null);
    try {
      const delayConfig = faultType === 'service_delay' ? `\n  delay_ms: ${delayMs}` : '';
      const yaml = `experiment:
  name: exp-${Date.now()}
target:
  service: ${selectedTarget}
fault:
  type: ${faultType}
  duration: ${duration}${delayConfig}
validation:
  expected_data_loss: ${expectedLoss}
  max_recovery_time: ${maxRecovery}
`;
      const { experiment_id } = await createExperiment(yaml, pipelineYaml);
      await runExperiment(experiment_id);
      onExperimentStarted(experiment_id);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to start experiment');
    } finally {
      setLoading(false);
    }
  };

  return (
    <form onSubmit={handleSubmit} className="bg-slate-800 p-6 rounded-lg border border-slate-700 space-y-4">
      <h3 className="text-lg font-medium text-slate-100">Create Reliability Experiment</h3>
      
      {error && <div className="text-rose-400 text-sm">{error}</div>}

      <div className="grid grid-cols-2 gap-4">
        <div>
          <label className="block text-sm text-slate-400 mb-1">Target Service</label>
          <select 
            value={selectedTarget} onChange={e => setSelectedTarget(e.target.value as AllowedService)}
            className="w-full bg-slate-900 border border-slate-700 rounded p-2 text-slate-200"
          >
            <option value="">-- Select --</option>
            {targets.map(t => <option key={t} value={t}>{t}</option>)}
          </select>
          {faultType && <p className="mt-2 text-xs text-slate-400">{faultDescriptions[faultType]}</p>}
        </div>
        
        <div>
          <label className="block text-sm text-slate-400 mb-1">Fault Type</label>
          <select 
            value={faultType} onChange={e => setFaultType(e.target.value as FaultType)}
            className="w-full bg-slate-900 border border-slate-700 rounded p-2 text-slate-200"
          >
            <option value="">-- Select --</option>
            {faultOptions.map(f => <option key={f} value={f}>{f}</option>)}
          </select>
        </div>

        <div>
          <label className="block text-sm text-slate-400 mb-1">Duration (s)</label>
          <input 
            type="number" min="1" max="300"
            value={duration} onChange={e => setDuration(Number(e.target.value))}
            className="w-full bg-slate-900 border border-slate-700 rounded p-2 text-slate-200"
          />
        </div>

        {faultType === 'service_delay' && (
          <div>
            <label className="block text-sm text-slate-400 mb-1">Service Delay (ms)</label>
            <input
              type="number" min="1" max="10000"
              value={delayMs} onChange={e => setDelayMs(Number(e.target.value))}
              className="w-full bg-slate-900 border border-slate-700 rounded p-2 text-slate-200"
            />
          </div>
        )}

        <div>
          <label className="block text-sm text-slate-400 mb-1">Expected Data Loss</label>
          <input
            type="number" min="0"
            value={expectedLoss} onChange={e => setExpectedLoss(Number(e.target.value))}
            className="w-full bg-slate-900 border border-slate-700 rounded p-2 text-slate-200"
          />
        </div>

        <div>
          <label className="block text-sm text-slate-400 mb-1">Max Recovery Time (s)</label>
          <input 
            type="number" 
            value={maxRecovery} onChange={e => setMaxRecovery(Number(e.target.value))}
            className="w-full bg-slate-900 border border-slate-700 rounded p-2 text-slate-200"
          />
        </div>
        <div className="col-span-2">
          <label className="block text-sm text-slate-400 mb-1">Pipeline YAML (optional)</label>
          <textarea
            value={pipelineYaml}
            onChange={e => setPipelineYaml(e.target.value)}
            rows={6}
            placeholder="Attach a pipeline to validate post-recovery output"
            className="w-full bg-slate-900 border border-slate-700 rounded p-2 font-mono text-sm text-slate-200"
          />
        </div>
      </div>

      <button 
        type="submit" disabled={loading}
        className="px-4 py-2 bg-indigo-600 hover:bg-indigo-500 rounded text-white font-medium disabled:opacity-50"
      >
        {loading ? 'Starting...' : 'Start Experiment'}
      </button>
    </form>
  );
}
