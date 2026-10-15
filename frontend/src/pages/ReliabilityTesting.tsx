import React, { useState, useEffect, useCallback } from 'react';
import { Flame, RefreshCw } from 'lucide-react';
import { Experiment, ExperimentOptions } from '../types/experiment';
import { getOptions, listExperiments } from '../api/experiments';
import { ExperimentWizard } from '../components/experiments/ExperimentWizard';
import { ActiveExperimentMonitor } from '../components/experiments/ActiveExperimentMonitor';
import { RecentExperimentsList } from '../components/experiments/RecentExperimentsList';

export default function ReliabilityTesting() {
  const [options, setOptions] = useState<ExperimentOptions | null>(null);
  const [experiments, setExperiments] = useState<Experiment[]>([]);
  const [activeExperimentId, setActiveExperimentId] = useState<string | null>(null);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);

  const loadData = useCallback(async () => {
    try {
      setLoading(true);
      setError(null);
      const [opts, exps] = await Promise.all([getOptions(), listExperiments()]);
      setOptions(opts);
      setExperiments(exps);

      // Auto-select running experiment if exists, or latest
      const runningExp = exps.find((e) => e.status === 'running' || e.status === 'pending');
      if (runningExp) {
        setActiveExperimentId(runningExp.id);
      } else if (exps.length > 0) {
        setActiveExperimentId((prev) => prev || exps[0].id);
      }
    } catch (err: unknown) {
      setError((err as Error).message || 'Failed to load reliability testing options.');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    loadData();
  }, [loadData]);

  const runningExperiment = experiments.find(
    (e) => e.status === 'running' || e.status === 'pending'
  );

  const handleExperimentStarted = (id: string) => {
    setActiveExperimentId(id);
    listExperiments().then(setExperiments).catch(() => {});
  };

  const handleExperimentFinished = () => {
    listExperiments().then(setExperiments).catch(() => {});
  };

  if (loading && !options) {
    return (
      <div className="p-8 flex items-center justify-center text-slate-400 text-sm">
        <RefreshCw className="w-5 h-5 animate-spin mr-2 text-brand" />
        Loading ChaosLab environment...
      </div>
    );
  }

  if (error || !options) {
    return (
      <div className="p-8 max-w-lg mx-auto bg-rose-950/60 border border-rose-800 rounded-xl text-rose-300 text-sm">
        <p className="font-semibold text-rose-200 mb-1">Failed to initialize ChaosLab</p>
        <p className="text-xs">{error || 'Unable to fetch experiment options.'}</p>
        <button
          onClick={loadData}
          className="mt-4 px-3 py-1.5 bg-rose-900/80 hover:bg-rose-800 text-white rounded text-xs font-semibold"
        >
          Retry
        </button>
      </div>
    );
  }

  return (
    <div data-testid="page-reliability" className="space-y-6 max-w-7xl mx-auto">
      {/* Page Header */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
        <div>
          <h1 className="text-2xl font-bold text-slate-100 flex items-center gap-2.5">
            <Flame className="w-6 h-6 text-amber-500" />
            ChaosLab Reliability Testing
          </h1>
          <p className="text-xs text-slate-400 mt-1">
            Conduct controlled failure injection experiments against live cluster infrastructure and verify automated recovery.
          </p>
        </div>

        <button
          type="button"
          onClick={loadData}
          className="self-start sm:self-auto px-3 py-1.5 bg-slate-800 hover:bg-slate-700 text-slate-300 rounded-lg text-xs font-medium flex items-center gap-1.5 border border-slate-700 transition-colors"
        >
          <RefreshCw className="w-3.5 h-3.5" />
          Refresh
        </button>
      </div>

      {/* 2-Column Responsive Layout */}
      <div className="grid grid-cols-1 lg:grid-cols-12 gap-6 items-start">
        {/* Left Column: New Experiment Wizard */}
        <div className="lg:col-span-5">
          <ExperimentWizard
            options={options}
            runningExperimentName={runningExperiment ? runningExperiment.name : null}
            onExperimentStarted={handleExperimentStarted}
          />
        </div>

        {/* Right Column: Active Experiment Card on Top, Recent Experiments List Below */}
        <div className="lg:col-span-7 flex flex-col gap-6">
          {activeExperimentId ? (
            <ActiveExperimentMonitor
              experimentId={activeExperimentId}
              onFinished={handleExperimentFinished}
            />
          ) : (
            <div className="bg-slate-900 border border-slate-800 rounded-xl p-8 text-center text-slate-400 text-sm">
              No active experiment selected. Configure and launch a new experiment from the wizard on the left.
            </div>
          )}

          <RecentExperimentsList
            experiments={experiments}
            selectedId={activeExperimentId}
            onSelect={(id) => setActiveExperimentId(id)}
          />
        </div>
      </div>
    </div>
  );
}
