import { Eye } from 'lucide-react';
import { Experiment } from '../../types/experiment';

interface Props {
  experiments: Experiment[];
  selectedId: string | null;
  onSelect: (id: string) => void;
}

export function RecentExperimentsList({ experiments, selectedId, onSelect }: Props) {
  if (experiments.length === 0) {
    return (
      <div
        data-testid="recent-experiments"
        className="bg-slate-900 border border-slate-800 rounded-xl p-6 text-center text-slate-400 text-xs"
      >
        No experiments created yet.
      </div>
    );
  }

  const statusColors: Record<string, string> = {
    running: 'bg-blue-950 text-blue-300 border-blue-800 animate-pulse',
    succeeded: 'bg-emerald-950 text-emerald-300 border-emerald-800',
    failed: 'bg-rose-950 text-rose-300 border-rose-800',
    cancelled: 'bg-slate-800 text-slate-400 border-slate-700',
    pending: 'bg-amber-950 text-amber-300 border-amber-800',
  };

  return (
    <div
      data-testid="recent-experiments"
      className="bg-slate-900 border border-slate-800 rounded-xl p-5 shadow-xl flex flex-col gap-4"
    >
      <div className="flex items-center justify-between border-b border-slate-800 pb-3">
        <h3 className="text-sm font-semibold text-slate-200">Recent Experiments</h3>
        <span className="text-xs text-slate-400 font-mono">{experiments.length} runs</span>
      </div>

      <div className="flex flex-col gap-2 max-h-[360px] overflow-y-auto">
        {experiments.map((exp) => {
          const isSelected = selectedId === exp.id;
          const statusKey = exp.status || 'pending';

          return (
            <div
              key={exp.id}
              onClick={() => onSelect(exp.id)}
              className={`p-3 rounded-lg border cursor-pointer transition-all flex items-center justify-between gap-3 ${
                isSelected
                  ? 'bg-brand/10 border-brand shadow-sm'
                  : 'bg-slate-800/40 border-slate-700/60 hover:bg-slate-800 hover:border-slate-600'
              }`}
            >
              <div className="flex flex-col gap-1 min-w-0">
                <div className="flex items-center gap-2">
                  <span className="font-semibold text-xs text-slate-100 font-mono truncate">
                    {exp.name}
                  </span>
                  <span
                    className={`px-2 py-0.5 rounded text-[10px] font-semibold border ${
                      statusColors[statusKey] || statusColors.pending
                    }`}
                  >
                    {statusKey}
                  </span>
                </div>

                <div className="flex items-center gap-2 text-[11px] text-slate-400">
                  <span>Target: {exp.target || 'N/A'}</span>
                  <span>•</span>
                  <span>Fault: {exp.faultType || 'N/A'}</span>
                  {exp.createdAt && (
                    <>
                      <span>•</span>
                      <span className="font-mono">
                        {new Date(exp.createdAt).toLocaleDateString()}
                      </span>
                    </>
                  )}
                </div>
              </div>

              <button
                type="button"
                onClick={(e) => {
                  e.stopPropagation();
                  onSelect(exp.id);
                }}
                className={`p-1.5 rounded-lg text-xs transition-colors shrink-0 ${
                  isSelected
                    ? 'bg-brand text-white'
                    : 'bg-slate-800 text-slate-400 hover:text-slate-200 hover:bg-slate-700'
                }`}
                title="View in monitor"
              >
                <Eye className="w-3.5 h-3.5" />
              </button>
            </div>
          );
        })}
      </div>
    </div>
  );
}
