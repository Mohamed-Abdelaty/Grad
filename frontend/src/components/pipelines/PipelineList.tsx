import React from 'react';
import { PipelineRun } from '../../types/pipeline';
import { useNavigate } from 'react-router-dom';

interface Props {
  runs: PipelineRun[];
}

export default function PipelineList({ runs }: Props) {
  const navigate = useNavigate();

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'succeeded': return 'bg-emerald-500/20 text-emerald-400';
      case 'failed': return 'bg-rose-500/20 text-rose-400';
      case 'running': return 'bg-indigo-500/20 text-indigo-400';
      default: return 'bg-slate-500/20 text-slate-400';
    }
  };

  return (
    <div className="bg-slate-800 rounded-lg overflow-hidden border border-slate-700">
      <table className="w-full text-left text-sm">
        <thead className="bg-slate-700/50">
          <tr>
            <th className="px-4 py-3">Pipeline Name</th>
            <th className="px-4 py-3">Status</th>
            <th className="px-4 py-3">Records Processed</th>
            <th className="px-4 py-3">Started At</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-700">
          {runs.map((run) => (
            <tr
              key={run.id}
              className="hover:bg-slate-700/30 cursor-pointer transition-colors"
              onClick={() => navigate(`/pipelines/${run.id}`)}
            >
              <td className="px-4 py-3 font-medium">{run.name}</td>
              <td className="px-4 py-3">
                <span className={`px-2.5 py-0.5 rounded-full text-xs ${getStatusColor(run.status)}`}>
                  {run.status}
                </span>
              </td>
              <td className="px-4 py-3">{run.recordsProcessed ?? '-'}</td>
              <td className="px-4 py-3">
                {run.startedAt ? new Date(run.startedAt).toLocaleString() : '-'}
              </td>
            </tr>
          ))}
          {runs.length === 0 && (
            <tr>
              <td colSpan={4} className="px-4 py-8 text-center text-slate-400">
                No pipeline runs found.
              </td>
            </tr>
          )}
        </tbody>
      </table>
    </div>
  );
}
