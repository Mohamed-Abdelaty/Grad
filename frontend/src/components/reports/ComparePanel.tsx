import React from 'react';
import { Link } from 'react-router-dom';
import { CheckCircle2, ExternalLink, X, XCircle } from 'lucide-react';
import { ReportListItem } from '../../types/report';

interface Props {
  open: boolean;
  onClose: () => void;
  reports: ReportListItem[];
}

export function ComparePanel({ open, onClose, reports }: Props) {
  React.useEffect(() => {
    if (!open) return;
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
    };
    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [open, onClose]);

  if (!open || reports.length === 0) return null;

  return (
    <div
      role="dialog"
      aria-modal="true"
      aria-labelledby="compare-panel-title"
      data-testid="compare-panel"
      className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-slate-950/80 backdrop-blur-sm"
    >
      <div className="bg-slate-900 border border-slate-700 rounded-2xl shadow-2xl max-w-5xl w-full max-h-[90vh] flex flex-col overflow-hidden animate-in fade-in zoom-in-95 duration-150">
        {/* Header */}
        <div className="flex items-center justify-between p-5 border-b border-slate-800 bg-slate-900/80">
          <div>
            <h2 id="compare-panel-title" className="text-lg font-bold text-slate-100">
              Report Comparison ({reports.length} selected)
            </h2>
            <p className="text-xs text-slate-400 mt-0.5">
              Side-by-side resilience performance metrics across test runs.
            </p>
          </div>
          <button
            type="button"
            data-testid="btn-close-compare"
            onClick={onClose}
            className="p-1.5 rounded-lg text-slate-400 hover:text-white hover:bg-slate-800 transition-colors"
            title="Close comparison"
          >
            <X className="w-5 h-5" />
          </button>
        </div>

        {/* Content Table */}
        <div className="p-6 overflow-auto">
          <table className="w-full text-left text-xs border border-slate-800 rounded-xl overflow-hidden divide-y divide-slate-800">
            <thead className="bg-slate-950 text-slate-400 uppercase text-[11px] font-semibold">
              <tr>
                <th className="py-3 px-4 font-semibold w-40 text-slate-300">Metric</th>
                {reports.map((r) => (
                  <th key={r.id} className="py-3 px-4 min-w-[180px]">
                    <div className="flex flex-col gap-1">
                      <span className="font-mono text-slate-100 font-bold truncate">{r.title || r.id}</span>
                      <span className="text-[10px] text-slate-400 font-normal">
                        {r.createdAt ? new Date(r.createdAt).toLocaleDateString() : ''}
                      </span>
                    </div>
                  </th>
                ))}
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-800/80 font-mono text-slate-200">
              {/* Verdict */}
              <tr className="bg-slate-900/40">
                <td className="py-3 px-4 font-sans font-medium text-slate-300">Verdict</td>
                {reports.map((r) => (
                  <td key={r.id} className="py-3 px-4">
                    {r.result === 'PASS' ? (
                      <span className="inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded-full text-xs font-semibold bg-emerald-950 text-emerald-300 border border-emerald-800">
                        <CheckCircle2 className="w-3.5 h-3.5" /> PASS
                      </span>
                    ) : r.result === 'FAIL' ? (
                      <span className="inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded-full text-xs font-semibold bg-rose-950 text-rose-300 border border-rose-800">
                        <XCircle className="w-3.5 h-3.5" /> FAIL
                      </span>
                    ) : (
                      <span className="px-2.5 py-0.5 rounded-full text-xs bg-slate-800 text-slate-400">
                        INCOMPLETE
                      </span>
                    )}
                  </td>
                ))}
              </tr>

              {/* Target Service */}
              <tr>
                <td className="py-2.5 px-4 font-sans font-medium text-slate-300">Target Service</td>
                {reports.map((r) => (
                  <td key={r.id} className="py-2.5 px-4">
                    <span className="bg-slate-800 px-2 py-0.5 rounded text-slate-200">
                      {r.targetService || 'N/A'}
                    </span>
                  </td>
                ))}
              </tr>

              {/* Fault Type */}
              <tr className="bg-slate-900/40">
                <td className="py-2.5 px-4 font-sans font-medium text-slate-300">Fault Type</td>
                {reports.map((r) => (
                  <td key={r.id} className="py-2.5 px-4">
                    <span className="bg-slate-800 px-2 py-0.5 rounded text-slate-200">
                      {r.faultType || 'N/A'}
                    </span>
                  </td>
                ))}
              </tr>

              {/* Recovery Time */}
              <tr>
                <td className="py-2.5 px-4 font-sans font-medium text-slate-300">Recovery Time</td>
                {reports.map((r) => (
                  <td key={r.id} className="py-2.5 px-4 font-bold text-slate-100">
                    {r.recoveryTimeSeconds !== null ? `${r.recoveryTimeSeconds.toFixed(1)} s` : 'N/A'}
                  </td>
                ))}
              </tr>

              {/* Data Loss */}
              <tr className="bg-slate-900/40">
                <td className="py-2.5 px-4 font-sans font-medium text-slate-300">Data Loss</td>
                {reports.map((r) => (
                  <td key={r.id} className="py-2.5 px-4">
                    {r.dataLoss !== null && r.dataLoss !== undefined ? `${r.dataLoss} records` : 'N/A'}
                  </td>
                ))}
              </tr>

              {/* Throughput Baseline */}
              <tr>
                <td className="py-2.5 px-4 font-sans font-medium text-slate-300">Throughput Baseline</td>
                {reports.map((r) => (
                  <td key={r.id} className="py-2.5 px-4">
                    {r.throughputBaseline !== null ? `${r.throughputBaseline} rps` : 'N/A'}
                  </td>
                ))}
              </tr>

              {/* Throughput During Fault */}
              <tr className="bg-slate-900/40">
                <td className="py-2.5 px-4 font-sans font-medium text-slate-300">Throughput During Fault</td>
                {reports.map((r) => (
                  <td key={r.id} className="py-2.5 px-4">
                    {r.throughputDuringFault !== null ? `${r.throughputDuringFault} rps` : 'N/A'}
                  </td>
                ))}
              </tr>

              {/* Availability */}
              <tr>
                <td className="py-2.5 px-4 font-sans font-medium text-slate-300">Availability during fault window</td>
                {reports.map((r) => (
                  <td key={r.id} className="py-2.5 px-4 font-semibold">
                    {r.availabilityPercentage !== null ? `${r.availabilityPercentage.toFixed(1)}%` : 'N/A'}
                  </td>
                ))}
              </tr>

              {/* Error Rate */}
              <tr className="bg-slate-900/40">
                <td className="py-2.5 px-4 font-sans font-medium text-slate-300">Error Rate</td>
                {reports.map((r) => (
                  <td key={r.id} className="py-2.5 px-4">
                    {r.errorRate !== null ? `${(r.errorRate * 100).toFixed(2)}%` : 'N/A'}
                  </td>
                ))}
              </tr>

              {/* Link to detail */}
              <tr>
                <td className="py-3 px-4 font-sans font-medium text-slate-300">Detail Link</td>
                {reports.map((r) => (
                  <td key={r.id} className="py-3 px-4">
                    <Link
                      to={`/reports/${r.id}`}
                      className="inline-flex items-center gap-1 text-xs text-brand hover:underline font-sans font-semibold"
                    >
                      View Report <ExternalLink className="w-3 h-3" />
                    </Link>
                  </td>
                ))}
              </tr>
            </tbody>
          </table>
        </div>

        {/* Footer */}
        <div className="p-4 border-t border-slate-800 bg-slate-900/60 flex justify-end">
          <button
            type="button"
            onClick={onClose}
            className="px-4 py-2 bg-slate-800 hover:bg-slate-700 text-slate-200 rounded-lg text-xs font-semibold transition-colors"
          >
            Close
          </button>
        </div>
      </div>
    </div>
  );
}
