import React from 'react';
import { Report } from '../../types/report';

interface Props {
  report: Report;
}

function MetricCell({ label, value }: { label: string; value: string | null }) {
  return (
    <div>
      <span className="block text-slate-500">{label}</span>
      <span className="font-mono text-slate-200">{value ?? '—'}</span>
    </div>
  );
}

export default function ReportCard({ report }: Props) {
  const recovery =
    report.recoveryTimeSeconds !== null
      ? `${report.recoveryTimeSeconds.toFixed(2)}s`
      : null;
  const availability =
    report.availabilityPercentage !== null
      ? `${report.availabilityPercentage.toFixed(1)}%`
      : null;
  const dataLoss =
    report.dataLossPercentage !== null
      ? `${report.dataLossPercentage.toFixed(2)}%`
      : null;

  return (
    <div className="bg-slate-800 border border-slate-700 rounded-lg p-4 flex flex-col space-y-4">
      <div className="flex justify-between items-start">
        <div>
          <h3 className="font-semibold text-slate-100">{report.experimentName}</h3>
          <p className="text-sm text-slate-400">
            {report.target ? `Target: ${report.target}` : ''}
            {report.failureType ? ` | Fault: ${report.failureType}` : ''}
          </p>
        </div>
        <span className="text-xs text-slate-500">{new Date(report.createdAt).toLocaleDateString()}</span>
      </div>

      <div className="grid grid-cols-3 gap-4 text-sm bg-slate-900/50 p-3 rounded">
        <MetricCell label="Recovery Time" value={recovery} />
        <MetricCell label="Availability during fault window" value={availability} />
        <MetricCell label="Data Loss" value={dataLoss} />
      </div>
    </div>
  );
}
