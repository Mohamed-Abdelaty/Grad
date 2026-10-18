import React from 'react';
import { ComparisonTableRow } from '../../types/experiment';
import { HelpCircle } from 'lucide-react';

interface NormalVsFaultComparisonTableProps {
  rows?: ComparisonTableRow[] | null;
  repetitions?: number;
}

export const NormalVsFaultComparisonTable: React.FC<NormalVsFaultComparisonTableProps> = ({
  rows,
  repetitions = 1,
}) => {
  if (!rows || rows.length === 0) {
    return null;
  }

  const formatValue = (
    val: number | null | undefined,
    unit: string,
    reason?: string | null,
    subText?: string | null
  ) => {
    if (val === null || val === undefined) {
      return (
        <span className="text-slate-400 italic font-sans text-xs">
          {reason || 'not measured'}
        </span>
      );
    }
    let rendered = `${val} ${unit}`;
    if (unit === 'seconds') rendered = `${val.toFixed(1)} s`;
    else if (unit === '%') rendered = `${val.toFixed(1)}%`;
    else if (unit === 'records/s' || unit === 'records/sec') rendered = `${val.toLocaleString()} rec/s`;
    else if (unit === 'records') rendered = `${val.toLocaleString()} records`;

    return (
      <div className="flex flex-col">
        <span>{rendered}</span>
        {subText && <span className="text-[10px] text-slate-400 font-sans">{subText}</span>}
      </div>
    );
  };

  const formatDelta = (delta: number | null | undefined, pctChange: number | null | undefined, unit: string) => {
    if (delta === null || delta === undefined) {
      return <span className="text-slate-400 italic font-sans text-xs">-</span>;
    }
    const sign = delta > 0 ? '+' : '';
    let deltaFormatted = `${sign}${delta} ${unit}`;
    if (unit === 'seconds') deltaFormatted = `${sign}${delta.toFixed(1)} s`;
    if (unit === '%') deltaFormatted = `${sign}${delta.toFixed(1)}%`;
    if (unit === 'records/s' || unit === 'records/sec') deltaFormatted = `${sign}${delta.toLocaleString()} rec/s`;
    if (unit === 'records') deltaFormatted = `${sign}${delta.toLocaleString()} records`;

    const pctText = pctChange !== null && pctChange !== undefined ? ` (${pctChange > 0 ? '+' : ''}${pctChange.toFixed(1)}%)` : '';

    return (
      <span className={delta === 0 ? 'text-slate-300' : delta < 0 ? 'text-amber-400' : 'text-slate-200'}>
        {deltaFormatted}
        {pctText && <span className="text-[11px] text-slate-400 font-sans ml-1">{pctText}</span>}
      </span>
    );
  };

  return (
    <div data-testid="report-comparison-table" className="flex flex-col gap-3">
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-1">
        <div>
          <h2 className="text-sm font-semibold text-slate-200">Normal vs During-Fault Comparison</h2>
          <p className="text-xs text-slate-400">
            Side-by-side evaluation of baseline behavior against the fault injection window.
          </p>
        </div>
        {repetitions > 1 && (
          <span className="text-[11px] font-mono px-2 py-0.5 rounded bg-indigo-950 text-indigo-300 border border-indigo-800 self-start sm:self-auto">
            Aggregated across {repetitions} repetitions
          </span>
        )}
      </div>

      <div className="overflow-x-auto border border-slate-800 rounded-xl bg-slate-900/50">
        <table className="w-full text-left text-xs divide-y divide-slate-800">
          <thead className="bg-slate-950 text-slate-400 font-semibold uppercase text-[11px]">
            <tr>
              <th className="py-2.5 px-4">Metric</th>
              <th className="py-2.5 px-4">Normal (Baseline)</th>
              <th className="py-2.5 px-4">During Fault</th>
              <th className="py-2.5 px-4">Delta (Impact)</th>
              {repetitions > 1 && <th className="py-2.5 px-4">Repetition Statistics (Mean ± Std)</th>}
              <th className="py-2.5 px-4">Definition</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-800 font-mono text-slate-200">
            {rows.map((row) => {
              const normalVal = row.normal;
              const faultVal = row.during_fault !== undefined ? row.during_fault : row.duringFault;
              const deltaVal = row.delta;
              const pctChangeVal = row.pct_change !== undefined ? row.pct_change : row.pctChange;
              const baselineApplicable = row.baseline_applicable !== undefined ? row.baseline_applicable : row.baselineApplicable;
              const normalReason = row.normal_reason || row.normalReason;
              const faultReason = row.during_fault_reason || row.duringFaultReason;
              const agg = row.aggregate;
              const dupDetails = row.metric === 'duplicates' && (row.output_duplicates !== undefined || row.outputDuplicates !== undefined)
                ? `(output: ${row.output_duplicates ?? row.outputDuplicates ?? 0}, source: ${row.source_duplicates ?? row.sourceDuplicates ?? 0})`
                : null;

              return (
                <tr key={row.metric} className="hover:bg-slate-800/30">
                  <td className="py-2.5 px-4 font-sans font-medium text-slate-300">
                    <div className="flex flex-col">
                      <span>{row.label}</span>
                      <span className="text-[10px] text-slate-400 font-mono">({row.unit})</span>
                    </div>
                  </td>
                  <td className="py-2.5 px-4">
                    {!baselineApplicable ? (
                      <span className="text-slate-400 italic font-sans text-xs">n/a (by definition)</span>
                    ) : (
                      formatValue(normalVal, row.unit, normalReason)
                    )}
                  </td>
                  <td className="py-2.5 px-4 font-bold">{formatValue(faultVal, row.unit, faultReason, dupDetails)}</td>
                  <td className="py-2.5 px-4">
                    {!baselineApplicable ? (
                      <span className="text-slate-400 italic font-sans text-xs">n/a</span>
                    ) : (
                      formatDelta(deltaVal, pctChangeVal, row.unit)
                    )}
                  </td>
                  {repetitions > 1 && (
                    <td className="py-2.5 px-4 font-sans text-xs">
                      {agg && agg.mean !== null && agg.mean !== undefined ? (
                        <div className="flex flex-col gap-0.5 font-mono text-[11px]">
                          <span className="text-slate-200">
                            μ = {agg.mean.toFixed(1)} ± {agg.std !== null && agg.std !== undefined ? agg.std.toFixed(1) : '0.0'}
                          </span>
                          <span className="text-slate-400 text-[10px]">
                            [{agg.min !== null && agg.min !== undefined ? agg.min.toFixed(1) : '-'} .. {agg.max !== null && agg.max !== undefined ? agg.max.toFixed(1) : '-'}] (N={agg.n_samples ?? agg.nSamples ?? 0})
                          </span>
                        </div>
                      ) : (
                        <span className="text-slate-400 italic">not measured</span>
                      )}
                    </td>
                  )}
                  <td className="py-2.5 px-4 font-sans text-slate-400 text-xs max-w-xs">
                    <span className="flex items-center gap-1">
                      <HelpCircle className="w-3 h-3 text-slate-400 shrink-0 inline" />
                      {row.definition}
                    </span>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
};
