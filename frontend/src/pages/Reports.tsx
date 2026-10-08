import React from 'react';
import { useQuery } from '@tanstack/react-query';
import { analyzeReliability, getReports } from '../api/experiments';
import ReportChart from '../components/reports/ReportChart';
import ReportCard from '../components/reports/ReportCard';
import ReliabilityInsight from '../components/reports/ReliabilityInsight';

export default function Reports() {
  const { data: reports, isLoading } = useQuery({
    queryKey: ['reports'],
    queryFn: getReports,
  });

  const { data: analysis } = useQuery({
    queryKey: ['reliability-analysis', reports?.map((r) => r.id)],
    queryFn: () =>
      analyzeReliability(
        (reports ?? []).map((r) => ({
          recovery_time_seconds: r.recoveryTimeSeconds ?? 0,
          throughput_rows_per_second: r.throughputRecordsPerSec ?? 0,
          data_loss: r.dataLossPercentage ?? 0,
          error_rate:
            r.availabilityPercentage !== null
              ? 1 - r.availabilityPercentage / 100
              : 0,
        }))
      ),
    enabled: Boolean(reports?.length),
  });

  if (isLoading) return <div className="p-4 text-slate-400">Loading reports…</div>;

  return (
    <div className="max-w-5xl space-y-6">
      <h2 className="text-2xl font-bold">Resilience Reports</h2>
      <p className="text-sm text-slate-400">
        Reports without an attached pipeline show data validation as skipped.
      </p>

      {reports && reports.length > 0 ? (
        <>
          <ReportChart reports={reports} />
          {analysis && <ReliabilityInsight analysis={analysis} />}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {reports.map((report) => (
              <ReportCard key={report.id} report={report} />
            ))}
          </div>
        </>
      ) : (
        <div className="text-slate-400">
          No reports generated yet. Run experiments to see data here.
        </div>
      )}
    </div>
  );
}
