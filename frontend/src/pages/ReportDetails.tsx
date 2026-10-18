import React, { useState, useEffect, useMemo } from 'react';
import { useParams, Link } from 'react-router-dom';
import {
  AlertTriangle,
  ArrowLeft,
  Bot,
  CheckCircle2,
  ChevronDown,
  ChevronRight,
  Copy,
  Download,
  ExternalLink,
  MinusCircle,
  Printer,
  RefreshCw,
  XCircle,
} from 'lucide-react';
import {
  BarChart,
  Bar,
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  CartesianGrid,
} from 'recharts';
import { toast } from 'sonner';
import { getReport } from '../api/reports';
import { getExperimentEvents } from '../api/experiments';
import { ReportDetail } from '../types/report';
import { ExperimentEvent } from '../types/experiment';
import {
  availabilityStepData,
  criterionLabels,
  resourceSeries,
  throughputBars,
} from '../lib/reliability';
import { LogViewer } from '../components/ui/LogViewer';
import { NormalVsFaultComparisonTable } from '../components/reports/NormalVsFaultComparisonTable';

export default function ReportDetails() {
  const { id } = useParams<{ id: string }>();

  const [detail, setDetail] = useState<ReportDetail | null>(null);
  const [events, setEvents] = useState<ExperimentEvent[]>([]);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);

  const [aiExpanded, setAiExpanded] = useState<boolean>(true);
  const [logLevel, setLogLevel] = useState<string>('ALL');
  const [logSearch, setLogSearch] = useState<string>('');

  useEffect(() => {
    if (!id) return;

    let active = true;
    async function loadReport() {
      setLoading(true);
      setError(null);
      try {
        const data = await getReport(id!);
        if (!active) return;
        setDetail(data);

        // Fetch execution events
        try {
          const evts = await getExperimentEvents(id!, 0);
          if (active) setEvents(evts);
        } catch {
          // Fall back to detail.logs if events API fails
        }
      } catch (err: unknown) {
        if (!active) return;
        setError(err instanceof Error ? err.message : 'Failed to load report');
      } finally {
        if (active) setLoading(false);
      }
    }

    loadReport();

    return () => {
      active = false;
    };
  }, [id]);

  const criteria = useMemo(() => {
    return detail ? criterionLabels(detail) : [];
  }, [detail]);

  const availabilityData = useMemo(() => {
    if (!detail) return null;
    const samples = detail.results?.availabilitySamples;
    if (!samples) return null;
    return availabilityStepData(samples, {
      start: detail.faultInjectedAt,
      end: detail.faultRevertedAt,
    });
  }, [detail]);

  const throughputData = useMemo(() => {
    if (!detail || !detail.results) return null;
    return throughputBars(detail.results);
  }, [detail]);

  const cpuData = useMemo(() => {
    if (!detail) return null;
    const samples = detail.results?.cpuSamples;
    return samples ? resourceSeries(samples) : null;
  }, [detail]);

  const handleExportJson = () => {
    if (!detail) return;
    const blob = new Blob([JSON.stringify(detail, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `resilience-report-${detail.id}.json`;
    a.click();
    URL.revokeObjectURL(url);
    toast.success('Report JSON downloaded');
  };

  const handlePrint = () => {
    window.print();
  };

  const handleCopyLogs = () => {
    const lines =
      events.length > 0
        ? events.map((e) => `[${e.ts}] [${e.level}] [${e.phase}] ${e.message}`).join('\n')
        : (detail?.logs || []).join('\n');
    navigator.clipboard.writeText(lines);
    toast.success('Execution logs copied to clipboard');
  };

  if (loading) {
    return (
      <div className="p-12 flex items-center justify-center text-slate-400 text-sm">
        <RefreshCw className="w-5 h-5 animate-spin mr-2 text-brand" />
        Loading resilience report...
      </div>
    );
  }

  if (error || !detail) {
    return (
      <div className="max-w-xl mx-auto p-8 bg-rose-950/60 border border-rose-800 rounded-xl text-rose-300 text-sm">
        <p className="font-semibold text-rose-200 mb-1">Failed to load report</p>
        <p className="text-xs">{error || 'Report not found.'}</p>
        <Link
          to="/reports"
          className="mt-4 inline-flex items-center gap-1.5 px-3 py-1.5 bg-slate-800 text-slate-200 rounded text-xs hover:bg-slate-700 transition-colors"
        >
          <ArrowLeft className="w-3.5 h-3.5" /> Back to Reports
        </Link>
      </div>
    );
  }

  const results = detail.results || {};
  const isPass = detail.result === 'PASS';
  const isFail = detail.result === 'FAIL';

  const logLines =
    events.length > 0
      ? events
          .filter((e) => logLevel === 'ALL' || e.level === logLevel)
          .filter(
            (e) =>
              !logSearch ||
              e.message.toLowerCase().includes(logSearch.toLowerCase()) ||
              e.phase.toLowerCase().includes(logSearch.toLowerCase())
          )
          .map((e) => `[${new Date(e.ts).toLocaleTimeString()}] [${e.level}] [${e.phase}] ${e.message}`)
      : (detail.logs || []).filter((l) => !logSearch || l.toLowerCase().includes(logSearch.toLowerCase()));

  return (
    <div data-testid="page-report-details" className="space-y-6 max-w-6xl mx-auto pb-12">
      {/* Top Navigation & Actions Bar */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4 no-print">
        <Link
          to="/reports"
          className="inline-flex items-center gap-1.5 text-xs font-semibold text-slate-400 hover:text-slate-200 transition-colors"
        >
          <ArrowLeft className="w-4 h-4" />
          Back to Reports
        </Link>

        <div className="flex items-center gap-3">
          <button
            type="button"
            data-testid="btn-export-json"
            onClick={handleExportJson}
            className="px-3.5 py-1.5 bg-slate-800 hover:bg-slate-700 border border-slate-700 text-slate-200 text-xs font-semibold rounded-lg flex items-center gap-1.5 transition-colors"
            title="Download full report as JSON"
          >
            <Download className="w-3.5 h-3.5" />
            Export JSON
          </button>
          <button
            type="button"
            onClick={handlePrint}
            className="px-3.5 py-1.5 bg-indigo-600 hover:bg-indigo-700 text-white text-xs font-semibold rounded-lg flex items-center gap-1.5 transition-colors shadow-sm"
            title="Print or export as PDF"
          >
            <Printer className="w-3.5 h-3.5" />
            Print Report
          </button>
        </div>
      </div>

      {/* Report Header Card */}
      <div className="bg-slate-900 border border-slate-800 rounded-xl p-6 shadow-xl flex flex-col gap-6">
        <div className="flex flex-col md:flex-row md:items-start justify-between gap-4 border-b border-slate-800 pb-5">
          <div className="flex flex-col gap-2">
            <div className="flex items-center gap-3">
              <span
                data-testid="report-verdict-pill"
                className={`px-3.5 py-1 rounded-full text-xs font-bold uppercase tracking-wider flex items-center gap-1.5 ${
                  isPass
                    ? 'bg-emerald-950 text-emerald-300 border border-emerald-800'
                    : isFail
                    ? 'bg-rose-950 text-rose-300 border border-rose-800'
                    : 'bg-slate-800 text-slate-400 border border-slate-700'
                }`}
              >
                {isPass ? <CheckCircle2 className="w-4 h-4" /> : isFail ? <XCircle className="w-4 h-4" /> : null}
                {detail.result || 'INCOMPLETE'}
              </span>
              <h1 className="text-xl sm:text-2xl font-bold text-slate-100 font-mono">
                {detail.name || `Report #${detail.id.slice(0, 8)}`}
              </h1>
            </div>

            <p className="text-xs text-slate-400">
              Report ID: <span className="font-mono text-slate-300">{detail.id}</span>
              {detail.createdAt && (
                <>
                  {' '}• Created on{' '}
                  <span className="font-mono text-slate-300">
                    {new Date(detail.createdAt).toLocaleString()}
                  </span>
                </>
              )}
            </p>
          </div>

          {/* Target, Fault & Pipeline Info Badges */}
          <div className="flex flex-wrap items-center gap-2 text-xs">
            <span className="px-3 py-1 rounded-lg bg-slate-800 border border-slate-700 text-slate-300 font-medium">
              Target: <strong className="text-white">{detail.targetService}</strong>
            </span>
            <span className="px-3 py-1 rounded-lg bg-slate-800 border border-slate-700 text-slate-300 font-medium">
              Fault: <strong className="text-white">{detail.faultType}</strong>
            </span>
            <span className="px-3 py-1 rounded-lg bg-slate-800 border border-slate-700 text-slate-300 font-medium font-mono">
              Duration: {detail.durationSeconds}s
            </span>
            {detail.pipelineRunId && (
              <Link
                to={`/pipelines/${detail.pipelineRunId}`}
                className="px-3 py-1 rounded-lg bg-brand/20 border border-brand/50 text-brand-light hover:underline font-medium flex items-center gap-1"
              >
                Pipeline Run #{detail.pipelineRunId.slice(0, 8)}
                <ExternalLink className="w-3 h-3" />
              </Link>
            )}
          </div>
        </div>

        {/* Failed Criteria / Pass Notice */}
        {detail.failedCriteria.length > 0 ? (
          <div className="p-4 rounded-xl bg-rose-950/50 border border-rose-800 text-rose-300 text-xs flex flex-col gap-1.5">
            <span className="font-bold text-rose-200 flex items-center gap-1.5">
              <AlertTriangle className="w-4 h-4 text-rose-400" />
              Violated Acceptance Criteria:
            </span>
            <ul className="list-disc list-inside space-y-1 pl-1 text-rose-300">
              {detail.failedCriteria.map((c, i) => (
                <li key={i}>{c}</li>
              ))}
            </ul>
          </div>
        ) : isPass ? (
          <div className="p-4 rounded-xl bg-emerald-950/40 border border-emerald-800 text-emerald-300 text-xs flex flex-col gap-1.5">
            <div className="flex items-center gap-2 font-medium">
              <CheckCircle2 className="w-4 h-4 text-emerald-400 shrink-0" />
              <span>
                All evaluated resilience acceptance thresholds met. Service successfully self-healed within budget.
              </span>
            </div>
            <div className="text-[11px] text-slate-300 pl-6 flex flex-wrap gap-x-3 gap-y-1">
              <span><strong>Evaluated criteria:</strong> Recovery time</span>
              {results.dataLoss === null || results.dataLoss === undefined ? (
                <span className="text-amber-300/90"><strong>Unmeasured criteria:</strong> Data loss (standalone fault without associated pipeline)</span>
              ) : (
                <span><strong>Evaluated criteria:</strong> Data loss ({results.dataLoss} records lost)</span>
              )}
            </div>
          </div>
        ) : null}

        {/* Normal vs During-Fault Comparison Table */}
        {((detail.comparisonTable || detail.comparison_table || results.comparisonTable || results.comparison_table)) ? (
          <NormalVsFaultComparisonTable
            rows={detail.comparisonTable || detail.comparison_table || results.comparisonTable || results.comparison_table}
            repetitions={detail.totalRepetitions ?? 1}
          />
        ) : null}

        {/* Criteria Evaluation Table */}
        <div data-testid="report-criteria-table" className="flex flex-col gap-2">
          <h2 className="text-sm font-semibold text-slate-200">Acceptance Criteria Evaluation</h2>
          <div className="overflow-x-auto border border-slate-800 rounded-xl">
            <table className="w-full text-left text-xs divide-y divide-slate-800">
              <thead className="bg-slate-950 text-slate-400 font-semibold uppercase text-[11px]">
                <tr>
                  <th className="py-2.5 px-4">Evaluation Criterion</th>
                  <th className="py-2.5 px-4">Expected Threshold</th>
                  <th className="py-2.5 px-4">Measured Value</th>
                  <th className="py-2.5 px-4">Status</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800 font-mono text-slate-200">
                {criteria.map((c) => (
                  <tr key={c.name} className="hover:bg-slate-800/30">
                    <td className="py-2.5 px-4 font-sans font-medium text-slate-300">{c.name}</td>
                    <td className="py-2.5 px-4 text-slate-400">{c.threshold}</td>
                    <td className="py-2.5 px-4 font-bold">{c.actual}</td>
                    <td className="py-2.5 px-4">
                      {c.passed === null ? (
                        <span className="text-slate-400 font-semibold flex items-center gap-1 font-sans">
                          <MinusCircle className="w-3.5 h-3.5" /> NOT MEASURED
                        </span>
                      ) : c.passed ? (
                        <span className="text-emerald-400 font-semibold flex items-center gap-1 font-sans">
                          <CheckCircle2 className="w-3.5 h-3.5" /> PASS
                        </span>
                      ) : (
                        <span className="text-rose-400 font-semibold flex items-center gap-1 font-sans">
                          <XCircle className="w-3.5 h-3.5" /> FAIL
                        </span>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        {/* Key Metrics Table */}
        <div data-testid="report-key-metrics" className="flex flex-col gap-2">
          <h2 className="text-sm font-semibold text-slate-200">Key Execution Metrics</h2>
          <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-3">
            <div className="bg-slate-950/60 border border-slate-800 p-3 rounded-xl flex flex-col gap-1">
              <span className="text-[11px] text-slate-400">Recovery Time</span>
              <span className="text-lg font-bold font-mono text-slate-100">
                {results.recoveryTimeSeconds !== null && results.recoveryTimeSeconds !== undefined
                  ? `${results.recoveryTimeSeconds.toFixed(1)} s`
                  : 'N/A'}
              </span>
            </div>

            <div className="bg-slate-950/60 border border-slate-800 p-3 rounded-xl flex flex-col gap-1">
              <span className="text-[11px] text-slate-400">Data Loss</span>
              <span className="text-lg font-bold font-mono text-slate-100">
                {results.dataLoss !== null && results.dataLoss !== undefined ? `${results.dataLoss} records` : 'N/A'}
              </span>
            </div>

            <div className="bg-slate-950/60 border border-slate-800 p-3 rounded-xl flex flex-col gap-1">
              <span className="text-[11px] text-slate-400">Expected vs Processed</span>
              <span className="text-sm font-bold font-mono text-slate-100 truncate">
                {results.expectedRecords !== null && results.expectedRecords !== undefined
                  ? `${results.expectedRecords} / ${results.processedRecords ?? 'N/A'}`
                  : 'N/A'}
              </span>
            </div>

            <div className="bg-slate-950/60 border border-slate-800 p-3 rounded-xl flex flex-col gap-1">
              <span className="text-[11px] text-slate-400">Duplicate Records</span>
              <span className="text-lg font-bold font-mono text-slate-100">
                {results.duplicates !== null && results.duplicates !== undefined ? results.duplicates : 'N/A'}
              </span>
            </div>

            <div className="bg-slate-950/60 border border-slate-800 p-3 rounded-xl flex flex-col gap-1">
              <span className="text-[11px] text-slate-400">Availability during fault window</span>
              <span className="text-lg font-bold font-mono text-slate-100">
                {results.availabilityPercentage !== null && results.availabilityPercentage !== undefined
                  ? `${results.availabilityPercentage.toFixed(1)}%`
                  : 'N/A'}
              </span>
            </div>

            <div className="bg-slate-950/60 border border-slate-800 p-3 rounded-xl flex flex-col gap-1">
              <span className="text-[11px] text-slate-400">Throughput Degradation</span>
              <span className="text-lg font-bold font-mono text-slate-100">
                {results.throughputDegradationPercent !== null &&
                results.throughputDegradationPercent !== undefined
                  ? `${results.throughputDegradationPercent.toFixed(1)}%`
                  : 'N/A'}
              </span>
            </div>

            <div className="bg-slate-950/60 border border-slate-800 p-3 rounded-xl flex flex-col gap-1">
              <span className="text-[11px] text-slate-400">Error Rate / Count</span>
              <span className="text-sm font-bold font-mono text-slate-100 truncate">
                {results.errorRate !== null && results.errorRate !== undefined
                  ? `${(results.errorRate * 100).toFixed(2)}% (${results.errorCount ?? 0} errors)`
                  : 'N/A'}
              </span>
            </div>

            <div className="bg-slate-950/60 border border-slate-800 p-3 rounded-xl flex flex-col gap-1">
              <span className="text-[11px] text-slate-400">Total Operations</span>
              <span className="text-lg font-bold font-mono text-slate-100">
                {results.totalOperations !== null && results.totalOperations !== undefined ? results.totalOperations : 'N/A'}
              </span>
            </div>
          </div>
        </div>

        {/* 12-Phase Lifecycle Timeline */}
        <div data-testid="report-timeline" className="flex flex-col gap-2">
          <h2 className="text-sm font-semibold text-slate-200">Lifecycle Phases Timeline</h2>
          <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-6 gap-2 bg-slate-950/60 p-3 rounded-xl border border-slate-800">
            {detail.phases.map((p) => {
              const startMs = p.startedAt ? new Date(p.startedAt).getTime() : null;
              const finishMs = p.finishedAt ? new Date(p.finishedAt).getTime() : null;
              const durSec =
                startMs && finishMs ? Math.max(0, Math.round((finishMs - startMs) / 1000)) : null;

              return (
                <div
                  key={p.name}
                  className="bg-slate-900 border border-slate-800 p-2.5 rounded-lg flex flex-col justify-between gap-1 text-xs"
                >
                  <div className="flex items-center justify-between">
                    <span className="font-semibold text-slate-200 capitalize truncate">
                      {p.name.replace(/_/g, ' ')}
                    </span>
                    <span
                      className={`text-[10px] px-1.5 py-0.2 rounded font-semibold ${
                        p.state === 'done'
                          ? 'bg-emerald-950 text-emerald-400'
                          : p.state === 'error'
                          ? 'bg-rose-950 text-rose-400'
                          : p.state === 'skipped'
                          ? 'bg-slate-800 text-slate-500'
                          : 'bg-blue-950 text-blue-400'
                      }`}
                    >
                      {p.state}
                    </span>
                  </div>

                  <div className="text-[10px] font-mono text-slate-400 flex flex-col">
                    <span>{p.startedAt ? new Date(p.startedAt).toLocaleTimeString() : '—'}</span>
                    <span>Dur: {durSec !== null ? `${durSec} s` : '—'}</span>
                  </div>
                </div>
              );
            })}
          </div>
        </div>

        {/* Dynamic Charts Section */}
        <div data-testid="report-charts" className="flex flex-col gap-3">
          <h2 className="text-sm font-semibold text-slate-200">Operational Telemetry & Performance Charts</h2>
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {/* Availability Step Chart */}
          {availabilityData && (
            <div className="bg-slate-950/60 border border-slate-800 p-4 rounded-xl flex flex-col gap-2">
              <span className="text-xs font-semibold text-slate-300">
                Per-Second Availability Timeline
              </span>
              <div className="h-48 w-full">
                <ResponsiveContainer width="100%" height="100%">
                  <LineChart data={availabilityData} margin={{ top: 10, right: 10, left: -25, bottom: 0 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#334155" opacity={0.5} />
                    <XAxis dataKey="t" stroke="#64748b" tick={{ fontSize: 9 }} />
                    <YAxis domain={[0, 1]} ticks={[0, 1]} stroke="#64748b" tick={{ fontSize: 10 }} />
                    <Tooltip
                      contentStyle={{
                        backgroundColor: '#0f172a',
                        borderColor: '#334155',
                        fontSize: '0.75rem',
                      }}
                    />
                    <Line
                      type="stepAfter"
                      dataKey="available"
                      name="Availability"
                      stroke="#34d399"
                      strokeWidth={2}
                      dot={false}
                    />
                  </LineChart>
                </ResponsiveContainer>
              </div>
            </div>
          )}

          {/* Throughput Bars */}
          {throughputData && (
            <div className="bg-slate-950/60 border border-slate-800 p-4 rounded-xl flex flex-col gap-2">
              <span className="text-xs font-semibold text-slate-300">
                Throughput Across Phases (records / sec)
              </span>
              <div className="h-48 w-full">
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart data={throughputData} margin={{ top: 10, right: 10, left: -15, bottom: 0 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#334155" opacity={0.5} />
                    <XAxis dataKey="name" stroke="#64748b" tick={{ fontSize: 10 }} />
                    <YAxis stroke="#64748b" tick={{ fontSize: 10 }} unit=" rps" />
                    <Tooltip
                      contentStyle={{
                        backgroundColor: '#0f172a',
                        borderColor: '#334155',
                        fontSize: '0.75rem',
                      }}
                    />
                    <Bar dataKey="throughput" name="Throughput" fill="#6366f1" radius={[4, 4, 0, 0]} />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </div>
          )}

          {/* Resource Usage Line Chart */}
          {cpuData && (
            <div className="lg:col-span-2 bg-slate-950/60 border border-slate-800 p-4 rounded-xl flex flex-col gap-2">
              <span className="text-xs font-semibold text-slate-300">
                CPU Utilization During Chaos Injection
              </span>
              <div className="h-48 w-full">
                <ResponsiveContainer width="100%" height="100%">
                  <LineChart data={cpuData} margin={{ top: 10, right: 10, left: -20, bottom: 0 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#334155" opacity={0.5} />
                    <XAxis dataKey="t" stroke="#64748b" tick={{ fontSize: 9 }} />
                    <YAxis stroke="#64748b" tick={{ fontSize: 10 }} unit="%" />
                    <Tooltip
                      contentStyle={{
                        backgroundColor: '#0f172a',
                        borderColor: '#334155',
                        fontSize: '0.75rem',
                      }}
                    />
                    <Line
                      type="monotone"
                      dataKey="percent"
                      name="CPU %"
                      stroke="#f59e0b"
                      strokeWidth={2}
                      dot={false}
                    />
                  </LineChart>
                </ResponsiveContainer>
              </div>
            </div>
          )}
          </div>
        </div>

        {/* AI Analysis Panel */}
        <div data-testid="report-ai-panel" className="border border-slate-800 rounded-xl overflow-hidden bg-slate-950/40">
          <button
            type="button"
            onClick={() => setAiExpanded(!aiExpanded)}
            className="w-full p-4 flex items-center justify-between text-left hover:bg-slate-900/60 transition-colors"
          >
            <div className="flex items-center gap-2.5">
              <Bot className="w-5 h-5 text-indigo-400" />
              <div>
                <h3 className="text-sm font-semibold text-slate-200">AI Reliability Insights</h3>
                <p className="text-[11px] text-slate-400">
                  Automated root cause attribution and resilience hardening recommendations.
                </p>
              </div>
            </div>
            {aiExpanded ? (
              <ChevronDown className="w-4 h-4 text-slate-400" />
            ) : (
              <ChevronRight className="w-4 h-4 text-slate-400" />
            )}
          </button>

          {aiExpanded && (
            <div className="p-4 border-t border-slate-800/80 bg-slate-900/20 text-xs">
              {detail.aiAnalysis ? (
                <div className="flex flex-col gap-3">
                  <div className="flex items-center gap-4">
                    <span className="font-semibold text-slate-300">
                      Risk Score: {String(detail.aiAnalysis.risk_score || 'N/A')}
                    </span>
                    <span className="font-semibold text-slate-300">
                      Risk Level: {String(detail.aiAnalysis.risk_level || 'N/A')}
                    </span>
                  </div>

                  {Array.isArray(detail.aiAnalysis.recommendations) && (
                    <div>
                      <span className="font-semibold text-slate-200 block mb-1">
                        Recommendations:
                      </span>
                      <ul className="list-disc list-inside space-y-1 text-slate-300">
                        {detail.aiAnalysis.recommendations.map((rec: string, i: number) => (
                          <li key={i}>{rec}</li>
                        ))}
                      </ul>
                    </div>
                  )}
                </div>
              ) : (
                <p className="text-slate-400 italic">
                  AI analysis not generated for this run; run with AI insights enabled to populate.
                </p>
              )}
            </div>
          )}
        </div>

        {/* Execution Log */}
        <div data-testid="report-logs" className="flex flex-col gap-3">
          <div className="flex flex-wrap items-center justify-between gap-3 text-xs">
            <span className="font-semibold text-slate-300">
              Execution Log Output ({logLines.length} lines)
            </span>

            <div className="flex items-center gap-2 no-print">
              <select
                aria-label="Filter log level"
                value={logLevel}
                onChange={(e) => setLogLevel(e.target.value)}
                className="bg-slate-800 border border-slate-700 rounded px-2 py-1 text-slate-200 text-xs focus:outline-none"
              >
                <option value="ALL">All Levels</option>
                <option value="INFO">INFO</option>
                <option value="WARNING">WARNING</option>
                <option value="ERROR">ERROR</option>
              </select>

              <input
                type="text"
                placeholder="Search logs..."
                value={logSearch}
                onChange={(e) => setLogSearch(e.target.value)}
                className="bg-slate-800 border border-slate-700 rounded px-2 py-1 text-slate-200 text-xs focus:outline-none w-36"
              />

              <button
                type="button"
                onClick={handleCopyLogs}
                className="px-2.5 py-1 rounded bg-slate-800 border border-slate-700 text-slate-300 hover:text-white flex items-center gap-1"
                title="Copy all logs"
              >
                <Copy className="w-3 h-3" />
                Copy
              </button>
            </div>
          </div>

          <LogViewer
            lines={logLines}
            label="Report execution logs"
            maxHeight="18rem"
            emptyMessage="No execution logs captured for this report."
          />
        </div>
      </div>
    </div>
  );
}
