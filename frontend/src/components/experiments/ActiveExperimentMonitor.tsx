import React, { useState, useEffect, useRef, useMemo } from 'react';
import { Link } from 'react-router-dom';
import { toast } from 'sonner';
import {
  Activity,
  AlertCircle,
  AlertTriangle,
  ArrowRight,
  CheckCircle2,
  Clock,
  Download,
  Flame,
  Pause,
  Play,
  RotateCcw,
  ShieldAlert,
  StopCircle,
  XCircle,
  Zap,
} from 'lucide-react';
import { ExperimentDetail, ExperimentEvent } from '../../types/experiment';
import { getExperiment, getExperimentEvents, cancelExperiment } from '../../api/experiments';
import {
  criterionLabels,
  experimentToProgress,
  faultCountdown,
  phaseElapsedSeconds,
  serverClockOffset,
} from '../../lib/reliability';
import { PipelineVisualization } from '../../features/viz/PipelineVisualization';
import { useRunLive } from '../../hooks/useRunLive';
import { LogViewer } from '../ui/LogViewer';
import { ConfirmDialog } from '../ui/ConfirmDialog';

interface Props {
  experimentId: string;
  onFinished?: () => void;
}

export function ActiveExperimentMonitor({ experimentId, onFinished }: Props) {
  const [detail, setDetail] = useState<ExperimentDetail | null>(null);
  const [events, setEvents] = useState<ExperimentEvent[]>([]);
  const [nowAligned, setNowAligned] = useState<number>(Date.now());
  const [cancelModalOpen, setCancelModalOpen] = useState<boolean>(false);
  const [isCancelling, setIsCancelling] = useState<boolean>(false);
  const [selectedLogLevel, setSelectedLogLevel] = useState<string>('ALL');
  const [logSearchQuery, setLogSearchQuery] = useState<string>('');

  const lastSeqRef = useRef<number>(0);
  const terminalNotifiedRef = useRef<boolean>(false);

  // Poll experiment detail
  useEffect(() => {
    let active = true;
    let timerId: ReturnType<typeof setTimeout> | null = null;

    async function fetchDetail() {
      try {
        const data = await getExperiment(experimentId);
        if (!active) return;
        setDetail(data);

        // Calculate server clock alignment
        const offset = serverClockOffset(data.serverTime, Date.now());
        setNowAligned(Date.now() + offset);

        const isRunning = data.status === 'running' || data.status === 'pending';
        if (!isRunning) {
          if (!terminalNotifiedRef.current) {
            terminalNotifiedRef.current = true;
            onFinished?.();
          }
        } else {
          timerId = setTimeout(fetchDetail, 1000);
        }
      } catch (err) {
        if (!active) return;
        timerId = setTimeout(fetchDetail, 2000);
      }
    }

    fetchDetail();

    return () => {
      active = false;
      if (timerId) clearTimeout(timerId);
    };
  }, [experimentId, onFinished]);

  // Clock tick for live timers
  useEffect(() => {
    const interval = setInterval(() => {
      if (detail?.serverTime) {
        const offset = serverClockOffset(detail.serverTime, Date.now());
        setNowAligned(Date.now() + offset);
      } else {
        setNowAligned(Date.now());
      }
    }, 1000);
    return () => clearInterval(interval);
  }, [detail?.serverTime]);

  // Poll experiment events
  useEffect(() => {
    let active = true;
    let timerId: ReturnType<typeof setTimeout> | null = null;

    async function fetchEvents() {
      try {
        const incoming = await getExperimentEvents(experimentId, lastSeqRef.current);
        if (!active) return;

        if (incoming.length > 0) {
          setEvents((prev) => {
            const seen = new Set(prev.map((e) => e.seq));
            const fresh = incoming.filter((e) => !seen.has(e.seq));
            const merged = [...prev, ...fresh].sort((a, b) => a.seq - b.seq);
            if (merged.length > 0) {
              lastSeqRef.current = merged[merged.length - 1].seq;
            }
            return merged.slice(-2000);
          });
        }

        const isTerminal =
          detail?.status === 'succeeded' ||
          detail?.status === 'failed' ||
          detail?.status === 'cancelled';

        if (!isTerminal) {
          timerId = setTimeout(fetchEvents, 1500);
        }
      } catch (err) {
        if (!active) return;
        timerId = setTimeout(fetchEvents, 3000);
      }
    }

    fetchEvents();

    return () => {
      active = false;
      if (timerId) clearTimeout(timerId);
    };
  }, [experimentId, detail?.status]);

  // Live pipeline hook if pipelineRunId exists
  const livePipeline = useRunLive(detail?.pipelineRunId || undefined);

  const effectiveProgress = useMemo(() => {
    if (detail?.pipelineRunId && livePipeline.progress) {
      return livePipeline.progress;
    }
    return detail ? experimentToProgress(detail) : null;
  }, [detail, livePipeline.progress]);

  const handleCancel = async () => {
    try {
      setIsCancelling(true);
      await cancelExperiment(experimentId);
      toast.info('Cancel signal sent. Reverting fault and cleaning up...');
      setCancelModalOpen(false);
      // Re-fetch detail
      const updated = await getExperiment(experimentId);
      setDetail(updated);
    } catch (err: any) {
      toast.error(`Cancel failed: ${err.message || 'Unknown error'}`);
    } finally {
      setIsCancelling(false);
    }
  };

  const handleDownloadEvents = () => {
    const text = events
      .map((e) => `[${e.ts}] [${e.level}] [${e.phase}] (seq ${e.seq}) ${e.message}`)
      .join('\n');
    const blob = new Blob([text], { type: 'text/plain' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `experiment-${detail?.name || experimentId}-events.txt`;
    a.click();
    URL.revokeObjectURL(url);
  };

  if (!detail) {
    return (
      <div
        data-testid="exp-monitor"
        className="bg-slate-900 border border-slate-800 rounded-xl p-8 flex items-center justify-center text-slate-400 text-sm animate-pulse"
      >
        Loading experiment monitor...
      </div>
    );
  }

  const isRunning = detail.status === 'running' || detail.status === 'pending';
  const isFinished = !isRunning;
  const countdown = faultCountdown(detail, nowAligned);
  const phaseElapsed = phaseElapsedSeconds(detail, nowAligned);
  const criteria = isFinished ? criterionLabels(detail) : [];

  const faultPhaseLabels: Record<string, string> = {
    not_injected: 'Not Injected',
    injected: 'Injected',
    recovering: 'Recovering',
    recovered: 'Recovered',
  };

  const faultPhaseColors: Record<string, string> = {
    not_injected: 'bg-slate-800 text-slate-400 border-slate-700',
    injected: 'bg-rose-950 text-rose-300 border-rose-800 animate-pulse',
    recovering: 'bg-amber-950 text-amber-300 border-amber-800 animate-pulse',
    recovered: 'bg-emerald-950 text-emerald-300 border-emerald-800',
  };

  const filteredEvents = events.filter((e) => {
    const matchesLevel = selectedLogLevel === 'ALL' || e.level === selectedLogLevel;
    const matchesQuery =
      !logSearchQuery ||
      e.message.toLowerCase().includes(logSearchQuery.toLowerCase()) ||
      e.phase.toLowerCase().includes(logSearchQuery.toLowerCase());
    return matchesLevel && matchesQuery;
  });

  const formattedLogLines = filteredEvents.map(
    (e) => `[${new Date(e.ts).toLocaleTimeString()}] [${e.level}] [${e.phase}] ${e.message}`
  );

  return (
    <div
      data-testid="exp-monitor"
      className="bg-slate-900 border border-slate-800 rounded-xl p-6 shadow-xl flex flex-col gap-6"
    >
      {/* Polite live region for accessibility */}
      <div aria-live="polite" className="sr-only">
        Phase: {detail.currentPhase}, Fault phase: {faultPhaseLabels[detail.faultPhase] || detail.faultPhase}
        {countdown.active ? `, Fault active: ${countdown.elapsed} seconds of ${countdown.total} seconds` : ''}
      </div>

      {/* Header */}
      <div className="flex flex-col md:flex-row md:items-center justify-between gap-4 border-b border-slate-800 pb-5">
        <div className="flex flex-col gap-1.5">
          <div className="flex items-center gap-3">
            <h2 className="text-xl font-bold text-slate-100 font-mono">{detail.name}</h2>
            <span
              className={`px-2.5 py-0.5 rounded-full text-xs font-semibold uppercase tracking-wider ${
                detail.status === 'running'
                  ? 'bg-blue-950 text-blue-300 border border-blue-800 animate-pulse'
                  : detail.status === 'succeeded'
                  ? 'bg-emerald-950 text-emerald-300 border border-emerald-800'
                  : detail.status === 'failed'
                  ? 'bg-rose-950 text-rose-300 border border-rose-800'
                  : detail.status === 'cancelled'
                  ? 'bg-slate-800 text-slate-400 border border-slate-700'
                  : 'bg-amber-950 text-amber-300 border border-amber-800'
              }`}
            >
              {detail.status}
            </span>
          </div>
          <div className="flex items-center gap-2 text-xs text-slate-400">
            <span className="bg-slate-800 px-2 py-0.5 rounded text-slate-300 font-medium">
              Target: {detail.targetService}
            </span>
            <span className="bg-slate-800 px-2 py-0.5 rounded text-slate-300 font-medium">
              Fault: {detail.faultType}
            </span>
            {detail.runStartedAt && (
              <span className="flex items-center gap-1 font-mono text-slate-400">
                <Clock className="w-3.5 h-3.5" />
                Started: {new Date(detail.runStartedAt).toLocaleTimeString()}
              </span>
            )}
          </div>
        </div>

        {/* Fault Phase Badge & Countdown */}
        <div className="flex items-center gap-3">
          <div className="flex flex-col items-end gap-1">
            <span
              data-testid="fault-phase"
              className={`px-3 py-1 rounded-lg text-xs font-semibold border flex items-center gap-1.5 ${
                faultPhaseColors[detail.faultPhase] || faultPhaseColors.not_injected
              }`}
            >
              <Zap className="w-3.5 h-3.5" />
              Fault Phase: {faultPhaseLabels[detail.faultPhase] || detail.faultPhase}
            </span>

            {countdown.active && (
              <span className="text-xs text-rose-400 font-mono font-semibold animate-pulse">
                Fault active: {countdown.elapsed} s of {countdown.total} s
              </span>
            )}
          </div>

          {/* Cancel Button */}
          {isRunning && (
            <button
              type="button"
              onClick={() => setCancelModalOpen(true)}
              disabled={isCancelling}
              className="px-3.5 py-1.5 bg-rose-950/80 hover:bg-rose-900 border border-rose-800 text-rose-200 text-xs font-semibold rounded-lg transition-colors flex items-center gap-1.5"
            >
              <StopCircle className="w-4 h-4 text-rose-400" />
              {isCancelling ? 'Cancelling...' : 'Cancel'}
            </button>
          )}
        </div>
      </div>

      {/* 12-Phase Lifecycle Stepper */}
      <div className="flex flex-col gap-2">
        <div className="flex items-center justify-between text-xs text-slate-400">
          <span className="font-semibold text-slate-300">Lifecycle Phases</span>
          {isRunning && (
            <span className="font-mono text-brand-light">
              Current Phase: {detail.currentPhase} ({phaseElapsed}s)
            </span>
          )}
        </div>

        <div
          data-testid="lifecycle"
          className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-6 gap-2 bg-slate-950/60 p-3 rounded-xl border border-slate-800/80"
        >
          {detail.phases.map((phase) => {
            const isPhaseActive = phase.state === 'active';
            const isDone = phase.state === 'done';
            const isError = phase.state === 'error';
            const isSkipped = phase.state === 'skipped';

            return (
              <div
                key={phase.name}
                data-testid={`phase-${phase.name}`}
                data-state={phase.state}
                className={`p-2.5 rounded-lg border text-xs flex flex-col justify-between gap-1 transition-all ${
                  isPhaseActive
                    ? 'bg-brand/20 border-brand text-brand-light shadow-md ring-1 ring-brand'
                    : isDone
                    ? 'bg-emerald-950/40 border-emerald-800/60 text-emerald-300'
                    : isError
                    ? 'bg-rose-950/50 border-rose-800 text-rose-300'
                    : isSkipped
                    ? 'bg-slate-900/40 border-slate-800/40 text-slate-600 line-through'
                    : 'bg-slate-900/60 border-slate-800 text-slate-400'
                }`}
              >
                <div className="flex items-center justify-between gap-1">
                  <span className="font-medium truncate capitalize">
                    {phase.name.replace(/_/g, ' ')}
                  </span>
                  {isPhaseActive && <Activity className="w-3 h-3 text-brand animate-spin shrink-0" />}
                  {isDone && <CheckCircle2 className="w-3 h-3 text-emerald-400 shrink-0" />}
                  {isError && <XCircle className="w-3 h-3 text-rose-400 shrink-0" />}
                </div>

                <div className="text-[10px] font-mono text-slate-400 flex items-center justify-between">
                  <span>{phase.state}</span>
                  {phase.startedAt && (
                    <span>{new Date(phase.startedAt).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' })}</span>
                  )}
                </div>
              </div>
            );
          })}
        </div>
      </div>

      {/* Compact Pipeline Visualization */}
      <div className="flex flex-col gap-2">
        <div className="flex items-center justify-between text-xs text-slate-400">
          <span className="font-semibold text-slate-300">Live Architecture State</span>
          <span className="text-[11px] text-slate-400">Truthful Overlay</span>
        </div>
        <div className="bg-slate-950 rounded-xl border border-slate-800 p-2 overflow-hidden min-h-[220px]">
          <PipelineVisualization progress={effectiveProgress} compact={true} />
        </div>
      </div>

      {/* Result Summary (shown when finished) */}
      {isFinished && (
        <div
          data-testid="exp-result"
          className="bg-slate-950 border border-slate-800 rounded-xl p-5 flex flex-col gap-5 shadow-lg"
        >
          {/* Banner */}
          <div
            className={`p-4 rounded-xl border flex items-start justify-between gap-4 ${
              detail.result === 'PASS'
                ? 'bg-emerald-950/40 border-emerald-800 text-emerald-300'
                : 'bg-rose-950/40 border-rose-800 text-rose-300'
            }`}
          >
            <div className="flex items-start gap-3">
              {detail.result === 'PASS' ? (
                <CheckCircle2 className="w-6 h-6 text-emerald-400 shrink-0 mt-0.5" />
              ) : (
                <XCircle className="w-6 h-6 text-rose-400 shrink-0 mt-0.5" />
              )}
              <div>
                <h3 className="text-base font-bold">
                  Experiment Result: {detail.result || 'INCOMPLETE'}
                </h3>
                {detail.failedCriteria.length > 0 ? (
                  <ul className="mt-2 text-xs space-y-1 list-disc list-inside text-rose-200">
                    {detail.failedCriteria.map((c, i) => (
                      <li key={i}>{c}</li>
                    ))}
                  </ul>
                ) : (
                  <p className="mt-1 text-xs text-emerald-200">
                    All resilience thresholds satisfied. System recovered within allowable constraints.
                  </p>
                )}
              </div>
            </div>

            <Link
              to={`/reports/${detail.id}`}
              className="px-4 py-2 bg-slate-800 hover:bg-slate-700 text-white rounded-lg text-xs font-semibold flex items-center gap-1.5 transition-colors shrink-0"
            >
              Full Report
              <ArrowRight className="w-3.5 h-3.5" />
            </Link>
          </div>

          {/* Criteria Evaluation Table */}
          <div className="overflow-x-auto">
            <table className="w-full text-left text-xs border border-slate-800 rounded-lg overflow-hidden">
              <thead className="bg-slate-900 text-slate-400 font-semibold uppercase tracking-wider text-[11px]">
                <tr>
                  <th className="py-2.5 px-3">Criterion</th>
                  <th className="py-2.5 px-3">Threshold</th>
                  <th className="py-2.5 px-3">Measured Actual</th>
                  <th className="py-2.5 px-3">Status</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800/60 font-mono text-slate-200">
                {criteria.map((c) => (
                  <tr key={c.name} className="hover:bg-slate-900/40">
                    <td className="py-2 px-3 font-sans font-medium text-slate-300">{c.name}</td>
                    <td className="py-2 px-3 text-slate-400">{c.threshold}</td>
                    <td className="py-2 px-3">{c.actual}</td>
                    <td className="py-2 px-3">
                      {c.passed ? (
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

          {/* Metric Tiles */}
          <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-3">
            <div className="bg-slate-900 border border-slate-800 p-3 rounded-lg flex flex-col gap-1">
              <span className="text-[11px] text-slate-400">Recovery Time</span>
              <span className="text-base font-bold font-mono text-slate-100">
                {detail.results.recoveryTimeSeconds !== null
                  ? `${detail.results.recoveryTimeSeconds.toFixed(1)} s`
                  : 'N/A'}
              </span>
            </div>
            <div className="bg-slate-900 border border-slate-800 p-3 rounded-lg flex flex-col gap-1">
              <span className="text-[11px] text-slate-400">Data Loss</span>
              <span className="text-base font-bold font-mono text-slate-100">
                {detail.results.dataLoss}
              </span>
            </div>
            <div className="bg-slate-900 border border-slate-800 p-3 rounded-lg flex flex-col gap-1">
              <span className="text-[11px] text-slate-400">Expected / Processed</span>
              <span className="text-sm font-bold font-mono text-slate-100 truncate">
                {detail.results.expectedRecords} / {detail.results.processedRecords}
              </span>
            </div>
            <div className="bg-slate-900 border border-slate-800 p-3 rounded-lg flex flex-col gap-1">
              <span className="text-[11px] text-slate-400">Duplicates</span>
              <span className="text-base font-bold font-mono text-slate-100">
                {detail.results.duplicates}
              </span>
            </div>
            <div className="bg-slate-900 border border-slate-800 p-3 rounded-lg flex flex-col gap-1">
              <span className="text-[11px] text-slate-400">Availability</span>
              <span className="text-base font-bold font-mono text-slate-100">
                {detail.results.availabilityPercentage !== null
                  ? `${detail.results.availabilityPercentage.toFixed(1)}%`
                  : 'N/A'}
              </span>
            </div>
            <div className="bg-slate-900 border border-slate-800 p-3 rounded-lg flex flex-col gap-1">
              <span className="text-[11px] text-slate-400">Throughput Degr.</span>
              <span className="text-base font-bold font-mono text-slate-100">
                {detail.results.throughputDegradationPercent !== null
                  ? `${detail.results.throughputDegradationPercent.toFixed(1)}%`
                  : 'N/A'}
              </span>
            </div>
          </div>
        </div>
      )}

      {/* Execution Events Log with LogViewer */}
      <div className="flex flex-col gap-3">
        <div className="flex flex-wrap items-center justify-between gap-3 text-xs">
          <span className="font-semibold text-slate-300">
            Real Execution Events ({events.length})
          </span>

          <div className="flex items-center gap-2">
            {/* Level filter */}
            <select
              aria-label="Filter log level"
              value={selectedLogLevel}
              onChange={(e) => setSelectedLogLevel(e.target.value)}
              className="bg-slate-800 border border-slate-700 rounded px-2 py-1 text-slate-200 text-xs focus:outline-none"
            >
              <option value="ALL">All Levels</option>
              <option value="INFO">INFO</option>
              <option value="WARNING">WARNING</option>
              <option value="ERROR">ERROR</option>
            </select>

            {/* Search */}
            <input
              type="text"
              placeholder="Search logs..."
              value={logSearchQuery}
              onChange={(e) => setLogSearchQuery(e.target.value)}
              className="bg-slate-800 border border-slate-700 rounded px-2 py-1 text-slate-200 text-xs focus:outline-none w-36"
            />

            {/* Download */}
            <button
              type="button"
              onClick={handleDownloadEvents}
              disabled={events.length === 0}
              className="p-1 rounded bg-slate-800 border border-slate-700 text-slate-300 hover:text-white disabled:opacity-40"
              title="Download events log"
            >
              <Download className="w-3.5 h-3.5" />
            </button>
          </div>
        </div>

        <LogViewer
          lines={formattedLogLines}
          label="Experiment execution log"
          maxHeight="16rem"
          emptyMessage="No execution events logged yet."
        />
      </div>

      {/* Cancel Confirmation Dialog */}
      <ConfirmDialog
        open={cancelModalOpen}
        onClose={() => setCancelModalOpen(false)}
        onConfirm={handleCancel}
        title="Cancel Chaos Experiment?"
        description="Cancelling reverts the fault immediately and initiates safe system recovery. The experiment will terminate as CANCELLED."
        confirmLabel="Yes, Revert Fault"
        confirmVariant="danger"
        loading={isCancelling}
      />
    </div>
  );
}
