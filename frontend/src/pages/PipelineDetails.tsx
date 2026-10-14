import React, { useEffect, useMemo, useRef, useState } from 'react';
import { useParams, useSearchParams, useNavigate } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';
import { getPipelineRun } from '../api/pipelines';
import { useRunLive } from '../hooks/useRunLive';
import { useRunEvents } from '../hooks/useRunEvents';
import { elapsedMs, stageLabel } from '../lib/format';
import { Tabs, TabList, Tab, TabPanel } from '../components/ui/Tabs';
import { ErrorState } from '../components/ui/ErrorState';
import { Skeleton } from '../components/ui/Skeleton';
import { RunHeader } from '../components/pipelines/details/RunHeader';
import { StageStepper } from '../components/pipelines/details/StageStepper';
import { ActiveProgress } from '../components/pipelines/details/ActiveProgress';
import { CountersGrid } from '../components/pipelines/details/CountersGrid';
import { ThroughputChart } from '../components/pipelines/details/ThroughputChart';
import { VizSlot } from '../components/pipelines/details/VizSlot';
import { ServicesPanel } from '../components/pipelines/details/ServicesPanel';
import { FailurePanel } from '../components/pipelines/details/FailurePanel';
import { SuccessSummary } from '../components/pipelines/details/SuccessSummary';
import { LogsTab } from '../components/pipelines/details/LogsTab';
import { ConfigTab } from '../components/pipelines/details/ConfigTab';
import { OutputTab } from '../components/pipelines/details/OutputTab';

const VALID_TABS = ['overview', 'logs', 'configuration', 'output'] as const;
type TabType = (typeof VALID_TABS)[number];

export default function PipelineDetails() {
  const { runId } = useParams<{ runId: string }>();
  const [searchParams, setSearchParams] = useSearchParams();
  const navigate = useNavigate();

  // Tab synchronization with URL query param ?tab=
  const tabParam = searchParams.get('tab') as TabType;
  const activeTab: TabType = VALID_TABS.includes(tabParam) ? tabParam : 'overview';

  const handleTabChange = (tabId: string) => {
    if (VALID_TABS.includes(tabId as TabType)) {
      setSearchParams({ tab: tabId }, { replace: true });
    }
  };

  // Fetch initial run record (metadata, name, yamlContent, etc.)
  const {
    data: run,
    isLoading: isLoadingRun,
    isError: isRunError,
    refetch: refetchRun,
  } = useQuery({
    queryKey: ['pipelineRun', runId],
    queryFn: () => getPipelineRun(runId!),
    enabled: Boolean(runId),
  });

  // Live real-time stream hook
  const { progress, connection, lastUpdatedAt, error: streamError } = useRunLive(runId);

  // Status computation
  const status = progress?.status || run?.status || 'pending';
  const isRunning = status === 'running';

  // Live ticker for elapsed time
  const [now, setNow] = useState(Date.now());
  useEffect(() => {
    if (!isRunning) return;
    const timer = setInterval(() => {
      setNow(Date.now());
    }, 1000);
    return () => clearInterval(timer);
  }, [isRunning]);

  const currentElapsedMs = useMemo(() => {
    const started = progress?.startedAt || run?.startedAt;
    const completed = progress?.completedAt || run?.finishedAt;
    return elapsedMs(started, completed, now);
  }, [progress?.startedAt, progress?.completedAt, run?.startedAt, run?.finishedAt, now]);

  // Hook for events (needed for failure panel details as well)
  const { events } = useRunEvents(runId, isRunning);

  // Accessibility: aria-live="polite" region
  // Announces ONLY stage transitions and terminal states, throttled to once per 2 seconds
  const [announcement, setAnnouncement] = useState('');
  const lastAnnounceTimeRef = useRef(0);
  const prevActiveStageRef = useRef<string | null>(null);
  const prevStatusRef = useRef<string | null>(null);

  useEffect(() => {
    if (!progress) return;
    const currentActive = progress.stages.find((s) => s.state === 'active')?.key || null;
    const currentStatus = progress.status;

    let nextText = '';

    if (currentActive && currentActive !== prevActiveStageRef.current) {
      nextText = `${stageLabel(currentActive)} processing started`;
      prevActiveStageRef.current = currentActive;
    } else if (currentStatus !== prevStatusRef.current) {
      if (currentStatus === 'succeeded') {
        nextText = 'Run succeeded';
      } else if (currentStatus === 'failed') {
        nextText = 'Run failed';
      } else if (currentStatus === 'cancelled') {
        nextText = 'Run cancelled';
      }
      prevStatusRef.current = currentStatus;
    }

    if (nextText) {
      const currentTime = Date.now();
      if (currentTime - lastAnnounceTimeRef.current >= 2000) {
        setAnnouncement(nextText);
        lastAnnounceTimeRef.current = currentTime;
      }
    }
  }, [progress]);

  // Loading skeleton state
  if (isLoadingRun && !progress) {
    return (
      <div className="max-w-7xl mx-auto space-y-6 animate-pulse">
        <Skeleton className="h-28 w-full rounded-xl" />
        <Skeleton className="h-10 w-64 rounded-lg" />
        <Skeleton className="h-64 w-full rounded-xl" />
      </div>
    );
  }

  // 404 / Error State
  if ((isRunError && !progress) || streamError === 'Run not found') {
    return (
      <div className="max-w-4xl mx-auto space-y-6">
        <ErrorState
          title="Run not found"
          description={streamError || 'The requested pipeline run could not be found or failed to load.'}
          onRetry={() => {
            refetchRun();
          }}
        />
      </div>
    );
  }

  const failingStageKey = progress?.stages.find((s) => s.state === 'error')?.key || null;

  return (
    <div className="max-w-7xl mx-auto space-y-6">
      {/* Visually hidden polite announcement region */}
      <div
        aria-live="polite"
        aria-atomic="true"
        className="sr-only"
        data-testid="a11y-announcement"
      >
        {announcement}
      </div>

      {/* Cancelled Banner if cancelled */}
      {status === 'cancelled' && (
        <div
          data-testid="cancelled-banner"
          className="p-3.5 bg-slate-800 border border-slate-700 text-slate-300 rounded-xl flex items-center justify-between text-sm"
        >
          <span>
            Run cancelled at{' '}
            <strong className="text-white">
              {progress?.completedAt
                ? new Date(progress.completedAt).toLocaleString()
                : progress?.updatedAt
                ? new Date(progress.updatedAt).toLocaleString()
                : 'unknown time'}
            </strong>
          </span>
        </div>
      )}

      {/* Run Header */}
      <RunHeader
        run={run || null}
        progress={progress}
        connection={connection}
        lastUpdatedAt={lastUpdatedAt}
        elapsedMs={currentElapsedMs}
        onRefreshRun={refetchRun}
      />

      {/* Tabs */}
      <Tabs value={activeTab} onChange={handleTabChange}>
        <TabList>
          <Tab id="overview" label="Overview" />
          <Tab id="logs" label="Logs" />
          <Tab id="configuration" label="Configuration" />
          <Tab id="output" label="Output" />
        </TabList>

        {/* Tab 1: Overview */}
        <TabPanel id="overview">
          <div className="space-y-6">
            {/* 1. Visualization Slot (UI-7 reserved) */}
            <VizSlot />

            {/* 2. Stage Stepper */}
            <StageStepper
              stages={progress?.stages || []}
              hdfsVerified={progress?.hdfs?.verified ?? null}
              failingStageKey={failingStageKey}
            />

            {/* 3. Failure Panel if failed */}
            {status === 'failed' && (
              <FailurePanel
                progress={progress}
                errorMessage={run?.errorMessage}
                events={events}
                onRerun={() => {
                  if (run?.yamlContent) {
                    navigate('/pipelines', { state: { yaml: run.yamlContent } });
                  }
                }}
                canRerun={Boolean(run?.yamlContent)}
              />
            )}

            {/* 4. Success Summary if succeeded */}
            {status === 'succeeded' && (
              <SuccessSummary
                progress={progress}
                outputPath={run?.actualOutputPath || run?.outputPath}
                elapsedMs={currentElapsedMs}
                onViewOutput={() => handleTabChange('output')}
              />
            )}

            {/* 5. Active Stage Progress (hidden if terminated and not streaming) */}
            {status !== 'succeeded' && status !== 'failed' && status !== 'cancelled' && (
              <ActiveProgress progress={progress} />
            )}

            {/* 6. Counters Grid */}
            <CountersGrid progress={progress} elapsedMs={currentElapsedMs} />

            {/* 7. Throughput Chart */}
            <ThroughputChart samples={progress?.throughput?.samples || []} />

            {/* 8. Infrastructure Services Panel */}
            <ServicesPanel progress={progress} />
          </div>
        </TabPanel>

        {/* Tab 2: Logs */}
        <TabPanel id="logs">
          <LogsTab
            runId={runId || ''}
            pipelineName={run?.name}
            isRunning={isRunning}
            startedAt={progress?.startedAt || run?.startedAt}
          />
        </TabPanel>

        {/* Tab 3: Configuration */}
        <TabPanel id="configuration">
          <ConfigTab yamlContent={run?.yamlContent} />
        </TabPanel>

        {/* Tab 4: Output */}
        <TabPanel id="output">
          <OutputTab
            runId={runId || ''}
            status={status}
            progress={progress}
            actualOutputPath={run?.actualOutputPath}
            fallbackOutputPath={run?.outputPath}
          />
        </TabPanel>
      </Tabs>
    </div>
  );
}
