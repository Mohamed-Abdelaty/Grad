import React, { useState } from 'react';
import { PipelineVisualization } from './PipelineVisualization';
import { RunProgress } from '../../types/runProgress';

export type PresetKey =
  | 'pending'
  | 'kafka-40'
  | 'spark-active'
  | 'spark-unknown'
  | 'hdfs-writing'
  | 'succeeded'
  | 'failed-spark'
  | 'fault-injected'
  | 'fault-recovering'
  | 'fault-recovered'
  | 'streaming-lag'
  | 'cancelled';

const BASE_PROGRESS: RunProgress = {
  runId: 'dev-run-001',
  status: 'running',
  mode: 'batch',
  startedAt: new Date(Date.now() - 60000).toISOString(),
  updatedAt: new Date().toISOString(),
  completedAt: null,
  stages: [
    { key: 'source', state: 'pending', startedAt: null, finishedAt: null, error: null },
    { key: 'kafka', state: 'pending', startedAt: null, finishedAt: null, error: null },
    { key: 'spark', state: 'pending', startedAt: null, finishedAt: null, error: null },
    { key: 'hdfs', state: 'pending', startedAt: null, finishedAt: null, error: null },
  ],
  source: { type: 'csv', rowsTotal: 10000, rowsRead: 0 },
  kafka: { topic: 'orders', partitions: 3, published: 0, failed: 0, endOffsets: 0, consumerLag: null },
  spark: { appId: null, tasksTotal: null, tasksDone: null, recordsIn: null, recordsOut: null, durationMs: null, activeStage: null },
  hdfs: { path: '/data/orders', filesWritten: null, bytesWritten: null, rowsWritten: null, verified: null },
  throughput: { recordsPerSecond: null, samples: [] },
  etaSeconds: null,
  errors: 0,
  fault: null,
};

export const PRESETS: Record<PresetKey, { label: string; progress: RunProgress }> = {
  pending: {
    label: 'Pending',
    progress: {
      ...BASE_PROGRESS,
      status: 'pending',
      stages: [
        { key: 'source', state: 'pending', startedAt: null, finishedAt: null, error: null },
        { key: 'kafka', state: 'pending', startedAt: null, finishedAt: null, error: null },
        { key: 'spark', state: 'pending', startedAt: null, finishedAt: null, error: null },
        { key: 'hdfs', state: 'pending', startedAt: null, finishedAt: null, error: null },
      ],
    },
  },
  'kafka-40': {
    label: 'Kafka 40%',
    progress: {
      ...BASE_PROGRESS,
      stages: [
        { key: 'source', state: 'done', startedAt: null, finishedAt: null, error: null },
        { key: 'kafka', state: 'active', startedAt: null, finishedAt: null, error: null },
        { key: 'spark', state: 'pending', startedAt: null, finishedAt: null, error: null },
        { key: 'hdfs', state: 'pending', startedAt: null, finishedAt: null, error: null },
      ],
      source: { type: 'csv', rowsTotal: 10000, rowsRead: 10000 },
      kafka: { topic: 'orders', partitions: 3, published: 4000, failed: 0, endOffsets: 4000, consumerLag: null },
      throughput: { recordsPerSecond: 250, samples: [] },
    },
  },
  'spark-active': {
    label: 'Spark active with 3 of 8 tasks',
    progress: {
      ...BASE_PROGRESS,
      stages: [
        { key: 'source', state: 'done', startedAt: null, finishedAt: null, error: null },
        { key: 'kafka', state: 'done', startedAt: null, finishedAt: null, error: null },
        { key: 'spark', state: 'active', startedAt: null, finishedAt: null, error: null },
        { key: 'hdfs', state: 'pending', startedAt: null, finishedAt: null, error: null },
      ],
      source: { type: 'csv', rowsTotal: 10000, rowsRead: 10000 },
      kafka: { topic: 'orders', partitions: 3, published: 10000, failed: 0, endOffsets: 10000, consumerLag: null },
      spark: {
        appId: 'app-2026-001',
        tasksTotal: 8,
        tasksDone: 3,
        recordsIn: null,
        recordsOut: null,
        durationMs: 4500,
        activeStage: { id: 1, name: 'Stage 1', tasksTotal: 8, tasksDone: 3 },
      },
      throughput: { recordsPerSecond: 180, samples: [] },
    },
  },
  'spark-unknown': {
    label: 'Spark unknown',
    progress: {
      ...BASE_PROGRESS,
      stages: [
        { key: 'source', state: 'done', startedAt: null, finishedAt: null, error: null },
        { key: 'kafka', state: 'done', startedAt: null, finishedAt: null, error: null },
        { key: 'spark', state: 'active', startedAt: null, finishedAt: null, error: null },
        { key: 'hdfs', state: 'pending', startedAt: null, finishedAt: null, error: null },
      ],
      source: { type: 'csv', rowsTotal: 10000, rowsRead: 10000 },
      kafka: { topic: 'orders', partitions: 3, published: 10000, failed: 0, endOffsets: 10000, consumerLag: null },
      spark: { appId: 'app-2026-002', tasksTotal: null, tasksDone: null, recordsIn: null, recordsOut: null, durationMs: null, activeStage: null },
    },
  },
  'hdfs-writing': {
    label: 'HDFS writing',
    progress: {
      ...BASE_PROGRESS,
      stages: [
        { key: 'source', state: 'done', startedAt: null, finishedAt: null, error: null },
        { key: 'kafka', state: 'done', startedAt: null, finishedAt: null, error: null },
        { key: 'spark', state: 'done', startedAt: null, finishedAt: null, error: null },
        { key: 'hdfs', state: 'active', startedAt: null, finishedAt: null, error: null },
      ],
      source: { type: 'csv', rowsTotal: 10000, rowsRead: 10000 },
      kafka: { topic: 'orders', partitions: 3, published: 10000, failed: 0, endOffsets: 10000, consumerLag: null },
      spark: { appId: 'app-2026-003', tasksTotal: 8, tasksDone: 8, recordsIn: 10000, recordsOut: 9950, durationMs: 12000, activeStage: null },
      hdfs: { path: '/data/orders', filesWritten: 3, bytesWritten: 2048576, rowsWritten: 5000, verified: false },
    },
  },
  succeeded: {
    label: 'Succeeded',
    progress: {
      ...BASE_PROGRESS,
      status: 'succeeded',
      completedAt: new Date().toISOString(),
      stages: [
        { key: 'source', state: 'done', startedAt: null, finishedAt: null, error: null },
        { key: 'kafka', state: 'done', startedAt: null, finishedAt: null, error: null },
        { key: 'spark', state: 'done', startedAt: null, finishedAt: null, error: null },
        { key: 'hdfs', state: 'done', startedAt: null, finishedAt: null, error: null },
      ],
      source: { type: 'csv', rowsTotal: 10000, rowsRead: 10000 },
      kafka: { topic: 'orders', partitions: 3, published: 10000, failed: 0, endOffsets: 10000, consumerLag: null },
      spark: { appId: 'app-2026-003', tasksTotal: 8, tasksDone: 8, recordsIn: 10000, recordsOut: 9950, durationMs: 12000, activeStage: null },
      hdfs: { path: '/data/orders', filesWritten: 5, bytesWritten: 5242880, rowsWritten: 9950, verified: true },
    },
  },
  'failed-spark': {
    label: 'Failed at Spark',
    progress: {
      ...BASE_PROGRESS,
      status: 'failed',
      completedAt: new Date().toISOString(),
      stages: [
        { key: 'source', state: 'done', startedAt: null, finishedAt: null, error: null },
        { key: 'kafka', state: 'done', startedAt: null, finishedAt: null, error: null },
        { key: 'spark', state: 'error', startedAt: null, finishedAt: null, error: 'Out of memory' },
        { key: 'hdfs', state: 'skipped', startedAt: null, finishedAt: null, error: null },
      ],
      source: { type: 'csv', rowsTotal: 10000, rowsRead: 10000 },
      kafka: { topic: 'orders', partitions: 3, published: 10000, failed: 0, endOffsets: 10000, consumerLag: null },
      spark: { appId: 'app-2026-004', tasksTotal: 8, tasksDone: 2, recordsIn: 10000, recordsOut: null, durationMs: null, activeStage: null },
    },
  },
  'fault-injected': {
    label: 'Fault injected on Kafka',
    progress: {
      ...BASE_PROGRESS,
      stages: [
        { key: 'source', state: 'done', startedAt: null, finishedAt: null, error: null },
        { key: 'kafka', state: 'active', startedAt: null, finishedAt: null, error: null },
        { key: 'spark', state: 'pending', startedAt: null, finishedAt: null, error: null },
        { key: 'hdfs', state: 'pending', startedAt: null, finishedAt: null, error: null },
      ],
      source: { type: 'csv', rowsTotal: 10000, rowsRead: 10000 },
      kafka: { topic: 'orders', partitions: 3, published: 2500, failed: 12, endOffsets: 2500, consumerLag: null },
      fault: {
        experimentId: 'exp-fault-1',
        target: 'kafka',
        type: 'container_stop',
        phase: 'injected',
        injectedAt: new Date().toISOString(),
        durationSeconds: 15,
        recoverySeconds: null,
      },
    },
  },
  'fault-recovering': {
    label: 'Fault recovering',
    progress: {
      ...BASE_PROGRESS,
      stages: [
        { key: 'source', state: 'done', startedAt: null, finishedAt: null, error: null },
        { key: 'kafka', state: 'active', startedAt: null, finishedAt: null, error: null },
        { key: 'spark', state: 'pending', startedAt: null, finishedAt: null, error: null },
        { key: 'hdfs', state: 'pending', startedAt: null, finishedAt: null, error: null },
      ],
      source: { type: 'csv', rowsTotal: 10000, rowsRead: 10000 },
      kafka: { topic: 'orders', partitions: 3, published: 2500, failed: 12, endOffsets: 2500, consumerLag: null },
      fault: {
        experimentId: 'exp-fault-2',
        target: 'kafka',
        type: 'container_stop',
        phase: 'recovering',
        injectedAt: new Date(Date.now() - 10000).toISOString(),
        durationSeconds: 15,
        recoverySeconds: null,
      },
    },
  },
  'fault-recovered': {
    label: 'Fault recovered 12.4 s',
    progress: {
      ...BASE_PROGRESS,
      stages: [
        { key: 'source', state: 'done', startedAt: null, finishedAt: null, error: null },
        { key: 'kafka', state: 'active', startedAt: null, finishedAt: null, error: null },
        { key: 'spark', state: 'pending', startedAt: null, finishedAt: null, error: null },
        { key: 'hdfs', state: 'pending', startedAt: null, finishedAt: null, error: null },
      ],
      source: { type: 'csv', rowsTotal: 10000, rowsRead: 10000 },
      kafka: { topic: 'orders', partitions: 3, published: 6000, failed: 12, endOffsets: 6000, consumerLag: null },
      fault: {
        experimentId: 'exp-fault-3',
        target: 'kafka',
        type: 'container_stop',
        phase: 'recovered',
        injectedAt: new Date(Date.now() - 20000).toISOString(),
        durationSeconds: 15,
        recoverySeconds: 12.4,
      },
    },
  },
  'streaming-lag': {
    label: 'Streaming with lag',
    progress: {
      ...BASE_PROGRESS,
      mode: 'streaming',
      stages: [
        { key: 'source', state: 'done', startedAt: null, finishedAt: null, error: null },
        { key: 'kafka', state: 'active', startedAt: null, finishedAt: null, error: null },
        { key: 'spark', state: 'active', startedAt: null, finishedAt: null, error: null },
        { key: 'hdfs', state: 'active', startedAt: null, finishedAt: null, error: null },
      ],
      kafka: { topic: 'events', partitions: 4, published: 50000, failed: 0, endOffsets: 50000, consumerLag: 450 },
      spark: { appId: 'streaming-app', tasksTotal: 10, tasksDone: 6, recordsIn: 48000, recordsOut: 47500, durationMs: 40000, activeStage: null },
      hdfs: { path: '/stream/out', filesWritten: 12, bytesWritten: 10485760, rowsWritten: 47500, verified: false },
      throughput: { recordsPerSecond: 350, samples: [] },
    },
  },
  cancelled: {
    label: 'Cancelled',
    progress: {
      ...BASE_PROGRESS,
      status: 'cancelled',
      completedAt: new Date().toISOString(),
      stages: [
        { key: 'source', state: 'done', startedAt: null, finishedAt: null, error: null },
        { key: 'kafka', state: 'active', startedAt: null, finishedAt: null, error: null },
        { key: 'spark', state: 'pending', startedAt: null, finishedAt: null, error: null },
        { key: 'hdfs', state: 'pending', startedAt: null, finishedAt: null, error: null },
      ],
    },
  },
};

export default function Playground() {
  const [selectedPreset, setSelectedPreset] = useState<PresetKey>('spark-active');
  const [isSimulatedReducedMotion, setIsSimulatedReducedMotion] = useState(false);

  const currentProgress = PRESETS[selectedPreset].progress;

  return (
    <div
      data-testid="viz-playground"
      className="min-h-screen bg-slate-950 text-slate-100 p-6 flex flex-col gap-6"
    >
      <div className="flex flex-col md:flex-row items-start md:items-center justify-between gap-4 border-b border-slate-800 pb-4">
        <div>
          <h1 className="text-xl font-bold">Visualization State Playground</h1>
          <p className="text-xs text-slate-400">
            Dev-only visual testing tool for all pipeline states & animations.
          </p>
        </div>

        <div className="flex items-center gap-2">
          <button
            type="button"
            data-testid="btn-reduced-motion"
            onClick={() => setIsSimulatedReducedMotion((p) => !p)}
            className={`px-3 py-1.5 text-xs font-medium rounded-lg border transition-colors ${
              isSimulatedReducedMotion
                ? 'bg-indigo-600 border-indigo-500 text-white'
                : 'bg-slate-900 border-slate-700 text-slate-300 hover:bg-slate-800'
            }`}
          >
            {isSimulatedReducedMotion ? 'Reduced Motion: ON' : 'Reduced Motion: OFF'}
          </button>
        </div>
      </div>

      {/* Preset selection grid */}
      <div className="flex flex-wrap gap-2">
        {(Object.keys(PRESETS) as PresetKey[]).map((key) => {
          const item = PRESETS[key];
          const isSelected = selectedPreset === key;
          return (
            <button
              key={key}
              type="button"
              data-testid={`preset-${key}`}
              onClick={() => setSelectedPreset(key)}
              className={`px-3 py-1.5 text-xs font-medium rounded-lg border transition-all ${
                isSelected
                  ? 'bg-indigo-600 border-indigo-500 text-white shadow-md'
                  : 'bg-slate-900 border-slate-800 text-slate-300 hover:bg-slate-800 hover:border-slate-700'
              }`}
            >
              {item.label}
            </button>
          );
        })}
      </div>

      {/* Visualization Canvas */}
      <div className="max-w-5xl w-full mx-auto">
        <PipelineVisualization progress={currentProgress} />
      </div>

      {/* State Inspector */}
      <div className="max-w-5xl w-full mx-auto p-4 rounded-xl bg-slate-900/50 border border-slate-800 text-xs text-slate-300">
        <div className="font-semibold text-slate-200 mb-2">Selected Preset: {PRESETS[selectedPreset].label}</div>
        <div className="font-mono text-[11px] text-slate-400">
          Run ID: {currentProgress.runId} | Status: {currentProgress.status} | Mode: {currentProgress.mode}
        </div>
      </div>
    </div>
  );
}
