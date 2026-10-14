import { describe, it, expect } from 'vitest';
import { renderHook } from '@testing-library/react';
import { toSceneModel, SceneContext } from '../features/viz/model';
import { useSceneContext } from '../features/viz/useSceneContext';
import { RunProgress, StageProgress } from '../types/runProgress';

function makeProgress(overrides: Partial<RunProgress> = {}): RunProgress {
  const defaultStages: StageProgress[] = [
    { key: 'source', state: 'pending', startedAt: null, finishedAt: null, error: null },
    { key: 'kafka', state: 'pending', startedAt: null, finishedAt: null, error: null },
    { key: 'spark', state: 'pending', startedAt: null, finishedAt: null, error: null },
    { key: 'hdfs', state: 'pending', startedAt: null, finishedAt: null, error: null },
  ];

  return {
    runId: 'run-test-1',
    status: 'running',
    mode: 'batch',
    startedAt: '2026-10-14T10:00:00Z',
    updatedAt: '2026-10-14T10:00:00Z',
    completedAt: null,
    stages: defaultStages,
    source: { type: 'csv', rowsTotal: 1000, rowsRead: 100 },
    kafka: { topic: 'test-topic', partitions: 1, published: 100, failed: 0, endOffsets: 100, consumerLag: null },
    spark: { appId: null, tasksTotal: 10, tasksDone: 2, recordsIn: null, recordsOut: null, durationMs: null, activeStage: null },
    hdfs: { path: '/data/output', filesWritten: 1, bytesWritten: 1024, rowsWritten: 100, verified: false },
    throughput: { recordsPerSecond: 50, samples: [] },
    etaSeconds: 30,
    errors: 0,
    fault: null,
    ...overrides,
  };
}

describe('toSceneModel - Table-driven unit tests (30+ cases)', () => {
  const defaultCtx: SceneContext = { peakLag: 1000, peakRate: 500 };

  // 1-5: Station state transitions for Source
  it('1. Source pending', () => {
    const p = makeProgress({ stages: [{ key: 'source', state: 'pending', startedAt: null, finishedAt: null, error: null }] });
    const model = toSceneModel(p, defaultCtx);
    expect(model.stations[0].state).toBe('pending');
    expect(model.stations[0].working).toBe(false);
  });

  it('2. Source active', () => {
    const p = makeProgress({ stages: [{ key: 'source', state: 'active', startedAt: null, finishedAt: null, error: null }] });
    const model = toSceneModel(p, defaultCtx);
    expect(model.stations[0].state).toBe('active');
    expect(model.stations[0].working).toBe(true);
  });

  it('3. Source done', () => {
    const p = makeProgress({ stages: [{ key: 'source', state: 'done', startedAt: null, finishedAt: null, error: null }] });
    const model = toSceneModel(p, defaultCtx);
    expect(model.stations[0].state).toBe('done');
    expect(model.stations[0].working).toBe(false);
  });

  it('4. Source error with note', () => {
    const p = makeProgress({ stages: [{ key: 'source', state: 'error', startedAt: null, finishedAt: null, error: 'Read timeout' }] });
    const model = toSceneModel(p, defaultCtx);
    expect(model.stations[0].state).toBe('error');
    expect(model.stations[0].note).toBe('Read timeout');
  });

  it('5. Source skipped', () => {
    const p = makeProgress({ stages: [{ key: 'source', state: 'skipped', startedAt: null, finishedAt: null, error: null }] });
    const model = toSceneModel(p, defaultCtx);
    expect(model.stations[0].state).toBe('skipped');
  });

  // 6-8: Source level and counter
  it('6. Source level drains correctly', () => {
    const p = makeProgress({ source: { type: 'csv', rowsTotal: 1000, rowsRead: 250 } });
    const model = toSceneModel(p, defaultCtx);
    expect(model.stations[0].level).toBeCloseTo(0.75, 4);
    expect(model.stations[0].counter).toBe('250 / 1,000');
  });

  it('7. Source null rowsTotal gives null level and counter handles nulls', () => {
    const p = makeProgress({ source: { type: 'csv', rowsTotal: null, rowsRead: null } });
    const model = toSceneModel(p, defaultCtx);
    expect(model.stations[0].level).toBeNull();
    expect(model.stations[0].counter).toBeNull();
  });

  it('8. Source zero rowsTotal gives null level', () => {
    const p = makeProgress({ source: { type: 'csv', rowsTotal: 0, rowsRead: 0 } });
    const model = toSceneModel(p, defaultCtx);
    expect(model.stations[0].level).toBeNull();
    expect(model.stations[0].counter).toBe('0 / 0');
  });

  // 9-13: Kafka Batch vs Streaming
  it('9. Kafka batch mode level & counter', () => {
    const p = makeProgress({
      mode: 'batch',
      source: { type: 'csv', rowsTotal: 1000, rowsRead: 400 },
      kafka: { topic: 'topic', partitions: 1, published: 400, failed: 0, endOffsets: null, consumerLag: null },
    });
    const model = toSceneModel(p, defaultCtx);
    expect(model.stations[1].level).toBeCloseTo(0.4, 4);
    expect(model.stations[1].counter).toBe('400');
    expect(model.stations[1].levelMeaning).toBe('Records published to the topic');
  });

  it('10. Kafka batch mode with failed records', () => {
    const p = makeProgress({
      mode: 'batch',
      source: { type: 'csv', rowsTotal: 1000, rowsRead: 500 },
      kafka: { topic: 'topic', partitions: 1, published: 500, failed: 15, endOffsets: null, consumerLag: null },
    });
    const model = toSceneModel(p, defaultCtx);
    expect(model.stations[1].counter).toBe('500, 15 failed');
  });

  it('11. Kafka streaming mode with consumer lag & peakLag > 0', () => {
    const p = makeProgress({
      mode: 'streaming',
      kafka: { topic: 'topic', partitions: 1, published: 2000, failed: 0, endOffsets: null, consumerLag: 500 },
    });
    const model = toSceneModel(p, { peakLag: 1000, peakRate: 100 });
    expect(model.stations[1].level).toBeCloseTo(0.5, 4);
    expect(model.stations[1].counter).toBe('lag: 500');
    expect(model.stations[1].levelMeaning).toContain('Backlog');
  });

  it('12. Kafka streaming mode with peakLag = 0', () => {
    const p = makeProgress({
      mode: 'streaming',
      kafka: { topic: 'topic', partitions: 1, published: 200, failed: 0, endOffsets: null, consumerLag: 50 },
    });
    const model = toSceneModel(p, { peakLag: 0, peakRate: 0 });
    expect(model.stations[1].level).toBe(0);
    expect(model.stations[1].counter).toBe('lag: 50');
  });

  it('13. Kafka streaming mode with null consumerLag sets note', () => {
    const p = makeProgress({
      mode: 'streaming',
      kafka: { topic: 'topic', partitions: 1, published: null, failed: 0, endOffsets: null, consumerLag: null },
    });
    const model = toSceneModel(p, defaultCtx);
    expect(model.stations[1].level).toBeNull();
    expect(model.stations[1].note).toBe('No consumer attached');
  });

  // 14-17: Spark with & without activeStage
  it('14. Spark with activeStage calculates level from tasksDone / tasksTotal', () => {
    const p = makeProgress({
      spark: {
        appId: 'app-1',
        tasksTotal: 100,
        tasksDone: 40,
        recordsIn: null,
        recordsOut: null,
        durationMs: null,
        activeStage: { id: 1, name: 'Stage 1', tasksTotal: 20, tasksDone: 10 },
      },
    });
    const model = toSceneModel(p, defaultCtx);
    expect(model.stations[2].level).toBeCloseTo(0.5, 4);
    expect(model.stations[2].counter).toBe('40 tasks done');
  });

  it('15. Spark without activeStage gives null level', () => {
    const p = makeProgress({
      spark: {
        appId: 'app-1',
        tasksTotal: 100,
        tasksDone: 25,
        recordsIn: null,
        recordsOut: null,
        durationMs: null,
        activeStage: null,
      },
    });
    const model = toSceneModel(p, defaultCtx);
    expect(model.stations[2].level).toBeNull();
    expect(model.stations[2].counter).toBe('25 tasks done');
  });

  it('16. Spark finished counter appends in/out records', () => {
    const p = makeProgress({
      status: 'succeeded',
      stages: [{ key: 'spark', state: 'done', startedAt: null, finishedAt: null, error: null }],
      spark: {
        appId: 'app-1',
        tasksTotal: 50,
        tasksDone: 50,
        recordsIn: 10000,
        recordsOut: 9800,
        durationMs: 5000,
        activeStage: null,
      },
    });
    const model = toSceneModel(p, defaultCtx);
    expect(model.stations[2].counter).toBe('50 tasks done, in 10,000 / out 9,800');
  });

  it('17. Spark active working state is true', () => {
    const p = makeProgress({
      stages: [{ key: 'spark', state: 'active', startedAt: null, finishedAt: null, error: null }],
    });
    const model = toSceneModel(p, defaultCtx);
    expect(model.stations[2].working).toBe(true);
  });

  // 18-20: HDFS verified and counters
  it('18. HDFS verified true gives level 1', () => {
    const p = makeProgress({
      hdfs: { path: '/out', filesWritten: 3, bytesWritten: 2048, rowsWritten: 500, verified: true },
    });
    const model = toSceneModel(p, defaultCtx);
    expect(model.stations[3].level).toBe(1);
    expect(model.stations[3].counter).toBe('3 files, 2 KB, rows: 500');
  });

  it('19. HDFS verified false / null gives level null (indeterminate)', () => {
    const p = makeProgress({
      hdfs: { path: '/out', filesWritten: 2, bytesWritten: 1024, rowsWritten: null, verified: false },
    });
    const model = toSceneModel(p, defaultCtx);
    expect(model.stations[3].level).toBeNull();
    expect(model.stations[3].counter).toBe('2 files, 1 KB');
  });

  it('20. HDFS only rowsWritten', () => {
    const p = makeProgress({
      hdfs: { path: '/out', filesWritten: null, bytesWritten: null, rowsWritten: 1200, verified: null },
    });
    const model = toSceneModel(p, defaultCtx);
    expect(model.stations[3].counter).toBe('rows: 1,200');
  });

  // 21-26: Fault mapping & phases
  it('21. Fault target kafka container_stop -> STOPPED', () => {
    const p = makeProgress({
      fault: {
        experimentId: 'exp-1',
        target: 'kafka',
        type: 'container_stop',
        phase: 'injected',
        injectedAt: '2026-10-14T10:00:00Z',
        durationSeconds: 15,
        recoverySeconds: null,
      },
    });
    const model = toSceneModel(p, defaultCtx);
    expect(model.stations[1].state).toBe('faulted');
    expect(model.stations[1].note).toBe('STOPPED (fault: container_stop)');
  });

  it('22. Fault target spark-worker cpu_stress -> DEGRADED', () => {
    const p = makeProgress({
      fault: {
        experimentId: 'exp-2',
        target: 'spark-worker-1',
        type: 'cpu_stress',
        phase: 'injected',
        injectedAt: '2026-10-14T10:00:00Z',
        durationSeconds: 10,
        recoverySeconds: null,
      },
    });
    const model = toSceneModel(p, defaultCtx);
    expect(model.stations[2].state).toBe('faulted');
    expect(model.stations[2].note).toBe('DEGRADED (fault: cpu_stress)');
  });

  it('23. Fault target namenode service_delay -> DEGRADED on HDFS', () => {
    const p = makeProgress({
      fault: {
        experimentId: 'exp-3',
        target: 'namenode',
        type: 'service_delay',
        phase: 'injected',
        injectedAt: '2026-10-14T10:00:00Z',
        durationSeconds: 20,
        recoverySeconds: null,
      },
    });
    const model = toSceneModel(p, defaultCtx);
    expect(model.stations[3].state).toBe('faulted');
    expect(model.stations[3].note).toBe('DEGRADED (fault: service_delay)');
  });

  it('24. Fault phase recovering', () => {
    const p = makeProgress({
      fault: {
        experimentId: 'exp-4',
        target: 'datanode',
        type: 'container_stop',
        phase: 'recovering',
        injectedAt: '2026-10-14T10:00:00Z',
        durationSeconds: 15,
        recoverySeconds: null,
      },
    });
    const model = toSceneModel(p, defaultCtx);
    expect(model.stations[3].state).toBe('recovering');
    expect(model.stations[3].note).toBe('Recovering...');
  });

  it('25. Fault phase recovered with recoverySeconds', () => {
    const p = makeProgress({
      stages: [{ key: 'kafka', state: 'active', startedAt: null, finishedAt: null, error: null }],
      fault: {
        experimentId: 'exp-5',
        target: 'kafka',
        type: 'container_stop',
        phase: 'recovered',
        injectedAt: '2026-10-14T10:00:00Z',
        durationSeconds: 15,
        recoverySeconds: 12.4,
      },
    });
    const model = toSceneModel(p, defaultCtx);
    expect(model.stations[1].state).toBe('active');
    expect(model.stations[1].note).toBe('Recovered in 12.4 s');
  });

  it('26. Fault target unknown (postgres) maps to no station', () => {
    const p = makeProgress({
      fault: {
        experimentId: 'exp-6',
        target: 'postgres',
        type: 'container_stop',
        phase: 'injected',
        injectedAt: '2026-10-14T10:00:00Z',
        durationSeconds: 10,
        recoverySeconds: null,
      },
    });
    const model = toSceneModel(p, defaultCtx);
    expect(model.stations.every((s) => s.state !== 'faulted')).toBe(true);
  });

  // 27-30: Pipes flow & blocked rules
  it('27. Pipe source to kafka flow scales with peakRate when active', () => {
    const p = makeProgress({
      stages: [
        { key: 'source', state: 'done', startedAt: null, finishedAt: null, error: null },
        { key: 'kafka', state: 'active', startedAt: null, finishedAt: null, error: null },
      ],
      throughput: { recordsPerSecond: 250, samples: [] },
    });
    const model = toSceneModel(p, { peakLag: 100, peakRate: 500 });
    expect(model.pipes[0].flow).toBeCloseTo(0.5, 2);
    expect(model.pipes[0].blocked).toBe(false);
  });

  it('28. Pipe downstream faulted blocks pipe from active/done upstream', () => {
    const p = makeProgress({
      stages: [
        { key: 'source', state: 'done', startedAt: null, finishedAt: null, error: null },
        { key: 'kafka', state: 'active', startedAt: null, finishedAt: null, error: null },
      ],
      fault: {
        experimentId: 'exp-7',
        target: 'kafka',
        type: 'container_stop',
        phase: 'injected',
        injectedAt: '2026-10-14T10:00:00Z',
        durationSeconds: 10,
        recoverySeconds: null,
      },
    });
    const model = toSceneModel(p, defaultCtx);
    expect(model.pipes[0].blocked).toBe(true);
  });

  it('29. Pipe upstream pending does NOT block even if downstream faulted', () => {
    const p = makeProgress({
      stages: [
        { key: 'source', state: 'pending', startedAt: null, finishedAt: null, error: null },
        { key: 'kafka', state: 'active', startedAt: null, finishedAt: null, error: null },
      ],
      fault: {
        experimentId: 'exp-8',
        target: 'kafka',
        type: 'container_stop',
        phase: 'injected',
        injectedAt: '2026-10-14T10:00:00Z',
        durationSeconds: 10,
        recoverySeconds: null,
      },
    });
    const model = toSceneModel(p, defaultCtx);
    expect(model.pipes[0].blocked).toBe(false);
  });

  it('30. Downstream stages flow is 0.5 when active, 0 when inactive', () => {
    const p = makeProgress({
      stages: [
        { key: 'spark', state: 'active', startedAt: null, finishedAt: null, error: null },
        { key: 'hdfs', state: 'pending', startedAt: null, finishedAt: null, error: null },
      ],
    });
    const model = toSceneModel(p, defaultCtx);
    expect(model.pipes[1].flow).toBe(0.5); // kafka to spark
    expect(model.pipes[2].flow).toBe(0); // spark to hdfs
  });

  // 31-35: Completion mapping & Announce priority
  it('31. Completion status mappings', () => {
    expect(toSceneModel(makeProgress({ status: 'succeeded' }), defaultCtx).completion).toBe('succeeded');
    expect(toSceneModel(makeProgress({ status: 'failed' }), defaultCtx).completion).toBe('failed');
    expect(toSceneModel(makeProgress({ status: 'cancelled' }), defaultCtx).completion).toBe('cancelled');
    expect(toSceneModel(makeProgress({ status: 'running', mode: 'batch' }), defaultCtx).completion).toBe('running');
    expect(toSceneModel(makeProgress({ status: 'running', mode: 'streaming' }), defaultCtx).completion).toBe('live');
  });

  it('32. Announce priority: error beats fault', () => {
    const p = makeProgress({
      status: 'failed',
      stages: [{ key: 'source', state: 'error', startedAt: null, finishedAt: null, error: 'Disk full' }],
      fault: {
        experimentId: 'e-1',
        target: 'kafka',
        type: 'container_stop',
        phase: 'injected',
        injectedAt: '2026-10-14T10:00:00Z',
        durationSeconds: 10,
        recoverySeconds: null,
      },
    });
    const model = toSceneModel(p, defaultCtx);
    expect(model.announce).toContain('failed');
  });

  it('33. Announce priority: fault beats completion and active stage', () => {
    const p = makeProgress({
      status: 'running',
      stages: [{ key: 'spark', state: 'active', startedAt: null, finishedAt: null, error: null }],
      fault: {
        experimentId: 'e-2',
        target: 'spark-master',
        type: 'cpu_stress',
        phase: 'injected',
        injectedAt: '2026-10-14T10:00:00Z',
        durationSeconds: 10,
        recoverySeconds: null,
      },
    });
    const model = toSceneModel(p, defaultCtx);
    expect(model.announce).toBe('Fault injected on Spark: cpu_stress.');
  });

  it('34. Announce stage start when active without fault/error', () => {
    const p = makeProgress({
      status: 'running',
      stages: [
        { key: 'source', state: 'done', startedAt: null, finishedAt: null, error: null },
        { key: 'kafka', state: 'active', startedAt: null, finishedAt: null, error: null },
      ],
    });
    const model = toSceneModel(p, defaultCtx);
    expect(model.announce).toBe('Kafka processing started.');
  });

  it('35. Determinism: identical outputs on identical inputs', () => {
    const p = makeProgress();
    const out1 = toSceneModel(p, defaultCtx);
    const out2 = toSceneModel(p, defaultCtx);
    expect(out1).toEqual(out2);
  });

  it('36. No output contains NaN or Infinity', () => {
    const p = makeProgress({
      source: { type: 'csv', rowsTotal: 0, rowsRead: 0 },
      throughput: { recordsPerSecond: 0, samples: [] },
    });
    const json = JSON.stringify(toSceneModel(p, { peakLag: 0, peakRate: 0 }));
    expect(json).not.toContain('NaN');
    expect(json).not.toContain('Infinity');
  });

  // 200 random snapshot property test
  it('37. Property test: 200 random snapshots adhere to invariants', () => {
    const states: ('pending' | 'active' | 'done' | 'error' | 'skipped')[] = ['pending', 'active', 'done', 'error', 'skipped'];
    const targets = ['kafka', 'spark-master', 'namenode', 'datanode', 'postgres', 'unknown'];
    const faultTypes = ['container_stop', 'cpu_stress', 'service_delay', 'network_disruption'];
    const phases: ('injected' | 'recovering' | 'recovered')[] = ['injected', 'recovering', 'recovered'];

    for (let i = 0; i < 200; i++) {
      const rowsTotal = Math.random() > 0.2 ? Math.floor(Math.random() * 50000) : null;
      const rowsRead = rowsTotal != null ? Math.floor(Math.random() * (rowsTotal + 500)) : null;
      const consumerLag = Math.random() > 0.3 ? Math.floor(Math.random() * 1000) : null;
      const peakLag = Math.floor(Math.random() * 2000);
      const peakRate = Math.floor(Math.random() * 5000);
      const rps = Math.random() > 0.2 ? Math.floor(Math.random() * 3000) : null;

      const hasFault = Math.random() > 0.5;
      const fault = hasFault
        ? {
            experimentId: `exp-${i}`,
            target: targets[i % targets.length],
            type: faultTypes[i % faultTypes.length],
            phase: phases[i % phases.length],
            injectedAt: '2026-10-14T10:00:00Z',
            durationSeconds: 15,
            recoverySeconds: Math.random() > 0.5 ? Math.random() * 20 : null,
          }
        : null;

      const p = makeProgress({
        mode: i % 2 === 0 ? 'batch' : 'streaming',
        stages: [
          { key: 'source', state: states[(i * 3) % states.length], startedAt: null, finishedAt: null, error: null },
          { key: 'kafka', state: states[(i * 7) % states.length], startedAt: null, finishedAt: null, error: null },
          { key: 'spark', state: states[(i * 11) % states.length], startedAt: null, finishedAt: null, error: null },
          { key: 'hdfs', state: states[(i * 13) % states.length], startedAt: null, finishedAt: null, error: null },
        ],
        source: { type: 'csv', rowsTotal, rowsRead },
        kafka: { topic: 'topic', partitions: 1, published: rowsRead, failed: i % 5 === 0 ? 3 : 0, endOffsets: null, consumerLag },
        spark: {
          appId: 'app',
          tasksTotal: 100,
          tasksDone: Math.floor(Math.random() * 100),
          recordsIn: 1000,
          recordsOut: 900,
          durationMs: 2000,
          activeStage: Math.random() > 0.5 ? { id: 1, name: 'S1', tasksTotal: 20, tasksDone: Math.floor(Math.random() * 20) } : null,
        },
        hdfs: {
          path: '/out',
          filesWritten: Math.floor(Math.random() * 5),
          bytesWritten: Math.floor(Math.random() * 1000000),
          rowsWritten: rowsRead,
          verified: Math.random() > 0.5,
        },
        throughput: { recordsPerSecond: rps, samples: [] },
        fault,
      });

      const model = toSceneModel(p, { peakLag, peakRate });

      expect(model.stations).toHaveLength(4);
      expect(model.pipes).toHaveLength(3);

      for (const st of model.stations) {
        if (st.level !== null) {
          expect(st.level).toBeGreaterThanOrEqual(0);
          expect(st.level).toBeLessThanOrEqual(1);
          expect(Number.isNaN(st.level)).toBe(false);
        }
      }

      for (const pipe of model.pipes) {
        expect(pipe.flow).toBeGreaterThanOrEqual(0);
        expect(pipe.flow).toBeLessThanOrEqual(1);
        expect(Number.isNaN(pipe.flow)).toBe(false);
      }
    }
  });
});

describe('useSceneContext', () => {
  it('tracks peakLag and peakRate, updating on increases and resetting on runId change', () => {
    let p = makeProgress({
      runId: 'run-alpha',
      kafka: { topic: 't', partitions: 1, published: 10, failed: 0, endOffsets: null, consumerLag: 100 },
      throughput: { recordsPerSecond: 50, samples: [] },
    });

    const { result, rerender } = renderHook(({ prog }) => useSceneContext(prog), {
      initialProps: { prog: p },
    });

    expect(result.current).toEqual({ peakLag: 100, peakRate: 50 });

    // Higher values increase the peaks
    p = makeProgress({
      runId: 'run-alpha',
      kafka: { topic: 't', partitions: 1, published: 20, failed: 0, endOffsets: null, consumerLag: 250 },
      throughput: { recordsPerSecond: 120, samples: [] },
    });
    rerender({ prog: p });
    expect(result.current).toEqual({ peakLag: 250, peakRate: 120 });

    // Lower values keep the peaks
    p = makeProgress({
      runId: 'run-alpha',
      kafka: { topic: 't', partitions: 1, published: 30, failed: 0, endOffsets: null, consumerLag: 50 },
      throughput: { recordsPerSecond: 40, samples: [] },
    });
    rerender({ prog: p });
    expect(result.current).toEqual({ peakLag: 250, peakRate: 120 });

    // RunId change resets the peaks
    p = makeProgress({
      runId: 'run-beta',
      kafka: { topic: 't', partitions: 1, published: 5, failed: 0, endOffsets: null, consumerLag: 30 },
      throughput: { recordsPerSecond: 15, samples: [] },
    });
    rerender({ prog: p });
    expect(result.current).toEqual({ peakLag: 30, peakRate: 15 });
  });
});
