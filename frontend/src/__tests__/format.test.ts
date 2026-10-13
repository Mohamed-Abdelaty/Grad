import { describe, it, expect } from 'vitest';
import {
  formatCount,
  formatBytes,
  formatDuration,
  formatRate,
  elapsedMs,
  formatEventMessage,
  activeStageSummary,
  stageLabel,
  stageStateLabel,
  EM_DASH,
} from '../lib/format';
import { RunProgress, RunEvent } from '../types/runProgress';

describe('format.ts utilities', () => {
  describe('formatCount', () => {
    it('formats null and undefined as em dash', () => {
      expect(formatCount(null)).toBe(EM_DASH);
      expect(formatCount(undefined)).toBe(EM_DASH);
      expect(formatCount(NaN)).toBe(EM_DASH);
    });

    it('formats 0 as "0"', () => {
      expect(formatCount(0)).toBe('0');
    });

    it('formats positive numbers with thousands separators', () => {
      expect(formatCount(1234567)).toBe('1,234,567');
      expect(formatCount(42)).toBe('42');
    });
  });

  describe('formatBytes', () => {
    it('formats null and negative numbers as em dash', () => {
      expect(formatBytes(null)).toBe(EM_DASH);
      expect(formatBytes(undefined)).toBe(EM_DASH);
      expect(formatBytes(-10)).toBe(EM_DASH);
    });

    it('formats bytes with 1024 base', () => {
      expect(formatBytes(0)).toBe('0 B');
      expect(formatBytes(512)).toBe('512 B');
      expect(formatBytes(1536)).toBe('1.5 KB');
      expect(formatBytes(12.3 * 1024 * 1024)).toBe('12.3 MB');
      expect(formatBytes(1.2 * 1024 * 1024 * 1024)).toBe('1.2 GB');
    });
  });

  describe('formatDuration', () => {
    it('formats null and negative values as em dash', () => {
      expect(formatDuration(null)).toBe(EM_DASH);
      expect(formatDuration(-100)).toBe(EM_DASH);
    });

    it('formats sub-second, second, minute, hour intervals', () => {
      expect(formatDuration(0)).toBe('0 ms');
      expect(formatDuration(850)).toBe('850 ms');
      expect(formatDuration(12400)).toBe('12.4 s');
      expect(formatDuration(185000)).toBe('3 m 05 s');
      expect(formatDuration(3720000)).toBe('1 h 02 m');
    });
  });

  describe('formatRate', () => {
    it('formats rates or em dash', () => {
      expect(formatRate(null)).toBe(EM_DASH);
      expect(formatRate(-5)).toBe(EM_DASH);
      expect(formatRate(0)).toBe('0 rec/s');
      expect(formatRate(1250.4)).toBe('1,250 rec/s');
    });
  });

  describe('elapsedMs', () => {
    it('uses server timestamps and completedAt if available', () => {
      expect(elapsedMs(null, null)).toBe(0);
      const start = '2026-10-13T10:00:00Z';
      const end = '2026-10-13T10:01:30Z';
      expect(elapsedMs(start, end)).toBe(90000);
      // Never negative
      expect(elapsedMs(end, start)).toBe(0);
    });
  });

  describe('stageLabel and stageStateLabel', () => {
    it('returns expected display labels', () => {
      expect(stageLabel('source')).toBe('Source');
      expect(stageLabel('kafka')).toBe('Kafka');
      expect(stageLabel('spark')).toBe('Spark');
      expect(stageLabel('hdfs')).toBe('HDFS');

      expect(stageStateLabel('pending')).toBe('Pending');
      expect(stageStateLabel('active')).toBe('Active');
      expect(stageStateLabel('done')).toBe('Completed');
      expect(stageStateLabel('error')).toBe('Failed');
      expect(stageStateLabel('skipped')).toBe('Skipped');
    });
  });

  describe('formatEventMessage', () => {
    it('maps known event types to friendly sentences', () => {
      const e1: RunEvent = {
        seq: 1,
        eventType: 'source_start',
        payload: {},
        ts: '2026-10-13T10:00:00Z',
      };
      expect(formatEventMessage(e1)).toEqual({
        level: 'info',
        text: 'Source ingestion started.',
      });

      const e2: RunEvent = {
        seq: 2,
        eventType: 'spark_done',
        payload: { recordsIn: 20, recordsOut: 17, durationMs: 36700 },
        ts: '2026-10-13T10:00:00Z',
      };
      expect(formatEventMessage(e2).text).toContain('Spark job finished: 20 rows in, 17 rows out, 36.7 s');

      const eErr: RunEvent = {
        seq: 3,
        eventType: 'run_failed',
        payload: { error: 'Broken node' },
        ts: '2026-10-13T10:00:00Z',
      };
      expect(formatEventMessage(eErr).level).toBe('error');
    });

    it('falls back to eventType plus compact JSON for unknown events', () => {
      const eUnknown: RunEvent = {
        seq: 4,
        eventType: 'custom_cluster_event',
        payload: { cluster: 'prod-1' },
        ts: '2026-10-13T10:00:00Z',
      };
      const res = formatEventMessage(eUnknown);
      expect(res.level).toBe('info');
      expect(res.text).toContain('custom_cluster_event');
      expect(res.text).toContain('"cluster":"prod-1"');
    });
  });

  describe('activeStageSummary', () => {
    it('returns Idle when no active stage exists', () => {
      const p: Partial<RunProgress> = {
        stages: [
          { key: 'source', state: 'done', startedAt: null, finishedAt: null, error: null },
          { key: 'kafka', state: 'done', startedAt: null, finishedAt: null, error: null },
        ],
      };
      expect(activeStageSummary(p as RunProgress)).toEqual({
        label: 'Idle',
        ratio: null,
        indeterminate: false,
      });
    });

    it('calculates source ratio when rowsTotal is known', () => {
      const p: Partial<RunProgress> = {
        stages: [{ key: 'source', state: 'active', startedAt: null, finishedAt: null, error: null }],
        source: { type: 'csv', rowsTotal: 100, rowsRead: 50 },
      };
      expect(activeStageSummary(p as RunProgress)).toEqual({
        label: 'Source: reading records',
        ratio: 0.5,
        indeterminate: false,
      });
    });

    it('returns indeterminate when source rowsTotal is unknown', () => {
      const p: Partial<RunProgress> = {
        stages: [{ key: 'source', state: 'active', startedAt: null, finishedAt: null, error: null }],
        source: { type: 'csv', rowsTotal: null, rowsRead: 50 },
      };
      expect(activeStageSummary(p as RunProgress)).toEqual({
        label: 'Source: reading records',
        ratio: null,
        indeterminate: true,
      });
    });

    it('calculates kafka ratio using source.rowsTotal', () => {
      const p: Partial<RunProgress> = {
        stages: [{ key: 'kafka', state: 'active', startedAt: null, finishedAt: null, error: null }],
        source: { type: 'csv', rowsTotal: 200, rowsRead: 200 },
        kafka: { topic: 'test', partitions: 1, published: 50, failed: 0, endOffsets: 50, consumerLag: null },
      };
      expect(activeStageSummary(p as RunProgress)).toEqual({
        label: 'Kafka: publishing records',
        ratio: 0.25,
        indeterminate: false,
      });
    });

    it('calculates spark ratio using activeStage tasksDone / tasksTotal', () => {
      const p: Partial<RunProgress> = {
        stages: [{ key: 'spark', state: 'active', startedAt: null, finishedAt: null, error: null }],
        spark: {
          appId: 'app-1',
          tasksTotal: 10,
          tasksDone: 4,
          recordsIn: 100,
          recordsOut: 90,
          durationMs: 5000,
          activeStage: { id: 1, name: 'mapPartitions', tasksTotal: 8, tasksDone: 6 },
        },
      };
      expect(activeStageSummary(p as RunProgress)).toEqual({
        label: 'Spark: processing stage "mapPartitions"',
        ratio: 0.75,
        indeterminate: false,
      });
    });

    it('hdfs is indeterminate until verified === true', () => {
      const p1: Partial<RunProgress> = {
        stages: [{ key: 'hdfs', state: 'active', startedAt: null, finishedAt: null, error: null }],
        hdfs: { path: '/data', filesWritten: 1, bytesWritten: 500, rowsWritten: 50, verified: false },
      };
      expect(activeStageSummary(p1 as RunProgress)).toEqual({
        label: 'HDFS: storing and verifying records',
        ratio: null,
        indeterminate: true,
      });

      const p2: Partial<RunProgress> = {
        stages: [{ key: 'hdfs', state: 'active', startedAt: null, finishedAt: null, error: null }],
        hdfs: { path: '/data', filesWritten: 1, bytesWritten: 500, rowsWritten: 50, verified: true },
      };
      expect(activeStageSummary(p2 as RunProgress)).toEqual({
        label: 'HDFS: records verified',
        ratio: 1,
        indeterminate: false,
      });
    });
  });
});
