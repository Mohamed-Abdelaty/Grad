/* eslint-disable @typescript-eslint/no-explicit-any, @typescript-eslint/no-unused-vars */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import { useRunLive } from '../hooks/useRunLive';
import * as runProgressApi from '../api/runProgress';
import * as fetchEventSourceModule from '@microsoft/fetch-event-source';
import { RunProgress } from '../types/runProgress';

const mockLogout = vi.fn();
vi.mock('../hooks/useAuth', () => ({
  useAuth: () => ({
    logout: mockLogout,
    token: 'test-token',
    username: 'admin',
    role: 'admin',
    login: vi.fn(),
  }),
}));

const mockProgressData: RunProgress = {
  runId: 'run-123',
  status: 'running',
  mode: 'batch',
  startedAt: '2026-10-13T10:00:00Z',
  updatedAt: '2026-10-13T10:00:00Z',
  completedAt: null,
  stages: [
    { key: 'source', state: 'active', startedAt: '2026-10-13T10:00:00Z', finishedAt: null, error: null },
    { key: 'kafka', state: 'pending', startedAt: null, finishedAt: null, error: null },
    { key: 'spark', state: 'pending', startedAt: null, finishedAt: null, error: null },
    { key: 'hdfs', state: 'pending', startedAt: null, finishedAt: null, error: null },
  ],
  source: { type: 'csv', rowsTotal: 100, rowsRead: 20 },
  kafka: { topic: 't', partitions: 1, published: 0, failed: 0, endOffsets: 0, consumerLag: null },
  spark: { appId: null, tasksTotal: null, tasksDone: null, recordsIn: null, recordsOut: null, durationMs: null, activeStage: null },
  hdfs: { path: '/p', filesWritten: null, bytesWritten: null, rowsWritten: null, verified: null },
  throughput: { recordsPerSecond: 10, samples: [] },
  etaSeconds: 8,
  errors: 0,
  fault: null,
};

describe('useRunLive hook', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('initially fetches progress, renders snapshot, then opens stream', async () => {
    const getProgressSpy = vi.spyOn(runProgressApi, 'getProgress').mockResolvedValue(mockProgressData);
    let capturedOnMessage: ((msg: any) => void) | null = null;
    let capturedOnOpen: ((res: any) => Promise<void>) | null = null;

    vi.spyOn(fetchEventSourceModule, 'fetchEventSource').mockImplementation(async (_url, opts: any) => {
      capturedOnOpen = opts.onopen;
      capturedOnMessage = opts.onmessage;
      if (opts.onopen) {
        await opts.onopen({
          ok: true,
          status: 200,
          headers: new Headers({ 'content-type': 'text/event-stream' }),
        });
      }
    });

    const { result } = renderHook(() => useRunLive('run-123'));

    // Wait for getProgress promise
    await act(async () => {
      await Promise.resolve();
    });

    expect(getProgressSpy).toHaveBeenCalledWith('run-123');
    expect(result.current.progress?.runId).toBe('run-123');
    expect(result.current.connection).toBe('live');

    // Simulate progress frame update
    const updatedProgress = { ...mockProgressData, status: 'running' as const, source: { ...mockProgressData.source, rowsRead: 50 } };
    act(() => {
      capturedOnMessage?.({
        event: 'progress',
        data: JSON.stringify(updatedProgress),
      });
    });

    expect(result.current.progress?.source.rowsRead).toBe(50);
  });

  it('heartbeat only updates lastUpdatedAt', async () => {
    vi.spyOn(runProgressApi, 'getProgress').mockResolvedValue(mockProgressData);
    let capturedOnMessage: ((msg: any) => void) | null = null;

    vi.spyOn(fetchEventSourceModule, 'fetchEventSource').mockImplementation(async (_url, opts: any) => {
      capturedOnMessage = opts.onmessage;
      if (opts.onopen) {
        await opts.onopen({
          ok: true,
          status: 200,
          headers: new Headers({ 'content-type': 'text/event-stream' }),
        });
      }
    });

    const { result } = renderHook(() => useRunLive('run-123'));
    await act(async () => {
      await Promise.resolve();
    });

    const prevUpdatedAt = result.current.lastUpdatedAt;
    vi.setSystemTime(Date.now() + 5000);

    act(() => {
      capturedOnMessage?.({ event: 'heartbeat', data: '{}' });
    });

    expect(result.current.lastUpdatedAt).toBeGreaterThan(prevUpdatedAt || 0);
  });

  it('done frame triggers final fetch and closes stream', async () => {
    const terminalProgress: RunProgress = { ...mockProgressData, status: 'succeeded' };
    const getProgressSpy = vi
      .spyOn(runProgressApi, 'getProgress')
      .mockResolvedValueOnce(mockProgressData)
      .mockResolvedValueOnce(terminalProgress);

    let capturedOnMessage: ((msg: any) => void) | null = null;
    vi.spyOn(fetchEventSourceModule, 'fetchEventSource').mockImplementation(async (_url, opts: any) => {
      capturedOnMessage = opts.onmessage;
      if (opts.onopen) {
        await opts.onopen({
          ok: true,
          status: 200,
          headers: new Headers({ 'content-type': 'text/event-stream' }),
        });
      }
    });

    const { result } = renderHook(() => useRunLive('run-123'));
    await act(async () => {
      await Promise.resolve();
    });

    await act(async () => {
      capturedOnMessage?.({ event: 'done', data: '{"status":"succeeded"}' });
      await Promise.resolve();
    });

    expect(result.current.connection).toBe('closed');
    expect(result.current.progress?.status).toBe('succeeded');
    expect(getProgressSpy).toHaveBeenCalledTimes(2);
  });

  it('switches to polling after 3 consecutive failures', async () => {
    vi.spyOn(runProgressApi, 'getProgress').mockResolvedValue(mockProgressData);

    vi.spyOn(fetchEventSourceModule, 'fetchEventSource').mockImplementation(async () => {
      throw new Error('Network error');
    });

    const { result } = renderHook(() => useRunLive('run-123'));
    await act(async () => {
      await Promise.resolve();
    });

    // Initial failure -> reconnecting
    expect(result.current.connection).toBe('reconnecting');

    // Trigger backoff timer 1
    await act(async () => {
      vi.advanceTimersByTime(2000);
      await Promise.resolve();
    });

    // Trigger backoff timer 2
    await act(async () => {
      vi.advanceTimersByTime(3000);
      await Promise.resolve();
    });

    // Trigger backoff timer 3 -> should switch to polling!
    await act(async () => {
      vi.advanceTimersByTime(5000);
      await Promise.resolve();
    });

    expect(result.current.connection).toBe('polling');
  });

  it('handles 401 by calling logout and stops', async () => {
    vi.spyOn(runProgressApi, 'getProgress').mockResolvedValue(mockProgressData);

    vi.spyOn(fetchEventSourceModule, 'fetchEventSource').mockImplementation(async (_url, opts: any) => {
      await opts.onopen?.({
        ok: false,
        status: 401,
        headers: new Headers(),
      });
    });

    renderHook(() => useRunLive('run-123'));
    await act(async () => {
      await Promise.resolve();
    });

    expect(mockLogout).toHaveBeenCalled();
  });

  it('handles 404 by setting error Run not found', async () => {
    vi.spyOn(runProgressApi, 'getProgress').mockResolvedValue(mockProgressData);

    vi.spyOn(fetchEventSourceModule, 'fetchEventSource').mockImplementation(async (_url, opts: any) => {
      await opts.onopen?.({
        ok: false,
        status: 404,
        headers: new Headers(),
      });
    });

    const { result } = renderHook(() => useRunLive('run-123'));
    await act(async () => {
      await Promise.resolve();
    });

    expect(result.current.error).toBe('Run not found');
    expect(result.current.connection).toBe('closed');
  });

  it('terminal status on initial fetch stops immediately without stream', async () => {
    const succeededProgress: RunProgress = { ...mockProgressData, status: 'succeeded' };
    vi.spyOn(runProgressApi, 'getProgress').mockResolvedValue(succeededProgress);
    const fetchSpy = vi.spyOn(fetchEventSourceModule, 'fetchEventSource');

    const { result } = renderHook(() => useRunLive('run-123'));
    await act(async () => {
      await Promise.resolve();
    });

    expect(result.current.connection).toBe('closed');
    expect(result.current.progress?.status).toBe('succeeded');
    expect(fetchSpy).not.toHaveBeenCalled();
  });
});
