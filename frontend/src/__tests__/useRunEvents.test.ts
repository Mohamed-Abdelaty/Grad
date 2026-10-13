import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import { useRunEvents } from '../hooks/useRunEvents';
import * as runProgressApi from '../api/runProgress';
import { RunEvent } from '../types/runProgress';

const makeEvent = (seq: number, eventType = 'kafka_progress'): RunEvent => ({
  seq,
  eventType,
  payload: { n: seq },
  ts: new Date().toISOString(),
});

describe('useRunEvents hook', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('fetches initial events, advances cursor, dedupes, and caps', async () => {
    const getEventsSpy = vi.spyOn(runProgressApi, 'getEvents')
      .mockResolvedValueOnce([makeEvent(1), makeEvent(2)])
      .mockResolvedValueOnce([makeEvent(2), makeEvent(3)]); // seq 2 is duplicate

    const { result } = renderHook(() => useRunEvents('run-123', true));

    // Initial fetch resolves
    await act(async () => {
      await Promise.resolve();
    });

    expect(result.current.events).toHaveLength(2);
    expect(result.current.events.map((e) => e.seq)).toEqual([1, 2]);

    // Advance timer for poll
    await act(async () => {
      vi.advanceTimersByTime(1500);
      await Promise.resolve();
    });

    expect(getEventsSpy).toHaveBeenCalledTimes(2);
    expect(getEventsSpy).toHaveBeenLastCalledWith('run-123', 2);
    expect(result.current.events).toHaveLength(3);
    expect(result.current.events.map((e) => e.seq)).toEqual([1, 2, 3]);
  });

  it('performs final fetch when running transitions to false', async () => {
    const getEventsSpy = vi.spyOn(runProgressApi, 'getEvents')
      .mockResolvedValueOnce([makeEvent(1)])
      .mockResolvedValueOnce([makeEvent(2)]);

    const { result, rerender } = renderHook(
      ({ running }) => useRunEvents('run-123', running),
      { initialProps: { running: true } }
    );

    await act(async () => {
      await Promise.resolve();
    });

    expect(result.current.events).toHaveLength(1);

    // Transition running: true -> false
    rerender({ running: false });

    await act(async () => {
      await Promise.resolve();
    });

    expect(getEventsSpy).toHaveBeenCalledTimes(2);
    expect(result.current.events.map((e) => e.seq)).toEqual([1, 2]);
  });
});
