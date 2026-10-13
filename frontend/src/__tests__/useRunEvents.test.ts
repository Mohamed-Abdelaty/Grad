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

  // 1. Cursor advance
  it('advances cursor based on highest received sequence number', async () => {
    const getEventsSpy = vi.spyOn(runProgressApi, 'getEvents')
      .mockResolvedValueOnce([makeEvent(5), makeEvent(12)])
      .mockResolvedValueOnce([makeEvent(15)]);

    const { result } = renderHook(() => useRunEvents('run-123', true));

    await act(async () => {
      await Promise.resolve();
    });

    expect(result.current.events).toHaveLength(2);
    expect(getEventsSpy).toHaveBeenCalledWith('run-123', 0);

    // Advance timer for next poll
    await act(async () => {
      vi.advanceTimersByTime(1500);
      await Promise.resolve();
    });

    expect(getEventsSpy).toHaveBeenCalledTimes(2);
    expect(getEventsSpy).toHaveBeenLastCalledWith('run-123', 12);
    expect(result.current.events.map((e) => e.seq)).toEqual([5, 12, 15]);
  });

  // 2. Dedupe by seq
  it('deduplicates events by sequence number when overlapping seqs are returned', async () => {
    vi.spyOn(runProgressApi, 'getEvents')
      .mockResolvedValueOnce([makeEvent(1), makeEvent(2)])
      .mockResolvedValueOnce([makeEvent(2), makeEvent(3)]); // seq 2 repeated

    const { result } = renderHook(() => useRunEvents('run-123', true));

    await act(async () => {
      await Promise.resolve();
    });

    expect(result.current.events).toHaveLength(2);

    await act(async () => {
      vi.advanceTimersByTime(1500);
      await Promise.resolve();
    });

    expect(result.current.events).toHaveLength(3);
    expect(result.current.events.map((e) => e.seq)).toEqual([1, 2, 3]);
  });

  // 3. Final fetch when running turns false
  it('performs final fetch when running transitions from true to false', async () => {
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

  // 4. 5000-event cap
  it('enforces the 5000-event cap discarding oldest events', async () => {
    const batch1 = Array.from({ length: 3000 }, (_, i) => makeEvent(i + 1));
    const batch2 = Array.from({ length: 2500 }, (_, i) => makeEvent(i + 3001));

    vi.spyOn(runProgressApi, 'getEvents')
      .mockResolvedValueOnce(batch1)
      .mockResolvedValueOnce(batch2);

    const { result } = renderHook(() => useRunEvents('run-123', true));

    await act(async () => {
      await Promise.resolve();
    });

    expect(result.current.events).toHaveLength(3000);

    // Second poll brings total to 5500 -> capped at 5000
    await act(async () => {
      vi.advanceTimersByTime(1500);
      await Promise.resolve();
    });

    expect(result.current.events).toHaveLength(5000);
    // Oldest 500 events (1..500) dropped; first item is 501, last is 5500
    expect(result.current.events[0].seq).toBe(501);
    expect(result.current.events[result.current.events.length - 1].seq).toBe(5500);
  });
});
