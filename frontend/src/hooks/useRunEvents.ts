import { useEffect, useRef, useState, useCallback } from 'react';
import { RunEvent } from '../types/runProgress';
import { getEvents } from '../api/runProgress';

export interface UseRunEventsReturn {
  events: RunEvent[];
  isLoading: boolean;
  error: string | null;
}

const MAX_EVENTS = 5000;
const POLL_INTERVAL_MS = 1500;

export function useRunEvents(
  runId: string | undefined,
  running: boolean
): UseRunEventsReturn {
  const [events, setEvents] = useState<RunEvent[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const lastSeqRef = useRef<number>(0);
  const isMountedRef = useRef<boolean>(true);
  const prevRunningRef = useRef<boolean>(running);

  const fetchEvents = useCallback(async (currentRunId: string) => {
    try {
      const newEvents = await getEvents(currentRunId, lastSeqRef.current);
      if (!isMountedRef.current) return;

      if (newEvents.length > 0) {
        setEvents((prev) => {
          const seenSeqs = new Set(prev.map((e) => e.seq));
          const fresh = newEvents.filter((e) => !seenSeqs.has(e.seq));
          if (fresh.length === 0) return prev;

          const combined = [...prev, ...fresh].sort((a, b) => a.seq - b.seq);
          const highestSeq = combined[combined.length - 1].seq;
          lastSeqRef.current = Math.max(lastSeqRef.current, highestSeq);

          if (combined.length > MAX_EVENTS) {
            return combined.slice(-MAX_EVENTS);
          }
          return combined;
        });
      }
      setError(null);
    } catch (err: unknown) {
      if (!isMountedRef.current) return;
      const msg = err instanceof Error ? err.message : String(err);
      setError(msg);
    }
  }, []);

  const lastFetchedRunIdRef = useRef<string | null>(null);

  // Reset when runId changes
  useEffect(() => {
    lastSeqRef.current = 0;
    setEvents([]);
    setError(null);
  }, [runId]);

  useEffect(() => {
    isMountedRef.current = true;
    if (!runId) {
      setIsLoading(false);
      lastFetchedRunIdRef.current = null;
      return;
    }

    // Initial fetch on mount or runId change
    if (lastFetchedRunIdRef.current !== runId) {
      lastFetchedRunIdRef.current = runId;
      setIsLoading(true);
      fetchEvents(runId).finally(() => {
        if (isMountedRef.current) setIsLoading(false);
      });
    }

    let timer: ReturnType<typeof setTimeout> | null = null;
    if (running) {
      timer = setInterval(() => {
        fetchEvents(runId);
      }, POLL_INTERVAL_MS);
    } else if (prevRunningRef.current && !running) {
      // Final fetch when transitioning from running -> completed
      fetchEvents(runId);
    }

    prevRunningRef.current = running;

    return () => {
      isMountedRef.current = false;
      if (timer) clearInterval(timer);
    };
  }, [runId, running, fetchEvents]);

  return { events, isLoading, error };
}
