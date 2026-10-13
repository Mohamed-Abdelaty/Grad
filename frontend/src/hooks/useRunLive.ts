import { useEffect, useRef, useState, useCallback } from 'react';
import { fetchEventSource } from '@microsoft/fetch-event-source';
import { RunProgress } from '../types/runProgress';
import { getProgress, isRunProgress, mapRawProgress } from '../api/runProgress';
import { getApiToken } from '../api/client';
import { useAuth } from './useAuth';

export type ConnectionState = 'connecting' | 'live' | 'reconnecting' | 'polling' | 'closed';

export interface UseRunLiveReturn {
  progress: RunProgress | null;
  connection: ConnectionState;
  lastUpdatedAt: number | null;
  error: string | null;
}

const BACKOFF_DELAYS = [1000, 2000, 4000, 8000, 15000];

class FatalError extends Error {}

export function useRunLive(runId: string | undefined): UseRunLiveReturn {
  const { logout } = useAuth();
  const [progress, setProgress] = useState<RunProgress | null>(null);
  const [connection, setConnection] = useState<ConnectionState>('connecting');
  const [lastUpdatedAt, setLastUpdatedAt] = useState<number | null>(null);
  const [error, setError] = useState<string | null>(null);

  // References for mutable state across effects and callbacks
  const progressRef = useRef<RunProgress | null>(null);
  progressRef.current = progress;

  const failureCountRef = useRef(0);
  const abortCtrlRef = useRef<AbortController | null>(null);
  const backoffTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const pollingTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const streamRetryTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const isStoppedRef = useRef(false);
  const isConnectingRef = useRef(false);

  const clearAllTimers = useCallback(() => {
    if (backoffTimerRef.current) {
      clearTimeout(backoffTimerRef.current);
      backoffTimerRef.current = null;
    }
    if (pollingTimerRef.current) {
      clearInterval(pollingTimerRef.current);
      pollingTimerRef.current = null;
    }
    if (streamRetryTimerRef.current) {
      clearInterval(streamRetryTimerRef.current);
      streamRetryTimerRef.current = null;
    }
  }, []);

  const stopAndClose = useCallback(async (currentRunId: string) => {
    isStoppedRef.current = true;
    clearAllTimers();
    if (abortCtrlRef.current) {
      abortCtrlRef.current.abort();
      abortCtrlRef.current = null;
    }
    try {
      const finalProgress = await getProgress(currentRunId);
      setProgress(finalProgress);
      progressRef.current = finalProgress;
      setLastUpdatedAt(Date.now());
    } catch {
      // ignore terminal fetch failure
    }
    setConnection('closed');
  }, [clearAllTimers]);

  useEffect(() => {
    if (!runId) {
      setProgress(null);
      setConnection('closed');
      setError(null);
      return;
    }

    isStoppedRef.current = false;
    failureCountRef.current = 0;
    setError(null);
    setConnection('connecting');

    const ctrl = new AbortController();
    abortCtrlRef.current = ctrl;

    const startPolling = () => {
      if (isStoppedRef.current) return;
      setConnection('polling');
      if (pollingTimerRef.current) clearInterval(pollingTimerRef.current);

      pollingTimerRef.current = setInterval(async () => {
        if (isStoppedRef.current) return;
        try {
          const snap = await getProgress(runId);
          setProgress(snap);
          progressRef.current = snap;
          setLastUpdatedAt(Date.now());

          if (['succeeded', 'failed', 'cancelled'].includes(snap.status)) {
            stopAndClose(runId);
          }
        } catch (err: unknown) {
          if (import.meta.env.DEV) {
            console.warn('Polling getProgress error:', err);
          }
        }
      }, 2000);

      // Try re-establishing stream every 30s while in polling mode
      if (streamRetryTimerRef.current) clearInterval(streamRetryTimerRef.current);
      streamRetryTimerRef.current = setInterval(() => {
        if (isStoppedRef.current) return;
        failureCountRef.current = 0;
        openStream();
      }, 30000);
    };

    const scheduleReconnect = () => {
      if (isStoppedRef.current) return;
      failureCountRef.current += 1;

      if (failureCountRef.current >= 3) {
        startPolling();
        return;
      }

      setConnection('reconnecting');
      const idx = Math.min(failureCountRef.current - 1, BACKOFF_DELAYS.length - 1);
      const baseDelay = BACKOFF_DELAYS[idx];
      const jitter = (Math.random() * 0.4 - 0.2) * baseDelay; // ±20%
      const delay = Math.max(100, Math.round(baseDelay + jitter));

      if (backoffTimerRef.current) clearTimeout(backoffTimerRef.current);
      backoffTimerRef.current = setTimeout(() => {
        openStream();
      }, delay);
    };

    const openStream = async () => {
      if (isStoppedRef.current || isConnectingRef.current) return;
      isConnectingRef.current = true;

      if (abortCtrlRef.current) {
        abortCtrlRef.current.abort();
      }
      const streamCtrl = new AbortController();
      abortCtrlRef.current = streamCtrl;

      const token = getApiToken();
      const headers: Record<string, string> = {};
      if (token) {
        headers['Authorization'] = `Bearer ${token}`;
      }

      try {
        await fetchEventSource(`/api/pipeline-runs/${runId}/stream`, {
          method: 'GET',
          headers,
          signal: streamCtrl.signal,
          openWhenHidden: true,
          async onopen(response) {
            isConnectingRef.current = false;
            if (response.status === 401) {
              logout();
              throw new FatalError('Unauthorized');
            }
            if (response.status === 404) {
              setError('Run not found');
              setConnection('closed');
              throw new FatalError('Run not found');
            }
            if (!response.ok || !response.headers.get('content-type')?.includes('text/event-stream')) {
              throw new Error(`Unexpected response: ${response.status}`);
            }

            // Stream connected successfully
            failureCountRef.current = 0;
            if (pollingTimerRef.current) {
              clearInterval(pollingTimerRef.current);
              pollingTimerRef.current = null;
            }
            if (streamRetryTimerRef.current) {
              clearInterval(streamRetryTimerRef.current);
              streamRetryTimerRef.current = null;
            }
            setConnection('live');
          },
          onmessage(msg) {
            if (isStoppedRef.current) return;
            if (msg.event === 'heartbeat') {
              setLastUpdatedAt(Date.now());
              return;
            }
            if (msg.event === 'done') {
              stopAndClose(runId);
              return;
            }
            if (msg.event === 'progress') {
              try {
                const parsed = JSON.parse(msg.data);
                if (isRunProgress(parsed)) {
                  const mapped = mapRawProgress(parsed);
                  setProgress(mapped);
                  progressRef.current = mapped;
                  setLastUpdatedAt(Date.now());

                  if (['succeeded', 'failed', 'cancelled'].includes(mapped.status)) {
                    stopAndClose(runId);
                  }
                }
              } catch (parseErr) {
                if (import.meta.env.DEV) {
                  console.warn('Malformed progress frame ignored:', parseErr);
                }
              }
            }
          },
          onclose() {
            isConnectingRef.current = false;
            if (!isStoppedRef.current) {
              scheduleReconnect();
            }
          },
          onerror(err) {
            isConnectingRef.current = false;
            if (err instanceof FatalError) {
              throw err; // Stop retrying
            }
            scheduleReconnect();
            throw err;
          },
        });
      } catch (err: unknown) {
        isConnectingRef.current = false;
        if (err instanceof FatalError) {
          isStoppedRef.current = true;
          clearAllTimers();
        } else if (!isStoppedRef.current) {
          scheduleReconnect();
        }
      }
    };

    // 1. Initial snapshot fetch for immediate render
    getProgress(runId)
      .then((initial) => {
        if (isStoppedRef.current) return;
        setProgress(initial);
        progressRef.current = initial;
        setLastUpdatedAt(Date.now());

        if (['succeeded', 'failed', 'cancelled'].includes(initial.status)) {
          setConnection('closed');
          return;
        }

        // 2. Open live stream if not already terminal
        openStream();
      })
      .catch((err: unknown) => {
        if (isStoppedRef.current) return;
        const msg = err instanceof Error ? err.message : String(err);
        if (msg.includes('404')) {
          setError('Run not found');
          setConnection('closed');
        } else {
          // Fall back to opening stream or scheduling reconnect
          openStream();
        }
      });

    return () => {
      isStoppedRef.current = true;
      isConnectingRef.current = false;
      clearAllTimers();
      if (abortCtrlRef.current) {
        abortCtrlRef.current.abort();
        abortCtrlRef.current = null;
      }
    };
  }, [runId, logout, stopAndClose, clearAllTimers]);

  return {
    progress,
    connection,
    lastUpdatedAt,
    error,
  };
}
