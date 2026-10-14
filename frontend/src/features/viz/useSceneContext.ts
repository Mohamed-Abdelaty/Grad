import { useState, useEffect, useRef } from 'react';
import { RunProgress } from '../../types/runProgress';
import { SceneContext } from './model';

export function useSceneContext(progress: RunProgress | null | undefined): SceneContext {
  const [ctx, setCtx] = useState<SceneContext>({ peakLag: 0, peakRate: 0 });
  const lastRunIdRef = useRef<string | null>(null);

  useEffect(() => {
    if (!progress) return;

    const runId = progress.runId;
    const currentLag = typeof progress.kafka?.consumerLag === 'number' && !Number.isNaN(progress.kafka.consumerLag)
      ? progress.kafka.consumerLag
      : 0;
    const currentRate = typeof progress.throughput?.recordsPerSecond === 'number' && !Number.isNaN(progress.throughput.recordsPerSecond)
      ? progress.throughput.recordsPerSecond
      : 0;

    if (runId !== lastRunIdRef.current) {
      lastRunIdRef.current = runId;
      setCtx({
        peakLag: Math.max(0, currentLag),
        peakRate: Math.max(0, currentRate),
      });
      return;
    }

    setCtx((prev) => {
      const newLag = Math.max(prev.peakLag, currentLag);
      const newRate = Math.max(prev.peakRate, currentRate);
      if (newLag === prev.peakLag && newRate === prev.peakRate) {
        return prev;
      }
      return { peakLag: newLag, peakRate: newRate };
    });
  }, [progress?.runId, progress?.kafka?.consumerLag, progress?.throughput?.recordsPerSecond]);

  return ctx;
}
