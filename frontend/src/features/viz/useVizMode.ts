import { useState, useEffect, useCallback, useRef } from 'react';
import { toast } from 'sonner';

export type VizQuality = 'high' | 'low' | '2d';
export type VizMode = '3d' | '2d';

export interface UseVizModeReturn {
  mode: VizMode;
  quality: VizQuality;
  setQuality: (q: VizQuality) => void;
  reason: string | null;
  paused: boolean;
  setPaused: (p: boolean) => void;
  togglePaused: () => void;
  reportFps: (avg: number) => void;
  onChunkLoadError: () => void;
  onContextLost: () => void;
}

let cachedWebGLAvailable: boolean | null = null;

export function isWebGLAvailable(): boolean {
  if (cachedWebGLAvailable !== null) {
    return cachedWebGLAvailable;
  }
  try {
    const canvas = document.createElement('canvas');
    const gl = canvas.getContext('webgl2') || canvas.getContext('webgl');
    cachedWebGLAvailable = gl !== null && gl !== undefined;
  } catch {
    cachedWebGLAvailable = false;
  }
  return cachedWebGLAvailable;
}

export function resetWebGLCacheForTesting(): void {
  cachedWebGLAvailable = null;
}

const STORAGE_KEY = 'chaosflow.vizQuality';

function getStoredQuality(): VizQuality {
  try {
    const val = localStorage.getItem(STORAGE_KEY);
    if (val === 'high' || val === 'low' || val === '2d') {
      return val;
    }
  } catch {
    // localStorage not accessible
  }
  return 'high';
}

function checkReducedMotion(): boolean {
  if (typeof window !== 'undefined' && typeof window.matchMedia === 'function') {
    return window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  }
  return false;
}

function checkNarrowViewport(): boolean {
  if (typeof window !== 'undefined') {
    return window.innerWidth < 640;
  }
  return false;
}

export function useVizMode(): UseVizModeReturn {
  const [quality, setQualityState] = useState<VizQuality>(getStoredQuality);
  const [chunkLoadError, setChunkLoadError] = useState(false);
  const [contextLost, setContextLost] = useState(false);
  const [reducedMotion, setReducedMotion] = useState(checkReducedMotion);
  const [narrow, setNarrow] = useState(checkNarrowViewport);
  const [paused, setPaused] = useState<boolean>(() => checkReducedMotion());

  const downgradeTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  // Sync preference with localStorage
  const setQuality = useCallback((q: VizQuality) => {
    setQualityState(q);
    if (q !== '2d') {
      setContextLost(false);
    }
    try {
      localStorage.setItem(STORAGE_KEY, q);
    } catch {
      // ignore
    }
  }, []);

  const togglePaused = useCallback(() => {
    setPaused((prev) => !prev);
  }, []);

  const onChunkLoadError = useCallback(() => {
    setChunkLoadError(true);
  }, []);

  const onContextLost = useCallback(() => {
    setContextLost(true);
  }, []);

  // Listen to prefers-reduced-motion
  useEffect(() => {
    if (typeof window === 'undefined' || typeof window.matchMedia !== 'function') return;
    const mql = window.matchMedia('(prefers-reduced-motion: reduce)');
    const handler = (e: MediaQueryListEvent) => {
      setReducedMotion(e.matches);
      if (e.matches) {
        setPaused(true);
      }
    };
    mql.addEventListener?.('change', handler);
    return () => {
      mql.removeEventListener?.('change', handler);
    };
  }, []);

  // Listen to window resize
  useEffect(() => {
    if (typeof window === 'undefined') return;
    const onResize = () => {
      setNarrow(window.innerWidth < 640);
    };
    window.addEventListener('resize', onResize);
    return () => {
      window.removeEventListener('resize', onResize);
    };
  }, []);

  // Clean up downgrade timer on unmount or quality change
  useEffect(() => {
    return () => {
      if (downgradeTimerRef.current) {
        clearTimeout(downgradeTimerRef.current);
        downgradeTimerRef.current = null;
      }
    };
  }, [quality]);

  // Auto downgrade based on FPS
  const reportFps = useCallback(
    (avg: number) => {
      if (
        typeof window !== 'undefined' &&
        (window as unknown as { __DISABLE_AUTO_DOWNGRADE__?: boolean }).__DISABLE_AUTO_DOWNGRADE__
      ) {
        return;
      }
      if (quality === 'high') {
        if (avg < 30) {
          if (!downgradeTimerRef.current) {
            downgradeTimerRef.current = setTimeout(() => {
              downgradeTimerRef.current = null;
              setQuality('low');
              toast.info('Switched to the simplified view for smoother performance');
            }, 3000);
          }
        } else {
          if (downgradeTimerRef.current) {
            clearTimeout(downgradeTimerRef.current);
            downgradeTimerRef.current = null;
          }
        }
      } else if (quality === 'low') {
        if (avg < 24) {
          if (!downgradeTimerRef.current) {
            downgradeTimerRef.current = setTimeout(() => {
              downgradeTimerRef.current = null;
              setQuality('2d');
              toast.info('Switched to the simplified view for smoother performance');
            }, 3000);
          }
        } else {
          if (downgradeTimerRef.current) {
            clearTimeout(downgradeTimerRef.current);
            downgradeTimerRef.current = null;
          }
        }
      }
    },
    [quality, setQuality]
  );

  // Forced 2D checks
  let reason: string | null = null;
  const webGLAvailable = isWebGLAvailable();

  if (contextLost) {
    reason = 'WebGL context lost';
  } else if (chunkLoadError) {
    reason = '3D renderer failed to load';
  } else if (!webGLAvailable) {
    reason = 'WebGL unavailable';
  } else if (reducedMotion) {
    reason = 'Reduced motion enabled';
  } else if (narrow) {
    reason = 'Screen width below 640px';
  } else if (quality === '2d') {
    reason = '2D mode selected';
  }

  const mode: VizMode = reason !== null ? '2d' : '3d';

  return {
    mode,
    quality,
    setQuality,
    reason,
    paused,
    setPaused,
    togglePaused,
    reportFps,
    onChunkLoadError,
    onContextLost,
  };
}
