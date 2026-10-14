import React, { useMemo, useState, useEffect, useRef, useCallback, Suspense } from 'react';
import { toast } from 'sonner';
import { RunProgress } from '../../types/runProgress';
import { toSceneModel } from './model';
import { useSceneContext } from './useSceneContext';
import { useVizMode, VizQuality } from './useVizMode';
import { Scene2D } from './Scene2D';

const LazyScene3D = React.lazy(() => import('./Scene3D'));

export interface PipelineVisualizationProps {
  progress: RunProgress | null;
  compact?: boolean;
}

interface ErrorBoundaryProps {
  fallback: React.ReactNode;
  onError?: (error: Error) => void;
  children: React.ReactNode;
}

interface ErrorBoundaryState {
  hasError: boolean;
}

class VizErrorBoundary extends React.Component<ErrorBoundaryProps, ErrorBoundaryState> {
  constructor(props: ErrorBoundaryProps) {
    super(props);
    this.state = { hasError: false };
  }

  static getDerivedStateFromError() {
    return { hasError: true };
  }

  componentDidCatch(error: Error) {
    this.props.onError?.(error);
  }

  render() {
    if (this.state.hasError) {
      return this.props.fallback;
    }
    return this.props.children;
  }
}

export const PipelineVisualization: React.FC<PipelineVisualizationProps> = ({
  progress,
  compact = false,
}) => {
  const [legendOpen, setLegendOpen] = useState(false);
  const [announcement, setAnnouncement] = useState('');

  const lastAnnounceRef = useRef<string>('');
  const lastAnnounceTimeRef = useRef<number>(0);
  const debounceTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const ctx = useSceneContext(progress);
  const model = useMemo(() => {
    if (!progress) return null;
    return toSceneModel(progress, ctx);
  }, [progress, ctx]);

  const {
    mode,
    quality,
    setQuality,
    reason,
    paused,
    togglePaused,
    reportFps,
    onChunkLoadError,
    onContextLost,
  } = useVizMode();

  // Live region announcement: only when model.announce changes, at most once per 2s
  useEffect(() => {
    if (!model?.announce || model.announce === lastAnnounceRef.current) return;

    const nextAnnounce = model.announce;
    const now = Date.now();
    const elapsed = now - lastAnnounceTimeRef.current;

    if (elapsed >= 2000 || lastAnnounceTimeRef.current === 0) {
      lastAnnounceRef.current = nextAnnounce;
      lastAnnounceTimeRef.current = now;
      setAnnouncement(nextAnnounce);
    } else {
      if (debounceTimerRef.current) {
        clearTimeout(debounceTimerRef.current);
      }
      debounceTimerRef.current = setTimeout(() => {
        lastAnnounceRef.current = nextAnnounce;
        lastAnnounceTimeRef.current = Date.now();
        setAnnouncement(nextAnnounce);
        debounceTimerRef.current = null;
      }, 2000 - elapsed);
    }

    return () => {
      if (debounceTimerRef.current) {
        clearTimeout(debounceTimerRef.current);
        debounceTimerRef.current = null;
      }
    };
  }, [model?.announce]);

  const handle3DError = useCallback(
    (err: Error) => {
      onChunkLoadError();
      toast.error('Failed to load 3D view, falling back to 2D');
    },
    [onChunkLoadError]
  );

  const containerHeightClass = compact
    ? 'h-[240px]'
    : 'h-[280px] md:h-[360px]';

  // If progress is null, render skeleton of same height
  if (!progress || !model) {
    return (
      <div
        data-testid="viz"
        className={`w-full rounded-xl bg-slate-900/40 border border-slate-800 p-4 flex flex-col justify-between animate-pulse ${containerHeightClass}`}
      >
        <div data-testid="viz-skeleton" className="flex items-center justify-between">
          <div className="h-5 w-36 bg-slate-800 rounded" />
          <div className="h-7 w-28 bg-slate-800 rounded" />
        </div>
        <div className="flex justify-around items-center h-40">
          <div className="w-24 h-28 bg-slate-800 rounded-lg" />
          <div className="w-16 h-1 bg-slate-800" />
          <div className="w-24 h-28 bg-slate-800 rounded-lg" />
          <div className="w-16 h-1 bg-slate-800" />
          <div className="w-24 h-28 bg-slate-800 rounded-lg" />
          <div className="w-16 h-1 bg-slate-800" />
          <div className="w-24 h-28 bg-slate-800 rounded-lg" />
        </div>
        <div className="h-4 w-48 bg-slate-800 rounded self-center" />
      </div>
    );
  }

  return (
    <div
      data-testid="viz"
      className={`relative w-full rounded-xl bg-slate-900/60 border border-slate-800 p-3 flex flex-col ${containerHeightClass} shadow-lg`}
    >
      {/* Hidden text equivalent for screen readers */}
      <ul
        aria-label="Pipeline stage status"
        data-testid="viz-text-equivalent"
        className="sr-only"
      >
        {model.summary.map((text, idx) => (
          <li key={idx}>{text}</li>
        ))}
      </ul>

      {/* Polite live region */}
      <div
        aria-live="polite"
        aria-atomic="true"
        className="sr-only"
        data-testid="viz-live-region"
      >
        {announcement}
      </div>

      {/* Hidden mode indicator */}
      <span className="sr-only" data-testid="viz-mode">
        {mode}
      </span>
      {reason && (
        <span className="sr-only" data-testid="viz-reason">
          {reason}
        </span>
      )}

      {/* Header Toolbar */}
      <div className="flex items-center justify-between mb-2 px-1 gap-2 flex-wrap">
        <div className="flex items-center gap-2">
          <span className="text-xs font-semibold text-slate-300 uppercase tracking-wider">
            Pipeline Flow
          </span>
          {reason && (
            <span
              className="text-[10px] text-amber-400 bg-amber-950/60 border border-amber-800/60 px-1.5 py-0.5 rounded"
              title={reason}
            >
              {reason}
            </span>
          )}
        </div>

        <div className="flex items-center gap-2">
          {/* Quality segmented control */}
          <div
            data-testid="viz-quality"
            className="inline-flex rounded-md border border-slate-700 bg-slate-950/60 p-0.5 text-xs"
            role="group"
            aria-label="Visualization quality"
          >
            {(['high', 'low', '2d'] as VizQuality[]).map((q) => (
              <button
                key={q}
                type="button"
                onClick={() => setQuality(q)}
                className={`px-2 py-0.5 rounded font-medium transition-colors ${
                  quality === q
                    ? 'bg-indigo-600 text-white shadow-sm'
                    : 'text-slate-400 hover:text-slate-200'
                }`}
              >
                {q === '2d' ? '2D' : q === 'high' ? 'High' : 'Low'}
              </button>
            ))}
          </div>

          {/* Pause / Play button */}
          <button
            type="button"
            data-testid="viz-pause"
            aria-pressed={paused}
            onClick={togglePaused}
            className={`px-2 py-1 text-xs font-medium rounded border transition-colors ${
              paused
                ? 'bg-amber-900/60 border-amber-700 text-amber-200'
                : 'bg-slate-800 hover:bg-slate-700 border-slate-700 text-slate-300'
            }`}
            title={paused ? 'Resume animation' : 'Pause animation'}
          >
            {paused ? 'Resume' : 'Pause'}
          </button>

          {/* Legend toggle */}
          {!compact && (
            <button
              type="button"
              data-testid="viz-legend"
              onClick={() => setLegendOpen((prev) => !prev)}
              aria-expanded={legendOpen}
              className={`px-2 py-1 text-xs font-medium rounded border transition-colors ${
                legendOpen
                  ? 'bg-slate-700 border-slate-600 text-white'
                  : 'bg-slate-800 hover:bg-slate-700 border-slate-700 text-slate-300'
              }`}
            >
              Legend
            </button>
          )}
        </div>
      </div>

      {/* Main Visualization Canvas Container */}
      <div className="relative flex-1 w-full min-h-0 overflow-hidden rounded-lg bg-slate-950/80 border border-slate-800/80">
        {mode === '3d' ? (
          <Suspense
            fallback={
              <div className="w-full h-full flex items-center justify-center text-xs text-slate-400">
                Loading 3D scene...
              </div>
            }
          >
            <VizErrorBoundary
              fallback={<Scene2D model={model} paused={paused} />}
              onError={handle3DError}
            >
              <LazyScene3D
                model={model}
                quality={quality === 'high' ? 'high' : 'low'}
                paused={paused}
                onFps={reportFps}
                onContextLost={onContextLost}
              />
            </VizErrorBoundary>
          </Suspense>
        ) : (
          <Scene2D model={model} paused={paused} />
        )}
      </div>

      {/* Collapsible Legend Drawer */}
      {!compact && legendOpen && (
        <div
          data-testid="viz-legend-drawer"
          className="mt-2 p-2.5 bg-slate-950/90 border border-slate-800 rounded-lg text-[11px] text-slate-300 space-y-1.5 shadow-xl animate-in fade-in"
        >
          <div className="font-semibold text-slate-200">Visualization Legend:</div>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-x-4 gap-y-1">
            {model.stations.map((s) => (
              <div key={s.key}>
                <span className="font-medium text-slate-400">{s.label}: </span>
                <span>{s.levelMeaning}</span>
              </div>
            ))}
            <div>
              <span className="font-medium text-slate-400">Shimmer animation: </span>
              <span>Processing in progress with unknown remaining total</span>
            </div>
            <div>
              <span className="font-medium text-slate-400">Particles: </span>
              <span>Rate of records transferring between active stages</span>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};
