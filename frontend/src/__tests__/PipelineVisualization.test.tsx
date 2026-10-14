import React from 'react';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, act, fireEvent } from '@testing-library/react';
import { axe } from 'vitest-axe';
import { PipelineVisualization } from '../features/viz/PipelineVisualization';
import { RunProgress } from '../types/runProgress';
import { resetWebGLCacheForTesting } from '../features/viz/useVizMode';

vi.mock('sonner', () => ({
  toast: {
    info: vi.fn(),
    success: vi.fn(),
    error: vi.fn(),
  },
}));

function makeProgress(overrides: Partial<RunProgress> = {}): RunProgress {
  return {
    runId: 'run-viz-1',
    status: 'running',
    mode: 'batch',
    startedAt: '2026-10-14T10:00:00Z',
    updatedAt: '2026-10-14T10:00:00Z',
    completedAt: null,
    stages: [
      { key: 'source', state: 'active', startedAt: null, finishedAt: null, error: null },
      { key: 'kafka', state: 'pending', startedAt: null, finishedAt: null, error: null },
      { key: 'spark', state: 'pending', startedAt: null, finishedAt: null, error: null },
      { key: 'hdfs', state: 'pending', startedAt: null, finishedAt: null, error: null },
    ],
    source: { type: 'csv', rowsTotal: 1000, rowsRead: 200 },
    kafka: { topic: 'topic', partitions: 1, published: 0, failed: 0, endOffsets: null, consumerLag: null },
    spark: { appId: null, tasksTotal: null, tasksDone: null, recordsIn: null, recordsOut: null, durationMs: null, activeStage: null },
    hdfs: { path: '/out', filesWritten: null, bytesWritten: null, rowsWritten: null, verified: null },
    throughput: { recordsPerSecond: 100, samples: [] },
    etaSeconds: 20,
    errors: 0,
    fault: null,
    ...overrides,
  };
}

describe('PipelineVisualization Component', () => {
  const originalInnerWidth = window.innerWidth;
  const originalGetContext = HTMLCanvasElement.prototype.getContext;

  beforeEach(() => {
    localStorage.clear();
    resetWebGLCacheForTesting();
    window.innerWidth = 1024;
    vi.clearAllMocks();

    HTMLCanvasElement.prototype.getContext = vi.fn().mockImplementation((contextId) => {
      if (contextId === 'webgl2' || contextId === 'webgl') {
        return {} as unknown as RenderingContext;
      }
      return null;
    });

    window.matchMedia = vi.fn().mockImplementation((query) => ({
      matches: false,
      media: query,
      addListener: vi.fn(),
      removeListener: vi.fn(),
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
      dispatchEvent: vi.fn(),
    }));
  });

  afterEach(() => {
    window.innerWidth = originalInnerWidth;
    HTMLCanvasElement.prototype.getContext = originalGetContext;
    resetWebGLCacheForTesting();
    vi.useRealTimers();
  });

  it('renders Skeleton when progress is null', () => {
    render(<PipelineVisualization progress={null} />);
    expect(screen.getByTestId('viz')).toBeInTheDocument();
    expect(screen.getByTestId('viz-skeleton')).toBeInTheDocument();
  });

  it('renders text equivalent containing one li per station (4 total)', () => {
    const p = makeProgress();
    render(<PipelineVisualization progress={p} />);

    const textEquiv = screen.getByTestId('viz-text-equivalent');
    expect(textEquiv).toBeInTheDocument();
    const items = textEquiv.querySelectorAll('li');
    expect(items).toHaveLength(4);
  });

  it('forces 2D when WebGL is unavailable and reports reason', () => {
    HTMLCanvasElement.prototype.getContext = vi.fn().mockReturnValue(null);
    resetWebGLCacheForTesting();

    const p = makeProgress();
    render(<PipelineVisualization progress={p} />);

    expect(screen.getByTestId('viz-mode').textContent).toBe('2d');
    expect(screen.getByTestId('viz-reason').textContent).toBe('WebGL unavailable');
  });

  it('forces 2D and starts paused when prefers-reduced-motion is reduce', () => {
    window.matchMedia = vi.fn().mockImplementation((query) => ({
      matches: query.includes('prefers-reduced-motion: reduce'),
      media: query,
      addListener: vi.fn(),
      removeListener: vi.fn(),
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
      dispatchEvent: vi.fn(),
    }));

    const p = makeProgress();
    render(<PipelineVisualization progress={p} />);

    expect(screen.getByTestId('viz-mode').textContent).toBe('2d');
    expect(screen.getByTestId('viz-reason').textContent).toBe('Reduced motion enabled');
    expect(screen.getByTestId('viz-pause').getAttribute('aria-pressed')).toBe('true');
  });

  it('forces 2D when viewport width is below 640px', () => {
    window.innerWidth = 480;
    const p = makeProgress();
    render(<PipelineVisualization progress={p} />);

    expect(screen.getByTestId('viz-mode').textContent).toBe('2d');
    expect(screen.getByTestId('viz-reason').textContent).toBe('Screen width below 640px');
  });

  it('forces 2D when quality is set to 2d via toolbar button', () => {
    const p = makeProgress();
    render(<PipelineVisualization progress={p} />);

    const btn2D = screen.getByRole('button', { name: '2D' });
    fireEvent.click(btn2D);

    expect(screen.getByTestId('viz-mode').textContent).toBe('2d');
    expect(screen.getByTestId('viz-reason').textContent).toBe('2D mode selected');
  });

  it('toggles pause and legend controls', () => {
    const p = makeProgress();
    render(<PipelineVisualization progress={p} />);

    const pauseBtn = screen.getByTestId('viz-pause');
    expect(pauseBtn.getAttribute('aria-pressed')).toBe('false');

    fireEvent.click(pauseBtn);
    expect(pauseBtn.getAttribute('aria-pressed')).toBe('true');
    expect(pauseBtn.textContent).toBe('Resume');

    // Legend
    const legendBtn = screen.getByTestId('viz-legend');
    expect(screen.queryByTestId('viz-legend-drawer')).not.toBeInTheDocument();

    fireEvent.click(legendBtn);
    expect(screen.getByTestId('viz-legend-drawer')).toBeInTheDocument();
  });

  it('announces model.announce only on change and at most once per 2 s (fake timers)', () => {
    vi.useFakeTimers();

    let p = makeProgress({
      stages: [{ key: 'source', state: 'active', startedAt: null, finishedAt: null, error: null }],
    });

    const { rerender } = render(<PipelineVisualization progress={p} />);
    const liveRegion = screen.getByTestId('viz-live-region');
    expect(liveRegion.textContent).toBe('Source processing started.');

    // Immediate change at 500 ms
    act(() => {
      vi.advanceTimersByTime(500);
    });

    p = makeProgress({
      stages: [
        { key: 'source', state: 'done', startedAt: null, finishedAt: null, error: null },
        { key: 'kafka', state: 'active', startedAt: null, finishedAt: null, error: null },
      ],
    });
    rerender(<PipelineVisualization progress={p} />);

    // Should NOT have announced yet (debounced to 2s window)
    expect(liveRegion.textContent).toBe('Source processing started.');

    // Advance to 2000 ms
    act(() => {
      vi.advanceTimersByTime(1500);
    });

    expect(liveRegion.textContent).toBe('Kafka processing started.');
  });

  describe('Accessibility (axe-core)', () => {
    it('has zero accessibility violations in 2D mode', async () => {
      localStorage.setItem('chaosflow.vizQuality', '2d');
      const p = makeProgress();
      const { container } = render(<PipelineVisualization progress={p} />);

      const results = await axe(container, {
        rules: {
          'color-contrast': { enabled: false }, // jsdom does not calculate accurate colors
        },
      });

      expect(results).toHaveNoViolations();
    });

    it('has zero accessibility violations in 3D mode (mocked)', async () => {
      const p = makeProgress();
      const { container } = render(<PipelineVisualization progress={p} />);

      const results = await axe(container, {
        rules: {
          'color-contrast': { enabled: false },
        },
      });

      expect(results).toHaveNoViolations();
    });
  });
});
