import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import { useVizMode, isWebGLAvailable, resetWebGLCacheForTesting } from '../features/viz/useVizMode';
import { toast } from 'sonner';

vi.mock('sonner', () => ({
  toast: {
    info: vi.fn(),
    success: vi.fn(),
    error: vi.fn(),
  },
}));

describe('useVizMode', () => {
  const originalInnerWidth = window.innerWidth;
  const originalGetContext = HTMLCanvasElement.prototype.getContext;

  beforeEach(() => {
    localStorage.clear();
    resetWebGLCacheForTesting();
    window.innerWidth = 1024;
    vi.clearAllMocks();

    // Default WebGL available
    HTMLCanvasElement.prototype.getContext = vi.fn().mockImplementation((contextId) => {
      if (contextId === 'webgl2' || contextId === 'webgl') {
        return {} as any;
      }
      return null;
    });

    // Default matchMedia: no reduced motion
    window.matchMedia = vi.fn().mockImplementation((query) => ({
      matches: false,
      media: query,
      onchange: null,
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

  it('defaults to high quality and 3D mode on desktop with WebGL', () => {
    const { result } = renderHook(() => useVizMode());
    expect(result.current.quality).toBe('high');
    expect(result.current.mode).toBe('3d');
    expect(result.current.reason).toBeNull();
    expect(result.current.paused).toBe(false);
  });

  it('loads stored quality from localStorage', () => {
    localStorage.setItem('chaosflow.vizQuality', 'low');
    const { result } = renderHook(() => useVizMode());
    expect(result.current.quality).toBe('low');
    expect(result.current.mode).toBe('3d');
  });

  it('updates and persists quality', () => {
    const { result } = renderHook(() => useVizMode());
    act(() => {
      result.current.setQuality('low');
    });
    expect(result.current.quality).toBe('low');
    expect(localStorage.getItem('chaosflow.vizQuality')).toBe('low');
  });

  it('forces 2D when quality is set to 2d', () => {
    const { result } = renderHook(() => useVizMode());
    act(() => {
      result.current.setQuality('2d');
    });
    expect(result.current.mode).toBe('2d');
    expect(result.current.reason).toBe('2D mode selected');
  });

  it('forces 2D when WebGL is unavailable and caches result', () => {
    HTMLCanvasElement.prototype.getContext = vi.fn().mockReturnValue(null);
    resetWebGLCacheForTesting();

    expect(isWebGLAvailable()).toBe(false);

    const { result } = renderHook(() => useVizMode());
    expect(result.current.mode).toBe('2d');
    expect(result.current.reason).toBe('WebGL unavailable');
  });

  it('forces 2D and starts paused under prefers-reduced-motion', () => {
    window.matchMedia = vi.fn().mockImplementation((query) => ({
      matches: query.includes('prefers-reduced-motion: reduce'),
      media: query,
      addListener: vi.fn(),
      removeListener: vi.fn(),
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
      dispatchEvent: vi.fn(),
    }));

    const { result } = renderHook(() => useVizMode());
    expect(result.current.mode).toBe('2d');
    expect(result.current.reason).toBe('Reduced motion enabled');
    expect(result.current.paused).toBe(true);
  });

  it('forces 2D when viewport width is below 640px', () => {
    window.innerWidth = 480;
    const { result } = renderHook(() => useVizMode());
    expect(result.current.mode).toBe('2d');
    expect(result.current.reason).toBe('Screen width below 640px');
  });

  it('forces 2D when chunk fails to load', () => {
    const { result } = renderHook(() => useVizMode());
    expect(result.current.mode).toBe('3d');

    act(() => {
      result.current.onChunkLoadError();
    });

    expect(result.current.mode).toBe('2d');
    expect(result.current.reason).toBe('3D renderer failed to load');
  });

  it('forces 2D when WebGL context is lost', () => {
    const { result } = renderHook(() => useVizMode());
    expect(result.current.mode).toBe('3d');

    act(() => {
      result.current.onContextLost();
    });

    expect(result.current.mode).toBe('2d');
    expect(result.current.reason).toBe('WebGL context lost');
  });

  it('toggles paused state', () => {
    const { result } = renderHook(() => useVizMode());
    expect(result.current.paused).toBe(false);

    act(() => {
      result.current.togglePaused();
    });
    expect(result.current.paused).toBe(true);

    act(() => {
      result.current.togglePaused();
    });
    expect(result.current.paused).toBe(false);
  });

  describe('auto downgrade with fake timers', () => {
    beforeEach(() => {
      vi.useFakeTimers();
    });

    it('downgrades high to low when fps < 30 for 3 seconds', () => {
      const { result } = renderHook(() => useVizMode());
      expect(result.current.quality).toBe('high');

      act(() => {
        result.current.reportFps(25);
      });

      // Advance by 2.9 seconds: should not have downgraded yet
      act(() => {
        vi.advanceTimersByTime(2900);
      });
      expect(result.current.quality).toBe('high');
      expect(toast.info).not.toHaveBeenCalled();

      // Advance remaining 200 ms
      act(() => {
        vi.advanceTimersByTime(200);
      });
      expect(result.current.quality).toBe('low');
      expect(toast.info).toHaveBeenCalledWith('Switched to the simplified view for smoother performance');
    });

    it('cancels downgrade if fps recovers before 3 seconds', () => {
      const { result } = renderHook(() => useVizMode());
      expect(result.current.quality).toBe('high');

      act(() => {
        result.current.reportFps(20);
      });

      act(() => {
        vi.advanceTimersByTime(1500);
      });

      // Recover
      act(() => {
        result.current.reportFps(58);
      });

      // Advance past 3s
      act(() => {
        vi.advanceTimersByTime(2000);
      });

      expect(result.current.quality).toBe('high');
      expect(toast.info).not.toHaveBeenCalled();
    });

    it('downgrades low to 2d when fps < 24 for 3 seconds', () => {
      localStorage.setItem('chaosflow.vizQuality', 'low');
      const { result } = renderHook(() => useVizMode());
      expect(result.current.quality).toBe('low');

      act(() => {
        result.current.reportFps(18);
      });

      act(() => {
        vi.advanceTimersByTime(3100);
      });

      expect(result.current.quality).toBe('2d');
      expect(result.current.mode).toBe('2d');
      expect(toast.info).toHaveBeenCalledWith('Switched to the simplified view for smoother performance');
    });
  });
});
