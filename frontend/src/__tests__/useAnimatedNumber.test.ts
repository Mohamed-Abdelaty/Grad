import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import { useAnimatedNumber } from '../hooks/useAnimatedNumber';

describe('useAnimatedNumber', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('returns null for null target', () => {
    const { result } = renderHook(() => useAnimatedNumber(null));
    expect(result.current).toBeNull();
  });

  it('returns target immediately if durationMs is 0', () => {
    const { result } = renderHook(() => useAnimatedNumber(100, { durationMs: 0 }));
    expect(result.current).toBe(100);
  });

  it('interpolates toward target and never exceeds it', () => {
    let now = 1000;
    vi.spyOn(performance, 'now').mockImplementation(() => now);
    vi.spyOn(window, 'requestAnimationFrame').mockImplementation((cb) => {
      return setTimeout(() => cb(now), 16) as unknown as number;
    });

    const { result, rerender } = renderHook(
      ({ val }) => useAnimatedNumber(val, { durationMs: 200 }),
      { initialProps: { val: 0 } }
    );

    expect(result.current).toBe(0);

    // Update target to 100
    rerender({ val: 100 });

    // Advance 100ms
    now += 100;
    act(() => {
      vi.advanceTimersByTime(100);
    });

    expect(result.current).toBeGreaterThan(0);
    expect(result.current).toBeLessThanOrEqual(100);

    // Advance past end
    now += 200;
    act(() => {
      vi.advanceTimersByTime(200);
    });

    expect(result.current).toBe(100);
  });

  it('returns target immediately when prefers-reduced-motion is active', () => {
    vi.spyOn(window, 'matchMedia').mockImplementation((query) => ({
      matches: query.includes('prefers-reduced-motion'),
      media: query,
      onchange: null,
      addListener: vi.fn(),
      removeListener: vi.fn(),
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
      dispatchEvent: vi.fn(),
    }));

    const { result } = renderHook(() => useAnimatedNumber(500, { durationMs: 600 }));
    expect(result.current).toBe(500);
  });
});
