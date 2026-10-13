import { useEffect, useRef, useState } from 'react';

interface UseAnimatedNumberOptions {
  durationMs?: number;
}

export function useAnimatedNumber(
  target: number | null | undefined,
  options: UseAnimatedNumberOptions = {}
): number | null {
  const { durationMs = 600 } = options;

  const [current, setCurrent] = useState<number | null>(() => (target == null ? null : target));
  const prevTargetRef = useRef<number | null>(target == null ? null : target);
  const animRef = useRef<number | null>(null);

  useEffect(() => {
    // If target is null or undefined, reset immediately to null
    if (target == null) {
      if (animRef.current != null) {
        cancelAnimationFrame(animRef.current);
        animRef.current = null;
      }
      prevTargetRef.current = null;
      setCurrent(null);
      return;
    }

    // Check prefers-reduced-motion
    const prefersReducedMotion =
      typeof window !== 'undefined' &&
      window.matchMedia &&
      window.matchMedia('(prefers-reduced-motion: reduce)').matches;

    if (prefersReducedMotion || durationMs <= 0) {
      if (animRef.current != null) {
        cancelAnimationFrame(animRef.current);
        animRef.current = null;
      }
      prevTargetRef.current = target;
      setCurrent(target);
      return;
    }

    const startValue = prevTargetRef.current == null ? target : prevTargetRef.current;
    if (startValue === target) {
      setCurrent(target);
      return;
    }

    const startTime = performance.now();
    const delta = target - startValue;

    const tick = (now: number) => {
      const elapsed = now - startTime;
      const progress = Math.min(1, Math.max(0, elapsed / durationMs));
      // Ease out cubic
      const easeOut = 1 - Math.pow(1 - progress, 3);
      const nextValue = startValue + delta * easeOut;

      // Clamp so it NEVER exceeds target (if increasing) or drops below target (if decreasing)
      let clamped = delta >= 0 ? Math.min(nextValue, target) : Math.max(nextValue, target);
      if (Number.isInteger(target)) {
        clamped = delta >= 0 ? Math.min(Math.floor(clamped), target) : Math.max(Math.ceil(clamped), target);
      }

      setCurrent(clamped);

      if (progress < 1) {
        animRef.current = requestAnimationFrame(tick);
      } else {
        prevTargetRef.current = target;
        setCurrent(target);
        animRef.current = null;
      }
    };

    if (animRef.current != null) {
      cancelAnimationFrame(animRef.current);
    }
    animRef.current = requestAnimationFrame(tick);

    return () => {
      if (animRef.current != null) {
        cancelAnimationFrame(animRef.current);
        animRef.current = null;
      }
    };
  }, [target, durationMs]);

  return current;
}
