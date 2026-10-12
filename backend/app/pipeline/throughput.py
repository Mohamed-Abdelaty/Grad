"""
backend/app/pipeline/throughput.py
---------------------------------
Server-side Exponential Moving Average (EMA) throughput estimator
and ETA calculator for pipeline execution stages.
"""

from __future__ import annotations

import collections
import math


class ThroughputEstimator:
    """
    Calculates real-time publication throughput using EMA (alpha=0.3)
    and estimates remaining time (ETA) based on measured velocity.

    Maintains a ring buffer capped at the last 120 samples.
    """

    def __init__(self, alpha: float = 0.3, maxlen: int = 120) -> None:
        self.alpha = float(alpha)
        self._samples: collections.deque[dict[str, float]] = collections.deque(maxlen=maxlen)
        self._last_time: float | None = None
        self._last_published: int | None = None
        self._ema: float | None = None

    def update(self, published: int, now: float) -> None:
        """
        Ingest a progress counter update.

        Calculates instantaneous rate and updates EMA only if at least
        0.5 seconds has elapsed since the previous update.
        Guards against dt <= 0 and negative published deltas.
        """
        if self._last_time is None or self._last_published is None:
            self._last_time = float(now)
            self._last_published = int(published)
            return

        dt = float(now) - self._last_time
        if dt < 0.5:
            return

        delta_pub = int(published) - self._last_published
        if dt <= 0 or delta_pub < 0:
            self._last_time = float(now)
            self._last_published = int(published)
            return

        rate = delta_pub / dt
        if self._ema is None:
            self._ema = rate
        else:
            self._ema = self.alpha * rate + (1.0 - self.alpha) * self._ema

        self._samples.append({"t": round(float(now), 3), "v": round(self._ema, 2)})
        self._last_time = float(now)
        self._last_published = int(published)

    @property
    def records_per_second(self) -> float | None:
        """Current EMA rate, or None before the first recorded sample."""
        if self._ema is None or len(self._samples) == 0:
            return None
        return round(self._ema, 2)

    @property
    def samples(self) -> list[dict[str, float]]:
        """List of the last 120 recorded rate samples."""
        return list(self._samples)

    def eta_seconds(self, total: int | None, published: int) -> int | None:
        """
        Estimate remaining seconds until published reaches total.

        Returns ceil((total - published) / ema) only if total is known,
        ema > 0, and at least 3 samples exist; otherwise None.
        """
        if total is None or self._ema is None or self._ema <= 0:
            return None
        if len(self._samples) < 3:
            return None
        remaining = total - published
        if remaining <= 0:
            return 0
        return math.ceil(remaining / self._ema)
