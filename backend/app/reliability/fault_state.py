"""
app/reliability/fault_state.py
--------------------------------
Thread-safe registry holding active or recently recovered chaos fault state.
Bridges the chaos runner and the pipeline progress tracker so live pipeline
visualizations can display truthful fault overlays.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Callable

logger = logging.getLogger(__name__)


@dataclass
class FaultState:
    experimentId: str
    target: str
    type: str
    phase: str  # "injected" | "recovering" | "recovered"
    injectedAt: str
    durationSeconds: int
    recoverySeconds: float | None = None
    expires_at: float | None = None  # monotonic clock expiry

    def to_dict(self) -> dict[str, Any]:
        return {
            "experimentId": self.experimentId,
            "target": self.target,
            "type": self.type,
            "phase": self.phase,
            "injectedAt": self.injectedAt,
            "durationSeconds": self.durationSeconds,
            "recoverySeconds": self.recoverySeconds,
        }


class FaultStateRegistry:
    """
    Thread-safe registry holding current active or recently recovered chaos fault state.
    """

    def __init__(self, time_fn: Callable[[], float] = time.monotonic) -> None:
        self._lock = threading.RLock()
        self._current: FaultState | None = None
        self._time_fn = time_fn

    def set_injected(
        self,
        experiment_id: str,
        target: str,
        fault_type: str,
        duration_seconds: int,
        injected_at: str | None = None,
    ) -> FaultState:
        with self._lock:
            ts = injected_at or datetime.now(tz=timezone.utc).isoformat()
            self._current = FaultState(
                experimentId=experiment_id,
                target=target,
                type=fault_type,
                phase="injected",
                injectedAt=ts,
                durationSeconds=duration_seconds,
                recoverySeconds=None,
                expires_at=None,
            )
            logger.info(
                "FaultStateRegistry: set_injected exp=%s target=%s type=%s duration=%ds",
                experiment_id,
                target,
                fault_type,
                duration_seconds,
            )
            return self._current

    def set_recovering(self, experiment_id: str) -> FaultState | None:
        with self._lock:
            if self._current is None or self._current.experimentId != experiment_id:
                return None
            self._current.phase = "recovering"
            logger.info(
                "FaultStateRegistry: set_recovering exp=%s target=%s",
                experiment_id,
                self._current.target,
            )
            return self._current

    def set_recovered(
        self, experiment_id: str, recovery_seconds: float | None
    ) -> FaultState | None:
        with self._lock:
            if self._current is None or self._current.experimentId != experiment_id:
                return None
            self._current.phase = "recovered"
            self._current.recoverySeconds = recovery_seconds
            # 60s grace period so clients can still see "recovered"
            self._current.expires_at = self._time_fn() + 60.0
            logger.info(
                "FaultStateRegistry: set_recovered exp=%s target=%s recovery=%.2fs expires_in=60s",
                experiment_id,
                self._current.target,
                recovery_seconds if recovery_seconds is not None else -1.0,
            )
            return self._current

    def clear(self, experiment_id: str | None = None) -> None:
        with self._lock:
            if experiment_id is None or (
                self._current and self._current.experimentId == experiment_id
            ):
                logger.info("FaultStateRegistry: cleared state for exp=%s", experiment_id)
                self._current = None

    def current(self) -> FaultState | None:
        with self._lock:
            if self._current is None:
                return None
            if self._current.expires_at is not None:
                if self._time_fn() >= self._current.expires_at:
                    logger.debug("FaultStateRegistry: state expired, clearing.")
                    self._current = None
                    return None
            return self._current

    def snapshot(self) -> dict[str, Any] | None:
        curr = self.current()
        if curr is None:
            return None
        return curr.to_dict()


# Global singleton instance
fault_state_registry = FaultStateRegistry()
