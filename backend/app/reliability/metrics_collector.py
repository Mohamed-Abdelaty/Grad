"""
app/reliability/metrics_collector.py
--------------------------------------
Captures and persists quantitative measurements throughout a chaos experiment.

Metrics are stored in the existing ``Metric`` SQLAlchemy model from Phase 1.
Because the ``Metric`` model is FK'd to ``pipeline_runs.id``, and chaos
experiments don't always have an associated pipeline run, we store
experiment-level metrics using the ``ExperimentRun.id`` in the ``step``
column (as an identifier hack) and ``pipeline_run_id=None``-safe workaround:
we record metrics using the ``ExperimentRun`` relation via the ``results``
JSON field, and also persist each named metric as a separate ``Metric`` row
when a ``pipeline_run_id`` is available (i.e. when the experiment ran a
pipeline), for queryability.

For experiment-level measurements without a pipeline run, metrics are stored
directly in ``ExperimentRun.results`` (a JSON column) — no FK dependency.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any, Callable

from app.reliability.docker_controller import DockerController

logger = logging.getLogger(__name__)


class MetricsCollector:
    """
    Capture, aggregate, and persist chaos experiment metrics.

    Designed to be instantiated once per experiment run and reused across
    all lifecycle stages.
    """

    # Polling interval for availability timeline samples (seconds)
    AVAILABILITY_POLL_INTERVAL: float = 1.0

    def capture_baseline(self, pipeline_run_context: dict) -> dict:
        """
        Record the pre-experiment baseline state.

        Parameters
        ----------
        pipeline_run_context : dict
            Context from a prior pipeline run (e.g. ``{"records_processed": N}``).

        Returns
        -------
        dict
            ``{"timestamp": str, "expected_record_count": int,
            "baseline_captured_at": str}``
        """
        now = datetime.now(tz=timezone.utc).isoformat()
        baseline = {
            "timestamp": now,
            "baseline_captured_at": now,
            "expected_record_count": pipeline_run_context.get(
                "records_processed",
                pipeline_run_context.get("rows_written", 0),
            ),
            "pipeline_run_context": pipeline_run_context,
        }
        logger.info("MetricsCollector: baseline captured — %s", baseline)
        return baseline

    def capture_during_experiment(
        self,
        target_service: str,
        docker_controller: DockerController,
        duration_seconds: int,
        is_cancelled_fn: Callable[[], bool] | None = None,
        health_check_fn: Callable | None = None,
    ) -> dict:
        """
        Poll availability every second and build an availability timeline.

        Checks that the container is running and, if ``health_check_fn`` is provided,
        that the deep health probe returns ``{"connected": True}``.

        Parameters
        ----------
        target_service : str
            Service being monitored.
        docker_controller : DockerController
            Used to check container status (already safety-gated).
        duration_seconds : int
            How many seconds to monitor (normally equals ``fault.duration``).
        is_cancelled_fn : Callable[[], bool] | None
            Optional cancellation check callback.
        health_check_fn : Callable | None
            Optional deeper health probe (e.g. ``check_kafka_connectivity``).

        Returns
        -------
        dict
            ``{"samples": [{timestamp, available}], "availability_percentage": float,
            "total_samples": int, "available_samples": int}``
        """
        samples: list[dict[str, Any]] = []
        interval = self.AVAILABILITY_POLL_INTERVAL
        num_ticks = max(1, int(round(duration_seconds / interval))) if interval > 0 else 1
        start_time = time.monotonic()

        for i in range(num_ticks):
            if is_cancelled_fn is not None and is_cancelled_fn():
                logger.info("MetricsCollector: monitoring aborted due to cancellation.")
                break
            tick_start = time.monotonic()
            ts = datetime.now(tz=timezone.utc).isoformat()
            try:
                container_ok = docker_controller.is_container_running(target_service)
            except Exception:
                container_ok = False

            if container_ok:
                if health_check_fn is not None:
                    try:
                        health = health_check_fn(timeout=0.5)
                    except TypeError:
                        health = health_check_fn()
                    except Exception:
                        health = {"connected": False}
                    available = bool(health.get("connected", False))
                else:
                    available = True
            else:
                available = False

            samples.append({"timestamp": ts, "available": available})
            elapsed = time.monotonic() - tick_start
            sleep_time = interval - elapsed
            if sleep_time > 0 and (i < num_ticks - 1 or time.monotonic() < start_time + duration_seconds):
                time.sleep(sleep_time)

        total = len(samples)
        available_count = sum(1 for s in samples if s["available"])
        availability_pct = (available_count / total * 100.0) if total > 0 else 0.0

        result = {
            "samples": samples,
            "total_samples": total,
            "available_samples": available_count,
            "availability_percentage": round(availability_pct, 2),
        }
        logger.info(
            "MetricsCollector: availability during experiment = %.2f%% (%d/%d samples)",
            availability_pct,
            available_count,
            total,
        )
        return result

    def measure_recovery_time(
        self,
        target_service: str,
        docker_controller: DockerController,
        poll_interval: float = 1.0,
        max_wait: float = 120.0,
        health_check_fn: Callable | None = None,
        start_time: float | None = None,
        is_cancelled_fn: Callable[[], bool] | None = None,
    ) -> float | None:
        """
        Poll until the service recovers and measure elapsed time.

        Recovery is defined as:
        1. The container is running (``is_container_running`` returns True), AND
        2. If ``health_check_fn`` is provided, it returns a dict with
           ``{"connected": True}`` (reuses Phase 4-6 health check functions).

        Parameters
        ----------
        target_service : str
            Service to poll.
        docker_controller : DockerController
            Used for container status checks.
        poll_interval : float
            Seconds between polls (default 1s).
        max_wait : float
            Give up after this many seconds (default 120s).
        health_check_fn : Callable | None
            Optional additional health check (e.g. ``check_kafka_connectivity``).
        start_time : float | None
            Monotonic timestamp when recovery window began (e.g. fault revert).
            If None, starts at invocation time.
        is_cancelled_fn : Callable[[], bool] | None
            Optional cancellation check callback.

        Returns
        -------
        float | None
            Elapsed seconds to recovery, or ``None`` if ``max_wait`` exceeded.
        """
        start = start_time if start_time is not None else time.monotonic()
        deadline = time.monotonic() + max_wait

        while time.monotonic() < deadline:
            if is_cancelled_fn is not None and is_cancelled_fn():
                logger.info("MetricsCollector: recovery polling aborted due to cancellation.")
                return None
            try:
                container_ok = docker_controller.is_container_running(target_service)
            except Exception:
                container_ok = False

            if container_ok:
                # Optional deeper health check
                if health_check_fn is not None:
                    try:
                        health = health_check_fn()
                        service_ok = bool(health.get("connected", False))
                    except Exception:
                        service_ok = False
                else:
                    service_ok = True

                if service_ok:
                    elapsed = round(time.monotonic() - start, 2)
                    logger.info(
                        "MetricsCollector: '%s' recovered in %.2fs.",
                        target_service,
                        elapsed,
                    )
                    return round(elapsed, 2)

            time.sleep(poll_interval)

        logger.warning(
            "MetricsCollector: '%s' did not recover within %.0fs.",
            target_service,
            max_wait,
        )
        return None

    def compute_throughput(
        self, records_processed: int, duration_seconds: float
    ) -> float:
        """
        Compute throughput in records per second.

        Parameters
        ----------
        records_processed : int
            Total records processed during the measurement window.
        duration_seconds : float
            Duration of the measurement window in seconds.

        Returns
        -------
        float
            Records per second (0.0 if duration is 0).
        """
        if duration_seconds <= 0:
            return 0.0
        return round(records_processed / duration_seconds, 2)

    def persist_metrics(
        self,
        db_session,
        experiment_run_id,
        metrics: dict,
    ) -> None:
        """
        Persist collected metrics to the ``ExperimentRun.results`` JSON column.

        Rather than FK-coupling experiment metrics to ``pipeline_runs``,
        we store them in the ``ExperimentRun.results`` JSON field which is
        queryable and avoids a schema dependency.

        Parameters
        ----------
        db_session : sqlalchemy.orm.Session
            Active synchronous database session.
        experiment_run_id : uuid.UUID
            The experiment run to update.
        metrics : dict
            Collected metrics dict to merge into ``ExperimentRun.results``.
        """
        from app.models.experiment_run import ExperimentRun

        run = db_session.get(ExperimentRun, experiment_run_id)
        if run is None:
            logger.warning(
                "MetricsCollector: ExperimentRun %s not found — metrics not persisted.",
                experiment_run_id,
            )
            return

        existing = run.results or {}
        existing.update(metrics)
        run.results = existing
        db_session.add(run)
        db_session.flush()
        logger.info(
            "MetricsCollector: metrics persisted for run %s.", experiment_run_id
        )
