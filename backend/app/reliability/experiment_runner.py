"""
app/reliability/experiment_runner.py
--------------------------------------
Orchestrates the full chaos experiment lifecycle:

    VALIDATING → PREPARING → BASELINE → INJECTING → MONITORING →
    ROLLING_BACK → WAITING_FOR_RECOVERY → VALIDATING_DATA →
    COLLECTING_METRICS → ANALYZING → CLEANING_UP → GENERATING_REPORT

Each stage is implemented as a private method.  The runner persists the
current lifecycle stage to ``ExperimentRun.stage`` at every transition,
maintains the 12-phase list with start/finish times, records execution events
in ``ExperimentEvent``, and updates ``FaultStateRegistry`` for truthful visualization.
"""

from __future__ import annotations

import asyncio
import logging
import math
import time
import uuid
import yaml
from datetime import datetime, timezone
from typing import Any

from app.reliability.data_consistency_validator import DataConsistencyValidator
from app.reliability.docker_controller import DockerController
from app.reliability.exceptions import (
    ExperimentTimeoutError,
    FaultInjectionError,
    TooManyConcurrentExperimentsError,
)
from app.reliability.experiment_models import ExperimentModel
from app.reliability.fault_injectors.base_fault import FAULT_INJECTOR_REGISTRY
from app.reliability.fault_state import fault_state_registry
from app.reliability.metrics_collector import MetricsCollector
from app.reliability.report_generator import ReportGenerator
from app.reliability.safety import SafetyValidator

logger = logging.getLogger(__name__)

# Grace period added to fault.duration to form the global experiment timeout
_TIMEOUT_GRACE_SECONDS: int = 60

LIFECYCLE_PHASES: list[str] = [
    "validating",
    "preparing",
    "baseline",
    "injecting",
    "monitoring",
    "rolling_back",
    "waiting_for_recovery",
    "validating_data",
    "collecting_metrics",
    "analyzing",
    "cleaning_up",
    "generating_report",
]


class ExperimentRunner:
    """
    Full lifecycle orchestrator for ChaosLab experiments.
    """

    def __init__(
        self,
        db_session,
        safety: SafetyValidator | None = None,
        docker_controller: DockerController | None = None,
    ) -> None:
        """Initialise the runner with injected or default components."""
        self._db = db_session
        self._safety = safety or SafetyValidator()
        self._docker = docker_controller or DockerController(self._safety)
        self._metrics = MetricsCollector()
        self._validator = DataConsistencyValidator()
        self._reporter = ReportGenerator()
        self._cancelled: bool = False
        self._event_seq: int = 0

    def cancel(self) -> None:
        """Signal the runner to cancel at the next stage boundary."""
        self._cancelled = True
        logger.warning("ExperimentRunner: cancellation requested.")

    def _emit_event(
        self,
        experiment_id: Any,
        run_id: Any,
        phase: str,
        message: str,
        level: str = "INFO",
    ) -> None:
        """Write an execution event to the experiment_events table."""
        if not experiment_id:
            return
        try:
            exp_uuid = uuid.UUID(str(experiment_id))
        except (ValueError, TypeError, AttributeError):
            return

        run_uuid = None
        if run_id:
            try:
                run_uuid = uuid.UUID(str(run_id))
            except (ValueError, TypeError, AttributeError):
                run_uuid = None

        self._event_seq += 1
        try:
            from app.models.experiment_event import ExperimentEvent

            event = ExperimentEvent(
                experiment_id=exp_uuid,
                experiment_run_id=run_uuid,
                seq=self._event_seq,
                level=level,
                phase=phase,
                message=message,
                ts=datetime.now(tz=timezone.utc),
            )
            self._db.add(event)
            self._db.flush()
            self._db.commit()
        except Exception as exc:
            logger.warning("Failed to emit event: %s", exc)

    def run_experiment(
        self,
        experiment_config: ExperimentModel,
        associated_pipeline_yaml: str = "",
        existing_experiment_id: str | None = None,
        existing_run_id: str | None = None,
    ) -> dict[str, Any]:
        """
        Execute the full experiment lifecycle synchronously.
        """
        from app.models.experiment import Experiment
        from app.models.experiment_run import ExperimentRun

        target_service = experiment_config.target.service
        fault_type = experiment_config.fault.type
        fault_duration = experiment_config.fault.duration

        # ── CREATE: persist experiment + run records ───────────────────────────
        if existing_experiment_id:
            experiment = self._db.get(Experiment, existing_experiment_id)
            if experiment is None:
                raise ValueError(f"Experiment '{existing_experiment_id}' was not found.")
        else:
            experiment = Experiment(
                name=experiment_config.experiment.name,
                description=(
                    f"ChaosLab experiment: {fault_type} on {target_service} "
                    f"for {fault_duration}s"
                ),
                experiment_yaml=yaml.dump(experiment_config.model_dump()),
                pipeline_yaml=associated_pipeline_yaml or "",
            )
            self._db.add(experiment)
            self._db.flush()

        initial_phases = [
            {"name": p, "state": "pending", "startedAt": None, "finishedAt": None}
            for p in LIFECYCLE_PHASES
        ]

        if existing_run_id:
            run_uuid = (
                uuid.UUID(existing_run_id)
                if isinstance(existing_run_id, str)
                else existing_run_id
            )
            run = self._db.get(ExperimentRun, run_uuid)
            if run is None:
                raise ValueError(f"Experiment run '{existing_run_id}' was not found.")
            if run.experiment_id != experiment.id:
                raise ValueError("Experiment run does not belong to this experiment.")
        else:
            run = ExperimentRun(
                experiment_id=experiment.id,
                status="created",
                params={
                    "target_service": target_service,
                    "fault_type": fault_type,
                    "fault_duration": fault_duration,
                    "experiment_config": experiment_config.model_dump(),
                },
                results={"phases": initial_phases},
                started_at=datetime.now(tz=timezone.utc),
            )
            if run.id is None:
                run.id = uuid.uuid4()
            self._db.add(run)
            self._db.flush()
        self._db.commit()
        run_id = run.id
        self._safety.reap_stale_experiments(self._db)
        self._safety.enforce_max_concurrent_experiments(
            self._db, excluding_run_id=run_id
        )
        run.status = "running"
        self._merge_results(run, {"phases": initial_phases})
        self._emit_event(
            experiment.id,
            run_id,
            "validating",
            f"Initialized experiment '{experiment.name}' targeting '{target_service}'.",
        )
        logger.info(
            "ExperimentRunner: created run %s (experiment=%s).",
            run_id,
            experiment.id,
        )

        total_repetitions = getattr(experiment_config, "repetitions", 1) or 1
        run.repetition_count = total_repetitions
        self._db.add(run)
        self._db.flush()

        if total_repetitions == 1:
            return self._execute_single_run(
                run=run,
                experiment=experiment,
                experiment_config=experiment_config,
                associated_pipeline_yaml=associated_pipeline_yaml,
            )

        # ── REPETITION LOOP (repetitions > 1) ─────────────────────────────────
        repetition_reports: list[dict[str, Any]] = []
        repetition_results: list[dict[str, Any]] = []
        completed_count = 0
        failed_count = 0
        skipped_count = 0
        aborted = False

        self._emit_event(
            experiment.id,
            run_id,
            "validating",
            f"Starting {total_repetitions} sequential repetitions.",
        )

        for rep_idx in range(1, total_repetitions + 1):
            if self._cancelled:
                skipped_count += (total_repetitions - rep_idx + 1)
                break

            if aborted:
                skipped_count += 1
                repetition_results.append({
                    "repetition_index": rep_idx,
                    "status": "skipped",
                    "result": "SKIPPED",
                    "error": "Previous repetition failed or recovery health check failed",
                })
                continue

            # Update parent current_repetition for live monitoring
            self._merge_results(run, {"current_repetition": rep_idx})

            # Create child run with CASCADE FK
            child_run = ExperimentRun(
                experiment_id=experiment.id,
                parent_run_id=run.id,
                repetition_index=rep_idx,
                repetition_count=total_repetitions,
                status="running",
                params={
                    "target_service": target_service,
                    "fault_type": fault_type,
                    "fault_duration": fault_duration,
                    "experiment_config": experiment_config.model_dump(),
                    "repetition_index": rep_idx,
                    "total_repetitions": total_repetitions,
                },
                results={"phases": [
                    {"name": p, "state": "pending", "startedAt": None, "finishedAt": None}
                    for p in LIFECYCLE_PHASES
                ]},
                pipeline_run_id=run.pipeline_run_id,
                started_at=datetime.now(tz=timezone.utc),
            )
            if child_run.id is None:
                child_run.id = uuid.uuid4()
            self._db.add(child_run)
            self._db.flush()
            self._db.commit()

            self._emit_event(
                experiment.id,
                run.id,
                "validating",
                f"Starting repetition {rep_idx}/{total_repetitions}.",
            )

            try:
                rep_report = self._execute_single_run(
                    run=child_run,
                    experiment=experiment,
                    experiment_config=experiment_config,
                    associated_pipeline_yaml=associated_pipeline_yaml,
                    parent_run=run,
                )
                repetition_reports.append(rep_report)
                completed_count += 1

                rep_res = {
                    "repetition_index": rep_idx,
                    "run_id": str(child_run.id),
                    "status": child_run.status,
                    "result": rep_report.get("result", "FAIL"),
                    "recovery_time_seconds": rep_report.get("recovery_time_seconds"),
                    "data_loss": rep_report.get("data_loss"),
                    "duplicates": rep_report.get("duplicates"),
                    "availability_percentage": rep_report.get("availability_percentage"),
                    "throughput_records_per_sec": rep_report.get("average_throughput") or (child_run.results or {}).get("throughput_records_per_sec"),
                    "error_rate": rep_report.get("error_rate"),
                    "pre_fault_accounting": child_run.pre_fault_accounting,
                    "error": (child_run.results or {}).get("error"),
                }
                repetition_results.append(rep_res)

                if rep_report.get("result") != "PASS":
                    failed_count += 1
                    aborted = True
                    self._emit_event(
                        experiment.id,
                        run.id,
                        "analyzing",
                        f"Repetition {rep_idx} failed acceptance criteria. Aborting remaining repetitions.",
                        level="WARNING",
                    )
                else:
                    # Health-gate verification before next repetition
                    if rep_idx < total_repetitions:
                        health_fn = self._get_health_check_fn(target_service)
                        is_healthy = self._verify_target_health(
                            target_service,
                            health_fn=health_fn,
                            max_wait=float(experiment_config.validation.max_recovery_time),
                        )
                        if not is_healthy:
                            aborted = True
                            self._emit_event(
                                experiment.id,
                                run.id,
                                "waiting_for_recovery",
                                f"Service '{target_service}' health not restored after repetition {rep_idx}. Aborting remaining repetitions.",
                                level="ERROR",
                            )

            except Exception as rep_exc:
                failed_count += 1
                aborted = True
                repetition_results.append({
                    "repetition_index": rep_idx,
                    "run_id": str(child_run.id),
                    "status": "failed",
                    "result": "FAIL",
                    "error": str(rep_exc),
                })
                self._emit_event(
                    experiment.id,
                    run.id,
                    "error",
                    f"Repetition {rep_idx} encountered error: {rep_exc}. Aborting remaining repetitions.",
                    level="ERROR",
                )

        # Compute statistical aggregates across completed repetitions
        aggregated_stats = self._compute_aggregate_metrics(repetition_reports)
        run.aggregated_metrics = aggregated_stats

        # Parent pass/fail decision:
        # Parent passes iff ALL N repetitions completed and ALL passed criteria
        all_passed = (
            completed_count == total_repetitions
            and failed_count == 0
            and all(r.get("result") == "PASS" for r in repetition_results)
        )
        parent_result = "PASS" if all_passed else "FAIL"

        run.status = "succeeded" if all_passed else "failed"
        run.stage = "generating_report"
        run.finished_at = datetime.now(tz=timezone.utc)

        # Merge summary results onto parent run
        parent_results = dict(run.results or {})
        parent_results["result"] = parent_result
        parent_results["repetitions"] = total_repetitions
        parent_results["completed_repetitions"] = completed_count
        parent_results["failed_repetitions"] = failed_count
        parent_results["skipped_repetitions"] = skipped_count
        parent_results["repetition_results"] = repetition_results
        parent_results["aggregated_metrics"] = aggregated_stats

        phases = list(parent_results.get("phases") or [])
        fin_iso = run.finished_at.isoformat()
        for p in phases:
            p["state"] = "done"
            if not p.get("finishedAt"):
                p["finishedAt"] = fin_iso
        parent_results["phases"] = phases
        run.results = parent_results

        self._db.add(run)
        self._db.flush()

        parent_report = self._reporter.generate(run_id, self._db)
        self._db.commit()

        self._emit_event(
            experiment.id,
            run_id,
            "generating_report",
            f"All {total_repetitions} repetitions processed: completed={completed_count}, failed={failed_count}, skipped={skipped_count}. Parent result={parent_result}.",
        )
        return parent_report

    def _verify_target_health(
        self,
        target_service: str,
        health_fn: Callable | None = None,
        max_wait: float = 60.0,
    ) -> bool:
        """Poll target service health until healthy or max_wait seconds elapse."""
        start_t = time.monotonic()
        while time.monotonic() - start_t < max_wait:
            if self._cancelled:
                return False
            container_ok = self._docker.is_container_running(target_service)
            if container_ok:
                if health_fn is None:
                    return True
                try:
                    if health_fn():
                        return True
                except Exception:
                    pass
            time.sleep(1.0)
        return False

    def _execute_single_run(
        self,
        run: Any,
        experiment: Any,
        experiment_config: ExperimentModel,
        associated_pipeline_yaml: str = "",
        parent_run: Any = None,
    ) -> dict[str, Any]:
        """Execute a single repetition / run lifecycle."""
        target_service = experiment_config.target.service
        fault_type = experiment_config.fault.type
        fault_duration = experiment_config.fault.duration
        run_id = run.id

        global_timeout = fault_duration + _TIMEOUT_GRACE_SECONDS
        experiment_start = time.monotonic()
        injection_metadata: dict = {}
        recovery_time = None
        result = "FAIL"
        failed_criteria: list[str] = []

        try:
            # ── VALIDATE ──────────────────────────────────────────────────────
            self._update_status(run, "validating")
            self._emit_event(
                experiment.id,
                run_id,
                "validating",
                "Validating experiment configuration and safety rules.",
            )
            self._validate(experiment_config)
            self._emit_event(
                experiment.id,
                run_id,
                "validating",
                f"Configuration valid. Duration {fault_duration}s within limit.",
            )

            # ── PREPARE ───────────────────────────────────────────────────────
            self._update_status(run, "preparing")
            self._emit_event(
                experiment.id,
                run_id,
                "preparing",
                "Preparing execution environment and pipeline context.",
            )
            pipeline_context = self._prepare(associated_pipeline_yaml)
            self._emit_event(
                experiment.id,
                run_id,
                "preparing",
                "Pipeline execution context ready.",
            )

            # ── BASELINE ──────────────────────────────────────────────────────
            self._update_status(run, "baseline")
            self._emit_event(
                experiment.id,
                run_id,
                "baseline",
                "Capturing pre-fault operational baseline.",
            )
            baseline = self._metrics.capture_baseline(pipeline_context)
            self._merge_results(run, baseline)
            expected_records = baseline.get("expected_record_count", 0)
            self._emit_event(
                experiment.id,
                run_id,
                "baseline",
                f"Baseline captured. Expected record count: {expected_records}.",
            )

            # ── Get the fault injector ─────────────────────────────────────────
            injector_class = FAULT_INJECTOR_REGISTRY.get(fault_type)
            if injector_class is None:
                raise FaultInjectionError(
                    f"No fault injector registered for type '{fault_type}'.",
                    fault_type=fault_type,
                    target_service=target_service,
                )
            injector = injector_class(self._docker)

            # Snapshot pre-fault accounting counters immediately prior to injection
            pre_accounting = self._snapshot_pre_fault_accounting(run)
            run.pre_fault_accounting = pre_accounting
            if parent_run is not None and parent_run.pre_fault_accounting is None:
                parent_run.pre_fault_accounting = pre_accounting
                self._db.add(parent_run)
            self._db.add(run)
            self._db.flush()

            try:
                # ── INJECT FAILURE ─────────────────────────────────────────────
                self._update_status(run, "injecting")
                self._check_cancelled(run, experiment.id)
                self._emit_event(
                    experiment.id,
                    run_id,
                    "injecting",
                    f"Applying fault '{fault_type}' to '{target_service}' for {fault_duration}s.",
                )
                params = experiment_config.fault.model_dump()
                try:
                    injection_metadata = injector.inject(target_service, params)
                except Exception as exc:
                    logger.error("Fault injection failed for '%s': %s", target_service, exc)
                    self._emit_event(
                        experiment.id,
                        run_id,
                        "injecting",
                        f"Fault injection failed: {exc}",
                        level="ERROR",
                    )
                    error_str = str(exc)
                    if "tc" in error_str.lower() or "iproute2" in error_str.lower():
                        reason = "tc"
                    elif "stress-ng" in error_str.lower():
                        reason = "stress-ng"
                    elif "stress" in error_str.lower():
                        reason = "stress"
                    elif "not found" in error_str.lower() or "executable file not found" in error_str.lower():
                        reason = "required tool"
                    else:
                        reason = error_str
                    failed_criteria = [
                        f"The fault could not be applied: {reason} is not available in the {target_service} container, so the system was not tested."
                    ]
                    result = "FAIL"
                    self._merge_results(run, {
                        "result": result,
                        "failed_criteria": failed_criteria,
                        "error": str(exc),
                    })
                    self._update_status(run, "cleaning_up")
                    self._update_status(run, "generating_report")
                    run.finished_at = datetime.now(tz=timezone.utc)
                    run.status = "failed"
                    fin_iso = run.finished_at.isoformat()
                    phases = list(run.results.get("phases") or [])
                    for p in phases:
                        p["state"] = "done"
                        if not p.get("finishedAt"):
                            p["finishedAt"] = fin_iso
                    self._merge_results(run, {"phases": phases})
                    self._db.flush()
                    report = self._reporter.generate(run_id, self._db)
                    self._db.commit()
                    return report
                injected_at = (
                    injection_metadata.get("stopped_at")
                    or injection_metadata.get("started_at")
                    or injection_metadata.get("applied_at")
                    or datetime.now(tz=timezone.utc).isoformat()
                )
                injection_metadata["target_service"] = target_service
                injection_metadata["fault_type"] = fault_type
                injection_metadata["fault_duration"] = fault_duration
                injection_metadata["fault_injected_at"] = injected_at
                self._merge_results(run, injection_metadata)

                # Update FaultStateRegistry
                fault_state_registry.set_injected(
                    experiment_id=str(experiment.id),
                    target=target_service,
                    fault_type=fault_type,
                    duration_seconds=fault_duration,
                    injected_at=injected_at,
                )

                # Persist FaultInjection audit record
                self._persist_fault_injection(run, experiment_config, injection_metadata)
                self._emit_event(
                    experiment.id,
                    run_id,
                    "injecting",
                    f"Fault '{fault_type}' applied to '{target_service}'.",
                )

                # ── MONITOR ───────────────────────────────────────────────────
                self._update_status(run, "monitoring")
                self._check_cancelled(run, experiment.id)
                self._check_timeout(experiment_start, global_timeout)
                self._emit_event(
                    experiment.id,
                    run_id,
                    "monitoring",
                    f"Fault active. Sampling availability for {fault_duration}s.",
                )

                health_check_fn = self._get_health_check_fn(target_service, timeout_seconds=0.5)
                availability_data = self._metrics.capture_during_experiment(
                    target_service,
                    self._docker,
                    fault_duration,
                    is_cancelled_fn=lambda: self._cancelled,
                    health_check_fn=health_check_fn,
                )
                self._merge_results(run, availability_data)
                self._check_cancelled(run, experiment.id)
                self._emit_event(
                    experiment.id,
                    run_id,
                    "monitoring",
                    f"Availability monitoring complete: {availability_data.get('availability_percentage', 0.0)}% available.",
                )

            finally:
                # ── ROLLBACK (unconditional) ───────────────────────────────────
                self._update_status(run, "rolling_back")
                reverted_at = datetime.now(tz=timezone.utc).isoformat()
                fault_revert_monotonic = time.monotonic()
                self._merge_results(run, {"fault_reverted_at": reverted_at})
                fault_state_registry.set_recovering(str(experiment.id))
                self._emit_event(
                    experiment.id,
                    run_id,
                    "rolling_back",
                    f"Reverting fault '{fault_type}' on '{target_service}'.",
                )
                try:
                    injector.rollback(target_service, injection_metadata)
                    self._emit_event(
                        experiment.id,
                        run_id,
                        "rolling_back",
                        f"Rollback completed for '{target_service}'.",
                    )
                except Exception as rollback_exc:
                    logger.error(
                        "ExperimentRunner: rollback raised an exception: %s. "
                        "Manual recovery may be required for '%s'.",
                        rollback_exc,
                        target_service,
                    )
                    self._emit_event(
                        experiment.id,
                        run_id,
                        "rolling_back",
                        f"Rollback raised exception: {rollback_exc}",
                        level="ERROR",
                    )

            # ── WAIT FOR RECOVERY ─────────────────────────────────────────────
            self._update_status(run, "waiting_for_recovery")
            self._check_cancelled(run, experiment.id)
            self._check_timeout(experiment_start, global_timeout)
            self._emit_event(
                experiment.id,
                run_id,
                "waiting_for_recovery",
                f"Polling service health checks for '{target_service}'.",
            )

            remaining_timeout = global_timeout - (time.monotonic() - experiment_start)
            if remaining_timeout <= 0:
                self._check_timeout(experiment_start, global_timeout)

            health_check_fn = self._get_health_check_fn(target_service)

            recovery_time = self._metrics.measure_recovery_time(
                target_service,
                self._docker,
                poll_interval=1.0,
                max_wait=min(
                    experiment_config.validation.max_recovery_time + 30,
                    remaining_timeout,
                ),
                health_check_fn=health_check_fn,
                start_time=fault_revert_monotonic,
                is_cancelled_fn=lambda: self._cancelled,
            )
            recovered_at = datetime.now(tz=timezone.utc).isoformat() if recovery_time is not None else None
            self._merge_results(
                run,
                {
                    "recovery_time_seconds": recovery_time,
                    "recovered_at": recovered_at,
                },
            )
            fault_state_registry.set_recovered(str(experiment.id), recovery_time)

            if recovery_time is not None:
                from app.metrics import pipeline_recovery_time_seconds
                pipeline_recovery_time_seconds.labels(target_service).observe(recovery_time)
                self._emit_event(
                    experiment.id,
                    run_id,
                    "waiting_for_recovery",
                    f"Service '{target_service}' recovered in {recovery_time:.2f}s.",
                )
            else:
                self._emit_event(
                    experiment.id,
                    run_id,
                    "waiting_for_recovery",
                    f"Service '{target_service}' did not recover within timeout.",
                    level="WARNING",
                )

            # ── VALIDATE DATA ─────────────────────────────────────────────────
            self._update_status(run, "validating_data")
            self._check_cancelled(run, experiment.id)
            self._emit_event(
                experiment.id,
                run_id,
                "validating_data",
                "Verifying data completeness and zero record loss.",
            )
            if associated_pipeline_yaml:
                post_recovery_context = self._prepare(associated_pipeline_yaml)
                output_location = post_recovery_context.get("output_location", {})
                if not output_location:
                    raise RuntimeError(
                        "Post-recovery pipeline run completed without an output location."
                    )
                consistency = self._validator.validate(expected_records, output_location)
                consistency["data_validation"] = "completed"
            elif run.pipeline_run_id:
                # Step 8.3: Data consistency validation for active / streaming pipeline runs
                from app.models.pipeline_run import PipelineRun

                p_run = self._db.get(PipelineRun, run.pipeline_run_id)
                # Wait for active pipeline run to finish draining/processing if still running
                max_drain_wait = 360
                drain_start = time.monotonic()
                while (
                    p_run
                    and p_run.status in ("pending", "running")
                    and (time.monotonic() - drain_start) < max_drain_wait
                ):
                    time.sleep(2)
                    self._db.refresh(p_run)

                # Expected records equals verified pipeline output (Spark output_rows / HDFS rowsWritten),
                # NOT raw input rows, so filters/dedupes that legitimately reduce rows do not count as data loss.
                snapshot = p_run.last_progress_snapshot or {} if p_run else {}
                spark_out = (snapshot.get("spark") or {}).get("recordsOut")
                hdfs_written = (snapshot.get("hdfs") or {}).get("rowsWritten")
                k_published = (snapshot.get("kafka") or {}).get("published")
                if spark_out is None and hdfs_written is None and p_run:
                    try:
                        from app.pipeline.progress import tracker
                        snap = tracker.get(str(p_run.id))
                        if snap:
                            spark_out = (snap.get("spark") or {}).get("recordsOut")
                            hdfs_written = (snap.get("hdfs") or {}).get("rowsWritten")
                            k_published = (snap.get("kafka") or {}).get("published")
                    except Exception:
                        pass
                if spark_out is not None:
                    expected_records = int(spark_out)
                elif hdfs_written is not None:
                    expected_records = int(hdfs_written)
                else:
                    expected_records = int(k_published or 0)

                out_path = p_run.output_path if p_run else None
                if not out_path and p_run and p_run.config_yaml:
                    try:
                        p_cfg = yaml.safe_load(p_run.config_yaml) or {}
                        out_path = p_cfg.get("storage", {}).get("path")
                    except Exception:
                        pass
                if not out_path:
                    out_path = "/data/output/streaming_chaos_1m_run"

                storage_type = "hdfs" if (out_path.startswith("hdfs://") or out_path.startswith("/data")) else "local"
                output_location = {"storage_type": storage_type, "path": out_path}

                consistency = self._validator.validate(expected_records, output_location)
                consistency["data_validation"] = "completed"
                consistency["expected_record_count"] = expected_records
                consistency["expected_records"] = expected_records
                consistency["actual_records"] = consistency.get("actual_records", 0)
                consistency["processed_records"] = consistency.get("actual_records", 0)
            else:
                consistency = self._skipped_data_validation()
            self._merge_results(run, consistency)
            self._emit_event(
                experiment.id,
                run_id,
                "validating_data",
                f"Data validation: expected={expected_records}, actual={consistency.get('actual_records')}, lost={consistency.get('data_loss')}.",
            )

            # ── COLLECT METRICS ───────────────────────────────────────────────
            self._update_status(run, "collecting_metrics")
            self._emit_event(
                experiment.id,
                run_id,
                "collecting_metrics",
                "Computing throughput and persisting performance metrics.",
            )
            elapsed_total = time.monotonic() - experiment_start
            if run.pipeline_run_id is not None:
                throughput = self._metrics.compute_throughput(
                    records_processed=consistency.get("actual_records") or 0,
                    duration_seconds=elapsed_total,
                )
                self._merge_results(run, {"throughput_records_per_sec": throughput})
                self._metrics.persist_metrics(self._db, run_id, run.results or {})
                self._emit_event(
                    experiment.id,
                    run_id,
                    "collecting_metrics",
                    f"Computed throughput: {throughput} records/s over {elapsed_total:.1f}s.",
                )
            else:
                self._merge_results(
                    run,
                    {
                        "throughput_records_per_sec": None,
                        "throughput_baseline": None,
                        "throughput_during_fault": None,
                        "throughput_degradation_percent": None,
                        "error_rate": None,
                        "error_count": None,
                        "total_operations": None,
                        "expected_records": None,
                        "processed_records": None,
                        "expected_record_count": None,
                        "actual_records": None,
                        "data_loss": None,
                        "duplicate_ids_found": None,
                    },
                )
                self._emit_event(
                    experiment.id,
                    run_id,
                    "collecting_metrics",
                    "Throughput not measured (no pipeline run linked).",
                )

            # ── ANALYZE ───────────────────────────────────────────────────────
            self._update_status(run, "analyzing")
            self._emit_event(
                experiment.id,
                run_id,
                "analyzing",
                "Evaluating criteria against configured validation thresholds.",
            )
            data_loss_val = consistency.get("data_loss")
            result, failed_criteria = self._analyze(
                experiment_config=experiment_config,
                recovery_time=recovery_time,
                data_loss=data_loss_val,
            )
            evaluated = ["recovery_time"] + (["data_loss"] if data_loss_val is not None else [])
            unmeasured = ["data_loss"] if data_loss_val is None else []
            verdict_text = (
                f"Evaluated criteria: {', '.join(evaluated)}. "
                + (f"Unmeasured criteria: {', '.join(unmeasured)}. " if unmeasured else "")
                + (f"Failed: {'; '.join(failed_criteria)}." if failed_criteria else "All evaluated criteria passed.")
            )
            self._merge_results(
                run,
                {
                    "result": result,
                    "failed_criteria": failed_criteria,
                    "evaluated_criteria": evaluated,
                    "unmeasured_criteria": unmeasured,
                    "verdict_text": verdict_text,
                },
            )
            self._emit_event(
                experiment.id,
                run_id,
                "analyzing",
                f"Result: {result}." + (f" Failed criteria: {'; '.join(failed_criteria)}" if failed_criteria else " All criteria met."),
            )

            # ── CLEANUP ───────────────────────────────────────────────────────
            self._update_status(run, "cleaning_up")
            self._emit_event(
                experiment.id,
                run_id,
                "cleaning_up",
                "Cleaning up experiment test context and validating container status.",
            )

            # ── GENERATE REPORT ───────────────────────────────────────────────
            self._update_status(run, "generating_report")
            self._emit_event(
                experiment.id,
                run_id,
                "generating_report",
                "Assembling and persisting structured experiment report.",
            )
            run.finished_at = datetime.now(tz=timezone.utc)
            run.status = "succeeded" if result == "PASS" else "failed"

            fin_iso = run.finished_at.isoformat()
            phases = list(run.results.get("phases") or [])
            for p in phases:
                p["state"] = "done"
                if not p.get("finishedAt"):
                    p["finishedAt"] = fin_iso
            self._merge_results(run, {"phases": phases})
            self._db.flush()

            report = self._reporter.generate(run_id, self._db)
            from app.metrics import experiment_duration_seconds
            experiment_duration_seconds.labels(fault_type).observe(
                time.monotonic() - experiment_start
            )
            self._db.commit()

            self._emit_event(
                experiment.id,
                run_id,
                "generating_report",
                f"Report generated: report_id={report.get('id') or report.get('report_id')}.",
            )
            logger.info(
                "ExperimentRunner: run %s completed with result=%s.",
                run_id,
                result,
            )
            return report

        except Exception as exc:
            logger.error("ExperimentRunner: experiment %s failed: %s", experiment.id, exc)
            fault_state_registry.clear(str(experiment.id))
            if run is not None:
                run.status = "failed"
                run.finished_at = datetime.now(tz=timezone.utc)
                res = dict(run.results or {})
                res["error"] = str(exc)
                res["result"] = "FAIL"
                run.results = res
                try:
                    self._db.add(run)
                    self._db.commit()
                except Exception:
                    pass
            try:
                self._emit_event(
                    experiment.id,
                    run_id,
                    run.stage if run else "error",
                    f"Experiment failed: {exc}",
                    level="ERROR",
                )
            except Exception:
                pass
            raise

    # ── Private stage methods ──────────────────────────────────────────────────

    def _validate(self, experiment_config: ExperimentModel) -> None:
        """Validate the experiment config against the safety layer."""
        self._safety.validate_target(experiment_config.target.service)
        self._safety.validate_duration(experiment_config.fault.duration)

    @staticmethod
    def _extract_pipeline_output(result: dict, pipeline_yaml: str) -> dict:
        """Extract processed output metadata from executor metric shapes."""
        metrics = result.get("metrics", {})
        spark_job = metrics.get("spark_job")
        if isinstance(spark_job, dict) and spark_job.get("output_path"):
            storage_type = "hdfs" if spark_job["output_path"].startswith("hdfs://") else "local"
            try:
                import yaml

                storage_type = (
                    (yaml.safe_load(pipeline_yaml) or {})
                    .get("storage", {})
                    .get("type", storage_type)
                )
            except Exception:
                pass
            return {
                "records_processed": int(spark_job.get("output_rows") or 0),
                "output_location": {
                    "storage_type": storage_type,
                    "path": spark_job["output_path"],
                },
                "pipeline_run_result": result,
            }

        for metric_name, metric in metrics.items():
            if not metric_name.startswith("storage:") or not isinstance(metric, dict):
                continue
            path = metric.get("path") or metric.get("hdfs_path")
            if path:
                return {
                    "records_processed": int(metric.get("rows_written") or 0),
                    "output_location": {
                        "storage_type": metric_name.split(":", 1)[1],
                        "path": path,
                    },
                    "pipeline_run_result": result,
                }
        return {
            "records_processed": 0,
            "output_location": {},
            "pipeline_run_result": result,
        }

    @staticmethod
    def _skipped_data_validation() -> dict:
        """Return an explicit non-result when no pipeline was supplied."""
        return {
            "data_validation": "skipped",
            "expected_records": None,
            "actual_records": None,
            "data_loss": None,
            "data_loss_percentage": None,
            "duplicate_ids_found": None,
            "id_column": None,
            "validation_passed": None,
        }

    def _prepare(self, pipeline_yaml: str) -> dict:
        """
        Run the associated pipeline if YAML is provided, capture output context.
        """
        if not pipeline_yaml:
            return {"records_processed": 0, "output_location": {}}

        try:
            from app.services.pipeline_execution_service import run_pipeline_from_yaml

            result = run_pipeline_from_yaml(pipeline_yaml)
            if result.get("status") != "success":
                raise RuntimeError(
                    "Baseline pipeline run failed: "
                    f"{result.get('errors') or result.get('logs') or result.get('status')}"
                )
            return self._extract_pipeline_output(result, pipeline_yaml)
        except Exception as exc:
            raise RuntimeError(f"Pipeline preparation failed: {exc}") from exc

    def _analyze(
        self,
        experiment_config: ExperimentModel,
        recovery_time: float | None,
        data_loss: int | None,
    ) -> tuple[str, list[str]]:
        """
        Compare evaluated results against configured acceptance criteria.

        PASS Rule:
        1. All evaluated criteria must satisfy their configured thresholds.
        2. Unmeasured criteria (e.g. data_loss when no pipeline was run) are recorded
           as unmeasured and do NOT count as satisfied data loss validations.
        3. At least one operational criterion (e.g. recovery_time) must be evaluated and met.
        4. If any evaluated criterion is violated or recovery timed out, result is FAIL.
        """
        criteria = experiment_config.validation
        max_allowed_loss = criteria.expected_data_loss
        max_allowed_recovery = criteria.max_recovery_time

        failed_criteria: list[str] = []
        evaluated_criteria: list[str] = []
        unmeasured_criteria: list[str] = []

        # Data loss evaluation: evaluated only if measured
        if data_loss is not None:
            evaluated_criteria.append("data_loss")
            if data_loss > max_allowed_loss:
                failed_criteria.append(
                    f"Data loss of {data_loss} records exceeded the maximum allowed of {max_allowed_loss}."
                )
        else:
            unmeasured_criteria.append("data_loss")

        # Recovery time evaluation: must be measured and within threshold
        evaluated_criteria.append("recovery_time")
        if recovery_time is None:
            failed_criteria.append(
                f"Service did not recover within {max_allowed_recovery:.1f} s."
            )
        elif recovery_time > max_allowed_recovery:
            failed_criteria.append(
                f"Recovery time {recovery_time:.1f} s exceeded the maximum of {max_allowed_recovery:.1f} s."
            )

        if failed_criteria:
            logger.warning(
                "ExperimentRunner._analyze: FAIL — %s", "; ".join(failed_criteria)
            )
            return "FAIL", failed_criteria

        logger.info(
            "ExperimentRunner._analyze: PASS — evaluated criteria met: %s (unmeasured: %s)",
            evaluated_criteria,
            unmeasured_criteria,
        )
        return "PASS", []

    def _persist_fault_injection(
        self,
        run: Any,
        experiment_config: ExperimentModel,
        injection_metadata: dict,
    ) -> None:
        """Persist a FaultInjection audit record linked to the experiment run."""
        audit = {
            "fault_type": experiment_config.fault.type,
            "target_service": experiment_config.target.service,
            "fault_params": experiment_config.fault.model_dump(),
            "injection_metadata": injection_metadata,
            "injected_at": injection_metadata.get("stopped_at")
            or injection_metadata.get("started_at")
            or injection_metadata.get("applied_at")
            or datetime.now(tz=timezone.utc).isoformat(),
        }
        self._merge_results(run, {"fault_audit": audit})

    def _update_status(self, run: Any, status: str) -> None:
        """Persist the current lifecycle stage and progress through the 12 phases."""
        now_iso = datetime.now(tz=timezone.utc).isoformat()
        run.stage = status
        results = dict(run.results or {})
        phases = list(
            results.get("phases")
            or [
                {"name": p, "state": "pending", "startedAt": None, "finishedAt": None}
                for p in LIFECYCLE_PHASES
            ]
        )

        found = False
        for p in phases:
            if p["name"] == status:
                p["state"] = "active"
                if not p.get("startedAt"):
                    p["startedAt"] = now_iso
                found = True
            elif not found:
                p["state"] = "done"
                if not p.get("finishedAt"):
                    p["finishedAt"] = now_iso
            else:
                p["state"] = "pending"

        from sqlalchemy.orm.attributes import flag_modified

        results["phases"] = phases
        run.results = results
        flag_modified(run, "results")
        self._db.add(run)
        self._db.flush()
        self._db.commit()
        logger.debug("ExperimentRunner: run %s → status='%s'.", run.id, status)

    def _merge_results(self, run: Any, data: dict) -> None:
        """Merge ``data`` into ``run.results`` and flush to the DB."""
        from sqlalchemy.orm.attributes import flag_modified

        existing = dict(run.results or {})
        existing.update(data)
        run.results = existing
        flag_modified(run, "results")
        self._db.add(run)
        self._db.flush()
        self._db.commit()

    def _check_cancelled(self, run: Any, experiment_id: Any = None) -> None:
        """Raise RuntimeError if cancellation has been requested."""
        if self._cancelled:
            run.status = "cancelled"
            run.finished_at = datetime.now(tz=timezone.utc)
            self._db.add(run)
            self._db.flush()
            if experiment_id:
                fault_state_registry.clear(str(experiment_id))
                try:
                    self._emit_event(
                        experiment_id,
                        run.id,
                        run.stage or "cancelled",
                        "Experiment cancelled by operator.",
                        level="WARNING",
                    )
                except Exception:
                    pass
            raise RuntimeError("Experiment cancelled by operator.")

    @staticmethod
    def _get_health_check_fn(
        target_service: str,
        timeout_seconds: float | None = None,
    ) -> Callable | None:
        """Resolve deeper connectivity probe for the given target service."""
        import functools

        if "kafka" in target_service:
            try:
                from app.api.routes.health import check_kafka_connectivity

                if timeout_seconds is not None:
                    return functools.partial(
                        check_kafka_connectivity,
                        timeout_seconds=timeout_seconds,
                    )
                return check_kafka_connectivity
            except Exception:
                return None
        elif "spark" in target_service:
            try:
                from app.api.routes.health import check_spark_connectivity
                return check_spark_connectivity
            except Exception:
                return None
        elif "hdfs" in target_service or "namenode" in target_service or "datanode" in target_service:
            try:
                from app.api.routes.health import check_hdfs_connectivity
                return check_hdfs_connectivity
            except Exception:
                return None
        return None

    def _check_timeout(self, start: float, timeout: float) -> None:
        """Raise ExperimentTimeoutError if the global timeout has elapsed."""
        elapsed = time.monotonic() - start
        if elapsed >= timeout:
            raise ExperimentTimeoutError(timeout_seconds=int(timeout))

    def _snapshot_pre_fault_accounting(self, run: Any) -> dict[str, Any] | None:
        """
        Snapshot trusted pipeline counters immediately prior to fault injection.

        Returns None if no pipeline run is linked (standalone container fault).
        """
        if run.pipeline_run_id is None:
            return None

        from app.models.pipeline_run import PipelineRun

        p_run = self._db.get(PipelineRun, run.pipeline_run_id)
        raw_snapshot: dict[str, Any] = {}
        if p_run and p_run.last_progress_snapshot:
            raw_snapshot = dict(p_run.last_progress_snapshot)

        # Fallback to in-memory tracker if available
        try:
            from app.pipeline.progress import tracker

            snap = tracker.get_progress(str(run.pipeline_run_id))
            if snap:
                raw_snapshot = snap
        except Exception:
            pass

        source_info = raw_snapshot.get("source") or {}
        kafka_info = raw_snapshot.get("kafka") or {}
        spark_info = raw_snapshot.get("spark") or {}
        hdfs_info = raw_snapshot.get("hdfs") or {}

        records_received = (
            kafka_info.get("published")
            if kafka_info.get("published") is not None
            else (
                spark_info.get("recordsIn")
                if spark_info.get("recordsIn") is not None
                else source_info.get("rowsRead")
            )
        )
        records_processed = spark_info.get("recordsOut")
        records_written = hdfs_info.get("rowsWritten")

        return {
            "snapshot_timestamp": datetime.now(tz=timezone.utc).isoformat(),
            "pipeline_run_id": str(run.pipeline_run_id),
            "records_received": records_received,
            "records_processed": records_processed,
            "records_written": records_written,
        }

    @staticmethod
    def _compute_aggregate_metrics(
        repetition_reports: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """
        Compute statistical aggregations across completed repetitions.

        For N=1, sample std is 0.0.
        For N>1, sample std is computed with N-1 denominator.
        For N=0, all stats are None with n_samples=0.
        """
        metric_aliases: dict[str, list[str]] = {
            "recovery_time_seconds": ["recovery_time_seconds", "recoveryTimeSeconds"],
            "data_loss": ["data_loss", "dataLoss"],
            "duplicates": ["duplicates", "duplicate_ids_found"],
            "availability_percentage": ["availability_percentage", "availabilityPercentage"],
            "throughput_records_per_sec": ["throughput_records_per_sec", "average_throughput", "throughputAfter"],
            "error_rate": ["error_rate", "errorRate"],
        }
        aggregates: dict[str, Any] = {}

        for key, aliases in metric_aliases.items():
            raw_vals: list[float] = []
            for rep in repetition_reports:
                val = None
                for alias in aliases:
                    if alias in rep and rep[alias] is not None:
                        val = rep[alias]
                        break
                    results = rep.get("results") or {}
                    if alias in results and results[alias] is not None:
                        val = results[alias]
                        break
                if val is not None:
                    try:
                        raw_vals.append(float(val))
                    except (ValueError, TypeError):
                        pass

            n = len(raw_vals)
            if n == 0:
                aggregates[key] = {
                    "n_samples": 0,
                    "mean": None,
                    "std": None,
                    "min": None,
                    "max": None,
                }
            elif n == 1:
                aggregates[key] = {
                    "n_samples": 1,
                    "mean": round(raw_vals[0], 4),
                    "std": 0.0,
                    "min": round(raw_vals[0], 4),
                    "max": round(raw_vals[0], 4),
                }
            else:
                mean_val = sum(raw_vals) / n
                variance = sum((x - mean_val) ** 2 for x in raw_vals) / (n - 1)
                std_val = math.sqrt(variance)
                aggregates[key] = {
                    "n_samples": n,
                    "mean": round(mean_val, 4),
                    "std": round(std_val, 4),
                    "min": round(min(raw_vals), 4),
                    "max": round(max(raw_vals), 4),
                }

        return aggregates
