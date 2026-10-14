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
import time
import uuid
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
        except Exception:
            pass

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
            )
            self._db.add(experiment)
            self._db.flush()

        initial_phases = [
            {"name": p, "state": "pending", "startedAt": None, "finishedAt": None}
            for p in LIFECYCLE_PHASES
        ]

        if existing_run_id:
            run = self._db.get(ExperimentRun, existing_run_id)
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
            self._db.add(run)
            self._db.flush()
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
                injection_metadata = injector.inject(target_service, params)
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

                availability_data = self._metrics.capture_during_experiment(
                    target_service, self._docker, fault_duration
                )
                self._merge_results(run, availability_data)
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

            recovery_time = self._metrics.measure_recovery_time(
                target_service,
                self._docker,
                poll_interval=1.0,
                max_wait=min(
                    experiment_config.validation.max_recovery_time + 30,
                    remaining_timeout,
                ),
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

            # ── ANALYZE ───────────────────────────────────────────────────────
            self._update_status(run, "analyzing")
            self._emit_event(
                experiment.id,
                run_id,
                "analyzing",
                "Evaluating criteria against configured validation thresholds.",
            )
            result, failed_criteria = self._analyze(
                experiment_config=experiment_config,
                recovery_time=recovery_time,
                data_loss=consistency.get("data_loss") or 0,
            )
            self._merge_results(run, {"result": result, "failed_criteria": failed_criteria})
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
            try:
                self._emit_event(
                    experiment.id,
                    run_id,
                    run.stage or "error",
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
        data_loss: int,
    ) -> tuple[str, list[str]]:
        """
        Compare results against validation criteria and return (PASS/FAIL, failed_criteria).
        """
        criteria = experiment_config.validation
        max_allowed_loss = criteria.expected_data_loss
        max_allowed_recovery = criteria.max_recovery_time

        failed_criteria: list[str] = []

        if data_loss > max_allowed_loss:
            failed_criteria.append(
                f"Data loss of {data_loss} records exceeded the maximum allowed of {max_allowed_loss}."
            )

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

        logger.info("ExperimentRunner._analyze: PASS — all criteria met.")
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

        results["phases"] = phases
        run.results = results
        self._db.add(run)
        self._db.flush()
        logger.debug("ExperimentRunner: run %s → status='%s'.", run.id, status)

    def _merge_results(self, run: Any, data: dict) -> None:
        """Merge ``data`` into ``run.results`` and flush to the DB."""
        existing = dict(run.results or {})
        existing.update(data)
        run.results = existing
        self._db.add(run)
        self._db.flush()

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

    def _check_timeout(self, start: float, timeout: float) -> None:
        """Raise ExperimentTimeoutError if the global timeout has elapsed."""
        elapsed = time.monotonic() - start
        if elapsed >= timeout:
            raise ExperimentTimeoutError(timeout_seconds=int(timeout))
