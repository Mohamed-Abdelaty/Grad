"""
app/reliability/report_generator.py
--------------------------------------
Assembles and persists the final experiment report.

The report is assembled from persisted ``ExperimentRun.results`` (metrics)
and the ``ExperimentRun`` record itself, then written to the ``Report`` model.

Report JSON shape (canonical — used by both the API and Phase 8 frontend):

.. code-block:: json

    {
        "experiment_name": "Kafka Failure Test",
        "target": "kafka",
        "failure_type": "container_stop",
        "failure_duration_seconds": 20,
        "recovery_time_seconds": 12.4,
        "expected_records": 10000,
        "processed_records": 10000,
        "data_loss": 0,
        "average_throughput": 1250.0,
        "availability_percentage": 99.8,
        "result": "PASS"
    }
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)


class ReportGenerator:
    """
    Build and persist a structured experiment report.

    Reads from ``ExperimentRun.results`` (persisted by ``MetricsCollector``)
    and writes the canonical report dict to the ``Report`` model.
    """

    def generate(self, experiment_run_id: uuid.UUID, db_session) -> dict[str, Any]:
        """
        Assemble a complete experiment report from persisted run data.

        Parameters
        ----------
        experiment_run_id : uuid.UUID
            The completed experiment run to report on.
        db_session : sqlalchemy.orm.Session
            Active synchronous database session.

        Returns
        -------
        dict
            The canonical report structure (see module docstring).
        """
        from app.models.experiment_run import ExperimentRun
        from app.models.experiment import Experiment
        from app.models.report import Report

        run = db_session.get(ExperimentRun, experiment_run_id)
        if run is None:
            raise ValueError(f"ExperimentRun {experiment_run_id} not found.")

        # Retrieve the parent experiment for the name
        experiment = db_session.get(Experiment, run.experiment_id)
        experiment_name = experiment.name if experiment else "Unknown"

        results = run.results or {}

        params = run.params or {}
        exp_config = params.get("experiment_config") or {}
        validation_cfg = exp_config.get("validation") or {}
        target_service = results.get("target_service") or params.get("target_service") or "unknown"
        fault_type = results.get("fault_type") or params.get("fault_type") or "unknown"
        fault_duration = results.get("fault_duration") or params.get("fault_duration") or 0
        raw_samples = results.get("samples") or []
        avail_samples = [
            {"t": s["timestamp"], "available": s["available"]}
            for s in raw_samples
            if isinstance(s, dict) and "timestamp" in s and "available" in s
        ] if raw_samples else None

        avail_pct = results.get("availability_percentage")
        if avail_pct is not None:
            err_rate = round(max(0.0, min(1.0, (100.0 - float(avail_pct)) / 100.0)), 4)
        else:
            err_rate = None

        total_ops = results.get("total_samples", len(raw_samples) if raw_samples else 0)
        avail_ops = results.get("available_samples", sum(1 for s in raw_samples if s.get("available")))
        err_count = total_ops - avail_ops if total_ops is not None else 0

        # ── Extract metrics from results dict ──────────────────────────────────
        report_dict: dict[str, Any] = {
            "id": "",  # populated after insert
            "experiment_id": str(run.experiment_id),
            "experiment_run_id": str(experiment_run_id),
            "experiment_name": experiment_name,
            "target": target_service,
            "targetService": target_service,
            "failure_type": fault_type,
            "faultType": fault_type,
            "failure_duration_seconds": fault_duration,
            "durationSeconds": fault_duration,
            "faultParams": exp_config.get("fault", {}),
            "thresholds": {
                "expectedDataLoss": validation_cfg.get("expected_data_loss", 0),
                "maxRecoverySeconds": validation_cfg.get("max_recovery_time", 60.0),
            },
            "recovery_time_seconds": results.get("recovery_time_seconds"),
            "recoveryTimeSeconds": results.get("recovery_time_seconds"),
            "expected_records": results.get("expected_record_count", 0),
            "expectedRecords": results.get("expected_record_count", 0),
            "processed_records": results.get("actual_records", 0),
            "processedRecords": results.get("actual_records", 0),
            "data_loss": results.get("data_loss", 0),
            "dataLoss": results.get("data_loss", 0),
            "duplicates": results.get("duplicate_ids_found", 0),
            "average_throughput": results.get("throughput_records_per_sec", 0.0),
            "throughputBaseline": results.get("throughput_baseline"),
            "throughputDuringFault": None,
            "throughputAfter": results.get("throughput_records_per_sec"),
            "throughputDegradationPercent": results.get("throughput_degradation_percent"),
            "availability_percentage": avail_pct,
            "availabilityPercentage": avail_pct,
            "error_rate": err_rate,
            "errorRate": err_rate,
            "errorCount": err_count,
            "totalOperations": total_ops,
            "cpuAvgPercent": None,
            "memoryAvgBytes": None,
            "result": results.get("result", "UNKNOWN"),
            "failed_criteria": results.get("failed_criteria", []),
            "failedCriteria": results.get("failed_criteria", []),
            "started_at": run.started_at.isoformat() if run.started_at else None,
            "finished_at": run.finished_at.isoformat() if run.finished_at else None,
            "status": run.status,
            "pipelineRunId": str(run.pipeline_run_id) if run.pipeline_run_id else None,
            "phases": results.get("phases", []),
            "series": {
                "availabilitySamples": avail_samples,
                "throughputSamples": None,
                "cpuSamples": None,
                "memorySamples": None,
            },
            "results": {
                "recoveryTimeSeconds": results.get("recovery_time_seconds"),
                "dataLoss": results.get("data_loss", 0),
                "duplicates": results.get("duplicate_ids_found", 0),
                "expectedRecords": results.get("expected_record_count", 0),
                "processedRecords": results.get("actual_records", 0),
                "throughputBaseline": results.get("throughput_baseline"),
                "throughputDuringFault": None,
                "throughputAfter": results.get("throughput_records_per_sec"),
                "throughputDegradationPercent": results.get("throughput_degradation_percent"),
                "availabilityPercentage": avail_pct,
                "errorRate": err_rate,
                "errorCount": err_count,
                "totalOperations": total_ops,
                "availabilitySamples": avail_samples,
                "throughputSamples": None,
                "cpuSamples": None,
                "memorySamples": None,
            },
        }

        # ── Persist to Report model ────────────────────────────────────────────
        report = Report(
            title=f"Experiment Report: {experiment_name}",
            report_type="chaos_experiment",
            content=None,
            storage_uri=None,
            metadata_=report_dict,
        )
        db_session.add(report)
        db_session.flush()
        db_session.refresh(report)

        report_dict["id"] = str(report.id)
        report_dict["report_id"] = str(report.id)
        logger.info(
            "ReportGenerator: report %s generated for run %s (result=%s).",
            report.id,
            experiment_run_id,
            report_dict["result"],
        )
        return report_dict
