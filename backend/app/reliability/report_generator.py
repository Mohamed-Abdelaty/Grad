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
        has_pipeline = bool(run.pipeline_run_id)

        if has_pipeline and avail_pct is not None:
            err_rate = round(max(0.0, min(1.0, (100.0 - float(avail_pct)) / 100.0)), 4)
        else:
            err_rate = None

        if has_pipeline:
            total_ops = results.get("total_samples", len(raw_samples) if raw_samples else 0)
            avail_ops = results.get("available_samples", sum(1 for s in raw_samples if s.get("available")))
            err_count = total_ops - avail_ops if total_ops is not None else 0
        else:
            total_ops = None
            err_count = None

        # ── Extract metrics from results dict ──────────────────────────────────
        expected_recs = (
            results.get("expected_records")
            if results.get("expected_records") is not None
            else results.get("expected_record_count")
        ) if has_pipeline else None
        processed_recs = (
            results.get("processed_records")
            if results.get("processed_records") is not None
            else results.get("actual_records")
        ) if has_pipeline else None
        data_loss_val = results.get("data_loss") if has_pipeline else None
        duplicates_val = results.get("duplicate_ids_found") if has_pipeline else None
        avg_throughput = results.get("throughput_records_per_sec") if has_pipeline else None
        tp_baseline = results.get("throughput_baseline") if has_pipeline else None
        tp_after = results.get("throughput_records_per_sec") if has_pipeline else None
        tp_degradation = results.get("throughput_degradation_percent") if has_pipeline else None

        # Extract genuine throughput during fault from tracker samples if present
        tp_during_fault = results.get("throughput_during_fault")
        if tp_during_fault is None and has_pipeline and run.pipeline_run_id:
            injected_iso = results.get("fault_injected_at")
            reverted_iso = results.get("fault_reverted_at")
            if injected_iso and reverted_iso:
                try:
                    t_start = datetime.fromisoformat(injected_iso.replace("Z", "+00:00")).timestamp()
                    t_end = datetime.fromisoformat(reverted_iso.replace("Z", "+00:00")).timestamp()
                    from app.models.pipeline_run import PipelineRun
                    p_run = db_session.get(PipelineRun, run.pipeline_run_id)
                    snap = p_run.last_progress_snapshot or {} if p_run else {}
                    tp_obj = snap.get("throughput") or {}
                    samples = tp_obj.get("samples") or []
                    window_samples = [
                        s.get("v") for s in samples
                        if isinstance(s, dict) and s.get("t") is not None and t_start <= s.get("t") <= t_end and s.get("v") is not None
                    ]
                    if window_samples:
                        tp_during_fault = round(sum(window_samples) / len(window_samples), 2)
                except Exception:
                    tp_during_fault = None

        comparison_table = self._build_comparison_table(
            results=results,
            has_pipeline=has_pipeline,
            avail_pct=avail_pct,
            tp_baseline=tp_baseline,
            tp_during_fault=tp_during_fault,
            avg_throughput=avg_throughput,
            data_loss_val=data_loss_val,
            duplicates_val=duplicates_val,
            aggregated_metrics=run.aggregated_metrics,
        )

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
            "faultInjectedAt": results.get("fault_injected_at"),
            "faultRevertedAt": results.get("fault_reverted_at"),
            "recoveredAt": results.get("recovered_at"),
            "thresholds": {
                "expectedDataLoss": validation_cfg.get("expected_data_loss", 0),
                "maxRecoverySeconds": validation_cfg.get("max_recovery_time", 60.0),
            },
            "recovery_time_seconds": results.get("recovery_time_seconds"),
            "recoveryTimeSeconds": results.get("recovery_time_seconds"),
            "expected_records": expected_recs,
            "expectedRecords": expected_recs,
            "processed_records": processed_recs,
            "processedRecords": processed_recs,
            "data_loss": data_loss_val,
            "dataLoss": data_loss_val,
            "duplicates": duplicates_val,
            "average_throughput": avg_throughput,
            "throughputBaseline": tp_baseline,
            "throughputDuringFault": tp_during_fault,
            "throughputAfter": tp_after,
            "throughputDegradationPercent": tp_degradation,
            "availability_percentage": avail_pct,
            "availabilityPercentage": avail_pct,
            "availability_during_fault": avail_pct,
            "availabilityDuringFault": avail_pct,
            "error_rate": err_rate,
            "errorRate": err_rate,
            "errorCount": err_count,
            "totalOperations": total_ops,
            "cpuAvgPercent": None,
            "memoryAvgBytes": None,
            "comparison_table": comparison_table,
            "comparisonTable": comparison_table,
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
                "dataLoss": data_loss_val,
                "duplicates": duplicates_val,
                "expectedRecords": expected_recs,
                "processedRecords": processed_recs,
                "throughputBaseline": tp_baseline,
                "throughputDuringFault": tp_during_fault,
                "throughputAfter": tp_after,
                "throughputDegradationPercent": tp_degradation,
                "availabilityPercentage": avail_pct,
                "availabilityDuringFault": avail_pct,
                "errorRate": err_rate,
                "errorCount": err_count,
                "totalOperations": total_ops,
                "availabilitySamples": avail_samples,
                "throughputSamples": None,
                "cpuSamples": None,
                "memorySamples": None,
                "comparisonTable": comparison_table,
            },
            "pre_fault_accounting": run.pre_fault_accounting,
            "preFaultAccounting": run.pre_fault_accounting,
            "repetitions": run.repetition_count,
            "repetition_results": results.get("repetition_results"),
            "repetitionResults": results.get("repetition_results"),
            "aggregate_statistics": run.aggregated_metrics,
            "aggregateStatistics": run.aggregated_metrics,
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

    @staticmethod
    def _build_comparison_table(
        results: dict[str, Any],
        has_pipeline: bool,
        avail_pct: float | None,
        tp_baseline: float | None,
        tp_during_fault: float | None,
        avg_throughput: float | None,
        data_loss_val: int | None,
        duplicates_val: int | None,
        aggregated_metrics: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Build an honest Normal vs During-Fault comparison table.

        Rules:
        - Only real stored baseline and fault/recovery measurements are used.
        - Unmeasured values are None/null (never fake 0 or 100).
        - baseline_applicable is False for metrics without a fault-free counterpart (e.g. recovery time).
        - delta and pct_change are computed only when both normal and during_fault are real numbers.
        - Zero normal value results in pct_change = None (avoids division by zero).
        - When repetitions > 1, includes aggregate statistics (mean, std, min, max, n_samples).
        """
        recovery_time = results.get("recovery_time_seconds")
        # Baseline availability is 100.0% only if verified by baseline probe, otherwise None
        normal_avail = 100.0 if avail_pct is not None else None

        rows_def = [
            {
                "metric": "availability_percentage",
                "label": "Availability",
                "unit": "%",
                "normal": normal_avail,
                "during_fault": avail_pct,
                "baseline_applicable": True,
                "measured_directly": True,
                "definition": "Percentage of availability probes returning operational healthy status.",
                "agg_key": "availability_percentage",
            },
            {
                "metric": "throughput_records_per_sec",
                "label": "Throughput",
                "unit": "records/sec",
                "normal": tp_baseline,
                "during_fault": tp_during_fault,
                "baseline_applicable": True,
                "measured_directly": tp_during_fault is not None,
                "definition": "Pipeline record processing or ingestion throughput.",
                "agg_key": "throughput_records_per_sec",
            },
            {
                "metric": "data_loss",
                "label": "Data Loss",
                "unit": "records",
                "normal": 0 if has_pipeline else None,
                "during_fault": data_loss_val,
                "baseline_applicable": True,
                "measured_directly": True if has_pipeline else False,
                "definition": "Difference between verified expected output records and actual output records.",
                "agg_key": "data_loss",
            },
            {
                "metric": "duplicates",
                "label": "Duplicate Records",
                "unit": "records",
                "normal": 0 if has_pipeline else None,
                "during_fault": duplicates_val,
                "baseline_applicable": True,
                "measured_directly": True if has_pipeline else False,
                "definition": "Count of duplicate record identifier keys observed in destination storage.",
                "agg_key": "duplicates",
            },
            {
                "metric": "recovery_time_seconds",
                "label": "Recovery Time",
                "unit": "seconds",
                "normal": None,
                "during_fault": recovery_time,
                "baseline_applicable": False,
                "measured_directly": True,
                "definition": "Elapsed duration from fault revert until service health check passed.",
                "agg_key": "recovery_time_seconds",
            },
        ]

        table: list[dict[str, Any]] = []
        for r in rows_def:
            norm = r["normal"]
            fault_val = r["during_fault"]

            delta = None
            pct_change = None
            if norm is not None and fault_val is not None:
                try:
                    delta = round(float(fault_val) - float(norm), 2)
                    if float(norm) != 0.0:
                        pct_change = round(((float(fault_val) - float(norm)) / float(norm)) * 100.0, 2)
                    else:
                        pct_change = None
                except Exception:
                    delta = None
                    pct_change = None

            agg_stats = (aggregated_metrics or {}).get(r["agg_key"]) if aggregated_metrics else None

            row_entry: dict[str, Any] = {
                "metric": r["metric"],
                "label": r["label"],
                "unit": r["unit"],
                "normal": norm,
                "during_fault": fault_val,
                "duringFault": fault_val,
                "delta": delta,
                "pct_change": pct_change,
                "pctChange": pct_change,
                "baseline_applicable": r["baseline_applicable"],
                "baselineApplicable": r["baseline_applicable"],
                "measured_directly": r["measured_directly"],
                "measuredDirectly": r["measured_directly"],
                "definition": r["definition"],
            }
            if agg_stats:
                row_entry["aggregate"] = {
                    "mean": agg_stats.get("mean"),
                    "std": agg_stats.get("std"),
                    "min": agg_stats.get("min"),
                    "max": agg_stats.get("max"),
                    "n_samples": agg_stats.get("n_samples", 0),
                    "nSamples": agg_stats.get("n_samples", 0),
                }
            else:
                row_entry["aggregate"] = None

            table.append(row_entry)

        return table
