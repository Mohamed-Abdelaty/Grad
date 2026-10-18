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
        has_pipeline = bool(run.pipeline_run_id) or bool(experiment and experiment.pipeline_yaml)

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

        tp_during_fault = results.get("throughput_during_fault")
        if tp_during_fault is None and has_pipeline:
            # For parent run, average child runs' throughput_during_fault and gather child samples
            if run.parent_run_id is None:
                from app.models.experiment_run import ExperimentRun
                children = db_session.query(ExperimentRun).filter(ExperimentRun.parent_run_id == run.id).all()
                child_fault_tp_vals = [
                    (c.results or {}).get("throughput_during_fault")
                    for c in children
                    if (c.results or {}).get("throughput_during_fault") is not None
                ]
                if child_fault_tp_vals:
                    tp_during_fault = round(sum(child_fault_tp_vals) / len(child_fault_tp_vals), 2)
                
                # If parent results has no samples, collect total samples from children
                if not results.get("samples"):
                    child_samples = []
                    for c in children:
                        c_samps = (c.results or {}).get("samples") or []
                        child_samples.extend(c_samps)
                    if child_samples:
                        results["samples"] = child_samples

                if results.get("output_duplicates") is None:
                    c_out_dups = [
                        (c.results or {}).get("output_duplicates")
                        for c in children
                        if (c.results or {}).get("output_duplicates") is not None
                    ]
                    if c_out_dups:
                        results["output_duplicates"] = round(sum(c_out_dups) / len(c_out_dups))

                if results.get("source_duplicates") is None:
                    c_src_dups = [
                        (c.results or {}).get("source_duplicates")
                        for c in children
                        if (c.results or {}).get("source_duplicates") is not None
                    ]
                    if c_src_dups:
                        results["source_duplicates"] = round(sum(c_src_dups) / len(c_src_dups))

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
            "baseline": results.get("baseline"),
            "pre_fault_accounting": run.pre_fault_accounting,
            "preFaultAccounting": run.pre_fault_accounting,
            "output_duplicates": results.get("output_duplicates"),
            "source_duplicates": results.get("source_duplicates"),
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
        - Every cell carries an explicit honest reason when null.
        - baseline_applicable is False for metrics without a fault-free counterpart (recovery time, data loss).
        - delta and pct_change are computed only when both normal and during_fault are real numbers.
        - Zero normal value results in pct_change = None (avoids division by zero).
        - When repetitions > 1, includes aggregate statistics (mean, std, min, max, n_samples).
        """
        recovery_time = results.get("recovery_time_seconds")
        baseline = results.get("baseline") or {}
        baseline_reasons = baseline.get("reasons") or {}

        # 1. Availability
        normal_avail = baseline.get("availability_percentage")
        normal_avail_n = baseline.get("availability_n_samples")
        normal_avail_reason = None if normal_avail is not None else baseline_reasons.get("availability_percentage", "not measured")
        fault_avail_reason = None if avail_pct is not None else "not measured"

        # 2. Throughput
        normal_tp = baseline.get("throughput_records_per_sec")
        normal_tp_n = baseline.get("throughput_n_samples")
        reps_cnt = results.get("repetitions") or 1
        if normal_tp is not None:
            if normal_tp_n == 1 and reps_cnt > 1:
                normal_tp_reason = f"measured in 1 of {reps_cnt} repetitions"
            else:
                normal_tp_reason = None
        else:
            normal_tp_reason = (
                baseline_reasons.get("throughput_records_per_sec")
                or ("no pipeline linked" if not has_pipeline else "pipeline idle / no progress in baseline window")
            )

        # Counter provenance and pipeline activity at injection
        counter_used = results.get("counter_used") or (
            "spark.recordsOut" if has_pipeline else None
        )
        pipeline_active_at_injection = results.get("pipeline_active_at_injection")
        if pipeline_active_at_injection is None and has_pipeline:
            # Check pre_fault_accounting or repetition results
            pre_acc = results.get("pre_fault_accounting")
            if pre_acc is not None:
                rec_in = pre_acc.get("records_received") or pre_acc.get("records_processed")
                pipeline_active_at_injection = bool(rec_in and rec_in > 0)
            else:
                # If no pre_fault_accounting provided (e.g. synthetic test call), assume True
                pipeline_active_at_injection = True
        elif not has_pipeline:
            pipeline_active_at_injection = None

        if not has_pipeline:
            fault_tp_val = None
            fault_tp_reason = "no pipeline linked"
        elif pipeline_active_at_injection is False:
            fault_tp_val = None
            fault_tp_reason = "pipeline not active at injection"
        elif tp_during_fault is not None:
            fault_tp_val = tp_during_fault
            fault_tp_reason = None
        else:
            fault_tp_val = None
            fault_tp_reason = "pipeline idle / no progress during fault"

        # 3. Data loss
        # Data loss is a fault-induced delta, baseline has no counterpart
        normal_data_loss = None
        normal_data_loss_reason = "n/a (by definition)"
        fault_loss_reason = None if data_loss_val is not None else ("no pipeline linked" if not has_pipeline else "not measured")

        # 4. Duplicates
        # Duplicates baseline is not measured during baseline phase; normal is null
        normal_dups = None
        normal_dups_reason = "not measured during baseline"
        out_dups = results.get("output_duplicates")
        src_dups = results.get("source_duplicates")
        if out_dups is not None and src_dups is not None:
            fault_induced_dups = max(0, out_dups - src_dups)
            fault_dups_reason = None
        elif duplicates_val is not None:
            fault_induced_dups = duplicates_val
            fault_dups_reason = None
        else:
            fault_induced_dups = None
            fault_dups_reason = "source or output duplicates unmeasured" if has_pipeline else "no pipeline linked"

        # 5. Recovery time
        normal_rec = None
        normal_rec_reason = "n/a (by definition)"
        fault_rec_reason = None if recovery_time is not None else "not measured"

        # Count availability samples dynamically from stored run/child samples
        raw_samples = results.get("samples") or []
        n_avail_samples = len(raw_samples) if raw_samples else (results.get("total_samples") or 0)
        rep_results = results.get("repetition_results") or []
        if not n_avail_samples and rep_results:
            n_avail_samples = sum(
                len(r.get("samples") or []) if isinstance(r, dict) else 0
                for r in rep_results
            )
        avail_def = (
            f"Percentage of availability probes returning operational healthy status during fault window "
            f"(target poll interval: 1.0s, deep probe timeout up to 5.0s when service unresponsive, observed samples: {n_avail_samples}). "
            "Note: For container_restart, Docker SDK restart blocks until the container is operational, "
            "so availability probes during the fault window may observe 100% availability."
        )

        rows_def = [
            {
                "metric": "availability_percentage",
                "label": "Availability",
                "unit": "%",
                "normal": normal_avail,
                "normal_reason": normal_avail_reason,
                "normal_n_samples": normal_avail_n if normal_avail is not None else None,
                "during_fault": avail_pct,
                "during_fault_reason": fault_avail_reason,
                "baseline_applicable": True,
                "measured_directly": True,
                "definition": avail_def,
                "agg_key": "availability_percentage",
            },
            {
                "metric": "throughput_records_per_sec",
                "label": "Throughput",
                "unit": "records/sec",
                "normal": normal_tp,
                "normal_reason": normal_tp_reason,
                "normal_n_samples": normal_tp_n if normal_tp is not None else None,
                "during_fault": fault_tp_val,
                "during_fault_reason": fault_tp_reason,
                "baseline_applicable": True,
                "measured_directly": fault_tp_val is not None,
                "definition": "Pipeline record processing or ingestion throughput during fault window.",
                "agg_key": "throughput_records_per_sec",
                "counter_used": counter_used,
                "pipeline_active_at_injection": pipeline_active_at_injection,
            },
            {
                "metric": "data_loss",
                "label": "Data Loss",
                "unit": "records",
                "normal": normal_data_loss,
                "normal_reason": normal_data_loss_reason,
                "normal_n_samples": None,
                "during_fault": data_loss_val,
                "during_fault_reason": fault_loss_reason,
                "baseline_applicable": False,
                "measured_directly": True if has_pipeline else False,
                "definition": "Difference between verified expected output records and actual output records.",
                "agg_key": "data_loss",
            },
            {
                "metric": "duplicates",
                "label": "Duplicate Records",
                "unit": "records",
                "normal": normal_dups,
                "normal_reason": normal_dups_reason,
                "normal_n_samples": None,
                "during_fault": fault_induced_dups,
                "during_fault_reason": fault_dups_reason,
                "baseline_applicable": False,
                "measured_directly": True if has_pipeline else False,
                "definition": "Count of duplicate record identifier keys observed in destination storage.",
                "agg_key": "duplicates",
                "output_duplicates": out_dups,
                "source_duplicates": src_dups,
                "fault_induced_duplicates": fault_induced_dups,
            },
            {
                "metric": "recovery_time_seconds",
                "label": "Recovery Time",
                "unit": "seconds",
                "normal": normal_rec,
                "normal_reason": normal_rec_reason,
                "normal_n_samples": None,
                "during_fault": recovery_time,
                "during_fault_reason": fault_rec_reason,
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
            if r.get("baseline_applicable", True) and norm is not None and fault_val is not None:
                try:
                    delta = round(float(fault_val) - float(norm), 2)
                    if float(norm) != 0.0:
                        pct_change = round(((float(fault_val) - float(norm)) / float(norm)) * 100.0, 2)
                    else:
                        pct_change = None  # Zero baseline MUST always result in null pct_change (never 0.0)
                except Exception:
                    delta = None
                    pct_change = None

            agg_stats = (aggregated_metrics or {}).get(r["agg_key"]) if aggregated_metrics else None
            if agg_stats is None and r["agg_key"] == "throughput_during_fault" and aggregated_metrics:
                agg_stats = aggregated_metrics.get("throughput_records_per_sec")

            row_entry: dict[str, Any] = {
                "metric": r["metric"],
                "label": r["label"],
                "unit": r["unit"],
                "normal": norm,
                "normal_reason": r.get("normal_reason"),
                "normalReason": r.get("normal_reason"),
                "normal_n_samples": r.get("normal_n_samples") if norm is not None else None,
                "normalNSamples": r.get("normal_n_samples") if norm is not None else None,
                "during_fault": fault_val,
                "duringFault": fault_val,
                "during_fault_reason": r.get("during_fault_reason"),
                "duringFaultReason": r.get("during_fault_reason"),
                "delta": delta,
                "pct_change": pct_change,
                "pctChange": pct_change,
                "baseline_applicable": r["baseline_applicable"],
                "baselineApplicable": r["baseline_applicable"],
                "measured_directly": r["measured_directly"],
                "measuredDirectly": r["measured_directly"],
                "definition": r["definition"],
            }
            if r["metric"] == "throughput_records_per_sec":
                row_entry["counter_used"] = r.get("counter_used")
                row_entry["counterUsed"] = r.get("counter_used")
                row_entry["pipeline_active_at_injection"] = r.get("pipeline_active_at_injection")
                row_entry["pipelineActiveAtInjection"] = r.get("pipeline_active_at_injection")

            if r["metric"] == "duplicates":
                row_entry["output_duplicates"] = r.get("output_duplicates")
                row_entry["outputDuplicates"] = r.get("output_duplicates")
                row_entry["source_duplicates"] = r.get("source_duplicates")
                row_entry["sourceDuplicates"] = r.get("source_duplicates")
                row_entry["fault_induced_duplicates"] = r.get("fault_induced_duplicates")
                row_entry["faultInducedDuplicates"] = r.get("fault_induced_duplicates")

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
