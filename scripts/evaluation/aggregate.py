#!/usr/bin/env python3
"""
scripts/evaluation/aggregate.py
-------------------------------
Deterministic statistical aggregator for ChaosFlow evaluation runs.

Reads ONLY from docs/evaluation/raw/<name>/ without database access.
Groups the 5 single-repetition experiments of each scenario, computes:
- n (sample count of measured values)
- mean
- sample std (Bessel-corrected N-1, or 0.0 if n == 1 or identical values)
- min
- max
- null_count (count of null / unmeasured values across repetitions)

Outputs deterministic CSVs with stable column ordering and fixed number formatting (4 decimal places).
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any


METRIC_NAMES = [
    "recovery_time_seconds",
    "throughput_normal_records_per_sec",
    "throughput_during_fault_records_per_sec",
    "throughput_pct_change",
    "data_loss_records",
    "duplicates_records",
    "fault_induced_duplicates_records",
]


def compute_sample_stats(values: list[float]) -> dict[str, Any]:
    """
    Compute n, mean, sample std (N-1), min, max, or None if empty.
    For N=1 or identical values, sample std is 0.0.
    """
    valid = [v for v in values if v is not None and not (isinstance(v, float) and math.isnan(v))]
    n = len(valid)
    if n == 0:
        return {
            "n": 0,
            "mean": None,
            "std": None,
            "min": None,
            "max": None,
        }

    mean_val = sum(valid) / n
    if n == 1:
        std_val = 0.0
    else:
        var = sum((x - mean_val) ** 2 for x in valid) / (n - 1)
        # Numerical guard against tiny negative due to floating point inaccuracies
        std_val = math.sqrt(max(0.0, var))

    return {
        "n": n,
        "mean": mean_val,
        "std": std_val,
        "min": min(valid),
        "max": max(valid),
    }


def extract_metrics_from_report(report_json: dict[str, Any]) -> dict[str, float | None]:
    """Extract standard evaluation metrics from a single repetition's report.json."""
    metrics: dict[str, float | None] = {}

    # 1. Comparison table parsing
    comp_table = report_json.get("comparison_table") or report_json.get("results", {}).get("comparison_table") or []
    by_metric = {row.get("metric"): row for row in comp_table if "metric" in row}

    # Recovery time
    rt_row = by_metric.get("recovery_time_seconds", {})
    metrics["recovery_time_seconds"] = rt_row.get("during_fault")

    # Throughput
    tp_row = by_metric.get("throughput_records_per_sec", {})
    metrics["throughput_normal_records_per_sec"] = tp_row.get("normal")
    metrics["throughput_during_fault_records_per_sec"] = tp_row.get("during_fault")
    metrics["throughput_pct_change"] = tp_row.get("pct_change")

    # Data loss
    dl_row = by_metric.get("data_loss", {})
    metrics["data_loss_records"] = dl_row.get("during_fault")

    # Duplicates
    dup_row = by_metric.get("duplicates", {})
    metrics["duplicates_records"] = dup_row.get("during_fault")

    # Fault induced duplicates
    fid_row = by_metric.get("fault_induced_duplicates", {})
    metrics["fault_induced_duplicates_records"] = fid_row.get("during_fault")

    # Fallback to direct fields if comparison table missing a metric
    res = report_json.get("results", report_json)
    if metrics["recovery_time_seconds"] is None and "recovery_time" in res:
        metrics["recovery_time_seconds"] = res.get("recovery_time")
    if metrics["data_loss_records"] is None and "data_loss" in res:
        metrics["data_loss_records"] = res.get("data_loss")

    return metrics


def aggregate_raw_data(raw_dir: Path) -> list[dict[str, Any]]:
    """
    Read all experiment runs under raw_dir, group by scenario (e.g. eval-kafka-stop),
    and aggregate each metric.
    """
    if not raw_dir.exists():
        return []

    # Map scenario -> list of run metric dicts
    scenario_runs: dict[str, list[dict[str, float | None]]] = defaultdict(list)

    for run_dir in sorted(raw_dir.iterdir()):
        if not run_dir.is_dir():
            continue
        report_file = run_dir / "report.json"
        if not report_file.exists():
            continue

        with open(report_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        m = extract_metrics_from_report(data)

        # Parse scenario name from dir: eval-<exp_id>-r<idx> -> eval-<exp_id>
        dir_name = run_dir.name
        if "-r" in dir_name:
            scenario_name = dir_name.rsplit("-r", 1)[0]
        else:
            scenario_name = dir_name

        scenario_runs[scenario_name].append(m)

    # Compute aggregates per scenario and per metric
    rows = []
    for scenario in sorted(scenario_runs.keys()):
        runs = scenario_runs[scenario]
        total_runs = len(runs)

        for metric in METRIC_NAMES:
            vals = [r.get(metric) for r in runs]
            stats = compute_sample_stats(vals)
            null_count = sum(1 for v in vals if v is None)

            rows.append({
                "scenario": scenario,
                "metric": metric,
                "repetitions_total": total_runs,
                "n_measured": stats["n"],
                "null_count": null_count,
                "mean": stats["mean"],
                "std": stats["std"],
                "min": stats["min"],
                "max": stats["max"],
            })

    return rows


def format_val(val: float | None) -> str:
    """Deterministic number formatting: 4 decimal places or empty string."""
    if val is None:
        return ""
    return f"{val:.4f}"


def write_aggregated_csv(rows: list[dict[str, Any]], output_path: Path) -> None:
    """Write rows to CSV deterministically with fixed formatting and CRLF/LF normalization."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "scenario",
        "metric",
        "repetitions_total",
        "n_measured",
        "null_count",
        "mean",
        "std",
        "min",
        "max",
    ]

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(fieldnames)
        for r in rows:
            writer.writerow([
                r["scenario"],
                r["metric"],
                r["repetitions_total"],
                r["n_measured"],
                r["null_count"],
                format_val(r["mean"]),
                format_val(r["std"]),
                format_val(r["min"]),
                format_val(r["max"]),
            ])


def main():
    parser = argparse.ArgumentParser(description="Deterministic Aggregator for ChaosFlow Evaluation")
    parser.add_argument("--raw-dir", default="docs/evaluation/raw", help="Directory containing raw runs")
    parser.add_argument("--output", default="docs/evaluation/summary_table.csv", help="Output summary CSV file")
    args = parser.parse_args()

    rows = aggregate_raw_data(Path(args.raw_dir))
    write_aggregated_csv(rows, Path(args.output))
    print(f"Aggregated {len(rows)} metric entries to {args.output}")


if __name__ == "__main__":
    main()
