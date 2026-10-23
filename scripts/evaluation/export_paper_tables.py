"""
scripts/evaluation/export_paper_tables.py
-----------------------------------------
Exports paper_tables.csv and per_run_metrics.csv from results_master.json.
Guarantees flat CSV representation for paper submission and external analysis.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path


def export_csvs(base_path: Path):
    results_dir = base_path / "docs" / "evaluation" / "results"
    master_path = results_dir / "results_master.json"
    if not master_path.exists():
        raise FileNotFoundError(f"Missing master file: {master_path}")

    master = json.loads(master_path.read_text(encoding="utf-8"))
    exps = master["experiments"]
    runs = master["runs"]

    # 1. paper_tables.csv
    paper_rows = []
    headers = [
        "experiment",
        "target_container",
        "baseline_pw_mean",
        "baseline_pw_std",
        "baseline_10s_mean",
        "baseline_10s_std",
        "during_fault_mean",
        "during_fault_std",
        "degradation_pw_pct",
        "degradation_ctrl_pct",
        "rec_container_mean",
        "rec_container_std",
        "rec_tp90_mean",
        "rec_tp90_std",
        "missing_rows",
        "extra_rows",
        "excess_dups",
        "validator_output",
        "executed_n",
        "survived_n",
        "complete_n",
    ]

    for exp_id in ["control", "kafka-stop", "kafka-restart", "spark-worker-stop", "spark-worker-restart", "datanode-stop"]:
        e = exps[exp_id]
        
        # Target container
        if exp_id == "control":
            tgt = "None"
            pv_out = "not run"
        elif exp_id == "kafka-stop" or exp_id == "kafka-restart":
            tgt = "platform-kafka"
            pv_out = f"{e['platform_validator_data_loss']['mean']:.1f}" if e['platform_validator_data_loss']['mean'] is not None else "0.0"
        elif exp_id == "spark-worker-stop" or exp_id == "spark-worker-restart":
            tgt = "platform-spark-worker"
            pv_out = f"{e['platform_validator_data_loss']['mean']:.1f}" if e['platform_validator_data_loss']['mean'] is not None else "0.0"
        elif exp_id == "datanode-stop":
            tgt = "platform-datanode"
            pv_out = f"{e['platform_validator_data_loss']['mean']:.1f}" if e['platform_validator_data_loss']['mean'] is not None else "0.0"

        pw_m = e["post_warmup_baseline"]["mean"]
        pw_s = e["post_warmup_baseline"]["std"]
        b10_m = e["baseline_10s_window"]["mean"]
        b10_s = e["baseline_10s_window"]["std"]
        df_m = e["throughput_during_fault_window"]["mean"]
        df_s = e["throughput_during_fault_window"]["std"]
        deg_pw = e["degradation_ratio_of_means_vs_post_warmup"]
        deg_ctrl = e["degradation_ratio_of_means_vs_control"]
        rc_m = e["container_recovery_seconds"]["mean"]
        rc_s = e["container_recovery_seconds"]["std"]
        rt_m = e["throughput_90pct_recovery_seconds"]["mean"]
        rt_s = e["throughput_90pct_recovery_seconds"]["std"]
        miss_m = e["missing_rows"]["mean"]
        extra_m = e["extra_rows"]["mean"]
        dups_m = e["excess_duplicates"]["mean"]
        exec_n = e["n_runs"]
        surv_n = e["survived_count"]
        comp_n = e["data_complete_count"]

        paper_rows.append({
            "experiment": exp_id,
            "target_container": tgt,
            "baseline_pw_mean": f"{pw_m:.2f}" if pw_m is not None else "",
            "baseline_pw_std": f"{pw_s:.2f}" if pw_s is not None else "",
            "baseline_10s_mean": f"{b10_m:.2f}" if b10_m is not None else "",
            "baseline_10s_std": f"{b10_s:.2f}" if b10_s is not None else "",
            "during_fault_mean": f"{df_m:.2f}" if df_m is not None else "",
            "during_fault_std": f"{df_s:.2f}" if df_s is not None else "",
            "degradation_pw_pct": f"{deg_pw:.1f}" if deg_pw is not None else "",
            "degradation_ctrl_pct": f"{deg_ctrl:.1f}" if deg_ctrl is not None else "",
            "rec_container_mean": f"{rc_m:.2f}" if rc_m is not None else "",
            "rec_container_std": f"{rc_s:.2f}" if rc_s is not None else "",
            "rec_tp90_mean": f"{rt_m:.2f}" if rt_m is not None else "",
            "rec_tp90_std": f"{rt_s:.2f}" if rt_s is not None else "",
            "missing_rows": f"{miss_m:.1f}" if miss_m is not None else "0.0",
            "extra_rows": f"{extra_m:.1f}" if extra_m is not None else "0.0",
            "excess_dups": f"{dups_m:.1f}" if dups_m is not None else "0.0",
            "validator_output": pv_out,
            "executed_n": exec_n,
            "survived_n": surv_n,
            "complete_n": comp_n,
        })

    paper_csv_path = results_dir / "paper_tables.csv"
    with open(paper_csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(paper_rows)
    print(f"Wrote {paper_csv_path}")

    # 2. per_run_metrics.csv
    run_headers = [
        "run_name",
        "experiment",
        "repetition",
        "target_container",
        "target_service",
        "fault_type",
        "pipeline_run_id",
        "driver_status",
        "platform_status",
        "validator_verdict",
        "physical_loss_verdict",
        "query_survived",
        "data_complete",
        "physical_committed_rows",
        "missing_rows",
        "extra_rows",
        "excess_duplicates",
        "platform_validator_output",
        "post_warmup_baseline_rec_per_sec",
        "baseline_10s_window_rec_per_sec",
        "during_fault_throughput_rec_per_sec",
        "container_recovery_seconds",
        "throughput_90pct_recovery_seconds",
        "stop_latency_seconds",
        "fault_window_seconds",
        "host_memory_pressure",
        "host_min_ram_observed_mb",
    ]

    run_rows = []
    for r in runs:
        run_rows.append({
            "run_name": r["run_name"],
            "experiment": r["experiment"],
            "repetition": r["repetition"],
            "target_container": r["target_container"],
            "target_service": r["target_service"],
            "fault_type": r["fault_type"],
            "pipeline_run_id": r["pipeline_run_id"] or "",
            "driver_status": r["statuses"]["driver_status"],
            "platform_status": r["statuses"]["platform_status"],
            "validator_verdict": r["statuses"]["validator_verdict"],
            "physical_loss_verdict": r["statuses"]["physical_loss_verdict"],
            "query_survived": r["statuses"]["query_survived"],
            "data_complete": r["statuses"]["data_complete"],
            "physical_committed_rows": r["data_integrity"]["physical_committed_rows"],
            "missing_rows": r["data_integrity"]["missing_rows"],
            "extra_rows": r["data_integrity"]["extra_rows"],
            "excess_duplicates": r["data_integrity"]["excess_duplicates"],
            "platform_validator_output": r["validator"]["output_data_loss"] if r["validator"]["output_data_loss"] is not None else "not run" if r["experiment"] == "control" else "",
            "post_warmup_baseline_rec_per_sec": r["baselines"]["post_warmup_baseline_rec_per_sec"] or "",
            "baseline_10s_window_rec_per_sec": r["baselines"]["baseline_10s_window_rec_per_sec"] or "",
            "during_fault_throughput_rec_per_sec": r["during_fault"]["throughput_window_rec_per_sec"] if r["during_fault"]["throughput_window_rec_per_sec"] is not None else "",
            "container_recovery_seconds": r["recovery"]["container_recovery_seconds"] if r["recovery"]["container_recovery_seconds"] is not None else "",
            "throughput_90pct_recovery_seconds": r["recovery"]["throughput_90pct_recovery_seconds"] if r["recovery"]["throughput_90pct_recovery_seconds"] is not None else "",
            "stop_latency_seconds": r["timestamps"]["stop_latency_seconds"] or "",
            "fault_window_seconds": r["timestamps"]["fault_window_seconds"] or "",
            "host_memory_pressure": r["host_environment"]["host_memory_pressure"],
            "host_min_ram_observed_mb": r["host_environment"]["host_min_ram_observed_mb"] or "",
        })

    run_csv_path = results_dir / "per_run_metrics.csv"
    with open(run_csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=run_headers)
        writer.writeheader()
        writer.writerows(run_rows)
    print(f"Wrote {run_csv_path}")


def main():
    base_dir = Path(__file__).resolve().parent.parent.parent
    export_csvs(base_dir)


if __name__ == "__main__":
    main()
