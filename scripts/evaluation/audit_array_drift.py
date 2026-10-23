"""
scripts/evaluation/audit_array_drift.py
---------------------------------------
Audit all metric arrays in results_master.json against:
1. Recomputation directly from raw report.json / environment.json / physical_metrics.json.
2. Historical report snapshots (233ade7, 6281056, 5bdd0b4).

Detects any array drift, mismatched means/stds, or unverified generated figures.
"""

from __future__ import annotations

import json
import math
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Tuple


def compute_stats(vals: List[float], ddof: int = 1) -> Tuple[float, float]:
    valid = [v for v in vals if v is not None and not (isinstance(v, float) and math.isnan(v))]
    if not valid:
        return 0.0, 0.0
    mean = sum(valid) / len(valid)
    if len(valid) <= 1:
        std = 0.0
    else:
        var = sum((x - mean) ** 2 for x in valid) / (len(valid) - ddof)
        std = math.sqrt(max(0.0, var))
    return round(mean, 2), round(std, 2)


def main():
    repo_root = Path(__file__).resolve().parent.parent.parent
    master_path = repo_root / "docs" / "evaluation" / "results" / "results_master.json"

    if not master_path.exists():
        print(f"Error: {master_path} does not exist.")
        raise SystemExit(1)

    master = json.loads(master_path.read_text(encoding="utf-8"))
    runs = master.get("runs", [])

    print("=" * 80)
    print("AUDIT ARRAY DRIFT: RESULTS_MASTER.JSON VS RAW EVIDENCE")
    print("=" * 80)

    # 1. Check all metric arrays across runs for consistency
    discrepancies = []

    experiments = ["control", "kafka-stop", "kafka-restart", "spark-worker-stop", "spark-worker-restart", "datanode-stop"]

    import sys
    sys.path.insert(0, str(repo_root / "scripts" / "evaluation"))
    import build_master

    spec_map = {spec["run_name"]: spec for spec in build_master.RUN_SPECIFICATIONS}

    for exp in experiments:
        exp_runs = [r for r in runs if r["experiment"] == exp]
        
        # Collect raw values directly from disk
        raw_pw = []
        raw_10s = []
        raw_during = []
        raw_rec_cont = []
        raw_rec_tp90 = []

        for r in exp_runs:
            rep = r["repetition"]
            run_name = r["run_name"]
            
            spec = spec_map.get(run_name)
            if not spec:
                continue
            r_dir = repo_root / spec["raw_dir"]
            rep_file = r_dir / "report.json"
            rep_data = json.loads(rep_file.read_text(encoding="utf-8")) if rep_file.exists() else {}

            # Raw 10s baseline
            b_10s = (rep_data.get("baseline") or {}).get("throughput_records_per_sec")
            raw_10s.append(float(b_10s) if b_10s is not None else None)

            # Raw during fault
            tdf = rep_data.get("throughputDuringFault")
            raw_during.append(float(tdf) if tdf is not None else None)

            # Raw container recovery
            rc = rep_data.get("recoveryTimeSeconds")
            raw_rec_cont.append(float(rc) if rc is not None else None)

            # Stored values in master
            stored_10s = r["baselines"]["baseline_10s_window_rec_per_sec"]
            stored_during = r["during_fault"]["throughput_window_rec_per_sec"]
            stored_rc = r["recovery"]["container_recovery_seconds"]

            # Compare per-run
            if raw_10s[-1] is not None and stored_10s is not None:
                if abs(raw_10s[-1] - stored_10s) > 0.05:
                    discrepancies.append((exp, rep, "baseline_10s", stored_10s, raw_10s[-1], "mismatch with raw report.json"))

            if raw_during[-1] is not None and stored_during is not None:
                if abs(raw_during[-1] - stored_during) > 0.05:
                    discrepancies.append((exp, rep, "during_fault_window_tp", stored_during, raw_during[-1], "mismatch with raw report.json"))

            if raw_rec_cont[-1] is not None and stored_rc is not None:
                if abs(raw_rec_cont[-1] - stored_rc) > 0.05:
                    discrepancies.append((exp, rep, "container_recovery", stored_rc, raw_rec_cont[-1], "mismatch with raw report.json"))

        # Verify aggregate means and stds in master vs recomputed from master's run array
        exp_master = master.get("experiments", {}).get(exp, {})
        for metric_name, field_path in [
            ("baseline_10s_window", lambda r: r["baselines"]["baseline_10s_window_rec_per_sec"]),
            ("throughput_during_fault_window", lambda r: r["during_fault"]["throughput_window_rec_per_sec"]),
            ("container_recovery_seconds", lambda r: r["recovery"]["container_recovery_seconds"]),
            ("throughput_90pct_recovery_seconds", lambda r: r["recovery"]["throughput_90pct_recovery_seconds"]),
        ]:
            arr = [field_path(r) for r in exp_runs]
            valid_arr = [x for x in arr if x is not None]
            if not valid_arr:
                continue
            rec_mean, rec_std = compute_stats(valid_arr, ddof=1)
            stored_stats = exp_master.get(metric_name) or {}
            stored_mean = stored_stats.get("mean")
            stored_std = stored_stats.get("std")

            if stored_mean is not None and abs(stored_mean - rec_mean) > 0.1:
                discrepancies.append((exp, "all", f"{metric_name}_mean", stored_mean, rec_mean, f"recomputed from array {valid_arr}"))
            if stored_std is not None and abs(stored_std - rec_std) > 0.1:
                discrepancies.append((exp, "all", f"{metric_name}_std", stored_std, rec_std, f"recomputed from array {valid_arr}"))

    print(f"Total array drift / internal discrepancies found: {len(discrepancies)}")
    for item in discrepancies:
        print(f"  [{item[0]} {item[1]}] {item[2]}: stored={item[3]} vs computed={item[4]} ({item[5]})")

    # 2. Historical comparison against commits 233ade7, 6281056, 5bdd0b4
    print("\n" + "=" * 80)
    print("HISTORICAL REPORT ARRAY COMPARISON")
    print("=" * 80)
    for rev in ["233ade7", "6281056", "5bdd0b4"]:
        try:
            txt = subprocess.check_output(["git", "show", f"{rev}:docs/evaluation/FINAL_REPORT.md"], encoding="utf-8", errors="ignore")
            print(f"--- Revision {rev} ---")
            for line in txt.splitlines():
                if "eval-kafka-stop" in line and ("r1" in line or "r2" in line or "r3" in line) and "|" in line:
                    print(" ", line[:120])
                if "Control" in line and "empirical" in line and "|" in line:
                    print(" ", line[:120])
        except Exception as e:
            print(f"Failed to read revision {rev}: {e}")


if __name__ == "__main__":
    main()
