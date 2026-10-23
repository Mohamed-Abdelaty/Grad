"""Unit tests for report consistency, linter execution, and results_master.json validity.
"""
from __future__ import annotations

import json
import math
from pathlib import Path
import subprocess
import sys
import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
DOCS_DIR = REPO_ROOT / "docs" / "evaluation"
RESULTS_DIR = DOCS_DIR / "results"
MASTER_JSON = RESULTS_DIR / "results_master.json"


def test_master_json_structure():
    assert MASTER_JSON.exists(), f"results_master.json missing at {MASTER_JSON}"
    with open(MASTER_JSON, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    assert "metadata" in data
    assert "experiments" in data
    assert "runs" in data
    assert len(data["runs"]) == 30
    assert len(data["experiments"]) == 6
    assert data["metadata"]["statistical_convention"]["control_during_fault_reference_rec_per_sec"] == 1498.1


def test_full_row_multiset_integrity():
    with open(MASTER_JSON, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    for run in data["runs"]:
        integ = run["data_integrity"]
        survived = run["statuses"]["query_survived"]
        if survived:
            assert integ["missing_rows"] == 0
            assert integ["extra_rows"] == 0
            assert integ["excess_duplicates"] == 0
            assert integ["physical_committed_rows"] == 289744
        else:
            # Datanode stop crashed runs
            assert run["experiment"] == "datanode-stop"
            assert integ["missing_rows"] > 0
            assert integ["physical_committed_rows"] in (54995, 54990)


def test_uniform_control_degradation_reference():
    with open(MASTER_JSON, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    ref = data["metadata"]["statistical_convention"]["control_during_fault_reference_rec_per_sec"]
    assert ref == 1498.1
    
    for exp_name, exp in data["experiments"].items():
        if exp_name == "control":
            continue
        during_tp = exp["throughput_during_fault_window"]["mean"]
        expected_deg = round(((ref - during_tp) / ref) * 100.0, 1)
        actual_deg = round(exp["degradation_ratio_of_means_vs_control"], 1)
        assert abs(actual_deg - expected_deg) <= 0.1


def test_sample_std_ddof_1_used_everywhere():
    """Verify that every summary statistic block in master uses ddof=1 (Bessel's correction)."""
    with open(MASTER_JSON, "r", encoding="utf-8") as f:
        data = json.load(f)

    def check_stats(obj, path=""):
        if isinstance(obj, dict):
            if "n" in obj and "std" in obj:
                if obj["n"] > 1:
                    assert obj.get("ddof") == 1, f"Missing or non-1 ddof at {path}: {obj}"
            for k, v in obj.items():
                check_stats(v, f"{path}.{k}" if path else k)
        elif isinstance(obj, list):
            for i, v in enumerate(obj):
                check_stats(v, f"{path}[{i}]")

    check_stats(data)


def test_single_rounding_formatter():
    """Verify consistent formatting of statistics across the results structure."""
    with open(MASTER_JSON, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Standard control during reference must be 1498.1
    assert data["metadata"]["statistical_convention"]["control_during_fault_reference_rec_per_sec"] == 1498.1
    # Check control during throughput
    ctrl_during = data["experiments"]["control"]["throughput_during_fault_window"]["mean"]
    assert round(ctrl_during, 1) == 1498.1


def test_ratio_of_means_degradation_formula():
    """Verify that degradation columns use the ratio of means: 1 - mean(during) / mean(baseline)."""
    with open(MASTER_JSON, "r", encoding="utf-8") as f:
        data = json.load(f)

    ctrl_ref = data["metadata"]["statistical_convention"]["control_during_fault_reference_rec_per_sec"]

    for exp_name, exp in data["experiments"].items():
        during = exp["throughput_during_fault_window"]["mean"]
        pw_baseline = exp["post_warmup_baseline"]["mean"]

        expected_deg_pw = round((1.0 - (during / pw_baseline)) * 100.0, 1)
        expected_deg_ctrl = round((1.0 - (during / ctrl_ref)) * 100.0, 1)

        assert abs(exp["degradation_ratio_of_means_vs_post_warmup"] - expected_deg_pw) <= 0.1, (
            f"{exp_name}: expected {expected_deg_pw}, got {exp['degradation_ratio_of_means_vs_post_warmup']}"
        )
        assert abs(exp["degradation_ratio_of_means_vs_control"] - expected_deg_ctrl) <= 0.1, (
            f"{exp_name}: expected {expected_deg_ctrl}, got {exp['degradation_ratio_of_means_vs_control']}"
        )


def test_window_vs_true_halted_throughput():
    """Verify stop faults show non-zero window throughput (pre-halt drain) and 0.00 true halted throughput."""
    with open(MASTER_JSON, "r", encoding="utf-8") as f:
        data = json.load(f)

    stop_faults = ["kafka-stop", "spark-worker-stop", "datanode-stop"]
    for sf in stop_faults:
        exp = data["experiments"][sf]
        # Window throughput must be > 0 due to graceful shutdown / drain
        assert exp["throughput_during_fault_window"]["mean"] > 0
        # True halted throughput is 0.00 rec/s across all stop faults
        for run in data["runs"]:
            if run["experiment"] == sf:
                assert run["during_fault"]["throughput_true_halted_rec_per_sec"] == 0.0


def test_executed_survived_data_complete_columns():
    """Verify Executed / Survived / Data-Complete counts across all 6 scenarios."""
    with open(MASTER_JSON, "r", encoding="utf-8") as f:
        data = json.load(f)

    expected_counts = {
        "control": (5, 5, 5),
        "kafka-stop": (5, 5, 5),
        "kafka-restart": (5, 5, 5),
        "spark-worker-stop": (5, 5, 5),
        "spark-worker-restart": (5, 5, 5),
        "datanode-stop": (5, 3, 3),
    }

    for exp_name, (exp_cnt, surv_cnt, comp_cnt) in expected_counts.items():
        runs = [r for r in data["runs"] if r["experiment"] == exp_name]
        assert len(runs) == exp_cnt
        actual_surv = sum(1 for r in runs if r["statuses"]["query_survived"])
        actual_comp = sum(1 for r in runs if r["data_integrity"]["physical_committed_rows"] == 289744)
        assert actual_surv == surv_cnt, f"{exp_name} survived: expected {surv_cnt}, got {actual_surv}"
        assert actual_comp == comp_cnt, f"{exp_name} complete: expected {comp_cnt}, got {actual_comp}"


def test_crashed_run_conventions():
    """Verify dual reporting for datanode-stop (3 surviving runs vs 5 runs with crashed=0 convention)."""
    with open(MASTER_JSON, "r", encoding="utf-8") as f:
        data = json.load(f)

    ds_runs = [r for r in data["runs"] if r["experiment"] == "datanode-stop"]
    # Convention A: 5-run mean with crashed=0.0 rec/s
    tp_5runs = [r["during_fault"]["throughput_window_rec_per_sec"] if r["statuses"]["query_survived"] else 0.0 for r in ds_runs]
    mean_5runs = round(sum(tp_5runs) / 5.0, 2)
    assert mean_5runs == 295.34

    # Convention B: 3 surviving runs
    surv_runs = [r for r in ds_runs if r["statuses"]["query_survived"]]
    mean_surv = round(sum(r["during_fault"]["throughput_window_rec_per_sec"] for r in surv_runs) / 3.0, 2)
    assert mean_surv == 492.23


def test_lint_report_passes():
    cmd = [sys.executable, str(REPO_ROOT / "scripts" / "evaluation" / "lint_report.py")]
    res = subprocess.run(cmd, capture_output=True, text=True, cwd=str(REPO_ROOT))
    assert res.returncode == 0, f"lint_report.py failed:\n{res.stdout}\n{res.stderr}"
    assert "PASSED: Zero consistency errors." in res.stdout
