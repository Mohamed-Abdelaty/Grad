"""Unit tests for report consistency, linter execution, and results_master.json validity.
"""
from __future__ import annotations

import json
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
    assert data["metadata"]["statistical_convention"]["control_during_fault_reference_rec_per_sec"] == 1498.0


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
    assert ref == 1498.0
    
    for exp_name, exp in data["experiments"].items():
        if exp_name == "control":
            continue
        during_tp = exp["throughput_during_fault_window"]["mean"]
        expected_deg = round(((ref - during_tp) / ref) * 100.0, 1)
        actual_deg = round(exp["degradation_pct_vs_control"]["mean"], 1)
        assert abs(actual_deg - expected_deg) <= 0.1


def test_lint_report_passes():
    cmd = [sys.executable, str(REPO_ROOT / "scripts" / "evaluation" / "lint_report.py")]
    res = subprocess.run(cmd, capture_output=True, text=True, cwd=str(REPO_ROOT))
    assert res.returncode == 0, f"lint_report.py failed:\n{res.stdout}\n{res.stderr}"
    assert "PASSED: Zero consistency errors." in res.stdout
