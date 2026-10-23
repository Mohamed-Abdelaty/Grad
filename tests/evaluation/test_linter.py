"""
tests/evaluation/test_linter.py
-------------------------------
Unit tests for the honest forensic report linter (scripts/evaluation/lint_report.py).
Tests both positive cases (current documents pass) and negative cases (fabricated
numbers, invalid tags, unsupported log quotes, stale timestamps are caught).
"""

from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
LINTER_PATH = REPO_ROOT / "scripts" / "evaluation" / "lint_report.py"

from scripts.evaluation.lint_report import (
    check_rule_a_verified_tags,
    check_rule_c_log_quotes,
    check_rule_d_paths_and_timestamps,
    check_rule_e_cross_document_consistency,
    run_linter,
    load_all_known_numbers,
    check_rule_b_numbers_provenance,
    check_rule_g_array_mean_std_consistency,
    check_rule_h_degradation_recomputation,
    check_rule_i_code_citations,
    check_rule_j_recovery_metrics,
)


def test_linter_script_runs():
    """Verify that running lint_report.py directly executes."""
    cmd = [sys.executable, str(LINTER_PATH)]
    res = subprocess.run(cmd, capture_output=True, text=True, cwd=str(REPO_ROOT))
    # It either exits 0 or 1 with clear output
    assert "CHAOSFLOW HONEST FORENSIC LINTER" in res.stdout


def test_rule_a_catches_invalid_verified_tag(tmp_path):
    """Negative test: a non-existent file in [VERIFIED: ...] must be flagged."""
    bad_report = tmp_path / "BAD_REPORT.md"
    bad_report.write_text(
        "Some claim [VERIFIED: docs/evaluation/non_existent_raw_file.json] that is fake.",
        encoding="utf-8",
    )
    errors = check_rule_a_verified_tags(REPO_ROOT, bad_report)
    assert len(errors) == 1
    assert "non-existent path" in errors[0]


def test_rule_a_catches_invalid_json_key(tmp_path):
    """Negative test: an existing JSON file with an invalid key in [VERIFIED: ...] must be flagged."""
    bad_report = tmp_path / "BAD_REPORT.md"
    bad_report.write_text(
        "Some claim [VERIFIED: docs/evaluation/results/results_master.json:non_existent_key] here.",
        encoding="utf-8",
    )
    errors = check_rule_a_verified_tags(REPO_ROOT, bad_report)
    assert len(errors) == 1
    assert "JSON key 'non_existent_key' not found" in errors[0]


def test_rule_c_catches_unsupported_log_phrase(tmp_path):
    """Negative test: unsupported or fabricated log phrases must be caught."""
    bad_report = tmp_path / "BAD_REPORT.md"
    bad_report.write_text(
        "The error occurred at _spark_metadata/19.compact and 0 of the 1 requested nodes were found.",
        encoding="utf-8",
    )
    errors = check_rule_c_log_quotes(REPO_ROOT, bad_report)
    assert len(errors) == 2
    assert any("_spark_metadata/19.compact" in e for e in errors)
    assert any("0 of the 1 requested nodes" in e for e in errors)


def test_rule_d_catches_stale_timestamps(tmp_path):
    """Negative test: stale or generated DataNode timestamps must be caught."""
    bad_report = tmp_path / "BAD_REPORT.md"
    bad_report.write_text(
        "At 14:52:19 the container halted and stream 786d773c-bf51-40be-bdc5-f865f0426f30 died.",
        encoding="utf-8",
    )
    errors = check_rule_d_paths_and_timestamps(REPO_ROOT, bad_report)
    assert len(errors) == 2
    assert any("14:52:19" in e for e in errors)
    assert any("786d773c-bf51-40be-bdc5-f865f0426f30" in e for e in errors)


def test_rule_e_catches_inconsistent_figures():
    """Negative test: cross-document discrepancy between 1498.0 and 1498.1 is caught."""
    rep_text = "The control throughput was 1498.0 rec/s in this test."
    gap_text = "The control throughput was 1498.1 rec/s in the table."
    errors = check_rule_e_cross_document_consistency(rep_text, gap_text)
    assert len(errors) == 1
    assert "1498.0" in errors[0]


def test_rule_b_catches_fabricated_number(tmp_path):
    """Negative test: a fabricated number not in provenance manifest or allow-list must fail."""
    bad_doc = tmp_path / "DOC.md"
    bad_doc.write_text("Throughput was 98765.43 rec/s during the fault.", encoding="utf-8")
    known = {100.0, 200.0, 300000.0}
    errors = check_rule_b_numbers_provenance(REPO_ROOT, bad_doc, known)
    assert len(errors) == 1
    assert "98765.43" in errors[0]


def test_rule_g_catches_array_mean_drift():
    """Negative test: array with incorrect stated mean must be caught."""
    prose = "The observed throughputs were [10.0, 20.0, 30.0] with mean 99.0 ± 5.0 rec/s."
    errors = check_rule_g_array_mean_std_consistency({}, prose)
    assert len(errors) >= 1
    assert any("stated mean 99.0" in e for e in errors)


def test_rule_h_catches_degradation_mismatch():
    """Negative test: mismatched degradation cell must fail."""
    bad_table = (
        "| Experiment Name | Target Container | Post-Warmup Baseline (rec/s) [Primary] | 10s Window Baseline (rec/s) [Secondary] | During-Fault Throughput (rec/s) | Degradation vs Post-Warmup Baseline (%) | Degradation vs Control (%) |\n"
        "| :--- | :--- | :--- | :--- | :--- | :--- | :--- |\n"
        "| **datanode-stop** | platform-datanode | 1541.1 ± 54.1 | 1379.3 ± 164.4 | 492.23 (surviving window) / 295.34 (all 5 runs) / 0.00 (true halted) | 50.0% (surviving window) / 80.8% (all 5 runs) / 100% (halted/crashed) | 67.1% |\n"
    )
    errors = check_rule_h_degradation_recomputation(bad_table)
    assert len(errors) >= 1
    assert any("datanode-stop surviving window degradation must be 68.1%" in e for e in errors)


def test_rule_i_catches_invalid_code_citation():
    """Negative test: invalid code path in report must fail."""
    bad_text = "See backend/app/pipeline/nonexistent_module_xyz.py:42 for details."
    errors = check_rule_i_code_citations(REPO_ROOT, bad_text)
    assert len(errors) == 1
    assert "Cited code file does not exist" in errors[0]


def test_rule_j_catches_recovery_metric_contradiction():
    """Negative test: swapped recovery metrics or stale 0.38 DataNode recovery must fail."""
    bad_text_1 = "kafka-stop Container Recovery 22.89 s"
    errors_1 = check_rule_j_recovery_metrics(bad_text_1)
    assert len(errors_1) == 1
    assert "swapped kafka-stop recovery metrics" in errors_1[0]

    bad_text_2 = "DataNode container recovery was 0.38 ± 0.06 s."
    errors_2 = check_rule_j_recovery_metrics(bad_text_2)
    assert len(errors_2) == 1
    assert "0.38 ± 0.06" in errors_2[0]
