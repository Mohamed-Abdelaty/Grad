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
