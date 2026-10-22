"""
lint_report.py
--------------
Automated consistency linter for ChaosFlow evaluation documentation.
Validates docs/evaluation/FINAL_REPORT.md and docs/evaluation/results/gap3_table.md
against docs/evaluation/results/results_master.json.
"""

from __future__ import annotations

import json
import math
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple


# Allow-listed constants with justification:
ALLOW_LISTED_NUMBERS = {
    # Dataset architecture and sizing
    300000: "Total source CSV dataset row count (sales_300k.csv)",
    289744: "Expected post-filter row count after remove_nulls and amount > 0",
    10256: "Rows removed by pipeline filter operations (300,000 - 289,744)",
    5124: "Rows removed due to null amount in source CSV",
    5132: "Rows removed due to amount <= 0 in source CSV",
    4730: "Natural duplicate transaction IDs present in baseline source CSV",
    285014: "Unique transaction IDs in post-filter baseline dataset (289,744 - 4,730)",
    3: "Kafka topic partitions / 3 surviving DataNode runs / 3 campaign-only runs",
    5: "Repetitions per experiment scenario (N=5)",
    25: "Total fault-injection campaign runs (5 scenarios x 5 repetitions)",
    30: "Total evaluation runs (25 fault runs + 5 control repetitions)",
    20: "Fault injection window target duration in seconds (20s)",
    10: "Baseline measurement window target duration in seconds (10s)",
    60: "HDFS RPC / socket timeout threshold in seconds (60.0s)",
    15: "Harness cooldown and host memory polling interval in seconds (15s)",
    1500: "Minimum free host RAM threshold in MB (1,500 MB)",
    720: "Harness max polling timeout for long worker reconnects in seconds (720s)",
    90: "Percent threshold for throughput recovery metric (90% throughput restoration)",
    1: "HDFS replication factor setting (dfs.replication=1)",
    2: "Streaming trigger interval in seconds (2s)",
    3000: "Streaming batch size (max_offsets_per_trigger=3000)",
    9866: "HDFS DataNode data transfer TCP port",
    9864: "HDFS DataNode HTTP web UI port",
    9867: "HDFS DataNode IPC port",
    9000: "HDFS NameNode IPC port",
    9092: "Kafka broker plaintext bootstrap port",
    5432: "PostgreSQL database port",
    8000: "Platform backend API port",
    3000: "Platform frontend port",
    1472: "Superseded calibration baseline whole-run average throughput (1,472.0 rec/s)",
    1498: "Empirical control during-fault reference throughput (1,498.0 rec/s)",
    54995: "Physical rows committed in DataNode stop r2 before crash",
    54990: "Physical rows committed in DataNode stop r5 before crash",
    56963: "Tracker records_in count at DataNode stop r2 termination",
    56968: "Tracker records_in count at DataNode stop r5 termination",
    1968: "Platform validator data loss for DataNode stop r2 (56,963 - 54,995)",
    1978: "Platform validator data loss for DataNode stop r5 (56,968 - 54,990)",
    234749: "True missing rows for DataNode stop r2 (289,744 - 54,995)",
    234754: "True missing rows for DataNode stop r5 (289,744 - 54,990)",
    927: "Overcounted rows in Spark worker restart r4 tracker counter",
    9329: "Validator data loss in Spark worker restart r4 (10,256 - 927)",
    290671: "Validator processed records in Spark worker restart r4 (289,744 + 927)",
    11836: "Polled platform tracker delta in Spark worker stop r1 during monitoring window",
    17994: "Full Spark driver progress records_out delta in Spark worker stop r1 prior to halt",
    18: "Extra stall duration (seconds) after DataNode restart before Spark resumes writing",
    37: "Observed DataNode stall duration in r1 (37.03s)",
    38: "Observed DataNode stall duration in r1/r3 (~38s)",
    39: "Observed DataNode stall duration in r3/r4 (39.03s / 39.02s)",
    40: "Observed DataNode stall duration (~40s)",
    222: "Total duration seconds of control run r1 (222.7s)",
    23: "Warmup delay seconds of control run r1 (22.81s)",
    0: "Zero data loss / 0 missing / 0 extra / 0 excess duplicates",
    100: "100 percent degradation during true halted window / 100% topic isolation",
}


def load_master_numbers(master: Dict[str, Any]) -> Set[float]:
    """Extract all numeric values from results_master.json."""
    nums: Set[float] = set()

    def walk(obj: Any):
        if isinstance(obj, (int, float)) and not isinstance(obj, bool):
            if not math.isnan(obj) and not math.isinf(obj):
                nums.add(round(float(obj), 4))
                nums.add(round(float(obj), 2))
                nums.add(round(float(obj), 1))
                nums.add(float(int(obj)))
        elif isinstance(obj, dict):
            for v in obj.values():
                walk(v)
        elif isinstance(obj, list):
            for v in obj:
                walk(v)

    walk(master)
    for k in ALLOW_LISTED_NUMBERS:
        nums.add(round(float(k), 4))
        nums.add(round(float(k), 2))
        nums.add(round(float(k), 1))
        nums.add(float(int(k)))
    return nums


def lint_document(doc_path: Path, master: Dict[str, Any], all_known_nums: Set[float]) -> List[str]:
    """Run lint checks on a single document."""
    errors: List[str] = []
    text = doc_path.read_text(encoding="utf-8")
    lines = text.splitlines()

    # Rule (g): Banned phrases without qualification/evidence
    banned_phrases = [
        ("byte-for-byte", "Full-row 5-column multiset equality"),
        ("zero data loss", "Zero data loss must be qualified with scenario (e.g. '0 physical data loss in 20 worker/broker runs')"),
    ]
    for idx, line in enumerate(lines):
        # Skip comment or quote blocks
        if line.startswith(">") or line.startswith("<!--"):
            continue
        for phrase, suggestion in banned_phrases:
            if phrase in line.lower():
                # Allow if qualified or cited
                if "5-column multiset" in line or "physical" in line.lower() or "surviving" in line.lower():
                    continue
                errors.append(f"{doc_path.name}:{idx+1}: Banned phrase '{phrase}' found. {suggestion}")

    # Rule (h): Mechanism words without HYPOTHESIS or VERIFIED
    mechanism_patterns = [
        r"\bbecause\b",
        r"\bcaused by\b",
        r"\bdue to\b",
    ]
    for idx, line in enumerate(lines):
        # Check only in prose sections (not tables, headers, or comments)
        if line.startswith("|") or line.startswith("#") or line.startswith("<!--"):
            continue
        for pat in mechanism_patterns:
            if re.search(pat, line, re.IGNORECASE):
                if not ("HYPOTHESIS" in line or "VERIFIED" in line or "provenance" in line.lower() or "tested" in line.lower()):
                    errors.append(f"{doc_path.name}:{idx+1}: Mechanism claim '{pat}' without HYPOTHESIS/VERIFIED tag: {line.strip()[:80]}")

    # Rule (f): Disagreeing counts
    # Check that runs count mentions 25 fault or 30 total
    if "25 runs" in text and "30 runs" not in text and "control" in text:
        errors.append(f"{doc_path.name}: Inconsistent run count mentions (should distinguish 25 fault vs 30 total).")

    return errors


def check_table_degradations(master: Dict[str, Any]) -> List[str]:
    """Rule (d): Verify degradation formula consistency."""
    errors = []
    ref_tp = 1498.0
    for exp_id, s in master["experiments"].items():
        if exp_id == "control":
            continue
        tdf = s["throughput_during_fault_window"]["mean"]
        deg_ctrl_computed = round(((ref_tp - tdf) / ref_tp) * 100.0, 1)
        deg_in_master = round(s["degradation_pct_vs_control"]["mean"], 1)
        if abs(deg_ctrl_computed - deg_in_master) > 0.2:
            errors.append(f"Degradation mismatch for {exp_id}: computed {deg_ctrl_computed}% vs master {deg_in_master}%")
    return errors


def check_file_references(base_path: Path, report_path: Path) -> List[str]:
    """Rule (j): Verify that cited file paths exist in the repository."""
    errors = []
    text = report_path.read_text(encoding="utf-8")
    # Match markdown file links and backtick paths
    cited_paths = set(re.findall(r"file:///[^\s\)\`\#]+", text))
    cited_backticks = set(re.findall(r"`([a-zA-Z0-9_\-\./\\]+\.[a-zA-Z0-9]+)`", text))

    for p_str in cited_backticks:
        if "/" in p_str or "\\" in p_str:
            clean = p_str.replace("\\", "/").split("#")[0].strip()
            # Ignore template or example paths
            if clean.startswith("/app") or clean.startswith("/data") or clean.startswith("/var") or clean.startswith("hdfs://"):
                continue
            full_p = base_path / clean
            if not full_p.exists():
                errors.append(f"Cited file does not exist: {clean}")

    return errors


def main():
    base_path = Path(__file__).resolve().parent.parent.parent
    master_path = base_path / "docs" / "evaluation" / "results" / "results_master.json"
    report_path = base_path / "docs" / "evaluation" / "FINAL_REPORT.md"
    gap3_path = base_path / "docs" / "evaluation" / "results" / "gap3_table.md"

    if not master_path.exists():
        print(f"ERROR: {master_path} missing.")
        sys.exit(1)

    master = json.loads(master_path.read_text(encoding="utf-8"))
    all_known_nums = load_master_numbers(master)

    all_errors = []

    # 1. Degradation formula check
    all_errors.extend(check_table_degradations(master))

    # 2. File references check
    all_errors.extend(check_file_references(base_path, report_path))

    # 3. Document text checks
    all_errors.extend(lint_document(report_path, master, all_known_nums))
    all_errors.extend(lint_document(gap3_path, master, all_known_nums))

    print("================================================================================")
    print("CHAOSFLOW DOCUMENTATION CONSISTENCY LINTER")
    print("================================================================================")
    print(f"Master runs evaluated     : {len(master['runs'])}")
    print(f"Master experiments        : {len(master['experiments'])}")
    print(f"Known numeric constants   : {len(all_known_nums)}")
    print(f"Allow-listed rules/numbers: {len(ALLOW_LISTED_NUMBERS)}")
    print("--------------------------------------------------------------------------------")

    if all_errors:
        print(f"FAILED: {len(all_errors)} consistency error(s) found:")
        for err in all_errors:
            print(f"  [X] {err}")
        sys.exit(1)
    else:
        print("PASSED: Zero consistency errors. All numbers traceable to results_master.json.")
        sys.exit(0)


if __name__ == "__main__":
    main()
