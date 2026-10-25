"""
scripts/evaluation/print_run_timestamps.py
-----------------------------------------
Extracts and prints the exact started_at and faultInjectedAt timestamps
from raw report.json (and environment.json) for all 30 evaluation runs.
Guarantees byte-for-byte fidelity with raw disk artifacts.
"""

from __future__ import annotations

import json
from pathlib import Path


def main():
    repo_root = Path(__file__).resolve().parent.parent.parent
    raw_dir = repo_root / "docs" / "evaluation" / "raw"

    runs_def = [
        # Control runs (5)
        ("control", f"r{i}", f"eval-control-r{i}", raw_dir / "control" / f"eval-control-r{i}")
        for i in range(1, 6)
    ] + [
        # Kafka Stop (5 runs)
        ("kafka-stop", "r1", "eval-kafka-stop-r1", raw_dir / "pilot-v3" / "eval-kafka-stop-r1"),
        ("kafka-stop", "r2", "eval-kafka-stop-r2", raw_dir / "pilot-v3" / "eval-kafka-stop-r2"),
        ("kafka-stop", "r3", "eval-kafka-stop-r3", raw_dir / "campaign" / "kafka-stop" / "eval-kafka-stop-r3"),
        ("kafka-stop", "r4", "eval-kafka-stop-r4", raw_dir / "campaign" / "kafka-stop" / "eval-kafka-stop-r4"),
        ("kafka-stop", "r5", "eval-kafka-stop-r5", raw_dir / "campaign" / "kafka-stop" / "eval-kafka-stop-r5"),
    ] + [
        ("kafka-restart", f"r{i}", f"eval-kafka-restart-r{i}", raw_dir / "campaign" / "kafka-restart" / f"eval-kafka-restart-r{i}")
        for i in range(1, 6)
    ] + [
        ("spark-worker-stop", f"r{i}", f"eval-spark-worker-stop-r{i}", raw_dir / "campaign" / "spark-worker-stop" / f"eval-spark-worker-stop-r{i}")
        for i in range(1, 6)
    ] + [
        ("spark-worker-restart", f"r{i}", f"eval-spark-worker-restart-r{i}", raw_dir / "campaign" / "spark-worker-restart" / f"eval-spark-worker-restart-r{i}")
        for i in range(1, 6)
    ] + [
        ("datanode-stop", f"r{i}", f"eval-datanode-stop-r{i}", raw_dir / "campaign" / "datanode-stop" / f"eval-datanode-stop-r{i}")
        for i in range(1, 6)
    ]

    print("=" * 115)
    print("ALL 30 EVALUATION RUNS: RAW TIMESTAMPS FROM DISK")
    print("=" * 115)
    print(f"{'Run Name':<28} {'Experiment':<22} {'Started At (UTC)':<32} {'Fault Injected At (UTC)':<32}")
    print("-" * 115)

    fault_starts = []
    fault_injections = []
    control_starts = []

    for exp, rep, name, r_path in runs_def:
        rep_file = r_path / "report.json"
        env_file = r_path / "environment.json"

        s_at = "N/A"
        inj_at = "N/A"

        if rep_file.exists():
            d = json.loads(rep_file.read_text(encoding="utf-8"))
            s_at = d.get("started_at") or "N/A"
            inj_at = d.get("faultInjectedAt") or "None (unperturbed)"

        if exp == "control":
            if s_at != "N/A":
                control_starts.append(s_at)
        else:
            if s_at != "N/A":
                fault_starts.append(s_at)
            if inj_at not in ("N/A", "None (unperturbed)"):
                fault_injections.append(inj_at)

        print(f"{name:<28} {exp:<22} {s_at:<32} {inj_at:<32}")

    print("-" * 115)
    fault_starts.sort()
    fault_injections.sort()
    control_starts.sort()

    print(f"Summary of raw UTC timestamp ranges:")
    print(f"  Fault runs started_at range:    {fault_starts[0]} to {fault_starts[-1]}")
    print(f"  Fault runs faultInjected range: {fault_injections[0]} to {fault_injections[-1]}")
    print(f"  Control runs started_at range:  {control_starts[0]} to {control_starts[-1]}")
    print("=" * 115)


if __name__ == "__main__":
    main()
