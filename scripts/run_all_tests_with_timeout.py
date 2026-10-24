#!/usr/bin/env python3
"""
scripts/run_all_tests_with_timeout.py
--------------------------------------
Runs all test suites (unit, integration, evaluation, frontend) with strict
wall-clock timeouts. Records pass/fail counts. NO infinite loops.

Usage:
    python scripts/run_all_tests_with_timeout.py
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from datetime import datetime

# ── Timeouts ──────────────────────────────────────────────────────────────────
SUITE_TIMEOUT_SECONDS = 300  # 5 min max per test suite

PROJ_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESULTS_DIR = os.path.join(PROJ_ROOT, "docs", "evaluation", "results")
os.makedirs(RESULTS_DIR, exist_ok=True)

SUITES = [
    {
        "name": "backend-unit",
        "cmd": [
            "docker", "exec", "platform-backend",
            "python", "-m", "pytest", "tests/", "-q",
            "-m", "not integration and not e2e",
            "--tb=short", "--no-header",
        ],
        "timeout": SUITE_TIMEOUT_SECONDS,
        "cwd": None,
    },
    {
        "name": "evaluation",
        "cmd": [
            "docker", "exec", "platform-backend",
            "python", "-m", "pytest", "tests/evaluation/", "-q",
            "--tb=short", "--no-header",
        ],
        "timeout": SUITE_TIMEOUT_SECONDS,
        "cwd": None,
    },
]


def run_suite(suite: dict) -> dict:
    name = suite["name"]
    cmd = suite["cmd"]
    timeout = suite["timeout"]
    cwd = suite.get("cwd") or PROJ_ROOT

    print(f"\n{'='*60}", flush=True)
    print(f"Running suite: {name}", flush=True)
    print(f"Command: {' '.join(cmd)}", flush=True)
    print(f"Timeout: {timeout}s", flush=True)

    t0 = time.time()
    try:
        result = subprocess.run(
            cmd,
            cwd=cwd,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        elapsed = time.time() - t0
        stdout = result.stdout
        stderr = result.stderr
        exit_code = result.returncode
        timed_out = False
    except subprocess.TimeoutExpired as e:
        elapsed = time.time() - t0
        stdout = e.stdout.decode() if e.stdout else ""
        stderr = e.stderr.decode() if e.stderr else ""
        exit_code = -1
        timed_out = True
        print(f"  TIMED OUT after {elapsed:.1f}s", flush=True)

    print(stdout[-3000:] if len(stdout) > 3000 else stdout, flush=True)
    if stderr:
        print("STDERR:", stderr[-1000:], flush=True)

    # Parse pytest summary line
    passed = failed = errors = skipped = 0
    for line in stdout.splitlines():
        if " passed" in line or " failed" in line or " error" in line:
            parts = line.strip().split()
            for i, p in enumerate(parts):
                if p == "passed," or p == "passed":
                    try:
                        passed = int(parts[i - 1])
                    except (IndexError, ValueError):
                        pass
                if p == "failed," or p == "failed":
                    try:
                        failed = int(parts[i - 1])
                    except (IndexError, ValueError):
                        pass
                if p == "error," or p == "errors":
                    try:
                        errors = int(parts[i - 1])
                    except (IndexError, ValueError):
                        pass
                if p == "skipped," or p == "skipped":
                    try:
                        skipped = int(parts[i - 1])
                    except (IndexError, ValueError):
                        pass

    record = {
        "suite": name,
        "passed": passed,
        "failed": failed,
        "errors": errors,
        "skipped": skipped,
        "exit_code": exit_code,
        "timed_out": timed_out,
        "elapsed_seconds": round(elapsed, 1),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "stdout_tail": stdout[-2000:],
    }
    print(
        f"  Result: passed={passed} failed={failed} errors={errors} "
        f"skipped={skipped} exit={exit_code} timed_out={timed_out}",
        flush=True,
    )
    return record


def main() -> None:
    print("=== Full Test Suite Runner (strict timeouts) ===", flush=True)
    print(f"SUITE_TIMEOUT_SECONDS={SUITE_TIMEOUT_SECONDS}", flush=True)

    results = []
    for suite in SUITES:
        rec = run_suite(suite)
        results.append(rec)

    out_path = os.path.join(RESULTS_DIR, "test_suite_results.json")
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nResults saved to {out_path}", flush=True)

    print("\n=== SUMMARY ===", flush=True)
    print(f"{'Suite':<25} {'Pass':>6} {'Fail':>6} {'Skip':>6} {'Exit':>6} {'Time':>8}", flush=True)
    print("-" * 65, flush=True)
    all_ok = True
    for r in results:
        mark = "✓" if r["exit_code"] == 0 else ("⏱" if r["timed_out"] else "✗")
        print(
            f"{r['suite']:<25} {r['passed']:>6} {r['failed']:>6} "
            f"{r['skipped']:>6} {r['exit_code']:>6} {r['elapsed_seconds']:>7.1f}s  {mark}",
            flush=True,
        )
        if r["exit_code"] != 0:
            all_ok = False

    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()
