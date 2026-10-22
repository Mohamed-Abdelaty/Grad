# Reproducing Evaluation Analysis & Report Artifacts

This document provides exact, deterministic instructions to reproduce the evaluation data processing, statistical aggregations, markdown report tables, consistency linting, and test suites from raw artifacts.

---

## 1. Environment & Prerequisites

- **Python**: Python 3.11 (`py -3.11` on Windows or `python3.11` on Linux/macOS)
- **Node.js**: Node 18+ and npm
- **Docker**: Docker engine running with platform containers:
  - `platform-backend`
  - `platform-kafka`
  - `platform-spark-master`
  - `platform-spark-worker`
  - `platform-namenode`
  - `platform-datanode`
  - `platform-postgres`
- **Dependencies**:
  ```bash
  py -3.11 -m pip install pytest pyyaml pyarrow requests
  ```

---

## 2. Deterministic Pipeline: From Raw Artifacts to Report

All evaluation numbers originate from raw per-run artifacts located in `docs/evaluation/raw/`. No metric is manually entered.

### Step 1: Build the Single Source of Truth (`results_master.json`)

Runs the aggregation pipeline across all 30 campaign runs (5 control + 25 fault):
```bash
py -3.11 scripts/evaluation/build_master.py
```
- **Input**: `docs/evaluation/raw/**/report.json`, `environment.json`, `physical_metrics.json`
- **Output**: `docs/evaluation/results/results_master.json`
- **Statistical Rules Applied**:
  - Sample standard deviation ($N-1$ denominator, `ddof=1`) across all metric series.
  - Uniform control reference ($1,498.0\text{ rec/s}$, the 5-run mean simulated fault window throughput) used for all degradation comparisons.
  - Dual baseline tracking: unquantized post-warmup baseline (primary) and 10s-window baseline (secondary).
  - Stop faults during-fault throughput reported both over the 20s window (including drain) and during true halted state ($0.00\text{ rec/s}$).

### Step 2: Render Markdown Report Tables

Injects generated Markdown tables into `gap3_table.md` and `FINAL_REPORT.md` within designated comment markers:
```bash
py -3.11 scripts/evaluation/render_report_tables.py
```
- **Generated Tables**:
  1. `gap3_primary_table`: Primary Gap 3 comparison against empirical control.
  2. `audit_30_runs_table`: Complete 30-run audit of HDFS committed rows, validator outputs, and statuses.
  3. `stop_faults_records_reconciliation_table`: Per-run records accounting at snapshot, halt, and revert.
  4. `baseline_quantization_table`: 10s window vs post-warmup baseline quantization comparison.
  5. `datanode_timeline_table`: DataNode container halt vs block reporting stall breakdown.

### Step 3: Run Documentation Consistency Linter

Verifies that every number, percentage, count, and claim in `FINAL_REPORT.md` and `gap3_table.md` matches `results_master.json` with zero discrepancy:
```bash
py -3.11 scripts/evaluation/lint_report.py
```
- **Enforces**:
  - All numbers traceable to `results_master.json` or allow-listed constants.
  - Absolute ban on unqualified "zero data loss" phrases.
  - Mandatory `[VERIFIED]` or `[HYPOTHESIS]` tags on all causal/mechanism assertions.

### Step 4: Verify Checksums

Verifies SHA-256 hashes of all 134 raw artifacts and master JSON:
```bash
py -3.11 -c "
import hashlib
from pathlib import Path

checksums_file = Path('docs/evaluation/results/CHECKSUMS.txt')
for line in checksums_file.read_text(encoding='utf-8').strip().split('\n'):
    expected_hash, rel_path = line.split('  ')
    actual_hash = hashlib.sha256(Path(rel_path).read_bytes()).hexdigest()
    assert expected_hash == actual_hash, f'Checksum mismatch for {rel_path}'
print('All checksums verified successfully.')
"
```

---

## 3. Running Test Suites

### Evaluation Test Suite
```bash
py -3.11 -m pytest tests/evaluation -v
```
- Runs 33 evaluation tests covering:
  - Evaluation harness (`test_evaluation_harness.py`: 22 tests)
  - Statistical aggregation engine (`test_aggregate.py`: 7 tests)
  - Master JSON and documentation consistency (`test_report_consistency.py`: 4 tests)

### Backend Unit Tests
```bash
docker exec platform-backend pytest tests/ -m "not integration" -q
```
- Executes 610 unit tests (594 passed, 16 skipped for unstarted Airflow scheduler).

### Backend Integration Tests
```bash
docker exec platform-backend pytest tests/ -m "integration" -q
```
- Executes 44 live integration tests across Kafka, Spark, and HDFS.

### Frontend Unit & Component Tests
```bash
cd frontend && npm test
```
- Runs Vitest component and accessibility test suites.
