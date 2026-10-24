# Reproducing Evaluation Analysis & Report Artifacts

This document provides exact, deterministic instructions to reproduce the evaluation data processing, statistical aggregations, markdown report tables, consistency linting, and test suites from raw artifacts.

---

## 1. Environment & Prerequisites

> [!IMPORTANT]
> **Configuration Drift Notice**: The 30 fault-injection campaign runs recorded in docs/evaluation/raw/ were conducted under the campaign configuration (commit 9b52db4):
> - SPARK_WORKER_CORES: 2, SPARK_WORKER_MEMORY: 2G
> - spark.cores.max: 1, Driver Memory: 1G, Executor Memory: 1G
> - Backend: uvicorn app.main:app --reload
> Subsequent HEAD tuning for 1M batch processing increased SPARK_WORKER_CORES to 4, spark.cores.max to 4, and spark.executor.memory to 2G. These batch settings were NOT re-evaluated under fault injection.


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
  - Uniform control reference ($1,498.1\text{ rec/s}$, the 5-run mean simulated fault window throughput) used for all degradation comparisons.
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
  - Rule (a): Recomputation of all mean/std metrics with sample standard deviation (`ddof=1`).
  - Rule (b): Ratio of means degradation formula ($1 - \text{mean}(\text{during}) / \text{mean}(\text{baseline})$).
  - Rule (c): Cross-document consistency between `FINAL_REPORT.md` and `gap3_table.md`.
  - Rule (d): Existence of all referenced file/script paths in `[VERIFIED: <path>]` tags.
  - Rule (e): Mandatory `[HYPOTHESIS]` or `[VERIFIED]` tags on all causal/mechanism assertions.
  - Rule (f): Accurate canonical validator formulas distinguishing normal runs, r4 counter discrepancy, and crash snapshots.
  - Rule (g): Four-layer status consistency (driver, platform, validator, physical loss).
  - Rule (h): Absolute ban on stale failing paths (`_spark_metadata/19.compact`) or unqualified claims.

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
- Runs 46 evaluation tests covering:
  - Evaluation harness (`test_evaluation_harness.py`: 22 tests)
  - Statistical aggregation engine (`test_aggregate.py`: 7 tests)
  - Master JSON and documentation consistency (`test_report_consistency.py`: 10 tests)
  - Forensic linter verification and negative tests (`test_linter.py`: 7 tests)

### Backend Unit Tests
```bash
docker exec platform-backend pytest tests/ -m "not integration" -q
```
- Executes 610 unit tests (594 passed, 16 skipped for unstarted optional services, 44 integration deselected).

### Backend Integration Tests
```bash
docker exec platform-backend pytest tests/integration/ -q
```
- Executes 16 live integration tests in `tests/integration/` against Docker Compose platform stack (all 16 passed).
- Note: Across the entire backend repository, 44 tests are marked with `@pytest.mark.integration` (16 in `tests/integration/`, 5 in `tests/messaging/`, 5 in `tests/reliability/`, 4 in `tests/spark/`, 14 in `tests/storage/`).

### Frontend Unit & Component Tests
```bash
cd frontend && npm test -- --run
```
- Runs Vitest component and accessibility test suites (25 test files, 249 tests passed).

### Frontend E2E Tests (Playwright)
> **Note on WSL2 Memory Ceiling**: Under standard WSL2 memory limits (5.8 GiB allocation ceiling on 16 GiB hosts), running all 60 tests sequentially in a single pass leads to container memory exhaustion during 1M-row streaming (7.7 min sequential duration vs 1.4 min in isolation). For host systems with >= 16 GB RAM, setting memory=8GB in %UserProfile%\\.wslconfig prevents host paging. To guarantee clean execution within default limits, execute in 3 chunks with stack restarts between chunks:

`ash
cd frontend
# Chunk 1: General workspace, pipeline, and reliability specs (52 tests)
npx playwright test e2e/bundle.spec.ts e2e/output.spec.ts e2e/pipelines.spec.ts e2e/polish.spec.ts e2e/reliability.spec.ts e2e/run-details.spec.ts e2e/screenshots.spec.ts e2e/smoke.spec.ts --reporter=line

# Chunk 2: Streaming specs (3 tests)
docker compose restart backend frontend spark-worker kafka
npx playwright test e2e/streaming.spec.ts --reporter=line

# Chunk 3: 3D Visualization and Demo specs (5 tests)
docker compose restart backend frontend spark-worker kafka
npx playwright test e2e/viz.spec.ts e2e/demo.spec.ts --reporter=line
`
- Total: 60 Playwright E2E tests across 11 spec files against live stack (all 60 passed).
