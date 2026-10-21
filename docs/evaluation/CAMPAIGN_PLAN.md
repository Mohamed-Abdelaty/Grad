# Evaluation Campaign Plan
**Branch**: `feature/gap-closure`  
**Created**: 2026-10-21  
**Status**: IN PROGRESS

---

## 1. Objective

Produce research-grade, reproducible evidence for the thesis:
> A configuration-driven resilient big data pipeline can recover from infrastructure faults with zero data loss.

All results must be independently verified via physical HDFS row counts (multiset comparison), not platform tracker counters (which are confirmed unreliable).

---

## 2. Dataset & Expected Baseline

| Parameter | Value |
|---|---|
| Input file | `/app/sample-data/sales_300k.csv` (300,000 rows) |
| Valid rows after `remove_nulls` + `amount > 0` | **289,744** |
| Baseline duplicate transaction_ids in valid set | **4,730** |
| Expected missing rows (VALID run) | **0** |
| Expected excess duplicates (calibration / no-fault) | **0** |
| Expected excess duplicates (fault runs, at-least-once) | **≥ 0** (allowed) |

Platform validator numbers (recordsIn, recordsOut, rowsWritten) are confirmed unreliable and are recorded as metadata only.

---

## 3. Fixed Experimental Conditions

These values are **frozen** and must not be tuned after seeing results:

| Setting | Value |
|---|---|
| `repetitions_per_experiment` | 5 |
| `fault_duration_seconds` | 20 |
| `baseline_seconds` | 10 |
| `max_recovery_threshold_seconds` | 60.0 |
| `max_offsets_per_trigger` | 3,000 |
| `trigger_interval_seconds` | 2 s |
| `idle_timeout_seconds` | 20 s |
| `max_runtime_seconds` | 600 s |
| `min_host_free_ram_mb` | 1,500 MB |
| Pages/sec pre-flight threshold | ≤ 100 (30 s avg) |

**Stack**: 7 core containers only (`platform-backend`, `platform-postgres`, `platform-kafka`, `platform-spark-master`, `platform-spark-worker`, `platform-namenode`, `platform-datanode`). Frontend, Prometheus, Grafana, and Airflow containers stopped.

---

## 4. Experiment Inventory

### 4.1 Supported Experiments (from `matrix.yaml`)

| # | ID | Name | Target | Fault Type | Status |
|---|---|---|---|---|---|
| 1 | `kafka-stop` | Kafka Broker Container Stop | kafka | container_stop | 2 valid reps (pilot-v3) |
| 2 | `kafka-restart` | Kafka Broker Container Restart | kafka | container_restart | NOT STARTED |
| 3 | `spark-worker-stop` | Spark Worker Container Stop | spark-worker | container_stop | NOT STARTED |
| 4 | `spark-worker-restart` | Spark Worker Container Restart | spark-worker | container_restart | NOT STARTED |
| 5 | `datanode-stop` | HDFS DataNode Container Stop | datanode | container_stop | NOT STARTED |

### 4.2 Unsupported / Not Applicable

| ID | Reason |
|---|---|
| `kafka-delay` (if added) | `tc` traffic control not installed in `resilient-kafka` image — UNSUPPORTED |
| `datanode-restart` | Not in matrix.yaml |

### 4.3 Control Baseline (Gap 3)

No separate "control" experiment type exists in the platform (experiment runner raises error for unknown fault_type). The **calibration run** (`run_calibration()`) provides the no-fault throughput baseline. A single valid calibration run is already stored at `docs/evaluation/raw/calibration/calibration_report.json` (physical rows: **289,744**, throughput: ~1,472 rows/s, excess dups: 0).

Gap 3 comparison: **calibration throughput baseline** vs **during-fault throughput** from fault runs. Computed in the aggregate analysis script using:
- Control: `calibration_report.json` → micro_batch telemetry → steady-state rows/s
- Each fault run: `physical_metrics.json` + `report.json` → `throughput_baseline_records_per_sec` vs `throughput_during_fault_records_per_sec`

---

## 5. Phase Plan

### Phase 0 — Inventory & Plan (this document)
- [x] Read `matrix.yaml` — all 5 experiments enumerated
- [x] Read `experiment_runner.py` — no `fault_type: none` support confirmed
- [x] Confirm Gap 3 approach — calibration as control
- [x] Check disk space — HDFS 896 GB free, host ~19 GB free — **SUFFICIENT**
- [x] Write `CAMPAIGN_PLAN.md` — this file
- [ ] Commit Phase 0 plan

### Phase 1 — Control Calibration
- [x] Calibration run already PASSED: `pipe-eval-calibration-1792537189` → 289,744 rows, 0 missing, 0 extra, 0 excess dups
- Status: **COMPLETE** — no re-run needed

### Phase 2 — Fault Matrix (5 reps each)

#### `kafka-stop` (5 reps)
- r1: VALID (pilot-v3) — 289,744 rows, recovery 10.06 s  
- r2: VALID (pilot-v3) — 289,744 rows, recovery 12.74 s  
- r3: NOT STARTED  
- r4: NOT STARTED  
- r5: NOT STARTED  

#### `kafka-restart` (5 reps)
- r1–r5: NOT STARTED

#### `spark-worker-stop` (5 reps)
- r1–r5: NOT STARTED

#### `spark-worker-restart` (5 reps)
- r1–r5: NOT STARTED

#### `datanode-stop` (5 reps)
- r1–r5: NOT STARTED

### Phase 3 — Aggregate Analysis & Gap 3

- [ ] Write `scripts/evaluation/aggregate.py`
- [ ] Generate `docs/evaluation/results/gap3_table.csv` + `.md`
- [ ] Generate per-experiment summary statistics
- [ ] Limitations section

### Phase 4 — Final Report

- [ ] Write `docs/evaluation/FINAL_REPORT.md`
- [ ] Clean working tree
- [ ] STOP

---

## 6. Run Naming Convention

| Artifact | Path |
|---|---|
| Campaign runs | `docs/evaluation/raw/campaign/<exp-id>/<exp-name>/` |
| Pilot-v3 (already valid) | `docs/evaluation/raw/pilot-v3/eval-kafka-stop-r{1,2}/` |
| Calibration | `docs/evaluation/raw/calibration/calibration_report.json` |
| Progress tracker | `docs/evaluation/raw/campaign/progress.md` |
| Aggregate results | `docs/evaluation/results/` |

**Note**: Pilot-v3 r1 and r2 count as the first 2 valid reps of `kafka-stop`. Campaign runs for `kafka-stop` start at r3.

---

## 7. Validity Rules

A run is **VALID** if and only if:
1. `run_classification == "VALID"` in `physical_metrics.json`
2. `physical_rows == 289,744` (no data loss)
3. `missing_rows == 0` (multiset comparison)
4. `excess_duplicates >= 0` (at-least-once delivery allowed)
5. `host_memory_pressure` noted but does NOT invalidate the run if RAM > 1,500 MB at start

A run is **INVALID** if:
- `physical_rows == 0` (empty output)
- `_spark_metadata` guard triggered (stale path)
- Kafka topic not empty at start (topic reuse)
- `MeasurementError` raised (MEASUREMENT_FAILED)

---

## 8. Stop Rules

- If 3 consecutive runs of any experiment are INVALID: STOP and report
- If disk free < 50 GB HDFS or < 5 GB host: STOP and report
- If any container exits unexpectedly during a run: mark run INVALID, check containers, report
- If harness tests regress after any commit: revert and report
- Never tune matrix parameters after seeing results
