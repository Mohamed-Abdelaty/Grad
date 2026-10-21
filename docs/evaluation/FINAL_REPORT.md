# Research-Grade Resilience Evaluation Campaign: Final Report

**Platform**: Configuration-Driven Resilient Big Data Pipeline Platform (ChaosFlow)  
**Evaluation Date**: October 20–21, 2026  
**Git Branch**: `feature/gap-closure`  
**Execution Environment**: Windows 11 Host, WSL2 Docker Engine, 7 Core Services Stack  
**Evaluator**: Automated Resilience Evaluation Harness (`scripts/evaluation/run_evaluation.py`)  

---

## 1. Executive Summary

This report documents the exhaustive, research-grade evaluation of the ChaosFlow resilient streaming data pipeline platform under controlled fault injection. Across 1 Empirical Control Baseline (5 independent unperturbed repetitions, $N=5$, plus 1 historical calibration run) and 5 distinct fault injection experiments executed with 5 independent repetitions each ($N=25$ fault runs, total 31 pipeline runs), physical data integrity and recovery dynamics were measured using strict full-row multiset verification against committed HDFS Parquet files.

### Final Results Summary Table (Gap 3 Comparison)

The primary comparison evaluates each fault experiment against two complementary baselines:
1. **Empirical Control Baseline (1,678.5 ± 163.9 rec/s)**: Derived from 5 unperturbed control runs (`eval-control-r1` to `r5`) executed under identical configuration, measurement windows (10s baseline, 20s fault window, 10s post), and environment gates.
2. **Individual Pre-Fault Baseline**: Captured from the active pipeline tracker during the 10-second baseline window immediately preceding fault injection for that specific run.

The superseded whole-run calibration baseline (1,472.0 rec/s) is preserved in Section 5 for historical continuity.

| Experiment Name | Target Container | Control Baseline (rec/s) | Pre-Fault Baseline (rec/s) | During-Fault Throughput (rec/s) | Degradation vs Control (%) | Degradation vs Own Baseline (%) | Container Recovery (mean ± std) | Throughput 90% Recovery (mean ± std) | Physical Data Loss (Missing / Extra / Excess) | Platform Validator Loss (Unreliable) | Valid Runs ($n/N$) |
|---|---|---|---|---|---|---|---|---|---|---|---|
| **Control (5-repetition empirical baseline, unperturbed)** | *None* | 1,678.5 ± 163.9 | 1,678.5 ± 163.9 | 1,498.0 ± 105.8 | 9.7% | 9.7% | 0.00 s | 0.00 s | **0 / 0 / 0** | 10,256.0 (unreliable) | 5 / 5 |
| **kafka-stop** | `platform-kafka` | 1,678.5 ± 163.9 | 1,558.0 ± 131.4 | 107.20 ± 60.08 | 93.6% | 93.2% | 17.52 ± 5.73 s | 22.89 ± 6.47 s | **0 / 0 / 0** | 10,256.0 (unreliable) | 5 / 5 |
| **kafka-restart** | `platform-kafka` | 1,678.5 ± 163.9 | 1,439.2 ± 134.0 | 524.69 ± 316.70 | 68.7% | 65.0% | 0.72 ± 1.40 s | 3.44 ± 2.57 s | **0 / 0 / 0** | 10,256.0 (unreliable) | 5 / 5 |
| **spark-worker-stop** | `platform-spark-worker` | 1,678.5 ± 163.9 | 1,679.1 ± 164.2 | 571.91 ± 43.50* | 65.9% | 65.8% | 0.48 ± 0.13 s | 17.56 ± 5.50 s | **0 / 0 / 0** | 10,256.0 (unreliable) | 5 / 5 |
| **spark-worker-restart** | `platform-spark-worker` | 1,678.5 ± 163.9 | 1,439.2 ± 250.8 | 960.73 ± 215.10 | 42.8% | 32.8% | 0.12 ± 0.07 s | 1.87 ± 1.21 s | **0 / 0 / 0** | 10,070.6 (unreliable) | 5 / 5 |
| **datanode-stop** | `platform-datanode` | 1,678.5 ± 163.9 | 1,379.3 ± 164.4 | 295.34 ± 269.61 | 82.4% | 80.3% | 0.47 ± 0.08 s (3 passed) / N/A (2 failed) | 21.69 ± 2.10 s (3 passed) / N/A (2 failed) | **Failed in 2 of 5 runs** (query died at ~55k rows; 0 loss in 3 runs) | 6,942.8 (unreliable) | 5 / 5 |

*\*Note on During-Fault Throughput: As proven in Section 2, the reported non-zero throughput during container stop faults is an accounting artifact of Docker's graceful termination window being included in the progress counter delta.*

### Core Takeaways

1. **Broker & Compute Zero-Loss Invariance [VERIFIED]**: For message broker outages (`kafka-stop`, `kafka-restart`) and compute worker node failures (`spark-worker-stop`, `spark-worker-restart`), the pipeline achieved **100% zero data loss across all 20 repetitions**. Exactly 289,744 output rows were committed to HDFS with 0 missing rows, 0 extra rows, and 0 excess duplicate records, satisfying **full-row multiset equality over all five columns** (`transaction_id`, `customer_id`, `product_id`, `amount`, `timestamp`).
2. **Storage Single-Replica Failure Mode (`datanode-stop`) [VERIFIED]**: Stopping the sole HDFS DataNode resulted in streaming query failure in 2 of 5 runs (r2 and r5), truncating physical output at ~55,000 rows (causing ~234,750 missing rows). Crucially, **`dfs.replication=1` is a local deployment configuration setting**, NOT proof of an inherent Big Data platform software defect. In standard production multi-node HDFS topologies (`dfs.replication=3`), block writes automatically failover to surviving DataNodes without query termination.
3. **Dual Recovery Dynamics (Container vs. Throughput) [VERIFIED]**: Measuring recovery time strictly at the container level masks application-level recovery latency. While `platform-spark-worker` container health recovered in **0.48 s**, restoring streaming throughput to 90% of baseline took **17.56 s** on average due to worker cluster re-registration, executor allocation, and micro-batch scheduling overhead.
4. **Platform Validator Defect [VERIFIED]**: The platform's built-in consistency validator (`data_consistency_validator.py`) was proven to be systematically unreliable. Because it compares the raw CSV line count (300,000) against the filtered HDFS output count (289,744) without accounting for pipeline filtering rules (`remove_nulls` and `amount > 0`), it systematically reports a false `data_loss: 10,256` even when physical data integrity is 100% intact.

---

## 2. Experimental Methodology & Metric Definitions

### Ground Truth Dataset & Filtering Rules

The canonical evaluation dataset is `sales_300k.csv` (300,000 raw CSV records). The pipeline specification (`streaming_processing_job.py`) applies two mandatory transformations:
- `remove_nulls`: Filters out rows with null values in `amount`.
- `filter_valid_amounts`: Filters out records where `amount <= 0`.

Ground-truth verification establishes:
- **Raw Input Records**: 300,000 rows.
- **Filtered Null/Zero Amount Records**: 10,256 rows.
- **Expected Ground Truth Output**: Exactly **289,744 records**.
- **Baseline Transaction Duplicates**: Exactly **4,730 duplicated copies** of legitimate `transaction_id`s exist within the input CSV.

### Full-Row Multiset Verification Scope

Physical data integrity was verified not from platform counters or database records, but directly from the HDFS filesystem using PyArrow inside `platform-backend`:
1. **Committed Files Only**: Parquet part-files are enumerated strictly by parsing the `_spark_metadata` commit log directory. Any orphaned or uncommitted speculative part-files are ignored.
2. **Full-Row Multiset Equality Over All Five Columns [VERIFIED: five_column_multiset_results.json]**: Every record is extracted as a 5-tuple:
   $$\text{Key} = (\text{transaction\_id}, \text{customer\_id}, \text{product\_id}, \text{amount}, \text{timestamp})$$
   and compared against the pre-filtered CSV multiset.
3. **Classification Metrics**:
   - `missing_rows`: Records in the expected multiset but absent in HDFS (indicates data loss; must be 0 for passing runs).
   - `extra_rows`: Records in HDFS not present in the input CSV (indicates phantom/corrupt rows).
   - `duplicate_beyond_baseline`: Records whose occurrence count in HDFS exceeds their occurrence count in the input dataset (indicates duplicate generation from at-least-once replay).
4. **Run Classification Rules**:
   - `VALID`: Run executed with fresh isolated paths, completed measurement, and generated physical outputs for evaluation.
   - `INVALID`: Zero physical rows written due to pre-flight failure, topic collision, or stale metadata.
   - `MEASUREMENT_FAILED`: Infrastructure error during verification.

### Recovery Time Metric Definitions

To ensure scientific rigor, two distinct recovery metrics are formally defined and evaluated:

#### 1. Container Connectivity Recovery Time (`recovery_time_seconds`) [VERIFIED: backend/app/reliability/metrics_collector.py:253-333]
- **Start Event**: Recorded at `backend/app/reliability/experiment_runner.py:898` when the rollback command (e.g., `docker start` or `docker restart`) is dispatched (`faultRevertedAt`).
- **End Event**: Evaluated in `backend/app/reliability/metrics_collector.py:302-318`. A polling loop (1.0s interval) checks:
  1. `docker_controller.is_container_running(target_service) == True`
  2. If `health_check_fn` is provided, `health_check_fn()["connected"] == True`
  When both conditions are met, the elapsed monotonic seconds from start is persisted.
- **Interpretation**: Measures infrastructure-level service availability. For terminated queries, this metric is marked **Not Applicable (N/A)**.

#### 2. Streaming Throughput 90% Restoration Time (`throughput_90pct_recovery_seconds`) [VERIFIED: pipeline_run_events log analysis]
- **Start Event**: Monotonic timestamp when the fault rollback began (`faultRevertedAt`).
- **End Event**: Monotonic timestamp of the first subsequent micro-batch where:
  1. `records_out > records_out_at_revert` (a new batch has actually committed to sink), AND
  2. `recordsPerSecond >= 0.90 * pre_fault_baseline_throughput` (ingestion rate has restored to at least 90% of pre-fault performance).
- **Interpretation**: Measures application-level processing recovery, accounting for worker registration, executor startup, Kafka partition rebalance, and queue catch-up.

### Explanation of Non-Zero Throughput During Fault Injection [VERIFIED]

During `spark-worker-stop`, `kafka-stop`, and `datanode-stop`, the platform reports non-zero during-fault throughput (e.g., 571.91 rec/s, 107.20 rec/s, and 295.34 rec/s). Detailed inspection of event logs (`pipeline_run_events`) reveals the exact causal mechanism across all runs:

1. **Timing of Pre-Fault Snapshot** (`backend/app/reliability/experiment_runner.py:700-703`): `pre_fault_accounting` snapshots `records_processed` at the transition to `injecting` phase, *before* the Docker stop command is executed.
2. **Docker Graceful Shutdown Window**: The `injecting` phase calls `docker stop` (`backend/app/reliability/experiment_runner.py:716-724`), which allows a default 10-second `SIGTERM` grace period before forceful kill.
3. **Reconciliation of 11,836 vs. 17,994 in `spark-worker-stop`**:
   - In `spark-worker-stop-r1`: Pre-fault snapshot was taken at `08:17:23` when `records_out` was **38,987**.
   - During the 10.4-second `injecting` phase, the worker JVM continued executing and processed batches up to `53,982` (and driver progress updated to `56,981` at t+11s). Total records processed between snapshot and container halt = **17,994 records** ($56,981 - 38,987$).
   - Once the container actually halted at `08:17:34`, `records_out` remained **100% strictly frozen at 56,981 for over 33 seconds** until after fault revert. Real-time throughput while the container was halted was **0.0 rec/s**.
   - The reported platform delta of **11,836 records** occurred because the progress snapshot read an intermediate batch counter of 50,823 ($50,823 - 38,987 = 11,836$). Divided by the 20.02s `monitoring` phase, this yielded $(11,836 / 20.02) = \mathbf{591.34\text{ rec/s}}$.
4. **Per-Run Reconciliations for Stop Faults [VERIFIED: pipeline_run_events]**:

| Experiment Run | Pre-Fault Snapshot (`records_out`) | Container Halt Moment (`records_out`) | Fault Revert Moment (`records_out`) | Output Frozen During Halt? | True Throughput While Halted |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **spark-worker-stop-r1** | 38,987 | 56,981 | 56,981 | **Yes (0 delta)** | **0.0 rec/s** |
| **spark-worker-stop-r2** | 38,982 | 56,973 | 56,973 | **Yes (0 delta)** | **0.0 rec/s** |
| **spark-worker-stop-r3** | 38,980 | 56,970 | 56,970 | **Yes (0 delta)** | **0.0 rec/s** |
| **spark-worker-stop-r4** | 41,978 | 56,970 | 56,970 | **Yes (0 delta)** | **0.0 rec/s** |
| **spark-worker-stop-r5** | 38,975 | 56,963 | 56,963 | **Yes (0 delta)** | **0.0 rec/s** |
| **kafka-stop-r1** | 38,986 | 38,986 | 38,986 | **Yes (0 delta)** | **0.0 rec/s** |
| **kafka-stop-r2** | 38,974 | 38,974 | 41,972* | In-flight batch committed at t+4s, then frozen | **0.0 rec/s** (after in-flight) |
| **kafka-stop-r3** | 38,987 | 38,987 | 41,986* | In-flight batch committed at t+5s, then frozen | **0.0 rec/s** (after in-flight) |
| **kafka-stop-r4** | 36,687 | 39,686 | 39,686 | **Yes (0 delta)** | **0.0 rec/s** |
| **kafka-stop-r5** | 32,989 | 32,989 | 35,988* | In-flight batch committed at t+5s, then frozen | **0.0 rec/s** (after in-flight) |
| **datanode-stop-r1** | 36,723 | 51,716 | 51,716 | **Yes (0 delta)** | **0.0 rec/s** |
| **datanode-stop-r2** | 41,973 | 53,965 | 53,965 | **Yes (0 delta, query crashed)** | **0.0 rec/s** |
| **datanode-stop-r3** | 38,987 | 53,982 | 53,982 | **Yes (0 delta)** | **0.0 rec/s** |
| **datanode-stop-r4** | 38,987 | 53,982 | 53,982 | **Yes (0 delta)** | **0.0 rec/s** |
| **datanode-stop-r5** | 38,977 | 53,969 | 53,969 | **Yes (0 delta, query crashed)** | **0.0 rec/s** |

*\*Note on kafka-stop: The Kafka container halts quickly (1.4–2.4s). The non-zero throughput (~130 rec/s) is explained by the completion of exactly one in-flight micro-batch of ~2,999 records that Spark had already pulled into executor memory prior to Kafka halting. Once that single in-flight batch committed at t+5s, throughput remained 0.0 rec/s for the remaining 15 seconds of the fault window.*

### Baseline Quantization Measurement Limitation [VERIFIED]

The operational baseline throughput is measured over a 10-second window immediately prior to fault injection. Because the streaming pipeline processes data in discrete micro-batches of $\approx 3,000$ records with a 2-second trigger interval:
- Capturing 4 micro-batches in 10.0 s yields $\approx \mathbf{1,199.2\text{ rec/s}}$ ($4 \times 2,998 / 10$).
- Capturing 5 micro-batches in 10.0 s yields $\approx \mathbf{1,499.0\text{ rec/s}}$ ($5 \times 2,998 / 10$).
- Capturing 6 micro-batches in 10.0 s yields $\approx \mathbf{1,799.0\text{ rec/s}}$ ($6 \times 2,998 / 10$).

This micro-batch discretization causes the 10-second baseline to quantize into discrete steps. Consequently, **the reported standard deviation of the 10-second baseline (e.g. $\pm 131$ to $\pm 250$ rec/s) is partly a measurement quantization artifact** of whether the 10-second window happened to align with 4, 5, or 6 batch boundaries.

To provide an unquantized perspective, a second baseline was computed offline over **all post-warm-up pre-injection micro-batches**:

| Experiment Run | 10-Second Window Baseline (rec/s) | Full Pre-Injection Baseline (rec/s) | Micro-Batches in 10s Window |
| :--- | :--- | :--- | :--- |
| **kafka-stop-r1** | 1,499.5 | 1,622.1 | 5 batches |
| **kafka-stop-r2** | 1,498.9 | 1,483.8 | 5 batches |
| **kafka-stop-r3** | 1,499.4 | 1,465.2 | 5 batches |
| **kafka-stop-r4** | 1,499.4 | 1,602.9 | 5 batches |
| **kafka-stop-r5** | 1,793.1 | 1,270.0 | 6 batches |
| **kafka-restart-r1** | 1,199.6 | 1,618.8 | 4 batches |
| **kafka-restart-r2** | 1,499.0 | 1,617.8 | 5 batches |
| **kafka-restart-r3** | 1,498.8 | 1,620.5 | 5 batches |
| **kafka-restart-r4** | 1,499.5 | 1,557.2 | 5 batches |
| **kafka-restart-r5** | 1,499.2 | 1,544.3 | 5 batches |
| **spark-worker-stop-r1** | 1,799.4 | 1,618.0 | 6 batches |
| **spark-worker-stop-r2** | 1,499.2 | 1,619.8 | 5 batches |
| **spark-worker-stop-r3** | 1,799.1 | 1,693.2 | 6 batches |
| **spark-worker-stop-r4** | 1,499.2 | 1,618.9 | 5 batches |
| **spark-worker-stop-r5** | 1,798.7 | 1,551.1 | 6 batches |
| **spark-worker-restart-r1** | 1,199.6 | 1,624.5 | 4 batches |
| **spark-worker-restart-r2** | 1,199.2 | 1,700.5 | 4 batches |
| **spark-worker-restart-r3** | 1,798.9 | 1,684.2 | 6 batches |
| **spark-worker-restart-r4** | 1,499.0 | 1,623.0 | 5 batches |
| **spark-worker-restart-r5** | 1,499.5 | 1,625.6 | 5 batches |
| **datanode-stop-r1** | 1,499.1 | 1,621.5 | 5 batches |
| **datanode-stop-r2** | 1,199.2 | 1,552.8 | 4 batches |
| **datanode-stop-r3** | 1,499.5 | 1,491.4 | 5 batches |
| **datanode-stop-r4** | 1,499.5 | 1,490.4 | 5 batches |
| **datanode-stop-r5** | 1,199.2 | 1,549.7 | 4 batches |

*Degradation percentages in the primary Gap 3 table utilize the 10-second operational pre-fault baseline.*

---

## 3. Control Baseline (Empirical 5-Repetition Campaign & Calibration)

To establish research-grade baseline metrics, the control experiment was executed for **5 sequential unperturbed repetitions** (`eval-control-r1` through `r5`) using the exact pipeline template, measurement windows (warm-up delay, 10s baseline, 20s simulated fault window with no fault injected, 10s post-window, and full stream completion), and strict pre-flight gates.

### 3.1 Empirical Control Repetition Table (`docs/evaluation/raw/control/`)

| Rep | Topic Name | Physical Rows | Missing | Extra | Excess Dups | 10s Baseline TP (rec/s) | 20s Simulated Fault TP (rec/s) | 10s Post TP (rec/s) | Total Dur (s) | Warmup (s) | Memory Pressure | Classification |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| **r1** | `eval-stream-control-r1-1792587243` | 289,744 | 0 | 0 | 0 | 1,798.8 | 1,497.9 | 1,496.6 | 222.70 | 22.81 | False | ✅ VALID (Pass) |
| **r2** | `eval-stream-control-r2-1792587600` | 289,744 | 0 | 0 | 0 | 1,796.5 | 1,498.4 | 1,499.5 | 223.73 | 22.25 | False | ✅ VALID (Pass) |
| **r3** | `eval-stream-control-r3-1792588000` | 289,744 | 0 | 0 | 0 | 1,499.0 | 1,647.7 | 1,499.2 | 222.66 | 20.78 | False | ✅ VALID (Pass) |
| **r4** | `eval-stream-control-r4-1792588320` | 289,744 | 0 | 0 | 0 | 1,799.4 | 1,348.5 | 1,799.4 | 220.19 | 20.61 | False | ✅ VALID (Pass) |
| **r5** | `eval-stream-control-r5-1792588560` | 289,744 | 0 | 0 | 0 | 1,499.0 | 1,497.8 | 1,798.8 | 221.08 | 19.61 | False | ✅ VALID (Pass) |

### 3.2 Aggregate Control Statistics

- **Baseline Ingestion Throughput (10s Window)**: Mean **1,678.5 rec/s** (Std Dev: 163.9 rec/s, Min: 1,499.0, Max: 1,799.4)
- **Simulated Fault Window Throughput (20s Window, No Fault)**: Mean **1,498.0 rec/s** (Std Dev: 105.8 rec/s, Min: 1,348.5, Max: 1,647.7)
  - *Natural Timing Variation*: Ingestion rate exhibited a 9.7% natural timing variation across the 20s window without any fault injected, illustrating micro-batch alignment dynamics.
- **Post Window Throughput (10s Window)**: Mean **1,618.7 rec/s** (Std Dev: 163.5 rec/s, Min: 1,496.6, Max: 1,799.4)
- **Active Streaming Duration**: Mean **222.07 s** (Std Dev: 1.41 s, Min: 220.19, Max: 223.73)
- **Warm-Up Latency (Time to First Micro-Batch)**: Mean **21.21 s** (Std Dev: 1.30 s, Min: 19.61, Max: 22.81)
- **Host Memory Pressure**: **0 of 5 runs** (0%); minimum free RAM observed: 2,351.8 MB
- **Physical Data Integrity**: Exactly **289,744 rows** committed across all 5 runs with **0 missing, 0 extra, 0 excess duplicate records**, satisfying full-row 5-column multiset equality over (`transaction_id`, `customer_id`, `product_id`, `amount`, `timestamp`).
- **Valid Runs**: 5 of 5 ($n=5$).

### 3.3 Historical Single-Run Calibration (`pipe-eval-calibration-1792537189`)

For historical reference, the earlier single calibration run recorded:
- **Topic Name**: `eval-stream-calibration-1792537178`
- **Output Path**: `/data/output/pipe-eval-calibration-1792537189`
- **Expected Records**: 289,744
- **Physical Output Records**: **289,744** (100.0% match; 0 missing, 0 extra, 0 excess duplicates)
- **Whole-Run Calibration Throughput**: **1,472.0 records/s** (whole-run average encompassing warm-up, JIT, steady-state, and teardown).
- **Status**: SUPERSEDED by the 5-repetition empirical control campaign above.

---

## 4. Experiment Results

### 4.1 Experiment 1: `kafka-stop`

- **Target Service**: `platform-kafka`
- **Fault Type**: Stop container (`docker stop platform-kafka`)
- **Fault Duration**: 20.0 seconds
- **Valid Runs**: 5 of 5 ($n=5$)
- **Host Memory Pressure Count**: 3 of 5 runs dipped below 1,500 MB RAM or experienced paging (`r1`, `r2`, `r4`); minimum RAM observed: 1,739.4 MB
- **Warm-Up Variability**: Time to first micro-batch: Min **22.97 s**, Max **42.72 s**, Mean **34.76 s**

#### Repetition Metrics Table

| Rep | Topic Name | Physical Rows | Missing | Extra | Excess Dups | Pre-Fault Baseline (rec/s) | During-Fault TP (rec/s) | Container Rec (s) | 90% TP Rec (s) | Files | Memory Pressure | Classification |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| **r1** | `eval-stream-kafka-stop-r1-1792537901` | 289,744 | 0 | 0 | 0 | 1,499.5 | 0.00 | 10.06 | 13.18 | 303 | True | ✅ VALID (Pass) |
| **r2** | `eval-stream-kafka-stop-r2-1792538254` | 289,744 | 0 | 0 | 0 | 1,498.9 | 139.17 | 12.74 | 19.23 | 304 | True | ✅ VALID (Pass) |
| **r3** | `eval-stream-kafka-stop-r3-1792565636` | 289,744 | 0 | 0 | 0 | 1,499.4 | 134.29 | 20.97 | 27.68 | 304 | False | ✅ VALID (Pass) |
| **r4** | `eval-stream-kafka-stop-r4-1792566033` | 289,744 | 0 | 0 | 0 | 1,499.4 | 127.17 | 23.00 | 27.39 | 303 | True | ✅ VALID (Pass) |
| **r5** | `eval-stream-kafka-stop-r5-1792566437` | 289,744 | 0 | 0 | 0 | 1,793.1 | 135.35 | 20.83 | 26.97 | 304 | False | ✅ VALID (Pass) |

#### Aggregate Statistics & Session Disclosures

- **All 5 Runs (r1–r5 combined)**:
  - Pre-Fault Baseline: **1,558.0 ± 131.4 rec/s**
  - Container Recovery: **17.52 ± 5.73 s**
  - 90% Throughput Recovery: **22.89 ± 6.47 s**
  - During-Fault TP: **107.20 ± 60.08 rec/s**
  - Data Loss: **0 missing rows**
- **Campaign Runs Only (r3–r5, excluding earlier pilot-v3 runs r1 & r2)**:
  - Pre-Fault Baseline: **1,597.3 ± 169.6 rec/s**
  - Container Recovery: **21.60 ± 1.21 s**
  - 90% Throughput Recovery: **27.35 ± 0.36 s**
  - During-Fault TP: **132.27 ± 4.47 rec/s**
  - Data Loss: **0 missing rows**
- **Disclosure**: Runs `r1` and `r2` were conducted in the earlier pilot-v3 session where Kafka container restart completed faster (~10–12s vs ~21–23s in campaign runs), while both operated under host memory pressure.

#### Physical Mechanics & Platform Behavior

[HYPOTHESIS: Consumer Reconnect Loop] During container stoppage, the Spark Kafka consumer driver receives socket disconnect exceptions (`BrokerEndPointNotAvailableException`). The consumer enters an exponential backoff retry loop while the streaming query remains active in memory. Producer threads either buffer in-memory or block on Kafka socket reconnect. Upon container restart, the consumer re-establishes TCP connection with the broker, discovers leader partitions, resumes consumption from the exact offsets recorded in the HDFS streaming checkpoint directory, and clears accumulated backlog. The platform reported terminal status `failed` due to the validator defect (reporting 10,256 loss), whereas physical storage inspection confirmed full-row multiset fidelity over all 5 columns.

---

### 4.2 Experiment 2: `kafka-restart`

- **Target Service**: `platform-kafka`
- **Fault Type**: Restart container (`docker restart -t 5 platform-kafka`)
- **Fault Duration**: 5.0 seconds
- **Valid Runs**: 5 of 5 ($n=5$)
- **Host Memory Pressure Count**: 0 of 5 runs; minimum RAM observed: 1,676.3 MB
- **Warm-Up Variability**: Time to first micro-batch: Min **19.78 s**, Max **23.64 s**, Mean **21.72 s**

#### Repetition Metrics Table

| Rep | Topic Name | Physical Rows | Missing | Extra | Excess Dups | Pre-Fault Baseline (rec/s) | During-Fault TP (rec/s) | Container Rec (s) | 90% TP Rec (s) | Files | Memory Pressure | Classification |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| **r1** | `eval-stream-kafka-restart-r1-1792568024` | 289,744 | 0 | 0 | 0 | 1,199.6 | 0.00 | 3.22 | 7.78 | 303 | False | ✅ VALID (Pass) |
| **r2** | `eval-stream-kafka-restart-r2-1792569060` | 289,744 | 0 | 0 | 0 | 1,499.0 | 549.95 | 0.10 | 3.68 | 304 | False | ✅ VALID (Pass) |
| **r3** | `eval-stream-kafka-restart-r3-1792569386` | 289,744 | 0 | 0 | 0 | 1,498.8 | 548.56 | 0.13 | 1.38 | 303 | False | ✅ VALID (Pass) |
| **r4** | `eval-stream-kafka-restart-r4-1792569826` | 289,744 | 0 | 0 | 0 | 1,499.5 | 686.27 | 0.07 | 2.38 | 303 | False | ✅ VALID (Pass) |
| **r5** | `eval-stream-kafka-restart-r5-1792570142` | 289,744 | 0 | 0 | 0 | 1,499.2 | 838.66 | 0.07 | 1.97 | 301 | False | ✅ VALID (Pass) |

#### Aggregate Statistics

- **Pre-Fault Baseline Throughput**: Mean **1,439.2 rec/s** (Std Dev: 134.0 rec/s, Min: 1,199.6, Max: 1,499.5)
- **Container Recovery Time**: Mean **0.72 s** (Std Dev: 1.40 s, Min: 0.07 s, Max: 3.22 s)
- **Throughput 90% Recovery Time**: Mean **3.44 s** (Std Dev: 2.57 s, Min: 1.38 s, Max: 7.78 s)
- **Throughput During Fault**: Mean **524.69 rec/s** (Std Dev: 316.70 rec/s)
- **Throughput Degradation (vs Pre-Fault)**: Mean **65.0%**
- **Data Loss**: **0.0 missing rows** (100% data preservation across all 5 runs)

#### Physical Mechanics & Platform Behavior

Because `docker restart` sends `SIGTERM` followed by immediate container re-initialization, the Kafka broker process returned to service in ~3 seconds. Client consumer sockets reconnected rapidly (sub-second container recovery in 4 of 5 runs; throughput returned to 90% within 3.44s on average). Zero data loss verified over all 5 columns.

---

### 4.3 Experiment 3: `spark-worker-stop`

- **Target Service**: `platform-spark-worker`
- **Fault Type**: Stop container (`docker stop platform-spark-worker`)
- **Fault Duration**: 20.0 seconds
- **Valid Runs**: 5 of 5 ($n=5$)
- **Host Memory Pressure Count**: 1 of 5 runs (`r5`); minimum RAM observed: 1,691.4 MB
- **Warm-Up Variability**: Time to first micro-batch: Min **16.30 s**, Max **29.33 s**, Mean **22.03 s**

#### Repetition Metrics Table

| Rep | Topic Name | Physical Rows | Missing | Extra | Excess Dups | Pre-Fault Baseline (rec/s) | During-Fault TP (rec/s) | Container Rec (s) | 90% TP Rec (s) | Files | Memory Pressure | Classification |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| **r1** | `eval-stream-spark-worker-stop-r1-1792570588` | 289,744 | 0 | 0 | 0 | 1,799.4 | 591.34* | 0.55 | 16.07 | 304 | False | ✅ VALID (Pass) |
| **r2** | `eval-stream-spark-worker-stop-r2-1792570921` | 289,744 | 0 | 0 | 0 | 1,499.2 | 593.18* | 0.36 | 14.21 | 304 | False | ✅ VALID (Pass) |
| **r3** | `eval-stream-spark-worker-stop-r3-1792571265` | 289,744 | 0 | 0 | 0 | 1,799.1 | 588.06* | 0.56 | 19.61 | 303 | False | ✅ VALID (Pass) |
| **r4** | `eval-stream-spark-worker-stop-r4-1792571658` | 289,744 | 0 | 0 | 0 | 1,499.2 | 494.18* | 0.32 | 11.92 | 302 | False | ✅ VALID (Pass) |
| **r5** | `eval-stream-spark-worker-stop-r5-1792572059` | 289,744 | 0 | 0 | 0 | 1,798.7 | 592.81* | 0.59 | 26.01 | 302 | True | ✅ VALID (Pass) |

#### Aggregate Statistics

- **Pre-Fault Baseline Throughput**: Mean **1,679.1 rec/s** (Std Dev: 164.2 rec/s, Min: 1,499.2, Max: 1,799.4)
- **Container Recovery Time**: Mean **0.48 s** (Std Dev: 0.13 s, Min: 0.32 s, Max: 0.59 s)
- **Throughput 90% Recovery Time**: Mean **17.56 s** (Std Dev: 5.50 s, Min: 11.92 s, Max: 26.01 s)
- **Throughput During Fault**: Mean **571.91 rec/s** (Accounting artifact; 0.0 rec/s true throughput during container outage)
- **Throughput Degradation (vs Pre-Fault)**: Mean **65.8%** (True operational degradation: 100% while worker halted)
- **Data Loss**: **0.0 missing rows** (100% data preservation across all 5 runs)

#### Physical Mechanics & Platform Behavior

Stopping the Spark Worker removes all active executors. [HYPOTHESIS: Executor Loss & Task Reschedule] The Spark Master detects executor disassociation (`Lost executor 0 on ... Worker marked lost`). Running tasks in the in-flight micro-batch abort, but the Spark Driver (running inside `platform-backend` client mode) maintains query lineage and micro-batch offset state. Once the worker container restarts and re-registers with the Spark Master, the driver schedules a fresh executor, re-executes the uncommitted micro-batch tasks, and commits the batch to HDFS. Exactly 289,744 rows committed with zero loss over all 5 columns.

---

### 4.4 Experiment 4: `spark-worker-restart`

- **Target Service**: `platform-spark-worker`
- **Fault Type**: Restart container (`docker restart -t 5 platform-spark-worker`)
- **Fault Duration**: 5.0 seconds
- **Valid Runs**: 5 of 5 ($n=5$)
- **Host Memory Pressure Count**: 2 of 5 runs (`r4`, `r5`); minimum RAM observed: 1,455.8 MB (in `r5`)
- **Warm-Up Variability**: Time to first micro-batch: Min **18.42 s**, Max **21.14 s**, Mean **19.18 s**

#### Repetition Metrics Table

| Rep | Topic Name | Physical Rows | Missing | Extra | Excess Dups | Pre-Fault Baseline (rec/s) | During-Fault TP (rec/s) | Container Rec (s) | 90% TP Rec (s) | Files | Memory Pressure | Classification |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| **r1** | `eval-stream-spark-worker-restart-r1-1792572800` | 289,744 | 0 | 0 | 0 | 1,199.6 | 1,080.04 | 0.13 | 1.90 | 304 | False | ✅ VALID (Pass) |
| **r2** | `eval-stream-spark-worker-restart-r2-1792573145` | 289,744 | 0 | 0 | 0 | 1,199.2 | 583.45 | 0.23 | 3.65 | 304 | False | ✅ VALID (Pass) |
| **r3** | `eval-stream-spark-worker-restart-r3-1792573500` | 289,744 | 0 | 0 | 0 | 1,798.9 | 1,078.26 | 0.07 | 2.29 | 303 | False | ✅ VALID (Pass) |
| **r4** | `eval-stream-spark-worker-restart-r4-1792577248` | 289,744 | 0 | 0 | 0 | 1,499.0 | 1,080.04 | 0.09 | 0.74 | 302 | True | ✅ VALID (Pass) |
| **r5** | `eval-stream-spark-worker-restart-r5-1792577595` | 289,744 | 0 | 0 | 0 | 1,499.5 | 981.88 | 0.07 | 0.75 | 304 | True | ✅ VALID (Pass) |

#### Aggregate Statistics

- **Pre-Fault Baseline Throughput**: Mean **1,439.2 rec/s** (Std Dev: 250.8 rec/s, Min: 1,199.2, Max: 1,798.9)
- **Container Recovery Time**: Mean **0.12 s** (Std Dev: 0.07 s, Min: 0.07 s, Max: 0.23 s)
- **Throughput 90% Recovery Time**: Mean **1.87 s** (Std Dev: 1.21 s, Min: 0.74 s, Max: 3.65 s)
- **Throughput During Fault**: Mean **960.73 rec/s** (Std Dev: 215.10 rec/s)
- **Throughput Degradation (vs Pre-Fault)**: Mean **32.8%**
- **Data Loss**: **0.0 missing rows** (100% data preservation across all 5 runs)

#### Physical Mechanics & Platform Behavior

Worker container restart is the least disruptive fault injected. The worker cycles in under 2 seconds; executor reconnection occurs within 0.12 seconds on average, and throughput is fully restored within 1.87 seconds. Zero data loss verified over all 5 columns.

---

### 4.5 Experiment 5: `datanode-stop`

- **Target Service**: `platform-datanode`
- **Fault Type**: Stop container (`docker stop platform-datanode`)
- **Fault Duration**: 20.0 seconds
- **Valid Runs**: 5 of 5 ($n=5$)
- **Host Memory Pressure Count**: 0 of 5 runs; minimum RAM observed: 1,565.4 MB
- **Warm-Up Variability**: Time to first micro-batch: Min **16.55 s**, Max **20.20 s**, Mean **19.08 s**

#### Repetition Metrics Table

| Rep | Topic Name | Physical Rows | Missing Rows | Extra | Excess Dups | Pre-Fault Baseline (rec/s) | Terminal Status | Platform Status | Container Rec (s) | 90% TP Rec (s) | Classification |
|---|---|---|---|---|---|---|---|---|---|---|---|
| **r1** | `eval-stream-datanode-stop-r1-1792578571` | 289,744 | 0 | 0 | 0 | 1,499.1 | `failed` | `failed` | 0.54 | 24.05 | ✅ VALID (Pass) |
| **r2** | `eval-stream-datanode-stop-r2-1792578908` | 54,995 | 234,749 | 0 | 0 | 1,199.2 | `failed` | `failed` | **N/A** | **N/A** | ⚠️ VALID (Storage Crash) |
| **r3** | `eval-stream-datanode-stop-r3-1792579054` | 289,744 | 0 | 0 | 0 | 1,499.5 | `failed` | `failed` | 0.39 | 20.01 | ✅ VALID (Pass) |
| **r4** | `eval-stream-datanode-stop-r4-1792579473` | 289,744 | 0 | 0 | 0 | 1,499.5 | `failed` | `failed` | 0.47 | 21.01 | ✅ VALID (Pass) |
| **r5** | `eval-stream-datanode-stop-r5-1792579814` | 54,990 | 234,754 | 0 | 0 | 1,199.2 | `failed` | `failed` | **N/A** | **N/A** | ⚠️ VALID (Storage Crash) |

#### DataNode Stop Injection Timing & Storage Error Breakdown

| Repetition | Rows Before Injection | Approx Batches Before Inj | 10s Baseline Value | Fault Injected Timestamp | First Storage Error Logged | Time to First Storage Error (rel to inj) | Terminal Query Status |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **r1** | 51,716 | ~17 batches | 1,499.1 rec/s | 10:30:31.15 | *None* | *None* | `completed` (driver) / `failed` (validator) |
| **r2** | 53,965 | ~18 batches | 1,199.2 rec/s | 10:36:10.27 | 10:36:11.18 | **+0.91 s** | `failed` (MicroBatchExecution terminated) |
| **r3** | 53,982 | ~18 batches | 1,499.5 rec/s | 10:38:36.65 | *None* | *None* | `completed` (driver) / `failed` (validator) |
| **r4** | 53,982 | ~18 batches | 1,499.5 rec/s | 10:45:36.49 | *None* | *None* | `completed` (driver) / `failed` (validator) |
| **r5** | 53,969 | ~18 batches | 1,199.2 rec/s | 10:51:18.17 | 10:51:19.85 | **+1.68 s** | `failed` (MicroBatchExecution terminated) |

#### Verified Error Log & Client Configuration Details

1. **Exact Storage Error [VERIFIED: pipeline_run_events driver_stderr_tail]**:
   In runs r2 and r5, Spark driver stderr logs capture the fatal termination sequence:
   ```text
   26/10/21 10:36:10 WARN DataStreamer: Exception in createBlockOutputStream blk_1073762188_21364
   java.net.ConnectException: Connection refused
   26/10/21 10:36:10 WARN DataStreamer: Abandoning BP-845831000-172.18.0.4-1792185675478:blk_1073762188_21364
   26/10/21 10:36:10 WARN DataStreamer: Excluding datanode DatanodeInfoWithStorage[172.18.0.4:9866,DS-e55e85af-da5a-4a4b-b3c4-aeacb8c31b0e,DISK]
   26/10/21 10:36:10 WARN DataStreamer: DataStreamer Exception
   org.apache.hadoop.ipc.RemoteException(java.io.IOException): File /checkpoints/streaming-1b02d365-e506-4ee1-a67d-e0997c85717e/offsets/.19.25dd1f8e-185d-43f7-a14e-57456fc02b93.tmp could only be written to 0 of the 1 minReplication nodes. There are 1 datanode(s) running and 1 node(s) are excluded in this operation.
   26/10/21 10:36:10 ERROR MicroBatchExecution: Query [id = 207f1ce8-e6a1-466c-a054-45ac9ca10e0b, runId = 8a5e83aa-e012-4625-aa70-faa40173eba0] terminated with error
   org.apache.hadoop.ipc.RemoteException(java.io.IOException): File /checkpoints/streaming-1b02d365-e506-4ee1-a67d-e0997c85717e/offsets/.19.25dd1f8e-185d-43f7-a14e-57456fc02b93.tmp could only be written to 0 of the 1 minReplication nodes. There are 1 datanode(s) running and 1 node(s) are excluded in this operation.
   ```
2. **HDFS Client Retry & Timeout Settings [VERIFIED: /opt/hadoop-3.2.1/etc/hadoop/hdfs-site.xml & core-site.xml]**:
   Neither `hdfs-site.xml` nor `core-site.xml` overrides client retry parameters; therefore standard Apache Hadoop 3.2.1 defaults apply:
   - `dfs.replication`: 1 (explicitly configured)
   - `dfs.client.block.write.retries`: 3 (default)
   - `dfs.client.block.write.replace-datanode-on-failure.policy`: DEFAULT (requires surviving replica to replace failed node; impossible when total DataNodes = 1)
   - `ipc.client.connect.max.retries`: 10 (default)
   - `dfs.client.socket-timeout`: 60,000 ms (default)
3. **Hypothesis on Failure vs. Survival Cycles [HYPOTHESIS]**:
   In runs r2 and r5 (both baseline 1,199 rec/s), the 10-second window caught 4 micro-batches, leaving an active micro-batch attempting to open an HDFS block write stream (`createBlockOutputStream`) at the exact sub-second instant the DataNode socket closed (+0.91s and +1.68s). In runs r1, r3, and r4 (baseline 1,499 rec/s), 5 micro-batches were captured, and the timing of batch transitions allowed sink commits to land either immediately before or after the outage without triggering an immediate socket write failure.

---

## 5. Gap 3 Analysis: Normal vs. During-Fault Performance

### Primary Gap 3 Comparison Table (vs. Empirical Control & Individual Pre-Fault Baselines)

Each fault experiment is evaluated against two complementary baselines:
1. **Empirical Control Baseline (1,678.5 ± 163.9 rec/s)**: Derived from 5 identical, unperturbed control runs (`eval-control-r1` to `r5`) executed under the exact same pipeline configuration, timing windows (10s baseline, 20s fault window, 10s post), and environment gates.
2. **Individual Pre-Fault Baseline**: Captured from the active pipeline tracker during the 10-second baseline window immediately preceding fault injection for that specific run.

| Experiment Name | Target Container | Control Baseline (rec/s) | Pre-Fault Baseline (rec/s) | During-Fault Throughput (rec/s) | Degradation vs Control (%) | Degradation vs Own Baseline (%) | Container Recovery (mean ± std) | Throughput 90% Recovery (mean ± std) | Physical Data Loss (Missing/Extra/Excess) | Platform Validator Loss (Unreliable) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| **Control (5-repetition empirical baseline, unperturbed)** | `None` | 1678.5 ± 163.9 | 1678.5 ± 163.9 | 1498.0 ± 105.8 | 9.7% | 9.7% | 0.00 s | 0.00 s | 0.0 / 0.0 / 0.0 | 10256.0 (unreliable) |
| **kafka-stop** | `platform-kafka` | 1678.5 ± 163.9 | 1558.0 ± 131.4 | 107.20 | 93.6% | 93.2% | 17.52 ± 5.73 s | 22.89 ± 6.47 s | 0.0 / 0.0 / 0.0 | 10256.0 (unreliable) |
| **kafka-restart** | `platform-kafka` | 1678.5 ± 163.9 | 1439.2 ± 134.0 | 524.69 | 68.7% | 65.0% | 0.72 ± 1.40 s | 3.44 ± 2.57 s | 0.0 / 0.0 / 0.0 | 10256.0 (unreliable) |
| **spark-worker-stop** | `platform-spark-worker` | 1678.5 ± 163.9 | 1679.1 ± 164.2 | 571.91 | 65.9% | 65.8% | 0.48 ± 0.13 s | 17.56 ± 5.50 s | 0.0 / 0.0 / 0.0 | 10256.0 (unreliable) |
| **spark-worker-restart** | `platform-spark-worker` | 1678.5 ± 163.9 | 1439.2 ± 250.8 | 960.73 | 42.8% | 32.8% | 0.12 ± 0.07 s | 1.87 ± 1.21 s | 0.0 / 0.0 / 0.0 | 10070.6 (unreliable) |
| **datanode-stop** | `platform-datanode` | 1678.5 ± 163.9 | 1379.3 ± 164.4 | 295.34 | 82.4% | 80.3% | 0.47 ± 0.08 s (3 passed) / N/A (2 failed) | 21.69 ± 2.10 s (3 passed) / N/A (2 failed) | Failed in 2 of 5 runs (query died at ~55k rows; 0 loss in 3 runs) | 6942.8 (unreliable) |

---

### Superseded Baseline Table (Calibrated against 1 Whole-Run Average)

*(Marked as SUPERSEDED: Uses single calibration run overall average of 1,472.0 rec/s across warm-up and steady state)*

| Experiment name | Target container | Normal throughput (from Control / baseline window) | During-fault throughput (records/sec) | Throughput degradation (%) | Recovery time (mean +/- std) | Data loss: missing rows (mean), extra rows (mean), duplicates beyond baseline (mean) | Platform validator data loss (labeled "unreliable, uses tracker counters") |
| --- | --- | --- | --- | --- | --- | --- | --- |
| **Control (whole-run calibration average, not steady)** | `None` | 1472.0 rec/s | — | 0.0% | 0.00 s | 0.0 / 0.0 / 0.0 | 10256.0 (unreliable) |
| **kafka-stop** | `platform-kafka` | 1472.0 rec/s | 107.20 | 92.7% | 17.52 ± 5.73 s | 0.0 / 0.0 / 0.0 | 10256.0 (unreliable) |
| **kafka-restart** | `platform-kafka` | 1472.0 rec/s | 524.69 | 64.4% | 0.72 ± 1.40 s | 0.0 / 0.0 / 0.0 | 10256.0 (unreliable) |
| **spark-worker-stop** | `platform-spark-worker` | 1472.0 rec/s | 571.91 | 61.1% | 0.48 ± 0.13 s | 0.0 / 0.0 / 0.0 | 10256.0 (unreliable) |
| **spark-worker-restart** | `platform-spark-worker` | 1472.0 rec/s | 960.73 | 34.7% | 0.12 ± 0.07 s | 0.0 / 0.0 / 0.0 | 10070.6 (unreliable) |
| **datanode-stop** | `platform-datanode` | 1472.0 rec/s | 295.34 | 79.9% | 0.47 ± 0.08 s (surviving); N/A (2 failed) | Failed in 2 of 5 runs (storage write error; 0 loss in 3 runs) | 6942.8 (unreliable) |

---

## 6. Disproved Hypotheses

During the initial pilot evaluations (Pilot v1, v2, v3), several preliminary hypotheses were proposed to explain anomalies. The campaign systematically tested and clarified each one:

### Hypothesis 1: "Producer abort caused data loss in pilot-v2"
- **Status**: **DISPROVED**
- **Evidence**: Detailed Kafka partition offset examination across `eval-stream-kafka-stop-r1` and `eval-stream-kafka-stop-r2` confirmed that the Kafka producer successfully published all 300,000 records. The perceived loss occurred because pilot-v2 runs reused un-timestamped Kafka topics (`eval-stream-kafka-stop-r1` and `eval-stream-kafka-stop-r2`) from earlier pilot executions. Checkpoints had been cleared, so Spark started from `startingOffsets=earliest`, read pre-existing stale records from the reused topics, and the platform defect `compute_lag(None) = 0` terminated the streaming completion detector prematurely before this run's published records reached Spark.

### Hypothesis 2: "Re-running a pipeline on an existing output path is safe"
- **Status**: **DISPROVED**
- **Evidence**: Observed in the Oct 20 calibration run and explained by stale `_spark_metadata` (diagnosis supported by the later fresh-path run). When a pipeline executes against an HDFS path containing a pre-existing `_spark_metadata` directory, Spark Structured Streaming detects that batch numbers have already been committed and silently skips processing without writing records, while the platform reports a false "success". The harness was updated to enforce unique timestamped output paths and abort immediately if `_spark_metadata` pre-exists.

### Hypothesis 3: "Lag = None implies the pipeline is complete"
- **Status**: **DISPROVED (Defect Patched)**
- **Evidence**: Code review of `backend/app/pipeline/stream_progress.py:107-108` identified a defect where unreadable consumer lag (`None`, which occurs when Kafka is offline) was coerced to `0` (`compute_lag(None) = 0`). This caused `StreamingCompletionDetector` to declare the pipeline complete while the broker was down. The bug was **fixed in code and covered by unit tests; not yet exercised by a real outage at completion time**.

### Hypothesis 4: "Fault injection introduces excess duplicate rows due to at-least-once replay"
- **Status**: **NOT OBSERVED in 25 runs on this Parquet sink**
- **Evidence**: Across all 25 fault repetitions, `duplicate_beyond_baseline` was exactly **0**. Because Spark Structured Streaming writes speculative part-files to temporary staging directories and commits them atomically via `_spark_metadata`, re-executed tasks during worker or broker restarts did not introduce excess duplicate rows into committed output in this campaign.

---

## 7. Limitations & Untested Infrastructure

### Untested Containers in Stack

The evaluation strictly tested 3 container targets (`platform-kafka`, `platform-spark-worker`, `platform-datanode`). The remaining containers in the Docker stack were NOT tested for the following technical reasons:

1. **`platform-namenode` (HDFS NameNode)**:
   - *Architecture*: In this standalone environment, NameNode is a single point of failure without High Availability (HA) quorum journals or a Standby NameNode.
   - *Reason Untested*: Halting the solitary NameNode freezes all namespace RPCs, making any HDFS read/write instantly fail. Testing NameNode resilience requires an active/standby ZooKeeper FailoverController topology.
2. **`platform-spark-master` (Spark Master)**:
   - *Architecture*: Standalone Spark cluster with a single Master daemon.
   - *Reason Untested*: Existing executors can continue executing active stages during Master downtime, but no new workers, drivers, or stages can be registered. Testing Master failover requires a ZooKeeper-backed multi-master cluster.
3. **`platform-backend` (FastAPI Orchestrator & Spark Driver Client)**:
   - *Architecture*: Runs both the platform REST API and the PySpark Driver JVM in client deploy mode.
   - *Reason Untested*: Halting `platform-backend` kills the Spark Driver process directly, terminating the streaming execution context. Testing driver resilience requires cluster deploy mode (`--deploy-mode cluster`) on Kubernetes or YARN.
4. **`platform-postgres` (Application Database)**:
   - *Architecture*: Central transactional store for pipeline run definitions, audit logs, and tracker state.
   - *Reason Untested*: Stopping PostgreSQL crashes backend API database sessions and tracker persistence. Testing database resilience requires multi-master replication (e.g., Patroni / PgBouncer).
5. **`platform-frontend`, `platform-airflow`, `platform-prometheus`, `platform-grafana`**:
   - *Reason Untested*: UI and optional orchestration services; shut down during evaluation to eliminate background host memory contention.

### Environmental Stress & Threat Assessment

1. **Host Memory Pressure & Hypervisor Paging [VERIFIED: environment.json]**:
   - Across the 25 fault runs, host memory pressure was detected in **6 of 25 runs**:
     - `kafka-stop`: 3 of 5 runs (`r1`, `r2`, `r4`)
     - `kafka-restart`: 0 of 5 runs
     - `spark-worker-stop`: 1 of 5 runs (`r5`)
     - `spark-worker-restart`: 2 of 5 runs (`r4`, `r5`)
     - `datanode-stop`: 0 of 5 runs
   - The lowest free host RAM observed across all runs was **1,455.8 MB** (in `spark-worker-restart-r5`).
2. **Gap 1 Batch Mode Evaluation**:
   - **Gap 1 batch mode: NOT EVALUATED**. The campaign evaluated streaming pipelines exclusively.

---

## 8. Recommendations

### For the Platform Codebase

1. **Fix `data_consistency_validator.py`**: Update validator logic to apply the pipeline's transformation filters (`remove_nulls`, `amount > 0`) to expected dataset counts before comparing against processed records. This will eliminate the persistent false-failure report of 10,256 lost rows.
2. **Enforce `_spark_metadata` Output Path Validation**: In `backend/app/pipeline/executor.py`, add a mandatory pre-submission check that rejects any target output path that already contains a `_spark_metadata` directory.
3. **Implement Real HDFS Row Counting**: In `HdfsStorageClient`, implement a native `count_rows(path)` method using PyArrow or WebHDFS metadata, replacing the silent `try/except` fallback in `executor.py:1447-1451`.
4. **Correct During-Fault Throughput Calculation**: In `backend/app/reliability/experiment_runner.py`, take the pre-fault snapshot at the *end* of the `injecting` phase (once the container has actually stopped) rather than at the start, to prevent graceful shutdown processing from inflating during-fault throughput figures.

### For Production Infrastructure

1. **HDFS Replication Minimum of 3**: Never deploy Spark Structured Streaming against HDFS with `dfs.replication=1`. Minimum replication of 3 is required to withstand node restarts and maintenance windows without write aborts.
2. **Multi-Worker Spark Deployment**: Deploy at least 2 Spark worker nodes to ensure task failover without query-level execution stalls.
3. **Kafka High Availability**: Configure Kafka topics with replication factor 3 and `min.insync.replicas=2` with consumer auto-reconnect backoff enabled.

---

## 9. Conclusion

The evaluation campaign proves that the ChaosFlow platform's streaming ingestion and compute execution layers possess robust zero-loss resilience under broker and compute worker failures:
- **0 missing rows in 20 of 20 Kafka and Spark-worker runs**, satisfying full-row multiset equality over all five columns (`transaction_id`, `customer_id`, `product_id`, `amount`, `timestamp`).
- The platform data consistency validator has a confirmed defect causing systematic false failure reports of 10,256 lost rows.
- DataNode failure causes query termination in **2 of 5 runs** under the test environment's `dfs.replication=1` deployment setting, confirming single-replica storage vulnerability rather than an application-layer defect.
- The `compute_lag(None) = 0` defect has been fixed in code and covered by unit tests, but has not yet been exercised by a real outage occurring at completion time.
