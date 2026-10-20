# Pilot Run Invalidation & System Limitations Notes

## Status: pilot-invalid
**Run Identifier**: `eval-kafka-stop-r1`  
**Execution Timestamp**: 2026-10-19 11:25:21 UTC to 11:26:42 UTC  
**Pipeline Run ID**: `ba89b977-dbf2-46cb-ae93-c9ad369a0ae9`  
**Experiment Run ID**: `62e0861c-8433-4df4-a953-e9d6d338ce2d`  

---

### Root Causes for Invalidation

1. **Pipeline Finished Before Fault Window Ended**:
   - The streaming pipeline processed the entire 300,000 row input dataset and terminated at `11:26:25.798 UTC`.
   - The fault injection (`container.stop("kafka")`) completed at `11:26:07.411 UTC` and was scheduled for 20 seconds duration followed by recovery observation.
   - At injection time, `pipeline_active_at_injection` was recorded as `false`.
   - Result: Throughput baseline and throughput during fault were recorded as `null`, rendering recovery and throughput degradation metrics non-evaluable.

2. **Pre-Fault Accounting Measured Zero Counters**:
   - `records_received` and `records_processed` in `pre_fault_accounting` were recorded as `0`.
   - The first Spark micro-batch was compiling and initializing drivers during the baseline measurement window, so `recordsIn`/`recordsOut` had not yet ticked above 0 when the snapshot was captured.
   - Furthermore, in the pipeline executor progress updates, `published` was passed as `None` once the producer thread completed, causing the experiment runner's pre-fault snapshot to read 0.

3. **Baseline Phase Timing Skew**:
   - The baseline phase took ~23 seconds instead of the expected 10 seconds because availability probing (10s) and throughput baseline sampling (`window_sleep = 10s`) execute sequentially, not concurrently.
   - Together with container stop overhead (~10s) and pipeline wait overhead, the fault hit at $t \approx 35\text{s}$, when the 300k backlog was already mostly drained.

4. **Host Memory Pressure (WSL 2 Expansion)**:
   - During the run, host free RAM dropped below the safety limit (from 1765 MB to 528 MB) as WSL 2 expanded its cache, resulting in sustained elevated page fault rates (`Pages/sec > 100`).

5. **Discrepancy in Observed Processing Rate**:
   - The historical discrepancy between the theoretical cap (5,000 rows/s under 10k/2s) and the previously observed ~9,259 rows/s in the old pilot remains **unexplained**. Calibration at a reduced cap of 3,000 rows / 2s yielded a steady ingestion throughput of ~1,472 rows/s across 101 underlying triggers captured in 89 poll events (77 single-trigger polls of ~2,998 rows, 10 double-trigger polls of ~5,997 rows coinciding with 15s host telemetry pauses, 1 triple-trigger poll of 8,994 rows during startup, and 1 tail poll of 159 rows: $77 + (10 \times 2) + (1 \times 3) + 1 = 101$ triggers = 300,000 rows), confirming that the 3,000 offset cap is strictly respected. However, this does not explain why the earlier 10,000 cap configuration exceeded 5,000 rows/s; the discrepancy remains logged as unexplained.

---

### Verified Platform & Experimental Limitations

- **Baseline Phase Sequential Execution**: The baseline phase measures availability and throughput sequentially, so it lasts approximately $2 \times \text{baseline\_seconds}$ plus probe overhead (23.1 s observed for `baseline_seconds: 10`).
- **Pre-Fault Accounting Counter Fallback**: In `backend/app/pipeline/executor.py:1325`, the executor passes `published=None` once the producer thread finishes publishing. The experiment runner's `_snapshot_pre_fault_accounting` therefore falls back to reading Spark `recordsIn`.
- **First Micro-Batch Warm-Up**: Warm-up (time from pipeline launch to first non-zero `recordsIn`) varied across calibration runs: 16 s (Oct 19 calibration), 27 s (prior Oct 20 calibration, t=26.95s), 44.64 s (confirmed Oct 20 calibration run `eval-stream-calibration-1792525960`). The delay reflects pipeline submission, Docker container scheduling, Spark driver registration, and initial micro-batch compilation. This variability must be accounted for when sizing baseline windows.
- **Stale `_spark_metadata` Silent Skip**: Re-running a Spark Structured Streaming pipeline against an output path that already contains a `_spark_metadata/` directory causes Spark to silently skip all micro-batches (the `FileStreamSink` checks `batchId <= fileLog.getLatest()` and logs `Skipping already committed batch` but writes no data). The platform backend reports the job as `success` and the tracker counters (recordsIn, recordsOut, rowsWritten) still increment normally, making the silent skip undetectable without a physical HDFS row count. This is not being fixed in the application. The harness avoids it by generating a unique timestamped output path per run and aborting if `_spark_metadata` already exists at the target path (VERIFIED: `run_evaluation.py`, `check_hdfs_metadata_exists()`).
- **Host Free RAM Margin Risk**: During calibration, host free RAM dropped to **1,564 MB** (prior calibration) and **983 MB** (confirmed Oct 20 calibration `eval-stream-calibration-1792525960`, at t≈124 s) against the **1,500 MB** pre-flight threshold, even though RAM started at 3.8 GB. WSL 2 memory expansion during Spark job execution can cause rapid drops of 2–3 GB. Host memory fluctuations during pipeline execution present a potential risk of tripping memory bounds if checked during runtime.
- **Fault Injection Latency**: Stopping Kafka cleanly via `docker.stop("kafka")` takes ~9.7 s, so the effective fault injection latency is ~12 s after the baseline concludes.
- **No Fault-Free Delay Control**: Traffic control (`tc`) is not installed in the `resilient-kafka` container image, meaning `service_delay` is unsupported on Kafka and cannot serve as a fault-free control.
- **Host Resource Boundaries**: Single physical machine (16 GB RAM), WSL 2 memory capped at 6 GB via `.wslconfig`, swap disabled.
- **Kafka Disk Log Volume**: Old Kafka topics and commit log segments remaining on the disk volume from previous runs may affect broker restart duration.
- **Validating Data Drain Loop Decoupling**: In `backend/app/reliability/experiment_runner.py:1008-1019`, `_check_timeout` is NOT called inside the `validating_data` drain loop; the drain loop uses an independent hardcoded `max_drain_wait = 360` s.

---

### Action Taken
- Artifacts preserved intact in `docs/evaluation/raw/eval-kafka-stop-r1/`.
- Aborted attempt logs preserved.
- No data deleted or modified.
- Pilot flagged as `pilot-invalid`.


---

## Status: pilot-evaluated
**Run Identifier**: `eval-kafka-stop-r2`  
**Execution Timestamp**: 2026-10-20 12:22:30 UTC to 12:24:42 UTC  
**Pipeline Run ID**: `75eb98aa-4959-4bd6-a3ca-5990c2ffe0d9`  
**Experiment Run ID**: `a97b2c16-7279-46e2-ad5b-a59a2ffdbd15`  

### Key Findings & Observations
1. **Pipeline Active at Injection & Throughput Baseline Measured**:
   - `pipeline_active_at_injection` = `true`.
   - Baseline throughput successfully captured: `1199.17 records/sec`.
   - Throughput during fault measured: `0.0 records/sec` (-100.0% degradation).
   - Recovery time: `20.02s` (Kafka container stopped and restored).
2. **Experiment Result Status: FAIL due to Data Loss Calculation**:
   - `expected_records`: 41,189 records (captured from `spark.recordsOut` snapshot at validation time).
   - `processed_records`: 0 records (actual rows read from HDFS output directory).
   - `data_loss`: 41,189 records.
   - Result: Platform SafetyValidator evaluated status as `failed` because `data_loss (41,189) > max_allowed (0)`.
3. **Host Memory & Paging Conditions**:
   - Host free RAM maintained between 2,933 MB and 3,602 MB throughout the run.
   - Host hard page faults rate remained between 0.0 and 7.9 Pages/sec.
   - `host_memory_pressure` = `false`.


---

## INVALIDATION NOTICE: eval-kafka-stop-r1 and eval-kafka-stop-r2

**Both repetitions are retroactively marked INVALID due to schema mismatch causing empty HDFS output.**

- **Root cause**: The pipeline schema in `scripts/evaluation/run_evaluation.py` declared column `sale_date` (type `string`), but `sample-data/sales_300k.csv` has column `timestamp`. Spark received `sale_date=null` for every row, the `remove_nulls` operation dropped all 300,000 rows, and HDFS output contained 0 data rows in all parquet files.
- **Impact on r1**: `expected_records=0`, `processed_records=0`, `data_loss=0`. Result appeared to be `succeeded` with zero loss only because the validator compared 0 against 0.
- **Impact on r2**: `expected_records=41,189` (taken from a mid-run `spark.recordsOut` snapshot), `processed_records=0` (HDFS rows). `data_loss=41,189` reported by the platform. This is NOT a fault-induced data loss — it reflects the same 0-row HDFS output from the schema mismatch.
- **The `recordsOut = 41,189` vs `0 rows in HDFS` discrepancy**: Unexplained for the old invalid runs. The counter values are now understood separately: `spark.recordsOut` was populated by `executor.py:1228` which unconditionally copies `total_input_rows` into `records_out` (VERIFIED: `backend/app/pipeline/executor.py:1228`, `records_out=total_input_rows`); `rowsWritten` was populated by `executor.py:1447-1451` which calls `hdfs_client.count_rows(raw_path)` in a try/except that silently swallows an `AttributeError` (HdfsStorageClient has no `count_rows` method) and falls back to `total_rows = 300,000` (VERIFIED: `backend/app/pipeline/executor.py:1447-1451`). The discrepancy between the two counter values (41,189 vs 0 rows) as reported by the platform validator for r2 remains unexplained and is to be investigated if reproduced with valid data.
- **Status**: `invalid: schema mismatch, empty output`
- **Raw artifacts preserved**: All `docs/evaluation/raw/eval-kafka-stop-r1/` and `docs/evaluation/raw/eval-kafka-stop-r2/` artifacts retained in original form. Nothing deleted or modified.

**Fix applied** (harness only, no application code):
- `scripts/evaluation/run_evaluation.py`: schema aligned with CSV (`timestamp`, not `sale_date`).
- HDFS sanity gate added to `run_calibration()`: if HDFS physical rows == 0 or != expected, calibration is marked FAILED.

---

## INVALIDATION NOTICE: October 19 & October 20 Calibrations for Output Verification

Both prior calibration runs are retroactively marked **INVALID for output verification**:

1. **October 19 Calibration Run** (`3c65c593`):
   - **Flaw**: Schema mismatch (`sale_date` instead of `timestamp`). The pipeline operation `remove_nulls` dropped all 300,000 rows.
   - **Result**: 102 Parquet part files were created in `/data/output/pipe-eval-calibration/`, but all files had 0 physical data rows (header only, 691 bytes each).
   - **Status**: `invalid: schema mismatch, 0 physical data rows`.

2. **October 20 Calibration Run** (`1a15e16e`):
   - **Flaw**: Reused the output directory `/data/output/pipe-eval-calibration/`.
   - **Mechanism**: The directory retained `_spark_metadata/` containing commit files for batches `0` through `101` dated Oct 19. Spark's Structured Streaming `FileStreamSink` executes `if (batchId <= fileLog.getLatest()) logInfo("Skipping already committed batch")`. Spark skipped all micro-batches 0..101. Zero new data or commit files were written.
   - **Status**: `invalid: stale metadata reuse, 0 physical data rows`.

---

## CALIBRATION VERIFICATION & ROOT CAUSE CONFIRMATION (October 20)

**Run Identifier**: `eval-stream-calibration-1792525960`  
**Pipeline Run ID**: `5a1a58f9-6862-4415-847c-eb4fa5fd7c0d`  
**Execution Timestamp**: 2026-10-20 19:52:44 UTC to 19:57:02 UTC  
**Output Path**: `/data/output/pipe-eval-calibration-1792525960` (fresh, isolated)  

### Root Cause Diagnosis: CONFIRMED & SUPPORTED
- The hypothesis that stale `_spark_metadata` caused zero physical rows in previous runs is **conclusively confirmed**.
- Upon executing the calibration with the corrected schema against a brand-new, unique HDFS path, Spark Structured Streaming committed all micro-batches and wrote physical Parquet files containing real rows.

### Key Metrics & Physical Ground Truth
- **Total Duration**: 257.41 s (active processing duration: 212.77 s — **ABOVE the 150–200 s target range**; warm-up / first micro-batch delay: 44.64 s).
- **Physical HDFS Row Count**: **289,744** across 304 Parquet part files.
  - Matches 100% of expected valid rows: $300,000\text{ (total)} - 5,124\text{ (null amount)} - 5,132\text{ (negative amount)} = 289,744$.
  - Physical data loss = **0**.
- **Transaction ID Uniqueness & Duplicate Accounting**:
  - The raw input file `sales_300k.csv` has 295,068 unique `transaction_id`s out of 300,000 total rows. Among the 289,744 valid rows, there are exactly 285,014 unique IDs and 4,730 duplicate copies.
  - The physical Parquet output in HDFS contains exactly **285,014 unique transaction_ids** and **4,730 duplicate copies**.
  - **Excess duplicates beyond input baseline**: **0**.
- **Platform Tracker Unreliability Verified**:
  - Tracker reported `recordsIn = 300,000`, `recordsOut = 300,000`, `rowsWritten = 300,000`.
  - The platform tracker falsely claims 300,000 rows written because `executor.py:1228` copies input to output and `executor.py:1449` falls back to `total_rows` upon an unhandled exception. The physical count is 289,744.
  - Platform validator numbers are confirmed unreliable and must be audited against independent physical HDFS counts.
- **Pre-Flight Gates**:
  - Host Free RAM: 3,814.8 MB / 3,795.1 MB / 3,736.6 MB ($\ge 1,500$ MB $\to$ **PASS**).
  - Host Pages/sec: 2.66 30s avg ($\le 100$ $\to$ **PASS**).
  - Telemetry during active streaming: Free RAM ranged from **983 MB** (minimum, at t≈124 s) to 3,848 MB (start).
- **HDFS Sanity Gate**: **PASSED**.

