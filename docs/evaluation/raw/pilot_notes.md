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
- **Platform Tracker `records_out` = Input Rows (Known Limitation, Unreliable)**: `executor.py:1228` unconditionally sets `records_out = total_input_rows` (VERIFIED: `backend/app/pipeline/executor.py:1228`). When `recordsOut` is `None` or missing, `experiment_runner.py:1039` falls back to `expected_records = 0`, which would cause the safety validator to report 0 data loss even during total output destruction. Both behaviors are known limitations, left untouched by design. The harness judges data loss exclusively from physical HDFS output.
- **Fault Injection Latency**: Stopping Kafka cleanly via `docker.stop("kafka")` takes ~9.7 s, so the effective fault injection latency is ~12 s after the baseline concludes.
- **No Fault-Free Delay Control**: Traffic control (`tc`) is not installed in the `resilient-kafka` container image, meaning `service_delay` is unsupported on Kafka and cannot serve as a fault-free control.
- **Host Resource Boundaries**: Single physical machine (16 GB RAM), WSL 2 memory capped at 6 GB via `.wslconfig`, swap disabled.
- **Experimental Conditions**: Stack during evaluation: 7 core containers only. Frontend, Prometheus, Grafana and Airflow (postgres, webserver, scheduler) were stopped.
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

---

## CALIBRATION RUN RECORD: a4cee60d (October 20)

**Run Identifier**: `eval-stream-calibration-1792529256`  
**Pipeline Run ID**: `a4cee60d-4619-4471-97a0-22064307dcce`  
**Execution Timestamp**: 2026-10-20 20:47:40 UTC to 20:51:21 UTC  
**Output Path**: `/data/output/pipe-eval-calibration-1792529256`  
**Status**: `harness measurement error (pyarrow SIGABRT exit 134 at interpreter teardown); output verified manually: 289,744 rows, multiset 0 missing / 0 extra`

### Verified Observations & Measurements:
- **Harness Status**: Failed sanity gate due to measurement execution crash: in `scripts/evaluation/run_evaluation.py:get_hdfs_physical_metrics`, `docker exec platform-backend python -c` exited with code 134 (SIGABRT: `terminate called without an active exception` during PyArrow C++ atexit interpreter teardown after successfully printing JSON). Subprocess check raised an exception which fell back to `physical_rows=0`.
- **Physical Output Verified via Direct/Fixed Inspection**:
  - Parquet Part Files: **304** files in `/data/output/pipe-eval-calibration-1792529256`
  - Physical Row Count: **289,744** (matches expected 289,744 exactly)
  - Multiset Comparison: **0 missing rows**, **0 extra rows**, **0 excess duplicates beyond baseline**
  - Unique Transaction IDs: **285,014** (4,730 baseline duplicates)
- **Run Timings & Resources**:
  - Total Duration: **221.22 s**
  - Active Processing Duration: **201.69 s** (slightly above the 150–200 s target range)
  - First Micro-Batch Delay: **19.53 s**
  - Host Free RAM: Minimum **2,906 MB** (started at 4,487 MB)
  - Host Hard Page Faults: Pages/sec 0.0–1.0 during steady stream (max 27.6 at startup)
- **Platform Tracker Counters (unreliable)**:
  - `recordsIn`: 300,000
  - `recordsOut`: 300,000 (from `executor.py:1228` copying input rows, untouched by design)
  - `rowsWritten`: `None` (verified: no longer falls back to fake 300,000 after A3(c) fix)
- **Artifacts Preserved**: Output path `/data/output/pipe-eval-calibration-1792529256` and raw report in `docs/evaluation/raw/calibration-a4cee60d/` preserved intact as evidence.

---

## CALIBRATION RUN RECORD: 7737791b (October 21)

**Run Identifier**: `eval-stream-calibration-1792532497`  
**Pipeline Run ID**: `7737791b-5efb-4c7a-8985-1dc0448388a1`  
**Execution Timestamp**: 2026-10-20 21:41:44 UTC to 21:45:57 UTC  
**Output Path**: `/data/output/pipe-eval-calibration-1792532497`  
**Status**: `PASSED (fully automated, exit 134 resolved, sanity gate passed)`  

### Verified Observations & Physical Ground Truth:
- **HDFS Sanity Gate**: **PASSED** (`run_valid`: `True`)
- **Parquet Part Files**: **304** files in `/data/output/pipe-eval-calibration-1792532497`
- **Physical Row Count**: **289,744** (matches expected 289,744 exactly)
- **Multiset Comparison**:
  - `missing_rows`: **0**
  - `extra_rows`: **0**
  - `duplicate_beyond_baseline`: **0**
  - `physical_data_loss`: **0**
- **Transaction ID Accounting**:
  - Distinct IDs: **285,014**
  - Baseline Duplicate Copies: **4,730**
  - Excess Duplicates: **0**
- **Timings & Telemetry**:
  - Total Duration: **253.23 s**
  - Active Processing Duration: **206.39 s** (target: 150–200 s)
  - First Micro-Batch Delay: **46.84 s**
  - Host Pre-flight: Free RAM 4,049.1 MB, Pages/sec 6.2 30s avg.
  - Telemetry during stream: Free RAM minimum 2,607.4 MB (started 4,143.6 MB).
- **Platform Tracker Counters (unreliable)**:
  - `recordsIn`: 300,000
  - `recordsOut`: 300,000 (copied from input by `executor.py:1228`)
  - `rowsWritten`: `None` (verified: no longer falls back to fake 300,000 after A3(c) fix)

---

## PILOT RUN RECORD: pilot-v2 (October 21) [PRE-FIX EVIDENCE]

> [!CAUTION]
> **INVALIDATION AS FAULT EVIDENCE**: The row counts and "data loss" numbers in pilot-v2 r1 and r2 must **NOT** be used as evidence of fault-induced data loss. None of this run's 300,000 records ever reached Spark (Spark was ingesting old records 0 to ~42,000 from prior runs due to topic reuse). All pilot runs executed on Oct 19–20 were contaminated by topic reuse without timestamps. Pilot-v2 is preserved strictly as **pre-fix evidence** documenting the topic isolation gap and the completion detector defect.

**Target**: `kafka` | **Fault**: `container_stop` | **Repetitions**: 2  
**Target Output Directory**: `docs/evaluation/raw/pilot-v2/`  
**Host Experimental Conditions**: 7 core containers only (`platform-backend`, `platform-postgres`, `platform-kafka`, `platform-spark-master`, `platform-spark-worker`, `platform-namenode`, `platform-datanode`). Auxiliary containers (`frontend`, `prometheus`, `grafana`, `airflow-*`) stopped.  

### Repetition 1: `eval-kafka-stop-r1`
- **Pipeline Run ID**: `c777c253-eb55-479c-8422-fd07f74ce6e8`  
- **Experiment Run ID**: `0dacabfb-25d0-415d-8d23-00125b50924d`  
- **Output Path**: `/data/output/pipe-eval-kafka-stop-r1-1792533072`  
- **First Micro-Batch Wait**: 21.38 s  
- **Platform Experiment Status**: `failed` (`host_memory_pressure`: `false`)  
- **Fault Recovery Time**: 13.31 s  
- **Availability During Fault**: 0.0% (-100% delta)  
- **Throughput Baseline**: 1,498.94 records/sec $\to$ 0.0 records/sec during fault (-100% degradation)  
- **Physical HDFS Measurement**: `MEASUREMENT_FAILED` (`run_valid`: `false`)
  - **Verified Cause**: During container stop, Spark structured streaming write was interrupted mid-task, leaving a 4-byte truncated Parquet file (`part-00001-1cf3358d-2b32-415b-8e16-a8cc2dcd217f-c000.snappy.parquet`, 4 bytes: header `PAR1` only, missing footer).
  - PyArrow failed with `ERROR: Parquet file size is 4 bytes, smaller than the minimum file footer (8 bytes)`.
  - Harness caught exception via `MeasurementError` and recorded `MEASUREMENT_FAILED` honestly without defaulting to 0.
- **Platform Validator (unreliable)**:
  - `expected_records`: 41,973
  - `processed_records`: 0 (platform validator also failed on the 4-byte parquet file)
  - `data_loss`: 41,973

### Repetition 2: `eval-kafka-stop-r2`
- **Pipeline Run ID**: `04e775f3-8d8e-4e2f-aebc-422d1f7cf980`  
- **Experiment Run ID**: `3faa85ab-6baf-4ffb-bca1-7f9d8a2256fe`  
- **Output Path**: `/data/output/pipe-eval-kafka-stop-r2-1792533218`  
- **First Micro-Batch Wait**: 23.47 s  
- **Platform Experiment Status**: `failed` (`host_memory_pressure`: `false`)  
- **Fault Recovery Time**: 11.04 s  
- **Availability During Fault**: 0.0% (-100% delta)  
- **Throughput Baseline**: 1,499.47 records/sec $\to$ 140.18 records/sec during fault (-90.65% degradation)  
- **Physical HDFS Ground Truth & Multiset Comparison**: `VALID` (`run_valid`: `true`)
  - **Parquet Part Files**: 42 files in `/data/output/pipe-eval-kafka-stop-r2-1792533218`
  - **Physical Rows Written**: **40,536**
  - **Expected Rows**: 289,744
  - **Physical Data Loss (`missing_rows`)**: **249,208**
  - **Extra Rows (`extra_rows`)**: **0**
  - **Excess Duplicates beyond Baseline**: **0**
  - **Distinct Transaction IDs**: 39,869
  - **Duplicate Copies**: 667 (exact match to input baseline duplicate rate)
  - **DISPROVED HYPOTHESIS & ROOT CAUSE**: An earlier hypothesis suggested that the Kafka producer thread encountered a broker disconnection and aborted after publishing only ~42,000 records. **This hypothesis has been thoroughly disproved by Kafka broker offset inspection.** The producer actually published all 300,000 records successfully (end offsets reached 600,000 from starting offset 300,000). The observed missing rows were caused entirely by topic reuse (topic held 600,000 cumulative records across runs; Spark started at offset 0 and only ingested old-run records 0 to ~42,000 before the `compute_lag(None) = 0` detector bug prematurely terminated the job).
  - **INVALIDATION NOTICE: NOT FAULT-INDUCED DATA LOSS**: The pilot-v2 r1 and r2 numbers must **NOT** be used as evidence of fault-induced data loss. None of this run's 300,000 records reached Spark before the job terminated. All pilot runs executed between October 19 and October 20 (`ba89b977`, `75eb98aa`, `c777c253`, `04e775f3`) were contaminated by topic reuse due to the lack of timestamp suffixes on topic names.
- **Platform Validator (unreliable)**:
  - `expected_records`: 41,986 (captured from a mid-stream `recordsOut` snapshot)
  - `processed_records`: 40,536
  - `data_loss`: 1,450 (vastly understates true data loss of 249,208 due to validator's reliance on snapshot counters)
- **Host Telemetry**:
  - Host Free RAM: 2,498.3 MB minimum (started 3,663.1 MB, pre-flight gate 3,860.9 MB)
  - Pages/sec: 0.0–0.99 during active stream (no memory pressure)




---

## PILOT-V2 ROOT CAUSE DIAGNOSIS & DEFECT VERIFICATION

**Status**: Verified Diagnosis (Read-Only Evidence Analysis)  
**Classification of pilot-v2**: Marked as **pre-fix evidence**. The runs exposed two critical system behaviors:
1. A confirmed platform defect in finite stream completion detection during broker outages.
2. An evaluation harness gap in topic isolation and uncommitted task-file filtering.

### 1. Topic Offset Accounting & Topic Reuse (VERIFIED)
- **Topic Names**: In pilot-v2, the evaluation harness generated topic names as `eval-stream-{exp_id}-r{rep_idx}` without timestamp suffixes (VERIFIED: `run_evaluation.py:840`).
- **Cumulative Record Counts**:
  - `eval-stream-kafka-stop-r1`: Reused across 3 runs (`0d930d18` Oct 19 10:36, `ba89b977` Oct 19 11:25, `c777c253` Oct 20 21:51). Sum of end offsets across 3 partitions = **900,000** (VERIFIED via Kafka `GetOffsetShell`).
  - `eval-stream-kafka-stop-r2`: Reused across 2 runs (`75eb98aa` Oct 20 12:22, `04e775f3` Oct 20 21:53). Sum of end offsets across 3 partitions = **600,000** (VERIFIED via Kafka `GetOffsetShell`).
- **Producer Output in THIS Run (VERIFIED)**:
  - Both r1 and r2 configured 300,000 rows from `/app/sample-data/sales_300k.csv`.
  - In r1 (`c777c253`): Initial topic end offset was 600,000; producer published all 300,000 records; topic end offset reached 900,000 (offset range for this run: 600,000–900,000).
  - In r2 (`04e775f3`): Initial topic end offset was 300,000; producer published all 300,000 records; topic end offset reached 600,000 (offset range for this run: 300,000–600,000).
  - Delivery callbacks and broker log persistence confirmed 300,000 records written in both runs. Stage A data loss (never published) = **0**.

### 2. Streaming Ingestion & Offset Mechanics (VERIFIED)
- Spark Structured Streaming was configured with `starting_offsets: "earliest"` (VERIFIED: `streaming_processing_job.py:186-189`).
- Because checkpoints were deleted between runs (`executor.py:1469`), Spark started ingestion from partition offset 0.
- Spark consumed offsets 0 to ~41,973 (r1) and 0 to ~41,986 (r2)—which corresponded to old records published during prior runs—before Kafka was stopped. None of the new 300,000 records published in this run were consumed before the job terminated.

### 3. Confirmed Platform Defect: `compute_lag(None) = 0` (VERIFIED)
- **Defect Location**: `backend/app/pipeline/stream_progress.py:107-108`:
  ```python
  if end_offsets is None:
      return 0  # DEFECT: Unknown lag treated as zero
  ```
- **Failure Mechanism**:
  1. During Kafka container stop, `get_topic_partition_end_offsets()` encounters a transport disconnection (`KafkaError{code=_TRANSPORT}`) and returns `None`.
  2. `compute_lag(None, ...)` returns `0` instead of `None`.
  3. `StreamingCompletionDetector.is_complete()` checks Condition 2: `current_lag == 0 and idle_time >= 20.0s`.
  4. While Kafka is stopped, no new rows are ingested (idle time accumulates). Because lag is falsely reported as 0 rather than unknown, the detector triggers completion after 20 seconds.
  5. The platform terminates the pipeline cleanly with status `succeeded`, falsely declaring the finite stream fully processed when only ~40,536 rows were committed.
- **Evaluation Significance**: This is a confirmed platform defect found directly by the evaluation. Finite stream completion must require a verified offset read; unknown lag (`None`) must never count as zero or trigger stream completion.

### 4. Loss Classification for THIS Run's Offset Range (VERIFIED)
Using the offset range belonging to this run (r1: 600,000–900,000; r2: 300,000–600,000):
- **Stage A (Never published)**: **0** (All 300,000 records reached Kafka).
- **Stage B (Published, not consumed)**: **300,000** (All 300,000 records remained in Kafka unconsumed when the job prematurely terminated).
- **Stage C (Consumed, not committed)**: **0** (No records from this run reached the executor; the 1,450 uncommitted rows in r1 were from the earlier run's aborted batch).

### 5. Resolution of r1 Physical Measurement Failure (VERIFIED)
- In r1, an aborted Spark write task left an uncommitted 4-byte stub file (`part-00001-1cf3358d-...`, header `PAR1` only).
- Verified from `_spark_metadata/` commit logs that this file was never committed (0 occurrences in metadata).
- Harness updated to inspect only files listed as `action="add"` in `_spark_metadata` commit logs.
- Physical rows from committed files:
  - **r1**: 42 committed files = **40,523** rows (VERIFIED)
  - **r2**: 42 committed files = **40,536** rows (VERIFIED)

### 6. Throughput During Fault Window (HYPOTHESIS)
- The observed ~140 records/sec throughput during the fault window in r2 is hypothesized to result from either:
  1. Spark Kafka consumer prefetch buffer: Records fetched into memory before the broker socket severed were processed in the subsequent micro-batch.
  2. Progress counter artifact: Tracker counters deriving rates from intermediate progress snapshots.

---

## PILOT RUN RECORD: pilot-v3 (October 21) [POST-FIX EVALUATION]

**Target**: `kafka` | **Fault**: `container_stop` (20 s duration) | **Repetitions**: 2  
**Target Output Directory**: `docs/evaluation/raw/pilot-v3/`  
**Host Experimental Conditions**: 7 core containers only (`platform-backend`, `platform-postgres`, `platform-kafka`, `platform-spark-master`, `platform-spark-worker`, `platform-namenode`, `platform-datanode`). Auxiliary containers (`frontend`, `prometheus`, `grafana`, `airflow-*`) stopped.  
**Applied Fixes Active During Run**:
1. **Topic Isolation**: Timestamped topic names (`eval-stream-kafka-stop-r{rep}-{timestamp}`) with empty-topic assertion (`get_kafka_topic_total_records == 0`).
2. **Metadata Filtering**: Counting physical rows strictly from committed files listed in `_spark_metadata` commit logs, ignoring aborted uncommitted task stubs.
3. **Finite Stream Completion Fix (Q4)**: `compute_lag(None)` returns `None`, and `StreamingCompletionDetector.is_complete()` requires valid non-None lag (unreachable broker prevents premature idle termination).

---

### Repetition 1: `eval-kafka-stop-r1`

- **Run Classification**: **`VALID`** (VERIFIED: `physical_metrics.json:11-12`)
- **Pipeline Run ID**: `16de5509-9d65-47c9-bcfa-6ea49d810a66` (VERIFIED: `environment.json:8`)
- **Experiment ID**: `ab977705-c898-465f-b010-517e084771f3` | **Run ID**: `9f0ef62f-6010-452a-abf2-16d400ecc3e7` (VERIFIED: `report.json:3-4`)
- **Topic Name**: `eval-stream-kafka-stop-r1-1792537901` (VERIFIED: `environment.json:7`)
- **Output Path**: `/data/output/pipe-eval-kafka-stop-r1-1792537912` (VERIFIED: `environment.json:9`)
- **First Micro-Batch Delay**: 28.38 s (VERIFIED: `environment.json:38`)
- **Topic Offsets for THIS Run** (VERIFIED via Kafka `kafka-get-offsets` & `pipeline_run_events`):
  - Partition 0: Begin = 0, End = 112,109
  - Partition 1: Begin = 0, End = 93,500
  - Partition 2: Begin = 0, End = 94,391
  - **Total Published to Kafka**: **300,000** records (`kafka_done` event payload: `published=300000`, `failed=0`)
- **Records Consumed by Spark**: **300,000** records (`spark.recordsIn = 300,000`, 100.0% of published records consumed) (VERIFIED: `pipeline_runs.last_progress_snapshot`)
- **Physical Output & Multiset Ground Truth** (VERIFIED: `physical_metrics.json:3-20`):
  - Committed Parquet Files: **303** files (all tracked in `_spark_metadata`)
  - **Physical Rows Written**: **289,744** (matches expected 289,744 exactly)
  - **Missing Rows (`missing_rows`)**: **0** (Zero physical data loss)
  - **Extra Rows (`extra_rows`)**: **0**
  - **Excess Duplicates beyond Baseline**: **0**
  - Distinct Transaction IDs: 285,014
  - Baseline Duplicate Copies: 4,730 (exact match to input CSV duplicate rate)
- **Fault Dynamics & Timings** (VERIFIED: `report.json:20-44`):
  - Fault Injected: `2026-10-20T23:12:51.792595+00:00` (Kafka stopped)
  - Fault Reverted: `2026-10-20T23:13:11.834248+00:00` (Docker start issued after 20 s)
  - Service Recovered: `2026-10-20T23:13:21.892533+00:00`
  - **Recovery Time**: **10.06 s**
  - Availability During Fault: 0.0% (-100.0% delta)
- **Throughput Profile** (VERIFIED: `report.json:36-39, 79-85`):
  - Normal Baseline: **1,499.46 records/sec**
  - During Fault Window: **0.0 records/sec** (-100.0% degradation)
  - Post-Fault Average: **1,216.35 records/sec**
- **Pipeline Active Status** (VERIFIED):
  - Active at Injection: **`true`** (`report.json:98-99`)
  - Active at Recovery: **`true`** (Kafka recovered at 23:13:21 UTC; streaming job continued processing batches until 23:16:18 UTC)
- **Platform Status vs. Physical Result Disagreement** (VERIFIED):
  - Physical Ground Truth: **100% Data Integrity (0 missing, 0 excess duplicates, VALID)**.
  - Platform Final Status: **`failed`** (`data_loss = 10,256 > expected 0`).
  - **Verified Cause**: In `backend/app/reliability/data_consistency_validator.py:166-180`, the platform validator computes data loss by subtracting HDFS row count (289,744) from the raw input CSV row count (300,000). The platform validator is completely unaware of the pipeline's configured `remove_nulls` filter operation (`amount IS NOT NULL AND amount > 0`), which legitimately filters out 10,256 rows with null or negative amounts.
- **Host Telemetry & Memory Pressure** (VERIFIED: `environment.json:43-45`):
  - Free RAM Start: 3,353.0 MB | End: 2,827.9 MB | Minimum: **1,739.4 MB**
  - Pages/sec: 0.0–33.5 steady state; spike of 2,479.6 at t+16s.
  - `host_memory_pressure`: `true` (due to 2 consecutive sampling intervals with Pages/sec > 100).
- **Spark Driver Stderr Tail** (VERIFIED: `pipeline_run_events`):
  - 1 harmless batch warning: `ProcessingTimeExecutor: Current batch is falling behind. The trigger interval is 2000 milliseconds, but spent 2700 milliseconds`. Zero errors.

---

### Repetition 2: `eval-kafka-stop-r2`

- **Run Classification**: **`VALID`** (VERIFIED: `physical_metrics.json:11-12`)
- **Pipeline Run ID**: `e74d9029-9769-48a9-abf1-5864db12f21b` (VERIFIED: `environment.json:8`)
- **Experiment ID**: `365b7426-992c-4f0f-973f-8f5cb73ee5b6` | **Run ID**: `7c4ef4c5-87c7-4f90-8341-1cd60a70e8cc` (VERIFIED: `report.json:3-4`)
- **Topic Name**: `eval-stream-kafka-stop-r2-1792538254` (VERIFIED: `environment.json:7`)
- **Output Path**: `/data/output/pipe-eval-kafka-stop-r2-1792538260` (VERIFIED: `environment.json:9`)
- **First Micro-Batch Delay**: 22.97 s (VERIFIED: `environment.json:38`)
- **Topic Offsets for THIS Run** (VERIFIED via Kafka `kafka-get-offsets` & `pipeline_run_events`):
  - Partition 0: Begin = 0, End = 100,187
  - Partition 1: Begin = 0, End = 100,773
  - Partition 2: Begin = 0, End = 99,040
  - **Total Published to Kafka**: **300,000** records (`kafka_done` event payload: `published=300000`, `failed=0`)
- **Records Consumed by Spark**: **300,000** records (`spark.recordsIn = 300,000`, 100.0% of published records consumed) (VERIFIED: `pipeline_runs.last_progress_snapshot`)
- **Physical Output & Multiset Ground Truth** (VERIFIED: `physical_metrics.json:3-20`):
  - Committed Parquet Files: **304** files (all tracked in `_spark_metadata`)
  - **Physical Rows Written**: **289,744** (matches expected 289,744 exactly)
  - **Missing Rows (`missing_rows`)**: **0** (Zero physical data loss)
  - **Extra Rows (`extra_rows`)**: **0**
  - **Excess Duplicates beyond Baseline**: **0**
  - Distinct Transaction IDs: 285,014
  - Baseline Duplicate Copies: 4,730 (exact match to input CSV duplicate rate)
- **Fault Dynamics & Timings** (VERIFIED: `report.json:20-44`):
  - Fault Injected: `2026-10-20T23:18:31.792720+00:00` (Kafka stopped)
  - Fault Reverted: `2026-10-20T23:18:51.836238+00:00` (Docker start issued after 20 s)
  - Service Recovered: `2026-10-20T23:19:04.571400+00:00`
  - **Recovery Time**: **12.74 s**
  - Availability During Fault: 0.0% (-100.0% delta)
- **Throughput Profile** (VERIFIED: `report.json:36-39, 79-85`):
  - Normal Baseline: **1,498.90 records/sec**
  - During Fault Window: **139.17 records/sec** (-90.72% degradation)
  - Post-Fault Average: **1,190.44 records/sec**
  - **Mechanism for 139.17 records/sec Throughput During Fault Window** (HYPOTHESIS): Micro-batch in-flight during container stop completed its sink write before the client socket severed, contributing rows to the first interval of the 20s fault window before throughput dropped to 0.
- **Pipeline Active Status** (VERIFIED):
  - Active at Injection: **`true`** (`report.json:98-99`)
  - Active at Recovery: **`true`** (Kafka recovered at 23:19:04 UTC; streaming job continued processing batches until 23:22:00 UTC)
- **Platform Status vs. Physical Result Disagreement** (VERIFIED):
  - Physical Ground Truth: **100% Data Integrity (0 missing, 0 excess duplicates, VALID)**.
  - Platform Final Status: **`failed`** (`data_loss = 10,256 > expected 0`).
  - **Verified Cause**: Unawareness of `remove_nulls` filter operation in `data_consistency_validator.py`.
- **Host Telemetry & Memory Pressure** (VERIFIED: `environment.json:43-45`):
  - Free RAM Start: 3,153.6 MB | End: 2,593.2 MB | Minimum: **1,832.4 MB**
  - Pages/sec: 0.0–3.0 steady state; brief spikes to 306.3 at t+114s and 104.2 at t+130s.
  - `host_memory_pressure`: `true` (due to 2 consecutive sampling intervals with Pages/sec > 100).
- **Spark Driver Stderr Tail** (VERIFIED: `pipeline_run_events`):
  - 0 warning lines, 0 error lines. Zero errors.
