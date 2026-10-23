# Gap 3: Normal vs During-Fault Resilience Comparison Table

Deterministic aggregate metrics across 5 fault scenarios and empirical control baseline.
Evaluates system degradation, physical data integrity (5-column multiset equality), and dual recovery metrics.

<!-- BEGIN GENERATED:gap3_primary_table -->
| Experiment Name | Target Container | Post-Warmup Baseline (rec/s) [Primary] | 10s Window Baseline (rec/s) [Secondary] | During-Fault Throughput (rec/s) | Degradation vs Post-Warmup Baseline (%) | Degradation vs Control (%) | Container Recovery (mean ± std) | Throughput 90% Recovery (mean ± std) | Physical Data Loss (Missing/Extra/Excess) | Platform Validator Output | Executed (n/N) | Query Survived (n/N) | Data-Complete (n/N) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Control** (5-repetition empirical baseline, unperturbed) | None | 1784.6 ± 183.6 | 1678.5 ± 163.9 | 1498.1 ± 105.8 (unperturbed) | 16.1% drop to simulated window (-10.8% vs 10s baseline) | Baseline reference (0%) | 0.00 s | 0.00 s | 0.0 / 0.0 / 0.0 | not run (validator is fault-run only) | 5 / 5 | 5 / 5 | 5 / 5 |
| **kafka-stop** | platform-kafka | 1488.8 ± 140.7 | 1558.1 ± 131.4 | 107.20 (window incl. in-flight batch) / 0.00 (true halted) | 92.8% (window) / 100% (halted) | 92.8% (window) / 100% (halted) | 17.52 ± 5.73 s | 22.89 ± 6.47 s | 0.0 / 0.0 / 0.0 | 10256.0 (unreliable) | 5 / 5 | 5 / 5 | 5 / 5 |
| **kafka-restart** | platform-kafka | 1591.7 ± 37.7 | 1439.2 ± 134.0 | 524.69 (transient ~3s restart) | 67.0% | 65.0% | 0.72 ± 1.40 s | 3.44 ± 2.57 s | 0.0 / 0.0 / 0.0 | 10256.0 (unreliable) | 5 / 5 | 5 / 5 | 5 / 5 |
| **spark-worker-stop** | platform-spark-worker | 1620.2 ± 50.3 | 1679.1 ± 164.3 | 571.91 (window incl. pre-halt drain) / 0.00 (true halted) | 64.7% (window) / 100% (halted) | 61.8% (window) / 100% (halted) | 0.48 ± 0.13 s | 17.56 ± 5.50 s | 0.0 / 0.0 / 0.0 | 10256.0 (unreliable) | 5 / 5 | 5 / 5 | 5 / 5 |
| **spark-worker-restart** | platform-spark-worker | 1651.6 ± 37.7 | 1439.2 ± 250.8 | 960.73 (transient ~2s restart) | 41.8% | 35.9% (transient ~2s restart) | 0.12 ± 0.07 s | 1.87 ± 1.21 s | 0.0 / 0.0 / 0.0 | 10070.6 ± 414.6 (unreliable) | 5 / 5 | 5 / 5 | 5 / 5 |
| **datanode-stop** | platform-datanode | 1541.1 ± 54.1 | 1379.3 ± 164.4 | 492.23 (surviving window) / 295.34 (all 5 runs) / 0.00 (true halted) | 67.9% (surviving window) / 80.8% (all 5 runs) / 100% (halted/crashed) | 67.1% (surviving window) / 80.3% (all 5 runs) / 100% (halted/crashed) | 0.47 ± 0.08 s (3 passed) / N/A (2 failed) | 21.69 ± 2.10 s (3 passed) / N/A (2 failed) | Failed in 2 of 5 runs (query died at ~55k rows; 0 loss in 3 runs) | 6942.8 ± 4536.8 (unreliable) | 5 / 5 | 3 / 5 | 3 / 5 |
<!-- END GENERATED:gap3_primary_table -->

### Statistical and Metric Definitions
1. **Primary Baseline**: Post-warmup unquantized throughput measured from completion of first micro-batch (warmup cutoff batch id: 0) to fault injection start (mean: 1,488.8 – 1,784.6 rec/s).
2. **Secondary Baseline**: 10-second window baseline immediately preceding fault injection (mean: 1,379.3 – 1,679.1 rec/s).
3. **During-Fault Throughput (Stop Faults)**: Dual reporting:
   - *Window Throughput*: Cumulative delta divided by 20.0s window length (includes ~10s in-flight task drain prior to container halt).
   - *True Halted Throughput*: 0.00 rec/s across all stop faults after container process termination.
4. **Degradation Formula (Ratio of Means)**: Uniformly computed using ratio of means for all rows:
   $$\text{Degradation} = \left(1 - \frac{\text{mean}(\text{Throughput}_{\text{during-fault}})}{\text{mean}(\text{Baseline Throughput})}\right) \times 100$$
   Against Control, the uniform empirical reference is 1,498.1 rec/s (mean of 5 unperturbed control runs: 1498.052 -> 1498.1).
   *Baseline Comparability Note*: Control post-warmup baseline was measured over 10.0s (capturing early post-warmup burst of ~20,000 records at ~1,980 rec/s), whereas fault runs used ~23s steady-state baselines (~36,000 records at ~1,560 rec/s). Consequently, the control throughput during the simulated window (1,498.1 rec/s) is 16.1% below its own post-warmup baseline and 10.8% below its 10s baseline.
5. **Dual Recovery Metrics**:
   - *Container Recovery*: Elapsed time from fault revert command until container health check passes.
   - *Throughput 90% Recovery*: Time required for pipeline streaming throughput to return to >= 90% of pre-fault baseline.
6. **Physical Data Loss**: Full-row 5-column multiset equality `(transaction_id, customer_id, product_id, amount, timestamp)` against the expected 289,744 post-filter records. Reported as `Missing / Extra / Excess Duplicates`.
7. **Platform Validator Output**: Platform UI validator output. Unreliable due to filter-unaware comparison (300,000 raw input vs 289,744 post-filter output = 10,256 constant delta). Control runs did not run the validator (not run).
8. **Survival & Completeness**:
   - *Executed (n/N)*: Runs completed through test harness (5/5 for all).
   - *Query Survived (n/N)*: Structured Streaming query remained active without terminal exception (5/5 for Kafka and Spark Worker; 3/5 for DataNode under `dfs.replication=1`).
   - *Data-Complete (n/N)*: Stream committed all 289,744 expected rows with 0 loss (5/5 for Kafka and Spark Worker; 3/5 for DataNode).
