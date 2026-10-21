# Gap 3: Normal vs During-Fault Comparison Analysis

## 1. Primary Analysis: Fault Runs vs. Post-Warmup Baseline & Empirical Control

Each fault experiment is evaluated against its unquantized post-warmup operational baseline and compared against the 5-repetition empirical control campaign:
1. **Post-Warmup Baseline [Primary]**: Computed over all completed micro-batches from the end of warm-up until fault injection, eliminating discrete 10-second window batch boundary quantization artifacts.
2. **10s Window Baseline [Secondary]**: Captured during the 10-second baseline window immediately preceding fault injection.
3. **Empirical Control Baseline (1,784.8 ± 180.7 rec/s post-warmup, 1,678.5 ± 163.9 rec/s 10s-window)**: Derived from 5 unperturbed control runs (`eval-control-r1` to `r5`) executed under identical configuration and environment gates. Baseline coefficient of variation (CV) is 9.8% ($163.9 / 1,678.5$). In the simulated fault window, control throughput dropped 10.8% ($1,678.5 \to 1,498.0 \pm 105.8$ rec/s) due to micro-batch alignment variation.

| Experiment Name | Target Container | Post-Warmup Baseline (rec/s) [Primary] | 10s Window Baseline (rec/s) [Secondary] | During-Fault Throughput (rec/s) | Degradation vs Post-Warmup Baseline (%) | Degradation vs Control (%) | Container Recovery (mean ± std) | Throughput 90% Recovery (mean ± std) | Physical Data Loss (Missing/Extra/Excess) | Platform Validator Output | Valid Runs (n/N) | Query Survived (n/N) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Control (5-repetition empirical baseline, unperturbed) | None | 1784.6 ± 183.6 | 1678.5 ± 163.9 | 1498.0 ± 105.8 (unperturbed) | Within control variation (CV: 9.8%, 10.8% drop to simulated fault window) | Baseline reference (0%) | 0.00 s | 0.00 s | 0.0 / 0.0 / 0.0 | 10256.0 (filter-unaware comparison, 0 physical loss) | 5 / 5 | 5 / 5 |
| kafka-stop | platform-kafka | 1488.8 ± 140.8 | 1558.0 ± 131.4 | 107.20 (window incl. in-flight batch) / 0.00 (true halted) | 92.8% (window) / 100% (halted) | 92.8% (window) / 100% (halted) | 17.52 ± 5.73 s | 22.89 ± 6.47 s | 0.0 / 0.0 / 0.0 | 10256.0 (unreliable) | 5 / 5 | 5 / 5 |
| kafka-restart | platform-kafka | 1591.7 ± 37.7 | 1439.2 ± 134.0 | 524.69 (transient ~3s restart) | 67.0% | 65.0% | 0.72 ± 1.40 s | 3.44 ± 2.57 s | 0.0 / 0.0 / 0.0 | 10256.0 (unreliable) | 5 / 5 | 5 / 5 |
| spark-worker-stop | platform-spark-worker | 1620.2 ± 50.3 | 1679.1 ± 164.2 | 571.91 (window incl. pre-halt drain) / 0.00 (true halted) | 64.7% (window) / 100% (halted) | 61.8% (window) / 100% (halted) | 0.48 ± 0.13 s | 17.56 ± 5.50 s | 0.0 / 0.0 / 0.0 | 10256.0 (unreliable) | 5 / 5 | 5 / 5 |
| spark-worker-restart | platform-spark-worker | 1651.6 ± 37.7 | 1439.2 ± 250.8 | 960.73 (transient ~2s restart) | 41.8% | 35.9% (transient ~2s restart) | 0.12 ± 0.07 s | 1.87 ± 1.21 s | 0.0 / 0.0 / 0.0 | 10070.6 (unreliable) | 5 / 5 | 5 / 5 |
| datanode-stop | platform-datanode | 1541.2 ± 54.1 | 1379.3 ± 164.4 | 295.34 (window incl. pre-halt drain) / 0.00 (true halted) | 80.8% (surviving window) / 100% (halted/crashed) | 80.3% (surviving window) / 100% (halted/crashed) | 0.47 ± 0.08 s (3 passed) / N/A (2 failed) | 21.69 ± 2.10 s (3 passed) / N/A (2 failed) | Failed in 2 of 5 runs (query died at ~55k rows; 0 loss in 3 runs) | 6942.8 (unreliable) | 5 / 5 | 3 / 5 |

> [!NOTE]
> - **Post-Warmup Baseline [Primary]**: Steady-state ingestion rate computed across all micro-batches between warm-up completion and fault injection.
> - **10s Window Baseline [Secondary]**: Traditional 10s operational sampling immediately preceding fault injection.
> - **During-Fault Throughput**: For container stop faults (`kafka-stop`, `spark-worker-stop`, `datanode-stop`), the first figure reports the 20s window delta (which includes graceful container termination drain and in-flight micro-batch completion), while `0.00 rec/s` reflects the true throughput while the container was completely halted.
> - **Degradation vs Control**: Relative throughput drop compared against the Control simulated fault window throughput (1,498.0 rec/s). Baseline CV is 9.8% (163.9 / 1,678.5), with a 10.8% natural micro-batch alignment drop between measurement windows in the unperturbed control.
> - **Container Recovery**: Time from fault rollback until Docker container health check passes (`docker_controller.is_container_running` + connectivity).
> - **Throughput 90% Recovery**: Time from fault rollback until a newly completed micro-batch restores ingestion rate to $\ge 90\%$ of baseline.
> - **Storage Failure Mode**: `datanode-stop` failed in 2 of 5 runs due to `dfs.replication=1` deployment setting causing NameNode write rejection. For failed runs, recovery time is Not Applicable (N/A) because the query terminated.
> - **Platform Validator Output**: The 10,256 row discrepancy in the platform validator is a known filter-unaware comparison defect ($300,000 - 289,744 = 10,256$), not physical data loss.

---

## 2. Superseded Baseline Table (Calibrated against 1 Whole-Run Average)

*(Marked as SUPERSEDED: Uses single calibration run overall average of 1,472.0 rec/s across warm-up and steady state)*

| Experiment name | Target container | Normal throughput (from Control / baseline window) | During-fault throughput (records/sec) | Throughput degradation (%) | Recovery time (mean +/- std) | Data loss: missing rows (mean), extra rows (mean), duplicates beyond baseline (mean) | Platform validator data loss (labeled "unreliable, uses tracker counters") |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Control (whole-run calibration average, not steady) | None | 1472.0 rec/s | — | 0.0% | 0.00 s | 0.0 / 0.0 / 0.0 | 10256.0 (unreliable) |
| kafka-stop | platform-kafka | 1472.0 rec/s | 107.20 | 92.7% | 17.52 ± 5.73 s | 0.0 / 0.0 / 0.0 | 10256.0 (unreliable) |
| kafka-restart | platform-kafka | 1472.0 rec/s | 524.69 | 64.4% | 0.72 ± 1.40 s | 0.0 / 0.0 / 0.0 | 10256.0 (unreliable) |
| spark-worker-stop | platform-spark-worker | 1472.0 rec/s | 571.91 | 61.1% | 0.48 ± 0.13 s | 0.0 / 0.0 / 0.0 | 10256.0 (unreliable) |
| spark-worker-restart | platform-spark-worker | 1472.0 rec/s | 960.73 | 34.7% | 0.12 ± 0.07 s | 0.0 / 0.0 / 0.0 | 10070.6 (unreliable) |
| datanode-stop | platform-datanode | 1472.0 rec/s | 295.34 | 79.9% | 0.47 ± 0.08 s (surviving); N/A (2 failed) | Failed in 2 of 5 runs (storage write error; 0 loss in 3 runs) | 6942.8 (unreliable) |
