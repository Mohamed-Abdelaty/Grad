# Gap 3: Normal vs During-Fault Comparison Analysis

## 1. Primary Analysis: Fault Runs vs. Empirical Control & Individual Pre-Fault Baselines

Each fault experiment is evaluated against two complementary baselines:
1. **Empirical Control Baseline (1,678.5 ± 163.9 rec/s)**: Derived from 5 identical, unperturbed control runs (`eval-control-r1` to `r5`) executed under the exact same pipeline configuration, timing windows (10s baseline, 20s fault window, 10s post), and environment gates.
2. **Individual Pre-Fault Baseline**: Captured from the active pipeline tracker during the 10-second baseline window immediately preceding fault injection for that specific run.

| Experiment Name | Target Container | Control Baseline (rec/s) | Pre-Fault Baseline (rec/s) | During-Fault Throughput (rec/s) | Degradation vs Control (%) | Degradation vs Own Baseline (%) | Container Recovery (mean ± std) | Throughput 90% Recovery (mean ± std) | Physical Data Loss (Missing/Extra/Excess) | Platform Validator Loss (Unreliable) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Control (5-repetition empirical baseline, unperturbed) | None | 1678.5 ± 163.9 | 1678.5 ± 163.9 | 1498.0 ± 105.8 | 9.7% | 9.7% | 0.00 s | 0.00 s | 0.0 / 0.0 / 0.0 | 10256.0 (unreliable) |
| kafka-stop | platform-kafka | 1678.5 ± 163.9 | 1558.0 ± 131.4 | 107.20 | 93.6% | 93.2% | 17.52 ± 5.73 s | 22.89 ± 6.47 s | 0.0 / 0.0 / 0.0 | 10256.0 (unreliable) |
| kafka-restart | platform-kafka | 1678.5 ± 163.9 | 1439.2 ± 134.0 | 524.69 | 68.7% | 65.0% | 0.72 ± 1.40 s | 3.44 ± 2.57 s | 0.0 / 0.0 / 0.0 | 10256.0 (unreliable) |
| spark-worker-stop | platform-spark-worker | 1678.5 ± 163.9 | 1679.1 ± 164.2 | 571.91 | 65.9% | 65.8% | 0.48 ± 0.13 s | 17.56 ± 5.50 s | 0.0 / 0.0 / 0.0 | 10256.0 (unreliable) |
| spark-worker-restart | platform-spark-worker | 1678.5 ± 163.9 | 1439.2 ± 250.8 | 960.73 | 42.8% | 32.8% | 0.12 ± 0.07 s | 1.87 ± 1.21 s | 0.0 / 0.0 / 0.0 | 10070.6 (unreliable) |
| datanode-stop | platform-datanode | 1678.5 ± 163.9 | 1379.3 ± 164.4 | 295.34 | 82.4% | 80.3% | 0.47 ± 0.08 s (3 passed) / N/A (2 failed) | 21.69 ± 2.10 s (3 passed) / N/A (2 failed) | Failed in 2 of 5 runs (query died at ~55k rows; 0 loss in 3 runs) | 6942.8 (unreliable) |

> [!NOTE]
> - **Control Baseline**: Mean throughput over the 10s baseline window across the 5 empirical control runs ($1,678.5 \pm 163.9$ rec/s). In the unperturbed simulated fault window (20s), control throughput was $1,498.1 \pm 105.8$ rec/s ($9.7\%$ natural micro-batch timing variation).
> - **Pre-Fault Baseline**: Captured from the active pipeline tracker during the 10-second `baseline` phase immediately preceding fault injection.
> - **Container Recovery**: Time from fault rollback until Docker container health check passes (`docker_controller.is_container_running` + connectivity).
> - **Throughput 90% Recovery**: Time from fault rollback until a newly completed micro-batch restores ingestion rate to $\ge 90\%$ of the pre-fault baseline.
> - **Storage Failure Mode**: `datanode-stop` failed in 2 of 5 runs due to `dfs.replication=1` deployment setting causing NameNode write rejection. For failed runs, recovery time is Not Applicable (N/A) because the query terminated.

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
