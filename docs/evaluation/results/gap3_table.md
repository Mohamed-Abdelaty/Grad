# Gap 3: Normal vs During-Fault Comparison Analysis

## 1. Primary Analysis: Fault Runs vs. Individual Pre-Fault Baselines

Each fault experiment was monitored during its own 10-second pre-fault operational baseline window immediately prior to fault injection. This table evaluates during-fault degradation and recovery dynamics against that pre-fault baseline.

| Experiment Name | Target Container | Pre-Fault Baseline (rec/s) | During-Fault Throughput (rec/s) | Throughput Degradation (%) | Container Recovery (mean ± std) | Throughput 90% Recovery (mean ± std) | Physical Data Loss (Missing/Extra/Excess) | Platform Validator Loss (Unreliable) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| kafka-stop | platform-kafka | 1558.0 ± 131.4 | 107.20 | 93.2% | 17.52 ± 5.73 s | 22.89 ± 6.47 s | 0.0 / 0.0 / 0.0 | 10256.0 (unreliable) |
| kafka-restart | platform-kafka | 1439.2 ± 134.0 | 524.69 | 65.0% | 0.72 ± 1.40 s | 3.44 ± 2.57 s | 0.0 / 0.0 / 0.0 | 10256.0 (unreliable) |
| spark-worker-stop | platform-spark-worker | 1679.1 ± 164.2 | 571.91 | 65.8% | 0.48 ± 0.13 s | 17.56 ± 5.50 s | 0.0 / 0.0 / 0.0 | 10256.0 (unreliable) |
| spark-worker-restart | platform-spark-worker | 1439.2 ± 250.8 | 960.73 | 32.8% | 0.12 ± 0.07 s | 1.87 ± 1.21 s | 0.0 / 0.0 / 0.0 | 10070.6 (unreliable) |
| datanode-stop | platform-datanode | 1379.3 ± 164.4 | 295.34 | 80.3% | 0.47 ± 0.08 s (3 passed) / N/A (2 failed) | 21.69 ± 2.10 s (3 passed) / N/A (2 failed) | Failed in 2 of 5 runs (query died at ~55k rows; 0 loss in 3 runs) | 6942.8 (unreliable) |

> [!NOTE]
> - **Pre-Fault Baseline**: Captured from the active pipeline tracker during the 10-second `baseline` phase immediately preceding fault injection.
> - **Container Recovery**: Time from fault rollback until Docker container health check passes (`docker_controller.is_container_running` + connectivity).
> - **Throughput 90% Recovery**: Time from fault rollback until a newly completed micro-batch restores ingestion rate to $\ge 90\%$ of the pre-fault baseline.
> - **Storage Failure Mode**: `datanode-stop` failed in 2 of 5 runs due to `dfs.replication=1` deployment setting causing NameNode write rejection. For failed runs, recovery time is Not Applicable (N/A) because the query terminated.

---

## 2. Superseded Baseline Table (Calibrated against 1 Whole-Run Average)

*(Marked as SUPERSEDED: Uses single calibration run overall average of 1,472.0 rec/s across warm-up and steady state)*

| Experiment name | Target container | Normal throughput (from Control / baseline window) | During-fault throughput (records/sec) | Throughput degradation (%) | Recovery time (mean +/- std) | Data loss: missing rows (mean), extra rows (mean), duplicates beyond baseline (mean) | Platform validator data loss (labeled "unreliable, uses tracker counters") |
| --- | --- | --- | --- | --- | --- | --- | --- |
| kafka-stop | platform-kafka | 1472.0 rec/s | 107.20 | 92.7% | 17.52 ± 5.73 s | 0.0 / 0.0 / 0.0 | 10256.0 (unreliable) |
| kafka-restart | platform-kafka | 1472.0 rec/s | 524.69 | 64.4% | 0.72 ± 1.40 s | 0.0 / 0.0 / 0.0 | 10256.0 (unreliable) |
| spark-worker-stop | platform-spark-worker | 1472.0 rec/s | 571.91 | 61.1% | 0.48 ± 0.13 s | 0.0 / 0.0 / 0.0 | 10256.0 (unreliable) |
| spark-worker-restart | platform-spark-worker | 1472.0 rec/s | 960.73 | 34.7% | 0.12 ± 0.07 s | 0.0 / 0.0 / 0.0 | 10070.6 (unreliable) |
| datanode-stop | platform-datanode | 1472.0 rec/s | 295.34 | 79.9% | 0.47 ± 0.08 s (surviving); N/A (2 failed) | Failed in 2 of 5 runs (storage write error; 0 loss in 3 runs) | 6942.8 (unreliable) |
