# Gap 3: Normal vs During-Fault Comparison Table

Comparison of steady-state normal baseline performance against during-fault degradation and recovery characteristics across all 5 fault experiments (5 repetitions each, N=25 total).

| Experiment name | Target container | Normal throughput (from Control / baseline window) | During-fault throughput (records/sec) | Throughput degradation (%) | Recovery time (mean +/- std) | Data loss: missing rows (mean), extra rows (mean), duplicates beyond baseline (mean) | Platform validator data loss (labeled "unreliable, uses tracker counters") |
| --- | --- | --- | --- | --- | --- | --- | --- |
| kafka-stop | platform-kafka | 1472.0 rec/s | 107.20 | 92.7% | 17.52 ± 5.73 s | 0.0 / 0.0 / 0.0 | 10256.0 (unreliable) |
| kafka-restart | platform-kafka | 1472.0 rec/s | 524.69 | 64.4% | 0.72 ± 1.40 s | 0.0 / 0.0 / 0.0 | 10256.0 (unreliable) |
| spark-worker-stop | platform-spark-worker | 1472.0 rec/s | 571.91 | 61.1% | 0.48 ± 0.13 s | 0.0 / 0.0 / 0.0 | 10256.0 (unreliable) |
| spark-worker-restart | platform-spark-worker | 1472.0 rec/s | 960.73 | 34.7% | 0.12 ± 0.07 s | 0.0 / 0.0 / 0.0 | 10070.6 (unreliable) |
| datanode-stop | platform-datanode | 1472.0 rec/s | 295.34 | 79.9% | 0.44 ± 0.07 s | 93900.6 / 0.0 / 0.0 | 6942.8 (unreliable) |

> [!NOTE]
> - **Normal throughput**: Determined from the Control Baseline (calibration) steady-state streaming ingestion rate (1,472.0 rec/s).
> - **Physical Data Loss**: Counted strictly from committed files in `_spark_metadata` verified against the input CSV multiset (`missing / extra / excess_duplicates`).
> - **Platform Validator**: Uses unadjusted tracker counters comparing processed rows against raw 300,000 records without filtering (labeled unreliable).
> - **Storage Failure Mode**: `datanode-stop` reflects single-replica HDFS architecture where DataNode failure causes block write failures in 2 of 5 runs.
