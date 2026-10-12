# Live Progress Signals Architecture and Field Mapping

This document specifies the telemetry signal sources, measurement mechanisms, update frequencies, nullability contracts, and operational limits of the ChaosFlow pipeline progress tracker.

---

## 1. Field Source and Telemetry Mapping

The table below details every field in the `RunProgress` telemetry snapshot served by `GET /api/pipeline-runs/{id}/progress` and SSE `event: progress` on `GET /api/pipeline-runs/{id}/stream`.

| Field | Real Source (File / Function) | When It Is `null` | Update Frequency |
|---|---|---|---|
| `runId` | `RunProgressTracker.start` (`app/pipeline/progress.py`) | Never | Set at run start |
| `status` | `RunProgressTracker.emit` (`app/pipeline/progress.py`) | Never | On state transitions (`running`, `succeeded`, `failed`, `cancelled`) |
| `mode` | `RunProgressTracker.start` / Pipeline Config | Never | Set at run start (`batch` or `streaming`) |
| `startedAt` | `RunProgressTracker.start` (`app/pipeline/progress.py`) | Never | Set at run start (ISO-8601 UTC) |
| `updatedAt` | `RunProgressTracker.get_progress` (`app/pipeline/progress.py`) | Never | Every snapshot request / event emission (ISO-8601 UTC) |
| `completedAt` | `RunProgressTracker.emit` (`run_success`, `run_failed`, `run_cancelled`) | While the run is in progress | Set upon terminal state |
| `stages[].key` | Static stage list: `["source", "kafka", "spark", "hdfs"]` | Never | Initialized at run start |
| `stages[].state` | `RunProgressTracker.set_stage_state` / `executor.py` events | Never | Transitions between `pending`, `active`, `done`, `error`, `skipped` |
| `stages[].startedAt` | `RunProgressTracker.emit` (`<stage>_start`) | While stage is `pending` or `skipped` | Set upon entering `active` state |
| `stages[].finishedAt` | `RunProgressTracker.emit` (`<stage>_done` / `run_failed` / `run_cancelled`) | While stage is `pending` or `active` | Set upon transitioning to `done`, `error`, or `skipped` |
| `stages[].error` | `RunProgressTracker.emit` (`<stage>_failed` / `run_failed`) | When stage is successful or pending | Set upon failure or cancellation |
| `source.type` | `executor.py:run_pipeline_stages` (`source_start` payload) | Before source stage starts | Set at source start (e.g. `csv`, `json`, `api`) |
| `source.rowsTotal` | `executor.py:run_pipeline_stages` (`source_done` payload `len(df)`) | Before source reading completes, or if source fails | Set upon source read completion |
| `source.rowsRead` | `executor.py:run_pipeline_stages` (`source_start` = 0, `source_done` = `len(df)`) | Before source stage starts | Set to 0 at start, `len(df)` when read completes |
| `kafka.topic` | `executor.py:run_pipeline_stages` (`kafka_start` payload `topic`) | Before Kafka stage starts | Set at Kafka publish start |
| `kafka.partitions` | `executor.py:_publish_to_kafka` via `get_topic_end_offsets` (`app/messaging/kafka_producer.py`) | Before first Kafka batch callback | Polled via admin/consumer client metadata |
| `kafka.published` | `executor.py:_publish_to_kafka` (delivery callback `delivered_count`) | Before first delivery callback confirms | Polled every 500 ms or 1,000 messages |
| `kafka.failed` | `executor.py:_publish_to_kafka` (delivery callback `failed_count`) | Never (defaults to `0`) | Polled every 500 ms or on publish error |
| `kafka.endOffsets` | `executor.py:_publish_to_kafka` via `get_topic_end_offsets` | Before Kafka batch publishes or if topic offsets cannot be queried | Polled during Kafka stage and on `kafka_done` |
| `kafka.consumerLag` | `RunProgressTracker` | **Always `null` in batch pipelines** (see limits below) | Never measured in batch mode |
| `spark.appId` | `spark-jobs/batch_processing_job.py` via `sc.applicationId` | Before Spark job initializes context | Polled via stdout progress line `SPARK_JOB_PROGRESS` |
| `spark.tasksTotal` | `spark-jobs/batch_processing_job.py` via `statusTracker.getStageInfo` | Before first Spark stage is submitted | Polled every 1.0 s by background daemon in Spark job |
| `spark.tasksDone` | `spark-jobs/batch_processing_job.py` via `stageInfo.numCompletedTasks` | Before first Spark task finishes | Polled every 1.0 s by background daemon in Spark job |
| `spark.activeStage` | `spark-jobs/batch_processing_job.py` (`ActiveStage` dictionary) | When no active stage is running or before Spark starts | Polled every 1.0 s; `null` between stages |
| `spark.recordsIn` | `batch_processing_job.py` (`SPARK_JOB_RESULT` `records_in`) | While Spark job is running | Set upon Spark job completion |
| `spark.recordsOut` | `batch_processing_job.py` (`SPARK_JOB_RESULT` `records_out`) | While Spark job is running | Set upon Spark job completion |
| `spark.durationMs` | `spark-jobs/batch_processing_job.py` (`duration_ms`) | While Spark job is running | Set upon Spark job completion |
| `hdfs.path` | `executor.py:run_pipeline_stages` (`hdfs_start` payload) | Before HDFS stage begins | Set at start of HDFS stage |
| `hdfs.filesWritten` | `app/storage/hdfs_client.py:content_summary` (`fileCount`) | Before output directory exists in WebHDFS | Polled every 1.0 s during Spark execution |
| `hdfs.bytesWritten` | `app/storage/hdfs_client.py:content_summary` (`length`) | Before output directory exists in WebHDFS | Polled every 1.0 s during Spark execution |
| `hdfs.rowsWritten` | `executor.py:_run_spark_job` storage verification count | While Spark job is running or until verified | Set upon successful storage verification |
| `hdfs.verified` | `executor.py:_run_spark_job` storage verification check | While Spark job is running | Set to `True` (or `False` on failure) after verification |
| `throughput.recordsPerSecond` | `app/pipeline/throughput.py:ThroughputEstimator` (EMA alpha=0.3) | If fewer than 2 valid samples or during non-publish stages | Computed on Kafka progress events with interval >= 0.5 s; overall average at `run_success` |
| `throughput.samples` | `app/pipeline/throughput.py:ThroughputEstimator` ring buffer | Never (empty list `[]` when no samples) | Up to 120 timestamp/value tuples |
| `etaSeconds` | `app/pipeline/throughput.py:ThroughputEstimator.eta_seconds` | Outside Kafka publish phase or if throughput <= 0 or samples < 3 | Updated with each Kafka progress tick |
| `errors` | `RunProgressTracker.emit` (`*_failed` or `run_failed`) | When no errors have occurred (empty list `[]`) | On any stage or execution error |
| `fault` | Injected fault descriptor | When no chaos fault is injected | Populated if chaos experiment injects a fault |

---

## 2. Honest Operational Limits and Boundaries

1. **Batch mode has no Kafka consumer lag (`kafka.consumerLag` is always `null`):**
   In batch pipelines, Spark reads directly from Kafka topics using batch partition offsets (`startingOffsets` to `endingOffsets`) rather than a continuous consumer group polling an infinite stream. Because there is no long-running consumer group committing periodic offset checkpoints to Kafka's consumer group coordinator, consumer group lag is neither applicable nor measurable. The field is strictly set to `null` to avoid reporting fabricated zeros.

2. **Spark task totals are cumulative and can grow dynamically:**
   Spark DAG execution submits stages lazily as dependencies resolve. `spark.tasksTotal` represents the sum of tasks across known and active stages reported by the PySpark `statusTracker`. As wide transformations (e.g. `groupByKey`, `reduceByKey`, shuffle partitions) transition into subsequent stages, `spark.tasksTotal` will grow. It should not be assumed to be a fixed constant from job start.

3. **HDFS file and byte counts during execution include temporary files:**
   The WebHDFS `GETCONTENTSUMMARY` endpoint counts all entries under the target output directory path, including files in Spark's `_temporary/0/_temporary/attempt_*` directories before stage commitment. As tasks finish writing partition parts, `filesWritten` and `bytesWritten` rise steadily. Once Spark completes the commit protocol, temporary files are replaced with the final `part-*.parquet` files.

4. **ETA (`etaSeconds`) exists only during the Kafka publish phase:**
   Calculating remaining execution time requires knowing the remaining work (`source.rowsTotal - kafka.published`) and having a stable non-zero rate. During the Spark phase, Spark job execution time depends on cluster resource contention, shuffles, and garbage collection, which cannot be accurately predicted by linear row rates. Therefore, `etaSeconds` is strictly cleared to `null` once the Kafka stage finishes.

5. **Source rows jump (`source.rowsRead`):**
   Standard tabular sources (CSV, Parquet, JSON files) are parsed by Pandas / Polars in a single blocking read operation. Consequently, `source.rowsRead` transitions from `0` on `source_start` directly to `rowsTotal` upon completion of `source_done`.
