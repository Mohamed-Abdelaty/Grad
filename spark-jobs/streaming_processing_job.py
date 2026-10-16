"""
spark-jobs/streaming_processing_job.py
-----------------------------------------
PySpark Structured Streaming job.

Reads from a Kafka topic (specified in the ``messaging`` section of
``--config-json``), parses JSON-encoded message values using the
pipeline schema, applies watermark and processing operations, and writes
the result to ``--output-path`` as an append-mode Parquet streaming sink.

Emits structured events to stdout:
- STREAM_STARTED: {"query_id": "...", "run_id": "..."}
- STREAM_PROGRESS: {"batchId": 0, "numInputRows": 50, ...}
- SPARK_JOB_RESULT: {"input_rows": N, "output_rows": N, ...}

Gracefully handles SIGTERM and SIGINT to flush in-flight micro-batches
and write final metrics before process termination.
"""

from __future__ import annotations

import argparse
import json
import logging
import signal
import sys
import threading
import time
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from base_job import BaseSparkJob, KAFKA_SPARK_PACKAGE
from operations.filter_ops import _check_condition

logger = logging.getLogger(__name__)

# Map Phase 2 ColumnType strings to Spark SQL types
_COLUMN_TYPE_MAP: dict[str, object] = {
    "string": StringType(),
    "integer": IntegerType(),
    "double": DoubleType(),
    "boolean": BooleanType(),
    "timestamp": TimestampType(),
}


class StreamingProcessingJob(BaseSparkJob):
    """
    PySpark Structured Streaming pipeline job.

    Processes streaming events from Kafka with event-time watermarking,
    schema enforcement, streaming operations, and HDFS checkpointing.
    """

    def _build_session(self):
        """Build SparkSession capped to 1 core for predictable multi-tenant scheduling."""
        builder = (
            self.SparkSession.builder.appName(self.__class__.__name__)
            if hasattr(self, "SparkSession")
            else super()._build_session().builder
        )
        return (
            super()
            ._build_session()
            .builder.config("spark.jars.packages", KAFKA_SPARK_PACKAGE)
            .config("spark.cores.max", "1")
            .config("spark.sql.adaptive.enabled", "true")
            .getOrCreate()
        )

    @classmethod
    def _build_arg_parser(cls) -> argparse.ArgumentParser:
        """Extend argument parser with streaming-specific flags."""
        parser = super()._build_arg_parser()
        parser.add_argument(
            "--checkpoint-path",
            default=None,
            help="HDFS checkpoint directory. Defaults to <output-path>/_checkpoints",
        )
        parser.add_argument(
            "--trigger-seconds",
            type=int,
            default=None,
            help="Micro-batch trigger interval in seconds.",
        )
        parser.add_argument(
            "--starting-offsets",
            default=None,
            help="Kafka starting offset strategy ('earliest' or 'latest').",
        )
        parser.add_argument(
            "--max-offsets-per-trigger",
            type=int,
            default=None,
            help="Max offsets to consume per micro-batch trigger.",
        )
        return parser

    def _build_schema(self, schema_def: list[dict[str, Any]]) -> StructType:
        """Convert pipeline schema definition to Spark StructType."""
        fields = []
        for col in schema_def:
            name = col["name"]
            type_str = col.get("type", "string")
            spark_type = _COLUMN_TYPE_MAP.get(type_str, StringType())
            nullable = col.get("nullable", True)
            fields.append(StructField(name, spark_type, nullable))
        return StructType(fields)

    def _apply_streaming_operations(
        self, df: DataFrame, operations: list[dict[str, Any]]
    ) -> DataFrame:
        """
        Apply operations to streaming DataFrame.

        Avoids eager count() actions which are prohibited on streaming DataFrames.
        """
        for op in operations:
            op_type = op.get("type")
            if op_type == "remove_nulls":
                df = df.na.drop()
            elif op_type == "remove_duplicates":
                df = df.dropDuplicates()
            elif op_type == "filter":
                cond = op["condition"]
                _check_condition(cond)
                df = df.filter(cond)
            elif op_type == "select_columns":
                cols = op["columns"]
                missing = [c for c in cols if c not in df.columns]
                if missing:
                    raise ValueError(
                        f"select_columns: columns {missing} not found in DataFrame. Available: {df.columns}"
                    )
                df = df.select(*cols)
            elif op_type == "rename_columns":
                mapping = op.get("mapping", {})
                for old, new in mapping.items():
                    if old not in df.columns:
                        raise ValueError(
                            f"rename_columns: column '{old}' not found. Available: {df.columns}"
                        )
                    df = df.withColumnRenamed(old, new)
            elif op_type == "type_conversion":
                col_name = op["column"]
                target_str = op["to_type"]
                if col_name not in df.columns:
                    raise ValueError(
                        f"convert_type: column '{col_name}' not found. Available: {df.columns}"
                    )
                spark_type = _COLUMN_TYPE_MAP.get(target_str)
                if spark_type is None:
                    raise ValueError(
                        f"convert_type: unsupported type '{target_str}'. Supported: {list(_COLUMN_TYPE_MAP.keys())}"
                    )
                df = df.withColumn(col_name, F.col(col_name).cast(spark_type))
            else:
                self.logger.warning("Unsupported streaming operation '%s' skipped.", op_type)
        return df

    def run(self) -> None:
        """Start Structured Streaming query and run monitor loop until stopped."""
        start_time = time.perf_counter()
        messaging_cfg = self.config.get("messaging", {})
        kafka_topic = messaging_cfg.get("topic", "")
        bootstrap_servers = messaging_cfg.get("bootstrap_servers", "kafka:9092")

        streaming_cfg = self.config.get("streaming", {})
        trigger_seconds = (
            self.args.trigger_seconds
            if self.args.trigger_seconds is not None
            else streaming_cfg.get("trigger_interval_seconds", 5)
        )
        starting_offsets = (
            self.args.starting_offsets
            if self.args.starting_offsets is not None
            else streaming_cfg.get("starting_offsets", "earliest")
        )
        max_offsets = (
            self.args.max_offsets_per_trigger
            if self.args.max_offsets_per_trigger is not None
            else streaming_cfg.get("max_offsets_per_trigger")
        )

        output_path = self.args.output_path
        checkpoint_path = self.args.checkpoint_path or f"{output_path}/_checkpoints"

        self.logger.info(
            "StreamingProcessingJob starting: topic=%s, servers=%s, output=%s, checkpoint=%s",
            kafka_topic,
            bootstrap_servers,
            output_path,
            checkpoint_path,
        )

        # 1. Read Kafka Stream
        reader = (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", bootstrap_servers)
            .option("subscribe", kafka_topic)
            .option("startingOffsets", starting_offsets)
            .option("failOnDataLoss", "false")
        )
        if max_offsets is not None:
            reader = reader.option("maxOffsetsPerTrigger", int(max_offsets))

        raw_stream = reader.load()

        # 2. Parse JSON Value with Schema
        schema_def = self.config.get("schema", [])
        spark_schema = self._build_schema(schema_def)
        parsed_df = raw_stream.select(
            F.from_json(F.col("value").cast("string"), spark_schema).alias("data")
        ).select("data.*")

        # 3. Apply Watermark if Configured
        watermark_cfg = streaming_cfg.get("watermark")
        if watermark_cfg:
            wm_col = watermark_cfg.get("column")
            wm_delay = watermark_cfg.get("delay_seconds", 10)
            if wm_col in parsed_df.columns:
                parsed_df = parsed_df.withWatermark(wm_col, f"{wm_delay} seconds")
                self.logger.info("Watermark applied on '%s' with %d seconds delay", wm_col, wm_delay)

        # 4. Apply Operations
        processing_cfg = self.config.get("processing", {})
        operations = processing_cfg.get("operations", [])
        processed_df = self._apply_streaming_operations(parsed_df, operations)

        # 5. Start Parquet Append Sink
        query: StreamingQuery = (
            processed_df.writeStream.format("parquet")
            .outputMode("append")
            .option("path", output_path)
            .option("checkpointLocation", checkpoint_path)
            .trigger(processingTime=f"{trigger_seconds} seconds")
            .start()
        )

        # Emit STREAM_STARTED immediately
        print(
            f"STREAM_STARTED:{json.dumps({'query_id': str(query.id), 'run_id': str(query.runId)})}",
            flush=True,
        )
        self.logger.info("Streaming query started: queryId=%s runId=%s", query.id, query.runId)

        # Graceful shutdown handler
        stop_event = threading.Event()

        def _signal_handler(signum, frame):
            self.logger.info("Signal %s received; requesting query stop...", signum)
            stop_event.set()

        try:
            signal.signal(signal.SIGTERM, _signal_handler)
            signal.signal(signal.SIGINT, _signal_handler)
        except (ValueError, AttributeError):
            pass

        last_seen_batch_id = -1
        total_input_rows = 0

        # Monitor loop: poll progress every 1s
        while query.isActive and not stop_event.is_set():
            time.sleep(1.0)
            lp = query.lastProgress
            if lp is not None:
                bid = lp.get("batchId", -1)
                if bid > last_seen_batch_id:
                    last_seen_batch_id = bid
                    num_rows = lp.get("numInputRows", 0)
                    total_input_rows += num_rows
                    sources = lp.get("sources", [])
                    end_offset = sources[0].get("endOffset") if sources else {}
                    progress_payload = {
                        "batchId": bid,
                        "numInputRows": num_rows,
                        "inputRowsPerSecond": round(lp.get("inputRowsPerSecond", 0.0), 2),
                        "processedRowsPerSecond": round(lp.get("processedRowsPerSecond", 0.0), 2),
                        "endOffset": end_offset,
                        "totalInputRows": total_input_rows,
                        "timestamp": lp.get("timestamp"),
                    }
                    print(f"STREAM_PROGRESS:{json.dumps(progress_payload)}", flush=True)

        if stop_event.is_set():
            self.logger.info("Stopping streaming query gracefully...")
            query.stop()

        try:
            query.awaitTermination(timeout=30)
        except Exception as exc:
            self.logger.warning("Query termination timeout/exception: %s", exc)

        # Capture final progress if any completed during stop
        lp = query.lastProgress
        if lp is not None and lp.get("batchId", -1) > last_seen_batch_id:
            bid = lp.get("batchId")
            num_rows = lp.get("numInputRows", 0)
            total_input_rows += num_rows
            sources = lp.get("sources", [])
            end_offset = sources[0].get("endOffset") if sources else {}
            progress_payload = {
                "batchId": bid,
                "numInputRows": num_rows,
                "inputRowsPerSecond": round(lp.get("inputRowsPerSecond", 0.0), 2),
                "processedRowsPerSecond": round(lp.get("processedRowsPerSecond", 0.0), 2),
                "endOffset": end_offset,
                "totalInputRows": total_input_rows,
                "timestamp": lp.get("timestamp"),
            }
            print(f"STREAM_PROGRESS:{json.dumps(progress_payload)}", flush=True)

        duration = round(time.perf_counter() - start_time, 3)
        status = "failed" if query.exception() else "success"
        result_payload = {
            "input_rows": total_input_rows,
            "output_rows": total_input_rows,
            "duration_seconds": duration,
            "status": status,
        }
        self._emit_result(result_payload)
        self.logger.info("Streaming job finished: %s", result_payload)


if __name__ == "__main__":
    StreamingProcessingJob.main()
