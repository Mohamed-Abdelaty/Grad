"""
spark-jobs/batch_processing_job.py
------------------------------------
PySpark batch processing job.

Reads a source file (CSV or JSON), applies the ordered list of processing
operations from ``--config-json``, writes the result to ``--output-path``
as Parquet, and emits a ``SPARK_JOB_RESULT:`` line to stdout so the
backend can parse job metrics.

Submitted via::

    spark-submit \\
        --master spark://spark-master:7077 \\
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \\
        batch_processing_job.py \\
        --config-json '{"processing": {...}}' \\
        --input-source /data/sales.csv \\
        --output-path /output/sales-processed

"""

from __future__ import annotations

import json
import threading
import time

from pyspark.sql import DataFrame

from base_job import BaseSparkJob
from operations.aggregate_ops import aggregate
from operations.cleaning_ops import remove_duplicates, remove_nulls
from operations.column_ops import convert_type, rename_columns, select_columns
from operations.filter_ops import apply_filter


class BatchProcessingJob(BaseSparkJob):
    """
    Batch job: read file → apply operations → write Parquet.

    Supports the same operation types defined in Phase 2's
    ``ProcessingOperation`` discriminated union:
    - ``remove_nulls``
    - ``remove_duplicates``
    - ``filter``
    - ``select_columns``
    - ``rename_columns``
    - ``type_conversion``
    - ``aggregate``
    """

    def run(self) -> None:
        """Execute the batch pipeline and write results to Parquet."""
        start = time.perf_counter()
        self.logger.info(
            "BatchProcessingJob starting — source: %s, output: %s",
            self.args.input_source,
            self.args.output_path,
        )

        stages: dict[int, tuple[int, int, str]] = {}
        stop_event = threading.Event()

        def _scan_stages(stages_dict: dict[int, tuple[int, int, str]]) -> tuple[int, int, dict | None]:
            sc = self.spark.sparkContext
            tracker = sc.statusTracker()
            active_ids = tracker.getActiveStageIds()
            latest_active_stage = None

            all_stage_ids = set(active_ids)
            try:
                for jid in tracker.getActiveJobsIds():
                    jinfo = tracker.getJobInfo(jid)
                    if jinfo and jinfo.stageIds:
                        all_stage_ids.update(jinfo.stageIds)
            except Exception:
                pass

            try:
                for jid in tracker.getJobIdsForGroup(None) or []:
                    jinfo = tracker.getJobInfo(jid)
                    if jinfo and jinfo.stageIds:
                        all_stage_ids.update(jinfo.stageIds)
            except Exception:
                pass

            max_known = max(all_stage_ids | set(stages_dict.keys()), default=-1)
            for sid in range(max(0, max_known + 10)):
                all_stage_ids.add(sid)

            for sid in sorted(all_stage_ids):
                try:
                    info = tracker.getStageInfo(sid)
                    if info is not None:
                        num_tasks = getattr(info, "numTasks", None)
                        if callable(num_tasks):
                            num_tasks = num_tasks()
                        completed_tasks = getattr(info, "numCompletedTasks", None)
                        if callable(completed_tasks):
                            completed_tasks = completed_tasks()
                        name = getattr(info, "name", None)
                        if callable(name):
                            name = name()
                        stages_dict[sid] = (num_tasks or 0, completed_tasks or 0, str(name or ""))
                        if sid in active_ids:
                            latest_active_stage = {
                                "id": sid,
                                "name": str(name or ""),
                                "tasksTotal": num_tasks or 0,
                                "tasksDone": completed_tasks or 0,
                            }
                except Exception:
                    pass

            tasks_total = sum(s[0] for s in stages_dict.values())
            tasks_done = sum(s[1] for s in stages_dict.values())
            return tasks_total, tasks_done, latest_active_stage

        def _monitor_progress() -> None:
            while not stop_event.is_set():
                try:
                    tasks_total, tasks_done, latest_active_stage = _scan_stages(stages)
                    sc = self.spark.sparkContext
                    progress_payload = {
                        "appId": sc.applicationId,
                        "tasksTotal": tasks_total,
                        "tasksDone": tasks_done,
                        "activeStage": latest_active_stage,
                        "ts": time.time(),
                    }
                    print(f"SPARK_JOB_PROGRESS:{json.dumps(progress_payload)}", flush=True)
                except Exception:
                    pass
                stop_event.wait(0.5)

        monitor_thread = threading.Thread(target=_monitor_progress, daemon=True)
        monitor_thread.start()

        # ── Read source ────────────────────────────────────────────────────────
        df = self._read_source(self.args.input_source)
        input_rows = df.count()
        self.logger.info("Read %d rows from source.", input_rows)

        # ── Apply operations ───────────────────────────────────────────────────
        processing_cfg = self.config.get("processing", {})
        operations = processing_cfg.get("operations", [])
        df = self._apply_operations(df, operations)

        # ── Write output ───────────────────────────────────────────────────────
        output_rows = df.count()
        df.write.mode("overwrite").parquet(self.args.output_path)
        self.logger.info("Wrote %d rows to %s", output_rows, self.args.output_path)

        stop_event.set()
        monitor_thread.join(timeout=2.0)

        # Emit final progress snapshot with complete task counts
        try:
            tasks_total, tasks_done, _ = _scan_stages(stages)
            sc = self.spark.sparkContext
            progress_payload = {
                "appId": sc.applicationId,
                "tasksTotal": tasks_total,
                "tasksDone": tasks_done,
                "activeStage": None,
                "ts": time.time(),
            }
            print(f"SPARK_JOB_PROGRESS:{json.dumps(progress_payload)}", flush=True)
        except Exception:
            pass

        duration = round(time.perf_counter() - start, 3)
        self._emit_result(
            {
                "input_rows": input_rows,
                "output_rows": output_rows,
                "duration_seconds": duration,
                "status": "success",
            }
        )

    # ── Private helpers ────────────────────────────────────────────────────────

    def _read_source(self, path: str) -> DataFrame:
        """
        Read CSV or JSON based on the file extension.

        Parameters
        ----------
        path : str
            Absolute path to the input file.

        Returns
        -------
        DataFrame
            Spark DataFrame with inferred schema.
        """
        if path.endswith(".json"):
            return self.spark.read.option("multiline", "true").json(path)
        # Default to CSV (includes .csv and unrecognised extensions)
        return self.spark.read.option("header", "true").option("inferSchema", "true").csv(path)

    def _apply_operations(
        self, df: DataFrame, operations: list[dict]
    ) -> DataFrame:
        """
        Apply the ordered list of processing operations to *df*.

        Each operation dict must have a ``type`` key matching a supported
        operation name.  Unknown operation types are logged and skipped
        (rather than failing) to match Phase 3 behaviour.

        Parameters
        ----------
        df : DataFrame
            Input DataFrame.
        operations : list[dict]
            Sequence of operation dicts from the pipeline config.

        Returns
        -------
        DataFrame
            Transformed DataFrame.
        """
        for op in operations:
            op_type = op.get("type")
            self.logger.info("Applying operation: %s", op_type)

            if op_type == "remove_nulls":
                df = remove_nulls(df)
            elif op_type == "remove_duplicates":
                df = remove_duplicates(df)
            elif op_type == "filter":
                df = apply_filter(df, op["condition"])
            elif op_type == "select_columns":
                df = select_columns(df, op["columns"])
            elif op_type == "rename_columns":
                df = rename_columns(df, op["mapping"])
            elif op_type == "type_conversion":
                df = convert_type(df, op["column"], op["to_type"])
            elif op_type == "aggregate":
                df = aggregate(
                    df,
                    group_by=op["group_by"],
                    operation=op["operation"],
                    field=op["field"],
                )
            else:
                self.logger.warning("Unknown operation type '%s' — skipping.", op_type)

        return df


if __name__ == "__main__":
    BatchProcessingJob.main()
