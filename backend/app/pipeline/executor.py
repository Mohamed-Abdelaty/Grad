"""
app/pipeline/executor.py
--------------------------
Executes an :class:`~app.pipeline.generator.ExecutionPlan` step-by-step,
collecting per-stage metrics and structured logs into a
:class:`~app.pipeline.context.PipelineExecutionContext`.

Error handling contract
------------------------
The executor catches *every* exception raised by any plugin and wraps it in
:class:`PipelineExecutionError`.  This guarantees:

* The HTTP layer never receives an unhandled exception from a plugin.
* The partial :class:`~app.pipeline.context.PipelineExecutionContext` (showing
  how far execution reached and what metrics were collected) is always
  accessible via ``PipelineExecutionError.context``.
* Stack traces are preserved in the ``__cause__`` chain for debugging.

Execution order
---------------
1. Source step  → ``plugin.read(config)`` → DataFrame
2. [Optional] Kafka publish — if ``plan.messaging_config`` is set
3. [Branch A — Spark] if ``plan.processing_engine_type == "spark"``:
     a. Write DataFrame to a temp CSV
     b. Submit ``batch_processing_job.py`` via ``SparkJobSubmitter``
     c. Record ``spark_job`` metrics; storage step reads from Parquet output
4. [Branch B — Pandas] otherwise:
     Processor steps (in plan order) → ``plugin.process(df, config)`` → DataFrame
     then Storage step → ``plugin.write(df, config)`` → metadata dict

Each step's row count and wall-clock duration are recorded in
``context.metrics[step_name]``.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
import uuid
from typing import Any

import pandas as pd

from app.pipeline.context import PipelineExecutionContext
from app.pipeline.generator import ExecutionPlan, ExecutionStep
from app.spark.spark_job_submitter import SparkJobSubmitter

# Import the global progress tracker (populated during app startup by main.py).
# The import is at module level so the singleton is shared across all callers.
# In testing, the tracker is a no-op when run_id is None.
from app.pipeline.progress import tracker as _progress_tracker

logger = logging.getLogger(__name__)

# ── Shared-filesystem rule ─────────────────────────────────────────────────────
#
# In Spark standalone cluster mode (spark://spark-master:7077 with client
# deploy mode) the driver runs on the backend container but executors run on
# spark-worker containers.  Local file-system paths (/tmp/, /app/ etc.) are
# container-local: a file in the backend's /tmp/ is invisible to Spark workers,
# and part files written by workers into their /tmp/ are invisible to the
# backend.
#
# Rule: for Spark pipelines ALL inputs and outputs must live on a shared
# filesystem — either HDFS (hdfs://namenode:9000/...) or a Docker volume
# mounted to BOTH the backend and all spark-worker containers.
#
# Currently the only shared bind-mount is ./backend/sample-data:/app/sample-data
# (present on backend, spark-master, and spark-worker in docker-compose.yml).
# Everything else, including /tmp/, must go to HDFS.
#
# _upload_df_to_hdfs() and the output-path normalisation in _run_spark_job()
# enforce this rule automatically so callers never need to know about it.


def _upload_df_to_hdfs(df: pd.DataFrame, hdfs_path: str) -> None:
    """
    Upload *df* as a UTF-8 CSV to HDFS using the WebHDFS REST API.

    WebHDFS CREATE uses a mandatory two-step HTTP redirect:

    1. ``PUT NameNode?op=CREATE`` → 307 Temporary Redirect with the DataNode
       upload URL in the ``Location`` header.
    2. ``PUT DataNode`` with the CSV bytes → 201 Created.

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame to serialise and upload.
    hdfs_path : str
        Absolute HDFS path (e.g. ``/tmp/spark-input-abc123.csv``).

    Raises
    ------
    RuntimeError
        If either WebHDFS step fails (non-redirect / non-2xx response).
    """
    import requests as _requests
    from app.core.config import get_settings

    if hdfs_path.startswith("hdfs://"):
        without_scheme = hdfs_path.split("://", 1)[1]
        slash = without_scheme.find("/")
        hdfs_path = without_scheme[slash:] if slash >= 0 else "/"

    settings = get_settings()
    namenode_url = settings.HDFS_NAMENODE_URL   # http://namenode:9870
    user = settings.HDFS_USER                   # root

    csv_bytes = df.to_csv(index=False).encode("utf-8")

    # Step 1 — initiate CREATE; NameNode returns 307 Redirect to DataNode
    create_url = (
        f"{namenode_url}/webhdfs/v1{hdfs_path}"
        f"?op=CREATE&user.name={user}&overwrite=true&replication=1"
    )
    resp1 = _requests.put(create_url, allow_redirects=False, timeout=15)
    if resp1.status_code not in (200, 201, 307):
        raise RuntimeError(
            f"HDFS CREATE (step 1) failed with HTTP {resp1.status_code}: "
            f"{resp1.text[:300]}"
        )

    datanode_url = resp1.headers.get("Location", "")
    if not datanode_url:
        raise RuntimeError("HDFS CREATE (step 1) returned no Location header.")

    # Step 2 — upload CSV bytes to the DataNode URL
    resp2 = _requests.put(datanode_url, data=csv_bytes, timeout=60)
    if resp2.status_code not in (200, 201):
        raise RuntimeError(
            f"HDFS CREATE (step 2) failed with HTTP {resp2.status_code}: "
            f"{resp2.text[:300]}"
        )

    logger.info(
        "Staged %d-row DataFrame (%d bytes) to HDFS %s",
        len(df), len(csv_bytes), hdfs_path,
    )


def _delete_hdfs_path(hdfs_path: str, recursive: bool = False) -> None:
    """
    Delete an HDFS path using the WebHDFS REST DELETE API.

    Called in the ``finally`` block of :meth:`PipelineExecutor._run_spark_job`
    to clean up temporary input files created for non-file sources (carry-over b),
    and cleanup partial outputs on failure/cancellation.

    Failures are logged as warnings and never propagated — cleanup is best-effort.

    Parameters
    ----------
    hdfs_path : str
        Absolute HDFS path (e.g. ``/tmp/spark-input-abc123.csv``) or full URI.
    recursive : bool
        Whether to delete directories recursively.
    """
    try:
        import requests as _requests
        from app.core.config import get_settings

        if hdfs_path.startswith("hdfs://"):
            without_scheme = hdfs_path.split("://", 1)[1]
            slash = without_scheme.find("/")
            hdfs_path = without_scheme[slash:] if slash >= 0 else "/"

        settings = get_settings()
        rec_str = "true" if recursive else "false"
        url = (
            f"{settings.HDFS_NAMENODE_URL}/webhdfs/v1{hdfs_path}"
            f"?op=DELETE&recursive={rec_str}&user.name={settings.HDFS_USER}"
        )
        resp = _requests.delete(url, timeout=10)
        if resp.status_code in (200, 404):
            logger.info("Deleted HDFS path %s (HTTP %d).", hdfs_path, resp.status_code)
        else:
            logger.warning(
                "HDFS DELETE %s returned unexpected HTTP %d: %s",
                hdfs_path,
                resp.status_code,
                resp.text[:200],
            )
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to delete HDFS path %s: %s", hdfs_path, exc)


# ── Executor exception ─────────────────────────────────────────────────────────


class PipelineExecutionError(Exception):
    """
    Raised when any plugin step fails during pipeline execution.

    Wraps the original exception and carries the partial execution context so
    callers can inspect how far the pipeline progressed before failing.

    Attributes
    ----------
    step_name : str
        Name of the step that failed (e.g. ``"processor:filter:2"``).
    context : PipelineExecutionContext
        The partial context accumulated up to the point of failure.  Metrics
        and logs for all *successfully completed* steps are present.
    cause : Exception
        The original exception raised by the failing plugin.
    """

    def __init__(
        self,
        message: str,
        step_name: str,
        context: PipelineExecutionContext,
        cause: Exception,
    ) -> None:
        super().__init__(message)
        self.step_name = step_name
        self.context = context
        self.cause = cause


# ── Executor ───────────────────────────────────────────────────────────────────


class PipelineExecutor:
    """
    Executes a :class:`~app.pipeline.generator.ExecutionPlan` produced by
    :class:`~app.pipeline.generator.PipelineGenerator`.

    Each call to :meth:`execute` is independent — a new
    :class:`~app.pipeline.context.PipelineExecutionContext` is created for
    every run.

    Usage::

        plan = PipelineGenerator().generate(config)
        context = PipelineExecutor().execute(plan)
        print(context.status)   # "success"
        print(context.metrics)  # {step: {rows_out, duration_ms}, ...}
    """

    def execute(
        self, plan: ExecutionPlan, tracker_run_id: str | None = None
    ) -> PipelineExecutionContext:
        """
        Run all steps in *plan* in order and return the execution context.

        Parameters
        ----------
        plan : ExecutionPlan
            Resolved execution plan from :class:`~app.pipeline.generator.PipelineGenerator`.
        tracker_run_id : str | None
            When provided, progress events are emitted to the global
            :data:`~app.pipeline.progress.tracker` under this key.  This must
            match the ``pipeline_run_id`` stored in the database so the SSE
            endpoint and REST endpoints can serve the same events.

        Returns
        -------
        PipelineExecutionContext
            Context with ``status="success"`` and metrics/logs for every step.

        Raises
        ------
        PipelineExecutionError
            Any step raises an exception.  The partial context is embedded in
            the error object.
        """
        ctx = PipelineExecutionContext()
        ctx.log_info(
            "executor",
            f"Starting pipeline '{plan.pipeline_name}' v{plan.pipeline_version} "
            f"— run_id={ctx.run_id} engine={plan.processing_engine_type or 'pandas'}",
        )

        df: pd.DataFrame | None = None

        # ── Source step ────────────────────────────────────────────────────────
        source_step = next(s for s in plan.steps if s.step_type == "source")
        df = self._run_source(source_step, ctx, tracker_run_id)

        # ── Cancel check ───────────────────────────────────────────────────────
        if tracker_run_id and _progress_tracker.is_cancel_requested(tracker_run_id):
            ctx.status = "failed"
            ctx.log_info("executor", "Pipeline cancelled after source stage.")
            raise PipelineExecutionError(
                message="Cancelled",
                step_name="executor",
                context=ctx,
                cause=RuntimeError("Cancelled by user request"),
            )

        # ── Optional Kafka publish ─────────────────────────────────────────────
        if plan.messaging_config:
            self._publish_to_kafka(plan.messaging_config, df, ctx, tracker_run_id)
        else:
            if tracker_run_id:
                _progress_tracker.set_stage_state(tracker_run_id, "kafka", "skipped")

        # ── Cancel check ───────────────────────────────────────────────────────
        if tracker_run_id and _progress_tracker.is_cancel_requested(tracker_run_id):
            ctx.status = "failed"
            ctx.log_info("executor", "Pipeline cancelled after Kafka stage.")
            raise PipelineExecutionError(
                message="Cancelled",
                step_name="executor",
                context=ctx,
                cause=RuntimeError("Cancelled by user request"),
            )

        # ── Processing branch ──────────────────────────────────────────────────
        if plan.processing_engine_type == "spark":
            # Spark path: submit the whole processing block as a Spark job.
            # The storage step afterward is skipped — Spark writes Parquet directly.
            self._run_spark_job(plan, df, ctx, tracker_run_id)
        else:
            # Pandas path (Phase 3): run processors in-process, then storage.
            for step in plan.steps:
                if step.step_type == "processor":
                    df = self._run_processor(step, df, ctx)
                elif step.step_type == "storage":
                    _progress_tracker.emit(tracker_run_id, "hdfs_start")
                    self._run_storage(step, df, ctx)
                    _progress_tracker.emit(
                        tracker_run_id,
                        "hdfs_done",
                        {"rows": len(df)},
                    )
                # source already handled above

        ctx.status = "success"
        from app.metrics import pipeline_records_processed_total
        pipeline_records_processed_total.labels(plan.pipeline_name).inc(len(df))
        ctx.log_info("executor", f"Pipeline '{plan.pipeline_name}' completed successfully.")
        logger.info("Pipeline '%s' run %s completed.", plan.pipeline_name, ctx.run_id)
        return ctx

    # ── Step runners ───────────────────────────────────────────────────────────

    def _run_source(
        self,
        step: ExecutionStep,
        ctx: PipelineExecutionContext,
        tracker_run_id: str | None = None,
    ) -> pd.DataFrame:
        """Execute the source step and record metrics."""
        _progress_tracker.emit(tracker_run_id, "source_start", {"plugin": step.plugin_type})
        ctx.log_info(step.name, f"Reading data via '{step.plugin_type}' source.")
        t0 = time.perf_counter()

        try:
            df = step.plugin.read(step.config)
        except Exception as exc:
            _progress_tracker.emit(
                tracker_run_id,
                "source_failed",
                {"error": str(exc)},
            )
            self._handle_failure(step, ctx, exc)

        duration_ms = round((time.perf_counter() - t0) * 1000, 2)
        rows = len(df)
        ctx.record_stage(step.name, {"rows_out": rows, "duration_ms": duration_ms})
        from app.metrics import pipeline_processing_latency_seconds
        pipeline_processing_latency_seconds.labels(step.name).observe(duration_ms / 1000)
        ctx.log_info(step.name, f"Read {rows} row(s) in {duration_ms}ms.")
        _progress_tracker.emit(
            tracker_run_id,
            "source_done",
            {"rows": rows, "duration_ms": duration_ms},
        )
        return df

    def _run_processor(
        self,
        step: ExecutionStep,
        df: pd.DataFrame,
        ctx: PipelineExecutionContext,
    ) -> pd.DataFrame:
        """Execute a processor step, feeding in *df* and recording metrics."""
        rows_in = len(df)
        ctx.log_info(
            step.name, f"Applying '{step.plugin_type}' processor ({rows_in} rows in)."
        )
        t0 = time.perf_counter()

        try:
            df_out = step.plugin.process(df, step.config)
        except Exception as exc:
            self._handle_failure(step, ctx, exc)

        duration_ms = round((time.perf_counter() - t0) * 1000, 2)
        rows_out = len(df_out)
        ctx.record_stage(
            step.name,
            {"rows_in": rows_in, "rows_out": rows_out, "duration_ms": duration_ms},
        )
        from app.metrics import pipeline_processing_latency_seconds
        pipeline_processing_latency_seconds.labels(step.name).observe(duration_ms / 1000)
        ctx.log_info(
            step.name,
            f"Processor '{step.plugin_type}' complete: {rows_in} → {rows_out} rows "
            f"in {duration_ms}ms.",
        )
        return df_out

    def _run_storage(
        self,
        step: ExecutionStep,
        df: pd.DataFrame,
        ctx: PipelineExecutionContext,
    ) -> None:
        """Execute the storage step and record write metadata."""
        ctx.log_info(
            step.name, f"Writing {len(df)} row(s) via '{step.plugin_type}' storage."
        )
        t0 = time.perf_counter()

        try:
            write_meta: dict = step.plugin.write(df, step.config)
        except Exception as exc:
            self._handle_failure(step, ctx, exc)

        duration_ms = round((time.perf_counter() - t0) * 1000, 2)
        meta_with_timing = {**write_meta, "duration_ms": duration_ms}
        ctx.record_stage(step.name, meta_with_timing)
        from app.metrics import pipeline_processing_latency_seconds
        pipeline_processing_latency_seconds.labels(step.name).observe(duration_ms / 1000)
        ctx.log_info(
            step.name,
            f"Wrote {write_meta.get('rows_written', '?')} row(s) to "
            f"'{write_meta.get('path', '?')}' in {duration_ms}ms.",
        )

    # ── Kafka publish ──────────────────────────────────────────────────────────

    def _publish_to_kafka(
        self,
        messaging_config: dict,
        df: pd.DataFrame,
        ctx: PipelineExecutionContext,
        tracker_run_id: str | None = None,
    ) -> None:
        """
        Publish raw source records to Kafka if messaging configuration is present.

        Parameters
        ----------
        messaging_config : dict
            Messaging section from PipelineConfig.
        df : pandas.DataFrame
            Raw source DataFrame to publish.
        ctx : PipelineExecutionContext
            Execution context to record metrics and logs.
        tracker_run_id : str | None
            If provided, emits kafka_start / kafka_done events to the tracker.
        """
        topic = messaging_config.get("topic")
        if not topic:
            return

        partitions = messaging_config.get("partitions", 1)
        replication_factor = messaging_config.get("replication_factor", 1)

        t0 = time.perf_counter()

        try:
            import json

            from app.messaging.kafka_admin import KafkaAdminClient
            from app.messaging.kafka_producer import KafkaProducerClient, get_topic_end_offsets

            admin = KafkaAdminClient()
            admin.create_topic(
                topic,
                partitions=partitions,
                replication_factor=replication_factor,
            )

            producer = KafkaProducerClient()
            actual_partitions = producer.get_topic_partitions(topic) or partitions
            _progress_tracker.emit(
                tracker_run_id,
                "kafka_start",
                {"topic": topic, "partitions": actual_partitions},
            )
            ctx.log_info(
                "kafka_publish", f"Publishing raw source records to Kafka topic '{topic}'."
            )

            last_probe_time = 0.0
            last_end_offsets = None

            def _on_kafka_progress(published: int, failed: int) -> None:
                nonlocal last_probe_time, last_end_offsets
                now = time.monotonic()
                if now - last_probe_time >= 1.0:
                    last_end_offsets = get_topic_end_offsets(topic)
                    last_probe_time = now
                if tracker_run_id:
                    _progress_tracker.update_kafka(
                        tracker_run_id,
                        published=published,
                        failed=failed,
                        end_offsets=last_end_offsets,
                        partitions=actual_partitions,
                        topic=topic,
                    )

            records = json.loads(df.to_json(orient="records", date_format="iso"))
            result = producer.produce_batch(
                topic,
                records,
                on_progress=_on_kafka_progress,
            )

            final_end_offsets = get_topic_end_offsets(topic)
            duration_ms = round((time.perf_counter() - t0) * 1000, 2)
            meta_with_timing = {**result, "duration_ms": duration_ms}
            ctx.record_stage("kafka_publish", meta_with_timing)
            published = result.get("produced", 0)
            failed = result.get("failed", 0)
            ctx.log_info(
                "kafka_publish",
                f"Published {published} record(s), failed {failed} to topic "
                f"'{topic}' in {duration_ms}ms.",
            )
            _progress_tracker.emit(
                tracker_run_id,
                "kafka_done",
                {
                    "count": published,
                    "published": published,
                    "failed": failed,
                    "end_offsets": final_end_offsets,
                    "partitions": actual_partitions,
                    "topic": topic,
                    "duration_ms": duration_ms,
                },
            )
        except Exception as exc:
            if tracker_run_id:
                _progress_tracker.set_stage_state(
                    tracker_run_id, "kafka", "error", error=str(exc)
                )
            publish_step = ExecutionStep(
                name="kafka_publish",
                step_type="messaging",
                plugin_type="kafka",
                plugin=None,
                config=messaging_config,
            )
            self._handle_failure(publish_step, ctx, exc)

    # ── Spark branch ───────────────────────────────────────────────────────────

    def _run_spark_job(
        self,
        plan: ExecutionPlan,
        df: pd.DataFrame,
        ctx: PipelineExecutionContext,
        tracker_run_id: str | None = None,
    ) -> None:
        """
        Submit a Spark batch job for the pipeline's processing + storage steps.

        Workflow
        --------
        1. Stage the post-source DataFrame to HDFS when the source has no file
           path (e.g. api_source). Temp files are deleted in the finally block
           regardless of outcome (carry-over b).
        2. Collect the processing operations from the plan's processor steps
           back into a config dict for ``batch_processing_job.py``.
        3. Normalise the output path to HDFS (shared filesystem rule).
        4. Submit via ``SparkJobSubmitter``, passing ``tracker_run_id`` so the
           subprocess PID is registered for cancel support.
        5. Emit ``spark_start`` / ``spark_done`` progress events.

        Parameters
        ----------
        plan : ExecutionPlan
            The full execution plan.
        df : pandas.DataFrame
            Post-source, post-Kafka DataFrame to process.
        ctx : PipelineExecutionContext
            Execution context for logging and metrics.
        tracker_run_id : str | None
            When provided, emits spark_start/spark_done events and registers
            the subprocess PID with the tracker for cancel support.

        Raises
        ------
        PipelineExecutionError
            If the Spark job fails or times out.
        """
        from app.spark.exceptions import SparkJobFailedError, SparkJobTimeoutError

        _progress_tracker.emit(tracker_run_id, "spark_start", {})
        ctx.log_info("spark_job", "Submitting batch Spark job for processing steps.")
        t0 = time.perf_counter()

        hdfs_tmp_input: str | None = None   # HDFS path created for non-file sources
        output_path: str | None = None
        try:
            source_step = next(s for s in plan.steps if s.step_type == "source")
            input_source: str | None = source_step.config.get("path")

            if not input_source:
                # Source has no file path (e.g. api_source, kafka_source).
                # The executor has already read data into *df* via the source plugin.
                # We must stage *df* to HDFS so that Spark workers can read it —
                # the backend's local /tmp/ is invisible to spark-worker containers.
                hdfs_tmp_input = f"hdfs:///tmp/spark-input-{uuid.uuid4().hex[:8]}.csv"
                _upload_df_to_hdfs(df, hdfs_tmp_input)
                raw_input_path = hdfs_tmp_input.replace("hdfs://", "")
                if not raw_input_path.startswith("/"):
                    raw_input_path = f"/{raw_input_path}"
                input_source = f"hdfs://namenode:9000{raw_input_path}"
                ctx.log_info(
                    "spark_job",
                    f"Staged non-file source DataFrame ({len(df)} rows) "
                    f"to HDFS at {input_source}.",
                )

            # ── Collect operations from plan ───────────────────────────────────
            operations = [
                step.config
                for step in plan.steps
                if step.step_type == "processor"
            ]

            # ── Determine output path ──────────────────────────────────────────
            storage_step = next(
                (s for s in plan.steps if s.step_type == "storage"), None
            )
            raw_path = (
                storage_step.config.get("path", f"/tmp/spark-output-{ctx.run_id}")
                if storage_step
                else f"/tmp/spark-output-{ctx.run_id}"
            )
            is_hdfs_storage = (
                storage_step is not None
                and storage_step.plugin_type == "hdfs"
            )

            if raw_path.startswith("hdfs://"):
                # Already a qualified HDFS URI — use as-is
                output_path = raw_path
            elif is_hdfs_storage and raw_path.startswith("/"):
                # hdfs plugin with an absolute path — qualify it
                output_path = f"hdfs://namenode:9000{raw_path}"
            else:
                # Local path (local plugin, /tmp fallback, or any non-HDFS path).
                # Spark workers cannot access the backend's local filesystem, so
                # we redirect to HDFS preserving the original path component.
                # Rule: in Spark mode, ALL outputs must be on a shared filesystem.
                if raw_path.startswith("/"):
                    output_path = f"hdfs://namenode:9000{raw_path}"
                else:
                    output_path = f"hdfs://namenode:9000/tmp/spark-output-{ctx.run_id}"
                ctx.log_info(
                    "spark_job",
                    f"Redirected local storage path '{raw_path}' to HDFS "
                    f"'{output_path}' (Spark workers cannot see backend-local paths).",
                )

            processing_config = {"processing": {"operations": operations}}

            # ── Submit job ─────────────────────────────────────────────────────
            submitter = SparkJobSubmitter()

            async def _submit() -> dict[str, Any]:
                return await submitter.submit_batch_job(
                    input_source=input_source,
                    output_path=output_path,
                    processing_config=processing_config,
                    run_id=tracker_run_id,   # register PID for cancel
                )

            result = asyncio.run(_submit())

            duration_ms = round((time.perf_counter() - t0) * 1000, 2)
            ctx.record_stage(
                "spark_job",
                {
                    "input_rows": result.get("input_rows"),
                    "output_rows": result.get("output_rows"),
                    "duration_seconds": result.get("duration_seconds"),
                    "output_path": output_path,
                    "actual_output_path": output_path,
                    "duration_ms": duration_ms,
                },
            )
            ctx.log_info(
                "spark_job",
                f"Spark job completed: {result.get('input_rows')} → "
                f"{result.get('output_rows')} rows in {result.get('duration_seconds')}s. "
                f"Output: {output_path}",
            )
            _progress_tracker.emit(
                tracker_run_id,
                "spark_done",
                {
                    "input_rows": result.get("input_rows"),
                    "output_rows": result.get("output_rows"),
                    "output_path": output_path,
                    "actual_output_path": output_path,
                    "duration_ms": duration_ms,
                },
            )

        except (SparkJobFailedError, SparkJobTimeoutError) as exc:
            spark_step = ExecutionStep(
                name="spark_job",
                step_type="spark",
                plugin_type="spark",
                plugin=None,
                config={},
            )
            if hasattr(exc, "stderr") and exc.stderr:
                logger.error(
                    "Spark job stderr:\n%s", "\n".join(exc.stderr.splitlines()[-30:])
                )
            self._handle_failure(spark_step, ctx, exc)
        except Exception as exc:
            spark_step = ExecutionStep(
                name="spark_job",
                step_type="spark",
                plugin_type="spark",
                plugin=None,
                config={},
            )
            self._handle_failure(spark_step, ctx, exc)
        finally:
            # Carry-over (b): always delete temporary HDFS input files to prevent
            # orphaned /tmp/spark-input-*.csv files on the namenode.
            if hdfs_tmp_input is not None:
                _delete_hdfs_path(hdfs_tmp_input)
            
            # Step 7: Delete fallback output on fail/cancel
            if ctx.status != "success":
                fallback_path = f"/tmp/spark-output-{ctx.run_id}"
                current_out = output_path or ""
                if fallback_path in current_out:
                    _delete_hdfs_path(current_out, recursive=True)


    # ── Error handling ─────────────────────────────────────────────────────────

    def _handle_failure(
        self,
        step: ExecutionStep,
        ctx: PipelineExecutionContext,
        exc: Exception,
    ) -> None:
        """
        Record the failure in the context and raise :class:`PipelineExecutionError`.

        This method **always raises** — the ``-> None`` return type is a
        typing convenience so callers don't need ``raise self._handle_failure()``.
        """
        msg = f"Step '{step.name}' failed: {type(exc).__name__}: {exc}"
        ctx.log_error(step.name, msg)
        ctx.status = "failed"
        from app.metrics import pipeline_errors_total
        pipeline_errors_total.labels(step.name).inc()
        logger.error("Pipeline step '%s' failed: %s", step.name, exc, exc_info=True)
        raise PipelineExecutionError(
            message=msg,
            step_name=step.name,
            context=ctx,
            cause=exc,
        ) from exc
