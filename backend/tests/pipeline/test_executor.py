"""
tests/pipeline/test_executor.py
---------------------------------
End-to-end and unit tests for :class:`~app.pipeline.executor.PipelineExecutor`.

Covers:
* Full pipeline execution (CSV → remove_nulls → remove_duplicates → filter →
  select_columns → aggregate → local Parquet) — pandas path (no Spark cluster needed).
* PipelineExecutionContext is populated with run_id, status="success", and
  one metrics entry per step.
* A deliberately broken pipeline (processor raises) results in
  PipelineExecutionError with the partial context and logs preserved.
* The error log entry references the failing step name.
* Spark branch: mocked SparkJobSubmitter verifies executor routes correctly and
  records spark_job metrics / raises PipelineExecutionError on failure.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import pytest

import app.plugins  # noqa: F401 — ensures all plugins are registered

from app.pipeline.config_models import PipelineConfig
from app.pipeline.context import PipelineExecutionContext
from app.pipeline.executor import PipelineExecutionError, PipelineExecutor
from app.pipeline.generator import ExecutionPlan, ExecutionStep, PipelineGenerator
from app.plugins.base import ProcessorPlugin
from app.plugins.registry import registry

SAMPLE_DATA_DIR = Path(__file__).parent.parent.parent / "sample-data"


# ── Helpers ────────────────────────────────────────────────────────────────────


def _make_sales_config(storage_path: str) -> PipelineConfig:
    """
    Build a runnable sales pipeline config that reads the sample CSV and
    writes to *storage_path* (a local temp directory).

    No ``processing_engine`` is set so the pandas in-process path is used —
    fast and independent of any Spark cluster.
    """
    return PipelineConfig.model_validate(
        {
            "pipeline": {"name": "exec-test-sales", "version": "1.0"},
            "source": {
                "type": "csv",
                "path": str(SAMPLE_DATA_DIR / "sales_sample.csv"),
            },
            "schema": [
                {"name": "transaction_id", "type": "string"},
                {"name": "amount", "type": "double"},
                {"name": "customer_id", "type": "string"},
                {"name": "product_id", "type": "string"},
                {"name": "sale_date", "type": "string"},
            ],
            "processing": {
                "operations": [
                    {"type": "remove_nulls"},
                    {"type": "remove_duplicates"},
                    {"type": "filter", "condition": "amount > 0"},
                    {
                        "type": "select_columns",
                        "columns": ["transaction_id", "amount", "customer_id", "sale_date"],
                    },
                    {
                        "type": "rename_columns",
                        "mapping": {"transaction_id": "txn_id", "sale_date": "date"},
                    },
                    {"type": "type_conversion", "column": "amount", "to_type": "double"},
                    {
                        "type": "aggregate",
                        "group_by": "customer_id",
                        "operation": "sum",
                        "field": "amount",
                    },
                ]
            },
            "streaming": {"enabled": False},
            # No processing_engine → pandas in-process path (safe for unit tests)
            "storage": {"type": "local", "path": storage_path},
        }
    )


# ── Shared fixtures ────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def generator() -> PipelineGenerator:
    return PipelineGenerator()


@pytest.fixture(scope="module")
def executor() -> PipelineExecutor:
    return PipelineExecutor()


# ── Full end-to-end execution ──────────────────────────────────────────────────


class TestFullSalesPipelineExecution:
    """Full CSV → processors → Parquet execution with real sample data (pandas path)."""

    @pytest.fixture()
    def context(
        self,
        tmp_path: Path,
        generator: PipelineGenerator,
        executor: PipelineExecutor,
    ) -> PipelineExecutionContext:
        """Execute the sales pipeline once; produces a fresh context per test."""
        config = _make_sales_config(str(tmp_path / "output"))
        plan = generator.generate(config)
        return executor.execute(plan)

    def test_status_is_success(self, context: PipelineExecutionContext) -> None:
        assert context.status == "success"

    def test_run_id_is_set_and_non_empty(self, context: PipelineExecutionContext) -> None:
        assert context.run_id
        assert len(context.run_id) == 36  # UUID4 has 36 chars including hyphens

    def test_parquet_file_was_written(
        self, tmp_path: Path, context: PipelineExecutionContext
    ) -> None:
        storage_meta = context.metrics.get("storage:local", {})
        written_path = storage_meta.get("path", "")
        assert written_path, "storage:local metric must include 'path'"
        assert Path(written_path).exists()

    def test_output_row_count_less_than_input(
        self, tmp_path: Path, context: PipelineExecutionContext
    ) -> None:
        """
        Input CSV has 20 rows.  After remove_nulls (→17) + remove_duplicates (→15)
        + aggregate (→4 customers), final count must be < 20.
        """
        storage_meta = context.metrics.get("storage:local", {})
        written_path = storage_meta.get("path", "")
        df_out = pd.read_parquet(written_path)
        input_rows = 20  # sales_sample.csv has 20 rows
        assert len(df_out) < input_rows

    def test_aggregate_produces_one_row_per_customer(
        self, tmp_path: Path, context: PipelineExecutionContext
    ) -> None:
        """After aggregation there should be exactly 4 unique customers."""
        storage_meta = context.metrics.get("storage:local", {})
        written_path = storage_meta.get("path", "")
        df_out = pd.read_parquet(written_path)
        assert len(df_out) == 4

    def test_metrics_has_entry_for_every_step(
        self, context: PipelineExecutionContext
    ) -> None:
        """Every step (1 source + 7 processors + 1 storage) must have a metric entry."""
        expected_step_count = 9  # 1 + 7 + 1
        assert len(context.metrics) == expected_step_count, (
            f"Expected {expected_step_count} metric entries, got: {list(context.metrics.keys())}"
        )

    def test_source_metric_has_rows_out(
        self, context: PipelineExecutionContext
    ) -> None:
        source_metric = context.metrics.get("source:csv")
        assert source_metric is not None
        assert source_metric["rows_out"] == 20

    def test_remove_nulls_metric_shows_fewer_rows(
        self, context: PipelineExecutionContext
    ) -> None:
        metric = context.metrics.get("processor:remove_nulls:0")
        assert metric is not None
        assert metric["rows_in"] == 20
        assert metric["rows_out"] == 17  # 3 null rows removed

    def test_remove_duplicates_metric_shows_fewer_rows(
        self, context: PipelineExecutionContext
    ) -> None:
        metric = context.metrics.get("processor:remove_duplicates:1")
        assert metric is not None
        assert metric["rows_in"] == 17
        assert metric["rows_out"] == 15  # 2 duplicate rows removed

    def test_storage_metric_has_rows_written(
        self, context: PipelineExecutionContext
    ) -> None:
        metric = context.metrics.get("storage:local")
        assert metric is not None
        assert "rows_written" in metric
        assert metric["rows_written"] == 4

    def test_all_metrics_have_duration_ms(
        self, context: PipelineExecutionContext
    ) -> None:
        for step_name, metric in context.metrics.items():
            assert "duration_ms" in metric, (
                f"Step '{step_name}' is missing 'duration_ms' in metrics"
            )

    def test_logs_is_non_empty(self, context: PipelineExecutionContext) -> None:
        assert len(context.logs) > 0

    def test_all_log_entries_have_required_fields(
        self, context: PipelineExecutionContext
    ) -> None:
        required_fields = {"timestamp", "level", "step", "message"}
        for entry in context.logs:
            assert required_fields.issubset(entry.keys()), (
                f"Log entry missing fields: {required_fields - entry.keys()}. Entry: {entry}"
            )

    def test_no_error_level_logs_on_success(
        self, context: PipelineExecutionContext
    ) -> None:
        error_logs = [e for e in context.logs if e["level"] == "ERROR"]
        assert error_logs == []

    def test_to_summary_is_json_serialisable(
        self, context: PipelineExecutionContext
    ) -> None:
        import json
        summary = context.to_summary()
        serialised = json.dumps(summary)
        parsed = json.loads(serialised)
        assert parsed["status"] == "success"
        assert parsed["run_id"] == context.run_id


# ── Broken pipeline — executor catches step failures ───────────────────────────


class TestBrokenPipelineExecution:
    """A failing processor step must produce PipelineExecutionError with partial context."""

    @pytest.fixture()
    def broken_plan(self, tmp_path: Path) -> ExecutionPlan:
        """Build a plan with a real source but a processor that always raises."""

        class _ExplodingProcessor(ProcessorPlugin):
            plugin_type = "_test_exploding"

            def process(self, df: pd.DataFrame, config: dict) -> pd.DataFrame:
                raise RuntimeError("This processor always fails — by design.")

        from app.plugins.sources.csv_source import CsvSource
        from app.plugins.storage.local_storage import LocalStorage

        source_step = ExecutionStep(
            name="source:csv",
            step_type="source",
            plugin_type="csv",
            plugin=CsvSource(),
            config={"path": str(SAMPLE_DATA_DIR / "sales_sample.csv")},
        )
        bad_processor_step = ExecutionStep(
            name="processor:_test_exploding:0",
            step_type="processor",
            plugin_type="_test_exploding",
            plugin=_ExplodingProcessor(),
            config={},
        )
        storage_step = ExecutionStep(
            name="storage:local",
            step_type="storage",
            plugin_type="local",
            plugin=LocalStorage(),
            config={"path": str(tmp_path / "broken_output")},
        )

        return ExecutionPlan(
            pipeline_name="broken-test-pipeline",
            pipeline_version="1.0",
            steps=[source_step, bad_processor_step, storage_step],
        )

    def test_broken_processor_raises_pipeline_execution_error(
        self, broken_plan: ExecutionPlan, executor: PipelineExecutor
    ) -> None:
        with pytest.raises(PipelineExecutionError):
            executor.execute(broken_plan)

    def test_error_carries_partial_context(
        self, broken_plan: ExecutionPlan, executor: PipelineExecutor
    ) -> None:
        with pytest.raises(PipelineExecutionError) as exc_info:
            executor.execute(broken_plan)
        ctx = exc_info.value.context
        assert ctx is not None
        assert isinstance(ctx, PipelineExecutionContext)

    def test_partial_context_status_is_failed(
        self, broken_plan: ExecutionPlan, executor: PipelineExecutor
    ) -> None:
        with pytest.raises(PipelineExecutionError) as exc_info:
            executor.execute(broken_plan)
        assert exc_info.value.context.status == "failed"

    def test_completed_source_step_metric_is_preserved(
        self, broken_plan: ExecutionPlan, executor: PipelineExecutor
    ) -> None:
        """Source ran successfully before the processor failed — its metric must survive."""
        with pytest.raises(PipelineExecutionError) as exc_info:
            executor.execute(broken_plan)
        ctx = exc_info.value.context
        assert "source:csv" in ctx.metrics

    def test_error_log_entry_references_failing_step(
        self, broken_plan: ExecutionPlan, executor: PipelineExecutor
    ) -> None:
        with pytest.raises(PipelineExecutionError) as exc_info:
            executor.execute(broken_plan)
        ctx = exc_info.value.context
        error_logs = [e for e in ctx.logs if e["level"] == "ERROR"]
        assert len(error_logs) >= 1
        assert any("_test_exploding" in e["step"] for e in error_logs)

    def test_error_log_message_includes_exception_text(
        self, broken_plan: ExecutionPlan, executor: PipelineExecutor
    ) -> None:
        with pytest.raises(PipelineExecutionError) as exc_info:
            executor.execute(broken_plan)
        ctx = exc_info.value.context
        messages = " ".join(e["message"] for e in ctx.logs if e["level"] == "ERROR")
        assert "always fails" in messages or "RuntimeError" in messages

    def test_error_step_name_is_set(
        self, broken_plan: ExecutionPlan, executor: PipelineExecutor
    ) -> None:
        with pytest.raises(PipelineExecutionError) as exc_info:
            executor.execute(broken_plan)
        assert exc_info.value.step_name == "processor:_test_exploding:0"

    def test_original_cause_is_preserved(
        self, broken_plan: ExecutionPlan, executor: PipelineExecutor
    ) -> None:
        with pytest.raises(PipelineExecutionError) as exc_info:
            executor.execute(broken_plan)
        assert isinstance(exc_info.value.cause, RuntimeError)
        assert "always fails" in str(exc_info.value.cause)

# ── HDFS storage fails gracefully ─────────────────────────────────────────────


class TestHdfsStorageFailsGracefully:
    """
    Phase 6: HDFS storage errors (connection failures, write errors) must
    be caught by the executor and converted to PipelineExecutionError — not
    propagated as naked HdfsWriteError or HdfsConnectionError.

    HdfsStorageClient is mocked — no live HDFS cluster needed.
    """

    def test_hdfs_storage_raises_pipeline_execution_error(
        self, tmp_path: Path, generator: PipelineGenerator, executor: PipelineExecutor
    ) -> None:
        """When the HDFS write fails, the executor must raise PipelineExecutionError."""
        from unittest.mock import patch
        from app.storage.exceptions import HdfsWriteError

        config = PipelineConfig.model_validate(
            {
                "pipeline": {"name": "hdfs-test", "version": "1.0"},
                "source": {
                    "type": "csv",
                    "path": str(SAMPLE_DATA_DIR / "sales_sample.csv"),
                },
                "schema": [
                    {"name": "transaction_id", "type": "string"},
                    {"name": "amount", "type": "double"},
                    {"name": "customer_id", "type": "string"},
                    {"name": "product_id", "type": "string"},
                    {"name": "sale_date", "type": "string"},
                ],
                "processing": {"operations": [{"type": "remove_nulls"}]},
                "streaming": {"enabled": False},
                # No processing_engine → pandas path; HDFS write is mocked to fail
                "storage": {"type": "hdfs", "path": "/hdfs/data/output"},
            }
        )
        plan = generator.generate(config)

        # Mock the HDFS client so we don't need a real cluster in unit tests
        with patch("app.plugins.storage.hdfs_storage.HdfsStorageClient") as MockClient:
            MockClient.return_value.write_dataframe.side_effect = HdfsWriteError(
                "NameNode not reachable", hdfs_path="/hdfs/data/output"
            )
            with pytest.raises(PipelineExecutionError):
                executor.execute(plan)


# ── Spark branch — unit tests with mocked submitter ───────────────────────────


class TestSparkBranchWithMockedSubmitter:
    """
    Unit tests for the Spark execution branch.

    ``SparkJobSubmitter`` is mocked — no ``spark-submit`` is invoked.
    Verifies routing, metrics recording, and failure handling.
    """

    def _make_plan_with_spark(self, tmp_path: Path) -> ExecutionPlan:
        """Build a minimal ExecutionPlan with processing_engine_type='spark'."""
        from app.plugins.sources.csv_source import CsvSource
        from app.plugins.storage.local_storage import LocalStorage

        source_step = ExecutionStep(
            name="source:csv",
            step_type="source",
            plugin_type="csv",
            plugin=CsvSource(),
            config={"path": str(SAMPLE_DATA_DIR / "sales_sample.csv")},
        )
        storage_step = ExecutionStep(
            name="storage:local",
            step_type="storage",
            plugin_type="local",
            plugin=LocalStorage(),
            config={"path": str(tmp_path / "spark-output")},
        )
        return ExecutionPlan(
            pipeline_name="spark-test",
            pipeline_version="1.0",
            steps=[source_step, storage_step],
            processing_engine_type="spark",
        )

    def test_spark_job_metrics_recorded_on_success(
        self, tmp_path: Path, executor: PipelineExecutor
    ) -> None:
        """When the Spark job succeeds, spark_job metrics must appear in the context."""
        from unittest.mock import AsyncMock, patch

        mock_result = {
            "input_rows": 20,
            "output_rows": 17,
            "duration_seconds": 5.2,
            "status": "success",
        }

        plan = self._make_plan_with_spark(tmp_path)
        with patch("app.pipeline.executor.SparkJobSubmitter") as MockSubmitter:
            instance = MockSubmitter.return_value
            instance.submit_batch_job = AsyncMock(return_value=mock_result)
            ctx = executor.execute(plan)

        assert ctx.status == "success"
        assert "spark_job" in ctx.metrics
        assert ctx.metrics["spark_job"]["input_rows"] == 20
        assert ctx.metrics["spark_job"]["output_rows"] == 17

    def test_spark_job_failure_raises_pipeline_execution_error(
        self, tmp_path: Path, executor: PipelineExecutor
    ) -> None:
        """A failing Spark job must raise PipelineExecutionError."""
        from unittest.mock import AsyncMock, patch
        from app.spark.exceptions import SparkJobFailedError

        plan = self._make_plan_with_spark(tmp_path)
        with patch("app.pipeline.executor.SparkJobSubmitter") as MockSubmitter:
            instance = MockSubmitter.return_value
            instance.submit_batch_job = AsyncMock(
                side_effect=SparkJobFailedError(
                    "Spark failed", stdout="", stderr="OOM: Java heap space"
                )
            )
            with pytest.raises(PipelineExecutionError) as exc_info:
                executor.execute(plan)

        assert exc_info.value.step_name == "spark_job"
        assert exc_info.value.context.status == "failed"

    def test_spark_job_cleans_up_hdfs_tmp_input(
        self, tmp_path: Path, executor: PipelineExecutor
    ) -> None:
        """When the source requires staging to HDFS, the tmp file must be deleted."""
        from unittest.mock import AsyncMock, patch
        from app.plugins.sources.api_source import ApiSource

        # Make a plan with a non-path source (e.g. API) to trigger HDFS staging
        source_step = ExecutionStep(
            name="source:api",
            step_type="source",
            plugin_type="api",
            plugin=ApiSource(),
            config={"endpoint": "http://example.com/data"},
        )
        plan = self._make_plan_with_spark(tmp_path)
        plan.steps[0] = source_step
        
        mock_result = {
            "input_rows": 5,
            "output_rows": 5,
            "duration_seconds": 1.0,
            "status": "success",
        }

        with patch("app.pipeline.executor.SparkJobSubmitter") as MockSubmitter, \
             patch("app.pipeline.executor._upload_df_to_hdfs") as mock_upload, \
             patch("app.pipeline.executor._delete_hdfs_path") as mock_delete, \
             patch.object(ApiSource, "read", return_value=pd.DataFrame({"a": [1]})):
            
            instance = MockSubmitter.return_value
            instance.submit_batch_job = AsyncMock(return_value=mock_result)
            mock_upload.return_value = "hdfs:///tmp/spark-input-123.csv"
            
            ctx = executor.execute(plan)

        assert ctx.status == "success"
        mock_upload.assert_called_once()
        mock_delete.assert_called_once()
        deleted_path = mock_delete.call_args[0][0]
        assert deleted_path.startswith("hdfs:///tmp/spark-input-")
        assert deleted_path.endswith(".csv")

    def test_spark_job_cleans_up_hdfs_tmp_input_on_failure(
        self, tmp_path: Path, executor: PipelineExecutor
    ) -> None:
        """When the spark job fails, the tmp file must still be deleted."""
        from unittest.mock import AsyncMock, patch
        from app.plugins.sources.api_source import ApiSource
        from app.spark.exceptions import SparkJobFailedError

        source_step = ExecutionStep(
            name="source:api",
            step_type="source",
            plugin_type="api",
            plugin=ApiSource(),
            config={"endpoint": "http://example.com/data"},
        )
        plan = self._make_plan_with_spark(tmp_path)
        plan.steps[0] = source_step
        
        with patch("app.pipeline.executor.SparkJobSubmitter") as MockSubmitter, \
             patch("app.pipeline.executor._upload_df_to_hdfs") as mock_upload, \
             patch("app.pipeline.executor._delete_hdfs_path") as mock_delete, \
             patch.object(ApiSource, "read", return_value=pd.DataFrame({"a": [1]})):
            
            instance = MockSubmitter.return_value
            instance.submit_batch_job = AsyncMock(side_effect=SparkJobFailedError("failed", "", ""))
            mock_upload.return_value = "hdfs:///tmp/spark-input-fail.csv"
            
            with pytest.raises(PipelineExecutionError):
                executor.execute(plan)

        mock_upload.assert_called_once()
        mock_delete.assert_called_once()
        deleted_path = mock_delete.call_args[0][0]
        assert deleted_path.startswith("hdfs:///tmp/spark-input-")
        assert deleted_path.endswith(".csv")

    def test_spark_job_cleans_up_hdfs_tmp_input_on_cancel(
        self, tmp_path: Path, executor: PipelineExecutor
    ) -> None:
        """When the spark job is cancelled, the tmp file must still be deleted."""
        from unittest.mock import AsyncMock, patch
        from app.plugins.sources.api_source import ApiSource
        from app.spark.exceptions import SparkJobFailedError

        source_step = ExecutionStep(
            name="source:api",
            step_type="source",
            plugin_type="api",
            plugin=ApiSource(),
            config={"endpoint": "http://example.com/data"},
        )
        plan = self._make_plan_with_spark(tmp_path)
        plan.steps[0] = source_step

        with patch("app.pipeline.executor.SparkJobSubmitter") as MockSubmitter, \
             patch("app.pipeline.executor._upload_df_to_hdfs") as mock_upload, \
             patch("app.pipeline.executor._delete_hdfs_path") as mock_delete, \
             patch.object(ApiSource, "read", return_value=pd.DataFrame({"a": [1]})):

            instance = MockSubmitter.return_value
            # Cancellation surfaces as a SparkJobFailedError or abort
            instance.submit_batch_job = AsyncMock(
                side_effect=SparkJobFailedError("Job was cancelled by user signal", "", "")
            )
            mock_upload.return_value = "hdfs:///tmp/spark-input-cancel.csv"

            with pytest.raises(PipelineExecutionError):
                executor.execute(plan)

        mock_upload.assert_called_once()
        mock_delete.assert_called_once()
        deleted_path = mock_delete.call_args[0][0]
        assert deleted_path.startswith("hdfs:///tmp/spark-input-")
        assert deleted_path.endswith(".csv")

    def test_spark_job_cleans_up_fallback_output_on_failure(
        self, tmp_path: Path, executor: PipelineExecutor
    ) -> None:
        """When using fallback output path and the job fails, fallback directory is deleted."""
        from unittest.mock import AsyncMock, patch
        from app.spark.exceptions import SparkJobFailedError

        plan = self._make_plan_with_spark(tmp_path)
        # Remove storage step so it uses fallback output path
        plan.steps = [s for s in plan.steps if s.step_type != "storage"]

        with patch("app.pipeline.executor.SparkJobSubmitter") as MockSubmitter, \
             patch("app.pipeline.executor._delete_hdfs_path") as mock_delete:
            
            instance = MockSubmitter.return_value
            instance.submit_batch_job = AsyncMock(side_effect=SparkJobFailedError("failed", "", ""))

            with pytest.raises(PipelineExecutionError):
                executor.execute(plan)

        mock_delete.assert_called_once()
        deleted_path, kwargs = mock_delete.call_args[0][0], mock_delete.call_args[1]
        assert "/tmp/spark-output-" in deleted_path
        assert kwargs.get("recursive") is True

    def test_pandas_path_used_when_no_engine_set(
        self, tmp_path: Path, executor: PipelineExecutor
    ) -> None:
        """When processing_engine_type is None, the pandas path must run (no submitter called)."""
        from unittest.mock import patch

        config = _make_sales_config(str(tmp_path / "pandas-output"))
        plan = generator() if False else PipelineGenerator().generate(config)
        assert plan.processing_engine_type is None

        # Submitter must NOT be called — patch it to fail if it is
        with patch("app.pipeline.executor.SparkJobSubmitter") as MockSubmitter:
            ctx = executor.execute(plan)
            MockSubmitter.assert_not_called()

        assert ctx.status == "success"
        assert "storage:local" in ctx.metrics

    def test_spark_prerun_hdfs_delete_runs_on_overwrite(
        self, tmp_path: Path, executor: PipelineExecutor
    ) -> None:
        """When write mode is overwrite, pre-run HDFS delete is executed on the run output path."""
        from unittest.mock import AsyncMock, patch

        plan = self._make_plan_with_spark(tmp_path)
        storage_step = next(s for s in plan.steps if s.step_type == "storage")
        storage_step.config["path"] = "/data/custom_sales"
        storage_step.config["mode"] = "overwrite"

        mock_result = {
            "input_rows": 10,
            "output_rows": 10,
            "duration_seconds": 1.0,
            "status": "success",
        }

        with patch("app.pipeline.executor.SparkJobSubmitter") as MockSubmitter, \
             patch("app.pipeline.executor.HdfsStorageClient") as MockHdfsClient:
            mock_client_instance = MockHdfsClient.return_value
            mock_client_instance.path_exists.return_value = True
            mock_client_instance.content_summary.return_value = {"fileCount": 1, "length": 100}
            mock_client_instance._normalize_hdfs_path.side_effect = lambda p: p.replace("hdfs://namenode:9000", "")

            instance = MockSubmitter.return_value
            instance.submit_batch_job = AsyncMock(return_value=mock_result)

            ctx = executor.execute(plan)

        assert ctx.status == "success"
        mock_client_instance.delete.assert_called_with("hdfs://namenode:9000/data/custom_sales", recursive=True)

    def test_spark_prerun_hdfs_delete_skipped_on_append(
        self, tmp_path: Path, executor: PipelineExecutor
    ) -> None:
        """When write mode is append, pre-run HDFS delete must NEVER be called."""
        from unittest.mock import AsyncMock, patch

        plan = self._make_plan_with_spark(tmp_path)
        storage_step = next(s for s in plan.steps if s.step_type == "storage")
        storage_step.config["path"] = "/data/custom_sales"
        storage_step.config["mode"] = "append"

        mock_result = {
            "input_rows": 10,
            "output_rows": 10,
            "duration_seconds": 1.0,
            "status": "success",
        }

        with patch("app.pipeline.executor.SparkJobSubmitter") as MockSubmitter, \
             patch("app.pipeline.executor.HdfsStorageClient") as MockHdfsClient:
            mock_client_instance = MockHdfsClient.return_value
            mock_client_instance.path_exists.return_value = True
            mock_client_instance.content_summary.return_value = {"fileCount": 1, "length": 100}

            instance = MockSubmitter.return_value
            instance.submit_batch_job = AsyncMock(return_value=mock_result)

            ctx = executor.execute(plan)

        assert ctx.status == "success"
        mock_client_instance.delete.assert_not_called()

    def test_spark_prerun_hdfs_delete_safeguards_base_paths(
        self, tmp_path: Path, executor: PipelineExecutor
    ) -> None:
        """Safeguards must prevent deleting base root directories like /data or /tmp."""
        from unittest.mock import AsyncMock, patch

        plan = self._make_plan_with_spark(tmp_path)
        storage_step = next(s for s in plan.steps if s.step_type == "storage")
        storage_step.config["path"] = "/data"
        storage_step.config["mode"] = "overwrite"

        mock_result = {
            "input_rows": 10,
            "output_rows": 10,
            "duration_seconds": 1.0,
            "status": "success",
        }

        with patch("app.pipeline.executor.SparkJobSubmitter") as MockSubmitter, \
             patch("app.pipeline.executor.HdfsStorageClient") as MockHdfsClient:
            mock_client_instance = MockHdfsClient.return_value
            mock_client_instance.path_exists.return_value = True
            mock_client_instance.content_summary.return_value = {"fileCount": 1, "length": 100}
            mock_client_instance._normalize_hdfs_path.side_effect = lambda p: p.replace("hdfs://namenode:9000", "")

            instance = MockSubmitter.return_value
            instance.submit_batch_job = AsyncMock(return_value=mock_result)

            ctx = executor.execute(plan)

        assert ctx.status == "success"
        mock_client_instance.delete.assert_not_called()

