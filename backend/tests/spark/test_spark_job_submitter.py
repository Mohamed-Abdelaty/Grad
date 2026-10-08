"""
backend/tests/spark/test_spark_job_submitter.py
--------------------------------------------------
Integration tests for SparkJobSubmitter.

These tests require the full Docker Compose stack (postgres, kafka,
spark-master, spark-worker, backend) to be running.  They are marked
``@pytest.mark.integration`` and skipped automatically when the Spark
cluster is unreachable (see conftest.py).

Root cause note — local output paths in cluster mode
-----------------------------------------------------
``spark-submit`` against ``spark://spark-master:7077`` runs the driver
locally (on the backend container) but executors on the Spark worker
containers.  When the output path is a ``file://`` (local) path, Spark
writes:

* ``_SUCCESS`` → on the **driver** (backend container local FS)
* ``part-*.parquet`` → on each **executor** (worker container local FS)

The backend container therefore sees only ``_SUCCESS`` with no part files.
The fix is to use an HDFS path (``hdfs://namenode:9000/...``) which is a
shared, cluster-accessible filesystem visible to both driver and executors.
Output verification is done via the HDFS WebHDFS REST API.
"""

from __future__ import annotations

import time
import uuid

import httpx
import pytest

from app.spark.exceptions import SparkJobFailedError
from app.spark.spark_job_submitter import SparkJobSubmitter
from app.core.config import get_settings

# Path to the sample CSV inside the backend container (mounted from host)
_SAMPLE_CSV = "/app/sample-data/sales_sample.csv"

# HDFS base URI for Spark jobs (cluster-accessible shared filesystem)
_HDFS_BASE = "hdfs://namenode:9000"


def _hdfs_webhdfs_url(path: str) -> str:
    """Return the WebHDFS REST URL for listing *path*."""
    settings = get_settings()
    # HDFS_NAMENODE_URL is http://namenode:9870
    return f"{settings.HDFS_NAMENODE_URL}/webhdfs/v1{path}?op=LISTSTATUS&user.name={settings.HDFS_USER}"


def _list_hdfs_dir(path: str) -> list[str]:
    """
    List file names in an HDFS directory using the WebHDFS REST API.

    Returns an empty list if the directory does not exist or has no files.
    """
    url = _hdfs_webhdfs_url(path)
    try:
        resp = httpx.get(url, timeout=10.0, follow_redirects=True)
        if resp.status_code == 404:
            return []
        resp.raise_for_status()
        statuses = resp.json().get("FileStatuses", {}).get("FileStatus", [])
        return [s["pathSuffix"] for s in statuses]
    except Exception:
        return []


@pytest.mark.integration
class TestSparkJobSubmitterIntegration:
    """Real cluster integration tests for SparkJobSubmitter."""

    @pytest.fixture
    def submitter(self) -> SparkJobSubmitter:
        """Return a fresh SparkJobSubmitter instance."""
        return SparkJobSubmitter()

    @pytest.fixture
    def hdfs_output_path(self) -> str:
        """
        Return a unique HDFS path for Parquet output.

        Uses HDFS so that both the Spark driver (backend container) and
        executors (worker containers) can access the same shared filesystem.
        The path is unique per test run to avoid cross-test interference.
        """
        return f"/tmp/spark_test_{uuid.uuid4().hex[:8]}"

    @pytest.mark.asyncio
    async def test_batch_job_produces_parquet_output(
        self, submitter: SparkJobSubmitter, hdfs_output_path: str
    ) -> None:
        """
        Submitting a real batch job must write Parquet output and return
        correct row counts.

        Output path is an HDFS URI so that Spark executors (which run on
        worker containers) write to the shared cluster filesystem rather
        than their local container-local ``/tmp/``.
        """
        processing_config = {
            "processing": {
                "operations": [
                    {"type": "remove_nulls"},
                    {"type": "remove_duplicates"},
                ]
            }
        }

        hdfs_uri = f"{_HDFS_BASE}{hdfs_output_path}"
        result = await submitter.submit_batch_job(
            input_source=_SAMPLE_CSV,
            output_path=hdfs_uri,
            processing_config=processing_config,
        )

        assert result["status"] == "success"
        assert result["input_rows"] > 0, "Expected at least one input row"
        assert result["output_rows"] >= 0
        assert result["output_rows"] <= result["input_rows"]
        assert isinstance(result["duration_seconds"], (int, float))
        assert result["duration_seconds"] > 0

        # Wait briefly for HDFS replication to settle, then verify via WebHDFS.
        time.sleep(2)
        files = _list_hdfs_dir(hdfs_output_path)
        parquet_files = [f for f in files if f.endswith(".parquet")]
        assert len(parquet_files) > 0, (
            f"Expected Parquet part files in HDFS {hdfs_output_path}, "
            f"got: {files}. "
            f"Job result was: {result}"
        )

    @pytest.mark.asyncio
    async def test_batch_job_with_filter_reduces_row_count(
        self, submitter: SparkJobSubmitter, hdfs_output_path: str
    ) -> None:
        """
        A filter that excludes some but not all rows must:
        1. Return ``output_rows > 0`` and ``output_rows < input_rows``.
        2. Write real Parquet part files to HDFS (non-zero output always produces parts).

        Filter: ``amount > 150``
        sales_sample.csv has 20 rows.  Rows with NULL amount (3) and amounts
        ≤ 150 (75.25 ×2, 99.00, 89.99, 125.00, 110.00) are excluded.
        Remaining: 11 rows (150.50 ×2, 200.00, 450.75, 215.00, 340.00, 180.00,
        240.00, 195.00, 280.00, 165.00).
        """
        # sales_sample.csv amounts and expected filtering:
        #   NULL ×3  → excluded (Spark treats NULL > 150 as false)
        #   ≤ 150    → 75.25, 75.25(dup), 99.00, 89.99, 125.00, 110.00  → 6 excluded
        #   > 150    → 11 rows pass (including the 150.50 duplicate)
        EXPECTED_INPUT_ROWS = 20
        EXPECTED_OUTPUT_ROWS = 11

        processing_config = {
            "processing": {
                "operations": [
                    {"type": "filter", "condition": "amount > 150"},
                ]
            }
        }

        hdfs_uri = f"{_HDFS_BASE}{hdfs_output_path}"
        result = await submitter.submit_batch_job(
            input_source=_SAMPLE_CSV,
            output_path=hdfs_uri,
            processing_config=processing_config,
        )

        assert result["status"] == "success"
        assert result["input_rows"] == EXPECTED_INPUT_ROWS
        assert result["output_rows"] == EXPECTED_OUTPUT_ROWS
        assert result["output_rows"] > 0, "Filter must not eliminate all rows"
        assert result["output_rows"] < result["input_rows"], (
            "Filter must reduce the row count"
        )

        # Non-zero output_rows means Spark MUST write at least one part file.
        # Verify via WebHDFS — this also confirms output is on HDFS (shared FS).
        import time
        time.sleep(2)
        files = _list_hdfs_dir(hdfs_output_path)
        parquet_files = [f for f in files if f.endswith(".parquet")]
        assert len(parquet_files) > 0, (
            f"Expected Parquet part files in HDFS {hdfs_output_path}, "
            f"got: {files}. "
            f"Job result was: {result}"
        )

    @pytest.mark.asyncio
    async def test_health_check_returns_connected(
        self, submitter: SparkJobSubmitter
    ) -> None:
        """
        health_check() must return connected=True with at least 1 worker
        when the cluster is running.
        """
        result = await submitter.health_check()
        assert result["connected"] is True, (
            f"Expected connected=True but got: {result}"
        )
        assert result["worker_count"] >= 1, (
            f"Expected at least 1 registered worker but got: {result['worker_count']}"
        )

    @pytest.mark.asyncio
    async def test_invalid_filter_raises_spark_job_failed_error(
        self, submitter: SparkJobSubmitter, hdfs_output_path: str
    ) -> None:
        """
        A filter containing a banned pattern must fail with SparkJobFailedError
        (the injection guard in filter_ops.py causes the job to exit non-zero).
        """
        processing_config = {
            "processing": {
                "operations": [
                    {"type": "filter", "condition": "__import__('os').system('id')"},
                ]
            }
        }

        with pytest.raises(SparkJobFailedError) as exc_info:
            await submitter.submit_batch_job(
                input_source=_SAMPLE_CSV,
                output_path=f"{_HDFS_BASE}{hdfs_output_path}",
                processing_config=processing_config,
            )

        err = exc_info.value
        # The exception message must include stderr content for debugging
        assert err.stderr or str(err), "Exception must carry stderr or message"
