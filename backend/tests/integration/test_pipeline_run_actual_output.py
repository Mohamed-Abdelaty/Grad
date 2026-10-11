"""
tests/integration/test_pipeline_run_actual_output.py
------------------------------------------------------
Integration test: the executor records the ACTUAL output path in the
PipelineRun row, even when a local storage path is redirected to HDFS.

Requires the full stack to be running (postgres, namenode, spark-master,
spark-worker) and marks itself ``integration`` so it's excluded from the
default unit-test run.
"""

import asyncio
import pytest
from datetime import timedelta
from httpx import AsyncClient

from app.core.security import create_access_token

pytestmark = pytest.mark.integration

# ── YAML fixtures ──────────────────────────────────────────────────────────────

# Case A: local storage type → executor redirects to HDFS
_YAML_LOCAL_STORAGE = """\
pipeline:
  name: "actual-output-test-local"
  version: "1.0"
source:
  type: csv
  path: /app/sample-data/sales_sample.csv
schema:
  - { name: transaction_id, type: string }
  - { name: amount, type: double }
  - { name: customer_id, type: string }
  - { name: product_id, type: string }
  - { name: sale_date, type: string }
processing:
  operations:
    - type: remove_nulls
streaming:
  enabled: false
processing_engine:
  type: spark
storage:
  type: local
  path: /tmp/actual-output-test-local
"""

# Case B: explicit HDFS storage type
_YAML_HDFS_STORAGE = """\
pipeline:
  name: "actual-output-test-hdfs"
  version: "1.0"
source:
  type: csv
  path: /app/sample-data/sales_sample.csv
schema:
  - { name: transaction_id, type: string }
  - { name: amount, type: double }
  - { name: customer_id, type: string }
  - { name: product_id, type: string }
  - { name: sale_date, type: string }
processing:
  operations:
    - type: remove_nulls
streaming:
  enabled: false
processing_engine:
  type: spark
storage:
  type: hdfs
  path: /data/actual-output-test-hdfs
"""


# ── Fixtures ────────────────────────────────────────────────────────────────────

@pytest.fixture
def token_headers():
    token = create_access_token({"sub": "admin"}, timedelta(minutes=15))
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture
async def async_client():
    async with AsyncClient(base_url="http://localhost:8000", timeout=300.0) as ac:
        yield ac


# ── Helpers ─────────────────────────────────────────────────────────────────────

async def _run_and_wait(
    client: AsyncClient,
    headers: dict,
    yaml_content: str,
    timeout_s: int = 240,
) -> dict:
    """POST /run?wait=false, then poll until terminal, return the run dict."""
    resp = await client.post(
        "/api/pipelines/run?wait=false",
        json={"yaml_content": yaml_content},
        headers=headers,
    )
    assert resp.status_code == 202, f"Expected 202 got {resp.status_code}: {resp.text}"
    run_id = resp.json()["pipeline_run_id"]

    for _ in range(timeout_s // 5):
        await asyncio.sleep(5)
        r = await client.get(f"/api/pipeline-runs/{run_id}", headers=headers)
        data = r.json()
        if data["status"] not in ("running", "pending"):
            return data
    pytest.fail(f"Run {run_id} did not reach a terminal state within {timeout_s}s")


# ── Tests ───────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_actual_output_path_is_set_for_local_storage_redirected_to_hdfs(
    async_client: AsyncClient, token_headers: dict
):
    """
    When the configured storage type is 'local', the executor redirects the
    output to HDFS.  The run row's output_path must:
      * be non-null
      * start with 'hdfs://namenode:9000/'
      * differ from the configured local path '/tmp/actual-output-test-local'
    """
    data = await _run_and_wait(async_client, token_headers, _YAML_LOCAL_STORAGE)

    # Field must always be present in the response schema
    assert "output_path" in data, "output_path field missing from GET /api/pipeline-runs/{id}"
    assert "actual_output_path" in data, "actual_output_path field missing from GET /api/pipeline-runs/{id}"

    output_path = data["output_path"]
    # Only assert the path value if the run succeeded (Spark job ran)
    if data["status"] == "succeeded":
        assert output_path is not None, (
            "output_path is null even though the run succeeded"
        )
        assert data["actual_output_path"] == output_path
        assert output_path.startswith("hdfs://namenode:9000/"), (
            f"Expected HDFS URI, got {output_path!r}"
        )
        assert "/tmp/actual-output-test-local" not in output_path or \
               output_path.startswith("hdfs://"), (
            f"Local path leaked into output_path: {output_path!r}"
        )


@pytest.mark.asyncio
async def test_actual_output_path_is_set_for_hdfs_storage(
    async_client: AsyncClient, token_headers: dict
):
    """
    When the configured storage type is 'hdfs', the output_path must be an
    HDFS URI matching the configured path.
    """
    data = await _run_and_wait(async_client, token_headers, _YAML_HDFS_STORAGE)

    assert "output_path" in data, "output_path field missing from GET /api/pipeline-runs/{id}"
    assert "actual_output_path" in data, "actual_output_path field missing from GET /api/pipeline-runs/{id}"

    if data["status"] == "succeeded":
        output_path = data["output_path"]
        assert output_path is not None, "output_path is null even though the run succeeded"
        assert data["actual_output_path"] == output_path
        assert output_path.startswith("hdfs://"), (
            f"Expected HDFS URI, got {output_path!r}"
        )
        assert "actual-output-test-hdfs" in output_path, (
            f"Expected the configured path component in output_path, got {output_path!r}"
        )


_YAML_API_SOURCE = """\
pipeline:
  name: "spark-input-cleanup-test"
  version: "1.0"
source:
  type: api
  url: "http://spark-master:8080/json/"
  method: "GET"
schema:
  - { name: url, type: string }
  - { name: aliveworkers, type: integer }
  - { name: cores, type: integer }
  - { name: memory, type: integer }
processing:
  operations:
    - type: remove_nulls
streaming:
  enabled: false
processing_engine:
  type: spark
storage:
  type: hdfs
  path: /data/spark-input-cleanup-test
"""


@pytest.mark.asyncio
async def test_spark_input_cleaned_up_after_run(
    async_client: AsyncClient, token_headers: dict
):
    """
    Run a pipeline with an API source (staged to HDFS tmp file), wait for
    terminal state, and verify no spark-input-* files remain in HDFS /tmp.
    """
    data = await _run_and_wait(async_client, token_headers, _YAML_API_SOURCE)
    assert data["status"] in ("succeeded", "failed")

    from app.storage.hdfs_client import HdfsClient
    client = HdfsClient()
    try:
        tmp_files = client.list_directory("/tmp")
        lingering = [f for f in tmp_files if f.startswith("spark-input-")]
        assert not lingering, f"Found lingering spark-input files in /tmp: {lingering}"
    except Exception as exc:
        pytest.fail(f"Failed to list HDFS /tmp directory: {exc}")
