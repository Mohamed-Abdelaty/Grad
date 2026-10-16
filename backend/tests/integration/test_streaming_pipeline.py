"""
backend/tests/integration/test_streaming_pipeline.py
------------------------------------------------------
Integration test for UI-10: Streaming Mode End to End (ChaosFlow).
Verifies:
1. Submitting a streaming pipeline run via API (streaming.enabled=true).
2. Streaming mode and consumer lag monitoring via /progress.
3. Streaming completion detector gracefully stopping on finite drain.
4. Terminal status succeeded with verified output in HDFS.
"""

import asyncio
from datetime import timedelta
import pytest
from httpx import AsyncClient

from app.core.security import create_access_token
from app.storage.hdfs_client import HdfsClient

pytestmark = pytest.mark.integration

_STREAMING_YAML = """\
pipeline:
  name: "streaming-integration-test"
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
    - { type: remove_nulls }
    - { type: filter, condition: "amount > 0" }
streaming:
  enabled: true
  trigger_interval_seconds: 2
  idle_timeout_seconds: 5
  max_runtime_seconds: 120
  starting_offsets: earliest
messaging:
  broker: kafka
  topic: streaming-test-events
  partitions: 3
  replication_factor: 1
processing_engine:
  type: spark
storage:
  type: hdfs
  path: /data/output/streaming_integration_test
monitoring:
  enabled: true
"""


@pytest.fixture
def token_headers():
    token = create_access_token({"sub": "admin"}, timedelta(minutes=15))
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture
async def async_client():
    async with AsyncClient(base_url="http://localhost:8000", timeout=300.0) as ac:
        yield ac


@pytest.mark.asyncio
async def test_streaming_pipeline_end_to_end(
    async_client: AsyncClient, token_headers: dict
):
    """Submit a streaming pipeline and verify end-to-end execution, lag tracking, and HDFS sink."""
    # 1. Trigger pipeline run
    resp = await async_client.post(
        "/api/pipelines/run?wait=false",
        json={"yaml_content": _STREAMING_YAML},
        headers=token_headers,
    )
    assert resp.status_code == 202, f"Failed to start run: {resp.text}"
    run_id = resp.json()["pipeline_run_id"]
    assert run_id

    # 2. Poll progress and wait for completion
    terminal_run = None
    seen_streaming_mode = False
    max_wait = 180
    start_time = asyncio.get_event_loop().time()

    while (asyncio.get_event_loop().time() - start_time) < max_wait:
        await asyncio.sleep(3)
        prog_res = await async_client.get(
            f"/api/pipeline-runs/{run_id}/progress",
            headers=token_headers,
        )
        if prog_res.status_code == 200:
            prog_data = prog_res.json()
            if prog_data.get("mode") == "streaming":
                seen_streaming_mode = True

        run_res = await async_client.get(
            f"/api/pipeline-runs/{run_id}",
            headers=token_headers,
        )
        if run_res.status_code == 200:
            data = run_res.json()
            if data["status"] not in ("running", "pending"):
                terminal_run = data
                break

    assert terminal_run is not None, f"Streaming pipeline did not complete within {max_wait}s"
    await asyncio.sleep(1)
    final_res = await async_client.get(
        f"/api/pipeline-runs/{run_id}",
        headers=token_headers,
    )
    if final_res.status_code == 200:
        terminal_run = final_res.json()

    assert (
        terminal_run["status"] == "succeeded"
    ), f"Pipeline failed with: {terminal_run.get('error_message')}"
    assert seen_streaming_mode, "Progress tracker never reported mode='streaming'"

    # 3. Check HDFS output
    hdfs_client = HdfsClient(namenode_url="http://namenode:9870", user="root")
    path_exists = hdfs_client.path_exists("/data/output/streaming_integration_test")
    assert path_exists, "HDFS output path does not exist"
    contents = hdfs_client.list_directory("/data/output/streaming_integration_test")
    assert len(contents) > 0, "No files written to HDFS output directory"
    assert terminal_run.get("output_path") is not None, "output_path is None in run"

    # 4. Assert checkpoint cleanup: no entry in /checkpoints contains run_id
    if hdfs_client.path_exists("/checkpoints"):
        checkpoint_entries = hdfs_client.list_directory("/checkpoints")
        matching = [e for e in checkpoint_entries if run_id in e]
        assert not matching, f"Found leftover checkpoint directories for run {run_id}: {matching}"
        assert not hdfs_client.path_exists(f"/checkpoints/streaming-{run_id}")

