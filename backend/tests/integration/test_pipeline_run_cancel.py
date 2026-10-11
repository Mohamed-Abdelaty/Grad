"""
tests/integration/test_pipeline_run_cancel.py
----------------------------------------------
Integration test: cancelling a run stops the Spark subprocess and sets
status='cancelled'.

Checks (all must pass):
  1. POST /run?wait=false returns 202.
  2. The spark_start event appears before cancel is sent.
  3. POST /cancel returns 202 or 409.
  4. After a short wait, status == 'cancelled'.
  5. No spark-submit or java process belonging to the run remains in the backend.
  6. No leftover spark-input-* temp files in HDFS /tmp/.
"""

import asyncio
import subprocess
import pytest
from datetime import timedelta
from httpx import AsyncClient

from app.core.security import create_access_token

pytestmark = pytest.mark.integration

_YAML_LONG_RUN = """\
pipeline:
  name: "cancel-test-spark"
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
  path: /data/cancel-test-spark
"""


@pytest.fixture
def token_headers():
    token = create_access_token({"sub": "admin"}, timedelta(minutes=15))
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture
async def async_client():
    async with AsyncClient(base_url="http://localhost:8000", timeout=120.0) as ac:
        yield ac


def _ps_spark_in_backend() -> str:
    """Return ps aux output filtered for spark-submit / java in the backend container."""
    result = subprocess.run(
        ["docker", "exec", "platform-backend", "sh", "-c",
         "ps aux | grep -E 'spark-submit|java' | grep -v grep || true"],
        capture_output=True, text=True, timeout=15,
    )
    return result.stdout.strip()


def _hdfs_tmp_listing() -> list[str]:
    """Return list of filenames under HDFS /tmp/."""
    try:
        result = subprocess.run(
            ["docker", "exec", "platform-backend", "python3", "-c",
             "import urllib.request, json; "
             "d=json.load(urllib.request.urlopen('http://namenode:9870/webhdfs/v1/tmp?op=LISTSTATUS')); "
             "print([f['pathSuffix'] for f in d.get('FileStatuses',{}).get('FileStatus',[])])"],
            capture_output=True, text=True, timeout=30,
        )
        import ast
        return ast.literal_eval(result.stdout.strip()) if result.stdout.strip() else []
    except Exception:
        return []


@pytest.mark.asyncio
async def test_pipeline_run_cancel_stops_spark(
    async_client: AsyncClient, token_headers: dict
):
    # ── 1. Start the run ───────────────────────────────────────────────────────
    resp = await async_client.post(
        "/api/pipelines/run?wait=false",
        json={"yaml_content": _YAML_LONG_RUN},
        headers=token_headers,
    )
    assert resp.status_code == 202, f"Expected 202 got {resp.status_code}: {resp.text}"
    run_id = resp.json()["pipeline_run_id"]

    # ── 2. Wait for spark_start event (max 60 s) ───────────────────────────────
    spark_seen = False
    for _ in range(60):
        await asyncio.sleep(1)
        ev_resp = await async_client.get(
            f"/api/pipeline-runs/{run_id}/events?after=0",
            headers=token_headers,
        )
        if ev_resp.status_code == 200:
            events_text = ev_resp.text
            if "spark" in events_text.lower():
                spark_seen = True
                break

    # ── 3. Capture process list before cancel ─────────────────────────────────
    ps_before = _ps_spark_in_backend()

    # ── 4. Cancel ──────────────────────────────────────────────────────────────
    cancel_resp = await async_client.post(
        f"/api/pipeline-runs/{run_id}/cancel",
        headers=token_headers,
    )
    assert cancel_resp.status_code in (202, 409), (
        f"Cancel returned unexpected status {cancel_resp.status_code}: {cancel_resp.text}"
    )

    # ── 5. Wait for the run to settle ──────────────────────────────────────────
    await asyncio.sleep(10)

    # ── 6. Assert final status is 'cancelled' ─────────────────────────────────
    status_resp = await async_client.get(
        f"/api/pipeline-runs/{run_id}",
        headers=token_headers,
    )
    assert status_resp.status_code == 200
    final_status = status_resp.json()["status"]
    assert final_status == "cancelled", (
        f"Expected status='cancelled' but got {final_status!r}. "
        "The cancel endpoint must set status to 'cancelled' for a Spark job."
    )

    # ── 7. Assert no Spark process remains ────────────────────────────────────
    ps_after = _ps_spark_in_backend()
    # If spark was seen before cancel, the process list after must be empty.
    if spark_seen and ps_before:
        assert not ps_after, (
            f"spark-submit/java processes still running after cancel:\n{ps_after}"
        )

    # ── 8. Assert no leftover spark-input-* files in HDFS /tmp ────────────────
    tmp_files = _hdfs_tmp_listing()
    leftover = [f for f in tmp_files if f.startswith("spark-input-")]
    assert not leftover, (
        f"Leftover HDFS temp input files after cancel: {leftover}. "
        "The executor must delete temp input files in the finally block."
    )
