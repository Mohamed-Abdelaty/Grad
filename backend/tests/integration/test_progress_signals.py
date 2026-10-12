"""
tests/integration/test_progress_signals.py
-------------------------------------------
Step 10.1 Integration test:
Run a large pipeline via POST /api/pipelines/run?wait=false, poll /progress
every second until terminal status (max 300 s), and assert:
- kafka.published never decreases and ends equal to source.rowsTotal (with kafka.failed == 0);
- at least 3 distinct kafka.published values were observed while running;
- spark.tasksDone ends greater than 0;
- hdfs.bytesWritten > 0 and hdfs.verified is True at the end;
- spark.recordsOut == hdfs.rowsWritten;
- every field documented as null in a given phase is null in that phase (e.g. kafka.consumerLag always null).
"""

import asyncio
from datetime import timedelta
from pathlib import Path
import pytest
from httpx import AsyncClient

from app.core.security import create_access_token

pytestmark = pytest.mark.integration

YAML_CONFIG = """\
pipeline:
  name: "progress-signals-integration-test"
  version: "1.0"
source:
  type: csv
  path: /app/sample-data/sales_300k.csv
schema:
  - { name: transaction_id, type: string }
  - { name: customer_id, type: string }
  - { name: product_id, type: string }
  - { name: amount, type: double }
  - { name: timestamp, type: string }
processing:
  operations:
    - type: filter
      condition: "amount > 0"
streaming:
  enabled: false
messaging:
  broker: kafka
  topic: progress-signals-test
processing_engine:
  type: spark
storage:
  type: hdfs
  path: /data/progress-signals-integration-test
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
async def test_progress_signals_real_counters(
    async_client: AsyncClient, token_headers: dict
):
    # Ensure source CSV exists (created on host or mount)
    csv_path = Path("/app/sample-data/sales_300k.csv")
    if not csv_path.exists():
        # Fall back to sales_large.csv if 300k file isn't present
        large_path = Path("/app/sample-data/sales_large.csv")
        assert large_path.exists(), "Neither sales_300k.csv nor sales_large.csv found in /app/sample-data"
        yaml_content = YAML_CONFIG.replace("sales_300k.csv", "sales_large.csv")
    else:
        yaml_content = YAML_CONFIG

    # 1. Start pipeline run via API
    resp = await async_client.post(
        "/api/pipelines/run?wait=false",
        json={"yaml_content": yaml_content},
        headers=token_headers,
    )
    assert resp.status_code == 202, f"Expected 202, got {resp.status_code}: {resp.text}"
    body = resp.json()
    run_id = body.get("pipeline_run_id")
    assert run_id is not None, f"Response missing pipeline_run_id: {body}"

    # 2. Poll /progress every second until terminal state (max 300 s)
    kafka_published_series: list[int] = []
    observed_snapshots: list[dict] = []

    last_published = 0
    terminal_snapshot = None

    for _ in range(300):
        await asyncio.sleep(1.0)
        p_resp = await async_client.get(
            f"/api/pipeline-runs/{run_id}/progress",
            headers=token_headers,
        )
        assert p_resp.status_code == 200, f"Failed GET /progress: {p_resp.status_code}"
        p = p_resp.json()
        observed_snapshots.append(p)

        # Assert kafka.consumerLag is always null in batch mode
        assert p["kafka"]["consumerLag"] is None, (
            f"kafka.consumerLag must be null, got: {p['kafka']['consumerLag']}"
        )

        current_pub = p["kafka"]["published"]
        if current_pub is not None:
            # Assert kafka.published never decreases
            assert current_pub >= last_published, (
                f"kafka.published decreased: {current_pub} < {last_published}"
            )
            last_published = current_pub
            kafka_published_series.append(current_pub)

        # Assert kafka.failed is 0 (or not increased unexpectedly)
        assert p["kafka"]["failed"] == 0, f"Expected 0 kafka.failed, got: {p['kafka']['failed']}"

        if p["status"] in ("succeeded", "failed", "cancelled"):
            terminal_snapshot = p
            break

    assert terminal_snapshot is not None, "Pipeline did not reach terminal status within 300s"
    assert terminal_snapshot["status"] == "succeeded", (
        f"Pipeline ended with {terminal_snapshot['status']}: {terminal_snapshot.get('errors')}"
    )

    # 3. Assertions on final snapshot and collected progress
    # - kafka.published ends equal to source.rowsTotal (with kafka.failed == 0)
    assert terminal_snapshot["source"]["rowsTotal"] is not None
    assert terminal_snapshot["kafka"]["published"] == terminal_snapshot["source"]["rowsTotal"]
    assert terminal_snapshot["kafka"]["failed"] == 0

    # - at least 3 distinct kafka.published values were observed while running
    distinct_kafka_pub = set(kafka_published_series)
    assert len(distinct_kafka_pub) >= 3, (
        f"Expected at least 3 distinct kafka.published values, observed: {distinct_kafka_pub}"
    )

    # - spark.tasksDone ends greater than 0
    assert terminal_snapshot["spark"]["tasksDone"] is not None
    assert terminal_snapshot["spark"]["tasksDone"] > 0

    # - hdfs.bytesWritten > 0 and hdfs.verified is True at the end
    assert terminal_snapshot["hdfs"]["bytesWritten"] is not None
    assert terminal_snapshot["hdfs"]["bytesWritten"] > 0
    assert terminal_snapshot["hdfs"]["verified"] is True

    # - spark.recordsOut == hdfs.rowsWritten
    assert terminal_snapshot["spark"]["recordsOut"] is not None
    assert terminal_snapshot["hdfs"]["rowsWritten"] is not None
    assert terminal_snapshot["spark"]["recordsOut"] == terminal_snapshot["hdfs"]["rowsWritten"]
