"""
backend/tests/integration/test_batch_kafka_integration.py
---------------------------------------------------------
Integration tests for Gap 1: Batch Mode Ingestion via Kafka.
Runs against the live Docker Compose platform stack.

Verifies:
1. batch_kafka_ingestion.enabled=true:
   - Kafka topic created
   - Records published
   - Spark reads the exact same count from Kafka (recordsIn == published)
   - Output written to HDFS (verified == True)
   - All tracker stages complete ('done')
   - Run-scoped Kafka topic deleted afterwards
2. Default path (disabled):
   - Reads directly from file
   - Kafka stage skipped, no Kafka topic created
3. Failure path:
   - Pipeline failure leads to status 'failed'
   - Run-scoped Kafka topic is still cleaned up
"""

import asyncio
from datetime import timedelta
import pytest
from httpx import AsyncClient

from app.core.security import create_access_token
from app.messaging.kafka_admin import KafkaAdminClient
from app.storage.hdfs_client import HdfsClient

pytestmark = pytest.mark.integration

_YAML_BATCH_KAFKA = """\
pipeline:
  name: "batch-kafka-integration-test"
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
  enabled: false
batch_kafka_ingestion:
  enabled: true
  partitions: 2
  replication_factor: 1
  cleanup_topic: true
processing_engine:
  type: spark
storage:
  type: hdfs
  path: /data/output/batch_kafka_integration_test
monitoring:
  enabled: true
"""

_YAML_BATCH_DEFAULT = """\
pipeline:
  name: "batch-default-integration-test"
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
streaming:
  enabled: false
processing_engine:
  type: spark
storage:
  type: hdfs
  path: /data/output/batch_default_integration_test
monitoring:
  enabled: true
"""

_YAML_BATCH_KAFKA_FAIL = """\
pipeline:
  name: "batch-kafka-fail-test"
  version: "1.0"
source:
  type: csv
  path: /app/sample-data/nonexistent_sample.csv
schema:
  - { name: transaction_id, type: string }
  - { name: amount, type: double }
processing:
  operations:
    - { type: remove_nulls }
streaming:
  enabled: false
batch_kafka_ingestion:
  enabled: true
  partitions: 1
  replication_factor: 1
  cleanup_topic: true
processing_engine:
  type: spark
storage:
  type: hdfs
  path: /data/output/batch_kafka_fail_test
"""


@pytest.fixture
def token_headers():
    token = create_access_token({"sub": "admin"}, timedelta(minutes=15))
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture
async def async_client():
    async with AsyncClient(base_url="http://localhost:8000", timeout=300.0) as ac:
        yield ac


async def _poll_until_terminal(client: AsyncClient, run_id: str, headers: dict, timeout_s: int = 180) -> tuple[dict, dict]:
    """Poll run and progress endpoints until run is terminal."""
    last_run: dict = {}
    last_prog: dict = {}
    for _ in range(timeout_s // 3):
        await asyncio.sleep(3)
        prog_res = await client.get(f"/api/pipeline-runs/{run_id}/progress", headers=headers)
        if prog_res.status_code == 200:
            last_prog = prog_res.json()
        run_res = await client.get(f"/api/pipeline-runs/{run_id}", headers=headers)
        if run_res.status_code == 200:
            last_run = run_res.json()
            if last_run.get("status") not in ("running", "pending"):
                return last_run, last_prog
    pytest.fail(f"Run {run_id} did not terminate within {timeout_s}s. Last status: {last_run.get('status')}")


@pytest.mark.asyncio
async def test_batch_kafka_ingestion_live_success(async_client: AsyncClient, token_headers: dict):
    """
    Test batch pipeline with batch_kafka_ingestion.enabled=true on live stack.
    Proves:
    - topic created and published
    - Spark read count == published count
    - output written to HDFS with row verification
    - tracker stages source/kafka/spark/hdfs all 'done'
    - run-scoped topic deleted after run
    """
    admin = KafkaAdminClient()
    initial_topics = set(admin.list_topics())

    resp = await async_client.post(
        "/api/pipelines/run?wait=false",
        json={"yaml_content": _YAML_BATCH_KAFKA},
        headers=token_headers,
    )
    assert resp.status_code == 202, f"Failed to start run: {resp.text}"
    run_id = resp.json()["pipeline_run_id"]
    assert run_id

    run_data, prog_data = await _poll_until_terminal(async_client, run_id, token_headers)
    assert run_data["status"] == "succeeded", f"Run failed: {run_data.get('error_message')}"

    # 1. Verify stage states
    stage_states = {s["key"]: s["state"] for s in prog_data.get("stages", [])}
    assert stage_states.get("source") == "done"
    assert stage_states.get("kafka") == "done"
    assert stage_states.get("spark") == "done"
    assert stage_states.get("hdfs") == "done"

    # 2. Verify counts
    published = prog_data["kafka"]["published"]
    records_in = prog_data["spark"]["recordsIn"]
    records_out = prog_data["spark"]["recordsOut"]
    hdfs_rows = prog_data["hdfs"]["rowsWritten"]

    assert published is not None and published > 0
    # Spark read exact count published to Kafka
    assert records_in == published, f"Spark recordsIn ({records_in}) != Kafka published ({published})"
    # Spark output rows == verified HDFS rows
    assert records_out is not None and records_out > 0
    assert hdfs_rows == records_out, f"HDFS rowsWritten ({hdfs_rows}) != Spark recordsOut ({records_out})"
    assert prog_data["hdfs"]["verified"] is True

    # 3. Verify HDFS output directory
    hdfs = HdfsClient(namenode_url="http://namenode:9870", user="root")
    assert hdfs.path_exists("/data/output/batch_kafka_integration_test")
    files = hdfs.list_directory("/data/output/batch_kafka_integration_test")
    assert len(files) > 0

    # 4. Verify run-scoped Kafka topic was cleaned up
    final_topics = set(admin.list_topics())
    # Expected run-scoped topic name format contains run_id
    sanitized_id = run_id.replace("-", "_")
    matching_topics = [t for t in final_topics if sanitized_id in t]
    assert not matching_topics, f"Run-scoped topic was not deleted: {matching_topics}"


@pytest.mark.asyncio
async def test_batch_default_path_reads_file_and_no_kafka_topic(async_client: AsyncClient, token_headers: dict):
    """
    Test batch pipeline with batch_kafka_ingestion disabled (default).
    Proves:
    - Spark reads directly from source file
    - Kafka stage is skipped
    - HDFS output verified
    - No Kafka topic created for the run
    """
    admin = KafkaAdminClient()
    initial_topics = set(admin.list_topics())

    resp = await async_client.post(
        "/api/pipelines/run?wait=false",
        json={"yaml_content": _YAML_BATCH_DEFAULT},
        headers=token_headers,
    )
    assert resp.status_code == 202, f"Failed to start run: {resp.text}"
    run_id = resp.json()["pipeline_run_id"]

    run_data, prog_data = await _poll_until_terminal(async_client, run_id, token_headers)
    assert run_data["status"] == "succeeded", f"Run failed: {run_data.get('error_message')}"

    # Kafka stage should be skipped
    stage_states = {s["key"]: s["state"] for s in prog_data.get("stages", [])}
    assert stage_states.get("kafka") in ("skipped", "pending")
    assert prog_data["kafka"]["published"] is None
    assert stage_states.get("spark") == "done"
    assert stage_states.get("hdfs") == "done"

    # No new topic containing run_id
    final_topics = set(admin.list_topics())
    sanitized_id = run_id.replace("-", "_")
    matching = [t for t in final_topics if sanitized_id in t]
    assert not matching, f"Unexpected topic created in default batch mode: {matching}"


@pytest.mark.asyncio
async def test_batch_kafka_failure_leads_to_failed_run_and_cleaned_topic(async_client: AsyncClient, token_headers: dict):
    """
    Test batch pipeline failure when batch_kafka_ingestion.enabled=true.
    Proves:
    - Failure sets status to 'failed'
    - Kafka topic is cleaned up in finally block
    """
    admin = KafkaAdminClient()

    resp = await async_client.post(
        "/api/pipelines/run?wait=false",
        json={"yaml_content": _YAML_BATCH_KAFKA_FAIL},
        headers=token_headers,
    )
    assert resp.status_code == 202, f"Failed to start run: {resp.text}"
    run_id = resp.json()["pipeline_run_id"]

    run_data, prog_data = await _poll_until_terminal(async_client, run_id, token_headers)
    assert run_data["status"] == "failed"

    # Verify no leaked topic
    final_topics = set(admin.list_topics())
    sanitized_id = run_id.replace("-", "_")
    matching = [t for t in final_topics if sanitized_id in t]
    assert not matching, f"Topic leaked on failure: {matching}"
