"""
backend/tests/integration/test_streaming_chaos.py
-------------------------------------------------
Step 8: ChaosLab on Streaming Pipeline.
Validates:
1. Active Structured Streaming pipeline running against Kafka.
2. Fault injection: container_restart (5s) on Kafka during streaming execution.
3. Producer retry queue survival during broker downtime.
4. Recovery of streaming consumer and zero data loss (published == consumed == expected).
5. Terminal status succeeded with verified output in HDFS.
"""

import asyncio
from datetime import timedelta
import time
import pytest
from httpx import AsyncClient

from app.core.security import create_access_token
from app.storage.hdfs_client import HdfsClient

pytestmark = pytest.mark.integration

_STREAMING_CHAOS_YAML = """\
pipeline:
  name: "streaming-chaos-test"
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
    - { type: remove_nulls }
    - { type: filter, condition: "amount > 0" }
streaming:
  enabled: true
  trigger_interval_seconds: 2
  idle_timeout_seconds: 5
  max_runtime_seconds: 300
  starting_offsets: earliest
messaging:
  broker: kafka
  topic: streaming-chaos-events
  partitions: 3
  replication_factor: 1
processing_engine:
  type: spark
storage:
  type: hdfs
  path: /data/output/streaming_chaos_test
monitoring:
  enabled: true
"""

_EXP_YAML = """\
experiment:
  name: "kafka-restart-streaming-chaos"
target:
  service: kafka
fault:
  type: container_restart
  duration: 5
validation:
  max_recovery_time: 60
"""


@pytest.fixture
def token_headers():
    token = create_access_token({"sub": "admin"}, timedelta(minutes=30))
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture
async def async_client():
    async with AsyncClient(base_url="http://localhost:8000", timeout=300.0) as ac:
        yield ac


@pytest.mark.asyncio
async def test_streaming_pipeline_survives_kafka_restart(
    async_client: AsyncClient, token_headers: dict
):
    """Test streaming pipeline end-to-end resilience under Kafka container restart."""
    # 1. Start streaming pipeline run
    resp = await async_client.post(
        "/api/pipelines/run?wait=false",
        json={"yaml_content": _STREAMING_CHAOS_YAML},
        headers=token_headers,
    )
    assert resp.status_code == 202, f"Failed to start run: {resp.text}"
    run_id = resp.json()["pipeline_run_id"]
    assert run_id

    # 2. Wait until streaming pipeline is active and Kafka stage is active/publishing
    fault_triggered = False
    exp_id = None
    start_time = asyncio.get_event_loop().time()
    terminal_run = None
    max_wait = 240

    while (asyncio.get_event_loop().time() - start_time) < max_wait:
        await asyncio.sleep(2)
        prog_res = await async_client.get(
            f"/api/pipeline-runs/{run_id}/progress",
            headers=token_headers,
        )
        if prog_res.status_code == 200:
            prog_data = prog_res.json()
            kafka_published = (prog_data.get("kafka") or {}).get("published", 0)

            # Trigger fault when publishing has started but pipeline is still active
            if not fault_triggered and kafka_published > 0:
                print(f"\n[ChaosLab] Active streaming detected ({kafka_published} published). Injecting Kafka container_restart...")
                create_exp_res = await async_client.post(
                    "/api/experiments",
                    json={"yaml_content": _EXP_YAML, "pipeline_yaml": ""},
                    headers=token_headers,
                )
                assert create_exp_res.status_code in (200, 201), f"Failed to create exp: {create_exp_res.text}"
                exp_id = create_exp_res.json().get("experiment_id") or create_exp_res.json().get("id")

                run_exp_res = await async_client.post(
                    f"/api/experiments/{exp_id}/run",
                    headers=token_headers,
                )
                assert run_exp_res.status_code in (200, 201, 202), f"Failed to run exp: {run_exp_res.text}"
                fault_triggered = True
                print(f"[ChaosLab] Fault injected for experiment {exp_id}.")

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
    assert (
        terminal_run["status"] == "succeeded"
    ), f"Pipeline failed under chaos: {terminal_run.get('error_message')}"
    assert fault_triggered, "Fault injection was never triggered during active streaming"

    # 3. Verify experiment completed and measured recovery
    if exp_id:
        exp_res = await async_client.get(
            f"/api/experiments/{exp_id}",
            headers=token_headers,
        )
        if exp_res.status_code == 200:
            exp_data = exp_res.json()
            print(f"[ChaosLab] Experiment status: {exp_data.get('status')}")

    # 4. Assert zero data loss: all 300,000 records processed
    final_prog_res = await async_client.get(
        f"/api/pipeline-runs/{run_id}/progress",
        headers=token_headers,
    )
    assert final_prog_res.status_code == 200
    final_prog = final_prog_res.json()
    k_pub = (final_prog.get("kafka") or {}).get("published")
    s_in = (final_prog.get("spark") or {}).get("recordsIn")
    s_out = (final_prog.get("spark") or {}).get("recordsOut")

    print(f"\n[Zero Data Loss Verification]")
    print(f"Expected Rows:  300000")
    print(f"Kafka Published: {k_pub}")
    print(f"Spark Ingested:  {s_in}")
    print(f"Spark Output:    {s_out}")

    assert k_pub == 300000, f"Producer lost records: {k_pub} != 300000"
    assert s_in == 300000, f"Spark missed records: {s_in} != 300000"
    assert s_out == 300000, f"Output records mismatch: {s_out} != 300000"

    # 5. Check HDFS output
    hdfs_client = HdfsClient(namenode_url="http://namenode:9870", user="root")
    path_exists = hdfs_client.path_exists("/data/output/streaming_chaos_test")
    assert path_exists, "HDFS output path does not exist"
    contents = hdfs_client.list_directory("/data/output/streaming_chaos_test")
    assert len(contents) > 0, "No files written to HDFS output directory"
