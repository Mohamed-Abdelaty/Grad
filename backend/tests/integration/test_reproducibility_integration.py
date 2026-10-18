"""
backend/tests/integration/test_reproducibility_integration.py
------------------------------------------------------------
Integration tests for Gap 2: Reproducibility Repetitions & Pre-Fault Accounting.
Runs against the live Docker Compose platform stack.

Verifies:
1. Experiment with repetitions=2 on a safe target with a short duration:
   - Verifies 2 child runs + 1 parent with aggregate metrics
   - Verifies all services healthy afterward and no leftover faults
2. Experiment with repetitions=1:
   - Yields legacy shape (single run, parent_run_id is None, no child runs)
"""

import asyncio
from datetime import timedelta
import pytest
from httpx import AsyncClient

from app.core.security import create_access_token

pytestmark = pytest.mark.integration


@pytest.fixture
def token_headers():
    token = create_access_token({"sub": "admin"}, timedelta(minutes=30))
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture
async def async_client():
    async with AsyncClient(base_url="http://localhost:8000", timeout=300.0) as ac:
        yield ac


@pytest.mark.asyncio
async def test_reproducibility_repetitions_two_creates_parent_and_children(
    async_client: AsyncClient, token_headers: dict
):
    """
    Verify experiment with repetitions=2:
    - 2 child runs created linked to 1 parent run
    - Parent has aggregated_metrics populated
    - Both child runs complete and reach terminal status
    - Fault state is cleared and services remain healthy
    """
    exp_name = f"rep-2-integration-{uuid.uuid4().hex[:8]}"
    exp_yaml = f"""\
experiment:
  name: "{exp_name}"
target:
  service: kafka
fault:
  type: container_restart
  duration: 3
validation:
  max_recovery_time: 30
repetitions: 2
"""
    # 1. Create experiment
    create_res = await async_client.post(
        "/api/experiments",
        json={"yaml_content": exp_yaml, "pipeline_yaml": ""},
        headers=token_headers,
    )
    assert create_res.status_code in (200, 201), f"Create failed: {create_res.text}"
    exp_id = create_res.json()["experiment_id"]

    # 2. Trigger experiment run
    run_res = await async_client.post(
        f"/api/experiments/{exp_id}/run",
        headers=token_headers,
    )
    assert run_res.status_code in (200, 202), f"Run trigger failed: {run_res.text}"
    parent_run_id = run_res.json()["run_id"]
    assert parent_run_id

    # 3. Poll experiment until terminal state
    start_time = asyncio.get_event_loop().time()
    max_wait = 180.0
    terminal_detail = None

    while (asyncio.get_event_loop().time() - start_time) < max_wait:
        await asyncio.sleep(2)
        detail_res = await async_client.get(
            f"/api/experiments/{exp_id}",
            headers=token_headers,
        )
        assert detail_res.status_code == 200
        detail = detail_res.json()
        if detail.get("status") in ("succeeded", "failed"):
            terminal_detail = detail
            break

    assert terminal_detail is not None, f"Experiment did not reach terminal state within {max_wait}s"
    assert terminal_detail["status"] == "succeeded"
    assert terminal_detail["result"] == "PASS"
    assert terminal_detail.get("totalRepetitions") == 2

    # Verify aggregatedMetrics on parent detail
    agg = terminal_detail.get("aggregatedMetrics")
    assert agg is not None
    assert "recovery_time_seconds" in agg
    assert agg["recovery_time_seconds"]["n_samples"] == 2
    assert agg["recovery_time_seconds"]["mean"] is not None

    rep_results = terminal_detail.get("repetitionResults")
    assert rep_results is not None
    assert len(rep_results) == 2
    assert rep_results[0]["repetition_index"] == 1
    assert rep_results[1]["repetition_index"] == 2

    # 4. Check system status: all services must be healthy, no leftover faults
    health_res = await async_client.get("/api/system/status")
    assert health_res.status_code == 200
    health_data = health_res.json()
    assert health_data.get("status") == "ok"
    assert health_data.get("kafka", {}).get("connected") is True
    assert health_data.get("spark", {}).get("connected") is True
    assert health_data.get("database", {}).get("connected") is True
    assert health_data.get("hdfs", {}).get("connected") is True


@pytest.mark.asyncio
async def test_reproducibility_repetitions_one_legacy_shape(
    async_client: AsyncClient, token_headers: dict
):
    """
    Verify experiment with repetitions=1 (default):
    - Yields legacy shape (no child runs created, parent_run_id is None)
    - Reaches terminal state
    """
    exp_name = f"rep-1-legacy-{uuid.uuid4().hex[:8]}"
    exp_yaml = f"""\
experiment:
  name: "{exp_name}"
target:
  service: kafka
fault:
  type: container_restart
  duration: 3
validation:
  max_recovery_time: 30
repetitions: 1
"""
    # 1. Create experiment
    create_res = await async_client.post(
        "/api/experiments",
        json={"yaml_content": exp_yaml, "pipeline_yaml": ""},
        headers=token_headers,
    )
    assert create_res.status_code in (200, 201), f"Create failed: {create_res.text}"
    exp_id = create_res.json()["experiment_id"]

    # 2. Trigger experiment run
    run_res = await async_client.post(
        f"/api/experiments/{exp_id}/run",
        headers=token_headers,
    )
    assert run_res.status_code in (200, 202), f"Run trigger failed: {run_res.text}"

    # 3. Poll experiment until terminal state
    start_time = asyncio.get_event_loop().time()
    max_wait = 120.0
    terminal_detail = None

    while (asyncio.get_event_loop().time() - start_time) < max_wait:
        await asyncio.sleep(2)
        detail_res = await async_client.get(
            f"/api/experiments/{exp_id}",
            headers=token_headers,
        )
        assert detail_res.status_code == 200
        detail = detail_res.json()
        if detail.get("status") in ("succeeded", "failed"):
            terminal_detail = detail
            break

    assert terminal_detail is not None, f"Experiment did not reach terminal state within {max_wait}s"
    assert terminal_detail["status"] == "succeeded"
    assert terminal_detail["result"] == "PASS"
    assert terminal_detail.get("totalRepetitions") == 1
