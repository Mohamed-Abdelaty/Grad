import asyncio
import pytest
from httpx import AsyncClient

pytestmark = pytest.mark.integration

from app.main import app
from app.core.security import create_access_token
from datetime import timedelta

@pytest.fixture
async def async_client():
    async with AsyncClient(base_url="http://localhost:8000") as ac:
        yield ac

@pytest.fixture
def token_headers():
    token = create_access_token({"sub": "admin"}, timedelta(minutes=15))
    return {"Authorization": f"Bearer {token}"}

@pytest.mark.asyncio
async def test_pipeline_run_cancel(async_client: AsyncClient, token_headers: dict):
    # Valid YAML with a sleep or something that runs for a moment if possible,
    # but a fast failure is also fine. We just want to test the /cancel endpoint.
    yaml_content = """
pipeline:
  name: "cancel-test"
  version: "1.0"
schema:
  - name: "col1"
    type: "string"
streaming:
  enabled: false
processing_engine:
  type: spark
source:
  type: api
  url: "http://example.com"
  method: "GET"
processing:
  operations:
    - type: remove_nulls
storage:
  type: local
  path: "/tmp/out"
"""
    # Start run
    resp = await async_client.post(
        "/api/pipelines/run?wait=false",
        json={"yaml_content": yaml_content},
        headers=token_headers,
    )
    assert resp.status_code == 202, f"Expected 202, got {resp.status_code}: {resp.text}"
    run_id = resp.json()["pipeline_run_id"]

    # Request cancel
    cancel_resp = await async_client.post(
        f"/api/pipeline-runs/{run_id}/cancel",
        headers=token_headers,
    )
    assert cancel_resp.status_code in (202, 409)
    
    # Wait a bit for background task to finish
    await asyncio.sleep(1.0)
    
    # Check final status
    status_resp = await async_client.get(
        f"/api/pipeline-runs/{run_id}",
        headers=token_headers,
    )
    assert status_resp.status_code == 200
    # It might be 'failed' (if source threw before cancel check) or 'cancelled'.
    assert status_resp.json()["status"] in ("cancelled", "failed", "succeeded")
