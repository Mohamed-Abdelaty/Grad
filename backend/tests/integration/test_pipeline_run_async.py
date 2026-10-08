import asyncio
import json
from httpx import AsyncClient

import pytest

pytestmark = pytest.mark.integration

SAMPLE_YAML = """
pipeline:
  name: "async-test"
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
async def test_pipeline_run_async_and_sse(async_client: AsyncClient, token_headers: dict):
    # 1. Run pipeline in async mode (wait=false)
    resp = await async_client.post(
        "/api/pipelines/run?wait=false",
        json={"yaml_content": SAMPLE_YAML},
        headers=token_headers,
    )
    assert resp.status_code == 202
    data = resp.json()
    assert data["status"] == "running"
    run_id = data["pipeline_run_id"]
    assert run_id is not None

    # 2. Check /progress endpoint immediately
    prog_resp = await async_client.get(
        f"/api/pipeline-runs/{run_id}/progress",
        headers=token_headers,
    )
    assert prog_resp.status_code == 200
    prog_data = prog_resp.json()
    assert prog_data["status"] in ("running", "succeeded", "failed")

    # 3. Stream SSE using ?token= query param
    token = token_headers["Authorization"].split(" ")[1]
    
    events_received = []
    
    # Use standard requests streaming over async client for SSE testing
    async with async_client.stream(
        "GET", 
        f"/api/pipeline-runs/{run_id}/stream?token={token}"
    ) as stream_resp:
        assert stream_resp.status_code == 200
        
        async for line in stream_resp.aiter_lines():
            line = line.strip()
            if not line or not line.startswith("data: "):
                continue
            data_str = line[len("data: "):]
            event_data = json.loads(data_str)
            events_received.append(event_data)
            
            # The SSE stream should eventually yield 'run_failed' or 'run_success'
            # then close automatically.
            if event_data.get("status") in ("success", "failed", "cancelled"):
                # this is the 'event: done' data payload
                break

    # We should have received at least some events (since the run is backgrounded)
    assert len(events_received) > 0
    # It might fail because the API source url is 'http://test/api' which doesn't exist.
    # But the SSE machinery and tracker must work regardless.
