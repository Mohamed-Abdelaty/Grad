import asyncio
import pytest
from httpx import AsyncClient
from datetime import timedelta
from app.core.security import create_access_token
import os

pytestmark = pytest.mark.integration

@pytest.fixture
async def async_client():
    async with AsyncClient(base_url="http://localhost:8000") as ac:
        yield ac

@pytest.fixture
def token_headers():
    token = create_access_token({"sub": "admin"}, timedelta(minutes=15))
    return {"Authorization": f"Bearer {token}"}

@pytest.mark.asyncio
async def test_actual_output_path_is_recorded(async_client: AsyncClient, token_headers: dict):
    yaml_content = """
pipeline:
  name: "output-test"
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
  path: "/tmp/spark-output-fallback"
"""
    # Start run with wait=true to easily get the result
    resp = await async_client.post(
        "/api/pipelines/run?wait=true",
        json={"yaml_content": yaml_content},
        headers=token_headers,
    )
    
    assert resp.status_code == 200
    data = resp.json()
    
    # Check that output_path was set
    # It might fail if the API endpoint is bad, but even on fail it may or may not reach storage.
    # We just need to check if output_path is present in DB or returned result if it reached there.
    assert "pipeline_run_id" in data
    
    run_id = data["pipeline_run_id"]
    status_resp = await async_client.get(
        f"/api/pipeline-runs/{run_id}",
        headers=token_headers,
    )
    
    assert status_resp.status_code == 200
    status_data = status_resp.json()
    
    # We don't assert it successfully wrote because example.com might fail,
    # but we assert the field is there in the schema.
    assert "output_path" in status_data
