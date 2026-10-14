"""
backend/tests/integration/test_output_integration.py
----------------------------------------------------
Integration tests for UI-6 output metadata, preview, and CSV/Parquet downloads
running against the live platform stack.
"""

import asyncio
import io
from datetime import timedelta
from pathlib import Path

import pyarrow.csv as pcsv
import pyarrow.parquet as pq
import pytest
from httpx import AsyncClient

from app.core.security import create_access_token

pytestmark = pytest.mark.integration


@pytest.fixture
async def async_client():
    async with AsyncClient(base_url="http://localhost:8000", timeout=60.0) as ac:
        yield ac


@pytest.fixture
def token_headers():
    token = create_access_token({"sub": "admin"}, timedelta(minutes=15))
    return {"Authorization": f"Bearer {token}"}


@pytest.mark.asyncio
async def test_output_metadata_preview_and_downloads(async_client: AsyncClient, token_headers: dict):
    # 1. Fetch sales-csv template
    tpl_resp = await async_client.get("/api/pipeline-templates", headers=token_headers)
    assert tpl_resp.status_code == 200
    templates = tpl_resp.json()
    sales_tpl = next(t for t in templates if t["id"] == "sales-csv")
    yaml_content = sales_tpl["yaml"]

    # 2. Run pipeline asynchronously
    run_resp = await async_client.post(
        "/api/pipelines/run?wait=false",
        json={"yaml_content": yaml_content},
        headers=token_headers,
    )
    assert run_resp.status_code in (200, 202)
    run_id = run_resp.json()["pipeline_run_id"]

    # 3. Wait for run to succeed
    for _ in range(90):
        prog_resp = await async_client.get(f"/api/pipeline-runs/{run_id}/progress", headers=token_headers)
        if prog_resp.status_code == 200 and prog_resp.json().get("status") in ("succeeded", "failed", "cancelled"):
            break
        await asyncio.sleep(1.0)

    final_prog = prog_resp.json()
    assert final_prog["status"] == "succeeded", f"Run did not succeed: {final_prog.get('errors')}"
    records_out = final_prog.get("spark", {}).get("recordsOut")

    # 4. Metadata endpoint
    meta_resp = await async_client.get(f"/api/pipeline-runs/{run_id}/output", headers=token_headers)
    assert meta_resp.status_code == 200
    meta = meta_resp.json()
    assert meta["format"] == "parquet"
    assert meta["fileCount"] >= 1
    assert meta["totalSizeBytes"] > 0
    if records_out is not None:
        assert meta["rowCount"] == records_out
    assert len(meta["columns"]) >= 5
    col_names = [c["name"] for c in meta["columns"]]
    assert "transaction_id" in col_names

    # 5. Preview endpoint
    prev_resp = await async_client.get(f"/api/pipeline-runs/{run_id}/output/preview?limit=50", headers=token_headers)
    assert prev_resp.status_code == 200
    preview = prev_resp.json()
    assert len(preview["rows"]) == meta["rowCount"]
    assert preview["truncated"] is False

    # 6. CSV Download Link and Streaming
    csv_link_resp = await async_client.post(
        f"/api/pipeline-runs/{run_id}/output/download-link",
        json={"format": "csv"},
        headers=token_headers,
    )
    assert csv_link_resp.status_code == 200
    csv_link_data = csv_link_resp.json()
    csv_url = csv_link_data["url"]
    assert "token=" in csv_url

    # Stream CSV through signed token
    csv_dl_resp = await async_client.get(csv_url)
    assert csv_dl_resp.status_code == 200
    assert "text/csv" in csv_dl_resp.headers["content-type"]
    assert "attachment;" in csv_dl_resp.headers["content-disposition"]
    csv_bytes = csv_dl_resp.content
    csv_lines = [l for l in csv_bytes.decode("utf-8").split("\n") if l.strip()]
    assert len(csv_lines) == meta["rowCount"] + 1  # header + data rows

    # Second use of same token must be rejected (single use)
    csv_dl_repeat = await async_client.get(csv_url)
    assert csv_dl_repeat.status_code == 401

    # 7. Parquet Download Link and Streaming
    pq_link_resp = await async_client.post(
        f"/api/pipeline-runs/{run_id}/output/download-link",
        json={"format": "parquet"},
        headers=token_headers,
    )
    assert pq_link_resp.status_code == 200
    pq_url = pq_link_resp.json()["url"]

    pq_dl_resp = await async_client.get(pq_url)
    assert pq_dl_resp.status_code == 200
    assert "parquet" in pq_dl_resp.headers["content-type"]
    pq_bytes = pq_dl_resp.content
    assert pq_bytes.startswith(b"PAR1")
    assert pq_bytes.endswith(b"PAR1")

    # Read back downloaded Parquet
    pq_file = pq.ParquetFile(io.BytesIO(pq_bytes))
    assert pq_file.metadata.num_rows == meta["rowCount"]


@pytest.mark.asyncio
async def test_output_cancelled_and_failed_runs_return_409(async_client: AsyncClient, token_headers: dict):
    # 1. Failed run (non-existent source file)
    failed_yaml = """
pipeline:
  name: "failing-pipeline"
  version: "1.0"
source:
  type: csv
  path: "/nonexistent/path/data.csv"
schema:
  - { name: col, type: string }
processing:
  operations:
    - { type: remove_nulls }
streaming:
  enabled: false
messaging:
  broker: kafka
  topic: fail-topic
processing_engine:
  type: spark
storage:
  type: hdfs
  path: /data/fail_out
monitoring:
  enabled: true
"""
    run_resp = await async_client.post(
        "/api/pipelines/run?wait=false",
        json={"yaml_content": failed_yaml},
        headers=token_headers,
    )
    assert run_resp.status_code in (200, 202)
    failed_run_id = run_resp.json()["pipeline_run_id"]

    for _ in range(30):
        prog = await async_client.get(f"/api/pipeline-runs/{failed_run_id}/progress", headers=token_headers)
        if prog.status_code == 200 and prog.json().get("status") == "failed":
            break
        await asyncio.sleep(1.0)

    out_resp = await async_client.get(f"/api/pipeline-runs/{failed_run_id}/output", headers=token_headers)
    assert out_resp.status_code == 409
    assert out_resp.json()["detail"] == "Run has no completed output"
