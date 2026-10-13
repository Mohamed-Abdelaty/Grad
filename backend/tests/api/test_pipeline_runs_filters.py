"""
backend/tests/api/test_pipeline_runs_filters.py
-----------------------------------------------
Unit tests for pipeline run filtering, pagination, sorting, headers,
and yaml_content field persistence.
"""

from datetime import datetime, timezone
import json
import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from app.api.routes.pipeline_runs import list_pipeline_runs, get_pipeline_run
from app.models.pipeline import Pipeline
from app.models.pipeline_run import PipelineRun


@pytest.mark.asyncio
async def test_list_runs_filters_and_headers():
    mock_db = AsyncMock()

    # Mock count result
    mock_count_result = MagicMock()
    mock_count_result.scalar_one.return_value = 42

    # Mock runs query result
    p1 = Pipeline(id=uuid.uuid4(), name="sales-pipeline")
    r1 = PipelineRun(
        id=uuid.uuid4(),
        pipeline_id=p1.id,
        status="running",
        started_at=datetime.now(tz=timezone.utc),
        config_yaml="pipeline:\n  name: sales-pipeline\n",
    )
    r1.pipeline = p1

    mock_runs_result = MagicMock()
    mock_runs_result.scalars.return_value.all.return_value = [r1]

    # Return count on first execute, runs on second execute
    mock_db.execute.side_effect = [mock_count_result, mock_runs_result]

    resp = await list_pipeline_runs(
        db=mock_db,
        status=["running"],
        q="sales",
        limit=25,
        offset=0,
        sort="started_at_desc",
    )

    assert resp.headers["X-Total-Count"] == "42"
    assert resp.headers["Access-Control-Expose-Headers"] == "X-Total-Count"

    body = json.loads(resp.body.decode("utf-8"))
    assert len(body) == 1
    assert body[0]["id"] == str(r1.id)
    assert body[0]["name"] == "sales-pipeline"
    assert body[0]["status"] == "running"
    assert body[0]["yaml_content"] == "pipeline:\n  name: sales-pipeline\n"


@pytest.mark.asyncio
async def test_list_runs_invalid_status_raises_422():
    mock_db = AsyncMock()
    with pytest.raises(HTTPException) as exc_info:
        await list_pipeline_runs(db=mock_db, status=["invalid_status"])
    assert exc_info.value.status_code == 422


@pytest.mark.asyncio
async def test_list_runs_invalid_sort_raises_422():
    mock_db = AsyncMock()
    with pytest.raises(HTTPException) as exc_info:
        await list_pipeline_runs(db=mock_db, sort="random_sort")
    assert exc_info.value.status_code == 422


@pytest.mark.asyncio
async def test_get_run_yaml_content_new_and_old_runs():
    mock_db = AsyncMock()

    # 1. New run with yaml_content
    run_id_new = uuid.uuid4()
    p1 = Pipeline(id=uuid.uuid4(), name="new-pipeline")
    r_new = PipelineRun(
        id=run_id_new,
        pipeline_id=p1.id,
        status="succeeded",
        started_at=datetime.now(tz=timezone.utc),
        config_yaml="pipeline:\n  name: new-pipeline\n",
    )
    r_new.pipeline = p1

    mock_exec_new = MagicMock()
    mock_exec_new.scalars.return_value.first.return_value = r_new
    mock_db.execute.return_value = mock_exec_new

    resp_new = await get_pipeline_run(str(run_id_new), db=mock_db)
    data_new = json.loads(resp_new.body.decode("utf-8"))
    assert data_new["yaml_content"] == "pipeline:\n  name: new-pipeline\n"

    # 2. Older run without yaml_content (config_yaml is None)
    run_id_old = uuid.uuid4()
    p2 = Pipeline(id=uuid.uuid4(), name="old-pipeline")
    r_old = PipelineRun(
        id=run_id_old,
        pipeline_id=p2.id,
        status="succeeded",
        started_at=datetime.now(tz=timezone.utc),
        config_yaml=None,
    )
    r_old.pipeline = p2

    mock_exec_old = MagicMock()
    mock_exec_old.scalars.return_value.first.return_value = r_old
    mock_db.execute.return_value = mock_exec_old

    resp_old = await get_pipeline_run(str(run_id_old), db=mock_db)
    data_old = json.loads(resp_old.body.decode("utf-8"))
    assert data_old["yaml_content"] is None
