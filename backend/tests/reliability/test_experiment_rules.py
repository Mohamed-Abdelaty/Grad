"""Tests for experiment safety rules, target validation, and concurrency limits."""

from __future__ import annotations

import asyncio
import uuid
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from app.api.routes.experiments import (
    ExperimentCreateRequest,
    create_experiment,
    run_experiment,
)
from app.reliability.exceptions import TooManyConcurrentExperimentsError


class _FakeAsyncDb:
    def __init__(self):
        self.added = []

    def add(self, item):
        self.added.append(item)
        if not hasattr(item, "id") or item.id is None:
            item.id = uuid.uuid4()

    async def flush(self):
        pass

    async def refresh(self, item):
        pass

    async def commit(self):
        pass

    async def get(self, model, identifier):
        return None


def _make_yaml(target: str = "kafka", fault: str = "container_stop", duration: int = 20) -> str:
    return f"""
experiment:
  name: "test-safety-rules"
target:
  service: "{target}"
fault:
  type: "{fault}"
  duration: {duration}
validation:
  expected_data_loss: 0
  max_recovery_time: 30
"""


def test_create_experiment_rejects_critical_postgres():
    db = _FakeAsyncDb()
    req = ExperimentCreateRequest(yaml_content=_make_yaml(target="postgres"))
    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(create_experiment(request=req, db=db))
    assert exc_info.value.status_code == 422
    assert "protected platform service" in str(exc_info.value.detail).lower() or "cannot be targeted" in str(exc_info.value.detail).lower()


def test_create_experiment_rejects_critical_backend():
    db = _FakeAsyncDb()
    req = ExperimentCreateRequest(yaml_content=_make_yaml(target="backend"))
    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(create_experiment(request=req, db=db))
    assert exc_info.value.status_code == 422
    assert "protected platform service" in str(exc_info.value.detail).lower() or "cannot be targeted" in str(exc_info.value.detail).lower()


def test_create_experiment_rejects_unallowlisted_target():
    db = _FakeAsyncDb()
    req = ExperimentCreateRequest(yaml_content=_make_yaml(target="redis_cache"))
    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(create_experiment(request=req, db=db))
    assert exc_info.value.status_code == 422


def test_create_experiment_rejects_duration_above_limit():
    db = _FakeAsyncDb()
    req = ExperimentCreateRequest(yaml_content=_make_yaml(target="kafka", duration=301))
    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(create_experiment(request=req, db=db))
    assert exc_info.value.status_code == 422
    assert "duration" in str(exc_info.value.detail).lower() or "ceiling" in str(exc_info.value.detail).lower()


def test_create_experiment_rejects_zero_or_negative_duration():
    db = _FakeAsyncDb()
    req = ExperimentCreateRequest(yaml_content=_make_yaml(target="kafka", duration=0))
    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(create_experiment(request=req, db=db))
    assert exc_info.value.status_code == 422


def test_run_experiment_rejects_concurrent_run():
    exp_id = uuid.uuid4()
    bg_tasks = MagicMock()
    db = _FakeAsyncDb()

    with patch("app.reliability.safety.SafetyValidator.enforce_max_concurrent_experiments") as mock_enforce:
        mock_enforce.side_effect = TooManyConcurrentExperimentsError("Another experiment is already running.")
        with pytest.raises(HTTPException) as exc_info:
            asyncio.run(run_experiment(experiment_id=exp_id, background_tasks=bg_tasks, db=db))
        assert exc_info.value.status_code == 409
        assert "already running" in str(exc_info.value.detail)
