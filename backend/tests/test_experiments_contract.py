"""Coverage for the experiments list/detail contract used by the frontend."""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timezone

import pytest
from fastapi import HTTPException

from app.api.routes.experiments import get_experiment, list_experiments
from app.models.experiment import Experiment
from app.models.experiment_run import ExperimentRun

STARTED = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
FINISHED = datetime(2026, 1, 1, 12, 1, 30, tzinfo=timezone.utc)


class _Result:
    def __init__(self, items):
        self._items = items

    def scalars(self):
        return self

    def all(self):
        return self._items

    def scalar_one_or_none(self):
        return self._items[0] if self._items else None


class _FakeDb:
    """Returns queued results from execute() in call order."""

    def __init__(self, results, get_result=None):
        self._results = list(results)
        self._get_result = get_result

    async def execute(self, _stmt):
        return _Result(self._results.pop(0))

    async def get(self, _model, _identifier):
        return self._get_result


def _experiment(name="exp") -> Experiment:
    exp = Experiment(id=uuid.uuid4(), name=name, experiment_yaml="x", pipeline_yaml=None)
    exp.created_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    return exp


def _run(experiment_id, **kwargs) -> ExperimentRun:
    run = ExperimentRun(id=uuid.uuid4(), experiment_id=experiment_id, **kwargs)
    run.created_at = STARTED
    return run


def test_list_includes_target_service_and_fault_type_from_latest_run() -> None:
    exp = _experiment("kafka-stop")
    run = _run(
        exp.id,
        status="succeeded",
        params={"target_service": "kafka", "fault_type": "container_stop"},
    )
    db = _FakeDb([[exp], [run]])

    items = asyncio.run(list_experiments(db=db))

    assert len(items) == 1
    item = items[0]
    assert item["experiment_id"] == str(exp.id)
    assert item["latest_run_status"] == "succeeded"
    assert item["latest_run_id"] == str(run.id)
    assert item["target_service"] == "kafka"
    assert item["fault_type"] == "container_stop"


def test_list_returns_none_fields_when_experiment_never_ran() -> None:
    exp = _experiment()
    db = _FakeDb([[exp], []])

    item = asyncio.run(list_experiments(db=db))[0]

    assert item["latest_run_status"] is None
    assert item["latest_run_id"] is None
    assert item["target_service"] is None
    assert item["fault_type"] is None


def test_detail_exposes_real_timestamps_and_runner_logs() -> None:
    exp = _experiment()
    run = _run(
        exp.id,
        status="succeeded",
        stage="generating_report",
        params={"target_service": "kafka", "fault_type": "container_stop"},
        results={"logs": ["injecting fault", "recovered"], "recovery_time_seconds": 4.2},
    )
    run.started_at = STARTED
    run.finished_at = FINISHED
    db = _FakeDb([[run]], get_result=exp)

    detail = asyncio.run(get_experiment(experiment_id=exp.id, db=db))

    assert detail["runStartedAt"] == STARTED.isoformat()
    assert detail["runFinishedAt"] == FINISHED.isoformat()
    assert detail["logs"] == ["injecting fault", "recovered"]
    assert detail["results"]["recovery_time_seconds"] == 4.2


def test_detail_logs_are_empty_list_not_static_placeholders_when_runner_wrote_none() -> None:
    exp = _experiment()
    run = _run(exp.id, status="running", params={}, results={})
    run.started_at = STARTED
    run.finished_at = None
    db = _FakeDb([[run]], get_result=exp)

    detail = asyncio.run(get_experiment(experiment_id=exp.id, db=db))

    assert detail["logs"] == []
    assert detail["runFinishedAt"] is None


def test_detail_without_any_run_has_null_timestamps() -> None:
    exp = _experiment()
    db = _FakeDb([[]], get_result=exp)

    detail = asyncio.run(get_experiment(experiment_id=exp.id, db=db))

    assert detail["status"] == "pending"
    assert detail["runStartedAt"] is None
    assert detail["runFinishedAt"] is None
    assert detail["logs"] == []


def test_detail_unknown_experiment_is_404() -> None:
    db = _FakeDb([], get_result=None)
    with pytest.raises(HTTPException) as exc:
        asyncio.run(get_experiment(experiment_id=uuid.uuid4(), db=db))
    assert exc.value.status_code == 404
