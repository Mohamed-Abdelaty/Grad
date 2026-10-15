"""
backend/tests/test_dashboard_summary.py
---------------------------------------
Unit and performance tests for GET /api/dashboard/summary.

Validates:
- Authentication required (401 without credentials)
- Validation on windowHours parameter (ge=1, le=168, returns 422)
- Correctness of SQL aggregate counts (active, total, succeeded, failed, cancelled)
- Zero-filling of hourly buckets over the requested window
- Null KPIs when no completed experiments exist
- KPIs and trends limited to the last 10 completed experiments
- Query count stays strictly constant (monitored via SQLAlchemy event listener)
- Query performance executes in under 300 ms for 10 000 seeded pipeline runs
"""

from __future__ import annotations

import time
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, event
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import Session

from app.api.deps import get_current_user
from app.database.base import Base
from app.database.session import get_db
from app.main import app
from app.models.experiment import Experiment
from app.models.experiment_run import ExperimentRun
from app.models.pipeline import Pipeline
from app.models.pipeline_run import PipelineRun
from app.models.report import Report


# Enable SQLite support for PostgreSQL JSONB columns in test runs
@compiles(JSONB, "sqlite")
def _compile_jsonb_sqlite(type_, compiler, **kw):
    return "TEXT"


class _AsyncSessionAdapter:
    """Synchronous Session adapter that satisfies the AsyncSession interface for testing."""

    def __init__(self, sync_session: Session):
        self._sync = sync_session

    async def execute(self, statement, *args, **kwargs):
        return self._sync.execute(statement, *args, **kwargs)

    def get_bind(self):
        return self._sync.get_bind()

    async def commit(self):
        self._sync.commit()

    async def rollback(self):
        self._sync.rollback()

    async def close(self):
        self._sync.close()


from sqlalchemy.pool import StaticPool

@pytest.fixture
def test_db():
    """Create a fresh in-memory SQLite database populated with Schema."""
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    session = Session(engine)
    yield engine, session
    session.close()
    engine.dispose()


@pytest.fixture
def client_authenticated(test_db):
    """TestClient with overridden get_db and authenticated user."""
    engine, session = test_db

    async def _override_get_db():
        yield _AsyncSessionAdapter(session)

    async def _override_user():
        return "admin"

    app.dependency_overrides[get_db] = _override_get_db
    app.dependency_overrides[get_current_user] = _override_user
    with TestClient(app) as c:
        yield c, session, engine
    app.dependency_overrides.pop(get_db, None)
    app.dependency_overrides.pop(get_current_user, None)


def test_dashboard_unauthenticated_returns_401():
    """Requesting summary without auth token or override returns 401."""
    # Ensure no override for get_current_user
    app.dependency_overrides.pop(get_current_user, None)
    with TestClient(app) as c:
        res = c.get("/api/dashboard/summary")
        assert res.status_code == 401


def test_dashboard_invalid_window_hours_returns_422(client_authenticated):
    """windowHours outside 1..168 returns 422 Unprocessable Entity."""
    c, _, _ = client_authenticated
    # Test below minimum
    res = c.get("/api/dashboard/summary?windowHours=0")
    assert res.status_code == 422

    # Test above maximum
    res2 = c.get("/api/dashboard/summary?windowHours=169")
    assert res2.status_code == 422


def test_dashboard_empty_db_returns_null_kpis_and_zero_counts(client_authenticated):
    """An empty database returns 0 counts and null for all KPIs."""
    c, _, _ = client_authenticated
    res = c.get("/api/dashboard/summary?windowHours=24")
    assert res.status_code == 200
    data = res.json()

    assert data["windowHours"] == 24
    assert data["bucketHours"] == 1
    assert data["runs"] == {"active": 0, "total": 0, "succeeded": 0, "failed": 0, "cancelled": 0}
    assert data["experiments"] == {"running": 0, "total": 0, "passed": 0, "failed": 0}
    assert data["kpis"] == {
        "avgRecoverySeconds": None,
        "totalDataLoss": None,
        "avgThroughput": None,
        "avgAvailabilityPercent": None,
    }
    assert len(data["runsPerHour"]) >= 24
    for b in data["runsPerHour"]:
        assert b["total"] == 0
        assert b["failed"] == 0
    assert data["recoveryTrend"] == []
    assert data["throughputTrend"] == []
    assert data["recentFailures"] == []


def test_dashboard_zero_filling_runs_per_hour(client_authenticated):
    """runsPerHour has zero-filled buckets spanning the requested window."""
    c, session, _ = client_authenticated
    now = datetime.now(timezone.utc)

    # Insert a pipeline and 1 run 3 hours ago
    pipe = Pipeline(id=uuid.uuid4(), name="sales-pipe")
    session.add(pipe)
    run = PipelineRun(
        id=uuid.uuid4(),
        pipeline_id=pipe.id,
        status="succeeded",
        started_at=now - timedelta(hours=3),
        finished_at=now - timedelta(hours=2, minutes=58),
    )
    session.add(run)
    session.commit()

    res = c.get("/api/dashboard/summary?windowHours=12")
    assert res.status_code == 200
    data = res.json()

    assert data["windowHours"] == 12
    assert data["bucketHours"] == 1
    # Check that at least 12 buckets are present and ascending
    assert len(data["runsPerHour"]) >= 12
    hours = [b["hour"] for b in data["runsPerHour"]]
    assert hours == sorted(hours)

    total_runs_in_buckets = sum(b["total"] for b in data["runsPerHour"])
    assert total_runs_in_buckets == 1


def test_dashboard_aggregates_and_numbers(client_authenticated):
    """Assert all counts, KPI calculations, and trends match seeded data."""
    c, session, _ = client_authenticated
    now = datetime.now(timezone.utc)

    pipe = Pipeline(id=uuid.uuid4(), name="analytics-pipe")
    session.add(pipe)

    # Add 1 active run, 2 succeeded runs within window, 1 failed run within window, 1 outside window
    session.add(PipelineRun(id=uuid.uuid4(), pipeline_id=pipe.id, status="running", started_at=now - timedelta(hours=1)))
    session.add(PipelineRun(id=uuid.uuid4(), pipeline_id=pipe.id, status="succeeded", started_at=now - timedelta(hours=5)))
    session.add(PipelineRun(id=uuid.uuid4(), pipeline_id=pipe.id, status="succeeded", started_at=now - timedelta(hours=6)))
    session.add(PipelineRun(id=uuid.uuid4(), pipeline_id=pipe.id, status="failed", started_at=now - timedelta(hours=8), error_message="Kafka timeout"))
    session.add(PipelineRun(id=uuid.uuid4(), pipeline_id=pipe.id, status="succeeded", started_at=now - timedelta(hours=30)))

    # Add experiments and reports
    exp = Experiment(id=uuid.uuid4(), name="broker-kill", experiment_yaml="...", pipeline_yaml=None)
    session.add(exp)
    session.commit()

    # Completed report 1: PASS
    session.add(Report(
        id=uuid.uuid4(),
        title="Broker Kill Report 1",
        report_type="chaos_experiment",
        created_at=now - timedelta(hours=2),
        metadata_={
            "experiment_id": str(exp.id),
            "result": "PASS",
            "recoveryTimeSeconds": 10.0,
            "dataLoss": 0,
            "average_throughput": 1000.0,
            "throughputBaseline": 1200.0,
            "throughputDuringFault": 400.0,
            "availabilityPercentage": 99.5,
        }
    ))

    # Completed report 2: FAIL
    session.add(Report(
        id=uuid.uuid4(),
        title="Broker Kill Report 2",
        report_type="chaos_experiment",
        created_at=now - timedelta(hours=1),
        metadata_={
            "experiment_id": str(exp.id),
            "result": "FAIL",
            "failedCriteria": ["Recovery time exceeded: 20.0s > 15.0s"],
            "recoveryTimeSeconds": 20.0,
            "dataLoss": 5,
            "average_throughput": 800.0,
            "throughputBaseline": 1100.0,
            "throughputDuringFault": 200.0,
            "availabilityPercentage": 95.0,
        }
    ))
    session.commit()

    res = c.get("/api/dashboard/summary?windowHours=24")
    assert res.status_code == 200
    data = res.json()

    # Runs assertions
    assert data["runs"]["active"] == 1
    assert data["runs"]["total"] == 4  # 1 running + 2 succeeded + 1 failed inside window
    assert data["runs"]["succeeded"] == 2
    assert data["runs"]["failed"] == 1
    assert data["runs"]["cancelled"] == 0

    # Experiments assertions
    assert data["experiments"]["total"] == 2

    # KPIs assertions (avg over completed)
    # recovery: (10 + 20) / 2 = 15.0
    assert data["kpis"]["avgRecoverySeconds"] == 15.0
    # data loss: 0 + 5 = 5
    assert data["kpis"]["totalDataLoss"] == 5
    # throughput: (1000 + 800) / 2 = 900.0
    assert data["kpis"]["avgThroughput"] == 900.0
    # availability: (99.5 + 95.0) / 2 = 97.25
    assert data["kpis"]["avgAvailabilityPercent"] == 97.25

    # Trends: 2 items, ascending
    assert len(data["recoveryTrend"]) == 2
    assert data["recoveryTrend"][0]["recoverySeconds"] == 10.0
    assert data["recoveryTrend"][1]["recoverySeconds"] == 20.0

    assert len(data["throughputTrend"]) == 2
    assert data["throughputTrend"][0]["baseline"] == 1200.0
    assert data["throughputTrend"][1]["baseline"] == 1100.0

    # Recent failures: 1 failed run and 1 failed experiment
    assert len(data["recentFailures"]) == 2
    types = {f["type"] for f in data["recentFailures"]}
    assert types == {"run", "experiment"}


def test_dashboard_10_experiment_limit_for_kpis(client_authenticated):
    """KPIs and trends only consider the last 10 completed experiments."""
    c, session, _ = client_authenticated
    now = datetime.now(timezone.utc)

    exp = Experiment(id=uuid.uuid4(), name="exp-limit", experiment_yaml="...", pipeline_yaml=None)
    session.add(exp)

    # Seed 15 completed reports with recovery times 1..15
    for i in range(1, 16):
        session.add(Report(
            id=uuid.uuid4(),
            title=f"Report {i}",
            report_type="chaos_experiment",
            created_at=now - timedelta(minutes=60 - i),  # i=15 is newest
            metadata_={
                "experiment_id": str(exp.id),
                "result": "PASS",
                "recoveryTimeSeconds": float(i),
                "dataLoss": 0,
                "average_throughput": 100.0,
                "availabilityPercentage": 100.0,
            }
        ))
    session.commit()

    res = c.get("/api/dashboard/summary?windowHours=24")
    assert res.status_code == 200
    data = res.json()

    # Last 10 reports are i=6..15
    # avg of 6..15 = (6+7+8+9+10+11+12+13+14+15)/10 = 105/10 = 10.5
    assert data["kpis"]["avgRecoverySeconds"] == 10.5
    assert len(data["recoveryTrend"]) == 10
    # Ordered ascending
    assert data["recoveryTrend"][0]["recoverySeconds"] == 6.0
    assert data["recoveryTrend"][-1]["recoverySeconds"] == 15.0


def test_dashboard_query_count_constant(client_authenticated):
    """Assert query count stays constant when data volume scales."""
    c, session, engine = client_authenticated
    now = datetime.now(timezone.utc)

    pipe = Pipeline(id=uuid.uuid4(), name="query-count-pipe")
    session.add(pipe)
    session.commit()

    # Track executed queries
    queries = []

    def before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
        queries.append(statement)

    event.listen(engine, "before_cursor_execute", before_cursor_execute)

    # First call with minimal data
    queries.clear()
    c.get("/api/dashboard/summary?windowHours=24")
    first_query_count = len(queries)
    assert first_query_count <= 8  # At most 8 SQL statements

    # Now add 500 runs and 20 reports
    runs = [
        PipelineRun(
            id=uuid.uuid4(),
            pipeline_id=pipe.id,
            status="succeeded",
            started_at=now - timedelta(minutes=i),
        )
        for i in range(500)
    ]
    session.bulk_save_objects(runs)
    session.commit()

    # Second call with 500+ items
    queries.clear()
    c.get("/api/dashboard/summary?windowHours=24")
    second_query_count = len(queries)

    event.remove(engine, "before_cursor_execute", before_cursor_execute)

    # The query count MUST remain constant!
    assert second_query_count == first_query_count


def test_dashboard_performance_under_300ms_for_10k_runs(client_authenticated):
    """Response time must be strictly under 300 ms for 10,000 seeded pipeline runs."""
    c, session, _ = client_authenticated
    now = datetime.now(timezone.utc)

    pipe = Pipeline(id=uuid.uuid4(), name="perf-pipe")
    session.add(pipe)
    session.commit()

    # Seed 10,000 runs
    runs = [
        PipelineRun(
            id=uuid.uuid4(),
            pipeline_id=pipe.id,
            status="succeeded" if i % 10 != 0 else "failed",
            started_at=now - timedelta(minutes=i),
            finished_at=now - timedelta(minutes=i - 1),
        )
        for i in range(10000)
    ]
    session.bulk_save_objects(runs)
    session.commit()

    # Measure timing over the request
    t0 = time.perf_counter()
    res = c.get("/api/dashboard/summary?windowHours=24")
    t1 = time.perf_counter()

    elapsed_ms = (t1 - t0) * 1000.0
    print(f"\n[BENCHMARK] 10,000 runs dashboard summary response time: {elapsed_ms:.2f} ms")

    assert res.status_code == 200
    assert elapsed_ms < 300.0, f"Query took {elapsed_ms:.2f} ms, exceeding 300 ms limit"
