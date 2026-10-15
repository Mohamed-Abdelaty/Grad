"""
tests/test_health.py
--------------------
Tests for the /health and /api/system/status endpoints.

These tests use FastAPI's TestClient backed by httpx and run without a running
PostgreSQL, Kafka, Spark, or HDFS instance — all external dependencies are mocked.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from app.main import app

# ── Fixtures ───────────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def client() -> TestClient:
    """Return a synchronous TestClient wrapping the FastAPI app."""
    with TestClient(app) as c:
        yield c


# ── /health ────────────────────────────────────────────────────────────────────


class TestHealthCheck:
    """Tests for the GET /health liveness endpoint."""

    def test_returns_200(self, client: TestClient) -> None:
        """The endpoint must respond with HTTP 200."""
        response = client.get("/health")
        assert response.status_code == 200

    def test_returns_correct_body(self, client: TestClient) -> None:
        """The response body must be exactly ``{"status": "ok"}``."""
        response = client.get("/health")
        assert response.json() == {"status": "ok"}

    def test_content_type_is_json(self, client: TestClient) -> None:
        """The Content-Type header must indicate JSON."""
        response = client.get("/health")
        assert "application/json" in response.headers.get("content-type", "")

    def test_preserves_request_correlation_id(self, client: TestClient) -> None:
        response = client.get("/health", headers={"X-Request-ID": "audit-123"})
        assert response.headers["X-Request-ID"] == "audit-123"


# ── /api/system/status ─────────────────────────────────────────────────────────


class TestSystemStatus:
    """Tests for the GET /api/system/status readiness endpoint."""

    def _mock_db(self):
        """Return an async mock that simulates a healthy database session."""
        mock_session = AsyncMock()
        mock_session.execute.return_value = MagicMock()
        return mock_session

    def _kafka_ok(self) -> dict:
        return {"connected": True, "broker_count": 1, "topic_count": 0, "error": None}

    def _kafka_down(self) -> dict:
        return {"connected": False, "broker_count": 0, "topic_count": 0, "error": "Broker transport failure"}

    def _spark_ok(self) -> dict:
        return {"connected": True, "worker_count": 1, "status": "ALIVE", "error": None}

    def _spark_down(self) -> dict:
        return {"connected": False, "worker_count": 0, "status": "UNREACHABLE", "error": "Connection refused"}

    def _hdfs_ok(self) -> dict:
        return {"connected": True, "namenode_url": "http://namenode:9870", "num_live_datanodes": 1, "error": None}

    def _hdfs_down(self) -> dict:
        return {"connected": False, "namenode_url": "http://namenode:9870", "num_live_datanodes": None, "error": "Connection refused"}

    def _all_mocks_ok(self, health_module):
        """Return a context manager patching all three external checks."""
        return (
            patch.object(health_module, "check_kafka_connectivity", return_value=self._kafka_ok()),
            patch.object(health_module, "check_spark_connectivity", return_value=self._spark_ok()),
            patch.object(health_module, "check_hdfs_connectivity", return_value=self._hdfs_ok()),
        )

    def test_returns_200_with_healthy_db(self, client: TestClient) -> None:
        """Endpoint returns 200 when the database session executes without error."""
        from app.database.session import get_db

        async def override_get_db():
            yield self._mock_db()

        app.dependency_overrides[get_db] = override_get_db
        try:
            response = client.get("/api/system/status")
            assert response.status_code == 200
        finally:
            app.dependency_overrides.clear()

    def test_response_contains_required_keys(self, client: TestClient) -> None:
        """Response JSON must contain status, app_version, environment, database, kafka, spark, hdfs."""
        from app.database.session import get_db
        from app.api.routes import health as health_module

        async def override_get_db():
            yield self._mock_db()

        app.dependency_overrides[get_db] = override_get_db
        try:
            k, s, h = self._all_mocks_ok(health_module)
            with k, s, h:
                body = client.get("/api/system/status").json()
            assert "status" in body
            assert "app_version" in body
            assert "environment" in body
            assert "database" in body
            assert "kafka" in body    # Phase 4
            assert "spark" in body    # Phase 5
            assert "hdfs" in body     # Phase 6
        finally:
            app.dependency_overrides.clear()

    def test_database_connected_true_on_success(self, client: TestClient) -> None:
        """database.connected must be True when the mock session succeeds."""
        from app.database.session import get_db
        from app.api.routes import health as health_module

        async def override_get_db():
            yield self._mock_db()

        app.dependency_overrides[get_db] = override_get_db
        try:
            k, s, h = self._all_mocks_ok(health_module)
            with k, s, h:
                body = client.get("/api/system/status").json()
            assert body["database"]["connected"] is True
            assert body["database"]["error"] is None
            assert body["kafka"]["connected"] is True
            assert body["spark"]["connected"] is True
            assert body["hdfs"]["connected"] is True
            assert body["status"] == "ok"
        finally:
            app.dependency_overrides.clear()

    def test_database_connected_false_on_failure(self, client: TestClient) -> None:
        """database.connected must be False when the session raises an exception."""
        from app.database.session import get_db

        failing_session = AsyncMock()
        failing_session.execute.side_effect = Exception("Connection refused")

        async def override_get_db():
            yield failing_session

        app.dependency_overrides[get_db] = override_get_db
        try:
            body = client.get("/api/system/status").json()
            assert body["database"]["connected"] is False
            assert body["database"]["error"] is not None
            assert body["status"] == "degraded"
        finally:
            app.dependency_overrides.clear()

    def test_kafka_connected_false_when_broker_down(self, client: TestClient) -> None:
        """status must be 'degraded' when Kafka is unreachable."""
        from app.database.session import get_db
        from app.api.routes import health as health_module

        async def override_get_db():
            yield self._mock_db()

        app.dependency_overrides[get_db] = override_get_db
        try:
            with (
                patch.object(health_module, "check_kafka_connectivity", return_value=self._kafka_down()),
                patch.object(health_module, "check_spark_connectivity", return_value=self._spark_ok()),
                patch.object(health_module, "check_hdfs_connectivity", return_value=self._hdfs_ok()),
            ):
                body = client.get("/api/system/status").json()
            assert body["kafka"]["connected"] is False
            assert body["status"] == "degraded"
        finally:
            app.dependency_overrides.clear()

    def test_spark_connected_false_when_cluster_down(self, client: TestClient) -> None:
        """status must be 'degraded' when Spark is unreachable."""
        from app.database.session import get_db
        from app.api.routes import health as health_module

        async def override_get_db():
            yield self._mock_db()

        app.dependency_overrides[get_db] = override_get_db
        try:
            with (
                patch.object(health_module, "check_kafka_connectivity", return_value=self._kafka_ok()),
                patch.object(health_module, "check_spark_connectivity", return_value=self._spark_down()),
                patch.object(health_module, "check_hdfs_connectivity", return_value=self._hdfs_ok()),
            ):
                body = client.get("/api/system/status").json()
            assert body["spark"]["connected"] is False
            assert body["status"] == "degraded"
        finally:
            app.dependency_overrides.clear()

    def test_hdfs_connected_false_when_namenode_down(self, client: TestClient) -> None:
        """status must be 'degraded' when HDFS NameNode is unreachable."""
        from app.database.session import get_db
        from app.api.routes import health as health_module

        async def override_get_db():
            yield self._mock_db()

        app.dependency_overrides[get_db] = override_get_db
        try:
            with (
                patch.object(health_module, "check_kafka_connectivity", return_value=self._kafka_ok()),
                patch.object(health_module, "check_spark_connectivity", return_value=self._spark_ok()),
                patch.object(health_module, "check_hdfs_connectivity", return_value=self._hdfs_down()),
            ):
                body = client.get("/api/system/status").json()
            assert body["hdfs"]["connected"] is False
            assert body["status"] == "degraded"
        finally:
            app.dependency_overrides.clear()


class TestKafkaConnectivityTimeout:
    """Verify check_kafka_connectivity default timeout unchanged and monitor probe passes 0.5."""

    def test_default_timeout_unchanged(self) -> None:
        """check_kafka_connectivity() default timeout must remain 5.0 seconds."""
        from app.api.routes.health import check_kafka_connectivity
        from unittest.mock import MagicMock, patch

        mock_admin = MagicMock()
        mock_admin.health_check.return_value = {"connected": True}
        with patch("app.messaging.kafka_admin.KafkaAdminClient", return_value=mock_admin):
            res = check_kafka_connectivity()
            mock_admin.health_check.assert_called_once_with(timeout=5.0)
            assert res == {"connected": True}

    def test_monitor_passes_point_five(self) -> None:
        """ExperimentRunner._get_health_check_fn with timeout_seconds=0.5 scopes 0.5s to check_kafka_connectivity."""
        from app.reliability.experiment_runner import ExperimentRunner
        from unittest.mock import patch

        with patch("app.api.routes.health.check_kafka_connectivity") as mock_check:
            mock_check.return_value = {"connected": True}
            probe = ExperimentRunner._get_health_check_fn("kafka", timeout_seconds=0.5)
            assert probe is not None
            probe()
            mock_check.assert_called_once_with(timeout_seconds=0.5)

    def test_recovery_keeps_default_timeout(self) -> None:
        """ExperimentRunner._get_health_check_fn for recovery leaves default timeout unchanged."""
        from app.reliability.experiment_runner import ExperimentRunner
        from unittest.mock import patch

        with patch("app.api.routes.health.check_kafka_connectivity") as mock_check:
            mock_check.return_value = {"connected": True}
            probe = ExperimentRunner._get_health_check_fn("kafka")
            assert probe is not None
            probe()
            mock_check.assert_called_once_with()

