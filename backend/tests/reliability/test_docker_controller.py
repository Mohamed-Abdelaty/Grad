"""
tests/reliability/test_docker_controller.py
---------------------------------------------
Integration tests for DockerController against a real Docker socket.

All tests are marked ``@pytest.mark.integration`` and skipped gracefully
when Docker is unavailable.

Safety design for integration tests
-------------------------------------
Tests NEVER touch real platform services (kafka, spark-master, etc.).
Instead, each test spins up a throwaway ``alpine:latest`` container labeled
with the platform's compose project label, exercises DockerController against
that safe disposable container, then cleans it up unconditionally.

This tests the real Docker operations without risking the platform state.
"""

from __future__ import annotations

import uuid
from unittest.mock import MagicMock

import pytest

from app.reliability.exceptions import UnsafeTargetError


# ── Skip fixture ───────────────────────────────────────────────────────────────

def _docker_available() -> bool:
    """Return True if the Docker socket is accessible."""
    try:
        import docker
        client = docker.from_env()
        client.ping()
        return True
    except Exception:
        return False


skip_if_no_docker = pytest.mark.skipif(
    not _docker_available(),
    reason="Docker socket not accessible — run with the full stack or mount the socket.",
)


# ── Helpers ────────────────────────────────────────────────────────────────────

PROJECT_LABEL = "resilient-big-data-platform"


def _make_test_container(client):
    """
    Create a throwaway alpine container labeled with the platform compose label.

    The container runs ``sleep infinity`` so it stays alive for the test.
    """
    test_id = uuid.uuid4().hex[:8]
    container = client.containers.run(
        "alpine:latest",
        command=["sleep", "infinity"],
        name=f"chaoslab-test-{test_id}",
        labels={
            "com.docker.compose.project": PROJECT_LABEL,
            "com.docker.compose.service": f"chaoslab-test-{test_id}",
            "chaoslab_test": "true",
        },
        detach=True,
        remove=False,
    )
    return container


def _cleanup_container(container) -> None:
    """Force-remove a test container."""
    try:
        container.stop(timeout=3)
    except Exception:
        pass
    try:
        container.remove(force=True)
    except Exception:
        pass


# ── Integration tests ──────────────────────────────────────────────────────────


@pytest.mark.integration
@skip_if_no_docker
class TestDockerControllerIntegration:
    """Real Docker operations against a throwaway test container."""

    @pytest.fixture(autouse=True)
    def test_container(self):
        """Spin up a throwaway alpine container; clean it up after the test."""
        import docker
        client = docker.from_env()
        container = _make_test_container(client)
        self._container = container
        self._service_name = container.labels["com.docker.compose.service"]
        try:
            yield container
        finally:
            _cleanup_container(container)

    def _make_controller(self):
        """Build a DockerController wired to a SafetyValidator that finds our test container."""
        import docker
        from app.reliability.safety import SafetyValidator
        from app.reliability.docker_controller import DockerController

        real_client = docker.from_env()
        safety = SafetyValidator(docker_client=real_client)

        # Patch ALLOWED_TARGET_SERVICES to include our test service
        import app.reliability.safety as safety_module
        original = safety_module.ALLOWED_TARGET_SERVICES
        test_service = self._service_name

        # We patch validate_target to bypass allow-list for the test service
        original_validate = safety.validate_target

        def patched_validate(service_name):
            if service_name == test_service:
                # Still do the container + label check but skip the allow-list
                container = safety._find_container(service_name)
                project_label = (container.labels or {}).get("com.docker.compose.project")
                if project_label != PROJECT_LABEL:
                    from app.reliability.exceptions import UnsafeTargetError
                    raise UnsafeTargetError(f"Label mismatch: {project_label}")
                return
            return original_validate(service_name)

        safety.validate_target = patched_validate
        return DockerController(safety=safety)

    def test_stop_and_start_container(self) -> None:
        """stop_container → is_container_running=False → start_container → True."""
        ctrl = self._make_controller()
        service = self._service_name

        result = ctrl.stop_container(service)
        assert "stopped_at" in result
        assert "container_id" in result

        self._container.reload()
        assert self._container.status in ("exited", "stopped", "dead")

        ctrl.start_container(service)
        self._container.reload()
        assert self._container.status == "running"

    def test_restart_container_returns_metadata(self) -> None:
        """restart_container must return a dict with restarted_at."""
        ctrl = self._make_controller()
        result = ctrl.restart_container(self._service_name)
        assert "restarted_at" in result

        self._container.reload()
        assert self._container.status == "running"

    def test_is_container_running_true_for_running(self) -> None:
        """is_container_running must return True for a running container."""
        ctrl = self._make_controller()
        assert ctrl.is_container_running(self._service_name) is True

    def test_is_container_running_false_after_stop(self) -> None:
        """is_container_running must return False after stopping."""
        ctrl = self._make_controller()
        ctrl.stop_container(self._service_name)
        assert ctrl.is_container_running(self._service_name) is False

    def test_exec_in_container_runs_command(self) -> None:
        """exec_in_container must run a command and return output."""
        ctrl = self._make_controller()
        exit_code, output = ctrl.exec_in_container(self._service_name, ["echo", "chaoslab"])
        assert exit_code == 0
        assert "chaoslab" in output


# ── Unit tests (no Docker required) ───────────────────────────────────────────


class TestDockerControllerUnit:
    """Unit tests with fully mocked Docker SDK."""

    def _make_controller(self):
        from app.reliability.safety import SafetyValidator
        from app.reliability.docker_controller import DockerController

        safety = MagicMock(spec=SafetyValidator)
        # _find_container returns a mock container
        mock_container = MagicMock()
        mock_container.id = "abc123def456"
        mock_container.name = "platform-kafka"
        mock_container.status = "running"
        safety._find_container.return_value = mock_container
        safety.validate_target.return_value = None
        safety._client = MagicMock()

        ctrl = DockerController(safety=safety)
        ctrl._mock_container = mock_container
        return ctrl

    def test_stop_container_calls_validate_then_stop(self) -> None:
        """stop_container must call safety.validate_target before Docker stop."""
        ctrl = self._make_controller()
        result = ctrl.stop_container("kafka")
        ctrl._safety.validate_target.assert_called_once_with("kafka")
        ctrl._mock_container.stop.assert_called_once()
        assert "stopped_at" in result

    def test_start_container_calls_validate_then_start(self) -> None:
        """start_container must call validate before Docker start."""
        ctrl = self._make_controller()
        ctrl.start_container("kafka")
        ctrl._safety.validate_target.assert_called_once_with("kafka")
        ctrl._mock_container.start.assert_called_once()

    def test_restart_container_calls_restart(self) -> None:
        """restart_container must call Docker restart."""
        ctrl = self._make_controller()
        result = ctrl.restart_container("kafka")
        ctrl._mock_container.restart.assert_called_once()
        assert "restarted_at" in result

    def test_is_container_running_returns_true(self) -> None:
        """is_container_running must return True when status is 'running'."""
        ctrl = self._make_controller()
        ctrl._mock_container.status = "running"
        assert ctrl.is_container_running("kafka") is True

    def test_safety_validate_called_before_every_action(self) -> None:
        """Every DockerController method must call validate_target."""
        ctrl = self._make_controller()
        ctrl.stop_container("kafka")
        ctrl.start_container("kafka")
        ctrl.restart_container("kafka")
        assert ctrl._safety.validate_target.call_count == 3
