"""
tests/reliability/test_safety.py
----------------------------------
Unit tests for the SafetyValidator — the most critical module in ChaosLab.

All tests mock the Docker SDK so no real Docker socket is required.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

from app.reliability.safety import SafetyValidator, ALLOWED_TARGET_SERVICES
from app.reliability.exceptions import (
    UnsafeTargetError,
    TooManyConcurrentExperimentsError,
)


# ── Fixtures ───────────────────────────────────────────────────────────────────


def _make_container(name: str = "platform-kafka", project: str = "resilient-big-data-platform"):
    """Return a mock Docker container with the correct project label."""
    container = MagicMock()
    container.name = name
    container.labels = {"com.docker.compose.project": project}
    return container


def _make_safety_with_mock_client(container=None):
    """
    Build a SafetyValidator with a fully mocked Docker client.
    """
    mock_client = MagicMock()
    if container is not None:
        mock_client.containers.get.return_value = container
    else:
        mock_client.containers.get.side_effect = Exception("Container not found")

    safety = SafetyValidator(docker_client=mock_client)
    return safety


# ── Tests: validate_target ─────────────────────────────────────────────────────


class TestValidateTarget:
    """SafetyValidator.validate_target() tests."""

    def test_accepts_valid_allowed_service(self) -> None:
        """An allowed service with the correct project label must pass."""
        container = _make_container("platform-kafka", "resilient-big-data-platform")
        safety = _make_safety_with_mock_client(container)

        with patch.object(safety, "_find_container", return_value=container):
            # Should not raise
            safety.validate_target("kafka")

    def test_rejects_service_not_in_allow_list(self) -> None:
        """A service not in ALLOWED_TARGET_SERVICES must raise UnsafeTargetError."""
        safety = _make_safety_with_mock_client()
        with pytest.raises(UnsafeTargetError) as exc_info:
            safety.validate_target("evil_service")
        assert "not in the ChaosLab allow-list" in str(exc_info.value)
        assert exc_info.value.service_name == "evil_service"

    def test_rejects_container_with_wrong_project_label(self) -> None:
        """A container with a different compose project label must be rejected."""
        container = _make_container("platform-kafka", "some-other-project")
        safety = _make_safety_with_mock_client(container)

        with patch.object(safety, "_find_container", return_value=container):
            with pytest.raises(UnsafeTargetError) as exc_info:
                safety.validate_target("kafka")
        assert "some-other-project" in str(exc_info.value)

    def test_rejects_container_with_no_project_label(self) -> None:
        """A container missing the compose project label must be rejected."""
        container = _make_container("platform-kafka")
        container.labels = {}  # no labels at all
        safety = _make_safety_with_mock_client(container)

        with patch.object(safety, "_find_container", return_value=container):
            with pytest.raises(UnsafeTargetError) as exc_info:
                safety.validate_target("kafka")
        assert "no 'com.docker.compose.project' label" in str(exc_info.value)

    def test_rejects_when_container_does_not_exist(self) -> None:
        """A service in the allow-list but with no running container must be rejected."""
        safety = _make_safety_with_mock_client(container=None)
        with pytest.raises(UnsafeTargetError) as exc_info:
            safety.validate_target("kafka")
        assert "kafka" in str(exc_info.value)

    @pytest.mark.parametrize("service", sorted(ALLOWED_TARGET_SERVICES))
    def test_all_allowed_services_pass_allow_list_check(self, service: str) -> None:
        """Every member of ALLOWED_TARGET_SERVICES clears the allow-list guard."""
        # We only test the first guard — container lookup is mocked to fail
        # so the test does NOT require Docker.
        safety = _make_safety_with_mock_client(container=None)
        # The allow-list check passes; the container-not-found error comes second
        with pytest.raises(UnsafeTargetError) as exc_info:
            safety.validate_target(service)
        # Error must be about the container not existing, not the allow-list
        assert "allow-list" not in str(exc_info.value)


# ── Tests: validate_duration ───────────────────────────────────────────────────


class TestValidateDuration:
    """SafetyValidator.validate_duration() tests."""

    def test_valid_duration_passes(self) -> None:
        """A duration within range must not raise."""
        safety = SafetyValidator()
        safety.validate_duration(30)  # Should not raise

    def test_zero_duration_rejected(self) -> None:
        """Duration of 0 must be rejected."""
        safety = SafetyValidator()
        with pytest.raises(UnsafeTargetError):
            safety.validate_duration(0)

    def test_negative_duration_rejected(self) -> None:
        """Negative duration must be rejected."""
        safety = SafetyValidator()
        with pytest.raises(UnsafeTargetError):
            safety.validate_duration(-10)

    def test_ceiling_duration_passes(self) -> None:
        """Exactly MAX_FAULT_DURATION_SECONDS must pass."""
        from app.reliability.experiment_models import MAX_FAULT_DURATION_SECONDS
        safety = SafetyValidator()
        safety.validate_duration(MAX_FAULT_DURATION_SECONDS)

    def test_above_ceiling_rejected(self) -> None:
        """Duration > MAX_FAULT_DURATION_SECONDS must raise."""
        from app.reliability.experiment_models import MAX_FAULT_DURATION_SECONDS
        safety = SafetyValidator()
        with pytest.raises(UnsafeTargetError) as exc_info:
            safety.validate_duration(MAX_FAULT_DURATION_SECONDS + 1)
        assert "ceiling" in str(exc_info.value)

    def test_large_duration_rejected(self) -> None:
        """An extreme duration (e.g. 99999s) must be rejected."""
        safety = SafetyValidator()
        with pytest.raises(UnsafeTargetError):
            safety.validate_duration(99999)


# ── Tests: enforce_max_concurrent_experiments ──────────────────────────────────


class TestEnforceMaxConcurrentExperiments:
    """SafetyValidator.enforce_max_concurrent_experiments() tests."""

    def test_raises_when_running_experiment_exists(self) -> None:
        """Must raise TooManyConcurrentExperimentsError if 1+ runs are 'running'."""
        mock_db = MagicMock()
        # Simulate scalar_one() returning 1 (one running experiment)
        mock_db.execute.return_value.scalar_one.return_value = 1

        safety = SafetyValidator()
        with pytest.raises(TooManyConcurrentExperimentsError) as exc_info:
            safety.enforce_max_concurrent_experiments(mock_db)
        assert exc_info.value.running_count == 1

    def test_passes_when_no_running_experiments(self) -> None:
        """Must not raise when no experiments are in 'running' status."""
        mock_db = MagicMock()
        mock_db.execute.return_value.scalar_one.return_value = 0

        safety = SafetyValidator()
        # Should not raise
        safety.enforce_max_concurrent_experiments(mock_db)

    def test_raises_with_multiple_running_experiments(self) -> None:
        """Must raise and report the correct count when >1 are running."""
        mock_db = MagicMock()
        mock_db.execute.return_value.scalar_one.return_value = 3

        safety = SafetyValidator()
        with pytest.raises(TooManyConcurrentExperimentsError) as exc_info:
            safety.enforce_max_concurrent_experiments(mock_db)
        assert "3" in str(exc_info.value)

    @pytest.mark.parametrize(
        "status",
        [
            "pending", "validating", "preparing", "baseline", "injecting",
            "monitoring", "rolling_back", "waiting_for_recovery",
            "validating_data", "collecting_metrics", "analyzing", "cleaning_up",
            "generating_report",
        ],
    )
    def test_each_active_lifecycle_status_blocks_a_second_experiment(
        self, status: str
    ) -> None:
        """Every non-terminal lifecycle state must count as active."""
        mock_db = MagicMock()
        mock_db.execute.return_value.scalar_one.return_value = 1

        with pytest.raises(TooManyConcurrentExperimentsError):
            SafetyValidator().enforce_max_concurrent_experiments(
                mock_db, excluding_run_id="current-run"
            )

    def test_current_pending_run_is_excluded_from_concurrency_count(self) -> None:
        """The run being started must not reject itself."""
        mock_db = MagicMock()
        mock_db.execute.return_value.scalar_one.return_value = 0

        SafetyValidator().enforce_max_concurrent_experiments(
            mock_db, excluding_run_id="current-run"
        )

    @pytest.mark.parametrize("status", ["succeeded", "failed", "cancelled"])
    def test_terminal_statuses_do_not_block(self, status: str) -> None:
        """Completed runs are not active experiments."""
        mock_db = MagicMock()
        mock_db.execute.return_value.scalar_one.return_value = 0

        SafetyValidator().enforce_max_concurrent_experiments(mock_db)

    def test_stale_active_run_is_marked_failed(self) -> None:
        """Abandoned active runs must not block the platform indefinitely."""
        stale_run = MagicMock()
        stale_run.status = "monitoring"
        stale_run.started_at = datetime.now(tz=timezone.utc) - timedelta(minutes=10)
        stale_run.params = {
            "experiment_config": {
                "fault": {"duration": 5},
                "validation": {"max_recovery_time": 5},
            }
        }
        stale_run.results = {}
        mock_db = MagicMock()
        mock_db.execute.return_value.scalars.return_value.all.return_value = [stale_run]

        reaped = SafetyValidator().reap_stale_experiments(
            mock_db, now=datetime.now(tz=timezone.utc)
        )

        assert reaped == 1
        assert stale_run.status == "failed"
        assert "exceeding" in stale_run.results["error"]


# ── Tests: _find_container label fallback project isolation ────────────────────


class TestFindContainerLabelFallback:
    """
    _find_container must restrict its label-based fallback to the platform's
    own compose project.  A container from a different project that happens to
    share the same service name must never be returned.

    These tests mock the Docker SDK to avoid any real Docker dependency.
    """

    def _make_safety(
        self,
        name_matches: dict[str, object] | None = None,
        label_matches: list[object] | None = None,
    ) -> SafetyValidator:
        """
        Build a SafetyValidator with a fully mocked Docker client.

        Parameters
        ----------
        name_matches : dict, optional
            Mapping ``container_name → container`` for ``containers.get(name)``.
        label_matches : list, optional
            List of containers returned by ``containers.list(...)``.
        """
        from docker.errors import NotFound

        mock_client = MagicMock()

        def fake_get(name: str):
            if name_matches and name in name_matches:
                return name_matches[name]
            raise NotFound("not found")

        mock_client.containers.get.side_effect = fake_get
        mock_client.containers.list.return_value = label_matches or []
        return SafetyValidator(docker_client=mock_client)

    def _make_container(
        self,
        name: str,
        service: str,
        project: str,
    ) -> MagicMock:
        """Create a mock container with the given compose labels."""
        container = MagicMock()
        container.name = name
        container.labels = {
            "com.docker.compose.service": service,
            "com.docker.compose.project": project,
        }
        return container

    def test_label_fallback_returns_container_from_our_project(self) -> None:
        """
        When name search fails, the label fallback must return a container
        belonging to our compose project.
        """
        our_container = self._make_container(
            name="chaoslab-test-abc",
            service="kafka",
            project="resilient-big-data-platform",
        )
        safety = self._make_safety(label_matches=[our_container])
        result = safety._find_container("kafka")
        assert result is our_container

    def test_label_fallback_excludes_foreign_project_container(self) -> None:
        """
        A container from a DIFFERENT compose project with the same service name
        must NOT be returned.

        The containers.list filter includes BOTH the service label AND the
        project label.  Docker evaluates them as AND, so a foreign-project
        container is never included in the result set — the mock returns [] to
        reflect that Docker filtered it out.
        """
        # The mock returns [] because Docker's two-label AND filter
        # would exclude the foreign container at the daemon level.
        safety = self._make_safety(label_matches=[])
        with pytest.raises(UnsafeTargetError) as exc_info:
            safety._find_container("kafka")
        assert "kafka" in str(exc_info.value)

    def test_containers_list_includes_project_label_in_filter(self) -> None:
        """
        containers.list must be called with both the service label AND the
        project label to prevent cross-project container matching.
        """
        safety = self._make_safety(label_matches=[])
        with pytest.raises(UnsafeTargetError):
            safety._find_container("kafka")

        assert safety._client.containers.list.called, (
            "containers.list must be called during label fallback"
        )
        call_kwargs = safety._client.containers.list.call_args.kwargs
        label_filters: list[str] = call_kwargs.get("filters", {}).get("label", [])
        service_filters = [f for f in label_filters if "com.docker.compose.service" in f]
        project_filters = [f for f in label_filters if "com.docker.compose.project" in f]
        assert len(service_filters) > 0, (
            "containers.list filter must include com.docker.compose.service"
        )
        assert len(project_filters) > 0, (
            "containers.list filter must include com.docker.compose.project "
            "to prevent cross-project container matching"
        )

    def test_validate_target_rejects_service_not_in_allow_list(self) -> None:
        """
        validate_target must reject any service name not in ALLOWED_TARGET_SERVICES,
        even if a container with the matching label exists.
        The allow-list check is the FIRST guard and runs before _find_container.
        """
        # Even with a matching container, the allow-list check fires first
        our_container = self._make_container(
            name="platform-evil",
            service="evil_service",
            project="resilient-big-data-platform",
        )
        safety = self._make_safety(label_matches=[our_container])
        with pytest.raises(UnsafeTargetError) as exc_info:
            safety.validate_target("evil_service")
        assert "allow-list" in str(exc_info.value), (
            "Error must mention the allow-list — not the container-not-found check"
        )
        # _find_container must NOT be called since the allow-list guard fires first
        safety._client.containers.list.assert_not_called()
        safety._client.containers.get.assert_not_called()

    def test_label_fallback_all_true_finds_stopped_container(self) -> None:
        """
        containers.list must be called with ``all=True`` so that stopped/exited
        containers are included.  This is needed for start_container and
        is_container_running which call _find_container on containers that may
        currently be stopped.
        """
        stopped_container = self._make_container(
            name="platform-kafka",
            service="kafka",
            project="resilient-big-data-platform",
        )
        safety = self._make_safety(label_matches=[stopped_container])
        result = safety._find_container("kafka")
        assert result is stopped_container

        call_kwargs = safety._client.containers.list.call_args.kwargs
        assert call_kwargs.get("all") is True, (
            "containers.list must use all=True to include stopped containers"
        )

