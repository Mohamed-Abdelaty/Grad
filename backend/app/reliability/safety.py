"""
app/reliability/safety.py
--------------------------
THE MOST CRITICAL MODULE IN THE CHAOSLAB RELIABILITY ENGINE.

Safety Guarantees
-----------------
This module is the **single authority** through which every fault injector
and the experiment runner must pass before any Docker-level action is taken.

Guarantees provided:

1. **Allow-list enforcement** — only the exact set of services this platform
   owns (``ALLOWED_TARGET_SERVICES``) can ever be targeted.  An attacker who
   manipulates the ``PROJECT_CONTAINER_PREFIX`` env variable or crafts a
   malicious experiment YAML still cannot target arbitrary host containers,
   because the Docker label check (``com.docker.compose.project``) is a
   second, independent guard.

2. **Compose-project label verification** — the resolved Docker container
   MUST carry the label ``com.docker.compose.project`` matching this
   platform's own project name.  This makes it structurally impossible to
   target any container outside our own Docker Compose environment, even if
   the container name happens to match the expected pattern.

3. **Duration ceiling** — fault durations above ``MAX_FAULT_DURATION_SECONDS``
   are rejected here *in addition to* the Pydantic model validator.  Two
   independent enforcement points mean a misconfigured or patched Pydantic
   model cannot accidentally allow unbounded faults.

4. **Concurrency limit** — only one chaos experiment may run at a time.
   Compounding concurrent failures make measurement meaningless and could
   genuinely take down the entire stack.

Architectural rule
------------------
Every fault injector and DockerController method MUST call through
SafetyValidator.  No code in this codebase may call the Docker SDK for
reliability purposes without first passing through this module.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from app.core.config import get_settings
from app.reliability.experiment_models import MAX_FAULT_DURATION_SECONDS
from app.reliability.exceptions import (
    TooManyConcurrentExperimentsError,
    UnsafeTargetError,
)

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)

# ── Allow-list ─────────────────────────────────────────────────────────────────

ALLOWED_TARGET_SERVICES: frozenset[str] = frozenset(
    {
        "kafka",
        "spark-master",
        "spark-worker",
        "namenode",
        "datanode",
        "postgres",
        "backend",
    }
)
"""
Exact set of services that ChaosLab is permitted to target.

This is intentionally a ``frozenset`` (immutable) — nothing can add entries
at runtime.  To add a new allowed service, it must be added here explicitly
in a code review, not by configuration.
"""

ACTIVE_EXPERIMENT_STATUSES: frozenset[str] = frozenset({"pending", "running"})
"""All non-terminal ChaosLab lifecycle states."""

TERMINAL_EXPERIMENT_STATUSES: frozenset[str] = frozenset(
    {"succeeded", "failed", "cancelled"}
)


class SafetyValidator:
    """
    Gatekeeper for all chaos engineering operations.

    Every public method raises a typed exception when a safety constraint is
    violated — callers should never suppress these exceptions.

    Parameters
    ----------
    docker_client : docker.DockerClient | None
        Injected Docker client.  If ``None``, a default client is created on
        first use.  Accepting it as a parameter allows unit tests to mock the
        Docker SDK without patching global state.
    """

    def __init__(self, docker_client=None) -> None:
        """Initialise with an optional pre-built Docker client."""
        self._docker_client = docker_client

    @property
    def _client(self):
        """Lazy Docker client — not constructed until first use."""
        if self._docker_client is None:
            import docker  # type: ignore[import-untyped]

            self._docker_client = docker.from_env()
        return self._docker_client

    def _resolve_container_name(self, service_name: str) -> str:
        """
        Build the expected container name from the project prefix and service.

        Docker Compose names containers as ``<project>-<service>-<index>`` or
        ``<project>_<service>_<index>`` depending on version.  We try the
        ``-`` separator first (Compose v2 default) then fall back to ``_``
        (Compose v1 / older Docker).

        Returns
        -------
        str
            The resolved container name, or raises ``UnsafeTargetError``.
        """
        settings = get_settings()
        prefix = settings.PROJECT_CONTAINER_PREFIX

        # Compose v2 uses hyphens; v1 uses underscores
        candidates = [
            f"{prefix}-{service_name}-1",
            f"{prefix}-{service_name}",
            f"{prefix}_{service_name}_1",
            f"{prefix}_{service_name}",
            # Also accept containers named explicitly by container_name:
            f"platform-{service_name}",
        ]
        return candidates  # caller iterates

    def _find_container(self, service_name: str):
        """
        Locate the Docker container for ``service_name``.

        Search strategy (in order):
        1. Name-based: try all Compose naming conventions (v1/v2 prefix patterns
           and explicit ``platform-*`` names from docker-compose.yml).
        2. Label-based: if no name matches, search all running containers for
           one whose ``com.docker.compose.service`` label equals ``service_name``.
           This covers containers created with a custom name but the correct
           Compose service label (e.g. integration-test fixtures).

        Safety is **not** weakened by strategy 2: ``validate_target`` still
        enforces the allow-list and the project label check independently.

        Returns the container object or raises ``UnsafeTargetError``.
        """
        from docker.errors import DockerException, NotFound

        candidates = self._resolve_container_name(service_name)
        for name in candidates:
            try:
                container = self._client.containers.get(name)
                return container
            except NotFound:
                continue
            except (DockerException, OSError, PermissionError) as exc:
                raise UnsafeTargetError(
                    "ChaosLab cannot access the Docker socket. The backend "
                    "must be granted Docker socket permissions before it can "
                    "run reliability experiments.",
                    service_name=service_name,
                ) from exc
            except Exception:
                # A non-Docker error is treated as a name miss so the next
                # Compose naming convention can still be tried.
                continue

        # Fallback: search by com.docker.compose.service AND com.docker.compose.project.
        # Two-label filter is required: Docker has many compose projects on the host,
        # and a service name like "kafka" is common.  Using only the service label would
        # silently match a container from a completely different project.
        # all=True includes stopped/exited containers — _find_container is called before
        # is_container_running, start_container, and exec_in_container, all of which may
        # legitimately target a stopped container.
        try:
            project = get_settings().PROJECT_CONTAINER_PREFIX
            matches = self._client.containers.list(
                all=True,
                filters={
                    "label": [
                        f"com.docker.compose.service={service_name}",
                        f"com.docker.compose.project={project}",
                    ]
                },
            )
            if matches:
                return matches[0]
        except (DockerException, OSError, PermissionError) as exc:
            raise UnsafeTargetError(
                "ChaosLab cannot access the Docker socket.",
                service_name=service_name,
            ) from exc

        raise UnsafeTargetError(
            f"No running container found for service '{service_name}'. "
            f"Tried names: {candidates} and label "
            f"com.docker.compose.service={service_name!r}. Is the platform running?",
            service_name=service_name,
        )

    def validate_target(self, service_name: str) -> None:
        """
        Confirm it is safe to target ``service_name``.

        Checks (in order, all must pass):
        1. ``service_name`` is in ``ALLOWED_TARGET_SERVICES``.
        2. A container with the expected name actually exists in Docker.
        3. The container carries the ``com.docker.compose.project`` label
           matching this platform's own project name — prevents targeting
           any container outside our compose environment.

        Parameters
        ----------
        service_name : str
            The service to validate.

        Raises
        ------
        UnsafeTargetError
            If any check fails.
        """
        # ── Guard 1: allow-list ────────────────────────────────────────────────
        if service_name not in ALLOWED_TARGET_SERVICES:
            raise UnsafeTargetError(
                f"Service '{service_name}' is not in the ChaosLab allow-list. "
                f"Permitted targets: {sorted(ALLOWED_TARGET_SERVICES)}.",
                service_name=service_name,
            )

        # ── Guard 2: container must exist ──────────────────────────────────────
        container = self._find_container(service_name)

        # ── Guard 3: compose project label must match ──────────────────────────
        settings = get_settings()
        project_label = (container.labels or {}).get("com.docker.compose.project")
        expected_project = settings.PROJECT_CONTAINER_PREFIX

        if project_label is None:
            raise UnsafeTargetError(
                f"Container for service '{service_name}' has no "
                "'com.docker.compose.project' label. "
                "ChaosLab refuses to target containers it cannot confirm "
                "belong to this platform.",
                service_name=service_name,
            )

        # The compose project label is typically the lowercase directory name
        # or the value set by COMPOSE_PROJECT_NAME.  We match case-insensitively.
        if project_label.lower() != expected_project.lower():
            raise UnsafeTargetError(
                f"Container for service '{service_name}' belongs to project "
                f"'{project_label}', not '{expected_project}'. "
                "ChaosLab will not target containers from a different project.",
                service_name=service_name,
            )

        logger.info(
            "SafetyValidator: target '%s' (container '%s') validated OK.",
            service_name,
            container.name,
        )

    def validate_duration(self, duration: int) -> None:
        """
        Enforce the hard ceiling on fault duration.

        This is defence-in-depth: the Pydantic model validator enforces the
        same ceiling.  Two independent enforcement points ensure a patched or
        bypassed model cannot allow unbounded faults.

        Parameters
        ----------
        duration : int
            Fault duration in seconds.

        Raises
        ------
        UnsafeTargetError
            If ``duration`` exceeds ``MAX_FAULT_DURATION_SECONDS`` or <= 0.
        """
        if duration <= 0:
            raise UnsafeTargetError(
                f"Fault duration must be positive, got {duration}s."
            )
        if duration > MAX_FAULT_DURATION_SECONDS:
            raise UnsafeTargetError(
                f"Fault duration {duration}s exceeds the hard safety ceiling "
                f"of {MAX_FAULT_DURATION_SECONDS}s. "
                "This limit is enforced regardless of YAML configuration."
            )

    def enforce_max_concurrent_experiments(
        self, db_session, excluding_run_id=None
    ) -> None:
        """
        Reject a new experiment if one is already running.

        The PostgreSQL advisory lock serializes the count-and-start decision
        across backend workers. The caller's already-created ``pending`` run
        is excluded from the count.

        Parameters
        ----------
        db_session : sqlalchemy.orm.Session
            A synchronous SQLAlchemy session (the experiment runner uses sync
            sessions for simplicity; the Docker operations are blocking anyway).

        Raises
        ------
        TooManyConcurrentExperimentsError
            If one or more experiments currently have ``status='running'``.
        """
        from sqlalchemy import func, select, text
        from app.models.experiment_run import ExperimentRun

        bind = db_session.get_bind()
        if bind.dialect.name == "postgresql":
            db_session.execute(text("SELECT pg_advisory_xact_lock(918273645)"))

        stmt = select(func.count()).select_from(ExperimentRun).where(
            ExperimentRun.status.in_(ACTIVE_EXPERIMENT_STATUSES)
        )
        if excluding_run_id is not None:
            stmt = stmt.where(ExperimentRun.id != excluding_run_id)
        running_count = db_session.execute(stmt).scalar_one()

        if running_count > 0:
            raise TooManyConcurrentExperimentsError(running_count=running_count)

        logger.debug(
            "SafetyValidator: concurrency check passed (0 other active experiments)."
        )

    def reap_stale_experiments(self, db_session, now: datetime | None = None) -> int:
        """Fail active runs that outlived their declared recovery window."""
        from sqlalchemy import select
        from app.models.experiment_run import ExperimentRun

        now = now or datetime.now(tz=timezone.utc)
        runs = db_session.execute(
            select(ExperimentRun).where(
                ExperimentRun.status.in_(ACTIVE_EXPERIMENT_STATUSES)
            )
        ).scalars().all()
        reaped = 0
        for run in runs:
            if run.started_at is None:
                continue
            config = (run.params or {}).get("experiment_config", {})
            fault = config.get("fault", {})
            validation = config.get("validation", {})
            max_age = (
                float(fault.get("duration", 0))
                + 60
                + float(validation.get("max_recovery_time", 60))
            )
            if (now - run.started_at).total_seconds() <= max_age:
                continue
            results = dict(run.results or {})
            results["error"] = (
                "Experiment was marked failed after exceeding its fault duration, "
                "grace period, and maximum recovery time."
            )
            run.results = results
            run.status = "failed"
            run.finished_at = now
            reaped += 1
        if reaped:
            db_session.flush()
        return reaped
