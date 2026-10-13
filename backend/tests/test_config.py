"""
tests/test_config.py
--------------------
Tests that the Docker Compose project name is pinned consistently across three
sources of truth:

  1. docker-compose.yml   top-level ``name:``
  2. backend service env  ``COMPOSE_PROJECT_NAME:``
  3. Settings default     ``Settings.COMPOSE_PROJECT_NAME``

Any disagreement here means safety.py would use a different project name than
the one Docker actually assigned to the containers, breaking the label check.
"""

import os
import pathlib

import pytest
import yaml  # PyYAML — available in backend requirements

from app.core.config import Settings

# Resolve docker-compose.yml.
# On the host the layout is: tests/ -> backend/ -> repo-root/ -> docker-compose.yml
# Inside the container /app/docker-compose.yml is intentionally NOT mounted.
_REPO_ROOT = pathlib.Path(__file__).parent.parent.parent
_HOST_COMPOSE = _REPO_ROOT / "docker-compose.yml"
_CONTAINER_COMPOSE = pathlib.Path("/app/docker-compose.yml")
_COMPOSE_FILE = _HOST_COMPOSE if _HOST_COMPOSE.exists() else _CONTAINER_COMPOSE

CANONICAL_NAME = "resilient-big-data-platform"


def test_compose_project_name_is_consistent():
    """
    Parse docker-compose.yml with PyYAML and assert that:
      1. docker-compose.yml top-level ``name`` matches CANONICAL_NAME.
      2. The backend service's COMPOSE_PROJECT_NAME env var matches CANONICAL_NAME.
      3. The Settings default for COMPOSE_PROJECT_NAME matches CANONICAL_NAME.
    """
    if not _COMPOSE_FILE.exists():
        pytest.skip(
            f"docker-compose.yml not found next to repo root (checked {_COMPOSE_FILE}); "
            "run from repo root or in CI"
        )

    with open(_COMPOSE_FILE, "r", encoding="utf-8") as fh:
        compose = yaml.safe_load(fh)

    # ── 1. top-level name: ─────────────────────────────────────────────────────
    compose_name = compose.get("name")
    assert compose_name == CANONICAL_NAME, (
        f"docker-compose.yml top-level name={compose_name!r} != {CANONICAL_NAME!r}. "
        "Update docker-compose.yml to add 'name: resilient-big-data-platform'."
    )

    # ── 2. backend service COMPOSE_PROJECT_NAME env var ────────────────────────
    backend_env = (
        compose.get("services", {})
        .get("backend", {})
        .get("environment", {})
    )
    # environment may be a dict or a list of "KEY=VAL" strings
    if isinstance(backend_env, dict):
        env_val = backend_env.get("COMPOSE_PROJECT_NAME") or backend_env.get(
            "PROJECT_CONTAINER_PREFIX"
        )
    else:
        env_val = None
        for entry in (backend_env or []):
            if entry.startswith("COMPOSE_PROJECT_NAME="):
                env_val = entry.split("=", 1)[1]
                break

    # The backend service uses PROJECT_CONTAINER_PREFIX which reads from
    # COMPOSE_PROJECT_NAME at runtime; if it uses the interpolation
    # ${COMPOSE_PROJECT_NAME:-resilient-big-data-platform} that is also fine.
    # Accept either the literal value or the interpolation pattern.
    if env_val is not None and "${" not in str(env_val):
        assert env_val == CANONICAL_NAME, (
            f"backend service COMPOSE_PROJECT_NAME={env_val!r} != {CANONICAL_NAME!r}."
        )
    # If env_val is None the project name comes only from the top-level name: pin.

    # ── 3. Settings default ─────────────────────────────────────────────────────
    # Temporarily clear COMPOSE_PROJECT_NAME so we read the baked-in default.
    saved = os.environ.pop("COMPOSE_PROJECT_NAME", None)
    try:
        settings = Settings()
        assert settings.compose_project_name == CANONICAL_NAME, (
            f"Settings.compose_project_name default={settings.compose_project_name!r} "
            f"!= {CANONICAL_NAME!r}."
        )
        assert settings.COMPOSE_PROJECT_NAME == CANONICAL_NAME, (
            f"Settings.COMPOSE_PROJECT_NAME default={settings.COMPOSE_PROJECT_NAME!r} "
            f"!= {CANONICAL_NAME!r}. Update backend/app/core/config.py."
        )
        assert settings.PROJECT_CONTAINER_PREFIX == CANONICAL_NAME, (
            f"Settings.PROJECT_CONTAINER_PREFIX default={settings.PROJECT_CONTAINER_PREFIX!r} "
            f"!= {CANONICAL_NAME!r}."
        )
    finally:
        if saved is not None:
            os.environ["COMPOSE_PROJECT_NAME"] = saved


def test_custom_compose_project_name_propagates_to_settings():
    """
    When COMPOSE_PROJECT_NAME is set to a different value (simulating a clone
    in a differently-named folder), Settings.PROJECT_CONTAINER_PREFIX must
    reflect that value so safety.py uses the correct project label.
    """
    saved = os.environ.get("COMPOSE_PROJECT_NAME")
    os.environ["COMPOSE_PROJECT_NAME"] = "some-other-folder-name"
    try:
        settings = Settings()
        assert settings.PROJECT_CONTAINER_PREFIX == "some-other-folder-name", (
            "PROJECT_CONTAINER_PREFIX did not pick up the custom "
            "COMPOSE_PROJECT_NAME value."
        )
    finally:
        if saved is None:
            os.environ.pop("COMPOSE_PROJECT_NAME", None)
        else:
            os.environ["COMPOSE_PROJECT_NAME"] = saved
