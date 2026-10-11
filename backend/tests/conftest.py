"""
backend/tests/conftest.py
Global fixtures and session-level teardown hooks for pytest.
"""

import logging
import pytest

logger = logging.getLogger(__name__)


def _sweep_chaoslab_containers() -> None:
    """Find and force-remove any containers labeled with chaoslab_test=true."""
    try:
        import docker
        client = docker.from_env()
        containers = client.containers.list(
            all=True,
            filters={"label": "chaoslab_test=true"}
        )
        for c in containers:
            try:
                c.remove(force=True)
            except Exception:
                pass
    except Exception:
        pass


@pytest.fixture(scope="session", autouse=True)
def sweep_chaoslab_test_containers():
    """Ensure all chaoslab test containers are swept at session start and end."""
    _sweep_chaoslab_containers()
    try:
        yield
    finally:
        _sweep_chaoslab_containers()
