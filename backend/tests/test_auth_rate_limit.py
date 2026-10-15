"""
backend/tests/test_auth_rate_limit.py
-------------------------------------
Unit tests for login sliding-window rate limiting.
"""

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.api.routes.auth import reset_login_rate_limits
from app.core.config import get_settings


@pytest.fixture(autouse=True)
def clean_rate_limits():
    """Ensure rate limits are clean before each test."""
    reset_login_rate_limits()
    yield
    reset_login_rate_limits()


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


def test_auth_rate_limit_enforcement(client: TestClient):
    """5 failed attempts return 401; 6th returns 429 with Retry-After header."""
    headers = {"X-Forwarded-For": "192.168.1.100"}
    bad_data = {"username": "admin", "password": "wrongpassword"}

    # 5 failed attempts
    for i in range(5):
        resp = client.post("/api/auth/login", data=bad_data, headers=headers)
        assert resp.status_code == 401, f"Attempt {i+1} should fail with 401"

    # 6th attempt should be blocked by rate limiter
    resp = client.post("/api/auth/login", data=bad_data, headers=headers)
    assert resp.status_code == 429, "6th attempt should be rate limited with 429"
    assert "Retry-After" in resp.headers
    retry_after = int(resp.headers["Retry-After"])
    assert 0 < retry_after <= 60


def test_auth_rate_limit_reset_on_success(client: TestClient):
    """Successful login resets failed attempts counter."""
    settings = get_settings()
    headers = {"X-Forwarded-For": "192.168.1.101"}
    bad_data = {"username": settings.ADMIN_USERNAME, "password": "wrongpassword"}
    good_data = {"username": settings.ADMIN_USERNAME, "password": settings.ADMIN_PASSWORD}

    # 4 failed attempts
    for _ in range(4):
        resp = client.post("/api/auth/login", data=bad_data, headers=headers)
        assert resp.status_code == 401

    # 5th attempt succeeds
    resp = client.post("/api/auth/login", data=good_data, headers=headers)
    assert resp.status_code == 200
    assert "access_token" in resp.json()

    # Next failed attempt should be attempt 1 again (401, not 429)
    resp = client.post("/api/auth/login", data=bad_data, headers=headers)
    assert resp.status_code == 401


def test_auth_rate_limit_independent_ips(client: TestClient):
    """Different IPs or usernames have independent buckets."""
    bad_data = {"username": "admin", "password": "wrongpassword"}

    # Exhaust limit for IP 1
    for _ in range(5):
        client.post("/api/auth/login", data=bad_data, headers={"X-Forwarded-For": "10.0.0.1"})

    resp1 = client.post("/api/auth/login", data=bad_data, headers={"X-Forwarded-For": "10.0.0.1"})
    assert resp1.status_code == 429

    # IP 2 should still be allowed
    resp2 = client.post("/api/auth/login", data=bad_data, headers={"X-Forwarded-For": "10.0.0.2"})
    assert resp2.status_code == 401
