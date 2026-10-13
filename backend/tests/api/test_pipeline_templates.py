"""
backend/tests/api/test_pipeline_templates.py
--------------------------------------------
Unit/API tests for the /api/pipeline-templates endpoint.
"""

from datetime import timedelta
import pytest
from fastapi.testclient import TestClient

from app.core.security import create_access_token
from app.main import app
from app.services.pipeline_config_service import validate_pipeline_config


@pytest.fixture
def auth_headers():
    token = create_access_token({"sub": "admin"}, timedelta(minutes=15))
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture
def client():
    with TestClient(app) as c:
        yield c


def test_templates_unauthenticated_returns_401(client: TestClient):
    resp = client.get("/api/pipeline-templates")
    assert resp.status_code == 401


def test_templates_authenticated_returns_exactly_three(client: TestClient, auth_headers: dict):
    resp = client.get("/api/pipeline-templates", headers=auth_headers)
    assert resp.status_code == 200
    templates = resp.json()
    assert isinstance(templates, list)
    assert len(templates) == 3

    ids = [t["id"] for t in templates]
    assert sorted(ids) == ["iot-json", "network-logs", "sales-csv"]

    for t in templates:
        assert t["name"]
        assert t["description"]
        assert t["yaml"]
        # Content parses and validates cleanly
        val = validate_pipeline_config(t["yaml"])
        assert val["valid"] is True, f"Template {t['id']} failed validation: {val['errors']}"


def test_templates_no_directory_traversal(client: TestClient, auth_headers: dict):
    # Verify that query params or path params cannot be used to read arbitrary files
    resp = client.get("/api/pipeline-templates?file=../../etc/passwd", headers=auth_headers)
    assert resp.status_code == 200
    templates = resp.json()
    assert len(templates) == 3
    for t in templates:
        assert t["id"] in {"sales-csv", "iot-json", "network-logs"}
