"""Unit and contract tests for GET /api/experiments/options."""

from __future__ import annotations

import asyncio
from typing import Any, ClassVar

import pytest
from fastapi.testclient import TestClient

from app.api.routes.experiments import get_experiment_options_endpoint
from app.main import app
from app.reliability.fault_injectors.base_fault import (
    FAULT_INJECTOR_REGISTRY,
    BaseFaultInjector,
    register_fault_injector,
)
from app.reliability.safety import (
    ALLOWED_TARGET_SERVICES,
    CRITICAL_PLATFORM_SERVICES,
    get_experiment_options,
)


def test_options_lists_allow_listed_targets_excluding_critical():
    options = get_experiment_options()
    targets = options["targets"]
    target_ids = {t["id"] for t in targets}

    expected_targets = set(ALLOWED_TARGET_SERVICES) - set(CRITICAL_PLATFORM_SERVICES)
    assert target_ids == expected_targets

    # Critical platform services must never appear
    for critical in CRITICAL_PLATFORM_SERVICES:
        assert critical not in target_ids
        for t in targets:
            assert t["id"] != critical
            assert t["service"] != critical


def test_options_lists_registered_faults():
    options = get_experiment_options()
    faults = options["faults"]
    fault_ids = {f["id"] for f in faults}

    assert fault_ids == set(FAULT_INJECTOR_REGISTRY.keys())
    assert "container_stop" in fault_ids
    assert "service_delay" in fault_ids

    # Every fault has required fields
    for f in faults:
        assert "id" in f
        assert "label" in f
        assert "description" in f
        assert "blastRadius" in f
        assert "params" in f
        assert "icon" in f


def test_dynamic_registry_addition_appears_in_options():
    fake_fault_type = "custom_chaos_test_fault"

    @register_fault_injector
    class FakeChaosFault(BaseFaultInjector):
        fault_type = fake_fault_type
        label = "Custom Chaos Test"
        description = "Test description"
        blast_radius = "Test blast radius"
        params = [{"name": "fake_param", "type": "str"}]
        icon = "zap"

        def inject(self, target_service: str, params: dict) -> dict:
            return {}

        def rollback(self, target_service: str, injection_metadata: dict) -> None:
            pass

    try:
        options = get_experiment_options()
        fault_ids = {f["id"] for f in options["faults"]}
        assert fake_fault_type in fault_ids
        fake_entry = next(f for f in options["faults"] if f["id"] == fake_fault_type)
        assert fake_entry["label"] == "Custom Chaos Test"
        assert fake_entry["blastRadius"] == "Test blast radius"
        assert fake_entry["icon"] == "zap"
        assert fake_entry["params"] == [{"name": "fake_param", "type": "str"}]
    finally:
        FAULT_INJECTOR_REGISTRY.pop(fake_fault_type, None)


def test_options_limits():
    options = get_experiment_options()
    limits = options["limits"]
    assert limits["minDurationSeconds"] == 1
    assert limits["maxDurationSeconds"] == 300


def test_options_endpoint_authenticated():
    res = asyncio.run(get_experiment_options_endpoint(current_user="admin"))
    assert "targets" in res
    assert "faults" in res
    assert "limits" in res
    assert res["limits"]["minDurationSeconds"] == 1
    assert res["limits"]["maxDurationSeconds"] == 300
