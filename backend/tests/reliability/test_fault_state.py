"""Tests for FaultStateRegistry and its integration with RunProgressTracker."""

from __future__ import annotations

import threading
import time
from unittest.mock import MagicMock

import pytest

from app.pipeline.progress import RunProgressTracker, tracker
from app.reliability.fault_state import FaultStateRegistry, fault_state_registry


@pytest.fixture(autouse=True)
def reset_global_fault_state():
    fault_state_registry.clear()
    yield
    fault_state_registry.clear()


def test_fault_state_transitions():
    fake_time = 100.0
    reg = FaultStateRegistry(time_fn=lambda: fake_time)

    assert reg.current() is None
    assert reg.snapshot() is None

    # Inject
    state = reg.set_injected(
        experiment_id="exp-1",
        target="kafka",
        fault_type="container_stop",
        duration_seconds=20,
        injected_at="2026-10-15T00:00:00Z",
    )
    assert state.phase == "injected"
    assert state.target == "kafka"
    assert reg.current() is not None
    assert reg.current().phase == "injected"

    snap = reg.snapshot()
    assert snap["experimentId"] == "exp-1"
    assert snap["phase"] == "injected"
    assert snap["target"] == "kafka"
    assert snap["durationSeconds"] == 20

    # Recovering
    reg.set_recovering("exp-1")
    assert reg.current().phase == "recovering"
    assert reg.snapshot()["phase"] == "recovering"

    # Recovered
    reg.set_recovered("exp-1", recovery_seconds=12.5)
    assert reg.current().phase == "recovered"
    assert reg.current().recoverySeconds == 12.5
    assert reg.snapshot()["recoverySeconds"] == 12.5
    assert reg.current().expires_at == 160.0

    # Before expiry: still returns recovered
    fake_time = 159.0
    assert reg.current() is not None
    assert reg.current().phase == "recovered"

    # After expiry: returns None
    fake_time = 160.1
    assert reg.current() is None
    assert reg.snapshot() is None


def test_clear_on_failure_and_cancel():
    reg = FaultStateRegistry()
    reg.set_injected("exp-2", "spark-master", "cpu_stress", 30)
    assert reg.current() is not None

    # Clear for another experiment does nothing
    reg.clear("other-exp")
    assert reg.current() is not None

    # Clear for this experiment removes it immediately
    reg.clear("exp-2")
    assert reg.current() is None


def test_tracker_merges_fault_for_running_run():
    prog_tracker = RunProgressTracker()
    run_id = "run-test-active"
    prog_tracker.start(run_id, "sales_pipeline")

    # Before fault: fault is None
    snap_before = prog_tracker.get_progress(run_id)
    assert snap_before["fault"] is None

    # Now inject fault in global registry
    fault_state_registry.set_injected("exp-run-1", "kafka", "container_stop", 20)
    try:
        # Running run gets fault merged
        snap_during = prog_tracker.get_progress(run_id)
        assert snap_during["fault"] is not None
        assert snap_during["fault"]["experimentId"] == "exp-run-1"
        assert snap_during["fault"]["target"] == "kafka"
        assert snap_during["fault"]["phase"] == "injected"

        # snapshot() helper also returns this
        snap_helper = prog_tracker.snapshot(run_id)
        assert snap_helper["fault"] is not None
        assert snap_helper["fault"]["target"] == "kafka"
    finally:
        fault_state_registry.clear("exp-run-1")


def test_tracker_does_not_merge_fault_for_run_ended_before_injection():
    prog_tracker = RunProgressTracker()
    old_run_id = "run-test-completed"
    prog_tracker.start(old_run_id, "old_pipeline")

    # Complete the run
    prog_tracker.emit(old_run_id, "run_success", {})
    snap_finished = prog_tracker.get_progress(old_run_id)
    assert snap_finished["status"] == "succeeded"
    assert snap_finished["fault"] is None

    # Now inject fault later
    fault_state_registry.set_injected("exp-run-2", "namenode", "service_delay", 15)
    try:
        # Finished run still has fault=None
        snap_after = prog_tracker.get_progress(old_run_id)
        assert snap_after["fault"] is None
    finally:
        fault_state_registry.clear("exp-run-2")


def test_thread_safety_under_concurrent_reads():
    reg = FaultStateRegistry()
    reg.set_injected("exp-concurrent", "kafka", "container_stop", 10)

    errors = []

    def reader():
        try:
            for _ in range(500):
                c = reg.current()
                s = reg.snapshot()
                if c is not None and s is None:
                    errors.append("Mismatch between current and snapshot")
        except Exception as e:
            errors.append(str(e))

    threads = [threading.Thread(target=reader) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(errors) == 0
    reg.clear("exp-concurrent")
