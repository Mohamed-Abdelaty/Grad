import asyncio
from typing import Any

import pytest
from app.pipeline.progress import RunProgressTracker

@pytest.fixture
def tracker():
    return RunProgressTracker()

def test_tracker_start_finish(tracker):
    tracker.start("run-123", "Test Pipeline")
    assert tracker.is_active("run-123")
    
    prog = tracker.get_progress("run-123")
    assert prog["status"] in ("active", "running")
    
    tracker.finish("run-123")
    assert not tracker.is_active("run-123")

def test_tracker_emit_and_get_events(tracker):
    tracker.start("run-1", "Test")
    tracker.emit("run-1", "test_event", {"foo": "bar"})
    
    events = tracker.get_events_after("run-1", 0)
    assert len(events) == 1
    assert events[0]["seq"] == 1
    assert events[0]["event_type"] == "test_event"
    assert events[0]["payload"] == {"foo": "bar"}

@pytest.mark.asyncio
async def test_tracker_subscribe_and_emit(tracker):
    tracker.set_loop(asyncio.get_running_loop())
    tracker.start("run-async", "Test Async")
    
    queue = tracker.subscribe("run-async")
    
    tracker.emit("run-async", "my_event", {})
    
    event = await asyncio.wait_for(queue.get(), timeout=1.0)
    assert event["event_type"] == "my_event"
    assert event["seq"] == 1
    
    tracker.unsubscribe("run-async", queue)
    tracker.finish("run-async")

def test_tracker_cancel_request(tracker):
    tracker.start("run-cancel", "Cancel Test")
    
    assert not tracker.is_cancel_requested("run-cancel")
    accepted = tracker.request_cancel("run-cancel")
    assert accepted is True
    assert tracker.is_cancel_requested("run-cancel")
    
    # finished runs shouldn't be cancellable
    tracker.finish("run-cancel")
    accepted2 = tracker.request_cancel("run-cancel")
    assert accepted2 is False


def test_tracker_cancel_sends_sigterm_and_sigkill(tracker):
    """Verify request_cancel sends SIGTERM then SIGKILL to the process group."""
    import signal
    import threading
    from unittest.mock import patch

    tracker.start("run-term-kill", "Test Term Kill")
    tracker.register_proc_pid("run-term-kill", 9999)

    killed_signals = []
    kill_done_event = threading.Event()

    def mock_killpg(pgid, sig):
        killed_signals.append((pgid, sig))
        if sig == signal.SIGKILL:
            kill_done_event.set()

    with patch("os.getpgid", return_value=8888), \
         patch("os.getpgrp", return_value=1111), \
         patch("os.killpg", side_effect=mock_killpg), \
         patch("time.sleep", return_value=None):
        accepted = tracker.request_cancel("run-term-kill")
        assert accepted is True

        assert kill_done_event.wait(timeout=2.0) is True

    assert (8888, signal.SIGTERM) in killed_signals
    assert (8888, signal.SIGKILL) in killed_signals


def test_tracker_cancel_does_not_terminate_backend_process(tracker):
    """Verify request_cancel will NEVER kill the backend's own process group."""
    from unittest.mock import patch

    tracker.start("run-safe", "Test Safe")
    tracker.register_proc_pid("run-safe", 1234)

    killed = []

    with patch("os.getpgid", return_value=5555), \
         patch("os.getpgrp", return_value=5555), \
         patch("os.killpg", side_effect=lambda pgid, sig: killed.append(sig)), \
         patch("time.sleep", return_value=None):
        accepted = tracker.request_cancel("run-safe")
        assert accepted is True

        import time
        time.sleep(0.05)

    # Process group was identical to backend's own process group, so killpg must not be called
    assert len(killed) == 0, f"Backend process group was targeted: {killed}"
