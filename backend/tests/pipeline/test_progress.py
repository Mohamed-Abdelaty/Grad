import asyncio
from typing import Any

import pytest
from app.pipeline.progress import RunProgressTracker

@pytest.fixture
def tracker():
    return RunProgressTracker()

def test_tracker_start_finish(tracker):
    tracker.start("run-123", "Test Pipeline")
    assert tracker.is_running("run-123")
    
    prog = tracker.get_progress("run-123")
    assert prog["status"] == "running"
    
    tracker.finish("run-123")
    assert not tracker.is_running("run-123")

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
