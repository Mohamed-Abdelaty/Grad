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


def test_progress_canonical_schema_all_keys_and_defaults(tracker):
    """Verify that get_progress returns all 15 top-level keys with canonical defaults."""
    run_id = "test-schema-run-1"
    tracker.start(run_id, "Test Schema")

    prog = tracker.get_progress(run_id)
    assert prog is not None

    expected_keys = {
        "runId", "status", "mode", "startedAt", "updatedAt", "completedAt",
        "stages", "source", "kafka", "spark", "hdfs",
        "throughput", "etaSeconds", "errors", "fault",
    }
    assert set(prog.keys()) == expected_keys
    assert prog["runId"] == run_id
    assert prog["status"] == "running"
    assert prog["mode"] == "batch"
    assert prog["completedAt"] is None
    assert prog["etaSeconds"] is None
    assert prog["fault"] is None
    assert prog["errors"] == []
    assert prog["throughput"] == {"recordsPerSecond": None, "samples": []}
    assert prog["source"] == {"type": None, "rowsTotal": None, "rowsRead": None}
    assert prog["kafka"] == {
        "topic": None, "partitions": None, "published": None, "failed": 0,
        "endOffsets": None, "consumerLag": None,
    }
    assert prog["spark"] == {
        "appId": None, "tasksTotal": None, "tasksDone": None, "activeStage": None,
        "recordsIn": None, "recordsOut": None, "durationMs": None,
    }
    assert prog["hdfs"] == {
        "path": None, "filesWritten": None, "bytesWritten": None,
        "rowsWritten": None, "verified": None,
    }

    # Verify exactly 4 stages present with valid initial states
    assert len(prog["stages"]) == 4
    stage_keys = [s["key"] for s in prog["stages"]]
    assert stage_keys == ["source", "kafka", "spark", "hdfs"]
    for s in prog["stages"]:
        assert set(s.keys()) == {"key", "state", "startedAt", "finishedAt", "error"}
        assert s["state"] == "pending"
        assert s["startedAt"] is None
        assert s["finishedAt"] is None
        assert s["error"] is None

    tracker.finish(run_id)


def test_progress_stage_transitions_and_allowed_states(tracker):
    """Verify stage transitions and that all states come from the allowed set."""
    run_id = "test-transitions-run"
    tracker.start(run_id, "Test Transitions")

    # 1. Start source stage
    tracker.emit(run_id, "source_start", {"plugin": "csv"})
    prog = tracker.get_progress(run_id)
    stages = {s["key"]: s for s in prog["stages"]}
    assert stages["source"]["state"] == "active"
    assert stages["source"]["startedAt"] is not None
    assert prog["source"]["type"] == "csv"

    # 2. Finish source stage
    tracker.emit(run_id, "source_done", {"rows": 150})
    prog = tracker.get_progress(run_id)
    stages = {s["key"]: s for s in prog["stages"]}
    assert stages["source"]["state"] == "done"
    assert stages["source"]["finishedAt"] is not None
    assert prog["source"]["rowsTotal"] == 150
    assert prog["source"]["rowsRead"] == 150

    # 3. Simulate failure during kafka stage
    tracker.emit(run_id, "kafka_start", {"topic": "test-topic"})
    tracker.emit(run_id, "run_failed", {"error": "Broker unavailable"})
    prog = tracker.get_progress(run_id)
    assert prog["status"] == "failed"
    assert prog["errors"] == ["Broker unavailable"]

    stages = {s["key"]: s for s in prog["stages"]}
    assert stages["source"]["state"] == "done"
    assert stages["kafka"]["state"] == "error"
    assert stages["kafka"]["error"] == "Broker unavailable"
    assert stages["spark"]["state"] == "skipped"
    assert stages["hdfs"]["state"] == "skipped"

    allowed_states = {"pending", "active", "done", "error", "skipped"}
    for s in prog["stages"]:
        assert s["state"] in allowed_states

    tracker.finish(run_id)


@pytest.mark.asyncio
async def test_sse_generator_emits_progress_frames_and_done():
    """Verify _sse_generator emits at least 3 progress frames and ends with event: done."""
    import uuid
    from unittest.mock import AsyncMock, MagicMock
    from app.pipeline.progress import tracker as global_tracker
    from app.api.routes.pipeline_runs import _sse_generator

    global_tracker.set_loop(asyncio.get_running_loop())
    run_id = str(uuid.uuid4())
    global_tracker.start(run_id, "SSE Test")

    # Create mock AsyncSession
    mock_db = AsyncMock()
    mock_run = MagicMock()
    mock_run.status = "running"
    mock_db.get.return_value = mock_run

    frames = []

    async def _consume():
        gen = _sse_generator(run_id, mock_db)
        async for chunk in gen:
            frames.append(chunk)
            # Once we see 3 progress frames, end the run to verify clean termination
            progress_count = sum(1 for f in frames if f.startswith("event: progress"))
            if progress_count >= 3:
                global_tracker.emit(run_id, "run_success", {})

    # Run generator with a timeout guard
    await asyncio.wait_for(_consume(), timeout=5.0)

    # Validate output frames
    progress_frames = [f for f in frames if f.startswith("event: progress")]
    done_frames = [f for f in frames if f.startswith("event: done")]

    assert len(progress_frames) >= 3, f"Expected at least 3 progress frames, got {len(progress_frames)}"
    assert len(done_frames) == 1, f"Expected 1 done frame, got {len(done_frames)}"
    assert '"status": "succeeded"' in done_frames[0]

    global_tracker.finish(run_id)


@pytest.mark.asyncio
async def test_sse_heartbeat_interval_and_emission():
    """Verify that HEARTBEAT_INTERVAL is 15.0 seconds and heartbeats are emitted."""
    from app.api.routes.pipeline_runs import HEARTBEAT_INTERVAL, _sse_generator
    from unittest.mock import AsyncMock, MagicMock, patch
    import uuid

    assert HEARTBEAT_INTERVAL == 15.0

    mock_db = AsyncMock()
    mock_run = MagicMock()
    mock_run.status = "running"
    mock_db.get.return_value = mock_run

    run_id = str(uuid.uuid4())
    from app.pipeline.progress import tracker as global_tracker
    global_tracker.set_loop(asyncio.get_running_loop())
    global_tracker.start(run_id, "Heartbeat Test")

    frames = []
    fake_time = [1000.0]

    def get_fake_time():
        # Advance by 16 seconds once initial frames are emitted
        if len(frames) >= 1:
            fake_time[0] += 16.0
        return fake_time[0]

    async def _consume():
        with patch("time.time", side_effect=get_fake_time):
            gen = _sse_generator(run_id, mock_db)
            async for chunk in gen:
                frames.append(chunk)
                if any(": heartbeat" in f or "event: heartbeat" in f for f in frames):
                    global_tracker.emit(run_id, "run_success", {})
                    break

    await asyncio.wait_for(_consume(), timeout=5.0)

    heartbeat_frames = [f for f in frames if ": heartbeat" in f or "event: heartbeat" in f]
    assert len(heartbeat_frames) >= 1, f"Expected heartbeat frame, got {frames}"
    assert ": heartbeat" in heartbeat_frames[0]
    assert "event: heartbeat" in heartbeat_frames[0]

    global_tracker.finish(run_id)


def test_schema_additions_skipped_active_stage_kafka_failed(tracker):
    """Step 2: Verify skipped stage state, spark.activeStage, and kafka.failed."""
    from app.schemas.progress import RunProgress

    tracker.start("run-schema-test", "Schema Test Pipeline")
    
    # 1. Check default values right after start
    prog = tracker.get_progress("run-schema-test")
    assert prog is not None
    assert prog["kafka"]["failed"] == 0
    assert prog["kafka"]["published"] is None
    assert prog["spark"]["activeStage"] is None

    # Validate against Pydantic schema
    validated = RunProgress.model_validate(prog)
    assert validated.kafka.failed == 0
    assert validated.spark.activeStage is None

    # 2. Update with activeStage, kafka failed, and skipped stage
    tracker.set_stage_state("run-schema-test", "kafka", "skipped")
    tracker.update_kafka("run-schema-test", published=100, failed=3, partitions=6, end_offsets=100)
    tracker.update_spark(
        "run-schema-test",
        app_id="app-12345",
        tasks_total=20,
        tasks_done=10,
        active_stage={"id": 1, "name": "count at job.py:45", "tasksTotal": 8, "tasksDone": 4},
    )

    prog2 = tracker.get_progress("run-schema-test")
    assert prog2["kafka"]["failed"] == 3
    assert prog2["kafka"]["published"] == 100
    assert prog2["kafka"]["partitions"] == 6
    assert prog2["kafka"]["endOffsets"] == 100
    assert prog2["spark"]["appId"] == "app-12345"
    assert prog2["spark"]["tasksTotal"] == 20
    assert prog2["spark"]["tasksDone"] == 10
    assert prog2["spark"]["activeStage"] == {
        "id": 1,
        "name": "count at job.py:45",
        "tasksTotal": 8,
        "tasksDone": 4,
    }

    # Verify stage state is 'skipped'
    kafka_stage = next(s for s in prog2["stages"] if s["key"] == "kafka")
    assert kafka_stage["state"] == "skipped"

    # Validate full snapshot against Pydantic schema
    val2 = RunProgress.model_validate(prog2)
    assert val2.kafka.failed == 3
    assert val2.spark.activeStage.name == "count at job.py:45"
    assert any(s.key == "kafka" and s.state == "skipped" for s in val2.stages)

    tracker.finish("run-schema-test")


def test_source_progress_transitions_success_failure_unknown_total(tracker):
    """Step 3: Unit-test source stage tracker transitions for success, failure, and unknown total."""
    # Case 1: Success
    run_id_success = "run-src-success"
    tracker.start(run_id_success, "Success Source Pipeline")

    p0 = tracker.get_progress(run_id_success)
    src_stage0 = next(s for s in p0["stages"] if s["key"] == "source")
    assert src_stage0["state"] == "pending"
    assert p0["source"]["rowsTotal"] is None
    assert p0["source"]["rowsRead"] is None

    tracker.emit(run_id_success, "source_start", {"plugin": "csv"})
    p1 = tracker.get_progress(run_id_success)
    src_stage1 = next(s for s in p1["stages"] if s["key"] == "source")
    assert src_stage1["state"] == "active"
    assert p1["source"]["type"] == "csv"
    assert p1["source"]["rowsTotal"] is None
    assert p1["source"]["rowsRead"] == 0

    tracker.emit(run_id_success, "source_done", {"rows": 1250, "duration_ms": 45.2})
    p2 = tracker.get_progress(run_id_success)
    src_stage2 = next(s for s in p2["stages"] if s["key"] == "source")
    assert src_stage2["state"] == "done"
    assert p2["source"]["rowsTotal"] == 1250
    assert p2["source"]["rowsRead"] == 1250
    tracker.finish(run_id_success)

    # Case 2: Failure
    run_id_fail = "run-src-fail"
    tracker.start(run_id_fail, "Fail Source Pipeline")
    tracker.emit(run_id_fail, "source_start", {"plugin": "csv"})
    tracker.emit(run_id_fail, "source_failed", {"error": "FileNotFoundError: /app/missing.csv"})

    pf = tracker.get_progress(run_id_fail)
    src_stage_f = next(s for s in pf["stages"] if s["key"] == "source")
    assert src_stage_f["state"] == "error"
    assert "FileNotFoundError" in src_stage_f["error"]
    assert pf["source"]["rowsTotal"] is None
    tracker.finish(run_id_fail)

    # Case 3: Unknown total (streaming or generator source where count is unknown)
    run_id_unknown = "run-src-unknown"
    tracker.start(run_id_unknown, "Unknown Total Source Pipeline")
    tracker.emit(run_id_unknown, "source_start", {"plugin": "stream"})
    tracker.emit(run_id_unknown, "source_done", {"rows": None})

    pu = tracker.get_progress(run_id_unknown)
    src_stage_u = next(s for s in pu["stages"] if s["key"] == "source")
    assert src_stage_u["state"] == "done"
    assert pu["source"]["rowsTotal"] is None
    assert pu["source"]["rowsRead"] == 0
    tracker.finish(run_id_unknown)


@pytest.mark.asyncio
async def test_persisted_progress_snapshot_for_finished_run():
    """Step 8: Verify GET /progress returns last_progress_snapshot from DB when tracker has ended."""
    import uuid
    from unittest.mock import AsyncMock, MagicMock
    from app.api.routes.pipeline_runs import get_run_progress
    from app.models.pipeline_run import PipelineRun

    run_id = str(uuid.uuid4())
    dummy_snapshot = {
        "runId": run_id,
        "status": "succeeded",
        "mode": "batch",
        "startedAt": "2026-10-12T10:00:00Z",
        "updatedAt": "2026-10-12T10:01:00Z",
        "completedAt": "2026-10-12T10:01:00Z",
        "stages": [{"key": "source", "state": "done", "startedAt": None, "finishedAt": None, "error": None}],
        "source": {"type": "csv", "rowsTotal": 1000, "rowsRead": 1000},
        "kafka": {"topic": "sales", "partitions": 6, "published": 1000, "failed": 0, "endOffsets": 1000, "consumerLag": None},
        "spark": {"appId": "app-1", "tasksTotal": 10, "tasksDone": 10, "activeStage": None, "recordsIn": 1000, "recordsOut": 950, "durationMs": 5000},
        "hdfs": {"path": "/data/out", "filesWritten": 3, "bytesWritten": 40960, "rowsWritten": 950, "verified": True},
        "throughput": {"recordsPerSecond": 200.0, "samples": []},
        "etaSeconds": None,
        "errors": [],
        "fault": None,
    }

    mock_db = AsyncMock()
    mock_run = MagicMock(spec=PipelineRun)
    mock_run.id = uuid.UUID(run_id)
    mock_run.status = "succeeded"
    mock_run.last_progress_snapshot = dummy_snapshot
    mock_db.get.return_value = mock_run
    mock_db.execute.return_value.scalar_one_or_none.return_value = mock_run

    resp = await get_run_progress(run_id, mock_db)
    import json
    data = json.loads(resp.body.decode("utf-8"))
    assert data == dummy_snapshot


