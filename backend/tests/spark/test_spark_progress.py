"""
backend/tests/spark/test_spark_progress.py
-----------------------------------------
Unit tests for Spark progress line parsing, streaming output consumption,
and deadlock prevention in SparkJobSubmitter.
"""

from __future__ import annotations

import asyncio
import json
import sys
import time
from typing import Any

import pytest

from app.pipeline.progress import tracker
from app.spark.exceptions import SparkJobFailedError
from app.spark.spark_job_submitter import (
    SparkJobSubmitter,
    parse_progress_line,
)


def test_parse_progress_line_valid() -> None:
    """A valid SPARK_JOB_PROGRESS line is parsed into a dictionary."""
    line = (
        'SPARK_JOB_PROGRESS:{"appId": "app-123", "tasksTotal": 10, '
        '"tasksDone": 4, "activeStage": {"id": 1, "name": "count", '
        '"tasksTotal": 5, "tasksDone": 2}, "ts": 1700000000.0}'
    )
    result = parse_progress_line(line)
    assert result is not None
    assert result["appId"] == "app-123"
    assert result["tasksTotal"] == 10
    assert result["tasksDone"] == 4
    assert result["activeStage"]["name"] == "count"


def test_parse_progress_line_malformed() -> None:
    """A malformed JSON line returns None and never raises an exception."""
    line = 'SPARK_JOB_PROGRESS:{"appId": "incomplete_json'
    assert parse_progress_line(line) is None


def test_parse_progress_line_spark_log_noise_ignored() -> None:
    """Lines with Spark log noise preceding the prefix must be ignored."""
    line = (
        '2026-10-12 10:00:00 INFO TaskSetManager: '
        'SPARK_JOB_PROGRESS:{"appId": "app-123"}'
    )
    assert parse_progress_line(line) is None


def test_parse_progress_line_very_long_line() -> None:
    """Very long lines (e.g. 200KB) are handled safely without crashing."""
    large_payload = "x" * 200000
    line_valid = f'SPARK_JOB_PROGRESS:{{"data": "{large_payload}"}}'
    parsed = parse_progress_line(line_valid)
    assert parsed is not None
    assert parsed["data"] == large_payload

    line_noise = "A" * 200000
    assert parse_progress_line(line_noise) is None


@pytest.mark.asyncio
async def test_submitter_fake_process_progress_streaming() -> None:
    """
    Submitter streams stdout line-by-line while process runs, invoking on_progress
    before the process exits, and preserving identical final result parsing.
    """
    submitter = SparkJobSubmitter()
    progress_events: list[tuple[float, dict[str, Any]]] = []

    def _on_progress(payload: dict[str, Any]) -> None:
        progress_events.append((time.perf_counter(), payload))

    script = (
        "import sys, time, json\n"
        "for i in range(3):\n"
        "    print(f'SPARK_JOB_PROGRESS:{json.dumps({\"step\": i+1})}', flush=True)\n"
        "    time.sleep(0.3)\n"
        "print('SPARK_JOB_RESULT:{\"input_rows\": 100, \"output_rows\": 90, "
        "\"duration_seconds\": 1.0, \"status\": \"success\"}', flush=True)\n"
    )

    cmd = [sys.executable, "-c", script]
    t_start = time.perf_counter()
    stdout, stderr = await submitter._run_subprocess(
        cmd, run_id="test-progress-stream", on_progress=_on_progress
    )
    t_end = time.perf_counter()

    assert len(progress_events) == 3
    # Check that events were received over time before process exited
    for event_ts, event_data in progress_events:
        assert t_start <= event_ts < t_end

    # Result parsing remains identical
    result = submitter._parse_result(stdout, stderr)
    assert result["status"] == "success"
    assert result["input_rows"] == 100
    assert result["output_rows"] == 90
    assert result["duration_seconds"] == 1.0


@pytest.mark.asyncio
async def test_fake_process_5mb_stderr_no_deadlock() -> None:
    """
    A subprocess writing 5 MB to stderr while writing the result to stdout
    must not deadlock the pipe buffer and must parse correctly.
    """
    submitter = SparkJobSubmitter()

    script = (
        "import sys, json\n"
        "chunk = 'E' * 65536 + '\\n'\n"
        "for _ in range(80):\n"  # 80 * 65KB ~ 5.2 MB
        "    sys.stderr.write(chunk)\n"
        "    sys.stderr.flush()\n"
        "print('SPARK_JOB_RESULT:{\"input_rows\": 500, \"output_rows\": 500, "
        "\"duration_seconds\": 0.5, \"status\": \"success\"}', flush=True)\n"
    )

    cmd = [sys.executable, "-c", script]
    stdout, stderr = await asyncio.wait_for(
        submitter._run_subprocess(cmd, run_id="test-5mb-stderr"),
        timeout=10.0,
    )

    result = submitter._parse_result(stdout, stderr)
    assert result["status"] == "success"
    assert result["input_rows"] == 500
    assert len(stderr) > 0


@pytest.mark.asyncio
async def test_cancel_terminates_fake_process() -> None:
    """
    Cancellation request terminates the running process cleanly via tracker PID.
    """
    submitter = SparkJobSubmitter()
    run_id = "test-cancel-fake-job"

    # Register a dummy run state in tracker so request_cancel allows it
    tracker.start(run_id, pipeline_name="dummy-pipe")

    script = (
        "import time, sys\n"
        "print('started', flush=True)\n"
        "time.sleep(30)\n"
    )
    cmd = [sys.executable, "-c", script]

    task = asyncio.create_task(
        submitter._run_subprocess(cmd, run_id=run_id)
    )

    # Allow child process to start and register its PID
    await asyncio.sleep(0.4)

    # Cancel via tracker
    cancelled = tracker.request_cancel(run_id)
    assert cancelled is True

    # Subprocess should be killed and raise SparkJobFailedError
    with pytest.raises(SparkJobFailedError):
        await asyncio.wait_for(task, timeout=5.0)
