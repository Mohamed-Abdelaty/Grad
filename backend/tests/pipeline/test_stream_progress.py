"""
backend/tests/pipeline/test_stream_progress.py
----------------------------------------------
Unit tests for stream progress parser, lag calculation, and completion detector.
"""

from __future__ import annotations

import time
from app.pipeline.stream_progress import (
    StreamingCompletionDetector,
    compute_lag,
    extract_spark_offset_sum,
    parse_stream_progress_line,
    parse_stream_started_line,
)


def test_parse_stream_started_valid() -> None:
    line = 'STREAM_STARTED:{"query_id": "qid-123", "run_id": "rid-456"}\n'
    res = parse_stream_started_line(line)
    assert res == {"query_id": "qid-123", "run_id": "rid-456"}


def test_parse_stream_started_invalid() -> None:
    assert parse_stream_started_line("SOME_OTHER_LINE:{}") is None
    assert parse_stream_started_line("STREAM_STARTED:invalid-json") is None


def test_parse_stream_progress_valid() -> None:
    line = (
        'STREAM_PROGRESS:{"batchId": 3, "numInputRows": 50, "inputRowsPerSecond": 10.0, '
        '"processedRowsPerSecond": 25.0, "endOffset": {"0": 100, "1": 150}, "totalInputRows": 250}'
    )
    res = parse_stream_progress_line(line)
    assert res is not None
    assert res["batchId"] == 3
    assert res["numInputRows"] == 50
    assert res["totalInputRows"] == 250
    assert res["endOffset"] == {"0": 100, "1": 150}


def test_parse_stream_progress_invalid() -> None:
    assert parse_stream_progress_line("SPARK_JOB_PROGRESS:{}") is None
    assert parse_stream_progress_line("STREAM_PROGRESS:{not valid json}") is None


def test_extract_spark_offset_sum() -> None:
    assert extract_spark_offset_sum(100) == 100
    assert extract_spark_offset_sum("150") == 150
    assert extract_spark_offset_sum({"0": 50, "1": 70, "2": 30}) == 150
    assert extract_spark_offset_sum({"events": {"0": 100, "1": 200}}) == 300
    assert extract_spark_offset_sum(None) == 0


def test_compute_lag() -> None:
    end_offsets = {0: 100, 1: 150, 2: 250}  # total 500
    spark_offset = {"0": 90, "1": 140, "2": 200}  # total 430
    assert compute_lag(end_offsets, spark_offset) == 70

    # Fully caught up
    assert compute_lag(end_offsets, {"0": 100, "1": 150, "2": 250}) == 0

    # Spark consumed ahead (cannot be negative)
    assert compute_lag(end_offsets, {"0": 120, "1": 160, "2": 260}) == 0

    # Single integer end offset
    assert compute_lag(500, 450) == 50

    # None end offsets
    assert compute_lag(None, 450) == 0


def test_streaming_completion_detector_expected_rows_reached() -> None:
    detector = StreamingCompletionDetector(idle_timeout_seconds=5.0, max_runtime_seconds=60.0)

    # In flight: lag > 0
    assert not detector.is_complete(current_lag=20, current_total_rows=80, expected_total_rows=100, producer_done=True)

    # In flight: lag == 0 but producer not done
    assert not detector.is_complete(current_lag=0, current_total_rows=80, expected_total_rows=100, producer_done=False)

    # Producer done, expected rows reached, lag 0 -> completes immediately
    assert detector.is_complete(current_lag=0, current_total_rows=100, expected_total_rows=100, producer_done=True)


def test_streaming_completion_detector_idle_timeout() -> None:
    detector = StreamingCompletionDetector(idle_timeout_seconds=0.1, max_runtime_seconds=60.0)

    # Activity recorded
    detector.record_activity(current_total_rows=50, current_lag=0)

    # Immediately after activity, not complete
    assert not detector.is_complete(current_lag=0, current_total_rows=50, expected_total_rows=None, producer_done=False)

    # Wait past idle timeout
    time.sleep(0.15)
    assert detector.is_complete(current_lag=0, current_total_rows=50, expected_total_rows=None, producer_done=False)


def test_streaming_completion_detector_max_runtime() -> None:
    detector = StreamingCompletionDetector(idle_timeout_seconds=10.0, max_runtime_seconds=0.05)
    time.sleep(0.08)
    assert detector.is_timed_out()
