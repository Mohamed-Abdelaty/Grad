"""
backend/app/pipeline/stream_progress.py
-----------------------------------------
Parsers and state tracking for PySpark Structured Streaming jobs.

Extracts structured progress lines emitted by ``streaming_processing_job.py``:
- STREAM_STARTED: {"query_id": "...", "run_id": "..."}
- STREAM_PROGRESS: {"batchId": 0, "numInputRows": 50, ...}
- SPARK_JOB_RESULT: {"input_rows": N, "output_rows": N, ...}

Also provides consumer lag calculation across partitions and a
``StreamingCompletionDetector`` for finite stream completion.
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any

logger = logging.getLogger(__name__)

STREAM_STARTED_PREFIX = "STREAM_STARTED:"
STREAM_PROGRESS_PREFIX = "STREAM_PROGRESS:"
SPARK_JOB_RESULT_PREFIX = "SPARK_JOB_RESULT:"


def parse_stream_started_line(line: str) -> dict[str, Any] | None:
    """Parse a STREAM_STARTED line into a dictionary, or return None."""
    line = line.strip()
    if not line.startswith(STREAM_STARTED_PREFIX):
        return None
    raw = line[len(STREAM_STARTED_PREFIX):].strip()
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, ValueError) as exc:
        logger.debug("Malformed STREAM_STARTED line: %s (%s)", line, exc)
        return None


def parse_stream_progress_line(line: str) -> dict[str, Any] | None:
    """Parse a STREAM_PROGRESS line into a dictionary, or return None."""
    line = line.strip()
    if not line.startswith(STREAM_PROGRESS_PREFIX):
        return None
    raw = line[len(STREAM_PROGRESS_PREFIX):].strip()
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, ValueError) as exc:
        logger.debug("Malformed STREAM_PROGRESS line: %s (%s)", line, exc)
        return None


def extract_spark_offset_sum(offset_obj: Any) -> int:
    """
    Sum offsets from a Spark endOffset JSON structure or integer.

    Spark Structured Streaming offsets can be:
    - An integer
    - A dict mapping partition strings/ints to integer offsets: {"0": 100, "1": 200}
    - A dict mapping topic to partition dict: {"topic": {"0": 100, "1": 200}}
    """
    if offset_obj is None:
        return 0
    if isinstance(offset_obj, (int, float)):
        return int(offset_obj)
    if isinstance(offset_obj, str):
        try:
            parsed = json.loads(offset_obj)
            return extract_spark_offset_sum(parsed)
        except Exception:
            try:
                return int(offset_obj)
            except ValueError:
                return 0
    if isinstance(offset_obj, dict):
        total = 0
        for val in offset_obj.values():
            if isinstance(val, (int, float)):
                total += int(val)
            elif isinstance(val, (dict, list, str)):
                total += extract_spark_offset_sum(val)
        return total
    return 0


def compute_lag(
    end_offsets: dict[int, int] | int | None,
    spark_consumed_offset: Any,
) -> int:
    """
    Compute consumer lag as max(0, sum(end_offsets) - sum(spark_consumed_offsets)).

    Parameters
    ----------
    end_offsets : dict[int, int] | int | None
        Topic end offsets (high watermarks) from Kafka Admin / consumer.
    spark_consumed_offset : Any
        Spark's current consumed endOffset from StreamingQueryProgress.

    Returns
    -------
    int
        Non-negative consumer lag count.
    """
    if end_offsets is None:
        return 0

    if isinstance(end_offsets, dict):
        end_sum = sum(int(v) for v in end_offsets.values() if v is not None)
    elif isinstance(end_offsets, (int, float)):
        end_sum = int(end_offsets)
    else:
        end_sum = 0

    spark_sum = extract_spark_offset_sum(spark_consumed_offset)
    return max(0, end_sum - spark_sum)


class StreamingCompletionDetector:
    """
    Detects finite stream completion based on producer finish, consumer lag, and idle timeout.

    A stream is considered complete when:
    1. Producer has completed (expected_total_rows is provided) and spark has processed
       at least expected_total_rows and consumer lag is 0; OR
    2. Consumer lag is 0 and no new rows have been processed for ``idle_timeout_seconds``; OR
    3. ``max_runtime_seconds`` hard cap is exceeded.
    """

    def __init__(
        self,
        idle_timeout_seconds: float = 20.0,
        max_runtime_seconds: float = 600.0,
    ) -> None:
        self.idle_timeout_seconds = float(idle_timeout_seconds)
        self.max_runtime_seconds = float(max_runtime_seconds)
        self.start_time = time.monotonic()
        self.last_activity_time = time.monotonic()
        self.last_total_rows = 0

    def record_activity(self, current_total_rows: int, current_lag: int) -> None:
        """Update last activity timestamp if new rows arrived or lag > 0."""
        now = time.monotonic()
        if current_total_rows > self.last_total_rows or current_lag > 0:
            self.last_activity_time = now
            self.last_total_rows = current_total_rows

    def is_complete(
        self,
        current_lag: int,
        current_total_rows: int,
        expected_total_rows: int | None = None,
        producer_done: bool = False,
    ) -> bool:
        """
        Check if the streaming query has processed all available records and drained.
        """
        now = time.monotonic()
        self.record_activity(current_total_rows, current_lag)

        # Condition 1: Known producer total reached and lag is zero
        if producer_done and expected_total_rows is not None and expected_total_rows > 0:
            if current_total_rows >= expected_total_rows and current_lag == 0:
                logger.info(
                    "Streaming completion: expected rows (%d) reached with lag 0",
                    expected_total_rows,
                )
                return True

        # Condition 2: Lag is 0 and idle for idle_timeout_seconds
        idle_time = now - self.last_activity_time
        if current_lag == 0 and idle_time >= self.idle_timeout_seconds:
            logger.info(
                "Streaming completion: idle for %.1fs (threshold %.1fs) with lag 0",
                idle_time,
                self.idle_timeout_seconds,
            )
            return True

        return False

    def is_timed_out(self) -> bool:
        """Check if max runtime hard cap has been exceeded."""
        elapsed = time.monotonic() - self.start_time
        if elapsed >= self.max_runtime_seconds:
            logger.warning(
                "Streaming job reached max runtime limit: %.1fs >= %.1fs",
                elapsed,
                self.max_runtime_seconds,
            )
            return True
        return False
