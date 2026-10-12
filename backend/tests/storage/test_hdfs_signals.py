"""
backend/tests/storage/test_hdfs_signals.py
-----------------------------------------
Unit tests for HDFS content summary queries, poller thread lifecycle,
and HDFS progress signal transitions with mocked WebHDFS responses.
"""

from __future__ import annotations

import threading
import time
from unittest.mock import MagicMock, patch

import httpx
import pytest

from app.pipeline.progress import RunProgressTracker
from app.storage.hdfs_client import HdfsStorageClient


def test_content_summary_404_returns_none() -> None:
    """When WebHDFS returns HTTP 404, content_summary returns None."""
    client = HdfsStorageClient(namenode_url="http://mock-namenode:9870", user="testuser")
    mock_resp = MagicMock()
    mock_resp.status_code = 404

    with patch("httpx.get", return_value=mock_resp):
        result = client.content_summary("/data/output/sales")
        assert result is None


def test_content_summary_valid_response() -> None:
    """Valid WebHDFS ContentSummary response is parsed correctly."""
    client = HdfsStorageClient(namenode_url="http://mock-namenode:9870", user="testuser")
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {
        "ContentSummary": {
            "fileCount": 5,
            "length": 1048576,
            "directoryCount": 2,
            "quota": -1,
            "spaceConsumed": 3145728,
            "spaceQuota": -1,
        }
    }

    with patch("httpx.get", return_value=mock_resp):
        result = client.content_summary("hdfs://namenode:9000/data/output/sales")
        assert result == {
            "fileCount": 5,
            "length": 1048576,
            "directoryCount": 2,
        }


def test_content_summary_malformed_json_returns_none() -> None:
    """Malformed or invalid ContentSummary returns None and does not raise."""
    client = HdfsStorageClient(namenode_url="http://mock-namenode:9870", user="testuser")
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"BadKey": {}}

    with patch("httpx.get", return_value=mock_resp):
        assert client.content_summary("/data/output") is None

    mock_resp.json.side_effect = ValueError("Invalid JSON")
    with patch("httpx.get", return_value=mock_resp):
        assert client.content_summary("/data/output") is None


def test_content_summary_timeout_returns_none() -> None:
    """Network timeout does not crash the client and returns None."""
    client = HdfsStorageClient(namenode_url="http://mock-namenode:9870", user="testuser")

    with patch("httpx.get", side_effect=httpx.TimeoutException("Connection timed out")):
        assert client.content_summary("/data/output") is None


def test_hdfs_poller_thread_lifecycle_and_value_preservation() -> None:
    """
    Poller updates tracker during execution, preserves previous values when a
    poll fails, and stops cleanly when the stop event is signaled.
    """
    tracker = RunProgressTracker()
    run_id = "test-hdfs-poller-run"
    tracker.start(run_id, "test-pipeline")

    client = HdfsStorageClient(namenode_url="http://mock-namenode:9870", user="testuser")
    stop_event = threading.Event()
    last_files_written = 0
    last_bytes_written = 0
    output_path = "/output/sales"

    # Sequence of returns: 404 -> valid -> None (poll fail) -> stop
    poll_results = [
        None,
        {"fileCount": 4, "length": 8192, "directoryCount": 1},
        None,
    ]
    poll_idx = 0

    def mock_content_summary(_path: str) -> dict[str, int] | None:
        nonlocal poll_idx
        res = poll_results[min(poll_idx, len(poll_results) - 1)]
        poll_idx += 1
        return res

    with patch.object(client, "content_summary", side_effect=mock_content_summary):
        def _poller_loop() -> None:
            nonlocal last_files_written, last_bytes_written
            while not stop_event.is_set():
                summary = client.content_summary(output_path)
                if summary is not None:
                    last_files_written = summary.get("fileCount", 0)
                    last_bytes_written = summary.get("length", 0)
                    tracker.emit(
                        run_id,
                        "hdfs_progress",
                        {
                            "path": output_path,
                            "filesWritten": last_files_written,
                            "bytesWritten": last_bytes_written,
                        },
                    )
                stop_event.wait(0.05)

        thread = threading.Thread(target=_poller_loop, daemon=True)
        thread.start()

        # Let the poller run a few iterations
        time.sleep(0.2)
        stop_event.set()
        thread.join(timeout=1.0)
        assert not thread.is_alive()

        # Stage should have transitioned to active on fileCount > 0
        progress = tracker.get_progress(run_id)
        assert progress is not None
        hdfs_stage = next(s for s in progress["stages"] if s["key"] == "hdfs")
        assert hdfs_stage["state"] == "active"
        assert progress["hdfs"]["filesWritten"] == 4
        assert progress["hdfs"]["bytesWritten"] == 8192

        # Complete verification
        tracker.emit(
            run_id,
            "hdfs_done",
            {
                "path": output_path,
                "rows": 1000,
                "filesWritten": 4,
                "bytesWritten": 8192,
                "verified": True,
            },
        )

        final_progress = tracker.get_progress(run_id)
        assert final_progress is not None
        final_hdfs_stage = next(s for s in final_progress["stages"] if s["key"] == "hdfs")
        assert final_hdfs_stage["state"] == "done"
        assert final_progress["hdfs"]["rowsWritten"] == 1000
        assert final_progress["hdfs"]["verified"] is True


def test_storage_verification_failure_sets_error_and_unverified() -> None:
    """When storage verification fails, verified is False and stage is error."""
    tracker = RunProgressTracker()
    run_id = "test-hdfs-verify-fail"
    tracker.start(run_id, "test-pipeline")

    tracker.update_hdfs(run_id, verified=False)
    tracker.set_stage_state(run_id, "hdfs", "error", error="Verification failed")

    progress = tracker.get_progress(run_id)
    assert progress is not None
    hdfs_stage = next(s for s in progress["stages"] if s["key"] == "hdfs")
    assert hdfs_stage["state"] == "error"
    assert progress["hdfs"]["verified"] is False
