"""
app/pipeline/progress.py
------------------------
Thread-safe pipeline run progress tracker and SSE event broker.

The tracker bridges the synchronous executor (runs in a thread pool via
asyncio.to_thread) and the async SSE endpoint (runs in the FastAPI event loop).

Thread-safety guarantees
-------------------------
* All mutable state is protected by ``_lock`` (threading.Lock).
* ``emit()`` is safe to call from any thread, including the executor thread.
* SSE subscribers receive events via asyncio Queues.  Emission crosses the
  thread/loop boundary via ``loop.call_soon_threadsafe``.
* Cancel uses ``os.kill`` on the stored subprocess PID — safe from any thread.

Event types (event_type values)
--------------------------------
source_start, source_done,
kafka_start, kafka_done,
spark_start, spark_done,
hdfs_start, hdfs_done,
run_success, run_failed, run_cancelled,
heartbeat (emitted by the SSE endpoint, not the executor)
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import threading
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)


def _utc_now() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


class RunProgressTracker:
    """
    In-memory, thread-safe tracker for active pipeline run progress.

    Lifecycle::

        # Async background task:
        tracker.start(run_id, pipeline_name)

        # Executor thread (sync):
        tracker.emit(run_id, "source_start", {})
        tracker.emit(run_id, "source_done", {"rows": 20})
        tracker.register_proc_pid(run_id, proc.pid)   # when spark-submit starts

        # SSE endpoint (async):
        queue = tracker.subscribe(run_id)
        try:
            ...stream events from queue...
        finally:
            tracker.unsubscribe(run_id, queue)

        # Cancel endpoint (async):
        tracker.request_cancel(run_id)

        # Background task cleanup:
        tracker.finish(run_id)
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        # run_id → progress state dict
        self._runs: dict[str, dict[str, Any]] = {}
        # run_id → list of asyncio.Queue (one per SSE subscriber)
        self._subscribers: dict[str, list[asyncio.Queue]] = defaultdict(list)
        # run_id → spark-submit subprocess PID (for cancellation)
        self._proc_pids: dict[str, int] = {}
        # set of run_ids for which cancel was requested
        self._cancel_requested: set[str] = set()
        # Reference to the main event loop (set during app startup)
        self._loop: asyncio.AbstractEventLoop | None = None

    # ── Startup ────────────────────────────────────────────────────────────────

    def set_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        """
        Store the main event loop.

        Must be called during application startup (inside ``lifespan``) so
        cross-thread notifications work correctly.
        """
        self._loop = loop

    # ── Run lifecycle ──────────────────────────────────────────────────────────

    def start(self, run_id: str, pipeline_name: str) -> None:
        """
        Register a new running pipeline.

        Call this *before* spawning the executor thread so events emitted
        immediately by the thread are not silently dropped.
        """
        with self._lock:
            self._runs[run_id] = {
                "run_id": run_id,
                "pipeline_name": pipeline_name,
                "status": "active",
                "stages": {
                    "source": {"key": "source", "state": "pending", "startedAt": None, "finishedAt": None, "error": None},
                    "kafka": {"key": "kafka", "state": "pending", "startedAt": None, "finishedAt": None, "error": None},
                    "spark": {"key": "spark", "state": "pending", "startedAt": None, "finishedAt": None, "error": None},
                    "hdfs": {"key": "hdfs", "state": "pending", "startedAt": None, "finishedAt": None, "error": None}
                },
                "started_at": _utc_now(),
                "finished_at": None,
                "error": None,
                "events": [],
                "seq": 0,
            }

    def finish(self, run_id: str) -> None:
        """
        Remove a completed run from the in-memory tracker.

        Safe to call multiple times.  Does not affect DB records.
        """
        with self._lock:
            self._runs.pop(run_id, None)
            self._subscribers.pop(run_id, None)
            self._proc_pids.pop(run_id, None)
            self._cancel_requested.discard(run_id)

    # ── Emission ───────────────────────────────────────────────────────────────

    def emit(
        self,
        run_id: str,
        event_type: str,
        payload: dict[str, Any] | None = None,
    ) -> None:
        """
        Emit a progress event.

        Thread-safe: safe to call from the executor thread or any coroutine.
        If the run is not tracked (e.g. ``wait=True`` path), this is a no-op.
        """
        payload = payload or {}
        with self._lock:
            state = self._runs.get(run_id)
            if state is None:
                return
            state["seq"] += 1
            event: dict[str, Any] = {
                "seq": state["seq"],
                "event_type": event_type,
                "payload": payload,
                "ts": _utc_now(),
            }
            state["events"].append(event)
            self._apply_event(state, event_type, payload)
            subs = list(self._subscribers[run_id])

        # Cross-thread notification: push to each subscriber's async queue
        if self._loop and self._loop.is_running():
            for q in subs:
                self._loop.call_soon_threadsafe(q.put_nowait, event)

    def _apply_event(
        self, state: dict[str, Any], event_type: str, payload: dict[str, Any]
    ) -> None:
        """Update the cached progress snapshot.  Must be called under ``_lock``."""
        stages = state["stages"]
        
        # Helper to ensure a stage exists


        if event_type == "source_start":
            st = stages["source"]
            st["state"] = "active"
            st["startedAt"] = _utc_now()
        elif event_type == "source_done":
            st = stages["source"]
            st["state"] = "done"
            st["finishedAt"] = _utc_now()
            state["source_metrics"] = payload.get("rows")
        elif event_type == "kafka_start":
            st = stages["kafka"]
            st["state"] = "active"
            st["startedAt"] = _utc_now()
        elif event_type == "kafka_done":
            st = stages["kafka"]
            st["state"] = "done"
            st["finishedAt"] = _utc_now()
            state["kafka_metrics"] = payload.get("count")
        elif event_type == "spark_start":
            st = stages["spark"]
            st["state"] = "active"
            st["startedAt"] = _utc_now()
        elif event_type == "spark_done":
            st = stages["spark"]
            st["state"] = "done"
            st["finishedAt"] = _utc_now()
            state["spark_metrics"] = payload
        elif event_type == "hdfs_start":
            st = stages["hdfs"]
            st["state"] = "active"
            st["startedAt"] = _utc_now()
        elif event_type == "hdfs_done":
            st = stages["hdfs"]
            st["state"] = "done"
            st["finishedAt"] = _utc_now()
            state["storage_metrics"] = payload.get("rows")
        elif event_type == "run_success":
            state["status"] = "success"
            state["finished_at"] = payload.get("finished_at")
            for st in stages.values():
                if st["state"] in ("pending", "active"):
                    st["state"] = "done"
                    st["finishedAt"] = _utc_now()
        elif event_type == "run_failed":
            state["status"] = "failed"
            state["finished_at"] = payload.get("finished_at")
            state["error"] = payload.get("error")
            for st in stages.values():
                if st["state"] == "active":
                    st["state"] = "error"
                    st["error"] = payload.get("error")
                    st["finishedAt"] = _utc_now()
                elif st["state"] == "pending":
                    st["state"] = "skipped"
                    st["finishedAt"] = _utc_now()
        elif event_type == "run_cancelled":
            state["status"] = "cancelled"
            state["finished_at"] = payload.get("finished_at")
            for st in stages.values():
                if st["state"] == "active":
                    st["state"] = "error"
                    st["error"] = "Cancelled"
                    st["finishedAt"] = _utc_now()
                elif st["state"] == "pending":
                    st["state"] = "skipped"
                    st["finishedAt"] = _utc_now()


    # ── Queries ────────────────────────────────────────────────────────────────

    def get_progress(self, run_id: str) -> dict[str, Any] | None:
        """
        Return the current progress snapshot for *run_id*, or None if not tracked.
        """
        with self._lock:
            state = self._runs.get(run_id)
            if state is None:
                return None
                
            events = state.get("events", [])
            updated_at = events[-1]["ts"] if events else state.get("started_at")
            
            spark_m = state.get("spark_metrics", {})
            
            return {
                "runId": run_id,
                "status": state["status"],
                "mode": "batch",
                "startedAt": state.get("started_at"),
                "updatedAt": updated_at,
                "completedAt": state.get("finished_at"),
                "stages": list(state.get("stages", {}).values()),
                "source": {
                    "type": None, 
                    "rowsTotal": state.get("source_metrics"), 
                    "rowsRead": state.get("source_metrics")
                },
                "kafka": {
                    "topic": None, 
                    "partitions": None, 
                    "published": state.get("kafka_metrics"), 
                    "endOffsets": None, 
                    "consumerLag": None
                },
                "spark": {
                    "appId": None, 
                    "tasksTotal": None, 
                    "tasksDone": None, 
                    "recordsIn": spark_m.get("input_rows"), 
                    "recordsOut": spark_m.get("output_rows"), 
                    "durationMs": spark_m.get("duration_ms")
                },
                "hdfs": {
                    "path": spark_m.get("output_path"), 
                    "filesWritten": None, 
                    "bytesWritten": None, 
                    "rowsWritten": state.get("storage_metrics"), 
                    "verified": None
                },
                "throughput": {"recordsPerSecond": None, "samples": []},
                "etaSeconds": None,
                "errors": [state["error"]] if state.get("error") else [],
                "fault": None
            }

    def get_events_after(self, run_id: str, after_seq: int) -> list[dict]:
        """Return all in-memory events with seq > *after_seq*."""
        with self._lock:
            state = self._runs.get(run_id)
            if state is None:
                return []
            return [e for e in state["events"] if e["seq"] > after_seq]

    def replay_events(self, run_id: str) -> list[dict]:
        """Return all past events (for SSE catch-up on reconnect)."""
        with self._lock:
            state = self._runs.get(run_id)
            return list(state["events"]) if state else []

    def is_active(self, run_id: str) -> bool:
        with self._lock:
            state = self._runs.get(run_id)
            return state is not None and state["status"] == "active"

    # ── SSE subscriptions ──────────────────────────────────────────────────────

    def subscribe(self, run_id: str) -> asyncio.Queue:
        """
        Create and return an asyncio.Queue that receives future events.

        The caller owns the queue and must call :meth:`unsubscribe` when done.
        """
        q: asyncio.Queue = asyncio.Queue()
        with self._lock:
            self._subscribers[run_id].append(q)
        return q

    def unsubscribe(self, run_id: str, q: asyncio.Queue) -> None:
        """Remove an SSE subscriber queue."""
        with self._lock:
            subs = self._subscribers.get(run_id, [])
            try:
                subs.remove(q)
            except ValueError:
                pass

    # ── Cancel ─────────────────────────────────────────────────────────────────

    def register_proc_pid(self, run_id: str, pid: int) -> None:
        """
        Register the spark-submit subprocess PID so :meth:`request_cancel`
        can send SIGTERM to it.  Thread-safe.
        """
        with self._lock:
            self._proc_pids[run_id] = pid

    def deregister_proc_pid(self, run_id: str) -> None:
        """Remove the subprocess PID registration (call when spark-submit exits)."""
        with self._lock:
            self._proc_pids.pop(run_id, None)

    def request_cancel(self, run_id: str) -> bool:
        """
        Request cancellation of a running pipeline.

        Sends SIGTERM to the registered spark-submit process (if any).
        Also sets the cancel-requested flag so the executor checks at each
        stage boundary.

        Returns True if the request was accepted (run exists and is running),
        False otherwise.
        """
        with self._lock:
            state = self._runs.get(run_id)
            if state is None or state["status"] != "active":
                return False
            self._cancel_requested.add(run_id)
            pid = self._proc_pids.get(run_id)

        if pid is not None:
            try:
                os.killpg(os.getpgid(pid), signal.SIGTERM)
                logger.info(
                    "Sent SIGTERM to spark-submit process group %d for run %s", pid, run_id
                )
            except ProcessLookupError:
                logger.debug("PID %d already gone when cancel requested.", pid)
            except Exception as e:
                logger.warning("Failed to kill process group for PID %d: %s", pid, e)
        else:
            logger.info(
                "Cancel requested for run %s — no Spark PID registered; "
                "executor will abort at next stage boundary.",
                run_id,
            )
        return True

    def is_cancel_requested(self, run_id: str) -> bool:
        """Return True if cancellation has been requested for *run_id*."""
        with self._lock:
            return run_id in self._cancel_requested


# ── Module-level singleton ──────────────────────────────────────────────────────
#
# Imported by executor.py, pipeline_runs.py, and pipelines.py.
# All three modules share this instance so events flow from executor → SSE.
tracker = RunProgressTracker()
