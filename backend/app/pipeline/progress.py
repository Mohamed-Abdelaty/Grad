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


def build_progress_dict(
    run_id: str,
    status: str,
    mode: str = "batch",
    started_at: str | None = None,
    updated_at: str | None = None,
    completed_at: str | None = None,
    stages: list[dict[str, Any]] | None = None,
    source_metrics: Any = None,
    source_type: str | None = None,
    source_rows_total: int | None = None,
    source_rows_read: int | None = None,
    kafka_metrics: Any = None,
    kafka_topic: str | None = None,
    kafka_partitions: int | None = None,
    kafka_published: int | None = None,
    kafka_failed: int = 0,
    kafka_end_offsets: int | None = None,
    spark_metrics: dict[str, Any] | None = None,
    spark_app_id: str | None = None,
    spark_tasks_total: int | None = None,
    spark_tasks_done: int | None = None,
    spark_active_stage: dict[str, Any] | None = None,
    spark_records_in: int | None = None,
    spark_records_out: int | None = None,
    spark_duration_ms: float | None = None,
    storage_metrics: Any = None,
    hdfs_path: str | None = None,
    hdfs_files_written: int | None = None,
    hdfs_bytes_written: int | None = None,
    hdfs_rows_written: int | None = None,
    hdfs_verified: bool | None = None,
    throughput_rps: float | None = None,
    throughput_samples: list[dict[str, Any]] | None = None,
    eta_seconds: int | None = None,
    error: str | None = None,
    fault: Any = None,
) -> dict[str, Any]:
    """
    Construct the canonical progress dictionary following Section 8.1 schema.

    Contains all 15 top-level keys:
    runId, status, mode, startedAt, updatedAt, completedAt,
    stages, source, kafka, spark, hdfs, throughput, etaSeconds, errors, fault.
    Unknown values are null (never 0, never invented).
    stages always contains exactly the 4 keys: source, kafka, spark, hdfs.
    Allowed stage states: pending | active | done | error | skipped.
    """
    spark_m = spark_metrics or {}

    stage_order = ["source", "kafka", "spark", "hdfs"]
    stage_map = {s["key"]: s for s in (stages or []) if isinstance(s, dict) and "key" in s}
    final_stages = []
    for k in stage_order:
        if k in stage_map:
            st = stage_map[k]
            final_stages.append({
                "key": k,
                "state": st.get("state", "pending"),
                "startedAt": st.get("startedAt"),
                "finishedAt": st.get("finishedAt"),
                "error": st.get("error"),
            })
        else:
            final_stages.append({
                "key": k,
                "state": "pending",
                "startedAt": None,
                "finishedAt": None,
                "error": None,
            })

    rows_total = source_rows_total if source_rows_total is not None else source_metrics
    rows_read = source_rows_read if source_rows_read is not None else source_metrics
    published = kafka_published if kafka_published is not None else kafka_metrics

    return {
        "runId": run_id,
        "status": status,
        "mode": mode,
        "startedAt": started_at,
        "updatedAt": updated_at or _utc_now(),
        "completedAt": completed_at,
        "stages": final_stages,
        "source": {
            "type": source_type,
            "rowsTotal": rows_total,
            "rowsRead": rows_read,
        },
        "kafka": {
            "topic": kafka_topic,
            "partitions": kafka_partitions,
            "published": published,
            "failed": kafka_failed if kafka_failed is not None else 0,
            "endOffsets": kafka_end_offsets,
            "consumerLag": None,
        },
        "spark": {
            "appId": spark_app_id,
            "tasksTotal": spark_tasks_total,
            "tasksDone": spark_tasks_done,
            "activeStage": spark_active_stage,
            "recordsIn": spark_records_in if spark_records_in is not None else spark_m.get("input_rows"),
            "recordsOut": spark_records_out if spark_records_out is not None else spark_m.get("output_rows"),
            "durationMs": spark_duration_ms if spark_duration_ms is not None else spark_m.get("duration_ms"),
        },
        "hdfs": {
            "path": hdfs_path or spark_m.get("output_path"),
            "filesWritten": hdfs_files_written,
            "bytesWritten": hdfs_bytes_written,
            "rowsWritten": hdfs_rows_written if hdfs_rows_written is not None else storage_metrics,
            "verified": hdfs_verified,
        },
        "throughput": {
            "recordsPerSecond": throughput_rps,
            "samples": throughput_samples if throughput_samples is not None else [],
        },
        "etaSeconds": eta_seconds,
        "errors": [error] if error else [],
        "fault": fault,
    }


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
                "status": "running",
                "stages": {
                    "source": {"key": "source", "state": "pending", "startedAt": None, "finishedAt": None, "error": None},
                    "kafka": {"key": "kafka", "state": "pending", "startedAt": None, "finishedAt": None, "error": None},
                    "spark": {"key": "spark", "state": "pending", "startedAt": None, "finishedAt": None, "error": None},
                    "hdfs": {"key": "hdfs", "state": "pending", "startedAt": None, "finishedAt": None, "error": None}
                },
                "started_at": _utc_now(),
                "finished_at": None,
                "error": None,
                "source_type": None,
                "source_rows_total": None,
                "source_rows_read": None,
                "kafka_topic": None,
                "kafka_partitions": None,
                "kafka_published": None,
                "kafka_failed": 0,
                "kafka_end_offsets": None,
                "spark_app_id": None,
                "spark_tasks_total": None,
                "spark_tasks_done": None,
                "spark_active_stage": None,
                "spark_records_in": None,
                "spark_records_out": None,
                "spark_duration_ms": None,
                "spark_metrics": None,
                "hdfs_path": None,
                "hdfs_files_written": None,
                "hdfs_bytes_written": None,
                "hdfs_rows_written": None,
                "hdfs_verified": None,
                "throughput_rps": None,
                "throughput_samples": [],
                "eta_seconds": None,
                "fault": None,
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

        if event_type == "source_start":
            st = stages["source"]
            st["state"] = "active"
            st["startedAt"] = _utc_now()
            state["source_type"] = payload.get("plugin")
            state["source_rows_read"] = 0
            state["source_rows_total"] = None
        elif event_type == "source_done":
            st = stages["source"]
            st["state"] = "done"
            st["finishedAt"] = _utc_now()
            rows = payload.get("rows")
            state["source_metrics"] = rows
            state["source_rows_total"] = rows
            state["source_rows_read"] = rows if rows is not None else 0
        elif event_type == "source_failed":
            st = stages["source"]
            st["state"] = "error"
            st["error"] = payload.get("error", "Source failed")
            st["finishedAt"] = _utc_now()
        elif event_type == "kafka_start":
            st = stages["kafka"]
            st["state"] = "active"
            st["startedAt"] = _utc_now()
            state["kafka_topic"] = payload.get("topic")
            if "partitions" in payload:
                state["kafka_partitions"] = payload["partitions"]
            state["kafka_published"] = 0
            state["kafka_failed"] = 0
        elif event_type == "kafka_progress":
            if "published" in payload:
                state["kafka_published"] = payload["published"]
            if "failed" in payload:
                state["kafka_failed"] = payload["failed"]
            if "end_offsets" in payload:
                state["kafka_end_offsets"] = payload["end_offsets"]
            if "partitions" in payload:
                state["kafka_partitions"] = payload["partitions"]
            if "recordsPerSecond" in payload:
                state["throughput_rps"] = payload["recordsPerSecond"]
            if "samples" in payload:
                state["throughput_samples"] = payload["samples"]
            if "etaSeconds" in payload:
                state["eta_seconds"] = payload["etaSeconds"]
        elif event_type == "kafka_done":
            st = stages["kafka"]
            st["state"] = "done"
            st["finishedAt"] = _utc_now()
            pub = payload.get("published", payload.get("count"))
            if pub is not None:
                state["kafka_published"] = pub
                state["kafka_metrics"] = pub
            if "failed" in payload:
                state["kafka_failed"] = payload["failed"]
            if "end_offsets" in payload:
                state["kafka_end_offsets"] = payload["end_offsets"]
            if "partitions" in payload:
                state["kafka_partitions"] = payload["partitions"]
            if payload.get("topic"):
                state["kafka_topic"] = payload.get("topic")
        elif event_type == "stage_skipped":
            stage_key = payload.get("stage")
            if stage_key in stages:
                stages[stage_key]["state"] = "skipped"
                stages[stage_key]["finishedAt"] = _utc_now()
        elif event_type == "spark_start":
            st = stages["spark"]
            st["state"] = "active"
            st["startedAt"] = _utc_now()
            if "appId" in payload:
                state["spark_app_id"] = payload["appId"]
        elif event_type == "spark_progress":
            if "appId" in payload:
                state["spark_app_id"] = payload["appId"]
            if "tasksTotal" in payload:
                state["spark_tasks_total"] = payload["tasksTotal"]
            if "tasksDone" in payload:
                state["spark_tasks_done"] = payload["tasksDone"]
            if "activeStage" in payload:
                state["spark_active_stage"] = payload["activeStage"]
        elif event_type == "spark_done":
            st = stages["spark"]
            st["state"] = "done"
            st["finishedAt"] = _utc_now()
            state["spark_metrics"] = payload
            state["spark_records_in"] = payload.get("input_rows")
            state["spark_records_out"] = payload.get("output_rows")
            dur = payload.get("duration_ms")
            if dur is None and payload.get("duration_seconds") is not None:
                dur = round(payload["duration_seconds"] * 1000.0, 1)
            state["spark_duration_ms"] = dur
        elif event_type == "hdfs_start":
            st = stages["hdfs"]
            st["state"] = "active"
            st["startedAt"] = _utc_now()
            if "path" in payload:
                state["hdfs_path"] = payload["path"]
        elif event_type == "hdfs_progress":
            if "path" in payload:
                state["hdfs_path"] = payload["path"]
            if "filesWritten" in payload:
                state["hdfs_files_written"] = payload["filesWritten"]
            if "bytesWritten" in payload:
                state["hdfs_bytes_written"] = payload["bytesWritten"]
            # Active the first time fileCount > 0
            if state["hdfs_files_written"] and state["hdfs_files_written"] > 0:
                if stages["hdfs"]["state"] == "pending":
                    stages["hdfs"]["state"] = "active"
                    stages["hdfs"]["startedAt"] = _utc_now()
        elif event_type == "hdfs_done":
            st = stages["hdfs"]
            st["state"] = "done"
            st["finishedAt"] = _utc_now()
            state["storage_metrics"] = payload.get("rows")
            state["hdfs_rows_written"] = payload.get("rows")
            state["hdfs_verified"] = payload.get("verified", True)
        elif event_type == "throughput_update":
            if "recordsPerSecond" in payload:
                state["throughput_rps"] = payload["recordsPerSecond"]
            if "samples" in payload:
                state["throughput_samples"] = payload["samples"]
            if "etaSeconds" in payload:
                state["eta_seconds"] = payload["etaSeconds"]
        elif event_type == "run_success":
            state["status"] = "succeeded"
            state["finished_at"] = payload.get("finished_at") or _utc_now()
            for st in stages.values():
                if st["state"] in ("pending", "active"):
                    st["state"] = "done"
                    st["finishedAt"] = _utc_now()
        elif event_type == "run_failed":
            state["status"] = "failed"
            state["finished_at"] = payload.get("finished_at") or _utc_now()
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
            state["finished_at"] = payload.get("finished_at") or _utc_now()
            for st in stages.values():
                if st["state"] == "active":
                    st["state"] = "error"
                    st["error"] = "Cancelled"
                    st["finishedAt"] = _utc_now()
                elif st["state"] == "pending":
                    st["state"] = "skipped"
                    st["finishedAt"] = _utc_now()

    # ── Granular Updates ───────────────────────────────────────────────────────

    def update_source(
        self,
        run_id: str,
        rows_total: int | None = None,
        rows_read: int | None = None,
        plugin: str | None = None,
    ) -> None:
        """Update source stage counters."""
        with self._lock:
            state = self._runs.get(run_id)
            if state is None:
                return
            if rows_total is not None:
                state["source_rows_total"] = rows_total
            if rows_read is not None:
                state["source_rows_read"] = rows_read
            if plugin is not None:
                state["source_type"] = plugin

    def update_kafka(
        self,
        run_id: str,
        published: int | None = None,
        failed: int | None = None,
        end_offsets: int | None = None,
        partitions: int | None = None,
        topic: str | None = None,
    ) -> None:
        """Update kafka stage counters."""
        with self._lock:
            state = self._runs.get(run_id)
            if state is None:
                return
            if published is not None:
                state["kafka_published"] = published
                state["kafka_metrics"] = published
            if failed is not None:
                state["kafka_failed"] = failed
            if end_offsets is not None:
                state["kafka_end_offsets"] = end_offsets
            if partitions is not None:
                state["kafka_partitions"] = partitions
            if topic is not None:
                state["kafka_topic"] = topic

    def update_spark(
        self,
        run_id: str,
        app_id: str | None = None,
        tasks_total: int | None = None,
        tasks_done: int | None = None,
        active_stage: dict[str, Any] | None = None,
        records_in: int | None = None,
        records_out: int | None = None,
        duration_ms: float | None = None,
    ) -> None:
        """Update spark stage counters."""
        with self._lock:
            state = self._runs.get(run_id)
            if state is None:
                return
            if app_id is not None:
                state["spark_app_id"] = app_id
            if tasks_total is not None:
                state["spark_tasks_total"] = tasks_total
            if tasks_done is not None:
                state["spark_tasks_done"] = tasks_done
            if active_stage is not None:
                state["spark_active_stage"] = active_stage
            if records_in is not None:
                state["spark_records_in"] = records_in
            if records_out is not None:
                state["spark_records_out"] = records_out
            if duration_ms is not None:
                state["spark_duration_ms"] = duration_ms

    def update_hdfs(
        self,
        run_id: str,
        path: str | None = None,
        files_written: int | None = None,
        bytes_written: int | None = None,
        rows_written: int | None = None,
        verified: bool | None = None,
    ) -> None:
        """Update HDFS stage counters."""
        with self._lock:
            state = self._runs.get(run_id)
            if state is None:
                return
            if path is not None:
                state["hdfs_path"] = path
            if files_written is not None:
                state["hdfs_files_written"] = files_written
                if files_written > 0 and state["stages"]["hdfs"]["state"] == "pending":
                    state["stages"]["hdfs"]["state"] = "active"
                    state["stages"]["hdfs"]["startedAt"] = _utc_now()
            if bytes_written is not None:
                state["hdfs_bytes_written"] = bytes_written
            if rows_written is not None:
                state["hdfs_rows_written"] = rows_written
                state["storage_metrics"] = rows_written
            if verified is not None:
                state["hdfs_verified"] = verified

    def update_throughput(
        self,
        run_id: str,
        rps: float | None = None,
        samples: list[dict[str, Any]] | None = None,
        eta_seconds: int | None = None,
    ) -> None:
        """Update throughput and ETA metrics."""
        with self._lock:
            state = self._runs.get(run_id)
            if state is None:
                return
            state["throughput_rps"] = rps
            if samples is not None:
                state["throughput_samples"] = samples
            state["eta_seconds"] = eta_seconds

    def set_stage_state(
        self,
        run_id: str,
        stage_key: str,
        stage_state: str,
        error: str | None = None,
    ) -> None:
        """Set state of a specific stage (pending, active, done, error, skipped)."""
        with self._lock:
            state = self._runs.get(run_id)
            if state is None:
                return
            st = state["stages"].get(stage_key)
            if st is not None:
                st["state"] = stage_state
                if stage_state == "active" and st["startedAt"] is None:
                    st["startedAt"] = _utc_now()
                elif stage_state in ("done", "error", "skipped"):
                    st["finishedAt"] = _utc_now()
                if error is not None:
                    st["error"] = error

    # ── Queries ────────────────────────────────────────────────────────────────

    def get_progress(self, run_id: str) -> dict[str, Any] | None:
        """
        Return the current progress snapshot for *run_id*, or None if not tracked.
        """
        with self._lock:
            state = self._runs.get(run_id)
            if state is None:
                return None

            return build_progress_dict(
                run_id=run_id,
                status=state["status"],
                mode="batch",
                started_at=state.get("started_at"),
                updated_at=_utc_now(),
                completed_at=state.get("finished_at"),
                stages=list(state.get("stages", {}).values()),
                source_metrics=state.get("source_metrics"),
                source_type=state.get("source_type"),
                source_rows_total=state.get("source_rows_total"),
                source_rows_read=state.get("source_rows_read"),
                kafka_metrics=state.get("kafka_metrics"),
                kafka_topic=state.get("kafka_topic"),
                kafka_partitions=state.get("kafka_partitions"),
                kafka_published=state.get("kafka_published"),
                kafka_failed=state.get("kafka_failed", 0),
                kafka_end_offsets=state.get("kafka_end_offsets"),
                spark_metrics=state.get("spark_metrics"),
                spark_app_id=state.get("spark_app_id"),
                spark_tasks_total=state.get("spark_tasks_total"),
                spark_tasks_done=state.get("spark_tasks_done"),
                spark_active_stage=state.get("spark_active_stage"),
                spark_records_in=state.get("spark_records_in"),
                spark_records_out=state.get("spark_records_out"),
                spark_duration_ms=state.get("spark_duration_ms"),
                storage_metrics=state.get("storage_metrics"),
                hdfs_path=state.get("hdfs_path"),
                hdfs_files_written=state.get("hdfs_files_written"),
                hdfs_bytes_written=state.get("hdfs_bytes_written"),
                hdfs_rows_written=state.get("hdfs_rows_written"),
                hdfs_verified=state.get("hdfs_verified"),
                throughput_rps=state.get("throughput_rps"),
                throughput_samples=state.get("throughput_samples"),
                eta_seconds=state.get("eta_seconds"),
                error=state.get("error"),
                fault=state.get("fault"),
            )

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
            return state is not None and state["status"] in ("active", "running")

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
            if state is None or state["status"] not in ("active", "running"):
                return False
            self._cancel_requested.add(run_id)
            pid = self._proc_pids.get(run_id)

        if pid is not None:
            def _kill_process_group(target_pid: int, target_run: str) -> None:
                import time as _time
                try:
                    pgid = os.getpgid(target_pid)
                    # Safety guard: never kill the platform backend process group itself
                    if pgid == os.getpgrp():
                        logger.warning(
                            "Target PGID %d matches backend process group; refusing to kill self.",
                            pgid,
                        )
                        return
                    os.killpg(pgid, signal.SIGTERM)
                    logger.info(
                        "Sent SIGTERM to spark-submit process group %d for run %s",
                        target_pid,
                        target_run,
                    )
                    _time.sleep(2.0)
                    try:
                        # Follow up with SIGKILL if still running
                        os.killpg(pgid, signal.SIGKILL)
                        logger.info(
                            "Sent SIGKILL to spark-submit process group %d for run %s",
                            target_pid,
                            target_run,
                        )
                    except ProcessLookupError:
                        pass
                except ProcessLookupError:
                    logger.debug("PID %d already gone when cancel requested.", target_pid)
                except Exception as e:
                    logger.warning("Failed to terminate process group for PID %d: %s", target_pid, e)

            threading.Thread(
                target=_kill_process_group,
                args=(pid, run_id),
                daemon=True,
            ).start()
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
