"""
app/api/routes/pipeline_runs.py
---------------------------------
REST and SSE endpoints for monitoring and controlling pipeline runs.

Endpoints added in UI-2
-----------------------
GET  /api/pipeline-runs/{id}/progress
    JSON snapshot of the current run state (stages, status, times).
    Returns tracker data for active runs; DB data for finished runs.

GET  /api/pipeline-runs/{id}/events?after={seq}
    All progress events with seq > after.
    Returns tracker memory for active runs; DB rows for finished runs.

GET  /api/pipeline-runs/{id}/stream
    Server-Sent Events stream with real-time progress events.
    Supports Bearer token via Authorization header OR ?token= query param
    (EventSource in some browsers cannot send custom headers).
    Sends heartbeats every 15 s and a final ``event: done`` on completion.

POST /api/pipeline-runs/{id}/cancel
    Request graceful cancellation.  Sends SIGTERM to the spark-submit
    subprocess (if active) and sets the cancel-requested flag so the
    executor aborts at the next stage boundary.

Existing endpoints (unchanged)
-------------------------------
GET  /api/pipeline-runs
GET  /api/pipeline-runs/{run_id}
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid as _uuid
from datetime import datetime, timezone
from typing import Any, AsyncGenerator

from fastapi import APIRouter, Depends, Header, HTTPException, Query, status
from fastapi.responses import JSONResponse, StreamingResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user
from app.core.security import decode_access_token
from app.database.session import get_db
from app.models.pipeline_run import PipelineRun
from app.models.pipeline_run_event import PipelineRunEvent
from app.pipeline.progress import tracker

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/pipeline-runs", tags=["pipeline-runs"])


# ── Helpers ────────────────────────────────────────────────────────────────────


def _parse_run_id(run_id: str) -> _uuid.UUID:
    try:
        return _uuid.UUID(run_id)
    except ValueError:
        raise HTTPException(status_code=422, detail=f"Invalid run UUID: {run_id!r}")


async def _get_run_or_404(db: AsyncSession, run_uuid: _uuid.UUID) -> PipelineRun:
    run = await db.get(PipelineRun, run_uuid)
    if run is None:
        raise HTTPException(status_code=404, detail=f"Pipeline run {run_uuid} not found.")
    return run


# ── SSE auth helper ────────────────────────────────────────────────────────────


async def _verify_sse_token(token: str | None) -> str:
    """
    Decode and verify a JWT token for the SSE endpoint.

    Raises HTTP 401 when the token is missing or invalid.
    Returns the username (``sub`` claim) on success.
    """
    from jose import JWTError

    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing authentication token.",
        )
    try:
        payload = decode_access_token(token)
        username: str | None = payload.get("sub")
        if not username:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token: missing sub claim.",
            )
        return username
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token.",
        )


# ── GET /api/pipeline-runs ─────────────────────────────────────────────────────


@router.get(
    "",
    summary="List all pipeline runs",
    dependencies=[Depends(get_current_user)],
)
async def list_pipeline_runs(
    db: AsyncSession = Depends(get_db),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> JSONResponse:
    """Return a paginated list of pipeline runs ordered by start time descending."""
    result = await db.execute(
        select(PipelineRun)
        .order_by(PipelineRun.started_at.desc().nullslast())
        .limit(limit)
        .offset(offset)
    )
    runs = result.scalars().all()
    return JSONResponse(
        content=[
            {
                "id": str(run.id),
                "pipeline_id": str(run.pipeline_id),
                "pipeline_version_id": str(run.pipeline_version_id),
                "status": run.status,
                "started_at": run.started_at.isoformat() if run.started_at else None,
                "finished_at": run.finished_at.isoformat() if run.finished_at else None,
                "error_message": run.error_message,
                "output_path": run.output_path,
                "actual_output_path": run.output_path,
            }
            for run in runs
        ]
    )


# ── GET /api/pipeline-runs/{run_id} ───────────────────────────────────────────


@router.get(
    "/{run_id}",
    summary="Get a single pipeline run",
    dependencies=[Depends(get_current_user)],
)
async def get_pipeline_run(
    run_id: str,
    db: AsyncSession = Depends(get_db),
) -> JSONResponse:
    run_uuid = _parse_run_id(run_id)
    run = await _get_run_or_404(db, run_uuid)
    return JSONResponse(
        content={
            "id": str(run.id),
            "pipeline_id": str(run.pipeline_id),
            "pipeline_version_id": str(run.pipeline_version_id),
            "status": run.status,
            "started_at": run.started_at.isoformat() if run.started_at else None,
            "finished_at": run.finished_at.isoformat() if run.finished_at else None,
            "error_message": run.error_message,
            "output_path": run.output_path,
            "actual_output_path": run.output_path,
        }
    )


# ── GET /api/pipeline-runs/{id}/progress ──────────────────────────────────────


@router.get(
    "/{run_id}/progress",
    summary="Get current progress snapshot for a run",
    dependencies=[Depends(get_current_user)],
)
async def get_run_progress(
    run_id: str,
    db: AsyncSession = Depends(get_db),
) -> JSONResponse:
    """
    Return the current progress snapshot.

    If the run is still active, data comes from the in-memory tracker (fast,
    live).  If the run has finished, data is reconstructed from the DB row
    and any persisted events.

    The ``stages`` object follows the RunProgress schema from Section 8.1:
    unknown values are ``null``, never fabricated.
    """
    run_uuid = _parse_run_id(run_id)

    # Try in-memory tracker first (active runs)
    progress = tracker.get_progress(run_id)
    if progress is not None:
        return JSONResponse(content=progress)

    # Fall back to DB for finished runs
    run = await _get_run_or_404(db, run_uuid)

    # Reconstruct stage data from persisted events
    events_result = await db.execute(
        select(PipelineRunEvent)
        .where(PipelineRunEvent.pipeline_run_id == run_uuid)
        .order_by(PipelineRunEvent.seq)
    )
    events = events_result.scalars().all()

    # Build stage summary from event history
    stages: dict[str, Any] = {}
    for ev in events:
        et = ev.event_type
        pl = ev.payload or {}
        if et == "source_done":
            stages["source"] = {"status": "done", "rows": pl.get("rows")}
        elif et == "kafka_done":
            stages["kafka"] = {"status": "done", "published": pl.get("count")}
        elif et == "spark_done":
            stages["spark"] = {
                "status": "done",
                "input_rows": pl.get("input_rows"),
                "output_rows": pl.get("output_rows"),
                "output_path": pl.get("output_path"),
            }
        elif et == "storage_done":
            stages["storage"] = {"status": "done", "rows": pl.get("rows")}

    return JSONResponse(
        content={
            "run_id": run_id,
            "status": run.status,
            "stages": stages,
            "started_at": run.started_at.isoformat() if run.started_at else None,
            "finished_at": run.finished_at.isoformat() if run.finished_at else None,
            "error": run.error_message,
            "pipeline_name": None,  # not stored on the run row; available via pipeline join
        }
    )


# ── GET /api/pipeline-runs/{id}/events?after= ─────────────────────────────────


@router.get(
    "/{run_id}/events",
    summary="List progress events since a sequence number",
    dependencies=[Depends(get_current_user)],
)
async def get_run_events(
    run_id: str,
    after: int = Query(
        default=0,
        ge=0,
        description="Return events with seq > this value (0 = all events).",
    ),
    db: AsyncSession = Depends(get_db),
) -> JSONResponse:
    """
    Return progress events for a run since *after*.

    If the run is still active, events are read from the in-memory tracker.
    If the run is finished, events are read from the ``pipeline_run_events`` table.
    """
    run_uuid = _parse_run_id(run_id)

    # Active run — serve from tracker
    if tracker.is_active(run_id):
        events = tracker.get_events_after(run_id, after)
        return JSONResponse(content={"run_id": run_id, "events": events})

    # Finished run — check run exists then query DB
    await _get_run_or_404(db, run_uuid)
    result = await db.execute(
        select(PipelineRunEvent)
        .where(
            PipelineRunEvent.pipeline_run_id == run_uuid,
            PipelineRunEvent.seq > after,
        )
        .order_by(PipelineRunEvent.seq)
    )
    db_events = result.scalars().all()
    return JSONResponse(
        content={
            "run_id": run_id,
            "events": [
                {
                    "seq": ev.seq,
                    "event_type": ev.event_type,
                    "payload": ev.payload,
                    "ts": ev.emitted_at.isoformat(),
                }
                for ev in db_events
            ],
        }
    )


# ── GET /api/pipeline-runs/{id}/stream ────────────────────────────────────────


async def _sse_generator(
    run_id: str, db: AsyncSession
) -> AsyncGenerator[str, None]:
    """
    Async generator that produces SSE-formatted strings.

    Protocol
    --------
    * Replays all past events first (catch-up on reconnect).
    * Subscribes to new events via the tracker.
    * Sends ``event: heartbeat`` every 15 s when idle.
    * Sends ``event: done`` when the run finishes and closes the stream.
    """
    run_uuid = _parse_run_id(run_id)

    HEARTBEAT_INTERVAL = 15.0  # seconds
    _sentinel = object()       # signals end-of-stream

    def _fmt_event(ev: dict, event_name: str | None = None) -> str:
        name = event_name or ev.get("event_type", "message")
        return f"event: {name}\ndata: {json.dumps(ev)}\n\n"

    def _fmt_heartbeat() -> str:
        return f"event: heartbeat\ndata: {json.dumps({'ts': datetime.now(tz=timezone.utc).isoformat()})}\n\n"

    # If the run is already finished, stream events from DB and close immediately
    run = await db.get(PipelineRun, run_uuid)
    if run is None:
        yield f"event: error\ndata: {json.dumps({'detail': 'Run not found.'})}\n\n"
        return

    if run.status in ("succeeded", "failed", "cancelled"):
        # Run already finished — replay from DB and close
        result = await db.execute(
            select(PipelineRunEvent)
            .where(PipelineRunEvent.pipeline_run_id == run_uuid)
            .order_by(PipelineRunEvent.seq)
        )
        for ev in result.scalars().all():
            yield _fmt_event({
                "seq": ev.seq,
                "event_type": ev.event_type,
                "payload": ev.payload,
                "ts": ev.emitted_at.isoformat(),
            })
        yield f"event: done\ndata: {json.dumps({'status': run.status})}\n\n"
        return

    # Run is active — subscribe to tracker
    queue = tracker.subscribe(run_id)
    try:
        # Replay past events first
        for ev in tracker.replay_events(run_id):
            yield _fmt_event(ev)

        import time
        last_progress_ts = time.time()
        last_heartbeat_ts = time.time()

        # Stream new events as they arrive
        while True:
            try:
                event = await asyncio.wait_for(queue.get(), timeout=1.0)
                yield _fmt_event(event)

                # Terminal events — close stream after flushing
                if event.get("event_type") in ("run_success", "run_failed", "run_cancelled"):
                    status_map = {
                        "run_success": "success",
                        "run_failed": "failed",
                        "run_cancelled": "cancelled",
                    }
                    final_status = status_map.get(event["event_type"], "unknown")
                    progress = tracker.get_progress(run_id)
                    if progress:
                        yield _fmt_event(progress, "progress")
                    yield f"event: done\ndata: {json.dumps({'status': final_status})}\n\n"
                    return

            except asyncio.TimeoutError:
                pass
            
            now = time.time()
            if now - last_progress_ts >= 1.0:
                progress = tracker.get_progress(run_id)
                if progress:
                    yield _fmt_event(progress, "progress")
                last_progress_ts = now

            if now - last_heartbeat_ts >= HEARTBEAT_INTERVAL:
                # No event in 15 s — send heartbeat to keep the connection alive
                yield _fmt_heartbeat()
                last_heartbeat_ts = now

                # If the run is no longer tracked (tracker.finish() was called),
                # close the stream gracefully
                if not tracker.is_active(run_id):
                    final_run = await db.get(PipelineRun, run_uuid)
                    final_status = final_run.status if final_run else "unknown"
                    yield f"event: done\ndata: {json.dumps({'status': final_status})}\n\n"
                    return
    finally:
        tracker.unsubscribe(run_id, queue)


@router.get(
    "/{run_id}/stream",
    summary="Server-Sent Events stream for live run progress",
    response_class=StreamingResponse,
)
async def stream_run_progress(
    run_id: str,
    token: str | None = Query(
        default=None,
        description=(
            "JWT bearer token.  Use this query parameter when the client cannot "
            "send custom headers (e.g. browser EventSource).  The Authorization "
            "header is preferred."
        ),
    ),
    authorization: str | None = Header(default=None),
    db: AsyncSession = Depends(get_db),
) -> StreamingResponse:
    """
    Open a Server-Sent Events stream for live progress updates.

    Authentication is required.  Pass the JWT token either as:
    * ``Authorization: Bearer <token>`` header (preferred), or
    * ``?token=<jwt>`` query parameter (for browser EventSource).

    The stream sends:
    * All past events (replay) immediately on connect.
    * New events as they are emitted by the executor.
    * ``event: heartbeat`` every 15 s when idle.
    * ``event: done`` when the run finishes, then closes.
    """
    # Auth: check Authorization header first, fall back to ?token
    raw_token = token
    if not raw_token and authorization:
        parts = authorization.split()
        if len(parts) == 2 and parts[0].lower() == "bearer":
            raw_token = parts[1]

    await _verify_sse_token(raw_token)

    return StreamingResponse(
        _sse_generator(run_id, db),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",    # disable nginx buffering for this stream
        },
    )


# ── POST /api/pipeline-runs/{id}/cancel ───────────────────────────────────────


@router.post(
    "/{run_id}/cancel",
    summary="Request cancellation of a running pipeline",
    dependencies=[Depends(get_current_user)],
)
async def cancel_pipeline_run(
    run_id: str,
    db: AsyncSession = Depends(get_db),
) -> JSONResponse:
    """
    Send a cancellation request to a running pipeline.

    If the run is in the Spark stage, SIGTERM is sent to the spark-submit
    subprocess.  Otherwise the executor will abort at the next stage boundary.

    Returns ``{"accepted": true}`` when the request is accepted, or
    ``{"accepted": false, "detail": "..."}`` when the run is not found or
    not currently running.
    """
    run_uuid = _parse_run_id(run_id)
    run = await db.get(PipelineRun, run_uuid)
    if run is None:
        return JSONResponse(
            {"accepted": False, "detail": "Run not found."},
            status_code=404,
        )
    if run.status not in ("running", "active"):
        return JSONResponse(
            {
                "accepted": False,
                "detail": (
                    f"Run {run_id} is not in 'running' state "
                    f"(current status: '{run.status}')."
                ),
            },
            status_code=409,
        )

    accepted = tracker.request_cancel(run_id)
    logger.info("Cancel requested for run %s: accepted=%s", run_id, accepted)
    return JSONResponse(
        content={"accepted": accepted, "run_id": run_id},
        status_code=202 if accepted else 409,
    )
