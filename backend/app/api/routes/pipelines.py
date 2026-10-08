"""
app/api/routes/pipelines.py
-----------------------------
API routes for pipeline config validation and execution.

Phase 2 changes (UI-2)
-----------------------
* POST /api/pipelines/run now returns **202 Accepted** by default.
  The PipelineRun row is created immediately with ``status="running"``; the
  actual execution happens in a background asyncio task.
* ``?wait=true`` restores the original blocking behaviour (200 OK with the
  full result) for callers that cannot consume SSE (e.g., smoke tests,
  ``wait=true`` from the pipeline management page).
* The response for a 202 is ``{"pipeline_run_id": "...", "status": "running"}``.
  Clients poll ``GET /api/pipeline-runs/{id}/progress`` or open the SSE
  stream at ``GET /api/pipeline-runs/{id}/stream`` for live updates.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

import yaml
from fastapi import APIRouter, Depends, Query
from fastapi.responses import JSONResponse
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.schemas.pipeline_config import (
    ValidatePipelineRequest,
    ValidatePipelineResponse,
)
from app.services.pipeline_config_service import validate_pipeline_config
from app.services.pipeline_execution_service import run_pipeline_from_yaml
from app.database.session import get_db, _get_session_factory
from app.models.metric import Metric
from app.models.pipeline import Pipeline
from app.models.pipeline_run import PipelineRun
from app.models.pipeline_version import PipelineVersion
from app.pipeline.progress import tracker

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/pipelines", tags=["pipelines"])


# ── Helpers ────────────────────────────────────────────────────────────────────


def _extract_pipeline_name(yaml_content: str) -> str:
    """Best-effort extraction of pipeline.name from raw YAML."""
    try:
        cfg = yaml.safe_load(yaml_content)
        pipeline_data = cfg.get("pipeline") if isinstance(cfg, dict) else None
        if isinstance(pipeline_data, dict):
            return pipeline_data.get("name", "unknown") or "unknown"
    except Exception:  # noqa: BLE001
        pass
    return "unknown"


async def _get_or_create_pipeline_and_version(
    db: AsyncSession, yaml_content: str
) -> tuple[Pipeline, PipelineVersion]:
    """
    Return (Pipeline, PipelineVersion) for the given YAML, creating rows
    as needed.  Assumes the YAML has already been validated.
    """
    config = yaml.safe_load(yaml_content)
    pipeline_data = config.get("pipeline") if isinstance(config, dict) else None
    name = (
        pipeline_data.get("name", "unknown")
        if isinstance(pipeline_data, dict)
        else "unknown"
    )

    pipeline = (
        await db.execute(select(Pipeline).where(Pipeline.name == name))
    ).scalar_one_or_none()
    if pipeline is None:
        pipeline = Pipeline(name=name)
        db.add(pipeline)
        await db.flush()

    version = (
        await db.execute(
            select(PipelineVersion)
            .where(PipelineVersion.pipeline_id == pipeline.id)
            .order_by(PipelineVersion.version_number.desc())
            .limit(1)
        )
    ).scalar_one_or_none()
    if version is None or version.config != config:
        next_ver = (
            await db.scalar(
                select(func.coalesce(func.max(PipelineVersion.version_number), 0) + 1)
                .where(PipelineVersion.pipeline_id == pipeline.id)
            )
        )
        version = PipelineVersion(
            pipeline_id=pipeline.id,
            version_number=int(next_ver),
            config=config,
        )
        db.add(version)
        await db.flush()

    return pipeline, version


async def _create_running_run(
    db: AsyncSession, pipeline: Pipeline, version: PipelineVersion
) -> PipelineRun:
    """Insert a PipelineRun row with status='running' and commit it."""
    run = PipelineRun(
        pipeline_id=pipeline.id,
        pipeline_version_id=version.id,
        status="running",
        started_at=datetime.now(tz=timezone.utc),
    )
    db.add(run)
    await db.flush()
    return run


async def _finalize_run(
    db: AsyncSession, run_id: str, result: dict[str, Any], is_cancelled: bool = False
) -> None:
    """Update the PipelineRun row and insert Metrics after execution completes."""
    from sqlalchemy import update as sa_update
    import uuid as _uuid

    finished_at = datetime.now(tz=timezone.utc)

    if is_cancelled:
        db_status = "cancelled"
        error_msg = None
    elif result.get("status") == "success":
        db_status = "succeeded"
        error_msg = None
    else:
        db_status = "failed"
        failed_log = next(
            (
                e.get("message")
                for e in reversed(result.get("logs", []))
                if e.get("level") == "ERROR"
            ),
            None,
        )
        error_msg = (failed_log or "")[:4000]

    # Resolve output_path from spark metrics
    stages = result.get("metrics", {})
    spark_metrics = stages.get("spark_job", {})
    output_path = spark_metrics.get("output_path")

    run_uuid = _uuid.UUID(run_id)

    # Update run row
    await db.execute(
        sa_update(PipelineRun)
        .where(PipelineRun.id == run_uuid)
        .values(
            status=db_status,
            finished_at=finished_at,
            error_message=error_msg,
            output_path=output_path,
        )
    )

    # Insert metrics
    source_metrics = next(
        (v for k, v in stages.items() if k.startswith("source:")), {}
    )
    records_processed = spark_metrics.get("output_rows", source_metrics.get("rows_out"))
    if records_processed is not None:
        db.add(
            Metric(
                pipeline_run_id=run_uuid,
                name="records_processed",
                value=float(records_processed),
                unit="records",
            )
        )
    duration_ms = spark_metrics.get("duration_ms")
    if duration_ms is not None:
        db.add(
            Metric(
                pipeline_run_id=run_uuid,
                name="duration_ms",
                value=float(duration_ms),
                unit="ms",
            )
        )


async def _persist_events(run_id: str, events: list[dict]) -> None:
    """Batch-insert progress events to ``pipeline_run_events`` after a run."""
    if not events:
        return

    import uuid as _uuid
    from app.models.pipeline_run_event import PipelineRunEvent

    run_uuid = _uuid.UUID(run_id)
    async with _get_session_factory()() as db:
        try:
            for ev in events:
                db.add(
                    PipelineRunEvent(
                        pipeline_run_id=run_uuid,
                        seq=ev["seq"],
                        event_type=ev["event_type"],
                        payload=ev.get("payload", {}),
                        emitted_at=datetime.fromisoformat(ev["ts"]),
                    )
                )
            await db.commit()
        except Exception as exc:  # noqa: BLE001
            await db.rollback()
            logger.warning("Failed to persist run events for %s: %s", run_id, exc)


async def _run_pipeline_background(run_id: str, yaml_content: str) -> None:
    """
    Execute a pipeline in the background and update the DB row when done.

    This coroutine is launched via ``asyncio.create_task`` so the HTTP
    response (202) is returned immediately.  It creates its own database
    session (the request's session closes when the response is sent).
    """
    is_cancelled = False
    result: dict[str, Any] = {"status": "failed", "metrics": {}, "logs": []}

    try:
        result = await asyncio.to_thread(
            run_pipeline_from_yaml, yaml_content, run_id
        )
    except Exception as exc:  # noqa: BLE001
        logger.error(
            "Unhandled exception in background pipeline run %s: %s", run_id, exc
        )
        result = {
            "status": "failed",
            "metrics": {},
            "logs": [
                {
                    "timestamp": datetime.now(tz=timezone.utc).isoformat(),
                    "level": "ERROR",
                    "step": "background_task",
                    "message": str(exc),
                }
            ],
        }
    finally:
        # Determine if the run was cancelled (SIGTERM caused a SparkJobFailedError
        # which the service returns as status="failed", but the cancel flag is set)
        is_cancelled = tracker.is_cancel_requested(run_id)

        # Emit the final tracker event so SSE subscribers receive it
        finished_at = datetime.now(tz=timezone.utc).isoformat()
        if is_cancelled:
            tracker.emit(run_id, "run_cancelled", {"finished_at": finished_at})
        elif result.get("status") == "success":
            tracker.emit(run_id, "run_success", {"finished_at": finished_at})
        else:
            error_msg = next(
                (
                    e.get("message")
                    for e in reversed(result.get("logs", []))
                    if e.get("level") == "ERROR"
                ),
                "Unknown error",
            )
            tracker.emit(
                run_id,
                "run_failed",
                {"finished_at": finished_at, "error": error_msg},
            )

        # Collect events before cleaning up tracker
        events = tracker.get_events_after(run_id, 0)

        # Persist events and update DB
        async with _get_session_factory()() as db:
            try:
                await _finalize_run(db, run_id, result, is_cancelled=is_cancelled)
                await db.commit()
            except Exception as exc:  # noqa: BLE001
                await db.rollback()
                logger.error("Failed to finalize run %s in DB: %s", run_id, exc)

        await _persist_events(run_id, events)

        # Clean up tracker memory (after a short delay for SSE stream to drain)
        await asyncio.sleep(5)
        tracker.finish(run_id)

    logger.info(
        "Background run %s finished: status=%s cancelled=%s",
        run_id,
        result.get("status"),
        is_cancelled,
    )


# ── Legacy helper (used by wait=true path) ─────────────────────────────────────


async def _record_pipeline_run(
    db: AsyncSession, run: PipelineRun, result: dict[str, Any]
) -> None:
    """Update a PipelineRun row and its Metrics after synchronous execution."""
    await _finalize_run(db, str(run.id), result, is_cancelled=False)


# ── POST /api/pipelines/validate ───────────────────────────────────────────────


@router.post(
    "/validate",
    response_model=ValidatePipelineResponse,
    summary="Validate a pipeline YAML configuration",
    response_description=(
        "Validation result — HTTP 200 always.  "
        "Inspect the 'valid' flag and 'errors' list for the outcome."
    ),
)
async def validate_pipeline(
    request: ValidatePipelineRequest,
) -> ValidatePipelineResponse:
    """
    Parse and validate a pipeline configuration YAML document.

    Accepts raw YAML text in the request body and runs it through two
    validation stages:

    1. **Structural validation** — Pydantic checks types, required fields,
       and discriminated-union operation types.
    2. **Business rules** — cross-section constraints such as "streaming
       pipelines must have a messaging config" or "aggregate columns must
       exist in the schema".

    The response is always HTTP 200.  Use the ``valid`` boolean and ``errors``
    list to determine the outcome — do not rely on 4xx codes.

    This endpoint has **no side effects**: it does not persist anything to
    the database.
    """
    logger.info("Received pipeline config validation request")

    result = validate_pipeline_config(request.yaml_content)

    return ValidatePipelineResponse(
        valid=result["valid"],
        errors=result["errors"],
    )


# ── POST /api/pipelines/run ────────────────────────────────────────────────────


@router.post(
    "/run",
    summary="Execute a pipeline from a YAML configuration",
    response_description=(
        "202 Accepted with {pipeline_run_id, status='running'} by default.  "
        "Add ?wait=true for the original blocking 200 response."
    ),
)
async def run_pipeline(
    request: ValidatePipelineRequest,
    db: AsyncSession = Depends(get_db),
    wait: bool = Query(
        default=False,
        description=(
            "Block until the pipeline finishes and return the full result (200). "
            "Default: return 202 immediately and stream progress via SSE."
        ),
    ),
) -> JSONResponse:
    """
    Parse, validate, and execute a pipeline from a YAML configuration.

    **Default (async) mode** (``wait=false``):

    1. Validates the YAML (same checks as ``/validate``).
    2. Creates a ``PipelineRun`` DB row with ``status="running"`` immediately.
    3. Launches execution in a background asyncio task.
    4. Returns **202 Accepted** with ``{"pipeline_run_id": "...", "status": "running"}``.

    Clients may then:
    * Poll ``GET /api/pipeline-runs/{id}/progress`` for a snapshot.
    * Subscribe to ``GET /api/pipeline-runs/{id}/stream`` (SSE) for live events.
    * Fetch events since a seq: ``GET /api/pipeline-runs/{id}/events?after=N``.
    * Cancel the run: ``POST /api/pipeline-runs/{id}/cancel``.

    **Blocking mode** (``?wait=true``):

    Waits for the execution to complete and returns the full result with
    ``status 200``.  Identical to the pre-UI-2 behaviour.  Use this for
    automated smoke tests or CLI tools that cannot consume SSE.
    """
    logger.info("Received pipeline run request (wait=%s)", wait)

    # ── Quick validation (no side effects) ────────────────────────────────────
    validation = validate_pipeline_config(request.yaml_content)
    if not validation["valid"]:
        return JSONResponse(
            {
                "status": "invalid",
                "valid": False,
                "errors": validation["errors"],
                "run_id": None,
                "pipeline_run_id": None,
                "metrics": {},
                "logs": [],
            },
            status_code=200,  # preserve existing contract for invalid YAML
        )

    # ── Create Pipeline / PipelineVersion / PipelineRun rows ─────────────────
    pipeline, version = await _get_or_create_pipeline_and_version(db, request.yaml_content)
    run = await _create_running_run(db, pipeline, version)
    await db.commit()                      # commit before background task starts
    pipeline_run_id = str(run.id)

    # ── Blocking mode (wait=true) ──────────────────────────────────────────────
    if wait:
        result: dict[str, Any] = await asyncio.to_thread(
            run_pipeline_from_yaml,
            request.yaml_content,
            None,   # no tracker for blocking mode
        )
        async with _get_session_factory()() as bg_db:
            await _finalize_run(bg_db, pipeline_run_id, result)
            await bg_db.commit()

        result["pipeline_run_id"] = pipeline_run_id
        logger.info(
            "Pipeline run (wait=true) finished: status='%s' run=%s",
            result.get("status"),
            pipeline_run_id,
        )
        return JSONResponse(content=result, status_code=200)

    # ── Async mode (default) ───────────────────────────────────────────────────
    # Register with tracker BEFORE launching the task so events are not lost.
    tracker.start(pipeline_run_id, _extract_pipeline_name(request.yaml_content))
    asyncio.create_task(
        _run_pipeline_background(pipeline_run_id, request.yaml_content),
        name=f"pipeline-run-{pipeline_run_id}",
    )

    logger.info("Pipeline run launched (async): run=%s", pipeline_run_id)
    return JSONResponse(
        content={"pipeline_run_id": pipeline_run_id, "status": "running"},
        status_code=202,
    )
