"""
app/api/routes/experiments.py
-------------------------------
ChaosLab experiment management and reporting API.

Endpoints:
    POST   /api/experiments                — create + validate an experiment
    GET    /api/experiments                — list all experiments
    GET    /api/experiments/{id}           — full detail with current stage
    POST   /api/experiments/{id}/run       — trigger async experiment run
    POST   /api/experiments/{id}/cancel    — cancel a running experiment
    GET    /api/reports                    — list all experiment reports
    GET    /api/reports/{id}               — full report detail

Architecture:
    Routes contain no business logic — they call service functions only.
    ExperimentRunner is triggered via ``BackgroundTasks`` so the HTTP call
    returns immediately with the run ID (Phase 8 polls for live status).
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any

import yaml
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.database.session import get_db
from app.reliability.experiment_models import ExperimentModel
from app.reliability.exceptions import (
    ExperimentValidationError,
    TooManyConcurrentExperimentsError,
    UnsafeTargetError,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["experiments"])


from app.api.deps import get_current_user

@router.get(
    "/api/experiments/allowed-targets",
    summary="List all allowed target services for chaos experiments",
)
async def get_allowed_targets() -> dict:
    """Return the strict allow-list of services ChaosLab may target."""
    from app.reliability.safety import ALLOWED_TARGET_SERVICES

    return {"allowed_services": sorted(ALLOWED_TARGET_SERVICES)}


@router.get(
    "/api/experiments/options",
    summary="List available targets, faults, and limits for chaos experiments",
)
async def get_experiment_options_endpoint(
    current_user: str = Depends(get_current_user),
) -> dict:
    """Return allow-listed non-critical targets, registered faults, and duration limits."""
    from app.reliability.safety import get_experiment_options

    return get_experiment_options()

# ── In-memory cancellation registry ───────────────────────────────────────────
# Maps experiment_run_id (str) → ExperimentRunner instance (if running).
# A production system would use Redis or a DB flag; for Phase 7 this is
# sufficient for single-process, single-worker deployments.
_RUNNING_RUNNERS: dict[str, Any] = {}


# ── Request / Response schemas ─────────────────────────────────────────────────


class ExperimentCreateRequest(BaseModel):
    """Request body for POST /api/experiments."""

    yaml_content: str = ""
    """Raw YAML string of the experiment definition."""

    pipeline_yaml: str = ""
    """Optional pipeline YAML to run as part of the experiment."""


class ExperimentCreateResponse(BaseModel):
    """Response for POST /api/experiments."""

    experiment_id: str
    experiment_name: str
    target_service: str
    fault_type: str
    fault_duration: int
    status: str = "created"


# ── Helper: parse + validate experiment YAML ──────────────────────────────────

def _parse_experiment_yaml(yaml_content: str) -> ExperimentModel:
    """Parse and validate experiment YAML, raising HTTPException on error."""
    try:
        data = yaml.safe_load(yaml_content)
        return ExperimentModel.model_validate(data)
    except Exception as exc:
        raise HTTPException(
            status_code=422,
            detail=f"Experiment YAML validation failed: {exc}",
        )


def _get_sync_session():
    """Create a synchronous SQLAlchemy session for the background runner."""
    from app.database.session import SyncSessionLocal

    session = SyncSessionLocal()
    try:
        yield session
    finally:
        session.close()


# ── Background runner task ─────────────────────────────────────────────────────

def _run_experiment_background(
    experiment_db_id: str,
    experiment_run_id: str,
    experiment_config: ExperimentModel,
    pipeline_yaml: str,
) -> None:
    """
    Run the full experiment lifecycle in a background thread.

    Persists a ``ChaosExperimentRecord`` DB row with the result.
    Updates status at every stage so the GET endpoint shows live progress.
    """
    from app.database.session import SyncSessionLocal
    from app.reliability.experiment_runner import ExperimentRunner

    with SyncSessionLocal() as db:
        runner = ExperimentRunner(db_session=db)
        _RUNNING_RUNNERS[experiment_db_id] = runner
        try:
            report = runner.run_experiment(
                experiment_config=experiment_config,
                associated_pipeline_yaml=pipeline_yaml,
                existing_experiment_id=experiment_db_id,
                existing_run_id=experiment_run_id,
            )
            logger.info(
                "Background experiment %s completed: result=%s.",
                experiment_db_id,
                report.get("result"),
            )
        except TooManyConcurrentExperimentsError as exc:
            logger.warning("Experiment %s rejected: %s", experiment_db_id, exc)
            _mark_background_run_failed(db, experiment_run_id, str(exc))
        except Exception as exc:
            logger.error(
                "Background experiment %s failed: %s", experiment_db_id, exc
            )
            _mark_background_run_failed(db, experiment_run_id, str(exc))
        finally:
            _RUNNING_RUNNERS.pop(experiment_db_id, None)


def _mark_background_run_failed(db: Any, run_id: str, error: str) -> None:
    """Persist an early runner failure so the UI never polls forever."""
    from app.models.experiment_run import ExperimentRun

    run = db.get(ExperimentRun, run_id)
    if run is None or run.status == "cancelled":
        return

    results = run.results or {}
    results["error"] = error
    run.results = results
    run.status = "failed"
    run.finished_at = datetime.now(tz=timezone.utc)
    db.add(run)
    db.commit()


# ── Routes ─────────────────────────────────────────────────────────────────────


@router.post(
    "/api/experiments",
    status_code=201,
    summary="Create and validate a new chaos experiment definition",
)
async def create_experiment(
    request: ExperimentCreateRequest,
    db: AsyncSession = Depends(get_db),
) -> ExperimentCreateResponse:
    """
    Parse, validate, and persist a new experiment definition.

    Does NOT run the experiment — call ``POST /api/experiments/{id}/run``
    to trigger execution.

    Raises
    ------
    422
        If the YAML fails Pydantic validation or safety pre-checks.
    """
    experiment_config = _parse_experiment_yaml(request.yaml_content)

    # Pre-validate safety (before any DB write)
    try:
        from app.reliability.safety import SafetyValidator

        safety = SafetyValidator()
        safety.validate_duration(experiment_config.fault.duration)
    except UnsafeTargetError as exc:
        raise HTTPException(status_code=422, detail=str(exc))

    # Persist experiment record via async session
    from app.models.experiment import Experiment

    experiment = Experiment(
        name=experiment_config.experiment.name,
        description=None,
        experiment_yaml=request.yaml_content,
        pipeline_yaml=request.pipeline_yaml or None,
    )
    db.add(experiment)
    await db.flush()
    await db.refresh(experiment)

    await db.commit()

    return ExperimentCreateResponse(
        experiment_id=str(experiment.id),
        experiment_name=experiment_config.experiment.name,
        target_service=experiment_config.target.service,
        fault_type=experiment_config.fault.type,
        fault_duration=experiment_config.fault.duration,
        status="created",
    )


@router.get(
    "/api/experiments",
    summary="List all experiments with their latest status",
)
async def list_experiments(
    db: AsyncSession = Depends(get_db),
) -> list[dict]:
    """Return all experiments and their most recent run status."""
    from sqlalchemy import select
    from app.models.experiment import Experiment
    from app.models.experiment_run import ExperimentRun

    stmt = select(Experiment).order_by(Experiment.created_at.desc())
    result = await db.execute(stmt)
    experiments = result.scalars().all()

    output = []
    for exp in experiments:
        # Get latest run
        run_stmt = (
            select(ExperimentRun)
            .where(ExperimentRun.experiment_id == exp.id)
            .order_by(ExperimentRun.created_at.desc())
            .limit(1)
        )
        run_result = await db.execute(run_stmt)
        latest_run = run_result.scalar_one_or_none()

        run_params = latest_run.params if latest_run and latest_run.params else {}
        output.append(
            {
                "experiment_id": str(exp.id),
                "name": exp.name,
                "created_at": exp.created_at.isoformat(),
                "latest_run_status": latest_run.status if latest_run else None,
                "latest_run_id": str(latest_run.id) if latest_run else None,
                "target_service": run_params.get("target_service"),
                "fault_type": run_params.get("fault_type"),
            }
        )
    return output


@router.get(
    "/api/experiments/{experiment_id}",
    summary="Full experiment detail with current lifecycle stage",
)
async def get_experiment(
    experiment_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
) -> dict:
    """Return the full experiment record including the latest run's live status."""
    from sqlalchemy import select
    from app.models.experiment import Experiment
    from app.models.experiment_run import ExperimentRun

    experiment = await db.get(Experiment, experiment_id)
    if experiment is None:
        raise HTTPException(status_code=404, detail="Experiment not found.")

    run_stmt = (
        select(ExperimentRun)
        .where(ExperimentRun.experiment_id == experiment_id)
        .order_by(ExperimentRun.created_at.desc())
        .limit(1)
    )
    run_result = await db.execute(run_stmt)
    latest_run = run_result.scalar_one_or_none()

    params = latest_run.params if latest_run and latest_run.params else {}
    results = latest_run.results if latest_run and latest_run.results else {}
    # Stage-transition log lines are stored in results["logs"] by the runner.
    # Fall back to an empty list so the UI always receives a list (never null).
    execution_logs = results.get("logs") if isinstance(results.get("logs"), list) else []
    return {
        "id": str(experiment.id),
        "name": experiment.name,
        "description": experiment.description,
        "experimentYaml": experiment.experiment_yaml,
        "pipelineYaml": experiment.pipeline_yaml,
        "target": params.get("target_service", "not started"),
        "faultType": params.get("fault_type", "not started"),
        "status": latest_run.status if latest_run else "pending",
        "createdAt": experiment.created_at.isoformat(),
        "lifecycleStage": latest_run.stage if latest_run and latest_run.stage else "pending",
        # Real timestamps so the UI can compute elapsed time from server clock.
        "runStartedAt": latest_run.started_at.isoformat() if latest_run and latest_run.started_at else None,
        "runFinishedAt": latest_run.finished_at.isoformat() if latest_run and latest_run.finished_at else None,
        "logs": execution_logs,
        "results": results,
    }



@router.post(
    "/api/experiments/{experiment_id}/run",
    status_code=202,
    summary="Trigger experiment execution asynchronously",
)
async def run_experiment(
    experiment_id: uuid.UUID,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    request: ExperimentCreateRequest | None = None,
) -> dict:
    """
    Trigger the ExperimentRunner asynchronously.

    Returns immediately with a ``run_id`` (HTTP 202).  Poll
    ``GET /api/experiments/{id}`` for live lifecycle stage progress.

    The request body must contain the experiment YAML (and optionally
    the pipeline YAML) since experiments are stateless definitions.
    """
    experiment_config = None

    try:
        from app.reliability.safety import SafetyValidator

        safety = SafetyValidator()
        pass
        # Note: validate_target requires Docker socket — deferred to runner
    except UnsafeTargetError as exc:
        raise HTTPException(status_code=422, detail=str(exc))

    from app.models.experiment import Experiment
    from app.models.experiment_run import ExperimentRun

    experiment = await db.get(Experiment, experiment_id)
    if experiment is None:
        raise HTTPException(status_code=404, detail="Experiment not found.")

    if request is not None and request.yaml_content.strip():
        if (
            request.yaml_content != experiment.experiment_yaml
            or (request.pipeline_yaml or None) != experiment.pipeline_yaml
        ):
            raise HTTPException(
                status_code=409,
                detail="Run definitions must match the persisted experiment definition.",
            )
    if not experiment.experiment_yaml:
        raise HTTPException(
            status_code=409,
            detail="Experiment has no persisted definition; recreate it before running.",
        )
    experiment_config = _parse_experiment_yaml(experiment.experiment_yaml)
    SafetyValidator().validate_duration(experiment_config.fault.duration)

    run = ExperimentRun(
        experiment_id=experiment.id,
        status="pending",
        params={
            "target_service": experiment_config.target.service,
            "fault_type": experiment_config.fault.type,
            "fault_duration": experiment_config.fault.duration,
            "experiment_config": experiment_config.model_dump(),
        },
        results={},
        started_at=datetime.now(tz=timezone.utc),
    )
    db.add(run)
    await db.commit()
    await db.refresh(run)
    run_id = str(run.id)

    background_tasks.add_task(
        _run_experiment_background,
        str(experiment_id),
        run_id,
        experiment_config,
        experiment.pipeline_yaml or "",
    )

    return {
        "message": "Experiment run triggered.",
        "experiment_id": str(experiment_id),
        "run_id": run_id,
        "status": "running",
        "poll_url": f"/api/experiments/{experiment_id}",
    }


@router.post(
    "/api/experiments/{experiment_id}/cancel",
    summary="Cancel a running experiment",
)
async def cancel_experiment(
    experiment_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
) -> dict:
    """
    Signal the running ExperimentRunner to cancel at the next stage boundary.

    The runner's ``finally`` block will still execute rollback.
    """
    runner = _RUNNING_RUNNERS.get(str(experiment_id))
    if runner is not None:
        runner.cancel()
        return {
            "message": "Cancellation requested.",
            "experiment_id": str(experiment_id),
        }

    from sqlalchemy import select
    from app.models.experiment import Experiment
    from app.models.experiment_run import ExperimentRun

    experiment = await db.get(Experiment, experiment_id)
    if experiment is None:
        raise HTTPException(status_code=404, detail="Experiment not found.")

    result = await db.execute(
        select(ExperimentRun)
        .where(ExperimentRun.experiment_id == experiment_id)
        .order_by(ExperimentRun.created_at.desc())
        .limit(1)
    )
    run = result.scalar_one_or_none()
    if run is None:
        run = ExperimentRun(
            experiment_id=experiment.id,
            status="cancelled",
            params={},
            results={"message": "Cancelled before the run started."},
            started_at=datetime.now(tz=timezone.utc),
            finished_at=datetime.now(tz=timezone.utc),
        )
        db.add(run)
    elif run.status not in {"succeeded", "failed", "cancelled"}:
        results = run.results or {}
        results["message"] = "Cancelled by operator."
        run.results = results
        run.status = "cancelled"
        run.finished_at = datetime.now(tz=timezone.utc)
        db.add(run)
    await db.commit()

    return {"message": "Cancellation requested.", "experiment_id": str(experiment_id)}


@router.get(
    "/api/reports",
    summary="List all generated experiment reports",
)
async def list_reports(db: AsyncSession = Depends(get_db)) -> list[dict]:
    """Return all persisted experiment reports."""
    from sqlalchemy import select
    from app.models.report import Report

    stmt = (
        select(Report)
        .where(Report.report_type == "chaos_experiment")
        .order_by(Report.created_at.desc())
    )
    result = await db.execute(stmt)
    reports = result.scalars().all()
    return [
        {
            "report_id": str(r.id),
            "title": r.title,
            "created_at": r.created_at.isoformat(),
            "summary": r.metadata_,
        }
        for r in reports
    ]


@router.get(
    "/api/reports/{report_id}",
    summary="Full experiment report detail",
)
async def get_report(
    report_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
) -> dict:
    """Return the full report matching the canonical JSON shape from Section 10."""
    from app.models.report import Report

    report = await db.get(Report, report_id)
    if report is None:
        raise HTTPException(status_code=404, detail="Report not found.")

    return report.metadata_ or {
        "report_id": str(report.id),
        "title": report.title,
        "created_at": report.created_at.isoformat(),
    }

