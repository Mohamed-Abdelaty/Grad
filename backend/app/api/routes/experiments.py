"""
app/api/routes/experiments.py
-------------------------------
ChaosLab experiment management and reporting API.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any

import yaml
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, Response
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user
from app.database.session import get_db
from app.reliability.exceptions import (
    ExperimentValidationError,
    TooManyConcurrentExperimentsError,
    UnsafeTargetError,
)
from app.reliability.experiment_models import ExperimentModel

logger = logging.getLogger(__name__)

router = APIRouter(tags=["experiments"])


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
_RUNNING_RUNNERS: dict[str, Any] = {}


# ── Request / Response schemas ─────────────────────────────────────────────────


class ExperimentCreateRequest(BaseModel):
    """Request body for POST /api/experiments."""

    yaml_content: str = ""
    pipeline_yaml: str = ""


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

    run_uuid = uuid.UUID(run_id) if isinstance(run_id, str) else run_id
    run = db.get(ExperimentRun, run_uuid)
    if run is None or run.status == "cancelled":
        return

    results = run.results or {}
    results["error"] = error
    results["result"] = "FAIL"
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
    """
    experiment_config = _parse_experiment_yaml(request.yaml_content)

    # Pre-validate safety (before any DB write)
    try:
        from app.reliability.safety import CRITICAL_PLATFORM_SERVICES, SafetyValidator

        target_service = experiment_config.target.service
        if target_service in CRITICAL_PLATFORM_SERVICES:
            raise UnsafeTargetError(
                f"Service '{target_service}' is a protected platform service and cannot be targeted.",
                service_name=target_service,
            )
        safety = SafetyValidator()
        safety.validate_duration(experiment_config.fault.duration)
    except UnsafeTargetError as exc:
        raise HTTPException(status_code=422, detail=str(exc))

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
        run_stmt = (
            select(ExperimentRun)
            .where(ExperimentRun.experiment_id == exp.id)
            .order_by(ExperimentRun.created_at.desc())
            .limit(1)
        )
        run_result = await db.execute(run_stmt)
        latest_run = run_result.scalar_one_or_none()

        run_params = latest_run.params if latest_run and latest_run.params else {}
        results = latest_run.results if latest_run and latest_run.results else {}
        output.append(
            {
                "experiment_id": str(exp.id),
                "id": str(exp.id),
                "name": exp.name,
                "created_at": exp.created_at.isoformat(),
                "createdAt": exp.created_at.isoformat(),
                "latest_run_status": latest_run.status if latest_run else None,
                "latest_run_id": str(latest_run.id) if latest_run else None,
                "target_service": run_params.get("target_service"),
                "target": run_params.get("target_service"),
                "fault_type": run_params.get("fault_type"),
                "faultType": run_params.get("fault_type"),
                "status": latest_run.status if latest_run else "pending",
                "result": results.get("result"),
                "duration": run_params.get("fault_duration"),
                "startedAt": latest_run.started_at.isoformat() if latest_run and latest_run.started_at else None,
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
    from app.reliability.experiment_runner import LIFECYCLE_PHASES
    from app.reliability.fault_state import fault_state_registry

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
    exp_config = params.get("experiment_config") or {}
    validation_cfg = exp_config.get("validation") or {}
    fault_cfg = exp_config.get("fault") or {}

    raw_samples = results.get("samples") or []
    avail_samples = [
        {"t": s["timestamp"], "available": s["available"]}
        for s in raw_samples
        if isinstance(s, dict) and "timestamp" in s and "available" in s
    ] if raw_samples else None

    avail_pct = results.get("availability_percentage")
    if avail_pct is not None:
        err_rate = round(max(0.0, min(1.0, (100.0 - float(avail_pct)) / 100.0)), 4)
    else:
        err_rate = None

    total_ops = results.get("total_samples", len(raw_samples) if raw_samples else 0)
    avail_ops = results.get("available_samples", sum(1 for s in raw_samples if s.get("available")))
    err_count = total_ops - avail_ops if total_ops is not None else 0

    target_service = params.get("target_service") or results.get("target_service") or "not started"
    fault_type = params.get("fault_type") or results.get("fault_type") or "not started"
    fault_duration = params.get("fault_duration") or results.get("fault_duration") or 20

    # Ensure all 12 phases always present
    recorded_phases = {p["name"]: p for p in (results.get("phases") or []) if isinstance(p, dict) and "name" in p}
    full_phases = []
    current_phase = latest_run.stage if latest_run and latest_run.stage else "validating"
    for phase_name in LIFECYCLE_PHASES:
        if phase_name in recorded_phases:
            full_phases.append(recorded_phases[phase_name])
        else:
            full_phases.append({
                "name": phase_name,
                "state": "pending",
                "startedAt": None,
                "finishedAt": None,
            })

    # Fault phase badge calculation
    curr_fault = fault_state_registry.current()
    if curr_fault and curr_fault.experimentId == str(experiment.id):
        fault_phase = curr_fault.phase
    elif results.get("recovered_at") or results.get("recovery_time_seconds") is not None:
        fault_phase = "recovered"
    elif results.get("fault_reverted_at"):
        fault_phase = "recovering"
    elif results.get("fault_injected_at"):
        fault_phase = "injected"
    else:
        fault_phase = "not_injected"

    audit = results.get("fault_audit") or {}
    fault_injected_at = results.get("fault_injected_at") or audit.get("injected_at")
    fault_reverted_at = results.get("fault_reverted_at")
    recovered_at = results.get("recovered_at")

    results_payload = dict(results)
    results_payload.update({
        "recoveryTimeSeconds": results.get("recovery_time_seconds"),
        "dataLoss": results.get("data_loss", 0),
        "duplicates": results.get("duplicate_ids_found", 0),
        "expectedRecords": results.get("expected_record_count", 0),
        "processedRecords": results.get("actual_records", 0),
        "throughputBaseline": results.get("throughput_baseline"),
        "throughputDuringFault": None,
        "throughputAfter": results.get("throughput_records_per_sec"),
        "throughputDegradationPercent": results.get("throughput_degradation_percent"),
        "availabilityPercentage": avail_pct,
        "errorRate": err_rate,
        "errorCount": err_count,
        "totalOperations": total_ops,
        "availabilitySamples": avail_samples,
        "throughputSamples": None,
        "cpuSamples": None,
        "memorySamples": None,
    })

    # Stage-transition log lines are stored in results["logs"] by the runner.
    execution_logs = results.get("logs") if isinstance(results.get("logs"), list) else []

    return {
        "id": str(experiment.id),
        "name": experiment.name,
        "description": experiment.description,
        "experimentYaml": experiment.experiment_yaml,
        "pipelineYaml": experiment.pipeline_yaml,
        "target": target_service,
        "targetService": target_service,
        "faultType": fault_type,
        "faultParams": fault_cfg,
        "durationSeconds": fault_duration,
        "thresholds": {
            "expectedDataLoss": validation_cfg.get("expected_data_loss", 0),
            "maxRecoverySeconds": validation_cfg.get("max_recovery_time", 60.0),
        },
        "status": latest_run.status if latest_run else "pending",
        "currentPhase": current_phase,
        "lifecycleStage": current_phase,
        "phases": full_phases,
        "runId": str(latest_run.id) if latest_run else None,
        "runStartedAt": latest_run.started_at.isoformat() if latest_run and latest_run.started_at else None,
        "runFinishedAt": latest_run.finished_at.isoformat() if latest_run and latest_run.finished_at else None,
        "faultInjectedAt": fault_injected_at,
        "faultRevertedAt": fault_reverted_at,
        "recoveredAt": recovered_at,
        "faultPhase": fault_phase,
        "recoveryTimeSeconds": results.get("recovery_time_seconds"),
        "availabilityPercentage": avail_pct,
        "availabilitySamples": avail_samples,
        "serverTime": datetime.now(tz=timezone.utc).isoformat(),
        "pipelineRunId": str(latest_run.pipeline_run_id) if latest_run and latest_run.pipeline_run_id else None,
        "result": results.get("result"),
        "failedCriteria": results.get("failed_criteria", []),
        "results": results_payload,
        "logs": execution_logs,
    }


@router.get(
    "/api/experiments/{experiment_id}/events",
    summary="Get execution events for an experiment run",
)
async def get_experiment_events(
    experiment_id: uuid.UUID,
    after: int = 0,
    db: AsyncSession = Depends(get_db),
) -> list[dict]:
    """Return real execution events emitted by the runner with seq > after."""
    from sqlalchemy import select
    from app.models.experiment_event import ExperimentEvent

    stmt = (
        select(ExperimentEvent)
        .where(ExperimentEvent.experiment_id == experiment_id)
        .where(ExperimentEvent.seq > after)
        .order_by(ExperimentEvent.seq.asc())
    )
    result = await db.execute(stmt)
    events = result.scalars().all()
    return [
        {
            "seq": e.seq,
            "level": e.level,
            "phase": e.phase,
            "message": e.message,
            "ts": e.ts.isoformat(),
        }
        for e in events
    ]


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
    """
    from app.database.session import SyncSessionLocal
    from app.models.experiment import Experiment
    from app.models.experiment_run import ExperimentRun
    from app.reliability.safety import SafetyValidator

    # Check concurrency before accepting
    with SyncSessionLocal() as sync_db:
        safety = SafetyValidator()
        safety.reap_stale_experiments(sync_db)
        try:
            safety.enforce_max_concurrent_experiments(sync_db)
        except TooManyConcurrentExperimentsError as exc:
            raise HTTPException(
                status_code=409,
                detail=str(exc),
            )

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

    initial_phases = [
        {"name": p, "state": "pending", "startedAt": None, "finishedAt": None}
        for p in [
            "validating",
            "preparing",
            "baseline",
            "injecting",
            "monitoring",
            "rolling_back",
            "waiting_for_recovery",
            "validating_data",
            "collecting_metrics",
            "analyzing",
            "cleaning_up",
            "generating_report",
        ]
    ]

    run = ExperimentRun(
        experiment_id=experiment.id,
        status="pending",
        stage="validating",
        params={
            "target_service": experiment_config.target.service,
            "fault_type": experiment_config.fault.type,
            "fault_duration": experiment_config.fault.duration,
            "experiment_config": experiment_config.model_dump(),
        },
        results={"phases": initial_phases},
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
    """
    from app.models.experiment import Experiment
    from app.models.experiment_run import ExperimentRun
    from app.reliability.fault_state import fault_state_registry

    fault_state_registry.clear(str(experiment_id))

    runner = _RUNNING_RUNNERS.get(str(experiment_id))
    if runner is not None:
        runner.cancel()

    from sqlalchemy import select

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
            stage="cancelled",
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
        run.stage = "cancelled"
        run.finished_at = datetime.now(tz=timezone.utc)
        db.add(run)
    await db.commit()

    return {"message": "Cancellation requested.", "experiment_id": str(experiment_id)}


@router.get(
    "/api/reports",
    summary="List all generated experiment reports",
)
async def list_reports(
    response: Response,
    target: str | None = None,
    fault: str | None = None,
    result: str | None = None,
    from_date: str | None = Query(default=None, alias="from"),
    to_date: str | None = Query(default=None, alias="to"),
    sort: str = "created_desc",
    limit: int = Query(50, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
) -> list[dict]:
    """Return filtered, sorted experiment reports with X-Total-Count header."""
    from sqlalchemy import func, select
    from app.models.report import Report

    # Validate query values
    if result is not None and result not in ("", "PASS", "FAIL"):
        raise HTTPException(status_code=422, detail=f"Invalid result filter: '{result}'. Must be PASS or FAIL.")

    valid_sorts = {"created_desc", "created_asc", "recovery_asc", "recovery_desc"}
    if sort not in valid_sorts:
        raise HTTPException(status_code=422, detail=f"Invalid sort parameter: '{sort}'. Must be one of {valid_sorts}.")

    parsed_from = None
    if from_date:
        try:
            parsed_from = datetime.fromisoformat(from_date.replace("Z", "+00:00"))
        except Exception:
            raise HTTPException(status_code=422, detail=f"Invalid 'from' date format: '{from_date}'.")

    parsed_to = None
    if to_date:
        try:
            parsed_to = datetime.fromisoformat(to_date.replace("Z", "+00:00"))
        except Exception:
            raise HTTPException(status_code=422, detail=f"Invalid 'to' date format: '{to_date}'.")

    query = select(Report).where(Report.report_type == "chaos_experiment")

    if target:
        query = query.where(
            (Report.metadata_["target"].as_string() == target)
            | (Report.metadata_["targetService"].as_string() == target)
        )
    if fault:
        query = query.where(
            (Report.metadata_["failure_type"].as_string() == fault)
            | (Report.metadata_["faultType"].as_string() == fault)
        )
    if result:
        query = query.where(Report.metadata_["result"].as_string() == result)
    if parsed_from:
        query = query.where(Report.created_at >= parsed_from)
    if parsed_to:
        query = query.where(Report.created_at <= parsed_to)

    # Count total
    count_query = select(func.count()).select_from(query.subquery())
    total_res = await db.execute(count_query)
    total_count = total_res.scalar() or 0
    response.headers["X-Total-Count"] = str(total_count)

    # Sorting
    if sort == "created_asc":
        query = query.order_by(Report.created_at.asc())
    elif sort == "recovery_asc":
        query = query.order_by(Report.metadata_["recovery_time_seconds"].as_float().asc().nullslast())
    elif sort == "recovery_desc":
        query = query.order_by(Report.metadata_["recovery_time_seconds"].as_float().desc().nullslast())
    else:  # created_desc
        query = query.order_by(Report.created_at.desc())

    query = query.offset(offset).limit(limit)
    res = await db.execute(query)
    reports = res.scalars().all()

    output = []
    for r in reports:
        meta = r.metadata_ or {}
        output.append({
            "id": str(r.id),
            "report_id": str(r.id),
            "experimentId": meta.get("experiment_id") or meta.get("experiment_run_id"),
            "title": r.title,
            "targetService": meta.get("targetService") or meta.get("target"),
            "faultType": meta.get("faultType") or meta.get("failure_type"),
            "result": meta.get("result"),
            "createdAt": r.created_at.isoformat(),
            "recoveryTimeSeconds": meta.get("recoveryTimeSeconds") if meta.get("recoveryTimeSeconds") is not None else meta.get("recovery_time_seconds"),
            "dataLoss": meta.get("dataLoss") if meta.get("dataLoss") is not None else meta.get("data_loss"),
            "throughputBaseline": meta.get("throughputBaseline") if meta.get("throughputBaseline") is not None else meta.get("throughput_baseline"),
            "throughputDuringFault": meta.get("throughputDuringFault"),
            "availabilityPercentage": meta.get("availabilityPercentage") if meta.get("availabilityPercentage") is not None else meta.get("availability_percentage"),
            "errorRate": meta.get("errorRate") if meta.get("errorRate") is not None else meta.get("error_rate"),
            "cpuAvgPercent": meta.get("cpuAvgPercent"),
            "memoryAvgBytes": meta.get("memoryAvgBytes"),
            "summary": meta,
        })

    return output


@router.get(
    "/api/reports/{report_id}",
    summary="Full experiment report detail",
)
async def get_report(
    report_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
) -> dict:
    """Return the full report matching Section 2.1 and 2.2."""
    from app.models.report import Report

    report = await db.get(Report, report_id)
    if report is None:
        raise HTTPException(status_code=404, detail="Report not found.")

    meta = dict(report.metadata_ or {})
    meta["id"] = str(report.id)
    meta["report_id"] = str(report.id)
    meta["title"] = report.title
    meta["createdAt"] = report.created_at.isoformat()
    meta["aiAnalysis"] = meta.get("aiAnalysis")  # null when not computed

    return meta
