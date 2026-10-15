"""
app/api/routes/dashboard.py
---------------------------
High-performance Dashboard summary endpoint.

Provides high-level system metrics, runs aggregate counts, experiment KPIs,
hourly run trends, recovery/throughput trends, and recent failures.
All aggregated via SQL statements with zero Python row iterations.
"""

from __future__ import annotations

import logging
import math
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import APIRouter, Depends, Query
from sqlalchemy import and_, case, func, or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.functions import coalesce

from app.api.deps import get_current_user
from app.database.session import get_db
from app.models.experiment import Experiment
from app.models.experiment_run import ExperimentRun
from app.models.pipeline import Pipeline
from app.models.pipeline_run import PipelineRun
from app.models.report import Report

logger = logging.getLogger(__name__)

router = APIRouter(tags=["dashboard"])


@router.get(
    "/api/dashboard/summary",
    summary="Dashboard aggregate statistics and trends",
)
async def get_dashboard_summary(
    windowHours: int = Query(24, ge=1, le=168, description="Time window in hours (1..168)"),
    current_user: str = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    """
    Return high-performance dashboard summary with SQL aggregates.
    """
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=windowHours)
    seven_days_ago = now - timedelta(days=7)

    # 1. Pipeline Runs aggregates (window-scoped, active is global)
    runs_stmt = select(
        func.count(case((PipelineRun.status == "running", 1))).label("active"),
        func.count(case((PipelineRun.started_at >= cutoff, 1))).label("total"),
        func.count(
            case((and_(PipelineRun.started_at >= cutoff, PipelineRun.status == "succeeded"), 1))
        ).label("succeeded"),
        func.count(
            case((and_(PipelineRun.started_at >= cutoff, PipelineRun.status == "failed"), 1))
        ).label("failed"),
        func.count(
            case((and_(PipelineRun.started_at >= cutoff, PipelineRun.status == "cancelled"), 1))
        ).label("cancelled"),
    )
    runs_res = (await db.execute(runs_stmt)).one()
    runs_dict = {
        "active": runs_res.active or 0,
        "total": runs_res.total or 0,
        "succeeded": runs_res.succeeded or 0,
        "failed": runs_res.failed or 0,
        "cancelled": runs_res.cancelled or 0,
    }

    # 2. Experiments aggregates (window-scoped, running is global)
    exp_time_col = coalesce(ExperimentRun.started_at, ExperimentRun.created_at)
    exp_stmt = select(
        func.count(case((ExperimentRun.status == "running", 1))).label("running"),
        func.count(case((exp_time_col >= cutoff, 1))).label("total"),
        func.count(
            case((and_(exp_time_col >= cutoff, ExperimentRun.status == "succeeded"), 1))
        ).label("succeeded"),
        func.count(
            case((and_(exp_time_col >= cutoff, ExperimentRun.status == "failed"), 1))
        ).label("failed"),
    )
    exp_res = (await db.execute(exp_stmt)).one()

    # Also check Report table for result = PASS / FAIL within window
    rep_stmt = select(
        func.count(Report.id).label("total"),
    ).where(Report.report_type == "chaos_experiment", Report.created_at >= cutoff)
    rep_count = (await db.execute(rep_stmt)).scalar() or 0

    exp_total = max(exp_res.total or 0, rep_count)
    exp_running = exp_res.running or 0
    exp_passed = exp_res.succeeded or 0
    exp_failed = exp_res.failed or 0

    experiments_dict = {
        "running": exp_running,
        "total": exp_total,
        "passed": exp_passed,
        "failed": exp_failed,
    }

    # 3. Hourly Runs trend (SQL aggregate)
    # Determine bucket size: 1 hour if windowHours <= 48, else 2 hours
    if windowHours <= 48:
        bucket_hours = 1
    elif windowHours <= 96:
        bucket_hours = 2
    else:
        bucket_hours = math.ceil(windowHours / 48)

    # Use dialect-appropriate hour truncation
    bind = db.get_bind()
    is_sqlite = bind and bind.dialect.name == "sqlite"

    if is_sqlite:
        hr_expr = text("strftime('%Y-%m-%d %H:00:00', started_at) AS hr")
    else:
        hr_expr = text("date_trunc('hour', started_at) AS hr")

    hourly_stmt = (
        select(
            hr_expr,
            func.count(PipelineRun.id).label("total"),
            func.count(case((PipelineRun.status == "failed", 1))).label("failed"),
        )
        .where(PipelineRun.started_at >= cutoff)
        .group_by(text("hr"))
        .order_by(text("hr"))
    )
    hourly_rows = (await db.execute(hourly_stmt)).all()

    # Map hourly SQL rows by timestamp
    counts_by_hour: dict[int, dict[str, int]] = {}
    for r in hourly_rows:
        hr_val = r[0]
        if isinstance(hr_val, str):
            try:
                dt_key = datetime.fromisoformat(hr_val.replace("Z", "+00:00"))
            except Exception:
                continue
        elif isinstance(hr_val, datetime):
            dt_key = hr_val
        else:
            continue
        if dt_key.tzinfo is None:
            dt_key = dt_key.replace(tzinfo=timezone.utc)
        epoch_hr = int(dt_key.timestamp() // 3600)
        counts_by_hour[epoch_hr] = {
            "total": r[1] or 0,
            "failed": r[2] or 0,
        }

    # Build zero-filled buckets
    runs_per_hour = []
    # Start at cutoff rounded down to nearest bucket_hours
    start_time = cutoff.replace(minute=0, second=0, microsecond=0)
    current_bucket_time = start_time
    while current_bucket_time <= now:
        bucket_total = 0
        bucket_failed = 0
        for h in range(bucket_hours):
            h_dt = current_bucket_time + timedelta(hours=h)
            epoch_h = int(h_dt.timestamp() // 3600)
            if epoch_h in counts_by_hour:
                bucket_total += counts_by_hour[epoch_h]["total"]
                bucket_failed += counts_by_hour[epoch_h]["failed"]

        runs_per_hour.append({
            "hour": current_bucket_time.isoformat(),
            "total": bucket_total,
            "failed": bucket_failed,
        })
        current_bucket_time += timedelta(hours=bucket_hours)

    # 4. KPIs over the LAST 10 COMPLETED EXPERIMENTS (not window)
    # Query last 10 completed reports
    rep_stmt = (
        select(Report)
        .where(Report.report_type == "chaos_experiment")
        .order_by(Report.created_at.desc())
        .limit(10)
    )
    reports = (await db.execute(rep_stmt)).scalars().all()

    # Also fallback/supplement with last 10 completed ExperimentRuns if fewer than 10 reports
    exp_run_stmt = (
        select(ExperimentRun)
        .where(ExperimentRun.status.in_(["succeeded", "failed"]))
        .where(ExperimentRun.results.isnot(None))
        .order_by(ExperimentRun.created_at.desc())
        .limit(10)
    )
    exp_runs = (await db.execute(exp_run_stmt)).scalars().all()

    # Extract metrics from reports and/or experiment runs
    completed_items: list[dict[str, Any]] = []
    seen_exp_ids = set()

    for rep in reports:
        meta = rep.metadata_ or {}
        exp_id = meta.get("experiment_id") or meta.get("experimentId") or str(rep.id)
        seen_exp_ids.add(exp_id)
        completed_items.append({
            "experimentId": exp_id,
            "createdAt": rep.created_at.isoformat() if rep.created_at else now.isoformat(),
            "created_at_dt": rep.created_at or now,
            "recoverySeconds": meta.get("recoveryTimeSeconds") or meta.get("recovery_time_seconds"),
            "dataLoss": meta.get("dataLoss") if meta.get("dataLoss") is not None else meta.get("data_loss"),
            "throughput": meta.get("average_throughput") or meta.get("averageThroughput"),
            "baseline": meta.get("throughputBaseline"),
            "during": meta.get("throughputDuringFault") or meta.get("throughputAfter") or meta.get("average_throughput"),
            "availability": meta.get("availabilityPercentage") if meta.get("availabilityPercentage") is not None else meta.get("availability_percentage"),
            "result": meta.get("result", "UNKNOWN"),
        })

    for er in exp_runs:
        exp_id = str(er.experiment_id)
        if exp_id in seen_exp_ids:
            continue
        res = er.results or {}
        completed_items.append({
            "experimentId": exp_id,
            "createdAt": er.created_at.isoformat() if er.created_at else now.isoformat(),
            "created_at_dt": er.created_at or now,
            "recoverySeconds": res.get("recovery_time_seconds"),
            "dataLoss": res.get("data_loss"),
            "throughput": res.get("throughput_records_per_sec"),
            "baseline": res.get("throughput_baseline"),
            "during": res.get("throughput_records_per_sec"),
            "availability": res.get("availability_percentage"),
            "result": res.get("result", "PASS" if er.status == "succeeded" else "FAIL"),
        })

    # Sort descending by date and take last 10
    completed_items.sort(key=lambda x: x["created_at_dt"], reverse=True)
    last_10_completed = completed_items[:10]

    if not last_10_completed:
        kpis = {
            "avgRecoverySeconds": None,
            "totalDataLoss": None,
            "avgThroughput": None,
            "avgAvailabilityPercent": None,
        }
        recovery_trend: list[dict[str, Any]] = []
        throughput_trend: list[dict[str, Any]] = []
    else:
        rec_list = [x["recoverySeconds"] for x in last_10_completed if x["recoverySeconds"] is not None]
        avg_recovery = round(sum(rec_list) / len(rec_list), 2) if rec_list else None

        loss_list = [x["dataLoss"] for x in last_10_completed if x["dataLoss"] is not None]
        total_loss = sum(loss_list) if loss_list else None

        tp_list = [x["throughput"] for x in last_10_completed if x["throughput"] is not None]
        avg_tp = round(sum(tp_list) / len(tp_list), 2) if tp_list else None

        avail_list = [x["availability"] for x in last_10_completed if x["availability"] is not None]
        avg_avail = round(sum(avail_list) / len(avail_list), 2) if avail_list else None

        kpis = {
            "avgRecoverySeconds": avg_recovery,
            "totalDataLoss": total_loss,
            "avgThroughput": avg_tp,
            "avgAvailabilityPercent": avg_avail,
        }

        # Trend items: last 10 experiments with data, ascending order
        rec_items = [
            {
                "experimentId": x["experimentId"],
                "createdAt": x["createdAt"],
                "created_at_dt": x["created_at_dt"],
                "recoverySeconds": float(x["recoverySeconds"]),
                "result": x["result"],
            }
            for x in completed_items
            if x["recoverySeconds"] is not None
        ]
        rec_items.sort(key=lambda x: x["created_at_dt"], reverse=True)
        rec_items = rec_items[:10]
        rec_items.sort(key=lambda x: x["created_at_dt"])  # ascending
        recovery_trend = [
            {"experimentId": x["experimentId"], "createdAt": x["createdAt"], "recoverySeconds": x["recoverySeconds"], "result": x["result"]}
            for x in rec_items
        ]

        tp_items = [
            {
                "experimentId": x["experimentId"],
                "createdAt": x["createdAt"],
                "created_at_dt": x["created_at_dt"],
                "baseline": x["baseline"],
                "during": x["during"],
            }
            for x in completed_items
            if x["baseline"] is not None or x["during"] is not None
        ]
        tp_items.sort(key=lambda x: x["created_at_dt"], reverse=True)
        tp_items = tp_items[:10]
        tp_items.sort(key=lambda x: x["created_at_dt"])  # ascending
        throughput_trend = [
            {"experimentId": x["experimentId"], "createdAt": x["createdAt"], "baseline": x["baseline"], "during": x["during"]}
            for x in tp_items
        ]

    # 5. Recent Failures (last 7 days, max 8, newest first)
    failures: list[dict[str, Any]] = []

    # Failed pipeline runs
    failed_runs_stmt = (
        select(
            PipelineRun.id,
            PipelineRun.started_at,
            PipelineRun.finished_at,
            PipelineRun.created_at,
            PipelineRun.error_message,
            Pipeline.name.label("pipeline_name"),
        )
        .join(Pipeline, Pipeline.id == PipelineRun.pipeline_id)
        .where(PipelineRun.status == "failed")
        .where(coalesce(PipelineRun.finished_at, PipelineRun.started_at, PipelineRun.created_at) >= seven_days_ago)
        .order_by(coalesce(PipelineRun.finished_at, PipelineRun.started_at, PipelineRun.created_at).desc())
        .limit(8)
    )
    failed_runs = (await db.execute(failed_runs_stmt)).all()
    for fr in failed_runs:
        at_dt = fr.finished_at or fr.started_at or fr.created_at or now
        failures.append({
            "type": "run",
            "id": str(fr.id),
            "title": fr.pipeline_name or f"Pipeline Run {str(fr.id)[:8]}",
            "at": at_dt.isoformat(),
            "at_dt": at_dt,
            "message": fr.error_message or "Run failed",
        })

    # Failed experiment reports
    failed_rep_stmt = (
        select(Report)
        .where(Report.report_type == "chaos_experiment")
        .where(Report.created_at >= seven_days_ago)
        .order_by(Report.created_at.desc())
        .limit(8)
    )
    failed_reports = (await db.execute(failed_rep_stmt)).scalars().all()
    for frep in failed_reports:
        meta = frep.metadata_ or {}
        if meta.get("result") == "FAIL":
            failed_criteria = meta.get("failedCriteria") or meta.get("failed_criteria") or []
            msg = failed_criteria[0] if failed_criteria else "Experiment failed"
            at_dt = frep.created_at or now
            failures.append({
                "type": "experiment",
                "id": str(frep.id),
                "title": frep.title or f"Experiment {str(frep.id)[:8]}",
                "at": at_dt.isoformat(),
                "at_dt": at_dt,
                "message": msg,
            })

    # Sort all failures newest first and limit to 8
    failures.sort(key=lambda x: x["at_dt"], reverse=True)
    recent_failures = [
        {"type": f["type"], "id": f["id"], "title": f["title"], "at": f["at"], "message": f["message"]}
        for f in failures[:8]
    ]

    return {
        "windowHours": windowHours,
        "bucketHours": bucket_hours,
        "generatedAt": now.isoformat(),
        "runs": runs_dict,
        "experiments": experiments_dict,
        "kpis": kpis,
        "runsPerHour": runs_per_hour,
        "recoveryTrend": recovery_trend,
        "throughputTrend": throughput_trend,
        "recentFailures": recent_failures,
    }
