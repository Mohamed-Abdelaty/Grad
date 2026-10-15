"""
app/api/routes/health.py
------------------------
Health and system-status endpoints.

``GET /health``
    Lightweight liveness probe — no external dependencies.  Returns
    immediately with ``{"status": "ok"}``.

``GET /api/system/status``
    Deeper readiness probe that performs a real ``SELECT 1`` against
    PostgreSQL, a Kafka broker API-versions probe, a Spark master
    REST probe, and an HDFS WebHDFS root probe to confirm all four
    backend dependencies are up.

No business logic lives here; connectivity checks are delegated to the
service functions below.
"""

import logging
import time
from typing import Annotated

from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import Settings, get_settings
from app.database.session import get_db

logger = logging.getLogger(__name__)

router = APIRouter(tags=["health"])


# ── Service helpers ────────────────────────────────────────────────────────────

async def check_database_connectivity(db: AsyncSession) -> dict:
    """
    Execute a trivial ``SELECT 1`` and return a connectivity status dict.

    Returns
    -------
    dict
        ``{"connected": bool, "latency_ms": float | None, "error": str | None}``
    """
    start = time.monotonic()
    try:
        await db.execute(text("SELECT 1"))
        latency_ms = round((time.monotonic() - start) * 1000, 2)
        return {"connected": True, "latency_ms": latency_ms, "error": None}
    except Exception as exc:
        logger.error("Database connectivity check failed: %s", exc)
        return {"connected": False, "latency_ms": None, "error": str(exc)}


def check_kafka_connectivity(timeout: float = 0.5) -> dict:
    """
    Check Kafka broker connectivity via AdminClient list_topics probe.

    Returns
    -------
    dict
        ``{"connected": bool, "broker_count": int, "topic_count": int, "error": str | None}``
    """
    try:
        from app.messaging.kafka_admin import KafkaAdminClient

        admin = KafkaAdminClient()
        return admin.health_check(timeout=timeout)
    except Exception as exc:
        logger.warning("Kafka connectivity check failed: %s", exc)
        return {
            "connected": False,
            "broker_count": 0,
            "topic_count": 0,
            "error": str(exc),
        }


def check_spark_connectivity() -> dict:
    """
    Check Spark master availability via the REST API.

    Uses a thread pool to run the async ``SparkJobSubmitter.health_check()``
    from a synchronous context without blocking the event loop.

    Returns
    -------
    dict
        ``{"connected": bool, "worker_count": int, "status": str, "error": str | None}``
    """
    try:
        import asyncio
        import concurrent.futures
        from app.spark.spark_job_submitter import SparkJobSubmitter

        submitter = SparkJobSubmitter()

        def _run() -> dict:
            return asyncio.run(submitter.health_check())

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(_run)
            return future.result(timeout=6)
    except Exception as exc:
        logger.warning("Spark connectivity check failed: %s", exc)
        return {
            "connected": False,
            "worker_count": 0,
            "status": "UNREACHABLE",
            "error": str(exc),
        }


def check_hdfs_connectivity() -> dict:
    """
    Check HDFS NameNode reachability via the WebHDFS root-status probe.

    Returns
    -------
    dict
        ``{"connected": bool, "namenode_url": str, "num_live_datanodes": int | None,
        "error": str | None}``

    The full JMX capacity stats are intentionally omitted from this summary
    view; use ``GET /api/storage/health`` for the enriched response.
    """
    try:
        from app.storage.hdfs_client import HdfsStorageClient

        client = HdfsStorageClient()
        result = client.health_check()
        # Return a trimmed summary suitable for the system-status dashboard
        return {
            "connected": result["connected"],
            "namenode_url": result["namenode_url"],
            "num_live_datanodes": result.get("num_live_datanodes"),
            "error": result.get("error"),
        }
    except Exception as exc:
        logger.warning("HDFS connectivity check failed: %s", exc)
        return {
            "connected": False,
            "namenode_url": None,
            "num_live_datanodes": None,
            "error": str(exc),
        }


# ── Route handlers ─────────────────────────────────────────────────────────────

@router.get(
    "/health",
    summary="Liveness probe",
    response_description="Service is alive",
)
async def health_check() -> dict:
    """
    Liveness probe — no external dependencies.

    Returns ``{"status": "ok"}`` immediately.  Kubernetes / Docker health-
    checks should hit this endpoint.
    """
    return {"status": "ok"}


@router.get(
    "/api/system/status",
    summary="Readiness probe — database, Kafka, Spark, and HDFS connectivity",
    response_description="System status including live dependency checks",
)
async def system_status(
    db: Annotated[AsyncSession, Depends(get_db)],
    settings: Annotated[Settings, Depends(get_settings)],
) -> dict:
    """
    Readiness probe verifying live PostgreSQL, Kafka, Spark, and HDFS connectivity.

    All ``*.connected`` fields reflect **actual** live connectivity — they are
    never hard-coded.  The top-level ``status`` is ``"ok"`` only when all four
    dependencies are reachable.
    """
    db_status = await check_database_connectivity(db)
    kafka_status = check_kafka_connectivity()
    spark_status = check_spark_connectivity()
    hdfs_status = check_hdfs_connectivity()

    is_healthy = (
        db_status.get("connected", False)
        and kafka_status.get("connected", False)
        and spark_status.get("connected", False)
        and hdfs_status.get("connected", False)
    )

    return {
        "status": "ok" if is_healthy else "degraded",
        "app_version": settings.APP_VERSION,
        "environment": settings.APP_ENV,
        "database": db_status,
        "kafka": kafka_status,
        "spark": spark_status,
        "hdfs": hdfs_status,
    }
