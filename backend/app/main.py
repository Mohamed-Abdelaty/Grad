"""
app/main.py
-----------
FastAPI application factory.

This module creates and configures the FastAPI application instance,
registers all routers, configures CORS, and sets up structured JSON
logging.  It is the entrypoint consumed by uvicorn.

Phase 2 additions
-----------------
* Registers the ``/api/pipelines`` router (pipeline config validation).
"""

import json
import logging
import logging.config
from contextlib import asynccontextmanager
from contextvars import ContextVar
from typing import AsyncGenerator
from uuid import uuid4

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

from app.api.routes.health import router as health_router
from app.api.routes.messaging import router as messaging_router
from app.api.routes.pipelines import router as pipelines_router
from app.api.routes.spark import router as spark_router
from app.api.routes.storage import router as storage_router
from app.api.routes.experiments import router as experiments_router
from app.api.routes.ai import router as ai_router
from app.api.routes.metrics import router as metrics_router
from app.core.config import get_settings

# ── Structured JSON logging setup ─────────────────────────────────────────────

request_id_context: ContextVar[str | None] = ContextVar("request_id", default=None)


class RequestContextMiddleware(BaseHTTPMiddleware):
    """Attach a correlation ID to each request, response, and JSON log line."""

    async def dispatch(self, request: Request, call_next):
        request_id = request.headers.get("X-Request-ID") or str(uuid4())
        token = request_id_context.set(request_id)
        try:
            response = await call_next(request)
            response.headers["X-Request-ID"] = request_id
            return response
        finally:
            request_id_context.reset(token)


class _JsonFormatter(logging.Formatter):
    """
    Emit every log record as a single-line JSON object.

    Fields: timestamp, level, logger, message, and any ``extra`` kwargs
    passed to the logger call.
    """

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        request_id = request_id_context.get()
        if request_id:
            payload["request_id"] = request_id
        # Merge any extra fields attached to the record
        for key, value in record.__dict__.items():
            if key not in {
                "name", "msg", "args", "levelname", "levelno", "pathname",
                "filename", "module", "exc_info", "exc_text", "stack_info",
                "lineno", "funcName", "created", "msecs", "relativeCreated",
                "thread", "threadName", "processName", "process", "message",
                "taskName",
            }:
                payload[key] = value
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload)


def _configure_logging(log_level: str) -> None:
    """Apply the JSON formatter to the root logger."""
    handler = logging.StreamHandler()
    handler.setFormatter(_JsonFormatter())
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(log_level.upper())


# ── Application lifespan ───────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """
    Manage application startup and shutdown events.

    Startup:
        - Configure JSON logging
        - Log "Application started" with environment metadata

    Shutdown:
        - Log "Application shutting down"
    """
    import asyncio
    from app.pipeline.progress import tracker

    settings = get_settings()
    _configure_logging(settings.LOG_LEVEL)

    logger = logging.getLogger(__name__)
    logger.info(
        "Application started",
        extra={
            "app_name": settings.APP_NAME,
            "version": settings.APP_VERSION,
            "environment": settings.APP_ENV,
        },
    )

    # Bind the progress tracker to the main event loop so the synchronous
    # executor thread can safely notify async SSE subscribers.
    tracker.set_loop(asyncio.get_running_loop())

    yield  # ← the application runs here

    logger.info("Application shutting down")



# ── Application factory ────────────────────────────────────────────────────────

def create_app() -> FastAPI:
    """
    Construct and configure the FastAPI application.

    Returns
    -------
    FastAPI
        A fully configured application instance ready to be served by uvicorn.
    """
    settings = get_settings()

    app = FastAPI(
        title=settings.APP_NAME,
        version=settings.APP_VERSION,
        description=(
            "Resilient Big Data Pipeline Platform — Phase 1 Foundation API.\n\n"
            "Provides health checks, system status, and will expose pipeline "
            "management, experiment tracking, and fault-injection endpoints in "
            "subsequent phases."
        ),
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
        lifespan=lifespan,
    )

    # ── CORS ───────────────────────────────────────────────────────────────────
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.CORS_ORIGINS,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.add_middleware(RequestContextMiddleware)

    # ── Routers ────────────────────────────────────────────────────────────────
    app.include_router(health_router)
    app.include_router(pipelines_router)
    app.include_router(messaging_router)
    app.include_router(spark_router)
    app.include_router(storage_router)   # Phase 6 — HDFS storage health
    app.include_router(experiments_router)  # Phase 7 — ChaosLab reliability

    app.include_router(ai_router)
    app.include_router(metrics_router)

    from app.api.routes.auth import router as auth_router
    from app.api.routes.pipeline_runs import router as pipeline_runs_router
    app.include_router(auth_router)
    app.include_router(pipeline_runs_router)

    # ── Prometheus Metrics ─────────────────────────────────────────────────────
    from prometheus_client import make_asgi_app
    app.mount("/metrics", make_asgi_app())

    return app


# ── Module-level app instance (consumed by uvicorn) ───────────────────────────
app = create_app()
