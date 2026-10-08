"""
app/services/pipeline_execution_service.py
--------------------------------------------
Orchestrates the full pipeline execution lifecycle:
  Phase 2 validation → plan generation → execution → result summary.

The function :func:`run_pipeline_from_yaml` is the single entry point called
by API routes.  It is designed to **never raise** — all failure modes are
captured and returned as a structured dict so the HTTP layer can always
return a meaningful JSON body.

Failure modes and their return shapes
--------------------------------------
1. **YAML parse / schema validation failure** (Phase 2 ConfigParseError /
   ConfigValidationError):
   ``{"status": "invalid", "valid": False, "errors": [...], "run_id": None,
     "metrics": {}, "logs": []}``

2. **Business rule validation failure** (Phase 2 PipelineBusinessValidator):
   Same shape as (1).

3. **Plugin-not-found at plan generation** (PluginNotFoundError):
   ``{"status": "failed", "run_id": None, "metrics": {},
     "logs": [{"level": "ERROR", ...}]}``

4. **Step execution failure** (PipelineExecutionError — wraps any plugin exc):
   ``{"status": "failed", "run_id": "<uuid>", "metrics": {<partial>},
     "logs": [<partial + error entry>]}``

5. **Full success**:
   ``{"status": "success", "run_id": "<uuid>",
     "metrics": {<all stages>}, "logs": [<all entries>]}``
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

# Ensure all plugins are registered before the generator runs.
# This import triggers the module-level @registry.register_* decorators in
# every plugin file — safe to call multiple times (Python caches modules).
import app.plugins  # noqa: F401

from app.pipeline.exceptions import ConfigParseError, ConfigValidationError
from app.pipeline.executor import PipelineExecutionError, PipelineExecutor
from app.pipeline.generator import PipelineGenerator
from app.pipeline.parser import PipelineConfigParser
from app.pipeline.validator import PipelineBusinessValidator
from app.plugins.registry import PluginNotFoundError

logger = logging.getLogger(__name__)

# Module-level singletons — all are stateless.
_parser = PipelineConfigParser()
_business_validator = PipelineBusinessValidator()
_generator = PipelineGenerator()
_executor = PipelineExecutor()


def run_pipeline_from_yaml(
    yaml_content: str,
    tracker_run_id: str | None = None,
) -> dict:
    """
    Parse, validate, generate, and execute a pipeline from a YAML string.

    This function is the single integration point between the HTTP layer and
    the pipeline engine.  It never raises — every failure mode is returned as
    a structured dict.

    Parameters
    ----------
    yaml_content : str
        Raw YAML string representing a pipeline configuration document.
    tracker_run_id : str | None
        When provided, the executor emits progress events to the global
        :data:`~app.pipeline.progress.tracker` under this key.  Should match
        the ``pipeline_run_id`` stored in the database for the current run.
        Pass ``None`` for the synchronous (``wait=True``) path.

    Returns
    -------
    dict
        Always contains at least ``"status"`` and ``"logs"``.  Additional
        keys depend on the outcome — see the module docstring for the full
        shape of each failure mode.
    """
    # ── Phase 1: parse + schema validation ────────────────────────────────────
    try:
        config = _parser.parse_string(yaml_content)
    except ConfigParseError as exc:
        logger.info("YAML parse error during pipeline run: %s", exc.message)
        return _invalid(errors=[{"field": "yaml", "message": f"YAML parse error: {exc.message}"}])
    except ConfigValidationError as exc:
        logger.info("Schema validation failed with %d error(s).", len(exc.errors))
        return _invalid(errors=exc.errors)

    # ── Phase 2: business rules ────────────────────────────────────────────────
    business_errors = _business_validator.validate(config)
    if business_errors:
        logger.info("Business validation failed with %d error(s).", len(business_errors))
        return _invalid(errors=business_errors)

    # ── Phase 3: generate execution plan ──────────────────────────────────────
    try:
        plan = _generator.generate(config)
    except PluginNotFoundError as exc:
        logger.error("Plugin resolution failed during plan generation: %s", exc)
        return {
            "status": "failed",
            "run_id": None,
            "metrics": {},
            "logs": [
                {
                    "timestamp": datetime.now(tz=timezone.utc).isoformat(),
                    "level": "ERROR",
                    "step": "generator",
                    "message": str(exc),
                }
            ],
        }

    # ── Phase 4: execute ───────────────────────────────────────────────────────
    try:
        context = _executor.execute(plan, tracker_run_id=tracker_run_id)
        return context.to_summary()
    except PipelineExecutionError as exc:
        # The partial context already contains the error log entry and
        # metrics for all completed steps.
        logger.error(
            "Pipeline '%s' run failed at step '%s': %s",
            plan.pipeline_name,
            exc.step_name,
            exc.cause,
        )
        return exc.context.to_summary()


# ── Private helpers ────────────────────────────────────────────────────────────


def _invalid(errors: list[dict]) -> dict:
    """Return the standard 'invalid config' response shape."""
    return {
        "status": "invalid",
        "valid": False,
        "errors": errors,
        "run_id": None,
        "metrics": {},
        "logs": [],
    }
