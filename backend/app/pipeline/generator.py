"""
app/pipeline/generator.py
--------------------------
Translates a validated :class:`~app.pipeline.config_models.PipelineConfig`
into an :class:`ExecutionPlan` — an ordered list of concrete plugin instances
ready to be executed by :class:`~app.pipeline.executor.PipelineExecutor`.

Role in the architecture
-------------------------
The generator sits between *configuration* and *execution*:

    YAML text
        └─ PipelineConfigParser  →  PipelineConfig  (Phase 2)
                └─ PipelineGenerator  →  ExecutionPlan  (this file)
                        └─ PipelineExecutor  →  PipelineExecutionContext

By resolving plugins at *generation time* (not execution time), the executor
can focus purely on data flow and metric collection without any plugin-lookup
logic.  Any misconfigured plugin type (e.g. requesting ``"minio"`` storage
before the minio plugin is implemented) is detected early, before any data
is read from the source.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from app.pipeline.config_models import PipelineConfig
from app.plugins.base import ProcessorPlugin, SourcePlugin, StoragePlugin
from app.plugins.registry import PluginNotFoundError, registry

logger = logging.getLogger(__name__)


# ── Data structures ────────────────────────────────────────────────────────────


@dataclass
class ExecutionStep:
    """
    A single resolved step in an execution plan.

    Attributes
    ----------
    name : str
        Human-readable step identifier used for logging and metrics, e.g.
        ``"source:csv"``, ``"processor:filter:2"``, ``"storage:local"``.
    step_type : str
        One of ``"source"``, ``"processor"``, ``"storage"``.
    plugin_type : str
        The ``plugin_type`` string of the concrete plugin (e.g. ``"csv"``).
    plugin : Any
        Instantiated plugin object ready to call.
    config : dict
        Configuration dict derived from the validated ``PipelineConfig``
        and passed verbatim to the plugin's action method.
    """

    name: str
    step_type: str
    plugin_type: str
    plugin: Any
    config: dict


@dataclass
class ExecutionPlan:
    """
    An ordered sequence of :class:`ExecutionStep` objects derived from a
    :class:`~app.pipeline.config_models.PipelineConfig`.

    Steps are ordered: source → processors (in config order) → storage.

    Attributes
    ----------
    pipeline_name : str
        The pipeline's slug name (from ``PipelineConfig.pipeline.name``).
    pipeline_version : str
        The pipeline's version string.
    steps : list[ExecutionStep]
        Ordered list of steps to execute.
    """

    pipeline_name: str
    pipeline_version: str
    steps: list[ExecutionStep] = field(default_factory=list)
    messaging_config: dict | None = None
    processing_engine_type: str | None = None  # e.g. "spark"; None → pandas path
    streaming_config: dict | None = None
    streaming_enabled: bool = False
    schema_def: list[dict] = field(default_factory=list)

    @property
    def source_step(self) -> ExecutionStep:
        """Return the source step (always index 0)."""
        return self.steps[0]

    @property
    def processor_steps(self) -> list[ExecutionStep]:
        """Return all processor steps (everything between source and storage)."""
        return [s for s in self.steps if s.step_type == "processor"]

    @property
    def storage_step(self) -> ExecutionStep:
        """Return the storage step (always the last step)."""
        return self.steps[-1]


# ── Generator ──────────────────────────────────────────────────────────────────


class PipelineGenerator:
    """
    Converts a validated :class:`~app.pipeline.config_models.PipelineConfig`
    into a concrete :class:`ExecutionPlan`.

    Plugin lookup happens at generation time so that any misconfigured or
    unregistered plugin type is detected before execution begins.  This gives
    operators a fast-fail signal without wasting time reading potentially large
    source files.

    Usage::

        plan = PipelineGenerator().generate(config)
        context = PipelineExecutor().execute(plan)
    """

    def generate(self, config: PipelineConfig) -> ExecutionPlan:
        """
        Build an :class:`ExecutionPlan` from *config*.

        Parameters
        ----------
        config : PipelineConfig
            A fully-validated pipeline configuration (output of
            :class:`~app.pipeline.parser.PipelineConfigParser`).

        Returns
        -------
        ExecutionPlan
            Ordered list of resolved execution steps.

        Raises
        ------
        PluginNotFoundError
            A plugin type referenced in the config is not registered in the
            registry.  The error message lists available alternatives.
        """
        logger.info(
            "Generating execution plan for pipeline '%s' v%s",
            config.pipeline.name,
            config.pipeline.version,
        )

        steps: list[ExecutionStep] = []

        # ── Step 1: source ─────────────────────────────────────────────────────
        source_plugin: SourcePlugin = self._resolve_source(config)
        source_config = config.source.model_dump(exclude_none=True)
        steps.append(
            ExecutionStep(
                name=f"source:{config.source.type}",
                step_type="source",
                plugin_type=config.source.type,
                plugin=source_plugin,
                config=source_config,
            )
        )

        # ── Step 2: processors (in config order) ───────────────────────────────
        for idx, operation in enumerate(config.processing.operations):
            op_dict = operation.model_dump()
            op_type: str = op_dict["type"]
            processor_plugin: ProcessorPlugin = self._resolve_processor(
                op_type, idx
            )
            steps.append(
                ExecutionStep(
                    name=f"processor:{op_type}:{idx}",
                    step_type="processor",
                    plugin_type=op_type,
                    plugin=processor_plugin,
                    config=op_dict,
                )
            )

        # ── Step 3: storage ────────────────────────────────────────────────────
        storage_plugin: StoragePlugin = self._resolve_storage(config)
        storage_config = {
            "type": config.storage.type,
            "path": config.storage.path,
            "mode": getattr(config.storage, "mode", "overwrite"),
        }
        steps.append(
            ExecutionStep(
                name=f"storage:{config.storage.type}",
                step_type="storage",
                plugin_type=config.storage.type,
                plugin=storage_plugin,
                config=storage_config,
            )
        )

        messaging_dict = config.messaging.model_dump() if config.messaging is not None else None
        engine_type = (
            config.processing_engine.type
            if config.processing_engine is not None
            else None
        )
        streaming_dict = (
            config.streaming.model_dump() if config.streaming is not None else None
        )
        streaming_enabled = (
            bool(config.streaming.enabled) if config.streaming is not None else False
        )
        schema_list = [col.model_dump() for col in config.pipeline_schema]

        plan = ExecutionPlan(
            pipeline_name=config.pipeline.name,
            pipeline_version=config.pipeline.version,
            steps=steps,
            messaging_config=messaging_dict,
            processing_engine_type=engine_type,
            streaming_config=streaming_dict,
            streaming_enabled=streaming_enabled,
            schema_def=schema_list,
        )

        logger.info(
            "Plan generated: %d steps (%d processors)",
            len(steps),
            len(plan.processor_steps),
        )
        return plan

    # ── Private resolver helpers ───────────────────────────────────────────────

    def _resolve_source(self, config: PipelineConfig) -> SourcePlugin:
        """Look up the source plugin or raise with a clear message."""
        source_type = config.source.type
        try:
            return registry.get_source(source_type)
        except PluginNotFoundError:
            raise PluginNotFoundError(
                plugin_type=source_type,
                available=registry.list_sources(),
                kind="source",
            ) from None

    def _resolve_processor(self, op_type: str, idx: int) -> ProcessorPlugin:
        """Look up a processor plugin or raise with a clear message."""
        try:
            return registry.get_processor(op_type)
        except PluginNotFoundError:
            raise PluginNotFoundError(
                plugin_type=op_type,
                available=registry.list_processors(),
                kind="processor",
            ) from None

    def _resolve_storage(self, config: PipelineConfig) -> StoragePlugin:
        """Look up the storage plugin or raise with a clear message."""
        storage_type = config.storage.type
        try:
            return registry.get_storage(storage_type)
        except PluginNotFoundError:
            raise PluginNotFoundError(
                plugin_type=storage_type,
                available=registry.list_storage(),
                kind="storage",
            ) from None
