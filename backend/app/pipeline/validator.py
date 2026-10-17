"""
app/pipeline/validator.py
--------------------------
Business-rule validation that runs *after* successful Pydantic parsing.

Pydantic catches type violations, unknown keys, and missing required fields.
This module catches higher-level constraints that can't be expressed in a
single-field type annotation — rules that require cross-referencing multiple
parts of the config or checking conditional requirements.

All rules are implemented as private methods of :class:`PipelineBusinessValidator`
so they can be tested in isolation and new rules can be added without
modifying existing ones.

Rules
-----
1. **Streaming requires messaging** — if ``streaming.enabled`` is True,
   ``messaging`` must not be None.
2. **Aggregate column cross-reference** — ``aggregate.group_by`` and
   ``aggregate.field`` must name columns declared in ``schema``.
3. **API source requires url** — if ``source.type == "api"``, ``source.url``
   must be provided.
4. **Absolute storage path** — ``storage.path`` must start with ``/``.
5. **No duplicate schema column names** — every column name must appear
   exactly once.
"""

from __future__ import annotations

import logging
import re

from app.pipeline.config_models import (
    AggregateOperation,
    FilterOperation,
    PipelineConfig,
    RemoveDuplicatesOperation,
)
from app.pipeline.exceptions import ValidationErrorDetail
from app.pipeline.filter_expression import validate_filter_condition

logger = logging.getLogger(__name__)


class PipelineBusinessValidator:
    """
    Validates business rules on a :class:`~app.pipeline.config_models.PipelineConfig`
    that has already passed Pydantic type-checking.

    Usage::

        errors = PipelineBusinessValidator().validate(config)
        if errors:
            # surface them as {"valid": false, "errors": [...]}
            ...

    Returns
    -------
    list[ValidationErrorDetail]
        An empty list when the config satisfies all rules, otherwise one
        ``{"field": str, "message": str}`` dict per violation.
    """

    def validate(self, config: PipelineConfig) -> list[ValidationErrorDetail]:
        """
        Run all business rules against *config* and return any violations.

        Every rule is invoked regardless of whether earlier rules failed,
        so callers receive the full set of problems in a single pass.

        Parameters
        ----------
        config:
            A Pydantic-validated :class:`PipelineConfig` instance.

        Returns
        -------
        list[ValidationErrorDetail]
            Zero or more ``{"field": ..., "message": ...}`` dicts.
        """
        errors: list[ValidationErrorDetail] = []

        errors.extend(self._check_streaming_requires_messaging(config))
        errors.extend(self._check_streaming_unsupported_operations(config))
        errors.extend(self._check_streaming_watermark_column(config))
        errors.extend(self._check_aggregate_column_references(config))
        errors.extend(self._check_api_source_requires_url(config))
        errors.extend(self._check_storage_path_is_absolute(config))
        errors.extend(self._check_no_duplicate_schema_columns(config))
        errors.extend(self._check_filter_conditions(config))
        errors.extend(self._check_batch_kafka_ingestion(config))

        if errors:
            logger.debug(
                "Business validation found %d error(s) for pipeline '%s'",
                len(errors),
                config.pipeline.name,
            )
        return errors

    def _check_filter_conditions(self, config: PipelineConfig) -> list[ValidationErrorDetail]:
        errors: list[ValidationErrorDetail] = []
        for idx, operation in enumerate(config.processing.operations):
            if not isinstance(operation, FilterOperation):
                continue
            try:
                validate_filter_condition(operation.condition)
            except ValueError as exc:
                errors.append({"field": f"processing.operations.{idx}.condition", "message": str(exc)})
        return errors

    # ── Rule implementations ───────────────────────────────────────────────────

    def _check_streaming_requires_messaging(
        self, config: PipelineConfig
    ) -> list[ValidationErrorDetail]:
        """
        Rule 1: A streaming pipeline must declare a message broker.

        Rationale: streaming mode publishes events to a broker; without
        ``messaging`` the pipeline has nowhere to send data.
        """
        if config.streaming.enabled and config.messaging is None:
            return [
                {
                    "field": "messaging",
                    "message": (
                        "streaming.enabled is true but the 'messaging' section is "
                        "missing.  Provide a broker configuration (e.g. Kafka) so "
                        "the pipeline knows where to publish events."
                    ),
                }
            ]
        return []

    def _check_streaming_unsupported_operations(
        self, config: PipelineConfig
    ) -> list[ValidationErrorDetail]:
        """
        Rule: In streaming mode, aggregate operations are not supported yet,
        and remove_duplicates requires a watermark on a timestamp column.
        """
        if not config.streaming.enabled:
            return []

        errors: list[ValidationErrorDetail] = []
        for idx, op in enumerate(config.processing.operations):
            if isinstance(op, AggregateOperation):
                errors.append(
                    {
                        "field": f"processing.operations.{idx}",
                        "message": (
                            "Aggregate operations are not supported in streaming mode yet; "
                            "remove the aggregate operation or set streaming.enabled to false."
                        ),
                    }
                )
            elif isinstance(op, RemoveDuplicatesOperation):
                if config.streaming.watermark is None:
                    errors.append(
                        {
                            "field": f"processing.operations.{idx}",
                            "message": (
                                "remove_duplicates in streaming mode requires streaming.watermark "
                                "to be configured on an event-time timestamp column."
                            ),
                        }
                    )
        return errors

    def _check_streaming_watermark_column(
        self, config: PipelineConfig
    ) -> list[ValidationErrorDetail]:
        """
        Rule: If streaming.watermark is configured, the watermark column must exist
        in the declared schema and must have type 'timestamp'.
        """
        if not config.streaming.enabled or config.streaming.watermark is None:
            return []

        errors: list[ValidationErrorDetail] = []
        schema_map = {col.name: col.type for col in config.pipeline_schema}
        wm_col = config.streaming.watermark.column

        if wm_col not in schema_map:
            errors.append(
                {
                    "field": "streaming.watermark.column",
                    "message": (
                        f"Watermark column '{wm_col}' is not declared in the schema. "
                        f"Declared columns: {sorted(schema_map.keys())}."
                    ),
                }
            )
        elif schema_map[wm_col] != "timestamp":
            errors.append(
                {
                    "field": "streaming.watermark.column",
                    "message": (
                        f"Watermark column '{wm_col}' must have type 'timestamp', "
                        f"but got '{schema_map[wm_col]}'."
                    ),
                }
            )

        return errors

    def _check_aggregate_column_references(
        self, config: PipelineConfig
    ) -> list[ValidationErrorDetail]:
        """
        Rule 2: ``aggregate`` operations must reference declared schema columns.

        Both ``group_by`` and ``field`` are checked.  This prevents silent
        runtime failures where the processing engine tries to aggregate a
        column that doesn't exist.
        """
        declared_columns = {col.name for col in config.pipeline_schema}
        errors: list[ValidationErrorDetail] = []

        for idx, op in enumerate(config.processing.operations):
            if not isinstance(op, AggregateOperation):
                continue

            if op.group_by not in declared_columns:
                errors.append(
                    {
                        "field": f"processing.operations.{idx}.group_by",
                        "message": (
                            f"Column '{op.group_by}' referenced in aggregate.group_by "
                            f"is not declared in the schema.  "
                            f"Declared columns: {sorted(declared_columns)}."
                        ),
                    }
                )

            if op.field not in declared_columns:
                errors.append(
                    {
                        "field": f"processing.operations.{idx}.field",
                        "message": (
                            f"Column '{op.field}' referenced in aggregate.field "
                            f"is not declared in the schema.  "
                            f"Declared columns: {sorted(declared_columns)}."
                        ),
                    }
                )

        return errors

    def _check_api_source_requires_url(
        self, config: PipelineConfig
    ) -> list[ValidationErrorDetail]:
        """
        Rule 3: ``source.type == 'api'`` requires ``source.url``.

        The ``url`` field is optional at the Pydantic level (to avoid making
        it required for non-API sources), but is functionally mandatory when
        the source type is ``api``.
        """
        if config.source.type == "api" and not config.source.url:
            return [
                {
                    "field": "source.url",
                    "message": (
                        "source.type is 'api' but source.url is missing.  "
                        "Provide the full API endpoint URL, e.g. "
                        "'https://api.example.com/v1/data'."
                    ),
                }
            ]
        return []

    def _check_storage_path_is_absolute(
        self, config: PipelineConfig
    ) -> list[ValidationErrorDetail]:
        """
        Rule 4: ``storage.path`` must be an absolute path (starts with ``/``).

        Relative paths are ambiguous — they depend on the working directory of
        whatever process runs the pipeline, which varies across environments.
        Absolute paths are deterministic and environment-agnostic.
        """
        if not config.storage.path.startswith("/"):
            return [
                {
                    "field": "storage.path",
                    "message": (
                        f"Storage path {config.storage.path!r} is required and must "
                        "be an absolute path (starting with '/').  "
                        "Example: '/data/output/sales'."
                    ),
                }
            ]
        return []

    def _check_no_duplicate_schema_columns(
        self, config: PipelineConfig
    ) -> list[ValidationErrorDetail]:
        """
        Rule 5: Every column name in ``schema`` must be unique.

        Duplicate names cause ambiguity in downstream operations such as
        ``select_columns``, ``rename_columns``, and ``aggregate``.
        """
        seen: set[str] = set()
        duplicates: list[str] = []

        for col in config.pipeline_schema:
            if col.name in seen:
                if col.name not in duplicates:
                    duplicates.append(col.name)
            else:
                seen.add(col.name)

        if duplicates:
            dup_list = ", ".join(f"'{d}'" for d in duplicates)
            return [
                {
                    "field": "schema",
                    "message": (
                        f"Duplicate column name(s) detected: {dup_list}.  "
                        "Each column name must appear exactly once in the schema."
                    ),
                }
            ]
        return []

    def _check_batch_kafka_ingestion(
        self, config: PipelineConfig
    ) -> list[ValidationErrorDetail]:
        """
        Validate batch Kafka ingestion settings when enabled.

        - Can only be enabled when streaming.enabled is False.
        - Requires processing_engine.type == 'spark'.
        - Topic name (if specified) must be valid Kafka topic identifier.
        """
        errors: list[ValidationErrorDetail] = []
        bki = config.batch_kafka_ingestion
        if bki is None or not bki.enabled:
            return errors

        if config.streaming.enabled:
            errors.append(
                {
                    "field": "batch_kafka_ingestion",
                    "message": "batch_kafka_ingestion cannot be enabled when streaming.enabled is True.",
                }
            )

        engine_type = (
            config.processing_engine.type
            if config.processing_engine is not None
            else None
        )
        if engine_type != "spark":
            errors.append(
                {
                    "field": "batch_kafka_ingestion",
                    "message": "batch_kafka_ingestion requires processing_engine.type to be 'spark'.",
                }
            )

        if bki.topic is not None:
            topic = bki.topic.strip()
            if not topic:
                errors.append(
                    {
                        "field": "batch_kafka_ingestion.topic",
                        "message": "batch_kafka_ingestion.topic cannot be empty.",
                    }
                )
            elif len(topic) > 249:
                errors.append(
                    {
                        "field": "batch_kafka_ingestion.topic",
                        "message": "batch_kafka_ingestion.topic exceeds maximum length of 249 characters.",
                    }
                )
            elif not re.match(r"^[a-zA-Z0-9._-]+$", topic) or topic in (".", ".."):
                errors.append(
                    {
                        "field": "batch_kafka_ingestion.topic",
                        "message": (
                            f"batch_kafka_ingestion.topic '{topic}' contains invalid characters. "
                            "Only letters, numbers, dots, underscores, and hyphens are allowed."
                        ),
                    }
                )

        return errors

