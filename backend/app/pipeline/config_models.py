"""
app/pipeline/config_models.py
------------------------------
Strictly-typed Pydantic v2 models representing the full pipeline configuration
schema.  Every model uses ``extra="forbid"`` so unknown YAML keys are rejected
immediately instead of being silently swallowed.

Model hierarchy
---------------
PipelineConfig
├── pipeline          : PipelineMeta
├── source            : SourceConfig
├── schema            : list[ColumnSchema]           (alias "schema")
├── processing        : ProcessingConfig
│   └── operations    : list[ProcessingOperation]    (discriminated union)
│       ├── RemoveNullsOperation
│       ├── RemoveDuplicatesOperation
│       ├── FilterOperation
│       ├── SelectColumnsOperation
│       ├── RenameColumnsOperation
│       ├── TypeConversionOperation
│       └── AggregateOperation
├── streaming         : StreamingConfig
├── messaging         : MessagingConfig | None
├── processing_engine : ProcessingEngineConfig
├── storage           : StorageConfig
└── monitoring        : MonitoringConfig | None
"""

from __future__ import annotations

import re
from typing import Annotated, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator


# ── Reusable type aliases ──────────────────────────────────────────────────────

ColumnType = Literal["string", "integer", "double", "boolean", "timestamp"]
"""Exhaustive set of supported column data types."""


# ── Column Schema ──────────────────────────────────────────────────────────────


class ColumnSchema(BaseModel):
    """
    Defines a single typed column in the pipeline's data schema.

    Used both for input schema declaration and for cross-referencing in the
    business validator (e.g. checking that ``aggregate.group_by`` names a real
    column).
    """

    model_config = ConfigDict(extra="forbid")

    name: str = Field(..., description="Column identifier — must be unique within the schema.")
    type: ColumnType = Field(..., description="Data type for this column.")


# ── Processing Operations (discriminated union) ────────────────────────────────


class RemoveNullsOperation(BaseModel):
    """Drops any row that contains at least one null value."""

    model_config = ConfigDict(extra="forbid")
    type: Literal["remove_nulls"]


class RemoveDuplicatesOperation(BaseModel):
    """Removes rows that are exact duplicates of a previously seen row."""

    model_config = ConfigDict(extra="forbid")
    type: Literal["remove_duplicates"]


class FilterOperation(BaseModel):
    """Retains only rows where *condition* evaluates to true."""

    model_config = ConfigDict(extra="forbid")
    type: Literal["filter"]
    condition: str = Field(
        ...,
        description="Boolean expression evaluated per row, e.g. \"amount > 0\".",
    )


class SelectColumnsOperation(BaseModel):
    """Drops all columns not listed in *columns*."""

    model_config = ConfigDict(extra="forbid")
    type: Literal["select_columns"]
    columns: list[str] = Field(
        ...,
        min_length=1,
        description="Ordered list of column names to retain.",
    )


class RenameColumnsOperation(BaseModel):
    """Renames columns using a {old_name: new_name} mapping dict."""

    model_config = ConfigDict(extra="forbid")
    type: Literal["rename_columns"]
    mapping: dict[str, str] = Field(
        ...,
        description="Mapping of existing column name → desired column name.",
    )


class TypeConversionOperation(BaseModel):
    """Casts *column* to *to_type*, failing fast on unconvertible values."""

    model_config = ConfigDict(extra="forbid")
    type: Literal["type_conversion"]
    column: str = Field(..., description="Column to cast.")
    to_type: ColumnType = Field(..., description="Target data type.")


class AggregateOperation(BaseModel):
    """Groups data by *group_by*, then applies *operation* to *field*."""

    model_config = ConfigDict(extra="forbid")
    type: Literal["aggregate"]
    group_by: str = Field(..., description="Column name to group rows by.")
    operation: Literal["sum", "avg", "count", "min", "max"] = Field(
        ..., description="Aggregation function to apply."
    )
    field: str = Field(..., description="Column name to aggregate over.")


# ── Discriminated union — one type per operation variant ──────────────────────

ProcessingOperation = Annotated[
    Union[
        RemoveNullsOperation,
        RemoveDuplicatesOperation,
        FilterOperation,
        SelectColumnsOperation,
        RenameColumnsOperation,
        TypeConversionOperation,
        AggregateOperation,
    ],
    Field(discriminator="type"),
]
"""
Tagged union resolved by the ``type`` discriminator field.

Pydantic v2 uses the literal value of ``type`` to pick the correct model,
which gives precise, actionable validation errors (e.g. "unknown type
'transform_magic'" rather than a generic union failure).
"""


# ── Pipeline sections ──────────────────────────────────────────────────────────


class PipelineMeta(BaseModel):
    """
    Top-level identity block for the pipeline.

    *name* is validated against a slug pattern (lowercase, hyphens, digits).
    *version* is validated against a simplified semver pattern (``N.N`` or
    ``N.N.N``).  String values like ``"latest"`` or ``"v2"`` are rejected.
    """

    model_config = ConfigDict(extra="forbid")

    name: str = Field(
        ...,
        description="Pipeline slug: lowercase letters, digits, and hyphens only.",
    )
    version: str = Field(
        ...,
        description="Semantic version like '1.0' or '2.3.1'.",
    )

    @field_validator("name")
    @classmethod
    def validate_name_slug(cls, v: str) -> str:
        """Reject names that contain spaces, uppercase letters, or special characters."""
        # Allow single-char names too (e.g. "x") — the alternation handles that.
        pattern = r"^[a-z0-9]([a-z0-9-]*[a-z0-9])?$"
        if not re.match(pattern, v):
            raise ValueError(
                f"Pipeline name {v!r} must consist of lowercase letters, digits, "
                "and hyphens, and must start and end with a letter or digit. "
                "Spaces and uppercase letters are not allowed."
            )
        return v

    @field_validator("version")
    @classmethod
    def validate_semver(cls, v: str) -> str:
        """Accept 'N.N' or 'N.N.N' — reject 'latest', 'v1', etc."""
        if not re.match(r"^\d+\.\d+(\.\d+)?$", v):
            raise ValueError(
                f"Version {v!r} is not a valid semantic version. "
                "Expected a format like '1.0' or '2.3.1' (digits and dots only)."
            )
        return v


class SourceConfig(BaseModel):
    """
    Describes where the pipeline should read its input data from.

    For ``type: csv`` and ``type: json``, *path* is the file system location.
    For ``type: api``, *url* is required (enforced by the business validator,
    not at the Pydantic level, because the requirement is conditional).
    *method* and *headers* are optional API-only fields.
    """

    model_config = ConfigDict(extra="forbid")

    type: Literal["csv", "json", "api", "kafka"] = Field(..., description="Source type.")
    path: Optional[str] = Field(None, description="File-system path (csv/json sources).")
    url: Optional[str] = Field(None, description="API endpoint URL (api source).")
    method: Optional[str] = Field(None, description="HTTP method, e.g. GET or POST.")
    headers: Optional[dict[str, str]] = Field(
        None, description="HTTP headers to include in API requests."
    )
    topic: Optional[str] = Field(None, description="Kafka topic (kafka source).")
    group_id: Optional[str] = Field(None, description="Consumer group ID (kafka source).")
    max_messages: Optional[int] = Field(None, description="Max messages to consume.")
    timeout: Optional[float] = Field(None, description="Consume timeout.")


class ProcessingConfig(BaseModel):
    """Ordered list of transformation operations applied to the data."""

    model_config = ConfigDict(extra="forbid")

    operations: list[ProcessingOperation] = Field(
        ...,
        min_length=1,
        description="At least one processing operation must be defined.",
    )


class StreamingConfig(BaseModel):
    """Controls whether the pipeline runs in streaming or batch mode."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = Field(..., description="True for streaming mode, False for batch.")


class MessagingConfig(BaseModel):
    """
    Message broker settings.

    Only required when ``streaming.enabled`` is True.  The business validator
    enforces this cross-section constraint.
    """

    model_config = ConfigDict(extra="forbid")

    broker: Literal["kafka"] = Field(..., description="Message broker technology.")
    topic: str = Field(..., description="Broker topic name to publish/consume events.")
    partitions: int = Field(
        default=1, ge=1, description="Number of topic partitions (default 1)."
    )
    replication_factor: int = Field(
        default=1, ge=1, description="Replication factor for the topic (default 1)."
    )


class ProcessingEngineConfig(BaseModel):
    """Distributed processing engine to use for computation."""

    model_config = ConfigDict(extra="forbid")

    type: Literal["spark"] = Field(..., description="Processing engine type.")


class StorageConfig(BaseModel):
    """
    Describes where the pipeline should write its output.

    *path* must be an absolute path (enforced by the business validator with
    a clear message rather than a generic Pydantic error).
    """

    model_config = ConfigDict(extra="forbid")

    type: Literal["hdfs", "local", "minio"] = Field(
        ..., description="Storage backend type."
    )
    path: str = Field(..., description="Absolute output path.")
    mode: Literal["overwrite", "append"] = Field(
        default="overwrite", description="Write mode: overwrite (default) or append."
    )


class MonitoringConfig(BaseModel):
    """Optional monitoring / observability settings."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = Field(..., description="True to enable monitoring hooks.")


# ── Top-level config ───────────────────────────────────────────────────────────


class PipelineConfig(BaseModel):
    """
    Root model for a complete pipeline configuration document.

    Parsing a YAML file against this model guarantees that every required
    section is present, every field has the correct type, and every
    discriminated-union operation has been recognised.

    The ``schema`` field uses Pydantic's standard field name because Pydantic
    v2 replaced the deprecated ``BaseModel.schema()`` classmethod with
    ``model_json_schema()`` — the instance field name no longer clashes.
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    pipeline: PipelineMeta
    source: SourceConfig
    # Python attribute is named pipeline_schema; YAML key and API alias is "schema".
    # The alias avoids shadowing Pydantic's deprecated BaseModel.schema() classmethod.
    pipeline_schema: list[ColumnSchema] = Field(
        ...,
        alias="schema",
        min_length=1,
        description="Data schema — at least one column must be declared.",
    )
    processing: ProcessingConfig
    streaming: StreamingConfig
    messaging: Optional[MessagingConfig] = Field(
        None,
        description="Broker config; required when streaming.enabled is True.",
    )
    processing_engine: Optional[ProcessingEngineConfig] = Field(
        None,
        description="Processing engine config. When absent or None, the pandas in-process path is used.",
    )
    storage: StorageConfig
    monitoring: Optional[MonitoringConfig] = None
