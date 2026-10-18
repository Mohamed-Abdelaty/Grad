"""
app/reliability/experiment_models.py
--------------------------------------
Pydantic v2 schema for ChaosLab experiment configuration YAML.

Matches the canonical YAML shape::

    experiment:
      name: kafka-failure-test

    target:
      service: kafka

    fault:
      type: container_stop
      duration: 20

    validation:
      expected_data_loss: 0
      max_recovery_time: 30

Design follows Phase 2 style:
- ``ConfigDict(extra="forbid")`` on all models.
- Pydantic ``field_validator`` for duration ceiling enforcement (defence in
  depth — ``safety.py`` enforces the same ceiling independently).
- ``Literal`` types for controlled enumerations.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

# ── Constants ─────────────────────────────────────────────────────────────────

MAX_FAULT_DURATION_SECONDS: int = 300
"""
Hard ceiling on fault duration.  No experiment YAML can exceed this value.
``SafetyValidator.validate_duration()`` enforces the same ceiling as defence
in depth — never rely on only one validation layer.
"""

ALLOWED_FAULT_TYPES = Literal[
    "container_stop",
    "container_restart",
    "network_disruption",
    "cpu_stress",
    "memory_stress",
    "disk_pressure",
    "service_delay",
]

ALLOWED_TARGET_SERVICES_LITERAL = Literal[
    "kafka",
    "spark-master",
    "spark-worker",
    "namenode",
    "datanode",
    "postgres",
    "backend",
]


# ── Sub-models ─────────────────────────────────────────────────────────────────

class ExperimentMeta(BaseModel):
    """Top-level experiment identity block."""

    model_config = ConfigDict(extra="forbid")

    name: str
    """Human-readable experiment name, used in reports and log entries."""


class TargetConfig(BaseModel):
    """Specifies which platform service to target."""

    model_config = ConfigDict(extra="forbid")

    service: ALLOWED_TARGET_SERVICES_LITERAL
    """
    Target service name.  Must be one of the platform services defined in
    ``ALLOWED_TARGET_SERVICES_LITERAL``.  The safety layer (``safety.py``)
    is the REAL enforcement point — this type hint is a first line of defence.
    """


class FaultConfig(BaseModel):
    """Specifies the fault type and how long to apply it."""

    model_config = ConfigDict(extra="forbid")

    type: ALLOWED_FAULT_TYPES
    """Fault type — must match a registered ``BaseFaultInjector.fault_type``."""

    duration: int = 20
    """
    Duration in seconds to hold the fault applied.
    Hard ceiling: ``MAX_FAULT_DURATION_SECONDS`` (300s).
    Not meaningful for ``container_restart`` (instantaneous fault) but
    accepted for schema uniformity.
    """

    cores: int | None = None
    """CPU cores to stress (cpu_stress only)."""

    memory_bytes: str | None = None
    """Memory to stress, e.g. ``'256M'`` (memory_stress only)."""

    disk_size_mb: int | None = None
    """Disk pressure size in MB (disk_pressure only, max 500 MB enforced by injector)."""

    delay_ms: int | None = None
    """Added network latency in milliseconds (service_delay only, max 10,000 ms)."""

    @field_validator("delay_ms")
    @classmethod
    def delay_must_be_safe(cls, value: int | None) -> int | None:
        """Keep the service-delay injector bounded and reversible."""
        if value is not None and not 1 <= value <= 10_000:
            raise ValueError("fault.delay_ms must be between 1 and 10000.")
        return value

    @field_validator("duration")
    @classmethod
    def duration_must_be_positive_and_within_ceiling(cls, v: int) -> int:
        """Reject durations <= 0 or > MAX_FAULT_DURATION_SECONDS."""
        if v <= 0:
            raise ValueError(f"fault.duration must be a positive integer, got {v}.")
        if v > MAX_FAULT_DURATION_SECONDS:
            raise ValueError(
                f"fault.duration {v}s exceeds maximum ceiling of "
                f"{MAX_FAULT_DURATION_SECONDS}s."
            )
        return v


class ValidationConfig(BaseModel):
    """Defines the pass/fail criteria for the experiment."""

    model_config = ConfigDict(extra="forbid")

    expected_data_loss: int = 0
    """
    Maximum number of records that may be lost for the experiment to PASS.
    ``0`` means zero-tolerance data loss.
    """

    max_recovery_time: float = 60.0
    """
    Maximum seconds the system may take to recover before the experiment FAILs.
    """


# ── Top-level model ────────────────────────────────────────────────────────────

class ExperimentModel(BaseModel):
    """
    Root model for a complete ChaosLab experiment definition.

    ``extra="forbid"`` ensures no unknown top-level keys slip in from YAML,
    consistent with the Phase 2 ``PipelineConfig`` design.
    """

    model_config = ConfigDict(extra="forbid")

    experiment: ExperimentMeta
    target: TargetConfig
    fault: FaultConfig
    validation: ValidationConfig = ValidationConfig()
    repetitions: int = Field(
        default=1,
        ge=1,
        le=10,
        description="Number of sequential experiment repetitions for reproducibility.",
    )
    baseline_seconds: int = Field(
        default=3,
        ge=1,
        le=60,
        description="Duration in seconds for pre-fault baseline measurement window.",
    )
