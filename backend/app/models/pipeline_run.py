"""
app/models/pipeline_run.py
--------------------------
ORM model for a single execution of a Pipeline at a specific version.

Tracks runtime state (status, timing) and links metrics and fault-injection
events back to the triggering run.
"""

import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database.base import Base


class PipelineRun(Base):
    """
    A single, time-bounded execution of a Pipeline.

    Inherits ``id``, ``created_at``, and ``updated_at`` from :class:`Base`.
    """

    __tablename__ = "pipeline_runs"

    # ── Parent references ──────────────────────────────────────────────────────
    pipeline_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("pipelines.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
        comment="FK to the Pipeline that this run belongs to.",
    )

    pipeline_version_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("pipeline_versions.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
        comment="FK to the specific PipelineVersion executed.",
    )

    # ── Execution state ────────────────────────────────────────────────────────
    status: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        default="pending",
        server_default="pending",
        comment="Lifecycle status: pending | running | succeeded | failed | cancelled.",
    )

    started_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="Timestamp when execution actually began (after scheduling lag).",
    )

    finished_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="Timestamp when execution completed (any terminal status).",
    )

    error_message: Mapped[str | None] = mapped_column(
        String(4096),
        nullable=True,
        comment="Human-readable error detail when status is 'failed'.",
    )

    output_path: Mapped[str | None] = mapped_column(
        String(1024),
        nullable=True,
        comment=(
            "Actual output path for the run's data (may differ from the "
            "configured path when the executor redirects a local path to HDFS)."
        ),
    )

    # ── Relationships ──────────────────────────────────────────────────────────
    pipeline: Mapped["Pipeline"] = relationship(  # noqa: F821
        "Pipeline",
        back_populates="runs",
    )

    pipeline_version: Mapped["PipelineVersion"] = relationship(  # noqa: F821
        "PipelineVersion",
        back_populates="runs",
    )

    metrics: Mapped[list["Metric"]] = relationship(  # noqa: F821
        "Metric",
        back_populates="pipeline_run",
        cascade="all, delete-orphan",
    )

    fault_injections: Mapped[list["FaultInjection"]] = relationship(  # noqa: F821
        "FaultInjection",
        back_populates="pipeline_run",
        cascade="all, delete-orphan",
    )

    events: Mapped[list["PipelineRunEvent"]] = relationship(  # noqa: F821
        "PipelineRunEvent",
        back_populates="pipeline_run",
        cascade="all, delete-orphan",
        order_by="PipelineRunEvent.seq",
    )

    def __repr__(self) -> str:
        return f"<PipelineRun id={self.id} status={self.status!r}>"
