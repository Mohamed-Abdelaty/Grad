"""
app/models/experiment_run.py
----------------------------
ORM model for a single execution of an Experiment with a specific
set of hyperparameters or configuration values.
"""

import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, JSON, String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database.base import Base


class ExperimentRun(Base):
    """
    One trial within an Experiment, executed with a specific configuration.

    Inherits ``id``, ``created_at``, and ``updated_at`` from :class:`Base`.
    """

    __tablename__ = "experiment_runs"

    # ── Parent reference ───────────────────────────────────────────────────────
    experiment_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("experiments.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
        comment="FK to the parent Experiment.",
    )

    # ── Execution state ────────────────────────────────────────────────────────
    status: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        default="pending",
        server_default="pending",
        comment="Run status: pending | running | succeeded | failed | cancelled.",
    )

    stage: Mapped[str | None] = mapped_column(
        String(50),
        nullable=True,
        comment="Detailed ChaosLab lifecycle stage while status is running.",
    )

    params: Mapped[dict | None] = mapped_column(
        JSON,
        nullable=True,
        comment="Key-value hyperparameters used for this run.",
    )

    results: Mapped[dict | None] = mapped_column(
        JSON,
        nullable=True,
        comment="Aggregated output metrics produced by this run.",
    )

    started_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="Timestamp when the run started.",
    )

    finished_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="Timestamp when the run completed.",
    )

    pipeline_run_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        nullable=True,
        comment="Associated pipeline run ID if driven by this experiment.",
    )

    parent_run_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("experiment_runs.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
        comment="Parent experiment run ID for grouped repetitions.",
    )

    repetition_index: Mapped[int | None] = mapped_column(
        nullable=True,
        comment="1-based index of this repetition within the parent run.",
    )

    repetition_count: Mapped[int] = mapped_column(
        nullable=False,
        default=1,
        server_default="1",
        comment="Total number of repetitions configured for this experiment.",
    )

    pre_fault_accounting: Mapped[dict | None] = mapped_column(
        JSON,
        nullable=True,
        comment="Snapshot of pipeline counters immediately prior to fault injection.",
    )

    aggregated_metrics: Mapped[dict | None] = mapped_column(
        JSON,
        nullable=True,
        comment="Statistical aggregation across completed repetitions (parent run only).",
    )

    # ── Relationships ──────────────────────────────────────────────────────────
    experiment: Mapped["Experiment"] = relationship(  # noqa: F821
        "Experiment",
        back_populates="runs",
    )

    parent_run: Mapped["ExperimentRun | None"] = relationship(
        "ExperimentRun",
        remote_side="ExperimentRun.id",
        back_populates="child_runs",
    )

    child_runs: Mapped[list["ExperimentRun"]] = relationship(
        "ExperimentRun",
        cascade="all, delete-orphan",
        back_populates="parent_run",
    )

    def __repr__(self) -> str:
        return f"<ExperimentRun id={self.id} status={self.status!r}>"
