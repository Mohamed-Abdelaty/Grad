"""
app/models/experiment_event.py
-------------------------------
ORM model for persisted experiment events.
"""

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import BigInteger, DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from app.database.base import Base


class _EventBase(DeclarativeBase):
    metadata = Base.metadata


class ExperimentEvent(_EventBase):
    """
    A single execution event emitted during a ChaosLab experiment.
    """

    __tablename__ = "experiment_events"

    id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=True,
        comment="Auto-incrementing event ID.",
    )

    experiment_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("experiments.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
        comment="FK to the parent Experiment.",
    )

    experiment_run_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("experiment_runs.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
        comment="FK to the ExperimentRun.",
    )

    seq: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        comment="Ordinal sequence number within the run (1-based).",
    )

    level: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        default="INFO",
        server_default="INFO",
        comment="Event severity: INFO | WARNING | ERROR | DEBUG.",
    )

    phase: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        comment="Lifecycle phase at emission time.",
    )

    message: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        comment="Human-readable event message.",
    )

    ts: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        comment="UTC timestamp when the event was emitted.",
    )

    def __repr__(self) -> str:
        return f"<ExperimentEvent id={self.id} exp={self.experiment_id} seq={self.seq} phase={self.phase!r}>"
