"""
app/models/pipeline_run_event.py
---------------------------------
ORM model for persisted pipeline run progress events.

One row per event emitted by the executor (source_start, spark_done, etc.)
for a given pipeline run.  Rows are batch-inserted after the run completes
so they are available for historical replay via the REST API even after the
in-memory RunProgressTracker has been cleaned up.

The ``seq`` column preserves the original emission order and lets clients
poll ``GET /api/pipeline-runs/{id}/events?after={seq}`` efficiently.
"""

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import BigInteger, DateTime, ForeignKey, Integer, String
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database.base import Base


class PipelineRunEvent(Base):
    """
    A single progress event emitted during a pipeline run.

    Inherits ``id``, ``created_at``, and ``updated_at`` from :class:`Base`.
    ``id`` here is a BIGSERIAL surrogate key for efficient range queries;
    ``pipeline_run_id`` is the FK back to :class:`PipelineRun`.
    """

    __tablename__ = "pipeline_run_events"

    # Override Base.id with a BIGSERIAL for efficient ordering / range scans
    id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=True,
        comment="Auto-incrementing event ID.  Used as cursor for /events?after=.",
    )

    pipeline_run_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("pipeline_runs.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
        comment="FK to the PipelineRun that emitted this event.",
    )

    seq: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        comment="Ordinal sequence number within the run (1-based, monotonically increasing).",
    )

    event_type: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        comment="Event type string, e.g. 'source_done', 'spark_start', 'run_success'.",
    )

    payload: Mapped[dict] = mapped_column(
        JSONB,
        nullable=False,
        server_default="{}",
        comment="Event-specific payload (e.g. {rows: 20} for source_done).",
    )

    emitted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default="now()",
        comment="UTC timestamp when the event was emitted by the executor.",
    )

    # ── Relationships ──────────────────────────────────────────────────────────

    pipeline_run: Mapped["PipelineRun"] = relationship(  # noqa: F821
        "PipelineRun",
        back_populates="events",
    )

    def __repr__(self) -> str:
        return (
            f"<PipelineRunEvent id={self.id}"
            f" run={self.pipeline_run_id}"
            f" seq={self.seq}"
            f" type={self.event_type!r}>"
        )
