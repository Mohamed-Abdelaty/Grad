"""Add experiment_events table and pipeline_run_id column to experiment_runs.

Revision ID: 0006_add_experiment_events
Revises: 0005_add_config_yaml
Create Date: 2026-10-15 00:00:00.000000

Changes
-------
* New table ``experiment_events`` — persists execution events emitted by the
  runner for live/historical display via GET /api/experiments/{id}/events.
* New column ``experiment_runs.pipeline_run_id`` — records linked pipeline run.

Migration is written using Alembic operations only (no raw SQL).
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "0006_add_experiment_events"
down_revision: Union[str, None] = "0005_add_config_yaml"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ── experiment_events table ────────────────────────────────────────────────
    op.create_table(
        "experiment_events",
        sa.Column(
            "id",
            sa.BigInteger(),
            autoincrement=True,
            nullable=False,
            primary_key=True,
            comment="Auto-incrementing event ID.",
        ),
        sa.Column(
            "experiment_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("experiments.id", ondelete="CASCADE"),
            nullable=False,
            index=True,
            comment="FK to the parent Experiment.",
        ),
        sa.Column(
            "experiment_run_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("experiment_runs.id", ondelete="CASCADE"),
            nullable=True,
            index=True,
            comment="FK to the ExperimentRun.",
        ),
        sa.Column(
            "seq",
            sa.Integer(),
            nullable=False,
            comment="Monotonic sequence number within the experiment run.",
        ),
        sa.Column(
            "level",
            sa.String(length=20),
            nullable=False,
            server_default="INFO",
            comment="Log level: INFO | WARNING | ERROR | DEBUG.",
        ),
        sa.Column(
            "phase",
            sa.String(length=50),
            nullable=False,
            comment="Lifecycle phase at emission time.",
        ),
        sa.Column(
            "message",
            sa.Text(),
            nullable=False,
            comment="Human-readable event message.",
        ),
        sa.Column(
            "ts",
            sa.DateTime(timezone=True),
            nullable=False,
            comment="UTC timestamp when the event was emitted.",
        ),
    )
    op.create_index(
        "ix_experiment_events_exp_seq",
        "experiment_events",
        ["experiment_id", "seq"],
    )

    # ── pipeline_run_id on experiment_runs ─────────────────────────────────────
    op.add_column(
        "experiment_runs",
        sa.Column(
            "pipeline_run_id",
            postgresql.UUID(as_uuid=True),
            nullable=True,
            comment="FK or reference to an associated PipelineRun.",
        ),
    )


def downgrade() -> None:
    op.drop_column("experiment_runs", "pipeline_run_id")
    op.drop_index("ix_experiment_events_exp_seq", table_name="experiment_events")
    op.drop_table("experiment_events")
