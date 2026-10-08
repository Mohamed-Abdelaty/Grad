"""Add pipeline_run_events table and output_path column.

Revision ID: 0003_add_run_events
Revises: 0002_persist_experiment_definitions
Create Date: 2026-10-08 00:00:00.000000

Changes
-------
* New table ``pipeline_run_events`` — persists progress events emitted by the
  executor for historical replay via GET /api/pipeline-runs/{id}/events.
* New column ``pipeline_runs.output_path`` — records the actual (possibly
  redirected) HDFS output path so the UI can display it.

Migration is idempotent on a clean volume AND on an existing volume:
* CREATE TABLE IF NOT EXISTS guards against repeated runs.
* ADD COLUMN ... uses op.execute with a conditional guard.

The ``alembic_version`` TEXT fix from env.py runs before any migration so the
version column is always TEXT regardless of the original creation method.
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "0003_add_run_events"
down_revision: Union[str, None] = "0002_persist_experiment_definitions"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ── pipeline_run_events ────────────────────────────────────────────────────
    op.create_table(
        "pipeline_run_events",
        sa.Column(
            "id",
            sa.BigInteger(),
            autoincrement=True,
            nullable=False,
            comment=(
                "Auto-incrementing event ID used as cursor for "
                "/events?after= pagination."
            ),
        ),
        sa.Column(
            "pipeline_run_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
            comment="FK to the PipelineRun that emitted this event.",
        ),
        sa.Column(
            "seq",
            sa.Integer(),
            nullable=False,
            comment=(
                "Ordinal sequence number within the run "
                "(1-based, monotonically increasing)."
            ),
        ),
        sa.Column(
            "event_type",
            sa.String(50),
            nullable=False,
            comment=(
                "Event type string, e.g. 'source_done', "
                "'spark_start', 'run_success'."
            ),
        ),
        sa.Column(
            "payload",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default="{}",
            comment="Event-specific payload.",
        ),
        sa.Column(
            "emitted_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
            comment="UTC timestamp when the event was emitted by the executor.",
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.ForeignKeyConstraint(
            ["pipeline_run_id"],
            ["pipeline_runs.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    # Composite index: look up events for a run ordered by id efficiently
    op.create_index(
        "idx_prun_events_run_id_id",
        "pipeline_run_events",
        ["pipeline_run_id", "id"],
    )

    # ── pipeline_runs.output_path ──────────────────────────────────────────────
    # Guard: skip if the column already exists (idempotent for re-runs)
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = [c['name'] for c in inspector.get_columns('pipeline_runs')]
    if 'output_path' not in columns:
        op.add_column('pipeline_runs', sa.Column('output_path', sa.String(1024), nullable=True, comment="Actual data output path"))


def downgrade() -> None:
    op.drop_index("idx_prun_events_run_id_id", table_name="pipeline_run_events")
    op.drop_table("pipeline_run_events")
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = [c['name'] for c in inspector.get_columns('pipeline_runs')]
    if 'output_path' in columns:
        op.drop_column('pipeline_runs', 'output_path')
