"""Add reproducibility and pre-fault accounting columns to experiment_runs.

Revision ID: 0007_add_reproducibility_and_pre_fault_accounting
Revises: 0006_add_experiment_events
Create Date: 2026-10-17 15:00:00.000000

Changes
-------
* New column ``experiment_runs.parent_run_id`` — FK self-reference for grouped repetitions.
* New column ``experiment_runs.repetition_index`` — 1-based repetition index (null for parent).
* New column ``experiment_runs.repetition_count`` — Total configured repetitions (default 1).
* New column ``experiment_runs.pre_fault_accounting`` — Pipeline counters snapshot before fault injection.
* New column ``experiment_runs.aggregated_metrics`` — Summary statistics across repetitions on parent run.
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "0007_add_reproducibility_and_pre_fault_accounting"
down_revision: Union[str, None] = "0006_add_experiment_events"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "experiment_runs",
        sa.Column(
            "parent_run_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("experiment_runs.id", ondelete="CASCADE"),
            nullable=True,
            comment="Parent experiment run ID for grouped repetitions.",
        ),
    )
    op.create_index(
        "ix_experiment_runs_parent_run_id",
        "experiment_runs",
        ["parent_run_id"],
    )
    op.add_column(
        "experiment_runs",
        sa.Column(
            "repetition_index",
            sa.Integer(),
            nullable=True,
            comment="1-based index of this repetition within the parent run.",
        ),
    )
    op.add_column(
        "experiment_runs",
        sa.Column(
            "repetition_count",
            sa.Integer(),
            nullable=False,
            server_default="1",
            comment="Total number of repetitions configured for this experiment.",
        ),
    )
    op.add_column(
        "experiment_runs",
        sa.Column(
            "pre_fault_accounting",
            sa.JSON(),
            nullable=True,
            comment="Snapshot of pipeline counters immediately prior to fault injection.",
        ),
    )
    op.add_column(
        "experiment_runs",
        sa.Column(
            "aggregated_metrics",
            sa.JSON(),
            nullable=True,
            comment="Statistical aggregation across completed repetitions (parent run only).",
        ),
    )


def downgrade() -> None:
    op.drop_column("experiment_runs", "aggregated_metrics")
    op.drop_column("experiment_runs", "pre_fault_accounting")
    op.drop_column("experiment_runs", "repetition_count")
    op.drop_column("experiment_runs", "repetition_index")
    op.drop_index("ix_experiment_runs_parent_run_id", table_name="experiment_runs")
    op.drop_column("experiment_runs", "parent_run_id")
