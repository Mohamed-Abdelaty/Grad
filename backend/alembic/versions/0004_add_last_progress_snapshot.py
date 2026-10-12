"""Add last_progress_snapshot JSON column to pipeline_runs table.

Revision ID: 0004_add_last_progress_snapshot
Revises: 0003_add_run_events
Create Date: 2026-10-12 00:00:00.000000

Changes
-------
* New column ``pipeline_runs.last_progress_snapshot`` (JSON, nullable) —
  stores the final complete RunProgress snapshot upon run termination
  so that /progress can serve it even after backend restart or memory cleanup.

Migration is written using Alembic operations only (no raw SQL).
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0004_add_last_progress_snapshot"
down_revision: Union[str, None] = "0003_add_run_events"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "pipeline_runs",
        sa.Column(
            "last_progress_snapshot",
            sa.JSON(),
            nullable=True,
            comment="Final RunProgress JSON snapshot persisted upon completion.",
        ),
    )


def downgrade() -> None:
    op.drop_column("pipeline_runs", "last_progress_snapshot")
