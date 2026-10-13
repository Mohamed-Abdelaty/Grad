"""Add config_yaml text column to pipeline_runs table.

Revision ID: 0005_add_config_yaml
Revises: 0004_add_last_progress_snapshot
Create Date: 2026-10-13 00:00:00.000000

Changes
-------
* New column ``pipeline_runs.config_yaml`` (Text, nullable) —
  stores the submitted YAML configuration so that GET /api/pipeline-runs/{id}
  can return it as yaml_content. Older runs return null.

Migration is written using Alembic operations only (no raw SQL).
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0005_add_config_yaml"
down_revision: Union[str, None] = "0004_add_last_progress_snapshot"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "pipeline_runs",
        sa.Column(
            "config_yaml",
            sa.Text(),
            nullable=True,
            comment="Original submitted YAML configuration.",
        ),
    )


def downgrade() -> None:
    op.drop_column("pipeline_runs", "config_yaml")
