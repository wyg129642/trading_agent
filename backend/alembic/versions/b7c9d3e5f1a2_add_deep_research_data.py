"""Add deep_research_data JSONB column to research_reports

Stores structured citations, news timeline, referenced sources,
and search queries for frontend display.

Revision ID: b7c9d3e5f1a2
Revises: a3f8e2c71b90
Create Date: 2026-03-12 10:00:00.000000
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision: str = 'b7c9d3e5f1a2'
down_revision: Union[str, None] = 'a3f8e2c71b90'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        'research_reports',
        sa.Column('deep_research_data', JSONB, server_default='{}', nullable=False),
    )


def downgrade() -> None:
    op.drop_column('research_reports', 'deep_research_data')
