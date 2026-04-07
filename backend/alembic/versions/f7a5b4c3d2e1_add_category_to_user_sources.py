"""Add category column to user_sources

Revision ID: f7a5b4c3d2e1
Revises: e6f4a3b5c7d8
Create Date: 2026-03-26 12:00:00.000000
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = 'f7a5b4c3d2e1'
down_revision: Union[str, None] = 'e6f4a3b5c7d8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('user_sources', sa.Column('category', sa.String(length=50), server_default='', nullable=False))


def downgrade() -> None:
    op.drop_column('user_sources', 'category')
