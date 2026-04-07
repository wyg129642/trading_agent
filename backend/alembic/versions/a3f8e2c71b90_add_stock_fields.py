"""Add stock fields to user_sources and engine_status table

Revision ID: a3f8e2c71b90
Revises: 5ddf6f55ba47
Create Date: 2026-03-10 22:00:00.000000
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = 'a3f8e2c71b90'
down_revision: Union[str, None] = '5ddf6f55ba47'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add stock subscription fields to user_sources
    op.add_column('user_sources', sa.Column('stock_market', sa.String(length=20), nullable=True))
    op.add_column('user_sources', sa.Column('stock_ticker', sa.String(length=20), nullable=True))
    op.add_column('user_sources', sa.Column('stock_name', sa.String(length=100), nullable=True))


def downgrade() -> None:
    op.drop_column('user_sources', 'stock_name')
    op.drop_column('user_sources', 'stock_ticker')
    op.drop_column('user_sources', 'stock_market')
