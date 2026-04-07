"""Add ticker_sentiments and sector_sentiments columns to analysis_results

Per-stock sentiment allows different sentiment labels for each affected ticker
in the same news item, improving signal accuracy. sector_sentiments stores
sentiment for industry sectors when no specific stock is identified.

Revision ID: h9c7d6e5f4a3
Revises: g8b6c5d4e3f2
Create Date: 2026-03-26 18:00:00.000000
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision: str = 'h9c7d6e5f4a3'
down_revision: Union[str, None] = 'g8b6c5d4e3f2'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('analysis_results', sa.Column('ticker_sentiments', JSONB, server_default='{}', nullable=True))
    op.add_column('analysis_results', sa.Column('sector_sentiments', JSONB, server_default='{}', nullable=True))


def downgrade() -> None:
    op.drop_column('analysis_results', 'sector_sentiments')
    op.drop_column('analysis_results', 'ticker_sentiments')
