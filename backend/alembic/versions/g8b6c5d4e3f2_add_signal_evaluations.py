"""Add signal_evaluations table for source leaderboard

Revision ID: g8b6c5d4e3f2
Revises: f7a5b4c3d2e1
Create Date: 2026-03-26 14:00:00.000000
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = 'g8b6c5d4e3f2'
down_revision: Union[str, None] = 'f7a5b4c3d2e1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'signal_evaluations',
        sa.Column('id', postgresql.UUID(as_uuid=True), server_default=sa.text('gen_random_uuid()'), primary_key=True),
        sa.Column('news_item_id', sa.String(100), nullable=False, index=True),
        sa.Column('source_name', sa.String(200), nullable=False, index=True),
        sa.Column('category', sa.String(50), server_default='', nullable=False),
        sa.Column('ticker', sa.String(20), nullable=False),
        sa.Column('market', sa.String(20), nullable=False),
        sa.Column('signal_time', sa.DateTime(timezone=True), nullable=False),
        sa.Column('predicted_sentiment', sa.String(20), nullable=False),
        sa.Column('price_at_signal', sa.Float, nullable=True),
        sa.Column('return_t0', sa.Float, nullable=True),
        sa.Column('return_t1', sa.Float, nullable=True),
        sa.Column('return_t5', sa.Float, nullable=True),
        sa.Column('return_t20', sa.Float, nullable=True),
        sa.Column('correct_t0', sa.Boolean, nullable=True),
        sa.Column('correct_t1', sa.Boolean, nullable=True),
        sa.Column('correct_t5', sa.Boolean, nullable=True),
        sa.Column('correct_t20', sa.Boolean, nullable=True),
        sa.Column('evaluated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    )
    op.create_index('ix_signal_eval_source_time', 'signal_evaluations', ['source_name', 'signal_time'])


def downgrade() -> None:
    op.drop_index('ix_signal_eval_source_time')
    op.drop_table('signal_evaluations')
