"""Add multi-horizon predicted sentiment, score, and confidence columns

Per-horizon columns allow the LLM to express different directional views
and confidence levels for T+1, T+5, and T+20 horizons independently,
enabling more granular signal evaluation and IC/ICIR analytics.

Revision ID: i1a2b3c4d5e6
Revises: h9c7d6e5f4a3
Create Date: 2026-03-27 10:00:00.000000
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = 'i1a2b3c4d5e6'
down_revision: Union[str, None] = 'h9c7d6e5f4a3'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Per-horizon predicted sentiments (categorical)
    op.add_column('signal_evaluations', sa.Column('predicted_sentiment_t1', sa.String(20), nullable=True))
    op.add_column('signal_evaluations', sa.Column('predicted_sentiment_t5', sa.String(20), nullable=True))
    op.add_column('signal_evaluations', sa.Column('predicted_sentiment_t20', sa.String(20), nullable=True))
    # Per-horizon numerical scores (continuous alpha factor, [-1.0, +1.0])
    op.add_column('signal_evaluations', sa.Column('sentiment_score_t1', sa.Float, nullable=True))
    op.add_column('signal_evaluations', sa.Column('sentiment_score_t5', sa.Float, nullable=True))
    op.add_column('signal_evaluations', sa.Column('sentiment_score_t20', sa.Float, nullable=True))
    # Per-horizon confidence ([0.0, 1.0])
    op.add_column('signal_evaluations', sa.Column('confidence_t1', sa.Float, nullable=True))
    op.add_column('signal_evaluations', sa.Column('confidence_t5', sa.Float, nullable=True))
    op.add_column('signal_evaluations', sa.Column('confidence_t20', sa.Float, nullable=True))
    # Composite index for ticker + signal_time lookups
    op.create_index('ix_signal_eval_ticker_time', 'signal_evaluations', ['ticker', 'signal_time'])


def downgrade() -> None:
    op.drop_index('ix_signal_eval_ticker_time', table_name='signal_evaluations')
    op.drop_column('signal_evaluations', 'confidence_t20')
    op.drop_column('signal_evaluations', 'confidence_t5')
    op.drop_column('signal_evaluations', 'confidence_t1')
    op.drop_column('signal_evaluations', 'sentiment_score_t20')
    op.drop_column('signal_evaluations', 'sentiment_score_t5')
    op.drop_column('signal_evaluations', 'sentiment_score_t1')
    op.drop_column('signal_evaluations', 'predicted_sentiment_t20')
    op.drop_column('signal_evaluations', 'predicted_sentiment_t5')
    op.drop_column('signal_evaluations', 'predicted_sentiment_t1')
