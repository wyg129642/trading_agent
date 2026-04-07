"""Add topic_cluster_results table

Revision ID: e6f4a3b5c7d8
Revises: d5e9f1a2b3c4
Create Date: 2026-03-18 16:00:00.000000
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = 'e6f4a3b5c7d8'
down_revision: Union[str, None] = 'd5e9f1a2b3c4'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'topic_cluster_results',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('cluster_date', sa.Date(), nullable=False),
        sa.Column('run_time', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('total_items', sa.Integer(), server_default='0', nullable=False),
        sa.Column('n_clusters', sa.Integer(), server_default='0', nullable=False),
        sa.Column('anomalies', postgresql.JSONB(), server_default='[]', nullable=False),
        sa.Column('top_clusters', postgresql.JSONB(), server_default='[]', nullable=False),
        sa.Column('summary', sa.Text(), server_default='', nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('ix_tc_cluster_date', 'topic_cluster_results', ['cluster_date'])


def downgrade() -> None:
    op.drop_index('ix_tc_cluster_date', table_name='topic_cluster_results')
    op.drop_table('topic_cluster_results')
