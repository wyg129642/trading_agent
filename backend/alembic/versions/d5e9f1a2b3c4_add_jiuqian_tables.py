"""Add Jiuqian (久谦) tables

Revision ID: d5e9f1a2b3c4
Revises: c4a2b1d9e3f7
Create Date: 2026-03-16 20:00:00.000000
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = 'd5e9f1a2b3c4'
down_revision: Union[str, None] = 'd5b3c2e8f4a1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Forum expert calls
    op.create_table(
        'jiuqian_forum',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('industry', sa.String(100), nullable=True),
        sa.Column('related_targets', sa.Text(), nullable=True),
        sa.Column('title', sa.Text(), nullable=False),
        sa.Column('author', sa.String(200), nullable=True),
        sa.Column('expert_information', sa.Text(), nullable=True),
        sa.Column('topic', sa.Text(), nullable=True),
        sa.Column('summary', sa.Text(), nullable=True),
        sa.Column('content', sa.Text(), server_default='', nullable=False),
        sa.Column('insight', sa.Text(), nullable=True),
        sa.Column('create_time', sa.DateTime(timezone=True), nullable=True),
        sa.Column('meeting_time', sa.DateTime(timezone=True), nullable=True),
        sa.Column('operation_time', sa.DateTime(timezone=True), nullable=True),
        sa.Column('enrichment', postgresql.JSONB(), server_default='{}', nullable=False),
        sa.Column('is_enriched', sa.Boolean(), server_default='false', nullable=False),
        sa.Column('synced_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('ix_jq_forum_meeting', 'jiuqian_forum', ['meeting_time'])
    op.create_index('ix_jq_forum_enriched', 'jiuqian_forum', ['is_enriched'])
    op.create_index('ix_jq_forum_industry', 'jiuqian_forum', ['industry'])

    # Research minutes
    op.create_table(
        'jiuqian_minutes',
        sa.Column('id', sa.String(100), nullable=False),
        sa.Column('platform', sa.String(50), nullable=True),
        sa.Column('source', sa.String(200), nullable=True),
        sa.Column('pub_time', sa.DateTime(timezone=True), nullable=True),
        sa.Column('title', sa.Text(), nullable=False),
        sa.Column('summary', sa.Text(), nullable=True),
        sa.Column('content', sa.Text(), server_default='', nullable=False),
        sa.Column('author', sa.String(200), nullable=True),
        sa.Column('company', postgresql.JSONB(), server_default='[]', nullable=False),
        sa.Column('enrichment', postgresql.JSONB(), server_default='{}', nullable=False),
        sa.Column('is_enriched', sa.Boolean(), server_default='false', nullable=False),
        sa.Column('synced_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('ix_jq_min_pub', 'jiuqian_minutes', ['pub_time'])
    op.create_index('ix_jq_min_enriched', 'jiuqian_minutes', ['is_enriched'])
    op.create_index('ix_jq_min_source', 'jiuqian_minutes', ['source'])

    # WeChat articles
    op.create_table(
        'jiuqian_wechat',
        sa.Column('id', sa.String(100), nullable=False),
        sa.Column('platform', sa.String(50), nullable=True),
        sa.Column('source', sa.String(200), nullable=True),
        sa.Column('district', sa.String(50), nullable=True),
        sa.Column('pub_time', sa.DateTime(timezone=True), nullable=True),
        sa.Column('title', sa.Text(), nullable=False),
        sa.Column('summary', sa.Text(), nullable=True),
        sa.Column('content', sa.Text(), server_default='', nullable=False),
        sa.Column('post_url', sa.Text(), server_default='', nullable=False),
        sa.Column('enrichment', postgresql.JSONB(), server_default='{}', nullable=False),
        sa.Column('is_enriched', sa.Boolean(), server_default='false', nullable=False),
        sa.Column('synced_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('ix_jq_wx_pub', 'jiuqian_wechat', ['pub_time'])
    op.create_index('ix_jq_wx_enriched', 'jiuqian_wechat', ['is_enriched'])
    op.create_index('ix_jq_wx_source', 'jiuqian_wechat', ['source'])

    # Sync state
    op.create_table(
        'jiuqian_sync_state',
        sa.Column('source_name', sa.String(50), nullable=False),
        sa.Column('last_processed_ids', sa.Integer(), server_default='0', nullable=False),
        sa.Column('last_sync_time', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('total_synced', sa.Integer(), server_default='0', nullable=False),
        sa.Column('last_error', sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint('source_name'),
    )


def downgrade() -> None:
    op.drop_table('jiuqian_sync_state')
    op.drop_table('jiuqian_wechat')
    op.drop_table('jiuqian_minutes')
    op.drop_table('jiuqian_forum')
