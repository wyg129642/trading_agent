"""Add user_favorites table

Stores user bookmarks/favorites for news, wechat articles,
roadshows, and comments.

Revision ID: c4a2b1d9e3f7
Revises: b7c9d3e5f1a2
Create Date: 2026-03-14 10:00:00.000000
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = 'c4a2b1d9e3f7'
down_revision: Union[str, None] = 'b7c9d3e5f1a2'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'user_favorites',
        sa.Column('id', sa.Integer, primary_key=True, autoincrement=True),
        sa.Column('user_id', sa.dialects.postgresql.UUID(as_uuid=True), sa.ForeignKey('users.id', ondelete='CASCADE'), nullable=False),
        sa.Column('item_type', sa.String(30), nullable=False),
        sa.Column('item_id', sa.Text, nullable=False),
        sa.Column('note', sa.Text, nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.UniqueConstraint('user_id', 'item_type', 'item_id', name='uq_user_favorites'),
        sa.Index('ix_user_favorites_user', 'user_id'),
    )


def downgrade() -> None:
    op.drop_table('user_favorites')
