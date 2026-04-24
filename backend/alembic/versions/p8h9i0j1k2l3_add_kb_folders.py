"""Add kb_folders table for personal/public knowledge-base hierarchy.

Revision ID: p8h9i0j1k2l3
Revises: o7g8h9i0j1k2
Create Date: 2026-04-22 00:00:00.000000

Introduces a folder tree on top of the existing (flat) MongoDB user_kb
documents collection. Documents stay in Mongo — we only store tree
structure + metadata here. The Mongo docs grow two new nullable fields
(folder_id, scope) in the service layer; no Mongo migration is needed.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID

revision: str = "p8h9i0j1k2l3"
down_revision: Union[str, None] = "o7g8h9i0j1k2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "kb_folders",
        sa.Column(
            "id",
            UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        # user_id is NULL for public folders (any admin/boss can write).
        sa.Column(
            "user_id",
            UUID(as_uuid=True),
            sa.ForeignKey("users.id", ondelete="CASCADE"),
            nullable=True,
        ),
        sa.Column("scope", sa.String(16), nullable=False),  # 'public' | 'personal'
        # Self-reference for tree. NULL parent = root folder under the scope.
        sa.Column(
            "parent_id",
            UUID(as_uuid=True),
            sa.ForeignKey("kb_folders.id", ondelete="CASCADE"),
            nullable=True,
        ),
        sa.Column("name", sa.String(255), nullable=False),
        # folder_type: 'stock' | 'industry' | 'general'
        sa.Column("folder_type", sa.String(16), nullable=False),
        # Stock binding (only for folder_type='stock')
        sa.Column("stock_ticker", sa.String(32), nullable=True),
        sa.Column("stock_market", sa.String(8), nullable=True),
        sa.Column("stock_name", sa.String(255), nullable=True),
        sa.Column(
            "order_index", sa.Integer, nullable=False, server_default="0",
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "scope IN ('public','personal')", name="ck_kb_folders_scope",
        ),
        sa.CheckConstraint(
            "folder_type IN ('stock','industry','general')",
            name="ck_kb_folders_type",
        ),
        # Public folders have user_id NULL, personal folders have user_id set.
        sa.CheckConstraint(
            "(scope = 'public' AND user_id IS NULL) "
            "OR (scope = 'personal' AND user_id IS NOT NULL)",
            name="ck_kb_folders_scope_user",
        ),
        # Stock folders require a ticker.
        sa.CheckConstraint(
            "(folder_type = 'stock' AND stock_ticker IS NOT NULL) "
            "OR (folder_type IN ('industry','general') AND stock_ticker IS NULL)",
            name="ck_kb_folders_stock_ticker",
        ),
    )
    # Listing a user's tree efficiently.
    op.create_index(
        "ix_kb_folders_user_scope", "kb_folders", ["user_id", "scope"],
    )
    # Finding children of a folder.
    op.create_index("ix_kb_folders_parent", "kb_folders", ["parent_id"])
    # Public folders listing.
    op.create_index(
        "ix_kb_folders_scope_parent", "kb_folders", ["scope", "parent_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_kb_folders_scope_parent", "kb_folders")
    op.drop_index("ix_kb_folders_parent", "kb_folders")
    op.drop_index("ix_kb_folders_user_scope", "kb_folders")
    op.drop_table("kb_folders")
