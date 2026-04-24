"""Add kb_skill_templates table + user_preferences.kb_workspace_subfolders_initialized_at.

Revision ID: r1s2t3u4v5w6
Revises: q9i0j1k2l3m4
Create Date: 2026-04-23 10:00:00.000000

Two workspace-redesign additions:

1. ``user_preferences.kb_workspace_subfolders_initialized_at`` (nullable ts)
   gates the stock-folder sub-taxonomy seed (研报 / 公司公告 / 专家访谈 /
   公司交流 / 调研 / 模型 / 轮播 / 其他 plus key-driver.md and notes.md).
   NULL = never seeded; once set, the seeder is idempotent and respects
   user-deleted items (same pattern as ``kb_holdings_initialized_at``).

2. ``kb_skill_templates`` — per-user and per-org reusable templates. A
   "skill" is a bundle of folders + markdown + workbook files that can be
   materialized into any target folder (with ``{{stock_name}}`` and friends
   interpolated). The spec lives in ``spec`` as JSONB so we can evolve the
   schema without another migration.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID, JSONB

revision: str = "r1s2t3u4v5w6"
down_revision: Union[str, None] = "q9i0j1k2l3m4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "user_preferences",
        sa.Column(
            "kb_workspace_subfolders_initialized_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )

    op.create_table(
        "kb_skill_templates",
        sa.Column(
            "id",
            UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        # Null for `system` and `public` scopes. Set for `personal`.
        sa.Column(
            "owner_id",
            UUID(as_uuid=True),
            sa.ForeignKey("users.id", ondelete="CASCADE"),
            nullable=True,
        ),
        # system = shipped with app, public = org-wide (admin write), personal = per user
        sa.Column("scope", sa.String(16), nullable=False, server_default="personal"),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("description", sa.Text, nullable=False, server_default=""),
        sa.Column("icon", sa.String(64), nullable=False, server_default="ThunderboltOutlined"),
        # Where this skill can be installed. CSV of folder types.
        sa.Column(
            "target_types", sa.String(128), nullable=False,
            server_default="stock,industry,general",
        ),
        # Stable external key — for `system` skills we use a short slug
        # ("dcf_standard", "sensitivity_2d", ...) so the startup seeder can
        # detect existing rows idempotently and upgrade their spec.
        sa.Column("slug", sa.String(64), nullable=True),
        sa.Column("spec", JSONB, nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column(
            "is_published", sa.Boolean, nullable=False, server_default=sa.text("true"),
        ),
        sa.Column(
            "installs_count", sa.Integer, nullable=False, server_default="0",
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
            "scope IN ('system','public','personal')",
            name="ck_kb_skills_scope",
        ),
        sa.CheckConstraint(
            "(scope = 'personal' AND owner_id IS NOT NULL) "
            "OR (scope IN ('system','public') AND owner_id IS NULL)",
            name="ck_kb_skills_scope_owner",
        ),
    )
    op.create_index(
        "ix_kb_skills_scope", "kb_skill_templates", ["scope", "is_published"],
    )
    op.create_index(
        "ix_kb_skills_owner", "kb_skill_templates", ["owner_id"],
    )
    op.create_index(
        "ix_kb_skills_slug", "kb_skill_templates", ["slug"], unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_kb_skills_slug", "kb_skill_templates")
    op.drop_index("ix_kb_skills_owner", "kb_skill_templates")
    op.drop_index("ix_kb_skills_scope", "kb_skill_templates")
    op.drop_table("kb_skill_templates")
    op.drop_column(
        "user_preferences", "kb_workspace_subfolders_initialized_at",
    )
