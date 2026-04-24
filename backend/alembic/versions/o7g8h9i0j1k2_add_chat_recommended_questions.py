"""Add chat_recommended_questions table for per-user daily quick-start suggestions.

Revision ID: o7g8h9i0j1k2
Revises: n6f7a8b9c0d1
Create Date: 2026-04-15 10:00:00.000000
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID, JSONB

revision: str = "o7g8h9i0j1k2"
down_revision: Union[str, None] = "n6f7a8b9c0d1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "chat_recommended_questions",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("user_id", UUID(as_uuid=True), sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False, unique=True),
        sa.Column("questions", JSONB, server_default="[]"),
        sa.Column("source_digest", sa.String(64), nullable=True),
        sa.Column("generated_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
    )
    op.create_index("ix_chat_rec_q_user", "chat_recommended_questions", ["user_id"])
    op.create_index("ix_chat_rec_q_generated", "chat_recommended_questions", ["generated_at"])


def downgrade() -> None:
    op.drop_index("ix_chat_rec_q_generated", "chat_recommended_questions")
    op.drop_index("ix_chat_rec_q_user", "chat_recommended_questions")
    op.drop_table("chat_recommended_questions")
