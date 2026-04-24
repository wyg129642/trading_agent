"""Add chat feedback + per-user memory tables for AI assistant self-evolution.

* chat_feedback_events — one row per user feedback submission on a model response
  (rating + qualitative tags + free-text). Consumed by the memory processor.
* user_chat_memories — distilled long-term user memories (preference / style /
  profile / topic_interest / domain_knowledge / correction) deduplicated by
  (user_id, memory_key). Injected into chat system prompt.
* chat_messages.memory_processed_at — mark message-level passive extraction
  done, so the daemon doesn't re-scan older conversations on each tick.

Revision ID: v4w5x6y7z8a9
Revises: u3v4w5x6y7z8
Create Date: 2026-04-24 12:00:00.000000
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID, JSONB


revision: str = "v4w5x6y7z8a9"
down_revision: Union[str, None] = "u3v4w5x6y7z8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ── chat_feedback_events ────────────────────────────────────
    op.create_table(
        "chat_feedback_events",
        sa.Column(
            "id", UUID(as_uuid=True), primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "response_id", UUID(as_uuid=True),
            sa.ForeignKey("chat_model_responses.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "user_id", UUID(as_uuid=True),
            sa.ForeignKey("users.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("rating", sa.Integer(), nullable=True),
        sa.Column("feedback_text", sa.Text(), nullable=False, server_default=""),
        sa.Column(
            "feedback_tags", JSONB(), nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
        sa.Column(
            "sentiment", sa.String(20), nullable=False,
            server_default="neutral",
        ),
        sa.Column(
            "processed", sa.Boolean(), nullable=False,
            server_default=sa.text("false"),
        ),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("process_error", sa.Text(), nullable=True),
        sa.Column(
            "memory_ids_created", JSONB(), nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint(
            "rating IS NULL OR (rating >= 1 AND rating <= 5)",
            name="ck_chat_feedback_rating_range",
        ),
        sa.CheckConstraint(
            "sentiment IN ('positive','negative','neutral','mixed')",
            name="ck_chat_feedback_sentiment",
        ),
    )
    op.create_index(
        "ix_chat_feedback_user", "chat_feedback_events", ["user_id"],
    )
    op.create_index(
        "ix_chat_feedback_response", "chat_feedback_events", ["response_id"],
    )
    op.create_index(
        "ix_chat_feedback_processed", "chat_feedback_events", ["processed"],
    )
    op.create_index(
        "ix_chat_feedback_created_at", "chat_feedback_events", ["created_at"],
    )

    # ── user_chat_memories ─────────────────────────────────────
    op.create_table(
        "user_chat_memories",
        sa.Column(
            "id", UUID(as_uuid=True), primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "user_id", UUID(as_uuid=True),
            sa.ForeignKey("users.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("memory_type", sa.String(30), nullable=False),
        sa.Column("memory_key", sa.String(120), nullable=False),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column(
            "evidence", JSONB(), nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
        sa.Column(
            "confidence_score", sa.Float(), nullable=False,
            server_default="0.6",
        ),
        sa.Column(
            "source_type", sa.String(30), nullable=False,
            server_default="feedback_derived",
        ),
        sa.Column(
            "usage_count", sa.Integer(), nullable=False, server_default="0",
        ),
        sa.Column("last_used_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "is_active", sa.Boolean(), nullable=False, server_default=sa.text("true"),
        ),
        sa.Column(
            "is_pinned", sa.Boolean(), nullable=False, server_default=sa.text("false"),
        ),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint(
            "memory_type IN ('preference','style','profile','topic_interest',"
            "'domain_knowledge','correction')",
            name="ck_user_memory_type",
        ),
        sa.CheckConstraint(
            "source_type IN ('feedback_derived','conversation_derived','manual')",
            name="ck_user_memory_source",
        ),
        sa.CheckConstraint(
            "confidence_score >= 0 AND confidence_score <= 1",
            name="ck_user_memory_confidence",
        ),
        sa.UniqueConstraint(
            "user_id", "memory_key", name="uq_user_chat_memory_key",
        ),
    )
    op.create_index(
        "ix_user_chat_memory_user_active",
        "user_chat_memories",
        ["user_id", "is_active"],
    )
    op.create_index(
        "ix_user_chat_memory_user_type",
        "user_chat_memories",
        ["user_id", "memory_type"],
    )

    # ── chat_messages.memory_processed_at ──────────────────────
    op.add_column(
        "chat_messages",
        sa.Column("memory_processed_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        "ix_chat_msg_memory_processed",
        "chat_messages",
        ["memory_processed_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_chat_msg_memory_processed", table_name="chat_messages")
    op.drop_column("chat_messages", "memory_processed_at")

    op.drop_index("ix_user_chat_memory_user_type", table_name="user_chat_memories")
    op.drop_index("ix_user_chat_memory_user_active", table_name="user_chat_memories")
    op.drop_table("user_chat_memories")

    op.drop_index("ix_chat_feedback_created_at", table_name="chat_feedback_events")
    op.drop_index("ix_chat_feedback_processed", table_name="chat_feedback_events")
    op.drop_index("ix_chat_feedback_response", table_name="chat_feedback_events")
    op.drop_index("ix_chat_feedback_user", table_name="chat_feedback_events")
    op.drop_table("chat_feedback_events")
