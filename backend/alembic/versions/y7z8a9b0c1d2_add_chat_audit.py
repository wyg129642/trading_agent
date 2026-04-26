"""Add chat_audit_run + chat_audit_event tables.

Persistent record of every AI-chat request: who asked what, which models
ran, which tools / searches / KB lookups they made, what came back, what the
final reply + citations were. Powers the admin/user "AI conversation audit"
page. Schema is two-table (run + event timeline) with JSONB payloads so
new event types ship without migrations.

Revision ID: y7z8a9b0c1d2
Revises: x6y7z8a9b0c1
Create Date: 2026-04-25 12:00:00.000000
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "y7z8a9b0c1d2"
down_revision: Union[str, None] = "x6y7z8a9b0c1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "chat_audit_run",
        sa.Column(
            "id", postgresql.UUID(as_uuid=True),
            primary_key=True, server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("trace_id", sa.String(64), nullable=False, unique=True),
        sa.Column(
            "user_id", postgresql.UUID(as_uuid=True),
            sa.ForeignKey("users.id", ondelete="SET NULL"), nullable=True,
        ),
        sa.Column("username", sa.String(120), nullable=False, server_default=""),
        sa.Column(
            "conversation_id", postgresql.UUID(as_uuid=True),
            sa.ForeignKey("chat_conversations.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "message_id", postgresql.UUID(as_uuid=True),
            sa.ForeignKey("chat_messages.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("user_content", sa.Text(), nullable=False, server_default=""),
        sa.Column("models_requested", postgresql.JSONB(), nullable=True),
        sa.Column("mode", sa.String(40), nullable=False, server_default="standard"),
        sa.Column("web_search_mode", sa.String(20), nullable=False, server_default="off"),
        sa.Column("feature_flags", postgresql.JSONB(), nullable=True),
        sa.Column("system_prompt_len", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("history_messages", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("tools_offered", postgresql.JSONB(), nullable=True),
        sa.Column("status", sa.String(20), nullable=False, server_default="running"),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("rounds_used", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("tool_calls_total", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("tool_calls_by_name", postgresql.JSONB(), nullable=True),
        sa.Column("urls_searched", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("urls_read", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("citations_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("total_tokens", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("total_cost_usd", sa.Numeric(12, 6), nullable=True),
        sa.Column("total_latency_ms", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("final_content_len", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("client_ip", postgresql.INET(), nullable=True),
        sa.Column("user_agent", sa.Text(), nullable=True),
        sa.Column(
            "started_at", sa.DateTime(timezone=True),
            nullable=False, server_default=sa.text("now()"),
        ),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_chat_audit_run_started", "chat_audit_run", ["started_at"])
    op.create_index("ix_chat_audit_run_user_started", "chat_audit_run", ["user_id", "started_at"])
    op.create_index("ix_chat_audit_run_conv", "chat_audit_run", ["conversation_id"])
    op.create_index("ix_chat_audit_run_status", "chat_audit_run", ["status"])

    op.create_table(
        "chat_audit_event",
        sa.Column(
            "id", postgresql.UUID(as_uuid=True),
            primary_key=True, server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "run_id", postgresql.UUID(as_uuid=True),
            sa.ForeignKey("chat_audit_run.id", ondelete="CASCADE"), nullable=False,
        ),
        sa.Column("trace_id", sa.String(64), nullable=False),
        sa.Column("sequence", sa.Integer(), nullable=False),
        sa.Column("event_type", sa.String(40), nullable=False),
        sa.Column("model_id", sa.String(100), nullable=True),
        sa.Column("round_num", sa.Integer(), nullable=True),
        sa.Column("tool_name", sa.String(60), nullable=True),
        sa.Column("latency_ms", sa.Integer(), nullable=True),
        sa.Column("payload", postgresql.JSONB(), nullable=True),
        sa.Column(
            "payload_truncated", sa.Boolean(),
            nullable=False, server_default=sa.text("false"),
        ),
        sa.Column(
            "created_at", sa.DateTime(timezone=True),
            nullable=False, server_default=sa.text("now()"),
        ),
    )
    op.create_index("ix_chat_audit_event_run_seq", "chat_audit_event", ["run_id", "sequence"])
    op.create_index("ix_chat_audit_event_trace", "chat_audit_event", ["trace_id"])
    op.create_index("ix_chat_audit_event_type", "chat_audit_event", ["event_type"])
    op.create_index("ix_chat_audit_event_tool", "chat_audit_event", ["tool_name"])
    op.create_index("ix_chat_audit_event_created", "chat_audit_event", ["created_at"])


def downgrade() -> None:
    op.drop_index("ix_chat_audit_event_created", table_name="chat_audit_event")
    op.drop_index("ix_chat_audit_event_tool", table_name="chat_audit_event")
    op.drop_index("ix_chat_audit_event_type", table_name="chat_audit_event")
    op.drop_index("ix_chat_audit_event_trace", table_name="chat_audit_event")
    op.drop_index("ix_chat_audit_event_run_seq", table_name="chat_audit_event")
    op.drop_table("chat_audit_event")

    op.drop_index("ix_chat_audit_run_status", table_name="chat_audit_run")
    op.drop_index("ix_chat_audit_run_conv", table_name="chat_audit_run")
    op.drop_index("ix_chat_audit_run_user_started", table_name="chat_audit_run")
    op.drop_index("ix_chat_audit_run_started", table_name="chat_audit_run")
    op.drop_table("chat_audit_run")
