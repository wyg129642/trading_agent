"""Add debate columns and tracking tables

- chat_messages: add is_debate boolean
- chat_model_responses: add debate_round integer
- chat_tracking_topics: new table for user tracking topics
- chat_tracking_alerts: new table for matched tracking alerts

Revision ID: l4d5e6f7a8b9
Revises: k3c4d5e6f7a8
Create Date: 2026-04-01 18:00:00.000000
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID, JSONB

revision: str = "l4d5e6f7a8b9"
down_revision: Union[str, None] = "k3c4d5e6f7a8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Debate columns on existing tables
    op.add_column("chat_messages", sa.Column("is_debate", sa.Boolean, server_default="false"))
    op.add_column("chat_model_responses", sa.Column("debate_round", sa.Integer, nullable=True))

    # Tracking topics
    op.create_table(
        "chat_tracking_topics",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("user_id", UUID(as_uuid=True), sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False),
        sa.Column("topic", sa.Text, nullable=False),
        sa.Column("keywords", JSONB, server_default="[]"),
        sa.Column("related_tickers", JSONB, server_default="[]"),
        sa.Column("related_sectors", JSONB, server_default="[]"),
        sa.Column("notify_channels", JSONB, server_default='["browser"]'),
        sa.Column("is_active", sa.Boolean, server_default="true"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
        sa.Column("last_checked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_triggered_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_tracking_user", "chat_tracking_topics", ["user_id"])
    op.create_index("ix_tracking_active", "chat_tracking_topics", ["is_active"])

    # Tracking alerts
    op.create_table(
        "chat_tracking_alerts",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("topic_id", UUID(as_uuid=True), sa.ForeignKey("chat_tracking_topics.id", ondelete="CASCADE"), nullable=False),
        sa.Column("news_item_id", sa.Text, sa.ForeignKey("news_items.id", ondelete="CASCADE"), nullable=False),
        sa.Column("match_score", sa.Float, nullable=False),
        sa.Column("match_reason", sa.Text, server_default=""),
        sa.Column("is_read", sa.Boolean, server_default="false"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
    )
    op.create_index("ix_tracking_alert_topic", "chat_tracking_alerts", ["topic_id"])
    op.create_index("ix_tracking_alert_unread", "chat_tracking_alerts", ["is_read"])


def downgrade() -> None:
    op.drop_table("chat_tracking_alerts")
    op.drop_table("chat_tracking_topics")
    op.drop_column("chat_model_responses", "debate_round")
    op.drop_column("chat_messages", "is_debate")
