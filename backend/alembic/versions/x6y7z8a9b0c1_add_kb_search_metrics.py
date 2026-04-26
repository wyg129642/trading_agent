"""Add kb_search_metrics table for the AI-chat retrieval observability dashboard.

One row per kb_search / user_kb_search / kb_fetch_document call. Bounded
volume (~110k rows / year on current usage) so plain Postgres + a few
btree indexes is sufficient — no ClickHouse dependency.

Revision ID: x6y7z8a9b0c1
Revises: w5x6y7z8a9b0
Create Date: 2026-04-24 18:30:00.000000
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "x6y7z8a9b0c1"
down_revision: Union[str, None] = "w5x6y7z8a9b0"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "kb_search_metrics",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("ts", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("now()")),
        sa.Column("trace_id", sa.String(64), nullable=False, server_default=""),
        sa.Column("user_id", sa.String(64), nullable=False, server_default=""),
        sa.Column("tool_name", sa.String(40), nullable=False, server_default=""),
        sa.Column("query", sa.Text(), nullable=False, server_default=""),
        sa.Column("query_len", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("ticker_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("has_date_filter", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("top_k", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("result_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("embed_ms", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("milvus_ms", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("mongo_ms", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("total_ms", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("mode", sa.String(20), nullable=False, server_default=""),
        sa.Column("error", sa.Text(), nullable=False, server_default=""),
    )
    op.create_index("ix_kb_metrics_ts", "kb_search_metrics", ["ts"])
    op.create_index("ix_kb_metrics_trace", "kb_search_metrics", ["trace_id"])
    op.create_index("ix_kb_metrics_user", "kb_search_metrics", ["user_id"])
    op.create_index("ix_kb_metrics_tool", "kb_search_metrics", ["tool_name"])
    op.create_index("ix_kb_metrics_results", "kb_search_metrics", ["result_count"])
    op.create_index("ix_kb_metrics_total_ms", "kb_search_metrics", ["total_ms"])


def downgrade() -> None:
    op.drop_index("ix_kb_metrics_total_ms", table_name="kb_search_metrics")
    op.drop_index("ix_kb_metrics_results", table_name="kb_search_metrics")
    op.drop_index("ix_kb_metrics_tool", table_name="kb_search_metrics")
    op.drop_index("ix_kb_metrics_user", table_name="kb_search_metrics")
    op.drop_index("ix_kb_metrics_trace", table_name="kb_search_metrics")
    op.drop_index("ix_kb_metrics_ts", table_name="kb_search_metrics")
    op.drop_table("kb_search_metrics")
