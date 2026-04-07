"""Add proactive portfolio scan tables

Revision ID: n6f7a8b9c0d1
Revises: m5e6f7a8b9c0
Create Date: 2026-04-01 23:00:00.000000
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

# revision identifiers, used by Alembic.
revision: str = "n6f7a8b9c0d1"
down_revision: Union[str, None] = "m5e6f7a8b9c0"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Per-stock baseline memory
    op.create_table(
        "portfolio_scan_baselines",
        sa.Column("ticker", sa.Text(), nullable=False),
        sa.Column("name_cn", sa.Text(), nullable=False, server_default=""),
        sa.Column("name_en", sa.Text(), nullable=True, server_default=""),
        sa.Column("market", sa.Text(), nullable=False, server_default=""),
        sa.Column("last_scan_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_narrative", sa.Text(), server_default=""),
        sa.Column("known_developments", JSONB(), server_default="[]"),
        sa.Column("known_content_ids", JSONB(), server_default="[]"),
        sa.Column("sentiment_history", JSONB(), server_default="[]"),
        sa.Column("scan_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("alert_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("last_alert_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("ticker"),
    )

    # Audit trail of every scan
    op.create_table(
        "portfolio_scan_results",
        sa.Column("id", sa.Text(), nullable=False),
        sa.Column("ticker", sa.Text(), nullable=False),
        sa.Column("scan_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("internal_item_count", sa.Integer(), server_default="0"),
        sa.Column("internal_new_count", sa.Integer(), server_default="0"),
        sa.Column("external_result_count", sa.Integer(), server_default="0"),
        sa.Column("delta_detected", sa.Boolean(), server_default="false"),
        sa.Column("delta_magnitude", sa.Text(), server_default="none"),
        sa.Column("delta_description", sa.Text(), server_default=""),
        sa.Column("new_developments", JSONB(), server_default="[]"),
        sa.Column("deep_research_performed", sa.Boolean(), server_default="false"),
        sa.Column("research_iterations", sa.Integer(), server_default="0"),
        sa.Column("key_findings", JSONB(), server_default="[]"),
        sa.Column("news_timeline", JSONB(), server_default="[]"),
        sa.Column("referenced_sources", JSONB(), server_default="[]"),
        sa.Column("should_alert", sa.Boolean(), server_default="false"),
        sa.Column("alert_confidence", sa.Float(), server_default="0.0"),
        sa.Column("alert_rationale", sa.Text(), server_default=""),
        sa.Column("full_analysis", JSONB(), server_default="{}"),
        sa.Column("snapshot_summary", JSONB(), server_default="{}"),
        sa.Column("tokens_used", sa.Integer(), server_default="0"),
        sa.Column("cost_cny", sa.Float(), server_default="0.0"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_psr_ticker_time", "portfolio_scan_results", ["ticker", sa.text("scan_time DESC")])
    op.create_index(
        "ix_psr_alert", "portfolio_scan_results", ["should_alert"],
        postgresql_where=sa.text("should_alert = true"),
    )
    op.create_index(
        "ix_psr_delta", "portfolio_scan_results", ["delta_magnitude"],
        postgresql_where=sa.text("delta_magnitude != 'none'"),
    )


def downgrade() -> None:
    op.drop_index("ix_psr_delta")
    op.drop_index("ix_psr_alert")
    op.drop_index("ix_psr_ticker_time")
    op.drop_table("portfolio_scan_results")
    op.drop_table("portfolio_scan_baselines")
