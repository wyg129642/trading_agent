"""Add portfolio_signal_dismissals — per-user 已阅 state for breaking news.

Revision ID: a1b2c3d4e5f6
Revises: z8a9b0c1d2e3
Create Date: 2026-04-30 12:00:00.000000
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID

revision: str = "a1b2c3d4e5f6"
down_revision: Union[str, None] = "z8a9b0c1d2e3"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "portfolio_signal_dismissals",
        sa.Column("user_id", UUID(as_uuid=True), nullable=False),
        sa.Column("signal_id", sa.Text(), nullable=False),
        sa.Column(
            "dismissed_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("user_id", "signal_id"),
    )
    op.create_index(
        "ix_psd_user_dismissed_at",
        "portfolio_signal_dismissals",
        ["user_id", sa.text("dismissed_at DESC")],
    )


def downgrade() -> None:
    op.drop_index("ix_psd_user_dismissed_at", table_name="portfolio_signal_dismissals")
    op.drop_table("portfolio_signal_dismissals")
