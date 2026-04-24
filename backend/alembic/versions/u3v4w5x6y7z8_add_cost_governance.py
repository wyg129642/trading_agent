"""Add cost-governance + hallucination-guard columns.

* users.llm_budget_usd_monthly, users.llm_run_cap_usd
* recipe_runs.estimated_cost_usd, .cost_cap_usd, .paused_reason
* revenue_models.paused_by_guard, .paused_reason

Revision ID: u3v4w5x6y7z8
Revises: t2u3v4w5x6y7
Create Date: 2026-04-23 22:00:00.000000
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "u3v4w5x6y7z8"
down_revision: Union[str, None] = "t2u3v4w5x6y7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # users — LLM budget / per-run cap
    op.add_column(
        "users",
        sa.Column("llm_budget_usd_monthly", sa.Float(), nullable=True),
    )
    op.add_column(
        "users",
        sa.Column("llm_run_cap_usd", sa.Float(), nullable=True),
    )

    # recipe_runs — pre-flight estimate + hard cap + pause reason
    op.add_column(
        "recipe_runs",
        sa.Column(
            "estimated_cost_usd", sa.Float(), nullable=False,
            server_default="0.0",
        ),
    )
    op.add_column(
        "recipe_runs",
        sa.Column("cost_cap_usd", sa.Float(), nullable=True),
    )
    op.add_column(
        "recipe_runs",
        sa.Column("paused_reason", sa.String(60), nullable=True),
    )

    # revenue_models — hallucination-guard pause sentinel
    op.add_column(
        "revenue_models",
        sa.Column(
            "paused_by_guard", sa.Boolean(), nullable=False,
            server_default=sa.text("false"),
        ),
    )
    op.add_column(
        "revenue_models",
        sa.Column("paused_reason", sa.String(200), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("revenue_models", "paused_reason")
    op.drop_column("revenue_models", "paused_by_guard")
    op.drop_column("recipe_runs", "paused_reason")
    op.drop_column("recipe_runs", "cost_cap_usd")
    op.drop_column("recipe_runs", "estimated_cost_usd")
    op.drop_column("users", "llm_run_cap_usd")
    op.drop_column("users", "llm_budget_usd_monthly")
