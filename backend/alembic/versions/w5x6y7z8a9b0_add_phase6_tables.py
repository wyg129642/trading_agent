"""Phase 6: canonical recipe, PR workflow, segment snapshot.

* recipes.canonical (Boolean default False)
* recipe_change_requests (new table)
* segment_revenue_snapshot (new table)

Revision ID: w5x6y7z8a9b0
Revises: v4w5x6y7z8a9
Create Date: 2026-04-24 14:30:00.000000
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID, JSONB


revision: str = "w5x6y7z8a9b0"
down_revision: Union[str, None] = "v4w5x6y7z8a9"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "recipes",
        sa.Column(
            "canonical", sa.Boolean(), nullable=False,
            server_default=sa.text("false"),
        ),
    )

    op.create_table(
        "recipe_change_requests",
        sa.Column("id", UUID(as_uuid=True), primary_key=True,
                  server_default=sa.text("gen_random_uuid()")),
        sa.Column("canonical_recipe_id", UUID(as_uuid=True),
                  sa.ForeignKey("recipes.id", ondelete="CASCADE"), nullable=False),
        sa.Column("fork_recipe_id", UUID(as_uuid=True),
                  sa.ForeignKey("recipes.id", ondelete="CASCADE"), nullable=False),
        sa.Column("title", sa.String(200), nullable=False, server_default=""),
        sa.Column("description", sa.Text, nullable=False, server_default=""),
        sa.Column("requested_by", UUID(as_uuid=True),
                  sa.ForeignKey("users.id", ondelete="SET NULL"), nullable=True),
        sa.Column("status", sa.String(20), nullable=False, server_default="open"),
        sa.Column("graph_diff", JSONB, nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("review_note", sa.Text, nullable=False, server_default=""),
        sa.Column("reviewed_by", UUID(as_uuid=True),
                  sa.ForeignKey("users.id", ondelete="SET NULL"), nullable=True),
        sa.Column("reviewed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
        sa.CheckConstraint(
            "status IN ('open','approved','rejected','merged','withdrawn')",
            name="ck_recipe_cr_status",
        ),
    )
    op.create_index("ix_recipe_cr_canonical", "recipe_change_requests", ["canonical_recipe_id"])
    op.create_index("ix_recipe_cr_status", "recipe_change_requests", ["status"])

    op.create_table(
        "segment_revenue_snapshot",
        sa.Column("id", UUID(as_uuid=True), primary_key=True,
                  server_default=sa.text("gen_random_uuid()")),
        sa.Column("ticker", sa.String(40), nullable=False),
        sa.Column("industry", sa.String(80), nullable=False, server_default=""),
        sa.Column("segment_slug", sa.String(120), nullable=False),
        sa.Column("period", sa.String(20), nullable=False),
        sa.Column("metric", sa.String(40), nullable=False, server_default="revenue"),
        sa.Column("value", sa.Float, nullable=True),
        sa.Column("unit", sa.String(40), nullable=False, server_default=""),
        sa.Column("confidence", sa.String(10), nullable=False, server_default="MEDIUM"),
        sa.Column("source_type", sa.String(20), nullable=False, server_default="assumption"),
        sa.Column("source_model_id", UUID(as_uuid=True),
                  sa.ForeignKey("revenue_models.id", ondelete="SET NULL"), nullable=True),
        sa.Column("source_cell_path", sa.String(500), nullable=False, server_default=""),
        sa.Column("citations", JSONB, nullable=False, server_default=sa.text("'[]'::jsonb")),
        sa.Column("updated_at", sa.DateTime(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
        sa.UniqueConstraint(
            "ticker", "period", "segment_slug", "metric",
            name="uq_segment_snapshot_natural_key",
        ),
    )
    op.create_index("ix_segment_snapshot_ticker_period", "segment_revenue_snapshot",
                    ["ticker", "period"])
    op.create_index("ix_segment_snapshot_industry", "segment_revenue_snapshot", ["industry"])


def downgrade() -> None:
    op.drop_index("ix_segment_snapshot_industry", "segment_revenue_snapshot")
    op.drop_index("ix_segment_snapshot_ticker_period", "segment_revenue_snapshot")
    op.drop_table("segment_revenue_snapshot")
    op.drop_index("ix_recipe_cr_status", "recipe_change_requests")
    op.drop_index("ix_recipe_cr_canonical", "recipe_change_requests")
    op.drop_table("recipe_change_requests")
    op.drop_column("recipes", "canonical")
