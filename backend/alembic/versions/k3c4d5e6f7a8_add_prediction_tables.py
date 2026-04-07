"""Add stock prediction scoring tables

Tables: stock_predictions, prediction_edit_logs, prediction_evaluations
for the analyst stock prediction scoring and backtest system.

Revision ID: k3c4d5e6f7a8
Revises: j2b3c4d5e6f7
Create Date: 2026-04-01 12:00:00.000000
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID, JSONB

# revision identifiers, used by Alembic.
revision: str = "k3c4d5e6f7a8"
down_revision: Union[str, None] = "j2b3c4d5e6f7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # stock_predictions
    op.create_table(
        "stock_predictions",
        sa.Column("id", UUID(as_uuid=True), server_default=sa.text("gen_random_uuid()"), primary_key=True),
        sa.Column("user_id", UUID(as_uuid=True), sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False),
        sa.Column("submitted_by_id", UUID(as_uuid=True), sa.ForeignKey("users.id", ondelete="SET NULL"), nullable=False),
        sa.Column("stock_code", sa.String(20), nullable=False),
        sa.Column("stock_name", sa.String(100), nullable=False),
        sa.Column("market", sa.String(10), nullable=False),
        sa.Column("direction", sa.String(10), nullable=False),
        sa.Column("time_horizon", sa.String(10), nullable=False),
        sa.Column("reason", sa.Text, nullable=True),
        sa.Column("confidence", sa.Integer, default=3),
        sa.Column("price_at_submit", sa.Float, nullable=True),
        sa.Column("target_price", sa.Float, nullable=True),
        sa.Column("status", sa.String(20), server_default="active"),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
    )
    op.create_index("ix_predictions_user_id", "stock_predictions", ["user_id"])
    op.create_index("ix_predictions_status", "stock_predictions", ["status"])
    op.create_index("ix_predictions_created_at", "stock_predictions", ["created_at"])
    op.create_index("ix_predictions_stock_code", "stock_predictions", ["stock_code"])
    op.create_index("ix_predictions_expires_at", "stock_predictions", ["expires_at"])

    # prediction_edit_logs
    op.create_table(
        "prediction_edit_logs",
        sa.Column("id", UUID(as_uuid=True), server_default=sa.text("gen_random_uuid()"), primary_key=True),
        sa.Column("prediction_id", UUID(as_uuid=True), sa.ForeignKey("stock_predictions.id", ondelete="CASCADE"), nullable=False),
        sa.Column("edited_by_id", UUID(as_uuid=True), sa.ForeignKey("users.id", ondelete="SET NULL"), nullable=False),
        sa.Column("field_changed", sa.String(50), nullable=False),
        sa.Column("old_value", sa.Text, nullable=True),
        sa.Column("new_value", sa.Text, nullable=True),
        sa.Column("edited_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
    )
    op.create_index("ix_edit_logs_prediction_id", "prediction_edit_logs", ["prediction_id"])

    # prediction_evaluations
    op.create_table(
        "prediction_evaluations",
        sa.Column("id", UUID(as_uuid=True), server_default=sa.text("gen_random_uuid()"), primary_key=True),
        sa.Column("prediction_id", UUID(as_uuid=True), sa.ForeignKey("stock_predictions.id", ondelete="CASCADE"), nullable=False, unique=True),
        sa.Column("price_at_end", sa.Float, nullable=True),
        sa.Column("return_pct", sa.Float, nullable=True),
        sa.Column("is_direction_correct", sa.Boolean, nullable=True),
        sa.Column("score", sa.Float, nullable=True),
        sa.Column("max_favorable_pct", sa.Float, nullable=True),
        sa.Column("max_adverse_pct", sa.Float, nullable=True),
        sa.Column("price_series", JSONB, nullable=True),
        sa.Column("evaluated_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
    )
    op.create_index("ix_evaluations_prediction_id", "prediction_evaluations", ["prediction_id"])


def downgrade() -> None:
    op.drop_table("prediction_evaluations")
    op.drop_table("prediction_edit_logs")
    op.drop_table("stock_predictions")
