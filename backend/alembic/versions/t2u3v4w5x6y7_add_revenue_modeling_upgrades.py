"""Add upgrades to revenue modeling: citation_audit_log, revenue_model_backtest,
expert_call_requests, model_comments, model_collaborators, recipe_runs.ab_group.

Revision ID: t2u3v4w5x6y7
Revises: s1t2u3v4w5x6
Create Date: 2026-04-23 19:00:00.000000
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID, JSONB


revision: str = "t2u3v4w5x6y7"
down_revision: Union[str, None] = "s1t2u3v4w5x6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ── citation_audit_log ────────────────────────────────────
    op.create_table(
        "citation_audit_log",
        sa.Column("id", UUID(as_uuid=True), primary_key=True,
                  server_default=sa.text("gen_random_uuid()")),
        sa.Column("model_id", UUID(as_uuid=True),
                  sa.ForeignKey("revenue_models.id", ondelete="CASCADE"), nullable=False),
        sa.Column("cell_id", UUID(as_uuid=True), nullable=True),
        sa.Column("cell_path", sa.String(500), nullable=False),
        sa.Column("citation_title", sa.Text, nullable=False, server_default=""),
        sa.Column("citation_url", sa.Text, nullable=False, server_default=""),
        sa.Column("claimed_snippet", sa.Text, nullable=False, server_default=""),
        sa.Column("verdict", sa.String(30), nullable=False),
        sa.Column("verdict_reason", sa.Text, nullable=False, server_default=""),
        sa.Column("details", JSONB, nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("tokens_used", sa.Integer, nullable=False, server_default="0"),
        sa.Column("latency_ms", sa.Integer, nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
    )
    op.create_index("ix_cit_audit_model", "citation_audit_log", ["model_id"])
    op.create_index("ix_cit_audit_verdict", "citation_audit_log", ["verdict"])
    op.create_index("ix_cit_audit_created_at", "citation_audit_log", ["created_at"])

    # ── revenue_model_backtest ────────────────────────────────
    op.create_table(
        "revenue_model_backtest",
        sa.Column("id", UUID(as_uuid=True), primary_key=True,
                  server_default=sa.text("gen_random_uuid()")),
        sa.Column("model_id", UUID(as_uuid=True),
                  sa.ForeignKey("revenue_models.id", ondelete="CASCADE"), nullable=False),
        sa.Column("cell_path", sa.String(500), nullable=False),
        sa.Column("period", sa.String(20), nullable=False),
        sa.Column("predicted_value", sa.Float, nullable=True),
        sa.Column("predicted_confidence", sa.String(10), nullable=False, server_default="MEDIUM"),
        sa.Column("actual_value", sa.Float, nullable=True),
        sa.Column("abs_error", sa.Float, nullable=True),
        sa.Column("pct_error", sa.Float, nullable=True),
        sa.Column("actual_source", sa.String(200), nullable=False, server_default=""),
        sa.Column("actual_reported_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("prediction_made_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("details", JSONB, nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("created_at", sa.DateTime(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
    )
    op.create_index("ix_backtest_model", "revenue_model_backtest", ["model_id"])
    op.create_index("ix_backtest_path_period", "revenue_model_backtest",
                    ["cell_path", "period"])
    op.create_index("ix_backtest_confidence", "revenue_model_backtest",
                    ["predicted_confidence"])

    # ── expert_call_requests ──────────────────────────────────
    op.create_table(
        "expert_call_requests",
        sa.Column("id", UUID(as_uuid=True), primary_key=True,
                  server_default=sa.text("gen_random_uuid()")),
        sa.Column("model_id", UUID(as_uuid=True),
                  sa.ForeignKey("revenue_models.id", ondelete="CASCADE"), nullable=False),
        sa.Column("cell_path", sa.String(500), nullable=True),
        sa.Column("ticker", sa.String(40), nullable=False),
        sa.Column("topic", sa.Text, nullable=False, server_default=""),
        sa.Column("questions", JSONB, nullable=False, server_default=sa.text("'[]'::jsonb")),
        sa.Column("rationale", sa.Text, nullable=False, server_default=""),
        sa.Column("status", sa.String(20), nullable=False, server_default="open"),
        sa.Column("requested_by", UUID(as_uuid=True),
                  sa.ForeignKey("users.id", ondelete="SET NULL"), nullable=True),
        sa.Column("assigned_to", UUID(as_uuid=True), nullable=True),
        sa.Column("interview_doc_id", sa.String(200), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "status IN ('open','scheduled','completed','cancelled')",
            name="ck_expert_call_status",
        ),
    )
    op.create_index("ix_expert_call_model", "expert_call_requests", ["model_id"])
    op.create_index("ix_expert_call_status", "expert_call_requests", ["status"])

    # ── model_comments ────────────────────────────────────────
    op.create_table(
        "model_comments",
        sa.Column("id", UUID(as_uuid=True), primary_key=True,
                  server_default=sa.text("gen_random_uuid()")),
        sa.Column("model_id", UUID(as_uuid=True),
                  sa.ForeignKey("revenue_models.id", ondelete="CASCADE"), nullable=False),
        sa.Column("cell_id", UUID(as_uuid=True),
                  sa.ForeignKey("model_cells.id", ondelete="CASCADE"), nullable=True),
        sa.Column("author_id", UUID(as_uuid=True),
                  sa.ForeignKey("users.id", ondelete="SET NULL"), nullable=True),
        sa.Column("mentions", JSONB, nullable=False, server_default=sa.text("'[]'::jsonb")),
        sa.Column("body", sa.Text, nullable=False),
        sa.Column("resolved", sa.Boolean, nullable=False, server_default=sa.text("false")),
        sa.Column("created_at", sa.DateTime(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
    )
    op.create_index("ix_model_comments_model", "model_comments", ["model_id"])
    op.create_index("ix_model_comments_cell", "model_comments", ["cell_id"])

    # ── model_collaborators ───────────────────────────────────
    op.create_table(
        "model_collaborators",
        sa.Column("id", UUID(as_uuid=True), primary_key=True,
                  server_default=sa.text("gen_random_uuid()")),
        sa.Column("model_id", UUID(as_uuid=True),
                  sa.ForeignKey("revenue_models.id", ondelete="CASCADE"), nullable=False),
        sa.Column("user_id", UUID(as_uuid=True),
                  sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False),
        sa.Column("role", sa.String(20), nullable=False, server_default="viewer"),
        sa.Column("added_at", sa.DateTime(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
        sa.CheckConstraint(
            "role IN ('viewer','editor','admin')",
            name="ck_model_collab_role",
        ),
        sa.UniqueConstraint("model_id", "user_id", name="uq_model_collab"),
    )

    # ── recipe_runs extra columns ─────────────────────────────
    op.add_column(
        "recipe_runs",
        sa.Column("ab_group", sa.String(20), nullable=False, server_default=""),
    )
    op.add_column(
        "recipe_runs",
        sa.Column("ab_session", sa.String(80), nullable=False, server_default=""),
    )
    op.create_index("ix_recipe_runs_ab_session", "recipe_runs", ["ab_session"])


def downgrade() -> None:
    op.drop_index("ix_recipe_runs_ab_session", table_name="recipe_runs")
    op.drop_column("recipe_runs", "ab_session")
    op.drop_column("recipe_runs", "ab_group")

    op.drop_table("model_collaborators")
    op.drop_index("ix_model_comments_cell", table_name="model_comments")
    op.drop_index("ix_model_comments_model", table_name="model_comments")
    op.drop_table("model_comments")
    op.drop_index("ix_expert_call_status", table_name="expert_call_requests")
    op.drop_index("ix_expert_call_model", table_name="expert_call_requests")
    op.drop_table("expert_call_requests")
    op.drop_index("ix_backtest_confidence", table_name="revenue_model_backtest")
    op.drop_index("ix_backtest_path_period", table_name="revenue_model_backtest")
    op.drop_index("ix_backtest_model", table_name="revenue_model_backtest")
    op.drop_table("revenue_model_backtest")
    op.drop_index("ix_cit_audit_created_at", table_name="citation_audit_log")
    op.drop_index("ix_cit_audit_verdict", table_name="citation_audit_log")
    op.drop_index("ix_cit_audit_model", table_name="citation_audit_log")
    op.drop_table("citation_audit_log")
