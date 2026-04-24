"""Add Revenue Modeling system tables.

Tables:
  * revenue_models
  * model_cells
  * model_cell_versions
  * provenance_traces
  * debate_opinions
  * sanity_issues
  * recipes
  * recipe_runs
  * user_feedback_events
  * pending_lessons

Revision ID: s1t2u3v4w5x6
Revises: r1s2t3u4v5w6
Create Date: 2026-04-23 17:30:00.000000
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID, JSONB


revision: str = "s1t2u3v4w5x6"
down_revision: Union[str, None] = "r1s2t3u4v5w6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ── recipes ─────────────────────────────────────────────────
    op.create_table(
        "recipes",
        sa.Column("id", UUID(as_uuid=True), primary_key=True,
                  server_default=sa.text("gen_random_uuid()")),
        sa.Column("name", sa.String(200), nullable=False),
        sa.Column("slug", sa.String(200), nullable=False),
        sa.Column("industry", sa.String(80), nullable=True),
        sa.Column("description", sa.Text, server_default=""),
        sa.Column("graph", JSONB, server_default="{}"),
        sa.Column("version", sa.Integer, server_default="1", nullable=False),
        sa.Column("is_public", sa.Boolean, server_default="false", nullable=False),
        sa.Column("parent_recipe_id", UUID(as_uuid=True),
                  sa.ForeignKey("recipes.id", ondelete="SET NULL"), nullable=True),
        sa.Column("created_by", UUID(as_uuid=True),
                  sa.ForeignKey("users.id", ondelete="SET NULL"), nullable=True),
        sa.Column("pack_ref", sa.String(200), nullable=True),
        sa.Column("tags", JSONB, server_default="[]"),
        sa.Column("created_at", sa.DateTime(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
        sa.UniqueConstraint("slug", "version", name="uq_recipes_slug_version"),
    )
    op.create_index("ix_recipes_industry", "recipes", ["industry"])
    op.create_index("ix_recipes_is_public", "recipes", ["is_public"])
    op.create_index("ix_recipes_pack_ref", "recipes", ["pack_ref"])

    # ── revenue_models ──────────────────────────────────────────
    op.create_table(
        "revenue_models",
        sa.Column("id", UUID(as_uuid=True), primary_key=True,
                  server_default=sa.text("gen_random_uuid()")),
        sa.Column("ticker", sa.String(40), nullable=False),
        sa.Column("company_name", sa.String(200), nullable=False),
        sa.Column("industry", sa.String(80), nullable=False),
        sa.Column("fiscal_periods", JSONB, server_default="[]", nullable=False),
        sa.Column("recipe_id", UUID(as_uuid=True),
                  sa.ForeignKey("recipes.id", ondelete="SET NULL"), nullable=True),
        sa.Column("recipe_version", sa.Integer, nullable=True),
        sa.Column("status", sa.String(20), server_default="draft", nullable=False),
        sa.Column("conversation_id", UUID(as_uuid=True), nullable=True),
        sa.Column("owner_user_id", UUID(as_uuid=True),
                  sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False),
        sa.Column("title", sa.String(200), server_default="", nullable=False),
        sa.Column("notes", sa.Text, server_default="", nullable=False),
        sa.Column("base_currency", sa.String(8), server_default="USD", nullable=False),
        sa.Column("cell_count", sa.Integer, server_default="0", nullable=False),
        sa.Column("flagged_count", sa.Integer, server_default="0", nullable=False),
        sa.Column("last_run_id", UUID(as_uuid=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
        sa.CheckConstraint(
            "status IN ('draft','running','ready','archived','failed')",
            name="ck_revenue_models_status",
        ),
    )
    op.create_index("ix_revenue_models_owner", "revenue_models", ["owner_user_id"])
    op.create_index("ix_revenue_models_ticker", "revenue_models", ["ticker"])
    op.create_index("ix_revenue_models_industry", "revenue_models", ["industry"])
    op.create_index("ix_revenue_models_status", "revenue_models", ["status"])

    # ── provenance_traces ───────────────────────────────────────
    op.create_table(
        "provenance_traces",
        sa.Column("id", UUID(as_uuid=True), primary_key=True,
                  server_default=sa.text("gen_random_uuid()")),
        sa.Column("model_id", UUID(as_uuid=True),
                  sa.ForeignKey("revenue_models.id", ondelete="CASCADE"),
                  nullable=False),
        sa.Column("cell_path", sa.String(500), nullable=True),
        sa.Column("step_id", sa.String(80), nullable=True),
        sa.Column("steps", JSONB, server_default="[]", nullable=False),
        sa.Column("raw_evidence", JSONB, server_default="[]", nullable=False),
        sa.Column("total_tokens", sa.Integer, server_default="0", nullable=False),
        sa.Column("total_latency_ms", sa.Integer, server_default="0", nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
    )
    op.create_index("ix_provenance_model", "provenance_traces", ["model_id"])
    op.create_index("ix_provenance_cell_path", "provenance_traces", ["cell_path"])

    # ── model_cells ─────────────────────────────────────────────
    op.create_table(
        "model_cells",
        sa.Column("id", UUID(as_uuid=True), primary_key=True,
                  server_default=sa.text("gen_random_uuid()")),
        sa.Column("model_id", UUID(as_uuid=True),
                  sa.ForeignKey("revenue_models.id", ondelete="CASCADE"),
                  nullable=False),
        sa.Column("path", sa.String(500), nullable=False),
        sa.Column("label", sa.String(200), server_default="", nullable=False),
        sa.Column("period", sa.String(20), server_default="", nullable=False),
        sa.Column("unit", sa.String(40), server_default="", nullable=False),
        sa.Column("value", sa.Float, nullable=True),
        sa.Column("value_text", sa.Text, nullable=True),
        sa.Column("formula", sa.Text, nullable=True),
        sa.Column("depends_on", JSONB, server_default="[]", nullable=False),
        sa.Column("value_type", sa.String(20), server_default="number", nullable=False),
        sa.Column("source_type", sa.String(20), server_default="assumption", nullable=False),
        sa.Column("confidence", sa.String(10), server_default="MEDIUM", nullable=False),
        sa.Column("confidence_reason", sa.Text, server_default="", nullable=False),
        sa.Column("citations", JSONB, server_default="[]", nullable=False),
        sa.Column("notes", sa.Text, server_default="", nullable=False),
        sa.Column("alternative_values", JSONB, server_default="[]", nullable=False),
        sa.Column("provenance_trace_id", UUID(as_uuid=True),
                  sa.ForeignKey("provenance_traces.id", ondelete="SET NULL"),
                  nullable=True),
        sa.Column("locked_by_human", sa.Boolean, server_default="false", nullable=False),
        sa.Column("human_override", sa.Boolean, server_default="false", nullable=False),
        sa.Column("review_status", sa.String(20), server_default="pending", nullable=False),
        sa.Column("extra", JSONB, server_default="{}", nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
        sa.UniqueConstraint("model_id", "path", name="uq_model_cell_path"),
        sa.CheckConstraint(
            "source_type IN ('historical','guidance','expert','inferred','assumption','derived')",
            name="ck_model_cells_source_type",
        ),
        sa.CheckConstraint(
            "confidence IN ('HIGH','MEDIUM','LOW')",
            name="ck_model_cells_confidence",
        ),
        sa.CheckConstraint(
            "value_type IN ('number','percent','currency','count','text')",
            name="ck_model_cells_value_type",
        ),
        sa.CheckConstraint(
            "review_status IN ('pending','approved','flagged')",
            name="ck_model_cells_review_status",
        ),
    )
    op.create_index("ix_model_cells_model_id", "model_cells", ["model_id"])
    op.create_index("ix_model_cells_path", "model_cells", ["path"])
    op.create_index("ix_model_cells_review", "model_cells", ["review_status"])

    # ── model_cell_versions ─────────────────────────────────────
    op.create_table(
        "model_cell_versions",
        sa.Column("id", UUID(as_uuid=True), primary_key=True,
                  server_default=sa.text("gen_random_uuid()")),
        sa.Column("cell_id", UUID(as_uuid=True),
                  sa.ForeignKey("model_cells.id", ondelete="CASCADE"),
                  nullable=False),
        sa.Column("model_id", UUID(as_uuid=True),
                  sa.ForeignKey("revenue_models.id", ondelete="CASCADE"),
                  nullable=False),
        sa.Column("value", sa.Float, nullable=True),
        sa.Column("value_text", sa.Text, nullable=True),
        sa.Column("formula", sa.Text, nullable=True),
        sa.Column("source_type", sa.String(20), server_default="assumption", nullable=False),
        sa.Column("confidence", sa.String(10), server_default="MEDIUM", nullable=False),
        sa.Column("edited_by", UUID(as_uuid=True), nullable=True),
        sa.Column("edit_reason", sa.Text, server_default="", nullable=False),
        sa.Column("snapshot", JSONB, server_default="{}", nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
    )
    op.create_index("ix_cell_versions_cell", "model_cell_versions",
                    ["cell_id", "created_at"])
    op.create_index("ix_cell_versions_model", "model_cell_versions", ["model_id"])

    # ── debate_opinions ─────────────────────────────────────────
    op.create_table(
        "debate_opinions",
        sa.Column("id", UUID(as_uuid=True), primary_key=True,
                  server_default=sa.text("gen_random_uuid()")),
        sa.Column("model_id", UUID(as_uuid=True),
                  sa.ForeignKey("revenue_models.id", ondelete="CASCADE"),
                  nullable=False),
        sa.Column("cell_id", UUID(as_uuid=True),
                  sa.ForeignKey("model_cells.id", ondelete="CASCADE"),
                  nullable=False),
        sa.Column("model_key", sa.String(80), nullable=False),
        sa.Column("role", sa.String(20), nullable=False),
        sa.Column("value", sa.Float, nullable=True),
        sa.Column("reasoning", sa.Text, server_default="", nullable=False),
        sa.Column("citations", JSONB, server_default="[]", nullable=False),
        sa.Column("confidence", sa.String(10), server_default="MEDIUM", nullable=False),
        sa.Column("tokens_used", sa.Integer, server_default="0", nullable=False),
        sa.Column("latency_ms", sa.Integer, server_default="0", nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
        sa.CheckConstraint(
            "role IN ('drafter','verifier','tiebreaker')",
            name="ck_debate_role",
        ),
    )
    op.create_index("ix_debate_cell", "debate_opinions", ["cell_id"])
    op.create_index("ix_debate_model", "debate_opinions", ["model_id"])

    # ── sanity_issues ───────────────────────────────────────────
    op.create_table(
        "sanity_issues",
        sa.Column("id", UUID(as_uuid=True), primary_key=True,
                  server_default=sa.text("gen_random_uuid()")),
        sa.Column("model_id", UUID(as_uuid=True),
                  sa.ForeignKey("revenue_models.id", ondelete="CASCADE"),
                  nullable=False),
        sa.Column("issue_type", sa.String(50), nullable=False),
        sa.Column("severity", sa.String(10), nullable=False),
        sa.Column("cell_paths", JSONB, server_default="[]", nullable=False),
        sa.Column("message", sa.Text, server_default="", nullable=False),
        sa.Column("suggested_fix", sa.Text, server_default="", nullable=False),
        sa.Column("details", JSONB, server_default="{}", nullable=False),
        sa.Column("resolved", sa.Boolean, server_default="false", nullable=False),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
        sa.CheckConstraint(
            "severity IN ('info','warn','error')",
            name="ck_sanity_severity",
        ),
    )
    op.create_index("ix_sanity_model_severity", "sanity_issues",
                    ["model_id", "severity"])
    op.create_index("ix_sanity_type", "sanity_issues", ["issue_type"])

    # ── recipe_runs ─────────────────────────────────────────────
    op.create_table(
        "recipe_runs",
        sa.Column("id", UUID(as_uuid=True), primary_key=True,
                  server_default=sa.text("gen_random_uuid()")),
        sa.Column("recipe_id", UUID(as_uuid=True),
                  sa.ForeignKey("recipes.id", ondelete="CASCADE"),
                  nullable=False),
        sa.Column("recipe_version", sa.Integer, server_default="1", nullable=False),
        sa.Column("model_id", UUID(as_uuid=True),
                  sa.ForeignKey("revenue_models.id", ondelete="CASCADE"),
                  nullable=False),
        sa.Column("ticker", sa.String(40), nullable=False),
        sa.Column("started_by", UUID(as_uuid=True),
                  sa.ForeignKey("users.id", ondelete="SET NULL"), nullable=True),
        sa.Column("status", sa.String(20), server_default="pending", nullable=False),
        sa.Column("current_step_id", sa.String(80), nullable=True),
        sa.Column("step_results", JSONB, server_default="{}", nullable=False),
        sa.Column("total_tokens", sa.Integer, server_default="0", nullable=False),
        sa.Column("total_cost_usd", sa.Float, server_default="0.0", nullable=False),
        sa.Column("error", sa.Text, nullable=True),
        sa.Column("settings", JSONB, server_default="{}", nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "status IN ('pending','running','paused_for_human','completed','failed','cancelled')",
            name="ck_recipe_runs_status",
        ),
    )
    op.create_index("ix_recipe_runs_recipe", "recipe_runs", ["recipe_id"])
    op.create_index("ix_recipe_runs_model", "recipe_runs", ["model_id"])
    op.create_index("ix_recipe_runs_status", "recipe_runs", ["status"])
    op.create_index("ix_recipe_runs_started_by", "recipe_runs", ["started_by"])

    # ── user_feedback_events ────────────────────────────────────
    op.create_table(
        "user_feedback_events",
        sa.Column("id", UUID(as_uuid=True), primary_key=True,
                  server_default=sa.text("gen_random_uuid()")),
        sa.Column("user_id", UUID(as_uuid=True),
                  sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False),
        sa.Column("event_type", sa.String(40), nullable=False),
        sa.Column("model_id", UUID(as_uuid=True), nullable=True),
        sa.Column("cell_id", UUID(as_uuid=True), nullable=True),
        sa.Column("recipe_id", UUID(as_uuid=True), nullable=True),
        sa.Column("industry", sa.String(80), nullable=True),
        sa.Column("cell_path", sa.String(500), nullable=True),
        sa.Column("payload", JSONB, server_default="{}", nullable=False),
        sa.Column("consumed", sa.Boolean, server_default="false", nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
    )
    op.create_index("ix_feedback_user", "user_feedback_events", ["user_id"])
    op.create_index("ix_feedback_industry", "user_feedback_events", ["industry"])
    op.create_index("ix_feedback_event_type", "user_feedback_events", ["event_type"])
    op.create_index("ix_feedback_consumed", "user_feedback_events", ["consumed"])
    op.create_index("ix_feedback_created_at", "user_feedback_events", ["created_at"])

    # ── pending_lessons ─────────────────────────────────────────
    op.create_table(
        "pending_lessons",
        sa.Column("id", UUID(as_uuid=True), primary_key=True,
                  server_default=sa.text("gen_random_uuid()")),
        sa.Column("industry", sa.String(80), nullable=False),
        sa.Column("lesson_id", sa.String(60), nullable=False, unique=True),
        sa.Column("title", sa.String(200), nullable=False),
        sa.Column("body", sa.Text, nullable=False),
        sa.Column("scenario", sa.Text, server_default="", nullable=False),
        sa.Column("observation", sa.Text, server_default="", nullable=False),
        sa.Column("rule", sa.Text, server_default="", nullable=False),
        sa.Column("sources", JSONB, server_default="[]", nullable=False),
        sa.Column("status", sa.String(20), server_default="pending", nullable=False),
        sa.Column("reviewed_by", UUID(as_uuid=True),
                  sa.ForeignKey("users.id", ondelete="SET NULL"), nullable=True),
        sa.Column("review_note", sa.Text, server_default="", nullable=False),
        sa.Column("batch_week", sa.String(20), server_default="", nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
        sa.Column("reviewed_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "status IN ('pending','approved','rejected','archived')",
            name="ck_pending_lessons_status",
        ),
    )
    op.create_index("ix_pending_lessons_status", "pending_lessons", ["status"])
    op.create_index("ix_pending_lessons_industry", "pending_lessons", ["industry"])
    op.create_index("ix_pending_lessons_batch_week", "pending_lessons", ["batch_week"])


def downgrade() -> None:
    op.drop_table("pending_lessons")
    op.drop_table("user_feedback_events")
    op.drop_table("recipe_runs")
    op.drop_table("sanity_issues")
    op.drop_table("debate_opinions")
    op.drop_table("model_cell_versions")
    op.drop_table("model_cells")
    op.drop_table("provenance_traces")
    op.drop_table("revenue_models")
    op.drop_table("recipes")
