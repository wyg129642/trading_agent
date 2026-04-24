"""Recipe domain — the reusable, research-analyst-editable workflow definition."""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    Boolean, DateTime, Float, ForeignKey, Index, Integer, String, Text,
    UniqueConstraint, text, CheckConstraint,
)
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from backend.app.core.database import Base


def utcnow():
    return datetime.now(timezone.utc)


class Recipe(Base):
    """A reusable, versioned, editable DAG workflow.

    Research analysts can fork and edit published recipes without
    touching python. The ``graph`` JSONB is a DAG of step nodes; each
    step has a type (see step_executors) and a config (prompt template,
    tool switches, confidence threshold...).
    """
    __tablename__ = "recipes"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    slug: Mapped[str] = mapped_column(String(200), nullable=False)
    industry: Mapped[str | None] = mapped_column(String(80), nullable=True)
    description: Mapped[str] = mapped_column(Text, nullable=False, default="")
    # DAG: {"nodes":[...], "edges":[...]}; see RECIPE_SCHEMA in schemas/recipe.py
    graph: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    is_public: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    parent_recipe_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("recipes.id", ondelete="SET NULL"), nullable=True,
    )
    created_by: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="SET NULL"), nullable=True,
    )
    # Reference to an Industry Pack seed recipe, when originally loaded from disk
    pack_ref: Mapped[str | None] = mapped_column(String(200), nullable=True)
    # Tags for discovery
    tags: Mapped[list[str]] = mapped_column(JSONB, nullable=False, default=list)
    # Canonical = team-official. Only admins can edit canonical recipes;
    # researchers fork them and submit a ChangeRequest instead.
    canonical: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, server_default=text("now()"),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, server_default=text("now()"),
        onupdate=utcnow,
    )

    __table_args__ = (
        UniqueConstraint("slug", "version", name="uq_recipes_slug_version"),
        Index("ix_recipes_industry", "industry"),
        Index("ix_recipes_is_public", "is_public"),
        Index("ix_recipes_pack_ref", "pack_ref"),
    )


class RecipeRun(Base):
    """One execution of a recipe against a RevenueModel."""
    __tablename__ = "recipe_runs"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    recipe_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("recipes.id", ondelete="CASCADE"),
        nullable=False,
    )
    recipe_version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    model_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("revenue_models.id", ondelete="CASCADE"),
        nullable=False,
    )
    ticker: Mapped[str] = mapped_column(String(40), nullable=False)
    started_by: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="SET NULL"), nullable=True,
    )
    # pending / running / paused_for_human / completed / failed / cancelled
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="pending")
    current_step_id: Mapped[str | None] = mapped_column(String(80), nullable=True)
    # Per-step results: {step_id: {started_at, ended_at, status, output_paths, error}}
    step_results: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    total_tokens: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    total_cost_usd: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    # Arbitrary run-scoped settings (LLM overrides, skip_debate, dry_run...)
    settings: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    # A/B group labels for prompt experimentation ('A' / 'B'); empty if not A/B
    ab_group: Mapped[str] = mapped_column(String(20), nullable=False, default="")
    ab_session: Mapped[str] = mapped_column(String(80), nullable=False, default="")
    # Pre-flight estimate (snapshot) captured when the run was submitted
    estimated_cost_usd: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    # Hard stop — if run.total_cost_usd exceeds this, recipe_engine pauses run.
    # Null / 0 means no per-run cap (use user quota only).
    cost_cap_usd: Mapped[float | None] = mapped_column(Float, nullable=True)
    # When True, the run was paused by recipe_engine because it exceeded a
    # cost gate — stored separately from status because an operator can resume.
    paused_reason: Mapped[str | None] = mapped_column(String(60), nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, server_default=text("now()"),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, server_default=text("now()"),
        onupdate=utcnow,
    )
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        Index("ix_recipe_runs_recipe", "recipe_id"),
        Index("ix_recipe_runs_model", "model_id"),
        Index("ix_recipe_runs_status", "status"),
        Index("ix_recipe_runs_started_by", "started_by"),
        CheckConstraint(
            "status IN ('pending','running','paused_for_human','completed','failed','cancelled')",
            name="ck_recipe_runs_status",
        ),
    )
