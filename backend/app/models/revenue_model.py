"""Database models for the Revenue Modeling system.

Core domain:

* ``RevenueModel``   — a single run of revenue decomposition for a company
                       over a set of fiscal periods (e.g. LITE FY25E/26E/27E).
* ``ModelCell``      — the atomic unit: one number, one formula, one
                       assumption. Every cell carries provenance, citations,
                       confidence, and a source-type label.
* ``ModelCellVersion`` — complete edit history (agent + human) for audit.
* ``ProvenanceTrace`` — the agent's reasoning chain for how a value was
                       produced (tools called, LLM decisions).
* ``DebateOpinion``  — per-cell opinions from the 3-way LLM debate.
* ``SanityIssue``    — numerical sanity check findings.
"""
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


class RevenueModel(Base):
    """One revenue-breakdown modeling exercise for a single ticker."""
    __tablename__ = "revenue_models"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    ticker: Mapped[str] = mapped_column(String(40), nullable=False)
    company_name: Mapped[str] = mapped_column(String(200), nullable=False)
    industry: Mapped[str] = mapped_column(String(80), nullable=False)
    fiscal_periods: Mapped[list[str]] = mapped_column(JSONB, nullable=False, default=list)
    # Recipe that produced this model (nullable for blank models)
    recipe_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("recipes.id", ondelete="SET NULL"), nullable=True,
    )
    recipe_version: Mapped[int | None] = mapped_column(Integer, nullable=True)
    # draft / running / ready / archived
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="draft")
    # Link back to chat conversation that created this model (optional)
    conversation_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), nullable=True,
    )
    owner_user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False,
    )
    # Free-form title; default to company+periods
    title: Mapped[str] = mapped_column(String(200), nullable=False, default="")
    notes: Mapped[str] = mapped_column(Text, nullable=False, default="")
    # Currency for monetary cells (USD/CNY/HKD...). Unit pack can override per cell.
    base_currency: Mapped[str] = mapped_column(String(8), nullable=False, default="USD")
    # Aggregated stats (denormalized — updated by recipe engine/sanity)
    cell_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    flagged_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_run_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    # Hallucination-guard circuit breaker: True when auto-paused by the weekly
    # citation-audit review. UI must strip the "ready" badge when set. Does
    # NOT touch ``status`` so the run history + re-run semantics stay intact.
    paused_by_guard: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    paused_reason: Mapped[str | None] = mapped_column(String(200), nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, server_default=text("now()"),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, server_default=text("now()"),
        onupdate=utcnow,
    )

    cells = relationship(
        "ModelCell", back_populates="model", cascade="all, delete-orphan",
        lazy="noload",
    )

    __table_args__ = (
        Index("ix_revenue_models_owner", "owner_user_id"),
        Index("ix_revenue_models_ticker", "ticker"),
        Index("ix_revenue_models_industry", "industry"),
        Index("ix_revenue_models_status", "status"),
        CheckConstraint(
            "status IN ('draft','running','ready','archived','failed')",
            name="ck_revenue_models_status",
        ),
    )


class ModelCell(Base):
    """One modeling cell — number, formula, assumption, or derived value."""
    __tablename__ = "model_cells"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    model_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("revenue_models.id", ondelete="CASCADE"),
        nullable=False,
    )
    # Dot-path identifier, e.g. "segment.HDD.Nearline.volume.FY26"
    path: Mapped[str] = mapped_column(String(500), nullable=False)
    label: Mapped[str] = mapped_column(String(200), nullable=False, default="")
    period: Mapped[str] = mapped_column(String(20), nullable=False, default="")
    unit: Mapped[str] = mapped_column(String(40), nullable=False, default="")
    # Final display value (computed for formula cells)
    value: Mapped[float | None] = mapped_column(Float, nullable=True)
    # Raw string value (for text-typed cells or display-only versions)
    value_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    # Formula like "=segment.HDD.rev.FY26 * segment.HDD.margin.FY26"; None = hard-coded
    formula: Mapped[str | None] = mapped_column(Text, nullable=True)
    # List of cell paths this formula references (for dependency graph)
    depends_on: Mapped[list[str]] = mapped_column(JSONB, nullable=False, default=list)
    # number / percent / currency / count / text
    value_type: Mapped[str] = mapped_column(String(20), nullable=False, default="number")
    # historical / guidance / expert / inferred / assumption / derived
    source_type: Mapped[str] = mapped_column(String(20), nullable=False, default="assumption")
    # HIGH / MEDIUM / LOW
    confidence: Mapped[str] = mapped_column(String(10), nullable=False, default="MEDIUM")
    confidence_reason: Mapped[str] = mapped_column(Text, nullable=False, default="")
    # [{index:1, source_id:..., url:..., title:..., snippet:..., date:..., tool:..., page:...}]
    citations: Mapped[list[dict]] = mapped_column(JSONB, nullable=False, default=list)
    notes: Mapped[str] = mapped_column(Text, nullable=False, default="")
    # Parallel alternative sources: [{value:..., source:..., citation_idx:..., label:...}]
    alternative_values: Mapped[list[dict]] = mapped_column(JSONB, nullable=False, default=list)
    provenance_trace_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("provenance_traces.id", ondelete="SET NULL"),
        nullable=True,
    )
    # Once locked, recipe re-runs do not overwrite
    locked_by_human: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    human_override: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    # pending / approved / flagged
    review_status: Mapped[str] = mapped_column(String(20), nullable=False, default="pending")
    # Extra JSONB slot for pack-specific metadata (growth_profile, price_driver...)
    extra: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, server_default=text("now()"),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, server_default=text("now()"),
        onupdate=utcnow,
    )

    model = relationship("RevenueModel", back_populates="cells", lazy="noload")

    __table_args__ = (
        UniqueConstraint("model_id", "path", name="uq_model_cell_path"),
        Index("ix_model_cells_model_id", "model_id"),
        Index("ix_model_cells_path", "path"),
        Index("ix_model_cells_review", "review_status"),
        CheckConstraint(
            "source_type IN ('historical','guidance','expert','inferred','assumption','derived')",
            name="ck_model_cells_source_type",
        ),
        CheckConstraint(
            "confidence IN ('HIGH','MEDIUM','LOW')",
            name="ck_model_cells_confidence",
        ),
        CheckConstraint(
            "value_type IN ('number','percent','currency','count','text')",
            name="ck_model_cells_value_type",
        ),
        CheckConstraint(
            "review_status IN ('pending','approved','flagged')",
            name="ck_model_cells_review_status",
        ),
    )


class ModelCellVersion(Base):
    """Full edit history for a cell (agent writes + human overrides)."""
    __tablename__ = "model_cell_versions"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    cell_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("model_cells.id", ondelete="CASCADE"),
        nullable=False,
    )
    model_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("revenue_models.id", ondelete="CASCADE"),
        nullable=False,
    )
    value: Mapped[float | None] = mapped_column(Float, nullable=True)
    value_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    formula: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_type: Mapped[str] = mapped_column(String(20), nullable=False, default="assumption")
    confidence: Mapped[str] = mapped_column(String(10), nullable=False, default="MEDIUM")
    # null = agent
    edited_by: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    edit_reason: Mapped[str] = mapped_column(Text, nullable=False, default="")
    # Snapshot of citations / notes / alternatives at this point
    snapshot: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, server_default=text("now()"),
    )

    __table_args__ = (
        Index("ix_cell_versions_cell", "cell_id", "created_at"),
        Index("ix_cell_versions_model", "model_id"),
    )


class ProvenanceTrace(Base):
    """Full agent reasoning chain that produced a cell value."""
    __tablename__ = "provenance_traces"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    # Nullable to allow multi-cell traces (e.g. DECOMPOSE_SEGMENTS produces many cells)
    model_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("revenue_models.id", ondelete="CASCADE"),
        nullable=False,
    )
    cell_path: Mapped[str | None] = mapped_column(String(500), nullable=True)
    step_id: Mapped[str | None] = mapped_column(String(80), nullable=True)
    # List of {step_type, tool, query, result_preview, llm_reasoning, tokens, latency}
    steps: Mapped[list[dict]] = mapped_column(JSONB, nullable=False, default=list)
    # Frozen evidence snapshot (URL, title, snippet) so source changes don't break audit
    raw_evidence: Mapped[list[dict]] = mapped_column(JSONB, nullable=False, default=list)
    total_tokens: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    total_latency_ms: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, server_default=text("now()"),
    )

    __table_args__ = (
        Index("ix_provenance_model", "model_id"),
        Index("ix_provenance_cell_path", "cell_path"),
    )


class DebateOpinion(Base):
    """Per-cell opinion from the 3-way LLM debate (Opus/Gemini/GPT)."""
    __tablename__ = "debate_opinions"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    model_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("revenue_models.id", ondelete="CASCADE"),
        nullable=False,
    )
    cell_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("model_cells.id", ondelete="CASCADE"),
        nullable=False,
    )
    # anthropic/claude-opus-4-7 / google/gemini-3.1-pro / openai/gpt-5.4
    model_key: Mapped[str] = mapped_column(String(80), nullable=False)
    # drafter / verifier / tiebreaker
    role: Mapped[str] = mapped_column(String(20), nullable=False)
    value: Mapped[float | None] = mapped_column(Float, nullable=True)
    reasoning: Mapped[str] = mapped_column(Text, nullable=False, default="")
    citations: Mapped[list[dict]] = mapped_column(JSONB, nullable=False, default=list)
    confidence: Mapped[str] = mapped_column(String(10), nullable=False, default="MEDIUM")
    tokens_used: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    latency_ms: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, server_default=text("now()"),
    )

    __table_args__ = (
        Index("ix_debate_cell", "cell_id"),
        Index("ix_debate_model", "model_id"),
        CheckConstraint(
            "role IN ('drafter','verifier','tiebreaker')",
            name="ck_debate_role",
        ),
    )


class SanityIssue(Base):
    """Numerical / structural sanity check finding."""
    __tablename__ = "sanity_issues"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    model_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("revenue_models.id", ondelete="CASCADE"),
        nullable=False,
    )
    # sum_mismatch / yoy_out_of_range / margin_out_of_range / unit_mismatch /
    # cycle / unknown_dep / div0 / outlier / peer_sanity
    issue_type: Mapped[str] = mapped_column(String(50), nullable=False)
    severity: Mapped[str] = mapped_column(String(10), nullable=False)  # info/warn/error
    cell_paths: Mapped[list[str]] = mapped_column(JSONB, nullable=False, default=list)
    message: Mapped[str] = mapped_column(Text, nullable=False, default="")
    suggested_fix: Mapped[str] = mapped_column(Text, nullable=False, default="")
    details: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    resolved: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, server_default=text("now()"),
    )

    __table_args__ = (
        Index("ix_sanity_model_severity", "model_id", "severity"),
        Index("ix_sanity_type", "issue_type"),
        CheckConstraint(
            "severity IN ('info','warn','error')",
            name="ck_sanity_severity",
        ),
    )
