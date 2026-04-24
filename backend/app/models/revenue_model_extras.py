"""Extra tables around RevenueModel — collaboration, backtest, expert calls."""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    Boolean, CheckConstraint, DateTime, Float, ForeignKey, Index, Integer,
    String, Text, UniqueConstraint, text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from backend.app.core.database import Base


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class RevenueModelBacktest(Base):
    """Historical accuracy of a revenue-model prediction once actuals land."""
    __tablename__ = "revenue_model_backtest"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    model_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("revenue_models.id", ondelete="CASCADE"),
        nullable=False,
    )
    cell_path: Mapped[str] = mapped_column(String(500), nullable=False)
    period: Mapped[str] = mapped_column(String(20), nullable=False)
    predicted_value: Mapped[float | None] = mapped_column(Float, nullable=True)
    predicted_confidence: Mapped[str] = mapped_column(String(10), nullable=False, default="MEDIUM")
    actual_value: Mapped[float | None] = mapped_column(Float, nullable=True)
    abs_error: Mapped[float | None] = mapped_column(Float, nullable=True)
    pct_error: Mapped[float | None] = mapped_column(Float, nullable=True)
    actual_source: Mapped[str] = mapped_column(String(200), nullable=False, default="")
    actual_reported_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    prediction_made_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    details: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("now()"), default=utcnow,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("now()"), default=utcnow,
        onupdate=utcnow,
    )

    __table_args__ = (
        Index("ix_backtest_model", "model_id"),
        Index("ix_backtest_path_period", "cell_path", "period"),
        Index("ix_backtest_confidence", "predicted_confidence"),
    )


class ExpertCallRequest(Base):
    """VERIFY_AND_ASK produces these when the model detects missing evidence."""
    __tablename__ = "expert_call_requests"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    model_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("revenue_models.id", ondelete="CASCADE"),
        nullable=False,
    )
    cell_path: Mapped[str | None] = mapped_column(String(500), nullable=True)
    ticker: Mapped[str] = mapped_column(String(40), nullable=False)
    topic: Mapped[str] = mapped_column(Text, nullable=False, default="")
    questions: Mapped[list[str]] = mapped_column(JSONB, nullable=False, default=list)
    rationale: Mapped[str] = mapped_column(Text, nullable=False, default="")
    # open / scheduled / completed / cancelled
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="open")
    requested_by: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="SET NULL"), nullable=True,
    )
    assigned_to: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    interview_doc_id: Mapped[str | None] = mapped_column(String(200), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("now()"), default=utcnow,
    )
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        Index("ix_expert_call_model", "model_id"),
        Index("ix_expert_call_status", "status"),
        CheckConstraint(
            "status IN ('open','scheduled','completed','cancelled')",
            name="ck_expert_call_status",
        ),
    )


class ModelComment(Base):
    """Threaded comments on a model / cell."""
    __tablename__ = "model_comments"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    model_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("revenue_models.id", ondelete="CASCADE"),
        nullable=False,
    )
    cell_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("model_cells.id", ondelete="CASCADE"),
        nullable=True,
    )
    author_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
    )
    mentions: Mapped[list[str]] = mapped_column(JSONB, nullable=False, default=list)
    body: Mapped[str] = mapped_column(Text, nullable=False, default="")
    resolved: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("now()"), default=utcnow,
    )

    __table_args__ = (
        Index("ix_model_comments_model", "model_id"),
        Index("ix_model_comments_cell", "cell_id"),
    )


class ModelCollaborator(Base):
    """Grants a user access to a model with a specific role."""
    __tablename__ = "model_collaborators"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    model_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("revenue_models.id", ondelete="CASCADE"),
        nullable=False,
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
    )
    # viewer / editor / admin
    role: Mapped[str] = mapped_column(String(20), nullable=False, default="viewer")
    added_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("now()"), default=utcnow,
    )

    __table_args__ = (
        UniqueConstraint("model_id", "user_id", name="uq_model_collab"),
        CheckConstraint(
            "role IN ('viewer','editor','admin')",
            name="ck_model_collab_role",
        ),
    )
