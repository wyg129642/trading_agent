"""Segment revenue snapshot — materialised cross-model view.

When a new RevenueModel is built for a ticker that has an existing (approved)
model for the same period, the step executors can reuse the segment
revenue extraction cells instead of re-running gather_context + decompose.

This table is the single point of truth for "latest approved value per
(ticker, period, segment_slug, metric)". It is updated by
``segment_snapshot_service.refresh_for_model`` when a model transitions to
``ready`` or when a cell is approved.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    DateTime, Float, ForeignKey, Index, String, Text,
    UniqueConstraint, text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from backend.app.core.database import Base


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class SegmentRevenueSnapshot(Base):
    """Latest approved (ticker, period, segment, metric) value across all models."""
    __tablename__ = "segment_revenue_snapshot"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    ticker: Mapped[str] = mapped_column(String(40), nullable=False)
    industry: Mapped[str] = mapped_column(String(80), nullable=False, default="")
    segment_slug: Mapped[str] = mapped_column(String(120), nullable=False)
    period: Mapped[str] = mapped_column(String(20), nullable=False)
    # revenue | volume | asp | margin | growth_rate
    metric: Mapped[str] = mapped_column(String(40), nullable=False, default="revenue")
    value: Mapped[float | None] = mapped_column(Float, nullable=True)
    unit: Mapped[str] = mapped_column(String(40), nullable=False, default="")
    confidence: Mapped[str] = mapped_column(String(10), nullable=False, default="MEDIUM")
    source_type: Mapped[str] = mapped_column(String(20), nullable=False, default="assumption")
    # Which model + cell produced this snapshot
    source_model_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("revenue_models.id", ondelete="SET NULL"),
        nullable=True,
    )
    source_cell_path: Mapped[str] = mapped_column(String(500), nullable=False, default="")
    citations: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=text("now()"), default=utcnow, onupdate=utcnow,
    )

    __table_args__ = (
        UniqueConstraint(
            "ticker", "period", "segment_slug", "metric",
            name="uq_segment_snapshot_natural_key",
        ),
        Index("ix_segment_snapshot_ticker_period", "ticker", "period"),
        Index("ix_segment_snapshot_industry", "industry"),
    )
