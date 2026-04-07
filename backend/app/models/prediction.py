"""Database models for stock prediction scoring system."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    text, String, Text, Boolean, Integer, Float, DateTime,
    ForeignKey, Index,
)
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from backend.app.core.database import Base


def utcnow():
    return datetime.now(timezone.utc)


class StockPrediction(Base):
    """A stock prediction made by an analyst (or submitted on their behalf)."""
    __tablename__ = "stock_predictions"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    # The analyst whose prediction this is
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False,
    )
    # Who physically submitted it (could be admin/boss on behalf of analyst)
    submitted_by_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="SET NULL"), nullable=False,
    )
    stock_code: Mapped[str] = mapped_column(String(20), nullable=False)
    stock_name: Mapped[str] = mapped_column(String(100), nullable=False)
    market: Mapped[str] = mapped_column(String(10), nullable=False)  # A股/港股/美股
    direction: Mapped[str] = mapped_column(String(10), nullable=False)  # bullish/bearish
    time_horizon: Mapped[str] = mapped_column(String(10), nullable=False)  # 1w/2w/1m/3m/6m
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    confidence: Mapped[int] = mapped_column(Integer, default=3)  # 1-5 scale
    price_at_submit: Mapped[float | None] = mapped_column(Float, nullable=True)
    target_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    # Status: pending (waiting for price), active (in evaluation window),
    #         expired (window ended, awaiting backtest), evaluated (scored)
    status: Mapped[str] = mapped_column(
        String(20), default="active", server_default="active",
    )
    expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, server_default=text("now()"),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, server_default=text("now()"),
        onupdate=utcnow,
    )

    # Relationships
    user = relationship("User", foreign_keys=[user_id], lazy="joined")
    submitted_by = relationship("User", foreign_keys=[submitted_by_id], lazy="joined")
    edit_logs: Mapped[list[PredictionEditLog]] = relationship(
        "PredictionEditLog", back_populates="prediction", cascade="all, delete-orphan",
        order_by="PredictionEditLog.edited_at.desc()",
    )
    evaluation: Mapped[PredictionEvaluation | None] = relationship(
        "PredictionEvaluation", back_populates="prediction", uselist=False,
        cascade="all, delete-orphan",
    )

    __table_args__ = (
        Index("ix_predictions_user_id", "user_id"),
        Index("ix_predictions_status", "status"),
        Index("ix_predictions_created_at", "created_at"),
        Index("ix_predictions_stock_code", "stock_code"),
        Index("ix_predictions_expires_at", "expires_at"),
    )

    def __repr__(self) -> str:
        return f"<StockPrediction {self.stock_code} {self.direction} by user={self.user_id}>"


class PredictionEditLog(Base):
    """Audit trail for prediction edits."""
    __tablename__ = "prediction_edit_logs"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    prediction_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("stock_predictions.id", ondelete="CASCADE"),
        nullable=False,
    )
    edited_by_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="SET NULL"), nullable=False,
    )
    field_changed: Mapped[str] = mapped_column(String(50), nullable=False)
    old_value: Mapped[str | None] = mapped_column(Text, nullable=True)
    new_value: Mapped[str | None] = mapped_column(Text, nullable=True)
    edited_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, server_default=text("now()"),
    )

    prediction: Mapped[StockPrediction] = relationship(
        "StockPrediction", back_populates="edit_logs",
    )
    edited_by = relationship("User", foreign_keys=[edited_by_id], lazy="joined")

    __table_args__ = (
        Index("ix_edit_logs_prediction_id", "prediction_id"),
    )


class PredictionEvaluation(Base):
    """Backtest evaluation result for a single prediction."""
    __tablename__ = "prediction_evaluations"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    prediction_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("stock_predictions.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
    )
    price_at_end: Mapped[float | None] = mapped_column(Float, nullable=True)
    return_pct: Mapped[float | None] = mapped_column(Float, nullable=True)
    # Was the direction prediction correct?
    is_direction_correct: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    # Composite score for this prediction (0-100)
    score: Mapped[float | None] = mapped_column(Float, nullable=True)
    # Max favorable move during the prediction window
    max_favorable_pct: Mapped[float | None] = mapped_column(Float, nullable=True)
    # Max adverse move during the prediction window
    max_adverse_pct: Mapped[float | None] = mapped_column(Float, nullable=True)
    # Price data snapshot for charting (JSON array of {date, close})
    price_series: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    evaluated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, server_default=text("now()"),
    )

    prediction: Mapped[StockPrediction] = relationship(
        "StockPrediction", back_populates="evaluation",
    )

    __table_args__ = (
        Index("ix_evaluations_prediction_id", "prediction_id"),
    )
