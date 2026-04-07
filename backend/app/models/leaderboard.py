"""Signal evaluation and source leaderboard models."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import text, String, Text, Boolean, Integer, Float, DateTime, Index
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from backend.app.core.database import Base


def utcnow():
    return datetime.now(timezone.utc)


class SignalEvaluation(Base):
    """Stores per-signal accuracy evaluations comparing predicted sentiment
    against actual stock price movements across multiple time horizons."""

    __tablename__ = "signal_evaluations"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    news_item_id: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    source_name: Mapped[str] = mapped_column(String(200), nullable=False, index=True)
    category: Mapped[str] = mapped_column(String(50), default="", server_default="")
    ticker: Mapped[str] = mapped_column(String(20), nullable=False)
    market: Mapped[str] = mapped_column(String(20), nullable=False)

    # Signal timestamp in UTC
    signal_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    predicted_sentiment: Mapped[str] = mapped_column(String(20), nullable=False)
    # Per-horizon predicted sentiments (categorical)
    predicted_sentiment_t1: Mapped[str | None] = mapped_column(String(20), nullable=True)
    predicted_sentiment_t5: Mapped[str | None] = mapped_column(String(20), nullable=True)
    predicted_sentiment_t20: Mapped[str | None] = mapped_column(String(20), nullable=True)
    # Per-horizon numerical scores (continuous alpha factor, [-1.0, +1.0])
    sentiment_score_t1: Mapped[float | None] = mapped_column(Float, nullable=True)
    sentiment_score_t5: Mapped[float | None] = mapped_column(Float, nullable=True)
    sentiment_score_t20: Mapped[float | None] = mapped_column(Float, nullable=True)
    # Per-horizon confidence ([0.0, 1.0])
    confidence_t1: Mapped[float | None] = mapped_column(Float, nullable=True)
    confidence_t5: Mapped[float | None] = mapped_column(Float, nullable=True)
    confidence_t20: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Price at signal time (closing price on signal date)
    price_at_signal: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Returns at each time horizon (fractional, e.g. 0.02 = +2%)
    return_t0: Mapped[float | None] = mapped_column(Float, nullable=True)
    return_t1: Mapped[float | None] = mapped_column(Float, nullable=True)
    return_t5: Mapped[float | None] = mapped_column(Float, nullable=True)
    return_t20: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Whether the prediction was correct at each horizon
    correct_t0: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    correct_t1: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    correct_t5: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    correct_t20: Mapped[bool | None] = mapped_column(Boolean, nullable=True)

    evaluated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, server_default=text("now()"),
    )

    __table_args__ = (
        Index("ix_signal_eval_source_time", "source_name", "signal_time"),
        Index("ix_signal_eval_ticker_time", "ticker", "signal_time"),
    )

    def __repr__(self) -> str:
        return f"<SignalEvaluation {self.source_name} {self.ticker} {self.predicted_sentiment}>"
