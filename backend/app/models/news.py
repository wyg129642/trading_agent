from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import String, Text, Boolean, Integer, Float, DateTime, ForeignKey, Index, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from backend.app.core.database import Base


def utcnow():
    return datetime.now(timezone.utc)


class NewsItem(Base):
    __tablename__ = "news_items"

    id: Mapped[str] = mapped_column(Text, primary_key=True)
    source_name: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    url: Mapped[str] = mapped_column(Text, nullable=False)
    content: Mapped[str] = mapped_column(Text, default="", server_default="")
    content_hash: Mapped[str] = mapped_column(
        String(64), unique=True, nullable=False, index=True,
    )
    published_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True,
    )
    fetched_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True,
    )
    language: Mapped[str] = mapped_column(String(5), default="zh", server_default="zh")
    market: Mapped[str] = mapped_column(String(20), default="china", server_default="china")
    metadata_: Mapped[dict] = mapped_column(
        "metadata", JSONB, default=dict, server_default="{}",
    )

    # Relationships
    filter_result: Mapped[FilterResult | None] = relationship(
        "FilterResult", back_populates="news_item", uselist=False, cascade="all, delete-orphan",
    )
    analysis_result: Mapped[AnalysisResult | None] = relationship(
        "AnalysisResult", back_populates="news_item", uselist=False, cascade="all, delete-orphan",
    )
    research_report: Mapped[ResearchReport | None] = relationship(
        "ResearchReport", back_populates="news_item", uselist=False, cascade="all, delete-orphan",
    )

    def __repr__(self) -> str:
        return f"<NewsItem {self.id} {self.title[:40]}>"


class FilterResult(Base):
    __tablename__ = "filter_results"

    news_item_id: Mapped[str] = mapped_column(
        Text, ForeignKey("news_items.id", ondelete="CASCADE"), primary_key=True,
    )
    is_relevant: Mapped[bool] = mapped_column(Boolean, nullable=False)
    relevance_score: Mapped[float] = mapped_column(Float, nullable=False)
    reason: Mapped[str] = mapped_column(Text, default="", server_default="")
    filtered_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
    )

    # Relationships
    news_item: Mapped[NewsItem] = relationship(
        "NewsItem", back_populates="filter_result",
    )

    def __repr__(self) -> str:
        return f"<FilterResult {self.news_item_id} relevant={self.is_relevant}>"


class AnalysisResult(Base):
    __tablename__ = "analysis_results"

    news_item_id: Mapped[str] = mapped_column(
        Text, ForeignKey("news_items.id", ondelete="CASCADE"), primary_key=True,
    )
    sentiment: Mapped[str] = mapped_column(
        String(20), default="neutral", server_default="neutral",
    )
    impact_magnitude: Mapped[str] = mapped_column(
        String(20), default="low", server_default="low",
    )
    impact_timeframe: Mapped[str] = mapped_column(
        String(20), default="short_term", server_default="short_term",
    )
    affected_tickers: Mapped[list] = mapped_column(
        JSONB, default=list, server_default="[]",
    )
    affected_sectors: Mapped[list] = mapped_column(
        JSONB, default=list, server_default="[]",
    )
    category: Mapped[str] = mapped_column(
        String(50), default="other", server_default="other",
    )
    summary: Mapped[str] = mapped_column(Text, default="", server_default="")
    key_facts: Mapped[list] = mapped_column(
        JSONB, default=list, server_default="[]",
    )
    bull_case: Mapped[str] = mapped_column(Text, default="", server_default="")
    bear_case: Mapped[str] = mapped_column(Text, default="", server_default="")
    requires_deep_research: Mapped[bool] = mapped_column(
        Boolean, default=False, server_default="false",
    )
    research_questions: Mapped[list] = mapped_column(
        JSONB, default=list, server_default="[]",
    )
    analyzed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
    )
    model_used: Mapped[str] = mapped_column(
        String(100), default="", server_default="",
    )
    surprise_factor: Mapped[float] = mapped_column(
        Float, default=0.5, server_default="0.5",
    )
    is_routine: Mapped[bool] = mapped_column(
        Boolean, default=False, server_default="false",
    )
    market_expectation: Mapped[str] = mapped_column(
        Text, default="", server_default="",
    )
    quantified_evidence: Mapped[list] = mapped_column(
        JSONB, default=list, server_default="[]",
    )

    # Concept & industry tags
    concept_tags: Mapped[list] = mapped_column(
        JSONB, default=list, server_default="[]",
    )
    industry_tags: Mapped[list] = mapped_column(
        JSONB, default=list, server_default="[]",
    )

    # Per-stock sentiment: {"NVDA(英伟达)": "bearish", "AAPL(苹果)": "neutral"}
    # Falls back to global `sentiment` when empty
    ticker_sentiments: Mapped[dict] = mapped_column(
        JSONB, default=dict, server_default="{}",
    )
    # Per-sector sentiment (when no specific stock): {"半导体": "bearish"}
    sector_sentiments: Mapped[dict] = mapped_column(
        JSONB, default=dict, server_default="{}",
    )

    __table_args__ = (
        Index("ix_analysis_results_impact_magnitude", "impact_magnitude"),
    )

    # Relationships
    news_item: Mapped[NewsItem] = relationship(
        "NewsItem", back_populates="analysis_result",
    )

    def __repr__(self) -> str:
        return f"<AnalysisResult {self.news_item_id} sentiment={self.sentiment}>"


class ResearchReport(Base):
    __tablename__ = "research_reports"

    news_item_id: Mapped[str] = mapped_column(
        Text, ForeignKey("news_items.id", ondelete="CASCADE"), primary_key=True,
    )
    executive_summary: Mapped[str] = mapped_column(
        Text, default="", server_default="",
    )
    context: Mapped[str] = mapped_column(Text, default="", server_default="")
    affected_securities: Mapped[str] = mapped_column(
        Text, default="", server_default="",
    )
    historical_precedent: Mapped[str] = mapped_column(
        Text, default="", server_default="",
    )
    bull_scenario: Mapped[str] = mapped_column(Text, default="", server_default="")
    bear_scenario: Mapped[str] = mapped_column(Text, default="", server_default="")
    recommended_actions: Mapped[str] = mapped_column(
        Text, default="", server_default="",
    )
    risk_factors: Mapped[str] = mapped_column(Text, default="", server_default="")
    confidence: Mapped[float] = mapped_column(
        Float, default=0.0, server_default="0.0",
    )
    full_report: Mapped[str] = mapped_column(Text, default="", server_default="")
    market_data_snapshot: Mapped[dict] = mapped_column(
        JSONB, default=dict, server_default="{}",
    )
    deep_research_data: Mapped[dict] = mapped_column(
        JSONB, default=dict, server_default="{}",
    )
    researched_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
    )
    model_used: Mapped[str] = mapped_column(
        String(100), default="", server_default="",
    )

    # Relationships
    news_item: Mapped[NewsItem] = relationship(
        "NewsItem", back_populates="research_report",
    )

    def __repr__(self) -> str:
        return f"<ResearchReport {self.news_item_id}>"
