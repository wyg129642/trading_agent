"""Pydantic schemas for news feed and analysis data."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class NewsItemBrief(BaseModel):
    """News item as shown in the feed list."""
    id: str
    source_name: str
    title: str
    title_zh: str | None = None  # Chinese translation of title (from LLM)
    url: str
    published_at: datetime | None
    fetched_at: datetime
    language: str
    market: str
    # Analysis summary (joined)
    sentiment: str | None = None
    impact_magnitude: str | None = None
    surprise_factor: float | None = None
    affected_tickers: list[str] = []
    affected_sectors: list[str] = []
    summary: str | None = None
    category: str | None = None
    concept_tags: list[str] = []
    industry_tags: list[str] = []
    ticker_sentiments: dict[str, Any] = {}
    sector_sentiments: dict[str, Any] = {}
    # User read state
    is_read: bool = False
    # Timestamp type: "published" if published_at is available, "crawled" otherwise
    time_type: str = "crawled"
    # Processing status (for admin master feed)
    is_relevant: bool | None = None
    has_analysis: bool = False

    model_config = {"from_attributes": True}


class FilterResultDetail(BaseModel):
    is_relevant: bool
    relevance_score: float
    reason: str

    model_config = {"from_attributes": True}


class AnalysisResultDetail(BaseModel):
    sentiment: str
    impact_magnitude: str
    impact_timeframe: str
    affected_tickers: list[str] = []
    affected_sectors: list[str] = []
    category: str
    summary: str
    key_facts: list[str] = []
    bull_case: str
    bear_case: str
    surprise_factor: float
    market_expectation: str
    concept_tags: list[str] = []
    industry_tags: list[str] = []
    ticker_sentiments: dict[str, Any] = {}
    sector_sentiments: dict[str, Any] = {}
    analyzed_at: datetime
    model_used: str

    model_config = {"from_attributes": True}


class ResearchReportDetail(BaseModel):
    executive_summary: str
    context: str
    affected_securities: str
    historical_precedent: str
    bull_scenario: str
    bear_scenario: str
    recommended_actions: str
    risk_factors: str
    confidence: float
    full_report: str
    market_data_snapshot: dict[str, Any] = {}
    deep_research_data: dict[str, Any] = {}
    researched_at: datetime
    model_used: str

    model_config = {"from_attributes": True}


class NewsItemDetail(BaseModel):
    """Full news item with all analysis phases."""
    id: str
    source_name: str
    title: str
    url: str
    content: str
    published_at: datetime | None
    fetched_at: datetime
    language: str
    market: str
    metadata: dict[str, Any] = {}
    # Phase 1
    filter_result: FilterResultDetail | None = None
    # Phase 2 + 3
    analysis: AnalysisResultDetail | None = None
    # Deep research
    research: ResearchReportDetail | None = None
    # Timestamp type
    time_type: str = "crawled"

    model_config = {"from_attributes": True}


class NewsListResponse(BaseModel):
    items: list[NewsItemBrief]
    total: int
    page: int
    page_size: int
    has_next: bool


class NewsStatsResponse(BaseModel):
    total_today: int = 0
    total_week: int = 0
    analyzed_today: int = 0
    sentiment_distribution: dict[str, int] = {}
    impact_distribution: dict[str, int] = {}
    top_tickers: list[dict[str, Any]] = []
    top_sectors: list[dict[str, Any]] = []


class NewsSearchRequest(BaseModel):
    q: str = Field(min_length=1, max_length=500)
    page: int = 1
    page_size: int = 20
