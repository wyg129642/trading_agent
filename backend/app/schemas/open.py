"""Pydantic schemas for the Open API (agent-to-agent)."""
from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


# ── Stock suggest ──────────────────────────────────────────────────────
class StockSuggestion(BaseModel):
    name: str
    code: str
    market: str
    label: str  # "英伟达(NVDA)"


class SuggestResponse(BaseModel):
    suggestions: list[StockSuggestion]


# ── Search ─────────────────────────────────────────────────────────────
class ResolvedStock(BaseModel):
    name: str
    code: str
    market: str


class SearchItem(BaseModel):
    id: str
    source_type: str = Field(description="news | alphapai_wechat | alphapai_comment | alphapai_roadshow_cn | alphapai_roadshow_us | jiuqian_forum | jiuqian_minutes | jiuqian_wechat")
    source_label: str = Field(description="Human-readable source name, e.g. 资讯中心")
    title: str
    title_zh: str | None = None
    summary: str | None = None
    tickers: list[str] = []
    sectors: list[str] = []
    sentiment: str | None = None
    impact_magnitude: str | None = None
    concept_tags: list[str] = []
    published_at: str | None = None
    detail_url: str = Field(description="API URL to fetch full content")
    site_url: str = Field(description="Frontend page URL for human viewing")
    original_url: str | None = Field(None, description="Original news source URL")


class SearchResponse(BaseModel):
    query: str
    resolved_stock: ResolvedStock | None = None
    total: int
    page: int
    page_size: int
    has_next: bool
    items: list[SearchItem]
    source_counts: dict[str, int] = {}


# ── Detail ─────────────────────────────────────────────────────────────
class AnalysisBrief(BaseModel):
    sentiment: str | None = None
    impact_magnitude: str | None = None
    impact_timeframe: str | None = None
    summary: str | None = None
    key_facts: list[str] = []
    bull_case: str | None = None
    bear_case: str | None = None
    ticker_sentiments: dict[str, Any] = {}
    surprise_factor: float | None = None
    concept_tags: list[str] = []
    industry_tags: list[str] = []


class ResearchBrief(BaseModel):
    executive_summary: str | None = None
    context: str | None = None
    historical_precedent: str | None = None
    bull_scenario: str | None = None
    bear_scenario: str | None = None
    recommended_actions: str | None = None
    risk_factors: str | None = None
    confidence: float | None = None
    citations: list[dict[str, Any]] = []


class DetailResponse(BaseModel):
    id: str
    source_type: str
    title: str
    title_zh: str | None = None
    content: str | None = Field(None, description="Full text content")
    published_at: str | None = None
    original_url: str | None = None
    site_url: str
    tickers: list[str] = []
    sectors: list[str] = []
    analysis: AnalysisBrief | None = None
    research: ResearchBrief | None = None
