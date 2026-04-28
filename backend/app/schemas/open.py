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
    canonical_id: str | None = None  # CODE.MARKET form matching MongoDB `_canonical_tickers`


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


# ── Knowledge Base ─────────────────────────────────────────────────────
class KbDateRange(BaseModel):
    gte: str | None = Field(None, description="Start date YYYY-MM-DD (inclusive)")
    lte: str | None = Field(None, description="End date YYYY-MM-DD (inclusive)")


class KbSearchRequest(BaseModel):
    query: str = Field("", description="Natural language query, CN or EN. Empty allowed for recency-only filter browse.")
    tickers: list[str] | None = Field(None, description="Stock codes. Canonical CODE.MARKET preferred (NVDA.US, 0700.HK, 600519.SH); bare codes are auto-expanded.")
    doc_types: list[str] | None = Field(None, description="Filter by doc_type enum (see /kb/meta).")
    sources: list[str] | None = Field(None, description="Filter by source platform (alphapai, jinmen, meritco, thirdbridge, funda, gangtise, acecamp).")
    date_range: KbDateRange | None = None
    top_k: int = Field(20, ge=1, le=30)


class KbHit(BaseModel):
    doc_id: str = Field(description="Stable id for kb/fetch. Format: '<source>:<collection>:<_id>'")
    source: str
    doc_type: str
    doc_type_cn: str
    title: str
    snippet: str
    date: str
    release_ms: int | None = None
    institution: str
    tickers: list[str] = []
    url: str = ""
    text_len: int


class KbSearchResponse(BaseModel):
    query: str
    total: int
    hits: list[KbHit]


class KbFetchRequest(BaseModel):
    doc_id: str
    max_chars: int = Field(30000, ge=1000, le=30000)


class KbFetchResponse(BaseModel):
    found: bool
    doc_id: str
    source: str | None = None
    doc_type: str | None = None
    doc_type_cn: str | None = None
    title: str | None = None
    text: str | None = None
    full_text_len: int | None = None
    truncated: bool | None = None
    date: str | None = None
    release_ms: int | None = None
    institution: str | None = None
    tickers: list[str] = []
    url: str | None = None
    has_pdf: bool | None = None
    error: str | None = None


class KbFacetsFilters(BaseModel):
    tickers: list[str] | None = None
    doc_types: list[str] | None = None
    sources: list[str] | None = None
    date_range: KbDateRange | None = None


class KbFacetsRequest(BaseModel):
    dimension: str = Field(description="sources | doc_types | tickers | date_histogram")
    filters: KbFacetsFilters | None = None
    top: int = Field(20, ge=1, le=200)


class KbFacetsResponse(BaseModel):
    dimension: str
    rows: list[dict[str, Any]]


class KbCollectionInfo(BaseModel):
    source: str
    collection: str
    doc_type: str
    doc_type_cn: str
    has_pdf: bool


class KbMetaResponse(BaseModel):
    sources: list[str]
    doc_types: list[str]
    collections: list[KbCollectionInfo]
    notes: str


# ── Personal Knowledge Base (team-shared) ──────────────────────
# The personal-KB is shared team-wide for retrieval. /user_kb/search
# returns hits across every team member's uploads in one call.

class UserKbSearchRequest(BaseModel):
    query: str = Field("", description="Natural-language query, CN or EN.")
    top_k: int = Field(20, ge=1, le=30)
    document_ids: list[str] | None = Field(
        None,
        description="Optional pre-filter: restrict search to specific document_ids.",
    )
    mode: str = Field(
        "hybrid",
        description="Retrieval mode: 'hybrid' (BM25+dense RRF, default), 'lexical' (BM25 only), 'semantic' (dense only).",
    )


class UserKbHit(BaseModel):
    document_id: str = Field(description="Mongo ObjectId hex (24 chars). Pass to /user_kb/fetch.")
    title: str
    original_filename: str
    chunk_index: int
    text: str = Field(description="Matching chunk body (typically a few hundred chars).")
    created_at: str
    uploader_user_id: str = Field(
        "",
        description="The user who originally uploaded this doc — empty when unknown.",
    )


class UserKbSearchResponse(BaseModel):
    query: str
    total: int
    hits: list[UserKbHit]


class UserKbFetchRequest(BaseModel):
    document_id: str = Field(description="Mongo ObjectId hex (24 chars), as returned by /user_kb/search.")
    max_chars: int = Field(30000, ge=1000, le=30000)


class UserKbFetchResponse(BaseModel):
    found: bool
    document_id: str
    title: str | None = None
    original_filename: str | None = None
    doc_type: str | None = None
    mime_type: str | None = None
    text: str | None = None
    full_text_len: int | None = None
    truncated: bool | None = None
    created_at: str | None = None
    uploader_user_id: str | None = None
    error: str | None = None
