"""Data models for the trading agent system.

Three-phase pipeline models:
  Phase 1: InitialEvaluation — relevance + market impact + search queries
  Phase 2: DeepResearchResult — iterative search + URL fetch + price data
  Phase 3: FinalAssessment — surprise factor + sentiment + final analysis

Legacy models (FilterResult, AnalysisResult, ResearchReport) are kept for
DB compatibility and backward-compatible storage.
"""

from __future__ import annotations

import hashlib
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


class Priority(str, Enum):
    P0 = "p0"
    P1 = "p1"
    P2 = "p2"


class Sentiment(str, Enum):
    VERY_BULLISH = "very_bullish"
    BULLISH = "bullish"
    NEUTRAL = "neutral"
    BEARISH = "bearish"
    VERY_BEARISH = "very_bearish"


class ImpactMagnitude(str, Enum):
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class AlertLevel(str, Enum):
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"


@dataclass
class NewsItem:
    """A single news/announcement item fetched from a source."""

    source_name: str
    title: str
    url: str
    content: str = ""
    published_at: datetime | None = None
    fetched_at: datetime = field(default_factory=datetime.now)
    language: str = "zh"  # "zh" or "en"
    market: str = "china"  # "china", "us", "global"
    metadata: dict[str, Any] = field(default_factory=dict)
    id: str = field(default_factory=lambda: uuid.uuid4().hex[:16])

    @property
    def content_hash(self) -> str:
        """SHA-256 hash of title+url for deduplication."""
        raw = f"{self.title}|{self.url}".encode("utf-8")
        return hashlib.sha256(raw).hexdigest()[:32]

    def snippet(self, max_len: int = 500) -> str:
        """Return a truncated version of the content for LLM input."""
        text = self.content or self.title
        if len(text) > max_len:
            return text[:max_len] + "..."
        return text


# ============================================================
# Phase 1: Initial Evaluation
# ============================================================

@dataclass
class InitialEvaluation:
    """Phase 1 output: initial LLM evaluation of news relevance and market impact.

    If may_affect_market is True, includes related stocks, sectors, and
    search queries covering 3 categories.
    """

    news_item_id: str
    relevance_score: float = 0.0
    may_affect_market: bool = False
    reason: str = ""
    title_zh: str = ""  # Chinese translation of the title (same if already Chinese)
    # Only populated when may_affect_market is True
    related_stocks: list[dict[str, str]] = field(default_factory=list)  # [{"name": "...", "ticker": "..."}]
    related_sectors: list[str] = field(default_factory=list)
    search_queries: dict[str, list[str]] = field(default_factory=dict)
    # search_queries keys: "news_coverage", "historical_impact", "stock_performance"
    google_queries: dict[str, list[str]] = field(default_factory=dict)
    # google_queries: English queries for Google Custom Search (same category keys)
    evaluated_at: datetime = field(default_factory=datetime.now)
    model_used: str = ""


# ============================================================
# Phase 2: Deep Research
# ============================================================

@dataclass
class SearchResultItem:
    """A single search result from Baidu or Google."""

    title: str = ""
    url: str = ""
    content: str = ""
    date: str = ""
    score: float = 0.0
    source: str = ""  # "baidu" or "google"
    website: str = ""
    category: str = ""  # "news_coverage", "stock_info", "historical_impact"
    query: str = ""  # The query that produced this result


@dataclass
class FetchedPage:
    """Content fetched from a specific URL requested by the LLM."""

    url: str = ""
    title: str = ""
    content: str = ""
    fetch_success: bool = False
    error: str = ""


@dataclass
class ResearchIteration:
    """One iteration of the deep research loop."""

    iteration: int = 0
    search_results: list[SearchResultItem] = field(default_factory=list)
    fetched_pages: list[FetchedPage] = field(default_factory=list)
    price_data: dict[str, str] = field(default_factory=dict)
    llm_response: dict[str, Any] = field(default_factory=dict)
    is_sufficient: bool = False
    new_queries: list[str] = field(default_factory=list)
    urls_to_fetch: list[str] = field(default_factory=list)


@dataclass
class DeepResearchResult:
    """Phase 2 output: accumulated deep research across all iterations.

    Contains all search results, fetched pages, price data, and LLM reasoning.
    """

    news_item_id: str
    iterations: list[ResearchIteration] = field(default_factory=list)
    total_iterations: int = 0
    # Aggregated citations by category
    citations: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    # citations keys: "news_coverage", "stock_info", "historical_impact"
    price_data: dict[str, str] = field(default_factory=dict)  # {ticker: formatted_data}
    all_search_results: list[SearchResultItem] = field(default_factory=list)
    all_fetched_pages: list[FetchedPage] = field(default_factory=list)
    research_summary: str = ""  # LLM's accumulated reasoning
    # Structured data for frontend display
    news_timeline: list[dict[str, Any]] = field(default_factory=list)
    # [{"time": "...", "source": "...", "title": "...", "url": "..."}]
    referenced_sources: list[dict[str, Any]] = field(default_factory=list)
    # [{"title": "...", "url": "...", "snippet": "...", "source_engine": "...", "relevance": "..."}]
    search_queries_used: dict[str, list[str]] = field(default_factory=dict)
    # {"baidu": ["q1", "q2"], "google": ["q1", "q2"]}


# ============================================================
# Phase 3: Final Assessment
# ============================================================

@dataclass
class FinalAssessment:
    """Phase 3 output: final sentiment and surprise factor assessment.

    Produced after all research is complete, with full context available.
    """

    news_item_id: str
    surprise_factor: float = 0.5  # 0.0-1.0
    sentiment: str = "neutral"  # overall fallback: very_bullish, bullish, neutral, bearish, very_bearish
    impact_magnitude: str = "low"  # critical, high, medium, low
    impact_timeframe: str = "short_term"
    timeliness: str = "timely"  # timely, medium, low
    summary: str = ""
    key_findings: list[str] = field(default_factory=list)
    bull_case: str = ""
    bear_case: str = ""
    market_expectation: str = ""
    recommended_action: str = ""
    confidence: float = 0.5
    assessed_at: datetime = field(default_factory=datetime.now)
    model_used: str = ""
    # Per-stock sentiment: [{"ticker": "NVDA", "name": "英伟达", "short_term": {...}, ...}]
    per_stock_sentiment: list[dict[str, Any]] = field(default_factory=list)
    # Per-sector sentiment (fallback when no stock identified):
    # [{"sector": "半导体", "short_term": {...}, ...}]
    per_sector_sentiment: list[dict[str, Any]] = field(default_factory=list)


# ============================================================
# Legacy Models (kept for DB compatibility)
# ============================================================

@dataclass
class FilterResult:
    """Stage 1 output: relevance classification (legacy, maps from InitialEvaluation)."""

    news_item_id: str
    is_relevant: bool
    relevance_score: float  # 0.0 - 1.0
    reason: str = ""


@dataclass
class AnalysisResult:
    """Stage 2 output: structured stock analysis (legacy, maps from InitialEvaluation + FinalAssessment)."""

    news_item_id: str
    sentiment: str = "neutral"
    impact_magnitude: str = "low"
    impact_timeframe: str = "short_term"
    affected_tickers: list[str] = field(default_factory=list)
    affected_sectors: list[str] = field(default_factory=list)
    category: str = "other"
    summary: str = ""
    key_facts: list[str] = field(default_factory=list)
    bull_case: str = ""
    bear_case: str = ""
    requires_deep_research: bool = False
    research_questions: list[str] = field(default_factory=list)
    analyzed_at: datetime = field(default_factory=datetime.now)
    model_used: str = ""
    # Signal quality fields
    surprise_factor: float = 0.5
    is_routine: bool = False
    market_expectation: str = ""
    quantified_evidence: list[str] = field(default_factory=list)
    # Search verification questions
    search_questions: list[str] = field(default_factory=list)
    # Concept & industry tags (populated by tagging phase)
    concept_tags: list[str] = field(default_factory=list)   # up to 3 THS concept names
    industry_tags: list[str] = field(default_factory=list)   # 1-3 CITIC level-1 industry names
    # Per-stock sentiment: {"NVDA(英伟达)": "bearish", "AAPL(苹果)": "neutral"}
    # Falls back to global `sentiment` when empty
    ticker_sentiments: dict[str, str] = field(default_factory=dict)
    # Per-sector sentiment (when no specific stock): {"半导体": "bearish"}
    sector_sentiments: dict[str, str] = field(default_factory=dict)


@dataclass
class ResearchReport:
    """Stage 3 output: deep research report (legacy, maps from DeepResearchResult)."""

    news_item_id: str
    executive_summary: str = ""
    context: str = ""
    affected_securities: str = ""
    historical_precedent: str = ""
    bull_scenario: str = ""
    bear_scenario: str = ""
    recommended_actions: str = ""
    risk_factors: str = ""
    confidence: float = 0.0
    full_report: str = ""
    market_data_snapshot: dict[str, Any] = field(default_factory=dict)
    researched_at: datetime = field(default_factory=datetime.now)
    model_used: str = ""


@dataclass
class SearchVerification:
    """Stage 2.5 output: web search verification results (legacy, maps from DeepResearchResult)."""

    news_item_id: str
    related_news: list[dict[str, Any]] = field(default_factory=list)
    price_data: dict[str, Any] = field(default_factory=dict)
    verification_summary: str = ""
    timeliness_info: str = ""
    search_results_raw: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class SourceHealth:
    """Track health status of each monitoring source."""

    source_name: str
    last_success: datetime | None = None
    last_failure: datetime | None = None
    consecutive_failures: int = 0
    total_items_fetched: int = 0
    is_healthy: bool = True
