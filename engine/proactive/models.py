"""Data models for the proactive portfolio monitoring system.

v3: Event-driven breaking news detection with historical price impact validation.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class PortfolioHolding:
    """A single stock in the portfolio, parsed from portfolio_sources.yaml."""

    ticker: str              # "GLW", "300394", "06869"
    name_cn: str             # "康宁", "天孚通信"
    name_en: str             # "Corning", "Tianfu Communication"
    market: str              # "us", "china", "hk", "kr", "jp"
    market_label: str        # "美股", "创业板", "港股"
    tags: list[str] = field(default_factory=list)
    search_terms: list[str] = field(default_factory=list)

    @staticmethod
    def from_source_config(cfg: dict) -> PortfolioHolding:
        """Build a PortfolioHolding from a portfolio_sources.yaml entry."""
        ticker = cfg.get("stock_ticker", "")
        name_cn = cfg.get("stock_name", "")
        # Derive English name from the source name if it contains both
        raw_name = cfg.get("name", "")
        name_en = ""
        if " " in raw_name:
            parts = raw_name.split()
            en_parts = [p for p in parts if p.isascii() and p.lower() not in (
                "newsroom", "news", "blog", "ir", "investor", "relations",
                "press", "releases", "room", "center", "media",
            )]
            name_en = " ".join(en_parts)

        market_label = cfg.get("stock_market", "")
        market = _label_to_market(market_label)
        tags = cfg.get("tags", [])

        search_terms = _build_search_terms(ticker, name_cn, name_en)

        return PortfolioHolding(
            ticker=ticker,
            name_cn=name_cn,
            name_en=name_en,
            market=market,
            market_label=market_label,
            tags=tags,
            search_terms=search_terms,
        )


def _label_to_market(label: str) -> str:
    """Convert Chinese market label to internal market code."""
    mapping = {
        "美股": "us",
        "主板": "china", "创业板": "china", "科创板": "china",
        "港股": "hk",
        "韩股": "kr",
        "日股": "jp",
    }
    return mapping.get(label, "us")


def _build_search_terms(ticker: str, name_cn: str, name_en: str) -> list[str]:
    """Build a deduplicated list of search terms for a stock."""
    terms: list[str] = []
    if ticker:
        terms.append(ticker)
        if "." in ticker:
            terms.append(ticker.split(".")[0])
    if name_cn:
        terms.append(name_cn)
    if name_en:
        terms.append(name_en)
    return list(dict.fromkeys(t for t in terms if t))


# -----------------------------------------------------------------------
# Breaking News item (time-gate output)
# -----------------------------------------------------------------------

@dataclass
class BreakingNewsItem:
    """A single news item that passed the 24-hour time gate."""

    title: str = ""
    url: str = ""
    content: str = ""
    source_engine: str = ""      # "baidu" | "tavily" | "jina" | "internal"
    source_label: str = ""       # "AlphaPai路演" | "百度搜索" | "Tavily"
    published_at: datetime | None = None  # UTC-aware
    is_date_verified: bool = False  # True if date came from page-level extraction
    age_hours: float = 0.0       # Hours since publication (for display)


# -----------------------------------------------------------------------
# Historical price impact precedent
# -----------------------------------------------------------------------

@dataclass
class HistoricalPrecedent:
    """A historical event analogous to current breaking news, with price data."""

    event_date: str = ""         # "2025-06-15"
    description: str = ""        # "Intel announced $10B fab expansion"
    ticker: str = ""             # "INTC"
    market: str = ""             # "us" | "china" | "hk"
    return_1d: float | None = None
    return_3d: float | None = None
    return_5d: float | None = None
    price_before: float | None = None
    price_after_1d: float | None = None
    source: str = ""             # Where we found this precedent


# -----------------------------------------------------------------------
# Context models (unchanged)
# -----------------------------------------------------------------------

@dataclass
class InternalDataContext:
    """Aggregated internal platform data for a single stock."""

    source_items: dict[str, list[dict]] = field(default_factory=dict)
    total_count: int = 0
    new_count: int = 0
    sentiment_distribution: dict[str, int] = field(default_factory=dict)
    formatted_text: str = ""


@dataclass
class ExternalSearchContext:
    """Aggregated external search results for a single stock."""

    search_results: dict[str, list[dict]] = field(default_factory=dict)
    fetched_pages: list[dict] = field(default_factory=list)
    total_results: int = 0
    formatted_text: str = ""


@dataclass
class StockSnapshot:
    """Point-in-time comprehensive view of a stock."""

    holding: PortfolioHolding = field(default_factory=lambda: PortfolioHolding("", "", "", "", ""))
    scan_time: datetime = field(default_factory=_utcnow)
    internal_context: InternalDataContext = field(default_factory=InternalDataContext)
    external_context: ExternalSearchContext = field(default_factory=ExternalSearchContext)
    price_data: str = ""
    current_narrative: str = ""


# -----------------------------------------------------------------------
# Baseline — per-stock persistent memory
# -----------------------------------------------------------------------

@dataclass
class StockBaseline:
    """Persistent memory of what we already know about a stock.

    v3: Shifted from 'narrative comparison' to 'event dedup log'.
    The time gate (published_at < 24h) is the primary freshness filter;
    known_event_hashes prevents re-alerting on the same event across scans.
    """

    ticker: str = ""
    last_scan_time: datetime = field(default_factory=_utcnow)
    last_narrative: str = ""  # Kept for backward compat, no longer actively written
    known_developments: list[str] = field(default_factory=list)
    known_content_ids: set[str] = field(default_factory=set)
    known_event_hashes: set[str] = field(default_factory=set)  # v3: event dedup
    recent_alert_times: list[datetime] = field(default_factory=list)
    sentiment_history: list[tuple[datetime, str]] = field(default_factory=list)

    # Bookkeeping
    scan_count: int = 0
    alert_count: int = 0
    last_alert_at: datetime | None = None

    def to_dict(self) -> dict:
        """Serialize for Redis/PostgreSQL storage."""
        return {
            "ticker": self.ticker,
            "last_scan_time": self.last_scan_time.isoformat(),
            "last_narrative": self.last_narrative,
            "known_developments": self.known_developments[-30:],
            "known_content_ids": list(self.known_content_ids)[-500:],
            "known_event_hashes": list(self.known_event_hashes)[-500:],
            "recent_alert_times": [t.isoformat() for t in self.recent_alert_times[-20:]],
            "sentiment_history": [
                (t.isoformat(), s) for t, s in self.sentiment_history[-20:]
            ],
            "scan_count": self.scan_count,
            "alert_count": self.alert_count,
            "last_alert_at": self.last_alert_at.isoformat() if self.last_alert_at else None,
        }

    @staticmethod
    def from_dict(data: dict) -> StockBaseline:
        """Deserialize from Redis/PostgreSQL storage."""
        from datetime import datetime, timezone

        def _parse_dt(s: str | None) -> datetime:
            if not s:
                return datetime.now(timezone.utc)
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt

        return StockBaseline(
            ticker=data.get("ticker", ""),
            last_scan_time=_parse_dt(data.get("last_scan_time")),
            last_narrative=data.get("last_narrative", ""),
            known_developments=data.get("known_developments", []),
            known_content_ids=set(data.get("known_content_ids", [])),
            known_event_hashes=set(data.get("known_event_hashes", [])),
            recent_alert_times=[_parse_dt(t) for t in data.get("recent_alert_times", [])],
            sentiment_history=[
                (_parse_dt(entry[0]), entry[1])
                for entry in data.get("sentiment_history", [])
                if isinstance(entry, (list, tuple)) and len(entry) >= 2
            ],
            scan_count=data.get("scan_count", 0),
            alert_count=data.get("alert_count", 0),
            last_alert_at=_parse_dt(data["last_alert_at"]) if data.get("last_alert_at") else None,
        )


# -----------------------------------------------------------------------
# Scan result — complete output of one scan cycle for one stock
# -----------------------------------------------------------------------

@dataclass
class ProactiveScanResult:
    """Complete result of a proactive scan cycle for one stock.

    v3: Breaking-news-centric fields replace delta-centric ones.
    """

    holding: PortfolioHolding = field(default_factory=lambda: PortfolioHolding("", "", "", "", ""))
    scan_time: datetime = field(default_factory=_utcnow)
    snapshot: StockSnapshot = field(default_factory=StockSnapshot)

    # Stage 0: Time gate
    recent_items_count: int = 0

    # Stage 1: Breaking news triage
    breaking_news_detected: bool = False
    news_materiality: str = "none"  # "none" | "routine" | "material" | "critical"
    news_summary: str = ""
    new_developments: list[str] = field(default_factory=list)

    # Stage 2: Novelty verification + deep research
    novelty_verified: bool = False
    novelty_status: str = ""  # "verified_fresh" | "likely_fresh" | "stale" | "repackaged"
    earliest_report_time: datetime | None = None
    deep_research_performed: bool = False
    research_iterations: int = 0
    key_findings: list[str] = field(default_factory=list)
    news_timeline: list[dict] = field(default_factory=list)
    referenced_sources: list[dict] = field(default_factory=list)

    # Stage 3: Historical price impact
    historical_precedents: list[dict] = field(default_factory=list)

    # Stage 4: Alert decision
    should_alert: bool = False
    alert_confidence: float = 0.0
    alert_rationale: str = ""
    full_analysis: dict | None = None

    # Metadata
    tokens_used: int = 0
    cost_cny: float = 0.0

    # Backward compat aliases
    @property
    def delta_detected(self) -> bool:
        return self.breaking_news_detected

    @property
    def delta_magnitude(self) -> str:
        mapping = {"none": "none", "routine": "minor", "material": "significant", "critical": "critical"}
        return mapping.get(self.news_materiality, "none")

    @property
    def delta_description(self) -> str:
        return self.news_summary
