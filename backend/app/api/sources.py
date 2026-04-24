"""Sources API: system config sources, portfolio holdings, user custom sources, health."""

from __future__ import annotations

import uuid
from functools import lru_cache
from pathlib import Path

import yaml
from fastapi import APIRouter, Depends, HTTPException, status


def _parse_uuid(value: str, label: str = "ID") -> uuid.UUID:
    """Parse a UUID string, raising 400 on invalid format."""
    try:
        return uuid.UUID(value)
    except (ValueError, AttributeError):
        raise HTTPException(status_code=400, detail=f"Invalid {label}: {value}")
from pydantic import BaseModel, Field
from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.app.deps import get_current_user, get_db
from backend.app.models.source import SourceHealth, UserSource
from backend.app.models.user import User
from backend.app.schemas.analytics import SourceHealthListResponse, SourceHealthResponse
from backend.app.services.stock_quote import get_quotes
from backend.app.services.consensus_forecast import fetch_consensus
from fastapi import Request
from dataclasses import asdict

router = APIRouter()

# ─── Config file loading ─────────────────────────────────────────────

_CONFIG_DIR = Path(__file__).resolve().parent.parent.parent.parent / "config"


@lru_cache(maxsize=1)
def _load_sources_yaml() -> list[dict]:
    """Load sources.yaml (cached, restart to reload)."""
    path = _CONFIG_DIR / "sources.yaml"
    if not path.exists():
        return []
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return data.get("sources", [])


@lru_cache(maxsize=1)
def _load_portfolio_yaml() -> list[dict]:
    """Load portfolio_sources.yaml (cached, restart to reload)."""
    path = _CONFIG_DIR / "portfolio_sources.yaml"
    if not path.exists():
        return []
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return data.get("sources", [])


@lru_cache(maxsize=1)
def _load_companies_map() -> dict:
    """Load companies mapping from sources.yaml."""
    path = _CONFIG_DIR / "sources.yaml"
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return data.get("companies", {})


# ─── Schemas ─────────────────────────────────────────────────────────

# ─── Category definitions ────────────────────────────────────────────
CATEGORY_LABELS = {
    "ai_technology":      {"en": "AI & Technology",         "zh": "AI与科技"},
    "semiconductors":     {"en": "Semiconductors",          "zh": "半导体"},
    "financial_news":     {"en": "Financial News",          "zh": "财经新闻"},
    "central_banks":      {"en": "Central Banks",           "zh": "央行政策"},
    "macro_economics":    {"en": "Macro Economics",         "zh": "宏观经济"},
    "commodities_energy": {"en": "Commodities & Energy",    "zh": "大宗商品与能源"},
    "regulatory":         {"en": "Regulatory & Policy",     "zh": "监管与政策"},
    "pharma_healthcare":  {"en": "Pharma & Healthcare",     "zh": "医药与健康"},
    "china_news":         {"en": "China Hot News",          "zh": "中国热点"},
    "exchanges":          {"en": "Exchanges & Futures",     "zh": "交易所与期货"},
    "geopolitics":        {"en": "Geopolitics & Trade",     "zh": "地缘政治与贸易"},
    "portfolio":          {"en": "Portfolio Holdings",      "zh": "持仓股监控"},
}


class UserSourceCreate(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    url: str = Field(max_length=2048, default="")
    source_type: str = "rss"
    priority: str = "p1"
    category: str = ""
    config: dict = {}
    stock_market: str | None = None  # US, A, HK, KR, JP
    stock_ticker: str | None = None  # e.g. NVDA, 300394
    stock_name: str | None = None    # Display name


class UserSourceResponse(BaseModel):
    id: str
    name: str
    url: str
    source_type: str
    priority: str
    category: str
    config: dict
    is_active: bool
    stock_market: str | None = None
    stock_ticker: str | None = None
    stock_name: str | None = None

    model_config = {"from_attributes": True}


class UserSourceUpdate(BaseModel):
    name: str | None = None
    url: str | None = None
    is_active: bool | None = None
    priority: str | None = None
    category: str | None = None
    stock_market: str | None = None
    stock_ticker: str | None = None
    stock_name: str | None = None


# ─── Categories ──────────────────────────────────────────────────────

@router.get("/categories")
async def list_categories(
    user: User = Depends(get_current_user),
):
    """List all source categories with labels."""
    return {"categories": CATEGORY_LABELS}


# ─── Config Sources (read-only, from YAML) ───────────────────────────

@router.get("/config")
async def list_config_sources(
    user: User = Depends(get_current_user),
):
    """List all system monitoring sources from config/sources.yaml."""
    raw = _load_sources_yaml()
    sources = []
    for s in raw:
        sources.append({
            "name": s.get("name", ""),
            "type": s.get("type", ""),
            "url": s.get("url", ""),
            "enabled": s.get("enabled", True),
            "priority": s.get("priority", "p2"),
            "market": s.get("market", ""),
            "category": s.get("category", ""),
            "requires_browser": s.get("requires_browser", False),
            "group": s.get("group", ""),
            "tags": s.get("tags", []),
            "stock_ticker": s.get("stock_ticker", ""),
            "stock_name": s.get("stock_name", ""),
            "stock_market": s.get("stock_market", ""),
        })
    return {"sources": sources, "total": len(sources)}


@router.get("/portfolio")
async def list_portfolio_holdings(
    user: User = Depends(get_current_user),
):
    """List portfolio holdings from config/portfolio_sources.yaml (global/public list).

    Multiple YAML entries for the same stock (e.g. News + IR pages) are merged
    into a single holding, with tags combined and deduplicated.
    """
    raw = _load_portfolio_yaml()
    # Deduplicate by stock_ticker: merge tags from multiple entries
    seen: dict[str, dict] = {}  # key = stock_ticker
    for s in raw:
        ticker = s.get("stock_ticker", "")
        if not ticker:
            continue
        if ticker in seen:
            # Merge tags (deduplicate, preserve order)
            existing_tags = seen[ticker]["tags"]
            for tag in s.get("tags", []):
                if tag not in existing_tags:
                    existing_tags.append(tag)
        else:
            seen[ticker] = {
                "name": s.get("name", ""),
                "url": s.get("url", ""),
                "enabled": s.get("enabled", True),
                "priority": s.get("priority", "p1"),
                "market": s.get("market", ""),
                "category": s.get("category", "portfolio"),
                "group": s.get("group", "portfolio"),
                "tags": list(s.get("tags", [])),  # copy to avoid mutation
                "stock_ticker": ticker,
                "stock_name": s.get("stock_name", ""),
                "stock_market": s.get("stock_market", ""),
                "requires_browser": s.get("requires_browser", False),
            }
    holdings = list(seen.values())
    return {"holdings": holdings, "total": len(holdings)}


@router.get("/portfolio/quotes")
async def get_portfolio_quotes(
    request: Request,
    user: User = Depends(get_current_user),
    refresh: bool = False,
):
    """Return realtime quote data for every portfolio holding.

    Source routing: US → Alpaca IEX (realtime), A-shares → ClickHouse db_market
    (realtime), HK/KR/JP/AU → yfinance (15-min delayed). Cached in Redis for 1
    min. Pass `?refresh=true` to bypass the cache.
    """
    from backend.app.config import get_settings
    raw = _load_portfolio_yaml()
    pairs: dict[str, str] = {}
    for s in raw:
        ticker = s.get("stock_ticker", "")
        if ticker and ticker not in pairs:
            pairs[ticker] = s.get("stock_market", "")
    redis = getattr(request.app.state, "redis", None)
    quotes = await get_quotes(
        list(pairs.items()),
        redis=redis,
        settings=get_settings(),
        use_cache=not refresh,
    )
    return {"quotes": {k: asdict(v) for k, v in quotes.items()}, "total": len(quotes)}


@router.get("/portfolio/consensus")
async def get_portfolio_consensus(
    request: Request,
    user: User = Depends(get_current_user),
    refresh: bool = False,
):
    """A-share analyst consensus forecast (一致预期) for every portfolio holding.

    Source: Wind `ASHARECONSENSUS*` tables on 192.168.31.176 (read-only MySQL).
    Non-A-share holdings are skipped. Cached in Redis for 30 min; pass
    `?refresh=true` to bypass the cache.
    """
    from backend.app.config import get_settings
    raw = _load_portfolio_yaml()
    pairs: list[tuple[str, str]] = []
    seen: set[str] = set()
    for s in raw:
        ticker = s.get("stock_ticker", "")
        market = s.get("stock_market", "")
        if ticker and ticker not in seen:
            seen.add(ticker)
            pairs.append((ticker, market))
    redis = getattr(request.app.state, "redis", None)
    data = await fetch_consensus(
        pairs,
        settings=get_settings(),
        redis=redis,
        use_cache=not refresh,
    )
    return {"consensus": {k: asdict(v) for k, v in data.items()}, "total": len(data)}


@router.get("/companies")
async def list_tracked_companies(
    user: User = Depends(get_current_user),
):
    """List all tracked companies from config/sources.yaml companies mapping."""
    return _load_companies_map()


# ─── Combined source listing ─────────────────────────────────────────

@router.get("")
async def list_sources(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """List all sources: system sources (with health) + user custom sources."""
    # System source health from DB (populated by engine)
    health_result = await db.execute(select(SourceHealth).order_by(SourceHealth.source_name))
    health_rows = health_result.scalars().all()
    health_map = {h.source_name: h for h in health_rows}

    # Build system sources from config + health data
    config_sources = _load_sources_yaml()
    portfolio_sources = _load_portfolio_yaml()
    all_config = config_sources + portfolio_sources

    system_sources = []
    for cfg in all_config:
        name = cfg.get("name", "")
        h = health_map.pop(name, None)
        system_sources.append({
            "name": name,
            "type": "system",
            "source_type": cfg.get("type", ""),
            "url": cfg.get("url", ""),
            "enabled": cfg.get("enabled", True),
            "priority": cfg.get("priority", "p2"),
            "market": cfg.get("market", ""),
            "category": cfg.get("category", cfg.get("group", "")),
            "group": cfg.get("group", ""),
            "tags": cfg.get("tags", []),
            "stock_ticker": cfg.get("stock_ticker", ""),
            "stock_name": cfg.get("stock_name", ""),
            "is_healthy": h.is_healthy if h else None,
            "last_success": h.last_success.isoformat() if h and h.last_success else None,
            "last_failure": h.last_failure.isoformat() if h and h.last_failure else None,
            "consecutive_failures": h.consecutive_failures if h else 0,
            "total_items_fetched": h.total_items_fetched if h else 0,
        })

    # Add any health entries not in config (legacy or renamed sources)
    for name, h in health_map.items():
        system_sources.append({
            "name": name,
            "type": "system",
            "source_type": "",
            "url": "",
            "enabled": True,
            "priority": "",
            "market": "",
            "category": "",
            "group": "",
            "tags": [],
            "stock_ticker": "",
            "stock_name": "",
            "is_healthy": h.is_healthy,
            "last_success": h.last_success.isoformat() if h.last_success else None,
            "last_failure": h.last_failure.isoformat() if h.last_failure else None,
            "consecutive_failures": h.consecutive_failures,
            "total_items_fetched": h.total_items_fetched,
        })

    # User custom sources
    user_result = await db.execute(
        select(UserSource).where(UserSource.user_id == user.id).order_by(UserSource.created_at)
    )
    user_sources = [
        {
            "id": str(s.id),
            "name": s.name,
            "url": s.url,
            "type": "custom",
            "source_type": s.source_type,
            "priority": s.priority,
            "category": s.category or "",
            "is_active": s.is_active,
            "stock_market": s.stock_market,
            "stock_ticker": s.stock_ticker,
            "stock_name": s.stock_name,
        }
        for s in user_result.scalars().all()
    ]

    return {"system_sources": system_sources, "user_sources": user_sources}


# ─── User custom source CRUD ─────────────────────────────────────────

@router.post("", response_model=UserSourceResponse, status_code=status.HTTP_201_CREATED)
async def create_source(
    body: UserSourceCreate,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Add a custom news source or stock subscription for the current user."""
    source = UserSource(
        user_id=user.id,
        name=body.name,
        url=body.url,
        source_type=body.source_type,
        priority=body.priority,
        category=body.category,
        config=body.config,
        stock_market=body.stock_market,
        stock_ticker=body.stock_ticker,
        stock_name=body.stock_name,
    )
    db.add(source)
    await db.commit()
    await db.refresh(source)

    return UserSourceResponse(
        id=str(source.id),
        name=source.name,
        url=source.url,
        source_type=source.source_type,
        priority=source.priority,
        category=source.category or "",
        config=source.config or {},
        is_active=source.is_active,
        stock_market=source.stock_market,
        stock_ticker=source.stock_ticker,
        stock_name=source.stock_name,
    )


@router.put("/{source_id}", response_model=UserSourceResponse)
async def update_source(
    source_id: str,
    body: UserSourceUpdate,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Update a user's custom source."""
    source = await db.scalar(
        select(UserSource).where(
            and_(UserSource.id == _parse_uuid(source_id, "source_id"), UserSource.user_id == user.id)
        )
    )
    if not source:
        raise HTTPException(status_code=404, detail="Source not found")

    for field, val in body.model_dump(exclude_unset=True).items():
        setattr(source, field, val)

    await db.commit()
    await db.refresh(source)

    return UserSourceResponse(
        id=str(source.id),
        name=source.name,
        url=source.url,
        source_type=source.source_type,
        priority=source.priority,
        category=source.category or "",
        config=source.config or {},
        is_active=source.is_active,
        stock_market=source.stock_market,
        stock_ticker=source.stock_ticker,
        stock_name=source.stock_name,
    )


@router.delete("/{source_id}", status_code=204)
async def delete_source(
    source_id: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Delete a user's custom source."""
    source = await db.scalar(
        select(UserSource).where(
            and_(UserSource.id == _parse_uuid(source_id, "source_id"), UserSource.user_id == user.id)
        )
    )
    if not source:
        raise HTTPException(status_code=404, detail="Source not found")

    await db.delete(source)
    await db.commit()


# ─── Source Health ────────────────────────────────────────────────────

@router.get("/health", response_model=SourceHealthListResponse)
async def get_source_health(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Get health status for all system sources."""
    result = await db.execute(select(SourceHealth).order_by(SourceHealth.source_name))
    sources = result.scalars().all()

    healthy = sum(1 for s in sources if s.is_healthy)

    return SourceHealthListResponse(
        sources=[SourceHealthResponse.model_validate(s) for s in sources],
        total_healthy=healthy,
        total_unhealthy=len(sources) - healthy,
    )
