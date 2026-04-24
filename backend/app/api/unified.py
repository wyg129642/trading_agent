"""Cross-platform ticker aggregation.

Query every crawled source by a single canonical ticker (``CODE.MARKET``) and
return a unified, training-friendly list of mentions.

Prerequisite: run ``scripts/enrich_tickers.py`` at least once so each document
has a ``_canonical_tickers`` field.

Endpoints
---------
``GET /api/unified/by-symbol/{canonical_id}``
    All mentions of one symbol across all sources, sorted by release time.

``GET /api/unified/symbols/search?q=…``
    Probe the alias table — convert a user-typed name (e.g. "英伟达") into the
    canonical ID for the query above.

``GET /api/unified/normalize``
    Debugging helper — normalize a raw input string and return the canonical
    tickers plus any unmatched fragments.
"""
from __future__ import annotations

import asyncio
import logging
from functools import lru_cache
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel

from backend.app.config import Settings, get_settings
from backend.app.deps import get_current_user
from backend.app.models.user import User
from backend.app.services.ticker_normalizer import (
    _alias_table,
    normalize,
    normalize_with_unmatched,
)

logger = logging.getLogger(__name__)
router = APIRouter()


# --------------------------------------------------------------------------- #
# Static source registry — one row per collection we enrich.
# Each row knows where to find the doc's title / date / preview / url so the
# unified response can be flat.
# --------------------------------------------------------------------------- #
class SourceSpec:
    def __init__(
        self,
        *,
        source: str,
        uri_attr: str,
        db_attr: str,
        collection: str,
        time_field: str,
        url_field: str | None,
        preview_field: str | None,
        source_label: str,
    ) -> None:
        self.source = source
        self.uri_attr = uri_attr
        self.db_attr = db_attr
        self.collection = collection
        self.time_field = time_field
        self.url_field = url_field
        self.preview_field = preview_field
        self.source_label = source_label


_SOURCES: list[SourceSpec] = [
    SourceSpec(
        source="alphapai",
        uri_attr="alphapai_mongo_uri",
        db_attr="alphapai_mongo_db",
        collection="roadshows",
        time_field="publish_time",
        url_field="web_url",
        preview_field="content",
        source_label="AlphaPai · 会议纪要",
    ),
    SourceSpec(
        source="alphapai",
        uri_attr="alphapai_mongo_uri",
        db_attr="alphapai_mongo_db",
        collection="reports",
        time_field="publish_time",
        url_field="web_url",
        preview_field="content",
        source_label="AlphaPai · 研报",
    ),
    SourceSpec(
        source="alphapai",
        uri_attr="alphapai_mongo_uri",
        db_attr="alphapai_mongo_db",
        collection="comments",
        time_field="publish_time",
        url_field="web_url",
        preview_field="content",
        source_label="AlphaPai · 点评",
    ),
    SourceSpec(
        source="alphapai",
        uri_attr="alphapai_mongo_uri",
        db_attr="alphapai_mongo_db",
        collection="wechat_articles",
        time_field="publish_time",
        url_field="web_url",
        preview_field="content",
        source_label="AlphaPai · 微信",
    ),
    SourceSpec(
        source="jinmen",
        uri_attr="jinmen_mongo_uri",
        db_attr="jinmen_mongo_db",
        collection="meetings",
        time_field="release_time",
        url_field="present_url",
        preview_field="points_md",
        source_label="进门 · 会议纪要",
    ),
    SourceSpec(
        source="meritco",
        uri_attr="meritco_mongo_uri",
        db_attr="meritco_mongo_db",
        collection="forum",
        time_field="release_time",
        url_field=None,
        preview_field="summary_md",
        source_label="久谦中台 · 论坛",
    ),
    SourceSpec(
        source="thirdbridge",
        uri_attr="thirdbridge_mongo_uri",
        db_attr="thirdbridge_mongo_db",
        collection="interviews",
        time_field="release_time",
        url_field=None,  # we build https://forum.thirdbridge.com/zh/interview/<uuid> client-side
        preview_field="agenda_md",
        source_label="高临 · 专家访谈",
    ),
    SourceSpec(
        source="funda",
        uri_attr="funda_mongo_uri",
        db_attr="funda_mongo_db",
        collection="posts",
        time_field="release_time",
        url_field="web_url",
        preview_field="content_md",
        source_label="Funda · 研究",
    ),
    SourceSpec(
        source="funda",
        uri_attr="funda_mongo_uri",
        db_attr="funda_mongo_db",
        collection="earnings_reports",
        time_field="release_time",
        url_field="web_url",
        preview_field="content_md",
        source_label="Funda · 8-K",
    ),
    SourceSpec(
        source="funda",
        uri_attr="funda_mongo_uri",
        db_attr="funda_mongo_db",
        collection="earnings_transcripts",
        time_field="release_time",
        url_field="web_url",
        preview_field="content_md",
        source_label="Funda · 业绩会",
    ),
    SourceSpec(
        source="gangtise",
        uri_attr="gangtise_mongo_uri",
        db_attr="gangtise_mongo_db",
        collection="summaries",
        time_field="release_time",
        url_field="web_url",
        preview_field="content_md",
        source_label="港推 · 纪要",
    ),
    SourceSpec(
        source="gangtise",
        uri_attr="gangtise_mongo_uri",
        db_attr="gangtise_mongo_db",
        collection="researches",
        time_field="release_time",
        url_field="web_url",
        preview_field="brief_md",
        source_label="港推 · 研报",
    ),
    SourceSpec(
        source="gangtise",
        uri_attr="gangtise_mongo_uri",
        db_attr="gangtise_mongo_db",
        collection="chief_opinions",
        time_field="release_time",
        url_field="web_url",
        preview_field="content_md",
        source_label="港推 · 首席观点",
    ),
    SourceSpec(
        source="acecamp",
        uri_attr="acecamp_mongo_uri",
        db_attr="acecamp_mongo_db",
        collection="articles",
        time_field="release_time",
        url_field="web_url",
        preview_field="content_md",
        source_label="本营 · 观点/纪要",
    ),
    SourceSpec(
        source="acecamp",
        uri_attr="acecamp_mongo_uri",
        db_attr="acecamp_mongo_db",
        collection="events",
        time_field="release_time",
        url_field="web_url",
        preview_field="description_md",
        source_label="本营 · 调研",
    ),
    SourceSpec(
        source="semianalysis",
        uri_attr="semianalysis_mongo_uri",
        db_attr="semianalysis_mongo_db",
        collection="semianalysis_posts",
        time_field="release_time",
        url_field="canonical_url",
        preview_field="content_md",
        source_label="SemiAnalysis · 研究",
    ),
]


@lru_cache(maxsize=8)
def _client(uri: str) -> AsyncIOMotorClient:
    return AsyncIOMotorClient(uri, tz_aware=True)


def _preview(text: Any, limit: int = 260) -> str:
    if not isinstance(text, str):
        return ""
    s = text.strip()
    return s[:limit] + ("…" if len(s) > limit else "")


async def _query_source(
    spec: SourceSpec,
    settings: Settings,
    canonical_id: str,
    *,
    from_date: str | None,
    to_date: str | None,
    per_source_limit: int,
) -> list[dict]:
    uri = getattr(settings, spec.uri_attr)
    db_name = getattr(settings, spec.db_attr)
    coll = _client(uri)[db_name][spec.collection]

    match: dict[str, Any] = {"_canonical_tickers": canonical_id}
    if from_date or to_date:
        rng: dict[str, str] = {}
        if from_date:
            rng["$gte"] = from_date
        if to_date:
            # exclusive upper bound: add "~" so any same-day time slips in
            rng["$lte"] = f"{to_date} 23:59"
        match[spec.time_field] = rng

    projection = {
        "title": 1,
        "_canonical_tickers": 1,
        spec.time_field: 1,
    }
    if spec.url_field:
        projection[spec.url_field] = 1
    if spec.preview_field:
        # Only grab the first 400 chars of content via $substr — cheaper
        projection[spec.preview_field] = {"$substrCP": [f"${spec.preview_field}", 0, 400]}

    # Third Bridge uses release_time_ms for sorting accuracy; fall back to string sort
    sort_field = spec.time_field
    cursor = (
        coll.find(match, projection=projection)
        .sort(sort_field, -1)
        .limit(per_source_limit)
    )

    items: list[dict] = []
    async for doc in cursor:
        # For thirdbridge, derive URL from uuid (known pattern)
        url = doc.get(spec.url_field) if spec.url_field else None
        if spec.source == "thirdbridge" and not url:
            url = f"https://forum.thirdbridge.com/zh/interview/{doc.get('_id')}"
        preview_raw = doc.get(spec.preview_field) if spec.preview_field else None
        items.append(
            {
                "source": spec.source,
                "collection": spec.collection,
                "source_label": spec.source_label,
                "id": str(doc.get("_id")),
                "title": doc.get("title") or "",
                "release_time": doc.get(spec.time_field),
                "url": url,
                "preview": _preview(preview_raw),
                "tickers": doc.get("_canonical_tickers") or [],
            }
        )
    return items


# --------------------------------------------------------------------------- #
# Response models
# --------------------------------------------------------------------------- #
class UnifiedItem(BaseModel):
    source: str
    collection: str
    source_label: str
    id: str
    title: str
    release_time: str | None
    url: str | None
    preview: str
    tickers: list[str]


class UnifiedResponse(BaseModel):
    canonical_id: str
    total: int
    by_source: dict[str, int]
    items: list[UnifiedItem]


class NormalizeResponse(BaseModel):
    matched: list[str]
    unmatched: list[str]


# --------------------------------------------------------------------------- #
# Endpoints
# --------------------------------------------------------------------------- #
@router.get("/by-symbol/{canonical_id}", response_model=UnifiedResponse)
async def by_symbol(
    canonical_id: str,
    limit: int = Query(200, ge=1, le=1000, description="Total items across all sources"),
    per_source_limit: int = Query(100, ge=1, le=500),
    sources: str | None = Query(
        None,
        description="Comma-separated source filter: alphapai,jinmen,meritco,thirdbridge,funda,gangtise",
    ),
    from_date: str | None = Query(
        None, description="Inclusive YYYY-MM-DD lower bound on release_time"
    ),
    to_date: str | None = Query(
        None, description="Inclusive YYYY-MM-DD upper bound on release_time"
    ),
    user: User = Depends(get_current_user),
):
    """All cross-platform mentions of one canonical ticker."""
    canonical_id = canonical_id.strip().upper().replace(" ", "")
    if "." not in canonical_id or len(canonical_id) > 32:
        raise HTTPException(
            400, "canonical_id must be in format CODE.MARKET, e.g. INTC.US or 603061.SH"
        )

    # Filter source specs
    specs = _SOURCES
    if sources:
        allowed = {s.strip() for s in sources.split(",") if s.strip()}
        specs = [s for s in specs if s.source in allowed]
        if not specs:
            raise HTTPException(400, f"No sources match {sources}")

    settings = get_settings()

    results = await asyncio.gather(
        *(
            _query_source(
                spec,
                settings,
                canonical_id,
                from_date=from_date,
                to_date=to_date,
                per_source_limit=per_source_limit,
            )
            for spec in specs
        ),
        return_exceptions=True,
    )

    flat: list[dict] = []
    by_source: dict[str, int] = {}
    for spec, res in zip(specs, results):
        if isinstance(res, Exception):
            logger.warning("unified.by_symbol: source %s failed: %s", spec.source, res)
            continue
        for item in res:
            by_source[item["source"]] = by_source.get(item["source"], 0) + 1
            flat.append(item)

    # Sort by release_time desc (strings are YYYY-MM-DD HH:MM so lex sort works)
    flat.sort(key=lambda x: x.get("release_time") or "", reverse=True)
    flat = flat[:limit]

    return UnifiedResponse(
        canonical_id=canonical_id,
        total=len(flat),
        by_source=by_source,
        items=[UnifiedItem(**x) for x in flat],
    )


@router.get("/normalize", response_model=NormalizeResponse)
async def normalize_endpoint(
    q: str = Query(..., description="Raw string — any ticker/name format"),
    user: User = Depends(get_current_user),
):
    """Debug helper: run the normalizer on raw input."""
    matched, unmatched = normalize_with_unmatched(q)
    return NormalizeResponse(matched=matched, unmatched=unmatched)


class SymbolSuggestion(BaseModel):
    alias: str
    canonical_id: str


@router.get("/symbols/search", response_model=list[SymbolSuggestion])
async def symbols_search(
    q: str = Query(..., min_length=1, description="Name or ticker fragment"),
    limit: int = Query(20, ge=1, le=100),
    user: User = Depends(get_current_user),
):
    """Probe the alias table for autocomplete."""
    q_low = q.strip().lower()
    table = _alias_table()
    # Also include direct normalize attempts (e.g. user types "603061")
    seen: set[str] = set()
    out: list[SymbolSuggestion] = []

    for alias, canonical in table.items():
        if q_low in alias.lower() or (canonical and q_low in canonical.lower()):
            if canonical and canonical not in seen:
                out.append(SymbolSuggestion(alias=alias, canonical_id=canonical))
                seen.add(canonical)
                if len(out) >= limit:
                    break

    # If the raw string normalizes directly, surface it first
    direct = normalize(q)
    for d in direct:
        if d not in seen:
            out.insert(0, SymbolSuggestion(alias=q, canonical_id=d))
            seen.add(d)

    return out[:limit]
