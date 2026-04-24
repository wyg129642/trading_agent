"""REST API exposing MongoDB-backed Funda (funda.ai) data.

Three collections populated by `crawl/funda/scraper.py`:
  - posts              (research articles)
  - earnings_reports   (8-K filings — content_md stripped from HTML)
  - earnings_transcripts (earnings-call transcripts — plain text)
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from typing import Any

BEIJING_TZ = timezone(timedelta(hours=8))

from fastapi import APIRouter, Depends, HTTPException, Query
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pydantic import BaseModel

from backend.app.config import get_settings
from backend.app.deps import get_current_user
from backend.app.models.user import User

logger = logging.getLogger(__name__)
router = APIRouter()


CATEGORY_COLLECTION = {
    "post": "posts",
    "earnings_report": "earnings_reports",
    "earnings_transcript": "earnings_transcripts",
}

CATEGORY_LABEL = {
    "post": "研究文章",
    "earnings_report": "财报 (8-K)",
    "earnings_transcript": "业绩会逐字稿",
}


@lru_cache(maxsize=1)
def _mongo_client() -> AsyncIOMotorClient:
    return AsyncIOMotorClient(get_settings().funda_mongo_uri, tz_aware=True)


def _db() -> AsyncIOMotorDatabase:
    return _mongo_client()[get_settings().funda_mongo_db]


def _extract_tickers(doc: dict) -> list[str]:
    """Ticker is either a top-level string or lives inside entities.tickers[]."""
    out: list[str] = []
    t = doc.get("ticker")
    if isinstance(t, str) and t:
        out.append(t)
    ent = doc.get("entities") or {}
    if isinstance(ent, dict):
        for tk in ent.get("tickers") or []:
            if isinstance(tk, str) and tk and tk not in out:
                out.append(tk)
    return out


def _tag_names(doc: dict) -> list[str]:
    tags = doc.get("tags") or []
    if not isinstance(tags, list):
        return []
    names: list[str] = []
    for t in tags:
        if isinstance(t, dict) and t.get("name"):
            names.append(str(t["name"]))
        elif isinstance(t, str):
            names.append(t)
    return names


def _brief(doc: dict) -> dict:
    content = doc.get("content_md") or ""
    excerpt = (doc.get("excerpt") or doc.get("subtitle") or "").strip()
    preview = excerpt if excerpt else content
    if len(preview) > 360:
        preview = preview[:360] + "…"
    stats = doc.get("stats") or {}
    return {
        "id": str(doc.get("_id")),
        "category": doc.get("category"),
        "category_label": CATEGORY_LABEL.get(doc.get("category") or "", "未知"),
        "title": doc.get("title"),
        "release_time": doc.get("release_time"),
        "web_url": doc.get("web_url"),
        "source_url": doc.get("sourceUrl") or "",
        "tickers": _extract_tickers(doc),
        "industry": doc.get("industry") or "",
        "year": doc.get("year"),
        "period": doc.get("period") or "",
        "access_level": doc.get("accessLevel") or "",
        "type": doc.get("type") or "",
        "tags": _tag_names(doc),
        "views": int(doc.get("views") or 0),
        "preview": preview,
        "stats": {
            "chars": int(stats.get("chars") or 0),
            "html_chars": int(stats.get("html_chars") or 0),
        },
        "has_html": bool(doc.get("content_html")),
        "crawled_at": doc.get("crawled_at"),
    }


class ItemListResponse(BaseModel):
    items: list[dict]
    total: int
    page: int
    page_size: int
    has_next: bool


class StatsResponse(BaseModel):
    total: int
    per_category: dict[str, int]
    today: dict[str, int]
    latest_per_category: dict[str, str | None]
    crawler_state: list[dict]
    daily_platform_stats: dict | None


@router.get("/items", response_model=ItemListResponse)
async def list_items(
    category: str = Query(
        "post",
        pattern="^(post|earnings_report|earnings_transcript)$",
    ),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    q: str | None = Query(None, description="Full-text on title/content_md/excerpt"),
    ticker: str | None = Query(None, description="Ticker filter (top-level or entities.tickers[])"),
    industry: str | None = None,
    user: User = Depends(get_current_user),
):
    coll = _db()[CATEGORY_COLLECTION[category]]
    match: dict[str, Any] = {}
    if q:
        match["$or"] = [
            {"title": {"$regex": q, "$options": "i"}},
            {"excerpt": {"$regex": q, "$options": "i"}},
            {"subtitle": {"$regex": q, "$options": "i"}},
            {"content_md": {"$regex": q, "$options": "i"}},
        ]
    if ticker:
        tk = ticker.upper()
        ticker_or = [
            {"ticker": {"$regex": f"^{tk}$", "$options": "i"}},
            {"entities.tickers": {"$regex": f"^{tk}$", "$options": "i"}},
        ]
        match = (
            {"$and": [match, {"$or": ticker_or}]}
            if match
            else {"$or": ticker_or}
        )
    if industry:
        match["industry"] = {"$regex": industry, "$options": "i"}

    total = await coll.count_documents(match)
    cursor = (
        coll.find(
            match,
            projection={
                "list_item": 0,
                "detail_result": 0,
                "content_html": 0,
                "content_md": 0,
            },
        )
        .sort("release_time_ms", -1)
        .skip((page - 1) * page_size)
        .limit(page_size)
    )
    items = [_brief(d) async for d in cursor]
    return ItemListResponse(
        items=items,
        total=total,
        page=page,
        page_size=page_size,
        has_next=(page * page_size) < total,
    )


@router.get("/items/{category}/{item_id}")
async def get_item(
    category: str,
    item_id: str,
    user: User = Depends(get_current_user),
):
    if category not in CATEGORY_COLLECTION:
        raise HTTPException(400, "Unknown category")
    coll = _db()[CATEGORY_COLLECTION[category]]
    doc = await coll.find_one({"_id": item_id})
    if not doc:
        doc = await coll.find_one({"id": item_id})
    if not doc:
        raise HTTPException(404, "Item not found")
    return {
        **_brief(doc),
        "content_md": doc.get("content_md") or "",
        "content_html": doc.get("content_html") or "",
        "preview_body": doc.get("previewBody") or "",
    }


# ============================================================ #
# 情绪因子 (funda.sentiments) — 由 crawl/funda/scraper.py --sentiment 灌入
# ============================================================ #

def _sentiment_brief(doc: dict) -> dict:
    return {
        "ticker": doc.get("ticker") or "",
        "date": doc.get("date") or "",
        "company": doc.get("company") or "",
        "sector": doc.get("sector") or "",
        "industry": doc.get("industry") or "",
        "reddit_score": doc.get("reddit_score"),
        "reddit_count": doc.get("reddit_count") or 0,
        "twitter_score": doc.get("twitter_score"),
        "twitter_count": doc.get("twitter_count") or 0,
        "ai_summary": doc.get("ai_summary") or "",
        "crawled_at": (doc.get("crawled_at").isoformat()
                       if hasattr(doc.get("crawled_at"), "isoformat")
                       else doc.get("crawled_at")),
    }


@router.get("/sentiment")
async def list_sentiment(
    tickers: str | None = Query(None, description="逗号分隔的 ticker 列表; 空=返回全部"),
    days: int = Query(1, ge=1, le=30, description="最近 N 天"),
    user: User = Depends(get_current_user),
):
    """列出情绪因子. 可按 tickers 过滤 (工作台 watchlist tickers) 或拉取全部.

    返回每 ticker 最新一条 (同 ticker 多天时按 date desc 取最新).
    `latest` = per-ticker 最新一条 (用于概览卡)
    `history` = 原始 items 列表 (最近 days 天, 用于画趋势)
    """
    db = _db()
    coll = db["sentiments"]

    # Build date filter (YYYY-MM-DD strings compare lexically)
    today = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d")
    from datetime import timedelta as _td
    cutoff = (datetime.now(BEIJING_TZ) - _td(days=days - 1)).strftime("%Y-%m-%d")

    match: dict[str, Any] = {"date": {"$gte": cutoff, "$lte": today}}
    if tickers:
        ticker_list = [t.strip() for t in tickers.split(",") if t.strip()]
        match["ticker"] = {"$in": ticker_list}

    # Full history (sorted desc)
    cursor = coll.find(match).sort([("date", -1), ("ticker", 1)]).limit(1000)
    history = [_sentiment_brief(d) async for d in cursor]

    # Latest per ticker (dedupe by ticker keeping first seen = most recent date)
    latest_by_ticker: dict[str, dict] = {}
    for item in history:
        t = item["ticker"]
        if t and t not in latest_by_ticker:
            latest_by_ticker[t] = item
    latest = sorted(
        latest_by_ticker.values(),
        key=lambda x: -(x.get("twitter_score") or x.get("reddit_score") or 0),
    )

    return {
        "date_range": {"from": cutoff, "to": today, "days": days},
        "total": await coll.count_documents(match),
        "latest": latest,
        "history": history,
    }


@router.get("/sentiment/my-watchlist")
async def my_watchlist_sentiment(
    days: int = Query(7, ge=1, le=30),
    user: User = Depends(get_current_user),
):
    """取"持仓概览"里所有 ticker 的情绪因子.

    "工作台" 的定义按用户明确要求 = Dashboard 持仓概览里的股票,
    数据源是 config/portfolio_sources.yaml (同 /api/sources/portfolio).
    """
    # 避免循环依赖, 懒 import
    from backend.app.api.sources import _load_portfolio_yaml

    raw = _load_portfolio_yaml()
    # 同 sources.py 的去重逻辑: 按 stock_ticker 去重
    seen: dict[str, dict] = {}
    for s in raw:
        ticker = (s.get("stock_ticker") or "").strip().upper()
        if not ticker:
            continue
        if ticker not in seen:
            seen[ticker] = {
                "ticker": ticker,
                "name": s.get("stock_name") or s.get("name") or "",
                "market": s.get("stock_market") or s.get("market") or "",
            }
    tickers = sorted(seen.keys())

    # Funda 用 Yahoo 风格的 ticker (US 裸码, 其他加后缀 .KS/.HK/.SZ/.SS/.T).
    # 持仓的 ticker 大多是裸码, 为每只股生成候选集 + 反向映射到原始 ticker.
    def _variants(t: str, market: str) -> list[str]:
        t = t.upper()
        m = (market or "").upper()
        out = {t}
        # A 股: 0/3 开头 = 深圳 (.SZ), 6 开头 = 上海 (.SS)
        if t.isdigit() and len(t) == 6:
            if t[0] == "6":
                out.add(f"{t}.SS")
            else:
                out.add(f"{t}.SZ")
        # 港股: 数字, 通常 1-5 位, 加 .HK; funda 也可能用前导 0 补 4 位
        elif t.isdigit() and 1 <= len(t) <= 5:
            out.add(f"{t}.HK")
            out.add(f"{t.zfill(4)}.HK")
            out.add(f"{t.zfill(5)}.HK")
            out.add(t.zfill(4))
            out.add(t.zfill(5))
        # 韩股: 6 位数字 + .KS
        if t.isdigit() and len(t) == 6:
            out.add(f"{t}.KS")
        # 日股: 4 位数字 + .T (根据 market 判断)
        if "日" in (market or "") or "JP" in m.upper():
            out.add(f"{t}.T")
        return sorted(out)

    all_variants: list[str] = []
    variant_to_portfolio: dict[str, str] = {}
    for t, info in seen.items():
        for v in _variants(t, info["market"]):
            all_variants.append(v)
            variant_to_portfolio.setdefault(v, t)

    if not tickers:
        today_s = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d")
        return {
            "tickers_in_watchlist": [],
            "covered_tickers": [],
            "missing_tickers": [],
            "date_range": {"from": today_s, "to": today_s, "days": days},
            "latest": [],
            "history": [],
            "note": "持仓概览 (config/portfolio_sources.yaml) 是空的, 请先添加股票",
        }

    # 从 sentiments 里拉这些 ticker (及其后缀变种) 的数据
    db = _db()
    coll = db["sentiments"]
    today = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d")
    from datetime import timedelta as _td
    cutoff = (datetime.now(BEIJING_TZ) - _td(days=days - 1)).strftime("%Y-%m-%d")
    match = {"date": {"$gte": cutoff, "$lte": today},
             "ticker": {"$in": all_variants}}
    cursor = coll.find(match).sort([("date", -1), ("ticker", 1)]).limit(2000)

    # 把每条响应的 funda ticker 映射回持仓里的裸码, 带上持仓的中文名 + market 标签
    history: list[dict] = []
    async for d in cursor:
        brief = _sentiment_brief(d)
        funda_ticker = brief["ticker"]
        portfolio_ticker = variant_to_portfolio.get(funda_ticker, funda_ticker)
        info = seen.get(portfolio_ticker) or {}
        brief["portfolio_ticker"] = portfolio_ticker
        brief["funda_ticker"] = funda_ticker
        brief["ticker"] = portfolio_ticker           # 前端按持仓 ticker 显示
        brief["stock_name"] = info.get("name") or brief.get("company") or ""
        brief["stock_market"] = info.get("market") or ""
        history.append(brief)

    # 按持仓 ticker 去重, 同一 ticker 多个变种 / 多天记录取最新
    latest_by_ticker: dict[str, dict] = {}
    for item in history:
        t = item["ticker"]
        if t and t not in latest_by_ticker:
            latest_by_ticker[t] = item
    covered = sorted(latest_by_ticker.keys())
    latest = sorted(
        latest_by_ticker.values(),
        key=lambda x: -(x.get("twitter_score") or x.get("reddit_score") or 0),
    )

    # 未覆盖的股也带上名字 + market, 前端底部列表美观些
    covered_set = set(covered)
    missing = [
        {"ticker": t,
         "stock_name": seen[t].get("name") or "",
         "stock_market": seen[t].get("market") or ""}
        for t in tickers if t not in covered_set
    ]

    return {
        "tickers_in_watchlist": tickers,
        "covered_tickers": covered,
        "missing_tickers": [t for t in tickers if t not in covered_set],
        "missing_tickers_detail": missing,
        "date_range": {"from": cutoff, "to": today, "days": days},
        "latest": latest,
        "history": history,
    }


def _classify_trend(delta: float, latest: float | None) -> tuple[str, float]:
    if latest is None:
        return ("LOW_DATA", 0.0)
    if delta >= 1.5:
        return ("HEATING", delta)
    if delta >= 0.5:
        return ("WARMING", delta)
    if delta <= -1.5:
        return ("FALLING", delta)
    if delta <= -0.5:
        return ("COOLING", delta)
    return ("STABLE", delta)


def _resolve_date_range(date_from: str | None, date_to: str | None) -> tuple[str, str, list[str]]:
    """Normalize from/to and produce date list (newest first)."""
    today_dt = datetime.now(BEIJING_TZ)
    if not date_to:
        date_to = today_dt.strftime("%Y-%m-%d")
    if not date_from:
        date_from = (today_dt - timedelta(days=6)).strftime("%Y-%m-%d")
    try:
        from_dt = datetime.strptime(date_from, "%Y-%m-%d")
        to_dt = datetime.strptime(date_to, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(400, "date 参数必须是 YYYY-MM-DD")
    if to_dt < from_dt:
        raise HTTPException(400, "to < from")
    dates: list[str] = []
    cur = to_dt
    while cur >= from_dt:
        dates.append(cur.strftime("%Y-%m-%d"))
        cur -= timedelta(days=1)
    return date_from, date_to, dates


def _finalize_trend_rows(
    by_ticker: dict[str, dict],
    min_scored_days: int,
) -> tuple[list[dict], int, set[str], set[str]]:
    """Classify trend + collect sectors/industries."""
    rows: list[dict] = []
    total_scored = 0
    sectors_set: set[str] = set()
    industries_set: set[str] = set()
    for row in by_ticker.values():
        if row.get("sector"):
            sectors_set.add(row["sector"])
        if row.get("industry"):
            industries_set.add(row["industry"])
        sorted_dates = sorted(row["scores"].keys())
        if sorted_dates:
            latest_score = row["scores"][sorted_dates[-1]]
            earliest_score = row["scores"][sorted_dates[0]]
            delta = round(latest_score - earliest_score, 2)
        else:
            latest_score = None
            delta = 0.0
        trend_label, delta_val = _classify_trend(delta, latest_score)
        scored_days = len(row["scores"])
        if scored_days >= min_scored_days:
            total_scored += 1
        row.update({
            "trend_label": trend_label,
            "trend_delta": delta_val,
            "latest_score": latest_score,
            "latest_date": sorted_dates[-1] if sorted_dates else None,
            "scored_days": scored_days,
            "low_data": scored_days < min_scored_days,
        })
        rows.append(row)
    return rows, total_scored, sectors_set, industries_set


@router.get("/sentiment/dashboard")
async def sentiment_dashboard(
    date_from: str | None = Query(None, alias="from", description="YYYY-MM-DD 起始 (含)"),
    date_to: str | None = Query(None, alias="to", description="YYYY-MM-DD 截止 (含)"),
    ticker: str | None = Query(None, description="模糊匹配 ticker (前缀 / 包含)"),
    sector: str | None = Query(None, description="精确匹配 sector"),
    industry: str | None = Query(None, description="精确匹配 industry"),
    trend: str | None = Query(None, description="warming|cooling|stable|heating|falling|all"),
    min_scored_days: int = Query(2, ge=1, le=30, description="少于此天数视为 Low Data 隐藏"),
    user: User = Depends(get_current_user),
):
    """Sentiment Changes Dashboard — 镜像 funda.ai 的跨 ticker 趋势网格.

    返回每 ticker 一行, 行内按 date 列出 0-10 分 (取 twitter_score, 缺则 reddit_score).
    Trend = 区间内最新日均分 - 最早日均分, 按阈值标签 WARMING/COOLING 等.
    """
    db = _db()
    coll = db["sentiments"]

    date_from, date_to, dates = _resolve_date_range(date_from, date_to)

    match: dict[str, Any] = {"date": {"$gte": date_from, "$lte": date_to}}
    if ticker:
        match["ticker"] = {"$regex": f"^{ticker.upper()}", "$options": "i"}
    if sector:
        match["sector"] = sector
    if industry:
        match["industry"] = industry

    cursor = coll.find(match, projection={
        "ticker": 1, "date": 1, "company": 1, "sector": 1, "industry": 1,
        "twitter_score": 1, "reddit_score": 1,
        "twitter_count": 1, "reddit_count": 1,
    }).sort([("ticker", 1), ("date", -1)])

    # 以 ticker 归并
    by_ticker: dict[str, dict] = {}
    async for d in cursor:
        t = d.get("ticker") or ""
        if not t:
            continue
        row = by_ticker.setdefault(t, {
            "ticker": t,
            "company": d.get("company") or "",
            "sector": d.get("sector") or "",
            "industry": d.get("industry") or "",
            "scores": {},          # date → score
            "counts": {},          # date → count (for low-confidence marker)
        })
        # 每天优先取 twitter_score (funda 主指标); 没有才 fallback reddit
        tw = d.get("twitter_score")
        rd = d.get("reddit_score")
        score = tw if isinstance(tw, (int, float)) else (
            rd if isinstance(rd, (int, float)) else None
        )
        count = (d.get("twitter_count") or 0) if isinstance(tw, (int, float)) else (
            d.get("reddit_count") or 0
        )
        if score is not None:
            row["scores"][d.get("date")] = round(float(score), 2)
            row["counts"][d.get("date")] = int(count)
        # 保持 company/sector/industry 第一次非空值
        for k in ("company", "sector", "industry"):
            if not row[k] and d.get(k):
                row[k] = d[k]

    rows, total_scored, sectors_set, industries_set = _finalize_trend_rows(
        by_ticker, min_scored_days
    )

    # 过滤 trend
    if trend and trend.lower() != "all":
        want = trend.upper()
        rows = [r for r in rows if r["trend_label"] == want]

    # 排序: 非 low_data 在前, 再按 |delta| desc
    rows.sort(key=lambda r: (
        1 if r["low_data"] else 0,
        -abs(r.get("trend_delta") or 0.0),
    ))

    return {
        "date_range": {"from": date_from, "to": date_to, "days": len(dates), "dates": dates},
        "total_tickers": len(rows),
        "total_scored": total_scored,
        "rows": rows,
        "sectors": sorted(sectors_set),
        "industries": sorted(industries_set),
        "trend_labels": ["HEATING", "WARMING", "STABLE", "COOLING", "FALLING", "LOW_DATA"],
    }


def _portfolio_ticker_variants(t: str, market: str) -> list[str]:
    """持仓裸码 → funda.ai (Yahoo 风) 候选变种.

    与 sentiment/my-watchlist 保持一致: US 裸码直给, 其他加 .HK/.SZ/.SS/.KS/.T.
    """
    t = (t or "").upper()
    m = (market or "").upper()
    out = {t}
    if t.isdigit() and len(t) == 6:
        if t[0] == "6":
            out.add(f"{t}.SS")
        else:
            out.add(f"{t}.SZ")
        out.add(f"{t}.KS")
    elif t.isdigit() and 1 <= len(t) <= 5:
        out.add(f"{t}.HK")
        out.add(f"{t.zfill(4)}.HK")
        out.add(f"{t.zfill(5)}.HK")
        out.add(t.zfill(4))
        out.add(t.zfill(5))
    if "日" in (market or "") or "JP" in m:
        out.add(f"{t}.T")
    return sorted(out)


@router.get("/sentiment/dashboard/my-portfolio")
async def sentiment_dashboard_my_portfolio(
    date_from: str | None = Query(None, alias="from"),
    date_to: str | None = Query(None, alias="to"),
    trend: str | None = Query(None),
    min_scored_days: int = Query(1, ge=1, le=30,
        description="持仓股默认 1 天即可显示 (避免空集)"),
    user: User = Depends(get_current_user),
):
    """持仓情绪趋势 — 仅 config/portfolio_sources.yaml 里的股票.

    与 /sentiment/dashboard 同 schema, 额外在每行携带:
      - stock_name, stock_market (持仓 YAML 的中文名 / 市场标签)
      - funda_ticker (funda.ai 原始带后缀 ticker, 便于 debug)
    ticker 字段一律回映为持仓裸码.
    """
    # 懒 import 避免循环
    from backend.app.api.sources import _load_portfolio_yaml

    raw = _load_portfolio_yaml()
    seen: dict[str, dict] = {}
    for s in raw:
        t = (s.get("stock_ticker") or "").strip().upper()
        if not t:
            continue
        if t not in seen:
            seen[t] = {
                "ticker": t,
                "stock_name": s.get("stock_name") or s.get("name") or "",
                "stock_market": s.get("stock_market") or s.get("market") or "",
            }
    portfolio_tickers = sorted(seen.keys())

    date_from, date_to, dates = _resolve_date_range(date_from, date_to)

    # 没持仓直接返回空壳
    if not portfolio_tickers:
        return {
            "date_range": {"from": date_from, "to": date_to, "days": len(dates), "dates": dates},
            "total_tickers": 0,
            "total_scored": 0,
            "rows": [],
            "sectors": [],
            "industries": [],
            "trend_labels": ["HEATING", "WARMING", "STABLE", "COOLING", "FALLING", "LOW_DATA"],
            "portfolio_total": 0,
            "portfolio_covered": 0,
            "portfolio_missing": [],
            "note": "持仓概览 (config/portfolio_sources.yaml) 是空的",
        }

    # 构建变种 -> 持仓裸码 的反向映射
    all_variants: list[str] = []
    variant_to_portfolio: dict[str, str] = {}
    for pt, info in seen.items():
        for v in _portfolio_ticker_variants(pt, info["stock_market"]):
            all_variants.append(v)
            variant_to_portfolio.setdefault(v, pt)

    db = _db()
    coll = db["sentiments"]
    match: dict[str, Any] = {
        "date": {"$gte": date_from, "$lte": date_to},
        "ticker": {"$in": all_variants},
    }
    cursor = coll.find(match, projection={
        "ticker": 1, "date": 1, "company": 1, "sector": 1, "industry": 1,
        "twitter_score": 1, "reddit_score": 1,
        "twitter_count": 1, "reddit_count": 1,
    }).sort([("ticker", 1), ("date", -1)])

    # 以 "持仓裸码" 归并 (多个变种可能命中同一只股, 取最早出现的为准)
    by_ticker: dict[str, dict] = {}
    async for d in cursor:
        funda_t = d.get("ticker") or ""
        pt = variant_to_portfolio.get(funda_t, funda_t)
        if not pt:
            continue
        info = seen.get(pt) or {}
        row = by_ticker.setdefault(pt, {
            "ticker": pt,
            "funda_ticker": funda_t if funda_t != pt else None,
            "company": d.get("company") or "",
            "sector": d.get("sector") or "",
            "industry": d.get("industry") or "",
            "stock_name": info.get("stock_name") or "",
            "stock_market": info.get("stock_market") or "",
            "scores": {},
            "counts": {},
        })
        tw = d.get("twitter_score")
        rd = d.get("reddit_score")
        score = tw if isinstance(tw, (int, float)) else (
            rd if isinstance(rd, (int, float)) else None
        )
        count = (d.get("twitter_count") or 0) if isinstance(tw, (int, float)) else (
            d.get("reddit_count") or 0
        )
        dt = d.get("date")
        if score is not None and dt not in row["scores"]:
            row["scores"][dt] = round(float(score), 2)
            row["counts"][dt] = int(count)
        for k in ("company", "sector", "industry"):
            if not row[k] and d.get(k):
                row[k] = d[k]

    rows, total_scored, sectors_set, industries_set = _finalize_trend_rows(
        by_ticker, min_scored_days
    )

    if trend and trend.lower() != "all":
        want = trend.upper()
        rows = [r for r in rows if r["trend_label"] == want]

    # 排序: 非 low_data 在前, 再按 |delta| desc
    rows.sort(key=lambda r: (
        1 if r["low_data"] else 0,
        -abs(r.get("trend_delta") or 0.0),
    ))

    covered = {r["ticker"] for r in by_ticker.values() if r["scores"]}
    missing = [
        {"ticker": t, "stock_name": seen[t]["stock_name"],
         "stock_market": seen[t]["stock_market"]}
        for t in portfolio_tickers if t not in covered
    ]

    return {
        "date_range": {"from": date_from, "to": date_to, "days": len(dates), "dates": dates},
        "total_tickers": len(rows),
        "total_scored": total_scored,
        "rows": rows,
        "sectors": sorted(sectors_set),
        "industries": sorted(industries_set),
        "trend_labels": ["HEATING", "WARMING", "STABLE", "COOLING", "FALLING", "LOW_DATA"],
        "portfolio_total": len(portfolio_tickers),
        "portfolio_covered": len(covered),
        "portfolio_missing": missing,
    }


@router.get("/stats", response_model=StatsResponse)
async def get_stats(user: User = Depends(get_current_user)):
    db = _db()
    now_cn = datetime.now(BEIJING_TZ)
    day_start_cn = now_cn.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end_cn = day_start_cn + timedelta(days=1)
    today_start_ms = int(day_start_cn.timestamp() * 1000)
    today_end_ms = int(day_end_cn.timestamp() * 1000)
    today_str = now_cn.strftime("%Y-%m-%d")

    per_category: dict[str, int] = {}
    today: dict[str, int] = {}
    latest: dict[str, str | None] = {}

    for cat, coll_name in CATEGORY_COLLECTION.items():
        coll = db[coll_name]
        per_category[cat] = await coll.count_documents({})
        today[cat] = await coll.count_documents(
            {"release_time_ms": {"$gte": today_start_ms, "$lt": today_end_ms}}
        )
        latest_doc = await coll.find_one(
            {}, sort=[("release_time_ms", -1)], projection={"release_time": 1}
        )
        latest[cat] = latest_doc.get("release_time") if latest_doc else None

    # Crawler checkpoints (one per category)
    state_coll = db["_state"]
    crawler_state: list[dict] = []
    async for s in state_coll.find({"_id": {"$regex": "^crawler_"}}):
        key = str(s["_id"]).replace("crawler_", "")
        crawler_state.append(
            {
                "category": key,
                "label": CATEGORY_LABEL.get(key, key),
                "in_progress": bool(s.get("in_progress")),
                "last_processed_at": s.get("last_processed_at"),
                "last_run_end_at": s.get("last_run_end_at"),
                "last_run_stats": s.get("last_run_stats") or {},
                "top_id": s.get("top_id"),
            }
        )

    daily_doc = await state_coll.find_one({"_id": f"daily_{today_str}"})
    daily_platform_stats = None
    if daily_doc:
        daily_platform_stats = {
            cat: {
                "platform_count": (daily_doc.get(cat) or {}).get("platform_count", 0),
                "in_db": (daily_doc.get(cat) or {}).get("in_db", 0),
                "missing": (daily_doc.get(cat) or {}).get("missing", 0),
            }
            for cat in CATEGORY_COLLECTION
        }

    return StatsResponse(
        total=sum(per_category.values()),
        per_category=per_category,
        today=today,
        latest_per_category=latest,
        crawler_state=crawler_state,
        daily_platform_stats=daily_platform_stats,
    )
