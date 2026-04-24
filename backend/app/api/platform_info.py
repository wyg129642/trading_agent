"""AlphaPai platform homepage info proxy + aggregator.

Exposes a unified read-only feed of the homepage widgets (hot searches, hot
stocks, daily summary topics, institution-preferred stocks, etc.) by
proxying the same endpoints the SPA itself calls. Shared cache (60s) avoids
burning API quota when multiple clients hit the page simultaneously.

Endpoints are read from the alphapai_crawl credentials.json token — no extra
account setup needed. Cached for 60s.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Optional

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query
from functools import lru_cache as _lru_cache
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

from backend.app.config import get_settings
from backend.app.deps import get_current_user
from backend.app.models.user import User

logger = logging.getLogger(__name__)
router = APIRouter()

ALPHAPAI_API = "https://alphapai-web.rabyte.cn/external/alpha/api"
CREDS_PATH = Path("/home/ygwang/trading_agent/crawl/alphapai_crawl/credentials.json")

_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
       "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")


def _load_token() -> str:
    try:
        return json.loads(CREDS_PATH.read_text()).get("token", "")
    except Exception:
        return ""


_CACHE: dict[str, tuple[float, Any]] = {}
# Short TTL so the sidebar feels live. Frontend polls every 15s;
# at TTL=20s each client call triggers a backend refresh ~every 2nd hit,
# which keeps load light while surfacing new topics / counts within 15-35s
# of them appearing on the platform itself.
_CACHE_TTL = 20.0
_CACHE_LOCK = asyncio.Lock()


async def _alphapai_call(method: str, path: str, body: Optional[dict] = None,
                          timeout: float = 15.0) -> dict:
    token = _load_token()
    if not token:
        raise HTTPException(503, "alphapai credentials missing")
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "Referer": "https://alphapai-web.rabyte.cn/",
        "Origin":  "https://alphapai-web.rabyte.cn",
        "Authorization": token,
        "User-Agent": _UA,
        "x-from": "web",
        "platform": "web",
    }
    url = f"{ALPHAPAI_API}/{path.lstrip('/')}"
    async with httpx.AsyncClient(timeout=timeout, trust_env=False) as c:
        if method == "GET":
            r = await c.get(url, headers=headers)
        else:
            r = await c.request(method, url, headers=headers, json=body or {})
    if r.status_code != 200:
        raise HTTPException(502, f"alphapai {method} {path}: HTTP {r.status_code}")
    j = r.json()
    if j.get("code") != 200000:
        raise HTTPException(502, f"alphapai {method} {path}: code={j.get('code')} msg={j.get('message')}")
    return j.get("data") or {}


async def _cached(key: str, fetch):
    now = time.time()
    hit = _CACHE.get(key)
    if hit and now - hit[0] < _CACHE_TTL:
        return hit[1]
    async with _CACHE_LOCK:
        hit = _CACHE.get(key)
        if hit and time.time() - hit[0] < _CACHE_TTL:
            return hit[1]
        data = await fetch()
        _CACHE[key] = (time.time(), data)
        return data


# ── Individual widgets ──────────────────────────────────────────────────

@router.get("/hot-words", summary="首页热搜词")
async def hot_words(_: User = Depends(get_current_user)):
    """Words trending in search. Updates ~every hour."""
    async def fetch():
        data = await _alphapai_call("GET", "reading/hot/word")
        if not isinstance(data, list):
            return {"list": []}
        return {"list": sorted(data, key=lambda x: x.get("score", 999))}
    return await _cached("hot_words", fetch)


@router.get("/hot-stocks", summary="首页热度个股")
async def hot_stocks(_: User = Depends(get_current_user)):
    """Stocks with the most reports/mentions today."""
    async def fetch():
        data = await _alphapai_call("GET", "reading/stock/hot/recommend")
        if not isinstance(data, list):
            return {"list": []}
        return {"list": data}
    return await _cached("hot_stocks", fetch)


@router.get("/public-fund-stocks", summary="公募私募热议个股 (胖子模式)")
async def public_fund_stocks(_: User = Depends(get_current_user)):
    """Public funds + private funds hot stock list (institution-segmented)."""
    async def fetch():
        data = await _alphapai_call("GET", "mix/hot/topic/stock/list")
        return data if isinstance(data, dict) else {"publicList": [], "privateList": []}
    return await _cached("public_fund_stocks", fetch)


@router.get("/hot-topics", summary="热议话题 (带摘要)")
async def hot_topics(
    limit: int = Query(20, ge=1, le=100),
    _: User = Depends(get_current_user),
):
    """Current batch of hot-debated topics with AI summary."""
    async def fetch():
        data = await _alphapai_call("GET", "mix/hot/topic/current/batch/list")
        if not isinstance(data, list):
            return {"list": [], "updated_at": None}
        # fetch the batch update time
        try:
            t = await _alphapai_call("GET", "mix/hot/topic/current/batch/list/update/time")
        except Exception:
            t = None
        return {"list": data, "updated_at": t if isinstance(t, str) else None}
    data = await _cached("hot_topics", fetch)
    return {"list": (data.get("list") or [])[:limit], "updated_at": data.get("updated_at")}


@router.get("/daily-summary", summary="每日早/午/晚版研报摘要")
async def daily_summary(_: User = Depends(get_current_user)):
    """The 早/午/晚版 daily summary (一句话概括当日市场动态)."""
    async def fetch():
        data = await _alphapai_call("GET", "mix/hot/topic/report/latest/v2")
        if isinstance(data, list):
            return {"list": data}
        if isinstance(data, dict):
            return {"list": [data]}
        return {"list": []}
    return await _cached("daily_summary", fetch)


@router.get("/hot-reports", summary="热门研报推荐")
async def hot_reports(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    _: User = Depends(get_current_user),
):
    """Top N hot-recommend reports (ranked, not chrono)."""
    async def fetch():
        data = await _alphapai_call("POST", "reading/report/hot/recommend",
                                      {"pageNum": page, "pageSize": page_size})
        return data
    return await _cached(f"hot_reports:{page}:{page_size}", fetch)


@router.get("/hot-roadshows", summary="热门路演推荐")
async def hot_roadshows(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    _: User = Depends(get_current_user),
):
    """Top N hot roadshow summaries."""
    async def fetch():
        data = await _alphapai_call("POST", "reading/roadshow/summary/hot/recommend",
                                      {"pageNum": page, "pageSize": page_size})
        return data
    return await _cached(f"hot_roadshows:{page}:{page_size}", fetch)


@router.get("/today-counts", summary="今日各类内容计数")
async def today_counts(_: User = Depends(get_current_user)):
    """Today's counts: roadshowSummaryNum / investigationNum / industryWechatArticleNum /
    institutionWechatArticleNum / commentNum + research calendar (研报日历)."""
    async def fetch():
        today = datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d")
        count = await _alphapai_call("GET", "reading/count/today")
        # CDP verified 2026-04-23 17:15: the "今日 N 篇" big card on the SPA 研报 页
        # renders from `reading/report/count/today.data.total` (ahNum+usNum+
        # independentNum).  `calendar.todayNum` is a different, slightly larger
        # counter shown only in the date panel/footer. Frontend "今日研报" card
        # aligns with the 3-tab sum so DB ≈ SPA header at all times.
        report_count = await _alphapai_call("GET", "reading/report/count/today")
        cal = await _alphapai_call("POST", "reading/report/calendar",
                                     {"startDate": today, "endDate": today})
        yday = (datetime.now(timezone(timedelta(hours=8))) - timedelta(days=1)).strftime("%Y-%m-%d")
        cal_range = await _alphapai_call("POST", "reading/report/calendar",
                                           {"startDate": yday, "endDate": today})
        report_today_num = None
        report_breakdown = {}
        if isinstance(report_count, dict):
            report_today_num = report_count.get("total")
            report_breakdown = {
                "ahNum": report_count.get("ahNum"),
                "usNum": report_count.get("usNum"),
                "independentNum": report_count.get("independentNum"),
                "total": report_count.get("total"),
            }
        return {
            "today": today,
            "count_today": count if isinstance(count, dict) else {},
            # Aligned to the SPA header's big card number (ah + us + independent).
            "report_today_num": report_today_num,
            "report_breakdown": report_breakdown,
            # calendar fields kept for the date-chart / yesterday / weekly widgets.
            "report_calendar_today_num": cal.get("todayNum") if isinstance(cal, dict) else None,
            "report_yesterday_num": cal.get("yesterdayNum") if isinstance(cal, dict) else None,
            "report_week_num": cal.get("thisWeekNum") if isinstance(cal, dict) else None,
            "report_calendar_last_7d": (cal_range.get("calendar") or {}) if isinstance(cal_range, dict) else {},
        }
    return await _cached("today_counts", fetch)


@router.get("/suggested-questions", summary="AI派派建议问题")
async def suggested_questions(_: User = Depends(get_current_user)):
    """AI-suggested questions the platform shows as conversation starters."""
    async def fetch():
        data = await _alphapai_call("GET", "mix/question/list")
        return {"list": data if isinstance(data, list) else []}
    return await _cached("suggested_questions", fetch)


@router.get("/info-flow", summary="跟踪个股 + 行业 资讯流")
async def info_flow(
    page: int = Query(1, ge=1),
    page_size: int = Query(30, ge=1, le=100),
    _: User = Depends(get_current_user),
):
    """My-focus feed: stock info + institution wechat + comments + announcements.
    This is the SPA's /reading/home/my-focus feed."""
    async def fetch():
        data = await _alphapai_call(
            "POST", "reading/information/flow/stock/information/list",
            {"pageNum": page, "pageSize": page_size},
        )
        return data
    return await _cached(f"info_flow:{page}:{page_size}", fetch)


# ── Aggregated summary for sidebar landing page ─────────────────────────

@router.get("/summary", summary="平台信息首页聚合")
async def summary(_: User = Depends(get_current_user)):
    """All-in-one homepage summary for the new sidebar tab.
    Combines today counts + hot words + hot stocks + daily summary + topics."""
    # Fetch in parallel; each already cached.
    try:
        counts, words, stocks, public_stocks, topics, daily = await asyncio.gather(
            today_counts(_=_),
            hot_words(_=_),
            hot_stocks(_=_),
            public_fund_stocks(_=_),
            hot_topics(limit=20, _=_),
            daily_summary(_=_),
            return_exceptions=True,
        )
    except Exception as e:
        raise HTTPException(502, f"summary fetch error: {e}")

    def _safe(x):
        return x if not isinstance(x, Exception) else {"error": str(x)}

    return {
        "counts":        _safe(counts),
        "hot_words":     _safe(words),
        "hot_stocks":    _safe(stocks),
        "public_fund_stocks": _safe(public_stocks),
        "hot_topics":    _safe(topics),
        "daily_summary": _safe(daily),
        "generated_at":  datetime.now(timezone.utc).isoformat(),
    }


# ============================================================
# AceCamp 本营 主页信息聚合
# 代理 api.acecamptech.com 的 feeds/opinions/popular_corporations 端点,
# 对标 AlphaPai 的首页信息. 用的是 crawl/AceCamp/credentials.json 里的
# cookie 凭证, 30s 内共享缓存避免刷爆额度.
# ============================================================

ACECAMP_API = "https://api.acecamptech.com/api/v1"
ACECAMP_CREDS_PATH = Path("/home/ygwang/trading_agent/crawl/AceCamp/credentials.json")
_ACECAMP_CACHE: dict[str, tuple[float, Any]] = {}
_ACECAMP_CACHE_TTL = 30.0
_ACECAMP_LOCK = asyncio.Lock()


def _acecamp_cookie() -> str:
    try:
        return json.loads(ACECAMP_CREDS_PATH.read_text()).get("cookie", "")
    except Exception:
        return ""


async def _acecamp_call(path: str, params: Optional[dict] = None,
                        timeout: float = 15.0) -> Any:
    cookie = _acecamp_cookie()
    if not cookie:
        raise HTTPException(503, "acecamp credentials missing")
    headers = {
        "Cookie": cookie,
        "Accept": "application/json, text/plain, */*",
        "Origin":  "https://www.acecamptech.com",
        "Referer": "https://www.acecamptech.com/",
        "User-Agent": _UA,
        "X-Requested-With": "XMLHttpRequest",
    }
    merged = {"version": "2.0"}
    if params:
        merged.update({k: v for k, v in params.items() if v is not None})
    url = f"{ACECAMP_API}/{path.lstrip('/')}"
    async with httpx.AsyncClient(timeout=timeout, trust_env=False) as c:
        r = await c.get(url, headers=headers, params=merged)
    if r.status_code in (401, 403):
        raise HTTPException(401, f"acecamp auth expired ({r.status_code})")
    if r.status_code != 200:
        raise HTTPException(502, f"acecamp {path}: HTTP {r.status_code}")
    j = r.json()
    code = j.get("code")
    # AceCamp 用 code=200 表示业务成功, 其他值一律报错
    if code != 200:
        raise HTTPException(502, f"acecamp {path}: code={code} msg={j.get('msg') or j.get('message')}")
    return j.get("data")


async def _acecamp_cached(key: str, fetch):
    now = time.time()
    hit = _ACECAMP_CACHE.get(key)
    if hit and now - hit[0] < _ACECAMP_CACHE_TTL:
        return hit[1]
    async with _ACECAMP_LOCK:
        hit = _ACECAMP_CACHE.get(key)
        if hit and time.time() - hit[0] < _ACECAMP_CACHE_TTL:
            return hit[1]
        data = await fetch()
        _ACECAMP_CACHE[key] = (time.time(), data)
        return data


@router.get("/acecamp/hot-keywords", summary="AceCamp 热搜关键词")
async def acecamp_hot_keywords(_: User = Depends(get_current_user)):
    async def fetch():
        data = await _acecamp_call("feeds/trends")
        return {"list": data if isinstance(data, list) else []}
    return await _acecamp_cached("ace_hot_kw", fetch)


@router.get("/acecamp/today-counts", summary="AceCamp 今日 / 本周发布统计")
async def acecamp_today_counts(_: User = Depends(get_current_user)):
    """Returns per-type counts from the platform itself — 今日 + 本周."""
    async def fetch():
        day = await _acecamp_call("feeds/statistics", {"date_type": "day"}) or {}
        week = await _acecamp_call("feeds/statistics", {"date_type": "week"}) or {}
        return {"today": day, "week": week,
                "updated_at": datetime.now(timezone.utc).isoformat()}
    return await _acecamp_cached("ace_today_counts", fetch)


@router.get("/acecamp/hot-feeds", summary="AceCamp 热门 feed (综合)")
async def acecamp_hot_feeds(_: User = Depends(get_current_user)):
    async def fetch():
        data = await _acecamp_call("feeds", {"page_size": 20, "collection": "hot",
                                              "topping": "false"})
        if not isinstance(data, dict):
            return {"feeds": [], "real_spotlights": []}
        return {
            "feeds": data.get("feeds") or [],
            "real_spotlights": data.get("real_spotlights") or [],
        }
    return await _acecamp_cached("ace_hot_feeds", fetch)


@router.get("/acecamp/opinions-index", summary="AceCamp 观点广场 (最新)")
async def acecamp_opinions_index(_: User = Depends(get_current_user)):
    async def fetch():
        data = await _acecamp_call("opinions/index", {"page": 1, "per_page": 20})
        return {"list": data if isinstance(data, list) else []}
    return await _acecamp_cached("ace_opinions_idx", fetch)


@router.get("/acecamp/popular-corporations", summary="AceCamp 热门公司")
async def acecamp_popular_corporations(_: User = Depends(get_current_user)):
    async def fetch():
        data = await _acecamp_call("popular_corporations/populars",
                                    {"page": 1, "per_page": 20, "recent_updates": "true"})
        if not isinstance(data, dict):
            return {"corporations": [], "updated_at": None}
        return {
            "corporations": data.get("corporations") or [],
            "updated_at": data.get("updated_at"),
        }
    return await _acecamp_cached("ace_popular_corps", fetch)


@router.get("/acecamp/summary", summary="AceCamp 首页一次性聚合")
async def acecamp_summary(_: User = Depends(get_current_user)):
    """All-in-one homepage aggregate. Each sub-endpoint is cached separately,
    so repeated hits here are cheap."""
    try:
        counts, kws, feeds, opinions, corps = await asyncio.gather(
            acecamp_today_counts(_=_),
            acecamp_hot_keywords(_=_),
            acecamp_hot_feeds(_=_),
            acecamp_opinions_index(_=_),
            acecamp_popular_corporations(_=_),
            return_exceptions=True,
        )
    except Exception as e:
        raise HTTPException(502, f"acecamp summary fetch error: {e}")

    def _safe(x):
        return x if not isinstance(x, Exception) else {"error": str(x)}

    return {
        "counts":        _safe(counts),
        "hot_keywords":  _safe(kws),
        "hot_feeds":     _safe(feeds),
        "opinions":      _safe(opinions),
        "corporations":  _safe(corps),
        "generated_at":  datetime.now(timezone.utc).isoformat(),
    }


# ============================================================
# Gangtise 港推 主页快照 —— 从 MongoDB gangtise.homepage 直接读.
# Scraper: crawl/gangtise/scraper_home.py (每 10min 落库一次 8 个模块).
# ============================================================

GANGTISE_HOMEPAGE_COL = "homepage"

GANGTISE_MODULE_ORDER = [
    "hot_stocks",      # 机构热议个股
    "hot_concepts",    # A 股热门题材
    "hot_topics",      # 每日热点话题
    "hot_meetings",    # 机构热议纪要
    "research_sched",  # 近期研究行程
    "quick_entries",   # 快速入口
    "market_index",    # 大盘指数
    "banners",         # 运营 Banner
]


@_lru_cache(maxsize=1)
def _gangtise_mongo_client() -> AsyncIOMotorClient:
    return AsyncIOMotorClient(get_settings().gangtise_mongo_uri, tz_aware=True)


def _gangtise_db() -> AsyncIOMotorDatabase:
    return _gangtise_mongo_client()[get_settings().gangtise_mongo_db]


def _shape_gangtise(doc: Optional[dict]) -> Optional[dict]:
    if not doc:
        return None
    fetched_at = doc.get("fetched_at")
    if isinstance(fetched_at, datetime):
        if fetched_at.tzinfo is None:
            fetched_at = fetched_at.replace(tzinfo=timezone.utc)
        age_s = int((datetime.now(timezone.utc) - fetched_at).total_seconds())
        fetched_iso = fetched_at.isoformat()
    else:
        age_s = None
        fetched_iso = None
    return {
        "key": doc.get("_id"),
        "label": doc.get("label") or doc.get("_id"),
        "ok": bool(doc.get("ok")),
        "item_count": int(doc.get("item_count") or 0),
        "items": doc.get("items") or [],
        "status_msg": doc.get("status_msg"),
        "method": doc.get("method"),
        "path": doc.get("path"),
        "fetched_at": fetched_iso,
        "latency_ms": doc.get("latency_ms"),
        "age_seconds": age_s,
    }


@router.get("/gangtise", summary="Gangtise 港推 主页快照")
async def gangtise_snapshot(
    include_raw: bool = Query(False),
    _: User = Depends(get_current_user),
):
    """返回 gangtise.homepage 里全部模块, 按 GANGTISE_MODULE_ORDER 排序.
    scraper 每轮覆盖同一 _id=<key>, 所以这里拿到的就是最新快照."""
    db = _gangtise_db()
    projection = None if include_raw else {"raw": 0}
    docs: dict[str, Any] = {}
    async for d in db[GANGTISE_HOMEPAGE_COL].find({}, projection):
        docs[d["_id"]] = d

    ordered: list[dict] = []
    for key in GANGTISE_MODULE_ORDER:
        if key in docs:
            shaped = _shape_gangtise(docs[key])
            if shaped:
                ordered.append(shaped)
    for k, d in docs.items():
        if k in GANGTISE_MODULE_ORDER:
            continue
        shaped = _shape_gangtise(d)
        if shaped:
            ordered.append(shaped)

    total_items = sum(m["item_count"] for m in ordered)
    ok_count = sum(1 for m in ordered if m["ok"])
    oldest = newest = None
    for m in ordered:
        ts = m.get("fetched_at")
        if not ts:
            continue
        if oldest is None or ts < oldest:
            oldest = ts
        if newest is None or ts > newest:
            newest = ts
    return {
        "platform": "gangtise",
        "platform_label": "Gangtise 港推",
        "modules": ordered,
        "module_count": len(ordered),
        "ok_count": ok_count,
        "total_items": total_items,
        "oldest_fetched_at": oldest,
        "newest_fetched_at": newest,
    }


# ============================================================
# Gangtise 今日数据量对齐 — 平台实时 today vs MongoDB 入库
# 用的是和 scraper.py --today 相同的逻辑, 但跑在 backend 进程里用 httpx
# 异步并发, 读 credentials.json 里的 G_token.
# ============================================================

_GANGTISE_API = "https://open.gangtise.com"
_GANGTISE_CREDS = Path("/home/ygwang/trading_agent/crawl/gangtise/credentials.json")

# 平台"真正多出来的分页"的安全上限 —— 当 list 按 msgTime 倒序, 跨过 today_start
# 即停; 防止打穿的 hard ceiling.
_GANGTISE_MAX_PAGES_PER_CLASSIFY = 20
_GANGTISE_PAGE_SIZE = 200          # research / chief 单页最大可以 1000, 保守用 200
_GANGTISE_SUMMARY_PAGE_SIZE = 50   # summary 单类通常 <100/天
_GANGTISE_TIMEOUT_S = 25.0

# 对齐请求成本高 (~12-15 s cold), 以 300s TTL 缓存
_GANGTISE_DAILY_TTL = 300.0
_GANGTISE_DAILY_CACHE: dict[str, tuple[float, Any]] = {}
_GANGTISE_DAILY_LOCK = asyncio.Lock()

_SUMMARY_CLASSIFIES = [
    {"id": 17, "name": "帕米尔研究",   "param": {"sourceList": [100100262], "brokerList": ["C900000031"]}},
    {"id": 11, "name": "A股会议",      "param": {"sourceList": [100100178, 100100262], "columnIdList": [98]}},
    {"id": 12, "name": "港股会议",     "param": {"sourceList": [100100178, 100100262], "columnIdList": [99]}},
    {"id": 13, "name": "美股会议",     "param": {"sourceList": [100100178, 100100262], "columnIdList": [101]}},
    {"id": 14, "name": "专家会议",     "param": {"columnIdList": [104]}},
    {"id": 15, "name": "投关活动记录", "param": {"sourceList": [100100263]}},
    {"id": 16, "name": "网络资源",     "param": {"sourceList": [100100262]}},
]

_CHIEF_VARIANTS = [
    {"key": "domestic_institution",  "name": "内资机构观点",
     "path": "/application/glory/chief/v2/queryOpinionList",       "biz_params": None},
    {"key": "foreign_institution",   "name": "外资机构观点",
     "path": "/application/glory/chief/foreign/queryOpinionList",  "biz_params": {"foreignType": "researchSource"}},
    {"key": "foreign_independent",   "name": "外资独立观点",
     "path": "/application/glory/chief/foreign/queryOpinionList",  "biz_params": {"foreignType": "independent"}},
    {"key": "kol",                   "name": "大V观点",
     "path": "/application/glory/chief/foreign/queryOpinionList",  "biz_params": None},
]


def _gangtise_token() -> str:
    try:
        return (json.loads(_GANGTISE_CREDS.read_text()).get("token") or "").strip()
    except Exception:
        return ""


def _gangtise_headers(token: str) -> dict:
    return {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Content-Type": "application/json",
        "Referer": "https://open.gangtise.com/",
        "Origin":  "https://open.gangtise.com",
        "Authorization": f"bearer {token}",
        "access_token": token,
    }


_GANGTISE_OK_CODES = {"000000", 0, "10010000", 10010000, 200, "200"}


def _is_gangtise_ok(resp: Any) -> bool:
    if not isinstance(resp, dict):
        return False
    if resp.get("status") is True:
        return True
    return resp.get("code") in _GANGTISE_OK_CODES


def _items_of(resp: dict, kind: str) -> list[dict]:
    if not _is_gangtise_ok(resp):
        return []
    data = resp.get("data")
    if kind == "summary":
        return (data or {}).get("summList") or [] if isinstance(data, dict) else []
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return data.get("list") or data.get("records") or []
    return []


def _item_ts_ms(item: dict, kind: str) -> Optional[int]:
    if kind == "summary":
        ms = item.get("msgTime") or item.get("summTime")
    elif kind == "research":
        ms = item.get("pubTime")
    else:
        ms = item.get("msgTime")
    try:
        return int(ms) if ms else None
    except (TypeError, ValueError):
        return None


def _item_dedup_key(item: dict) -> str:
    return str(item.get("id") or item.get("msgId") or item.get("rptId") or id(item))


async def _gangtise_post(client: httpx.AsyncClient, path: str, body: dict) -> dict:
    try:
        r = await client.post(f"{_GANGTISE_API}{path}", json=body, timeout=_GANGTISE_TIMEOUT_S)
        if r.status_code in (401, 403):
            return {"code": r.status_code, "msg": "auth dead", "data": None}
        if r.status_code != 200:
            return {"code": r.status_code, "msg": f"HTTP {r.status_code}", "data": None}
        try:
            return r.json()
        except Exception:
            return {"code": -1, "msg": "non-json", "data": None}
    except Exception as e:  # noqa: BLE001
        return {"code": -1, "msg": f"{type(e).__name__}: {e}", "data": None}


async def _count_summary_platform(client: httpx.AsyncClient,
                                    start_ms: int, end_ms: int) -> dict:
    seen: set[str] = set()
    per_classify: list[dict] = []
    for cl in _SUMMARY_CLASSIFIES:
        page, cnt, scanned, pages = 1, 0, 0, 0
        while page <= _GANGTISE_MAX_PAGES_PER_CLASSIFY:
            body = {"pageNum": page, "pageSize": _GANGTISE_SUMMARY_PAGE_SIZE, **cl["param"]}
            resp = await _gangtise_post(client, "/application/summary/queryPage", body)
            items = _items_of(resp, "summary")
            pages += 1
            if not items:
                break
            scanned += len(items)
            stop = False
            for it in items:
                ts = _item_ts_ms(it, "summary")
                if ts is None:
                    continue
                if ts < start_ms:
                    stop = True
                    break
                if ts > end_ms:
                    continue
                key = _item_dedup_key(it)
                if key in seen:
                    continue
                seen.add(key)
                cnt += 1
            if stop or len(items) < _GANGTISE_SUMMARY_PAGE_SIZE:
                break
            page += 1
        per_classify.append({
            "key": cl["id"], "name": cl["name"],
            "platform_count": cnt, "scanned": scanned, "pages": pages,
        })
    return {"total": len(seen), "classifies": per_classify}


async def _count_research_platform(client: httpx.AsyncClient,
                                     start_ms: int, end_ms: int) -> dict:
    page, cnt, scanned, pages = 1, 0, 0, 0
    while page <= _GANGTISE_MAX_PAGES_PER_CLASSIFY:
        from_offset = (page - 1) * _GANGTISE_PAGE_SIZE
        body = {
            "from": from_offset, "size": _GANGTISE_PAGE_SIZE,
            "searchType": 1, "kw": "",
            "startDate": None, "endDate": None,
            "rptIds": [], "industryList": [], "columnList": [],
            "orgList": [], "orgTypeList": [], "honorTypeList": [],
            "authorList": [], "rateList": [], "changeList": [],
            "source": [0, 1], "exactStockList": [],
            "realTime": None, "curated": None, "typeList": [],
        }
        resp = await _gangtise_post(client, "/application/glory/research/v2/queryByCondition", body)
        items = _items_of(resp, "research")
        pages += 1
        if not items:
            break
        scanned += len(items)
        stop = False
        for it in items:
            ts = _item_ts_ms(it, "research")
            if ts is None:
                continue
            if ts < start_ms:
                stop = True
                break
            if ts > end_ms:
                continue
            cnt += 1
        if stop or len(items) < _GANGTISE_PAGE_SIZE:
            break
        page += 1
    return {"total": cnt, "scanned": scanned, "pages": pages, "classifies": []}


async def _count_chief_platform(client: httpx.AsyncClient,
                                  start_ms: int, end_ms: int) -> dict:
    seen: set[str] = set()
    per_variant: list[dict] = []
    for v in _CHIEF_VARIANTS:
        page, cnt, scanned, pages = 1, 0, 0, 0
        while page <= _GANGTISE_MAX_PAGES_PER_CLASSIFY:
            from_offset = (page - 1) * _GANGTISE_PAGE_SIZE
            condition: dict = {
                "keywords": {}, "matches": {},
                "from": from_offset, "size": _GANGTISE_PAGE_SIZE,
                "sort": {"msgTime": 1},
                "range": {"msgTime": {}},
                "filter": {"isOpn": 1},
            }
            if v.get("biz_params"):
                condition["bizParams"] = dict(v["biz_params"])
            resp = await _gangtise_post(client, v["path"], {"condition": condition})
            items = _items_of(resp, "chief")
            pages += 1
            if not items:
                break
            scanned += len(items)
            stop = False
            for it in items:
                ts = _item_ts_ms(it, "chief")
                if ts is None:
                    continue
                if ts < start_ms:
                    stop = True
                    break
                if ts > end_ms:
                    continue
                key = _item_dedup_key(it)
                if key in seen:
                    continue
                seen.add(key)
                cnt += 1
            if stop or len(items) < _GANGTISE_PAGE_SIZE:
                break
            page += 1
        per_variant.append({
            "key": v["key"], "name": v["name"],
            "platform_count": cnt, "scanned": scanned, "pages": pages,
        })
    return {"total": len(seen), "classifies": per_variant}


async def _count_db_today(start_ms: int, end_ms: int) -> dict:
    """MongoDB 上 release_time_ms 落在 today 的 doc 数.

    排除两类:
      1. `_orphan=True`  —— 已确认的幽灵条目
      2. `_orphan_candidate_count >= 1` —— 至少被扫过一轮"平台今日没有"
    这样新产生的 candidate 立即不计入. flag_orphans.py 在发现条目重回平台列表时
    会 `$unset _orphan* + $set candidate=0`, 让 false positive 自愈.
    """
    db = _gangtise_db()
    flt = {
        "release_time_ms": {"$gte": start_ms, "$lte": end_ms},
        "_orphan": {"$ne": True},
        "$or": [
            {"_orphan_candidate_count": {"$exists": False}},
            {"_orphan_candidate_count": {"$lt": 1}},
        ],
    }
    summary, research, chief = await asyncio.gather(
        db["summaries"].count_documents(flt),
        db["researches"].count_documents(flt),
        db["chief_opinions"].count_documents(flt),
    )
    return {"summary": summary, "research": research, "chief": chief}


def _cst_today_ms_range(date_str: Optional[str] = None) -> tuple[int, int, str]:
    """Return (start_ms, end_ms, yyyy-mm-dd) for CST day boundary."""
    cst = timezone(timedelta(hours=8))
    if date_str:
        day_start = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=cst)
    else:
        day_start = datetime.now(cst).replace(hour=0, minute=0, second=0, microsecond=0)
    day_end = day_start + timedelta(days=1) - timedelta(milliseconds=1)
    return (int(day_start.timestamp() * 1000),
            int(day_end.timestamp() * 1000),
            day_start.strftime("%Y-%m-%d"))


@router.get("/gangtise/daily-counts", summary="Gangtise 今日平台 vs 入库对齐")
async def gangtise_daily_counts(
    date: Optional[str] = Query(None, description="YYYY-MM-DD, 默认今日 CST"),
    refresh: bool = Query(False, description="绕过 5min 缓存, 强制重算"),
    _: User = Depends(get_current_user),
):
    """三类 (纪要/研报/观点) 今日平台 count vs MongoDB count, 带每分类明细.
    Cost: 冷调用 ~12-18s (分类串行, 3 类并行). 结果缓存 300s.

    诊断用途:
      - gap > 0 → 入库落后于平台 (watcher 漏抓 / token 问题 / 分类拿不全)
      - gap < 0 → DB 多出 (跨日时区漂移 / release_time_ms 误差)
    """
    start_ms, end_ms, target = _cst_today_ms_range(date)
    cache_key = f"gangtise_daily:{target}"
    now = time.time()
    if not refresh:
        hit = _GANGTISE_DAILY_CACHE.get(cache_key)
        if hit and now - hit[0] < _GANGTISE_DAILY_TTL:
            return hit[1]

    async with _GANGTISE_DAILY_LOCK:
        hit = _GANGTISE_DAILY_CACHE.get(cache_key)
        if not refresh and hit and time.time() - hit[0] < _GANGTISE_DAILY_TTL:
            return hit[1]

        token = _gangtise_token()
        if not token:
            raise HTTPException(503, "gangtise credentials missing")

        t0 = time.time()
        async with httpx.AsyncClient(
            headers=_gangtise_headers(token),
            trust_env=False,    # Clash 7890 会打断 gangtise TLS (infra_proxy memory)
            timeout=_GANGTISE_TIMEOUT_S,
        ) as client:
            summary_platform, research_platform, chief_platform, db_counts = (
                await asyncio.gather(
                    _count_summary_platform(client, start_ms, end_ms),
                    _count_research_platform(client, start_ms, end_ms),
                    _count_chief_platform(client, start_ms, end_ms),
                    _count_db_today(start_ms, end_ms),
                    return_exceptions=True,
                )
            )

        def _safe(v, default):
            return default if isinstance(v, Exception) else v

        summary_platform  = _safe(summary_platform,  {"total": 0, "classifies": []})
        research_platform = _safe(research_platform, {"total": 0, "classifies": []})
        chief_platform    = _safe(chief_platform,    {"total": 0, "classifies": []})
        db_counts         = _safe(db_counts,         {"summary": 0, "research": 0, "chief": 0})

        def _mk(kind: str, label: str, platform: dict) -> dict:
            p = int(platform.get("total") or 0)
            d = int(db_counts.get(kind) or 0)
            aligned_pct = (min(d, p) / p * 100.0) if p > 0 else (100.0 if d == 0 else 0.0)
            return {
                "kind": kind, "label": label,
                "platform_count": p, "db_count": d,
                "gap": p - d,
                "aligned_pct": round(aligned_pct, 1),
                "classifies": platform.get("classifies") or [],
            }

        result = {
            "date": target,
            "tz": "Asia/Shanghai",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "elapsed_s": round(time.time() - t0, 1),
            "types": [
                _mk("summary",  "纪要", summary_platform),
                _mk("research", "研报", research_platform),
                _mk("chief",    "观点", chief_platform),
            ],
        }
        _GANGTISE_DAILY_CACHE[cache_key] = (time.time(), result)
        return result


# 放在最后: 捕获 /gangtise/<任意> 的模块读取. 必须排在
# /gangtise/daily-counts 等具体子路由的后面, 否则会先被 module_key 吃掉.
@router.get("/gangtise/{module_key}", summary="Gangtise 单个模块")
async def gangtise_module(
    module_key: str,
    include_raw: bool = Query(False),
    _: User = Depends(get_current_user),
):
    db = _gangtise_db()
    projection = None if include_raw else {"raw": 0}
    doc = await db[GANGTISE_HOMEPAGE_COL].find_one({"_id": module_key}, projection)
    if not doc:
        raise HTTPException(404, f"module {module_key} not found")
    return _shape_gangtise(doc)
