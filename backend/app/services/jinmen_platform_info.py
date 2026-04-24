"""Fetch Jinmen (进门财经 brm.comein.cn) homepage widgets.

Exposes the handful of SPA data endpoints that populate 热搜 / 快速入口 / 机构
热议 / 活动日历 / 资讯 so we can mirror them into our own `平台信息` page.

All endpoints live under ``https://server.comein.cn/comein/json_<mod>_<act>``
and respond with **plain JSON** (unlike the `/api/v1` summary/research family
which AES-encrypts). One shared request signer that mirrors the scraper's
header scheme (`app`/`mod`/`act` + `uid`/`token` + identity headers).

## Anti-detection posture

Upstream is sensitive to bursty behaviour (see `feedback_crawler_antidetection`).
We therefore:

* **Cache aggressively** — every endpoint has a 15-min Redis TTL. Frontend
  traffic doesn't hit Jinmen at all; one background refresh per 15 min max.
* **Serve stale on upstream error** — if the live call fails, return the last
  good cache even if expired. Never throw the user into a loop that retries
  the upstream on every click.
* **Jitter refreshes** — the lock-and-refresh path sleeps 1-3s before the
  network call so concurrent first-fetches don't stampede.
* **One auth load per process** — scraper credentials read once + cached.

Entry point: ``get_platform_info(key)`` where ``key`` is one of the
``WIDGETS`` dict keys. Returns ``{ok, data, fetched_at, stale}``.
"""
from __future__ import annotations

import asyncio
import base64
import json as _json
import logging
import random
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx

logger = logging.getLogger(__name__)

_REPO_ROOT = Path(__file__).resolve().parents[3]
_API_BASE = "https://server.comein.cn/comein"

# Endpoint -> (mod, act, default POST body builder)
# Body builders accept `**kwargs` so route handlers can pass through page/time
# params where relevant (e.g. news-articles pagination).
WIDGETS: dict[str, dict[str, Any]] = {
    "hot_search": {
        # 热搜 top-10 (混合类型;服务端按平台整体热度排;isUp=1/-1 是涨跌箭头)
        "endpoint": "json_search_hotbytype",
        "body_fn": lambda **_: {"location": 1, "searchContent": ""},
    },
    "search_recommend": {
        # 快速入口 (搜索推荐: 5 条平台精选,通常是当下热门机构/个股名)
        "endpoint": "json_searchrecommend_firstlist",
        "body_fn": lambda **_: {"location": 1, "searchContent": ""},
    },
    "news_articles": {
        # 资讯 (公众号聚合 news feed; list 模式, 30 条/页)
        "endpoint": "json_news_article-list",
        "body_fn": lambda page=1, size=30, **_: {
            "pageIndex": int(page), "pageSize": int(size),
            "accountIdList": [], "industryIdList": [],
            "isSelect": 1, "keywords": "",
        },
    },
    "news_accounts": {
        # 机构热议 = 推荐公众号 (研报/调研/评论类账号)
        "endpoint": "json_news_official-account-list-recommendation",
        "body_fn": lambda **_: {},
    },
    "meeting_calendar": {
        # 活动日历 (按 type=2 研讨会/会议 — 可按 day 汇总数量)
        "endpoint": "json_meeting-activity_count-by-day",
        "body_fn": lambda start_ms=None, end_ms=None, **_: {
            "startTime": int(start_ms) if start_ms
                         else int((time.time() - 86400) * 1000),
            "endTime":   int(end_ms)   if end_ms
                         else int((time.time() + 86400 * 2) * 1000),
            "type": 2,
        },
    },
    "industries": {
        # 一级行业 (总量/消费/科技/制造/金融/健康/周期/海外/... 用于筛选栏)
        "endpoint": "json_get_first-industry",
        "body_fn": lambda **_: {},
    },
    # ---------------- Content feeds (via the master search-by-type index) ----
    # Shared body template; only `type` differs. sortType=2/orderType=2 ⇒
    # release-time desc. Returns {rows: [...], total: N}. Field schemas vary
    # between types — see frontend types for the per-type renderers.
    "latest_roadshow": {
        # type=4 — 最新路演 (hasAISummary/hasVideo/industries/themes)
        "endpoint": "json_search_search-by-type",
        "body_fn": lambda size=10, **_: _sbt_body(4, size),
    },
    "latest_report": {
        # type=2 — 最新研报 (org+analysts+companies+pdf)
        "endpoint": "json_search_search-by-type",
        "body_fn": lambda size=10, **_: _sbt_body(2, size),
    },
    "latest_summary": {
        # type=13 — 最新纪要 (featuredTag "首席分析师"等, industries, themes, stocks)
        "endpoint": "json_search_search-by-type",
        "body_fn": lambda size=10, **_: _sbt_body(13, size),
    },
    "latest_comment": {
        # type=11 — 最新点评 (stockList + industryList + organizationList)
        "endpoint": "json_search_search-by-type",
        "body_fn": lambda size=10, **_: _sbt_body(11, size),
    },
}


def _sbt_body(type_id: int, size: int = 10) -> dict:
    """json_search_search-by-type body template — release-time desc, no filter."""
    return {
        "page": 1, "size": int(size), "type": int(type_id),
        "sortType": 2, "orderType": 2, "input": "",
        "options": {
            "needParticiple": True, "allowInputEmpty": True, "searchScope": 0,
            "fullCodes": "", "industryTagIds": "", "roadshowTypeIds": "",
            "topicIdsArr": "", "startTime": "", "endTime": "",
            "featuredTag": "", "authTag": "", "speakerTag": "", "contentTypeTag": "",
        },
    }

# ------------------------------------------------------------------ #
# Auth — same scheme as scraper.create_session; reads JM_AUTH_INFO blob.
# ------------------------------------------------------------------ #
_JM_AUTH: dict[str, str] | None = None


def _load_jm_auth() -> dict[str, str] | None:
    global _JM_AUTH
    if _JM_AUTH is not None:
        return _JM_AUTH or None
    blob = ""
    creds = _REPO_ROOT / "crawl" / "jinmen" / "credentials.json"
    if creds.exists():
        try:
            d = _json.loads(creds.read_text(encoding="utf-8"))
            blob = d.get("token") or d.get("JM_AUTH_INFO") or ""
        except Exception as exc:
            logger.warning("jinmen credentials.json parse: %s", exc)
    if not blob:
        sp = _REPO_ROOT / "crawl" / "jinmen" / "scraper.py"
        if sp.exists():
            try:
                m = re.search(r'JM_AUTH_INFO\s*=\s*"([^"]+)"',
                              sp.read_text(encoding="utf-8"))
                if m:
                    blob = m.group(1)
            except Exception as exc:
                logger.warning("jinmen scraper.py read: %s", exc)
    if not blob:
        _JM_AUTH = {}
        return None
    try:
        inner = (_json.loads(base64.b64decode(blob).decode("utf-8")) or {}).get("value") or {}
    except Exception as exc:
        logger.warning("jinmen auth decode: %s", exc)
        _JM_AUTH = {}
        return None
    uid = str(inner.get("uid") or "")
    tok = str(inner.get("webtoken") or inner.get("token") or "")
    if not (uid and tok):
        _JM_AUTH = {}
        return None
    _JM_AUTH = {
        "uid": uid,
        "token": tok,
        "realm": str(inner.get("organizationId") or ""),
    }
    return _JM_AUTH


def _headers_for(endpoint: str) -> dict[str, str]:
    """Mirror ``crawl/jinmen/scraper.py::create_session + headers_for``."""
    parts = endpoint.split("_", 2)
    app_mod_act = {"app": parts[0], "mod": parts[1], "act": parts[2]} if len(parts) == 3 else {}
    auth = _load_jm_auth() or {}
    return {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Content-Type": "application/json",
        "Referer": "https://brm.comein.cn/",
        "Origin": "https://brm.comein.cn",
        "uid": auth.get("uid", ""),
        "token": auth.get("token", ""),
        "web_token": auth.get("token", ""),
        "realm": auth.get("realm", ""),
        "os": "brm",
        "c": "pc",
        "b": "4.2.0800",
        "brandChannel": "windows",
        "webenv": "comein",
        "language": "zh-CN",
        "s": "",
        "uc": "comein-p",
        **app_mod_act,
    }


# ------------------------------------------------------------------ #
# Cache: in-memory fallback if Redis not wired. Redis is best-effort.
# ------------------------------------------------------------------ #
_MEM_CACHE: dict[str, tuple[float, Any]] = {}  # key → (expires_at, payload)
_CACHE_TTL_S = 15 * 60
_CACHE_STALE_GRACE_S = 6 * 3600      # serve stale up to 6h if upstream down
_LOCK = asyncio.Lock()                # one-flight per process


async def _call_upstream(endpoint: str, body: dict) -> dict:
    """POST the body to server.comein.cn. Returns the parsed JSON."""
    url = f"{_API_BASE}/{endpoint}"
    hdrs = _headers_for(endpoint)
    # 1-3s jitter — prevents burst reads across multiple widgets
    await asyncio.sleep(random.uniform(1.0, 3.0))
    async with httpx.AsyncClient(timeout=20.0, trust_env=False) as cli:
        r = await cli.post(url, json=body, headers=hdrs)
    r.raise_for_status()
    return r.json()


async def get_platform_info(key: str, **params) -> dict:
    """Fetch one widget's data, via cache.

    Returns ``{ok, data, code, msg, fetched_at, stale}``:
      * ``ok`` — True if we have *any* payload (fresh or stale) to show
      * ``data`` — the upstream ``data`` field unchanged
      * ``stale`` — True when serving beyond TTL
    """
    if key not in WIDGETS:
        return {"ok": False, "data": None, "code": "bad_key",
                "msg": f"unknown widget: {key}", "fetched_at": None, "stale": False}
    cfg = WIDGETS[key]
    body = cfg["body_fn"](**params)
    cache_key = f"{key}:{_stable_hash(body)}"

    now = time.time()
    cached = _MEM_CACHE.get(cache_key)
    if cached and cached[0] > now:
        # Fresh hit — return immediately, no upstream call
        return {**cached[1], "stale": False}

    async with _LOCK:
        # Double-check after acquiring lock
        cached = _MEM_CACHE.get(cache_key)
        if cached and cached[0] > now:
            return {**cached[1], "stale": False}

        try:
            resp = await _call_upstream(cfg["endpoint"], body)
            # Normalize payload: most endpoints wrap in `data`, but
            # `json_search_search-by-type` puts the list in `rows` at top
            # level. Expose a uniform dict shape to the frontend.
            data = resp.get("data")
            if data is None and isinstance(resp.get("rows"), list):
                data = {"list": resp["rows"], "total": resp.get("total", 0)}
            payload = {
                "ok": str(resp.get("code")) in ("0", "200"),
                "data": data,
                "code": resp.get("code"),
                "msg": resp.get("msg") or resp.get("errordesc") or "",
                "fetched_at": datetime.now(timezone.utc).isoformat(),
            }
            _MEM_CACHE[cache_key] = (now + _CACHE_TTL_S, payload)
            return {**payload, "stale": False}
        except Exception as exc:
            logger.warning("jinmen platform-info upstream %s failed: %s", key, exc)
            # Serve stale within grace window, otherwise error
            if cached and cached[0] > now - _CACHE_STALE_GRACE_S:
                return {**cached[1], "stale": True, "msg": f"upstream: {exc}"}
            return {
                "ok": False, "data": None,
                "code": "upstream_error", "msg": str(exc)[:200],
                "fetched_at": datetime.now(timezone.utc).isoformat(),
                "stale": False,
            }


def _stable_hash(obj: Any) -> str:
    return _json.dumps(obj, sort_keys=True, ensure_ascii=False)
