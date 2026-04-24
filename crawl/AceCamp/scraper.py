#!/usr/bin/env python3
"""
api.acecamptech.com 爬虫 (MongoDB 存储)

抓取内容类型 (2026-04-23 起, 路演 events 被移除):
  articles  文章 / 纪要 / 调研  (按 subtype 三分, 爬虫按 list_item.type + title 分流)
  opinions  观点 (用户短评, 带 expected_trend)

使用方法:
  1. 浏览器登录 www.acecamptech.com
  2. F12 → Network → 任一 api.acecamptech.com/api/v1/... 请求 → 复制 Cookie 完整字符串
  3. 粘贴到 credentials.json {"cookie": "..."} 或通过 --auth / env ACECAMP_AUTH
  4. 启动 MongoDB (默认 mongodb://localhost:27017)
  5. 运行:
       python3 scraper.py --show-state           # checkpoint + cookie 健康
       python3 scraper.py --max 10               # 各类各爬 10 条小试
       python3 scraper.py                        # 全量
       python3 scraper.py --watch --resume --interval 600
       python3 scraper.py --today
       python3 scraper.py --type articles --max 500

MongoDB 数据模型 (database=acecamp):
  articles       — 文章 + 纪要 + 调研, _id = a<article id>  (int), subtype ∈ {minutes, research, article}
  opinions       — 用户观点, _id = o<opinion id>  (int)
  account        — 账户 / 元信息
  _state         — checkpoint + daily stats

与 gangtise / jinmen 同构. 参考 crawl/README.md §3-§4.
"""
from __future__ import annotations

import argparse
import hashlib
import html
import json
import os
import re
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Optional

import requests
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from tqdm import tqdm

# 共享反爬模块 (crawl/antibot.py)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from antibot import (  # noqa: E402
    AdaptiveThrottle, DailyCap, SessionDead,
    parse_retry_after, is_auth_dead,
    add_antibot_args, throttle_from_args, cap_from_args,
    AccountBudget, SoftCooldown, detect_soft_warning,
    headers_for_platform, log_config_stamp, budget_from_args,
)
from ticker_tag import stamp as _stamp_ticker  # noqa: E402


def _upsert_preserve_crawled_at(col, did: str, doc: dict) -> None:
    """Upsert that preserves the *first* crawled_at — critical for the dashboard's
    "今日新增" metric: without this, re-running the scraper with --force or a
    content re-fetch will rewrite every doc's crawled_at to "now", making the
    entire collection look like it was crawled today.

    Uses $set for everything + $setOnInsert for crawled_at. The caller still
    passes crawled_at in `doc` so the INSERT path has it; we lift it into
    $setOnInsert and drop it from $set.
    """
    first_crawled = doc.pop("crawled_at", None)
    set_only = {k: v for k, v in doc.items() if k != "_id"}
    update: dict = {"$set": set_only}
    if first_crawled is not None:
        update["$setOnInsert"] = {"crawled_at": first_crawled}
    col.update_one({"_id": did}, update, upsert=True)

# 模块级 throttle — main() 会用 CLI 覆盖
# 默认值 2026-04-24 从 (3.0, 2.0, 40) 收紧到 (4.0, 2.5, 20) — AceCamp 账号封控
# 事故后的新基线, 跟 ANTIBOT.md backfill v1 表 (3.5, 2.0, 30) 再保守一档.
# AceCamp API 的 detail 端点有独立 quota (10003/10040), 模块级 fallback 用在
# 任何没走 throttle_from_args 的路径; 默认 4s 间隔 + 20 burst 能保证即使脚本
# 以老方式启动也不会一分钟烧掉当日全部 quota.
_THROTTLE: AdaptiveThrottle = AdaptiveThrottle(base_delay=4.0, jitter=2.5,
                                                burst_size=20,
                                                platform="acecamp")
_BUDGET: AccountBudget = AccountBudget("acecamp", "default", 0)
_PLATFORM = "acecamp"

# ==================== 请配置以下内容 ====================
# 浏览器 F12 → Network → api.acecamptech.com 请求 → Request Headers → Cookie 整串
# 包含: user_token (JWT, 3 个月以上), _ace_camp_tech_production_session (Rails),
#       aceid, HMACCOUNT, Hm_lvt_*, Hm_lpvt_*
# Cookie 过期表现: HTTP 401 / 响应 ret=false & code 401
ACECAMP_COOKIE = ""  # 默认空, 由 credentials.json / env / CLI 提供

# ==================== 以下无需修改 ====================

CREDS_FILE = Path(__file__).resolve().parent / "credentials.json"


def _load_cookie_from_file() -> str:
    """credentials.json 里的 cookie 优先, 允许飞书机器人热更新."""
    if not CREDS_FILE.exists():
        return ""
    try:
        d = json.loads(CREDS_FILE.read_text(encoding="utf-8"))
        # 兼容 token / cookie 两种键名
        return (d.get("cookie") or d.get("token") or "").strip()
    except Exception:
        return ""


API_BASE = "https://api.acecamptech.com/api/v1"
WEB_BASE = "https://www.acecamptech.com"

# MongoDB 配置
MONGO_URI_DEFAULT = os.environ.get(
    "MONGO_URI",
    "mongodb://u_spider:prod_X5BKVbAc@192.168.31.176:35002/?authSource=admin",
)
MONGO_DB_DEFAULT = os.environ.get("MONGO_DB", "acecamp")
COL_ACCOUNT = "account"
COL_STATE = "_state"


# ==================== 网络 / 会话 ====================

def create_session(cookie: str) -> requests.Session:
    """构造带完整 AceCamp 头的 Session. 禁用环境代理 (CN CDN + Clash SSL-EOF)."""
    s = requests.Session()
    s.trust_env = False  # 忽略 $HTTP(S)_PROXY, Clash 会中断 CN CDN TLS
    h = headers_for_platform("acecamp")
    h.update({
        "Referer": f"{WEB_BASE}/",
        "Origin": WEB_BASE,
        "Cookie": cookie,
        "X-Requested-With": "XMLHttpRequest",
    })
    s.headers.update(h)
    return s


def api_call(session: requests.Session, method: str, path: str,
             json_body: Optional[dict] = None,
             params: Optional[dict] = None,
             retries: int = 2, timeout: int = 30,
             expected_json: bool = True) -> dict:
    """统一请求: 统一错误返回, 统一节流退避.

    - 401/403 → SessionDead (Cookie 失效, 调用方需退出提示重登)
    - 429/5xx → 指数退避 + 尊重 Retry-After
    - 2xx 非 JSON → 原样返回 {"_raw": text, "_status": code}
    - 业务层 ret=false 在调用方通过 _is_ok 判定
    """
    url = f"{API_BASE}{path if path.startswith('/') else '/' + path}"
    last_exc = None
    for attempt in range(1, retries + 2):
        try:
            r = session.request(method, url, json=json_body, params=params, timeout=timeout)
            if is_auth_dead(r.status_code):
                raise SessionDead(f"HTTP {r.status_code} on {path}: {r.text[:200]}")
            if r.status_code == 429 or 500 <= r.status_code < 600:
                if r.status_code == 429:
                    SoftCooldown.trigger(_PLATFORM, reason=f"http_429:{path}",
                                          minutes=45)
                ra = parse_retry_after(r.headers.get("Retry-After"))
                _THROTTLE.on_retry(retry_after_sec=ra, attempt=attempt)
                _THROTTLE.sleep_before_next()
                last_exc = f"HTTP {r.status_code}"
                continue
            if r.status_code != 200:
                return {"code": r.status_code, "msg": f"HTTP {r.status_code}",
                        "data": None, "ret": False}
            if not expected_json:
                return {"_raw": r.text, "_status": r.status_code,
                        "_bytes": r.content, "_headers": dict(r.headers)}
            body = r.json()
            # 业务层判 401 / 1001 等鉴权码
            if isinstance(body, dict) and body.get("ret") is False:
                code = body.get("code")
                msg = str(body.get("msg") or "")
                if code in (401, 403, 1001) or "登录" in msg or "login" in msg.lower():
                    raise SessionDead(f"业务鉴权失败 code={code} msg={msg}")
                # AceCamp VIP 团队金卡 quota 耗尽 — detail 端点 balance:0 典型信号.
                # 单平台共享 30min 静默: 其它 watcher / backfill 都收到同一 Redis flag
                # 直接暂停, 避免一起硬打 detail 把 quota 封停时间越拖越长.
                # 10003 / 10040 都是 balance/quota 错误码 (见 memory crawler_acecamp_quota).
                if code in (10003, 10040):
                    SoftCooldown.trigger(_PLATFORM,
                                          reason=f"acecamp_quota:code_{code}",
                                          minutes=30)
                    _THROTTLE.on_warning()
            # 软警告 (限流/quota/captcha cookie)
            reason = detect_soft_warning(r.status_code, body=body if isinstance(body, dict) else None,
                                          text_preview=r.text[:400] if r.text else "",
                                          cookies=dict(r.cookies))
            if reason:
                mins = 30 if "quota" in reason or "code_7" in reason else 60
                SoftCooldown.trigger(_PLATFORM, reason=reason, minutes=mins)
                _THROTTLE.on_warning()
            return body
        except SessionDead:
            raise
        except (requests.RequestException, ValueError) as e:
            last_exc = e
            if attempt < retries + 1:
                _THROTTLE.on_retry(attempt=attempt)
                _THROTTLE.sleep_before_next()
    return {"code": -1, "msg": f"req_err: {last_exc}", "data": None, "ret": False}


def _is_ok(resp: dict) -> bool:
    """业务成功判定 — AceCamp 统一 ret=True + code=200."""
    if not isinstance(resp, dict):
        return False
    if resp.get("ret") is True:
        return True
    return resp.get("code") == 200 and resp.get("data") is not None


# ==================== 数据类型 / 配置 ====================
#
# 每种内容类型 (articles / opinions) 都有同一套生命周期:
#   list(page)    → 列表项 (含核心字段)
#   detail(id)    → 详情 (articles 有大量正文 + transcribe)
#   key(item)     → 稳定主键
#
# TYPE_ORDER 决定跑的顺序. articles 优先 (信息量最大).
# 2026-04-23: events/路演 类型已从 UI 移除 — scraper 也不再抓, MongoDB events
# collection 已 drop. 要恢复: 从 git log 取旧版 fetch_events_list/dump_event.
# ----------------------------------------------------------

TYPE_ORDER = ["articles", "opinions"]
PAGE_SIZE_DEFAULT = 30

# 按标题判定 "调研/访谈" 属于独立的 research 子类 (平台 type=minute 下混着
# 业绩会纪要/专家调研/专家访谈;UI 把带"调研"字样的拆出来单独展示).
_RESEARCH_TITLE_RE = re.compile(
    r"调研|访谈|专家会议|专家[^（(]{0,12}会|专家交流|field\s*trip",
    re.IGNORECASE,
)


def _article_subtype(item_type: str, title: str) -> str:
    """articles 按平台 list_item.type + title 分三类:
    - type=original                 → "article"    (原创文章 / 研报 / 白皮书)
    - type=minute + 标题含调研/访谈  → "research"   (产业/专家调研纪要)
    - type=minute  其他             → "minutes"    (业绩会 / 公司纪要)
    - 其他                          → "minutes"    (兜底)
    """
    t = (item_type or "").strip().lower()
    if t == "original":
        return "article"
    if t == "minute" and title and _RESEARCH_TITLE_RE.search(title):
        return "research"
    return "minutes"


# ==================== 工具函数 ====================

def _hash_id(*parts: Any) -> str:
    raw = "|".join("" if p is None else str(p) for p in parts)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _sec_to_str(sec: Any) -> str:
    """AceCamp release_time 是秒级 unix timestamp → 'YYYY-MM-DD HH:MM'."""
    if not sec:
        return ""
    try:
        ts = int(sec)
        if ts == 0:
            return ""
        # AceCamp 用秒, 不是毫秒. 平台时间是 Asia/Shanghai 壁钟时间.
        return datetime.fromtimestamp(
            ts, tz=timezone(timedelta(hours=8)),
        ).strftime("%Y-%m-%d %H:%M")
    except (TypeError, ValueError, OSError):
        return ""


def _strip_html(s: str) -> str:
    """去掉 HTML 残留, 但保留换行."""
    if not s:
        return ""
    s = re.sub(r"<br\s*/?>", "\n", s, flags=re.I)
    s = re.sub(r"</p\s*>", "\n", s, flags=re.I)
    s = re.sub(r"<[^>]+>", "", s)
    s = html.unescape(s)
    return s.strip()


def _item_corporations(item: dict) -> list[dict]:
    """AceCamp 的 corporations: [{id, name, code?, ...}]. 归一化供 ticker enrich."""
    raw = item.get("corporations") or []
    if not isinstance(raw, list):
        return []
    out = []
    for c in raw:
        if not isinstance(c, dict):
            continue
        out.append({
            "id": c.get("id"),
            "name": c.get("name") or c.get("short_name") or "",
            "code": c.get("code") or c.get("stock_code") or "",
            "exchange": c.get("exchange") or "",
        })
    return out


def _item_hashtags(item: dict) -> list[str]:
    """AceCamp hashtags: [{id, name}] or list[str]."""
    raw = item.get("hashtags") or []
    if not isinstance(raw, list):
        return []
    out = []
    for h in raw:
        if isinstance(h, dict):
            n = h.get("name") or h.get("title")
            if n:
                out.append(n)
        elif isinstance(h, str):
            out.append(h)
    return out


# ==================== 列表抓取 ====================

def fetch_articles_list(session, page: int, size: int) -> dict:
    """观点 / 纪要列表. GET /articles/article_list?page=N&per_page=M."""
    return api_call(session, "GET", "/articles/article_list",
                    params={"page": page, "per_page": size})


def fetch_opinions_list(session, page: int, size: int) -> dict:
    """观点列表. GET /opinions/opinion_list?page=N&per_page=M.
    观点 = 用户短评 / 股票行业观点, 字段含 expected_trend (bullish/bearish) 等.
    """
    return api_call(session, "GET", "/opinions/opinion_list",
                    params={"page": page, "per_page": size})


# ==================== 详情抓取 ====================

# Detail 端点连续失败/空正文的上限. 账号被封控时 detail 会稳定返回
# code=10003/10040 (quota 耗尽) 或正文为空, 但 list 端点仍然能返摘要,
# 如果不早停, scraper 就会一直 upsert 新 doc, content_md 全是空壳 —
# dashboard 看着像"今日入库很正常", 实际全是垃圾数据.
# 连续 N 次失败/空正文 → 抛 SessionDead 让调用方退出.
_DETAIL_FAIL_STREAK = {"article": 0, "opinion": 0}
_DETAIL_FAIL_THRESHOLD = 15
# code=10003/10040 是 AceCamp 团队金卡 quota 耗尽 (balance:0) 的典型 code,
# 其余业务 code (如 10004 not found) 不算, 正常跳过单条即可.
_QUOTA_CODES = {10003, 10040}


def _tripwire_record_detail(kind: str, *, resp: dict | None, content_len: int) -> None:
    """记录一次 detail 调用的成败. 累积到阈值就抛 SessionDead.

    kind: "article" | "opinion".
    成功 (content_len ≥ 50 的真正文) 会重置 streak; 失败 +1.
    """
    failed = False
    reason = ""
    if resp is not None:
        err = resp.get("_err") if isinstance(resp, dict) else None
        code = None
        if isinstance(err, dict):
            code = err.get("code")
        if code in _QUOTA_CODES:
            failed = True
            reason = f"quota code={code}"
    # 正文太短 (≤ 50 字符, 实质仅标题/tiny preview) 视为失败
    if not failed and content_len < 50:
        failed = True
        reason = f"content_md len={content_len} (too thin)"

    if failed:
        _DETAIL_FAIL_STREAK[kind] += 1
        if _DETAIL_FAIL_STREAK[kind] >= _DETAIL_FAIL_THRESHOLD:
            raise SessionDead(
                f"detail 端点连续 {_DETAIL_FAIL_THRESHOLD} 次失败 "
                f"({kind}, 最后原因: {reason}) — 账号 detail 权限疑似被封控, "
                f"继续抓只会灌空壳数据. 请到 /data-sources 查看状态 / 重登."
            )
    else:
        _DETAIL_FAIL_STREAK[kind] = 0


def fetch_article_detail(session, article_id) -> dict:
    """GET /articles/article_info?id=X → 完整详情, 含 content / transcribe / summary."""
    if not article_id:
        return {}
    resp = api_call(session, "GET", "/articles/article_info",
                    params={"id": article_id})
    if not _is_ok(resp):
        return {"_err": {"code": resp.get("code"), "msg": resp.get("msg")}}
    data = resp.get("data") or {}
    if isinstance(data, list) and data:
        return data[0]
    return data if isinstance(data, dict) else {}



def fetch_opinion_detail(session, opinion_id) -> dict:
    """GET /opinions/opinion_info?id=X → 观点详情."""
    if not opinion_id:
        return {}
    resp = api_call(session, "GET", "/opinions/opinion_info",
                    params={"id": opinion_id})
    if not _is_ok(resp):
        return {"_err": {"code": resp.get("code"), "msg": resp.get("msg")}}
    data = resp.get("data") or {}
    if isinstance(data, list) and data:
        return data[0]
    return data if isinstance(data, dict) else {}


def fetch_article_download_url(session, article_id) -> Optional[str]:
    """GET /articles/download_url?id=X → 返回 PDF URL (多数文章为 null)."""
    if not article_id:
        return None
    resp = api_call(session, "GET", "/articles/download_url",
                    params={"id": article_id})
    if not _is_ok(resp):
        return None
    data = resp.get("data") or {}
    return data.get("download_url") if isinstance(data, dict) else None


# ==================== 主键 + 文档组装 ====================

def dedup_id_article(item: dict) -> str:
    rid = item.get("id")
    if rid:
        return f"a{rid}"
    return "a" + _hash_id(item.get("title"), item.get("release_time"))


def dedup_id_opinion(item: dict) -> str:
    rid = item.get("id")
    if rid:
        return f"o{rid}"
    return "o" + _hash_id(item.get("title"), item.get("release_time"))


def _pick_article_content(detail: dict, list_item: dict) -> tuple[str, str, str]:
    """从详情提取正文. 优先: content > transcribe > summary.

    返回 (content_md, transcribe_md, summary_md). transcribe 单独保留供 AI / debug.
    """
    content = _strip_html(str(detail.get("content") or "").strip())
    transcribe = _strip_html(str(detail.get("transcribe") or "").strip())
    summary = _strip_html(str(detail.get("summary") or list_item.get("summary") or "").strip())

    # article_speech 是语音转写, 作为 content 兜底
    if not content:
        speech = detail.get("article_speech")
        if isinstance(speech, dict):
            text = speech.get("text") or speech.get("content") or ""
            content = _strip_html(text)
        elif isinstance(speech, str):
            content = _strip_html(speech)

    return content, transcribe, summary


def dump_article(session, db, item: dict, force: bool = False,
                 skip_detail: bool = False) -> tuple[str, dict]:
    """文章入库. collection=articles, _id=a<id>."""
    col = db["articles"]
    did = dedup_id_article(item)

    if not force:
        ex = col.find_one({"_id": did}, {"_id": 1, "content_md": 1, "stats": 1})
        if ex and (ex.get("content_md") or ""):
            return "skipped", ex.get("stats") or {"content_chars": len(ex.get("content_md") or "")}

    # 拉详情 (含正文). skip_detail=True 时只存列表元数据.
    detail = {} if skip_detail else fetch_article_detail(session, item.get("id"))

    content_md, transcribe_md, summary_md = _pick_article_content(detail, item)
    # list_item 自身有 summary 字段; 详情拉不到时兜底
    if not summary_md:
        summary_md = _strip_html(str(item.get("summary") or "").strip())

    release_sec = item.get("release_time") or detail.get("release_time") or 0
    release_time = _sec_to_str(release_sec)

    organization = item.get("organization") or detail.get("organization") or {}
    org_name = ""
    org_id = None
    if isinstance(organization, dict):
        org_name = organization.get("name") or organization.get("share_display_name") or ""
        org_id = organization.get("id")

    # 细分 subtype (按平台 type 字段 + title 关键字):
    #   type=original                   → "article"  原创研报 / 白皮书
    #   type=minute + 标题含调研/访谈    → "research" 专家调研纪要
    #   type=minute  其他               → "minutes"  业绩会 / 公司纪要
    item_type = (item.get("type") or detail.get("type") or "").strip().lower()
    _title_for_subtype = (item.get("title") or detail.get("title") or "")
    subtype = _article_subtype(item_type, _title_for_subtype)

    # 下载链接: /articles/download_url 端点要 premium 权限, 且多数返 null,
    # 每条调一次既拖慢又会 401 挂掉整个 run.
    # 直接信任 list 里的 can_download 标记, 真要下载时前端跳 web_url 即可.
    download_url = None

    # brief 取 summary 前 500 字 (AceCamp 没有独立 brief 字段)
    brief_md = (summary_md or content_md)[:500]

    doc = {
        "_id": did,
        "category": "article",
        "subtype": subtype,  # minutes | research | article
        "raw_id": item.get("id"),
        "title": item.get("title") or detail.get("title") or "",
        "original_title": item.get("original_title") or detail.get("original_title") or "",
        "release_time": release_time,
        "release_time_ms": int(release_sec) * 1000 if release_sec else None,
        "release_time_sec": int(release_sec) if release_sec else None,
        "organization": org_name,
        "organization_id": org_id,
        "organization_raw": organization,
        "corporations": _item_corporations({**item, **detail}),
        "corporation_ids": item.get("corporation_ids") or detail.get("corporation_ids") or [],
        "hashtags": _item_hashtags({**item, **detail}),
        "industry_ids": item.get("industry_ids") or detail.get("industry_ids") or [],
        "custom_sector_ids": item.get("custom_sector_ids") or detail.get("custom_sector_ids") or [],
        "event_ids": item.get("event_ids") or detail.get("event_ids") or [],
        "meeting_ids": item.get("meeting_ids") or detail.get("meeting_ids") or [],
        "events": item.get("events") or detail.get("events") or [],
        "expert_public_resume": item.get("expert_public_resume") or detail.get("expert_public_resume"),
        "source_url": item.get("source_url") or detail.get("source_url"),
        "views": item.get("views") or detail.get("views") or 0,
        "likes": item.get("likes") or detail.get("likes") or 0,
        "favorites": item.get("favorites") or detail.get("favorites") or 0,
        "comment_count": item.get("comment_count") or detail.get("comment_count") or 0,
        "has_vip": bool(item.get("has_vip")),
        "free": bool(item.get("free")),
        "need_to_pay": bool(item.get("need_to_pay")),
        "has_paid": bool(item.get("has_paid")),
        "can_download": bool(item.get("can_download")),
        "brief_md": brief_md,
        "summary_md": summary_md,
        "content_md": content_md or summary_md,  # 有 content 用 content, 否则 summary 兜底
        "transcribe_md": transcribe_md,
        "download_url": download_url,
        "list_item": item,
        "detail_result": detail if detail and not detail.get("_err") else None,
        "web_url": f"{WEB_BASE}/article/detail?id={item.get('id')}",
        "stats": {
            "content_chars": len(content_md),
            "transcribe_chars": len(transcribe_md),
            "summary_chars": len(summary_md),
        },
        "crawled_at": datetime.now(timezone.utc),
    }
    _stamp_ticker(doc, "acecamp", col)
    _upsert_preserve_crawled_at(col, did, doc)
    # tripwire: 只在实际拉了 detail 的路径上计数 (skip_detail 模式是主动选的,
    # 不应该触发 abort). detail 返回的错误 code 在 _pick_article_content 之前
    # 的 detail.get('_err') 里, content_md 空就是 empty-detail 信号.
    if not skip_detail:
        _tripwire_record_detail("article", resp=detail, content_len=len(content_md))
    if content_md or transcribe_md:
        return "added", doc["stats"]
    return "added_no_content", doc["stats"]


def dump_opinion(session, db, item: dict, force: bool = False,
                 skip_detail: bool = False) -> tuple[str, dict]:
    """观点入库. collection=opinions, _id=o<id>.

    平台 /opinions/opinion_list 返回:
      id, title, content (HTML), type (summary/repost/...), identity,
      expected_trend (bullish/bearish/neutral), stock_tracing,
      corporations, industries, hashtags, user (nickname/org/position),
      view_count, like_count, favorite_count, comment_count, release_time (sec).
    """
    col = db["opinions"]
    did = dedup_id_opinion(item)

    if not force:
        ex = col.find_one({"_id": did}, {"_id": 1, "content_md": 1, "stats": 1})
        if ex and (ex.get("content_md") or ""):
            return "skipped", ex.get("stats") or {"content_chars": len(ex.get("content_md") or "")}

    detail = {} if skip_detail else fetch_opinion_detail(session, item.get("id"))

    content_md = _strip_html(str(detail.get("content") or item.get("content") or "").strip())
    title = (item.get("title") or detail.get("title") or "").strip()
    # title 经常为空 (短观点), 用正文前 60 字作为标题兜底
    if not title and content_md:
        title = content_md[:60].replace("\n", " ")

    release_sec = item.get("release_time") or detail.get("release_time") or 0
    release_time = _sec_to_str(release_sec)

    user = item.get("user") or detail.get("user") or {}
    org_name = ""
    org_id = None
    if isinstance(user, dict):
        org_name = (user.get("organization_name") or user.get("nickname")
                    or user.get("name") or "")
        org_id = user.get("organization_id") or user.get("id")

    stock_tracing = item.get("stock_tracing") or detail.get("stock_tracing") or {}
    brief_md = content_md[:500]

    doc = {
        "_id": did,
        "category": "opinion",
        "subtype": (item.get("type") or "").strip().lower() or "opinion",
        "raw_id": item.get("id"),
        "title": title,
        "release_time": release_time,
        "release_time_ms": int(release_sec) * 1000 if release_sec else None,
        "release_time_sec": int(release_sec) if release_sec else None,
        "organization": org_name,
        "organization_id": org_id,
        "organization_raw": user,
        "identity": item.get("identity") or detail.get("identity"),
        "expected_trend": item.get("expected_trend") or detail.get("expected_trend"),
        "stock_tracing": stock_tracing,
        "corporations": _item_corporations({**item, **detail}),
        "corporation_ids": item.get("corporation_ids") or detail.get("corporation_ids") or [],
        "industries": item.get("industries") or detail.get("industries") or [],
        "industry_ids": item.get("industry_ids") or detail.get("industry_ids") or [],
        "custom_sector_ids": item.get("custom_sector_ids") or detail.get("custom_sector_ids") or [],
        "hashtags": _item_hashtags({**item, **detail}),
        "related_topics": item.get("related_topics") or detail.get("related_topics") or [],
        "cover_image": item.get("cover_image") or detail.get("cover_image"),
        "views": int(item.get("view_count") or detail.get("view_count") or 0),
        "likes": int(item.get("like_count") or detail.get("like_count") or 0),
        "dislikes": int(item.get("dislike_count") or detail.get("dislike_count") or 0),
        "favorites": int(item.get("favorite_count") or detail.get("favorite_count") or 0),
        "comment_count": int(item.get("comment_count") or detail.get("comment_count") or 0),
        "brief_md": brief_md,
        "content_md": content_md,
        "summary_md": "",
        "transcribe_md": "",
        "list_item": item,
        "detail_result": detail if detail and not detail.get("_err") else None,
        "web_url": f"{WEB_BASE}/viewpoint/detail/{item.get('id')}",
        "stats": {
            "content_chars": len(content_md),
        },
        "crawled_at": datetime.now(timezone.utc),
    }
    _stamp_ticker(doc, "acecamp", col)
    _upsert_preserve_crawled_at(col, did, doc)
    if not skip_detail:
        _tripwire_record_detail("opinion", resp=detail, content_len=len(content_md))
    return "added", doc["stats"]


# ==================== 分页抓取 ====================

def _items_from_list_resp(resp: dict) -> list[dict]:
    """AceCamp 列表响应 data 已是数组."""
    if not _is_ok(resp):
        return []
    data = resp.get("data")
    return data if isinstance(data, list) else []


def _item_time_sec(item: dict) -> Optional[int]:
    """AceCamp 用秒级 unix timestamp."""
    sec = item.get("release_time") or item.get("shown_time")
    try:
        return int(sec) if sec else None
    except (TypeError, ValueError):
        return None


_LIST_FETCHERS = {
    "articles": fetch_articles_list,
    "opinions": fetch_opinions_list,
}


def fetch_items_paginated(session, content_type: str,
                          max_items: Optional[int],
                          page_size: int,
                          stop_at_id: Optional[str] = None,
                          stop_before_ms: Optional[int] = None,
                          make_dedup=None) -> list[dict]:
    """按 content_type 分页抓取, 直到 list 空 / 命中 stop 条件.

    - stop_at_id: 命中上次 top 即停 (增量模式)
    - stop_before_ms: 条目毫秒时间戳 < 该值即停 (--since-hours)
    """
    all_items: list[dict] = []
    page = 1
    empty_streak = 0
    while True:
        try:
            resp = _LIST_FETCHERS[content_type](session, page, page_size)
        except SessionDead:
            raise
        except Exception as e:
            tqdm.write(f"  [{content_type} p{page}] 请求异常: {e}")
            break

        items = _items_from_list_resp(resp)
        if not items:
            empty_streak += 1
            msg = resp.get("msg") or ""
            code = resp.get("code")
            tqdm.write(f"  [{content_type} p{page}] 空 (code={code} msg={msg}) streak={empty_streak}")
            if empty_streak >= 2:
                break
            _THROTTLE.sleep_before_next()
            page += 1
            continue
        empty_streak = 0

        hit_known = hit_old = False
        new_this = 0
        for it in items:
            # 列表不保证严格时间降序, 扫完本页再让 dump 逐条 dedup
            if stop_at_id and make_dedup and make_dedup(it) == stop_at_id:
                hit_known = True
                continue
            if stop_before_ms is not None:
                ts_sec = _item_time_sec(it)
                if ts_sec is not None and ts_sec * 1000 < stop_before_ms:
                    hit_old = True
                    continue
            all_items.append(it)
            new_this += 1
            if max_items and len(all_items) >= max_items:
                return all_items[:max_items]

        # 页头/尾时间戳 (便于日志观测)
        meta = resp.get("meta") or {}
        total = meta.get("total")
        tqdm.write(f"  [{content_type} p{page}] +{new_this}/{len(items)} "
                   f"(累计 {len(all_items)}/total={total}) "
                   f"hit_known={hit_known} hit_old={hit_old}")

        if hit_known or hit_old:
            break
        if len(items) < page_size:
            break
        page += 1
        _THROTTLE.sleep_before_next()
    return all_items[:max_items] if max_items else all_items


# ==================== checkpoint / _state ====================

def state_doc_id(content_type: str) -> str:
    return f"crawler_{content_type}"


def load_state(db, content_type: str) -> dict:
    return db[COL_STATE].find_one({"_id": state_doc_id(content_type)}) or {}


def save_state(db, content_type: str, **kwargs) -> None:
    kwargs["updated_at"] = datetime.now(timezone.utc)
    db[COL_STATE].update_one(
        {"_id": state_doc_id(content_type)},
        {"$set": kwargs}, upsert=True,
    )


# ==================== 一轮抓取 ====================

_DEDUP_FUNC = {
    "articles": dedup_id_article,
    "opinions": dedup_id_opinion,
}

_DUMP_FUNC = {
    "articles": lambda sess, db, item, args: dump_article(
        sess, db, item, force=args.force, skip_detail=args.skip_detail),
    "opinions": lambda sess, db, item, args: dump_opinion(
        sess, db, item, force=args.force, skip_detail=args.skip_detail),
}

_COL_NAME = {"articles": "articles", "opinions": "opinions"}
_LABEL = {"articles": "文章/纪要/调研", "opinions": "观点"}


def run_type(session, db, content_type: str, args) -> dict:
    """流式抓取: 每抓到一页立刻入库, 不把整个列表先载入内存.

    好处:
      1. 启动后第一分钟就能看到库里有新数据, 不用等 30min 翻完 833 页
      2. 中途 kill 后, 已抓的都在库里, 下次 resume 靠 item-level dedup 跳过, 不丢
      3. top_dedup_id 只在"run 完整走完"时才推进, 避免提前推进导致未来 run 漏抓
    """
    cfg_label = _LABEL[content_type]
    dedup = _DEDUP_FUNC[content_type]
    col = db[_COL_NAME[content_type]]
    print(f"\n{'─' * 60}\n[{cfg_label} / {content_type}]  collection={_COL_NAME[content_type]}\n{'─' * 60}")

    state = load_state(db, content_type)
    stop_id = state.get("top_dedup_id") if args.resume else None
    if args.resume and stop_id:
        print(f"[恢复] 上次 top={stop_id[:24]}.. updated_at={state.get('updated_at')}")
    elif args.resume:
        print("[恢复] 未找到 checkpoint, 全量跑")

    stop_ms = None
    if getattr(args, "since_hours", None) is not None:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=args.since_hours)
        stop_ms = int(cutoff.timestamp() * 1000)
        print(f"[时间窗] cutoff={cutoff:%Y-%m-%d %H:%M} ({args.since_hours}h)")

    print(f"[流式抓取] max={args.max or '全部'} page_size={args.page_size}")

    added = skipped = failed = 0
    cap = cap_from_args(args)
    new_top_id = None          # 由第一页第一条决定; 完整跑完才落库
    run_completed = False      # 走到列表尽头 / 命中 stop 条件 = True
    page = 1
    total_processed = 0        # 用于 --max 上限

    pbar = tqdm(desc=cfg_label, unit="条", dynamic_ncols=True,
                bar_format="{l_bar}| {n_fmt} [{elapsed}] {postfix}")

    while True:
        if cap.exhausted() or _BUDGET.exhausted():
            tqdm.write(f"  [antibot] 达到 daily-cap={cap.max_items}, 本轮停 (防风控)")
            break
        if args.max and total_processed >= args.max:
            tqdm.write(f"  [max] 达到 --max={args.max}, 本轮停")
            run_completed = True  # 可以推进 top
            break

        # ---- 抓一页 ----
        try:
            resp = _LIST_FETCHERS[content_type](session, page, args.page_size)
        except SessionDead:
            raise
        except Exception as e:
            tqdm.write(f"  [{content_type} p{page}] 请求异常: {e}")
            break

        items = _items_from_list_resp(resp)
        if not items:
            tqdm.write(f"  [{content_type} p{page}] 空响应 → 走到列表尽头")
            run_completed = True
            break

        # 第一页第一条 = 本次 run 的 top 水位 (仅记住, 完整走完才写)
        if new_top_id is None:
            new_top_id = dedup(items[0])

        # ---- 逐条入库 ----
        page_added = page_skipped = page_failed = 0
        hit_known = hit_old = False
        for it in items:
            did = dedup(it)
            # 列表不保证严格时间降序, hit_known/hit_old 仅作翻页提示, 继续本页剩余条目
            # (下游 dump_* 有 per-item dedup, 已存在会 return "skipped")
            if stop_id and did == stop_id:
                hit_known = True
                continue
            if stop_ms is not None:
                ts_sec = _item_time_sec(it)
                if ts_sec is not None and ts_sec * 1000 < stop_ms:
                    hit_old = True
                    continue
            if cap.exhausted() or _BUDGET.exhausted():
                break
            if args.max and total_processed >= args.max:
                break

            title = (it.get("title") or it.get("name") or "")[:60]
            try:
                status, info = _DUMP_FUNC[content_type](session, db, it, args)
                if status == "skipped":
                    skipped += 1; page_skipped += 1
                    # 首轮全量时大量已入库 => 安静跳过, 避免日志爆炸
                else:
                    added += 1; page_added += 1
                    cap.bump(); _BUDGET.bump()
                    parts = []
                    if info.get("content_chars"):
                        parts.append(f"content={info['content_chars']}字")
                    if info.get("transcribe_chars"):
                        parts.append(f"transcribe={info['transcribe_chars']}字")
                    if info.get("description_chars") and not info.get("content_chars"):
                        parts.append(f"desc={info['description_chars']}字")
                    suffix = "  " + "  ".join(parts) if parts else ""
                    marker = "✓" if status == "added" else "◉"
                    tqdm.write(f"  {marker} [{did[:16]}] {title}{suffix}")
            except SessionDead:
                raise
            except Exception as e:
                failed += 1; page_failed += 1
                tqdm.write(f"  ✗ [{did[:16]}] {title}  ERR: {type(e).__name__}: {e}")

            total_processed += 1
            pbar.update(1)
            pbar.set_postfix_str(f"p{page} +{added} ={skipped} ✗{failed}")
            # 细粒度 ping, 便于监控实时看到进展 (但不推进 top)
            save_state(db, content_type,
                       last_dedup_id=did,
                       last_processed_at=datetime.now(timezone.utc),
                       current_page=page,
                       in_progress=True)
            # 只在真的打了网络 (add/update/error) 时节流; skipped 是纯 mongo 查询, 不需要
            if status != "skipped":
                _THROTTLE.sleep_before_next()

        meta = resp.get("meta") or {}
        tqdm.write(f"  [{content_type} p{page}] 页内 +{page_added} ={page_skipped} ✗{page_failed} "
                   f"(累计入库 +{added}/total={meta.get('total','?')}) "
                   f"hit_known={hit_known} hit_old={hit_old}")

        if hit_known or hit_old:
            run_completed = True
            break
        if len(items) < args.page_size:
            run_completed = True  # 最后一页
            break
        page += 1
        _THROTTLE.sleep_before_next()

    pbar.close()

    # 关键: 只有 run 完整走完才推进 top_dedup_id.
    # 提前推进会导致未来 watch 漏抓 (stop_at_id 在半截地方停止).
    state_update = {
        "in_progress": False,
        "last_run_end_at": datetime.now(timezone.utc),
        "last_run_stats": {"added": added, "skipped": skipped, "failed": failed,
                           "partial": not run_completed, "pages_scanned": page},
    }
    if run_completed and new_top_id:
        state_update["top_dedup_id"] = new_top_id
    save_state(db, content_type, **state_update)

    total = col.estimated_document_count()
    tag = "✓ 完整" if run_completed else "✗ 中断 (top 未推进, 下轮重扫)"
    print(f"  {tag}: 新增 {added} / 跳过 {skipped} / 失败 {failed} / 扫过 {page} 页")
    print(f"  {_COL_NAME[content_type]} 总数: {total}")
    return {"added": added, "skipped": skipped, "failed": failed}


def run_once(session, db, args) -> dict:
    types = TYPE_ORDER if args.type == "all" else [args.type]
    summary: dict = {}
    for t in types:
        try:
            summary[t] = run_type(session, db, t, args)
        except KeyboardInterrupt:
            raise
        except SessionDead as e:
            print(f"\n[致命] 会话失效: {e}")
            print(f"  → 浏览器重登 {WEB_BASE}, 更新 credentials.json 里的 cookie.")
            summary[t] = {"added": 0, "skipped": 0, "failed": -1, "error": "SessionDead"}
            break
        except Exception as e:
            tqdm.write(f"\n[{t}] 异常: {type(e).__name__}: {e}")
            summary[t] = {"added": 0, "skipped": 0, "failed": -1, "error": str(e)}
    print(f"\n{'═' * 60}")
    print("本轮汇总: " + "  ".join(
        f"{t}+{s.get('added',0)}/={s.get('skipped',0)}/✗{s.get('failed',0)}"
        for t, s in summary.items()))
    print(f"{'═' * 60}")
    return summary


# ==================== 当日统计 --today ====================

_BJ_TZ = timezone(timedelta(hours=8))


def count_today(session, db, args) -> dict:
    # AceCamp release_time 是 Asia/Shanghai 秒级时间戳, --today 按 BJ 日历日对齐.
    if args.date:
        day_start = datetime.strptime(args.date, "%Y-%m-%d").replace(tzinfo=_BJ_TZ)
        target = args.date
    else:
        day_start = datetime.now(_BJ_TZ).replace(hour=0, minute=0, second=0, microsecond=0)
        target = day_start.strftime("%Y-%m-%d")
    day_end = day_start.replace(hour=23, minute=59, second=59)
    start_sec = int(day_start.timestamp())
    end_sec = int(day_end.timestamp())

    print(f"[统计] 扫描各类 {target} 平台条数...")
    types = TYPE_ORDER if args.type == "all" else [args.type]
    overall: dict = {"date": target}
    for t in types:
        dedup = _DEDUP_FUNC[t]
        items_today: list[dict] = []
        page = 1
        stop = False
        scanned = 0
        while not stop:
            try:
                resp = _LIST_FETCHERS[t](session, page, args.page_size)
            except Exception as e:
                print(f"  [{t} p{page}] 失败: {e}")
                break
            items = _items_from_list_resp(resp)
            if not items:
                break
            scanned += len(items)
            for it in items:
                ts = _item_time_sec(it)
                if ts is None:
                    continue
                if ts < start_sec:
                    stop = True
                    break
                if ts <= end_sec:
                    items_today.append(it)
            if len(items) < args.page_size:
                break
            page += 1
            _THROTTLE.sleep_before_next()

        ids = [dedup(it) for it in items_today]
        in_db = db[_COL_NAME[t]].count_documents({"_id": {"$in": ids}}) if ids else 0
        overall[t] = {
            "platform_count": len(items_today),
            "in_db": in_db,
            "missing": len(items_today) - in_db,
            "pages_scanned": page,
        }
        print(f"  {_LABEL[t]:>10s} ({t}): 平台 {overall[t]['platform_count']:>4d}  "
              f"入库 {in_db:>4d}  缺 {overall[t]['missing']:>4d}")

    overall["scanned_at"] = datetime.now(timezone.utc)
    db[COL_STATE].replace_one({"_id": f"daily_{target}"},
                              {"_id": f"daily_{target}", **overall},
                              upsert=True)
    print(f"\n已存 {COL_STATE} (_id=daily_{target})")
    return overall


# ==================== account / 元数据 ====================

ACCOUNT_ENDPOINTS = [
    ("user-center", "GET", "/users/account", None),
    ("user-info", "GET", "/users/info", None),
    ("feeds-trends", "GET", "/feeds/trends", None),
    ("feeds-statistics", "GET", "/feeds/statistics", None),
    ("knowledge-labels", "GET", "/knowledge/labels/list", None),
]


def dump_account(session, db) -> None:
    print("\n[账户] 抓取账户级 / 元数据接口...")
    col = db[COL_ACCOUNT]
    now = datetime.now(timezone.utc)
    for name, method, path, body in ACCOUNT_ENDPOINTS:
        try:
            resp = api_call(session, method, path, json_body=body)
        except SessionDead:
            print(f"  [✗] {name}  Cookie 失效")
            continue
        except Exception as e:
            resp = {"_err": str(e)}
        col.replace_one(
            {"_id": name},
            {"_id": name, "endpoint": path, "method": method,
             "response": resp, "updated_at": now},
            upsert=True,
        )
        ok = _is_ok(resp)
        tag = "✓" if ok else f"code={resp.get('code')}"
        print(f"  [{tag}] {name}")


# ==================== CLI ====================

def parse_args():
    p = argparse.ArgumentParser(
        description="api.acecamptech.com 文章/纪要/调研/观点/路演 爬虫 (MongoDB)")
    p.add_argument("--type", choices=["all", *TYPE_ORDER], default="all",
                   help=f"指定类型 (默认 all). 可选: {', '.join(TYPE_ORDER)}")
    p.add_argument("--max", type=int, default=None,
                   help="最多爬 N 条 (每类). 默认翻页到尽头")
    p.add_argument("--page-size", type=int, default=PAGE_SIZE_DEFAULT,
                   help=f"每页大小 (默认 {PAGE_SIZE_DEFAULT})")
    p.add_argument("--force", action="store_true",
                   help="强制重爬已入库的内容")
    p.add_argument("--resume", action="store_true",
                   help="增量模式: 遇到上次 top_dedup_id 即停止分页")
    p.add_argument("--stream-backfill", action="store_true",
                   help="兼容 flag, no-op (acecamp 未实现流式, 走普通翻页即可)")
    p.add_argument("--skip-detail", action="store_true",
                   help="不拉详情, 只存列表字段 (快速但无 content/transcribe)")
    p.add_argument("--watch", action="store_true",
                   help="实时模式: 定时轮询. Ctrl+C 退出")
    p.add_argument("--interval", type=int, default=600,
                   help="实时模式轮询间隔秒数 (默认 600)")
    p.add_argument("--since-hours", type=float, default=None,
                   help="仅抓过去 N 小时内的内容 (按 release_time)")
    p.add_argument("--show-state", action="store_true",
                   help="打印各类 checkpoint + cookie 健康检查 后退出")
    p.add_argument("--reset-state", action="store_true",
                   help="清除所有 crawler_* checkpoint (保留 daily_* 统计)")
    p.add_argument("--today", action="store_true",
                   help="扫各类平台当日条数 vs 本地库, 结果存 _state")
    p.add_argument("--date", metavar="YYYY-MM-DD", default=None,
                   help="配合 --today 指定日期")
    p.add_argument("--clean", choices=list(TYPE_ORDER), default=None,
                   help="清空指定类型集合 + checkpoint 后退出")
    p.add_argument("--auth",
                   default=_load_cookie_from_file() or os.environ.get("ACECAMP_AUTH") or ACECAMP_COOKIE,
                   help="Cookie 字符串 (优先级: credentials.json > env ACECAMP_AUTH > 脚本内 ACECAMP_COOKIE)")
    p.add_argument("--mongo-uri", default=MONGO_URI_DEFAULT,
                   help=f"MongoDB URI (默认 {MONGO_URI_DEFAULT})")
    p.add_argument("--mongo-db", default=MONGO_DB_DEFAULT,
                   help=f"MongoDB 数据库名 (默认 {MONGO_DB_DEFAULT})")
    # 反爬节流默认 — 2026-04-24 事故后重写 (原 base=2.5/jitter=1.5/burst=30/cap=500
    # 导致 detail quota 几分钟烧光). 新默认跟 ANTIBOT.md backfill 默认 (3.5/2.0/30/400)
    # 对齐偏保守: 4.0/2.5/20/300. 实际调用方 (crawler_manager.SPECS / daily_catchup /
    # backfill_6months) 会再覆盖成 variant-specific 值, 这里是 CLI 裸跑兜底.
    add_antibot_args(p, default_base=4.0, default_jitter=2.5,
                     default_burst=20, default_cap=300, platform="acecamp")
    return p.parse_args()


def connect_mongo(uri: str, dbname: str):
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    try:
        client.admin.command("ping")
    except PyMongoError as e:
        print(f"错误: 无法连接 MongoDB ({uri}): {e}")
        sys.exit(1)
    db = client[dbname]
    for t, cname in _COL_NAME.items():
        col = db[cname]
        col.create_index("title")
        col.create_index("release_time")
        col.create_index("release_time_ms")
        col.create_index("organization")
        col.create_index("crawled_at")
        col.create_index("subtype")  # articles 区分 minutes|research|article
        col.create_index("corporation_ids")
        if cname == "opinions":
            col.create_index("expected_trend")
    print(f"[Mongo] 已连接 {uri} -> db: {dbname}")
    return db


def show_state(session, db) -> None:
    print("=" * 60)
    print("AceCamp Checkpoint")
    print("=" * 60)
    for t in TYPE_ORDER:
        s = load_state(db, t)
        if not s:
            print(f"  {t:>10s}: (无)")
            continue
        print(f"  {t:>10s}: top={str(s.get('top_dedup_id'))[:24]}.. "
              f"updated_at={s.get('updated_at')} "
              f"last_run={s.get('last_run_stats')}")
    print()
    print("Collection 总数:")
    for t, cname in _COL_NAME.items():
        n = db[cname].estimated_document_count()
        print(f"  {t:>10s} ({cname}): {n}")
    # Cookie 健康 — 调一个真实业务接口
    print()
    try:
        r = api_call(session, "GET", "/articles/article_list",
                     params={"page": 1, "per_page": 1})
        if _is_ok(r):
            meta = r.get("meta") or {}
            print(f"[cookie] ✓ 列表 OK, total={meta.get('total')} ts={meta.get('ts')}")
        else:
            print(f"[cookie] ✗ code={r.get('code')} msg={r.get('msg')}")
    except SessionDead as e:
        print(f"[cookie] ✗ Cookie 失效: {e}")
    except Exception as e:
        print(f"[cookie] ? 探测异常: {e}")


def main():
    args = parse_args()

    # 全局停爬闸门 (2026-04-24 加): 账号被平台封控期间,list 端点仍返摘要但
    # detail 返 quota code, scraper 会灌一堆 content_md 空的壳子, 把 dashboard
    # 的 "今日入库" 数字染成假的绿。用户明确要求:账号恢复前一律不抓。
    # 触发条件 (任一命中即 abort): 仓库根 crawl/AceCamp/DISABLED 文件存在 或
    # env ACECAMP_DISABLED 非空。放在 --show-state 之后的所有路径之前,让健康
    # 探针/诊断依然可用, 但任何抓取命令都被拦住。
    # 恢复方法: `rm crawl/AceCamp/DISABLED` 后正常启动 scraper / watcher。
    _DISABLE_FILE = Path(__file__).parent / "DISABLED"
    _is_probe_only = (args.show_state or args.today or args.clean or args.reset_state)
    if not _is_probe_only:
        if _DISABLE_FILE.exists() or os.getenv("ACECAMP_DISABLED"):
            reason = ""
            if _DISABLE_FILE.exists():
                try:
                    reason = _DISABLE_FILE.read_text(encoding="utf-8").strip()[:500]
                except Exception:
                    pass
            print("=" * 60)
            print("[AceCamp] 抓取已被全局关闭 — 拒绝启动任何抓取任务")
            print(f"  闸门文件: {_DISABLE_FILE}")
            if reason:
                print(f"  原因: {reason}")
            print("  恢复: 删除 DISABLED 文件后重新启动")
            print("=" * 60)
            sys.exit(0)

    if not args.auth:
        print("错误: 未提供 Cookie. 用 --auth / env ACECAMP_AUTH 传入, "
              "或编辑 credentials.json 的 {\"cookie\": \"...\"}.")
        sys.exit(1)

    global _THROTTLE, _BUDGET
    _THROTTLE = throttle_from_args(args, platform="acecamp")
    # account_id 取 Cookie 里 user_token JWT 第二段的 uid (找不到回 hash)
    _account_id = "h_unknown"
    try:
        import re as _re
        import base64 as _b64
        import json as _json
        m = _re.search(r"user_token=([^;\s]+)", args.auth or "")
        if m:
            jwt_body = m.group(1).split(".")
            if len(jwt_body) >= 2:
                pad = jwt_body[1] + "=" * (-len(jwt_body[1]) % 4)
                payload = _json.loads(_b64.urlsafe_b64decode(pad))
                for k in ("user_id", "userId", "uid", "id", "sub"):
                    v = payload.get(k)
                    if v:
                        _account_id = f"u_{v}"
                        break
    except Exception:
        pass
    if _account_id == "h_unknown":
        import hashlib as _hl
        _account_id = "h_" + _hl.md5((args.auth or "").encode()).hexdigest()[:12]
    _BUDGET = budget_from_args(args, account_id=_account_id, platform="acecamp")
    log_config_stamp(_THROTTLE, cap=cap_from_args(args), budget=_BUDGET,
                     extra=f"acct={_account_id}")

    db = connect_mongo(args.mongo_uri, args.mongo_db)
    session = create_session(args.auth)

    if args.show_state:
        show_state(session, db)
        return

    if args.reset_state:
        n = db[COL_STATE].delete_many({"_id": {"$regex": "^crawler_"}}).deleted_count
        print(f"已清除 {n} 条 crawler_* checkpoint (daily_* 统计保留)")
        return

    if args.clean:
        cname = _COL_NAME[args.clean]
        n_docs = db[cname].estimated_document_count()
        db[cname].drop()
        n_state = db[COL_STATE].delete_many({"_id": state_doc_id(args.clean)}).deleted_count
        print(f"已清除 {cname} 集合 ({n_docs} 条) + crawler_{args.clean} checkpoint "
              f"({n_state} 条).")
        return

    if args.today:
        count_today(session, db, args)
        return

    # 首次抓元数据
    if db[COL_ACCOUNT].estimated_document_count() == 0 or args.force:
        dump_account(session, db)

    if args.watch:
        print(f"\n[实时模式] 每 {args.interval}s 轮询. Ctrl+C 退出.")
        round_num = 0
        while True:
            round_num += 1
            print(f"\n{'═' * 60}\n[轮次 {round_num}] {datetime.now():%Y-%m-%d %H:%M:%S}\n{'═' * 60}")
            try:
                run_once(session, db, args)
            except KeyboardInterrupt:
                print("\n[实时模式] Ctrl+C 退出"); break
            except Exception as e:
                print(f"[轮次 {round_num}] 异常: {type(e).__name__}: {e}")
            _THROTTLE.reset()
            try:
                time.sleep(args.interval)
            except KeyboardInterrupt:
                print("\n[实时模式] Ctrl+C 退出"); break
    else:
        run_once(session, db, args)


if __name__ == "__main__":
    main()
