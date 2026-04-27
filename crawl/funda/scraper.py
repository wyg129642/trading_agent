#!/usr/bin/env python3
"""
funda.ai 多分类爬虫 (MongoDB 存储).

抓取三大数据类别:
  post                funda_reports: Funda AI 研究文章 (中/英双语, markdown body)
  earnings_report     earnings-report: 8-K SEC 业绩公告 (HTML content)
  earnings_transcript earnings-transcript: 财报电话会逐字稿 (plain text)

所有列表/详情都走 tRPC batch=1 接口; 鉴权只需要 session-token cookie.

使用方法:
  1. 浏览器登录 https://funda.ai
  2. F12 → Application → Cookies 或 Console 敲 `document.cookie` 整行复制
  3. 粘到 credentials.json 的 "cookie" 字段 (已 gitignore)
  4. 启动本地 MongoDB (默认 mongodb://localhost:27017)
  5. 运行:
       python3 scraper.py --show-state               # token 健康 + checkpoint
       python3 scraper.py --max 20                   # 各分类各抓 20 条
       python3 scraper.py --category post --max 50   # 单分类
       python3 scraper.py --watch --resume --interval 600
       python3 scraper.py --today                    # 今日统计

数据存储:
  - MongoDB db=funda, 集合: posts / earnings_reports / earnings_transcripts
  - account: 用户信息 / 可用 ticker 列表 / 权限
  - _state: checkpoint + 日统计

共享 crawl/ 爬虫约定 (见 crawl/README.md 第 4 节 + 第 7 节反爬).
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import urllib.parse
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
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
    warmup_session,
)
from ticker_tag import stamp as _stamp_ticker  # noqa: E402

# ==================== 可调常量 ====================

BASE_URL = "https://funda.ai"
TRPC_BASE = f"{BASE_URL}/api/trpc"

CREDS_FILE = Path(__file__).resolve().parent / "credentials.json"

DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36"
)

# 不走 trpc-accept: application/jsonl — 那样响应是分块 JSONL, 解析麻烦.
# 缺省就是 "application/json", 整包 `[{result:{data:{json:{...}}}}]` 更好处理.
DEFAULT_HEADERS = {
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
    "Referer": f"{BASE_URL}/reports",
    "Origin": BASE_URL,
    "x-trpc-source": "nextjs-react",
    # 重要: 不要设 Content-Type: application/json (只有 GET 列表/详情, POST 少)
    # 也不要设 accept-encoding: br — httpx 自己处理
}

MONGO_URI_DEFAULT = os.environ.get(
    "MONGO_URI",
    "mongodb://127.0.0.1:27018/",
)
MONGO_DB_DEFAULT = os.environ.get("MONGO_DB", "funda")
COL_ACCOUNT = "account"
COL_STATE = "_state"

DEFAULT_TIMEOUT = 30.0
DEFAULT_MAX_RETRIES = 4

# 模块级 throttle — main() 用 CLI 参数覆盖后再放回来
# 2026-04-24: base 3.0→3.5 / jitter 2.0→2.5. 起因: 04-23 earnings_report
# watcher 吃了一次 HTTP 401, 且平台 Q1 2026 财报季即将到来 (5月1日起业绩会密集发布).
# 在 ANTIBOT_V2 historical 档基础上 +0.5s 缓冲, σ 从 1.0 → 1.25 弱化节奏指纹.
# 此默认仅影响手动/backfill 调用; crawler_monitor 实时档通过 CLI 显式传 1.5/1.0.
_THROTTLE: AdaptiveThrottle = AdaptiveThrottle(base_delay=3.5, jitter=2.5,
                                                burst_size=40,
                                                platform="funda")
_BUDGET: AccountBudget = AccountBudget("funda", "default", 0)
_PLATFORM = "funda"


# ==================== 分类配置 ====================
#
# 三个分类字段说明:
#   label                 人类可读名
#   collection            MongoDB 集合名
#   list_procedure        tRPC procedure, 列表接口
#   detail_procedure      tRPC procedure, 详情接口 (None = 列表已含全量, 不抓详情)
#   list_json_extra       列表 query 默认 body (除 limit/cursor/direction 外)
#   list_meta_undef       meta.values 中的 undefined 字段列表 (tRPC superjson 需要)
#   cursor_kind           "string" (单字段 id) / "object" (多字段 {id, publishedAt})
#   detail_input_kind     "slug_string" (post.fetchBySlug 吃 string) / "id_obj" ({id}) / None
#   time_field            列表项发布时间字段名 (用于 --today / since-hours)
# ==================================================================

CATEGORIES: Dict[str, Dict[str, Any]] = {
    "post": {
        "label": "Funda 研究文章",
        "collection": "posts",
        "list_procedure": "post.fetchInfinite",
        "detail_procedure": "post.fetchBySlug",
        "list_json_extra": {
            "sortBy": "latest",
            "tag": None,
            "tickers": None,
            "enterpriseOnly": None,
        },
        "list_meta_undef": ["tag", "tickers", "enterpriseOnly"],
        "cursor_kind": "object",   # {id, publishedAt}
        "detail_input_kind": "slug_string",
        "time_field": "publishedAt",
        "web_url_fmt": BASE_URL + "/reports/{slug}",
    },
    "earnings_report": {
        "label": "8-K 业绩公告",
        "collection": "earnings_reports",
        "list_procedure": "companyEarning.fetchEightKReports",
        "detail_procedure": "companyEarning.fetchById",
        "list_json_extra": {
            "dateFilter": "all",
            "customDate": None,
            "ticker": None,
            "tickers": None,
            "industry": "",
            "searchQuery": None,
        },
        "list_meta_undef": ["customDate", "ticker", "tickers", "searchQuery"],
        "cursor_kind": "string",
        "detail_input_kind": "id_obj",
        "time_field": "date",
        "web_url_fmt": BASE_URL + "/reports/earnings/{id}",
        "sweep_key": "earnings_report",  # 2026-04-23: --sweep-today --date 注入入口
    },
    "earnings_transcript": {
        "label": "财报电话会逐字稿",
        "collection": "earnings_transcripts",
        "list_procedure": "companyEarning.fetchTranscripts",
        "detail_procedure": "companyEarning.fetchById",
        "list_json_extra": {
            "dateFilter": "all",
            "customDate": None,
            "ticker": None,
            "tickers": None,
            "industry": "",
            "searchQuery": None,
        },
        "list_meta_undef": ["customDate", "ticker", "tickers", "searchQuery"],
        "cursor_kind": "string",
        "detail_input_kind": "id_obj",
        "time_field": "date",
        "web_url_fmt": BASE_URL + "/reports/earnings/{id}",
        "sweep_key": "earnings_transcript",
    },
}

CATEGORY_ORDER = ["post", "earnings_report", "earnings_transcript"]

# ==================== 情绪因子 (api.funda.ai) ====================

SENTIMENT_API_BASE = "https://api.funda.ai/v1"
SENTIMENT_API_PATH = "/sentiment-scores"
COL_SENTIMENTS = "sentiments"


# ==================== 凭证 ====================

def load_creds() -> tuple[str, str]:
    """读取 credentials.json. 返回 (cookie, user_agent)."""
    cookie, ua, _ = load_creds_ext()
    return cookie, ua


def load_creds_ext() -> tuple[str, str, str]:
    """Extended: 返回 (cookie, user_agent, api_key). api_key 用于 api.funda.ai/v1/*."""
    if not CREDS_FILE.exists():
        print(f"错误: 未找到 {CREDS_FILE}")
        print("请创建 credentials.json, 至少包含 cookie 字段. 示例:")
        print('  {"cookie": "__Secure-x-geo-country=...; session-token=...; ...",')
        print('   "api_key": "funda-sk-...", "user_agent": "Mozilla/5.0 ..."}')
        sys.exit(1)
    try:
        data = json.loads(CREDS_FILE.read_text())
    except json.JSONDecodeError as e:
        print(f"错误: credentials.json 解析失败: {e}")
        sys.exit(1)
    cookie = (data.get("cookie") or "").strip()
    ua = (data.get("user_agent") or "").strip() or DEFAULT_UA
    api_key = (data.get("api_key") or "").strip()
    if not cookie:
        print("错误: credentials.json 缺少 'cookie' 字段")
        sys.exit(1)
    return cookie, ua, api_key


def parse_cookies(cookie_str: str) -> Dict[str, str]:
    """'k=v; k2=v2' → dict."""
    out: Dict[str, str] = {}
    for part in cookie_str.split(";"):
        part = part.strip()
        if not part or "=" not in part:
            continue
        k, v = part.split("=", 1)
        k, v = k.strip(), v.strip()
        if not k or k.lower() == "set-cookie":
            continue
        out[k] = v
    return out


def extract_session_token(cookie_str: str) -> Optional[str]:
    for part in cookie_str.split(";"):
        part = part.strip()
        if part.startswith("session-token="):
            return part.split("=", 1)[1]
    return None


# ==================== HTTP ====================

def create_client(cookie: str, user_agent: str,
                  timeout: float = DEFAULT_TIMEOUT) -> httpx.Client:
    """trust_env=False 绕系统代理; follow_redirects 处理 WAF 中转.
    UA 走 antibot.headers_for_platform("funda") — 18 个 watcher 自动分到 5-8 个 UA."""
    headers = headers_for_platform("funda")
    if user_agent:
        headers["User-Agent"] = user_agent
    # Funda 特定头 (referer/origin 已由 platform 设, x-trpc-source 是它独有的)
    headers["x-trpc-source"] = "nextjs-react"
    c = httpx.Client(
        base_url=BASE_URL,
        cookies=parse_cookies(cookie),
        headers=headers,
        trust_env=False,
        timeout=timeout,
        follow_redirects=True,
    )
    # Warmup: 先 GET funda.ai landing 再发 tRPC
    warmup_session(c, "funda")
    return c


def _enc_input(inp: dict) -> str:
    """tRPC batch=1 input 的 URL-encoded JSON."""
    return urllib.parse.quote(json.dumps(inp, separators=(",", ":")))


def trpc_get(client: httpx.Client, procedure: str, input_obj: dict,
             what: str = "") -> dict:
    """发一个 batch=1 的 tRPC GET, 返回 result.data.json (即内层业务 data).

    自动:
      - 401/403 → SessionDead
      - 429/5xx → 退避重试 (antibot)
      - 其他 HTTP 错 → raise_for_status
      - 返回体不是预期结构 → RuntimeError
    """
    url = f"/api/trpc/{procedure}?batch=1&input={_enc_input(input_obj)}"
    last_err: Any = None
    for attempt in range(1, DEFAULT_MAX_RETRIES + 1):
        try:
            resp = client.get(url)
        except (httpx.ReadTimeout, httpx.ConnectTimeout, httpx.ConnectError,
                httpx.RemoteProtocolError) as e:
            last_err = e
            tqdm.write(f"  [{what or procedure}] net err {attempt}/{DEFAULT_MAX_RETRIES}: "
                       f"{type(e).__name__} {e}")
            _THROTTLE.on_retry(attempt=attempt)
            _THROTTLE.sleep_before_next()
            continue
        if is_auth_dead(resp.status_code):
            raise SessionDead(f"HTTP {resp.status_code} on {procedure}: "
                              f"{resp.text[:200]}")
        if resp.status_code == 429 or 500 <= resp.status_code < 600:
            if resp.status_code == 429:
                SoftCooldown.trigger(_PLATFORM, reason=f"http_429:{procedure}",
                                      minutes=10)
            ra = parse_retry_after(resp.headers.get("Retry-After"))
            tqdm.write(f"  [{what or procedure}] HTTP {resp.status_code} "
                       f"retry {attempt}/{DEFAULT_MAX_RETRIES}"
                       + (f" (Retry-After={ra:.0f}s)" if ra else ""))
            _THROTTLE.on_retry(retry_after_sec=ra, attempt=attempt)
            _THROTTLE.sleep_before_next()
            last_err = f"HTTP {resp.status_code}"
            continue
        if resp.status_code != 200:
            raise RuntimeError(f"[{procedure}] HTTP {resp.status_code}: "
                               f"{resp.text[:200]}")
        # 解析 tRPC batch 响应: 期望 `[{result:{data:{json:{...}}}}]`
        try:
            arr = resp.json()
        except ValueError as e:
            raise RuntimeError(f"[{procedure}] 响应非 JSON: {e}; "
                               f"preview={resp.text[:200]}")
        if not isinstance(arr, list) or not arr:
            raise RuntimeError(f"[{procedure}] 响应不是非空 list: {arr!r}"[:300])
        first = arr[0]
        if "error" in first:
            # tRPC 业务错误也过一遍软警告 (rate limit / quota)
            err = first.get("error") or {}
            err_msg = (err.get("message") or "") if isinstance(err, dict) else str(err)
            reason = detect_soft_warning(200, body={"message": err_msg})
            if reason:
                SoftCooldown.trigger(_PLATFORM, reason=reason, minutes=10)
                _THROTTLE.on_warning()
            raise RuntimeError(f"[{procedure}] tRPC error: "
                               f"{first.get('error')!r}"[:400])
        result = first.get("result") or {}
        data_wrap = result.get("data") or {}
        body_json = data_wrap.get("json") or {}
        # Body 层软警告 (有 hasPermission:False 等业务限流字段时)
        if isinstance(body_json, dict):
            reason = detect_soft_warning(200, body=body_json,
                                          cookies=dict(resp.cookies))
            if reason:
                SoftCooldown.trigger(_PLATFORM, reason=reason, minutes=10)
                _THROTTLE.on_warning()
        return body_json
    raise RuntimeError(f"[{procedure}] 达到最大重试次数 ({DEFAULT_MAX_RETRIES}), "
                       f"最后错误: {last_err}")


# ==================== 列表 / 详情 ====================

# --sweep-today 注入: {category_key: "YYYY-MM-DD"}. 仅对 list_procedure 走
# tRPC date 校验的分类生效 (earnings_report / earnings_transcript). 每次
# build_list_input 时把 ISO Date 塞进 body.customDate 并标记 meta.values.customDate=["Date"].
_SWEEP_CUSTOM_DATE: Dict[str, str] = {}


def build_list_input(cfg: dict, limit: int, cursor: Any = None,
                     direction: str = "forward") -> dict:
    """为 tRPC GET 构造 input={"0":{"json":..., "meta":...}}."""
    body: Dict[str, Any] = {
        "limit": limit,
        "direction": direction,
    }
    body.update(cfg.get("list_json_extra") or {})
    # undefined 标记 (tRPC superjson: null 和 undefined 要区分)
    meta_undef_vals: Dict[str, List[str]] = {
        k: ["undefined"] for k in cfg.get("list_meta_undef") or []
    }
    # --sweep-today: 若当前 category 在 _SWEEP_CUSTOM_DATE 里,注入
    # dateFilter=custom + customDate=<ISO Date>,并取消 customDate 的 undefined 标记,
    # 改加 Date SuperJSON 类型标记。
    sweep_key = cfg.get("sweep_key") or ""
    if sweep_key and sweep_key in _SWEEP_CUSTOM_DATE:
        iso = _SWEEP_CUSTOM_DATE[sweep_key]
        body["dateFilter"] = "custom"
        body["customDate"] = iso
        meta_undef_vals.pop("customDate", None)
        meta_undef_vals["customDate"] = ["Date"]
    if cursor is not None:
        body["cursor"] = cursor
        if cfg["cursor_kind"] == "object":
            # cursor.publishedAt 是 Date
            meta_undef_vals["cursor.publishedAt"] = ["Date"]
    meta: Dict[str, Any] = {"v": 1}
    if meta_undef_vals:
        meta["values"] = meta_undef_vals
    return {"0": {"json": body, "meta": meta}}


def fetch_list_page(client: httpx.Client, cfg: dict, limit: int,
                    cursor: Any = None) -> dict:
    """返回 {items:[], nextCursor, totalCount?, counts?}."""
    inp = build_list_input(cfg, limit=limit, cursor=cursor)
    return trpc_get(client, cfg["list_procedure"], inp, what=cfg["label"])


def build_detail_input(cfg: dict, item: dict) -> Optional[dict]:
    kind = cfg.get("detail_input_kind")
    if kind == "slug_string":
        # 2026-04-24: funda 服务端 zod schema 从 `z.string()` 改到 `z.object({slug})`.
        # 原来发裸字符串 → HTTP 400 "Invalid input: expected object, received string".
        # 症状: 2026-04-19 起 14 条 post 的 detail_result 只有 _err, content_md = 0.
        # 保留 "slug_string" kind 名字 (配置向后兼容), 但载荷形态现在是 object.
        slug = item.get("slug")
        if not slug:
            return None
        return {"0": {"json": {"slug": slug}}}
    if kind == "id_obj":
        rid = item.get("id")
        if not rid:
            return None
        return {"0": {"json": {"id": rid}}}
    return None


def fetch_detail(client: httpx.Client, cfg: dict, item: dict) -> dict:
    """详情 GET. 失败抛 RuntimeError (调用方可捕获放 detail._err)."""
    if not cfg.get("detail_procedure"):
        return {}
    inp = build_detail_input(cfg, item)
    if inp is None:
        return {}
    try:
        return trpc_get(client, cfg["detail_procedure"], inp,
                        what=cfg["label"] + "/detail")
    except SessionDead:
        raise
    except Exception as e:
        return {"_err": str(e)[:300]}


# ==================== 时间 / ID ====================

_ISO_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T")


def parse_iso(s: Any) -> Optional[datetime]:
    if not s:
        return None
    if isinstance(s, datetime):
        return s
    if not isinstance(s, str):
        return None
    try:
        # 2026-04-16T16:16:05.648Z → drop Z, parse
        if s.endswith("Z"):
            s2 = s[:-1]
        else:
            s2 = s
        # fromisoformat handles fractional seconds since 3.11; be safe with split
        if "." in s2:
            head, frac = s2.split(".", 1)
            # truncate fractional to 6 digits
            frac = (frac + "000000")[:6]
            s2 = f"{head}.{frac}"
        return datetime.fromisoformat(s2)
    except (ValueError, TypeError):
        return None


def fmt_publish(dt_or_str: Any) -> str:
    """输出北京时间字符串, 保持和其他平台 (alphapai/jinmen/gangtise/meritco/acecamp) 一致,
    方便回测时按 date 字符串 join. release_time_ms 仍是 UTC epoch (TZ-free, 回测首选)."""
    dt = parse_iso(dt_or_str) if isinstance(dt_or_str, str) else dt_or_str
    if isinstance(dt, datetime):
        from datetime import timezone as _tz, timedelta as _td
        if dt.tzinfo is None:
            # 约定: funda 源 ISO 不带 TZ 时视作 UTC (和 release_time_ms 一致)
            dt = dt.replace(tzinfo=_tz.utc)
        return dt.astimezone(_tz(_td(hours=8))).strftime("%Y-%m-%d %H:%M")
    return str(dt_or_str or "")[:16]


def item_time_iso(item: dict, cfg: dict) -> Optional[str]:
    v = item.get(cfg["time_field"])
    if isinstance(v, str):
        return v
    return None


def item_time_dt(item: dict, cfg: dict) -> Optional[datetime]:
    return parse_iso(item.get(cfg["time_field"]))


def make_dedup_id(item: dict) -> str:
    """funda 所有分类都返回稳定 ID (UUID / cuid), 直接用."""
    rid = item.get("id")
    if not rid:
        raise ValueError(f"item 无 id: {item!r}"[:300])
    return str(rid)


# ==================== Mongo 存储 ====================

def html_to_text(html: str, max_len: int = 2000) -> str:
    """粗暴 strip HTML (只用于 stats 预览; 真 HTML 保留在 detail_result)."""
    if not isinstance(html, str):
        return ""
    text = re.sub(r"<[^>]+>", " ", html)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:max_len]


def build_doc(category_key: str, cfg: dict, item: dict,
              detail: dict) -> dict:
    """把 list_item + detail_result 拼成 MongoDB doc."""
    dedup_id = make_dedup_id(item)
    title = item.get("title") or ""
    time_iso = item_time_iso(item, cfg)
    time_dt = parse_iso(time_iso)
    release_time_ms: Optional[int] = None
    if time_dt:
        release_time_ms = int(time_dt.replace(tzinfo=timezone.utc).timestamp() * 1000) \
            if time_dt.tzinfo is None else int(time_dt.timestamp() * 1000)

    web_url = ""
    try:
        web_url = cfg["web_url_fmt"].format(
            id=item.get("id", ""),
            slug=item.get("slug", ""),
        )
    except KeyError:
        pass

    doc: Dict[str, Any] = {
        "_id": dedup_id,
        "id": dedup_id,
        "category": category_key,
        "title": title,
        "release_time": fmt_publish(time_iso),
        "release_time_ms": release_time_ms,
        "web_url": web_url,
        "list_item": item,
        "detail_result": detail,
        "crawled_at": datetime.now(timezone.utc),
    }

    # 常用字段抽到顶层 (方便 Mongo 查询 / 索引)
    for k in ("slug", "ticker", "year", "period", "industry", "sourceUrl",
              "accessLevel", "coverImageUrls", "excerpt", "subtitle",
              "tags", "entities", "views"):
        v = item.get(k)
        if v not in (None, "", []):
            doc[k] = v

    # 把 detail 里的"正文"抽到 content_md / content_html
    content = ""
    content_html = ""
    if isinstance(detail, dict):
        # post.fetchBySlug 返回 body (markdown-like)
        body = detail.get("body")
        if isinstance(body, str) and body:
            content = body
        # companyEarning.fetchById 返回 content
        c = detail.get("content")
        if isinstance(c, str) and c:
            # 用类型判断是 HTML 还是纯文本
            if detail.get("type") == "EIGHT_K" or c.lstrip().startswith("<"):
                content_html = c
                content = html_to_text(c, max_len=200_000)
            else:
                content = c
        # 其他可能有用的元数据
        for k in ("visibility", "previewBody", "totalComments", "likesCount",
                  "type", "createdAt", "updatedAt", "attachments"):
            v = detail.get(k)
            if v not in (None, "", []):
                doc.setdefault(k, v)

    if content:
        doc["content_md"] = content
    if content_html:
        doc["content_html"] = content_html

    doc["stats"] = {
        "chars": len(content),
        "html_chars": len(content_html),
    }
    return doc


def dump_one(client: httpx.Client, db, category_key: str, cfg: dict,
             item: dict, force: bool = False) -> str:
    """入库单条. 返回 'added' / 'skipped' / 'updated' / 'failed'."""
    col = db[cfg["collection"]]
    dedup_id = make_dedup_id(item)
    if not force:
        if col.find_one({"_id": dedup_id}, {"_id": 1}):
            return "skipped"
    detail = fetch_detail(client, cfg, item)
    _THROTTLE.sleep_before_next()
    doc = build_doc(category_key, cfg, item, detail)
    existed = col.find_one({"_id": dedup_id}, {"_id": 1})
    _stamp_ticker(doc, "funda", col)
    col.replace_one({"_id": dedup_id}, doc, upsert=True)
    return "updated" if existed else "added"


# ==================== Checkpoint ====================

def state_doc_id(category_key: str) -> str:
    return f"crawler_{category_key}"


def load_state(db, category_key: str) -> dict:
    return db[COL_STATE].find_one({"_id": state_doc_id(category_key)}) or {}


def save_state(db, category_key: str, **kwargs) -> None:
    kwargs["updated_at"] = datetime.now(timezone.utc)
    db[COL_STATE].update_one(
        {"_id": state_doc_id(category_key)},
        {"$set": kwargs},
        upsert=True,
    )


# ==================== 翻页 ====================

def iter_list(client: httpx.Client, cfg: dict, page_size: int,
              max_items: Optional[int] = None,
              stop_at_id: Optional[str] = None,
              stop_before_dt: Optional[datetime] = None):
    """generator: 分页产出 list_item. 同时 yield 'top_id' 一次 (第一条) 作为副产物.

    停止条件:
      - max_items 达到
      - 命中 stop_at_id (上次已爬过的顶部) 立即停 (不含)
      - 列表项时间 < stop_before_dt (向前扫到过期) 停
      - nextCursor 为 None
    """
    cursor: Any = None
    yielded = 0
    first_id_yielded: Optional[str] = None
    page = 0
    while True:
        page += 1
        data = fetch_list_page(client, cfg, limit=page_size, cursor=cursor)
        items: List[dict] = data.get("items") or []
        next_cursor = data.get("nextCursor")
        if not items:
            tqdm.write(f"  [page {page}] 空列表, 停")
            return
        new_count = 0
        hit_known = False
        hit_old = False
        for it in items:
            try:
                iid = make_dedup_id(it)
            except ValueError:
                continue
            # 扫完本页再让 dump 逐条 dedup (避免"跳号"新条目被漏抓)
            if stop_at_id and iid == stop_at_id:
                hit_known = True
                continue
            if stop_before_dt is not None:
                dt = item_time_dt(it, cfg)
                if dt is not None:
                    dt_n = dt.replace(tzinfo=None) if dt.tzinfo else dt
                    sbt_n = stop_before_dt.replace(tzinfo=None) \
                        if stop_before_dt.tzinfo else stop_before_dt
                    if dt_n < sbt_n:
                        hit_old = True
                        continue
            if first_id_yielded is None:
                first_id_yielded = iid
            yield it
            yielded += 1
            new_count += 1
            if max_items and yielded >= max_items:
                break
        tqdm.write(f"  [page {page}] +{new_count}/{len(items)} "
                   f"(累计 {yielded}) cursor→{_short(next_cursor)} "
                   f"hit_known={hit_known} hit_old={hit_old}")
        if hit_known or hit_old:
            return
        if max_items and yielded >= max_items:
            return
        if not next_cursor:
            return
        cursor = next_cursor
        _THROTTLE.sleep_before_next()


def _short(cursor: Any) -> str:
    if cursor is None:
        return "None"
    if isinstance(cursor, str):
        return cursor[:12] + ("…" if len(cursor) > 12 else "")
    if isinstance(cursor, dict):
        cid = cursor.get("id", "")
        return f"{{id={str(cid)[:12]}…, publishedAt={cursor.get('publishedAt', '')[:10]}}}"
    return str(cursor)[:30]


# ==================== 一轮抓取 ====================

def run_category(client, db, category_key: str, args) -> dict:
    cfg = CATEGORIES[category_key]
    print(f"\n{'─' * 60}")
    print(f"[{cfg['label']}] collection={cfg['collection']}")
    print(f"{'─' * 60}")

    state = load_state(db, category_key)
    stop_id = state.get("top_id") if args.resume else None
    if args.resume and stop_id:
        print(f"[恢复] 上次 top_id={str(stop_id)[:24]} updated_at={state.get('updated_at')} "
              f"→ 增量到此停")
    elif args.resume:
        print("[恢复] 未找到 checkpoint, 全量爬")

    stop_dt: Optional[datetime] = None
    if getattr(args, "since_hours", None) is not None:
        stop_dt = datetime.now(timezone.utc) - timedelta(hours=args.since_hours)
        print(f"[时间窗] 仅抓 {args.since_hours}h 内 (cutoff={stop_dt:%Y-%m-%d %H:%M} UTC)")

    # 先拉一页探下 top_id (不用再多消耗 request — iter_list 内部复用)
    added = skipped = failed = updated = 0
    new_top_id: Optional[str] = None

    cap = cap_from_args(args)
    pbar: Optional[tqdm] = None

    try:
        for item in iter_list(client, cfg, page_size=args.page_size,
                              max_items=args.max,
                              stop_at_id=stop_id,
                              stop_before_dt=stop_dt):
            if pbar is None:
                # 延迟构造进度条, 让 iter_list 可以打第 1 条日志
                pbar = tqdm(desc=cfg["label"], unit="条", dynamic_ncols=True,
                            bar_format="{l_bar}{bar}| {n_fmt} [{elapsed}<{remaining}] {postfix}")
            if cap.exhausted() or _BUDGET.exhausted():
                tqdm.write(f"  [antibot] daily-cap={cap.max_items} 已满, 本轮停")
                break
            if new_top_id is None:
                try:
                    new_top_id = make_dedup_id(item)
                except ValueError:
                    pass
            title = (item.get("title") or "")[:60]
            t_iso = item_time_iso(item, cfg) or ""
            try:
                status = dump_one(client, db, category_key, cfg, item,
                                  force=args.force)
            except SessionDead:
                raise
            except Exception as e:
                failed += 1
                tqdm.write(f"  ✗ {t_iso[:10]} {title}  ERR: {e}"[:200])
                pbar.update(1)
                continue
            if status == "skipped":
                skipped += 1
                tqdm.write(f"  · {t_iso[:10]} {title}  已存在")
            elif status == "updated":
                updated += 1
                tqdm.write(f"  ↻ {t_iso[:10]} {title}  更新")
                cap.bump(); _BUDGET.bump()
            else:
                added += 1
                tqdm.write(f"  ✓ {t_iso[:10]} {title}")
                cap.bump(); _BUDGET.bump()
            pbar.update(1)
            pbar.set_postfix_str(f"+{added} ↻{updated} ={skipped} ✗{failed}")
            save_state(db, category_key,
                       last_processed_id=make_dedup_id(item),
                       last_processed_at=datetime.now(timezone.utc),
                       in_progress=True)
    finally:
        if pbar is not None:
            pbar.close()

    if new_top_id:
        save_state(db, category_key,
                   top_id=new_top_id,
                   in_progress=False,
                   last_run_end_at=datetime.now(timezone.utc),
                   last_run_stats={"added": added, "updated": updated,
                                   "skipped": skipped, "failed": failed})

    total = db[cfg["collection"]].estimated_document_count()
    print(f"  完成: 新增 {added} / 更新 {updated} / 跳过 {skipped} / 失败 {failed}")
    print(f"  当前 {cfg['collection']} 总数: {total}")
    return {"added": added, "updated": updated, "skipped": skipped, "failed": failed}


def run_once(client, db, args) -> Dict[str, dict]:
    cats = CATEGORY_ORDER if args.category == "all" else [args.category]
    summary: Dict[str, dict] = {}
    for c in cats:
        try:
            summary[c] = run_category(client, db, c, args)
        except SessionDead:
            raise
        except KeyboardInterrupt:
            raise
        except Exception as e:
            tqdm.write(f"\n[{c}] 分类异常: {e}")
            summary[c] = {"added": 0, "updated": 0, "skipped": 0,
                          "failed": -1, "error": str(e)}
    print(f"\n{'═' * 60}")
    print("本轮汇总: " + "  ".join(
        f"{c}+{s.get('added', 0)}/↻{s.get('updated', 0)}/={s.get('skipped', 0)}/✗{s.get('failed', 0)}"
        for c, s in summary.items()
    ))
    print(f"{'═' * 60}")
    return summary


# ==================== 当日统计 ====================

def count_today(client, db, args) -> dict:
    if args.date:
        target = args.date
        day_start = datetime.strptime(args.date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    else:
        day_start = datetime.now(timezone.utc).replace(hour=0, minute=0,
                                                        second=0, microsecond=0)
        target = day_start.strftime("%Y-%m-%d")
    day_end = day_start + timedelta(days=1)
    print(f"\n[统计] 扫描各分类 {target} 平台条数 (UTC)...")

    cats = CATEGORY_ORDER if args.category == "all" else [args.category]
    overall: Dict[str, Any] = {"date": target}

    for c in cats:
        cfg = CATEGORIES[c]
        items_today: List[dict] = []
        # 走 iter_list, 但 stop_before_dt = day_start
        scanned = 0
        for it in iter_list(client, cfg, page_size=args.page_size,
                            max_items=None, stop_at_id=None,
                            stop_before_dt=day_start):
            scanned += 1
            dt = item_time_dt(it, cfg)
            if dt is None:
                continue
            # 统一 aware
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            if day_start <= dt < day_end:
                items_today.append(it)
        ids = [make_dedup_id(it) for it in items_today]
        in_db = db[cfg["collection"]].count_documents({"_id": {"$in": ids}}) if ids else 0
        cat_stat = {
            "platform_count": len(items_today),
            "in_db": in_db,
            "missing": len(items_today) - in_db,
            "scanned": scanned,
        }
        overall[c] = cat_stat
        print(f"  {cfg['label']:>20s}: 平台 {cat_stat['platform_count']:>4d}  "
              f"已入库 {cat_stat['in_db']:>4d}  待入库 {cat_stat['missing']:>4d}  "
              f"(扫描 {scanned} 条)")

    overall["scanned_at"] = datetime.now(timezone.utc)
    db[COL_STATE].replace_one(
        {"_id": f"daily_{target}"},
        {"_id": f"daily_{target}", **overall},
        upsert=True,
    )
    print(f"\n已保存到 {COL_STATE} (_id=daily_{target})")
    return overall


# ==================== account / 元数据 ====================

ACCOUNT_ENDPOINTS = [
    ("user-profile", "user.getUserProfile",
     {"0": {"json": None, "meta": {"values": ["undefined"], "v": 1}}}),
    ("cms-access", "cmsAccess.getCurrentAccess",
     {"0": {"json": None, "meta": {"values": ["undefined"], "v": 1}}}),
    ("scaling-up-config", "appConfig.fetchScalingUpConfig",
     {"0": {"json": None, "meta": {"values": ["undefined"], "v": 1}}}),
    ("post-available-tickers", "post.fetchAvailableTickers",
     {"0": {"json": None, "meta": {"values": ["undefined"], "v": 1}}}),
    ("post-tag-counts", "post.fetchTagCounts",
     {"0": {"json": None, "meta": {"values": ["undefined"], "v": 1}}}),
    ("earning-available-tickers", "companyEarning.fetchAvailableTickers",
     {"0": {"json": {"type": None}, "meta": {"values": {"type": ["undefined"]}, "v": 1}}}),
    ("earning-industries", "companyEarning.fetchAvailableIndustries",
     {"0": {"json": {"type": None}, "meta": {"values": {"type": ["undefined"]}, "v": 1}}}),
]


def dump_account(client, db) -> None:
    print("\n[账户] 抓取账户 / 元数据接口...")
    col = db[COL_ACCOUNT]
    now = datetime.now(timezone.utc)
    for name, procedure, input_obj in ACCOUNT_ENDPOINTS:
        try:
            data = trpc_get(client, procedure, input_obj, what=name)
            ok = True
        except SessionDead:
            raise
        except Exception as e:
            data = {"_err": str(e)[:300]}
            ok = False
        col.replace_one(
            {"_id": name},
            {"_id": name, "procedure": procedure, "response": data,
             "updated_at": now},
            upsert=True,
        )
        tag = "✓" if ok else "✗"
        print(f"  [{tag}] {name}")
        _THROTTLE.sleep_before_next()


# ==================== 情绪因子抓取 (api.funda.ai) ====================

def fetch_sentiment_page(api_key: str, ua: str, date_from: str, date_to: str,
                         page: int = 0, page_size: int = 500,
                         tickers: Optional[List[str]] = None,
                         timeout: float = 30.0) -> dict:
    """GET api.funda.ai/v1/sentiment-scores. Bearer auth.

    Response shape: {code, message, data: {items: [...], next_page, page, page_size, total_count}}.
    - code=0 成功
    - next_page=-1 表示没有下一页
    - items[i]: {id, ticker, date, company, sector, industry,
                 reddit_score, reddit_count, twitter_score, twitter_count,
                 ai_summary, created_at, updated_at}
    """
    params: Dict[str, Any] = {
        "page_size": page_size, "page": page,
        "date_from": date_from, "date_to": date_to,
    }
    if tickers:
        params["tickers"] = ",".join(tickers)
    for attempt in range(1, DEFAULT_MAX_RETRIES + 1):
        try:
            r = httpx.get(
                f"{SENTIMENT_API_BASE}{SENTIMENT_API_PATH}",
                params=params,
                headers={"Authorization": f"Bearer {api_key}",
                         "User-Agent": ua,
                         "Accept": "application/json",
                         "Referer": f"{BASE_URL}/"},
                trust_env=False, timeout=timeout,
            )
        except (httpx.ReadTimeout, httpx.ConnectTimeout, httpx.ConnectError) as e:
            tqdm.write(f"  [sentiment] net err {attempt}/{DEFAULT_MAX_RETRIES}: {e}")
            _THROTTLE.on_retry(attempt=attempt)
            _THROTTLE.sleep_before_next()
            continue
        if is_auth_dead(r.status_code, r.text[:200]):
            raise SessionDead(
                f"HTTP {r.status_code} on sentiment API: {r.text[:200]} "
                "(api_key 过期? 浏览器 Network 重新抓 Authorization 更新 credentials.json)")
        if r.status_code == 429 or 500 <= r.status_code < 600:
            ra = parse_retry_after(r.headers.get("Retry-After"))
            _THROTTLE.on_retry(retry_after_sec=ra, attempt=attempt)
            _THROTTLE.sleep_before_next()
            continue
        if r.status_code != 200:
            raise RuntimeError(f"sentiment HTTP {r.status_code}: {r.text[:200]}")
        return r.json()
    raise RuntimeError(f"sentiment 达到最大重试 ({DEFAULT_MAX_RETRIES})")


def fetch_sentiment_all(api_key: str, ua: str, date_from: str, date_to: str,
                        tickers: Optional[List[str]] = None) -> List[dict]:
    """翻页拉到尽, 合并所有 items."""
    all_items: List[dict] = []
    page = 0
    page_size = 500
    while True:
        resp = fetch_sentiment_page(api_key, ua, date_from, date_to,
                                    page=page, page_size=page_size,
                                    tickers=tickers)
        if resp.get("code") != "0" and resp.get("code") != 0:
            raise RuntimeError(f"sentiment code={resp.get('code')} msg={resp.get('message')}")
        data = resp.get("data") or {}
        items = data.get("items") or []
        all_items.extend(items)
        nxt = data.get("next_page")
        if nxt is None or nxt == -1 or len(items) < page_size:
            break
        page = nxt
        _THROTTLE.sleep_before_next()
    return all_items


def run_sentiment(db, args) -> dict:
    """抓取 date_from~date_to 的情绪因子, upsert 到 funda.sentiments collection.

    _id = f"{ticker}|{date}" (ticker 可能含 ".", Mongo key 允许)
    """
    _, ua, api_key = load_creds_ext()
    if not api_key:
        print("错误: credentials.json 缺少 'api_key'. 浏览器 Network 找 Authorization: Bearer ...")
        sys.exit(2)

    # Funda 是美股数据, "today" 用 US/Eastern — CN 服务器 UTC+8 取 today 会抢先一天.
    _US_ET = timezone(timedelta(hours=-5))   # 粗略 EST (不做 DST 切换,误差 1h 可接受)
    today = datetime.now(_US_ET).strftime("%Y-%m-%d")
    date_to = args.sentiment_date_to or today
    if args.sentiment_days and args.sentiment_days > 0:
        dfrom = (datetime.strptime(date_to, "%Y-%m-%d") -
                 timedelta(days=args.sentiment_days - 1)).strftime("%Y-%m-%d")
    else:
        dfrom = args.sentiment_date_from or date_to

    print(f"\n[情绪因子] {dfrom} ~ {date_to}")
    items = fetch_sentiment_all(api_key, ua, dfrom, date_to)
    print(f"[情绪因子] 平台返回 {len(items)} 条")
    if not items:
        return {"added": 0, "updated": 0, "skipped": 0}

    col = db[COL_SENTIMENTS]
    added = updated = skipped = 0
    now = datetime.now(timezone.utc)
    for it in items:
        ticker = (it.get("ticker") or "").strip()
        date = (it.get("date") or "").strip()
        if not ticker or not date:
            skipped += 1
            continue
        _id = f"{ticker}|{date}"
        doc = {
            "_id": _id,
            "ticker": ticker,
            "date": date,
            "company": it.get("company") or "",
            "sector": it.get("sector") or "",
            "industry": it.get("industry") or "",
            "reddit_score": it.get("reddit_score"),
            "reddit_count": it.get("reddit_count") or 0,
            "twitter_score": it.get("twitter_score"),
            "twitter_count": it.get("twitter_count") or 0,
            "ai_summary": it.get("ai_summary") or "",
            "source_id": it.get("id"),
            "source_created_at": it.get("created_at"),
            "source_updated_at": it.get("updated_at"),
            "crawled_at": now,
        }
        res = col.replace_one({"_id": _id}, doc, upsert=True)
        if res.upserted_id is not None:
            added += 1
        elif res.modified_count > 0:
            updated += 1
        else:
            skipped += 1

    total = col.estimated_document_count()
    print(f"\n本轮: 新增 {added} / 更新 {updated} / 跳过 {skipped}")
    print(f"MongoDB sentiments 总数: {total}")

    # 也把今日运行状态存一条, 便于监控 / --show-state
    db[COL_STATE].replace_one(
        {"_id": "crawler_sentiment"},
        {"_id": "crawler_sentiment",
         "last_date_from": dfrom, "last_date_to": date_to,
         "last_run_end_at": now, "updated_at": now,
         "last_run_stats": {"added": added, "updated": updated, "skipped": skipped,
                             "fetched": len(items)}},
        upsert=True,
    )
    return {"added": added, "updated": updated, "skipped": skipped,
            "fetched": len(items)}


# ==================== CLI ====================

def parse_args():
    p = argparse.ArgumentParser(
        description="funda.ai 多分类爬虫 (MongoDB 存储)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--category", choices=["all", *CATEGORY_ORDER], default="all",
                   help=f"指定分类 (默认 all). 可选: {', '.join(CATEGORY_ORDER)}")
    p.add_argument("--max", type=int, default=None,
                   help="最多爬 N 条 (单分类). 默认翻到 nextCursor=null")
    p.add_argument("--page-size", type=int, default=20,
                   help="每页大小 (默认 20 — 与 funda.ai 前端一致)")
    p.add_argument("--force", action="store_true",
                   help="强制重爬已入库 + 强制刷新 account")
    p.add_argument("--resume", action="store_true",
                   help="增量模式: 遇到上次已爬过的 top_id 即停")
    p.add_argument("--stream-backfill", action="store_true",
                   help="兼容 flag, no-op (funda 未实现流式, 走普通翻页即可)")
    p.add_argument("--watch", action="store_true",
                   help="实时模式: 定时轮询. Ctrl+C 退出")
    p.add_argument("--interval", type=int, default=600,
                   help="实时模式轮询间隔秒数 (默认 600)")
    p.add_argument("--since-hours", type=float, default=None,
                   help="只抓过去 N 小时内内容 (按 time_field)")
    p.add_argument("--show-state", action="store_true",
                   help="打印 checkpoint + 凭证健康检查后退出")
    p.add_argument("--reset-state", action="store_true",
                   help="清除所有 crawler checkpoint 后退出")
    p.add_argument("--today", action="store_true",
                   help="统计今日各分类平台条数对比本地库, 结果存 _state")
    p.add_argument("--date", metavar="YYYY-MM-DD", default=None,
                   help="配合 --today / --sweep-today 指定日期 (默认今天, UTC)")
    p.add_argument("--sweep-today", action="store_true",
                   help="日扫模式 (仅 earnings_report / earnings_transcript): "
                        "注入 dateFilter=custom + customDate=<--date ISO>. "
                        "禁 resume / since-hours 早停.")
    p.add_argument("--auth", default=None,
                   help="覆盖 credentials.json 里的 cookie (整串 document.cookie)")
    p.add_argument("--mongo-uri", default=MONGO_URI_DEFAULT)
    p.add_argument("--mongo-db", default=MONGO_DB_DEFAULT)
    p.add_argument("--clean-posts", action="store_true",
                   help="清空 posts 集合 + crawler_post checkpoint")
    p.add_argument("--clean-earnings-reports", action="store_true",
                   help="清空 earnings_reports 集合 + crawler_earnings_report checkpoint")
    p.add_argument("--clean-earnings-transcripts", action="store_true",
                   help="清空 earnings_transcripts 集合 + crawler_earnings_transcript checkpoint")

    # --- 情绪因子 (api.funda.ai/v1/sentiment-scores) ---
    p.add_argument("--sentiment", action="store_true",
                   help="抓取 funda.ai 情绪因子 (不走 tRPC, 走 api.funda.ai/v1 Bearer API)")
    p.add_argument("--sentiment-date-from", metavar="YYYY-MM-DD", default=None,
                   help="情绪因子起始日 (默认同 date-to)")
    p.add_argument("--sentiment-date-to", metavar="YYYY-MM-DD", default=None,
                   help="情绪因子结束日 (默认今天)")
    p.add_argument("--sentiment-days", type=int, default=None,
                   help="抓 date-to 往前 N 天的情绪 (覆盖 date-from)")
    p.add_argument("--clean-sentiments", action="store_true",
                   help="清空 sentiments 集合 + crawler_sentiment checkpoint")

    # 反爬 (crawl/antibot.py)
    # 2026-04-24 默认从 3.0/2.0 上调到 3.5/2.5, burst_cd 的默认 (30-60s) 保持,
    # daily_cap 500 / acct_budget 2500 由 antibot.py 负责. 实时档通过 crawler_manager
    # 的 _RT 显式传 --throttle-base 1.5 --throttle-jitter 1.0 覆盖, 不受影响.
    # 2026-04-25 default_cap 500→0: 实时档不再数量闸. 实时档通过 crawler_manager
    # _RT 显式传 --throttle-base 1.5 --throttle-jitter 1.0 覆盖节奏, 不受影响.
    add_antibot_args(p, default_base=3.5, default_jitter=2.5,
                     default_burst=40, default_cap=0, platform="funda")
    return p.parse_args()


def connect_mongo(uri: str, dbname: str):
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    try:
        client.admin.command("ping")
    except PyMongoError as e:
        print(f"错误: 无法连接 MongoDB ({uri}): {e}")
        sys.exit(1)
    db = client[dbname]
    for cfg in CATEGORIES.values():
        col = db[cfg["collection"]]
        col.create_index("title")
        col.create_index("release_time")
        col.create_index("release_time_ms")
        col.create_index("ticker")
        col.create_index("category")
        col.create_index("crawled_at")
    # 情绪因子索引
    senti = db[COL_SENTIMENTS]
    senti.create_index("ticker")
    senti.create_index("date")
    senti.create_index([("ticker", 1), ("date", -1)])
    senti.create_index("crawled_at")
    print(f"[Mongo] 已连接 {uri} -> db: {dbname}")
    return db


def show_state(client, db) -> None:
    print("=" * 60)
    print("凭证健康检查")
    print("=" * 60)
    # 调用真实业务接口探测
    try:
        profile = trpc_get(client, "user.getUserProfile",
                           {"0": {"json": None, "meta": {"values": ["undefined"], "v": 1}}},
                           what="user-profile")
        tier = (profile.get("org") or {}).get("tier")
        region = (profile.get("org") or {}).get("region")
        app_mode = profile.get("appMode")
        org_id = profile.get("orgId")
        print(f"  ✓ user.getUserProfile: tier={tier} region={region} "
              f"appMode={app_mode} orgId={org_id}")
    except SessionDead as e:
        print(f"  ✗ 会话已失效: {e}")
        return
    except Exception as e:
        print(f"  ✗ user.getUserProfile 失败: {e}")
    # 探 post.fetchInfinite 一页 (确认数据权限)
    try:
        data = fetch_list_page(client, CATEGORIES["post"], limit=1)
        items = data.get("items") or []
        tot = "?" if not items else str(items[0].get("publishedAt", ""))[:19]
        print(f"  ✓ post.fetchInfinite: 返回 {len(items)} 条, 最新 publishedAt={tot}")
    except SessionDead as e:
        print(f"  ✗ post 列表被 401/403: {e}")
    except Exception as e:
        print(f"  ✗ post.fetchInfinite 失败: {e}")

    print()
    print("Checkpoint")
    print("=" * 60)
    for c in CATEGORY_ORDER:
        s = load_state(db, c)
        if not s:
            print(f"  {c:>22s}: (无)")
            continue
        print(f"  {c:>22s}: top_id={str(s.get('top_id'))[:24]} "
              f"updated={s.get('updated_at')} "
              f"last_run={s.get('last_run_stats')}")
    print()
    print("Collection 总数:")
    for c, cfg in CATEGORIES.items():
        n = db[cfg["collection"]].estimated_document_count()
        print(f"  {c:>22s} ({cfg['collection']}): {n}")


def main():
    args = parse_args()

    # 凭证
    if args.auth:
        cookie, ua = args.auth, DEFAULT_UA
    else:
        cookie, ua = load_creds()

    # throttle + budget
    global _THROTTLE, _BUDGET
    _THROTTLE = throttle_from_args(args, platform="funda")
    sess_token = extract_session_token(cookie)
    # account_id 取 session-token 的 hash (Funda 没有显式 uid 在 cookie 里)
    import hashlib as _hl
    _account_id = "h_" + _hl.md5((sess_token or cookie or "").encode()).hexdigest()[:12]
    _BUDGET = budget_from_args(args, account_id=_account_id, platform="funda")
    log_config_stamp(_THROTTLE, cap=cap_from_args(args), budget=_BUDGET,
                     extra=f"acct={_account_id}")
    if sess_token:
        print(f"[认证] session-token={sess_token[:16]}... UA={ua[:40]}...")

    db = connect_mongo(args.mongo_uri, args.mongo_db)

    client = create_client(cookie, ua)
    try:
        if args.show_state:
            show_state(client, db)
            return

        if args.reset_state:
            n = db[COL_STATE].delete_many(
                {"_id": {"$regex": "^crawler_"}}).deleted_count
            print(f"已清除 {n} 条 crawler checkpoint (daily_* 统计保留)")
            return

        for flag, cat in (("clean_posts", "post"),
                          ("clean_earnings_reports", "earnings_report"),
                          ("clean_earnings_transcripts", "earnings_transcript")):
            if getattr(args, flag, False):
                cfg = CATEGORIES[cat]
                col = db[cfg["collection"]]
                n = col.estimated_document_count()
                col.drop()
                ns = db[COL_STATE].delete_one(
                    {"_id": state_doc_id(cat)}).deleted_count
                print(f"已清除 {cfg['collection']} ({n} 条) + {state_doc_id(cat)} "
                      f"({ns} 条 checkpoint)")
                return

        if args.clean_sentiments:
            n = db[COL_SENTIMENTS].estimated_document_count()
            db[COL_SENTIMENTS].drop()
            ns = db[COL_STATE].delete_one({"_id": "crawler_sentiment"}).deleted_count
            print(f"已清除 {COL_SENTIMENTS} ({n} 条) + crawler_sentiment ({ns} 条)")
            return

        # 情绪因子: 独立的 api.funda.ai/v1 入口, 不经过 tRPC / cookie auth
        if args.sentiment:
            try:
                run_sentiment(db, args)
            except SessionDead as e:
                print(f"\n[错误] {e}")
                sys.exit(2)
            return

        if args.today:
            count_today(client, db, args)
            return

        # --sweep-today: 把 --date 转成 ISO 日期塞进 _SWEEP_CUSTOM_DATE. 仅
        # earnings_report / earnings_transcript 的 cfg 里有 sweep_key, 其他分类
        # build_list_input 会忽略.
        if args.sweep_today:
            if args.category == "all" or args.category not in (
                "earnings_report", "earnings_transcript"
            ):
                print(f"[sweep-today] 仅 --category earnings_report / earnings_transcript "
                      f"支持 (当前 --category={args.category});忽略")
            else:
                date_str = args.date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
                iso = f"{date_str}T00:00:00.000Z"
                _SWEEP_CUSTOM_DATE.clear()
                _SWEEP_CUSTOM_DATE[args.category] = iso
                print(f"[sweep-today] {args.category} dateFilter=custom "
                      f"customDate={iso} — 禁 resume / since-hours 早停")
                args.resume = False
                args.since_hours = None

        # 首次 / 强制刷新元数据
        if db[COL_ACCOUNT].estimated_document_count() == 0 or args.force:
            try:
                dump_account(client, db)
            except SessionDead as e:
                print(f"\n[错误] 会话失效: {e}")
                print("  → 浏览器重登 https://funda.ai, 更新 credentials.json 的 cookie")
                sys.exit(2)

        if args.watch:
            print(f"\n[实时模式] 每 {args.interval}s 轮询. Ctrl+C 退出.")
            round_num = 0
            while True:
                round_num += 1
                print(f"\n{'═' * 60}\n[轮次 {round_num}] "
                      f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n{'═' * 60}")
                try:
                    run_once(client, db, args)
                except SessionDead as e:
                    print(f"\n[错误] 会话失效: {e}")
                    print("  → 浏览器重登 https://funda.ai, 更新 credentials.json 的 cookie")
                    sys.exit(2)
                except KeyboardInterrupt:
                    print("\n[实时模式] Ctrl+C 退出")
                    break
                except Exception as e:
                    print(f"[轮次 {round_num}] 异常: {e}")
                _THROTTLE.reset()
                try:
                    time.sleep(args.interval)
                except KeyboardInterrupt:
                    print("\n[实时模式] Ctrl+C 退出")
                    break
        else:
            try:
                run_once(client, db, args)
            except SessionDead as e:
                print(f"\n[错误] 会话失效: {e}")
                print("  → 浏览器重登 https://funda.ai, 更新 credentials.json 的 cookie")
                sys.exit(2)
    finally:
        client.close()


if __name__ == "__main__":
    main()
