#!/usr/bin/env python3
"""
forum.thirdbridge.com (Third Bridge Forum) 爬虫 — 专家访谈 + 逐字稿 → MongoDB.

使用方法:
  1. 浏览器登录 https://forum.thirdbridge.com
  2. F12 → Application → Cookies → 把所有 cookie 拼成 "k1=v1; k2=v2" 形式
     (或 Console 里 `document.cookie` 直接复制整行)
  3. 粘到 credentials.json 的 "cookie" 字段
     (credentials.json 已 gitignore, 不会泄漏)
  4. python3 scraper.py --max 100             # 首次入库 100 条
  5. python3 scraper.py --watch --resume      # 实时增量轮询

数据存储:
  - MongoDB (默认 mongodb://localhost:27017, db=thirdbridge)
  - `interviews`: 每个 uuid 一条
  - `account`: 账户信息 (liyuhan@yaojingriver.com)
  - `_state`: checkpoint / daily 统计

共享 crawl/ 爬虫约定 (见 crawl/README.md 第 4 节).
"""

from __future__ import annotations

import argparse
import json
import os
import random
import re
import sys
import time
from collections import Counter
from datetime import datetime, timezone, timedelta
from pathlib import Path

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
)
from ticker_tag import stamp as _stamp_ticker  # noqa: E402


# ==================== 可调常量 ====================

BASE_URL = "https://forum.thirdbridge.com"
SEARCH_API = "/api/interview/search"
DETAIL_API_TMPL = "/api/interview/{lang}/{uuid}"
ACCOUNT_API = "/api/client-users/account-management"
FEATURE_API = "/api/feature-manager"
COMMENTARY_API = "/api/expert-commentary/specialist-commentary-api/v1/comment-data/by-interview-uuids"
FILTERS_API = "/api/interview/filters"

# 主要业务头 — Content-Type / Accept 基本固定
DEFAULT_HEADERS_BASE = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Content-Type": "application/json",
    "Referer": "https://forum.thirdbridge.com/zh/home/all",
    "Origin": "https://forum.thirdbridge.com",
}

DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36"
)

CREDS_FILE = Path(__file__).resolve().parent / "credentials.json"

# MongoDB
MONGO_URI_DEFAULT = os.environ.get(
    "MONGO_URI",
    "mongodb://127.0.0.1:27018/",
)
MONGO_DB_DEFAULT = os.environ.get("MONGO_DB", "third-bridge")
COL_INTERVIEWS = "interviews"
COL_ACCOUNT = "account"
COL_STATE = "_state"

STATE_CHECKPOINT_ID = "crawler_interviews"  # _state checkpoint key

DEFAULT_TIMEOUT = 30.0
DEFAULT_MAX_RETRIES = 5

# 模块级 throttle — main() 用 CLI 参数覆盖后再放回来.
# Third Bridge 反爬最严 (4⭐, AWS WAF + Cognito), 默认更保守.
_THROTTLE: AdaptiveThrottle = AdaptiveThrottle(base_delay=4.0, jitter=3.0,
                                                burst_size=30,
                                                burst_cooldown_min=45,
                                                burst_cooldown_max=90,
                                                platform="thirdbridge")
_BUDGET: AccountBudget = AccountBudget("thirdbridge", "default", 0)
_PLATFORM = "thirdbridge"


# ==================== 异常 ====================

# AuthExpired 保持兼容 — antibot.SessionDead 语义相同, AuthExpired 是 SessionDead 的别名
class AuthExpired(SessionDead):
    """Cookie 过期 / 会话失效. 调用方应提示用户重登并更新 credentials.json."""


# ==================== 凭证 ====================

def load_creds_from_file() -> tuple[str, str]:
    """从 credentials.json 读取 cookie + UA. 返回 (cookie, user_agent)."""
    if not CREDS_FILE.exists():
        print(f"错误: 未找到 {CREDS_FILE}. 请创建并写入 cookie (见脚本顶部注释).")
        sys.exit(1)
    try:
        data = json.loads(CREDS_FILE.read_text())
    except json.JSONDecodeError as e:
        print(f"错误: credentials.json 解析失败: {e}")
        sys.exit(1)
    cookie = (data.get("cookie") or "").strip()
    ua = (data.get("user_agent") or "").strip() or DEFAULT_UA
    if not cookie:
        print("错误: credentials.json 缺少 'cookie' 字段")
        sys.exit(1)
    return cookie, ua


def parse_cookies(cookie_str: str) -> dict:
    """把 "k1=v1; k2=v2" 形式切成 dict (httpx.Client 支持)."""
    out: dict[str, str] = {}
    for part in cookie_str.split(";"):
        part = part.strip()
        if not part or "=" not in part:
            continue
        k, v = part.split("=", 1)
        k = k.strip()
        v = v.strip()
        # `set-cookie` 和 空 key 混在原始 document.cookie 里会捣乱, 跳过
        if not k or k.lower() == "set-cookie":
            continue
        out[k] = v
    return out


# ==================== HTTP 客户端 ====================

def create_client(cookie: str, user_agent: str,
                  timeout: float = DEFAULT_TIMEOUT) -> httpx.Client:
    """trust_env=False 绕系统代理; cookie jar 用 dict 注入.

    **故意不调 antibot.warmup_session**:AWS Cognito cookie jar 极度敏感,
    warmup 一次额外 GET 可能触发 forum Cognito challenge 或覆盖带认证的 cookie.
    third_bridge 的 --interval 1800s 已经足够稀疏, 不靠 warmup 模拟浏览器启动.
    """
    headers = headers_for_platform("thirdbridge")
    if user_agent:
        headers["User-Agent"] = user_agent
    headers["Content-Type"] = "application/json"
    headers["Referer"] = "https://forum.thirdbridge.com/zh/home/all"
    return httpx.Client(
        base_url=BASE_URL,
        cookies=parse_cookies(cookie),
        headers=headers,
        trust_env=False,
        timeout=timeout,
        follow_redirects=True,
    )


def _raise_auth_or_http(resp: httpx.Response) -> None:
    """鉴权失败 → AuthExpired; 其他 4xx/5xx → raise_for_status."""
    if resp.status_code in (401, 403):
        raise AuthExpired(f"HTTP {resp.status_code}: {resp.text[:200]}")
    if resp.status_code == 302:
        # 被 WAF / Cognito 重定向到登录页 = 会话过期
        loc = resp.headers.get("location", "")
        if "login" in loc.lower() or "sign-in" in loc.lower():
            raise AuthExpired(f"redirected to login: {loc}")
    resp.raise_for_status()


def _retry_request(fn, *, what: str, max_retries: int = DEFAULT_MAX_RETRIES):
    """对 429 / 5xx / ReadTimeout 指数退避重试, 用 AdaptiveThrottle 控制节奏.

    401/403 (会话失效) 直接抛 AuthExpired, 不重试.
    """
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            resp = fn()
        except (httpx.ReadTimeout, httpx.ConnectTimeout, httpx.ConnectError) as e:
            last_err = e
            tqdm.write(f"  [{what}] network err attempt {attempt}/{max_retries}: {type(e).__name__}")
            _THROTTLE.on_retry(attempt=attempt)
            _THROTTLE.sleep_before_next()
            continue
        if is_auth_dead(resp.status_code, resp.text[:200]):
            raise AuthExpired(f"HTTP {resp.status_code}: {resp.text[:200]}")
        if resp.status_code == 429 or 500 <= resp.status_code < 600:
            if resp.status_code == 429:
                # ThirdBridge 走 AWS WAF, 429 一旦出 → 全平台 10min 静默
                SoftCooldown.trigger(_PLATFORM, reason=f"http_429:{what}",
                                      minutes=10)
            ra = parse_retry_after(resp.headers.get("Retry-After"))
            tqdm.write(f"  [{what}] HTTP {resp.status_code}, retry {attempt}/{max_retries}"
                       + (f" (Retry-After={ra:.0f}s)" if ra else ""))
            _THROTTLE.on_retry(retry_after_sec=ra, attempt=attempt)
            _THROTTLE.sleep_before_next()
            last_err = f"HTTP {resp.status_code}"
            continue
        # 业务层软警告 (captcha cookie / WAF challenge keyword)
        try:
            body = resp.json() if "json" in (resp.headers.get("content-type") or "") else None
        except Exception:
            body = None
        reason = detect_soft_warning(resp.status_code, body=body if isinstance(body, dict) else None,
                                      text_preview=resp.text[:400] if resp.text else "",
                                      cookies=dict(resp.cookies))
        if reason:
            SoftCooldown.trigger(_PLATFORM, reason=reason, minutes=10)
            _THROTTLE.on_warning()
        return resp
    raise RuntimeError(f"[{what}] 达到最大重试次数 ({max_retries}), 最后错误: {last_err}")


# ==================== 列表 / 详情 / 账户 ====================

def fetch_list(client: httpx.Client, page_from: int = 0, page_size: int = 32,
               lang: str = "zh") -> dict:
    """搜索 API: POST /api/interview/search.

    Body 完全对应前端搜索组件:
      - pageFrom: 偏移 (0-based), pageSize: 每页大小
      - sortBy: {field: "startAt", order: "desc"} — 按开始时间倒序
      - filters: [] / groups: [] / showNeuralSearch: False — 搜全部

    Response: {count: <total>, results: [...], tags: [...], request: {...}, requestId, ...}
    count 是"平台总数", results 是当页数据.
    """
    body = {
        "lang": lang,
        "groups": [],
        "sortBy": {"field": "startAt", "order": "desc"},
        "showNeuralSearch": False,
        "filters": [],
        "pageSize": page_size,
        "pageFrom": page_from,
    }
    resp = _retry_request(
        lambda: client.post(SEARCH_API, json=body),
        what="list",
    )
    _raise_auth_or_http(resp)
    return resp.json()


def fetch_detail(client: httpx.Client, uuid: str, lang: str = "zh",
                 with_transcript: bool = True) -> dict:
    """详情 API: GET /api/interview/<lang>/<uuid>?withTranscript=true.

    返回完整 interview 对象, 包括 transcript (逐字稿) + agenda + specialists 等.
    """
    path = DETAIL_API_TMPL.format(lang=lang, uuid=uuid)
    resp = _retry_request(
        lambda: client.get(path, params={"source": "", "withTranscript": str(with_transcript).lower()}),
        what=f"detail:{uuid[:8]}",
    )
    _raise_auth_or_http(resp)
    return resp.json()


def fetch_commentary(client: httpx.Client, uuids: list[str]) -> dict:
    """专家点评 (可选, 按 uuid 批量查).

    返回 shape 通常是 ``{<uuid>: [<commentary item>, ...], ...}`` 或 ``{"data": {...}}``.
    某些访谈会 500 / 1002 (无权限), 静默吞掉返回空 dict.
    """
    if not uuids:
        return {}
    try:
        resp = client.post(COMMENTARY_API, json={"interviewUuids": uuids})
        if resp.status_code in (401, 403):
            raise AuthExpired(f"commentary {resp.status_code}")
        if resp.status_code != 200:
            return {}
        return resp.json() or {}
    except (httpx.ReadTimeout, httpx.ConnectTimeout, httpx.ConnectError):
        return {}


def fetch_filters(client: httpx.Client) -> dict:
    """拉取平台的筛选项字典 (行业 / 主题 / 地域 / 专家类型 / 内容类型 ...).

    GET /api/interview/filters —— 前端搜索组件用于渲染多选侧栏。
    一次拉回整张 taxonomy 图，入库到 `account` collection 作为元数据。

    ⚠ 观测：部分账号层级 / cookie 会让这个 endpoint **hang** (无响应直到超时)。
    先尝试 GET, 再 fallback 到 POST + search-like body; 两者都超时就返回 {}.
    用短超时 (8s) 避免卡住 account 流程.
    """
    short_timeout = 8.0
    # 1) GET
    try:
        resp = client.get(FILTERS_API, timeout=short_timeout)
        if resp.status_code in (401, 403):
            raise AuthExpired(f"filters {resp.status_code}")
        if resp.status_code == 200:
            try:
                data = resp.json()
                if data:
                    return data
            except Exception:
                pass
    except (httpx.ReadTimeout, httpx.ConnectTimeout, httpx.ConnectError):
        pass

    # 2) POST with search-like body (some tenants require this shape)
    try:
        body = {
            "lang": "zh",
            "groups": [],
            "filters": [],
            "pageSize": 32,
            "pageFrom": 0,
            "sortBy": {"field": "startAt", "order": "desc"},
            "showNeuralSearch": False,
        }
        resp = client.post(FILTERS_API, json=body, timeout=short_timeout)
        if resp.status_code in (401, 403):
            raise AuthExpired(f"filters {resp.status_code}")
        if resp.status_code == 200:
            try:
                return resp.json() or {}
            except Exception:
                return {}
    except (httpx.ReadTimeout, httpx.ConnectTimeout, httpx.ConnectError):
        pass

    return {}


def _format_commentary_for_uuid(commentary_payload: dict, uuid: str) -> tuple[list, str]:
    """Extract commentary items for one uuid and format a markdown blob.

    The API shape has varied across versions — we accept a few layouts:
      * ``{<uuid>: [items…]}``
      * ``{"data": {<uuid>: [items…]}}``
      * ``{"commentaryByInterview": {<uuid>: [items…]}}``

    Returns (raw_items, markdown_text). Empty if nothing found for that uuid.
    """
    if not isinstance(commentary_payload, dict) or not commentary_payload:
        return [], ""

    candidates: list = []
    for key in (uuid,):
        v = commentary_payload.get(key)
        if isinstance(v, list):
            candidates = v
            break
    if not candidates:
        for wrapper in ("data", "commentaryByInterview", "result"):
            inner = commentary_payload.get(wrapper)
            if isinstance(inner, dict):
                v = inner.get(uuid)
                if isinstance(v, list):
                    candidates = v
                    break

    if not candidates:
        return [], ""

    lines: list[str] = []
    for idx, it in enumerate(candidates, 1):
        if not isinstance(it, dict):
            continue
        # Best-effort extraction — field names vary between tenants.
        expert_name = (
            it.get("specialistName")
            or it.get("expertName")
            or (it.get("specialist") or {}).get("name")
            or ""
        )
        title = it.get("title") or it.get("questionTitle") or it.get("heading") or ""
        body = (
            it.get("content")
            or it.get("commentary")
            or it.get("answer")
            or it.get("text")
            or ""
        )
        ts = it.get("createdAt") or it.get("updatedAt") or ""
        header_bits = [f"#{idx}"]
        if expert_name:
            header_bits.append(expert_name)
        if ts:
            header_bits.append(str(ts)[:10])
        header = " · ".join(header_bits)
        lines.append(f"### {header}")
        if title:
            lines.append(f"**{title}**")
        if body:
            lines.append(str(body).strip())
        lines.append("")
    return candidates, "\n".join(lines).strip()


def fetch_account(client: httpx.Client) -> dict:
    resp = client.get(ACCOUNT_API)
    _raise_auth_or_http(resp)
    return resp.json()


def check_token(client: httpx.Client) -> dict:
    """轻量健康检查 — 只拉 account 信息."""
    try:
        info = fetch_account(client)
    except AuthExpired as e:
        return {"ok": False, "error": f"AuthExpired: {e}"}
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}
    return {
        "ok": True,
        "uuid": info.get("uuid"),
        "email": info.get("email"),
        "company": info.get("companyName"),
        "forum_status": info.get("forumStatus"),
        "has_ai_search": info.get("hasAiSearchAccess") or info.get("isActive"),
    }


# ==================== 文本格式化 ====================

_WS_RE = re.compile(r"\s+", re.UNICODE)


def _squash(s: str) -> str:
    return _WS_RE.sub(" ", (s or "").strip())


def _ts_to_local(iso: str) -> tuple[str, int]:
    """ISO UTC → (本地可读 'YYYY-MM-DD HH:MM', 毫秒时间戳).

    例: '2026-04-17T09:00:00.000Z' → ('2026-04-17 17:00', 1776399600000)
        (假定本机 Asia/Shanghai, 但不强行转, 直接用 datetime)
    """
    if not iso:
        return "", 0
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
    except ValueError:
        return iso, 0
    ms = int(dt.timestamp() * 1000)
    local = dt.astimezone().strftime("%Y-%m-%d %H:%M")
    return local, ms


def format_agenda(agenda: list) -> str:
    """访谈议程 → '- a\n- b\n- c'."""
    if not agenda:
        return ""
    return "\n".join(f"- {_squash(str(a))}" for a in agenda if a)


def format_transcript(items: list) -> str:
    """transcript: [{timestamp, discussionItem:[{id, content, ...}]}, ...] → markdown.

    格式 (每轮):
      [00:01:35]
      <content>
      ---
    """
    if not items:
        return ""
    out: list[str] = []
    for block in items:
        ts = _squash(str(block.get("timestamp") or ""))
        dis = block.get("discussionItem") or []
        texts = []
        for d in dis:
            c = (d.get("content") or "").strip()
            if c:
                texts.append(c)
        if not texts:
            continue
        head = ts if ts else ""
        body = "\n\n".join(texts)
        out.append((head + "\n" + body).strip() if head else body)
    return "\n\n".join(out)


def format_specialists(specs: list) -> str:
    """specialists → '<title>\nfirstName lastName (type)' 多行."""
    if not specs:
        return ""
    lines = []
    for s in specs:
        name = _squash(f"{s.get('firstName') or ''} {s.get('lastName') or ''}")
        title = _squash(s.get("title") or "")
        stype = ",".join(s.get("specialistType") or [])
        line = name
        if title:
            line += f" — {title}"
        if stype:
            line += f" ({stype})"
        if line.strip():
            lines.append(line)
    return "\n".join(lines)


def format_company_list(items: list) -> list:
    """targetCompanies / relevantCompanies → 简化后的结构."""
    out = []
    for c in items or []:
        country = c.get("countryOfDomicile") or {}
        country_label = country.get("label") or {}
        sector = c.get("sector") or {}
        out.append({
            "id": c.get("id"),
            "label": c.get("label"),
            "ticker": c.get("ticker"),
            "public": c.get("public"),
            "country": country_label.get("zho") or country_label.get("eng") or "",
            "sector": sector.get("label") or "",
        })
    return out


# ==================== 入库 ====================

def _pick_labels(items: list) -> list:
    return [it.get("label") for it in items or [] if isinstance(it, dict) and it.get("label")]


def dump_one(client: httpx.Client, db, item: dict, lang: str,
             force: bool = False) -> dict:
    """抓一条 interview, 写入 interviews collection. _id = uuid."""
    uuid = item.get("uuid")
    if not uuid:
        return {"状态": "跳过(无uuid)", "标题": ""}
    col = db[COL_INTERVIEWS]
    title = item.get("title") or ""

    if not force:
        existing = col.find_one({"_id": uuid}, {"_id": 1, "stats": 1})
        if existing:
            return {"状态": "已跳过", "标题": title,
                    **(existing.get("stats") or {"转录字数": 0, "转录段数": 0, "专家数": 0})}

    detail = fetch_detail(client, uuid, lang=lang, with_transcript=True)

    release_time, release_ms = _ts_to_local(detail.get("start") or item.get("start") or "")
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    is_future = bool(release_ms and release_ms > now_ms)

    transcript_items = detail.get("transcript") or []
    transcript_md = format_transcript(transcript_items)
    introduction_items = detail.get("introduction") or []
    introduction_md = format_transcript(introduction_items)

    specialists = detail.get("specialists") or []
    moderators = detail.get("moderators") or []
    target_companies = format_company_list(detail.get("targetCompanies"))
    relevant_companies = format_company_list(detail.get("relevantCompanies"))

    agenda = detail.get("agenda") or []
    agenda_md = format_agenda(agenda)
    specialists_md = format_specialists(specialists)

    lang_obj = detail.get("language") or {}
    ctype_obj = detail.get("contentType") or {}

    transcripts_langs = sorted(
        k for k, v in (detail.get("transcripts") or {}).items() if v
    )
    pdf_langs = sorted(
        k for k, v in (detail.get("pdfAvailableLanguages") or {}).items() if v
    )

    # 专家点评 —— 仅在 hasCommentary=True 时按 uuid 拉; 失败静默
    commentary_items: list = []
    commentary_md: str = ""
    if detail.get("hasCommentary"):
        payload = fetch_commentary(client, [uuid])
        commentary_items, commentary_md = _format_commentary_for_uuid(payload, uuid)

    doc = {
        "_id": uuid,
        "uuid": uuid,
        "title": title,
        "release_time": release_time,
        "release_time_ms": release_ms,
        "web_url": f"https://forum.thirdbridge.com/zh/interview/{uuid}",
        "status": detail.get("status"),
        "is_future": is_future,
        "language_id": lang_obj.get("id"),
        "language_label": lang_obj.get("label"),
        "content_type_id": ctype_obj.get("id"),
        "content_type_label": ctype_obj.get("label"),
        "agenda": agenda,
        "agenda_md": agenda_md,
        "target_companies": target_companies,
        "relevant_companies": relevant_companies,
        "specialists": specialists,
        "specialists_md": specialists_md,
        "moderators": moderators,
        "researcher_email": (detail.get("researcher") or {}).get("email"),
        "themes": _pick_labels(detail.get("themes")),
        "sectors": _pick_labels(detail.get("sectors")),
        "geographies": _pick_labels(detail.get("geographies")),
        "transcripts_available": transcripts_langs,
        "pdf_available": pdf_langs,
        "audio": bool(detail.get("audio")),
        "has_commentary": bool(detail.get("hasCommentary")),
        "expert_commentary_count": detail.get("expertCommentaryCount") or 0,

        # 可读文本
        "transcript_md": transcript_md,
        "introduction_md": introduction_md,
        "transcript_items": transcript_items,
        "introduction_items": introduction_items,
        "commentary_items": commentary_items,
        "commentary_md": commentary_md,

        # 原始
        "list_item": item,
        "detail_result": detail,
        "entitlements": detail.get("entitlements"),
        "rules": detail.get("rules"),

        "stats": {
            "转录段数": len(transcript_items),
            "转录字数": len(transcript_md),
            "议程条数": len(agenda),
            "专家数": len(specialists),
            "目标公司": len(target_companies),
            "相关公司": len(relevant_companies),
            "点评条数": len(commentary_items),
            "点评字数": len(commentary_md),
        },
        "crawled_at": datetime.now(timezone.utc),
    }
    # Truncated guard: real body (transcript / intro / commentary) all empty
    # → 付费墙锁住 / 实录还没出 / 该账号无 transcripts entitlement.
    # 议程 + 专家列表是会前 metadata, 不算正文 — 单凭它们入库会污染 kb_search +
    # StockHub (LLM 拿到只有 agenda 的 stub 没法答问题). 跳过不入库, 让下次再
    # 撞同 uuid 时重新走 detail (cookie 在 + 实录上线就能正式入库).
    if not (
        (transcript_md and transcript_md.strip())
        or (introduction_md and introduction_md.strip())
        or (commentary_md and commentary_md.strip())
    ):
        return {"状态": "跳过-空正文", "标题": title, **doc["stats"]}
    _stamp_ticker(doc, "thirdbridge", col)
    col.replace_one({"_id": uuid}, doc, upsert=True)
    return {"状态": "重爬" if force else "新增", "标题": title, **doc["stats"]}


# ==================== 翻页 / 增量 ====================

def _item_start_ms(it: dict) -> int | None:
    _, ms = _ts_to_local(it.get("start") or "")
    return ms if ms else None


def fetch_items_paginated(client: httpx.Client, max_items: int | None = None,
                           page_size: int = 32, lang: str = "zh",
                           stop_at_uuid: str | None = None,
                           stop_before_ms: int | None = None,
                           skip_future: bool = True) -> list:
    """分页翻列表. API 用 pageFrom 偏移 (不是 page number).

    stop_at_uuid:   遇到即停 (增量锚点, --resume). **只对 past 访谈判定**, future 访谈
                    先跳过再继续翻, 因为 top_uuid 锚点永远记录的是最近一条 past 访谈.
    stop_before_ms: start < 该毫秒戳则停 (--since-hours).
    skip_future:    True (默认) → start > 当前时间的访谈 (未排期完成) 直接跳过, 不计入
                    max / 不入库. 高临 startAt desc 排列前面全是已预约但还没发生的,
                    transcript 都是空的, 入库也没用.
    """
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    all_items: list = []
    page_from = 0
    seen_uuids: set[str] = set()
    skipped_future = 0
    while True:
        try:
            resp = fetch_list(client, page_from=page_from, page_size=page_size, lang=lang)
        except Exception as e:
            tqdm.write(f"  [offset {page_from}] 列表失败: {e}")
            break
        results = resp.get("results") or []
        total = resp.get("count") or 0

        if not results:
            break

        hit_known = hit_old = False
        new_n = 0
        page_future = 0
        for it in results:
            uid = it.get("uuid")
            if not uid or uid in seen_uuids:
                continue
            seen_uuids.add(uid)
            # 跳过未来访谈 (未完成, 还没 transcript)
            ms = _item_start_ms(it)
            if skip_future and ms is not None and ms > now_ms:
                page_future += 1
                skipped_future += 1
                continue
            # 只对 past 访谈应用 resume / since 停止条件.
            # 列表不保证严格时间降序, hit_known/hit_old 扫完本页再用作翻页提示
            if stop_at_uuid and uid == stop_at_uuid:
                hit_known = True
                continue
            if stop_before_ms is not None and ms is not None and ms < stop_before_ms:
                hit_old = True
                continue
            all_items.append(it)
            new_n += 1
            if max_items and len(all_items) >= max_items:
                tqdm.write(f"  [offset {page_from}] 达到 max={max_items}, 停. (本批跳过 future {skipped_future})")
                return all_items[:max_items]

        tqdm.write(f"  [offset {page_from}] +{new_n} (累计 {len(all_items)}/{total}) "
                   f"future跳过{page_future}/本次{skipped_future}  "
                   f"hit_known={hit_known} hit_old={hit_old}")
        if hit_known or hit_old:
            break
        if len(results) < page_size:
            break
        page_from += page_size
        if total and page_from >= total:
            break
        _throttle()
    if skipped_future:
        tqdm.write(f"  (合计跳过未来访谈 {skipped_future} 条; --include-future 可保留)")
    return all_items


def _throttle() -> None:
    """Back-compat shim; real pacing 走模块级 _THROTTLE."""
    _THROTTLE.sleep_before_next()


# ==================== 账户 / 状态 / 统计 ====================

def dump_account(client: httpx.Client, db) -> None:
    print("[账户] 抓取账户级接口...")
    col = db[COL_ACCOUNT]
    now = datetime.now(timezone.utc)
    for name, fn in (
        ("account-management", lambda: fetch_account(client)),
        ("feature-manager", lambda: _safe_get(client, FEATURE_API)),
        ("filters", lambda: fetch_filters(client)),
    ):
        try:
            resp = fn()
            col.replace_one({"_id": name},
                            {"_id": name, "endpoint": name, "response": resp, "updated_at": now},
                            upsert=True)
            tag = "✓"
        except AuthExpired:
            raise
        except Exception as e:
            tag = f"ERR {type(e).__name__}"
        print(f"  [{tag}] {name}")


def _safe_get(client: httpx.Client, path: str, params: dict | None = None) -> dict:
    resp = client.get(path, params=params or {})
    _raise_auth_or_http(resp)
    try:
        return resp.json()
    except json.JSONDecodeError:
        return {"_raw": resp.text[:2000]}


def load_state(db) -> dict:
    return db[COL_STATE].find_one({"_id": STATE_CHECKPOINT_ID}) or {}


def save_state(db, **kwargs) -> None:
    kwargs["updated_at"] = datetime.now(timezone.utc)
    db[COL_STATE].update_one({"_id": STATE_CHECKPOINT_ID}, {"$set": kwargs}, upsert=True)


_BJ_TZ = timezone(timedelta(hours=8))


def count_today(client: httpx.Client, db, date_str: str | None = None,
                save_to_db: bool = True, lang: str = "zh") -> dict:
    """统计平台某天发布了多少访谈 (按 start 毫秒戳, 列表倒序, 翻到前一天即停).
    Asia/Shanghai 日历日对齐 — 服务器 TZ 不同也不会错位 8 小时."""
    if date_str:
        day = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=_BJ_TZ)
    else:
        day = datetime.now(_BJ_TZ)
    day_start = day.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end = day_start.replace(hour=23, minute=59, second=59, microsecond=999000)
    start_ms = int(day_start.timestamp() * 1000)
    end_ms = int(day_end.timestamp() * 1000)
    target = day_start.strftime("%Y-%m-%d")

    print(f"[统计] 扫描平台 {target} 的访谈 (lang={lang})...")
    items_today: list = []
    page_from = 0
    page_size = 40
    pages_scanned = 0
    while True:
        try:
            resp = fetch_list(client, page_from=page_from, page_size=page_size, lang=lang)
        except Exception as e:
            print(f"  [offset {page_from}] 失败: {e}")
            break
        results = resp.get("results") or []
        total = resp.get("count") or 0
        pages_scanned += 1
        if not results:
            break
        stop = False
        for it in results:
            ms = _item_start_ms(it)
            if ms is None:
                continue
            if ms < start_ms:
                stop = True
                break
            if ms <= end_ms:
                items_today.append(it)
        print(f"  [offset {page_from}] 扫 {len(results)} 条, 今日累计 {len(items_today)}, stop={stop}")
        if stop:
            break
        if len(results) < page_size:
            break
        page_from += page_size
        if total and page_from >= total:
            break
        _throttle()

    today_uuids = [it.get("uuid") for it in items_today if it.get("uuid")]
    in_db = db[COL_INTERVIEWS].count_documents({"_id": {"$in": today_uuids}}) if today_uuids else 0

    type_count = Counter()
    sector_count = Counter()
    company_count = Counter()
    for it in items_today:
        ctype = (it.get("contentType") or {}).get("label") or "未知"
        type_count[ctype] += 1
        for s in it.get("sectors") or []:
            sector_count[s.get("label") or "未知"] += 1
        for c in it.get("targetCompanies") or []:
            company_count[c.get("label") or "未知"] += 1

    stats = {
        "date": target,
        "total_on_platform": len(items_today),
        "in_db": in_db,
        "not_in_db": len(items_today) - in_db,
        "by_content_type": dict(type_count),
        "by_sector_top10": sector_count.most_common(10),
        "by_target_company_top10": company_count.most_common(10),
        "pages_scanned": pages_scanned,
        "scanned_at": datetime.now(timezone.utc),
    }

    print(f"\n{'=' * 55}")
    print(f"📅 {target} Third Bridge 访谈统计")
    print(f"{'=' * 55}")
    print(f"  平台总数:      {stats['total_on_platform']}")
    print(f"  本地已入库:    {stats['in_db']}")
    print(f"  待入库:        {stats['not_in_db']}")
    print("\n  按内容类型:")
    for t, n in sorted(stats["by_content_type"].items(), key=lambda x: -x[1]):
        print(f"    {str(t)[:30].ljust(30)}  {n}")
    print("\n  按行业 Top10:")
    for s, n in stats["by_sector_top10"]:
        print(f"    {str(s)[:30].ljust(30)}  {n}")
    print("\n  按目标公司 Top10:")
    for c, n in stats["by_target_company_top10"]:
        print(f"    {str(c)[:30].ljust(30)}  {n}")
    print(f"{'=' * 55}\n")

    if save_to_db:
        doc = {**stats, "_id": f"daily_{target}"}
        doc["by_sector_top10"] = [[s, n] for s, n in stats["by_sector_top10"]]
        doc["by_target_company_top10"] = [[c, n] for c, n in stats["by_target_company_top10"]]
        db[COL_STATE].replace_one({"_id": doc["_id"]}, doc, upsert=True)
        print(f"已保存到 {COL_STATE} collection (_id={doc['_id']})\n")
    return stats


# ==================== 主循环 ====================

def run_once(client: httpx.Client, db, args) -> dict:
    # 账户级接口 (首次或 --force)
    if db[COL_ACCOUNT].estimated_document_count() == 0 or args.force:
        dump_account(client, db)
    else:
        print("[账户] 已有数据, 跳过 (用 --force 可刷新)")

    state = load_state(db)
    stop_uuid = state.get("top_uuid") if args.resume else None
    if args.resume and stop_uuid:
        last = state.get("updated_at")
        print(f"[恢复] 上次爬取到 uuid={stop_uuid} (时间 {last}), 遇到即停")
    elif args.resume:
        print("[恢复] 未找到 checkpoint, 按普通模式全量爬")

    stop_ms = None
    if getattr(args, "since_hours", None) is not None:
        cutoff_dt = datetime.now(timezone.utc) - timedelta(hours=args.since_hours)
        stop_ms = int(cutoff_dt.timestamp() * 1000)
        local_str = cutoff_dt.astimezone().strftime("%Y-%m-%d %H:%M")
        print(f"[时间窗] 仅抓 {args.since_hours}h 内 (cutoff={local_str})")

    skip_future = not getattr(args, "include_future", False)
    print(f"\n[列表] 抓取访谈列表 max={args.max or '全部'} "
          f"page_size={args.page_size} lang={args.lang} "
          f"skip_future={skip_future}")
    items = fetch_items_paginated(
        client, max_items=args.max, page_size=args.page_size,
        lang=args.lang,
        stop_at_uuid=stop_uuid, stop_before_ms=stop_ms,
        skip_future=skip_future,
    )
    print(f"[列表] 共 {len(items)} 条待处理\n")
    if not items:
        print("无新访谈 (或账号失效)")
        return {"added": 0, "skipped": 0, "failed": 0}

    new_top = items[0].get("uuid")
    added = skipped = failed = 0
    cap = cap_from_args(args)

    pbar = tqdm(items, desc="访谈", unit="条", dynamic_ncols=True,
                bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}] {postfix}")
    for item in pbar:
        if cap.exhausted() or _BUDGET.exhausted():
            tqdm.write(f"  [antibot] 达到 daily-cap={cap.max_items}, 本轮停 (防风控)")
            break
        uid = item.get("uuid")
        title = item.get("title") or ""
        try:
            row = dump_one(client, db, item, lang=args.lang, force=args.force)
            if row["状态"] == "已跳过":
                skipped += 1
                tqdm.write(f"  · [{uid[:8]}] {title[:60]}  已存在, 跳过")
            else:
                added += 1
                cap.bump(); _BUDGET.bump()
                tqdm.write(f"  ✓ [{uid[:8]}] {title[:60]}  "
                           f"转录{row['转录字数']}字/{row['转录段数']}段 专家{row['专家数']} 公司{row['目标公司']}")
        except AuthExpired:
            raise
        except Exception as e:
            failed += 1
            tqdm.write(f"  ✗ [{uid[:8] if uid else '?'}] {title[:60]}  ERR: {type(e).__name__}: {e}")

        pbar.set_postfix_str(f"新增={added} 跳过={skipped} 失败={failed}")

        save_state(db, last_processed_uuid=uid,
                   last_processed_at=datetime.now(timezone.utc),
                   in_progress=True)
        _throttle()
    pbar.close()

    save_state(db, top_uuid=new_top, in_progress=False,
               last_run_end_at=datetime.now(timezone.utc),
               last_run_stats={"added": added, "skipped": skipped, "failed": failed})

    total = db[COL_INTERVIEWS].estimated_document_count()
    print(f"\n本轮完成: 新增 {added} / 跳过 {skipped} / 失败 {failed}")
    print(f"MongoDB 当前访谈总数: {total}")
    return {"added": added, "skipped": skipped, "failed": failed}


# ==================== CLI ====================

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="forum.thirdbridge.com 专家访谈爬虫 (MongoDB 存储)")
    p.add_argument("--max", type=int, default=None,
                   help="最多爬取条数 (默认: 全部, 翻到尽头)")
    p.add_argument("--page-size", type=int, default=32,
                   help="每页大小 (默认 32, 与前端一致)")
    p.add_argument("--force", action="store_true",
                   help="强制重爬已入库 + 强制刷新 account")
    p.add_argument("--resume", action="store_true",
                   help="增量模式: 从上次 checkpoint 续, 遇到 top_uuid 停")
    p.add_argument("--show-state", action="store_true",
                   help="显示 checkpoint + token 健康检查后退出")
    p.add_argument("--reset-state", action="store_true",
                   help="清空 checkpoint + daily 统计后退出")
    p.add_argument("--today", action="store_true",
                   help="统计今日平台访谈 vs 本地库, 结果存 _state")
    p.add_argument("--date", metavar="YYYY-MM-DD", default=None,
                   help="配合 --today 指定日期 (默认今天)")
    p.add_argument("--watch", action="store_true",
                   help="实时模式: 定时轮询. Ctrl+C 退出")
    p.add_argument("--interval", type=int, default=600,
                   help="实时模式轮询间隔秒数 (默认 600)")
    p.add_argument("--since-hours", type=float, default=None,
                   help="只抓过去 N 小时内的访谈 (基于 start 时间戳)")
    p.add_argument("--include-future", action="store_true",
                   help="保留已排期但尚未发生的访谈 (默认跳过, 因为 transcript 都是空的)")
    p.add_argument("--lang", choices=("zh", "en", "jp"), default="zh",
                   help="详情/搜索语言 (默认 zh)")
    p.add_argument("--auth", default=os.environ.get("TB_AUTH"),
                   help="Cookie 字符串 (覆盖 credentials.json; 或 env TB_AUTH)")
    p.add_argument("--mongo-uri", default=MONGO_URI_DEFAULT,
                   help=f"MongoDB URI (默认 {MONGO_URI_DEFAULT}, env MONGO_URI)")
    p.add_argument("--mongo-db", default=MONGO_DB_DEFAULT,
                   help=f"MongoDB 数据库名 (默认 {MONGO_DB_DEFAULT}, env MONGO_DB)")
    # 反爬节流 (crawl/antibot.py) — third_bridge 检测最严, 节奏保守;
    # default_cap 2026-04-25 300→0: 数量闸反爬价值≈0, 靠节奏/指纹/SoftCooldown.
    # 裸跑长时间回填仍可 CLI 传 --daily-cap N 自保.
    add_antibot_args(p, default_base=4.0, default_jitter=3.0,
                     default_burst=30, default_cap=0, platform="thirdbridge")
    return p.parse_args()


def connect_mongo(uri: str, dbname: str):
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    try:
        client.admin.command("ping")
    except PyMongoError as e:
        print(f"错误: 无法连接 MongoDB ({uri}): {e}")
        sys.exit(1)
    db = client[dbname]
    db[COL_INTERVIEWS].create_index("title")
    db[COL_INTERVIEWS].create_index("release_time")
    db[COL_INTERVIEWS].create_index("release_time_ms")
    db[COL_INTERVIEWS].create_index("is_future")
    db[COL_INTERVIEWS].create_index("crawled_at")
    print(f"[Mongo] 已连接 {uri} -> db: {dbname}")
    return db


def _fmt_state(s: dict) -> str:
    return json.dumps(
        {k: str(v) if isinstance(v, datetime) else v for k, v in s.items()},
        ensure_ascii=False, indent=2,
    )


def main() -> None:
    args = parse_args()

    # 用 CLI 参数覆盖模块级 throttle + 接 budget
    global _THROTTLE, _BUDGET
    _THROTTLE = throttle_from_args(args, platform="thirdbridge")
    # account_id 取 cookie 整串 hash (Cognito sub 解 JWT 嵌套结构, 太麻烦, 用 hash)
    cookie, _ = load_creds_from_file()
    import hashlib as _hl
    _account_id = "h_" + _hl.md5(cookie.encode()).hexdigest()[:12]
    _BUDGET = budget_from_args(args, account_id=_account_id, platform="thirdbridge")
    log_config_stamp(_THROTTLE, cap=cap_from_args(args), budget=_BUDGET,
                     extra=f"acct={_account_id}")

    db = connect_mongo(args.mongo_uri, args.mongo_db)

    if args.reset_state:
        r = db[COL_STATE].delete_many({})
        print(f"已清除 {r.deleted_count} 条 checkpoint")
        return

    # Show-state 要发请求做健康检查, 所以也需要 client
    if args.auth:
        cookie = args.auth
        ua = DEFAULT_UA
    else:
        cookie, ua = load_creds_from_file()
    client = create_client(cookie, ua)

    if args.show_state:
        s = load_state(db)
        print("--- 访谈 checkpoint (crawler_interviews) ---")
        print(_fmt_state(s) if s else "  无")
        print(f"\ninterviews: {db[COL_INTERVIEWS].estimated_document_count()}  "
              f"account: {db[COL_ACCOUNT].estimated_document_count()}")
        print()
        h = check_token(client)
        if h["ok"]:
            print(f"[token] ✓ uuid={h['uuid']} email={h['email']} "
                  f"company={h['company']} forum_status={h['forum_status']}")
        else:
            print(f"[token] ✗ {h['error']}")
            print("  → 浏览器重登 forum.thirdbridge.com, 更新 credentials.json 的 cookie")
        return

    if args.today:
        try:
            count_today(client, db, date_str=args.date, lang=args.lang)
        except AuthExpired as e:
            print(f"\n[错误] 会话失效: {e}")
            print("  → 浏览器重登 forum.thirdbridge.com, 更新 credentials.json")
            sys.exit(2)
        return

    try:
        if args.watch:
            print(f"[实时模式] 每 {args.interval}s 轮询一次. Ctrl+C 退出.\n")
            round_num = 0
            while True:
                round_num += 1
                print(f"\n{'=' * 60}\n[轮次 {round_num}] "
                      f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n{'=' * 60}")
                try:
                    run_once(client, db, args)
                except AuthExpired as e:
                    print(f"[轮次 {round_num}] 会话失效: {e}")
                    print("  → 请更新 credentials.json 后重启")
                    break
                except KeyboardInterrupt:
                    print("\n[实时模式] Ctrl+C 退出")
                    break
                except Exception as e:
                    print(f"[轮次 {round_num}] 异常: {type(e).__name__}: {e}")
                try:
                    time.sleep(args.interval)
                except KeyboardInterrupt:
                    print("\n[实时模式] Ctrl+C 退出")
                    break
        else:
            try:
                run_once(client, db, args)
            except AuthExpired as e:
                print(f"\n[错误] 会话失效: {e}")
                print("  → 浏览器重登 forum.thirdbridge.com, 更新 credentials.json")
                sys.exit(2)
    finally:
        client.close()


if __name__ == "__main__":
    main()
