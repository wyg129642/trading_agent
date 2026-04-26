#!/usr/bin/env python3
"""
www.alphaengine.top 爬虫 (MongoDB 存储)

抓取 4 大数据类别 (对应 /summary-center 的 4 个 tab):

  summary        纪要     (AI 会议纪要 / 业绩会 / 调研 / 专家会)
  chinaReport    国内研报  (内资券商 / 期货研究 — 含 PDF)
  foreignReport  海外研报  (Citi / JPM / GS 等外资 — 含 PDF)
  news           资讯     (TMTB / 海内外媒体 EOD Wrap 等)

使用方法:
  1. 浏览器登录 https://www.alphaengine.top/
  2. F12 → Application → Local Storage → 复制 `token` 值 (JWT, 以 eyJ 开头)
  3. 粘贴到 credentials.json {"token": "<JWT>"} 或通过 --auth / env ALPHAENGINE_AUTH
  4. 启动 MongoDB (默认 mongodb://localhost:27017)
  5. 运行:
       python3 scraper.py --show-state          # checkpoint + token 健康
       python3 scraper.py --max 5               # 每类各爬 5 条小试
       python3 scraper.py                       # 全量 (翻到 has_next_page=false)
       python3 scraper.py --watch --interval 60 --resume
       python3 scraper.py --category chinaReport --max 200

MongoDB 数据模型 (database=alphaengine):
  summaries        — 纪要, _id = doc_id
  china_reports    — 国内研报, _id = doc_id
  foreign_reports  — 海外研报, _id = doc_id
  news_items       — 资讯, _id = doc_id
  account          — 账户 / 元信息
  _state           — checkpoint + daily stats

与 gangtise / jinmen / AceCamp 同构, 参考 crawl/README.md §3-§4.

API 说明 (逆向自 SummaryCenter.<hash>.js):
  - 列表: POST /api/v1/kmpsummary/summary/search/streamSearch (SSE)
    body 关键字段: {code, size, realtime, search_after}
  - PDF:  GET  /api/v1/kmpsummary/download/<doc_id>
  - 资讯/纪要无 PDF 下载权限, 仅以列表响应中的 doc_introduce 做预览
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

# 共享反爬模块 (crawl/antibot.py) + 实时推送 (crawl/crawler_push.py)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from antibot import (  # noqa: E402
    AdaptiveThrottle, DailyCap, SessionDead,
    parse_retry_after, is_auth_dead,
    add_antibot_args, throttle_from_args, cap_from_args,
    AccountBudget, SoftCooldown, detect_soft_warning,
    headers_for_platform, log_config_stamp, budget_from_args,
    account_id_for_alphaengine, warmup_session,
)
from ticker_tag import stamp as _stamp_ticker  # noqa: E402
try:
    from crawler_push import get_realtime_pusher  # noqa: E402
except Exception:  # pragma: no cover — Redis / import issues are non-fatal
    def get_realtime_pusher(platform=None):  # type: ignore
        return None

_THROTTLE: AdaptiveThrottle = AdaptiveThrottle(base_delay=3.0, jitter=2.0,
                                                burst_size=40,
                                                platform="alphaengine")
_BUDGET: AccountBudget = AccountBudget("alphaengine", "default", 0)
_PLATFORM = "alphaengine"

# ==================== 配置 ====================
# 浏览器 Application → Local Storage → `token` (JWT, 以 eyJ 开头, 约 30 天有效)
# 登录态过期 (401) 需重登浏览器拷贝
ALPHAENGINE_TOKEN = ""

CREDS_FILE = Path(__file__).resolve().parent / "credentials.json"


def _load_token_from_file() -> str:
    if not CREDS_FILE.exists():
        return ""
    try:
        d = json.loads(CREDS_FILE.read_text(encoding="utf-8"))
        return (d.get("token") or "").strip()
    except Exception:
        return ""


def _load_refresh_token_from_file() -> str:
    """Second token used by /kmpadmin/auth/refresh to mint a fresh access_token
    without user re-login. Rotates on every successful refresh — scraper must
    call ``_persist_tokens`` to write the new chain back."""
    if not CREDS_FILE.exists():
        return ""
    try:
        d = json.loads(CREDS_FILE.read_text(encoding="utf-8"))
        return (d.get("refresh_token") or "").strip()
    except Exception:
        return ""


def _persist_tokens(access: str, refresh: str) -> None:
    """Atomic credentials.json update — preserves other fields.

    /kmpadmin/auth/refresh invalidates the OLD refresh_token (server-side Redis
    key). If we don't persist the new one, the next refresh round fails with
    `refreshToken与redis不一致` and we lose the chain.
    """
    data = {}
    if CREDS_FILE.exists():
        try:
            data = json.loads(CREDS_FILE.read_text(encoding="utf-8")) or {}
        except Exception:
            data = {}
    data["token"] = access
    if refresh:
        data["refresh_token"] = refresh
    data["token_refreshed_at"] = datetime.now(timezone.utc).isoformat()
    tmp = CREDS_FILE.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(CREDS_FILE)


def refresh_access_token(refresh_token: str,
                         client_flag: str = "pc",
                         timeout: float = 10.0) -> tuple[Optional[str], Optional[str]]:
    """POST /api/v1/kmpadmin/auth/refresh with ``Authorization-Refresh`` header.

    Returns (new_access, new_refresh). Either is None on failure.
    Server payload:
      {"code":200, "data":{"token":..., "refresh_token":..., "user_id":...}}
    On stale chain: {"code":420, "msg":"refreshToken与redis不一致"} → None.
    """
    if not refresh_token:
        return None, None
    url = f"{API_BASE}/api/v1/kmpadmin/auth/refresh"
    try:
        r = requests.post(
            url,
            headers={
                "Authorization-Refresh": refresh_token,
                "Content-Type": "application/json",
                "Origin": API_BASE, "Referer": f"{API_BASE}/",
                "User-Agent": "Mozilla/5.0",
            },
            json={"clientFlag": client_flag}, timeout=timeout,
        )
        try:
            body = r.json()
        except Exception:
            return None, None
        if body.get("code") != 200:
            print(f"[refresh] 失败 code={body.get('code')} msg={body.get('msg')}")
            return None, None
        data = body.get("data") or {}
        return data.get("token"), data.get("refresh_token")
    except requests.RequestException as e:
        print(f"[refresh] 请求异常: {e}")
        return None, None


def refresh_with_file_lock(client_flag: str = "pc",
                           min_age_s: float = 60.0) -> tuple[Optional[str], bool]:
    """Coordinated refresh across N concurrent scraper processes.

    AlphaEngine enforces **single active session per user** — each successful
    refresh invalidates all prior refresh_tokens. If 4 watchers each fire
    /auth/refresh simultaneously, one wins and the other 3 get
    ``refreshToken与redis不一致`` → session dead for them.

    This helper uses an fcntl exclusive file lock around credentials.json:
      - First process grabs the lock, reads current refresh_token, calls
        /auth/refresh, writes new access+refresh back, releases lock.
      - Subsequent processes block on lock, then re-read the (now fresh)
        credentials.json — they skip the refresh call entirely if the file
        was modified within ``min_age_s`` seconds.

    Returns (new_access_token, did_refresh). ``did_refresh=False`` means the
    caller picked up a fresh token from the file without calling the server.
    """
    import fcntl
    lock_path = CREDS_FILE.with_suffix(".lock")
    # Touch lock file if missing
    lock_path.touch(exist_ok=True)
    with open(lock_path, "r+") as lf:
        fcntl.flock(lf.fileno(), fcntl.LOCK_EX)
        try:
            # Re-read credentials inside the lock — another process may have
            # just rotated the token.
            if CREDS_FILE.exists():
                try:
                    d = json.loads(CREDS_FILE.read_text(encoding="utf-8")) or {}
                except Exception:
                    d = {}
                refreshed_at = d.get("token_refreshed_at")
                if refreshed_at:
                    try:
                        ts = datetime.fromisoformat(refreshed_at.replace("Z", "+00:00"))
                        age = (datetime.now(timezone.utc) - ts).total_seconds()
                        if age < min_age_s:
                            tok = d.get("token") or ""
                            if tok:
                                print(f"[refresh] 信任 {age:.0f}s 前另一进程刚刷的 token, 跳过")
                                return tok, False
                    except Exception:
                        pass
                rt = d.get("refresh_token") or ""
            else:
                rt = ""
            if not rt:
                return None, False
            new_access, new_refresh = refresh_access_token(rt, client_flag=client_flag)
            if new_access:
                _persist_tokens(new_access, new_refresh or rt)
                return new_access, True
            return None, False
        finally:
            fcntl.flock(lf.fileno(), fcntl.LOCK_UN)


API_BASE = "https://www.alphaengine.top"
SEARCH_PATH = "/api/v1/kmpsummary/summary/search/streamSearch"
DOWNLOAD_PATH = "/api/v1/kmpsummary/download"
# List-vs-detail 配额不对称: list 端点撞 REFRESH_LIMIT 时, detail 端点仍返
# 完整正文 + 签名 COS URL 可直下 PDF. 参考 CRAWLERS.md §9.5.8.
# URL 模式: GET /api/v1/kmpsummary/summary/detail/<doc_id>/true
# 返回 {code:200, data:{content, digest, file_path (signed 3h), section, question_answer, ...}}
DETAIL_PATH = "/api/v1/kmpsummary/summary/detail"

MONGO_URI_DEFAULT = os.environ.get(
    "MONGO_URI",
    "mongodb://127.0.0.1:27018/",
)
MONGO_DB_DEFAULT = os.environ.get("MONGO_DB", "alphaengine")
COL_ACCOUNT = "account"
COL_STATE = "_state"

# PDF 本地存放目录 (大体积数据, 迁出项目树)
PDF_DIR_DEFAULT = os.environ.get(
    "ALPHAENGINE_PDF_DIR",
    "/home/ygwang/crawl_data/alphaengine_pdfs",
)

# ==================== 分类定义 ====================
#
# code 值是 SummaryCenter.js 的 tabsActive, 映射关系:
#   summary       → summaries       (纪要)
#   chinaReport   → china_reports   (国内研报, PDF)
#   foreignReport → foreign_reports (海外研报, PDF)
#   news          → news_items      (资讯)

CATEGORIES: list[dict] = [
    {
        "key": "summary",
        "code": "summary",
        "label": "纪要",
        "collection": "summaries",
        # summary 也走同一个 /kmpsummary/download/<id> 端点拿 PDF
        # (见 Vue 源码: ["summary","chinaReport","foreignReport","thirdReport",
        # "companyAllAnnouncement","globalThinkTank"] 全部用这个 endpoint).
        # 大部分 company meeting 纪要有 PDF (带正文); 没有 PDF 的会返
        # code=500 "下载文件失败", 我们识别后跳过并 mark pdf_unavailable.
        "has_pdf": True,
    },
    {
        "key": "chinaReport",
        "code": "chinaReport",
        "label": "国内研报",
        "collection": "china_reports",
        "has_pdf": True,
    },
    {
        "key": "foreignReport",
        "code": "foreignReport",
        "label": "海外研报",
        "collection": "foreign_reports",
        "has_pdf": True,
    },
    {
        "key": "news",
        "code": "news",
        "label": "资讯",
        "collection": "news_items",
        "has_pdf": False,
    },
]
CATEGORY_KEYS = [c["key"] for c in CATEGORIES]
_CAT_BY_KEY = {c["key"]: c for c in CATEGORIES}


# ==================== 网络 / 会话 ====================

def create_session(token: str) -> requests.Session:
    """构造带 AlphaEngine bearer 头的 Session.

    注意: CN CDN (EdgeOne) 这家域能透 Clash, 保留 trust_env=True 即可.
    与 Gangtise 不同 (那家 CDN 明确禁代理).
    """
    s = requests.Session()
    h = headers_for_platform("alphaengine")
    h.update({
        "Accept": "*/*",
        "Referer": f"{API_BASE}/",
        "Origin": API_BASE,
        "Authorization": f"Bearer {token}",
    })
    s.headers.update(h)
    # Warmup: 先 GET alphaengine.top landing 再调 streamSearch / detail
    warmup_session(s, "alphaengine")
    return s


def _parse_sse(text: str) -> list[dict]:
    """解析 SSE `id:\nevent:\ndata:{json}\n\n` 格式, 返回所有 data payload.

    注意: 服务端在 `data:` 字段里直接塞了带换行的 JSON (违反 SSE 规范, 但
    浏览器原生 EventSource 因内部实现能兼容). 所以不能用 splitlines — 必须
    逐 frame 找到 `data:` prefix 后取到 frame 末尾.
    """
    out: list[dict] = []
    for chunk in re.split(r"\n\n+", text):
        if not chunk.strip():
            continue
        # Strip the leading `id:` and `event:` lines. `data:` can span many
        # physical lines (JSON with embedded \n).
        lines = chunk.split("\n")
        idx = 0
        while idx < len(lines) and not lines[idx].startswith("data:"):
            idx += 1
        if idx >= len(lines):
            continue
        payload = "\n".join(lines[idx:])[len("data:"):].strip()
        if not payload:
            continue
        try:
            out.append(json.loads(payload))
        except Exception:
            # SSE spec says consecutive data: lines are joined with \n; try
            # stripping any leading `data:` from continuation lines too.
            cleaned = re.sub(r"\ndata:", "\n", payload)
            try:
                out.append(json.loads(cleaned))
            except Exception:
                continue
    return out


class RefreshLimit(Exception):
    """Raised when the server returns {"code":450, "data":{"code":"REFRESH_LIMIT"}}.

    This is a *soft* per-account rate limit on search/list requests, NOT a
    permanent auth failure. Caller should back off (minutes, not seconds) and
    retry. Don't blow the checkpoint — quota resets.

    When raised mid-pagination, ``partial_items`` carries every item the loop
    successfully fetched before the quota gate triggered. Earlier behavior
    discarded all of them, so an empty foreignReport (~10k+ historical items
    that need >1 day of quota to drain) could never persist a single doc.
    """
    def __init__(self, msg: str = "", partial_items: Optional[list] = None,
                 last_search_after: Optional[list] = None):
        super().__init__(msg)
        self.partial_items = partial_items or []
        self.last_search_after = last_search_after


def fetch_list_page(session: requests.Session, code: str,
                    size: int = 20,
                    search_after: Optional[list] = None,
                    retries: int = 2,
                    timeout: int = 30) -> dict:
    """POST streamSearch, 读完 SSE, 返回最终 {results, has_next_page, search_after, total}.

    401/403 → SessionDead (token 失效, 要重登)
    HTTP 200 + JSON body {code:450, data.code:REFRESH_LIMIT} → RefreshLimit (per-account 刷新配额)
    HTTP 200 + JSON body {code:500, ...} → {"_err": msg} (其他业务错误)
    429/5xx → 退避重试
    """
    url = f"{API_BASE}{SEARCH_PATH}"
    body: dict = {"code": code, "size": size, "realtime": False}
    if search_after:
        body["search_after"] = search_after

    last_exc = None
    for attempt in range(1, retries + 2):
        try:
            r = session.post(url, json=body, timeout=timeout,
                             headers={"Content-Type": "application/json"})
            if is_auth_dead(r.status_code):
                raise SessionDead(f"HTTP {r.status_code} on {SEARCH_PATH}: {r.text[:200]}")
            if r.status_code == 429 or 500 <= r.status_code < 600:
                if r.status_code == 429:
                    SoftCooldown.trigger(_PLATFORM, reason=f"http_429:{SEARCH_PATH}",
                                          minutes=45)
                ra = parse_retry_after(r.headers.get("Retry-After"))
                _THROTTLE.on_retry(retry_after_sec=ra, attempt=attempt)
                _THROTTLE.sleep_before_next()
                last_exc = f"HTTP {r.status_code}"
                continue
            if r.status_code != 200:
                return {"_err": f"HTTP {r.status_code}", "results": [],
                        "has_next_page": False}
            # When the account hits a REFRESH_LIMIT, the server switches
            # Content-Type from text/event-stream to application/json and
            # returns: {"code":450, "msg":"无查看权限或额度已达上限",
            #           "data":{"code":"REFRESH_LIMIT", "description":"用户基础刷新额度达到上限"},
            #           "success":false}
            ctype = (r.headers.get("content-type") or "").lower()
            body_text = r.content.decode("utf-8", errors="replace")
            if "application/json" in ctype or body_text.lstrip().startswith("{"):
                try:
                    err = json.loads(body_text)
                except Exception:
                    err = None
                if isinstance(err, dict):
                    inner = (err.get("data") or {})
                    inner_code = inner.get("code") if isinstance(inner, dict) else None
                    if err.get("code") == 450 and inner_code == "REFRESH_LIMIT":
                        # REFRESH_LIMIT 是该账号 list quota 用尽 — 触发同平台 30 min
                        # 软冷却让其它 watcher 一起退场, detail enrich worker 仍能跑
                        SoftCooldown.trigger(_PLATFORM, reason="REFRESH_LIMIT",
                                              minutes=30)
                        raise RefreshLimit(
                            f"REFRESH_LIMIT: {inner.get('description') or err.get('msg')}")
                    # 通用业务级软警告 (限流关键词等)
                    reason = detect_soft_warning(200, body=err)
                    if reason:
                        mins = 30 if "quota" in reason or "code_7" in reason else 60
                        SoftCooldown.trigger(_PLATFORM, reason=reason, minutes=mins)
                        _THROTTLE.on_warning()
                    return {"_err": f"biz code={err.get('code')} msg={err.get('msg')}",
                            "results": [], "has_next_page": False}

            # Normal SSE path: UTF-8 decode (server drops the charset param)
            events = _parse_sse(body_text)
            final = None
            for ev in reversed(events):
                if ev.get("id") == "_final" and isinstance(ev.get("content"), dict):
                    final = ev["content"]
                    break
            if final is None:
                for ev in events:
                    c = ev.get("content")
                    if isinstance(c, dict) and c.get("results") is not None:
                        final = c
                        break
            if final is None:
                return {"_err": "no _final event", "results": [],
                        "has_next_page": False}
            return {
                "results": final.get("results") or [],
                "has_next_page": bool(final.get("has_next_page")),
                "search_after": final.get("search_after"),
                "total": final.get("total") or 0,
            }
        except (SessionDead, RefreshLimit):
            raise
        except requests.RequestException as e:
            last_exc = e
            if attempt < retries + 1:
                _THROTTLE.on_retry(attempt=attempt)
                _THROTTLE.sleep_before_next()
    return {"_err": f"req_err: {last_exc}", "results": [], "has_next_page": False}


def fetch_detail(session: requests.Session, doc_id: str,
                 timeout: int = 20) -> Optional[dict]:
    """GET /api/v1/kmpsummary/summary/detail/<doc_id>/true.

    CRITICAL: this endpoint **bypasses the list REFRESH_LIMIT** quota, even
    though the list endpoint (streamSearch) is hard-locked. See CRAWLERS.md
    §9.5.8 "list-vs-detail 配额不对称" for the general methodology, validated
    for AlphaEngine 2026-04-22.

    Returns the `data` dict (unwrapped) or None on error. Useful fields:
      - content:       full extracted text (research: PDF text, meetings: transcript)
      - digest / digest_cn: AI summary
      - main_point:    bulleted key points
      - section:       timestamped/speaker-attributed transcript (summary only)
      - question_answer: COS URL to QA JSON
      - file_path:     **signed COS URL to original PDF**, valid 3h — direct
                       download bypasses BOTH list quota AND download quota
      - file_format:   "pdf" | "audio" | ...
      - original_text: raw transcript (summary only, when available)

    Raises SessionDead on 401. Does NOT raise RefreshLimit (detail is quota-free).
    """
    if not doc_id:
        return None
    url = f"{API_BASE}{DETAIL_PATH}/{doc_id}/true"
    try:
        r = session.get(url, timeout=timeout)
    except requests.RequestException as e:
        print(f"  [detail {doc_id}] 请求异常: {e}")
        return None
    if is_auth_dead(r.status_code):
        raise SessionDead(f"detail {doc_id}: HTTP {r.status_code}")
    if r.status_code != 200:
        print(f"  [detail {doc_id}] HTTP {r.status_code}")
        return None
    try:
        body = r.json()
    except Exception:
        return None
    # Detect 401 `刷新 token` → surface as SessionDead so auto-refresh kicks in
    if body.get("code") == 401 or "刷新 token" in str(body.get("msg") or ""):
        raise SessionDead(f"detail biz 401: {body.get('msg')}")
    if body.get("code") != 200:
        return None
    return body.get("data") or None


def fetch_cos_pdf(session: requests.Session, signed_url: str,
                  dest: Path, timeout: int = 60) -> tuple[int, Optional[str]]:
    """Download PDF from the signed 腾讯云 COS URL returned by detail endpoint.

    The URL has ``q-sign-time;q-key-time`` ~3 hour window, after which it
    returns 403 ``SignatureExpired``. Use within the same request cycle.

    Advantages vs ``/kmpsummary/download/<id>``:
      - Hits CDN/COS directly, **no account-level quota check**
      - No ``权益额度已达上限`` error possible (storage layer, not app)
      - Works while the download endpoint is rate-limited
    """
    if dest.exists() and dest.stat().st_size > 0:
        return dest.stat().st_size, ""
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    try:
        r = session.get(signed_url, timeout=timeout)
        if r.status_code != 200:
            return 0, f"http_{r.status_code}"
        content = r.content
        if not content.startswith(b"%PDF"):
            return 0, f"not PDF (prefix={content[:40].decode('utf-8', errors='replace')!r})"
        with tmp.open("wb") as f:
            f.write(content)
        tmp.replace(dest)
        return len(content), None
    except (requests.RequestException, IOError) as e:
        try:
            if tmp.exists(): tmp.unlink()
        except Exception: pass
        return 0, f"{type(e).__name__}: {e}"


class PdfQuotaExhausted(Exception):
    """Raised when the PDF download endpoint returns `权益额度已达上限`.

    This is a PER-ACCOUNT daily download quota, not a permanent failure.
    Caller should stop PDF downloads for this scraper round (metadata can
    still be ingested) and retry later when quota resets.
    """


# Known JSON error messages returned by the download endpoint instead of PDF bytes:
#   code=500 "下载文件失败"         → 该 doc 本身没有 PDF (permanent skip)
#   code=450 "权益额度已达上限"     → 账号今日下载配额用完 (retry tomorrow)
#   code=450 msg 含 "无下载权限"    → 该账号 tier 不能下 (permanent for this account)
_PDF_PERMANENT_ERR_MSGS = ("下载文件失败", "无下载权限")
_PDF_QUOTA_MSGS = ("权益额度已达上限", "额度已达上限", "基础刷新额度")


def download_pdf(session: requests.Session, doc_id: str,
                 dest: Path, timeout: int = 60,
                 max_retries: int = 3) -> tuple[int, Optional[str]]:
    """GET /api/v1/kmpsummary/download/<doc_id>. 返回 (bytes_written, err).

    响应要么是 PDF 二进制流 (首 4 字节 %PDF), 要么是 JSON 错误:
      {"code":500, "msg":"下载文件失败", ...}  — 该 doc 无 PDF
      {"code":450, "msg":"权益额度已达上限", "data":{"code":1, ...}}  — 今日配额耗尽
      {"code":450, "msg":"无下载权限"}  — 该账号 tier 无此权限

    配额耗尽时抛 PdfQuotaExhausted, 上层停本轮 PDF 下载; 其他错误返 (0, err_msg).
    """
    if not doc_id:
        return 0, "no doc_id"
    if dest.exists() and dest.stat().st_size > 0:
        return dest.stat().st_size, ""
    dest.parent.mkdir(parents=True, exist_ok=True)
    url = f"{API_BASE}{DOWNLOAD_PATH}/{doc_id}"
    tmp = dest.with_suffix(dest.suffix + ".part")
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            r = session.get(url, timeout=timeout)
            if True:
                if is_auth_dead(r.status_code):
                    return 0, f"auth_dead HTTP {r.status_code}"
                if r.status_code != 200:
                    last_err = f"http_{r.status_code}"
                    if r.status_code == 429 or 500 <= r.status_code < 600:
                        _THROTTLE.on_retry(attempt=attempt)
                        _THROTTLE.sleep_before_next()
                        continue
                    return 0, last_err

                # Dispatch by Content-Type. Server sends `application/pdf`
                # for real files and `application/json` for error envelopes,
                # but some error paths leak through with a pdf content-type
                # (the body is still JSON). Treat raw bytes as the source of
                # truth and fall back to content-type only as a hint.
                ctype = (r.headers.get("content-type") or "").lower()
                raw = r.content
                is_json_body = (b"application/json" in ctype.encode() or
                                raw[:1] in (b"{", b"["))
                if is_json_body or not raw.startswith(b"%PDF"):
                    # Fast raw-byte scan for quota markers — survives utf-8
                    # truncation that would trip json.loads.
                    for marker in ("权益额度已达上限".encode("utf-8"),
                                   "额度已达上限".encode("utf-8"),
                                   "基础刷新额度".encode("utf-8")):
                        if marker in raw:
                            raise PdfQuotaExhausted(
                                f"quota (raw match): {raw[:120].decode('utf-8', errors='replace')}")
                    # Server may signal "stale token, call /auth/refresh" via
                    # HTTP 200 + biz code 401 `用户状态发生变更，刷新 token`.
                    # Surface as SessionDead so the caller's refresh hook fires.
                    if b'"code":401' in raw or "刷新 token".encode() in raw or b"\xe5\x88\xb7\xe6\x96\xb0 token" in raw:
                        raise SessionDead(
                            f"biz 401 (token stale): {raw[:120].decode('utf-8', errors='replace')}")
                    body_text = raw.decode("utf-8", errors="replace")
                    try:
                        err = json.loads(body_text)
                    except (json.JSONDecodeError, ValueError):
                        return 0, f"not PDF ({len(raw)}B prefix={body_text[:80]!r})"
                    msg = str(err.get("msg") or "")
                    if any(q in msg for q in _PDF_QUOTA_MSGS):
                        raise PdfQuotaExhausted(f"quota: {msg}")
                    if err.get("code") == 401 or "刷新 token" in msg or "凭证无效" in msg:
                        raise SessionDead(f"biz 401: {msg[:100]}")
                    return 0, f"biz code={err.get('code')} msg={msg[:80]}"

                # Real PDF — dump the buffered bytes directly.
                written = len(raw)
                with tmp.open("wb") as f:
                    f.write(raw)
            tmp.replace(dest)
            return written, None
        except PdfQuotaExhausted:
            raise
        except (requests.RequestException, IOError) as e:
            last_err = f"{type(e).__name__}: {e}"
            try:
                if tmp.exists():
                    tmp.unlink()
            except Exception:
                pass
            _THROTTLE.on_retry(attempt=attempt)
            _THROTTLE.sleep_before_next()
    return 0, last_err or "exhausted retries"


# ==================== 工具函数 ====================

_SAFE_FNAME_RE = re.compile(r'[\\/:*?"<>|\x00-\x1f]')


def _safe_filename(name: str, max_len: int = 160) -> str:
    cleaned = _SAFE_FNAME_RE.sub("_", name or "").strip().strip(".")
    return cleaned[:max_len] or "untitled"


def _strip_html(s: str) -> str:
    if not s:
        return ""
    s = re.sub(r"<br\s*/?>", "\n", s, flags=re.I)
    s = re.sub(r"<[^>]+>", "", s)
    s = html.unescape(s)
    return s.strip()


def _parse_cst_to_ms(v: Any) -> Optional[int]:
    """'YYYY-MM-DD HH:MM:SS' 本地 CST → UTC ms."""
    if not v:
        return None
    try:
        dt = datetime.strptime(str(v).strip(), "%Y-%m-%d %H:%M:%S")
        return int(dt.replace(tzinfo=timezone(timedelta(hours=8))).timestamp() * 1000)
    except Exception:
        return None


def _release_ms_from_item(item: dict) -> Optional[int]:
    """优先 rank_date (列表排序依据) > publish_time. 格式 'YYYY-MM-DD HH:MM:SS' 本地 CST.

    保留兼容性: 如果新字段 rank_date_ms 已经算过, 直接用. 否则算一次.
    """
    for key in ("rank_date", "publish_time"):
        ms = _parse_cst_to_ms(item.get(key))
        if ms is not None:
            return ms
    return None


def _release_time_str(item: dict) -> str:
    """UI 主显示时间. 2026-04-22 起统一为 rank_date (平台排序时间) — 与 BE
    排序依据 release_time_ms 一致, 与原始网站列表展示一致. 原始 publish_time
    作为 `publish_time` 字段单独保留供详情展示 "报告发布日期"."""
    for key in ("rank_date", "publish_time"):
        v = item.get(key)
        if v:
            return str(v)[:16]
    return ""


def dedup_id(item: dict) -> str:
    rid = item.get("doc_id") or item.get("summary_id") or item.get("id")
    if rid:
        return str(rid)
    base = f"{item.get('title','')}|{item.get('publish_time','')}|{item.get('rank_date','')}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()


def _strip_logos(item: dict) -> dict:
    """Remove embedded base64 institution logos (can be 2-5 KB each)
    so Mongo docs don't balloon. Keep institution_id for cross-ref."""
    out = dict(item)
    if "logos" in out and isinstance(out["logos"], list):
        out["logos"] = [
            {k: v for k, v in (lg or {}).items() if k != "logo"}
            for lg in out["logos"]
        ]
    if "institution_logo" in out:
        # keep None / empty, drop long base64
        lg = out.get("institution_logo") or ""
        if isinstance(lg, str) and len(lg) > 200:
            out["institution_logo"] = f"<b64:{len(lg)}>"
    return out


# ==================== 文档入库 ====================

def dump_item(db, category: dict, item: dict,
              session: Optional[requests.Session] = None,
              pdf_dir: Optional[Path] = None,
              download_pdf_flag: bool = True,
              force: bool = False) -> tuple[str, dict]:
    """Upsert one item into the right collection. Returns (status, stats_dict).

    PdfQuotaExhausted bubbles up — caller should disable PDF for the rest of
    the round and keep ingesting metadata.
    """
    col = db[category["collection"]]
    did = dedup_id(item)

    ex = col.find_one(
        {"_id": did},
        {"_id": 1, "pdf_local_path": 1, "pdf_size_bytes": 1,
         "pdf_unavailable": 1, "stats": 1},
    )
    if ex and not force:
        if category["has_pdf"] and download_pdf_flag:
            # Already have PDF — skip.
            if ex.get("pdf_local_path") and ex.get("pdf_size_bytes", 0) > 0:
                return "skipped", ex.get("stats") or {}
            # Permanently marked as "no PDF available for this doc" — skip.
            if ex.get("pdf_unavailable"):
                return "skipped", ex.get("stats") or {}
            # else fall through to retry (quota may have reset)
        else:
            return "skipped", ex.get("stats") or {}

    release_ms = _release_ms_from_item(item)
    rank_date_ms = _parse_cst_to_ms(item.get("rank_date"))
    publish_time_ms = _parse_cst_to_ms(item.get("publish_time"))
    # Clamp "future" timestamps (some 研报 have next-day publish_time) to now.
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    if release_ms and release_ms > now_ms + 3600_000:
        release_ms = now_ms
    if rank_date_ms and rank_date_ms > now_ms + 3600_000:
        rank_date_ms = now_ms
    release_time = _release_time_str(item) or (
        datetime.fromtimestamp(
            release_ms / 1000, tz=timezone(timedelta(hours=8)),
        ).strftime("%Y-%m-%d %H:%M")
        if release_ms else ""
    )

    institutions = item.get("institution_name") or []
    organization = institutions[0] if institutions else ""
    industries = item.get("industry_name") or item.get("first_industry_name") or []
    if not isinstance(industries, list):
        industries = [str(industries)]
    companies = item.get("company_code") or []
    if not isinstance(companies, list):
        companies = [str(companies)]
    company_names = item.get("company_name") or []
    if not isinstance(company_names, list):
        company_names = [str(company_names)]
    authors = item.get("author_list") or []
    if authors and not isinstance(authors, list):
        authors = [str(authors)]

    doc_introduce = _strip_html(item.get("doc_introduce") or "")
    content_md = doc_introduce  # list endpoint is the only source of text for most types

    # PDF download (summary / chinaReport / foreignReport all use the same
    # /kmpsummary/download/<id> endpoint; news is HTML-only).
    # Quota-exhausted (code=450) propagates via PdfQuotaExhausted so the
    # caller can disable PDF attempts for the rest of the round.
    pdf_local = ""
    pdf_size = 0
    pdf_err = ""
    pdf_rel_path = ""
    pdf_unavailable = False
    if category["has_pdf"] and download_pdf_flag and session and pdf_dir:
        ym = (release_time or "unknown")[:7] or "unknown"
        fname = _safe_filename((item.get("title") or did)[:120] + ".pdf")
        fname = f"{did}_{fname}"
        dest = pdf_dir / category["key"] / ym / fname
        pdf_size, err = download_pdf(session, did, dest)
        pdf_err = err or ""
        if pdf_size > 0:
            pdf_local = str(dest)
            pdf_rel_path = str(dest.relative_to(pdf_dir))
        elif pdf_err and "下载文件失败" in pdf_err:
            # Permanent — this doc has no PDF on the platform.
            pdf_unavailable = True

    cleaned_item = _strip_logos(item)

    stats = {
        "content_chars": len(content_md),
        "page_num": item.get("page_num") or 0,
        "pdf_size": pdf_size,
    }
    doc = {
        "_id": did,
        "category": category["key"],
        "doc_id": item.get("doc_id"),
        "summary_id": item.get("summary_id"),
        "title": item.get("title") or "",
        "title_cn": item.get("title_cn") or "",
        "search_title": item.get("search_title") or "",
        "release_time": release_time,
        "release_time_ms": release_ms,
        # Separate ms fields so BE/FE can sort by EITHER dimension explicitly.
        # rank_date = 平台重新索引时间 (default list order, matches UI)
        # publish_time = 报告原始发布时间 (day-granularity for most reports)
        "rank_date_ms": rank_date_ms,
        "publish_time_ms": publish_time_ms,
        "publish_time": item.get("publish_time"),
        "rank_date": item.get("rank_date"),
        "organization": organization,
        "institution_names": institutions,
        "institution_ids": item.get("institution_id") or [],
        "authors": authors,
        "document_type_id": item.get("document_type_id"),
        "document_type_name": item.get("document_type_name"),
        "type_full_name": item.get("type_full_name"),
        "type_full_id": item.get("type_full_id"),
        "first_type_name": item.get("first_type_name"),
        "type_show_name": item.get("type_show_name"),
        "type_id": item.get("type_id"),
        "industry_names": industries,
        "first_industry_name": item.get("first_industry_name") or [],
        "company_codes": companies,
        "company_names": company_names,
        "company_show_name": item.get("company_show_name") or [],
        "company_multi_map": item.get("company_multi_map") or {},
        "doc_icon": item.get("doc_icon"),
        "page_num": item.get("page_num") or 0,
        "depth_flag": item.get("depth_flag"),
        "sensitive": item.get("sensitive"),
        "sensitive_permission": item.get("sensitive_permission"),
        "realtime": item.get("realtime"),
        "share_link": item.get("share_link"),
        "doc_introduce": doc_introduce,
        "content_md": content_md,
        "pdf_rel_path": pdf_rel_path,
        "pdf_local_path": pdf_local,
        "pdf_size_bytes": pdf_size,
        "pdf_download_error": pdf_err,
        "pdf_unavailable": pdf_unavailable,
        "list_item": cleaned_item,
        "web_url": f"{API_BASE}/#/summary-center?tabsActive={category['code']}&sub_id={did}",
        "stats": stats,
        "crawled_at": datetime.now(timezone.utc),
    }
    _stamp_ticker(doc, "alphaengine", col)
    col.replace_one({"_id": did}, doc, upsert=True)
    status = "added" if (not category["has_pdf"] or pdf_size > 0 or not download_pdf_flag) else "added_no_pdf"
    return status, stats


# ==================== checkpoint ====================

def state_doc_id(category_key: str) -> str:
    return f"crawler_{category_key}"


def load_state(db, category_key: str) -> dict:
    return db[COL_STATE].find_one({"_id": state_doc_id(category_key)}) or {}


def save_state(db, category_key: str, **kwargs) -> None:
    kwargs["updated_at"] = datetime.now(timezone.utc)
    db[COL_STATE].update_one(
        {"_id": state_doc_id(category_key)},
        {"$set": kwargs}, upsert=True,
    )


# ==================== 分页抓取 ====================

def fetch_items_paginated(session, category: dict, max_items: Optional[int],
                          page_size: int, stop_at_id: Optional[str] = None,
                          stop_before_ms: Optional[int] = None,
                          start_search_after: Optional[list] = None) -> list[dict]:
    """拉 `category` 的列表, 用 search_after cursor 翻页. 到以下任一条件即停:
      - max_items 达标
      - has_next_page = False
      - 某条 doc_id == stop_at_id (增量模式, 命中上次 top)
      - 某条 release_ms < stop_before_ms (--since-hours)

    ``start_search_after``: 上次 REFRESH_LIMIT 中断时挂在 state 上的 cursor.
    传入后从那个游标继续翻, 而不是从头. 用于历史回灌跨多日 quota 接力.
    """
    all_items: list[dict] = []
    search_after = list(start_search_after) if start_search_after else None
    seen_keys: set[str] = set()
    page_idx = 0
    empty_streak = 0

    label = f"{category['key']}"

    while True:
        page_idx += 1
        try:
            resp = fetch_list_page(session, category["code"], size=page_size,
                                   search_after=search_after)
        except RefreshLimit as rl:
            # Per-account refresh quota hit. Stash whatever pages we already
            # fetched before propagating, so the caller can persist them
            # rather than throwing 4000 IDs away every quota cycle.
            tqdm.write(f"  [{label} p{page_idx}] REFRESH_LIMIT: {rl} — 暂停本类别抓取 "
                       f"(已抓 {len(all_items)} 条, 已挂在异常上让 run_category 入库)")
            raise RefreshLimit(str(rl), partial_items=list(all_items),
                               last_search_after=search_after)
        if resp.get("_err"):
            tqdm.write(f"  [{label} p{page_idx}] 错误: {resp['_err']}")
            empty_streak += 1
            if empty_streak >= 2:
                break
            _THROTTLE.sleep_before_next()
            continue
        empty_streak = 0
        items = resp.get("results") or []
        has_next = resp.get("has_next_page")
        next_cursor = resp.get("search_after")

        if not items:
            tqdm.write(f"  [{label} p{page_idx}] 空列表, 停")
            break

        hit_known = hit_old = False
        new_this = 0
        for it in items:
            did = dedup_id(it)
            if stop_at_id and did == stop_at_id:
                hit_known = True
                continue
            if stop_before_ms is not None:
                ms = _release_ms_from_item(it)
                if ms is not None and ms < stop_before_ms:
                    hit_old = True
                    continue
            if did in seen_keys:
                continue
            seen_keys.add(did)
            all_items.append(it)
            new_this += 1
            if max_items and len(all_items) >= max_items:
                break

        tqdm.write(f"  [{label} p{page_idx}] +{new_this}/{len(items)} "
                   f"(累计 {len(all_items)}) hit_known={hit_known} hit_old={hit_old} "
                   f"has_next={has_next}")

        if max_items and len(all_items) >= max_items:
            break
        if hit_known or hit_old:
            break
        if not has_next or not next_cursor:
            break
        search_after = next_cursor
        _THROTTLE.sleep_before_next()

    return all_items[:max_items] if max_items else all_items


# ==================== 一轮抓取 ====================

def run_category(session, db, category: dict, args) -> dict:
    print(f"\n{'─' * 60}\n[{category['label']} / {category['key']}]  "
          f"collection={category['collection']}\n{'─' * 60}")

    state = load_state(db, category["key"])
    backfill_cursor = state.get("backfill_search_after")
    has_existing_data = db[category["collection"]].estimated_document_count() > 0
    # During historical backfill (cursor outstanding) — paginate from that
    # cursor and skip the stop_at_id gate (otherwise items[0] would hit_known
    # immediately and break out, and we'd never finish the backfill).
    if backfill_cursor:
        stop_id = None
        print(f"[回灌恢复] 沿用 backfill cursor={str(backfill_cursor)[:60]}.. "
              f"(history backfill 接力, 暂不应用 stop_at_id)")
    else:
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

    print(f"[列表] max={args.max or '全部'} page_size={args.page_size}"
          + (f"  start_cursor={str(backfill_cursor)[:40]}.." if backfill_cursor else ""))
    refresh_limit_hit: Optional[RefreshLimit] = None
    try:
        items = fetch_items_paginated(session, category,
                                      max_items=args.max,
                                      page_size=args.page_size,
                                      stop_at_id=stop_id,
                                      stop_before_ms=stop_ms,
                                      start_search_after=backfill_cursor)
    except RefreshLimit as rl:
        # Quota burned mid-pagination — keep whatever we got so far so the
        # write loop below can persist them, then re-raise after writes so the
        # outer watch loop knows to sleep until midnight CST.
        items = list(getattr(rl, "partial_items", None) or [])
        refresh_limit_hit = rl
        print(f"[{category['label']}] REFRESH_LIMIT 中断, 保留已抓 {len(items)} 条进入写库阶段")
    if not items:
        if refresh_limit_hit is not None:
            # Even with 0 items, persist the cursor so next run resumes.
            save_state(db, category["key"],
                       backfill_search_after=getattr(refresh_limit_hit,
                                                     "last_search_after", None) or backfill_cursor,
                       last_run_end_at=datetime.now(timezone.utc),
                       last_run_stats={"added": 0, "skipped": 0, "failed": 0,
                                       "refresh_limit": True})
            raise refresh_limit_hit
        # Natural completion with no new items — backfill is done if we had a cursor.
        if backfill_cursor:
            save_state(db, category["key"], backfill_search_after=None)
            print(f"[{category['label']}] backfill 完成, 清掉 cursor")
        print(f"[{category['label']}] 无新条目")
        return {"added": 0, "skipped": 0, "failed": 0}

    new_top_id = dedup_id(items[0])
    added = skipped = failed = 0
    pdf_quota_exhausted = False        # Flip once PdfQuotaExhausted fires — skip PDF for rest of round
    cap = cap_from_args(args)
    pdf_dir = Path(args.pdf_dir) if args.pdf_dir else None

    # Publisher for realtime push (no-op if backend Redis unreachable)
    pusher = get_realtime_pusher()

    pbar = tqdm(items, desc=category["label"], unit="条", dynamic_ncols=True,
                bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}] {postfix}")
    for it in pbar:
        if cap.exhausted() or _BUDGET.exhausted():
            tqdm.write(f"  [antibot] 达到 daily-cap={cap.max_items}, 停")
            break
        did = dedup_id(it)
        title = (it.get("title") or "")[:60]
        was_skip = False
        try:
            # Once quota is hit for this round, keep ingesting metadata but
            # don't waste requests hitting download endpoint — they'll all 450.
            download_ok = (not args.skip_pdf) and (not pdf_quota_exhausted)
            status, info = dump_item(db, category, it,
                                     session=session,
                                     pdf_dir=pdf_dir,
                                     download_pdf_flag=download_ok,
                                     force=args.force)
            if status == "skipped":
                skipped += 1
                was_skip = True
                tqdm.write(f"  · [{did[:16]}] {title}  已存在")
            else:
                added += 1
                cap.bump(); _BUDGET.bump()
                parts = []
                if info.get("content_chars"):
                    parts.append(f"text={info['content_chars']}字")
                if info.get("pdf_size"):
                    parts.append(f"PDF={info['pdf_size']:,}B")
                elif pdf_quota_exhausted and category.get("has_pdf"):
                    parts.append("PDF=quota")
                if info.get("page_num"):
                    parts.append(f"{info['page_num']}页")
                suffix = "  " + "  ".join(parts) if parts else ""
                tqdm.write(f"  ✓ [{did[:16]}] {title}{suffix}")
                # Realtime push to backend (best-effort, fire & forget)
                if pusher is not None:
                    try:
                        pusher.publish_new_item(
                            category=category["key"],
                            collection=category["collection"],
                            doc_id=did, title=(it.get("title") or ""),
                            release_time=_release_time_str(it) or None,
                            release_time_ms=_release_ms_from_item(it),
                            organization=(it.get("institution_name") or [None])[0],
                            industry=(it.get("industry_name") or [None])[0],
                            has_pdf=bool(info.get("pdf_size")),
                        )
                    except Exception:
                        pass  # pushing must never block the scraper
        except SessionDead:
            raise
        except PdfQuotaExhausted as qe:
            # PDF daily quota exhausted — record metadata without PDF and
            # stop downloading PDFs for the rest of this round. Next round
            # (or next day) retries.
            pdf_quota_exhausted = True
            tqdm.write(f"  ⚠ PDF 配额耗尽: {qe}. 本轮后续只入元数据.")
            try:
                status, info = dump_item(db, category, it,
                                         session=session,
                                         pdf_dir=pdf_dir,
                                         download_pdf_flag=False,
                                         force=args.force)
                if status != "skipped":
                    added += 1
                    cap.bump(); _BUDGET.bump()
                    tqdm.write(f"  ✓ [{did[:16]}] {title}  (meta only)")
            except Exception as e2:
                failed += 1
                tqdm.write(f"  ✗ [{did[:16]}] {title}  meta ERR: {e2}")
        except Exception as e:
            failed += 1
            tqdm.write(f"  ✗ [{did[:16]}] {title}  ERR: {type(e).__name__}: {e}")
        pbar.set_postfix_str(f"+{added} ={skipped} ✗{failed}")
        save_state(db, category["key"], last_dedup_id=did,
                   last_processed_at=datetime.now(timezone.utc),
                   in_progress=True)
        # DB dedup hits made no remote call — skip the throttle.
        if not was_skip:
            _THROTTLE.sleep_before_next()
    pbar.close()

    # State update logic (3 cases):
    #   - RefreshLimit hit: PRESERVE the cursor so next round resumes from
    #     the last successful page; only set top_dedup_id once the historical
    #     backfill finishes (otherwise items[0] of a partial fetch would lock
    #     in as "newest seen" and stop_at_id would block all future backfill).
    #   - Natural completion + had a cursor: backfill is done, clear cursor
    #     and now lock in items[0] as the new top_dedup_id.
    #   - Normal incremental run: just update top_dedup_id as before.
    state_update: dict = {
        "in_progress": False,
        "pdf_quota_exhausted_at": (datetime.now(timezone.utc) if pdf_quota_exhausted else None),
        "last_run_end_at": datetime.now(timezone.utc),
        "last_run_stats": {"added": added, "skipped": skipped, "failed": failed,
                           "pdf_quota_exhausted": pdf_quota_exhausted,
                           "refresh_limit": refresh_limit_hit is not None},
    }
    if refresh_limit_hit is not None:
        cur = (getattr(refresh_limit_hit, "last_search_after", None)
               or backfill_cursor)
        state_update["backfill_search_after"] = cur
        # Don't touch top_dedup_id during partial pagination.
    else:
        # Natural completion of the pagination.
        state_update["top_dedup_id"] = new_top_id
        if backfill_cursor:
            state_update["backfill_search_after"] = None
            print(f"  [回灌完成] 已清 backfill cursor, 切到增量模式")
    save_state(db, category["key"], **state_update)

    total = db[category["collection"]].estimated_document_count()
    print(f"  完成: 新增 {added} / 跳过 {skipped} / 失败 {failed}"
          + ("  [PDF 配额今日耗尽]" if pdf_quota_exhausted else "")
          + ("  [REFRESH_LIMIT 中途断, cursor 已存]" if refresh_limit_hit else ""))
    print(f"  {category['collection']} 总数: {total}")
    if refresh_limit_hit is not None:
        # Re-raise so run_once tags the round summary with refresh_limit and
        # the watch loop applies long backoff (sleep until midnight CST).
        # Stash the partial-write stats on the exception so the round summary
        # shows the actual added count instead of pretending we got nothing.
        refresh_limit_hit.run_stats = {"added": added, "skipped": skipped,
                                       "failed": failed,
                                       "pdf_quota_exhausted": pdf_quota_exhausted}
        raise refresh_limit_hit
    return {"added": added, "skipped": skipped, "failed": failed,
            "pdf_quota_exhausted": pdf_quota_exhausted}


def run_once(session, db, args) -> dict:
    """Run every requested category once.

    Returns `{key: stats}`, where stats may include `refresh_limit: True` if the
    per-account quota blocked the run. Caller (watch loop) uses this to decide
    how long to sleep before the next round.
    """
    cats = [_CAT_BY_KEY[args.category]] if args.category != "all" else CATEGORIES
    summary: dict = {}
    for cat in cats:
        try:
            summary[cat["key"]] = run_category(session, db, cat, args)
        except KeyboardInterrupt:
            raise
        except SessionDead as e:
            print(f"\n[致命] 会话失效: {e}")
            print("  → 浏览器重登 https://www.alphaengine.top/, "
                  "更新 credentials.json 里的 token.")
            summary[cat["key"]] = {"added": 0, "skipped": 0, "failed": -1,
                                    "error": "SessionDead"}
            break
        except RefreshLimit as rl:
            stats = getattr(rl, "run_stats", None) or {}
            added_partial = int(stats.get("added", 0))
            skipped_partial = int(stats.get("skipped", 0))
            failed_partial = int(stats.get("failed", 0))
            print(f"\n[{cat['key']}] REFRESH_LIMIT hit — {rl} "
                  f"(partial: +{added_partial}/={skipped_partial}/✗{failed_partial})")
            summary[cat["key"]] = {"added": added_partial,
                                    "skipped": skipped_partial,
                                    "failed": failed_partial,
                                    "refresh_limit": True, "error": str(rl)}
        except Exception as e:
            tqdm.write(f"\n[{cat['key']}] 异常: {type(e).__name__}: {e}")
            summary[cat["key"]] = {"added": 0, "skipped": 0, "failed": -1,
                                    "error": str(e)}

    print(f"\n{'═' * 60}")
    print("本轮汇总: " + "  ".join(
        f"{k}+{s.get('added',0)}/={s.get('skipped',0)}/✗{s.get('failed',0)}"
        + ("*REFRESH_LIMIT*" if s.get("refresh_limit") else "")
        for k, s in summary.items()))
    print(f"{'═' * 60}")
    return summary


# ==================== PDF 回填 (bypass REFRESH_LIMIT) ====================

def _backfill_refresh_session(old_session: requests.Session) -> Optional[requests.Session]:
    """Same file-locked refresh as the watch loop uses. Returns a new session
    bound to the fresh token, or None if refresh failed.
    """
    new_access, did = refresh_with_file_lock(client_flag="pc", min_age_s=120)
    if new_access:
        msg = "refresh 成功" if did else "跟随其他进程"
        print(f"  [refresh] ✓ {msg}, 切换 session")
        return create_session(new_access)
    print("  [refresh] ✗ refresh_token 链已失效, 需浏览器重登")
    return None


def backfill_pdfs(session, db, args) -> dict:
    """Iterate existing Mongo docs in has_pdf categories and download missing PDFs.

    AlphaEngine enforces two INDEPENDENT rate limits:
      - REFRESH_LIMIT on list/search endpoints (list blocked while this quota burns)
      - PDF quota on /kmpsummary/download/<id>

    When the list quota is exhausted, the /download/ path can STILL work for
    categories whose PDF tier is fresh (2026-04-22 observation: chinaReport
    downloads succeed while ALL streamSearch calls return 450 REFRESH_LIMIT).
    This mode lets the scraper keep ingesting full-text PDFs even during a
    list-level block.

    Targets:
      - pdf_size_bytes == 0 AND pdf_unavailable != True
      - pdf_download_error is empty OR contains a transient marker
    """
    pdf_dir = Path(args.pdf_dir) if args.pdf_dir else None
    if not pdf_dir:
        print("[backfill] 缺少 --pdf-dir"); return {}
    pdf_dir.mkdir(parents=True, exist_ok=True)

    cats_to_try = [c for c in CATEGORIES if c["has_pdf"]]
    if args.category != "all":
        cats_to_try = [c for c in cats_to_try if c["key"] == args.category]
    if not cats_to_try:
        print(f"[backfill] 分类 {args.category} 无 PDF 支持")
        return {}

    overall: dict = {}
    cap = cap_from_args(args)

    for cat in cats_to_try:
        coll = db[cat["collection"]]
        query = {
            "pdf_size_bytes": {"$in": [0, None]},
            "$or": [
                {"pdf_unavailable": {"$ne": True}},
                {"pdf_unavailable": {"$exists": False}},
            ],
        }
        cursor = coll.find(query, {"_id": 1, "doc_id": 1, "title": 1,
                                    "release_time": 1, "pdf_download_error": 1,
                                    "pdf_rel_path": 1})
        if args.backfill_max:
            cursor = cursor.limit(args.backfill_max)
        # Materialize so the cursor doesn't time out during slow downloads.
        items = list(cursor)
        if not items:
            print(f"[backfill/{cat['key']}] 无待补 PDF")
            overall[cat["key"]] = {"attempted": 0, "ok": 0, "quota_exhausted": False,
                                    "no_pdf": 0, "failed": 0}
            continue

        print(f"[backfill/{cat['key']}] 待补 {len(items)} 条 → {cat['collection']}")
        ok = no_pdf = failed = 0
        quota_hit = False

        pbar = tqdm(items, desc=f"{cat['label']} PDF", unit="条",
                    bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}] {postfix}")
        session_dead_count = 0
        for doc in pbar:
            if cap.exhausted() or _BUDGET.exhausted():
                tqdm.write(f"  [backfill] 达到 daily-cap={cap.max_items}, 停")
                break
            did = str(doc["_id"])
            title = (doc.get("title") or "")[:40]
            release_time = doc.get("release_time") or ""
            ym = release_time[:7] or "unknown"
            fname = _safe_filename(f"{did}_{(doc.get('title') or did)[:80]}.pdf")
            dest = pdf_dir / cat["key"] / ym / fname
            try:
                n, err = download_pdf(session, did, dest)
            except PdfQuotaExhausted as qe:
                quota_hit = True
                tqdm.write(f"  ⚠ quota exhausted ({cat['key']}): {qe}")
                break
            except SessionDead as sd:
                # Server says "刷新 token". Rotate via file lock and retry once.
                session_dead_count += 1
                if session_dead_count > 3:
                    tqdm.write(f"  ✗ [backfill] 连续 3 次 SessionDead, 停. {sd}")
                    break
                tqdm.write(f"  ↻ [backfill] token stale, 触发 refresh: {sd}")
                new_session = _backfill_refresh_session(session)
                if new_session is None:
                    tqdm.write(f"  ✗ [backfill] refresh 失败, 停")
                    break
                session = new_session
                # Retry the same doc with fresh session.
                try:
                    n, err = download_pdf(session, did, dest)
                except Exception as e2:
                    failed += 1
                    tqdm.write(f"  ✗ [{did[:16]}] {title}  retry ERR: {e2}")
                    _THROTTLE.sleep_before_next()
                    continue
                if n == 0 and "biz code=401" in (err or ""):
                    tqdm.write(f"  ✗ [backfill] 新 token 还是 401, 退出. {err}")
                    break
                session_dead_count = 0  # reset streak on successful retry
            if n > 0:
                ok += 1
                cap.bump(); _BUDGET.bump()
                rel = str(dest.relative_to(pdf_dir))
                coll.update_one({"_id": did}, {"$set": {
                    "pdf_local_path": str(dest),
                    "pdf_rel_path": rel,
                    "pdf_size_bytes": n,
                    "pdf_download_error": "",
                    "pdf_unavailable": False,
                }})
                tqdm.write(f"  ✓ [{did[:16]}] {title}  {n:,}B")
                # Realtime push — the item now has a PDF
                pusher = get_realtime_pusher()
                if pusher:
                    try:
                        pusher.publish_new_item(
                            category=cat["key"], collection=cat["collection"],
                            doc_id=did, title=doc.get("title") or "",
                            release_time=release_time or None,
                            release_time_ms=None,
                            organization=None, industry=None, has_pdf=True,
                            extra={"event": "pdf_backfill"})
                    except Exception:
                        pass
            else:
                # err like "not PDF (64B prefix='{\"code\":500,\"msg\":\"下载文件失败\"..."
                if err and "下载文件失败" in err:
                    no_pdf += 1
                    coll.update_one({"_id": did}, {"$set": {
                        "pdf_unavailable": True, "pdf_download_error": err,
                    }})
                    tqdm.write(f"  ∅ [{did[:16]}] {title}  (no PDF on platform)")
                else:
                    failed += 1
                    coll.update_one({"_id": did}, {"$set": {
                        "pdf_download_error": err or "unknown",
                    }})
                    tqdm.write(f"  ✗ [{did[:16]}] {title}  err={err}")
            pbar.set_postfix_str(f"+{ok} ∅{no_pdf} ✗{failed}")
            _THROTTLE.sleep_before_next()
        pbar.close()
        overall[cat["key"]] = {"attempted": len(items), "ok": ok,
                                "no_pdf": no_pdf, "failed": failed,
                                "quota_exhausted": quota_hit}
        print(f"  完成: +{ok} / ∅{no_pdf} / ✗{failed}"
              + ("  [quota 耗尽, 跳过剩余]" if quota_hit else ""))

    # Watch mode: loop every interval
    if args.backfill_watch:
        print(f"[backfill-watch] 第一轮完成, {args.interval}s 后再试")
        while True:
            try:
                time.sleep(args.interval)
                print(f"\n{'─'*60}\n[backfill 轮次] "
                      f"{datetime.now():%Y-%m-%d %H:%M:%S}\n{'─'*60}")
                backfill_pdfs_once = backfill_pdfs(session, db, argparse.Namespace(
                    **{**vars(args), "backfill_watch": False}))
            except KeyboardInterrupt:
                print("\n[backfill] Ctrl+C 退出"); break

    return overall


# ==================== Enrich via detail endpoint (quota bypass) ====================

def enrich_via_detail(session, db, args) -> dict:
    """Backfill content + PDFs for existing items via the detail endpoint.

    Uses ``GET /api/v1/kmpsummary/summary/detail/<doc_id>/true`` which bypasses
    the list REFRESH_LIMIT entirely. Per CRAWLERS.md §9.5.8 "list-vs-detail
    配额不对称" — the platform put the quota gate on the list endpoint but
    forgot to add it on detail, so we can fetch full content + signed COS URLs
    for PDFs one-by-one as long as we know the IDs.

    Targets (per category):
      - Items where ``content_md`` is short (< 1000 chars) — need full body
      - Items where ``pdf_size_bytes == 0`` — need PDF (bypasses download quota)

    Triggered by:
      scraper.py --enrich-via-detail [--category X] [--max N]

    Also runnable as a recurring `--enrich-watch` every args.interval seconds.
    """
    pdf_dir = Path(args.pdf_dir) if args.pdf_dir else None

    cats = [_CAT_BY_KEY[args.category]] if args.category != "all" else CATEGORIES
    overall = {}
    cap = cap_from_args(args)

    for cat in cats:
        coll = db[cat["collection"]]
        # Candidates: short content OR missing PDF
        query = {
            "$or": [
                {"content_md": {"$exists": False}},
                {"content_md": ""},
                # Short content means we only saved the doc_introduce preview (~400 chars)
                {"$expr": {"$lt": [{"$strLenCP": {"$ifNull": ["$content_md", ""]}}, 1000]}},
            ]
        }
        if cat.get("has_pdf"):
            # Also pull in docs lacking PDF — detail gives us signed COS URL
            query = {"$or": query["$or"] + [
                {"pdf_size_bytes": {"$in": [0, None]},
                 "pdf_unavailable": {"$ne": True}},
            ]}
        cursor = coll.find(query, {
            "_id": 1, "doc_id": 1, "title": 1, "release_time": 1,
            "content_md": 1, "pdf_size_bytes": 1, "pdf_local_path": 1,
        })
        if args.backfill_max:
            cursor = cursor.limit(args.backfill_max)
        items = list(cursor)
        if not items:
            print(f"[enrich/{cat['key']}] 无待补项")
            overall[cat["key"]] = {"attempted": 0, "enriched": 0, "pdf_got": 0, "failed": 0}
            continue

        print(f"[enrich/{cat['key']}] 待补 {len(items)} 条 via detail 端点 (绕 REFRESH_LIMIT)")
        enriched = pdf_got = failed = 0

        pbar = tqdm(items, desc=f"{cat['label']} detail", unit="条",
                    bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}] {postfix}")
        for doc in pbar:
            if cap.exhausted() or _BUDGET.exhausted():
                tqdm.write(f"  [enrich] daily-cap={cap.max_items} 达到, 停")
                break
            did = str(doc["_id"])
            title = (doc.get("title") or "")[:50]
            try:
                data = fetch_detail(session, did)
            except SessionDead as sd:
                tqdm.write(f"  ↻ token stale on detail: {sd}")
                # Reuse the file-locked refresh helper (defined at module scope)
                new_access, _did_refresh = refresh_with_file_lock(client_flag="pc", min_age_s=120)
                if new_access:
                    session = create_session(new_access)
                    try:
                        data = fetch_detail(session, did)
                    except Exception as e2:
                        failed += 1
                        tqdm.write(f"  ✗ [{did[:16]}] retry ERR: {e2}")
                        _THROTTLE.sleep_before_next()
                        continue
                else:
                    tqdm.write("  ✗ [enrich] refresh 失败, 停")
                    break
            if not data:
                failed += 1
                tqdm.write(f"  ✗ [{did[:16]}] detail 无返回")
                _THROTTLE.sleep_before_next()
                continue

            # Extract everything useful
            content = data.get("content") or ""
            digest = data.get("digest") or data.get("digest_cn") or ""
            main_point = data.get("main_point") or ""
            original_text = data.get("original_text") or ""
            question_answer_url = data.get("question_answer") or ""
            section = data.get("section") or None
            signed_pdf_url = data.get("file_path") if data.get("file_format") == "pdf" else None
            file_format = data.get("file_format")

            # Compose a richer content_md: prefer `content` (full text). For
            # summary items, `content` IS the transcript; for research, it's
            # the PDF text. Always includes digest/main_point headers so the
            # UI drawer shows them prominently.
            parts = []
            if digest:
                parts.append(f"### 摘要\n\n{digest}")
            if main_point:
                parts.append(f"### 核心观点\n\n{main_point}")
            if content:
                parts.append(f"### 正文\n\n{content}")
            elif original_text:
                parts.append(f"### 原文\n\n{original_text}")
            new_content_md = "\n\n".join(parts) or content

            update: dict = {
                "content_md": new_content_md,
                "digest_md": digest,
                "main_point_md": main_point,
                "original_text_md": original_text,
                "question_answer_url": question_answer_url,
                "detail_enriched_at": datetime.now(timezone.utc),
                "stats.content_chars": len(new_content_md),
            }
            if section:
                update["section"] = section

            # Fetch PDF via signed COS URL (bypasses download quota)
            got_pdf_now = False
            if signed_pdf_url and cat.get("has_pdf") and not args.skip_pdf and pdf_dir:
                release_time = doc.get("release_time") or ""
                ym = release_time[:7] or "unknown"
                fname = _safe_filename(f"{did}_{(doc.get('title') or did)[:80]}.pdf")
                dest = pdf_dir / cat["key"] / ym / fname
                n, err = fetch_cos_pdf(session, signed_pdf_url, dest)
                if n > 0:
                    got_pdf_now = True
                    pdf_got += 1
                    update["pdf_local_path"] = str(dest)
                    update["pdf_rel_path"] = str(dest.relative_to(pdf_dir))
                    update["pdf_size_bytes"] = n
                    update["pdf_download_error"] = ""
                    update["pdf_unavailable"] = False
                else:
                    update["pdf_download_error"] = err or "cos signed url failed"

            coll.update_one({"_id": did}, {"$set": update})
            enriched += 1
            cap.bump(); _BUDGET.bump()

            # Realtime push — this item now has full content + possibly PDF
            pusher = get_realtime_pusher()
            if pusher:
                try:
                    pusher.publish_new_item(
                        category=cat["key"], collection=cat["collection"],
                        doc_id=did, title=doc.get("title") or "",
                        release_time=doc.get("release_time") or None,
                        release_time_ms=None, organization=None, industry=None,
                        has_pdf=got_pdf_now or bool(doc.get("pdf_local_path")),
                        extra={"event": "detail_enriched",
                               "content_chars": len(new_content_md)},
                    )
                except Exception:
                    pass

            pdf_tag = f" PDF={n}B" if got_pdf_now else ""
            tqdm.write(f"  ✓ [{did[:16]}] {title}  content={len(content)}字"
                       f" digest={len(digest)}字{pdf_tag}")
            pbar.set_postfix_str(f"enrich={enriched} pdf={pdf_got} fail={failed}")
            _THROTTLE.sleep_before_next()
        pbar.close()

        overall[cat["key"]] = {"attempted": len(items), "enriched": enriched,
                               "pdf_got": pdf_got, "failed": failed}
        print(f"  完成: 补正文 {enriched} / 补 PDF {pdf_got} / 失败 {failed}")

    # Watch mode
    if args.enrich_watch:
        print(f"[enrich-watch] 第一轮完成, {args.interval}s 后再试")
        while True:
            try:
                time.sleep(args.interval)
                print(f"\n{'─'*60}\n[enrich 轮次] "
                      f"{datetime.now():%Y-%m-%d %H:%M:%S}\n{'─'*60}")
                enrich_via_detail(session, db, argparse.Namespace(
                    **{**vars(args), "enrich_watch": False}))
            except KeyboardInterrupt:
                print("\n[enrich] Ctrl+C 退出"); break

    return overall


# ==================== 当日统计 --today ====================

_BJ_TZ = timezone(timedelta(hours=8))


def count_today(session, db, args) -> dict:
    # AlphaEngine publish_time/rank_date 是 Asia/Shanghai 壁钟, --today 用 BJ 对齐.
    if args.date:
        day_start = datetime.strptime(args.date, "%Y-%m-%d").replace(tzinfo=_BJ_TZ)
        target = args.date
    else:
        day_start = datetime.now(_BJ_TZ).replace(hour=0, minute=0, second=0, microsecond=0)
        target = day_start.strftime("%Y-%m-%d")
    day_end = day_start.replace(hour=23, minute=59, second=59)
    start_ms = int(day_start.timestamp() * 1000)
    end_ms = int(day_end.timestamp() * 1000)

    print(f"[统计] 扫 AlphaEngine {target} 各分类平台条数...")
    cats = [_CAT_BY_KEY[args.category]] if args.category != "all" else CATEGORIES
    overall: dict = {"date": target}
    for cat in cats:
        items_today: list[dict] = []
        search_after = None
        page = 0
        stop = False
        while not stop:
            page += 1
            resp = fetch_list_page(session, cat["code"], size=args.page_size,
                                   search_after=search_after)
            if resp.get("_err") or not resp.get("results"):
                break
            for it in resp["results"]:
                ms = _release_ms_from_item(it)
                if ms is None:
                    continue
                if ms < start_ms:
                    stop = True
                    break
                if ms <= end_ms:
                    items_today.append(it)
            if stop:
                break
            if not resp.get("has_next_page"):
                break
            search_after = resp.get("search_after")
            _THROTTLE.sleep_before_next()

        ids = [dedup_id(it) for it in items_today]
        in_db = db[cat["collection"]].count_documents({"_id": {"$in": ids}}) if ids else 0
        overall[cat["key"]] = {
            "platform_count": len(items_today),
            "in_db": in_db,
            "missing": len(items_today) - in_db,
            "pages_scanned": page,
        }
        print(f"  {cat['label']:>6s} ({cat['key']}): 平台 {len(items_today):>4d}  "
              f"入库 {in_db:>4d}  缺 {overall[cat['key']]['missing']:>4d}")

    overall["scanned_at"] = datetime.now(timezone.utc)
    db[COL_STATE].replace_one({"_id": f"daily_{target}"},
                              {"_id": f"daily_{target}", **overall},
                              upsert=True)
    print(f"\n已存 {COL_STATE} (_id=daily_{target})")
    return overall


# ==================== 账户 ====================

def dump_account(session, db) -> None:
    """Probe list endpoint for each category, save one sample for observability."""
    print("\n[账户] 抓一条样本做健康检查...")
    col = db[COL_ACCOUNT]
    now = datetime.now(timezone.utc)
    for cat in CATEGORIES:
        resp = fetch_list_page(session, cat["code"], size=1)
        col.replace_one(
            {"_id": f"sample-{cat['key']}"},
            {"_id": f"sample-{cat['key']}", "code": cat["code"],
             "total": resp.get("total"), "has_next_page": resp.get("has_next_page"),
             "sample": (resp.get("results") or [{}])[0] if resp.get("results") else {},
             "updated_at": now},
            upsert=True,
        )
        tag = "✓" if not resp.get("_err") else resp.get("_err")
        print(f"  [{tag}] {cat['key']} total={resp.get('total')}")


# ==================== CLI ====================

def parse_args():
    p = argparse.ArgumentParser(
        description="www.alphaengine.top (阿尔法引擎) 爬虫 — 纪要 / 国内研报 / 海外研报 / 资讯")
    p.add_argument("--category", choices=["all", *CATEGORY_KEYS], default="all",
                   help=f"指定分类 (默认 all). 可选: {', '.join(CATEGORY_KEYS)}")
    p.add_argument("--max", type=int, default=None,
                   help="最多爬 N 条 (每类). 默认翻页到 has_next_page=false")
    p.add_argument("--page-size", type=int, default=20,
                   help="每页大小 (默认 20, 接口硬限 ~30)")
    p.add_argument("--force", action="store_true",
                   help="强制重爬已入库的内容")
    p.add_argument("--stream-backfill", action="store_true",
                   help="流式回填: alphaengine 已有 backfill_search_after 光标机制, "
                        "本 flag 目前是 no-op, 保持 flag 兼容其他 scraper.")
    p.add_argument("--resume", action="store_true",
                   help="增量模式: 遇到上次 top_dedup_id 即停分页")
    p.add_argument("--watch", action="store_true",
                   help="实时模式: 定时轮询. Ctrl+C 退出")
    p.add_argument("--interval", type=int, default=600,
                   help="实时模式轮询间隔秒数 (默认 600)")
    p.add_argument("--since-hours", type=float, default=None,
                   help="仅抓过去 N 小时内的内容 (按 publish_time/rank_date)")
    p.add_argument("--show-state", action="store_true",
                   help="打印各分类 checkpoint + token 健康 后退出")
    p.add_argument("--reset-state", action="store_true",
                   help="清除所有 crawler_* checkpoint")
    p.add_argument("--today", action="store_true",
                   help="扫各分类当日平台条数 vs 本地库")
    p.add_argument("--date", metavar="YYYY-MM-DD", default=None,
                   help="配合 --today 指定日期")
    p.add_argument("--backfill-pdfs", action="store_true",
                   help="仅下载已入库但缺 PDF 的文档 (绕过 REFRESH_LIMIT). "
                        "PDF quota 与 list quota 独立, 此模式在 list 被限流时仍可跑.")
    p.add_argument("--backfill-max", type=int, default=None,
                   help="配合 --backfill-pdfs / --enrich-via-detail 限制本轮最多处理多少条")
    p.add_argument("--backfill-watch", action="store_true",
                   help="配合 --backfill-pdfs 启用持续运行 (每 --interval 秒一轮)")
    p.add_argument("--enrich-via-detail", action="store_true",
                   help="终极配额绕过: 用 /summary/detail/<id>/true 补正文 + 通过"
                        "签名 COS URL 直下 PDF. 同时绕过 list REFRESH_LIMIT 和 "
                        "download 配额. 见 CRAWLERS.md §9.5.8.")
    p.add_argument("--enrich-watch", action="store_true",
                   help="配合 --enrich-via-detail 持续运行 (每 --interval 秒一轮)")
    p.add_argument("--skip-pdf", action="store_true",
                   help="研报模式不下载 PDF")
    p.add_argument("--pdf-dir", default=PDF_DIR_DEFAULT,
                   help=f"研报 PDF 存放目录 (默认 {PDF_DIR_DEFAULT})")
    p.add_argument("--clean", choices=CATEGORY_KEYS, default=None,
                   help="清空指定分类集合 + checkpoint 后退出")
    p.add_argument("--auth",
                   default=_load_token_from_file() or os.environ.get("ALPHAENGINE_AUTH") or ALPHAENGINE_TOKEN,
                   help="JWT token (优先级: credentials.json > env ALPHAENGINE_AUTH > 脚本内)")
    p.add_argument("--mongo-uri", default=MONGO_URI_DEFAULT,
                   help=f"MongoDB URI (默认 {MONGO_URI_DEFAULT})")
    p.add_argument("--mongo-db", default=MONGO_DB_DEFAULT,
                   help=f"MongoDB 数据库名 (默认 {MONGO_DB_DEFAULT})")
    # 2026-04-25 default_cap 500→0: 实时档不再数量闸 (antibot.py 顶部 §5).
    add_antibot_args(p, default_base=3.0, default_jitter=2.0,
                     default_burst=40, default_cap=0, platform="alphaengine")
    return p.parse_args()


def connect_mongo(uri: str, dbname: str):
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    try:
        client.admin.command("ping")
    except PyMongoError as e:
        print(f"错误: 无法连接 MongoDB ({uri}): {e}")
        sys.exit(1)
    db = client[dbname]
    for cat in CATEGORIES:
        col = db[cat["collection"]]
        col.create_index("title")
        col.create_index("release_time")
        col.create_index("release_time_ms")
        col.create_index("organization")
        col.create_index("crawled_at")
        col.create_index("_canonical_tickers")
    print(f"[Mongo] 已连接 {uri} -> db: {dbname}")
    return db


def show_state(session, db) -> None:
    print("=" * 60)
    print("AlphaEngine Checkpoint")
    print("=" * 60)
    for cat in CATEGORIES:
        s = load_state(db, cat["key"])
        if not s:
            print(f"  {cat['key']:>14s}: (无)")
            continue
        print(f"  {cat['key']:>14s}: top={str(s.get('top_dedup_id'))[:24]}.. "
              f"updated_at={s.get('updated_at')} "
              f"last_run={s.get('last_run_stats')}")
    print()
    print("Collection 总数:")
    for cat in CATEGORIES:
        n = db[cat["collection"]].estimated_document_count()
        print(f"  {cat['key']:>14s} ({cat['collection']}): {n}")
    # token health: probe any code with size=1
    print()
    resp = fetch_list_page(session, "summary", size=1)
    if resp.get("_err"):
        print(f"[token] ✗ {resp['_err']}")
    else:
        print(f"[token] ✓ total={resp.get('total')} has_next={resp.get('has_next_page')}")


def main():
    args = parse_args()
    if not args.auth:
        print("错误: 未提供 token. 用 --auth / env ALPHAENGINE_AUTH 传入, "
              "或编辑 credentials.json.")
        sys.exit(1)

    global _THROTTLE, _BUDGET
    _THROTTLE = throttle_from_args(args, platform="alphaengine")
    # JWT 第二段 payload 取 uid (alphaengine 是标准 JWT)
    _account_id_base = "h_unknown"
    try:
        import base64 as _b64
        import json as _json
        parts = (args.auth or "").split(".")
        if len(parts) >= 2:
            pad = parts[1] + "=" * (-len(parts[1]) % 4)
            payload = _json.loads(_b64.urlsafe_b64decode(pad))
            for k in ("uid", "userId", "user_id", "id", "sub"):
                v = payload.get(k)
                if v:
                    _account_id_base = f"u_{v}"; break
    except Exception:
        pass
    if _account_id_base == "h_unknown":
        import hashlib as _hl
        _account_id_base = "h_" + _hl.md5((args.auth or "").encode()).hexdigest()[:12]
    # 按 worker category 独立 24h 预算 — 4 list watcher + enrich + pdf_backfill
    # 每桶 1500. 预算 key 变成 crawl:budget:alphaengine:<uid>:<category>,
    # 某个 category 撞 REFRESH_LIMIT 退避不会占用其他桶.  --category all 时
    # 统一 "all" (极少用, 仅一次性全扫); enrich/backfill 模式各自用独立 suffix.
    if args.enrich_via_detail:
        _cat_for_budget = "enrich"
    elif args.backfill_pdfs:
        _cat_for_budget = "pdf_backfill"
    elif args.category and args.category != "all":
        _cat_for_budget = args.category
    else:
        _cat_for_budget = "all"
    _account_id = account_id_for_alphaengine(_account_id_base, _cat_for_budget)
    _BUDGET = budget_from_args(args, account_id=_account_id, platform="alphaengine")
    log_config_stamp(_THROTTLE, cap=cap_from_args(args), budget=_BUDGET,
                     extra=f"acct={_account_id}")

    db = connect_mongo(args.mongo_uri, args.mongo_db)
    session = create_session(args.auth)

    if args.show_state:
        show_state(session, db)
        return

    if args.reset_state:
        n = db[COL_STATE].delete_many({"_id": {"$regex": "^crawler_"}}).deleted_count
        print(f"已清除 {n} 条 crawler_* checkpoint")
        return

    if args.clean:
        cat = _CAT_BY_KEY[args.clean]
        n_docs = db[cat["collection"]].estimated_document_count()
        db[cat["collection"]].drop()
        n_state = db[COL_STATE].delete_many({"_id": state_doc_id(args.clean)}).deleted_count
        print(f"已清除 {cat['collection']} ({n_docs} 条) + crawler_{args.clean} "
              f"checkpoint ({n_state} 条)")
        return

    if args.today:
        count_today(session, db, args)
        return

    if args.backfill_pdfs:
        backfill_pdfs(session, db, args)
        return

    if args.enrich_via_detail:
        enrich_via_detail(session, db, args)
        return

    if db[COL_ACCOUNT].estimated_document_count() == 0 or args.force:
        dump_account(session, db)

    if args.watch:
        # Stagger per-category watchers so 4 processes don't POST streamSearch
        # simultaneously. Prioritized offset: categories that are EMPTY in Mongo
        # go first (they need history + the user sees 0 items without them),
        # categories that already have data go later. Within each bucket,
        # deterministic offset per name for consistent behavior across restarts.
        #
        # 2026-04-22 bug this fixes: foreignReport's random offset was the
        # largest, so when quota exhausted mid-round, it got nothing while
        # summary/chinaReport/news each grabbed their first batch. Empty
        # collection for days after. Now empty-first guarantees fresh installs
        # always pull foreignReport before quota is spent.
        cat_collection = _CAT_BY_KEY[args.category]["collection"] if args.category != "all" else None
        is_empty = False
        if cat_collection:
            try:
                is_empty = db[cat_collection].estimated_document_count() == 0
            except Exception:
                pass
        import random as _rand
        _rand.seed(args.category)
        if is_empty:
            # Empty collection: start IMMEDIATELY so this watcher wins the
            # race for daily quota. Max 5s offset just to stagger ties across
            # multiple newly-empty watchers.
            initial_offset = _rand.randint(0, 5)
            print(f"\n[实时模式] 每 {args.interval}s 轮询 (分类 {args.category} "
                  f"首次启动-空集合, 抢占调度 offset={initial_offset}s). Ctrl+C 退出.")
        else:
            # Has data: normal stagger (0-45s) so 3-4 non-empty watchers don't
            # race each other.
            initial_offset = 10 + _rand.randint(0, min(35, max(5, args.interval - 15)))
            print(f"\n[实时模式] 每 {args.interval}s 轮询 (分类 {args.category} "
                  f"初始偏移 {initial_offset}s 避免并发). Ctrl+C 退出.")

        if initial_offset:
            try:
                time.sleep(initial_offset)
            except KeyboardInterrupt:
                return

        # First round for an EMPTY collection: blow past --since-hours so we
        # grab a full page of history (scraper.py normally caps at 24h which
        # gives ~0-20 items on a slow day). Subsequent rounds use user's
        # --since-hours as configured.
        first_round_since_override: Optional[float] = None
        if is_empty:
            first_round_since_override = 24 * 30  # 30 days = cover gaps
            print(f"[首次启动] {args.category} 历史空集, 首轮临时放宽 "
                  f"--since-hours=720 (30天) 拉满第一页历史")

        round_num = 0
        refresh_limit_streak = 0
        # 服务端日配额按 CST 午夜重置 (2026-04-22 实测). 在退避阶段检测到连
        # 续多轮 REFRESH_LIMIT 后, 算到下个 00:00 CST, 一次性 sleep 到那里,
        # 省得每 5 min 空打 API. 超过 3 连击认为当日配额确实耗尽.
        SHORT_BACKOFFS = 3      # 连击 ≤ 3 走短退避 (5/10/15 min), 也许是瞬时限流
        def _next_midnight_cst() -> float:
            """Seconds until next 00:05 Asia/Shanghai."""
            cst = timezone(timedelta(hours=8))
            now = datetime.now(cst)
            tomorrow = (now + timedelta(days=1)).replace(hour=0, minute=5, second=0, microsecond=0)
            return max(60.0, (tomorrow - now).total_seconds())
        # Token auto-refresh: swap access+refresh tokens proactively every
        # 24h to stay ahead of the 30-day JWT expiry. Also reactively on
        # SessionDead (401/403).
        last_token_refresh_at = time.time()
        TOKEN_REFRESH_INTERVAL_S = 6 * 3600   # 6h; JWT valid 30d, so 4x/day is plenty

        def _try_refresh_token() -> bool:
            """Rotate the access token via file-locked helper.

            Multiple concurrent watchers coordinate via fcntl lock on
            credentials.lock — only one actually POSTs /auth/refresh; the rest
            trust the file after a recent rotation (single-active-session).
            Returns True on session update (either freshly refreshed or
            picked up from another process's recent rotation).
            """
            nonlocal session, last_token_refresh_at
            new_access, did_refresh = refresh_with_file_lock(client_flag="pc", min_age_s=120)
            if new_access:
                session = create_session(new_access)
                last_token_refresh_at = time.time()
                msg = "refresh 成功" if did_refresh else "跟随其他进程更新"
                print(f"[refresh] ✓ {msg}, access token 已切换 (len={len(new_access)})")
                return True
            return False

        while True:
            round_num += 1
            print(f"\n{'═' * 60}\n[轮次 {round_num}] "
                  f"{datetime.now():%Y-%m-%d %H:%M:%S}\n{'═' * 60}")

            # Proactive token refresh every 6h
            if time.time() - last_token_refresh_at > TOKEN_REFRESH_INTERVAL_S:
                print("[refresh] 定期刷新 access token ...")
                _try_refresh_token()

            try:
                # First round on empty collection uses a wider --since-hours
                # to pick up multi-day history in one shot. Reset after.
                if first_round_since_override is not None:
                    saved_since = getattr(args, "since_hours", None)
                    args.since_hours = first_round_since_override
                    try:
                        summary = run_once(session, db, args)
                    finally:
                        args.since_hours = saved_since
                        first_round_since_override = None
                else:
                    summary = run_once(session, db, args)
                # If any category hit REFRESH_LIMIT this round, sleep longer:
                # 5 min × streak (capped at 30 min). Reset streak on a round
                # that completes without hitting the limit.
                got_limit = any(
                    s.get("refresh_limit") or s.get("error") == "REFRESH_LIMIT"
                    for s in (summary or {}).values()
                )
                if got_limit:
                    refresh_limit_streak = min(refresh_limit_streak + 1, 99)
                    if refresh_limit_streak <= SHORT_BACKOFFS:
                        # First few streaks: short backoff in case it's transient
                        backoff_s = 300 * refresh_limit_streak
                        print(f"[REFRESH_LIMIT 退避] streak={refresh_limit_streak} "
                              f"短退避 {backoff_s}s ({backoff_s//60} min)")
                    else:
                        # After 3 consecutive hits → definitely daily-quota exhausted.
                        # Sleep until 00:05 CST (quota reset + 5 min slack) and try
                        # again. During sleep, refresh the token at 6h boundaries
                        # so we're not using a stale session when quota resets.
                        #
                        # Priority-aware wake: if THIS watcher's collection is
                        # still empty (never got a full first batch), wake at
                        # the earliest moment (00:05 CST) so it wins the race
                        # for the fresh quota. Watchers whose collections are
                        # already populated hold off an extra few minutes so
                        # the empty ones get served first.
                        base_s = int(_next_midnight_cst())
                        if cat_collection:
                            try:
                                still_empty = (
                                    db[cat_collection].estimated_document_count() == 0
                                )
                            except Exception:
                                still_empty = False
                        else:
                            still_empty = False
                        # Non-empty watchers add 3 min per-category buffer based on
                        # name hash so they don't all wake at the same second
                        # when quota opens up.
                        if still_empty:
                            priority_delay_s = 0
                        else:
                            import random as _r2
                            _r2.seed(args.category)
                            priority_delay_s = 180 + _r2.randint(0, 240)   # 3–7 min
                        backoff_s = base_s + priority_delay_s
                        wake = datetime.now(timezone(timedelta(hours=8))) + timedelta(seconds=backoff_s)
                        tag = "(空集合, 优先抢占)" if still_empty else f"(已有数据, 延后 {priority_delay_s}s)"
                        print(f"[REFRESH_LIMIT 日限额] streak={refresh_limit_streak} "
                              f"sleep 至 {wake:%Y-%m-%d %H:%M CST} {tag}")
                    try:
                        time.sleep(backoff_s)
                    except KeyboardInterrupt:
                        break
                    _THROTTLE.reset()
                    # Post-sleep: if collection was empty, redo one wide-window
                    # round to catch multi-day history in one shot.
                    if cat_collection:
                        try:
                            if db[cat_collection].estimated_document_count() == 0:
                                first_round_since_override = 24 * 30
                                print(f"[午夜恢复] {args.category} 仍为空, "
                                      f"下一轮临时放宽 --since-hours=720")
                        except Exception:
                            pass
                    continue
                refresh_limit_streak = 0
            except KeyboardInterrupt:
                print("\n[实时模式] Ctrl+C 退出"); break
            except SessionDead as e:
                # Token expired or revoked — try auto-refresh once before
                # falling back to the human-in-the-loop path.
                print(f"[SessionDead] {e}. 尝试 refresh_token 自救...")
                if _try_refresh_token():
                    # Success: the session is rotated, retry this round.
                    print("[SessionDead] 已换新 token, 不 sleep, 立即重试")
                    continue
                print("[SessionDead] refresh_token 也失效. 需要浏览器重登.")
                break
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
