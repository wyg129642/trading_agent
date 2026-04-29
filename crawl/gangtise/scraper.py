#!/usr/bin/env python3
"""
open.gangtise.com 爬虫 (MongoDB 存储)

抓取三大数据类别:
  summary   纪要   (会议 / 投资者关系 / 公司公告 AI 纪要)
  research  研报   (券商研究报告, 含 PDF)
  chief     观点   (内资 / 外资首席观点)

使用方法:
  1. 浏览器登录 open.gangtise.com
  2. F12 → Application → Local Storage → 复制 G_token 的值 (UUID)
  3. 粘贴到 credentials.json {"token": "<G_token>"} 或通过 --auth / env GANGTISE_AUTH
  4. 启动 MongoDB (默认 mongodb://localhost:27017)
  5. 运行:
       python3 scraper.py --show-state          # 检查 checkpoint + token 健康
       python3 scraper.py --max 10              # 各类各爬 10 条小试
       python3 scraper.py                       # 全量 (各类翻到 list 尽头)
       python3 scraper.py --watch --interval 600 --resume
       python3 scraper.py --today               # 今日各类平台条数 vs 本地库
       python3 scraper.py --type summary --max 500

MongoDB 数据模型:
  summaries            — 纪要, _id = gangtise summary id (int)
  researches           — 研报, _id = rptId (string)
  chief_opinions       — 首席观点, _id = id (int)
  account              — 账户 / 元信息
  _state               — checkpoint + 日统计

参考 crawl/README.md §3-§4, 与 alphapai / jinmen 同构.
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
import urllib.parse
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
    warmup_session,
)
from ticker_tag import stamp as _stamp_ticker  # noqa: E402

# 模块级 throttle — main() 会用 CLI 覆盖
_THROTTLE: AdaptiveThrottle = AdaptiveThrottle(base_delay=3.0, jitter=2.0,
                                                burst_size=40,
                                                platform="gangtise")
_BUDGET: AccountBudget = AccountBudget("gangtise", "default", 0)
_PLATFORM = "gangtise"

# ==================== 请配置以下内容 ====================
# 浏览器 localStorage → G_token 的值（UUID 形如 c97e37da-8198-4e14-aefd-36f814c28013）
# 登录态 expires_in ≈ 10800s (3h), token 过期需重登浏览器拷贝
GANGTISE_TOKEN = "c97e37da-8198-4e14-aefd-36f814c28013"

# ==================== 以下无需修改 ====================

CREDS_FILE = Path(__file__).resolve().parent / "credentials.json"


def _load_token_from_file() -> str:
    """credentials.json 里的 token 优先, 允许飞书机器人热更新."""
    if not CREDS_FILE.exists():
        return ""
    try:
        d = json.loads(CREDS_FILE.read_text(encoding="utf-8"))
        return (d.get("token") or "").strip()
    except Exception:
        return ""


API_BASE = "https://open.gangtise.com"

# MongoDB 配置
MONGO_URI_DEFAULT = os.environ.get(
    "MONGO_URI",
    "mongodb://127.0.0.1:27018/",
)
MONGO_DB_DEFAULT = os.environ.get("MONGO_DB", "gangtise-full")
COL_ACCOUNT = "account"
COL_STATE = "_state"

# 研报 PDF 本地存放目录 (可被 --pdf-dir 或 env GANGTISE_PDF_DIR 覆盖).
# 2026-04-17: 迁移到 /home/ygwang/crawl_data/gangtise_pdfs
PDF_DIR_DEFAULT = os.environ.get(
    "GANGTISE_PDF_DIR",
    "/home/ygwang/crawl_data/gangtise_pdfs",
)

# 成功码 (不同子系统有不同成功码)
OK_CODES = {"000000", "10010000", 0, "0"}


# ==================== 网络 / 会话 ====================

def create_session(token: str) -> requests.Session:
    """构造带完整 gangtise 头的 Session. 禁用环境代理 (Clash 会 SSL-EOF CN CDN)."""
    s = requests.Session()
    s.trust_env = False  # 忽略 $HTTP(S)_PROXY, Clash 会中断 open.gangtise.com TLS
    h = headers_for_platform("gangtise")
    h.update({
        "Content-Type": "application/json",
        "Authorization": f"bearer {token}",
        "access_token": token,
    })
    s.headers.update(h)
    # Warmup: 先 GET /research SPA landing 再调 API
    warmup_session(s, "gangtise")
    return s


def api_call(session: requests.Session, method: str, path: str,
             json_body: Optional[dict] = None,
             params: Optional[dict] = None,
             retries: int = 2, timeout: int = 20,
             expected_json: bool = True) -> dict:
    """统一请求: 统一错误返回, 统一节流退避.

    - 401/403 → SessionDead (会话失效, 调用方需退出提示重登)
    - 429/5xx → 指数退避 + 尊重 Retry-After
    - 2xx 非 JSON → 原样返回 {"_raw": text, "_status": code}
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
                                          minutes=10)
                ra = parse_retry_after(r.headers.get("Retry-After"))
                _THROTTLE.on_retry(retry_after_sec=ra, attempt=attempt)
                _THROTTLE.sleep_before_next()
                last_exc = f"HTTP {r.status_code}"
                # gangtise 的 summary v2 会返回业务级 500 — 记录但不重试
                if r.status_code == 500 and path.endswith(("queryPage", "queryOpinionList")):
                    try:
                        return r.json()
                    except Exception:
                        return {"code": 500, "msg": "HTTP 500", "data": None, "status": False}
                continue
            if r.status_code != 200:
                return {"code": r.status_code, "msg": f"HTTP {r.status_code}", "data": None}
            if not expected_json:
                return {"_raw": r.text, "_status": r.status_code,
                        "_bytes": r.content, "_headers": dict(r.headers)}
            body = r.json()
            # 业务层软警告 (带 platform kwarg → 激活 _PLATFORM_SOFT_BIZ_CODES["gangtise"])
            reason = detect_soft_warning(r.status_code, body=body if isinstance(body, dict) else None,
                                          text_preview=r.text[:400] if r.text else "",
                                          cookies=dict(r.cookies),
                                          platform="gangtise")
            if reason:
                SoftCooldown.trigger(_PLATFORM, reason=reason, minutes=10)
                _THROTTLE.on_warning()
            return body
        except SessionDead:
            raise
        except (requests.RequestException, ValueError) as e:
            last_exc = e
            if attempt < retries + 1:
                _THROTTLE.on_retry(attempt=attempt)
                _THROTTLE.sleep_before_next()
    return {"code": -1, "msg": f"req_err: {last_exc}", "data": None}


def _is_ok(resp: dict) -> bool:
    """业务成功判定. gangtise 常见成功码: '000000' (application/*) / 10010000 (glory/*)."""
    if not isinstance(resp, dict):
        return False
    if resp.get("status") is True:
        return True
    code = resp.get("code")
    return code in OK_CODES


# ==================== 数据类型 / 配置 ====================
#
# 每种内容类型 (summary / research / chief) 都有同一套生命周期:
#   list(page)  → 列表项 (含预览字段)
#   detail(id)  → 详情 (研报用, 纪要 / 观点 列表已含 brief, 不需要二次拉)
#   content(id) → 正文 (纪要走 download, 研报下 PDF, 观点自带 msgText JSON)
#   key(item)   → 稳定主键
#
# TYPE_ORDER 决定跑的顺序. 研报最慢 (含 PDF), 放最后.
# ----------------------------------------------------------

TYPE_ORDER = ["summary", "research", "chief"]

# 纪要常用 source 列表 (从 summary/getSourceList 拉到)
SUMMARY_SOURCES = [100100178, 100100263, 100100262]   # 会议平台 / 公司公告 / 网络资源

# 纪要的 7 个分类 (从 summary/getClassifyList 拉到 —— 对应 UI 左侧边栏:
# 帕米尔研究 / A股会议 / 港股会议 / 美股会议 / 专家会议 / 投关活动记录 / 网络资源).
#
# 注意 queryPage 服务端每次最多返回 top 10000 条, 按 msgTime 倒序. 光靠
# 三个 source 的 union, 帕米尔 (broker=C900000031) 这种深层分类会被挤到翻不完
# 的分页里 → 分类轮询是唯一保得齐的办法. 每条 item 上会打 `classify_id` /
# `classify_name` 方便后续按类检索.
SUMMARY_CLASSIFIES: list[dict] = [
    {"id": 17, "name": "帕米尔研究",   "param": {"sourceList": [100100262], "brokerList": ["C900000031"]}},
    {"id": 11, "name": "A股会议",      "param": {"sourceList": [100100178, 100100262], "columnIdList": [98]}},
    {"id": 12, "name": "港股会议",     "param": {"sourceList": [100100178, 100100262], "columnIdList": [99]}},
    {"id": 13, "name": "美股会议",     "param": {"sourceList": [100100178, 100100262], "columnIdList": [101]}},
    {"id": 14, "name": "专家会议",     "param": {"columnIdList": [104]}},
    {"id": 15, "name": "投关活动记录", "param": {"sourceList": [100100263]}},
    {"id": 16, "name": "网络资源",     "param": {"sourceList": [100100262]}},
]

# 首席观点 — UI 下分 4 个并列 tab, 每个走不同的 endpoint+bizParams 组合.
# 端点映射由 JS bundle app.js 逆向得到 (/chief/js/app.14360064.js):
#   内资机构观点 -> /chief/v2/queryOpinionList
#   外资机构观点 -> /chief/foreign/queryOpinionList  + foreignType=researchSource
#   外资独立观点 -> /chief/foreign/queryOpinionList  + foreignType=independent
#   大V观点     -> /chief/foreign/queryOpinionList  (无 bizParams, legacy)
#
# 2026-04 实测: 3 个 foreign/* 端点返回 1000 条 (时间跨度最晚 2026-03-08);
# v2 端点返回 1000 条 (最晚 2026-01-06). 活跃度取决于用户订阅级别.
#
# body 形状: {"condition": {pageNum, pageSize, keywords:{}, industryIds:[], partyIds:[], bizParams:{...}}}
# 其中 bizParams 为空时直接省略 (sdk 会剥掉 {bizParams:{}}).
CHIEF_VARIANTS: list[dict] = [
    {"key": "domestic_institution",
     "name": "内资机构观点",
     "path": "/application/glory/chief/v2/queryOpinionList",
     "biz_params": None},
    {"key": "foreign_institution",
     "name": "外资机构观点",
     "path": "/application/glory/chief/foreign/queryOpinionList",
     "biz_params": {"foreignType": "researchSource"}},
    {"key": "foreign_independent",
     "name": "外资独立观点",
     "path": "/application/glory/chief/foreign/queryOpinionList",
     "biz_params": {"foreignType": "independent"}},
    {"key": "kol",
     "name": "大V观点",
     "path": "/application/glory/chief/foreign/queryOpinionList",
     "biz_params": None},
]

# Back-compat — some code paths still reference CHIEF_TYPES
CHIEF_TYPES = [1]


# ==================== 工具函数 ====================

def _hash_id(*parts: Any) -> str:
    raw = "|".join("" if p is None else str(p) for p in parts)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _ms_to_str(ms: Any) -> str:
    """毫秒时间戳 → 'YYYY-MM-DD HH:MM' in Asia/Shanghai. 失败返回空串."""
    if not ms:
        return ""
    try:
        ts = int(ms)
        if ts == 0:
            return ""
        return datetime.fromtimestamp(
            ts / 1000, tz=timezone(timedelta(hours=8)),
        ).strftime("%Y-%m-%d %H:%M")
    except (TypeError, ValueError, OSError):
        return ""


def _strip_html(s: str) -> str:
    """去掉 <br/> / &nbsp; 等常见 HTML 残留, 但保留换行."""
    if not s:
        return ""
    s = re.sub(r"<br\s*/?>", "\n", s, flags=re.I)
    s = re.sub(r"<[^>]+>", "", s)
    s = html.unescape(s)
    return s.strip()


try:
    from markdownify import markdownify as _markdownify
    _HAS_MARKDOWNIFY = True
except ImportError:
    _HAS_MARKDOWNIFY = False

_BLOCK_HTML_RE = re.compile(
    r"<\s*(h[1-6]|p|ul|ol|li|table|tr|td|th|div|section|article|blockquote|strong|em|span)\b",
    re.I,
)
# 纪要正文里的时间戳锚点 (跳转到对应音频段) — 渲染时就是噪声, 转成简洁的 [N] 标记.
_MEETING_NUM_RE = re.compile(
    r"<span\s+class=['\"]meeting_summary_num['\"][^>]*>\s*(\d+)\s*</span>",
    re.I,
)


def _summary_text_to_md(text: str) -> str:
    """Normalize Gangtise summary body to clean Markdown.

    Since 2026-04-21 the `summary/download` endpoint started returning HTML
    fragments (<h1>/<p>/<ul>/<li>/<span>) for newer docs while older ones stay
    as plain text. Pass HTML through markdownify so MarkdownRenderer doesn't
    show raw tags. Plain text passes through untouched.
    """
    if not text:
        return ""
    if not _BLOCK_HTML_RE.search(text):
        return text  # 旧格式: 纯文本, 不动
    s = _MEETING_NUM_RE.sub(r"[\1]", text)
    if _HAS_MARKDOWNIFY:
        try:
            md = _markdownify(s, heading_style="ATX", bullets="-",
                              strip=["script", "style"])
            md = re.sub(r"\n{3,}", "\n\n", md).strip()
            return md
        except Exception:
            pass
    return _strip_html(s)


_SAFE_FNAME_RE = re.compile(r'[\\/:*?"<>|\x00-\x1f]')


def _safe_filename(name: str, max_len: int = 160) -> str:
    cleaned = _SAFE_FNAME_RE.sub("_", name or "").strip().strip(".")
    return cleaned[:max_len] or "untitled"


# Cross-collection dedup: chief_opinions vs researches.
# Gangtise pushes the same external broker report to both feeds (different
# _id schemes — rptId vs c<msgId>), so within-collection dedup misses it.
# `_norm_title` is written into both researches and chief_opinions docs and
# matched on (organization, release_time_ms, _norm_title) — strict three-key.
import unicodedata as _unicodedata

_NORM_TITLE_PUNCT_RE = re.compile(r"[\s　\W_]+", re.UNICODE)


def _normalize_chief_title(s: Any) -> str:
    if not s:
        return ""
    s = _unicodedata.normalize("NFKC", str(s)).lower().strip()
    return _NORM_TITLE_PUNCT_RE.sub("", s)


def _is_quote_noise(text: str) -> bool:
    """True if the OCR'd text looks like a stock-quote screen noise dump
    rather than real article body. Heuristics tuned 2026-04-29 after finding
    that ~86% of ``is_attachment=True`` chief_opinions actually carry
    high-quality WeChat article OCR in ``parsed_msg.content`` (broker research
    summaries, 数百字), with only a small minority being price-screen junk.

    Considered noise if:
      - very short (<60 chars), OR
      - high digit + symbol density typical of K-line / quote captions
        AND short enough to plausibly be a screenshot of a price panel.
    """
    if not text:
        return True
    s = text.strip()
    if len(s) < 60:
        return True
    digit_ratio = sum(1 for ch in s if ch.isdigit()) / max(len(s), 1)
    symbol_chars = "%¥$.,()[]+-/:"
    symbol_ratio = sum(1 for ch in s if ch in symbol_chars) / max(len(s), 1)
    if len(s) < 300 and digit_ratio > 0.25 and symbol_ratio > 0.05:
        return True
    if "成交量" in s and len(s) < 300 and digit_ratio > 0.2:
        return True
    return False


def _is_title_echo(text: str, title: str) -> bool:
    """True if `text` is just a (truncated/exact) echo of `title`, not a body.

    Used by dump_chief's empty-content guard to drop "WeChat link-only" items
    where parsed_msg.description repeats the title verbatim or is a strict
    substring (~Sigma Lithium 2025Q4 季度报告 case). Real analyst bodies
    are >80 chars and don't sit inside the title.
    """
    if not text:
        return True
    t_norm = _normalize_chief_title(text)
    title_norm = _normalize_chief_title(title)
    if not t_norm or not title_norm:
        return False
    if t_norm == title_norm:
        return True
    if len(text) <= 80 and (t_norm in title_norm or title_norm.startswith(t_norm)):
        return True
    return False


def _find_dup_research(db, organization: str, release_time_ms: Optional[int],
                       title: str) -> Optional[dict]:
    if not (organization and release_time_ms and title):
        return None
    norm = _normalize_chief_title(title)
    if not norm:
        return None
    return db["researches"].find_one(
        {
            "organization": organization,
            "release_time_ms": release_time_ms,
            "_norm_title": norm,
        },
        {"_id": 1},
    )


# ==================== 列表抓取 (每种类型一个) ====================

def fetch_summary_list(session, page: int, size: int,
                       classify_param: Optional[dict] = None) -> dict:
    """纪要列表. data.summList 为条目数组, 按 msgTime 倒序.

    classify_param 来自 SUMMARY_CLASSIFIES[i]["param"] —— 传入则轮询单个分类
    (sourceList / columnIdList / brokerList), 不传则回退到三个 source 的 union
    (兼容调用方未升级的路径).
    """
    body = {"pageNum": page, "pageSize": size}
    if classify_param:
        body.update(classify_param)
    else:
        body["sourceList"] = SUMMARY_SOURCES
    return api_call(session, "POST", "/application/summary/queryPage", json_body=body)


# 当 --sweep-today 生效时,由 main() 塞入 {"startDate": <ms>, "endDate": <ms>}
# java 后端 ResearchRequest["startDate"] 类型是 java.lang.Long → 必须毫秒时间戳.
_RESEARCH_DATE_OVERRIDE: dict = {}


def fetch_research_list(session, page: int, size: int) -> dict:
    """研报列表 — 真正的分页接口.

    **2026-04-22 完整字段对齐** (Playwright 抓包 UI 实际调用):
    body 必须是 ES 风格 from/size + 全套 filter lists, 服务端才会走"真实搜索"
    路径. 缺少 `source/exactStockList/realTime/curated/typeList` 字段会
    fallback 到 top-10. 实测 size=1000 可以一次拉一整页.

    - source=[0,1]   同时抓国内 + 外资 (分别对应 source=[0] / [1])
    - searchType=1   ES 的 match-all + time sort
    - realTime/curated=null  不做额外过滤

    单日发布 ~1000 篇 (内资 233 + 外资 772), 老的 pageNum 方案只能抓 top-10,
    造成 77/1005. 这个新方式直接翻页即可.

    **2026-04-23** 加 `--sweep-today --date` 支持: startDate/endDate 接受毫秒
    epoch (`java.lang.Long`). 设置单日区间后,当天发布 ~1000 篇也能完整翻页.
    """
    from_offset = max(0, (page - 1) * size)
    body = {
        "from": from_offset, "size": size,
        "searchType": 1, "kw": "",
        "startDate": None, "endDate": None,
        "rptIds": [], "industryList": [], "columnList": [],
        "orgList": [], "orgTypeList": [], "honorTypeList": [],
        "authorList": [], "rateList": [], "changeList": [],
        "source": [0, 1],        # 国内 + 外资 (UI 默认值)
        "exactStockList": [],
        "realTime": None, "curated": None,
        "typeList": [],
    }
    if _RESEARCH_DATE_OVERRIDE:
        body.update(_RESEARCH_DATE_OVERRIDE)
    return api_call(
        session, "POST",
        "/application/glory/research/v2/queryByCondition",
        json_body=body,
    )


def fetch_chief_list(session, page: int, size: int,
                     variant: dict | None = None,
                     chief_type: int = 1) -> dict:
    """首席观点列表 — 对应 UI 4 个 tab.

    **2026-04-22 重大修正**: 通过 Playwright 拦截 UI 真实请求, 发现 body 是
    Elasticsearch 风格而不是传统 pageNum/pageSize. 用老 body 服务器返回的是
    "推荐 top 1000" (没时效性); 新 body 才能按时间倒序拿到今日条目:

        {
          "condition": {
            "keywords": {}, "matches": {},
            "from": <offset>, "size": <batch>,
            "sort": {"msgTime": 1},
            "range": {"msgTime": {}},
            "filter": {"isOpn": 1},
            "bizParams": {<per-tab>}   # 可选
          }
        }

    关键字段:
      - filter.isOpn=1 → 只要"观点"类条目 (UI "观点" tab 等价)
      - sort.msgTime=1 → 时间倒序 (服务器约定 1=desc, 实测最新在前)
      - from/size → 传统 offset 分页, size≤1000
      - bizParams.foreignType 决定外资 institution/independent 分流
    """
    if variant is None:
        variant = next(v for v in CHIEF_VARIANTS if v["key"] == "foreign_independent")

    # 把传入的 pageNum + pageSize 转成 ES 的 from + size
    from_offset = max(0, (page - 1) * size)
    condition: dict = {
        "keywords": {}, "matches": {},
        "from": from_offset, "size": size,
        "sort": {"msgTime": 1},
        "range": {"msgTime": {}},
        "filter": {"isOpn": 1},
    }
    if variant.get("biz_params"):
        condition["bizParams"] = dict(variant["biz_params"])
    return api_call(session, "POST", variant["path"],
                    json_body={"condition": condition})


# ==================== 内容抓取 ====================

def _fetch_summary_text_via_s3(session, token: str, msg_text_url: str) -> str:
    """绕开 /summary/download 的每日配额 (超额后只返回 ~500 字试读 + 403 903301) —
    直接打 /storage/s3/download.

    2026-04-21 实测:
      - bucket=20002 (config.js 里的 PRIVATEOBSMAPPINGID) 是正确的 mapping
      - **必须带 Range 头** (`Range: bytes=0-`), 否则 s3 层返回 400 "range error"
      - 带 Range 后返回 206 Partial Content 但含完整文件 (HW OBS 特性)
      - 拉到的 txt 完整正文, 不受 quota 限制

    研报 PDF 也走同样机制, 只是 requests 对大文件流式读取自动发了 Range.
    """
    if not msg_text_url or not token:
        return ""
    url = (f"{API_BASE}/application/download/storage/s3/download/20002/"
           f"{msg_text_url}")
    try:
        r = session.get(url, params={"access_token": token},
                        headers={"Range": "bytes=0-"},
                        timeout=30)
    except Exception:
        return ""
    if r.status_code not in (200, 206):
        return ""
    try:
        txt = r.content.decode("utf-8", errors="replace").strip()
    except Exception:
        return ""
    return txt if len(txt) > 200 else ""


def fetch_summary_text(session, summary_id, msg_text_url: str,
                       token: str = "") -> str:
    """纪要正文 txt.

    两条获取路径 (按优先级):
      1. `/application/summary/download` — quota-gated, 超过日额返回 ~500 字试读
      2. `/application/download/storage/s3/download/<bucket>/<path>` — s3 直连,
         研报外资 PDF 已证明走此路径不走 quota

    这里先走标准 endpoint. 如果拿到的文本疑似截断 + 有 token 可用,
    再试 s3 直连看能不能补上全文.
    """
    if not msg_text_url:
        return ""
    primary = ""
    try:
        resp = api_call(session, "GET", "/application/summary/download",
                        params={"id": summary_id, "path": msg_text_url},
                        expected_json=False)
        raw = resp.get("_raw") or ""
        primary = raw.strip() if isinstance(raw, str) else ""
    except SessionDead as e:
        msg = str(e)
        # 403 的几种 per-document 限制 (不是 session 失效, 跳过此条就行):
        #   - 白名单限制 (部分券商的专属内容)
        #   - 903301 当日下载配额用尽 (restrict=60/current=60)
        per_doc_markers = ("白名单", "903301", "quota", "restrict")
        if "403" in msg and any(m in msg for m in per_doc_markers):
            primary = ""
        else:
            raise

    # s3 直连回补条件:
    #   - /summary/download 直接 403 → primary 为空
    #   - 或返回了疑似试读 (< 1500 字且不以句号结尾)
    looks_truncated = bool(
        primary and len(primary) < 1500
        and primary.strip()[-1:] not in "。！？.!?》」）)]}"
    )
    if token and (not primary or looks_truncated):
        s3_text = _fetch_summary_text_via_s3(session, token, msg_text_url)
        if s3_text and len(s3_text) > len(primary):
            return s3_text
    return primary


def fetch_research_detail(session, rpt_id: str) -> dict:
    """研报详情 GET /application/glory/research/<rptId>. 返回含 author 明细 / aflScr / aflBlock 的 dict."""
    if not rpt_id:
        return {}
    resp = api_call(session, "GET", f"/application/glory/research/{rpt_id}")
    if not _is_ok(resp):
        return {"_err": {"code": resp.get("code"), "msg": resp.get("msg")}}
    data = resp.get("data")
    if isinstance(data, list) and data:
        return data[0]
    if isinstance(data, dict):
        return data
    return {}


def download_research_pdf(session, relpath: str, token: str, dest: Path,
                          timeout: int = 60,
                          max_retries: int = 3) -> tuple[int, Optional[str]]:
    """下载研报 PDF 到本地文件. 返回 (bytes_written, err).

    URL: /application/download/storage/s3/download/20002/<relpath>?access_token=<token>
    校验首字节 %PDF, 失败自动重试.
    """
    if not relpath:
        return 0, "no relpath"
    # Some brokers set `file` to an external URL (WeChat article / third-party
    # permalink) instead of an S3 relative path — these can't be PDF'd via
    # our download endpoint. Short-circuit with a clear marker so the walker
    # doesn't retry them forever.
    if relpath.startswith(("http://", "https://")):
        return 0, "external_url"
    if dest.exists() and dest.stat().st_size > 0:
        return dest.stat().st_size, ""
    dest.parent.mkdir(parents=True, exist_ok=True)
    encoded = urllib.parse.quote(relpath, safe="/")
    url = (f"{API_BASE}/application/download/storage/s3/download/20002/{encoded}"
           f"?access_token={token}")
    tmp = dest.with_suffix(dest.suffix + ".part")
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            with session.get(url, stream=True, timeout=timeout) as r:
                if is_auth_dead(r.status_code):
                    return 0, f"auth_dead HTTP {r.status_code}"
                if r.status_code not in (200, 206):
                    last_err = f"http_{r.status_code}"
                    if r.status_code == 429 or 500 <= r.status_code < 600:
                        _THROTTLE.on_retry(attempt=attempt)
                        _THROTTLE.sleep_before_next()
                        continue
                    return 0, last_err
                it = r.iter_content(65536)
                first = next(it, b"")
                if not first.startswith(b"%PDF"):
                    prefix = first[:40].decode("utf-8", errors="replace")
                    return 0, f"not PDF (prefix={prefix!r})"
                written = 0
                with tmp.open("wb") as f:
                    f.write(first); written += len(first)
                    for chunk in it:
                        if chunk:
                            f.write(chunk); written += len(chunk)
            tmp.replace(dest)
            return written, None
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


# ==================== 主键 + 文档组装 ====================

def dedup_id_summary(item: dict) -> str:
    rid = item.get("id")
    if rid:
        return f"s{rid}"
    return "s" + _hash_id(item.get("title"), item.get("msgTime"))


def dedup_id_research(item: dict) -> str:
    rid = item.get("rptId") or item.get("id")
    if rid:
        return str(rid)
    return _hash_id("r", item.get("title"), item.get("pubTime"))


def dedup_id_chief(item: dict) -> str:
    rid = item.get("id") or item.get("msgId")
    if rid:
        return f"c{rid}"
    return "c" + _hash_id(item.get("username"), item.get("msgTime"), item.get("msgText", "")[:64])


def _item_stocks(item: dict) -> list[dict]:
    """纪要 / 研报 共用的股票字段归一化."""
    raw = item.get("stock") or []
    out = []
    if isinstance(raw, list):
        for s in raw:
            if not isinstance(s, dict):
                continue
            out.append({
                "code": s.get("gts_code") or s.get("gtsCode") or "",
                "scr_id": s.get("scr_id") or s.get("scrId") or "",
                "name": s.get("scr_abbr") or s.get("scrAbbr") or s.get("name") or "",
            })
    # 研报的 aflScr 也带 stock
    afl = item.get("aflScr")
    if isinstance(afl, dict):
        for d in (afl.get("detail") or []):
            if isinstance(d, dict) and d.get("gtsCode"):
                out.append({
                    "code": d["gtsCode"],
                    "scr_id": d.get("scrId") or "",
                    "name": d.get("scrAbbr") or afl.get("display") or "",
                    "rating": (d.get("rate") or {}).get("name") if isinstance(d.get("rate"), dict) else None,
                    "rating_change": (d.get("change") or {}).get("name") if isinstance(d.get("change"), dict) else None,
                })
    # 去重 by code
    seen: set[str] = set()
    uniq: list[dict] = []
    for s in out:
        key = s.get("code") or s.get("name")
        if key and key not in seen:
            seen.add(key)
            uniq.append(s)
    return uniq


def _item_industries(item: dict) -> list[str]:
    names: list[str] = []
    for b in (item.get("block") or []):
        if isinstance(b, dict) and b.get("block_name"):
            names.append(b["block_name"])
    afl = item.get("aflBlock")
    if isinstance(afl, dict):
        disp = afl.get("display")
        if disp:
            names.append(disp)
        for dd in (afl.get("detail") or []):
            for bl in (dd.get("block") or []):
                if isinstance(bl, dict) and bl.get("name"):
                    names.append(bl["name"])
    # unique preserve order
    seen: set[str] = set()
    out: list[str] = []
    for n in names:
        if n and n not in seen:
            seen.add(n)
            out.append(n)
    return out


def _looks_truncated(content: str, brief: str, full_text_ok: bool) -> bool:
    """Detect when content_md is NOT the real full transcript.

    Two failure modes seen on Gangtise:
      1. /summary/download returned 403 (quota exhausted) → scraper falls back
         to the list's brief. Detectable by ``content == brief`` and
         ``full_text_ok == False``.
      2. /summary/download returned 200 OK but with a ~400-600 字 "试读" —
         the text is cut mid-sentence without closing punctuation.

    A genuinely short summary (company Q&A ~300 字) should NOT be flagged.
    Those typically end with a proper period/！/？.
    """
    if not content:
        return False
    n = len(content)
    # Mode 1: fallback-to-brief.
    if not full_text_ok and brief and content == brief:
        return True
    if n > 1500:
        return False
    last_char = content.rstrip()[-1] if content.strip() else ""
    # 中英句末标点 → 文本看起来有完整结尾, 不是截断
    if last_char in "。！？.!?》」）)]}":
        return False
    # 非标点结尾 + 短 + 来自试读 → 截断
    return True


def dump_summary(session, db, item: dict, force: bool = False,
                 token: str = "") -> tuple[str, dict]:
    """纪要入库. collection=summaries, _id=s<id>.

    每日额度用尽时 server 会返回试读 (~500 字截断). 如果 DB 里已有完整正文,
    不覆盖; 如果本次也是截断, 标记 content_truncated=True 让下次再试.
    """
    col = db["summaries"]
    did = dedup_id_summary(item)

    ex = col.find_one({"_id": did},
                      {"_id": 1, "content_md": 1, "content_truncated": 1, "stats": 1})
    if ex and not force:
        ex_content = ex.get("content_md") or ""
        # 已有完整 (非截断) 正文 → 跳过
        if ex_content and not ex.get("content_truncated"):
            return "skipped", ex.get("stats") or {"content_chars": len(ex_content)}

    # 列表项 brief 是截断版; 通过 msgText.url 拉完整正文 txt
    brief = _strip_html(item.get("brief") or "")
    full_text = ""
    msg_texts = item.get("msgText") or []
    if isinstance(msg_texts, list):
        for mt in msg_texts:
            if not isinstance(mt, dict):
                continue
            url = mt.get("url")
            ext = (mt.get("extension") or "").lower()
            if url and ext in ("", ".txt"):
                full_text = fetch_summary_text(session, item.get("id"), url, token=token)
                break
    content = _summary_text_to_md(full_text or brief)
    full_text_ok = bool(full_text)
    truncated = _looks_truncated(content, brief, full_text_ok)

    # 已有 DB 记录且之前也是截断, 本次仍然截断 & 更短 → 保留更长的
    if ex and ex.get("content_truncated") and truncated:
        old = ex.get("content_md") or ""
        if len(old) >= len(content):
            return "skipped", ex.get("stats") or {"content_chars": len(old)}

    release_ms = item.get("msgTime") or item.get("summTime") or 0
    release_time = _ms_to_str(release_ms)

    doc = {
        "_id": did,
        "category": "summary",
        "raw_id": item.get("id"),
        "msg_id": item.get("msgId"),
        "title": item.get("title") or "",
        "release_time": release_time,
        "release_time_ms": int(release_ms) if release_ms else None,
        "source_id": item.get("source"),
        "source_name": item.get("sourceName") or "",
        "classify_id": item.get("_classify_id"),
        "classify_name": item.get("_classify_name") or "",
        "column_names": item.get("columnNames") or [],
        "organization": "",
        "guest": item.get("guest") or "",
        "researcher": item.get("researcher") or "",
        "location": item.get("location") or "",
        "stocks": _item_stocks(item),
        "industries": _item_industries(item),
        "has_audio": bool(item.get("canSeeAudio")),
        "duration_ms": item.get("duration"),
        "brief_md": brief,
        "content_md": content,
        "content_truncated": truncated,
        "msg_text": msg_texts,
        "list_item": item,
        "web_url": f"https://open.gangtise.com/summary/?id={item.get('id')}#/detail",
        "stats": {"content_chars": len(content), "brief_chars": len(brief),
                  "truncated": truncated},
        "crawled_at": datetime.now(timezone.utc),
    }
    _stamp_ticker(doc, "gangtise", col)
    col.replace_one({"_id": did}, doc, upsert=True)
    return "added", doc["stats"]


def dump_research(session, db, item: dict, pdf_dir: Path,
                  token: str, download_pdf: bool = True,
                  force: bool = False) -> tuple[str, dict]:
    """研报入库. collection=researches, _id=rptId (string)."""
    col = db["researches"]
    did = dedup_id_research(item)

    if not force:
        ex = col.find_one({"_id": did},
                          {"_id": 1, "pdf_local_path": 1, "pdf_size_bytes": 1, "stats": 1})
        if ex and ex.get("pdf_local_path") and ex.get("pdf_size_bytes", 0) > 0:
            return "skipped", ex.get("stats") or {}
        if ex and not download_pdf:
            return "skipped", ex.get("stats") or {}

    detail = fetch_research_detail(session, item.get("rptId"))
    # 2026-04-29: gangtise 平台返回 brief (纯文本) + formattedBrief (HTML 版).
    # 部分外资研报 brief 在中途断句 (e.g. 德意志银行 Google Cloud Next 那条
    # brief=2690 字结尾 "from: a) its", formattedBrief=3198 字含 b)/c)). 优先
    # 用 formattedBrief 后 strip HTML, 不够再退到 brief.
    fmt_raw = item.get("formattedBrief") or detail.get("formattedBrief") or ""
    plain_raw = item.get("brief") or detail.get("brief") or ""
    fmt_text = _strip_html(fmt_raw) if fmt_raw else ""
    plain_text = _strip_html(plain_raw)
    brief = fmt_text if len(fmt_text) > len(plain_text) else plain_text

    release_ms = item.get("pubTime") or detail.get("pubTime") or 0
    release_time = _ms_to_str(release_ms)

    rel_path_raw = item.get("file") or detail.get("file") or ""
    # 2026-04-26 schema fix: gangtise 平台对部分 "research" 把外链 URL (主要是
    # 公众号 mp.weixin.qq.com/...) 也塞到 file 字段, 以前直接落到 pdf_rel_path
    # 跟真正的 CDN 相对路径混淆 (4016/41994 ≈ 10% 是 WeChat URL, pdf_local_path
    # 全空). 现在拆开:
    #   - 真 PDF (相对路径 + .pdf 扩展) → pdf_rel_path
    #   - 任何 http(s):// URL → external_url
    # 同时 user 要求暂时不抓公众号文章 (低质量), 直接 skip 不入库.
    is_external_url = bool(rel_path_raw) and rel_path_raw.startswith(("http://", "https://"))
    is_wechat = is_external_url and "mp.weixin.qq.com" in rel_path_raw
    if is_wechat:
        # 完全跳过, 不入库 (避免污染 researches 集合).
        # 返回 "skipped" 让上游 page-counter 把它算进 skipped (line 1311 严等于检查).
        return "skipped", {"brief_chars": 0, "pages": 0, "pdf_size": 0, "skip_reason": "wechat_url"}

    if is_external_url:
        # 非微信外链 — 保留但放到 external_url, 不再借 pdf_rel_path
        rel_path = ""
        external_url = rel_path_raw
    else:
        rel_path = rel_path_raw
        external_url = ""

    pdf_local = ""
    pdf_size = 0
    pdf_err = ""
    if download_pdf and rel_path and (item.get("extension") or ".pdf").lower() == ".pdf":
        fname = _safe_filename(Path(rel_path).name or (item.get("rptId") or "report") + ".pdf")
        ym = (release_time or "unknown")[:7] or "unknown"
        dest = pdf_dir / ym / fname
        pdf_size, err = download_research_pdf(session, rel_path, token, dest)
        pdf_err = err or ""
        if pdf_size > 0:
            pdf_local = str(dest)

    author = item.get("author") or detail.get("author") or {}
    authors = []
    if isinstance(author, dict):
        for a in (author.get("detail") or []):
            if isinstance(a, dict) and a.get("name"):
                authors.append({"id": a.get("id"), "name": a["name"]})
        if not authors and author.get("display"):
            authors = [{"name": n.strip()} for n in str(author["display"]).split(",") if n.strip()]

    doc = {
        "_id": did,
        "category": "research",
        "raw_id": item.get("id"),
        "rpt_id": item.get("rptId"),
        "title": item.get("title") or "",
        "_norm_title": _normalize_chief_title(item.get("title") or ""),
        "release_time": release_time,
        "release_time_ms": int(release_ms) if release_ms else None,
        "rpt_date": item.get("rptDate"),
        "organization": item.get("issuerStmt") or "",
        "issuer": item.get("issuer"),
        "rpt_type": item.get("rptType"),
        "rpt_type_name": item.get("rptTypeStmt") or "",
        "authors": authors,
        "author_display": author.get("display") if isinstance(author, dict) else "",
        "stocks": _item_stocks(item),
        "industries": _item_industries(item),
        "pages": item.get("page") or detail.get("page") or 0,
        "size_bytes": item.get("size") or detail.get("size") or 0,
        "head_party": bool(item.get("headParty")),
        "foreign_party": bool(item.get("foreignParty")),
        "first_coverage": bool(item.get("firstCoverage")),
        "brief_md": brief,
        "content_md": brief,  # 研报 brief 已经是核心观点; PDF 才是全文
        "pdf_rel_path": rel_path,
        "pdf_local_path": pdf_local,
        "pdf_size_bytes": pdf_size,
        "pdf_download_error": pdf_err,
        "external_url": external_url,  # 非 PDF 外链 (WeChat 已 skip; 其它外链放这)
        "list_item": item,
        "detail_result": detail,
        "web_url": f"https://open.gangtise.com/research/?id={item.get('rptId')}#/ResearchDetails",
        "stats": {
            "brief_chars": len(brief),
            "pages": item.get("page") or detail.get("page") or 0,
            "pdf_size": pdf_size,
        },
        "crawled_at": datetime.now(timezone.utc),
    }
    # Truncated guard: brief 空 + PDF 没下到 → 整条无内容. brief 是核心观点
    # (研报 list 自带), 一般非空; 真到这里大多是平台数据缺失.
    if (not brief.strip()) and pdf_size <= 0:
        return "skipped_empty", doc["stats"]
    _stamp_ticker(doc, "gangtise", col)
    col.replace_one({"_id": did}, doc, upsert=True)
    # Reverse cross-collection cleanup: if the matching chief_opinion was
    # ingested earlier (chief feed dropped first; research arrived later),
    # soft-delete it now so users only see one card per logical document.
    # Soft delete (not hard) per user policy; sweep_deleted_docs picks the
    # tombstone up on the next daily Milvus pass.
    if doc.get("organization") and doc.get("release_time_ms") and doc.get("_norm_title"):
        try:
            db["chief_opinions"].update_many(
                {
                    "organization": doc["organization"],
                    "release_time_ms": doc["release_time_ms"],
                    "_norm_title": doc["_norm_title"],
                    "deleted": {"$ne": True},
                },
                {"$set": {
                    "deleted": True,
                    "_deleted_at": datetime.now(timezone.utc),
                    "_deleted_reason": f"dup_research:{did}",
                }},
            )
        except PyMongoError:
            pass  # best-effort; missing index just means slower query
    status = "added" if (pdf_size or not download_pdf) else "added_no_pdf"
    return status, doc["stats"]


def dump_chief(session, db, item: dict, force: bool = False,
               variant: dict | None = None) -> tuple[str, dict]:
    """首席观点入库. collection=chief_opinions, _id=c<id>.

    msgText 是个 JSON 字符串 (key=title/description/…), 解析并提炼.
    `variant` tags the doc with which of the 4 UI tabs produced it.
    """
    col = db["chief_opinions"]
    did = dedup_id_chief(item)

    if not force:
        existing = col.find_one({"_id": did}, {"_id": 1, "chief_variant": 1})
        if existing:
            # If this variant already known, skip. If item came from a
            # NEW variant (e.g. cross-listed between tabs), upsert the
            # variant list so we track tab membership.
            if variant and existing.get("chief_variant") != variant.get("key"):
                known = existing.get("chief_variants") or [existing.get("chief_variant")]
                if variant["key"] not in known:
                    known = list(known) + [variant["key"]]
                    col.update_one({"_id": did}, {"$set": {"chief_variants": known}})
            return "skipped", {}

    raw_msg = item.get("msgText") or ""
    parsed: dict = {}
    if isinstance(raw_msg, str) and raw_msg.strip().startswith("{"):
        try:
            parsed = json.loads(raw_msg)
        except Exception:
            parsed = {}
    elif isinstance(raw_msg, dict):
        parsed = raw_msg

    # 微信机器人转发的图片/附件消息 (msgType=1 + extension=.jpg/.png/.pdf 之类)
    # 原始 parsed.title 是图片文件名 (hash.jpg), parsed.content 是图片 OCR 出来的
    # 股票行情页噪声 (价格 / MA 线 / 成交量数字). 直接照搬进 DB 是 UI "首席观点
    # 格式丑" 的直接源头. 特殊处理:
    #   - title: 改用 "[图片/PDF] 发布者 · 时间"
    #   - content_md: 不要 OCR 噪声, 留空
    #   - attachment_url: 保留图片/PDF 路径 (供 UI 渲染或后续 OCR 替换)
    ext = (parsed.get("extension") or "").lower()
    is_attachment = ext in (".jpg", ".jpeg", ".png", ".gif", ".webp", ".pdf",
                            ".bmp", ".svg")
    # 某些条目 title 本身就是 hash.jpg 即使 extension 为空
    raw_title = (parsed.get("title") or item.get("title") or "").strip()
    import re as _re
    if _re.fullmatch(r'[a-f0-9]{20,}\.(?:jpg|jpeg|png|gif|webp|pdf|bmp|svg)',
                     raw_title, _re.IGNORECASE):
        is_attachment = True

    if is_attachment:
        poster = (item.get("username") or item.get("partyName") or "匿名").strip()
        release_ms = item.get("msgTime") or 0
        title_fmt = datetime.fromtimestamp(
            release_ms / 1000, tz=timezone(timedelta(hours=8)),
        ).strftime("%Y-%m-%d %H:%M") if release_ms else ""
        kind = "PDF" if ext == ".pdf" else "图片"
        title = f"[{kind}] {poster}" + (f" · {title_fmt}" if title_fmt else "")
        # 2026-04-29: parsed.content 实际上 ~86% 是高质量微信文章 OCR
        # (券商小作文 数百字), 只有少量是行情屏截图噪声 (价格/MA 数字). 之前
        # 整段丢导致 1054/1056 attachment chief 的 content_md 为空, UI 全显示
        # "[平台未提供文字摘要]". 改为只过滤明显的行情噪声 (_is_quote_noise),
        # 真 OCR 正文进 content_md.
        description = _strip_html((parsed.get("description") or "").strip())
        ocr_raw = _strip_html((parsed.get("content") or "").strip())
        ocr = "" if _is_quote_noise(ocr_raw) else ocr_raw
        body = ocr or description
        attachment_url = parsed.get("url") or ""
        source_url = ""
    else:
        title = raw_title
        description = _strip_html((parsed.get("description") or "").strip())
        content = _strip_html((parsed.get("content") or "").strip())
        # 2026-04-29: title fallback removed. Letting body fall back to title
        # produced "empty" chief cards (content_md == title, no real body) that
        # bypassed the empty-content guard below. Now: body stays empty when
        # both content and description are missing, and the guard skips them.
        body = content or description
        attachment_url = ""
        # Link-only chief items (msgText carries only title + a WeChat URL,
        # content/description both null) are now treated as garbage per user
        # decision (2026-04-29) — skipped by the empty-content guard. The
        # source_url is still captured for any non-empty chief that happens
        # to also link out, but on its own it's not enough to keep the doc.
        raw_url = (parsed.get("url") or "").strip()
        source_url = raw_url if raw_url.startswith("http") else ""

    release_ms = item.get("msgTime") or 0
    release_time = _ms_to_str(release_ms)

    # rsrchDir = "|机械|" → ["机械"]
    def _bars_to_list(v):
        if not isinstance(v, str):
            return []
        return [x for x in v.strip("|").split("|") if x]

    doc = {
        "_id": did,
        "category": "chief",
        "raw_id": item.get("id"),
        "msg_id": item.get("msgId"),
        "title": title,
        "_norm_title": _normalize_chief_title(raw_title),
        "release_time": release_time,
        "release_time_ms": int(release_ms) if release_ms else None,
        "organization": item.get("partyName") or "",
        "party_id": item.get("partyId"),
        "analyst": item.get("username") or "",
        "analyst_id": item.get("userId"),
        "phone": item.get("phone"),
        "research_directions": _bars_to_list(item.get("rsrchDir")),
        "industry_ids": _bars_to_list(item.get("rsrchSector")),
        "msg_type": item.get("msgType"),
        "description_md": description,
        "content_md": body,
        "brief_md": description[:500] if description else body[:500],
        "is_attachment": is_attachment,
        "attachment_type": ext.lstrip(".") if is_attachment else "",
        "attachment_url": attachment_url,
        "source_url": source_url,
        "parsed_msg": parsed,
        "list_item": item,
        "web_url": f"https://open.gangtise.com/chief/?id={item.get('id')}#/detail",
        "chief_variant": variant.get("key") if variant else None,
        "chief_variant_name": variant.get("name") if variant else None,
        "stats": {
            "content_chars": len(body),
            "description_chars": len(description),
            "is_attachment": is_attachment,
        },
        "crawled_at": datetime.now(timezone.utc),
    }
    # Empty-content guard. 2026-04-29 unified rule (replacing earlier
    # attachment-exempt version):
    #   - 任何条目, 不管 is_attachment, content_md / description_md 必须有
    #     真实正文 (非 title-echo). attachment 现在也走 OCR 提取
    #     (parsed.content), 行情噪声被 _is_quote_noise 过滤掉, 真 OCR 文章正文
    #     会进 body — 所以 attachment 在这里和普通条目共用同一个判定.
    #   - 没有真实正文的条目整条跳 (不入库). UI 上的 "[平台未提供文字摘要]"
    #     就是这种条目造成的噪声.
    body_real = bool((body or "").strip()) and not _is_title_echo(body, title)
    desc_real = bool((description or "").strip()) and not _is_title_echo(description, title)
    if not body_real and not desc_real:
        return "skipped_empty", doc["stats"]
    # Cross-collection dedup: drop chief that duplicates an existing research
    # doc. Strict 3-key (organization, release_time_ms, _norm_title); see
    # `_find_dup_research`. Attachments stay (raw_title is a hash filename
    # so the lookup naturally returns nothing — but we early-exit anyway).
    if not is_attachment:
        dup = _find_dup_research(
            db, doc["organization"], doc["release_time_ms"], raw_title,
        )
        if dup:
            return "skipped_dup_research", {"dup_of": dup["_id"]}
    _stamp_ticker(doc, "gangtise", col)
    col.replace_one({"_id": did}, doc, upsert=True)
    return "added", doc["stats"]


# ==================== 分页抓取 ====================

def _items_from_list_resp(resp: dict, content_type: str) -> list[dict]:
    if not _is_ok(resp):
        return []
    data = resp.get("data")
    if content_type == "summary":
        if isinstance(data, dict):
            return data.get("summList") or []
        return []
    # research / chief: data 是条目数组
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return data.get("list") or data.get("records") or []
    return []


def _item_time_ms(item: dict, content_type: str) -> Optional[int]:
    if content_type == "summary":
        ms = item.get("msgTime") or item.get("summTime")
    elif content_type == "research":
        ms = item.get("pubTime")
    else:  # chief
        ms = item.get("msgTime")
    try:
        return int(ms) if ms else None
    except (TypeError, ValueError):
        return None


def fetch_items_paginated(session, content_type: str, max_items: Optional[int],
                          page_size: int, stop_at_id: Optional[str] = None,
                          stop_before_ms: Optional[int] = None,
                          make_dedup=None) -> list[dict]:
    """按 content_type 分页抓取, 直到 list 空 / 命中 stop 条件.

    - stop_at_id: 命中上次 top 即停 (增量模式)
    - stop_before_ms: 条目时间 < 该毫秒即停 (--since-hours)

    summary 走 SUMMARY_CLASSIFIES 的 7 个分类轮询 (每类自带 top-10000 上限),
    相同 item 由下游 make_dedup_id 去重. 其他类型保持原逻辑.
    """
    # Per-content-type "classify" loop so we poll every UI tab separately.
    #   summary: 7 个分类 (SUMMARY_CLASSIFIES)
    #   chief:   4 个 tab    (CHIEF_VARIANTS)
    #   research: single feed
    if content_type == "summary":
        classifies: list[Optional[dict]] = [
            {"id": c["id"], "name": c["name"], "param": c["param"]}
            for c in SUMMARY_CLASSIFIES
        ]
    elif content_type == "chief":
        classifies = [dict(v) for v in CHIEF_VARIANTS]
    else:
        classifies = [None]

    all_items: list[dict] = []
    seen_keys: set = set()  # 跨 classify 去重, key=raw id 字符串

    for classify in classifies:
        label = f"{content_type}/{classify['name']}" if classify else content_type
        page = 1
        empty_streak = 0
        class_hits = 0
        while True:
            try:
                if content_type == "summary":
                    resp = fetch_summary_list(session, page, page_size,
                                              classify_param=classify["param"] if classify else None)
                elif content_type == "research":
                    resp = fetch_research_list(session, page, page_size)
                else:
                    resp = fetch_chief_list(session, page, page_size, variant=classify)
            except SessionDead:
                raise
            except Exception as e:
                tqdm.write(f"  [{label} p{page}] 请求异常: {e}")
                break

            items = _items_from_list_resp(resp, content_type)
            if not items:
                empty_streak += 1
                msg = resp.get("msg") or resp.get("errordesc") or ""
                code = resp.get("code")
                tqdm.write(f"  [{label} p{page}] 空 (code={code} msg={msg}) streak={empty_streak}")
                if empty_streak >= 2:
                    break
                _THROTTLE.sleep_before_next()
                page += 1
                continue
            empty_streak = 0

            hit_known = hit_old = False
            new_this = 0
            for it in items:
                # Stamp classify origin so dump_* can persist it.
                if classify:
                    if content_type == "chief":
                        it.setdefault("_chief_variant", classify["key"])
                        it.setdefault("_chief_variant_name", classify["name"])
                    else:
                        it.setdefault("_classify_id", classify["id"])
                        it.setdefault("_classify_name", classify["name"])
                if stop_at_id and make_dedup and make_dedup(it) == stop_at_id:
                    hit_known = True
                    continue
                if stop_before_ms is not None:
                    ts = _item_time_ms(it, content_type)
                    if ts is not None and ts < stop_before_ms:
                        hit_old = True
                        continue
                # Cross-classify dedup (same msgId appears in multiple classifies).
                key = str(it.get("id") or it.get("msgId") or id(it))
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                all_items.append(it)
                new_this += 1
                class_hits += 1
                if max_items and len(all_items) >= max_items:
                    return all_items[:max_items]

            tqdm.write(f"  [{label} p{page}] +{new_this}/{len(items)} "
                       f"(累计 {len(all_items)}) hit_known={hit_known} hit_old={hit_old}")

            if hit_known or hit_old:
                break
            if len(items) < page_size:
                break
            page += 1
            _THROTTLE.sleep_before_next()
        if classify:
            tqdm.write(f"  [{label}] 分类完成, 新增 {class_hits} (累计跨分类 {len(all_items)})")
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
    "summary": dedup_id_summary,
    "research": dedup_id_research,
    "chief": dedup_id_chief,
}

_DUMP_FUNC = {
    "summary": lambda sess, db, item, args: dump_summary(sess, db, item, force=args.force,
                                                         token=args.auth),
    "research": lambda sess, db, item, args: dump_research(
        sess, db, item, Path(args.pdf_dir), args.auth,
        download_pdf=not args.skip_pdf, force=args.force),
    "chief": lambda sess, db, item, args: dump_chief(
        sess, db, item, force=args.force,
        variant=next((v for v in CHIEF_VARIANTS
                      if v["key"] == item.get("_chief_variant")), None)),
}

_COL_NAME = {"summary": "summaries", "research": "researches", "chief": "chief_opinions"}
_LABEL = {"summary": "纪要", "research": "研报", "chief": "观点"}


def run_type_streaming(session, db, content_type: str, args) -> dict:
    """Gangtise streaming: 按 classify×page 顺序逐页 fetch→dump→checkpoint.

    checkpoint 字段 backfill_deep:
      {"classify_idx": N, "page": M}
    空页 / partial 页推进到下一个 classify; 全部 classify 跑完则重置 {0,1}.
    """
    cfg_label = _LABEL[content_type]
    dedup = _DEDUP_FUNC[content_type]
    col = db[_COL_NAME[content_type]]
    print(f"\n[STREAM {cfg_label}/{content_type}] collection={_COL_NAME[content_type]}")

    if content_type == "summary":
        classifies: list = [{"id": c["id"], "name": c["name"], "param": c["param"]} for c in SUMMARY_CLASSIFIES]
    elif content_type == "chief":
        classifies = [dict(v) for v in CHIEF_VARIANTS]
    else:
        classifies = [None]

    state = load_state(db, content_type) or {}
    ck = state.get("backfill_deep") or {}
    start_ci = int(ck.get("classify_idx") or 0)
    start_page = int(ck.get("page") or 1)
    print(f"[stream] resume from classify_idx={start_ci} page={start_page} (total {len(classifies)} classifies)")

    cap = cap_from_args(args)
    added = skipped = failed = 0
    first_top: Optional[str] = None
    total_seen = 0
    seen_keys: set = set()

    for ci in range(start_ci, len(classifies)):
        classify = classifies[ci]
        label = f"{content_type}/{classify['name']}" if classify else content_type
        page = start_page if ci == start_ci else 1
        empty_streak = 0
        while True:
            if cap.exhausted() or _BUDGET.exhausted():
                print(f"  [antibot] daily-cap 达到, 停")
                break
            try:
                if content_type == "summary":
                    resp = fetch_summary_list(session, page, args.page_size,
                                              classify_param=classify["param"] if classify else None)
                elif content_type == "research":
                    resp = fetch_research_list(session, page, args.page_size)
                else:
                    resp = fetch_chief_list(session, page, args.page_size, variant=classify)
            except SessionDead:
                raise
            except Exception as e:
                print(f"  [{label} p{page}] 请求异常: {e}")
                break

            items = _items_from_list_resp(resp, content_type)
            if not items:
                empty_streak += 1
                print(f"  [{label} p{page}] 空 streak={empty_streak}")
                if empty_streak >= 2:
                    break
                _THROTTLE.sleep_before_next()
                page += 1
                continue
            empty_streak = 0

            for it in items:
                if classify:
                    if content_type == "chief":
                        it.setdefault("_chief_variant", classify["key"])
                        it.setdefault("_chief_variant_name", classify["name"])
                    else:
                        it.setdefault("_classify_id", classify["id"])
                        it.setdefault("_classify_name", classify["name"])
                key = str(it.get("id") or it.get("msgId") or id(it))
                if key in seen_keys:
                    continue
                seen_keys.add(key)

            if first_top is None:
                first_top = dedup(items[0])

            page_added = page_skipped = page_failed = 0
            for it in items:
                if cap.exhausted() or _BUDGET.exhausted():
                    break
                key = str(it.get("id") or it.get("msgId") or id(it))
                did = dedup(it)
                title = (it.get("title") or (it.get("msgText") or {}).get("title", "") if isinstance(it.get("msgText"), dict) else it.get("title") or "")[:60]
                was_skip = False
                try:
                    status, info = _DUMP_FUNC[content_type](session, db, it, args)
                    # `status` ∈ {skipped, skipped_empty, skipped_dup_research,
                    # added, added_no_pdf}. Anything starting with "skipped"
                    # never hit Mongo and counts as skipped, not added.
                    if status.startswith("skipped"):
                        skipped += 1; page_skipped += 1; was_skip = True
                        if status != "skipped":
                            tag = status.removeprefix("skipped_")
                            print(f"  · [{did[:16]}] {title}  跳过({tag})")
                    else:
                        added += 1; page_added += 1
                        cap.bump(); _BUDGET.bump()
                        print(f"  ✓ [{did[:16]}] {title}")
                except SessionDead:
                    raise
                except Exception as e:
                    failed += 1; page_failed += 1
                    print(f"  ✗ [{did[:16]}] {title}  ERR: {type(e).__name__}: {e}")
                total_seen += 1
                if not was_skip:
                    _THROTTLE.sleep_before_next()
                if args.max and total_seen >= args.max:
                    break

            save_state(db, content_type,
                       backfill_deep={"classify_idx": ci, "page": page + 1},
                       backfill_last_page_at=datetime.now(timezone.utc),
                       in_progress=True)
            print(f"  [{label} p{page}] +{page_added} ={page_skipped} ✗{page_failed}")

            if args.max and total_seen >= args.max:
                break
            if len(items) < args.page_size:
                print(f"  [{label} p{page}] partial, 本 classify 到底")
                break
            page += 1
            _THROTTLE.sleep_before_next()

        if args.max and total_seen >= args.max:
            break

    # 完整过完所有 classify: 重置 checkpoint
    else:
        save_state(db, content_type, backfill_deep={"classify_idx": 0, "page": 1},
                   backfill_last_run_end_at=datetime.now(timezone.utc))
        if first_top is not None:
            save_state(db, content_type, top_dedup_id=first_top)

    save_state(db, content_type, in_progress=False,
               last_run_end_at=datetime.now(timezone.utc),
               last_run_stats={"added": added, "skipped": skipped, "failed": failed})
    total = col.estimated_document_count()
    print(f"  完成: +{added} ={skipped} ✗{failed}  当前 {_COL_NAME[content_type]} 总数: {total}")
    return {"added": added, "skipped": skipped, "failed": failed}


def run_type(session, db, content_type: str, args) -> dict:
    if getattr(args, "stream_backfill", False):
        return run_type_streaming(session, db, content_type, args)

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

    print(f"[列表] max={args.max or '全部'} page_size={args.page_size}")
    items = fetch_items_paginated(session, content_type,
                                  max_items=args.max,
                                  page_size=args.page_size,
                                  stop_at_id=stop_id,
                                  stop_before_ms=stop_ms,
                                  make_dedup=dedup)
    if not items:
        print(f"[{cfg_label}] 无新条目 (token 失效或服务器无返回)")
        return {"added": 0, "skipped": 0, "failed": 0}

    new_top_id = dedup(items[0])
    added = skipped = failed = 0
    cap = cap_from_args(args)

    pbar = tqdm(items, desc=cfg_label, unit="条", dynamic_ncols=True,
                bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}] {postfix}")
    for it in pbar:
        if cap.exhausted() or _BUDGET.exhausted():
            tqdm.write(f"  [antibot] 达到 daily-cap={cap.max_items}, 本轮停 (防风控)")
            break
        did = dedup(it)
        title = (it.get("title") or (it.get("msgText") or {}).get("title", "") if isinstance(it.get("msgText"), dict) else it.get("title") or "")[:60]
        if content_type == "chief" and not title:
            # chief 的 title 在 msgText 里 (JSON string)
            raw = it.get("msgText") or ""
            if isinstance(raw, str) and raw.startswith("{"):
                try:
                    title = (json.loads(raw).get("title") or "")[:60]
                except Exception:
                    pass

        was_skip = False
        try:
            status, info = _DUMP_FUNC[content_type](session, db, it, args)
            # `status` ∈ {skipped, skipped_empty, skipped_dup_research, added,
            # added_no_pdf}. Anything starting with "skipped" never hit Mongo.
            if status.startswith("skipped"):
                skipped += 1
                was_skip = True
                if status == "skipped":
                    tqdm.write(f"  · [{did[:16]}] {title}  已存在")
                else:
                    tag = status.removeprefix("skipped_")
                    tqdm.write(f"  · [{did[:16]}] {title}  跳过({tag})")
            else:
                added += 1
                cap.bump(); _BUDGET.bump()
                parts = []
                if info.get("content_chars"):
                    parts.append(f"content={info['content_chars']}字")
                if info.get("brief_chars") and not info.get("content_chars"):
                    parts.append(f"brief={info['brief_chars']}字")
                if info.get("pdf_size"):
                    parts.append(f"PDF={info['pdf_size']:,}B")
                if info.get("pages"):
                    parts.append(f"{info['pages']}页")
                suffix = "  " + "  ".join(parts) if parts else ""
                tqdm.write(f"  ✓ [{did[:16]}] {title}{suffix}")
        except SessionDead:
            raise
        except Exception as e:
            failed += 1
            tqdm.write(f"  ✗ [{did[:16]}] {title}  ERR: {type(e).__name__}: {e}")

        pbar.set_postfix_str(f"+{added} ={skipped} ✗{failed}")
        save_state(db, content_type, last_dedup_id=did,
                   last_processed_at=datetime.now(timezone.utc),
                   in_progress=True)
        # DB dedup hits made no remote call — skip the 3-5s throttle.
        if not was_skip:
            _THROTTLE.sleep_before_next()
    pbar.close()

    save_state(db, content_type,
               top_dedup_id=new_top_id, in_progress=False,
               last_run_end_at=datetime.now(timezone.utc),
               last_run_stats={"added": added, "skipped": skipped, "failed": failed})

    total = col.estimated_document_count()
    print(f"  完成: 新增 {added} / 跳过 {skipped} / 失败 {failed}")
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
            print("  → 浏览器重登 open.gangtise.com, 更新 credentials.json 里的 token.")
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
    # 岗底斯 release_time 是 Asia/Shanghai 壁钟,--today 必须用 BJ TZ 对齐.
    if args.date:
        day_start = datetime.strptime(args.date, "%Y-%m-%d").replace(tzinfo=_BJ_TZ)
        target = args.date
    else:
        day_start = datetime.now(_BJ_TZ).replace(hour=0, minute=0, second=0, microsecond=0)
        target = day_start.strftime("%Y-%m-%d")
    day_end = day_start.replace(hour=23, minute=59, second=59)
    start_ms = int(day_start.timestamp() * 1000)
    end_ms = int(day_end.timestamp() * 1000)

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
                if t == "summary":
                    resp = fetch_summary_list(session, page, args.page_size)
                elif t == "research":
                    resp = fetch_research_list(session, page, args.page_size)
                else:
                    resp = fetch_chief_list(session, page, args.page_size)
            except Exception as e:
                print(f"  [{t} p{page}] 失败: {e}")
                break
            items = _items_from_list_resp(resp, t)
            if not items:
                break
            scanned += len(items)
            for it in items:
                ts = _item_time_ms(it, t)
                if ts is None:
                    continue
                if ts < start_ms:
                    stop = True
                    break
                if ts <= end_ms:
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
        print(f"  {_LABEL[t]:>6s} ({t}): 平台 {overall[t]['platform_count']:>4d}  "
              f"入库 {in_db:>4d}  缺 {overall[t]['missing']:>4d}")

    overall["scanned_at"] = datetime.now(timezone.utc)
    db[COL_STATE].replace_one({"_id": f"daily_{target}"},
                              {"_id": f"daily_{target}", **overall},
                              upsert=True)
    print(f"\n已存 {COL_STATE} (_id=daily_{target})")
    return overall


# ==================== account / 元数据 ====================

ACCOUNT_ENDPOINTS = [
    ("user-account", "GET", "/application/userCenter/userCenter/api/account"),
    ("summary-source-list", "GET", "/application/summary/getSourceList"),
    ("summary-classify-list", "GET", "/application/summary/getClassifyList"),
    ("chief-industry-group", "POST", "/application/glory/chief/industryGroup"),
    ("research-category-tree", "POST", "/application/glory/research/v2/categoryTree"),
    ("research-query-parameters", "POST", "/application/glory/research/v2/queryParameters"),
]


def dump_account(session, db) -> None:
    print("\n[账户] 抓取账户级 / 元数据接口...")
    col = db[COL_ACCOUNT]
    now = datetime.now(timezone.utc)
    for name, method, path in ACCOUNT_ENDPOINTS:
        resp = api_call(session, method, path, json_body={} if method == "POST" else None)
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
        description="open.gangtise.com 纪要 / 研报 / 观点 爬虫 (MongoDB)")
    p.add_argument("--type", choices=["all", *TYPE_ORDER], default="all",
                   help=f"指定类型 (默认 all). 可选: {', '.join(TYPE_ORDER)}")
    p.add_argument("--max", type=int, default=None,
                   help="最多爬 N 条 (每类). 默认翻页到尽头")
    p.add_argument("--page-size", type=int, default=40,
                   help="每页大小 (默认 40, 研报最多 40)")
    p.add_argument("--force", action="store_true",
                   help="强制重爬已入库的内容")
    p.add_argument("--stream-backfill", action="store_true",
                   help="流式回填: 每抓完一页立即入库 + deep_page checkpoint")
    p.add_argument("--resume", action="store_true",
                   help="增量模式: 遇到上次 top_dedup_id 即停止分页")
    p.add_argument("--watch", action="store_true",
                   help="实时模式: 定时轮询. Ctrl+C 退出")
    p.add_argument("--interval", type=int, default=600,
                   help="实时模式轮询间隔秒数 (默认 600)")
    p.add_argument("--since-hours", type=float, default=None,
                   help="仅抓过去 N 小时内的内容 (按 msgTime/pubTime)")
    p.add_argument("--show-state", action="store_true",
                   help="打印各类 checkpoint + token 健康检查 后退出")
    p.add_argument("--reset-state", action="store_true",
                   help="清除所有 crawler_* checkpoint (保留 daily_* 统计)")
    p.add_argument("--today", action="store_true",
                   help="扫各类平台当日条数 vs 本地库, 结果存 _state")
    p.add_argument("--date", metavar="YYYY-MM-DD", default=None,
                   help="配合 --today 或 --sweep-today 指定日期")
    p.add_argument("--sweep-today", action="store_true",
                   help="日扫模式 (仅 --type research): 注入 startDate=endDate="
                        "--date 对应毫秒 epoch, 禁用 top-dedup / since-hours 早停. "
                        "必须配合 --date YYYY-MM-DD.")
    p.add_argument("--skip-pdf", action="store_true",
                   help="研报模式不下载 PDF 文件")
    p.add_argument("--pdf-dir", default=PDF_DIR_DEFAULT,
                   help=f"研报 PDF 存放目录 (默认 {PDF_DIR_DEFAULT})")
    p.add_argument("--clean", choices=TYPE_ORDER, default=None,
                   help="清空指定类型集合 + checkpoint 后退出 (不删 PDF)")
    p.add_argument("--auth",
                   default=_load_token_from_file() or os.environ.get("GANGTISE_AUTH") or GANGTISE_TOKEN,
                   help="G_token (优先级: credentials.json > env GANGTISE_AUTH > 脚本内 GANGTISE_TOKEN)")
    p.add_argument("--mongo-uri", default=MONGO_URI_DEFAULT,
                   help=f"MongoDB URI (默认 {MONGO_URI_DEFAULT})")
    p.add_argument("--mongo-db", default=MONGO_DB_DEFAULT,
                   help=f"MongoDB 数据库名 (默认 {MONGO_DB_DEFAULT})")
    # 反爬节流 — default_cap 2026-04-25 500→0: 实时档不再数量闸
    add_antibot_args(p, default_base=3.0, default_jitter=2.0,
                     default_burst=40, default_cap=0, platform="gangtise")
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
    # Cross-collection dedup (chief_opinions ↔ researches): 严格三键
    # (organization, release_time_ms, _norm_title) → compound + _norm_title
    # indexes on both, plus a `deleted` index on chief_opinions for the
    # soft-delete `$ne True` filter that every consumer uses.
    for cname in ("researches", "chief_opinions"):
        col = db[cname]
        col.create_index([("organization", 1), ("release_time_ms", 1)],
                         name="org_release_time_ms")
        col.create_index("_norm_title")
    db["chief_opinions"].create_index("deleted", sparse=True)
    print(f"[Mongo] 已连接 {uri} -> db: {dbname}")
    return db


def show_state(session, db) -> None:
    print("=" * 60)
    print("gangtise Checkpoint")
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
    # token 健康 — 调一个真实业务接口
    print()
    r = api_call(session, "GET", "/application/userCenter/userCenter/api/account")
    if _is_ok(r):
        d = r.get("data") or {}
        print(f"[token] ✓ uid={d.get('uid')} user={d.get('userName')} "
              f"company={d.get('companyName')} level={d.get('level')}")
    else:
        print(f"[token] ✗ code={r.get('code')} msg={r.get('msg')}")


def main():
    args = parse_args()
    if not args.auth:
        print("错误: 未提供 G_token. 用 --auth / env GANGTISE_AUTH 传入, "
              "或编辑脚本顶部 GANGTISE_TOKEN.")
        sys.exit(1)

    global _THROTTLE, _BUDGET
    _THROTTLE = throttle_from_args(args, platform="gangtise")
    # G_token 是 UUID, 没有内嵌 uid → 用 hash 当 account_id
    import hashlib as _hl
    _account_id = "h_" + _hl.md5((args.auth or "").encode()).hexdigest()[:12]
    _BUDGET = budget_from_args(args, account_id=_account_id, platform="gangtise")
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
              f"({n_state} 条). 本地 PDF 保留.")
        return

    if args.today:
        count_today(session, db, args)
        return

    # --sweep-today: 把 --date 换成毫秒 epoch 区间,注入到 fetch_research_list
    # 的 body 里。Watch 模式每轮不跨天时可复用同一区间;跨天的话 watch 轮询里
    # 应在轮次开始重算(这里简化:仅在主进程启动时设置一次,日切请在外部重启)。
    if args.sweep_today:
        if args.type != "research":
            print("[sweep-today] 仅 --type research 支持 (当前 --type=%s);忽略" % args.type)
        else:
            date_str = args.date or datetime.now(_BJ_TZ).strftime("%Y-%m-%d")
            day_start = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=_BJ_TZ)
            day_end = day_start + timedelta(days=1) - timedelta(milliseconds=1)
            start_ms = int(day_start.timestamp() * 1000)
            end_ms = int(day_end.timestamp() * 1000)
            _RESEARCH_DATE_OVERRIDE.clear()
            _RESEARCH_DATE_OVERRIDE["startDate"] = start_ms
            _RESEARCH_DATE_OVERRIDE["endDate"] = end_ms
            print(f"[sweep-today] research startDate={start_ms} ({date_str} 00:00) "
                  f"endDate={end_ms} ({date_str} 23:59:59.999) — 禁 resume / since-hours 早停")
            # 关掉早停: resume / since-hours 会因 top-id / ts 比较提前退出全日扫描
            args.resume = False
            args.since_hours = None

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
