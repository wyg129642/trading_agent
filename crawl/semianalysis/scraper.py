#!/usr/bin/env python3
"""SemiAnalysis (newsletter.semianalysis.com) Substack 爬虫.

抓 SemiAnalysis 研究 newsletter 的所有 posts:
  - archive list:  GET /api/v1/archive?sort=new&search=&offset=N&limit=12
  - post detail:   GET /api/v1/posts/by-id/<post_id>

写入 MongoDB funda DB 的 `semianalysis_posts` collection (u_spider 无权建新
DB, 所以 co-host 到 funda — 同 sentimentrader_indicators 的做法). Checkpoint
走独立 `_state_semianalysis` collection, 跟 funda 自身的 _state 隔离.

鉴权:
  - 匿名可抓到 free 全文 + paid preview (大约 170 KB HTML).
  - credentials.json 里放 substack cookie (整串 document.cookie, 含
    `substack.sid=...`), 付费内容 body_html 可拿到全文. 字段为 "cookie".
  - 解锁 cookie 不可用时自动降级到匿名, 不中断.

网络:
  - SemiAnalysis 走 Cloudflare/美西 CDN, 本机直连超时, 必须走 Clash
    127.0.0.1:7890. 默认从 HTTP_PROXY / HTTPS_PROXY env 拿代理, 没配就按
    127.0.0.1:7890 兜底. 命令行 --proxy 可覆盖.

使用方法:
  python3 scraper.py --show-state             # 凭证 + checkpoint
  python3 scraper.py --max 5                  # 先抓 5 条试水
  python3 scraper.py --resume                 # 增量模式
  python3 scraper.py --watch --resume --interval 600  # 实时模式
  python3 scraper.py --today                  # 今日对齐统计

环境变量:
  MONGO_URI      (默认 远端 u_spider)
  MONGO_DB       (默认 funda — co-host)
  HTTP_PROXY / HTTPS_PROXY  (默认 http://127.0.0.1:7890)

依赖 crawl/antibot.py (共享节流栈) + crawl/ticker_tag.py (ticker 富化).
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
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

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

# ==================== 常量 ====================

PLATFORM = "semianalysis"

BASE_URL = "https://newsletter.semianalysis.com"
ARCHIVE_PATH = "/api/v1/archive"
POST_BY_ID_PATH = "/api/v1/posts/by-id"   # + /<id>
POST_BY_SLUG_PATH = "/api/v1/posts"       # + /<slug>  (fallback)

CREDS_FILE = Path(__file__).resolve().parent / "credentials.json"

# Co-host 到 funda DB (u_spider 无权建新 DB; 同 sentimentrader_indicators 模式)
MONGO_URI_DEFAULT = os.environ.get(
    "MONGO_URI",
    "mongodb://u_spider:prod_X5BKVbAc@192.168.31.176:35002/?authSource=admin",
)
MONGO_DB_DEFAULT = os.environ.get("MONGO_DB", "funda")
COL_POSTS = "semianalysis_posts"
COL_STATE = "_state_semianalysis"
COL_ACCOUNT = "_state_semianalysis"   # account meta goes into the same state collection (namespaced by _id)

# US CDN — 必须走 Clash 代理 (本机直连 TCP 超时).
DEFAULT_PROXY = os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY") \
    or "http://127.0.0.1:7890"

DEFAULT_TIMEOUT = 30.0
DEFAULT_MAX_RETRIES = 3
ARCHIVE_PAGE_SIZE = 12   # Substack SPA 默认

# 模块级 throttle — main() 用 CLI 参数覆盖.
_THROTTLE: AdaptiveThrottle = AdaptiveThrottle(
    base_delay=3.0, jitter=2.0, burst_size=40, platform=PLATFORM)
_BUDGET: AccountBudget = AccountBudget(PLATFORM, "default", 0)


# ==================== 凭证 ====================

def _load_cookie_from_file() -> str:
    """credentials.json 里的 cookie 字段 (整串 document.cookie 或只含
    `substack.sid=...`). 匿名用户直接返空串."""
    if not CREDS_FILE.exists():
        return ""
    try:
        d = json.loads(CREDS_FILE.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"[warn] credentials.json 解析失败: {e}")
        return ""
    return (d.get("cookie") or d.get("token") or "").strip()


def _account_id_from_cookie(cookie: str) -> str:
    """substack.sid 是主会话 cookie; hash 它作为 AccountBudget 键.
    匿名用户用固定 'anon' 键, 所有匿名 watcher 共享一个桶 (等同 IP-rate)."""
    if not cookie:
        return "anon"
    # 尝试提 substack.sid
    m = re.search(r"substack\.sid=([^;\s]+)", cookie)
    key = m.group(1) if m else cookie
    return "u_" + hashlib.md5(key.encode()).hexdigest()[:12]


# ==================== HTTP session ====================

def create_session(cookie: str, proxy: Optional[str] = None) -> requests.Session:
    """建一个 SemiAnalysis 专用 session.

    - 走 Clash 代理 (美西 CDN, 直连超时)
    - UA / Accept-Language / sec-ch-ua 对齐 US Chrome (en-US + macOS / Win)
    - cookie 原样注入, 匿名留空
    """
    s = requests.Session()
    # 走 antibot.headers_for_platform, 但 platform 必须已注册 — 先 fallback 到
    # "funda" (en-US Windows) 如果 semianalysis 未注册 (避免启动期 KeyError).
    hdrs = headers_for_platform(PLATFORM if PLATFORM in _SUPPORTED_PLATFORMS() else "funda")
    hdrs["Accept"] = "application/json, text/plain, */*"
    hdrs["Referer"] = f"{BASE_URL}/archive"
    hdrs["Origin"] = BASE_URL
    if cookie:
        hdrs["Cookie"] = cookie
    s.headers.update(hdrs)
    # 代理: 默认走 Clash
    proxy = proxy or DEFAULT_PROXY
    if proxy:
        s.proxies = {"http": proxy, "https": proxy}
    s.trust_env = False   # proxies 已显式设, 不吃 env (避免二次代理)
    return s


def _SUPPORTED_PLATFORMS() -> set:
    """哪些 platform 在 antibot._PLATFORM_HEADERS 里注册了 — 运行时探测,
    避免 semianalysis 未注册时 create_session 崩."""
    try:
        from antibot import _PLATFORM_HEADERS   # type: ignore
        return set(_PLATFORM_HEADERS.keys())
    except Exception:
        return set()


# ==================== API 封装 ====================

def api_get(session: requests.Session, path: str, params: Optional[dict] = None,
            retries: int = DEFAULT_MAX_RETRIES, timeout: float = DEFAULT_TIMEOUT,
            what: str = "") -> Any:
    """GET 请求, 返回解析好的 JSON (可能是 dict 或 list).

    - 401/403 → SessionDead (匿名只要 free posts 仍 200, 不会触发)
    - 429 / 5xx → 尊重 Retry-After, 指数退避, 最多重试 `retries` 次
    - detect_soft_warning → SoftCooldown.trigger
    """
    last_err: Optional[Exception] = None
    url = f"{BASE_URL}{path}"
    for attempt in range(retries + 1):
        try:
            r = session.get(url, params=params, timeout=timeout,
                            allow_redirects=True)
        except requests.RequestException as e:
            last_err = e
            _THROTTLE.on_retry()
            time.sleep(min(2 ** attempt + 1, 15))
            continue

        status = r.status_code
        # 401/403 永久死
        if is_auth_dead(status):
            body = r.text[:400]
            raise SessionDead(
                f"{what or path} → HTTP {status}. 可能 cookie 失效或 IP 被封. body={body}")

        # 429/5xx 退避重试 + 触发软冷却
        if status == 429 or 500 <= status < 600:
            ra = parse_retry_after(r.headers.get("Retry-After"))
            reason = detect_soft_warning(status_code=status,
                                          body=None,
                                          text_preview=r.text[:400],
                                          cookies=dict(r.cookies),
                                          platform=PLATFORM)
            if reason:
                SoftCooldown.trigger(PLATFORM, reason=reason,
                                     minutes=45 if status == 429 else 30)
            wait = ra if ra is not None else min(2 ** attempt + 2, 30)
            _THROTTLE.on_retry()
            if attempt < retries:
                print(f"  [retry {attempt+1}/{retries}] {what or path} "
                      f"HTTP {status}, 等 {wait:.1f}s", flush=True)
                time.sleep(wait)
                continue
            raise RuntimeError(f"{what or path} HTTP {status} after {retries} retries")

        # 其它非 2xx
        if not (200 <= status < 300):
            body = r.text[:400]
            raise RuntimeError(f"{what or path} HTTP {status}: {body}")

        # 解析 JSON
        ct = (r.headers.get("Content-Type") or "").lower()
        if "json" not in ct:
            # 极少数 Cloudflare challenge 可能跳 HTML, 按软警告处理
            body = r.text[:400]
            if "Just a moment" in body or "cf-chl" in body.lower():
                SoftCooldown.trigger(PLATFORM, reason="waf_cookie:cf_challenge",
                                     minutes=60)
                raise RuntimeError(f"{what or path} Cloudflare challenge (HTML)")
            raise RuntimeError(f"{what or path} 非 JSON 响应 ct={ct}: {body}")
        try:
            data = r.json()
        except Exception as e:
            raise RuntimeError(f"{what or path} JSON 解析失败: {e}")

        # body 层软警告 (Substack 基本不会命中, 保险起见)
        reason = detect_soft_warning(status_code=200, body=data,
                                      cookies=dict(r.cookies),
                                      platform=PLATFORM)
        if reason:
            SoftCooldown.trigger(PLATFORM, reason=reason, minutes=45)
            _THROTTLE.on_warning()

        return data

    raise RuntimeError(f"{what or path} 全部重试失败: {last_err}")


def fetch_archive(session: requests.Session, offset: int,
                  limit: int = ARCHIVE_PAGE_SIZE) -> List[dict]:
    """拉 archive 一页. 返回 list of post stubs."""
    data = api_get(session, ARCHIVE_PATH,
                   params={"sort": "new", "search": "",
                           "offset": offset, "limit": limit},
                   what=f"archive offset={offset}")
    if not isinstance(data, list):
        raise RuntimeError(f"archive 响应不是 list, got {type(data).__name__}")
    return data


def fetch_post(session: requests.Session, post_id: int) -> dict:
    """拉单条 post 详情. 返回 {post, publication, publicationSettings} 的 post
    子对象 (不关心 publication meta)."""
    data = api_get(session, f"{POST_BY_ID_PATH}/{post_id}",
                   what=f"post by-id={post_id}")
    if isinstance(data, dict) and "post" in data:
        return data["post"]
    # 极少数情况 Substack 直接返回 post
    return data


def fetch_publication_info(session: requests.Session) -> dict:
    """顺手拉一次 pub 元数据 (付费/免费策略, subscriber 数等), 用 /archive?limit=1
    + 首 post 的 by-id 响应里也能拿到, 这里单独 call 便于 show-state / account 记录."""
    data = api_get(session, ARCHIVE_PATH,
                   params={"sort": "new", "search": "",
                           "offset": 0, "limit": 1},
                   what="publication meta (via archive[0])")
    if not data:
        return {}
    head = data[0] if isinstance(data, list) else {}
    # 再取 post detail 拿 publication 字段
    try:
        pid = head.get("id")
        if pid:
            resp = api_get(session, f"{POST_BY_ID_PATH}/{pid}",
                           what=f"pub meta via post {pid}")
            if isinstance(resp, dict):
                pub = resp.get("publication") or {}
                return {
                    "publication_id": pub.get("id"),
                    "name": pub.get("name"),
                    "subdomain": pub.get("subdomain"),
                    "custom_domain": pub.get("custom_domain"),
                    "copyright_text": pub.get("copyright"),
                    "total_subscribers": pub.get("total_subscribers"),
                    "paid_subscribers": pub.get("paid_subscribers"),
                    "sampled_head": {k: head.get(k) for k in
                                     ("id", "slug", "title", "post_date", "audience")},
                }
    except Exception as e:
        return {"error": str(e), "sampled_head": head}
    return {"sampled_head": head}


# ==================== 文档构造 ====================

_RE_TAG = re.compile(r"<[^>]+>")
_RE_WS = re.compile(r"[ \t]+")
_RE_NL = re.compile(r"\n\s*\n\s*\n+")


def html_to_markdown(src: str) -> str:
    """把 Substack body_html 转成轻量 Markdown (保留标题/粗体/链接/列表/引用).

    这不是完整的 HTML → Markdown 实现, 而是按 Substack bundle 的主要输出
    结构挨个替换. 够在 chat LLM 和 MarkdownRenderer 里读得通顺. 不依赖第三方
    html2text / markdownify 库 (agent 环境没装).
    """
    if not src:
        return ""
    s = src
    # figure / img — 留下文字说明
    s = re.sub(r"<figure[^>]*>", "\n\n", s, flags=re.I)
    s = re.sub(r"</figure>", "\n\n", s, flags=re.I)
    s = re.sub(r'<img[^>]*alt="([^"]*)"[^>]*>', r'![\\1]()', s, flags=re.I)
    s = re.sub(r"<img[^>]*>", "", s, flags=re.I)
    s = re.sub(r"<figcaption[^>]*>", "\n_", s, flags=re.I)
    s = re.sub(r"</figcaption>", "_\n", s, flags=re.I)
    # 标题
    for i in (1, 2, 3, 4, 5, 6):
        s = re.sub(rf"<h{i}[^>]*>", f"\n\n{'#' * i} ", s, flags=re.I)
        s = re.sub(rf"</h{i}>", "\n\n", s, flags=re.I)
    # 段落
    s = re.sub(r"<p[^>]*>", "\n\n", s, flags=re.I)
    s = re.sub(r"</p>", "\n", s, flags=re.I)
    # <br>
    s = re.sub(r"<br\s*/?>", "\n", s, flags=re.I)
    # 粗体 / 斜体
    s = re.sub(r"<(strong|b)[^>]*>", "**", s, flags=re.I)
    s = re.sub(r"</(strong|b)>", "**", s, flags=re.I)
    s = re.sub(r"<(em|i)[^>]*>", "*", s, flags=re.I)
    s = re.sub(r"</(em|i)>", "*", s, flags=re.I)
    # 链接
    s = re.sub(r'<a[^>]*href="([^"]+)"[^>]*>(.*?)</a>',
               r"[\\2](\\1)", s, flags=re.I | re.S)
    # 列表
    s = re.sub(r"<li[^>]*>", "\n- ", s, flags=re.I)
    s = re.sub(r"</li>", "", s, flags=re.I)
    s = re.sub(r"<ul[^>]*>|</ul>|<ol[^>]*>|</ol>", "\n", s, flags=re.I)
    # 引用
    s = re.sub(r"<blockquote[^>]*>", "\n> ", s, flags=re.I)
    s = re.sub(r"</blockquote>", "\n\n", s, flags=re.I)
    # pre/code
    s = re.sub(r"<pre[^>]*>", "\n```\n", s, flags=re.I)
    s = re.sub(r"</pre>", "\n```\n", s, flags=re.I)
    s = re.sub(r"<code[^>]*>", "`", s, flags=re.I)
    s = re.sub(r"</code>", "`", s, flags=re.I)
    # 剩下的全删
    s = _RE_TAG.sub("", s)
    s = html.unescape(s)
    # 合并空白
    s = s.replace("\r", "")
    s = _RE_WS.sub(" ", s)
    s = _RE_NL.sub("\n\n", s)
    return s.strip()


def _parse_iso_ms(iso: Optional[str]) -> Optional[int]:
    """Substack `post_date` 是 `2026-04-20T14:21:59.538Z` 形式."""
    if not iso:
        return None
    try:
        # 兼容带/不带毫秒的 Z 时区
        dt = datetime.strptime(iso.replace("Z", "+00:00").split(".")[0] + "+00:00",
                                "%Y-%m-%dT%H:%M:%S%z")
        # 优先精确到毫秒
        if "." in iso:
            frac = iso.split(".", 1)[1].rstrip("Z")
            try:
                ms = int(frac[:3])
                return int(dt.timestamp() * 1000) + ms
            except Exception:
                pass
        return int(dt.timestamp() * 1000)
    except Exception:
        return None


def _to_cst_str(iso: Optional[str]) -> str:
    """把 UTC ISO 转 Asia/Shanghai 'YYYY-MM-DD HH:MM' (跟其它 scraper 一致,
    让前端 dayjs 拿到 naive 本地时间直接显示)."""
    if not iso:
        return ""
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        cst = dt.astimezone(timezone(timedelta(hours=8)))
        return cst.strftime("%Y-%m-%d %H:%M")
    except Exception:
        return iso[:16].replace("T", " ")


def build_doc(post: dict, list_item: Optional[dict] = None,
              content_truncated: bool = False) -> dict:
    """从 /posts/by-id 响应的 post 对象构造入库 doc."""
    pid = int(post.get("id") or 0)
    title = str(post.get("title") or "").strip()
    post_date = post.get("post_date") or ""
    release_ms = _parse_iso_ms(post_date)
    release_str = _to_cst_str(post_date)
    body_html = post.get("body_html") or ""
    content_md = html_to_markdown(body_html)
    audience = post.get("audience") or "everyone"
    slug = post.get("slug") or ""
    canonical = post.get("canonical_url") or (
        f"{BASE_URL}/p/{slug}" if slug else "")
    subtitle = (post.get("subtitle") or "").strip()
    description = (post.get("description")
                   or post.get("search_engine_description")
                   or "").strip()

    # 封面图 — Substack 通常在 cover_image_url / socialTitle image
    cover = (post.get("cover_image") or post.get("cover_image_url")
             or post.get("top_image") or "")
    podcast_url = post.get("podcast_url") or ""
    section_name = post.get("section_name") or ""
    section_slug = post.get("section_slug") or ""

    # 作者 — post['postTags'] 不含, 真正的作者在 publishedBylines 或 attributes
    authors: list[str] = []
    for key in ("publishedBylines", "bylines", "post_authors"):
        lst = post.get(key)
        if isinstance(lst, list):
            for a in lst:
                if isinstance(a, dict):
                    n = (a.get("name") or a.get("handle")
                         or a.get("publicName") or "").strip()
                    if n and n not in authors:
                        authors.append(n)
                elif isinstance(a, str):
                    authors.append(a)
    organization = ", ".join(authors) or "SemiAnalysis"

    stats = {
        "wordcount": post.get("wordcount") or 0,
        "content_chars": len(content_md),
        "html_chars": len(body_html),
        "reaction_count": (post.get("reactions") or {}).get("❤") or 0
                          if isinstance(post.get("reactions"), dict) else 0,
    }

    doc = {
        "_id": f"s{pid}",                         # namespaced per platform
        "post_id": pid,
        "slug": slug,
        "title": title,
        "release_time": release_str,
        "release_time_ms": release_ms,
        "post_date": post_date,                   # 原始 ISO
        "audience": audience,                     # 'everyone' | 'only_paid' | 'founding'
        "is_paid": audience != "everyone",
        "content_truncated": bool(audience != "everyone") or content_truncated,
        "section_name": section_name,
        "section_slug": section_slug,
        "subtitle": subtitle,
        "description": description,
        "canonical_url": canonical,
        "cover_image": cover,
        "podcast_url": podcast_url,
        "organization": organization,
        "authors": authors,
        "content_md": content_md,
        "content_html": body_html,
        "truncated_body_text": post.get("truncated_body_text") or "",
        "stats": stats,
        "list_item": list_item or {},
        "detail_result": {k: post.get(k) for k in (
            # 保留关键字段, 不整包塞 (post.body_html 已在顶层, 避免 2 份)
            "id", "slug", "title", "post_date", "audience", "type",
            "wordcount", "podcast_duration", "podcast_url",
            "section_id", "section_name", "section_slug",
            "canonical_url", "reactions", "restacks",
            "publishedBylines", "postTags",
        )},
        "crawled_at": datetime.now(timezone.utc),
    }
    return doc


# ==================== Mongo ====================

def connect_mongo(uri: str, dbname: str):
    client = MongoClient(uri, serverSelectionTimeoutMS=5000, tz_aware=True)
    try:
        client.admin.command("ping")
    except PyMongoError as e:
        print(f"错误: 无法连接 MongoDB ({uri}): {e}")
        sys.exit(1)
    db = client[dbname]
    col = db[COL_POSTS]
    # 索引 — 跟其它 _db.py 约定一致
    col.create_index("title")
    col.create_index("release_time")
    col.create_index("release_time_ms")
    col.create_index("slug")
    col.create_index("audience")
    col.create_index("section_name")
    col.create_index("crawled_at")
    col.create_index("_canonical_tickers")
    col.create_index([("release_time_ms", -1), ("post_id", -1)])
    print(f"[Mongo] 已连接 {uri} -> db={dbname}  collection={COL_POSTS}",
          flush=True)
    return db


def load_state(db) -> dict:
    return db[COL_STATE].find_one({"_id": "crawler_semianalysis"}) or {}


def save_state(db, **kwargs) -> None:
    kwargs["updated_at"] = datetime.now(timezone.utc)
    db[COL_STATE].update_one(
        {"_id": "crawler_semianalysis"},
        {"$set": kwargs},
        upsert=True,
    )


def save_account(db, info: dict) -> None:
    db[COL_ACCOUNT].update_one(
        {"_id": "semianalysis_publication"},
        {"$set": {"info": info,
                  "updated_at": datetime.now(timezone.utc)}},
        upsert=True,
    )


# ==================== 主抓取 ====================

def iter_archive(session: requests.Session,
                 max_items: Optional[int] = None,
                 stop_at_id: Optional[int] = None,
                 stop_before_ms: Optional[int] = None):
    """generator: 翻 archive, 按 new-first 顺序逐条 yield stub.

    停止条件:
      - max_items 达到
      - 命中 stop_at_id 立即停
      - stub.post_date < stop_before_ms (时间窗过期) 整页停
      - 翻页返回空 list
    """
    offset = 0
    yielded = 0
    while True:
        page = fetch_archive(session, offset=offset, limit=ARCHIVE_PAGE_SIZE)
        if not page:
            tqdm.write(f"  [page offset={offset}] 空, 停")
            return
        new_in_page = 0
        hit_known = False
        hit_old = False
        for stub in page:
            try:
                pid = int(stub.get("id") or 0)
            except Exception:
                continue
            if not pid:
                continue
            if stop_at_id and pid == stop_at_id:
                hit_known = True
                break
            ms = _parse_iso_ms(stub.get("post_date"))
            if stop_before_ms is not None and ms is not None and ms < stop_before_ms:
                hit_old = True
                break
            yield stub
            yielded += 1
            new_in_page += 1
            if max_items and yielded >= max_items:
                tqdm.write(f"  [max {max_items}] 达到, 停")
                return
        tqdm.write(f"  [page offset={offset}] +{new_in_page}/{len(page)} "
                   f"(累计 {yielded}) hit_known={hit_known} hit_old={hit_old}")
        if hit_known or hit_old:
            return
        if len(page) < ARCHIVE_PAGE_SIZE:
            return
        offset += ARCHIVE_PAGE_SIZE
        _THROTTLE.sleep_before_next()


def dump_post(session: requests.Session, db, stub: dict,
              force: bool = False) -> str:
    """抓详情 + 入库. 返回 'added' / 'skipped' / 'updated' / 'failed'."""
    col = db[COL_POSTS]
    try:
        pid = int(stub.get("id") or 0)
    except Exception:
        return "failed"
    if not pid:
        return "failed"
    _id = f"s{pid}"
    existed = col.find_one({"_id": _id}, {"_id": 1, "content_truncated": 1,
                                            "audience": 1})
    if existed and not force:
        # 对付费文章, 若之前写入时 cookie 还没解锁, 现在 cookie 解锁了
        # 可以重拉一次 — 判依据: audience=only_paid 且 content_truncated=True
        if not existed.get("content_truncated"):
            return "skipped"
        # 有 cookie 再重试; 无 cookie 本来也只能拿 preview, 无意义
    post = fetch_post(session, pid)
    doc = build_doc(post, list_item=stub, content_truncated=False)
    _stamp_ticker(doc, "semianalysis", col)
    col.replace_one({"_id": _id}, doc, upsert=True)
    return "updated" if existed else "added"


def run_once(session: requests.Session, db, args) -> dict:
    """一轮扫 archive (new-first) + dump. 返回统计."""
    state = load_state(db)
    stop_id = state.get("top_id") if args.resume else None
    if args.resume and stop_id:
        print(f"[恢复] 上次 top_id={stop_id} → 增量到此停")
    elif args.resume:
        print("[恢复] 未找到 checkpoint, 全量爬")

    stop_ms: Optional[int] = None
    if getattr(args, "since_hours", None) is not None:
        stop_ms = int((datetime.now(timezone.utc)
                        - timedelta(hours=args.since_hours)).timestamp() * 1000)
        print(f"[时间窗] 仅抓 {args.since_hours}h 内 "
              f"(cutoff={datetime.fromtimestamp(stop_ms/1000, timezone.utc):%Y-%m-%d %H:%M} UTC)")

    cap = cap_from_args(args)
    added = updated = skipped = failed = 0
    new_top_id: Optional[int] = None
    pbar: Optional[tqdm] = None

    state_in_progress = dict(state)
    save_state(db, in_progress=True, last_run_start_at=datetime.now(timezone.utc))
    try:
        for stub in iter_archive(session, max_items=args.max,
                                  stop_at_id=stop_id,
                                  stop_before_ms=stop_ms):
            if pbar is None:
                pbar = tqdm(desc="SemiAnalysis", unit="条", dynamic_ncols=True,
                            bar_format="{l_bar}{bar}| {n_fmt} [{elapsed}<{remaining}] {postfix}")
            if cap.exhausted() or _BUDGET.exhausted():
                tqdm.write(f"  [antibot] 配额 daily_cap={cap.max_items} "
                           f"budget={_BUDGET.daily_limit}/24h 已满, 停")
                break
            if new_top_id is None:
                try:
                    new_top_id = int(stub.get("id") or 0) or None
                except Exception:
                    pass
            title = (stub.get("title") or "")[:70]
            t_iso = (stub.get("post_date") or "")[:16].replace("T", " ")
            try:
                status = dump_post(session, db, stub, force=args.force)
            except SessionDead:
                raise
            except Exception as e:
                failed += 1
                tqdm.write(f"  ✗ {t_iso} {title}  ERR: {e}"[:220])
                pbar.update(1)
                continue
            if status == "skipped":
                skipped += 1
                tqdm.write(f"  · {t_iso} {title}  已存在")
            elif status == "updated":
                updated += 1
                tqdm.write(f"  ↻ {t_iso} {title}  更新 (paid 补全?)")
            elif status == "added":
                added += 1
                tqdm.write(f"  + {t_iso} {title}  新增")
            cap.bump()
            _BUDGET.bump()
            pbar.update(1)
            _THROTTLE.sleep_before_next()
    finally:
        if pbar is not None:
            pbar.close()

    stats = {"added": added, "updated": updated,
             "skipped": skipped, "failed": failed}
    save_state(db, in_progress=False,
               last_run_end_at=datetime.now(timezone.utc),
               last_run_stats=stats,
               top_id=(new_top_id or state_in_progress.get("top_id")))
    total = db[COL_POSTS].estimated_document_count()
    print(f"\n[本轮统计] 新增 {added} · 更新 {updated} · 已有 {skipped} "
          f"· 失败 {failed} · 库存 {total}")
    return stats


# ==================== 今日 / show-state ====================

def count_today(session: requests.Session, db, args) -> dict:
    """扫 archive 直到命中昨日之前的条, 统计今日平台 vs 库.

    Substack new-first, 所以翻到 stub.post_date < today00 可以整页 break.
    """
    date_str = args.date or datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d")
    today_cst = datetime.strptime(date_str, "%Y-%m-%d").replace(
        tzinfo=timezone(timedelta(hours=8)))
    today_utc_ms = int(today_cst.astimezone(timezone.utc).timestamp() * 1000)
    tomorrow_utc_ms = today_utc_ms + 86400 * 1000

    platform_ids: list[int] = []
    offset = 0
    while True:
        page = fetch_archive(session, offset=offset, limit=ARCHIVE_PAGE_SIZE)
        if not page:
            break
        stop = False
        for stub in page:
            ms = _parse_iso_ms(stub.get("post_date"))
            if ms is None:
                continue
            if ms < today_utc_ms:
                stop = True
                break
            if ms < tomorrow_utc_ms:
                try:
                    platform_ids.append(int(stub.get("id")))
                except Exception:
                    pass
        if stop or len(page) < ARCHIVE_PAGE_SIZE:
            break
        offset += ARCHIVE_PAGE_SIZE
        _THROTTLE.sleep_before_next()

    in_db = 0
    if platform_ids:
        in_db = db[COL_POSTS].count_documents(
            {"_id": {"$in": [f"s{i}" for i in platform_ids]}})
    missing = len(platform_ids) - in_db
    doc = {
        "_id": f"daily_semianalysis_{date_str}",
        "date": date_str,
        "total_on_platform": len(platform_ids),
        "in_db": in_db,
        "not_in_db": missing,
        "platform_ids": platform_ids,
        "scanned_at": datetime.now(timezone.utc),
    }
    db[COL_STATE].update_one({"_id": doc["_id"]}, {"$set": doc}, upsert=True)
    print(f"[今日 {date_str}] 平台 {len(platform_ids)} · 库 {in_db} · 漏 {missing}")
    return doc


def show_state(session: requests.Session, db) -> None:
    print("=" * 60)
    print(f"SemiAnalysis 爬虫状态")
    print("=" * 60)

    # 1. 探测 archive 可达性
    try:
        head = fetch_archive(session, offset=0, limit=1)
        print(f"[archive]  ✓ 可达. 最新: "
              f"id={head[0].get('id') if head else '?'} "
              f"title={(head[0].get('title') if head else '')[:60]!r} "
              f"audience={head[0].get('audience') if head else '?'}")
    except SessionDead as e:
        print(f"[archive]  ✗ 401/403 (登录失效): {e}")
    except Exception as e:
        print(f"[archive]  ✗ 错误: {e}")

    # 2. cookie 状态
    cookie = _load_cookie_from_file()
    if cookie:
        has_sid = "substack.sid=" in cookie
        print(f"[cookie]   {'✓' if has_sid else '?'} credentials.json "
              f"长度={len(cookie)}  含 substack.sid={has_sid}")
    else:
        print("[cookie]   - 未配置 (匿名模式; paid 内容将是 preview)")

    # 3. checkpoint
    st = load_state(db)
    if st:
        print(f"[state]    top_id={st.get('top_id')} "
              f"updated={st.get('updated_at')}")
        lrs = st.get("last_run_stats")
        if lrs:
            print(f"           last_run={lrs}")
    else:
        print("[state]    (无 checkpoint — 首次运行)")

    # 4. collection 总数 + 最新 5 条
    col = db[COL_POSTS]
    n = col.estimated_document_count()
    print(f"[collection] {COL_POSTS}: {n} 条")
    for d in col.find({}, {"_id": 1, "title": 1, "release_time": 1,
                            "audience": 1}).sort("release_time_ms", -1).limit(5):
        print(f"    {d.get('release_time','-')}  [{d.get('audience','?')}]  "
              f"{d.get('title','')[:70]}")
    print()


# ==================== parse_args + main ====================

def parse_args():
    p = argparse.ArgumentParser(
        description="SemiAnalysis (newsletter.semianalysis.com) Substack 爬虫",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--max", type=int, default=None,
                   help="最多抓 N 条. 默认翻到 archive 末尾")
    p.add_argument("--force", action="store_true",
                   help="强制重抓已入库")
    p.add_argument("--resume", action="store_true",
                   help="增量模式: 遇上次 top_id 即停")
    p.add_argument("--watch", action="store_true",
                   help="实时模式: 定时轮询. Ctrl+C 退出")
    p.add_argument("--interval", type=int, default=600,
                   help="实时模式轮询间隔秒数 (默认 600)")
    p.add_argument("--since-hours", type=float, default=None,
                   help="仅抓过去 N 小时内内容")
    p.add_argument("--today", action="store_true",
                   help="统计今日平台 vs 库, 结果存 _state_semianalysis")
    p.add_argument("--date", metavar="YYYY-MM-DD", default=None,
                   help="配合 --today 指定日期 (默认今天, CST)")
    p.add_argument("--show-state", action="store_true",
                   help="打印 checkpoint + 凭证健康检查")
    p.add_argument("--reset-state", action="store_true",
                   help="清除 crawler checkpoint (daily_* 保留)")
    p.add_argument("--auth", default=None,
                   help="覆盖 credentials.json 的 cookie (整串 document.cookie)")
    p.add_argument("--proxy", default=None,
                   help=f"HTTP 代理 (默认 {DEFAULT_PROXY}). 设 '' 或 'none' 关闭")
    p.add_argument("--mongo-uri", default=MONGO_URI_DEFAULT)
    p.add_argument("--mongo-db", default=MONGO_DB_DEFAULT)
    # 兼容 orchestrator 注入的 flag — backfill / crawler_monitor 可能传, 这里
    # 接受不报错 (SemiAnalysis 本身没独立 backfill 脚本, 但 historical 档会传).
    p.add_argument("--start-offset", type=float, default=0.0,
                   help="启动随机偏移 (秒, 由 crawler_monitor 注入, 这里延迟启动)")

    # 反爬 (crawl/antibot.py) — 默认跟 US 平台 funda 一致
    add_antibot_args(p, default_base=3.0, default_jitter=2.0,
                     default_burst=40, default_cap=400, platform=PLATFORM)
    return p.parse_args()


def main():
    args = parse_args()

    # 启动随机偏移 (crawler_monitor 注入, 打散 tick)
    if args.start_offset and args.start_offset > 0:
        print(f"[start-offset] 等 {args.start_offset:.1f}s 启动 ...")
        time.sleep(args.start_offset)

    # 凭证
    cookie = args.auth if args.auth is not None else _load_cookie_from_file()
    proxy = None if (args.proxy in ("", "none")) else (args.proxy or DEFAULT_PROXY)

    # throttle + budget — CLI 参数覆盖模块默认
    global _THROTTLE, _BUDGET
    _THROTTLE = throttle_from_args(args, platform=PLATFORM)
    acct_id = _account_id_from_cookie(cookie)
    _BUDGET = budget_from_args(args, account_id=acct_id, platform=PLATFORM)
    log_config_stamp(_THROTTLE, cap=cap_from_args(args), budget=_BUDGET,
                     extra=f"acct={acct_id} proxy={proxy or 'none'}")

    # Mongo
    db = connect_mongo(args.mongo_uri, args.mongo_db)

    # Session
    session = create_session(cookie, proxy=proxy)
    try:
        if args.show_state:
            show_state(session, db)
            return

        if args.reset_state:
            n = db[COL_STATE].delete_one({"_id": "crawler_semianalysis"}).deleted_count
            print(f"已清除 {n} 条 checkpoint (daily_* 保留)")
            return

        # 首次或强制刷新 publication meta
        if db[COL_STATE].count_documents({"_id": "semianalysis_publication"}) == 0 \
                or args.force:
            try:
                info = fetch_publication_info(session)
                save_account(db, info)
                print(f"[publication] {info.get('name') or 'SemiAnalysis'}  "
                      f"pub_id={info.get('publication_id')}")
            except SessionDead as e:
                print(f"\n[错误] 会话失效: {e}")
                print("  → 浏览器重登, 更新 credentials.json 的 cookie")
                sys.exit(2)
            except Exception as e:
                print(f"[publication] 拉取失败 (不致命): {e}")

        if args.today:
            count_today(session, db, args)
            return

        if args.watch:
            print(f"\n[实时模式] 每 {args.interval}s 轮询. Ctrl+C 退出.")
            round_num = 0
            while True:
                round_num += 1
                print(f"\n{'═' * 60}\n[轮次 {round_num}] "
                      f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n{'═' * 60}")
                try:
                    run_once(session, db, args)
                except SessionDead as e:
                    print(f"\n[错误] 会话失效: {e}")
                    print("  → 浏览器重登 substack, 更新 credentials.json")
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
                run_once(session, db, args)
            except SessionDead as e:
                print(f"\n[错误] 会话失效: {e}")
                print("  → 浏览器重登, 更新 credentials.json")
                sys.exit(2)
    finally:
        try:
            session.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
