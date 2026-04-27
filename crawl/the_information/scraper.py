#!/usr/bin/env python3
"""The Information (theinformation.com) 爬虫.

抓 The Information 的所有文章列表 + 卡片字段 (匿名模式可拿到 6100+ 历史
归档的 title / authors / publish_date / excerpt / image / category / paywall
标记). 全文 body 在付费墙后, 当前匿名模式不抓.

数据落点: MongoDB `foreign-website` DB → `theinformation_posts` collection.
Checkpoint: `_state_theinformation` collection.

架构 (per FINDINGS.md 2026-04-24):
  - 列表: GET /articles?page=N (整数翻页, 1 ~ ~678, 每页 9 条)
  - 详情: GET /articles/<slug> (slug-based URL, 无 numeric id 在 path 里)
  - SSR HTML, 没有客户端 /api/, 用 BeautifulSoup parse 卡片块
  - Cloudflare 前置 (Turnstile 卡 /sign-in), 匿名访问 / + /articles 不被拦

使用方法:
  python3 scraper.py --show-state             # 凭证 + checkpoint
  python3 scraper.py --max 5                  # 先抓 5 条试水
  python3 scraper.py --resume                 # 增量到已知 top_id 即停
  python3 scraper.py --watch --resume --interval 1800  # 实时模式 (30 min)
  python3 scraper.py --since-hours 24         # 仅抓 24h 内
  python3 scraper.py --start-page 1 --max-page 678  # 全量回灌

环境变量:
  MONGO_URI / MONGO_DB         默认本机 27018 / foreign-website
  HTTP_PROXY / HTTPS_PROXY     默认 http://127.0.0.1:7890 (Clash, US 站需要)
  TI_COOKIE                    可选, 整串 document.cookie (覆盖 credentials.json)

依赖 crawl/antibot.py (共享反爬栈) + bs4.
"""
from __future__ import annotations

import argparse
import hashlib
import html as html_lib
import json
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup
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

# ==================== 常量 ====================

PLATFORM = "the_information"

BASE_URL = "https://www.theinformation.com"
ARTICLES_LIST_PATH = "/articles"

CREDS_FILE = Path(__file__).resolve().parent / "credentials.json"

MONGO_URI_DEFAULT = os.environ.get(
    "MONGO_URI",
    "mongodb://127.0.0.1:27018/",
)
MONGO_DB_DEFAULT = os.environ.get("MONGO_DB", "foreign-website")
COL_POSTS = "theinformation_posts"
COL_STATE = "_state_theinformation"

# US 站, 必须走 Clash 代理
DEFAULT_PROXY = os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY") \
    or "http://127.0.0.1:7890"

DEFAULT_TIMEOUT = 30.0
DEFAULT_MAX_RETRIES = 3
LIST_PAGE_SIZE = 9   # 每页 9 篇 (FINDINGS)
MAX_PAGE_DEFAULT = 700   # 1 ~ ~678 实测 + 余量
WATCH_INTERVAL_DEFAULT = 1800   # 30 min

# 模块级 throttle — main() 用 CLI 参数覆盖. FINDINGS 实测 4 个 URL 40s 内就触发
# 403, base 5s + jitter 3s 是地板.
_THROTTLE: AdaptiveThrottle = AdaptiveThrottle(
    base_delay=5.0, jitter=3.0, burst_size=20, platform=PLATFORM)
_BUDGET: AccountBudget = AccountBudget(PLATFORM, "default", 0)

# 北京时区 (release_time 字段统一展示用)
BJ_TZ = timezone(timedelta(hours=8))


# ==================== 凭证 ====================

def _load_cookie_from_file() -> str:
    """credentials.json 里的 cookie 字段. 匿名用户直接返空串."""
    if not CREDS_FILE.exists():
        return ""
    try:
        d = json.loads(CREDS_FILE.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"[warn] credentials.json 解析失败: {e}")
        return ""
    return (d.get("cookie") or d.get("token") or "").strip()


def _load_cookie() -> str:
    """优先级: env TI_COOKIE > credentials.json"""
    env_c = os.environ.get("TI_COOKIE", "").strip()
    return env_c or _load_cookie_from_file()


def _account_id_from_cookie(cookie: str) -> str:
    if not cookie:
        return "anon"
    return "u_" + hashlib.md5(cookie.encode()).hexdigest()[:12]


# ==================== HTTP session ====================

def create_session(cookie: str = "", proxy: Optional[str] = None) -> requests.Session:
    s = requests.Session()
    hdrs = headers_for_platform(PLATFORM)
    hdrs["Accept"] = ("text/html,application/xhtml+xml,application/xml;q=0.9,"
                      "image/avif,image/webp,*/*;q=0.8")
    hdrs["Accept-Encoding"] = "gzip, deflate, br"
    if cookie:
        hdrs["Cookie"] = cookie
    s.headers.update(hdrs)
    proxy = proxy or DEFAULT_PROXY
    if proxy:
        s.proxies = {"http": proxy, "https": proxy}
    s.trust_env = False
    return s


def http_get(session: requests.Session, path_or_url: str,
             params: Optional[dict] = None,
             retries: int = DEFAULT_MAX_RETRIES,
             timeout: float = DEFAULT_TIMEOUT,
             what: str = "") -> str:
    """GET → text. 401/403 → SessionDead. 429/5xx → 退避重试."""
    last_err: Optional[Exception] = None
    if path_or_url.startswith("http"):
        url = path_or_url
    else:
        url = f"{BASE_URL}{path_or_url}"

    for attempt in range(retries + 1):
        try:
            r = session.get(url, params=params, timeout=timeout,
                            allow_redirects=True)
        except requests.RequestException as e:
            last_err = e
            _THROTTLE.on_retry()
            if attempt < retries:
                time.sleep(min(2 ** attempt + 1, 15))
                continue
            raise RuntimeError(f"{what or url} 网络异常: {e}")

        status = r.status_code

        # Cloudflare 拦截 (403) → 软冷却 (匿名访问的 /articles 不应该 403)
        if status == 403:
            body = r.text[:400]
            if "Just a moment" in body or "challenge" in body.lower() or "cloudflare" in body.lower():
                SoftCooldown.trigger(PLATFORM, reason="cf_challenge", minutes=10)
                raise RuntimeError(f"{what or url} Cloudflare 拦截 (cf_challenge)")
            # 普通 403, 走 SessionDead
            raise SessionDead(f"{what or url} → HTTP 403, body={body}")

        if is_auth_dead(status):
            raise SessionDead(f"{what or url} → HTTP {status}")

        if status == 429 or 500 <= status < 600:
            ra = parse_retry_after(r.headers.get("Retry-After"))
            wait = ra if ra is not None else min(2 ** attempt + 2, 30)
            _THROTTLE.on_retry()
            if attempt < retries:
                print(f"  [retry {attempt+1}/{retries}] {what or url} "
                      f"HTTP {status}, 等 {wait:.1f}s", flush=True)
                time.sleep(wait)
                continue
            raise RuntimeError(f"{what or url} HTTP {status} after {retries} retries")

        # 404 (slug 不存在) → 不重试, 不当 fatal
        if status == 404:
            raise FileNotFoundError(f"{what or url} HTTP 404")

        if not (200 <= status < 300):
            raise RuntimeError(f"{what or url} HTTP {status}: {r.text[:200]}")

        return r.text

    raise RuntimeError(f"{what or url} 全部重试失败: {last_err}")


# ==================== 时间解析 ====================

# Mapping for PDT/PST/EDT/EST → UTC 偏移小时
_TZ_OFFSETS = {
    "PDT": -7, "PST": -8,   # Pacific
    "EDT": -4, "EST": -5,   # Eastern
    "CDT": -5, "CST": -6,   # Central US
    "MDT": -6, "MST": -7,   # Mountain
    "UTC": 0,  "GMT": 0,
}

_MONTH_MAP = {m.lower(): i for i, m in enumerate(
    ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
     "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"])}


def parse_publish_datetime(text: str) -> Tuple[Optional[int], Optional[str]]:
    """The Information 卡片里的 "Apr 23, 2026 7:41pm PDT" 风格 → (ms, display).

    返回:
        (release_time_ms_utc, display_string)  显示 string 用 BJ 时间.

    不强求时间部分: 部分老文章只有 "Apr 23, 2026" 没有时分, 默认补 12:00 当地.
    """
    if not text:
        return None, None
    # 折叠多个空格 + 折掉中点
    t = re.sub(r"\s+", " ", text.replace("·", " ")).strip()
    # 形式 1: "Apr 23, 2026 7:41pm PDT"
    m = re.search(
        r"([A-Za-z]+)\s+(\d{1,2}),?\s+(\d{4})(?:\s+(\d{1,2}):(\d{2})\s*([ap]m))?\s*([A-Z]{2,4})?",
        t, flags=re.IGNORECASE)
    if not m:
        return None, None
    mon_s, day_s, year_s, hh_s, mm_s, ampm_s, tz_s = m.groups()
    mon = _MONTH_MAP.get(mon_s.lower()[:3])
    if not mon:
        return None, None
    try:
        day = int(day_s); year = int(year_s)
    except ValueError:
        return None, None
    if hh_s and mm_s and ampm_s:
        hh = int(hh_s); mm = int(mm_s)
        if ampm_s.lower() == "pm" and hh != 12:
            hh += 12
        elif ampm_s.lower() == "am" and hh == 12:
            hh = 0
    else:
        hh, mm = 12, 0
    tz_offset_h = _TZ_OFFSETS.get((tz_s or "").upper(), -8)  # 默认 PST
    # 构造 UTC 时间
    dt_local_naive = datetime(year, mon, day, hh, mm, 0)
    dt_utc = dt_local_naive - timedelta(hours=tz_offset_h)
    dt_utc = dt_utc.replace(tzinfo=timezone.utc)
    ms = int(dt_utc.timestamp() * 1000)
    # display 用 BJ 时间, 形式 "YYYY-MM-DD HH:MM"
    dt_bj = dt_utc.astimezone(BJ_TZ)
    display = dt_bj.strftime("%Y-%m-%d %H:%M")
    return ms, display


# ==================== HTML parsing ====================

# 关键 selectors (per HTML probe 2026-04-25)
SEL_ARTICLE_BLOCK = "div.article.feed-item"
SEL_LINK = "a.article-link"           # 主链接, 带 id="article-<num>"
SEL_TITLE_LINK = "h3.title a"         # 标题文字 + slug
SEL_AUTHORS = "div.authors"           # 作者 + 时间区域
SEL_AUTHOR_A = "a.author_link"        # 单个作者链接
SEL_CATEGORY = "div.category-content a.highlight"
SEL_EXCERPT_LONG = ".long-excerpt"
SEL_EXCERPT_SHORT = ".short-excerpt"
SEL_IMG = "picture img.article-image"


_SLUG_RE = re.compile(r"/articles/([^/?#]+)")


def _parse_article_id_from_link(a_tag) -> Optional[int]:
    """`<a id="article-12345">` → 12345."""
    if not a_tag:
        return None
    aid = a_tag.get("id") or ""
    m = re.match(r"article-(\d+)", aid)
    return int(m.group(1)) if m else None


def _slug_from_href(href: str) -> Optional[str]:
    if not href:
        return None
    m = _SLUG_RE.search(href)
    return m.group(1) if m else None


def parse_article_card(card) -> Optional[Dict[str, Any]]:
    """单个 <div class="article feed-item"> → dict.

    返回 None = 卡片缺关键字段 (slug 或 article_id), 上层应当跳过.
    """
    a_link = card.select_one(SEL_LINK)
    article_id = _parse_article_id_from_link(a_link)
    href = (a_link.get("href") if a_link else "") or ""
    slug = _slug_from_href(href)
    if not slug or article_id is None:
        return None

    # title
    title_a = card.select_one(SEL_TITLE_LINK)
    title = (title_a.get_text(strip=True) if title_a else "")
    title = html_lib.unescape(title)

    # authors + date
    authors_div = card.select_one(SEL_AUTHORS)
    authors_list: List[Dict[str, str]] = []
    publish_date_text = ""
    if authors_div:
        for a in authors_div.select(SEL_AUTHOR_A):
            name = a.get_text(strip=True)
            ahref = a.get("href") or ""
            slug_a = ahref.lstrip("/").replace("u/", "", 1) if "/u/" in ahref else ahref
            if name:
                authors_list.append({"name": name, "slug": slug_a, "href": ahref})
        # 把作者链接全文先剥掉, 剩下的就是 "By X, Y · Apr 23, 2026 7:41pm PDT"
        authors_text = authors_div.get_text(" ", strip=True)
        # 去掉 "By " 前缀
        authors_text = re.sub(r"^By\s+", "", authors_text, count=1)
        # 去掉所有作者名 (按 a 标签的 text 顺序)
        for a in authors_list:
            authors_text = authors_text.replace(a["name"], "", 1)
        # 残余里的 "and" / "," / "·" 都剥掉
        publish_date_text = re.sub(r"\b(and)\b", " ", authors_text)
        publish_date_text = re.sub(r"[,·]+", " ", publish_date_text)
        publish_date_text = re.sub(r"\s+", " ", publish_date_text).strip()

    release_time_ms, release_time_display = parse_publish_datetime(publish_date_text)

    # category / feature label (e.g. "Exclusive", "Crypto")
    cat_a = card.select_one(SEL_CATEGORY)
    category = cat_a.get_text(strip=True) if cat_a else ""
    category_href = (cat_a.get("href") if cat_a else "") or ""

    # excerpt
    excerpt_long = card.select_one(SEL_EXCERPT_LONG)
    excerpt_short = card.select_one(SEL_EXCERPT_SHORT)
    excerpt = (excerpt_long or excerpt_short)
    excerpt_text = (excerpt.get_text(" ", strip=True) if excerpt else "")
    excerpt_text = html_lib.unescape(excerpt_text)

    # image
    img = card.select_one(SEL_IMG)
    image_url = ""
    image_alt = ""
    if img:
        image_url = img.get("src") or ""
        # 去掉 imgix 的尺寸参数, 留 base + auto=compress
        if "?" in image_url:
            base, qs = image_url.split("?", 1)
            # 保留 fm + auto, 丢掉尺寸 (取大图)
            keep = []
            for p in qs.split("&"):
                if p.startswith("fm=") or p.startswith("auto="):
                    keep.append(p)
            image_url = base + ("?" + "&".join(keep) if keep else "")
        image_alt = img.get("alt") or ""

    return {
        "_id": slug,                          # slug-based 主键 (URL stable)
        "id": slug,                            # 与其它平台保持 id == _id
        "article_id": article_id,              # 平台内 numeric id (单调)
        "slug": slug,
        "title": title,
        "category": category,                  # "Exclusive" / "Crypto" / etc.
        "category_href": category_href,
        "authors": authors_list,               # [{name, slug, href}]
        "publish_date_text": publish_date_text,  # 原文 "Apr 23, 2026 7:41pm PDT"
        "release_time": release_time_display,  # "YYYY-MM-DD HH:MM" BJ 时间
        "release_time_ms": release_time_ms,    # UTC ms
        "excerpt": excerpt_text,
        "image_url": image_url,
        "image_alt": image_alt,
        "original_url": f"{BASE_URL}/articles/{slug}",
        "link_url": f"{BASE_URL}/articles/{slug}",
        "language": "en",
        "audience": "paid_preview",            # 默认假设付费墙(免费的会在详情阶段更新)
        "isContentPaywalled": True,
        "_canonical_extract_source": "the_information_card",
    }


def parse_list_page_html(html: str) -> List[Dict[str, Any]]:
    """主入口: 列表页 HTML → 卡片 dict 列表 (顺序与页面一致, 新→旧)."""
    soup = BeautifulSoup(html, "lxml")
    cards = soup.select(SEL_ARTICLE_BLOCK)
    out: List[Dict[str, Any]] = []
    for card in cards:
        d = parse_article_card(card)
        if d:
            out.append(d)
    return out


def fetch_list_page(session: requests.Session, page: int) -> List[Dict[str, Any]]:
    """GET /articles?page=N → list of card dicts."""
    SoftCooldown.wait_if_active(PLATFORM)
    _THROTTLE.sleep_before_next()
    text = http_get(session, ARTICLES_LIST_PATH,
                    params={"page": page} if page > 1 else None,
                    what=f"list page={page}")
    _BUDGET.bump(1)
    return parse_list_page_html(text)


# ==================== Mongo upsert ====================

def dump_article(col, doc: Dict[str, Any], crawled_at: datetime,
                 force: bool = False) -> str:
    """upsert 一条 doc. 返回 'added' / 'skipped' / 'updated'."""
    doc = dict(doc)  # 拷贝, 不污染 caller
    _id = doc["_id"]
    existing = col.find_one({"_id": _id}, {"_id": 1, "title": 1})
    doc["crawled_at"] = crawled_at
    doc.setdefault("_canonical_tickers", [])

    if existing and not force:
        # 只更新可能 drift 的字段 (title / excerpt / publish_date / category)
        update_fields = {
            "title": doc["title"],
            "excerpt": doc.get("excerpt", ""),
            "publish_date_text": doc.get("publish_date_text", ""),
            "release_time": doc.get("release_time"),
            "release_time_ms": doc.get("release_time_ms"),
            "category": doc.get("category", ""),
            "image_url": doc.get("image_url", ""),
            "authors": doc.get("authors", []),
            "_last_seen_at": crawled_at,
        }
        col.update_one({"_id": _id}, {"$set": update_fields})
        return "skipped"

    col.replace_one({"_id": _id}, doc, upsert=True)
    return "added"


# ==================== State ====================

def load_state(col_state) -> Dict[str, Any]:
    s = col_state.find_one({"_id": "crawler_articles"}) or {}
    return s


def save_state(col_state, **fields) -> None:
    fields["updated_at"] = datetime.now(timezone.utc)
    col_state.update_one({"_id": "crawler_articles"},
                        {"$set": fields, "$setOnInsert": {
                            "created_at": datetime.now(timezone.utc),
                        }}, upsert=True)


def save_account_meta(col_state, cookie: str, http_status: int = 0,
                      reason: str = "") -> None:
    """记录最近一次"凭证健康"探测结果, monitor 用它给 auth.health 着色."""
    col_state.update_one({"_id": "account"},
                        {"$set": {
                            "cookie_present": bool(cookie),
                            "cookie_account_id": _account_id_from_cookie(cookie),
                            "last_check_status": http_status,
                            "last_check_reason": reason,
                            "checked_at": datetime.now(timezone.utc),
                        }}, upsert=True)


# ==================== 主循环 ====================

def cmd_show_state(args) -> None:
    """打印 checkpoint + 凭证状态 + collection 统计 (monitor 用 stdout 取)."""
    cookie = _load_cookie()
    cli = MongoClient(args.mongo_uri)
    db = cli[args.mongo_db]
    col = db[COL_POSTS]
    col_state = db[COL_STATE]

    # warmup probe — 让 monitor 看到 auth.health
    sess = create_session(cookie, proxy=args.proxy)
    probe_ok = False
    probe_status = 0
    probe_reason = ""
    try:
        text = http_get(sess, "/articles", what="warmup")
        probe_ok = "feed-item" in text
        probe_status = 200
        probe_reason = "ok" if probe_ok else "no_feed_items"
    except Exception as e:
        probe_reason = type(e).__name__ + ": " + str(e)[:120]
        if isinstance(e, SessionDead):
            probe_status = 403
        else:
            probe_status = -1
    save_account_meta(col_state, cookie, probe_status, probe_reason)
    print(f"  [warmup] the_information GET /articles -> "
          f"{'200 OK' if probe_ok else 'FAIL ('+probe_reason+')'}")

    print("=" * 60)
    print("the_information Checkpoint")
    print("=" * 60)
    state = load_state(col_state)
    if state:
        print(f"     top_id (numeric): {state.get('top_id', '-')}")
        print(f"     top_slug:         {state.get('top_slug', '-')}")
        print(f"     last_processed:   {state.get('last_processed_id', '-')}")
        print(f"     last_run_at:      {state.get('last_run_at', '-')}")
        print(f"     last_run_stats:   {state.get('last_run_stats', {})}")
    else:
        print("  (no checkpoint yet — first run)")
    print()
    print("Collection 总数:")
    try:
        total = col.estimated_document_count()
        latest = col.find_one({}, sort=[("article_id", -1)],
                             projection={"title": 1, "release_time": 1, "article_id": 1})
        print(f"     {COL_POSTS}: {total}")
        if latest:
            print(f"     latest: article_id={latest.get('article_id','?')}  "
                  f"{latest.get('release_time','?')}  {latest.get('title','?')[:60]}")
    except Exception as e:
        print(f"     [err] {e}")

    print()
    cookie_pref = (cookie[:30] + "...") if len(cookie) > 30 else (cookie or "(empty — anonymous mode)")
    print(f"[cookie] {cookie_pref}")
    print(f"[probe] status={probe_status} reason={probe_reason}")


def _within_window(release_ms: Optional[int], cutoff_ms: Optional[int]) -> bool:
    if cutoff_ms is None or release_ms is None:
        return True
    return release_ms >= cutoff_ms


def run_once(args, *, max_count: int = 0) -> Dict[str, int]:
    """跑一轮 (--watch 时被反复调用). 返回 stats."""
    cookie = _load_cookie()
    sess = create_session(cookie, proxy=args.proxy)

    cli = MongoClient(args.mongo_uri)
    db = cli[args.mongo_db]
    col = db[COL_POSTS]
    col_state = db[COL_STATE]
    col.create_index("article_id")
    col.create_index("release_time_ms")
    col.create_index("crawled_at")

    state = load_state(col_state)
    known_top_id = int(state.get("top_id") or 0)
    known_top_slug = state.get("top_slug") or ""

    cutoff_ms: Optional[int] = None
    if args.since_hours and args.since_hours > 0:
        cutoff_dt = datetime.now(timezone.utc) - timedelta(hours=args.since_hours)
        cutoff_ms = int(cutoff_dt.timestamp() * 1000)
        print(f"[时间窗] 仅抓 {args.since_hours}h 内 (cutoff_utc_ms={cutoff_ms})")

    start_page = max(1, args.start_page)
    end_page = min(args.max_page, MAX_PAGE_DEFAULT)
    stats = {"added": 0, "skipped": 0, "failed": 0, "out_of_window": 0,
             "stop_at_known": False}
    processed = 0
    new_top_id = known_top_id
    new_top_slug = known_top_slug

    crawled_at = datetime.now(timezone.utc)
    save_state(col_state, in_progress=True, last_run_at=crawled_at)

    pbar = tqdm(total=end_page - start_page + 1, unit="page",
                desc="the_information articles")
    page = start_page
    try:
        while page <= end_page:
            try:
                cards = fetch_list_page(sess, page)
            except SessionDead as e:
                print(f"\n[fatal] {e}", flush=True)
                save_account_meta(col_state, cookie, 403, str(e)[:200])
                stats["failed"] += 1
                break
            except FileNotFoundError:
                print(f"\n[end] page={page} 404, 停止翻页")
                break
            except Exception as e:
                print(f"\n[err page={page}] {type(e).__name__}: {e}", flush=True)
                stats["failed"] += 1
                page += 1
                pbar.update(1)
                continue

            if not cards:
                print(f"\n[end] page={page} 无卡片, 停止")
                break

            page_added = 0
            page_skipped = 0
            for card in cards:
                aid = card.get("article_id") or 0
                slug = card.get("slug") or ""
                rms = card.get("release_time_ms")

                # --resume + 已知 top_id: 命中即停 (确保 top_id 单调)
                if args.resume and aid and known_top_id and aid <= known_top_id:
                    stats["stop_at_known"] = True
                    print(f"\n[resume] hit known top article_id={aid} (slug={slug}), 停止翻页")
                    page = end_page + 1
                    break

                # --since-hours: 过窗口直接停 (按 release_time_ms 单调)
                if cutoff_ms and not _within_window(rms, cutoff_ms):
                    stats["out_of_window"] += 1
                    if stats["out_of_window"] >= 5:   # 容错: 连续 5 篇出窗
                        print(f"\n[since] 累计 {stats['out_of_window']} 篇出窗, 停止")
                        page = end_page + 1
                        break
                    continue

                try:
                    status = dump_article(col, card, crawled_at, force=args.force)
                    if status == "added":
                        stats["added"] += 1; page_added += 1
                        if aid > new_top_id:
                            new_top_id = aid
                            new_top_slug = slug
                    else:
                        stats["skipped"] += 1; page_skipped += 1
                except PyMongoError as e:
                    stats["failed"] += 1
                    print(f"\n[mongo err] aid={aid} slug={slug} {e}", flush=True)

                processed += 1
                if max_count > 0 and processed >= max_count:
                    print(f"\n[--max] 达到 {max_count} 条, 停止")
                    page = end_page + 1
                    break

            pbar.set_postfix_str(
                f"+{stats['added']}/={stats['skipped']}/✗{stats['failed']} "
                f"page={page} added={page_added}")
            page += 1
            pbar.update(1)

            # 每页落盘一次 state (防止中断丢点)
            save_state(col_state,
                      top_id=new_top_id, top_slug=new_top_slug,
                      last_processed_id=processed,
                      last_run_stats=stats)
    finally:
        pbar.close()
        save_state(col_state,
                  in_progress=False,
                  last_run_at=datetime.now(timezone.utc),
                  last_run_stats=stats,
                  top_id=new_top_id, top_slug=new_top_slug,
                  last_processed_id=processed)
        save_account_meta(col_state, cookie, 200, "ok")
        cli.close()

    return stats


def cmd_today(args) -> None:
    """今日入库统计 (BJ 时区)。monitor 的 today_added 也是从这个口径取."""
    cli = MongoClient(args.mongo_uri)
    db = cli[args.mongo_db]
    col = db[COL_POSTS]
    today_bj = datetime.now(BJ_TZ).strftime("%Y-%m-%d")
    cutoff_ms = int(datetime(*[int(x) for x in today_bj.split("-")],
                             tzinfo=BJ_TZ).astimezone(timezone.utc).timestamp() * 1000)
    n_db_today = col.count_documents({"release_time_ms": {"$gte": cutoff_ms}})
    n_total = col.estimated_document_count()
    print(f"  日期 (BJ): {today_bj}")
    print(f"  release_time_ms >= 今日 0:00 BJ: {n_db_today}")
    print(f"  Collection 总数: {n_total}")


def cmd_watch(args) -> None:
    interval = args.interval or WATCH_INTERVAL_DEFAULT
    print(f"\n[实时模式] 每 {interval}s 轮询. Ctrl+C 退出.\n")
    rnd = 0
    while True:
        rnd += 1
        ts = datetime.now(BJ_TZ).strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n{'='*60}")
        print(f"[轮次 {rnd}] {ts} BJ")
        print(f"{'='*60}")
        try:
            stats = run_once(args, max_count=args.max)
            print(f"\n[轮次 {rnd} 完成] +{stats['added']} ={stats['skipped']} "
                  f"✗{stats['failed']} stop_at_known={stats.get('stop_at_known', False)}")
        except KeyboardInterrupt:
            print("\n[interrupt] 退出 watch loop")
            return
        except Exception as e:
            print(f"\n[轮次 {rnd} 出错] {type(e).__name__}: {e}", flush=True)
            # 错误不退, 等下一轮
        try:
            time.sleep(interval)
        except KeyboardInterrupt:
            print("\n[interrupt] 退出 watch loop")
            return


# ==================== CLI ====================

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="The Information 列表爬虫 (匿名)")
    p.add_argument("--mongo-uri", default=MONGO_URI_DEFAULT)
    p.add_argument("--mongo-db", default=MONGO_DB_DEFAULT)
    p.add_argument("--proxy", default=DEFAULT_PROXY,
                  help=f"HTTP/HTTPS proxy (默认 {DEFAULT_PROXY}). 设 '' 禁用.")
    p.add_argument("--max", type=int, default=0,
                  help="本轮抓 N 条后停 (0=不限, 走 max-page)")
    p.add_argument("--start-page", type=int, default=1,
                  help="起始页 (默认 1, 即最新)")
    p.add_argument("--max-page", type=int, default=MAX_PAGE_DEFAULT,
                  help=f"翻到第几页停 (默认 {MAX_PAGE_DEFAULT})")
    p.add_argument("--resume", action="store_true",
                  help="增量模式: 命中已知 top article_id 即停翻页")
    p.add_argument("--force", action="store_true",
                  help="对已存在的 doc 强制 replace (默认只 update 部分字段)")
    p.add_argument("--since-hours", type=float, default=0,
                  help="只抓 release_time 在 N 小时内 (默认 0=不限)")
    p.add_argument("--watch", action="store_true",
                  help="实时模式: 每 --interval 秒跑一轮 run_once")
    p.add_argument("--interval", type=int, default=WATCH_INTERVAL_DEFAULT,
                  help=f"watch 模式的轮询间隔 (默认 {WATCH_INTERVAL_DEFAULT}s)")
    p.add_argument("--show-state", action="store_true",
                  help="打印 checkpoint + 凭证 + 集合统计后退出 (monitor 用)")
    p.add_argument("--today", action="store_true",
                  help="打印今日 (BJ) 入库统计后退出")

    add_antibot_args(p, default_base=5.0, default_jitter=3.0,
                     default_burst=20, default_cap=200, platform=PLATFORM)
    return p.parse_args()


def main() -> int:
    args = parse_args()

    # 应用 antibot 配置
    global _THROTTLE, _BUDGET
    _THROTTLE = throttle_from_args(args, platform=PLATFORM)
    _DAILYCAP = cap_from_args(args)
    cookie = _load_cookie()
    acct_id = _account_id_from_cookie(cookie)
    _BUDGET = budget_from_args(args, account_id=acct_id, platform=PLATFORM,
                              role="bg")
    log_config_stamp(_THROTTLE, cap=_DAILYCAP, budget=_BUDGET,
                    extra=f"acct={acct_id}")
    print(f"[Mongo] {args.mongo_uri.split('@')[-1]} -> db={args.mongo_db}  collection={COL_POSTS}")

    if args.show_state:
        cmd_show_state(args)
        return 0

    if args.today:
        cmd_today(args)
        return 0

    if args.watch:
        try:
            cmd_watch(args)
        except KeyboardInterrupt:
            print("\n[interrupt] 退出")
        return 0

    # 一次性模式
    stats = run_once(args, max_count=args.max)
    print(f"\n[完成] +{stats['added']} ={stats['skipped']} "
          f"✗{stats['failed']} stop_at_known={stats.get('stop_at_known', False)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
