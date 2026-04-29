#!/usr/bin/env python3
"""
微信公众号 (mp.weixin.qq.com) 直采爬虫

走的是 **MP 后台路径**,需要一个已注册公众号的微信号扫码登录后台:
  - searchbiz 搜公众号 → 拿 fakeid (公众号唯一标识)
  - appmsg/list_ex 翻历史文章 (单号最深 ~5000 篇)
  - 直接 GET https://mp.weixin.qq.com/s/<sn> 拿正文 HTML
  - 解析 <div id="js_content"> → markdownify
  - <img data-src="https://mmbiz.qpic.cn/..."> → 本地下载并改写 src

凭证:`credentials.json` 内含 `token` (URL 查询参数,~4 天) + `cookies` 列表。
失效时通过 auto_login.py 扫码续期。

白名单:`accounts.yaml` 列举要跟踪的公众号名;不在白名单内的不抓。

CLI:
  python -m crawl.wechat_mp.scraper --account 机器之心 --max 5
  python -m crawl.wechat_mp.scraper --watch --interval 600 --resume
  python -m crawl.wechat_mp.scraper --backfill --since-days 180
  python -m crawl.wechat_mp.scraper --show-state

MongoDB 数据模型 (database=wechat-mp):
  articles  — 主文章流, _id = "{biz}:{appmsgid}:{itemidx}"
  account   — 账户元信息 (fakeid 缓存等)
  _state    — checkpoint + daily stats
"""

from __future__ import annotations

import argparse
import html as _html
import json
import os
import random
import re
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlparse, parse_qs, urljoin

import requests
import yaml  # type: ignore
from pymongo import MongoClient, UpdateOne
from pymongo.errors import PyMongoError

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from antibot import (  # noqa: E402
    AdaptiveThrottle,
    DailyCap,
    SessionDead,
    parse_retry_after,
    is_auth_dead,
    add_antibot_args,
    throttle_from_args,
    cap_from_args,
    AccountBudget,
    SoftCooldown,
    detect_soft_warning,
    headers_for_platform,
    log_config_stamp,
    budget_from_args,
)
from ticker_tag import stamp as _stamp_ticker  # noqa: E402

# image_dl 在当前包目录下, 直接路径导入避免 `python scraper.py` 与 `python -m
# crawl.wechat_mp.scraper` 两种调用方式都能工作
sys.path.insert(0, str(Path(__file__).resolve().parent))
import image_dl as _image_dl  # noqa: E402

# ==================== 配置 ====================

API_BASE = "https://mp.weixin.qq.com"
SEARCHBIZ_PATH = "/cgi-bin/searchbiz"
APPMSG_LIST_PATH = "/cgi-bin/appmsg"

CREDS_FILE = Path(__file__).resolve().parent / "credentials.json"
ACCOUNTS_FILE = Path(__file__).resolve().parent / "accounts.yaml"

MONGO_URI_DEFAULT = os.environ.get("MONGO_URI", "mongodb://127.0.0.1:27018/")
MONGO_DB_DEFAULT = os.environ.get("WECHAT_MP_MONGO_DB", "wechat-mp")
COL_ARTICLES = "articles"
COL_ACCOUNT = "account"
COL_STATE = "_state"

IMAGE_ROOT = Path(
    os.environ.get("WECHAT_MP_IMAGE_ROOT")
    or "/home/ygwang/crawl_data/wechat_mp_images"
)

PLATFORM = "wechat_mp"

_THROTTLE: AdaptiveThrottle = AdaptiveThrottle(
    base_delay=3.0, jitter=2.0, burst_size=30, platform=PLATFORM,
)
_BUDGET: AccountBudget = AccountBudget(PLATFORM, "default", 0)


# ==================== HTML → Markdown ====================

def _coerce_to_markdown(html_str: str) -> str:
    """跟 alphaengine/_coerce_to_markdown 同形:strip script/style → markdownify
    → unescape entities。失败回退到正则 strip。"""
    if not isinstance(html_str, str) or not html_str.strip():
        return ""
    if not html_str.lstrip().startswith("<"):
        return html_str
    try:
        from bs4 import BeautifulSoup
        from markdownify import markdownify as _md
        try:
            soup = BeautifulSoup(html_str, "lxml")
        except Exception:
            soup = BeautifulSoup(html_str, "html.parser")
        for tag in soup.find_all(["script", "style", "meta", "title", "link", "noscript"]):
            tag.decompose()
        md = _md(str(soup), heading_style="ATX")
        md = _html.unescape(md)
        md = re.sub(r"[ \t]+\n", "\n", md)
        md = re.sub(r"\n{3,}", "\n\n", md)
        return md.strip()
    except Exception:
        text = re.sub(r"<[^>]+>", " ", html_str)
        return _html.unescape(re.sub(r"\s+", " ", text)).strip()


# ==================== 凭证 ====================

def _load_creds() -> dict:
    if not CREDS_FILE.exists():
        return {}
    try:
        return json.loads(CREDS_FILE.read_text(encoding="utf-8")) or {}
    except Exception:
        return {}


def _persist_creds(updates: dict) -> None:
    """credentials.json 原子追写。"""
    data = _load_creds()
    data.update(updates)
    data["updated_at"] = datetime.now(timezone.utc).isoformat()
    tmp = CREDS_FILE.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(CREDS_FILE)


# ==================== 网络会话 ====================

def create_session(creds: dict) -> requests.Session:
    s = requests.Session()
    h = headers_for_platform(PLATFORM)
    h.update({
        "Accept": "*/*",
        "Origin": API_BASE,
        "Referer": f"{API_BASE}/",
        "X-Requested-With": "XMLHttpRequest",
    })
    s.headers.update(h)
    for c in creds.get("cookies") or []:
        s.cookies.set(
            c["name"], c["value"],
            domain=c.get("domain") or ".weixin.qq.com",
            path=c.get("path") or "/",
        )
    return s


def _expect_ok(body: dict, ctx: str) -> None:
    base = body.get("base_resp") or {}
    ret = base.get("ret")
    if ret in (None, 0):
        return
    msg = base.get("err_msg") or "?"
    if ret == -6 or "freq" in msg or "frequency" in msg:
        SoftCooldown.trigger(PLATFORM, reason=f"freq_limit:{ctx}:{ret}", minutes=15)
        raise RuntimeError(f"频率限制 ret={ret} msg={msg} ctx={ctx}")
    if "login" in msg.lower() or ret in (200003, 200004, 200013):
        # 200013 也是频控,但单账号触发不一定要换号。先按 SessionDead 抛
        raise SessionDead(f"need login ret={ret} msg={msg} ctx={ctx}")
    raise RuntimeError(f"业务错误 ret={ret} msg={msg} ctx={ctx}")


# ==================== API: searchbiz / appmsg ====================

def _rand_str() -> str:
    return f"{random.random():.16f}"


def search_account(session: requests.Session, token: str, query: str) -> list[dict]:
    """根据公众号名搜索, 返回 list[ {fakeid, nickname, alias, signature} ]."""
    url = f"{API_BASE}{SEARCHBIZ_PATH}"
    params = {
        "action": "search_biz", "token": token, "lang": "zh_CN",
        "f": "json", "ajax": "1", "random": _rand_str(),
        "query": query, "begin": "0", "count": "5",
    }
    _BUDGET.bump(1)
    _THROTTLE.sleep_before_next()
    try:
        r = session.get(url, params=params, timeout=20)
    except requests.RequestException as e:
        _THROTTLE.on_retry(attempt=1)
        raise RuntimeError(f"searchbiz 请求失败: {e}") from e
    if is_auth_dead(r.status_code):
        raise SessionDead(f"HTTP {r.status_code} on searchbiz")
    if r.status_code != 200:
        ra = parse_retry_after(r.headers.get("Retry-After"))
        _THROTTLE.on_retry(retry_after_sec=ra, attempt=1)
        raise RuntimeError(f"searchbiz HTTP {r.status_code}")
    body = r.json()
    _expect_ok(body, "searchbiz")
    out = body.get("list") or []
    soft = detect_soft_warning(200, body=body)
    if soft:
        SoftCooldown.trigger(PLATFORM, reason=soft, minutes=10)
        _THROTTLE.on_warning()
    return out


def list_articles(session: requests.Session, token: str, fakeid: str,
                  begin: int = 0, count: int = 5) -> dict:
    """翻一页文章列表. begin 是偏移, count 单页 5 (官方上限).

    返回 {app_msg_cnt, app_msg_list, base_resp}.
    """
    url = f"{API_BASE}{APPMSG_LIST_PATH}"
    params = {
        "action": "list_ex", "token": token, "lang": "zh_CN",
        "f": "json", "ajax": "1", "random": _rand_str(),
        "begin": str(begin), "count": str(count), "query": "",
        "fakeid": fakeid, "type": "9",
    }
    _BUDGET.bump(1)
    _THROTTLE.sleep_before_next()
    try:
        r = session.get(url, params=params, timeout=20)
    except requests.RequestException as e:
        _THROTTLE.on_retry(attempt=1)
        raise RuntimeError(f"list_ex 请求失败: {e}") from e
    if is_auth_dead(r.status_code):
        raise SessionDead(f"HTTP {r.status_code} on list_ex")
    if r.status_code != 200:
        ra = parse_retry_after(r.headers.get("Retry-After"))
        _THROTTLE.on_retry(retry_after_sec=ra, attempt=1)
        raise RuntimeError(f"list_ex HTTP {r.status_code}")
    body = r.json()
    _expect_ok(body, "list_ex")
    return body


# ==================== 文章页解析 ====================

_BIZ_RE = re.compile(r"__biz=([A-Za-z0-9=+/_-]+)")
_MID_RE = re.compile(r"[?&]mid=(\d+)")
_IDX_RE = re.compile(r"[?&]idx=(\d+)")
_SN_RE = re.compile(r"[?&]sn=([0-9a-fA-F]+)")
_CT_RE = re.compile(r'(?:var\s+ct\s*=\s*|"createTime"\s*:\s*)"?(\d{9,11})"?')
_PUBLISH_TIME_RE = re.compile(r'(?:publish_time|publishTime)["\']?\s*[:=]\s*["\'](\d{9,11})["\']')


def parse_article_url(url: str) -> dict:
    """从 list_ex 返回的 link 里抠 __biz / mid / idx / sn."""
    out: dict = {"url": url, "biz": None, "mid": None, "idx": None, "sn": None}
    if not url:
        return out
    qs = parse_qs(urlparse(url).query)
    out["biz"] = (qs.get("__biz") or [None])[0]
    out["mid"] = (qs.get("mid") or [None])[0]
    out["idx"] = (qs.get("idx") or [None])[0]
    out["sn"] = (qs.get("sn") or [None])[0]
    # 短链 /s/<hash> 的兜底:sn 直接是 hash
    if not out["sn"]:
        m = re.search(r"/s/([A-Za-z0-9_-]+)", url)
        if m:
            out["sn"] = m.group(1)
    return out


def fetch_article_html(session: requests.Session, url: str) -> str:
    """直接 GET 文章 URL — mp.weixin.qq.com/s 在已登录会话下不需要额外参数。
    返回原始 HTML;失败抛 RuntimeError。"""
    _THROTTLE.sleep_before_next()
    try:
        r = session.get(
            url,
            timeout=25,
            headers={
                "Referer": f"{API_BASE}/",
                "Accept": (
                    "text/html,application/xhtml+xml,application/xml;q=0.9,"
                    "image/avif,image/webp,*/*;q=0.8"
                ),
            },
        )
    except requests.RequestException as e:
        _THROTTLE.on_retry(attempt=1)
        raise RuntimeError(f"article fetch 请求失败: {e}") from e
    if r.status_code != 200:
        raise RuntimeError(f"article fetch HTTP {r.status_code} url={url}")
    txt = r.text
    # 微信偶尔在公司网络/IP 异常时返回反爬挡板("此内容因违规无法查看" / "环境异常")
    if "环境异常" in txt or "请在微信客户端打开链接" in txt:
        raise RuntimeError("article fetch blocked: 环境异常 / 需在微信客户端打开")
    return txt


def parse_article_html(html_str: str) -> dict:
    """解析正文页, 返回 {title, author, account_name, release_time_ms,
    content_md, html_content, images}.

    images 是 [{"src": "https://mmbiz.qpic.cn/..."}],尚未下载;由 dump_one 调
    image_dl.download_many 补 local_path。"""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html_str, "lxml" if "lxml" in sys.modules or _has_lxml() else "html.parser")

    title = ""
    h1 = soup.select_one("h1#activity-name") or soup.select_one("h1.rich_media_title")
    if h1:
        title = h1.get_text(strip=True)

    author = ""
    a_node = (
        soup.select_one("#meta_content_author_nickname")
        or soup.select_one(".rich_media_meta_text")
        or soup.select_one("a#meta_content_hide_info")
    )
    if a_node:
        author = a_node.get_text(strip=True)

    account_name = ""
    js_name = soup.select_one("#js_name") or soup.select_one("a.rich_media_meta_nickname")
    if js_name:
        account_name = js_name.get_text(strip=True)

    # publish 时间:页面里有 var ct = "1761712800"; (epoch seconds) 这种 JS 字符串
    release_time_ms: Optional[int] = None
    for pat in (_CT_RE, _PUBLISH_TIME_RE):
        m = pat.search(html_str)
        if m:
            try:
                release_time_ms = int(m.group(1)) * 1000
                break
            except Exception:
                pass

    # 正文容器
    body = soup.select_one("div#js_content") or soup.select_one("div.rich_media_content")
    images: list[dict] = []
    if body is not None:
        # img.data-src 是真正的图片 URL,src 可能是 base64 占位
        for img in body.find_all("img"):
            src = img.get("data-src") or img.get("src") or ""
            if src.startswith("//"):
                src = "https:" + src
            if not src or src.startswith("data:"):
                continue
            images.append({"src": src})
        html_content = str(body)
    else:
        html_content = ""

    content_md = _coerce_to_markdown(html_content) if html_content else ""

    return {
        "title": title,
        "author": author,
        "account_name": account_name,
        "release_time_ms": release_time_ms,
        "content_md": content_md,
        "html_content": html_content,
        "images": images,
    }


def _has_lxml() -> bool:
    try:
        import lxml  # noqa: F401
        return True
    except Exception:
        return False


def rewrite_image_srcs(html_content: str, dl_images: list[dict]) -> str:
    """把 HTML 里 data-src 指向 mmbiz.qpic.cn 的 src 改成 local_path 引用。
    rewrite 后的 HTML 用于 markdownify,生成的 markdown 里图片就指向本地路径。
    没下成功的图片保留原 URL。"""
    if not html_content or not dl_images:
        return html_content
    by_src = {x.get("src"): x for x in dl_images if x.get("src")}
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html_content, "lxml" if _has_lxml() else "html.parser")
    for img in soup.find_all("img"):
        src = img.get("data-src") or img.get("src") or ""
        if src.startswith("//"):
            src = "https:" + src
        info = by_src.get(src)
        if info and info.get("local_path"):
            new = "wechat_mp_images/" + info["local_path"]
            img["src"] = new
            if "data-src" in img.attrs:
                img["data-src"] = new
    return str(soup)


# ==================== Mongo ====================

def get_db(uri: str = MONGO_URI_DEFAULT, db: str = MONGO_DB_DEFAULT):
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    return client, client[db]


def ensure_indexes(db) -> None:
    db[COL_ARTICLES].create_index([("biz", 1), ("release_time_ms", -1)])
    db[COL_ARTICLES].create_index([("release_time_ms", -1)])
    db[COL_ARTICLES].create_index([("account_name", 1), ("release_time_ms", -1)])
    db[COL_ARTICLES].create_index([("_canonical_tickers", 1)])
    db[COL_ARTICLES].create_index([("url", 1)], sparse=True)


def stable_id(biz: str | None, appmsgid: Any, itemidx: Any, sn: str | None) -> str:
    """优先 biz:appmsgid:itemidx, 兜底 sn。"""
    if biz and appmsgid is not None and itemidx is not None:
        return f"{biz}:{appmsgid}:{itemidx}"
    if sn:
        return f"sn:{sn}"
    raise ValueError("无法生成稳定 _id")


# ==================== checkpoint state ====================

def state_id(account_name: str) -> str:
    return f"crawler_account_{account_name}"


def load_state(db, account_name: str) -> dict:
    return db[COL_STATE].find_one({"_id": state_id(account_name)}) or {}


def save_state(db, account_name: str, **patch) -> None:
    db[COL_STATE].update_one(
        {"_id": state_id(account_name)},
        {"$set": {**patch, "updated_at": datetime.now(timezone.utc).isoformat()}},
        upsert=True,
    )


def bump_daily(db, account_name: str, n: int) -> None:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    db[COL_STATE].update_one(
        {"_id": f"daily_{account_name}_{today}"},
        {"$inc": {"new_articles": n},
         "$set": {"date": today, "account": account_name,
                  "updated_at": datetime.now(timezone.utc).isoformat()}},
        upsert=True,
    )


# ==================== accounts.yaml ====================

def load_accounts() -> list[dict]:
    if not ACCOUNTS_FILE.exists():
        return []
    data = yaml.safe_load(ACCOUNTS_FILE.read_text(encoding="utf-8")) or {}
    return [a for a in (data.get("accounts") or []) if a.get("enabled", True)]


def persist_account_fakeid(db, name: str, fakeid: str, meta: dict | None = None) -> None:
    """把 searchbiz 解析到的 fakeid 缓存到 Mongo account collection,避免每次启动都搜。"""
    db[COL_ACCOUNT].update_one(
        {"_id": name},
        {"$set": {"name": name, "fakeid": fakeid, "meta": meta or {},
                  "updated_at": datetime.now(timezone.utc).isoformat()}},
        upsert=True,
    )


def lookup_account_fakeid(db, name: str) -> Optional[str]:
    doc = db[COL_ACCOUNT].find_one({"_id": name})
    return (doc or {}).get("fakeid")


def resolve_fakeid(session: requests.Session, token: str, db, account: dict) -> str:
    """缓存优先,缺失时走 searchbiz。多个匹配时按完全相同的 nickname 优先。"""
    name = account["name"]
    cached = lookup_account_fakeid(db, name)
    if cached:
        return cached
    print(f"[searchbiz] {name} 未缓存, 调 searchbiz 查 fakeid")
    hits = search_account(session, token, name)
    if not hits:
        raise RuntimeError(f"searchbiz 没找到公众号: {name}")
    # 优先名字完全一致
    exact = [h for h in hits if h.get("nickname") == name]
    chosen = (exact or hits)[0]
    fakeid = chosen.get("fakeid")
    if not fakeid:
        raise RuntimeError(f"searchbiz 命中但无 fakeid: {chosen}")
    persist_account_fakeid(db, name, fakeid, meta={
        "nickname": chosen.get("nickname"),
        "alias": chosen.get("alias"),
        "signature": chosen.get("signature"),
        "round_head_img": chosen.get("round_head_img"),
        "service_type": chosen.get("service_type"),
    })
    print(f"[searchbiz] {name} → fakeid={fakeid}")
    return fakeid


# ==================== 单篇 dump ====================

def dump_one(db, session: requests.Session, account_name: str, list_item: dict,
             *, fetch_article: bool = True) -> Optional[dict]:
    """处理一条 app_msg_list 元素 → upsert articles。返回写入的 doc(或 None 跳过)。

    fetch_article=False 时只写 list_item 浅层信息,不抓正文(用于轻量 checkpoint
    更新,目前未使用)。
    """
    link = list_item.get("link") or ""
    parsed = parse_article_url(link)
    appmsgid = list_item.get("appmsgid")
    itemidx = list_item.get("itemidx")
    sn = parsed["sn"]
    biz = parsed["biz"] or list_item.get("biz")
    try:
        _id = stable_id(biz, appmsgid, itemidx, sn)
    except ValueError:
        print(f"  ⚠ 跳过: 无法生成 _id link={link!r}")
        return None

    # 已存在则跳过(--resume 真正的退出条件由调用方判断;dump_one 自身只负责
    # 幂等 upsert)
    existing = db[COL_ARTICLES].find_one({"_id": _id}, {"_id": 1, "content_md": 1})
    if existing and existing.get("content_md"):
        return existing

    create_ts = list_item.get("create_time") or list_item.get("update_time")
    release_time_ms_list = int(create_ts) * 1000 if create_ts else None

    doc: dict = {
        "_id": _id,
        "url": link,
        "biz": biz,
        "appmsgid": appmsgid,
        "itemidx": itemidx,
        "sn": sn,
        "account_name": account_name,
        "title": list_item.get("title") or "",
        "digest": list_item.get("digest") or "",
        "cover": list_item.get("cover") or "",
        "list_item": list_item,
        "release_time_ms": release_time_ms_list,
        "release_time": _ms_to_str(release_time_ms_list),
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
    }

    if fetch_article and link:
        try:
            html_str = fetch_article_html(session, link)
        except SessionDead:
            raise
        except Exception as e:
            print(f"  ⚠ 抓正文失败: {_id} err={e}")
            doc["fetch_error"] = str(e)[:300]
        else:
            parsed_art = parse_article_html(html_str)
            # 时间戳:页面解析比 list 接口更准
            if parsed_art.get("release_time_ms"):
                doc["release_time_ms"] = parsed_art["release_time_ms"]
                doc["release_time"] = _ms_to_str(parsed_art["release_time_ms"])
            doc["title"] = parsed_art.get("title") or doc["title"]
            doc["author"] = parsed_art.get("author") or ""
            if parsed_art.get("account_name") and not doc.get("account_name"):
                doc["account_name"] = parsed_art["account_name"]
            doc["html_raw"] = html_str if len(html_str) < 1_500_000 else html_str[:1_500_000]
            html_content = parsed_art.get("html_content") or ""
            images = parsed_art.get("images") or []
            if images and biz and sn:
                dl_images = _image_dl.download_many(images, biz, sn, root=IMAGE_ROOT)
                doc["images"] = dl_images
                # 改写 HTML 中的图片 src 为本地路径再 markdownify
                if html_content:
                    html_content = rewrite_image_srcs(html_content, dl_images)
                    doc["content_md"] = _coerce_to_markdown(html_content) or parsed_art.get("content_md", "")
                else:
                    doc["content_md"] = parsed_art.get("content_md", "")
            else:
                doc["images"] = []
                doc["content_md"] = parsed_art.get("content_md", "")

    # ticker 打标 (无 wechat_mp 专用 extractor 时, 内部回退到 title 正则)
    try:
        _stamp_ticker(doc, PLATFORM, db[COL_ARTICLES])
    except Exception as e:
        print(f"  ⚠ ticker_tag stamp 失败: {e}")

    db[COL_ARTICLES].update_one({"_id": _id}, {"$set": doc}, upsert=True)
    return doc


def _ms_to_str(ms: Optional[int]) -> Optional[str]:
    if not ms:
        return None
    try:
        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).astimezone(
            timezone(timedelta(hours=8))
        ).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return None


# ==================== 编排:resume / backfill / watch ====================

def run_once(db, session: requests.Session, token: str, account: dict,
             *, max_items: int = 0, resume: bool = True,
             since_ms: Optional[int] = None) -> int:
    """跑一遍单个公众号。返回新增数。

    - resume=True 命中 dupe 即停 (用于轮询)
    - max_items > 0 限制本次最多抓多少条 (冷启动)
    - since_ms 只抓 release_time_ms ≥ since_ms 的 (回填窗口)
    """
    name = account["name"]
    fakeid = resolve_fakeid(session, token, db, account)
    state = load_state(db, name)
    last_seen_ts = state.get("last_seen_release_time_ms") or 0
    last_seen_id = state.get("last_seen_id") or ""

    new_count = 0
    begin = 0
    PAGE = 5  # 官方上限

    print(f"\n→ {name} (fakeid={fakeid}) resume={resume} max={max_items} since_ms={since_ms}")

    while True:
        body = list_articles(session, token, fakeid, begin=begin, count=PAGE)
        items = body.get("app_msg_list") or []
        total = body.get("app_msg_cnt") or 0
        if not items:
            print(f"  · 翻完(begin={begin} total={total})")
            break

        for it in items:
            ts = (it.get("create_time") or it.get("update_time") or 0) * 1000
            link = it.get("link") or ""
            parsed = parse_article_url(link)
            try:
                doc_id = stable_id(parsed["biz"], it.get("appmsgid"), it.get("itemidx"), parsed["sn"])
            except ValueError:
                continue

            if since_ms and ts and ts < since_ms:
                # 翻到了窗口外, 直接结束(--backfill 6 个月时这个最先触发)
                print(f"  · {name} 触达 since_ms,停翻")
                save_state(db, name, last_run_at=datetime.now(timezone.utc).isoformat())
                bump_daily(db, name, new_count)
                return new_count

            existing = db[COL_ARTICLES].find_one({"_id": doc_id}, {"content_md": 1})
            if existing and existing.get("content_md") and resume:
                # resume 模式命中 dupe 立即终止整个翻页 (top-of-feed dedupe)
                print(f"  · {name} resume 命中 dupe {doc_id} → stop")
                save_state(db, name, last_run_at=datetime.now(timezone.utc).isoformat())
                bump_daily(db, name, new_count)
                return new_count

            try:
                doc = dump_one(db, session, name, it, fetch_article=True)
            except SessionDead:
                raise
            except Exception as e:
                print(f"  ⚠ dump_one 异常 {doc_id}: {e}")
                continue
            if doc:
                new_count += 1
                ts_ok = doc.get("release_time_ms") or ts
                if ts_ok and ts_ok > last_seen_ts:
                    last_seen_ts = ts_ok
                    last_seen_id = doc_id
                save_state(
                    db, name,
                    last_seen_release_time_ms=last_seen_ts,
                    last_seen_id=last_seen_id,
                    last_run_at=datetime.now(timezone.utc).isoformat(),
                )
                # 节流——文章页 fetch + 列表 fetch 已经各 wait 一次,这里不再加

            if max_items and new_count >= max_items:
                print(f"  · {name} 触达 max_items={max_items},停")
                bump_daily(db, name, new_count)
                return new_count

        begin += len(items)
        if total and begin >= total:
            print(f"  · 翻完(begin={begin}≥total={total})")
            break

    save_state(db, name, last_run_at=datetime.now(timezone.utc).isoformat())
    bump_daily(db, name, new_count)
    return new_count


def run_watch(db, creds: dict, accounts: list[dict],
              *, interval: int = 600, resume: bool = True) -> None:
    print(f"[watch] interval={interval}s, accounts={[a['name'] for a in accounts]}")
    while True:
        token = (_load_creds().get("token") or "").strip()
        if not token:
            print("[watch] credentials.json 缺 token, 等 30s 后重试 (请扫码登录)")
            time.sleep(30)
            continue
        session = create_session(_load_creds())
        try:
            for acc in accounts:
                try:
                    run_once(db, session, token, acc, resume=resume)
                except SessionDead as e:
                    print(f"[watch] session 死了: {e}, 退出 watch (需重新扫码)")
                    return
                except Exception as e:
                    print(f"[watch] {acc['name']} 异常: {e}, 跳过")
        finally:
            session.close()
        time.sleep(interval)


# ==================== CLI ====================

def show_state(db) -> None:
    print(f"--- credentials.json ---")
    creds = _load_creds()
    print(f"  token         = {(creds.get('token') or '')[:8]}…")
    print(f"  cookies count = {len(creds.get('cookies') or [])}")
    print(f"  updated_at    = {creds.get('updated_at')}")
    print(f"\n--- accounts ---")
    for acc in load_accounts():
        name = acc["name"]
        fakeid = lookup_account_fakeid(db, name) or "(未发现)"
        st = load_state(db, name)
        n = db[COL_ARTICLES].count_documents({"account_name": name})
        latest = db[COL_ARTICLES].find_one(
            {"account_name": name},
            sort=[("release_time_ms", -1)],
            projection={"title": 1, "release_time": 1},
        ) or {}
        print(f"  {name:<12} fakeid={fakeid}")
        print(f"    docs={n} last_run_at={st.get('last_run_at')}")
        if latest:
            print(f"    latest: [{latest.get('release_time')}] {latest.get('title','')[:60]}")
    print(f"\n--- throttle ---")
    log_config_stamp(_THROTTLE, budget=_BUDGET)


def main() -> int:
    p = argparse.ArgumentParser(description="微信公众号 (mp.weixin.qq.com) 直采爬虫")
    p.add_argument("--account", action="append", default=None,
                   help="只跑指定公众号 (可多次), 默认跑 accounts.yaml 全表")
    p.add_argument("--max", type=int, default=0,
                   help="单号本次最多抓多少篇, 0=不限")
    p.add_argument("--resume", action="store_true",
                   help="命中 dupe 即停 (轮询模式默认行为)")
    p.add_argument("--watch", action="store_true",
                   help="进入轮询模式, 每 --interval 秒跑一遍全表")
    p.add_argument("--interval", type=int, default=600,
                   help="轮询间隔秒, 默认 600")
    p.add_argument("--backfill", action="store_true",
                   help="回填模式, 配合 --since-days 一起用")
    p.add_argument("--since-days", type=int, default=180,
                   help="回填多少天 (默认 180)")
    p.add_argument("--show-state", action="store_true",
                   help="打印 checkpoint + token 健康")
    p.add_argument("--mongo-uri", default=MONGO_URI_DEFAULT)
    p.add_argument("--mongo-db", default=MONGO_DB_DEFAULT)

    add_antibot_args(p, default_base=3.0, default_jitter=2.0,
                     default_burst=30, default_cap=500)
    args = p.parse_args()

    # 应用 antibot CLI 参数 → 全局 _THROTTLE / _BUDGET
    global _THROTTLE, _BUDGET
    _THROTTLE = throttle_from_args(args, platform=PLATFORM)
    _BUDGET = budget_from_args(args, account_id="default", platform=PLATFORM)
    cap = cap_from_args(args)
    log_config_stamp(_THROTTLE, cap=cap, budget=_BUDGET)

    client, db = get_db(args.mongo_uri, args.mongo_db)
    try:
        ensure_indexes(db)
    except PyMongoError as e:
        print(f"⚠ ensure_indexes: {e}")

    if args.show_state:
        show_state(db)
        return 0

    creds = _load_creds()
    token = (creds.get("token") or "").strip()
    if not token:
        print("ERROR: credentials.json 缺 token — 先 python -m crawl.wechat_mp.auto_login")
        return 2

    accounts = load_accounts()
    if args.account:
        wanted = set(args.account)
        accounts = [a for a in accounts if a["name"] in wanted]
        if not accounts:
            print(f"ERROR: --account {args.account} 不在 accounts.yaml 白名单里")
            return 2
    if not accounts:
        print("ERROR: accounts.yaml 没有 enabled 条目")
        return 2

    if args.watch:
        run_watch(db, creds, accounts, interval=args.interval, resume=True)
        return 0

    session = create_session(creds)
    try:
        since_ms: Optional[int] = None
        if args.backfill:
            since_ms = int((datetime.now(timezone.utc)
                            - timedelta(days=args.since_days)).timestamp() * 1000)
            print(f"[backfill] since_days={args.since_days} since_ms={since_ms}")

        total_new = 0
        for acc in accounts:
            try:
                n = run_once(
                    db, session, token, acc,
                    max_items=args.max,
                    resume=args.resume or False,
                    since_ms=since_ms,
                )
                total_new += n
            except SessionDead as e:
                print(f"FATAL: session 死了 ({e}) — 请重新扫码登录")
                return 3
        print(f"\n=== 完成,共新增 {total_new} 篇 ===")
        return 0
    finally:
        session.close()
        client.close()


if __name__ == "__main__":
    sys.exit(main())
