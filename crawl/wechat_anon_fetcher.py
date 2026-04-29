"""Anonymous mp.weixin.qq.com article fetcher — no credentials, no MP backend.

The original wechat_mp/scraper.py uses an authenticated MP-backend session for
list APIs and article content. Many *other* crawlers (gangtise/researches,
gangtise/chief_opinions) reference wechat URLs as the "actual full body" of an
otherwise content=None upstream response. We can fetch those public article
URLs anonymously *if and only if* we (a) bypass any local proxy that might be
intercepting the request and (b) present a mobile-browser UA.

This module exists so those callers can opportunistically backfill body without
risking the MP backend account that wechat_mp/scraper.py uses.

Verified 2026-04-29: anonymous mobile-UA fetch returns HTTP 200 + 1.7k–30k
char ``js_content`` for gangtise-published broker reports.

Design constraints:
  * NEVER read or mutate the wechat_mp credentials. This is a *separate*
    requests.Session that does not load anything from credentials.json.
  * Force ``proxies={"http":"","https":""}`` on every request so Clash on
    127.0.0.1:7890 cannot eat the traffic (which was the source of the
    earlier "环境异常" misdiagnosis).
  * Default to ``https://`` even when the source URL says ``http://``, since
    weixin redirects http → https with cookies attached and we don't want
    cookies set.
  * Conservative pacing — ``MIN_INTERVAL_SEC`` between calls, plus a soft
    cooldown when we see "环境异常" or 429 (back off 5+ minutes).
  * Single-call API: ``fetch_article_anon(url)`` returns ``parse_article_html``
    output dict on success, or raises one of:
        ``WechatBlocked``    — wechat says 环境异常 / 链接已失效 / etc.
        ``WechatTransient``  — network error / 5xx / SSL flake
        ``WechatNotFound``   — 404 / 公众号文章被删除
        ``WechatUnknown``    — anything else
"""
from __future__ import annotations

import logging
import os
import re
import threading
import time
from typing import Optional

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger("wechat_anon_fetcher")


class WechatBlocked(RuntimeError):
    """Anti-bot wall: 环境异常 / 链接已失效 / 请在微信客户端打开."""


class WechatTransient(RuntimeError):
    """Network error / SSL / 5xx — retry-able later."""


class WechatNotFound(RuntimeError):
    """404 / 已被发布者删除 / 此内容已被发布者删除."""


class WechatUnknown(RuntimeError):
    """Anything else (200 but no js_content, malformed html, etc.)."""


_UA_MOBILE = (
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1"
)

# Pacing — conservative to avoid IP-based rate limit. ~30 articles per minute
# max if backfilling continuously. Adjust via env if needed.
MIN_INTERVAL_SEC = float(os.environ.get("WECHAT_ANON_INTERVAL_SEC", "2.0"))
COOLDOWN_AFTER_BLOCK_SEC = int(os.environ.get("WECHAT_ANON_BLOCK_COOLDOWN_SEC", "300"))
HTTP_TIMEOUT_SEC = int(os.environ.get("WECHAT_ANON_TIMEOUT_SEC", "20"))

_BLOCK_MARKERS = (
    "环境异常", "请在微信客户端打开", "请在微信中打开",
)
_DELETED_MARKERS = (
    "已被发布者删除", "此内容已被发布者删除", "由用户投诉,",
    "由相关投诉人", "无法浏览", "已被屏蔽",
)


class _RateLimiter:
    """Single-process rate limiter shared across all fetcher calls."""
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._last_call = 0.0
        self._cooldown_until = 0.0

    def before_call(self) -> None:
        with self._lock:
            now = time.time()
            wait_for_cooldown = max(0.0, self._cooldown_until - now)
            if wait_for_cooldown > 0:
                # Caller should have backed off; sleeping here is just a safety net.
                time.sleep(wait_for_cooldown)
                now = time.time()
            wait = max(0.0, MIN_INTERVAL_SEC - (now - self._last_call))
            if wait > 0:
                time.sleep(wait)
            self._last_call = time.time()

    def after_block(self) -> None:
        """Called when wechat returned a block marker — extend cooldown."""
        with self._lock:
            self._cooldown_until = max(self._cooldown_until, time.time() + COOLDOWN_AFTER_BLOCK_SEC)

    def in_cooldown(self) -> bool:
        return time.time() < self._cooldown_until

    def cooldown_remaining(self) -> float:
        return max(0.0, self._cooldown_until - time.time())


_LIMITER = _RateLimiter()


def _make_session() -> requests.Session:
    """Build a fresh anonymous Session — no cookies, no auth, no proxy."""
    s = requests.Session()
    s.headers.update({
        "User-Agent": _UA_MOBILE,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9",
    })
    # Force-disable any proxy at session level; explicit empty strings on per-
    # request `proxies` argument also override env proxies.
    s.trust_env = False  # ignores HTTP_PROXY / HTTPS_PROXY env vars
    s.proxies = {"http": "", "https": ""}
    return s


# Module-level singleton; cheap to share since we don't carry cookies.
_session: Optional[requests.Session] = None


def _get_session() -> requests.Session:
    global _session
    if _session is None:
        _session = _make_session()
    return _session


def fetch_article_anon(url: str, *, timeout: Optional[int] = None) -> dict:
    """Fetch an mp.weixin.qq.com article URL without any credentials.

    Returns the parse_article_html dict: title / author / account_name /
    release_time_ms / content_md / html_content / images.

    Raises WechatBlocked / WechatNotFound / WechatTransient / WechatUnknown.
    """
    if not url:
        raise WechatUnknown("empty url")
    if "mp.weixin.qq.com" not in url:
        raise WechatUnknown(f"not a wechat URL: {url[:60]}")

    if _LIMITER.in_cooldown():
        raise WechatBlocked(f"global cooldown active ({_LIMITER.cooldown_remaining():.0f}s)")

    # Force https — wechat redirects http→https anyway and the redirect can
    # set Set-Cookie that we'd carry forward; cleaner to just start at https.
    https_url = url.replace("http://", "https://", 1) if url.startswith("http://") else url

    sess = _get_session()
    _LIMITER.before_call()
    try:
        r = sess.get(
            https_url,
            timeout=timeout or HTTP_TIMEOUT_SEC,
            proxies={"http": "", "https": ""},  # belt-and-suspenders against env
            allow_redirects=True,
        )
    except requests.exceptions.SSLError as e:
        raise WechatTransient(f"ssl: {e}") from e
    except requests.exceptions.Timeout as e:
        raise WechatTransient(f"timeout: {e}") from e
    except requests.exceptions.RequestException as e:
        raise WechatTransient(f"network: {e}") from e

    if r.status_code == 404:
        raise WechatNotFound(f"http 404 for {https_url[:80]}")
    if r.status_code in (429, 503):
        _LIMITER.after_block()
        raise WechatBlocked(f"http {r.status_code} (rate-limited)")
    if 500 <= r.status_code < 600:
        raise WechatTransient(f"http {r.status_code}")
    if r.status_code != 200:
        raise WechatUnknown(f"http {r.status_code}")

    text = r.text
    for m in _BLOCK_MARKERS:
        if m in text:
            _LIMITER.after_block()
            raise WechatBlocked(m)
    for m in _DELETED_MARKERS:
        if m in text:
            raise WechatNotFound(m)

    parsed = _parse_article_html(text)
    body_md = (parsed.get("content_md") or "").strip()
    if not body_md:
        raise WechatUnknown("no js_content extracted (page may be ad/index/empty)")
    return parsed


# ---------------------------------------------------------------------------
# HTML → metadata + markdown.
#
# Public mp.weixin.qq.com article structure:
#   h1#activity-name           — title
#   #meta_content_author_nickname / .rich_media_meta_text — author
#   #js_name / a.rich_media_meta_nickname                 — public-account name
#   <div id="js_content">      — body (rich text)
#   var ct = "<epoch_seconds>"  — publish time
# ---------------------------------------------------------------------------

_CT_RE = re.compile(r'var\s+ct\s*=\s*"(\d+)"')
_PUBLISH_TIME_RE = re.compile(r'"publish_time"\s*:\s*"(\d+)"')


def _strip_to_markdown(html: str) -> str:
    """Convert wechat article body HTML to markdown."""
    if not html:
        return ""
    try:
        from markdownify import markdownify as _md
    except ImportError:
        # Fallback: bs4 plain-text — preserves line breaks.
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup(["script", "style"]):
            tag.decompose()
        return soup.get_text(separator="\n", strip=True)
    return _md(html, heading_style="ATX", strip=["script", "style"])


def _parse_article_html(html_str: str) -> dict:
    """Parse a public mp.weixin.qq.com article page into structured fields."""
    try:
        soup = BeautifulSoup(html_str, "lxml")
    except Exception:
        soup = BeautifulSoup(html_str, "html.parser")

    title = ""
    h1 = soup.select_one("h1#activity-name") or soup.select_one("h1.rich_media_title")
    if h1:
        title = h1.get_text(strip=True)

    author = ""
    a_node = (
        soup.select_one("#meta_content_author_nickname")
        or soup.select_one(".rich_media_meta_text")
    )
    if a_node:
        author = a_node.get_text(strip=True)

    account_name = ""
    js_name = soup.select_one("#js_name") or soup.select_one("a.rich_media_meta_nickname")
    if js_name:
        account_name = js_name.get_text(strip=True)

    release_time_ms: Optional[int] = None
    for pat in (_CT_RE, _PUBLISH_TIME_RE):
        m = pat.search(html_str)
        if m:
            try:
                release_time_ms = int(m.group(1)) * 1000
                break
            except Exception:
                pass

    body = soup.select_one("div#js_content") or soup.select_one("div.rich_media_content")
    images: list[dict] = []
    html_content = ""
    if body is not None:
        for img in body.find_all("img"):
            src = img.get("data-src") or img.get("src") or ""
            if src.startswith("//"):
                src = "https:" + src
            if src and not src.startswith("data:"):
                images.append({"src": src})
        html_content = str(body)

    content_md = _strip_to_markdown(html_content)

    return {
        "title": title,
        "author": author,
        "account_name": account_name,
        "release_time_ms": release_time_ms,
        "content_md": content_md,
        "html_content": html_content,
        "images": images,
    }


__all__ = [
    "fetch_article_anon",
    "WechatBlocked", "WechatTransient", "WechatNotFound", "WechatUnknown",
]
