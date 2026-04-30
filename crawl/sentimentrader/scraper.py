#!/usr/bin/env python3
"""SentimenTrader chart scraper (MongoDB store).

Scrapes three paid chart pages the user has a subscription to:
  - model_smart_dumb_spread  -> Smart Money / Dumb Money Confidence Spread
  - model_cnn_fear_greed     -> Fear & Greed Model
  - etf_qqq                  -> QQQ Optix (sentiment / optimism index)

The three pages render time-series data via Highcharts. We log in once with
Playwright (reused via storage_state.json), navigate to each chart, and
read `window.Highcharts.charts[0].series` directly — no need to reverse any
XHR. Docs land in MongoDB `sentimentrader.indicators`, one row per indicator.

Usage:
    python scraper.py --once             # fetch all 3 indicators once
    python scraper.py --watch --interval 86400   # run daily
    python scraper.py --show-state       # print last update per indicator
    python scraper.py --force-login      # ignore saved session, re-login
    SENTIMENTRADER_EMAIL / SENTIMENTRADER_PASSWORD env vars override --email/--password
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from playwright.async_api import async_playwright, Browser, BrowserContext, Page

CRAWL_DIR = Path(__file__).resolve().parent
PLAYWRIGHT_DATA = CRAWL_DIR / "playwright_data"
PLAYWRIGHT_DATA.mkdir(exist_ok=True)
STORAGE_STATE = PLAYWRIGHT_DATA / "storage_state.json"
LOG_DIR = CRAWL_DIR.parent.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)
PROJECT_ROOT = CRAWL_DIR.parent.parent
# Screenshots of the Highcharts container go here — large-ish PNGs (~50-150KB
# each) kept outside the repo. Matches the alphapai/jinmen/gangtise PDF pattern.
IMG_DIR = Path(os.environ.get("SENTIMENTRADER_IMAGE_DIR", "/home/ygwang/crawl_data/sentimentrader_images"))
IMG_DIR.mkdir(parents=True, exist_ok=True)

LOGIN_URL = "https://sentimentrader.com/login"
USERS_HOME = "https://users.sentimentrader.com/users/"

# slug -> (page url, stable human-readable name, benchmark label, secondary_series_name_or_None)
# The fifth field names a SECOND indicator line to capture alongside the primary
# one (e.g. smart_dumb shows Smart Money AND Dumb Money on the same axis).
# Pass None for single-indicator charts.
TARGETS: list[tuple[str, str, str, str, str | None]] = [
    ("smart_dumb_spread",
     "https://users.sentimentrader.com/users/charts/model_smart_dumb_spread",
     "Smart Money / Dumb Money Confidence Spread",
     "SPX", None),
    ("cnn_fear_greed",
     "https://users.sentimentrader.com/users/charts/model_cnn_fear_greed",
     "Fear & Greed Model",
     "SPX", None),
    ("etf_qqq",
     "https://users.sentimentrader.com/users/charts/etf_qqq",
     "QQQ Optix",
     "QQQ", None),
    # Shows the two raw confidence lines (Smart + Dumb) instead of their spread.
    ("smart_dumb",
     "https://users.sentimentrader.com/users/charts/smart_dumb",
     "Smart Money / Dumb Money Confidence",
     "SPX", "Dumb Money"),
]

# Windows UA — SentimenTrader's paying users are overwhelmingly on Windows/Mac
# desktops. Linux UA stood out as a bot-ish signal and made Cloudflare slower
# to greenlight us. Kept Chrome 124 to match our Playwright binary's reported
# version (detected mismatches between UA-claimed and real Chromium ping the
# same WAFs).
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)


# ──────────────────────────────────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────────────────────────────────

def _log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    try:
        (LOG_DIR / "sentimentrader.log").open("a", encoding="utf-8").write(line + "\n")
    except Exception:
        pass


# ──────────────────────────────────────────────────────────────────────────
# Proxy resolution & preflight
# ──────────────────────────────────────────────────────────────────────────
#
# The host is GFW-bound and SentimenTrader's login + chart pages embed
# www.google.com/recaptcha/api.js. Without a working forward proxy the page
# never resolves and every goto times out at the wall-clock deadline.
#
# We don't trust the inherited env blindly — cron historically hardcoded a
# now-dead Clash forward (127.0.0.1:7890). Instead we (a) read the project
# `.env` file when env vars are missing, (b) actively probe the resolved
# proxy URL, (c) refuse to launch Chromium against a dead proxy.

_DOTENV_KEYS = ("HTTPS_PROXY", "HTTP_PROXY", "ALL_PROXY", "NO_PROXY")


def _load_env_proxy_from_dotenv() -> None:
    """If proxy env vars aren't already set, copy them from the project's .env.

    `start_web.sh::_load_env_var` does the same thing for the web service;
    cron has historically `export`ed proxy explicitly, but the proxy URL
    changes faster than crontabs do, so the .env is now the single source of
    truth. Only sets keys that are *missing*; existing env values win.
    """
    if any(os.environ.get(k) or os.environ.get(k.lower()) for k in ("HTTPS_PROXY", "HTTP_PROXY")):
        return
    env_file = PROJECT_ROOT / ".env"
    if not env_file.exists():
        return
    try:
        for raw in env_file.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            key = key.strip()
            val = val.strip().strip('"').strip("'")
            if key in _DOTENV_KEYS and val:
                os.environ.setdefault(key, val)
                os.environ.setdefault(key.lower(), val)
    except Exception as e:
        _log(f"WARN: failed to read .env for proxy: {e}")


def _resolve_proxy() -> str | None:
    return (
        os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY")
        or os.environ.get("https_proxy") or os.environ.get("http_proxy")
        or None
    )


def _proxy_alive(proxy_url: str, timeout: float = 6.0) -> tuple[bool, str]:
    """Verify the proxy can actually reach SentimenTrader.

    A "listening but dead" forward (e.g. retired Clash on 127.0.0.1:7890 — TCP
    accepts the connect, the upstream is gone) returns success on a socket
    probe but hangs on real traffic. So we issue an HTTPS request through the
    proxy with a hard timeout and only treat 2xx/3xx/4xx as alive.

    Probe target is sentimentrader's own login page — minimal payload, the
    only host we actually care about. Returns (alive, detail_message).
    """
    try:
        import urllib.request
        import urllib.error
        opener = urllib.request.build_opener(
            urllib.request.ProxyHandler({"http": proxy_url, "https": proxy_url})
        )
        req = urllib.request.Request(
            "https://sentimentrader.com/login",
            headers={"User-Agent": USER_AGENT},
        )
        with opener.open(req, timeout=timeout) as resp:
            code = resp.status
            return (200 <= code < 500), f"probe {proxy_url} → HTTP {code}"
    except urllib.error.HTTPError as e:
        # 4xx still means the proxy works — the upstream just rejected us.
        return (e.code < 500), f"probe {proxy_url} → HTTP {e.code}"
    except Exception as e:
        return False, f"probe {proxy_url} → {type(e).__name__}: {e}"


# ──────────────────────────────────────────────────────────────────────────
# Playwright session
# ──────────────────────────────────────────────────────────────────────────

async def _new_context(pw, headless: bool = True) -> tuple[Browser, BrowserContext]:
    # SentimenTrader's login + chart pages embed www.google.com/recaptcha/api.js
    # which is GFW-blocked from the China host. Without a proxy the script
    # never resolves, DOMContentLoaded never fires, and goto() times out at
    # 45s. Read the proxy from env (cron must set HTTPS_PROXY) and pass it
    # to chromium.launch explicitly — Chromium does not always inherit
    # *_PROXY env vars in headless mode.
    launch_kwargs: dict[str, Any] = {"headless": headless, "args": ["--no-sandbox"]}
    proxy_url = (
        os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY")
        or os.environ.get("https_proxy") or os.environ.get("http_proxy")
    )
    if proxy_url:
        launch_kwargs["proxy"] = {"server": proxy_url}
    browser = await pw.chromium.launch(**launch_kwargs)
    # Locale aligned to SentimenTrader's US subscriber base (en-US / NY).
    # Mismatched defaults (UTC timezone in headless Chromium) is a subtle
    # fingerprint tell; also Cloudflare + SentimenTrader's own session
    # fingerprinting key on it.
    kwargs: dict[str, Any] = dict(
        user_agent=USER_AGENT,
        viewport={"width": 1440, "height": 900},
        locale="en-US",
        timezone_id="America/New_York",
        extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
    )
    if STORAGE_STATE.exists():
        kwargs["storage_state"] = str(STORAGE_STATE)
    ctx = await browser.new_context(**kwargs)
    ctx.set_default_timeout(30_000)
    return browser, ctx


async def _is_logged_in(page: Page) -> bool:
    """Heuristic: after goto, are we on the users.* dashboard (vs a login screen)?"""
    try:
        url = page.url or ""
        if "sentimentrader.com/login" in url:
            return False
        if "users.sentimentrader.com/users" in url:
            return True
        # Fallback: look for a logout link.
        logout = await page.locator('a:has-text("Log out"), a:has-text("Logout"), a[href*="logout"]').count()
        return logout > 0
    except Exception:
        return False


async def _do_login(page: Page, email: str, password: str) -> None:
    """Fill the login form at sentimentrader.com/login."""
    _log(f"logging in as {email}")
    await page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=60_000)
    await page.wait_for_timeout(2000)

    email_selectors = [
        'input[name="email"]',
        'input[type="email"]',
        'input#email',
        'input[placeholder*="mail" i]',
    ]
    pass_selectors = [
        'input[name="password"]',
        'input[type="password"]',
        'input#password',
    ]
    submit_selectors = [
        'button[type="submit"]',
        'input[type="submit"]',
        'button:has-text("Sign in")',
        'button:has-text("Log in")',
        'button:has-text("Login")',
    ]

    async def first_visible(selectors):
        for s in selectors:
            try:
                loc = page.locator(s).first
                if await loc.is_visible(timeout=1500):
                    return loc
            except Exception:
                continue
        return None

    email_el = await first_visible(email_selectors)
    pass_el = await first_visible(pass_selectors)
    if not email_el or not pass_el:
        raise RuntimeError("could not find login email/password fields — site layout changed?")

    await email_el.fill(email)
    await pass_el.fill(password)
    submit = await first_visible(submit_selectors)
    if submit:
        await submit.click()
    else:
        await pass_el.press("Enter")

    try:
        await page.wait_for_load_state("networkidle", timeout=45_000)
    except Exception:
        pass
    await page.wait_for_timeout(2500)

    if not await _is_logged_in(page):
        raise RuntimeError(f"login did not redirect to users dashboard (now at {page.url})")
    _log(f"login ok, landed at {page.url}")


# ──────────────────────────────────────────────────────────────────────────
# Subscription-state detection
# ──────────────────────────────────────────────────────────────────────────


class SubscriptionInactive(Exception):
    """Raised when the chart/users page is the "Subscription not active" landing.

    SentimenTrader serves a 403 page with a generic "Your Subscription is not
    Active" body when an account is expired or cancelled — the URL doesn't
    redirect, the chart container simply isn't present. Without explicit
    detection, our retry loop wastes 3×60 s per chart slugging away at a page
    that will never render Highcharts. We detect early and bail the run with
    a status that the credential probe can surface as
    "❌ 订阅已过期 / 账户已取消 — 需续订".
    """


_SUBSCRIPTION_DEAD_MARKERS: tuple[str, ...] = (
    "your subscription is not active",
    "your account has either expired or has been canceled",
    "your account has been canceled",
    "your account has expired",
    "subscription has expired",
    "subscription is not active",
)


async def _detect_subscription_expired(page: Page) -> tuple[bool, str]:
    """Probe the current page for the subscription-dead landing.

    Returns (is_expired, evidence). The evidence string is the matched marker
    truncated to a reasonable length so it's safe to drop into a Mongo doc /
    operator-facing badge without leaking the whole 17 KB landing page.
    """
    try:
        text = await page.evaluate("document.body && document.body.innerText || ''")
    except Exception:
        return False, ""
    if not text:
        return False, ""
    text_lc = text.lower()
    for marker in _SUBSCRIPTION_DEAD_MARKERS:
        if marker in text_lc:
            return True, marker
    return False, ""


# ──────────────────────────────────────────────────────────────────────────
# Chart extraction
# ──────────────────────────────────────────────────────────────────────────

# JS payload: read all Highcharts series off the first chart on the page.
# We keep this self-contained and defensive: if any series has no .options.data
# we fall back to .points.
_EXTRACT_JS = r"""
() => {
  const out = { charts: [] };
  if (!window.Highcharts || !window.Highcharts.charts) return out;
  for (const c of window.Highcharts.charts) {
    if (!c) continue;
    const chart = {
      title: (c.title && c.title.textStr) || '',
      y_axis_titles: (c.yAxis || []).map(a => ((a.options || {}).title || {}).text || ''),
      series: [],
    };
    for (const s of (c.series || [])) {
      try {
        let pts = (s.options && s.options.data) ? s.options.data : null;
        if (!pts && s.points) pts = s.points.map(p => [p.x, p.y]);
        chart.series.push({
          name: s.name || '',
          type: s.type || '',
          visible: s.visible !== false,
          data: Array.isArray(pts) ? pts : [],
        });
      } catch (e) { chart.series.push({ err: String(e) }); }
    }
    out.charts.push(chart);
  }
  return out;
}
"""


async def _extract_chart(page: Page, slug: str, url: str,
                          max_attempts: int = 3,
                          hydration_deadline_s: float = 60.0) -> dict:
    """Navigate to chart, read the full Highcharts series, and screenshot the
    rendered chart container so we can show the official image on the UI.

    Retries up to ``max_attempts`` times on hydration failures. Each attempt
    does a fresh goto with ``hydration_deadline_s`` worth of polling.
    Highcharts on this site has been observed taking 30-50 s to hydrate when
    the proxy is slow; the previous 25 s hard deadline was the silent root
    cause of the 04-28→04-29 wave of "no Highcharts.charts found" failures.
    """
    probe: dict | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            _log(f"[{slug}] goto {url} (attempt {attempt}/{max_attempts})")
            await page.goto(url, wait_until="domcontentloaded", timeout=90_000)

            # Cheap subscription-dead check before we burn 60 s polling for
            # Highcharts that will never appear. If the page is the "your
            # subscription is not active" landing, abort the entire run —
            # no slug-level retry will help.
            expired, marker = await _detect_subscription_expired(page)
            if expired:
                raise SubscriptionInactive(
                    f"[{slug}] subscription not active (matched: '{marker}')"
                )

            deadline = time.time() + hydration_deadline_s
            probe = None
            while time.time() < deadline:
                probe = await page.evaluate(_EXTRACT_JS)
                if probe and probe.get("charts"):
                    charts = probe["charts"]
                    if charts and charts[0].get("series"):
                        if any((s.get("data") or []) for s in charts[0]["series"]):
                            break
                await page.wait_for_timeout(500)
            if not probe or not probe.get("charts"):
                # Re-check at hydration deadline: SPA could have rendered the
                # subscription-dead landing client-side after a delay.
                expired, marker = await _detect_subscription_expired(page)
                if expired:
                    raise SubscriptionInactive(
                        f"[{slug}] subscription not active (matched: '{marker}')"
                    )
                raise RuntimeError(f"[{slug}] no Highcharts.charts found on page")
            charts = probe.get("charts") or []
            if not charts or not charts[0].get("series"):
                raise RuntimeError(f"[{slug}] charts present but empty series")
            break
        except SubscriptionInactive:
            # Don't retry — the entire run is doomed; bubble up so run_once
            # can write a clear health doc and skip remaining slugs.
            raise
        except Exception as e:
            _log(f"[{slug}] attempt {attempt} failed: {e}")
            if attempt >= max_attempts:
                raise
            await page.wait_for_timeout(2000 + 1000 * attempt)
            continue

    # Let the chart finish rendering (legend, navigator, Bollinger bands).
    # Highcharts hydrates data first, then polishes the layout — 1s is enough
    # for the final frame based on observed renders.
    await page.wait_for_timeout(1200)

    # Screenshot the Highcharts container only (not the surrounding site
    # chrome). There's typically one `.highcharts-container` per page; fall
    # back to the parent wrapper if that selector isn't present.
    img_path = IMG_DIR / f"{slug}.png"
    for selector in (".highcharts-container", "#chartcontainer", "[class*='chart']"):
        try:
            el = await page.query_selector(selector)
            if el:
                # Larger viewport → sharper chart. The container itself is the
                # element we capture; Playwright handles clipping.
                await el.screenshot(path=str(img_path), type="png", timeout=15_000)
                probe["screenshot_path"] = str(img_path)
                break
        except Exception as e:
            _log(f"[{slug}] screenshot via {selector} failed: {e}")
            continue
    if "screenshot_path" not in probe:
        _log(f"[{slug}] WARN: no screenshot captured; UI will fall back to sparkline")

    return probe


# ──────────────────────────────────────────────────────────────────────────
# Data shaping
# ──────────────────────────────────────────────────────────────────────────

def _pick_indicator_and_benchmark(chart: dict, benchmark_name: str,
                                    secondary_name: str | None = None) -> dict:
    """Reduce a raw Highcharts dump to just what the API needs.

    Primary pick:
      - benchmark = series whose `name` stem matches benchmark_name (e.g. "SPX")
      - indicator = first "(Last = …)"-labelled series that isn't the benchmark

    When `secondary_name` is provided (e.g. "Dumb Money"), we also capture a
    second indicator series whose name contains that substring. This is how
    dual-line charts like `smart_dumb` surface both confidence lines.

    Returned shape (flat):
      chart_title, benchmark_name, benchmark_series, latest_benchmark_value,
      indicator_name, indicator_series, latest_value, latest_ts_ms,
      [optional] secondary_indicator_name, secondary_indicator_series, secondary_latest_value.
    """
    series = chart.get("series") or []

    def to_pairs(s):
        out = []
        for p in (s.get("data") or []):
            if isinstance(p, (list, tuple)) and len(p) >= 2 and p[0] is not None and p[1] is not None:
                try:
                    out.append([int(p[0]), float(p[1])])
                except Exception:
                    continue
        return out

    def name_stem(s: dict) -> str:
        """'SPX (Last = 7109.14)' → 'SPX'. Handles names with and without the suffix."""
        return (s.get("name") or "").split("(Last")[0].strip()

    benchmark = next((s for s in series if name_stem(s) == benchmark_name), None)
    if benchmark is None:
        # Fall back to first line series with data.
        benchmark = next((s for s in series if s.get("type") == "line" and (s.get("data") or [])), series[0] if series else {})

    # Indicator = first "(Last = …)" series that isn't the benchmark.
    # Needed because on some charts the benchmark ALSO has a "(Last = …)"
    # suffix (e.g. smart_dumb labels SPX as "SPX (Last = 7109.14)").
    indicator = None
    for s in series:
        if s is benchmark:
            continue
        name = s.get("name") or ""
        if "(Last" in name and s.get("data"):
            indicator = s
            break
    if indicator is None:
        for s in series:
            if s is benchmark:
                continue
            if s.get("type") == "line" and (s.get("data") or []):
                indicator = s
                break
    if indicator is None:
        raise RuntimeError("no indicator series found")

    bench_pairs = to_pairs(benchmark)
    ind_pairs = to_pairs(indicator)
    if not ind_pairs:
        raise RuntimeError("indicator series has no data points")

    latest_ts, latest_val = ind_pairs[-1]
    latest_bench_val = bench_pairs[-1][1] if bench_pairs else None

    result = {
        "chart_title": chart.get("title") or "",
        "benchmark_name": name_stem(benchmark) or benchmark_name,
        "benchmark_series": bench_pairs,
        "indicator_name": name_stem(indicator) or chart.get("title") or "",
        "indicator_series": ind_pairs,
        "latest_ts_ms": latest_ts,
        "latest_value": latest_val,
        "latest_benchmark_value": latest_bench_val,
    }

    if secondary_name:
        secondary = None
        # Match by substring; "Dumb Money" must find "Dumb Money (Last = 0.72)".
        for s in series:
            if s is benchmark or s is indicator:
                continue
            n = (s.get("name") or "").lower()
            if secondary_name.lower() in n and s.get("data"):
                secondary = s
                break
        if secondary is not None:
            sec_pairs = to_pairs(secondary)
            if sec_pairs:
                result["secondary_indicator_name"] = name_stem(secondary) or secondary_name
                result["secondary_indicator_series"] = sec_pairs
                result["secondary_latest_value"] = sec_pairs[-1][1]

    return result


def _trim_history(pairs: list, max_points: int = 750) -> list:
    """Keep at most max_points most-recent points."""
    if len(pairs) <= max_points:
        return pairs
    return pairs[-max_points:]


# ──────────────────────────────────────────────────────────────────────────
# MongoDB
# ──────────────────────────────────────────────────────────────────────────

def _mongo_collection(uri: str, db_name: str):
    from pymongo import MongoClient
    # 2026-04-23: 迁移期间 sentimentrader 合并进 funda DB (远端 u_spider 无权限
    # 创建 sentimentrader DB), 2026-04-26 迁回本机后保留同样布局以避免触发
    # 重新爬取与去重逻辑变更。当 db_name == "funda" 时 collection 用
    # "sentimentrader_indicators" 避撞; 旧 db_name == "sentimentrader" 时保持
    # "indicators"。
    coll_name = os.environ.get("SENTIMENTRADER_COLLECTION")
    if not coll_name:
        coll_name = "sentimentrader_indicators" if db_name == "funda" else "indicators"
    return MongoClient(uri, serverSelectionTimeoutMS=5000)[db_name][coll_name]


def _write_health(col, status: str, ok_count: int, fail_count: int,
                  error: str | None = None,
                  proxy_used: str | None = None) -> None:
    """Persist a single ``_id="_health"`` doc summarising the latest run.

    Drives the dashboard's "Last success / 疑似失败" badge via
    `_probe_sentimentrader` in credential_manager. Always written, even when
    the scrape fails outright — silent failures were what made the original
    storage_state-mtime-only probe so deceptive (the file got refreshed even
    when the site never returned valid data).
    """
    now = datetime.now(timezone.utc)
    full_success = (fail_count == 0 and ok_count > 0)
    update: dict[str, Any] = {
        "$set": {
            "_id": "_health",
            "kind": "health",
            "last_attempt_at": now,
            "last_status": status,
            "last_ok_count": ok_count,
            "last_fail_count": fail_count,
            "last_error": error,
            "last_proxy": proxy_used,
        },
    }
    if full_success:
        update["$set"]["last_success_at"] = now
        update["$set"]["consecutive_failures"] = 0
    else:
        # Increment failure streak so credential_manager can reason about
        # "1-off blip vs sustained outage" without scanning logs.
        update["$inc"] = {"consecutive_failures": 1}
        update["$set"]["last_failure_at"] = now
    try:
        col.update_one({"_id": "_health"}, update, upsert=True)
    except Exception as e:
        _log(f"WARN: could not write health doc: {e}")


def _upsert_indicator(col, slug: str, name: str, url: str, shaped: dict,
                      screenshot_path: str | None = None) -> None:
    now = datetime.now(timezone.utc)
    doc = {
        "_id": slug,
        "slug": slug,
        "name": name,
        "source_url": url,
        "chart_title": shaped["chart_title"],
        "indicator_name": shaped["indicator_name"],
        "benchmark_name": shaped["benchmark_name"],
        "latest_value": shaped["latest_value"],
        "latest_ts_ms": shaped["latest_ts_ms"],
        "latest_benchmark_value": shaped["latest_benchmark_value"],
        "history_trimmed": _trim_history(shaped["indicator_series"], 750),
        "benchmark_trimmed": _trim_history(shaped["benchmark_series"], 750),
        "full_point_count": len(shaped["indicator_series"]),
        "screenshot_path": screenshot_path,
        "updated_at": now,
        # Shared field used by backend credential_manager._probe_data_freshness
        # to compute "last_data_at" for the 数据源管理 page.
        "crawled_at": now,
    }
    # Dual-line charts: add secondary indicator fields when present.
    if "secondary_indicator_series" in shaped:
        doc["secondary_indicator_name"] = shaped["secondary_indicator_name"]
        doc["secondary_latest_value"] = shaped["secondary_latest_value"]
        doc["secondary_history_trimmed"] = _trim_history(
            shaped["secondary_indicator_series"], 750
        )
    col.replace_one({"_id": slug}, doc, upsert=True)


# ──────────────────────────────────────────────────────────────────────────
# One-shot runner
# ──────────────────────────────────────────────────────────────────────────

async def run_once(email: str, password: str, mongo_uri: str, mongo_db: str,
                   headless: bool = True, force_login: bool = False) -> dict:
    """Do a single full refresh of all indicators. Returns a summary dict.

    Stability guarantees written into this function:

    1. **Proxy preflight.** Resolves the forward proxy from env (or .env if
       env is bare), then hits sentimentrader's login URL through it with a
       6 s timeout. If dead — no Chromium is launched, a health doc records
       the reason, and the function exits early. Saves ~3 minutes per
       attempt vs. discovering the dead proxy via Chromium's full goto
       timeout chain.
    2. **Force-relogin recovery.** If the warm-up goto fails to land us on
       the user dashboard *and* a stored session exists, we wipe the
       session and re-login fresh. Saved sessions go stale silently
       (Cloudflare cookie rotation, JWT TTL).
    3. **Per-chart retry.** ``_extract_chart`` does up to 3 reload attempts
       with 60 s hydration deadline each — see its docstring for context.
    4. **Health doc.** Writes ``_id="_health"`` to the indicators
       collection with success/failure counts every run.
    """
    col = _mongo_collection(mongo_uri, mongo_db)
    summary: dict[str, Any] = {
        "ok": [],
        "failed": [],
        "started_at": datetime.now(timezone.utc).isoformat(),
    }

    # ── Step 1: Proxy resolution + preflight ─────────────────────────────
    _load_env_proxy_from_dotenv()
    proxy_url = _resolve_proxy()
    if not proxy_url:
        msg = "no HTTPS_PROXY configured (env or .env) — refusing to launch (recaptcha is GFW-blocked)"
        _log(f"FATAL: {msg}")
        _write_health(col, "no_proxy", 0, len(TARGETS), error=msg, proxy_used=None)
        summary["failed"] = [{"slug": s[0], "error": "no_proxy"} for s in TARGETS]
        summary["fatal"] = msg
        summary["finished_at"] = datetime.now(timezone.utc).isoformat()
        return summary

    alive, detail = _proxy_alive(proxy_url, timeout=6.0)
    _log(f"proxy preflight: {detail} → {'ALIVE' if alive else 'DEAD'}")
    if not alive:
        msg = f"proxy {proxy_url} is dead: {detail}"
        _log(f"FATAL: {msg}")
        _write_health(col, "proxy_dead", 0, len(TARGETS), error=msg, proxy_used=proxy_url)
        summary["failed"] = [{"slug": s[0], "error": "proxy_dead"} for s in TARGETS]
        summary["fatal"] = msg
        summary["finished_at"] = datetime.now(timezone.utc).isoformat()
        return summary

    # ── Step 2: Drive Playwright ─────────────────────────────────────────
    crash_err: str | None = None
    async with async_playwright() as pw:
        if force_login and STORAGE_STATE.exists():
            STORAGE_STATE.unlink()

        browser, ctx = await _new_context(pw, headless=headless)
        try:
            page = await ctx.new_page()

            # Warm-up: see if the saved session is still valid. If not, log in.
            warmup_ok = False
            try:
                await page.goto(USERS_HOME, wait_until="domcontentloaded", timeout=60_000)
                await page.wait_for_timeout(2000)
                warmup_ok = True
            except Exception as e:
                _log(f"warm-up goto failed: {e} — will force relogin")

            need_login = (not warmup_ok) or (not await _is_logged_in(page))
            if need_login:
                # Stored session may be silently stale; wipe and login fresh.
                if STORAGE_STATE.exists():
                    try:
                        STORAGE_STATE.unlink()
                        _log("wiped stale storage_state for fresh login")
                    except Exception:
                        pass
                await _do_login(page, email, password)

            subscription_dead = False
            subscription_marker = ""
            for slug, url, human, bench, secondary in TARGETS:
                try:
                    dump = await _extract_chart(page, slug, url)
                    if not dump.get("charts"):
                        raise RuntimeError("empty charts dump")
                    shaped = _pick_indicator_and_benchmark(dump["charts"][0], bench, secondary)
                    _upsert_indicator(col, slug, human, url, shaped,
                                       screenshot_path=dump.get("screenshot_path"))
                    latest_iso = datetime.fromtimestamp(shaped["latest_ts_ms"] / 1000, tz=timezone.utc).date().isoformat()
                    sec_note = ""
                    if "secondary_latest_value" in shaped:
                        sec_note = f"  secondary={shaped['secondary_indicator_name']}={shaped['secondary_latest_value']}"
                    _log(f"[{slug}] OK  latest={shaped['latest_value']} on {latest_iso}  "
                         f"benchmark={shaped['benchmark_name']}={shaped['latest_benchmark_value']}"
                         f"{sec_note}  points={len(shaped['indicator_series'])}")
                    summary["ok"].append({
                        "slug": slug,
                        "latest_value": shaped["latest_value"],
                        "latest_date": latest_iso,
                    })
                except SubscriptionInactive as e:
                    _log(f"FATAL: {e} — aborting remaining slugs (no point retrying)")
                    subscription_dead = True
                    subscription_marker = str(e)
                    # Mark the current slug failed AND every remaining slug,
                    # so the dashboard sees a clean "all 4 failed" picture.
                    summary["failed"].append({"slug": slug, "error": "subscription_inactive"})
                    remaining = [t[0] for t in TARGETS if t[0] != slug
                                 and t[0] not in {f["slug"] for f in summary["failed"]}
                                 and t[0] not in {o["slug"] for o in summary["ok"]}]
                    for r in remaining:
                        summary["failed"].append({"slug": r, "error": "subscription_inactive"})
                    break
                except Exception as e:
                    _log(f"[{slug}] FAIL: {e}")
                    summary["failed"].append({"slug": slug, "error": str(e)})

            if subscription_dead:
                # Bubble out before relogin tries. Stash on summary for the
                # health-doc write below.
                summary["subscription_inactive"] = True
                summary["subscription_marker"] = subscription_marker

            # If every chart failed but we hadn't tried a fresh login this
            # round, give it one more shot with a clean session — the
            # warm-up dashboard may have been served from a cached
            # auth cookie that wasn't actually authoritative for the chart
            # XHRs. (Observed once on 2026-04-25 after CF cookie rotation.)
            # Skip when subscription is inactive: a fresh login still lands
            # on the same 403 page, just wastes 30 s.
            if (len(summary["failed"]) == len(TARGETS) and not need_login
                    and not subscription_dead):
                _log("all 4 charts failed on cached session — retrying with fresh login")
                try:
                    if STORAGE_STATE.exists():
                        STORAGE_STATE.unlink()
                    await _do_login(page, email, password)
                except Exception as e:
                    _log(f"fresh-login retry failed: {e}")
                else:
                    retry_ok: list = []
                    retry_fail: list = []
                    for slug, url, human, bench, secondary in TARGETS:
                        try:
                            dump = await _extract_chart(page, slug, url)
                            shaped = _pick_indicator_and_benchmark(dump["charts"][0], bench, secondary)
                            _upsert_indicator(col, slug, human, url, shaped,
                                               screenshot_path=dump.get("screenshot_path"))
                            latest_iso = datetime.fromtimestamp(shaped["latest_ts_ms"] / 1000, tz=timezone.utc).date().isoformat()
                            _log(f"[{slug}] OK (after relogin) latest={shaped['latest_value']} on {latest_iso}")
                            retry_ok.append({"slug": slug, "latest_value": shaped["latest_value"], "latest_date": latest_iso})
                        except Exception as e:
                            _log(f"[{slug}] FAIL after relogin: {e}")
                            retry_fail.append({"slug": slug, "error": str(e)})
                    summary["ok"] = retry_ok
                    summary["failed"] = retry_fail
                    summary["recovered_via_relogin"] = bool(retry_ok)

            # Persist session for next run.
            try:
                await ctx.storage_state(path=str(STORAGE_STATE))
            except Exception as e:
                _log(f"could not save storage_state: {e}")
        except Exception as e:
            crash_err = f"{type(e).__name__}: {e}"
            _log(f"run_once playwright crash: {crash_err}")
        finally:
            try:
                await ctx.close()
            finally:
                await browser.close()

    # ── Step 3: write health regardless of outcome ───────────────────────
    ok_count = len(summary["ok"])
    fail_count = len(summary["failed"])
    if crash_err:
        status = "playwright_crash"
        # If we crashed before any chart finished, mark every target failed.
        if fail_count + ok_count == 0:
            summary["failed"] = [{"slug": s[0], "error": crash_err} for s in TARGETS]
            fail_count = len(summary["failed"])
    elif summary.get("subscription_inactive"):
        status = "subscription_inactive"
    elif fail_count == 0:
        status = "ok"
    elif ok_count == 0:
        status = "all_failed"
    else:
        status = "partial"

    if status == "subscription_inactive":
        # Distinct, action-oriented message — operator must renew the
        # SentimenTrader plan; no code-side fix is possible.
        error_msg = (
            "订阅已过期 / 账户已取消 — 请登录 sentimentrader.com 续订后再启动爬虫 "
            f"(检测匹配: {summary.get('subscription_marker') or 'subscription not active'})"
        )
    else:
        error_msg = crash_err or (
            "; ".join(f"{f['slug']}={f['error'][:80]}" for f in summary["failed"][:3]) if fail_count else None
        )
    _write_health(col, status, ok_count, fail_count, error=error_msg, proxy_used=proxy_url)
    summary["finished_at"] = datetime.now(timezone.utc).isoformat()
    summary["status"] = status
    return summary


# ──────────────────────────────────────────────────────────────────────────
# Retry-until-fresh runner (for cron-based scheduling)
# ──────────────────────────────────────────────────────────────────────────

async def run_until_fresh(email: str, password: str, mongo_uri: str, mongo_db: str,
                           headless: bool = True, force_login: bool = False,
                           max_retries: int = 8, retry_interval_s: int = 3600) -> dict:
    """Run one scrape; if the data didn't advance vs what's already stored,
    sleep retry_interval_s and try again. Cap at max_retries retries (so up
    to max_retries+1 total scrape attempts).

    This exists because sentimentrader publishes EOD data at variable times
    after US market close (typically 1–3 hours). A 06:00 CST cron might land
    right before their publish; we want to wait it out rather than miss a day.

    Returns summary with added keys: attempts, advanced (bool), before, after.
    """
    col = _mongo_collection(mongo_uri, mongo_db)

    def _snapshot() -> dict[str, int | None]:
        # Skip the `_health` sentinel — it doesn't carry indicator data.
        return {r["slug"]: r.get("latest_ts_ms")
                for r in col.find({"slug": {"$exists": True}}, {"slug": 1, "latest_ts_ms": 1})}

    before = _snapshot()
    _log(f"pre-scrape latest_ts_ms: {before or '(empty — first run)'}")

    last_summary: dict = {}
    for attempt in range(1, max_retries + 2):
        last_summary = await run_once(
            email=email, password=password,
            mongo_uri=mongo_uri, mongo_db=mongo_db,
            headless=headless, force_login=force_login,
        )
        force_login = False  # only honor on first attempt

        after = _snapshot()
        advanced_any = any(
            (after.get(s) or 0) > (before.get(s) or 0) for s in after
        )
        # First-ever run: no "before" to compare, so a successful scrape alone counts.
        fresh = advanced_any or (not before and after and last_summary.get("ok"))

        last_summary["attempts"] = attempt
        last_summary["advanced"] = bool(fresh)
        last_summary["before"] = before
        last_summary["after"] = after

        if fresh:
            _log(f"fresh data on attempt #{attempt}")
            return last_summary

        # Terminal failure modes — sleeping won't change the outcome. Bail
        # immediately so we don't burn ~3 hours of retry-until-fresh on a
        # dead proxy or an expired subscription.
        terminal = (last_summary.get("status") in
                    ("subscription_inactive", "no_proxy", "proxy_dead"))
        if terminal:
            _log(f"terminal status={last_summary.get('status')} — skipping further retries")
            return last_summary

        if attempt > max_retries:
            _log(f"gave up after {attempt} attempts — data still stale "
                 f"(after={after}). Source likely hasn't published yet "
                 f"or this is a non-trading day.")
            return last_summary

        _log(f"data unchanged on attempt #{attempt} — sleeping {retry_interval_s}s "
             f"(will try {max_retries + 1 - attempt} more time(s))")
        await asyncio.sleep(retry_interval_s)

    return last_summary


# ──────────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────────

def _show_state(mongo_uri: str, mongo_db: str) -> None:
    col = _mongo_collection(mongo_uri, mongo_db)
    rows = list(col.find({}, {"slug": 1, "indicator_name": 1, "latest_value": 1,
                              "latest_ts_ms": 1, "updated_at": 1}))
    if not rows:
        print("no indicators stored yet")
        return
    print(f"{'slug':<22} {'latest':>8}  {'data_date':<12}  updated_at")
    print("-" * 72)
    for r in rows:
        d = datetime.fromtimestamp((r.get("latest_ts_ms") or 0) / 1000, tz=timezone.utc).date().isoformat()
        u = r.get("updated_at")
        u_s = u.isoformat() if hasattr(u, "isoformat") else str(u)
        print(f"{r.get('slug',''):<22} {r.get('latest_value',''):>8}  {d:<12}  {u_s}")


def _load_creds_file() -> tuple[str, str]:
    """Fallback: load email/password from crawl/sentimentrader/credentials.json."""
    path = CRAWL_DIR / "credentials.json"
    if not path.exists():
        return "", ""
    try:
        d = json.loads(path.read_text(encoding="utf-8"))
        return (d.get("email") or "").strip(), (d.get("password") or "").strip()
    except Exception:
        return "", ""


def parse_args() -> argparse.Namespace:
    file_email, file_password = _load_creds_file()
    p = argparse.ArgumentParser()
    p.add_argument("--email", default=os.environ.get("SENTIMENTRADER_EMAIL", file_email))
    p.add_argument("--password", default=os.environ.get("SENTIMENTRADER_PASSWORD", file_password))
    p.add_argument("--mongo-uri", default=os.environ.get(
        "MONGO_URI",
        "mongodb://127.0.0.1:27018/",
    ))
    # sentimentrader 合并在 funda DB 的 sentimentrader_indicators 集合下
    p.add_argument("--mongo-db", default=os.environ.get("SENTIMENTRADER_DB", "funda"))
    p.add_argument("--once", action="store_true", default=True, help="(default) run one pass")
    p.add_argument("--watch", action="store_true", help="run on interval")
    p.add_argument("--interval", type=int, default=86400, help="watch interval seconds (default 86400 = daily)")
    p.add_argument("--force-login", action="store_true", help="ignore saved browser state and re-login")
    p.add_argument("--show-state", action="store_true", help="print stored indicator snapshot and exit")
    p.add_argument("--headful", action="store_true", help="show the browser (for debugging)")
    p.add_argument("--retry-until-fresh", action="store_true",
                   help="retry (up to --max-retries, every --retry-interval) until "
                        "the indicator timestamps advance past what's stored. "
                        "Exit 0 if fresh, 3 if gave up stale. Cron-friendly.")
    p.add_argument("--max-retries", type=int, default=8,
                   help="max retry attempts when using --retry-until-fresh (default 8 ≈ 8 h coverage)")
    p.add_argument("--retry-interval", type=int, default=3600,
                   help="seconds between retries when using --retry-until-fresh (default 3600 = 1 h)")
    return p.parse_args()


async def _amain(args: argparse.Namespace) -> int:
    if args.show_state:
        _show_state(args.mongo_uri, args.mongo_db)
        return 0

    if not args.email or not args.password:
        _log("ERROR: email/password required (use --email/--password or env SENTIMENTRADER_EMAIL/PASSWORD)")
        return 2

    # One-shot retry-until-fresh mode (used by cron). Exits 0 if we got fresh
    # data, 3 if we gave up stale, 1 on hard failure.
    if args.retry_until_fresh:
        try:
            summary = await run_until_fresh(
                email=args.email, password=args.password,
                mongo_uri=args.mongo_uri, mongo_db=args.mongo_db,
                headless=not args.headful, force_login=args.force_login,
                max_retries=args.max_retries, retry_interval_s=args.retry_interval,
            )
        except Exception as e:
            _log(f"run_until_fresh crashed: {e}")
            return 1
        _log(f"retry-until-fresh: attempts={summary.get('attempts')} "
             f"advanced={summary.get('advanced')} "
             f"status={summary.get('status')} "
             f"ok={len(summary.get('ok') or [])} failed={len(summary.get('failed') or [])}")
        if summary.get("advanced"):
            return 0
        # Surface the same exit-code taxonomy as the --once path so cron /
        # monitoring can distinguish "still no fresh data" (3) from
        # "subscription dead" (6) without parsing the log.
        st = summary.get("status")
        if st == "subscription_inactive":
            return 6
        if st in ("no_proxy", "proxy_dead"):
            return 3
        return 3

    last_status = "unknown"
    while True:
        try:
            summary = await run_once(
                email=args.email,
                password=args.password,
                mongo_uri=args.mongo_uri,
                mongo_db=args.mongo_db,
                headless=not args.headful,
                force_login=args.force_login,
            )
            last_status = summary.get("status", "unknown")
            _log(f"summary: status={last_status} ok={len(summary['ok'])} failed={len(summary['failed'])}")
            # Subsequent runs in watch mode shouldn't re-try to clobber the session
            args.force_login = False
        except Exception as e:
            last_status = "crash"
            _log(f"run_once crashed: {e}")

        if not args.watch:
            # Non-zero on hard failure so cron / flock / dashboards can tell.
            #   0 = at least one indicator updated this run
            #   3 = no_proxy / proxy_dead — operator must fix the env
            #   4 = all_failed — site likely changed or session truly stuck
            #   5 = playwright_crash / unknown crash
            #   6 = subscription_inactive — operator must renew the plan
            if last_status == "ok" or last_status == "partial":
                return 0
            if last_status in ("no_proxy", "proxy_dead"):
                return 3
            if last_status == "subscription_inactive":
                return 6
            if last_status == "all_failed":
                return 4
            return 5
        _log(f"sleeping {args.interval}s before next run…")
        await asyncio.sleep(args.interval)


def main() -> None:
    args = parse_args()
    sys.exit(asyncio.run(_amain(args)))


if __name__ == "__main__":
    main()
