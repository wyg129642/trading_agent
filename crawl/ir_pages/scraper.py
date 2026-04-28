#!/usr/bin/env python3
"""IR Pages scraper (Phase 2) — pulls **investor presentations / deck PDFs /
fact sheets** linked from per-company IR pages, into Mongo
``ir_filings.ir_pages``.

Why
---
Exchange filings (sec_edgar / hkex / asx) cover statutory disclosures, but
companies post **other revenue-modeling-relevant artifacts** only on their IR
pages: investor-day decks, segment fact sheets, capacity ramp slides, KPI
dashboards. This scraper sweeps the IR URLs already curated in
``config/portfolio_sources.yaml`` (entries tagged ``IR``) and downloads any
linked PDFs, attaching them to the unified ``ir_filings`` corpus so KB search
treats them as first-class citizens alongside the exchange filings.

Source URLs
-----------
``config/portfolio_sources.yaml`` IR section — 9 US tickers (Corning, Coherent,
Intel, WD, Micron, Bloom Energy, AAOI, TSMC, Alphabet) + 1 HK ticker (YOFC).
The YAML carries CSS selectors, requires_browser flag, and stock_ticker /
stock_market metadata that map cleanly to the IR-filings schema.

Flow per page
-------------
1. Fetch (Playwright if requires_browser=true, else plain HTTP).
2. Parse anchors with BeautifulSoup, filter for PDF candidates
   (href endswith .pdf, OR href matches the configured item_selector pattern).
3. For each candidate, derive a stable doc_id from the URL hash, dedupe vs Mongo,
   download new PDFs.
4. Best-effort title from anchor text + filename.
5. Best-effort release_time from URL path (YYYY/MM/DD or YYYYMMDD substrings),
   anchor text, or fall back to "now" (the only timestamp we can guarantee).

CLI
---
  python3 scraper.py --show-state
  python3 scraper.py --ticker GLW --max 5
  python3 scraper.py                          # all enabled IR pages
  python3 scraper.py --watch --interval 7200  # 2h poll loop
  python3 scraper.py --no-browser             # disable Playwright entirely
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, Iterable
from urllib.parse import urljoin, urlparse

import requests
import yaml

_HERE = Path(__file__).resolve().parent
_CRAWL_ROOT = _HERE.parent
_REPO_ROOT = _CRAWL_ROOT.parent
sys.path.insert(0, str(_REPO_ROOT))
sys.path.insert(0, str(_CRAWL_ROOT))

from crawl.ir_filings.common import (  # noqa: E402
    PDF_ROOT, ensure_indexes, get_collection, get_db,
    load_state, make_filing_doc, pdf_dir, record_daily_stat,
    safe_filename, save_state, setup_logging, upsert_filing,
)
from crawl.ir_filings.tickers import (  # noqa: E402
    BY_LISTING_CODE, IrTicker, ALL_TICKERS,
)

# Patch source registry (ir_pages is the 7th IR-filings source)
from crawl.ir_filings import common as _common  # noqa: E402
_common.COLLECTION_FOR_SOURCE.setdefault("ir_pages", "ir_pages")

SOURCE = "ir_pages"
IR_PAGES_PDF_ROOT = pdf_dir(SOURCE)
PORTFOLIO_YAML = _REPO_ROOT / "config" / "portfolio_sources.yaml"

THROTTLE_BASE_S = float(os.environ.get("IR_PAGES_THROTTLE", "3.0"))
THROTTLE_JITTER_S = 1.5

# Anchor-text → category heuristics (for the unified-schema `category` field)
CATEGORY_PATTERNS: list[tuple[re.Pattern, str, str]] = [
    (re.compile(r"earnings.*release|press release.*earnings", re.I), "earnings_release", "Earnings Release"),
    (re.compile(r"earnings.*deck|earnings.*presentation|earnings.*slides", re.I), "earnings_deck", "Earnings Deck"),
    (re.compile(r"investor (day|update|presentation|deck|brief)", re.I), "investor_presentation", "Investor Presentation"),
    (re.compile(r"fact sheet|financial summary|company overview", re.I), "fact_sheet", "Fact Sheet"),
    (re.compile(r"annual report|10-?k", re.I), "annual_report", "Annual Report"),
    (re.compile(r"q[1-4].*(report|results)|quarterly", re.I), "quarterly_report", "Quarterly Report"),
    (re.compile(r"transcript|conference call", re.I), "transcript", "Transcript"),
    (re.compile(r"esg|sustainability", re.I), "esg_report", "ESG Report"),
]

# Map portfolio_sources.yaml stock_market labels → market suffix
MARKET_MAP = {
    "美股":  "US",
    "港股":  "HK",
    "韩股":  "KR",
    "日股":  "JP",
    "澳股":  "AU",
}

UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

logger = setup_logging(SOURCE)

# ============================================================
# Source loader
# ============================================================

def load_ir_sources() -> list[dict]:
    """Return enabled portfolio_sources.yaml entries that look like IR pages.

    Filter: group=portfolio AND tags contains 'IR'. The author convention:
    "*投资者关系*" name + tags=[..., "IR"].
    """
    raw = yaml.safe_load(PORTFOLIO_YAML.read_text(encoding="utf-8")) or {}
    items = (raw.get("sources") or [])
    out: list[dict] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        if not it.get("enabled"):
            continue
        if (it.get("group") or "") != "portfolio":
            continue
        tags = it.get("tags") or []
        if "IR" not in tags:
            continue
        out.append(it)
    return out


def resolve_ticker(entry: dict) -> Optional[IrTicker]:
    """Find the IrTicker that matches a YAML IR entry. Cross-reference by
    (market, listing_code)."""
    raw_market = entry.get("stock_market") or ""
    market_suffix = MARKET_MAP.get(raw_market)
    if not market_suffix:
        return None
    code = str(entry.get("stock_ticker") or "")
    return BY_LISTING_CODE.get((market_suffix, code))


# ============================================================
# Fetching — plain HTTP + Playwright fallback
# ============================================================

class FetchError(Exception):
    pass


def _make_http_session() -> requests.Session:
    s = requests.Session()
    s.trust_env = False
    s.proxies = {}
    s.headers.update({
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9,zh-CN;q=0.8",
    })
    return s


def fetch_html_simple(s: requests.Session, url: str) -> str:
    r = s.get(url, timeout=60)
    if r.status_code != 200:
        raise FetchError(f"HTTP {r.status_code}")
    if "text/html" not in (r.headers.get("content-type") or ""):
        raise FetchError(f"unexpected content-type: {r.headers.get('content-type')}")
    return r.text


_PLAYWRIGHT_BROWSER = None


async def _ensure_playwright():
    global _PLAYWRIGHT_BROWSER
    if _PLAYWRIGHT_BROWSER is not None:
        return _PLAYWRIGHT_BROWSER
    try:
        from playwright.async_api import async_playwright
    except ImportError as e:
        raise FetchError(f"playwright not installed: {e}")
    p = await async_playwright().start()
    browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
    _PLAYWRIGHT_BROWSER = (p, browser)
    return _PLAYWRIGHT_BROWSER


async def fetch_html_browser(url: str, *, timeout_ms: int = 30000,
                              wait_until: str = "domcontentloaded",
                              extra_wait_ms: int = 2000) -> str:
    """Render JS-heavy IR pages via Playwright. Reuses one Chromium across the
    run."""
    p, browser = await _ensure_playwright()
    context = await browser.new_context(
        user_agent=UA,
        ignore_https_errors=True,
        viewport={"width": 1280, "height": 900},
    )
    page = await context.new_page()
    try:
        await page.goto(url, timeout=timeout_ms, wait_until=wait_until)
        if extra_wait_ms:
            await page.wait_for_timeout(extra_wait_ms)
        html = await page.content()
        return html
    finally:
        await page.close()
        await context.close()


# ============================================================
# Link extraction
# ============================================================

_DATE_PATS = [
    re.compile(r"(20\d{2})[-/_](\d{1,2})[-/_](\d{1,2})"),
    re.compile(r"(20\d{2})(\d{2})(\d{2})"),
    re.compile(r"q([1-4])[\s\-_]*(20\d{2})", re.I),
    re.compile(r"fy[\s\-_]?(20\d{2})", re.I),
]


def _classify(text: str) -> tuple[str, str]:
    for pat, cat, label in CATEGORY_PATTERNS:
        if pat.search(text):
            return cat, label
    return "other", "Other IR Document"


def _guess_release_time(text: str, url: str) -> int:
    """Best-effort release date from anchor text or URL path. Returns UTC ms.
    Falls back to current time if nothing parseable found."""
    for source_str in (text or "", url or ""):
        for pat in _DATE_PATS:
            m = pat.search(source_str)
            if not m:
                continue
            groups = m.groups()
            try:
                if pat.pattern.startswith("q"):
                    q, year = int(groups[0]), int(groups[1])
                    month = q * 3
                    dt = datetime(year, month, 28, tzinfo=timezone.utc)
                elif pat.pattern.startswith("fy"):
                    year = int(groups[0])
                    dt = datetime(year, 12, 31, tzinfo=timezone.utc)
                else:
                    y, mo, d = int(groups[0]), int(groups[1]), int(groups[2])
                    if not (1 <= mo <= 12 and 1 <= d <= 31):
                        continue
                    dt = datetime(y, mo, d, tzinfo=timezone.utc)
                return int(dt.timestamp() * 1000)
            except ValueError:
                continue
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def extract_pdf_links(html: str, base_url: str) -> list[dict]:
    """Find every anchor whose href looks like a PDF candidate. Returns list
    of {url, text, category, category_name}."""
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        # Best-effort regex fallback if bs4 missing
        out: list[dict] = []
        for m in re.finditer(r'<a[^>]+href=["\']([^"\']+)["\'][^>]*>([^<]*)</a>', html, re.I):
            href, text = m.group(1), (m.group(2) or "").strip()
            if not href.lower().endswith(".pdf"):
                continue
            full = urljoin(base_url, href)
            cat, label = _classify(text + " " + href)
            out.append({"url": full, "text": text or href, "category": cat, "category_name": label})
        return _dedup(out)
    soup = BeautifulSoup(html, "html.parser")
    out: list[dict] = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        text = (a.get_text() or "").strip()
        if not href.lower().endswith(".pdf"):
            # Some IR pages link via /download?id=… or /press-release/<slug>
            # — only ingest direct .pdf to keep noise down at v1.
            continue
        full = urljoin(base_url, href)
        cat, label = _classify(text + " " + href)
        out.append({
            "url": full, "text": text or href.rsplit("/", 1)[-1],
            "category": cat, "category_name": label,
        })
    return _dedup(out)


def _dedup(items: list[dict]) -> list[dict]:
    seen: set[str] = set()
    out: list[dict] = []
    for it in items:
        if it["url"] in seen:
            continue
        seen.add(it["url"])
        out.append(it)
    return out


# ============================================================
# Download + Mongo
# ============================================================

def _doc_id_for(ticker: IrTicker, pdf_url: str) -> str:
    """Stable doc id: source + ticker + url-hash. Same URL re-visited gives
    same _id, so dedup is automatic."""
    h = hashlib.sha1(pdf_url.encode("utf-8")).hexdigest()[:16]
    return f"ir_pages_{ticker.canonical}_{h}"


def download_pdf(s: requests.Session, url: str, dest: Path) -> tuple[int, str]:
    if dest.exists() and dest.stat().st_size > 0:
        return dest.stat().st_size, ""
    try:
        dest.parent.mkdir(parents=True, exist_ok=True)
        r = s.get(url, timeout=300, stream=True, headers={
            "User-Agent": UA,
            "Accept": "application/pdf,*/*",
        })
        if r.status_code != 200:
            return 0, f"HTTP {r.status_code}"
        ctype = r.headers.get("content-type") or ""
        if "pdf" not in ctype.lower() and not url.lower().endswith(".pdf"):
            # Allow octet-stream + servers that mis-tag PDFs
            return 0, f"unexpected content-type: {ctype}"
        size = 0
        with dest.open("wb") as f:
            for chunk in r.iter_content(64 * 1024):
                if not chunk:
                    continue
                f.write(chunk)
                size += len(chunk)
        return size, ""
    except Exception as e:
        return 0, str(e)[:300]


def ingest_pdf(coll, ticker: IrTicker, ir_url: str, link: dict,
               http_s: requests.Session, *, source_label: str) -> str:
    pdf_url = link["url"]
    doc_id = _doc_id_for(ticker, pdf_url)
    existing = coll.find_one({"_id": doc_id}, {"pdf_local_path": 1})
    if existing and existing.get("pdf_local_path"):
        if Path(existing["pdf_local_path"]).exists():
            return "skipped"

    text = link["text"]
    release_ms = _guess_release_time(text, pdf_url)
    ym = datetime.fromtimestamp(release_ms / 1000, tz=timezone.utc).strftime("%Y-%m")
    fname = safe_filename(text or pdf_url.rsplit("/", 1)[-1], max_len=140)
    if not fname.lower().endswith(".pdf"):
        fname += ".pdf"
    dest = IR_PAGES_PDF_ROOT / ticker.listing_code / ym / fname
    size, err = download_pdf(http_s, pdf_url, dest)
    pdf_local = str(dest) if size > 0 else ""
    rel_path = ""
    if pdf_local:
        try:
            rel_path = str(Path(pdf_local).relative_to(IR_PAGES_PDF_ROOT))
        except ValueError:
            rel_path = pdf_local

    extra = {
        "ir_url":         ir_url,
        "pdf_url":        pdf_url,
        "anchor_text":    text,
        "source_label":   source_label,
    }

    doc = make_filing_doc(
        doc_id=doc_id,
        source=SOURCE,
        category=link["category"],
        category_name=link["category_name"],
        title=text or pdf_url.rsplit("/", 1)[-1],
        release_time_ms=release_ms,
        organization=ticker.name_en,
        ticker_local=ticker.listing_code,
        ticker_canonical=ticker.canonical,
        lang="en",
        doc_introduce=text,
        content_md="",
        pdf_rel_path=rel_path,
        pdf_local_path=pdf_local,
        pdf_size_bytes=size,
        pdf_download_error=err,
        pdf_unavailable=(size == 0),
        web_url=pdf_url,
        list_item={"link": link, "ir_url": ir_url, "source_label": source_label},
        extra=extra,
    )
    upsert_filing(coll, doc)
    time.sleep(THROTTLE_BASE_S)
    return "updated" if existing else "added"


# ============================================================
# Driver
# ============================================================

async def crawl_one_entry(http_s: requests.Session, entry: dict, *,
                          allow_browser: bool, limit: Optional[int]) -> dict[str, int]:
    ticker = resolve_ticker(entry)
    if not ticker:
        logger.warning("[%s] no ticker registry match for %s", entry.get("name"),
                       entry.get("stock_ticker"))
        return {"errors": 1}
    url = entry.get("url") or ""
    if not url:
        return {"errors": 1}
    coll = get_collection(SOURCE)
    counters = {"added": 0, "updated": 0, "skipped": 0, "errors": 0}

    needs_browser = bool(entry.get("requires_browser")) and allow_browser
    html = ""
    try:
        if needs_browser:
            html = await fetch_html_browser(
                url,
                timeout_ms=int(entry.get("browser_timeout_ms") or 30000),
                wait_until=entry.get("browser_wait_until") or "domcontentloaded",
                extra_wait_ms=int(entry.get("browser_extra_wait_ms") or 2000),
            )
        else:
            html = fetch_html_simple(http_s, url)
    except Exception as e:
        logger.error("[%s] fetch failed (browser=%s): %s",
                     ticker.canonical, needs_browser, e)
        counters["errors"] += 1
        save_state(SOURCE, bucket=ticker.canonical,
                   last_run_at=datetime.now(timezone.utc),
                   error=str(e)[:300])
        return counters

    links = extract_pdf_links(html, url)
    if limit:
        links = links[:limit]
    logger.info("[%s] %s → %d PDF candidates",
                ticker.canonical, entry.get("name"), len(links))

    for link in links:
        try:
            status = ingest_pdf(coll, ticker, url, link, http_s,
                                source_label=entry.get("name") or "")
            counters[status] = counters.get(status, 0) + 1
        except Exception as e:
            logger.error("[%s] ingest %s failed: %s",
                         ticker.canonical, link.get("url"), e)
            counters["errors"] += 1

    save_state(SOURCE, bucket=ticker.canonical,
               last_run_at=datetime.now(timezone.utc),
               links_seen=len(links))
    record_daily_stat(SOURCE, ticker.canonical,
                      added=counters["added"], skipped=counters["skipped"],
                      errors=counters["errors"], pdfs=counters["added"])
    return counters


async def crawl_all(*, ticker_filter: Optional[set[str]] = None,
                    limit: Optional[int] = None,
                    allow_browser: bool = True) -> dict[str, int]:
    ensure_indexes(SOURCE)
    sources = load_ir_sources()
    if ticker_filter:
        wanted = {t.upper() for t in ticker_filter}
        sources = [s for s in sources if str(s.get("stock_ticker") or "").upper() in wanted]

    http_s = _make_http_session()
    totals = {"added": 0, "updated": 0, "skipped": 0, "errors": 0}
    for entry in sources:
        c = await crawl_one_entry(http_s, entry, allow_browser=allow_browser, limit=limit)
        for k, v in c.items():
            totals[k] = totals.get(k, 0) + v
    # Cleanup playwright
    if _PLAYWRIGHT_BROWSER:
        p, browser = _PLAYWRIGHT_BROWSER
        try:
            await browser.close()
            await p.stop()
        except Exception:
            pass
    return totals


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--ticker", action="append",
                   help="Listing code (GLW / 06869). Repeatable.")
    p.add_argument("--max", type=int, default=None,
                   help="Max PDFs per IR page per run.")
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--no-browser", action="store_true",
                   help="Disable Playwright; only crawl pages without requires_browser.")
    p.add_argument("--show-state", action="store_true")
    p.add_argument("--watch", action="store_true")
    p.add_argument("--interval", type=int, default=7200)
    args = p.parse_args(sys.argv[1:] if len(sys.argv) > 1 else [])

    ensure_indexes(SOURCE)

    if args.show_state:
        coll = get_collection(SOURCE)
        for entry in load_ir_sources():
            t = resolve_ticker(entry)
            if not t:
                print(f"  ?? {entry.get('stock_ticker'):<10} {entry.get('name'):<40}  no-ticker-registry-match")
                continue
            n = coll.count_documents({"ticker_canonical": t.canonical})
            st = load_state(SOURCE, bucket=t.canonical)
            print(f"  {t.canonical:>10} {entry.get('name'):<48}  pdfs={n:>3}  last_run={st.get('last_run_at') or '—'}")
        return

    limit = args.limit or args.max

    while True:
        totals = asyncio.run(crawl_all(
            ticker_filter=set(args.ticker) if args.ticker else None,
            limit=limit,
            allow_browser=not args.no_browser,
        ))
        logger.info("ROUND DONE — %s", totals)
        if not args.watch:
            break
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
