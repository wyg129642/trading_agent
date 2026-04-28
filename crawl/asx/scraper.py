#!/usr/bin/env python3
"""ASX scraper — pulls Australian-listed announcements + 3-yr revenue panel for
the AU portfolio holdings into Mongo ``ir_filings.asx``.

Why
---
ASX (via Markit Digital JSON, the same vendor powering asx.com.au) is the
official venue for "continuous disclosure" obligations under Listing Rule 3.1.
For mining explorers like SGQ the Quarterly Activities Report (Appendix 5B)
is the primary signal — drill results, exploration spend, cashflow runway —
and it's PDF-only. Half-year / annual reports carry segment + revenue.

Endpoints (verified live 2026-04-28)
-------------------------------------
  GET https://asx.api.markitdigital.com/asx-research/1.0/companies/{TICKER}/announcements
       (last 5 only — hard cap, no pagination)
  GET https://asx.api.markitdigital.com/asx-research/1.0/companies/{TICKER}/key-statistics
       (3-yr revenue/netIncome panel)
  GET https://www.asx.com.au/asx/v2/statistics/displayAnnouncement.do?display=pdf&idsId={ID}
       (HTML interstitial — the real PDF URL is in <input name="pdfURL">)
  GET https://announcements.asx.com.au/asxpdf/{YYYYMMDD}/pdf/{server-id}.pdf
       (extracted from interstitial; ``id`` is unguessable, must be scraped per-doc)

Critical gotchas (from research):
  - The Markit ``url`` field is **always empty**. Always go through interstitial.
  - PDF date in path is **upload date**, not announcement date. Use JSON ``date`` for ``release_time``.
  - Tarpit-style throttling on Markit (latency, not 429s) — burst > 5 requests
    triggers 30-70s slow-walks. Throttle base 4s + jitter 2s, no bursts.
  - No pagination; the 5-item cap is a hard limit. Build history by polling.

CLI
---
  python3 scraper.py --show-state
  python3 scraper.py --ticker SGQ --max 5
  python3 scraper.py --watch --interval 1800     # 30min poll loop
  python3 scraper.py --refresh-key-stats         # only refresh key-statistics panel

Doc shape (filings collection):
  See crawl/ir_filings/common.py::make_filing_doc. Source-specific extras:
    document_key, ids_id, announcement_type, is_price_sensitive,
    file_size_kb, pdf_url_resolved, headline_only
"""
from __future__ import annotations

import argparse
import json
import os
import random
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import requests

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
from crawl.ir_filings.tickers import AU_TICKERS, IrTicker  # noqa: E402

SOURCE = "asx"
ASX_PDF_ROOT = pdf_dir(SOURCE)
COLL_FILINGS = "asx"        # via fallthrough in COLLECTION_FOR_SOURCE
COLL_KEY_STATS = "asx_key_statistics"

# Markit Digital + ASX endpoints (verified live 2026-04-28)
API_ROOT = "https://asx.api.markitdigital.com/asx-research/1.0"
ANN_ROOT = "https://www.asx.com.au/asx/v2/statistics"
PDF_RE = re.compile(r'name="pdfURL"\s+value="([^"]+)"')

# Imperva tarpits sequential bursts. Default to 4-6s spacing.
THROTTLE_BASE_S = float(os.environ.get("ASX_THROTTLE", "4.0"))
THROTTLE_JITTER_S = 2.0

# Browser-class UA — Markit accepts python-requests/* but the announcements
# host (Imperva-fronted) prefers a real browser sig.
UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

# Useful announcement types (per ASX taxonomy enum). 'SECURITY HOLDER DETAILS'
# is director-interest noise; skip per spec. 'ISSUED CAPITAL' is mostly
# cleansing-notice spam — keep but down-rank in UI.
WANTED_TYPES = {
    "QUARTERLY ACTIVITIES REPORT",     # ★★★★★ for explorers (SGQ)
    "PERIODIC REPORTS",                # half-year / annual / Appendix 4D / 4E
    "DISTRIBUTION ANNOUNCEMENT",       # dividends + buyback updates
    "OTHER",                            # presentations, webinars, trading updates
    "ISSUED CAPITAL",                   # capital raises (some have segment commentary)
}

# Re-register collection names in common (non-default) — common.py only knows
# about the original 5; ASX is the 6th. Patch at import time.
from crawl.ir_filings import common as _common  # noqa: E402
_common.COLLECTION_FOR_SOURCE.setdefault(SOURCE, SOURCE)

logger = setup_logging(SOURCE)

# ============================================================
# HTTP
# ============================================================

def make_session() -> requests.Session:
    s = requests.Session()
    s.trust_env = False              # bypass Clash on 7890
    s.proxies = {}
    s.headers.update({
        "User-Agent": UA,
        "Accept": "application/json,text/html,*/*",
        "Accept-Language": "en-US,en;q=0.9",
    })
    return s


def _sleep(): time.sleep(THROTTLE_BASE_S + random.uniform(0, THROTTLE_JITTER_S))


def http_get(s: requests.Session, url: str, *,
             accept: str = "application/json",
             params: Optional[dict] = None,
             referer: Optional[str] = None,
             timeout: int = 120,
             retries: int = 2) -> requests.Response:
    headers = {"Accept": accept}
    if referer:
        headers["Referer"] = referer
    last_err: Optional[Exception] = None
    for attempt in range(retries + 1):
        try:
            r = s.get(url, params=params, headers=headers, timeout=timeout)
            _sleep()
            if r.status_code == 200:
                return r
            if r.status_code in (429, 503):
                wait = (attempt + 1) * 30
                logger.warning("ASX %s on %s — backing off %ss", r.status_code, url, wait)
                time.sleep(wait)
                continue
            r.raise_for_status()
        except requests.RequestException as e:
            last_err = e
            time.sleep(2 ** attempt * 5)
    raise RuntimeError(f"ASX GET failed: {url} ({last_err})")


# ============================================================
# Announcements + key statistics
# ============================================================

def list_announcements(s: requests.Session, ticker: str) -> list[dict]:
    """Markit JSON returns the most recent 5 announcements per ticker, no
    pagination, no historical. Build history by polling at a cadence shorter
    than the issuer's emission rate."""
    r = http_get(
        s, f"{API_ROOT}/companies/{ticker.upper()}/announcements",
        referer=f"https://www.asx.com.au/markets/company/{ticker.lower()}",
    )
    payload = r.json() or {}
    return ((payload.get("data") or {}).get("items") or [])


def fetch_key_statistics(s: requests.Session, ticker: str) -> Optional[dict]:
    """3-yr income statement panel + ratios. Daily refresh is enough."""
    try:
        r = http_get(
            s, f"{API_ROOT}/companies/{ticker.upper()}/key-statistics",
            referer=f"https://www.asx.com.au/markets/company/{ticker.lower()}",
        )
        return r.json().get("data")
    except Exception as e:
        logger.warning("[%s] key-statistics fetch failed: %s", ticker, e)
        return None


def resolve_pdf_url(s: requests.Session, document_key: str) -> Optional[str]:
    """document_key e.g. '2924-03082939-6A1322267'. Middle segment is idsId.
    Returns the GCS-backed PDF URL (extracted from ToS interstitial) or None."""
    try:
        ids_id = document_key.split("-")[1]
    except IndexError:
        return None
    r = http_get(
        s, f"{ANN_ROOT}/displayAnnouncement.do",
        params={"display": "pdf", "idsId": ids_id},
        accept="text/html",
        referer=f"{ANN_ROOT}/todayAnns.do",
    )
    m = PDF_RE.search(r.text)
    return m.group(1) if m else None


def download_pdf(s: requests.Session, pdf_url: str, dest: Path) -> tuple[int, str]:
    """Returns (size_bytes, error_str)."""
    try:
        if dest.exists() and dest.stat().st_size > 0:
            return dest.stat().st_size, ""
        dest.parent.mkdir(parents=True, exist_ok=True)
        r = http_get(s, pdf_url, accept="application/pdf,*/*",
                     referer=f"{ANN_ROOT}/displayAnnouncement.do",
                     timeout=300)
        ctype = r.headers.get("content-type", "")
        if not ctype.startswith("application/pdf"):
            return 0, f"unexpected content-type: {ctype}"
        dest.write_bytes(r.content)
        return len(r.content), ""
    except Exception as e:
        return 0, str(e)[:300]


# ============================================================
# Filing → Mongo
# ============================================================

_FILE_SIZE_RE = re.compile(r"(\d+)\s*KB", re.I)


def _parse_iso(s: str) -> int:
    """Markit dates are `2026-04-27T22:54:23.000Z` — UTC."""
    try:
        dt = datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        try:
            dt = datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            return 0
    return int(dt.timestamp() * 1000)


def ingest_one(s: requests.Session, coll, ticker: IrTicker, item: dict, *,
               download_pdf_flag: bool = True) -> str:
    document_key = item.get("documentKey") or ""
    if not document_key:
        return "skipped"
    doc_id = f"asx_{ticker.listing_code}_{document_key}"

    existing = coll.find_one({"_id": doc_id}, {"pdf_local_path": 1, "headline_only": 1})
    if existing and existing.get("pdf_local_path"):
        if Path(existing["pdf_local_path"]).exists():
            return "skipped"

    headline = (item.get("headline") or "").strip()
    ann_type = item.get("announcementType") or ""
    release_ms = _parse_iso(item.get("date") or "")
    file_size_kb = 0
    fs_match = _FILE_SIZE_RE.search(item.get("fileSize") or "")
    if fs_match:
        file_size_kb = int(fs_match.group(1))

    # Resolve PDF — every item's `url` is empty; must hit interstitial
    pdf_local = ""
    pdf_size = 0
    pdf_err = ""
    pdf_unavailable = False
    pdf_url_resolved = ""
    headline_only = False

    if download_pdf_flag:
        try:
            pdf_url_resolved = resolve_pdf_url(s, document_key) or ""
        except Exception as e:
            pdf_err = f"interstitial: {e}"

        if pdf_url_resolved:
            ymd = (item.get("date") or "")[:10].replace("-", "") or "unknown"
            ym = ymd[:6]               # YYYYMM bucket for filesystem
            ym_dir = f"{ym[:4]}-{ym[4:]}"
            fname = safe_filename(f"{document_key}_{headline}", max_len=140) + ".pdf"
            dest = ASX_PDF_ROOT / ticker.listing_code / ym_dir / fname
            sz, err = download_pdf(s, pdf_url_resolved, dest)
            if sz > 0:
                pdf_local = str(dest)
                pdf_size = sz
            else:
                pdf_err = err or pdf_err
                pdf_unavailable = True
        else:
            # Headline-only filing (no PDF; ASX trading-halt notices etc.)
            headline_only = True

    rel_path = ""
    if pdf_local:
        try:
            rel_path = str(Path(pdf_local).relative_to(ASX_PDF_ROOT))
        except ValueError:
            rel_path = pdf_local

    # For headline-only filings, surface the headline as content_md so kb_search
    # has SOMETHING to score against (continuous-disclosure rule means even
    # text-only notices can be material).
    content_md = headline if headline_only else ""

    web_url = (f"{ANN_ROOT}/displayAnnouncement.do"
               f"?display=pdf&idsId={document_key.split('-')[1] if '-' in document_key else ''}")

    extra = {
        "document_key":         document_key,
        "ids_id":               document_key.split("-")[1] if "-" in document_key else "",
        "announcement_type":    ann_type,
        "is_price_sensitive":   bool(item.get("isPriceSensitive")),
        "file_size_kb":         file_size_kb,
        "pdf_url_resolved":     pdf_url_resolved,
        "headline_only":        headline_only,
        "issuer_name":          ticker.name_en,
    }

    doc = make_filing_doc(
        doc_id=doc_id,
        source=SOURCE,
        category=ann_type,
        category_name=ann_type.title(),
        title=headline,
        title_local=headline,
        release_time_ms=release_ms,
        organization=ticker.name_en,
        ticker_local=ticker.listing_code,
        ticker_canonical=ticker.canonical,
        lang="en",
        doc_introduce=headline,
        content_md=content_md,
        pdf_rel_path=rel_path,
        pdf_local_path=pdf_local,
        pdf_size_bytes=pdf_size,
        pdf_download_error=pdf_err,
        pdf_unavailable=pdf_unavailable,
        web_url=web_url,
        list_item=item,
        extra=extra,
    )
    upsert_filing(coll, doc)
    return "updated" if existing else "added"


def upsert_key_stats(coll, ticker: IrTicker, payload: dict) -> None:
    """One doc per ticker, refreshed each run."""
    coll.replace_one(
        {"_id": ticker.canonical},
        {
            "_id":               ticker.canonical,
            "ticker_canonical":  ticker.canonical,
            "ticker_local":      ticker.listing_code,
            "organization":      ticker.name_en,
            "data":              payload,
            "_canonical_tickers": [ticker.canonical],
            "ingested_at":       datetime.now(timezone.utc),
        },
        upsert=True,
    )


# ============================================================
# Driver
# ============================================================

def crawl_one_ticker(s: requests.Session, ticker: IrTicker, *,
                     limit: Optional[int] = None,
                     download_pdf: bool = True,
                     skip_key_stats: bool = False,
                     all_types: bool = False) -> dict[str, int]:
    coll = get_collection(SOURCE)
    counters = {"added": 0, "updated": 0, "skipped": 0, "errors": 0}

    try:
        items = list_announcements(s, ticker.listing_code)
    except Exception as e:
        logger.error("[%s] announcements fetch failed: %s", ticker.canonical, e)
        counters["errors"] += 1
        return counters

    n = 0
    for item in items:
        if limit and n >= limit:
            break
        if not all_types and item.get("announcementType") not in WANTED_TYPES:
            counters["skipped"] += 1
            n += 1
            continue
        try:
            status = ingest_one(s, coll, ticker, item, download_pdf_flag=download_pdf)
            counters[status] = counters.get(status, 0) + 1
        except Exception as e:
            logger.error("[%s] ingest %s failed: %s",
                         ticker.canonical, item.get("documentKey"), e)
            counters["errors"] += 1
        n += 1

    if not skip_key_stats:
        ks = fetch_key_statistics(s, ticker.listing_code)
        if ks:
            try:
                upsert_key_stats(get_db()[COLL_KEY_STATS], ticker, ks)
                logger.info("[%s] key_statistics refreshed", ticker.canonical)
            except Exception as e:
                logger.warning("[%s] key_statistics persist failed: %s",
                               ticker.canonical, e)

    save_state(SOURCE, bucket=ticker.canonical,
               last_run_at=datetime.now(timezone.utc),
               filings_seen=n)
    record_daily_stat(SOURCE, ticker.canonical,
                      added=counters["added"], skipped=counters["skipped"],
                      errors=counters["errors"], pdfs=counters["added"])
    return counters


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--ticker", action="append")
    p.add_argument("--max", type=int, default=None,
                   help="Max items per ticker per run (Markit caps at 5).")
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--no-pdf", action="store_true")
    p.add_argument("--all-types", action="store_true",
                   help="Don't filter by WANTED_TYPES.")
    p.add_argument("--skip-key-stats", action="store_true")
    p.add_argument("--refresh-key-stats", action="store_true",
                   help="Only refresh key_statistics panel; skip announcements.")
    p.add_argument("--show-state", action="store_true")
    p.add_argument("--watch", action="store_true")
    p.add_argument("--interval", type=int, default=1800,
                   help="Watch interval — 30 min default. Markit caps history at 5, "
                        "so polling more often than the issuer emits doesn't help.")
    args = p.parse_args(sys.argv[1:] if len(sys.argv) > 1 else [])

    ensure_indexes(SOURCE)
    tickers = AU_TICKERS
    if args.ticker:
        wanted = {t.upper() for t in args.ticker}
        tickers = [t for t in AU_TICKERS
                   if t.canonical.upper() in wanted or t.listing_code.upper() in wanted]

    if args.show_state:
        coll = get_collection(SOURCE)
        ks_coll = get_db()[COLL_KEY_STATS]
        for t in tickers:
            st = load_state(SOURCE, bucket=t.canonical)
            n = coll.count_documents({"ticker_canonical": t.canonical})
            ks = ks_coll.count_documents({"ticker_canonical": t.canonical})
            print(f"  {t.canonical:>10} {t.listing_code} {t.name_en:<35} "
                  f"filings={n:>3}  key_stats={ks:>1}  "
                  f"last_run={st.get('last_run_at') or '—'}")
        return

    sess = make_session()
    limit = args.limit or args.max

    while True:
        round_start = datetime.now(timezone.utc)
        totals = {"added": 0, "updated": 0, "skipped": 0, "errors": 0}
        for t in tickers:
            if args.refresh_key_stats:
                ks = fetch_key_statistics(sess, t.listing_code)
                if ks:
                    upsert_key_stats(get_db()[COLL_KEY_STATS], t, ks)
                    logger.info("[%s] key_statistics-only refreshed", t.canonical)
                continue
            c = crawl_one_ticker(sess, t, limit=limit,
                                 download_pdf=not args.no_pdf,
                                 skip_key_stats=args.skip_key_stats,
                                 all_types=args.all_types)
            for k, v in c.items():
                totals[k] = totals.get(k, 0) + v
            logger.info("[%s] %s", t.canonical, c)
        elapsed = (datetime.now(timezone.utc) - round_start).total_seconds()
        logger.info("ROUND DONE in %.1fs — %s", elapsed, totals)

        if not args.watch:
            break
        sleep_s = max(60, args.interval - int(elapsed))
        logger.info("Watch loop: sleeping %ss", sleep_s)
        time.sleep(sleep_s)


if __name__ == "__main__":
    main()
