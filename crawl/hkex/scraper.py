#!/usr/bin/env python3
"""HKEXnews scraper — pulls disclosure announcements + financial reports for the
14 in-scope HK-listed holdings into Mongo ``ir_filings.hkex``.

Why
---
HKEX has no public RSS / API but the JS-driven titleSearchServlet behind
``hkexnews.hk/search`` is stable and serves bilingual full filings (PDFs).
Annual / interim reports contain the HKFRS 8 segment-revenue Note (typically
Note 5 or 6) which is the killer source for HK revenue modeling — there is no
HK-side XBRL.

Endpoints (verified live 2026-04-28)
-------------------------------------
  GET https://www1.hkexnews.hk/search/titlesearch.xhtml?lang=en      # cookie seed
  GET https://www1.hkexnews.hk/search/titleSearchServlet.do?<params> # JSON workhorse
  GET https://www1.hkexnews.hk/ncms/script/eds/activestock_sehk_e.json
       (and `_c` for TC)                                              # ticker → internal stockId
  GET https://www1.hkexnews.hk/ncms/script/eds/{tierone,tiertwo,tiertwogrp}_e.json
       (and `_c`)                                                     # category taxonomy
  GET https://www1.hkexnews.hk/listedco/listconews/sehk/YYYY/MMDD/{ID}.pdf

Crawler shape
-------------
For each of the 14 tickers, daily-window queries (`fromDate`/`toDate` as today
or last N days) over both languages (`E` + `ZH`). Dedupe across languages on
NEWS_ID. Filters apply tier-2 codes for the revenue-modeling subset (see
DOC_TYPE_FILTER below) — easily widened to all-categories with `--all-categories`.

Throttle: 2.5-4.5s base, 30s burst cooldown. HKEX/Akamai bans on IP within
~2 min of 5 req/s — see CLAUDE.md memory.

CLI
---
  python3 scraper.py --show-state
  python3 scraper.py --ticker 01347 --days 365
  python3 scraper.py --max 50                # all tickers, ≤50 filings each
  python3 scraper.py --watch --interval 1800
  python3 scraper.py --refresh-taxonomy      # force re-pull of activestock + tierone/tiertwo

Doc shape (filings collection):
  See ``crawl/ir_filings/common.py::make_filing_doc``. Source-specific extras:
    news_id, stock_code, t1code, t2code, file_link_en, file_link_zh,
    file_size_str, file_type, languages_seen[]
"""
from __future__ import annotations

import argparse
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
from crawl.ir_filings.tickers import HK_TICKERS, IrTicker  # noqa: E402

# ============================================================
# Constants
# ============================================================

SOURCE = "hkex"
COLL_FILINGS = SOURCE
HKEX_PDF_ROOT = pdf_dir(SOURCE)

BASE = "https://www1.hkexnews.hk"
SEARCH_HTML = f"{BASE}/search/titlesearch.xhtml?lang=en"
SEARCH_JSON = f"{BASE}/search/titleSearchServlet.do"

# Cached taxonomy / stock index (refreshed weekly via --refresh-taxonomy)
TAXONOMY_DIR = _HERE / "cache"
TAXONOMY_DIR.mkdir(parents=True, exist_ok=True)
STOCK_INDEX_PATH = TAXONOMY_DIR / "activestock_sehk_e.json"
TIERONE_PATH     = TAXONOMY_DIR / "tierone_e.json"
TIERTWO_PATH     = TAXONOMY_DIR / "tiertwo_e.json"
INDEX_TTL_DAYS   = 7

# Browser-class UA (HKEX rejects python-requests UAs; verified by research agent)
UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
      "Chrome/120.0 Safari/537.36")

# Per-request throttle (seconds). Conservative default; HKEX/Akamai bans at ~5 req/s.
THROTTLE_BASE_S = float(os.environ.get("HKEX_THROTTLE", "2.8"))
THROTTLE_JITTER_S = 1.5
COOLDOWN_EVERY = 20                       # cooldown after every N requests
COOLDOWN_S = 30
HARD_STOP_ON_503 = 600                    # 10min hard stop on 503

# Tier-2 codes deemed useful for revenue modeling (see CLAUDE.md memory):
#   13300 Final Results, 13400 Interim Results, 13600 Quarterly Results,
#   13500 Profit Warning, 19760 Business Update, 19800 Trading Update,
#   19750 Inside Information, 40100 Annual Report, 40200 Interim Report
# Tier-1 codes (for filtering): 10000 Announcements, 40000 Financial Statements
DOC_TYPE_FILTER = {
    13300: "Final Results",
    13400: "Interim Results",
    13600: "Quarterly Results",
    13500: "Profit Warning",
    19760: "Business Update",
    19800: "Trading Update",
    19750: "Inside Information",
    40100: "Annual Report",
    40200: "Interim Report",
}

logger = setup_logging(SOURCE)

# ============================================================
# HTTP session
# ============================================================

def make_session() -> requests.Session:
    s = requests.Session()
    # HKEX is China-friendly + fast direct; routing through Clash adds latency
    # and triggers Akamai variance. Direct is fine.
    s.trust_env = False
    s.proxies = {}
    s.headers.update({
        "User-Agent": UA,
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9,zh-HK;q=0.8",
    })
    # Seed JSESSIONID + TS* cookies from the search page; required by titleSearchServlet
    r = s.get(SEARCH_HTML, timeout=15)
    r.raise_for_status()
    return s


_REQ_COUNT = 0


def _throttle(burst: bool = False) -> None:
    """Sleep base + jitter; cooldown every COOLDOWN_EVERY requests."""
    global _REQ_COUNT
    import random
    sleep = THROTTLE_BASE_S + random.uniform(0, THROTTLE_JITTER_S)
    time.sleep(sleep)
    _REQ_COUNT += 1
    if _REQ_COUNT % COOLDOWN_EVERY == 0:
        logger.info("HKEX cooldown after %d reqs (%ds)", _REQ_COUNT, COOLDOWN_S)
        time.sleep(COOLDOWN_S)


def http_get(session: requests.Session, url: str, *,
             params: Optional[dict] = None,
             accept: str = "application/json",
             stream: bool = False) -> requests.Response:
    headers = {"Accept": accept,
               "Referer": SEARCH_HTML,
               "X-Requested-With": "XMLHttpRequest" if "Servlet" in url else ""}
    headers = {k: v for k, v in headers.items() if v}
    r = session.get(url, params=params, headers=headers, timeout=60, stream=stream)
    _throttle()
    if r.status_code == 503:
        logger.warning("HKEX 503 — Akamai blocking. Hard stop %ds", HARD_STOP_ON_503)
        time.sleep(HARD_STOP_ON_503)
        # one retry only — if still 503, fail loudly
        r = session.get(url, params=params, headers=headers, timeout=60, stream=stream)
        _throttle()
    r.raise_for_status()
    return r


# ============================================================
# Taxonomy + stock index (cached weekly)
# ============================================================

def _load_or_refresh(session: requests.Session, path: Path, url: str) -> Any:
    if path.exists():
        age_days = (time.time() - path.stat().st_mtime) / 86400
        if age_days < INDEX_TTL_DAYS:
            return json.loads(path.read_text(encoding="utf-8"))
    logger.info("Refreshing HKEX taxonomy: %s", path.name)
    r = http_get(session, url)
    data = r.json()
    path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    return data


def load_stock_index(session: requests.Session, force: bool = False) -> dict[str, int]:
    """Returns {code5digit: internal stockId i}. The internal `i` is what
    titleSearchServlet expects as `stockId` (NOT the listing code — see
    CLAUDE.md memory)."""
    if force and STOCK_INDEX_PATH.exists():
        STOCK_INDEX_PATH.unlink()
    data = _load_or_refresh(session, STOCK_INDEX_PATH,
                             f"{BASE}/ncms/script/eds/activestock_sehk_e.json")
    return {row["c"]: row["i"] for row in data}


def load_tier_taxonomy(session: requests.Session) -> tuple[dict, dict]:
    t1 = _load_or_refresh(session, TIERONE_PATH,
                          f"{BASE}/ncms/script/eds/tierone_e.json")
    t2 = _load_or_refresh(session, TIERTWO_PATH,
                          f"{BASE}/ncms/script/eds/tiertwo_e.json")
    t1_map = {int(row["code"]): row["name"] for row in t1 if row.get("code")}
    t2_map = {int(row["code"]): row["name"] for row in t2 if row.get("code")}
    return t1_map, t2_map


# ============================================================
# Search + parse
# ============================================================

def search(session: requests.Session, stock_id: int, *,
           lang: str = "E",
           from_date: str,
           to_date: str,
           t1code: int = -2,
           t2code: int = -2,
           row_range: int = 100) -> dict:
    """Single page fetch. Returns dict with rows[], total, has_next."""
    params = {
        "sortDir": 0, "sortByOptions": "DateTime",
        "category": 0, "market": "SEHK",
        "stockId": stock_id, "documentType": -1,
        "fromDate": from_date, "toDate": to_date,
        "title": "",
        "searchType": 1,                 # post-2006 (modern taxonomy)
        "t1code": t1code, "t2Gcode": -2, "t2code": t2code,
        "rowRange": row_range, "lang": lang,
    }
    r = http_get(session, SEARCH_JSON, params=params)
    payload = r.json()
    raw_result = payload.get("result") or "[]"
    rows = json.loads(raw_result) if isinstance(raw_result, str) else raw_result
    return {
        "rows":     rows or [],
        "total":    int(payload.get("recordCnt") or 0),
        "loaded":   int(payload.get("loadedRecord") or 0),
        "has_next": bool(payload.get("hasNextRow")),
    }


def search_paginated(session: requests.Session, stock_id: int, *,
                     lang: str, from_date: str, to_date: str,
                     t1code: int = -2, t2code: int = -2,
                     max_rows: int = 1000) -> list[dict]:
    """Walk pages by growing `row_range` until has_next=False or max_rows hit."""
    page = 100
    last_rows: list[dict] = []
    while page <= max_rows:
        out = search(session, stock_id, lang=lang,
                     from_date=from_date, to_date=to_date,
                     t1code=t1code, t2code=t2code, row_range=page)
        last_rows = out["rows"]
        if not out["has_next"] or len(last_rows) >= out["total"]:
            break
        page += 100
    return last_rows


# ============================================================
# PDF download
# ============================================================

_HTML_PLACEHOLDER_RE = re.compile(
    r"published\s+by\s+the\s+issuer\s+in\s+the\s+Chinese\s+section",
    re.IGNORECASE,
)


def _decode(text: str) -> str:
    """HKEX titles ship HTML entities (`&#x2f;` `&amp;` etc.) — unescape so
    the stored title is readable."""
    return html.unescape(text or "").strip()


def download_announcement(session: requests.Session, *,
                          stock_code: str, news_id: str,
                          file_link: str, lang: str) -> tuple[Path, int, str, bool]:
    """Download announcement (PDF or HTM placeholder). Returns (path, size,
    error, is_placeholder)."""
    url = BASE + file_link if file_link.startswith("/") else file_link
    # Storage: <root>/<stock_code>/<YYYY-MM>/<news_id>_<lang>.<ext>
    # Pull YYYY/MM out of file_link (.../sehk/YYYY/MMDD/...).
    m = re.search(r"/sehk/(\d{4})/(\d{2})(\d{2})/", file_link)
    if m:
        ym = f"{m.group(1)}-{m.group(2)}"
    else:
        ym = "unknown"
    ext = ".pdf" if file_link.lower().endswith(".pdf") else ".htm"
    suffix = "_zh" if lang == "ZH" else "_en"
    out_dir = HKEX_PDF_ROOT / stock_code / ym
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{news_id}{suffix}{ext}"
    if out_path.exists() and out_path.stat().st_size > 0:
        # Check placeholder status from cached file
        is_ph = ext == ".htm" and out_path.stat().st_size < 4096
        return out_path, out_path.stat().st_size, "", is_ph
    try:
        r = http_get(session, url, accept="*/*", stream=True)
        body = r.content
        out_path.write_bytes(body)
        is_ph = (ext == ".htm" and len(body) < 4096
                 and bool(_HTML_PLACEHOLDER_RE.search(body.decode("utf-8", errors="ignore"))))
        return out_path, len(body), "", is_ph
    except Exception as e:
        return out_path, 0, str(e)[:500], False


# ============================================================
# Filing → Mongo
# ============================================================

def _parse_dt(raw: str) -> int:
    """HKEX `DATE_TIME` is `DD/MM/YYYY HH:MM` HK time (UTC+8). Return UTC ms."""
    try:
        dt_local = datetime.strptime(raw.strip(), "%d/%m/%Y %H:%M")
        # HK is UTC+8, no DST
        from datetime import timezone as tz, timedelta as td
        dt_utc = dt_local.replace(tzinfo=tz(td(hours=8))).astimezone(tz.utc)
        return int(dt_utc.timestamp() * 1000)
    except Exception:
        return 0


def ingest_announcement(session: requests.Session, coll, ticker: IrTicker,
                        row_en: Optional[dict], row_zh: Optional[dict],
                        t1_map: dict, t2_map: dict, *,
                        download_pdf: bool = True) -> str:
    """One NEWS_ID = one Mongo doc, with up to two PDF paths (en + zh).

    Either side may be missing (HK issuers can file CN-only with EN placeholder
    HTM). When EN body is the placeholder stub, we prefer ZH PDF as the
    canonical body."""
    # Pick whichever side is present as the primary metadata source
    primary = row_en or row_zh
    secondary = row_zh if primary is row_en else row_en
    if not primary:
        return "skipped"

    news_id = str(primary.get("NEWS_ID") or "")
    if not news_id:
        return "skipped"

    title_en = _decode((row_en or {}).get("TITLE", ""))
    title_zh = _decode((row_zh or {}).get("TITLE", ""))
    long_text_en = _decode((row_en or {}).get("LONG_TEXT", ""))
    long_text_zh = _decode((row_zh or {}).get("LONG_TEXT", ""))

    release_ms = _parse_dt(primary.get("DATE_TIME") or "")
    stock_code = str(primary.get("STOCK_CODE") or ticker.listing_code)
    stock_name = _decode(primary.get("STOCK_NAME") or ticker.name_en)

    # Categorize by tier-2 (preferred) — fallback to tier-1
    # Note: titleSearchServlet doesn't return raw t1/t2 codes; they come from
    # parsing LONG_TEXT against our taxonomy. The format is:
    #   "Announcements and Notices - [Quarterly Results]"
    #   "Financial Statements/ESG Information - [Annual Report]"
    t1_label, t2_label = _split_long_text(long_text_en or long_text_zh)
    t1code = _label_to_code(t1_label, t1_map)
    t2code = _label_to_code(t2_label, t2_map)
    category = f"t1={t1code}|t2={t2code}"
    category_name = t2_label or t1_label or "Other"

    # Download PDFs (both languages if both rows present)
    en_path = ""
    en_size = 0
    en_err = ""
    zh_path = ""
    zh_size = 0
    zh_err = ""
    placeholder_en = False
    if download_pdf and row_en and row_en.get("FILE_LINK"):
        path, sz, err, is_ph = download_announcement(
            session, stock_code=stock_code, news_id=news_id,
            file_link=row_en["FILE_LINK"], lang="E",
        )
        if sz > 0:
            en_path = str(path)
            en_size = sz
            placeholder_en = is_ph
        en_err = err
    if download_pdf and row_zh and row_zh.get("FILE_LINK"):
        path, sz, err, _ = download_announcement(
            session, stock_code=stock_code, news_id=news_id,
            file_link=row_zh["FILE_LINK"], lang="ZH",
        )
        if sz > 0:
            zh_path = str(path)
            zh_size = sz
        zh_err = err

    # Pick canonical PDF: ZH wins if EN side is the "published in Chinese
    # section" placeholder; otherwise prefer EN. This is what extract_pdf_texts
    # will read.
    if placeholder_en and zh_path:
        primary_path = zh_path
        primary_size = zh_size
        primary_lang = "zh-TW"
    elif en_path:
        primary_path = en_path
        primary_size = en_size
        primary_lang = "en"
    elif zh_path:
        primary_path = zh_path
        primary_size = zh_size
        primary_lang = "zh-TW"
    else:
        primary_path = ""
        primary_size = 0
        primary_lang = "en"

    rel_path = ""
    if primary_path:
        try:
            rel_path = str(Path(primary_path).relative_to(HKEX_PDF_ROOT))
        except ValueError:
            rel_path = primary_path

    web_url = (f"{BASE}/search/titlesearch.xhtml?lang=en"
               f"#stockcode={stock_code}&newsId={news_id}")

    list_item = {
        "en": row_en, "zh": row_zh,
        "placeholder_en": placeholder_en,
    }

    extra = {
        "news_id":        news_id,
        "stock_code":     stock_code,
        "stock_name":     stock_name,
        "t1code":         t1code,
        "t2code":         t2code,
        "t1_label":       t1_label,
        "t2_label":       t2_label,
        "title_en":       title_en,
        "long_text_en":   long_text_en,
        "long_text_zh":   long_text_zh,
        "file_link_en":   (row_en or {}).get("FILE_LINK", ""),
        "file_link_zh":   (row_zh or {}).get("FILE_LINK", ""),
        "file_size_str_en": (row_en or {}).get("FILE_INFO", ""),
        "file_size_str_zh": (row_zh or {}).get("FILE_INFO", ""),
        "file_type":      primary.get("FILE_TYPE") or "PDF",
        "languages_seen": [lang for lang, row in [("en", row_en), ("zh", row_zh)] if row],
        "pdf_path_en":    en_path,
        "pdf_path_zh":    zh_path,
        "pdf_size_en":    en_size,
        "pdf_size_zh":    zh_size,
        "pdf_error_en":   en_err,
        "pdf_error_zh":   zh_err,
        "placeholder_en": placeholder_en,
    }

    doc = make_filing_doc(
        doc_id=news_id,
        source=SOURCE,
        category=str(t2code) if t2code else str(t1code),
        category_name=category_name,
        title=title_en or title_zh,
        title_local=title_zh,
        release_time_ms=release_ms,
        organization=stock_name,
        ticker_local=stock_code,
        ticker_canonical=ticker.canonical,
        lang=primary_lang,
        doc_introduce=long_text_en or long_text_zh,
        content_md="",
        pdf_rel_path=rel_path,
        pdf_local_path=primary_path,
        pdf_size_bytes=primary_size,
        pdf_download_error=(en_err or zh_err),
        pdf_unavailable=(primary_size == 0),
        web_url=web_url,
        list_item=list_item,
        extra=extra,
    )

    existing = coll.find_one({"_id": news_id}, {"pdf_local_path": 1})
    upsert_filing(coll, doc)
    return "updated" if existing else "added"


_LONG_TEXT_RE = re.compile(r"^(.*?)\s*-\s*\[(.+?)\]\s*$")


def _split_long_text(s: str) -> tuple[str, str]:
    """`Announcements and Notices - [Quarterly Results]` → (`Announcements and
    Notices`, `Quarterly Results`)."""
    if not s:
        return "", ""
    m = _LONG_TEXT_RE.match(s)
    if not m:
        return s, ""
    return m.group(1).strip(), m.group(2).strip()


def _label_to_code(label: str, code_map: dict) -> int:
    if not label:
        return 0
    for code, name in code_map.items():
        if name.strip().lower() == label.strip().lower():
            return code
    return 0


# ============================================================
# Driver
# ============================================================

def crawl_one_ticker(session: requests.Session, ticker: IrTicker, *,
                     stock_id_map: dict[str, int],
                     t1_map: dict, t2_map: dict,
                     days: int = 365,
                     limit: Optional[int] = None,
                     download_pdf: bool = True,
                     all_categories: bool = False) -> dict[str, int]:
    coll = get_collection(SOURCE)
    counters = {"added": 0, "updated": 0, "skipped": 0, "errors": 0}

    sid = stock_id_map.get(ticker.listing_code)
    if not sid:
        logger.error("[%s] no stockId in activestock_sehk index — refresh-taxonomy?",
                     ticker.canonical)
        counters["errors"] += 1
        return counters

    today = datetime.now(timezone.utc)
    from_date = (today - timedelta(days=days)).strftime("%Y%m%d")
    to_date = today.strftime("%Y%m%d")

    # Per-language paginated walk. We always pull both EN and ZH so we can
    # detect the "published in Chinese section" placeholder pattern.
    rows_by_lang: dict[str, list[dict]] = {}
    for lang in ("E", "ZH"):
        try:
            rows = search_paginated(
                session, sid, lang=lang,
                from_date=from_date, to_date=to_date,
                t1code=-2, t2code=-2,
            )
        except Exception as e:
            logger.error("[%s] search %s failed: %s", ticker.canonical, lang, e)
            counters["errors"] += 1
            rows = []
        rows_by_lang[lang] = rows

    # Index by NEWS_ID for cross-language join
    by_id_en = {str(r.get("NEWS_ID")): r for r in rows_by_lang.get("E", []) if r.get("NEWS_ID")}
    by_id_zh = {str(r.get("NEWS_ID")): r for r in rows_by_lang.get("ZH", []) if r.get("NEWS_ID")}
    all_ids = sorted(set(by_id_en) | set(by_id_zh),
                     key=lambda i: int(i) if i.isdigit() else 0,
                     reverse=True)

    if not all_categories:
        # Filter by tier-2 to revenue-modeling subset
        def _keep(news_id: str) -> bool:
            r = by_id_en.get(news_id) or by_id_zh.get(news_id)
            t1, t2 = _split_long_text(_decode(r.get("LONG_TEXT", "")))
            t2_code = _label_to_code(t2, t2_map)
            return t2_code in DOC_TYPE_FILTER
        all_ids = [i for i in all_ids if _keep(i)]

    n = 0
    for news_id in all_ids:
        if limit and n >= limit:
            break
        try:
            status = ingest_announcement(
                session, coll, ticker,
                row_en=by_id_en.get(news_id),
                row_zh=by_id_zh.get(news_id),
                t1_map=t1_map, t2_map=t2_map,
                download_pdf=download_pdf,
            )
            counters[status] = counters.get(status, 0) + 1
        except Exception as e:
            logger.error("[%s] ingest %s failed: %s", ticker.canonical, news_id, e)
            counters["errors"] += 1
        n += 1

    save_state(SOURCE, bucket=ticker.canonical,
               last_run_at=datetime.now(timezone.utc),
               filings_seen=n)
    record_daily_stat(SOURCE, ticker.canonical,
                      added=counters["added"], skipped=counters["skipped"],
                      errors=counters["errors"], pdfs=counters["added"])
    return counters


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--ticker", action="append",
                   help="Listing code (e.g. 01347) or canonical (01347.HK). Repeatable.")
    p.add_argument("--days", type=int, default=365,
                   help="Days back to query (default 365).")
    p.add_argument("--max", type=int, default=None,
                   help="Max filings per ticker per run.")
    p.add_argument("--limit", type=int, default=None, help="alias for --max")
    p.add_argument("--no-pdf", action="store_true", help="Skip PDF download.")
    p.add_argument("--all-categories", action="store_true",
                   help="Don't filter by DOC_TYPE_FILTER — pull every announcement.")
    p.add_argument("--refresh-taxonomy", action="store_true",
                   help="Force re-pull of activestock + tierone/tiertwo JSONs.")
    p.add_argument("--show-state", action="store_true")
    p.add_argument("--watch", action="store_true")
    p.add_argument("--interval", type=int, default=1800,
                   help="Watch loop interval (default 30min).")
    args = p.parse_args(sys.argv[1:] if len(sys.argv) > 1 else [])

    ensure_indexes(SOURCE)

    tickers = HK_TICKERS
    if args.ticker:
        wanted = {t.upper() for t in args.ticker}
        tickers = [t for t in HK_TICKERS
                   if t.canonical.upper() in wanted or t.listing_code.upper() in wanted]

    if args.show_state:
        coll = get_collection(SOURCE)
        for t in tickers:
            st = load_state(SOURCE, bucket=t.canonical)
            n = coll.count_documents({"ticker_canonical": t.canonical})
            print(f"  {t.canonical:>10} {t.listing_code} {t.name_en:<35} "
                  f"filings={n:>4}  last_run={st.get('last_run_at') or '—'}")
        return

    sess = make_session()
    if args.refresh_taxonomy:
        for p in (STOCK_INDEX_PATH, TIERONE_PATH, TIERTWO_PATH):
            if p.exists():
                p.unlink()

    stock_index = load_stock_index(sess)
    t1_map, t2_map = load_tier_taxonomy(sess)
    logger.info("HKEX taxonomy loaded: %d stocks, %d t1, %d t2",
                len(stock_index), len(t1_map), len(t2_map))

    limit = args.limit or args.max

    while True:
        round_start = datetime.now(timezone.utc)
        totals = {"added": 0, "updated": 0, "skipped": 0, "errors": 0}
        for t in tickers:
            c = crawl_one_ticker(
                sess, t,
                stock_id_map=stock_index,
                t1_map=t1_map, t2_map=t2_map,
                days=args.days, limit=limit,
                download_pdf=not args.no_pdf,
                all_categories=args.all_categories,
            )
            for k, v in c.items():
                totals[k] = totals.get(k, 0) + v
            logger.info("[%s] %s", t.canonical, c)

        elapsed = (datetime.now(timezone.utc) - round_start).total_seconds()
        logger.info("ROUND DONE in %.1fs — totals=%s", elapsed, totals)

        if not args.watch:
            break
        sleep_s = max(60, args.interval - int(elapsed))
        logger.info("Watch loop: sleeping %ss", sleep_s)
        time.sleep(sleep_s)


if __name__ == "__main__":
    main()
