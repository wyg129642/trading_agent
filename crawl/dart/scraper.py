#!/usr/bin/env python3
"""DART (전자공시시스템 / 금융감독원) scraper — pulls Korean statutory filings
(사업보고서 / 반기보고서 / 분기보고서 / 주요사항보고) for the 3 in-scope KR
holdings into Mongo ``ir_filings.dart`` + structured FS line items into
``ir_filings.dart_fnltt``.

Why
---
DART is the official Korean disclosure system, free to use. Filings are
bilingual-ish (most are Korean-only; Samsung/SK Hynix have separate English IR
PDFs *outside* DART that we'd need to scrape via ir_pages later). The
fnltt API exposes structured FS line items (Income / Balance Sheet) ready
for joining; segment revenue still requires HTML parsing of Note 38 영업부문.

Auth
----
crtfc_key (40-hex string) issued at https://opendart.fss.or.kr — single key
per member; ~20k req/day. Drop into ``crawl/dart/credentials.json`` as
``{"crtfc_key": "..."}`` or set env ``DART_CRTFC_KEY``.

CLI
---
  python3 scraper.py --show-state
  python3 scraper.py --refresh-corp-codes      # pull/cache corpCode.xml ZIP
  python3 scraper.py --ticker 005930 --days 365
  python3 scraper.py --max 30 --watch --interval 7200
"""
from __future__ import annotations

import argparse
import io
import json
import os
import sys
import time
import xml.etree.ElementTree as ET
import zipfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

import requests

_HERE = Path(__file__).resolve().parent
_CRAWL_ROOT = _HERE.parent
_REPO_ROOT = _CRAWL_ROOT.parent
sys.path.insert(0, str(_REPO_ROOT))
sys.path.insert(0, str(_CRAWL_ROOT))

from crawl.ir_filings.common import (  # noqa: E402
    ensure_indexes, get_collection, get_db,
    load_state, make_filing_doc, pdf_dir, record_daily_stat,
    safe_filename, save_state, setup_logging, upsert_filing,
)
from crawl.ir_filings.tickers import KR_TICKERS, IrTicker  # noqa: E402

SOURCE = "dart"
DART_PDF_ROOT = pdf_dir(SOURCE)
COLL_FILINGS = SOURCE
COLL_FNLTT = "dart_fnltt"

API_BASE = "https://opendart.fss.or.kr/api"
CREDS_FILE = _HERE / "credentials.json"
CORP_CODE_CACHE = _HERE / "cache" / "corpCode.xml"
CORP_CODE_TTL_DAYS = 7

THROTTLE_S = float(os.environ.get("DART_THROTTLE", "0.3"))

# Periodic-disclosure detail types (pblntf_detail_ty)
WANTED_DETAIL_TYPES = {
    "A001": "사업보고서 Annual Business Report",       # ★★★★★ (Note 38 segment)
    "A002": "반기보고서 Semi-Annual Report",          # ★★★★
    "A003": "분기보고서 Quarterly Report",            # ★★★
}

# Major-events (B-type) we care about for revenue modeling — title substrings
B_TYPE_TITLE_PATTERNS = [
    "매출액또는손익구조",      # material revenue/profit change disclosure
    "타법인 주식 및 출자증권 취득",
    "유상증자결정",
    "전환사채 발행결정",
    "회사분할결정",
    "합병결정",
]

# DART pblntf_ty
PBLNTF_TYPES = ["A", "B"]                # A=periodic, B=major events; can extend later

# fnltt reprt_code mapping
REPRT_CODE_FOR_PERIOD = {
    "Q1":      "11013",
    "H1":      "11012",
    "Q3":      "11014",
    "FY":      "11011",
}

logger = setup_logging(SOURCE)

# ============================================================
# Credentials
# ============================================================

def load_key() -> str:
    key = os.environ.get("DART_CRTFC_KEY", "").strip()
    if key:
        return key
    if CREDS_FILE.exists():
        try:
            return (json.loads(CREDS_FILE.read_text(encoding="utf-8"))
                    .get("crtfc_key") or "").strip()
        except Exception as e:
            logger.error("credentials.json unreadable: %s", e)
    return ""


# ============================================================
# HTTP
# ============================================================

def make_session() -> requests.Session:
    s = requests.Session()
    s.trust_env = False                  # bypass Clash
    s.proxies = {}
    s.headers.update({
        "User-Agent": "Mozilla/5.0 trading-intel-research",
        "Accept": "*/*",
    })
    return s


def http_get(s: requests.Session, url: str, params: dict, *,
             timeout: int = 60, stream: bool = False) -> requests.Response:
    r = s.get(url, params=params, timeout=timeout, stream=stream)
    time.sleep(THROTTLE_S)
    r.raise_for_status()
    return r


# ============================================================
# corp_code resolver (cached weekly)
# ============================================================

def refresh_corp_codes(s: requests.Session, key: str) -> bytes:
    CORP_CODE_CACHE.parent.mkdir(parents=True, exist_ok=True)
    r = http_get(s, f"{API_BASE}/corpCode.xml", params={"crtfc_key": key},
                 timeout=120)
    body = r.content
    # body is a ZIP containing CORPCODE.xml (despite the .xml URL)
    with zipfile.ZipFile(io.BytesIO(body)) as z:
        xml_bytes = z.read("CORPCODE.xml")
    CORP_CODE_CACHE.write_bytes(xml_bytes)
    return xml_bytes


def load_corp_codes(s: requests.Session, key: str, *,
                    force: bool = False) -> dict[str, str]:
    """Returns {stock_code (6-digit): corp_code (8-digit)}. Listed only."""
    if not force and CORP_CODE_CACHE.exists():
        age_days = (time.time() - CORP_CODE_CACHE.stat().st_mtime) / 86400
        if age_days >= CORP_CODE_TTL_DAYS:
            try:
                refresh_corp_codes(s, key)
            except Exception as e:
                logger.warning("corp_codes refresh failed: %s — using cache", e)
    elif force or not CORP_CODE_CACHE.exists():
        refresh_corp_codes(s, key)
    xml_bytes = CORP_CODE_CACHE.read_bytes()
    root = ET.fromstring(xml_bytes)
    out: dict[str, str] = {}
    for node in root.findall("list"):
        sc = (node.findtext("stock_code") or "").strip()
        cc = (node.findtext("corp_code") or "").strip()
        if sc and cc:
            out[sc] = cc
    return out


# ============================================================
# Filing list
# ============================================================

def list_filings(s: requests.Session, key: str, corp_code: str, *,
                 days: int = 365,
                 pblntf_ty: str = "A",
                 page_count: int = 100) -> list[dict]:
    out: list[dict] = []
    end = date.today().strftime("%Y%m%d")
    bgn = (date.today() - timedelta(days=days)).strftime("%Y%m%d")
    page_no = 1
    while True:
        params = {
            "crtfc_key": key, "corp_code": corp_code,
            "bgn_de": bgn, "end_de": end,
            "pblntf_ty": pblntf_ty,
            "page_no": str(page_no), "page_count": str(page_count),
        }
        try:
            r = http_get(s, f"{API_BASE}/list.json", params=params)
            payload = r.json()
        except Exception as e:
            logger.error("list.json corp=%s page=%s failed: %s",
                         corp_code, page_no, e)
            break
        st = str(payload.get("status"))
        if st == "013":                  # no data
            break
        if st != "000":
            logger.error("list.json corp=%s status=%s msg=%s",
                         corp_code, st, payload.get("message"))
            break
        items = payload.get("list") or []
        out.extend(items)
        if page_no >= int(payload.get("total_page") or 1):
            break
        page_no += 1
    return out


# ============================================================
# Document download (HTML inside ZIP)
# ============================================================

def download_document(s: requests.Session, key: str, rcept_no: str, *,
                      corp_code: str) -> tuple[Path, int, str, str]:
    """Pull /document.xml — returns ZIP. Extract primary HTML and write both
    the raw ZIP + extracted HTML to disk. Returns (html_path, total_size, err,
    extracted_text)."""
    ym = datetime.now(timezone.utc).strftime("%Y-%m")
    out_dir = DART_PDF_ROOT / corp_code / ym
    out_dir.mkdir(parents=True, exist_ok=True)
    zip_path = out_dir / f"{rcept_no}.zip"
    html_path = out_dir / f"{rcept_no}.html"
    if html_path.exists() and html_path.stat().st_size > 0:
        return html_path, html_path.stat().st_size, "", ""
    try:
        r = http_get(s, f"{API_BASE}/document.xml",
                     params={"crtfc_key": key, "rcept_no": rcept_no},
                     timeout=180, stream=False)
        body = r.content
        # detect error envelope
        ctype = r.headers.get("Content-Type", "")
        if ctype.startswith("application/xml") and len(body) < 1000:
            return html_path, 0, body.decode("utf-8", errors="ignore")[:200], ""
        zip_path.write_bytes(body)
        # Extract primary HTML — typically named <rcept_no>.xml inside (XHTML
        # body despite .xml extension)
        try:
            with zipfile.ZipFile(io.BytesIO(body)) as zf:
                html_name = None
                for n in zf.namelist():
                    if n.endswith(".xml"):
                        html_name = n
                        break
                if html_name:
                    html_bytes = zf.read(html_name)
                    html_path.write_bytes(html_bytes)
                    return html_path, len(body), "", ""
        except zipfile.BadZipFile as e:
            return html_path, 0, f"bad zip: {e}", ""
        return html_path, len(body), "no html in zip", ""
    except Exception as e:
        return html_path, 0, str(e)[:300], ""


# ============================================================
# Structured FS via fnltt API
# ============================================================

def reprt_code_for_report_nm(report_nm: str) -> Optional[str]:
    """Map filing title → reprt_code for fnltt fetch."""
    if "사업보고서" in report_nm:
        return "11011"        # FY annual
    if "반기보고서" in report_nm:
        return "11012"        # H1
    if "분기보고서" in report_nm:
        # Q1 vs Q3 — guess from the period in parens. Default Q1 (DART itself
        # ambiguates this — proper resolution requires matching against the
        # fiscal calendar, but Samsung/SK Hynix file Q1 in May, Q3 in Nov, so
        # the period parsing is mostly cosmetic for our use).
        return "11013"
    return None


def fetch_fnltt(s: requests.Session, key: str, corp_code: str, *,
                year: int, reprt_code: str, fs_div: str = "CFS") -> list[dict]:
    """fnltt = full statement (every line item across BS/IS/CIS/CF/SCE)."""
    try:
        r = http_get(s, f"{API_BASE}/fnlttSinglAcntAll.json", params={
            "crtfc_key": key, "corp_code": corp_code,
            "bsns_year": str(year), "reprt_code": reprt_code,
            "fs_div": fs_div,
        }, timeout=60)
    except Exception as e:
        logger.warning("fnltt %s/%s/%s failed: %s",
                       corp_code, year, reprt_code, e)
        return []
    payload = r.json()
    st = str(payload.get("status"))
    if st in ("013",):                   # no data — pre-2015 or KOSDAQ company
        return []
    if st != "000":
        logger.warning("fnltt %s/%s/%s status=%s msg=%s",
                       corp_code, year, reprt_code, st, payload.get("message"))
        return []
    return payload.get("list") or []


def upsert_fnltt_rows(coll, ticker: IrTicker, rows: list[dict], *,
                       year: int, reprt_code: str) -> int:
    n = 0
    now = datetime.now(timezone.utc)
    for i, row in enumerate(rows):
        sj_div = row.get("sj_div") or ""
        account_id = row.get("account_id") or ""
        key = f"{ticker.corp_code}_{row.get('rcept_no','')}_{sj_div}_{account_id}_{i}"
        doc = {
            "_id":              key,
            "corp_code":        ticker.corp_code,
            "ticker_canonical": ticker.canonical,
            "ticker_local":     ticker.listing_code,
            "organization":     ticker.name_en,
            "rcept_no":         row.get("rcept_no"),
            "bsns_year":        year,
            "reprt_code":       reprt_code,
            "fs_div":           row.get("fs_div"),
            "fs_nm":            row.get("fs_nm"),
            "sj_div":           sj_div,
            "sj_nm":            row.get("sj_nm"),
            "account_id":       account_id,
            "account_nm":       row.get("account_nm"),
            "account_detail":   row.get("account_detail"),
            "thstrm_amount":    row.get("thstrm_amount"),
            "frmtrm_amount":    row.get("frmtrm_amount"),
            "bfefrmtrm_amount": row.get("bfefrmtrm_amount"),
            "currency":         row.get("currency"),
            "_canonical_tickers": [ticker.canonical],
            "ingested_at":      now,
        }
        coll.replace_one({"_id": key}, doc, upsert=True)
        n += 1
    return n


# ============================================================
# Filing → Mongo
# ============================================================

def _parse_dt(yyyymmdd: str) -> int:
    """`20260317` (KST date-only) → UTC ms (assume 18:00 KST file time =
    09:00 UTC; close enough for sorting purposes — DART doesn't expose
    intraday time)."""
    try:
        dt_local = datetime.strptime(yyyymmdd.strip(), "%Y%m%d")
        from datetime import timezone as tz, timedelta as td
        # default to noon KST so sort is stable
        dt_kst = dt_local.replace(hour=12, tzinfo=tz(td(hours=9)))
        return int(dt_kst.astimezone(tz.utc).timestamp() * 1000)
    except Exception:
        return 0


def categorize_filing(report_nm: str) -> tuple[str, str]:
    """Map report_nm → category code + label. Periodic gets A001/A002/A003;
    major events get a 'B_<keyword>' synthetic code."""
    if "사업보고서" in report_nm:
        return "A001", WANTED_DETAIL_TYPES["A001"]
    if "반기보고서" in report_nm:
        return "A002", WANTED_DETAIL_TYPES["A002"]
    if "분기보고서" in report_nm:
        return "A003", WANTED_DETAIL_TYPES["A003"]
    for pattern in B_TYPE_TITLE_PATTERNS:
        if pattern in report_nm:
            return f"B_{pattern[:20]}", report_nm
    return "OTHER", report_nm


def ingest_one(s: requests.Session, key: str, coll, fnltt_coll,
               ticker: IrTicker, filing: dict, *,
               download_pdf: bool = True,
               fetch_fnltt_data: bool = True) -> str:
    rcept_no = filing.get("rcept_no") or ""
    if not rcept_no:
        return "skipped"

    existing = coll.find_one({"_id": rcept_no}, {"pdf_local_path": 1})
    if existing and existing.get("pdf_local_path"):
        if Path(existing["pdf_local_path"]).exists():
            return "skipped"

    report_nm = filing.get("report_nm") or ""
    category, category_name = categorize_filing(report_nm)
    rcept_dt = filing.get("rcept_dt") or ""
    release_ms = _parse_dt(rcept_dt)
    fy = None
    try:
        # YYYYMMDD — fiscal year ≈ calendar year of filing for KR (Mar-end is
        # rare, most are Dec-end); refined later if needed.
        fy = int(rcept_dt[:4])
    except (TypeError, ValueError):
        pass
    fp = None
    if "사업보고서" in report_nm:
        fp = "FY"
    elif "반기보고서" in report_nm:
        fp = "H1"
    elif "분기보고서" in report_nm:
        fp = "Q1"                        # heuristic; 분기 doesn't specify Q1 vs Q3 in title

    # Download primary HTML (within ZIP)
    pdf_local = ""
    pdf_size = 0
    pdf_err = ""
    pdf_unavailable = False
    if download_pdf:
        path, size, err, _ = download_document(s, key, rcept_no,
                                                corp_code=ticker.corp_code)
        if size > 0:
            pdf_local = str(path)
            pdf_size = size
        else:
            pdf_err = err
            pdf_unavailable = True

    rel_path = ""
    if pdf_local:
        try:
            rel_path = str(Path(pdf_local).relative_to(DART_PDF_ROOT))
        except ValueError:
            rel_path = pdf_local

    web_url = f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept_no}"

    extra = {
        "rcept_no":     rcept_no,
        "corp_code":    ticker.corp_code,
        "stock_code":   filing.get("stock_code"),
        "corp_name":    filing.get("corp_name"),
        "corp_cls":     filing.get("corp_cls"),
        "report_nm":    report_nm,
        "flr_nm":       filing.get("flr_nm"),
        "rcept_dt":     rcept_dt,
        "rm":           filing.get("rm"),
    }

    doc = make_filing_doc(
        doc_id=rcept_no,
        source=SOURCE,
        category=category,
        category_name=category_name,
        title=report_nm,
        title_local=report_nm,
        release_time_ms=release_ms,
        organization=filing.get("corp_name") or ticker.name_local or ticker.name_en,
        ticker_local=filing.get("stock_code") or ticker.listing_code,
        ticker_canonical=ticker.canonical,
        period_start=None,
        period_end=None,
        fiscal_year=fy,
        fiscal_period=fp,
        lang="ko",
        doc_introduce=report_nm,
        content_md="",
        pdf_rel_path=rel_path,
        pdf_local_path=pdf_local,
        pdf_size_bytes=pdf_size,
        pdf_download_error=pdf_err,
        pdf_unavailable=pdf_unavailable,
        web_url=web_url,
        list_item=filing,
        extra=extra,
    )
    upsert_filing(coll, doc)

    # Pull structured FS for periodic reports (A001/A002/A003)
    if fetch_fnltt_data and category in ("A001", "A002", "A003") and fy:
        reprt_code = reprt_code_for_report_nm(report_nm)
        if reprt_code:
            try:
                rows = fetch_fnltt(s, key, ticker.corp_code,
                                   year=fy, reprt_code=reprt_code, fs_div="CFS")
                if rows:
                    n = upsert_fnltt_rows(fnltt_coll, ticker, rows,
                                          year=fy, reprt_code=reprt_code)
                    logger.info("[%s] fnltt %s/%s: %d line items",
                                ticker.canonical, fy, reprt_code, n)
            except Exception as e:
                logger.warning("[%s] fnltt fetch failed: %s",
                               ticker.canonical, e)

    return "updated" if existing else "added"


# ============================================================
# Driver
# ============================================================

def crawl_one_ticker(s: requests.Session, key: str, ticker: IrTicker, *,
                     days: int = 365, limit: Optional[int] = None,
                     download_pdf: bool = True,
                     all_categories: bool = False,
                     fetch_fnltt_data: bool = True) -> dict[str, int]:
    coll = get_collection(SOURCE)
    fnltt_coll = get_db()[COLL_FNLTT]
    counters = {"added": 0, "updated": 0, "skipped": 0, "errors": 0}

    if not ticker.corp_code:
        logger.error("[%s] no corp_code — refresh-corp-codes?", ticker.canonical)
        counters["errors"] += 1
        return counters

    all_filings: list[dict] = []
    for ty in PBLNTF_TYPES:
        try:
            items = list_filings(s, key, ticker.corp_code, days=days, pblntf_ty=ty)
            all_filings.extend(items)
        except Exception as e:
            logger.error("[%s] list (%s) failed: %s", ticker.canonical, ty, e)
            counters["errors"] += 1

    # Filter to revenue-modeling-relevant filings unless --all
    if not all_categories:
        kept = []
        for f in all_filings:
            nm = f.get("report_nm") or ""
            if any(p in nm for p in ("사업보고서", "반기보고서", "분기보고서")):
                kept.append(f)
                continue
            if any(p in nm for p in B_TYPE_TITLE_PATTERNS):
                kept.append(f)
        all_filings = kept

    # Sort newest-first then limit
    all_filings.sort(key=lambda f: (f.get("rcept_dt") or "", f.get("rcept_no") or ""),
                     reverse=True)
    if limit:
        all_filings = all_filings[:limit]

    for f in all_filings:
        try:
            status = ingest_one(s, key, coll, fnltt_coll, ticker, f,
                                 download_pdf=download_pdf,
                                 fetch_fnltt_data=fetch_fnltt_data)
            counters[status] = counters.get(status, 0) + 1
        except Exception as e:
            logger.error("[%s] ingest %s failed: %s",
                         ticker.canonical, f.get("rcept_no"), e)
            counters["errors"] += 1

    save_state(SOURCE, bucket=ticker.canonical,
               last_run_at=datetime.now(timezone.utc),
               filings_seen=len(all_filings))
    record_daily_stat(SOURCE, ticker.canonical,
                      added=counters["added"], skipped=counters["skipped"],
                      errors=counters["errors"], pdfs=counters["added"])
    return counters


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--ticker", action="append")
    p.add_argument("--days", type=int, default=365)
    p.add_argument("--max", type=int, default=None)
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--no-pdf", action="store_true")
    p.add_argument("--no-fnltt", action="store_true")
    p.add_argument("--all-categories", action="store_true")
    p.add_argument("--refresh-corp-codes", action="store_true")
    p.add_argument("--show-state", action="store_true")
    p.add_argument("--watch", action="store_true")
    p.add_argument("--interval", type=int, default=7200)
    args = p.parse_args(sys.argv[1:] if len(sys.argv) > 1 else [])

    ensure_indexes(SOURCE)

    tickers = KR_TICKERS
    if args.ticker:
        wanted = {t.upper() for t in args.ticker}
        tickers = [t for t in KR_TICKERS
                   if t.canonical.upper() in wanted or t.listing_code.upper() in wanted]

    if args.show_state:
        coll = get_collection(SOURCE)
        fnltt = get_db()[COLL_FNLTT]
        for t in tickers:
            st = load_state(SOURCE, bucket=t.canonical)
            n = coll.count_documents({"ticker_canonical": t.canonical})
            nf = fnltt.count_documents({"ticker_canonical": t.canonical})
            print(f"  {t.canonical:>10} {t.listing_code} {t.name_en:<35} "
                  f"filings={n:>4} fnltt={nf:>5} last_run={st.get('last_run_at') or '—'}")
        return

    key = load_key()
    if not key:
        print("ERROR: no DART crtfc_key. Set DART_CRTFC_KEY env "
              "or write to crawl/dart/credentials.json: {\"crtfc_key\":\"...\"}",
              file=sys.stderr)
        sys.exit(2)

    sess = make_session()

    # Fill in any missing corp_codes from cached corpCode.xml (e.g. SHINSUNG E&G)
    code_map = load_corp_codes(sess, key, force=args.refresh_corp_codes)
    for t in tickers:
        if not t.corp_code and t.listing_code in code_map:
            # immutable dataclass — replace via __setattr__ wrapper trick
            object.__setattr__(t, "corp_code", code_map[t.listing_code])
            logger.info("[%s] resolved corp_code %s from cache",
                        t.canonical, t.corp_code)

    limit = args.limit or args.max
    while True:
        round_start = datetime.now(timezone.utc)
        totals = {"added": 0, "updated": 0, "skipped": 0, "errors": 0}
        for t in tickers:
            c = crawl_one_ticker(sess, key, t,
                                 days=args.days, limit=limit,
                                 download_pdf=not args.no_pdf,
                                 all_categories=args.all_categories,
                                 fetch_fnltt_data=not args.no_fnltt)
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
