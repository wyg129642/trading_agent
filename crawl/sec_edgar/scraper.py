#!/usr/bin/env python3
"""SEC EDGAR scraper — pulls IR filings + XBRL companyfacts for the 22 in-scope
US-listed holdings into Mongo ``ir_filings.sec_edgar`` and ``ir_filings.sec_xbrl_facts``.

Why
---
Reliable filings beat per-company IR scraping by a huge margin. EDGAR is free,
has a stable JSON API at ``data.sec.gov``, no key required (just a declared
User-Agent), and ships every periodic filing (10-K/10-Q) in inline XBRL with
a separate companyfacts JSON that the chat / revenue model can join on.

Endpoints used (verified live 2026-04-28)
------------------------------------------
  GET https://www.sec.gov/files/company_tickers.json     # ticker → CIK (cached locally)
  GET https://data.sec.gov/submissions/CIK{padded}.json   # filing index per ticker
  GET https://data.sec.gov/api/xbrl/companyfacts/CIK{padded}.json  # XBRL fact dump
  GET https://www.sec.gov/Archives/edgar/data/{cik_int}/{accn_nodash}/{filename}
  GET https://www.sec.gov/Archives/edgar/data/{cik_int}/{accn_nodash}/FilingSummary.xml

Forms ingested by default (revenue-modeling priority — see CLAUDE.md memory):
  10-K, 10-K/A, 10-Q, 10-Q/A, 8-K (Item 2.02 only), 8-K/A,
  20-F, 20-F/A, 6-K, 6-K/A,         # FPI: TSM, NOK, NBIS, SGML, TSEM
  DEF 14A, DEFA14A,                  # proxy / supplementary
  S-1, S-1/A, F-1, F-1/A, 424B4      # IPO prospectus (CRCL, NBIS, NEOV)

CLI
---
  python3 scraper.py --show-state
  python3 scraper.py --ticker INTC --limit 5
  python3 scraper.py --max 50                  # all tickers, ≤50 filings each
  python3 scraper.py --watch --interval 900    # 15min poll loop
  python3 scraper.py --xbrl-only --ticker INTC # only refresh companyfacts (no filings)
  python3 scraper.py --since-hours 24

Doc shape (filings collection):
  See ``crawl/ir_filings/common.py::make_filing_doc``. Source-specific extras:
    cik_padded, accession, form, items (8-K), is_inline_xbrl, is_xbrl_numeric,
    primary_doc_filename, primary_doc_description, attachments[]

Doc shape (sec_xbrl_facts):
  _id = f"{cik_padded}_{taxonomy}_{tag}_{frame}_{accn_no_dash}"
  cik, ticker_canonical, taxonomy, tag, label, frame, fy, fp, form, accn, filed,
  start, end, val, unit
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Optional

import requests

# Crawl + ir_filings module imports
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
from crawl.ir_filings.tickers import US_TICKERS, IrTicker  # noqa: E402

# ============================================================
# Constants
# ============================================================

UA_DEFAULT = "Trading Intel Research belloannette726@gmail.com"
UA = os.environ.get("SEC_EDGAR_UA", UA_DEFAULT)

# 0.15s base = ~6.5 req/s; 10 req/s is the documented hard cap. Stay comfortably
# under it across all 22 tickers without burning through too slowly.
THROTTLE_BASE_S = float(os.environ.get("SEC_EDGAR_THROTTLE", "0.15"))

WANTED_FORMS = {
    "10-K", "10-K/A",
    "10-Q", "10-Q/A",
    "8-K",  "8-K/A",
    "20-F", "20-F/A",
    "6-K",  "6-K/A",
    "DEF 14A", "DEFA14A",
    "S-1",  "S-1/A",
    "F-1",  "F-1/A",
    "424B4",
}

# 8-K is firehose; only ingest the ones with earnings results (Item 2.02). Other
# 8-K item codes (e.g. 5.02 director changes, 1.01 contracts) are noise for
# revenue modeling — excluded by default. Override via --all-8k.
EARNINGS_8K_ITEM = "2.02"

# Collection names (set in common.py.COLLECTION_FOR_SOURCE)
SOURCE = "sec_edgar"
COLL_FILINGS = SOURCE
COLL_FACTS = "sec_xbrl_facts"

# PDF root for SEC: <root>/sec_edgar/<cik>/<accn>/<filename>
SEC_PDF_ROOT = pdf_dir(SOURCE)

logger = setup_logging(SOURCE)

# ============================================================
# HTTP session
# ============================================================

def make_session() -> requests.Session:
    s = requests.Session()
    # Bypass Clash on :7890 — SEC is public, US-hosted, requires no proxy. Going
    # through the local Clash proxy returned HTTP 400 (Clash mangles something
    # on data.sec.gov requests; verified 2026-04-28 — direct curl 200, proxy 400).
    s.trust_env = False
    s.proxies = {}
    s.headers.update({
        "User-Agent": UA,
        "Accept-Encoding": "gzip, deflate",
    })
    return s


def get(session: requests.Session, url: str, *, accept: str = "application/json",
        retries: int = 3) -> requests.Response:
    """Polite SEC GET with backoff. SEC enforces 10 req/s edge-side; we sleep
    THROTTLE_BASE_S between calls to land at ~6 req/s."""
    headers = {"Accept": accept}
    last_err: Optional[Exception] = None
    for attempt in range(retries):
        try:
            r = session.get(url, headers=headers, timeout=60)
            time.sleep(THROTTLE_BASE_S)
            if r.status_code == 200:
                return r
            if r.status_code in (429, 503):
                wait = 2 ** attempt * 2
                logger.warning("SEC %s on %s — backing off %ss", r.status_code, url, wait)
                time.sleep(wait)
                continue
            if r.status_code == 403:
                # missing/invalid UA — fatal
                raise RuntimeError(f"SEC 403 on {url} — User-Agent rejected: {UA!r}")
            r.raise_for_status()
        except requests.RequestException as e:
            last_err = e
            time.sleep(2 ** attempt)
    raise RuntimeError(f"SEC GET failed after {retries} retries: {url} ({last_err})")


# ============================================================
# Filing index
# ============================================================

def fetch_submissions(session: requests.Session, cik_padded: str) -> dict:
    """Pull all filings for one CIK. If `filings.files` populated (legacy filers
    with >1000 historical filings — INTC, GLW, MU, NOK, CIEN are likely), walk
    them too and return a merged ``recent``-shaped struct."""
    url = f"https://data.sec.gov/submissions/CIK{cik_padded}.json"
    sub = get(session, url).json()
    rec = sub["filings"]["recent"]
    files = sub["filings"].get("files") or []
    for f in files:
        url = f"https://data.sec.gov/submissions/{f['name']}"
        page = get(session, url).json()
        # page is shaped like `recent` directly (no "filings" wrapper)
        for k, v in page.items():
            if k in rec:
                rec[k].extend(v)
    sub["filings"]["recent"] = rec
    return sub


def iter_filings(sub: dict, ticker: IrTicker, *,
                 forms: Optional[set] = None,
                 since_ms: Optional[int] = None,
                 include_all_8k: bool = False):
    """Yield row-oriented filing dicts from a submissions response. Filters
    to ``WANTED_FORMS`` (or `forms` override), drops 8-K without 2.02 unless
    `include_all_8k`."""
    rec = sub["filings"]["recent"]
    n = len(rec.get("accessionNumber", []))
    forms = forms or WANTED_FORMS
    for i in range(n):
        form = rec["form"][i]
        if form not in forms:
            continue
        items = rec["items"][i] or ""
        if form.startswith("8-K") and not include_all_8k and EARNINGS_8K_ITEM not in items.split(","):
            continue
        accept = rec["acceptanceDateTime"][i]      # "2026-04-27T21:27:13.000Z"
        # Convert to UTC ms — SEC uses ISO with millisecond fractional + Z
        try:
            dt = datetime.strptime(accept[:23], "%Y-%m-%dT%H:%M:%S.%f").replace(tzinfo=timezone.utc)
        except ValueError:
            dt = datetime.strptime(accept[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
        ms = int(dt.timestamp() * 1000)
        if since_ms is not None and ms < since_ms:
            continue
        yield {
            "accession":         rec["accessionNumber"][i],
            "form":              form,
            "filing_date":       rec["filingDate"][i],
            "report_date":       (rec["reportDate"][i] or None),
            "acceptance_dt_utc": accept,
            "release_time_ms":   ms,
            "items":             items or None,
            "primary_doc":       rec["primaryDocument"][i],
            "primary_doc_desc":  rec["primaryDocDescription"][i],
            "size_bytes":        rec["size"][i],
            "is_inline_xbrl":    bool(rec["isInlineXBRL"][i]),
            "is_xbrl_numeric":   bool(rec["isXBRLNumeric"][i]),
            "_cik":              ticker.cik,
            "_ticker":           ticker,
        }


def doc_url(cik_padded: str, accession: str, primary_doc: str) -> str:
    cik_int = int(cik_padded)
    accn = accession.replace("-", "")
    return f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{accn}/{primary_doc}"


def filing_index_url(cik_padded: str, accession: str) -> str:
    cik_int = int(cik_padded)
    accn = accession.replace("-", "")
    return f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{accn}/"


def web_url(cik_padded: str, accession: str) -> str:
    cik_int = int(cik_padded)
    return (f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany"
            f"&CIK={cik_int}&type=&dateb=&owner=include&count=40")


# ============================================================
# Document download
# ============================================================

def download_primary(session: requests.Session, cik_padded: str, accession: str,
                     primary_doc: str) -> tuple[Path, int, str]:
    """Download the primary doc into ``<SEC_PDF_ROOT>/<cik>/<accn>/<filename>``.
    Returns (local_path, size_bytes, error_str)."""
    out_dir = SEC_PDF_ROOT / cik_padded / accession.replace("-", "")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / safe_filename(primary_doc, max_len=200)
    if out_path.exists() and out_path.stat().st_size > 0:
        return out_path, out_path.stat().st_size, ""
    url = doc_url(cik_padded, accession, primary_doc)
    try:
        accept = "application/pdf" if primary_doc.lower().endswith(".pdf") else "*/*"
        r = get(session, url, accept=accept)
        out_path.write_bytes(r.content)
        return out_path, len(r.content), ""
    except Exception as e:
        return out_path, 0, str(e)[:500]


def fetch_filing_summary_attachments(session: requests.Session,
                                      cik_padded: str, accession: str) -> list[dict]:
    """Parse FilingSummary.xml to enumerate every attachment (R*.htm rendered
    statement tables, exhibits, charts). Returned list is purely metadata — we
    don't bulk-download R*.htm here, that's terabytes across 22 tickers; only
    the primary doc + any PDF exhibits get pulled. Used so the chat tool /
    backend mirror can surface attachments as clickable links."""
    url = filing_index_url(cik_padded, accession) + "FilingSummary.xml"
    try:
        r = get(session, url, accept="application/xml")
    except Exception as e:
        logger.debug("FilingSummary not available for %s: %s", accession, e)
        return []
    import xml.etree.ElementTree as ET
    try:
        root = ET.fromstring(r.content)
    except ET.ParseError:
        return []
    out: list[dict] = []
    for rep in root.findall(".//Report"):
        out.append({
            "short_name": (rep.findtext("ShortName") or "").strip(),
            "long_name":  (rep.findtext("LongName") or "").strip(),
            "html":       (rep.findtext("HtmlFileName") or "").strip(),
            "type":       (rep.findtext("ReportType") or "").strip(),
            "menu":       (rep.findtext("MenuCategory") or "").strip(),
        })
    return out


# ============================================================
# Filing → Mongo
# ============================================================

# Map raw form code → human label for category_name (UI-friendly)
FORM_LABELS = {
    "10-K":     "Annual Report",
    "10-K/A":   "Annual Report (Amended)",
    "10-Q":     "Quarterly Report",
    "10-Q/A":   "Quarterly Report (Amended)",
    "8-K":      "Current Report",
    "8-K/A":    "Current Report (Amended)",
    "20-F":     "Annual Report (FPI)",
    "20-F/A":   "Annual Report (FPI, Amended)",
    "6-K":      "Foreign Issuer Current Report",
    "6-K/A":    "Foreign Issuer Current Report (Amended)",
    "DEF 14A":  "Definitive Proxy Statement",
    "DEFA14A":  "Additional Proxy Materials",
    "S-1":      "IPO Registration",
    "S-1/A":    "IPO Registration (Amended)",
    "F-1":      "FPI IPO Registration",
    "F-1/A":    "FPI IPO Registration (Amended)",
    "424B4":    "Final Prospectus",
}


def ingest_filing(session: requests.Session, coll, filing: dict, *,
                  download_pdf: bool = True) -> str:
    """One filing → one Mongo doc. Returns 'added' / 'updated' / 'skipped' /
    'error'."""
    ticker = filing["_ticker"]
    cik_padded = filing["_cik"]
    accession = filing["accession"]
    form = filing["form"]
    primary_doc = filing["primary_doc"]
    items = filing["items"]
    release_ms = filing["release_time_ms"]

    # Skip if already present and the PDF / primary file is on disk
    existing = coll.find_one({"_id": accession}, {"pdf_local_path": 1})
    if existing and existing.get("pdf_local_path"):
        p = Path(existing["pdf_local_path"])
        if p.exists() and p.stat().st_size > 0:
            return "skipped"

    # Build title — primary_doc_desc is usually generic ("10-Q"); enrich with form + period
    period = filing["report_date"] or ""
    title_pieces = [ticker.name_en, form]
    if period:
        title_pieces.append(period)
    if items:
        title_pieces.append(f"items={items}")
    title = " | ".join(title_pieces)

    # Download primary doc
    local_path = ""
    rel_path = ""
    size = 0
    err = ""
    unavailable = False
    if download_pdf and primary_doc:
        path, size, err = download_primary(session, cik_padded, accession, primary_doc)
        if size > 0:
            local_path = str(path)
            try:
                rel_path = str(path.relative_to(SEC_PDF_ROOT))
            except ValueError:
                rel_path = str(path)
        else:
            unavailable = True

    # Optional: enumerate attachments (don't download). Keep it cheap — one
    # FilingSummary call per filing is fine within rate limit.
    attachments: list[dict] = []
    try:
        attachments = fetch_filing_summary_attachments(session, cik_padded, accession)
    except Exception as e:
        logger.debug("attachments enumerate failed for %s: %s", accession, e)

    fp = _fiscal_period_for(form, period)
    fy = _fiscal_year_for(period)

    # Strip the runtime-only keys before persisting (`_ticker` is an IrTicker
    # dataclass instance, `_cik` is duplicated in the doc body, `release_time_ms`
    # already gets canonicalized by make_filing_doc).
    list_item = {k: v for k, v in filing.items()
                 if k not in {"_ticker", "_cik", "release_time_ms"}}

    doc = make_filing_doc(
        doc_id=accession,
        source=SOURCE,
        category=form,
        category_name=FORM_LABELS.get(form, form),
        title=title,
        release_time_ms=release_ms,
        organization=ticker.name_en,
        ticker_local=ticker.cik,
        ticker_canonical=ticker.canonical,
        list_item=list_item,
        period_start=None,
        period_end=period or None,
        fiscal_year=fy,
        fiscal_period=fp,
        lang="en",
        doc_introduce=filing["primary_doc_desc"] or "",
        content_md="",                   # filled by extract_pdf_texts.py later (pdf_text_md)
        pdf_rel_path=rel_path,
        pdf_local_path=local_path,
        pdf_size_bytes=size,
        pdf_download_error=err,
        pdf_unavailable=unavailable,
        attachments=attachments,
        web_url=web_url(cik_padded, accession),
        extra={
            "cik_padded":             cik_padded,
            "accession":              accession,
            "form":                   form,
            "items":                  items,
            "is_inline_xbrl":         filing["is_inline_xbrl"],
            "is_xbrl_numeric":        filing["is_xbrl_numeric"],
            "primary_doc_filename":   primary_doc,
            "primary_doc_description": filing["primary_doc_desc"],
            "filing_date":            filing["filing_date"],
            "report_date":            filing["report_date"],
            "acceptance_dt_utc":      filing["acceptance_dt_utc"],
            "size_bytes_reported":    filing["size_bytes"],
            "filing_index_url":       filing_index_url(cik_padded, accession),
            "primary_doc_url":        doc_url(cik_padded, accession, primary_doc),
        },
    )
    upsert_filing(coll, doc)
    return "updated" if existing else "added"


def _fiscal_period_for(form: str, report_date: str) -> Optional[str]:
    """Heuristic period label. Annual filings → FY; quarterly → Q1/Q2/Q3/Q4
    based on report_date month (calendar, not company fiscal — a known
    approximation, refined later by XBRL `frame`)."""
    if form.startswith("10-K") or form.startswith("20-F"):
        return "FY"
    if not report_date:
        return None
    if form.startswith("10-Q"):
        try:
            month = int(report_date[5:7])
            return {1:"Q1", 2:"Q1", 3:"Q1",
                    4:"Q2", 5:"Q2", 6:"Q2",
                    7:"Q3", 8:"Q3", 9:"Q3",
                    10:"Q4", 11:"Q4", 12:"Q4"}.get(month)
        except (IndexError, ValueError):
            return None
    return None


def _fiscal_year_for(report_date: str) -> Optional[int]:
    if not report_date:
        return None
    try:
        return int(report_date[:4])
    except (IndexError, ValueError):
        return None


# ============================================================
# XBRL companyfacts → Mongo (sec_xbrl_facts)
# ============================================================

def fetch_companyfacts(session: requests.Session, cik_padded: str) -> Optional[dict]:
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik_padded}.json"
    try:
        return get(session, url).json()
    except Exception as e:
        # Not every issuer has companyfacts (no XBRL filings yet — newly-IPO'd)
        logger.info("companyfacts unavailable for %s: %s", cik_padded, e)
        return None


# Tags considered for the segment-revenue model. Both us-gaap and ifrs-full
# (TSM, NOK, NBIS, SGML are FPIs filing in IFRS).
_REVENUE_TAGS = {
    "us-gaap": [
        "Revenues",
        "RevenueFromContractWithCustomerExcludingAssessedTax",
        "RevenueFromContractWithCustomerIncludingAssessedTax",
        "SalesRevenueNet",
        "SalesRevenueGoodsNet",
        "SalesRevenueServicesNet",
        "SegmentReportingInformationRevenueFromExternalCustomers",
        "EntityWideDisclosureOnGeographicAreasRevenueFromExternalCustomersAttributedToEntitysCountryOfDomicile",
        "EntityWideDisclosureOnGeographicAreasRevenueFromExternalCustomersAttributedToForeignCountries",
    ],
    "ifrs-full": [
        "Revenue",
        "RevenueFromContractsWithCustomers",
    ],
    "dei": [
        "EntityCommonStockSharesOutstanding",
    ],
}


def ingest_xbrl_facts(facts_db, ticker: IrTicker, companyfacts: dict, *,
                      tags: Optional[dict] = None) -> int:
    """Denormalize companyfacts → one row per (concept, frame) into
    ir_filings.sec_xbrl_facts. Returns the number of rows upserted.

    Stored as a separate collection (not nested on the filing) because:
      - the same fact is reported in 10-Q AND amendments AND restatements
      - downstream revenue-model queries are mostly time-series across companies
        (frames API style), and a flat table answers them with one Mongo find()
    """
    cik_padded = ticker.cik
    facts = (companyfacts or {}).get("facts", {})
    tags = tags or _REVENUE_TAGS

    bulk: list[Any] = []
    n = 0
    for taxonomy, want_tags in tags.items():
        bag = facts.get(taxonomy, {})
        for tag in want_tags:
            entry = bag.get(tag)
            if not entry:
                continue
            label = entry.get("label", "")
            for unit, points in (entry.get("units") or {}).items():
                for p in points:
                    accn = p.get("accn", "")
                    frame = p.get("frame", "")
                    end = p.get("end", "")
                    # Stable composite key — same fact across re-filings stays one row
                    key = f"{cik_padded}_{taxonomy}_{tag}_{frame or end}_{accn}"
                    row = {
                        "_id":                key,
                        "cik":                cik_padded,
                        "ticker_canonical":   ticker.canonical,
                        "ticker_local":       ticker.listing_code,
                        "organization":       ticker.name_en,
                        "taxonomy":           taxonomy,
                        "tag":                tag,
                        "label":              label,
                        "frame":              frame,
                        "fy":                 p.get("fy"),
                        "fp":                 p.get("fp"),
                        "form":               p.get("form"),
                        "accn":               accn,
                        "filed":              p.get("filed"),
                        "start":              p.get("start"),
                        "end":                end,
                        "val":                p.get("val"),
                        "unit":               unit,
                        "_canonical_tickers": [ticker.canonical],
                        "ingested_at":        datetime.now(timezone.utc),
                    }
                    facts_db.replace_one({"_id": key}, row, upsert=True)
                    n += 1
    return n


# ============================================================
# Driver
# ============================================================

def crawl_one_ticker(session: requests.Session, ticker: IrTicker, *,
                     limit: Optional[int] = None,
                     since_ms: Optional[int] = None,
                     download_pdf: bool = True,
                     include_all_8k: bool = False,
                     skip_xbrl: bool = False,
                     forms: Optional[set] = None) -> dict[str, int]:
    """Pull filings + XBRL for one ticker. Returns counter dict for logging."""
    coll = get_collection(SOURCE)
    facts_coll = get_db()[COLL_FACTS]
    counters = {"added": 0, "updated": 0, "skipped": 0, "errors": 0, "facts": 0}

    logger.info("[%s] fetching submissions (CIK %s)", ticker.canonical, ticker.cik)
    try:
        sub = fetch_submissions(session, ticker.cik)
    except Exception as e:
        logger.error("[%s] submissions fetch failed: %s", ticker.canonical, e)
        counters["errors"] += 1
        return counters

    n = 0
    for filing in iter_filings(sub, ticker, forms=forms,
                               since_ms=since_ms,
                               include_all_8k=include_all_8k):
        if limit and n >= limit:
            break
        try:
            status = ingest_filing(session, coll, filing, download_pdf=download_pdf)
            counters[status] = counters.get(status, 0) + 1
        except Exception as e:
            logger.error("[%s] ingest %s failed: %s", ticker.canonical,
                         filing.get("accession"), e)
            counters["errors"] += 1
        # Increment regardless of outcome — otherwise --max is ignored when
        # everything errors out (we'd loop the entire submissions list).
        n += 1

    save_state(SOURCE, bucket=ticker.canonical,
               last_run_at=datetime.now(timezone.utc),
               filings_seen=n)

    if not skip_xbrl:
        cf = fetch_companyfacts(session, ticker.cik)
        if cf:
            try:
                added = ingest_xbrl_facts(facts_coll, ticker, cf)
                counters["facts"] = added
                logger.info("[%s] XBRL facts ingested: %s rows", ticker.canonical, added)
            except Exception as e:
                logger.error("[%s] XBRL ingest failed: %s", ticker.canonical, e)
                counters["errors"] += 1

    record_daily_stat(SOURCE, ticker.canonical,
                      added=counters["added"], skipped=counters["skipped"],
                      errors=counters["errors"], pdfs=counters["added"])
    return counters


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--ticker", action="append",
                   help="canonical (e.g. INTC.US) or listing code (INTC). Repeatable. Default: all 22.")
    p.add_argument("--max", type=int, default=None,
                   help="Max filings per ticker per run (default: unlimited).")
    p.add_argument("--limit", type=int, default=None, help="alias for --max")
    p.add_argument("--since-hours", type=int, default=None,
                   help="Only filings filed within last N hours.")
    p.add_argument("--no-pdf", action="store_true", help="Skip primary-doc download.")
    p.add_argument("--all-8k", action="store_true",
                   help="Include all 8-K items, not just earnings (Item 2.02).")
    p.add_argument("--xbrl-only", action="store_true",
                   help="Skip filings index — refresh companyfacts only.")
    p.add_argument("--skip-xbrl", action="store_true",
                   help="Skip companyfacts refresh.")
    p.add_argument("--show-state", action="store_true",
                   help="Print per-ticker checkpoints + counts and exit.")
    p.add_argument("--watch", action="store_true",
                   help="Loop forever, sleeping --interval between rounds.")
    p.add_argument("--interval", type=int, default=900,
                   help="Watch loop interval (seconds). Default 900 = 15min.")
    args = p.parse_args(sys.argv[1:] if len(sys.argv) > 1 else [])

    ensure_indexes(SOURCE)

    tickers = US_TICKERS
    if args.ticker:
        wanted = {t.upper() for t in args.ticker}
        tickers = [t for t in US_TICKERS
                   if t.canonical in wanted or t.listing_code in wanted]
        missing = wanted - {t.canonical for t in tickers} - {t.listing_code for t in tickers}
        if missing:
            print(f"WARN: unknown tickers ignored: {missing}", file=sys.stderr)

    if args.show_state:
        coll = get_collection(SOURCE)
        facts = get_db()[COLL_FACTS]
        for t in tickers:
            st = load_state(SOURCE, bucket=t.canonical)
            n_filings = coll.count_documents({"ticker_canonical": t.canonical})
            n_facts = facts.count_documents({"ticker_canonical": t.canonical})
            print(f"  {t.canonical:>10} ({t.cik}) — filings={n_filings:>4}  facts={n_facts:>5}  "
                  f"last_run={st.get('last_run_at') or '—'}")
        return

    sess = make_session()
    limit = args.limit or args.max
    since_ms = None
    if args.since_hours:
        since_ms = int((datetime.now(timezone.utc)
                        - timedelta(hours=args.since_hours)).timestamp() * 1000)

    while True:
        round_start = datetime.now(timezone.utc)
        totals = {"added": 0, "updated": 0, "skipped": 0, "errors": 0, "facts": 0}
        for t in tickers:
            if args.xbrl_only:
                cf = fetch_companyfacts(sess, t.cik)
                if cf:
                    n = ingest_xbrl_facts(get_db()[COLL_FACTS], t, cf)
                    totals["facts"] += n
                    logger.info("[%s] XBRL-only: %s facts", t.canonical, n)
                continue
            c = crawl_one_ticker(
                sess, t,
                limit=limit, since_ms=since_ms,
                download_pdf=not args.no_pdf,
                include_all_8k=args.all_8k,
                skip_xbrl=args.skip_xbrl,
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
