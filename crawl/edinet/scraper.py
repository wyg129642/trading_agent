#!/usr/bin/env python3
"""EDINET (金融庁) scraper — pulls Japanese statutory filings (有価証券報告書 /
半期報告書 / 大量保有報告書 etc) for the 2 in-scope JP holdings into Mongo
``ir_filings.edinet``.

Why
---
EDINET is the official 金融庁 disclosure system; it ships every annual /
semi-annual filing as PDF + XBRL + a pre-converted CSV (``type=5``, UTF-16 BOM
TSV) that exposes ``SegmentInformationOfFinancialDataTextBlock`` — the segment
revenue note ready to ingest. CSV avoids writing a full XBRL parser.

Critical post-2024 change: the 四半期報告書 (quarterly report, code 130) was
**abolished 2024-07-01**. From Q1 FY2024 forward only the new 半期報告書
(code 160, form 043A00) is filed to EDINET; quarterly numbers come ONLY via
TDnet 決算短信 (the sibling tdnet/scraper.py).

Auth
----
v2 requires a free Subscription-Key (manual registration with phone MFA).
Drop into ``crawl/edinet/credentials.json`` as ``{"subscription_key": "..."}``
or set env ``EDINET_SUBSCRIPTION_KEY``. Pass as **query string**, not header.

CLI
---
  python3 scraper.py --show-state
  python3 scraper.py --days 30 --max 10
  python3 scraper.py --ticker 5801 --days 365
  python3 scraper.py --watch --interval 7200    # 2h poll loop

Doc shape additions:
  doc_id (EDINET docID), edinet_code, sec_code, ordinance_code, form_code,
  doc_type_code, withdrawal_status, has_xbrl, has_csv, has_english,
  xbrl_zip_path, csv_zip_path, segment_info_text (extracted)
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import os
import sys
import time
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
    PDF_ROOT, ensure_indexes, get_collection, get_db,
    load_state, make_filing_doc, pdf_dir, record_daily_stat,
    safe_filename, save_state, setup_logging, upsert_filing,
)
from crawl.ir_filings.tickers import JP_TICKERS, IrTicker  # noqa: E402

SOURCE = "edinet"
EDINET_PDF_ROOT = pdf_dir(SOURCE)

API_BASE = "https://api.edinet-fsa.go.jp/api/v2"
CREDS_FILE = _HERE / "credentials.json"

# Throttle: spec doesn't publish a rate limit; community wisdom is 1 req/s safe.
THROTTLE_S = float(os.environ.get("EDINET_THROTTLE", "1.0"))

# docTypeCode → human label + revenue-modeling priority
DOC_TYPE_LABELS = {
    "010": "Securities Notification",
    "020": "Securities Registration",
    "030": "Amended Securities Registration",
    "120": "Annual Securities Report (有報)",          # ★★★★★
    "130": "Quarterly Report (Q, abolished 2024-07)",  # legacy backfill only
    "135": "Management Confirmation",
    "140": "Semi-Annual Report (legacy)",              # ★★★★
    "150": "Amended Annual Securities Report",
    "160": "Semi-Annual Report (new, post-2024-07)",  # ★★★★★
    "170": "Amended Semi-Annual Report",
    "180": "Parent Company Report",
    "220": "Buyback Report",
    "230": "Tender Offer (TOB)",
    "240": "Amended TOB",
    "250": "TOB Result Report",
    "260": "Opinion on TOB",
    "350": "Large Shareholder Report (5%+)",
    "360": "Amended Large Shareholder Report",
}

# Default ingest list (rev-model priority)
WANTED_DOC_TYPES = {"120", "130", "140", "150", "160", "170", "180", "350"}

logger = setup_logging(SOURCE)

# ============================================================
# Credentials
# ============================================================

def load_subscription_key() -> str:
    key = os.environ.get("EDINET_SUBSCRIPTION_KEY", "").strip()
    if key:
        return key
    if CREDS_FILE.exists():
        try:
            data = json.loads(CREDS_FILE.read_text(encoding="utf-8"))
            return (data.get("subscription_key") or "").strip()
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
        "Accept": "application/json",
    })
    return s


def http_get(s: requests.Session, url: str, *, params: dict,
             timeout: int = 60) -> requests.Response:
    """EDINET v2: pass key via query string. HTTP 200 even on errors — caller
    must inspect Content-Type."""
    r = s.get(url, params=params, timeout=timeout)
    time.sleep(THROTTLE_S)
    r.raise_for_status()
    return r


# ============================================================
# Document list (one date at a time)
# ============================================================

def list_filings_for_date(s: requests.Session, key: str, d: date) -> list[dict]:
    url = f"{API_BASE}/documents.json"
    params = {"date": d.isoformat(), "type": 2, "Subscription-Key": key}
    try:
        r = http_get(s, url, params=params)
    except Exception as e:
        logger.error("documents.json %s failed: %s", d, e)
        return []
    if not r.headers.get("Content-Type", "").startswith("application/json"):
        logger.error("documents.json %s returned non-JSON: %s", d, r.text[:200])
        return []
    payload = r.json()
    meta = payload.get("metadata", {})
    if str(meta.get("status")) not in ("200", "404"):
        logger.error("documents.json %s status=%s msg=%s",
                     d, meta.get("status"), meta.get("message"))
        return []
    return payload.get("results") or []


def fetch_doc(s: requests.Session, key: str, doc_id: str, *,
              type_code: int) -> tuple[bytes, str]:
    """Returns (body_bytes, content_type). Caller decides what to do based on
    content_type (application/pdf / application/octet-stream / json error)."""
    url = f"{API_BASE}/documents/{doc_id}"
    params = {"type": type_code, "Subscription-Key": key}
    r = http_get(s, url, params=params, timeout=180)
    return r.content, r.headers.get("Content-Type", "")


# ============================================================
# CSV → segment text extraction
# ============================================================

def extract_segment_text(zip_bytes: bytes) -> Optional[str]:
    """Walk XBRL_TO_CSV/*.csv (UTF-16 BOM TSV) for the
    SegmentInformationOfFinancialDataTextBlock cell."""
    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            for name in zf.namelist():
                if not name.startswith("XBRL_TO_CSV/") or not name.endswith(".csv"):
                    continue
                try:
                    raw = zf.read(name).decode("utf-16")
                except UnicodeDecodeError:
                    raw = zf.read(name).decode("utf-8", errors="ignore")
                reader = csv.DictReader(io.StringIO(raw), delimiter="\t")
                for row in reader:
                    elem = (row.get("要素ID") or "")
                    if "SegmentInformationOfFinancialDataTextBlock" in elem:
                        val = row.get("値") or ""
                        if val:
                            return val
    except zipfile.BadZipFile:
        return None
    except Exception as e:
        logger.warning("extract_segment_text failed: %s", e)
    return None


# ============================================================
# Filing → Mongo
# ============================================================

def _parse_submit(s: str) -> int:
    """`2026-04-25 09:30` JST → UTC ms."""
    try:
        dt_local = datetime.strptime(s.strip(), "%Y-%m-%d %H:%M")
        from datetime import timezone as tz, timedelta as td
        dt_utc = dt_local.replace(tzinfo=tz(td(hours=9))).astimezone(tz.utc)
        return int(dt_utc.timestamp() * 1000)
    except Exception:
        return 0


def ingest_one(s: requests.Session, key: str, coll, ticker: IrTicker,
               filing: dict, *, download_pdf: bool = True) -> str:
    doc_id = filing.get("docID") or ""
    if not doc_id:
        return "skipped"

    existing = coll.find_one({"_id": doc_id}, {"pdf_local_path": 1})
    if existing and existing.get("pdf_local_path"):
        if Path(existing["pdf_local_path"]).exists():
            return "skipped"

    submit_dt = filing.get("submitDateTime") or ""
    release_ms = _parse_submit(submit_dt)
    period_start = filing.get("periodStart") or None
    period_end = filing.get("periodEnd") or None
    fiscal_year = None
    fiscal_period = None
    if period_end:
        try:
            fiscal_year = int(period_end[:4])
        except ValueError:
            pass

    doc_type = str(filing.get("docTypeCode") or "")
    category_name = DOC_TYPE_LABELS.get(doc_type, filing.get("docDescription") or doc_type)
    title = filing.get("docDescription") or category_name

    # Storage paths
    ym = (period_end or filing.get("submitDateTime", "")[:7] or "unknown")[:7] or "unknown"
    base_dir = EDINET_PDF_ROOT / ticker.sec_code / ym
    base_dir.mkdir(parents=True, exist_ok=True)

    pdf_local = ""
    pdf_size = 0
    pdf_err = ""
    pdf_unavailable = False

    csv_local = ""
    csv_size = 0
    segment_text = ""

    if download_pdf:
        # PDF (type=2) — only if pdfFlag=1
        if str(filing.get("pdfFlag")) == "1":
            try:
                body, ctype = fetch_doc(s, key, doc_id, type_code=2)
                if ctype.startswith("application/json"):
                    pdf_err = body.decode("utf-8", errors="ignore")[:200]
                    pdf_unavailable = True
                elif ctype.startswith("application/pdf"):
                    out = base_dir / f"{doc_id}.pdf"
                    out.write_bytes(body)
                    pdf_local = str(out)
                    pdf_size = len(body)
            except Exception as e:
                pdf_err = str(e)[:300]

        # CSV (type=5) — for segment extraction; only if csvFlag=1
        if str(filing.get("csvFlag")) == "1":
            try:
                body, ctype = fetch_doc(s, key, doc_id, type_code=5)
                if ctype.startswith("application/json"):
                    logger.warning("csv fetch error %s: %s",
                                   doc_id, body.decode("utf-8", errors="ignore")[:120])
                else:
                    out = base_dir / f"{doc_id}_csv.zip"
                    out.write_bytes(body)
                    csv_local = str(out)
                    csv_size = len(body)
                    seg = extract_segment_text(body)
                    if seg:
                        segment_text = seg
                        (base_dir / f"{doc_id}_segment.html").write_text(
                            seg, encoding="utf-8",
                        )
            except Exception as e:
                logger.warning("csv fetch failed %s: %s", doc_id, e)

    rel_path = ""
    if pdf_local:
        try:
            rel_path = str(Path(pdf_local).relative_to(EDINET_PDF_ROOT))
        except ValueError:
            rel_path = pdf_local

    web_url = (f"https://disclosure2.edinet-fsa.go.jp/WEEK0010.aspx?"
               f"DocID={doc_id}")

    extra = {
        "doc_id":             doc_id,
        "edinet_code":        filing.get("edinetCode"),
        "sec_code":           filing.get("secCode"),
        "fund_code":          filing.get("fundCode"),
        "jcn":                filing.get("JCN"),
        "ordinance_code":     filing.get("ordinanceCode"),
        "form_code":          filing.get("formCode"),
        "doc_type_code":      doc_type,
        "withdrawal_status":  filing.get("withdrawalStatus"),
        "doc_info_edit_status": filing.get("docInfoEditStatus"),
        "disclosure_status":  filing.get("disclosureStatus"),
        "has_pdf":            str(filing.get("pdfFlag")) == "1",
        "has_xbrl":           str(filing.get("xbrlFlag")) == "1",
        "has_attach":         str(filing.get("attachDocFlag")) == "1",
        "has_english":        str(filing.get("englishDocFlag")) == "1",
        "has_csv":            str(filing.get("csvFlag")) == "1",
        "legal_status":       filing.get("legalStatus"),
        "filer_name":         filing.get("filerName"),
        "issuer_edinet_code": filing.get("issuerEdinetCode"),
        "subject_edinet_code": filing.get("subjectEdinetCode"),
        "subsidiary_edinet_code": filing.get("subsidiaryEdinetCode"),
        "current_report_reason": filing.get("currentReportReason"),
        "parent_doc_id":      filing.get("parentDocID"),
        "submit_dt_jst":      submit_dt,
        "csv_local_path":     csv_local,
        "csv_size_bytes":     csv_size,
        "segment_info_text":  segment_text,
    }

    doc = make_filing_doc(
        doc_id=doc_id,
        source=SOURCE,
        category=doc_type,
        category_name=category_name,
        title=title,
        title_local=filing.get("docDescription") or "",
        release_time_ms=release_ms,
        organization=filing.get("filerName") or ticker.name_local or ticker.name_en,
        ticker_local=filing.get("secCode") or ticker.sec_code,
        ticker_canonical=ticker.canonical,
        period_start=period_start,
        period_end=period_end,
        fiscal_year=fiscal_year,
        fiscal_period=fiscal_period,
        lang="ja",
        doc_introduce=filing.get("docDescription") or "",
        content_md=segment_text or "",         # if segment text extracted, surface it directly
        pdf_rel_path=rel_path,
        pdf_local_path=pdf_local,
        pdf_size_bytes=pdf_size,
        pdf_download_error=pdf_err,
        pdf_unavailable=pdf_unavailable,
        xbrl_data_path=csv_local,
        xbrl_summary={"segment_info_html": segment_text} if segment_text else {},
        web_url=web_url,
        list_item=filing,
        extra=extra,
    )
    upsert_filing(coll, doc)
    return "updated" if existing else "added"


# ============================================================
# Driver
# ============================================================

def crawl(s: requests.Session, key: str, *, days: int,
          tickers: list[IrTicker], limit: Optional[int] = None,
          download_pdf: bool = True,
          all_doc_types: bool = False) -> dict[str, int]:
    """EDINET has no per-issuer endpoint — walk dates, filter client-side by
    secCode."""
    coll = get_collection(SOURCE)
    counters = {"added": 0, "updated": 0, "skipped": 0, "errors": 0}
    sec_codes = {t.sec_code: t for t in tickers if t.sec_code}
    edinet_codes = {t.edinet_code: t for t in tickers if t.edinet_code}

    today = date.today()
    n_per_ticker: dict[str, int] = {t.canonical: 0 for t in tickers}

    for offset in range(days):
        d = today - timedelta(days=offset)
        if d.weekday() >= 5:                     # Sat/Sun — TSE closed
            continue
        results = list_filings_for_date(s, key, d)
        if not results:
            continue
        for filing in results:
            sec = str(filing.get("secCode") or "")
            ec = str(filing.get("edinetCode") or "")
            ticker = sec_codes.get(sec) or edinet_codes.get(ec)
            if not ticker:
                continue
            doc_type = str(filing.get("docTypeCode") or "")
            if not all_doc_types and doc_type not in WANTED_DOC_TYPES:
                continue
            if limit and n_per_ticker[ticker.canonical] >= limit:
                continue
            try:
                status = ingest_one(s, key, coll, ticker, filing,
                                    download_pdf=download_pdf)
                counters[status] = counters.get(status, 0) + 1
                n_per_ticker[ticker.canonical] += 1
            except Exception as e:
                logger.error("ingest %s (%s) failed: %s",
                             filing.get("docID"), ticker.canonical, e)
                counters["errors"] += 1

    for t in tickers:
        save_state(SOURCE, bucket=t.canonical,
                   last_run_at=datetime.now(timezone.utc),
                   filings_seen=n_per_ticker.get(t.canonical, 0))
        record_daily_stat(SOURCE, t.canonical,
                          added=counters["added"], skipped=counters["skipped"],
                          errors=counters["errors"], pdfs=counters["added"])
    return counters


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--ticker", action="append",
                   help="Ticker (5801 or 5801.JP). Repeatable.")
    p.add_argument("--days", type=int, default=30,
                   help="Days back to walk (default 30; for full backfill use 3650).")
    p.add_argument("--max", type=int, default=None,
                   help="Max filings per ticker per run.")
    p.add_argument("--limit", type=int, default=None, help="alias for --max")
    p.add_argument("--no-pdf", action="store_true")
    p.add_argument("--all-doc-types", action="store_true",
                   help="Don't filter by WANTED_DOC_TYPES — pull all.")
    p.add_argument("--show-state", action="store_true")
    p.add_argument("--watch", action="store_true")
    p.add_argument("--interval", type=int, default=7200, help="Watch interval (default 2h).")
    args = p.parse_args(sys.argv[1:] if len(sys.argv) > 1 else [])

    ensure_indexes(SOURCE)
    tickers = JP_TICKERS
    if args.ticker:
        wanted = {t.upper() for t in args.ticker}
        tickers = [t for t in JP_TICKERS
                   if t.canonical.upper() in wanted or t.listing_code.upper() in wanted]

    if args.show_state:
        coll = get_collection(SOURCE)
        for t in tickers:
            st = load_state(SOURCE, bucket=t.canonical)
            n = coll.count_documents({"ticker_canonical": t.canonical})
            print(f"  {t.canonical:>10} {t.sec_code} {t.name_en:<40} "
                  f"filings={n:>4}  last_run={st.get('last_run_at') or '—'}")
        return

    key = load_subscription_key()
    if not key:
        print("ERROR: no EDINET subscription key. Set EDINET_SUBSCRIPTION_KEY env "
              "or write to crawl/edinet/credentials.json: {\"subscription_key\":\"...\"}",
              file=sys.stderr)
        sys.exit(2)

    sess = make_session()
    limit = args.limit or args.max

    while True:
        round_start = datetime.now(timezone.utc)
        c = crawl(sess, key, days=args.days, tickers=tickers, limit=limit,
                  download_pdf=not args.no_pdf, all_doc_types=args.all_doc_types)
        elapsed = (datetime.now(timezone.utc) - round_start).total_seconds()
        logger.info("ROUND DONE in %.1fs — %s", elapsed, c)
        if not args.watch:
            break
        sleep_s = max(60, args.interval - int(elapsed))
        logger.info("Watch loop: sleeping %ss", sleep_s)
        time.sleep(sleep_s)


if __name__ == "__main__":
    main()
