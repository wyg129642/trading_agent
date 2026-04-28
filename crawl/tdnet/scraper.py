#!/usr/bin/env python3
"""TDnet scraper — pulls Japanese timely disclosures (適時開示) including
決算短信 (earnings flash) for the 2 in-scope JP holdings via the Yanoshin
mirror, into Mongo ``ir_filings.tdnet``.

Why TDnet (not just EDINET)
---------------------------
- TDnet hosts 決算短信 (earnings flash within 45d of QE) which are the **only**
  quarterly source post-2024-07 (when 四半期報告書 was abolished from EDINET).
- TDnet also has ad-hoc disclosures (M&A, capex, guidance revision) that
  don't appear in EDINET until the next 半期報告書.
- Yanoshin mirror works around two TDnet pain points: 31-day retention on the
  primary site + Akamai UA-gating on PDFs.

Source
------
``https://webapi.yanoshin.jp/webapi/tdnet/list/{TICKER}.json?limit=N`` returns
JSON with `Tdnet.id` (Yanoshin's stable monotonic int) + `document_url` (a
``rd.php?...`` redirect that bypasses TDnet's UA gate). No auth.

CLI
---
  python3 scraper.py --show-state
  python3 scraper.py --ticker 5801 --limit 100
  python3 scraper.py --watch --interval 600    # 10min poll loop during JST trading
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

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
from crawl.ir_filings.tickers import JP_TICKERS, IrTicker  # noqa: E402

SOURCE = "tdnet"
TDNET_PDF_ROOT = pdf_dir(SOURCE)

YANOSHIN_BASE = "https://webapi.yanoshin.jp/webapi/tdnet/list"

THROTTLE_S = float(os.environ.get("TDNET_THROTTLE", "1.0"))

logger = setup_logging(SOURCE)


# Title classifier — TDnet doesn't have category codes; we infer from title.
# Pattern → (category, label, priority for revenue modeling)
TITLE_PATTERNS = [
    # earnings flashes
    (re.compile(r"決算短信"),                   ("kessan_tanshin", "決算短信 Earnings Flash")),
    (re.compile(r"業績予想.*修正"),              ("guidance_rev", "業績予想の修正 Guidance Revision")),
    (re.compile(r"配当予想.*修正"),              ("dividend_rev", "配当予想の修正 Dividend Revision")),
    (re.compile(r"(中間|半期).*業績"),            ("interim_results", "中間/半期業績")),
    (re.compile(r"通期.*業績"),                  ("annual_results", "通期業績")),
    # corporate actions
    (re.compile(r"(資本業務提携|株式取得|買収)"), ("ma", "M&A / Acquisition")),
    (re.compile(r"設備投資"),                   ("capex", "Capex")),
    (re.compile(r"(株式分割|自己株式)"),         ("equity_action", "Equity Action")),
    # default
]


def make_session() -> requests.Session:
    s = requests.Session()
    s.trust_env = False
    s.proxies = {}
    s.headers.update({"User-Agent": "trading-intel-research"})
    return s


def http_get(s: requests.Session, url: str, **kw) -> requests.Response:
    r = s.get(url, timeout=60, **kw)
    time.sleep(THROTTLE_S)
    r.raise_for_status()
    return r


def list_for_ticker(s: requests.Session, ticker_code: str, limit: int = 300,
                    has_xbrl: bool = False) -> list[dict]:
    params = {"limit": str(limit)}
    if has_xbrl:
        params["hasXBRL"] = "1"
    url = f"{YANOSHIN_BASE}/{ticker_code}.json"
    r = http_get(s, url, params=params)
    payload = r.json()
    items = payload.get("items") or []
    out = []
    for it in items:
        td = it.get("Tdnet") or {}
        if td:
            out.append(td)
    return out


def _parse_pubdate(s: str) -> int:
    """`2026-03-30 15:30:00` JST → UTC ms."""
    try:
        dt_local = datetime.strptime(s.strip(), "%Y-%m-%d %H:%M:%S")
        from datetime import timezone as tz, timedelta as td
        dt_utc = dt_local.replace(tzinfo=tz(td(hours=9))).astimezone(tz.utc)
        return int(dt_utc.timestamp() * 1000)
    except Exception:
        return 0


def classify_title(title: str) -> tuple[str, str]:
    for pat, (cat, label) in TITLE_PATTERNS:
        if pat.search(title or ""):
            return cat, label
    return "other", "Other"


def download_doc(s: requests.Session, *, ticker_code: str, td_id: str,
                 url: str, kind: str) -> tuple[Path, int, str]:
    """Download via Yanoshin's `rd.php?<actual url>` redirect (bypasses TDnet
    UA gate). `kind` ∈ {pdf, xbrl}. Returns (path, size, error)."""
    ext = ".zip" if kind == "xbrl" else ".pdf"
    ym = datetime.now(timezone.utc).strftime("%Y-%m")
    out_dir = TDNET_PDF_ROOT / ticker_code / ym
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{td_id}{('_xbrl' if kind=='xbrl' else '')}{ext}"
    if out_path.exists() and out_path.stat().st_size > 0:
        return out_path, out_path.stat().st_size, ""
    try:
        r = http_get(s, url)
        out_path.write_bytes(r.content)
        return out_path, len(r.content), ""
    except Exception as e:
        return out_path, 0, str(e)[:300]


def ingest_one(s: requests.Session, coll, ticker: IrTicker, td: dict, *,
               download_pdf: bool = True) -> str:
    td_id = str(td.get("id") or "")
    if not td_id:
        return "skipped"

    existing = coll.find_one({"_id": td_id}, {"pdf_local_path": 1})
    if existing and existing.get("pdf_local_path"):
        if Path(existing["pdf_local_path"]).exists():
            return "skipped"

    title = (td.get("title") or "").strip()
    pubdate = td.get("pubdate") or ""
    release_ms = _parse_pubdate(pubdate)
    category, category_name = classify_title(title)

    pdf_local = ""
    pdf_size = 0
    pdf_err = ""
    pdf_unavailable = False
    xbrl_local = ""

    if download_pdf:
        url = td.get("document_url") or ""
        if url:
            path, size, err = download_doc(
                s, ticker_code=ticker.listing_code, td_id=td_id,
                url=url, kind="pdf",
            )
            if size > 0:
                pdf_local = str(path)
                pdf_size = size
            else:
                pdf_err = err
                pdf_unavailable = True

        xbrl_url = td.get("url_xbrl") or ""
        if xbrl_url:
            path, size, err = download_doc(
                s, ticker_code=ticker.listing_code, td_id=td_id,
                url=xbrl_url, kind="xbrl",
            )
            if size > 0:
                xbrl_local = str(path)

    rel_path = ""
    if pdf_local:
        try:
            rel_path = str(Path(pdf_local).relative_to(TDNET_PDF_ROOT))
        except ValueError:
            rel_path = pdf_local

    extra = {
        "td_id":           td_id,
        "company_code":    td.get("company_code"),
        "company_name":    td.get("company_name"),
        "markets":         td.get("markets_string"),
        "pubdate_jst":     pubdate,
        "document_url":    td.get("document_url"),
        "url_report_type_summary": td.get("url_report_type_summary"),
        "url_report_type_fs_consolidated": td.get("url_report_type_fs_consolidated"),
        "url_xbrl":        td.get("url_xbrl"),
        "update_history":  td.get("update_history"),
        "xbrl_local_path": xbrl_local,
    }

    doc = make_filing_doc(
        doc_id=td_id,
        source=SOURCE,
        category=category,
        category_name=category_name,
        title=title,
        title_local=title,
        release_time_ms=release_ms,
        organization=td.get("company_name") or ticker.name_local or ticker.name_en,
        ticker_local=ticker.listing_code,
        ticker_canonical=ticker.canonical,
        lang="ja",
        doc_introduce=title,
        content_md="",
        pdf_rel_path=rel_path,
        pdf_local_path=pdf_local,
        pdf_size_bytes=pdf_size,
        pdf_download_error=pdf_err,
        pdf_unavailable=pdf_unavailable,
        xbrl_data_path=xbrl_local,
        web_url=f"https://www.release.tdnet.info/inbs/I_main_00.html",
        list_item=td,
        extra=extra,
    )
    upsert_filing(coll, doc)
    return "updated" if existing else "added"


def crawl_one_ticker(s: requests.Session, ticker: IrTicker, *,
                     limit: int, download_pdf: bool = True) -> dict[str, int]:
    coll = get_collection(SOURCE)
    counters = {"added": 0, "updated": 0, "skipped": 0, "errors": 0}
    try:
        items = list_for_ticker(s, ticker.listing_code, limit=limit)
    except Exception as e:
        logger.error("[%s] yanoshin list failed: %s", ticker.canonical, e)
        counters["errors"] += 1
        return counters
    for td in items:
        try:
            status = ingest_one(s, coll, ticker, td, download_pdf=download_pdf)
            counters[status] = counters.get(status, 0) + 1
        except Exception as e:
            logger.error("[%s] ingest %s failed: %s",
                         ticker.canonical, td.get("id"), e)
            counters["errors"] += 1
    save_state(SOURCE, bucket=ticker.canonical,
               last_run_at=datetime.now(timezone.utc),
               filings_seen=len(items))
    record_daily_stat(SOURCE, ticker.canonical,
                      added=counters["added"], skipped=counters["skipped"],
                      errors=counters["errors"], pdfs=counters["added"])
    return counters


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--ticker", action="append")
    p.add_argument("--limit", type=int, default=300,
                   help="Max items per ticker (Yanoshin caps ~300).")
    p.add_argument("--no-pdf", action="store_true")
    p.add_argument("--show-state", action="store_true")
    p.add_argument("--watch", action="store_true")
    p.add_argument("--interval", type=int, default=600,
                   help="Watch interval (default 10min — TDnet posts cluster at 13:30/15:00/16:00 JST).")
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
            print(f"  {t.canonical:>10} {t.listing_code} {t.name_en:<40} "
                  f"filings={n:>4}  last_run={st.get('last_run_at') or '—'}")
        return

    sess = make_session()
    while True:
        round_start = datetime.now(timezone.utc)
        totals = {"added": 0, "updated": 0, "skipped": 0, "errors": 0}
        for t in tickers:
            c = crawl_one_ticker(sess, t, limit=args.limit,
                                 download_pdf=not args.no_pdf)
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
