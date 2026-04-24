#!/usr/bin/env python3
"""
进门外资研报 bulk downloader
===========================

`json_oversea-research_preview` endpoint accepts arbitrary `researchId`
from 1 to ~1.67M and returns full metadata + `homeOssPdfUrl` **regardless
of paywall** (`isUnlock=0` items still get the OSS URL). That lets us
backfill every historical oversea report by brute-forcing the ID space —
without being limited by the list API's top-10k pagination cap.

Per id:
  1. POST json_oversea-research_preview?researchId=<id> via scraper.create_session
     (reuses all the auth+AES plumbing so we don't re-implement).
     - code=0  → valid report, has homeOssPdfUrl
     - code=500 "外资研报已失效" → gap (purged); skip
     - other   → error, retry next run
  2. Upsert metadata to `jinmen.oversea_reports` ( _id = id ). Shape matches
     scraper.dump_oversea_report so downstream enrich_tickers + frontend
     read it identically.
  3. GET homeOssPdfUrl → write to
     /home/ygwang/crawl_data/jinmen_pdfs/YYYY-MM/mndj_rtime_<N>.pdf

Safety:
  - Skip if DB already has doc + PDF (`--force` to re-download).
  - Progress JSON every 30s; `--resume` continues from last_scanned_id+1.
  - ThreadPoolExecutor with default 10 workers — preview is authed, burst
    risk higher than domestic mndj OSS. Raise concurrency at your own risk.

Usage::

    python3 download_oversea_pdfs.py --start 1 --end 1700000
    python3 download_oversea_pdfs.py --start 1 --end 5000 --concurrency 5   # small test
    python3 download_oversea_pdfs.py --start 1_000_000 --end 1_700_000 --skip-pdf
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))
# Reuse all scraper plumbing: session, headers, AES decrypt, PDF path helper.
from scraper import (  # noqa: E402
    create_session, parse_auth, headers_for, decrypt_response,
    OVERSEA_REPORT_DETAIL_API, COL_OVERSEA_REPORTS,
    MONGO_URI_DEFAULT, MONGO_DB_DEFAULT, PDF_DIR_DEFAULT, JM_AUTH_INFO,
    _pdf_dest_path,
)
import requests
from pymongo import MongoClient
from tqdm import tqdm

PROGRESS_FILE = SCRIPT_DIR / "_progress_oversea.json"


def load_progress(start_id: int) -> dict:
    if PROGRESS_FILE.exists():
        try:
            return json.loads(PROGRESS_FILE.read_text())
        except Exception:
            pass
    return {
        "last_scanned_id": start_id - 1,
        "downloaded": 0, "meta_only": 0,
        "skipped_existing": 0, "skipped_meta": 0,
        "invalid": 0, "error": 0, "pdf_fail": 0,
        "started_at": datetime.now(timezone.utc).isoformat(),
    }


def save_progress(p: dict) -> None:
    p["updated_at"] = datetime.now(timezone.utc).isoformat()
    PROGRESS_FILE.write_text(json.dumps(p, indent=2))


_stop = False


def _sig(*_):
    global _stop
    _stop = True
    print("\n[signal] stopping after current batch…", flush=True)


signal.signal(signal.SIGINT, _sig)
signal.signal(signal.SIGTERM, _sig)


def fetch_preview(session: requests.Session, rid: int) -> dict | None:
    """Reuse scraper's session + decrypt_response. Returns data dict on
    success, {'_invalid': True} on 500/已失效, None on hard error."""
    for attempt in range(2):
        try:
            r = session.post(
                OVERSEA_REPORT_DETAIL_API,
                json={"researchId": rid},
                headers=headers_for("json_oversea-research_preview"),
                timeout=15,
            )
            if r.status_code != 200:
                if attempt == 0:
                    time.sleep(1.5)
                    continue
                return None
            d = decrypt_response(r)
            if str(d.get("code")) != "0":
                return {"_invalid": True, "_msg": d.get("msg", "")}
            return d.get("data") or {}
        except (requests.RequestException, ValueError):
            if attempt == 0:
                time.sleep(1.5)
                continue
            return None
    return None


def download_pdf(session: requests.Session, url: str, dest: Path) -> tuple[int, str]:
    """Direct OSS GET. Returns (bytes_written, error_str)."""
    try:
        r = session.get(url, timeout=60)
        if r.status_code != 200:
            return 0, f"HTTP {r.status_code}"
        content = r.content
        if not content[:4].startswith(b"%PDF"):
            return 0, "not a PDF"
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(content)
        return len(content), ""
    except (requests.RequestException, OSError) as e:
        return 0, f"{type(e).__name__}: {str(e)[:80]}"


def build_doc(rid: int, data: dict, crawled_at: datetime) -> dict:
    """Shape-matches scraper.dump_oversea_report."""
    release_ms = data.get("releaseDate") or 0
    release_time = ""
    if release_ms:
        try:
            release_time = datetime.fromtimestamp(
                int(release_ms) / 1000, tz=timezone(timedelta(hours=8)),
            ).strftime("%Y-%m-%d %H:%M")
        except Exception:
            release_time = ""

    return {
        "_id": rid,
        "id": rid,
        "report_id": data.get("reportId") or "",
        "title": data.get("titleChn") or data.get("title") or "",
        "title_cn": data.get("titleChn") or "",
        "title_en": data.get("title") or "",
        "release_time": release_time,
        "release_time_ms": int(release_ms) if release_ms else None,
        "organization_name": data.get("orgNameChn") or data.get("orgName") or "",
        "organization_name_en": data.get("orgName") or "",
        "report_type": data.get("reportType") or "",
        "language_list": data.get("languageList") or [],
        "country_list": data.get("countryList") or [],
        "trans_status": data.get("transStatus"),
        "is_realtime": bool(data.get("isRealtime")),
        "stocks": data.get("stockList") or [],
        "stock_codes": data.get("stockCodeList") or [],
        "stock_names": data.get("stockNameList") or [],
        "full_codes": data.get("fullCodeList") or [],
        "industries": data.get("industryNameList") or [],
        "authors": data.get("authorList") or [],
        "pdf_num": data.get("pdfNum") or 0,
        "has_image": bool(data.get("hasImage")),
        "summary_md": (data.get("summary") or "").strip(),
        "original_url": data.get("homeOssPdfUrl") or "",
        "link_url": data.get("linkUrl") or "",
        "web_url": data.get("linkUrl") or data.get("homeOssPdfUrl") or "",
        "source_url": data.get("homeOssPdfUrl") or "",
        "preview_result": data,
        "crawled_at": crawled_at,
        "_canonical_extract_source": "jinmen_oversea_bulk",
    }


def worker(rid: int, session: requests.Session, col, pdf_dir: Path,
           skip_existing: bool, download_pdfs: bool) -> str:
    """Per-ID pipeline. Returns status tag for stats."""
    if skip_existing:
        existing = col.find_one({"_id": rid}, {"pdf_local_path": 1, "pdf_size_bytes": 1})
        if existing:
            if not download_pdfs:
                return "skipped_meta"
            if existing.get("pdf_local_path") and (existing.get("pdf_size_bytes") or 0) > 0:
                return "skipped_existing"

    data = fetch_preview(session, rid)
    if data is None:
        return "error"
    if data.get("_invalid"):
        return "invalid"

    pdf_url = data.get("homeOssPdfUrl") or ""
    release_ms = data.get("releaseDate") or 0
    report_id = data.get("reportId") or f"mndj_rtime_{rid}"
    title = data.get("titleChn") or data.get("title") or ""

    pdf_local = ""
    pdf_size = 0
    pdf_err = ""
    if download_pdfs and pdf_url:
        dest = _pdf_dest_path(pdf_dir, release_ms, report_id, rid, title)
        pdf_size, pdf_err = download_pdf(session, pdf_url, dest)
        if pdf_size > 0:
            pdf_local = str(dest)

    doc = build_doc(rid, data, datetime.now(timezone.utc))
    doc["pdf_local_path"] = pdf_local
    doc["pdf_size_bytes"] = pdf_size
    doc["pdf_download_error"] = pdf_err
    col.replace_one({"_id": rid}, doc, upsert=True)

    if pdf_size > 0:
        return "downloaded"
    if not download_pdfs:
        return "meta_only"
    return "pdf_fail"


def run(args):
    auth = parse_auth(os.environ.get("JM_AUTH", JM_AUTH_INFO))
    session = create_session(auth)

    cli = MongoClient(args.mongo_uri)
    db = cli[args.mongo_db]
    col = db[COL_OVERSEA_REPORTS]
    col.create_index("release_time")
    col.create_index("crawled_at")

    pdf_dir = Path(args.pdf_dir)
    pdf_dir.mkdir(parents=True, exist_ok=True)

    progress = load_progress(args.start)
    start_from = max(args.start, progress["last_scanned_id"] + 1) if args.resume else args.start
    end_at = args.end

    print(f"[bulk] range [{start_from}, {end_at}]  concurrency={args.concurrency}  "
          f"pdf_dir={pdf_dir}  skip_existing={args.skip_existing}  "
          f"download_pdfs={not args.skip_pdf}")

    stats = dict(downloaded=0, meta_only=0, skipped_existing=0, skipped_meta=0,
                 invalid=0, error=0, pdf_fail=0)
    last_save = time.time()

    # Pool of sessions — one per worker thread so requests.Session is thread-safe.
    # (requests.Session is NOT thread-safe for concurrent .post() calls on the
    # same instance.) Each thread pulls its own session via thread-local.
    import threading
    _tls = threading.local()

    def get_session():
        s = getattr(_tls, "s", None)
        if s is None:
            s = create_session(auth)
            _tls.s = s
        return s

    def _worker(rid):
        return worker(rid, get_session(), col, pdf_dir,
                      args.skip_existing, not args.skip_pdf)

    total = end_at - start_from + 1
    batch_size = 500
    with ThreadPoolExecutor(max_workers=args.concurrency) as ex:
        with tqdm(total=total, desc="外资回灌", unit="id", dynamic_ncols=True) as bar:
            for batch_start in range(start_from, end_at + 1, batch_size):
                if _stop:
                    break
                batch_end = min(batch_start + batch_size - 1, end_at)
                futs = [ex.submit(_worker, rid) for rid in range(batch_start, batch_end + 1)]
                for fut in as_completed(futs):
                    status = fut.result()
                    stats[status] = stats.get(status, 0) + 1
                    bar.update(1)
                    bar.set_postfix_str(
                        f"DL={stats['downloaded']} meta={stats['meta_only']} "
                        f"skip={stats['skipped_existing']+stats['skipped_meta']} "
                        f"inv={stats['invalid']} err={stats['error']+stats['pdf_fail']}"
                    )
                progress["last_scanned_id"] = batch_end
                for k, v in stats.items():
                    progress[k] = v
                if time.time() - last_save > 30:
                    save_progress(progress)
                    last_save = time.time()

    save_progress(progress)
    cli.close()
    print(f"\n[bulk] done. stats: {stats}")


def parse_args():
    p = argparse.ArgumentParser(description="Jinmen 外资研报 bulk backfiller (preview-API brute-force)")
    p.add_argument("--start", type=int, default=1, help="起始 researchId (默认 1)")
    p.add_argument("--end", type=int, default=1_700_000,
                   help="结束 researchId (默认 1,700,000 — 实测约到 1,669,000 有效)")
    p.add_argument("--concurrency", type=int, default=10,
                   help="并发线程数 (默认 10; preview API authed, 别超 30 防风控)")
    p.add_argument("--pdf-dir", default=PDF_DIR_DEFAULT,
                   help=f"PDF 存放目录 (默认 {PDF_DIR_DEFAULT})")
    p.add_argument("--skip-pdf", action="store_true",
                   help="只写 metadata, 不下载 PDF (快 5×, 适合先扫一遍 ID 空间)")
    p.add_argument("--force", dest="skip_existing", action="store_false", default=True,
                   help="强制重抓已入库的 doc (默认跳过 _id 已存在且有 PDF 的)")
    p.add_argument("--resume", action="store_true",
                   help="从 _progress_oversea.json 的 last_scanned_id+1 续跑")
    p.add_argument("--mongo-uri", default=MONGO_URI_DEFAULT)
    p.add_argument("--mongo-db", default=MONGO_DB_DEFAULT)
    return p.parse_args()


if __name__ == "__main__":
    try:
        run(parse_args())
    except KeyboardInterrupt:
        print("\n[bulk] interrupted")
