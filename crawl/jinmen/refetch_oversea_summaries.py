#!/usr/bin/env python3
"""Re-fetch oversea_reports summary_md for docs where it's currently empty.

The earlier `download_oversea_pdfs.py --skip-pdf` pass populated all 37 fields
for 1.5M docs, but `summary_md` is only 51% covered (773k of 1.51M). Those
missing are docs where the preview API returned no `summary` string at the
time of scan — usually because the report's AI summary hadn't been generated
yet. Retrying those IDs against the same `json_oversea-research_preview`
endpoint now typically fills them in.

This script only touches `summary_md` and `preview_result`; it does NOT
overwrite enrichment fields (`_canonical_tickers`, etc.) or PDF fields.

Resume-safe: restartable via `_progress_oversea_summary.json`.

Usage::
    python3 refetch_oversea_summaries.py                   # walk all missing
    python3 refetch_oversea_summaries.py --concurrency 15  # more threads
    python3 refetch_oversea_summaries.py --max 10000       # test run
"""
from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

# Clash proxy must be off for local Mongo + CN endpoint
for _k in ("http_proxy", "https_proxy", "all_proxy", "no_proxy",
           "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY"):
    os.environ.pop(_k, None)

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))
from scraper import (  # noqa: E402
    create_session, parse_auth, headers_for, decrypt_response,
    OVERSEA_REPORT_DETAIL_API, COL_OVERSEA_REPORTS,
    MONGO_URI_DEFAULT, MONGO_DB_DEFAULT, JM_AUTH_INFO,
)
import requests  # noqa: E402
from pymongo import MongoClient  # noqa: E402
from tqdm import tqdm  # noqa: E402

PROGRESS_FILE = SCRIPT_DIR / "_progress_oversea_summary.json"

_stop = False


def _sig(*_):
    global _stop
    _stop = True
    print("\n[signal] stopping after current batch…", flush=True)


signal.signal(signal.SIGINT, _sig)
signal.signal(signal.SIGTERM, _sig)


def load_progress() -> dict:
    if PROGRESS_FILE.exists():
        try:
            return json.loads(PROGRESS_FILE.read_text())
        except Exception:
            pass
    return {
        "last_processed_id": 0,
        "filled": 0, "still_empty": 0, "invalid": 0, "error": 0, "skipped": 0,
        "started_at": datetime.now(timezone.utc).isoformat(),
    }


def save_progress(p: dict) -> None:
    p["updated_at"] = datetime.now(timezone.utc).isoformat()
    PROGRESS_FILE.write_text(json.dumps(p, indent=2))


def fetch_preview(session: requests.Session, rid: int) -> dict | None:
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


def run(args):
    auth = parse_auth(os.environ.get("JM_AUTH", JM_AUTH_INFO))
    cli = MongoClient(args.mongo_uri)
    db = cli[args.mongo_db]
    col = db[COL_OVERSEA_REPORTS]

    progress = load_progress()
    since_id = max(args.since_id, progress["last_processed_id"]) if args.resume else args.since_id

    # Target: docs missing summary_md that haven't been attempted in a refill
    # pass yet. `summary_refill_at` is written by this script on every visit,
    # so the same doc isn't re-queried on a follow-up --resume run. To retry
    # after some time, drop the `summary_refill_at` filter (or `$unset` it).
    query = {
        "_id": {"$gt": since_id},
        "$or": [
            {"summary_md": {"$exists": False}},
            {"summary_md": None},
            {"summary_md": ""},
        ],
        "summary_refill_at": {"$exists": False},
    }
    total_missing = col.count_documents(query)
    print(f"[refill] missing summary_md: {total_missing} docs (since _id > {since_id})")
    if args.max:
        print(f"[refill] limited to --max={args.max}")

    # Stream IDs so we don't load 740k into memory
    cursor = col.find(query, projection={"_id": 1}, sort=[("_id", 1)],
                      no_cursor_timeout=True, batch_size=500)

    _tls = threading.local()

    def get_session() -> requests.Session:
        s = getattr(_tls, "s", None)
        if s is None:
            s = create_session(auth)
            _tls.s = s
        return s

    stats = dict(filled=0, still_empty=0, invalid=0, error=0, skipped=0)

    def worker(rid: int) -> tuple[int, str]:
        data = fetch_preview(get_session(), rid)
        if data is None:
            return rid, "error"
        if data.get("_invalid"):
            # Record invalid so we don't re-query; mark summary_md as empty str
            col.update_one(
                {"_id": rid},
                {"$set": {
                    "summary_md": "",
                    "summary_refill_at": datetime.now(timezone.utc),
                    "summary_refill_invalid": True,
                }},
            )
            return rid, "invalid"
        summary = (data.get("summary") or "").strip()
        update = {
            "summary_refill_at": datetime.now(timezone.utc),
            "preview_result": data,  # also refresh full preview in case other fields gained data
        }
        if summary:
            update["summary_md"] = summary
            col.update_one({"_id": rid}, {"$set": update})
            return rid, "filled"
        else:
            update["summary_md"] = ""
            col.update_one({"_id": rid}, {"$set": update})
            return rid, "still_empty"

    last_save = time.time()
    processed = 0
    with ThreadPoolExecutor(max_workers=args.concurrency) as ex:
        with tqdm(total=min(total_missing, args.max or total_missing),
                  desc="summary 补抓", unit="id", dynamic_ncols=True) as bar:
            batch: list[int] = []
            for doc in cursor:
                if _stop:
                    break
                if args.max and processed >= args.max:
                    break
                batch.append(doc["_id"])
                if len(batch) < 500:
                    continue
                futs = [ex.submit(worker, rid) for rid in batch]
                for fut in as_completed(futs):
                    rid, status = fut.result()
                    stats[status] = stats.get(status, 0) + 1
                    progress["last_processed_id"] = max(progress["last_processed_id"], rid)
                    processed += 1
                    bar.update(1)
                    bar.set_postfix_str(
                        f"filled={stats['filled']} empty={stats['still_empty']} "
                        f"inv={stats['invalid']} err={stats['error']}"
                    )
                batch = []
                for k, v in stats.items():
                    progress[k] = v
                if time.time() - last_save > 30:
                    save_progress(progress)
                    last_save = time.time()
            # tail
            if batch and not _stop:
                futs = [ex.submit(worker, rid) for rid in batch]
                for fut in as_completed(futs):
                    rid, status = fut.result()
                    stats[status] = stats.get(status, 0) + 1
                    progress["last_processed_id"] = max(progress["last_processed_id"], rid)
                    processed += 1
                    bar.update(1)

    cursor.close()
    for k, v in stats.items():
        progress[k] = v
    save_progress(progress)
    cli.close()
    print(f"\n[refill] done. processed={processed} stats={stats}")


def parse_args():
    p = argparse.ArgumentParser(
        description="Refetch jinmen oversea_reports preview for docs missing summary_md. "
                    "Only writes summary_md / preview_result / summary_refill_at — "
                    "does NOT touch _canonical_tickers, pdf_local_path, etc.")
    p.add_argument("--concurrency", type=int, default=10,
                   help="并发线程数 (默认 10; 与 download_oversea_pdfs 一致)")
    p.add_argument("--since-id", type=int, default=0,
                   help="只处理 _id > 该值的 doc (默认 0, 全扫)")
    p.add_argument("--max", type=int, default=0,
                   help="最多处理 N 条 (0=不限)")
    p.add_argument("--resume", action="store_true",
                   help="从 _progress_oversea_summary.json 的 last_processed_id 继续")
    p.add_argument("--mongo-uri", default=MONGO_URI_DEFAULT)
    p.add_argument("--mongo-db", default=MONGO_DB_DEFAULT)
    return p.parse_args()


if __name__ == "__main__":
    try:
        run(parse_args())
    except KeyboardInterrupt:
        print("\n[refill] interrupted")
