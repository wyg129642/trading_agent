#!/usr/bin/env python3
"""Meritco forum bypass backfill via direct detail-API ID enumeration.

Background
----------
Platform list endpoint `/matrix-search/forum/select/list` hard-caps at
top ~1815 items (type=2) / ~471 items (type=3). Regardless of how many pages
you scroll, the API only returns the most-recent 2286 entries in total —
historical docs older than that (the漏爬 gap from 2025-12 / 2026-02 / 2026-03
where watchers were broken) are permanently invisible through pagination.

The detail endpoint `/matrix-search/forum/select/id?forumId=<N>` has no such
cap: passing any historical forum_id (up to platform's max) returns the full
doc regardless of whether it's in the current top-1815 list. That's the
bypass — brute-force enumerate every ID in the observed range and call
detail on each, filling DB gaps.

ID range observed in our DB (2026-04-23):  328..3113  (2786 positions)
Our DB has 2286 docs → 500 IDs never crawled. This script targets those gaps.

Usage::
    # small test (50 IDs)
    python3 bypass_backfill.py --start 328 --end 380 --skip-pdf

    # production: walk the known-gap span, skip PDFs, 3 workers
    python3 bypass_backfill.py --start 1 --end 3200 --skip-pdf --concurrency 3

    # resume from last checkpoint
    python3 bypass_backfill.py --resume --skip-pdf
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

# Clash proxy must be off for local Mongo + CN LAN endpoint
for _k in ("http_proxy", "https_proxy", "all_proxy", "no_proxy",
           "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY"):
    os.environ.pop(_k, None)

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

# Import everything we need from the main scraper
from scraper import (  # noqa: E402
    HttpConfig, AuthExpired, SessionDead,
    COL_FORUM, COL_STATE,
    MONGO_URI_DEFAULT, MONGO_DB_DEFAULT, PDF_DIR_DEFAULT,
    load_creds_from_file, default_user_agent, create_client,
    fetch_detail, build_doc, download_attachments, parse_pdf_url_field,
    pick_time, time_to_ms, with_retry,
)
from ticker_tag import stamp as _stamp_ticker  # noqa: E402
from pymongo import MongoClient  # noqa: E402
from tqdm import tqdm  # noqa: E402

PROGRESS_FILE = SCRIPT_DIR / "_progress_bypass.json"

_stop = False


def _sig(*_):
    global _stop
    _stop = True
    print("\n[signal] 捕获, 当前 batch 完成后停止…", flush=True)


signal.signal(signal.SIGINT, _sig)
signal.signal(signal.SIGTERM, _sig)


def load_progress(start_id: int) -> dict:
    if PROGRESS_FILE.exists():
        try:
            return json.loads(PROGRESS_FILE.read_text())
        except Exception:
            pass
    return {
        "last_scanned_id": start_id - 1,
        "added": 0, "skipped_existing": 0,
        "invalid": 0, "error": 0, "auth_dead": 0,
        "started_at": datetime.now(timezone.utc).isoformat(),
    }


def save_progress(p: dict) -> None:
    p["updated_at"] = datetime.now(timezone.utc).isoformat()
    PROGRESS_FILE.write_text(json.dumps(p, indent=2, ensure_ascii=False))


def fetch_and_store_one(
    client, db, forum_id: int, pdf_dir: Path | None, skip_pdf: bool, force: bool,
) -> str:
    """Return status tag: added / skipped_existing / invalid / error / auth_dead."""
    col = db[COL_FORUM]
    if not force:
        ex = col.find_one({"_id": forum_id}, {"_id": 1})
        if ex:
            return "skipped_existing"

    try:
        data = fetch_detail(client, forum_id)
    except AuthExpired:
        return "auth_dead"
    except SessionDead:
        return "auth_dead"
    except Exception:  # noqa: BLE001
        return "error"

    if not isinstance(data, dict):
        return "invalid"
    if data.get("code") != 200:
        # Common codes: 404 / doc deleted
        return "invalid"
    result = data.get("result") or {}
    if not result or not isinstance(result, dict):
        return "invalid"

    # Synthesize a list-like `item` dict that build_doc expects.
    # detail response fields align with list item fields mostly.
    item = {
        "id": result.get("id") or forum_id,
        "title": result.get("title", ""),
        "operationTime": result.get("operationTime"),
        "createTime": result.get("createTime"),
        "meetingTime": result.get("meetingTime"),
        "recommendTime": result.get("recommendTime"),
        "releaseTime": result.get("releaseTime"),
        "type": result.get("type"),
        "industry": result.get("industry"),
        "language": result.get("language"),
        "author": result.get("author"),
        "operator": result.get("operator"),
        "expertInformation": result.get("expertInformation"),
        "expertTypeName": result.get("expertTypeName"),
        "reportTypeName": result.get("reportTypeName"),
    }
    forum_type = result.get("type") or 2
    release_time = pick_time(item)

    doc = build_doc(item, result, forum_type, release_time)

    # PDF attachments (if caller enabled)
    if pdf_dir is not None:
        attachments = parse_pdf_url_field(doc.get("pdf_url"))
        if attachments:
            try:
                atts = download_attachments(
                    client, attachments, forum_id, release_time,
                    item.get("title", ""), pdf_dir,
                    force=False, skip_download=skip_pdf,
                )
                doc["pdf_attachments"] = atts
                if atts:
                    first = atts[0]
                    for k in ("pdf_rel_path", "pdf_local_path",
                              "pdf_size_bytes", "pdf_download_error"):
                        if first.get(k) is not None:
                            doc[k] = first[k]
            except Exception:  # noqa: BLE001 — PDF failure non-fatal for metadata
                pass

    try:
        _stamp_ticker(doc, "meritco", col)
        col.replace_one({"_id": forum_id}, doc, upsert=True)
    except Exception:  # noqa: BLE001
        return "error"
    return "added"


def _probe_max_id(mongo_uri: str, mongo_db: str) -> int:
    """Look up the current highest forum_id in DB (for --watch mode)."""
    try:
        cli = MongoClient(mongo_uri, serverSelectionTimeoutMS=3000)
        d = cli[mongo_db][COL_FORUM].find_one({}, sort=[("_id", -1)], projection={"_id": 1})
        cli.close()
        return int(d["_id"]) if d else 0
    except Exception:  # noqa: BLE001
        return 0


def run(args):
    # Loop mode: re-run periodically, auto-extending end= to follow new IDs
    # published by the platform. Each pass dedup-skips existing — only truly
    # new IDs incur a detail call.
    if args.loop:
        pass_n = 0
        while True:
            pass_n += 1
            probed_max = _probe_max_id(args.mongo_uri, args.mongo_db)
            # end = max(configured end, probed_max + lookahead_buffer)
            effective_end = max(args.end, probed_max + args.lookahead)
            print(f"\n[bypass loop] pass #{pass_n}  scan {args.start}..{effective_end}  "
                  f"(probed DB max={probed_max})")
            # 1-shot run with adjusted end
            saved_end = args.end
            args.end = effective_end
            try:
                _run_once(args)
            finally:
                args.end = saved_end
            if _stop:
                break
            print(f"[bypass loop] sleep {args.loop_interval}s 前, 下轮 pass #{pass_n+1}")
            slept = 0
            while slept < args.loop_interval and not _stop:
                time.sleep(min(10, args.loop_interval - slept))
                slept += 10
            if _stop:
                break
        return 0
    return _run_once(args)


def _run_once(args):
    cookie, ua = load_creds_from_file()
    if not cookie:
        cookie = os.environ.get("MERITCO_AUTH", "")
    if not cookie:
        print("[error] 缺 cookie/token. 在 credentials.json 或 MERITCO_AUTH 里设置")
        return 1

    # Per-thread httpx client via thread-local, because httpx.Client is not
    # thread-safe for concurrent use. Same pattern as download_oversea_pdfs.
    _tls = threading.local()

    def get_client():
        cli = getattr(_tls, "cli", None)
        if cli is None:
            cli = create_client(cookie, ua or default_user_agent())
            _tls.cli = cli
        return cli

    pdf_dir: Path | None = None
    if args.pdf_dir and not args.skip_pdf_entirely:
        pdf_dir = Path(args.pdf_dir).expanduser().resolve()
        pdf_dir.mkdir(parents=True, exist_ok=True)

    cli_mongo = MongoClient(args.mongo_uri)
    db = cli_mongo[args.mongo_db]

    progress = load_progress(args.start)
    start = max(args.start, progress["last_scanned_id"] + 1) if args.resume else args.start
    end = args.end
    if end < start:
        print(f"[error] end {end} < start {start}")
        return 1

    print(f"[bypass] ID range [{start}, {end}]  concurrency={args.concurrency}  "
          f"skip_pdf={args.skip_pdf}  resume={args.resume}")

    stats = dict(added=0, skipped_existing=0, invalid=0, error=0, auth_dead=0)
    last_save = time.time()

    def _worker(forum_id: int) -> tuple[int, str]:
        status = fetch_and_store_one(
            get_client(), db, forum_id, pdf_dir,
            skip_pdf=args.skip_pdf, force=args.force,
        )
        return forum_id, status

    total = end - start + 1
    batch_size = 200
    auth_dead_consecutive = 0
    try:
        with ThreadPoolExecutor(max_workers=args.concurrency) as ex:
            with tqdm(total=total, desc="meritco bypass", unit="id",
                      dynamic_ncols=True) as bar:
                for batch_start in range(start, end + 1, batch_size):
                    if _stop:
                        break
                    batch_end = min(batch_start + batch_size - 1, end)
                    ids = list(range(batch_start, batch_end + 1))
                    futs = [ex.submit(_worker, fid) for fid in ids]
                    batch_auth_dead = 0
                    for fut in as_completed(futs):
                        fid, status = fut.result()
                        stats[status] = stats.get(status, 0) + 1
                        if status == "auth_dead":
                            batch_auth_dead += 1
                        bar.update(1)
                        bar.set_postfix_str(
                            f"+{stats['added']} dup={stats['skipped_existing']} "
                            f"inv={stats['invalid']} err={stats['error']} "
                            f"auth_dead={stats['auth_dead']}"
                        )
                    progress["last_scanned_id"] = batch_end
                    for k, v in stats.items():
                        progress[k] = v
                    if time.time() - last_save > 30:
                        save_progress(progress)
                        last_save = time.time()
                    # If an entire batch hit auth_dead, abort — cookie revoked
                    if batch_auth_dead == len(ids):
                        auth_dead_consecutive += 1
                    else:
                        auth_dead_consecutive = 0
                    if auth_dead_consecutive >= 2:
                        print("\n[bypass] 连续 2 个 batch 全 auth_dead — cookie 失效, 停止")
                        break
                    # Gentle between-batch rest to avoid bursts
                    time.sleep(args.batch_rest)
    finally:
        save_progress(progress)
        cli_mongo.close()
    print(f"\n[bypass] done. stats: {stats}")
    return 0


def parse_args():
    p = argparse.ArgumentParser(
        description="Meritco forum by-ID bypass — 绕过 list API top-1815 封顶, 直接枚举 detail"
    )
    p.add_argument("--start", type=int, default=1,
                   help="起始 forum_id (默认 1)")
    p.add_argument("--end", type=int, default=3300,
                   help="结束 forum_id (默认 3300; DB 当前 max 是 3113, 留点余量)")
    p.add_argument("--concurrency", type=int, default=3,
                   help="并发线程数 (默认 3; detail 签名调用, 谨慎)")
    p.add_argument("--batch-rest", type=float, default=2.0,
                   help="每 batch (200 ID) 之间的休息秒 (默认 2s)")
    p.add_argument("--skip-pdf", action="store_true",
                   help="不下载 PDF 附件, 只存 metadata (推荐, 快 5-10x)")
    p.add_argument("--skip-pdf-entirely", action="store_true",
                   help="完全不处理 PDF 字段 (pdf_dir=None)")
    p.add_argument("--pdf-dir", default=PDF_DIR_DEFAULT)
    p.add_argument("--force", action="store_true",
                   help="已有 _id 也重抓覆盖 (默认跳过)")
    p.add_argument("--resume", action="store_true",
                   help="从 _progress_bypass.json 续跑")
    p.add_argument("--loop", action="store_true",
                   help="循环模式: 每轮扫完 sleep 后再扫. 自动把 end 扩到 DB max+lookahead")
    p.add_argument("--loop-interval", type=int, default=1800,
                   help="loop 模式两轮之间秒数 (默认 1800=30 分钟)")
    p.add_argument("--lookahead", type=int, default=100,
                   help="loop 模式每轮 end 至少 = DB max + N (默认 100)")
    p.add_argument("--mongo-uri", default=MONGO_URI_DEFAULT)
    p.add_argument("--mongo-db", default=MONGO_DB_DEFAULT)
    return p.parse_args()


if __name__ == "__main__":
    try:
        sys.exit(run(parse_args()))
    except KeyboardInterrupt:
        print("\n[bypass] 中断")
