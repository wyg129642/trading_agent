#!/usr/bin/env python3
"""
Jinmen oversea Phase 2 — auth-free OSS direct download
======================================================

After Phase 1 (`download_oversea_pdfs.py --skip-pdf`) wrote 1.5M
`oversea_reports` docs with `original_url`, this script downloads each PDF
from Aliyun OSS directly. No auth required (OSS bucket is public-read).

Strategy:
  - Read Mongo docs where `pdf_size_bytes` is missing/0 (Phase 2 not yet
    done) AND `original_url` is set.
  - Concurrency 30 by default. OSS handles this fine; it's the network +
    disk write that dominates.
  - Resumable: skips docs whose target file already exists on disk
    (handles both Phase-2 partial runs and rsync overlap).
  - Bulk-updates Mongo every batch (500 docs).
  - Logs to /home/ygwang/crawl_data/migration_logs/oversea_phase2_<ts>.log.

This is INDEPENDENT of `download_oversea_pdfs.py`:
  - That script is Phase 1 (auth-required preview API for metadata).
  - This script is Phase 2 (auth-free OSS for binaries).
  - Running both in parallel is safe (different account credentials,
    different concurrency budgets).
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys
import time
from datetime import datetime
from pathlib import Path

import httpx
from pymongo import MongoClient, UpdateOne

MONGO_URI_DEFAULT = "mongodb://127.0.0.1:27018"
DB_DEFAULT        = "jinmen-full"
COLL_DEFAULT      = "oversea_reports"
DEST_ROOT_DEFAULT = "/home/ygwang/crawl_data/overseas_pdf"
LOG_DIR           = Path("/home/ygwang/crawl_data/migration_logs")

stop_flag = False


def _handle_signal(*_):
    global stop_flag
    stop_flag = True
    logging.warning("[signal] graceful stop requested — flushing current batch...")


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


def dest_for(doc, dest_root: Path) -> Path:
    """Pick local destination path for a doc.

    Trust `pdf_local_path` if it's already under our dest_root (Phase 1
    pre-set it). Otherwise compute from release_time_ms + _id.
    """
    p = (doc.get("pdf_local_path") or "").strip()
    if p:
        candidate = Path(p)
        try:
            candidate.relative_to(dest_root)
            return candidate
        except ValueError:
            pass

    rt_ms = doc.get("release_time_ms") or 0
    rid = doc["_id"]
    if rt_ms:
        ym = datetime.fromtimestamp(rt_ms / 1000).strftime("%Y-%m")
        name = f"mndj_rtime_{rt_ms}_{rid}.pdf"
    else:
        ym = "unknown"
        name = f"mndj_rtime_{rid}.pdf"
    return dest_root / ym / name


async def fetch_one(client: httpx.AsyncClient, sem: asyncio.Semaphore,
                    doc: dict, dest_root: Path):
    """Returns (status, _id, dest_path_str, size, err_msg)."""
    async with sem:
        if stop_flag:
            return ("stopped", doc["_id"], "", 0, "")
        url = (doc.get("original_url") or "").strip()
        if not url:
            return ("no_url", doc["_id"], "", 0, "")
        dest = dest_for(doc, dest_root)
        try:
            sz_existing = dest.stat().st_size if dest.is_file() else 0
        except OSError:
            sz_existing = 0
        if sz_existing > 0:
            return ("skip_existing", doc["_id"], str(dest), sz_existing, "")

        try:
            r = await client.get(url)
        except httpx.HTTPError as exc:
            return ("net_err", doc["_id"], str(dest), 0,
                    f"{type(exc).__name__}: {str(exc)[:120]}")
        if r.status_code != 200:
            return ("http_err", doc["_id"], str(dest), 0, f"HTTP {r.status_code}")
        data = r.content
        if not data:
            return ("empty", doc["_id"], str(dest), 0, "empty body")
        if not data[:4].startswith(b"%PDF"):
            return ("not_pdf", doc["_id"], str(dest), 0,
                    f"magic={data[:8].hex()}")

        try:
            dest.parent.mkdir(parents=True, exist_ok=True)
            tmp = dest.with_suffix(dest.suffix + ".part")
            await asyncio.to_thread(tmp.write_bytes, data)
            await asyncio.to_thread(tmp.replace, dest)
        except OSError as exc:
            return ("io_err", doc["_id"], str(dest), 0,
                    f"{type(exc).__name__}: {exc}")
        return ("done", doc["_id"], str(dest), len(data), "")


async def main(args):
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = LOG_DIR / f"oversea_phase2_{ts}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.FileHandler(log_file), logging.StreamHandler()],
    )
    log = logging.getLogger()

    dest_root = Path(args.dest_root)
    dest_root.mkdir(parents=True, exist_ok=True)

    cli = MongoClient(args.mongo_uri, serverSelectionTimeoutMS=10000)
    coll = cli[args.mongo_db][args.coll]

    query = {
        "original_url": {"$exists": True, "$nin": ["", None]},
        "$or": [
            {"pdf_size_bytes": {"$in": [None, 0]}},
            {"pdf_size_bytes": {"$exists": False}},
        ],
    }
    count_query = dict(query)
    if args.start_id is not None:
        count_query["_id"] = {"$gte": args.start_id}

    total_remaining = coll.count_documents(count_query) if args.limit == 0 else min(args.limit, coll.count_documents(count_query))
    log.info(f"=== Phase 2 START  pid={pid_str()} ===")
    log.info(f"mongo={args.mongo_uri} db={args.mongo_db} coll={args.coll}")
    log.info(f"dest_root={dest_root}  concurrency={args.concurrency}  batch={args.batch}")
    log.info(f"need download (--limit {args.limit}): {total_remaining:,}")

    sem = asyncio.Semaphore(args.concurrency)
    stats = {"done": 0, "skip_existing": 0, "no_url": 0,
             "net_err": 0, "http_err": 0, "not_pdf": 0,
             "empty": 0, "io_err": 0, "stopped": 0}
    bytes_done = 0
    started = time.time()
    last_progress_log = started

    timeout_cfg = httpx.Timeout(args.timeout, connect=15.0)
    limits = httpx.Limits(
        max_connections=args.concurrency * 2,
        max_keepalive_connections=args.concurrency,
    )
    async with httpx.AsyncClient(
        trust_env=False, follow_redirects=True,
        timeout=timeout_cfg, limits=limits,
        headers={"User-Agent": "Mozilla/5.0"},
    ) as client:
        n_processed = 0
        last_id = (args.start_id - 1) if args.start_id is not None else None
        projection = {"_id": 1, "original_url": 1, "pdf_local_path": 1,
                      "release_time_ms": 1, "report_id": 1}
        while True:
            if stop_flag:
                break
            page_query = dict(query)
            if last_id is not None:
                page_query["_id"] = {"$gt": last_id}
            remaining = args.batch
            if args.limit:
                remaining = min(remaining, args.limit - n_processed)
                if remaining <= 0:
                    break
            try:
                batch_docs = await asyncio.to_thread(
                    lambda q=page_query, lim=remaining: list(
                        coll.find(q, projection).sort("_id", 1).limit(lim)
                    )
                )
            except Exception as exc:
                log.warning(f"page query failed (last_id={last_id}): {exc}; retrying in 5s")
                await asyncio.sleep(5)
                continue
            if not batch_docs:
                break
            last_id = max(d["_id"] for d in batch_docs)
            n_processed += await flush_batch(
                client, sem, batch_docs, dest_root, coll,
                stats, log, started)
            bytes_done = stats.get("_bytes_done", bytes_done)
            now = time.time()
            if now - last_progress_log > 10:
                log_progress(log, stats, n_processed, total_remaining, started)
                last_progress_log = now

    elapsed = time.time() - started
    log_progress(log, stats,
                 sum(v for k, v in stats.items() if not k.startswith("_")),
                 total_remaining, started, final=True)
    log.info(f"=== Phase 2 END  elapsed={elapsed/60:.1f}min  stopped={stop_flag} ===")
    cli.close()


async def flush_batch(client, sem, batch_docs, dest_root, coll, stats, log, started):
    """Run a batch of fetches; bulk-update Mongo. Returns count processed."""
    results = await asyncio.gather(
        *[fetch_one(client, sem, d, dest_root) for d in batch_docs],
        return_exceptions=False,
    )
    ops = []
    for status, _id, dest_str, size, err in results:
        stats[status] = stats.get(status, 0) + 1
        if status == "done":
            stats["_bytes_done"] = stats.get("_bytes_done", 0) + size
            ops.append(UpdateOne(
                {"_id": _id},
                {"$set": {"pdf_local_path": dest_str,
                          "pdf_size_bytes": size,
                          "pdf_download_error": ""}},
            ))
        elif status == "skip_existing":
            ops.append(UpdateOne(
                {"_id": _id},
                {"$set": {"pdf_local_path": dest_str,
                          "pdf_size_bytes": size,
                          "pdf_download_error": ""}},
            ))
        elif status in ("net_err", "http_err", "not_pdf", "empty", "io_err"):
            ops.append(UpdateOne(
                {"_id": _id},
                {"$set": {"pdf_download_error": err[:200]}},
            ))
    if ops:
        try:
            await asyncio.to_thread(coll.bulk_write, ops, ordered=False)
        except Exception as exc:
            log.warning(f"bulk_write failed: {exc}")
    return len(batch_docs)


def log_progress(log, stats, n_processed, total, started, final=False):
    elapsed = time.time() - started
    rate = n_processed / max(elapsed, 1)
    eta_s = (total - n_processed) / max(rate, 0.01) if rate > 0 else 0
    bytes_mb = stats.get("_bytes_done", 0) / 1024 / 1024
    bw_mbps = bytes_mb / max(elapsed, 1)
    s = ", ".join(f"{k}={v}" for k, v in sorted(stats.items()) if not k.startswith("_"))
    prefix = "FINAL" if final else "progress"
    log.info(f"{prefix}: {n_processed:,}/{total:,}  "
             f"rate={rate:.1f}/s  bw={bw_mbps:.2f} MB/s  "
             f"eta={eta_s/60:.1f}min  bytes={bytes_mb:.1f}MB  [{s}]")


def pid_str():
    import os
    return f"pid={os.getpid()} ppid={os.getppid()}"


def parse_args():
    p = argparse.ArgumentParser(description="Jinmen oversea Phase 2 (auth-free OSS direct download)")
    p.add_argument("--mongo-uri", default=MONGO_URI_DEFAULT)
    p.add_argument("--mongo-db", default=DB_DEFAULT)
    p.add_argument("--coll", default=COLL_DEFAULT)
    p.add_argument("--dest-root", default=DEST_ROOT_DEFAULT,
                   help="本地目标根目录")
    p.add_argument("--concurrency", type=int, default=30,
                   help="并发请求数 (默认 30)")
    p.add_argument("--batch", type=int, default=500,
                   help="每批处理的 doc 数 (默认 500)")
    p.add_argument("--timeout", type=float, default=60.0,
                   help="单请求超时秒数 (默认 60)")
    p.add_argument("--limit", type=int, default=0,
                   help="只处理前 N 个 (0=无限, 用于 smoke test)")
    p.add_argument("--start-id", type=int, default=None,
                   help="只处理 _id >= 此值的 doc (默认全量)")
    return p.parse_args()


if __name__ == "__main__":
    try:
        asyncio.run(main(parse_args()))
    except KeyboardInterrupt:
        print("\n[phase2] interrupted")
        sys.exit(130)
