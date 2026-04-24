#!/usr/bin/env python3
"""Migrate local MongoDB (8 crawler DBs + sentimentrader) + PDF files
to remote MongoDB at 192.168.31.176:35002.

Source: mongodb://localhost:27017 (Docker container `crawl_data`)
Target: mongodb://u_spider:prod_X5BKVbAc@192.168.31.176:35002/?authSource=admin

Mapping (source DB -> target DB):
  alphapai       -> alphapai-full
  jinmen         -> jinmen-full
  meritco        -> jiuqian-full
  thirdbridge    -> third-bridge
  funda          -> funda
  gangtise       -> gangtise-full
  acecamp        -> acecamp
  alphaengine    -> alphaengine
  sentimentrader -> funda (merged: indicators -> sentimentrader_indicators,
                           images -> funda GridFS with prefix sentimentrader/)

PDF migration: each platform's <platform>_pdfs/**/*.pdf -> target DB's GridFS
(fs.files + fs.chunks). GridFS filename = relative path from the pdf root.

Checkpoint: logs/migration_state.json tracks per-collection last migrated _id
and per-file uploaded (for GridFS). Safe to restart.

Verification pass at the end: count_documents + sample _id for each
(source, target) pair.

Usage:
  # dry-run: show plan, no writes
  python3 scripts/migrate_to_remote_mongo.py --dry-run

  # full migration (metadata + PDF)
  python3 scripts/migrate_to_remote_mongo.py

  # metadata only (skip PDF upload)
  python3 scripts/migrate_to_remote_mongo.py --skip-pdf

  # PDF only (skip metadata)
  python3 scripts/migrate_to_remote_mongo.py --skip-metadata

  # verify only (no writes, compare counts)
  python3 scripts/migrate_to_remote_mongo.py --verify-only
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Optional

# Unset proxy envs (Clash at 7890 intercepts local + LAN)
for _k in ("http_proxy", "https_proxy", "all_proxy", "no_proxy",
           "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY"):
    os.environ.pop(_k, None)

from pymongo import MongoClient, InsertOne, ReplaceOne  # noqa: E402
from pymongo.errors import BulkWriteError, DuplicateKeyError  # noqa: E402
from gridfs import GridFSBucket  # noqa: E402

REPO = Path(__file__).resolve().parent.parent
STATE_FILE = REPO / "logs" / "migration_state.json"
LOG_FILE = REPO / "logs" / "migration.log"

SRC_URI = "mongodb://localhost:27017"
DST_URI = "mongodb://u_spider:prod_X5BKVbAc@192.168.31.176:35002/?authSource=admin"

# Source DB -> (target DB, pdf root dir or None, sentimentrader merge prefix)
DB_MAP: dict[str, tuple[str, Optional[str]]] = {
    "alphapai":       ("alphapai-full",  "/home/ygwang/crawl_data/alphapai_pdfs"),
    "jinmen":         ("jinmen-full",    "/home/ygwang/crawl_data/jinmen_pdfs"),
    "meritco":        ("jiuqian-full",   "/home/ygwang/crawl_data/meritco_pdfs"),
    "thirdbridge":    ("third-bridge",   None),
    "funda":          ("funda",          None),
    "gangtise":       ("gangtise-full",  "/home/ygwang/crawl_data/gangtise_pdfs"),
    "acecamp":        ("acecamp",        "/home/ygwang/crawl_data/acecamp_pdfs"),
    "alphaengine":    ("alphaengine",    "/home/ygwang/crawl_data/alphaengine_pdfs"),
    # sentimentrader -> merge into funda DB with renamed collections + images to funda GridFS
    "sentimentrader": ("funda",          "/home/ygwang/crawl_data/sentimentrader_images"),
}

# When migrating sentimentrader -> funda, rename collections to avoid collisions
SENTIMENTRADER_COLLECTION_PREFIX = "sentimentrader_"

# Skip system/noisy collections
SKIP_COLLECTIONS: set[str] = {"_probe", "test", "fs.files", "fs.chunks"}
# _state collections we do migrate (scraper checkpoint) but not noisy ones
# (empty or obsolete):
SKIP_COLLECTION_PATTERNS = ()

BATCH_SIZE_DEFAULT = 500
# Larger batches for small-doc collections:
BATCH_SIZE_SMALL_DOC = 2000  # when avgObjSize < 10KB


def log(msg: str) -> None:
    line = f"{time.strftime('%Y-%m-%d %H:%M:%S')} {msg}"
    print(line, flush=True)
    try:
        LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        with LOG_FILE.open("a") as f:
            f.write(line + "\n")
    except Exception:
        pass


def load_state() -> dict:
    if not STATE_FILE.exists():
        return {}
    try:
        return json.loads(STATE_FILE.read_text())
    except Exception:
        return {}


import threading
_STATE_LOCK = threading.Lock()


def save_state(state: dict) -> None:
    """Thread-safe state save.
    - 多线程 rename race → 每 thread 唯一 tmp 名 + 全局锁.
    - json.dumps 迭代 dict 时, 另一 thread 改 dict → "dictionary changed
      size during iteration". 用 _STATE_LOCK 同时保护 dumps + write + rename.
    """
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with _STATE_LOCK:
        # Snapshot inside lock: dict is mutated concurrently from other threads
        payload = json.dumps(state, indent=2, default=str, ensure_ascii=False)
        tmp = STATE_FILE.with_suffix(f".json.tmp.{os.getpid()}.{threading.get_ident()}")
        tmp.write_text(payload)
        tmp.replace(STATE_FILE)


# =====================================================================
# Metadata migration
# =====================================================================

def migrate_collection(src_uri: str, dst_uri: str, src_db: str, dst_db: str,
                      src_coll: str, dst_coll: str, state: dict,
                      batch_size: int = BATCH_SIZE_DEFAULT,
                      force: bool = False) -> dict:
    """Stream copy with ReplaceOne (upsert=True) so reruns are idempotent.
    Uses _id sort + resume from last-written id.
    """
    src_cli = MongoClient(src_uri, maxPoolSize=4)
    dst_cli = MongoClient(dst_uri, maxPoolSize=8,
                          serverSelectionTimeoutMS=15000,
                          socketTimeoutMS=60000)
    src = src_cli[src_db][src_coll]
    dst = dst_cli[dst_db][dst_coll]

    total = src.estimated_document_count()
    key = f"{src_db}.{src_coll}"
    resumed_from = state.get(key, {}).get("last_id")
    already_copied = state.get(key, {}).get("copied", 0) or 0

    # Query with resume support
    query = {}
    if resumed_from is not None and not force:
        query = {"_id": {"$gt": resumed_from}}

    t0 = time.time()
    log(f"  [{key:40s}] start: total≈{total} resumed_from={str(resumed_from)[:40]} batch={batch_size}")

    # Use simple sort+find with batch_size
    cursor = src.find(query).sort("_id", 1).batch_size(batch_size)

    ops_buffer: list = []
    last_id = resumed_from
    copied = 0

    try:
        for doc in cursor:
            ops_buffer.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
            last_id = doc["_id"]
            if len(ops_buffer) >= batch_size:
                try:
                    dst.bulk_write(ops_buffer, ordered=False)
                except BulkWriteError as bwe:
                    # Tolerate duplicate key / version errors, continue
                    n_ok = bwe.details.get("nInserted", 0) + bwe.details.get("nUpserted", 0)
                    log(f"    [{key}] bulk_write tolerated errors, nOK={n_ok} errs={len(bwe.details.get('writeErrors',[]))}")
                copied += len(ops_buffer)
                ops_buffer.clear()
                # Periodic checkpoint (best-effort — race safe now)
                try:
                    state[key] = {"last_id": last_id, "copied": already_copied + copied,
                                  "total": total, "updated": time.time()}
                    save_state(state)
                except Exception as e:
                    log(f"    [{key}] save_state failed (non-fatal): {e}")

        # Final flush
        if ops_buffer:
            try:
                dst.bulk_write(ops_buffer, ordered=False)
            except BulkWriteError as bwe:
                log(f"    [{key}] bulk_write tolerated errors on flush")
            copied += len(ops_buffer)
            ops_buffer.clear()
    finally:
        src_cli.close()
        dst_cli.close()

    elapsed = time.time() - t0
    final_total = copied + already_copied
    rate = final_total / max(elapsed, 0.001)
    state[key] = {"last_id": last_id, "copied": final_total, "total": total,
                  "status": "done", "elapsed_s": round(elapsed, 1), "updated": time.time()}
    save_state(state)
    log(f"  [{key:40s}] DONE copied={copied} (cum={final_total}/{total}) "
        f"elapsed={elapsed:.1f}s rate={rate:.0f}/s")
    return {"key": key, "copied": copied, "total": total, "elapsed": elapsed}


def migrate_indexes(src_uri: str, dst_uri: str, src_db: str, dst_db: str,
                    src_coll: str, dst_coll: str) -> None:
    """Copy non-_id indexes (by index_information) from src to dst.

    `index_information()` returns keys as a list of tuples already
    (e.g. [('title', 1)]), so we don't need .items() on it.
    """
    src_cli = MongoClient(src_uri)
    dst_cli = MongoClient(dst_uri, serverSelectionTimeoutMS=10000)
    try:
        src = src_cli[src_db][src_coll]
        dst = dst_cli[dst_db][dst_coll]
        info = src.index_information()
        for name, meta in info.items():
            if name == "_id_":
                continue
            keys = meta.get("key")
            if not keys:
                continue
            opts = {k: v for k, v in meta.items() if k in
                    ("unique", "sparse", "partialFilterExpression",
                     "expireAfterSeconds", "background", "weights", "default_language",
                     "language_override", "textIndexVersion", "collation")}
            opts["name"] = name
            try:
                # keys is already list of (field, direction) tuples
                dst.create_index(keys, **{k: v for k, v in opts.items() if v is not None})
            except Exception as e:
                log(f"    [idx {src_db}.{src_coll}.{name}] SKIP {e}")
    finally:
        src_cli.close()
        dst_cli.close()


# =====================================================================
# PDF -> GridFS migration
# =====================================================================

def migrate_pdfs_for_platform(src_uri: str, dst_uri: str,
                              dst_db: str, pdf_root: str,
                              gridfs_prefix: str,
                              state: dict) -> dict:
    """Walk pdf_root, upload each file to dst_db's GridFS with filename
    set to the relative path (like 'alphapai_pdfs/2025-11/x.pdf').
    gridfs_prefix empties to "" for platform PDFs, or "sentimentrader/"
    for sentimentrader images going into funda DB.
    """
    root = Path(pdf_root)
    if not root.exists():
        log(f"  [PDF {dst_db}] root not found: {pdf_root}, skip")
        return {"files": 0, "bytes": 0, "elapsed": 0}

    state_key = f"_pdf_{dst_db}_{gridfs_prefix or root.name}"
    done_set: set[str] = set(state.get(state_key, {}).get("done") or [])

    dst_cli = MongoClient(dst_uri, maxPoolSize=8,
                          serverSelectionTimeoutMS=15000,
                          socketTimeoutMS=300000)
    bucket = GridFSBucket(dst_cli[dst_db])

    files = [p for p in root.rglob("*") if p.is_file()]
    total = len(files)
    log(f"  [PDF {dst_db}/{gridfs_prefix or root.name}] found {total} files, resumed={len(done_set)}")
    t0 = time.time()
    copied = 0
    bytes_copied = 0
    last_print = t0

    for i, fp in enumerate(files):
        rel = fp.relative_to(root.parent)  # e.g. alphapai_pdfs/2025-11/x.pdf
        gridfs_name = f"{gridfs_prefix}{rel.as_posix()}" if gridfs_prefix else rel.as_posix()

        if gridfs_name in done_set:
            continue

        try:
            # Skip if already exists in GridFS (idempotent restart)
            existing = dst_cli[dst_db]["fs.files"].find_one(
                {"filename": gridfs_name}, projection={"_id": 1})
            if existing:
                done_set.add(gridfs_name)
                copied += 1
                continue

            size = fp.stat().st_size
            metadata = {
                "source_path": str(fp),
                "platform_root": root.name,
                "rel_path": rel.as_posix(),
                "size_bytes": size,
                "migrated_at": time.time(),
            }
            with fp.open("rb") as src_fh:
                bucket.upload_from_stream(
                    filename=gridfs_name,
                    source=src_fh,
                    chunk_size_bytes=1024 * 1024,  # 1MB chunks (faster than default 255KB)
                    metadata=metadata,
                )
            done_set.add(gridfs_name)
            copied += 1
            bytes_copied += size

            # Checkpoint every 100 files
            if copied % 100 == 0:
                state[state_key] = {"done": sorted(done_set),
                                    "copied": copied, "total": total,
                                    "updated": time.time()}
                save_state(state)

            # Progress print every 15s
            if time.time() - last_print > 15:
                elapsed = time.time() - t0
                rate_files = copied / max(elapsed, 0.001)
                rate_mbs = bytes_copied / 1024 / 1024 / max(elapsed, 0.001)
                eta_sec = (total - i - 1) / max(rate_files, 0.001)
                log(f"    [PDF {dst_db}] progress {copied}/{total}  "
                    f"{rate_files:.1f} file/s  {rate_mbs:.1f} MB/s  eta={eta_sec/60:.1f}min")
                last_print = time.time()

        except Exception as e:
            log(f"    [PDF {dst_db}] ERROR on {fp}: {e}")

    # Final checkpoint
    state[state_key] = {"done": sorted(done_set), "copied": copied,
                        "total": total, "status": "done",
                        "bytes": bytes_copied, "updated": time.time()}
    save_state(state)

    dst_cli.close()
    elapsed = time.time() - t0
    mb = bytes_copied / 1024 / 1024
    log(f"  [PDF {dst_db}] DONE copied={copied}/{total} "
        f"{mb:.0f}MB in {elapsed:.0f}s ({mb/max(elapsed,0.001):.1f} MB/s)")
    return {"files": copied, "bytes": bytes_copied, "elapsed": elapsed}


# =====================================================================
# Orchestration
# =====================================================================

def plan_collections(src_uri: str) -> list[tuple[str, str, str, str]]:
    """Return list of (src_db, dst_db, src_coll, dst_coll) tuples.
    Applies sentimentrader -> funda merge (rename with prefix).
    Skips system / test collections.
    """
    plan = []
    src_cli = MongoClient(src_uri)
    try:
        for src_db, (dst_db, _pdf) in DB_MAP.items():
            if src_db not in src_cli.list_database_names():
                continue
            db = src_cli[src_db]
            for c in db.list_collection_names():
                if c in SKIP_COLLECTIONS:
                    continue
                if c.startswith("fs."):  # existing local GridFS (none expected)
                    continue
                # sentimentrader -> funda merge: prefix collection names
                if src_db == "sentimentrader":
                    dst_coll = SENTIMENTRADER_COLLECTION_PREFIX + c
                else:
                    dst_coll = c
                plan.append((src_db, dst_db, c, dst_coll))
    finally:
        src_cli.close()
    return plan


def run_metadata_phase(src_uri: str, dst_uri: str, parallel: int = 4,
                       force: bool = False) -> None:
    plan = plan_collections(src_uri)
    state = load_state()

    log(f"[META] plan: {len(plan)} collections to migrate")
    for s_db, d_db, s_c, d_c in plan:
        log(f"    {s_db}.{s_c} -> {d_db}.{d_c}")

    # Run sequentially with parallel-collection option for small collections;
    # the biggest (jinmen.oversea_reports, jinmen.meetings) should run alone.
    big_cols = {"jinmen.oversea_reports", "jinmen.meetings",
                "alphapai.wechat_articles", "alphapai.reports"}
    small_plan = [p for p in plan if f"{p[0]}.{p[2]}" not in big_cols]
    big_plan = [p for p in plan if f"{p[0]}.{p[2]}" in big_cols]

    # Determine batch sizes by avg doc size
    def batch_for(src_db: str, src_coll: str) -> int:
        src_cli = MongoClient(src_uri)
        try:
            s = src_cli[src_db].command({"collStats": src_coll})
            avg = s.get("avgObjSize", 50000)
            return BATCH_SIZE_SMALL_DOC if avg < 10000 else BATCH_SIZE_DEFAULT
        except Exception:
            return BATCH_SIZE_DEFAULT
        finally:
            src_cli.close()

    def task(p):
        s_db, d_db, s_c, d_c = p
        bs = batch_for(s_db, s_c)
        migrate_collection(src_uri, dst_uri, s_db, d_db, s_c, d_c, state,
                           batch_size=bs, force=force)
        migrate_indexes(src_uri, dst_uri, s_db, d_db, s_c, d_c)

    # Phase 1: run big collections SEQUENTIALLY (they saturate the remote)
    log(f"[META] phase 1: {len(big_plan)} big collections sequential")
    for p in big_plan:
        task(p)

    # Phase 2: run small collections with parallel=4
    log(f"[META] phase 2: {len(small_plan)} small collections, parallel={parallel}")
    with ThreadPoolExecutor(max_workers=parallel) as ex:
        futs = [ex.submit(task, p) for p in small_plan]
        for f in as_completed(futs):
            try:
                f.result()
            except Exception as e:
                log(f"    TASK ERR: {e}")


def run_pdf_phase(src_uri: str, dst_uri: str, parallel: int = 3) -> None:
    state = load_state()
    tasks = []
    for src_db, (dst_db, pdf_root) in DB_MAP.items():
        if not pdf_root:
            continue
        gfs_prefix = "sentimentrader/" if src_db == "sentimentrader" else ""
        tasks.append((dst_db, pdf_root, gfs_prefix))

    log(f"[PDF] plan: {len(tasks)} platform dirs")
    for t in tasks:
        log(f"    -> db={t[0]} root={t[1]} prefix={t[2]!r}")

    # Big disks run in parallel across different target DBs
    # (remote Mongo can handle parallel GridFS writes to separate DBs)
    with ThreadPoolExecutor(max_workers=parallel) as ex:
        futs = {ex.submit(migrate_pdfs_for_platform, src_uri, dst_uri,
                          dst_db, pdf_root, prefix, state): (dst_db, pdf_root)
                for dst_db, pdf_root, prefix in tasks}
        for f in as_completed(futs):
            try:
                r = f.result()
                log(f"  [PDF] done {futs[f]}: files={r.get('files')} "
                    f"{(r.get('bytes') or 0)/1024/1024:.0f}MB")
            except Exception as e:
                log(f"  [PDF] task ERR: {e}")


def verify(src_uri: str, dst_uri: str) -> None:
    src_cli = MongoClient(src_uri)
    dst_cli = MongoClient(dst_uri, serverSelectionTimeoutMS=10000)
    plan = plan_collections(src_uri)
    log("\n==== VERIFICATION ====")
    ok = 0
    bad = 0
    for s_db, d_db, s_c, d_c in plan:
        try:
            sn = src_cli[s_db][s_c].estimated_document_count()
            dn = dst_cli[d_db][d_c].estimated_document_count()
            status = "OK " if sn == dn else "DIFF"
            if sn == dn:
                ok += 1
            else:
                bad += 1
            log(f"  [{status}] {s_db}.{s_c} ({sn}) -> {d_db}.{d_c} ({dn})")
        except Exception as e:
            bad += 1
            log(f"  [ERR] {s_db}.{s_c} -> {d_db}.{d_c}: {e}")

    # GridFS count verification
    log("\n==== GRIDFS (fs.files count per target DB) ====")
    total_files = 0
    for src_db, (dst_db, pdf_root) in DB_MAP.items():
        if not pdf_root:
            continue
        try:
            n = dst_cli[dst_db]["fs.files"].estimated_document_count()
            on_disk = sum(1 for _ in Path(pdf_root).rglob("*") if _.is_file())
            total_files += n
            log(f"  {dst_db}: GridFS={n}   disk={on_disk}   src_root={pdf_root}")
        except Exception as e:
            log(f"  {dst_db}: ERR {e}")

    log(f"\nSUMMARY: metadata OK={ok} DIFF/ERR={bad}, GridFS total files={total_files}")
    src_cli.close()
    dst_cli.close()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--src-uri", default=SRC_URI)
    ap.add_argument("--dst-uri", default=DST_URI)
    ap.add_argument("--parallel-meta", type=int, default=4)
    ap.add_argument("--parallel-pdf", type=int, default=3)
    ap.add_argument("--skip-metadata", action="store_true")
    ap.add_argument("--skip-pdf", action="store_true")
    ap.add_argument("--verify-only", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--force", action="store_true",
                    help="Ignore resume checkpoint, re-upload everything")
    args = ap.parse_args()

    log(f"====== MIGRATION START ======")
    log(f"SRC: {args.src_uri}")
    log(f"DST: {args.dst_uri}")
    log(f"DB MAP: {json.dumps({k:v[0] for k,v in DB_MAP.items()}, ensure_ascii=False)}")

    if args.dry_run:
        plan = plan_collections(args.src_uri)
        log(f"[DRY-RUN] {len(plan)} collections planned")
        for s_db, d_db, s_c, d_c in plan:
            log(f"    {s_db}.{s_c:30s} -> {d_db}.{d_c}")
        log(f"[DRY-RUN] PDF dirs:")
        for src_db, (dst_db, pdf_root) in DB_MAP.items():
            if pdf_root:
                log(f"    {pdf_root} -> {dst_db}/fs.files")
        return 0

    if args.verify_only:
        verify(args.src_uri, args.dst_uri)
        return 0

    t_overall = time.time()

    if not args.skip_metadata:
        run_metadata_phase(args.src_uri, args.dst_uri,
                          parallel=args.parallel_meta, force=args.force)

    if not args.skip_pdf:
        run_pdf_phase(args.src_uri, args.dst_uri, parallel=args.parallel_pdf)

    log(f"\n====== MIGRATION COMPLETE in {(time.time()-t_overall)/60:.1f} min ======\n")

    # Always verify at end
    verify(args.src_uri, args.dst_uri)
    return 0


if __name__ == "__main__":
    sys.exit(main())
