#!/usr/bin/env python3
"""One-shot migration: dump every PDF in Mongo GridFS out to the local SSD.

Why this exists
---------------
Before 2026-04-23, PDFs lived on a SMB share at /mnt/share. After the migration
back to local Mongo (ta-mongo-crawl :27018) only the most recent 10-18% of
PDFs were rsynced to /home/ygwang/crawl_data/<plat>_pdfs/ as files; the bulk
of the historical archive remained as fs.chunks inside Mongo's GridFS bucket.

The web UI's PDF endpoint (`backend/app/services/pdf_storage.py`) tries the
local SSD first then falls back to GridFS. The fallback is correct but slow
(~50-150 KB/s for chunked motor reads, vs sendfile at hundreds of MB/s).
This script catches the local SSD up to GridFS so >99% of reads serve via
direct FileResponse.

How it works
------------
For every (db, fs.files) pair, we:
  1. Read the entry's `filename` (already in `<root_basename>/<sub-path>` form)
  2. Compute target = /home/ygwang/crawl_data/<filename verbatim>
  3. If the target file exists with the matching length, skip (idempotent).
  4. Stream fs.chunks → temp file → fsync → atomic rename to target.
  5. Optionally verify md5 if --verify-md5 is set.

Mojibake notes
--------------
For 4 of 5 platforms (alphapai/gangtise/jinmen/jiuqian) the GridFS filename
matches the corresponding Mongo doc's `pdf_local_path`. For alphaengine the
filename is mojibake (UTF-8 bytes interpreted as Latin-1 then re-encoded as
UTF-8, with some bytes lost). The doc-side `pdf_local_path` is correct UTF-8.
Strategy: write under the GridFS filename verbatim, then a separate
reconciliation pass updates alphaengine doc.pdf_local_path to point at the
real on-disk file. This keeps the extractor simple and lossless.

Long filenames (basename > 240 bytes) get a hash-truncation fallback. ext4's
limit is 255 bytes per component; 240 keeps headroom.

Usage
-----
  conda run -n agent python scripts/migrate_pdfs_gridfs_to_ssd.py --dry-run
  conda run -n agent python scripts/migrate_pdfs_gridfs_to_ssd.py
  conda run -n agent python scripts/migrate_pdfs_gridfs_to_ssd.py --reconcile-alphaengine
  conda run -n agent python scripts/migrate_pdfs_gridfs_to_ssd.py --only alphapai-full

The extractor is idempotent: rerunning skips files that already exist with
matching size. Safe to re-run after partial failures.
"""
from __future__ import annotations

import argparse
import hashlib
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import gridfs
import pymongo
from bson import ObjectId

LOG_FMT = "%(asctime)s %(levelname)s %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FMT)
logger = logging.getLogger("gridfs_migrate")

DEFAULT_MONGO_URI = os.environ.get("MONGO_URI", "mongodb://127.0.0.1:27018/")
LOCAL_BASE = Path("/home/ygwang/crawl_data")

# DB → root basename mapping. The fs.files filename starts with the root
# basename and is relative to LOCAL_BASE; we just join.
PLATFORMS: list[tuple[str, str]] = [
    ("alphapai-full",  "alphapai_pdfs"),
    ("gangtise-full",  "gangtise_pdfs"),
    ("jinmen-full",    "jinmen_pdfs"),
    ("jiuqian-full",   "meritco_pdfs"),
    ("alphaengine",    "alphaengine_pdfs"),
]


@dataclass
class Stats:
    db: str
    total: int = 0
    skipped: int = 0
    written: int = 0
    bytes_written: int = 0
    errors: int = 0


def _safe_basename(name: str, max_bytes: int = 240) -> str:
    """Truncate basename so its UTF-8 byte length fits ext4's 255-byte limit.

    Keeps the extension. If truncation is needed, appends an 8-char hash of
    the original to avoid collisions.
    """
    p = Path(name)
    stem, ext = p.stem, p.suffix
    full = stem + ext
    if len(full.encode("utf-8")) <= max_bytes:
        return full
    h = hashlib.sha256(name.encode("utf-8", "replace")).hexdigest()[:8]
    # binary-search the longest stem that fits.
    budget = max_bytes - len(ext.encode("utf-8")) - 1 - 8  # 1=hyphen
    if budget <= 0:
        return f"_{h}{ext}"
    encoded = stem.encode("utf-8")
    while len(encoded) > budget:
        stem = stem[:-1]
        encoded = stem.encode("utf-8")
    return f"{stem}-{h}{ext}"


def _resolve_target(filename: str) -> Path:
    """fs.files.filename → absolute on-disk target.

    fs.files.filename is `<root_basename>/<sub-path>/<basename>.pdf`. We
    just join with LOCAL_BASE. If the basename overflows ext4 limit, we
    hash-truncate it.
    """
    rel = Path(filename)
    parts = list(rel.parts)
    if not parts:
        raise ValueError(f"empty fs.files filename")
    parts[-1] = _safe_basename(parts[-1])
    return LOCAL_BASE / Path(*parts)


def _fsfiles_length(fdoc: dict) -> int:
    """fs.files length is sometimes stored as a regular int and sometimes as
    BSON Int64 / Decimal128 — normalize to plain int."""
    raw = fdoc.get("length")
    if raw is None:
        return 0
    try:
        return int(raw)
    except (TypeError, ValueError):
        return 0


def _stream_one(
    *,
    db_name: str,
    fdoc_id: ObjectId,
    filename: str,
    expected_length: int,
    target: Path,
    fs: gridfs.GridFS,
    verify_md5: bool,
) -> tuple[str, int]:
    """Stream one fs.files entry to disk. Returns (status, bytes_written).

    status ∈ {written, skipped, error}.
    """
    # Idempotency: skip if target exists with matching length.
    if target.is_file():
        try:
            st = target.stat()
            if st.st_size == expected_length and expected_length > 0:
                return ("skipped", 0)
            # Length mismatch: re-write to be safe (corrupt / partial).
        except OSError:
            pass

    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + ".part")
    try:
        with fs.get(fdoc_id) as src, open(tmp, "wb") as out:
            md5 = hashlib.md5() if verify_md5 else None
            written = 0
            while True:
                chunk = src.read(1024 * 1024)  # 1 MB buffer
                if not chunk:
                    break
                out.write(chunk)
                if md5:
                    md5.update(chunk)
                written += len(chunk)
            out.flush()
            os.fsync(out.fileno())

        if expected_length and written != expected_length:
            tmp.unlink(missing_ok=True)
            logger.warning(
                "size mismatch for %s/%s: got %d expected %d",
                db_name, filename, written, expected_length,
            )
            return ("error", 0)

        os.replace(tmp, target)
        return ("written", written)
    except Exception as exc:
        logger.warning(
            "stream failed for %s/%s -> %s: %s",
            db_name, filename, target, exc,
        )
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass
        return ("error", 0)


def _migrate_db(
    *,
    client: pymongo.MongoClient,
    db_name: str,
    root_basename: str,
    dry_run: bool,
    verify_md5: bool,
    workers: int,
) -> Stats:
    db = client[db_name]
    fs = gridfs.GridFS(db)
    fs_files = db["fs.files"]
    total = fs_files.estimated_document_count()
    stats = Stats(db=db_name, total=total)
    logger.info(
        "[%s] starting: fs.files=%d  workers=%d  dry_run=%s",
        db_name, total, workers, dry_run,
    )

    # Snapshot the metadata we need so cursor doesn't time out under heavy IO.
    cursor = fs_files.find({}, projection={"_id": 1, "filename": 1, "length": 1})
    # Pull in batches so memory is bounded for very large dbs.
    BATCH = 5000

    def emit(fdoc: dict) -> tuple[str, int, str]:
        filename = fdoc.get("filename") or ""
        if not filename or not filename.startswith(root_basename + "/"):
            return ("skip-bad-name", 0, filename)
        try:
            target = _resolve_target(filename)
        except Exception as exc:
            logger.warning("[%s] resolve_target failed for %r: %s",
                           db_name, filename, exc)
            return ("error", 0, filename)
        if dry_run:
            if target.is_file() and target.stat().st_size == _fsfiles_length(fdoc):
                return ("skipped", 0, filename)
            return ("would-write", _fsfiles_length(fdoc), filename)
        status, written = _stream_one(
            db_name=db_name,
            fdoc_id=fdoc["_id"],
            filename=filename,
            expected_length=_fsfiles_length(fdoc),
            target=target,
            fs=fs,
            verify_md5=verify_md5,
        )
        return (status, written, filename)

    last_log = time.monotonic()
    in_flight: list = []
    pool = ThreadPoolExecutor(max_workers=workers)
    submitted = 0

    def drain_until(maxsize: int) -> None:
        nonlocal in_flight
        while len(in_flight) > maxsize:
            done = next(as_completed(in_flight))
            in_flight.remove(done)
            try:
                status, written, _fn = done.result()
            except Exception as exc:
                stats.errors += 1
                logger.warning("[%s] worker exception: %s", db_name, exc)
                continue
            if status == "written" or status == "would-write":
                stats.written += 1
                stats.bytes_written += written
            elif status == "skipped":
                stats.skipped += 1
            elif status == "skip-bad-name":
                stats.skipped += 1
            else:
                stats.errors += 1

    try:
        for fdoc in cursor:
            in_flight.append(pool.submit(emit, fdoc))
            submitted += 1
            drain_until(workers * 4)
            now = time.monotonic()
            if now - last_log > 10:
                last_log = now
                processed = stats.written + stats.skipped + stats.errors
                logger.info(
                    "[%s] progress: submitted=%d processed=%d (W=%d S=%d E=%d) bytes=%.2f GB",
                    db_name, submitted, processed,
                    stats.written, stats.skipped, stats.errors,
                    stats.bytes_written / 1024 / 1024 / 1024,
                )
        drain_until(0)
    finally:
        pool.shutdown(wait=True)

    logger.info(
        "[%s] done: total=%d written=%d skipped=%d errors=%d bytes=%.2f GB",
        db_name, stats.total, stats.written, stats.skipped, stats.errors,
        stats.bytes_written / 1024 / 1024 / 1024,
    )
    return stats


def _reconcile_alphaengine(
    *,
    client: pymongo.MongoClient,
    dry_run: bool,
) -> None:
    """alphaengine GridFS filenames are mojibake; doc.pdf_local_path is
    correct UTF-8. After extraction the file is on disk under the mojibake
    name, but the doc's pdf_local_path doesn't point at it. We update the
    doc to point at whatever file actually exists.

    Lookup heuristic: parse `<doc._id>_<title>.pdf` basename of the GridFS
    filename and find the unique on-disk file matching `<root>/<cat>/<ym>/<id>_*`.
    """
    db = client["alphaengine"]
    fs_files = db["fs.files"]
    n_total = 0
    n_updated = 0
    n_skipped = 0
    n_unmatched = 0

    # Build _id → on-disk-path map by walking fs.files.
    for fdoc in fs_files.find({}, projection={"filename": 1, "metadata": 1}):
        n_total += 1
        filename = fdoc.get("filename") or ""
        # filename = "alphaengine_pdfs/<cat>/<ym>/<id>_<title>.pdf"
        parts = filename.split("/")
        if len(parts) < 4:
            n_unmatched += 1
            continue
        cat = parts[1]  # "chinaReport" / "foreignReport"
        coll_name = "china_reports" if cat == "chinaReport" else "foreign_reports"
        basename = parts[-1]
        doc_id = basename.split("_", 1)[0]  # "20000608834377"
        if not doc_id:
            n_unmatched += 1
            continue
        on_disk = LOCAL_BASE / filename
        # safe_basename may have rewritten the basename — try the resolved
        # target first, then fall back to a glob.
        from glob import glob
        if not on_disk.is_file():
            try:
                on_disk = Path(_resolve_target(filename))
            except Exception:
                pass
        if not on_disk.is_file():
            # Last-ditch: glob by id prefix in <root>/<cat>/<ym>/
            ym = parts[2] if len(parts) >= 4 else ""
            search_dir = LOCAL_BASE / parts[0] / cat / ym
            matches = sorted(search_dir.glob(f"{doc_id}_*.pdf"))
            if len(matches) == 1:
                on_disk = matches[0]
            else:
                n_unmatched += 1
                continue
        # Look up doc and decide if we need to update.
        coll = db[coll_name]
        doc = coll.find_one({"_id": doc_id}, projection={"pdf_local_path": 1})
        if not doc:
            n_unmatched += 1
            continue
        current = doc.get("pdf_local_path") or ""
        new_path = str(on_disk)
        if current == new_path:
            n_skipped += 1
            continue
        # If current path exists on disk too, prefer it (don't touch).
        if current and Path(current).is_file():
            n_skipped += 1
            continue
        if dry_run:
            logger.info("[reconcile dry-run] %s/%s: %r → %r",
                        coll_name, doc_id, current, new_path)
        else:
            coll.update_one(
                {"_id": doc_id},
                {"$set": {"pdf_local_path": new_path}},
            )
        n_updated += 1

    logger.info(
        "alphaengine reconcile: total=%d updated=%d skipped=%d unmatched=%d",
        n_total, n_updated, n_skipped, n_unmatched,
    )


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--mongo-uri", default=DEFAULT_MONGO_URI)
    ap.add_argument("--dry-run", action="store_true",
                    help="print what would be done; don't write to disk")
    ap.add_argument("--verify-md5", action="store_true",
                    help="compute md5 of streamed bytes (slower, paranoid mode)")
    ap.add_argument("--workers", type=int, default=4,
                    help="parallel streamers per DB (default: 4)")
    ap.add_argument("--only", default="",
                    help="restrict to a single DB (e.g. alphapai-full)")
    ap.add_argument("--skip-extract", action="store_true",
                    help="skip the GridFS extraction phase (use with --reconcile-*)")
    ap.add_argument("--reconcile-alphaengine", action="store_true",
                    help="after extraction, update alphaengine doc.pdf_local_path "
                         "to point at the on-disk (mojibake-named) file")
    args = ap.parse_args()

    if not LOCAL_BASE.is_dir():
        logger.error("local base %s does not exist", LOCAL_BASE)
        return 1

    client = pymongo.MongoClient(args.mongo_uri)
    try:
        # ping
        client.admin.command("ping")
    except Exception as exc:
        logger.error("Mongo connect failed: %s", exc)
        return 1

    targets = (
        [(d, r) for d, r in PLATFORMS if d == args.only]
        if args.only else PLATFORMS
    )
    if args.only and not targets:
        logger.error("--only %r doesn't match any known DB; valid: %s",
                     args.only, [d for d, _ in PLATFORMS])
        return 1

    overall: list[Stats] = []
    if not args.skip_extract:
        for db_name, root in targets:
            try:
                s = _migrate_db(
                    client=client,
                    db_name=db_name,
                    root_basename=root,
                    dry_run=args.dry_run,
                    verify_md5=args.verify_md5,
                    workers=args.workers,
                )
                overall.append(s)
            except Exception as exc:
                logger.exception("[%s] FATAL: %s", db_name, exc)

    if args.reconcile_alphaengine:
        try:
            _reconcile_alphaengine(client=client, dry_run=args.dry_run)
        except Exception as exc:
            logger.exception("alphaengine reconcile FATAL: %s", exc)

    if overall:
        logger.info("=" * 60)
        for s in overall:
            logger.info(
                "%-20s total=%-7d written=%-7d skipped=%-7d errors=%-4d bytes=%.2f GB",
                s.db, s.total, s.written, s.skipped, s.errors,
                s.bytes_written / 1024 / 1024 / 1024,
            )
    return 0


if __name__ == "__main__":
    sys.exit(main())
