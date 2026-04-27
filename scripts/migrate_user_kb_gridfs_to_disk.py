"""Migrate user-KB original binaries from legacy GridFS to local SSD.

Background
----------
Until 2026-04-27, ``backend/app/services/user_kb_service.create_document``
streamed every uploaded file (PDF / DOCX / XLSX / audio / …) into the
``ti-user-knowledge-base`` GridFS bucket (``fs.files`` + ``fs.chunks``).
GridFS gave us nothing local SSD didn't already give us, so the service
was switched to write straight to ``settings.user_kb_disk_root`` with the
GridFS path retained only as a read fallback for un-migrated rows.

This script does the one-shot backfill:

* Walks every ``documents`` row that has a ``gridfs_file_id`` set
  (regardless of whether ``local_path`` is also set — we re-verify),
* Streams the GridFS blob to disk under
  ``<root>/<user_id>/<oid><.ext>`` atomically (tmp + fsync + rename),
* SHA-256 verifies the on-disk bytes against ``documents.content_hash``
  so we never declare success without proof,
* Writes ``local_path`` back to the documents row,
* Optionally drops the GridFS object after verify (``--drop-gridfs``).

Idempotent: running twice is safe — already-migrated rows are skipped if
``local_path`` is set, the file exists, and its SHA-256 matches.

Usage
-----
    # Dry run — list what would be migrated, no writes
    python3 scripts/migrate_user_kb_gridfs_to_disk.py --dry-run

    # Migrate (writes disk + sets local_path; leaves GridFS intact for now)
    python3 scripts/migrate_user_kb_gridfs_to_disk.py

    # Migrate + drop the GridFS blobs once verified (after staging soak)
    python3 scripts/migrate_user_kb_gridfs_to_disk.py --drop-gridfs

    # Verify only — no writes; flag any row whose local_path is missing
    # or whose on-disk bytes don't match content_hash
    python3 scripts/migrate_user_kb_gridfs_to_disk.py --verify-only

Defaults to the same Mongo URI + DB the service uses (``settings.user_kb_mongo_uri``
+ ``settings.user_kb_mongo_db``); pass ``--mongo-uri`` / ``--db`` to override.
"""
from __future__ import annotations

import argparse
import hashlib
import logging
import os
import sys
import time
from pathlib import Path
from typing import Iterable

# Project root on sys.path so `from backend.app.config import …` works
# whether you launch from the repo root or anywhere else.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import gridfs  # noqa: E402
from pymongo import MongoClient  # noqa: E402

from backend.app.config import get_settings  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("user_kb_migrate")


# ── Disk helpers (mirror backend/app/services/user_kb_service.py) ───


def disk_path_for(root: Path, user_id: str, document_oid: str, original_filename: str) -> Path:
    ext = ""
    if "." in original_filename:
        ext = "." + original_filename.rsplit(".", 1)[-1].lower()
        if not ext[1:].isalnum() or len(ext) > 12:
            ext = ""
    return root / user_id / f"{document_oid}{ext}"


def relpath(root: Path, p: Path) -> str:
    try:
        return str(p.relative_to(root))
    except ValueError:
        return str(p)


def atomic_write(path: Path, data: bytes) -> None:
    """Write `data` to `path` atomically. Same semantics as the service."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    try:
        with open(tmp, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
        dir_fd = os.open(path.parent, os.O_DIRECTORY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    finally:
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass


def sha256_file(path: Path, chunk: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            buf = f.read(chunk)
            if not buf:
                break
            h.update(buf)
    return h.hexdigest()


# ── Main migration loop ─────────────────────────────────────────


def iter_targets(docs) -> Iterable[dict]:
    """Yield every documents row that has a gridfs_file_id set, oldest first."""
    cursor = docs.find(
        {"gridfs_file_id": {"$exists": True, "$ne": None}},
        {
            "_id": 1, "user_id": 1, "original_filename": 1,
            "file_size_bytes": 1, "content_hash": 1, "gridfs_file_id": 1,
            "local_path": 1,
        },
        no_cursor_timeout=True,
    ).sort("_id", 1)
    try:
        for row in cursor:
            yield row
    finally:
        cursor.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mongo-uri", default=None, help="Override Mongo URI")
    parser.add_argument("--db", default=None, help="Override DB name")
    parser.add_argument("--bucket", default=None, help="GridFS bucket (default: settings.user_kb_gridfs_bucket)")
    parser.add_argument("--disk-root", default=None, help="Override disk root")
    parser.add_argument("--dry-run", action="store_true", help="List, don't write")
    parser.add_argument(
        "--drop-gridfs", action="store_true",
        help="After successful disk verify, delete the GridFS blob",
    )
    parser.add_argument(
        "--verify-only", action="store_true",
        help="Only verify already-migrated rows; no new writes",
    )
    args = parser.parse_args()

    settings = get_settings()
    mongo_uri = args.mongo_uri or settings.user_kb_mongo_uri
    db_name = args.db or settings.user_kb_mongo_db
    bucket_name = args.bucket or settings.user_kb_gridfs_bucket
    disk_root = Path(args.disk_root or settings.user_kb_disk_root)

    log.info(
        "user_kb migration: mongo=%s db=%s bucket=%s root=%s dry=%s drop=%s verify_only=%s",
        mongo_uri, db_name, bucket_name, disk_root,
        args.dry_run, args.drop_gridfs, args.verify_only,
    )

    client = MongoClient(mongo_uri)
    db = client[db_name]
    docs = db[settings.user_kb_docs_collection]
    fs = gridfs.GridFS(db, collection=bucket_name)

    n_total = 0
    n_already = 0
    n_migrated = 0
    n_verified = 0
    n_dropped = 0
    n_errors = 0
    bytes_written = 0
    t0 = time.monotonic()

    for row in iter_targets(docs):
        n_total += 1
        oid = row["_id"]
        doc_id_str = str(oid)
        user_id = row.get("user_id") or ""
        filename = row.get("original_filename") or ""
        gridfs_id = row.get("gridfs_file_id")
        existing_local = row.get("local_path") or ""
        expected_size = int(row.get("file_size_bytes") or 0)
        expected_hash = row.get("content_hash") or ""

        if not user_id or not filename or gridfs_id is None:
            log.warning("skip %s — missing required fields", doc_id_str)
            n_errors += 1
            continue

        target = disk_path_for(disk_root, user_id, doc_id_str, filename)
        target_rel = relpath(disk_root, target)

        # Already-migrated check: row carries a local_path AND the file
        # exists AND its hash matches. Otherwise we re-do it.
        if existing_local:
            existing_abs = disk_root / existing_local if not Path(existing_local).is_absolute() else Path(existing_local)
            if existing_abs.exists():
                if not expected_hash:
                    log.info("ok (already migrated, no hash to verify): %s", doc_id_str)
                    n_already += 1
                    continue
                if sha256_file(existing_abs) == expected_hash:
                    n_verified += 1
                    log.info("ok (already migrated, hash verified): %s", doc_id_str)
                    if args.drop_gridfs and not args.dry_run:
                        try:
                            fs.delete(gridfs_id)
                            n_dropped += 1
                            docs.update_one(
                                {"_id": oid},
                                {"$set": {"gridfs_file_id": None}},
                            )
                        except Exception as e:
                            log.warning("gridfs drop failed for %s: %s", doc_id_str, e)
                    continue
                log.warning(
                    "hash mismatch for %s — re-migrating from GridFS",
                    doc_id_str,
                )

        if args.verify_only:
            log.warning("verify-only: %s has no usable local_path", doc_id_str)
            n_errors += 1
            continue

        if args.dry_run:
            log.info(
                "DRY: would migrate %s → %s (size=%d, gridfs=%s)",
                doc_id_str, target_rel, expected_size, gridfs_id,
            )
            n_migrated += 1
            continue

        # Pull the bytes from GridFS, hash, write, verify.
        try:
            gf = fs.get(gridfs_id)
            data = gf.read()
        except Exception as e:
            log.error("gridfs read failed for %s: %s", doc_id_str, e)
            n_errors += 1
            continue

        actual_size = len(data)
        actual_hash = hashlib.sha256(data).hexdigest()
        if expected_hash and actual_hash != expected_hash:
            # Refuse to overwrite if the source is corrupt — better to error
            # loudly than to write bad bytes and call it done.
            log.error(
                "GridFS bytes for %s don't match documents.content_hash "
                "(got %s, expected %s) — ABORT for this row",
                doc_id_str, actual_hash, expected_hash,
            )
            n_errors += 1
            continue
        if expected_size and actual_size != expected_size:
            log.warning(
                "size mismatch for %s — got %d, expected %d (continuing)",
                doc_id_str, actual_size, expected_size,
            )

        try:
            atomic_write(target, data)
        except Exception as e:
            log.error("disk write failed for %s → %s: %s", doc_id_str, target, e)
            n_errors += 1
            continue

        # Cross-check the on-disk file again — caught one disk-fault scenario
        # in the crawler PDF migration.
        if expected_hash and sha256_file(target) != expected_hash:
            log.error(
                "post-write hash mismatch for %s (file=%s) — leaving GridFS in place",
                doc_id_str, target,
            )
            n_errors += 1
            continue

        # Record the path on the row.
        try:
            docs.update_one(
                {"_id": oid},
                {"$set": {"local_path": target_rel}},
            )
        except Exception as e:
            log.error(
                "documents.update_one failed for %s after disk write: %s",
                doc_id_str, e,
            )
            n_errors += 1
            continue

        bytes_written += actual_size
        n_migrated += 1
        log.info(
            "migrated %s → %s (%.1f KiB)",
            doc_id_str, target_rel, actual_size / 1024.0,
        )

        if args.drop_gridfs:
            try:
                fs.delete(gridfs_id)
                n_dropped += 1
                docs.update_one(
                    {"_id": oid},
                    {"$set": {"gridfs_file_id": None}},
                )
            except Exception as e:
                log.warning("gridfs drop failed for %s: %s", doc_id_str, e)

    dt = time.monotonic() - t0
    log.info(
        "DONE in %.1fs — total=%d already=%d migrated=%d verified=%d "
        "dropped=%d errors=%d bytes=%d",
        dt, n_total, n_already, n_migrated, n_verified, n_dropped, n_errors,
        bytes_written,
    )
    return 0 if n_errors == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
