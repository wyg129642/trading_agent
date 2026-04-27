"""Extract text from crawled PDFs and store as pdf_text_md in Mongo.

Closes the biggest gap in the retrieval stack: crawler PDFs (~150k files across
alphapai/jinmen/gangtise/meritco/alphaengine/acecamp) were stored as GridFS
bytes + disk files but their contents were never extracted. As a result:

- Ticker tagging (enrich_tickers.py) only ever saw titles + structured fields,
  leaving ~52% of docs with empty ``_canonical_tickers`` and invisible to the
  per-stock hub aggregator.
- Vector ingest (kb_vector_ingest.py) skipped PDF-only docs entirely since it
  reads ``content_md`` / ``summary_md`` / etc. — so they never reached Milvus.

This script walks each (platform, collection) pair that stores PDFs, pulls the
file bytes (GridFS first, local disk fallback — same shape as pdf_storage.py),
parses them with opendataloader-pdf (Java; preserves headings, tables, reading
order), and writes back ``pdf_text_md`` plus audit fields.

Idempotent by filter: docs with ``pdf_text_md`` already set are skipped. Failed
parses record ``pdf_text_error`` so ``--retry-errors`` can re-try them later.

Usage
-----
  PYTHONPATH=. python scripts/extract_pdf_texts.py --list
  PYTHONPATH=. python scripts/extract_pdf_texts.py \\
      --platform alphapai --collection reports --limit 20
  PYTHONPATH=. python scripts/extract_pdf_texts.py \\
      --workers 3          # 3 targets in parallel
  PYTHONPATH=. python scripts/extract_pdf_texts.py \\
      --platform gangtise --retry-errors

Runs against the shared local crawler Mongo (`ta-mongo-crawl` :27018, MONGO_URI
env). Same data seen by both prod and staging — intentional, since the
extracted text benefits both worktrees.
"""
from __future__ import annotations

import argparse
import json
import logging
import logging.handlers
import os
import sys
import tempfile
import threading

# Clash on :7890 silently intercepts local-LAN TCP when NO_PROXY doesn't
# cover the target host (see infra_proxy memory). Remote Mongo lives on
# 192.168.31.0/24 — pin it here before importing pymongo so GridFS reads
# don't get routed through the proxy (observed: 10–270s/PDF via proxy,
# <1s direct).
os.environ["NO_PROXY"] = (
    os.environ.get("NO_PROXY", "")
    + ",127.0.0.1,localhost,192.168.31.0/24,192.168.31.176"
)
os.environ["no_proxy"] = os.environ["NO_PROXY"]
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from gridfs import GridFS
from pymongo import MongoClient

from backend.app.config import get_settings
from backend.app.services.user_kb_parser import (
    ParseError,
    _parse_pdf_with_pypdf,
)

logger = logging.getLogger("extract_pdf_texts")


# Stay comfortably under Mongo's 16 MB doc cap even with other fields present.
# Empirically the largest extracted markdown we've seen is ~498 KB (large
# gangtise/researches doc); 5 MB gives ~10x headroom for outlier filings
# (annual reports, long industry deep-dives) without bumping the doc cap.
# Truncation is now a true edge case — kb_search Phase A and Milvus ingest
# both consume the full text, so silent tail-truncation degrades retrieval.
MAX_TEXT_BYTES = 5_000_000

# PDFs per JVM invocation. opendataloader-pdf accepts a list of paths per
# convert() call, amortizing ~3 s of JVM startup. 8 balances speedup vs. how
# much progress is lost on interrupt.
DEFAULT_BATCH_SIZE = 8


# Persistent progress checkpoint: written atomically (rename-on-write) after
# every batch so an operator can `cat` for live status without grepping logs.
# Path resolves under the staging worktree; created on first write.
STATE_FILE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "logs", "pdf_parse_state.json",
)
_STATE_LOCK = threading.Lock()


def _update_state(target_key: str, **counters: Any) -> None:
    """Merge the latest counters for target_key into logs/pdf_parse_state.json.

    Atomic: writes a sibling .tmp file then renames. Tolerant of read failures
    (treats them as a fresh state). Worker-safe via STATE_LOCK so multiple
    --workers threads don't overwrite each other's updates.
    """
    with _STATE_LOCK:
        try:
            os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
            try:
                with open(STATE_FILE, "r", encoding="utf-8") as f:
                    state = json.load(f)
                if not isinstance(state, dict):
                    state = {}
            except (FileNotFoundError, json.JSONDecodeError):
                state = {}
            entry = state.get(target_key) or {}
            entry.update(counters)
            entry["updated_at"] = datetime.now(timezone.utc).isoformat()
            state[target_key] = entry
            tmp = STATE_FILE + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(state, f, ensure_ascii=False, indent=2, sort_keys=True)
            os.replace(tmp, STATE_FILE)
        except Exception as e:  # noqa: BLE001 — checkpoint is best-effort
            logger.debug("state.json update failed (%s): %s", target_key, e)


@dataclass(frozen=True)
class PdfTarget:
    platform: str
    collection: str
    uri_attr: str
    db_attr: str
    pdf_dir_attr: str


# Every (platform, collection) that stores PDF files. Keep aligned with
# backend/app/api/<platform>_db.py pdf_route definitions.
TARGETS: list[PdfTarget] = [
    PdfTarget("alphapai",    "reports",         "alphapai_mongo_uri",    "alphapai_mongo_db",    "alphapai_pdf_dir"),
    PdfTarget("jinmen",      "reports",         "jinmen_mongo_uri",      "jinmen_mongo_db",      "jinmen_pdf_dir"),
    PdfTarget("jinmen",      "oversea_reports", "jinmen_mongo_uri",      "jinmen_mongo_db",      "jinmen_pdf_dir"),
    PdfTarget("gangtise",    "researches",      "gangtise_mongo_uri",    "gangtise_mongo_db",    "gangtise_pdf_dir"),
    PdfTarget("gangtise",    "summaries",       "gangtise_mongo_uri",    "gangtise_mongo_db",    "gangtise_pdf_dir"),
    PdfTarget("meritco",     "forum",           "meritco_mongo_uri",     "meritco_mongo_db",     "meritco_pdf_dir"),
    PdfTarget("alphaengine", "china_reports",   "alphaengine_mongo_uri", "alphaengine_mongo_db", "alphaengine_pdf_dir"),
    PdfTarget("alphaengine", "foreign_reports", "alphaengine_mongo_uri", "alphaengine_mongo_db", "alphaengine_pdf_dir"),
    PdfTarget("acecamp",     "articles",        "acecamp_mongo_uri",     "acecamp_mongo_db",     "acecamp_pdf_dir"),
]


# --------------------------------------------------------------------- #
# PDF fetch — mirror the logic in backend/app/services/pdf_storage.py so
# a missing GridFS entry falls back to the pre-migration disk layout.
# --------------------------------------------------------------------- #
def _gridfs_filename(pdf_local_path: str, pdf_root: str) -> str:
    """Map a stored pdf_local_path to the GridFS filename.

    Migration convention (pdf_storage.py): filename starts with the platform
    root basename, e.g. ``alphapai_pdfs/2025-11/x.pdf``. Scrapers may record
    any of:
      - relative under root: ``alphapai_pdfs/2025-11/x.pdf`` or ``2025-11/x.pdf``
      - absolute on this host: ``/home/ygwang/crawl_data/alphapai_pdfs/...``
      - absolute on the scraper host: ``/mnt/share/ygwang/alphapai_pdfs/...``

    For absolute paths, locate ``root_name`` anywhere in the parts and slice
    from there — robust to a foreign mount prefix.
    """
    root = Path(pdf_root).resolve()
    root_name = root.name
    p = Path(pdf_local_path)

    if not p.is_absolute():
        if p.parts and p.parts[0] == root_name:
            return p.as_posix()
        return f"{root_name}/{p.as_posix()}"

    for i, part in enumerate(p.parts):
        if part == root_name:
            return Path(*p.parts[i:]).as_posix()
    return p.name


def _fetch_pdf_bytes(fs: GridFS, pdf_local_path: str, pdf_root: str) -> bytes:
    """Disk-first fetch with GridFS fallback.

    Order inverted from backend/app/services/pdf_storage.py: the read-path
    service serves one file per request to an interactive user and can tolerate
    a slow GridFS, but this batch loop does tens of thousands of reads and
    GridFS on remote Mongo is ~10–270 s per PDF (cold TCP + chunk streaming).
    Local disk (NFS/CIFS mount of the scraper host) serves the same bytes in
    well under a second. We always prefer disk when the file is present and
    fall through to GridFS only if disk is missing.
    """
    name = _gridfs_filename(pdf_local_path, pdf_root)
    root = Path(pdf_root).resolve()
    if "/" in name:
        target = (root.parent / name).resolve()
    else:
        target = (root / name).resolve()
    try:
        target.relative_to(root)
        disk_ok = True
    except ValueError:
        disk_ok = False

    if disk_ok and target.is_file():
        return target.read_bytes()

    gf = fs.find_one({"filename": name})
    if gf is not None:
        return gf.read()

    raise FileNotFoundError(f"not on disk and not in GridFS: {name}")


# --------------------------------------------------------------------- #
# Batched opendataloader call — one JVM startup for N PDFs. Per-item
# pypdf fallback handles PDFs the Java side skips (scanned / malformed).
# --------------------------------------------------------------------- #
def _parse_pdfs_batched(pdf_bytes_list: list[bytes]) -> list[tuple[str, str, str | None]]:
    """Return (markdown_text, parser_name, error_or_None) per input."""
    n = len(pdf_bytes_list)
    out: list[tuple[str, str, str | None]] = [("", "none", "unprocessed")] * n
    if n == 0:
        return out

    with tempfile.TemporaryDirectory(prefix="pdf_text_batch_") as tmp:
        tmp_path = Path(tmp)
        in_dir = tmp_path / "in"
        out_dir = tmp_path / "out"
        in_dir.mkdir()
        out_dir.mkdir()

        in_files: list[Path] = []
        for i, data in enumerate(pdf_bytes_list):
            p = in_dir / f"{i:04d}.pdf"
            p.write_bytes(data)
            in_files.append(p)

        try:
            import opendataloader_pdf  # type: ignore
            # image_output="off" skips image extraction + PNG writing, which
            # on dense research reports is the dominant cost (~60 s/PDF → ~5 s/PDF).
            # We're indexing text for retrieval — images don't help ticker tagging
            # or BM25/dense search, so dropping them is a clean trade.
            opendataloader_pdf.convert(
                input_path=[str(p) for p in in_files],
                output_dir=str(out_dir),
                format="markdown",
                quiet=True,
                image_output="off",
            )
            batch_ok = True
        except Exception as e:  # noqa: BLE001 — log and fall back per-item
            logger.warning("opendataloader batch failed (%s); falling back to pypdf", e)
            batch_ok = False

        for i, in_p in enumerate(in_files):
            text = ""
            if batch_ok:
                md = out_dir / f"{in_p.stem}.md"
                if not md.is_file():
                    matches = list(out_dir.rglob(f"{in_p.stem}.md"))
                    md = matches[0] if matches else None  # type: ignore[assignment]
                if md is not None and md.is_file():
                    text = md.read_text(encoding="utf-8", errors="replace")
            if text.strip():
                out[i] = (text, "opendataloader-pdf", None)
                continue
            # opendataloader skipped or produced empty → try pypdf for this one.
            try:
                text = _parse_pdf_with_pypdf(pdf_bytes_list[i])
                out[i] = (text, "pypdf", None)
            except ParseError as pe:
                out[i] = ("", "none", str(pe))
            except Exception as pe:  # noqa: BLE001 — last-line defense
                out[i] = ("", "none", f"{type(pe).__name__}: {pe}")
    return out


# --------------------------------------------------------------------- #
# Per-target driver.
# --------------------------------------------------------------------- #
def process_target(
    target: PdfTarget,
    settings: Any,
    *,
    limit: int | None,
    batch_size: int,
    dry_run: bool,
    retry_errors: bool,
    skip_missing_pdf: bool = True,
    id_mod: int = 0,
    id_rem: int = 0,
) -> tuple[int, int, int]:
    uri = getattr(settings, target.uri_attr)
    dbname = getattr(settings, target.db_attr)
    pdf_root = getattr(settings, target.pdf_dir_attr)

    client = MongoClient(uri, serverSelectionTimeoutMS=10_000)
    db = client[dbname]
    coll = db[target.collection]
    fs = GridFS(db)

    filt: dict[str, Any] = {
        "pdf_local_path": {"$exists": True, "$nin": [None, ""]},
        "pdf_text_md": {"$exists": False},
    }
    if not retry_errors:
        # Skip docs we've already tried and failed on, unless explicitly asked
        filt["pdf_text_error"] = {"$exists": False}
    if id_mod > 0:
        # Doc-id sharding: only process docs where _id % id_mod == id_rem.
        # Used to run multiple parallel processes against a single huge
        # collection (e.g. jinmen oversea_reports) without overlap.
        # Requires _id to be numeric — only enable for collections we know
        # use integer IDs (currently jinmen reports + oversea_reports).
        filt["$expr"] = {"$eq": [{"$mod": ["$_id", id_mod]}, id_rem]}

    # Pre-scan GridFS once per (DB) so we can skip docs whose PDF was never
    # downloaded — avoids polluting them with `pdf_text_error` rows for what
    # is really a scraper-side gap. jinmen oversea_reports alone has ~393 k
    # `pdf_local_path` claims but only ~13.7 k actual GridFS files; without
    # this pre-filter the script writes ~380 k useless error rows.
    valid_filenames: set[str] | None = None
    if skip_missing_pdf:
        try:
            t_scan = __import__("time").monotonic()
            valid_filenames = set(db["fs.files"].distinct("filename"))
            scan_dt = __import__("time").monotonic() - t_scan
            logger.info(
                "[%s/%s] gridfs prescan: %d filenames in %.1fs",
                target.platform, target.collection, len(valid_filenames), scan_dt,
            )
        except Exception as e:  # noqa: BLE001 — fall back to no prefilter
            logger.warning(
                "[%s/%s] gridfs prescan failed (%s); proceeding without skip-missing",
                target.platform, target.collection, e,
            )
            valid_filenames = None

    projection = {"_id": 1, "pdf_local_path": 1, "title": 1}

    try:
        total = coll.count_documents(filt) if not limit else min(
            limit, coll.count_documents(filt, limit=limit)
        )
    except Exception as e:  # noqa: BLE001 — count is advisory
        total = -1
        logger.warning("[%s/%s] count failed: %s", target.platform, target.collection, e)
    logger.info(
        "[%s/%s] candidates=%s pdf_root=%s",
        target.platform, target.collection, total if total >= 0 else "?", pdf_root,
    )

    # Explicit session keeps the cursor alive past the 30-min server-side idle
    # timeout on long backfills (no_cursor_timeout alone is not enough per
    # MongoDB 4.4+ — the session's idle timeout overrides it).
    session = client.start_session()
    cursor = coll.find(
        filt, projection=projection, no_cursor_timeout=True, session=session,
    ).batch_size(max(batch_size * 4, 32))
    if limit:
        cursor = cursor.limit(limit)

    ok = failed = skipped = missing = 0
    target_key = f"{target.platform}/{target.collection}"
    started_iso = datetime.now(timezone.utc).isoformat()
    _update_state(target_key, started_at=started_iso, ok=0, failed=0,
                  skipped=0, missing=0, status="running")

    def flush(batch: list[dict]) -> None:
        nonlocal ok, failed, skipped, missing
        if not batch:
            return
        fetched: list[tuple[dict, bytes | None, str | None]] = []
        for doc in batch:
            # Skip docs whose PDF was never downloaded (scraper-side gap), so we
            # don't pollute them with `pdf_text_error` rows. They'll be retried
            # automatically when this script next runs after the scraper fixes
            # the download.
            if valid_filenames is not None:
                gname = _gridfs_filename(doc["pdf_local_path"], pdf_root)
                if gname not in valid_filenames:
                    # disk fallback: per pdf_storage.py some PDFs still live on
                    # disk only (pre-migration). Cheap stat check before skip.
                    from pathlib import Path as _Path
                    rp = _Path(doc["pdf_local_path"])
                    if rp.is_absolute():
                        disk_target = rp
                    else:
                        disk_target = _Path(pdf_root).resolve().parent / gname
                    if not disk_target.is_file():
                        missing += 1
                        continue
            try:
                data = _fetch_pdf_bytes(fs, doc["pdf_local_path"], pdf_root)
                if not data:
                    fetched.append((doc, None, "empty bytes"))
                else:
                    fetched.append((doc, data, None))
            except FileNotFoundError:
                # Same intent as the prescan skip above — surface as `missing`,
                # not `failed`, and don't write a per-doc error.
                missing += 1
                continue
            except Exception as e:  # noqa: BLE001
                fetched.append((doc, None, f"fetch error: {e}"))

        parse_indices = [i for i, (_, b, _) in enumerate(fetched) if b is not None]
        parse_bytes = [fetched[i][1] for i in parse_indices]  # type: ignore[misc]
        results: dict[int, tuple[str, str, str | None]] = {}
        if parse_bytes:
            parsed = _parse_pdfs_batched(parse_bytes)  # type: ignore[arg-type]
            for idx, r in zip(parse_indices, parsed):
                results[idx] = r

        now = datetime.now(timezone.utc)
        for i, (doc, _bytes, fetch_err) in enumerate(fetched):
            if fetch_err:
                logger.warning("[%s/%s %s] fetch: %s",
                               target.platform, target.collection, doc["_id"], fetch_err)
                if not dry_run:
                    coll.update_one(
                        {"_id": doc["_id"]},
                        {"$set": {
                            "pdf_text_error": fetch_err,
                            "pdf_text_extracted_at": now,
                        }},
                    )
                failed += 1
                continue
            text, parser, err = results.get(i, ("", "none", "no result"))
            if err:
                logger.warning("[%s/%s %s] parse: %s",
                               target.platform, target.collection, doc["_id"], err)
                if not dry_run:
                    coll.update_one(
                        {"_id": doc["_id"]},
                        {"$set": {
                            "pdf_text_error": err,
                            "pdf_text_extracted_at": now,
                        }},
                    )
                failed += 1
                continue
            if not text.strip():
                logger.info("[%s/%s %s] empty extraction",
                            target.platform, target.collection, doc["_id"])
                if not dry_run:
                    coll.update_one(
                        {"_id": doc["_id"]},
                        {"$set": {
                            "pdf_text_error": "empty extraction",
                            "pdf_text_extracted_at": now,
                        }},
                    )
                skipped += 1
                continue
            logger.debug("[%s/%s %s] ok parser=%s len=%d",
                         target.platform, target.collection, doc["_id"], parser, len(text))

            encoded = text.encode("utf-8", errors="replace")
            truncated = False
            if len(encoded) > MAX_TEXT_BYTES:
                text = encoded[:MAX_TEXT_BYTES].decode("utf-8", errors="replace")
                truncated = True

            if not dry_run:
                update_set: dict[str, Any] = {
                    "pdf_text_md": text,
                    "pdf_text_len": len(text),
                    "pdf_parser": parser,
                    "pdf_text_extracted_at": now,
                }
                if truncated:
                    update_set["pdf_text_truncated"] = True
                coll.update_one(
                    {"_id": doc["_id"]},
                    {"$set": update_set, "$unset": {"pdf_text_error": ""}},
                )
            ok += 1

    try:
        batch: list[dict] = []
        for doc in cursor:
            batch.append(doc)
            if len(batch) >= batch_size:
                flush(batch)
                batch = []
                logger.info(
                    "[%s/%s] ok=%d failed=%d skipped=%d missing=%d",
                    target.platform, target.collection, ok, failed, skipped, missing,
                )
                _update_state(target_key, ok=ok, failed=failed, skipped=skipped,
                              missing=missing)
        flush(batch)
    finally:
        cursor.close()
        session.end_session()
        client.close()

    _update_state(target_key, ok=ok, failed=failed, skipped=skipped,
                  missing=missing, status="done",
                  finished_at=datetime.now(timezone.utc).isoformat())
    return ok, failed, skipped


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Extract text from crawled PDFs into Mongo pdf_text_md.",
    )
    ap.add_argument("--platform", help="Filter to one platform")
    ap.add_argument("--collection", help="Filter to one collection")
    ap.add_argument("--limit", type=int, help="Per-target cap on docs processed")
    ap.add_argument(
        "--batch-size", type=int, default=DEFAULT_BATCH_SIZE,
        help=f"PDFs per JVM invocation (default {DEFAULT_BATCH_SIZE})",
    )
    ap.add_argument(
        "--workers", type=int, default=1,
        help="Parallel targets — one thread per (platform, collection)",
    )
    ap.add_argument("--dry-run", action="store_true", help="Don't write to Mongo")
    ap.add_argument(
        "--retry-errors", action="store_true",
        help="Include docs previously marked pdf_text_error",
    )
    ap.add_argument(
        "--no-skip-missing-pdf", action="store_true",
        help=(
            "Disable the GridFS pre-scan that skips docs whose PDF was never "
            "downloaded. Default behaviour is to skip such docs silently "
            "(without writing pdf_text_error) so a later scraper backfill "
            "can re-trigger parsing automatically."
        ),
    )
    ap.add_argument("--list", action="store_true", help="List targets and exit")
    ap.add_argument(
        "--id-mod", type=int, default=0,
        help=(
            "Doc-id sharding modulus. Pair with --id-rem to run multiple "
            "parallel processes against a single big collection (e.g. jinmen "
            "oversea_reports). Requires numeric _id."
        ),
    )
    ap.add_argument(
        "--id-rem", type=int, default=0,
        help="Doc-id sharding remainder (0..id_mod-1).",
    )
    args = ap.parse_args()

    # Console + rotating file logger. The file handler keeps long backfill runs
    # auditable without the operator pinning a tmux pane open. Mirrors the
    # chat_debug.log pattern (50 MB × 10 backups) — see CLAUDE.md.
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    log_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs",
    )
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, "pdf_parse.log")
    fh = logging.handlers.RotatingFileHandler(
        log_path, maxBytes=50 * 1024 * 1024, backupCount=10, encoding="utf-8",
    )
    fh.setFormatter(logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s %(message)s",
    ))
    fh.setLevel(logging.INFO)
    logging.getLogger().addHandler(fh)
    logger.info("rotating file logger attached: %s", log_path)

    targets = [
        t for t in TARGETS
        if (not args.platform or t.platform == args.platform)
        and (not args.collection or t.collection == args.collection)
    ]

    if args.list:
        for t in targets:
            print(f"{t.platform}/{t.collection}  (db={t.db_attr}, root={t.pdf_dir_attr})")
        return 0

    if not targets:
        logger.error("No matching targets. Use --list to see options.")
        return 2

    settings = get_settings()
    total_ok = total_failed = total_skipped = 0

    def run_one(t: PdfTarget) -> tuple[PdfTarget, int, int, int]:
        try:
            ok, failed, skipped = process_target(
                t, settings,
                limit=args.limit,
                batch_size=args.batch_size,
                dry_run=args.dry_run,
                retry_errors=args.retry_errors,
                skip_missing_pdf=not args.no_skip_missing_pdf,
                id_mod=args.id_mod,
                id_rem=args.id_rem,
            )
            return t, ok, failed, skipped
        except Exception as e:  # noqa: BLE001
            logger.exception("[%s/%s] target failed: %s", t.platform, t.collection, e)
            return t, 0, 0, 0

    if args.workers > 1 and len(targets) > 1:
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            for t, ok, failed, skipped in pool.map(run_one, targets):
                total_ok += ok
                total_failed += failed
                total_skipped += skipped
                logger.info(
                    "DONE %s/%s ok=%d failed=%d skipped=%d",
                    t.platform, t.collection, ok, failed, skipped,
                )
    else:
        for t in targets:
            _, ok, failed, skipped = run_one(t)
            total_ok += ok
            total_failed += failed
            total_skipped += skipped
            logger.info(
                "DONE %s/%s ok=%d failed=%d skipped=%d",
                t.platform, t.collection, ok, failed, skipped,
            )

    logger.info(
        "ALL DONE ok=%d failed=%d skipped=%d",
        total_ok, total_failed, total_skipped,
    )
    return 0 if total_failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
