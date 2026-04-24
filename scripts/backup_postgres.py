#!/usr/bin/env python3
"""Daily backup of the trading_agent Postgres DB.

- pg_dump runs inside the ta-postgres-dev container (no host pg_dump required)
- local copy:  /home/ygwang/backups/postgres/trading_agent_YYYY-MM-DD_HHMM.sql.gz
- remote copy: GridFS bucket `pg_backup` inside Mongo
               mongodb://u_spider@192.168.31.176:35002/ti-user-knowledge-base
- retention: local 30 days, remote 90 days

Install via cron:
    10 3 * * *  /usr/bin/python3 /home/ygwang/trading_agent/scripts/backup_postgres.py >> /home/ygwang/backups/postgres/backup.log 2>&1
"""
from __future__ import annotations

import datetime as dt
import gzip
import os
import socket
import subprocess
import sys
from pathlib import Path

import gridfs
from pymongo import MongoClient

PG_CONTAINER = "ta-postgres-dev"
PG_USER = "trading_agent"
PG_DB = "trading_agent"

# Tables excluded from backup — these are either raw mirrors of the remote
# MongoDB crawler data (already backed up there) or analytical outputs that
# can be regenerated from the raw corpus. Dropping them shrinks the backup
# from ~4.5 GB → ~50 MB, making daily-to-remote realistic.
#
# Keeps: all chat_*, users, user_preferences, user_news_read, user_sources,
# user_favorites, watchlists, watchlist_items, stock_predictions*,
# alert_rules, kb_folders, api_keys, alphapai_sync_state, alphapai_digests,
# sources, source_health, portfolio_scan_results, and anything new that's
# added later (whitelist-by-exclusion is safer than by inclusion).
EXCLUDE_TABLES = [
    "alphapai_articles",
    "alphapai_comments",
    "alphapai_roadshows_cn",
    "alphapai_roadshows_us",
    "jiuqian_forum",
    "jiuqian_minutes",
    "jiuqian_wechat",
    "news_items",
    "filter_results",
    "analysis_results",
    "research_reports",
    "signal_evaluations",
    "token_usage",
]

LOCAL_DIR = Path("/home/ygwang/backups/postgres")
LOCAL_RETENTION_DAYS = 30

REMOTE_URI = "mongodb://u_spider:prod_X5BKVbAc@192.168.31.176:35002/?authSource=admin"
REMOTE_DB = "ti-user-knowledge-base"
REMOTE_BUCKET = "pg_backup"
REMOTE_RETENTION_DAYS = 90

os.environ["NO_PROXY"] = (
    os.environ.get("NO_PROXY", "") + ",192.168.31.176,127.0.0.1,localhost"
)


def log(msg: str) -> None:
    stamp = dt.datetime.now().isoformat(timespec="seconds")
    print(f"[{stamp}] {msg}", flush=True)


def dump_postgres(out_path: Path) -> int:
    """Stream pg_dump out of the container directly into a gzip file."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "docker", "exec", PG_CONTAINER,
        "pg_dump",
        "-U", PG_USER,
        "-d", PG_DB,
        "--no-owner",
        "--no-privileges",
    ]
    for t in EXCLUDE_TABLES:
        cmd += ["--exclude-table-data", t]
    log(f"running: {' '.join(cmd)}")
    # NOTE: `subprocess.run(stdout=gz)` bypasses the GzipFile wrapper and
    # writes plaintext directly to the underlying fd, so we stream via PIPE
    # and gzip the bytes ourselves.
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        with gzip.open(out_path, "wb", compresslevel=6) as gz:
            while True:
                chunk = proc.stdout.read(1 << 16)
                if not chunk:
                    break
                gz.write(chunk)
    finally:
        proc.stdout.close()
        proc.wait()
    if proc.returncode != 0:
        err = proc.stderr.read().decode("utf-8", errors="replace")[:2000]
        out_path.unlink(missing_ok=True)
        raise RuntimeError(f"pg_dump exit={proc.returncode}: {err}")
    return out_path.stat().st_size


def table_row_counts() -> dict[str, int]:
    """Snapshot row counts per user table so the metadata is self-describing."""
    sql = (
        "SELECT tablename FROM pg_tables WHERE schemaname='public' "
        "ORDER BY tablename"
    )
    r = subprocess.run(
        ["docker", "exec", PG_CONTAINER, "psql", "-U", PG_USER, "-d", PG_DB,
         "-Atc", sql],
        capture_output=True, check=True,
    )
    tables = [t for t in r.stdout.decode().splitlines() if t]
    counts: dict[str, int] = {}
    for t in tables:
        try:
            rr = subprocess.run(
                ["docker", "exec", PG_CONTAINER, "psql", "-U", PG_USER, "-d", PG_DB,
                 "-Atc", f"SELECT count(*) FROM {t}"],
                capture_output=True, check=True,
            )
            counts[t] = int(rr.stdout.decode().strip() or 0)
        except Exception as e:
            counts[t] = -1
    return counts


def upload_remote(local_path: Path, counts: dict[str, int]) -> str:
    client = MongoClient(REMOTE_URI, serverSelectionTimeoutMS=8000)
    client.admin.command("ping")
    db = client[REMOTE_DB]
    bucket = gridfs.GridFSBucket(db, bucket_name=REMOTE_BUCKET)
    meta = {
        "kind": "pg_backup",
        "db_name": PG_DB,
        "backup_at": dt.datetime.now(dt.timezone.utc),
        "source_host": socket.gethostname(),
        "size_bytes": local_path.stat().st_size,
        "sha_prefix": "",  # placeholder; filled below
        "row_counts": counts,
    }
    import hashlib
    h = hashlib.sha256()
    with local_path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    meta["sha_prefix"] = h.hexdigest()[:16]

    with local_path.open("rb") as f:
        fid = bucket.upload_from_stream(local_path.name, f, metadata=meta)
    client.close()
    return str(fid)


def prune_local() -> int:
    cutoff = dt.datetime.now() - dt.timedelta(days=LOCAL_RETENTION_DAYS)
    removed = 0
    for p in LOCAL_DIR.glob("trading_agent_*.sql.gz"):
        if dt.datetime.fromtimestamp(p.stat().st_mtime) < cutoff:
            p.unlink(missing_ok=True)
            removed += 1
    return removed


def prune_remote() -> int:
    client = MongoClient(REMOTE_URI, serverSelectionTimeoutMS=8000)
    db = client[REMOTE_DB]
    bucket = gridfs.GridFSBucket(db, bucket_name=REMOTE_BUCKET)
    files = db[f"{REMOTE_BUCKET}.files"]
    cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=REMOTE_RETENTION_DAYS)
    stale = list(files.find(
        {"metadata.kind": "pg_backup", "metadata.backup_at": {"$lt": cutoff}},
        {"_id": 1},
    ))
    for rec in stale:
        try:
            bucket.delete(rec["_id"])
        except Exception as e:
            log(f"prune_remote: failed to delete {rec['_id']}: {e}")
    client.close()
    return len(stale)


def main() -> int:
    stamp = dt.datetime.now().strftime("%Y-%m-%d_%H%M")
    out = LOCAL_DIR / f"trading_agent_{stamp}.sql.gz"
    try:
        size = dump_postgres(out)
        log(f"dumped {out.name} size={size/1024/1024:.2f} MB")
        counts = table_row_counts()
        log(f"tables: {len(counts)}, chat_messages={counts.get('chat_messages', '?')}")
        fid = upload_remote(out, counts)
        log(f"remote GridFS upload ok, file_id={fid}")
        rm_l = prune_local()
        rm_r = prune_remote()
        log(f"pruned local={rm_l} remote={rm_r}")
        return 0
    except Exception as e:
        log(f"BACKUP FAILED: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
