"""Re-fetch gangtise summaries that were stored with `content_truncated=True`
by hitting the S3 bypass endpoint (no daily quota).

Background: the original ``/application/summary/download`` endpoint has a
hard 60-doc/day quota; when exhausted it returns a ~500-char "试读" preview
which the scraper stored as `content_md` and flagged `content_truncated=True`.

The newer S3 bypass — ``/application/download/storage/s3/download/20002/<path>``
— is NOT quota-gated and returns the full HTML body. The scraper's
``_fetch_summary_text_via_s3`` was added 2026-04-21, but the ~52K truncated
docs from earlier runs never got refetched (the scraper only processes new
list pages, not old DB rows).

This one-shot walks the truncated set, fetches the full text via S3,
re-runs `_summary_text_to_md` + `_build_essence_md`, and writes back
`content_md` + `essence_md` + clears the truncated flag.

Usage:
    PYTHONPATH=. python3 scripts/refetch_gangtise_truncated.py [--limit N]
        [--dry-run] [--id sXXXX]
"""
from __future__ import annotations

import argparse
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from crawl.gangtise.scraper import (  # noqa: E402
    create_session,
    _fetch_summary_text_via_s3,
    _summary_text_to_md,
    _build_essence_md,
    _looks_truncated,
    _strip_html,
)
from pymongo import MongoClient, UpdateOne  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("refetch")

import json  # noqa: E402

CREDS_PATH = REPO / "crawl" / "gangtise" / "credentials.json"
MONGO_URI = "mongodb://127.0.0.1:27018/"
COLL = "gangtise-full.summaries"


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--limit", type=int, default=None,
                   help="cap docs processed (default: no limit)")
    p.add_argument("--dry-run", action="store_true", help="don't write to Mongo")
    p.add_argument("--id", default=None, help="only this _id (e.g. s4939435)")
    p.add_argument("--sleep", type=float, default=0.4,
                   help="sleep between S3 fetches (default 0.4s)")
    args = p.parse_args()

    creds = json.loads(CREDS_PATH.read_text())
    token = creds.get("token")
    if not token:
        log.error("no token in %s", CREDS_PATH)
        return 1
    session = create_session(token)

    cli = MongoClient(MONGO_URI)
    db, coll = COLL.split(".")
    c = cli[db][coll]

    if args.id:
        cursor = c.find({"_id": args.id})
    else:
        cursor = c.find({"content_truncated": True}, no_cursor_timeout=True).batch_size(50)

    n_seen = 0
    n_ok = 0
    n_no_url = 0
    n_no_text = 0
    n_still_short = 0
    n_skipped = 0
    n_essence = 0
    ops: list[UpdateOne] = []
    t0 = time.monotonic()

    try:
        for d in cursor:
            n_seen += 1
            if args.limit and n_seen > args.limit:
                break

            msg_texts = d.get("msg_text") or []
            url_path = None
            if isinstance(msg_texts, list):
                for mt in msg_texts:
                    if not isinstance(mt, dict):
                        continue
                    u = mt.get("url")
                    ext = (mt.get("extension") or "").lower()
                    # 100% of pre-2026-04-21 truncated docs use `.html`
                    # urls (research_data_s3/prs/...). Original scraper
                    # filter rejected them, leaving these stuck on the
                    # 500-char preview. S3 endpoint returns full HTML
                    # which `_summary_text_to_md` cleans up just fine.
                    if u and ext in ("", ".txt", ".html", ".htm"):
                        url_path = u
                        break
            if not url_path:
                n_no_url += 1
                continue

            try:
                raw = _fetch_summary_text_via_s3(session, token, url_path)
            except Exception as e:
                log.warning("[%s] S3 err: %s", d["_id"], e)
                raw = ""

            if not raw:
                n_no_text += 1
                continue

            new_content = _summary_text_to_md(raw)
            old_brief = _strip_html(d.get("list_item", {}).get("brief") or "")
            if not new_content:
                n_no_text += 1
                continue
            still_truncated = _looks_truncated(new_content, old_brief, full_text_ok=True)
            if still_truncated and len(new_content) <= len(d.get("content_md") or ""):
                n_still_short += 1
                continue
            new_essence = _build_essence_md(d.get("list_item", {}).get("essence"))
            updates = {
                "content_md": new_content,
                "content_truncated": still_truncated,
                "stats": {
                    "content_chars": len(new_content),
                    "brief_chars": len(old_brief),
                    "essence_chars": len(new_essence),
                    "truncated": still_truncated,
                    "refetched_at": datetime.now(timezone.utc).isoformat(),
                },
            }
            if new_essence:
                updates["essence_md"] = new_essence
                n_essence += 1

            if args.dry_run:
                log.info("[dry] %s  %d→%d chars  essence=%dc  truncated=%s",
                         d["_id"], len(d.get("content_md") or ""), len(new_content),
                         len(new_essence), still_truncated)
            else:
                ops.append(UpdateOne({"_id": d["_id"]}, {"$set": updates}))
            n_ok += 1

            if len(ops) >= 50:
                c.bulk_write(ops, ordered=False)
                ops.clear()

            if n_seen % 50 == 0:
                elapsed = time.monotonic() - t0
                rate = n_seen / max(elapsed, 1e-6)
                log.info("seen=%d ok=%d no_url=%d no_text=%d short=%d essence=%d  (%.1f docs/s)",
                         n_seen, n_ok, n_no_url, n_no_text, n_still_short, n_essence, rate)

            if args.sleep > 0:
                time.sleep(args.sleep)

        if ops and not args.dry_run:
            c.bulk_write(ops, ordered=False)
    finally:
        if not args.id:
            cursor.close()

    elapsed = time.monotonic() - t0
    log.info("DONE  seen=%d ok=%d no_url=%d no_text=%d still_short=%d essence_added=%d  elapsed=%.0fs",
             n_seen, n_ok, n_no_url, n_no_text, n_still_short, n_essence, elapsed)
    return 0


if __name__ == "__main__":
    sys.exit(main())
