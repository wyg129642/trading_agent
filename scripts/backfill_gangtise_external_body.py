"""Slowly backfill gangtise/researches and gangtise/chief_opinions docs that
were soft-deleted as `external_link_only`. Anonymous fetch via
crawl/wechat_anon_fetcher.py — no credentials, conservative pacing.

Run small batches per cron tick. On block, abort the batch and let the next
tick try again later (so wechat's IP rate-limiter can cool).

For each successfully fetched doc:
  - $set content_md, brief_md (truncated), release_time_ms (if upstream had it)
  - $unset deleted, _deleted_at, _deleted_reason, _low_value_chars_at_delete,
          platform_no_body
  - $set _backfilled_from_weixin=True, _backfilled_at=<utc>

Usage:
    PYTHONPATH=. python3 scripts/backfill_gangtise_external_body.py            # dry-run
    PYTHONPATH=. python3 scripts/backfill_gangtise_external_body.py --apply --limit 30
    PYTHONPATH=. python3 scripts/backfill_gangtise_external_body.py --apply --collection chief_opinions
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from pymongo import MongoClient

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "crawl"))
from wechat_anon_fetcher import (  # noqa: E402
    fetch_article_anon,
    WechatBlocked,
    WechatNotFound,
    WechatTransient,
    WechatUnknown,
)

logger = logging.getLogger("backfill_gangtise_external_body")
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://127.0.0.1:27018/")


def iter_candidates(coll, collection_name: str, limit: int):
    """Yield docs that need backfill, oldest-deleted-first (FIFO)."""
    if collection_name == "researches":
        flt = {"deleted": True, "_deleted_reason": "external_link_only",
               "list_item.file": {"$regex": "^http://mp.weixin"}}
        proj = {"_id": 1, "title": 1, "list_item.file": 1, "release_time_ms": 1}
        url_field = "list_item.file"
    elif collection_name == "chief_opinions":
        flt = {"deleted": True, "_deleted_reason": "external_link_only",
               "parsed_msg.url": {"$regex": "mp\\.weixin\\.qq\\.com"}}
        proj = {"_id": 1, "title": 1, "parsed_msg.url": 1, "release_time_ms": 1}
        url_field = "parsed_msg.url"
    else:
        raise ValueError(f"Unknown collection: {collection_name}")

    cursor = coll.find(flt, proj).sort("_deleted_at", 1).limit(limit)
    for d in cursor:
        # Walk dotted path to extract URL
        url = d
        for part in url_field.split("."):
            url = (url or {}).get(part) if isinstance(url, dict) else None
        if isinstance(url, str) and url.startswith(("http://", "https://")):
            yield d, url


def restore_doc(coll, doc_id, parsed: dict, dry_run: bool) -> bool:
    """Write fetched body back to mongo, un-deleting the doc."""
    md = (parsed.get("content_md") or "").strip()
    if len(md) < 100:
        # Below the visibility threshold — keep deleted, just record we tried.
        if dry_run:
            return False
        coll.update_one({"_id": doc_id}, {"$set": {
            "_backfill_attempted_at": datetime.now(timezone.utc),
            "_backfill_result": "body_too_short",
            "_backfill_chars": len(md),
        }})
        return False

    set_fields = {
        "content_md": md,
        "brief_md": md[:500],
        "_backfilled_from_weixin": True,
        "_backfilled_at": datetime.now(timezone.utc),
        "_backfill_chars": len(md),
    }
    rt_ms = parsed.get("release_time_ms")
    if rt_ms:
        set_fields["release_time_ms_weixin"] = rt_ms
    if dry_run:
        return True
    coll.update_one(
        {"_id": doc_id},
        {"$set": set_fields,
         "$unset": {
             "deleted": "",
             "_deleted_at": "",
             "_deleted_reason": "",
             "_low_value_chars_at_delete": "",
             "platform_no_body": "",
         }},
    )
    return True


def run_one_batch(coll, coll_name: str, *, limit: int, dry_run: bool) -> dict:
    stats = {"tried": 0, "ok": 0, "too_short": 0, "blocked": 0,
             "not_found": 0, "transient": 0, "unknown": 0}
    blocked_at_idx = None

    for idx, (d, url) in enumerate(iter_candidates(coll, coll_name, limit), 1):
        stats["tried"] += 1
        try:
            parsed = fetch_article_anon(url)
        except WechatBlocked as e:
            logger.warning("[%s/%s %s] BLOCKED: %s — abort batch",
                           coll.database.name, coll_name, d["_id"], e)
            stats["blocked"] += 1
            blocked_at_idx = idx
            break
        except WechatNotFound as e:
            stats["not_found"] += 1
            if not dry_run:
                coll.update_one({"_id": d["_id"]}, {"$set": {
                    "_backfill_result": "wechat_not_found",
                    "_backfill_attempted_at": datetime.now(timezone.utc),
                }})
            logger.info("[%s 404] %s :: %s",
                        d["_id"], (d.get("title") or "")[:50], e)
            continue
        except WechatTransient as e:
            stats["transient"] += 1
            logger.info("[%s TRANS] %s", d["_id"], e)
            continue
        except WechatUnknown as e:
            stats["unknown"] += 1
            logger.info("[%s UNK] %s", d["_id"], e)
            continue

        ok = restore_doc(coll, d["_id"], parsed, dry_run)
        if ok:
            stats["ok"] += 1
            logger.info("[%s ok] %s :: chars=%d",
                        d["_id"], (d.get("title") or "")[:50],
                        len(parsed.get("content_md") or ""))
        else:
            stats["too_short"] += 1

    if blocked_at_idx is not None:
        stats["aborted_after"] = blocked_at_idx
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true",
                        help="Actually write to Mongo. Default is dry-run.")
    parser.add_argument("--limit", type=int, default=30,
                        help="Max docs per cron tick (default 30, conservative).")
    parser.add_argument("--collection", choices=("researches", "chief_opinions", "both"),
                        default="both", help="Which collection to backfill.")
    parser.add_argument("--mongo-uri", default=MONGO_URI)
    args = parser.parse_args()

    client = MongoClient(args.mongo_uri, serverSelectionTimeoutMS=5_000)
    db = client["gangtise-full"]
    targets = ["researches", "chief_opinions"] if args.collection == "both" else [args.collection]

    logger.info("backfill mode=%s limit=%d targets=%s",
                "APPLY" if args.apply else "DRY-RUN", args.limit, targets)

    grand = {}
    for cname in targets:
        coll = db[cname]
        logger.info("[%s] starting batch...", cname)
        stats = run_one_batch(coll, cname, limit=args.limit, dry_run=not args.apply)
        logger.info("[%s] done: %s", cname, stats)
        grand[cname] = stats
        if stats.get("blocked"):
            logger.warning("[%s] aborted on block — skipping further targets this tick", cname)
            break
        # Brief pause between collections to be kind
        time.sleep(10)

    print(f"\nFINAL: {grand}")


if __name__ == "__main__":
    main()
