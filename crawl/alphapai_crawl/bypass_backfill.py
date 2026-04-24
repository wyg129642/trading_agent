#!/usr/bin/env python3
"""Bypass backfill for AlphaPai roadshow content.

Background:
  - List endpoint `reading/roadshow/summary/list` enforces 100/day quota; over
    the limit returns `hasPermission:False` + ~220-char preview.
  - Detail endpoint `reading/roadshow/summary/detail?id=X` returns the real
    AI summary (`aiSummary.content` 3-18k chars) + US earnings transcript
    (`usSummary.content` 20k-140k chars) with the SAME 100/day quota, but
    **counted separately from the list quota** and NOT tied to the list-side
    `hasPermission:False` outcome. That's the 2026-04-22 bypass: each day we
    can detail-fetch ~100 previously-unseen records.

This script walks every roadshow document missing full content and retries
the detail endpoint forever, respecting both rate-limit layers:

  - `code=500020`  ── short-term throttle after ~345KB cumulative response
                      body. Cools down in ~90s. We sleep 90-120s and retry.
  - `code=400000`  ── daily view quota exhausted. Sleep until next CST
                      midnight + 2min and retry.

Records with cur_len <500 AND `content_truncated=True` are treated as
per-ID permanent locks (tested 2026-04-22: returned 400000 for 20
consecutive probes even with fresh daily quota headroom on other IDs).
We still try them once per day but stop early if the first 10 consecutive
calls on this category all return 400000.

Usage:
  python3 bypass_backfill.py                  # run until all records attempted
  python3 bypass_backfill.py --max 500        # cap at 500 successful fetches
  python3 bypass_backfill.py --log-every 5    # progress log cadence
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Ensure localhost Mongo connection doesn't go through Clash proxy
for _k in ('http_proxy', 'https_proxy', 'HTTP_PROXY', 'HTTPS_PROXY'):
    os.environ.pop(_k, None)

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))
sys.path.insert(0, str(_HERE.parent))

from pymongo import MongoClient  # noqa: E402
from scraper import (  # noqa: E402
    _load_token_from_file,
    create_session,
    api_call,
    _extract_roadshow_content,
)


CST = timezone(timedelta(hours=8))


def sleep_until_next_cst_midnight(extra_seconds: int = 120) -> None:
    """Block until 00:00 CST + extra_seconds. Needed when daily quota is exhausted."""
    now_cst = datetime.now(CST)
    tomorrow = (now_cst + timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0,
    )
    delta = (tomorrow - now_cst).total_seconds() + extra_seconds
    logging.info(
        "DAILY_QUOTA_EXHAUSTED: sleeping %.1f minutes until %s (CST)",
        delta / 60, tomorrow.isoformat(),
    )
    # Wake up every 5 min to log the wait so monitoring can see we're alive.
    end = time.time() + delta
    while time.time() < end:
        remaining = end - time.time()
        logging.info("... still sleeping, %.1f min to go", remaining / 60)
        time.sleep(min(300, remaining))


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument('--max', type=int, default=0,
                   help='Max successful fetches before exit (0 = unlimited)')
    p.add_argument('--batch', type=int, default=5,
                   help='Fetches per batch before short rest')
    p.add_argument('--batch-rest', type=int, default=60,
                   help='Seconds to rest between batches')
    p.add_argument('--call-interval', type=float, default=2.0,
                   help='Seconds between individual calls within a batch')
    p.add_argument('--cooldown-500020', type=int, default=120,
                   help='Seconds to sleep after hitting 500020')
    p.add_argument('--max-consecutive-quota', type=int, default=10,
                   help='Consecutive 400000 responses before deciding quota is exhausted')
    p.add_argument('--min-gain-chars', type=int, default=500,
                   help='Ignore updates where new content gains <N chars')
    p.add_argument('--log-file', default=str(_HERE.parent.parent / 'logs' / 'alphapai_bypass_backfill.log'),
                   help='Log file path')
    p.add_argument('--log-every', type=int, default=1,
                   help='Log every N fetches (1 = every)')
    p.add_argument('--mongo-uri', default='mongodb://localhost:27017')
    p.add_argument('--mongo-db', default='alphapai')
    p.add_argument('--collection', default='roadshows')
    p.add_argument('--skip-perm-locked', action='store_true', default=True,
                   help='Skip content_truncated=True records with cur_len <500 (persistent locks)')
    p.add_argument('--retry-perm-locked', action='store_true',
                   help='Override --skip-perm-locked and try them anyway')
    return p.parse_args()


def setup_logging(path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    fmt = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        level=logging.INFO,
        format=fmt,
        handlers=[
            logging.FileHandler(path, encoding='utf-8'),
            logging.StreamHandler(sys.stdout),
        ],
    )


# Graceful shutdown
_shutdown = False
def _sigterm(_signum, _frame):
    global _shutdown
    _shutdown = True
    logging.warning("Shutdown signal received — will exit after current record")
signal.signal(signal.SIGTERM, _sigterm)
signal.signal(signal.SIGINT, _sigterm)


def collect_candidates(col, skip_perm_locked: bool) -> list[dict]:
    """Return list of records to attempt, in priority order.

    Priority:
      1. legacy records (no content_truncated flag) sorted by cur_len desc
         (longer cur_len → more likely to have rich usSummary waiting)
      2. truncated records — only if --retry-perm-locked, else excluded
    """
    query = {
        'raw_id': {'$exists': True, '$ne': None},
        '_bypass_updated_at': {'$exists': False},
    }
    if skip_perm_locked:
        # Skip already-known permanent locks (content_truncated=True records
        # with very short cur_len)
        query['$or'] = [
            {'content_truncated': {'$exists': False}},
            {'content_truncated': False},
            {'$expr': {'$gt': [{'$strLenCP': {'$ifNull': ['$content', '']}}, 500]}},
        ]

    # Rank by cur_len desc so we try most-promising first.
    pipeline = [
        {'$match': query},
        {'$addFields': {'_cur_len': {'$strLenCP': {'$ifNull': ['$content', '']}}}},
        {'$sort': {'_cur_len': -1}},
        {'$project': {'raw_id': 1, 'title': 1, 'content': 1,
                     'content_truncated': 1, '_cur_len': 1, 'crawled_at': 1}},
    ]
    return list(col.aggregate(pipeline))


def main():
    args = parse_args()
    setup_logging(args.log_file)

    logging.info("=" * 70)
    logging.info("AlphaPai roadshow bypass backfill — START")
    logging.info("  batch=%d  batch_rest=%ds  call_interval=%.1fs  max=%d",
                 args.batch, args.batch_rest, args.call_interval, args.max)

    client = MongoClient(args.mongo_uri)
    col = client[args.mongo_db][args.collection]

    token = _load_token_from_file()
    sess = create_session(token)
    sess.trust_env = False
    sess.proxies = {'http': None, 'https': None}

    skip_locked = args.skip_perm_locked and not args.retry_perm_locked
    candidates = collect_candidates(col, skip_perm_locked=skip_locked)
    logging.info("Candidates: %d  (skip_perm_locked=%s)", len(candidates), skip_locked)
    logging.info("  Pre-backfill stats: total=%d  bypass_updated=%d  truncated=%d  legacy=%d",
                 col.count_documents({}),
                 col.count_documents({'_bypass_updated_at': {'$exists': True}}),
                 col.count_documents({'content_truncated': True}),
                 col.count_documents({'content_truncated': {'$exists': False}}))

    stats = {'attempted': 0, 'improved': 0, 'no_gain': 0,
             'quota_blocked_400': 0, 'throttled_500020': 0,
             'other_err': 0, 'total_chars_gained': 0,
             'quota_cycles': 0, 'throttle_cycles': 0, 'started_at': time.time()}
    batch_count = 0
    consecutive_quota = 0

    for idx, d in enumerate(candidates):
        if _shutdown:
            break
        if args.max and stats['improved'] >= args.max:
            logging.info("Hit --max %d, exiting", args.max)
            break

        rid = d.get('raw_id')
        cur = d.get('_cur_len', 0)
        title_short = (d.get('title') or '')[:60]

        # Rate-limit loop: keep retrying this record until we get 200000 OR
        # 500020/400000. After resolving, move on.
        attempts_this_record = 0
        while True:
            if _shutdown:
                break
            attempts_this_record += 1
            try:
                r = api_call(sess, 'GET',
                             f'reading/roadshow/summary/detail?id={rid}') or {}
            except Exception as e:
                stats['other_err'] += 1
                logging.warning("  request error: %s", e)
                time.sleep(5)
                if attempts_this_record >= 3:
                    break
                continue

            code = r.get('code')

            if code == 200000:
                stats['attempted'] += 1
                consecutive_quota = 0
                detail = r.get('data') or {}
                main_md, seg_md = _extract_roadshow_content(detail)
                new_len = len(main_md or '')
                gain = new_len - cur

                if gain >= args.min_gain_chars:
                    stats['improved'] += 1
                    stats['total_chars_gained'] += gain
                    update = {
                        'content': main_md,
                        'content_truncated': False,
                        '_bypass_updated_at': datetime.now(timezone.utc),
                        'detail': detail,
                    }
                    if seg_md:
                        update['segments_md'] = seg_md
                    col.update_one({'_id': d['_id']}, {'$set': update})
                    if stats['improved'] % args.log_every == 0 or gain > 20000:
                        elapsed = time.time() - stats['started_at']
                        logging.info(
                            "✓ [%d/%d] %+6d chars  (%d→%d)  tot=%.1fk  rate=%.1f/min  · %s",
                            stats['improved'], len(candidates),
                            gain, cur, new_len,
                            stats['total_chars_gained'] / 1000,
                            stats['improved'] / max(elapsed / 60, 1e-6),
                            title_short,
                        )
                else:
                    stats['no_gain'] += 1
                    # Mark that we've checked it so next run skips.
                    col.update_one({'_id': d['_id']}, {'$set': {
                        '_bypass_updated_at': datetime.now(timezone.utc),
                        'detail': detail,
                    }})
                break  # move on to next record

            if code == 500020:
                # Short-term throttle. Cool down and re-try THIS record.
                stats['throttle_cycles'] += 1
                logging.info("→ 500020 throttle, sleeping %ds  (batch_count=%d)",
                             args.cooldown_500020, batch_count)
                time.sleep(args.cooldown_500020)
                batch_count = 0  # throttle counts as a forced rest
                continue  # retry same rid

            if code == 400000:
                stats['quota_blocked_400'] += 1
                consecutive_quota += 1
                if consecutive_quota >= args.max_consecutive_quota:
                    # Today's daily quota exhausted. Sleep to next CST midnight.
                    stats['quota_cycles'] += 1
                    logging.info(
                        "→ 400000 ×%d consecutive — daily quota exhausted. "
                        "Session stats: improved=%d  chars_gained=%.1fk  uptime=%.1fh",
                        consecutive_quota, stats['improved'],
                        stats['total_chars_gained'] / 1000,
                        (time.time() - stats['started_at']) / 3600,
                    )
                    sleep_until_next_cst_midnight(extra_seconds=120)
                    consecutive_quota = 0
                    continue  # retry same rid after midnight
                # Not yet at threshold — this might be a per-ID lock. Skip this record.
                logging.info("  [%d/%d] 400000 (consec=%d) · skipping · %s",
                             idx, len(candidates), consecutive_quota, title_short)
                break

            # Unknown non-success code — log and move on
            stats['other_err'] += 1
            logging.warning("  unknown code=%s msg=%s · %s",
                            code, (r.get('message') or '')[:30], title_short)
            break

        batch_count += 1
        if batch_count >= args.batch:
            # Brief batch rest to avoid tripping 500020
            time.sleep(args.batch_rest)
            batch_count = 0
        else:
            time.sleep(args.call_interval)

    # Final stats
    elapsed = time.time() - stats['started_at']
    logging.info("=" * 70)
    logging.info("BACKFILL COMPLETE")
    logging.info("  uptime:                 %.2f hours", elapsed / 3600)
    logging.info("  improved (saved):       %d", stats['improved'])
    logging.info("  no_gain (probe stored): %d", stats['no_gain'])
    logging.info("  quota_blocked (400000): %d", stats['quota_blocked_400'])
    logging.info("  throttled   (500020):   %d  (%d cooldown cycles)",
                 stats['throttle_cycles'], stats['throttle_cycles'])
    logging.info("  other errors:           %d", stats['other_err'])
    logging.info("  quota_reset_cycles:     %d  (days waited through)",
                 stats['quota_cycles'])
    logging.info("  total chars gained:     %d", stats['total_chars_gained'])
    logging.info("  Post-backfill: total=%d  bypass_updated=%d  truncated=%d",
                 col.count_documents({}),
                 col.count_documents({'_bypass_updated_at': {'$exists': True}}),
                 col.count_documents({'content_truncated': True}))


if __name__ == '__main__':
    main()
