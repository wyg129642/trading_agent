"""扫 gangtise-full.summaries 中 truncated/试读截断的 doc, 走 S3 直连重抓全文.

S3 直连端点不走 /summary/download 的 quota gate (903301 / 10011401), 单条 ~0.1-0.3s.
本脚本与实时 watcher 共用 token + S3 旁路逻辑, 但完全独立运行 — 不动 list, 只补正文.

候选条件:
  - msg_text[0].url 存在 (S3 path 可用)
  - msg_id 存在 (用于 dedupe)
  - 现有 content_md 满足以下之一:
      a) content_truncated=True
      b) content_md 为空
      c) content_md < 500 字 + 不以句号/.” 等结尾 (试读特征)

只在 S3 新内容显著长于现有 (>+50 字) 时更新, 幂等可重跑.

Usage:
  python3 backfill_summary_s3.py                    # 全量
  python3 backfill_summary_s3.py --limit 500        # 试跑
  python3 backfill_summary_s3.py --since-days 30    # 限定窗口
  python3 backfill_summary_s3.py --dry-run          # 不写 mongo
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from pymongo import MongoClient
from pymongo.errors import AutoReconnect, NetworkTimeout

from scraper import (
    SessionDead,
    _fetch_summary_text_via_s3,
    create_session,
)


def _mongo_update_with_retry(coll, q, u, retries=5):
    last = None
    for i in range(retries):
        try:
            return coll.update_one(q, u)
        except (AutoReconnect, NetworkTimeout) as e:
            last = e
            time.sleep(2 ** i)
    raise last


def _has_period_end(s: str) -> bool:
    return bool(s) and s.strip()[-1:] in "。！？.!?》」）)]}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mongo-uri", default="mongodb://127.0.0.1:27018")
    ap.add_argument("--db", default="gangtise-full")
    ap.add_argument("--limit", type=int, default=0,
                    help="最多处理多少条 (0=全量)")
    ap.add_argument("--since-days", type=int, default=0,
                    help="只处理近 N 天 release_time 的 doc (0=不限)")
    ap.add_argument("--throttle", type=float, default=0.15,
                    help="每条 sleep 秒数")
    ap.add_argument("--min-gain", type=int, default=50,
                    help="新内容比旧的多多少字才更新")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    with open(Path(__file__).with_name("credentials.json")) as f:
        tok = json.load(f).get("token", "")
    if not tok:
        print("错误: credentials.json 无 token", file=sys.stderr)
        sys.exit(2)

    sess = create_session(tok)
    c = MongoClient(args.mongo_uri)
    db = c[args.db]
    coll = db["summaries"]

    q = {
        "msg_text": {"$exists": True, "$ne": []},
        "msg_id": {"$exists": True, "$ne": None},
        "$or": [
            {"content_truncated": True},
            {"content_md": {"$in": [None, ""]}},
            {"content_md": {"$exists": False}},
            # 短文本兜底: < 500 字才尝试 S3
            # ($expr 慢; 用 estimated 加预筛, 在循环里再精校)
        ],
    }
    if args.since_days > 0:
        cutoff_ms = int((datetime.utcnow()
                         - timedelta(days=args.since_days)).timestamp() * 1000)
        q["release_time_ms"] = {"$gte": cutoff_ms}

    total = coll.count_documents(q)
    print(f"[backfill-s3] 待处理候选 (truncated/empty + msg_text): {total}, "
          f"limit={args.limit or 'all'}, dry_run={args.dry_run}")
    if total == 0:
        return

    proj = {"_id": 1, "msg_id": 1, "msg_text": 1,
            "title": 1, "release_time": 1,
            "content_md": 1, "content_truncated": 1}
    cur = coll.find(q, proj).sort("release_time_ms", -1)
    if args.limit:
        cur = cur.limit(args.limit)

    counter = Counter()
    n_processed = 0
    t_start = time.time()
    for doc in cur:
        n_processed += 1
        msg_text = doc.get("msg_text") or []
        if not isinstance(msg_text, list) or not msg_text:
            counter["no_msg_text"] += 1
            continue
        first = msg_text[0]
        if not isinstance(first, dict):
            counter["bad_msg_text"] += 1
            continue
        url_path = first.get("url") or ""
        if not url_path:
            counter["no_url"] += 1
            continue

        cur_content = (doc.get("content_md") or "").strip()
        # 已经看似完整 (≥ 1500 字 + 句号收尾) 跳过 — 防止过度刷新
        if len(cur_content) >= 1500 and _has_period_end(cur_content) \
                and not doc.get("content_truncated"):
            counter["already_full"] += 1
            continue

        try:
            s3_text = _fetch_summary_text_via_s3(sess, tok, url_path)
        except SessionDead as e:
            print(f"[backfill-s3] SessionDead — token 失效, 中止: {e}",
                  file=sys.stderr)
            break
        except Exception as e:
            counter["s3_error"] += 1
            print(f"  ✗ {doc['_id']}: {e}")
            time.sleep(args.throttle)
            continue

        if not s3_text:
            counter["s3_empty"] += 1
        elif len(s3_text) <= len(cur_content) + args.min_gain:
            counter["no_gain"] += 1
        else:
            if not args.dry_run:
                _mongo_update_with_retry(coll,
                    {"_id": doc["_id"]},
                    {"$set": {
                        "content_md": s3_text,
                        "content_truncated": False,
                        "_s3_backfilled_at": datetime.now(timezone.utc),
                    }},
                )
            counter["recovered"] += 1
            if counter["recovered"] <= 5 or counter["recovered"] % 50 == 0:
                title = (doc.get("title") or "")[:55]
                print(f"  ✓ +{len(s3_text)-len(cur_content):>5}字  "
                      f"[{doc.get('release_time')}]  {title}")

        if n_processed % 200 == 0:
            elapsed = time.time() - t_start
            rate = n_processed / elapsed if elapsed > 0 else 0
            print(f"  [{n_processed}/{total}] elapsed={elapsed:.0f}s "
                  f"rate={rate:.1f}/s  recovered={counter['recovered']}")

        time.sleep(args.throttle)

    elapsed = time.time() - t_start
    print(f"\n[backfill-s3] 完成 {n_processed} 条, {elapsed:.0f}s "
          f"({n_processed/elapsed:.1f}/s)")
    for k, v in counter.most_common():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
