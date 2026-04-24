"""Tag-only backfill for AlphaPai sub-category tabs.

Supports 3 categories (each has its own SPA tabs):
  - roadshow: ashare / hk / us / web / ir / hot
  - report:   ashare / us / indep
  - comment:  selected / regular

For each requested (category, subtype), paginate the list endpoint and
`$addToSet` the key into every visible doc's `_{category}_subcategories`
array.  Does NOT touch content/detail — pure tag merge.

Usage:
    python tag_subcategories.py                                   # all cats, all types, --max 500
    python tag_subcategories.py --category roadshow               # only roadshow (6 types)
    python tag_subcategories.py --category report --types indep   # single run
    python tag_subcategories.py --max 1000                        # deeper
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from pymongo import MongoClient, UpdateOne  # noqa: E402

from scraper import (  # noqa: E402
    API_BASE,
    CATEGORIES,
    SUBTYPES,
    _load_token_from_file,
    api_call,
    create_session,
    fetch_list_page,
    make_dedup_id,
)


def collect_ids(session, category_key: str, market_type: str, max_items: int,
                page_size: int, throttle: float) -> list[str]:
    cfg = CATEGORIES[category_key]
    ids: list[str] = []
    page = 1
    while len(ids) < max_items:
        resp = fetch_list_page(session, cfg, page, page_size,
                               market_type=market_type,
                               category_key=category_key)
        if resp.get("code") != 200000:
            print(f"  [{category_key}/{market_type}] page {page} err: "
                  f"code={resp.get('code')} msg={resp.get('message')}")
            break
        data = resp.get("data") or {}
        items = data.get("list") or []
        if not items:
            break
        for it in items:
            ids.append(make_dedup_id(category_key, it, cfg))
            if len(ids) >= max_items:
                break
        total = data.get("total")
        print(f"  [{category_key}/{market_type}] page {page}: +{len(items)}  "
              f"(累计 {len(ids)}/{max_items}, total≈{total})")
        if len(items) < page_size:
            print(f"  [{category_key}/{market_type}] partial page, list exhausted")
            break
        page += 1
        time.sleep(throttle)
    return ids


def tag_docs(col, ids: list[str], category_key: str, market_type: str) -> tuple[int, int]:
    if not ids:
        return (0, 0)
    sub_field = f"_{category_key}_subcategories"
    ops = [UpdateOne({"_id": _id},
                     {"$addToSet": {sub_field: market_type}})
           for _id in ids]
    r = col.bulk_write(ops, ordered=False)
    return (r.matched_count, r.modified_count)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--category", choices=list(SUBTYPES.keys()), default=None,
                    help="one of roadshow/report/comment; omitted = all categories")
    ap.add_argument("--types", nargs="+", default=None,
                    help="subset of subtype keys; default = all for the category(ies)")
    ap.add_argument("--max", type=int, default=500)
    ap.add_argument("--page-size", type=int, default=50)
    ap.add_argument("--throttle", type=float, default=1.5)
    ap.add_argument("--mongo-uri", default="mongodb://localhost:27017")
    ap.add_argument("--mongo-db", default="alphapai")
    args = ap.parse_args()

    token = _load_token_from_file()
    session = create_session(token)
    session.trust_env = False
    session.proxies = {"http": None, "https": None}

    client = MongoClient(args.mongo_uri)
    db = client[args.mongo_db]

    cat_list = [args.category] if args.category else list(SUBTYPES.keys())
    print(f"api base: {API_BASE}   max/type: {args.max}   throttle: {args.throttle}s")
    print(f"categories: {cat_list}")
    print()

    totals: dict[str, dict[str, dict]] = {}
    for cat in cat_list:
        type_keys = list(SUBTYPES[cat].keys())
        if args.types:
            type_keys = [t for t in type_keys if t in args.types]
        totals[cat] = {}
        coll = db[CATEGORIES[cat]["collection"]]
        for mt in type_keys:
            label = SUBTYPES[cat][mt]["label"]
            print(f"--- {cat}/{mt} ({label}) ---")
            ids = collect_ids(session, cat, mt, args.max,
                              args.page_size, args.throttle)
            matched, modified = tag_docs(coll, ids, cat, mt)
            print(f"  collected {len(ids)} ids, matched {matched}, modified {modified}")
            totals[cat][mt] = {"collected": len(ids), "matched": matched,
                               "modified": modified}
            print()

    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    for cat, sub in totals.items():
        coll = db[CATEGORIES[cat]["collection"]]
        print(f"[{cat}]")
        for mt, t in sub.items():
            label = SUBTYPES[cat][mt]["label"]
            n = coll.count_documents({f"_{cat}_subcategories": mt})
            print(f"  {mt:10s} ({label}): collected={t['collected']:4d} "
                  f"modified={t['modified']:4d} final_tagged={n:5d}")


if __name__ == "__main__":
    main()
