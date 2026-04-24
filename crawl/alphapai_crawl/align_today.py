"""Align today's DB coverage against the platform for a given category.

Workflow (same pattern that aligned the roadshow 会议纪要 page):
  1. For each sub-type in SUBTYPES[category], paginate list API with
     startDate=today..endDate=today (or walk pages stopping when dt<today).
  2. Collect the dedup_id for every item the platform actually returns today.
  3. Diff against DB; for every missing _id call the scraper's dump_one so
     the item gets fully ingested (detail + content + PDF).
  4. Also $addToSet the sub-type tag onto every matched id so the frontend's
     sub-tabs stay correct.

Usage:
    python align_today.py --category report
    python align_today.py --category report --subtypes ashare us
"""
from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parent))

from pymongo import MongoClient  # noqa: E402

from scraper import (  # noqa: E402
    CATEGORIES,
    SUBTYPES,
    _load_token_from_file,
    _extract_time_str,
    _parse_time_to_dt,
    create_session,
    dump_one,
    fetch_list_page,
    make_dedup_id,
)


def enumerate_today_ids(session, category_key: str, market_type: str,
                        page_size: int, throttle: float,
                        max_pages: int = 60) -> list[tuple[str, dict]]:
    """Enumerate every list-item whose list-time falls in today's date."""
    cfg = CATEGORIES[category_key]
    today_dt = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    items: list[tuple[str, dict]] = []
    page = 1
    stop = False
    while page <= max_pages and not stop:
        resp = fetch_list_page(session, cfg, page, page_size,
                               market_type=market_type,
                               category_key=category_key)
        if resp.get("code") != 200000:
            print(f"  [{market_type}] page {page} err: code={resp.get('code')}")
            break
        data = resp.get("data") or {}
        page_items = data.get("list") or []
        if not page_items:
            break
        for it in page_items:
            dt = _parse_time_to_dt(_extract_time_str(it, cfg["time_field"]))
            if dt is None:
                continue
            if dt < today_dt:
                stop = True
                break
            if dt.date() == today_dt.date():
                items.append((make_dedup_id(category_key, it, cfg), it))
        print(f"  [{market_type}] page {page}: +{len(page_items)}  "
              f"(累计 today {len(items)}, total≈{data.get('total')})")
        if len(page_items) < page_size:
            break
        page += 1
        time.sleep(throttle)
    return items


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--category", choices=list(SUBTYPES.keys()), required=True)
    ap.add_argument("--subtypes", nargs="+", default=None,
                    help="default = all subtypes for the category")
    ap.add_argument("--page-size", type=int, default=100)
    ap.add_argument("--throttle", type=float, default=1.5)
    ap.add_argument("--skip-pdf", action="store_true")
    ap.add_argument("--mongo-uri", default="mongodb://localhost:27017")
    ap.add_argument("--mongo-db", default="alphapai")
    args = ap.parse_args()

    token = _load_token_from_file()
    session = create_session(token)
    session.trust_env = False
    session.proxies = {"http": None, "https": None}

    client = MongoClient(args.mongo_uri)
    db = client[args.mongo_db]
    cfg = CATEGORIES[args.category]
    col = db[cfg["collection"]]

    subtypes = args.subtypes or list(SUBTYPES[args.category].keys())
    print(f"category={args.category}  subtypes={subtypes}")
    print()

    all_ids: dict[str, dict] = {}           # _id -> item (for missing re-ingest)
    by_subtype: dict[str, list[str]] = {}    # subtype -> [_id,...]
    for mt in subtypes:
        print(f"--- enumerate {args.category}/{mt} ({SUBTYPES[args.category][mt]['label']}) ---")
        pairs = enumerate_today_ids(session, args.category, mt,
                                    args.page_size, args.throttle)
        print(f"  today-items: {len(pairs)}")
        by_subtype[mt] = [pid for pid, _ in pairs]
        for pid, it in pairs:
            all_ids.setdefault(pid, it)

    print()
    print(f"=== UNIONED platform-today ids: {len(all_ids)} ===")

    # Diff vs DB
    existing = set(d["_id"] for d in col.find(
        {"_id": {"$in": list(all_ids.keys())}}, {"_id": 1}))
    missing_ids = [i for i in all_ids.keys() if i not in existing]
    print(f"  already in DB: {len(existing)}")
    print(f"  missing: {len(missing_ids)}")

    # $addToSet sub-type tags for every visible id (covers newly-ingested later)
    from pymongo import UpdateOne
    sub_field = f"_{args.category}_subcategories"
    tag_ops = []
    for mt, ids in by_subtype.items():
        for _id in ids:
            tag_ops.append(UpdateOne({"_id": _id},
                                     {"$addToSet": {sub_field: mt}}))
    if tag_ops:
        r = col.bulk_write(tag_ops, ordered=False)
        print(f"  tag $addToSet: matched {r.matched_count} modified {r.modified_count}")

    # Ingest missing items via dump_one
    if not missing_ids:
        print("\nNo missing items — fully aligned.")
        return

    print()
    print(f"=== INGESTING {len(missing_ids)} missing items ===")
    pdf_dir = Path(getattr(cfg, "pdf_dir", None) or "/tmp/alphapai_pdf_placeholder")
    if args.category == "report":
        from scraper import PDF_DIR_DEFAULT
        pdf_dir = Path(PDF_DIR_DEFAULT)
    added = failed = 0
    for i, _id in enumerate(missing_ids, 1):
        item = all_ids[_id]
        # find which subtype(s) this id belongs to
        mts = [mt for mt, ids in by_subtype.items() if _id in ids]
        primary_mt = mts[0] if mts else None
        try:
            status, info = dump_one(
                session, db, args.category, cfg, item,
                force=False,
                pdf_dir=pdf_dir if args.category == "report" else None,
                download_pdf=not args.skip_pdf,
                token=token,
                market_type=primary_mt,
            )
            if status == "added":
                added += 1
            print(f"  [{i:3d}/{len(missing_ids)}] {status:8s} [{primary_mt}] "
                  f"{(item.get('title') or '')[:55]}  content={info.get('content_len',0)}")
        except Exception as e:
            failed += 1
            print(f"  [{i:3d}/{len(missing_ids)}] FAILED: {e}")
        time.sleep(args.throttle)

    # If an item matched multiple subtypes, stamp the remaining ones too
    if added:
        for mt in subtypes:
            extras = [i for i in missing_ids if i in by_subtype.get(mt, [])]
            if extras:
                col.bulk_write(
                    [UpdateOne({"_id": i},
                               {"$addToSet": {sub_field: mt}}) for i in extras],
                    ordered=False,
                )

    print()
    print(f"SUMMARY: added={added} failed={failed}")

    # Post-check
    today = datetime.now().strftime("%Y-%m-%d")
    n_db = col.count_documents({"publish_time": {"$regex": "^" + today}})
    print(f"DB after: today={n_db}")


if __name__ == "__main__":
    main()
