"""`python3 -m scripts.kb_vector verify --coll <db>/<c>` — drift detail.

Enumerates Mongo `_id`s in a collection and Milvus `doc_id`s in the kb_chunks
collection matching that (db, collection), then reports:

  - missing_in_milvus : docs in Mongo but no chunks in Milvus
                        (ingestion hasn't caught up, or crawler wrote with
                         release_time_ms < last_watermark after a backfill)
  - missing_in_mongo  : doc_ids in Milvus for which Mongo has no _id
                        (Mongo was cleaned; delete sweep should remove these)
  - matched           : the happy middle

Read-only. Use `--json` to pipe into other tools; `--limit N` to cap Mongo scan.
"""
from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Iterable

from bson import ObjectId
from pymilvus import MilvusClient
from pymongo import MongoClient

from backend.app.config import get_settings
from backend.app.services.kb_service import SPECS_BY_KEY


# ── Doc ID convention ────────────────────────────────────────────
# Vector "doc_id" is "<db>:<collection>:<mongo_oid_str>" (see kb_vector_ingest).


def _to_vector_doc_id(db: str, collection: str, mongo_id: Any) -> str:
    return f"{db}:{collection}:{str(mongo_id)}"


def _from_vector_doc_id(doc_id: str) -> tuple[str, str, str] | None:
    parts = doc_id.split(":", 2)
    if len(parts) != 3:
        return None
    return parts[0], parts[1], parts[2]


def _fetch_mongo_ids(
    mc: MongoClient, db: str, coll: str, limit: int | None = None
) -> set[str]:
    # `db` here is the SPEC label ("alphapai" etc.), not the physical Mongo
    # name. Translate at I/O boundary via kb_service.mongo_db_name_for.
    from backend.app.services.kb_service import MONGO_DB_ALIASES
    real_db = MONGO_DB_ALIASES.get(db, db)
    cursor = mc[real_db][coll].find(
        {"_id": {"$not": {"$regex": r"^(crawler_|daily_|_probe$|_state$|account$|test$)"}}},
        projection={"_id": 1},
    )
    if limit:
        cursor = cursor.limit(limit)
    return {str(doc["_id"]) for doc in cursor}


def _fetch_milvus_doc_ids(
    mv: MilvusClient, collection: str, db: str, mongo_coll: str, batch: int = 16384
) -> set[str]:
    """Enumerate every unique doc_id stored in Milvus for (db, mongo_coll)."""
    ids: set[str] = set()
    # Milvus has no native "distinct". Page via chunk_id. For POC this is fine
    # at 10k–100k rows; if per-collection count exceeds that we'll paginate via
    # a second field.
    cursor_token = ""
    while True:
        filter_expr = f'db == "{db}" and collection == "{mongo_coll}"'
        if cursor_token:
            filter_expr += f' and chunk_id > "{cursor_token}"'
        try:
            page = mv.query(
                collection_name=collection,
                filter=filter_expr,
                output_fields=["chunk_id", "doc_id"],
                limit=batch,
            )
        except Exception as e:
            print(f"ERROR querying Milvus: {e}", file=sys.stderr)
            break
        if not page:
            break
        ids.update(row["doc_id"] for row in page)
        last = max(row["chunk_id"] for row in page)
        if last == cursor_token or len(page) < batch:
            break
        cursor_token = last
    return ids


# ── Main ─────────────────────────────────────────────────────────


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--coll", required=True, help="e.g. alphapai/roadshows")
    ap.add_argument("--limit", type=int, help="Cap Mongo scan (for quick tests)")
    ap.add_argument("--json", action="store_true", help="Machine-readable output")
    ap.add_argument("--show-examples", type=int, default=5,
                    help="In human mode, show up to N example drifted IDs (default 5)")
    args = ap.parse_args(argv)

    if args.coll not in SPECS_BY_KEY:
        print(f"unknown collection '{args.coll}'", file=sys.stderr)
        return 2
    spec = SPECS_BY_KEY[args.coll]

    s = get_settings()
    mc = MongoClient(s.alphapai_mongo_uri, serverSelectionTimeoutMS=3000)
    mv = MilvusClient(uri=f"http://{s.milvus_host}:{s.milvus_port}")

    mongo_ids = _fetch_mongo_ids(mc, spec.db, spec.collection, limit=args.limit)
    mongo_vector_ids = {_to_vector_doc_id(spec.db, spec.collection, mid)
                        for mid in mongo_ids}
    milvus_doc_ids = _fetch_milvus_doc_ids(mv, s.milvus_collection, spec.db, spec.collection)

    missing_in_milvus = mongo_vector_ids - milvus_doc_ids
    missing_in_mongo = milvus_doc_ids - mongo_vector_ids
    matched = mongo_vector_ids & milvus_doc_ids

    result = {
        "collection": args.coll,
        "mongo_unique_ids": len(mongo_ids),
        "milvus_unique_doc_ids": len(milvus_doc_ids),
        "matched": len(matched),
        "missing_in_milvus": len(missing_in_milvus),
        "missing_in_mongo": len(missing_in_mongo),
        "drift_total": len(missing_in_milvus) + len(missing_in_mongo),
    }

    if args.json:
        if args.show_examples:
            result["examples_missing_in_milvus"] = sorted(missing_in_milvus)[:args.show_examples]
            result["examples_missing_in_mongo"] = sorted(missing_in_mongo)[:args.show_examples]
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0 if result["drift_total"] == 0 else 1

    # Human
    print(f"\n== verify {args.coll} ==")
    for k, v in result.items():
        print(f"  {k:26s}: {v}")

    if args.show_examples and missing_in_milvus:
        print(f"\n  example missing_in_milvus (first {min(args.show_examples, len(missing_in_milvus))}):")
        for did in sorted(missing_in_milvus)[:args.show_examples]:
            print(f"    {did}")
    if args.show_examples and missing_in_mongo:
        print(f"\n  example missing_in_mongo (first {min(args.show_examples, len(missing_in_mongo))}):")
        for did in sorted(missing_in_mongo)[:args.show_examples]:
            print(f"    {did}")
    print()

    return 0 if result["drift_total"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
