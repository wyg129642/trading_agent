"""`python3 -m scripts.kb_vector status` — overview of the vector stack.

Shows, for each collection registered in kb_service.SPECS_LIST:

    db/collection   Mongo(count)  Milvus(count)  drift  watermark(last_release_time_ms)

Plus a global summary: TEI health (/health), Milvus total rows, Redis queue depth
(rag:ingest + rag:dead), last daily delete-sweep timestamp.

Read-only. No side effects.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import datetime, timezone
from typing import Any

import httpx
from pymilvus import MilvusClient
from pymongo import MongoClient

from backend.app.config import get_settings
from backend.app.services.kb_service import SPECS_LIST, SPECS_BY_KEY


# ── Helpers ────────────────────────────────────────────────────────


def _mongo_count(mc: MongoClient, db: str, coll: str) -> int:
    """Count 'real' content docs, excluding crawler/daily/account meta.

    Implementation note: `count_documents(filter)` requires the `aggregate`
    privilege which the remote crawler Mongo's `u_spider` account doesn't
    have. We route through `estimated_document_count()` (uses the simpler
    `count` command, auth-allowed) and subtract a small bounded count of
    known meta doc ids found via `find()`. For collections with a handful of
    meta docs this is accurate within ±5.
    """
    from backend.app.services.kb_service import MONGO_DB_ALIASES
    real_db = MONGO_DB_ALIASES.get(db, db)
    c = mc[real_db][coll]
    total = c.estimated_document_count()
    # Count known meta docs by cheap id match (small set, fast). `find()`
    # works under u_spider's auth; `count_documents(filter)` does NOT because
    # it routes through aggregate.
    meta_ids = ["_state", "_probe", "account", "test"]
    try:
        meta = len(list(c.find(
            {"_id": {"$in": meta_ids}}, projection={"_id": 1}
        )))
    except Exception:
        meta = 0
    return max(0, total - meta)


def _milvus_counts(mv: MilvusClient, collection: str, db: str, mongo_coll: str) -> tuple[int, int]:
    """Return (unique_doc_count, chunk_count) for a (db, mongo_coll) in Milvus.

    Paginated by chunk_id to handle >16k chunks. Distinct doc_ids via set.
    """
    doc_ids: set[str] = set()
    chunk_count = 0
    last = ""
    try:
        while True:
            filt = f'db == "{db}" and collection == "{mongo_coll}"'
            if last:
                filt += f' and chunk_id > "{last}"'
            page = mv.query(
                collection_name=collection,
                filter=filt,
                output_fields=["chunk_id", "doc_id"],
                limit=16384,
            )
            if not page:
                break
            chunk_count += len(page)
            for r in page:
                doc_ids.add(r["doc_id"])
            new_last = max(r["chunk_id"] for r in page)
            if new_last == last or len(page) < 16384:
                break
            last = new_last
        return len(doc_ids), chunk_count
    except Exception:
        return -1, -1


def _milvus_total_rows(mv: MilvusClient, collection: str) -> int:
    try:
        stats = mv.get_collection_stats(collection_name=collection)
        return int(stats.get("row_count", 0))
    except Exception:
        return -1


async def _tei_health() -> tuple[bool, str]:
    s = get_settings()
    if not s.tei_api_key:
        return False, "TEI_API_KEY unset (run deploy_jumpbox_tei.sh first)"
    try:
        async with httpx.AsyncClient(trust_env=False, proxy=None, timeout=3.0) as c:
            r = await c.get(f"{s.tei_base_url.rstrip('/')}/health",
                            headers={"Authorization": f"Bearer {s.tei_api_key}"})
        return r.status_code == 200, f"HTTP {r.status_code}"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def _redis_queue_depth() -> dict[str, int]:
    try:
        import redis  # lazy: redis is already in backend/requirements.txt
        s = get_settings()
        r = redis.Redis.from_url(s.redis_url)
        return {
            "rag:ingest": r.xlen("rag:ingest") if r.exists("rag:ingest") else 0,
            "rag:dead": r.xlen("rag:dead") if r.exists("rag:dead") else 0,
        }
    except Exception as e:
        return {"error": str(e)}


def _load_watermark(mc_unused: MongoClient, db: str, coll: str) -> dict[str, Any]:
    """Watermark state lives in the LOCAL state-Mongo (independent of the
    remote crawler DB). See kb_vector_ingest._state_client().
    Returns an empty dict if the collection hasn't been ingested yet."""
    from backend.app.services.kb_vector_ingest import _state_client, SYNC_STATE_DB
    key = f"{db}/{coll}"
    try:
        doc = _state_client()[SYNC_STATE_DB]["vector_sync_state"].find_one({"_id": key}) or {}
    except Exception:
        doc = {}
    return doc


# ── Main ───────────────────────────────────────────────────────────


def _format_ts(ms: int | None) -> str:
    if not ms:
        return "-"
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")


def _render_table(rows: list[list[str]], headers: list[str]) -> str:
    widths = [max(len(r[i]) for r in [headers, *rows]) for i in range(len(headers))]
    fmt = "  ".join(f"{{:<{w}}}" for w in widths)
    out = [fmt.format(*headers), fmt.format(*["─" * w for w in widths])]
    for r in rows:
        out.append(fmt.format(*r))
    return "\n".join(out)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--coll", help="Filter to one collection, e.g. alphapai/roadshows")
    ap.add_argument("--json", action="store_true", help="Machine-readable JSON output")
    args = ap.parse_args(argv)

    s = get_settings()
    mc = MongoClient(s.alphapai_mongo_uri, serverSelectionTimeoutMS=3000)
    mv = MilvusClient(uri=f"http://{s.milvus_host}:{s.milvus_port}")

    specs = SPECS_LIST
    if args.coll:
        if args.coll not in SPECS_BY_KEY:
            print(f"unknown collection '{args.coll}'. Known: "
                  + ", ".join(sorted(SPECS_BY_KEY)), file=sys.stderr)
            return 2
        specs = [SPECS_BY_KEY[args.coll]]

    # Per-collection table — drift is doc-level (what verify counts), not chunk-level.
    rows = []
    for spec in specs:
        mongo_n = _mongo_count(mc, spec.db, spec.collection)
        milvus_docs, milvus_chunks = _milvus_counts(
            mv, s.milvus_collection, spec.db, spec.collection,
        )
        doc_drift = mongo_n - milvus_docs if milvus_docs >= 0 else -1
        wm = _load_watermark(mc, spec.db, spec.collection)
        rows.append([
            f"{spec.db}/{spec.collection}",
            f"{mongo_n:,}",
            f"{milvus_docs:,}" if milvus_docs >= 0 else "ERR",
            f"{milvus_chunks:,}" if milvus_chunks >= 0 else "ERR",
            f"{doc_drift:+,}" if doc_drift != 0 else "0",
            _format_ts(wm.get("last_release_time_ms")),
        ])

    # Global
    milvus_total = _milvus_total_rows(mv, s.milvus_collection)
    tei_ok, tei_msg = asyncio.run(_tei_health())
    queue = _redis_queue_depth()

    if args.json:
        print(json.dumps({
            "milvus_total_rows": milvus_total,
            "tei_ok": tei_ok,
            "tei_msg": tei_msg,
            "redis_queue": queue,
            "per_collection": [
                {
                    "collection": r[0],
                    "mongo_docs": int(r[1].replace(",", "")),
                    "milvus_docs": int(r[2].replace(",", "")) if r[2] != "ERR" else None,
                    "milvus_chunks": int(r[3].replace(",", "")) if r[3] != "ERR" else None,
                    "doc_drift": int(r[4].replace(",", "").replace("+", "")) if r[4] != "ERR" else None,
                    "last_release_time": r[5],
                }
                for r in rows
            ],
        }, indent=2, ensure_ascii=False))
        return 0

    # Human
    print(f"\n== kb_vector status @ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ==")
    print(f"\n  Milvus total rows       : {milvus_total:,}")
    print(f"  TEI /health             : {'OK' if tei_ok else 'DOWN'}  ({tei_msg})")
    q_ing = queue.get("rag:ingest", "?")
    q_dea = queue.get("rag:dead", "?")
    print(f"  Redis rag:ingest depth  : {q_ing}")
    print(f"  Redis rag:dead depth    : {q_dea}")
    print()
    print(_render_table(
        rows,
        ["db/collection", "mongo_docs", "milvus_docs", "milvus_chunks",
         "doc_drift", "last_release_time_utc"],
    ))
    print()

    if any(r[4] != "0" and r[4] != "ERR" for r in rows):
        print("  hint: non-zero doc_drift → run "
              "`python3 -m scripts.kb_vector verify --coll <k>` for detail", file=sys.stderr)

    return 0


if __name__ == "__main__":
    sys.exit(main())
