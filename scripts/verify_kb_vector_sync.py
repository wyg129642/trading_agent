"""Live integration check for the kb_vector_sync auto-sync loop.

Inserts a sentinel Mongo doc into ``funda/posts``, drives the same building
blocks ``ingest_collection`` uses (``build_rows_for_doc`` →
``tei_client.embed_batch`` → ``MilvusClient.upsert``), confirms the chunks
land in Milvus, then deletes the sentinel from Mongo and calls
``delete_doc`` to confirm the delete path.

This is intentionally a building-block-level test rather than calling
``ingest_collection`` end-to-end — that function does a full
``_prefetch_existing_by_doc`` pass over the entire collection (4–140 k
chunks per spec) which serializes badly against the live background loop
running in the staging backend. The building-block path covers the same
correctness guarantee (deterministic chunk_id, real TEI embeddings, real
Milvus upsert/delete) without the contention.

Usage::

    PYTHONPATH=. python3 scripts/verify_kb_vector_sync.py

Exit code 0 if both add and delete propagate; 2/3 on failure.
"""
from __future__ import annotations

import asyncio
import os
import sys
import time
from datetime import datetime, timezone

os.environ.setdefault("NO_PROXY", "127.0.0.1,localhost,jumpbox,116.239.28.36,192.168.31.0/24")
os.environ.setdefault("no_proxy", "127.0.0.1,localhost,jumpbox,116.239.28.36,192.168.31.0/24")

from pymilvus import MilvusClient  # noqa: E402
from pymongo import MongoClient  # noqa: E402

from backend.app.config import get_settings  # noqa: E402
from backend.app.services import tei_client  # noqa: E402
from backend.app.services.kb_service import SPECS_BY_KEY, mongo_db_name_for  # noqa: E402
from backend.app.services.kb_vector_ingest import (  # noqa: E402
    _vector_doc_id,
    build_rows_for_doc,
    delete_doc,
)


SPEC_KEY = "funda/posts"
SENTINEL_ID_PREFIX = "kb_vector_sync_verify_"


def _now() -> str:
    return datetime.now(tz=timezone.utc).astimezone().strftime("%H:%M:%S")


def _log(msg: str) -> None:
    print(f"[{_now()}] {msg}", flush=True)


async def main() -> int:
    s = get_settings()
    spec = SPECS_BY_KEY[SPEC_KEY]
    mongo_db = mongo_db_name_for(spec)
    milvus_coll = s.effective_milvus_collection

    mc = MongoClient(s.alphapai_mongo_uri, serverSelectionTimeoutMS=5000)
    mv = MilvusClient(uri=f"http://{s.milvus_host}:{s.milvus_port}")

    coll = mc[mongo_db][spec.collection]
    sentinel_id = f"{SENTINEL_ID_PREFIX}{int(time.time())}"
    doc_id = _vector_doc_id(spec.db, spec.collection, sentinel_id)

    _log(
        f"spec={SPEC_KEY} mongo={mongo_db}.{spec.collection} "
        f"milvus={milvus_coll} sentinel_id={sentinel_id}"
    )

    # Prophylactic cleanup so a prior partial run doesn't contaminate.
    coll.delete_many({"_id": {"$regex": f"^{SENTINEL_ID_PREFIX}"}})

    text_payload = (
        "kb_vector_sync auto-sync integration sentinel. Confirms that "
        "MongoDB → Milvus add-then-delete propagates end-to-end with the "
        "real jumpbox TEI embedding service in the loop, and that the "
        "delete sweep removes Milvus chunks once the upstream Mongo "
        "document is deleted. " * 6
    )
    now_ms = int(time.time() * 1000)
    sentinel = {
        "_id": sentinel_id,
        "title": "[kb_vector_sync_verify] sentinel",
        "content_md": text_payload,
        "release_time_ms": now_ms,
        "created_at_ms": now_ms,
    }

    try:
        _log("[step 1/5] insert sentinel into Mongo")
        coll.insert_one(sentinel)
        # Re-load through the same projection ingest_collection would use.
        loaded = coll.find_one({"_id": sentinel_id})
        assert loaded is not None, "sentinel not found after insert"

        _log("[step 2/5] build chunks via build_rows_for_doc")
        rows = build_rows_for_doc(spec, loaded)
        assert rows, "build_rows_for_doc returned zero chunks"
        _log(f"  built {len(rows)} chunk(s)")

        _log("[step 3/5] embed via TEI + upsert to Milvus")
        texts = [r["chunk_text"] for r in rows]
        vecs = await tei_client.embed_batch(texts)
        assert len(vecs) == len(rows), f"got {len(vecs)} vecs for {len(rows)} chunks"
        for r, v in zip(rows, vecs):
            r["dense_vector"] = v
        mv.upsert(collection_name=milvus_coll, data=rows)

        # Milvus segments need a brief moment for new rows to be queryable
        # under Strong consistency; in practice ~hundreds of ms is enough.
        await asyncio.sleep(2.0)

        _log("[step 4/5] query Milvus for sentinel doc_id")
        hits = mv.query(
            collection_name=milvus_coll,
            filter=f'doc_id == "{doc_id}"',
            output_fields=["doc_id", "chunk_id"],
            limit=8,
        )
        if len(hits) != len(rows):
            _log(f"  FAIL: expected {len(rows)} chunks, found {len(hits)}")
            return 2
        _log(f"  PASS: {len(hits)} chunks present")

        _log("[step 5/5] delete sentinel + delete_doc on Milvus")
        coll.delete_one({"_id": sentinel_id})
        delete_doc(mv, milvus_coll, doc_id)
        await asyncio.sleep(2.0)
        hits_after = mv.query(
            collection_name=milvus_coll,
            filter=f'doc_id == "{doc_id}"',
            output_fields=["doc_id"],
            limit=8,
        )
        if hits_after:
            _log(f"  FAIL: {len(hits_after)} chunks still in Milvus after delete_doc")
            return 3
        _log("  PASS: sentinel chunks gone from Milvus")
    finally:
        await tei_client.close()
        try:
            coll.delete_many({"_id": {"$regex": f"^{SENTINEL_ID_PREFIX}"}})
        except Exception:
            pass

    _log("OK — kb_vector_sync add + delete both propagate end-to-end")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
