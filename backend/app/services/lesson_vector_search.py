"""Lesson semantic retrieval via Milvus (optional).

Supplements the existing filesystem + pattern-based ``playbook_service.search_lessons``
with dense-vector semantic matching. When a cell path's text context (label,
notes, path segments) is semantically close to a lesson's body, return that
lesson — even if the ``applicable_path_patterns`` don't match.

Design goals:
* **Fail-open** — if Milvus or TEI is down, the caller still gets the legacy
  lexical result. This module never raises.
* **Idempotent sync** — re-indexing the same lesson overwrites its vector.
* **Small collection** — lessons are <10K per pack; no sharding needed.
* **Honour DEPRECATED** — the deprecation check is applied at search time,
  consistent with lessons.md (see ``lesson_versioning.py``).

Collection schema (``playbook_chunks``):
  * chunk_id    VARCHAR (PK) — ``{slug}::{lesson_id}``
  * industry    VARCHAR — index for filter
  * lesson_id   VARCHAR
  * title       VARCHAR(300)
  * body        VARCHAR(4000) — short text stored inline for result rendering
  * status      VARCHAR(30)   — active | deprecated | archived
  * updated_ms  INT64         — for staleness check
  * dense       FLOAT_VECTOR(4096) — Qwen3-Embedding-8B
"""
from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

COLLECTION_NAME = "playbook_chunks"
DENSE_DIM = 4096


_mv_client = None
_mv_ready = False


async def _milvus_client():
    """Lazy-create a MilvusClient. Never raises — returns None on failure."""
    global _mv_client
    if _mv_client is not None:
        return _mv_client
    if os.environ.get("PLAYBOOK_VECTOR_DISABLE") == "1":
        return None
    try:
        # Proxy bypass — see infra_proxy memory
        os.environ["NO_PROXY"] = (
            os.environ.get("NO_PROXY", "") + ",127.0.0.1,localhost,jumpbox"
        )
        from pymilvus import MilvusClient
        from backend.app.config import get_settings
        s = get_settings()
        _mv_client = MilvusClient(uri=f"http://{s.milvus_host}:{s.milvus_port}")
        return _mv_client
    except Exception as e:
        logger.debug("playbook milvus unavailable: %s", e)
        return None


async def _ensure_collection(mv) -> bool:
    """Create the collection + index if missing. Returns True on success."""
    global _mv_ready
    if _mv_ready:
        return True
    try:
        existing = mv.list_collections()
        if COLLECTION_NAME in existing:
            _mv_ready = True
            return True
        from pymilvus import DataType
        schema = mv.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field("chunk_id", DataType.VARCHAR, max_length=200, is_primary=True)
        schema.add_field("industry", DataType.VARCHAR, max_length=80)
        schema.add_field("lesson_id", DataType.VARCHAR, max_length=60)
        schema.add_field("title", DataType.VARCHAR, max_length=300)
        schema.add_field("body", DataType.VARCHAR, max_length=4000)
        schema.add_field("status", DataType.VARCHAR, max_length=30)
        schema.add_field("updated_ms", DataType.INT64)
        schema.add_field("dense", DataType.FLOAT_VECTOR, dim=DENSE_DIM)
        index_params = mv.prepare_index_params()
        index_params.add_index(field_name="dense", index_type="HNSW",
                               metric_type="COSINE",
                               params={"M": 16, "efConstruction": 200})
        mv.create_collection(
            collection_name=COLLECTION_NAME,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
        )
        _mv_ready = True
        return True
    except Exception:
        logger.exception("_ensure_collection failed for playbook_chunks")
        return False


def _lesson_text_for_embedding(title: str, body: str) -> str:
    """Truncate + combine for a single embedding (<8k chars keeps TEI happy)."""
    combined = f"{title}\n\n{body}"
    return combined[:6000]


async def upsert_lesson(
    industry: str, lesson_id: str, title: str, body: str,
    status: str = "active",
) -> bool:
    """Index (or re-index) a single lesson. Returns True on success."""
    mv = await _milvus_client()
    if mv is None:
        return False
    if not await _ensure_collection(mv):
        return False
    try:
        from backend.app.services.tei_client import embed_query
        vec = await embed_query(_lesson_text_for_embedding(title, body))
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        row = {
            "chunk_id": f"{industry}::{lesson_id}",
            "industry": industry,
            "lesson_id": lesson_id,
            "title": (title or "")[:300],
            "body": (body or "")[:4000],
            "status": status,
            "updated_ms": now_ms,
            "dense": vec,
        }
        # Idempotent upsert — pymilvus's upsert handles both insert + overwrite
        mv.upsert(collection_name=COLLECTION_NAME, data=[row])
        return True
    except Exception:
        logger.exception("upsert_lesson failed for %s/%s", industry, lesson_id)
        return False


async def search_lessons_semantic(
    industry: str,
    query_text: str,
    *,
    limit: int = 5,
    exclude_deprecated: bool = True,
) -> list[dict[str, Any]]:
    """Dense-vector search over a pack's lessons.

    Returns a list of {lesson_id, title, body, score, status}. Never raises.
    """
    mv = await _milvus_client()
    if mv is None or not query_text:
        return []
    if not await _ensure_collection(mv):
        return []
    try:
        from backend.app.services.tei_client import embed_query
        vec = await embed_query(query_text[:3000])
    except Exception:
        logger.debug("embed_query failed, skipping semantic lesson search", exc_info=True)
        return []
    try:
        expr = f'industry == "{industry}"'
        if exclude_deprecated:
            expr += ' and status != "deprecated" and status != "archived"'
        out = mv.search(
            collection_name=COLLECTION_NAME,
            data=[vec],
            anns_field="dense",
            search_params={"metric_type": "COSINE", "params": {"ef": 128}},
            limit=max(1, min(limit, 20)),
            filter=expr,
            output_fields=["lesson_id", "title", "body", "status", "updated_ms"],
        )
        hits = list(out[0]) if out else []
        return [{
            "lesson_id": h.get("entity", {}).get("lesson_id"),
            "title": h.get("entity", {}).get("title"),
            "body": h.get("entity", {}).get("body"),
            "status": h.get("entity", {}).get("status"),
            "updated_ms": h.get("entity", {}).get("updated_ms"),
            "score": float(h.get("distance", 0.0)),
        } for h in hits]
    except Exception:
        logger.debug("playbook_chunks search failed", exc_info=True)
        return []


async def delete_lesson(industry: str, lesson_id: str) -> bool:
    """Remove a lesson's vector (hard delete). Used by archive flow."""
    mv = await _milvus_client()
    if mv is None:
        return False
    try:
        mv.delete(
            collection_name=COLLECTION_NAME,
            filter=f'chunk_id == "{industry}::{lesson_id}"',
        )
        return True
    except Exception:
        logger.debug("delete_lesson failed", exc_info=True)
        return False


async def reindex_pack(
    industry: str,
    lessons: list[dict[str, str]],
    *,
    statuses: dict[str, str] | None = None,
) -> dict:
    """Bulk re-index all lessons for a pack. ``lessons`` is ``list_lessons()`` output."""
    mv = await _milvus_client()
    if mv is None:
        return {"indexed": 0, "reason": "milvus_unavailable"}
    ok = 0
    for l in lessons:
        status = (statuses or {}).get(l["id"], "active")
        if await upsert_lesson(industry, l["id"], l.get("title", ""), l.get("body", ""), status):
            ok += 1
        await asyncio.sleep(0)  # yield to event loop
    return {"indexed": ok, "total": len(lessons)}
