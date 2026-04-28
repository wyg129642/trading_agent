"""Milvus vector store for the personal knowledge base.

A dedicated collection (``user_kb_chunks`` by default) holds a dense-vector
mirror of every chunk that also lives in MongoDB. The split of duties is:

* **MongoDB**: source of truth for chunk rows (text, tokens, metadata),
  plus the ``$text`` BM25 index. Also hosts the raw file (GridFS) and
  document-level metadata.
* **Milvus**: dense-vector side of hybrid retrieval. Stores only what's
  needed to serve vector search — ``chunk_id`` (deterministic PK), a
  copy of ``text`` (for display so we don't need a second Mongo trip on
  the hot path), the dense embedding, and enough metadata for filtering
  and attribution (document_id, user_id, chunk_index, created_at_ms).

Why a new collection instead of reusing the crawled ``kb_chunks``?
1. Different embedding model → different dimension (1536 vs 4096).
2. Different schema — crawled KB indexes tickers/date/doc_type, none of
   which apply to user uploads.
3. Access control differs: crawled KB is a shared read-only corpus;
   user-KB writes/deletes happen online per upload.

The module fails open: if Milvus is unreachable or the circuit is tripped,
every function returns an empty / zero-effect result and the caller's
hybrid search degrades to BM25-only. We never take down the chat path
for a vector-store hiccup.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import threading
from dataclasses import dataclass
from typing import Any, Sequence

from backend.app.config import get_settings

# Proxy bypass for localhost gRPC — Clash 7890 otherwise intercepts.
# Same defensive pattern as kb_vector_query / kb_vector_ingest.
os.environ.setdefault("no_proxy", "127.0.0.1,localhost,jumpbox,116.239.28.36,192.168.31.0/24")
os.environ.setdefault("NO_PROXY", "127.0.0.1,localhost,jumpbox,116.239.28.36,192.168.31.0/24")

logger = logging.getLogger(__name__)


# ── Typed result for downstream consumers ─────────────────────


@dataclass
class VectorHit:
    chunk_id: str
    document_id: str
    user_id: str
    chunk_index: int
    text: str


class VectorStoreUnavailable(RuntimeError):
    """Raised inside the module; callers catch this and degrade."""


# ── Deterministic chunk id ─────────────────────────────────────


def make_chunk_id(document_id: str, chunk_index: int) -> str:
    """Stable, URL-safe, 24-char chunk id.

    Must be deterministic so re-parsing the same document produces the same
    ids — we upsert on top of existing rows rather than leaking orphans.
    """
    h = hashlib.sha256(f"{document_id}:{chunk_index}".encode()).hexdigest()
    return h[:24]


# ── Milvus client singleton ────────────────────────────────────


_client_lock = threading.Lock()
_client: Any | None = None
_collection_ensured: bool = False
_ensure_lock = threading.Lock()


def _get_milvus_client():
    """Return a pymilvus MilvusClient, creating it once per process.

    Unlike Motor / httpx, the MilvusClient is synchronous and not bound to
    an event loop, so a single process-wide instance is safe.

    Raises VectorStoreUnavailable if the import or connect fails — caller
    catches and degrades.
    """
    global _client
    if _client is not None:
        return _client
    with _client_lock:
        if _client is not None:
            return _client
        try:
            from pymilvus import MilvusClient
        except Exception as e:
            raise VectorStoreUnavailable(f"pymilvus not installed: {e}") from e
        settings = get_settings()
        try:
            _client = MilvusClient(
                uri=f"http://{settings.milvus_host}:{settings.milvus_port}",
            )
        except Exception as e:
            raise VectorStoreUnavailable(
                f"cannot reach Milvus at {settings.milvus_host}:{settings.milvus_port}: {e}"
            ) from e
        return _client


def _collection_name() -> str:
    # Returns the env-scoped Milvus collection (prod: `user_kb_chunks`,
    # staging: `user_kb_chunks_staging`) so the two environments never
    # fight over the same vector index.
    return get_settings().effective_user_kb_milvus_collection


# ── Collection schema + indexes ────────────────────────────────


def _ensure_collection_sync() -> bool:
    """Create the collection (with schema + HNSW index + load) if missing.

    Idempotent and cached: after the first successful call in a process we
    skip the round-trip. Runs synchronously because pymilvus's DDL API is
    sync-only; call via ``asyncio.to_thread`` from async code.

    Returns True on success / already-exists, False on any failure (so the
    caller can disable the dense side for the rest of the process if the
    vector store is simply not available).
    """
    global _collection_ensured
    if _collection_ensured:
        return True
    with _ensure_lock:
        if _collection_ensured:
            return True
        try:
            from pymilvus import DataType
        except Exception as e:
            logger.warning("pymilvus unavailable, vector search disabled: %s", e)
            return False
        try:
            mc = _get_milvus_client()
        except VectorStoreUnavailable as e:
            logger.warning("Milvus unavailable, vector search disabled: %s", e)
            return False

        settings = get_settings()
        coll = _collection_name()

        try:
            if coll in mc.list_collections():
                # Collection exists — sanity check the dim matches config.
                info = mc.describe_collection(coll)
                for field in info.get("fields", []):
                    if field.get("type") == DataType.FLOAT_VECTOR:
                        existing_dim = (field.get("params") or {}).get("dim")
                        if existing_dim and int(existing_dim) != settings.user_kb_embedding_dim:
                            logger.error(
                                "user_kb Milvus collection %s has dim=%s but "
                                "config user_kb_embedding_dim=%d — refusing to "
                                "use it. Either change the config or drop the "
                                "collection to rebuild with the new model.",
                                coll, existing_dim, settings.user_kb_embedding_dim,
                            )
                            return False
                _collection_ensured = True
                return True

            # Create from scratch.
            schema = mc.create_schema(auto_id=False, enable_dynamic_field=False)
            schema.add_field("chunk_id", DataType.VARCHAR, is_primary=True, max_length=64)
            schema.add_field("document_id", DataType.VARCHAR, max_length=64)
            schema.add_field("user_id", DataType.VARCHAR, max_length=64)
            schema.add_field("chunk_index", DataType.INT32)
            schema.add_field("text", DataType.VARCHAR, max_length=65535)
            schema.add_field("created_at_ms", DataType.INT64)
            schema.add_field(
                "dense_vector",
                DataType.FLOAT_VECTOR,
                dim=settings.user_kb_embedding_dim,
            )

            index_params = mc.prepare_index_params()
            index_params.add_index(
                field_name="dense_vector",
                index_type="HNSW",
                metric_type="COSINE",   # OpenAI embeddings are L2-normalised,
                                         # cosine is the canonical similarity.
                params={"M": 16, "efConstruction": 200},
            )
            # Scalar index on document_id lets delete-by-doc stay fast as the
            # collection grows.
            index_params.add_index(field_name="document_id", index_type="INVERTED")
            index_params.add_index(field_name="user_id", index_type="INVERTED")

            mc.create_collection(
                collection_name=coll,
                schema=schema,
                index_params=index_params,
                consistency_level="Strong",  # hybrid search must see just-upserted rows
            )
            mc.load_collection(coll)
            logger.info("user_kb: created Milvus collection %s", coll)
            _collection_ensured = True
            return True
        except Exception as e:
            logger.exception("user_kb Milvus ensure_collection failed: %s", e)
            return False


async def ensure_collection() -> bool:
    """Async wrapper around :func:`_ensure_collection_sync`."""
    return await asyncio.to_thread(_ensure_collection_sync)


def _reset_ensure_for_tests() -> None:
    """Drop the cached collection-ensured flag so next call re-checks."""
    global _collection_ensured
    _collection_ensured = False


# ── Write path ─────────────────────────────────────────────────


@dataclass
class PendingChunk:
    chunk_id: str
    document_id: str
    user_id: str
    chunk_index: int
    text: str
    created_at_ms: int
    dense_vector: list[float]


async def upsert_chunks(chunks: Sequence[PendingChunk]) -> int:
    """Upsert chunks into Milvus. Returns the number written.

    Silently returns 0 if the collection isn't ready / Milvus is down —
    the caller will keep Mongo BM25 working and log the degradation at
    the service layer.
    """
    if not chunks:
        return 0
    if not await ensure_collection():
        return 0

    rows = [
        {
            "chunk_id": c.chunk_id,
            "document_id": c.document_id,
            "user_id": c.user_id,
            "chunk_index": c.chunk_index,
            # Milvus rejects VARCHAR over max_length. Our schema caps at
            # 65535; normal chunks are ~1000 chars, but defensive clamp
            # protects against future chunker changes.
            "text": c.text[:65000],
            "created_at_ms": c.created_at_ms,
            "dense_vector": c.dense_vector,
        }
        for c in chunks
    ]

    def _upsert() -> int:
        try:
            mc = _get_milvus_client()
            mc.upsert(collection_name=_collection_name(), data=rows)
            return len(rows)
        except Exception as e:
            logger.warning("user_kb Milvus upsert failed: %s", e)
            return 0

    return await asyncio.to_thread(_upsert)


async def delete_by_document(document_id: str) -> int:
    """Remove every chunk belonging to ``document_id``. Returns # deleted.

    Called on document deletion and before a reparse that shrinks the
    chunk count (so stale high-index chunks don't linger).
    """
    if not document_id:
        return 0
    if not await ensure_collection():
        return 0

    def _delete() -> int:
        try:
            mc = _get_milvus_client()
            # Milvus delete by expression; no count returned, we trust it.
            mc.delete(
                collection_name=_collection_name(),
                filter=f'document_id == "{_escape_str(document_id)}"',
            )
            return 1
        except Exception as e:
            logger.warning(
                "user_kb Milvus delete_by_document(%s) failed: %s",
                document_id, e,
            )
            return 0

    return await asyncio.to_thread(_delete)


# ── Read path ──────────────────────────────────────────────────


async def vector_search(
    query_vector: Sequence[float],
    *,
    top_k: int = 30,
    user_id: str | None = None,
    document_ids: list[str] | None = None,
) -> list[VectorHit]:
    """ANN search. Returns empty list if the vector store is unavailable.

    :param query_vector: dense embedding of the query (same model as ingest).
    :param top_k: how many hits to return. Over-fetch a bit vs the final
        fusion target since RRF will drop some to dedupe by parent doc.
    :param user_id: if provided, restrict to that user's uploads (rare —
        the product default is team-shared, but ``scope=mine`` callers
        use this).
    :param document_ids: optional list restricting to specific documents.
    """
    if not query_vector:
        return []
    if not await ensure_collection():
        return []

    filters: list[str] = []
    if user_id:
        filters.append(f'user_id == "{_escape_str(user_id)}"')
    if document_ids:
        quoted = ", ".join(f'"{_escape_str(d)}"' for d in document_ids)
        filters.append(f"document_id in [{quoted}]")
    filter_expr = " and ".join(filters) if filters else None

    def _search() -> list[VectorHit]:
        try:
            mc = _get_milvus_client()
            results = mc.search(
                collection_name=_collection_name(),
                data=[list(query_vector)],
                anns_field="dense_vector",
                search_params={"metric_type": "COSINE", "params": {"ef": 128}},
                limit=top_k,
                filter=filter_expr,
                output_fields=["chunk_id", "document_id", "user_id",
                               "chunk_index", "text"],
            )
        except Exception as e:
            logger.warning("user_kb Milvus vector_search failed: %s", e)
            return []

        hits: list[VectorHit] = []
        for row in results[0] if results else []:
            entity = row.get("entity", {})
            hits.append(
                VectorHit(
                    chunk_id=str(entity.get("chunk_id") or row.get("id") or ""),
                    document_id=str(entity.get("document_id") or ""),
                    user_id=str(entity.get("user_id") or ""),
                    chunk_index=int(entity.get("chunk_index") or 0),
                    text=str(entity.get("text") or ""),
                )
            )
        return hits

    return await asyncio.to_thread(_search)


# ── Misc ───────────────────────────────────────────────────────


def _escape_str(v: str) -> str:
    """Safe quoting for Milvus filter expression VARCHAR literals."""
    return v.replace("\\", "\\\\").replace('"', '\\"')


async def is_healthy() -> bool:
    """Probe for /health endpoints. Used by the KB ping endpoint."""
    try:
        mc = _get_milvus_client()
    except VectorStoreUnavailable:
        return False
    def _ping():
        try:
            mc.list_collections()
            return True
        except Exception:
            return False
    return await asyncio.to_thread(_ping)


async def count_chunks() -> int:
    """Total rows in the user_kb Milvus collection. For debugging/UI."""
    if not await ensure_collection():
        return 0
    def _count():
        try:
            mc = _get_milvus_client()
            return mc.get_collection_stats(_collection_name()).get("row_count", 0)
        except Exception as e:
            logger.debug("count_chunks failed: %s", e)
            return 0
    return await asyncio.to_thread(_count)
