"""Hybrid retrieval (dense + BM25) over the kb_chunks Milvus collection.

Entry point: ``await hybrid_search(query, **filters, top_k=20)`` — drop-in
semantic replacement for ``kb_service.search`` (the Phase A filter-first scorer).

Pipeline per query:
    1. Embed query via TEI (LRU-cached) → 4096-dim dense vector.
    2. Build Milvus scalar filter expression from (tickers, date_range,
       doc_types, sources).
    3. Issue one ``hybrid_search`` call: dense top-100 + BM25 top-100 → RRF.
    4. Apply per-doc cap = 3 for diversity (top-K pollution from long transcripts).
    5. Truncate to ``top_k`` and return in a shape compatible with kb_service.search.

Fails open for TEI outages: if the embedding call trips the circuit breaker,
we fall back to sparse-only retrieval (BM25) so the chat tool keeps serving.
"""
from __future__ import annotations

# Proxy bypass for localhost gRPC.
import os
os.environ.setdefault("no_proxy", "127.0.0.1,localhost,jumpbox,116.239.28.36,192.168.31.0/24")
os.environ.setdefault("NO_PROXY", "127.0.0.1,localhost,jumpbox,116.239.28.36,192.168.31.0/24")

import asyncio
import logging
import time
from collections import defaultdict
from typing import Any

from pymilvus import AnnSearchRequest, MilvusClient, RRFRanker

from backend.app.config import get_settings
from backend.app.services import tei_client
from backend.app.services.kb_service import (
    SPECS_BY_KEY,
    SPECS_LIST,
    normalize_ticker_input,
    _str_to_ms,
)
from backend.app.services.tei_client import TEICircuitOpen, TEIError

logger = logging.getLogger(__name__)


# ── Constants ─────────────────────────────────────────────────────

DENSE_CANDIDATES = 100
SPARSE_CANDIDATES = 100
RRF_LIMIT = 50            # after fusion, before per-doc cap
PER_DOC_CAP = 3           # max chunks from same doc in final output
HNSW_EF = 128             # runtime ef for query
BM25_DROP_RATIO = 0.2     # prune bottom 20% sparse terms for speed


# ── Milvus client singleton ───────────────────────────────────────

_mv_client: MilvusClient | None = None
_mv_lock = asyncio.Lock()


async def _get_milvus() -> MilvusClient:
    global _mv_client
    if _mv_client is not None:
        return _mv_client
    async with _mv_lock:
        if _mv_client is not None:
            return _mv_client
        s = get_settings()
        _mv_client = MilvusClient(uri=f"http://{s.milvus_host}:{s.milvus_port}")
        return _mv_client


# ── Filter expression builder ─────────────────────────────────────


def _escape_str(v: str) -> str:
    """Safe VARCHAR literal for Milvus filter expression."""
    return v.replace("\\", "\\\\").replace('"', '\\"')


# Low-quality doc types that kb_service marks low_quality=True. Phase A
# excludes them via _pick_specs; Phase B needs a mirror filter because the
# Milvus collection stores every doc_type, including WeChat aggregators.
# Keep this list in sync with ``CollectionSpec.low_quality=True`` entries.
_LOW_QUALITY_DOC_TYPES: tuple[str, ...] = ("wechat_article",)


def _build_milvus_expr(
    *,
    tickers: list[str] | None,
    date_range: dict | None,
    doc_types: list[str] | None,
    sources: list[str] | None,
    include_low_quality: bool = False,
) -> str:
    """Build a Milvus scalar filter expression (pre-filter before ANN).

    Pre-filter is strictly preferred over post-filter: Milvus fuses it into the
    HNSW / SPARSE_WAND traversal, avoiding the tiny-recall trap of post-filter.
    """
    clauses: list[str] = []

    # Tickers (expanded canonical forms ORed over ARRAY_CONTAINS_ANY).
    if tickers:
        norm: list[str] = []
        for t in tickers:
            norm.extend(normalize_ticker_input(t))
        norm = list(dict.fromkeys(norm))  # dedup preserve order
        if norm:
            lit = ", ".join(f'"{_escape_str(t)}"' for t in norm)
            clauses.append(f"ARRAY_CONTAINS_ANY(tickers, [{lit}])")

    # Date range (release_time_ms is INT64 epoch ms).
    if date_range:
        gte = date_range.get("gte")
        lte = date_range.get("lte")
        if gte:
            g = _str_to_ms(gte) if isinstance(gte, str) else int(gte)
            if g:
                clauses.append(f"release_time_ms >= {g}")
        if lte:
            l = _str_to_ms(lte, end_of_day=True) if isinstance(lte, str) else int(lte)
            if l:
                clauses.append(f"release_time_ms <= {l}")

    # Doc types (canonical enum: report, roadshow, meeting, ...).
    if doc_types:
        lit = ", ".join(f'"{_escape_str(d)}"' for d in doc_types)
        clauses.append(f"doc_type in [{lit}]")
    elif not include_low_quality and _LOW_QUALITY_DOC_TYPES:
        # No explicit doc_type filter → exclude low-quality automatically so
        # wechat_article etc. don't pollute semantic ranking. Milvus does not
        # support ``not in``; emit an explicit !=/and chain instead.
        for dt in _LOW_QUALITY_DOC_TYPES:
            clauses.append(f'doc_type != "{_escape_str(dt)}"')

    # Sources (platform / db: alphapai | jinmen | meritco | thirdbridge | funda | gangtise | acecamp).
    if sources:
        lit = ", ".join(f'"{_escape_str(s)}"' for s in sources)
        clauses.append(f"db in [{lit}]")

    return " and ".join(clauses) if clauses else ""


# ── Core hybrid query ──────────────────────────────────────────────


_OUTPUT_FIELDS = (
    "chunk_id", "doc_id", "db", "collection",
    "source_type", "doc_type", "doc_type_cn",
    "title", "release_time_ms", "tickers",
    "chunk_index", "char_start", "char_end",
    "chunk_hash", "parent_id", "parent_text",
    "lang", "snippet_text", "url",
)


async def _milvus_hybrid(
    *,
    mv: MilvusClient,
    collection: str,
    q_vec: list[float] | None,
    q_text: str,
    expr: str,
    top_k: int,
) -> list[dict]:
    """One hybrid_search RPC. Either dense+sparse or sparse-only (if q_vec is None)."""
    reqs: list[AnnSearchRequest] = []
    if q_vec is not None:
        reqs.append(AnnSearchRequest(
            data=[q_vec],
            anns_field="dense_vector",
            param={"metric_type": "COSINE", "params": {"ef": HNSW_EF}},
            limit=DENSE_CANDIDATES,
            expr=expr or None,
        ))
    reqs.append(AnnSearchRequest(
        data=[q_text],
        anns_field="sparse_vector",
        param={"metric_type": "BM25", "params": {"drop_ratio_search": BM25_DROP_RATIO}},
        limit=SPARSE_CANDIDATES,
        expr=expr or None,
    ))

    if len(reqs) == 1:
        # BM25-only fallback (TEI down). Can't use hybrid_search with one request
        # in all pymilvus versions — use plain search.
        try:
            rows = mv.search(
                collection_name=collection,
                data=[q_text],
                anns_field="sparse_vector",
                search_params={
                    "metric_type": "BM25",
                    "params": {"drop_ratio_search": BM25_DROP_RATIO},
                },
                limit=top_k,
                expr=expr or None,
                output_fields=list(_OUTPUT_FIELDS),
            )
        except Exception as e:
            logger.error("BM25-only search failed: %s", e)
            return []
        return [dict(hit) for hit in (rows[0] if rows else [])]

    # Dense + sparse with RRF fusion.
    ranker = RRFRanker(k=60)
    try:
        rows = mv.hybrid_search(
            collection_name=collection,
            reqs=reqs,
            ranker=ranker,
            limit=RRF_LIMIT,
            output_fields=list(_OUTPUT_FIELDS),
        )
    except Exception as e:
        logger.error("hybrid_search failed: %s", e)
        return []

    return [dict(hit) for hit in (rows[0] if rows else [])]


# ── Per-doc diversity cap ─────────────────────────────────────────


def _per_doc_cap(hits: list[dict], cap: int) -> list[dict]:
    """Keep at most `cap` chunks per doc_id, preserving RRF rank order."""
    by_doc: dict[str, int] = defaultdict(int)
    kept: list[dict] = []
    for h in hits:
        doc_id = h.get("doc_id") or ""
        if by_doc[doc_id] >= cap:
            continue
        by_doc[doc_id] += 1
        kept.append(h)
    return kept


# ── Hit normalization (kb_service.search-compatible shape) ────────


def _normalize_hit(row: dict, score: float) -> dict:
    """Translate a Milvus row + rank score into the LLM-facing shape.

    Compatible superset of kb_service._normalize_hit output, adding chunk-level
    fields. Legacy keys preserved verbatim so the existing _format_kb_hits and
    CitationTracker.add_kb_items paths work unchanged.
    """
    entity = row.get("entity") or row  # Milvus returns either depending on API
    # Fields can live at top level or under 'entity'; try both.
    def g(k: str, default=None):
        return entity.get(k, row.get(k, default))

    parent = (g("parent_text") or "")[:500].replace("\n", " ").strip()
    snippet = (g("snippet_text") or parent[:240]).replace("\n", " ").strip()
    release_ms = int(g("release_time_ms") or 0) or None
    date_str = ""
    if release_ms:
        from datetime import datetime, timezone
        try:
            date_str = datetime.fromtimestamp(release_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
        except (ValueError, OSError, OverflowError):
            date_str = ""

    return {
        # ── Legacy shape (kb_service.search compatible) ──
        "doc_id": g("doc_id"),
        "source": g("db"),
        "doc_type": g("doc_type"),
        "doc_type_cn": g("doc_type_cn"),
        "title": g("title") or "",
        "snippet": parent[:320] or snippet,
        "date": date_str,
        "release_ms": release_ms,
        "institution": "",  # kb_chunks does not store institution separately
        "tickers": list(g("tickers") or []),
        "url": g("url") or "",
        "text_len": len(g("parent_text") or ""),
        "score": round(float(score), 4),
        # ── New chunk-level fields (CitationTracker.add_kb_items consumes) ──
        "chunk_id": g("chunk_id"),
        "chunk_index": int(g("chunk_index") or 0),
        "char_start": int(g("char_start") or 0),
        "char_end": int(g("char_end") or 0),
        "collection": g("collection"),
        "snippet_text": snippet,
    }


# ── Public entry ─────────────────────────────────────────────────


async def hybrid_search(
    query: str = "",
    *,
    tickers: list[str] | None = None,
    doc_types: list[str] | None = None,
    sources: list[str] | None = None,
    date_range: dict | None = None,
    top_k: int = 20,
    include_low_quality: bool = False,
) -> list[dict]:
    """Drop-in vector + BM25 replacement for kb_service.search.

    Signature identical to ``kb_service.search`` so the kernel swap needs zero
    caller changes. top_k defaults to 20 (vector path can usefully return more
    than the old default of 8) but callers that pass their own top_k are honored.
    """
    q = (query or "").strip()
    if not q:
        # No query → fall back to metadata-only list via Milvus. Return empty
        # for now; future improvement: sort by release_time_ms desc.
        return []

    top_k = max(1, min(int(top_k or 20), 50))

    s = get_settings()

    # Embed query (LRU-cached). On TEI failure, degrade to BM25-only.
    q_vec: list[float] | None = None
    t0 = time.monotonic()
    try:
        q_vec = await tei_client.embed_query(q)
    except TEICircuitOpen:
        logger.warning("TEI circuit open → degrading to BM25-only for this query")
    except TEIError as e:
        logger.warning("TEI embed failed (%s) → degrading to BM25-only", e)
    embed_ms = int((time.monotonic() - t0) * 1000)

    expr = _build_milvus_expr(
        tickers=tickers,
        date_range=date_range,
        doc_types=doc_types,
        sources=sources,
        include_low_quality=include_low_quality,
    )

    mv = await _get_milvus()
    t1 = time.monotonic()
    raw = await asyncio.to_thread(
        _milvus_hybrid_sync,
        mv, s.effective_milvus_collection, q_vec, q, expr, top_k,
    )
    milvus_ms = int((time.monotonic() - t1) * 1000)

    # Apply per-doc cap for diversity.
    capped = _per_doc_cap(raw, PER_DOC_CAP)
    capped = capped[:top_k]

    logger.info(
        "kb_vector hybrid_search: q=%r results=%d embed_ms=%d milvus_ms=%d mode=%s",
        q[:60], len(capped), embed_ms, milvus_ms,
        "hybrid" if q_vec is not None else "bm25_only",
    )

    # Normalize — Milvus returns a distance/score per row; use that.
    out: list[dict] = []
    for rank, row in enumerate(capped, start=1):
        raw = row.get("distance")
        # Preserve legitimate 0.0 scores (cosine mean perfect match under some
        # metrics). Fall back to reciprocal rank only when score is truly absent.
        score = float(raw) if raw is not None else (1.0 / rank)
        out.append(_normalize_hit(row, score))
    return out


def _milvus_hybrid_sync(
    mv: MilvusClient,
    collection: str,
    q_vec: list[float] | None,
    q_text: str,
    expr: str,
    top_k: int,
) -> list[dict]:
    """Sync wrapper around _milvus_hybrid for asyncio.to_thread."""
    # We cannot call an async function from to_thread; so duplicate the body
    # here without async/await. Milvus client is sync-native.
    reqs: list[AnnSearchRequest] = []
    if q_vec is not None:
        reqs.append(AnnSearchRequest(
            data=[q_vec],
            anns_field="dense_vector",
            param={"metric_type": "COSINE", "params": {"ef": HNSW_EF}},
            limit=DENSE_CANDIDATES,
            expr=expr or None,
        ))
    reqs.append(AnnSearchRequest(
        data=[q_text],
        anns_field="sparse_vector",
        param={"metric_type": "BM25", "params": {"drop_ratio_search": BM25_DROP_RATIO}},
        limit=SPARSE_CANDIDATES,
        expr=expr or None,
    ))

    if len(reqs) == 1:
        try:
            rows = mv.search(
                collection_name=collection,
                data=[q_text],
                anns_field="sparse_vector",
                search_params={
                    "metric_type": "BM25",
                    "params": {"drop_ratio_search": BM25_DROP_RATIO},
                },
                limit=top_k,
                expr=expr or None,
                output_fields=list(_OUTPUT_FIELDS),
            )
        except Exception as e:
            logger.error("BM25-only search failed: %s", e)
            return []
        return [dict(hit) for hit in (rows[0] if rows else [])]

    ranker = RRFRanker(k=60)
    try:
        rows = mv.hybrid_search(
            collection_name=collection,
            reqs=reqs,
            ranker=ranker,
            limit=RRF_LIMIT,
            output_fields=list(_OUTPUT_FIELDS),
        )
    except Exception as e:
        logger.error("hybrid_search failed: %s", e)
        return []
    return [dict(hit) for hit in (rows[0] if rows else [])]
