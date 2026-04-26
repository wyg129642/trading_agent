"""Fire-and-forget metrics emitter for KB-related chat tools.

Called from ``kb_service.execute_tool`` and ``user_kb_tools.execute_tool``
after each search/fetch. Writes one row per call to ``kb_search_metrics``
in Postgres. Failures are swallowed (best-effort) — the chat path is the
hot path and must never block on observability.

Read side lives in ``backend/app/api/admin_kb_metrics.py``.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from backend.app.core.database import async_session_factory
from backend.app.models.kb_metrics import KbSearchMetric

logger = logging.getLogger(__name__)


def _safe_get_trace_id() -> str:
    try:
        from backend.app.services.chat_debug import get_current_trace_id
        return get_current_trace_id() or ""
    except Exception:
        return ""


async def _async_insert(row: dict) -> None:
    """Write a single metric row in its own session."""
    try:
        async with async_session_factory() as session:  # type: AsyncSession
            session.add(KbSearchMetric(**row))
            await session.commit()
    except Exception as e:
        # Don't crash the caller; just warn once. Volume is low enough that
        # individual losses don't matter.
        logger.warning("kb_metrics insert failed: %s", e)


def record_kb_search(
    *,
    tool_name: str,
    query: str = "",
    tickers: list[str] | None = None,
    date_range: dict | None = None,
    top_k: int = 0,
    result_count: int = 0,
    embed_ms: int = 0,
    milvus_ms: int = 0,
    mongo_ms: int = 0,
    total_ms: int = 0,
    mode: str = "",
    error: str = "",
    user_id: str = "",
) -> None:
    """Schedule a metric insert; never raises.

    The call returns immediately; the actual DB write runs as a background
    task on the running event loop. If there's no running loop (e.g. unit
    test calling this synchronously), the metric is silently dropped.
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        # No event loop — nothing we can do; drop the metric.
        return

    # Truncate the query so giant strings don't bloat the table.
    q = (query or "")[:1000]
    row = {
        "ts": datetime.now(timezone.utc),
        "trace_id": _safe_get_trace_id(),
        "user_id": user_id or "",
        "tool_name": tool_name or "",
        "query": q,
        "query_len": len(query or ""),
        "ticker_count": len(tickers or []),
        "has_date_filter": 1 if date_range else 0,
        "top_k": int(top_k or 0),
        "result_count": int(result_count or 0),
        "embed_ms": int(embed_ms or 0),
        "milvus_ms": int(milvus_ms or 0),
        "mongo_ms": int(mongo_ms or 0),
        "total_ms": int(total_ms or 0),
        "mode": (mode or "")[:20],
        "error": (error or "")[:500],
    }
    # Fire-and-forget — don't await.
    loop.create_task(_async_insert(row))
