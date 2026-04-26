"""Per-call KB search metrics — fuel for the admin observability dashboard.

One row per ``kb_search`` / ``user_kb_search`` call. Postgres rather than
ClickHouse because ClickHouse is opt-in on this stack and we want the
dashboard to work everywhere.

Volume budget: ~10 KB-ish active users × ~30 calls/day × ~365 days ≈ 110k
rows/year — comfortably small for a planner-friendly btree-indexed table.
"""
from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import DateTime, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from backend.app.core.database import Base


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class KbSearchMetric(Base):
    __tablename__ = "kb_search_metrics"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ts: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow, index=True,
    )
    trace_id: Mapped[str] = mapped_column(String(64), nullable=False, default="", index=True)
    user_id: Mapped[str] = mapped_column(String(64), nullable=False, default="", index=True)
    # tool_name: ``kb_search`` | ``user_kb_search`` | ``kb_fetch_document``
    tool_name: Mapped[str] = mapped_column(String(40), nullable=False, default="", index=True)
    # truncated query for the empty-/slow-queries view; not for full-text search
    query: Mapped[str] = mapped_column(Text, nullable=False, default="")
    query_len: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    ticker_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    has_date_filter: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    top_k: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    result_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, index=True)
    embed_ms: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    milvus_ms: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    mongo_ms: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    total_ms: Mapped[int] = mapped_column(Integer, nullable=False, default=0, index=True)
    # ``hybrid`` | ``bm25_only`` | ``phase_a_only`` | ``error``
    mode: Mapped[str] = mapped_column(String(20), nullable=False, default="")
    error: Mapped[str] = mapped_column(Text, nullable=False, default="")

    def __repr__(self) -> str:  # pragma: no cover
        return f"<KbSearchMetric {self.tool_name} hits={self.result_count} {self.total_ms}ms>"
