"""Topic cluster results — stores clustering anomaly detection output."""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import Integer, Date, DateTime, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from backend.app.core.database import Base


class TopicClusterResult(Base):
    """Stores results from periodic topic clustering over enriched data.

    Each row represents one clustering run. The anomalies field contains
    clusters that are significantly larger than average, indicating
    abnormal topic concentration (potential market-moving events).
    """
    __tablename__ = "topic_cluster_results"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    cluster_date: Mapped[datetime] = mapped_column(Date, nullable=False)
    run_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False,
    )
    total_items: Mapped[int] = mapped_column(Integer, default=0)
    n_clusters: Mapped[int] = mapped_column(Integer, default=0)
    anomalies: Mapped[dict] = mapped_column(JSONB, default=list, server_default="[]")
    top_clusters: Mapped[dict] = mapped_column(JSONB, default=list, server_default="[]")
    summary: Mapped[str] = mapped_column(Text, default="", server_default="")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False,
    )

    def __repr__(self) -> str:
        return f"<TopicClusterResult {self.cluster_date} items={self.total_items} anomalies={len(self.anomalies or [])}>"
