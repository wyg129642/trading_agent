"""Topic clustering service — detects abnormal topic concentration in enriched data.

Runs every 2 hours, clusters the past 24 hours of enriched AlphaPai + Jiuqian
data using SentenceTransformer embeddings + KMeans. When a cluster is
significantly larger than average (>2.5x), it's flagged as an anomaly and
optionally triggers a Feishu alert.

This is zero-LLM-cost: all computation is local (CPU embeddings + KMeans).
"""
from __future__ import annotations

import asyncio
import logging
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.app.config import Settings
from backend.app.core.database import async_session_factory
from backend.app.models.alphapai import AlphaPaiArticle, AlphaPaiComment
from backend.app.models.jiuqian import JiuqianForum, JiuqianMinutes, JiuqianWechat
from backend.app.models.topic_cluster import TopicClusterResult

logger = logging.getLogger(__name__)

# Anomaly detection thresholds
ANOMALY_MULTIPLIER = 2.5  # Cluster must be this many times larger than average
ANOMALY_MIN_PCT = 0.05    # Cluster must represent at least 5% of total items
MIN_ITEMS_FOR_CLUSTERING = 30  # Don't cluster if fewer than this many items
MAX_TEXT_LEN = 300  # Max chars per text for embedding


def _safe_enrichment(val) -> dict:
    """Ensure enrichment is a dict (sometimes stored as JSON string)."""
    if isinstance(val, dict):
        return val
    if isinstance(val, str) and val:
        import json
        try:
            return json.loads(val)
        except (json.JSONDecodeError, ValueError):
            pass
    return {}


class TopicClusterService:
    """Background service: periodic topic clustering with anomaly detection."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self._model = None  # Lazy-loaded SentenceTransformer
        self._running = False
        self._task: asyncio.Task | None = None
        self._interval = 7200  # 2 hours

    def _get_model(self):
        """Lazy-load the SentenceTransformer model (first call downloads ~470MB)."""
        if self._model is None:
            from sentence_transformers import SentenceTransformer
            self._model = SentenceTransformer("paraphrase-multilingual-MiniLM-L12-v2")
            logger.info("[TopicCluster] SentenceTransformer model loaded")
        return self._model

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #
    async def start(self) -> None:
        self._running = True
        self._task = asyncio.create_task(self._loop(), name="topic_cluster")
        logger.info("[TopicCluster] Service started (interval=%ds)", self._interval)

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("[TopicCluster] Service stopped")

    async def _loop(self) -> None:
        # Wait for enrichment services to populate data first
        await asyncio.sleep(300)
        while self._running:
            try:
                await self._run_clustering()
            except Exception:
                logger.exception("[TopicCluster] Clustering error")
            # Sleep in 1s increments for responsive shutdown
            for _ in range(self._interval):
                if not self._running:
                    return
                await asyncio.sleep(1)

    # ------------------------------------------------------------------ #
    # Core clustering logic
    # ------------------------------------------------------------------ #
    async def _run_clustering(self) -> None:
        """Cluster the past 24h of enriched data and detect anomalies."""
        async with async_session_factory() as db:
            texts, metadata = await self._gather_texts(db)

        if len(texts) < MIN_ITEMS_FOR_CLUSTERING:
            logger.info("[TopicCluster] Only %d items, skipping (min=%d)", len(texts), MIN_ITEMS_FOR_CLUSTERING)
            return

        logger.info("[TopicCluster] Clustering %d items...", len(texts))

        # Encode in thread pool (CPU-bound, ~30s for 5000 texts)
        model = self._get_model()
        embeddings = await asyncio.to_thread(
            model.encode, texts, show_progress_bar=False, batch_size=128,
        )

        # KMeans clustering
        from sklearn.cluster import KMeans
        n_clusters = max(5, min(len(texts) // 10, 50))
        kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
        labels = await asyncio.to_thread(kmeans.fit_predict, embeddings)

        # Analyze cluster sizes
        cluster_sizes = Counter(int(l) for l in labels)
        total = len(texts)
        avg_size = total / n_clusters

        # Detect anomalies
        anomalies = []
        for cluster_id, size in cluster_sizes.most_common(10):
            pct = size / total
            if size > avg_size * ANOMALY_MULTIPLIER and pct >= ANOMALY_MIN_PCT:
                indices = [i for i, l in enumerate(labels) if int(l) == cluster_id]
                rep_titles = [metadata[i]["title"] for i in indices[:8]]
                tickers = self._extract_tickers(indices, metadata)
                anomalies.append({
                    "cluster_id": cluster_id,
                    "size": size,
                    "pct": round(pct, 3),
                    "representative_titles": rep_titles,
                    "tickers": tickers,
                })

        # Build top clusters summary
        top_clusters = []
        for cluster_id, size in cluster_sizes.most_common(10):
            indices = [i for i, l in enumerate(labels) if int(l) == cluster_id]
            rep_titles = [metadata[i]["title"] for i in indices[:5]]
            tickers = self._extract_tickers(indices, metadata)
            top_clusters.append({
                "cluster_id": cluster_id,
                "size": size,
                "pct": round(size / total, 3),
                "representative_titles": rep_titles,
                "tickers": tickers,
            })

        # Store results
        async with async_session_factory() as db:
            result = TopicClusterResult(
                cluster_date=date.today(),
                total_items=total,
                n_clusters=n_clusters,
                anomalies=anomalies,
                top_clusters=top_clusters,
                summary=self._build_summary(anomalies, total, n_clusters),
            )
            db.add(result)
            await db.commit()

        logger.info(
            "[TopicCluster] Done: %d items, %d clusters, %d anomalies detected",
            total, n_clusters, len(anomalies),
        )

        # Send Feishu alert for anomalies
        if anomalies:
            await self._send_alert(anomalies, total, n_clusters)

    # ------------------------------------------------------------------ #
    # Data gathering
    # ------------------------------------------------------------------ #
    async def _gather_texts(self, db: AsyncSession) -> tuple[list[str], list[dict]]:
        """Gather enriched summaries from the past 24h across all data sources."""
        cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
        texts: list[str] = []
        metadata: list[dict] = []

        # AlphaPai articles
        rows = (await db.execute(
            select(AlphaPaiArticle)
            .where(AlphaPaiArticle.is_enriched.is_(True))
            .where(AlphaPaiArticle.publish_time >= cutoff)
        )).scalars().all()
        for r in rows:
            enrichment = _safe_enrichment(r.enrichment)
            if enrichment.get("skipped"):
                continue
            summary = enrichment.get("summary", "")
            text = f"{r.arc_name} {summary}"[:MAX_TEXT_LEN]
            if len(text) >= 10:
                texts.append(text)
                metadata.append({
                    "title": r.arc_name,
                    "source": "alphapai_article",
                    "enrichment": enrichment,
                })

        # AlphaPai comments
        rows = (await db.execute(
            select(AlphaPaiComment)
            .where(AlphaPaiComment.is_enriched.is_(True))
            .where(AlphaPaiComment.cmnt_date >= cutoff)
        )).scalars().all()
        for r in rows:
            enrichment = _safe_enrichment(r.enrichment)
            summary = enrichment.get("summary", "")
            text = f"{r.title} {summary}"[:MAX_TEXT_LEN]
            if len(text) >= 10:
                texts.append(text)
                metadata.append({
                    "title": r.title,
                    "source": "alphapai_comment",
                    "enrichment": enrichment,
                })

        # Jiuqian forum
        rows = (await db.execute(
            select(JiuqianForum)
            .where(JiuqianForum.is_enriched.is_(True))
            .where(JiuqianForum.meeting_time >= cutoff)
        )).scalars().all()
        for r in rows:
            enrichment = _safe_enrichment(r.enrichment)
            summary = enrichment.get("summary", "")
            text = f"{r.title} {summary}"[:MAX_TEXT_LEN]
            if len(text) >= 10:
                texts.append(text)
                metadata.append({
                    "title": r.title,
                    "source": "jiuqian_forum",
                    "enrichment": enrichment,
                })

        # Jiuqian minutes
        rows = (await db.execute(
            select(JiuqianMinutes)
            .where(JiuqianMinutes.is_enriched.is_(True))
            .where(JiuqianMinutes.pub_time >= cutoff)
        )).scalars().all()
        for r in rows:
            enrichment = _safe_enrichment(r.enrichment)
            summary = enrichment.get("summary", "")
            text = f"{r.title} {summary}"[:MAX_TEXT_LEN]
            if len(text) >= 10:
                texts.append(text)
                metadata.append({
                    "title": r.title,
                    "source": "jiuqian_minutes",
                    "enrichment": enrichment,
                })

        # Jiuqian wechat
        rows = (await db.execute(
            select(JiuqianWechat)
            .where(JiuqianWechat.is_enriched.is_(True))
            .where(JiuqianWechat.pub_time >= cutoff)
        )).scalars().all()
        for r in rows:
            enrichment = _safe_enrichment(r.enrichment)
            summary = enrichment.get("summary", "")
            text = f"{r.title} {summary}"[:MAX_TEXT_LEN]
            if len(text) >= 10:
                texts.append(text)
                metadata.append({
                    "title": r.title,
                    "source": "jiuqian_wechat",
                    "enrichment": enrichment,
                })

        logger.info(
            "[TopicCluster] Gathered %d texts (alphapai: %d, jiuqian: %d)",
            len(texts),
            sum(1 for m in metadata if m["source"].startswith("alphapai")),
            sum(1 for m in metadata if m["source"].startswith("jiuqian")),
        )
        return texts, metadata

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #
    @staticmethod
    def _extract_tickers(indices: list[int], metadata: list[dict]) -> list[dict]:
        """Extract unique tickers from a cluster's enrichment data."""
        seen: set[str] = set()
        tickers: list[dict] = []
        for i in indices:
            enrichment = metadata[i].get("enrichment", {})
            for t in enrichment.get("tickers", []):
                code = t.get("code", "") or t.get("ticker", "")
                if code and code not in seen:
                    seen.add(code)
                    tickers.append(t)
        return tickers[:10]  # Limit to 10 tickers per cluster

    @staticmethod
    def _build_summary(anomalies: list[dict], total: int, n_clusters: int) -> str:
        if not anomalies:
            return f"过去24h共{total}条数据，{n_clusters}个话题簇，未检测到异常聚集。"
        parts = [f"过去24h共{total}条数据，{n_clusters}个话题簇，检测到{len(anomalies)}个异常聚集话题："]
        for a in anomalies:
            titles_preview = "、".join(a["representative_titles"][:3])
            ticker_names = ", ".join(t.get("name", t.get("code", "")) for t in a["tickers"][:3])
            parts.append(
                f"  - 聚集{a['size']}条 ({a['pct']*100:.1f}%): {titles_preview}"
                + (f" [涉及: {ticker_names}]" if ticker_names else "")
            )
        return "\n".join(parts)

    async def _send_alert(self, anomalies: list[dict], total: int, n_clusters: int) -> None:
        """Send Feishu alert for detected anomalies."""
        try:
            import os
            feishu_url = os.getenv("FEISHU_WEBHOOK_URL", "")
            if not feishu_url:
                return

            import httpx
            msg_parts = [f"🔍 话题聚集异常检测\n过去24h共{total}条信息，{n_clusters}个话题簇：\n"]
            for a in anomalies:
                titles = "\n".join(f"  · {t}" for t in a["representative_titles"][:5])
                ticker_names = ", ".join(t.get("name", t.get("code", "")) for t in a["tickers"][:5])
                msg_parts.append(
                    f"📌 异常话题 ({a['size']}条, {a['pct']*100:.1f}%):\n{titles}"
                    + (f"\n  涉及股票: {ticker_names}" if ticker_names else "")
                )

            payload = {
                "msg_type": "text",
                "content": {"text": "\n\n".join(msg_parts)},
            }
            async with httpx.AsyncClient() as client:
                await client.post(feishu_url, json=payload, timeout=10)
            logger.info("[TopicCluster] Feishu alert sent for %d anomalies", len(anomalies))
        except Exception as e:
            logger.warning("[TopicCluster] Failed to send Feishu alert: %s", e)
