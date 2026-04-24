"""Per-stock baseline persistence — Redis (hot cache) + PostgreSQL (source of truth).

v3: Shifted from narrative comparison to event dedup log.
The baseline tracks known events via hashes to prevent re-alerting
on the same event across scan cycles.
"""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone

from engine.proactive.models import StockBaseline

logger = logging.getLogger(__name__)


def _parse_jsonb(val) -> list | dict:
    """Parse asyncpg JSONB value (returned as string) into Python object."""
    if isinstance(val, (list, dict)):
        return val
    if isinstance(val, str):
        try:
            return json.loads(val)
        except (json.JSONDecodeError, TypeError):
            return []
    return []


class BaselineStore:
    """Manage per-stock baselines with Redis cache and PostgreSQL persistence."""

    REDIS_PREFIX = "proactive:baseline:"
    REDIS_TTL = 86400  # 24 hours

    def __init__(self, db, redis=None):
        self._db = db
        self._redis = redis

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    async def load(self, ticker: str) -> StockBaseline | None:
        """Load baseline for a ticker. Redis first, then PostgreSQL fallback."""
        if self._redis:
            try:
                raw = await self._redis.get(f"{self.REDIS_PREFIX}{ticker}")
                if raw:
                    return StockBaseline.from_dict(json.loads(raw))
            except Exception as e:
                logger.debug("Redis baseline read failed for %s: %s", ticker, e)

        return await self._load_from_pg(ticker)

    async def _load_from_pg(self, ticker: str) -> StockBaseline | None:
        """Load a single baseline from PostgreSQL."""
        pool = self._db._pool
        if not pool:
            return None

        row = await pool.fetchrow(
            """SELECT ticker, last_scan_at, last_narrative,
                      known_developments, known_content_ids,
                      known_event_hashes,
                      sentiment_history, scan_count, alert_count, last_alert_at
               FROM portfolio_scan_baselines
               WHERE ticker = $1""",
            ticker,
        )
        if not row:
            return None

        return StockBaseline.from_dict({
            "ticker": row["ticker"],
            "last_scan_time": row["last_scan_at"].isoformat() if row["last_scan_at"] else None,
            "last_narrative": row["last_narrative"] or "",
            "known_developments": _parse_jsonb(row["known_developments"]),
            "known_content_ids": _parse_jsonb(row["known_content_ids"]),
            "known_event_hashes": _parse_jsonb(row.get("known_event_hashes") or "[]"),
            "sentiment_history": _parse_jsonb(row["sentiment_history"]),
            "scan_count": row["scan_count"] or 0,
            "alert_count": row["alert_count"] or 0,
            "last_alert_at": row["last_alert_at"].isoformat() if row["last_alert_at"] else None,
        })

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    async def save(self, baseline: StockBaseline) -> None:
        """Write-through save: update both Redis and PostgreSQL."""
        data = baseline.to_dict()

        if self._redis:
            try:
                await self._redis.set(
                    f"{self.REDIS_PREFIX}{baseline.ticker}",
                    json.dumps(data, ensure_ascii=False),
                    ex=self.REDIS_TTL,
                )
            except Exception as e:
                logger.debug("Redis baseline write failed for %s: %s", baseline.ticker, e)

        await self._save_to_pg(baseline)

    async def _save_to_pg(self, baseline: StockBaseline) -> None:
        """Upsert baseline into PostgreSQL."""
        pool = self._db._pool
        if not pool:
            return

        last_scan = baseline.last_scan_time
        if last_scan.tzinfo is None:
            last_scan = last_scan.replace(tzinfo=timezone.utc)

        last_alert = baseline.last_alert_at
        if last_alert and last_alert.tzinfo is None:
            last_alert = last_alert.replace(tzinfo=timezone.utc)

        await pool.execute(
            """INSERT INTO portfolio_scan_baselines
                   (ticker, name_cn, market, last_scan_at, last_narrative,
                    known_developments, known_content_ids, known_event_hashes,
                    sentiment_history,
                    scan_count, alert_count, last_alert_at, updated_at)
               VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8::jsonb, $9::jsonb,
                       $10, $11, $12, now())
               ON CONFLICT (ticker) DO UPDATE SET
                   last_scan_at = EXCLUDED.last_scan_at,
                   last_narrative = EXCLUDED.last_narrative,
                   known_developments = EXCLUDED.known_developments,
                   known_content_ids = EXCLUDED.known_content_ids,
                   known_event_hashes = EXCLUDED.known_event_hashes,
                   sentiment_history = EXCLUDED.sentiment_history,
                   scan_count = EXCLUDED.scan_count,
                   alert_count = EXCLUDED.alert_count,
                   last_alert_at = EXCLUDED.last_alert_at,
                   updated_at = now()""",
            baseline.ticker,
            "",  # name_cn — filled on first insert
            "",  # market — filled on first insert
            last_scan,
            baseline.last_narrative,
            json.dumps(baseline.known_developments[-30:], ensure_ascii=False),
            json.dumps(list(baseline.known_content_ids)[-500:], ensure_ascii=False),
            json.dumps(list(baseline.known_event_hashes)[-500:], ensure_ascii=False),
            json.dumps(
                [(t.isoformat(), s) for t, s in baseline.sentiment_history[-20:]],
                ensure_ascii=False,
            ),
            baseline.scan_count,
            baseline.alert_count,
            last_alert,
        )

    # ------------------------------------------------------------------
    # Initialize
    # ------------------------------------------------------------------

    async def initialize_baseline(
        self, ticker: str, name_cn: str, market: str,
        name_en: str = "", narrative: str = "",
    ) -> StockBaseline:
        """Create a fresh baseline for a stock (first-time setup).

        Uses ON CONFLICT DO UPDATE for name_cn/name_en/market to keep
        stock metadata up to date even if the baseline already exists.
        """
        baseline = StockBaseline(
            ticker=ticker,
            last_scan_time=datetime.now(timezone.utc),
            last_narrative=narrative,
        )

        pool = self._db._pool
        if pool:
            await pool.execute(
                """INSERT INTO portfolio_scan_baselines
                       (ticker, name_cn, name_en, market, last_scan_at, last_narrative,
                        known_developments, known_content_ids, known_event_hashes,
                        sentiment_history,
                        scan_count, alert_count, updated_at)
                   VALUES ($1, $2, $3, $4, $5, $6, '[]'::jsonb, '[]'::jsonb, '[]'::jsonb,
                           '[]'::jsonb, 0, 0, now())
                   ON CONFLICT (ticker) DO UPDATE SET
                       name_cn = CASE WHEN EXCLUDED.name_cn != '' THEN EXCLUDED.name_cn
                                      ELSE portfolio_scan_baselines.name_cn END,
                       name_en = CASE WHEN EXCLUDED.name_en != '' THEN EXCLUDED.name_en
                                      ELSE portfolio_scan_baselines.name_en END,
                       market = CASE WHEN EXCLUDED.market != '' THEN EXCLUDED.market
                                     ELSE portfolio_scan_baselines.market END""",
                ticker, name_cn, name_en, market,
                baseline.last_scan_time,
                narrative,
            )

        await self.save(baseline)
        return baseline

    # ------------------------------------------------------------------
    # Event dedup (v3)
    # ------------------------------------------------------------------

    @staticmethod
    def _hash_event(event_summary: str) -> str:
        """Hash a normalized event summary for dedup."""
        normalized = event_summary.strip().lower()
        return hashlib.sha256(normalized.encode()).hexdigest()[:16]

    def add_known_event(self, baseline: StockBaseline, event_summary: str) -> None:
        """Add an event to the dedup log. Mutates in place."""
        h = self._hash_event(event_summary)
        baseline.known_event_hashes.add(h)
        # Keep bounded
        if len(baseline.known_event_hashes) > 500:
            baseline.known_event_hashes = set(list(baseline.known_event_hashes)[-400:])

    def is_known_event(self, baseline: StockBaseline, event_summary: str) -> bool:
        """Check if an event was already seen."""
        h = self._hash_event(event_summary)
        return h in baseline.known_event_hashes

    # ------------------------------------------------------------------
    # Update helpers
    # ------------------------------------------------------------------

    def update_after_scan(
        self,
        baseline: StockBaseline,
        new_content_ids: set[str],
        sentiment: str | None = None,
    ) -> None:
        """Update baseline after any scan. Mutates in place.

        v3: No longer updates last_narrative (narrative comparison removed).
        """
        baseline.last_scan_time = datetime.now(timezone.utc)
        baseline.known_content_ids.update(new_content_ids)
        baseline.scan_count += 1

        if sentiment:
            baseline.sentiment_history.append(
                (datetime.now(timezone.utc), sentiment)
            )
            baseline.sentiment_history = baseline.sentiment_history[-20:]

    def update_after_delta(
        self,
        baseline: StockBaseline,
        new_developments: list[str],
    ) -> None:
        """Update baseline after breaking news events are detected."""
        baseline.known_developments.extend(new_developments)
        baseline.known_developments = baseline.known_developments[-30:]

    def update_after_alert(self, baseline: StockBaseline) -> None:
        """Update baseline after an alert is sent."""
        now = datetime.now(timezone.utc)
        baseline.alert_count += 1
        baseline.last_alert_at = now
        baseline.recent_alert_times.append(now)
        baseline.recent_alert_times = baseline.recent_alert_times[-20:]

    # ------------------------------------------------------------------
    # Scan result persistence
    # ------------------------------------------------------------------

    async def save_scan_result(self, result) -> None:
        """Persist a ProactiveScanResult to the audit trail table."""
        from engine.proactive.models import ProactiveScanResult

        pool = self._db._pool
        if not pool:
            return

        r: ProactiveScanResult = result
        scan_time = r.scan_time
        if scan_time.tzinfo is None:
            scan_time = scan_time.replace(tzinfo=timezone.utc)

        import uuid
        result_id = uuid.uuid4().hex[:16]

        await pool.execute(
            """INSERT INTO portfolio_scan_results
                   (id, ticker, scan_time, internal_item_count, internal_new_count,
                    external_result_count, delta_detected, delta_magnitude,
                    delta_description, new_developments, deep_research_performed,
                    research_iterations, key_findings, news_timeline,
                    referenced_sources, should_alert, alert_confidence,
                    alert_rationale, full_analysis, snapshot_summary,
                    tokens_used, cost_cny, created_at)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11,$12,$13::jsonb,
                       $14::jsonb,$15::jsonb,$16,$17,$18,$19::jsonb,$20::jsonb,
                       $21,$22,now())""",
            result_id,
            r.holding.ticker,
            scan_time,
            r.snapshot.internal_context.total_count,
            r.snapshot.internal_context.new_count,
            r.snapshot.external_context.total_results,
            r.breaking_news_detected,
            r.news_materiality,
            r.news_summary,
            json.dumps(r.new_developments, ensure_ascii=False),
            r.deep_research_performed,
            r.research_iterations,
            json.dumps(r.key_findings, ensure_ascii=False),
            json.dumps(r.news_timeline, ensure_ascii=False),
            json.dumps(r.referenced_sources, ensure_ascii=False),
            r.should_alert,
            r.alert_confidence,
            r.alert_rationale,
            json.dumps(r.full_analysis or {}, ensure_ascii=False),
            json.dumps({
                "price_data_len": len(r.snapshot.price_data),
                "internal_sources": list(r.snapshot.internal_context.source_items.keys()),
                "sentiment_dist": r.snapshot.internal_context.sentiment_distribution,
                "novelty_status": r.novelty_status,
                "earliest_report_time": r.earliest_report_time.isoformat() if r.earliest_report_time else None,
                "historical_precedents": r.historical_precedents[:5],
            }, ensure_ascii=False),
            r.tokens_used,
            r.cost_cny,
        )
