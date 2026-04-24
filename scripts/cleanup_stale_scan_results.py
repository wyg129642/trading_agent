"""One-shot cleanup: flag existing stale rows in portfolio_scan_results.

The freshness gate (engine/proactive/freshness_gate.py) marks new rows with
`full_analysis.rejection_reason="event_too_old"` when the underlying event
is too old and has no recent corroborating source. The API's /breaking-news
endpoint filters on that marker.

This script backfills that marker for historical rows written before the
gate shipped, so the UI stops showing week-old "breaking news" that slipped
past the old pipeline.

Usage:
    PYTHONPATH=. python3 scripts/cleanup_stale_scan_results.py [--dry-run]
    PYTHONPATH=. python3 scripts/cleanup_stale_scan_results.py --threshold-hours 48
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from datetime import datetime, timezone

import asyncpg

sys.path.insert(0, "/home/ygwang/trading_agent_staging")

from backend.app.config import get_settings  # noqa: E402

settings = get_settings()

logger = logging.getLogger("cleanup_stale")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def _dsn_for_asyncpg(url: str) -> str:
    """FastAPI uses postgresql+asyncpg://; asyncpg wants postgresql://."""
    return url.replace("postgresql+asyncpg://", "postgresql://")


def _parse_iso(value):
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if not isinstance(value, str):
        return None
    try:
        from dateutil import parser as dateutil_parser
        dt = dateutil_parser.parse(value, fuzzy=True)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except (ValueError, OverflowError, TypeError):
        return None


async def main(threshold_hours: int, dry_run: bool) -> None:
    dsn = _dsn_for_asyncpg(settings.database_url)
    logger.info("Connecting to %s", dsn.split("@")[-1])
    conn = await asyncpg.connect(dsn)

    try:
        rows = await conn.fetch(
            """
            SELECT id, ticker, scan_time, snapshot_summary, full_analysis, news_timeline
            FROM portfolio_scan_results
            WHERE delta_detected = true
              AND delta_magnitude IN ('material', 'critical')
              AND (
                full_analysis IS NULL
                OR full_analysis->>'rejection_reason' IS NULL
                OR full_analysis->>'rejection_reason' != 'event_too_old'
              )
            """
        )
    except Exception as exc:
        logger.error("Query failed: %s", exc)
        await conn.close()
        return

    now = datetime.now(timezone.utc)
    stale_updates: list[tuple[str, float, dict]] = []

    for row in rows:
        snapshot = row["snapshot_summary"]
        if isinstance(snapshot, str):
            try:
                snapshot = json.loads(snapshot)
            except Exception:
                snapshot = {}
        snapshot = snapshot or {}

        analysis = row["full_analysis"]
        if isinstance(analysis, str):
            try:
                analysis = json.loads(analysis)
            except Exception:
                analysis = {}
        analysis = analysis or {}

        timeline = row["news_timeline"]
        if isinstance(timeline, str):
            try:
                timeline = json.loads(timeline)
            except Exception:
                timeline = []
        timeline = timeline or []

        # Earliest source time: snapshot.earliest_report_time (LLM output) →
        # first news_timeline entry.
        earliest_iso = snapshot.get("earliest_report_time")
        if not earliest_iso and timeline:
            first = timeline[0] if isinstance(timeline[0], dict) else {}
            earliest_iso = first.get("time")
        earliest_dt = _parse_iso(earliest_iso)
        if earliest_dt is None:
            continue

        age_hours = max(0.0, (now - earliest_dt).total_seconds() / 3600)
        if age_hours <= threshold_hours:
            continue

        # Check for recent corroboration in any listed source
        has_recent = False
        cutoff = now.timestamp() - threshold_hours * 3600
        for entry in timeline:
            if not isinstance(entry, dict):
                continue
            dt = _parse_iso(entry.get("time"))
            if dt and dt.timestamp() >= cutoff:
                has_recent = True
                break
        if not has_recent:
            for src in (analysis.get("sources") or []):
                if not isinstance(src, dict):
                    continue
                dt = _parse_iso(src.get("date") or src.get("time"))
                if dt and dt.timestamp() >= cutoff:
                    has_recent = True
                    break
        if has_recent:
            continue

        # Stamp the rejection into a copy of full_analysis
        new_analysis = dict(analysis)
        new_analysis["event_age_hours"] = round(age_hours, 2)
        new_analysis["earliest_source_time"] = earliest_dt.isoformat()
        new_analysis["rejection_reason"] = "event_too_old"
        new_analysis["freshness_gate"] = "rejected_event_too_old"
        new_analysis["novelty_status_llm_original"] = snapshot.get("novelty_status", "")
        stale_updates.append((row["id"], age_hours, new_analysis))

    logger.info(
        "Found %d stale rows (event_age > %dh) out of %d candidates",
        len(stale_updates), threshold_hours, len(rows),
    )

    if dry_run:
        for rid, age, _ in stale_updates[:20]:
            logger.info("  [dry-run] would flag %s (event_age=%.1fh)", rid, age)
        if len(stale_updates) > 20:
            logger.info("  ... %d more", len(stale_updates) - 20)
        await conn.close()
        return

    updated = 0
    for rid, _age, new_analysis in stale_updates:
        await conn.execute(
            """UPDATE portfolio_scan_results
               SET should_alert = false,
                   full_analysis = $1::jsonb
               WHERE id = $2""",
            json.dumps(new_analysis, ensure_ascii=False),
            rid,
        )
        updated += 1

    logger.info("Updated %d rows.", updated)
    await conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--threshold-hours", type=int, default=48)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    asyncio.run(main(args.threshold_hours, args.dry_run))
