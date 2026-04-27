"""Real-time rule-based ticker enricher — backend lifespan task.

The rule path (``scripts/enrich_tickers.py``) used to run from cron every
10 minutes. That meant a freshly crawled doc could sit unlabeled for up to
10 minutes before the rule extractor saw it, which in turn delayed the LLM
fallback (``realtime_llm_tagger``) since the LLM gates on
``_canonical_tickers_at >= now-Nh``.

This loop runs the same logic in-process every ``RULE_TAG_REALTIME_INTERVAL_SEC``
seconds (default 30s), so new docs reach ``_canonical_tickers`` ~within one
cycle of being inserted. The cron entry stays as a daily safety net but can
be widened.

Design:

- **Reuses ``enrich_collection(..., incremental=True)``** from
  ``scripts/enrich_tickers.py`` — same query (``_canonical_tickers_at:
  {$exists: false}``), same fields, same provenance. Single source of truth.
- **Per-cycle batch cap** (``RULE_TAG_REALTIME_BATCH_SIZE``, default 200 per
  collection) keeps one cycle bounded even if a backlog appears.
- **Index ensure** runs once at startup so concurrent inserts don't race the
  index build.
- **Non-fatal**: every error is logged + swallowed; the loop never tears
  down the FastAPI app.

Disabled via ``RULE_TAG_REALTIME_ENABLED=false`` in ``.env`` — keep the cron
running in that case so the rule path doesn't fall behind.
"""
from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path

from motor.motor_asyncio import AsyncIOMotorClient

from backend.app.config import Settings

# Reuse the script — it has no import-time side effects beyond its own
# sys.path insertion (the same insertion we'd do here anyway).
_REPO_ROOT = Path(__file__).resolve().parents[3]
_SCRIPTS = _REPO_ROOT / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from enrich_tickers import (  # noqa: E402
    SOURCES,
    enrich_collection,
    ensure_index,
)

from backend.app.services.ticker_normalizer import EXTRACTORS  # noqa: E402

log = logging.getLogger(__name__)


async def realtime_rule_tagger_loop(settings: Settings) -> None:
    """Lifespan task. Polls every interval seconds; never raises out."""
    if not getattr(settings, "rule_tag_realtime_enabled", True):
        log.info("realtime_rule_tagger: disabled via config")
        return

    interval = int(getattr(settings, "rule_tag_realtime_interval_sec", 30))
    batch_size = int(getattr(settings, "rule_tag_realtime_batch_size", 200))
    exclude_csv = getattr(settings, "rule_tag_realtime_exclude", "") or ""
    exclude_set = {x.strip() for x in exclude_csv.split(",") if x.strip()}

    # One client per URI — motor pools internally
    clients_by_uri: dict[str, AsyncIOMotorClient] = {}

    def _client_for(uri: str) -> AsyncIOMotorClient:
        c = clients_by_uri.get(uri)
        if c is None:
            c = AsyncIOMotorClient(uri, tz_aware=True)
            clients_by_uri[uri] = c
        return c

    # Resolve every (source, coll) target once, build the work plan, and
    # ensure the canonical-ticker index exists on each. Failure to ensure an
    # index is non-fatal — the next cycle will retry.
    plan: list[tuple[str, str, str, object]] = []  # (source, db_name, coll_name, extractor)
    for source_key, (uri_attr, db_attr, collections) in SOURCES.items():
        try:
            uri = getattr(settings, uri_attr)
            db_name = getattr(settings, db_attr)
        except AttributeError:
            log.warning(
                "realtime_rule_tagger: missing settings %s/%s; skip source %s",
                uri_attr, db_attr, source_key,
            )
            continue
        extractor = EXTRACTORS.get(source_key)
        if extractor is None:
            log.warning(
                "realtime_rule_tagger: no extractor for source %s; skip", source_key,
            )
            continue
        client = _client_for(uri)
        for coll_name in collections:
            full_tag = f"{source_key}.{coll_name}"
            if full_tag in exclude_set:
                continue
            try:
                await ensure_index(client, db_name, coll_name)
            except Exception:  # noqa: BLE001
                log.exception("realtime_rule_tagger: ensure_index failed for %s", full_tag)
            plan.append((source_key, db_name, coll_name, extractor))

    log.info(
        "realtime_rule_tagger: started (interval=%ds, batch=%d, targets=%d, excluded=%d)",
        interval, batch_size, len(plan), len(exclude_set),
    )

    try:
        while True:
            try:
                cycle_scanned = 0
                cycle_updated = 0
                cycle_with_tickers = 0
                for source_key, db_name, coll_name, extractor in plan:
                    try:
                        client = _client_for(getattr(settings, SOURCES[source_key][0]))
                        scanned, updated, with_tickers, _unmatched, _from_title = (
                            await enrich_collection(
                                client,
                                db_name,
                                coll_name,
                                extractor,
                                source_key,
                                dry_run=False,
                                limit=batch_size,
                                incremental=True,
                                only_empty=False,
                            )
                        )
                        cycle_scanned += scanned
                        cycle_updated += updated
                        cycle_with_tickers += with_tickers
                    except Exception:  # noqa: BLE001
                        log.exception(
                            "realtime_rule_tagger: error processing %s.%s",
                            source_key, coll_name,
                        )
                if cycle_scanned > 0:
                    log.info(
                        "realtime_rule_tagger cycle: scanned=%d updated=%d with_tickers=%d",
                        cycle_scanned, cycle_updated, cycle_with_tickers,
                    )
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("realtime_rule_tagger: outer loop error (continuing)")

            try:
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                raise
    except asyncio.CancelledError:
        log.info("realtime_rule_tagger: cancelled")
    finally:
        for c in clients_by_uri.values():
            try:
                c.close()
            except Exception:
                pass


__all__ = ["realtime_rule_tagger_loop"]
