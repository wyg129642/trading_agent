"""Real-time LLM ticker tagger — backend lifespan task.

Polls every N seconds for documents where the rule path landed empty
(`_canonical_tickers: []`) and have **not** been LLM-tagged yet, runs them
through a cheap chat model (default ``qwen-plus``), and writes the standard
LLM fields:

    _llm_canonical_tickers / _llm_canonical_tickers_at /
    _llm_unmatched_raw    / _llm_extract_source: "<source>_llm:<model>"

Design choices
--------------
- **Hot-path only** — query gates on ``_canonical_tickers_at >= now-LOOKBACK_HOURS``
  so we tag freshly crawled docs fast without auto-running the 130k empty
  historical backlog. Use ``scripts/llm_tag_tickers.py`` for explicit backfills.
- **Hybrid with rule path** — never overwrites ``_canonical_tickers``. The
  ticker resolution stack is rule first, LLM only as fallback (matches
  ``$or`` query pattern documented in TICKER_AGGREGATION.md §1).
- **Daily budget cap via Redis** — accrues estimated USD into
  ``llm_tagger:cost:YYYY-MM-DD`` (1.5d TTL). When cap is reached the loop
  sleeps in long ticks until UTC midnight rolls over.
- **Non-fatal** — every Mongo / LLM / network error is logged and swallowed;
  the loop never tears down the FastAPI app.

Reuses:
- Model catalog + prompt + LLM client builder from ``scripts/llm_tag_tickers.py``
  (imported via ``sys.path`` injection — that module has no import-time side
  effects beyond its own ``sys.path.insert``).
- ``normalize_with_unmatched`` from ``ticker_normalizer``.

Disabled by default. Enable via ``LLM_TAG_REALTIME_ENABLED=true`` in
``.env``. See config flags at the bottom of ``backend/app/config.py``.
"""
from __future__ import annotations

import asyncio
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import UpdateOne

from backend.app.config import Settings
from backend.app.services.ticker_normalizer import normalize_with_unmatched

# Import reusable bits from the batch CLI script. The script's only module-level
# side effect is its own sys.path insertion (~ benign).
_REPO_ROOT = Path(__file__).resolve().parents[3]
_SCRIPTS = _REPO_ROOT / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from llm_tag_tickers import (  # noqa: E402
    MODELS,
    SOURCES,
    _build_client,
    _build_user_prompt,
    _call_llm,
    _load_alias_index,
    _validate_by_mention,
)

log = logging.getLogger(__name__)

# Redis key holding accumulated USD cost for today (UTC). 1.5-day TTL.
_REDIS_KEY_TPL = "llm_tagger:cost:{date}"
_REDIS_KEY_TTL_SEC = 60 * 60 * 36

# Skip these even when enabled — they bulk-dominate any realtime budget.
# Use the manual script for these with explicit budget caps.
_SKIP_COLLS: frozenset[str] = frozenset({
    "jinmen.oversea_reports",        # 1.5M list / 466k PDFs; LLM realtime would burn budget
    "alphapai.wechat_articles",      # signal too low (already disabled in monitor)
})


def _today_key() -> str:
    return _REDIS_KEY_TPL.format(date=datetime.now(timezone.utc).strftime("%Y-%m-%d"))


async def _get_today_cost(redis_client) -> float:
    raw = await redis_client.get(_today_key())
    try:
        return float(raw) if raw else 0.0
    except (TypeError, ValueError):
        return 0.0


async def _add_cost(redis_client, delta_usd: float) -> None:
    if delta_usd <= 0:
        return
    pipe = redis_client.pipeline()
    pipe.incrbyfloat(_today_key(), delta_usd)
    pipe.expire(_today_key(), _REDIS_KEY_TTL_SEC)
    await pipe.execute()


async def _process_collection_batch(
    *,
    mongo_client: AsyncIOMotorClient,
    db_name: str,
    coll_name: str,
    source: str,
    coll_spec,
    llm_client,
    model_spec,
    alias_index: dict,
    redis_client,
    daily_budget_usd: float,
    batch_size: int,
    lookback_hours: int,
) -> tuple[int, int, float, int]:
    """Process up to ``batch_size`` empty-canonical docs from one collection.

    Returns (scanned, tagged, cost_added_usd, failures).
    """
    coll = mongo_client[db_name][coll_name]
    cutoff = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
    query = {
        "_canonical_tickers": [],
        "_llm_canonical_tickers": {"$exists": False},
        "_canonical_tickers_at": {"$gte": cutoff},
    }
    projection: dict[str, Any] = {f: 1 for f in (
        *coll_spec.title_fields,
        *coll_spec.body_fields,
        "list_item",
    )}

    docs: list[dict] = []
    async for d in coll.find(query, projection=projection).limit(batch_size):
        docs.append(d)
    if not docs:
        return 0, 0, 0.0, 0

    sem = asyncio.Semaphore(model_spec.speed_qps)
    pending: list[UpdateOne] = []
    tagged = 0
    failures = 0
    cost_added = 0.0
    stop = asyncio.Event()

    async def _process_one(doc: dict) -> None:
        nonlocal tagged, failures, cost_added
        if stop.is_set():
            return
        # Re-check budget before each LLM call (tight cap)
        cur = await _get_today_cost(redis_client)
        if cur + cost_added >= daily_budget_usd:
            stop.set()
            return
        async with sem:
            user_prompt = _build_user_prompt(doc, coll_spec)
            try:
                tickers_raw, in_tok, out_tok = await _call_llm(
                    llm_client, model_spec, user_prompt,
                )
            except Exception as exc:  # noqa: BLE001
                failures += 1
                log.warning(
                    "realtime_llm_tagger LLM call failed (%s.%s/%s): %s",
                    db_name, coll_name, doc.get("_id"), exc,
                )
                return
            cost = (
                in_tok / 1_000_000 * model_spec.in_usd_per_mtok
                + out_tok / 1_000_000 * model_spec.out_usd_per_mtok
            )
            cost_added += cost

            matched, unmatched = normalize_with_unmatched(tickers_raw)
            kept, dropped = _validate_by_mention(matched, user_prompt, alias_index)
            if dropped:
                unmatched = list(unmatched) + [f"mention_drop:{tk}" for tk in dropped]
            matched = kept
            if matched:
                tagged += 1
            pending.append(UpdateOne(
                {"_id": doc["_id"]},
                {"$set": {
                    "_llm_canonical_tickers": matched,
                    "_llm_canonical_tickers_at": datetime.now(timezone.utc),
                    "_llm_unmatched_raw": unmatched,
                    "_llm_extract_source": f"{source}_llm:{model_spec.key}",
                }},
            ))

    await asyncio.gather(*(_process_one(d) for d in docs), return_exceptions=False)

    if pending:
        try:
            await coll.bulk_write(pending, ordered=False)
        except Exception:  # noqa: BLE001
            log.exception("realtime_llm_tagger bulk_write failed (%s.%s)", db_name, coll_name)
    if cost_added > 0:
        await _add_cost(redis_client, cost_added)
    return len(docs), tagged, cost_added, failures


async def realtime_llm_tagger_loop(settings: Settings, redis_client) -> None:
    """Lifespan task. Polls every interval seconds; never raises out."""
    if not getattr(settings, "llm_tag_realtime_enabled", False):
        log.info("realtime_llm_tagger: disabled via config")
        return

    model_key = getattr(settings, "llm_tag_realtime_model", "qwen-plus")
    if model_key not in MODELS:
        log.error(
            "realtime_llm_tagger: unknown model %r (choices=%s); disabling",
            model_key, list(MODELS),
        )
        return
    model_spec = MODELS[model_key]
    daily_budget = float(getattr(settings, "llm_tag_realtime_daily_budget_usd", 5.0))
    interval = int(getattr(settings, "llm_tag_realtime_interval_sec", 60))
    lookback_h = int(getattr(settings, "llm_tag_realtime_lookback_hours", 2))
    batch_size = int(getattr(settings, "llm_tag_realtime_batch_size", 50))

    # Per-URI Mongo client pool (one each because each platform DB has its own URI)
    mongo_clients: dict[str, AsyncIOMotorClient] = {}

    def _mongo_for(uri: str) -> AsyncIOMotorClient:
        client = mongo_clients.get(uri)
        if client is None:
            client = AsyncIOMotorClient(uri, tz_aware=True)
            mongo_clients[uri] = client
        return client

    try:
        llm_client = _build_client(model_spec)
    except Exception:
        log.exception("realtime_llm_tagger: failed to build LLM client; disabling")
        return

    alias_index = _load_alias_index()

    log.info(
        "realtime_llm_tagger: started "
        "(model=%s, daily_budget=$%.2f, interval=%ds, lookback=%dh, batch=%d)",
        model_key, daily_budget, interval, lookback_h, batch_size,
    )

    try:
        while True:
            try:
                today_cost = await _get_today_cost(redis_client)
                if today_cost >= daily_budget:
                    log.info(
                        "realtime_llm_tagger: daily budget reached ($%.4f >= $%.2f); long sleep",
                        today_cost, daily_budget,
                    )
                    await asyncio.sleep(interval * 5)
                    continue

                cycle_scanned = 0
                cycle_tagged = 0
                cycle_cost = 0.0
                cycle_failures = 0

                for source, coll_specs in SOURCES.items():
                    for coll_name, coll_spec in coll_specs.items():
                        if f"{source}.{coll_name}" in _SKIP_COLLS:
                            continue
                        # Refresh today's cost between collections to honor the cap
                        running = today_cost + cycle_cost
                        if running >= daily_budget:
                            break
                        try:
                            uri = getattr(settings, coll_spec.uri_attr)
                            db_name = getattr(settings, coll_spec.db_attr)
                        except AttributeError:
                            log.warning(
                                "realtime_llm_tagger: missing settings %s/%s; skip %s.%s",
                                coll_spec.uri_attr, coll_spec.db_attr, source, coll_name,
                            )
                            continue
                        mongo_client = _mongo_for(uri)
                        try:
                            s, t, c, f = await _process_collection_batch(
                                mongo_client=mongo_client,
                                db_name=db_name,
                                coll_name=coll_name,
                                source=source,
                                coll_spec=coll_spec,
                                llm_client=llm_client,
                                model_spec=model_spec,
                                alias_index=alias_index,
                                redis_client=redis_client,
                                daily_budget_usd=daily_budget,
                                batch_size=batch_size,
                                lookback_hours=lookback_h,
                            )
                            cycle_scanned += s
                            cycle_tagged += t
                            cycle_cost += c
                            cycle_failures += f
                        except Exception:
                            log.exception(
                                "realtime_llm_tagger: error processing %s.%s",
                                source, coll_name,
                            )

                if cycle_scanned > 0:
                    log.info(
                        "realtime_llm_tagger cycle: scanned=%d tagged=%d failures=%d "
                        "cycle_cost=$%.4f today_cost=$%.4f",
                        cycle_scanned, cycle_tagged, cycle_failures,
                        cycle_cost, today_cost + cycle_cost,
                    )
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("realtime_llm_tagger: outer loop error (continuing)")

            try:
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                raise
    except asyncio.CancelledError:
        log.info("realtime_llm_tagger: cancelled")
    finally:
        try:
            await llm_client.close()
        except Exception:
            pass
        for c in mongo_clients.values():
            try:
                c.close()
            except Exception:
                pass


__all__ = ["realtime_llm_tagger_loop"]
