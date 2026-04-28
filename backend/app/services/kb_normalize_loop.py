"""Background lifespan task: backfill ``_normalized_title`` + ``_inst_normalized``.

The mirror fold in ``kb_service._collapse_mirrors`` keys cross-platform
duplicates by ``(_inst_normalized, _normalized_title, release_day)``. To make
that key cheap at search time we precompute the two normalized fields on every
crawler-corpus Mongo doc and stash them on the doc itself, sparingly indexed
so future ``find`` projections are O(1).

Pipeline (mirroring ``kb_vector_sync._ingest_loop``):

1. Loop forever; each cycle iterates every spec in ``SPECS_LIST``.
2. For each spec: ``find({"_normalized_title": {"$exists": False}})`` ordered
   by ``release_time_ms`` desc (so hot recent docs get backfilled first),
   limited to ``kb_normalize_batch_size`` per cycle per spec.
3. For each doc: compute the two strings via ``kb_service._normalize_title`` /
   ``_normalize_institution`` and bulk-write them back via ``$set``.
4. Sleep ``kb_normalize_interval_seconds`` and repeat.

Failures are best-effort and never crash the app — a doc that fails one
cycle gets retried next cycle. The whole loop is gated by
``kb_normalize_enabled`` and (like ``kb_vector_sync``) defaults to running
on the staging worktree only because crawlers live there.
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Any

from pymongo import UpdateOne

from backend.app.config import Settings
from backend.app.services.kb_service import (
    SPECS_LIST,
    CollectionSpec,
    _coll,
    _extract_institution,
    _normalize_institution,
    _normalize_title,
)

logger = logging.getLogger(__name__)


# Inter-spec pause to keep the event loop responsive during catch-up.
_INTER_SPEC_PAUSE_S: float = 0.2


def _should_start(settings: Settings) -> tuple[bool, str]:
    """Gate: same staging-only ownership as kb_vector_sync."""
    if not getattr(settings, "kb_normalize_enabled", True):
        return False, "kb_normalize_enabled=false"
    if settings.is_staging:
        return True, "APP_ENV=staging"
    if os.environ.get("KB_NORMALIZE_ALLOW_PROD") == "1":
        return True, "KB_NORMALIZE_ALLOW_PROD=1"
    return False, (
        "APP_ENV=production and KB_NORMALIZE_ALLOW_PROD is not 1 — "
        "normalize loop owned by staging worktree"
    )


class KbNormalizeService:
    """Owns the title/institution backfill loop for the crawler corpus."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self._running = False
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop(), name="kb_normalize_loop")
        logger.info(
            "kb_normalize_loop started (interval=%ds, batch=%d/spec)",
            self.settings.kb_normalize_interval_seconds,
            self.settings.kb_normalize_batch_size,
        )

    async def stop(self) -> None:
        self._running = False
        if self._task is None:
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception("kb_normalize_loop shutdown raised")
        self._task = None
        logger.info("kb_normalize_loop stopped")

    async def _loop(self) -> None:
        # Stagger after kb_vector_sync (which sleeps 60s) so we don't pound
        # the same Mongo connection pool at the same instant.
        try:
            await asyncio.sleep(90)
        except asyncio.CancelledError:
            return

        while self._running:
            started = time.monotonic()
            total_updated = 0
            for spec in SPECS_LIST:
                if not self._running:
                    break
                try:
                    n = await self._backfill_spec(spec)
                    total_updated += n
                except Exception as e:
                    logger.warning(
                        "kb_normalize: spec %s/%s raised: %s",
                        spec.db, spec.collection, e,
                    )
                if _INTER_SPEC_PAUSE_S:
                    try:
                        await asyncio.sleep(_INTER_SPEC_PAUSE_S)
                    except asyncio.CancelledError:
                        return
            elapsed = time.monotonic() - started
            if total_updated:
                logger.info(
                    "kb_normalize cycle done: updated=%d elapsed=%.1fs",
                    total_updated, elapsed,
                )
            sleep_for = max(
                10.0,
                float(self.settings.kb_normalize_interval_seconds) - elapsed,
            )
            try:
                await asyncio.sleep(sleep_for)
            except asyncio.CancelledError:
                return

    async def _backfill_spec(self, spec: CollectionSpec) -> int:
        """Backfill one batch of docs missing _normalized_title.

        Returns the number of docs updated.
        """
        coll = _coll(spec)
        batch_size = max(10, int(self.settings.kb_normalize_batch_size))
        sort_field = spec.date_ms_field or spec.date_str_field
        proj: dict[str, Any] = {
            "_id": 1,
            spec.title_field: 1,
        }
        if spec.institution_field:
            proj[spec.institution_field] = 1
        if spec.ticker_fallback_path == "stocks":
            proj["stocks"] = 1
        if spec.ticker_fallback_path == "companies":
            proj["companies"] = 1

        cursor = coll.find(
            {"_normalized_title": {"$exists": False}},
            proj,
        )
        if sort_field:
            cursor = cursor.sort([(sort_field, -1)])
        cursor = cursor.limit(batch_size)

        ops: list[UpdateOne] = []
        async for doc in cursor:
            title_raw = (doc.get(spec.title_field) or "").strip()
            inst_raw = _extract_institution(spec, doc) or ""
            ops.append(UpdateOne(
                {"_id": doc["_id"]},
                {"$set": {
                    "_normalized_title": _normalize_title(title_raw),
                    "_inst_normalized": _normalize_institution(inst_raw),
                }},
            ))

        if not ops:
            return 0
        try:
            res = await coll.bulk_write(ops, ordered=False)
            return int(getattr(res, "modified_count", 0) or len(ops))
        except Exception as e:
            logger.warning(
                "kb_normalize bulk_write failed for %s/%s: %s",
                spec.db, spec.collection, e,
            )
            return 0


# Module-level singleton — main.py creates it during lifespan startup.
_service: KbNormalizeService | None = None


async def start_normalize_loop(settings: Settings) -> None:
    """Lifespan hook: gate + start the singleton service."""
    global _service
    if _service is not None:
        return
    ok, reason = _should_start(settings)
    if not ok:
        logger.info("kb_normalize_loop NOT started: %s", reason)
        return
    _service = KbNormalizeService(settings)
    await _service.start()


async def stop_normalize_loop() -> None:
    """Lifespan hook: gracefully shut down."""
    global _service
    if _service is None:
        return
    await _service.stop()
    _service = None
