"""Background service: incremental MongoDB → Milvus sync for the shared KB corpus.

The ingest + sweep functions in ``kb_vector_ingest`` are correct but one-shot.
This module schedules them so the vector index stays in lock-step with the
crawler corpus automatically, without an operator running the CLI.

Two loops, both best-effort — a failure in one spec never aborts the cycle,
and a failure in the whole cycle never takes down the chat path (vector
search already fails open via the circuit breaker in ``tei_client`` and
Milvus client wrappers):

* **ingest loop** (``kb_vector_sync_interval_seconds``, default 300 s) —
  iterates every spec in ``SPECS_LIST``, reads the last persisted watermark
  from the local sync-state Mongo, and calls ``ingest_collection`` with
  ``since_ms=watermark`` and a per-cycle per-spec ``limit``. Adds and
  updates propagate immediately; per-doc stale-chunk cleanup is handled
  inline by ``ingest_collection`` (delete of old chunk_ids that are no
  longer produced by the current parser).

* **sweep loop** — fires daily in a small local-time window (defaults to
  03:05-03:10) and calls ``sweep_deleted_docs`` per spec to reconcile
  Milvus to Mongo **deletes** (docs dropped by the crawlers upstream, or
  removed by operator surgery). This is the only path that covers the
  delete side, so without this loop a crawler-side tombstone would leave
  stale hits in the index indefinitely.

User-uploaded knowledge base documents are handled separately — they are
chunked + embedded inline by ``user_kb_service.parse_document`` on upload,
so they do not need this loop.

Topology guardrails
-------------------
Since crawlers live in STAGING after 2026-04-24, this loop also defaults to
STAGING-only. Prod can still opt in by setting ``KB_VECTOR_SYNC_ALLOW_PROD=1``
in its environment. Lease-based locking inside ``ingest_collection`` and
``sweep_deleted_docs`` makes double-ingest safe even if both worktrees
enabled the loop simultaneously — each spec has its own 2 h lease in the
shared local-Mongo state store (see ``kb_vector_ingest.acquire_lease``).
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from datetime import datetime

from backend.app.config import Settings
from backend.app.services.kb_service import SPECS_LIST
from backend.app.services.kb_vector_ingest import (
    get_last_pdf_watermark,
    get_last_watermark,
    get_last_watermark_str,
    ingest_collection,
    sweep_deleted_docs,
)

logger = logging.getLogger(__name__)


# Small pause between specs inside a single cycle so one spec doesn't
# monopolise TEI and Milvus upserts back-to-back. Keeps the event loop
# breathing for other requests during a long catch-up.
_INTER_SPEC_PAUSE_S: float = 0.5


def _should_start(settings: Settings) -> tuple[bool, str]:
    """Gate: decide whether this worktree should own the sync loop.

    Returns ``(start?, reason)`` where ``reason`` is a short human-readable
    string for the startup log.
    """
    if not settings.vector_sync_enabled:
        return False, "VECTOR_SYNC_ENABLED=false"

    # Crawlers live in staging (per CLAUDE.md + 2026-04-24 memory). Let
    # prod opt in explicitly so an operator can flip ownership later
    # without a code change.
    if settings.is_staging:
        return True, "APP_ENV=staging"
    if os.environ.get("KB_VECTOR_SYNC_ALLOW_PROD") == "1":
        return True, "KB_VECTOR_SYNC_ALLOW_PROD=1"
    return False, (
        "APP_ENV=production and KB_VECTOR_SYNC_ALLOW_PROD is not 1 — "
        "vector sync is owned by the staging worktree"
    )


class KbVectorSyncService:
    """Owns the MongoDB → Milvus sync loops for the crawler corpus."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self._running = False
        self._ingest_task: asyncio.Task | None = None
        self._sweep_task: asyncio.Task | None = None

    # ── Lifecycle ────────────────────────────────────────────

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._ingest_task = asyncio.create_task(
            self._ingest_loop(), name="kb_vector_sync_ingest"
        )
        self._sweep_task = asyncio.create_task(
            self._sweep_loop(), name="kb_vector_sync_sweep"
        )
        logger.info(
            "kb_vector_sync started (interval=%ds, per-spec limit=%d docs, "
            "batch=%d, sweep window=%02d:%02d-%02d:%02d local)",
            self.settings.kb_vector_sync_interval_seconds,
            self.settings.kb_vector_sync_per_spec_limit,
            self.settings.kb_vector_sync_embed_batch_size,
            self.settings.kb_vector_sync_sweep_hour,
            self.settings.kb_vector_sync_sweep_minute_start,
            self.settings.kb_vector_sync_sweep_hour,
            self.settings.kb_vector_sync_sweep_minute_end,
        )

    async def stop(self) -> None:
        self._running = False
        for t in (self._ingest_task, self._sweep_task):
            if t is None:
                continue
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.exception("kb_vector_sync task shutdown raised")
        self._ingest_task = None
        self._sweep_task = None
        logger.info("kb_vector_sync stopped")

    # ── Ingest loop ──────────────────────────────────────────

    async def _ingest_loop(self) -> None:
        # Let the app finish startup before we start pounding TEI.
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            return

        while self._running:
            started = time.monotonic()
            await self._run_one_cycle()
            elapsed = time.monotonic() - started

            # Short floor so back-to-back cycles still yield briefly even
            # when catching up a big backlog (cycle can exceed the nominal
            # interval — that's fine, we just sleep the floor).
            sleep_for = max(10.0, float(
                self.settings.kb_vector_sync_interval_seconds
            ) - elapsed)
            try:
                await asyncio.sleep(sleep_for)
            except asyncio.CancelledError:
                return

    async def _run_one_cycle(self) -> dict[str, int]:
        """Single pass over every spec. Exposed for tests + observability."""
        totals = {
            "specs_run": 0,
            "specs_skipped": 0,
            "chunks_upserted": 0,
            "chunks_deleted": 0,
            "docs_unchanged": 0,
            "docs_errored": 0,
        }
        for spec in SPECS_LIST:
            if not self._running:
                return totals
            key = f"{spec.db}/{spec.collection}"
            try:
                # Pick the right watermark for this spec's date shape. Five
                # specs (alphapai/*, jinmen/meetings) have no epoch-ms field —
                # they use a lexicographically sortable `publish_time` /
                # `release_time` string instead. Without this the `limit`
                # cursor keeps returning the oldest N docs forever and newer
                # ones never get ingested.
                if spec.date_ms_field:
                    since_ms: int | None = get_last_watermark(spec)
                    since_str: str | None = None
                else:
                    since_ms = None
                    since_str = get_last_watermark_str(spec)
                # PDF watermark: only meaningful for has_pdf specs. Lets the
                # cursor OR-include docs whose pdf_text_extracted_at is fresh
                # even if their release_time_ms is months old. First cycle
                # after deploy seeds at 0 (catch every existing PDF back-fill);
                # subsequent cycles use the persisted max from the prior pass.
                since_pdf_ms = get_last_pdf_watermark(spec)
                if spec.has_pdf and since_pdf_ms is None:
                    since_pdf_ms = 0
                stats = await ingest_collection(
                    spec,
                    since_ms=since_ms,
                    since_str=since_str,
                    since_pdf_ms=since_pdf_ms,
                    limit=self.settings.kb_vector_sync_per_spec_limit,
                    batch_size=self.settings.kb_vector_sync_embed_batch_size,
                    use_lease=True,
                )
                if stats.get("skipped_due_to_lease"):
                    totals["specs_skipped"] += 1
                    continue
                totals["specs_run"] += 1
                totals["chunks_upserted"] += int(stats.get("chunks_upserted") or 0)
                totals["chunks_deleted"] += int(stats.get("chunks_deleted") or 0)
                totals["docs_unchanged"] += int(stats.get("docs_unchanged") or 0)
                totals["docs_errored"] += int(stats.get("docs_errored") or 0)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "kb_vector_sync: ingest of %s failed (cycle continues)", key,
                )
            # Yield between specs so other tasks (chat requests, quote warmer)
            # get CPU during a long catch-up.
            try:
                await asyncio.sleep(_INTER_SPEC_PAUSE_S)
            except asyncio.CancelledError:
                raise
        logger.info(
            "kb_vector_sync ingest cycle done: specs_run=%d skipped=%d "
            "upserted=%d deleted=%d unchanged=%d errored=%d",
            totals["specs_run"], totals["specs_skipped"],
            totals["chunks_upserted"], totals["chunks_deleted"],
            totals["docs_unchanged"], totals["docs_errored"],
        )
        return totals

    # ── Sweep loop ───────────────────────────────────────────

    async def _sweep_loop(self) -> None:
        # Let the ingest loop settle before running a full scan.
        try:
            await asyncio.sleep(120)
        except asyncio.CancelledError:
            return

        last_run_date: str = ""
        while self._running:
            try:
                now = datetime.now()
                in_window = (
                    now.hour == self.settings.kb_vector_sync_sweep_hour
                    and self.settings.kb_vector_sync_sweep_minute_start
                    <= now.minute
                    < self.settings.kb_vector_sync_sweep_minute_end
                )
                today = now.strftime("%Y-%m-%d")
                if in_window and today != last_run_date:
                    last_run_date = today
                    await self._run_sweep_cycle()
                    # Skip past the window so we don't re-enter on the next tick.
                    try:
                        await asyncio.sleep(300)
                    except asyncio.CancelledError:
                        return
                else:
                    try:
                        await asyncio.sleep(60)
                    except asyncio.CancelledError:
                        return
            except asyncio.CancelledError:
                return
            except Exception:
                logger.exception("kb_vector_sync sweep loop iteration failed")
                try:
                    await asyncio.sleep(60)
                except asyncio.CancelledError:
                    return

    async def _run_sweep_cycle(self) -> dict[str, int]:
        """Single pass of the daily delete sweep."""
        totals = {"specs_run": 0, "tombstones": 0, "deleted": 0}
        for spec in SPECS_LIST:
            if not self._running:
                return totals
            key = f"{spec.db}/{spec.collection}"
            try:
                stats = await sweep_deleted_docs(spec, use_lease=True)
                if stats.get("skipped_due_to_lease"):
                    continue
                totals["specs_run"] += 1
                totals["tombstones"] += int(stats.get("tombstones") or 0)
                totals["deleted"] += int(stats.get("deleted") or 0)
                if stats.get("tombstones"):
                    logger.info(
                        "kb_vector_sync sweep %s: mongo_docs=%d milvus_docs=%d "
                        "tombstones=%d deleted=%d",
                        key,
                        stats.get("mongo_docs", -1),
                        stats.get("milvus_docs", -1),
                        stats.get("tombstones", 0),
                        stats.get("deleted", 0),
                    )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "kb_vector_sync: sweep of %s failed (cycle continues)", key,
                )
            try:
                await asyncio.sleep(_INTER_SPEC_PAUSE_S)
            except asyncio.CancelledError:
                raise
        logger.info(
            "kb_vector_sync sweep cycle done: specs_run=%d tombstones=%d deleted=%d",
            totals["specs_run"], totals["tombstones"], totals["deleted"],
        )
        return totals
