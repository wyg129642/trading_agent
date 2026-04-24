#!/usr/bin/env python3
"""Standalone daemon: distill chat feedback into long-term user memories.

Runs a simple poll loop:
  every POLL_INTERVAL_SECONDS (default 20s):
    1. Pick the oldest N unprocessed ChatFeedbackEvent rows.
    2. For each, load the response + the user message that produced it.
    3. Ask the enrichment LLM (Qwen-Plus via llm_enrichment_*) to extract
       typed, dedupable memories.
    4. Upsert memories into user_chat_memories.
    5. Mark the event processed (with any error message if extraction failed).

The process is designed to be long-running and is managed by start_web.sh
(see the `memory` subcommand). It handles SIGTERM/SIGINT cleanly, logs
everything to logs/memory_processor.log, and never raises out of the main
loop — a single bad event should never kill the daemon.

Usage:
    python run_chat_memory_processor.py               # full loop
    python run_chat_memory_processor.py --once        # one pass, then exit
    python run_chat_memory_processor.py --test-event ID
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import sys
import uuid
from datetime import datetime
from pathlib import Path

# Ensure project root is on path so `backend.app...` imports work when this
# script is invoked from anywhere (start_web.sh cd's into PROJECT_DIR but
# cron / systemd might not).
ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))


POLL_INTERVAL_SECONDS = int(os.getenv("CHAT_MEMORY_POLL_INTERVAL", "20"))
BATCH_SIZE = int(os.getenv("CHAT_MEMORY_BATCH_SIZE", "10"))
# Min gap between batches even when plenty of work is pending — avoids
# thrashing the LLM and DB when a bulk of feedback arrives at once.
MIN_CYCLE_SECONDS = int(os.getenv("CHAT_MEMORY_MIN_CYCLE", "5"))


def setup_logging() -> logging.Logger:
    log_dir = ROOT / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "memory_processor.log"

    root = logging.getLogger()
    root.setLevel(logging.INFO)
    # Avoid adding duplicate handlers on re-import in tests
    if not any(isinstance(h, logging.FileHandler) and Path(h.baseFilename) == log_file for h in root.handlers):
        fmt = "%(asctime)s | %(levelname)-7s | %(name)-30s | %(message)s"
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setFormatter(logging.Formatter(fmt))
        root.addHandler(fh)
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(logging.Formatter(fmt))
        root.addHandler(sh)

    # Quiet noisy dependencies
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("openai").setLevel(logging.WARNING)
    logging.getLogger("sqlalchemy").setLevel(logging.WARNING)

    return logging.getLogger("chat_memory_processor")


logger = logging.getLogger("chat_memory_processor")


async def _process_one_event(
    event_id: uuid.UUID,
    extractor,
) -> tuple[int, str, str | None]:
    """Process a single ChatFeedbackEvent by id.

    Returns (memories_produced, sentiment, error_or_None). Opens its own DB
    session so the daemon can recover from any single-event failure.
    """
    from sqlalchemy import select
    from backend.app.core.database import async_session_factory
    from backend.app.models.chat import ChatMessage, ChatModelResponse
    from backend.app.models.chat_memory import ChatFeedbackEvent
    from backend.app.services.chat_memory_extractor import ExtractionInput
    from backend.app.services.chat_memory_service import (
        upsert_memories, mark_feedback_processed,
    )

    async with async_session_factory() as db:
        event = await db.scalar(
            select(ChatFeedbackEvent).where(ChatFeedbackEvent.id == event_id)
        )
        if event is None:
            return 0, "neutral", "event_not_found"
        if event.processed:
            # Concurrency guard — another worker already claimed it
            return 0, event.sentiment, None

        resp = await db.scalar(
            select(ChatModelResponse).where(ChatModelResponse.id == event.response_id)
        )
        if resp is None:
            await mark_feedback_processed(db, event.id, [], event.sentiment, "response_missing")
            await db.commit()
            return 0, event.sentiment, "response_missing"

        msg = await db.scalar(select(ChatMessage).where(ChatMessage.id == resp.message_id))
        user_content = (msg.content if msg else "") or ""

        inp = ExtractionInput(
            user_message=user_content,
            assistant_response=resp.content or "",
            model_name=resp.model_name or resp.model_id or "",
            rating=event.rating,
            feedback_tags=list(event.feedback_tags or []),
            feedback_text=event.feedback_text or "",
        )
        result = await extractor.extract(inp)

        evidence_entry = {
            "type": "feedback",
            "event_id": str(event.id),
            "response_id": str(resp.id),
            "model_name": resp.model_name,
            "rating": event.rating,
            "tags": list(event.feedback_tags or []),
            "excerpt": (event.feedback_text or "")[:280],
            "when": event.created_at.isoformat() if event.created_at else None,
        }

        upserted = await upsert_memories(
            db, event.user_id, result.memories,
            evidence=[evidence_entry],
            source_type="feedback_derived",
        )

        await mark_feedback_processed(
            db, event.id, upserted, result.sentiment, result.error,
        )
        await db.commit()
        return len(upserted), result.sentiment, result.error


async def process_batch(extractor) -> int:
    """Process up to BATCH_SIZE unprocessed events. Returns events handled."""
    from sqlalchemy import select
    from backend.app.core.database import async_session_factory
    from backend.app.models.chat_memory import ChatFeedbackEvent

    async with async_session_factory() as db:
        rows = (await db.execute(
            select(ChatFeedbackEvent.id)
            .where(ChatFeedbackEvent.processed == False)  # noqa: E712
            .order_by(ChatFeedbackEvent.created_at.asc())
            .limit(BATCH_SIZE)
        )).scalars().all()

    if not rows:
        return 0

    handled = 0
    for event_id in rows:
        try:
            count, sentiment, err = await _process_one_event(event_id, extractor)
            handled += 1
            if err:
                logger.warning(
                    "event=%s processed (with error): memories=%d sentiment=%s err=%s",
                    event_id, count, sentiment, err,
                )
            else:
                logger.info(
                    "event=%s processed: memories=%d sentiment=%s",
                    event_id, count, sentiment,
                )
        except Exception:
            logger.exception("event=%s unhandled error (will retry next cycle)", event_id)
            # Do NOT mark processed — the next cycle will pick it up again.
    return handled


async def run_loop(extractor, stop_event: asyncio.Event) -> None:
    cycle = 0
    while not stop_event.is_set():
        cycle += 1
        started = asyncio.get_event_loop().time()
        try:
            handled = await process_batch(extractor)
        except Exception:
            logger.exception("process_batch failed — sleeping %ds", POLL_INTERVAL_SECONDS)
            handled = 0

        elapsed = asyncio.get_event_loop().time() - started
        if handled == 0:
            sleep_s = POLL_INTERVAL_SECONDS
        else:
            sleep_s = max(MIN_CYCLE_SECONDS, POLL_INTERVAL_SECONDS - int(elapsed))

        logger.debug("cycle=%d handled=%d elapsed=%.2fs sleep=%ds", cycle, handled, elapsed, sleep_s)
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=sleep_s)
        except asyncio.TimeoutError:
            pass


async def main_async(once: bool, test_event: str | None) -> int:
    setup_logging()
    logger.info("=" * 60)
    logger.info("Chat Memory Processor — starting (once=%s, test_event=%s)", once, test_event)
    logger.info("poll_interval=%ds batch=%d", POLL_INTERVAL_SECONDS, BATCH_SIZE)
    logger.info("=" * 60)

    from backend.app.config import get_settings
    from backend.app.services.chat_memory_extractor import ChatMemoryExtractor

    settings = get_settings()
    extractor = ChatMemoryExtractor(settings)
    if not extractor.is_configured:
        logger.error(
            "llm_enrichment_api_key not configured — memory extractor would "
            "produce no output. Refusing to spin forever; exiting."
        )
        return 2

    if test_event:
        try:
            event_uuid = uuid.UUID(test_event)
        except ValueError:
            logger.error("--test-event must be a UUID, got %r", test_event)
            return 2
        count, sentiment, err = await _process_one_event(event_uuid, extractor)
        logger.info("test result: memories=%d sentiment=%s error=%s", count, sentiment, err)
        await extractor.aclose()
        return 0

    stop_event = asyncio.Event()
    loop = asyncio.get_event_loop()

    def _shutdown():
        logger.info("shutdown signal received — finishing current batch and exiting")
        stop_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _shutdown)
        except NotImplementedError:
            # Windows: skip
            pass

    try:
        if once:
            await process_batch(extractor)
        else:
            await run_loop(extractor, stop_event)
    finally:
        await extractor.aclose()
        logger.info("Chat Memory Processor stopped at %s", datetime.now().isoformat())
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", action="store_true", help="Run one pass and exit")
    parser.add_argument(
        "--test-event", metavar="UUID",
        help="Process one specific feedback event and exit (for debugging)",
    )
    args = parser.parse_args()

    try:
        return asyncio.run(main_async(args.once, args.test_event))
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    sys.exit(main())
