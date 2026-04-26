"""Async batched writer for chat_audit_run + chat_audit_event tables.

Design: every ``ChatTrace.log_*`` call (in chat_debug.py) enqueues a record
on a process-wide ``asyncio.Queue``; one background task drains the queue
in batches and writes to Postgres. The chat hot path never awaits a DB
INSERT. The rotating ``logs/chat_debug.log`` file remains a parallel sink
so a writer outage never loses observability.

Public API:
    await start_writer()                                  # called once in lifespan
    submit_run_start(...)                                 # enqueue run INSERT
    submit_event(run_id, trace_id, event_type, ...)       # enqueue event INSERT
    submit_run_finalize(run_id, ...)                      # enqueue run UPDATE
    await stop_writer()                                   # drain + cancel

Three operation types share one queue so a finalize never overtakes its own
events. The consumer batches each type separately for INSERT efficiency.

Failure policy: any DB error inside the consumer is logged and the batch is
dropped. Chat traffic is never blocked by audit-log troubles.
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import insert, update
from sqlalchemy.ext.asyncio import AsyncSession

from backend.app.core.database import async_session_factory
from backend.app.models.chat_audit import ChatAuditEvent, ChatAuditRun

logger = logging.getLogger(__name__)

# ── Tunables ────────────────────────────────────────────────────────

#: Max payload size per event (bytes after JSON serialisation).
PAYLOAD_MAX_BYTES = 32 * 1024

#: Drain at most every N seconds even if the batch isn't full.
FLUSH_INTERVAL_S = 0.25

#: Or sooner once we hit this many events.
FLUSH_BATCH_SIZE = 200

#: Bound the in-memory queue so a runaway producer can't blow up RSS.
QUEUE_MAX = 10_000

#: Keys whose values are masked before being persisted. Lower-case.
SENSITIVE_KEYS = {
    "authorization", "cookie", "set-cookie", "x-api-key",
    "api_key", "apikey", "api-key",
    "password", "passwd", "pwd",
    "access_token", "refresh_token", "id_token",
    "secret", "client_secret", "token", "auth_token",
}

#: Token-shaped strings inside free-form text get stripped.
_BEARER_RE = re.compile(r"Bearer\s+[A-Za-z0-9._~+/=-]{8,}", re.I)
_SK_KEY_RE = re.compile(r"sk-[A-Za-z0-9]{20,}")


# ── Queue records ───────────────────────────────────────────────────

@dataclass
class _RunStart:
    payload: dict[str, Any]


@dataclass
class _Event:
    payload: dict[str, Any]


@dataclass
class _RunFinalize:
    run_id: uuid.UUID
    values: dict[str, Any]


@dataclass
class _Shutdown:
    """Sentinel pushed by stop_writer() to drain and exit."""
    done: asyncio.Event = field(default_factory=asyncio.Event)


# ── Module-level singleton state ────────────────────────────────────

_queue: asyncio.Queue | None = None
_task: asyncio.Task | None = None


def _get_queue() -> asyncio.Queue | None:
    return _queue


def is_running() -> bool:
    return _task is not None and not _task.done()


# ── Redaction ───────────────────────────────────────────────────────

def _redact_str(s: str) -> str:
    s = _BEARER_RE.sub("Bearer <redacted>", s)
    s = _SK_KEY_RE.sub("<redacted-key>", s)
    return s


def _redact_obj(obj: Any) -> Any:
    if isinstance(obj, dict):
        out: dict[str, Any] = {}
        for k, v in obj.items():
            kl = str(k).lower()
            if kl in SENSITIVE_KEYS:
                out[k] = "<redacted>"
            else:
                out[k] = _redact_obj(v)
        return out
    if isinstance(obj, (list, tuple)):
        return [_redact_obj(x) for x in obj]
    if isinstance(obj, str):
        return _redact_str(obj)
    return obj


def _clamp_payload(payload: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    """Clamp serialised JSON to PAYLOAD_MAX_BYTES. Returns (payload, truncated)."""
    redacted = _redact_obj(payload)
    try:
        encoded = json.dumps(redacted, ensure_ascii=False, default=str)
    except (TypeError, ValueError):
        encoded = json.dumps({"_serialize_error": True}, ensure_ascii=False)
    if len(encoded.encode("utf-8")) <= PAYLOAD_MAX_BYTES:
        return redacted, False
    half = PAYLOAD_MAX_BYTES // 2
    truncated = encoded[:half] + " ... [truncated] ... " + encoded[-half:]
    return {
        "_truncated_preview": truncated,
        "_original_size_bytes": len(encoded.encode("utf-8")),
    }, True


# ── Public submitters ───────────────────────────────────────────────

def submit_run_start(
    *,
    run_id: uuid.UUID,
    trace_id: str,
    user_id: uuid.UUID | None,
    username: str,
    conversation_id: uuid.UUID | None,
    message_id: uuid.UUID | None,
    user_content: str,
    models_requested: list[str],
    mode: str,
    web_search_mode: str,
    feature_flags: dict[str, Any],
    system_prompt_len: int,
    history_messages: int,
    tools_offered: list[str],
    client_ip: str | None = None,
    user_agent: str | None = None,
) -> None:
    """Insert the run header. Best-effort; drops if writer isn't running."""
    if _queue is None:
        return
    rec = _RunStart(payload=dict(
        id=run_id,
        trace_id=trace_id,
        user_id=user_id,
        username=username,
        conversation_id=conversation_id,
        message_id=message_id,
        user_content=user_content[:65_536],  # reasonable upper bound
        models_requested=models_requested,
        mode=mode,
        web_search_mode=web_search_mode,
        feature_flags=feature_flags,
        system_prompt_len=system_prompt_len,
        history_messages=history_messages,
        tools_offered=tools_offered,
        status="running",
        started_at=datetime.now(timezone.utc),
        client_ip=client_ip,
        user_agent=user_agent,
    ))
    try:
        _queue.put_nowait(rec)
    except asyncio.QueueFull:
        logger.warning("chat_audit_writer queue full; dropping run_start trace=%s", trace_id)


def submit_event(
    *,
    run_id: uuid.UUID,
    trace_id: str,
    sequence: int,
    event_type: str,
    payload: dict[str, Any] | None = None,
    model_id: str | None = None,
    round_num: int | None = None,
    tool_name: str | None = None,
    latency_ms: int | None = None,
) -> None:
    if _queue is None:
        return
    pl, truncated = _clamp_payload(payload or {})
    rec = _Event(payload=dict(
        id=uuid.uuid4(),
        run_id=run_id,
        trace_id=trace_id,
        sequence=sequence,
        event_type=event_type,
        model_id=model_id,
        round_num=round_num,
        tool_name=tool_name,
        latency_ms=latency_ms,
        payload=pl,
        payload_truncated=truncated,
        created_at=datetime.now(timezone.utc),
    ))
    try:
        _queue.put_nowait(rec)
    except asyncio.QueueFull:
        logger.warning(
            "chat_audit_writer queue full; dropping event=%s trace=%s",
            event_type, trace_id,
        )


def submit_run_finalize(
    *,
    run_id: uuid.UUID,
    status: str,
    error_message: str | None,
    rounds_used: int,
    tool_calls_total: int,
    tool_calls_by_name: dict[str, int],
    urls_searched: int,
    urls_read: int,
    citations_count: int,
    total_tokens: int,
    total_latency_ms: int,
    final_content_len: int,
) -> None:
    if _queue is None:
        return
    rec = _RunFinalize(
        run_id=run_id,
        values=dict(
            status=status,
            error_message=error_message,
            rounds_used=rounds_used,
            tool_calls_total=tool_calls_total,
            tool_calls_by_name=tool_calls_by_name,
            urls_searched=urls_searched,
            urls_read=urls_read,
            citations_count=citations_count,
            total_tokens=total_tokens,
            total_latency_ms=total_latency_ms,
            final_content_len=final_content_len,
            finished_at=datetime.now(timezone.utc),
        ),
    )
    try:
        _queue.put_nowait(rec)
    except asyncio.QueueFull:
        logger.warning("chat_audit_writer queue full; dropping run_finalize id=%s", run_id)


# ── Background consumer ─────────────────────────────────────────────

async def _flush_batch(
    session: AsyncSession,
    run_starts: list[_RunStart],
    events: list[_Event],
    finalizes: list[_RunFinalize],
) -> None:
    if run_starts:
        await session.execute(
            insert(ChatAuditRun),
            [r.payload for r in run_starts],
        )
    if events:
        await session.execute(
            insert(ChatAuditEvent),
            [e.payload for e in events],
        )
    for fin in finalizes:
        await session.execute(
            update(ChatAuditRun)
            .where(ChatAuditRun.id == fin.run_id)
            .values(**fin.values)
        )
    await session.commit()


async def _consumer_loop() -> None:
    assert _queue is not None
    pending_starts: list[_RunStart] = []
    pending_events: list[_Event] = []
    pending_finalizes: list[_RunFinalize] = []
    shutdown_signal: _Shutdown | None = None

    async def drain_once() -> None:
        nonlocal pending_starts, pending_events, pending_finalizes
        if not (pending_starts or pending_events or pending_finalizes):
            return
        try:
            async with async_session_factory() as session:
                await _flush_batch(
                    session, pending_starts, pending_events, pending_finalizes,
                )
        except Exception:
            logger.exception(
                "chat_audit_writer flush failed; dropping batch "
                "(starts=%d events=%d finalizes=%d)",
                len(pending_starts), len(pending_events), len(pending_finalizes),
            )
        finally:
            pending_starts = []
            pending_events = []
            pending_finalizes = []

    while True:
        try:
            timeout = FLUSH_INTERVAL_S
            try:
                item = await asyncio.wait_for(_queue.get(), timeout=timeout)
            except asyncio.TimeoutError:
                await drain_once()
                continue

            if isinstance(item, _RunStart):
                pending_starts.append(item)
            elif isinstance(item, _Event):
                pending_events.append(item)
            elif isinstance(item, _RunFinalize):
                pending_finalizes.append(item)
            elif isinstance(item, _Shutdown):
                shutdown_signal = item
                break

            total = len(pending_starts) + len(pending_events) + len(pending_finalizes)
            if total >= FLUSH_BATCH_SIZE:
                await drain_once()
        except asyncio.CancelledError:
            break
        except Exception:
            logger.exception("chat_audit_writer consumer iteration crashed")
            await asyncio.sleep(1.0)

    # Final drain on shutdown
    while not _queue.empty():
        try:
            item = _queue.get_nowait()
        except asyncio.QueueEmpty:
            break
        if isinstance(item, _RunStart):
            pending_starts.append(item)
        elif isinstance(item, _Event):
            pending_events.append(item)
        elif isinstance(item, _RunFinalize):
            pending_finalizes.append(item)
    await drain_once()
    if shutdown_signal is not None:
        shutdown_signal.done.set()


# ── Lifespan hooks ──────────────────────────────────────────────────

async def start_writer() -> None:
    """Initialise the queue + consumer task. Idempotent."""
    global _queue, _task
    if _task is not None and not _task.done():
        return
    _queue = asyncio.Queue(maxsize=QUEUE_MAX)
    _task = asyncio.create_task(_consumer_loop(), name="chat_audit_writer")
    logger.info("chat_audit_writer started (queue_max=%d, batch=%d, interval=%.2fs)",
                QUEUE_MAX, FLUSH_BATCH_SIZE, FLUSH_INTERVAL_S)


async def stop_writer(timeout_s: float = 5.0) -> None:
    """Drain the queue and stop the consumer."""
    global _queue, _task
    if _task is None:
        return
    if _queue is not None:
        sentinel = _Shutdown()
        try:
            _queue.put_nowait(sentinel)
            try:
                await asyncio.wait_for(sentinel.done.wait(), timeout=timeout_s)
            except asyncio.TimeoutError:
                logger.warning("chat_audit_writer drain timed out after %.1fs", timeout_s)
        except Exception:
            logger.exception("chat_audit_writer stop failed")
    if not _task.done():
        _task.cancel()
        try:
            await _task
        except (asyncio.CancelledError, Exception):
            pass
    _task = None
    _queue = None
    logger.info("chat_audit_writer stopped")
