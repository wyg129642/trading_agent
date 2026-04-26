"""Admin / user-facing AI chat audit log API.

Three views surfaced to the frontend:

- ``GET /runs``                — paginated list with filters
- ``GET /runs/{run_id}``       — header + linked user message + model replies
- ``GET /runs/{run_id}/events`` — ordered timeline of all events
- ``GET /runs/{run_id}/export`` — full JSON dump (download)
- ``GET /stats``               — summary cards (req/day, top tools, error rate)

Role semantics (boss is excluded entirely via ``get_current_non_boss``):

- ``user``: scoped to their own runs (``WHERE user_id = current_user.id``).
- ``admin``: full visibility, may filter by username / user_id.
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from sqlalchemy import and_, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.app.deps import get_current_non_boss, get_db
from backend.app.models.chat import ChatMessage, ChatModelResponse
from backend.app.models.chat_audit import ChatAuditEvent, ChatAuditRun
from backend.app.models.user import User

router = APIRouter()


# ── Helpers ─────────────────────────────────────────────────────────

def _is_admin(user: User) -> bool:
    return getattr(user, "role", "") == "admin"


def _scope_to_user(stmt, user: User):
    """Apply per-user scope when caller isn't admin."""
    if _is_admin(user):
        return stmt
    return stmt.where(ChatAuditRun.user_id == user.id)


def _serialise_run_summary(r: ChatAuditRun) -> dict[str, Any]:
    return {
        "id": str(r.id),
        "trace_id": r.trace_id,
        "user_id": str(r.user_id) if r.user_id else None,
        "username": r.username,
        "conversation_id": str(r.conversation_id) if r.conversation_id else None,
        "message_id": str(r.message_id) if r.message_id else None,
        "user_content_preview": (r.user_content or "")[:240],
        "models_requested": r.models_requested or [],
        "mode": r.mode,
        "web_search_mode": r.web_search_mode,
        "feature_flags": r.feature_flags or {},
        "status": r.status,
        "error_message": r.error_message,
        "rounds_used": r.rounds_used,
        "tool_calls_total": r.tool_calls_total,
        "tool_calls_by_name": r.tool_calls_by_name or {},
        "urls_searched": r.urls_searched,
        "urls_read": r.urls_read,
        "citations_count": r.citations_count,
        "total_tokens": r.total_tokens,
        "total_cost_usd": float(r.total_cost_usd) if r.total_cost_usd is not None else None,
        "total_latency_ms": r.total_latency_ms,
        "final_content_len": r.final_content_len,
        "started_at": r.started_at.isoformat() if r.started_at else None,
        "finished_at": r.finished_at.isoformat() if r.finished_at else None,
    }


def _serialise_event(e: ChatAuditEvent) -> dict[str, Any]:
    return {
        "id": str(e.id),
        "trace_id": e.trace_id,
        "sequence": e.sequence,
        "event_type": e.event_type,
        "model_id": e.model_id,
        "round_num": e.round_num,
        "tool_name": e.tool_name,
        "latency_ms": e.latency_ms,
        "payload": e.payload,
        "payload_truncated": e.payload_truncated,
        "created_at": e.created_at.isoformat() if e.created_at else None,
    }


# ── Endpoints ───────────────────────────────────────────────────────

@router.get("/runs")
async def list_runs(
    user_id: str | None = Query(None, description="Filter by user UUID (admin only)"),
    username: str | None = Query(None, description="Username substring (admin only)"),
    conversation_id: str | None = Query(None, description="Filter by conversation UUID"),
    model: str | None = Query(None, description="Run included this model in models_requested"),
    tool: str | None = Query(None, description="Run invoked this tool at least once"),
    status_: str | None = Query(None, alias="status", description="running / done / error / cancelled"),
    has_error: bool | None = Query(None, description="True → error or status=error"),
    q: str | None = Query(None, description="Free-text substring of user_content"),
    started_from: datetime | None = Query(None, description="ISO 8601"),
    started_to: datetime | None = Query(None, description="ISO 8601"),
    cursor: str | None = Query(None, description="started_at|id of last seen row"),
    limit: int = Query(50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_non_boss),
):
    stmt = select(ChatAuditRun)
    stmt = _scope_to_user(stmt, user)

    if _is_admin(user):
        if user_id:
            try:
                stmt = stmt.where(ChatAuditRun.user_id == uuid.UUID(user_id))
            except ValueError:
                raise HTTPException(400, "Invalid user_id")
        if username:
            like = f"%{username}%"
            stmt = stmt.where(ChatAuditRun.username.ilike(like))

    if conversation_id:
        try:
            stmt = stmt.where(ChatAuditRun.conversation_id == uuid.UUID(conversation_id))
        except ValueError:
            raise HTTPException(400, "Invalid conversation_id")

    if model:
        stmt = stmt.where(ChatAuditRun.models_requested.op("@>")([model]))

    if tool:
        stmt = stmt.where(ChatAuditRun.tool_calls_by_name.has_key(tool))  # noqa: W601 (JSONB has_key)

    if status_:
        stmt = stmt.where(ChatAuditRun.status == status_)

    if has_error:
        stmt = stmt.where(or_(
            ChatAuditRun.status == "error",
            ChatAuditRun.error_message.isnot(None),
        ))

    if q:
        stmt = stmt.where(ChatAuditRun.user_content.ilike(f"%{q}%"))

    if started_from:
        stmt = stmt.where(ChatAuditRun.started_at >= started_from)
    if started_to:
        stmt = stmt.where(ChatAuditRun.started_at <= started_to)

    # Cursor pagination: started_at|run_id
    if cursor:
        try:
            ts_str, rid_str = cursor.split("|", 1)
            cur_ts = datetime.fromisoformat(ts_str)
            cur_id = uuid.UUID(rid_str)
            stmt = stmt.where(or_(
                ChatAuditRun.started_at < cur_ts,
                and_(
                    ChatAuditRun.started_at == cur_ts,
                    ChatAuditRun.id < cur_id,
                ),
            ))
        except (ValueError, IndexError):
            raise HTTPException(400, "Invalid cursor")

    stmt = stmt.order_by(ChatAuditRun.started_at.desc(), ChatAuditRun.id.desc()).limit(limit + 1)

    rows = (await db.execute(stmt)).scalars().all()
    has_more = len(rows) > limit
    rows = rows[:limit]
    next_cursor = None
    if has_more and rows:
        last = rows[-1]
        next_cursor = f"{last.started_at.isoformat()}|{last.id}"

    return {
        "runs": [_serialise_run_summary(r) for r in rows],
        "next_cursor": next_cursor,
        "scope": "all" if _is_admin(user) else "self",
    }


@router.get("/runs/{run_id}")
async def get_run(
    run_id: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_non_boss),
):
    try:
        rid = uuid.UUID(run_id)
    except ValueError:
        raise HTTPException(400, "Invalid run_id")

    stmt = select(ChatAuditRun).where(ChatAuditRun.id == rid)
    stmt = _scope_to_user(stmt, user)
    run = (await db.execute(stmt)).scalar_one_or_none()
    if run is None:
        raise HTTPException(404, "Run not found")

    # User-facing message (the prompt that triggered this run) and the
    # final stored model responses + their citation sources, joined for
    # 1:1 display with what the user actually saw.
    message_data: dict[str, Any] | None = None
    model_responses: list[dict[str, Any]] = []
    if run.message_id:
        msg = await db.scalar(
            select(ChatMessage).where(ChatMessage.id == run.message_id),
        )
        if msg:
            message_data = {
                "id": str(msg.id),
                "conversation_id": str(msg.conversation_id),
                "role": msg.role,
                "content": msg.content,
                "attachments": msg.attachments or [],
                "created_at": msg.created_at.isoformat() if msg.created_at else None,
            }
        resps = (await db.execute(
            select(ChatModelResponse)
            .where(ChatModelResponse.message_id == run.message_id)
            .order_by(ChatModelResponse.created_at)
        )).scalars().all()
        for r in resps:
            model_responses.append({
                "id": str(r.id),
                "model_id": r.model_id,
                "model_name": r.model_name,
                "content": r.content,
                "tokens_used": r.tokens_used,
                "latency_ms": r.latency_ms,
                "rating": r.rating,
                "rating_comment": r.rating_comment,
                "error": r.error,
                "sources": r.sources or [],
                "created_at": r.created_at.isoformat() if r.created_at else None,
            })

    return {
        "run": _serialise_run_summary(run),
        "user_content_full": run.user_content,
        "message": message_data,
        "model_responses": model_responses,
    }


@router.get("/runs/{run_id}/events")
async def list_events(
    run_id: str,
    event_type: str | None = Query(None),
    model_id: str | None = Query(None),
    round_num: int | None = Query(None),
    tool_name: str | None = Query(None),
    after_seq: int | None = Query(None, description="Return events with sequence > after_seq"),
    limit: int = Query(1000, ge=1, le=5000),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_non_boss),
):
    try:
        rid = uuid.UUID(run_id)
    except ValueError:
        raise HTTPException(400, "Invalid run_id")

    # Authorization: load run first under the same role scope, then 404 if
    # absent or out-of-scope.
    run = (await db.execute(
        _scope_to_user(select(ChatAuditRun.id).where(ChatAuditRun.id == rid), user),
    )).scalar_one_or_none()
    if run is None:
        raise HTTPException(404, "Run not found")

    stmt = select(ChatAuditEvent).where(ChatAuditEvent.run_id == rid)
    if event_type:
        stmt = stmt.where(ChatAuditEvent.event_type == event_type)
    if model_id:
        stmt = stmt.where(ChatAuditEvent.model_id == model_id)
    if round_num is not None:
        stmt = stmt.where(ChatAuditEvent.round_num == round_num)
    if tool_name:
        stmt = stmt.where(ChatAuditEvent.tool_name == tool_name)
    if after_seq is not None:
        stmt = stmt.where(ChatAuditEvent.sequence > after_seq)

    stmt = stmt.order_by(ChatAuditEvent.sequence).limit(limit)
    events = (await db.execute(stmt)).scalars().all()
    return {
        "run_id": str(rid),
        "events": [_serialise_event(e) for e in events],
        "count": len(events),
    }


@router.get("/runs/{run_id}/export")
async def export_run(
    run_id: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_non_boss),
):
    """JSON dump of a run + every event. Streams as application/json file."""
    try:
        rid = uuid.UUID(run_id)
    except ValueError:
        raise HTTPException(400, "Invalid run_id")

    run = (await db.execute(
        _scope_to_user(select(ChatAuditRun).where(ChatAuditRun.id == rid), user),
    )).scalar_one_or_none()
    if run is None:
        raise HTTPException(404, "Run not found")

    events = (await db.execute(
        select(ChatAuditEvent)
        .where(ChatAuditEvent.run_id == rid)
        .order_by(ChatAuditEvent.sequence)
    )).scalars().all()

    payload = {
        "run": _serialise_run_summary(run),
        "user_content_full": run.user_content,
        "events": [_serialise_event(e) for e in events],
    }
    body = json.dumps(payload, ensure_ascii=False, indent=2, default=str)
    return Response(
        content=body,
        media_type="application/json",
        headers={
            "Content-Disposition": f'attachment; filename="chat-audit-{run.trace_id}.json"',
        },
    )


@router.get("/stats")
async def stats(
    days: int = Query(7, ge=1, le=90),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_non_boss),
):
    since = datetime.now(timezone.utc) - timedelta(days=days)
    base = select(ChatAuditRun).where(ChatAuditRun.started_at >= since)
    base = _scope_to_user(base, user)

    total = (await db.execute(
        select(func.count()).select_from(base.subquery()),
    )).scalar_one() or 0
    error_total = (await db.execute(
        select(func.count()).select_from(
            base.where(ChatAuditRun.status == "error").subquery(),
        ),
    )).scalar_one() or 0

    avg_latency = (await db.execute(
        select(func.avg(ChatAuditRun.total_latency_ms)).select_from(base.subquery()),
    )).scalar_one()
    p95_latency = None
    try:
        p95_latency = (await db.execute(
            select(
                func.percentile_cont(0.95).within_group(
                    ChatAuditRun.total_latency_ms.asc(),
                )
            ).select_from(base.subquery()),
        )).scalar_one()
    except Exception:
        # percentile_cont is Postgres-only; absent on SQLite test envs.
        pass

    sum_tokens = (await db.execute(
        select(func.sum(ChatAuditRun.total_tokens)).select_from(base.subquery()),
    )).scalar_one() or 0

    # Top tool counts: aggregate from the per-run JSONB roll-up. Avoids
    # scanning the events table for the dashboard.
    runs_for_tools = (await db.execute(
        select(ChatAuditRun.tool_calls_by_name).select_from(base.subquery()),
    )).scalars().all()
    tool_totals: dict[str, int] = {}
    for tcm in runs_for_tools:
        if not tcm:
            continue
        for k, v in tcm.items():
            try:
                tool_totals[k] = tool_totals.get(k, 0) + int(v)
            except (TypeError, ValueError):
                continue
    top_tools = sorted(tool_totals.items(), key=lambda kv: kv[1], reverse=True)[:10]

    # Top users by run count — admin only (regular users see only their own).
    top_users: list[dict[str, Any]] = []
    if _is_admin(user):
        rows = (await db.execute(
            select(
                ChatAuditRun.username,
                func.count(ChatAuditRun.id),
            )
            .where(ChatAuditRun.started_at >= since)
            .group_by(ChatAuditRun.username)
            .order_by(func.count(ChatAuditRun.id).desc())
            .limit(10)
        )).all()
        top_users = [{"username": r[0] or "(anon)", "count": int(r[1])} for r in rows]

    return {
        "since": since.isoformat(),
        "days": days,
        "total_runs": int(total),
        "error_runs": int(error_total),
        "error_rate": (float(error_total) / total) if total else 0.0,
        "avg_latency_ms": float(avg_latency) if avg_latency is not None else None,
        "p95_latency_ms": float(p95_latency) if p95_latency is not None else None,
        "total_tokens": int(sum_tokens),
        "top_tools": [{"name": k, "count": v} for k, v in top_tools],
        "top_users": top_users,
        "scope": "all" if _is_admin(user) else "self",
    }
