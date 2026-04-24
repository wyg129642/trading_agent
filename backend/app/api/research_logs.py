"""Admin-only API exposing the AI research assistant's interaction logs.

Reads from the MongoDB collection populated by
`backend/app/services/research_interaction_log.py`. The data visualizes every
chat request's full lifecycle: each LLM's rounds, tool calls (with complete
arguments), search fan-out, read_webpage contents, and final responses.

Endpoints:

  GET /api/research-logs/sessions?search=&user=&model=&status=&page=&page_size=
  GET /api/research-logs/sessions/{trace_id}
  GET /api/research-logs/stats

All endpoints require admin role.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, Query

from backend.app.deps import get_current_admin
from backend.app.models.user import User
from backend.app.services.research_interaction_log import get_recorder

logger = logging.getLogger(__name__)
router = APIRouter()


def _serialize_doc(doc: dict) -> dict:
    """Strip Mongo-specific keys, stringify datetimes, and restore model keys.

    Model IDs containing '.' (e.g. 'openai/gpt-5.4') had to be stored with
    '.' replaced by a sentinel to avoid MongoDB's dotted-path semantics; undo
    that here so the frontend sees the canonical model IDs.
    """
    if not doc:
        return {}
    out = dict(doc)
    out.pop("_id", None)
    from backend.app.services.research_interaction_log import _unsafe_key
    if isinstance(out.get("models"), dict):
        fixed = {}
        for safe_k, v in out["models"].items():
            real = _unsafe_key(safe_k)
            if isinstance(v, dict):
                v = {**v, "model_id": v.get("model_id") or real}
            fixed[real] = v
        out["models"] = fixed
    return out


def _list_projection() -> dict:
    """Projection for the paginated list view (keep the payload small)."""
    return {
        "trace_id": 1,
        "user_id": 1,
        "username": 1,
        "conversation_id": 1,
        "query": 1,
        "models_requested": 1,
        "mode": 1,
        "web_search": 1,
        "alphapai_enabled": 1,
        "jinmen_enabled": 1,
        "kb_enabled": 1,
        "tools_enabled": 1,
        "status": 1,
        "total_elapsed_ms": 1,
        "summary": 1,
        "created_at": 1,
        "updated_at": 1,
    }


@router.get("/sessions")
async def list_sessions(
    search: str | None = None,
    user: str | None = None,
    model: str | None = None,
    status: str | None = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(30, ge=1, le=200),
    _admin: User = Depends(get_current_admin),
):
    """List recent AI research sessions (admin only)."""
    rec = get_recorder()
    if not rec.enabled:
        raise HTTPException(
            status_code=503,
            detail=(
                "Research interaction recorder is not connected. "
                f"Reason: {rec._disabled_reason or 'unknown'}"
            ),
        )
    coll = rec._coll()
    if coll is None:
        raise HTTPException(status_code=503, detail="Recorder collection unavailable")

    q: dict = {}
    if search:
        q["query"] = {"$regex": search, "$options": "i"}
    if user:
        q["$or"] = [
            {"username": {"$regex": user, "$options": "i"}},
            {"user_id": user},
        ]
    if model:
        q["models_requested"] = model
    if status:
        q["status"] = status

    total = await coll.count_documents(q)
    cursor = (
        coll.find(q, projection=_list_projection())
        .sort("created_at", -1)
        .skip((page - 1) * page_size)
        .limit(page_size)
    )
    items = [_serialize_doc(d) async for d in cursor]
    return {
        "items": items,
        "total": total,
        "page": page,
        "page_size": page_size,
    }


@router.get("/sessions/{trace_id}")
async def get_session(
    trace_id: str,
    _admin: User = Depends(get_current_admin),
):
    """Return one full session document with complete tool-call bodies."""
    rec = get_recorder()
    if not rec.enabled:
        raise HTTPException(status_code=503, detail="Recorder not connected")
    coll = rec._coll()
    if coll is None:
        raise HTTPException(status_code=503, detail="Recorder collection unavailable")

    doc = await coll.find_one({"trace_id": trace_id})
    if not doc:
        raise HTTPException(status_code=404, detail="Session not found")
    return _serialize_doc(doc)


@router.get("/stats")
async def get_stats(
    days: int = Query(7, ge=1, le=90),
    _admin: User = Depends(get_current_admin),
):
    """Aggregated stats across the last `days` days."""
    rec = get_recorder()
    if not rec.enabled:
        raise HTTPException(status_code=503, detail="Recorder not connected")
    coll = rec._coll()
    if coll is None:
        raise HTTPException(status_code=503, detail="Recorder collection unavailable")

    since = datetime.now(timezone.utc) - timedelta(days=days)

    total = await coll.count_documents({"created_at": {"$gte": since}})
    running = await coll.count_documents({"status": "running"})

    pipeline = [
        {"$match": {"created_at": {"$gte": since}}},
        {"$group": {
            "_id": "$username",
            "requests": {"$sum": 1},
            "tool_calls_total": {"$sum": "$summary.tool_calls_total"},
            "total_tokens": {"$sum": "$summary.total_tokens"},
            "total_elapsed_ms": {"$sum": "$total_elapsed_ms"},
        }},
        {"$sort": {"requests": -1}},
        {"$limit": 20},
    ]
    per_user = [dict(r) async for r in coll.aggregate(pipeline)]

    pipeline_tools = [
        {"$match": {"created_at": {"$gte": since}}},
        {"$unwind": {"path": "$summary.tool_call_names", "preserveNullAndEmptyArrays": False}},
        {"$group": {"_id": "$summary.tool_call_names", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
    ]
    tool_usage = [dict(r) async for r in coll.aggregate(pipeline_tools)]

    pipeline_models = [
        {"$match": {"created_at": {"$gte": since}}},
        {"$unwind": {"path": "$models_requested", "preserveNullAndEmptyArrays": False}},
        {"$group": {"_id": "$models_requested", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
    ]
    model_usage = [dict(r) async for r in coll.aggregate(pipeline_models)]

    return {
        "window_days": days,
        "total_requests": total,
        "running_requests": running,
        "per_user": [{
            "username": r["_id"] or "(unknown)",
            "requests": r["requests"],
            "tool_calls_total": r.get("tool_calls_total") or 0,
            "total_tokens": r.get("total_tokens") or 0,
            "total_elapsed_ms": r.get("total_elapsed_ms") or 0,
        } for r in per_user],
        "tool_usage": [{"tool": r["_id"], "count": r["count"]} for r in tool_usage],
        "model_usage": [{"model": r["_id"], "count": r["count"]} for r in model_usage],
    }


@router.get("/health")
async def health(_admin: User = Depends(get_current_admin)):
    """Report recorder connection status."""
    rec = get_recorder()
    return {
        "enabled": rec.enabled,
        "uri": rec._mongo_uri.split("@")[-1] if "@" in rec._mongo_uri else rec._mongo_uri,
        "db": rec._db_name,
        "collection": rec._collection_name,
        "disabled_reason": rec._disabled_reason if not rec.enabled else None,
    }
