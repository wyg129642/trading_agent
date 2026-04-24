"""Playbook API — read packs, list lessons, approve pending lessons."""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.app.deps import get_current_boss_or_admin, get_current_user, get_db
from backend.app.models.feedback import PendingLesson
from backend.app.models.user import User
from backend.app.schemas.revenue_model import PendingLessonRead, PendingLessonReview
from backend.app.services import playbook_service
from backend.app.services import feedback_consolidator

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/packs")
async def list_packs(user: User = Depends(get_current_user)):
    return playbook_service.list_packs()


@router.get("/packs/{slug}")
async def read_pack(
    slug: str, user: User = Depends(get_current_user)
):
    data = playbook_service.read_pack_playbook(slug)
    if data is None:
        raise HTTPException(404, "pack not found")
    return data


class PackPlaybookUpdate(BaseModel):
    body: str
    filename: str = "lessons.md"


@router.patch("/packs/{slug}")
async def update_pack(
    slug: str,
    body: PackPlaybookUpdate,
    user: User = Depends(get_current_boss_or_admin),
):
    ok = playbook_service.save_pack_playbook(slug, body.filename, body.body)
    if not ok:
        raise HTTPException(400, "bad filename or pack not found")
    return {"ok": True}


# ── YAML + structured file I/O for the Pack Editor ────────────────

_YAML_FILES = ("pack.yaml", "segments_schema.yaml", "sanity_rules.yaml")


@router.get("/packs/{slug}/files")
async def read_pack_files(
    slug: str, user: User = Depends(get_current_user),
):
    """Return all YAML + markdown files for a pack as a filename→string map."""
    from industry_packs import pack_registry
    from pathlib import Path as _P
    p = pack_registry.get(slug)
    if p is None:
        raise HTTPException(404, "pack not found")
    out: dict[str, str] = {}
    for fname in _YAML_FILES:
        fp: _P = p.root / fname
        try:
            out[fname] = fp.read_text(encoding="utf-8") if fp.exists() else ""
        except Exception:
            out[fname] = ""
    return out


class PackFileUpdate(BaseModel):
    filename: str
    body: str


@router.patch("/packs/{slug}/files")
async def update_pack_file(
    slug: str, body: PackFileUpdate,
    user: User = Depends(get_current_boss_or_admin),
):
    """Write back an individual pack file (YAML or markdown). Validates YAML."""
    from industry_packs import pack_registry
    from pathlib import Path as _P
    import yaml as _yaml
    p = pack_registry.get(slug)
    if p is None:
        raise HTTPException(404, "pack not found")
    fname = body.filename.strip()
    # Markdown files go through the existing service for in-memory reload
    if fname in ("overview.md", "lessons.md", "rules.md"):
        ok = playbook_service.save_pack_playbook(slug, fname, body.body)
        if not ok:
            raise HTTPException(400, "save failed")
        return {"ok": True, "reloaded": True}
    if fname not in _YAML_FILES:
        raise HTTPException(400, f"unsupported filename: {fname}")
    # Validate YAML
    try:
        _yaml.safe_load(body.body)
    except _yaml.YAMLError as e:
        raise HTTPException(400, f"YAML syntax error: {e}")
    fp: _P = p.root / fname
    fp.parent.mkdir(parents=True, exist_ok=True)
    fp.write_text(body.body, encoding="utf-8")
    # Reload in-memory pack so subsequent requests see the new content
    pack_registry.reload()
    return {"ok": True, "reloaded": True}


@router.get("/packs/{slug}/lessons")
async def list_lessons(slug: str, user: User = Depends(get_current_user)):
    from backend.app.services import lesson_versioning
    lessons = playbook_service.list_lessons(slug)
    statuses = lesson_versioning.statuses_for_pack(slug)
    # Attach status + expired flag to each lesson
    for l in lessons:
        l["status"] = statuses.get(l["id"], "active")
        l["expired"] = lesson_versioning.is_expired(l.get("body", ""))
    return lessons


@router.post("/packs/{slug}/auto-archive")
async def auto_archive_expired_lessons(
    slug: str,
    user: User = Depends(get_current_boss_or_admin),
):
    """Admin: rewrite lessons.md to mark expired lessons as archived."""
    from backend.app.services import lesson_versioning
    return await lesson_versioning.auto_archive_expired(slug)


class ContradictionCheckRequest(BaseModel):
    industry: str
    body: str
    similarity_threshold: float = 0.88


@router.post("/lessons/check-contradictions")
async def check_contradictions(
    body: ContradictionCheckRequest,
    user: User = Depends(get_current_boss_or_admin),
):
    """Check whether a new (or edited) lesson body is semantically close to
    an existing lesson in the same industry — so approvers see the clash
    before writing."""
    from backend.app.services import lesson_versioning
    hits = await lesson_versioning.detect_contradictions(
        body.industry, body.body, similarity_threshold=body.similarity_threshold,
    )
    return {"contradictions": hits, "count": len(hits)}


@router.post("/packs/{slug}/reindex-vectors")
async def reindex_pack_vectors(
    slug: str,
    user: User = Depends(get_current_boss_or_admin),
):
    """Admin: bulk-reindex every lesson in a pack into the Milvus collection."""
    from backend.app.services import lesson_versioning
    from backend.app.services.lesson_vector_search import reindex_pack
    lessons = playbook_service.list_lessons(slug)
    statuses = lesson_versioning.statuses_for_pack(slug)
    return await reindex_pack(slug, lessons, statuses=statuses)


@router.get("/packs/{slug}/search")
async def search_lessons(
    slug: str,
    cell_path: str = Query(...),
    user: User = Depends(get_current_user),
):
    return {"snippets": playbook_service.search_lessons(slug, cell_path)}


@router.get("/pending", response_model=list[PendingLessonRead])
async def list_pending_lessons(
    industry: str | None = Query(None),
    status: str | None = Query("pending"),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_boss_or_admin),
):
    q = select(PendingLesson)
    if industry:
        q = q.where(PendingLesson.industry == industry)
    if status:
        q = q.where(PendingLesson.status == status)
    q = q.order_by(PendingLesson.created_at.desc())
    rows = list((await db.execute(q)).scalars().all())
    return [
        PendingLessonRead(
            id=str(r.id),
            industry=r.industry,
            lesson_id=r.lesson_id,
            title=r.title,
            body=r.body,
            scenario=r.scenario,
            observation=r.observation,
            rule=r.rule,
            sources=r.sources,
            status=r.status,
            reviewed_by=str(r.reviewed_by) if r.reviewed_by else None,
            review_note=r.review_note,
            batch_week=r.batch_week,
            created_at=r.created_at,
            reviewed_at=r.reviewed_at,
        )
        for r in rows
    ]


@router.post("/pending/{lesson_pk}/review")
async def review_lesson(
    lesson_pk: uuid.UUID,
    body: PendingLessonReview,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_boss_or_admin),
):
    row = await db.get(PendingLesson, lesson_pk)
    if not row:
        raise HTTPException(404)
    row.reviewed_by = user.id
    row.review_note = body.review_note
    row.reviewed_at = datetime.now(timezone.utc)
    auto_apply_summary = None
    if body.action == "approve":
        row.status = "approved"
        # Append to the industry pack's lessons.md
        final_body = body.edited_body or row.body
        playbook_service.append_lesson(row.industry, row.lesson_id, final_body)
        # Propagate to existing flagged cells — they may need a rerun now that
        # we have a new rule. Best-effort; errors log but don't abort approval.
        try:
            from backend.app.services.lesson_auto_apply import apply_approved_lesson
            # Use the edited body if provided, so pattern extraction sees final rule
            if body.edited_body:
                row.body = body.edited_body
            auto_apply_summary = await apply_approved_lesson(db, row, approver_id=user.id)
        except Exception:
            logger.exception(
                "lesson_auto_apply failed for lesson %s (non-fatal)", row.lesson_id
            )
    elif body.action == "reject":
        row.status = "rejected"
    elif body.action == "archive":
        row.status = "archived"
    await db.commit()
    resp = {"ok": True, "status": row.status}
    if auto_apply_summary is not None:
        resp["auto_apply"] = auto_apply_summary
    return resp


@router.post("/consolidate")
async def consolidate(
    dry_run: bool = Query(False),
    user: User = Depends(get_current_boss_or_admin),
):
    """Manually trigger feedback consolidation (admin/boss)."""
    return await feedback_consolidator.consolidate_feedback(dry_run=dry_run)


@router.post("/distill-ab-winners")
async def trigger_ab_distill(
    since_days: int = Query(30, ge=1, le=180),
    dry_run: bool = Query(False),
    user: User = Depends(get_current_boss_or_admin),
):
    """Manually trigger the A/B-winner lesson distiller (admin/boss)."""
    from backend.app.services.ab_winner_distiller import distill_ab_winners
    return await distill_ab_winners(since_days=since_days, dry_run=dry_run)


class PackBootstrapReq(BaseModel):
    slug: str
    name: str
    display_name_zh: str
    tickers: list[str]
    description: str = ""
    default_periods: list[str] = []
    base_currency: str = "USD"
    overwrite: bool = False


@router.post("/packs/bootstrap")
async def bootstrap_new_pack(
    body: PackBootstrapReq,
    user: User = Depends(get_current_boss_or_admin),
):
    from backend.app.services.pack_bootstrap import bootstrap_pack
    result = await bootstrap_pack(
        slug=body.slug,
        name=body.name,
        display_name_zh=body.display_name_zh,
        tickers=body.tickers,
        description=body.description,
        default_periods=body.default_periods or None,
        base_currency=body.base_currency,
        overwrite=body.overwrite,
    )
    if "error" in result:
        raise HTTPException(400, result["error"])
    return result


@router.get("/hallucination-rate")
async def get_hallucination_rate(
    since_days: int = Query(7, ge=1, le=365),
    user: User = Depends(get_current_boss_or_admin),
):
    from backend.app.services.citation_audit import hallucination_summary
    return await hallucination_summary(since_days=since_days)


@router.get("/feedback-dashboard")
async def feedback_dashboard(
    since_days: int = Query(30, ge=1, le=365),
    industry: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_boss_or_admin),
):
    """Lesson-effect dashboard: count of feedback events by type, lessons
    produced, hallucination-rate trend (weekly), and lesson impact (flagged
    cell count before/after approval for the top 5 recent lessons).
    """
    from datetime import datetime, timedelta, timezone
    from backend.app.models.feedback import UserFeedbackEvent, PendingLesson
    from backend.app.models.revenue_model import ModelCell, RevenueModel
    from backend.app.services.citation_audit import CitationAuditLog
    from sqlalchemy import func

    cutoff = datetime.now(timezone.utc) - timedelta(days=since_days)

    # Events by type
    q_events = (
        select(UserFeedbackEvent.event_type, func.count(UserFeedbackEvent.id))
        .where(UserFeedbackEvent.created_at >= cutoff)
        .group_by(UserFeedbackEvent.event_type)
    )
    if industry:
        q_events = q_events.where(UserFeedbackEvent.industry == industry)
    events_rows = (await db.execute(q_events)).all()
    events_by_type = {r[0]: r[1] for r in events_rows}

    # Lessons by status
    q_lessons = (
        select(PendingLesson.status, func.count(PendingLesson.id))
        .where(PendingLesson.created_at >= cutoff)
        .group_by(PendingLesson.status)
    )
    if industry:
        q_lessons = q_lessons.where(PendingLesson.industry == industry)
    lesson_rows = (await db.execute(q_lessons)).all()
    lessons_by_status = {r[0]: r[1] for r in lesson_rows}

    # Hallucination-rate weekly trend (last 8 weeks)
    trend: list[dict] = []
    for weeks_ago in range(7, -1, -1):
        w_start = datetime.now(timezone.utc) - timedelta(days=7 * (weeks_ago + 1))
        w_end = datetime.now(timezone.utc) - timedelta(days=7 * weeks_ago)
        q = (
            select(CitationAuditLog.verdict, func.count(CitationAuditLog.id))
            .where(
                CitationAuditLog.created_at >= w_start,
                CitationAuditLog.created_at < w_end,
            )
            .group_by(CitationAuditLog.verdict)
        )
        wrows = (await db.execute(q)).all()
        verdicts = {r[0]: r[1] for r in wrows}
        total = sum(verdicts.values())
        mismatches = verdicts.get("mismatch", 0)
        trend.append({
            "week_start": w_start.date().isoformat(),
            "total_sampled": total,
            "mismatches": mismatches,
            "hallucination_rate": round(mismatches / max(total, 1), 4),
            "verdicts": verdicts,
        })

    # Cell review-status distribution (scoped if industry filter set)
    cell_q = select(ModelCell.review_status, func.count(ModelCell.id))
    if industry:
        cell_q = cell_q.join(RevenueModel, ModelCell.model_id == RevenueModel.id).where(
            RevenueModel.industry == industry,
        )
    cell_q = cell_q.group_by(ModelCell.review_status)
    cell_rows = (await db.execute(cell_q)).all()
    cells_by_review_status = {r[0]: r[1] for r in cell_rows}

    # Recent lesson impact (top 5 approved in the window): how many cells each
    # lesson-applied event touched, so reviewers can see yield.
    recent_approved_q = (
        select(PendingLesson)
        .where(
            PendingLesson.status == "approved",
            PendingLesson.reviewed_at >= cutoff,
        )
        .order_by(PendingLesson.reviewed_at.desc())
        .limit(5)
    )
    if industry:
        recent_approved_q = recent_approved_q.where(PendingLesson.industry == industry)
    recent = list((await db.execute(recent_approved_q)).scalars().all())
    lesson_impact: list[dict] = []
    for l in recent:
        apply_q = (
            select(func.count(UserFeedbackEvent.id))
            .where(
                UserFeedbackEvent.event_type == "lesson_applied",
                UserFeedbackEvent.payload.op("->>")("lesson_id") == l.lesson_id,
            )
        )
        applied_count = (await db.execute(apply_q)).scalar_one() or 0
        lesson_impact.append({
            "lesson_id": l.lesson_id,
            "title": l.title,
            "industry": l.industry,
            "reviewed_at": l.reviewed_at.isoformat() if l.reviewed_at else None,
            "cells_touched_by_auto_apply": applied_count,
        })

    return {
        "since_days": since_days,
        "industry": industry,
        "events_by_type": events_by_type,
        "lessons_by_status": lessons_by_status,
        "total_events": sum(events_by_type.values()),
        "total_lessons": sum(lessons_by_status.values()),
        "hallucination_trend_weekly": trend,
        "cells_by_review_status": cells_by_review_status,
        "recent_lesson_impact": lesson_impact,
    }
