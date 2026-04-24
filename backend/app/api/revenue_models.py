"""Revenue Modeling REST + SSE API."""
from __future__ import annotations

import asyncio
import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from sqlalchemy import select, func, delete
from sqlalchemy.ext.asyncio import AsyncSession

from backend.app.deps import get_current_user, get_db
from backend.app.models.revenue_model import (
    DebateOpinion,
    ModelCell,
    ModelCellVersion,
    ProvenanceTrace,
    RevenueModel,
    SanityIssue,
)
from backend.app.models.recipe import Recipe, RecipeRun
from backend.app.models.user import User
from backend.app.schemas.revenue_model import (
    CellCreate,
    CellUpdate,
    DebateOpinionRead,
    FeedbackEventCreate,
    ModelCellRead,
    ProvenanceTraceRead,
    RecipeRunCreate,
    RecipeRunRead,
    RevenueModelCreate,
    RevenueModelDetail,
    RevenueModelRead,
    RevenueModelUpdate,
    SanityIssueRead,
)
from backend.app.services import model_cell_store as _store
from backend.app.services.recipe_engine import run_recipe, subscribe, unsubscribe
from backend.app.services.revenue_excel_export import export_model_to_excel
from fastapi.responses import Response

logger = logging.getLogger(__name__)
router = APIRouter()


# ── Models (list / create / detail / update / delete) ───────────

@router.get("", response_model=list[RevenueModelRead])
async def list_models(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
    ticker: str | None = Query(None),
    industry: str | None = Query(None),
    status: str | None = Query(None),
):
    q = select(RevenueModel).where(RevenueModel.owner_user_id == user.id)
    if ticker:
        q = q.where(RevenueModel.ticker == ticker)
    if industry:
        q = q.where(RevenueModel.industry == industry)
    if status:
        q = q.where(RevenueModel.status == status)
    q = q.order_by(RevenueModel.updated_at.desc())
    rows = list((await db.execute(q)).scalars().all())
    return [_mk_read(r) for r in rows]


@router.post("", response_model=RevenueModelRead)
async def create_model(
    body: RevenueModelCreate,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    m = RevenueModel(
        ticker=body.ticker,
        company_name=body.company_name,
        industry=body.industry,
        fiscal_periods=list(body.fiscal_periods),
        title=body.title or f"{body.company_name} ({body.ticker})",
        notes=body.notes,
        base_currency=body.base_currency,
        owner_user_id=user.id,
        conversation_id=uuid.UUID(body.conversation_id) if body.conversation_id else None,
        recipe_id=uuid.UUID(body.recipe_id) if body.recipe_id else None,
        status="draft",
    )
    db.add(m)
    await db.commit()
    await db.refresh(m)
    return _mk_read(m)


@router.get("/{model_id}", response_model=RevenueModelDetail)
async def get_model(
    model_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    m = await _get_model(db, model_id, user)
    cells = await _store.get_cells(db, model_id)
    return RevenueModelDetail(
        **_mk_read(m).dict(),
        cells=[_mk_cell(c) for c in cells],
    )


@router.patch("/{model_id}", response_model=RevenueModelRead)
async def update_model(
    model_id: uuid.UUID,
    body: RevenueModelUpdate,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    m = await _get_model(db, model_id, user)
    if body.title is not None:
        m.title = body.title
    if body.notes is not None:
        m.notes = body.notes
    if body.fiscal_periods is not None:
        m.fiscal_periods = list(body.fiscal_periods)
    if body.status is not None:
        m.status = body.status
    await db.commit()
    return _mk_read(m)


@router.delete("/{model_id}")
async def delete_model(
    model_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    m = await _get_model(db, model_id, user)
    await db.delete(m)
    await db.commit()
    return {"ok": True}


# ── Cells (CRUD + formula eval) ─────────────────────────────────

@router.get("/{model_id}/cells", response_model=list[ModelCellRead])
async def list_cells(
    model_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
    path_prefix: str | None = Query(None),
):
    await _get_model(db, model_id, user)
    q = select(ModelCell).where(ModelCell.model_id == model_id)
    if path_prefix:
        q = q.where(ModelCell.path.like(f"{path_prefix}%"))
    q = q.order_by(ModelCell.path)
    rows = list((await db.execute(q)).scalars().all())
    return [_mk_cell(c) for c in rows]


@router.post("/{model_id}/cells", response_model=ModelCellRead)
async def create_cell(
    model_id: uuid.UUID,
    body: CellCreate,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    await _get_model(db, model_id, user)
    cell = await _store.upsert_cell(
        db, model_id,
        path=body.path,
        label=body.label,
        period=body.period,
        unit=body.unit,
        value=body.value,
        value_text=body.value_text,
        formula=body.formula,
        value_type=body.value_type,
        source_type=body.source_type,
        confidence=body.confidence,
        confidence_reason=body.confidence_reason,
        citations=[c.dict() for c in body.citations],
        notes=body.notes,
        alternative_values=[a.dict() for a in body.alternative_values],
        extra=body.extra,
        edited_by=user.id,
        edit_reason="manual create",
    )
    # Evaluate formulas so the cascade reflects the new cell
    await _store.evaluate_formulas(db, model_id)
    await db.commit()
    await db.refresh(cell)
    # Feedback event
    await _store.emit_feedback(
        db, user_id=user.id, event_type="cell_edit",
        model_id=model_id, cell_id=cell.id,
        cell_path=body.path,
        payload={"action": "create", "value": body.value, "formula": body.formula},
    )
    await db.commit()
    return _mk_cell(cell)


@router.patch("/{model_id}/cells/{cell_id}", response_model=ModelCellRead)
async def update_cell(
    model_id: uuid.UUID,
    cell_id: uuid.UUID,
    body: CellUpdate,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    m = await _get_model(db, model_id, user)
    cell = await db.get(ModelCell, cell_id)
    if not cell or cell.model_id != model_id:
        raise HTTPException(404, "cell not found")
    prev_snapshot = {
        "value": cell.value,
        "formula": cell.formula,
        "source_type": cell.source_type,
        "confidence": cell.confidence,
    }

    if body.pick_alternative_idx is not None:
        alts = list(cell.alternative_values or [])
        idx = body.pick_alternative_idx
        if idx < 0 or idx >= len(alts):
            raise HTTPException(400, "pick_alternative_idx out of range")
        picked = alts[idx]
        cell.value = float(picked.get("value")) if picked.get("value") is not None else cell.value
        cell.confidence_reason = picked.get("notes") or cell.confidence_reason
        # rotate: put the current main value at the top of alts
        alts.pop(idx)
        alts.insert(0, {
            "value": prev_snapshot["value"], "source": "previous_main",
            "label": "previous main value",
        })
        cell.alternative_values = alts
        # feedback event for alternative pick
        await _store.emit_feedback(
            db, user_id=user.id, event_type="alternative_picked",
            model_id=model_id, cell_id=cell.id,
            industry=m.industry, cell_path=cell.path,
            payload={"picked_idx": idx, "picked_value": picked.get("value"),
                     "picked_label": picked.get("label")},
        )

    await _store.upsert_cell(
        db, model_id,
        path=cell.path,
        label=cell.label,
        value=body.value if body.value is not None else cell.value,
        value_text=body.value_text if body.value_text is not None else cell.value_text,
        formula=body.formula if body.formula is not None else cell.formula,
        value_type=cell.value_type,
        source_type=body.source_type or cell.source_type,
        confidence=body.confidence or cell.confidence,
        confidence_reason=cell.confidence_reason,
        citations=cell.citations,
        notes=body.notes if body.notes is not None else cell.notes,
        alternative_values=(
            [a.dict() for a in body.alternative_values]
            if body.alternative_values is not None
            else cell.alternative_values
        ),
        extra=cell.extra,
        edited_by=user.id,
        edit_reason=body.edit_reason or "manual edit",
        respect_lock=False,
    )
    if body.locked_by_human is not None:
        cell.locked_by_human = body.locked_by_human
    if body.review_status is not None:
        cell.review_status = body.review_status

    await _store.evaluate_formulas(db, model_id)
    await _store.update_model_counts(db, model_id)

    # Feedback events — emit one per type of change so the consolidator
    # can distinguish "researcher corrected the value" from "researcher
    # changed the source label" from "researcher flagged the cell".
    new_value = body.value if body.value is not None else cell.value
    if body.value is not None and body.value != prev_snapshot["value"]:
        await _store.emit_feedback(
            db, user_id=user.id, event_type="cell_edit",
            model_id=model_id, cell_id=cell.id,
            industry=m.industry, cell_path=cell.path,
            payload={
                "old": prev_snapshot,
                "new": {
                    "value": new_value,
                    "formula": body.formula,
                    "source_type": body.source_type,
                },
                "reason": body.edit_reason,
                "unit": cell.unit,
                "value_type": cell.value_type,
            },
        )
    if body.formula is not None and body.formula != prev_snapshot["formula"]:
        await _store.emit_feedback(
            db, user_id=user.id, event_type="formula_edit",
            model_id=model_id, cell_id=cell.id,
            industry=m.industry, cell_path=cell.path,
            payload={"old": prev_snapshot["formula"], "new": body.formula,
                     "reason": body.edit_reason},
        )
    if body.source_type is not None and body.source_type != prev_snapshot["source_type"]:
        await _store.emit_feedback(
            db, user_id=user.id, event_type="source_type_override",
            model_id=model_id, cell_id=cell.id,
            industry=m.industry, cell_path=cell.path,
            payload={"old": prev_snapshot["source_type"], "new": body.source_type,
                     "reason": body.edit_reason},
        )
    if body.confidence is not None and body.confidence != prev_snapshot["confidence"]:
        await _store.emit_feedback(
            db, user_id=user.id, event_type="confidence_override",
            model_id=model_id, cell_id=cell.id,
            industry=m.industry, cell_path=cell.path,
            payload={"old": prev_snapshot["confidence"], "new": body.confidence,
                     "reason": body.edit_reason},
        )
    if body.review_status is not None:
        await _store.emit_feedback(
            db, user_id=user.id, event_type="review_status_change",
            model_id=model_id, cell_id=cell.id,
            industry=m.industry, cell_path=cell.path,
            payload={"new": body.review_status, "reason": body.edit_reason},
        )
    if body.locked_by_human is not None:
        await _store.emit_feedback(
            db, user_id=user.id, event_type="lock_change",
            model_id=model_id, cell_id=cell.id,
            industry=m.industry, cell_path=cell.path,
            payload={"locked": body.locked_by_human},
        )
    await db.commit()
    await db.refresh(cell)
    return _mk_cell(cell)


@router.delete("/{model_id}/cells/{cell_id}")
async def delete_cell(
    model_id: uuid.UUID,
    cell_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    await _get_model(db, model_id, user)
    cell = await db.get(ModelCell, cell_id)
    if not cell or cell.model_id != model_id:
        raise HTTPException(404, "cell not found")
    await db.delete(cell)
    await _store.update_model_counts(db, model_id)
    await db.commit()
    return {"ok": True}


@router.post("/{model_id}/evaluate")
async def evaluate(
    model_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    await _get_model(db, model_id, user)
    result = await _store.evaluate_formulas(db, model_id)
    await db.commit()
    return result


@router.get("/{model_id}/export.xlsx")
async def export_xlsx(
    model_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    m = await _get_model(db, model_id, user)
    cells = await _store.get_cells(db, model_id)
    buf = export_model_to_excel(m, cells)
    fname = f"{m.ticker.replace('.', '_')}_{m.company_name[:20].replace(' ', '_')}.xlsx"
    return Response(
        content=buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )


# ── History, Provenance, Debate, Sanity ─────────────────────────

@router.get("/{model_id}/cells/{cell_id}/history")
async def cell_history(
    model_id: uuid.UUID,
    cell_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    await _get_model(db, model_id, user)
    q = (
        select(ModelCellVersion)
        .where(ModelCellVersion.cell_id == cell_id)
        .order_by(ModelCellVersion.created_at.desc())
    )
    rows = list((await db.execute(q)).scalars().all())
    return [
        {
            "id": str(r.id),
            "value": r.value,
            "value_text": r.value_text,
            "formula": r.formula,
            "source_type": r.source_type,
            "confidence": r.confidence,
            "edited_by": str(r.edited_by) if r.edited_by else None,
            "edit_reason": r.edit_reason,
            "snapshot": r.snapshot,
            "created_at": r.created_at.isoformat(),
        }
        for r in rows
    ]


@router.get("/{model_id}/provenance/{trace_id}", response_model=ProvenanceTraceRead)
async def get_provenance(
    model_id: uuid.UUID,
    trace_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    await _get_model(db, model_id, user)
    trace = await db.get(ProvenanceTrace, trace_id)
    if not trace or trace.model_id != model_id:
        raise HTTPException(404, "trace not found")
    return ProvenanceTraceRead(
        id=str(trace.id), model_id=str(trace.model_id),
        cell_path=trace.cell_path, step_id=trace.step_id,
        steps=trace.steps, raw_evidence=trace.raw_evidence,
        total_tokens=trace.total_tokens, total_latency_ms=trace.total_latency_ms,
        created_at=trace.created_at,
    )


@router.get("/{model_id}/cells/{cell_id}/debate", response_model=list[DebateOpinionRead])
async def list_debate(
    model_id: uuid.UUID,
    cell_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    await _get_model(db, model_id, user)
    q = select(DebateOpinion).where(
        DebateOpinion.cell_id == cell_id, DebateOpinion.model_id == model_id
    ).order_by(DebateOpinion.created_at)
    rows = list((await db.execute(q)).scalars().all())
    return [
        DebateOpinionRead(
            id=str(r.id), cell_id=str(r.cell_id), model_key=r.model_key,
            role=r.role, value=r.value, reasoning=r.reasoning,
            citations=r.citations, confidence=r.confidence,
            tokens_used=r.tokens_used, latency_ms=r.latency_ms,
            created_at=r.created_at,
        )
        for r in rows
    ]


@router.get("/{model_id}/sanity", response_model=list[SanityIssueRead])
async def list_sanity(
    model_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    await _get_model(db, model_id, user)
    q = select(SanityIssue).where(SanityIssue.model_id == model_id).order_by(
        SanityIssue.severity.desc(), SanityIssue.created_at.desc()
    )
    rows = list((await db.execute(q)).scalars().all())
    return [
        SanityIssueRead(
            id=str(r.id), model_id=str(r.model_id),
            issue_type=r.issue_type, severity=r.severity,
            cell_paths=r.cell_paths, message=r.message,
            suggested_fix=r.suggested_fix, details=r.details,
            resolved=r.resolved, created_at=r.created_at,
        )
        for r in rows
    ]


# ── Run the recipe ──────────────────────────────────────────────

from pydantic import BaseModel as _PydBase


class ABRunCreate(_PydBase):
    recipe_a_id: str
    recipe_b_id: str
    settings: dict[str, Any] = {}


@router.post("/{model_id}/ab-run")
async def start_ab_run(
    model_id: uuid.UUID,
    body: ABRunCreate,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Start two parallel runs (A=original recipe, B=challenger) against
    the same model. Each run uses its own isolated copy of the model so
    cells don't collide — the UI then diffs the two resulting cell sets.
    """
    src = await _get_model(db, model_id, user)
    from backend.app.models.revenue_model import ModelCell as _MC
    import uuid as _uuid
    session_id = _uuid.uuid4().hex

    # Duplicate the source model twice so the two runs don't stomp on each other
    async def _clone(ticker_suffix: str) -> RevenueModel:
        clone = RevenueModel(
            ticker=src.ticker,
            company_name=f"{src.company_name} [{ticker_suffix}]",
            industry=src.industry,
            fiscal_periods=list(src.fiscal_periods),
            title=f"{src.title or src.company_name} [{ticker_suffix}]",
            notes=f"A/B session {session_id}",
            base_currency=src.base_currency,
            owner_user_id=user.id,
            status="running",
            conversation_id=src.conversation_id,
        )
        db.add(clone)
        await db.flush()
        # copy cells (non-derived) so EXTRACT_HISTORICAL has history
        q = select(ModelCell).where(ModelCell.model_id == src.id)
        originals = list((await db.execute(q)).scalars().all())
        for c in originals:
            nc = ModelCell(
                model_id=clone.id,
                path=c.path, label=c.label, period=c.period, unit=c.unit,
                value=c.value, value_text=c.value_text, formula=c.formula,
                depends_on=list(c.depends_on or []),
                value_type=c.value_type, source_type=c.source_type,
                confidence=c.confidence, confidence_reason=c.confidence_reason,
                citations=list(c.citations or []),
                notes=c.notes, alternative_values=list(c.alternative_values or []),
                extra=dict(c.extra or {}),
            )
            db.add(nc)
        await db.flush()
        return clone

    clone_a = await _clone("A")
    clone_b = await _clone("B")

    # Resolve recipes
    rec_a = await db.get(Recipe, uuid.UUID(body.recipe_a_id))
    rec_b = await db.get(Recipe, uuid.UUID(body.recipe_b_id))
    if not rec_a or not rec_b:
        raise HTTPException(404, "Recipe A or B not found")

    dry_run = bool(body.settings.get("dry_run"))

    async def _mk_run(clone: RevenueModel, rec: Recipe, group: str) -> RecipeRun:
        run = RecipeRun(
            recipe_id=rec.id, recipe_version=rec.version,
            model_id=clone.id, ticker=clone.ticker,
            started_by=user.id, status="pending",
            settings=body.settings or {},
            ab_group=group,
            ab_session=session_id,
        )
        db.add(run)
        return run

    run_a = await _mk_run(clone_a, rec_a, "A")
    run_b = await _mk_run(clone_b, rec_b, "B")
    clone_a.recipe_id = rec_a.id
    clone_a.recipe_version = rec_a.version
    clone_b.recipe_id = rec_b.id
    clone_b.recipe_version = rec_b.version
    await db.commit()
    await db.refresh(run_a)
    await db.refresh(run_b)

    asyncio.create_task(run_recipe(run_a.id, dry_run=dry_run))
    asyncio.create_task(run_recipe(run_b.id, dry_run=dry_run))
    return {
        "session": session_id,
        "run_a": str(run_a.id),
        "run_b": str(run_b.id),
        "model_a": str(clone_a.id),
        "model_b": str(clone_b.id),
    }


@router.get("/ab/{session_id}")
async def get_ab_session(
    session_id: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    q = select(RecipeRun).where(RecipeRun.ab_session == session_id).order_by(RecipeRun.ab_group)
    runs = list((await db.execute(q)).scalars().all())
    if not runs:
        raise HTTPException(404, "AB session not found")
    out = []
    for r in runs:
        cells_q = select(ModelCell).where(ModelCell.model_id == r.model_id)
        cells = list((await db.execute(cells_q)).scalars().all())
        out.append({
            "group": r.ab_group,
            "run": _mk_run(r).dict(),
            "cells": [_mk_cell(c).dict() for c in cells],
        })
    return {"session": session_id, "groups": out}


@router.post("/{model_id}/runs", response_model=RecipeRunRead)
async def start_run(
    model_id: uuid.UUID,
    body: RecipeRunCreate,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    m = await _get_model(db, model_id, user)
    recipe_id = uuid.UUID(body.recipe_id) if body.recipe_id else m.recipe_id
    if not recipe_id:
        # try to pick a default recipe for the industry
        q = (
            select(Recipe)
            .where(Recipe.industry == m.industry, Recipe.is_public == True)  # noqa
            .order_by(Recipe.version.desc())
            .limit(1)
        )
        rec = (await db.execute(q)).scalar_one_or_none()
        if not rec:
            raise HTTPException(400, f"No recipe available for industry {m.industry}")
        recipe_id = rec.id
    rec = await db.get(Recipe, recipe_id)
    if not rec:
        raise HTTPException(404, "recipe not found")

    # Pre-flight cost gate — top-trading-firm best practice: every agent run
    # gets a dollar budget before it starts. Rejection is loud (HTTP 402) so
    # researchers know to request a quota bump rather than silently retry.
    from backend.app.services.cost_estimation import (
        estimate_recipe_cost as _estimate_cost,
        check_user_quota as _check_quota,
    )
    settings_bag = body.settings or {}
    preset_model = str(settings_bag.get("model_id") or "anthropic/claude-opus-4-7")
    debate_roles = int(settings_bag.get("debate_roles") or 3)
    is_dry = bool(settings_bag.get("dry_run"))
    estimate = _estimate_cost(
        rec.graph or {},
        default_model_id=preset_model,
        debate_roles=debate_roles,
    )
    # Dry runs don't burn real $, so the gate only applies to wet runs.
    if not is_dry:
        quota = await _check_quota(db, user, estimated_add_usd=estimate.total_usd)
        # Hard stop when quota exceeded unless caller explicitly overrides (admin only).
        if quota.exceeded and str(settings_bag.get("override_quota") or "").lower() != "true":
            raise HTTPException(
                status_code=402,
                detail={
                    "error": "monthly_quota_exceeded",
                    "estimated_cost_usd": estimate.total_usd,
                    "spent_this_month_usd": quota.spent_this_month_usd,
                    "monthly_budget_usd": quota.monthly_budget_usd,
                    "message": "Pre-flight cost estimate would exceed your monthly LLM budget.",
                },
            )
    run_cap = None
    explicit_cap = settings_bag.get("cost_cap_usd")
    if explicit_cap is not None:
        run_cap = float(explicit_cap)
    elif user.llm_run_cap_usd:
        run_cap = float(user.llm_run_cap_usd)
    elif not is_dry:
        # Default per-run cap = 3x the pre-flight estimate (catches runaway loops
        # without pinching well-behaved runs).
        run_cap = max(1.0, round(estimate.total_usd * 3.0, 2))

    run = RecipeRun(
        recipe_id=rec.id, recipe_version=rec.version,
        model_id=m.id, ticker=m.ticker,
        started_by=user.id, status="pending",
        settings=settings_bag,
        estimated_cost_usd=estimate.total_usd,
        cost_cap_usd=run_cap,
    )
    db.add(run)
    m.recipe_id = rec.id
    m.recipe_version = rec.version
    m.status = "running"
    await db.commit()
    await db.refresh(run)

    asyncio.create_task(run_recipe(run.id, dry_run=is_dry))
    return _mk_run(run)


@router.get("/{model_id}/runs", response_model=list[RecipeRunRead])
async def list_runs(
    model_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    await _get_model(db, model_id, user)
    q = select(RecipeRun).where(RecipeRun.model_id == model_id).order_by(RecipeRun.created_at.desc())
    rows = list((await db.execute(q)).scalars().all())
    return [_mk_run(r) for r in rows]


@router.get("/runs/{run_id}", response_model=RecipeRunRead)
async def get_run(
    run_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    r = await db.get(RecipeRun, run_id)
    if not r:
        raise HTTPException(404)
    return _mk_run(r)


@router.get("/runs/{run_id}/stream")
async def stream_run(run_id: uuid.UUID, request: Request):
    """SSE stream of step events for a running recipe."""
    q = subscribe(run_id)

    async def generator():
        try:
            # Send a hello event so client knows subscription is live
            yield f"data: {json.dumps({'type': 'subscribed', 'run_id': str(run_id)})}\n\n"
            while True:
                if await request.is_disconnected():
                    break
                try:
                    evt = await asyncio.wait_for(q.get(), timeout=15)
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
                    continue
                payload = json.dumps(evt.as_dict(), default=str, ensure_ascii=False)
                yield f"event: {evt.type}\ndata: {payload}\n\n"
                if evt.type in ("run_completed", "step_failed"):
                    break
        finally:
            unsubscribe(run_id, q)

    return StreamingResponse(generator(), media_type="text/event-stream", headers={
        "Cache-Control": "no-cache",
        "X-Accel-Buffering": "no",
    })


# ── Feedback (explicit event emit) ──────────────────────────────

@router.post("/{model_id}/feedback")
async def post_feedback(
    model_id: uuid.UUID,
    body: FeedbackEventCreate,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    m = await _get_model(db, model_id, user)
    await _store.emit_feedback(
        db, user_id=user.id, event_type=body.event_type,
        model_id=m.id,
        cell_id=uuid.UUID(body.cell_id) if body.cell_id else None,
        recipe_id=uuid.UUID(body.recipe_id) if body.recipe_id else None,
        industry=body.industry or m.industry,
        cell_path=body.cell_path,
        payload=body.payload,
    )
    await db.commit()
    return {"ok": True}


# ── Helpers ────────────────────────────────────────────────────

async def _get_model(
    db: AsyncSession, model_id: uuid.UUID, user: User, *, min_role: str = "viewer",
) -> RevenueModel:
    m = await db.get(RevenueModel, model_id)
    if not m:
        raise HTTPException(404, "model not found")
    if m.owner_user_id == user.id or user.role in ("admin", "boss"):
        return m
    # Check collaborator ACL
    try:
        from backend.app.models.revenue_model_extras import ModelCollaborator
        q = select(ModelCollaborator).where(
            ModelCollaborator.model_id == model_id,
            ModelCollaborator.user_id == user.id,
        )
        row = (await db.execute(q)).scalar_one_or_none()
        if row:
            rank = {"viewer": 0, "editor": 1, "admin": 2}
            if rank.get(row.role, 0) >= rank.get(min_role, 0):
                return m
    except Exception:
        pass
    raise HTTPException(403, "not your model")


def _mk_read(m: RevenueModel) -> RevenueModelRead:
    return RevenueModelRead(
        id=str(m.id),
        ticker=m.ticker,
        company_name=m.company_name,
        industry=m.industry,
        fiscal_periods=list(m.fiscal_periods or []),
        recipe_id=str(m.recipe_id) if m.recipe_id else None,
        recipe_version=m.recipe_version,
        status=m.status,
        title=m.title,
        notes=m.notes,
        base_currency=m.base_currency,
        cell_count=m.cell_count,
        flagged_count=m.flagged_count,
        owner_user_id=str(m.owner_user_id),
        last_run_id=str(m.last_run_id) if m.last_run_id else None,
        conversation_id=str(m.conversation_id) if m.conversation_id else None,
        paused_by_guard=bool(getattr(m, "paused_by_guard", False)),
        paused_reason=getattr(m, "paused_reason", None),
        created_at=m.created_at,
        updated_at=m.updated_at,
    )


def _mk_cell(c: ModelCell) -> ModelCellRead:
    return ModelCellRead(
        id=str(c.id),
        model_id=str(c.model_id),
        path=c.path,
        label=c.label,
        period=c.period,
        unit=c.unit,
        value=c.value,
        value_text=c.value_text,
        formula=c.formula,
        depends_on=list(c.depends_on or []),
        value_type=c.value_type,
        source_type=c.source_type,
        confidence=c.confidence,
        confidence_reason=c.confidence_reason,
        citations=c.citations or [],
        notes=c.notes,
        alternative_values=c.alternative_values or [],
        provenance_trace_id=str(c.provenance_trace_id) if c.provenance_trace_id else None,
        locked_by_human=c.locked_by_human,
        human_override=c.human_override,
        review_status=c.review_status,
        extra=c.extra or {},
        created_at=c.created_at,
        updated_at=c.updated_at,
    )


def _mk_run(r: RecipeRun) -> RecipeRunRead:
    return RecipeRunRead(
        id=str(r.id),
        recipe_id=str(r.recipe_id),
        recipe_version=r.recipe_version,
        model_id=str(r.model_id),
        ticker=r.ticker,
        started_by=str(r.started_by) if r.started_by else None,
        status=r.status,
        current_step_id=r.current_step_id,
        step_results=r.step_results or {},
        total_tokens=r.total_tokens,
        total_cost_usd=r.total_cost_usd,
        estimated_cost_usd=float(getattr(r, "estimated_cost_usd", 0.0) or 0.0),
        cost_cap_usd=float(r.cost_cap_usd) if getattr(r, "cost_cap_usd", None) else None,
        paused_reason=getattr(r, "paused_reason", None),
        error=r.error,
        settings=r.settings or {},
        created_at=r.created_at,
        updated_at=r.updated_at,
        completed_at=r.completed_at,
    )
