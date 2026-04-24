"""Helpers for reading/writing ModelCell rows.

This module centralizes:
  * ``upsert_cell`` — insert or update a cell (tracks version history).
  * ``set_cell_value`` — simple value write from an executor.
  * ``evaluate_formulas`` — run the formula engine over a model's cells
    and persist computed values back.
  * ``record_provenance`` — write a ProvenanceTrace row.
  * ``emit_feedback`` — log a UserFeedbackEvent.
"""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Iterable

from sqlalchemy import delete, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from backend.app.models.revenue_model import (
    ModelCell,
    ModelCellVersion,
    ProvenanceTrace,
    RevenueModel,
)
from backend.app.models.feedback import UserFeedbackEvent
from backend.app.services.formula_engine import (
    CycleError,
    FormulaEngine,
    extract_paths,
    parse_dependencies,
)

logger = logging.getLogger(__name__)


def _default_periods(model: RevenueModel) -> list[str]:
    return list(model.fiscal_periods) if model.fiscal_periods else []


async def upsert_cell(
    db: AsyncSession,
    model_id: uuid.UUID,
    *,
    path: str,
    label: str = "",
    period: str = "",
    unit: str = "",
    value: float | None = None,
    value_text: str | None = None,
    formula: str | None = None,
    value_type: str = "number",
    source_type: str = "assumption",
    confidence: str = "MEDIUM",
    confidence_reason: str = "",
    citations: list[dict] | None = None,
    notes: str = "",
    alternative_values: list[dict] | None = None,
    provenance_trace_id: uuid.UUID | None = None,
    extra: dict[str, Any] | None = None,
    edited_by: uuid.UUID | None = None,
    edit_reason: str = "",
    respect_lock: bool = True,
) -> ModelCell:
    """Insert or update a cell by (model_id, path). Records a cell_version row."""
    q = select(ModelCell).where(
        ModelCell.model_id == model_id, ModelCell.path == path
    )
    existing = (await db.execute(q)).scalar_one_or_none()

    deps: list[str] = []
    if formula:
        try:
            deps = parse_dependencies(formula)
        except Exception as e:
            logger.warning("Failed to parse deps for %s: %s", path, e)

    if existing is not None:
        if respect_lock and existing.locked_by_human and edited_by is None:
            # Agent is trying to overwrite a human-locked cell — skip.
            logger.info("Skipping agent overwrite of locked cell %s", path)
            return existing

        # Snapshot previous state into history
        db.add(
            ModelCellVersion(
                cell_id=existing.id,
                model_id=existing.model_id,
                value=existing.value,
                value_text=existing.value_text,
                formula=existing.formula,
                source_type=existing.source_type,
                confidence=existing.confidence,
                edited_by=edited_by,
                edit_reason=edit_reason or "upsert_cell",
                snapshot={
                    "citations": existing.citations,
                    "notes": existing.notes,
                    "alternative_values": existing.alternative_values,
                    "depends_on": existing.depends_on,
                },
            )
        )

        existing.label = label or existing.label
        existing.period = period or existing.period
        existing.unit = unit or existing.unit
        if formula is not None:
            existing.formula = formula
            existing.depends_on = deps
            existing.value = value
            existing.value_text = value_text
        else:
            existing.value = value
            existing.value_text = value_text
            existing.formula = None
            existing.depends_on = []
        existing.value_type = value_type
        existing.source_type = source_type
        existing.confidence = confidence
        existing.confidence_reason = confidence_reason
        if citations is not None:
            existing.citations = citations
        existing.notes = notes or existing.notes
        if alternative_values is not None:
            existing.alternative_values = alternative_values
        if provenance_trace_id is not None:
            existing.provenance_trace_id = provenance_trace_id
        if extra is not None:
            existing.extra = extra
        if edited_by is not None:
            existing.human_override = True
        await db.flush()
        return existing

    cell = ModelCell(
        model_id=model_id,
        path=path,
        label=label,
        period=period,
        unit=unit,
        value=value,
        value_text=value_text,
        formula=formula,
        depends_on=deps,
        value_type=value_type,
        source_type=source_type,
        confidence=confidence,
        confidence_reason=confidence_reason,
        citations=citations or [],
        notes=notes,
        alternative_values=alternative_values or [],
        provenance_trace_id=provenance_trace_id,
        human_override=bool(edited_by is not None),
        extra=extra or {},
    )
    db.add(cell)
    await db.flush()
    return cell


async def get_cells(db: AsyncSession, model_id: uuid.UUID) -> list[ModelCell]:
    q = (
        select(ModelCell)
        .where(ModelCell.model_id == model_id)
        .order_by(ModelCell.path)
    )
    return list((await db.execute(q)).scalars().all())


async def get_cells_by_paths(
    db: AsyncSession, model_id: uuid.UUID, paths: list[str]
) -> dict[str, ModelCell]:
    if not paths:
        return {}
    q = select(ModelCell).where(
        ModelCell.model_id == model_id, ModelCell.path.in_(paths)
    )
    rows = list((await db.execute(q)).scalars().all())
    return {r.path: r for r in rows}


async def evaluate_formulas(
    db: AsyncSession, model_id: uuid.UUID
) -> dict[str, Any]:
    """Evaluate every formula cell in a model; persist computed values.

    Returns summary::
        {
            "evaluated": int,
            "errors": [{path, error}],
            "cycle": [path1, path2] | None,
        }
    """
    cells = await get_cells(db, model_id)
    engine = FormulaEngine()
    for c in cells:
        if c.formula:
            engine.set_cell(c.path, formula=c.formula)
        else:
            engine.set_cell(c.path, value=c.value if c.value is not None else c.value_text)

    cycle = engine.find_cycle()
    if cycle:
        return {"evaluated": 0, "errors": [], "cycle": cycle}

    try:
        engine.evaluate_all()
    except CycleError as e:
        return {"evaluated": 0, "errors": [], "cycle": e.cycle}

    errors: list[dict[str, str]] = []
    evaluated = 0
    for c in cells:
        if not c.formula:
            continue
        err = engine.error(c.path)
        if err:
            errors.append({"path": c.path, "error": err})
            c.value = None
        else:
            v = engine.get(c.path)
            # Coerce numeric types to float for DB column; keep text in value_text
            if isinstance(v, (int, float)) and not isinstance(v, bool):
                c.value = float(v)
                c.value_text = None
            elif isinstance(v, bool):
                c.value = 1.0 if v else 0.0
                c.value_text = None
            elif isinstance(v, str):
                c.value = None
                c.value_text = v
            else:
                c.value = None
                c.value_text = None if v is None else str(v)
            evaluated += 1
    await db.flush()
    return {"evaluated": evaluated, "errors": errors, "cycle": None}


async def record_provenance(
    db: AsyncSession,
    model_id: uuid.UUID,
    *,
    cell_path: str | None,
    step_id: str | None,
    steps: list[dict[str, Any]],
    raw_evidence: list[dict[str, Any]] | None = None,
    total_tokens: int = 0,
    total_latency_ms: int = 0,
) -> ProvenanceTrace:
    trace = ProvenanceTrace(
        model_id=model_id,
        cell_path=cell_path,
        step_id=step_id,
        steps=steps,
        raw_evidence=raw_evidence or [],
        total_tokens=total_tokens,
        total_latency_ms=total_latency_ms,
    )
    db.add(trace)
    await db.flush()
    return trace


async def emit_feedback(
    db: AsyncSession,
    *,
    user_id: uuid.UUID,
    event_type: str,
    model_id: uuid.UUID | None = None,
    cell_id: uuid.UUID | None = None,
    recipe_id: uuid.UUID | None = None,
    industry: str | None = None,
    cell_path: str | None = None,
    payload: dict[str, Any] | None = None,
) -> UserFeedbackEvent:
    evt = UserFeedbackEvent(
        user_id=user_id,
        event_type=event_type,
        model_id=model_id,
        cell_id=cell_id,
        recipe_id=recipe_id,
        industry=industry,
        cell_path=cell_path,
        payload=payload or {},
    )
    db.add(evt)
    await db.flush()
    return evt


async def update_model_counts(db: AsyncSession, model_id: uuid.UUID) -> None:
    """Refresh denormalized counts on RevenueModel."""
    cells = await get_cells(db, model_id)
    flagged = sum(1 for c in cells if c.review_status == "flagged")
    mod = await db.get(RevenueModel, model_id)
    if mod:
        mod.cell_count = len(cells)
        mod.flagged_count = flagged
        mod.updated_at = datetime.now(timezone.utc)
    await db.flush()
