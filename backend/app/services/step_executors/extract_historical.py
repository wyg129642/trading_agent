"""Step 4 — EXTRACT_HISTORICAL.

Pulls 2–3 years of revenue history per segment from annual filings and
stores them as ``segment.{slug}.rev.FYxx`` cells with source_type=historical.
"""
from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import select

from backend.app.models.revenue_model import ModelCell
from backend.app.services import model_cell_store as _store
from ._llm_helper import call_llm_for_json, format_template
from .base import BaseStepExecutor, StepContext

logger = logging.getLogger(__name__)


def _infer_history_periods(current_periods: list[str]) -> list[str]:
    """Derive FYxx history labels immediately preceding the current set."""
    # Find earliest forward-looking period like FY25E or 26E
    head = None
    for p in current_periods:
        digits = "".join(c for c in p if c.isdigit())
        if digits:
            head = int(digits[-2:]) if len(digits) >= 2 else int(digits)
            break
    if head is None:
        return ["FY23", "FY24"]
    return [f"FY{(head - 2):02d}", f"FY{(head - 1):02d}"]


class ExtractHistoricalStep(BaseStepExecutor):
    async def run(self, ctx: StepContext) -> dict[str, Any]:
        cfg = ctx.step_config or {}

        # collect segment slugs
        q = (
            select(ModelCell)
            .where(
                ModelCell.model_id == ctx.model.id,
                ModelCell.path.like("segment.%.meta"),
            )
        )
        metas = list((await ctx.db.execute(q)).scalars().all())
        segments = [m.path.split(".")[1] for m in metas]
        if not segments:
            await ctx.emit("step_completed", {"output_paths": []})
            return {"output_paths": []}

        history_periods = cfg.get("history_periods") or _infer_history_periods(
            list(ctx.model.fiscal_periods)
        )
        prompt = format_template(
            cfg.get(
                "prompt_template",
                "For {ticker} ({company_name}), extract historical segment revenue "
                "for periods {history_periods} (currency: {currency}, unit: 亿美元). "
                "Segments: {segments}. Output JSON: "
                "{{\"historical\":[{{\"segment\":...,\"period\":...,\"rev\":...,\"source\":...,\"snippet\":...}}]}}.",
            ),
            {
                "ticker": ctx.model.ticker,
                "company_name": ctx.model.company_name,
                "history_periods": history_periods,
                "segments": segments,
                "currency": ctx.model.base_currency,
            },
        )
        await ctx.emit("step_progress", {"label": "extracting historical revenue"})
        parsed, citations, trace = await call_llm_for_json(
            ctx, user_prompt=prompt, path_hints=segments,
            tool_set=tuple(cfg.get("tools") or ("kb_search", "alphapai_recall")),
        )
        dry = ctx.dry_run or any(t.get("dry_run") for t in trace)
        rows = (parsed or {}).get("historical", []) or []
        trace_row = await _store.record_provenance(
            ctx.db, ctx.model.id,
            cell_path=None, step_id=ctx.step_id,
            steps=trace, raw_evidence=citations,
        )
        output_paths: list[str] = []
        for r in rows:
            seg = r.get("segment")
            period = r.get("period")
            if not seg or not period:
                continue
            try:
                rev = float(r.get("rev"))
            except (TypeError, ValueError):
                continue
            path = f"segment.{seg}.rev.{period}"
            # Prefer real tool-collected citations, fall back to LLM-asserted source
            cit: list[dict] = []
            if citations:
                cit = citations[:5]
            elif r.get("source"):
                cit = [{"index": 1, "title": r.get("source"), "snippet": r.get("snippet") or ""}]
            c = await _store.upsert_cell(
                ctx.db, ctx.model.id,
                path=path,
                label=f"{seg} 历史收入 {period}",
                period=period,
                unit="亿美元",
                value=rev,
                value_type="currency",
                source_type="historical",
                confidence=(
                    "HIGH" if (cit and r.get("source")) else "MEDIUM" if cit else "LOW"
                ),
                confidence_reason=(
                    "Sourced from " + (r.get("source") or "internal corpus")
                    if cit else "No citation captured for historical value"
                ),
                citations=cit,
                notes=r.get("snippet", ""),
                provenance_trace_id=trace_row.id,
                extra={"dry_run": True} if dry else None,
            )
            output_paths.append(c.path)

        await ctx.emit("step_completed", {"cells_written": len(output_paths), "dry_run": dry})
        return {"output_paths": output_paths, "periods": history_periods, "dry_run": dry,
                "citations": len(citations)}
