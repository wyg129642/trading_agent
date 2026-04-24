"""Step 3 — CLASSIFY_GROWTH_PROFILE.

For each segment, decide: stable / declining / high_growth / new.
Writes ``segment.{slug}.growth_profile`` cells (text-typed).
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

_VALID = {"stable", "declining", "high_growth", "new"}


class ClassifyGrowthProfileStep(BaseStepExecutor):
    async def run(self, ctx: StepContext) -> dict[str, Any]:
        # Load segment meta cells
        q = (
            select(ModelCell)
            .where(
                ModelCell.model_id == ctx.model.id,
                ModelCell.path.like("segment.%.meta"),
            )
        )
        metas = list((await ctx.db.execute(q)).scalars().all())
        segments = [
            {
                "slug": m.path.split(".")[1],
                "label": m.label,
                "note": m.value_text,
                "kind": m.extra.get("kind"),
            }
            for m in metas
        ]
        if not segments:
            await ctx.emit("step_completed", {"output_paths": [], "reason": "no segments"})
            return {"output_paths": []}

        prompt = format_template(
            (ctx.step_config or {}).get(
                "prompt_template",
                "For the company {ticker} ({company_name}), classify each segment's "
                "growth profile as one of [stable, declining, high_growth, new]. "
                "Segments: {segments}. Output JSON: "
                "{{\"classifications\":[{{\"segment\":...,\"profile\":...,\"reason\":...}}]}}.",
            ),
            {
                "ticker": ctx.model.ticker,
                "company_name": ctx.model.company_name,
                "segments": segments,
            },
        )
        await ctx.emit("step_progress", {"label": "classifying growth profiles"})
        parsed, citations, trace = await call_llm_for_json(
            ctx, user_prompt=prompt, path_hints=[s["slug"] for s in segments],
            tool_set=tuple((ctx.step_config or {}).get("tools") or ("kb_search", "alphapai_recall")),
        )
        dry = ctx.dry_run or any(t.get("dry_run") for t in trace)
        classifications = (parsed or {}).get("classifications", []) or []
        # If LLM fails, infer a reasonable default from the pack hint
        if not classifications:
            for s in segments:
                hint = None
                for sch in (ctx.pack.segments_schema.get("segments", []) if ctx.pack else []):
                    if sch.get("slug") == s["slug"]:
                        hint = sch.get("growth_profile_hint")
                        break
                classifications.append({
                    "segment": s["slug"],
                    "profile": hint or "stable",
                    "reason": "Fallback default",
                })

        trace_row = await _store.record_provenance(
            ctx.db, ctx.model.id,
            cell_path=None, step_id=ctx.step_id,
            steps=trace, raw_evidence=citations,
        )
        output_paths: list[str] = []
        for c in classifications:
            slug = c.get("segment")
            profile = (c.get("profile") or "stable").lower()
            if profile not in _VALID:
                profile = "stable"
            path = f"segment.{slug}.growth_profile"
            reason = c.get("reason", "")
            cell = await _store.upsert_cell(
                ctx.db, ctx.model.id,
                path=path,
                label=f"{slug} growth profile",
                value_text=profile,
                value_type="text",
                source_type="inferred",
                confidence="MEDIUM" if citations else "LOW",
                confidence_reason=reason or "LLM classification",
                notes=reason,
                citations=citations[:3],
                provenance_trace_id=trace_row.id,
                extra={"dry_run": True} if dry else None,
            )
            output_paths.append(cell.path)

        await ctx.emit("step_completed", {"classifications": len(output_paths)})
        return {"output_paths": output_paths}
