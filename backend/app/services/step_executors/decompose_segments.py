"""Step 2 — DECOMPOSE_SEGMENTS.

Uses the Industry Pack's ``segments_schema.yaml`` as a skeleton and asks
the LLM which segments actually apply to this ticker. Writes placeholder
cells (`segment.{slug}.meta` with label, unit hints, kind) so later steps
have structure to fill in.
"""
from __future__ import annotations

import logging
from typing import Any

from backend.app.services import model_cell_store as _store
from ._llm_helper import call_llm_for_json, format_template
from .base import BaseStepExecutor, StepContext

logger = logging.getLogger(__name__)


class DecomposeSegmentsStep(BaseStepExecutor):
    async def run(self, ctx: StepContext) -> dict[str, Any]:
        cfg = ctx.step_config or {}
        skeleton = (ctx.pack.segments_schema if ctx.pack else {}).get("segments", [])
        skeleton_desc = [
            {"slug": s.get("slug"), "label": s.get("label_zh"), "kind": s.get("kind")}
            for s in skeleton
        ]
        prompt = format_template(
            cfg.get(
                "prompt_template",
                "Analyze the actual disclosed business segments of {ticker} ({company_name}). "
                "Use the industry standard skeleton as REFERENCE (not constraint): {skeleton}. "
                "Output JSON: {{\"segments\":[{{\"slug\":...,\"label_zh\":...,\"kind\":\"module|chip|product\","
                "\"volume_unit\":...,\"asp_unit\":...,\"revenue_directly\":true|false,"
                "\"present_in_company\":true|false,\"note\":...}}]}}. Keep skeleton slugs when they apply.",
            ),
            {
                "ticker": ctx.model.ticker,
                "company_name": ctx.model.company_name,
                "skeleton": skeleton_desc,
            },
        )
        await ctx.emit("step_progress", {"label": "decomposing segments"})
        parsed, citations, trace = await call_llm_for_json(
            ctx, user_prompt=prompt, path_hints=["segment"],
            tool_set=tuple(cfg.get("tools") or ("kb_search", "alphapai_recall", "jinmen_search")),
        )
        dry = ctx.dry_run or any(t.get("dry_run") for t in trace)

        segs = (parsed or {}).get("segments", []) or []
        # If LLM returns empty, fall back to the pack skeleton so the run
        # is still useful (better than zero output).
        if not segs and skeleton:
            segs = [
                {
                    "slug": s.get("slug"),
                    "label_zh": s.get("label_zh"),
                    "kind": s.get("kind"),
                    "volume_unit": s.get("volume_unit"),
                    "asp_unit": s.get("asp_unit"),
                    "revenue_directly": bool(s.get("revenue_directly")),
                    "present_in_company": True,
                    "note": "Fallback from skeleton",
                }
                for s in skeleton
            ]
        trace_row = await _store.record_provenance(
            ctx.db, ctx.model.id,
            cell_path=None, step_id=ctx.step_id,
            steps=trace, raw_evidence=citations,
        )

        output_paths: list[str] = []
        for s in segs:
            if not s.get("present_in_company", True):
                continue
            slug = s.get("slug") or "unknown"
            path = f"segment.{slug}.meta"
            extra_payload = {
                "kind": s.get("kind"),
                "volume_unit": s.get("volume_unit"),
                "asp_unit": s.get("asp_unit"),
                "revenue_directly": bool(s.get("revenue_directly", False)),
            }
            if dry:
                extra_payload["dry_run"] = True
            c = await _store.upsert_cell(
                ctx.db, ctx.model.id,
                path=path,
                label=s.get("label_zh") or slug,
                unit="",
                value_text=s.get("note") or "",
                value_type="text",
                source_type="inferred",
                confidence="MEDIUM" if citations else "LOW",
                confidence_reason=(
                    "Decomposed from company disclosures + pack skeleton"
                    if citations else "No tool citations collected — structure only"
                ),
                citations=citations,
                provenance_trace_id=trace_row.id,
                extra=extra_payload,
            )
            output_paths.append(c.path)

        await ctx.emit("step_completed", {"segments": len(output_paths), "dry_run": dry})
        return {"output_paths": output_paths, "segments_found": len(output_paths), "dry_run": dry}
