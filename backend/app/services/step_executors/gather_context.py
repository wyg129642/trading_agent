"""Step 1 — GATHER_CONTEXT.

Reads recent earnings calls, 10-K, investor days and writes a small set
of company-overview cells (``company.overview.revenue_fy-1``, etc.) along
with a prose ``company.overview.summary`` cell.
"""
from __future__ import annotations

import logging
from typing import Any

from backend.app.services import model_cell_store as _store
from ._llm_helper import call_llm_for_json, format_template
from .base import BaseStepExecutor, StepContext

logger = logging.getLogger(__name__)


class GatherContextStep(BaseStepExecutor):
    async def run(self, ctx: StepContext) -> dict[str, Any]:
        cfg = ctx.step_config or {}
        prompt = format_template(
            cfg.get(
                "prompt_template",
                "Summarize the most recent 180 days of earnings calls, 10-K / 10-Q and "
                "investor days for {ticker} ({company_name}). Use the tools "
                "(kb_search, alphapai_recall, jinmen_search, web_search) to pull the "
                "actual material. Produce a JSON object: "
                "{{\"summary\":..., \"key_segments\":[{{\"name\":...,\"note\":...}}], "
                "\"management_guidance\":[{{\"metric\":..., \"value\":..., \"period\":..., \"citation\":\"[N]\"}}]}}.",
            ),
            {"ticker": ctx.model.ticker, "company_name": ctx.model.company_name},
        )
        await ctx.emit("step_progress", {"label": "calling LLM for context summary"})
        parsed, citations, trace = await call_llm_for_json(
            ctx,
            user_prompt=prompt,
            path_hints=["company.overview"],
            tool_set=tuple(cfg.get("tools") or ("kb_search", "alphapai_recall", "jinmen_search")),
        )
        dry = ctx.dry_run or any(t.get("dry_run") for t in trace)
        # Record provenance
        trace_row = await _store.record_provenance(
            ctx.db, ctx.model.id,
            cell_path="company.overview", step_id=ctx.step_id,
            steps=trace,
            raw_evidence=citations,
            total_tokens=sum(t.get("tokens", 0) for t in trace),
            total_latency_ms=sum(t.get("latency", 0) for t in trace),
        )
        output_paths: list[str] = []
        summary = (parsed or {}).get("summary", "") or "[no summary available]"
        cell = await _store.upsert_cell(
            ctx.db, ctx.model.id,
            path="company.overview.summary",
            label="Company overview",
            unit="",
            value_text=summary,
            value_type="text",
            source_type="inferred",
            confidence="MEDIUM" if citations else "LOW",
            confidence_reason=(
                f"Aggregated from {len(citations)} tool-retrieved source(s)"
                if citations else "No tool citations collected"
            ),
            citations=citations[:20],
            provenance_trace_id=trace_row.id,
            extra={"dry_run": dry} if dry else None,
        )
        output_paths.append(cell.path)

        for i, g in enumerate((parsed or {}).get("management_guidance", []) or []):
            metric = g.get("metric", f"guidance_{i}")
            period = g.get("period", "")
            value = g.get("value", None)
            try:
                val_f = float(value) if value is not None else None
            except (TypeError, ValueError):
                val_f = None
            path = f"company.overview.guidance.{_slugify(metric)}.{period or 'na'}"
            c = await _store.upsert_cell(
                ctx.db, ctx.model.id,
                path=path,
                label=f"Guidance: {metric} ({period})",
                period=period,
                value=val_f,
                value_text=str(value) if val_f is None else None,
                source_type="guidance",
                confidence="MEDIUM" if citations else "LOW",
                confidence_reason="Captured from management guidance pass",
                citations=citations[:5],
                provenance_trace_id=trace_row.id,
                extra={"dry_run": dry} if dry else None,
            )
            output_paths.append(c.path)

        await ctx.emit("step_completed", {"cells_written": len(output_paths), "dry_run": dry})
        ctx.total_tokens += sum(t.get("tokens", 0) for t in trace)
        ctx.total_latency_ms += sum(t.get("latency", 0) for t in trace)
        return {"output_paths": output_paths, "summary_len": len(summary), "citations": len(citations)}


def _slugify(s: str) -> str:
    import re
    return re.sub(r"[^a-z0-9_]+", "_", (s or "unknown").lower()).strip("_") or "unknown"
