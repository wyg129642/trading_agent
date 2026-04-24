"""Step — GROWTH_DECOMPOSITION.

For each segment with volume + asp cells, decompose revenue_growth into
``volume_growth + price_growth + mix_effect`` so margin_cascade can decide
the right EBIT conversion rate (涨价主导 = 高转化; 量增主导 = 温和转化).

Writes:

    segment.<slug>.growth_decomp.<period> = {
        "volume": 0.60,   # share of growth from volume
        "price":  0.30,
        "mix":    0.10,
    }

Also writes an aggregated ``operating_leverage.<slug>`` cell indicating
expected margin accretion per 1% revenue growth.
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


class GrowthDecompositionStep(BaseStepExecutor):
    async def run(self, ctx: StepContext) -> dict[str, Any]:
        cfg = ctx.step_config or {}

        q = select(ModelCell).where(
            ModelCell.model_id == ctx.model.id,
            ModelCell.path.like("segment.%.volume.%"),
        )
        vol_cells = list((await ctx.db.execute(q)).scalars().all())
        seg_periods: dict[str, list[str]] = {}
        for c in vol_cells:
            parts = c.path.split(".")
            if len(parts) != 4:
                continue
            seg = parts[1]
            period = parts[3]
            seg_periods.setdefault(seg, []).append(period)

        if not seg_periods:
            await ctx.emit("step_completed", {"output_paths": [], "reason": "no volume/asp segments"})
            return {"output_paths": []}

        output_paths: list[str] = []
        for seg, periods in seg_periods.items():
            periods.sort()
            prompt = format_template(
                cfg.get(
                    "prompt_template",
                    "For {ticker}'s segment `{segment}` across periods {periods}, decompose "
                    "each year's revenue growth into volume vs price vs mix. Use expert "
                    "interviews and company disclosures via the available tools. "
                    "Output JSON: {{\"decomposition\":{{\"<period>\":{{\"volume\":0-1,\"price\":0-1,\"mix\":0-1}}}},"
                    "\"operating_leverage\":number,\"confidence\":\"HIGH|MEDIUM|LOW\",\"rationale\":string}}. "
                    "Shares should sum to 1. operating_leverage = expected margin accretion "
                    "(in percentage points) per 10% revenue growth.",
                ),
                {
                    "ticker": ctx.model.ticker,
                    "segment": seg,
                    "periods": periods,
                },
            )
            await ctx.emit("step_progress", {"label": f"growth decomp for {seg}"})
            parsed, citations, trace = await call_llm_for_json(
                ctx, user_prompt=prompt, path_hints=[seg],
                tool_set=tuple(cfg.get("tools") or
                               ("alphapai_recall", "jinmen_search", "kb_search", "web_search")),
            )
            dry = ctx.dry_run or any(t.get("dry_run") for t in trace)
            trace_row = await _store.record_provenance(
                ctx.db, ctx.model.id,
                cell_path=f"segment.{seg}.growth_decomp", step_id=ctx.step_id,
                steps=trace, raw_evidence=citations,
            )

            decomp = (parsed or {}).get("decomposition") or {}
            for period, vals in decomp.items():
                if not isinstance(vals, dict):
                    continue
                path = f"segment.{seg}.growth_decomp.{period}"
                pretty = (
                    f"vol {vals.get('volume', 0):.0%} / "
                    f"price {vals.get('price', 0):.0%} / "
                    f"mix {vals.get('mix', 0):.0%}"
                )
                c = await _store.upsert_cell(
                    ctx.db, ctx.model.id,
                    path=path,
                    label=f"{seg} 增长拆解 {period}",
                    period=period,
                    value_text=pretty,
                    value_type="text",
                    source_type="expert" if citations else "inferred",
                    confidence=(parsed or {}).get("confidence") or "MEDIUM",
                    confidence_reason=(parsed or {}).get("rationale", ""),
                    citations=citations[:5],
                    provenance_trace_id=trace_row.id,
                    extra={
                        "volume_share": vals.get("volume"),
                        "price_share": vals.get("price"),
                        "mix_share": vals.get("mix"),
                        **({"dry_run": True} if dry else {}),
                    },
                )
                output_paths.append(c.path)

            op_lev = (parsed or {}).get("operating_leverage")
            if op_lev is not None:
                try:
                    op_lev_v = float(op_lev)
                except (TypeError, ValueError):
                    op_lev_v = None
                if op_lev_v is not None:
                    path = f"operating_leverage.{seg}"
                    c = await _store.upsert_cell(
                        ctx.db, ctx.model.id,
                        path=path,
                        label=f"{seg} 经营杠杆",
                        value=op_lev_v,
                        unit="pp / 10%",
                        value_type="number",
                        source_type="inferred",
                        confidence=(parsed or {}).get("confidence") or "MEDIUM",
                        confidence_reason=(parsed or {}).get("rationale", ""),
                        citations=citations[:3],
                        provenance_trace_id=trace_row.id,
                        extra={"dry_run": True} if dry else None,
                    )
                    output_paths.append(c.path)

        await ctx.emit("step_completed", {"cells_written": len(output_paths)})
        return {"output_paths": output_paths}
