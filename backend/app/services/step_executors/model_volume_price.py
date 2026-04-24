"""Step 5a — MODEL_VOLUME_PRICE.

For segments classified as high_growth or new, produce volume + ASP cells
per forecast period, plus a derived revenue cell = volume × asp / 10000
(converting 万块 × 美元 → 亿美元).

The heavy lifting is: asking the LLM to pull 2+ expert quotes and
construct ``alternative_values`` so the research analyst sees multiple
sources side-by-side.
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


class ModelVolumePriceStep(BaseStepExecutor):
    async def run(self, ctx: StepContext) -> dict[str, Any]:
        cfg = ctx.step_config or {}
        applies_to = set(cfg.get("applies_to_profiles") or ["high_growth", "new"])

        # Pull growth profile cells and their matching meta cells
        q = select(ModelCell).where(
            ModelCell.model_id == ctx.model.id,
            ModelCell.path.like("segment.%.growth_profile"),
        )
        profile_cells = list((await ctx.db.execute(q)).scalars().all())
        target_segments: list[str] = []
        for pc in profile_cells:
            profile = (pc.value_text or "").lower()
            if profile in applies_to:
                target_segments.append(pc.path.split(".")[1])
        if not target_segments:
            await ctx.emit("step_completed", {"output_paths": [], "reason": "no high_growth segments"})
            return {"output_paths": []}

        # Get meta cells to know units
        meta_q = select(ModelCell).where(
            ModelCell.model_id == ctx.model.id,
            ModelCell.path.in_([f"segment.{s}.meta" for s in target_segments]),
        )
        metas = {c.path.split(".")[1]: c for c in (await ctx.db.execute(meta_q)).scalars().all()}

        output_paths: list[str] = []
        for seg in target_segments:
            m = metas.get(seg)
            revenue_directly = bool(m and m.extra.get("revenue_directly"))
            vol_unit = (m.extra.get("volume_unit") if m else None) or "万块"
            asp_unit = (m.extra.get("asp_unit") if m else None) or "美元"

            prompt = format_template(
                cfg.get(
                    "prompt_template",
                    "For {ticker}'s {segment} segment across periods {periods}: "
                    "(1) identify if company has disclosed volume/price split in earnings call or 10-K; "
                    "(2) use alphapai_recall / jinmen_search to pull ≥ 2 expert quotes on volume and ASP; "
                    "(3) Output JSON: {{\"volume\":{{\"<period>\":number,\"unit\":\"{vol_unit}\"}}, "
                    "\"asp\":{{\"<period>\":number,\"unit\":\"{asp_unit}\"}}, "
                    "\"confidence\":\"HIGH|MEDIUM|LOW\", "
                    "\"sources\":[{{\"label\":...,\"snippet\":...,\"date\":...}}], "
                    "\"volume_driver_breakdown\":\"...\", \"price_driver_breakdown\":\"...\"}}. "
                    "If this segment is only disclosed as revenue (no volume breakout), "
                    "return {{\"revenue\":{{\"<period>\":number,\"unit\":\"亿美元\"}}, ...}} instead.",
                ),
                {
                    "ticker": ctx.model.ticker,
                    "segment": seg,
                    "periods": list(ctx.model.fiscal_periods),
                    "vol_unit": vol_unit,
                    "asp_unit": asp_unit,
                },
            )
            await ctx.emit("step_progress", {"label": f"modeling {seg} volume/price"})
            parsed, citations, trace = await call_llm_for_json(
                ctx, user_prompt=prompt, path_hints=[seg],
                tool_set=tuple(cfg.get("tools") or ("alphapai_recall", "jinmen_search", "kb_search", "web_search")),
            )
            dry = ctx.dry_run or any(t.get("dry_run") for t in trace)
            trace_row = await _store.record_provenance(
                ctx.db, ctx.model.id,
                cell_path=f"segment.{seg}", step_id=ctx.step_id,
                steps=trace, raw_evidence=citations,
            )
            # Prefer real tool-tracked citations; fall back to model-asserted
            sources = (parsed or {}).get("sources", []) or []
            if citations:
                cit_list = citations[:8]
            else:
                cit_list = [
                    {"index": i + 1, "title": s.get("label", ""),
                     "snippet": s.get("snippet", ""), "date": s.get("date", "")}
                    for i, s in enumerate(sources[:5])
                ]
            conf_raw = (parsed or {}).get("confidence") or "MEDIUM"
            # Down-grade confidence when no real tool citation was captured
            if not citations:
                conf = "LOW" if conf_raw == "HIGH" else conf_raw
            else:
                conf = conf_raw
            cell_extra_common = {"dry_run": True} if dry else None

            if revenue_directly or (parsed or {}).get("revenue"):
                rev_map = (parsed or {}).get("revenue") or {}
                # Treat as direct-revenue segment
                for period in ctx.model.fiscal_periods:
                    if period not in rev_map:
                        continue
                    try:
                        rv = float(rev_map[period])
                    except (TypeError, ValueError):
                        continue
                    path = f"segment.{seg}.rev.{period}"
                    c = await _store.upsert_cell(
                        ctx.db, ctx.model.id,
                        path=path,
                        label=f"{seg} 收入 {period}",
                        period=period,
                        unit="亿美元",
                        value=rv,
                        value_type="currency",
                        source_type="expert" if (citations or sources) else "inferred",
                        confidence=conf,
                        citations=cit_list,
                        notes=(parsed or {}).get("volume_driver_breakdown", ""),
                        provenance_trace_id=trace_row.id,
                        extra=cell_extra_common,
                    )
                    output_paths.append(c.path)
                continue

            vol_map = (parsed or {}).get("volume", {}) or {}
            asp_map = (parsed or {}).get("asp", {}) or {}
            for period in ctx.model.fiscal_periods:
                if period in vol_map:
                    try:
                        v = float(vol_map[period])
                    except (TypeError, ValueError):
                        continue
                    p = f"segment.{seg}.volume.{period}"
                    c = await _store.upsert_cell(
                        ctx.db, ctx.model.id,
                        path=p,
                        label=f"{seg} 出货 {period}",
                        period=period,
                        unit=vol_unit,
                        value=v,
                        value_type="count",
                        source_type="expert" if (citations or sources) else "inferred",
                        confidence=conf,
                        citations=cit_list,
                        notes=(parsed or {}).get("volume_driver_breakdown", ""),
                        provenance_trace_id=trace_row.id,
                        extra={
                            "volume_driver": (parsed or {}).get("volume_driver_breakdown", ""),
                            **({"dry_run": True} if dry else {}),
                        },
                    )
                    output_paths.append(c.path)
                if period in asp_map:
                    try:
                        a = float(asp_map[period])
                    except (TypeError, ValueError):
                        continue
                    p = f"segment.{seg}.asp.{period}"
                    c = await _store.upsert_cell(
                        ctx.db, ctx.model.id,
                        path=p,
                        label=f"{seg} ASP {period}",
                        period=period,
                        unit=asp_unit,
                        value=a,
                        value_type="currency",
                        source_type="expert" if (citations or sources) else "inferred",
                        confidence=conf,
                        citations=cit_list,
                        notes=(parsed or {}).get("price_driver_breakdown", ""),
                        provenance_trace_id=trace_row.id,
                        extra={
                            "price_driver": (parsed or {}).get("price_driver_breakdown", ""),
                            **({"dry_run": True} if dry else {}),
                        },
                    )
                    output_paths.append(c.path)
                # Derived revenue: volume × ASP / 10000  (to 亿美元 if both in 万× + 美元)
                if period in vol_map and period in asp_map:
                    rev_path = f"segment.{seg}.rev.{period}"
                    r = await _store.upsert_cell(
                        ctx.db, ctx.model.id,
                        path=rev_path,
                        label=f"{seg} 收入 {period}",
                        period=period,
                        unit="亿美元",
                        formula=f"=segment.{seg}.volume.{period} * segment.{seg}.asp.{period} / 10000",
                        value_type="currency",
                        source_type="derived",
                        confidence=conf,
                        citations=cit_list,
                        provenance_trace_id=trace_row.id,
                        extra=cell_extra_common,
                    )
                    output_paths.append(r.path)

        await ctx.emit("step_completed", {"cells_written": len(output_paths)})
        return {"output_paths": output_paths}
