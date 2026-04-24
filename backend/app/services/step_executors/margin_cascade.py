"""Step 6 — MARGIN_CASCADE.

Assembles the final operating-income → net-income → EPS → PE cascade.

Writes per-period cells::

    total_revenue.<period>      (formula = SUM of all segment.*.rev.<period>)
    operating_margin.<period>   (value, guidance-sourced or peer-comp)
    ebit.<period>               (formula = total_revenue * operating_margin)
    tax_rate.<period>           (value)
    ni.<period>                 (formula = ebit * (1 - tax_rate))
    shares.<period>             (value)
    eps.<period>                (formula = ni / shares * 10000)  # 亿美元/亿股 → USD/share × 10^4 conversion
    pe.<period>                 (formula = price / eps)

Notes on units: we keep revenue in 亿美元 and shares in 亿股 like the
Excel reference. EPS in USD/share requires a × 10,000 factor (since
1 亿美元 / 1 亿股 = 1 USD/share). Model.extra may carry a
``price_per_share`` override; otherwise PE is left as None.
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


class MarginCascadeStep(BaseStepExecutor):
    async def run(self, ctx: StepContext) -> dict[str, Any]:
        cfg = ctx.step_config or {}

        # 1) enumerate segment revenue paths per period
        q = select(ModelCell).where(
            ModelCell.model_id == ctx.model.id,
            ModelCell.path.like("segment.%.rev.%"),
        )
        rev_cells = list((await ctx.db.execute(q)).scalars().all())
        rev_by_period: dict[str, list[str]] = {}
        for c in rev_cells:
            p = c.period or c.path.split(".")[-1]
            rev_by_period.setdefault(p, []).append(c.path)

        # 2) Ask LLM for OM + NM + tax + shares
        periods = list(ctx.model.fiscal_periods)
        prompt = format_template(
            cfg.get(
                "prompt_template",
                "For {ticker}'s operating model in periods {periods}, produce: "
                "operating_margin per period (from management guidance if available else peer comps), "
                "tax_rate (default 15% if no guidance), shares outstanding per period. "
                "Output JSON: {{\"operating_margin\":{{\"<period>\":number}}, \"tax_rate\":number, "
                "\"shares\":{{\"<period>\":number}}, \"source\":\"guidance|peer_comp\", "
                "\"confidence\":\"HIGH|MEDIUM|LOW\"}}.",
            ),
            {"ticker": ctx.model.ticker, "periods": periods},
        )
        await ctx.emit("step_progress", {"label": "computing margin cascade"})
        parsed, citations, trace = await call_llm_for_json(
            ctx, user_prompt=prompt, path_hints=["operating_margin", "ebit", "ni"],
            tool_set=tuple(cfg.get("tools") or ("kb_search", "alphapai_recall", "jinmen_search", "consensus_forecast")),
        )
        dry = ctx.dry_run or any(t.get("dry_run") for t in trace)
        op_m = (parsed or {}).get("operating_margin") or {}
        tax = float((parsed or {}).get("tax_rate") or 0.15)
        shares = (parsed or {}).get("shares") or {}
        confidence = (parsed or {}).get("confidence") or "MEDIUM"
        source_kind = (parsed or {}).get("source") or "inferred"
        trace_row = await _store.record_provenance(
            ctx.db, ctx.model.id,
            cell_path=None, step_id=ctx.step_id,
            steps=trace, raw_evidence=citations,
        )

        output_paths: list[str] = []
        all_periods = sorted(set(list(rev_by_period.keys()) + list(periods)))

        for p in all_periods:
            # total revenue as SUM formula over segment rev cells
            seg_paths = rev_by_period.get(p, [])
            if seg_paths:
                if len(seg_paths) == 1:
                    rev_formula = f"={seg_paths[0]}"
                else:
                    rev_formula = "=" + " + ".join(seg_paths)
                cell = await _store.upsert_cell(
                    ctx.db, ctx.model.id,
                    path=f"total_revenue.{p}",
                    label=f"Total revenue {p}",
                    period=p,
                    unit="亿美元",
                    formula=rev_formula,
                    value_type="currency",
                    source_type="derived",
                    confidence="HIGH",
                    confidence_reason="Formula: sum of segment revenues",
                    provenance_trace_id=trace_row.id,
                )
                output_paths.append(cell.path)

            # operating margin (value, guidance or peer)
            om = op_m.get(p)
            try:
                om_v = float(om) if om is not None else None
            except (TypeError, ValueError):
                om_v = None
            if om_v is not None:
                cell_extra = {"dry_run": True} if dry else None
                cell = await _store.upsert_cell(
                    ctx.db, ctx.model.id,
                    path=f"operating_margin.{p}",
                    label=f"Operating margin {p}",
                    period=p,
                    unit="%",
                    value=om_v,
                    value_type="percent",
                    source_type="guidance" if source_kind == "guidance" else "inferred",
                    confidence=(
                        confidence if citations else ("LOW" if confidence == "HIGH" else confidence)
                    ),
                    citations=citations[:5],
                    notes=f"source: {source_kind}",
                    confidence_reason=f"From {source_kind}",
                    provenance_trace_id=trace_row.id,
                    extra=cell_extra,
                )
                output_paths.append(cell.path)

                # EBIT formula
                cell = await _store.upsert_cell(
                    ctx.db, ctx.model.id,
                    path=f"ebit.{p}",
                    label=f"EBIT {p}",
                    period=p,
                    unit="亿美元",
                    formula=f"=total_revenue.{p} * operating_margin.{p}",
                    value_type="currency",
                    source_type="derived",
                    confidence=confidence,
                    provenance_trace_id=trace_row.id,
                )
                output_paths.append(cell.path)

                # tax rate (same for all periods for simplicity; user can edit)
                cell = await _store.upsert_cell(
                    ctx.db, ctx.model.id,
                    path=f"tax_rate.{p}",
                    label=f"Tax rate {p}",
                    period=p,
                    unit="%",
                    value=tax,
                    value_type="percent",
                    source_type="inferred",
                    confidence="MEDIUM",
                    notes="Default 15% — override via guidance if available",
                    provenance_trace_id=trace_row.id,
                )
                output_paths.append(cell.path)

                # Net income formula
                cell = await _store.upsert_cell(
                    ctx.db, ctx.model.id,
                    path=f"ni.{p}",
                    label=f"Net income {p}",
                    period=p,
                    unit="亿美元",
                    formula=f"=ebit.{p} * (1 - tax_rate.{p})",
                    value_type="currency",
                    source_type="derived",
                    confidence=confidence,
                    provenance_trace_id=trace_row.id,
                )
                output_paths.append(cell.path)

            # shares
            sh = shares.get(p)
            try:
                sh_v = float(sh) if sh is not None else None
            except (TypeError, ValueError):
                sh_v = None
            if sh_v is not None:
                cell = await _store.upsert_cell(
                    ctx.db, ctx.model.id,
                    path=f"shares.{p}",
                    label=f"Shares outstanding {p}",
                    period=p,
                    unit="亿股",
                    value=sh_v,
                    value_type="count",
                    source_type="guidance" if source_kind == "guidance" else "inferred",
                    confidence=confidence,
                    provenance_trace_id=trace_row.id,
                )
                output_paths.append(cell.path)

                # EPS = NI / shares (both are 亿 units — ratio gives USD/share)
                cell = await _store.upsert_cell(
                    ctx.db, ctx.model.id,
                    path=f"eps.{p}",
                    label=f"EPS {p}",
                    period=p,
                    unit="USD/share",
                    formula=f"=ni.{p} / shares.{p}",
                    value_type="currency",
                    source_type="derived",
                    confidence=confidence,
                    provenance_trace_id=trace_row.id,
                )
                output_paths.append(cell.path)

                # PE if price available on the model
                # price cell lives at "market.price"; if missing, we skip.
                cell = await _store.upsert_cell(
                    ctx.db, ctx.model.id,
                    path=f"pe.{p}",
                    label=f"PE {p}",
                    period=p,
                    unit="倍",
                    formula=f"=IFERROR(market.price / eps.{p}, 0)",
                    value_type="number",
                    source_type="derived",
                    confidence="MEDIUM",
                    notes="Price fetched from market.price cell (user can set)",
                    provenance_trace_id=trace_row.id,
                )
                output_paths.append(cell.path)

        # Evaluate the full model so cascade values land
        eval_result = await _store.evaluate_formulas(ctx.db, ctx.model.id)
        await ctx.emit("step_completed", {
            "cells_written": len(output_paths),
            "evaluated": eval_result["evaluated"],
            "errors": eval_result.get("errors", [])[:5],
        })
        return {"output_paths": output_paths, "eval": eval_result}
