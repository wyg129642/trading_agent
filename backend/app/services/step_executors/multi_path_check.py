"""Step — MULTI_PATH_CHECK.

For each critical cell (typically segment revenue), compute up to 4
independent estimates:

  1. volume × ASP          (the primary derivation)
  2. management guidance    (from earnings calls / 10-K)
  3. peer-share implied     (peer revenue × our_share / peer_share)
  4. TAM × share            (industry TAM × our market share)

If the P90/P10 spread > 2× then flag the cell as "wide_band" —
researcher must pick or explain.

Results are stored in ``cell.alternative_values`` so the researcher can
see 4 numbers side-by-side, pick the best, and export their rationale.
"""
from __future__ import annotations

import fnmatch
import logging
import statistics
from typing import Any

from sqlalchemy import select

from backend.app.models.revenue_model import ModelCell, SanityIssue
from backend.app.services import model_cell_store as _store
from ._llm_helper import call_llm_for_json, format_template
from .base import BaseStepExecutor, StepContext

logger = logging.getLogger(__name__)


class MultiPathCheckStep(BaseStepExecutor):
    async def run(self, ctx: StepContext) -> dict[str, Any]:
        cfg = ctx.step_config or {}
        patterns = cfg.get("cell_patterns") or ["segment.*.rev.*"]
        spread_threshold = float(cfg.get("spread_threshold", 2.0))
        paths = cfg.get("paths") or ["volume_asp", "management_guidance",
                                      "peer_share_inferred", "tam_share"]

        q = select(ModelCell).where(ModelCell.model_id == ctx.model.id)
        cells = list((await ctx.db.execute(q)).scalars().all())
        targets = [c for c in cells if any(fnmatch.fnmatch(c.path, p) for p in patterns)]

        updated = 0
        flagged = 0
        output_paths: list[str] = []

        for cell in targets:
            if cell.value is None:
                continue
            # Build prompt asking the LLM to independently derive up to 4 paths
            prompt = format_template(
                cfg.get(
                    "prompt_template",
                    "You are doing a 4-path cross-verification of the modeled value at `{path}` "
                    "(ticker {ticker}, period {period}, current draft value = {draft}).\n"
                    "Independently derive the value via up to 4 paths:\n"
                    "  1. volume_asp — find volume & ASP separately from expert / earnings call sources\n"
                    "  2. management_guidance — pull the most recent management-disclosed number\n"
                    "  3. peer_share_inferred — estimate via peer's revenue × (our share / peer share)\n"
                    "  4. tam_share — industry TAM × our implied market share\n\n"
                    "Use kb_search, alphapai_recall, jinmen_search, web_search. "
                    "Output JSON:\n"
                    "{{\"paths\":[{{\"name\":\"volume_asp|management_guidance|peer_share_inferred|tam_share\","
                    "\"value\":number,\"rationale\":string,\"sources\":[{{\"title\":...,\"url\":...}}]}}],"
                    "\"aggregate\":number,\"confidence\":\"HIGH|MEDIUM|LOW\"}}.",
                ),
                {
                    "path": cell.path,
                    "ticker": ctx.model.ticker,
                    "period": cell.period,
                    "draft": cell.value,
                },
            )
            await ctx.emit("step_progress", {"label": f"multi-path for {cell.path}"})
            parsed, citations, trace = await call_llm_for_json(
                ctx, user_prompt=prompt, path_hints=[cell.path],
                tool_set=tuple(cfg.get("tools") or
                               ("kb_search", "alphapai_recall", "jinmen_search", "web_search")),
            )
            dry = ctx.dry_run or any(t.get("dry_run") for t in trace)

            trace_row = await _store.record_provenance(
                ctx.db, ctx.model.id,
                cell_path=cell.path, step_id=ctx.step_id,
                steps=trace, raw_evidence=citations,
            )

            path_vals = (parsed or {}).get("paths") or []
            numeric_vals = []
            alts: list[dict[str, Any]] = []
            for pv in path_vals:
                name = pv.get("name", "unknown")
                if name not in paths:
                    continue
                try:
                    v = float(pv.get("value"))
                except (TypeError, ValueError):
                    continue
                numeric_vals.append(v)
                alts.append({
                    "value": v,
                    "source": name,
                    "label": name,
                    "notes": pv.get("rationale", "")[:400],
                })

            if not numeric_vals:
                continue

            # Compute spread
            hi = max(numeric_vals)
            lo = min(numeric_vals)
            spread = hi / max(lo, 1e-9)
            median_val = statistics.median(numeric_vals)

            # Merge alternatives into cell
            cell.alternative_values = list(cell.alternative_values or []) + alts
            cell.provenance_trace_id = trace_row.id
            if dry:
                cell.extra = {**(cell.extra or {}), "dry_run": True}
            cell.extra = {
                **(cell.extra or {}),
                "multi_path_median": median_val,
                "multi_path_spread": spread,
                "multi_path_values": numeric_vals,
            }
            updated += 1
            output_paths.append(cell.path)

            # Flag wide-band cells
            if spread > spread_threshold:
                cell.review_status = "flagged"
                cell.confidence = "LOW"
                cell.confidence_reason = (
                    f"Multi-path spread {spread:.2f}× exceeds threshold {spread_threshold}× "
                    f"(min={lo:.4g}, max={hi:.4g}, median={median_val:.4g})"
                )
                ctx.db.add(SanityIssue(
                    model_id=ctx.model.id,
                    issue_type="multi_path_wide_band",
                    severity="warn",
                    cell_paths=[cell.path],
                    message=(
                        f"{cell.path} 4 paths disagree: min={lo:.4g} max={hi:.4g} "
                        f"(spread {spread:.2f}×). Researcher should pick."
                    ),
                    suggested_fix=(
                        "Open the cell inspector and choose which path most closely "
                        "matches your thesis. Lock the cell once decided."
                    ),
                    details={
                        "paths": [
                            {"name": pv.get("name"), "value": pv.get("value")}
                            for pv in path_vals
                        ],
                        "spread": spread,
                        "median": median_val,
                    },
                ))
                flagged += 1

        await _store.update_model_counts(ctx.db, ctx.model.id)
        await ctx.emit("step_completed", {
            "cells_checked": len(targets),
            "updated": updated,
            "flagged": flagged,
        })
        return {
            "output_paths": output_paths,
            "updated": updated,
            "flagged": flagged,
        }
