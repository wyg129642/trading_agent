"""Step 5b — APPLY_GUIDANCE.

For stable / declining segments, either read a guidance cell (written by
step 1) or fall back to +3% / -2% defaults. Produces revenue cells for
every forecast period.
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


class ApplyGuidanceStep(BaseStepExecutor):
    async def run(self, ctx: StepContext) -> dict[str, Any]:
        cfg = ctx.step_config or {}
        applies_to = set(cfg.get("applies_to_profiles") or ["stable", "declining"])
        default_growth = float(cfg.get("default_growth_rate", 0.03))
        negative_default = float(cfg.get("negative_default", -0.02))

        q = select(ModelCell).where(
            ModelCell.model_id == ctx.model.id,
            ModelCell.path.like("segment.%.growth_profile"),
        )
        profile_cells = list((await ctx.db.execute(q)).scalars().all())
        target_segments: list[tuple[str, str]] = []
        for pc in profile_cells:
            profile = (pc.value_text or "").lower()
            if profile in applies_to:
                target_segments.append((pc.path.split(".")[1], profile))
        if not target_segments:
            await ctx.emit("step_completed", {"output_paths": []})
            return {"output_paths": []}

        # Read history for each segment to derive the last revenue point
        seg_slugs = [s for s, _ in target_segments]
        hist_q = select(ModelCell).where(
            ModelCell.model_id == ctx.model.id,
            ModelCell.path.like("segment.%.rev.%"),
        )
        hist_cells = list((await ctx.db.execute(hist_q)).scalars().all())
        last_rev: dict[str, tuple[str, float]] = {}
        for h in hist_cells:
            if h.value is None or h.formula:  # skip derived
                continue
            # path: segment.<slug>.rev.<period>
            parts = h.path.split(".")
            if len(parts) != 4 or parts[0] != "segment" or parts[2] != "rev":
                continue
            slug = parts[1]
            if slug not in seg_slugs:
                continue
            per = parts[3]
            prev = last_rev.get(slug)
            if prev is None or per > prev[0]:
                last_rev[slug] = (per, float(h.value))

        output_paths: list[str] = []
        periods = list(ctx.model.fiscal_periods)

        for seg, profile in target_segments:
            last = last_rev.get(seg)
            if not last:
                # Skip if we have no historical anchor — MODEL_VOLUME_PRICE may fill later
                logger.info("No history for stable segment %s; skipping", seg)
                continue
            # Get LLM guidance if available; else use default rate
            prompt = format_template(
                cfg.get(
                    "prompt_template",
                    "For {ticker}'s {segment} ({profile}) segment, summarize any "
                    "management guidance for {periods} and return JSON: "
                    "{{\"growth_rate\":number, \"confidence\":\"HIGH|MEDIUM|LOW\", "
                    "\"source\":\"guidance|historical|default\", \"notes\":\"...\"}}. "
                    "If no explicit guidance, return growth_rate={default_growth} "
                    "({negative_default} if declining).",
                ),
                {
                    "ticker": ctx.model.ticker,
                    "segment": seg,
                    "profile": profile,
                    "periods": periods,
                    "default_growth": default_growth,
                    "negative_default": negative_default,
                },
            )
            await ctx.emit("step_progress", {"label": f"guidance for {seg}"})
            parsed, citations, trace = await call_llm_for_json(
                ctx, user_prompt=prompt, path_hints=[seg],
                tool_set=tuple(cfg.get("tools") or ("kb_search", "alphapai_recall")),
            )
            dry = ctx.dry_run or any(t.get("dry_run") for t in trace)
            try:
                growth = float((parsed or {}).get("growth_rate"))
            except (TypeError, ValueError):
                growth = default_growth if profile == "stable" else negative_default
            source_kind = (parsed or {}).get("source") or "default"
            trace_row = await _store.record_provenance(
                ctx.db, ctx.model.id,
                cell_path=f"segment.{seg}", step_id=ctx.step_id,
                steps=trace, raw_evidence=citations,
            )
            # Cascade revenue from last historical
            anchor_period, anchor_rev = last
            current_rev = anchor_rev
            for p in periods:
                current_rev = current_rev * (1.0 + growth)
                path = f"segment.{seg}.rev.{p}"
                conf_raw = (parsed or {}).get("confidence") or "MEDIUM"
                final_conf = conf_raw if citations else ("LOW" if conf_raw == "HIGH" else conf_raw)
                extra_apply = {
                    "growth_rate": growth,
                    "anchor_period": anchor_period,
                    "anchor_value": anchor_rev,
                    "growth_source": source_kind,
                }
                if dry:
                    extra_apply["dry_run"] = True
                c = await _store.upsert_cell(
                    ctx.db, ctx.model.id,
                    path=path,
                    label=f"{seg} 收入 {p}",
                    period=p,
                    unit="亿美元",
                    value=float(current_rev),
                    value_type="currency",
                    source_type="guidance" if source_kind == "guidance" else "inferred",
                    confidence=final_conf,
                    notes=f"growth={growth:.2%} from {source_kind}",
                    confidence_reason=f"growth rate {growth:.2%} applied from {source_kind}",
                    citations=citations[:5],
                    provenance_trace_id=trace_row.id,
                    extra=extra_apply,
                )
                output_paths.append(c.path)

        await ctx.emit("step_completed", {"cells_written": len(output_paths)})
        return {"output_paths": output_paths}
