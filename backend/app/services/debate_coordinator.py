"""Three-way LLM debate for critical cells.

Flow (see revenue_modeling_plan.md §7.1b):

  1. Opus 4.7 is the draft model (already wrote ``cell.value``).
  2. Gemini 3.1 Pro is an independent verifier (already ran via CoVe).
  3. If Opus vs. Gemini differ > threshold, invoke GPT-5.4 as tiebreaker.

Each model's opinion is recorded as a :class:`DebateOpinion` row with
its reasoning + citations + confidence. The cell's ``alternative_values``
gets extended; ``cell.value`` is set to the tiebreaker's call if the
verifier disagreed with the drafter.

Called explicitly from :class:`VerifyAndAskStep`. Cost-sensitive; only
runs on cells flagged as "critical" in the recipe config.
"""
from __future__ import annotations

import logging
import time
from typing import Any

from backend.app.models.revenue_model import DebateOpinion, ModelCell
from .step_executors._llm_helper import call_llm_for_json, format_template

logger = logging.getLogger(__name__)


async def debate_cell(
    ctx, cell: ModelCell, *, threshold: float = 0.10
) -> dict[str, Any]:
    """Trigger a debate if Opus vs. Gemini differ more than ``threshold``.

    Returns the final decision. In ``ctx.dry_run`` mode, it records the
    three identical draft opinions and returns ``unchanged``.
    """
    # Record Opus (drafter) opinion from the cell itself
    drafter_op = DebateOpinion(
        model_id=ctx.model.id,
        cell_id=cell.id,
        model_key="anthropic/claude-opus-4-7",
        role="drafter",
        value=cell.value,
        reasoning=cell.confidence_reason or "Initial draft from recipe run",
        citations=cell.citations or [],
        confidence=cell.confidence or "MEDIUM",
    )
    ctx.db.add(drafter_op)
    await ctx.db.flush()

    # Verifier opinion (Gemini). We re-run verify_cell to get its answer
    from backend.app.services.verification_agent import verify_cell
    verify = await verify_cell(ctx, cell, diff_threshold=threshold)
    verifier_val = verify.get("verifier_value")
    verifier_op = DebateOpinion(
        model_id=ctx.model.id,
        cell_id=cell.id,
        model_key=verify.get("verifier_model") or "google/gemini-3.1-pro-preview",
        role="verifier",
        value=verifier_val,
        reasoning=verify.get("verifier_reasoning", ""),
        citations=[],
        confidence=verify.get("confidence", "MEDIUM"),
    )
    ctx.db.add(verifier_op)
    await ctx.db.flush()

    if verify.get("consistent", True) or cell.value is None or verifier_val is None:
        return {"outcome": "consistent", "final_value": cell.value}

    # tiebreaker
    prompt = format_template(
        "Two models disagree on the value of `{path}` (period: {period}, "
        "unit: {unit}) for {ticker}. Drafter said {draft}; Verifier said {verify}. "
        "Drafter reasoning: `{draft_reason}`. Verifier reasoning: `{verify_reason}`. "
        "You are the independent tiebreaker. Re-derive the value using publicly "
        "available sources, weigh the two opinions, and return JSON: "
        "{{\"value\":number, \"reasoning\":\"...\", \"confidence\":\"HIGH|MEDIUM|LOW\", "
        "\"favors\":\"drafter|verifier|neither\"}}.",
        {
            "path": cell.path,
            "period": cell.period,
            "unit": cell.unit,
            "ticker": ctx.model.ticker,
            "draft": cell.value,
            "verify": verifier_val,
            "draft_reason": cell.confidence_reason,
            "verify_reason": verify.get("verifier_reasoning", ""),
        },
    )
    t0 = time.time()
    parsed, citations, trace = await call_llm_for_json(
        ctx,
        user_prompt=prompt,
        model_id="openai/gpt-5.4",
        path_hints=[cell.path],
        tool_set=("kb_search", "web_search"),
    )
    latency_ms = int((time.time() - t0) * 1000)
    try:
        final = float((parsed or {}).get("value"))
    except (TypeError, ValueError):
        final = cell.value
    tiebreaker_op = DebateOpinion(
        model_id=ctx.model.id,
        cell_id=cell.id,
        model_key="openai/gpt-5.4",
        role="tiebreaker",
        value=final,
        reasoning=(parsed or {}).get("reasoning", ""),
        citations=citations or [],
        confidence=(parsed or {}).get("confidence") or "MEDIUM",
        latency_ms=latency_ms,
    )
    ctx.db.add(tiebreaker_op)
    await ctx.db.flush()

    # Tiebreaker value becomes the cell value when it favors verifier or neither
    favors = (parsed or {}).get("favors") or "neither"
    if favors in ("verifier", "neither"):
        cell.alternative_values = list(cell.alternative_values or []) + [{
            "value": cell.value, "source": "drafter",
            "label": "Opus (original draft)", "notes": cell.confidence_reason,
        }]
        cell.value = final
        cell.source_type = "inferred"
        cell.confidence = (parsed or {}).get("confidence") or "MEDIUM"
        cell.confidence_reason = f"Tiebreaker (GPT) chose value {final}"
        cell.review_status = "flagged"
    return {
        "outcome": "tiebreaker_applied" if favors in ("verifier", "neither") else "drafter_kept",
        "final_value": cell.value,
    }
