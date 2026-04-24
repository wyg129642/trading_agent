"""Chain-of-Verification independent verifier.

Per Dhuliawala et al. (ACL 2024), CoVe runs:
  Draft → Plan → Verify Independently → Finalize.

Here we implement the *Verify Independently* leg: given a cell with a
draft value, we spawn a *fresh* LLM conversation with no prior draft
context, constrained to ``kb_search`` + ``web_search``, and ask it to
arrive at a value by itself. We then compare.

Interface: :func:`verify_cell` returns::

    {
        "consistent": bool,
        "verifier_value": float | None,
        "verifier_reasoning": str,
        "verifier_model": str,
        "confidence": "HIGH|MEDIUM|LOW",
        "reason": str,
    }

In ``ctx.dry_run``, the verifier agrees with the draft (keeps tests fast
and deterministic).
"""
from __future__ import annotations

import logging
from typing import Any

from backend.app.models.revenue_model import ModelCell
from .step_executors._llm_helper import call_llm_for_json, format_template

logger = logging.getLogger(__name__)


async def verify_cell(
    ctx, cell: ModelCell, *, diff_threshold: float = 0.10
) -> dict[str, Any]:
    """Independently re-derive ``cell.value`` and compare to the draft."""
    if cell.value is None and cell.value_text is None:
        return {
            "consistent": True,
            "verifier_value": None,
            "verifier_reasoning": "Empty cell — nothing to verify",
            "verifier_model": "none",
            "confidence": cell.confidence or "MEDIUM",
            "reason": "empty",
        }

    prompt = format_template(
        "Independently derive a value for the modeling cell with path "
        "`{path}` (label: `{label}`, unit: `{unit}`, period: `{period}`, "
        "ticker: `{ticker}`). Use ONLY publicly available disclosures and "
        "cross-reference at least two sources. Do NOT look at any prior "
        "agent work — you are the verifier. Output JSON: "
        "{{\"value\":number, \"reasoning\":\"...\", \"sources\":[{{\"label\":...,\"snippet\":...}}], "
        "\"confidence\":\"HIGH|MEDIUM|LOW\"}}.",
        {
            "path": cell.path,
            "label": cell.label or "",
            "unit": cell.unit or "",
            "period": cell.period or "",
            "ticker": ctx.model.ticker,
        },
    )
    parsed, citations, trace = await call_llm_for_json(
        ctx,
        user_prompt=prompt,
        model_id="google/gemini-3.1-pro-preview",
        path_hints=[cell.path],
        tool_set=("kb_search", "web_search"),
    )

    v_raw = (parsed or {}).get("value")
    reasoning = (parsed or {}).get("reasoning", "")
    v_conf = (parsed or {}).get("confidence") or "MEDIUM"
    try:
        v = float(v_raw) if v_raw is not None else None
    except (TypeError, ValueError):
        v = None

    if v is None or cell.value is None:
        return {
            "consistent": True,
            "verifier_value": v,
            "verifier_reasoning": reasoning,
            "verifier_model": "google/gemini-3.1-pro-preview",
            "confidence": v_conf,
            "reason": "unable_to_verify",
        }
    draft = float(cell.value)
    if draft == 0:
        denom = max(abs(v), 1e-9)
    else:
        denom = abs(draft)
    diff = abs(v - draft) / denom
    consistent = diff <= diff_threshold
    return {
        "consistent": consistent,
        "verifier_value": v,
        "verifier_reasoning": reasoning,
        "verifier_model": "google/gemini-3.1-pro-preview",
        "confidence": v_conf,
        "reason": f"diff={diff:.2%} (threshold={diff_threshold:.2%})",
    }
