"""Prompt variable substitution — safer + richer than plain ``{name}``.

Supports:
  * ``{name}`` — plain substitution (from `_llm_helper.format_template`)
  * ``{{#if var}}...{{/if}}`` — conditional block
  * ``{{#each list}} {{this}} {{/each}}`` — iteration
  * Context variables automatically injected from StepContext:
      - ticker, company_name, industry, fiscal_periods, currency
      - segments (list from ModelCell ``segment.*.meta``)
      - historical_cells, guidance_cells, growth_profiles
      - peer_tickers (from pack meta)

Researcher Prompt editors can click any variable in the UI to insert it
(see RecipeCanvasEditor.tsx AVAILABLE_VARIABLES). This module is the
runtime that fills them in.
"""
from __future__ import annotations

import json
import re
from typing import Any

from sqlalchemy import select

from backend.app.models.revenue_model import ModelCell

_VAR_RE = re.compile(r"\{([A-Za-z_][A-Za-z0-9_]*)\}")
_IF_BLOCK_RE = re.compile(r"\{\{#if\s+([A-Za-z_][A-Za-z0-9_]*)\s*\}\}(.*?)\{\{/if\}\}", re.DOTALL)
_EACH_BLOCK_RE = re.compile(r"\{\{#each\s+([A-Za-z_][A-Za-z0-9_]*)\s*\}\}(.*?)\{\{/each\}\}", re.DOTALL)


async def build_variables(ctx) -> dict[str, Any]:
    """Assemble the standard variable set for a step's prompt.

    Fails silently on unavailable fields — the template can reference
    anything; missing vars stay as ``{name}`` literal to aid debugging.
    """
    vars: dict[str, Any] = {
        "ticker": ctx.model.ticker,
        "company_name": ctx.model.company_name,
        "industry": ctx.model.industry,
        "fiscal_periods": list(ctx.model.fiscal_periods or []),
        "periods": list(ctx.model.fiscal_periods or []),
        "currency": ctx.model.base_currency,
    }

    # segments
    try:
        q = select(ModelCell).where(
            ModelCell.model_id == ctx.model.id,
            ModelCell.path.like("segment.%.meta"),
        )
        metas = list((await ctx.db.execute(q)).scalars().all())
        segments: list[dict[str, Any]] = []
        for m in metas:
            slug = m.path.split(".")[1]
            segments.append({
                "slug": slug,
                "label": m.label,
                "kind": (m.extra or {}).get("kind"),
                "volume_unit": (m.extra or {}).get("volume_unit"),
                "asp_unit": (m.extra or {}).get("asp_unit"),
                "revenue_directly": (m.extra or {}).get("revenue_directly", False),
            })
        vars["segments"] = segments
        vars["segment_slugs"] = [s["slug"] for s in segments]
    except Exception:
        vars.setdefault("segments", [])
        vars.setdefault("segment_slugs", [])

    # growth profiles
    try:
        q = select(ModelCell).where(
            ModelCell.model_id == ctx.model.id,
            ModelCell.path.like("segment.%.growth_profile"),
        )
        prof = list((await ctx.db.execute(q)).scalars().all())
        vars["growth_profiles"] = {
            c.path.split(".")[1]: c.value_text for c in prof
        }
    except Exception:
        vars.setdefault("growth_profiles", {})

    # historical revenue cells
    try:
        q = select(ModelCell).where(
            ModelCell.model_id == ctx.model.id,
            ModelCell.path.like("segment.%.rev.%"),
        )
        hist = list((await ctx.db.execute(q)).scalars().all())
        vars["historical_cells"] = [
            {"path": c.path, "period": c.period, "value": c.value}
            for c in hist if c.value is not None
        ]
    except Exception:
        vars.setdefault("historical_cells", [])

    # pack peer tickers
    peers: list[str] = []
    if ctx.pack:
        peers = list((ctx.pack.meta or {}).get("ticker_patterns") or [])
    vars["peer_tickers"] = peers

    # Pack skeleton for decompose
    vars["skeleton"] = list((ctx.pack.segments_schema.get("segments", []) if ctx.pack else []))

    return vars


def render_prompt(template: str, variables: dict[str, Any]) -> str:
    if not template:
        return ""

    # Handle {{#if var}}…{{/if}}
    def _if_repl(m: re.Match) -> str:
        key = m.group(1)
        val = variables.get(key)
        if _truthy(val):
            return m.group(2)
        return ""
    rendered = _IF_BLOCK_RE.sub(_if_repl, template)

    # Handle {{#each list}}…{{this}}…{{/each}}
    def _each_repl(m: re.Match) -> str:
        key = m.group(1)
        body = m.group(2)
        val = variables.get(key) or []
        if not isinstance(val, (list, tuple)):
            return ""
        out = []
        for i, item in enumerate(val):
            b = body.replace("{this}", _stringify(item)).replace("{@index}", str(i))
            out.append(b)
        return "".join(out)
    rendered = _EACH_BLOCK_RE.sub(_each_repl, rendered)

    # Plain {name}
    def _var_repl(m: re.Match) -> str:
        key = m.group(1)
        if key in variables:
            return _stringify(variables[key])
        return m.group(0)
    rendered = _VAR_RE.sub(_var_repl, rendered)

    return rendered


def _truthy(v: Any) -> bool:
    if v is None:
        return False
    if isinstance(v, (list, tuple, dict, str)):
        return len(v) > 0
    if isinstance(v, (int, float)):
        return v != 0
    return bool(v)


def _stringify(v: Any) -> str:
    if isinstance(v, (list, dict)):
        return json.dumps(v, ensure_ascii=False)
    if v is None:
        return ""
    return str(v)
