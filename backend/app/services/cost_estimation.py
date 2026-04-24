"""Cost estimation + budget / quota enforcement for revenue-modeling recipe runs.

Top trading firms treat agentic LLM cost the same way they treat execution cost:
every run must have a *pre-trade* estimate and a *hard post-trade* stop. This
module gives us:

* A small, auditable pricing table for the LLM models we actually use
  (anthropic / google / openai).
* :func:`estimate_recipe_cost` — pre-flight estimate from the recipe DAG so
  users see expected $ before they click "Run".
* :func:`cost_from_tokens` — per-step accounting from actual token usage.
* :func:`compute_monthly_spend` / :func:`check_user_quota` — per-user budget
  gate with monthly roll-over.

The pricing table is the single source of truth — keep it in sync with
chat_llm.py routing. All numbers in USD / 1M tokens.
"""
from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.app.models.recipe import RecipeRun
from backend.app.models.user import User

logger = logging.getLogger(__name__)


# ── Pricing table ────────────────────────────────────────────
# Prices in USD per 1M tokens (input, output).
# Updated 2026-04 to reflect current list prices.
PRICING_USD_PER_M: dict[str, tuple[float, float]] = {
    # Anthropic
    "anthropic/claude-opus-4-7": (15.0, 75.0),
    "anthropic/claude-opus-4-7-1m": (30.0, 150.0),
    "anthropic/claude-sonnet-4-6": (3.0, 15.0),
    "anthropic/claude-haiku-4-5-20251001": (0.80, 4.0),
    # Google
    "google/gemini-3.1-pro-preview": (1.25, 10.0),
    "google/gemini-3.1-pro": (1.25, 10.0),
    "google/gemini-3.1-flash": (0.075, 0.30),
    # OpenAI
    "openai/gpt-5.4": (5.0, 15.0),
    "openai/gpt-5.4-mini": (0.25, 2.0),
}

# Fallback when a model isn't in the table — use a conservative mid-market price
# so callers see a plausible but not misleadingly low estimate.
_FALLBACK_INPUT = 3.0
_FALLBACK_OUTPUT = 15.0


# ── Step-type estimation heuristics ──────────────────────────
# Empirical averages from ~200 recipe runs (2026-04 sample). Used only for
# pre-flight estimation. Actuals override these.
#
# Format: step_type → (avg_input_tokens, avg_output_tokens) per step invocation.
STEP_AVG_TOKENS: dict[str, tuple[int, int]] = {
    "GATHER_CONTEXT":         (12_000,  2_000),
    "DECOMPOSE_SEGMENTS":     ( 8_000,  1_500),
    "CLASSIFY_GROWTH_PROFILE":( 4_000,    800),
    "EXTRACT_HISTORICAL":     ( 8_000,  2_000),
    "MODEL_VOLUME_PRICE":     (10_000,  2_500),
    "APPLY_GUIDANCE":         ( 6_000,  1_500),
    "MARGIN_CASCADE":         ( 8_000,  2_000),
    "VERIFY_AND_ASK":         (15_000,  3_000),  # debate + verification is expensive
    "CONSENSUS_CHECK":        ( 4_000,  1_000),
    "CLASSIFY_PEERS":         ( 6_000,  1_500),
    "GROWTH_DECOMPOSITION":   ( 4_000,  1_000),
    "MULTI_PATH_CHECK":       (20_000,  4_000),  # 4 independent LLM paths
    "CHECK_MODEL":            (   500,    200),  # sanity pass is cheap
}
_FALLBACK_STEP_TOKENS = (6_000, 1_500)


def pricing_for(model_id: str) -> tuple[float, float]:
    """Return (input_usd_per_m, output_usd_per_m) for a model, with fallback."""
    if not model_id:
        return (_FALLBACK_INPUT, _FALLBACK_OUTPUT)
    if model_id in PRICING_USD_PER_M:
        return PRICING_USD_PER_M[model_id]
    # Try prefix match (e.g., "anthropic/claude-opus-4-7-v2" → claude-opus-4-7)
    for k, v in PRICING_USD_PER_M.items():
        if model_id.startswith(k):
            return v
    logger.debug("cost_estimation: unknown model %r, using fallback price", model_id)
    return (_FALLBACK_INPUT, _FALLBACK_OUTPUT)


def cost_from_tokens(
    model_id: str, input_tokens: int, output_tokens: int,
) -> float:
    """Compute USD cost from token counts. Non-negative, capped at 0 for bad input."""
    in_p, out_p = pricing_for(model_id)
    cost = (max(0, int(input_tokens)) * in_p + max(0, int(output_tokens)) * out_p) / 1_000_000
    return round(cost, 6)


def cost_from_flat_tokens(model_id: str, total_tokens: int, output_share: float = 0.25) -> float:
    """When only a total token count is available, split into in/out by output_share.

    Most of our step executors only surface ``total_tokens`` today, so we
    approximate — conservative default is 25% output (LLMs are often IO-heavy).
    """
    out_t = int(max(0, total_tokens) * output_share)
    in_t = max(0, total_tokens) - out_t
    return cost_from_tokens(model_id, in_t, out_t)


@dataclass
class RecipeCostEstimate:
    """Pre-flight cost estimate for a recipe run."""
    total_usd: float
    per_step_usd: dict[str, float]
    total_input_tokens: int
    total_output_tokens: int
    model_id: str
    step_count: int
    assumptions: list[str]


def estimate_recipe_cost(
    recipe_graph: dict,
    default_model_id: str = "anthropic/claude-opus-4-7",
    debate_roles: int = 3,
) -> RecipeCostEstimate:
    """Estimate the $ cost of running a recipe before pressing "Run".

    Multipliers applied:

    * VERIFY_AND_ASK / MULTI_PATH_CHECK get ``debate_roles`` multiplier to
      reflect the N-way LLM debate (default 3 — drafter / verifier / tiebreaker).
    """
    nodes = (recipe_graph or {}).get("nodes", []) if isinstance(recipe_graph, dict) else []
    per_step: dict[str, float] = {}
    total_in = 0
    total_out = 0
    assumptions: list[str] = []

    for node in nodes:
        if not isinstance(node, dict):
            continue
        step_type = node.get("type") or ""
        step_id = node.get("id") or step_type or "unknown"
        step_model = ((node.get("config") or {}).get("model_id")) or default_model_id
        in_t, out_t = STEP_AVG_TOKENS.get(step_type, _FALLBACK_STEP_TOKENS)
        mult = 1
        if step_type in ("VERIFY_AND_ASK", "MULTI_PATH_CHECK"):
            mult = max(1, int(debate_roles))
        in_t *= mult
        out_t *= mult
        step_cost = cost_from_tokens(step_model, in_t, out_t)
        per_step[step_id] = step_cost
        total_in += in_t
        total_out += out_t

    if not nodes:
        assumptions.append("Empty recipe graph; estimate is $0.")
    if debate_roles > 1:
        assumptions.append(
            f"VERIFY_AND_ASK / MULTI_PATH_CHECK multiplied by {debate_roles}x for debate."
        )
    assumptions.append("Fallback price $3/$15 per 1M tokens for unknown models.")

    total = round(sum(per_step.values()), 4)
    return RecipeCostEstimate(
        total_usd=total,
        per_step_usd={k: round(v, 4) for k, v in per_step.items()},
        total_input_tokens=total_in,
        total_output_tokens=total_out,
        model_id=default_model_id,
        step_count=len(nodes),
        assumptions=assumptions,
    )


# ── User quota ──────────────────────────────────────────────

# Default monthly LLM budget applied to new users until an admin overrides.
DEFAULT_MONTHLY_BUDGET_USD = 200.0


def _month_window(now: datetime | None = None) -> tuple[datetime, datetime]:
    """Return [first_of_this_month_utc, now_utc)."""
    dt = now or datetime.now(timezone.utc)
    start = dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    return start, dt


async def compute_monthly_spend(db: AsyncSession, user_id: uuid.UUID) -> float:
    """Sum up this month's completed-or-running recipe run costs for the user."""
    start, _ = _month_window()
    q = (
        select(func.coalesce(func.sum(RecipeRun.total_cost_usd), 0.0))
        .where(
            RecipeRun.started_by == user_id,
            RecipeRun.created_at >= start,
        )
    )
    row = (await db.execute(q)).scalar_one()
    return float(row or 0.0)


@dataclass
class QuotaStatus:
    user_id: uuid.UUID
    monthly_budget_usd: float
    spent_this_month_usd: float
    remaining_usd: float
    exceeded: bool
    warn_threshold_usd: float  # 80% of budget


async def check_user_quota(
    db: AsyncSession, user: User, *, estimated_add_usd: float = 0.0,
) -> QuotaStatus:
    """Return a quota status. ``exceeded=True`` if estimated run would push over."""
    budget = float(getattr(user, "llm_budget_usd_monthly", None) or DEFAULT_MONTHLY_BUDGET_USD)
    spent = await compute_monthly_spend(db, user.id)
    projected = spent + max(0.0, estimated_add_usd)
    return QuotaStatus(
        user_id=user.id,
        monthly_budget_usd=round(budget, 2),
        spent_this_month_usd=round(spent, 4),
        remaining_usd=round(max(0.0, budget - spent), 4),
        exceeded=projected > budget,
        warn_threshold_usd=round(budget * 0.8, 2),
    )


# ── Dashboard aggregations ─────────────────────────────────

async def aggregate_cost_by(
    db: AsyncSession,
    *,
    since: datetime | None = None,
    group_by: str = "industry",  # industry | user | recipe | day
) -> list[dict[str, Any]]:
    """Return cost aggregates by a dimension for the cost dashboard."""
    from backend.app.models.revenue_model import RevenueModel
    from backend.app.models.recipe import Recipe

    start = since or _month_window()[0]
    base = select(RecipeRun).where(RecipeRun.created_at >= start)
    runs = list((await db.execute(base)).scalars().all())

    by_key: dict[str, dict[str, Any]] = {}
    for r in runs:
        key = ""
        if group_by == "industry":
            m = await db.get(RevenueModel, r.model_id)
            key = m.industry if m else "(unknown)"
        elif group_by == "user":
            key = str(r.started_by) if r.started_by else "(unknown)"
        elif group_by == "recipe":
            rec = await db.get(Recipe, r.recipe_id)
            key = rec.slug if rec else "(unknown)"
        elif group_by == "day":
            key = r.created_at.date().isoformat()
        else:
            key = "all"
        bucket = by_key.setdefault(key, {
            "key": key, "runs": 0, "total_cost_usd": 0.0, "total_tokens": 0,
        })
        bucket["runs"] += 1
        bucket["total_cost_usd"] = round(bucket["total_cost_usd"] + float(r.total_cost_usd or 0.0), 4)
        bucket["total_tokens"] += int(r.total_tokens or 0)

    rows = list(by_key.values())
    rows.sort(key=lambda x: x["total_cost_usd"], reverse=True)
    return rows


def sum_runs_cost(runs: Iterable[RecipeRun]) -> float:
    return round(sum(float(r.total_cost_usd or 0.0) for r in runs), 4)
