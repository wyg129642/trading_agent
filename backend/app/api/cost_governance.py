"""Cost governance API — pre-flight estimate, quota, and cost dashboard.

Endpoints:

* ``POST /api/models/{id}/estimate-cost`` — returns a dollar estimate before
  the user presses Run. Handles industry default recipe lookup when none is
  specified.
* ``GET  /api/cost/quota`` — current user's monthly spend + budget.
* ``GET  /api/cost/dashboard`` — admin aggregated cost by industry / user /
  recipe / day for the last N days.
* ``PATCH /api/admin/users/{user_id}/budget`` — admin can raise a user's cap.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.app.api.auth import get_current_user
from backend.app.deps import get_db
from backend.app.models.recipe import Recipe, RecipeRun
from backend.app.models.revenue_model import RevenueModel
from backend.app.models.user import User
from backend.app.services.cost_estimation import (
    DEFAULT_MONTHLY_BUDGET_USD,
    aggregate_cost_by,
    check_user_quota,
    estimate_recipe_cost,
)

router = APIRouter()


class EstimateRequest(BaseModel):
    recipe_id: str | None = None
    model_id: str = "anthropic/claude-opus-4-7"
    debate_roles: int = 3


class EstimateResponse(BaseModel):
    model_id: str
    total_usd: float
    total_input_tokens: int
    total_output_tokens: int
    step_count: int
    per_step_usd: dict[str, float]
    assumptions: list[str]
    quota: dict
    recommendation: str  # "ok" | "warn" | "blocked"


@router.post("/models/{model_id}/estimate-cost", response_model=EstimateResponse)
async def estimate_cost(
    model_id: uuid.UUID,
    body: EstimateRequest,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    m = await db.get(RevenueModel, model_id)
    if not m:
        raise HTTPException(404, "Revenue model not found")

    recipe: Recipe | None = None
    if body.recipe_id:
        recipe = await db.get(Recipe, uuid.UUID(body.recipe_id))
    if recipe is None and m.recipe_id:
        recipe = await db.get(Recipe, m.recipe_id)
    if recipe is None:
        q = (
            select(Recipe)
            .where(Recipe.industry == m.industry, Recipe.is_public == True)  # noqa
            .order_by(Recipe.version.desc()).limit(1)
        )
        recipe = (await db.execute(q)).scalar_one_or_none()
    if recipe is None:
        raise HTTPException(400, f"No recipe available for industry {m.industry}")

    est = estimate_recipe_cost(
        recipe.graph or {},
        default_model_id=body.model_id,
        debate_roles=body.debate_roles,
    )
    quota = await check_user_quota(db, user, estimated_add_usd=est.total_usd)
    if quota.exceeded:
        rec = "blocked"
    elif quota.spent_this_month_usd + est.total_usd >= quota.warn_threshold_usd:
        rec = "warn"
    else:
        rec = "ok"
    return EstimateResponse(
        model_id=est.model_id,
        total_usd=est.total_usd,
        total_input_tokens=est.total_input_tokens,
        total_output_tokens=est.total_output_tokens,
        step_count=est.step_count,
        per_step_usd=est.per_step_usd,
        assumptions=est.assumptions,
        quota={
            "monthly_budget_usd": quota.monthly_budget_usd,
            "spent_this_month_usd": quota.spent_this_month_usd,
            "remaining_usd": quota.remaining_usd,
            "exceeded": quota.exceeded,
            "warn_threshold_usd": quota.warn_threshold_usd,
        },
        recommendation=rec,
    )


@router.get("/cost/quota")
async def get_my_quota(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    status = await check_user_quota(db, user)
    return {
        "user_id": str(user.id),
        "username": user.username,
        "monthly_budget_usd": status.monthly_budget_usd,
        "spent_this_month_usd": status.spent_this_month_usd,
        "remaining_usd": status.remaining_usd,
        "exceeded": status.exceeded,
        "warn_threshold_usd": status.warn_threshold_usd,
        "default_budget_usd": DEFAULT_MONTHLY_BUDGET_USD,
        "run_cap_usd": float(user.llm_run_cap_usd) if user.llm_run_cap_usd else None,
    }


@router.get("/cost/dashboard")
async def cost_dashboard(
    group_by: str = "industry",
    since_days: int = 30,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Admin-only aggregated cost view. Users see only their own slice."""
    since = datetime.now(timezone.utc) - timedelta(days=max(1, since_days))
    if user.role not in ("admin", "boss"):
        # Non-admin: return just this user's slice
        q = select(RecipeRun).where(
            RecipeRun.started_by == user.id,
            RecipeRun.created_at >= since,
        )
        runs = list((await db.execute(q)).scalars().all())
        by_recipe: dict[str, dict] = {}
        for r in runs:
            key = str(r.recipe_id)
            b = by_recipe.setdefault(key, {"key": key, "runs": 0, "total_cost_usd": 0.0, "total_tokens": 0})
            b["runs"] += 1
            b["total_cost_usd"] = round(b["total_cost_usd"] + float(r.total_cost_usd or 0.0), 4)
            b["total_tokens"] += int(r.total_tokens or 0)
        return {
            "group_by": "recipe",
            "since_days": since_days,
            "scope": "me",
            "rows": sorted(by_recipe.values(), key=lambda x: x["total_cost_usd"], reverse=True),
            "total_usd": round(sum(float(r.total_cost_usd or 0.0) for r in runs), 4),
        }

    # Admin / boss — full aggregation
    if group_by not in ("industry", "user", "recipe", "day"):
        raise HTTPException(400, "group_by must be one of: industry, user, recipe, day")
    rows = await aggregate_cost_by(db, since=since, group_by=group_by)
    total = round(sum(r["total_cost_usd"] for r in rows), 4)
    return {
        "group_by": group_by,
        "since_days": since_days,
        "scope": "all",
        "rows": rows,
        "total_usd": total,
    }


class BudgetPatch(BaseModel):
    monthly_budget_usd: float | None = None
    run_cap_usd: float | None = None


@router.patch("/admin/users/{user_id}/budget")
async def patch_user_budget(
    user_id: uuid.UUID,
    body: BudgetPatch,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    if user.role not in ("admin", "boss"):
        raise HTTPException(403, "admin only")
    u = await db.get(User, user_id)
    if not u:
        raise HTTPException(404)
    if body.monthly_budget_usd is not None:
        if body.monthly_budget_usd < 0:
            raise HTTPException(400, "monthly_budget_usd must be >= 0")
        u.llm_budget_usd_monthly = body.monthly_budget_usd
    if body.run_cap_usd is not None:
        if body.run_cap_usd < 0:
            raise HTTPException(400, "run_cap_usd must be >= 0")
        u.llm_run_cap_usd = body.run_cap_usd
    await db.commit()
    return {
        "user_id": str(u.id),
        "llm_budget_usd_monthly": float(u.llm_budget_usd_monthly) if u.llm_budget_usd_monthly else None,
        "llm_run_cap_usd": float(u.llm_run_cap_usd) if u.llm_run_cap_usd else None,
    }
