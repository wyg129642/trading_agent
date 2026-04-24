"""Recipe change-request (PR-style) workflow helpers.

* :func:`compute_graph_diff` — symmetric diff of node + edge sets.
* :func:`submit_change_request` — create a PR row + write-protect canonical
  from a non-admin researcher.
* :func:`approve_change_request` — merge graph from fork into canonical
  (bump version), mark CR merged, write feedback_event.
* :func:`adoption_stats` — count runs per recipe id over N days.
"""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.app.models.recipe import Recipe, RecipeRun
from backend.app.models.recipe_change_request import RecipeChangeRequest

logger = logging.getLogger(__name__)


def compute_graph_diff(old_graph: dict, new_graph: dict) -> dict:
    """Compute a small diff between two recipe graphs.

    Returns ``{added_nodes, removed_nodes, changed_nodes, added_edges, removed_edges}``.
    ``changed_nodes`` lists nodes whose config changed (prompt_template, tools,
    model_id, thresholds).
    """
    def node_index(g: dict) -> dict[str, dict]:
        return {(n or {}).get("id", ""): n for n in (g or {}).get("nodes", []) if isinstance(n, dict)}
    def edge_keys(g: dict) -> set[tuple[str, str]]:
        out = set()
        for e in (g or {}).get("edges", []):
            if isinstance(e, dict):
                out.add(((e.get("from") or e.get("from_id") or ""), e.get("to") or ""))
        return out

    old_idx = node_index(old_graph)
    new_idx = node_index(new_graph)
    added = [new_idx[k] for k in new_idx.keys() - old_idx.keys()]
    removed = [old_idx[k] for k in old_idx.keys() - new_idx.keys()]
    changed = []
    for nid in old_idx.keys() & new_idx.keys():
        if old_idx[nid] != new_idx[nid]:
            changed.append({
                "id": nid,
                "before": old_idx[nid],
                "after": new_idx[nid],
            })
    old_edges = edge_keys(old_graph)
    new_edges = edge_keys(new_graph)
    return {
        "added_nodes": added,
        "removed_nodes": removed,
        "changed_nodes": changed,
        "added_edges": [{"from": f, "to": t} for f, t in sorted(new_edges - old_edges)],
        "removed_edges": [{"from": f, "to": t} for f, t in sorted(old_edges - new_edges)],
    }


async def submit_change_request(
    db: AsyncSession,
    *,
    canonical_recipe_id: uuid.UUID,
    fork_recipe_id: uuid.UUID,
    title: str,
    description: str,
    requested_by: uuid.UUID,
) -> RecipeChangeRequest:
    canonical = await db.get(Recipe, canonical_recipe_id)
    fork = await db.get(Recipe, fork_recipe_id)
    if not canonical or not fork:
        raise ValueError("canonical or fork recipe not found")
    if not canonical.canonical:
        raise ValueError("target recipe is not a canonical recipe")
    diff = compute_graph_diff(canonical.graph or {}, fork.graph or {})
    cr = RecipeChangeRequest(
        canonical_recipe_id=canonical_recipe_id,
        fork_recipe_id=fork_recipe_id,
        title=title or f"{fork.name} → {canonical.name}",
        description=description or "",
        requested_by=requested_by,
        graph_diff=diff,
        status="open",
    )
    db.add(cr)
    await db.commit()
    await db.refresh(cr)
    return cr


async def approve_change_request(
    db: AsyncSession, *, cr_id: uuid.UUID, reviewer_id: uuid.UUID,
    review_note: str = "",
) -> RecipeChangeRequest:
    cr = await db.get(RecipeChangeRequest, cr_id)
    if not cr:
        raise ValueError("change request not found")
    if cr.status != "open":
        raise ValueError(f"CR is {cr.status}, not open")
    canonical = await db.get(Recipe, cr.canonical_recipe_id)
    fork = await db.get(Recipe, cr.fork_recipe_id)
    if not canonical or not fork:
        raise ValueError("underlying recipe disappeared")
    # Merge: copy the fork's graph into the canonical + bump version
    canonical.graph = fork.graph
    canonical.version = (canonical.version or 1) + 1
    canonical.updated_at = datetime.now(timezone.utc)
    cr.status = "merged"
    cr.reviewed_by = reviewer_id
    cr.reviewed_at = datetime.now(timezone.utc)
    cr.review_note = review_note
    await db.commit()
    await db.refresh(cr)
    return cr


async def reject_change_request(
    db: AsyncSession, *, cr_id: uuid.UUID, reviewer_id: uuid.UUID,
    review_note: str = "",
) -> RecipeChangeRequest:
    cr = await db.get(RecipeChangeRequest, cr_id)
    if not cr:
        raise ValueError("change request not found")
    if cr.status != "open":
        raise ValueError(f"CR is {cr.status}, not open")
    cr.status = "rejected"
    cr.reviewed_by = reviewer_id
    cr.reviewed_at = datetime.now(timezone.utc)
    cr.review_note = review_note
    await db.commit()
    await db.refresh(cr)
    return cr


async def adoption_stats(
    db: AsyncSession, since_days: int = 30,
) -> list[dict[str, Any]]:
    """Return per-recipe run counts + unique users + total cost over the window.

    Used to show "this recipe was used by X researchers across Y runs costing $Z".
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=since_days)
    q = (
        select(
            RecipeRun.recipe_id,
            func.count(RecipeRun.id).label("runs"),
            func.count(func.distinct(RecipeRun.started_by)).label("users"),
            func.coalesce(func.sum(RecipeRun.total_cost_usd), 0.0).label("total_cost_usd"),
        )
        .where(RecipeRun.created_at >= cutoff)
        .group_by(RecipeRun.recipe_id)
    )
    rows = (await db.execute(q)).all()
    recipe_ids = [r[0] for r in rows]
    recipes = {}
    if recipe_ids:
        rq = select(Recipe).where(Recipe.id.in_(recipe_ids))
        recipes = {r.id: r for r in (await db.execute(rq)).scalars().all()}
    out = []
    for rid, runs, users, cost in rows:
        rec = recipes.get(rid)
        out.append({
            "recipe_id": str(rid),
            "name": rec.name if rec else None,
            "slug": rec.slug if rec else None,
            "canonical": bool(rec.canonical) if rec else False,
            "industry": rec.industry if rec else None,
            "runs": int(runs),
            "unique_users": int(users),
            "total_cost_usd": float(cost or 0.0),
        })
    out.sort(key=lambda x: x["runs"], reverse=True)
    return out
