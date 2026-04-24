"""Recipes API — CRUD, fork, import from Industry Pack, list."""
from __future__ import annotations

import json
import logging
import uuid
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.app.deps import get_current_user, get_db
from backend.app.models.recipe import Recipe
from backend.app.models.user import User
from backend.app.schemas.revenue_model import (
    RecipeCreate,
    RecipeRead,
    RecipeUpdate,
)
from industry_packs import pack_registry

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("", response_model=list[RecipeRead])
async def list_recipes(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
    industry: str | None = Query(None),
    public_only: bool = Query(False),
):
    q = select(Recipe)
    conditions = []
    if industry:
        conditions.append(Recipe.industry == industry)
    if public_only:
        conditions.append(Recipe.is_public == True)  # noqa
    else:
        # Show public + user's own
        from sqlalchemy import or_
        conditions.append(or_(Recipe.is_public == True, Recipe.created_by == user.id))  # noqa
    for c in conditions:
        q = q.where(c)
    q = q.order_by(Recipe.updated_at.desc())
    rows = list((await db.execute(q)).scalars().all())
    return [_mk_read(r) for r in rows]


@router.post("", response_model=RecipeRead)
async def create_recipe(
    body: RecipeCreate,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    # unique slug scoped to (slug, version) — we bump version if duplicate slug exists
    existing = (
        await db.execute(
            select(Recipe).where(Recipe.slug == body.slug).order_by(Recipe.version.desc()).limit(1)
        )
    ).scalar_one_or_none()
    version = (existing.version + 1) if existing else 1
    r = Recipe(
        name=body.name,
        slug=body.slug,
        industry=body.industry,
        description=body.description,
        graph={"nodes": [n.dict() for n in body.graph.nodes],
               "edges": [e.dict(by_alias=True) for e in body.graph.edges]},
        version=version,
        is_public=body.is_public and user.role in ("admin", "boss"),
        created_by=user.id,
        pack_ref=body.pack_ref,
        tags=list(body.tags),
    )
    db.add(r)
    await db.commit()
    await db.refresh(r)
    return _mk_read(r)


@router.get("/{recipe_id}", response_model=RecipeRead)
async def get_recipe(
    recipe_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    r = await db.get(Recipe, recipe_id)
    if not r:
        raise HTTPException(404)
    return _mk_read(r)


@router.patch("/{recipe_id}", response_model=RecipeRead)
async def update_recipe(
    recipe_id: uuid.UUID,
    body: RecipeUpdate,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    r = await db.get(Recipe, recipe_id)
    if not r:
        raise HTTPException(404)
    if r.created_by != user.id and user.role not in ("admin", "boss"):
        raise HTTPException(403)
    # Canonical recipes are write-protected against direct edit.
    # Researchers must fork + submit a RecipeChangeRequest.
    if r.canonical and user.role not in ("admin", "boss"):
        raise HTTPException(
            403,
            "This recipe is canonical — fork it and submit a change request.",
        )
    prev_graph = r.graph
    if body.name is not None:
        r.name = body.name
    if body.description is not None:
        r.description = body.description
    if body.graph is not None:
        r.graph = {"nodes": [n.dict() for n in body.graph.nodes],
                   "edges": [e.dict(by_alias=True) for e in body.graph.edges]}
    if body.is_public is not None:
        if body.is_public and user.role not in ("admin", "boss"):
            raise HTTPException(403, "Only admin/boss can publish")
        r.is_public = body.is_public
    if body.tags is not None:
        r.tags = list(body.tags)
    # Emit feedback for graph edits (prompt changes, tool switches)
    if body.graph is not None and prev_graph != r.graph:
        from backend.app.models.feedback import UserFeedbackEvent
        diff_summary = _diff_graph(prev_graph, r.graph)
        db.add(UserFeedbackEvent(
            user_id=user.id,
            event_type="recipe_prompt_edit",
            recipe_id=r.id,
            industry=r.industry,
            payload={"diff": diff_summary, "recipe_slug": r.slug, "version": r.version},
        ))
    await db.commit()
    return _mk_read(r)


def _diff_graph(before: dict | None, after: dict) -> dict[str, Any]:
    before = before or {}
    bn = {n.get("id"): n for n in (before.get("nodes") or [])}
    an = {n.get("id"): n for n in (after.get("nodes") or [])}
    added = [k for k in an if k not in bn]
    removed = [k for k in bn if k not in an]
    changed = []
    for k in set(an) & set(bn):
        if an[k] != bn[k]:
            changed.append({
                "id": k,
                "type": an[k].get("type"),
                "prompt_before": (bn[k].get("config") or {}).get("prompt_template", "")[:300],
                "prompt_after":  (an[k].get("config") or {}).get("prompt_template", "")[:300],
                "tools_before": (bn[k].get("config") or {}).get("tools"),
                "tools_after":  (an[k].get("config") or {}).get("tools"),
            })
    return {"added": added, "removed": removed, "changed": changed}


@router.post("/{recipe_id}/fork", response_model=RecipeRead)
async def fork_recipe(
    recipe_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    src = await db.get(Recipe, recipe_id)
    if not src:
        raise HTTPException(404)
    fork = Recipe(
        name=f"{src.name} (fork)",
        slug=f"{src.slug}_fork_{uuid.uuid4().hex[:6]}",
        industry=src.industry,
        description=src.description,
        graph=src.graph,
        version=1,
        is_public=False,
        parent_recipe_id=src.id,
        created_by=user.id,
        pack_ref=src.pack_ref,
        tags=list(src.tags or []),
    )
    db.add(fork)
    await db.commit()
    await db.refresh(fork)
    return _mk_read(fork)


@router.delete("/{recipe_id}")
async def delete_recipe(
    recipe_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    r = await db.get(Recipe, recipe_id)
    if not r:
        raise HTTPException(404)
    if r.created_by != user.id and user.role not in ("admin", "boss"):
        raise HTTPException(403)
    await db.delete(r)
    await db.commit()
    return {"ok": True}


@router.post("/import-pack/{slug}")
async def import_pack_recipes(
    slug: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Seed all recipes from an Industry Pack into the Recipe DB (admin/boss)."""
    if user.role not in ("admin", "boss"):
        raise HTTPException(403)
    pack = pack_registry.get(slug)
    if not pack:
        raise HTTPException(404, f"Pack {slug} not found")
    imported: list[dict[str, Any]] = []
    for rname, rdata in pack.recipes.items():
        pack_ref = f"{slug}:{rname}"
        existing = (
            await db.execute(
                select(Recipe).where(Recipe.pack_ref == pack_ref).order_by(Recipe.version.desc()).limit(1)
            )
        ).scalar_one_or_none()
        version = (existing.version + 1) if existing else 1
        r = Recipe(
            name=rdata.get("name") or rname,
            slug=rdata.get("slug") or rname,
            industry=rdata.get("industry") or slug,
            description=rdata.get("description", ""),
            graph={"nodes": rdata.get("nodes", []), "edges": rdata.get("edges", [])},
            version=version,
            is_public=True,
            created_by=user.id,
            pack_ref=pack_ref,
            tags=list(rdata.get("tags") or []),
        )
        db.add(r)
        imported.append({"slug": r.slug, "name": r.name, "version": r.version})
    await db.commit()
    return {"imported": imported}


def _mk_read(r: Recipe) -> RecipeRead:
    return RecipeRead(
        id=str(r.id),
        name=r.name,
        slug=r.slug,
        industry=r.industry,
        description=r.description,
        graph=r.graph or {},
        version=r.version,
        is_public=r.is_public,
        parent_recipe_id=str(r.parent_recipe_id) if r.parent_recipe_id else None,
        created_by=str(r.created_by) if r.created_by else None,
        pack_ref=r.pack_ref,
        tags=list(r.tags or []),
        canonical=bool(getattr(r, "canonical", False)),
        created_at=r.created_at,
        updated_at=r.updated_at,
    )


# ── Canonical flag + Change-request PR flow ──────────────────────


class CanonicalToggle(BaseModel):
    canonical: bool


@router.patch("/{recipe_id}/canonical", response_model=RecipeRead)
async def set_canonical_flag(
    recipe_id: uuid.UUID,
    body: CanonicalToggle,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Admin/boss only: mark a recipe as canonical (team-official)."""
    if user.role not in ("admin", "boss"):
        raise HTTPException(403)
    r = await db.get(Recipe, recipe_id)
    if not r:
        raise HTTPException(404)
    r.canonical = bool(body.canonical)
    if r.canonical:
        r.is_public = True  # canonical implies public
    await db.commit()
    return _mk_read(r)


class ChangeRequestSubmit(BaseModel):
    canonical_recipe_id: str
    fork_recipe_id: str
    title: str = ""
    description: str = ""


@router.post("/change-requests")
async def submit_cr(
    body: ChangeRequestSubmit,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    from backend.app.services.recipe_pr_service import submit_change_request
    try:
        cr = await submit_change_request(
            db,
            canonical_recipe_id=uuid.UUID(body.canonical_recipe_id),
            fork_recipe_id=uuid.UUID(body.fork_recipe_id),
            title=body.title,
            description=body.description,
            requested_by=user.id,
        )
    except ValueError as e:
        raise HTTPException(400, str(e))
    return _cr_read(cr)


@router.get("/change-requests")
async def list_crs(
    status: str | None = None,
    canonical_recipe_id: str | None = None,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    from backend.app.models.recipe_change_request import RecipeChangeRequest
    q = select(RecipeChangeRequest).order_by(RecipeChangeRequest.created_at.desc())
    if status:
        q = q.where(RecipeChangeRequest.status == status)
    if canonical_recipe_id:
        q = q.where(RecipeChangeRequest.canonical_recipe_id == uuid.UUID(canonical_recipe_id))
    rows = list((await db.execute(q)).scalars().all())
    return [_cr_read(cr) for cr in rows]


@router.get("/change-requests/{cr_id}")
async def get_cr(
    cr_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    from backend.app.models.recipe_change_request import RecipeChangeRequest
    cr = await db.get(RecipeChangeRequest, cr_id)
    if not cr:
        raise HTTPException(404)
    return _cr_read(cr)


class CrReview(BaseModel):
    action: str  # approve | reject | withdraw
    note: str = ""


@router.post("/change-requests/{cr_id}/review")
async def review_cr(
    cr_id: uuid.UUID,
    body: CrReview,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    from backend.app.services.recipe_pr_service import (
        approve_change_request, reject_change_request,
    )
    from backend.app.models.recipe_change_request import RecipeChangeRequest
    if body.action == "withdraw":
        cr = await db.get(RecipeChangeRequest, cr_id)
        if not cr:
            raise HTTPException(404)
        if cr.requested_by != user.id and user.role not in ("admin", "boss"):
            raise HTTPException(403)
        if cr.status != "open":
            raise HTTPException(400, f"CR is {cr.status}")
        cr.status = "withdrawn"
        cr.review_note = body.note
        await db.commit()
        return _cr_read(cr)
    if user.role not in ("admin", "boss"):
        raise HTTPException(403, "approve/reject is admin-only")
    try:
        if body.action == "approve":
            cr = await approve_change_request(
                db, cr_id=cr_id, reviewer_id=user.id, review_note=body.note,
            )
        elif body.action == "reject":
            cr = await reject_change_request(
                db, cr_id=cr_id, reviewer_id=user.id, review_note=body.note,
            )
        else:
            raise HTTPException(400, "action must be approve|reject|withdraw")
    except ValueError as e:
        raise HTTPException(400, str(e))
    return _cr_read(cr)


@router.get("/adoption-stats")
async def adoption_stats(
    since_days: int = 30,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    from backend.app.services.recipe_pr_service import adoption_stats as _stats
    if user.role not in ("admin", "boss"):
        raise HTTPException(403)
    return await _stats(db, since_days=since_days)


def _cr_read(cr) -> dict:
    return {
        "id": str(cr.id),
        "canonical_recipe_id": str(cr.canonical_recipe_id),
        "fork_recipe_id": str(cr.fork_recipe_id),
        "title": cr.title,
        "description": cr.description,
        "requested_by": str(cr.requested_by) if cr.requested_by else None,
        "status": cr.status,
        "graph_diff": cr.graph_diff,
        "review_note": cr.review_note,
        "reviewed_by": str(cr.reviewed_by) if cr.reviewed_by else None,
        "reviewed_at": cr.reviewed_at.isoformat() if cr.reviewed_at else None,
        "created_at": cr.created_at.isoformat(),
    }
