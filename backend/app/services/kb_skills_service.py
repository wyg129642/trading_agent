"""Knowledge-base skills service.

A *skill* is a reusable bundle of folders + markdown + workbook files. It
materializes into a target folder when installed, with whitelisted
``{{variable}}`` interpolation.

Three scopes:

* ``system``  — shipped with the app, seeded by :func:`ensure_system_skills`
                on lifespan startup. Slug-addressed, idempotent upgrades.
* ``public``  — org-wide; admin/boss write. Visible to everyone.
* ``personal``— per-user; only the owner can edit/delete.

Spec shape (JSONB):

.. code-block:: python

    {
      "folders": ["研报", "公司公告"],                 # optional subfolders to create
      "files": [                                      # markdown / workbook seeds
        {"path": "key-driver.md", "kind": "markdown",
         "content": "# {{stock_name}} 关键驱动"},
        {"path": "估值表.xlsx", "kind": "workbook",
         "template": "dcf_standard"},               # reference to a factory
        {"path": "敏感性.xlsx", "kind": "workbook",
         "workbook": {...}},                        # or an inline workbook
      ],
    }
"""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Iterable

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.app.models.kb_skill_template import KbSkillTemplate
from backend.app.models.user import User
from backend.app.services import (
    kb_folder_service as folder_svc,
    user_kb_service as svc,
    user_kb_workbook,
)

logger = logging.getLogger(__name__)


SCOPE_SYSTEM = "system"
SCOPE_PUBLIC = "public"
SCOPE_PERSONAL = "personal"
_SCOPES = (SCOPE_SYSTEM, SCOPE_PUBLIC, SCOPE_PERSONAL)


class SkillError(Exception):
    """Base for skill-service errors. API layer maps to HTTPException."""


class SkillPermissionDenied(SkillError):
    pass


class SkillNotFound(SkillError):
    pass


class SkillValidationError(SkillError):
    pass


def _now() -> datetime:
    return datetime.now(timezone.utc)


# ── Workbook factories (for `"template"` references in specs) ─────────

_WORKBOOK_FACTORIES: dict[str, Any] = {
    "dcf_standard": user_kb_workbook.default_valuation_workbook,
    "three_statements": user_kb_workbook.three_statements_workbook,
}


def _build_sensitivity() -> dict:
    wb = user_kb_workbook.default_valuation_workbook()
    wb["sheets"].append(user_kb_workbook.sensitivity_sheet())
    return wb


_WORKBOOK_FACTORIES["dcf_with_sensitivity"] = _build_sensitivity


def _build_sensitivity_only() -> dict:
    return {
        "active_sheet_id": "sheet-1",
        "sheets": [user_kb_workbook.sensitivity_sheet("sheet-1", "敏感性分析")],
    }


_WORKBOOK_FACTORIES["sensitivity_only"] = _build_sensitivity_only


def _build_peer_comparison() -> dict:
    """5-metric × 5-company peer comparison scaffold."""
    headers = ["公司", "营收(亿)", "净利润(亿)", "毛利率(%)", "PE(TTM)", "市值(亿)"]
    cells: dict[str, dict[str, Any]] = {}
    for i, h in enumerate(headers):
        col = chr(ord("A") + i)
        cells[f"{col}1"] = {"v": h}
    cells["A2"] = {"v": "{{stock_name}}"}
    for r in range(3, 8):
        cells[f"A{r}"] = {"v": f"对标 {r - 2}"}
    return {
        "active_sheet_id": "peers",
        "sheets": [{
            "id": "peers", "name": "同业对比",
            "rows": 8, "cols": 6,
            "cells": cells,
            "col_widths": [160, 80, 80, 80, 80, 80],
        }],
    }


_WORKBOOK_FACTORIES["peer_comparison"] = _build_peer_comparison


def factory(template_name: str) -> dict | None:
    """Return a deep-copied workbook from the named factory, or None."""
    fn = _WORKBOOK_FACTORIES.get(template_name)
    if fn is None:
        return None
    try:
        return fn()
    except Exception as e:
        logger.warning("skill factory %s failed: %s", template_name, e)
        return None


# ── System skill definitions (seeded idempotently at startup) ─────────


_SYSTEM_SKILLS: list[dict[str, Any]] = [
    {
        "slug": "standard_dcf",
        "name": "标准 DCF 估值",
        "description": "5 年 DCF 估值模板，含营收/毛利/净利/现金流/WACC 假设。",
        "icon": "LineChartOutlined",
        "target_types": "stock",
        "spec": {
            "files": [
                {"path": "估值表.xlsx", "kind": "workbook", "template": "dcf_standard"},
            ],
        },
    },
    {
        "slug": "three_statements",
        "name": "三张报表",
        "description": "10 年利润表 / 资产负债表 / 现金流量表骨架。",
        "icon": "TableOutlined",
        "target_types": "stock",
        "spec": {
            "files": [
                {"path": "三张报表.xlsx", "kind": "workbook", "template": "three_statements"},
            ],
        },
    },
    {
        "slug": "sensitivity",
        "name": "估值敏感性",
        "description": "WACC × g 二维敏感性表。",
        "icon": "RadarChartOutlined",
        "target_types": "stock",
        "spec": {
            "files": [
                {"path": "敏感性分析.xlsx", "kind": "workbook", "template": "sensitivity_only"},
            ],
        },
    },
    {
        "slug": "peer_comparison",
        "name": "同业对比",
        "description": "主要财务指标的同业对标模板。",
        "icon": "DeploymentUnitOutlined",
        "target_types": "stock,industry",
        "spec": {
            "files": [
                {"path": "同业对比.xlsx", "kind": "workbook", "template": "peer_comparison"},
            ],
        },
    },
    {
        "slug": "research_notes",
        "name": "研报纪要模板",
        "description": "研究纪要 markdown 模板，支持要点/风险/观点三段式。",
        "icon": "FileMarkdownOutlined",
        "target_types": "stock,industry,general",
        "spec": {
            "files": [
                {
                    "path": "研报纪要.md",
                    "kind": "markdown",
                    "content": (
                        "# {{stock_name}} 研究纪要\n\n"
                        "> 日期：{{today}}\n\n"
                        "## 要点\n- \n\n"
                        "## 风险提示\n- \n\n"
                        "## 我的观点\n> "
                    ),
                },
            ],
        },
    },
]


async def ensure_system_skills(db: AsyncSession) -> int:
    """Upsert the hard-coded system skills. Safe to call on every startup.

    Returns the count of skills inserted or updated.
    """
    touched = 0
    for defn in _SYSTEM_SKILLS:
        slug = defn["slug"]
        stmt = select(KbSkillTemplate).where(
            KbSkillTemplate.scope == SCOPE_SYSTEM,
            KbSkillTemplate.slug == slug,
        )
        existing = (await db.execute(stmt)).scalar_one_or_none()
        if existing is None:
            skill = KbSkillTemplate(
                scope=SCOPE_SYSTEM,
                owner_id=None,
                name=defn["name"],
                description=defn["description"],
                icon=defn["icon"],
                target_types=defn["target_types"],
                slug=slug,
                spec=defn["spec"],
                is_published=True,
            )
            db.add(skill)
            touched += 1
            continue
        # Upgrade: refresh everything except installs_count + created_at.
        needs_update = (
            existing.name != defn["name"]
            or existing.description != defn["description"]
            or existing.icon != defn["icon"]
            or existing.target_types != defn["target_types"]
            or existing.spec != defn["spec"]
        )
        if needs_update:
            existing.name = defn["name"]
            existing.description = defn["description"]
            existing.icon = defn["icon"]
            existing.target_types = defn["target_types"]
            existing.spec = defn["spec"]
            existing.updated_at = _now()
            touched += 1
    if touched:
        await db.commit()
    return touched


# ── Permissions ────────────────────────────────────────────────────


def _is_admin_or_boss(user: User) -> bool:
    role = getattr(user, "role", None) or ""
    return role in ("admin", "boss")


def _can_write(user: User, skill: KbSkillTemplate) -> bool:
    if skill.scope == SCOPE_SYSTEM:
        return False  # system skills are upgraded only by ensure_system_skills
    if skill.scope == SCOPE_PUBLIC:
        return _is_admin_or_boss(user)
    # personal
    return skill.owner_id is not None and str(skill.owner_id) == str(user.id)


def _visible_scopes(user: User) -> list[str]:
    """What scopes a user can see. Everyone sees system+public+their own."""
    return [SCOPE_SYSTEM, SCOPE_PUBLIC, SCOPE_PERSONAL]


# ── CRUD ───────────────────────────────────────────────────────────


async def list_skills(
    db: AsyncSession,
    user: User,
    *,
    published_only: bool = True,
) -> list[KbSkillTemplate]:
    """List skills visible to ``user`` — system + public + own personal."""
    q = select(KbSkillTemplate).where(
        # system + public = always visible; personal = only own.
        (
            (KbSkillTemplate.scope == SCOPE_SYSTEM)
            | (KbSkillTemplate.scope == SCOPE_PUBLIC)
            | (
                (KbSkillTemplate.scope == SCOPE_PERSONAL)
                & (KbSkillTemplate.owner_id == user.id)
            )
        )
    )
    if published_only:
        q = q.where(KbSkillTemplate.is_published.is_(True))
    q = q.order_by(
        # system first so the built-ins always show at the top
        KbSkillTemplate.scope.desc(),
        KbSkillTemplate.updated_at.desc(),
    )
    rows = (await db.execute(q)).scalars().all()
    return list(rows)


async def get_skill(
    db: AsyncSession, user: User, skill_id: str,
) -> KbSkillTemplate:
    try:
        sid = uuid.UUID(skill_id)
    except Exception:
        raise SkillNotFound("invalid skill id")
    skill = (await db.execute(
        select(KbSkillTemplate).where(KbSkillTemplate.id == sid),
    )).scalar_one_or_none()
    if skill is None:
        raise SkillNotFound("skill not found")
    if skill.scope == SCOPE_PERSONAL and str(skill.owner_id) != str(user.id):
        raise SkillPermissionDenied("not your skill")
    return skill


async def create_skill(
    db: AsyncSession,
    user: User,
    *,
    scope: str,
    name: str,
    description: str = "",
    icon: str = "ThunderboltOutlined",
    target_types: str = "stock,industry,general",
    spec: dict | None = None,
) -> KbSkillTemplate:
    if scope not in (SCOPE_PUBLIC, SCOPE_PERSONAL):
        raise SkillValidationError("scope must be 'public' or 'personal'")
    if scope == SCOPE_PUBLIC and not _is_admin_or_boss(user):
        raise SkillPermissionDenied(
            "only admin/boss can create public skills",
        )
    name = (name or "").strip()
    if not name:
        raise SkillValidationError("name is required")
    _validate_spec(spec or {})
    skill = KbSkillTemplate(
        scope=scope,
        owner_id=user.id if scope == SCOPE_PERSONAL else None,
        name=name[:255],
        description=(description or "")[:4000],
        icon=icon[:64] or "ThunderboltOutlined",
        target_types=(target_types or "stock,industry,general")[:128],
        spec=spec or {},
        is_published=True,
    )
    db.add(skill)
    await db.commit()
    await db.refresh(skill)
    return skill


async def update_skill(
    db: AsyncSession,
    user: User,
    skill_id: str,
    *,
    name: str | None = None,
    description: str | None = None,
    icon: str | None = None,
    target_types: str | None = None,
    spec: dict | None = None,
    is_published: bool | None = None,
) -> KbSkillTemplate:
    skill = await get_skill(db, user, skill_id)
    if not _can_write(user, skill):
        raise SkillPermissionDenied("you cannot edit this skill")
    if name is not None:
        stripped = name.strip()
        if not stripped:
            raise SkillValidationError("name cannot be empty")
        skill.name = stripped[:255]
    if description is not None:
        skill.description = description[:4000]
    if icon is not None:
        skill.icon = icon[:64] or "ThunderboltOutlined"
    if target_types is not None:
        skill.target_types = target_types[:128]
    if spec is not None:
        _validate_spec(spec)
        skill.spec = spec
    if is_published is not None:
        skill.is_published = bool(is_published)
    skill.updated_at = _now()
    await db.commit()
    await db.refresh(skill)
    return skill


async def delete_skill(
    db: AsyncSession, user: User, skill_id: str,
) -> None:
    skill = await get_skill(db, user, skill_id)
    if not _can_write(user, skill):
        raise SkillPermissionDenied("you cannot delete this skill")
    await db.delete(skill)
    await db.commit()


def _validate_spec(spec: dict) -> None:
    if not isinstance(spec, dict):
        raise SkillValidationError("spec must be an object")
    files = spec.get("files") or []
    folders = spec.get("folders") or []
    if not isinstance(files, list) or not isinstance(folders, list):
        raise SkillValidationError("spec.files / spec.folders must be lists")
    if len(files) > 20:
        raise SkillValidationError("too many files (max 20)")
    if len(folders) > 20:
        raise SkillValidationError("too many folders (max 20)")
    for f in files:
        if not isinstance(f, dict):
            raise SkillValidationError("file entries must be objects")
        path = (f.get("path") or "").strip()
        kind = (f.get("kind") or "").strip()
        if not path:
            raise SkillValidationError("file.path required")
        if kind not in ("markdown", "workbook"):
            raise SkillValidationError("file.kind must be 'markdown' or 'workbook'")
        if kind == "markdown":
            if "content" in f and not isinstance(f["content"], str):
                raise SkillValidationError("markdown file.content must be a string")
        else:  # workbook
            tpl = f.get("template")
            wb = f.get("workbook")
            if tpl is not None and not isinstance(tpl, str):
                raise SkillValidationError("workbook file.template must be a string")
            if tpl is None and not isinstance(wb, dict):
                raise SkillValidationError(
                    "workbook file must carry either 'template' or 'workbook'"
                )
            if wb is not None:
                try:
                    user_kb_workbook.validate_for_write(wb)
                except user_kb_workbook.WorkbookValidationError as e:
                    raise SkillValidationError(f"workbook payload invalid: {e}")
    for name in folders:
        if not isinstance(name, str) or not name.strip():
            raise SkillValidationError("folder names must be non-empty strings")


# ── Install ────────────────────────────────────────────────────────


async def install_skill(
    db: AsyncSession,
    user: User,
    skill_id: str,
    target_folder_id: str,
) -> dict:
    """Materialize a skill's folders + files into ``target_folder_id``.

    Returns a summary:

    .. code-block:: python

        {
          "skill_id": "...",
          "folder_id": "...",
          "created_folders": [...uuids],
          "created_documents": [...mongo ids],
          "skipped_existing": int,
        }

    Conflicts: if a file with the same filename already exists in the
    target folder, we skip it (rather than overwrite — overwriting would
    silently destroy user edits). Conflicting subfolder names are also
    skipped; existing folders are reused.
    """
    skill = await get_skill(db, user, skill_id)
    # Target folder access / write gate.
    try:
        folder = await folder_svc.can_access_folder(db, target_folder_id, user)
    except folder_svc.FolderNotFound:
        raise SkillNotFound("target folder not found")
    except folder_svc.FolderPermissionDenied as e:
        raise SkillPermissionDenied(str(e))
    if folder.scope == folder_svc.SCOPE_PUBLIC and not folder_svc._can_write_public(user):
        raise SkillPermissionDenied(
            "only admin/boss can install skills into public folders",
        )
    # target_types enforcement (advisory — we don't block, but we do log).
    allowed = {t.strip() for t in (skill.target_types or "").split(",") if t.strip()}
    if allowed and folder.folder_type not in allowed:
        raise SkillValidationError(
            f"this skill targets {sorted(allowed)}, "
            f"but folder is {folder.folder_type}",
        )

    spec = dict(skill.spec or {})
    variables = user_kb_workbook.default_variables(
        stock_name=folder.stock_name or folder.name,
        ticker=folder.stock_ticker or "",
        market=folder.stock_market or "",
        user_name=getattr(user, "username", "") or "",
    )

    created_folders: list[str] = []
    created_docs: list[str] = []
    skipped = 0

    # ── 1. Folders ────────────────────────────────────────────────
    existing_children = await folder_svc.list_folders(db, user, folder.scope)
    children_by_name: dict[str, Any] = {
        f.name: f for f in existing_children if str(f.parent_id) == str(folder.id)
    }
    folder_name_to_id: dict[str, str] = {
        n: str(f.id) for n, f in children_by_name.items()
    }
    for raw_name in spec.get("folders") or []:
        name = user_kb_workbook.interpolate(str(raw_name), variables).strip()[:255]
        if not name:
            continue
        if name in children_by_name:
            skipped += 1
            continue
        try:
            sub = await folder_svc.create_folder(
                db, user,
                scope=folder.scope,
                name=name,
                folder_type=folder_svc.FOLDER_GENERAL,
                parent_id=str(folder.id),
            )
            await db.commit()
        except folder_svc.FolderValidationError as e:
            logger.warning("install_skill: folder '%s' skipped: %s", name, e)
            skipped += 1
            continue
        created_folders.append(str(sub.id))
        folder_name_to_id[name] = str(sub.id)

    # ── 2. Files ──────────────────────────────────────────────────
    # Pre-compute existing filenames in the target folder so we can skip
    # collisions without inserting duplicates.
    existing_names = await _existing_filenames_in_folder(
        str(user.id), folder.scope, str(folder.id),
    )
    for file_entry in spec.get("files") or []:
        path = user_kb_workbook.interpolate(
            str(file_entry.get("path") or ""), variables,
        ).strip()
        if not path:
            skipped += 1
            continue
        # Path may be "subfolder/file.md" — if the first segment matches a
        # created subfolder we place the file there; otherwise it goes at
        # the target folder root.
        parent_folder_id = str(folder.id)
        filename = path
        if "/" in path:
            head, _, tail = path.partition("/")
            head = head.strip()
            tail = tail.strip()
            if head in folder_name_to_id:
                parent_folder_id = folder_name_to_id[head]
                filename = tail
            else:
                # No matching subfolder; drop the prefix rather than create
                # a parent we weren't asked to create.
                filename = tail or head
        filename = filename[:200]
        if not filename:
            skipped += 1
            continue
        # Skip duplicate filenames in the same folder.
        dedup_key = (parent_folder_id, filename)
        if dedup_key in existing_names:
            skipped += 1
            continue

        kind = file_entry.get("kind")
        title_base = filename.rsplit(".", 1)[0]
        title = user_kb_workbook.interpolate(
            str(file_entry.get("title") or title_base), variables,
        )
        try:
            if kind == "markdown":
                content = user_kb_workbook.interpolate(
                    str(file_entry.get("content") or ""), variables,
                )
                new_id = await svc.create_markdown_document(
                    user_id=str(user.id),
                    original_filename=filename,
                    title=title,
                    folder_id=parent_folder_id,
                    scope=folder.scope,
                    content_md=content,
                    description=file_entry.get("description") or "",
                )
            elif kind == "workbook":
                workbook = file_entry.get("workbook")
                if workbook is None:
                    workbook = factory(str(file_entry.get("template") or ""))
                if workbook is None:
                    logger.warning(
                        "install_skill: workbook template '%s' not found",
                        file_entry.get("template"),
                    )
                    skipped += 1
                    continue
                workbook = user_kb_workbook.interpolate_workbook(workbook, variables)
                new_id = await svc.create_spreadsheet_document(
                    user_id=str(user.id),
                    original_filename=filename,
                    title=title,
                    folder_id=parent_folder_id,
                    scope=folder.scope,
                    spreadsheet_data=workbook,
                    description=file_entry.get("description") or "",
                )
            else:
                skipped += 1
                continue
        except Exception as e:
            logger.warning("install_skill: file '%s' failed: %s", filename, e)
            skipped += 1
            continue
        created_docs.append(new_id)
        existing_names.add(dedup_key)

    # Bump install counter (best-effort; don't fail the install if it errors).
    try:
        skill.installs_count = (skill.installs_count or 0) + 1
        skill.updated_at = _now()
        await db.commit()
    except Exception:
        await db.rollback()

    return {
        "skill_id": str(skill.id),
        "folder_id": str(folder.id),
        "created_folders": created_folders,
        "created_documents": created_docs,
        "skipped_existing": skipped,
    }


async def _existing_filenames_in_folder(
    user_id: str, scope: str, folder_id: str,
) -> set[tuple[str, str]]:
    """Return a set of ``(folder_id, filename)`` tuples for conflict checks."""
    # Pull all docs in this folder + direct subfolders. For a few-dozen-doc
    # folder this is a single sub-millisecond Mongo round trip.
    match = svc._scope_match_filter(user_id, scope)
    match["folder_id"] = folder_id
    seen: set[tuple[str, str]] = set()
    cursor = svc._docs().find(
        match, {"folder_id": 1, "original_filename": 1},
    )
    async for row in cursor:
        fid = str(row.get("folder_id") or "")
        fname = str(row.get("original_filename") or "")
        if fid and fname:
            seen.add((fid, fname))
    return seen


# ── Serialization (for the API layer) ─────────────────────────────


def serialize(skill: KbSkillTemplate) -> dict[str, Any]:
    return {
        "id": str(skill.id),
        "owner_id": str(skill.owner_id) if skill.owner_id else None,
        "scope": skill.scope,
        "name": skill.name,
        "description": skill.description or "",
        "icon": skill.icon,
        "target_types": [
            t.strip() for t in (skill.target_types or "").split(",") if t.strip()
        ],
        "slug": skill.slug,
        "spec": skill.spec or {},
        "is_published": bool(skill.is_published),
        "installs_count": int(skill.installs_count or 0),
        "created_at": skill.created_at.isoformat() if skill.created_at else "",
        "updated_at": skill.updated_at.isoformat() if skill.updated_at else "",
    }
