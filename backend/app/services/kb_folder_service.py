"""Knowledge-base folder tree service.

Layered over the :class:`KbFolder` SQLAlchemy model and MongoDB-backed
documents from ``user_kb_service``. The folder tree lives in Postgres
because it's a classic relational hierarchy (parent/child, per-user scoping,
uniqueness across siblings); documents stay in Mongo and carry a
``folder_id`` pointer.

Two scopes coexist:

* ``personal`` — only the owner can read or write. Folders have ``user_id``.
* ``public``   — readable by everyone, writable by admin/boss. ``user_id``
                 is NULL for public folders.
"""
from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from typing import Any

from sqlalchemy import and_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.app.models.kb_folder import KbFolder
from backend.app.models.user import User

logger = logging.getLogger(__name__)


# ── Constants ────────────────────────────────────────────────────

SCOPE_PERSONAL = "personal"
SCOPE_PUBLIC = "public"
_VALID_SCOPES = {SCOPE_PERSONAL, SCOPE_PUBLIC}

FOLDER_STOCK = "stock"
FOLDER_INDUSTRY = "industry"
FOLDER_GENERAL = "general"
_VALID_TYPES = {FOLDER_STOCK, FOLDER_INDUSTRY, FOLDER_GENERAL}

_WRITE_ROLES = {"admin", "boss"}
_MAX_DEPTH = 6  # prevent pathological nesting; VS Code-like UX rarely exceeds 3


# ── Errors ───────────────────────────────────────────────────────

class FolderError(Exception):
    """Base class for folder-related business errors."""


class FolderNotFound(FolderError):
    pass


class FolderPermissionDenied(FolderError):
    pass


class FolderValidationError(FolderError):
    pass


# ── Helpers ──────────────────────────────────────────────────────

def _assert_scope(scope: str) -> None:
    if scope not in _VALID_SCOPES:
        raise FolderValidationError(
            f"scope must be one of {sorted(_VALID_SCOPES)}"
        )


def _assert_type(folder_type: str) -> None:
    if folder_type not in _VALID_TYPES:
        raise FolderValidationError(
            f"folder_type must be one of {sorted(_VALID_TYPES)}"
        )


def _can_write_public(user: User) -> bool:
    return getattr(user, "role", "") in _WRITE_ROLES


def serialize(folder: KbFolder) -> dict:
    """Convert a KbFolder to a JSON-safe dict."""
    return {
        "id": str(folder.id),
        "user_id": str(folder.user_id) if folder.user_id else None,
        "scope": folder.scope,
        "parent_id": str(folder.parent_id) if folder.parent_id else None,
        "name": folder.name,
        "folder_type": folder.folder_type,
        "stock_ticker": folder.stock_ticker,
        "stock_market": folder.stock_market,
        "stock_name": folder.stock_name,
        "order_index": folder.order_index,
        "created_at": folder.created_at.isoformat() if folder.created_at else "",
        "updated_at": folder.updated_at.isoformat() if folder.updated_at else "",
    }


def _normalize_name(name: str | None) -> str:
    name = (name or "").strip()
    if not name:
        raise FolderValidationError("folder name is required")
    if len(name) > 255:
        raise FolderValidationError("folder name too long (max 255)")
    # Reserve a few characters that break filesystem-style UX.
    if any(c in name for c in ("/", "\\", "\n", "\r", "\t")):
        raise FolderValidationError("folder name contains invalid characters")
    return name


# ── Scope-aware queries ──────────────────────────────────────────


def _scope_filter(user: User, scope: str) -> Any:
    """Build a WHERE clause matching folders visible to this user in the scope."""
    if scope == SCOPE_PUBLIC:
        return and_(KbFolder.scope == SCOPE_PUBLIC, KbFolder.user_id.is_(None))
    return and_(KbFolder.scope == SCOPE_PERSONAL, KbFolder.user_id == user.id)


async def _get_folder_or_raise(
    db: AsyncSession, folder_id: str | uuid.UUID, user: User,
) -> KbFolder:
    try:
        fid = uuid.UUID(str(folder_id))
    except ValueError:
        raise FolderNotFound(f"invalid folder id: {folder_id}")
    folder = await db.scalar(select(KbFolder).where(KbFolder.id == fid))
    if folder is None:
        raise FolderNotFound("folder not found")
    # Scope check.
    if folder.scope == SCOPE_PUBLIC:
        pass  # readable by anyone
    elif folder.user_id != user.id:
        raise FolderPermissionDenied("not your folder")
    return folder


def assert_can_write(user: User, scope: str) -> None:
    """Raise if ``user`` cannot create/modify/delete folders in this scope."""
    _assert_scope(scope)
    if scope == SCOPE_PUBLIC and not _can_write_public(user):
        raise FolderPermissionDenied(
            "only admin/boss can modify the public knowledge base"
        )


async def can_access_folder(db: AsyncSession, folder_id: str, user: User) -> KbFolder:
    """Return the folder iff the user can read it; raise otherwise."""
    return await _get_folder_or_raise(db, folder_id, user)


# ── List / tree ──────────────────────────────────────────────────


async def list_folders(
    db: AsyncSession, user: User, scope: str,
) -> list[KbFolder]:
    """All folders in this scope visible to the user (flat, ordered)."""
    _assert_scope(scope)
    stmt = (
        select(KbFolder)
        .where(_scope_filter(user, scope))
        .order_by(
            KbFolder.parent_id.is_(None).desc(),  # roots first (NULL parent)
            KbFolder.order_index.asc(),
            KbFolder.created_at.asc(),
        )
    )
    result = await db.execute(stmt)
    return list(result.scalars().all())


@dataclass
class FolderTreeNode:
    folder: dict
    children: list["FolderTreeNode"]
    # document list is filled in the API layer by combining folder_id lookups.


def build_tree(folders: list[KbFolder]) -> list[dict]:
    """Turn a flat folder list into a nested tree of JSON-safe dicts."""
    by_id: dict[str, dict] = {}
    roots: list[dict] = []
    for f in folders:
        node = serialize(f)
        node["children"] = []
        by_id[node["id"]] = node
    for node in by_id.values():
        pid = node["parent_id"]
        if pid and pid in by_id:
            by_id[pid]["children"].append(node)
        else:
            roots.append(node)
    return roots


# ── Create ───────────────────────────────────────────────────────


async def create_folder(
    db: AsyncSession,
    user: User,
    *,
    scope: str,
    name: str,
    folder_type: str,
    parent_id: str | None = None,
    stock_ticker: str | None = None,
    stock_market: str | None = None,
    stock_name: str | None = None,
) -> KbFolder:
    """Create a folder; returns the persisted row."""
    _assert_scope(scope)
    _assert_type(folder_type)
    assert_can_write(user, scope)
    name = _normalize_name(name)

    parent_uuid: uuid.UUID | None = None
    if parent_id:
        parent = await _get_folder_or_raise(db, parent_id, user)
        if parent.scope != scope:
            raise FolderValidationError(
                "parent folder is in a different scope"
            )
        # Public folders can only have public parents, etc. (same-scope check).
        # Also enforce a depth cap.
        depth = await _folder_depth(db, parent)
        if depth + 1 >= _MAX_DEPTH:
            raise FolderValidationError(
                f"folder tree too deep (max {_MAX_DEPTH})"
            )
        parent_uuid = parent.id

    # Stock-type folders need a ticker binding; other types must not have one.
    ticker = (stock_ticker or "").strip() or None
    market = (stock_market or "").strip() or None
    sname = (stock_name or "").strip() or None
    if folder_type == FOLDER_STOCK:
        if not ticker:
            raise FolderValidationError(
                "stock folders require a stock_ticker"
            )
    else:
        if ticker or market or sname:
            raise FolderValidationError(
                "only stock folders can bind a ticker"
            )

    # Sibling name collision check (case-insensitive).
    dup = await db.scalar(
        select(KbFolder).where(
            _scope_filter(user, scope),
            KbFolder.parent_id.is_(None) if parent_uuid is None
                else KbFolder.parent_id == parent_uuid,
            KbFolder.name.ilike(name),
        )
    )
    if dup is not None:
        raise FolderValidationError(
            f"a folder named '{name}' already exists at this level"
        )

    # Next order_index among siblings
    max_order = await db.scalar(
        select(_max_order_index_for_siblings(user, scope, parent_uuid))
    )
    order_index = (max_order or 0) + 1

    folder = KbFolder(
        user_id=user.id if scope == SCOPE_PERSONAL else None,
        scope=scope,
        parent_id=parent_uuid,
        name=name,
        folder_type=folder_type,
        stock_ticker=ticker,
        stock_market=market,
        stock_name=sname,
        order_index=order_index,
    )
    db.add(folder)
    await db.flush()
    await db.refresh(folder)
    return folder


def _max_order_index_for_siblings(
    user: User, scope: str, parent_uuid: uuid.UUID | None,
):
    from sqlalchemy import func
    stmt = select(func.max(KbFolder.order_index)).where(
        _scope_filter(user, scope),
    )
    if parent_uuid is None:
        stmt = stmt.where(KbFolder.parent_id.is_(None))
    else:
        stmt = stmt.where(KbFolder.parent_id == parent_uuid)
    return stmt.scalar_subquery()


async def _folder_depth(db: AsyncSession, folder: KbFolder) -> int:
    """Return 0 for a root folder, 1 for a child of a root, etc."""
    depth = 0
    current = folder
    while current.parent_id is not None and depth < _MAX_DEPTH + 2:
        parent = await db.scalar(
            select(KbFolder).where(KbFolder.id == current.parent_id)
        )
        if parent is None:
            break
        depth += 1
        current = parent
    return depth


# ── Rename / move / update ───────────────────────────────────────


async def update_folder(
    db: AsyncSession,
    user: User,
    folder_id: str,
    *,
    name: str | None = None,
    parent_id: str | None = ...,  # sentinel — "..." means "don't touch"
    order_index: int | None = None,
) -> KbFolder:
    folder = await _get_folder_or_raise(db, folder_id, user)
    assert_can_write(user, folder.scope)

    if name is not None:
        new_name = _normalize_name(name)
        if new_name != folder.name:
            # Sibling collision check at current parent.
            dup = await db.scalar(
                select(KbFolder).where(
                    _scope_filter(user, folder.scope),
                    KbFolder.parent_id.is_(None) if folder.parent_id is None
                        else KbFolder.parent_id == folder.parent_id,
                    KbFolder.id != folder.id,
                    KbFolder.name.ilike(new_name),
                )
            )
            if dup is not None:
                raise FolderValidationError(
                    f"a folder named '{new_name}' already exists at this level"
                )
            folder.name = new_name

    if parent_id is not ...:
        new_parent_uuid: uuid.UUID | None = None
        if parent_id:
            new_parent = await _get_folder_or_raise(db, parent_id, user)
            if new_parent.scope != folder.scope:
                raise FolderValidationError(
                    "cannot move folder across scopes"
                )
            # Prevent cycles: can't move a folder into one of its descendants.
            if await _is_descendant(db, new_parent, folder.id):
                raise FolderValidationError(
                    "cannot move folder into its own descendant"
                )
            depth = await _folder_depth(db, new_parent)
            if depth + 1 >= _MAX_DEPTH:
                raise FolderValidationError(
                    f"folder tree too deep (max {_MAX_DEPTH})"
                )
            new_parent_uuid = new_parent.id
        folder.parent_id = new_parent_uuid

    if order_index is not None:
        folder.order_index = max(0, int(order_index))

    await db.flush()
    await db.refresh(folder)
    return folder


async def _is_descendant(
    db: AsyncSession, candidate: KbFolder, ancestor_id: uuid.UUID,
) -> bool:
    """Return True if ``candidate`` is a descendant of ``ancestor_id``."""
    cur: KbFolder | None = candidate
    seen: set[uuid.UUID] = set()
    while cur is not None:
        if cur.id == ancestor_id:
            return True
        if cur.id in seen:  # guard against pathological bad data
            return False
        seen.add(cur.id)
        if cur.parent_id is None:
            return False
        cur = await db.scalar(
            select(KbFolder).where(KbFolder.id == cur.parent_id)
        )
    return False


# ── Delete ───────────────────────────────────────────────────────


async def collect_descendant_ids(
    db: AsyncSession, folder: KbFolder,
) -> list[uuid.UUID]:
    """Return [folder.id, ...all descendants], breadth-first."""
    ids: list[uuid.UUID] = [folder.id]
    frontier: list[uuid.UUID] = [folder.id]
    while frontier:
        stmt = select(KbFolder.id).where(KbFolder.parent_id.in_(frontier))
        rows = [r[0] for r in (await db.execute(stmt)).all()]
        if not rows:
            break
        ids.extend(rows)
        frontier = rows
    return ids


async def delete_folder(
    db: AsyncSession, user: User, folder_id: str,
) -> list[uuid.UUID]:
    """Delete a folder and all descendants (Postgres CASCADE handles FKs).

    Returns the list of deleted folder IDs so callers (API layer) can also
    delete the corresponding MongoDB documents.
    """
    folder = await _get_folder_or_raise(db, folder_id, user)
    assert_can_write(user, folder.scope)
    ids = await collect_descendant_ids(db, folder)
    await db.delete(folder)  # CASCADE via parent_id FK removes the rest
    await db.flush()
    return ids
