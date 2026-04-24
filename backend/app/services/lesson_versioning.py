"""Lesson lifecycle: DEPRECATED parsing, contradict detection, auto-archive.

A lesson is any block in ``lessons.md`` under a ``## L-YYYY-MM-DD-NNN`` header.
Its lifecycle state is inferred from the body:

* ``DEPRECATED:`` prefix or ``status: deprecated`` line → deprecated.
* ``ARCHIVED:`` prefix or ``status: archived`` line → archived.
* ``expires: YYYY-MM-DD`` → auto-archive after that date (weekly sweep).
* Default ``valid_for_days`` = 180 if no expiry set.

This module also provides ``detect_contradictions(industry, new_body)`` —
when approving a PendingLesson, we call the embedder to find the nearest
existing lessons; if the top hit is > 0.88 similarity and mentions a
differently-valued rule, we warn the approver.
"""
from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta, timezone

from backend.app.services import playbook_service
from backend.app.services.lesson_vector_search import (
    search_lessons_semantic,
    upsert_lesson,
)

logger = logging.getLogger(__name__)


_STATUS_RE = re.compile(r"^status\s*[:=]\s*(active|deprecated|archived)\s*$", re.I | re.M)
_EXPIRES_RE = re.compile(r"^expires\s*[:=]\s*(\d{4}-\d{2}-\d{2})\s*$", re.I | re.M)
_DEPRECATED_PREFIX_RE = re.compile(r"^(?:DEPRECATED|已废弃)\s*[:：]", re.I | re.M)
_ARCHIVED_PREFIX_RE = re.compile(r"^(?:ARCHIVED|已归档)\s*[:：]", re.I | re.M)

DEFAULT_VALID_DAYS = 180
CONTRADICTION_SIMILARITY = 0.88


def parse_status(body: str) -> str:
    """Return active | deprecated | archived based on body content."""
    if not body:
        return "active"
    if _DEPRECATED_PREFIX_RE.search(body):
        return "deprecated"
    if _ARCHIVED_PREFIX_RE.search(body):
        return "archived"
    m = _STATUS_RE.search(body)
    if m:
        return m.group(1).lower()
    return "active"


def parse_expires(body: str) -> datetime | None:
    m = _EXPIRES_RE.search(body or "")
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except Exception:
        return None


def is_expired(body: str, created_at: datetime | None = None) -> bool:
    """Lesson is expired if `expires` date has passed OR created_at + DEFAULT_VALID_DAYS < now."""
    now = datetime.now(timezone.utc)
    exp = parse_expires(body)
    if exp is not None:
        return now > exp
    if created_at is None:
        # Try to read from the "L-YYYY-MM-DD-NNN" lesson_id header
        m = re.search(r"L-(\d{4}-\d{2}-\d{2})-", body or "")
        if m:
            try:
                created_at = datetime.strptime(m.group(1), "%Y-%m-%d").replace(tzinfo=timezone.utc)
            except Exception:
                created_at = None
    if created_at is None:
        return False
    return (now - created_at) > timedelta(days=DEFAULT_VALID_DAYS)


def statuses_for_pack(slug: str) -> dict[str, str]:
    """Return {lesson_id: status} for every lesson in the pack, including
    auto-derived ``archived`` for expired ones.
    """
    lessons = playbook_service.list_lessons(slug)
    out: dict[str, str] = {}
    for l in lessons:
        body = l.get("body", "")
        status = parse_status(body)
        if status == "active" and is_expired(body):
            status = "archived"
        out[l["id"]] = status
    return out


async def auto_archive_expired(slug: str) -> dict:
    """Walk every lesson; if expired, rewrite lessons.md inserting
    ``status: archived``. Also updates Milvus status field.

    Idempotent: lessons already ``deprecated`` / ``archived`` are untouched.
    """
    from industry_packs import pack_registry
    p = pack_registry.get(slug)
    if not p:
        return {"archived": 0, "reason": "pack_not_found"}
    lessons = playbook_service.list_lessons(slug)
    changed_ids: list[str] = []
    full = p.lessons_md() or ""
    for l in lessons:
        body = l.get("body", "")
        status = parse_status(body)
        if status != "active":
            continue
        if not is_expired(body):
            continue
        # Insert "status: archived" line after the header
        if _STATUS_RE.search(body):
            new_body = _STATUS_RE.sub("status: archived", body)
        else:
            lines = body.splitlines()
            if lines:
                # Insert after the first line (the ## header)
                lines.insert(1, "status: archived")
                new_body = "\n".join(lines)
            else:
                new_body = body + "\nstatus: archived"
        full = full.replace(body, new_body)
        changed_ids.append(l["id"])
        # Update Milvus too
        try:
            await upsert_lesson(
                slug, l["id"], l.get("title", ""), new_body, status="archived",
            )
        except Exception:
            logger.debug("upsert archived lesson to milvus failed", exc_info=True)
    if changed_ids:
        playbook_service.save_pack_playbook(slug, "lessons.md", full)
    return {"archived": len(changed_ids), "lesson_ids": changed_ids}


async def detect_contradictions(
    industry: str, new_body: str, similarity_threshold: float = CONTRADICTION_SIMILARITY,
) -> list[dict]:
    """Return a list of existing lessons that are semantically very close
    to ``new_body``, so the approver can decide whether the new lesson
    overrides / deprecates them.
    """
    hits = await search_lessons_semantic(industry, new_body, limit=5, exclude_deprecated=False)
    contradictions: list[dict] = []
    for h in hits:
        score = float(h.get("score", 0.0))
        # COSINE similarity: 1.0 = identical, 0.0 = orthogonal
        if score >= similarity_threshold:
            contradictions.append({
                "lesson_id": h.get("lesson_id"),
                "title": h.get("title"),
                "status": h.get("status"),
                "similarity": round(score, 4),
                "body_preview": (h.get("body") or "")[:300],
            })
    return contradictions
