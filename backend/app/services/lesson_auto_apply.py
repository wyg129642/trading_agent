"""Auto-apply approved lessons back onto flagged cells.

When an admin approves a PendingLesson, the lesson is appended to the
industry pack's ``lessons.md`` so future recipe runs pick it up. But
existing cells with ``review_status='flagged'`` don't benefit until a
researcher manually re-runs. This module closes that loop:

1. Parse ``applicable_path_patterns`` from the lesson body.
2. Find every cell in the industry whose path matches and is currently
   flagged.
3. Write a ``lesson_applied`` feedback event for audit.
4. Optionally enqueue a rerun_needed flag on the cell's ``extra`` blob so
   the UI can surface "this cell has a newer lesson — please rerun".

Returns a summary so the reviewer sees how many cells were touched.
"""
from __future__ import annotations

import fnmatch
import logging
import re
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.app.models.feedback import PendingLesson, UserFeedbackEvent
from backend.app.models.revenue_model import ModelCell, RevenueModel

logger = logging.getLogger(__name__)

_PATTERN_BLOCK_RE = re.compile(
    r"applicable_path_patterns?\s*[:=]\s*\[([^\]]+)\]",
    re.IGNORECASE,
)


def _extract_patterns(body: str) -> list[str]:
    if not body:
        return []
    m = _PATTERN_BLOCK_RE.search(body)
    if not m:
        return []
    raw = m.group(1)
    # Items can be quoted or bare; split on comma, strip quotes/whitespace
    out: list[str] = []
    for item in raw.split(","):
        s = item.strip().strip("'").strip('"').strip()
        if s:
            out.append(s)
    return out


def _path_matches_any(path: str, patterns: list[str]) -> bool:
    for p in patterns:
        if fnmatch.fnmatch(path, p):
            return True
    return False


async def apply_approved_lesson(
    db: AsyncSession,
    lesson: PendingLesson,
    approver_id,
    *,
    require_flagged: bool = True,
) -> dict[str, Any]:
    """Apply an approved lesson to matching cells in its industry.

    Returns: {
      'lesson_id': str, 'industry': str, 'patterns': [...],
      'matched_cells': int, 'touched_models': int,
      'events_written': int,
    }
    """
    patterns = _extract_patterns(lesson.body or "")
    if not patterns:
        logger.info(
            "Lesson %s has no applicable_path_patterns; skipping auto-apply",
            lesson.lesson_id,
        )
        return {
            "lesson_id": lesson.lesson_id,
            "industry": lesson.industry,
            "patterns": [],
            "matched_cells": 0,
            "touched_models": 0,
            "events_written": 0,
        }

    # All models in this industry
    q_models = select(RevenueModel).where(RevenueModel.industry == lesson.industry)
    models = list((await db.execute(q_models)).scalars().all())
    if not models:
        return {
            "lesson_id": lesson.lesson_id,
            "industry": lesson.industry,
            "patterns": patterns,
            "matched_cells": 0,
            "touched_models": 0,
            "events_written": 0,
        }

    model_ids = [m.id for m in models]
    cells_q = select(ModelCell).where(ModelCell.model_id.in_(model_ids))
    if require_flagged:
        cells_q = cells_q.where(ModelCell.review_status == "flagged")
    cells = list((await db.execute(cells_q)).scalars().all())

    matched = 0
    touched_model_ids: set = set()
    events = 0
    now = datetime.now(timezone.utc)
    for c in cells:
        if not _path_matches_any(c.path, patterns):
            continue
        matched += 1
        touched_model_ids.add(c.model_id)
        # Stamp a rerun-recommended hint so the UI can surface this
        extra = dict(c.extra or {})
        rerun_hints = list(extra.get("rerun_hints") or [])
        rerun_hints.append({
            "lesson_id": lesson.lesson_id,
            "approved_at": now.isoformat(),
            "reason": "approved_lesson_matches_this_cell_path",
        })
        extra["rerun_hints"] = rerun_hints[-5:]  # keep last 5 hints only
        extra["needs_rerun_from_lesson"] = True
        c.extra = extra
        # Feedback event for audit — let the consolidator see the
        # application and avoid re-distilling the same insight.
        db.add(UserFeedbackEvent(
            user_id=approver_id,
            model_id=c.model_id,
            cell_id=c.id,
            cell_path=c.path,
            industry=lesson.industry,
            event_type="lesson_applied",
            payload={
                "lesson_id": lesson.lesson_id,
                "lesson_title": lesson.title,
                "matched_pattern": next(
                    (p for p in patterns if fnmatch.fnmatch(c.path, p)), ""
                ),
            },
            consumed=False,
        ))
        events += 1

    await db.commit()
    return {
        "lesson_id": lesson.lesson_id,
        "industry": lesson.industry,
        "patterns": patterns,
        "matched_cells": matched,
        "touched_models": len(touched_model_ids),
        "events_written": events,
    }
