"""Playbook I/O service.

Provides:

* ``list_packs()`` — summary of available industry packs.
* ``read_pack_playbook(slug)`` — markdown contents.
* ``save_pack_playbook(slug, file, body)`` — write back markdown (admin).
* ``append_lesson(slug, lesson_body)`` — append a new lesson to
  ``lessons.md`` (idempotent on ``L-XXXX`` header).
* ``search_lessons(slug, path)`` — path-prefix BM25-ish retrieval of
  lessons (wraps :meth:`IndustryPack.playbook_snippets`).

The service is intentionally filesystem-backed — it's the simplest way
to keep the Claude-style "markdown is the source of truth" property.
Git history provides audit. Writes go through the same pack directory
the recipe engine reads from.
"""
from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any

from industry_packs import IndustryPack, pack_registry

logger = logging.getLogger(__name__)


# ── Read ────────────────────────────────────────────────────────

def list_packs() -> list[dict[str, Any]]:
    return [
        {
            "slug": p.slug,
            "name": p.name,
            "description": p.meta.get("description", ""),
            "ticker_patterns": p.meta.get("ticker_patterns", []),
            "default_periods": p.meta.get("default_periods", []),
            "recipe_count": len(p.recipes),
            "playbook_files": list(p.playbook.keys()),
        }
        for p in pack_registry.list()
    ]


def read_pack_playbook(slug: str) -> dict[str, str] | None:
    p = pack_registry.get(slug)
    if not p:
        return None
    return {"overview.md": p.overview_md(), "lessons.md": p.lessons_md(), "rules.md": p.rules_md()}


def search_lessons(
    slug: str, cell_path: str, *, max_chars: int = 1500,
    cell_context: str = "",
) -> str:
    """Retrieve lesson snippets relevant to a cell path.

    Three-layer:
      1. Pattern-based match (most specific): scan the lessons.md for
         ``applicable_path_patterns`` matching ``cell_path`` via fnmatch.
      2. BM25-style prefix / keyword match via ``playbook_snippets``.
      3. Dense-vector semantic search via Milvus (``lesson_vector_search``) —
         fail-open: Milvus down means we just skip this layer.

    ``cell_context`` is a concatenation of the cell's label, path, notes,
    unit — it's used as the query for the semantic layer so a lesson about
    "operating_margin" can match a cell labelled "营业利润率" even without
    explicit pattern tagging.
    """
    p = pack_registry.get(slug)
    if not p:
        return ""
    base = p.playbook_snippets(cell_path, max_chars=max_chars)

    # Pattern-based retrieval: find blocks that tag a pattern matching cell_path
    pattern_hits = _pattern_match_lessons(p.lessons_md() or "", cell_path, max_chars=max_chars)

    # Semantic layer — Milvus dense search. Synchronous wrapper because this
    # function is called from sync step-executor contexts; we use asyncio.run
    # only if there's no running loop (safe fallback).
    semantic_hits = ""
    try:
        semantic_hits = _safe_semantic_search(
            slug, cell_context or cell_path, max_chars=max_chars,
        )
    except Exception:
        logger.debug("semantic lesson search failed", exc_info=True)

    parts: list[str] = []
    if pattern_hits:
        parts.append(pattern_hits)
    if semantic_hits and semantic_hits.strip() not in "".join(parts):
        parts.append(semantic_hits)
    if base:
        parts.append(base)
    combined = "\n\n".join(p for p in parts if p)
    return combined[:max_chars]


def _safe_semantic_search(slug: str, query_text: str, *, max_chars: int) -> str:
    """Run the Milvus semantic search even from sync callers. Fail-open."""
    if not query_text:
        return ""
    import asyncio as _aio
    from .lesson_vector_search import search_lessons_semantic
    try:
        loop = _aio.get_running_loop()
    except RuntimeError:
        loop = None
    if loop and loop.is_running():
        # We're inside an async context — the caller needs to await us. Return
        # an empty string to avoid a cross-loop await; semantic matches then
        # depend on the async wrapper below.
        return ""
    try:
        hits = _aio.run(search_lessons_semantic(slug, query_text, limit=3))
    except Exception:
        return ""
    snippets: list[str] = []
    total = 0
    for h in hits:
        body = h.get("body") or ""
        if not body:
            continue
        seg = f"## {h.get('title') or h.get('lesson_id')}\n\n{body}"
        if total + len(seg) > max_chars:
            break
        snippets.append(seg)
        total += len(seg)
    return "\n\n".join(snippets)


async def search_lessons_async(
    slug: str, cell_path: str, *, max_chars: int = 1500,
    cell_context: str = "",
) -> str:
    """Async counterpart of :func:`search_lessons` that always runs the semantic
    layer in-process (usable from async step executors / API handlers)."""
    from .lesson_vector_search import search_lessons_semantic
    p = pack_registry.get(slug)
    if not p:
        return ""
    base = p.playbook_snippets(cell_path, max_chars=max_chars)
    pattern_hits = _pattern_match_lessons(p.lessons_md() or "", cell_path, max_chars=max_chars)

    semantic_hits = ""
    try:
        hits = await search_lessons_semantic(
            slug, cell_context or cell_path, limit=3,
        )
        segs: list[str] = []
        total = 0
        for h in hits:
            body = h.get("body") or ""
            if not body:
                continue
            seg = f"## {h.get('title') or h.get('lesson_id')}\n\n{body}"
            if total + len(seg) > max_chars:
                break
            segs.append(seg)
            total += len(seg)
        semantic_hits = "\n\n".join(segs)
    except Exception:
        logger.debug("async semantic lesson search failed", exc_info=True)

    parts = [x for x in (pattern_hits, semantic_hits, base) if x]
    combined = "\n\n".join(parts)
    return combined[:max_chars]


_PATTERN_LINE_RE = re.compile(r"applicable_path_patterns?\s*[:=]\s*\[([^\]]+)\]", re.I)


def _pattern_match_lessons(lessons_text: str, cell_path: str, max_chars: int) -> str:
    import fnmatch
    blocks = [b for b in lessons_text.split("\n\n") if b.strip()]
    hits: list[str] = []
    total = 0
    for b in blocks:
        m = _PATTERN_LINE_RE.search(b)
        if not m:
            continue
        raw = m.group(1)
        patterns = [s.strip().strip("'\"") for s in raw.split(",") if s.strip()]
        if any(fnmatch.fnmatchcase(cell_path, pat) for pat in patterns):
            if total + len(b) > max_chars:
                break
            hits.append(b)
            total += len(b)
    return "\n\n".join(hits)


# ── Write ───────────────────────────────────────────────────────

_LESSON_ID_RE = re.compile(r"^##\s+(L-\d{4}-\d{2}-\d{2}-\d+)", re.M)


def save_pack_playbook(slug: str, filename: str, body: str) -> bool:
    """Write back a playbook markdown file. Returns True on success."""
    p = pack_registry.get(slug)
    if not p:
        return False
    if filename not in {"overview.md", "lessons.md", "rules.md"}:
        return False
    path = p.root / "playbook" / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")
    # Refresh in-memory pack
    p.playbook[filename] = body
    return True


def append_lesson(slug: str, lesson_id: str, body: str) -> bool:
    """Append a lesson to lessons.md, skipping duplicates by ID.

    After writing, schedules an async Milvus upsert so the lesson is
    searchable by dense-vector next time. Best-effort: Milvus down → skip.
    """
    p = pack_registry.get(slug)
    if not p:
        return False
    cur = p.lessons_md() or ""
    ids = set(_LESSON_ID_RE.findall(cur))
    if lesson_id in ids:
        return False
    sep = "\n\n" if cur.strip() else ""
    new_content = f"{cur}{sep}{body.strip()}\n"
    ok = save_pack_playbook(slug, "lessons.md", new_content)
    if ok:
        # Fire-and-forget vector upsert (don't let Milvus lag block approve)
        try:
            import asyncio as _aio
            from .lesson_vector_search import upsert_lesson as _upsert
            # Extract title (first line after ## header)
            lines = body.strip().splitlines()
            title = lines[0].lstrip("# ").strip() if lines else lesson_id
            try:
                loop = _aio.get_running_loop()
                loop.create_task(_upsert(slug, lesson_id, title, body))
            except RuntimeError:
                _aio.run(_upsert(slug, lesson_id, title, body))
        except Exception:
            logger.debug("lesson upsert scheduling failed", exc_info=True)
    return ok


def list_lessons(slug: str) -> list[dict[str, str]]:
    p = pack_registry.get(slug)
    if not p:
        return []
    text = p.lessons_md() or ""
    # Split on top-level headers
    blocks = re.split(r"(?m)^(?=## )", text)
    lessons: list[dict[str, str]] = []
    for b in blocks:
        b = b.strip()
        if not b.startswith("##"):
            continue
        first_line, _, rest = b.partition("\n")
        title = first_line.lstrip("# ").strip()
        m = _LESSON_ID_RE.search(first_line)
        lesson_id = m.group(1) if m else title[:40]
        lessons.append({"id": lesson_id, "title": title, "body": b})
    return lessons
