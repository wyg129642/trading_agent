"""Shared Mongo filter helpers for crawler-output consumers.

Currently only one filter — soft-delete gating for `chief_opinions`. Lives in
its own module so all consumer sites import a named constant, which makes it
greppable when we extend soft-delete to other collections later.

Soft-delete contract (see scripts/cleanup_gangtise_chief.py and
crawl/gangtise/scraper.py::dump_research reverse hook):
    deleted: True
    _deleted_at: <utc datetime>
    _deleted_reason: "empty_content" | "dup_research:<rpt_id>"

Every read path against chief_opinions MUST merge `NOT_SOFT_DELETED` into
its filter so soft-deleted docs stay invisible to UI/KB/chat.
"""

from __future__ import annotations

NOT_SOFT_DELETED = {"deleted": {"$ne": True}}


def with_visible(extra: dict | None = None) -> dict:
    """Merge `NOT_SOFT_DELETED` into an existing filter dict.

    >>> with_visible({"organization": "X"})
    {'organization': 'X', 'deleted': {'$ne': True}}
    """
    if not extra:
        return dict(NOT_SOFT_DELETED)
    return {**extra, **NOT_SOFT_DELETED}
