"""Phase 6 tests — canonical recipe PR flow, segment snapshot, lesson
versioning, A/B winner distiller, KB snippet highlight, pack YAML editor,
lesson_auto_apply end-to-end wiring.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio
from sqlalchemy import delete, select

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture(autouse=True)
async def _dispose_engine():
    from backend.app.core import database as _db_mod
    await _db_mod.engine.dispose()
    yield
    await _db_mod.engine.dispose()


@pytest_asyncio.fixture
async def db_session():
    from backend.app.core.database import async_session_factory
    async with async_session_factory() as db:
        yield db


@pytest_asyncio.fixture
async def test_user(db_session):
    from backend.app.models.user import User
    u = User(
        email=f"phase6-{uuid.uuid4().hex[:8]}@test.local",
        username=f"ph6{uuid.uuid4().hex[:8]}",
        password_hash="$2b$12$x",
        display_name="Phase 6",
        role="user",
        is_active=True,
    )
    db_session.add(u)
    await db_session.commit()
    await db_session.refresh(u)
    yield u
    await db_session.execute(delete(User).where(User.id == u.id))
    await db_session.commit()


# ── Canonical + PR flow ────────────────────────────────────

def test_compute_graph_diff_basic():
    from backend.app.services.recipe_pr_service import compute_graph_diff
    old = {
        "nodes": [{"id": "a", "type": "GATHER"}, {"id": "b", "type": "CHECK"}],
        "edges": [{"from": "a", "to": "b"}],
    }
    new = {
        "nodes": [{"id": "a", "type": "GATHER"}, {"id": "c", "type": "NEW_STEP"}],
        "edges": [{"from": "a", "to": "c"}],
    }
    d = compute_graph_diff(old, new)
    assert {n["id"] for n in d["added_nodes"]} == {"c"}
    assert {n["id"] for n in d["removed_nodes"]} == {"b"}
    assert d["added_edges"] == [{"from": "a", "to": "c"}]
    assert d["removed_edges"] == [{"from": "a", "to": "b"}]


def test_compute_graph_diff_changed_node():
    from backend.app.services.recipe_pr_service import compute_graph_diff
    old = {"nodes": [{"id": "a", "type": "X", "config": {"prompt_template": "old"}}], "edges": []}
    new = {"nodes": [{"id": "a", "type": "X", "config": {"prompt_template": "new"}}], "edges": []}
    d = compute_graph_diff(old, new)
    assert len(d["changed_nodes"]) == 1
    assert d["changed_nodes"][0]["id"] == "a"


async def test_canonical_recipe_column_exists(db_session):
    from backend.app.models.recipe import Recipe
    r = await db_session.execute(select(Recipe.canonical).limit(1))
    _ = list(r.all())


async def test_recipe_change_request_table_exists(db_session):
    from backend.app.models.recipe_change_request import RecipeChangeRequest
    r = await db_session.execute(
        select(
            RecipeChangeRequest.id, RecipeChangeRequest.status,
            RecipeChangeRequest.graph_diff,
        ).limit(1)
    )
    _ = list(r.all())


# ── Segment snapshot ───────────────────────────────────

def test_parse_segment_path():
    from backend.app.services.segment_snapshot_service import _parse_segment_path
    assert _parse_segment_path("segment.HDD.revenue.FY26") == ("HDD", "revenue", "FY26")
    # rev alias
    assert _parse_segment_path("segment.module_800g.rev.FY27E") == ("module_800g", "revenue", "FY27E")
    assert _parse_segment_path("segment.A.margin.FY25") == ("A", "margin", "FY25")
    # Non-segment paths → None
    assert _parse_segment_path("peer.median.operating_margin") is None
    assert _parse_segment_path("consensus.revenue.FY26") is None


async def test_segment_snapshot_table_exists(db_session):
    from backend.app.models.revenue_snapshot import SegmentRevenueSnapshot
    r = await db_session.execute(
        select(SegmentRevenueSnapshot.ticker, SegmentRevenueSnapshot.metric).limit(1)
    )
    _ = list(r.all())


# ── Lesson versioning ─────────────────────────────────

def test_parse_status_deprecated_prefix():
    from backend.app.services.lesson_versioning import parse_status
    assert parse_status("## L-2026-01-01-001\nDEPRECATED: obsolete rule") == "deprecated"
    assert parse_status("## L-2026-01-01-002\nstatus: archived") == "archived"
    assert parse_status("## L-2026-01-01-003\nSome body") == "active"


def test_parse_expires():
    from backend.app.services.lesson_versioning import parse_expires
    d = parse_expires("expires: 2025-12-31\nbody")
    assert d is not None and d.year == 2025 and d.month == 12 and d.day == 31
    assert parse_expires("no expiry field") is None


def test_is_expired_by_explicit_date():
    from backend.app.services.lesson_versioning import is_expired
    assert is_expired("expires: 2020-01-01\nbody") is True
    assert is_expired("expires: 2099-01-01\nbody") is False


def test_is_expired_by_default_window():
    from backend.app.services.lesson_versioning import is_expired
    # 2 years ago header should be > 180d → expired
    old_id = "## L-2023-01-01-001\nbody"
    assert is_expired(old_id) is True
    # Yesterday should not be expired
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).date().isoformat()
    new_id = f"## L-{yesterday}-001\nbody"
    assert is_expired(new_id) is False


# ── KB snippet highlight ────────────────────────────────

async def test_fetch_document_signature():
    """fetch_document accepts the new highlight_snippet parameter."""
    from backend.app.services.kb_service import fetch_document
    # Invalid doc_id → we still get a structured error, and the function
    # accepts the kwarg without raising a TypeError.
    res = await fetch_document("bogus:format", highlight_snippet="test")
    assert res.get("found") is False


def test_kb_doc_viewer_route_mounted():
    from backend.app.api import kb_doc_viewer  # noqa
    assert kb_doc_viewer.router is not None


# ── A/B winner distiller ────────────────────────────

async def test_distill_no_events():
    from backend.app.services.ab_winner_distiller import distill_ab_winners
    r = await distill_ab_winners(since_days=30, dry_run=True)
    # No events present → processed=0; safe no-op
    assert r.get("processed") == 0


def test_ab_winner_extract_prompt_diff():
    from backend.app.services.ab_winner_distiller import _extract_prompt_diff
    winning = {"nodes": [{"id": "a", "type": "X", "config": {"prompt_template": "use careful"}}]}
    losing = {"nodes": [{"id": "a", "type": "X", "config": {"prompt_template": "go fast"}}]}
    d = _extract_prompt_diff(winning, losing)
    assert len(d["changed_nodes"]) == 1
    assert "careful" in d["changed_nodes"][0]["winning_prompt_preview"]


# ── Lesson vector search (fail-open) ──────────────

async def test_lesson_semantic_search_returns_empty_without_milvus(monkeypatch):
    """If Milvus is unavailable, search_lessons_semantic returns [] without raising."""
    import os
    os.environ["PLAYBOOK_VECTOR_DISABLE"] = "1"
    try:
        from backend.app.services.lesson_vector_search import search_lessons_semantic
        # Force re-read by clearing module-level _mv_client
        import backend.app.services.lesson_vector_search as mod
        mod._mv_client = None
        res = await search_lessons_semantic("optical_modules", "operating_margin", limit=3)
        assert res == []
    finally:
        os.environ.pop("PLAYBOOK_VECTOR_DISABLE", None)


async def test_upsert_lesson_returns_false_without_milvus(monkeypatch):
    import os
    os.environ["PLAYBOOK_VECTOR_DISABLE"] = "1"
    try:
        from backend.app.services.lesson_vector_search import upsert_lesson
        import backend.app.services.lesson_vector_search as mod
        mod._mv_client = None
        ok = await upsert_lesson("optical_modules", "L-test", "Title", "Body")
        assert ok is False
    finally:
        os.environ.pop("PLAYBOOK_VECTOR_DISABLE", None)


# ── Module importability ────────────────────────────

def test_phase6_modules_importable():
    from backend.app.services import (
        ab_winner_distiller, recipe_pr_service, segment_snapshot_service,
        lesson_versioning, lesson_vector_search,  # noqa: F401
    )
    from backend.app.api import (
        kb_doc_viewer, revenue_snapshot,  # noqa: F401
    )
