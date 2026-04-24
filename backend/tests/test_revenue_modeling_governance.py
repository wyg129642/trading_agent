"""Governance tests — cost cap/quota, hallucination guard, lesson auto-apply.

This covers the 2026-04-23 governance layer that surrounds the existing
revenue-modeling engine: pre-flight cost gate, citation-audit review loop,
and lesson auto-apply on approval.
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
        email=f"gov-{uuid.uuid4().hex[:8]}@test.local",
        username=f"gov{uuid.uuid4().hex[:8]}",
        password_hash="$2b$12$x",
        display_name="Governance Test",
        role="user",
        is_active=True,
    )
    db_session.add(u)
    await db_session.commit()
    await db_session.refresh(u)
    yield u
    await db_session.execute(delete(User).where(User.id == u.id))
    await db_session.commit()


# ── Cost estimation ────────────────────────────────────────

def test_pricing_for_known_and_unknown():
    from backend.app.services.cost_estimation import pricing_for, PRICING_USD_PER_M
    for k in PRICING_USD_PER_M:
        assert pricing_for(k) == PRICING_USD_PER_M[k]
    # Unknown gets a sensible fallback (positive rates)
    p = pricing_for("some/unknown-model-v3")
    assert p[0] > 0 and p[1] > 0


def test_cost_from_tokens_never_negative():
    from backend.app.services.cost_estimation import cost_from_tokens
    assert cost_from_tokens("anthropic/claude-opus-4-7", -5, -10) == 0.0
    c = cost_from_tokens("anthropic/claude-opus-4-7", 1_000_000, 200_000)
    # Opus: $15 in + $75 out per 1M → 15 + 15 = $30
    assert abs(c - 30.0) < 0.01


def test_estimate_recipe_cost_multiplies_debate_steps():
    from backend.app.services.cost_estimation import estimate_recipe_cost
    graph = {
        "nodes": [
            {"id": "a", "type": "GATHER_CONTEXT"},
            {"id": "b", "type": "VERIFY_AND_ASK"},
            {"id": "c", "type": "CHECK_MODEL"},
        ],
    }
    est_low = estimate_recipe_cost(graph, debate_roles=1)
    est_high = estimate_recipe_cost(graph, debate_roles=3)
    # VERIFY_AND_ASK is one of the multiplied steps → higher total
    assert est_high.total_usd > est_low.total_usd
    assert est_high.step_count == 3
    assert "VERIFY_AND_ASK" in {n["type"] for n in graph["nodes"]}


def test_estimate_recipe_cost_empty_graph():
    from backend.app.services.cost_estimation import estimate_recipe_cost
    est = estimate_recipe_cost({"nodes": []}, debate_roles=3)
    assert est.total_usd == 0.0
    assert est.step_count == 0
    assert any("Empty" in a for a in est.assumptions)


async def test_check_user_quota_default_and_remaining(db_session, test_user):
    """Fresh user with no runs → full budget remaining."""
    from backend.app.services.cost_estimation import check_user_quota
    status = await check_user_quota(db_session, test_user, estimated_add_usd=0.0)
    assert status.exceeded is False
    assert status.remaining_usd == status.monthly_budget_usd
    assert status.spent_this_month_usd == 0.0


async def test_check_user_quota_flags_exceeded(db_session, test_user):
    from backend.app.services.cost_estimation import check_user_quota
    # Set a tiny budget
    test_user.llm_budget_usd_monthly = 1.0
    await db_session.commit()
    status = await check_user_quota(db_session, test_user, estimated_add_usd=5.0)
    assert status.exceeded is True


# ── Hallucination guard ────────────────────────────────────

def test_hallucination_guard_constants_are_sane():
    from backend.app.services.hallucination_guard import (
        HALLUCINATION_RED_LINE, HALLUCINATION_WARN_LINE, DAILY_SAMPLE_SIZE,
    )
    assert 0.0 < HALLUCINATION_WARN_LINE < HALLUCINATION_RED_LINE < 1.0
    assert DAILY_SAMPLE_SIZE >= 1


async def test_weekly_review_no_data_does_not_crash(monkeypatch):
    """Empty audit log → summary with rate=0, no Feishu call."""
    from backend.app.services import hallucination_guard as hg
    sent = []

    async def _fake_send(summary, paused):
        sent.append((summary, paused))

    monkeypatch.setattr(hg, "_send_hallucination_feishu", _fake_send)
    result = await hg.weekly_review_and_alert(since_days=7, auto_pause=True)
    assert result["summary"]["hallucination_rate"] == 0
    # Rate 0 < WARN_LINE → no alert sent
    assert sent == []
    assert result["paused_models"] == []


# ── Lesson auto-apply ────────────────────────────────────

def test_extract_patterns_simple():
    from backend.app.services.lesson_auto_apply import _extract_patterns
    body = """
    # Lesson L-0001
    Some content.
    applicable_path_patterns: ['segment.*.rev.*', 'margin.operating.*']
    """
    patterns = _extract_patterns(body)
    assert "segment.*.rev.*" in patterns
    assert "margin.operating.*" in patterns


def test_extract_patterns_missing_returns_empty():
    from backend.app.services.lesson_auto_apply import _extract_patterns
    assert _extract_patterns("Just prose, no metadata") == []
    assert _extract_patterns("") == []


def test_path_matches_any_glob():
    from backend.app.services.lesson_auto_apply import _path_matches_any
    assert _path_matches_any("segment.HDD.rev.FY26", ["segment.*.rev.*"])
    assert _path_matches_any("margin.operating.FY27", ["margin.operating.*"])
    assert not _path_matches_any("segment.HDD.vol.FY26", ["segment.*.rev.*"])


async def test_apply_approved_lesson_empty_patterns(db_session, test_user):
    """Lesson with no metadata patterns → no-op, returns 0 counts."""
    from backend.app.models.feedback import PendingLesson
    from backend.app.services.lesson_auto_apply import apply_approved_lesson
    l = PendingLesson(
        industry="nonexistent_industry_xyz",
        lesson_id=f"L-test-{uuid.uuid4().hex[:6]}",
        title="Empty lesson",
        body="No patterns here",
        status="pending",
        batch_week="2026-04-20",
    )
    db_session.add(l)
    await db_session.commit()
    await db_session.refresh(l)
    summary = await apply_approved_lesson(db_session, l, approver_id=test_user.id)
    assert summary["matched_cells"] == 0
    assert summary["events_written"] == 0
    await db_session.execute(delete(PendingLesson).where(PendingLesson.id == l.id))
    await db_session.commit()


# ── Cost governance API imports ────────────────────────────

def test_cost_governance_router_importable():
    from backend.app.api import cost_governance  # noqa: F401
    assert cost_governance.router is not None


def test_hallucination_guard_module_importable():
    from backend.app.services import hallucination_guard  # noqa: F401
    assert hasattr(hallucination_guard, "weekly_review_and_alert")
    assert hasattr(hallucination_guard, "daily_sample_pass")


# ── RecipeRun new columns ────────────────────────────────

async def test_recipe_run_has_cost_governance_columns(db_session):
    """Migration applied successfully — columns are addressable."""
    from backend.app.models.recipe import RecipeRun
    # Query against an empty table is enough to prove the columns exist
    r = await db_session.execute(
        select(
            RecipeRun.estimated_cost_usd,
            RecipeRun.cost_cap_usd,
            RecipeRun.paused_reason,
        ).limit(1)
    )
    # Doesn't raise → columns exist
    _ = list(r.all())


async def test_revenue_model_has_guard_columns(db_session):
    from backend.app.models.revenue_model import RevenueModel
    r = await db_session.execute(
        select(
            RevenueModel.paused_by_guard,
            RevenueModel.paused_reason,
        ).limit(1)
    )
    _ = list(r.all())


async def test_user_has_budget_columns(db_session):
    from backend.app.models.user import User
    r = await db_session.execute(
        select(
            User.llm_budget_usd_monthly,
            User.llm_run_cap_usd,
        ).limit(1)
    )
    _ = list(r.all())
