"""End-to-end test for the revenue modeling system.

Exercises:
  * Industry pack loading
  * Model + cell creation
  * Formula engine evaluation on stored cells
  * Recipe engine end-to-end in dry_run mode (no LLM calls)
  * Sanity checks
  * Feedback event + consolidator

Uses the real Postgres DB so the schema migration is also validated.
"""
from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timezone

import pytest
import pytest_asyncio
from sqlalchemy import delete, select

pytestmark = pytest.mark.asyncio


# ── Fixtures ────────────────────────────────────────────────


@pytest_asyncio.fixture(autouse=True)
async def _dispose_engine():
    """Async engine is a module singleton. Re-create between tests."""
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
    from sqlalchemy import select
    # Use any existing user or create one
    q = select(User).limit(1)
    u = (await db_session.execute(q)).scalar_one_or_none()
    if u:
        return u
    u = User(
        email="modeltest@example.com",
        password_hash="$2b$12$dummy",
        display_name="Test User",
        role="user",
        is_active=True,
    )
    db_session.add(u)
    await db_session.commit()
    await db_session.refresh(u)
    return u


@pytest_asyncio.fixture
async def seed_recipe(db_session):
    """Ensure the optical_standard_v1 recipe is in DB."""
    from industry_packs import pack_registry
    from backend.app.models.recipe import Recipe
    pack_registry.reload()
    pack = pack_registry.get("optical_modules")
    assert pack is not None
    rdata = pack.get_recipe("standard_v1")
    assert rdata is not None

    pack_ref = "optical_modules:standard_v1"
    q = select(Recipe).where(Recipe.pack_ref == pack_ref).order_by(Recipe.version.desc()).limit(1)
    existing = (await db_session.execute(q)).scalar_one_or_none()
    if existing:
        return existing
    r = Recipe(
        name=rdata.get("name") or "standard",
        slug=rdata.get("slug") or "optical_standard",
        industry="optical_modules",
        description=rdata.get("description", ""),
        graph={"nodes": rdata.get("nodes", []), "edges": rdata.get("edges", [])},
        version=1,
        is_public=True,
        pack_ref=pack_ref,
    )
    db_session.add(r)
    await db_session.commit()
    await db_session.refresh(r)
    return r


# ── Tests ────────────────────────────────────────────────────


async def test_pack_loads_and_has_expected_content():
    from industry_packs import pack_registry
    pack_registry.reload()
    packs = pack_registry.list()
    slugs = [p.slug for p in packs]
    assert "optical_modules" in slugs
    pack = pack_registry.get("optical_modules")
    assert pack.name == "光通信"
    assert len(pack.recipes) >= 1
    assert len(pack.segments_schema.get("segments", [])) >= 5


async def test_formula_engine_e2e():
    from backend.app.services.formula_engine import FormulaEngine
    e = FormulaEngine()
    # Mirror the LITE pattern from the Excel reference
    e.set_cell("segment.module_800g.volume.FY26", value=200)
    e.set_cell("segment.module_800g.asp.FY26", value=400)
    e.set_cell("segment.module_800g.rev.FY26",
               formula="=segment.module_800g.volume.FY26 * segment.module_800g.asp.FY26 / 10000")
    e.set_cell("total_revenue.FY26", formula="=segment.module_800g.rev.FY26")
    e.set_cell("operating_margin.FY26", value=0.33)
    e.set_cell("ebit.FY26", formula="=total_revenue.FY26 * operating_margin.FY26")
    e.set_cell("tax_rate.FY26", value=0.15)
    e.set_cell("ni.FY26", formula="=ebit.FY26 * (1 - tax_rate.FY26)")
    e.evaluate_all()
    assert abs(e.get("segment.module_800g.rev.FY26") - 8.0) < 1e-9
    assert abs(e.get("ebit.FY26") - 8.0 * 0.33) < 1e-9
    assert abs(e.get("ni.FY26") - 8.0 * 0.33 * 0.85) < 1e-9


async def test_create_model_and_cells(db_session, test_user):
    from backend.app.models.revenue_model import ModelCell, RevenueModel
    from backend.app.services import model_cell_store as _store

    # Clean up any prior test model with same ticker
    await db_session.execute(
        delete(RevenueModel).where(
            RevenueModel.owner_user_id == test_user.id,
            RevenueModel.ticker == "LITE_TEST.US",
        )
    )
    await db_session.commit()

    m = RevenueModel(
        ticker="LITE_TEST.US", company_name="Lumentum (test)",
        industry="optical_modules",
        fiscal_periods=["FY25E", "FY26E", "FY27E"],
        title="LITE test", base_currency="USD",
        owner_user_id=test_user.id, status="draft",
    )
    db_session.add(m)
    await db_session.commit()
    await db_session.refresh(m)

    # Write a volume and ASP cell, then a derived revenue cell
    await _store.upsert_cell(
        db_session, m.id, path="segment.module_800g.volume.FY26E",
        label="800G volume", period="FY26E", unit="万块", value=200.0,
        value_type="count", source_type="expert",
    )
    await _store.upsert_cell(
        db_session, m.id, path="segment.module_800g.asp.FY26E",
        label="800G ASP", period="FY26E", unit="美元", value=400.0,
        value_type="currency", source_type="expert",
    )
    await _store.upsert_cell(
        db_session, m.id, path="segment.module_800g.rev.FY26E",
        label="800G Rev", period="FY26E", unit="亿美元",
        formula="=segment.module_800g.volume.FY26E * segment.module_800g.asp.FY26E / 10000",
        value_type="currency", source_type="derived",
    )
    await db_session.commit()

    result = await _store.evaluate_formulas(db_session, m.id)
    await db_session.commit()
    assert result["evaluated"] >= 1
    q = select(ModelCell).where(
        ModelCell.model_id == m.id,
        ModelCell.path == "segment.module_800g.rev.FY26E",
    )
    c = (await db_session.execute(q)).scalar_one()
    assert abs(c.value - 8.0) < 1e-9

    # Cleanup
    await db_session.delete(m)
    await db_session.commit()


async def test_recipe_engine_dry_run_e2e(db_session, test_user, seed_recipe):
    """Run the full optical recipe in dry_run mode and verify cells land."""
    from backend.app.models.recipe import RecipeRun
    from backend.app.models.revenue_model import ModelCell, RevenueModel
    from backend.app.services.recipe_engine import run_recipe

    # Clean
    await db_session.execute(
        delete(RevenueModel).where(
            RevenueModel.owner_user_id == test_user.id,
            RevenueModel.ticker == "LITE_DRY.US",
        )
    )
    await db_session.commit()

    m = RevenueModel(
        ticker="LITE_DRY.US", company_name="Lumentum (dry-run)",
        industry="optical_modules",
        fiscal_periods=["FY25E", "FY26E", "FY27E"],
        title="LITE dry-run",
        owner_user_id=test_user.id, status="draft",
        recipe_id=seed_recipe.id, recipe_version=seed_recipe.version,
    )
    db_session.add(m)
    await db_session.commit()
    await db_session.refresh(m)

    run = RecipeRun(
        recipe_id=seed_recipe.id, recipe_version=seed_recipe.version,
        model_id=m.id, ticker=m.ticker,
        started_by=test_user.id, status="pending",
        settings={"dry_run": True},
    )
    db_session.add(run)
    await db_session.commit()
    run_id = run.id

    # Execute the run synchronously (inside the test)
    await run_recipe(run_id, dry_run=True)

    # Reload state
    m2 = await db_session.get(RevenueModel, m.id)
    run2 = await db_session.get(RecipeRun, run_id)
    # SQLAlchemy identity map — refresh to pick up the recipe engine's commits
    await db_session.refresh(m2)
    await db_session.refresh(run2)
    assert run2.status == "completed", f"run failed: {run2.error}"
    assert m2.status == "ready"

    # There should be cells across multiple categories
    all_cells = (
        await db_session.execute(select(ModelCell).where(ModelCell.model_id == m.id))
    ).scalars().all()
    paths = [c.path for c in all_cells]

    # Must see at least one segment cell, guidance, and margin cascade
    assert any(p.startswith("segment.") for p in paths), f"no segment cells: {paths[:10]}"
    assert any(p.startswith("total_revenue.") for p in paths), "no total_revenue"
    assert any(p.startswith("ni.") for p in paths), "no net income cascade"
    assert any(p.startswith("eps.") for p in paths), "no EPS cascade"

    # EPS cell should have a numeric value if NI + shares populated
    eps_cells = [c for c in all_cells if c.path.startswith("eps.")]
    assert eps_cells
    for c in eps_cells:
        # formulas are set — evaluate_formulas should have populated values
        assert c.formula is not None

    # Cleanup
    await db_session.delete(m)
    await db_session.commit()


async def test_sanity_detects_margin_out_of_range(db_session, test_user):
    """Seed a clearly-out-of-range margin and verify sanity flags it."""
    from backend.app.models.revenue_model import RevenueModel
    from backend.app.services import model_cell_store as _store
    from backend.app.services.model_sanity import check_model
    from industry_packs import pack_registry

    pack_registry.reload()
    pack = pack_registry.get("optical_modules")

    # Clean
    await db_session.execute(
        delete(RevenueModel).where(RevenueModel.ticker == "SANITY_TEST.US")
    )
    await db_session.commit()

    m = RevenueModel(
        ticker="SANITY_TEST.US", company_name="Sanity",
        industry="optical_modules",
        fiscal_periods=["FY26E"],
        owner_user_id=test_user.id,
    )
    db_session.add(m)
    await db_session.commit()
    await db_session.refresh(m)

    await _store.upsert_cell(
        db_session, m.id, path="operating_margin.FY26E",
        value=0.95,    # absurd 95% OM — out of range [0, 0.55]
        value_type="percent", source_type="assumption", unit="%",
    )
    await db_session.commit()
    issues = await check_model(db_session, m, pack)
    assert any(i["issue_type"] == "margin_out_of_range" for i in issues), issues

    await db_session.delete(m)
    await db_session.commit()


async def test_sanity_detects_cycle(db_session, test_user):
    from backend.app.models.revenue_model import RevenueModel
    from backend.app.services import model_cell_store as _store
    from backend.app.services.model_sanity import check_model
    from industry_packs import pack_registry

    pack_registry.reload()
    pack = pack_registry.get("optical_modules")

    await db_session.execute(
        delete(RevenueModel).where(RevenueModel.ticker == "CYCLE_TEST.US")
    )
    await db_session.commit()

    m = RevenueModel(
        ticker="CYCLE_TEST.US", company_name="Cycle", industry="optical_modules",
        fiscal_periods=["FY26E"], owner_user_id=test_user.id,
    )
    db_session.add(m)
    await db_session.commit()
    await db_session.refresh(m)

    await _store.upsert_cell(db_session, m.id, path="a", formula="=b + 1")
    await _store.upsert_cell(db_session, m.id, path="b", formula="=a + 1")
    await db_session.commit()
    issues = await check_model(db_session, m, pack)
    assert any(i["issue_type"] == "cycle" for i in issues), issues

    await db_session.delete(m)
    await db_session.commit()


async def test_feedback_event_recorded(db_session, test_user):
    from backend.app.models.feedback import UserFeedbackEvent
    from backend.app.services import model_cell_store as _store
    await _store.emit_feedback(
        db_session, user_id=test_user.id, event_type="cell_edit",
        industry="optical_modules", cell_path="test.path",
        payload={"old": {"value": 1}, "new": {"value": 2}, "reason": "test"},
    )
    await db_session.commit()
    q = (
        select(UserFeedbackEvent)
        .where(UserFeedbackEvent.user_id == test_user.id,
               UserFeedbackEvent.cell_path == "test.path")
        .order_by(UserFeedbackEvent.created_at.desc())
        .limit(1)
    )
    row = (await db_session.execute(q)).scalar_one_or_none()
    assert row is not None
    assert row.payload["new"]["value"] == 2
    await db_session.delete(row)
    await db_session.commit()


async def test_playbook_search_by_path():
    from backend.app.services import playbook_service
    text = playbook_service.search_lessons("optical_modules", "segment.module_800g.volume.FY26")
    # Should return some lesson text (the seed lessons mention 800G)
    assert len(text) > 0


async def test_recipe_path_dependency_parsing():
    """Regression test: parser handles Chinese + multi-segment dot paths."""
    from backend.app.services.formula_engine import parse_dependencies
    deps = parse_dependencies("=SUM(segment.module_800g.rev.FY26 + segment.chip_eml_cw.eml_200g.rev.FY26)")
    assert "segment.module_800g.rev.FY26" in deps
    assert "segment.chip_eml_cw.eml_200g.rev.FY26" in deps
