"""Integration tests for the per-stock aggregator endpoints.

These tests spin up a MINIMAL FastAPI app that only mounts the
``stock_hub`` and the per-platform DB routers. Using ``create_app()`` from
``backend.app.main`` triggers the full lifespan (Futu/ClickHouse/consensus
warmers, quote warmer, scanner, etc.) which takes 10+ minutes to settle in
staging and is totally irrelevant here — we just need the router wiring +
``get_current_user`` auth override.

The tests hit real remote Mongo + local Postgres to catch:
1. Broken PDF routes: ``pdf_url`` 404s because the template doesn't match
   any actually-mounted route.
2. Empty detail: the drawer sees zero sections because the ``body_sections``
   field map is wrong for that platform.
"""
from __future__ import annotations

import uuid
from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from contextlib import asynccontextmanager

from backend.app.deps import get_current_user


CANONICAL_CANDIDATES = [
    "300750.SZ",  # CATL — wide A-share coverage
    "600519.SH",  # Kweichow Moutai
    "AAPL.US",    # Apple — US
    "00700.HK",   # Tencent — HK
    "NVDA.US",    # Nvidia — US
]


def _fake_user():
    u = MagicMock()
    u.id = uuid.uuid4()
    u.username = "pytest-stock-hub"
    return u


def _build_app() -> FastAPI:
    """Minimal app: only the routers needed to exercise stock_hub + PDF
    endpoints. Skips Futu, consensus warmer, quote warmer, etc. (all of
    which would block on their own network calls and make tests hang for
    minutes). We still need redis on app.state — set it to ``None`` so the
    cache code falls back to the "no redis" path.
    """
    import redis.asyncio as _redis_async

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # Attach a real local redis if available; otherwise None (the code
        # path tolerates a missing client).
        r = None
        try:
            r = _redis_async.Redis(host="localhost", port=6379, decode_responses=True)
            await r.ping()
        except Exception:
            r = None
        app.state.redis = r
        try:
            yield
        finally:
            if r is not None:
                try:
                    await r.aclose()
                except Exception:
                    pass

    app = FastAPI(lifespan=lifespan)
    # Mount just what we need
    from backend.app.api.stock_hub import router as stock_hub_router
    from backend.app.api.alphapai_db import router as alphapai_db_router
    from backend.app.api.jinmen_db import router as jinmen_db_router
    from backend.app.api.meritco_db import router as meritco_db_router
    from backend.app.api.gangtise_db import router as gangtise_db_router
    from backend.app.api.alphaengine_db import router as alphaengine_db_router
    from backend.app.api.acecamp_db import router as acecamp_db_router
    from backend.app.api.funda_db import router as funda_db_router
    from backend.app.api.thirdbridge_db import router as thirdbridge_db_router
    from backend.app.api.semianalysis_db import router as semianalysis_db_router

    app.include_router(stock_hub_router, prefix="/api/stock-hub")
    app.include_router(alphapai_db_router, prefix="/api/alphapai-db")
    app.include_router(jinmen_db_router, prefix="/api/jinmen-db")
    app.include_router(meritco_db_router, prefix="/api/meritco-db")
    app.include_router(gangtise_db_router, prefix="/api/gangtise-db")
    app.include_router(alphaengine_db_router, prefix="/api/alphaengine-db")
    app.include_router(acecamp_db_router, prefix="/api/acecamp-db")
    app.include_router(funda_db_router, prefix="/api/funda-db")
    app.include_router(thirdbridge_db_router, prefix="/api/thirdbridge-db")
    app.include_router(semianalysis_db_router, prefix="/api/semianalysis-db")
    app.dependency_overrides[get_current_user] = lambda: _fake_user()
    return app


@pytest.fixture(scope="module")
def app():
    a = _build_app()
    yield a
    # Drop dependency overrides so other tests don't inherit them
    a.dependency_overrides.clear()


@pytest.fixture(scope="module")
def client(app):
    with TestClient(app) as c:
        yield c


@pytest.fixture(scope="module")
def populated_canonical(client) -> str:
    for cid in CANONICAL_CANDIDATES:
        r = client.get(f"/api/stock-hub/{cid}", params={"limit": 20})
        if r.status_code == 200 and r.json().get("total", 0) > 0:
            return cid
    pytest.skip(f"No data for any candidate in {CANONICAL_CANDIDATES}")


def test_hub_list_returns_items(client, populated_canonical):
    r = client.get(f"/api/stock-hub/{populated_canonical}", params={"limit": 30})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["canonical_id"] == populated_canonical
    assert isinstance(body["items"], list)
    assert body["total"] >= 0
    assert set(body["by_category"].keys()) >= {
        "research", "commentary", "minutes", "interview", "breaking",
    }


def test_every_emitted_pdf_url_resolves(client, populated_canonical):
    """For each item that has a pdf_url, GET it and expect a STRUCTURED
    response — never a FastAPI catch-all 404 that means "route not matched".
    """
    r = client.get(f"/api/stock-hub/{populated_canonical}", params={"limit": 80})
    assert r.status_code == 200
    items_with_pdf = [i for i in r.json()["items"] if i.get("pdf_url")]
    if not items_with_pdf:
        pytest.skip("No items with pdf_url in sample")

    checked = 0
    broken: list[str] = []
    for item in items_with_pdf[:10]:
        pdf_url = item["pdf_url"]
        assert pdf_url.startswith("/api/"), pdf_url
        # Stream first 1 KB — enough to confirm the handler ran.
        resp = client.get(pdf_url, headers={"Range": "bytes=0-1023"})
        assert resp.status_code in (200, 206, 307, 404, 500), (
            f"PDF route {pdf_url} returned unexpected {resp.status_code}: "
            f"{resp.text[:200]}"
        )
        # The specific 404 shape we MUST NOT see is FastAPI's built-in
        # "route not matched" — detail == "Not Found". Our handlers always
        # return a longer string like "PDF not available: ..." or
        # "This report has no PDF on the platform".
        if resp.status_code == 404:
            try:
                detail = resp.json().get("detail", "")
            except Exception:
                detail = resp.text
            if detail == "Not Found":
                broken.append(f"{item['source']}.{item['collection']} → {pdf_url}")
        checked += 1
    assert not broken, (
        "These pdf_url templates hit an unmounted route (catch-all 404): "
        + ", ".join(broken)
    )
    assert checked > 0, "Did not actually exercise any PDF URL"


def test_detail_endpoint_returns_sections(client, populated_canonical):
    """Detail endpoint shape check: each (source, collection) pair returns
    a well-formed response. We don't assert section count > 0 because new
    scrapes may have list rows whose detail isn't filled in yet — but we DO
    assert the shape so a regression in the router itself would fail."""
    r = client.get(f"/api/stock-hub/{populated_canonical}", params={"limit": 80})
    assert r.status_code == 200
    items = r.json()["items"]
    if not items:
        pytest.skip("No items to drill into")

    seen: dict[str, bool] = {}
    for item in items:
        key = f"{item['source']}:{item['collection']}"
        if seen.get(key):
            continue
        if item["source"] == "newsfeed":
            path = f"/api/stock-hub/newsfeed/{item['id']}"
        else:
            path = (
                f"/api/stock-hub/doc/{item['source']}/"
                f"{item['collection']}/{item['id']}"
            )
        resp = client.get(path)
        assert resp.status_code == 200, (
            f"{path} → {resp.status_code}: {resp.text[:200]}"
        )
        body = resp.json()
        assert "sections" in body and isinstance(body["sections"], list), body
        assert body["title"] is not None, body
        assert body["source"] == item["source"], body
        assert body["collection"] == item["collection"], body
        seen[key] = True

    assert seen, "Detail endpoint not exercised"


def test_detail_404_on_unknown_id(client):
    r = client.get("/api/stock-hub/doc/alphapai/reports/does-not-exist-xyz")
    assert r.status_code == 404
    r = client.get("/api/stock-hub/doc/unknown_source/fake_coll/x")
    assert r.status_code == 404


def test_every_configured_pdf_route_template_is_mounted(client):
    """Compile-time-style check for pdf_route templates: probe with a
    placeholder ID. A catch-all 404 (``detail=="Not Found"``) means the
    router doesn't recognize the path at all — indistinguishable from a
    typo. Handlers that return any other status (or return 404 with their
    own detail string) prove the route is wired.
    """
    from backend.app.api.stock_hub import SOURCES

    broken = []
    for spec in SOURCES:
        if not spec.pdf_route:
            continue
        # Strip any query string so the route matcher has a clean path;
        # meritco adds ?i=0 for example.
        path = spec.pdf_route.split("?", 1)[0].replace("{id}", "__probe__")
        resp = client.get(path)
        if resp.status_code == 404:
            detail = ""
            try:
                detail = resp.json().get("detail", "")
            except Exception:
                pass
            if detail == "Not Found":
                broken.append(f"{spec.source}.{spec.collection} → {path}")
    assert not broken, (
        "These pdf_route templates do not match any mounted route: "
        + ", ".join(broken)
    )
