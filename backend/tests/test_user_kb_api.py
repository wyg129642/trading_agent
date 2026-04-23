"""API-level tests for the personal knowledge base.

Uses FastAPI's TestClient with ``app.dependency_overrides`` to stub
``get_current_user`` so we don't need a real JWT — the test harness
injects a fake ``User`` identity per-request. The underlying service
still hits real MongoDB (via an isolated test DB), which is what gives
this file value beyond the pure-unit tests.

Skipped when local Mongo is unreachable.
"""
from __future__ import annotations

import io
import uuid
from unittest.mock import MagicMock

import pytest
import pytest_asyncio
from fastapi.testclient import TestClient

from backend.app.config import get_settings
from backend.app.main import create_app
from backend.app.deps import get_current_user
from backend.app.services import user_kb_service as svc


def _mongo_up() -> bool:
    try:
        import pymongo
        c = pymongo.MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=1500)
        c.admin.command("ping")
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not _mongo_up(), reason="local MongoDB not reachable")


TEST_DB_NAME = "user_kb_api_test_suite"
USER_A_ID = uuid.UUID("33333333-3333-3333-3333-333333333333")
USER_B_ID = uuid.UUID("44444444-4444-4444-4444-444444444444")


def _fake_user(user_id: uuid.UUID):
    """Create a MagicMock that looks enough like a User for the route.

    The routes call ``user.id`` and pass it through ``str(...)``, so only the
    `id` attribute needs to exist. Using MagicMock avoids pulling the real
    SQLAlchemy model (which would need a DB session to hydrate).
    """
    u = MagicMock()
    u.id = user_id
    u.username = f"test-{user_id.hex[:6]}"
    return u


@pytest_asyncio.fixture
async def _reset_db():
    """Drop + recreate the test DB around each test + rebind the Motor client."""
    settings = get_settings()
    settings.user_kb_mongo_uri = "mongodb://localhost:27017"
    settings.user_kb_mongo_db = TEST_DB_NAME
    svc._clear_mongo_client_cache()
    svc._reset_index_init_for_retry()
    client = svc._mongo_client()
    await client.drop_database(TEST_DB_NAME)
    yield
    await client.drop_database(TEST_DB_NAME)
    svc._clear_mongo_client_cache()


@pytest.fixture
def app_factory(_reset_db):
    """Build a FastAPI app with ``get_current_user`` overridden per-test.

    Tests construct a ``TestClient`` via a ``with`` block over the returned
    app. Using the context manager keeps anyio's portal (and therefore the
    Motor client's event loop) stable for the duration of the test. Without
    it, each request spun a fresh loop which broke Motor's pool.
    """
    app = create_app()

    def _build(user_uuid: uuid.UUID):
        app.dependency_overrides[get_current_user] = lambda: _fake_user(user_uuid)
        return app

    yield _build
    app.dependency_overrides.clear()


def _wait_parse(c: TestClient, doc_id: str, timeout_s: float = 6.0) -> dict:
    """Poll the document endpoint until parse_status is terminal."""
    import time
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        r = c.get(f"/api/user-kb/documents/{doc_id}")
        assert r.status_code == 200, r.text
        d = r.json()
        if d["parse_status"] in ("completed", "failed"):
            return d
        time.sleep(0.1)
    raise AssertionError(f"parse did not finish in {timeout_s}s for {doc_id}")


# ── Endpoint round trips ───────────────────────────────────────


class TestPingStats:
    def test_ping(self, app_factory):
        app = app_factory(USER_A_ID)
        with TestClient(app) as c:
            r = c.get("/api/user-kb/ping")
            assert r.status_code == 200
            assert r.json()["ok"] is True

    def test_empty_stats(self, app_factory):
        app = app_factory(USER_A_ID)
        with TestClient(app) as c:
            r = c.get("/api/user-kb/stats")
            assert r.status_code == 200
            assert r.json()["total_documents"] == 0


class TestDocumentCrud:
    def test_upload_list_fetch_roundtrip(self, app_factory):
        app = app_factory(USER_A_ID)
        with TestClient(app) as c:
            body = b"# Note\n\nHello, this is a note about NVIDIA GPUs and AI demand."
            r = c.post(
                "/api/user-kb/documents",
                files={"file": ("note.md", body, "text/markdown")},
                data={"title": "My Test Note", "tags": "a,b,c"},
            )
            assert r.status_code == 200, r.text
            resp = r.json()
            doc_id = resp["document_id"]
            assert not resp["was_duplicate"]
            assert resp["document"]["title"] == "My Test Note"

            # List
            r2 = c.get("/api/user-kb/documents")
            assert r2.status_code == 200
            items = r2.json()["items"]
            assert len(items) == 1
            assert items[0]["id"] == doc_id

            d = _wait_parse(c, doc_id)
            assert d["parse_status"] == "completed", f"parse: {d.get('parse_error')}"
            assert d["num_chunks"] >= 1

            # Fetch content
            r3 = c.get(f"/api/user-kb/documents/{doc_id}/content")
            assert r3.status_code == 200
            assert "NVIDIA" in r3.json()["content"]

            # Download original
            r4 = c.get(f"/api/user-kb/documents/{doc_id}/file")
            assert r4.status_code == 200
            assert r4.content == body

            # Update metadata
            r5 = c.patch(
                f"/api/user-kb/documents/{doc_id}",
                json={"title": "Renamed", "tags": ["x", "y"]},
            )
            assert r5.status_code == 200
            assert r5.json()["title"] == "Renamed"
            assert r5.json()["tags"] == ["x", "y"]

            # Delete
            r6 = c.delete(f"/api/user-kb/documents/{doc_id}")
            assert r6.status_code == 200
            assert r6.json() == {"ok": True}

            # Confirm gone
            r7 = c.get(f"/api/user-kb/documents/{doc_id}")
            assert r7.status_code == 404

    def test_upload_rejects_bad_extension(self, app_factory):
        app = app_factory(USER_A_ID)
        with TestClient(app) as c:
            r = c.post(
                "/api/user-kb/documents",
                files={"file": ("hack.exe", b"MZ", "application/octet-stream")},
            )
            assert r.status_code == 400
            assert "unsupported" in r.json()["detail"].lower()

    def test_upload_rejects_empty_file(self, app_factory):
        app = app_factory(USER_A_ID)
        with TestClient(app) as c:
            r = c.post(
                "/api/user-kb/documents",
                files={"file": ("empty.txt", b"", "text/plain")},
            )
            assert r.status_code == 400

    def test_management_endpoints_stay_user_scoped(self, app_factory):
        """`/documents` listing and detail remain user-scoped so users can
        only see/edit/delete their own uploads, even though search is
        team-wide."""
        app_a = app_factory(USER_A_ID)
        with TestClient(app_a) as c:
            body = b"User A private data about Meituan delivery network."
            r = c.post(
                "/api/user-kb/documents",
                files={"file": ("a.txt", body, "text/plain")},
            )
            assert r.status_code == 200
            doc_id = r.json()["document_id"]
            _wait_parse(c, doc_id)

        # Switch to user B on the same app.
        app_a.dependency_overrides[get_current_user] = lambda: _fake_user(USER_B_ID)
        with TestClient(app_a) as c_b:
            # Management: B cannot see A's doc in the list.
            r3 = c_b.get("/api/user-kb/documents")
            assert r3.json()["items"] == []
            # And cannot fetch its metadata by id.
            r2 = c_b.get(f"/api/user-kb/documents/{doc_id}")
            assert r2.status_code == 404

    def test_search_is_cross_user_via_http(self, app_factory):
        """`POST /api/user-kb/search` with default ``scope=all`` matches the
        chat-tool behavior — user B sees A's content."""
        app_a = app_factory(USER_A_ID)
        with TestClient(app_a) as c:
            r = c.post(
                "/api/user-kb/documents",
                files={"file": ("a.txt",
                                 b"User A data about Meituan delivery network.",
                                 "text/plain")},
            )
            assert r.status_code == 200
            _wait_parse(c, r.json()["document_id"])

        app_a.dependency_overrides[get_current_user] = lambda: _fake_user(USER_B_ID)
        with TestClient(app_a) as c_b:
            # Default scope=all — cross-user hits.
            r = c_b.post(
                "/api/user-kb/search",
                json={"query": "Meituan delivery"},
            )
            assert r.status_code == 200
            hits = r.json()["hits"]
            assert len(hits) >= 1
            assert hits[0]["uploader_user_id"] == str(USER_A_ID)
            # scope=mine — B sees nothing (hasn't uploaded).
            r2 = c_b.post(
                "/api/user-kb/search",
                json={"query": "Meituan delivery", "scope": "mine"},
            )
            assert r2.status_code == 200
            assert r2.json()["hits"] == []


class TestReparse:
    def test_reparse_endpoint(self, app_factory):
        app = app_factory(USER_A_ID)
        with TestClient(app) as c:
            r = c.post(
                "/api/user-kb/documents",
                files={"file": ("r.md", b"# Reparse test\nsome content", "text/markdown")},
            )
            doc_id = r.json()["document_id"]
            _wait_parse(c, doc_id)
            r2 = c.post(f"/api/user-kb/documents/{doc_id}/reparse")
            assert r2.status_code == 200


class TestSearch:
    def test_search_endpoint(self, app_factory):
        app = app_factory(USER_A_ID)
        with TestClient(app) as c:
            r = c.post(
                "/api/user-kb/documents",
                files={"file": ("s.md", b"# Doc\nPinduoduo overseas expansion via Temu.",
                                 "text/markdown")},
            )
            doc_id = r.json()["document_id"]
            _wait_parse(c, doc_id)
            r2 = c.post("/api/user-kb/search", json={"query": "Temu overseas"})
            assert r2.status_code == 200
            body = r2.json()
            assert body["query"] == "Temu overseas"
            assert len(body["hits"]) >= 1
            assert body["hits"][0]["document_id"] == doc_id
            assert body["hits"][0]["uploader_user_id"] == str(USER_A_ID)

    def test_search_bad_scope_value(self, app_factory):
        app = app_factory(USER_A_ID)
        with TestClient(app) as c:
            r = c.post(
                "/api/user-kb/search",
                json={"query": "x", "scope": "nonsense"},
            )
            assert r.status_code == 400
