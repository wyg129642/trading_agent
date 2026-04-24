"""API-level tests for chat feedback + memory.

Uses FastAPI's TestClient + dependency_overrides to stub auth. Runs against
a real Postgres; skips cleanly when one isn't reachable.

Fixture DB access uses sync psycopg2, not the async engine — the module-level
async engine binds connections to whatever loop first touched it, and pytest
tears down / recreates loops around each TestClient call, so sharing the async
pool across test boundaries causes "attached to a different loop" errors.
"""
from __future__ import annotations

import uuid
from unittest.mock import MagicMock

import psycopg2
import psycopg2.extras
import pytest
from fastapi.testclient import TestClient

from backend.app.config import get_settings
from backend.app.deps import get_current_user
from backend.app.models.user import User


def _sync_dsn() -> str:
    s = get_settings()
    return (
        f"host={s.postgres_host} port={s.postgres_port} dbname={s.postgres_db} "
        f"user={s.postgres_user} password={s.postgres_password}"
    )


def _pg_up_sync() -> bool:
    try:
        with psycopg2.connect(_sync_dsn(), connect_timeout=3) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        return True
    except Exception:
        return False


def _user_table_exists_sync() -> bool:
    try:
        with psycopg2.connect(_sync_dsn(), connect_timeout=3) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT EXISTS (SELECT FROM information_schema.tables "
                    "WHERE table_name = 'user_chat_memories')"
                )
                return bool(cur.fetchone()[0])
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not (_pg_up_sync() and _user_table_exists_sync()),
    reason="Postgres not reachable or memory migration not yet applied",
)


TEST_USER_ID = uuid.UUID("55555555-5555-5555-5555-555555555555")
OTHER_USER_ID = uuid.UUID("66666666-6666-6666-6666-666666666666")


def _fake_user(user_id=TEST_USER_ID):
    u = MagicMock(spec=User)
    u.id = user_id
    u.username = f"test-{user_id.hex[:6]}"
    u.is_active = True
    u.role = "user"
    return u


@pytest.fixture
def app_with_auth():
    from backend.app.main import create_app
    app = create_app()
    app.dependency_overrides[get_current_user] = lambda: _fake_user()
    yield app
    app.dependency_overrides.clear()


@pytest.fixture
def client(app_with_auth):
    """Open TestClient as a context manager so all requests in a single test
    share one event loop — without this, each call spins up its own portal
    and asyncpg's pooled connections get re-used across loops, which raises
    "attached to a different loop"."""
    with TestClient(app_with_auth) as c:
        yield c


def _ensure_user_sync(user_id: uuid.UUID) -> None:
    """Insert a minimal user row via psycopg2 (idempotent)."""
    from backend.app.core.security import hash_password
    with psycopg2.connect(_sync_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO users (id, username, email, password_hash, role, is_active)
                VALUES (%s, %s, %s, %s, 'user', TRUE)
                ON CONFLICT (id) DO NOTHING
                """,
                (
                    str(user_id),
                    f"test_mem_{user_id.hex[:6]}",
                    f"test_mem_{user_id.hex[:6]}@example.com",
                    hash_password("x"),
                ),
            )
        conn.commit()


def _seed_response_sync(user_id: uuid.UUID) -> tuple[str, str, str]:
    """Create conv → msg → response directly via psycopg2, return ids."""
    _ensure_user_sync(user_id)
    conv_id = uuid.uuid4()
    msg_id = uuid.uuid4()
    resp_id = uuid.uuid4()
    with psycopg2.connect(_sync_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO chat_conversations (id, user_id, title) VALUES (%s, %s, %s)",
                (str(conv_id), str(user_id), "memory test"),
            )
            cur.execute(
                "INSERT INTO chat_messages (id, conversation_id, role, content) "
                "VALUES (%s, %s, 'user', %s)",
                (str(msg_id), str(conv_id), "What is NVDA?"),
            )
            cur.execute(
                "INSERT INTO chat_model_responses (id, message_id, model_id, model_name, content) "
                "VALUES (%s, %s, %s, %s, %s)",
                (
                    str(resp_id),
                    str(msg_id),
                    "openai/gpt-5.4",
                    "GPT-5.4",
                    "NVDA is a GPU leader...",
                ),
            )
        conn.commit()
    return str(resp_id), str(conv_id), str(msg_id)


@pytest.fixture
def seeded_response():
    """Seed a conversation → message → response chain for the test user."""
    return _seed_response_sync(TEST_USER_ID)


@pytest.fixture(autouse=True)
def _cleanup_memories():
    """Wipe test user memories + feedback rows (sync) before each test.

    Also dispose the async engine pool so the next TestClient boots fresh —
    asyncpg connections bind to the loop that opened them, and TestClient's
    per-call lifespan creates a new loop each time; reusing pooled
    connections across those boundaries raises "attached to a different loop".
    """
    def _clean():
        with psycopg2.connect(_sync_dsn()) as conn:
            with conn.cursor() as cur:
                for uid in (TEST_USER_ID, OTHER_USER_ID):
                    cur.execute(
                        "DELETE FROM user_chat_memories WHERE user_id = %s", (str(uid),)
                    )
                    cur.execute(
                        "DELETE FROM chat_feedback_events WHERE user_id = %s", (str(uid),)
                    )
            conn.commit()

    async def _dispose_pool():
        from backend.app.core.database import engine as _async_engine
        try:
            await _async_engine.dispose()
        except Exception:
            pass

    import asyncio

    def _dispose_sync():
        # Fresh loop per dispose so we don't accidentally reuse a dead one.
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(_dispose_pool())
        finally:
            loop.close()

    _clean()
    _dispose_sync()
    yield
    _clean()
    _dispose_sync()


# ─────────────────────────────────────────────────
# Feedback submit
# ─────────────────────────────────────────────────

def test_submit_feedback_requires_nonempty_payload(client, seeded_response):
    response_id, _, _ = seeded_response
    r = client.post(f"/api/chat-memory/feedback/{response_id}", json={
        "rating": None, "feedback_tags": [], "feedback_text": "",
    })
    assert r.status_code == 400


def test_submit_feedback_happy_path(client, seeded_response):
    response_id, _, _ = seeded_response
    r = client.post(f"/api/chat-memory/feedback/{response_id}", json={
        "rating": 4,
        "feedback_tags": ["clear", "helpful"],
        "feedback_text": "please use tables for comparisons next time",
    })
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["rating"] == 4
    assert set(body["feedback_tags"]) == {"clear", "helpful"}
    assert "tables" in body["feedback_text"]
    assert body["sentiment"] == "positive"  # positive tags + rating 4
    assert body["processed"] is False


def test_submit_feedback_negative_sentiment(client, seeded_response):
    response_id, _, _ = seeded_response
    r = client.post(f"/api/chat-memory/feedback/{response_id}", json={
        "rating": 2,
        "feedback_tags": ["too_long", "outdated"],
        "feedback_text": "",
    })
    assert r.status_code == 200
    assert r.json()["sentiment"] == "negative"


def test_submit_feedback_404_for_nonexistent(client):
    fake_id = str(uuid.uuid4())
    r = client.post(f"/api/chat-memory/feedback/{fake_id}", json={
        "rating": 5, "feedback_tags": [], "feedback_text": "",
    })
    assert r.status_code == 404


def test_submit_feedback_403_for_foreign_response(client, seeded_response):
    """Even with a valid response id, another user cannot submit feedback on it."""
    response_id, _, _ = seeded_response
    # Override auth to OTHER_USER
    from backend.app.main import create_app
    app = create_app()
    app.dependency_overrides[get_current_user] = lambda: _fake_user(OTHER_USER_ID)
    with TestClient(app) as other_client:
        r = other_client.post(f"/api/chat-memory/feedback/{response_id}", json={
            "rating": 5,
        })
        assert r.status_code == 403


def test_submit_feedback_normalizes_tags(client, seeded_response):
    response_id, _, _ = seeded_response
    r = client.post(f"/api/chat-memory/feedback/{response_id}", json={
        "rating": 3,
        "feedback_tags": ["Too Long!", "OFF_TOPIC", "wrong"],
        "feedback_text": "",
    })
    assert r.status_code == 200
    body = r.json()
    # Tags get lower-cased + snake-case'd
    assert "too_long" in body["feedback_tags"]
    assert "off_topic" in body["feedback_tags"]
    assert "wrong" in body["feedback_tags"]


def test_list_my_feedback(client, seeded_response):
    response_id, _, _ = seeded_response
    # Submit two feedback events
    client.post(f"/api/chat-memory/feedback/{response_id}", json={"rating": 5})
    client.post(f"/api/chat-memory/feedback/{response_id}", json={
        "rating": 2, "feedback_text": "actually, it was too shallow"
    })
    r = client.get("/api/chat-memory/feedback")
    assert r.status_code == 200
    rows = r.json()
    assert len(rows) >= 2
    # Most recent first
    assert rows[0]["rating"] == 2


# ─────────────────────────────────────────────────
# Memory CRUD
# ─────────────────────────────────────────────────

def test_create_memory_manual(client):
    r = client.post("/api/chat-memory/memories", json={
        "memory_type": "preference",
        "memory_key": "prefers_forward_pe",
        "content": "Always include forward PE in industry comparisons",
        "is_pinned": True,
    })
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["memory_type"] == "preference"
    assert body["memory_key"] == "prefers_forward_pe"
    assert body["is_pinned"] is True
    assert body["source_type"] == "manual"
    assert body["confidence_score"] == 1.0


def test_create_memory_rejects_duplicate_key(client):
    payload = {
        "memory_type": "preference", "memory_key": "dup_key",
        "content": "first", "is_pinned": False,
    }
    r1 = client.post("/api/chat-memory/memories", json=payload)
    assert r1.status_code == 200
    r2 = client.post("/api/chat-memory/memories", json=payload)
    assert r2.status_code == 409


def test_create_memory_rejects_invalid_type(client):
    r = client.post("/api/chat-memory/memories", json={
        "memory_type": "nonsense",
        "memory_key": "x", "content": "y",
    })
    assert r.status_code == 400


def test_list_memories_returns_counts(client):
    client.post("/api/chat-memory/memories", json={
        "memory_type": "preference", "memory_key": "k1", "content": "a",
    })
    client.post("/api/chat-memory/memories", json={
        "memory_type": "correction", "memory_key": "k2", "content": "b",
    })
    r = client.get("/api/chat-memory/memories")
    assert r.status_code == 200
    body = r.json()
    assert body["total"] >= 2
    assert body["total_active"] >= 2
    assert len(body["memories"]) >= 2


def test_list_memories_filter_by_type(client):
    client.post("/api/chat-memory/memories", json={
        "memory_type": "preference", "memory_key": "pk", "content": "p",
    })
    client.post("/api/chat-memory/memories", json={
        "memory_type": "correction", "memory_key": "ck", "content": "c",
    })
    r = client.get("/api/chat-memory/memories?memory_type=correction")
    assert r.status_code == 200
    body = r.json()
    types = {m["memory_type"] for m in body["memories"]}
    assert types == {"correction"}


def test_patch_memory_toggles(client):
    r = client.post("/api/chat-memory/memories", json={
        "memory_type": "preference", "memory_key": "toggle_me", "content": "x",
    })
    mem_id = r.json()["id"]

    r2 = client.patch(f"/api/chat-memory/memories/{mem_id}", json={"is_active": False})
    assert r2.status_code == 200
    assert r2.json()["is_active"] is False

    r3 = client.patch(f"/api/chat-memory/memories/{mem_id}", json={"is_pinned": True})
    assert r3.status_code == 200
    assert r3.json()["is_pinned"] is True


def test_patch_memory_updates_content(client):
    r = client.post("/api/chat-memory/memories", json={
        "memory_type": "preference", "memory_key": "edit_me", "content": "old content",
    })
    mem_id = r.json()["id"]

    r2 = client.patch(f"/api/chat-memory/memories/{mem_id}", json={
        "content": "new content with more detail",
    })
    assert r2.status_code == 200
    assert r2.json()["content"] == "new content with more detail"


def test_patch_memory_rejects_empty_content(client):
    r = client.post("/api/chat-memory/memories", json={
        "memory_type": "preference", "memory_key": "emptycheck", "content": "x",
    })
    mem_id = r.json()["id"]
    r2 = client.patch(f"/api/chat-memory/memories/{mem_id}", json={"content": "   "})
    assert r2.status_code == 400


def test_delete_memory(client):
    r = client.post("/api/chat-memory/memories", json={
        "memory_type": "preference", "memory_key": "delete_me", "content": "x",
    })
    mem_id = r.json()["id"]
    r2 = client.delete(f"/api/chat-memory/memories/{mem_id}")
    assert r2.status_code == 200
    # Second delete → 404
    r3 = client.delete(f"/api/chat-memory/memories/{mem_id}")
    assert r3.status_code == 404


def test_patch_nonexistent_memory_404(client):
    fake = str(uuid.uuid4())
    r = client.patch(f"/api/chat-memory/memories/{fake}", json={"is_active": False})
    assert r.status_code == 404


def test_patch_invalid_uuid_400(client):
    r = client.patch("/api/chat-memory/memories/not-a-uuid", json={"is_active": False})
    assert r.status_code == 400


# ─────────────────────────────────────────────────
# Cross-user isolation
# ─────────────────────────────────────────────────

def test_user_cannot_see_other_users_memories():
    """Two users submit memories — each only sees their own."""
    import asyncio as _asyncio

    def _dispose_engine():
        from backend.app.core.database import engine as _eng
        loop = _asyncio.new_event_loop()
        try:
            loop.run_until_complete(_eng.dispose())
        finally:
            loop.close()

    for uid in (TEST_USER_ID, OTHER_USER_ID):
        _ensure_user_sync(uid)

    from backend.app.main import create_app

    # User A adds a memory
    app_a = create_app()
    app_a.dependency_overrides[get_current_user] = lambda: _fake_user(TEST_USER_ID)
    with TestClient(app_a) as ca:
        r = ca.post("/api/chat-memory/memories", json={
            "memory_type": "preference", "memory_key": "user_a_secret", "content": "A only",
        })
        assert r.status_code == 200, r.text
    app_a.dependency_overrides.clear()
    # Release connections bound to the first TestClient's loop before opening a
    # second client — otherwise asyncpg reuses dead connections.
    _dispose_engine()

    # User B lists memories — should not see user A's
    app_b = create_app()
    app_b.dependency_overrides[get_current_user] = lambda: _fake_user(OTHER_USER_ID)
    with TestClient(app_b) as cb:
        r = cb.get("/api/chat-memory/memories")
        assert r.status_code == 200, r.text
        keys = {m["memory_key"] for m in r.json()["memories"]}
        assert "user_a_secret" not in keys
    app_b.dependency_overrides.clear()
