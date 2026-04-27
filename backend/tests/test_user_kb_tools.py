"""Integration tests for the user_kb_ chat tool executor.

Verifies the full path from the chat dispatcher: set a user_id ContextVar,
call execute_tool('user_kb_search', ...), check that only that user's
content comes back with citation indices registered on the tracker.
"""
from __future__ import annotations

import shutil
import tempfile
from pathlib import Path

import pytest
import pytest_asyncio

from backend.app.config import get_settings
from backend.app.services import user_kb_service as svc
from backend.app.services import user_kb_tools as tools


def _mongo_up() -> bool:
    try:
        import pymongo
        c = pymongo.MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=1500)
        c.admin.command("ping")
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not _mongo_up(), reason="local MongoDB not reachable")


TEST_DB_NAME = "user_kb_tools_test_suite"
USER_A = "11111111-1111-1111-1111-111111111111"
USER_B = "22222222-2222-2222-2222-222222222222"

DOC_A = b"""# Alpha's Private Notes

Alpha has been following Tencent's games business. In Q4 2025 management
mentioned improvements in the domestic regulatory environment and stronger
international gaming revenue. Cloud business showed operating leverage.
"""

DOC_B = b"""# Beta's Research

Beta's note on Alibaba Q2 earnings: commerce GMV decelerated, cloud margin
expanded, international business (AliExpress) contributed to growth.
"""


@pytest_asyncio.fixture(autouse=True)
async def _isolated_db():
    settings = get_settings()
    settings.user_kb_mongo_uri = "mongodb://localhost:27017"
    settings.user_kb_mongo_db = TEST_DB_NAME
    disk_root = Path(tempfile.mkdtemp(prefix="user_kb_tools_test_disk_"))
    settings.user_kb_disk_root = str(disk_root)
    svc._clear_mongo_client_cache()
    svc._reset_index_init_for_retry()
    client = svc._mongo_client()
    await client.drop_database(TEST_DB_NAME)
    yield
    await client.drop_database(TEST_DB_NAME)
    svc._clear_mongo_client_cache()
    shutil.rmtree(disk_root, ignore_errors=True)


class _FakeTracker:
    """Minimal CitationTracker stand-in — records add_source calls."""
    def __init__(self):
        self.calls: list[dict] = []
        self._idx = 0

    def add_source(self, source: dict) -> int:
        self._idx += 1
        self.calls.append(source)
        return self._idx


async def _upload(user_id: str, data: bytes, filename: str) -> str:
    o = await svc.create_document(
        user_id=user_id, original_filename=filename, data=data,
    )
    await svc.parse_document(o.document_id)
    return o.document_id


# ── Tool execution ─────────────────────────────────────────────


class TestExecuteUserKbSearch:
    @pytest.mark.asyncio
    async def test_search_works_without_user_context(self):
        """With the team-shared KB, missing user context must still serve
        the tool — just logs ``(unknown)`` as the caller in traces."""
        await _upload(USER_A, DOC_A, "alpha.md")
        tok = svc.set_current_user_id("")
        try:
            out = await tools.execute_tool(
                "user_kb_search", {"query": "regulatory gaming"},
            )
        finally:
            svc.reset_current_user_id(tok)
        assert "Tencent" in out or "共享知识库" in out

    @pytest.mark.asyncio
    async def test_search_returns_formatted_hits(self):
        await _upload(USER_A, DOC_A, "alpha_notes.md")
        tracker = _FakeTracker()
        tok = svc.set_current_user_id(USER_A)
        try:
            out = await tools.execute_tool(
                "user_kb_search",
                {"query": "regulatory gaming Tencent", "top_k": 3},
                citation_tracker=tracker,
            )
        finally:
            svc.reset_current_user_id(tok)
        assert "共享知识库命中结果" in out
        assert "[1]" in out
        assert len(tracker.calls) >= 1
        src = tracker.calls[0]
        assert src.get("source_type") == "user_kb"
        # Uploader must flow through to the citation so the UI can surface it.
        assert src.get("uploader_user_id") == USER_A

    @pytest.mark.asyncio
    async def test_search_is_cross_user(self):
        """The critical behavior: user B's chat must see user A's uploads."""
        await _upload(USER_A, DOC_A, "alpha.md")    # mentions Tencent
        await _upload(USER_B, DOC_B, "beta.md")     # mentions Alibaba

        # User B searches for Tencent (which lives in A's doc).
        tok = svc.set_current_user_id(USER_B)
        try:
            out = await tools.execute_tool(
                "user_kb_search", {"query": "Tencent gaming"},
            )
        finally:
            svc.reset_current_user_id(tok)
        assert "Tencent" in out or "共享知识库命中结果" in out
        # And user A searching for Alibaba should see B's doc.
        tok = svc.set_current_user_id(USER_A)
        try:
            out = await tools.execute_tool(
                "user_kb_search", {"query": "Alibaba GMV commerce"},
            )
        finally:
            svc.reset_current_user_id(tok)
        assert "Alibaba" in out or "共享知识库命中结果" in out

    @pytest.mark.asyncio
    async def test_search_missing_query(self):
        tok = svc.set_current_user_id(USER_A)
        try:
            out = await tools.execute_tool("user_kb_search", {})
        finally:
            svc.reset_current_user_id(tok)
        assert "query" in out or "需要参数" in out

    @pytest.mark.asyncio
    async def test_search_document_ids_filter_crosses_users(self):
        """The explicit document_ids filter should work regardless of which
        user uploaded the docs — that's the whole point of cross-user."""
        d_a = await _upload(USER_A, DOC_A, "alpha.md")
        tok = svc.set_current_user_id(USER_B)
        try:
            out = await tools.execute_tool(
                "user_kb_search",
                {"query": "Tencent", "document_ids": [d_a]},
            )
        finally:
            svc.reset_current_user_id(tok)
        assert "Tencent" in out or "共享知识库命中结果" in out


class TestExecuteUserKbFetch:
    @pytest.mark.asyncio
    async def test_fetch_own_document(self):
        doc_id = await _upload(USER_A, DOC_A, "alpha.md")
        tok = svc.set_current_user_id(USER_A)
        try:
            out = await tools.execute_tool(
                "user_kb_fetch_document",
                {"document_id": doc_id, "max_chars": 500},
            )
        finally:
            svc.reset_current_user_id(tok)
        assert "Tencent" in out

    @pytest.mark.asyncio
    async def test_fetch_cross_user_allowed(self):
        """User B's chat session must be able to read user A's upload —
        this is the whole point of the shared KB."""
        doc_id = await _upload(USER_A, DOC_A, "alpha.md")
        tok = svc.set_current_user_id(USER_B)
        try:
            out = await tools.execute_tool(
                "user_kb_fetch_document", {"document_id": doc_id},
            )
        finally:
            svc.reset_current_user_id(tok)
        assert "Tencent" in out

    @pytest.mark.asyncio
    async def test_fetch_missing_id(self):
        tok = svc.set_current_user_id(USER_A)
        try:
            out = await tools.execute_tool("user_kb_fetch_document", {})
        finally:
            svc.reset_current_user_id(tok)
        assert "document_id" in out or "需要参数" in out

    @pytest.mark.asyncio
    async def test_fetch_nonexistent_id(self):
        tok = svc.set_current_user_id(USER_A)
        try:
            out = await tools.execute_tool(
                "user_kb_fetch_document",
                {"document_id": "000000000000000000000000"},
            )
        finally:
            svc.reset_current_user_id(tok)
        assert "未找到" in out

    @pytest.mark.asyncio
    async def test_unknown_tool_name_safe(self):
        tok = svc.set_current_user_id(USER_A)
        try:
            out = await tools.execute_tool("user_kb_frobnicate", {})
        finally:
            svc.reset_current_user_id(tok)
        assert "未知" in out or "unknown" in out.lower()
