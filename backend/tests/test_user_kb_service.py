"""Integration tests for the personal-KB service layer — hits local MongoDB.

Every test runs against an isolated test database (``user_kb_test_suite``)
which is dropped before and after each module run so tests are independent
of each other and of any real user data.

Skipped automatically if local Mongo is unreachable.
"""
from __future__ import annotations

import asyncio
import shutil
import tempfile
from pathlib import Path

import pytest
import pytest_asyncio

from backend.app.config import get_settings
from backend.app.services import user_kb_service as svc


# ── Skip guard ─────────────────────────────────────────────────


def _mongo_up() -> bool:
    try:
        import pymongo
        c = pymongo.MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=1500)
        c.admin.command("ping")
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not _mongo_up(), reason="local MongoDB not reachable")


# ── Fixtures ───────────────────────────────────────────────────


TEST_DB_NAME = "user_kb_test_suite"
USER_A = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
USER_B = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"


@pytest_asyncio.fixture(autouse=True)
async def _isolated_db(tmp_path_factory):
    """Drop + recreate the test DB around every test.

    Mongo's lru_cache'd Motor client is bound to the event loop of its first
    call. Since pytest-asyncio gives us a fresh loop per test, we also clear
    the cache so the next call gets a client bound to the active loop.
    """
    settings = get_settings()
    # Tests always run against local Mongo, regardless of the configured
    # production URI (which points at the shared ops cluster).
    settings.user_kb_mongo_uri = "mongodb://localhost:27017"
    settings.user_kb_mongo_db = TEST_DB_NAME
    # Per-test temp dir for the on-disk binary store, so create_document
    # never writes into the real /home/ygwang/crawl_data/user_kb_files tree.
    disk_root = Path(tempfile.mkdtemp(prefix="user_kb_test_disk_"))
    settings.user_kb_disk_root = str(disk_root)

    # Clear Motor client cache so it rebinds to this test's event loop.
    svc._clear_mongo_client_cache()
    svc._reset_index_init_for_retry()

    client = svc._mongo_client()
    await client.drop_database(TEST_DB_NAME)
    yield
    await client.drop_database(TEST_DB_NAME)
    svc._clear_mongo_client_cache()
    shutil.rmtree(disk_root, ignore_errors=True)


# ── Small helpers ──────────────────────────────────────────────


MD_SAMPLE = b"""# Quarterly Note

This is a research note covering Tencent's Q4 2025 earnings.

## Key points

Management noted strong international gaming revenue. Domestic regulation is
easing. Advertising revenue grew 18 percent year over year.

## Risks

Primary risk is continued regulatory tightening in the mobile gaming segment.
"""


async def _upload_and_parse(user_id: str, filename: str = "note.md",
                             data: bytes = MD_SAMPLE, title: str | None = None) -> str:
    outcome = await svc.create_document(
        user_id=user_id, original_filename=filename, data=data, title=title,
    )
    await svc.parse_document(outcome.document_id)
    return outcome.document_id


# ── Tests ──────────────────────────────────────────────────────


class TestPing:
    @pytest.mark.asyncio
    async def test_ping(self):
        ok, _msg = await svc.ping()
        assert ok


class TestCreateDocument:
    @pytest.mark.asyncio
    async def test_basic_upload(self):
        outcome = await svc.create_document(
            user_id=USER_A, original_filename="a.md", data=MD_SAMPLE,
        )
        assert outcome.document_id
        assert not outcome.was_duplicate
        doc = await svc.get_document(USER_A, outcome.document_id)
        assert doc is not None
        assert doc["original_filename"] == "a.md"
        assert doc["upload_status"] == "completed"
        assert doc["parse_status"] == "pending"
        assert doc["file_size_bytes"] == len(MD_SAMPLE)

    @pytest.mark.asyncio
    async def test_duplicate_upload_returns_same_id(self):
        o1 = await svc.create_document(
            user_id=USER_A, original_filename="a.md", data=MD_SAMPLE,
        )
        o2 = await svc.create_document(
            user_id=USER_A, original_filename="a.md", data=MD_SAMPLE, title="different",
        )
        assert o2.was_duplicate
        assert o2.document_id == o1.document_id

    @pytest.mark.asyncio
    async def test_different_users_do_not_dedup(self):
        """Same bytes uploaded by different users are stored independently."""
        o1 = await svc.create_document(
            user_id=USER_A, original_filename="a.md", data=MD_SAMPLE,
        )
        o2 = await svc.create_document(
            user_id=USER_B, original_filename="a.md", data=MD_SAMPLE,
        )
        assert not o2.was_duplicate
        assert o1.document_id != o2.document_id

    @pytest.mark.asyncio
    async def test_rejects_empty_file(self):
        with pytest.raises(ValueError, match="empty"):
            await svc.create_document(user_id=USER_A, original_filename="a.txt", data=b"")

    @pytest.mark.asyncio
    async def test_rejects_unsupported_extension(self):
        with pytest.raises(ValueError, match="unsupported"):
            await svc.create_document(
                user_id=USER_A, original_filename="a.exe", data=b"MZ\x90",
            )

    @pytest.mark.asyncio
    async def test_rejects_oversized_file(self):
        settings = get_settings()
        orig = settings.user_kb_max_file_bytes
        settings.user_kb_max_file_bytes = 100
        try:
            with pytest.raises(ValueError, match="too large"):
                await svc.create_document(
                    user_id=USER_A, original_filename="big.txt", data=b"X" * 200,
                )
        finally:
            settings.user_kb_max_file_bytes = orig

    @pytest.mark.asyncio
    async def test_rejects_blank_user_id(self):
        with pytest.raises(ValueError, match="user_id"):
            await svc.create_document(
                user_id="", original_filename="a.md", data=MD_SAMPLE,
            )

    @pytest.mark.asyncio
    async def test_rejects_oversized_filename(self):
        settings = get_settings()
        orig = settings.user_kb_max_filename_length
        settings.user_kb_max_filename_length = 20
        try:
            with pytest.raises(ValueError, match="filename too long"):
                await svc.create_document(
                    user_id=USER_A,
                    original_filename=("x" * 30) + ".md",
                    data=MD_SAMPLE,
                )
        finally:
            settings.user_kb_max_filename_length = orig

    @pytest.mark.asyncio
    async def test_rejects_blank_filename(self):
        with pytest.raises(ValueError, match="filename"):
            await svc.create_document(
                user_id=USER_A, original_filename="   ", data=MD_SAMPLE,
            )

    @pytest.mark.asyncio
    async def test_quota_enforced(self):
        settings = get_settings()
        orig = settings.user_kb_max_docs_per_user
        settings.user_kb_max_docs_per_user = 2
        try:
            for i in range(2):
                await svc.create_document(
                    user_id=USER_A, original_filename=f"f{i}.md",
                    data=f"content {i}".encode(),
                )
            with pytest.raises(ValueError, match="quota"):
                await svc.create_document(
                    user_id=USER_A, original_filename="third.md", data=b"third",
                )
        finally:
            settings.user_kb_max_docs_per_user = orig


class TestReliabilityGuards:
    """Verify the reliability knobs (timeout, semaphore, empty-output warning)."""

    @pytest.mark.asyncio
    async def test_parse_timeout_marks_doc_failed(self):
        """A parse that exceeds the timeout must not leave the doc in
        ``parsing`` forever — it should flip to ``failed`` with a clear
        error message so the user can re-try."""
        settings = get_settings()
        orig_timeout = settings.user_kb_parse_timeout_seconds
        settings.user_kb_parse_timeout_seconds = 1
        # Monkey-patch the parser to simulate a slow parse.
        import backend.app.services.user_kb_parser as pr_mod
        original = pr_mod.parse_file
        import time as _time
        def _slow(filename, data):
            _time.sleep(3)
            return original(filename, data)
        pr_mod.parse_file = _slow
        try:
            outcome = await svc.create_document(
                user_id=USER_A, original_filename="slow.md", data=MD_SAMPLE,
            )
            await svc.parse_document(outcome.document_id)
            doc = await svc.get_document(USER_A, outcome.document_id)
            assert doc["parse_status"] == "failed"
            assert "timeout" in (doc["parse_error"] or "").lower()
        finally:
            pr_mod.parse_file = original
            settings.user_kb_parse_timeout_seconds = orig_timeout

    @pytest.mark.asyncio
    async def test_empty_parse_result_warns(self):
        """If the parser returns empty text, the doc still completes but
        carries a warning so the UI can flag it."""
        import backend.app.services.user_kb_parser as pr_mod
        original = pr_mod.parse_file

        def _empty(filename, data):
            return pr_mod.ParseResult(text="   ", parser="stub", warnings=[])

        pr_mod.parse_file = _empty
        try:
            outcome = await svc.create_document(
                user_id=USER_A, original_filename="blank.md", data=MD_SAMPLE,
            )
            await svc.parse_document(outcome.document_id)
            doc = await svc.get_document(USER_A, outcome.document_id)
            assert doc["parse_status"] == "completed"
            assert any("no searchable" in w for w in doc["parse_warnings"])
            assert doc["num_chunks"] == 0
        finally:
            pr_mod.parse_file = original

    @pytest.mark.asyncio
    async def test_concurrent_parses_bounded_by_semaphore(self):
        """With concurrency=1, two queued parses must be strictly serial."""
        settings = get_settings()
        orig = settings.user_kb_parse_concurrency
        settings.user_kb_parse_concurrency = 1
        # Force the semaphore to be rebuilt on next access.
        svc._parse_semaphores.clear()

        import backend.app.services.user_kb_parser as pr_mod
        import time as _time
        original = pr_mod.parse_file
        active = {"count": 0, "max": 0}

        def _tracked(filename, data):
            active["count"] += 1
            active["max"] = max(active["max"], active["count"])
            _time.sleep(0.2)
            active["count"] -= 1
            return original(filename, data)
        pr_mod.parse_file = _tracked
        try:
            o1 = await svc.create_document(
                user_id=USER_A, original_filename="s1.md", data=MD_SAMPLE,
            )
            o2 = await svc.create_document(
                user_id=USER_A, original_filename="s2.md",
                data=MD_SAMPLE + b"\n\nExtra content.",
            )
            # Launch both in parallel; semaphore must serialize them.
            await asyncio.gather(
                svc.parse_document(o1.document_id),
                svc.parse_document(o2.document_id),
            )
            assert active["max"] == 1, (
                f"semaphore breached: saw {active['max']} concurrent parses"
            )
        finally:
            pr_mod.parse_file = original
            settings.user_kb_parse_concurrency = orig
            svc._parse_semaphores.clear()


class TestParseDocument:
    @pytest.mark.asyncio
    async def test_parse_produces_chunks(self):
        doc_id = await _upload_and_parse(USER_A)
        doc = await svc.get_document(USER_A, doc_id)
        assert doc["parse_status"] == "completed"
        assert doc["num_chunks"] >= 1
        assert doc["extracted_char_count"] > 0
        assert doc["parse_error"] is None

    @pytest.mark.asyncio
    async def test_parse_idempotent(self):
        doc_id = await _upload_and_parse(USER_A)
        # Parsing again is a no-op (claim step returns None for already-completed).
        await svc.parse_document(doc_id)
        doc = await svc.get_document(USER_A, doc_id)
        assert doc["parse_status"] == "completed"

    @pytest.mark.asyncio
    async def test_reparse_rebuilds_chunks(self):
        doc_id = await _upload_and_parse(USER_A)
        original = await svc.get_document(USER_A, doc_id)
        # Trigger re-parse.
        ok = await svc.reparse_document(USER_A, doc_id)
        assert ok
        # Wait for the spawned task to finish.
        for _ in range(60):
            d = await svc.get_document(USER_A, doc_id)
            if d["parse_status"] == "completed":
                break
            await asyncio.sleep(0.1)
        d = await svc.get_document(USER_A, doc_id)
        assert d["parse_status"] == "completed"
        assert d["num_chunks"] == original["num_chunks"]

    @pytest.mark.asyncio
    async def test_parse_non_existent_doc_is_safe(self):
        # Invalid id — should not raise.
        await svc.parse_document("deadbeefdeadbeefdeadbeef")
        await svc.parse_document("not-an-object-id")


class TestListAndSearch:
    @pytest.mark.asyncio
    async def test_list_returns_only_requesting_user(self):
        await _upload_and_parse(USER_A, filename="a.md")
        await _upload_and_parse(USER_B, filename="b.md", data=b"other user data")
        items_a, total_a = await svc.list_documents(USER_A)
        items_b, total_b = await svc.list_documents(USER_B)
        assert total_a == 1
        assert total_b == 1
        assert items_a[0]["original_filename"] == "a.md"
        assert items_b[0]["original_filename"] == "b.md"

    @pytest.mark.asyncio
    async def test_list_filters_by_status(self):
        await _upload_and_parse(USER_A, filename="done.md")
        # Upload a second one WITHOUT parsing so status=pending.
        await svc.create_document(
            user_id=USER_A, original_filename="pending.md", data=b"x" * 50,
        )
        items_completed, _ = await svc.list_documents(USER_A, status="completed")
        items_pending, _ = await svc.list_documents(USER_A, status="pending")
        assert len(items_completed) == 1
        assert items_completed[0]["original_filename"] == "done.md"
        assert len(items_pending) == 1
        assert items_pending[0]["original_filename"] == "pending.md"

    @pytest.mark.asyncio
    async def test_list_search_substring(self):
        await _upload_and_parse(USER_A, filename="tencent_q4.md")
        await _upload_and_parse(USER_A, filename="alibaba_q4.md",
                                 data=b"Alibaba report")
        items, total = await svc.list_documents(USER_A, search="tencent")
        assert total == 1
        assert items[0]["original_filename"] == "tencent_q4.md"

    @pytest.mark.asyncio
    async def test_list_pagination(self):
        for i in range(5):
            await svc.create_document(
                user_id=USER_A, original_filename=f"doc{i}.md",
                data=f"content {i}".encode(),
            )
        items, total = await svc.list_documents(USER_A, limit=2, offset=0)
        assert total == 5
        assert len(items) == 2
        items2, _ = await svc.list_documents(USER_A, limit=2, offset=2)
        assert len(items2) == 2
        # Newest-first ordering: offset 0 and 2 must be disjoint.
        assert {i["id"] for i in items}.isdisjoint({i["id"] for i in items2})

    @pytest.mark.asyncio
    async def test_search_returns_relevant_hits(self):
        await _upload_and_parse(USER_A)
        hits = await svc.search_chunks("regulatory gaming", user_id=USER_A, top_k=5)
        assert len(hits) > 0
        assert hits[0].score > 0
        # Uploader must be populated for cross-user attribution.
        assert hits[0].uploader_user_id == USER_A

    @pytest.mark.asyncio
    async def test_search_scoped_by_user_when_requested(self):
        """Passing user_id restricts results to that user's uploads."""
        await _upload_and_parse(USER_A)
        hits_b = await svc.search_chunks("regulatory gaming", user_id=USER_B)
        assert hits_b == []
        hits_a = await svc.search_chunks("regulatory gaming", user_id=USER_A)
        assert len(hits_a) > 0

    @pytest.mark.asyncio
    async def test_search_is_cross_user_by_default(self):
        """With no user_id, search must span the whole team's uploads.

        This is the behavior the chat tool relies on — any user's AI
        assistant can retrieve content uploaded by any colleague.
        """
        await _upload_and_parse(USER_A, filename="alpha.md", data=MD_SAMPLE)
        # User B queries without a scope filter — should see A's doc.
        hits = await svc.search_chunks("regulatory gaming")
        assert len(hits) > 0, "cross-user search returned nothing"
        assert hits[0].uploader_user_id == USER_A
        # And vice versa: explicit empty string also means cross-user.
        hits2 = await svc.search_chunks("regulatory gaming", user_id="")
        assert len(hits2) > 0

    @pytest.mark.asyncio
    async def test_search_empty_query_returns_empty(self):
        await _upload_and_parse(USER_A)
        assert await svc.search_chunks("", user_id=USER_A) == []
        assert await svc.search_chunks("   ") == []

    @pytest.mark.asyncio
    async def test_search_document_ids_filter(self):
        d1 = await _upload_and_parse(USER_A, filename="a.md", data=MD_SAMPLE)
        d2 = await _upload_and_parse(USER_A, filename="b.md",
                                       data=b"Different content about regulatory policy.")
        # Restrict to d2 only.
        hits = await svc.search_chunks(
            "regulatory", user_id=USER_A, document_ids=[d2],
        )
        assert all(h.document_id == d2 for h in hits)
        # No valid IDs → empty results (not a broad search).
        hits_empty = await svc.search_chunks(
            "regulatory", user_id=USER_A, document_ids=["bad-id"],
        )
        assert hits_empty == []
        _ = d1  # kept to show the filter excluded the other doc


class TestDocumentCrud:
    @pytest.mark.asyncio
    async def test_get_document_wrong_user_returns_none(self):
        doc_id = await _upload_and_parse(USER_A)
        assert await svc.get_document(USER_B, doc_id) is None

    @pytest.mark.asyncio
    async def test_get_document_bad_id_returns_none(self):
        assert await svc.get_document(USER_A, "not-an-object-id") is None

    @pytest.mark.asyncio
    async def test_get_content_returns_full_text(self):
        doc_id = await _upload_and_parse(USER_A)
        text = await svc.get_document_content(USER_A, doc_id)
        assert text and "Tencent" in text

    @pytest.mark.asyncio
    async def test_get_content_honors_max_chars(self):
        doc_id = await _upload_and_parse(USER_A)
        text = await svc.get_document_content(USER_A, doc_id, max_chars=20)
        assert len(text) <= 20

    @pytest.mark.asyncio
    async def test_get_any_document_crosses_users(self):
        """The cross-user fetch path backs the chat tool — it must hand back
        another user's doc, since the KB is team-shared for reads."""
        doc_id = await _upload_and_parse(USER_A)
        meta = await svc.get_any_document(doc_id)
        assert meta is not None
        assert meta["user_id"] == USER_A

    @pytest.mark.asyncio
    async def test_get_any_document_content_crosses_users(self):
        doc_id = await _upload_and_parse(USER_A)
        text = await svc.get_any_document_content(doc_id)
        assert text and "Tencent" in text

    @pytest.mark.asyncio
    async def test_update_document_fields(self):
        doc_id = await _upload_and_parse(USER_A, title="Original")
        updated = await svc.update_document(
            USER_A, doc_id, title="New title", description="A note",
            tags=["tag1", "tag2", "tag1"],  # dedupe test
        )
        assert updated["title"] == "New title"
        assert updated["description"] == "A note"
        assert updated["tags"] == ["tag1", "tag2"]

    @pytest.mark.asyncio
    async def test_update_empty_title_rejected(self):
        doc_id = await _upload_and_parse(USER_A)
        with pytest.raises(ValueError):
            await svc.update_document(USER_A, doc_id, title="   ")

    @pytest.mark.asyncio
    async def test_update_other_user_returns_none(self):
        doc_id = await _upload_and_parse(USER_A)
        result = await svc.update_document(USER_B, doc_id, title="Hijack!")
        assert result is None
        # Original still intact.
        orig = await svc.get_document(USER_A, doc_id)
        assert orig["title"] != "Hijack!"

    @pytest.mark.asyncio
    async def test_update_tags_respects_limit(self):
        doc_id = await _upload_and_parse(USER_A)
        many = [f"tag-{i}" for i in range(50)]
        updated = await svc.update_document(USER_A, doc_id, tags=many)
        # Implementation caps at 32 tags.
        assert len(updated["tags"]) == 32

    @pytest.mark.asyncio
    async def test_delete_document_cascades(self):
        doc_id = await _upload_and_parse(USER_A)
        # Before delete: chunks exist.
        pre = await svc._chunks().count_documents({"user_id": USER_A})
        assert pre > 0
        ok = await svc.delete_document(USER_A, doc_id)
        assert ok
        # After delete: no doc, no chunks.
        assert await svc.get_document(USER_A, doc_id) is None
        post = await svc._chunks().count_documents({"user_id": USER_A})
        assert post == 0

    @pytest.mark.asyncio
    async def test_delete_other_user_fails(self):
        doc_id = await _upload_and_parse(USER_A)
        assert not await svc.delete_document(USER_B, doc_id)
        # And the doc is still there.
        assert await svc.get_document(USER_A, doc_id) is not None


class TestDownload:
    @pytest.mark.asyncio
    async def test_download_round_trip(self):
        doc_id = await _upload_and_parse(USER_A)
        result = await svc.download_file(USER_A, doc_id)
        assert result is not None
        meta, data = result
        assert data == MD_SAMPLE
        assert meta["user_id"] == USER_A


class TestStats:
    @pytest.mark.asyncio
    async def test_stats_reflect_state(self):
        await _upload_and_parse(USER_A, filename="done.md")
        await svc.create_document(
            user_id=USER_A, original_filename="pending.md", data=b"x" * 50,
        )
        stats = await svc.get_user_stats(USER_A)
        assert stats["total_documents"] == 2
        assert stats["status_completed"] == 1
        assert stats["status_pending"] == 1
        assert stats["total_bytes"] >= len(MD_SAMPLE)

    @pytest.mark.asyncio
    async def test_stats_isolated_per_user(self):
        await _upload_and_parse(USER_A)
        stats_b = await svc.get_user_stats(USER_B)
        assert stats_b["total_documents"] == 0


class TestRecoverStuckParses:
    @pytest.mark.asyncio
    async def test_recover_pending_docs(self):
        """A doc left in ``pending`` after a crash should be re-enqueued."""
        outcome = await svc.create_document(
            user_id=USER_A, original_filename="crashed.md", data=MD_SAMPLE,
        )
        # It's already "pending" by default.
        n = await svc.recover_stuck_parses()
        assert n >= 1
        for _ in range(60):
            d = await svc.get_document(USER_A, outcome.document_id)
            if d["parse_status"] == "completed":
                break
            await asyncio.sleep(0.1)
        d = await svc.get_document(USER_A, outcome.document_id)
        assert d["parse_status"] == "completed"

    @pytest.mark.asyncio
    async def test_recover_stale_parsing_docs(self):
        """A ``parsing`` doc whose worker died long ago must be reclaimed.

        We simulate the crash by flipping the doc to ``parsing`` with a
        ``parse_started_at`` far in the past.
        """
        from datetime import datetime, timezone, timedelta
        outcome = await svc.create_document(
            user_id=USER_A, original_filename="long_stuck.md", data=MD_SAMPLE,
        )
        await svc._docs().update_one(
            {"_id": svc._oid(outcome.document_id)},
            {"$set": {
                "parse_status": "parsing",
                "parse_started_at": datetime.now(timezone.utc) - timedelta(hours=2),
            }},
        )
        n = await svc.recover_stuck_parses()
        assert n >= 1

    @pytest.mark.asyncio
    async def test_does_not_reclaim_fresh_parsing_docs(self):
        """A doc in ``parsing`` that *just* started must NOT be touched.

        Racing the recovery sweeper with a live worker would double-parse
        the same doc, producing duplicate chunks and log spam.
        """
        outcome = await svc.create_document(
            user_id=USER_A, original_filename="in_flight.md", data=MD_SAMPLE,
        )
        from datetime import datetime, timezone
        await svc._docs().update_one(
            {"_id": svc._oid(outcome.document_id)},
            {"$set": {
                "parse_status": "parsing",
                "parse_started_at": datetime.now(timezone.utc),  # fresh
            }},
        )
        n = await svc.recover_stuck_parses()
        assert n == 0, "fresh parsing doc should not be reclaimed"


class TestIndexes:
    @pytest.mark.asyncio
    async def test_ensure_indexes_idempotent(self):
        await svc.ensure_indexes()
        await svc.ensure_indexes()  # second call is a no-op
        names = [idx["name"] async for idx in svc._chunks().list_indexes()]
        assert "chunk_tokens_idx" in names, f"indexes: {names}"

    @pytest.mark.asyncio
    async def test_text_index_targets_tokens_field(self):
        """The text index must point at ``tokens`` (jieba-preprocessed),
        not the raw ``text`` field. Without this, Chinese queries fail
        because MongoDB's default tokenizer can't split CJK spans."""
        await svc.ensure_indexes()
        async for idx in svc._chunks().list_indexes():
            if idx.get("weights"):
                weight_fields = set(idx.get("weights", {}).keys())
                assert weight_fields == {"tokens"}, (
                    f"text index covers unexpected fields: {weight_fields}"
                )
                return
        raise AssertionError("no text index found on chunks collection")

    @pytest.mark.asyncio
    async def test_stale_text_field_index_gets_replaced(self):
        """If a prior deploy left a text index on the ``text`` field
        (pre-jieba era), ``ensure_indexes`` drops it and rebuilds on
        ``tokens``."""
        from pymongo import TEXT
        # Plant the old-style text-only index on the raw text field.
        await svc._chunks().create_index(
            [("text", TEXT)],
            name="chunk_text_idx",
            default_language="none",
        )
        svc._reset_index_init_for_retry()
        await svc.ensure_indexes()
        names = [idx["name"] async for idx in svc._chunks().list_indexes()]
        assert "chunk_tokens_idx" in names
        assert "chunk_text_idx" not in names


class TestChineseSearch:
    """The raison d'être of the jieba preprocessing — Chinese queries must
    actually hit their substrings."""

    @pytest.mark.asyncio
    async def test_pure_cjk_query_hits_substring(self):
        """Upload a doc containing '接口说明', search for '接口说明',
        get the chunk back. This is the failure mode the migration fixes."""
        doc_body = (
            "# 服务接口说明\n\n"
            "本文档描述 HTTP 接口说明。每个接口说明包含请求参数、响应格式。\n"
        ).encode("utf-8")
        await _upload_and_parse(USER_A, filename="api.md", data=doc_body)
        hits = await svc.search_chunks("接口说明")
        assert len(hits) >= 1, "pure-CJK search returned nothing"
        assert "接口说明" in hits[0].text

    @pytest.mark.asyncio
    async def test_two_word_cjk_query_ranks_matches(self):
        """A multi-word CJK query should rank chunks that contain BOTH
        words higher than chunks with just one."""
        only_one = b"# Doc One\n\n\xe8\x85\xbe\xe8\xae\xaf\xe6\xb8\xb8\xe6\x88\x8f"  # 腾讯游戏
        both = (
            "# Doc Two\n\n"
            "腾讯的游戏业务 Q4 表现：管理层强调国际市场。"
        ).encode("utf-8")
        await _upload_and_parse(USER_A, filename="one.md", data=only_one)
        await _upload_and_parse(USER_A, filename="both.md", data=both)
        hits = await svc.search_chunks("腾讯 管理层", top_k=5)
        assert hits
        # The chunk containing both terms should score highest.
        assert "管理层" in hits[0].text

    @pytest.mark.asyncio
    async def test_mixed_cn_en_query_works(self):
        body = (
            "# Mixed\n\n"
            "英伟达 NVDA 在 AI 芯片市场占据领先地位。"
        ).encode("utf-8")
        await _upload_and_parse(USER_A, filename="mixed.md", data=body)
        # Both pure-CJK and mixed queries should work.
        hits1 = await svc.search_chunks("NVDA")
        hits2 = await svc.search_chunks("AI 芯片")
        hits3 = await svc.search_chunks("英伟达 NVDA")
        assert len(hits1) >= 1
        assert len(hits2) >= 1
        assert len(hits3) >= 1


class TestTokenBackfill:
    """Migration from the pre-jieba schema must populate tokens on legacy
    chunks so they become searchable."""

    @pytest.mark.asyncio
    async def test_backfill_populates_tokens_on_legacy_rows(self):
        # Manually insert a chunk without a tokens field (simulating a row
        # from before the jieba upgrade).
        from bson import ObjectId
        legacy_doc_id = ObjectId()
        await svc._chunks().insert_one({
            "user_id": USER_A,
            "document_id": legacy_doc_id,
            "chunk_index": 0,
            "text": "这是一段接口说明的测试。",
            "char_count": 12,
            # deliberately no "tokens" field
        })
        updated = await svc.backfill_chunk_tokens()
        assert updated == 1
        row = await svc._chunks().find_one({"document_id": legacy_doc_id})
        assert "接口" in row["tokens"]
        assert "说明" in row["tokens"]
        # Idempotent — re-running does nothing.
        again = await svc.backfill_chunk_tokens()
        assert again == 0

    @pytest.mark.asyncio
    async def test_backfill_force_refreshes_all(self):
        """``force=True`` rewrites tokens on every chunk, even those that
        already have a tokens field. Needed when the tokenizer algorithm
        changes (e.g. NFKC normalization added)."""
        await _upload_and_parse(USER_A)
        # Corrupt the stored tokens to simulate a stale tokenizer version.
        await svc._chunks().update_many(
            {"user_id": USER_A},
            {"$set": {"tokens": "stale stale stale"}},
        )
        updated = await svc.backfill_chunk_tokens(force=True)
        assert updated >= 1
        # Confirm the stored tokens no longer contain the stale marker.
        row = await svc._chunks().find_one({"user_id": USER_A})
        assert "stale" not in (row.get("tokens") or "")

    @pytest.mark.asyncio
    async def test_newly_parsed_chunks_already_have_tokens(self):
        """New parses must write tokens as part of the chunk row — so we
        don't rely on the migration for live ingest."""
        await _upload_and_parse(USER_A)
        rows = await svc._chunks().find(
            {"user_id": USER_A}, {"tokens": 1},
        ).to_list(length=100)
        assert rows
        assert all(r.get("tokens") for r in rows), (
            "fresh parses must populate tokens"
        )
