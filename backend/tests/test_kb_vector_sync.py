"""Unit tests for kb_vector_sync + the get_last_watermark helper.

These tests mock the ingest / sweep primitives in kb_vector_ingest so they
run offline — no Milvus, no TEI, no remote Mongo. The integration path is
exercised live by the /admin endpoints and via the CLI
``python3 -m scripts.kb_vector status`` after the service is wired up.
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from backend.app.config import get_settings
from backend.app.services import kb_vector_ingest, kb_vector_sync
from backend.app.services.kb_service import SPECS_LIST
from backend.app.services.kb_vector_sync import (
    KbVectorSyncService,
    _should_start,
)


# ── get_last_watermark ─────────────────────────────────────────────


def test_get_last_watermark_missing_doc_returns_none():
    spec = SPECS_LIST[0]
    fake_coll = MagicMock()
    fake_coll.find_one.return_value = None
    fake_db = MagicMock()
    fake_db.__getitem__.return_value = fake_coll
    fake_client = MagicMock()
    fake_client.__getitem__.return_value = fake_db

    with patch.object(kb_vector_ingest, "_state_client", return_value=fake_client):
        assert kb_vector_ingest.get_last_watermark(spec) is None


def test_get_last_watermark_reads_int():
    spec = SPECS_LIST[0]
    fake_coll = MagicMock()
    fake_coll.find_one.return_value = {"_id": "x", "last_release_time_ms": 1700000000000}
    fake_db = MagicMock()
    fake_db.__getitem__.return_value = fake_coll
    fake_client = MagicMock()
    fake_client.__getitem__.return_value = fake_db

    with patch.object(kb_vector_ingest, "_state_client", return_value=fake_client):
        assert kb_vector_ingest.get_last_watermark(spec) == 1700000000000


def test_get_last_watermark_handles_bad_value():
    spec = SPECS_LIST[0]
    fake_coll = MagicMock()
    fake_coll.find_one.return_value = {"_id": "x", "last_release_time_ms": "not-an-int"}
    fake_db = MagicMock()
    fake_db.__getitem__.return_value = fake_coll
    fake_client = MagicMock()
    fake_client.__getitem__.return_value = fake_db

    with patch.object(kb_vector_ingest, "_state_client", return_value=fake_client):
        assert kb_vector_ingest.get_last_watermark(spec) is None


def test_get_last_watermark_handles_exception():
    spec = SPECS_LIST[0]

    def _boom():
        raise RuntimeError("mongo down")

    with patch.object(kb_vector_ingest, "_state_client", side_effect=_boom):
        assert kb_vector_ingest.get_last_watermark(spec) is None


def test_get_last_watermark_str_reads_string():
    spec = SPECS_LIST[0]
    fake_coll = MagicMock()
    fake_coll.find_one.return_value = {
        "_id": "x",
        "last_release_time_str": "2026-04-24 17:30",
    }
    fake_db = MagicMock()
    fake_db.__getitem__.return_value = fake_coll
    fake_client = MagicMock()
    fake_client.__getitem__.return_value = fake_db

    with patch.object(kb_vector_ingest, "_state_client", return_value=fake_client):
        assert kb_vector_ingest.get_last_watermark_str(spec) == "2026-04-24 17:30"


def test_get_last_watermark_str_none_when_missing():
    spec = SPECS_LIST[0]
    fake_coll = MagicMock()
    fake_coll.find_one.return_value = {"_id": "x"}  # no str field
    fake_db = MagicMock()
    fake_db.__getitem__.return_value = fake_coll
    fake_client = MagicMock()
    fake_client.__getitem__.return_value = fake_db

    with patch.object(kb_vector_ingest, "_state_client", return_value=fake_client):
        assert kb_vector_ingest.get_last_watermark_str(spec) is None


# ── Ingest loop picks the right watermark per spec ─────────────────


@pytest.mark.asyncio
async def test_run_one_cycle_passes_str_watermark_for_str_only_specs():
    """Specs without date_ms_field must receive since_str, not since_ms."""
    settings = get_settings()
    svc = KbVectorSyncService(settings)
    svc._running = True

    # Sanity: confirm at least one str-only spec exists in the real SPECS_LIST.
    str_only_specs = [s for s in SPECS_LIST if not s.date_ms_field]
    assert str_only_specs, "expected at least one spec with date_ms_field=None"

    seen_args: list[dict] = []

    async def fake_ingest(spec, **kwargs):
        seen_args.append({"spec": spec, **kwargs})
        return {"chunks_upserted": 0}

    with patch.object(kb_vector_sync, "get_last_watermark", return_value=111), \
         patch.object(kb_vector_sync, "get_last_watermark_str", return_value="2026-04-20 09:00"), \
         patch.object(kb_vector_sync, "ingest_collection", side_effect=fake_ingest), \
         patch.object(kb_vector_sync, "_INTER_SPEC_PAUSE_S", 0.0):
        await svc._run_one_cycle()

    for args in seen_args:
        spec = args["spec"]
        if spec.date_ms_field:
            assert args["since_ms"] == 111, f"{spec.collection}: want ms watermark"
            assert args["since_str"] is None, f"{spec.collection}: should not pass str"
        else:
            assert args["since_str"] == "2026-04-20 09:00", (
                f"{spec.collection}: want str watermark"
            )
            assert args["since_ms"] is None, f"{spec.collection}: should not pass ms"


# ── _should_start gate ─────────────────────────────────────────────


def test_should_start_honors_disabled_flag(monkeypatch):
    class _S:
        vector_sync_enabled = False
        is_staging = True

    monkeypatch.delenv("KB_VECTOR_SYNC_ALLOW_PROD", raising=False)
    start, reason = _should_start(_S())
    assert start is False
    assert "VECTOR_SYNC_ENABLED" in reason


def test_should_start_on_staging(monkeypatch):
    class _S:
        vector_sync_enabled = True
        is_staging = True

    monkeypatch.delenv("KB_VECTOR_SYNC_ALLOW_PROD", raising=False)
    start, reason = _should_start(_S())
    assert start is True
    assert "staging" in reason


def test_should_start_prod_default_off(monkeypatch):
    class _S:
        vector_sync_enabled = True
        is_staging = False

    monkeypatch.delenv("KB_VECTOR_SYNC_ALLOW_PROD", raising=False)
    start, reason = _should_start(_S())
    assert start is False


def test_should_start_prod_allow_override(monkeypatch):
    class _S:
        vector_sync_enabled = True
        is_staging = False

    monkeypatch.setenv("KB_VECTOR_SYNC_ALLOW_PROD", "1")
    start, reason = _should_start(_S())
    assert start is True
    assert "ALLOW_PROD" in reason


# ── One ingest cycle ───────────────────────────────────────────────


@pytest.mark.asyncio
async def test_run_one_cycle_iterates_all_specs_and_aggregates():
    """A single cycle calls ingest_collection for every spec exactly once,
    sums the returned stats, and swallows per-spec exceptions."""
    settings = get_settings()
    svc = KbVectorSyncService(settings)
    svc._running = True

    fake_stats = {
        "chunks_upserted": 5,
        "chunks_deleted": 1,
        "docs_unchanged": 2,
        "docs_errored": 0,
    }

    async def fake_ingest(spec, **kwargs):
        # The third spec throws — cycle should still continue.
        if spec is SPECS_LIST[2]:
            raise RuntimeError("simulated per-spec failure")
        return dict(fake_stats)

    with patch.object(kb_vector_sync, "get_last_watermark", return_value=None), \
         patch.object(kb_vector_sync, "ingest_collection", side_effect=fake_ingest), \
         patch.object(kb_vector_sync, "_INTER_SPEC_PAUSE_S", 0.0):
        totals = await svc._run_one_cycle()

    ok_specs = len(SPECS_LIST) - 1
    assert totals["specs_run"] == ok_specs
    assert totals["chunks_upserted"] == 5 * ok_specs
    assert totals["chunks_deleted"] == 1 * ok_specs
    assert totals["docs_unchanged"] == 2 * ok_specs


@pytest.mark.asyncio
async def test_run_one_cycle_respects_lease_skip():
    """Specs that report ``skipped_due_to_lease`` do not add to run totals."""
    settings = get_settings()
    svc = KbVectorSyncService(settings)
    svc._running = True

    async def fake_ingest(spec, **kwargs):
        return {"skipped_due_to_lease": 1}

    with patch.object(kb_vector_sync, "get_last_watermark", return_value=None), \
         patch.object(kb_vector_sync, "ingest_collection", side_effect=fake_ingest), \
         patch.object(kb_vector_sync, "_INTER_SPEC_PAUSE_S", 0.0):
        totals = await svc._run_one_cycle()

    assert totals["specs_run"] == 0
    assert totals["specs_skipped"] == len(SPECS_LIST)
    assert totals["chunks_upserted"] == 0


@pytest.mark.asyncio
async def test_run_one_cycle_cooperates_with_cancellation():
    """Flipping ``_running=False`` stops the cycle between specs."""
    settings = get_settings()
    svc = KbVectorSyncService(settings)
    svc._running = True

    call_count = 0

    async def fake_ingest(spec, **kwargs):
        nonlocal call_count
        call_count += 1
        # Stop mid-way.
        if call_count == 3:
            svc._running = False
        return {"chunks_upserted": 1}

    with patch.object(kb_vector_sync, "get_last_watermark", return_value=None), \
         patch.object(kb_vector_sync, "ingest_collection", side_effect=fake_ingest), \
         patch.object(kb_vector_sync, "_INTER_SPEC_PAUSE_S", 0.0):
        totals = await svc._run_one_cycle()

    # Stops after the 3rd spec runs (loop checks _running BEFORE each spec).
    assert call_count == 3
    assert totals["specs_run"] == 3


# ── Sweep cycle ────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_run_sweep_cycle_aggregates_tombstones_and_deletes():
    settings = get_settings()
    svc = KbVectorSyncService(settings)
    svc._running = True

    async def fake_sweep(spec, **kwargs):
        return {"tombstones": 2, "deleted": 2, "mongo_docs": 10, "milvus_docs": 12}

    with patch.object(kb_vector_sync, "sweep_deleted_docs", side_effect=fake_sweep), \
         patch.object(kb_vector_sync, "_INTER_SPEC_PAUSE_S", 0.0):
        totals = await svc._run_sweep_cycle()

    n = len(SPECS_LIST)
    assert totals["specs_run"] == n
    assert totals["tombstones"] == 2 * n
    assert totals["deleted"] == 2 * n


# ── start/stop lifecycle ───────────────────────────────────────────


@pytest.mark.asyncio
async def test_start_and_stop_cleanly(monkeypatch):
    """Starting creates two tasks; stopping cancels them without leaking."""
    settings = get_settings()
    svc = KbVectorSyncService(settings)

    # Replace the loops with no-ops so the test exits fast.
    async def _idle():
        while True:
            await asyncio.sleep(10)

    monkeypatch.setattr(svc, "_ingest_loop", _idle)
    monkeypatch.setattr(svc, "_sweep_loop", _idle)

    await svc.start()
    assert svc._ingest_task is not None and not svc._ingest_task.done()
    assert svc._sweep_task is not None and not svc._sweep_task.done()

    await svc.stop()
    assert svc._ingest_task is None
    assert svc._sweep_task is None
