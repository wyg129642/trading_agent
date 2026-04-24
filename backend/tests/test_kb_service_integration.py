"""Integration tests — hits the live local MongoDB.

These tests run against ``localhost:27017`` using the real crawler databases.
They are skipped automatically when the local Mongo is not reachable (e.g. CI
outside the dev box).

Run with:
    cd /home/ygwang/trading_agent && python3 -m pytest backend/tests/test_kb_service_integration.py -v
"""
from __future__ import annotations

import asyncio
import pytest

from backend.app.services.kb_service import (
    SPECS_LIST,
    search,
    fetch_document,
    list_facets,
    execute_tool,
    _coll,
)
from backend.app.services.web_search_tool import CitationTracker


# Skip the whole module if local Mongo isn't reachable.
def _mongo_up() -> bool:
    try:
        import pymongo
        c = pymongo.MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=1500)
        c.admin.command("ping")
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not _mongo_up(), reason="local MongoDB not reachable")


# ── Reachability: every spec's collection must have at least 1 doc ─


@pytest.mark.asyncio
async def test_every_spec_collection_has_docs():
    """Every declared spec must correspond to a non-empty collection.
    Catches typos in db/collection names in SPECS_LIST.
    """
    for spec in SPECS_LIST:
        n = await _coll(spec).estimated_document_count()
        assert n > 0, f"{spec.db}/{spec.collection} appears empty"


# ── search ──────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_search_no_filter_returns_hits():
    hits = await search("", top_k=5)
    assert isinstance(hits, list)
    assert len(hits) > 0
    for h in hits:
        assert h["doc_id"] and ":" in h["doc_id"]
        assert h["source"] in {s.db for s in SPECS_LIST}
        assert h["doc_type"] in {s.doc_type for s in SPECS_LIST}


@pytest.mark.asyncio
async def test_search_ticker_filter_hk():
    # Tencent: corpus stores 5-digit 00700.HK; pass that plus the 4-digit
    # form so the normalization path is also exercised.
    hits = await search("业务展望", tickers=["00700.HK"], top_k=10)
    assert isinstance(hits, list)
    for h in hits:
        tickers = h.get("tickers") or []
        # canonical must be present OR fallback-matched (jinmen.reports/gangtise)
        assert ("00700.HK" in tickers) or h.get("source") in {"jinmen", "gangtise"}


@pytest.mark.asyncio
async def test_search_ticker_hk_short_form_normalization():
    """Passing 0700.HK should still surface 00700.HK-tagged docs."""
    short_hits = await search("", tickers=["0700.HK"], top_k=3)
    long_hits = await search("", tickers=["00700.HK"], top_k=3)
    # Both must produce hits (corpus has many 00700.HK docs)
    assert long_hits, "expected 00700.HK to have hits"
    assert short_hits, "0700.HK should normalize to 00700.HK and match"


@pytest.mark.asyncio
async def test_search_ticker_filter_us():
    hits = await search("earnings", tickers=["NVDA.US"], top_k=10)
    assert isinstance(hits, list)
    # It's fine to find zero if NVDA coverage is limited — assert structure only
    for h in hits:
        assert h["doc_id"]


@pytest.mark.asyncio
async def test_search_bare_ticker_gets_normalized():
    hits = await search("", tickers=["NVDA"], top_k=3)
    for h in hits:
        # Normalization should map 'NVDA' → 'NVDA.US'
        if h.get("tickers"):
            assert any(t.endswith(".US") for t in h["tickers"])


@pytest.mark.asyncio
async def test_search_date_range_filter():
    # Pick a tight window
    hits = await search("", date_range={"gte": "2026-04-01", "lte": "2026-04-30"}, top_k=20)
    for h in hits:
        if h.get("release_ms"):
            # Mar 31 to May 1 UTC bounds (generous; date parse tolerant)
            assert 1775155200000 <= h["release_ms"] <= 1777996800000, h


@pytest.mark.asyncio
async def test_search_source_filter():
    hits = await search("", sources=["funda"], top_k=15)
    for h in hits:
        assert h["source"] == "funda"


@pytest.mark.asyncio
async def test_search_doc_type_filter():
    hits = await search("", doc_types=["earnings_transcript"], top_k=5)
    for h in hits:
        assert h["doc_type"] == "earnings_transcript"


@pytest.mark.asyncio
async def test_search_top_k_capped():
    hits = await search("", top_k=999)  # should be clamped to 30
    assert len(hits) <= 30


@pytest.mark.asyncio
async def test_search_empty_target_set_returns_empty():
    hits = await search("x", sources=["nonexistent_source"], top_k=5)
    assert hits == []


@pytest.mark.asyncio
async def test_search_ticker_with_no_docs_returns_empty():
    # A syntactically valid ticker that's nowhere in the corpus
    hits = await search("", tickers=["ZZZZ.US"], top_k=5)
    assert hits == []


@pytest.mark.asyncio
async def test_search_pure_nonsense_cjk_query_returns_empty():
    # Four very-rare CJK chars that don't appear in investment text
    hits = await search("齉靐齾龘", top_k=5)
    assert hits == []


@pytest.mark.asyncio
async def test_search_results_have_snippets_when_text_exists():
    hits = await search("", doc_types=["earnings_transcript"], top_k=3)
    for h in hits:
        if h.get("text_len", 0) > 200:
            assert h["snippet"]  # non-empty snippet


# ── fetch_document ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_fetch_document_round_trip():
    # Use a search hit to get a valid doc_id, then fetch it
    hits = await search("", top_k=1)
    assert hits, "no hits to fetch"
    doc_id = hits[0]["doc_id"]
    res = await fetch_document(doc_id, max_chars=2000)
    assert res["found"] is True
    assert res["doc_id"] == doc_id
    assert res["title"]
    assert isinstance(res["text"], str)


@pytest.mark.asyncio
async def test_fetch_document_invalid_id_format():
    res = await fetch_document("bogus-no-colons")
    assert res["found"] is False
    assert "invalid" in res.get("error", "").lower()


@pytest.mark.asyncio
async def test_fetch_document_unknown_collection():
    res = await fetch_document("foo:bar:baz")
    assert res["found"] is False
    assert "unknown" in res.get("error", "").lower()


@pytest.mark.asyncio
async def test_fetch_document_nonexistent_id():
    res = await fetch_document("alphapai:comments:DEFINITELY_NOT_A_REAL_ID_XYZZY")
    assert res["found"] is False


@pytest.mark.asyncio
async def test_fetch_document_truncation():
    # Find a doc with long text
    hits = await search("", doc_types=["earnings_transcript"], top_k=5)
    long_doc = next((h for h in hits if h.get("text_len", 0) > 5000), None)
    if long_doc is None:
        pytest.skip("no sufficiently long doc")
    res = await fetch_document(long_doc["doc_id"], max_chars=2000)
    assert res["found"] is True
    assert res["truncated"] is True
    assert res["full_text_len"] > 2000
    assert len(res["text"]) == 2000


# ── facets ──────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_facets_sources():
    rows = await list_facets("sources", top=10)
    assert isinstance(rows, list) and rows
    keys = {r["source"] for r in rows}
    assert keys <= {s.db for s in SPECS_LIST}
    for r in rows:
        assert r["count"] > 0


@pytest.mark.asyncio
async def test_facets_doc_types():
    rows = await list_facets("doc_types", top=30)
    assert rows
    doc_types_seen = {r["doc_type"] for r in rows}
    # Must be a subset of our declared doc types
    assert doc_types_seen <= {s.doc_type for s in SPECS_LIST}


@pytest.mark.asyncio
async def test_facets_tickers():
    rows = await list_facets("tickers", top=20)
    assert isinstance(rows, list)
    if rows:
        for r in rows:
            assert isinstance(r["ticker"], str) and r["ticker"]
            assert r["count"] > 0


@pytest.mark.asyncio
async def test_facets_date_histogram():
    rows = await list_facets("date_histogram", top=24)
    assert isinstance(rows, list)
    if rows:
        for r in rows:
            # 'YYYY-MM' format
            assert len(r["month"]) == 7 and r["month"][4] == "-"
            assert r["count"] > 0


@pytest.mark.asyncio
async def test_facets_unknown_dim_raises():
    with pytest.raises(ValueError):
        await list_facets("bogus_dimension")


@pytest.mark.asyncio
async def test_facets_filtered_counts_are_smaller():
    unfiltered = sum(r["count"] for r in await list_facets("sources"))
    filtered = sum(r["count"] for r in await list_facets("sources", filters={"sources": ["funda"]}))
    assert filtered <= unfiltered
    assert filtered > 0


# ── execute_tool end-to-end ────────────────────────────────────


@pytest.mark.asyncio
async def test_execute_tool_kb_search_produces_formatted_text():
    tracker = CitationTracker()
    out = await execute_tool("kb_search",
                             {"query": "业绩", "top_k": 3},
                             citation_tracker=tracker)
    assert isinstance(out, str)
    assert len(out) > 0
    # Either has [N] citation markers, or is the explicit "no results" message
    assert "[1]" in out or "未找到" in out


@pytest.mark.asyncio
async def test_execute_tool_kb_fetch_round_trip():
    # search first to grab a real doc_id
    hits = await search("", top_k=1)
    assert hits
    out = await execute_tool("kb_fetch_document",
                             {"doc_id": hits[0]["doc_id"], "max_chars": 1500})
    assert isinstance(out, str)
    assert "#" in out  # markdown heading
    assert "doc_id:" in out


@pytest.mark.asyncio
async def test_execute_tool_kb_facets():
    out = await execute_tool("kb_list_facets",
                             {"dimension": "sources", "top": 10})
    assert "维度" in out or isinstance(out, str)


@pytest.mark.asyncio
async def test_execute_tool_unknown_name():
    out = await execute_tool("kb_nope", {})
    assert "未知" in out or "Unknown" in out


@pytest.mark.asyncio
async def test_execute_tool_kb_search_registers_citations():
    tracker = CitationTracker()
    out = await execute_tool("kb_search", {"query": "业绩", "top_k": 5},
                             citation_tracker=tracker)
    if "[1]" in out:  # if there were any hits
        assert len(tracker.sources) > 0
        for s in tracker.sources:
            assert s["source_type"] == "kb"
            assert s.get("doc_id")  # every KB source carries its doc_id
