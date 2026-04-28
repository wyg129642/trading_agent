"""Regression tests for the kb_search / user_kb_search dedup pipeline.

Covers the four fixes from `fuzzy-puzzling-tiger.md`:

* P1a — `_merge_hybrid_hits` score accumulation (vector loop must += not =).
* P1b — chunk-id keying with per-doc cap applied AFTER sort.
* P2  — CitationTracker emit suppression for cross-call repeats.
* P3  — `(institution, normalized_title, day)` mirror fold.

These are pure-logic tests — no Mongo/Milvus/TEI. They isolate the dedup
math from the IO so any regression shows up loud.
"""
from __future__ import annotations

import os

import pytest

# Force-disable the lifespan loop init since it would try to start a Mongo
# connection during import.
os.environ.setdefault("APP_ENV", "staging")
os.environ.setdefault("KB_NORMALIZE_ENABLED", "false")

from backend.app.services.kb_service import (  # noqa: E402
    _collapse_mirrors,
    _merge_hybrid_hits,
    _normalize_institution,
    _normalize_title,
)
from backend.app.services.user_kb_service import (  # noqa: E402
    SearchHit,
    _apply_per_doc_cap,
    _collapse_by_content_hash,
)
from backend.app.services.web_search_tool import CitationTracker  # noqa: E402


# ── P1a/P1b: hybrid merge ───────────────────────────────────────


def test_merge_hybrid_hits_score_accumulation_vector_path():
    """Multi-chunk vector hits must accumulate score (not overwrite).

    Regression of the `score_sum[did] = …` bug at kb_service.py:1128 —
    before P1a the vector loop was `=` instead of `+=`, so 3 chunks of
    one doc kept only the worst-ranked chunk's score.
    """
    # Doc A returns 3 chunks at top vec ranks; Doc B returns one chunk far
    # down the list. Pre-P1a the worst-ranked chunk of A would set the
    # score, putting B ahead of A. After P1a, A keeps its top rank.
    vec = [
        {"doc_id": "g:r:A", "chunk_id": "A:1"},
        {"doc_id": "g:r:A", "chunk_id": "A:2"},
        {"doc_id": "g:r:A", "chunk_id": "A:3"},
        {"doc_id": "a:r:B", "chunk_id": "B:1"},
    ]
    merged, _ = _merge_hybrid_hits(vec, [], top_k=10, per_doc_cap=3)
    # All 3 chunks of A should be present and lead B.
    a_positions = [i for i, h in enumerate(merged) if h["doc_id"] == "g:r:A"]
    b_positions = [i for i, h in enumerate(merged) if h["doc_id"] == "a:r:B"]
    assert len(a_positions) == 3
    assert b_positions[0] > max(a_positions), \
        "B at vec rank 4 should land below A's chunks (1,2,3)"


def test_per_doc_cap_after_sort():
    """Per-doc cap is applied after RRF sort, not pre-fusion."""
    vec = [{"doc_id": "g:r:A", "chunk_id": f"A:{i}"} for i in range(1, 6)]
    vec.append({"doc_id": "a:r:B", "chunk_id": "B:1"})
    merged, stats = _merge_hybrid_hits(vec, [], top_k=10, per_doc_cap=2)
    a_count = sum(1 for h in merged if h["doc_id"] == "g:r:A")
    assert a_count == 2, f"per_doc_cap=2 should keep 2 chunks of A, got {a_count}"
    assert stats["collapsed_by_doc"] == 3  # A:3, A:4, A:5
    # B must still surface even though A had way more chunks
    assert any(h["doc_id"] == "a:r:B" for h in merged)


def test_keyword_boost_distributes_across_vec_chunks():
    """A kw-side hit on doc A should lift every surfaced vec chunk of A."""
    vec = [
        {"doc_id": "g:r:A", "chunk_id": "A:1"},
        {"doc_id": "g:r:A", "chunk_id": "A:2"},
        {"doc_id": "a:r:B", "chunk_id": "B:1"},  # vec rank 3
    ]
    kw = [{"doc_id": "g:r:A"}]  # kw rank 1, lifts both A:1 and A:2
    merged, _ = _merge_hybrid_hits(vec, kw, top_k=10, per_doc_cap=2)
    # A's chunks should report dual-engine coverage; B should remain vector-only.
    a_engines = [h.get("_engines") for h in merged if h["doc_id"] == "g:r:A"]
    b_engines = next(h.get("_engines") for h in merged if h["doc_id"] == "a:r:B")
    assert all("keyword" in eng and "vector" in eng for eng in a_engines)
    assert b_engines == ["vector"]


def test_merge_handles_empty_inputs():
    merged, stats = _merge_hybrid_hits([], [], top_k=10)
    assert merged == []
    assert stats == {"after_score_merge": 0, "collapsed_by_doc": 0}


# ── P3: title / institution normalization + mirror fold ────────


def test_normalize_title_collapses_punctuation_and_spacing():
    """Cross-platform title variants (different dashes, spacing) collapse."""
    a = _normalize_title("中信证券 - 阿里巴巴 - 2026Q1 业绩点评！")
    b = _normalize_title("中信证券——阿里巴巴——2026Q1 业绩点评")
    c = _normalize_title("中信证券-阿里巴巴-2026Q1业绩点评")
    assert a == b == c, f"expected collapse, got {a!r} / {b!r} / {c!r}"


def test_normalize_title_does_not_collapse_distinct_titles():
    """Negative test: legitimately different titles must NOT collapse."""
    a = _normalize_title("本周观察")
    b = _normalize_title("阿里 Q1 业绩")
    assert a != b


def test_normalize_institution_alias_table():
    """Institution aliases resolve cross-language and short-form to canonical."""
    assert _normalize_institution("中信证券") == "CITICS"
    assert _normalize_institution("citic securities") == "CITICS"
    assert _normalize_institution("中信") == "CITICS"
    # Unknown brokerage falls back to lowercased raw — preserves matching at
    # least within the same platform's literal name.
    assert _normalize_institution("Unknown LLC") == "unknown llc"


def test_collapse_mirrors_cross_platform_brokerage_report():
    """Same (inst, title, day) across 3 platforms collapses to 1."""
    hits = [
        {"doc_id": "g:r:X1", "_normalized_title": "q1",
         "_inst_normalized": "CITICS", "date": "2026-04-20", "source": "gangtise"},
        {"doc_id": "a:r:X2", "_normalized_title": "q1",
         "_inst_normalized": "CITICS", "date": "2026-04-20", "source": "alphapai"},
        {"doc_id": "ae:c:X3", "_normalized_title": "q1",
         "_inst_normalized": "CITICS", "date": "2026-04-20", "source": "alphaengine"},
    ]
    out, n = _collapse_mirrors(hits, enabled=True)
    assert n == 2
    assert len(out) == 1
    assert out[0]["_mirror_count"] == 3
    assert sorted(out[0]["_mirror_sources"]) == ["alphaengine", "alphapai", "gangtise"]


def test_collapse_mirrors_does_not_fold_different_brokerages():
    """Three brokerages publishing same-titled '本周观察' on the same day
    must stay as 3 separate hits — institution disambiguates them.
    """
    hits = [
        {"doc_id": f"g:r:{i}", "_normalized_title": "本周观察",
         "_inst_normalized": inst, "date": "2026-04-20", "source": "gangtise"}
        for i, inst in enumerate(["CITICS", "HUATAI", "CICC"])
    ]
    out, n = _collapse_mirrors(hits, enabled=True)
    assert n == 0
    assert len(out) == 3


def test_collapse_mirrors_skips_rows_with_missing_key_components():
    """If any of the 3 key components is empty, the row is passed through.

    Defends against accidental over-collapsing on docs that haven't been
    backfilled by kb_normalize_loop yet.
    """
    hits = [
        # Missing inst → cannot fold even if title+day match
        {"doc_id": "g:r:Z1", "_normalized_title": "q1",
         "_inst_normalized": "", "date": "2026-04-20"},
        {"doc_id": "g:r:Z2", "_normalized_title": "q1",
         "_inst_normalized": "", "date": "2026-04-20"},
    ]
    out, n = _collapse_mirrors(hits, enabled=True)
    assert n == 0
    assert len(out) == 2


def test_collapse_mirrors_disabled_passthrough():
    hits = [{"doc_id": "x", "_normalized_title": "q",
             "_inst_normalized": "Y", "date": "2026-04-20"}] * 3
    out, n = _collapse_mirrors(hits, enabled=False)
    assert out is hits
    assert n == 0


# ── P2: CitationTracker emit suppression ───────────────────────


def test_citation_tracker_emit_suppression_helpers():
    """The new helpers form a contract: mark_chunk_emitted then
    is_chunk_already_emitted returns True."""
    t = CitationTracker()
    assert t.is_chunk_already_emitted("kb:chunk:abc") is False
    t.mark_chunk_emitted("kb:chunk:abc")
    assert t.is_chunk_already_emitted("kb:chunk:abc") is True
    # Unrelated key → False
    assert t.is_chunk_already_emitted("kb:chunk:xyz") is False


def test_citation_tracker_emit_suppression_handles_empty_key():
    """Empty key must not pollute the seen-set."""
    t = CitationTracker()
    t.mark_chunk_emitted("")
    assert t.is_chunk_already_emitted("") is False


def test_citation_tracker_add_kb_items_unchanged_contract():
    """add_kb_items still returns one entry per input with shared indices.

    The cross-call suppression is at the formatter level — `add_kb_items`
    semantics must not regress.
    """
    t = CitationTracker()
    items = [
        {"chunk_id": "abc", "doc_id": "X", "title": "a"},
        {"chunk_id": "abc", "doc_id": "X", "title": "a"},  # dup
        {"chunk_id": "def", "doc_id": "Y", "title": "b"},
    ]
    indexed = t.add_kb_items(items)
    assert len(indexed) == 3
    assert indexed[0]["citation_index"] == indexed[1]["citation_index"]
    assert indexed[2]["citation_index"] != indexed[0]["citation_index"]


# ── P4: user_kb content_hash fold + per-doc cap ────────────────


def _hit(doc_id: str, ch: str = "", uploader: str = "u1",
         chunk_index: int = 0) -> SearchHit:
    return SearchHit(
        document_id=doc_id, title=f"t-{doc_id}", original_filename=f"{doc_id}.pdf",
        chunk_index=chunk_index, text="body", created_at="2026-04-20T00:00:00",
        uploader_user_id=uploader, content_hash=ch,
    )


def test_user_kb_content_hash_collapse_across_users():
    """Two users uploading the same bytes → 1 hit with also_uploaded_by_count=2."""
    h1 = _hit("doc-1", ch="hash-A", uploader="alice")
    h2 = _hit("doc-2", ch="hash-A", uploader="bob")
    h3 = _hit("doc-3", ch="hash-B", uploader="alice")
    out, n = _collapse_by_content_hash([h1, h2, h3])
    assert n == 1
    assert len(out) == 2
    rep = next(h for h in out if h.content_hash == "hash-A")
    assert rep.also_uploaded_by_count == 2
    other = next(h for h in out if h.content_hash == "hash-B")
    assert other.also_uploaded_by_count == 1


def test_user_kb_content_hash_skips_empty_hash():
    """Old rows without content_hash must not collapse to one giant cluster."""
    h1 = _hit("doc-1", ch="", uploader="alice")
    h2 = _hit("doc-2", ch="", uploader="bob")
    out, n = _collapse_by_content_hash([h1, h2])
    assert n == 0
    assert len(out) == 2


def test_user_kb_per_doc_cap():
    """A 5-chunk PDF must not occupy all top_k=5 slots."""
    hits = [_hit("doc-1", chunk_index=i) for i in range(5)]
    hits.append(_hit("doc-2", chunk_index=0))
    out, collapsed = _apply_per_doc_cap(hits, cap=2)
    assert collapsed == 3
    doc1_count = sum(1 for h in out if h.document_id == "doc-1")
    assert doc1_count == 2
    assert any(h.document_id == "doc-2" for h in out)
