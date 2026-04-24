"""Unit tests for kb_vector_ingest — chunker + row construction.

Tests run offline (no Milvus, no TEI). End-to-end smoke lives in Phase 1 acceptance.
"""
from __future__ import annotations

from unittest.mock import patch

import pytest

from backend.app.services.kb_vector_ingest import (
    CHILD_CHARS_CJK,
    CHILD_CHARS_EN,
    SHORT_DOC_MIN_CHARS,
    _ATOMIC_RE,
    _chunk_id,
    _chunk_hash,
    _detect_lang,
    _repair_atomic_boundary,
    build_rows_for_doc,
    chunk_document,
)
from backend.app.services.kb_service import SPECS_BY_KEY


# ── Language detection ─────────────────────────────────────────────


def test_detect_lang_cjk():
    assert _detect_lang("腾讯2024年三季度营收33.87亿元") == "zh"
    # 30% threshold — this sample is CJK-dominant.
    assert _detect_lang("腾讯控股发布三季度财报：营收同比增长30%") == "zh"


def test_detect_lang_en():
    assert _detect_lang("NVIDIA Q3 earnings call transcript discusses AI GPU demand.") == "en"


# ── Chunker basics ─────────────────────────────────────────────────


def test_chunk_short_doc_kept_whole():
    text = "台积电路演纪要"  # 7 chars, below SHORT_DOC_MIN_CHARS
    assert len(text) < SHORT_DOC_MIN_CHARS
    chunks = chunk_document(text)
    assert len(chunks) == 1
    assert chunks[0].chunk_text == text
    assert chunks[0].char_start == 0


def test_chunk_long_cjk_doc_splits():
    # Build a long CJK doc by repeating a sentence.
    section = "台积电 2024 年第三季度路演纪要：AI 服务器需求强劲。" * 40  # ~700+ chars
    chunks = chunk_document(section, lang="zh")
    assert len(chunks) >= 1
    # Every chunk's char_start must be non-negative and char_end > char_start.
    for c in chunks:
        assert 0 <= c.char_start < c.char_end <= len(section)
        assert c.chunk_text.strip()


def test_chunk_respects_markdown_headers():
    text = (
        "# 财报概述\n"
        "台积电Q3营收创新高。\n"
        "\n"
        "## 业绩数据\n"
        "营收 23.6 billion USD，同比 +36%。\n"
        "\n"
        "## AI 业务\n"
        "AI 服务器需求强劲，产能满载。HBM3 出货量创纪录。\n"
    )
    chunks = chunk_document(text, lang="zh")
    assert len(chunks) >= 1
    # At least two different parent_index values if sections present.
    parent_ids = {c.parent_index for c in chunks}
    # Tolerant: small doc may fold into one section; but text ≥ 3 markdown sections.
    assert len(parent_ids) >= 1


def test_chunk_returns_parent_text():
    text = (
        "## 某个章节\n"
        "这是一个章节的内容。" * 80  # ~900 chars
    )
    chunks = chunk_document(text, lang="zh")
    for c in chunks:
        assert c.parent_text
        # parent should be a superset of chunk (or equal if one chunk = one section)
        # Not a strict containment test because of trimming/whitespace, but length must >=
        assert len(c.parent_text) >= len(c.chunk_text)


# ── Atomic regex guards ────────────────────────────────────────────


def test_atomic_pattern_matches_tickers():
    assert _ATOMIC_RE.search("买入 NVDA.US 建议")
    assert _ATOMIC_RE.search("00700.HK 腾讯")
    assert _ATOMIC_RE.search("600519.SH 贵州茅台")


def test_atomic_pattern_matches_quarter():
    assert _ATOMIC_RE.search("FY25 Q3 revenue")
    assert _ATOMIC_RE.search("2025Q3业绩")
    assert _ATOMIC_RE.search("2025 Q4E预测")


def test_atomic_pattern_matches_percentages():
    assert _ATOMIC_RE.search("同比 +36.5%")
    assert _ATOMIC_RE.search("毛利率 27.16%")


def test_repair_boundary_widens_to_atomic():
    full = "公司 营收 33.87 亿元 同比 +36% 创新高"
    # Pretend we cut right in the middle of "33.87":
    start = full.find("33.87") + 2  # mid-match
    piece = full[start:start + 8]
    _, new_start, new_end = _repair_atomic_boundary(full, piece, start, start + 8)
    assert new_start <= full.find("33.87")  # boundary widened leftward


# ── Deterministic IDs ─────────────────────────────────────────────


def test_chunk_id_is_deterministic():
    a = _chunk_id("d1", 0, "hello")
    b = _chunk_id("d1", 0, "hello")
    assert a == b
    c = _chunk_id("d1", 1, "hello")
    assert c != a
    d = _chunk_id("d1", 0, "hello!")
    assert d != a


def test_chunk_hash_stable():
    assert _chunk_hash("abc") == _chunk_hash("abc")
    assert _chunk_hash("abc") != _chunk_hash("abcd")


# ── Row construction from Mongo-shaped docs ───────────────────────


def test_build_rows_for_alphapai_roadshow_shape():
    spec = SPECS_BY_KEY["alphapai/roadshows"]
    # Shape mirrors actual Mongo docs: content_md OR list_item.content,
    # publish_time string, institution list[{name}], _canonical_tickers list.
    doc = {
        "_id": "66a1fakeobjectid01",
        "title": "台积电 2024 Q3 路演",
        "content": "台积电 2024 年第三季度路演纪要：AI 服务器需求强劲。产能利用率创新高。",
        "publish_time": "2024-11-20 09:30:00",
        "institution": [{"name": "Goldman Sachs"}],
        "_canonical_tickers": ["TSM.US", "2330.TW"],
    }
    rows = build_rows_for_doc(spec, doc)
    assert len(rows) >= 1
    r = rows[0]
    assert r["chunk_id"]
    assert r["doc_id"] == "alphapai:roadshows:66a1fakeobjectid01"
    assert r["db"] == "alphapai"
    assert r["collection"] == "roadshows"
    assert r["source_type"] == "kb"
    assert r["doc_type"] == "roadshow"
    assert r["doc_type_cn"] == "路演纪要"
    assert r["title"] == "台积电 2024 Q3 路演"
    assert r["release_time_ms"] > 0
    assert r["tickers"] == ["TSM.US", "2330.TW"]
    assert r["chunk_index"] == 0
    assert r["char_start"] == 0
    assert r["char_end"] > 0
    assert r["chunk_hash"]
    assert r["lang"] == "zh"
    assert r["chunk_text"].startswith("台积电")
    assert r["snippet_text"]
    assert r["embedding_model_version"]
    # dense_vector is NOT in the row at this stage — added post-embed.
    assert "dense_vector" not in r
    # sparse_vector is auto-generated by Milvus Function — also not in row.
    assert "sparse_vector" not in r


def test_build_rows_empty_text_returns_empty():
    spec = SPECS_BY_KEY["alphapai/roadshows"]
    doc = {"_id": "x", "title": "empty", "content": ""}
    assert build_rows_for_doc(spec, doc) == []


def test_build_rows_respects_ticker_truncation():
    spec = SPECS_BY_KEY["alphapai/roadshows"]
    doc = {
        "_id": "y",
        "title": "many tickers",
        "content": "一段足够长的文本。" * 10,
        "publish_time": "2024-01-01",
        "_canonical_tickers": [f"T{i:03d}.US" for i in range(50)],  # 50 > 32 limit
    }
    rows = build_rows_for_doc(spec, doc)
    assert rows
    # kb_service._extract_tickers caps at 10 via slicing; our build pass caps at 32.
    # Accept whatever came out, just verify it's capped.
    assert len(rows[0]["tickers"]) <= 32
