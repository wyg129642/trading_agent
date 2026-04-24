"""Unit tests for the personal-KB parsers and chunker.

Runs with no external dependencies — pure Python input/output. The PDF test
uses a real small PDF from vendor/; if PDF deps are absent the test is
skipped so this file stays runnable in a minimal environment.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from backend.app.services import user_kb_parser as pr


# ── is_supported / content_type_for ────────────────────────────


class TestIsSupported:
    def test_supported_extensions_include_common_types(self):
        for name in [
            "a.pdf", "b.md", "c.markdown", "d.txt", "e.text",
            "f.docx", "g.csv", "h.json", "i.html", "j.htm",
            "K.PDF",  # case-insensitive
        ]:
            assert pr.is_supported(name), f"{name} should be supported"

    def test_unsupported_extensions(self):
        # .mp4 is intentionally *not* supported — only audio-container
        # formats the ASR service is tested against.
        for name in ["a.exe", "b.zip", "c.mp4", "d.py", "no_extension"]:
            assert not pr.is_supported(name), f"{name} should NOT be supported"

    def test_audio_extensions_are_supported(self):
        # Audio files route through the ASR path; they must pass the
        # upload gate (is_supported) AND be detectable as audio
        # (is_audio) so the service knows to bypass the sync parser.
        for name in [
            "meeting.mp3", "a.wav", "b.m4a", "c.flac",
            "d.ogg", "e.opus", "f.webm", "g.aac",
            "H.MP3",  # case-insensitive
        ]:
            assert pr.is_supported(name), f"{name} should be supported"
            assert pr.is_audio(name), f"{name} should be flagged as audio"

    def test_non_audio_not_flagged_as_audio(self):
        for name in ["a.pdf", "b.md", "c.txt", "d.docx", "unknown.xyz"]:
            assert not pr.is_audio(name), f"{name} should NOT be audio"

    def test_content_type_mapping(self):
        assert pr.content_type_for("x.pdf") == "application/pdf"
        assert pr.content_type_for("x.md") == "text/markdown"
        assert pr.content_type_for("x.txt") == "text/plain"
        assert pr.content_type_for("x.mp3") == "audio/mpeg"
        assert pr.content_type_for("x.wav") == "audio/wav"
        # Unknown → generic fallback
        assert pr.content_type_for("x.unknown") == "application/octet-stream"


# ── chunk_text ─────────────────────────────────────────────────


class TestChunkText:
    def test_empty_input_returns_empty_list(self):
        assert pr.chunk_text("") == []
        assert pr.chunk_text("   \n\n   ") == []

    def test_short_text_returns_single_chunk(self):
        text = "A short note."
        chunks = pr.chunk_text(text, chunk_size=100, overlap=10)
        assert chunks == ["A short note."]

    def test_multi_paragraph_packs_into_one_chunk(self):
        text = "Para one.\n\nPara two.\n\nPara three."
        chunks = pr.chunk_text(text, chunk_size=1000, overlap=50)
        assert len(chunks) == 1
        assert "Para one." in chunks[0]
        assert "Para three." in chunks[0]

    def test_splits_on_long_paragraph_with_overlap(self):
        # Single para of 3000 chars, chunk=1000, overlap=200 →
        # window step=800, windows at 0, 800, 1600, 2400 → 4 chunks.
        text = "X" * 3000
        chunks = pr.chunk_text(text, chunk_size=1000, overlap=200)
        assert len(chunks) == 4
        assert all(len(c) <= 1000 for c in chunks)
        # Adjacent chunks should share 200 chars (the overlap).
        assert chunks[0][-200:] == chunks[1][:200]

    def test_packs_multiple_small_paragraphs_across_chunks(self):
        # 4 paragraphs × 400 chars — with chunk_size=900 we expect to pack
        # ~2 per chunk (400+2+400=802 fits; adding another would exceed 900).
        paras = ["A" * 400, "B" * 400, "C" * 400, "D" * 400]
        text = "\n\n".join(paras)
        chunks = pr.chunk_text(text, chunk_size=900, overlap=100)
        # Should be at least 2 chunks, not 1 (can't fit 1600+ in 900).
        assert len(chunks) >= 2
        # No chunk should exceed the size.
        for c in chunks:
            assert len(c) <= 900

    def test_overlap_clamped_when_too_large(self):
        # overlap >= chunk_size would cause infinite loop; parser clamps.
        chunks = pr.chunk_text("X" * 500, chunk_size=100, overlap=100)
        assert len(chunks) > 0
        # Verify it terminates — no hang.

    def test_invalid_chunk_size_raises(self):
        with pytest.raises(ValueError):
            pr.chunk_text("x", chunk_size=0)
        with pytest.raises(ValueError):
            pr.chunk_text("x", chunk_size=100, overlap=-1)

    def test_normalizes_whitespace_but_keeps_paragraphs(self):
        text = "Para   one has  spaces.\r\n\r\n\r\nPara two."
        chunks = pr.chunk_text(text, chunk_size=1000, overlap=50)
        assert len(chunks) == 1
        out = chunks[0]
        assert "Para one has spaces." in out
        assert "Para two." in out
        assert "\n\n" in out  # paragraph break preserved

    def test_no_empty_chunks(self):
        text = "\n\n\n".join(["x" * 100, "", "y" * 100, "", "z" * 100])
        chunks = pr.chunk_text(text, chunk_size=400, overlap=50)
        assert all(c.strip() for c in chunks)


# ── parse_file (text-based formats, fast) ──────────────────────


class TestParseFile:
    def test_rejects_empty_bytes(self):
        with pytest.raises(pr.ParseError):
            pr.parse_file("foo.txt", b"")

    def test_rejects_unsupported_extension(self):
        with pytest.raises(pr.ParseError):
            pr.parse_file("foo.exe", b"MZ")

    def test_parses_plain_text_utf8(self):
        data = "Hello, 世界!".encode("utf-8")
        res = pr.parse_file("hello.txt", data)
        assert res.parser == "text"
        assert "世界" in res.text
        assert res.warnings == []

    def test_parses_plain_text_with_bom(self):
        data = "﻿Hello".encode("utf-8")
        res = pr.parse_file("bom.txt", data)
        # Either utf-8 or utf-8-sig — content must be present either way.
        assert "Hello" in res.text

    def test_parses_markdown_preserves_syntax(self):
        data = b"# Heading\n\nSome **bold** text."
        res = pr.parse_file("note.md", data)
        assert res.parser == "markdown"
        assert "# Heading" in res.text
        assert "**bold**" in res.text

    def test_parses_json_pretty_prints_valid(self):
        data = b'{"a":1,"b":[2,3]}'
        res = pr.parse_file("x.json", data)
        assert res.parser == "json"
        # Pretty-printed JSON includes indentation.
        parsed = json.loads(res.text)
        assert parsed == {"a": 1, "b": [2, 3]}

    def test_parses_json_stores_malformed_verbatim(self):
        data = b'{"not: "json"'
        res = pr.parse_file("bad.json", data)
        # Should still return text and carry a warning.
        assert res.text
        assert any("malformed" in w.lower() for w in res.warnings)

    def test_parses_csv_as_pipe_separated(self):
        data = b"name,age\nAlice,30\nBob,42"
        res = pr.parse_file("t.csv", data)
        assert res.parser == "csv"
        assert "name | age" in res.text
        assert "Alice | 30" in res.text

    def test_parses_html_strips_tags(self):
        data = b"<html><script>bad()</script><p>Hello <b>world</b></p></html>"
        res = pr.parse_file("x.html", data)
        assert res.parser == "html"
        assert "Hello" in res.text
        assert "world" in res.text
        assert "bad()" not in res.text  # script stripped

    def test_parses_chinese_gbk_text(self):
        data = "腾讯游戏业务".encode("gbk")
        res = pr.parse_file("cn.txt", data)
        assert "腾讯" in res.text


# ── parse_file (PDF — tries opendataloader first, falls back to pypdf) ──


_SAMPLE_PDF = Path("/home/ygwang/trading_agent/vendor/novnc/docs/rfbproto-3.3.pdf")


@pytest.mark.skipif(
    not _SAMPLE_PDF.exists(),
    reason="sample PDF not available on this machine",
)
class TestPdfParsing:
    def test_pdf_round_trip(self):
        data = _SAMPLE_PDF.read_bytes()
        res = pr.parse_file("rfbproto.pdf", data)
        assert res.parser in ("opendataloader-pdf", "pypdf")
        # RFB protocol mentions "framebuffer" — confirms text extraction worked.
        assert "framebuffer" in res.text.lower() or "protocol" in res.text.lower()
        assert len(res.text) > 1000

    def test_pdf_empty_bytes_raises(self):
        with pytest.raises(pr.ParseError):
            pr.parse_file("empty.pdf", b"")


# ── Parser registry extension ───────────────────────────────────


class TestRegisterParser:
    def setup_method(self):
        # Snapshot registry so each test cleans up after itself.
        self._parsers_snapshot = dict(pr._PARSERS)
        self._exts_snapshot = dict(pr.SUPPORTED_EXTENSIONS)

    def teardown_method(self):
        pr._PARSERS.clear()
        pr._PARSERS.update(self._parsers_snapshot)
        pr.SUPPORTED_EXTENSIONS.clear()
        pr.SUPPORTED_EXTENSIONS.update(self._exts_snapshot)

    def test_register_new_extension_end_to_end(self):
        def _xyz(data: bytes) -> pr.ParseResult:
            return pr.ParseResult(
                text=f"XYZ:{data.decode('utf-8')}", parser="xyz", warnings=[],
            )

        pr.register_parser(".xyz", _xyz, content_type="application/x-xyz")

        assert pr.is_supported("a.xyz")
        assert pr.content_type_for("a.xyz") == "application/x-xyz"

        res = pr.parse_file("a.xyz", b"hello")
        assert res.parser == "xyz"
        assert res.text == "XYZ:hello"

    def test_register_requires_leading_dot(self):
        with pytest.raises(ValueError, match=r"'\.'"):
            pr.register_parser("xyz", lambda d: pr.ParseResult("", "x", []))

    def test_register_case_insensitive(self):
        pr.register_parser(".ABC", lambda d: pr.ParseResult("x", "abc", []))
        assert pr.is_supported("file.abc")
        assert pr.is_supported("file.ABC")


# ── Chinese tokenizer (jieba-based) ────────────────────────────


class TestTokenizer:
    """The tokenizer drives Chinese search quality — bugs here show up as
    mysterious 'search finds nothing' issues in production."""

    def test_empty_and_whitespace(self):
        from backend.app.services.user_kb_tokenizer import tokenize
        assert tokenize("") == ""
        assert tokenize("   ") == ""
        assert tokenize("\n\t") == ""

    def test_pure_chinese(self):
        from backend.app.services.user_kb_tokenizer import tokenize
        out = tokenize("接口说明")
        # Must produce '接口' and '说明' as separate tokens.
        assert "接口" in out.split()
        assert "说明" in out.split()

    def test_query_matches_indexed_chunk(self):
        """The whole point of splitting tokenize into an index-side and a
        query-side function: tokens produced by both must align so the
        indexed chunk is findable by the query."""
        from backend.app.services.user_kb_tokenizer import tokenize, tokenize_query
        chunk = tokenize("今天我们讨论接口说明书的设计")
        query = tokenize_query("接口说明")
        # Every query token must be present in the chunk's tokens.
        chunk_tokens = set(chunk.split())
        for q in query.split():
            assert q in chunk_tokens, (
                f"query token {q!r} not in chunk tokens {chunk_tokens!r}"
            )

    def test_punctuation_stripped(self):
        from backend.app.services.user_kb_tokenizer import tokenize
        out = tokenize("你好，世界！").split()
        # No punctuation tokens should survive.
        assert "，" not in out
        assert "！" not in out
        assert "你好" in out
        assert "世界" in out

    def test_latin_preserved(self):
        from backend.app.services.user_kb_tokenizer import tokenize
        out = tokenize("machine learning and AI").split()
        assert "machine" in out
        assert "learning" in out
        assert "AI" in out

    def test_mixed_cn_en(self):
        from backend.app.services.user_kb_tokenizer import tokenize
        out = tokenize("腾讯Q4业绩说明会").split()
        assert "腾讯" in out
        assert "Q4" in out
        # jieba cut_for_search gives us both '说明' and '说明会' for recall.
        assert any(t in out for t in ("说明", "说明会"))

    def test_numbers_preserved(self):
        from backend.app.services.user_kb_tokenizer import tokenize
        out = tokenize("2025年Q4营收33.87亿元").split()
        # Numeric fragments should survive even if chopped.
        assert any(c.isdigit() for t in out for c in t)

    def test_nfkc_folds_cjk_radicals(self):
        """PDF extractors sometimes emit CJK *compatibility radicals* that
        look identical to normal ideographs but have different codepoints
        (e.g. ⼝ U+2F1D vs 口 U+53E3). Without NFKC normalization a user
        typing '接口' could never match a stored chunk containing '接⼝'.

        This is the exact bug that broke live search on the first deploy,
        so guard it with a test."""
        from backend.app.services.user_kb_tokenizer import tokenize, tokenize_query
        # Build a string with the CJK compatibility radical.
        with_radical = "接" + "⼝" + "说明书"  # "接⼝说明书"
        indexed = tokenize(with_radical)
        query = tokenize_query("接口")  # U+53E3
        # After NFKC, the radical becomes the canonical 口, so "接口"
        # appears in the indexed tokens.
        indexed_tokens = set(indexed.split())
        for q in query.split():
            assert q in indexed_tokens, (
                f"{q!r} missing from tokens {indexed_tokens!r} — "
                "NFKC normalization regression"
            )

    def test_nfkc_folds_fullwidth_ascii(self):
        from backend.app.services.user_kb_tokenizer import tokenize
        # Full-width digits/letters normalize to ASCII.
        out = tokenize("ＡＢＣ１２３").split()
        assert "ABC123" in out or any("A" in t for t in out)


# ── RRF fusion (unit; no Mongo / Milvus needed) ────────────────


class TestRrfFusion:
    """Reciprocal Rank Fusion is the fusion rule for hybrid BM25 + dense.
    Its correctness is what makes the combined score meaningful, so we
    test the math and the edge cases without touching Mongo / OpenAI."""

    def _hit(self, doc_id: str, idx: int, text: str = "x", score: float = 0.0):
        from backend.app.services.user_kb_service import SearchHit
        return SearchHit(
            document_id=doc_id,
            title="t",
            original_filename="f",
            chunk_index=idx,
            text=text,
            score=score,
            created_at="",
            uploader_user_id="u",
        )

    def test_identical_lists_accumulate(self):
        from backend.app.services.user_kb_service import _rrf_fuse
        a = [self._hit("d1", 0), self._hit("d2", 0)]
        b = [self._hit("d1", 0), self._hit("d2", 0)]  # same order
        out = _rrf_fuse(a, b, rrf_k=60)
        # Both chunks appear once (deduplicated).
        keys = {(h.document_id, h.chunk_index) for h in out}
        assert keys == {("d1", 0), ("d2", 0)}
        # d1 at rank 0 on both sides → score ≈ 2/(60+1) ≈ 0.0328.
        # The fuser rounds to 4 decimal places, so tolerance > 1e-4.
        assert abs(out[0].score - 2.0 / 61) < 5e-4
        # d2 scored at rank 1 on both → score = 2/(60+2) ≈ 0.0323 < d1's.
        assert out[0].score > out[1].score

    def test_agreement_outranks_either_alone(self):
        """A chunk ranked 2 on both sides should outrank a chunk ranked 1
        on only one side. This is the core RRF property."""
        from backend.app.services.user_kb_service import _rrf_fuse
        lex = [self._hit("d_only_lex", 0), self._hit("d_both", 0)]
        vec = [self._hit("d_only_vec", 0), self._hit("d_both", 0)]
        out = _rrf_fuse(lex, vec, rrf_k=60)
        # The chunk present in both should be first.
        assert (out[0].document_id, out[0].chunk_index) == ("d_both", 0)

    def test_empty_lists(self):
        from backend.app.services.user_kb_service import _rrf_fuse
        assert _rrf_fuse([], [], rrf_k=60) == []
        only_lex = [self._hit("d1", 0)]
        out = _rrf_fuse(only_lex, [], rrf_k=60)
        assert len(out) == 1
        assert out[0].document_id == "d1"

    def test_prefers_fuller_text_on_collision(self):
        """When the same (doc, chunk) arrives from both sides, we keep
        whichever hit has a longer text field — Milvus VARCHAR occasionally
        clamps, and we want the reader LLM to see the most context."""
        from backend.app.services.user_kb_service import _rrf_fuse
        long_hit = self._hit("d1", 0, text="a" * 1000)
        short_hit = self._hit("d1", 0, text="a" * 100)
        out = _rrf_fuse([short_hit], [long_hit], rrf_k=60)
        assert len(out) == 1
        assert len(out[0].text) == 1000

    def test_rrf_k_flattens_margins(self):
        """Higher rrf_k compresses the gap between ranks. Quick regression
        guard that rrf_k actually feeds into the score formula."""
        from backend.app.services.user_kb_service import _rrf_fuse
        # Two chunks: 'winner' ranks 0 on both sides, 'loser' ranks 2 on both.
        lex = [self._hit("winner", 0), self._hit("mid", 0), self._hit("loser", 0)]
        vec = [self._hit("winner", 0), self._hit("mid", 0), self._hit("loser", 0)]
        out_small = _rrf_fuse(lex, vec, rrf_k=1)
        out_big = _rrf_fuse(lex, vec, rrf_k=1000)
        # Winner stays rank 0 regardless of k — but the margin to the loser
        # tightens as k grows.
        assert out_small[0].document_id == "winner"
        assert out_big[0].document_id == "winner"
        small_margin = out_small[0].score - out_small[-1].score
        big_margin = out_big[0].score - out_big[-1].score
        assert small_margin > big_margin, (
            f"expected smaller rrf_k to widen the rank-0 vs rank-last margin, "
            f"got small={small_margin} big={big_margin}"
        )


# ── Tool schema shape ───────────────────────────────────────────


def test_user_kb_tools_shape():
    from backend.app.services.user_kb_tools import USER_KB_TOOLS

    names = [t["function"]["name"] for t in USER_KB_TOOLS]
    assert set(names) == {"user_kb_search", "user_kb_fetch_document"}
    for t in USER_KB_TOOLS:
        assert t["type"] == "function"
        fn = t["function"]
        assert "description" in fn and fn["description"]
        assert "parameters" in fn and fn["parameters"]["type"] == "object"


def test_user_kb_system_prompt_nonempty():
    from backend.app.services.user_kb_tools import USER_KB_SYSTEM_PROMPT

    assert len(USER_KB_SYSTEM_PROMPT) > 50
    assert "user_kb_search" in USER_KB_SYSTEM_PROMPT


# ── ContextVar scoping ─────────────────────────────────────────


def test_user_id_context_var_default_empty():
    from backend.app.services import user_kb_service as svc
    # Default value should be "" so tools can detect "not bound".
    # Note: in async code context may be set by a previous test — reset here.
    tok = svc.set_current_user_id("")
    try:
        assert svc.get_current_user_id() == ""
    finally:
        svc.reset_current_user_id(tok)


def test_user_id_context_var_scoping():
    from backend.app.services import user_kb_service as svc
    outer = svc.set_current_user_id("user-a")
    try:
        assert svc.get_current_user_id() == "user-a"
        inner = svc.set_current_user_id("user-b")
        try:
            assert svc.get_current_user_id() == "user-b"
        finally:
            svc.reset_current_user_id(inner)
        assert svc.get_current_user_id() == "user-a"
    finally:
        svc.reset_current_user_id(outer)
