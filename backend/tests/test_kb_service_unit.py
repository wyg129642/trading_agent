"""Unit tests for kb_service — no Mongo required.

Covers: ticker normalization, date parsing, Mongo filter construction, text
scoring, snippet extraction, CitationTracker KB integration, and formatted
output rendering.
"""
from __future__ import annotations

import pytest

from backend.app.services.kb_service import (
    SPECS_LIST,
    SPECS_BY_KEY,
    SPECS_BY_DOC_TYPE,
    ALL_SOURCES,
    ALL_DOC_TYPES,
    KB_TOOLS,
    KB_SYSTEM_PROMPT,
    normalize_ticker_input,
    _str_to_ms,
    _str_to_day_str,
    _build_filter,
    _tokenize,
    _score,
    _build_snippet,
    _extract_text,
    _extract_tickers,
    _extract_date,
    _extract_institution,
    _format_search_result,
    _format_fetch_result,
    _format_facets_result,
)
from backend.app.services.web_search_tool import CitationTracker


# ── Specs registry ──────────────────────────────────────────────


def test_specs_are_nonempty_and_unique():
    assert len(SPECS_LIST) >= 15
    keys = [f"{s.db}/{s.collection}" for s in SPECS_LIST]
    assert len(keys) == len(set(keys)), f"Duplicate spec keys: {keys}"
    doc_types = [s.doc_type for s in SPECS_LIST]
    assert len(doc_types) == len(set(doc_types)), f"Duplicate doc_types: {doc_types}"


def test_specs_have_text_fields():
    for s in SPECS_LIST:
        assert s.title_field, s
        assert s.text_fields and all(f for f in s.text_fields), s
        assert s.date_ms_field or s.date_str_field, f"No date field on {s.db}/{s.collection}"


def test_specs_by_lookup_tables():
    assert SPECS_BY_KEY["alphapai/comments"].doc_type == "comment"
    assert SPECS_BY_DOC_TYPE["earnings_transcript"].collection == "earnings_transcripts"
    assert "alphapai" in ALL_SOURCES
    assert "earnings_transcript" in ALL_DOC_TYPES


# ── Ticker normalization ────────────────────────────────────────


@pytest.mark.parametrize("raw, expected_contains", [
    ("NVDA.US", ["NVDA.US"]),
    ("00700.HK", ["00700.HK"]),
    ("0700.HK", ["00700.HK"]),  # 4-digit HK → must expand to 5-digit corpus form
    ("600519.SH", ["600519.SH"]),
    ("NVDA", ["NVDA.US"]),
    ("nvda", ["NVDA.US"]),
    ("0700", ["00700.HK"]),
    ("00700", ["00700.HK"]),
    # 6-digit code → precise prefix classification (600 → SH)
    ("600519", ["600519.SH"]),
    ("", []),
    ("   ", []),
])
def test_normalize_ticker_input(raw, expected_contains):
    out = normalize_ticker_input(raw)
    for t in expected_contains:
        assert t in out, f"Expected {t} in {out}"


def test_normalize_ticker_hk_short_form_expands():
    """Analysts often type 0700.HK but corpus stores 00700.HK."""
    out = normalize_ticker_input("0700.HK")
    assert "00700.HK" in out


def test_normalize_ticker_hk_already_padded_stays():
    assert normalize_ticker_input("00700.HK") == ["00700.HK"]


def test_normalize_ticker_input_preserves_case_for_canonical():
    # Mixed case input that IS canonical format should be upper-cased
    assert normalize_ticker_input("nvda.us") == ["NVDA.US"]


def test_normalize_ticker_chinese_name_resolves():
    """Curated alias table maps Chinese company names to canonical tickers."""
    assert normalize_ticker_input("英伟达") == ["NVDA.US"]
    assert normalize_ticker_input("腾讯") == ["00700.HK"]
    assert normalize_ticker_input("苹果") == ["AAPL.US"]


def test_normalize_ticker_english_company_name_resolves():
    """English company names also hit the alias table; case-insensitive."""
    assert normalize_ticker_input("Intel") == ["INTC.US"]
    assert normalize_ticker_input("Apple") == ["AAPL.US"]
    assert normalize_ticker_input("NVIDIA") == ["NVDA.US"]
    # Case-insensitive — lower / mixed should still hit the alias.
    assert normalize_ticker_input("intel") == ["INTC.US"]


def test_normalize_ticker_composite_cn_en_form():
    """Crawler tag1 / alphapai labels often pack CN+EN: '谷歌/Google'."""
    assert normalize_ticker_input("谷歌/Google") == ["GOOGL.US"]


def test_normalize_ticker_a_share_classified_to_single_market():
    """6-digit A-share codes resolve via prefix classification (matches corpus
    ingestion logic), not multi-variant brute force."""
    assert normalize_ticker_input("600519") == ["600519.SH"]    # SH (600 prefix)
    assert normalize_ticker_input("000001") == ["000001.SZ"]    # SZ (000 prefix)
    assert normalize_ticker_input("300750") == ["300750.SZ"]    # ChiNext under SZ
    assert normalize_ticker_input("688981") == ["688981.SH"]    # STAR under SH


def test_normalize_ticker_unknown_six_digit_falls_back():
    """6-digit code with no recognized A-share prefix → defensive multi-variant."""
    out = normalize_ticker_input("999999")
    assert "999999.SH" in out
    assert "999999.SZ" in out
    assert "999999.BJ" in out


def test_normalize_ticker_unknown_letters_default_to_us():
    """Alphanumeric input that's not in alias table still defaults to .US."""
    out = normalize_ticker_input("NVDA1")
    assert out == ["NVDA1.US"]


def test_kb_search_tool_description_mentions_name_aliases():
    """Tool description must tell the LLM that company names are accepted, so
    it doesn't silently fall back to passing only canonical codes."""
    schema = next(t for t in KB_TOOLS if t["function"]["name"] == "kb_search")
    desc = schema["function"]["parameters"]["properties"]["tickers"]["description"]
    assert "中文" in desc, "tickers description should mention Chinese name support"
    assert "英文" in desc, "tickers description should mention English name support"
    # The misleading '0700.HK' example must have been corrected to padded form.
    assert "00700.HK" in desc
    assert "'0700.HK'" not in desc


# ── Bulk alias table (auto-generated from Tushare + prod CSV) ──────


def test_bulk_alias_table_loaded():
    """aliases_bulk.json should be present and contribute thousands of entries."""
    from backend.app.services.ticker_normalizer import _alias_table
    table = _alias_table()
    # Curated alone is ≈260 entries. Bulk should push us well past 10k.
    assert len(table) > 10_000, f"alias table only has {len(table)} entries — bulk file missing?"


def test_bulk_resolves_a_share_chinese_names():
    """A-share companies whose Chinese names live only in bulk (not curated)."""
    cases = [
        ("贵州茅台", "600519.SH"),
        ("招商银行", "600036.SH"),
        ("中国平安", "601318.SH"),
        ("工商银行", "601398.SH"),
        ("中信证券", "600030.SH"),
        ("宁德时代", "300750.SZ"),
        ("中芯国际", "688981.SH"),
    ]
    for name, expected in cases:
        assert normalize_ticker_input(name) == [expected], f"{name} → expected {expected}"


def test_bulk_resolves_hk_chinese_and_class_indicator():
    """HK names from Tushare hk_basic, plus -W/-SW class indicator stripping."""
    cases = [
        ("腾讯控股",   "00700.HK"),
        ("腾讯",       "00700.HK"),   # stem-stripped from 腾讯控股
        ("阿里巴巴-W", "09988.HK"),   # -W suffix preserved from Tushare
        ("阿里巴巴",   "09988.HK"),   # -W stripped via stem
        ("美团-W",     "03690.HK"),
        ("美团",       "03690.HK"),
        ("京东集团-SW", "09618.HK"),
    ]
    for name, expected in cases:
        assert normalize_ticker_input(name) == [expected], f"{name} → expected {expected}"


def test_bulk_resolves_us_names_via_intersection():
    """US names: Chinese (from prod CSV) + English (from Tushare) intersection."""
    cases = [
        ("苹果",      "AAPL.US"),
        ("微软",      "MSFT.US"),
        ("谷歌",      "GOOG.US"),    # could be GOOG or GOOGL — accept both
        ("Walmart",   "WMT.US"),
        ("Coca Cola", "KO.US"),
    ]
    for name, expected in cases:
        out = normalize_ticker_input(name)
        if name == "谷歌":
            # Prod CSV maps both GOOG and GOOGL to "谷歌" — first-write-wins
            assert out and out[0] in {"GOOG.US", "GOOGL.US"}, f"谷歌 → {out}"
        else:
            assert out == [expected], f"{name} → expected {expected}, got {out}"


def test_bulk_handles_legal_suffix_stems():
    """Tushare full forms with legal suffixes should resolve at every peel."""
    cases = [
        # Tencent Holdings Ltd. → Tencent Holdings → Tencent  (all → 00700.HK)
        ("Tencent Holdings Ltd.", "00700.HK"),
        ("Tencent Holdings",      "00700.HK"),
        ("Tencent",               "00700.HK"),
        # Long-form Alibaba bulk-stems still hit HK primary
        ("Alibaba Group Holding Limited", "09988.HK"),
        ("Alibaba Group Holding",         "09988.HK"),
        ("Alibaba Group",                 "09988.HK"),
        # but the short brand "Alibaba" alone is curated → US ADR (see next test)
    ]
    for name, expected in cases:
        assert normalize_ticker_input(name) == [expected], f"{name} → expected {expected}"


def test_curated_short_brand_routes_us_adr_for_dual_listed():
    """For dual-listed names, curated prefers ADR for English brand and HK for
    Chinese name. Alibaba is the canonical example of this divergence."""
    assert normalize_ticker_input("Alibaba") == ["BABA.US"]    # English short → ADR
    assert normalize_ticker_input("阿里巴巴")  == ["09988.HK"]   # Chinese name → HK
    # Long forms — curated has no entry, bulk wins → HK primary.
    assert normalize_ticker_input("Alibaba Group") == ["09988.HK"]


def test_curated_overrides_bulk_on_conflict():
    """If aliases.json has a different mapping than aliases_bulk.json for the
    same key, the curated one must win. NVDA is in both — curated says NVDA.US."""
    from backend.app.services.ticker_normalizer import _alias_table
    table = _alias_table()
    # NVDA itself is keyed by both curated and bulk; result should match curated.
    assert table.get("英伟达") == "NVDA.US"
    # Curated has Apple → AAPL.US; bulk would also provide it. Either way: AAPL.US.
    assert normalize_ticker_input("Apple") == ["AAPL.US"]


def test_bulk_skips_unsafe_keys():
    """Single/double-letter latin keys must NOT be hijacked by bulk so that
    'A', 'BP', 'GE' (all real US tickers) still resolve via the bare-code
    fallback rather than mapping to whatever Tushare's first-row enname was."""
    # 'A' is the literal Agilent ticker. Bulk should not have 'A' as a key.
    from backend.app.services.ticker_normalizer import _alias_table
    table = _alias_table()
    assert "A" not in table or table["A"] == "A.US"
    assert "BP" not in table or table["BP"] == "BP.US"
    # Bare 'NVDA' goes through alias table (curated) → NVDA.US. Same outcome
    # as the bare-code fallback would produce.
    assert normalize_ticker_input("NVDA") == ["NVDA.US"]


# ── Date parsing ────────────────────────────────────────────────


def test_str_to_ms_handles_date_only():
    ms = _str_to_ms("2026-04-17")
    assert ms is not None and ms > 0
    # Epoch ms for 2026-04-17 00:00 UTC
    assert ms == 1776384000000


def test_str_to_ms_handles_datetime():
    assert _str_to_ms("2026-04-17 12:30") > _str_to_ms("2026-04-17 00:00")
    assert _str_to_ms("2026-04-17 12:30:45") > _str_to_ms("2026-04-17 12:30")


def test_str_to_ms_end_of_day():
    start = _str_to_ms("2026-04-17")
    end = _str_to_ms("2026-04-17", end_of_day=True)
    assert end > start
    assert end - start > 86_000_000  # ~24 h


def test_str_to_ms_invalid_returns_none():
    assert _str_to_ms("not-a-date") is None
    assert _str_to_ms("") is None


def test_str_to_day_str_end_of_day():
    assert _str_to_day_str("2026-04-17") == "2026-04-17 00:00"
    assert _str_to_day_str("2026-04-17", end_of_day=True) == "2026-04-17 23:59"
    assert _str_to_day_str("2026-04-17 10:30") == "2026-04-17 10:30"


# ── Filter builder ──────────────────────────────────────────────


def test_build_filter_ticker_only_ms_spec():
    spec = SPECS_BY_KEY["funda/earnings_transcripts"]
    q = _build_filter(spec, tickers=["NVDA.US"], date_range=None)
    assert "$or" in q
    assert {"_canonical_tickers": {"$in": ["NVDA.US"]}} in q["$or"]
    assert spec.date_ms_field not in q


def test_build_filter_date_range_ms_spec():
    spec = SPECS_BY_KEY["funda/earnings_transcripts"]
    q = _build_filter(spec, tickers=None,
                      date_range={"gte": "2026-01-01", "lte": "2026-03-31"})
    assert spec.date_ms_field in q
    assert "$gte" in q[spec.date_ms_field]
    assert "$lte" in q[spec.date_ms_field]
    # lte must be > gte (end of day inclusive)
    assert q[spec.date_ms_field]["$lte"] > q[spec.date_ms_field]["$gte"]


def test_build_filter_date_range_string_spec():
    spec = SPECS_BY_KEY["alphapai/wechat_articles"]
    q = _build_filter(spec, tickers=None,
                      date_range={"gte": "2026-01-01", "lte": "2026-03-31"})
    assert "publish_time" in q
    assert q["publish_time"]["$gte"] == "2026-01-01 00:00"
    assert q["publish_time"]["$lte"] == "2026-03-31 23:59"


def test_build_filter_stocks_fallback():
    # gangtise/researches uses stocks.code fallback
    spec = SPECS_BY_KEY["gangtise/researches"]
    q = _build_filter(spec, tickers=["00136.HK"], date_range=None)
    or_clauses = q["$or"]
    assert {"_canonical_tickers": {"$in": ["00136.HK"]}} in or_clauses
    assert {"stocks.code": {"$in": ["00136.HK"]}} in or_clauses


def test_build_filter_companies_fallback_strips_leading_zeros():
    # jinmen/reports uses companies.stockcode fallback; stockcode is pure digits
    # (e.g. '920077') while canonical is '920077.BJ' — strip '.BJ' and feed digits.
    spec = SPECS_BY_KEY["jinmen/reports"]
    q = _build_filter(spec, tickers=["920077.BJ"], date_range=None)
    or_clauses = q["$or"]
    assert {"_canonical_tickers": {"$in": ["920077.BJ"]}} in or_clauses
    assert any("companies.stockcode" in c for c in or_clauses)
    found = [c["companies.stockcode"]["$in"] for c in or_clauses if "companies.stockcode" in c]
    assert found and "920077" in found[0]


def test_build_filter_empty_returns_empty_dict():
    spec = SPECS_BY_KEY["alphapai/comments"]
    assert _build_filter(spec, tickers=None, date_range=None) == {}


# ── Scoring ────────────────────────────────────────────────────


def test_tokenize_cjk_bigrams():
    bg, wd = _tokenize("腾讯游戏监管")
    assert "腾讯" in bg
    assert "游戏" in bg
    assert "监管" in bg
    assert wd == set()  # no latin


def test_tokenize_latin_words_lowered():
    bg, wd = _tokenize("NVIDIA Q4 2025")
    assert "nvidia" in wd
    assert "q4" in wd
    assert "2025" in wd
    assert bg == set()


def test_tokenize_mixed():
    bg, wd = _tokenize("腾讯 Q4 2025 游戏")
    assert "腾讯" in bg or "Q4" in wd  # at least one matched
    assert "q4" in wd
    assert "2025" in wd


def test_score_zero_when_no_match():
    assert _score("腾讯", "NVIDIA earnings call", "Quarterly results for 2025", None) == 0.0


def test_score_title_beats_body():
    s_title = _score("腾讯", "腾讯业绩", "无相关内容", None)
    s_body = _score("腾讯", "无相关内容", "腾讯业绩", None)
    assert s_title > s_body


def test_score_recency_boost():
    import time as _t
    now_ms = int(_t.time() * 1000)
    old_ms = now_ms - int(365 * 86400 * 1000)
    s_new = _score("腾讯", "腾讯", "", now_ms)
    s_old = _score("腾讯", "腾讯", "", old_ms)
    assert s_new > s_old


# ── Snippet extraction ──────────────────────────────────────────


def test_build_snippet_empty():
    assert _build_snippet("", "query") == ""
    assert _build_snippet("short text", "") == "short text"


def test_build_snippet_centers_on_match():
    text = "A" * 500 + "腾讯游戏" + "B" * 500
    sn = _build_snippet(text, "腾讯", max_chars=100)
    assert "腾讯" in sn
    assert len(sn) <= 200  # roughly max_chars plus ellipsis


def test_build_snippet_falls_back_to_head():
    text = "一段文字没有关键词匹配"
    sn = _build_snippet(text, "无关", max_chars=200)
    assert sn == text


# ── Extractors ─────────────────────────────────────────────────


def test_extract_text_prefers_primary_field():
    spec = SPECS_BY_KEY["meritco/forum"]
    doc = {"content_md": "primary", "insight_md": "secondary"}
    assert _extract_text(spec, doc) == "primary"


def test_extract_text_falls_back_to_secondary():
    spec = SPECS_BY_KEY["meritco/forum"]
    doc = {"content_md": "", "insight_md": "secondary"}
    assert _extract_text(spec, doc) == "secondary"


def test_extract_text_empty_returns_empty():
    spec = SPECS_BY_KEY["meritco/forum"]
    assert _extract_text(spec, {}) == ""


def test_extract_tickers_canonical_preferred():
    spec = SPECS_BY_KEY["gangtise/researches"]
    doc = {"_canonical_tickers": ["AAPL.US"], "stocks": [{"code": "MSFT.US"}]}
    assert _extract_tickers(spec, doc) == ["AAPL.US"]


def test_extract_tickers_fallback_stocks():
    spec = SPECS_BY_KEY["gangtise/researches"]
    doc = {"stocks": [{"code": "00136.HK", "name": "中国儒意"}]}
    assert _extract_tickers(spec, doc) == ["00136.HK"]


def test_extract_tickers_fallback_companies_fullcode():
    spec = SPECS_BY_KEY["jinmen/reports"]
    doc = {"companies": [{"fullCode": "bj920077", "stockcode": "920077"}]}
    assert "920077.BJ" in _extract_tickers(spec, doc)


def test_extract_date_ms_field():
    spec = SPECS_BY_KEY["funda/earnings_transcripts"]
    d, ms = _extract_date(spec, {"release_time_ms": 1776700800000})
    assert d.startswith("2026-04")
    assert ms == 1776700800000


def test_extract_date_string_field():
    spec = SPECS_BY_KEY["alphapai/wechat_articles"]
    d, ms = _extract_date(spec, {"publish_time": "2026-04-17 12:30"})
    assert d == "2026-04-17"
    assert ms and ms > 0


def test_extract_institution_str():
    spec = SPECS_BY_KEY["alphapai/roadshows"]
    assert _extract_institution(spec, {"publishInstitution": "中金"}) == "中金"


def test_extract_institution_list_dict_name():
    spec = SPECS_BY_KEY["alphapai/comments"]
    doc = {"institution": [{"code": "X", "name": "国盛证券"}]}
    assert _extract_institution(spec, doc) == "国盛证券"


def test_extract_institution_none_field():
    spec = SPECS_BY_KEY["thirdbridge/interviews"]
    assert _extract_institution(spec, {"anything": "ignored"}) == ""


# ── Formatting ─────────────────────────────────────────────────


def test_format_search_result_empty():
    out = _format_search_result([], None)
    assert "未找到" in out


def test_format_search_result_with_tracker_assigns_indices():
    tracker = CitationTracker()
    hits = [
        {"doc_id": "a:b:1", "title": "T1", "date": "2026-01-01",
         "institution": "I", "doc_type": "report", "doc_type_cn": "研报",
         "snippet": "S1", "source": "alphapai", "tickers": ["0700.HK"]},
        {"doc_id": "a:b:2", "title": "T2", "date": "2026-01-02",
         "institution": "I2", "doc_type": "comment", "doc_type_cn": "点评",
         "snippet": "S2", "source": "alphapai", "tickers": []},
    ]
    out = _format_search_result(hits, tracker)
    assert "[1]" in out and "[2]" in out
    assert "T1" in out and "T2" in out
    assert "doc_id: a:b:1" in out
    sources = tracker.sources
    assert len(sources) == 2
    assert sources[0]["source_type"] == "kb"
    assert sources[0]["doc_id"] == "a:b:1"


def test_format_search_result_dedups_same_doc_id():
    tracker = CitationTracker()
    hit = {
        "doc_id": "same:same:1", "title": "T", "date": "2026-01-01",
        "institution": "I", "doc_type": "r", "doc_type_cn": "研报",
        "snippet": "S", "source": "alphapai", "tickers": [],
    }
    _format_search_result([hit, hit], tracker)
    # Duplicate doc_id should collapse to one source
    assert len(tracker.sources) == 1


def test_format_fetch_result_not_found():
    out = _format_fetch_result({"found": False, "doc_id": "x:y:z", "error": "not found"})
    assert "x:y:z" in out
    assert "not found" in out


def test_format_fetch_result_success():
    out = _format_fetch_result({
        "found": True, "doc_id": "funda:earnings_transcripts:xxx",
        "title": "Test", "source": "funda", "doc_type": "earnings_transcript",
        "doc_type_cn": "业绩会纪要", "date": "2026-01-01", "institution": "",
        "tickers": ["NVDA.US"], "text": "Content body here", "truncated": False,
        "full_text_len": 17,
    })
    assert "# Test" in out
    assert "NVDA.US" in out
    assert "Content body here" in out


def test_format_fetch_result_truncation_notice():
    out = _format_fetch_result({
        "found": True, "doc_id": "x:y:z", "title": "T", "source": "x",
        "doc_type": "r", "doc_type_cn": "研报", "date": "", "institution": "",
        "tickers": [], "text": "A" * 8000, "truncated": True, "full_text_len": 50000,
    })
    assert "截取" in out
    assert "50000" in out


def test_format_facets_result_empty():
    assert "未找到" in _format_facets_result("sources", [])


def test_format_facets_result_sources():
    out = _format_facets_result("sources", [{"source": "alphapai", "count": 100}])
    assert "alphapai" in out and "100" in out


def test_format_facets_result_tickers():
    out = _format_facets_result("tickers", [{"ticker": "0700.HK", "count": 42}])
    assert "0700.HK" in out and "42" in out


# ── Tool schemas ────────────────────────────────────────────────


def test_tool_schemas_are_openai_compatible():
    names = set()
    for t in KB_TOOLS:
        assert t["type"] == "function"
        fn = t["function"]
        assert fn["name"] and fn["description"] and fn["parameters"]
        assert fn["parameters"]["type"] == "object"
        names.add(fn["name"])
    assert names == {"kb_search", "kb_fetch_document", "kb_list_facets"}


def test_kb_search_schema_has_all_sources_and_doc_types():
    search_fn = next(t for t in KB_TOOLS if t["function"]["name"] == "kb_search")
    props = search_fn["function"]["parameters"]["properties"]
    assert props["sources"]["items"]["enum"] == ALL_SOURCES
    assert set(props["doc_types"]["items"]["enum"]) == set(ALL_DOC_TYPES)


def test_system_prompt_mentions_seven_sources():
    assert "7" in KB_SYSTEM_PROMPT or "七" in KB_SYSTEM_PROMPT
    assert "kb_search" in KB_SYSTEM_PROMPT
    assert "kb_fetch_document" in KB_SYSTEM_PROMPT
    assert "引用编号" in KB_SYSTEM_PROMPT


# ── CitationTracker KB integration ──────────────────────────────


def test_citation_tracker_add_kb_items_assigns_global_indices():
    tracker = CitationTracker()
    # First add a web result so the KB indices start at 2
    tracker.add_results([{"title": "W", "url": "https://a.com", "website": "a", "date": "2026-01-01"}])
    kb_hits = [{"doc_id": "k:1", "title": "KB1", "date": "2026-01-02",
                "institution": "I", "doc_type": "r", "doc_type_cn": "研报",
                "source": "alphapai"}]
    indexed = tracker.add_kb_items(kb_hits)
    assert indexed[0]["citation_index"] == 2
    assert tracker.sources[1]["source_type"] == "kb"
    assert tracker.sources[1]["doc_id"] == "k:1"


def test_citation_tracker_kb_dedup_by_doc_id():
    tracker = CitationTracker()
    hit = {"doc_id": "k:1", "title": "KB1", "date": "2026-01-01",
           "institution": "I", "doc_type_cn": "研报", "source": "x"}
    tracker.add_kb_items([hit])
    tracker.add_kb_items([hit])
    assert len(tracker.sources) == 1


# ── Consolidation (2026-04-24): 8-platform coverage + WeChat exclusion ──


def test_all_eight_platforms_indexed():
    """AlphaEngine was previously absent from kb_service; regression guard."""
    expected = {"alphapai", "jinmen", "meritco", "thirdbridge", "funda",
                "gangtise", "acecamp", "alphaengine"}
    assert expected.issubset(set(ALL_SOURCES)), \
        f"missing platforms: {expected - set(ALL_SOURCES)}"


def test_newly_added_collections_present():
    """AlphaEngine 4 collections + Jinmen oversea_reports + AceCamp opinions
    + Meritco research must be in the spec list — these were missed before
    the 2026-04-24 consolidation and left ~1.5 M docs unsearchable."""
    expected_keys = [
        "alphaengine/summaries",
        "alphaengine/china_reports",
        "alphaengine/foreign_reports",
        "alphaengine/news_items",
        "jinmen/oversea_reports",
        "acecamp/opinions",
        "meritco/research",
    ]
    for k in expected_keys:
        assert k in SPECS_BY_KEY, f"expected spec missing: {k}"


def test_wechat_marked_low_quality():
    from backend.app.services.kb_service import SPECS_BY_KEY as _BY_KEY
    spec = _BY_KEY["alphapai/wechat_articles"]
    assert spec.low_quality is True


def test_all_doc_types_excludes_wechat_for_llm():
    """The enum exposed to the LLM must NOT include wechat_article so the
    model never proactively requests low-quality content."""
    assert "wechat_article" not in ALL_DOC_TYPES


def test_pick_specs_excludes_low_quality_by_default():
    from backend.app.services.kb_service import _pick_specs
    picked = _pick_specs(sources=None, doc_types=None)
    coll_keys = [f"{s.db}/{s.collection}" for s in picked]
    assert "alphapai/wechat_articles" not in coll_keys


def test_pick_specs_includes_low_quality_when_explicit_doc_type():
    """If the LLM explicitly asks for wechat_article, we honor it."""
    from backend.app.services.kb_service import _pick_specs
    picked = _pick_specs(sources=None, doc_types=["wechat_article"])
    coll_keys = [f"{s.db}/{s.collection}" for s in picked]
    assert "alphapai/wechat_articles" in coll_keys


def test_pick_specs_includes_low_quality_when_flag_set():
    from backend.app.services.kb_service import _pick_specs
    picked = _pick_specs(sources=None, doc_types=None, include_low_quality=True)
    coll_keys = [f"{s.db}/{s.collection}" for s in picked]
    assert "alphapai/wechat_articles" in coll_keys


def test_pick_specs_milvus_only():
    from backend.app.services.kb_service import _pick_specs
    picked = _pick_specs(sources=None, doc_types=None, milvus_only=True)
    for s in picked:
        assert s.milvus_indexed is True


def test_alphaengine_not_in_milvus_subset():
    """AlphaEngine not yet ingested into Milvus — must fall through to
    Phase A keyword/Mongo path."""
    from backend.app.services.kb_service import _pick_specs
    picked = _pick_specs(sources=["alphaengine"], doc_types=None, milvus_only=True)
    assert picked == []
    picked_all = _pick_specs(sources=["alphaengine"], doc_types=None)
    assert len(picked_all) == 4  # summaries, china_reports, foreign_reports, news_items


# ── Hybrid merge (Phase A + Phase B) ───────────────────────────


def test_merge_hybrid_hits_union_dedup():
    from backend.app.services.kb_service import _merge_hybrid_hits
    vec = [
        {"doc_id": "a:1", "title": "A", "score": 0.9},
        {"doc_id": "a:2", "title": "B", "score": 0.8},
    ]
    kw = [
        {"doc_id": "a:2", "title": "B", "score": 1.5},
        {"doc_id": "a:3", "title": "C", "score": 0.4},
    ]
    merged = _merge_hybrid_hits(vec, kw, top_k=10)
    doc_ids = [h["doc_id"] for h in merged]
    # Must include all three unique docs
    assert set(doc_ids) == {"a:1", "a:2", "a:3"}
    # a:2 appears in both engines → should rank at or near the top (sum of RRF)
    assert doc_ids.index("a:2") <= 1


def test_merge_hybrid_hits_empty_inputs():
    from backend.app.services.kb_service import _merge_hybrid_hits
    assert _merge_hybrid_hits([], [], top_k=5) == []


def test_merge_hybrid_hits_top_k_truncation():
    from backend.app.services.kb_service import _merge_hybrid_hits
    vec = [{"doc_id": f"v:{i}", "title": f"V{i}"} for i in range(20)]
    kw = [{"doc_id": f"k:{i}", "title": f"K{i}"} for i in range(20)]
    merged = _merge_hybrid_hits(vec, kw, top_k=5)
    assert len(merged) == 5


# ── Retired tool compatibility ─────────────────────────────────


def test_alphapai_tools_empty_list():
    """ALPHAPAI_TOOLS must be empty so the LLM never sees the retired tool."""
    from backend.app.services.alphapai_service import ALPHAPAI_TOOLS, ALPHAPAI_SYSTEM_PROMPT
    assert ALPHAPAI_TOOLS == []
    assert ALPHAPAI_SYSTEM_PROMPT == ""


def test_jinmen_tools_empty_list():
    from backend.app.services.jinmen_service import JINMEN_TOOLS, JINMEN_SYSTEM_PROMPT
    assert JINMEN_TOOLS == []
    assert JINMEN_SYSTEM_PROMPT == ""


def test_alphapai_stub_redirects_to_kb_search():
    """Stale history calling alphapai_recall must get a clear redirect message,
    not a silent failure or external API hit."""
    import asyncio
    from backend.app.services.alphapai_service import execute_tool
    out = asyncio.run(execute_tool("alphapai_recall", {"query": "欧陆通业务"}))
    assert "kb_search" in out
    assert "已停用" in out
    # And must not raise / reach the network


def test_jinmen_stub_redirects_to_kb_search():
    import asyncio
    from backend.app.services.jinmen_service import execute_tool
    out = asyncio.run(execute_tool("jinmen_search", {"query": "腾讯"}))
    assert "kb_search" in out
    assert "已停用" in out


# ── KB_SYSTEM_PROMPT consolidation markers ─────────────────────


def test_kb_system_prompt_mentions_all_eight_platforms():
    """The prompt must enumerate all 8 platforms so the LLM knows the scope."""
    for kw in ["Alpha派", "进门财经", "久谦", "第三方桥", "Funda",
               "岗底斯", "峰会", "阿尔法引擎"]:
        assert kw in KB_SYSTEM_PROMPT, f"missing platform mention: {kw}"


def test_kb_system_prompt_forbids_retired_tools():
    """Prompt must explicitly tell the LLM not to call alphapai_recall / jinmen_*."""
    assert "alphapai_recall" in KB_SYSTEM_PROMPT
    assert "外部 API" in KB_SYSTEM_PROMPT or "已停用" in KB_SYSTEM_PROMPT


def test_kb_system_prompt_emphasizes_time_filter():
    assert "date_range" in KB_SYSTEM_PROMPT
    assert "时间" in KB_SYSTEM_PROMPT


def test_kb_system_prompt_mentions_parallel_search():
    assert "并行" in KB_SYSTEM_PROMPT


# ── kb_search schema ──────────────────────────────────────────


def test_kb_search_schema_has_date_range():
    schema = next(t for t in KB_TOOLS if t["function"]["name"] == "kb_search")
    props = schema["function"]["parameters"]["properties"]
    assert "date_range" in props
    assert "gte" in props["date_range"]["properties"]
    assert "lte" in props["date_range"]["properties"]


def test_kb_search_schema_doc_types_enum_excludes_wechat():
    schema = next(t for t in KB_TOOLS if t["function"]["name"] == "kb_search")
    enum = schema["function"]["parameters"]["properties"]["doc_types"]["items"]["enum"]
    assert "wechat_article" not in enum


def test_kb_search_schema_sources_enum_has_all_8():
    schema = next(t for t in KB_TOOLS if t["function"]["name"] == "kb_search")
    enum = schema["function"]["parameters"]["properties"]["sources"]["items"]["enum"]
    for p in ["alphapai", "jinmen", "meritco", "thirdbridge", "funda",
              "gangtise", "acecamp", "alphaengine"]:
        assert p in enum, f"missing source enum: {p}"
