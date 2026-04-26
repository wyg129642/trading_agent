"""Ticker normalizer — turn any upstream stock reference into a canonical list.

Canonical format: ``<CODE>.<MARKET>`` where ``MARKET`` is a 2-letter code:
    SH  上交所 (600/601/603/605/688…)
    SZ  深交所 (000/001/002/003/300/301)
    BJ  北交所 (4/8/9 prefixes for NEEQ-listed)
    HK  港交所 (5-digit padded)
    US  NASDAQ/NYSE/AMEX
    DE  Xetra
    JP  Tokyo
    KS  Korea Exchange
    TW  Taiwan
    AU  ASX
    CA  TSX
    GB  LSE
    FR  Paris
    CH  SIX
    NL  Euronext Amsterdam
    SE  Stockholm
    NO  Oslo
    IT  Milan
    AT  Vienna
    NZ  NZX
    HE  Helsinki

Design principles
-----------------
- Pure function: zero side effects; same input → same output every call.
- Additive: produces ``_canonical_tickers: list[str]``; never mutates anything
  else on the source document.
- Deterministic order: input order preserved, duplicates removed.
- Unknown inputs become ``(None, raw_string)`` so the caller can store them in
  ``_unmatched_raw`` for later alias-table expansion.
"""
from __future__ import annotations

import json
import re
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterable

# --------------------------------------------------------------------------- #
# Static market maps
# --------------------------------------------------------------------------- #
# Jinmen (brm.comein.cn) lowercase 3-letter / 2-letter codes
_JINMEN_MARKET_MAP: dict[str, str] = {
    "sh": "SH", "sz": "SZ", "bj": "BJ",
    "hk": "HK",
    "us": "US",
    "jp": "JP", "jpn": "JP",
    "kr": "KS", "kor": "KS",
    "tw": "TW", "twn": "TW",
    "au": "AU", "aus": "AU",
    "ca": "CA", "can": "CA",
    "gb": "GB", "gbr": "GB", "uk": "GB",
    "fr": "FR", "fra": "FR",
    "de": "DE", "deu": "DE", "ger": "DE",
    "ch": "CH", "che": "CH", "swi": "CH",
    "nl": "NL", "nld": "NL",
    "se": "SE", "swe": "SE",
    "no": "NO", "nor": "NO",
    "it": "IT", "ita": "IT",
    "at": "AT", "aut": "AT",
    "nz": "NZ", "nzl": "NZ",
    "fi": "HE", "fin": "HE",  # funny: "fin" is "finland" not "finance" here
}

# Third Bridge uses 2-letter country codes after the ticker ("CN" = mainland china)
_TB_COUNTRY_MAP: dict[str, str] = {
    "US": "US",
    "CN": "CN",  # further split into SH/SZ/BJ by code prefix
    "HK": "HK",
    "JP": "JP",
    "KR": "KS",
    "TW": "TW",
    "AU": "AU",
    "CA": "CA",
    "GB": "GB",
    "FR": "FR",
    "DE": "DE",
    "CH": "CH",
    "NL": "NL",
    "SE": "SE",
    "NO": "NO",
    "IT": "IT",
    "AT": "AT",
    "NZ": "NZ",
    "FI": "HE",
}

# Markets we know about — used to validate ".X" suffixes already in canonical form.
# Extended 2026-04-24 to cover all markets observed in upstream crawler corpora
# (Jinmen oversea_reports, Gangtise, AlphaPai, AlphaEngine).
_KNOWN_MARKETS: set[str] = {
    # CN / HK / original set
    "SH", "SZ", "BJ", "HK", "US", "DE", "JP", "KS", "TW", "AU",
    "CA", "GB", "FR", "CH", "NL", "SE", "NO", "IT", "AT", "NZ", "HE",
    # New additions (2026-04-24) — canonical 2-letter ISO-ish codes
    "IN",  # India (NSE/BSE)
    "BR",  # Brazil (B3)
    "ES",  # Spain (BME)
    "DK",  # Denmark (Nasdaq Copenhagen)
    "SG",  # Singapore (SGX)
    "TH",  # Thailand (SET)
    "MY",  # Malaysia (Bursa)
    "ID",  # Indonesia (IDX)
    "PH",  # Philippines (PSE)
    "VN",  # Vietnam (HOSE)
    "TR",  # Turkey (BIST)
    "MX",  # Mexico (BMV)
    "AR",  # Argentina
    "CL",  # Chile
    "PE",  # Peru
    "CO",  # Colombia
    "SA",  # Saudi Arabia (Tadawul)
    "AE",  # UAE (ADX/DFM)
    "EG",  # Egypt
    "ZA",  # South Africa (JSE)
    "QA",  # Qatar (QSE)
    "IL",  # Israel (TASE)
    "HU",  # Hungary (BÉT)
    "CZ",  # Czechia (PSE)
    "PL",  # Poland (GPW)
    "BE",  # Belgium (Euronext Brussels)
    "PT",  # Portugal (Euronext Lisbon)
    "IE",  # Ireland (Euronext Dublin)
    "GR",  # Greece (ATHEX)
    "RU",  # Russia (MOEX)
}


# --------------------------------------------------------------------------- #
# Exchange-suffix aliases → canonical MARKET (2026-04-24)
# --------------------------------------------------------------------------- #
# Bloomberg / Reuters / Jinmen's self-coined 3-letter suffixes. Inputs like
# ``2371.JPN``, ``NESN.AUT``, ``PLX.PA``, ``BBY.N``, ``4190.SE`` are resolved
# through this table.
#
# ⚠️  Single-letter suffixes ``.A / .P / .V / .B`` are intentionally OMITTED —
#    they collide with company legal-form abbreviations (``S.p.A.``, ``J.P.``,
#    ``N.V.``, ``TECK.B``) and would generate heavy false positives on free
#    text. Use the colon-form parser (``ARX:CA``) or structured fields instead.
#
# ⚠️  ``.CA`` (dot-form) is ambiguous — could be Cairo (Reuters) or Canada
#    (older Bloomberg). Canada is resolved via ``CODE:CA`` colon-form or via
#    ``.TO`` / ``.V``. Bare ``.CA`` is left unmapped to avoid Egypt/Canada
#    confusion.
_EXCHANGE_SUFFIX_MAP: dict[str, str] = {
    # Identity for canonical 2-letter codes (populated from _KNOWN_MARKETS below)

    # --- Single-letter Bloomberg / Reuters ---
    "N": "US",   # NYSE
    "O": "US",   # NASDAQ composite
    "S": "CH",   # Swiss SIX
    "T": "JP",   # Tokyo
    "L": "GB",   # London
    "F": "DE",   # Frankfurt retail
    "J": "ZA",   # Johannesburg

    # --- 2-letter Bloomberg / Reuters ---
    "OQ": "US",  "PK": "US",   "NY": "US",
    "LN": "GB",
    "PA": "FR",  "FP": "FR",
    "AS": "NL",  "NA": "NL",
    "GY": "DE",  "GR": "DE",   # German Xetra
    "MI": "IT",  "IM": "IT",
    "MC": "ES",  "SM": "ES",
    "CO": "DK",  "DC": "DK",
    "ST": "SE",  "SS": "SE",
    "FH": "HE",
    "OL": "NO",
    "WA": "PL",  "PW": "PL",
    "VI": "AT",  "AV": "AT",
    "SW": "CH",  "VX": "CH",
    "BB": "BE",                # Brussels
    "LS": "PT",
    "AX": "AU",
    "TA": "IL",
    "BO": "IN",  "NS": "IN",  "IB": "IN",
    "KQ": "KS",
    "TT": "TW",  "TWO": "TW",
    "KL": "MY",  "MK": "MY",
    "BK": "TH",  "TB": "TH",
    "SI": "SG",  "SP": "SG",
    "JK": "ID",  "IJ": "ID",
    "PS": "PH",  "PM": "PH",
    "HM": "VN",
    "IS": "TR",  "TI": "TR",
    "BU": "HU",  "HB": "HU",
    "PR": "CZ",  "CP": "CZ",
    "SN": "CL",
    "BA": "AR",
    "MM": "MX",
    "TO": "CA",                # Toronto — colon-form primarily, but allow dotted too
    "QD": "QA",
    "AB": "SA",                # Saudi alt
    "DU": "AE",
    "RM": "RU",
    "AT": "GR",                # Reuters Athens — beware: canonical AT is Austria (distinct)
    # NOTE: these three are context-dependent; treat conservatively.
    # Jinmen convention: ``.SA`` = Brazil (São Paulo), ``.SE`` = Saudi
    "SA": "BR",                # Brazil B3 (Jinmen + Reuters)
    "SE": "SA",                # Saudi Tadawul (Jinmen + Reuters)

    # --- 3-letter Jinmen / Refinitiv style ---
    "JPN": "JP", "KOR": "KS", "TWN": "TW", "AUS": "AU",
    "CAN": "CA", "GBR": "GB", "FRA": "FR", "DEU": "DE", "GER": "DE",
    "CHE": "CH", "NLD": "NL", "SWE": "SE", "NOR": "NO",
    "ITA": "IT", "AUT": "AT",  # Note: Jinmen's .AUT is sometimes mislabeled
                               # (e.g. Nestle/ABInBev); kept for raw passthrough.
    "NZL": "NZ", "FIN": "HE", "POL": "PL", "ESP": "ES",
    "BEL": "BE", "PRT": "PT", "IRL": "IE", "GRC": "GR",
    "IND": "IN", "MYS": "MY", "PHL": "PH", "IDN": "ID",
    "SGP": "SG", "THA": "TH", "TUR": "TR", "ISR": "IL",
    "SAU": "SA", "ARE": "AE", "EGY": "EG", "ZAF": "ZA",
    "QAT": "QA", "HUN": "HU", "CZE": "CZ",
    "BRA": "BR", "ARG": "AR", "CHL": "CL", "MEX": "MX",
    "PER": "PE", "COL": "CO",
    "USA": "US", "HKG": "HK", "CHN": "CN",
    "RUS": "RU",
}
# Fold identity mappings (A → A) for canonical markets ONLY if they don’t
# already have a non-identity alias set above. This preserves the Jinmen/Reuters
# conventions for ambiguous suffixes like ``.SA`` (Brazil, not Saudi) and
# ``.SE`` (Saudi, not Sweden — Sweden uses ``.ST``).
for _m in _KNOWN_MARKETS:
    _EXCHANGE_SUFFIX_MAP.setdefault(_m, _m)


def _resolve_market_suffix(suffix: str) -> str | None:
    """Canonicalize any upstream market suffix (Bloomberg/Reuters/Jinmen) → 2-letter."""
    if not suffix:
        return None
    return _EXCHANGE_SUFFIX_MAP.get(suffix.upper())

# --------------------------------------------------------------------------- #
# Alias tables — bulk (auto-generated) + curated (hand-edited).
#
# Layered lookup: ``aliases_bulk.json`` (≈50k entries from Tushare + prod CSV,
# rebuilt by ``scripts/rebuild_aliases_bulk.py``) seeds the table, then
# ``aliases.json`` (≈260 hand-curated entries) overlays it. Curated wins on
# conflict so operator fixes always take precedence over auto-generated data.
# --------------------------------------------------------------------------- #
_ALIASES_PATH = Path(__file__).parent / "ticker_data" / "aliases.json"
_ALIASES_BULK_PATH = Path(__file__).parent / "ticker_data" / "aliases_bulk.json"


# Top-down brand-suffix expansion: for any CN brand-only key in the merged
# table (e.g. "阿里巴巴"), emit the same value under "<brand>+控股", "<brand>+集团"
# etc. Catches LLM phrasings like "阿里巴巴控股" / "腾讯股份" that Tushare's
# bottom-up stem chain doesn't generate.
_CN_BRAND_SUFFIXES: tuple[str, ...] = (
    "控股", "集团", "股份", "公司",
    "集团控股", "集团股份", "股份控股",
    "股份有限公司", "有限公司",
)
_CN_KNOWN_SUFFIX_RE = re.compile(
    r"(股份有限公司|有限公司|集团股份|集团控股|股份|控股|公司|集团|"
    r"-W|-SW|-S|-WR)$"
)
# Lookup-time English legal-suffix stripper (mirrors rebuild_aliases_bulk.py).
# Used by ``_parse_bare`` so "Apple Inc" / "NVIDIA Corporation" /
# "Tencent Holdings Ltd" hit the same canonical as their stem.
_EN_LEGAL_SUFFIX_RE = re.compile(
    r"[,\s]+("
    r"Co\.?,?\s*Ltd\.?|Co\.?,?\s*Limited|"
    r"Holdings|Holding|Group|"
    r"Limited|Ltd\.?|Inc\.?|Corp\.?|Corporation|Company|"
    r"Plc|Pty\.?\s*Ltd\.?|S\.A\.|N\.V\.|AG|SE"
    r")\.?\s*$",
    re.IGNORECASE,
)


def _load_alias_file(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return {k: v for k, v in data.items() if not k.startswith("_") and isinstance(v, str)}


def _is_cn_brand_only(key: str) -> bool:
    """True for CJK keys that don't already end in a known CN legal suffix."""
    if not key or not isinstance(key, str):
        return False
    if not re.search(r"[一-鿿]", key):
        return False
    if len(key) < 2 or len(key) > 8:  # 1-char ambiguous, 9+ already specific
        return False
    return _CN_KNOWN_SUFFIX_RE.search(key) is None


@lru_cache(maxsize=1)
def _alias_table() -> dict[str, str]:
    """Bulk-then-curated merged alias table, with CN brand-suffix expansion.

    Layered: ``aliases_bulk.json`` (~50k Tushare-derived) seeds, ``aliases.json``
    (~270 curated) overrides on conflict. Then a top-down expansion pass adds
    ``<brand>+控股/集团/股份/公司/...`` for every CN brand-only key — covers
    colloquial phrasings ("阿里巴巴控股", "腾讯股份") that Tushare's bottom-up
    stem chain misses because the intermediate string was never an input name.
    """
    merged = _load_alias_file(_ALIASES_BULK_PATH)
    merged.update(_load_alias_file(_ALIASES_PATH))

    expansions: dict[str, str] = {}
    for k, v in merged.items():
        if not _is_cn_brand_only(k):
            continue
        for suffix in _CN_BRAND_SUFFIXES:
            ek = k + suffix
            if ek not in merged and ek not in expansions:
                expansions[ek] = v
    merged.update(expansions)
    return merged


@lru_cache(maxsize=1)
def _alias_lc_index() -> dict[str, str]:
    """Lower-case key index (case-insensitive lookup, ~50k+ entries)."""
    return {k.lower(): v for k, v in _alias_table().items()}


@lru_cache(maxsize=1)
def _known_canonicals() -> frozenset[str]:
    """All canonical tickers (values) from the merged alias tables.

    Used by ``is_known_canonical`` to validate LLM-supplied dotted-form inputs
    and reject hallucinations like ``BABA.HK`` / ``9988.US`` / ``TSMC.US`` that
    pass syntactic parsing but don't correspond to a real listing.
    """
    return frozenset(_alias_table().values())


def is_known_canonical(canonical: str | None) -> bool:
    """True if ``canonical`` (e.g. ``00700.HK``) appears as a value in alias
    tables — i.e. is a real listing in our snapshot. Used at the LLM-input
    boundary to filter out hallucinated CODE.MARKET combinations."""
    if not canonical:
        return False
    return canonical in _known_canonicals()


@lru_cache(maxsize=1)
def _alias_by_length_lc() -> dict[int, frozenset[str]]:
    """Lower-case alias keys bucketed by string length.

    Used by ``extract_canonicals_from_query`` for greedy longest-match scan
    over free text. Skips L<2 (ambiguous) and L>20 (unlikely to appear
    verbatim in a chat-style query).
    """
    by_len: dict[int, set[str]] = {}
    for k in _alias_table():
        L = len(k)
        if L < 2 or L > 20:
            continue
        by_len.setdefault(L, set()).add(k.lower())
    return {L: frozenset(s) for L, s in by_len.items()}


@lru_cache(maxsize=1)
def _canonical_to_brands() -> dict[str, frozenset[str]]:
    """Reverse map ``canonical → set of brand-name keys``.

    Used by ``is_mentioned_in_query`` to detect whether an LLM-supplied
    ticker is referenced in the query (by code or any known brand name) —
    a sign that the LLM's choice is intentional rather than hallucinated.
    """
    by_value: dict[str, set[str]] = {}
    for k, v in _alias_table().items():
        by_value.setdefault(v, set()).add(k)
    return {v: frozenset(s) for v, s in by_value.items()}


def reload_aliases() -> None:
    """Force re-read both alias JSONs (useful after manual edit or rebuild)."""
    _alias_table.cache_clear()
    _alias_lc_index.cache_clear()
    _known_canonicals.cache_clear()
    _alias_by_length_lc.cache_clear()
    _canonical_to_brands.cache_clear()


# --------------------------------------------------------------------------- #
# Query-side entity extraction (cross-validation against LLM hallucinations)
# --------------------------------------------------------------------------- #
# Detects explicit ``CODE.MARKET`` patterns in free text. Stricter than the
# parenthesized extractor below: requires word boundaries (no leading dot)
# so that legal forms like ``S.p.A.`` / ``J.P.`` don't false-match.
_QUERY_DOTTED_RE = re.compile(
    r"(?<![A-Za-z0-9.])([A-Z0-9]{1,8}\.[A-Z]{1,4})(?![A-Za-z0-9.])"
)
# Bare 6-digit A-share code in query (rare but real: "公司 600519 最新动态").
# 4-5-digit numbers are intentionally NOT auto-treated as HK codes — those
# are overwhelmingly years / random numerics in query text. HK codes in query
# come almost always in dotted form (`00700.HK`); the rare bare-HK case is
# delegated to the LLM's tickers field.
_QUERY_6DIGIT_RE = re.compile(r"(?<!\d)(\d{6})(?!\d)")

# ASCII tokens that ARE legitimate alias keys but also common
# technical / financial terms — query-side extraction skips them to avoid
# false hits on free text. ``is_mentioned_in_query`` still recognises them
# (so the LLM CAN intentionally pick them via tickers), but they don't get
# auto-added from query text.
_QUERY_AMBIGUOUS_ASCII: frozenset[str] = frozenset({
    # AI / hardware acronyms
    "HBM", "AI", "ML", "DL", "GPU", "CPU", "TPU", "NPU", "DPU", "FPGA",
    "DRAM", "NAND", "SSD", "HDD", "RAM", "ROM",
    "API", "SDK", "OS", "PC", "VR", "AR", "XR", "MR", "OT",
    "5G", "6G", "IOT", "CDN", "VPN",
    # Finance acronyms
    "EPS", "PE", "PB", "PS", "ROE", "ROA", "EBIT", "EBITDA", "FCF",
    "CAPEX", "OPEX", "GAAP", "IFRS", "GMV", "DAU", "MAU", "WAU", "ARPU",
    "IPO", "ESG", "VC", "FED", "ECB", "BOJ", "PBOC",
    # Macro acronyms
    "GDP", "CPI", "PPI", "PMI", "RMB", "USD", "EUR", "JPY", "CNY",
    # Time / period — most common false-positive sources
    "Q1", "Q2", "Q3", "Q4", "H1", "H2", "FY", "YOY", "QOQ", "MOM",
    # Single 1-letter tokens are too noisy: would match T (AT&T) / F (Ford)
    # / V (Visa) / X (US Steel) inside any English sentence.
    "A", "B", "C", "I", "O", "T", "F", "V", "X", "Y", "Z",
    # NOTE: short real US tickers ARE deliberately allowed through:
    # LI / MS / MU / JD / NV / GE / GM / PG / BA / CB / SO / BB / IT
    # Under the per-market policy LLM uses letter codes for US, so when a
    # user types "LI vs NVDA Q3" we want LI to extract correctly. Users
    # asking about lithium chemistry / MS Office in this kb_search context
    # is unlikely; if they do, the worst case is an extra ticker filter.
})


def extract_canonicals_from_query(text: str | None) -> list[str]:
    """Best-effort entity extraction from a free-text query.

    Detects company references via two passes:

    1. **Substring scan** against the alias table (longest-match first, with
       greedy span masking). Catches Chinese names ("中际旭创") and English
       names ("Apple", "NVIDIA Corporation") that appear verbatim. ASCII
       matches require word boundaries to avoid 'apple' inside 'pineapple'.
    2. **Explicit code patterns** — ``CODE.MARKET`` (e.g. ``300308.SZ``) and
       bare 4–6-digit numerics, validated via ``is_known_canonical``.

    Returns canonical tickers in order of first appearance, deduplicated.

    Used by ``kb_service`` to cross-validate LLM-supplied ``tickers`` against
    entities the LLM actually *mentioned* in the query — disjoint cases are
    likely LLM hallucinations and get dropped.
    """
    if not text or not isinstance(text, str):
        return []
    n = len(text)
    if n == 0:
        return []
    text_lc = text.lower()
    by_len = _alias_by_length_lc()
    lc = _alias_lc_index()

    matched = bytearray(n)  # 0 = free, 1 = consumed by an earlier longer match
    out: list[str] = []
    seen: set[str] = set()

    def _is_word_boundary(start: int, end: int) -> bool:
        before = (start == 0) or not text[start - 1].isalnum()
        after = (end == n) or not text[end].isalnum()
        return before and after

    # Pass 1: longest-match alias substring scan
    for L in sorted((l for l in by_len if l <= n), reverse=True):
        keys = by_len[L]
        i = 0
        limit = n - L
        while i <= limit:
            if matched[i]:
                i += 1
                continue
            window = text_lc[i:i + L]
            if window not in keys:
                i += 1
                continue
            # ASCII keys: require word boundaries (prevents 'apple' in 'pineapple')
            if window.isascii():
                if not _is_word_boundary(i, i + L):
                    i += 1
                    continue
                # Skip ambiguous tech/finance terms (HBM, AI, EPS, GDP, ...)
                # — they pass syntactic match but are clearly not the company.
                if window.upper() in _QUERY_AMBIGUOUS_ASCII:
                    i += 1
                    continue
            canonical = lc.get(window)
            if not canonical:
                i += 1
                continue
            if canonical not in seen:
                seen.add(canonical)
                out.append(canonical)
            for j in range(i, i + L):
                matched[j] = 1
            i += L

    # Pass 2: explicit code patterns. Only CODE.MARKET (dotted) and 6-digit
    # A-share codes — bare 4-5 digit numerics are too noisy (years, model
    # numbers, "8/16/32 GB") to auto-extract as HK codes. The LLM should
    # supply HK codes via the ``tickers`` field, not embedded in query text.
    for m in _QUERY_DOTTED_RE.finditer(text):
        candidate = m.group(1).upper()
        if is_known_canonical(candidate) and candidate not in seen:
            seen.add(candidate)
            out.append(candidate)
    for m in _QUERY_6DIGIT_RE.finditer(text):
        digits = m.group(1)
        cls = _classify_ashare(digits)
        if cls:
            cand = f"{digits}.{cls}"
            if is_known_canonical(cand) and cand not in seen:
                seen.add(cand)
                out.append(cand)

    return out


def is_mentioned_in_query(canonical: str | None, query: str | None) -> bool:
    """True iff ``canonical`` is referenced in ``query`` text via either:

    - the canonical itself (``BABA.US`` substring),
    - just the code part (``BABA`` or ``300308`` substring with word boundaries),
    - or any known brand name mapping to the canonical (``阿里巴巴`` / ``Alibaba``).

    Used to validate LLM-supplied tickers — if the LLM picked a code whose
    name/code is nowhere in the query, that's the strongest hallucination
    signal we can detect from the LLM-input boundary.
    """
    if not canonical or not query:
        return False
    q_lc = query.lower()
    c_lc = canonical.lower()
    if c_lc in q_lc:
        return True
    code_part = canonical.rsplit(".", 1)[0]
    if code_part:
        cp_lc = code_part.lower()
        # Code parts need boundary-aware matching for short ASCII codes
        # (avoid "MU" inside "MUTUAL"); CJK/digit-only is fine substring.
        if len(cp_lc) >= 4 or not cp_lc.isascii():
            if cp_lc in q_lc:
                return True
        elif code_part.upper() in _QUERY_AMBIGUOUS_ASCII:
            # Short ASCII code that's also a common acronym (LI / MS / MU /
            # JD / NV / Q3 ...) — bare appearance in query is too noisy to
            # count as a mention. Fall through to brand-name match.
            pass
        else:
            # Short ASCII (2-3 chars): require word-boundary
            for m in re.finditer(re.escape(cp_lc), q_lc):
                start, end = m.start(), m.end()
                before = start == 0 or not query[start - 1].isalnum()
                after = end == len(query) or not query[end].isalnum()
                if before and after:
                    return True
    brands = _canonical_to_brands().get(canonical, frozenset())
    for b in brands:
        b_lc = b.lower()
        if not b_lc or len(b_lc) < 2:
            continue
        # Skip short ASCII brands that are also common English words /
        # acronyms (LI / MS / MU / HBM / Q3 / ...). They're poor signals
        # of intent — require a more specific brand match instead.
        if b.isascii() and len(b) <= 3 and b.upper() in _QUERY_AMBIGUOUS_ASCII:
            continue
        if b_lc in q_lc:
            # ASCII brand needs word-boundary too (e.g. "AMD" in "amdahl's")
            if b_lc.isascii() and len(b_lc) <= 4:
                for m in re.finditer(re.escape(b_lc), q_lc):
                    start, end = m.start(), m.end()
                    before = start == 0 or not query[start - 1].isalnum()
                    after = end == len(query) or not query[end].isalnum()
                    if before and after:
                        return True
                continue
            return True
    return False


# --------------------------------------------------------------------------- #
# A-share market classification by code prefix
# --------------------------------------------------------------------------- #
def _classify_ashare(code: str) -> str | None:
    """CN-mainland 6-digit codes → SH/SZ/BJ."""
    if not re.fullmatch(r"\d{6}", code):
        return None
    c = code
    # SH: 600/601/603/605/688, plus 900 B-shares
    if c[:3] in {"600", "601", "603", "605", "688"} or c[:1] == "9" and c[:3] in {"900"}:
        return "SH"
    # SZ: 000/001/002/003/300/301, plus 200 B-shares
    if c[:3] in {"000", "001", "002", "003", "300", "301"} or c[:3] == "200":
        return "SZ"
    # BJ NEEQ: 43xx / 83xx / 87xx / 88xx / 92xx / 920
    if c[:2] in {"43", "83", "87", "88", "92"}:
        return "BJ"
    return None


# --------------------------------------------------------------------------- #
# Low-level parsers
# --------------------------------------------------------------------------- #
def _pad_hk(code: str) -> str:
    """Normalize HK codes to 5 digits with leading zeros (HKEX canonical)."""
    digits = re.sub(r"\D", "", code)
    return digits.zfill(5) if digits else code


def _canonical_from_code_market(raw_code: str, raw_market: str | None) -> str | None:
    """Given a pair like (603061, sh) or (03896, hk) or (AAPL, us), produce canonical."""
    if not raw_code:
        return None
    code = raw_code.strip()
    market_in = (raw_market or "").strip().lower()
    market = _JINMEN_MARKET_MAP.get(market_in)
    if not market:
        return None

    # Disambiguate CN → SH/SZ/BJ
    if market == "CN":
        cls = _classify_ashare(code)
        if cls:
            return f"{code}.{cls}"
        return None

    if market == "HK":
        return f"{_pad_hk(code)}.HK"

    # US / EU / JP etc. — strip any trailing whitespace / pad as needed
    return f"{code.upper()}.{market}"


def _parse_dotted(raw: str) -> str | None:
    """Input already looks like ``CODE.SUFFIX`` — handles the full
    Bloomberg / Reuters / Jinmen suffix family via ``_resolve_market_suffix``.

    Examples:
      - ``603061.SH``  → ``603061.SH``
      - ``AAPL.US``    → ``AAPL.US``
      - ``2371.JPN``   → ``2371.JP``       (Jinmen 3-letter)
      - ``NESN.S``     → ``NESN.CH``       (Bloomberg 1-letter)
      - ``PLX.PA``     → ``PLX.FR``
      - ``4190.SE``    → ``4190.SA``       (Jinmen: .SE = Saudi)
      - ``CAST.ST``    → ``CAST.SE``       (Stockholm)
    """
    # Up to 4-letter suffix. Code accepts alphanum + hyphen + internal dot
    # (for class-share tickers like TECK.B). Non-greedy on code so suffix
    # claims the final `.XX` not the penultimate.
    m = re.fullmatch(r"([\w\-\.]+?)\.([A-Z]{1,4})", raw.strip())
    if not m:
        return None
    code, suffix = m.group(1).upper(), m.group(2).upper()
    if re.search(r"[\u4e00-\u9fff]", raw):
        return None
    canonical = _resolve_market_suffix(suffix)
    if not canonical:
        return None
    if canonical == "CN":
        cls = _classify_ashare(code)
        return f"{code}.{cls}" if cls else None
    if canonical == "HK":
        return f"{_pad_hk(code)}.HK"
    return f"{code}.{canonical}"


def _parse_reverse_dotted(raw: str) -> str | None:
    """AceCamp stores `ticker` market-first: `US.INTC`, `HK.04335`, `SH.603061`.
    Mirror of `_parse_dotted` for the opposite ordering."""
    m = re.fullmatch(r"([A-Z]{1,3})\.([\w\-]+)", raw.strip())
    if not m:
        return None
    prefix, code = m.group(1).upper(), m.group(2).upper()
    if prefix not in _KNOWN_MARKETS:
        return None
    if prefix == "HK":
        return f"{_pad_hk(code)}.HK"
    return f"{code}.{prefix}"


def _parse_colon_suffix(raw: str) -> str | None:
    """AlphaPai roadshows use colon-separated titles like ``(ARX:CA)``,
    ``(TECK.B:CA)``. Same logic as ``_parse_dotted`` but with ``:`` separator.
    """
    s = raw.strip()
    if ":" not in s:
        return None
    code, _, suffix = s.rpartition(":")
    if not code or not suffix:
        return None
    if not re.fullmatch(r"[A-Z]{1,4}", suffix.upper()):
        return None
    if not re.fullmatch(r"[\w\-\.]+", code):
        return None
    if re.search(r"[\u4e00-\u9fff]", raw):
        return None
    canonical = _resolve_market_suffix(suffix.upper())
    if not canonical:
        return None
    code_clean = code.upper().strip()
    if canonical == "CN":
        cls = _classify_ashare(code_clean)
        return f"{code_clean}.{cls}" if cls else None
    if canonical == "HK":
        return f"{_pad_hk(code_clean)}.HK"
    return f"{code_clean}.{canonical}"



def _parse_tb_ticker(raw: str) -> str | None:
    """Third Bridge: `NVDA US`, `300498 CN`, `1211 HK`, `HEXA B SE` (space-separated country)."""
    parts = raw.strip().split()
    if len(parts) < 2:
        return None
    country = parts[-1].upper()
    code = " ".join(parts[:-1]).strip()
    market = _TB_COUNTRY_MAP.get(country)
    if not market:
        return None

    if market == "CN":
        cls = _classify_ashare(code)
        return f"{code}.{cls}" if cls else None
    if market == "HK":
        return f"{_pad_hk(code)}.HK"

    # Some codes have internal spaces (e.g. "HEXA B") — keep as upper-case, spaces stripped
    code_clean = code.replace(" ", "").upper()
    return f"{code_clean}.{market}"


def _parse_jinmen_fullcode(fullcode: str) -> str | None:
    """Jinmen stores `fullCode` like `hk03896`, `sh601898`, `usKC`."""
    fullcode = fullcode.strip()
    m = re.match(r"^([a-z]{2,3})(.+)$", fullcode)
    if not m:
        return None
    market_in, code = m.group(1), m.group(2)
    return _canonical_from_code_market(code, market_in)


def _parse_bare(raw: str) -> str | None:
    """Bare strings like `INTC`, `AAPL`, `阳光电源`, `603061`, `03896`,
    or composite forms like `谷歌/Google` where meritco tag1 / alphapai labels
    pack CN+EN into one string.

    Priority:
      1. Alias table match (handles Chinese names + idiosyncratic mappings like MSFT)
      2. 6-digit CN code → SH/SZ/BJ classification
      3. 4-5 digit numeric → HK (padded)
      4. Pure upper-case alpha ≤ 6 → assume US
      5. Split on `/` or `／` and retry each half
      6. Give up
    """
    s = raw.strip()
    if not s:
        return None

    def _try(token: str) -> str | None:
        table = _alias_table()
        lc = _alias_lc_index()

        # Layered string lookup — tolerant to case + trailing-period variants.
        # Handles "Apple Inc" ↔ "Apple Inc." ↔ "APPLE INC" without polluting
        # the bulk table with every casing permutation.
        def _lookup(key: str) -> str | None:
            if not key:
                return None
            if key in table:
                return table[key]
            lk = key.lower()
            if lk in lc:
                return lc[lk]
            no_dot = key.rstrip(".").strip()
            if no_dot and no_dot != key:
                if no_dot in table:
                    return table[no_dot]
                if no_dot.lower() in lc:
                    return lc[no_dot.lower()]
            with_dot = no_dot + "." if no_dot else None
            if with_dot and with_dot != key:
                if with_dot in table:
                    return table[with_dot]
                if with_dot.lower() in lc:
                    return lc[with_dot.lower()]
            return None

        r = _lookup(token)
        if r:
            return r

        # Lookup-time EN legal-suffix stripping. ``Apple Inc`` /
        # ``NVIDIA Corporation`` / ``Tencent Holdings Ltd`` won't have an exact
        # entry but their bare-brand stem will. Iteratively peel one suffix per
        # pass (matches rebuild_aliases_bulk._en_stems shape).
        cur = token.strip()
        for _ in range(5):
            new = _EN_LEGAL_SUFFIX_RE.sub("", cur).strip().rstrip(",")
            if new == cur or len(new) < 2:
                break
            cur = new
            r = _lookup(cur)
            if r:
                return r

        if re.fullmatch(r"\d{6}", token):
            cls = _classify_ashare(token)
            if cls:
                return f"{token}.{cls}"
        if re.fullmatch(r"\d{3,5}", token):
            return f"{_pad_hk(token)}.HK"
        if re.fullmatch(r"[A-Z]{1,6}", token):
            return f"{token}.US"
        return None

    r = _try(s)
    if r:
        return r
    # Composite fallback: "谷歌/Google", "英伟达/NVIDIA", "美满科技／Marvell"
    for sep in ("/", "／"):
        if sep in s:
            for part in s.split(sep):
                p = part.strip()
                if not p:
                    continue
                r = _try(p)
                if r:
                    return r
    return None


# --------------------------------------------------------------------------- #
# Structured input adapters — one per known source shape
# --------------------------------------------------------------------------- #
def _from_alphapai_stock(d: Any) -> str | None:
    """`{code: "603061.SH", name: "金海通"}` or `{code: "AAPL.US", name: "苹果.US"}`."""
    if not isinstance(d, dict):
        return None
    code = d.get("code")
    if isinstance(code, str) and code:
        return _parse_dotted(code) or _parse_bare(code)
    name = d.get("name")
    if isinstance(name, str) and name:
        return _parse_bare(name)
    return None


def _from_jinmen_stock(d: Any) -> str | None:
    """`{name, code, fullCode, market}`."""
    if not isinstance(d, dict):
        return None
    fc = d.get("fullCode")
    if isinstance(fc, str) and fc:
        r = _parse_jinmen_fullcode(fc)
        if r:
            return r
    code = d.get("code")
    market = d.get("market")
    if isinstance(code, str) and code:
        r = _canonical_from_code_market(code, market) if market else None
        if r:
            return r
        # fallback: bare
        r = _parse_dotted(code) or _parse_bare(code)
        if r:
            return r
    name = d.get("name")
    if isinstance(name, str) and name:
        return _parse_bare(name)
    return None


def _from_gangtise_stock(d: Any) -> str | None:
    """Gangtise shape: summaries/researches store `stocks: [{code: "688315.SH", name: "诺禾致源", scr_id: ...}]`.
    chief_opinions `list_item.emoSecurities` stores `{code: "688072.SH", scrAbbr: "拓荆科技"}` (same shape),
    and `list_item.labelDisplays[].extra[]` uses `{code, scrAbbr, scrId}`. Handle all variants."""
    if not isinstance(d, dict):
        return None
    code = d.get("code") or d.get("gts_code") or d.get("gtsCode")
    if isinstance(code, str) and code:
        r = _parse_dotted(code) or _parse_bare(code)
        if r:
            return r
    name = d.get("name") or d.get("scr_abbr") or d.get("scrAbbr")
    if isinstance(name, str) and name:
        return _parse_bare(name)
    return None


def _from_acecamp_inner_corp(d: Any) -> str | None:
    """AceCamp list_item.corporations[]: {id, ticker: "US.INTC"/"HK.04335", name: "英特尔"}.
    Ticker is market-first; fallback to name alias."""
    if not isinstance(d, dict):
        return None
    tk = d.get("ticker")
    if isinstance(tk, str) and tk:
        r = _parse_reverse_dotted(tk) or _parse_dotted(tk) or _parse_bare(tk)
        if r:
            return r
    name = d.get("name")
    if isinstance(name, str) and name:
        return _parse_bare(name)
    return None


def _from_tb_company(d: Any) -> str | None:
    """Third Bridge `{label, ticker, country, sector, public}`."""
    if not isinstance(d, dict):
        return None
    tk = d.get("ticker")
    if isinstance(tk, str) and tk.strip():
        r = _parse_tb_ticker(tk)
        if r:
            return r
        r = _parse_dotted(tk) or _parse_bare(tk)
        if r:
            return r
    # try to extract ticker prefix from `label` like "DBK DE - Deutsche Bank AG"
    label = d.get("label")
    if isinstance(label, str) and label:
        m = re.match(r"^([\w\s.]+?)\s*-\s*", label)
        if m:
            prefix = m.group(1).strip()
            r = _parse_tb_ticker(prefix) or _parse_bare(prefix)
            if r:
                return r
        # else try alias match on full label (e.g. "Deutsche Bank AG")
        r = _parse_bare(label)
        if r:
            return r
    return None


# --------------------------------------------------------------------------- #
# Public entry points
# --------------------------------------------------------------------------- #
def normalize_one(raw: Any) -> str | None:
    """Single-item normalize. Returns canonical string or None."""
    if raw is None:
        return None
    if isinstance(raw, str):
        s = raw.strip()
        if not s:
            return None
        # Dotted canonical first (CODE.MARKET)
        r = _parse_dotted(s)
        if r:
            return r
        # Reverse-dotted (MARKET.CODE, AceCamp style)
        r = _parse_reverse_dotted(s)
        if r:
            return r
        # Colon-suffix (AlphaPai roadshows style: ARX:CA, TECK.B:CA)
        r = _parse_colon_suffix(s)
        if r:
            return r
        # Third-bridge "CODE COUNTRY" (space-separated)
        if " " in s and len(s.split()[-1]) == 2 and s.split()[-1].isupper():
            r = _parse_tb_ticker(s)
            if r:
                return r
        # Jinmen-style `hk03896`
        if re.match(r"^[a-z]{2,3}\d", s):
            r = _parse_jinmen_fullcode(s)
            if r:
                return r
        return _parse_bare(s)
    if isinstance(raw, dict):
        # Try each known shape adapter; first hit wins.
        # AceCamp-inner goes first because its `ticker: "US.INTC"` is the most specific signal.
        return (
            _from_acecamp_inner_corp(raw)
            or _from_gangtise_stock(raw)
            or _from_alphapai_stock(raw)
            or _from_jinmen_stock(raw)
            or _from_tb_company(raw)
        )
    return None


def _iter_flat(raw: Any) -> Iterable[Any]:
    """Flatten any nested input into individual candidate items."""
    if raw is None:
        return
    if isinstance(raw, list):
        for item in raw:
            yield from _iter_flat(item)
        return
    if isinstance(raw, dict):
        yield raw
        return
    yield raw


def normalize(raw: Any) -> list[str]:
    """Normalize any ticker reference (single value or list) → ordered canonical list.

    Accepts:
      - str: "INTC", "603061.SH", "hk03896", "1211 HK"
      - dict: {code, name, market, fullCode} or {ticker, label, country, ...}
      - list of the above
      - None / empty → []
    """
    seen: set[str] = set()
    out: list[str] = []
    for item in _iter_flat(raw):
        canonical = normalize_one(item)
        if canonical and canonical not in seen:
            seen.add(canonical)
            out.append(canonical)
    return out


def normalize_with_unmatched(raw: Any) -> tuple[list[str], list[str]]:
    """Like ``normalize()`` but also return the raw strings we could NOT map.

    Useful for the enrichment pass so operators can see what alias entries to add.
    """
    matched: list[str] = []
    unmatched: list[str] = []
    seen: set[str] = set()
    for item in _iter_flat(raw):
        canonical = normalize_one(item)
        if canonical:
            if canonical not in seen:
                seen.add(canonical)
                matched.append(canonical)
        else:
            # stringify for logging
            if isinstance(item, str) and item.strip():
                unmatched.append(item.strip())
            elif isinstance(item, dict):
                label = item.get("name") or item.get("label") or item.get("code") or item.get("ticker")
                if isinstance(label, str) and label.strip():
                    unmatched.append(label.strip())
    return matched, unmatched


# --------------------------------------------------------------------------- #
# Title / free-text extractor (2026-04-24)
# --------------------------------------------------------------------------- #
# Catches tickers embedded in parenthesized form inside free text like titles.
# Supported patterns (from empirical scan of the crawler corpus):
#   - Pluxee (PLX.PA): ...                   ← dot-separator
#   - ARC Resources Ltd. (ARX:CA) ...        ← colon-separator (AlphaPai style)
#   - Kakaku.com Inc.(2371.JPN)              ← 3-letter market (Jinmen style)
#   - Best Buy (BBY.N)                       ← 1-letter Bloomberg exchange
#   - 沙特阿美 (2222.SE)                       ← Saudi .SE (not Stockholm)
# Supports half-width `()` and full-width `（）` parens (mixed too).
# Skips empty parens and short legal-entity tokens to avoid "S.p.A." / "J.P." false hits.
_TEXT_TICKER_PAREN_RE = re.compile(
    r"[（(]"                                # open paren (half/full width)
    r"\s*"
    r"([0-9A-Z][0-9A-Za-z\.\-]{0,7}?)"             # code: starts with digit or CAP letter
    r"\s*[\.:]\s*"                                 # separator: dot or colon
    r"([A-Z]{1,4})"                                   # market suffix: 1-4 uppercase
    r"\s*"
    r"[）)]"                                # close paren (half/full width)
)


def extract_tickers_from_text(text: str | None) -> list[str]:
    """Scan free text for ``(CODE.MARKET)`` / ``(CODE:MARKET)`` patterns and return
    their canonical equivalents. Designed to be called as a fallback when the
    structured extractor returns nothing (e.g. jinmen.oversea_reports with empty
    ``stocks[]`` but ticker in title).

    Safety:
      - Ignores matches where MARKET doesn't resolve via ``_resolve_market_suffix``
      - Won't match bare ``CODE.MARKET`` outside parens (too many false positives
        from "S.p.A.", "N.V.", "J.P." in free text)
    """
    if not text or not isinstance(text, str):
        return []
    seen: set[str] = set()
    out: list[str] = []
    for m in _TEXT_TICKER_PAREN_RE.finditer(text):
        code, suffix = m.group(1), m.group(2)
        canonical = _resolve_market_suffix(suffix.upper())
        if not canonical:
            continue
        code_u = code.upper().strip(".")
        # Reject degenerate codes (e.g. only punctuation)
        if not re.search(r"[0-9A-Z]", code_u):
            continue
        # Reject very short all-caps codes that are likely legal-form abbreviations
        # (1-letter codes with 1-letter markets are suspicious)
        if len(code_u) == 1 and len(suffix) == 1:
            continue
        # Build canonical
        if canonical == "CN":
            cls = _classify_ashare(code_u)
            if not cls:
                continue
            canonical_str = f"{code_u}.{cls}"
        elif canonical == "HK":
            canonical_str = f"{_pad_hk(code_u)}.HK"
        elif canonical in ("SH", "SZ", "BJ"):
            canonical_str = f"{code_u}.{canonical}"
        else:
            canonical_str = f"{code_u}.{canonical}"
        if canonical_str not in seen:
            seen.add(canonical_str)
            out.append(canonical_str)
    return out


# --------------------------------------------------------------------------- #
# Per-collection extractors (know where each source puts its tickers)
# --------------------------------------------------------------------------- #
def extract_from_alphapai(doc: dict, collection: str) -> Any:
    """alphapai has category-specific locations."""
    if collection == "roadshows":
        return doc.get("stock")
    if collection in ("comments", "wechat_articles"):
        return (doc.get("list_item") or {}).get("stock")
    if collection == "reports":
        # Reports carry the covered companies under list_item.stock (same shape
        # as comments/wechat_articles). Prefer list_item because detail can be
        # {_err: ...} when the PDF-only doc couldn't be expanded.
        li = (doc.get("list_item") or {}).get("stock")
        if li:
            return li
        return (doc.get("detail") or {}).get("stock")
    return None


def extract_from_jinmen(doc: dict, collection: str) -> Any:
    """jinmen.meetings & jinmen.oversea_reports use `stocks[]`;
    jinmen.reports uses `companies[]` (different key names: stockcode/stockname/fullCode/market).
    Both share the canonical `fullCode` field that _from_jinmen_stock can parse."""
    if collection == "reports":
        return doc.get("companies")
    return doc.get("stocks")


def extract_from_meritco(doc: dict, _collection: str) -> Any:
    """forum uses `related_targets: [str]`; research has no such field —
    fall back to `list_item.tag1` which holds the expert's company name
    (e.g. "Neogen Corporation", "Bnp Paribas"). Names feed the alias table."""
    targets: list[Any] = []
    rt = doc.get("related_targets")
    if isinstance(rt, list):
        targets.extend(rt)
    elif isinstance(rt, str) and rt.strip():
        targets.append(rt)
    li = doc.get("list_item") or {}
    t1 = li.get("tag1")
    if isinstance(t1, str) and t1.strip():
        targets.append(t1.strip())
    return targets


def extract_from_thirdbridge(doc: dict, _collection: str) -> Any:
    targets = doc.get("target_companies") or []
    relevants = doc.get("relevant_companies") or []
    return list(targets) + list(relevants)


def extract_from_acecamp(doc: dict, _collection: str) -> Any:
    """AceCamp: prefer list_item.corporations[].ticker (`US.INTC`, `HK.04335`) —
    it's the most specific signal. Fall back to the outer `corporations` which
    only has `name` + empty `code/exchange` (name goes through alias table).
    """
    li = doc.get("list_item") or {}
    inner = li.get("corporations")
    if isinstance(inner, list) and inner:
        return inner
    return doc.get("corporations")


def extract_from_funda(doc: dict, collection: str) -> Any:
    if collection == "posts":
        ent = doc.get("entities") or {}
        return ent.get("tickers")
    # earnings_reports / earnings_transcripts have top-level `ticker` (scalar)
    return doc.get("ticker")


def extract_from_gangtise(doc: dict, collection: str) -> Any:
    """Gangtise tickers come from 3 shapes across 3 collections:
    - summaries / researches: top-level `stocks: [{code, name, scr_id, ...}]`
    - chief_opinions: `list_item.emoSecurities: [{code, scrAbbr, scrId}]`
                   + `list_item.labelDisplays[].extra[]` (same shape, fallback)
    Aggregate all available sources so re-run covers every collection uniformly."""
    out: list[Any] = []
    stocks = doc.get("stocks")
    if isinstance(stocks, list):
        out.extend(stocks)
    li = doc.get("list_item") or {}
    emo = li.get("emoSecurities")
    if isinstance(emo, list):
        out.extend(emo)
    for label in li.get("labelDisplays") or []:
        extras = (label or {}).get("extra") if isinstance(label, dict) else None
        if isinstance(extras, list):
            out.extend(extras)
    # researches store a deeper copy under list_item.aflScr.detail[]
    afl = li.get("aflScr")
    if isinstance(afl, dict):
        details = afl.get("detail")
        if isinstance(details, list):
            out.extend(details)
    # summaries store another copy at list_item.stock[]
    if collection == "summaries":
        li_stock = li.get("stock")
        if isinstance(li_stock, list):
            out.extend(li_stock)
    return out


def extract_from_alphaengine(doc: dict, _collection: str) -> Any:
    """AlphaEngine list items carry `company_codes` (Bloomberg-ish, e.g.
    ``600588.SH``, ``MMI.AX``, ``CPV.AX``) alongside readable ``company_names``.
    Return the parallel name/code pairs so the normalizer can pick whichever
    it recognises.
    """
    codes = doc.get("company_codes") or []
    names = doc.get("company_names") or []
    out: list[dict] = []
    if isinstance(codes, list):
        for i, c in enumerate(codes):
            if not c:
                continue
            out.append({"code": str(c).strip(),
                        "name": (names[i] if i < len(names) else "") or ""})
    # Also add any orphan names (rare, but news items sometimes have names
    # without codes — the normalizer can match via alias table).
    if isinstance(names, list):
        extra = len(names) - len(out)
        for i in range(max(0, extra)):
            n = names[len(codes) + i] if (len(codes) + i) < len(names) else None
            if n:
                out.append({"name": str(n).strip()})
    return out


def extract_from_semianalysis(doc: dict, _collection: str) -> Any:
    """SemiAnalysis (Substack) — cashtag-based extraction from title/subtitle/preview.
    Return raw strings; normalizer will alias-match via the cashtag table.
    """
    import re as _re
    raw: list[str] = []
    for field in ("title", "subtitle", "truncated_body_text"):
        txt = doc.get(field) or ""
        if isinstance(txt, str):
            for m in _re.findall(r"\$([A-Z]{1,5})(?:\.[A-Z]{1,5})?\b", txt):
                raw.append(m)
    for t in (doc.get("detail_result") or {}).get("postTags") or []:
        if isinstance(t, dict):
            n = t.get("name")
            if n: raw.append(str(n))
        elif isinstance(t, str):
            raw.append(t)
    return raw


EXTRACTORS = {
    "alphapai": extract_from_alphapai,
    "jinmen": extract_from_jinmen,
    "meritco": extract_from_meritco,
    "thirdbridge": extract_from_thirdbridge,
    "funda": extract_from_funda,
    "acecamp": extract_from_acecamp,
    "alphaengine": extract_from_alphaengine,
    "gangtise": extract_from_gangtise,
    "semianalysis": extract_from_semianalysis,
}
