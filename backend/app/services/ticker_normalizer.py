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

# Markets we know about — used to validate ".X" suffixes already in canonical form
_KNOWN_MARKETS: set[str] = {
    "SH", "SZ", "BJ", "HK", "US", "DE", "JP", "KS", "TW", "AU",
    "CA", "GB", "FR", "CH", "NL", "SE", "NO", "IT", "AT", "NZ", "HE",
}

# --------------------------------------------------------------------------- #
# Alias table (manually curated — see aliases.json)
# --------------------------------------------------------------------------- #
_ALIASES_PATH = Path(__file__).parent / "ticker_data" / "aliases.json"


@lru_cache(maxsize=1)
def _alias_table() -> dict[str, str]:
    """Load alias JSON. Values can be string (canonical) or null (known-unmappable)."""
    if not _ALIASES_PATH.exists():
        return {}
    try:
        data = json.loads(_ALIASES_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return {k: v for k, v in data.items() if not k.startswith("_") and isinstance(v, str)}


def reload_aliases() -> None:
    """Force re-read the alias JSON (useful after manual edit)."""
    _alias_table.cache_clear()


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
    """Input already looks like `CODE.SUFFIX` (e.g. `603061.SH`, `AAPL.US`)."""
    m = re.fullmatch(r"([\w\-]+)\.([A-Z]{1,3})", raw.strip())
    if not m:
        return None
    code, suffix = m.group(1).upper(), m.group(2).upper()
    # Chinese-name + .US / .HK pattern appears in alphapai (e.g. `苹果.US`) — skip, the sibling `code` field will have the real symbol
    if re.search(r"[\u4e00-\u9fff]", raw):
        return None
    if suffix in _KNOWN_MARKETS:
        # If HK, pad
        if suffix == "HK":
            return f"{_pad_hk(code)}.HK"
        return f"{code}.{suffix}"
    return None


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
        if token in table:
            return table[token]
        lower = token.lower()
        for k, v in table.items():
            if k.lower() == lower:
                return v
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
