"""Per-platform ``ticker_tags`` shape builder for DB API detail endpoints.

Frontend ``<TickerTagsTabs/>`` widget renders the same 3-tab layout
(上游原标 / 规则 canonical / LLM)across all 8 platform DB pages, so every
backend detail endpoint surfaces the same shape::

    {
      "raw":               [...],   # platform-native ticker field, picked clean
      "canonical":         [...],   # _canonical_tickers (rule-path matches)
      "canonical_source":  str|None,# _canonical_extract_source ("alphapai", "alphapai_title")
      "canonical_at":      datetime|None,
      "unmatched_raw":     [...],   # _unmatched_raw (rule-path misses)
      "llm_canonical":     [...],   # _llm_canonical_tickers (LLM-path matches)
      "llm_source":        str|None,# _llm_extract_source ("alphapai_llm:qwen-plus")
      "llm_at":            datetime|None,
      "llm_unmatched_raw": [...],   # _llm_unmatched_raw (incl mention_drop entries)
    }

For the **raw** tab we pick the cleanest *single* native field per platform
(see TICKER_AGGREGATION.md §4.1) — explicitly NOT the multi-source merge that
``EXTRACTORS`` does for canonicalization. Reason: gangtise's normalization
extractor merges ``doc.stocks + list_item.{stock,emoSecurities,labelDisplays
.extra,aflScr.detail}`` to maximize canonical recall, but for *display* that
yields the same stock duplicated 2-5x in different schema views. Display
should show the schema with the most metadata once.
"""
from __future__ import annotations

from typing import Any


# --------------------------------------------------------------------------- #
# Per-platform display-raw extractors. Return a list of dicts/strings ready to
# render as chips by the frontend. Prefers the schema with the most readable
# `name` + `code` (or just code/name string) and avoids merging multi-schema
# duplicates.
# --------------------------------------------------------------------------- #
def _alphapai_display_raw(doc: dict, collection: str) -> list:
    if collection == "roadshows":
        return _coerce_list(doc.get("stock"))
    if collection in ("comments", "wechat_articles"):
        li = doc.get("list_item") or {}
        return _coerce_list(li.get("stock") if isinstance(li, dict) else None)
    if collection == "reports":
        li = doc.get("list_item") or {}
        s = li.get("stock") if isinstance(li, dict) else None
        if s:
            return _coerce_list(s)
        d = doc.get("detail") or {}
        return _coerce_list(d.get("stock") if isinstance(d, dict) else None)
    return []


def _jinmen_display_raw(doc: dict, collection: str) -> list:
    if collection == "reports":
        return _coerce_list(doc.get("companies"))
    return _coerce_list(doc.get("stocks"))


def _meritco_display_raw(doc: dict, _collection: str) -> list:
    out: list = []
    rt = doc.get("related_targets")
    if isinstance(rt, list):
        out.extend([t for t in rt if isinstance(t, str) and t.strip()])
    elif isinstance(rt, str) and rt.strip():
        out.append(rt.strip())
    li = doc.get("list_item") or {}
    if isinstance(li, dict):
        t1 = li.get("tag1")
        if isinstance(t1, str) and t1.strip() and t1.strip() not in out:
            out.append(t1.strip())
    return out


def _thirdbridge_display_raw(doc: dict, _collection: str) -> list:
    out: list = []
    for key in ("target_companies", "relevant_companies"):
        for c in (doc.get(key) or []):
            if isinstance(c, dict):
                out.append(c)
    return out


def _funda_display_raw(doc: dict, collection: str) -> list:
    if collection == "posts":
        ent = doc.get("entities") or {}
        tickers = ent.get("tickers") if isinstance(ent, dict) else None
        return [t for t in (tickers or []) if isinstance(t, str) and t.strip()]
    if collection == "sentiments":
        # SentimenTrader docs key by ticker; surface that
        t = doc.get("ticker")
        return [t] if isinstance(t, str) and t.strip() else []
    # earnings_reports / earnings_transcripts: scalar `ticker`
    t = doc.get("ticker")
    return [t] if isinstance(t, str) and t.strip() else []


def _acecamp_display_raw(doc: dict, _collection: str) -> list:
    li = doc.get("list_item") or {}
    inner = li.get("corporations") if isinstance(li, dict) else None
    if isinstance(inner, list) and inner:
        return [c for c in inner if isinstance(c, dict)]
    outer = doc.get("corporations") or []
    return [c for c in outer if isinstance(c, dict)]


def _gangtise_display_raw(doc: dict, collection: str) -> list:
    """Single-source pick (avoid the 5-source merge that EXTRACTORS does)."""
    # summaries / researches: doc.stocks is the cleanest version with both code + name
    if collection in ("summaries", "researches"):
        s = doc.get("stocks")
        if isinstance(s, list) and s:
            return [x for x in s if isinstance(x, dict)]
        # fallback to list_item.stock if doc.stocks empty (rare)
        li = doc.get("list_item") or {}
        ls = li.get("stock") if isinstance(li, dict) else None
        if isinstance(ls, list) and ls:
            return [x for x in ls if isinstance(x, dict)]
    # chief_opinions has no doc.stocks; use list_item.emoSecurities
    if collection == "chief_opinions":
        li = doc.get("list_item") or {}
        emo = li.get("emoSecurities") if isinstance(li, dict) else None
        if isinstance(emo, list) and emo:
            # filter out the literal-string "null" code/scrId that gangtise sometimes ships
            return [x for x in emo if isinstance(x, dict) and x.get("code") not in ("null", None, "")]
    return []


def _alphaengine_display_raw(doc: dict, _collection: str) -> list:
    codes = doc.get("company_codes") or []
    names = doc.get("company_names") or []
    if not isinstance(codes, list):
        codes = []
    if not isinstance(names, list):
        names = []
    out: list = []
    for i, c in enumerate(codes):
        if not c:
            continue
        item = {"code": str(c).strip()}
        if i < len(names) and names[i]:
            item["name"] = str(names[i]).strip()
        out.append(item)
    # Trailing names that have no matching code (rare)
    if len(names) > len(codes):
        for n in names[len(codes):]:
            if n:
                out.append({"name": str(n).strip()})
    return out


def _semianalysis_display_raw(doc: dict, _collection: str) -> list:
    import re as _re
    out: list = []
    for field in ("title", "subtitle", "truncated_body_text"):
        txt = doc.get(field) or ""
        if isinstance(txt, str):
            for m in _re.findall(r"\$([A-Z]{1,5})(?:\.[A-Z]{1,5})?\b", txt):
                if m not in out:
                    out.append(m)
    for t in (doc.get("detail_result") or {}).get("postTags") or []:
        if isinstance(t, dict):
            n = t.get("name")
            if n and n not in out:
                out.append(str(n))
        elif isinstance(t, str) and t not in out:
            out.append(t)
    return out


_DISPLAY_RAW_EXTRACTORS = {
    "alphapai": _alphapai_display_raw,
    "jinmen": _jinmen_display_raw,
    "meritco": _meritco_display_raw,
    "thirdbridge": _thirdbridge_display_raw,
    "funda": _funda_display_raw,
    "acecamp": _acecamp_display_raw,
    "gangtise": _gangtise_display_raw,
    "alphaengine": _alphaengine_display_raw,
    "semianalysis": _semianalysis_display_raw,
}


def _coerce_list(v: Any) -> list:
    """Accept None / dict / list / str. Return a flat list (preserve dicts)."""
    if v is None:
        return []
    if isinstance(v, list):
        return [x for x in v if x is not None]
    return [v]


def build_ticker_tags(doc: dict | None, source: str, collection: str) -> dict:
    """Build the ``ticker_tags`` response shape from a Mongo document.

    ``source`` is the canonical platform key (``alphapai`` / ``jinmen`` /
    ``meritco`` / ``thirdbridge`` / ``funda`` / ``acecamp`` / ``gangtise`` /
    ``alphaengine`` / ``semianalysis``). ``collection`` is the Mongo collection
    name (e.g. ``roadshows`` / ``reports`` / ``meetings`` / ``forum``).

    Robust to ``None`` doc and to any of the underlying fields being missing.
    """
    if not isinstance(doc, dict):
        doc = {}
    extractor = _DISPLAY_RAW_EXTRACTORS.get(source)
    raw = extractor(doc, collection) if extractor else []
    return {
        "raw":               raw,
        "canonical":         doc.get("_canonical_tickers") or [],
        "canonical_source":  doc.get("_canonical_extract_source"),
        "canonical_at":      doc.get("_canonical_tickers_at"),
        "unmatched_raw":     doc.get("_unmatched_raw") or [],
        "llm_canonical":     doc.get("_llm_canonical_tickers") or [],
        "llm_source":        doc.get("_llm_extract_source"),
        "llm_at":            doc.get("_llm_canonical_tickers_at"),
        "llm_unmatched_raw": doc.get("_llm_unmatched_raw") or [],
    }


__all__ = ["build_ticker_tags"]
