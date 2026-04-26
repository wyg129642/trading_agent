"""Ticker resolution — confirm company name / bare code → canonical CODE.MARKET.

Exposed to the chat LLM as ``resolve_ticker`` so the model can disambiguate
before calling ``kb_search``. Covers three input shapes:

1. Canonical form (``NVDA.US``, ``00700.HK``) — passthrough with confirmation.
2. Bare code (``NVDA``, ``0700``, ``600519``) — expanded via the existing
   ``ticker_normalizer`` logic (HK zero-padding, A-share market triplication).
3. Company name (``腾讯``, ``Apple``, ``小米集团``) — resolved against the
   curated ``aliases.json`` + a reverse index over crawled ``_canonical_tickers``
   occurrences (so we can find popular companies by Chinese short name even if
   the alias table is incomplete).

The tool is read-only; it never mutates the alias table. Unmatched queries
come back with an empty ``matches`` list and a short explanation so the LLM
can retry with a more specific string.
"""
from __future__ import annotations

import logging
from typing import Any

from backend.app.services import ticker_normalizer
from backend.app.services.ticker_normalizer import _alias_table

logger = logging.getLogger(__name__)


# Market code → human-readable label (same set the corpus uses).
_MARKET_LABELS: dict[str, str] = {
    "US": "美股", "HK": "港股", "SH": "上交所", "SZ": "深交所", "BJ": "北交所",
    "DE": "德国", "JP": "日股", "KS": "韩股", "TW": "台股", "AU": "澳股",
    "CA": "加拿大", "GB": "伦交所", "FR": "法国", "CH": "瑞士", "NL": "荷兰",
    "SE": "瑞典", "NO": "挪威", "IT": "意大利", "AT": "奥地利", "NZ": "新西兰",
    "HE": "赫尔辛基",
}


def _build_reverse_index() -> dict[str, list[str]]:
    """Build an inverse name→[canonical] index from the alias table.

    Multiple aliases can map to the same ticker (e.g. ``Apple``, ``AAPL``,
    ``苹果`` → ``AAPL.US``); inversely, we need name→ticker. Returns a
    ``lower(name) → [canonical, ...]`` dict. Values are lists because some
    informal names (``小米``) are ambiguous across markets.
    """
    by_name: dict[str, list[str]] = {}
    inv_by_ticker: dict[str, list[str]] = {}
    for alias, canonical in _alias_table().items():
        if not canonical:
            continue
        inv_by_ticker.setdefault(canonical, []).append(alias)
        by_name.setdefault(alias.lower(), []).append(canonical)
    # Dedupe while preserving order.
    return {k: list(dict.fromkeys(v)) for k, v in by_name.items()}


def _lookup_by_name(q: str) -> list[str]:
    """Look up canonical tickers that match the query string.

    Priority:
      1. Exact alias hit (case-insensitive)
      2. Prefix hit (``小米`` matches ``小米集团``, ``小米汽车``)
      3. Substring hit (``腾讯控`` matches ``腾讯控股``)

    De-duplicated canonical list. Empty if nothing matches.
    """
    ql = q.strip().lower()
    if not ql:
        return []
    rev = _build_reverse_index()
    # Exact
    if ql in rev:
        return rev[ql]
    # Prefix — more precise than substring
    prefix_hits: list[str] = []
    for name, tickers in rev.items():
        if name.startswith(ql):
            prefix_hits.extend(tickers)
    if prefix_hits:
        return list(dict.fromkeys(prefix_hits))
    # Substring fallback (careful with very short queries).
    if len(ql) >= 2:
        sub_hits: list[str] = []
        for name, tickers in rev.items():
            if ql in name:
                sub_hits.extend(tickers)
        if sub_hits:
            return list(dict.fromkeys(sub_hits))
    return []


def _company_names_for(ticker: str) -> list[str]:
    """Given a canonical ticker, surface the known aliases (for display)."""
    names: list[str] = []
    for alias, canonical in _alias_table().items():
        if canonical == ticker:
            names.append(alias)
    # Prefer Chinese name first (informal convention in aliases.json).
    names.sort(key=lambda x: (all(ord(c) < 128 for c in x), len(x)))
    return names


def _resolve_one(q: str) -> dict[str, Any]:
    """Resolve a single query to one or more canonical tickers."""
    q = (q or "").strip()
    if not q:
        return {"query": q, "matches": [], "note": "空查询。"}

    # 1) If the input already looks canonical, confirm and return.
    variants = ticker_normalizer.normalize(q)
    if variants:
        # ``variants`` is a list — most inputs yield one canonical, but
        # 6-digit A-share codes fan out to SH/SZ/BJ.
        matches = [
            {
                "ticker": tk,
                "market": tk.split(".", 1)[1] if "." in tk else "",
                "market_label": _MARKET_LABELS.get(tk.split(".", 1)[1], "") if "." in tk else "",
                "names": _company_names_for(tk),
            }
            for tk in variants
        ]
        note = (
            "已识别为规范 ticker。" if len(matches) == 1
            else f"输入可能对应 {len(matches)} 个市场，请结合公司信息选择。"
        )
        return {"query": q, "matches": matches, "note": note}

    # 2) Name-based lookup against the curated alias table.
    name_matches = _lookup_by_name(q)
    if name_matches:
        matches = [
            {
                "ticker": tk,
                "market": tk.split(".", 1)[1] if "." in tk else "",
                "market_label": _MARKET_LABELS.get(tk.split(".", 1)[1], "") if "." in tk else "",
                "names": _company_names_for(tk),
            }
            for tk in name_matches
        ]
        note = (
            "唯一匹配。" if len(matches) == 1
            else f"匹配到 {len(matches)} 个候选，请根据公司信息判断。"
        )
        return {"query": q, "matches": matches, "note": note}

    # 3) Nothing hit.
    return {
        "query": q,
        "matches": [],
        "note": (
            "未能解析。如果是公司中文简称，可能 alias 表未收录——"
            "请直接传规范代码（NVDA.US / 00700.HK / 600519.SH）给 kb_search。"
        ),
    }


def resolve_tickers(queries: list[str]) -> list[dict[str, Any]]:
    """Batch resolve. Returns one result dict per query, in input order."""
    if not queries:
        return []
    return [_resolve_one(q) for q in queries if isinstance(q, str)]


# ── Chat tool surface ──────────────────────────────────────────────

RESOLVE_TICKER_TOOLS: list[dict[str, Any]] = [
    {
        "type": "function",
        "function": {
            "name": "resolve_ticker",
            "description": (
                "确认股票代码与公司的对应关系。输入公司名（中英文）或代码，"
                "返回每个查询对应的规范 ticker（CODE.MARKET）+ 市场 + 公司别名。"
                "**在 kb_search 之前调用**——尤其当用户用中文公司名、模糊指代、"
                "或裸代码（6 位 A 股可能在 SH/SZ/BJ 三市场）提问时。可一次传多个 query 并行确认。"
                "若返回多个候选，结合公司别名和市场信息选择最贴切的，再传给 kb_search.tickers。"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "queries": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": (
                            "公司名或代码列表。示例：['腾讯','NVDA','小米','600519','Apple']。"
                        ),
                    },
                },
                "required": ["queries"],
            },
        },
    },
]


def _format_resolve_result(results: list[dict[str, Any]]) -> str:
    """Markdown formatter for LLM consumption."""
    if not results:
        return "未收到有效查询。"
    lines: list[str] = []
    for r in results:
        q = r.get("query", "")
        matches = r.get("matches") or []
        note = r.get("note", "")
        if not matches:
            lines.append(f"**{q}** → ❌ 未解析\n  - {note}")
            continue
        if len(matches) == 1:
            m = matches[0]
            names = ("，".join(m.get("names") or [])) or "(无别名记录)"
            lines.append(
                f"**{q}** → ✅ `{m['ticker']}` ({m.get('market_label','')}) — {names}"
            )
        else:
            lines.append(f"**{q}** → ⚠ {len(matches)} 个候选，请选择：")
            for i, m in enumerate(matches, start=1):
                names = ("，".join(m.get("names") or [])) or "(无别名记录)"
                lines.append(
                    f"  {i}. `{m['ticker']}` ({m.get('market_label','')}) — {names}"
                )
            if note:
                lines.append(f"  - {note}")
    return "\n".join(lines)


async def execute_tool(
    name: str,
    arguments: dict[str, Any],
    citation_tracker: Any = None,  # unused; resolve_ticker produces no citations
) -> str:
    if name != "resolve_ticker":
        return f"未知的 ticker 工具: {name}"
    queries = arguments.get("queries") or []
    if not isinstance(queries, list):
        return "参数 queries 必须是字符串数组。"
    try:
        results = resolve_tickers([str(q) for q in queries])
    except Exception as e:
        logger.exception("resolve_ticker failed: %s", e)
        return f"resolve_ticker 执行失败: {e}"
    return _format_resolve_result(results)
