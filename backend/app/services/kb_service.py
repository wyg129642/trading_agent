"""Phase A knowledge base service — filter-first + text match over 7 local Mongo platforms.

Exposes three tools to the chat LLM:

- **kb_search**: Metadata filter (ticker / date / doc_type / source) + in-memory relevance
  scoring (char bigram for CN, token substring for EN, title boost, recency decay)
  across all 16 collections, concurrently. Returns the top-N hits, each with a
  stable `doc_id` the LLM can pass to `kb_fetch_document`.

- **kb_fetch_document**: Read the full text (up to 30 k chars) of a hit by its KB
  ``doc_id`` ("<source>:<collection>:<_id>"). Used when the snippet from
  `kb_search` isn't enough and the LLM needs the original context.

- **kb_list_facets**: Count docs along a dimension (sources / doc_types / tickers /
  date_histogram) subject to the same filter stack. For discovery before searching
  — "how many broker reports on NVDA in the last 3 months?".

No embeddings, no vector store, no reranker — see ``docs/knowledge_base_plan.md``
addendum for why Phase A is this simple. The service is designed to swap for a
vector path in Phase B without changing the tool surface.
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection

from backend.app.config import get_settings
from backend.app.services import ticker_normalizer

logger = logging.getLogger(__name__)


# ── Collection specs ─────────────────────────────────────────────


@dataclass(frozen=True)
class CollectionSpec:
    """Describes how to read one MongoDB collection as a KB source.

    A single normalization layer keeps kb_service agnostic of per-platform quirks.
    """
    db: str                           # mongo database name ("alphapai", "jinmen", ...)
    collection: str                   # mongo collection name
    doc_type: str                     # canonical doc type enum (tool schema enum)
    doc_type_cn: str                  # human-readable Chinese label (UI + formatted text)
    title_field: str                  # field holding the document title
    text_fields: tuple[str, ...]      # primary first; first non-empty wins for body
    date_str_field: str | None        # "YYYY-MM-DD HH:MM" style field, if any
    date_ms_field: str | None         # epoch ms integer field, if any
    ticker_field: str                 # usually "_canonical_tickers"
    ticker_fallback_path: str | None  # "stocks" (dicts with .code) / "companies" / None
    institution_field: str | None     # None = no institution metadata
    institution_kind: str             # "str" | "list_dict_name"
    url_field: str | None             # web url if present
    has_pdf: bool                     # collection stores PDFs
    low_quality: bool = False         # if True, excluded from default search (e.g. WeChat aggregators)
    milvus_indexed: bool = True       # if False, Phase B hybrid_search has no data; kb.search routes to Phase A only


def _build_specs() -> list[CollectionSpec]:
    return [
        # ─── AlphaPai (publish_time string) ─────────────────────
        # WeChat公众号文章质量低，标记为 low_quality；默认被 _pick_specs 排除，
        # 仅当 LLM 显式传 sources=["alphapai"] 且 doc_types 含 "wechat_article"
        # 或设置 include_low_quality=True 时才纳入搜索。
        CollectionSpec(
            db="alphapai", collection="wechat_articles",
            doc_type="wechat_article", doc_type_cn="微信文章",
            title_field="title", text_fields=("content",),
            date_str_field="publish_time", date_ms_field=None,
            ticker_field="_canonical_tickers", ticker_fallback_path=None,
            institution_field="accountName", institution_kind="str",
            url_field="url", has_pdf=False,
            low_quality=True,
        ),
        CollectionSpec(
            db="alphapai", collection="comments",
            doc_type="comment", doc_type_cn="券商点评",
            title_field="title", text_fields=("content",),
            date_str_field="publish_time", date_ms_field=None,
            ticker_field="_canonical_tickers", ticker_fallback_path=None,
            institution_field="institution", institution_kind="list_dict_name",
            url_field=None, has_pdf=False,
        ),
        CollectionSpec(
            db="alphapai", collection="reports",
            doc_type="report", doc_type_cn="券商研报",
            title_field="title", text_fields=("pdf_text_md", "content"),
            date_str_field="publish_time", date_ms_field=None,
            ticker_field="_canonical_tickers", ticker_fallback_path=None,
            institution_field="institution", institution_kind="list_dict_name",
            url_field=None, has_pdf=True,
        ),
        CollectionSpec(
            db="alphapai", collection="roadshows",
            doc_type="roadshow", doc_type_cn="路演纪要",
            title_field="title", text_fields=("content",),
            date_str_field="publish_time", date_ms_field=None,
            ticker_field="_canonical_tickers", ticker_fallback_path=None,
            institution_field="publishInstitution", institution_kind="str",
            url_field=None, has_pdf=False,
        ),
        # ─── Jinmen (meetings: string, reports: ms) ─────────────
        CollectionSpec(
            db="jinmen", collection="meetings",
            doc_type="meeting", doc_type_cn="进门会议纪要",
            title_field="title",
            text_fields=("transcript_md", "chapter_summary_md", "points_md"),
            date_str_field="release_time", date_ms_field=None,
            ticker_field="_canonical_tickers", ticker_fallback_path=None,
            institution_field="organization", institution_kind="str",
            url_field="present_url", has_pdf=False,
        ),
        CollectionSpec(
            db="jinmen", collection="reports",
            doc_type="jinmen_report", doc_type_cn="进门研报",
            title_field="title",
            text_fields=("pdf_text_md", "summary_md", "summary_point_md"),
            date_str_field=None, date_ms_field="release_time_ms",
            ticker_field="_canonical_tickers", ticker_fallback_path="companies",
            institution_field="organization_name", institution_kind="str",
            url_field="link_url", has_pdf=True,
        ),
        CollectionSpec(
            db="jinmen", collection="oversea_reports",
            doc_type="jinmen_oversea_report", doc_type_cn="进门海外研报",
            title_field="title",
            text_fields=("pdf_text_md", "summary_md", "summary_point_md", "content_md"),
            date_str_field=None, date_ms_field="release_time_ms",
            ticker_field="_canonical_tickers", ticker_fallback_path="companies",
            institution_field="organization_name", institution_kind="str",
            url_field="link_url", has_pdf=True,
            milvus_indexed=False,
        ),
        # ─── Meritco ────────────────────────────────────────────
        CollectionSpec(
            db="meritco", collection="forum",
            doc_type="expert_call", doc_type_cn="专家交流",
            title_field="title",
            text_fields=("pdf_text_md", "content_md", "insight_md", "summary_md"),
            date_str_field=None, date_ms_field="release_time_ms",
            ticker_field="_canonical_tickers", ticker_fallback_path=None,
            institution_field="author", institution_kind="str",
            url_field=None, has_pdf=True,
        ),
        CollectionSpec(
            db="meritco", collection="research",
            doc_type="meritco_research", doc_type_cn="久谦研究",
            title_field="title",
            text_fields=("content_md", "insight_md", "summary_md"),
            date_str_field=None, date_ms_field="release_time_ms",
            ticker_field="_canonical_tickers", ticker_fallback_path=None,
            institution_field="author", institution_kind="str",
            url_field=None, has_pdf=False,
            milvus_indexed=False,
        ),
        # ─── Third Bridge ───────────────────────────────────────
        CollectionSpec(
            db="thirdbridge", collection="interviews",
            doc_type="expert_interview", doc_type_cn="专家访谈",
            title_field="title",
            text_fields=("transcript_md", "commentary_md", "introduction_md", "agenda_md"),
            date_str_field=None, date_ms_field="release_time_ms",
            ticker_field="_canonical_tickers", ticker_fallback_path=None,
            institution_field=None, institution_kind="str",
            url_field=None, has_pdf=False,
        ),
        # ─── Funda ──────────────────────────────────────────────
        CollectionSpec(
            db="funda", collection="earnings_transcripts",
            doc_type="earnings_transcript", doc_type_cn="业绩会纪要",
            title_field="title", text_fields=("content_md",),
            date_str_field=None, date_ms_field="release_time_ms",
            ticker_field="_canonical_tickers", ticker_fallback_path=None,
            institution_field=None, institution_kind="str",
            url_field="web_url", has_pdf=False,
        ),
        CollectionSpec(
            db="funda", collection="earnings_reports",
            doc_type="earnings_report", doc_type_cn="业绩研报",
            title_field="title", text_fields=("content_md",),
            date_str_field=None, date_ms_field="release_time_ms",
            ticker_field="_canonical_tickers", ticker_fallback_path=None,
            institution_field=None, institution_kind="str",
            url_field="web_url", has_pdf=False,
        ),
        CollectionSpec(
            db="funda", collection="posts",
            doc_type="post", doc_type_cn="Funda点评",
            title_field="title", text_fields=("content_md",),
            date_str_field=None, date_ms_field="release_time_ms",
            ticker_field="_canonical_tickers", ticker_fallback_path=None,
            institution_field=None, institution_kind="str",
            url_field="web_url", has_pdf=False,
        ),
        # (funda.sentiments has no text — skipped from search; still usable via facets.)
        # ─── Gangtise (岗底斯) ──────────────────────────────────
        CollectionSpec(
            db="gangtise", collection="chief_opinions",
            doc_type="chief_opinion", doc_type_cn="首席观点",
            title_field="title", text_fields=("content_md", "brief_md"),
            date_str_field=None, date_ms_field="release_time_ms",
            ticker_field="_canonical_tickers", ticker_fallback_path=None,
            institution_field="organization", institution_kind="str",
            url_field="web_url", has_pdf=False,
        ),
        CollectionSpec(
            db="gangtise", collection="researches",
            doc_type="gangtise_research", doc_type_cn="岗底斯研报",
            title_field="title", text_fields=("pdf_text_md", "content_md", "brief_md"),
            date_str_field=None, date_ms_field="release_time_ms",
            ticker_field="_canonical_tickers", ticker_fallback_path="stocks",
            institution_field="organization", institution_kind="str",
            url_field="web_url", has_pdf=True,
        ),
        CollectionSpec(
            db="gangtise", collection="summaries",
            doc_type="gangtise_summary", doc_type_cn="岗底斯纪要",
            title_field="title", text_fields=("content_md", "brief_md"),
            date_str_field=None, date_ms_field="release_time_ms",
            ticker_field="_canonical_tickers", ticker_fallback_path="stocks",
            institution_field="organization", institution_kind="str",
            url_field="web_url", has_pdf=False,
        ),
        # ─── Acecamp ────────────────────────────────────────────
        CollectionSpec(
            db="acecamp", collection="articles",
            doc_type="acecamp_article", doc_type_cn="峰会文章",
            title_field="title",
            text_fields=("content_md", "summary_md", "transcribe_md"),
            date_str_field=None, date_ms_field="release_time_ms",
            ticker_field="_canonical_tickers", ticker_fallback_path=None,
            institution_field="organization", institution_kind="str",
            url_field="web_url", has_pdf=False,
        ),
        CollectionSpec(
            db="acecamp", collection="opinions",
            doc_type="acecamp_opinion", doc_type_cn="峰会观点",
            title_field="title",
            text_fields=("content_md", "summary_md"),
            date_str_field=None, date_ms_field="release_time_ms",
            ticker_field="_canonical_tickers", ticker_fallback_path=None,
            institution_field="organization", institution_kind="str",
            url_field="web_url", has_pdf=False,
            milvus_indexed=False,
        ),
        # ─── AlphaEngine (阿尔法引擎) ──────────────────────────
        CollectionSpec(
            db="alphaengine", collection="summaries",
            doc_type="alphaengine_summary", doc_type_cn="阿尔法引擎纪要",
            title_field="title",
            text_fields=("content_md", "doc_introduce"),
            date_str_field=None, date_ms_field="release_time_ms",
            ticker_field="_canonical_tickers", ticker_fallback_path=None,
            institution_field="organization", institution_kind="str",
            url_field="web_url", has_pdf=False,
            milvus_indexed=False,
        ),
        CollectionSpec(
            db="alphaengine", collection="china_reports",
            doc_type="alphaengine_china_report", doc_type_cn="阿尔法引擎内资研报",
            title_field="title",
            text_fields=("pdf_text_md", "content_md", "doc_introduce"),
            date_str_field=None, date_ms_field="release_time_ms",
            ticker_field="_canonical_tickers", ticker_fallback_path=None,
            institution_field="organization", institution_kind="str",
            url_field="web_url", has_pdf=True,
            milvus_indexed=False,
        ),
        CollectionSpec(
            db="alphaengine", collection="foreign_reports",
            doc_type="alphaengine_foreign_report", doc_type_cn="阿尔法引擎外资研报",
            title_field="title",
            text_fields=("pdf_text_md", "content_md", "doc_introduce"),
            date_str_field=None, date_ms_field="release_time_ms",
            ticker_field="_canonical_tickers", ticker_fallback_path=None,
            institution_field="organization", institution_kind="str",
            url_field="web_url", has_pdf=True,
            milvus_indexed=False,
        ),
        CollectionSpec(
            db="alphaengine", collection="news_items",
            doc_type="alphaengine_news", doc_type_cn="阿尔法引擎资讯",
            title_field="title",
            text_fields=("content_md", "doc_introduce"),
            date_str_field=None, date_ms_field="release_time_ms",
            ticker_field="_canonical_tickers", ticker_fallback_path=None,
            institution_field="organization", institution_kind="str",
            url_field="web_url", has_pdf=False,
            milvus_indexed=False,
        ),
        # SemiAnalysis — lives in its own foreign-website DB (see MONGO_DB_ALIASES)
        CollectionSpec(
            db="semianalysis", collection="semianalysis_posts",
            doc_type="semianalysis_post", doc_type_cn="SemiAnalysis 研究",
            title_field="title",
            text_fields=("content_md", "subtitle", "truncated_body_text"),
            date_str_field=None, date_ms_field="release_time_ms",
            ticker_field="_canonical_tickers", ticker_fallback_path=None,
            institution_field="organization", institution_kind="str",
            url_field="canonical_url", has_pdf=False,
            milvus_indexed=False,
        ),
    ]


SPECS_LIST: list[CollectionSpec] = _build_specs()
SPECS_BY_KEY: dict[str, CollectionSpec] = {f"{s.db}/{s.collection}": s for s in SPECS_LIST}
SPECS_BY_DOC_TYPE: dict[str, CollectionSpec] = {s.doc_type: s for s in SPECS_LIST}

ALL_SOURCES = sorted({s.db for s in SPECS_LIST})
# Doc-type enums exposed to the LLM exclude ``low_quality`` ones — the LLM
# shouldn't be nudged into requesting WeChat aggregator content. If a future
# use case needs it, pass ``include_low_quality=True`` to ``search()``.
ALL_DOC_TYPES = [s.doc_type for s in SPECS_LIST if not s.low_quality]
ALL_DOC_TYPES_WITH_LOW_QUALITY = [s.doc_type for s in SPECS_LIST]


# ── Mongo client + DB name mapping ──────────────────────────────
#
# The crawler Mongo was migrated to the remote node 192.168.31.176:35002 on
# 2026-04-23 (databases renamed with `-full` suffixes, meritco → jiuqian-full,
# thirdbridge → third-bridge) and migrated back to local ta-mongo-crawl
# :27018 on 2026-04-26. The `-full` DB names persisted across both moves.
#
# CollectionSpec.db values stay at the OLD short names ("alphapai", "jinmen",
# ...) because those strings are baked into:
#   • The 400k+ chunks already in Milvus (kb_chunks.db field)
#   • The LLM-facing `sources` enum in KB_TOOLS
#   • The citation source_type discriminator (frontend)
#   • doc_id format "<db>:<collection>:<_id>" used by kb_fetch_document
#
# So we translate spec.db → actual Mongo DB name at the I/O boundary only.
# All new Mongo-facing code MUST go through `_mongo_db_for(spec)` — do NOT
# hardcode `mc[spec.db]` any more.

MONGO_DB_ALIASES: dict[str, str] = {
    "alphapai":    "alphapai-full",
    "jinmen":      "jinmen-full",
    "meritco":     "jiuqian-full",
    "thirdbridge": "third-bridge",
    "gangtise":    "gangtise-full",
    # funda, acecamp unchanged — no entry means "use spec.db verbatim".
    # semianalysis lives in its own foreign-website DB (2026-04-24 迁出 funda).
    "semianalysis": "foreign-website",
}


def mongo_db_name_for(spec: "CollectionSpec | str") -> str:
    """Return the actual Mongo database name for a CollectionSpec or spec.db label.

    Callers outside this module should use this rather than ``spec.db`` when
    forming Mongo queries — ``spec.db`` is the stable LLM/Milvus label, NOT
    the physical DB name after the 2026-04-23 migration.
    """
    key = spec.db if isinstance(spec, CollectionSpec) else str(spec)
    return MONGO_DB_ALIASES.get(key, key)


@lru_cache(maxsize=1)
def _get_client() -> AsyncIOMotorClient:
    """Singleton Motor client. All 11 per-platform `*_mongo_uri` settings
    resolve to a single URI from `REMOTE_CRAWL_MONGO_URI` (in turn the
    `MONGO_URI` env var, default `mongodb://127.0.0.1:27018/` =
    `ta-mongo-crawl` container — see config.py)."""
    settings = get_settings()
    return AsyncIOMotorClient(
        settings.alphapai_mongo_uri,
        tz_aware=True,
        serverSelectionTimeoutMS=5000,
        connectTimeoutMS=5000,
        socketTimeoutMS=30000,
        maxPoolSize=50,
    )


def _coll(spec: CollectionSpec) -> AsyncIOMotorCollection:
    return _get_client()[mongo_db_name_for(spec)][spec.collection]


# ── Ticker normalization ─────────────────────────────────────────


_CANONICAL_TICKER_RE = re.compile(r"^[A-Z0-9]+\.[A-Z]+$")


def normalize_ticker_input(raw: str) -> list[str]:
    """Expand a user-supplied ticker into canonical variants used by the corpus.

    The crawler pipeline stores HK equities as **5-digit zero-padded** canonical
    strings (e.g. ``00700.HK``), so any user-supplied ``0700.HK`` or ``700.HK``
    must be expanded to ``00700.HK`` — otherwise the filter silently misses.

    Accepts:
      - Canonical form: ``NVDA.US``, ``00700.HK``, ``600519.SH``
      - HK short form: ``0700.HK`` → ``00700.HK``
      - Bare ticker: ``NVDA`` → ``NVDA.US``; ``0700`` → ``00700.HK``
      - 6-digit A-share code: ``600519`` → ``600519.SH`` (resolved via prefix
        classification; falls back to all three markets when prefix unknown)
      - Chinese company name: ``英伟达`` → ``NVDA.US`` (via curated alias table)
      - English company name: ``Intel`` → ``INTC.US``, ``Apple`` → ``AAPL.US``

    Routes through ``ticker_normalizer.normalize_one`` first — it handles the
    Bloomberg/Reuters/Jinmen suffix family plus the alias JSON. Falls back to a
    local heuristic only for inputs the curated parsers cannot resolve.

    **Hallucination guard (LLM-input boundary):** when the input looks like
    ``CODE.MARKET`` but the resulting canonical isn't a known listing in our
    snapshot (e.g. LLM-fabricated ``BABA.HK`` / ``9988.US`` / ``TSMC.US``), we
    try recovering by parsing the code-part as a name (``BABA`` → ``BABA.US``)
    and otherwise drop the filter so the search degrades to pure semantic
    rather than returning zero hits on a syntactically valid but nonexistent
    ticker.
    """
    if not raw:
        return []
    s = raw.strip()
    if not s:
        return []
    # Heuristic: input looks like CODE.MARKET (no spaces, no Chinese, single
    # dot followed by 1-4 letters). Used only to decide whether to validate.
    looks_dotted = bool(re.fullmatch(r"[A-Za-z0-9\-]+\.[A-Za-z]{1,4}", s))

    # Primary path: curated alias table + multi-format parsers handle Chinese
    # names, English company names, dotted/colon/space variants, etc.
    canonical = ticker_normalizer.normalize_one(s)
    if canonical:
        if looks_dotted and not ticker_normalizer.is_known_canonical(canonical):
            # Recovery: parse just the code-part (`BABA` from `BABA.HK`) — if
            # it resolves to a *known* canonical, prefer that. This rescues
            # market-suffix hallucinations (`BABA.HK` → `BABA.US`,
            # `9988.US` → `09988.HK`).
            code_part = s.rsplit(".", 1)[0]
            recovered = ticker_normalizer.normalize_one(code_part)
            if (recovered and recovered != canonical
                    and ticker_normalizer.is_known_canonical(recovered)):
                logger.info(
                    "normalize_ticker_input: recovered hallucinated %r → %r "
                    "(was %r, no such listing)",
                    s, recovered, canonical,
                )
                return [recovered]
            logger.info(
                "normalize_ticker_input: dropping hallucinated %r → %r "
                "(no such listing in alias snapshot)",
                s, canonical,
            )
            return []
        return [canonical]

    # Fallback heuristic — reached only for inputs the alias table does not
    # cover (e.g. brand-new IPOs, unknown 6-digit codes).
    t = s.upper()
    if _CANONICAL_TICKER_RE.match(t):
        code, market = t.split(".", 1)
        if market == "HK" and code.isdigit() and len(code) < 5:
            padded = f"{code.zfill(5)}.HK"
            if ticker_normalizer.is_known_canonical(padded):
                return [padded]
            return []
        if ticker_normalizer.is_known_canonical(t):
            return [t]
        return []  # syntactic CODE.MARKET but not a known listing → drop
    if t.isdigit():
        variants: list[str] = []
        if len(t) <= 5:
            variants.append(f"{t.zfill(5)}.HK")
        if len(t) == 6:
            variants += [f"{t}.SH", f"{t}.SZ", f"{t}.BJ"]
        elif len(t) < 6:
            padded = t.zfill(6)
            variants += [f"{padded}.SH", f"{padded}.SZ"]
        # Filter to known when we have a hit; otherwise fall back to all
        # candidates so brand-new IPOs (not yet in our alias snapshot) still
        # match if the corpus has them.
        valid = [v for v in variants if ticker_normalizer.is_known_canonical(v)]
        return valid or variants
    if re.match(r"^[A-Z][A-Z0-9.]*$", t):
        guess = f"{t}.US"
        if ticker_normalizer.is_known_canonical(guess):
            return [guess]
        return []  # drop fabricated US tickers (e.g. BYTEDANCE.US)
    return []  # garbage — drop, let semantic search take over


# ── Date handling ───────────────────────────────────────────────


_DATE_FMTS = ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d")


def _str_to_ms(date_str: str, end_of_day: bool = False) -> int | None:
    """Parse 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM[:SS]' to epoch ms (UTC).

    Returns None on parse failure.
    """
    if not date_str:
        return None
    s = date_str.strip()
    if end_of_day and len(s) == 10:  # pure date
        s = f"{s} 23:59:59"
    for fmt in _DATE_FMTS:
        try:
            dt = datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1000)
        except ValueError:
            continue
    return None


def _str_to_day_str(date_str: str, end_of_day: bool = False) -> str | None:
    """Normalize user date for string-field comparison on 'YYYY-MM-DD HH:MM' storage.

    Strings compare lexicographically, so returning 'YYYY-MM-DD 23:59' for an
    end-of-day bound gives correct inclusive behavior.
    """
    if not date_str:
        return None
    s = date_str.strip()
    if len(s) == 10:
        return f"{s} 23:59" if end_of_day else f"{s} 00:00"
    return s


# ── Filter builder ──────────────────────────────────────────────


def _build_filter(
    spec: CollectionSpec,
    tickers: list[str] | None,
    date_range: dict | None,
) -> dict:
    """Build a Mongo filter that narrows a collection by ticker + date.

    Ticker OR includes the canonical field plus any collection-specific fallback
    (e.g. gangtise.researches uses ``stocks.code`` which is already canonical;
    jinmen.reports uses ``companies.stockcode`` which is numeric only).
    """
    q: dict[str, Any] = {}

    if tickers:
        or_clauses: list[dict] = [{spec.ticker_field: {"$in": tickers}}]
        if spec.ticker_fallback_path == "stocks":
            or_clauses.append({"stocks.code": {"$in": tickers}})
        elif spec.ticker_fallback_path == "companies":
            # fullCode / stockcode on jinmen.reports is numeric+market prefix
            # e.g., 'bj920077', 'sh600519'. Extract numeric part of user's
            # canonical ticker for matching.
            nums = []
            for t in tickers:
                head = t.split(".", 1)[0].lstrip("0") or "0"
                if head.isdigit():
                    nums.append(head)
            if nums:
                # match bare stockcode OR any fullCode that ends with the digits
                or_clauses.append({"companies.stockcode": {"$in": nums}})
        q["$or"] = or_clauses

    if date_range:
        gte_in = date_range.get("gte") if isinstance(date_range, dict) else None
        lte_in = date_range.get("lte") if isinstance(date_range, dict) else None
        if spec.date_ms_field:
            ms_filter: dict[str, int] = {}
            g = _str_to_ms(gte_in) if gte_in else None
            l = _str_to_ms(lte_in, end_of_day=True) if lte_in else None
            if g is not None:
                ms_filter["$gte"] = g
            if l is not None:
                ms_filter["$lte"] = l
            if ms_filter:
                q[spec.date_ms_field] = ms_filter
        elif spec.date_str_field:
            s_filter: dict[str, str] = {}
            g = _str_to_day_str(gte_in) if gte_in else None
            l = _str_to_day_str(lte_in, end_of_day=True) if lte_in else None
            if g:
                s_filter["$gte"] = g
            if l:
                s_filter["$lte"] = l
            if s_filter:
                q[spec.date_str_field] = s_filter

    return q


# ── Projection (limits how much of a doc we pull for scoring) ───


def _build_projection(spec: CollectionSpec) -> dict:
    """Only fetch fields we need for scoring + normalization."""
    keep = {"_id": 1, spec.title_field: 1, "_canonical_tickers": 1}
    for f in spec.text_fields:
        keep[f] = 1
    if spec.date_str_field:
        keep[spec.date_str_field] = 1
    if spec.date_ms_field:
        keep[spec.date_ms_field] = 1
    if spec.institution_field:
        keep[spec.institution_field] = 1
    if spec.url_field:
        keep[spec.url_field] = 1
    if spec.ticker_fallback_path == "stocks":
        keep["stocks"] = 1
    if spec.ticker_fallback_path == "companies":
        keep["companies"] = 1
    return keep


# ── Extractors (per-spec field normalization) ───────────────────


def _extract_text(spec: CollectionSpec, doc: dict) -> str:
    for f in spec.text_fields:
        v = doc.get(f)
        if isinstance(v, str) and v.strip():
            return v
    return ""


def _extract_date(spec: CollectionSpec, doc: dict) -> tuple[str, int | None]:
    """Return (date_str_YYYY_MM_DD, release_ms_or_none)."""
    ms: int | None = None
    date_str = ""
    if spec.date_ms_field:
        raw = doc.get(spec.date_ms_field)
        if isinstance(raw, (int, float)) and raw > 0:
            ms = int(raw)
    if spec.date_str_field:
        raw = doc.get(spec.date_str_field)
        if isinstance(raw, str) and raw:
            date_str = raw[:19]
            if ms is None:
                ms = _str_to_ms(date_str)
    if ms is not None and not date_str:
        try:
            date_str = datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
        except (ValueError, OSError, OverflowError):
            date_str = ""
    return (date_str[:10] if date_str else ""), ms


def _extract_institution(spec: CollectionSpec, doc: dict) -> str:
    if not spec.institution_field:
        return ""
    v = doc.get(spec.institution_field)
    if v is None:
        return ""
    if spec.institution_kind == "str":
        return str(v)[:100]
    if spec.institution_kind == "list_dict_name":
        if isinstance(v, list) and v and isinstance(v[0], dict):
            return str(v[0].get("name", ""))[:100]
    return ""


def _extract_tickers(spec: CollectionSpec, doc: dict) -> list[str]:
    canonical = doc.get("_canonical_tickers")
    if isinstance(canonical, list) and canonical:
        return [t for t in canonical if isinstance(t, str)][:10]
    if spec.ticker_fallback_path == "stocks":
        stocks = doc.get("stocks") or []
        if isinstance(stocks, list):
            out = []
            for s in stocks:
                if isinstance(s, dict):
                    code = s.get("code")
                    if isinstance(code, str) and code:
                        out.append(code)
            return out[:10]
    if spec.ticker_fallback_path == "companies":
        companies = doc.get("companies") or []
        if isinstance(companies, list):
            out = []
            for c in companies:
                if isinstance(c, dict):
                    # fullCode: 'bj920077', 'sh600519' — normalize
                    full = c.get("fullCode")
                    if isinstance(full, str) and len(full) >= 4:
                        market = full[:2].upper()
                        code = full[2:]
                        if market in ("SH", "SZ", "BJ") and code.isdigit():
                            out.append(f"{code}.{market}")
                            continue
                    stockcode = c.get("stockcode")
                    if isinstance(stockcode, str) and stockcode:
                        out.append(stockcode)
            return out[:10]
    return []


# ── Scoring (char-bigram for CJK + token substring for Latin) ───


_CJK_RE = re.compile(r"[\u4e00-\u9fff]")
_LATIN_WORD_RE = re.compile(r"[a-zA-Z0-9]{2,}")
_MAX_BODY_SCAN = 4000  # chars — cap to keep Python scoring under 10 ms per doc


def _tokenize(text: str) -> tuple[set[str], set[str]]:
    """Return (CJK bigrams set, lowered Latin word set)."""
    if not text:
        return set(), set()
    bigrams: set[str] = set()
    if len(text) >= 2:
        # build CJK bigrams where both characters are CJK
        for i in range(len(text) - 1):
            a, b = text[i], text[i + 1]
            if _CJK_RE.match(a) and _CJK_RE.match(b):
                bigrams.add(a + b)
    words = {m.group(0).lower() for m in _LATIN_WORD_RE.finditer(text)}
    return bigrams, words


def _score(query: str, title: str, body: str, release_ms: int | None) -> float:
    q_bg, q_wd = _tokenize(query)
    if not q_bg and not q_wd:
        return 0.0
    t_bg, t_wd = _tokenize(title or "")
    b_bg, b_wd = _tokenize((body or "")[:_MAX_BODY_SCAN])

    content_score = 0.0
    # CJK bigram: title weight 3, body weight 1
    for bg in q_bg:
        if bg in t_bg:
            content_score += 3.0
        if bg in b_bg:
            content_score += 1.0
    # Latin words (2+ chars each, already filtered in regex)
    for w in q_wd:
        if w in t_wd:
            content_score += 3.0
        if w in b_wd:
            content_score += 1.0
    # Recency bonus only applies when there's at least some content match —
    # otherwise recency alone would keep unrelated recent docs in the top-K.
    if content_score <= 0:
        return 0.0
    if release_ms:
        age_days = max(0.0, (time.time() * 1000 - release_ms) / 86400000.0)
        content_score += 2.0 * max(0.0, 1.0 - age_days / 365.0)
    return content_score


# ── Snippet extraction ──────────────────────────────────────────


def _build_snippet(text: str, query: str, max_chars: int = 320) -> str:
    """Return a short snippet centered on the first query-term match, else head."""
    if not text:
        return ""
    if not query:
        return text[:max_chars].replace("\n", " ").strip()
    # find first CJK bigram or Latin word match position
    q_bg, q_wd = _tokenize(query)
    positions: list[int] = []
    for bg in q_bg:
        p = text.find(bg)
        if p >= 0:
            positions.append(p)
    lower = text.lower()
    for w in q_wd:
        p = lower.find(w)
        if p >= 0:
            positions.append(p)
    if positions:
        pivot = min(positions)
        half = max_chars // 2
        start = max(0, pivot - half)
        end = min(len(text), start + max_chars)
        s = text[start:end].replace("\n", " ").strip()
        if start > 0:
            s = "…" + s
        if end < len(text):
            s = s + "…"
        return s
    return text[:max_chars].replace("\n", " ").strip()


# ── Normalize a hit into a transport dict ───────────────────────


def _normalize_hit(spec: CollectionSpec, doc: dict, score: float, query: str) -> dict:
    title = (doc.get(spec.title_field) or "").strip()
    text = _extract_text(spec, doc)
    date_str, ms = _extract_date(spec, doc)
    inst = _extract_institution(spec, doc)
    tickers = _extract_tickers(spec, doc)
    url = ""
    if spec.url_field:
        u = doc.get(spec.url_field)
        url = str(u) if u else ""
    snippet = _build_snippet(text, query, 320)
    raw_id = str(doc.get("_id"))
    return {
        "doc_id": f"{spec.db}:{spec.collection}:{raw_id}",
        "source": spec.db,
        "doc_type": spec.doc_type,
        "doc_type_cn": spec.doc_type_cn,
        "title": title,
        "snippet": snippet,
        "date": date_str,
        "release_ms": ms,
        "institution": inst,
        "tickers": tickers,
        "url": url,
        "text_len": len(text),
        "score": round(float(score), 3),
    }


# ── Search ──────────────────────────────────────────────────────


_PER_COLLECTION_CANDIDATE_LIMIT = 300   # ample for ticker-scoped filters
_PER_COLLECTION_BROAD_LIMIT = 250       # when no ticker filter — 15 colls × 250 = 3.75K docs max
_GLOBAL_CANDIDATE_CAP = 4000            # hard safety cap


def _pick_specs(
    sources: list[str] | None,
    doc_types: list[str] | None,
    *,
    include_low_quality: bool = False,
    milvus_only: bool = False,
) -> list[CollectionSpec]:
    """Resolve the search target set.

    * ``sources``/``doc_types`` narrow by platform or doc type (AND across the two).
    * ``include_low_quality``: if False (default), WeChat-like aggregator sources
      are dropped — they pollute ranking with low-information content.
    * ``milvus_only``: if True, return only specs that are indexed in Milvus
      (for Phase B routing). Specs with ``milvus_indexed=False`` fall back to Phase A.
    """
    targets = SPECS_LIST
    if sources:
        s_set = {s.lower().strip() for s in sources}
        targets = [t for t in targets if t.db in s_set]
    if doc_types:
        d_set = {d.strip() for d in doc_types}
        targets = [t for t in targets if t.doc_type in d_set]
    # Exclude low-quality unless caller explicitly opts in OR the doc_type was
    # requested by name (then the caller has made an informed choice).
    if not include_low_quality and not doc_types:
        targets = [t for t in targets if not t.low_quality]
    if milvus_only:
        targets = [t for t in targets if t.milvus_indexed]
    return targets


async def search(
    query: str = "",
    *,
    tickers: list[str] | None = None,
    doc_types: list[str] | None = None,
    sources: list[str] | None = None,
    date_range: dict | None = None,
    top_k: int = 8,
    include_low_quality: bool = False,
) -> list[dict]:
    """Search the knowledge base — parallel hybrid (Milvus) + keyword (Mongo).

    Runs **both** retrieval engines concurrently and merges:

    * **Phase B (Milvus)** — dense (Qwen3-Embedding 4096d) + BM25 hybrid with
      RRF fusion. Covers the subset of specs with ``milvus_indexed=True``.
    * **Phase A (Mongo)**  — metadata-filter + char-bigram scorer. Covers the
      full spec list, including platforms not yet ingested into Milvus
      (AlphaEngine, Jinmen oversea_reports, AceCamp opinions, Meritco research).

    Results are unioned by ``doc_id`` — when both engines match, the Milvus
    (semantic) score wins. Low-quality sources (e.g. WeChat aggregators) are
    excluded unless ``include_low_quality=True``.

    Falls back to Phase A alone when ``KB_SEARCH_LEGACY=true`` or Milvus errors.

    Returns a list of hit dicts in the same shape as Phase A produces.
    """
    want_k = max(1, min(int(top_k or 8), 30))

    # Legacy rollback path — stays pure Phase A.
    if get_settings().kb_search_legacy:
        return await _legacy_search(
            query,
            tickers=tickers,
            doc_types=doc_types,
            sources=sources,
            date_range=date_range,
            top_k=want_k,
            include_low_quality=include_low_quality,
        )

    # Lazy import avoids loading pymilvus on legacy-only deployments.
    try:
        from backend.app.services.kb_vector_query import hybrid_search as _vector_search
    except Exception:  # pragma: no cover — pymilvus missing in some test envs
        _vector_search = None

    # Over-fetch a bit so the merge has room to prefer semantic hits.
    fetch_k = max(want_k * 2, 16)

    async def _run_vector() -> list[dict]:
        if _vector_search is None:
            return []
        try:
            return await _vector_search(
                query,
                tickers=tickers,
                doc_types=doc_types,
                sources=sources,
                date_range=date_range,
                top_k=fetch_k,
                include_low_quality=include_low_quality,
            )
        except Exception as e:
            logger.warning("kb_vector hybrid_search failed, using Phase A only: %s", e)
            return []

    async def _run_keyword() -> list[dict]:
        try:
            return await _legacy_search(
                query,
                tickers=tickers,
                doc_types=doc_types,
                sources=sources,
                date_range=date_range,
                top_k=fetch_k,
                include_low_quality=include_low_quality,
            )
        except Exception as e:
            logger.warning("kb_legacy keyword search failed: %s", e)
            return []

    vec_hits, kw_hits = await asyncio.gather(_run_vector(), _run_keyword())

    return _merge_hybrid_hits(vec_hits, kw_hits, top_k=want_k)


def _merge_hybrid_hits(
    vector_hits: list[dict],
    keyword_hits: list[dict],
    *,
    top_k: int,
) -> list[dict]:
    """Merge Milvus (vector+BM25) hits with Mongo keyword hits by ``doc_id``.

    Semantic score wins on duplicate. Ordering is an RRF-like blend: each engine
    contributes ``1/(rank+k)`` points, summed across engines, then sorted.
    k=60 matches the RRF constant Milvus uses internally, keeping the merge
    stable regardless of individual score distributions.
    """
    if not vector_hits and not keyword_hits:
        return []

    rrf_k = 60
    by_id: dict[str, dict] = {}
    score_sum: dict[str, float] = {}

    def _doc_id(h: dict) -> str:
        return str(h.get("doc_id") or "")

    for rank, hit in enumerate(vector_hits, start=1):
        did = _doc_id(hit)
        if not did:
            continue
        by_id[did] = {**hit, "_engines": ["vector"]}
        score_sum[did] = 1.0 / (rank + rrf_k)

    for rank, hit in enumerate(keyword_hits, start=1):
        did = _doc_id(hit)
        if not did:
            continue
        if did in by_id:
            existing = by_id[did]
            existing["_engines"] = sorted(set(existing.get("_engines", []) + ["keyword"]))
            # Keep semantic snippet when present, but accept keyword snippet if
            # the vector one is shorter than the keyword one (often richer).
            if not existing.get("snippet") and hit.get("snippet"):
                existing["snippet"] = hit["snippet"]
        else:
            by_id[did] = {**hit, "_engines": ["keyword"]}
        score_sum[did] = score_sum.get(did, 0.0) + 1.0 / (rank + rrf_k)

    merged = list(by_id.values())
    merged.sort(key=lambda h: -score_sum.get(_doc_id(h), 0.0))
    return merged[:top_k]


async def _legacy_search(
    query: str = "",
    *,
    tickers: list[str] | None = None,
    doc_types: list[str] | None = None,
    sources: list[str] | None = None,
    date_range: dict | None = None,
    top_k: int = 8,
    include_low_quality: bool = False,
) -> list[dict]:
    """Phase A filter-first + bigram scorer. Retained as rollback + fallback.

    Concurrency: one `find` task per target collection, gathered. Errors from a
    single collection degrade gracefully (logged, skipped) — the other
    collections still return results.
    """
    top_k = max(1, min(int(top_k or 8), 30))
    targets = _pick_specs(sources, doc_types, include_low_quality=include_low_quality)
    if not targets:
        return []

    # Normalize tickers
    norm_tickers: list[str] = []
    if tickers:
        for t in tickers:
            norm_tickers.extend(normalize_ticker_input(t))
        # dedup preserving order
        norm_tickers = list(dict.fromkeys(norm_tickers))

    has_ticker_filter = bool(norm_tickers)
    per_limit = _PER_COLLECTION_CANDIDATE_LIMIT if has_ticker_filter else _PER_COLLECTION_BROAD_LIMIT

    async def _fetch(spec: CollectionSpec) -> list[tuple[CollectionSpec, dict]]:
        try:
            q = _build_filter(spec, norm_tickers, date_range)
            projection = _build_projection(spec)
            sort_field = spec.date_ms_field or spec.date_str_field
            cursor = _coll(spec).find(q, projection)
            if sort_field:
                cursor = cursor.sort([(sort_field, -1)])
            cursor = cursor.limit(per_limit)
            return [(spec, d) for d in await cursor.to_list(length=per_limit)]
        except Exception as e:
            logger.warning("kb_search fetch failed for %s/%s: %s",
                           spec.db, spec.collection, e)
            return []

    results_per_coll = await asyncio.gather(*(_fetch(s) for s in targets))

    all_candidates: list[tuple[CollectionSpec, dict]] = []
    for rows in results_per_coll:
        all_candidates.extend(rows)
        if len(all_candidates) >= _GLOBAL_CANDIDATE_CAP:
            all_candidates = all_candidates[:_GLOBAL_CANDIDATE_CAP]
            break

    # Dedup by (source, collection, id)
    seen: set[tuple[str, str, str]] = set()
    deduped: list[tuple[CollectionSpec, dict]] = []
    for spec, doc in all_candidates:
        key = (spec.db, spec.collection, str(doc.get("_id")))
        if key in seen:
            continue
        seen.add(key)
        deduped.append((spec, doc))

    q = (query or "").strip()
    if q:
        scored = []
        for spec, doc in deduped:
            title = doc.get(spec.title_field) or ""
            text = _extract_text(spec, doc)
            _, ms = _extract_date(spec, doc)
            s = _score(q, title, text, ms)
            if s > 0:
                scored.append((s, spec, doc))
        scored.sort(key=lambda x: -x[0])
    else:
        # No query — recency-only, each candidate gets a nominal score
        scored_rec: list[tuple[int, CollectionSpec, dict]] = []
        for spec, doc in deduped:
            _, ms = _extract_date(spec, doc)
            scored_rec.append((ms or 0, spec, doc))
        scored_rec.sort(key=lambda x: -x[0])
        scored = [(1.0, sp, dc) for _, sp, dc in scored_rec]

    top = scored[:top_k]
    return [_normalize_hit(spec, doc, sc, q) for sc, spec, doc in top]


# ── Fetch full document ─────────────────────────────────────────


async def fetch_document(
    doc_id: str,
    max_chars: int = 8000,
    *,
    highlight_snippet: str | None = None,
) -> dict:
    """Fetch a KB document by its stable ``doc_id``.

    When ``highlight_snippet`` is provided, we locate the first occurrence of
    that substring (or its longest 80-char prefix, for robustness against
    LLM paraphrase) and attach ``snippet_start`` / ``snippet_end`` char
    offsets into the returned ``text`` so the frontend doc viewer can scroll
    and highlight.
    """
    max_chars = max(1000, min(int(max_chars or 8000), 30000))
    try:
        source, collection, raw_id = doc_id.split(":", 2)
    except ValueError:
        return {"found": False, "doc_id": doc_id, "error": "invalid doc_id"}
    spec = SPECS_BY_KEY.get(f"{source}/{collection}")
    if not spec:
        return {"found": False, "doc_id": doc_id, "error": f"unknown source/collection {source}/{collection}"}
    coll = _coll(spec)
    # String _id is standard across our crawlers; some collections use ObjectId though
    doc: dict | None = None
    try:
        doc = await coll.find_one({"_id": raw_id})
    except Exception as e:
        logger.warning("fetch_document find_one(str) failed: %s", e)
    if doc is None:
        try:
            from bson import ObjectId
            if len(raw_id) == 24:
                doc = await coll.find_one({"_id": ObjectId(raw_id)})
        except Exception:
            doc = None
    if doc is None:
        # Also try integer _id for jinmen.reports (report_id is numeric string
        # but _id might be numeric too in some docs)
        if raw_id.isdigit():
            try:
                doc = await coll.find_one({"_id": int(raw_id)})
            except Exception:
                pass
    if not doc:
        return {"found": False, "doc_id": doc_id, "error": "not found"}

    text = _extract_text(spec, doc)
    full_len = len(text)

    # Locate snippet in the full text before truncation so the snippet
    # offset survives truncation (expand the window around the snippet).
    snippet_start = -1
    snippet_end = -1
    if highlight_snippet and text:
        s = highlight_snippet.strip()[:400]
        if s:
            idx = text.find(s)
            if idx < 0 and len(s) > 80:
                idx = text.find(s[:80])
            if idx < 0 and len(s) > 40:
                idx = text.find(s[:40])
            if idx >= 0:
                snippet_start = idx
                snippet_end = idx + min(len(s), 400)

    truncated = full_len > max_chars
    out_text = text[:max_chars] if truncated else text
    # If the snippet is beyond the max_chars window, re-centre output text
    if snippet_start >= max_chars:
        half = max_chars // 2
        window_start = max(0, snippet_start - half)
        out_text = text[window_start:window_start + max_chars]
        truncated = True
        snippet_start -= window_start
        snippet_end -= window_start
    elif snippet_end > len(out_text):
        snippet_end = len(out_text)

    date_str, ms = _extract_date(spec, doc)
    return {
        "found": True,
        "doc_id": doc_id,
        "source": spec.db,
        "doc_type": spec.doc_type,
        "doc_type_cn": spec.doc_type_cn,
        "title": (doc.get(spec.title_field) or "").strip(),
        "text": out_text,
        "full_text_len": full_len,
        "truncated": truncated,
        "snippet_start": snippet_start,
        "snippet_end": snippet_end,
        "date": date_str,
        "release_ms": ms,
        "institution": _extract_institution(spec, doc),
        "tickers": _extract_tickers(spec, doc),
        "url": str(doc.get(spec.url_field) or "") if spec.url_field else "",
        "has_pdf": bool(spec.has_pdf and doc.get("pdf_local_path")),
    }


# ── Facets ──────────────────────────────────────────────────────


async def list_facets(
    dimension: str,
    filters: dict | None = None,
    top: int = 20,
) -> list[dict]:
    top = max(1, min(int(top or 20), 200))
    filters = filters or {}
    sources = filters.get("sources")
    doc_types = filters.get("doc_types")
    date_range = filters.get("date_range")
    tickers = filters.get("tickers")

    norm_tickers: list[str] = []
    if tickers:
        for t in tickers:
            norm_tickers.extend(normalize_ticker_input(t))
        norm_tickers = list(dict.fromkeys(norm_tickers))

    targets = _pick_specs(sources, doc_types)
    if not targets:
        return []

    if dimension == "sources":
        async def cnt(spec):
            q = _build_filter(spec, norm_tickers, date_range)
            try:
                return spec.db, await _coll(spec).count_documents(q, maxTimeMS=10000)
            except Exception as e:
                logger.warning("facets/sources count failed for %s: %s", spec.db, e)
                return spec.db, 0
        pairs = await asyncio.gather(*(cnt(s) for s in targets))
        agg: dict[str, int] = {}
        for db, n in pairs:
            agg[db] = agg.get(db, 0) + n
        return [
            {"source": k, "count": v}
            for k, v in sorted(agg.items(), key=lambda x: -x[1])[:top]
        ]

    if dimension == "doc_types":
        async def cnt(spec):
            q = _build_filter(spec, norm_tickers, date_range)
            try:
                return spec, await _coll(spec).count_documents(q, maxTimeMS=10000)
            except Exception as e:
                logger.warning("facets/doc_types count failed for %s: %s",
                               spec.doc_type, e)
                return spec, 0
        rows = await asyncio.gather(*(cnt(s) for s in targets))
        rows.sort(key=lambda x: -x[1])
        return [
            {
                "doc_type": spec.doc_type,
                "label": spec.doc_type_cn,
                "source": spec.db,
                "count": n,
            }
            for spec, n in rows[:top]
        ]

    if dimension == "tickers":
        async def agg(spec):
            q = _build_filter(spec, norm_tickers, date_range)
            pipeline: list[dict] = []
            if q:
                pipeline.append({"$match": q})
            pipeline += [
                {"$match": {"_canonical_tickers": {"$exists": True, "$ne": []}}},
                {"$unwind": "$_canonical_tickers"},
                {"$group": {"_id": "$_canonical_tickers", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
                {"$limit": top * 3},
            ]
            try:
                return await _coll(spec).aggregate(pipeline, maxTimeMS=15000).to_list(length=top * 3)
            except Exception as e:
                logger.warning("facets/tickers agg failed for %s: %s",
                               spec.doc_type, e)
                return []
        rows_per = await asyncio.gather(*(agg(s) for s in targets))
        totals: dict[str, int] = {}
        for rows in rows_per:
            for r in rows:
                k = r.get("_id")
                if not isinstance(k, str) or not k:
                    continue
                totals[k] = totals.get(k, 0) + int(r.get("count", 0))
        return [
            {"ticker": k, "count": v}
            for k, v in sorted(totals.items(), key=lambda x: -x[1])[:top]
        ]

    if dimension == "date_histogram":
        async def agg(spec):
            q = _build_filter(spec, norm_tickers, date_range)
            pipeline: list[dict] = []
            if q:
                pipeline.append({"$match": q})
            if spec.date_ms_field:
                pipeline += [
                    {"$match": {spec.date_ms_field: {"$gt": 0}}},
                    {"$addFields": {"_ym": {
                        "$dateToString": {
                            "format": "%Y-%m",
                            "date": {"$toDate": f"${spec.date_ms_field}"},
                        }
                    }}},
                    {"$group": {"_id": "$_ym", "count": {"$sum": 1}}},
                ]
            elif spec.date_str_field:
                pipeline += [
                    {"$match": {spec.date_str_field: {"$exists": True, "$ne": None}}},
                    {"$addFields": {"_ym": {"$substr": [f"${spec.date_str_field}", 0, 7]}}},
                    {"$group": {"_id": "$_ym", "count": {"$sum": 1}}},
                ]
            else:
                return []
            pipeline.append({"$sort": {"_id": -1}})
            try:
                return await _coll(spec).aggregate(pipeline, maxTimeMS=15000).to_list(length=240)
            except Exception as e:
                logger.warning("facets/date_histogram agg failed for %s: %s",
                               spec.doc_type, e)
                return []
        rows_per = await asyncio.gather(*(agg(s) for s in targets))
        totals: dict[str, int] = {}
        for rows in rows_per:
            for r in rows:
                k = r.get("_id")
                if not isinstance(k, str) or not k or k == "1970-01":
                    continue
                totals[k] = totals.get(k, 0) + int(r.get("count", 0))
        return [
            {"month": k, "count": v}
            for k, v in sorted(totals.items(), reverse=True)[:top]
        ]

    raise ValueError(f"Unknown facet dimension: {dimension}")


# ── Formatting (model-facing text + UI-facing sources via citation tracker) ─


def _format_search_result(hits: list[dict], citation_tracker: Any) -> str:
    """Turn KB hits into markdown text with [N] citation indices.

    When a citation tracker is provided, each hit is registered and the returned
    `citation_index` is used as the [N] marker — sharing the same index space as
    web_search / alphapai / jinmen so the LLM can cite any source uniformly.
    """
    if not hits:
        return "（知识库内未找到相关结果。可尝试放宽筛选条件、扩大日期范围或换用不同关键词。）"

    if citation_tracker is not None and hasattr(citation_tracker, "add_kb_items"):
        indexed = citation_tracker.add_kb_items(hits)
    else:
        indexed = [{**h, "citation_index": i + 1} for i, h in enumerate(hits)]

    lines = [f"共找到 {len(indexed)} 条知识库结果："]
    for h in indexed:
        idx = h.get("citation_index", 0)
        title = h.get("title") or "(无标题)"
        date = h.get("date") or "日期未知"
        inst = h.get("institution") or "—"
        label = h.get("doc_type_cn") or h.get("doc_type") or ""
        tickers = h.get("tickers") or []
        snippet = (h.get("snippet") or "").strip()
        doc_id = h.get("doc_id") or ""
        ticker_s = ("[" + "/".join(tickers[:4]) + "]") if tickers else ""
        header = f"[{idx}] 「{label}·{inst}·{date}」 {ticker_s} {title}".strip()
        lines.append(header)
        if snippet:
            lines.append(f"    摘要: {snippet}")
        if doc_id:
            lines.append(f"    doc_id: {doc_id}  (如需完整原文，调用 kb_fetch_document)")
        lines.append("")
    return "\n".join(lines).rstrip()


def _format_fetch_result(res: dict) -> str:
    if not res.get("found"):
        return f"未找到文档 {res.get('doc_id', '')}。原因: {res.get('error', '未知')}"
    head = (
        f"# {res.get('title', '')}\n\n"
        f"- 类型: {res.get('doc_type_cn', '')} ({res.get('doc_type', '')})\n"
        f"- 来源: {res.get('source', '')}\n"
        f"- 日期: {res.get('date', '')}\n"
        f"- 机构: {res.get('institution', '') or '—'}\n"
        f"- 标的: {', '.join(res.get('tickers') or []) or '—'}\n"
        f"- doc_id: {res.get('doc_id', '')}\n"
    )
    url = res.get("url")
    if url:
        head += f"- URL: {url}\n"
    body = res.get("text", "")
    total_len = res.get("full_text_len", len(body))
    if res.get("truncated"):
        head += f"- 注意: 已截取 {len(body)}/{total_len} 字符\n"
    return head + "\n---\n\n" + body


def _format_facets_result(dimension: str, rows: list[dict]) -> str:
    if not rows:
        return f"在维度 {dimension} 下未找到符合筛选条件的结果。"
    lines = [f"维度 `{dimension}` 分布（按数量降序）:"]
    for r in rows:
        if dimension == "sources":
            lines.append(f"- {r['source']}: {r['count']}")
        elif dimension == "doc_types":
            lines.append(f"- {r['label']} ({r['doc_type']}, {r['source']}): {r['count']}")
        elif dimension == "tickers":
            lines.append(f"- {r['ticker']}: {r['count']}")
        elif dimension == "date_histogram":
            lines.append(f"- {r['month']}: {r['count']}")
    return "\n".join(lines)


# ── Tool schemas ────────────────────────────────────────────────


KB_TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "kb_search",
            "description": (
                "检索公司聚合投研知识库——**所有平台数据均已同步到本库，不要再调用 alphapai_recall 或 jinmen_* 工具**。"
                "覆盖 8 个来源平台：Alpha派(券商点评/券商研报/路演纪要)、进门财经(会议纪要/研报/海外研报)、"
                "久谦中台(专家交流/研究)、第三方桥(专家访谈)、Funda(业绩会纪要/业绩研报/点评)、"
                "岗底斯(首席观点/研报/纪要)、峰会(峰会文章/观点)、阿尔法引擎(纪要/内资研报/外资研报/资讯)。"
                "**采用 vector 向量检索 + BM25 关键词检索双引擎并行**，按 RRF 融合排序，对长尾、同义表达、"
                "专业术语均能高精度命中。支持按股票代码、日期、文档类型、来源任意组合筛选。"
                "返回每条带 doc_id 和摘要片段；如需完整内容请调用 kb_fetch_document。"
                "**优先使用本工具而非 web_search** 回答个股、行业、宏观等投研问题。"
                "微信公众号文章质量低已默认排除，不会出现在结果中。"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "自然语言查询，中文或英文均可。示例：'腾讯游戏业务监管口风变化'。",
                    },
                    "tickers": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": (
                            "股票标识列表。**首选填名字**——别名表覆盖 5 万+ 条目（Tushare A/HK/US "
                            "全表 + 人工纠错）。\n"
                            "- 中文公司名（强烈推荐）：'英伟达'、'腾讯'、'比亚迪'、'阿里巴巴控股'、"
                            "'宁德时代'、'茅台'、'招行' 等都识别。简称/全称/带后缀('控股/集团/股份/"
                            "公司/-W')都自动归一。\n"
                            "- 英文公司名：'Intel'、'Apple Inc'、'NVIDIA Corporation'、"
                            "'Tencent Holdings Ltd' 全部支持；大小写、句号、Inc/Corp/Ltd 后缀宽容。\n"
                            "- 代码：'NVDA.US'、'00700.HK'、'600519.SH'；裸代码 'NVDA'/'0700'/"
                            "'600519' 也行。\n\n"
                            "**自动校验机制（不必担心代码错配 / 幻觉）**：\n"
                            "1. 后端会从 `query` 文本中自动抽取公司实体（substring 匹配 5 万+ 别名 + "
                            "识别 `CODE.MARKET` 与 6 位 A 股代码），所以**当 query 已经写了公司名时，"
                            "tickers 字段可以直接留空**——后端会替你补上。\n"
                            "2. 如果你填了代码但代码对应的公司既没出现在 query 文本里、也无法通过 query "
                            "里的公司名映射到，说明很可能是幻觉，会被自动丢弃。例：query='中际旭创 业绩' "
                            "tickers=['300750.SZ']→ 检测到 300750=宁德时代未在 query 出现，丢弃，"
                            "用 query 解析的 300308.SZ。\n"
                            "3. 错配后缀（'BABA.HK'/'9988.US'/'TSMC.US'）先尝试自动恢复"
                            "（'BABA.HK'→'BABA.US'），失败则丢弃。\n\n"
                            "**最佳实践**：把要找的公司名直接写在 query 里，tickers 留空或仅填你 100% "
                            "确定的代码。永远不要猜测代码的市场后缀。"
                        ),
                    },
                    "doc_types": {
                        "type": "array",
                        "items": {"type": "string", "enum": ALL_DOC_TYPES},
                        "description": "文档类型筛选，留空则搜索全部类型（不含微信公众号）。",
                    },
                    "sources": {
                        "type": "array",
                        "items": {"type": "string", "enum": ALL_SOURCES},
                        "description": "来源平台筛选，留空则搜索全部来源。",
                    },
                    "date_range": {
                        "type": "object",
                        "description": (
                            "时间范围筛选（强烈建议在涉及'最新/近期/本季度/最近1年'等时效性问题时传入，"
                            "可大幅提升准确性并避免返回过时数据）。"
                        ),
                        "properties": {
                            "gte": {"type": "string", "description": "起始日期 YYYY-MM-DD（含）。例：'2025-01-01'。"},
                            "lte": {"type": "string", "description": "结束日期 YYYY-MM-DD（含）。例：'2026-04-24'。"},
                        },
                    },
                    "top_k": {
                        "type": "integer",
                        "description": "返回结果数，默认8，最大30。复杂问题建议 15-20。",
                    },
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "kb_fetch_document",
            "description": (
                "读取 kb_search 结果中某条命中的完整文档原文。"
                "当摘要片段不足以回答问题、或需要引用具体段落时使用。最多返回 30000 字符。"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "doc_id": {
                        "type": "string",
                        "description": "kb_search 返回的 doc_id 字段（形如 'funda:earnings_transcripts:<id>'）。",
                    },
                    "max_chars": {
                        "type": "integer",
                        "description": "最多返回的字符数，默认 8000，上限 30000。",
                    },
                },
                "required": ["doc_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "kb_list_facets",
            "description": (
                "按维度统计知识库内容分布。用于搜索前的侦察——例如先查看'过去3个月哪些券商给 NVDA 发了研报'，"
                "再有针对性地调用 kb_search。"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "dimension": {
                        "type": "string",
                        "enum": ["sources", "doc_types", "tickers", "date_histogram"],
                        "description": "统计维度：sources(来源)、doc_types(类型)、tickers(标的)、date_histogram(按年月)。",
                    },
                    "filters": {
                        "type": "object",
                        "properties": {
                            "tickers": {"type": "array", "items": {"type": "string"}},
                            "doc_types": {"type": "array", "items": {"type": "string", "enum": ALL_DOC_TYPES}},
                            "sources": {"type": "array", "items": {"type": "string", "enum": ALL_SOURCES}},
                            "date_range": {
                                "type": "object",
                                "properties": {
                                    "gte": {"type": "string"},
                                    "lte": {"type": "string"},
                                },
                            },
                        },
                    },
                    "top": {
                        "type": "integer",
                        "description": "返回前 N 条，默认20，上限200。",
                    },
                },
                "required": ["dimension"],
            },
        },
    },
]


KB_SYSTEM_PROMPT = (
    "## 公司聚合投研知识库（kb_search — 唯一的本地投研检索工具）\n\n"
    "本平台已将所有 8 个外部平台的全量投研数据同步到本地聚合库，**请统一使用 `kb_search`** 检索，"
    "**不要再调用** `alphapai_recall` / `jinmen_*` 等外部 API 工具（相关数据已全部落地到本库，"
    "且外部工具已停用）。\n\n"
    "### 覆盖内容\n"
    "- Alpha派：券商点评 / 券商研报 / 路演纪要\n"
    "- 进门财经：会议纪要 / 研报 / 海外研报\n"
    "- 久谦中台：专家交流 / 研究报告\n"
    "- 第三方桥：海外专家访谈\n"
    "- Funda：美股业绩会纪要 / 业绩研报 / Funda点评\n"
    "- 岗底斯：港股首席观点 / 研报 / 纪要\n"
    "- 峰会(AceCamp)：峰会文章 / 观点\n"
    "- 阿尔法引擎：纪要 / 内资研报 / 外资研报 / 资讯\n\n"
    "### 检索技术栈\n"
    "`kb_search` 同时运行两套引擎并行，结果 RRF 融合排序：\n"
    "1. **向量检索**（Qwen3-Embedding-8B 4096 维 + Milvus HNSW）—— 擅长同义表达、语义等价、概念迁移；\n"
    "2. **关键词 BM25**（Milvus Function API + Mongo 字符 bigram 回退）—— 擅长专业术语、公司名、产品型号、"
    "代码等精确命中。\n\n"
    "### 使用规则\n"
    "1. **必须先调用 `kb_search`** 处理所有涉及个股、行业、业绩、管理层观点、财务拆分、产能、客户、"
    "估值等投研问题；本库质量和时效性都高于公开网页。\n"
    "2. **时间筛选**：涉及'最新/近期/本季度/最近 X 个月'等时效问题时，**必须传 `date_range.gte/lte`**，"
    "例如 `date_range={'gte':'2025-10-01','lte':'2026-04-24'}`——否则会把 3 年前的旧研报和最新"
    "数据混在一起降低准确性。跨多季度对比时可先用 `kb_list_facets(dimension='date_histogram', ...)` "
    "确认数据分布，再分时间段检索。\n"
    "3. **并行多角度搜索**（关键）：一轮内应并行发起 **2–4 个** 不同 query / 不同筛选组合的 kb_search，"
    "例如同时发：\n"
    "   - 中文关键词 query + ticker 筛选\n"
    "   - 英文关键词 query + ticker 筛选（同一标的的海外视角）\n"
    "   - 不同子议题 query（业务/财务/产能分别检索）\n"
    "   并行调用比串行快，也能从多视角汇聚证据。\n"
    "4. **读原文**：对高度相关的命中，调用 `kb_fetch_document(doc_id=...)` 读取完整原文，不要仅凭"
    "摘要片段就下结论。一次研究通常读 2–4 份最硬核的原文。\n"
    "5. **弱命中回退**：若结果空或命中度低，先放宽筛选（去掉 doc_types，拉长 date_range），"
    "再换不同关键词重试 kb_search；最后才考虑 web_search 公网信息。\n"
    "6. **侦察优先**：不确定库内是否有某主题的数据时，先调 `kb_list_facets` 看分布，再针对性检索——"
    "例如 `kb_list_facets(dimension='doc_types', filters={'tickers':['0700.HK']})` 看腾讯有哪些类型数据。\n"
    "7. **股票代码规范**：建议 CODE.MARKET 形式（`NVDA.US`、`0700.HK`、`600519.SH`）；裸代码会自动补全但"
    "稍不精确；HK 代码需 5 位补零（`00700.HK`）。\n"
    "8. **引用编号**：kb_search 返回每条都带 `[N]`（与 web_search / user_kb_search 共享全局编号）。"
    "回答中事实/数字/观点句末必须插入 `[N]`；不要在末尾罗列来源——UI 自动渲染。\n"
    "9. **低质过滤**：微信公众号等低信息密度聚合内容已默认排除；无需你再去过滤。\n"
)


# ── execute_tool — single entry point for the chat dispatcher ───


async def execute_tool(
    name: str,
    arguments: dict[str, Any],
    citation_tracker: Any = None,
) -> str:
    """Entry point called from chat_llm.dispatch_tool.

    Returns a single string (markdown) that is fed back to the LLM as the tool
    result. Failures produce a short human-readable error rather than raising —
    the LLM is free to retry with different arguments.
    """
    # Lazy import to avoid a circular import at module load time.
    try:
        from backend.app.services.chat_debug import chat_trace, get_current_trace_id
        # Keyword args matter: chat_trace's first positional is user_id.
        # model_id is picked up from the per-model contextvar set by chat_llm.
        trace = chat_trace(trace_id=get_current_trace_id())
    except Exception:  # chat_debug not configured → still operate
        trace = None

    try:
        if name == "kb_search":
            query = (arguments.get("query") or "").strip()
            tickers = arguments.get("tickers") or None
            doc_types = arguments.get("doc_types") or None
            sources = arguments.get("sources") or None
            date_range = arguments.get("date_range") or None
            top_k = int(arguments.get("top_k") or 8)
            # Low-quality sources (e.g. WeChat aggregators) are excluded by default.
            # If the LLM explicitly asks for a low-quality doc_type, _pick_specs
            # already honors it; the explicit flag here is an escape hatch.
            include_low_quality = bool(arguments.get("include_low_quality") or False)

            # Cross-validate LLM tickers against entities mentioned in query.
            # Defends against hallucinations like:
            #   query="中际旭创 业绩" tickers=["300750.SZ"]  → wrong company
            # Strategy: drop LLM tickers whose code/brand is NOT mentioned
            # anywhere in the query (likely hallucinated), then augment with
            # entities extracted from the query text. When query has no
            # extractable entities, LLM tickers are trusted as-is.
            llm_tickers_raw = list(tickers) if tickers else []
            normalized_llm: list[str] = []
            for t in llm_tickers_raw:
                normalized_llm.extend(normalize_ticker_input(t))
            normalized_llm = list(dict.fromkeys(normalized_llm))

            query_entities = ticker_normalizer.extract_canonicals_from_query(query)
            if query_entities:
                trusted_llm = [
                    t for t in normalized_llm
                    if ticker_normalizer.is_mentioned_in_query(t, query)
                ]
                dropped = [t for t in normalized_llm if t not in trusted_llm]
                if dropped:
                    logger.warning(
                        "kb_search: dropped LLM tickers %s — not mentioned in query "
                        "%r (possible hallucination). Query entities: %s",
                        dropped, query[:160], query_entities,
                    )
                final_tickers = list(dict.fromkeys(trusted_llm + query_entities))
            else:
                final_tickers = normalized_llm

            if final_tickers != normalized_llm:
                logger.info(
                    "kb_search: ticker cross-validation. raw=%s normalized=%s "
                    "query_entities=%s final=%s",
                    llm_tickers_raw, normalized_llm, query_entities, final_tickers,
                )

            if trace and hasattr(trace, "log_kb_request"):
                trace.log_kb_request(
                    query=query, tickers=final_tickers or None, doc_types=doc_types,
                    sources=sources, date_range=date_range, top_k=top_k,
                )
            hits = await search(
                query,
                tickers=final_tickers or None,
                doc_types=doc_types,
                sources=sources,
                date_range=date_range,
                top_k=top_k,
                include_low_quality=include_low_quality,
            )
            if trace and hasattr(trace, "log_kb_results"):
                trace.log_kb_results(
                    query=query,
                    result_count=len(hits),
                    top_titles=[(h.get("title") or "")[:160] for h in hits[:10]],
                    sources=[h.get("source") for h in hits[:10]],
                )
            return _format_search_result(hits, citation_tracker)

        if name == "kb_fetch_document":
            doc_id = (arguments.get("doc_id") or "").strip()
            max_chars = int(arguments.get("max_chars") or 8000)
            if not doc_id:
                return "缺少参数 doc_id。"
            if trace and hasattr(trace, "log_kb_request"):
                trace.log_kb_request(query=f"fetch:{doc_id}", top_k=1, sources=[doc_id.split(':', 1)[0]])
            res = await fetch_document(doc_id, max_chars=max_chars)
            return _format_fetch_result(res)

        if name == "kb_list_facets":
            dimension = (arguments.get("dimension") or "").strip()
            filters = arguments.get("filters") or {}
            top = int(arguments.get("top") or 20)
            if not dimension:
                return "缺少参数 dimension。可用: sources / doc_types / tickers / date_histogram。"
            rows = await list_facets(dimension, filters=filters, top=top)
            return _format_facets_result(dimension, rows)

        return f"未知的 KB 工具: {name}"

    except Exception as e:
        logger.exception("kb tool %s failed with args=%s", name, arguments)
        return f"KB 工具 `{name}` 执行失败: {e}"
