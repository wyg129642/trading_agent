"""Per-stock research hub — aggregate every piece of content tied to one
canonical ticker into a single categorized feed.

This is the backend for the "click a holding → open new tab with everything"
feature on the portfolio dashboard.

Categories (user-facing labels):
    research   研报          — analyst reports, earnings filings
    commentary 点评          — daily comments, chief opinions, article notes
    minutes    会议纪要       — roadshow / meeting / earnings-call transcripts
    interview  专家访谈       — expert interviews, forum Q&A
    breaking   突发新闻       — real-time news items (Postgres + alphaengine news)

Endpoint
--------
``GET /api/stock-hub/{canonical_id}``
    Fan-out across ~21 MongoDB collections (by `_canonical_tickers`) + Postgres
    `news_items` join (by `analysis_results.affected_tickers` LIKE).

    Returns per-category counts + the requested slice of items, sorted by
    release_time desc. Supports category filter + cursor-based pagination
    via `before_ms`.
"""
from __future__ import annotations

import asyncio
import html as html_lib
import logging
import re
from functools import lru_cache
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel
from sqlalchemy import String, desc, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.app.config import Settings, get_settings
from backend.app.deps import get_current_user, get_db
from backend.app.models.news import AnalysisResult, NewsItem
from backend.app.models.user import User

logger = logging.getLogger(__name__)
router = APIRouter()


# Categories — label ordering matches the UI chip order
CATEGORY_ORDER = ["research", "commentary", "minutes", "interview", "breaking"]
CATEGORY_LABELS = {
    "research": "研报",
    "commentary": "点评",
    "minutes": "会议纪要",
    "interview": "专家访谈",
    "breaking": "突发新闻",
}


# --------------------------------------------------------------------------- #
# Source registry — one row per (platform, collection) tagged with a category.
# --------------------------------------------------------------------------- #
class _Source:
    def __init__(
        self,
        *,
        source: str,
        db_attr: str,
        uri_attr: str,
        collection: str,
        category: str,
        time_field: str,
        time_ms_field: str | None,
        url_field: str | None,
        preview_field: str | tuple[str, ...] | None,
        title_field: str = "title",
        org_field: str | None = None,
        pdf_route: str | None = None,
        pdf_gate: tuple[str, ...] = ("pdf_size", "pdf_size_bytes", "has_pdf", "pdf_flag", "pdf_local_path"),
        body_sections: tuple[tuple[str, str], ...] = (),
        source_label: str,
        id_coerce: str = "str",
    ) -> None:
        # ``preview_field`` accepts either a single dotted path or a tuple of
        # fallback paths. The first non-empty string wins. Tuple form is for
        # platforms whose primary preview field is sometimes empty (e.g.
        # AlphaPai 研报 stores the preview in `list_item.content` for some doc
        # types and `list_item.contentCn` / top-level `content` for others).
        # ``body_sections`` is an ordered list of (label, field_path) pairs.
        # field_path can be dotted (nested). Empty / whitespace values are dropped
        # at render time so platforms that sometimes populate one field and
        # sometimes another (e.g. meetings: points_md vs transcript_md) all work.
        # ``pdf_gate`` is the list of Mongo fields that, when truthy, mean the
        # doc HAS a PDF — used at list time so we only emit ``pdf_url`` for
        # docs that actually resolve (otherwise the UI shows a broken button).
        # ``id_coerce`` tells the detail endpoint how to coerce the URL path
        # segment before the Mongo lookup. "int-or-str" tries int() first then
        # falls back to str — required for jinmen + meritco where _id is stored
        # as Int64/int but the URL carries a string.
        self.source = source
        self.db_attr = db_attr
        self.uri_attr = uri_attr
        self.collection = collection
        self.category = category
        self.time_field = time_field
        self.time_ms_field = time_ms_field
        self.url_field = url_field
        if preview_field is None:
            self.preview_fields: tuple[str, ...] = ()
        elif isinstance(preview_field, str):
            self.preview_fields = (preview_field,)
        else:
            self.preview_fields = tuple(preview_field)
        self.title_field = title_field
        self.org_field = org_field
        self.pdf_route = pdf_route  # Backend URL template — callers prefix /api
        self.pdf_gate = pdf_gate
        self.body_sections = body_sections
        self.source_label = source_label
        self.id_coerce = id_coerce


SOURCES: list[_Source] = [
    # ── AlphaPai ───────────────────────────────────────────────────────────
    _Source(
        source="alphapai", db_attr="alphapai_mongo_db", uri_attr="alphapai_mongo_uri",
        collection="reports", category="research",
        time_field="publish_time", time_ms_field="release_time_ms",
        url_field="web_url",
        # AlphaPai 研报 list_item.content is empty for the vast majority of
        # docs (English-language reports populate list_item.contentCn + top
        # `content`; Chinese A-share reports often have neither — only PDF).
        # Try contentCn → top content → list_item.content; the empty-fallback
        # branch in _query_spec then surfaces "[PDF 全文]" when none has text.
        preview_field=("list_item.contentCn", "content", "list_item.content"),
        org_field="institution",
        pdf_route="/api/alphapai-db/items/report/{id}/pdf",
        # The alphapai PDF endpoint requires BOTH pdf_flag (the platform claims
        # a PDF exists) AND pdf_local_path (we actually downloaded it). Gating
        # on pdf_flag alone surfaces a PDF button for ~22k of 42k reports that
        # 404 because the file was never downloaded (broken pipe, permission,
        # filename-too-long, orphans). Require pdf_local_path so the button
        # is only shown when the PDF is actually retrievable.
        pdf_gate=("pdf_local_path", "pdf_size"),
        body_sections=(
            ("中文正文", "list_item.contentCn"),
            ("正文", "content"),
            ("摘要", "list_item.content"),
        ),
        source_label="AlphaPai · 研报",
    ),
    _Source(
        source="alphapai", db_attr="alphapai_mongo_db", uri_attr="alphapai_mongo_uri",
        collection="comments", category="commentary",
        time_field="publish_time", time_ms_field="release_time_ms",
        url_field="web_url", preview_field="list_item.content",
        body_sections=(
            ("正文", "content"),
            ("摘要", "list_item.content"),
        ),
        source_label="AlphaPai · 点评",
    ),
    _Source(
        source="alphapai", db_attr="alphapai_mongo_db", uri_attr="alphapai_mongo_uri",
        collection="roadshows", category="minutes",
        time_field="publish_time", time_ms_field="release_time_ms",
        url_field="web_url", preview_field="list_item.content",
        body_sections=(
            ("正文", "content"),
            ("摘要", "list_item.content"),
        ),
        source_label="AlphaPai · 路演纪要",
    ),
    _Source(
        source="alphapai", db_attr="alphapai_mongo_db", uri_attr="alphapai_mongo_uri",
        collection="wechat_articles", category="commentary",
        time_field="publish_time", time_ms_field="release_time_ms",
        url_field="web_url", preview_field="list_item.content",
        body_sections=(
            ("正文", "content"),
            ("摘要", "list_item.content"),
        ),
        source_label="AlphaPai · 微信研究",
    ),
    # ── Jinmen ─────────────────────────────────────────────────────────────
    _Source(
        source="jinmen", db_attr="jinmen_mongo_db", uri_attr="jinmen_mongo_uri",
        collection="reports", category="research",
        time_field="release_time", time_ms_field="release_time_ms",
        url_field="link_url", preview_field="summary_md",
        org_field="organization_name",
        pdf_route="/api/jinmen-db/reports/{id}/pdf",
        pdf_gate=("pdf_local_path", "pdf_size_bytes"),
        body_sections=(
            ("要点", "summary_point_md"),
            ("摘要", "summary_md"),
        ),
        id_coerce="int-or-str",
        source_label="进门 · A股研报",
    ),
    _Source(
        source="jinmen", db_attr="jinmen_mongo_db", uri_attr="jinmen_mongo_uri",
        collection="oversea_reports", category="research",
        time_field="release_time", time_ms_field="release_time_ms",
        url_field="link_url", preview_field="summary_md",
        org_field="organization_name",
        # oversea_reports URL uses dash not underscore!
        pdf_route="/api/jinmen-db/oversea-reports/{id}/pdf",
        pdf_gate=("pdf_local_path", "pdf_size_bytes"),
        body_sections=(
            ("要点", "summary_point_md"),
            ("摘要", "summary_md"),
        ),
        id_coerce="int-or-str",
        source_label="进门 · 海外研报",
    ),
    _Source(
        source="jinmen", db_attr="jinmen_mongo_db", uri_attr="jinmen_mongo_uri",
        collection="meetings", category="minutes",
        time_field="release_time", time_ms_field="release_time_ms",
        url_field="present_url", preview_field="points_md",
        org_field="organization",
        # jinmen meetings have no PDF endpoint — keep pdf_route=None
        body_sections=(
            ("核心要点", "points_md"),
            ("章节小结", "chapter_summary_md"),
            ("关键指标", "indicators_md"),
            ("原始纪要", "transcript_md"),
        ),
        id_coerce="int-or-str",
        source_label="进门 · 会议纪要",
    ),
    # ── Gangtise ───────────────────────────────────────────────────────────
    _Source(
        source="gangtise", db_attr="gangtise_mongo_db", uri_attr="gangtise_mongo_uri",
        collection="researches", category="research",
        time_field="release_time", time_ms_field="release_time_ms",
        url_field="web_url", preview_field="brief_md",
        org_field="organization",
        pdf_route="/api/gangtise-db/items/research/{id}/pdf",
        pdf_gate=("pdf_local_path", "pdf_size_bytes"),
        body_sections=(
            ("摘要", "brief_md"),
            ("正文", "content_md"),
        ),
        source_label="岗底斯 · 研报",
    ),
    _Source(
        source="gangtise", db_attr="gangtise_mongo_db", uri_attr="gangtise_mongo_uri",
        collection="summaries", category="minutes",
        time_field="release_time", time_ms_field="release_time_ms",
        url_field="web_url", preview_field="content_md",
        org_field="organization",
        # summaries has no PDF endpoint in gangtise_db.py — drop pdf_route
        body_sections=(
            ("正文", "content_md"),
            ("摘要", "brief_md"),
        ),
        source_label="岗底斯 · 会议纪要",
    ),
    _Source(
        source="gangtise", db_attr="gangtise_mongo_db", uri_attr="gangtise_mongo_uri",
        collection="chief_opinions", category="commentary",
        time_field="release_time", time_ms_field="release_time_ms",
        url_field="web_url", preview_field="description_md",
        org_field="organization",
        body_sections=(
            ("正文", "description_md"),
            ("详情", "content_md"),
        ),
        source_label="岗底斯 · 首席观点",
    ),
    # ── Funda (US) ─────────────────────────────────────────────────────────
    _Source(
        source="funda", db_attr="funda_mongo_db", uri_attr="funda_mongo_uri",
        collection="posts", category="research",
        time_field="release_time", time_ms_field="release_time_ms",
        url_field="web_url", preview_field="content_md",
        org_field="author",
        body_sections=(("正文", "content_md"),),
        source_label="Funda · 独立研究",
    ),
    _Source(
        source="funda", db_attr="funda_mongo_db", uri_attr="funda_mongo_uri",
        collection="earnings_reports", category="research",
        time_field="release_time", time_ms_field="release_time_ms",
        url_field="web_url", preview_field="content_md",
        body_sections=(("正文", "content_md"),),
        source_label="Funda · 财报 (8-K)",
    ),
    _Source(
        source="funda", db_attr="funda_mongo_db", uri_attr="funda_mongo_uri",
        collection="earnings_transcripts", category="minutes",
        time_field="release_time", time_ms_field="release_time_ms",
        url_field="web_url", preview_field="content_md",
        body_sections=(("正文", "content_md"),),
        source_label="Funda · 业绩会实录",
    ),
    # SemiAnalysis newsletter — long-form industry research, co-hosted in funda DB.
    _Source(
        source="semianalysis", db_attr="semianalysis_mongo_db", uri_attr="semianalysis_mongo_uri",
        collection="semianalysis_posts", category="research",
        time_field="release_time", time_ms_field="release_time_ms",
        url_field="canonical_url", preview_field="subtitle",
        org_field="organization",
        body_sections=(
            ("副标题", "subtitle"),
            ("正文", "content_md"),
        ),
        source_label="SemiAnalysis · 产业研究",
    ),
    # ── AlphaEngine ────────────────────────────────────────────────────────
    _Source(
        source="alphaengine", db_attr="alphaengine_mongo_db", uri_attr="alphaengine_mongo_uri",
        collection="china_reports", category="research",
        time_field="release_time", time_ms_field="release_time_ms",
        url_field="web_url", preview_field="doc_introduce",
        org_field="organization",
        # alphaengine: pdf route is /items/{category}/{id}/pdf; category uses
        # the CATEGORY_SPEC key, not the collection name (chinaReport, not china_reports)
        pdf_route="/api/alphaengine-db/items/chinaReport/{id}/pdf",
        pdf_gate=("pdf_local_path", "pdf_size_bytes"),
        body_sections=(
            ("摘要", "doc_introduce"),
            ("正文", "content_md"),
        ),
        source_label="AlphaEngine · 国内研报",
    ),
    _Source(
        source="alphaengine", db_attr="alphaengine_mongo_db", uri_attr="alphaengine_mongo_uri",
        collection="foreign_reports", category="research",
        time_field="release_time", time_ms_field="release_time_ms",
        url_field="web_url", preview_field="doc_introduce",
        org_field="organization",
        pdf_route="/api/alphaengine-db/items/foreignReport/{id}/pdf",
        pdf_gate=("pdf_local_path", "pdf_size_bytes"),
        body_sections=(
            ("摘要", "doc_introduce"),
            ("正文", "content_md"),
        ),
        source_label="AlphaEngine · 海外研报",
    ),
    _Source(
        source="alphaengine", db_attr="alphaengine_mongo_db", uri_attr="alphaengine_mongo_uri",
        collection="summaries", category="minutes",
        time_field="release_time", time_ms_field="release_time_ms",
        url_field="web_url", preview_field="doc_introduce",
        org_field="organization",
        body_sections=(
            ("摘要", "doc_introduce"),
            ("正文", "content_md"),
        ),
        source_label="AlphaEngine · 会议纪要",
    ),
    _Source(
        source="alphaengine", db_attr="alphaengine_mongo_db", uri_attr="alphaengine_mongo_uri",
        collection="news_items", category="breaking",
        time_field="release_time", time_ms_field="release_time_ms",
        url_field="web_url", preview_field="doc_introduce",
        org_field="organization",
        body_sections=(
            ("摘要", "doc_introduce"),
            ("正文", "content_md"),
        ),
        source_label="AlphaEngine · 资讯",
    ),
    # ── AceCamp ────────────────────────────────────────────────────────────
    _Source(
        source="acecamp", db_attr="acecamp_mongo_db", uri_attr="acecamp_mongo_uri",
        collection="articles", category="commentary",
        time_field="release_time", time_ms_field="release_time_ms",
        url_field="web_url", preview_field="content_md",
        body_sections=(
            ("摘要", "summary_md"),
            ("正文", "content_md"),
            ("转录稿", "transcribe_md"),
            ("简介", "brief_md"),
        ),
        source_label="本营 · 观点/纪要",
    ),
    # ── Meritco (Jiuqian) ──────────────────────────────────────────────────
    _Source(
        source="meritco", db_attr="meritco_mongo_db", uri_attr="meritco_mongo_uri",
        collection="forum", category="interview",
        time_field="release_time", time_ms_field="release_time_ms",
        url_field=None, preview_field="summary_md",
        # meritco forum PDFs are indexed: /forum/{id}/pdf?i=0 (multiple attachments)
        # We default to i=0 here; if a forum has multiple PDFs, the detail endpoint
        # returns pdf_attachments with a pdf_url per attachment.
        pdf_route="/api/meritco-db/forum/{id}/pdf?i=0",
        pdf_gate=("pdf_attachments",),
        body_sections=(
            ("要点", "insight_md"),
            ("摘要", "summary_md"),
            ("专家观点", "expert_content_md"),
            ("背景", "background_md"),
            ("讨论主题", "topic_md"),
            ("正文", "content_md"),
        ),
        id_coerce="int-or-str",
        source_label="久谦中台 · 专家论坛",
    ),
    # ── Third Bridge ───────────────────────────────────────────────────────
    _Source(
        source="thirdbridge", db_attr="thirdbridge_mongo_db", uri_attr="thirdbridge_mongo_uri",
        collection="interviews", category="interview",
        time_field="release_time", time_ms_field="release_time_ms",
        url_field=None, preview_field="agenda_md",
        body_sections=(
            ("议程", "agenda_md"),
            ("专家简介", "specialists_md"),
            ("导言", "introduction_md"),
            ("正文", "transcript_md"),
            ("评论", "commentary_md"),
        ),
        source_label="高临 · 专家访谈",
    ),
]


# Quick lookup: (source, collection) -> _Source. Used by the detail endpoint.
_SOURCE_INDEX: dict[tuple[str, str], _Source] = {
    (s.source, s.collection): s for s in SOURCES
}


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
@lru_cache(maxsize=8)
def _client(uri: str) -> AsyncIOMotorClient:
    # minPoolSize keeps 8 connections warm so the 21-way fan-out doesn't pay
    # a TCP handshake cost per collection on the first request after a
    # restart. maxPoolSize caps the pool well below Mongo's default (100) to
    # leave room for the crawlers that share the same host.
    return AsyncIOMotorClient(
        uri,
        tz_aware=True,
        serverSelectionTimeoutMS=5000,
        connectTimeoutMS=5000,
        minPoolSize=8,
        maxPoolSize=40,
        maxIdleTimeMS=300000,
    )


# Repeat-click cache. 21 Mongo collections + a Postgres ILIKE join cost 3-10s
# even warm, so memoizing for 5 minutes makes tab-reload / back-button /
# re-click instant. Short enough that new crawler output shows up soon.
_CACHE_TTL_SECONDS = 300


def _preview(text: Any, limit: int = 320) -> str:
    if not isinstance(text, str):
        return ""
    s = text.strip()
    return s[:limit] + ("…" if len(s) > limit else "")


def _pick_nested(doc: dict, path: str) -> Any:
    cur: Any = doc
    for part in path.split("."):
        if isinstance(cur, dict):
            cur = cur.get(part)
        else:
            return None
    return cur


# Speaker-turn / transcript line: "[01:23] 名字: text" or "[hh:mm:ss] name: text".
# Used to detect content that is structurally one-paragraph-per-line but stored
# with single \n separators — promoted to \n\n so CommonMark renders breaks.
_TRANSCRIPT_LINE_RE = re.compile(r"^\s*\[\d{1,2}:\d{2}(?::\d{2})?\]")
# AceCamp legacy bug: `transcribe` was a dict but got str()'d into Mongo.
_DICT_REPR_HEAD_RE = re.compile(r"^\s*\{\s*['\"](id|asr|state|title)['\"]\s*:")
# Block-level HTML tags that almost always indicate a raw-HTML leak — alphaengine
# news_items has 4 docs with `<html><head>...` dumped into content_md.
_BLOCK_HTML_RE = re.compile(
    r"<\s*(?:html|head|body|meta|p|div|table|tbody|tr|td|ul|ol|li|h[1-6])[\s>]",
    re.IGNORECASE,
)
# Invisible / zero-width chars that pad docs without rendering anything.
# U+200B ZWSP, U+200C ZWNJ, U+200D ZWJ, U+2060 WORD JOINER, U+FEFF BOM.
_INVISIBLE_CHARS_RE = re.compile(r"[​-‍⁠﻿]")


def _strip_block_html(body: str) -> str:
    """Convert a raw-HTML leak into clean markdown via bs4 + markdownify.

    Triggered when *_md content has a block-level HTML tag (likely the entire
    HTML page got dumped). Falls back to a regex tag strip if bs4/markdownify
    aren't importable in this environment.
    """
    try:
        from bs4 import BeautifulSoup
        from markdownify import markdownify as _md
        soup = BeautifulSoup(body, "html.parser")
        for tag in soup.find_all(["script", "style", "head", "meta", "link"]):
            tag.decompose()
        out = _md(str(soup), heading_style="ATX", strip=["a", "img"])
        out = re.sub(r"[ \t]+\n", "\n", out)
        out = re.sub(r"\n{3,}", "\n\n", out).strip()
        return out
    except Exception:
        # Conservative regex fallback — drop tags, collapse whitespace
        out = re.sub(r"<[^>]+>", " ", body)
        out = re.sub(r"\s+", " ", out).strip()
        return out


def _normalize_markdown(body: str) -> str:
    """Cleanup pass before handing markdown to the frontend.

    Five things, in order:
      1. Dict-repr fallback — AceCamp legacy bug stored Python `{'asr': [...]}`
         repr in transcribe_md; show a placeholder instead of 130 KB of slop.
      2. Strip zero-width / invisible chars (U+200B etc) that pad doc length
         without rendering anything (Funda 8-K fragments).
      3. Decode HTML entities (`&#160;`, `&nbsp;`, `&#8220;`, ...). Both
         numeric and named — the markdown renderer treats them literally.
      4. If the body still looks like raw HTML (block-level tag visible), run
         it through bs4+markdownify. Catches alphaengine news_items where the
         scraper dumped the entire HTML page.
      5. Promote single-newline streams to `\n\n`. Two cases: timestamped
         transcript turns (Jinmen meetings — `[hh:mm] name: text`), AND
         general paragraphs without any `\n\n` at all (Gangtise English
         research notes — long sentences separated by single `\n`).
         Conservative: only triggers when the doc is structurally a stream
         of newline-separated paragraphs, not a hard-wrapped block.
    """
    if not isinstance(body, str) or not body.strip():
        return body or ""

    # (1) dict repr — fail loud
    if _DICT_REPR_HEAD_RE.match(body):
        return "_（原始转录数据格式异常，待后台重新解析）_"

    # (2) strip invisible chars
    if _INVISIBLE_CHARS_RE.search(body):
        body = _INVISIBLE_CHARS_RE.sub("", body)

    # (3) entity decode (named + numeric)
    if "&#" in body or "&amp;" in body or "&nbsp;" in body or "&quot;" in body \
       or "&ldquo;" in body or "&rdquo;" in body or "&mdash;" in body \
       or "&hellip;" in body:
        body = html_lib.unescape(body)

    # (4) raw block HTML leak — run through bs4+markdownify
    if _BLOCK_HTML_RE.search(body):
        body = _strip_block_html(body)

    # (5) single-newline → paragraph promotion. Two flavors:
    #     (a) transcript-line dominated (Jinmen meetings, AceCamp transcribe)
    #     (b) general paragraphs with NO \n\n at all and reasonably-long lines
    #         (Gangtise long English research notes)
    # Skip when the doc already has paragraph breaks or is mostly short lines
    # (likely a hard-wrapped block where line breaks are layout artifacts).
    if "\n\n" not in body and body.count("\n") >= 2:
        lines = body.split("\n")
        non_empty = [ln for ln in lines if ln.strip()]
        if non_empty:
            n = len(non_empty)
            timestamped = sum(1 for ln in non_empty if _TRANSCRIPT_LINE_RE.match(ln))
            max_len = max(len(ln) for ln in non_empty)
            # Hard-wrapped text has uniform-ish line lengths. Paragraph
            # streams (gangtise English research) have wildly varying lines
            # — short headers ("NTT") next to 200-char paragraphs.
            # If any line is >=120 chars, this is almost certainly meant
            # as paragraphs that just lost their double-newline.
            should_promote = (
                # (a) transcript style — most lines start with [hh:mm]
                timestamped / n >= 0.6
                # (b) paragraph-stream — at least one long line implies
                # the newlines are paragraph delimiters
                or max_len >= 120
            )
            if should_promote:
                body = "\n\n".join(ln.strip() for ln in non_empty)

    return body


def _extract_org(spec: _Source, doc: dict) -> str:
    """Resolve institution/author into a short string for the UI."""
    if not spec.org_field:
        return ""
    v = doc.get(spec.org_field)
    if isinstance(v, str):
        return v[:80]
    if isinstance(v, list) and v:
        first = v[0]
        if isinstance(first, dict):
            return str(first.get("name") or first.get("code") or "")[:80]
        return str(first)[:80]
    if isinstance(v, dict):
        return str(v.get("name") or "")[:80]
    return ""


# --------------------------------------------------------------------------- #
# Mongo fan-out
# --------------------------------------------------------------------------- #
async def _query_spec(
    spec: _Source,
    settings: Settings,
    canonical_id: str,
    *,
    before_ms: int | None,
    limit: int,
    with_fetch: bool = True,
) -> tuple[int, list[dict]]:
    """Return (total_count, slice) for one collection.

    ``with_fetch=False`` runs only the count query — used when an active
    category filter excludes this source, but we still want its count shown
    on the filter chip so switching filters doesn't zero everything else.
    """
    uri = getattr(settings, spec.uri_attr)
    db_name = getattr(settings, spec.db_attr)
    if not uri or not db_name:
        return 0, []
    coll = _client(uri)[db_name][spec.collection]

    base_match: dict[str, Any] = {"_canonical_tickers": canonical_id}

    # Pagination: if caller passed before_ms and the collection has a *_ms
    # field, use it as a strict less-than; otherwise fall back to skip-less
    # pagination sorted by string time_field desc.
    match = dict(base_match)
    sort_field = spec.time_ms_field or spec.time_field
    if before_ms is not None and spec.time_ms_field:
        match[spec.time_ms_field] = {"$lt": before_ms}

    projection: dict[str, Any] = {
        spec.title_field: 1,
        spec.time_field: 1,
        "_canonical_tickers": 1,
        "_id": 1,
    }
    if spec.time_ms_field:
        projection[spec.time_ms_field] = 1
    if spec.url_field:
        projection[spec.url_field] = 1
    if spec.org_field:
        projection[spec.org_field] = 1
    # Preview projection supports nested dotted paths; drop the $substr
    # optimization because Mongo's $project on dotted paths with $substr
    # requires aggregation. We'll cap in Python post-fetch. preview_fields
    # is a fallback chain — project the first segment of every candidate
    # so the post-fetch picker can try them in order.
    for path in spec.preview_fields:
        projection[path.split(".")[0]] = 1

    # pdf indicators (cheap) — project exactly the fields the spec gates on
    # so the list response can show/hide the PDF button accurately.
    for gate_field in spec.pdf_gate:
        projection[gate_field] = 1

    # Count + fetch concurrently — halves wall time vs. sequential await
    # since both round-trip the same Mongo host.
    async def _count() -> int:
        try:
            return await coll.count_documents(base_match, maxTimeMS=4000)
        except Exception as e:
            logger.warning("stock_hub count %s.%s failed: %s", db_name, spec.collection, e)
            return 0

    async def _fetch() -> list[dict]:
        try:
            return await (
                coll.find(match, projection=projection)
                .sort(sort_field, -1)
                .limit(limit)
                .to_list(length=limit)
            )
        except Exception as e:
            logger.warning("stock_hub fetch %s.%s failed: %s", db_name, spec.collection, e)
            return []

    if with_fetch:
        total, docs = await asyncio.gather(_count(), _fetch())
    else:
        total = await _count()
        docs = []
    if total == 0 and not docs:
        return 0, []

    items: list[dict] = []
    try:
        for doc in docs:
            title = doc.get(spec.title_field) or ""
            url = doc.get(spec.url_field) if spec.url_field else None
            # Third-bridge URL reconstruction
            if spec.source == "thirdbridge" and not url:
                url = f"https://forum.thirdbridge.com/zh/interview/{doc.get('_id')}"
            preview = ""
            for path in spec.preview_fields:
                candidate = _preview(_pick_nested(doc, path))
                if candidate:
                    preview = candidate
                    break
            pdf_url = None
            if spec.pdf_route:
                has_pdf = False
                # First-valid attachment index for meritco multi-PDF case;
                # the default ?i=0 in pdf_route would 404 if attachment[0]
                # is missing locally but [1] is downloaded.
                first_valid_att_idx: int | None = None
                for gate_field in spec.pdf_gate:
                    v = doc.get(gate_field)
                    # pdf_local_path — require non-empty string; size_bytes must be > 0;
                    # pdf_attachments — require a non-empty list with at least one entry
                    # that has a local file (meritco-specific).
                    if gate_field == "pdf_local_path":
                        # Require a companion size > 0 when the spec explicitly
                        # gates on it — legacy rows may keep the path while the
                        # download is broken (size=0), and that should NOT
                        # surface a PDF button that 404s.
                        if isinstance(v, str) and v.strip():
                            size_field = (
                                "pdf_size_bytes" if "pdf_size_bytes" in spec.pdf_gate
                                else "pdf_size" if "pdf_size" in spec.pdf_gate
                                else None
                            )
                            if size_field is None:
                                has_pdf = True
                                break
                            if int(doc.get(size_field) or 0) > 0:
                                has_pdf = True
                                break
                    elif gate_field == "pdf_attachments":
                        if isinstance(v, list):
                            for idx, att in enumerate(v):
                                if (
                                    isinstance(att, dict)
                                    and att.get("pdf_local_path")
                                    and int(att.get("pdf_size_bytes") or 0) > 0
                                    and not att.get("pdf_download_error")
                                ):
                                    has_pdf = True
                                    first_valid_att_idx = idx
                                    break
                            if has_pdf:
                                break
                    elif gate_field in ("pdf_size_bytes", "pdf_size"):
                        # companion gate for pdf_local_path — alone these
                        # don't prove availability (legacy rows may set
                        # size but clear the path); checked inline by the
                        # pdf_local_path branch above.
                        continue
                    elif v:
                        has_pdf = True
                        break
                if has_pdf:
                    pdf_url = spec.pdf_route.format(id=str(doc.get("_id")))
                    # Re-target the meritco list pdf_url to the first attachment
                    # that actually has a local file; the route template hard-codes
                    # ?i=0 but [0] may be missing while [1] is downloaded.
                    if first_valid_att_idx is not None and first_valid_att_idx != 0:
                        pdf_url = pdf_url.replace("?i=0", f"?i={first_valid_att_idx}")
            # Empty-preview fallback: when no preview_field had text, surface a
            # short hint so the list card isn't blank. Most common for AlphaPai
            # 研报 of pure-Chinese A-share names where the platform serves only
            # a PDF + title (no summary / contentCn). Without a hint the card
            # looks broken even though the PDF tab works.
            if not preview:
                if pdf_url:
                    preview = "[PDF 全文，点击「查看详情」后切换至 PDF 标签]"
                else:
                    preview = "[平台未提供文字摘要]"
            org = _extract_org(spec, doc)
            rt = doc.get(spec.time_field)
            rt_ms = doc.get(spec.time_ms_field) if spec.time_ms_field else None

            items.append(
                {
                    "id": str(doc.get("_id")),
                    "source": spec.source,
                    "source_label": spec.source_label,
                    "collection": spec.collection,
                    "category": spec.category,
                    "category_label": CATEGORY_LABELS[spec.category],
                    "title": str(title)[:400],
                    "release_time": rt,
                    "release_time_ms": rt_ms,
                    "url": url,
                    "pdf_url": pdf_url,
                    "preview": preview,
                    "organization": org,
                    "tickers": doc.get("_canonical_tickers") or [],
                }
            )
    except Exception as e:
        logger.warning("stock_hub fetch %s.%s failed: %s", db_name, spec.collection, e)

    return total, items


# --------------------------------------------------------------------------- #
# Postgres breaking news join (uses the same ILIKE pattern as news.py)
# --------------------------------------------------------------------------- #
def _escape_like(s: str) -> str:
    return s.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


async def _query_breaking_news(
    db: AsyncSession,
    canonical_id: str,
    stock_name: str | None,
    *,
    before_ms: int | None,
    limit: int,
    with_fetch: bool = True,
) -> tuple[int, list[dict]]:
    """Breaking news from Postgres news_items + analysis_results.

    Matching strategy: `affected_tickers` is a JSONB list whose items look like
    "英特尔(INTC)" or "天孚通信(300394.SZ)", so we cast to text and ILIKE by
    the code portion. We also fall back to title ILIKE so raw feed items
    without affected_tickers still surface.

    ``with_fetch=False`` runs only the count — used when the active category
    filter isn't 'breaking' but we still want the chip count accurate.
    """
    # Extract bare code from canonical (e.g. "600519.SH" -> "600519", "AAPL.US" -> "AAPL")
    code = canonical_id.split(".")[0]
    like_patterns = [code]
    if stock_name:
        like_patterns.append(stock_name)

    ticker_match = or_(
        *[
            AnalysisResult.affected_tickers.cast(String).ilike(f"%{_escape_like(p)}%")
            for p in like_patterns
        ],
        *[NewsItem.title.ilike(f"%{_escape_like(p)}%") for p in like_patterns],
    )

    from datetime import datetime, timezone

    # `SELECT count(DISTINCT ...)` — the original version materialised the full
    # row list via `.all()` just to take `len()`, which for a 50k-row ILIKE match
    # turns into tens of seconds. A scalar count is milliseconds.
    count_q = (
        select(func.count(NewsItem.id.distinct()))
        .outerjoin(AnalysisResult, AnalysisResult.news_item_id == NewsItem.id)
        .where(ticker_match)
    )
    total = (await db.execute(count_q)).scalar() or 0
    if total == 0 or not with_fetch:
        return total, []

    stmt = (
        select(NewsItem, AnalysisResult)
        .outerjoin(AnalysisResult, AnalysisResult.news_item_id == NewsItem.id)
        .where(ticker_match)
    )
    if before_ms is not None:
        stmt = stmt.where(
            NewsItem.published_at
            < datetime.fromtimestamp(before_ms / 1000, tz=timezone.utc)
        )
    stmt = stmt.order_by(desc(NewsItem.published_at)).limit(limit)

    rows = (await db.execute(stmt)).all()
    items: list[dict] = []
    for n, a in rows:
        pub = n.published_at
        rt_str = pub.strftime("%Y-%m-%d %H:%M") if pub else None
        rt_ms = int(pub.timestamp() * 1000) if pub else None
        summary = (a.summary if a else "") or ""
        items.append(
            {
                "id": n.id,
                "source": "newsfeed",
                "source_label": f"资讯中心 · {n.source_name}",
                "collection": "news_items",
                "category": "breaking",
                "category_label": CATEGORY_LABELS["breaking"],
                "title": (n.title or "")[:400],
                "release_time": rt_str,
                "release_time_ms": rt_ms,
                "url": n.url,
                "pdf_url": None,
                "preview": _preview(summary or (n.content or "")),
                "organization": n.source_name,
                "sentiment": (a.sentiment if a else None),
                "impact_magnitude": (a.impact_magnitude if a else None),
                "tickers": [],
            }
        )
    return total, items


# --------------------------------------------------------------------------- #
# Response schema
# --------------------------------------------------------------------------- #
class HubItem(BaseModel):
    id: str
    source: str
    source_label: str
    collection: str
    category: str
    category_label: str
    title: str
    release_time: str | None = None
    release_time_ms: int | None = None
    url: str | None = None
    pdf_url: str | None = None
    preview: str = ""
    organization: str = ""
    sentiment: str | None = None
    impact_magnitude: str | None = None
    tickers: list[str] = []


class HubResponse(BaseModel):
    canonical_id: str
    stock_name: str | None = None
    by_category: dict[str, int]
    by_source: dict[str, int]
    total: int
    items: list[HubItem]
    next_before_ms: int | None = None


# --------------------------------------------------------------------------- #
# Endpoint
# --------------------------------------------------------------------------- #
@router.get("/{canonical_id}", response_model=HubResponse)
async def stock_hub(
    canonical_id: str,
    request: Request,
    category: str | None = Query(
        None,
        description="Filter by category. Omit for all. One of: research, commentary, minutes, interview, breaking",
    ),
    limit: int = Query(80, ge=1, le=300),
    before_ms: int | None = Query(
        None, description="Cursor: only return items with release_time_ms < this value"
    ),
    stock_name: str | None = Query(
        None,
        description="Optional display name — improves breaking-news recall when titles don't contain the ticker",
    ),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Per-stock aggregated content across 8 crawler platforms + Postgres newsfeed."""
    canonical_id = canonical_id.strip().upper().replace(" ", "")
    if "." not in canonical_id or len(canonical_id) > 32:
        raise HTTPException(
            400, "canonical_id must be CODE.MARKET, e.g. AAPL.US or 600519.SH"
        )
    if category and category not in CATEGORY_LABELS:
        raise HTTPException(400, f"Unknown category '{category}'")

    # Redis cache check (5-min TTL, keyed by every query parameter).
    redis = getattr(request.app.state, "redis", None)
    cache_key = (
        f"stock-hub:v2:{canonical_id}:{category or 'all'}:"
        f"{before_ms or 0}:{limit}:{stock_name or ''}"
    )
    if redis is not None:
        try:
            cached = await redis.get(cache_key)
            if cached:
                return HubResponse.model_validate_json(cached)
        except Exception as e:
            logger.debug("stock_hub cache read failed: %s", e)

    settings = get_settings()
    # Per-source slice size: when filtering we pull `limit`; when aggregating all
    # categories we pull ~limit/4 per source so 21+ sources stay cheap.
    # We ALWAYS fan out counts over every source — otherwise switching to a
    # filter chip would zero the other chips (by_category only populated for
    # queried sources).
    if category:
        per_src = limit
    else:
        per_src = max(10, limit // 4)

    mongo_task = asyncio.gather(
        *(
            _query_spec(
                spec, settings, canonical_id,
                before_ms=before_ms, limit=per_src,
                with_fetch=(category is None or spec.category == category),
            )
            for spec in SOURCES
        ),
        return_exceptions=True,
    )

    # Always run the breaking-news count so the "突发新闻" chip stays accurate
    # even when the user has filtered to another category. Only fetch items
    # if the active filter is None or "breaking".
    breaking_task = asyncio.create_task(
        _query_breaking_news(
            db, canonical_id, stock_name,
            before_ms=before_ms, limit=per_src,
            with_fetch=(category is None or category == "breaking"),
        )
    )

    mongo_results = await mongo_task

    by_category: dict[str, int] = {k: 0 for k in CATEGORY_ORDER}
    by_source: dict[str, int] = {}
    items: list[dict] = []
    for spec, res in zip(SOURCES, mongo_results):
        if isinstance(res, Exception):
            logger.warning("stock_hub: %s.%s errored: %s", spec.source, spec.collection, res)
            continue
        total, slice_ = res
        by_category[spec.category] = by_category.get(spec.category, 0) + total
        by_source[spec.source] = by_source.get(spec.source, 0) + total
        items.extend(slice_)

    try:
        bk_total, bk_items = await breaking_task
    except Exception as e:
        logger.warning("stock_hub: breaking news query failed: %s", e)
        bk_total, bk_items = 0, []
    by_category["breaking"] = by_category.get("breaking", 0) + bk_total
    by_source["newsfeed"] = by_source.get("newsfeed", 0) + bk_total
    items.extend(bk_items)

    # Sort by release_time_ms desc (fall back to release_time string)
    def _sort_key(x: dict) -> tuple[int, str]:
        ms = x.get("release_time_ms") or 0
        return (-int(ms or 0), x.get("release_time") or "")

    items.sort(key=_sort_key)
    clipped = items[:limit]

    next_before = None
    if len(items) > limit and clipped:
        tail = clipped[-1]
        next_before = tail.get("release_time_ms")

    # Total is "total items available for the active filter", not just
    # this page. When category is set, total = by_category[category];
    # otherwise total = sum of all categories.
    if category:
        total = by_category.get(category, 0)
    else:
        total = sum(by_category.values())

    response = HubResponse(
        canonical_id=canonical_id,
        stock_name=stock_name,
        by_category=by_category,
        by_source=by_source,
        total=total,
        items=[HubItem(**x) for x in clipped],
        next_before_ms=next_before,
    )
    if redis is not None:
        try:
            await redis.set(cache_key, response.model_dump_json(), ex=_CACHE_TTL_SECONDS)
        except Exception as e:
            logger.debug("stock_hub cache write failed: %s", e)
    return response


# --------------------------------------------------------------------------- #
# Detail endpoint — fetch the full document for one card. The list endpoint
# returns only a 320-char preview so the per-stock feed stays fast; when the
# user clicks a card we load everything the DB has so the Drawer can render
# full markdown bodies without the frontend needing to know every platform's
# field shape (content / content_md / summary_md / transcript_md / agenda_md...).
# --------------------------------------------------------------------------- #
class DocSection(BaseModel):
    label: str
    markdown: str


class DocDetailResponse(BaseModel):
    source: str
    source_label: str
    collection: str
    category: str
    category_label: str
    id: str
    title: str
    release_time: str | None = None
    release_time_ms: int | None = None
    organization: str = ""
    url: str | None = None
    pdf_url: str | None = None
    pdf_urls: list[dict] = []  # multi-attachment case (meritco forum)
    tickers: list[str] = []
    sections: list[DocSection] = []
    sentiment: str | None = None
    impact_magnitude: str | None = None


def _coerce_id(item_id: str, mode: str) -> list[Any]:
    """Return candidate _id values to try in order. Some collections (jinmen,
    meritco) store _id as Int64/int but the URL carries a string."""
    if mode == "int-or-str":
        try:
            return [int(item_id), item_id]
        except ValueError:
            return [item_id]
    return [item_id]


@router.get("/doc/{source}/{collection}/{item_id}", response_model=DocDetailResponse)
async def stock_hub_doc(
    source: str,
    collection: str,
    item_id: str,
    user: User = Depends(get_current_user),
):
    """Return the full content for one document, normalized across sources.

    ``sections`` is an ordered list of (label, markdown) pairs — the platform
    decides which fields map to which sections via ``_Source.body_sections``.
    Empty sections are dropped so the Drawer only renders what exists.
    """
    spec = _SOURCE_INDEX.get((source, collection))
    if spec is None:
        raise HTTPException(
            404,
            f"Unknown (source, collection) = ({source!r}, {collection!r})",
        )

    settings = get_settings()
    uri = getattr(settings, spec.uri_attr)
    db_name = getattr(settings, spec.db_attr)
    if not uri or not db_name:
        raise HTTPException(503, f"{source} Mongo not configured")
    coll = _client(uri)[db_name][spec.collection]

    doc: dict | None = None
    for candidate in _coerce_id(item_id, spec.id_coerce):
        doc = await coll.find_one({"_id": candidate})
        if doc:
            break
    if doc is None:
        raise HTTPException(404, f"{source}.{collection}/{item_id} not found")

    # Assemble body sections
    sections: list[dict] = []
    seen_bodies: set[str] = set()
    for label, field_path in spec.body_sections:
        raw = _pick_nested(doc, field_path)
        if not isinstance(raw, str):
            continue
        body = _normalize_markdown(raw.strip())
        if not body or body in seen_bodies:
            continue
        seen_bodies.add(body)
        sections.append({"label": label, "markdown": body})

    # pdf_url (single) + pdf_urls (multi-attachment). For meritco we also expose
    # the per-attachment list so the UI can render tabs when a forum has >1 PDF.
    pdf_url: str | None = None
    pdf_urls: list[dict] = []
    if spec.pdf_route:
        has_pdf = False
        first_valid_att_idx: int | None = None
        for gate_field in spec.pdf_gate:
            v = doc.get(gate_field)
            if gate_field == "pdf_local_path":
                if isinstance(v, str) and v.strip():
                    size_field = (
                        "pdf_size_bytes" if "pdf_size_bytes" in spec.pdf_gate
                        else "pdf_size" if "pdf_size" in spec.pdf_gate
                        else None
                    )
                    if size_field is None:
                        has_pdf = True
                        break
                    if int(doc.get(size_field) or 0) > 0:
                        has_pdf = True
                        break
            elif gate_field == "pdf_attachments":
                if isinstance(v, list):
                    for idx, att in enumerate(v):
                        if (
                            isinstance(att, dict)
                            and att.get("pdf_local_path")
                            and int(att.get("pdf_size_bytes") or 0) > 0
                            and not att.get("pdf_download_error")
                        ):
                            has_pdf = True
                            if first_valid_att_idx is None:
                                first_valid_att_idx = idx
                            pdf_urls.append({
                                "index": idx,
                                "name": att.get("name") or f"附件 {idx + 1}",
                                "size_bytes": int(att.get("pdf_size_bytes") or 0),
                                # meritco pdf_route already ends with ?i=0 — swap index
                                "url": f"/api/meritco-db/forum/{str(doc.get('_id'))}/pdf?i={idx}",
                            })
                    break
            elif gate_field in ("pdf_size_bytes", "pdf_size"):
                continue
            elif v:
                has_pdf = True
                break
        if has_pdf:
            pdf_url = spec.pdf_route.format(id=str(doc.get("_id")))
            # Re-target meritco's single pdf_url (hardcoded ?i=0 in the route
            # template) to the first attachment that actually has a local file.
            if first_valid_att_idx is not None and first_valid_att_idx != 0:
                pdf_url = pdf_url.replace("?i=0", f"?i={first_valid_att_idx}")

    url = doc.get(spec.url_field) if spec.url_field else None
    if spec.source == "thirdbridge" and not url:
        url = f"https://forum.thirdbridge.com/zh/interview/{doc.get('_id')}"

    rt = doc.get(spec.time_field)
    rt_ms = doc.get(spec.time_ms_field) if spec.time_ms_field else None

    return DocDetailResponse(
        source=spec.source,
        source_label=spec.source_label,
        collection=spec.collection,
        category=spec.category,
        category_label=CATEGORY_LABELS[spec.category],
        id=str(doc.get("_id")),
        title=str(doc.get(spec.title_field) or "")[:400],
        release_time=rt if isinstance(rt, str) else None,
        release_time_ms=rt_ms if isinstance(rt_ms, int) else None,
        organization=_extract_org(spec, doc),
        url=url,
        pdf_url=pdf_url,
        pdf_urls=pdf_urls,
        tickers=doc.get("_canonical_tickers") or [],
        sections=[DocSection(**s) for s in sections],
    )


@router.get("/newsfeed/{news_id}", response_model=DocDetailResponse)
async def stock_hub_newsfeed_doc(
    news_id: str,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Detail for a Postgres breaking-news item."""
    stmt = (
        select(NewsItem, AnalysisResult)
        .outerjoin(AnalysisResult, AnalysisResult.news_item_id == NewsItem.id)
        .where(NewsItem.id == news_id)
    )
    row = (await db.execute(stmt)).first()
    if row is None:
        raise HTTPException(404, f"news_item {news_id} not found")
    n, a = row

    # Flatten AnalysisResult into readable markdown blocks. Doing it here
    # keeps the frontend dumb — it just renders whatever sections come back
    # and doesn't have to know about bull_case / bear_case / key_facts etc.
    sections: list[dict] = []
    if a and a.summary:
        sections.append({"label": "摘要", "markdown": a.summary})
    if a:
        if getattr(a, "market_expectation", ""):
            sections.append({"label": "市场预期", "markdown": a.market_expectation})
        if getattr(a, "bull_case", ""):
            sections.append({"label": "多头逻辑", "markdown": a.bull_case})
        if getattr(a, "bear_case", ""):
            sections.append({"label": "空头逻辑", "markdown": a.bear_case})
        key_facts = getattr(a, "key_facts", None) or []
        if isinstance(key_facts, list) and key_facts:
            lines = [f"- {str(kf)}" for kf in key_facts if kf]
            if lines:
                sections.append({"label": "关键事实", "markdown": "\n".join(lines)})
    if n.content:
        sections.append({"label": "正文", "markdown": n.content})

    pub = n.published_at
    return DocDetailResponse(
        source="newsfeed",
        source_label=f"资讯中心 · {n.source_name}",
        collection="news_items",
        category="breaking",
        category_label=CATEGORY_LABELS["breaking"],
        id=str(n.id),
        title=(n.title or "")[:400],
        release_time=pub.strftime("%Y-%m-%d %H:%M") if pub else None,
        release_time_ms=int(pub.timestamp() * 1000) if pub else None,
        organization=n.source_name or "",
        url=n.url,
        pdf_url=None,
        tickers=[],
        sections=[DocSection(**s) for s in sections],
        sentiment=(a.sentiment if a else None),
        impact_magnitude=(a.impact_magnitude if a else None),
    )


# --------------------------------------------------------------------------- #
# Tiny utility endpoint — lets the frontend avoid re-implementing the
# canonical normalizer when the portfolio already knows `(code, market_label)`.
# --------------------------------------------------------------------------- #
class CanonicalResolve(BaseModel):
    canonical_id: str | None


@router.get("/_resolve/by-portfolio", response_model=CanonicalResolve)
async def resolve_portfolio_ticker(
    ticker: str = Query(..., description="Raw stock_ticker e.g. 'AAPL', '600519', '03690'"),
    market: str = Query(..., description="stock_market label from portfolio yaml"),
    user: User = Depends(get_current_user),
):
    from backend.app.services.ticker_normalizer import (
        _canonical_from_code_market,
        _classify_ashare,
        _pad_hk,
        normalize,
    )

    market_map = {
        "美股": "us",
        "港股": "hk",
        "主板": None,  # classify by code
        "创业板": None,
        "科创板": None,
        "韩股": "kr",
        "日股": "jp",
        "澳股": "au",
        "德股": "de",
    }
    if market in ("主板", "创业板", "科创板"):
        cls = _classify_ashare(ticker)
        if cls:
            return CanonicalResolve(canonical_id=f"{ticker}.{cls}")
    if market == "港股":
        return CanonicalResolve(canonical_id=f"{_pad_hk(ticker)}.HK")
    mk = market_map.get(market)
    if mk:
        r = _canonical_from_code_market(ticker, mk)
        if r:
            return CanonicalResolve(canonical_id=r)
    # Last-chance: run full normalizer
    r = normalize(ticker)
    return CanonicalResolve(canonical_id=(r[0] if r else None))
