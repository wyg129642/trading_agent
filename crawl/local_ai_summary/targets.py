"""Per-collection summarization spec.

Each `Target` declares:
  - mongo db / collection name (matches ta-mongo-crawl on 127.0.0.1:27018)
  - which fields are *real* native summaries (skip LLM if any non-empty + long enough)
  - which fields contain the body to feed the LLM (priority order; first non-empty wins)
  - which field carries the canonical ticker list (always ``_canonical_tickers``
    after the platform's ticker_tag pass) — kept as an attribute for explicit
    documentation rather than hard-coded.
  - which field carries the doc's release time as epoch-ms (so we can window
    "last N days" cheaply via Mongo index).

A doc qualifies for LLM summarization when:
  1. its tickers ∩ portfolio is non-empty, AND
  2. all native-summary fields are empty (or below the min length), AND
  3. ``local_ai_summary.v`` is missing or below the current schema version, AND
  4. ``release_time_ms`` is within the lookback window.

Order of priority below mirrors what the user reads on StockHub — research
first (highest signal), then commentary / minutes / news. The runner walks
specs round-robin so every collection gets attention even under a budget cap.
"""

from __future__ import annotations

from dataclasses import dataclass, field


# Native summary must be at least this long to be considered "real". Below
# this, we treat it as junk (e.g. acecamp.summary_md sometimes stores 22-char
# author handles) and run the LLM anyway.
MIN_NATIVE_SUMMARY_LEN = 80

# Hard cap on input text fed to the LLM. qwen-plus context is generous but
# we don't need a 50k-token earnings transcript to write a 100-char card —
# the lede + opening few paragraphs already contain the key point.
MAX_INPUT_CHARS = 8000

# Default floor for "is there enough body to summarize". 60 chars rejects
# almost-empty bodies (e.g. just a title echoed into content_md) but is loose
# enough that a one-paragraph comment still qualifies.
DEFAULT_MIN_BODY_CHARS = 60


@dataclass(frozen=True)
class Target:
    db: str
    collection: str
    # First non-empty wins; checked AFTER stripping. Treated as authoritative
    # native summary when len(value) >= MIN_NATIVE_SUMMARY_LEN.
    native_summary_fields: tuple[str, ...] = ()
    # First non-empty wins; fed to LLM as input. Try summary-shaped fields
    # first (they're already compressed), fall back to full body / pdf text.
    body_fields: tuple[str, ...] = ()
    # Doc-level epoch-ms field for time windowing. All platforms write this
    # as part of the standard doc shape (release_time_ms or publish_time_ms).
    time_ms_field: str = "release_time_ms"
    # Human label for log lines.
    label: str = ""
    # Minimum picked-body length to warrant an LLM call. Bump this above the
    # default for collections where the body IS already a card-ready summary
    # (StockHub clamps card preview at 320 chars — bodies shorter than ~400
    # would just have the LLM paraphrase what the user already sees in full).
    min_body_chars: int = DEFAULT_MIN_BODY_CHARS


TARGETS: list[Target] = [
    # ── AlphaPai ───────────────────────────────────────────────────────────
    # reports: contentCn / content are the article body (often start with
    # disclaimers/header for foreign brokers) — no native summary, always LLM.
    Target(
        db="alphapai-full", collection="reports",
        native_summary_fields=(),
        body_fields=("list_item.contentCn", "content", "list_item.content", "pdf_text_md"),
        label="AlphaPai 研报",
    ),
    Target(
        db="alphapai-full", collection="comments",
        native_summary_fields=(),
        body_fields=("content", "list_item.content"),
        label="AlphaPai 点评",
    ),
    Target(
        db="alphapai-full", collection="roadshows",
        native_summary_fields=(),
        body_fields=("content", "segments_md", "list_item.content"),
        label="AlphaPai 路演",
    ),
    Target(
        db="alphapai-full", collection="wechat_articles",
        native_summary_fields=(),
        body_fields=("content", "list_item.content"),
        label="AlphaPai 微信",
    ),

    # ── Jinmen ─────────────────────────────────────────────────────────────
    # Reports + oversea_reports already have summary_md — usually 500-2000
    # chars, perfect for the card preview as-is. Skip LLM.
    Target(
        db="jinmen-full", collection="reports",
        native_summary_fields=("summary_md",),
        body_fields=("summary_md", "summary_point_md", "pdf_text_md"),
        label="进门 A股研报",
    ),
    Target(
        db="jinmen-full", collection="oversea_reports",
        native_summary_fields=("summary_md",),
        body_fields=("summary_md", "pdf_text_md"),
        label="进门 海外研报",
    ),
    # Meetings: chapter_summary_md is a long-form section summary, not a
    # card-ready tldr → run LLM over points_md (highest signal density).
    Target(
        db="jinmen-full", collection="meetings",
        native_summary_fields=(),
        body_fields=("points_md", "chapter_summary_md", "transcript_md"),
        label="进门 会议",
    ),

    # ── Gangtise ───────────────────────────────────────────────────────────
    # brief_md is a prefix of content_md (not a real summary) — always LLM
    # when long enough. min_body_chars=400: gangtise's brief / chief description
    # IS the curated short take. When it's already shorter than ~400 chars the
    # 320-char card preview shows the whole thing — an LLM tldr just paraphrases
    # what the user already reads. (Distribution: ~15-21% of researches and ~5-9%
    # of chief_opinions fall under this floor.)
    Target(
        db="gangtise-full", collection="researches",
        native_summary_fields=(),
        body_fields=("content_md", "brief_md", "pdf_text_md"),
        label="岗底斯 研报",
        min_body_chars=400,
    ),
    Target(
        db="gangtise-full", collection="summaries",
        native_summary_fields=(),
        body_fields=("content_md", "brief_md"),
        label="岗底斯 纪要",
        min_body_chars=400,
    ),
    Target(
        db="gangtise-full", collection="chief_opinions",
        native_summary_fields=(),
        body_fields=("description_md", "content_md", "brief_md"),
        label="岗底斯 首席观点",
        min_body_chars=400,
    ),

    # ── Funda ──────────────────────────────────────────────────────────────
    Target(
        db="funda", collection="posts",
        native_summary_fields=("subtitle",),  # subtitle is sometimes a real tagline
        body_fields=("content_md",),
        label="Funda 帖子",
    ),
    Target(
        db="funda", collection="earnings_reports",
        native_summary_fields=(),
        body_fields=("content_md",),
        label="Funda 财报",
    ),
    Target(
        db="funda", collection="earnings_transcripts",
        native_summary_fields=(),
        body_fields=("content_md",),
        label="Funda 电话会议",
    ),

    # ── AlphaEngine ────────────────────────────────────────────────────────
    # doc_introduce is a snippet of content_md (~150-500 char prefix), not
    # a real summary — always LLM.
    Target(
        db="alphaengine", collection="summaries",
        native_summary_fields=(),
        body_fields=("content_md", "doc_introduce"),
        label="AlphaEngine 纪要",
    ),
    Target(
        db="alphaengine", collection="china_reports",
        native_summary_fields=(),
        body_fields=("content_md", "pdf_text_md", "doc_introduce"),
        label="AlphaEngine 内资研报",
    ),
    Target(
        db="alphaengine", collection="foreign_reports",
        native_summary_fields=(),
        body_fields=("content_md", "pdf_text_md", "doc_introduce"),
        label="AlphaEngine 外资研报",
    ),
    Target(
        db="alphaengine", collection="news_items",
        native_summary_fields=(),
        body_fields=("content_md", "doc_introduce"),
        label="AlphaEngine 新闻",
    ),

    # ── AceCamp ────────────────────────────────────────────────────────────
    # summary_md is sometimes a real summary, sometimes 20-char junk — gate
    # on MIN_NATIVE_SUMMARY_LEN inside the runner.
    Target(
        db="acecamp", collection="articles",
        native_summary_fields=("summary_md",),
        body_fields=("content_md",),
        label="AceCamp 文章",
    ),

    # ── Meritco / Jiuqian ──────────────────────────────────────────────────
    Target(
        db="jiuqian-full", collection="forum",
        native_summary_fields=("summary_md", "insight_md"),
        body_fields=("content_md", "insight_md"),
        label="九纤 论坛",
    ),

    # ── Third Bridge ───────────────────────────────────────────────────────
    Target(
        db="third-bridge", collection="interviews",
        native_summary_fields=(),
        body_fields=("content_md",),
        label="Third Bridge 访谈",
    ),

    # NOTE: ir_filings collections (sec_edgar/hkex/asx/edinet/tdnet/dart/ir_pages)
    # are intentionally NOT summarized here — StockHub doesn't render them as
    # cards (they feed the revenue-segment modeling pipeline instead). Adding
    # them here just burns qwen-plus budget on docs the user can't see.
    # If StockHub ever adds an "IR filings" tab, re-add the targets here.
]


# Schema version — bump when the prompt or output shape changes meaningfully
# so existing ``local_ai_summary`` rows can be re-summarized on next pass.
SUMMARY_SCHEMA_VERSION = 3
