"""Soft-delete docs whose upstream genuinely has no body (Class B).

These docs cannot be recovered by re-crawling — the upstream platform itself
doesn't have the text, OR access is permanently paywalled, OR the source
indexes only metadata (title + URL) by design.

Each rule below was confirmed with raw-payload sampling (see conversation
log); we soft-delete with a per-rule `_deleted_reason` slug for forensics.

Cleanup contract — same shape as cleanup_alphapai_thin_clips.py:
    deleted: True
    _deleted_at: <utc>
    _deleted_reason: <slug>
    _low_value_chars_at_delete: <int — what the body actually was>

Read-side honoring (already wired):
  - backend/app/api/stock_hub.py::_query_spec — `deleted: {$ne: True}` for
    {alphapai/{reports,roadshows,comments}, gangtise/chief_opinions}.
  - backend/app/services/kb_service.py — same 4 collections.
  - kb_vector_ingest.sweep_deleted_docs — drops Milvus chunks at next 03:00 cron.

Other collections need the read-side filter extended; see the per-rule notes.

Usage:
    PYTHONPATH=. python3 scripts/cleanup_low_value_b_class.py            # dry-run
    PYTHONPATH=. python3 scripts/cleanup_low_value_b_class.py --apply
    PYTHONPATH=. python3 scripts/cleanup_low_value_b_class.py --apply --only alphapai_reports_paywall
"""
from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from pymongo import MongoClient


MONGO_URI_DEFAULT = os.environ.get("MONGO_URI", "mongodb://127.0.0.1:27018/")
DEFAULT_THRESHOLD = 100


def _len(field: str) -> dict:
    return {"$strLenCP": {"$ifNull": [f"${field}", ""]}}


def _sum_lens(*fields: str) -> dict:
    return {"$add": [_len(f) for f in fields]}


@dataclass
class CleanupRule:
    name: str
    db: str
    coll: str
    reason_slug: str
    description: str
    body_fields: tuple[str, ...]
    extra_filter: dict  # AND'd with body<150 + deleted!=True


# Body-length expression appended to extra_filter via $expr.
def _build_filter(rule: CleanupRule, threshold: int) -> dict:
    body_expr = _sum_lens(*rule.body_fields, "pdf_text_md")
    flt = dict(rule.extra_filter)
    expr = {"$lt": [body_expr, threshold]}
    if "$expr" in flt:
        flt["$expr"] = {"$and": [flt["$expr"], expr]}
    else:
        flt["$expr"] = expr
    flt["deleted"] = {"$ne": True}
    return flt


# ---------------------------------------------------------------------------
# Rules — one per (collection, root cause).
# ---------------------------------------------------------------------------
RULES: list[CleanupRule] = [
    CleanupRule(
        name="alphapai_reports_paywall",
        db="alphapai-full", coll="reports",
        reason_slug="permission_denied",
        description="alphapai/reports where PDF download hit upstream 401/403; "
                    "no body, no recoverable PDF — soft-delete.",
        body_fields=("content", "list_item.content", "list_item.contentCn"),
        extra_filter={"pdf_error_kind": "permission_denied"},
    ),
    CleanupRule(
        name="chief_opinions_no_url",
        db="gangtise-full", coll="chief_opinions",
        reason_slug="upstream_no_body",
        description="gangtise/chief_opinions with empty parsed_msg.url and tiny body; "
                    "upstream stored only title. No external link to fetch.",
        body_fields=("description_md", "content_md", "brief_md"),
        extra_filter={"$expr": {"$eq": [_len("parsed_msg.url"), 0]}},
    ),
    CleanupRule(
        name="gangtise_summaries_paywall",
        db="gangtise-full", coll="summaries",
        reason_slug="audio_paywall",
        description="gangtise/summaries where canSeeAudio=0 and body < 150; "
                    "transcript locked behind audio paywall, can't recover.",
        body_fields=("content_md", "brief_md"),
        extra_filter={"list_item.canSeeAudio": 0},
    ),
    CleanupRule(
        name="forum_upstream_staged",
        db="jiuqian-full", coll="forum",
        reason_slug="upstream_staged",
        description="jiuqian/forum stubs where insight/summary said '内容更新中' or "
                    "meeting upcoming/cancelled; upstream did not produce content.",
        body_fields=("insight_md", "summary_md", "expert_content_md",
                     "background_md", "topic_md", "content_md"),
        extra_filter={},  # body<150 alone catches them; no more specific signal
    ),
    CleanupRule(
        name="news_items_thin",
        db="alphaengine", coll="news_items",
        reason_slug="upstream_thin",
        description="alphaengine/news_items stubs — platform only stored a "
                    "few-line news brief, no full body or PDF.",
        body_fields=("doc_introduce", "content_md"),
        extra_filter={},
    ),
    CleanupRule(
        name="thirdbridge_no_transcript",
        db="third-bridge", coll="interviews",
        reason_slug="upstream_no_transcript",
        description="third-bridge/interviews stubs without transcript; "
                    "upstream listed the meeting but never published the body.",
        body_fields=("agenda_md", "specialists_md", "introduction_md",
                     "transcript_md", "commentary_md"),
        extra_filter={},
    ),
    CleanupRule(
        name="alphaengine_china_thin",
        db="alphaengine", coll="china_reports",
        reason_slug="upstream_thin",
        description="alphaengine/china_reports tiny-body stubs after PDF channel exhausted.",
        body_fields=("doc_introduce", "content_md"),
        extra_filter={},
    ),
    CleanupRule(
        name="alphaengine_summaries_thin",
        db="alphaengine", coll="summaries",
        reason_slug="upstream_thin",
        description="alphaengine/summaries tiny-body stubs.",
        body_fields=("doc_introduce", "content_md"),
        extra_filter={},
    ),
    CleanupRule(
        name="funda_earnings_reports_thin",
        db="funda", coll="earnings_reports",
        reason_slug="upstream_thin",
        description="funda/earnings_reports rare empty stubs (4 docs).",
        body_fields=("content_md",),
        extra_filter={},
    ),
    # Gangtise reports/chief where the actual body lives on mp.weixin.qq.com.
    # Anonymous bypass attempts (mobile UA, MicroMessenger UA, Jina Reader) all
    # blocked by wechat's "环境异常" wall (2026-04-29). Tested in conversation;
    # using our own MP credentials risks account ban. Mark as B-class with a
    # distinct reason so we can revisit if/when wechat fetcher is built.
    CleanupRule(
        name="gangtise_researches_weixin_only",
        db="gangtise-full", coll="researches",
        reason_slug="external_link_only",
        description="gangtise/researches with platform_no_body=True AND no "
                    "pdf_rel_path AND no pdf_local_path — gangtise API returned "
                    "content=None and there's no PDF channel to fall back on. "
                    "Excludes docs whose PDF is queued for download (recently "
                    "ingested CDN-PDF docs would heal once extract_pdf_texts.py "
                    "runs against them).",
        body_fields=("brief_md", "content_md"),
        extra_filter={
            "platform_no_body": True,
            "pdf_rel_path": {"$in": [None, ""]},
            "pdf_local_path": {"$in": [None, ""]},
        },
    ),
    CleanupRule(
        name="chief_opinions_weixin_only",
        db="gangtise-full", coll="chief_opinions",
        reason_slug="external_link_only",
        description="gangtise/chief_opinions with parsed_msg.url (weixin/youdao/95579) "
                    "but body still <100 — full text on external platform, anonymous "
                    "fetch blocked. Skip until external fetcher exists.",
        body_fields=("description_md", "content_md", "brief_md"),
        extra_filter={"$expr": {"$gt": [_len("parsed_msg.url"), 0]}},
    ),
]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mongo-uri", default=MONGO_URI_DEFAULT)
    parser.add_argument("--apply", action="store_true",
                        help="Actually soft-delete. Default is dry-run.")
    parser.add_argument("--only", action="append", default=[],
                        help="Restrict to specific rule names. Repeatable.")
    parser.add_argument("--threshold", type=int, default=DEFAULT_THRESHOLD,
                        help=f"Body+pdf_text length cutoff (default {DEFAULT_THRESHOLD}).")
    args = parser.parse_args()

    os.environ.setdefault("NO_PROXY", "127.0.0.1,localhost")
    os.environ.setdefault("no_proxy", "127.0.0.1,localhost")

    client = MongoClient(args.mongo_uri, serverSelectionTimeoutMS=5_000)

    rules = [r for r in RULES if not args.only or r.name in args.only]
    if not rules:
        print(f"[error] no rules matched --only={args.only}")
        return

    mode = "APPLY" if args.apply else "DRY-RUN"
    print(f"[cleanup_low_value_b_class] mode={mode} threshold={args.threshold} rules={len(rules)}")

    grand = grand_apply = 0
    for rule in rules:
        coll = client[rule.db][rule.coll]
        flt = _build_filter(rule, args.threshold)
        n = coll.count_documents(flt)
        print(f"\n  [{rule.name}] {rule.db}/{rule.coll} → {rule.reason_slug}")
        print(f"      {rule.description}")
        print(f"      match: {n}")

        if n == 0:
            continue

        # 3 sample titles for visibility
        for d in coll.find(flt, {"title": 1, "organization": 1, "release_time": 1}).limit(3):
            t = (d.get("title") or "").strip()[:80]
            print(f"        sample: [{d['_id']}] org={d.get('organization')!r} t={t!r}")

        grand += n

        if not args.apply:
            continue

        # Pipeline-form $set so we can capture the actual body length at delete time.
        body_expr = _sum_lens(*rule.body_fields, "pdf_text_md")
        update = [
            {"$set": {
                "deleted": True,
                "_deleted_at": datetime.now(timezone.utc),
                "_deleted_reason": rule.reason_slug,
                "_low_value_chars_at_delete": body_expr,
            }}
        ]
        res = coll.update_many(flt, update)
        print(f"      [apply] soft-deleted: {res.modified_count}")
        grand_apply += res.modified_count

    print()
    if args.apply:
        print(f"[summary] soft-deleted: {grand_apply}")
    else:
        print(f"[summary] would-soft-delete: {grand}")
        print(f"[hint] re-run with --apply to commit.")


if __name__ == "__main__":
    main()
