"""One-shot cleanup for gangtise chief_opinions:

  Phase 1 — empty content (legacy bug residue)
    Soft-delete chief_opinions where content / description are absent and the
    historical title-fallback bug had set content_md == title. Attachments
    (is_attachment=True) are exempt — image / PDF payload is the document.

  Phase 2 — cross-collection duplicates
    Soft-delete chief_opinions whose (organization, release_time_ms,
    normalized_title) matches an existing researches doc. Same upstream
    article posted to both gangtise feeds; researches is canonical.

Soft-delete writes {deleted: true, _deleted_at: <utc>, _deleted_reason: <str>}.
Consumers gate on `deleted: {$ne: true}`. The Milvus delete sweep
(`sweep_deleted_docs`) treats soft-deleted docs as tombstones and removes
their chunks on the next pass.

Usage:
    PYTHONPATH=. python3 scripts/cleanup_gangtise_chief.py            # dry-run
    PYTHONPATH=. python3 scripts/cleanup_gangtise_chief.py --apply
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import unicodedata
from collections import Counter
from datetime import datetime, timezone

from pymongo import MongoClient
from pymongo.errors import PyMongoError


MONGO_URI_DEFAULT = os.environ.get("MONGO_URI", "mongodb://127.0.0.1:27018/")
MONGO_DB_DEFAULT = os.environ.get("MONGO_DB", "gangtise-full")

_NORM_PUNCT = re.compile(r"[\s　\W_]+", re.UNICODE)


def _norm_title(s) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKC", str(s)).lower().strip()
    return _NORM_PUNCT.sub("", s)


def _is_quote_noise(text: str) -> bool:
    """Mirror of crawl/gangtise/scraper.py::_is_quote_noise. True if OCR text
    looks like stock-quote / K-line panel noise rather than article body."""
    if not text:
        return True
    s = text.strip()
    if len(s) < 60:
        return True
    digit_ratio = sum(1 for ch in s if ch.isdigit()) / max(len(s), 1)
    symbol_chars = "%¥$.,()[]+-/:"
    symbol_ratio = sum(1 for ch in s if ch in symbol_chars) / max(len(s), 1)
    if len(s) < 300 and digit_ratio > 0.25 and symbol_ratio > 0.05:
        return True
    if "成交量" in s and len(s) < 300 and digit_ratio > 0.2:
        return True
    return False


def _strip_html(s: str) -> str:
    """Plain-text fallback (mirrors scraper.py._strip_html lite version)."""
    if not s:
        return ""
    s = re.sub(r"<[^>]+>", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()


_URL_ONLY_RE = re.compile(r'^\s*https?://\S+\s*$', re.IGNORECASE)
_DOMAIN_ONLY_RE = re.compile(
    r'^\s*[\w.-]+\.(com|cn|net|org|io|app|co|me|info|xyz|hk|jp|kr|us|edu)(/\S*)?\s*$',
    re.IGNORECASE,
)


def _looks_like_url_or_domain(text: str) -> bool:
    """Mirror of crawl/gangtise/scraper.py::_looks_like_url_or_domain.

    True if text is just a URL or bare domain (e.g. "note.youdao.com",
    "https://share.note.youdao.com/...") — chief items where description
    is set to a click-through teaser instead of real body.
    """
    if not text:
        return False
    s = text.strip()
    if len(s) > 200:
        return False
    return bool(_URL_ONLY_RE.match(s) or _DOMAIN_ONLY_RE.match(s))


def _is_title_echo(text: str, title: str) -> bool:
    """Detect "description is just a title-echo" (no real body).

    Examples that should match (drop):
      title="Sigma Lithium2025Q4 季度报告【华西有色-晏溶团队】"
      desc ="Sigma Lithium2025Q4 季度报告"   ← title minus suffix → echo

      title="行业周报：原油价格回顾"
      desc ="行业周报：原油价格回顾"          ← exact equal → echo

    Examples that must NOT match (keep):
      title="泸州老窖：锚定创新高地"
      desc ="事件：今日，我们参加了泸州老窖2024年股东大会..."  ← real body
    """
    if not text:
        return True  # missing description ≡ echo (no body either way)
    t_norm = _norm_title(text)
    title_norm = _norm_title(title)
    if not t_norm or not title_norm:
        return False
    if t_norm == title_norm:
        return True
    # description is short AND is contained in title (the Sigma case is a
    # prefix; some are middle substrings). Bound by 80 raw chars so a real
    # 200-char analyst note can't accidentally match.
    if len(text) <= 80 and (t_norm in title_norm or title_norm.startswith(t_norm)):
        return True
    return False


def _is_empty_chief(doc: dict) -> bool:
    """Return True if this chief_opinion has no real text body.

    2026-04-29 unified rule:
      - attachments share the same gate (after OCR backfill step they have
        real content_md if anything useful was OCR'd);
      - description / content must be substantive: non-empty, non title-echo,
        and not a bare URL/domain (e.g. "note.youdao.com" — pure click-through
        teaser).
    """
    title = (doc.get("title") or "").strip()
    content = (doc.get("content_md") or "").strip()
    description = (doc.get("description_md") or "").strip()

    def _has_real_body(s: str) -> bool:
        if not s:
            return False
        if _is_title_echo(s, title):
            return False
        if _looks_like_url_or_domain(s):
            return False
        if s == title:
            return False
        return True

    return not (_has_real_body(description) or _has_real_body(content))


def phase0_backfill_attachment_ocr(db, *, apply: bool) -> dict:
    """Recover OCR-extracted article body that the old dump_chief threw away.

    Old code: ``body = description`` for attachments (assumed parsed.content
    was K-line / quote-screen noise). Reality: ~86% of attachment chief items
    have rich WeChat-article OCR in parsed_msg.content. Re-extract those:
      - if parsed_msg.content is a real OCR body (non-noise) → write
        content_md (and brief_md when missing).
      - if it's quote-screen noise or empty → leave alone; phase 1 will
        soft-delete in the next step.
    """
    coll = db["chief_opinions"]
    cursor = coll.find(
        {
            "is_attachment": True,
            "deleted": {"$ne": True},
            "$or": [
                {"content_md": {"$in": [None, ""]}},
                {"content_md": {"$exists": False}},
            ],
        },
        projection={"_id": 1, "parsed_msg.content": 1, "description_md": 1},
        no_cursor_timeout=True,
    )
    n_recovered = 0
    n_noise_or_empty = 0
    samples: list[tuple[str, int]] = []
    try:
        for doc in cursor:
            ocr_raw = ((doc.get("parsed_msg") or {}).get("content") or "")
            ocr = _strip_html(ocr_raw)
            if _is_quote_noise(ocr):
                n_noise_or_empty += 1
                continue
            n_recovered += 1
            if len(samples) < 8:
                samples.append((doc["_id"], len(ocr)))
            if apply:
                update = {"content_md": ocr, "_ocr_recovered_at": datetime.now(timezone.utc)}
                # 仅当 brief_md 也空时才写, 避免覆盖人工/平台已有 brief
                d2 = coll.find_one({"_id": doc["_id"]}, {"brief_md": 1})
                if not (d2 or {}).get("brief_md"):
                    update["brief_md"] = ocr[:500]
                coll.update_one({"_id": doc["_id"]}, {"$set": update})
    finally:
        cursor.close()

    print(f"\n=== Phase 0: attachment OCR backfill ({'APPLY' if apply else 'DRY-RUN'}) ===")
    print(f"  扫到 attachment 空 content_md 条目")
    print(f"  可回填 (parsed.content 非噪声): {n_recovered}")
    print(f"  无效 (噪声/空):                {n_noise_or_empty}")
    if samples:
        print("  Samples (recovered):")
        for cid, ln in samples:
            print(f"    [{cid:<24}] OCR_len={ln}")
    return {"recovered": n_recovered, "noise_or_empty": n_noise_or_empty}


def soft_delete(coll, _id: str, reason: str) -> None:
    coll.update_one(
        {"_id": _id},
        {"$set": {
            "deleted": True,
            "_deleted_at": datetime.now(timezone.utc),
            "_deleted_reason": reason,
        }},
    )


def phase1_empty(db, *, apply: bool) -> dict:
    coll = db["chief_opinions"]
    cursor = coll.find(
        {"deleted": {"$ne": True}},
        projection={
            "_id": 1, "title": 1, "content_md": 1,
            "description_md": 1, "is_attachment": 1, "organization": 1,
        },
        no_cursor_timeout=True,
    )
    by_org: Counter[str] = Counter()
    samples: list[tuple[str, str, str]] = []
    n_target = 0
    try:
        for doc in cursor:
            if not _is_empty_chief(doc):
                continue
            n_target += 1
            org = (doc.get("organization") or "").strip() or "(无)"
            by_org[org] += 1
            if len(samples) < 12:
                samples.append((doc["_id"], org,
                                (doc.get("title") or "").strip()[:60]))
            if apply:
                soft_delete(coll, doc["_id"], "empty_content")
    finally:
        cursor.close()

    print(f"\n=== Phase 1: empty content ({'APPLY' if apply else 'DRY-RUN'}) ===")
    print(f"  待软删: {n_target}")
    if by_org:
        print("  Top organizations:")
        for org, n in by_org.most_common(15):
            print(f"    {n:>5}  {org}")
    if samples:
        print("  Samples:")
        for _id, org, title in samples:
            print(f"    [{_id[:24]:<24}] {org} · {title}")
    return {"target": n_target, "applied": n_target if apply else 0}


def phase2_dup_research(db, *, apply: bool) -> dict:
    """Match (organization, release_time_ms, normalized_title) — three keys.

    Builds an in-memory index from researches first (~30k docs is fine), then
    walks chief_opinions and soft-deletes hits.
    """
    print("\n=== Phase 2: cross-collection duplicates ===")
    print("  building researches index (organization, release_time_ms, _norm_title)...")
    research_idx: dict[tuple[str, int, str], str] = {}
    rcur = db["researches"].find(
        {},
        projection={"_id": 1, "title": 1, "organization": 1,
                    "release_time_ms": 1, "_norm_title": 1},
        no_cursor_timeout=True,
    )
    try:
        for r in rcur:
            org = (r.get("organization") or "").strip()
            ms = r.get("release_time_ms")
            if not (org and ms):
                continue
            norm = r.get("_norm_title") or _norm_title(r.get("title"))
            if not norm:
                continue
            research_idx[(org, int(ms), norm)] = r["_id"]
    finally:
        rcur.close()
    print(f"  research index size: {len(research_idx)}")

    coll = db["chief_opinions"]
    cursor = coll.find(
        {"deleted": {"$ne": True}, "is_attachment": {"$ne": True}},
        projection={
            "_id": 1, "title": 1, "organization": 1,
            "release_time_ms": 1, "_norm_title": 1,
        },
        no_cursor_timeout=True,
    )
    by_org: Counter[str] = Counter()
    samples: list[tuple[str, str, str, str]] = []
    n_target = 0
    try:
        for doc in cursor:
            org = (doc.get("organization") or "").strip()
            ms = doc.get("release_time_ms")
            if not (org and ms):
                continue
            norm = doc.get("_norm_title") or _norm_title(doc.get("title"))
            if not norm:
                continue
            rpt_id = research_idx.get((org, int(ms), norm))
            if not rpt_id:
                continue
            n_target += 1
            by_org[org] += 1
            if len(samples) < 12:
                samples.append((doc["_id"], org,
                                (doc.get("title") or "").strip()[:50],
                                rpt_id))
            if apply:
                soft_delete(coll, doc["_id"], f"dup_research:{rpt_id}")
    finally:
        cursor.close()

    print(f"  待软删 ({'APPLY' if apply else 'DRY-RUN'}): {n_target}")
    if by_org:
        print("  Top organizations:")
        for org, n in by_org.most_common(15):
            print(f"    {n:>5}  {org}")
    if samples:
        print("  Samples (chief_id  org · title  → research_id):")
        for cid, org, title, rid in samples:
            print(f"    [{cid[:24]:<24}] {org} · {title}  → {rid}")
    return {"target": n_target, "applied": n_target if apply else 0}


def backfill_norm_title(db) -> dict:
    """Backfill `_norm_title` on existing docs (added by 2026-04-29 patch).

    Phase 2 needs `_norm_title` on both collections to match efficiently in
    production (chief side via the index, sweep-time check). Existing docs
    written before today have no `_norm_title` field. This is idempotent.
    """
    print("\n=== Backfill _norm_title (idempotent) ===")
    res_n = chief_n = 0
    for cname in ("researches", "chief_opinions"):
        coll = db[cname]
        cur = coll.find(
            {"_norm_title": {"$exists": False}},
            projection={"_id": 1, "title": 1},
            no_cursor_timeout=True,
        )
        try:
            for d in cur:
                norm = _norm_title(d.get("title"))
                coll.update_one({"_id": d["_id"]}, {"$set": {"_norm_title": norm}})
                if cname == "researches":
                    res_n += 1
                else:
                    chief_n += 1
        finally:
            cur.close()
    print(f"  researches  backfilled: {res_n}")
    print(f"  chief_opinions backfilled: {chief_n}")
    return {"researches": res_n, "chief_opinions": chief_n}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--mongo-uri", default=MONGO_URI_DEFAULT)
    ap.add_argument("--mongo-db", default=MONGO_DB_DEFAULT)
    ap.add_argument("--apply", action="store_true",
                    help="actually write the soft-delete (default: dry-run)")
    ap.add_argument("--skip-backfill", action="store_true",
                    help="skip the _norm_title backfill step (assume already done)")
    ap.add_argument("--phase", choices=["0", "1", "2", "all"], default="all")
    args = ap.parse_args()

    client = MongoClient(args.mongo_uri, serverSelectionTimeoutMS=5000)
    try:
        client.admin.command("ping")
    except PyMongoError as e:
        print(f"错误: 无法连接 MongoDB ({args.mongo_uri}): {e}")
        return 2
    db = client[args.mongo_db]
    print(f"[Mongo] {args.mongo_uri} -> db: {args.mongo_db}")
    print(f"[Mode] {'APPLY (writing)' if args.apply else 'DRY-RUN (no writes)'}")

    if not args.skip_backfill:
        backfill_norm_title(db)
    # Phase 0 must run BEFORE phase 1 — it recovers OCR'd article body that
    # the old dump_chief threw out, so phase 1 can correctly classify which
    # attachments are truly empty vs which are full-text articles.
    if args.phase in ("0", "all"):
        phase0_backfill_attachment_ocr(db, apply=args.apply)
    if args.phase in ("1", "all"):
        phase1_empty(db, apply=args.apply)
    if args.phase in ("2", "all"):
        phase2_dup_research(db, apply=args.apply)

    print("\nDone.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
