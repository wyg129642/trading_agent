"""Enrich every crawled document with ``_canonical_tickers`` + ``_unmatched_raw``
+ ``_raw_tickers``.

Idempotent, non-destructive — only adds/updates the derived fields; all
original fields are left untouched. Safe to re-run after alias-table edits.

Usage::

    # One-shot full pass (all 5 DBs × all collections)
    python3 scripts/enrich_tickers.py

    # Only one DB
    python3 scripts/enrich_tickers.py --source alphapai

    # Skip specific source.collection combos
    python3 scripts/enrich_tickers.py --exclude jinmen.oversea_reports

    # Show what would be updated without writing
    python3 scripts/enrich_tickers.py --dry-run

    # Only print the TOP-N unmatched raw strings (for alias dict expansion)
    python3 scripts/enrich_tickers.py --report-unmatched 30

Fields added to each document::

    _canonical_tickers:          list[str]   # e.g. ["INTC.US", "AAPL.US"]
    _canonical_tickers_at:       ISODate     # last enrichment timestamp
    _unmatched_raw:              list[str]   # raw strings we could NOT map
    _canonical_extract_source:   str         # which extractor was used (provenance)
    _raw_tickers:                list        # preserved raw extractor output
                                             # (list of dict | str), unified
                                             # access regardless of platform schema

Indexes created::

    <coll>._canonical_tickers       (ascending single-field)

The script discovers the MongoDB URI/DB from ``backend.app.config.Settings``
so no hard-coded config here.
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

from motor.motor_asyncio import AsyncIOMotorClient  # noqa: E402
from pymongo import UpdateOne  # noqa: E402

from backend.app.config import get_settings  # noqa: E402
from backend.app.services.ticker_normalizer import (  # noqa: E402
    EXTRACTORS,
    extract_tickers_from_text,
    normalize_raw_for_storage,
    normalize_with_unmatched,
    reload_aliases,
)


# Source → (settings_uri_attr, settings_db_attr, [collection list])
SOURCES: dict[str, tuple[str, str, list[str]]] = {
    "alphapai": (
        "alphapai_mongo_uri",
        "alphapai_mongo_db",
        ["roadshows", "reports", "comments", "wechat_articles"],
    ),
    "jinmen": ("jinmen_mongo_uri", "jinmen_mongo_db", ["meetings", "reports", "oversea_reports"]),
    "meritco": ("meritco_mongo_uri", "meritco_mongo_db", ["forum", "research"]),
    "thirdbridge": (
        "thirdbridge_mongo_uri",
        "thirdbridge_mongo_db",
        ["interviews"],
    ),
    "funda": (
        "funda_mongo_uri",
        "funda_mongo_db",
        ["posts", "earnings_reports", "earnings_transcripts", "sentiments"],
    ),
    "acecamp": (
        "acecamp_mongo_uri",
        "acecamp_mongo_db",
        ["articles", "events"],
    ),
    "alphaengine": (
        "alphaengine_mongo_uri",
        "alphaengine_mongo_db",
        ["summaries", "china_reports", "foreign_reports", "news_items"],
    ),
    "gangtise": (
        "gangtise_mongo_uri",
        "gangtise_mongo_db",
        ["summaries", "researches", "chief_opinions"],
    ),
    "semianalysis": (
        "semianalysis_mongo_uri",
        "semianalysis_mongo_db",
        ["semianalysis_posts"],
    ),
}


async def enrich_collection(
    client: AsyncIOMotorClient,
    db_name: str,
    coll_name: str,
    extractor,
    source_key: str,
    *,
    dry_run: bool = False,
    limit: int | None = None,
    incremental: bool = False,
    only_empty: bool = False,
) -> tuple[int, int, int, Counter, int]:
    """Enrich one collection.

    Returns (scanned, updated, with_tickers, unmatched_counter).

    ``incremental=True`` → only process docs missing the derived field (fast path
    after the initial full pass; suitable for nightly cron).
    """
    coll = client[db_name][coll_name]
    scanned = 0
    updated = 0
    with_tickers = 0
    with_tickers_from_title = 0
    unmatched_counter: Counter = Counter()

    query: dict = {}
    if incremental:
        query = {"_canonical_tickers_at": {"$exists": False}}
    elif only_empty:
        # Re-process only docs currently marked empty — useful after the
        # normalizer gets a new suffix-alias table or title fallback.
        query = {"_canonical_tickers": []}

    # Don't pull the heavy fields into memory — only what the extractor needs
    projection = {
        # alphapai
        "stock": 1,
        "list_item.stock": 1,
        # jinmen (meetings/oversea_reports use stocks; reports uses companies)
        "stocks": 1,
        "companies": 1,
        # meritco (forum: related_targets[]; research: list_item.tag1 is the expert's company)
        "related_targets": 1,
        "list_item.tag1": 1,
        # thirdbridge
        "target_companies": 1,
        "relevant_companies": 1,
        # funda
        "entities.tickers": 1,
        "ticker": 1,
        # acecamp (inner has ticker strings, outer only has name)
        "corporations": 1,
        "list_item.corporations": 1,
        # alphaengine
        "company_codes": 1,
        "company_names": 1,
        # gangtise (top-level stocks + chief_opinions nested shapes)
        "list_item.emoSecurities": 1,
        "list_item.labelDisplays": 1,
        "list_item.aflScr": 1,
        # semianalysis
        "title": 1,
        "subtitle": 1,
        "truncated_body_text": 1,
        "detail_result.postTags": 1,
        # Title fallback (2026-04-24): for every doc where the structured
        # extractor finds nothing, scan title for parenthesized `(CODE.MARKET)`
        # or `(CODE:MARKET)` patterns. Additional title-bearing fields follow.
        "title_cn": 1,
        "title_en": 1,
    }

    cursor = coll.find(query, projection=projection)
    if limit:
        cursor = cursor.limit(limit)

    now = datetime.now(timezone.utc)
    BATCH = 500
    pending: list[UpdateOne] = []

    async def _flush() -> None:
        nonlocal updated
        if not pending:
            return
        result = await coll.bulk_write(pending, ordered=False)
        updated += (result.matched_count or 0)
        pending.clear()

    async for doc in cursor:
        scanned += 1
        raw = extractor(doc, coll_name)
        matched, unmatched = normalize_with_unmatched(raw)
        extract_source = source_key

        # --- Title fallback (2026-04-24) -------------------------------------
        # If the structured extractor found nothing, scan titles for embedded
        # CODE.MARKET / CODE:MARKET parenthesized forms. Covers:
        #   - jinmen.oversea_reports: `Kakaku.com Inc.(2371.JPN)`
        #   - alphapai.roadshows: `Best Buy (BBY.N）`, `ARC Resources (ARX:CA)`
        #   - gangtise.researches / chief_opinions: `Pluxee (PLX.PA):`
        #   - alphaengine.foreign_reports: `Helmerich & Payne Inc. (HP)` …
        if not matched:
            for field in ("title", "title_cn", "title_en"):
                title = doc.get(field)
                if not isinstance(title, str) or not title.strip():
                    continue
                title_hits = extract_tickers_from_text(title)
                if title_hits:
                    matched = title_hits
                    extract_source = f"{source_key}_title"
                    break
        # --------------------------------------------------------------------

        if matched:
            with_tickers += 1
            if extract_source.endswith("_title"):
                with_tickers_from_title += 1
        for u in unmatched:
            unmatched_counter[u] += 1

        if dry_run:
            continue

        pending.append(UpdateOne(
            {"_id": doc["_id"]},
            {"$set": {
                "_canonical_tickers": matched,
                "_canonical_tickers_at": now,
                "_unmatched_raw": unmatched,
                "_canonical_extract_source": extract_source,
                "_raw_tickers": normalize_raw_for_storage(raw),
            }},
        ))
        if len(pending) >= BATCH:
            await _flush()

    if not dry_run:
        await _flush()

    return scanned, updated, with_tickers, unmatched_counter, with_tickers_from_title


async def ensure_index(client: AsyncIOMotorClient, db_name: str, coll_name: str) -> None:
    """Create the canonical-ticker index. Tolerate the case where another code
    path already created the same index under a different name (e.g. KB sync
    created `kb_canonical_tickers`). MongoDB raises 85 IndexOptionsConflict on
    re-create with a different name even though the spec is identical."""
    from pymongo.errors import OperationFailure
    coll = client[db_name][coll_name]
    try:
        await coll.create_index("_canonical_tickers")
    except OperationFailure as exc:
        if exc.code == 85:  # index exists with a different name — fine
            return
        raise


async def main_async(args: argparse.Namespace) -> int:
    settings = get_settings()

    # Which sources to run
    if args.source and args.source != "all":
        if args.source not in SOURCES:
            print(f"ERR: unknown --source '{args.source}'. Known: {list(SOURCES)}")
            return 2
        targets = {args.source: SOURCES[args.source]}
    else:
        targets = SOURCES

    if args.reload_aliases:
        reload_aliases()

    exclude_set: set[str] = set()
    if args.exclude:
        exclude_set = {x.strip() for x in args.exclude.split(",") if x.strip()}

    total_scanned = 0
    total_updated = 0
    total_with = 0
    total_from_title = 0
    total_unmatched: Counter = Counter()

    # One shared client with URI per source (motor pools connections anyway)
    clients_by_uri: dict[str, AsyncIOMotorClient] = {}

    print(f"{'SOURCE / COLLECTION':<36} {'scanned':>10} {'updated':>10} {'w/ticker':>9} {'of-title':>9}")
    print("-" * 85)
    for source_key, (uri_attr, db_attr, collections) in targets.items():
        uri = getattr(settings, uri_attr)
        db_name = getattr(settings, db_attr)
        if uri not in clients_by_uri:
            clients_by_uri[uri] = AsyncIOMotorClient(uri, tz_aware=True)
        client = clients_by_uri[uri]
        extractor = EXTRACTORS[source_key]

        for coll_name in collections:
            full_tag = f"{source_key}.{coll_name}"
            if full_tag in exclude_set:
                print(f"{full_tag:<36} {'(skipped via --exclude)':>50}")
                continue
            scanned, updated, with_tickers, unmatched, from_title = await enrich_collection(
                client,
                db_name,
                coll_name,
                extractor,
                source_key,
                dry_run=args.dry_run,
                limit=args.limit,
                incremental=args.incremental,
                only_empty=args.only_empty,
            )
            total_scanned += scanned
            total_updated += updated
            total_with += with_tickers
            total_from_title += from_title
            total_unmatched.update(unmatched)

            tag = f"{source_key}.{coll_name}"
            pct = f"({100 * with_tickers // scanned}%)" if scanned else "—"
            print(f"{tag:<36} {scanned:>10} {updated:>10} {with_tickers:>5} {pct:>5} {from_title:>8}")

            if not args.dry_run:
                await ensure_index(client, db_name, coll_name)

    print("-" * 85)
    print(f"{'TOTAL':<36} {total_scanned:>10} {total_updated:>10} {total_with:>9} {total_from_title:>9}")
    print(f"  of which {total_from_title:,} tagged via title-fallback regex (2026-04-24 feature)")
    if not args.dry_run:
        print("Indexes created: _canonical_tickers on every collection listed above.")

    if args.report_unmatched:
        print()
        print(f"=== Top {args.report_unmatched} unmatched raw strings (candidates for aliases.json) ===")
        for raw, n in total_unmatched.most_common(args.report_unmatched):
            print(f"  {n:>5} × {raw}")

    # Close clients
    for client in clients_by_uri.values():
        client.close()
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument(
        "--source",
        default="all",
        choices=["all", *SOURCES.keys()],
        help="Which data source to enrich (default: all)",
    )
    ap.add_argument("--limit", type=int, help="Max docs per collection (for testing)")
    ap.add_argument(
        "--incremental",
        action="store_true",
        help="Only process docs missing _canonical_tickers_at (fast nightly mode).",
    )
    ap.add_argument(
        "--only-empty",
        action="store_true",
        help="Only re-process docs currently marked _canonical_tickers:[] — "
             "useful after extending the market-suffix alias table or title regex.",
    )
    ap.add_argument("--dry-run", action="store_true", help="Don't write any fields")
    ap.add_argument(
        "--reload-aliases",
        action="store_true",
        help="Force reload aliases.json before running",
    )
    ap.add_argument(
        "--report-unmatched",
        type=int,
        default=0,
        metavar="N",
        help="Print top-N unmatched raw strings at the end (for alias dict expansion)",
    )
    ap.add_argument(
        "--exclude",
        type=str,
        default="",
        metavar="SOURCE.COLL[,SOURCE.COLL]",
        help="Skip these source.collection combos (e.g. jinmen.oversea_reports)",
    )
    args = ap.parse_args()
    return asyncio.run(main_async(args))


if __name__ == "__main__":
    sys.exit(main())
