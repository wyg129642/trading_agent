#!/usr/bin/env python3
"""Create MongoDB indexes required by the KB service.

Two index classes per collection:

1. **Filter indexes** (btree): ``_canonical_tickers`` multikey + sort field
   compound, plus sort field alone. These are what ``kb_search`` actually hits
   in the critical path.

2. **Text indexes** (optional): one per primary text field, ``default_language=none``
   to avoid English stemming being applied to Chinese text. The KB service scores
   in Python (char-bigram + token substring) so the text index is not required
   today, but is kept ready for Phase B or for ad-hoc full-text queries.

Idempotent: re-running is safe. ``create_index`` with the same key spec is a
no-op; ``create_index`` with the same name but different spec is an error that
we catch and skip.

Usage:
    python3 scripts/kb_create_indexes.py                     # default: create all
    python3 scripts/kb_create_indexes.py --dry-run           # just print plan
    python3 scripts/kb_create_indexes.py --skip-text         # skip $text indexes
    python3 scripts/kb_create_indexes.py --only alphapai     # one db only
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from dataclasses import dataclass

from pymongo import ASCENDING, DESCENDING, MongoClient, TEXT
from pymongo.errors import OperationFailure

# Allow running as a script from repo root
sys.path.insert(0, "/home/ygwang/trading_agent")

from backend.app.config import get_settings  # noqa: E402
from backend.app.services.kb_service import SPECS_LIST  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("kb_create_indexes")


@dataclass
class IndexPlan:
    db: str
    collection: str
    keys: list[tuple[str, int | str]]
    name: str
    kind: str  # "btree" | "text"
    kwargs: dict


def _plan(skip_text: bool) -> list[IndexPlan]:
    plans: list[IndexPlan] = []
    for s in SPECS_LIST:
        # 1) Sort field alone (recency scans with no ticker filter)
        sort_field = s.date_ms_field or s.date_str_field
        if sort_field:
            plans.append(IndexPlan(
                s.db, s.collection,
                [(sort_field, DESCENDING)],
                f"kb_{sort_field}_desc",
                "btree", {},
            ))
        # 2) Ticker + sort compound (ticker-filtered recency scans)
        if sort_field:
            plans.append(IndexPlan(
                s.db, s.collection,
                [("_canonical_tickers", ASCENDING), (sort_field, DESCENDING)],
                f"kb_canonical_tickers_{sort_field}",
                "btree", {},
            ))
        # 3) Ticker-only (for facets/tickers without sort)
        plans.append(IndexPlan(
            s.db, s.collection,
            [("_canonical_tickers", ASCENDING)],
            "kb_canonical_tickers",
            "btree", {},
        ))
        # 4) Ticker fallback path
        if s.ticker_fallback_path == "stocks":
            plans.append(IndexPlan(
                s.db, s.collection,
                [("stocks.code", ASCENDING)],
                "kb_stocks_code",
                "btree", {},
            ))
        elif s.ticker_fallback_path == "companies":
            plans.append(IndexPlan(
                s.db, s.collection,
                [("companies.stockcode", ASCENDING)],
                "kb_companies_stockcode",
                "btree", {},
            ))
        # 5) Text index on title + primary text field (optional; Phase B)
        if not skip_text:
            primary_text = s.text_fields[0] if s.text_fields else None
            if primary_text:
                text_keys = [(s.title_field, TEXT), (primary_text, TEXT)]
                plans.append(IndexPlan(
                    s.db, s.collection,
                    text_keys,
                    f"kb_text_{s.title_field}_{primary_text}",
                    "text",
                    {"default_language": "none", "weights": {s.title_field: 3, primary_text: 1}},
                ))
    return plans


def _apply(client: MongoClient, plan: IndexPlan, dry_run: bool) -> tuple[str, str]:
    coll = client[plan.db][plan.collection]
    if dry_run:
        return "DRY", f"{plan.db}.{plan.collection} / {plan.name}"
    start = time.monotonic()
    try:
        coll.create_index(plan.keys, name=plan.name, background=True, **plan.kwargs)
        elapsed = time.monotonic() - start
        return "OK", f"{plan.db}.{plan.collection} / {plan.name}  [{elapsed:.2f}s]"
    except OperationFailure as e:
        # index name conflict / options conflict: best-effort detail
        msg = str(e).split("\n")[0][:200]
        # 86 = IndexKeySpecsConflict, 85 = IndexOptionsConflict — "already exists with different options"
        if "already exists" in msg or "IndexOptionsConflict" in msg or e.code in (85, 86):
            return "SKIP", f"{plan.db}.{plan.collection} / {plan.name}  (already exists with differing options; leaving as-is)"
        if "ns not found" in msg or e.code == 26:
            return "SKIP", f"{plan.db}.{plan.collection} / {plan.name}  (collection does not exist)"
        return "ERR", f"{plan.db}.{plan.collection} / {plan.name}  → {msg}"
    except Exception as e:
        return "ERR", f"{plan.db}.{plan.collection} / {plan.name}  → {e}"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--skip-text", action="store_true",
                    help="Skip $text indexes (Phase A doesn't use them).")
    ap.add_argument("--only", default=None, help="Only one db, e.g. 'alphapai'")
    args = ap.parse_args()

    settings = get_settings()
    uri = settings.alphapai_mongo_uri  # all platforms share local Mongo URI
    log.info("Connecting to %s", uri)
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    try:
        client.admin.command("ping")
    except Exception as e:
        log.error("MongoDB not reachable: %s", e)
        return 2

    plans = _plan(skip_text=args.skip_text)
    if args.only:
        plans = [p for p in plans if p.db == args.only]
    log.info("Prepared %d index plans%s", len(plans), " (dry-run)" if args.dry_run else "")

    ok = skip = err = 0
    for p in plans:
        status, detail = _apply(client, p, dry_run=args.dry_run)
        if status == "OK":
            ok += 1
            log.info("  OK   %s", detail)
        elif status == "DRY":
            log.info("  DRY  %s  (%d keys, kind=%s)", detail, len(p.keys), p.kind)
        elif status == "SKIP":
            skip += 1
            log.info("  SKIP %s", detail)
        else:
            err += 1
            log.warning("  ERR  %s", detail)

    log.info("Summary: ok=%d skip=%d err=%d total=%d", ok, skip, err, len(plans))
    return 0 if err == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
