"""Ingest-time ticker stamping helper for all scrapers under ``crawl/``.

Called by every scraper immediately before ``col.replace_one({"_id": …}, doc, upsert=True)``
so new docs land with ``_canonical_tickers`` / ``_canonical_tickers_at`` /
``_unmatched_raw`` / ``_canonical_extract_source`` already set — no more waiting
for the hourly cron to catch up.

Design
------
- **Fail-open**: any import / extractor / normalizer exception is swallowed
  (the scraper's ingestion path must never break on a normalizer edge case).
  If stamping fails, the doc just goes in untagged and the enrich cron will
  pick it up on the next pass.
- **Idempotent**: same doc in, same four fields out. Re-running `stamp(doc, …)`
  is safe; callers don't need to guard.
- **Zero-alloc on no-op**: extractor returning `None` → writes empty-list fields
  (= "scanned, upstream has no ticker"), matching the enrich-script semantics.

Usage
-----
    from ticker_tag import stamp
    ...
    stamp(doc, "alphapai", col)           # mutates doc in place
    col.replace_one({"_id": dedup_id}, doc, upsert=True)

`col` may be either a pymongo Collection (we read `col.name`) or a plain str.
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# Repo root = crawl/.. ; needed so we can import from backend/app/services/.
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

try:
    from backend.app.services.ticker_normalizer import (  # noqa: E402
        EXTRACTORS,
        normalize_with_unmatched,
    )
    _AVAILABLE = True
except Exception:
    EXTRACTORS = {}  # type: ignore
    _AVAILABLE = False


def _coll_name(col: Any) -> str:
    """Accept pymongo Collection (has `.name`) or plain str."""
    if isinstance(col, str):
        return col
    name = getattr(col, "name", None)
    return name if isinstance(name, str) else ""


def stamp(doc: dict, source_key: str, col: Any) -> dict:
    """Add `_canonical_tickers` + siblings to `doc` in place. Returns `doc`.

    Never raises. On any failure the doc is returned unmodified (which means
    the enrich cron will re-process it later as if it hadn't been stamped).
    """
    if not _AVAILABLE or not isinstance(doc, dict):
        return doc
    extractor = EXTRACTORS.get(source_key)
    if extractor is None:
        return doc
    try:
        raw = extractor(doc, _coll_name(col))
        matched, unmatched = normalize_with_unmatched(raw)
        doc["_canonical_tickers"] = matched
        doc["_canonical_tickers_at"] = datetime.now(timezone.utc)
        doc["_unmatched_raw"] = unmatched
        doc["_canonical_extract_source"] = source_key
    except Exception:
        pass
    return doc


__all__ = ["stamp"]
