"""`python3 -m scripts.kb_vector sweep --coll <db>/<c>` — reconcile deletes.

Diffs Milvus `doc_id` set vs Mongo `_id` set for one (db, collection) and
deletes Milvus chunks whose source Mongo doc no longer exists. Meant to run
daily ~03:00 as part of the sync loop; can also be run on-demand by ops.

Read-only modes:
    --dry-run       report tombstones without deleting
    --coll all      sweep every collection in SPECS_LIST

Writes an audit entry to ``admin.vector_tombstones`` per sweep.
"""
from __future__ import annotations

import argparse
import json
import sys

from backend.app.services.kb_service import SPECS_BY_KEY, SPECS_LIST
from backend.app.services.kb_vector_ingest import sweep_deleted_docs


async def _run_all(dry_run: bool) -> list[dict]:
    import asyncio as _a
    results = []
    for spec in SPECS_LIST:
        r = await sweep_deleted_docs(spec, dry_run=dry_run)
        r["collection"] = f"{spec.db}/{spec.collection}"
        results.append(r)
    return results


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--coll", required=True,
                    help="e.g. alphapai/roadshows, or 'all'")
    ap.add_argument("--dry-run", action="store_true",
                    help="Report tombstones without deleting")
    ap.add_argument("--yes", action="store_true",
                    help="Required for non-dry-run destructive delete")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args(argv)

    if not args.dry_run and not args.yes:
        print("sweep requires --yes or --dry-run (delete is destructive)", file=sys.stderr)
        return 2

    import asyncio
    if args.coll == "all":
        results = asyncio.run(_run_all(args.dry_run))
    else:
        if args.coll not in SPECS_BY_KEY:
            print(f"unknown collection '{args.coll}'", file=sys.stderr)
            return 2
        spec = SPECS_BY_KEY[args.coll]
        r = asyncio.run(sweep_deleted_docs(spec, dry_run=args.dry_run))
        r["collection"] = args.coll
        results = [r]

    if args.json:
        print(json.dumps(results, indent=2, ensure_ascii=False))
        return 0

    print()
    print("== sweep result ==")
    for r in results:
        coll = r.get("collection", "?")
        if r.get("skipped_due_to_lease"):
            print(f"  {coll}: SKIPPED (lease held)")
            continue
        if "error" in r:
            print(f"  {coll}: ERROR — {r['error']}")
            continue
        marker = "(dry-run)" if r.get("dry_run") else ""
        print(f"  {coll}: mongo={r.get('mongo_docs',0):>6}  "
              f"milvus={r.get('milvus_docs',0):>6}  "
              f"tombstones={r.get('tombstones',0):>5}  "
              f"deleted={r.get('deleted',0):>5}  "
              f"elapsed={r.get('elapsed_s',0):>3}s {marker}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
