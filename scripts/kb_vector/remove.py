"""`python3 -m scripts.kb_vector remove --doc-id <id>` — surgical delete.

Removes every chunk in Milvus for a single document. Use for:
  • A doc the crawler hard-deleted (and you want immediate effect — don't
    wait for the 24h sweep).
  • Bad content that shouldn't be retrievable.

Destructive: requires --yes.
"""
from __future__ import annotations

import argparse
import sys

from pymilvus import MilvusClient

from backend.app.config import get_settings
from backend.app.services.kb_vector_ingest import delete_doc


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--doc-id", required=True,
                    help="canonical vector doc_id, e.g. alphapai:roadshows:<mongo_oid>")
    ap.add_argument("--yes", action="store_true", help="Required (destructive)")
    args = ap.parse_args(argv)

    if not args.yes:
        print("remove requires --yes (destructive)", file=sys.stderr)
        return 2

    s = get_settings()
    mv = MilvusClient(uri=f"http://{s.milvus_host}:{s.milvus_port}")
    n = delete_doc(mv, s.milvus_collection, args.doc_id)
    print(f"removed {n if n >= 0 else '?'} chunks for {args.doc_id}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
