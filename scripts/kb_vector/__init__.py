"""Operator CLI for the vector retrieval stack.

Usage::

    python3 -m scripts.kb_vector <subcommand> [options]

Subcommands:

    status          Global overview + per-collection counts
    verify          Diff Mongo IDs ↔ Milvus doc_ids
    # later phases:
    # reindex, remove, backfill, snapshot, dead

All subcommands read from both Mongo (local, port 27017) and Milvus (local,
port 19530). Never talks to TEI — pure inspection / diff.
"""
# Clash at 127.0.0.1:7890 intercepts localhost gRPC otherwise.
import os
os.environ.setdefault("no_proxy", "127.0.0.1,localhost,jumpbox,116.239.28.36,192.168.31.0/24")
os.environ.setdefault("NO_PROXY", "127.0.0.1,localhost,jumpbox,116.239.28.36,192.168.31.0/24")

import argparse
import sys
from importlib import import_module


SUBCOMMANDS = {
    "status": "scripts.kb_vector.status",
    "verify": "scripts.kb_vector.verify",
    "sweep":  "scripts.kb_vector.sweep",
    "remove": "scripts.kb_vector.remove",
}


def _print_usage() -> None:
    print(__doc__.strip(), file=sys.stderr)


def main(argv: list[str] | None = None) -> int:
    argv = argv if argv is not None else sys.argv[1:]
    if not argv or argv[0] in ("-h", "--help"):
        _print_usage()
        return 0 if argv and argv[0] in ("-h", "--help") else 1

    sub = argv[0]
    rest = argv[1:]
    if sub not in SUBCOMMANDS:
        print(f"unknown subcommand '{sub}'", file=sys.stderr)
        _print_usage()
        return 2

    mod = import_module(SUBCOMMANDS[sub])
    return mod.main(rest)


if __name__ == "__main__":
    sys.exit(main())
