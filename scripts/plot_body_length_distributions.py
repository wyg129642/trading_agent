#!/usr/bin/env python3
"""Plot body-length distribution per (db, collection) used by the local_ai_summary
runner, plus an aggregate "all data" distribution.

Reads the same TARGETS spec as crawl/local_ai_summary/runner.py so the per-target
body picking (first-non-empty of body_fields) matches what the LLM worker actually
sees. Output: one PNG with two figures stacked — per-target small-multiples
histograms, plus the aggregate.

Usage::

    PYTHONPATH=. python3 scripts/plot_body_length_distributions.py
    PYTHONPATH=. python3 scripts/plot_body_length_distributions.py \
        --sample 5000 --out /tmp/body_lengths.png
"""

from __future__ import annotations

import argparse
import math
import os
import sys
from pathlib import Path

# Make crawl.* importable when run from repo root or anywhere else.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import matplotlib

matplotlib.use("Agg")  # headless
import matplotlib.pyplot as plt
import numpy as np
from pymongo import MongoClient

from crawl.local_ai_summary.targets import TARGETS, Target


def _pick_nested(d: dict, dotted: str):
    cur = d
    for k in dotted.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
    return cur


# Universal superset of "body-shaped" fields across all 8 platforms + IR
# filings. We measure max(len) across whichever of these are present, so the
# histogram reflects the longest scraped/extracted text per doc — not whatever
# would win the LLM's first-non-empty priority pick. Catches cases where:
#   - jinmen.reports has both summary_md (always present, ~500-2000ch) and
#     pdf_text_md (longer when extracted) — we want the longer one.
#   - gangtise.summaries has content_md (sometimes brief-truncated to exactly
#     500 chars when content_truncated=True) AND essence_md (separate native
#     field). Picking max avoids the platform's 500-char paywall floor.
#   - alphapai.reports has list_item.contentCn (200-1000ch zh teaser) AND
#     pdf_text_md (full PDF, 5k-100kch). The teaser wins by priority but the
#     PDF is what's actually there.
WIDE_BODY_FIELDS: tuple[str, ...] = (
    # Generic full-body fields
    "content_md", "content",
    "list_item.contentCn", "list_item.content",
    # Platform "summary" / "preview" fields
    "summary_md", "summary_point_md",
    "brief_md", "description_md", "doc_introduce", "subtitle",
    # PDF-extracted text (always-present once cron has run)
    "pdf_text_md",
    # Meeting / interview shapes
    "transcript_md", "chapter_summary_md", "points_md",
    # Platform-specific extras
    "essence_md", "insight_md", "segments_md",
)


def _max_body_length(doc: dict, target: Target) -> int:
    """Return max(len) across all body-shaped fields present in this doc.

    Unlike runner._pick_body (which honours `body_fields` priority and stops
    at the first non-empty), we want the *largest* scraped/extracted text so
    the histogram reflects what was actually crawled, not what the LLM happens
    to pick. Walks the union of WIDE_BODY_FIELDS, target.body_fields, and
    target.native_summary_fields to be exhaustive.
    """
    candidates = set(WIDE_BODY_FIELDS) | set(target.body_fields) | set(target.native_summary_fields)
    best = 0
    for field in candidates:
        v = _pick_nested(doc, field)
        if isinstance(v, str):
            n = len(v.strip())
            if n > best:
                best = n
    return best


# Universal soft-delete gate. cleanup_low_value_b_class.py + chief_opinions
# cleanup + alphapai thin-clip / quota-stub cleanup all write `deleted=True`
# across many collections. We exclude these from the distribution so the
# histogram reflects only what the LLM worker / StockHub UI would actually
# see. Field is consistent across platforms (single greppable invariant —
# see backend/app/api/_mongo_filters.py and services/kb_service.py).
NOT_SOFT_DELETED = {"deleted": {"$ne": True}}


def collect_lengths(mc: MongoClient, target: Target, sample: int,
                    include_soft_deleted: bool = False) -> tuple[list[int], int]:
    """Return (max-body lengths, # soft-deleted in collection) for up to
    `sample` docs. If include_soft_deleted=False (default), filters out
    `deleted=True` docs to match UI/LLM/KB visibility."""
    try:
        db = mc[target.db]
        if target.collection not in db.list_collection_names():
            return [], 0
        col = db[target.collection]
    except Exception as e:
        print(f"  [{target.db}.{target.collection}] connect/list fail: {e}",
              file=sys.stderr)
        return [], 0

    # Project the union of body-shaped + native-summary fields so we can
    # measure max(len) across all of them. Using top-level field names so a
    # nested path like `list_item.contentCn` pulls the whole `list_item`
    # subdoc — fine, all fields under it are short enough that this isn't
    # a memory hazard.
    projection: dict[str, int] = {"_id": 0}
    for f in WIDE_BODY_FIELDS:
        projection[f.split(".")[0]] = 1
    for f in target.body_fields:
        projection[f.split(".")[0]] = 1
    for f in target.native_summary_fields:
        projection[f.split(".")[0]] = 1

    # Soft-delete count — `deleted` is unindexed across most collections,
    # so a full count_documents would table-scan on big DBs (jinmen-full.reports
    # at 100k+) and trip the connection timeout. Bound with maxTimeMS=5s and
    # treat failure as "unknown" rather than aborting the whole pass.
    try:
        deleted_total = col.count_documents({"deleted": True}, maxTimeMS=5000)
    except Exception:
        deleted_total = -1  # -1 == unknown / timed out

    lengths: list[int] = []
    base_filter = {} if include_soft_deleted else NOT_SOFT_DELETED
    try:
        # Sort by time descending so the sample is recent docs (matches the
        # 90d window the runner cares about). Without sort we'd get insert
        # order which on huge collections may be very stale.
        cur = (col.find(base_filter, projection)
                  .sort(target.time_ms_field, -1).limit(sample))
        for doc in cur:
            n = _max_body_length(doc, target)
            if n > 0:  # zero = empty body, drop from the histogram
                lengths.append(n)
    except Exception as e:
        print(f"  [{target.db}.{target.collection}] scan fail: {e}",
              file=sys.stderr)
    return lengths, deleted_total


def stats_line(lengths: list[int]) -> str:
    if not lengths:
        return "n=0"
    arr = np.array(lengths)
    return (f"n={len(arr)}  "
            f"p10={int(np.percentile(arr,10))}  "
            f"p50={int(np.percentile(arr,50))}  "
            f"p90={int(np.percentile(arr,90))}  "
            f"max={int(arr.max())}")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--mongo-uri", default="mongodb://127.0.0.1:27018")
    p.add_argument("--sample", type=int, default=5000,
                   help="max docs sampled per (db, collection) (default 5000)")
    p.add_argument("--out", type=Path,
                   default=Path("logs/body_length_distributions.png"),
                   help="output PNG path")
    p.add_argument("--bins", type=int, default=60)
    p.add_argument("--linear", action="store_true",
                   help="force linear x-axis (default: log scale, since the "
                        "corpus spans 5 orders of magnitude — 10ch comments to "
                        "450kch PDFs)")
    p.add_argument("--include-soft-deleted", action="store_true",
                   help="include docs with deleted=True in the histogram "
                        "(default: exclude — matches what UI/LLM/KB sees)")
    args = p.parse_args()

    args.out.parent.mkdir(parents=True, exist_ok=True)
    plt.rcParams["axes.unicode_minus"] = False

    # 60s socketTimeoutMS so a slow unindexed scan on one collection can't
    # kill the whole sweep. serverSelection stays tight (5s) so a wrong URI
    # fails fast.
    mc = MongoClient(
        args.mongo_uri,
        serverSelectionTimeoutMS=5000,
        socketTimeoutMS=60000,
    )

    # Collect once, then plot — avoids re-querying for the aggregate.
    per_target: list[tuple[Target, list[int], int]] = []
    mode_label = ("INCLUDED" if args.include_soft_deleted else "EXCLUDED")
    print(f"Sampling up to {args.sample} docs per target from {args.mongo_uri} "
          f"(soft-deleted {mode_label})…")
    for t in TARGETS:
        L, n_del = collect_lengths(mc, t, args.sample,
                                    include_soft_deleted=args.include_soft_deleted)
        per_target.append((t, L, n_del))
        print(f"  {t.label:32s} ({t.db}.{t.collection})  "
              f"{stats_line(L)}  soft_deleted={n_del}  "
              f"threshold={t.min_body_chars}")

    aggregate = [n for _, L, _ in per_target for n in L]
    total_deleted = sum(max(n, 0) for _, _, n in per_target)
    n_unknown = sum(1 for _, _, n in per_target if n < 0)
    unknown_note = f"; {n_unknown} count timed out" if n_unknown else ""
    print(f"\nAGGREGATE: {stats_line(aggregate)}  "
          f"(total soft-deleted excluded: {total_deleted}{unknown_note})")

    # ── plot layout ───────────────────────────────────────────────────────
    n_targets = len(per_target)
    ncols = 4
    nrows = math.ceil(n_targets / ncols)
    fig_h = nrows * 2.4 + 3.6  # leave room for the aggregate panel up top
    fig = plt.figure(figsize=(ncols * 4.0, fig_h))

    gs = fig.add_gridspec(
        nrows + 1, ncols,
        height_ratios=[1.6] + [1.0] * nrows,
        hspace=0.55, wspace=0.28,
    )

    # Find the global max so log-bin upper edge covers everything in the corpus.
    global_max = max((max(L) for _, L, _ in per_target if L), default=10000)
    use_log = not args.linear
    if use_log:
        # Log-spaced bins from 10 chars to global_max — covers the 5+ orders
        # of magnitude span (alphapai 11-char wechat captions to 450kch
        # gangtise PDFs) without compressing detail at either end.
        bins = np.logspace(1, np.log10(max(global_max, 100)), args.bins + 1)
    else:
        bins = np.linspace(0, global_max, args.bins + 1)

    def _arr(L):
        return np.array(L, dtype=float) if L else np.array([], dtype=float)

    # Aggregate panel (top, full width)
    ax_top = fig.add_subplot(gs[0, :])
    ax_top.hist(_arr(aggregate), bins=bins, color="#1f77b4",
                edgecolor="white", linewidth=0.4)
    ax_top.set_title(
        f"All collections aggregated  ·  {stats_line(aggregate)}",
        fontsize=12,
    )
    ax_top.set_xlabel("max body field length (chars)  —  union of content_md / "
                      "pdf_text_md / brief_md / summary_md / etc.  "
                      + ("[log scale]" if use_log else "[linear]"))
    ax_top.set_ylabel("docs")
    if use_log:
        ax_top.set_xscale("log")
    ax_top.axvline(60, color="gray", linestyle=":", linewidth=1,
                   label="default floor 60")
    ax_top.axvline(400, color="crimson", linestyle="--", linewidth=1,
                   label="gangtise floor 400")
    ax_top.legend(loc="upper right", fontsize=9)

    # Per-target small multiples
    for i, (t, L, n_del) in enumerate(per_target):
        r, c = i // ncols, i % ncols
        ax = fig.add_subplot(gs[r + 1, c])
        if L:
            ax.hist(_arr(L), bins=bins, color="#4c78a8",
                    edgecolor="white", linewidth=0.3)
            arr = np.array(L)
            p50 = int(np.percentile(arr, 50))
        else:
            ax.text(0.5, 0.5, "no data", ha="center", va="center",
                    transform=ax.transAxes, color="gray")
            p50 = 0
        if use_log:
            ax.set_xscale("log")
        ax.axvline(t.min_body_chars,
                   color="crimson", linestyle="--", linewidth=1)
        # Use ASCII db.collection (Target.label is Chinese — would render as
        # tofu boxes on hosts without a CJK font; we don't want to depend on
        # Noto-CJK being installed for this script to produce a readable plot).
        del_str = "?" if n_del < 0 else str(n_del)
        ax.set_title(
            f"{t.db}.{t.collection}\n"
            f"p50={p50}  n={len(L)}  floor={t.min_body_chars}  "
            f"soft_del={del_str}",
            fontsize=8,
        )
        ax.tick_params(axis="both", labelsize=7)
        if r == nrows - 1:
            ax.set_xlabel("chars (log)" if use_log else "chars", fontsize=8)
        if c == 0:
            ax.set_ylabel("docs", fontsize=8)

    sd_tag = ("with soft-deleted INCLUDED"
              if args.include_soft_deleted else "soft-deleted EXCLUDED")
    fig.suptitle(
        f"Crawled corpus  ·  max body field length distribution  "
        f"(union of content_md / pdf_text_md / brief_md / summary_md / ...) "
        f"·  {sd_tag}",
        fontsize=14, y=0.995,
    )
    fig.savefig(args.out, dpi=130, bbox_inches="tight")
    print(f"\nSaved → {args.out}  ({os.path.getsize(args.out)/1024:.0f} KB)")


if __name__ == "__main__":
    main()
