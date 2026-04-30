#!/usr/bin/env python3
"""Per-platform / per-collection coverage report after soft-delete cleanup.

For each (db, collection) registered in TARGETS:
  - total docs (raw)
  - soft-deleted (deleted=True)
  - visible (what UI / LLM / KB sees)
  - retention rate = visible / total
  - daily ingestion in the last 30d (visible vs soft-deleted), so you can
    spot collections where heavy soft-delete is recent (= ongoing quota
    issues) vs historical (= one-shot cleanup).

Outputs:
  - text table to stdout + logs/coverage_after_softdel.txt
  - logs/coverage_rates.png (bar chart, sorted by retention)
  - logs/coverage_daily_30d.png (per-collection daily ingestion stacked bars)

Usage::

    PYTHONPATH=. python3 scripts/plot_coverage_after_softdel.py
    PYTHONPATH=. python3 scripts/plot_coverage_after_softdel.py --days 60
"""

from __future__ import annotations

import argparse
import math
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from pymongo import MongoClient

from crawl.local_ai_summary.targets import TARGETS, Target


def _ms_now() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _ms_n_days_ago(d: int) -> int:
    return _ms_now() - d * 86400 * 1000


def collect_coverage(mc: MongoClient, target: Target, days: int) -> dict:
    """Return dict with total / deleted / visible counts and per-day buckets
    (last `days` days). Bounded with maxTimeMS so unindexed scans don't hang."""
    out = {
        "label": target.label,
        "key": f"{target.db}.{target.collection}",
        "total": -1, "deleted": -1, "visible": -1,
        "daily_visible": {}, "daily_deleted": {},
        "error": None,
    }
    try:
        db = mc[target.db]
        if target.collection not in db.list_collection_names():
            out["error"] = "collection-missing"
            return out
        col = db[target.collection]
    except Exception as e:
        out["error"] = f"connect-fail: {e}"
        return out

    # estimated_document_count is the only fast option on huge unindexed colls.
    try:
        out["total"] = col.estimated_document_count()
    except Exception as e:
        out["error"] = f"total-count-fail: {e}"

    # Soft-delete count — bounded by maxTimeMS, treat timeout as -1 (unknown).
    try:
        out["deleted"] = col.count_documents({"deleted": True}, maxTimeMS=10000)
    except Exception:
        out["deleted"] = -1

    if out["total"] >= 0 and out["deleted"] >= 0:
        out["visible"] = out["total"] - out["deleted"]
    elif out["total"] >= 0 and out["deleted"] == -1:
        out["visible"] = out["total"]  # best-guess

    # Daily ingestion last N days — single aggregation pass scoped to recent
    # window so it doesn't full-table-scan. release_time_ms IS indexed on most
    # platforms (sort key); even when it isn't this only touches recent docs.
    since_ms = _ms_n_days_ago(days)
    pipeline = [
        {"$match": {target.time_ms_field: {"$gte": since_ms}}},
        {"$group": {
            "_id": {
                "day": {"$dateToString": {
                    "format": "%Y-%m-%d",
                    "date": {"$toDate": f"${target.time_ms_field}"},
                }},
                "deleted": {"$ifNull": ["$deleted", False]},
            },
            "n": {"$sum": 1},
        }},
    ]
    try:
        for row in col.aggregate(pipeline, maxTimeMS=15000, allowDiskUse=True):
            day = row["_id"]["day"]
            n = row["n"]
            if row["_id"]["deleted"] is True:
                out["daily_deleted"][day] = out["daily_deleted"].get(day, 0) + n
            else:
                out["daily_visible"][day] = out["daily_visible"].get(day, 0) + n
    except Exception as e:
        # Aggregation failures shouldn't kill the whole report; keep counts.
        out["error"] = (out["error"] or "") + f" agg-fail: {e}"
    return out


def fmt_pct(num, denom) -> str:
    if denom is None or denom <= 0: return "  -  "
    return f"{num*100/denom:5.1f}%"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--mongo-uri", default="mongodb://127.0.0.1:27018")
    ap.add_argument("--days", type=int, default=30,
                    help="lookback window for daily ingestion plot (default 30)")
    ap.add_argument("--out-dir", type=Path,
                    default=Path("logs"))
    args = ap.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)
    plt.rcParams["axes.unicode_minus"] = False

    mc = MongoClient(args.mongo_uri,
                     serverSelectionTimeoutMS=5000, socketTimeoutMS=60000)
    rows: list[dict] = []
    print(f"Surveying {len(TARGETS)} targets at {args.mongo_uri} (days={args.days})…\n")
    for t in TARGETS:
        r = collect_coverage(mc, t, args.days)
        rows.append(r)
        if r["error"] and r["total"] < 0:
            print(f"  {r['key']:42s}  SKIPPED: {r['error']}")
            continue
        v_pct = fmt_pct(r["visible"], r["total"]) if r["visible"] is not None else "  -  "
        d_pct = fmt_pct(r["deleted"], r["total"]) if r["deleted"] >= 0 else "  ?  "
        print(f"  {r['key']:42s}  total={r['total']:7d}  "
              f"deleted={r['deleted']:7d} ({d_pct})  "
              f"visible={r['visible'] if r['visible'] is not None else '?':7}  "
              f"retention={v_pct}")

    # ── Text table ────────────────────────────────────────────────────────
    table_path = args.out_dir / "coverage_after_softdel.txt"
    with open(table_path, "w") as f:
        f.write(f"# Soft-delete coverage report\n")
        f.write(f"# generated {datetime.now().isoformat(timespec='seconds')} "
                f"(window: last {args.days}d)\n\n")
        f.write(f"{'collection':42s}  {'total':>8s}  {'deleted':>8s}  "
                f"{'del%':>6s}  {'visible':>8s}  {'keep%':>6s}\n")
        f.write("-" * 90 + "\n")
        # Sort by retention rate ascending (worst kept up top)
        ranked = sorted(
            rows,
            key=lambda r: (r["visible"]/r["total"]) if r.get("total",0) > 0 else 99,
        )
        for r in ranked:
            if r["total"] < 0: continue
            v = r["visible"] if r["visible"] is not None else r["total"]
            d = r["deleted"] if r["deleted"] >= 0 else 0
            ret = v / r["total"] if r["total"] else 0
            del_rate = d / r["total"] if r["total"] else 0
            f.write(f"{r['key']:42s}  {r['total']:8d}  {d:8d}  "
                    f"{del_rate*100:5.1f}%  {v:8d}  {ret*100:5.1f}%\n")
    print(f"\n→ wrote {table_path}")

    # ── Bar chart: retention rate per collection ──────────────────────────
    plotted = [r for r in rows if r["total"] > 0 and r["deleted"] >= 0]
    plotted.sort(key=lambda r: r["visible"] / r["total"])
    fig, ax = plt.subplots(figsize=(11, max(5, len(plotted) * 0.32)))
    labels = [r["key"] for r in plotted]
    visible_pct = [100 * r["visible"] / r["total"] for r in plotted]
    deleted_pct = [100 * r["deleted"] / r["total"] for r in plotted]
    y = np.arange(len(plotted))
    ax.barh(y, visible_pct, color="#2ca02c", label="visible")
    ax.barh(y, deleted_pct, left=visible_pct, color="#d62728", label="soft-deleted")
    for i, r in enumerate(plotted):
        ret = 100 * r["visible"] / r["total"]
        ax.text(101, i, f"  {ret:.1f}%  (total {r['total']:,})",
                va="center", fontsize=8)
    ax.set_yticks(y)
    ax.set_yticklabels(labels, fontsize=8)
    ax.set_xlim(0, 130)
    ax.set_xlabel("share of total (%)")
    ax.set_title(f"Soft-delete coverage  ·  retention rate per collection  "
                 f"(sorted worst-first)")
    ax.legend(loc="lower right")
    ax.grid(axis="x", alpha=0.3)
    bar_path = args.out_dir / "coverage_rates.png"
    fig.tight_layout()
    fig.savefig(bar_path, dpi=130, bbox_inches="tight")
    plt.close(fig)
    print(f"→ wrote {bar_path}")

    # ── Daily ingestion: stacked bars per collection ──────────────────────
    plotted2 = [r for r in rows if r.get("daily_visible") or r.get("daily_deleted")]
    if plotted2:
        ncols = 4
        nrows = math.ceil(len(plotted2) / ncols)
        fig, axes = plt.subplots(nrows, ncols, figsize=(ncols*4.5, nrows*2.4),
                                  squeeze=False)
        # Build common x-axis = last N days
        today = datetime.now(timezone.utc).date()
        days_axis = [
            (datetime.now(timezone.utc).date()
             - __import__("datetime").timedelta(days=d))
            for d in range(args.days - 1, -1, -1)
        ]
        x_labels = [d.strftime("%m-%d") for d in days_axis]
        x_keys = [d.isoformat() for d in days_axis]

        for i, r in enumerate(plotted2):
            ax = axes[i // ncols][i % ncols]
            v = [r["daily_visible"].get(k, 0) for k in x_keys]
            d = [r["daily_deleted"].get(k, 0) for k in x_keys]
            x = np.arange(len(x_keys))
            ax.bar(x, v, color="#2ca02c", label="visible")
            ax.bar(x, d, bottom=v, color="#d62728", label="soft-del")
            ax.set_title(
                f"{r['key']}  ·  {sum(v)}/{sum(v)+sum(d)} kept "
                f"(last {args.days}d)",
                fontsize=8,
            )
            # Tick every ~7 days to avoid crowding
            step = max(1, len(x_keys) // 6)
            ax.set_xticks(x[::step])
            ax.set_xticklabels(x_labels[::step], fontsize=6, rotation=45)
            ax.tick_params(axis="y", labelsize=7)

        # Hide empty axes
        for j in range(len(plotted2), nrows * ncols):
            axes[j // ncols][j % ncols].axis("off")

        fig.suptitle(f"Daily ingestion (last {args.days}d) · "
                     f"visible vs soft-deleted",
                     fontsize=13, y=0.995)
        fig.tight_layout(rect=(0, 0, 1, 0.97))
        daily_path = args.out_dir / "coverage_daily_30d.png"
        fig.savefig(daily_path, dpi=130, bbox_inches="tight")
        plt.close(fig)
        print(f"→ wrote {daily_path}")


if __name__ == "__main__":
    main()
