"""Daily coverage reconciliation for the 8 crawler platforms.

Complementary to crawl/daily_catchup.sh:
  - daily_catchup.sh   → spawns catch-up scrapers (action arm)
  - reconcile_crawlers → measures + audits + alerts (measurement arm)

Typical cron layout (Asia/Shanghai):
  05:00 — reconcile_crawlers.py --measure   (writes daily_<date> snapshots)
  05:30 — daily_catchup.sh                  (closes gaps found)
  07:00 — reconcile_crawlers.py --audit     (re-reads snapshots, feishu alert if gaps persist)

Invocation:
  python3 scripts/reconcile_crawlers.py --date 2026-04-22 --measure
  python3 scripts/reconcile_crawlers.py --date 2026-04-22 --audit --feishu-webhook $URL
  python3 scripts/reconcile_crawlers.py --date 2026-04-22 --trigger-backfill

Design notes:
  1. Each platform's scraper writes _state.daily_<date> via its own `--today --date` CLI
     (see crawl/*/scraper.py::count_today). We reuse that mechanism instead of
     reimplementing per-platform API counting.
  2. Measurement re-runs are safe (idempotent) but DO cost list-page requests.
     Do NOT run while watchers are actively backfilling the same category.
  3. No edits to anything under crawl/ — this file is strictly orchestration.
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

# Proxy bypass — Clash on 7890 blocks localhost mongo and LAN; mirrors memory `infra_proxy`.
for _k in ("http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY"):
    os.environ.pop(_k, None)
os.environ["NO_PROXY"] = (
    os.environ.get("NO_PROXY", "") + ",127.0.0.1,localhost,192.168.31.0/24"
).strip(",")

try:
    from pymongo import MongoClient
except ImportError:
    print("pymongo not installed. run: pip install pymongo", file=sys.stderr)
    sys.exit(2)

REPO_ROOT = Path(__file__).resolve().parent.parent
PY_BIN = "/home/ygwang/miniconda3/envs/agent/bin/python"
MONGO_URI = os.environ.get("CRAWLER_MONGO_URI", "mongodb://localhost:27017")
AUDIT_DB = os.environ.get("CRAWLER_RECONCILE_DB", "crawler_reconcile")
AUDIT_COLL = "audits"

# Each row binds a scraper CLI invocation to the _state.daily_<date> key it writes.
# The `daily_key` is the nested field inside `_state.daily_<date>` (see
# alphapai_crawl/scraper.py::count_today, jinmen/scraper.py::count_today, etc).
#
# Platforms where --today is not implemented (or not meaningful) are excluded:
#   sentimentrader — daily cron produces 1 indicator per session; no gap notion.
@dataclasses.dataclass(frozen=True)
class Row:
    platform: str
    db_name: str
    scraper_dir: str          # relative to crawl/
    today_args: list[str]     # CLI args passed to scraper.py --today --date <date> <args>
    daily_key: str            # key inside _state.daily_<date>
    catchup_filter: str       # value passed to daily_catchup.sh <platform>

ROWS: list[Row] = [
    # alphapai (4 categories — each writes its own key into daily_<date>)
    Row("alphapai",   "alphapai",   "alphapai_crawl", ["--category", "roadshow"],           "roadshow", "alphapai"),
    Row("alphapai",   "alphapai",   "alphapai_crawl", ["--category", "comment"],            "comment",  "alphapai"),
    Row("alphapai",   "alphapai",   "alphapai_crawl", ["--category", "report"],             "report",   "alphapai"),
    Row("alphapai",   "alphapai",   "alphapai_crawl", ["--category", "wechat"],             "wechat",   "alphapai"),
    # jinmen (3 categories)
    Row("jinmen",     "jinmen",     "jinmen",         [],                                    "meetings", "jinmen"),
    Row("jinmen",     "jinmen",     "jinmen",         ["--reports"],                         "reports",  "jinmen"),
    Row("jinmen",     "jinmen",     "jinmen",         ["--oversea-reports"],                 "oversea_reports", "jinmen"),
    # meritco (2 sub-types share one --today run)
    Row("meritco",    "meritco",    "meritco_crawl",  ["--type", "2,3"],                     "forum",    "meritco"),
    # third_bridge
    Row("thirdbridge","thirdbridge","third_bridge",   [],                                    "interviews","thirdbridge"),
    # funda (3 categories)
    Row("funda",      "funda",      "funda",          ["--category", "post"],                "post",              "funda"),
    Row("funda",      "funda",      "funda",          ["--category", "earnings_report"],     "earnings_report",   "funda"),
    Row("funda",      "funda",      "funda",          ["--category", "earnings_transcript"], "earnings_transcript","funda"),
    # gangtise (3 types)
    Row("gangtise",   "gangtise",   "gangtise",       ["--type", "summary",  "--skip-pdf"], "summary",  "gangtise"),
    Row("gangtise",   "gangtise",   "gangtise",       ["--type", "research", "--skip-pdf"], "research", "gangtise"),
    Row("gangtise",   "gangtise",   "gangtise",       ["--type", "chief",    "--skip-pdf"], "chief",    "gangtise"),
    # acecamp (3 types)
    Row("acecamp",    "acecamp",    "AceCamp",        ["--type", "articles"],                "articles", "acecamp"),
    Row("acecamp",    "acecamp",    "AceCamp",        ["--type", "events"],                  "events",   "acecamp"),
    Row("acecamp",    "acecamp",    "AceCamp",        ["--type", "opinions"],                "opinions", "acecamp"),
    # alphaengine (1 aggregate)
    Row("alphaengine","alphaengine","alphaengine",    ["--category", "all"],                 "all",      "alphaengine"),
]

# Severity buckets — tuned for typical daily volumes (see 2026-04-23 measurement).
# gap 0 = clean · 1-5 = minor · 6-20 = notable · 21+ = severe
SEVERITY_MINOR = 1
SEVERITY_NOTABLE = 6
SEVERITY_SEVERE = 21


def yesterday_cst() -> str:
    cst = dt.timezone(dt.timedelta(hours=8))
    return (dt.datetime.now(tz=cst) - dt.timedelta(days=1)).strftime("%Y-%m-%d")


def read_snapshot(client: MongoClient, db_name: str, date: str, daily_key: str) -> dict[str, Any] | None:
    """Return {platform_count, in_db, missing, scanned_pages} or None if snapshot absent."""
    state = client[db_name]["_state"].find_one({"_id": f"daily_{date}"})
    if not state:
        return None
    entry = state.get(daily_key)
    if not isinstance(entry, dict):
        return None
    return {
        "platform_count": int(entry.get("platform_count", 0)),
        "in_db": int(entry.get("in_db", 0)),
        "missing": int(entry.get("missing", entry.get("platform_count", 0) - entry.get("in_db", 0))),
        "scanned_pages": int(entry.get("scanned_pages", 0)),
        "scanned_at": state.get("scanned_at"),
    }


def classify(missing: int) -> str:
    if missing <= 0:
        return "clean"
    if missing < SEVERITY_NOTABLE:
        return "minor"
    if missing < SEVERITY_SEVERE:
        return "notable"
    return "severe"


def run_today_scan(row: Row, date: str, timeout_sec: int, log_dir: Path, dry_run: bool) -> tuple[int, str]:
    """Invoke `scraper.py --today --date <date> <row.today_args>`. Returns (returncode, log_path)."""
    log_path = log_dir / f"{row.platform}_{row.daily_key}_{date}.log"
    cwd = REPO_ROOT / "crawl" / row.scraper_dir
    cmd = [PY_BIN, "scraper.py", "--today", "--date", date, *row.today_args]
    print(f"  → [{row.platform}:{row.daily_key}] {' '.join(cmd)}  (log: {log_path})")
    if dry_run:
        return 0, "(dry-run)"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with open(log_path, "w") as lf:
        try:
            res = subprocess.run(cmd, cwd=cwd, stdout=lf, stderr=subprocess.STDOUT, timeout=timeout_sec)
            return res.returncode, str(log_path)
        except subprocess.TimeoutExpired:
            lf.write(f"\n[TIMEOUT after {timeout_sec}s]\n")
            return 124, str(log_path)


def trigger_catchup(platform_key: str, log_dir: Path, dry_run: bool) -> tuple[int, str]:
    """Invoke crawl/daily_catchup.sh <platform>. Blocking (it wait's on all its children)."""
    script = REPO_ROOT / "crawl" / "daily_catchup.sh"
    if not script.exists():
        return 127, "(daily_catchup.sh not present)"
    log_path = log_dir / f"catchup_{platform_key}.log"
    cmd = ["bash", str(script), platform_key]
    print(f"  → daily_catchup.sh {platform_key}  (log: {log_path})")
    if dry_run:
        return 0, "(dry-run)"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with open(log_path, "w") as lf:
        res = subprocess.run(cmd, cwd=REPO_ROOT, stdout=lf, stderr=subprocess.STDOUT)
    return res.returncode, str(log_path)


def post_feishu(webhook: str, title: str, body: str) -> None:
    """Fire-and-forget feishu markdown post."""
    import urllib.request
    payload = json.dumps({
        "msg_type": "interactive",
        "card": {
            "config": {"wide_screen_mode": True},
            "header": {"title": {"tag": "plain_text", "content": title}, "template": "red"},
            "elements": [{"tag": "markdown", "content": body}],
        },
    }).encode()
    req = urllib.request.Request(webhook, data=payload, headers={"Content-Type": "application/json"})
    try:
        urllib.request.urlopen(req, timeout=10).read()
    except Exception as e:
        print(f"  ! feishu post failed: {e}", file=sys.stderr)


def format_report(date: str, rows: list[dict[str, Any]]) -> str:
    """Markdown report table for CLI + feishu."""
    lines = [f"**爬虫每日对账 — {date} (CST)**", ""]
    header = "| 平台 | 分类 | 平台总数 | DB已入 | 缺口 | 严重度 |"
    sep    = "|---|---|---|---|---|---|"
    lines += [header, sep]
    total_missing = 0
    for r in rows:
        sev = r["severity"]
        icon = {"clean": "✓", "minor": "·", "notable": "!", "severe": "✗", "no_snapshot": "?"}[sev]
        plat = r["platform_count"] if r["platform_count"] is not None else "-"
        indb = r["in_db"] if r["in_db"] is not None else "-"
        miss = r["missing"] if r["missing"] is not None else "?"
        if isinstance(r["missing"], int):
            total_missing += r["missing"]
        lines.append(f"| {r['platform']} | {r['daily_key']} | {plat} | {indb} | {miss} | {icon} {sev} |")
    lines += ["", f"**合计缺口:** {total_missing} · **快照缺失行数:** {sum(1 for r in rows if r['severity']=='no_snapshot')}"]
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Daily crawler coverage reconciliation.")
    parser.add_argument("--date", default=yesterday_cst(), help="Target date (YYYY-MM-DD, CST). Default: yesterday.")
    parser.add_argument("--measure", action="store_true",
                        help="Run --today --date for rows without a snapshot (mutates _state).")
    parser.add_argument("--audit", action="store_true",
                        help="Read existing snapshots + write audit doc + optional feishu.")
    parser.add_argument("--trigger-backfill", action="store_true",
                        help="After audit, invoke daily_catchup.sh per-platform for any non-clean platform.")
    parser.add_argument("--platforms", default=None,
                        help="Comma-separated platform filter (e.g. alphapai,jinmen). Default: all.")
    parser.add_argument("--timeout", type=int, default=600,
                        help="Per-scraper --today timeout in seconds (default 600).")
    parser.add_argument("--feishu-webhook", default=os.environ.get("FEISHU_CRAWLER_WEBHOOK", ""),
                        help="Feishu custom-bot webhook. If set, alerts on severe gaps.")
    parser.add_argument("--dry-run", action="store_true", help="Print planned actions, don't execute.")
    parser.add_argument("--log-dir", default=str(REPO_ROOT / "logs" / "reconcile"),
                        help="Where to write per-row stdout logs.")
    args = parser.parse_args()

    if not (args.measure or args.audit or args.trigger_backfill):
        # Default behavior: read-only audit (safe).
        args.audit = True

    platform_filter = set(p.strip() for p in args.platforms.split(",")) if args.platforms else None
    rows_to_run = [r for r in ROWS if platform_filter is None or r.platform in platform_filter]

    log_dir = Path(args.log_dir) / args.date
    mongo = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000, tz_aware=True)

    # PHASE 1 — measure (fill missing snapshots)
    #
    # NOTE on destructive write: each scraper's count_today() writes the whole
    # daily_<date> doc rather than $set-ing a single key, so a per-category run
    # wipes sibling snapshots. Defense: snapshot-and-restore siblings around
    # every --today invocation. Detected 2026-04-23 on alphapai (see §4 note).
    if args.measure:
        print(f"[{time.strftime('%H:%M:%S')}] PHASE 1 — measure missing snapshots for {args.date}")
        missing_rows = [r for r in rows_to_run if read_snapshot(mongo, r.db_name, args.date, r.daily_key) is None]
        if not missing_rows:
            print(f"  all {len(rows_to_run)} rows already have snapshots — skipping.")
        for r in missing_rows:
            pre_doc = mongo[r.db_name]["_state"].find_one({"_id": f"daily_{args.date}"}) or {}
            pre_keys = {k: v for k, v in pre_doc.items() if k not in ("_id", "date", "scanned_at") and isinstance(v, dict)}
            run_today_scan(r, args.date, args.timeout, log_dir, args.dry_run)
            if args.dry_run:
                continue
            post_doc = mongo[r.db_name]["_state"].find_one({"_id": f"daily_{args.date}"}) or {}
            to_restore = {k: v for k, v in pre_keys.items() if k != r.daily_key and k not in post_doc}
            if to_restore:
                mongo[r.db_name]["_state"].update_one({"_id": f"daily_{args.date}"}, {"$set": to_restore})
                print(f"    [merge-restore] recovered siblings: {sorted(to_restore)}")

    # PHASE 2 — audit (read all snapshots, build report)
    audit_rows: list[dict[str, Any]] = []
    for r in rows_to_run:
        snap = read_snapshot(mongo, r.db_name, args.date, r.daily_key)
        if snap is None:
            audit_rows.append({
                "platform": r.platform, "daily_key": r.daily_key,
                "platform_count": None, "in_db": None, "missing": None,
                "severity": "no_snapshot",
            })
        else:
            audit_rows.append({
                "platform": r.platform, "daily_key": r.daily_key,
                **snap,
                "severity": classify(snap["missing"]),
            })

    report_md = format_report(args.date, audit_rows)
    print("\n" + report_md + "\n")

    if args.audit or args.trigger_backfill:
        audit_doc = {
            "_id": f"{args.date}_{int(time.time())}",
            "date": args.date,
            "run_at": dt.datetime.now(tz=dt.timezone.utc),
            "rows": audit_rows,
            "total_missing": sum(r["missing"] for r in audit_rows if isinstance(r.get("missing"), int)),
            "measure_invoked": args.measure,
            "trigger_invoked": False,
        }

        # PHASE 3 — trigger catch-up for dirty platforms
        if args.trigger_backfill:
            dirty_platforms = sorted({
                r["platform"] for r in audit_rows
                if r["severity"] in ("notable", "severe")
            })
            audit_doc["trigger_invoked"] = bool(dirty_platforms)
            audit_doc["triggered_platforms"] = dirty_platforms
            for p in dirty_platforms:
                # catchup_filter == platform_key for daily_catchup.sh
                platform_key = next((r.catchup_filter for r in ROWS if r.platform == p), p)
                print(f"[{time.strftime('%H:%M:%S')}] PHASE 3 — catch-up for {platform_key}")
                trigger_catchup(platform_key, log_dir, args.dry_run)

        if not args.dry_run:
            try:
                mongo[AUDIT_DB][AUDIT_COLL].insert_one(audit_doc)
                print(f"  ✓ audit row written to {AUDIT_DB}.{AUDIT_COLL} (_id={audit_doc['_id']})")
            except Exception as e:
                print(f"  ! failed to write audit row: {e}", file=sys.stderr)

        # PHASE 4 — feishu alert (only if any notable/severe gap)
        if args.feishu_webhook and any(r["severity"] in ("notable", "severe") for r in audit_rows):
            severe_count = sum(1 for r in audit_rows if r["severity"] == "severe")
            title = f"爬虫对账 — {args.date} · {severe_count} severe · 总缺口 {audit_doc['total_missing']}"
            if not args.dry_run:
                post_feishu(args.feishu_webhook, title, report_md)

    mongo.close()
    # Exit code: 0 clean · 1 any gap · 2 any severe
    if any(r["severity"] == "severe" for r in audit_rows):
        return 2
    if any(r["severity"] in ("minor", "notable", "no_snapshot") for r in audit_rows):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
