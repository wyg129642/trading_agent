#!/usr/bin/env python3
"""Date-sweep backfill for platforms whose list APIs honor startDate/endDate.

Why this exists
---------------
AlphaPai's `wechat` / `comment` / `roadshow` list endpoints cap depth at ~10-
15k items (page 101 for wechat, page 293 for roadshow) — you simply cannot
paginate back 6 months. Their `report` endpoint is different: it honors
`startDate/endDate` body fields, so we can walk day by day.

Similarly, gangtise's `research` endpoint has `startDate/endDate` in its ES-
style body (they're nulls by default). That enables per-day sweeps too.

Other platforms (jinmen meetings/reports, meritco, acecamp, funda) either
don't expose a date filter or their pagination is unbounded — the existing
`backfill_6months.py` streaming orchestrator handles those fine.

How it works
------------
Loop date = cutoff → yesterday (step 1 day):
  For each (platform, category) in TARGETS:
    If DB already has "enough" docs for that date, skip.
    Otherwise spawn `scraper.py --category <cat> --sweep-today --date <day>
    --skip-pdf` and let the scraper's dedup short-circuit handle the
    already-ingested subset within the day.

Checkpoint
----------
State file `logs/backfill_6months/by_date_state.json`:
  {"<platform>/<category>/<YYYY-MM-DD>": {"status": "done"|"running"|"skipped",
                                            "added": N, "at": iso}}
Rerunning the script picks up from the last incomplete (platform, cat, day).

Usage
-----
    nohup python3 -u crawl/backfill_by_date.py \
        --from 2025-10-23 --to yesterday \
        > logs/backfill_6months/by_date.out 2>&1 &
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

for _k in ("http_proxy", "https_proxy", "all_proxy", "no_proxy",
           "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY"):
    os.environ.pop(_k, None)

from pymongo import MongoClient  # noqa: E402

# Backfill antibot integration — orchestrator level: enforce work-hours window
# + per-platform single-instance lock + soft-cooldown wait + bg-budget gate
sys.path.insert(0, str(Path(__file__).resolve().parent))
from antibot import (  # noqa: E402
    BackfillWindow, BackfillLock, SoftCooldown, AccountBudget,
    _DEFAULT_ACCOUNT_BUDGET,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
LOG_DIR = REPO_ROOT / "logs" / "backfill_6months"
STATE_FILE = LOG_DIR / "by_date_state.json"
CST = timezone(timedelta(hours=8))

# 2026-04-23 migration: data is on remote 192.168.31.176:35002. Inherit URI
# from env (crawler_monitor + .env already set MONGO_URI) with remote fallback.
MONGO_URI = os.environ.get(
    "MONGO_URI",
    "mongodb://u_spider:prod_X5BKVbAc@192.168.31.176:35002/?authSource=admin",
)

# Remote DB names differ from the old local short names. Target.mongo_db keeps
# old aliases ("alphapai" / "gangtise" etc); translate at Mongo-query time.
_REMOTE_DB_ALIASES: dict[str, str] = {
    "alphapai":    "alphapai-full",
    "jinmen":      "jinmen-full",
    "meritco":     "jiuqian-full",
    "thirdbridge": "third-bridge",
    "gangtise":    "gangtise-full",
}


def _resolve_db(name: str) -> str:
    return _REMOTE_DB_ALIASES.get(name, name)


@dataclass
class Target:
    platform: str
    category: str
    cwd: str
    mongo_db: str
    mongo_coll: str
    extra_args: list

    @property
    def key(self) -> str:
        return f"{self.platform}/{self.category}"


TARGETS: list[Target] = [
    # 反爬 (2026-04-24): 改用 backfill 安全档参数 — base/jitter 4s/2.5s, burst=30,
    # daily_cap 400, 不再 --burst-size 0 --daily-cap 0 (那是事故隐患). 实际节奏
    # 比之前慢 2-3x, 但单日少漏几十条 → 多花 1 天补完一个月历史不亏.
    # alphapai report: list API 接受 startDate/endDate body 字段 (2026-04-23 实测确认).
    # 保留 PDF 下载 (不加 --skip-pdf).
    Target(
        platform="alphapai", category="report",
        cwd="crawl/alphapai_crawl",
        mongo_db="alphapai", mongo_coll="reports",
        extra_args=["--category", "report", "--sweep-today",
                    "--page-size", "100",
                    "--throttle-base", "4.0", "--throttle-jitter", "2.5",
                    "--burst-size", "30", "--daily-cap", "400"],
    ),
    # gangtise research: ES-body startDate/endDate 接受 java.lang.Long (毫秒 epoch).
    # 2026-04-23 新加 --sweep-today 支持. 单日 ~1000-2500 篇 (内资 233 + 外资 772).
    # page-size 100 翻 25 页够了. 不下 PDF (researches 本来就不带 PDF).
    Target(
        platform="gangtise", category="research",
        cwd="crawl/gangtise",
        mongo_db="gangtise", mongo_coll="researches",
        extra_args=["--type", "research", "--sweep-today",
                    "--page-size", "100",
                    "--throttle-base", "4.0", "--throttle-jitter", "2.5",
                    "--burst-size", "30", "--daily-cap", "400"],
    ),
    # funda earnings_report (8-K): tRPC dateFilter=custom + customDate=ISO Date.
    # 2026-04-23 新加 --sweep-today 支持. 单日量 ~几十条 (美股 8-K, 财报季集中).
    Target(
        platform="funda", category="earnings_report",
        cwd="crawl/funda",
        mongo_db="funda", mongo_coll="earnings_reports",
        extra_args=["--category", "earnings_report", "--sweep-today",
                    "--page-size", "50",
                    "--throttle-base", "4.0", "--throttle-jitter", "2.5",
                    "--burst-size", "30", "--daily-cap", "400"],
    ),
    # funda earnings_transcript (财报电话会逐字稿): 同 earnings_report 的 schema.
    Target(
        platform="funda", category="earnings_transcript",
        cwd="crawl/funda",
        mongo_db="funda", mongo_coll="earnings_transcripts",
        extra_args=["--category", "earnings_transcript", "--sweep-today",
                    "--page-size", "50",
                    "--throttle-base", "4.0", "--throttle-jitter", "2.5",
                    "--burst-size", "30", "--daily-cap", "400"],
    ),
]


def probe_count_for_day(mc: MongoClient, t: Target, day_str: str) -> int:
    """Count docs whose release_time_ms is within [day 00:00, day+1 00:00) UTC."""
    day = datetime.strptime(day_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    start_ms = int(day.timestamp() * 1000)
    end_ms = int((day + timedelta(days=1)).timestamp() * 1000)
    return mc[_resolve_db(t.mongo_db)][t.mongo_coll].count_documents({
        "release_time_ms": {"$gte": start_ms, "$lt": end_ms},
    })


def load_state() -> dict:
    if not STATE_FILE.exists():
        return {}
    try:
        return json.loads(STATE_FILE.read_text())
    except Exception:
        return {}


def save_state(state: dict) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2, ensure_ascii=False))


_stop = False


def _sig(*_):
    global _stop
    _stop = True
    logging.info("signal received; will stop after current day")


signal.signal(signal.SIGINT, _sig)
signal.signal(signal.SIGTERM, _sig)


def run_one_day(t: Target, day_str: str, timeout_sec: int) -> tuple[int, int]:
    """Spawn scraper for (target, day). Returns (rc, added). Log-to-file.

    反爬保护: 每天开跑前检查 backfill window + soft cooldown + bg budget.
    Window 关闭就 sleep 到打开; soft cooldown 触发就等到清; bg budget 满
    (rt sibling >= floor 70%) 就跳过这天 (返回特殊 rc=126 标 "暂时跳过").
    """
    # 1. Backfill window — CN 平台工作日 22:00-08:00 + 周末. 卡住就 sleep.
    if BackfillWindow.seconds_until_allowed(t.platform) > 0:
        BackfillWindow.wait_until_allowed(t.platform)

    # 2. Soft cooldown — 同平台任一 watcher 触发软警告就 wait
    if SoftCooldown.remaining(t.platform) > 0:
        SoftCooldown.wait_if_active(t.platform, verbose=True)

    # 3. bg budget gate — realtime 主桶用量 >= 70% 时 backfill 让位
    rt_limit = _DEFAULT_ACCOUNT_BUDGET.get(t.platform, 0)
    if rt_limit:
        # account_id 未知, 用 platform + "any" 估算 — 真正的 budget 由子进程
        # 内部 AccountBudget 处理. 这里只是 orchestrator 级早退提示.
        # 简化: 如果检测到任意账号 rt sibling 超 floor 就跳过该天.
        try:
            from antibot import _get_redis as _gr
            r = _gr()
            if r is not None:
                # 扫该平台所有 rt 桶, 找到任一 >= floor 就 skip
                for key in r.scan_iter(f"crawl:budget:{t.platform}:*", count=20):
                    if ":bg" in key or ":pdf" in key:
                        continue
                    used = r.zcard(key)
                    if used >= rt_limit * 0.7:
                        logging.info(f"[{t.key}/{day_str}] rt budget {key} used "
                                     f"{used}/{rt_limit} >= floor 70%, 让位 realtime")
                        return 126, 0
        except Exception:
            pass

    log_path = LOG_DIR / f"by_date_{t.platform}_{t.category}_{day_str}.log"
    # 注入 --account-role bg 让子 scraper 走后台桶 (不抢 realtime 主桶)
    cmd = (["python3", "-u", "scraper.py"] + t.extra_args
           + ["--date", day_str, "--account-role", "bg"])
    env = os.environ.copy()
    for k in ("http_proxy", "https_proxy", "all_proxy", "no_proxy",
              "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY"):
        env.pop(k, None)
    # Tell antibot which bf-lock-role this subprocess uses (so it claims a
    # distinct lock, not stomping on watcher's main lock).
    env["CRAWLER_PROCESS_LABEL"] = f"{t.platform}|backfill_by_date|{t.category}|{day_str}"
    logging.info(f"[{t.key}/{day_str}] START {' '.join(cmd)}")
    try:
        with log_path.open("wb") as f:
            f.write(f"\n===== {datetime.now(CST).isoformat()} {t.key}/{day_str} =====\n".encode())
            proc = subprocess.run(
                cmd, cwd=str(REPO_ROOT / t.cwd),
                stdout=f, stderr=subprocess.STDOUT, env=env,
                timeout=timeout_sec,
            )
        rc = proc.returncode
    except subprocess.TimeoutExpired:
        logging.warning(f"[{t.key}/{day_str}] timeout after {timeout_sec}s; skipping")
        return 124, 0
    except Exception as e:
        logging.error(f"[{t.key}/{day_str}] run error: {e}")
        return -1, 0
    # parse added count from log (best-effort)
    added = 0
    try:
        tail = log_path.read_text(errors="replace").splitlines()[-40:]
        for line in tail:
            if "本轮汇总:" in line:
                # e.g. "本轮汇总: report+15/=30/✗0"
                import re
                m = re.search(r"\+(\d+)/", line)
                if m:
                    added = int(m.group(1))
                    break
    except Exception:
        pass
    logging.info(f"[{t.key}/{day_str}] END rc={rc} added={added}")
    return rc, added


def probe_oldest_date_for_target(mc: MongoClient, t: Target):
    """Return (oldest_ms, oldest_date_str) for the target, or (None, None)."""
    from datetime import date as _date
    try:
        coll = mc[_resolve_db(t.mongo_db)][t.mongo_coll]
        doc = coll.find_one(
            {"release_time_ms": {"$gt": 0}},
            sort=[("release_time_ms", 1)],
            projection={"release_time_ms": 1},
        )
        if not doc:
            return None, None
        ms = int(doc["release_time_ms"])
        dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc).date()
        return ms, dt.isoformat()
    except Exception as e:  # noqa: BLE001
        logging.warning(f"[{t.key}] probe oldest failed: {e}")
        return None, None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--from", dest="from_date", default="2025-10-23",
                    help="起始日期 YYYY-MM-DD (默认 6 月前) [range 模式下使用]")
    ap.add_argument("--to", dest="to_date", default="yesterday",
                    help="结束日期 YYYY-MM-DD 或 'yesterday' [range 模式下使用]")
    ap.add_argument("--mode", choices=["recent-backward", "backward-from-oldest", "range"],
                    default="recent-backward",
                    help="recent-backward (默认): 从昨天开始往回一天一天爬, 直到 cutoff / max_days_back. "
                         "已有数据的天自动跳过 (通过 skip-if-coverage). "
                         "backward-from-oldest: 从 DB 最老日期继续往回爬. "
                         "range: 固定 [from, to] 区间.")
    ap.add_argument("--stop-after-empty-batches", type=int, default=3,
                    help="连续 N 轮 batch 没新数据则停 (平台历史到底, 默认 3)")
    ap.add_argument("--max-days-back", type=int, default=183,
                    help="最多往回爬 N 天 (默认 183=6 月, 0=不限)")
    ap.add_argument("--skip-if-coverage", type=int, default=20,
                    help="DB 该日 doc 数 ≥ N 则跳过 (默认 20; 平台典型日增 50-200 条)")
    ap.add_argument("--reverse", action="store_true",
                    help="[range 模式] 从今天往前扫")
    ap.add_argument("--only", default="",
                    help="仅跑指定 platform 或 platform/category (逗号分隔)")
    ap.add_argument("--timeout", type=int, default=3600,
                    help="单天单 target 超时秒 (默认 60 分钟;alphapai/report 大日有 300+ 条 PDF,30 分钟不够)")
    ap.add_argument("--workers", type=int, default=4,
                    help="并发 worker (默认 4)")
    ap.add_argument("--no-backfill-window", action="store_true",
                    help="禁用 orchestrator 级 backfill 窗口检查 (子 scraper 仍可独立检查)")
    ap.add_argument("--bf-force-lock", action="store_true",
                    help="orchestrator 级强制夺锁 (前一进程已死时用)")
    args = ap.parse_args()

    # 禁用 backfill window 时, monkey-patch BackfillWindow 让 run_one_day 跳过检查
    if args.no_backfill_window:
        BackfillWindow.seconds_until_allowed = staticmethod(lambda *a, **k: 0.0)
        BackfillWindow.wait_until_allowed = staticmethod(lambda *a, **k: None)

    # 整个 orchestrator 自己也持一把全局锁 — 防 2 个 backfill_by_date 同时跑
    if not BackfillLock.acquire("backfill_by_date", role="orchestrator",
                                  ttl_min=60, force=args.bf_force_lock):
        logging.error("backfill_by_date orchestrator 已被另一进程占用 (use --bf-force-lock)")
        return 2

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(LOG_DIR / "by_date.log"),
            logging.StreamHandler(),
        ],
    )

    filt = set(s.strip() for s in args.only.split(",") if s.strip())
    selected = []
    for t in TARGETS:
        if filt and t.platform not in filt and t.key not in filt:
            continue
        selected.append(t)
    if not selected:
        logging.error("no targets selected")
        return 1

    mc = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
    state = load_state()

    if args.mode == "range":
        today = datetime.now(CST).date()
        to = today - timedelta(days=1) if args.to_date == "yesterday" \
            else datetime.strptime(args.to_date, "%Y-%m-%d").date()
        frm = datetime.strptime(args.from_date, "%Y-%m-%d").date()
        if to < frm:
            logging.error("to < from, nothing to do")
            return 1
        logging.info(f"===== PLAN (range) ===== from={frm} to={to} targets={len(selected)}")
        days = []
        d = frm
        while d <= to:
            days.append(d.strftime("%Y-%m-%d"))
            d += timedelta(days=1)
        if args.reverse:
            days = list(reversed(days))
    else:
        # backward-from-oldest: dynamically compute days from current DB state.
        # We process in "rounds" — each round picks the next N days going backward
        # for each target, waits for them to complete, then re-probes DB oldest.
        # Stop when a round produces 0 new docs for all targets, repeated
        # `stop_after_empty_batches` times (platform historical horizon).
        logging.info(
            f"===== PLAN (backward-from-oldest) ===== targets={len(selected)} "
            f"workers={args.workers} max_days_back={args.max_days_back or 'unlimited'}"
        )
        for t in selected:
            ms, iso = probe_oldest_date_for_target(mc, t)
            logging.info(f"  {t.key}  current_oldest={iso or 'empty'}")
        days = []  # unused in backward mode; round loop below drives tasks

    # Build (target, day) task list
    tasks = []
    for day_str in days:
        for t in selected:
            tasks.append((t, day_str))

    total_added = 0
    total_skipped = 0
    total_done = 0
    total_errors = 0

    import threading
    from concurrent.futures import ThreadPoolExecutor, as_completed
    state_lock = threading.Lock()

    def process(task):
        t, day_str = task
        if _stop:
            return ("stopped", t, day_str, 0)
        state_key = f"{t.key}/{day_str}"
        with state_lock:
            prev = state.get(state_key)
        if prev and prev.get("status") == "done":
            return ("skipped_state", t, day_str, 0)
        cnt_before = probe_count_for_day(mc, t, day_str)
        if cnt_before >= args.skip_if_coverage:
            with state_lock:
                state[state_key] = {
                    "status": "skipped_coverage",
                    "count_in_db": cnt_before,
                    "at": datetime.now(CST).isoformat(),
                }
                save_state(state)
            return ("skipped_coverage", t, day_str, 0)
        rc, added = run_one_day(t, day_str, args.timeout)
        cnt_after = probe_count_for_day(mc, t, day_str)
        delta = cnt_after - cnt_before
        with state_lock:
            state[state_key] = {
                "status": "done" if rc == 0 else f"error_rc_{rc}",
                "added": added,
                "count_before": cnt_before,
                "count_after": cnt_after,
                "at": datetime.now(CST).isoformat(),
            }
            save_state(state)
        return ("done" if rc == 0 else "error", t, day_str, delta)

    if args.mode == "range":
        logging.info(f"===== 并发 workers={args.workers} =====")
        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            futures = [ex.submit(process, task) for task in tasks]
            for fut in as_completed(futures):
                if _stop:
                    break
                try:
                    status, t, day_str, delta = fut.result()
                except Exception as e:
                    logging.error(f"task error: {e}")
                    total_errors += 1
                    continue
                if status == "done":
                    total_done += 1
                    total_added += delta
                elif status in ("skipped_state", "skipped_coverage"):
                    total_skipped += 1
                else:
                    total_errors += 1
                if (total_done + total_skipped + total_errors) % 10 == 0:
                    logging.info(
                        f"progress: done={total_done} skip={total_skipped} "
                        f"err={total_errors} added={total_added}"
                    )
    elif args.mode == "recent-backward":
        # recent-backward mode: 从昨天开始, 一天一天往回爬, 直到 max_days_back.
        # skip-if-coverage 会自动跳过已有数据的日子, 所以这个模式对 "continue
        # from where we left off" 天然友好.
        today = datetime.now(CST).date()
        start_day = today - timedelta(days=1)  # 昨天
        end_day = today - timedelta(days=args.max_days_back) if args.max_days_back else datetime(2020, 1, 1).date()
        logging.info(
            f"===== PLAN (recent-backward) ===== targets={len(selected)} "
            f"start={start_day.isoformat()} end={end_day.isoformat()} "
            f"workers={args.workers} skip_if_coverage={args.skip_if_coverage}"
        )

        # Per-target state
        target_state: dict[str, dict[str, Any]] = {
            t.key: {
                "next_day": start_day,       # 下一个要爬的日期
                "empty_streak": 0,
                "days_processed": 0,
            }
            for t in selected
        }
        round_idx = 0
        while True:
            if _stop:
                break
            # 决定每个 target 是否继续
            active = []
            for t in selected:
                st = target_state[t.key]
                if st["next_day"] < end_day:
                    continue  # 已达 cutoff, 不再爬
                if st["empty_streak"] >= args.stop_after_empty_batches:
                    continue  # 连续 N 轮没新 → 平台历史到底或 API 不支持日期
                active.append(t)
            if not active:
                logging.info("===== 所有 target 到达停止条件")
                break

            round_idx += 1
            round_tasks: list[tuple[Target, str]] = []
            per_target_day_count: dict[str, int] = {}
            for t in active:
                st = target_state[t.key]
                picks = []
                d = st["next_day"]
                for _ in range(args.workers):
                    if d < end_day:
                        break
                    picks.append(d.strftime("%Y-%m-%d"))
                    d -= timedelta(days=1)
                if not picks:
                    continue
                # Advance next_day to the day AFTER the batch we're about to run
                st["next_day"] = d
                per_target_day_count[t.key] = len(picks)
                for p in picks:
                    round_tasks.append((t, p))

            if not round_tasks:
                break
            logging.info(
                f"===== round {round_idx} — {len(round_tasks)} tasks "
                f"(active targets: {[t.key for t in active]}) ====="
            )
            for tt, dd in round_tasks[:8]:
                logging.info(f"  + {tt.key}/{dd}")
            if len(round_tasks) > 8:
                logging.info(f"  … 以及另 {len(round_tasks)-8} 个")

            round_added_per_target: dict[str, int] = {t.key: 0 for t in active}
            round_actual_scrape_per_target: dict[str, int] = {t.key: 0 for t in active}
            with ThreadPoolExecutor(max_workers=max(args.workers, len(round_tasks))) as ex:
                futures = {ex.submit(process, task): task for task in round_tasks}
                for fut in as_completed(futures):
                    if _stop:
                        break
                    try:
                        status, t, day_str, delta = fut.result()
                    except Exception as e:
                        logging.error(f"task error: {e}")
                        total_errors += 1
                        continue
                    if status == "done":
                        total_done += 1
                        total_added += delta
                        round_added_per_target[t.key] = round_added_per_target.get(t.key, 0) + delta
                        round_actual_scrape_per_target[t.key] = round_actual_scrape_per_target.get(t.key, 0) + 1
                    elif status in ("skipped_state", "skipped_coverage"):
                        total_skipped += 1
                    else:
                        total_errors += 1

            for t in active:
                added_this_round = round_added_per_target.get(t.key, 0)
                actual_scrapes = round_actual_scrape_per_target.get(t.key, 0)
                target_state[t.key]["days_processed"] += per_target_day_count.get(t.key, 0)
                # Empty-streak logic: only count rounds where we ACTUALLY ran the
                # scraper (status=done). If every task was skipped (day already
                # covered), this doesn't signal "platform out of data" — we're
                # just fast-forwarding through already-ingested dates. Reset
                # (or hold) the streak rather than letting skipped rounds
                # prematurely stop the backfill.
                if actual_scrapes == 0:
                    # All skipped — neither incr nor reset. Just pass through.
                    pass
                elif added_this_round == 0:
                    target_state[t.key]["empty_streak"] += 1
                else:
                    target_state[t.key]["empty_streak"] = 0
                new_oldest_ms, new_iso = probe_oldest_date_for_target(mc, t)
                days_cov = None
                if new_oldest_ms:
                    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
                    days_cov = max(0, int((now_ms - new_oldest_ms) / 86400000))
                logging.info(
                    f"[{t.key}] round{round_idx} added={added_this_round} scraped={actual_scrapes} "
                    f"processed={target_state[t.key]['days_processed']}d "
                    f"next={target_state[t.key]['next_day']} "
                    f"oldest→{new_iso} coverage={days_cov}d "
                    f"empty_streak={target_state[t.key]['empty_streak']}/{args.stop_after_empty_batches}"
                )

    else:  # backward-from-oldest (legacy)
        target_state: dict[str, dict[str, Any]] = {
            t.key: {
                "empty_streak": 0,
                "days_pushed": 0,
                "initial_oldest_ms": probe_oldest_date_for_target(mc, t)[0],
            }
            for t in selected
        }
        round_idx = 0
        while True:
            if _stop:
                break
            active_targets = [t for t in selected
                              if target_state[t.key]["empty_streak"] < args.stop_after_empty_batches
                              and (not args.max_days_back or target_state[t.key]["days_pushed"] < args.max_days_back)]
            if not active_targets:
                break
            round_idx += 1
            round_tasks: list[tuple[Target, str]] = []
            per_target_day_count: dict[str, int] = {}
            for t in active_targets:
                oldest_ms, _ = probe_oldest_date_for_target(mc, t)
                if not oldest_ms:
                    target_state[t.key]["empty_streak"] = args.stop_after_empty_batches
                    continue
                oldest_date = datetime.fromtimestamp(oldest_ms / 1000, tz=timezone.utc).date()
                picks = [(oldest_date - timedelta(days=i)).strftime("%Y-%m-%d")
                         for i in range(1, args.workers + 1)]
                for d in picks:
                    round_tasks.append((t, d))
                per_target_day_count[t.key] = len(picks)
            if not round_tasks:
                break
            round_added_per_target: dict[str, int] = {t.key: 0 for t in active_targets}
            with ThreadPoolExecutor(max_workers=args.workers * len(active_targets)) as ex:
                futures = {ex.submit(process, task): task for task in round_tasks}
                for fut in as_completed(futures):
                    try:
                        status, t, day_str, delta = fut.result()
                    except Exception:
                        total_errors += 1
                        continue
                    if status == "done":
                        total_done += 1; total_added += delta
                        round_added_per_target[t.key] = round_added_per_target.get(t.key, 0) + delta
                    elif status in ("skipped_state", "skipped_coverage"):
                        total_skipped += 1
                    else:
                        total_errors += 1
            for t in active_targets:
                added_this_round = round_added_per_target.get(t.key, 0)
                target_state[t.key]["days_pushed"] += per_target_day_count.get(t.key, 0)
                if added_this_round == 0:
                    target_state[t.key]["empty_streak"] += 1
                else:
                    target_state[t.key]["empty_streak"] = 0

    logging.info(
        f"===== ALL DONE ===== done={total_done} skipped={total_skipped} "
        f"errors={total_errors} total_added={total_added}"
    )
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    finally:
        try:
            BackfillLock.release("backfill_by_date", role="orchestrator")
        except Exception:
            pass
