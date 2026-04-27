#!/usr/bin/env python3
"""6-month historical backfill orchestrator.

Runs scraper.py for each (platform, collection) without --resume so it pages from
the newest item down through the history, stops each subprocess when Mongo's
oldest doc crosses the cutoff date. Platforms run in parallel (different auth
domains); categories within a platform run sequentially (shared account / quota).

Usage:
    nohup python3 -u crawl/backfill_6months.py \
        > logs/backfill_6months/orchestrator.out 2>&1 &

Stop:
    pkill -TERM -f backfill_6months.py    # graceful: shuts down all children
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
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Clash proxy must be off for local Mongo + CN CDN downloads.
# Also unset ALL_PROXY — Clash sets socks5://127.0.0.1:7890 which requests/urllib3
# can't use without pysocks (-> "Missing dependencies for SOCKS support").
_PROXY_ENV_KEYS = (
    "http_proxy", "https_proxy", "all_proxy", "no_proxy",
    "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY",
)
for _k in _PROXY_ENV_KEYS:
    os.environ.pop(_k, None)

from pymongo import MongoClient  # noqa: E402

# Backfill antibot integration
sys.path.insert(0, str(Path(__file__).resolve().parent))
from antibot import (  # noqa: E402
    BackfillWindow, BackfillLock, SoftCooldown, _DEFAULT_ACCOUNT_BUDGET,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
LOG_DIR = REPO_ROOT / "logs" / "backfill_6months"
LOG_DIR.mkdir(parents=True, exist_ok=True)

CST = timezone(timedelta(hours=8))

# Post-2026-04-23 the local crawl_data Mongo container is gone and all data
# lives on remote 192.168.31.176:35002 behind u_spider auth. Inherit URI from
# env (crawler_monitor.py + .env set this) and fall back to the remote URI so
# standalone invocations (no env) still find the data.
MONGO_URI = os.environ.get(
    "MONGO_URI",
    "mongodb://127.0.0.1:27018/",
)

# Remote DB names differ from the old local short names; keep a translation so
# existing `Target.mongo_db="alphapai"` references still resolve correctly.
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
    platform: str          # group key (shared auth)
    task: str              # unique within platform
    cwd: str               # cwd for subprocess
    extra_args: list       # scraper flags (category, type, etc.)
    mongo_db: str
    mongo_coll: str
    date_fields: list = field(default_factory=lambda: ["release_time_ms"])
    mongo_filter: dict = field(default_factory=dict)
    # date_fields: some collections use different field names for the release timestamp
    # mongo_filter: extra query filter (e.g. {"type": 2} for meritco type-split forum coll)

    @property
    def key(self) -> str:
        return f"{self.platform}_{self.task}"


# ----- Per-task throttle ---------------------------------------------------
# Baseline (multiplier=1.0): 4s/2.5s Gaussian, burst 30, cap 400, --account-role
# bg (后台桶让位 realtime). Per-platform speed multipliers below scale these knobs
# (higher m = faster — shorter throttle, larger burst+cap, shorter cooldown).
#
# 2026-04-26 第二轮调速 — 基于真实测量校准:
#   gangtise     5x    原 10x 让 burst=300 在 2.6min 吃掉一大块 bg-budget;
#                      改 5x 让 base=0.8s/burst=150, 配合扩大的 bg-budget=8000
#                      慢慢 24h 撑开抓
#   jinmen       3x    15x 被平台返回 "msg:稍后再试" + 60min 静默, 实际比 1x 还慢;
#                      降到 3x = base 1.3s 接近合理边界
#   alphaengine  1x    5x 秒触发 REFRESH_LIMIT (quota 是平台日定额, 跟请求率无关),
#                      加速等于"快速消耗 quota → 进入 30min cooldown → 每天少做几个
#                      cycle", 反而是负优化. 1x 让单 cycle 慢慢吃满 400 条 quota.
SPEED_MULT: dict[str, float] = {
    "gangtise":     5.0,
    "jinmen":       3.0,
    "alphaengine":  1.0,
    # other platforms default to 1.0 (alphapai/meritco/funda/acecamp)
}

# 2026-04-27: 调高 24h backfill 桶上限解决 "scraper 启动立即 [antibot] daily-cap 达到"
# 现象 — antibot _PLATFORM_BACKFILL_DEFAULTS 的 1500/day 在 6 个月回填语境下太紧,
# burst+0.4s base 一启动 16min 就撞顶, 之后 24h 整个 zset 滚出窗口前都 0 入库.
# 用户授权: "增多一些上限 慢慢爬取一般不会封号的". 配合 SPEED_MULT 降速 + Gaussian
# throttle 节奏保持温和.
PLATFORM_BG_BUDGETS: dict[str, int] = {
    "alphapai":      5000,
    "jinmen":        6000,
    "meritco":       3000,
    "funda":         4000,
    "gangtise":     10000,   # 5 个 type 共用一个 G_token (research+summary+chief × backfill+watch)
    "acecamp":       2000,
    "alphaengine":   4000,
    "thirdbridge":    600,
    "semianalysis":  2000,
}


def universal_flags_for(platform: str) -> list[str]:
    m = SPEED_MULT.get(platform, 1.0)
    bg_budget = PLATFORM_BG_BUDGETS.get(platform, 4000)
    return [
        "--throttle-base",        f"{4.0 / m:.3f}",
        "--throttle-jitter",      f"{2.5 / m:.3f}",
        "--burst-size",           str(int(30 * m)),
        "--burst-cooldown-min",   str(max(5, int(60 / m))),
        "--burst-cooldown-max",   str(max(15, int(180 / m))),
        "--daily-cap",            str(int(400 * m)),
        "--account-role",         "bg",
        "--bg-budget",            str(bg_budget),
        # Stream mode: each scraper dumps per-page instead of list-first-then-dump,
        # so DB writes start on page 1 and deep_page checkpoint enables resume.
        "--stream-backfill",
    ]


TARGETS: list[Target] = [
    # AlphaPai — 3 active categories (comment / roadshow / report); shared JWT account.
    # wechat 微信社媒爬取已停用 (2026-04-24) — 已入库保留, 历史回填不再扩圈.
    # Target("alphapai", "wechat",   "crawl/alphapai_crawl",
    #        ["--category", "wechat",   "--page-size", "100"],
    #        "alphapai", "wechat_articles"),
    # Comment: split subtype, each has independent top-N (实测 selected/regular
    # 各 10k+, 混合查询封顶 ~10k = DB 饱和). 同 roadshow 策略.
    Target("alphapai", "comment_selected", "crawl/alphapai_crawl",
           ["--category", "comment", "--market-type", "selected", "--page-size", "50"],
           "alphapai", "comments"),
    Target("alphapai", "comment_regular", "crawl/alphapai_crawl",
           ["--category", "comment", "--market-type", "regular", "--page-size", "50"],
           "alphapai", "comments"),
    # Roadshow: split into 6 subtypes, each has independent top-N list.
    # Mixed query caps at ~1.5k (what we'd been hitting), but per-subtype lists
    # expose 10-112k each (实测 2026-04-23: ashare 111k / us 112k / hk 11k /
    # web 29k / ir 10k / hot 105). Splitting → 200× more docs reachable.
    Target("alphapai", "roadshow_ashare", "crawl/alphapai_crawl",
           ["--category", "roadshow", "--market-type", "ashare", "--page-size", "50"],
           "alphapai", "roadshows"),
    Target("alphapai", "roadshow_hk", "crawl/alphapai_crawl",
           ["--category", "roadshow", "--market-type", "hk", "--page-size", "50"],
           "alphapai", "roadshows"),
    Target("alphapai", "roadshow_us", "crawl/alphapai_crawl",
           ["--category", "roadshow", "--market-type", "us", "--page-size", "50"],
           "alphapai", "roadshows"),
    Target("alphapai", "roadshow_web", "crawl/alphapai_crawl",
           ["--category", "roadshow", "--market-type", "web", "--page-size", "50"],
           "alphapai", "roadshows"),
    Target("alphapai", "roadshow_ir", "crawl/alphapai_crawl",
           ["--category", "roadshow", "--market-type", "ir", "--page-size", "50"],
           "alphapai", "roadshows"),
    Target("alphapai", "report",   "crawl/alphapai_crawl",
           ["--category", "report",   "--page-size", "100"],
           "alphapai", "reports"),

    # Jinmen — meetings + reports + oversea_reports
    # 2026-04-26 disabled: jinmen.meetings 端点对当前 token 返回 code=500 用户信息不存在;
    # jinmen.oversea_reports 端点账号被冻结 (code=201). 都需要重登 jinmen 才能恢复;
    # 让 backfill orchestrator 不再 spawn 这两条避免持续打死端点 + 烧 fail-streak.
    # Target("jinmen", "meetings", "crawl/jinmen",
    #        ["--page-size", "50"],
    #        "jinmen", "meetings"),
    Target("jinmen", "reports",  "crawl/jinmen",
           ["--reports", "--page-size", "100"],
           "jinmen", "reports"),
    # Target("jinmen", "oversea_reports", "crawl/jinmen",
    #        ["--oversea-reports", "--page-size", "50", "--skip-pdf"],
    #        "jinmen", "oversea_reports"),

    # Meritco — type 2 (professional) + type 3 (jiuqian-native); same collection, filter by type
    Target("meritco", "type2", "crawl/meritco_crawl",
           ["--type", "2", "--page-size", "50"],
           "meritco", "forum", mongo_filter={"type": 2}),
    Target("meritco", "type3", "crawl/meritco_crawl",
           ["--type", "3", "--page-size", "50"],
           "meritco", "forum", mongo_filter={"type": 3}),

    # Gangtise — research + summary + chief (chief 有 2023-06 历史, 也跑保持活跃)
    # research uses ES-style from/size endpoint: size=1000 可用, 设 500 快 5x.
    Target("gangtise", "research", "crawl/gangtise",
           ["--type", "research", "--page-size", "500"],
           "gangtise", "researches"),
    Target("gangtise", "summary",  "crawl/gangtise",
           ["--type", "summary",  "--page-size", "50"],
           "gangtise", "summaries"),
    Target("gangtise", "chief",    "crawl/gangtise",
           ["--type", "chief",    "--page-size", "100"],
           "gangtise", "chief_opinions"),

    # Funda — 4 collections. Posts/earnings already >6mo covered, but keep one
    # scraper per category active per user requirement "每个板块都有一条爬虫".
    # sentiment 没在 live watcher 里, 这里接管持续拉.
    Target("funda", "post", "crawl/funda",
           ["--category", "post", "--page-size", "50"],
           "funda", "posts"),
    Target("funda", "earnings_report", "crawl/funda",
           ["--category", "earnings_report", "--page-size", "50"],
           "funda", "earnings_reports"),
    Target("funda", "earnings_transcript", "crawl/funda",
           ["--category", "earnings_transcript", "--page-size", "50"],
           "funda", "earnings_transcripts"),
    Target("funda", "sentiment", "crawl/funda",
           ["--sentiment", "--sentiment-days", "200"],
           "funda", "sentiments"),

    # AceCamp — articles + opinions (events 已于 2026-04 被平台移除)
    # 2026-04-24 封控事故后调整:
    # - 6 月回填不抓 article_info detail — VIP quota ~12/天, 回填 30 万条 detail
    #   根本不可能; 先把 list 元数据 (title/organization/release_time/hashtags)
    #   回灌到位, detail 留给实时活跃用户 + 未来 quota 恢复后的分批补齐脚本.
    # - page_size 30 (旧 50): 单 list 调用返回更少, 一轮切分更细, 配合
    #   UNIVERSAL_FLAGS 的 break_every 更频繁, 避免稳态高密度.
    Target("acecamp", "articles", "crawl/AceCamp",
           ["--type", "articles", "--page-size", "30", "--skip-detail"],
           "acecamp", "articles"),
    Target("acecamp", "opinions", "crawl/AceCamp",
           ["--type", "opinions", "--page-size", "30"],
           "acecamp", "opinions"),

    # AlphaEngine — requires CST 00:00 quota reset; launched separately via `at`
    # with --only alphaengine after midnight.
    Target("alphaengine", "chinaReport", "crawl/alphaengine",
           ["--category", "chinaReport", "--page-size", "50"],
           "alphaengine", "china_reports",
           date_fields=["release_time_ms", "publish_time_ms"]),
    Target("alphaengine", "summary", "crawl/alphaengine",
           ["--category", "summary", "--page-size", "50"],
           "alphaengine", "summaries",
           date_fields=["release_time_ms", "publish_time_ms"]),
    Target("alphaengine", "news", "crawl/alphaengine",
           ["--category", "news", "--page-size", "50"],
           "alphaengine", "news_items",
           date_fields=["release_time_ms", "publish_time_ms"]),
    Target("alphaengine", "foreignReport", "crawl/alphaengine",
           ["--category", "foreignReport", "--page-size", "50"],
           "alphaengine", "foreign_reports",
           date_fields=["release_time_ms", "publish_time_ms"]),
]

# Platforms whose targets run concurrently (one runner per target) instead of
# serially inside one runner. User requirement 每个板块都有一条爬虫工作:
# we put every platform in parallel so each category gets its own scraper.
# Risk: per-token quota may bite on small accounts — watch for 429/auth-dead
# errors in logs and narrow if needed.
PARALLEL_PLATFORMS: set[str] = {
    "alphapai", "jinmen", "meritco", "gangtise",
    "funda", "acecamp", "alphaengine",
}

# ThirdBridge: skipped — cookie 401-dead, user will refresh later
# Funda / AceCamp: already have ≥6-month coverage, skipped


# ----- Oldest-doc probe ----------------------------------------------------

def probe_oldest_ms(mc: MongoClient, target: Target) -> int | None:
    """Return oldest release_time_ms for a target, or None if empty."""
    coll = mc[_resolve_db(target.mongo_db)][target.mongo_coll]
    for f in target.date_fields:
        q = {f: {"$exists": True, "$gt": 0}}
        q.update(target.mongo_filter)
        doc = coll.find_one(q, sort=[(f, 1)], projection={f: 1})
        if doc and doc.get(f):
            return int(doc[f])
    return None


def probe_count(mc: MongoClient, target: Target) -> int:
    coll = mc[_resolve_db(target.mongo_db)][target.mongo_coll]
    if target.mongo_filter:
        return coll.count_documents(target.mongo_filter)
    return coll.estimated_document_count()


# ----- Runner --------------------------------------------------------------

class PlatformRunner:
    """Drives one platform's target list sequentially."""

    # Stall watchdog: kill child if its log file hasn't been written for this
    # many seconds while the process is still alive. Catches scrapers that
    # silently hang in select() / blocked socket I/O without exiting (observed
    # 2026-04-27: 3 alphaengine PIDs hung in do_select for 14h, orchestrator's
    # `proc.poll()` check kept them as "running" forever).
    STALL_TIMEOUT_S = 1800   # 30 min

    def __init__(self, platform: str, targets: list[Target], cutoff_ms: int):
        self.platform = platform
        self.targets = targets
        self.cutoff_ms = cutoff_ms
        self.idx = 0
        self.proc: subprocess.Popen | None = None
        self.log_file = None
        self.log_path: Path | None = None
        self.started_at: float | None = None
        self.start_count: int = 0
        self.start_oldest_ms: int | None = None

    @property
    def current(self) -> Target | None:
        if self.idx >= len(self.targets):
            return None
        return self.targets[self.idx]

    def maybe_skip_done(self, mc: MongoClient) -> None:
        """No-op in 'each-section has a scraper' mode.

        Previously skipped targets whose oldest was already past the cutoff,
        but user wants every 板块 to keep a running scraper (picks up newly
        published docs, periodic refresh, visible ongoing activity in the
        dashboard). We keep the probe for logging only.
        """
        if self.idx < len(self.targets):
            t = self.targets[self.idx]
            oldest = probe_oldest_ms(mc, t)
            cnt = probe_count(mc, t)
            if oldest is not None and oldest <= self.cutoff_ms:
                ts = datetime.fromtimestamp(oldest / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
                logging.info(f"[{t.key}] already covered (count={cnt} oldest={ts}) — still running scraper for refresh")

    def start_current(self) -> None:
        t = self.current
        if t is None:
            return
        # 平台级停爬闸门: 该 target 的 cwd 下有 DISABLED 文件就跳到下一个 target.
        disable_file = REPO_ROOT / t.cwd / "DISABLED"
        if disable_file.exists():
            logging.warning(f"[{t.key}] SKIPPED — {disable_file} exists (platform disabled)")
            self.idx += 1
            self.start_current()
            return
        # Snapshot pre-run state so final report can show delta
        mc = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
        self.start_count = probe_count(mc, t)
        self.start_oldest_ms = probe_oldest_ms(mc, t)
        mc.close()
        oldest_str = (
            datetime.fromtimestamp(self.start_oldest_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
            if self.start_oldest_ms else "empty"
        )
        log_path = LOG_DIR / f"{t.key}.log"
        self.log_path = log_path
        # Inject remote Mongo URI + resolved DB name. Post-migration the local
        # crawl_data container is gone; scraper's hardcoded "alphapai" default
        # must be overridden with the remote "alphapai-full" alias.
        remote_db = _resolve_db(t.mongo_db)
        platform_flags = universal_flags_for(t.platform)
        speed_mult = SPEED_MULT.get(t.platform, 1.0)
        extra = list(t.extra_args) + [
            "--mongo-uri", MONGO_URI,
            "--mongo-db", remote_db,
        ]
        self.log_file = open(log_path, "ab", buffering=0)
        header = (
            f"\n\n===== BACKFILL START {datetime.now(CST).isoformat()} =====\n"
            f"target={t.key} cwd={t.cwd} remote_db={remote_db} speed={speed_mult}x\n"
            f"starting state: count={self.start_count} oldest={oldest_str}\n"
            f"cmd: python3 -u scraper.py {' '.join(extra + platform_flags)}\n\n"
        ).encode()
        self.log_file.write(header)
        cmd = ["python3", "-u", "scraper.py"] + extra + platform_flags
        env = os.environ.copy()
        for k in _PROXY_ENV_KEYS:
            env.pop(k, None)
        env["MONGO_URI"] = MONGO_URI
        env["MONGO_DB"] = remote_db
        self.proc = subprocess.Popen(
            cmd,
            cwd=str(REPO_ROOT / t.cwd),
            stdout=self.log_file,
            stderr=subprocess.STDOUT,
            env=env,
            start_new_session=True,  # survive orchestrator death
        )
        self.started_at = time.time()
        logging.info(
            f"[{t.key}] START pid={self.proc.pid} "
            f"(pre: count={self.start_count} oldest={oldest_str})"
        )

    def terminate_current(self, reason: str) -> None:
        if not self.proc:
            return
        logging.info(f"[{self.current.key if self.current else '?'}] STOP ({reason}) pid={self.proc.pid}")
        try:
            os.killpg(os.getpgid(self.proc.pid), signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            self.proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(os.getpgid(self.proc.pid), signal.SIGKILL)
            except ProcessLookupError:
                pass
        if self.log_file:
            self.log_file.write(f"\n===== BACKFILL STOP ({reason}) =====\n\n".encode())
            self.log_file.close()
            self.log_file = None
        self.proc = None

    def poll(self, mc: MongoClient) -> str:
        """Return 'running' | 'advance' | 'restart' | 'done'."""
        t = self.current
        if t is None:
            return "done"
        # 1) cutoff check — previously advanced the runner (= stopped it), but
        # user wants each 板块 to keep a scraper alive for continuous refresh
        # (new docs published today still need ingestion). So cutoff is just
        # informational: we let the current scraper run its course and let the
        # restart loop (in step 2) keep respawning.
        oldest = probe_oldest_ms(mc, t)
        if oldest is not None and oldest <= self.cutoff_ms:
            # No-op — log the fact at INFO level once per cutoff-cross transition
            # if it hasn't been logged yet for this runner.
            if not getattr(self, "_cutoff_logged", False):
                logging.info(
                    f"[{t.key}] cutoff reached "
                    f"({datetime.fromtimestamp(oldest/1000, tz=timezone.utc).strftime('%Y-%m-%d')}) "
                    f"— scraper 保持活跃以 refresh 新 docs"
                )
                self._cutoff_logged = True
        # 2a) stall watchdog: if process is alive but its log file hasn't been
        # written for STALL_TIMEOUT_S seconds, force-kill so step 2b restarts it.
        # Guards against scrapers hanging in select() / blocked socket I/O.
        if self.proc and self.proc.poll() is None and self.log_path is not None:
            try:
                age = time.time() - self.log_path.stat().st_mtime
            except OSError:
                age = 0
            if age > self.STALL_TIMEOUT_S:
                logging.warning(
                    f"[{t.key}] STALLED — log idle {age/60:.1f}min "
                    f"(threshold {self.STALL_TIMEOUT_S/60:.0f}min), "
                    f"killing pid={self.proc.pid}"
                )
                try:
                    os.killpg(os.getpgid(self.proc.pid), signal.SIGKILL)
                except ProcessLookupError:
                    pass
                # Don't return — fall through to step 2b which will detect the
                # exit on this same poll cycle and restart.
                try:
                    self.proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    pass
        # 2b) process exit check
        if self.proc and self.proc.poll() is not None:
            rc = self.proc.returncode
            self._finalize_and_log(mc, f"process exited rc={rc}")
            if self.log_file:
                self.log_file.write(f"\n===== BACKFILL STOP (process exited rc={rc}) =====\n\n".encode())
                self.log_file.close()
                self.log_file = None
            self.proc = None
            # Always restart: user wants each section to always have a scraper.
            # rc != 0 (auth dead, quota exhausted) still restarts but after a
            # grace period tracked by restart_count — excessive restarts indicate
            # platform auth/API is broken, advance to next target.
            self._restart_count = getattr(self, "_restart_count", 0) + 1
            if rc != 0 and self._restart_count >= 10:
                logging.warning(
                    f"[{t.key}] rc={rc} consecutive failures={self._restart_count}; "
                    f"advancing (likely auth/quota broken)"
                )
                self._restart_count = 0
                self.idx += 1
                return "advance"
            if rc == 0:
                # Successful exit — reset fail counter, restart for continuous coverage.
                self._restart_count = 0
            logging.info(
                f"[{t.key}] rc={rc} restarting for continuous coverage "
                f"(fail_streak={self._restart_count})"
            )
            return "restart"
        return "running"

    def _finalize_and_log(self, mc: MongoClient, reason: str) -> None:
        t = self.current
        if t is None:
            return
        cnt = probe_count(mc, t)
        oldest = probe_oldest_ms(mc, t)
        oldest_str = (
            datetime.fromtimestamp(oldest / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
            if oldest else "empty"
        )
        elapsed = time.time() - (self.started_at or time.time())
        delta = cnt - self.start_count
        logging.info(
            f"[{t.key}] FINISH ({reason}) elapsed={elapsed/60:.1f}min "
            f"count: {self.start_count} → {cnt} (+{delta}) oldest={oldest_str}"
        )


# ----- Main loop -----------------------------------------------------------

def make_logger() -> logging.Logger:
    lg = logging.getLogger()
    lg.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")
    fh = logging.FileHandler(LOG_DIR / "orchestrator.log")
    fh.setFormatter(fmt)
    lg.addHandler(fh)
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    lg.addHandler(sh)
    return lg


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--cutoff", default="2025-10-23",
                    help="Target oldest date in DB (YYYY-MM-DD); backfill per target ends when oldest <= cutoff. Default 2025-10-23 (6 months).")
    ap.add_argument("--poll-interval", type=int, default=300,
                    help="Seconds between cutoff/process polls (default 300).")
    ap.add_argument("--dry-run", action="store_true",
                    help="Print per-target plan without launching scrapers.")
    ap.add_argument("--only", default="",
                    help="Comma-separated list of platform keys to include (alphapai,jinmen,meritco,gangtise). Default: all.")
    ap.add_argument("--no-backfill-window", action="store_true",
                    help="禁用 backfill 窗口检查 (orchestrator + 子 scraper 都不挡).")
    ap.add_argument("--bf-force-lock", action="store_true",
                    help="强制夺锁 (前一进程已死时用)")
    args = ap.parse_args()

    make_logger()

    # Orchestrator-level lock — 防 2 个 backfill_6months 同时跑
    if not BackfillLock.acquire("backfill_6months", role="orchestrator",
                                  ttl_min=120, force=args.bf_force_lock):
        logging.error("backfill_6months orchestrator 已被另一进程占用 (use --bf-force-lock)")
        return 2

    # Backfill window — 卡住就 sleep 到打开
    if not args.no_backfill_window and not args.dry_run:
        # 用第一个目标平台的窗口策略 (各平台都是 22-08 + 周末, 等价)
        first_pf = TARGETS[0].platform if TARGETS else "alphapai"
        BackfillWindow.wait_until_allowed(first_pf)

    cutoff_dt = datetime.strptime(args.cutoff, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    cutoff_ms = int(cutoff_dt.timestamp() * 1000)
    logging.info(f"Cutoff: {cutoff_dt.isoformat()} ({cutoff_ms} ms)")

    selected_platforms = set(p.strip() for p in args.only.split(",") if p.strip())
    targets = [t for t in TARGETS if (not selected_platforms or t.platform in selected_platforms)]

    # Group into runners. Default: one runner per platform (serial inside).
    # For platforms in PARALLEL_PLATFORMS: one runner per target (4x concurrent).
    groups: dict[str, list[Target]] = {}
    for t in targets:
        if t.platform in PARALLEL_PLATFORMS:
            key = f"{t.platform}/{t.task}"  # distinct runner per category
        else:
            key = t.platform
        groups.setdefault(key, []).append(t)

    mc = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)

    logging.info(f"===== PLAN ({len(groups)} platforms, {len(targets)} targets) =====")
    for plat, ts in groups.items():
        for t in ts:
            cnt = probe_count(mc, t)
            oldest = probe_oldest_ms(mc, t)
            if oldest:
                ts_str = datetime.fromtimestamp(oldest / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
                gap_days = (cutoff_dt - datetime.fromtimestamp(oldest / 1000, tz=timezone.utc)).days
                status = "DONE" if oldest <= cutoff_ms else f"need {gap_days:+d}d"
            else:
                ts_str = "empty"
                status = "need full"
            logging.info(f"  {t.key:<22}  count={cnt:>7}  oldest={ts_str:<12}  {status}")

    if args.dry_run:
        logging.info("Dry-run — exiting without launching.")
        return 0

    runners = [PlatformRunner(p, ts, cutoff_ms) for p, ts in groups.items()]

    # Skip already-done targets, start each platform's first live target
    for r in runners:
        r.maybe_skip_done(mc)
        if r.current is not None:
            r.start_current()
            time.sleep(5)  # small startup stagger between platforms

    def sig_handler(*_):
        logging.info("Signal received — terminating all children")
        for r in runners:
            r.terminate_current("orchestrator shutdown")
        sys.exit(0)

    signal.signal(signal.SIGTERM, sig_handler)
    signal.signal(signal.SIGINT, sig_handler)

    tick = 0
    while True:
        tick += 1
        all_done = True
        for r in runners:
            status = r.poll(mc)
            if status == "advance":
                r.maybe_skip_done(mc)
                if r.current is not None:
                    r.start_current()
            elif status == "restart":
                # Same target, stream-backfill checkpoint drives resume.
                r.start_current()
            if r.current is not None or r.proc is not None:
                all_done = False

        if all_done:
            logging.info("===== ALL TARGETS DONE =====")
            return 0

        # Progress snapshot every ~30 min
        if tick % max(1, int(1800 / args.poll_interval)) == 0:
            logging.info("----- progress snapshot -----")
            for r in runners:
                t = r.current
                if t is None:
                    logging.info(f"  [{r.platform}] all targets complete")
                    continue
                cnt = probe_count(mc, t)
                oldest = probe_oldest_ms(mc, t)
                oldest_str = (
                    datetime.fromtimestamp(oldest / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
                    if oldest else "empty"
                )
                pid = r.proc.pid if r.proc else "-"
                logging.info(f"  [{t.key}] pid={pid} count={cnt} oldest={oldest_str}")

        time.sleep(args.poll_interval)


if __name__ == "__main__":
    try:
        sys.exit(main())
    finally:
        try:
            BackfillLock.release("backfill_6months", role="orchestrator")
        except Exception:
            pass
