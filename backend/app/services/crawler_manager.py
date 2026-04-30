"""Spawn / monitor / stop per-platform scraper watchers.

The scraper processes are independent children of the API server — they run
`scraper.py --watch --resume --interval N` for their platform, write to
`logs/crawler_<platform>.log`, and persist their PID into Redis so the API
can report status across restarts.

On API restart we don't kill these processes; we just rebind (verify the PID
is still alive via `kill -0`). A user-spawned manual scraper (from the CLI)
isn't tracked here, but can be adopted by calling `register_existing()`.
"""

from __future__ import annotations

import asyncio
import os
import signal
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import redis.asyncio as aioredis


_REPO_ROOT = Path(__file__).resolve().parents[3]
_CRAWL_DIR = _REPO_ROOT / "crawl"
_LOGS_DIR = _REPO_ROOT / "logs"
_LOGS_DIR.mkdir(exist_ok=True)

# web_research/ 已迁到 proposal-agent 独立项目 (2026-04-29 二次)。
# WEB_RESEARCH_DIR env var 可覆盖默认路径(用于 dev/CI 切换)。
_RESEARCH_DIR = Path(
    os.environ.get("WEB_RESEARCH_DIR")
    or "/home/ygwang/proposal-agent/web_research"
)

# 已迁出 crawl/ 的平台:platform_key → 实际目录根。新增需在这里注册。
_EXTERNAL_DIR: dict[str, Path] = {
    "wechat_mp": _RESEARCH_DIR,   # proposal-agent/web_research/wechat_mp
}


@dataclass(frozen=True)
class CrawlerSpec:
    """Per-platform command template.

    `variants` is a {name: flags} dict. Platforms like Meritco with multiple
    forum types run one scraper process per variant so each can be tracked
    independently (both in our Redis state and in `crawler_monitor.py`).
    """

    platform: str
    dir_name: str
    variants: dict[str, tuple[str, ...]]


# 每平台拆成 per-category 独立 variant, 和 crawler_monitor.ALL_SCRAPERS 完全对齐.
# 前端数据源管理登录后调 start(platform, force=True) 会拉起该平台 **所有** variant,
# 每个 variant 独立 --category/--type/--reports flag, 对应 monitor UI 的 sub-tab.
# 统一参数: --interval 60, --throttle-base 1.5, --throttle-jitter 1.0 (实时模式)
_RT = ("--watch", "--resume", "--since-hours", "24", "--interval", "60",
       "--throttle-base", "1.5", "--throttle-jitter", "1.0")

# Gangtise 研报走 ES-style from/size 分页 (2026-04-22 从 SPA 抓包反解).
# 单 tick 能拉 100 条, 单日实际发布 ~1000 篇 (内资 233 + 外资 772),
# 30s tick + page_size=100 + 大 burst 就能跑满, 不需要 15s 高频.
# burst-size 200 关掉 40 条后的 30-60s 冷却, 避免一轮内被打断.
_RT_RESEARCH_FAST = (
    "--watch", "--resume", "--since-hours", "24",
    "--interval", "30",
    "--page-size", "100",
    "--throttle-base", "0.8", "--throttle-jitter", "0.5",
    "--burst-size", "200", "--daily-cap", "0",
)

SPECS: dict[str, CrawlerSpec] = {
    "alphapai":    CrawlerSpec("alphapai", "alphapai_crawl", {
        # --strict-today (2026-04-30): 配额完全留给当日 ≥ 北京 00:00 的新增,
        # 不扫昨夜 23:xx 残留. 跨午夜后 watch tick 重算 cutoff.
        "roadshow": tuple(a for a in _RT if a not in ("--since-hours", "24")) +
                    ("--strict-today", "--category", "roadshow"),
        "comment":  tuple(a for a in _RT if a not in ("--since-hours", "24")) +
                    ("--strict-today", "--category", "comment"),
        # report: --sweep-today 已经按 startDate=endDate=今天 在 list/v2 端过滤
        # (server-side), 严格今日语义已满足; --strict-today 在 sweep-today 路径
        # 会被 stop_dt=None 短路忽略, 留作 belt-and-suspenders.
        "report":   ("--watch", "--resume", "--strict-today",
                     "--interval", "180",
                     "--throttle-base", "1.5", "--throttle-jitter", "1.0",
                     "--category", "report", "--sweep-today",
                     "--page-size", "100"),
        # wechat 微信社媒爬取已停用 (2026-04-24) — 已入库保留, /data-sources 不再
        # 暴露启停按钮. 恢复时取消注释并保持与 crawler_monitor.ALL_SCRAPERS 对齐.
        # "wechat":   _RT + ("--category", "wechat"),
    }),
    "gangtise":    CrawlerSpec("gangtise", "gangtise", {
        # research 必须下 PDF — 列表页的 brief 只是观点摘要, 研报正文在 PDF 里.
        # 外资研报走 /storage/s3/download 绕过了 web 端的点数校验, 可以直接下.
        # research 用 15s tick (见 _RT_RESEARCH_FAST 注释); summary/chief 保持 60s.
        # 2026-04-22: page_size 40→100, burst 0→200 (轮询 7 分类时深度翻页
        # 不被 40-条 burst cooldown 打断; 配合 s3-bypass 能跑到 ~100 条/日).
        "summary":  _RT + ("--type", "summary",  "--skip-pdf",
                           "--page-size", "100",
                           "--burst-size", "200", "--daily-cap", "0"),
        "research": _RT_RESEARCH_FAST + ("--type", "research"),
        "chief":    _RT + ("--type", "chief",    "--skip-pdf",
                           "--page-size", "100",
                           "--burst-size", "200", "--daily-cap", "0"),
    }),
    "jinmen":      CrawlerSpec("jinmen", "jinmen", {
        # 2026-04-28: 从 _RT (1.5/1.0/burst 80, interval 60) 切到保守档位.
        # jinmen 历史被封控过 (账号锁 + 长冷却), 跟 AceCamp 一档对齐:
        #   interval 120s (实时档下限, 高于 60s 减一半 burst), base 2.5s/jitter 1.5s,
        #   burst 30 (40→25 收紧再放宽一点保留少量并发), 冷却 15-40s.
        # _RT 留给 alphapai/funda 等"压力大的高频源".
        "meetings": ("--watch", "--resume", "--since-hours", "24",
                     "--interval", "120",
                     "--throttle-base", "2.5", "--throttle-jitter", "1.5",
                     "--burst-size", "30",
                     "--burst-cooldown-min", "15", "--burst-cooldown-max", "40"),
        "reports":  ("--watch", "--resume", "--since-hours", "24",
                     "--interval", "120",
                     "--throttle-base", "2.5", "--throttle-jitter", "1.5",
                     "--burst-size", "30",
                     "--burst-cooldown-min", "15", "--burst-cooldown-max", "40",
                     "--reports"),
    }),
    "meritco":     CrawlerSpec("meritco", "meritco_crawl", {
        # type 2 (纪要 / 专业内容) + type 3 (久谦自研) 各起一条, 并行跑
        "t2": _RT + ("--type", "2"),
        "t3": _RT + ("--type", "3"),
    }),
    "funda":       CrawlerSpec("funda", "funda", {
        # 2026-04-24 funda-specific 反爬微调:
        # - 2026-04-23 21:08 earnings_report watcher 吃了一次 HTTP 401 (cookie 端点作用域问题)
        # - earnings_transcript 5/8/11 月财报峰值时, 分页深度 + top_dedup_id 早停会漏抓
        #   (实测 199 条 2024-05/2024-08/2025-02/2025-05/2025-11 漏损)
        # 三条 watcher:
        # - post: _RT 实时档 (60s, base 1.5s) — 低密度源 (30-200/天), 保持快速覆盖
        # - earnings_report: 间隔 90s, base 2.0 (略松), sweep-today 模式避免 top-id 早停
        # - earnings_transcript: 间隔 120s, base 2.2, sweep-today 模式
        # 两个 earnings 都走 --sweep-today 是因为它们的 list endpoint 接受
        # startDate/endDate 过滤 (见 memory `crawler_day_sweep_support`);
        # 这样每轮按当日日期扫完整 list, 不依赖 top_dedup_id, 根治财报季漏抓.
        "post":                _RT + ("--category", "post"),
        "earnings_report":     ("--watch", "--resume", "--since-hours", "24",
                                "--interval", "90",
                                "--throttle-base", "2.0", "--throttle-jitter", "1.2",
                                "--category", "earnings_report", "--sweep-today",
                                "--page-size", "100"),
        "earnings_transcript": ("--watch", "--resume", "--since-hours", "24",
                                "--interval", "120",
                                "--throttle-base", "2.2", "--throttle-jitter", "1.3",
                                "--category", "earnings_transcript", "--sweep-today",
                                "--page-size", "100"),
    }),
    # AceCamp realtime (2026-04-24 重整, 回应账号封控事故):
    # - detail 端点有 quota (VIP 团队金卡 balance:0 → ~12 次后返 10003/10040),
    #   list 端点无配额. realtime 只抓 list 摘要, 不触 detail, 让 quota 完全留给
    #   真正的用户点击 + backfill pacing 过的回填任务.
    # - scraper tripwire (_tripwire_record_detail) 在 detail 连续 15 次空壳时抛
    #   SessionDead, 配合这里的 --skip-detail 让 realtime 路径完全不触发它.
    # - 节奏: interval 120s (旧 60s 太紧), base 3.0s/jitter 2.0s (旧 1.5/1.0 太快),
    #   burst 20/10-25s 冷却. 跟 ANTIBOT.md realtime 默认 (base 1.5) 更保守 2×.
    # - 由于只拉 list, 20 条 burst 能覆盖整页 (AceCamp list 默认 per_page=20),
    #   够捕捉 2 min 内新发布条目.
    # opinions 保留 detail (内容很短 ~几十字, 不依赖独立 detail quota 池 — opinions
    #   的 opinion_info 和 articles 的 article_info 走不同端点, 实测不互相吃配额).
    "acecamp":     CrawlerSpec("acecamp", "AceCamp", {
        # 2026-04-25 (v2.2): --daily-cap 移除 — 实时档不靠数量闸, 靠
        # SoftCooldown (10003/10040 自动触发) + --skip-detail (detail 不触 quota).
        # 2026-04-28: 又紧一档 (3.0→3.5 base, 2.0→2.5 jitter, burst 20→15,
        # 冷却 15-40→25-60s). AceCamp 团队金卡封过一次, 用户明确要求"再小心一些".
        # 2026-04-28 (二次): 用户要求恢复 AceCamp 但速率降到事故时的 1/10
        #   事故时 base=2.5/jitter=1.5/burst=30/cap=500 → 1/10 = 25/15/3/50
        #   interval 也×10 (180→1800, 240→2400). 加 --daily-cap 50 兜住列表流量,
        #   即便 quota 真出事也不致于在一两分钟内灌满异常.
        # 2026-04-29: --skip-detail 移除. list-only 写库会写"付费内容提纲"作 stub
        #   (用户反馈"黄金再次新高的逻辑及后市展望"卡片), 现在 dump_article 在
        #   skip_detail 路径上强制不写, 配合这里去掉 flag 让 watcher 真正调 detail.
        #   detail quota 烧光由 _tripwire_record_detail (15 连空 SessionDead 退出)
        #   + SoftCooldown (10003/10040 自动 30min 静默) 兜住; SessionDead 现在会
        #   让 watcher 直接 sys.exit(2), 不再继续轮询灌空数据.
        "articles": ("--watch", "--resume", "--since-hours", "24",
                     "--interval", "1800",
                     "--throttle-base", "25.0", "--throttle-jitter", "15.0",
                     "--burst-size", "3",
                     "--burst-cooldown-min", "90",
                     "--burst-cooldown-max", "180",
                     "--daily-cap", "50",
                     "--type", "articles"),
        "opinions": ("--watch", "--resume", "--since-hours", "24",
                     "--interval", "2400",
                     "--throttle-base", "25.0", "--throttle-jitter", "15.0",
                     "--burst-size", "3",
                     "--burst-cooldown-min", "90",
                     "--burst-cooldown-max", "180",
                     "--daily-cap", "50",
                     "--type", "opinions"),
    }),
    "alphaengine": CrawlerSpec("alphaengine", "alphaengine", {
        # AlphaEngine 账号有**两种独立配额**, 都是账号级、按 userId 计数,
        # 无法靠 token 轮换 / header 变化绕过 (2026-04-22 实测):
        #   a) REFRESH_LIMIT — 列表端点 (streamSearch) 每日刷新次数上限.
        #      "基础刷新额度" Pro tier 实测 ~500/天. 4 watcher × 60s = 5760/天
        #      严重超额, 会被锁定 10+ 分钟.
        #   b) 权益额度 — /download/<id> PDF 下载, 每日 ~50 次.
        # 对策: 拉慢到每 20 min 一轮 (240/天), 4 分类并发下合计 ~960/天,
        # 但 `--resume` 增量模式下 95% 请求 hit_known=True 只消耗 1 次 list
        # 调用就返回 → 实际算下来贴着 quota 但不触顶. 配合 --since-hours 24
        # 每个 watcher 一天只抓出若干条新条目.
        # PDF 下载用独立的 `--backfill-pdfs` 模式 (见 scraper.py), 避开 list
        # 配额池.
        "summary":       ("--watch", "--resume", "--since-hours", "24",
                          "--interval", "1200",   # 20 min
                          "--throttle-base", "3", "--throttle-jitter", "2",
                          "--category", "summary", "--skip-pdf"),
        "china_report":  ("--watch", "--resume", "--since-hours", "24",
                          "--interval", "1200",
                          "--throttle-base", "3", "--throttle-jitter", "2",
                          "--category", "chinaReport", "--skip-pdf"),  # 先跳过 PDF, 靠 backfill 补
        "foreign_report":("--watch", "--resume", "--since-hours", "24",
                          "--interval", "1200",
                          "--throttle-base", "3", "--throttle-jitter", "2",
                          "--category", "foreignReport", "--skip-pdf"),
        # news (资讯) 永久停用 (2026-04-28). 4 watcher 共享同一 streamSearch
        # REFRESH_LIMIT 配额池 (~500/天, Pro tier), 资讯端密度极高 (24h 525 条)
        # 会把 summary/chinaReport/foreignReport 的额度挤掉. 用户判定: 占用就停.
        # 已入库 news_items collection 仅供查询, 不再增量抓取也不再 enrich.
        # "news":          ("--watch", "--resume", "--since-hours", "24",
        #                   "--interval", "1200",
        #                   "--throttle-base", "3", "--throttle-jitter", "2",
        #                   "--category", "news", "--skip-pdf"),
        # 配额绕过 worker — 使用 detail 端点 + 签名 COS URL, 完全绕过
        # list REFRESH_LIMIT 和 PDF 下载配额 (CRAWLERS.md §9.5.8 通用模式).
        # 每小时扫一轮, 把所有已入库但缺正文 / 缺 PDF 的条目补全. 即使 list
        # 全天被限流也能跑.
        "detail_enrich": ("--enrich-via-detail", "--enrich-watch",
                          "--category", "all",
                          "--interval", "3600",      # 1h between passes
                          "--throttle-base", "1.5", "--throttle-jitter", "1",
                          "--burst-size", "0",
                          # 2026-04-25 (v2.2): --daily-cap 移除, 靠节奏 + SoftCooldown
                          "--backfill-max", "100"),  # 业务参数: per-category cap per round
    }),
    "thirdbridge": CrawlerSpec("thirdbridge", "third_bridge",
                               # third_bridge 单 variant; WAF 敏感, interval 拉长
                               {"default": ("--watch", "--resume", "--interval", "1800",
                                            "--throttle-base", "4", "--throttle-jitter", "3")}),
    "semianalysis": CrawlerSpec("semianalysis", "semianalysis", {
        # 2026-04-25 (v2.2): --daily-cap 移除, 实时档不再数量闸.
        "default": ("--watch", "--resume", "--since-hours", "72",
                    "--interval", "1800",
                    "--throttle-base", "3.0", "--throttle-jitter", "2.0",
                    "--burst-size", "30"),
    }),

    # ─── IR Filings (2026-04-28) ────────────────────────────────────────
    # Each source = single variant `default`. Args don't use the `_RT` template
    # because IR scrapers don't speak the antibot v2 CLI; their throttling is
    # internal (env vars XXX_THROTTLE).
    "sec_edgar":  CrawlerSpec("sec_edgar", "sec_edgar", {
        "default": ("--watch", "--interval", "1800"),
    }),
    "hkex":       CrawlerSpec("hkex", "hkex", {
        "default": ("--watch", "--interval", "1800", "--days", "30"),
    }),
    "asx":        CrawlerSpec("asx", "asx", {
        "default": ("--watch", "--interval", "1800"),
    }),
    "tdnet":      CrawlerSpec("tdnet", "tdnet", {
        "default": ("--watch", "--interval", "600"),
    }),
    "edinet":     CrawlerSpec("edinet", "edinet", {
        # Requires Subscription-Key in crawl/edinet/credentials.json — scraper
        # exits with non-zero if missing; admin UI surfaces the error.
        "default": ("--watch", "--interval", "7200", "--days", "14"),
    }),
    "dart":       CrawlerSpec("dart", "dart", {
        # Requires crtfc_key in crawl/dart/credentials.json.
        "default": ("--watch", "--interval", "7200", "--days", "30"),
    }),

    # 微信公众号 (mp.weixin.qq.com) 直采 (2026-04-29).
    # 单 variant `default`, 跑 accounts.yaml 全表;白名单起步只放机器之心 1 个.
    # 反爆参数对齐 tmwgsicp/wechat-download-api 实测值: 3s base + 2s jitter,
    # daily-cap 500/天 (公众号管理员账号 ~4 天 session, 单号超 500/天有封号风险).
    # interval 600s (10 min) — 文章日均 ~5 篇, 10 min 一轮足够追实时.
    "wechat_mp":  CrawlerSpec("wechat_mp", "wechat_mp", {
        "default": ("--watch", "--resume",
                    "--interval", "600",
                    "--throttle-base", "3.0", "--throttle-jitter", "2.0",
                    "--burst-size", "30",
                    "--burst-cooldown-min", "30", "--burst-cooldown-max", "60",
                    "--daily-cap", "500"),
    }),
}


def _state_key(platform: str) -> str:
    return f"crawler:{platform}"


def _dir_path(platform: str) -> Path:
    parent = _EXTERNAL_DIR.get(platform, _CRAWL_DIR)
    return parent / SPECS[platform].dir_name


_VARIANT_LOG_NAME = {
    # 和 crawler_monitor.ALL_SCRAPERS 的第 3 元素对齐, 保证 monitor 能读到对应 log
    ("alphapai", "roadshow"):  "watch_roadshow.log",
    ("alphapai", "comment"):   "watch_comment.log",
    ("alphapai", "report"):    "watch_report.log",
    ("alphapai", "wechat"):    "watch_wechat.log",
    ("gangtise", "summary"):   "watch_summary.log",
    ("gangtise", "research"):  "watch_research.log",
    ("gangtise", "chief"):     "watch_chief.log",
    ("jinmen",   "meetings"):  "watch_meetings.log",
    ("jinmen",   "reports"):   "watch_reports.log",
    ("meritco",  "t2"):        "watch_type2.log",
    ("meritco",  "t3"):        "watch_type3.log",
    ("funda",    "post"):                "watch_post.log",
    ("funda",    "earnings_report"):     "watch_earnings_report.log",
    ("funda",    "earnings_transcript"): "watch_earnings_transcript.log",
    ("acecamp",  "articles"):  "watch_articles.log",
    ("acecamp",  "events"):    "watch_events.log",
    ("alphaengine", "summary"):        "watch_summary.log",
    ("alphaengine", "china_report"):   "watch_china_report.log",
    ("alphaengine", "foreign_report"): "watch_foreign_report.log",
    ("alphaengine", "news"):           "watch_news.log",
    ("alphaengine", "detail_enrich"):  "watch_detail_enrich.log",
    ("semianalysis", "default"):       "watch.log",
    ("wechat_mp",    "default"):       "watch.log",
}


def _log_path(platform: str, variant: str = "default") -> Path:
    """Per-variant log path (matches crawler_monitor.ALL_SCRAPERS 的 log_name)."""
    name = _VARIANT_LOG_NAME.get((platform, variant), "watch.log")
    p = _dir_path(platform) / "logs" / name
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def _cmd_matches_variant(platform: str, variant: str, cmdline: str) -> bool:
    """给定 /proc 的 cmdline, 判断是不是该 (platform, variant) 对应的进程.

    用 variant 在 SPECS 里的独特 flag 做匹配:
      alphapai / funda → `--category <name>`
      gangtise / acecamp / meritco → `--type <name>` (meritco 的 variant 键 t2/t3, cmd 里是 `--type 2` / `--type 3`)
      jinmen.reports → `--reports`
      jinmen.meetings → 不带 `--reports`
    """
    spec = SPECS.get(platform)
    if not spec:
        return False
    flags = spec.variants.get(variant)
    if not flags:
        return False
    # 取出 --category / --type 后面跟的值; --reports 是裸 flag
    tag_val = None
    for i, f in enumerate(flags):
        if f in ("--category", "--type") and i + 1 < len(flags):
            tag_val = (f, flags[i + 1])
            break
        if f == "--reports":
            tag_val = ("--reports", None)
            break
    if tag_val is None:
        # 没有分类 flag (e.g. jinmen meetings 默认), 取 "不带 --reports" 作为识别
        return "--reports" not in cmdline
    flag, val = tag_val
    if val is None:
        return flag in cmdline
    # 形如 "--type summary" 或 "--category roadshow"
    import re as _re
    return bool(_re.search(rf"{flag}\s+{_re.escape(val)}\b", cmdline))


def _pid_field(variant: str) -> str:
    return f"pid:{variant}"


def _started_field(variant: str) -> str:
    return f"started_at:{variant}"


def _pid_alive(pid: int) -> bool:
    """kill -0 doesn't actually signal; it just tests delivery permissions."""
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except (ProcessLookupError, PermissionError):
        return False


def _scan_live_scraper_pids(platform: str) -> list[dict[str, Any]]:
    """Walk /proc and return every python `scraper.py` process whose cwd is
    the platform's source dir. This is the ground truth for "is a crawler
    running" — Redis can drift if a process was spawned by a different
    orchestrator (crawler_monitor.start_all, run_all.sh, manual nohup,
    auto_login_runner auto-restart) and never registered here."""
    if platform not in SPECS:
        return []
    want_cwd = str(_dir_path(platform).resolve())
    found: list[dict[str, Any]] = []
    try:
        entries = os.listdir("/proc")
    except OSError:
        return []
    for name in entries:
        if not name.isdigit():
            continue
        pid = int(name)
        try:
            cwd = os.readlink(f"/proc/{pid}/cwd")
        except (OSError, FileNotFoundError):
            continue
        if cwd != want_cwd:
            continue
        try:
            with open(f"/proc/{pid}/cmdline", "rb") as f:
                cmdline = f.read().replace(b"\x00", b" ").decode("utf-8", errors="replace")
        except OSError:
            continue
        if "scraper.py" not in cmdline:
            continue
        # Process start time from /proc/<pid>/stat (field 22 = starttime in clock ticks
        # since boot). We don't need wall-clock precision — a nonzero value just lets
        # the UI show "running for X seconds" when Redis didn't register it.
        started_at: float | None = None
        try:
            st = os.stat(f"/proc/{pid}")
            started_at = st.st_mtime  # close enough for UI
        except OSError:
            pass
        found.append({
            "pid": pid,
            "cmdline": cmdline.strip(),
            "started_at": started_at,
        })
    return found


def _clean_env() -> dict[str, str]:
    """Same proxy-env-strip as auto_login_runner — CN CDN can't route through Clash."""
    blocked = {
        "http_proxy", "https_proxy", "all_proxy", "no_proxy",
        "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY",
    }
    return {k: v for k, v in os.environ.items() if k not in blocked}


async def _spawn_one(platform: str, variant: str, flags: tuple[str, ...]) -> int:
    """Spawn a single scraper process for one variant. Returns its PID."""
    script = _dir_path(platform) / "scraper.py"
    if not script.exists():
        raise FileNotFoundError(f"scraper missing: {script}")

    log_path = _log_path(platform, variant)
    cmd = [
        os.environ.get("CRAWLER_PYTHON", "/home/ygwang/miniconda3/envs/agent/bin/python"),
        "-u",
        str(script),
        *flags,
    ]
    # Per-variant log (ob 追加, 不同 variant 写不同文件 → monitor 各 sub-tab 读各自的)
    log_fd = open(log_path, "ab", buffering=0)
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        cwd=str(_dir_path(platform)),
        stdout=log_fd,
        stderr=log_fd,
        env=_clean_env(),
        start_new_session=True,
    )
    # Mark with a header line so we can tell which variant each log chunk belongs to.
    with open(log_path, "ab") as f:
        header = (f"\n\n=== [{platform}:{variant}] spawned pid={proc.pid} "
                  f"at {datetime_now()} cmd={' '.join(cmd)} ===\n").encode("utf-8")
        f.write(header)
    return proc.pid


def datetime_now() -> str:
    from datetime import datetime
    return datetime.utcnow().isoformat() + "Z"


async def start(redis: aioredis.Redis, platform: str, force: bool = False) -> dict[str, Any]:
    """Spawn every variant for `platform` that isn't already running. Returns
    the aggregate status dict (same shape as `status()`)."""
    if platform not in SPECS:
        raise ValueError(f"Unknown platform: {platform}")

    spec = SPECS[platform]
    # 平台级停爬闸门: crawl/<dir>/DISABLED 文件存在就拒绝拉起. 管理 UI 点
    # "启动" 会看到明确错误, scraper 自己 main() 里也有同样的检查做兜底
    # (防止 monitor / daily_catchup 等旁路拉起绕过这里).
    _disable_file = _CRAWL_DIR / spec.dir_name / "DISABLED"
    if _disable_file.exists():
        reason = ""
        try:
            reason = _disable_file.read_text(encoding="utf-8").strip()[:300]
        except Exception:
            pass
        return {
            "platform": platform,
            "disabled": True,
            "disable_file": str(_disable_file),
            "disable_reason": reason,
            "variants": {},
            "error": f"platform disabled by {_disable_file.name} — rm the file to re-enable",
        }
    key = _state_key(platform)
    existing = await status(redis, platform)

    # Drop any dead-PID Redis fields before spawning.
    for v in spec.variants:
        pid = existing["variants"].get(v, {}).get("pid") or 0
        if pid and not _pid_alive(pid):
            await redis.hdel(key, _pid_field(v), _started_field(v))

    errors: list[str] = []
    for variant, flags in spec.variants.items():
        # 每 variant 独立 log, 清掉旧的陈年错误让 monitor 的 health 重算
        if force:
            _log_path(platform, variant).write_text("", encoding="utf-8")

        # Re-read to pick up the cleanup we just did.
        pid_raw = await redis.hget(key, _pid_field(variant))
        if pid_raw and _pid_alive(int(pid_raw)) and not force:
            continue  # already running
        # If force and alive, stop first.
        if pid_raw and _pid_alive(int(pid_raw)) and force:
            try:
                os.killpg(int(pid_raw), signal.SIGTERM)
                await asyncio.sleep(0.5)
            except ProcessLookupError:
                pass
            await redis.hdel(key, _pid_field(variant), _started_field(variant))

        # force 模式: 还要杀掉 Redis 没记录但 /proc 里在跑的同 variant 进程
        # (e.g. crawler_monitor.start_all 之前拉起的). 按 cwd + --category/--type 过滤.
        if force:
            for live in _scan_live_scraper_pids(platform):
                cmd = live.get("cmdline", "")
                if _cmd_matches_variant(platform, variant, cmd):
                    try:
                        os.killpg(live["pid"], signal.SIGTERM)
                    except (ProcessLookupError, PermissionError):
                        pass
            await asyncio.sleep(0.5)

        try:
            pid = await _spawn_one(platform, variant, flags)
        except Exception as exc:
            errors.append(f"{variant}: {exc}")
            continue

        started_at = time.time()
        await redis.hset(key, mapping={
            "platform": platform,
            _pid_field(variant): str(pid),
            _started_field(variant): f"{started_at:.0f}",
            f"cmd:{variant}": " ".join(str(x) for x in flags),
            f"log_path:{variant}": str(_log_path(platform, variant)),
        })

        # Give 1s to verify it didn't die immediately.
        await asyncio.sleep(1.0)
        if not _pid_alive(pid):
            await redis.hdel(key, _pid_field(variant), _started_field(variant))
            tail = ""
            try:
                tail = _log_path(platform, variant).read_text(encoding="utf-8", errors="replace")[-800:]
            except Exception:
                pass
            errors.append(f"{variant} died immediately: {tail}")

    snap = await status(redis, platform)
    if errors and not snap["running"]:
        raise RuntimeError(" · ".join(errors))
    snap["spawn_errors"] = errors
    return snap


async def stop(redis: aioredis.Redis, platform: str) -> dict[str, Any]:
    """SIGTERM every variant, then SIGKILL after 5s grace."""
    if platform not in SPECS:
        raise ValueError(f"Unknown platform: {platform}")

    spec = SPECS[platform]
    key = _state_key(platform)
    killed: list[int] = []

    for variant in spec.variants:
        pid_raw = await redis.hget(key, _pid_field(variant))
        if not pid_raw:
            continue
        pid = int(pid_raw)
        if not _pid_alive(pid):
            await redis.hdel(key, _pid_field(variant), _started_field(variant))
            continue
        try:
            os.killpg(pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        killed.append(pid)

    # Wait up to 5s for all to die gracefully.
    for _ in range(25):
        if all(not _pid_alive(p) for p in killed):
            break
        await asyncio.sleep(0.2)
    for p in killed:
        if _pid_alive(p):
            try:
                os.killpg(p, signal.SIGKILL)
            except ProcessLookupError:
                pass

    # Clear all variant PIDs from Redis.
    for variant in spec.variants:
        await redis.hdel(key, _pid_field(variant), _started_field(variant))

    return {"stopped": True, "pids": killed}


async def status(redis: aioredis.Redis, platform: str) -> dict[str, Any]:
    """Aggregate status across all variants of a platform.

    Ground-truth order:
      1. Redis `pid:<variant>` — processes we spawned, first-class variants.
      2. `/proc/*/cwd` scan — catches processes spawned by other orchestration
         paths (crawler_monitor.start_all, run_all.sh, manual nohup,
         auto_login_runner auto-restart after QR login). Without this the UI
         reported "已停止" even when a healthy scraper was clearly running —
         the fix: if Redis has a dead PID and `/proc` has a live one in the
         same dir, adopt that PID and self-heal Redis.
    """
    if platform not in SPECS:
        raise ValueError(f"Unknown platform: {platform}")

    spec = SPECS[platform]
    key = _state_key(platform)
    data = await redis.hgetall(key)

    variants_out: dict[str, dict[str, Any]] = {}
    any_running = False
    earliest_start: float | None = None
    aggregate_pids: list[int] = []
    tracked_pids: set[int] = set()

    for variant in spec.variants:
        pid = int(data.get(_pid_field(variant), 0) or 0)
        started = float(data.get(_started_field(variant), 0) or 0)
        alive = _pid_alive(pid) if pid else False
        variants_out[variant] = {
            "pid": pid,
            "running": alive,
            "started_at": started or None,
            "uptime_s": int(time.time() - started) if started else 0,
        }
        if alive:
            any_running = True
            aggregate_pids.append(pid)
            tracked_pids.add(pid)
            if earliest_start is None or (started and started < earliest_start):
                earliest_start = started

    # Ground-truth fallback: processes running in the platform's dir that we
    # didn't track. Adopt them so the UI shows "运行中".
    external = [p for p in _scan_live_scraper_pids(platform) if p["pid"] not in tracked_pids]
    if external:
        # Heal: pick the first unclaimed variant slot and write Redis so future
        # /stop calls can kill the right PID.
        claimed = {v for v, info in variants_out.items() if info.get("running")}
        free_slots = [v for v in spec.variants if v not in claimed]
        for idx, ext in enumerate(external):
            slot = free_slots[idx] if idx < len(free_slots) else f"external_{idx}"
            variants_out[slot] = {
                "pid": ext["pid"],
                "running": True,
                "started_at": ext.get("started_at"),
                "uptime_s": int(time.time() - ext["started_at"]) if ext.get("started_at") else 0,
                "adopted": True,  # surfaced in the JSON for ops visibility
            }
            any_running = True
            aggregate_pids.append(ext["pid"])
            if ext.get("started_at") and (earliest_start is None or ext["started_at"] < earliest_start):
                earliest_start = ext["started_at"]
            # Persist into Redis so stop() can kill this PID too.
            try:
                await redis.hset(key, mapping={
                    "platform": platform,
                    _pid_field(slot): str(ext["pid"]),
                    _started_field(slot): f"{ext.get('started_at') or time.time():.0f}",
                    f"cmd:{slot}": ext.get("cmdline", ""),
                    "log_path": str(_log_path(platform)),
                })
            except Exception:
                pass  # best-effort; don't fail the status read on a Redis blip

    # Tail the shared log file — one copy is plenty for the UI.
    log_tail = ""
    log_path = _log_path(platform)
    if log_path.exists():
        try:
            raw = log_path.read_bytes()
            log_tail = raw[-2048:].decode("utf-8", errors="replace")
        except Exception:
            pass

    # Primary PID (oldest running) for UI convenience.
    primary_pid = aggregate_pids[0] if aggregate_pids else 0
    uptime_s = int(time.time() - earliest_start) if earliest_start else 0

    return {
        "platform": platform,
        "running": any_running,
        "pid": primary_pid,
        "variants": variants_out,
        "started_at": earliest_start,
        "uptime_s": uptime_s,
        "log_path": str(log_path),
        "log_tail": log_tail,
    }


async def start_all_if_healthy(redis: aioredis.Redis) -> dict[str, Any]:
    """Boot-time helper: for any platform whose credentials are healthy, make
    sure a crawler is running. Never restarts a running one."""
    # Import here to avoid a circular import at module load time.
    from backend.app.services import credential_manager as cm

    results: dict[str, Any] = {}
    for platform in SPECS:
        try:
            snap = await cm.status_with_health(platform)
        except Exception:
            continue
        if snap.health != "ok":
            continue
        try:
            results[platform] = await start(redis, platform)
        except Exception as exc:
            results[platform] = {"error": str(exc)}
    return results
