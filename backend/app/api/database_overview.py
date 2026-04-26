"""Admin-only database overview dashboard.

Aggregates current data volumes across every store the platform uses:
  - PostgreSQL: row counts for every application table, grouped by domain.
  - MongoDB:    document counts for each crawler platform's collections.
  - Redis:      total key count (DBSIZE).

Exposed at ``GET /api/admin/database-overview``.

Cost model
----------
- Postgres: tries ``pg_class.reltuples`` first (instant planner estimate);
  falls back to ``SELECT COUNT(*)`` for tables with fewer than 1k rows
  (where reltuples is often stale or -1 for never-analyzed tables). For
  large tables the estimate is marked ``approximate=True`` so the UI can
  show a "~" prefix.
- MongoDB: ``estimated_document_count()`` — O(1), reads collection metadata.
- Redis:   ``DBSIZE`` — O(1).
"""
from __future__ import annotations

import asyncio
import json
import logging
import subprocess
import time
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, Request
from motor.motor_asyncio import AsyncIOMotorClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from backend.app.config import Settings, get_settings
from backend.app.deps import get_current_user, get_db
from backend.app.models.user import User

logger = logging.getLogger(__name__)
router = APIRouter()


# ── PostgreSQL: semantic grouping of application tables ──────────────────
POSTGRES_GROUPS: list[tuple[str, list[tuple[str, str]]]] = [
    ("用户与权限", [
        ("users", "用户"),
        ("user_preferences", "偏好设置"),
        ("user_sources", "订阅源"),
        ("user_favorites", "收藏"),
        ("user_news_read", "已读记录"),
        ("api_keys", "开放 API key"),
    ]),
    ("自选与提醒", [
        ("watchlists", "自选列表"),
        ("watchlist_items", "自选标的"),
        ("alert_rules", "提醒规则"),
    ]),
    ("新闻分析流水线", [
        ("news_items", "原始新闻"),
        ("filter_results", "相关度判断"),
        ("analysis_results", "分析结果"),
        ("research_reports", "深度研究"),
        ("source_health", "源健康度"),
        ("signal_evaluations", "信号准确率评估"),
    ]),
    ("AI 助手", [
        ("chat_conversations", "会话"),
        ("chat_messages", "消息"),
        ("chat_model_responses", "模型响应"),
        ("chat_prompt_templates", "Prompt 模板"),
        ("chat_tracking_topics", "跟踪话题"),
        ("chat_tracking_alerts", "跟踪提醒"),
        ("chat_recommended_questions", "推荐提问"),
    ]),
    ("荐股评分", [
        ("stock_predictions", "预测记录"),
        ("prediction_edit_logs", "编辑日志"),
        ("prediction_evaluations", "评估结果"),
    ]),
    ("AlphaPai 镜像 (PG)", [
        ("alphapai_articles", "公众号文章"),
        ("alphapai_roadshows_cn", "A 股路演"),
        ("alphapai_roadshows_us", "美港股路演"),
        ("alphapai_comments", "点评"),
        ("alphapai_digests", "每日简报"),
        ("alphapai_sync_state", "同步状态"),
    ]),
    ("久谦镜像 (PG)", [
        ("jiuqian_forum", "论坛研报"),
        ("jiuqian_minutes", "会议纪要"),
        ("jiuqian_wechat", "公众号"),
        ("jiuqian_sync_state", "同步状态"),
    ]),
    ("系统运营", [
        ("token_usage", "LLM 成本记录"),
    ]),
]


# ── MongoDB: each crawler platform's collection layout ────────────────────
# Tuple: (platform_label, settings_uri_attr, settings_db_attr, [(collection, label), ...])
# Empty items list → auto-discover via list_collection_names().
MONGO_PLATFORMS: list[tuple[str, str, str, list[tuple[str, str]]]] = [
    ("AlphaPai (派派)", "alphapai_mongo_uri", "alphapai_mongo_db", [
        ("roadshows", "会议路演"),
        ("reports", "券商研报"),
        ("comments", "券商点评"),
        ("wechat_articles", "公众号文章"),
    ]),
    ("Jinmen (进门财经)", "jinmen_mongo_uri", "jinmen_mongo_db", [
        ("meetings", "会议纪要"),
        ("reports", "研报"),
        ("oversea_reports", "外资研报"),
    ]),
    ("Meritco (久谦中台)", "meritco_mongo_uri", "meritco_mongo_db", [
        ("forum", "论坛 (纪要/研报/调研)"),
    ]),
    ("Third Bridge (高临)", "thirdbridge_mongo_uri", "thirdbridge_mongo_db", [
        ("interviews", "专家访谈"),
    ]),
    ("Funda", "funda_mongo_uri", "funda_mongo_db", [
        ("posts", "研究文章"),
        ("earnings_reports", "财报 (8-K)"),
        ("earnings_transcripts", "业绩会逐字稿"),
        # sentiments 情绪因子故意隐藏 — 按产品要求不在看板上展示
    ]),
    ("Gangtise (岗底斯)", "gangtise_mongo_uri", "gangtise_mongo_db", [
        ("summaries", "纪要"),
        ("researches", "研报"),
        ("chief_opinions", "首席观点"),
    ]),
    # AceCamp — articles 单集合按 subtype 三分 (minutes/research/article),
    # 对齐前端侧边栏四类 (纪要 / 调研 / 文章 / 观点). events 集合已于
    # 2026-04-23 drop, 此处不再列出. 单行 articles 显示 collection 总量,
    # 四类细分在"回填进度"卡片里按 subtype filter 展示.
    ("AceCamp", "acecamp_mongo_uri", "acecamp_mongo_db", [
        ("articles", "纪要 / 调研 / 文章"),
        ("opinions", "观点"),
    ]),
    ("AlphaEngine", "alphaengine_mongo_uri", "alphaengine_mongo_db", [
        ("summaries", "纪要"),
        ("china_reports", "国内研报"),
        ("foreign_reports", "海外研报"),
        ("news_items", "资讯"),
    ]),
    ("SentimenTrader", "sentimentrader_mongo_uri", "sentimentrader_mongo_db", []),
    # Collection names are resolved dynamically at request time via
    # ``_scope_collections`` below so staging (APP_ENV=staging) sees its
    # ``stg_`` siblings instead of prod's data.
    ("User KB", "user_kb_mongo_uri", "user_kb_mongo_db", [
        ("__user_kb_docs__", "用户上传文档"),
        ("__user_kb_chunks__", "分片索引"),
        ("__user_kb_gridfs_files__", "GridFS 文件元数据"),
        ("__user_kb_gridfs_chunks__", "GridFS 二进制数据"),
    ]),
]


# Sentinel → settings-attr mapping. Kept narrow on purpose: only shared-DB
# collections (User KB) are env-scoped at the collection level; crawler
# platforms already sit in their own per-platform DB so they don't need
# staging-vs-prod disambiguation.
_DYNAMIC_COLL_SENTINELS: dict[str, str] = {
    "__user_kb_docs__": "user_kb_docs_collection",
    "__user_kb_chunks__": "user_kb_chunks_collection",
}


def _scope_collections(
    settings: Settings,
    pairs: list[tuple[str, str]],
) -> list[tuple[str, str]]:
    """Replace sentinel collection names with their env-scoped values.

    ``GridFS`` buckets expand into two Mongo collections (``<bucket>.files``
    and ``<bucket>.chunks``) — Settings exposes the bucket name so we build
    the concrete collection strings here.
    """
    out: list[tuple[str, str]] = []
    for coll, label in pairs:
        if coll == "__user_kb_gridfs_files__":
            out.append((f"{settings.user_kb_gridfs_bucket}.files", label))
        elif coll == "__user_kb_gridfs_chunks__":
            out.append((f"{settings.user_kb_gridfs_bucket}.chunks", label))
        elif coll in _DYNAMIC_COLL_SENTINELS:
            out.append((getattr(settings, _DYNAMIC_COLL_SENTINELS[coll]), label))
        else:
            out.append((coll, label))
    return out


# Small threshold under which we always do exact COUNT(*), since reltuples
# is frequently -1 or 0 for tables that haven't been analyzed recently.
_EXACT_COUNT_THRESHOLD = 1000


@lru_cache(maxsize=32)
def _cached_mongo_client(uri: str) -> AsyncIOMotorClient:
    """Re-use one Motor client per distinct URI across requests.

    A fresh client per request spawns connection pools that never get reused;
    caching at module scope lets subsequent dashboard loads stay cheap.
    """
    return AsyncIOMotorClient(uri, serverSelectionTimeoutMS=3000, tz_aware=True)


async def _pg_table_count(db: AsyncSession, table: str) -> tuple[int | None, bool, str | None]:
    """Return (count, approximate, error_message).

    Uses ``pg_class.reltuples`` for large tables (instant); falls back to
    ``COUNT(*)`` when the estimate is missing or the table is small.
    """
    try:
        res = await db.execute(
            text("SELECT reltuples::bigint FROM pg_class WHERE oid = to_regclass(:t)"),
            {"t": table},
        )
        est = res.scalar()
    except Exception as e:  # noqa: BLE001 — surface any schema/permission error
        logger.warning("pg reltuples probe failed for %s: %s", table, e)
        return None, False, str(e)

    # to_regclass returns NULL when the table does not exist; the join then
    # yields no row and scalar() is None.
    if est is None:
        return None, False, "table not found"

    if est < 0 or est < _EXACT_COUNT_THRESHOLD:
        try:
            res = await db.execute(text(f'SELECT COUNT(*) FROM "{table}"'))
            return int(res.scalar() or 0), False, None
        except Exception as e:  # noqa: BLE001
            logger.warning("pg count failed for %s: %s", table, e)
            return None, False, str(e)
    return int(est), True, None


async def _mongo_coll_count(client: AsyncIOMotorClient, db_name: str, coll: str) -> tuple[int | None, str | None]:
    try:
        return await client[db_name][coll].estimated_document_count(), None
    except Exception as e:  # noqa: BLE001
        logger.warning("mongo count failed for %s.%s: %s", db_name, coll, e)
        return None, str(e)


# release_time_ms 未索引, 聚合对 ~1M 文档约 1-3s. TTL 缓存 5 min 让看板
# 10s 刷一次也不会每次都扫全表. key = "<db>.<coll>".
_TIMERANGE_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_TIMERANGE_TTL_S = 300.0

# 全量 /database-overview 响应缓存. 前端每 N 秒自动刷新, 但后端一次响应要
# 做几十次 Mongo count + aggregate, 没 cache 会把轮询打爆. TTL 略短于前端
# 刷新周期 → 每次 poll 至多命中 1 次新鲜计算, 不会堆积请求; 多客户端 poll
# 共享结果. 设 5s 刚好和前端 "10s 自动刷新" 错位, 保证每次 poll 都能拿到
# ≤5s 老的数据.
_OVERVIEW_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_OVERVIEW_TTL_S = 5.0
_OVERVIEW_LOCK: asyncio.Lock | None = None


def _overview_lock() -> asyncio.Lock:
    """Single-flight lock: only one background rebuild at a time."""
    global _OVERVIEW_LOCK
    if _OVERVIEW_LOCK is None:
        _OVERVIEW_LOCK = asyncio.Lock()
    return _OVERVIEW_LOCK


async def _mongo_coll_time_range(
    client: AsyncIOMotorClient, db_name: str, coll: str
) -> dict[str, Any]:
    """返回数据时间范围 + **日级真实覆盖率** + 最大连续缺口 + 日均入库量.

    字段:
        oldest_ms, newest_ms      — 首尾 release_time (ms)
        days_covered              — 至少有 1 条数据的日数
        days_expected             — 首尾跨度的日数 (含首尾)
        coverage_pct              — days_covered / days_expected, 0-100
        max_gap_days              — 最大连续无数据日数
        max_gap_from / max_gap_to — 那段空洞起止 YYYY-MM-DD
        docs_per_active_day       — 平均每个 active day 的入库条数 (暴露 "每日 1000+" 类的未达成)
        total_in_range            — 区间总条数 (和 count 一致, 用于 per-day 计算)

    未索引的聚合 + 5 min TTL 缓存."""
    key = f"{db_name}.{coll}"
    hit = _TIMERANGE_CACHE.get(key)
    now = time.time()
    if hit and now - hit[0] < _TIMERANGE_TTL_S:
        return hit[1]

    try:
        c = client[db_name][coll]
        pipeline = [
            {"$match": {"release_time_ms": {"$gt": 0}}},
            {"$group": {
                "_id": {
                    "$dateToString": {
                        "format": "%Y-%m-%d",
                        "date": {"$toDate": "$release_time_ms"},
                        "timezone": "Asia/Shanghai",
                    }
                },
                "n": {"$sum": 1},
                "oldest_ms": {"$min": "$release_time_ms"},
                "newest_ms": {"$max": "$release_time_ms"},
            }},
            {"$sort": {"_id": 1}},
        ]
        buckets = await c.aggregate(pipeline, allowDiskUse=True).to_list(None)
    except Exception as e:  # noqa: BLE001
        logger.debug("mongo time_range failed for %s.%s: %s", db_name, coll, e)
        _TIMERANGE_CACHE[key] = (now, {})
        return {}

    result: dict[str, Any] = {}
    if buckets:
        o_ms = min(b["oldest_ms"] for b in buckets)
        n_ms = max(b["newest_ms"] for b in buckets)
        total_in_range = sum(b["n"] for b in buckets)

        # 按日期字符串比较 (YYYY-MM-DD 可 lexsort) 算跨度的日数
        from datetime import date as _date
        first_d = _date.fromisoformat(buckets[0]["_id"])
        last_d = _date.fromisoformat(buckets[-1]["_id"])
        days_expected = (last_d - first_d).days + 1
        days_covered = len(buckets)
        present = {b["_id"] for b in buckets}

        # 最大连续缺口 (按日): 从 first_d 滚到 last_d, 记录最长空洞
        max_gap = 0
        gap_from = gap_to = ""
        cur = 0
        cur_start = ""
        from datetime import timedelta as _td
        cursor = first_d
        while cursor <= last_d:
            ds = cursor.isoformat()
            if ds not in present:
                if cur == 0:
                    cur_start = ds
                cur += 1
                if cur > max_gap:
                    max_gap = cur
                    gap_from = cur_start
                    gap_to = ds
            else:
                cur = 0
            cursor += _td(days=1)

        result = {
            "oldest_ms": int(o_ms),
            "newest_ms": int(n_ms),
            "days_covered": days_covered,
            "days_expected": days_expected,
            "coverage_pct": round(days_covered / days_expected * 100) if days_expected else 100,
            "max_gap_days": max_gap,
            "max_gap_from": gap_from or None,
            "max_gap_to": gap_to or None,
            "docs_per_active_day": round(total_in_range / days_covered, 1) if days_covered else 0,
            "total_in_range": total_in_range,
        }

    _TIMERANGE_CACHE[key] = (now, result)
    return result


# ── Backfill progress: read orchestrator state files + process status ────

# Repo root relative to this file: backend/app/api/database_overview.py → ../../..
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent
_BACKFILL_LOG_DIR = _REPO_ROOT / "logs" / "backfill_6months"
_BY_DATE_STATE = _BACKFILL_LOG_DIR / "by_date_state.json"
_OVERSEA_REFILL_STATE = _REPO_ROOT / "crawl" / "jinmen" / "_progress_oversea_summary.json"


def _pgrep_matches(pattern: str) -> list[int]:
    """Return PIDs matching cmd-line pattern (empty if none / pgrep missing)."""
    try:
        out = subprocess.run(
            ["pgrep", "-f", pattern],
            capture_output=True, text=True, timeout=3,
        )
        return [int(x) for x in out.stdout.strip().splitlines() if x.strip().isdigit()]
    except Exception:  # noqa: BLE001
        return []


def _list_active_backfill_scrapers() -> list[dict[str, Any]]:
    """Scan running scraper.py processes and classify each into (platform, category, date).

    Matches processes launched by the backfill orchestrators:
      - streaming: `scraper.py ... --stream-backfill`
      - date-sweep: `scraper.py ... --sweep-today --date YYYY-MM-DD`
      - gangtise PDF 长期补齐: `backfill_pdfs.py --loop ...`
      - gangtise 今日列表补齐: `backfill_today.py --type {all|research|summary|chief}`
    Live watchers (--watch --resume) are excluded.
    """
    import re
    try:
        ps_out = subprocess.run(
            ["ps", "-eo", "pid,cmd"],
            capture_output=True, text=True, timeout=3,
        ).stdout.splitlines()[1:]
    except Exception:  # noqa: BLE001
        return []

    # Query BackfillLock holders from Redis once, up front — used to de-dup
    # `backfill_pdfs.py` / `backfill_today.py` processes. When `start_all`
    # repeatedly spawns these alongside a still-running copy, the extras fail
    # to `BackfillLock.acquire()` and sit in `--loop` sleeping 600s between
    # retries. They're visible to `ps` but do no work — treat them as zombies
    # and drop from the dashboard so the "回填进度" card shows reality.
    _lock_holder_pids: dict[str, int] = {}
    try:
        import redis as _redis_mod  # noqa: WPS433
        import os as _os
        _rc = _redis_mod.Redis.from_url(
            _os.environ.get("REDIS_URL", "redis://127.0.0.1:6379/0"),
            socket_timeout=1, socket_connect_timeout=1, decode_responses=True,
        )
        for _lock_key in ("crawl:bf_lock:gangtise:default",
                          "crawl:bf_lock:gangtise:backfill_today_all"):
            v = _rc.get(_lock_key)
            if v and ":" in v:
                try:
                    _lock_holder_pids[_lock_key] = int(v.split(":", 1)[0])
                except (ValueError, TypeError):
                    pass
    except Exception:  # noqa: BLE001
        # Redis unavailable → fall through; all matches included (best-effort).
        pass

    results: list[dict[str, Any]] = []
    for line in ps_out:
        parts = line.strip().split(None, 1)
        if len(parts) < 2:
            continue
        pid_s, cmd = parts
        if not pid_s.isdigit():
            continue
        pid = int(pid_s)
        # Meritco bypass (detail-by-ID brute force). Identify by script name.
        if "meritco_crawl/bypass_backfill.py" in cmd or "bypass_backfill.py" in cmd and "meritco" in cmd:
            results.append({
                "pid": pid,
                "platform": "meritco",
                "category": "bypass",
                "date": None,
                "mode": "bypass",
            })
            continue
        # Gangtise PDF 长期补齐 daemon (backfill_pdfs.py --loop) — 独立进程, 不是 scraper.py.
        # cmd 行没路径, 按脚本名唯一匹配 (repo 内仅 crawl/gangtise/backfill_pdfs.py 使用此文件名).
        # 仅保留 BackfillLock 持有者; 同时在跑的其它副本 acquire 失败后 return-early
        # 每 --interval 秒重试, 是"等位僵尸", 不算活跃 scraper.
        if "backfill_pdfs.py" in cmd:
            holder = _lock_holder_pids.get("crawl:bf_lock:gangtise:default")
            # 若无法确定 holder (Redis 不可用) 就保留所有匹配 — 宁可重复不漏报.
            if holder is None or holder == pid:
                results.append({
                    "pid": pid,
                    "platform": "gangtise",
                    "category": "pdf_backfill",
                    "date": None,
                    "mode": "pdf_backfill",
                })
            continue
        # Gangtise 今日列表缺漏补齐 (backfill_today.py --type X). 同样按 lock holder 去重.
        # --type all 会 fan-out 到 research/summary/chief 三行, 下游有处理.
        if "backfill_today.py" in cmd and "backfill_today_reports.py" not in cmd:
            holder = _lock_holder_pids.get("crawl:bf_lock:gangtise:backfill_today_all")
            if holder is not None and holder != pid:
                continue
            m = re.search(r"--type\s+(\S+)", cmd)
            bt_type = m.group(1) if m else "all"
            results.append({
                "pid": pid,
                "platform": "gangtise",
                "category": f"backfill_today:{bt_type}",
                "date": None,
                "mode": "today_catchup",
            })
            continue
        if "scraper.py" not in cmd:
            continue
        if "--watch" in cmd:
            continue  # live watcher, not backfill
        is_stream = "--stream-backfill" in cmd
        date_match = re.search(r"--date\s+(\S+)", cmd)
        date = date_match.group(1) if date_match else None
        is_date_sweep = "--sweep-today" in cmd and date is not None
        if not (is_stream or is_date_sweep):
            continue
        # Parse category/type flag first, then infer platform from the value
        # (each category label is unique across platforms).
        category = None
        platform = None
        m = re.search(r"--category\s+(\S+)", cmd)
        if m:
            val = m.group(1)
            if val in ("wechat", "comment", "roadshow", "report", "post",
                       "earnings_report", "earnings_transcript"):
                platform = "alphapai" if val in ("wechat", "comment", "roadshow", "report") else "funda"
                category = val
            elif val in ("chinaReport", "foreignReport", "summary", "news",
                         "chinaReport,foreignReport", "all"):
                # alphaengine uses --category news/summary/chinaReport/foreignReport
                # note: "summary" is also used by gangtise via --type; we distinguish by
                # the presence of --type vs --category below.
                platform = "alphaengine"
                category = val
            elif val in ("articles", "opinions"):
                # AceCamp 也接受 --category (老版本) — 实际 scraper 用 --type.
                # "articles" 的 list endpoint 混合三个 subtype (minutes/research/article),
                # 一个 articles watcher 覆盖三行, 分发在下方 meritco-bypass 的同样逻辑里处理.
                platform = "acecamp"; category = val
            else:
                category = val
        else:
            m = re.search(r"--type\s+(\S+)", cmd)
            if m:
                val = m.group(1)
                if val in ("2", "3"):
                    platform = "meritco"; category = f"type{val}"
                elif val in ("summary", "research", "chief"):
                    platform = "gangtise"; category = val
                elif val in ("articles", "opinions"):
                    # AceCamp scraper 用 --type articles / --type opinions.
                    platform = "acecamp"; category = val
                else:
                    category = val
            else:
                # no --category/--type → likely jinmen
                if "--reports" in cmd:
                    platform = "jinmen"; category = "reports"
                elif "--oversea-reports" in cmd:
                    platform = "jinmen"; category = "oversea_reports"
                elif "jinmen" in cmd or is_stream or is_date_sweep:
                    platform = "jinmen"; category = "meetings"
        results.append({
            "pid": pid,
            "platform": platform,
            "category": category,
            "date": date,
            "mode": "date_sweep" if is_date_sweep else "streaming",
        })
    return results


def _load_json_safe(path: Path) -> dict[str, Any]:
    try:
        if path.exists():
            return json.loads(path.read_text())
    except Exception:  # noqa: BLE001
        pass
    return {}


async def _gather_ingest_rates(settings: Settings) -> dict[str, Any]:
    """Count docs whose crawled_at ∈ {last 60s, 5min, 1h, today} per platform.

    Uses `crawled_at` (UTC datetime stamped by scrapers at dump_one time) rather
    than release_time, so the rate reflects **actual ingestion** (catches up on
    old doc that just arrived). Cheap when `crawled_at` is indexed (most
    platforms are; falls back to COLLSCAN on platforms without index).
    """
    now_utc = datetime.now(timezone.utc)
    # 今日 = Asia/Shanghai 日历日 00:00 (用户在国内,以北京时间为准).
    # 之前写法 `today_utc_midnight - 8h` 在 UTC 16:00-23:59 (= CST 00:00-07:59 次日)
    # 这段时间会算出"前天 16:00 UTC"作为 today_cst 起点 — 比正确值早 24h,
    # 导致今日计数把昨天 CST 一整天的入库也算进来,看起来翻倍.
    _BJ = timezone(timedelta(hours=8))
    today_cst_start = now_utc.astimezone(_BJ).replace(
        hour=0, minute=0, second=0, microsecond=0,
    ).astimezone(timezone.utc)
    windows = {
        "last_60s":  now_utc - timedelta(seconds=60),
        "last_5m":   now_utc - timedelta(minutes=5),
        "last_1h":   now_utc - timedelta(hours=1),
        "today_cst": today_cst_start,
    }

    # Per-platform collections to aggregate. Keep light — biggest platforms only.
    platform_colls = {
        "alphapai": ("alphapai_mongo_uri", "alphapai_mongo_db",
                     ["wechat_articles", "comments", "reports", "roadshows"]),
        "jinmen":   ("jinmen_mongo_uri", "jinmen_mongo_db",
                     ["meetings", "reports", "oversea_reports"]),
        "meritco":  ("meritco_mongo_uri", "meritco_mongo_db", ["forum"]),
        "gangtise": ("gangtise_mongo_uri", "gangtise_mongo_db",
                     ["researches", "summaries", "chief_opinions"]),
        "alphaengine": ("alphaengine_mongo_uri", "alphaengine_mongo_db",
                        ["china_reports", "foreign_reports", "summaries", "news_items"]),
        "funda":    ("funda_mongo_uri", "funda_mongo_db",
                     ["posts", "earnings_reports", "earnings_transcripts"]),
        "acecamp":  ("acecamp_mongo_uri", "acecamp_mongo_db", ["articles", "events"]),
    }

    # Split each window into realtime (crawled within 24h of publish) vs backfill.
    # Missing release_time_ms (e.g. funda.sentiments) is classified as realtime.
    BACKFILL_DELTA_MS = 24 * 3600 * 1000

    def _mode_filter(since_utc: datetime, mode: str) -> dict:
        base = {"crawled_at": {"$gte": since_utc}}
        if mode == "realtime":
            return {"$and": [base, {"$or": [
                {"release_time_ms": {"$in": [None, 0]}},
                {"release_time_ms": {"$exists": False}},
                {"$expr": {"$lte": [
                    {"$subtract": [{"$toLong": "$crawled_at"}, {"$ifNull": ["$release_time_ms", 0]}]},
                    BACKFILL_DELTA_MS,
                ]}},
            ]}]}
        return {"$and": [base, {"release_time_ms": {"$nin": [None, 0]}}, {"$expr": {"$gt": [
            {"$subtract": [{"$toLong": "$crawled_at"}, {"$ifNull": ["$release_time_ms", 0]}]},
            BACKFILL_DELTA_MS,
        ]}}]}

    async def _one(platform: str, uri_attr: str, db_attr: str, colls: list[str]) -> dict[str, Any]:
        uri = getattr(settings, uri_attr, None)
        db_name = getattr(settings, db_attr, None)
        result = {k: 0 for k in windows}
        realtime = {k: 0 for k in windows}
        backfill = {k: 0 for k in windows}
        if not uri or not db_name:
            return {"platform": platform, **result, "realtime": realtime,
                    "backfill": backfill, "error": "config missing"}
        try:
            cli = _cached_mongo_client(uri)
            tasks = []
            task_meta: list[tuple[str, str]] = []
            for coll in colls:
                for wname, wstart in windows.items():
                    for mode in ("realtime", "backfill"):
                        tasks.append(cli[db_name][coll].count_documents(_mode_filter(wstart, mode)))
                        task_meta.append((wname, mode))
            counts = await asyncio.gather(*tasks, return_exceptions=True)
            for (wname, mode), cnt in zip(task_meta, counts):
                if isinstance(cnt, Exception):
                    continue
                n = int(cnt)
                if mode == "realtime":
                    realtime[wname] += n
                else:
                    backfill[wname] += n
                result[wname] += n
            return {"platform": platform, **result,
                    "realtime": realtime, "backfill": backfill, "error": None}
        except Exception as e:  # noqa: BLE001
            return {"platform": platform, **result,
                    "realtime": realtime, "backfill": backfill, "error": str(e)}

    per_platform = await asyncio.gather(*[
        _one(plat, attrs[0], attrs[1], attrs[2]) for plat, attrs in platform_colls.items()
    ])
    totals = {k: sum(p.get(k, 0) for p in per_platform) for k in windows}
    totals_realtime = {k: sum((p.get("realtime") or {}).get(k, 0) for p in per_platform) for k in windows}
    totals_backfill = {k: sum((p.get("backfill") or {}).get(k, 0) for p in per_platform) for k in windows}
    return {
        "totals": totals,
        "totals_realtime": totals_realtime,
        "totals_backfill": totals_backfill,
        "per_platform": per_platform,
        "generated_at": now_utc.isoformat(),
    }


async def _gather_backfill_progress(
    client_map: dict[str, AsyncIOMotorClient],
    settings: Settings,
) -> dict[str, Any]:
    """Build the 回填进度 payload: per-target coverage vs 6mo cutoff + jobs alive.

    Pulls info from three sources:
      1. Live process scan (pgrep) — orchestrator / by_date / refill alive state.
      2. State files — logs/backfill_6months/by_date_state.json, oversea progress.
      3. Live Mongo probe — per (platform, collection) count + oldest.
    """
    # Default cutoff = 6 months back (approx 183 days)
    now_utc = datetime.now(timezone.utc)
    cutoff_dt = now_utc - timedelta(days=183)
    cutoff_ms = int(cutoff_dt.timestamp() * 1000)

    # 1) Process liveness
    procs = {
        "streaming_orchestrator": bool(_pgrep_matches(r"backfill_6months\.py --cutoff")),
        "by_date_orchestrator":   bool(_pgrep_matches(r"backfill_by_date\.py")),
        "oversea_summary_refill": bool(_pgrep_matches(r"refetch_oversea_summaries\.py")),
        "alphaengine_scheduled":  bool(_pgrep_matches(r"sleep 1[0-9]{4,5}.*backfill_6months")),
    }
    # Per-process classification (shows which day each scraper is on)
    active_scrapers = _list_active_backfill_scrapers()
    streaming_children = sum(1 for p in active_scrapers if p["mode"] == "streaming")
    date_sweep_children = sum(1 for p in active_scrapers if p["mode"] == "date_sweep")
    bypass_children = sum(1 for p in active_scrapers if p["mode"] == "bypass")
    pdf_backfill_children = sum(1 for p in active_scrapers if p["mode"] == "pdf_backfill")
    today_catchup_children = sum(1 for p in active_scrapers if p["mode"] == "today_catchup")

    # Group by (platform, category) for per-target display.
    # Meritco bypass is cross-type — attach it to BOTH type2 and type3 targets
    # so both rows on the dashboard show the active bypass process.
    # AceCamp 的 `articles` 爬虫实际覆盖三个 subtype (minutes/research/article),
    # 同样 fan out 到三行, 否则 UI 上这三类会看起来"没人在爬".
    from collections import defaultdict
    active_by_target: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for p in active_scrapers:
        if not p["platform"] or not p["category"]:
            continue
        entry = {"pid": p["pid"], "date": p["date"], "mode": p["mode"]}
        if p["platform"] == "meritco" and p["category"] == "bypass":
            # Show the bypass scraper against both meritco/type2 and meritco/type3
            active_by_target["meritco/type2"].append(entry)
            active_by_target["meritco/type3"].append(entry)
            continue
        if p["platform"] == "acecamp" and p["category"] == "articles":
            # articles endpoint 混 minutes/research/article 三 subtype, 单 watcher 覆盖仨
            active_by_target["acecamp/minutes"].append(entry)
            active_by_target["acecamp/research"].append(entry)
            active_by_target["acecamp/article"].append(entry)
            continue
        if p["platform"] == "acecamp" and p["category"] == "opinions":
            # scraper 用 --type opinions, UI 目标 key 用 opinion (对齐侧边栏)
            active_by_target["acecamp/opinion"].append(entry)
            continue
        # gangtise PDF 长期补齐 只处理 research 集合的 PDF — attach 到 research tab
        if p["platform"] == "gangtise" and p["category"] == "pdf_backfill":
            active_by_target["gangtise/research"].append(entry)
            continue
        # gangtise 今日列表补齐: --type all 扇出到 3 行, 单类型只挂对应行
        if p["platform"] == "gangtise" and (p["category"] or "").startswith("backfill_today:"):
            bt_type = p["category"].split(":", 1)[1]
            if bt_type == "all":
                active_by_target["gangtise/research"].append(entry)
                active_by_target["gangtise/summary"].append(entry)
                active_by_target["gangtise/chief"].append(entry)
            elif bt_type in ("research", "summary", "chief"):
                active_by_target[f"gangtise/{bt_type}"].append(entry)
            continue
        key = f"{p['platform']}/{p['category']}"
        active_by_target[key].append(entry)
    # Sort each target's entries: date desc if present, else by pid
    for k, lst in active_by_target.items():
        lst.sort(key=lambda x: (x.get("date") or "", x.get("pid", 0)), reverse=True)

    # 2) Per-target coverage
    # Each target is a (platform, category, mongo_db, mongo_coll, mode, mongo_filter).
    # "mode": 'date_sweep' if the orchestrator uses per-day sweeps, else 'streaming'.
    # The list mirrors backfill_6months.TARGETS + backfill_by_date.TARGETS.
    # `db` field is display-only metadata — actual Mongo routing goes through
    # settings.{platform}_mongo_db (see line ~665 below). Keep this aligned
    # with the post-2026-04-23 remote DB names so the UI doesn't show stale labels.
    targets_spec: list[dict[str, Any]] = [
        # AlphaPai — 4 cats.
        {"platform": "alphapai", "category": "wechat",   "db": "alphapai-full", "coll": "wechat_articles",  "mode": "streaming"},
        {"platform": "alphapai", "category": "comment",  "db": "alphapai-full", "coll": "comments",          "mode": "streaming"},
        {"platform": "alphapai", "category": "roadshow", "db": "alphapai-full", "coll": "roadshows",         "mode": "streaming"},
        {"platform": "alphapai", "category": "report",   "db": "alphapai-full", "coll": "reports",           "mode": "date_sweep"},
        # Jinmen — meetings + reports + oversea_reports
        {"platform": "jinmen",   "category": "meetings",        "db": "jinmen-full",   "coll": "meetings",          "mode": "streaming"},
        {"platform": "jinmen",   "category": "reports",         "db": "jinmen-full",   "coll": "reports",           "mode": "streaming"},
        # oversea_reports 大量 doc 只爬到 metadata, PDF 没下 — 这些不能算"已入库".
        # 额外 filter: pdf_local_path 非空 AND pdf_size_bytes > 0, 才视为真实有用数据.
        {"platform": "jinmen",   "category": "oversea_reports", "db": "jinmen-full",   "coll": "oversea_reports",   "mode": "streaming",
         "filter": {"pdf_local_path": {"$nin": [None, ""]}, "pdf_size_bytes": {"$gt": 0}}},
        # Meritco — type2/3 共用 forum collection (DB 名=jiuqian-full,久谦 pinyin)
        {"platform": "meritco",  "category": "type2",    "db": "jiuqian-full",  "coll": "forum",             "mode": "streaming", "filter": {"type": 2}},
        {"platform": "meritco",  "category": "type3",    "db": "jiuqian-full",  "coll": "forum",             "mode": "streaming", "filter": {"type": 3}},
        # Gangtise — research + summary + chief
        {"platform": "gangtise", "category": "research", "db": "gangtise-full", "coll": "researches",        "mode": "streaming"},
        {"platform": "gangtise", "category": "summary",  "db": "gangtise-full", "coll": "summaries",         "mode": "streaming"},
        {"platform": "gangtise", "category": "chief",    "db": "gangtise-full", "coll": "chief_opinions",    "mode": "streaming"},
        # Funda — 4 collections
        {"platform": "funda",    "category": "post",                "db": "funda", "coll": "posts",                "mode": "streaming"},
        {"platform": "funda",    "category": "earnings_report",     "db": "funda", "coll": "earnings_reports",     "mode": "date_sweep"},
        {"platform": "funda",    "category": "earnings_transcript", "db": "funda", "coll": "earnings_transcripts", "mode": "date_sweep"},
        # AceCamp — articles 按 subtype 三分 (minutes/research/article) + opinions,
        # 对齐前端侧边栏四类: /acecamp/{minutes,research,article,opinion}.
        {"platform": "acecamp",  "category": "minutes",   "db": "acecamp",  "coll": "articles",    "mode": "streaming", "filter": {"subtype": "minutes"}},
        {"platform": "acecamp",  "category": "research",  "db": "acecamp",  "coll": "articles",    "mode": "streaming", "filter": {"subtype": "research"}},
        {"platform": "acecamp",  "category": "article",   "db": "acecamp",  "coll": "articles",    "mode": "streaming", "filter": {"subtype": "article"}},
        {"platform": "acecamp",  "category": "opinion",   "db": "acecamp",  "coll": "opinions",    "mode": "streaming"},
        # AlphaEngine — 延迟到 CST 00:10 启动 (quota 重置)
        {"platform": "alphaengine", "category": "chinaReport",    "db": "alphaengine", "coll": "china_reports",    "mode": "streaming"},
        {"platform": "alphaengine", "category": "summary",        "db": "alphaengine", "coll": "summaries",        "mode": "streaming"},
        {"platform": "alphaengine", "category": "news",           "db": "alphaengine", "coll": "news_items",       "mode": "streaming"},
        {"platform": "alphaengine", "category": "foreignReport",  "db": "alphaengine", "coll": "foreign_reports",  "mode": "streaming"},
        # foreign-website DB (foreign news / newsletter sites)
        {"platform": "semianalysis",    "category": "posts",    "db": "foreign-website", "coll": "semianalysis_posts",    "mode": "streaming"},
        {"platform": "the_information", "category": "articles", "db": "foreign-website", "coll": "theinformation_posts",  "mode": "streaming"},
    ]

    # Map of platform → (uri_attr, db_attr) from config
    platform_conn = {
        "alphapai":        ("alphapai_mongo_uri", "alphapai_mongo_db"),
        "jinmen":          ("jinmen_mongo_uri", "jinmen_mongo_db"),
        "meritco":         ("meritco_mongo_uri", "meritco_mongo_db"),
        "gangtise":        ("gangtise_mongo_uri", "gangtise_mongo_db"),
        "funda":           ("funda_mongo_uri", "funda_mongo_db"),
        "acecamp":         ("acecamp_mongo_uri", "acecamp_mongo_db"),
        "alphaengine":     ("alphaengine_mongo_uri", "alphaengine_mongo_db"),
        "semianalysis":    ("semianalysis_mongo_uri", "semianalysis_mongo_db"),
        "the_information": ("the_information_mongo_uri", "the_information_mongo_db"),
    }

    async def _probe_target(tgt: dict[str, Any]) -> dict[str, Any]:
        plat = tgt["platform"]
        conn_attrs = platform_conn.get(plat)
        if conn_attrs is None:
            return {**tgt, "count": None, "oldest_ms": None, "error": "no connection config"}
        uri = getattr(settings, conn_attrs[0], None)
        db_name = getattr(settings, conn_attrs[1], None)
        if not uri or not db_name:
            return {**tgt, "count": None, "oldest_ms": None, "error": "config missing"}
        try:
            cl = _cached_mongo_client(uri)
            col = cl[db_name][tgt["coll"]]
            flt = tgt.get("filter") or {}
            # ── 回填完毕标记 ──────────────────────────────────────────────
            # 每个 scraper DB 的 `_state` collection 可以写 `backfill_complete:<category>`
            # 文档 (见 funda 2026-04-24 的做法). 后端探到就在 target 里展开字段,
            # 前端可以显示 "✓ 回填完毕" 徽标.
            mark_id = f"backfill_complete:{tgt['category']}"
            mark_doc = await cl[db_name]["_state"].find_one({"_id": mark_id})
            backfill_complete = bool(mark_doc)
            backfill_completed_at = (mark_doc or {}).get("completed_at") if mark_doc else None
            backfill_method = (mark_doc or {}).get("method") if mark_doc else None
            # count
            if flt:
                cnt = await col.count_documents(flt)
            else:
                cnt = await col.estimated_document_count()
            # oldest
            q = {"release_time_ms": {"$gt": 0}}
            q.update(flt)
            oldest = await col.find_one(q, sort=[("release_time_ms", 1)], projection={"release_time_ms": 1})
            oldest_ms = int(oldest["release_time_ms"]) if oldest and oldest.get("release_time_ms") else None
            gap_days = None
            covered = False
            days_covered = None
            continuous_days = None
            continuous_oldest_ms = None
            if oldest_ms:
                # 历史最旧一条数据是否触达 6mo cutoff (孤立数据点也算).
                # 注意:这只是参考值, 真正的 "达标" 用 continuous_days >= 183 重算 (见下).
                gap_days = max(0, int((oldest_ms - cutoff_ms) / 86400000))
                days_covered = max(0, int((int(now_utc.timestamp() * 1000) - oldest_ms) / 86400000))

                # Continuous-days = days of *consecutive* backward coverage from
                # today. Defined as: N days such that for each day in
                # [today-1, today-2, ..., today-N], at least one doc exists.
                # Stops at the first day with 0 docs. This is the meaningful
                # "已回填 N 天" number — absolute oldest_ms is misleading when
                # isolated ancient docs exist (e.g. gangtise/chief 2023 outlier).
                try:
                    pipeline = [
                        {"$match": q},
                        {"$group": {"_id": {
                            "$dateToString": {
                                "format": "%Y-%m-%d",
                                "date": {"$toDate": "$release_time_ms"},
                                "timezone": "Asia/Shanghai",
                            },
                        }}},
                    ]
                    present: set = set()
                    async for d in col.aggregate(pipeline, allowDiskUse=True):
                        try:
                            present.add(datetime.strptime(d["_id"], "%Y-%m-%d").date())
                        except Exception:  # noqa: BLE001
                            pass
                    # Continuous-days = 从今天开始往回连续有数据的天数 (今天=第 1 天).
                    # 仅对密集源 (Jinmen/Meritco/AlphaPai 等 daily+ 类) 有意义;
                    # 对 Funda posts (周更) / earnings_reports (季度) 这种稀疏源,
                    # 这个数天然很小,不能作为回填进度的主指标 — 主指标改用下面的
                    # days_in_window (窗口内实际有数据的日期数).
                    today_bj = now_utc.astimezone(timezone(timedelta(hours=8))).date()
                    cursor_day = today_bj
                    consec_days = 0
                    while cursor_day in present:
                        consec_days += 1
                        cursor_day -= timedelta(days=1)
                    continuous_days = consec_days
                    if consec_days > 0:
                        co = datetime.combine(cursor_day + timedelta(days=1),
                                              datetime.min.time(), tzinfo=timezone.utc)
                        continuous_oldest_ms = int(co.timestamp() * 1000)

                    # ── 新的"达标/爬取完毕"双条件判定 ──────────────────────
                    # ① 深度达标: 最老一条数据触达 6 个月前 (oldest_ms <= cutoff_ms)
                    # ② 密度达标: [cutoff - 30d, cutoff + 30d] 这 ±1 个月的
                    #    边界窗口里,至少 3 个不同日期有数据.
                    #    → 排除"一条孤立的 2023 年老数据假装回填到位"的情况
                    #    (gangtise/chief 的 2023 outlier 就是典型反例)
                    # ③ 近期密度达标: 最近 183 天窗口内至少 10 个不同日期有数据
                    #    → 对稀疏源 (Funda posts 每周 1 篇 ≈ 26 天/183, earnings
                    #    season 每次 ≈ 30-60 天) 是个低门槛,不会误杀;但孤立
                    #    老数据 + 近期无爬虫活动的"僵尸数据"过不了.
                    cutoff_day = datetime.fromtimestamp(
                        cutoff_ms / 1000, tz=timezone.utc,
                    ).astimezone(timezone(timedelta(hours=8))).date()
                    boundary_from = cutoff_day - timedelta(days=30)
                    boundary_to = cutoff_day + timedelta(days=30)
                    boundary_days = sum(1 for d in present if boundary_from <= d <= boundary_to)
                    window_days = sum(1 for d in present if d >= cutoff_day)
                    depth_ok = oldest_ms <= cutoff_ms
                    density_boundary = boundary_days >= 3
                    density_window = window_days >= 10
                    covered = depth_ok and density_boundary and density_window
                except Exception as e:  # noqa: BLE001
                    logger.warning("continuous-days probe failed for %s/%s: %s",
                                   tgt["platform"], tgt["category"], e)
            tgt_key = f"{tgt['platform']}/{tgt['category']}"
            active_list = active_by_target.get(tgt_key, [])
            return {
                **tgt,
                "count": cnt,
                "oldest_ms": oldest_ms,
                "gap_days_to_cutoff": gap_days,
                "covered_6m": covered,
                "days_covered": days_covered,
                "continuous_days": continuous_days,
                "continuous_oldest_ms": continuous_oldest_ms,
                # 稀疏源友好的新指标
                "window_days": window_days if oldest_ms else None,
                "boundary_days": boundary_days if oldest_ms else None,
                "depth_ok": depth_ok if oldest_ms else None,
                "active_scrapers": active_list,
                # 人工标记的"回填完毕" — 来自 <scraper_db>._state.backfill_complete:<category>
                "backfill_complete": backfill_complete,
                "backfill_completed_at": backfill_completed_at,
                "backfill_method": backfill_method,
                "error": None,
            }
        except Exception as e:  # noqa: BLE001
            return {**tgt, "count": None, "oldest_ms": None, "error": str(e)}

    per_target = await asyncio.gather(*[_probe_target(t) for t in targets_spec])

    # Ingest rates (real-time throughput indicator)
    try:
        ingest_rates = await _gather_ingest_rates(settings)
    except Exception as e:  # noqa: BLE001
        logger.warning("ingest rates probe failed: %s", e)
        ingest_rates = {"error": str(e)}

    # 3) by_date state file (alphapai report day-level progress)
    by_date_state = _load_json_safe(_BY_DATE_STATE)
    by_date_summary: dict[str, Any] = {
        "total_days": 0, "done": 0, "skipped_coverage": 0, "error": 0, "pending": 0,
    }
    for k, v in by_date_state.items():
        by_date_summary["total_days"] += 1
        st = (v or {}).get("status", "")
        if st == "done":
            by_date_summary["done"] += 1
        elif st == "skipped_coverage":
            by_date_summary["skipped_coverage"] += 1
        elif st.startswith("error"):
            by_date_summary["error"] += 1
        else:
            by_date_summary["pending"] += 1

    # 4) oversea summary refill progress file
    oversea_progress = _load_json_safe(_OVERSEA_REFILL_STATE)

    return {
        "cutoff_date": cutoff_dt.strftime("%Y-%m-%d"),
        "cutoff_ms": cutoff_ms,
        "processes": procs,
        "children": {
            "streaming_scrapers": streaming_children,
            "date_sweep_scrapers": date_sweep_children,
            "bypass_scrapers": bypass_children,
            "total_scrapers": streaming_children + date_sweep_children + bypass_children,
        },
        "targets": per_target,
        "active_scrapers_flat": active_scrapers,
        "ingest_rates": ingest_rates,
        "by_date": by_date_summary,
        "oversea_summary_refill": {
            "last_processed_id": oversea_progress.get("last_processed_id"),
            "filled": oversea_progress.get("filled"),
            "still_empty": oversea_progress.get("still_empty"),
            "invalid": oversea_progress.get("invalid"),
            "error": oversea_progress.get("error"),
            "started_at": oversea_progress.get("started_at"),
            "updated_at": oversea_progress.get("updated_at"),
        },
    }


@router.get("/database-overview")
async def database_overview(
    request: Request,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    """Aggregate data-volume dashboard. Any authenticated user may read it.

    Cached 5s + single-flight so frontend auto-refresh (10s interval) and
    multi-user polling don't pile up expensive Mongo aggregations.
    """
    # Serve cached response if fresh.
    now_ts = time.time()
    hit = _OVERVIEW_CACHE.get("_")
    if hit and now_ts - hit[0] < _OVERVIEW_TTL_S:
        return {**hit[1], "cache_age_ms": int((now_ts - hit[0]) * 1000), "cached": True}

    # Single-flight: if another request is rebuilding, wait for it.
    async with _overview_lock():
        # Re-check after acquiring the lock (another waiter may have filled it).
        hit = _OVERVIEW_CACHE.get("_")
        if hit and time.time() - hit[0] < _OVERVIEW_TTL_S:
            return {**hit[1], "cache_age_ms": int((time.time() - hit[0]) * 1000), "cached": True}
        payload = await _build_overview(request, db, settings)
        _OVERVIEW_CACHE["_"] = (time.time(), payload)
        return {**payload, "cache_age_ms": 0, "cached": False}


async def _build_overview(
    request: Request,
    db: AsyncSession,
    settings: Settings,
) -> dict[str, Any]:
    """The actual dashboard assembly — wrapped by the caching endpoint above."""
    started_at = datetime.now(timezone.utc)

    # ── PostgreSQL ─────────────────────────────────────────────────────
    pg_groups: list[dict[str, Any]] = []
    pg_total = 0
    pg_approx_any = False
    for group_label, tables in POSTGRES_GROUPS:
        items = []
        for table, label in tables:
            count, approx, err = await _pg_table_count(db, table)
            if count is not None:
                pg_total += count
                pg_approx_any = pg_approx_any or approx
            items.append({
                "table": table,
                "label": label,
                "count": count,
                "approximate": approx,
                "error": err,
            })
        pg_groups.append({"group": group_label, "items": items})

    # ── MongoDB ────────────────────────────────────────────────────────
    mongo_platforms: list[dict[str, Any]] = []
    mongo_total = 0
    for platform_label, uri_attr, db_attr, explicit in MONGO_PLATFORMS:
        uri = getattr(settings, uri_attr, None)
        db_name = getattr(settings, db_attr, None)
        if not uri or not db_name:
            continue
        entry: dict[str, Any] = {
            "platform": platform_label,
            "database": db_name,
            "items": [],
            "error": None,
        }
        try:
            client = _cached_mongo_client(uri)
            if explicit:
                # Resolve env-scoped sentinels (User KB / Research Log) so
                # staging sees `stg_*` collections and prod sees the bare names.
                pairs = _scope_collections(settings, list(explicit))
            else:
                # Auto-discover; filter checkpoint / state collections that
                # begin with an underscore to avoid noise.
                names = await client[db_name].list_collection_names()
                pairs = [(n, n) for n in sorted(names) if not n.startswith("_")]
            # 并发查所有 collection 的 count + time_range+coverage, 降低整体延时.
            tasks = []
            for coll, _label in pairs:
                tasks.append(_mongo_coll_count(client, db_name, coll))
                tasks.append(_mongo_coll_time_range(client, db_name, coll))
            results = await asyncio.gather(*tasks, return_exceptions=False)
            coll_olds: list[int] = []
            coll_news: list[int] = []
            worst_cov = 100  # 最差覆盖率 (平台级聚合)
            worst_gap = 0    # 最大缺口 (日)
            for idx, (coll, label) in enumerate(pairs):
                count, err = results[idx * 2]
                tr: dict[str, Any] = results[idx * 2 + 1]
                if count is not None:
                    mongo_total += count
                if tr.get("oldest_ms") and tr.get("newest_ms"):
                    coll_olds.append(int(tr["oldest_ms"]))
                    coll_news.append(int(tr["newest_ms"]))
                if tr.get("coverage_pct") is not None:
                    worst_cov = min(worst_cov, int(tr["coverage_pct"]))
                if tr.get("max_gap_days"):
                    worst_gap = max(worst_gap, int(tr["max_gap_days"]))
                entry["items"].append({
                    "collection": coll,
                    "label": label,
                    "count": count,
                    "error": err,
                    "oldest_ms": tr.get("oldest_ms"),
                    "newest_ms": tr.get("newest_ms"),
                    "coverage_pct": tr.get("coverage_pct"),
                    "max_gap_days": tr.get("max_gap_days"),
                    "max_gap_from": tr.get("max_gap_from"),
                    "max_gap_to": tr.get("max_gap_to"),
                    "docs_per_active_day": tr.get("docs_per_active_day"),
                })
            # 平台级并集: 取各 coll 里最小的 oldest + 最大的 newest, 最差覆盖率, 最大 gap
            if coll_olds and coll_news:
                o_ms = min(coll_olds)
                n_ms = max(coll_news)
                entry["oldest_ms"] = o_ms
                entry["newest_ms"] = n_ms
                entry["span_days"] = max(1, int((n_ms - o_ms) / 86400000))
                entry["coverage_pct"] = worst_cov
                entry["max_gap_days"] = worst_gap
            else:
                entry["oldest_ms"] = None
                entry["newest_ms"] = None
                entry["span_days"] = None
                entry["coverage_pct"] = None
                entry["max_gap_days"] = None
        except Exception as e:  # noqa: BLE001
            logger.warning("mongo listing failed for %s: %s", platform_label, e)
            entry["error"] = str(e)
        mongo_platforms.append(entry)

    # ── Redis ──────────────────────────────────────────────────────────
    redis_info: dict[str, Any] = {"available": False, "keys": None, "error": None}
    redis = getattr(request.app.state, "redis", None)
    if redis is not None:
        try:
            redis_info["keys"] = int(await redis.dbsize())
            redis_info["available"] = True
        except Exception as e:  # noqa: BLE001
            redis_info["error"] = str(e)

    # ── Backfill progress ──────────────────────────────────────────────
    try:
        backfill = await _gather_backfill_progress({}, settings)
    except Exception as e:  # noqa: BLE001 — never let backfill probe break the dashboard
        logger.warning("backfill progress probe failed: %s", e)
        backfill = {"error": str(e)}

    elapsed_ms = (datetime.now(timezone.utc) - started_at).total_seconds() * 1000
    return {
        "generated_at": started_at.isoformat(),
        "elapsed_ms": round(elapsed_ms, 1),
        "backfill": backfill,
        "postgres": {
            "total": pg_total,
            "approximate": pg_approx_any,
            "groups": pg_groups,
        },
        "mongodb": {
            "total": mongo_total,
            "platforms": mongo_platforms,
        },
        "redis": redis_info,
    }
