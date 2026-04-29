#!/usr/bin/env python3
"""
爬虫总控监控: meritco + jinmen + alphapai

用法:
    python3 crawler_monitor.py                                   # 命令行 live 仪表盘 (默认)
    python3 crawler_monitor.py --web                             # HTTP 仪表盘 (默认端口 8090)
    python3 crawler_monitor.py --web --port 9000
    python3 crawler_monitor.py --json                            # 一次性打印 JSON 状态
    python3 crawler_monitor.py --push-feishu                     # 立即推送一次飞书卡片并退出
    python3 crawler_monitor.py --web --feishu-webhook https://...   # web 模式 + 整点推送

飞书 webhook 也可通过 env FEISHU_WEBHOOK 传入.

数据源:
  - MongoDB: meritco (forum/_state), jinmen (meetings/reports/_state), alphapai (4 collections/_state)
  - 进程: `ps -ef | grep scraper.py --watch`
  - 日志: <scraper_dir>/logs/watch.log  (meritco-type3 是 watch_type3.log)
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import threading
import time
import urllib.request
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

from pymongo import MongoClient, DESCENDING

ROOT = Path(__file__).resolve().parent
MONGO_URI = os.environ.get(
    "MONGO_URI",
    "mongodb://127.0.0.1:27018/",
)

# 2026-04-23 迁移: 本地 -> 远端 DB 名映射 (源端硬编码 "meritco" 等 → 远端 "jiuqian-full").
# SOURCES 里把 db 字段从源名映射到远端名, 同时 DB 里 collection 名不变.
DB_MAP_LOCAL_TO_REMOTE = {
    "alphapai":       "alphapai-full",
    "jinmen":         "jinmen-full",
    "meritco":        "jiuqian-full",
    "thirdbridge":    "third-bridge",
    "funda":          "funda",
    "gangtise":       "gangtise-full",
    "acecamp":        "acecamp",
    "alphaengine":    "alphaengine",
    "sentimentrader":  "funda",  # 合并到 funda.sentimentrader_indicators
    "semianalysis":    "foreign-website",  # 2026-04-24 迁出 funda → foreign-website.semianalysis_posts
    "the_information": "foreign-website",  # 2026-04-25 新平台, 落 foreign-website.theinformation_posts
}

# 加载 .env (可选)
try:
    for env_path in [ROOT.parent / ".env", ROOT / ".env"]:
        if env_path.exists():
            for line in env_path.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                k = k.strip()
                v = v.strip().strip('"').strip("'")
                if k and v and k not in os.environ:
                    os.environ[k] = v
except Exception:
    pass

FEISHU_WEBHOOK_ENV = os.environ.get("FEISHU_WEBHOOK") or os.environ.get("FEISHU_WEBHOOK_URL") or ""
FEISHU_APP_ID_ENV = os.environ.get("FEISHU_APP_ID", "")
FEISHU_APP_SECRET_ENV = os.environ.get("FEISHU_APP_SECRET", "")
FEISHU_RECEIVE_ID_ENV = os.environ.get("FEISHU_RECEIVE_ID", "")
FEISHU_RECEIVE_ID_TYPE_ENV = os.environ.get("FEISHU_RECEIVE_ID_TYPE", "chat_id")


# ---------------- 平台 / 子分类定义 ----------------

PLATFORMS = [
    {"key": "meritco",     "label": "meritco · 久谦",       "color": "#5c8cff"},
    {"key": "jinmen",      "label": "jinmen · 进门",         "color": "#5dd39e"},
    {"key": "alphapai",    "label": "alphapai · Alpha派",   "color": "#f0c674"},
    {"key": "thirdbridge", "label": "thirdbridge · 高临",  "color": "#c77dff"},
    {"key": "funda",       "label": "funda · Funda.ai",     "color": "#ff9f7a"},
    {"key": "gangtise",    "label": "Gangtise",              "color": "#6bcdff"},
    {"key": "acecamp",     "label": "AceCamp",               "color": "#ff6b9d"},
    {"key": "alphaengine", "label": "AlphaEngine · 阿尔法引擎", "color": "#b388ff"},
    {"key": "semianalysis","label": "SemiAnalysis",           "color": "#ffd166"},
    {"key": "the_information", "label": "The Information",    "color": "#7dd3fc"},
]

SOURCES = [
    # --- meritco: type=2 (专业内容 / 纪要) + type=3 (久谦自研) 拆两进程并行 ---
    {
        "platform": "meritco",
        "key": "meritco_t2",
        "label": "纪要 (type 2)",
        "db": "meritco",
        "collection": "forum",
        "state_id": "crawler_type2",
        "doc_filter": {"forum_type": 2},
        "log": ROOT / "meritco_crawl" / "logs" / "watch_type2.log",
        # --type 2 (独立) 或 --type 2,3 (合并模式, 兼容)
        "proc_match": r"meritco_crawl.*scraper\.py.*--type\s+2(?!\d)",
        "item_fields": ["title", "industry", "type", "release_time"],
    },
    {
        "platform": "meritco",
        "key": "meritco_t3",
        "label": "研究 (type 3)",
        "db": "meritco",
        "collection": "forum",
        "state_id": "crawler_type3",
        "doc_filter": {"forum_type": 3},
        "log": ROOT / "meritco_crawl" / "logs" / "watch_type3.log",
        "proc_match": r"meritco_crawl.*scraper\.py.*--type\s+3",
        "item_fields": ["title", "industry", "type", "release_time"],
    },
    # --- jinmen ---
    {
        "platform": "jinmen",
        "key": "jinmen_meetings",
        "label": "纪要",
        "db": "jinmen",
        "collection": "meetings",
        "state_id": "crawler",
        "doc_filter": {},
        "log": ROOT / "jinmen" / "logs" / "watch_meetings.log",
        # Meetings = scraper.py without any --reports / --oversea-reports flag
        "proc_match": r"jinmen.*scraper\.py(?!.*--reports)(?!.*--oversea-reports)",
        "item_fields": ["title", "organization", "release_time"],
    },
    {
        "platform": "jinmen",
        "key": "jinmen_reports",
        "label": "研报",
        "db": "jinmen",
        "collection": "reports",
        "state_id": "crawler_reports",
        "doc_filter": {},
        "log": ROOT / "jinmen" / "logs" / "watch_reports.log",
        # \s 前缀避免误吃 --oversea-reports (它前面是 -, 不是空格)
        "proc_match": r"jinmen.*scraper\.py.*\s--reports\b",
        "item_fields": ["title", "organization", "release_time"],
    },
    {
        "platform": "jinmen",
        "key": "jinmen_oversea_reports",
        "label": "外资研报 (实时)",
        "db": "jinmen",
        "collection": "oversea_reports",
        "state_id": "crawler_oversea_reports",
        "doc_filter": {},
        "log": ROOT / "jinmen" / "logs" / "watch_oversea_reports.log",
        "proc_match": r"jinmen.*scraper\.py.*--oversea-reports",
        "item_fields": ["title", "organization_name", "release_time"],
    },
    # --- alphapai ---
    {
        "platform": "alphapai",
        "key": "alphapai_roadshow",
        "label": "路演 (roadshow)",
        "db": "alphapai",
        "collection": "roadshows",
        "state_id": "crawler_roadshow",
        "doc_filter": {},
        "log": ROOT / "alphapai_crawl" / "logs" / "watch_roadshow.log",
        "proc_match": r"alphapai_crawl.*scraper\.py.*--category\s+roadshow",
        "item_fields": ["title", "date", "release_time"],
    },
    {
        "platform": "alphapai",
        "key": "alphapai_comment",
        "label": "券商点评 (comment)",
        "db": "alphapai",
        "collection": "comments",
        "state_id": "crawler_comment",
        "doc_filter": {},
        "log": ROOT / "alphapai_crawl" / "logs" / "watch_comment.log",
        "proc_match": r"alphapai_crawl.*scraper\.py.*--category\s+comment",
        "item_fields": ["title", "time", "release_time"],
    },
    {
        "platform": "alphapai",
        "key": "alphapai_report",
        "label": "券商研报 (report)",
        "db": "alphapai",
        "collection": "reports",
        "state_id": "crawler_report",
        "doc_filter": {},
        "log": ROOT / "alphapai_crawl" / "logs" / "watch_report.log",
        "proc_match": r"alphapai_crawl.*scraper\.py.*--category\s+report\b",
        "item_fields": ["title", "time", "release_time"],
    },
    {
        "platform": "alphapai",
        "key": "alphapai_wechat",
        "label": "社媒/微信 (wechat)",
        "db": "alphapai",
        "collection": "wechat_articles",
        "state_id": "crawler_wechat",
        "doc_filter": {},
        "log": ROOT / "alphapai_crawl" / "logs" / "watch_wechat.log",
        "proc_match": r"alphapai_crawl.*scraper\.py.*--category\s+wechat",
        "item_fields": ["title", "publishDate", "release_time"],
        # 永久停用 (见 ALL_SCRAPERS 旁的注释). 标 disabled=True 让平台健康聚合
        # 把这个 tab 当作"不参与健康统计"的归档视图, 否则 wechat 的 proc_alive=False
        # 会把整个 alphapai 平台健康度拖到 stopped (页面变红, 误以为整站挂了).
        "disabled": True,
    },
    # --- thirdbridge ---
    {
        "platform": "thirdbridge",
        "key": "thirdbridge_interviews",
        "label": "专家访谈",
        "db": "thirdbridge",
        "collection": "interviews",
        "state_id": "crawler_interviews",
        "doc_filter": {},
        "log": ROOT / "third_bridge" / "logs" / "watch.log",
        "proc_match": r"third_bridge.*scraper\.py",
        "item_fields": ["title", "release_time"],
        "time_field": "release_time",
    },
    # --- funda ---
    {
        "platform": "funda",
        "key": "funda_post",
        "label": "研究文章",
        "db": "funda",
        "collection": "posts",
        "state_id": "crawler_post",
        "doc_filter": {},
        "log": ROOT / "funda" / "logs" / "watch_post.log",
        "proc_match": r"funda.*scraper\.py.*--category\s+post\b",
        "item_fields": ["title", "release_time"],
        "time_field": "release_time",
    },
    {
        "platform": "funda",
        "key": "funda_earnings_report",
        "label": "8-K 业绩公告",
        "db": "funda",
        "collection": "earnings_reports",
        "state_id": "crawler_earnings_report",
        "doc_filter": {},
        "log": ROOT / "funda" / "logs" / "watch_earnings_report.log",
        "proc_match": r"funda.*scraper\.py.*--category\s+earnings_report\b",
        "item_fields": ["title", "release_time"],
        "time_field": "release_time",
    },
    {
        "platform": "funda",
        "key": "funda_earnings_transcript",
        "label": "财报电话会",
        "db": "funda",
        "collection": "earnings_transcripts",
        "state_id": "crawler_earnings_transcript",
        "doc_filter": {},
        "log": ROOT / "funda" / "logs" / "watch_earnings_transcript.log",
        "proc_match": r"funda.*scraper\.py.*--category\s+earnings_transcript",
        "item_fields": ["title", "release_time"],
        "time_field": "release_time",
    },
    # --- gangtise (open.gangtise.com) ---
    {
        "platform": "gangtise",
        "key": "gangtise_summary",
        "label": "纪要",
        "db": "gangtise",
        "collection": "summaries",
        "state_id": "crawler_summary",
        "doc_filter": {},
        "log": ROOT / "gangtise" / "logs" / "watch_summary.log",
        "proc_match": r"gangtise.*scraper\.py.*--type\s+summary",
        "item_fields": ["title", "source_name", "release_time"],
        "time_field": "release_time",
    },
    {
        "platform": "gangtise",
        "key": "gangtise_research",
        "label": "研报",
        "db": "gangtise",
        "collection": "researches",
        "state_id": "crawler_research",
        "doc_filter": {},
        "log": ROOT / "gangtise" / "logs" / "watch_research.log",
        "proc_match": r"gangtise.*scraper\.py.*--type\s+research",
        "item_fields": ["title", "organization", "rpt_type_name", "release_time"],
        "time_field": "release_time",
    },
    {
        "platform": "gangtise",
        "key": "gangtise_chief",
        "label": "首席观点",
        "db": "gangtise",
        "collection": "chief_opinions",
        "state_id": "crawler_chief",
        "doc_filter": {},
        "log": ROOT / "gangtise" / "logs" / "watch_chief.log",
        "proc_match": r"gangtise.*scraper\.py.*--type\s+chief",
        "item_fields": ["title", "organization", "analyst", "release_time"],
        "time_field": "release_time",
    },
    {
        "platform": "gangtise",
        "key": "gangtise_homepage",
        "label": "平台首页",
        "db": "gangtise",
        "collection": "homepage",
        "state_id": "",       # scraper_home.py 每轮覆盖, 没有 checkpoint
        "doc_filter": {},
        "log": ROOT / "gangtise" / "logs" / "watch_home.log",
        "proc_match": r"gangtise.*scraper_home\.py",
        "item_fields": ["label", "item_count"],
        "time_field": "fetched_at",
    },
    # gangtise_pdf_backfill 曾是一个单独的 "PDF 补齐" tab, 展示 researches
    # 集合里 pdf_size_bytes>0 的子集 —— 与 "研报" tab 数据完全重复, 只是
    # 换个 filter 看同一堆 doc. 2026-04-23 移除: UI 上没有独立意义, 反而误导
    # 用户以为它是一个独立的数据源. PDF 下载的实际工作由 backfill_pdfs.py 在
    # 后台完成(见 ALL_SCRAPERS), 不需要独立 tab 显示.
    # --- acecamp (api.acecamptech.com) ---
    {
        "platform": "acecamp",
        "key": "acecamp_minutes",
        "label": "纪要",
        "db": "acecamp",
        "collection": "articles",
        "state_id": "crawler_articles",
        "doc_filter": {"subtype": "minutes"},
        "log": ROOT / "AceCamp" / "logs" / "watch_articles.log",
        "proc_match": r"AceCamp.*scraper\.py.*--type\s+articles",
        "item_fields": ["title", "organization", "release_time"],
        "time_field": "release_time",
    },
    {
        "platform": "acecamp",
        "key": "acecamp_research",
        "label": "调研",
        "db": "acecamp",
        "collection": "articles",
        "state_id": "crawler_articles",
        "doc_filter": {"subtype": "research"},
        "log": ROOT / "AceCamp" / "logs" / "watch_articles.log",
        "proc_match": r"AceCamp.*scraper\.py.*--type\s+articles",
        "item_fields": ["title", "organization", "release_time"],
        "time_field": "release_time",
    },
    {
        "platform": "acecamp",
        "key": "acecamp_article",
        "label": "文章",
        "db": "acecamp",
        "collection": "articles",
        "state_id": "crawler_articles",
        "doc_filter": {"subtype": "article"},
        "log": ROOT / "AceCamp" / "logs" / "watch_articles.log",
        "proc_match": r"AceCamp.*scraper\.py.*--type\s+articles",
        "item_fields": ["title", "organization", "release_time"],
        "time_field": "release_time",
    },
    {
        "platform": "acecamp",
        "key": "acecamp_opinion",
        "label": "观点",
        "db": "acecamp",
        "collection": "opinions",
        "state_id": "crawler_opinions",
        "doc_filter": {},
        "log": ROOT / "AceCamp" / "logs" / "watch_opinions.log",
        "proc_match": r"AceCamp.*scraper\.py.*--type\s+opinions",
        "item_fields": ["title", "organization", "release_time"],
        "time_field": "release_time",
    },
    # --- alphaengine (www.alphaengine.top) ---
    {
        "platform": "alphaengine",
        "key": "alphaengine_summary",
        "label": "纪要",
        "db": "alphaengine",
        "collection": "summaries",
        "state_id": "crawler_summary",
        "doc_filter": {},
        "log": ROOT / "alphaengine" / "logs" / "watch_summary.log",
        "proc_match": r"alphaengine.*scraper\.py.*--category\s+summary\b",
        "item_fields": ["title", "organization", "release_time"],
        "time_field": "release_time",
    },
    {
        "platform": "alphaengine",
        "key": "alphaengine_china_report",
        "label": "国内研报",
        "db": "alphaengine",
        "collection": "china_reports",
        "state_id": "crawler_chinaReport",
        "doc_filter": {},
        "log": ROOT / "alphaengine" / "logs" / "watch_china_report.log",
        "proc_match": r"alphaengine.*scraper\.py.*--category\s+chinaReport",
        "item_fields": ["title", "organization", "release_time"],
        "time_field": "release_time",
    },
    {
        "platform": "alphaengine",
        "key": "alphaengine_foreign_report",
        "label": "海外研报",
        "db": "alphaengine",
        "collection": "foreign_reports",
        "state_id": "crawler_foreignReport",
        "doc_filter": {},
        "log": ROOT / "alphaengine" / "logs" / "watch_foreign_report.log",
        "proc_match": r"alphaengine.*scraper\.py.*--category\s+foreignReport",
        "item_fields": ["title", "organization", "release_time"],
        "time_field": "release_time",
    },
    {
        "platform": "alphaengine",
        "key": "alphaengine_news",
        "label": "资讯",
        "db": "alphaengine",
        "collection": "news_items",
        "state_id": "crawler_news",
        "doc_filter": {},
        "log": ROOT / "alphaengine" / "logs" / "watch_news.log",
        "proc_match": r"alphaengine.*scraper\.py.*--category\s+news\b",
        "item_fields": ["title", "organization", "release_time"],
        "time_field": "release_time",
    },
    {
        # Detail-endpoint enrichment worker — bypasses BOTH list REFRESH_LIMIT
        # AND PDF download quota. See §9.5.8 of CRAWLERS.md (list-vs-detail
        # 配额不对称). Fills in full content (正文) for all categories and
        # downloads PDFs via signed COS URLs for research types.
        "platform": "alphaengine",
        "key": "alphaengine_detail_enrich",
        "label": "正文/PDF 补全",
        "db": "alphaengine",
        "collection": "china_reports",
        "state_id": "crawler_chinaReport",
        "doc_filter": {"detail_enriched_at": {"$exists": True}},
        "log": ROOT / "alphaengine" / "logs" / "watch_detail_enrich.log",
        "proc_match": r"alphaengine.*scraper\.py.*--enrich-via-detail",
        "item_fields": ["title", "organization", "release_time", "pdf_size_bytes"],
        "time_field": "release_time",
    },
    # --- semianalysis (newsletter.semianalysis.com, Substack) ---
    {
        "platform": "semianalysis",
        "key": "semianalysis_posts",
        "label": "SemiAnalysis newsletter",
        "db": "semianalysis",            # remapped to "foreign-website" via DB_MAP below
        "collection": "semianalysis_posts",
        "state_id": "crawler_semianalysis",
        "state_collection": "_state_semianalysis",
        "doc_filter": {},
        "log": ROOT / "semianalysis" / "logs" / "watch.log",
        "proc_match": r"semianalysis.*scraper\.py",
        "item_fields": ["title", "organization", "release_time"],
        "time_field": "release_time",
    },
    # --- the_information (theinformation.com) — 2026-04-25 ---
    # 列表 SSR HTML 抓 6100+ 历史归档卡片 (title / authors / date / category /
    # excerpt / image), 匿名模式. 详情页全文在付费墙后, 默认 isContentPaywalled=True.
    # _id 用 slug, article_id 是 numeric (单调). DB 落 foreign-website.theinformation_posts.
    {
        "platform": "the_information",
        "key": "theinformation_articles",
        "label": "Articles",
        "db": "the_information",          # remapped to "foreign-website" via DB_MAP below
        "collection": "theinformation_posts",
        "state_id": "crawler_articles",
        "state_collection": "_state_theinformation",
        "doc_filter": {},
        "log": ROOT / "the_information" / "logs" / "watch.log",
        "proc_match": r"the_information.*scraper\.py",
        "item_fields": ["title", "category", "release_time"],
        "time_field": "release_time",
    },
]

# 2026-04-23 迁移后, 把 SOURCES 里所有源 DB 名重写成远端 DB 名.
# SOURCES 内部写的仍是旧名 ("alphapai"/"jinmen"/"meritco" 等), 本映射一次性改掉.
for _src in SOURCES:
    _src["db"] = DB_MAP_LOCAL_TO_REMOTE.get(_src["db"], _src["db"])

# 补默认 time_field, 没显式写的用平台约定
_DEFAULT_TIME_FIELD = {
    "meritco_t2": "release_time",
    "meritco_t3": "release_time",
    "jinmen_meetings": "release_time",
    "jinmen_reports": "release_time",
    "alphapai_roadshow": "publish_time",
    "alphapai_comment": "publish_time",
    "alphapai_report": "publish_time",
    "alphapai_wechat": "publish_time",
}
for _s in SOURCES:
    _s.setdefault("time_field", _DEFAULT_TIME_FIELD.get(_s["key"], "release_time"))


# ---------------- 工具函数 ----------------

def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def fmt_dt(dt: Any) -> str:
    if not dt:
        return "-"
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone().strftime("%Y-%m-%d %H:%M:%S")
    return str(dt)


def fmt_delta(dt: Any) -> str:
    if not isinstance(dt, datetime):
        return "-"
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    sec = (now_utc() - dt).total_seconds()
    if sec < 0:
        return "未来"
    if sec < 60:
        return f"{int(sec)}s 前"
    if sec < 3600:
        return f"{int(sec // 60)}m 前"
    if sec < 86400:
        return f"{int(sec // 3600)}h 前"
    return f"{int(sec // 86400)}d 前"


def list_scraper_processes() -> list[dict]:
    try:
        out = subprocess.check_output(["ps", "-eo", "pid,etime,cmd"], text=True)
    except Exception:
        return []
    procs = []
    for line in out.splitlines()[1:]:
        parts = line.strip().split(None, 2)
        if len(parts) < 3:
            continue
        pid, etime, cmd = parts
        # 匹配 scraper.py 和 scraper_home.py 两种 entry
        if "scraper.py" not in cmd and "scraper_home.py" not in cmd:
            continue
        if "grep" in cmd:
            continue
        if not (cmd.lstrip().startswith("python") or "python" in cmd.split()[0]):
            continue
        # realtime watcher: cmdline 有独立的 --watch 参数 (不是 --enrich-watch 这种复合词)
        # backfill/一次性: 没有 --watch
        toks = cmd.split()
        mode = "realtime" if "--watch" in toks else "backfill"
        procs.append({"pid": int(pid), "etime": etime, "cmd": cmd, "mode": mode})
    return procs


def proc_cwd(pid: int) -> str:
    try:
        return os.readlink(f"/proc/{pid}/cwd")
    except Exception:
        return ""


def find_process_for(source: dict, procs: list[dict]) -> dict | None:
    pat = re.compile(source["proc_match"])
    for p in procs:
        cwd = proc_cwd(p["pid"])
        full = f"{cwd} {p['cmd']}"
        if pat.search(full):
            return p
    return None


def tail_log(path: Path, lines: int = 6) -> list[str]:
    if not path.exists():
        return []
    try:
        with path.open("rb") as f:
            f.seek(0, 2)
            size = f.tell()
            block = min(size, 64 * 1024)
            f.seek(size - block)
            data = f.read().decode("utf-8", errors="replace")
    except Exception:
        return []
    return [ln for ln in data.splitlines() if ln.strip()][-lines:]


# weekend_backfill 日志目录; backfill 模式下 scraper 写到这里, 不写 watch.log
_BACKFILL_LOG_DIR = ROOT.parent / "logs" / "weekend_backfill"

# source.key → backfill 日志文件名 (gangtise 两个 tab 写到不同文件; meritco 两个 tab 共用一个)
_BACKFILL_LOG_MAP = {
    "meritco_t2": "meritco.log",
    "meritco_t3": "meritco_type3.log",
    "jinmen_meetings": "jinmen.log",
    "jinmen_reports": "jinmen_reports.log",
    "jinmen_oversea_reports": "jinmen_oversea_reports.log",
    "alphapai_roadshow": "alphapai.log",
    "alphapai_comment": "alphapai.log",
    "alphapai_report": "alphapai.log",
    "alphapai_wechat": "alphapai.log",
    "thirdbridge_interviews": "third_bridge.log",
    "funda_post": "funda.log",
    "funda_earnings_report": "funda.log",
    "funda_earnings_transcript": "funda.log",
    "gangtise_summary": "gangtise_summary.log",  # 目前跳过, 文件不存在 → 回退 watch.log
    "gangtise_research": "gangtise_res.log",
    "gangtise_chief": "gangtise_chf.log",
    "alphaengine_summary": "alphaengine_summary.log",
    "alphaengine_china_report": "alphaengine_chinaReport.log",
    "alphaengine_foreign_report": "alphaengine_foreignReport.log",
    "alphaengine_news": "alphaengine_news.log",
    "alphaengine_pdf_backfill": "alphaengine_pdf_backfill.log",
}


def effective_log_path(source: dict) -> Path:
    """Pick whichever log file was modified most recently: watch.log or weekend_backfill/<x>.log."""
    default_log = source["log"]
    bf_name = _BACKFILL_LOG_MAP.get(source.get("key"))
    if not bf_name:
        return default_log
    bf_log = _BACKFILL_LOG_DIR / bf_name
    if not bf_log.exists():
        return default_log
    if not default_log.exists():
        return bf_log
    try:
        if bf_log.stat().st_mtime > default_log.stat().st_mtime:
            return bf_log
    except Exception:
        pass
    return default_log


# Each entry: (pattern, label, is_regex).
# WARNING: bare digit strings like "401" match *any* line containing those
# digits (round counters `[轮次 1401]`, meeting/item IDs, timestamps). Keep
# 401/403 patterns anchored to obvious HTTP-status context.
# Backend auth-state polling. Caches the /api/data-sources listing because
# each probe hits a real HTTP endpoint (AceCamp users/me, Gangtise account
# probe, etc) and we don't want to hammer those every 10s refresh. Backend's
# own credential_manager caches for 300s already, but we add an extra
# in-process layer so multiple `collect()` calls in the same snapshot share.
_AUTH_CACHE: dict[str, Any] = {"at": 0.0, "data": {}}
_AUTH_TTL = 60.0


def _fetch_backend_auth_states() -> dict[str, dict]:
    """Run per-platform real-login probes via backend's credential_manager.

    Imports directly from trading_agent/backend so we don't need to solve the
    HTTP-auth handshake between monitor and backend. Each probe is async, so
    we spin an event loop for the batch. Cached 60s to cap the query volume
    on each platform's auth endpoint.

    Returns {platform_key: {"health": "ok"|"expired"|"unknown",
                             "health_detail": str, "health_checked_at": str}}
    Backend import fails → empty dict (monitor keeps working standalone).
    """
    now = time.time()
    if now - _AUTH_CACHE["at"] < _AUTH_TTL and _AUTH_CACHE["data"]:
        return _AUTH_CACHE["data"]

    try:
        # Add repo root so `backend.app.services...` resolves.
        _repo = str(Path(__file__).resolve().parent.parent)
        if _repo not in sys.path:
            sys.path.insert(0, _repo)
        from backend.app.services.credential_manager import (  # type: ignore
            status_with_health, PLATFORMS,
        )
    except Exception:
        _AUTH_CACHE["at"] = now
        _AUTH_CACHE["data"] = {}
        return {}

    async def _run():
        import asyncio as _asy
        results = await _asy.gather(
            *(status_with_health(k) for k in PLATFORMS),
            return_exceptions=True,
        )
        return results

    try:
        import asyncio as _asy
        try:
            # If we happen to be in an event loop (we're not, monitor is sync,
            # but be safe): fall back to running in a new thread.
            loop = _asy.get_running_loop()
            import concurrent.futures as _cf
            with _cf.ThreadPoolExecutor(1) as ex:
                results = ex.submit(_asy.run, _run()).result(timeout=30)
        except RuntimeError:
            results = _asy.run(_run())
    except Exception:
        _AUTH_CACHE["at"] = now
        _AUTH_CACHE["data"] = {}
        return {}

    out: dict[str, dict] = {}
    for st in results:
        if isinstance(st, Exception):
            continue
        key = getattr(st, "key", None)
        if not key:
            continue
        out[key] = {
            "health": getattr(st, "health", "unknown"),
            "health_detail": getattr(st, "health_detail", "") or "",
            "health_checked_at": getattr(st, "health_checked_at", None),
        }
    _AUTH_CACHE["at"] = now
    _AUTH_CACHE["data"] = out
    return out


ERROR_SIGNATURES: list[tuple[str, str, bool]] = [
    (r"HTTP\s*401\b", "auth/401", True),
    (r'\bcode["\']?\s*[:=]\s*["\']?401\b', "auth/401", True),
    (r"\b401\s+Unauthorized\b", "auth/401", True),
    ("Not authorized", "auth/401", False),
    ("TOKEN EXPIRED", "auth/expired", False),
    ("token 过期", "auth/expired", False),
    ("AuthExpired", "auth/expired", False),
    ("会话失效", "auth/expired", False),
    ("token is invalid", "auth/expired", False),
    # 进门：服务端清 session/挤下线,接口直接 500 + 业务错误
    ("用户信息不存在", "auth/session_dead", False),
    ("账号已在其他设备登录", "auth/session_dead", False),
    ("参数错误", "sign/参数错误", False),  # meritco X-My-Header 缺失
    ("列表失败", "list/failed", False),
    ("ERR:", "runtime/error", False),
]

# 命中这些 label 视为"掉线",即便进程活着也要报红;进程死了时覆盖 "进程未运行" 以给出真实原因
_OFFLINE_LABELS = {"auth/401", "auth/expired", "auth/session_dead"}

_ERROR_SIGS_COMPILED = [
    (re.compile(p) if is_re else None, p, label, is_re)
    for p, label, is_re in ERROR_SIGNATURES
]


_ROUND_TS_RE = re.compile(
    r"\[轮次\s*\d+\]\s+(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})"
)


def _last_round_ts(log_lines: list[str]) -> datetime | None:
    """从日志里扒最后一个 '[轮次 N] YYYY-MM-DD HH:MM:SS' 行的时间 (本地时间)."""
    last = None
    for ln in log_lines:
        m = _ROUND_TS_RE.search(ln)
        if m:
            try:
                last = datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
            except ValueError:
                pass
    return last


def classify_health(log_lines: list[str], proc_alive: bool,
                    interval_s: int = 300,
                    disabled: bool = False) -> dict:
    """根据日志尾 + 进程状态给出健康度.
    ok / warn / stopped / disabled (永久停用, 不参与平台健康聚合)
    """
    # 永久停用 (source.disabled=True) — 进程本来就不该在跑, 不报红.
    # 只在进程还活着时降级到 warn, 提醒操作员: 停用闸门被绕过了.
    if disabled:
        if proc_alive:
            return {"state": "warn", "reason": "已停用但进程仍在运行"}
        return {"state": "disabled", "reason": "永久停用 (只读归档)"}
    # 先扫 tail: 如果是"掉线"级别的错误(认证/session 死), 无论进程死活都报红并给出具体原因;
    # 进程死 + 非认证错误 → stopped 但 reason 比 "进程未运行" 更具体.
    # 只看最后 20 行 —— 跑了好几天的 watcher 日志里更早的 401 不代表现在还 401.
    tail_joined = "\n".join(log_lines[-20:])
    for compiled, pat, label, is_re in _ERROR_SIGS_COMPILED:
        hit = compiled.search(tail_joined) if is_re else (pat in tail_joined)
        if not hit:
            continue
        if label in _OFFLINE_LABELS:
            return {"state": "stopped", "reason": f"掉线·{label}"}
        if not proc_alive:
            return {"state": "stopped", "reason": f"进程已退出·{label}"}
        return {"state": "warn", "reason": label}

    if not proc_alive:
        return {"state": "stopped", "reason": "进程未运行"}

    # 用日志里最后的 "[轮次 N] ts" 判断新鲜度
    # 放宽到 24x interval (2h @ interval=300s) —— 慢平台有时整小时无新数据也正常,
    # 只要进程还在跑就不报 warn; 超过 2h 无轮次打印才算真正卡住.
    last_round = _last_round_ts(log_lines)
    if last_round is not None:
        age = (datetime.now() - last_round).total_seconds()
        if age > interval_s * 24:
            return {"state": "warn", "reason": f"已 {int(age/60)}m 无轮次"}

    return {"state": "ok", "reason": ""}


_TIMERANGE_CACHE: dict[str, tuple[float, dict]] = {}  # key → (ts, {oldest_ms, newest_ms, span_days})
_TIMERANGE_TTL = 120.0


def _compute_time_range(coll, doc_filter: dict) -> dict:
    """返回 {oldest_ms, newest_ms, span_days} · 未命中返回 {}.
    release_time_ms 未索引, 对 ~1M 文档约 0.5-2s, 所以 collect 层 TTL 缓存 120s."""
    base = dict(doc_filter)
    base["release_time_ms"] = {"$gt": 0}  # 过滤 None/0, 否则 $min 会取到 0
    try:
        cur = coll.find(base, {"release_time_ms": 1}).sort("release_time_ms", 1).limit(1)
        docs = list(cur)
        oldest_ms = docs[0]["release_time_ms"] if docs else None
        cur = coll.find(base, {"release_time_ms": 1}).sort("release_time_ms", -1).limit(1)
        docs = list(cur)
        newest_ms = docs[0]["release_time_ms"] if docs else None
    except Exception:
        return {}
    if not oldest_ms or not newest_ms:
        return {}
    span_days = max(1, int((newest_ms - oldest_ms) / 86400000))
    return {"oldest_ms": int(oldest_ms), "newest_ms": int(newest_ms), "span_days": span_days}


def _cached_time_range(source: dict, coll) -> dict:
    key = source["db"] + "." + source["collection"] + ":" + json.dumps(source["doc_filter"], sort_keys=True)
    hit = _TIMERANGE_CACHE.get(key)
    now = time.time()
    if hit and now - hit[0] < _TIMERANGE_TTL:
        return hit[1]
    tr = _compute_time_range(coll, source["doc_filter"])
    _TIMERANGE_CACHE[key] = (now, tr)
    return tr


def collect(source: dict, client: MongoClient, procs: list[dict]) -> dict:
    db = client[source["db"]]
    state_coll_name = source.get("state_collection", "_state")
    state = db[state_coll_name].find_one({"_id": source["state_id"]}) or {}
    total = db[source["collection"]].count_documents(source["doc_filter"])
    time_range = _cached_time_range(source, db[source["collection"]])

    # 今日新增: 按 release/publish 时间算 (字段不同平台不同)
    # ISO 日期字符串可直接用 $gte 前缀比较, "2026-04-17 14:00" >= "2026-04-17"
    # 部分平台 (e.g. alphapai) release_time 存 None, 只有 release_time_ms
    # 有效 → 必须 OR 上 ms 回退, 否则 dashboard 今日总是 0.
    today_str = datetime.now().strftime("%Y-%m-%d")
    _cst_midnight_ms = int(
        datetime.strptime(today_str, "%Y-%m-%d")
        .replace(tzinfo=timezone(timedelta(hours=8)))
        .timestamp() * 1000
    )
    time_field = source.get("time_field", "release_time")
    today_added = db[source["collection"]].count_documents({
        **source["doc_filter"],
        "$or": [
            {time_field: {"$gte": today_str}},
            {"release_time_ms": {"$gte": _cst_midnight_ms}},
        ],
    })

    proj = {f: 1 for f in source["item_fields"]}
    proj.update({"crawled_at": 1, "_id": 1})
    cur = db[source["collection"]].find(source["doc_filter"], proj)\
        .sort("crawled_at", DESCENDING).limit(5)
    recent = []
    for doc in cur:
        recent.append({
            "_id": doc.get("_id"),
            **{f: doc.get(f) for f in source["item_fields"]},
            "crawled_at": doc.get("crawled_at"),
        })

    acc_col = db["account"]
    acc_count = acc_col.estimated_document_count()
    acc_latest = acc_col.find_one(sort=[("updated_at", DESCENDING)]) or {}
    token_info = {
        "count": acc_count,
        "updated_at": acc_latest.get("updated_at"),
    }

    proc = find_process_for(source, procs)

    latest_crawled_at = recent[0]["crawled_at"] if recent else None

    log_path = effective_log_path(source)
    log_tail_50 = tail_log(log_path, lines=80)
    health = classify_health(log_tail_50, bool(proc),
                             disabled=bool(source.get("disabled")))

    # 内容空检测 (OTP 锁侦测): 只对 jinmen 纪要生效 —— 平台对 aiSummaryAuth=0
    # 的条目要求 WAF 短信 OTP, scraper 访问到会存下 stats 全 0 的壳.
    # 近 20 条里只要有 >=1 条正文为空, 就把 health 降成 warn, 让用户一眼看到
    # 该 scraper 已经"不能正常取到内容", 去浏览器过一次 OTP 把设备信任续上.
    if source.get("key") == "jinmen_meetings" and health.get("state") == "ok":
        try:
            sample = list(
                db["meetings"]
                .find({}, {"stats": 1, "crawled_at": 1})
                .sort("crawled_at", DESCENDING)
                .limit(20)
            )
            if len(sample) >= 5:
                empty = 0
                for d in sample:
                    st = d.get("stats") or {}
                    total = sum(int(st.get(k, 0) or 0) for k in
                               ("速览字数", "章节", "指标", "对话条数"))
                    if total == 0:
                        empty += 1
                if empty >= 1:
                    health = {
                        "state": "warn",
                        "reason": (f"近 {len(sample)} 条纪要有 {empty} 条正文为空 "
                                   f"(OTP 锁, 去浏览器过一次验证码)"),
                    }
        except Exception:
            pass

    return {
        "platform": source["platform"],
        "health": health,
        "key": source["key"],
        "label": source["label"],
        "db": source["db"],
        "collection": source["collection"],
        "disabled": bool(source.get("disabled")),
        "total": total,
        "today_added": today_added,
        "time_range": time_range,  # {oldest_ms, newest_ms, span_days} 或 {}
        "latest_crawled_at": latest_crawled_at,
        "state": {
            "in_progress": state.get("in_progress"),
            "last_processed_id": state.get("last_processed_id") or state.get("last_processed_roadshow_id"),
            "top_id": state.get("top_id") or state.get("top_roadshow_id") or state.get("top_dedup_id"),
            "last_run_end_at": state.get("last_run_end_at"),
            "last_processed_at": state.get("last_processed_at"),
            "last_run_stats": state.get("last_run_stats") or {},
        },
        "recent": recent,
        "token": token_info,
        "process": proc,
        "log_tail": tail_log(log_path, lines=6),
        "log_path": str(log_path),
    }


FEED_EXCLUDE_KEYS = {"alphapai_wechat"}
# 这些 tab 不进"最近入库流": 社媒每小时几十到上百条,
# 体量太大会淹没其他有价值的 tab. 用户仍可在平台卡片的
# tab 视图里看到, 只是全局流里不展示.


def recent_feed(client: MongoClient, limit: int = 30,
                mode: str | None = None) -> list[dict]:
    """聚合全平台最近入库条目, 按 crawled_at 倒序.

    mode:
        None       - 全部 (向后兼容)
        "realtime" - 只返回 release_time_ms 在最近 24h 内的条目 (= 实时 watcher 抓的新鲜内容)
        "backfill" - 只返回 release_time_ms 超过 24h 的条目 (= 历史回填)
    切分依据: 实时 watcher 用 --since-hours 24, 所以 release_time 距今 < 24h = 实时流;
    更老的只能是 backfill 进程拿到的 (不然 watcher 根本不会去抓)."""
    cutoff_ms = int((now_utc() - timedelta(hours=24)).timestamp() * 1000)

    def classify(d: dict) -> str:
        """根据 release_time_ms 回落到 release_time 字符串比较, 判定这条属于哪一流."""
        rms = d.get("release_time_ms")
        if isinstance(rms, (int, float)) and rms > 0:
            return "realtime" if rms >= cutoff_ms else "backfill"
        # 回退: release_time / publish_time 字符串 — 格式 "YYYY-MM-DD HH:MM" lexsort 安全
        rt = d.get("release_time") or d.get("publish_time") or ""
        if rt:
            cutoff_str = (now_utc() + timedelta(hours=8) - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M")
            return "realtime" if str(rt) >= cutoff_str else "backfill"
        # 完全没有 release_time 就保守当 realtime (watcher 优先, 不丢入流)
        return "realtime"

    all_items: list[dict] = []
    # 每个 source 先拉最近 20~40 条作为候选, 再全局排序取前 limit
    # 按 mode 过滤时, 拉宽候选到 60 避免回填流早期全被 realtime 填满而截断
    fetch_k = 60 if mode else 20
    for s in SOURCES:
        if s["key"] in FEED_EXCLUDE_KEYS:
            continue
        db = client[s["db"]]
        proj = {f: 1 for f in s["item_fields"]}
        proj.update({"crawled_at": 1, "_id": 1, "release_time_ms": 1})
        try:
            cur = (db[s["collection"]]
                   .find(s["doc_filter"], proj)
                   .sort("crawled_at", DESCENDING).limit(fetch_k))
        except Exception:
            continue
        for d in cur:
            if not d.get("crawled_at"):
                continue
            if mode and classify(d) != mode:
                continue
            short_label = re.sub(r"\s*\([^)]+\)\s*$", "", s["label"])
            # 平台中文标签
            plabel = next(
                (p["label"] for p in PLATFORMS if p["key"] == s["platform"]),
                s["platform"],
            )
            all_items.append({
                "platform": s["platform"],
                "platform_label": plabel,
                "tab_key": s["key"],
                "tab_label": short_label,
                "source_color": next(
                    (p["color"] for p in PLATFORMS if p["key"] == s["platform"]), "#888"
                ),
                "_id": d.get("_id"),
                "title": (d.get("title") or "")[:200],
                "extra": d.get("industry") or d.get("organization") or "",
                "release_time": d.get("release_time") or d.get("publish_time") or "",
                "crawled_at": d.get("crawled_at"),
            })
    all_items.sort(key=lambda x: x["crawled_at"], reverse=True)
    return all_items[:limit]


def snapshot(with_feed: bool = True, feed_limit: int = 30) -> dict:
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
    try:
        client.admin.command("ping")
        mongo_ok = True
    except Exception:
        mongo_ok = False
    procs = list_scraper_processes()
    rows = []
    feed: list[dict] = []
    if mongo_ok:
        for s in SOURCES:
            try:
                rows.append(collect(s, client, procs))
            except Exception as e:
                rows.append({
                    "platform": s["platform"], "key": s["key"],
                    "label": s["label"], "error": str(e),
                })
        if with_feed:
            try:
                feed = recent_feed(client, limit=feed_limit)
            except Exception:
                feed = []
    client.close()

    # 从 backend 拉 per-platform 认证状态 (users/me 级别的真实登陆校验).
    # backend 未运行就安静退到 {} — 监控不依赖它.
    auth_states = _fetch_backend_auth_states()

    # 按 platform 聚合
    platforms = []
    for p in PLATFORMS:
        tabs = [r for r in rows if r.get("platform") == p["key"]]
        total = sum(r.get("total", 0) for r in tabs if "error" not in r)
        today = sum(r.get("today_added", 0) for r in tabs if "error" not in r)
        running = next((r["process"] for r in tabs if r.get("process")), None)
        latest_dts = [r["latest_crawled_at"] for r in tabs
                      if "error" not in r and r.get("latest_crawled_at")]
        latest = max(latest_dts) if latest_dts else None

        # 平台级健康度 = (子分类里最差的 — 跳过永久停用 tab) + (backend 认证状态)
        # severity: stopped > warn > ok. disabled 不参与聚合 (归档视图).
        severity = {"stopped": 2, "warn": 1, "ok": 0, "disabled": -1}
        worst = {"state": "ok", "reason": ""}
        for r in tabs:
            if r.get("disabled"):
                continue  # 永久停用 tab 不拖平台红
            if "error" in r:
                worst = {"state": "stopped", "reason": r.get("error", "")}
                continue
            h = r.get("health") or {"state": "ok", "reason": ""}
            if h.get("state") == "disabled":
                continue
            if severity.get(h["state"], 0) > severity.get(worst["state"], 0):
                worst = h

        # 叠加 backend 认证状态 — severity 映射:
        #   expired    → stopped (红, 必须重登)
        #   anonymous  → warn    (橙, 降级访问, 不致命)
        #   ratelimited→ warn    (橙, 平台日限流, 自动恢复)
        #   ok/unknown → 不影响 severity
        bauth = auth_states.get(p["key"])
        auth_display = None
        if bauth:
            auth_display = {
                "health": bauth.get("health", "unknown"),
                "detail": bauth.get("health_detail", ""),
                "checked_at": bauth.get("health_checked_at"),
            }
            bh = bauth.get("health")
            if bh == "expired":
                reason = bauth.get("health_detail", "凭证失效").split("·")[0].strip()[:40]
                if severity["stopped"] > severity.get(worst["state"], 0):
                    worst = {"state": "stopped", "reason": f"auth/{reason or '凭证失效'}"}
            elif bh in ("anonymous", "ratelimited"):
                label = {"anonymous": "匿名访问", "ratelimited": "额度用尽"}[bh]
                reason = bauth.get("health_detail", label).split("·")[0].strip()[:40]
                if severity["warn"] > severity.get(worst["state"], 0):
                    worst = {"state": "warn", "reason": f"auth/{reason or label}"}

        # 平台级时间范围 = 各 tab 时间范围的并集 (取最小 oldest + 最大 newest)
        olds = [t["time_range"].get("oldest_ms") for t in tabs
                if isinstance(t.get("time_range"), dict) and t["time_range"].get("oldest_ms")]
        news = [t["time_range"].get("newest_ms") for t in tabs
                if isinstance(t.get("time_range"), dict) and t["time_range"].get("newest_ms")]
        plat_range = {}
        if olds and news:
            plat_range = {
                "oldest_ms": min(olds),
                "newest_ms": max(news),
                "span_days": max(1, int((max(news) - min(olds)) / 86400000)),
            }

        platforms.append({
            "key": p["key"],
            "label": p["label"],
            "color": p["color"],
            "total": total,
            "today_added": today,
            "process": running,
            "tab_count": len(tabs),
            "latest_crawled_at": latest,
            "time_range": plat_range,
            "health": worst,
            "auth": auth_display,
            "tabs": tabs,
        })

    # 进程按 mode 拆: realtime watcher vs backfill 一次性
    realtime_count = sum(1 for p in procs if p.get("mode") == "realtime")
    backfill_count = sum(1 for p in procs if p.get("mode") == "backfill")

    return {
        "generated_at": now_utc().isoformat(),
        "mongo_ok": mongo_ok,
        "mongo_uri": MONGO_URI,
        "process_count": len(procs),
        "realtime_count": realtime_count,
        "backfill_count": backfill_count,
        "platforms": platforms,
        "feed": feed,
    }


# ---------------- 飞书卡片推送 ----------------

FEISHU_HOST = "https://open.feishu.cn"


def _http(method: str, url: str, body: dict | None = None,
          headers: dict | None = None, timeout: float = 10.0) -> dict:
    """简单的 urllib 封装, 跳过系统代理走直连."""
    data = json.dumps(body, ensure_ascii=False).encode("utf-8") if body is not None else None
    h = {"Content-Type": "application/json; charset=utf-8"}
    if headers:
        h.update(headers)
    req = urllib.request.Request(url, data=data, headers=h, method=method)
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(req, timeout=timeout) as resp:
        text = resp.read().decode("utf-8", errors="replace")
    try:
        return json.loads(text)
    except Exception:
        return {"raw": text, "http_status": resp.status}


def build_card_body(snap: dict) -> dict:
    """构造 interactive 卡片 body (config/header/elements), 不含 msg_type 包装.
    webhook 模式外面包 {msg_type, card}; im/v1/messages 模式用 json.dumps(body) 作为 content.
    """
    now_local = datetime.now().strftime("%Y-%m-%d %H:%M")

    elements: list[dict] = []

    mongo_str = "✅ OK" if snap["mongo_ok"] else "❌ 不可用"
    elements.append({
        "tag": "div",
        "text": {
            "tag": "lark_md",
            "content": (
                f"**🕐 {now_local} 整点快照**\n"
                f"Mongo: {mongo_str} · 活跃 scraper 进程: **{snap['process_count']}**"
            ),
        },
    })
    elements.append({"tag": "hr"})

    for p in snap["platforms"]:
        today = p["today_added"]
        h = p.get("health") or {}
        if h.get("state") == "stopped":
            running_mark = "🔴 停止"
        elif h.get("state") == "warn":
            running_mark = f"🟡 异常({h.get('reason', '')})"
        else:
            running_mark = "🟢 运行"
        latest_str = fmt_delta(p["latest_crawled_at"]) if p["latest_crawled_at"] else "-"

        lines = []
        for t in p["tabs"]:
            short_label = re.sub(r"\s*\([^)]+\)\s*$", "", t["label"])  # 去掉 (type=2) / (roadshow) 等后缀
            if "error" in t:
                lines.append(f"• **{short_label}**  ❌ {t['error']}")
                continue
            today_hl = f"今日 **+{t['today_added']}**" if t["today_added"] else "今日 +0"
            tab_latest = fmt_delta(t.get("latest_crawled_at")) if t.get("latest_crawled_at") else "-"
            lines.append(
                f"• **{short_label}** · {today_hl} · 最近更新 {tab_latest}"
            )

        content = (
            f"**{p['label']}** {running_mark} · 今日 **+{today}** · 最近更新 {latest_str}\n"
            + "\n".join(lines)
        )

        elements.append({
            "tag": "div",
            "text": {"tag": "lark_md", "content": content},
        })
        elements.append({"tag": "hr"})

    if elements and elements[-1].get("tag") == "hr":
        elements.pop()

    return {
        "config": {"wide_screen_mode": True, "enable_forward": True},
        "header": {
            "title": {"tag": "plain_text", "content": "📊 爬虫监控 · 整点快照"},
            "template": "blue",
        },
        "elements": elements,
    }


def send_feishu_webhook(webhook: str, card_body: dict) -> dict:
    """自定义机器人 webhook 模式 (老方案)."""
    payload = {"msg_type": "interactive", "card": card_body}
    return _http("POST", webhook, payload)


class FeishuAppClient:
    """基于 App ID + App Secret 的机器人客户端 (lark-samples 方式).

    与 `robot_quick_start/python/api.py` 相同的认证 + 消息流, 但用 urllib 实现,
    避免新增依赖. 支持 im/v1/messages (发消息) + im/v1/chats (列可达群).
    """

    def __init__(self, app_id: str, app_secret: str):
        self.app_id = app_id
        self.app_secret = app_secret
        self._token = ""
        self._token_exp_ts = 0.0

    def _tenant_access_token(self) -> str:
        if self._token and time.time() < self._token_exp_ts - 60:
            return self._token
        url = f"{FEISHU_HOST}/open-apis/auth/v3/tenant_access_token/internal"
        resp = _http("POST", url, {"app_id": self.app_id, "app_secret": self.app_secret})
        if resp.get("code") != 0:
            raise RuntimeError(
                f"tenant_access_token 换取失败: code={resp.get('code')} "
                f"msg={resp.get('msg')} — 检查 FEISHU_APP_ID / FEISHU_APP_SECRET"
            )
        self._token = resp["tenant_access_token"]
        self._token_exp_ts = time.time() + int(resp.get("expire", 7200))
        return self._token

    def _auth_headers(self) -> dict:
        return {"Authorization": f"Bearer {self._tenant_access_token()}"}

    def list_chats(self, page_size: int = 50) -> dict:
        """列出机器人可访问的群 (im:chat 权限范围)."""
        url = f"{FEISHU_HOST}/open-apis/im/v1/chats?page_size={page_size}"
        return _http("GET", url, headers=self._auth_headers())

    def send_interactive(self, receive_id_type: str, receive_id: str,
                         card_body: dict) -> dict:
        url = f"{FEISHU_HOST}/open-apis/im/v1/messages?receive_id_type={receive_id_type}"
        body = {
            "receive_id": receive_id,
            "msg_type": "interactive",
            "content": json.dumps(card_body, ensure_ascii=False),
        }
        return _http("POST", url, body, headers=self._auth_headers())


def push_card(snap: dict, *,
              app_id: str = "",
              app_secret: str = "",
              receive_id_type: str = "chat_id",
              receive_id: str = "",
              webhook: str = "") -> dict:
    """统一入口: 优先 App credentials, 其次 webhook.

    返回 API 响应 dict. 若同时没配置任何方式, 抛 ValueError.
    """
    card_body = build_card_body(snap)
    if app_id and app_secret and receive_id:
        client = FeishuAppClient(app_id, app_secret)
        return client.send_interactive(receive_id_type or "chat_id", receive_id, card_body)
    if webhook:
        return send_feishu_webhook(webhook, card_body)
    raise ValueError(
        "未配置飞书推送: 需要 (FEISHU_APP_ID + FEISHU_APP_SECRET + FEISHU_RECEIVE_ID) "
        "或 FEISHU_WEBHOOK_URL"
    )


# 每个爬虫的重启配置: credentials 文件路径 + 启动参数
# token_field: credentials.json 里存 token 的字段名
# token_check: 简单校验 token 格式的函数
RESTART_CONFIG: dict[str, dict] = {
    "alphapai": {
        "dir": ROOT / "alphapai_crawl",
        "log": ROOT / "alphapai_crawl" / "logs" / "watch.log",
        "proc_match": r"alphapai_crawl.*scraper\.py",
        "args": ["--watch", "--resume", "--since-hours", "24", "--interval", "30", "--throttle-base", "1.5", "--throttle-jitter", "1.0", "--burst-size", "0", "--daily-cap", "0"],
        "creds_file": ROOT / "alphapai_crawl" / "credentials.json",
        "token_field": "token",
        "token_check": lambda s: s.startswith("eyJ") and s.count(".") >= 2,
        "desc": "AlphaPai JWT (localStorage USER_AUTH_TOKEN, 以 eyJ 开头)",
    },
    "meritco": {
        "dir": ROOT / "meritco_crawl",
        "log": ROOT / "meritco_crawl" / "logs" / "watch.log",
        "proc_match": r"meritco_crawl.*scraper\.py",
        "args": ["--watch", "--resume", "--since-hours", "24", "--interval", "30", "--throttle-base", "1.5", "--throttle-jitter", "1.0", "--burst-size", "0", "--daily-cap", "0",
                 "--type", "2,3"],
        "creds_file": ROOT / "meritco_crawl" / "credentials.json",
        "token_field": "token",
        "token_check": lambda s: len(s) >= 16 and len(s) <= 128,
        "desc": "Meritco 请求头的 token 字段 (32 位 hex)",
    },
    "thirdbridge": {
        "dir": ROOT / "third_bridge",
        "log": ROOT / "third_bridge" / "logs" / "watch.log",
        "proc_match": r"third_bridge.*scraper\.py",
        "args": ["--watch", "--resume", "--since-hours", "24", "--interval", "30", "--throttle-base", "1.5", "--throttle-jitter", "1.0", "--burst-size", "0", "--daily-cap", "0"],
        "creds_file": ROOT / "third_bridge" / "credentials.json",
        "token_field": "cookie",
        "token_check": lambda s: "AWSELBAuthSessionCookie" in s,
        "desc": "forum.thirdbridge.com 的整条 Cookie",
    },
    "funda": {
        "dir": ROOT / "funda",
        "log": ROOT / "funda" / "logs" / "watch.log",
        "proc_match": r"funda.*scraper\.py",
        "args": ["--watch", "--resume", "--since-hours", "24", "--interval", "30", "--throttle-base", "1.5", "--throttle-jitter", "1.0", "--burst-size", "0", "--daily-cap", "0"],
        "creds_file": ROOT / "funda" / "credentials.json",
        "token_field": "cookie",
        "token_check": lambda s: "session-token" in s,
        "desc": "funda.ai 的整条 Cookie (至少含 session-token=...)",
    },
    "gangtise": {
        "dir": ROOT / "gangtise",
        "log": ROOT / "gangtise" / "logs" / "watch.log",
        "proc_match": r"gangtise.*scraper\.py",
        # 2026-04-22: 去掉 --skip-pdf. 以前这个 flag 让 list watcher 不下 PDF,
        # 导致当日入库的 research 条目全都缺 PDF (backfill 兜底要等 10 min).
        # 改成 list watcher 每处理一条就顺手下 PDF (dump_research 里的路径).
        # backfill_pdfs.py 作为兜底 watcher 保留, 只负责补漏.
        "args": ["--watch", "--resume", "--since-hours", "24", "--interval", "30",
                 "--throttle-base", "1.5", "--throttle-jitter", "1.0"],
        "creds_file": ROOT / "gangtise" / "credentials.json",
        "token_field": "token",
        "token_check": lambda s: len(s) >= 10,
        "desc": "open.gangtise.com 请求头的 token 字段",
    },
    "jinmen": {
        "dir": ROOT / "jinmen",
        "log": ROOT / "jinmen" / "logs" / "watch.log",
        "proc_match": r"crawl/jinmen.*scraper\.py",
        "args": ["--watch", "--resume", "--since-hours", "24", "--interval", "30", "--throttle-base", "1.5", "--throttle-jitter", "1.0", "--burst-size", "0", "--daily-cap", "0"],
        "creds_file": ROOT / "jinmen" / "credentials.json",
        "token_field": "JM_AUTH_INFO",
        "token_check": lambda s: len(s) >= 100 and s.startswith("eyJ"),
        "desc": "brm.comein.cn localStorage 的 JM_AUTH_INFO (base64 JSON, 以 eyJ 开头)",
    },
    "acecamp": {
        "dir": ROOT / "AceCamp",
        "log": ROOT / "AceCamp" / "logs" / "watch.log",
        "proc_match": r"AceCamp.*scraper\.py",
        "args": ["--watch", "--resume", "--since-hours", "24", "--interval", "30",
                 "--throttle-base", "1.5", "--throttle-jitter", "1.0",
                 "--burst-size", "0", "--daily-cap", "0"],
        "creds_file": ROOT / "AceCamp" / "credentials.json",
        "token_field": "cookie",
        "token_check": lambda s: "user_token=" in s or "_ace_camp_tech_production_session=" in s,
        "desc": "AceCamp 整条 Cookie (含 user_token=...)",
    },
    "alphaengine": {
        "dir": ROOT / "alphaengine",
        "log": ROOT / "alphaengine" / "logs" / "watch.log",
        "proc_match": r"alphaengine.*scraper\.py",
        # Account-level REFRESH_LIMIT 实测 ~500 list calls/day. 用 20 min 间
        # 隔 (1200s) 保持在 quota 以内 — 每 category 72 calls/day, 4 category
        # 合计 288 calls/day, 贴着基础配额下限 (见 crawler_manager.py 注释).
        "args": ["--watch", "--resume", "--since-hours", "24", "--interval", "1200",
                 "--throttle-base", "3", "--throttle-jitter", "2",
                 "--burst-size", "0", "--daily-cap", "0",
                 "--skip-pdf"],  # PDF 走独立 pdf_backfill variant, 避开 list 配额
        "creds_file": ROOT / "alphaengine" / "credentials.json",
        "token_field": "token",
        "token_check": lambda s: s.startswith("eyJ") and s.count(".") >= 2,
        "desc": "AlphaEngine JWT (localStorage `token`, 以 eyJ 开头). "
                "credentials.json 也存 refresh_token — scraper 每 6h 自动轮换 access token, "
                "无需人工重登 (除非 refresh_token 也过期, 30天有效).",
    },
    "semianalysis": {
        "dir": ROOT / "semianalysis",
        "log": ROOT / "semianalysis" / "logs" / "watch.log",
        "proc_match": r"semianalysis.*scraper\.py",
        "args": ["--watch", "--resume", "--since-hours", "72", "--interval", "1800",
                 "--throttle-base", "3.0", "--throttle-jitter", "2.0",
                 "--burst-size", "30", "--daily-cap", "200"],
        "creds_file": ROOT / "semianalysis" / "credentials.json",
        "token_field": "cookie",
        "token_check": lambda s: (s == "") or ("substack.sid=" in s),
        "desc": "SemiAnalysis Substack cookie (可选, 整串 document.cookie 含 substack.sid=...). "
                "留空即匿名模式 — free 内容可抓, paid 只拿 preview.",
    },
    "the_information": {
        "dir": ROOT / "the_information",
        "log": ROOT / "the_information" / "logs" / "watch.log",
        "proc_match": r"the_information.*scraper\.py",
        "args": ["--watch", "--resume", "--interval", "1800",
                 "--throttle-base", "5.0", "--throttle-jitter", "3.0",
                 "--burst-size", "20", "--daily-cap", "200"],
        "creds_file": ROOT / "the_information" / "credentials.json",
        "token_field": "cookie",
        "token_check": lambda s: (s == "") or ("session" in s.lower()) or ("cf_clearance" in s),
        "desc": "The Information cookie (可选, 整串 document.cookie 含 Rails session + cf_clearance). "
                "留空即匿名模式 — 可抓列表卡片 (title/authors/date/excerpt/image), "
                "全文体在付费墙后默认 isContentPaywalled=True.",
    },
    # 不是 scraper, 是 LLM 卡片摘要 worker (qwen-plus). 用 4-tuple 走自定义
    # script 路径; LLM key 全局来自 .env 不需要 creds_file. 仍然 register 进来
    # 是为了走 /api/start-all 的统一 spawn/kill + admin UI 状态.
    "local_ai_summary": {
        "dir": ROOT / "local_ai_summary",
        "log": ROOT / "local_ai_summary" / "logs" / "watch.log",
        "proc_match": r"local_ai_summary.*runner\.py",
        "args": [],
        "creds_file": None,
        "token_field": None,
        "token_check": lambda s: True,
        "desc": "qwen-plus 卡片摘要 worker; 凭 .env 里的 LLM_ENRICHMENT_API_KEY 工作, 无需独立凭证.",
    },
}


# 一键启动: 每平台一个 watcher 进程. meritco 单进程内轮询 type=2,3 (scraper.py 支持)
# 元组第 4 项可选: 自定义脚本文件名 (默认 scraper.py). 用于 scraper_home.py 这种独立 entry.
ALL_SCRAPERS: list[tuple] = [
    # 每平台拆成 per-category 独立进程并行. 避免 CATEGORY_ORDER 串行导致
    # 后面类别 (e.g. alphapai report+wechat) 拖慢前面类别 (roadshow, 纪要)
    # 的轮询频率.

    # meritco: type=2 (专业内容) + type=3 (久谦自研) 拆两进程并行
    # 2026-04-24 微调: meritco 是低频平台 (累计 forum 2.4k / 历年, 日均 <20 条/type),
    # _mode_args(realtime) 默认 30s 轮询对 meritco 远过激进 (风控风险 + AccountBudget
    # 浪费). 按 type 体量分层:
    #   - type=2 (专业内容, 日均 ~15-25): 180s 轮询, --since-hours 36 安全窗
    #   - type=3 (久谦自研,  日均 ~0-5 ): 360s 轮询, --since-hours 48 安全窗
    # --since-hours 覆盖实时 watcher gap-loss 模式 (feedback_realtime_backfill_pattern):
    # 高峰爆发时 top_id 早停会漏抓, 扩宽窗口后 dedup 天然幂等, 最多多几个 detail fetch.
    ("meritco",     ["--type", "2", "--interval", "180",
                     "--since-hours", "36"],                "watch_type2.log"),
    ("meritco",     ["--type", "3", "--interval", "360",
                     "--since-hours", "48"],                "watch_type3.log"),

    # jinmen: meetings (默认) + reports (--reports) + 外资研报 (--oversea-reports) 拆三进程
    # 2026-04-28: jinmen 三个 watcher 显式写死保守档, 跟 crawler_manager.SPECS
    # 对齐. 此前 [] 让 _mode_args(realtime) 把 base 1.5 / burst 80 灌过来,
    # 跟历史封控事故的诱因 (单 token 高 burst 高频) 一致, 风险高.
    # interval 120/120/180 (oversea 最低频), base 2.5/jitter 1.5, burst 30,
    # 跟 AceCamp (3.0/2.0/burst 20) 同档但 jinmen 账号活, 留少许速度.
    ("jinmen",      ["--interval", "120",
                     "--throttle-base", "2.5", "--throttle-jitter", "1.5",
                     "--burst-size", "30",
                     "--burst-cooldown-min", "15", "--burst-cooldown-max", "40"],
                                                           "watch_meetings.log"),
    ("jinmen",      ["--reports", "--interval", "120",
                     "--throttle-base", "2.5", "--throttle-jitter", "1.5",
                     "--burst-size", "30",
                     "--burst-cooldown-min", "15", "--burst-cooldown-max", "40"],
                                                           "watch_reports.log"),
    ("jinmen",      ["--oversea-reports", "--interval", "180",
                     "--throttle-base", "2.5", "--throttle-jitter", "1.5",
                     "--burst-size", "25",
                     "--burst-cooldown-min", "20", "--burst-cooldown-max", "50"],
                                                           "watch_oversea_reports.log"),

    # alphapai: 4 主分类 + 11 个子类 watcher 并行. 子类 watcher 用 --market-type 访问
    # SPA 上每个 tab 对应的过滤视图 (CDP 反解, 2026-04-23):
    #   roadshow: ashare=A股会议(10) / hk=港股会议(50) / us=美股会议(20) / web=网络资源(30)
    #             / ir=投资者关系(60) / hot=热门会议(70)
    #   report:   ashare=内资(marketType=21,usReport=false) / us=外资(30,true) / indep=独立(90,false)
    #   comment:  selected=干货点评(isSelected) / regular=日报周报(isRegular)
    # 每子类用自己的 `crawler_{cat}__{subtype}` checkpoint 独立推进 top_dedup_id,
    # 从根本解决 "高峰 burst 把新条目挤到 page-2+ 默认 watcher 早停漏抓" 的问题 —
    # 每个过滤视图体量小 1/3~1/6, 同样 60s 扫 page 1 能覆盖更长时间窗.
    # 主 watcher 仍保留 (默认视图是 union 兜底).
    ("alphapai",    ["--category", "roadshow"],                             "watch_roadshow.log"),
    ("alphapai",    ["--category", "roadshow", "--market-type", "ashare",
                     "--interval", "120"],                                  "watch_roadshow_ashare.log"),
    ("alphapai",    ["--category", "roadshow", "--market-type", "hk",
                     "--interval", "180"],                                  "watch_roadshow_hk.log"),
    ("alphapai",    ["--category", "roadshow", "--market-type", "us",
                     "--interval", "120"],                                  "watch_roadshow_us.log"),
    ("alphapai",    ["--category", "roadshow", "--market-type", "web",
                     "--interval", "240"],                                  "watch_roadshow_web.log"),
    ("alphapai",    ["--category", "roadshow", "--market-type", "ir",
                     "--interval", "300"],                                  "watch_roadshow_ir.log"),
    ("alphapai",    ["--category", "roadshow", "--market-type", "hot",
                     "--interval", "300"],                                  "watch_roadshow_hot.log"),

    ("alphapai",    ["--category", "comment"],                              "watch_comment.log"),
    ("alphapai",    ["--category", "comment", "--market-type", "selected",
                     "--interval", "180"],                                  "watch_comment_selected.log"),
    ("alphapai",    ["--category", "comment", "--market-type", "regular",
                     "--interval", "180"],                                  "watch_comment_regular.log"),

    # report: --sweep-today 每轮重算今日日期+完整扫描当日, 替代 top-pagination
    # 策略 (后者在高峰日 2000+研报时跟不上, 历史日均漏抓 1500+/天).
    # list/v2 仅 startDate/endDate 两个过滤字段生效 — 其余全部被服务端静默忽略.
    # interval 调到 180s: 每轮走 792 条/日 * 8 页 * 180s 间隔 = 负载适中.
    ("alphapai",    ["--category", "report", "--sweep-today",
                     "--page-size", "100",
                     "--interval", "180"],                                  "watch_report.log"),
    # 3 个 subtype watcher 走 --resume + top_dedup_id, 负责补盲区 (独立 checkpoint).
    ("alphapai",    ["--category", "report", "--market-type", "ashare",
                     "--interval", "180", "--page-size", "50"],             "watch_report_ashare.log"),
    ("alphapai",    ["--category", "report", "--market-type", "us",
                     "--interval", "180", "--page-size", "50"],             "watch_report_us.log"),
    ("alphapai",    ["--category", "report", "--market-type", "indep",
                     "--interval", "300", "--page-size", "50"],             "watch_report_indep.log"),

    # wechat 微信社媒爬取 **永久停用** (2026-04-24 确认) — 信号质量低, 用户明确要求
    # 长期关掉. 已入库数据 (44k+ docs) 保留仅供只读查询; scraper.py 的 CATEGORIES
    # 字典保留 wechat 条目只为历史 id 的 /detail 兼容, 不再拉新增.
    # 如果未来真要恢复: (1) alphapai_crawl/scraper.py::CATEGORY_ORDER 加回 "wechat",
    # (2) 这里加一行 ("alphapai", ["--category", "wechat"], "watch_wechat.log").
    # 默认**不要**打开 — 已明确不爬.

    # local_ai_summary: qwen-plus 卡片预览摘要 worker. 持仓股票相关、近 14 天、
    # 缺 native summary 的 doc 走 LLM 提炼成 100-150 字中文卡片摘要,
    # 写入 local_ai_summary.tldr → StockHub 列表卡片优先展示.
    # 第 4 元素 = 自定义 script (不是 scraper.py), 触发 crawler_monitor 的
    # 4-tuple spawn 路径 (line 1796).
    ("local_ai_summary",
        ["--watch", "--interval", "600", "--since-days", "14", "--max", "60"],
        "watch.log", "runner.py"),

    # funda: 3 分类并行
    ("funda",       ["--category", "post"],                "watch_post.log"),
    ("funda",       ["--category", "earnings_report"],     "watch_earnings_report.log"),
    ("funda",       ["--category", "earnings_transcript"], "watch_earnings_transcript.log"),

    # gangtise: 3 分类并行. research 走 ES-style from/size 分页 (SPA 抓包反解
    # 2026-04-22), 单 tick 能拉 100 条覆盖全日 ~1000 篇.
    # 反爬: 不再在 row 级覆盖 --throttle-*/--burst-*, 统一由 _mode_args 控制
    # (实时档保留 burst-size 80 防跑飞).
    ("gangtise",    ["--type", "summary",
                     "--page-size", "100"],                 "watch_summary.log"),
    ("gangtise",    ["--type", "research",
                     "--page-size", "100"],                 "watch_research.log"),
    ("gangtise",    ["--type", "chief",
                     "--page-size", "100"],                 "watch_chief.log"),

    # gangtise 主页快照 scraper_home.py — 跑独立脚本, 不要 --watch/--type 这些
    # 通用 flag (scraper_home 只支持 --watch/--interval). 每 10min 拉一轮.
    ("gangtise",    ["--watch", "--interval", "600"],       "watch_home.log",
                     "scraper_home.py"),

    # gangtise PDF 补齐 backfill_pdfs.py — 独立后台进程, 每 10min 扫一次
    # pdf_size_bytes<=0 && !=external_url 的条目, 批量下载. 跟 list watcher
    # 分进程跑, 不互相堵塞. --max 限制单轮 300 条防跑太久.
    ("gangtise",    ["--loop", "--interval", "600", "--max", "300",
                     "--sleep", "1.0"],                     "watch_pdf_backfill.log",
                     "backfill_pdfs.py"),

    # acecamp: 2 个并行 scraper 进程 (articles 内部含 minutes/research/article 三个 subtype)
    # 2026-04-23: events(路演) 已移除
    # 2026-04-24 账号封控事故后重整 (detail quota 10003/10040 多次耗尽):
    #   - articles 强制 --skip-detail: realtime 只拉 list 摘要, detail 配额留给回填.
    #     避免 "realtime watcher 首轮烧光 detail → dashboard 虚高 → 账号被官方封".
    #   - interval 120s / base 3.0s / jitter 2.0s / burst 20 + 15-40s 冷却 (跟
    #     crawler_manager SPECS 对齐). 默认 _mode_args 的 realtime 档 (base 1.5,
    #     burst 80) 对 AceCamp 过激, 这里显式写死走保守值.
    #   - opinions 保留 detail (用独立 opinion_info 端点, 不吃 article quota 池).
    # 2026-04-28: 跟 SPECS 同步又紧一档 — base 3.0→3.5, jitter 2.0→2.5,
    # burst 20→15, 冷却 15-40s→25-60s, interval 120/180→180/240. 历史封控过.
    # 2026-04-29: 三处同步 (这里 + crawler_manager.SPECS + daily_catchup +
    # backfill_6months):
    #   - articles: --skip-detail 移除 (用户禁止 list-only stub 写库, 见
    #     scraper.py dump_article line 558 + 启动期 startup guard).
    #   - 节奏跟 crawler_manager.SPECS["acecamp"] 完全对齐: 1/10 事故速率
    #     (25/15/3/1800), articles + opinions 都加 --daily-cap 50.
    ("acecamp",     ["--type", "articles",
                     "--interval", "1800",
                     "--throttle-base", "25.0", "--throttle-jitter", "15.0",
                     "--burst-size", "3",
                     "--burst-cooldown-min", "90", "--burst-cooldown-max", "180",
                     "--daily-cap", "50"],
                                                           "watch_articles.log"),
    ("acecamp",     ["--type", "opinions",
                     "--interval", "2400",
                     "--throttle-base", "25.0", "--throttle-jitter", "15.0",
                     "--burst-size", "3",
                     "--burst-cooldown-min", "90", "--burst-cooldown-max", "180",
                     "--daily-cap", "50"],
                                                           "watch_opinions.log"),

    # alphaengine: 4 分类并行 + 1 独立 PDF 回填 worker.
    # 所有 list watcher 都加 --skip-pdf + --interval 1200 (20 min) 覆盖
    # _mode_args 的 30s 默认值, 避免触发 REFRESH_LIMIT (见 RESTART_CONFIG 详解).
    # 节流参数 (--throttle-base/jitter, --burst-*) 由 _mode_args 兜底.
    ("alphaengine", ["--category", "summary",        "--skip-pdf",
                     "--interval", "1200"],                         "watch_summary.log"),
    ("alphaengine", ["--category", "chinaReport",    "--skip-pdf",
                     "--interval", "1200"],                         "watch_china_report.log"),
    ("alphaengine", ["--category", "foreignReport",  "--skip-pdf",
                     "--interval", "1200"],                         "watch_foreign_report.log"),
    # news (资讯) 永久停用 (2026-04-28) — 共享 streamSearch REFRESH_LIMIT
    # 配额池, 资讯每日 ~500 条会挤掉纪要+研报的额度. 见
    # backend/app/services/crawler_manager.py SPECS["alphaengine"] 同步注释.
    # ("alphaengine", ["--category", "news",           "--skip-pdf",
    #                  "--interval", "1200"],                         "watch_news.log"),
    # 配额绕过 enrich worker — 用 detail 端点 + 签名 COS URL 绕开 REFRESH_LIMIT
    # 和 PDF 下载双重配额. 每小时补 100/category 正文+PDF. 即使 list 被锁也能跑.
    # (CRAWLERS.md §9.5.8 list-vs-detail 配额不对称, 2026-04-22 AlphaEngine 验证)
    # --backfill-max 100 是业务参数 (每 tick 补多少条), 保留; 反爬数量闸靠
    # 节奏 + SoftCooldown, 不设 --daily-cap (2026-04-25 v2.2).
    ("alphaengine", ["--enrich-via-detail", "--enrich-watch", "--category", "all",
                     "--interval", "3600", "--backfill-max", "100"],
                                                                    "watch_detail_enrich.log"),

    # semianalysis (Substack) — low velocity (~3-5 posts/week), single watcher.
    # 30-min interval, anonymous by default, cookie unlocks paid bodies.
    ("semianalysis", ["--interval", "1800"],                        "watch.log"),

    # the_information — 列表 SSR 卡片抓取, 匿名模式 (~9 cards/page * ~678 pages),
    # 节奏慢 (FINDINGS: 4 URL 40s 内触发 403). 30-min 轮询, 增量到已知 article_id 即停.
    ("the_information", ["--interval", "1800",
                         "--start-page", "1", "--max-page", "700"], "watch.log"),

    # ("thirdbridge", [],                None),  # token expired, 先不拉

    # ─── IR Filings (US/HK/AU exchange disclosures, added 2026-04-28) ────
    # 5 数据源, 全部目标 Mongo `ir_filings` DB. JP/KR (edinet/tdnet/dart) 的
    # watcher 也注册以便 admin UI 显示, 但 EDINET/DART 缺密钥时 scraper 自身
    # 退出非 0 (logs/scraper.log 会有 "ERROR: no XXX key"). TDnet 无密钥可跑.
    #
    # 节流策略 (per-source):
    #   - sec_edgar: 10 req/s 硬上限, 22 ticker, 内置 0.15s 间隔. 30min 一轮.
    #   - hkex: Akamai 紧, 14 ticker, 2.8s+jitter, 30s 冷却 / 20 reqs.
    #     30min 一轮 (单 ticker 全 365 天 ~12 min).
    #   - asx: 1 ticker, Markit tarpit, 4-6s/req. 30min 一轮.
    #   - tdnet: Yanoshin 镜像, 1 req/s, 10min 一轮 (JST 盘后 13:30/15:00 集中发布).
    #   - edinet/dart: 缺密钥时 scraper 立即退出, 留行用于 UI 可见性.
    ("sec_edgar",  ["--watch", "--interval", "1800"],  "watch.log"),
    ("hkex",       ["--watch", "--interval", "1800",
                    "--days", "30"],                    "watch.log"),
    ("asx",        ["--watch", "--interval", "1800"],  "watch.log"),
    ("tdnet",      ["--watch", "--interval", "600"],   "watch.log"),
    ("edinet",     ["--watch", "--interval", "7200",
                    "--days", "14"],                    "watch.log"),
    ("dart",       ["--watch", "--interval", "7200",
                    "--days", "30"],                    "watch.log"),
]


def _mode_args(mode: str) -> list[str]:
    """realtime: 30s 轮询, 1.5s±1.0s Gaussian, micro-burst 冷却保险.
    backfill: 推荐回填档 — 强制工时禁跑 + bg 桶让位 realtime + 每 N 条强制阅读停留.
    historical: 紧急回填档 (老兼容, 不带工时禁跑也不走 bg 桶, 用在确认现在不冲突 realtime 的情况).
    dawn: 凌晨低峰档, cron 02:00-06:00 触发."""
    base = ["--watch", "--resume"]
    if mode == "realtime":
        # 2026-04-25 (v2.2): 实时档不再传数量闸 (--daily-cap / --account-budget)
        # 旧值 600 / 平台字典默认实际反爬价值≈0, 撞顶就漏抓增量 (alphapai
        # report 单日 881 条撞 3000 的顶). 反爬靠:
        #   - 节奏: --throttle-base/jitter (Gaussian), --burst-size 80 喘息保险
        #   - 指纹: UA 池 + headers_for_platform (sec-ch-ua / Referer / locale)
        #   - 联动: SoftCooldown (任一 watcher 撞警告 → 全平台静默 30~60min)
        #   - 时段: time_of_day_multiplier (夜/周末自动放慢)
        #   - 新: --idle-window-prob 0.03 — 3% 概率 60-180s 切 tab 停留
        # --burst-size 80 保留 (跟"数量闸"不同, 这是"喘息节奏", 模拟真人
        # 连续翻一会儿就停一下, 是浏览器行为模拟的一部分).
        return base + ["--since-hours", "24", "--interval", "30",
                       "--throttle-base", "1.5", "--throttle-jitter", "1.0",
                       "--burst-size", "80",
                       "--burst-cooldown-min", "10",
                       "--burst-cooldown-max", "25",
                       "--idle-window-prob", "0.03"]
    if mode == "backfill":
        # 推荐回填档 — 跟 historical 区别:
        #  • --account-role bg     →  走后台桶, realtime 主桶 ≥70% 时 bg 自动停
        #  • --enable-backfill-window → scraper 主循环顶部强制 sleep 到允许窗口
        #                              (CN 工作日 22:00-08:00 + 周末全天)
        #  • --bf-pace normal       →  每 50 条强制 5-10 min idle (BackfillSession)
        #  • interval 1200s 比 historical 600s 更慢, 因为加了强制阅读停留
        # 注: 这些 flag 由 backfill 脚本的 add_backfill_args 接收; 给主 scraper.py
        # 用时, 它们是 unrecognized → scraper 启动失败. 因此 backfill mode 推荐用
        # 在 backfill_*.py 脚本上, 而不是 ALL_SCRAPERS 的 watcher.
        return base + ["--interval", "1200",
                       "--throttle-base", "4.0", "--throttle-jitter", "2.5",
                       "--burst-size", "30",
                       "--burst-cooldown-min", "60",
                       "--burst-cooldown-max", "180",
                       "--daily-cap", "400",
                       "--account-role", "bg"]
    if mode == "historical":
        # 较长 interval 降负载; 不限 since-hours → 吃到上次 checkpoint 的所有历史.
        # 节奏比实时档保守: 3s±2s Gaussian, burst=40, 单轮上限 500.
        # 紧急用 — 不强制工时禁跑, 不走 bg 桶, 跟 realtime 共享主桶.
        return base + ["--interval", "600",
                       "--throttle-base", "3.0", "--throttle-jitter", "2.0",
                       "--burst-size", "40",
                       "--daily-cap", "500"]
    if mode == "dawn":
        # 凌晨低峰档 (cron 02:00-06:00 触发, daily_catchup.sh 用):
        # 比实时档慢一档 (5min interval), 比 historical 紧一档 (300s 而非 600s),
        # 突出"夜里悄悄追"的语义.
        return base + ["--since-hours", "36", "--interval", "300",
                       "--throttle-base", "2.5", "--throttle-jitter", "1.5",
                       "--burst-size", "60",
                       "--burst-cooldown-min", "20",
                       "--burst-cooldown-max", "45",
                       "--daily-cap", "400"]
    raise ValueError(f"unknown mode: {mode}")


def _source_args_clean(cfg: dict) -> list[str]:
    """剥离 cfg["args"] 里 mode_args/ALL_SCRAPERS extra 会覆盖的参数.
    保留源自定义参数 (如 gangtise 的 --skip-pdf). 拼接 mode args + extra 时避免重复.

    所有反爬旋钮 (--throttle-*, --burst-*, --daily-cap) 现在统一由 mode_args
    决定 — 历史的 cfg["args"] 里若残留这些, 一并剥掉以避免双重覆盖出错."""
    out = []
    drop = {"--watch", "--resume", "--since-hours", "--interval",
            "--throttle-base", "--throttle-jitter",
            "--burst-size", "--burst-cooldown-min", "--burst-cooldown-max",
            "--daily-cap", "--account-budget", "--idle-window-prob",
            "--category", "--type", "--reports"}
    drop_pair = {"--since-hours", "--interval",
                 "--throttle-base", "--throttle-jitter",
                 "--burst-size", "--burst-cooldown-min", "--burst-cooldown-max",
                 "--daily-cap", "--account-budget", "--idle-window-prob",
                 "--category", "--type"}  # 这些带值; --reports 是 flag 不带值
    args = list(cfg["args"])
    i = 0
    while i < len(args):
        a = args[i]
        if a in drop:
            if a in drop_pair and i + 1 < len(args):
                i += 2
            else:
                i += 1
            continue
        out.append(a)
        i += 1
    return out


def start_all(mode: str) -> list[dict]:
    """Kill all existing watchers (bulk by cwd) + start fresh in given mode.

    ALL_SCRAPERS 可以对同一 source 有多条 (extra, log_name) 元组, 每条拉一个独立
    进程, 对应一个 --category / --type 分板块. 这样 alphapai 的 roadshow 和 report
    能并行跑, 不再互相阻塞.
    """
    results = []
    procs = list_scraper_processes()

    # 1. 预规划每个 row 要跑什么
    #
    # 用 sys.executable 而不是硬编码 "python3" — 后者走 PATH 解析, 在
    # 某些启动环境 (PATH 里系统 /usr/bin 排在 conda 前) 会拿到系统 Python,
    # 再因为系统 Python 没装 pymongo/requests 等导致 scraper 启动即崩
    # (ModuleNotFoundError). sys.executable 是跑监控进程自身的 Python,
    # 跟监控共享同一套依赖, 一定可用.
    planned: list[tuple[str, dict, list[str], str, list[str]]] = []  # (key, cfg, extra, log_path, full_args)
    for row in ALL_SCRAPERS:
        # 兼容 3-tuple (老) 和 4-tuple (新, 自定义 script). 第 4 项是脚本文件名.
        if len(row) == 4:
            source_key, extra, log_name, script = row
        else:
            source_key, extra, log_name = row
            script = "scraper.py"
        cfg = RESTART_CONFIG.get(source_key)
        if not cfg:
            results.append({"source": source_key, "ok": False, "msg": "no config"})
            continue
        # 平台级停爬闸门: crawl/<dir>/DISABLED 文件存在就跳过. 用户明确要求
        # 账号恢复前不抓, 避免空壳 content_md 污染 dashboard.
        _disable_file = cfg["dir"] / "DISABLED"
        if _disable_file.exists():
            results.append({
                "source": source_key,
                "ok": False,
                "skipped": True,
                "msg": f"DISABLED ({_disable_file.name} exists)",
            })
            continue
        # scraper_home.py 和 scraper.py 命令行参数不同: home 只有 --watch/--interval,
        # 没有 _mode_args 里的 --resume / --since-hours. 直接用 extra, 不拼 mode.
        if script != "scraper.py":
            full_args = [sys.executable, "-u", script] + list(extra)
        else:
            clean_extra = _source_args_clean(cfg) + extra
            full_args = [sys.executable, "-u", "scraper.py"] + _mode_args(mode) + clean_extra
        log_path = cfg["dir"] / "logs" / (log_name or cfg["log"].name)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        planned.append((source_key, cfg, extra, str(log_path), full_args))

    # 2. 批量 kill: 凡是 cwd 在任一目标 source dir 下, 且是 **realtime watcher** (--watch)
    # 的 scraper 进程全干掉. backfill 进程 (p["mode"]=="backfill") 不动,
    # 让历史补齐一跑到底不被"实时模式"按钮误杀.
    target_dirs = {str(cfg["dir"]) for _, cfg, _, _, _ in planned}
    kill_targets = [p for p in procs
                    if proc_cwd(p["pid"]) in target_dirs
                    and p.get("mode") != "backfill"]
    for p in kill_targets:
        try:
            os.kill(p["pid"], 15)
        except ProcessLookupError:
            pass
    time.sleep(2)  # 给 SIGTERM 时间退出

    # 3. 再 hard-kill 还存活的 (避免 Popen 时 proc_match 撞上旧进程)
    for p in kill_targets:
        try:
            os.kill(p["pid"], 9)
        except ProcessLookupError:
            pass

    # 4. Spawn 所有计划的新进程
    #
    # 反爬关键: 18 个 watcher 同一秒 spawn 会让所有 tick 永远撞同一分钟 :00,
    # 后端按时间窗一聚类立刻看穿. 给每个进程注入随机 0~min(interval,60)s 的
    # **启动偏移**, 让 tick 散开到整个 interval 窗口里.
    #
    # 同时通过 CRAWLER_PROCESS_LABEL 环境变量把"该 watcher 的稳定标签"
    # 透传给 antibot.pick_user_agent — 重启不变, 跨进程不同 → 18 个 watcher
    # 拿到 5-8 个不同 UA, 不再共用一个 fingerprint.
    import random as _rand_local
    for source_key, cfg, extra, log_path_str, full_args in planned:
        from pathlib import Path
        log_path = Path(log_path_str)
        # 标签: key + 主要的 --category/--type 值 (方便日志查看)
        label = source_key
        for i, a in enumerate(extra):
            if a in ("--category", "--type", "--reports") and i + 1 < len(extra):
                label = f"{source_key}({extra[i+1]})"
                break
            if a == "--reports":
                label = f"{source_key}(reports)"
        # 进一步细化 label: 拼上 --market-type / --sweep-today 等子标识,
        # 让同一 source/category 的不同 subtype watcher 各自 hash 到独立 UA.
        sub_tokens = []
        for i, a in enumerate(extra):
            if a in ("--market-type", "--enrich-via-detail", "--sweep-today"):
                if a == "--market-type" and i + 1 < len(extra):
                    sub_tokens.append(extra[i + 1])
                else:
                    sub_tokens.append(a.lstrip("-"))
        full_label = label + ("|" + "_".join(sub_tokens) if sub_tokens else "")

        # 启动随机偏移: 找出 extra/full_args 里的 --interval 值
        interval_s = 60
        all_args = full_args + list(extra)
        for i, a in enumerate(all_args):
            if a == "--interval" and i + 1 < len(all_args):
                try:
                    interval_s = int(all_args[i + 1])
                except (ValueError, IndexError):
                    pass
                break
        # 钳到 [0, 60s] — interval 再大也不要拖太久, 用户期望"按按钮就动"
        offset_s = _rand_local.randint(0, min(60, max(5, interval_s // 2)))

        # 子环境: 透传 label + 启动偏移 (scraper 自己 sleep)
        proc_env = os.environ.copy()
        proc_env["CRAWLER_PROCESS_LABEL"] = full_label
        proc_env["CRAWLER_STARTUP_OFFSET_S"] = str(offset_s)

        try:
            logf = log_path.open("ab")
            # 启动偏移用 shell wrapper 实现 — 不需要修改每个 scraper
            # (sleep N && exec scraper_args). exec 让 scraper 直接顶替
            # shell PID, 监控的 cwd/cmdline 探测仍能匹配上.
            shell_cmd = f"sleep {offset_s} && exec " + " ".join(
                _shell_quote(a) for a in full_args
            )
            subprocess.Popen(
                ["/bin/sh", "-c", shell_cmd],
                cwd=str(cfg["dir"]),
                stdout=logf, stderr=subprocess.STDOUT,
                start_new_session=True,
                env=proc_env,
            )
            logf.close()
            results.append({
                "source": label,
                "ok": True,
                "args": " ".join(full_args),
                "offset_s": offset_s,
                "label": full_label,
            })
        except Exception as e:
            results.append({
                "source": label, "ok": False,
                "msg": f"{type(e).__name__}: {e}",
            })

    time.sleep(2)
    return results


def _shell_quote(s: str) -> str:
    """Minimal shell quoter for the spawn wrapper. We control the inputs
    (sys.executable, scraper.py, our own argv) so single-quote wrap is enough."""
    if not s:
        return "''"
    if all(c.isalnum() or c in "/-_=.,:" for c in s):
        return s
    return "'" + s.replace("'", "'\\''") + "'"


def _restart_watcher(source: str, new_credential: str) -> tuple[bool, str]:
    """写 credentials.json + kill 旧进程 + 起新进程. 返回 (ok, message)."""
    cfg = RESTART_CONFIG.get(source)
    if not cfg:
        return False, f"未知源 '{source}', 可用: {', '.join(RESTART_CONFIG)}"
    if not cfg["token_check"](new_credential):
        return False, f"凭证格式不对. 期望: {cfg['desc']}"

    # 写 credentials.json (合并已有字段)
    creds = {}
    if cfg["creds_file"].exists():
        try:
            creds = json.loads(cfg["creds_file"].read_text(encoding="utf-8"))
        except Exception:
            creds = {}
    creds[cfg["token_field"]] = new_credential
    cfg["creds_file"].parent.mkdir(parents=True, exist_ok=True)
    cfg["creds_file"].write_text(json.dumps(creds, ensure_ascii=False, indent=2))

    # 杀旧进程 (用 cwd+cmdline 精确匹配, 命令行无 cwd 信息)
    fake_source = {"proc_match": cfg["proc_match"]}
    old = find_process_for(fake_source, list_scraper_processes())
    if old:
        try:
            os.kill(old["pid"], 15)  # SIGTERM
            time.sleep(1.5)
            if os.path.exists(f"/proc/{old['pid']}"):
                os.kill(old["pid"], 9)  # SIGKILL
        except ProcessLookupError:
            pass
        except Exception:
            pass
        time.sleep(0.5)

    # 起新进程
    cfg["log"].parent.mkdir(parents=True, exist_ok=True)
    logf = cfg["log"].open("ab")
    try:
        subprocess.Popen(
            ["python3", "-u", "scraper.py"] + cfg["args"],
            cwd=str(cfg["dir"]),
            stdout=logf, stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    finally:
        logf.close()
    time.sleep(3)

    # 确认 (最多等 5s, 因为 python 启动 + import 需要时间)
    for _ in range(5):
        newp = find_process_for(fake_source, list_scraper_processes())
        if newp:
            return True, f"已重启, 新进程 pid={newp['pid']}"
        time.sleep(1)
    return False, "进程未启动, 查看日志"


def start_command_listener(app_id: str, app_secret: str) -> threading.Thread:
    """后台线程: 用 lark_oapi WS 长连接监听群里的指令.

    支持指令:
      状态 / /status / 报告 — 回复当前爬虫状态卡片
      /token <source> <credential>  — 更新凭证并重启对应 watcher
         source: alphapai / meritco / thirdbridge
    """
    import lark_oapi as lark
    from lark_oapi.api.im.v1 import (
        ReplyMessageRequest, ReplyMessageRequestBody,
    )

    status_commands = {
        "状态", "报告", "快照",
        "status", "/status", "report", "/report",
    }
    token_re = re.compile(r"^/token\s+(\S+)\s+(\S.*)$", re.IGNORECASE)

    def _extract_text(content_json: str) -> str:
        try:
            data = json.loads(content_json or "{}")
        except Exception:
            return ""
        text = data.get("text", "") or ""
        text = re.sub(r"@_user_\d+|@\S+", "", text).strip()
        return text

    client = lark.Client.builder().app_id(app_id).app_secret(app_secret).build()

    # message_id → first_seen_ts, 防止飞书重试同一消息触发二次回复
    seen_msgs: dict[str, float] = {}

    def _seen_recently(mid: str) -> bool:
        now = time.time()
        # 清理 5 分钟前的条目
        for k in [k for k, v in seen_msgs.items() if now - v > 300]:
            seen_msgs.pop(k, None)
        if mid in seen_msgs:
            return True
        seen_msgs[mid] = now
        return False

    def _reply(msg, *, text: str = "", card_body: dict | None = None):
        if card_body is not None:
            content = json.dumps(card_body, ensure_ascii=False)
            msg_type = "interactive"
        else:
            content = json.dumps({"text": text}, ensure_ascii=False)
            msg_type = "text"
        req = (
            ReplyMessageRequest.builder()
            .message_id(msg.message_id)
            .request_body(
                ReplyMessageRequestBody.builder()
                .content(content).msg_type(msg_type).build()
            ).build()
        )
        resp = client.im.v1.message.reply(req)
        return resp

    def on_message(data):
        try:
            msg = data.event.message
            if _seen_recently(msg.message_id):
                return  # 幂等去重: 同一消息 SDK 可能重试下发
            text = _extract_text(msg.content)
            ts = datetime.now().strftime("%H:%M:%S")

            # /token <source> <credential>
            m = token_re.match(text)
            if m:
                source = m.group(1).lower().strip()
                cred = m.group(2).strip()
                ok, info = _restart_watcher(source, cred)
                mark = "✅" if ok else "❌"
                _reply(msg, text=f"{mark} /token {source}: {info}")
                print(f"[飞书·指令] {ts} /token {source} {'OK' if ok else 'FAIL'} ({info})")
                return

            # 状态指令
            if text.lower() in status_commands:
                card_body = build_card_body(snapshot())
                resp = _reply(msg, card_body=card_body)
                if not resp.success():
                    print(f"[飞书·指令] {ts} reply 失败: {resp.code} {resp.msg}")
                else:
                    print(f"[飞书·指令] {ts} '{text}' → 已回复卡片")
                return

        except Exception as e:
            print(f"[飞书·指令] handler 异常: {type(e).__name__}: {e}")

    handler = (
        lark.EventDispatcherHandler.builder("", "")
        .register_p2_im_message_receive_v1(on_message)
        .build()
    )
    ws = lark.ws.Client(
        app_id, app_secret, event_handler=handler,
        log_level=lark.LogLevel.WARNING,
    )

    def loop():
        print(f"[飞书·指令] 监听已启动, 支持: {', '.join(sorted(status_commands))}; "
              f"/token <{' | '.join(RESTART_CONFIG)}> <credential>")
        try:
            ws.start()
        except Exception as e:
            print(f"[飞书·指令] WS 异常退出: {type(e).__name__}: {e}")

    t = threading.Thread(target=loop, name="feishu-listener", daemon=True)
    t.start()
    return t


# 掉线告警: 每个平台恢复凭证的步骤提示
CRED_HINTS = {
    "meritco": (
        "**恢复方法**: 在群里发\n"
        "`/token meritco <32位token>`\n\n"
        "**获取步骤**:\n"
        "1. 浏览器登录 https://research.meritco-group.com/forum\n"
        "2. F12 → Network → 随便点一个 XHR (如 `forum/select/list`)\n"
        "3. Request Headers 找 `token:` 行, 复制后面的 32 位 hex"
    ),
    "thirdbridge": (
        "**恢复方法**: 在群里发\n"
        "`/token thirdbridge <整条Cookie>`\n\n"
        "**获取步骤**:\n"
        "1. 浏览器登录 https://forum.thirdbridge.com\n"
        "2. F12 → Network → 任意请求 → Request Headers → Cookie 行\n"
        "3. 复制整条 `AWSELBAuthSessionCookie-0=...` (一直到末尾)"
    ),
    "alphapai": (
        "**恢复方法**: 在群里发\n"
        "`/token alphapai <JWT>`\n\n"
        "**获取步骤**:\n"
        "1. 浏览器登录 https://alphapai-web.rabyte.cn\n"
        "2. F12 → Application → Local Storage → `alphapai-web.rabyte.cn`\n"
        "3. 找 key `USER_AUTH_TOKEN`, 复制 value (以 `eyJ` 开头)"
    ),
    "jinmen": (
        "**恢复方法**: 暂不支持机器人热更, 联系管理员更新 JM_AUTH_INFO."
    ),
    "funda": (
        "**恢复方法**: 在群里发\n"
        "`/token funda <整条Cookie>`\n\n"
        "**获取步骤**:\n"
        "1. 浏览器登录 https://www.funda.ai\n"
        "2. F12 → Application → Cookies → `www.funda.ai`\n"
        "3. 或 Network → 任意请求 → Request Headers → Cookie 行 → 整条复制\n"
        "4. 关键字段: `session-token=...`"
    ),
    "gangtise": (
        "**恢复方法**: 在群里发\n"
        "`/token gangtise <token>`\n\n"
        "**获取步骤**:\n"
        "1. 浏览器登录 https://open.gangtise.com\n"
        "2. F12 → Network → 任意 API 请求 → Request Headers → `token:` 字段\n"
        "3. 复制整串"
    ),
}


def build_alert_card(platform: dict, prev_state: str, curr_state: str,
                     reason: str) -> dict:
    """构造掉线/恢复告警卡片."""
    key = platform["key"]
    label = platform["label"]
    now_local = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # 判方向: 掉线还是恢复
    sev_order = {"ok": 0, "warn": 1, "stopped": 2}
    degraded = sev_order.get(curr_state, 0) > sev_order.get(prev_state, 0)

    if degraded:
        icon = "🚨" if curr_state == "stopped" else "⚠️"
        title = f"{icon} 爬虫告警 · {label}"
        template = "red" if curr_state == "stopped" else "orange"
        status = f"{icon} **{curr_state}** ({reason})"
        hint = CRED_HINTS.get(key, "")
    else:
        title = f"✅ 爬虫恢复 · {label}"
        template = "green"
        status = "🟢 **ok** (已恢复正常)"
        hint = ""

    elements = [{
        "tag": "div",
        "text": {
            "tag": "lark_md",
            "content": (
                f"**{label}** · {now_local}\n"
                f"状态: {status}\n"
                f"(之前: {prev_state})"
            ),
        },
    }]
    if hint:
        elements.append({"tag": "hr"})
        elements.append({
            "tag": "div",
            "text": {"tag": "lark_md", "content": hint},
        })

    return {
        "config": {"wide_screen_mode": True},
        "header": {
            "title": {"tag": "plain_text", "content": title},
            "template": template,
        },
        "elements": elements,
    }


def start_health_watchdog(*, app_id: str, app_secret: str,
                          receive_id_type: str, receive_id: str,
                          webhook: str, check_interval_s: int = 300) -> threading.Thread:
    """后台线程: 每 `check_interval_s` 秒对比平台健康状态, 变差/恢复时推一张告警卡片.

    不再做整点整体播报. 只在 状态变化 时通知.
    """
    state_file = ROOT / "logs" / "monitor_health.json"
    state_file.parent.mkdir(parents=True, exist_ok=True)
    prev: dict[str, str] = {}
    if state_file.exists():
        try:
            prev = json.loads(state_file.read_text())
        except Exception:
            prev = {}

    def _send_alert(card: dict) -> None:
        if app_id and app_secret and receive_id:
            client = FeishuAppClient(app_id, app_secret)
            client.send_interactive(receive_id_type or "chat_id", receive_id, card)
        elif webhook:
            send_feishu_webhook(webhook, card)

    def loop():
        nonlocal prev
        print(f"[健康告警] 已启动 ({check_interval_s}s 一次, 仅状态变化时通知)")
        # 首次上线先记录基线, 不发通知
        try:
            snap = snapshot()
            for p in snap["platforms"]:
                prev[p["key"]] = (p.get("health") or {}).get("state", "ok")
            state_file.write_text(json.dumps(prev))
        except Exception as e:
            print(f"[健康告警] 基线失败: {e}")

        while True:
            time.sleep(check_interval_s)
            try:
                snap = snapshot()
                curr_map: dict[str, str] = {}
                alerts: list[dict] = []
                for p in snap["platforms"]:
                    h = p.get("health") or {}
                    curr = h.get("state", "ok")
                    reason = h.get("reason", "")
                    curr_map[p["key"]] = curr
                    was = prev.get(p["key"], "ok")
                    if curr != was:
                        alerts.append(build_alert_card(p, was, curr, reason))
                        print(f"[健康告警] {p['label']}: {was} → {curr} ({reason})")

                for card in alerts:
                    try:
                        _send_alert(card)
                    except Exception as e:
                        print(f"[健康告警] 推送失败: {type(e).__name__}: {e}")

                prev = curr_map
                state_file.write_text(json.dumps(prev))
            except Exception as e:
                print(f"[健康告警] 轮询异常: {type(e).__name__}: {e}")

    t = threading.Thread(target=loop, name="feishu-watchdog", daemon=True)
    t.start()
    return t


def _seconds_until_next_hour() -> float:
    now = datetime.now()
    nxt = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    return max(1.0, (nxt - now).total_seconds())


def start_feishu_scheduler(*, app_id: str, app_secret: str,
                           receive_id_type: str, receive_id: str,
                           webhook: str) -> threading.Thread:
    """后台线程: 每个整点推送一次."""
    mode = (
        f"App(chat={receive_id})" if app_id and app_secret and receive_id
        else "Webhook" if webhook else "未配置"
    )

    def loop():
        print(f"[飞书] 整点推送已启动 ({mode}), 下次 {_seconds_until_next_hour():.0f}s 后")
        while True:
            sleep_s = _seconds_until_next_hour()
            time.sleep(sleep_s)
            try:
                resp = push_card(
                    snapshot(),
                    app_id=app_id, app_secret=app_secret,
                    receive_id_type=receive_id_type, receive_id=receive_id,
                    webhook=webhook,
                )
                ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                ok = (resp.get("code") == 0) or ("StatusCode" in str(resp) and "0" in str(resp.get("StatusCode")))
                ok_mark = "✓" if ok else "✗"
                print(f"[飞书] {ts} {ok_mark} {resp}")
            except Exception as e:
                print(f"[飞书] 推送失败: {type(e).__name__}: {e}")

    t = threading.Thread(target=loop, name="feishu-hourly", daemon=True)
    t.start()
    return t


# ---------------- CLI (rich live) ----------------

def render_cli(snap: dict):
    from rich.console import Group
    from rich.panel import Panel
    from rich.table import Table
    from rich.text import Text

    overview = Table.grid(padding=(0, 2))
    overview.add_column(justify="right", style="bold cyan")
    overview.add_column()
    overview.add_row("时间", snap["generated_at"])
    overview.add_row("Mongo", f"[green]OK[/] {snap['mongo_uri']}" if snap["mongo_ok"] else f"[red]不可用[/] {snap['mongo_uri']}")
    overview.add_row("活跃进程", f"{snap['process_count']} 个 scraper --watch")

    tbl = Table(title="平台总览", expand=True, border_style="blue")
    tbl.add_column("平台", style="bold")
    tbl.add_column("进程", justify="center")
    tbl.add_column("总数", justify="right")
    tbl.add_column("今日", justify="right")
    tbl.add_column("分类数", justify="center")
    tbl.add_column("上轮结束")

    for p in snap["platforms"]:
        proc = p["process"]
        if proc:
            pid_col = f"[green]{proc['pid']}[/]\n{proc['etime']}"
        else:
            pid_col = "[red]未运行[/]"
        today_col = f"[bold green]+{p['today_added']}[/]" if p["today_added"] else "0"
        tbl.add_row(
            p["label"],
            pid_col,
            f"{p['total']:,}",
            today_col,
            str(p["tab_count"]),
            fmt_delta(p["last_run_end_at"]),
        )

    panels = [Panel(overview, title="系统概览", border_style="cyan"), tbl]

    for p in snap["platforms"]:
        sub = Table(show_header=True, header_style="bold magenta", expand=True, box=None)
        sub.add_column("分类", style="bold")
        sub.add_column("总数", justify="right")
        sub.add_column("今日", justify="right")
        sub.add_column("上轮结果")
        sub.add_column("top_id")
        sub.add_column("最近条目时间")

        for t in p["tabs"]:
            if "error" in t:
                sub.add_row(t["label"], "-", "-", f"[red]ERR {t['error']}[/]", "-", "-")
                continue
            st = t["state"]
            stats = st.get("last_run_stats") or {}
            stats_str = (
                f"+{stats.get('added', 0)}/skip {stats.get('skipped', 0)}/fail {stats.get('failed', 0)}"
                if stats else "-"
            )
            if st.get("in_progress"):
                stats_str += " [yellow](运行中)[/]"
            newest = t["recent"][0] if t["recent"] else None
            newest_t = fmt_delta(newest.get("crawled_at")) if newest else "-"
            sub.add_row(
                t["label"],
                f"{t['total']:,}",
                f"[green]+{t['today_added']}[/]" if t["today_added"] else "0",
                stats_str,
                str(st.get("top_id") or "-")[:20],
                newest_t,
            )
        panels.append(Panel(sub, title=p["label"], border_style=p["color"]))

    return Group(*panels)


def run_cli(refresh: float):
    from rich.console import Console
    from rich.live import Live

    console = Console()
    try:
        with Live(render_cli(snapshot()), console=console,
                  refresh_per_second=2, screen=True) as live:
            while True:
                time.sleep(refresh)
                live.update(render_cli(snapshot()))
    except KeyboardInterrupt:
        pass


# ---------------- Web (FastAPI) ----------------

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="utf-8"/>
<title>爬虫监控</title>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<style>
  :root { color-scheme: dark; }
  * { box-sizing: border-box; }
  body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "PingFang SC", "Microsoft YaHei", monospace; background:#0e1218; color:#d7dde5; margin:0; padding:24px; }
  h1 { margin:0 0 4px 0; font-size:20px; }
  .meta { color:#7d8a9c; font-size:12px; margin-bottom:20px; }
  .card { background:#151b24; border:1px solid #222c3a; border-radius:10px; padding:14px 18px; margin-bottom:16px; }
  .card h2 { margin:0 0 10px 0; font-size:15px; display:flex; align-items:center; gap:10px; }
  .color-dot { display:inline-block; width:10px; height:10px; border-radius:50%; }
  .dots-bar { display:inline-flex; gap:4px; vertical-align:middle; }
  .status-dot {
    display:inline-block; width:10px; height:10px; border-radius:50%;
    border:1px solid rgba(0,0,0,0.25); cursor:help;
  }
  .status-dot.dot-ok   { background:#5dd39e; box-shadow:0 0 4px rgba(93,211,158,0.35); }
  .status-dot.dot-warn { background:#f0c674; box-shadow:0 0 4px rgba(240,198,116,0.35); }
  .status-dot.dot-stop { background:#ef6f6c; box-shadow:0 0 4px rgba(239,111,108,0.4); }
  .status-dot.dot-dis  { background:#6b7688; box-shadow:none; opacity:0.55; }
  .auth-pill {
    display:inline-block; padding:2px 10px; border-radius:10px;
    font-size:11px; font-weight:500; cursor:help;
    border:1px solid rgba(0,0,0,0.3);
  }
  .auth-pill.auth-ok  { background:#14382a; color:#7ddfb0; border-color:#2d5f44; }
  .auth-pill.auth-bad  { background:#3d1a1d; color:#ef6f6c; border-color:#5e2a2e; }
  .auth-pill.auth-warn { background:#3d2a15; color:#f0c674; border-color:#5e3f20; }
  .auth-pill.auth-unk  { background:#2a2f3a; color:#94a3b8; border-color:#3d4553; }
  table { width:100%; border-collapse:collapse; font-size:13px; }
  th, td { padding:6px 8px; text-align:left; border-bottom:1px solid #222c3a; vertical-align:top; }
  th { color:#8a97ab; font-weight:500; background:#0f141c; }
  tr:last-child td { border-bottom:none; }
  .ok { color:#5dd39e; }
  .err { color:#ef6f6c; }
  .warn { color:#f0c674; }
  .dim { color:#6b7688; }
  .num { font-variant-numeric: tabular-nums; text-align:right; }
  .pill { display:inline-block; padding:1px 8px; border-radius:10px; font-size:11px; }
  .pill.run { background:#1a3d2a; color:#5dd39e; }
  .pill.stop { background:#3d1a1d; color:#ef6f6c; }
  .pill.prog { background:#3d331a; color:#f0c674; }
  pre { background:#0b0f16; padding:8px 10px; border-radius:6px; overflow-x:auto; font-size:12px; max-height:220px; color:#a3aec0; margin:0; }
  .grid { display:grid; grid-template-columns: repeat(auto-fit, minmax(480px, 1fr)); gap:14px; }
  .kv td:first-child { color:#8a97ab; width:110px; }
  .tabs { display:flex; gap:4px; margin: 4px 0 10px; border-bottom: 1px solid #222c3a; padding-bottom: 2px; }
  .tab-btn { background: transparent; border:1px solid transparent; color:#8a97ab; padding:4px 12px; cursor:pointer; font-size:12px; border-radius:6px 6px 0 0; font-family: inherit; }
  .tab-btn:hover { color:#d7dde5; background:#0f141c; }
  .tab-btn.active { color:#d7dde5; background:#0f141c; border-color:#222c3a; border-bottom-color:#151b24; margin-bottom:-1px; }
  .tab-panel { display:none; }
  .tab-panel.active { display:block; }
  .title-cell { max-width:360px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
  .badge-today { background: rgba(93, 211, 158, 0.18); color:#5dd39e; padding: 1px 7px; border-radius: 10px; font-size: 11px; margin-left: 6px; }
  .feed { max-height: 420px; overflow-y: auto; }
  .feed-row { display: grid; grid-template-columns: 70px 180px 1fr; gap: 8px; padding: 6px 8px; border-bottom: 1px solid #1a222e; font-size: 13px; align-items: baseline; }
  .feed-row:hover { background:#0f141c; }
  .feed-row.new { animation: flashNew 2.5s ease-out; }
  .feed-row.new-bf { animation: flashNewBf 2.5s ease-out; }
  @keyframes flashNew { 0% { background: rgba(93, 211, 158, 0.25); } 100% { background: transparent; } }
  @keyframes flashNewBf { 0% { background: rgba(240, 198, 116, 0.22); } 100% { background: transparent; } }
  .feed-time { color:#8a97ab; font-variant-numeric: tabular-nums; font-size:12px; white-space:nowrap; }
  .feed-src { font-size:12px; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
  .feed-title { color:#d7dde5; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
  .live-dot { display:inline-block; width:8px; height:8px; border-radius:50%; background:#5dd39e; animation: pulse 1.5s infinite; vertical-align:middle; margin-right:6px; }
  .live-dot.bf { background:#f0c674; }
  @keyframes pulse { 0%,100% { opacity: 1; } 50% { opacity: 0.3; } }
  .feed-grid { display:grid; grid-template-columns: 1fr 1fr; gap:14px; }
  @media (max-width: 1100px) { .feed-grid { grid-template-columns: 1fr; } }
  .feed-card { background:#151b24; border:1px solid #222c3a; border-radius:10px; padding:14px 18px; }
  .feed-card h2 { margin:0 0 10px 0; font-size:15px; display:flex; align-items:center; gap:10px; flex-wrap:wrap; }
  .feed-empty { color:#6b7688; padding:20px; text-align:center; font-size:12px; }
  .btn-bar { display:flex; gap:10px; margin: 12px 0 20px; flex-wrap: wrap; align-items:center; }
  .btn { padding: 8px 16px; border: 1px solid #2a3548; background:#151b24; color:#d7dde5; border-radius:6px; cursor:pointer; font-size:13px; font-family: inherit; transition: all 0.15s; }
  .btn:hover { border-color: #3d4c67; background: #1c2332; }
  .btn.primary { background:#1a3d2a; border-color:#2d5f44; color:#5dd39e; }
  .btn.primary:hover { background:#1f4a33; border-color:#3c7258; }
  .btn.warn { background:#3d331a; border-color:#5f4d2d; color:#f0c674; }
  .btn.warn:hover { background:#4a3f21; border-color:#72593c; }
  .btn:disabled { opacity: 0.5; cursor: not-allowed; }
  .toast { position: fixed; top: 20px; right: 20px; padding: 12px 20px; border-radius: 8px; font-size: 13px; box-shadow: 0 4px 16px rgba(0,0,0,0.3); z-index:9999; max-width: 400px; }
  .toast.ok { background: #1a3d2a; color: #5dd39e; border: 1px solid #2d5f44; }
  .toast.err { background: #3d1a1d; color: #ef6f6c; border: 1px solid #5f2d30; }
</style>
<script>
  function switchTab(platform, tabKey) {
    const root = document.querySelector('[data-platform="' + platform + '"]');
    if (!root) return;
    root.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
    root.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
    root.querySelector('[data-tab-btn="' + tabKey + '"]').classList.add('active');
    root.querySelector('[data-tab-panel="' + tabKey + '"]').classList.add('active');
  }

  // 一键启动: 提示确认 → POST → 反馈
  function showToast(msg, ok) {
    const t = document.createElement('div');
    t.className = 'toast ' + (ok ? 'ok' : 'err');
    t.textContent = msg;
    document.body.appendChild(t);
    setTimeout(() => t.remove(), 5000);
  }
  async function startAll(mode) {
    const labelMap = { realtime: '实时监控', backfill: '回填安全档', historical: '历史紧急档', dawn: '凌晨低峰档' };
    const label = labelMap[mode] || mode;
    let warn = '';
    if (mode === 'backfill') warn = '\\n\\n📦 回填安全档 (推荐): bg 桶 + realtime 让位 + 强制工时禁跑 (CN 工作日 22:00-08:00 + 周末). 日间启动会直接 sleep 到当晚.';
    else if (mode === 'historical') warn = '\\n\\n⚠️ 历史紧急档 (老兼容): 不带 backfill 窗口和 bg 桶 — 跟 realtime 抢主桶. 只在确认不冲突 realtime 时用.';
    else if (mode === 'dawn') warn = '\\n\\n🌙 凌晨档: 5min interval + Gaussian 节流, 给 cron 02:00-06:00 用. 工时段直接跑也行 (自动按时段倍增减速).';
    else warn = '\\n\\n实时模式: 只抓过去 24h 内 + 增量, 30s 轮询, 含 micro-burst 冷却保护.';
    if (!confirm('确认一键重启所有 watcher → ' + label + '?' + warn)) return;
    const btns = document.querySelectorAll('.btn');
    btns.forEach(b => b.disabled = true);
    try {
      const r = await fetch('/api/start-all?mode=' + mode, { method: 'POST' });
      const d = await r.json();
      if (d.ok) {
        showToast('✅ ' + label + ': 已启动 ' + d.started + '/' + d.total + ' 个 watcher', true);
      } else {
        showToast('❌ ' + (d.error || '失败'), false);
      }
    } catch (e) {
      showToast('❌ ' + e.message, false);
    } finally {
      setTimeout(() => btns.forEach(b => b.disabled = false), 2000);
    }
  }

  // 入库双流: realtime (近 24h release_time) + backfill (更老, 回填爬来的历史).
  // 两流各自独立 SEEN / 心跳 / poll, 互不干扰.
  const STREAMS = {
    realtime: { SEEN: new Set(), firstLoad: true, lastPoll: 0, lastInsert: null,
                boxId: 'feed-rt', hbId: 'feed-rt-heartbeat', flashCls: 'new',
                endpoint: '/api/recent?limit=40&mode=realtime' },
    backfill: { SEEN: new Set(), firstLoad: true, lastPoll: 0, lastInsert: null,
                boxId: 'feed-bf', hbId: 'feed-bf-heartbeat', flashCls: 'new-bf',
                endpoint: '/api/recent?limit=40&mode=backfill' },
  };
  function fmtAgoFromMs(ms) {
    const sec = Math.max(0, Math.floor((Date.now() - ms) / 1000));
    if (sec < 60) return sec + 's 前';
    if (sec < 3600) return Math.floor(sec/60) + 'm 前';
    if (sec < 86400) return Math.floor(sec/3600) + 'h 前';
    return Math.floor(sec/86400) + 'd 前';
  }
  function updateHeartbeat() {
    for (const k of Object.keys(STREAMS)) {
      const s = STREAMS[k];
      const hb = document.getElementById(s.hbId);
      if (!hb) continue;
      const pollAge = s.lastPoll ? fmtAgoFromMs(s.lastPoll) : '-';
      const insAge = s.lastInsert ? fmtAgoFromMs(new Date(s.lastInsert).getTime()) : '-';
      hb.textContent = `最近入库 ${insAge} · 上次轮询 ${pollAge}`;
    }
  }
  setInterval(updateHeartbeat, 1000);
  function fmtAgo(isoStr) {
    if (!isoStr) return '-';
    const d = new Date(isoStr), sec = (Date.now() - d.getTime()) / 1000;
    if (sec < 60) return Math.max(0, Math.floor(sec)) + 's 前';
    if (sec < 3600) return Math.floor(sec/60) + 'm 前';
    if (sec < 86400) return Math.floor(sec/3600) + 'h 前';
    return Math.floor(sec/86400) + 'd 前';
  }
  function esc(s) {
    const d = document.createElement('div'); d.textContent = s == null ? '' : String(s); return d.innerHTML;
  }
  async function pollStream(mode) {
    const s = STREAMS[mode];
    if (!s) return;
    try {
      const r = await fetch(s.endpoint, { cache: 'no-store' });
      if (!r.ok) return;
      const data = await r.json();
      s.lastPoll = Date.now();
      if (data.feed && data.feed.length && data.feed[0].crawled_at) {
        s.lastInsert = data.feed[0].crawled_at;
      }
      const box = document.getElementById(s.boxId);
      if (!box) return;
      // 按时间升序 prepend (最老的先加, 这样最新的在顶)
      const fresh = (data.feed || []).filter(it => !s.SEEN.has(it._id + '|' + it.tab_key));
      fresh.reverse().forEach(it => {
        const key = it._id + '|' + it.tab_key;
        s.SEEN.add(key);
        const row = document.createElement('div');
        row.className = 'feed-row' + (s.firstLoad ? '' : ' ' + s.flashCls);
        row.innerHTML = [
          '<span class="feed-time" title="' + esc(it.crawled_at) + '">' + esc(fmtAgo(it.crawled_at)) + '</span>',
          '<span class="feed-src"><span class="color-dot" style="background:' + esc(it.source_color) + '"></span> ' +
            esc(it.platform_label) + ' · ' + esc(it.tab_label) + '</span>',
          '<span class="feed-title" title="' + esc(it.title) + '">' + esc(it.title || '(无标题)') + '</span>',
        ].join('');
        box.insertBefore(row, box.firstChild);
      });
      // 限制显示行数, 删掉超出的旧行
      while (box.children.length > 80) box.removeChild(box.lastChild);
      // 刷新所有行的时间显示
      box.querySelectorAll('.feed-row').forEach(r => {
        const t = r.querySelector('.feed-time');
        if (t && t.getAttribute('title')) t.textContent = fmtAgo(t.getAttribute('title'));
      });
      // 首次拿到数据才切掉 firstLoad, 否则 backfill 空返回会永远闪烁
      if (data.feed && data.feed.length > 0) s.firstLoad = false;
      updateHeartbeat();
    } catch (e) { /* 静默 */ }
  }
  setInterval(() => { pollStream('realtime'); pollStream('backfill'); }, 2000);
  setTimeout(() => { pollStream('realtime'); pollStream('backfill'); }, 100);
  // 整页刷新: 60 秒 (feed 已 2s 自更新; 这里只为平台卡片全量刷新)
  setTimeout(() => location.reload(), 60000);
</script>
</head>
<body>
<h1>爬虫监控</h1>
<div class="meta">__GENERATED_AT__ · Mongo: __MONGO_STATUS__ · 实时 scraper: <b>__REALTIME_COUNT__</b> · 回填 scraper: <b>__BACKFILL_COUNT__</b> · 合计: __PROCESS_COUNT__</div>

<div class="btn-bar">
  <button class="btn primary" onclick="startAll('realtime')">🚀 一键启动 实时监控 (24h 窗口)</button>
  <button class="btn" onclick="startAll('backfill')">📦 回填安全档 (推荐, bg 桶+工时禁跑)</button>
  <button class="btn warn" onclick="startAll('historical')">⚠️ 历史紧急档 (跟 realtime 抢主桶)</button>
  <button class="btn" onclick="startAll('dawn')">🌙 凌晨低峰档 (cron 02:00 用)</button>
  <span class="dim" style="font-size:12px;">按钮会 kill 现有 watcher + 重启; 所有平台一次到位 · 启动随机 0~60s 偏移防同 tick</span>
</div>

<div class="card">
  <h2>总览</h2>
  <table>
    <thead><tr>
      <th>平台</th><th>进程状态 (per-variant)</th><th>真实登陆</th>
      <th class="num">今日新增</th><th>数据跨度</th><th>最近更新</th>
    </tr></thead>
    <tbody>__OVERVIEW_ROWS__</tbody>
  </table>
</div>

<div class="feed-grid">
  <div class="feed-card">
    <h2><span class="live-dot"></span>实时入库流
      <span class="dim" style="font-weight:normal;font-size:11px">发布时间 &lt; 24h · watcher 拿的新鲜内容</span>
      <span id="feed-rt-heartbeat" class="dim" style="font-weight:normal;font-size:11px;margin-left:auto;">最近入库 - · 上次轮询 -</span>
    </h2>
    <div class="feed" id="feed-rt"></div>
  </div>
  <div class="feed-card">
    <h2><span class="live-dot bf"></span>回填入库流
      <span class="dim" style="font-weight:normal;font-size:11px">发布时间 ≥ 24h · 历史回填爬的老数据</span>
      <span id="feed-bf-heartbeat" class="dim" style="font-weight:normal;font-size:11px;margin-left:auto;">最近入库 - · 上次轮询 -</span>
    </h2>
    <div class="feed" id="feed-bf"></div>
  </div>
</div>

<div class="grid">
__DETAIL_CARDS__
</div>

<div class="meta">自动刷新: 10s · 源: __SCRIPT_PATH__</div>
</body>
</html>
"""


def render_html(snap: dict) -> str:
    def esc(s: Any) -> str:
        return (str(s) if s is not None else "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    def fmt_range(tr: dict, compact: bool = True) -> str:
        """{oldest_ms, newest_ms, span_days} → '2025-10 → 2026-04 · 182天' (compact)
        或 '2025-10-24 12:00 → 2026-04-22 18:30 · 共 180 天' (完整)."""
        if not tr or not tr.get("oldest_ms") or not tr.get("newest_ms"):
            return "<span class='dim'>-</span>"
        from datetime import datetime as _dt, timezone as _tz, timedelta as _td
        o = _dt.fromtimestamp(tr["oldest_ms"] / 1000, tz=_tz(_td(hours=8)))
        n = _dt.fromtimestamp(tr["newest_ms"] / 1000, tz=_tz(_td(hours=8)))
        span = tr.get("span_days", 0)
        if compact:
            o_str = o.strftime("%Y-%m")
            n_str = n.strftime("%Y-%m")
            # 跨度染色: 6 月以上绿, 1-6 月黄, 不到 1 月红
            cls = "ok" if span >= 180 else ("warn" if span >= 30 else "err")
            return f"<span class='dim'>{o_str}</span> → <span class='dim'>{n_str}</span> <span class='{cls}' style='margin-left:4px'>{span}d</span>"
        o_str = o.strftime("%Y-%m-%d %H:%M")
        n_str = n.strftime("%Y-%m-%d %H:%M")
        return f"{o_str} → {n_str} <span class='dim'>· 共 {span} 天</span>"

    overview_rows = []
    detail_cards = []

    for p in snap["platforms"]:
        # overview 行 — 每个 variant 一个圆点, 颜色对应它的 health:
        # 绿 ok · 黄 warn · 红 stopped. 鼠标 hover 显示 variant 名 + PID + reason.
        dots_html = []
        for t in p.get("tabs", []):
            th = t.get("health") or {}
            tstate = th.get("state", "ok")
            dot_cls = {"ok": "dot-ok", "warn": "dot-warn",
                       "stopped": "dot-stop",
                       "disabled": "dot-dis"}.get(tstate, "dot-ok")
            tproc = t.get("process") or {}
            tip_parts = [str(t.get("label") or t.get("key"))]
            if tproc.get("pid"):
                tip_parts.append(f"PID {tproc['pid']} · {tproc.get('etime','')}")
            else:
                tip_parts.append("未运行")
            if tstate != "ok" and th.get("reason"):
                tip_parts.append(str(th["reason"]))
            tip = " | ".join(tip_parts)
            dots_html.append(
                f"<span class='status-dot {dot_cls}' title='{esc(tip)}'></span>"
            )
        dots_bar = "".join(dots_html) or "<span class='dim'>(无 variant)</span>"

        # 汇总: N/M 活跃 (不计 disabled tab — 它们是归档视图, 永久不跑)
        tab_visible = [t for t in p.get("tabs", []) if not t.get("disabled")]
        tab_total = len(tab_visible)
        tab_ok = sum(1 for t in tab_visible
                      if (t.get("health") or {}).get("state") == "ok")
        h = p.get("health") or {}
        summary_cls = "ok" if h.get("state") == "ok" else (
            "warn" if h.get("state") == "warn" else "err")
        summary_html = (
            f"<span class='{summary_cls}' style='font-size:11px;margin-left:6px'>"
            f"{tab_ok}/{tab_total}</span>"
        )
        if h.get("state") != "ok" and h.get("reason"):
            summary_html += f" <span class='dim' style='font-size:11px'>{esc(h['reason'])}</span>"

        # 认证徽章 — backend credential_manager.status_with_health 返回
        # ok / expired / anonymous / ratelimited / unknown 五档。
        # 红(✗) = 必须马上处理; 橙(⚠) = 不致命但有折扣; 绿(✓) = 健康; 灰(?) = 未知
        auth = p.get("auth") or {}
        auth_state = auth.get("health") or "unknown"
        auth_detail = auth.get("detail") or ""
        auth_badge = ""
        if auth_state == "ok":
            short = auth_detail.split("company=")[0].split(" company")[0][:60]
            auth_badge = (
                f"<span class='auth-pill auth-ok' title='{esc(auth_detail)}'>"
                f"✓ 已登陆</span>"
            )
        elif auth_state == "expired":
            reason = auth_detail[:120] or "凭证失效"
            auth_badge = (
                f"<span class='auth-pill auth-bad' title='{esc(reason)}'>"
                f"✗ 已过期</span>"
            )
        elif auth_state == "anonymous":
            reason = auth_detail[:120] or "匿名 session"
            auth_badge = (
                f"<span class='auth-pill auth-warn' title='{esc(reason)}'>"
                f"⚠ 匿名访问</span>"
            )
        elif auth_state == "ratelimited":
            reason = auth_detail[:120] or "每日额度用尽"
            auth_badge = (
                f"<span class='auth-pill auth-warn' title='{esc(reason)}'>"
                f"⚠ 额度用尽</span>"
            )
        elif auth_state == "unknown":
            _auth_tip = auth_detail or "尚未探活"
            auth_badge = (
                f"<span class='auth-pill auth-unk' title='{esc(_auth_tip)}'>"
                f"? 未知</span>"
            )
        proc_html = f"<span class='dots-bar'>{dots_bar}</span>{summary_html}"

        today_cls = "ok" if p["today_added"] else "dim"
        overview_rows.append(
            f"<tr>"
            f"<td><span class='color-dot' style='background:{p['color']}'></span> "
            f"<b>{esc(p['label'])}</b></td>"
            f"<td>{proc_html}</td>"
            f"<td>{auth_badge}</td>"
            f"<td class='num {today_cls}'>+{p['today_added']}</td>"
            f"<td style='font-size:12px'>{fmt_range(p.get('time_range') or {}, compact=True)}</td>"
            f"<td>{esc(fmt_dt(p['latest_crawled_at']))} "
            f"<span class='dim'>({esc(fmt_delta(p['latest_crawled_at']))})</span></td>"
            f"</tr>"
        )

        # detail 卡片 + tabs
        tabs_btns = []
        tabs_panels = []
        for i, t in enumerate(p["tabs"]):
            active_cls = " active" if i == 0 else ""
            today_badge = f"<span class='badge-today'>+{t.get('today_added', 0)}</span>" if t.get("today_added") else ""
            btn_label = f"{esc(t['label'])}{today_badge}"
            tabs_btns.append(
                f"<button class='tab-btn{active_cls}' data-tab-btn='{esc(t['key'])}' "
                f"onclick=\"switchTab('{esc(p['key'])}','{esc(t['key'])}')\">{btn_label}</button>"
            )

            if "error" in t:
                tabs_panels.append(
                    f"<div class='tab-panel{active_cls}' data-tab-panel='{esc(t['key'])}'>"
                    f"<div class='err'>ERR: {esc(t['error'])}</div></div>"
                )
                continue

            st = t["state"]
            stats = st.get("last_run_stats") or {}
            stats_html = (
                f"<span class='ok'>+{stats.get('added', 0)}</span> / "
                f"<span class='dim'>skip {stats.get('skipped', 0)}</span> / "
                f"<span class='err'>fail {stats.get('failed', 0)}</span>"
            ) if stats else "<span class='dim'>-</span>"
            if st.get("in_progress"):
                stats_html += " <span class='pill prog'>运行中</span>"

            tproc = t.get("process")
            tproc_html = (f"<span class='pill run'>PID {tproc['pid']}</span> <span class='dim'>{esc(tproc['etime'])}</span>"
                          if tproc else "<span class='pill stop'>未运行</span>")

            recent_rows = []
            for it in t["recent"]:
                title = it.get("title") or ""
                rid = it.get("_id")
                extra = it.get("industry") or it.get("organization") or ""
                recent_rows.append(
                    f"<tr>"
                    f"<td class='dim'>{esc(str(rid)[:16])}</td>"
                    f"<td class='title-cell' title='{esc(title)}'>{esc(title[:80])}</td>"
                    f"<td class='dim'>{esc(extra)[:20]}</td>"
                    f"<td class='dim'>{esc(it.get('release_time') or '')[:16]}</td>"
                    f"<td class='dim'>{esc(fmt_delta(it.get('crawled_at')))}</td>"
                    f"</tr>"
                )
            recent_html = (
                "<table><thead><tr><th>ID</th><th>标题</th><th>标签</th><th>时间</th><th>入库</th></tr></thead>"
                f"<tbody>{''.join(recent_rows)}</tbody></table>"
                if recent_rows else "<div class='dim'>暂无</div>"
            )

            log_tail = "\n".join(t["log_tail"]) or "(无日志)"

            tabs_panels.append(
                f"<div class='tab-panel{active_cls}' data-tab-panel='{esc(t['key'])}'>"
                f"<table class='kv'>"
                f"<tr><td>进程</td><td>{tproc_html}</td></tr>"
                f"<tr><td>数据库</td><td>{esc(t['db'])}.{esc(t['collection'])}</td></tr>"
                f"<tr><td>DB 总数</td><td><b>{t['total']:,}</b> · 今日 "
                f"<span class='{'ok' if t['today_added'] else 'dim'}'>+{t['today_added']}</span></td></tr>"
                f"<tr><td>数据时间范围</td><td>{fmt_range(t.get('time_range') or {}, compact=False)}</td></tr>"
                f"<tr><td>上轮结果</td><td>{stats_html}</td></tr>"
                f"<tr><td>last_processed</td><td>{esc(st.get('last_processed_id') or '-')} "
                f"<span class='dim'>({esc(fmt_delta(st.get('last_processed_at')))})</span></td></tr>"
                f"<tr><td>top_id</td><td>{esc(st.get('top_id') or '-')}</td></tr>"
                f"<tr><td>日志</td><td class='dim'>{esc(t['log_path'])}</td></tr>"
                f"</table>"
                f"<h3 style='margin:12px 0 6px;font-size:13px'>最近入库</h3>"
                f"{recent_html}"
                f"<h3 style='margin:12px 0 6px;font-size:13px'>日志 tail</h3>"
                f"<pre>{esc(log_tail)}</pre>"
                f"</div>"
            )

        # 再拉 auth_badge 放到详情卡 header 里, 和总览行保持一致
        detail_auth_badge = ""
        if auth_state == "ok":
            detail_auth_badge = (
                f" <span class='auth-pill auth-ok' title='{esc(auth_detail)}'>"
                f"✓ 已登陆 · {esc(auth_detail[:60])}</span>"
            )
        elif auth_state == "expired":
            detail_auth_badge = (
                f" <span class='auth-pill auth-bad' title='{esc(auth_detail)}'>"
                f"✗ 已过期 · {esc(auth_detail[:80])}</span>"
            )
        elif auth_state == "anonymous":
            detail_auth_badge = (
                f" <span class='auth-pill auth-warn' title='{esc(auth_detail)}'>"
                f"⚠ 匿名访问 · {esc(auth_detail[:80])}</span>"
            )
        elif auth_state == "ratelimited":
            detail_auth_badge = (
                f" <span class='auth-pill auth-warn' title='{esc(auth_detail)}'>"
                f"⚠ 额度用尽 · {esc(auth_detail[:80])}</span>"
            )

        detail_cards.append(
            f"<div class='card' data-platform='{esc(p['key'])}'>"
            f"<h2><span class='color-dot' style='background:{p['color']}'></span> {esc(p['label'])}"
            f"{detail_auth_badge}"
            f" <span class='dim' style='font-size:12px;font-weight:normal'>· 今日 +{p['today_added']}</span></h2>"
            f"<div class='tabs'>{''.join(tabs_btns)}</div>"
            f"{''.join(tabs_panels)}"
            f"</div>"
        )

    html = HTML_TEMPLATE
    subs = {
        "__GENERATED_AT__": esc(snap["generated_at"]),
        "__MONGO_STATUS__": "<span class='ok'>OK</span>" if snap["mongo_ok"] else "<span class='err'>不可用</span>",
        "__PROCESS_COUNT__": str(snap["process_count"]),
        "__REALTIME_COUNT__": str(snap.get("realtime_count", 0)),
        "__BACKFILL_COUNT__": str(snap.get("backfill_count", 0)),
        "__OVERVIEW_ROWS__": "".join(overview_rows) or "<tr><td colspan='6' class='dim'>无数据</td></tr>",
        "__DETAIL_CARDS__": "".join(detail_cards),
        "__SCRIPT_PATH__": esc(__file__),
    }
    for k, v in subs.items():
        html = html.replace(k, v)
    return html


def _json_norm(x: Any):
    if isinstance(x, dict):
        return {k: _json_norm(v) for k, v in x.items()}
    if isinstance(x, list):
        return [_json_norm(v) for v in x]
    if isinstance(x, datetime):
        if x.tzinfo is None:
            x = x.replace(tzinfo=timezone.utc)
        return x.isoformat()
    return x


def run_web(host: str, port: int, feishu_cfg: dict, enable_listener: bool = True):
    from fastapi import FastAPI
    from fastapi.responses import HTMLResponse, JSONResponse
    import uvicorn

    app = FastAPI(title="爬虫监控")

    has_app_creds = bool(feishu_cfg.get("app_id") and feishu_cfg.get("app_secret"))
    enable_push = (
        (has_app_creds and feishu_cfg.get("receive_id"))
        or feishu_cfg.get("webhook")
    )
    # 改成: 只在掉线/恢复时推送, 不做整点
    if enable_push:
        start_health_watchdog(**feishu_cfg, check_interval_s=300)
    if enable_listener and has_app_creds:
        start_command_listener(feishu_cfg["app_id"], feishu_cfg["app_secret"])

    @app.get("/", response_class=HTMLResponse)
    def index():
        return render_html(snapshot())

    @app.get("/api/status", response_class=JSONResponse)
    def status():
        return _json_norm(snapshot())

    @app.get("/api/recent", response_class=JSONResponse)
    def recent(limit: int = 30, mode: str | None = None):
        """mode = None | 'realtime' | 'backfill' — 默认返回全部 (向后兼容)."""
        if mode not in (None, "realtime", "backfill"):
            return {"error": "mode must be realtime, backfill, or omitted"}
        from pymongo import MongoClient
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
        try:
            feed = recent_feed(client, limit=limit, mode=mode)
        finally:
            client.close()
        return _json_norm({"feed": feed, "mode": mode,
                           "generated_at": now_utc().isoformat()})

    @app.post("/api/start-all")
    def start_all_watchers(mode: str = "realtime"):
        if mode not in ("realtime", "historical"):
            return {"ok": False, "error": "mode must be realtime or historical"}
        try:
            results = start_all(mode)
            ok_count = sum(1 for r in results if r.get("ok"))
            return {"ok": True, "mode": mode, "started": ok_count,
                    "total": len(results), "results": results}
        except Exception as e:
            return {"ok": False, "error": f"{type(e).__name__}: {e}"}

    @app.post("/api/push-feishu")
    def push_feishu():
        try:
            resp = push_card(snapshot(), **feishu_cfg)
            return {"ok": resp.get("code") == 0, "resp": resp}
        except ValueError as e:
            return {"ok": False, "error": str(e)}
        except Exception as e:
            return {"ok": False, "error": f"{type(e).__name__}: {e}"}

    print(f"[监控] http://{host}:{port}  (Ctrl+C 退出)", flush=True)
    try:
        uvicorn.run(app, host=host, port=port, log_level="warning")
    except SystemExit:
        raise
    except BaseException as e:
        import traceback
        print(f"[监控] uvicorn 异常退出: {type(e).__name__}: {e}", flush=True)
        traceback.print_exc()
        raise


# ---------------- 入口 ----------------

def main():
    ap = argparse.ArgumentParser(description="爬虫总控监控 (meritco + jinmen + alphapai)")
    ap.add_argument("--web", action="store_true", help="启动 HTTP 仪表盘")
    ap.add_argument("--host", default="0.0.0.0", help="HTTP 监听地址 (默认 0.0.0.0)")
    ap.add_argument("--port", type=int, default=8090, help="HTTP 端口 (默认 8090)")
    ap.add_argument("--refresh", type=float, default=3.0, help="CLI 刷新间隔秒 (默认 3)")
    ap.add_argument("--json", action="store_true", help="打印 JSON 快照后退出")
    # Feishu (两种模式任选其一)
    ap.add_argument("--feishu-app-id", default=FEISHU_APP_ID_ENV,
                    help="App ID (env FEISHU_APP_ID)")
    ap.add_argument("--feishu-app-secret", default=FEISHU_APP_SECRET_ENV,
                    help="App Secret (env FEISHU_APP_SECRET)")
    ap.add_argument("--feishu-receive-id", default=FEISHU_RECEIVE_ID_ENV,
                    help="接收方 ID (chat_id / open_id / email 视 --feishu-receive-id-type 而定)")
    ap.add_argument("--feishu-receive-id-type", default=FEISHU_RECEIVE_ID_TYPE_ENV or "chat_id",
                    choices=["chat_id", "open_id", "user_id", "union_id", "email"],
                    help="接收方类型 (默认 chat_id)")
    ap.add_argument("--feishu-webhook", default=FEISHU_WEBHOOK_ENV,
                    help="飞书自定义机器人 webhook URL (fallback)")
    # 动作
    ap.add_argument("--push-feishu", action="store_true",
                    help="立即推送一次卡片后退出")
    ap.add_argument("--feishu-list-chats", action="store_true",
                    help="列出机器人可访问的群聊 (需要 App ID/Secret)")
    ap.add_argument("--feishu-listen", action="store_true",
                    help="仅启动飞书指令监听 (WS 长连接), 不启 web. 收到 '状态'/'/status' 回复卡片")
    ap.add_argument("--no-listen", action="store_true",
                    help="web 模式下不启动指令监听 (默认会自动启)")
    args = ap.parse_args()

    feishu_cfg = {
        "app_id": args.feishu_app_id,
        "app_secret": args.feishu_app_secret,
        "receive_id": args.feishu_receive_id,
        "receive_id_type": args.feishu_receive_id_type,
        "webhook": args.feishu_webhook,
    }

    if args.json:
        print(json.dumps(_json_norm(snapshot()), ensure_ascii=False, indent=2, default=str))
        return 0

    if args.feishu_list_chats:
        if not (args.feishu_app_id and args.feishu_app_secret):
            print("错误: 需要 --feishu-app-id / --feishu-app-secret (或 env)", file=sys.stderr)
            return 1
        client = FeishuAppClient(args.feishu_app_id, args.feishu_app_secret)
        resp = client.list_chats()
        if resp.get("code") != 0:
            print(f"[飞书] 列群失败: {resp}", file=sys.stderr)
            return 1
        items = (resp.get("data") or {}).get("items") or []
        if not items:
            print("[飞书] 机器人没有可访问的群. 把机器人加到群/私聊后重试.")
            return 0
        print(f"[飞书] 共 {len(items)} 个可推送目标:")
        for it in items:
            print(f"  chat_id={it.get('chat_id')}  name={it.get('name')!r}  "
                  f"type={it.get('chat_mode')}  tenant={it.get('tenant_key')}")
        print("\n把想要的 chat_id 填到 .env 的 FEISHU_RECEIVE_ID= 即可.")
        return 0

    if args.push_feishu:
        try:
            resp = push_card(snapshot(), **feishu_cfg)
        except ValueError as e:
            print(f"错误: {e}", file=sys.stderr)
            return 1
        print(f"[飞书] 推送完成: {resp}")
        return 0 if resp.get("code") == 0 else 2

    if args.feishu_listen:
        if not (args.feishu_app_id and args.feishu_app_secret):
            print("错误: 需要 --feishu-app-id / --feishu-app-secret", file=sys.stderr)
            return 1
        t = start_command_listener(args.feishu_app_id, args.feishu_app_secret)
        try:
            t.join()
        except KeyboardInterrupt:
            print("\n[飞书·指令] 手动终止")
        return 0

    if args.web:
        run_web(args.host, args.port, feishu_cfg,
                enable_listener=not args.no_listen)
        return 0

    run_cli(args.refresh)
    return 0


if __name__ == "__main__":
    sys.exit(main())
