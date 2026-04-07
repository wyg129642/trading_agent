"""Main orchestrator — the heart of the trading agent system.

Initializes all components, starts monitors, and runs the analysis pipeline.
Handles graceful shutdown on Ctrl+C / SIGTERM.

Supports two database backends:
  - SQLite (default): set database.path in settings.yaml
  - PostgreSQL: set DATABASE_URL env var (shares data with web backend)
"""

from __future__ import annotations

import asyncio
import json as _json
import logging
import os
import signal
import sys
from datetime import datetime
from pathlib import Path

import aiohttp
import yaml

from src.alerting.feishu import FeishuAlerter
from src.analysis.llm_client import LLMClient
from src.analysis.pipeline import AnalysisPipeline
from src.analysis.trending_detector import TrendingDetector
from src.clickhouse_store import ClickHouseStore
from src.models import NewsItem
from src.monitors.api_monitor import API_MONITOR_MAP
from src.monitors.base import BaseMonitor
from src.monitors.rss_monitor import RSSMonitor
from src.monitors.web_scraper import WebScraperMonitor
from src.tools.uqer_api import get_market_data
from src.tools.web_search import web_search
from src.utils.browser_manager import BrowserManager
from src.utils.content_fetcher import ContentFetcher
from src.utils.token_tracker import TokenTracker

logger = logging.getLogger(__name__)


def load_config() -> tuple[dict, dict]:
    """Load settings.yaml, sources.yaml, and portfolio_sources.yaml.

    Portfolio sources are kept in a separate file for easy management
    (future web UI can add/delete entries without touching the main config).
    They are merged into the sources list with their group/tag metadata.
    """
    base = Path(__file__).resolve().parent.parent / "config"

    with open(base / "settings.yaml", "r", encoding="utf-8") as f:
        settings = yaml.safe_load(f)
    with open(base / "sources.yaml", "r", encoding="utf-8") as f:
        sources = yaml.safe_load(f)

    # Merge portfolio sources (separate file for easy CRUD via web UI)
    portfolio_path = base / "portfolio_sources.yaml"
    if portfolio_path.exists():
        with open(portfolio_path, "r", encoding="utf-8") as f:
            portfolio = yaml.safe_load(f)
        if portfolio and "sources" in portfolio:
            portfolio_list = portfolio["sources"]
            sources.setdefault("sources", []).extend(portfolio_list)
            logger.info("Loaded %d portfolio sources from portfolio_sources.yaml", len(portfolio_list))

    return settings, sources


def setup_logging(settings: dict) -> None:
    """Configure structured logging to console and file."""
    log_cfg = settings.get("logging", {})
    level = getattr(logging, log_cfg.get("level", "INFO").upper(), logging.INFO)
    log_dir = Path(log_cfg.get("log_dir", "logs"))
    log_dir.mkdir(parents=True, exist_ok=True)

    log_file = log_dir / f"agent_{datetime.now().strftime('%Y%m%d')}.log"

    fmt = "%(asctime)s | %(levelname)-7s | %(name)-25s | %(message)s"
    handlers = [
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_file, encoding="utf-8"),
    ]

    logging.basicConfig(level=level, format=fmt, handlers=handlers)

    # Suppress noisy libraries
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("aiohttp").setLevel(logging.WARNING)
    logging.getLogger("openai").setLevel(logging.WARNING)
    logging.getLogger("feedparser").setLevel(logging.WARNING)


class TradingAgent:
    """Main trading agent orchestrator."""

    def __init__(self):
        self.settings: dict = {}
        self.sources_config: dict = {}
        self.db: Database | None = None
        self.llm: LLMClient | None = None
        self.tracker: TokenTracker | None = None
        self.pipeline: AnalysisPipeline | None = None
        self.alerter: FeishuAlerter | None = None
        self.monitors: list[BaseMonitor] = []
        self.session: aiohttp.ClientSession | None = None
        self.browser_manager: BrowserManager | None = None
        self.ch_store: ClickHouseStore | None = None
        self.trending_detector: TrendingDetector | None = None
        self.news_queue: asyncio.Queue[NewsItem] = asyncio.Queue(maxsize=500)
        self._running = False
        self._shutdown_done = False
        self._tasks: list[asyncio.Task] = []
        self._shutdown_event = asyncio.Event()
        # Track system alert counts per source to cap notifications
        self._source_alert_counts: dict[str, int] = {}
        self._max_source_alerts = 5  # Max Feishu alerts per unhealthy source

    def _update_health(self, status: str, message: str = "", **extra) -> None:
        """Write engine health status to a JSON file for external monitoring."""
        health = {
            "status": status,
            "message": message,
            "pid": os.getpid(),
            "timestamp": datetime.now().isoformat(),
            "monitors": len(self.monitors),
            "queue_size": self.news_queue.qsize() if hasattr(self, "news_queue") else 0,
            **extra,
        }
        try:
            self._health_file.write_text(_json.dumps(health, ensure_ascii=False), encoding="utf-8")
        except Exception:
            pass

    async def initialize(self) -> None:
        """Initialize all components."""
        # Load config
        self.settings, self.sources_config = load_config()
        setup_logging(self.settings)
        logger.info("=" * 60)
        logger.info("AI Trading Agent — Starting up")
        logger.info("=" * 60)

        # Health status file for external monitoring
        self._health_file = Path(os.getenv("ENGINE_HEALTH_FILE", "data/engine_health.json"))
        self._health_file.parent.mkdir(parents=True, exist_ok=True)
        self._update_health("starting", "Initializing components")

        # Database — prefer PostgreSQL (shared with web backend) if configured
        pg_url = os.getenv("DATABASE_URL", "")
        redis_url = os.getenv("REDIS_URL", "")
        if pg_url:
            from src.pg_database import PostgresDatabase
            self.db = PostgresDatabase(pg_url, redis_url=redis_url or None)
            await self.db.initialize()
            logger.info("Using PostgreSQL database (shared with web backend)")
        else:
            from src.database import Database
            db_path = self.settings.get("database", {}).get("path", "data/news_monitor.db")
            self.db = Database(db_path)
            await self.db.initialize()
            logger.info("Using SQLite database at %s", db_path)

        # Token tracker
        budget_cfg = self.settings.get("token_budget", {})
        self.tracker = TokenTracker(
            daily_budget_cny=budget_cfg.get("daily_budget_cny", 50.0),
            rate_limit_tpm=budget_cfg.get("rate_limit_tpm", 1_000_000),
        )
        await self.tracker.load_today_from_db(self.db)

        # HTTP sessions
        # Proxy session for international sites (uses HTTP_PROXY from env)
        connector = aiohttp.TCPConnector(
            limit=self.settings.get("system", {}).get("max_concurrent_requests", 20),
            ttl_dns_cache=300,
        )
        self.session = aiohttp.ClientSession(connector=connector, trust_env=True)
        # Direct session for Chinese sites (bypasses proxy)
        direct_connector = aiohttp.TCPConnector(
            limit=self.settings.get("system", {}).get("max_concurrent_requests", 20),
            ttl_dns_cache=300,
        )
        self.direct_session = aiohttp.ClientSession(connector=direct_connector, trust_env=False)

        # Headless browser (Playwright)
        self.browser_manager = BrowserManager()

        # LLM client
        self.llm = LLMClient(self.settings, tracker=self.tracker)

        # Feishu alerter
        feishu_cfg = self.settings.get("feishu", {})
        alert_mgmt = self.settings.get("alert_management", {})
        self.alerter = FeishuAlerter(
            webhook_url=feishu_cfg["webhook_url"],
            alert_levels=feishu_cfg.get("alert_levels", ["critical", "high", "medium"]),
            max_alerts_per_ticker_per_hour=alert_mgmt.get("max_alerts_per_ticker_per_hour", 3),
            dedup_window_minutes=alert_mgmt.get("dedup_window_minutes", 60),
        )

        # Tool executor for legacy research agent (DuckDuckGo + Uqer)
        uqer_token = self.settings.get("uqer", {}).get("token", "")

        async def tool_executor(name: str, args: dict) -> str:
            if name == "web_search":
                return await web_search(
                    args["query"],
                    max_results=self.settings.get("web_search", {}).get("max_results", 5),
                )
            elif name == "get_market_data":
                return await get_market_data(
                    args["ticker"], args["begin_date"], args["end_date"], token=uqer_token
                )
            else:
                return f"Unknown tool: {name}"

        # Content fetcher
        fetch_cfg = self.settings.get("content_fetch", {})
        content_fetcher = ContentFetcher(
            session=self.session,
            max_content_chars=fetch_cfg.get("max_content_chars", 10000),
            timeout_seconds=fetch_cfg.get("timeout_seconds", 15),
            rate_limit_rps=fetch_cfg.get("rate_limit_rps", 1.0),
            browser_manager=self.browser_manager,
        ) if fetch_cfg.get("enabled", True) else None

        # Analysis pipeline (new 3-phase workflow with Baidu Search)
        self.pipeline = AnalysisPipeline(
            llm=self.llm, db=self.db, settings=self.settings,
            tool_executor=tool_executor, content_fetcher=content_fetcher,
            uqer_token=uqer_token,
        )

        # Create monitors from sources.yaml
        self._create_monitors()

        # Trending detector — alerts when portfolio holdings appear on hot lists
        portfolio_list = self.sources_config.get("sources", [])
        companies = self.sources_config.get("companies", {})
        self.trending_detector = TrendingDetector(portfolio_list, companies)

        # ClickHouse (optional)
        ch_cfg = self.settings.get("clickhouse", {})
        if ch_cfg.get("enabled", False):
            try:
                self.ch_store = ClickHouseStore(ch_cfg)
                await self.ch_store.initialize()
            except Exception as e:
                logger.warning("ClickHouse init failed (continuing without): %s", e)
                self.ch_store = None

        logger.info("Initialized %d monitors", len(self.monitors))
        for m in self.monitors:
            logger.info("  [%s] %s — %s", m.priority.upper(), m.name, "enabled" if m.enabled else "DISABLED")

    def _create_monitors(self) -> None:
        """Instantiate monitor objects from sources.yaml configuration."""
        source_list = self.sources_config.get("sources", [])

        for src_cfg in source_list:
            if not src_cfg.get("enabled", True):
                continue

            src_type = src_cfg.get("type", "")
            # Use direct (no-proxy) session for china-market sources
            market = src_cfg.get("market", "china")
            session = self.direct_session if market == "china" else self.session

            if src_type == "rss":
                monitor = RSSMonitor(src_cfg, session)
            elif src_type == "web_scraper":
                monitor = WebScraperMonitor(src_cfg, session, browser_manager=self.browser_manager)
            elif src_type == "api":
                api_type = src_cfg.get("api_type", "")
                monitor_class = API_MONITOR_MAP.get(api_type)
                if monitor_class:
                    monitor = monitor_class(src_cfg, session)
                else:
                    logger.warning("Unknown api_type: %s for source %s", api_type, src_cfg.get("name"))
                    continue
            else:
                logger.warning("Unknown source type: %s for source %s", src_type, src_cfg.get("name"))
                continue

            self.monitors.append(monitor)

    async def _warm_start_monitor(self, monitor: BaseMonitor) -> None:
        """Pre-populate a monitor's dedup cache from the database."""
        existing = await self.db.get_content_hashes_by_source(monitor.name)
        monitor._seen_hashes.update(existing)

        last_dt = await self.db.get_latest_published_at(monitor.name)
        monitor._last_seen_dt = last_dt

        if existing:
            logger.info(
                "[%s] Warm-start: loaded %d known hashes (last_seen=%s)",
                monitor.name, len(existing),
                last_dt.isoformat() if last_dt else "none",
            )

    async def _run_monitor(self, monitor: BaseMonitor) -> None:
        """Polling loop for a single monitor."""
        interval = monitor.interval_seconds(self.settings)

        await self._warm_start_monitor(monitor)

        logger.info("[%s] Starting polling loop (interval=%ds)", monitor.name, interval)

        while self._running:
            try:
                new_items = await monitor.poll()

                for item in new_items:
                    is_new = await self.db.save_news_item(item)
                    if is_new:
                        try:
                            self.news_queue.put_nowait(item)
                        except asyncio.QueueFull:
                            logger.warning("News queue full, dropping item: %s", item.title[:50])

                await self.db.update_source_health(monitor.health)

                if not monitor.health.is_healthy:
                    count = self._source_alert_counts.get(monitor.name, 0)
                    if count < self._max_source_alerts:
                        self._source_alert_counts[monitor.name] = count + 1
                        await self.alerter.send_system_alert(
                            f"数据源异常: {monitor.name} 连续失败 {monitor.health.consecutive_failures} 次\n"
                            f"URL: {monitor.url}\n"
                            f"(通知 {count + 1}/{self._max_source_alerts}，达到上限后将不再通知)"
                        )
                elif monitor.name in self._source_alert_counts:
                    # Source recovered — reset counter and notify
                    del self._source_alert_counts[monitor.name]
                    await self.alerter.send_system_alert(
                        f"数据源恢复: {monitor.name} 已恢复正常 ✅"
                    )

            except Exception as e:
                logger.error("[%s] Monitor loop error: %s", monitor.name, e)

            for _ in range(interval):
                if not self._running:
                    return
                await asyncio.sleep(1)

    def _is_similar_to_recent(self, item: NewsItem) -> bool:
        """Check if this item is too similar to a recently processed item.

        Two strategies:
        1. Character-level Jaccard > 0.5 (catches near-duplicates)
        2. Same-source + shared topic prefix (catches "英国X月失业率" / "英国X月就业人数")
           When titles from the same source share a leading topic keyword and both
           contain data patterns like "X%, 前值Y%", only the first one passes.
        """
        import re as _re
        import time

        if not hasattr(self, "_recent_titles"):
            self._recent_titles: list[tuple[float, str, str, set]] = []  # (ts, source, title, char_set)

        now = time.time()
        # Evict old entries (> 10 minutes)
        self._recent_titles = [e for e in self._recent_titles if now - e[0] < 600]

        # Build character set (strip digits/punctuation)
        clean = _re.sub(r"[\d\s%,.，。、：:；;！!？?（）()\-—·]", "", item.title)
        if len(clean) < 4:
            return False
        char_set = set(clean)

        # Extract topic prefix: first 2-4 Chinese characters (e.g. "英国", "美联储", "苹果公司")
        topic_match = _re.match(r"^([\u4e00-\u9fff]{2,4})", item.title)
        topic_prefix = topic_match.group(1) if topic_match else ""
        # Detect "data release" pattern: contains numbers + %, 前值, 预期
        is_data_release = bool(_re.search(r"[前值预期]\s*[\d.]", item.title))

        for ts, src, prev_title, prev_cs in self._recent_titles:
            if not prev_cs:
                continue

            # Strategy 1: Character Jaccard similarity > 0.5
            intersection = char_set & prev_cs
            union = char_set | prev_cs
            if union and len(intersection) / len(union) > 0.5:
                logger.info(
                    "[Pipeline] Similar to recent (jaccard), skipping: '%s' ≈ '%s'",
                    item.title[:40], prev_title[:40],
                )
                return True

            # Strategy 2: Same source + same topic prefix + both are data releases
            if (is_data_release and topic_prefix and
                    src == item.source_name and prev_title.startswith(topic_prefix)):
                prev_is_data = bool(_re.search(r"[前值预期]\s*[\d.]", prev_title))
                if prev_is_data:
                    logger.info(
                        "[Pipeline] Same-topic data batch, skipping: '%s' ≈ '%s'",
                        item.title[:40], prev_title[:40],
                    )
                    return True

        self._recent_titles.append((now, item.source_name, item.title, char_set))
        return False

    async def _analysis_consumer(self) -> None:
        """Consumer loop: picks items from queue and runs analysis pipeline."""
        logger.info("[Pipeline] Analysis consumer started")

        while self._running:
            try:
                try:
                    item = await asyncio.wait_for(self.news_queue.get(), timeout=2.0)
                except asyncio.TimeoutError:
                    continue

                # Skip items too similar to recently processed ones (aggregation)
                if self._is_similar_to_recent(item):
                    self.news_queue.task_done()
                    continue

                logger.info("[Pipeline] Processing: %s", item.title[:60])

                # Check if any portfolio holding is trending (zero LLM cost)
                if self.trending_detector:
                    matches = self.trending_detector.check_item(item)
                    if matches:
                        await self.trending_detector.alert_if_trending(
                            item, matches, self.alerter
                        )

                result = await self.pipeline.process(item)

                stage = result["stage"]
                filter_res = result["filter"]
                analysis = result["analysis"]
                research = result["research"]
                alert_level = result["alert_level"]
                signal = result.get("signal_score")
                search_verification = result.get("search_verification")
                initial_evaluation = result.get("initial_evaluation")
                deep_research = result.get("deep_research")
                final_assessment = result.get("final_assessment")

                if filter_res and not filter_res.is_relevant:
                    logger.debug(
                        "[Pipeline] Not relevant (score=%.2f): %s",
                        filter_res.relevance_score, item.title[:50],
                    )

                # Send alert for items passing Phase 1
                if alert_level and analysis:
                    await self.alerter.send_alert(
                        news=item,
                        analysis=analysis,
                        research=research,
                        alert_level=alert_level,
                        signal_score=signal.composite_score if signal else 0.0,
                        search_verification=search_verification,
                        signal_score_obj=signal,
                        initial_evaluation=initial_evaluation,
                        deep_research=deep_research,
                        final_assessment=final_assessment,
                    )

                # Write to ClickHouse
                if self.ch_store and filter_res:
                    try:
                        await self.ch_store.insert_news_analysis(
                            item=item,
                            filter_res=filter_res,
                            analysis=analysis,
                            signal=signal,
                            research=research,
                        )
                        if analysis:
                            await self.ch_store.insert_ticker_events(
                                item=item, analysis=analysis, signal=signal,
                            )
                    except Exception as e:
                        logger.debug("[ClickHouse] Insert error: %s", e)

                self.news_queue.task_done()

            except Exception as e:
                logger.error("[Pipeline] Consumer error: %s", e, exc_info=True)

    async def _periodic_tasks(self) -> None:
        """Periodic housekeeping: stats logging, token persistence, budget alerts."""
        cycle = 0
        while self._running:
            await asyncio.sleep(60)
            cycle += 1

            if cycle % 5 == 0 and self.tracker:
                saved = await self.tracker.persist_to_db(self.db)
                if saved:
                    logger.debug("[Tokens] Persisted %d records to DB", saved)

            if self.ch_store:
                try:
                    await self.ch_store.flush_all()
                except Exception as e:
                    logger.debug("[ClickHouse] Periodic flush error: %s", e)

            digest_interval = self.settings.get("alert_management", {}).get("digest_interval_minutes", 30)
            if cycle % digest_interval == 0 and self.alerter:
                await self.alerter.send_digest()

            if cycle % 30 == 0:
                try:
                    stats = await self.db.get_stats()
                    logger.info(
                        "[Stats] news=%d, filtered=%d, analyzed=%d, researched=%d",
                        stats.get("news_items", 0),
                        stats.get("filter_results", 0),
                        stats.get("analysis_results", 0),
                        stats.get("research_reports", 0),
                    )
                    self._update_health(
                        "running",
                        f"{len(self.monitors)} monitors, queue={self.news_queue.qsize()}",
                        stats=stats,
                    )
                except Exception as e:
                    logger.debug("Stats logging error: %s", e)

                if self.tracker:
                    logger.info("\n%s", self.tracker.format_report(hours=24))

            if cycle % 30 == 0 and self.tracker:
                remaining = self.tracker.daily_budget_remaining_cny()
                budget = self.tracker.daily_budget_cny
                if remaining < budget * 0.5:
                    summary = self.tracker.summary(hours=24)
                    await self.alerter.send_system_alert(
                        f"⚠️ Token预算告警\n"
                        f"今日已用: ¥{summary['total_cost_cny']:.4f} / ¥{budget:.2f}\n"
                        f"剩余: ¥{remaining:.4f}\n"
                        f"API调用: {summary['total_calls']} 次\n"
                        f"总Token: {summary['total_tokens']:,}"
                    )

    async def run(self) -> None:
        """Main run loop."""
        await self.initialize()
        self._running = True

        await self.alerter.send_system_alert(
            f"🚀 AI交易情报系统启动\n"
            f"数据源: {len(self.monitors)} 个\n"
            f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )

        for monitor in self.monitors:
            self._tasks.append(asyncio.create_task(
                self._run_monitor(monitor), name=f"monitor:{monitor.name}"
            ))

        self._tasks.append(asyncio.create_task(
            self._analysis_consumer(), name="analysis_consumer"
        ))

        self._tasks.append(asyncio.create_task(
            self._periodic_tasks(), name="periodic_tasks"
        ))

        logger.info("All %d tasks started. System is running.", len(self._tasks))
        logger.info("Press Ctrl+C to stop gracefully.")

        self._update_health("running", f"{len(self.monitors)} monitors active")

        await self._shutdown_event.wait()

        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

    async def shutdown(self) -> None:
        """Gracefully shut down all components."""
        if self._shutdown_done:
            return
        self._shutdown_done = True

        was_running = self._running
        self._running = False
        self._shutdown_event.set()
        self._update_health("stopped", "Graceful shutdown")

        if was_running:
            logger.info("Shutting down gracefully...")

        if self.tracker and self.db:
            saved = await self.tracker.persist_to_db(self.db)
            if saved:
                logger.info("[Tokens] Final usage persisted (%d records)", saved)
                logger.info("\n%s", self.tracker.format_report(hours=24))

        if self.ch_store:
            try:
                await self.ch_store.close()
            except Exception as e:
                logger.debug("ClickHouse close error: %s", e)

        if was_running and self.alerter:
            try:
                await self.alerter.send_system_alert("⏹️ AI交易情报系统已停止")
            except Exception as e:
                logger.debug("Shutdown Feishu alert failed: %s", e)

        if self.alerter:
            await self.alerter.close()
        if self.browser_manager:
            await self.browser_manager.close()
        if self.session:
            await self.session.close()
        if hasattr(self, 'direct_session') and self.direct_session:
            await self.direct_session.close()
        if self.db:
            await self.db.close()

        logger.info("Shutdown complete.")


async def main() -> None:
    """Entry point for the trading agent."""
    agent = TradingAgent()

    loop = asyncio.get_running_loop()
    shutdown_called = False

    def _signal_handler():
        nonlocal shutdown_called
        if shutdown_called:
            logger.warning("Force exit (second signal received)")
            sys.exit(1)
        shutdown_called = True
        logger.info("Received stop signal, shutting down...")
        asyncio.ensure_future(agent.shutdown())

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    try:
        await agent.run()
    finally:
        await agent.shutdown()
