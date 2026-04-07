"""ProactiveScanner — main orchestrator for proactive portfolio monitoring.

Responsibilities:
- Parse portfolio holdings from config
- Schedule scans based on market hours
- Batch stocks and stagger scan execution
- Coordinate the evaluation pipeline
- Send alerts and update baselines
- Generate morning briefings
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, time, timedelta, timezone
from pathlib import Path

import yaml

from engine.proactive.alert_formatter import ProactiveAlertFormatter
from engine.proactive.baseline import BaselineStore
from engine.proactive.data_sources.internal_db import InternalDBPlugin
from engine.proactive.data_sources.market_data import MarketDataPlugin
from engine.proactive.data_sources.web_search import WebSearchPlugin
from engine.proactive.evaluator import StockEvaluator
from engine.proactive.models import PortfolioHolding, StockBaseline
from engine.proactive.stock_context import StockContextBuilder

logger = logging.getLogger(__name__)


# Market hours in UTC
MARKET_HOURS = {
    "china": {"open": time(1, 30), "close": time(7, 0)},    # 9:30-15:00 CST
    "us":    {"open": time(13, 30), "close": time(20, 0)},   # 9:30-16:00 ET
    "hk":    {"open": time(1, 30), "close": time(8, 0)},     # 9:30-16:00 HKT
    "kr":    {"open": time(0, 0), "close": time(6, 0)},      # 9:00-15:00 KST
    "jp":    {"open": time(0, 0), "close": time(6, 0)},      # 9:00-15:00 JST
}


def _is_market_open(market: str, now_utc: datetime | None = None) -> bool:
    """Check if a market is currently open (simplified, ignores holidays)."""
    now = now_utc or datetime.now(timezone.utc)
    if now.weekday() >= 5:  # Saturday/Sunday
        return False
    hours = MARKET_HOURS.get(market)
    if not hours:
        return False
    current_time = now.time()
    return hours["open"] <= current_time <= hours["close"]


def _is_near_market_hours(market: str, buffer_hours: int = 2) -> bool:
    """Check if we're within buffer_hours of market open/close."""
    now = datetime.now(timezone.utc)
    if now.weekday() >= 5:
        return False
    hours = MARKET_HOURS.get(market)
    if not hours:
        return False
    current_minutes = now.hour * 60 + now.minute
    open_minutes = hours["open"].hour * 60 + hours["open"].minute
    close_minutes = hours["close"].hour * 60 + hours["close"].minute
    buffer = buffer_hours * 60
    return (open_minutes - buffer <= current_minutes <= close_minutes + buffer)


def _get_scan_interval(market: str, cfg: dict) -> int:
    """Get the appropriate scan interval in minutes based on market state."""
    now = datetime.now(timezone.utc)

    if now.weekday() >= 5:
        return cfg.get("scan_interval_weekend_min", 240)

    if _is_market_open(market):
        return cfg.get("scan_interval_active_min", 30)

    if _is_near_market_hours(market):
        return cfg.get("scan_interval_prepost_min", 60)

    return cfg.get("scan_interval_closed_min", 120)


class ProactiveScanner:
    """Main proactive portfolio monitoring orchestrator."""

    def __init__(
        self,
        llm,
        db,
        tracker,
        alerter,
        settings: dict,
        content_fetcher=None,
        uqer_token: str = "",
        session_direct=None,
        session_proxy=None,
        proxy_url: str = "",
        ch_store=None,
    ):
        self._llm = llm
        self._db = db
        self._tracker = tracker
        self._alerter = alerter
        self._settings = settings
        self._content_fetcher = content_fetcher
        self._uqer_token = uqer_token
        self._ch_store = ch_store  # Optional ClickHouseStore for breaking news persistence

        self._cfg = settings.get("proactive_monitoring", {})
        self._dry_run = self._cfg.get("dry_run", False)
        # Proactive alerts can use a separate webhook (for testing)
        self._webhook_url = self._cfg.get("webhook_url") or alerter.webhook_url
        self._running = False

        # Holdings loaded during initialize()
        self.holdings: list[PortfolioHolding] = []
        self._batches: list[list[PortfolioHolding]] = []

        # Components initialized during initialize()
        self._baseline_store: BaselineStore | None = None
        self._context_builder: StockContextBuilder | None = None
        self._evaluator: StockEvaluator | None = None
        self._alert_formatter: ProactiveAlertFormatter | None = None

        # Morning briefing tracking
        self._last_briefing_date: str = ""
        self._daily_scan_summaries: list[dict] = []

    async def initialize(self) -> None:
        """Initialize all components and load portfolio holdings."""

        # Load portfolio holdings from config
        self.holdings = self._load_holdings()
        if not self.holdings:
            logger.warning("[Proactive] No portfolio holdings found — scanner will be idle")
            return

        # Create batches
        batch_size = self._cfg.get("batch_size", 8)
        self._batches = [
            self.holdings[i:i + batch_size]
            for i in range(0, len(self.holdings), batch_size)
        ]

        # Initialize baseline store
        redis = getattr(self._db, "_redis", None)
        self._baseline_store = BaselineStore(self._db, redis=redis)

        # Initialize data source plugins (v3: time-gated)
        breaking_news_hours = self._cfg.get("breaking_news_window_hours", 24)
        internal_lookback = breaking_news_hours + 2  # Slightly wider for ingestion delay
        plugins = [
            InternalDBPlugin(self._db, lookback_hours=internal_lookback),
            WebSearchPlugin(
                self._settings,
                queries_per_stock=self._cfg.get("external_queries_per_stock", 3),
                breaking_news_window_hours=breaking_news_hours,
            ),
            MarketDataPlugin(lookback_days=30),
        ]
        self._context_builder = StockContextBuilder(
            plugins, content_fetcher=self._content_fetcher,
        )

        # Initialize evaluator
        self._evaluator = StockEvaluator(
            llm=self._llm,
            settings=self._settings,
            content_fetcher=self._content_fetcher,
            uqer_token=self._uqer_token,
        )

        # Initialize alert formatter
        self._alert_formatter = ProactiveAlertFormatter(self._alerter)

        # Initialize baselines for all holdings (create if not exists)
        for holding in self.holdings:
            existing = await self._baseline_store.load(holding.ticker)
            if not existing:
                await self._baseline_store.initialize_baseline(
                    ticker=holding.ticker,
                    name_cn=holding.name_cn,
                    market=holding.market,
                )
                logger.info("[Proactive] Initialized baseline for %s (%s)", holding.name_cn, holding.ticker)

        logger.info(
            "[Proactive] Initialized: %d holdings, %d batches",
            len(self.holdings), len(self._batches),
        )

    def _load_holdings(self) -> list[PortfolioHolding]:
        """Load portfolio holdings from portfolio_sources.yaml."""
        config_path = Path(__file__).resolve().parent.parent.parent / "config" / "portfolio_sources.yaml"
        if not config_path.exists():
            logger.warning("[Proactive] portfolio_sources.yaml not found at %s", config_path)
            return []

        with open(config_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)

        if not data or "sources" not in data:
            return []

        # Deduplicate by ticker
        seen_tickers: set[str] = set()
        holdings = []
        for src_cfg in data["sources"]:
            if src_cfg.get("group") != "portfolio":
                continue
            ticker = src_cfg.get("stock_ticker", "")
            if not ticker or ticker in seen_tickers:
                continue
            seen_tickers.add(ticker)

            holding = PortfolioHolding.from_source_config(src_cfg)
            if holding.ticker:
                holdings.append(holding)

        logger.info("[Proactive] Loaded %d unique portfolio holdings", len(holdings))
        return holdings

    # ------------------------------------------------------------------
    # Main run loop
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Main scan loop — runs indefinitely until shutdown."""
        self._running = True
        logger.info("[Proactive] Scanner started")

        while self._running:
            try:
                await self._run_scan_cycle()
            except Exception as e:
                logger.error("[Proactive] Scan cycle error: %s", e, exc_info=True)

            # Wait for next cycle
            # Use the shortest interval among all markets in the portfolio
            intervals = set()
            for h in self.holdings:
                intervals.add(_get_scan_interval(h.market, self._cfg))
            min_interval = min(intervals) if intervals else 30

            logger.info("[Proactive] Next scan cycle in %d minutes", min_interval)

            # Sleep in 1-second increments for responsive shutdown
            for _ in range(min_interval * 60):
                if not self._running:
                    return
                await asyncio.sleep(1)

    async def _run_scan_cycle(self) -> None:
        """Run one complete scan cycle across all batches."""
        now = datetime.now(timezone.utc)
        batch_stagger = self._cfg.get("batch_stagger_min", 7) * 60  # seconds

        logger.info(
            "[Proactive] Starting scan cycle at %s (%d batches)",
            now.strftime("%H:%M:%S"), len(self._batches),
        )

        # Check for morning briefing
        await self._check_morning_briefing()

        for batch_idx, batch in enumerate(self._batches):
            if not self._running:
                return

            logger.info(
                "[Proactive] Batch %d/%d: %s",
                batch_idx + 1, len(self._batches),
                ", ".join(h.ticker for h in batch),
            )

            # Process stocks in this batch concurrently
            tasks = [self._scan_stock(h) for h in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for holding, result in zip(batch, results):
                if isinstance(result, Exception):
                    logger.error(
                        "[Proactive:%s] Scan failed: %s",
                        holding.ticker, result,
                    )

            # Stagger between batches
            if batch_idx < len(self._batches) - 1 and self._running:
                logger.debug("[Proactive] Staggering %ds before next batch", batch_stagger)
                for _ in range(batch_stagger):
                    if not self._running:
                        return
                    await asyncio.sleep(1)

    async def _scan_stock(self, holding: PortfolioHolding) -> None:
        """Scan a single stock: context assembly → 5-stage breaking news pipeline."""

        # Check if this stock's market warrants a scan right now
        interval = _get_scan_interval(holding.market, self._cfg)

        # Load baseline
        baseline = await self._baseline_store.load(holding.ticker)
        if not baseline:
            baseline = StockBaseline(ticker=holding.ticker)

        # Check staleness — skip if recently scanned
        since_last = datetime.now(timezone.utc) - baseline.last_scan_time
        if since_last < timedelta(minutes=interval - 5):
            logger.debug(
                "[Proactive:%s] Recently scanned (%s ago), skipping",
                holding.ticker, since_last,
            )
            return

        logger.info("[Proactive:%s] Scanning...", holding.ticker)

        # Stage A: Context Assembly (all plugins in parallel)
        snapshot = await self._context_builder.build_context(holding, baseline)

        # Stage 0-4: Breaking news evaluation pipeline
        result = await self._evaluator.evaluate(holding, baseline, snapshot)

        # Update baseline — v3: event-based dedup instead of narrative
        new_content_ids = set()
        for items in snapshot.internal_context.source_items.values():
            new_content_ids.update(item.get("id", "") for item in items)
        new_content_ids.discard("")

        self._baseline_store.update_after_scan(
            baseline,
            new_content_ids=new_content_ids,
            sentiment=result.full_analysis.get("sentiment") if result.full_analysis else None,
        )

        # Add detected events to dedup log
        if result.breaking_news_detected and result.new_developments:
            for dev in result.new_developments:
                self._baseline_store.add_known_event(baseline, dev)
            self._baseline_store.update_after_delta(baseline, result.new_developments)

        # Send alert if warranted
        if result.should_alert:
            await self._send_alert(result, baseline)

        # Persist breaking news to ClickHouse (material+ results, not just alerted ones)
        if self._ch_store and result.breaking_news_detected and result.news_materiality in ("material", "critical"):
            try:
                await self._ch_store.insert_breaking_news(result)
            except Exception as e:
                logger.error("[Proactive:%s] ClickHouse breaking news insert failed: %s", holding.ticker, e)

        # Save baseline and scan result
        await self._baseline_store.save(baseline)
        await self._baseline_store.save_scan_result(result)

        # Track for morning briefing
        self._daily_scan_summaries.append({
            "ticker": holding.ticker,
            "name": holding.name_cn,
            "news_materiality": result.news_materiality,
            "news_summary": result.news_summary[:200],
            "deep_research": result.deep_research_performed,
            "alerted": result.should_alert,
            "scan_count": 1,
        })

        logger.info(
            "[Proactive:%s] Scan complete: materiality=%s, novelty=%s, alert=%s, tokens=%d",
            holding.ticker, result.news_materiality, result.novelty_status,
            result.should_alert, result.tokens_used,
        )

    async def _send_alert(self, result, baseline: StockBaseline) -> None:
        """Send a proactive alert via Feishu."""
        holding = result.holding
        ticker = holding.ticker

        # Check suppression
        max_per_4h = self._cfg.get("max_alerts_per_stock_per_4h", 1)
        max_per_hour = self._cfg.get("max_alerts_per_hour_global", 5)
        reason = self._alert_formatter.should_suppress(ticker, max_per_4h, max_per_hour)
        if reason:
            logger.info("[Proactive:%s] Alert suppressed: %s", ticker, reason)
            return

        # Build and send card
        card_payload = self._alert_formatter.build_card(result)

        if self._dry_run:
            logger.info(
                "[Proactive:%s] DRY RUN — would send alert (confidence=%.2f): %s",
                ticker, result.alert_confidence, result.delta_description[:100],
            )
            return

        try:
            resp = await self._post_feishu(card_payload)
            if resp and resp.status_code == 200:
                logger.info("[Proactive:%s] Alert sent successfully", ticker)
                self._alert_formatter.record_alert(ticker)
                self._baseline_store.update_after_alert(baseline)
            elif resp:
                logger.warning(
                    "[Proactive:%s] Alert HTTP %d: %s",
                    ticker, resp.status_code, resp.text[:200],
                )
        except Exception as e:
            logger.error("[Proactive:%s] Alert send failed: %s", ticker, e)

    # ------------------------------------------------------------------
    # Morning briefing
    # ------------------------------------------------------------------

    async def _check_morning_briefing(self) -> None:
        """Check if it's time to send the morning briefing."""
        if not self._cfg.get("morning_briefing_enabled", True):
            return

        now = datetime.now(timezone.utc)
        briefing_hour_cst = self._cfg.get("morning_briefing_hour_cst", 8)
        # CST is UTC+8
        target_utc_hour = (briefing_hour_cst - 8) % 24

        today_str = now.strftime("%Y-%m-%d")
        if self._last_briefing_date == today_str:
            return

        if now.hour == target_utc_hour and now.minute < 15:
            await self._send_morning_briefing()
            self._last_briefing_date = today_str

    async def _send_morning_briefing(self) -> None:
        """Generate and send the morning briefing."""
        if not self._daily_scan_summaries:
            logger.info("[Proactive] No scan data for morning briefing")
            return

        # Aggregate summaries per ticker (take latest for each)
        latest: dict[str, dict] = {}
        for s in self._daily_scan_summaries:
            ticker = s["ticker"]
            if ticker not in latest or s.get("delta_magnitude", "none") != "none":
                latest[ticker] = s

        summaries = list(latest.values())

        payload = self._alert_formatter.build_morning_briefing(summaries)

        if self._dry_run:
            logger.info("[Proactive] DRY RUN — morning briefing:\n%s", payload.get("content", {}).get("text", ""))
            return

        try:
            resp = await self._post_feishu(payload)
            if resp and resp.status_code == 200:
                logger.info("[Proactive] Morning briefing sent")
            elif resp:
                logger.warning("[Proactive] Morning briefing HTTP %d", resp.status_code)
        except Exception as e:
            logger.error("[Proactive] Morning briefing failed: %s", e)

        # Reset daily summaries
        self._daily_scan_summaries = []

    # ------------------------------------------------------------------
    # Feishu posting (proxy-aware)
    # ------------------------------------------------------------------

    async def _post_feishu(self, payload: dict):
        """Post to Feishu webhook, bypassing SOCKS proxy for domestic endpoint."""
        import httpx
        import os

        # Temporarily remove proxy env vars (Feishu is a domestic endpoint)
        saved = {}
        for key in ("HTTP_PROXY", "http_proxy", "HTTPS_PROXY", "https_proxy", "ALL_PROXY", "all_proxy"):
            if key in os.environ:
                saved[key] = os.environ.pop(key)
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.post(
                    self._webhook_url,
                    json=payload,
                    headers={"Content-Type": "application/json"},
                )
                return resp
        finally:
            os.environ.update(saved)

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------

    async def shutdown(self) -> None:
        """Graceful shutdown."""
        self._running = False
        logger.info("[Proactive] Scanner shutting down")
