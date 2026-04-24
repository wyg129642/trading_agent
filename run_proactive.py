#!/usr/bin/env python3
"""Standalone runner for the proactive portfolio scanner.

Runs the proactive scanner independently of the main engine,
connecting to the same PostgreSQL database and using the same
LLM/search infrastructure.

Usage:
    python run_proactive.py               # Run the full scanner loop
    python run_proactive.py --test INTC   # Test scan a single stock
    python run_proactive.py --test-all    # Test scan all stocks once (no loop)
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys
from datetime import datetime
from pathlib import Path

import yaml

# Ensure project root is on path
sys.path.insert(0, str(Path(__file__).resolve().parent))


def load_settings() -> dict:
    base = Path(__file__).resolve().parent / "config"
    with open(base / "settings.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def setup_logging(settings: dict) -> None:
    log_cfg = settings.get("logging", {})
    level = getattr(logging, log_cfg.get("level", "INFO").upper(), logging.INFO)
    log_dir = Path(log_cfg.get("log_dir", "logs"))
    log_dir.mkdir(parents=True, exist_ok=True)

    log_file = log_dir / f"proactive_{datetime.now().strftime('%Y%m%d')}.log"
    fmt = "%(asctime)s | %(levelname)-7s | %(name)-25s | %(message)s"
    handlers = [
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_file, encoding="utf-8"),
    ]
    logging.basicConfig(level=level, format=fmt, handlers=handlers)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("aiohttp").setLevel(logging.WARNING)
    logging.getLogger("openai").setLevel(logging.WARNING)


logger = logging.getLogger("proactive_runner")


async def run_scanner(test_ticker: str | None = None, test_all: bool = False):
    """Initialize and run the proactive scanner."""
    import aiohttp
    from engine.analysis.llm_client import LLMClient
    from engine.pg_database import PostgresDatabase
    from engine.alerting.feishu import FeishuAlerter
    from engine.utils.token_tracker import TokenTracker
    from engine.utils.content_fetcher import ContentFetcher
    from engine.utils.browser_manager import BrowserManager
    from engine.proactive.scanner import ProactiveScanner

    settings = load_settings()
    setup_logging(settings)

    logger.info("=" * 60)
    logger.info("Proactive Portfolio Scanner — Starting")
    logger.info("=" * 60)

    # Database
    pg_url = os.getenv("DATABASE_URL", "")
    if not pg_url:
        pg_host = os.getenv("POSTGRES_HOST", "localhost")
        pg_port = os.getenv("POSTGRES_PORT", "5432")
        pg_user = os.getenv("POSTGRES_USER", "trading_agent")
        pg_pass = os.getenv("POSTGRES_PASSWORD", "TradingAgent2025Secure")
        pg_db = os.getenv("POSTGRES_DB", "trading_agent")
        pg_url = f"postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"

    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")

    db = PostgresDatabase(pg_url, redis_url=redis_url)
    await db.initialize()
    logger.info("PostgreSQL connected")

    # Token tracker
    budget_cfg = settings.get("token_budget", {})
    tracker = TokenTracker(
        daily_budget_cny=budget_cfg.get("daily_budget_cny", 200.0),
        rate_limit_tpm=budget_cfg.get("rate_limit_tpm", 1_000_000),
    )
    await tracker.load_today_from_db(db)

    # LLM client
    llm = LLMClient(settings, tracker=tracker)

    # Feishu alerter (used for cross-system dedup checking)
    feishu_cfg = settings.get("feishu", {})
    alerter = FeishuAlerter(
        webhook_url=feishu_cfg["webhook_url"],
        alert_levels=feishu_cfg.get("alert_levels", ["critical", "high", "medium"]),
    )

    # Content fetcher
    proxy_url = os.getenv("HTTP_PROXY") or os.getenv("http_proxy") or "http://127.0.0.1:7890"
    browser_manager = BrowserManager(proxy_url=proxy_url)

    session = aiohttp.ClientSession()
    fetch_cfg = settings.get("content_fetch", {})
    content_fetcher = ContentFetcher(
        session=session,
        max_content_chars=fetch_cfg.get("max_content_chars", 10000),
        timeout_seconds=fetch_cfg.get("timeout_seconds", 15),
        rate_limit_rps=fetch_cfg.get("rate_limit_rps", 1.0),
        browser_manager=browser_manager,
        proxy_url=proxy_url,
    ) if fetch_cfg.get("enabled", True) else None

    uqer_token = settings.get("uqer", {}).get("token", "")

    # ClickHouse (optional — for breaking news persistence)
    ch_store = None
    ch_cfg = settings.get("clickhouse", {})
    if ch_cfg.get("enabled", False):
        try:
            from engine.clickhouse_store import ClickHouseStore
            ch_store = ClickHouseStore(ch_cfg)
            await ch_store.initialize()
            logger.info("ClickHouse connected for breaking news storage")
        except Exception as e:
            logger.warning("ClickHouse init failed (breaking news won't be persisted): %s", e)
            ch_store = None

    # Create scanner
    scanner = ProactiveScanner(
        llm=llm,
        db=db,
        tracker=tracker,
        alerter=alerter,
        settings=settings,
        content_fetcher=content_fetcher,
        uqer_token=uqer_token,
        ch_store=ch_store,
    )
    await scanner.initialize()

    # Send startup notification
    webhook_url = settings.get("proactive_monitoring", {}).get("webhook_url") or feishu_cfg["webhook_url"]
    await _send_startup_notification(webhook_url, scanner)

    if test_ticker:
        # Test single stock
        logger.info("Testing single stock: %s", test_ticker)
        holding = next((h for h in scanner.holdings if h.ticker == test_ticker), None)
        if not holding:
            logger.error("Ticker %s not found in portfolio", test_ticker)
            # List available tickers
            logger.info("Available: %s", ", ".join(h.ticker for h in scanner.holdings))
        else:
            await scanner._scan_stock(holding)
            logger.info("Single stock test complete for %s", test_ticker)

    elif test_all:
        # Test all stocks once (no loop)
        logger.info("Running single scan cycle for all %d stocks...", len(scanner.holdings))
        await scanner._run_scan_cycle()
        logger.info("Single cycle complete")

    else:
        # Full scanner loop
        loop = asyncio.get_event_loop()

        def _signal_handler():
            logger.info("Shutdown signal received")
            asyncio.ensure_future(scanner.shutdown())

        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, _signal_handler)

        await scanner.run()

    # Cleanup
    if ch_store:
        await ch_store.close()
    await alerter.close()
    await browser_manager.close()
    await session.close()
    await db.close()

    # Persist token usage
    if tracker:
        saved = await tracker.persist_to_db(db)
        if saved:
            logger.info("Token usage persisted (%d records)", saved)

    logger.info("Proactive scanner stopped")


async def _send_startup_notification(webhook_url: str, scanner) -> None:
    """Send startup message to Feishu."""
    import httpx, os

    holdings = scanner.holdings
    by_market: dict[str, list] = {}
    for h in holdings:
        by_market.setdefault(h.market_label, []).append(h.ticker)

    market_str = "\n".join(
        f"  {m}: {', '.join(tickers)}"
        for m, tickers in by_market.items()
    )

    text = (
        f"🔍 持仓主动监控系统启动\n\n"
        f"持仓数量: {len(holdings)} 只\n"
        f"批次: {len(scanner._batches)} 组\n"
        f"扫描频率: 交易时段每30分钟\n\n"
        f"持仓列表:\n{market_str}\n\n"
        f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )

    payload = {"msg_type": "text", "content": {"text": text}}

    saved_proxy = {}
    for key in ("HTTP_PROXY", "http_proxy", "HTTPS_PROXY", "https_proxy", "ALL_PROXY", "all_proxy"):
        if key in os.environ:
            saved_proxy[key] = os.environ.pop(key)
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                webhook_url, json=payload,
                headers={"Content-Type": "application/json"},
            )
            if resp.status_code == 200:
                logger.info("Startup notification sent to Feishu")
            else:
                logger.warning("Startup notification failed: HTTP %d", resp.status_code)
    except Exception as e:
        logger.warning("Startup notification failed: %s", e)
    finally:
        os.environ.update(saved_proxy)


if __name__ == "__main__":
    args = sys.argv[1:]

    test_ticker = None
    test_all = False

    if "--test" in args:
        idx = args.index("--test")
        if idx + 1 < len(args):
            test_ticker = args[idx + 1]
        else:
            print("Usage: python run_proactive.py --test TICKER")
            sys.exit(1)
    elif "--test-all" in args:
        test_all = True

    asyncio.run(run_scanner(test_ticker=test_ticker, test_all=test_all))
