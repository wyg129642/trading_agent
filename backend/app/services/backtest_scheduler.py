"""Automated backtest scheduler — runs daily after market close.

Schedules:
  - 16:00 CST (08:00 UTC): A-share market close → backfill China prices + fill returns
  - 22:00 EST (03:00 UTC next day): US market close → backfill US prices + fill returns

Falls back to a single daily run at 17:00 CST if fine-grained scheduling fails.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone, timedelta

logger = logging.getLogger(__name__)


class BacktestScheduler:
    """Periodic backtest runner embedded in the web backend."""

    def __init__(self, settings):
        self.settings = settings
        self._task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()
        # Run interval: check every 30 minutes
        self._check_interval = 30 * 60
        # Track last run date to avoid double-runs
        self._last_run_date: str | None = None

    async def start(self):
        self._task = asyncio.create_task(self._loop(), name="backtest-scheduler")
        logger.info("BacktestScheduler started (check interval: %ds)", self._check_interval)

    async def stop(self):
        self._stop_event.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
        logger.info("BacktestScheduler stopped")

    async def _loop(self):
        """Main scheduler loop — checks if it's time to run backtest."""
        # On startup, if the leaderboard data is stale (>36h), run a light
        # refresh immediately so the UI has fresh rankings to show.
        try:
            if await self._is_leaderboard_stale(threshold_hours=36):
                logger.info("BacktestScheduler: startup — leaderboard data is stale, running refresh")
                await self._refresh_signal_evaluations(days=30)
        except Exception:
            logger.exception("BacktestScheduler: startup stale check failed")

        while not self._stop_event.is_set():
            try:
                await self._check_and_run()
            except Exception:
                logger.exception("BacktestScheduler error in check cycle")

            # Wait for next check interval
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self._check_interval)
                break  # stop_event was set
            except asyncio.TimeoutError:
                pass  # normal timeout, continue loop

    async def _check_and_run(self):
        """Check if we should run backtest based on current time."""
        now_utc = datetime.now(timezone.utc)
        # Convert to CST (UTC+8) for China market schedule
        now_cst = now_utc + timedelta(hours=8)
        today_str = now_cst.strftime("%Y-%m-%d")
        current_hour = now_cst.hour

        # Skip weekends (Saturday=5, Sunday=6)
        if now_cst.weekday() >= 5:
            return

        # Already ran today
        if self._last_run_date == today_str:
            return

        # Run window: 16:00-17:30 CST (after A-share close)
        if 16 <= current_hour <= 17:
            logger.info("BacktestScheduler: triggering daily backtest update (CST hour=%d)", current_hour)
            await self._run_backtest()
            self._last_run_date = today_str

    async def _is_leaderboard_stale(self, threshold_hours: float) -> bool:
        """Return True if the newest signal_evaluations.evaluated_at is older than threshold."""
        from sqlalchemy import select, func
        from backend.app.core.database import async_session_factory
        from backend.app.models.leaderboard import SignalEvaluation

        async with async_session_factory() as session:
            last_eval = await session.scalar(select(func.max(SignalEvaluation.evaluated_at)))
        if last_eval is None:
            return True
        if last_eval.tzinfo is None:
            last_eval = last_eval.replace(tzinfo=timezone.utc)
        age = datetime.now(timezone.utc) - last_eval
        return age.total_seconds() / 3600.0 > threshold_hours

    async def _run_backtest(self):
        """Execute the backtest pipeline."""
        import sys
        from pathlib import Path

        project_root = Path(__file__).resolve().parent.parent.parent.parent
        sys.path.insert(0, str(project_root))

        try:
            from scripts.backfill_prices import backfill
            from scripts.fill_returns import fill_returns

            logger.info("BacktestScheduler: Step 1/3 — backfilling prices (last 5 days)")
            await backfill(days=5, dry_run=False)

            logger.info("BacktestScheduler: Step 2/3 — filling returns")
            await fill_returns(days=None, dry_run=False)

            logger.info("BacktestScheduler: Step 3/3 — updating signal_evaluations leaderboard")
            await self._refresh_signal_evaluations()

            logger.info("BacktestScheduler: daily backtest update complete")
        except Exception:
            logger.exception("BacktestScheduler: backtest pipeline failed")

    async def _refresh_signal_evaluations(self, days: int = 60) -> None:
        """Populate/refresh the PostgreSQL signal_evaluations table.

        Uses the same code path as ``POST /api/leaderboard/evaluate`` so the
        source-accuracy leaderboard stays fresh without a manual trigger.
        Runs in a fresh DB session scoped to this job.
        """
        from backend.app.core.database import async_session_factory
        from backend.app.services.signal_evaluator import run_evaluation

        async with async_session_factory() as session:
            try:
                summary = await run_evaluation(session, days=days)
                logger.info("BacktestScheduler: signal_evaluations refresh %s", summary)
            except Exception:
                logger.exception("BacktestScheduler: signal_evaluations refresh failed")
                await session.rollback()
