"""Engine lifecycle manager — runs the trading engine as a managed subprocess.

The engine process shares PostgreSQL + Redis with the web backend so all
analysis results appear instantly on the frontend via WebSocket.

Usage (integrated into FastAPI lifespan in main.py):
    engine_mgr = EngineManager(settings)
    await engine_mgr.start()
    ...
    await engine_mgr.stop()
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import sys
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

from backend.app.config import Settings

logger = logging.getLogger(__name__)

# Maximum lines of recent logs to keep in memory
MAX_LOG_LINES = 500


class EngineManager:
    """Manages the trading engine as a subprocess with health monitoring."""

    def __init__(self, settings: Settings):
        self._settings = settings
        self._process: asyncio.subprocess.Process | None = None
        self._log_reader_task: asyncio.Task | None = None
        self._recent_logs: deque[str] = deque(maxlen=MAX_LOG_LINES)
        self._start_time: datetime | None = None
        self._restart_count: int = 0
        self._auto_restart: bool = True
        self._stopping: bool = False
        self._health_file = Path("data/engine_health.json")

        # Engine script location
        self._engine_dir = Path(__file__).resolve().parent.parent.parent.parent
        self._run_py = self._engine_dir / "run.py"

    @property
    def is_running(self) -> bool:
        return self._process is not None and self._process.returncode is None

    def _build_env(self) -> dict[str, str]:
        """Build environment variables for the engine subprocess."""
        env = os.environ.copy()
        # Pass database URL so engine uses PostgreSQL
        env["DATABASE_URL"] = self._settings.database_url
        env["REDIS_URL"] = self._settings.redis_url
        env["ENGINE_HEALTH_FILE"] = str(self._health_file)
        # Ensure engine can find its own packages
        env["PYTHONPATH"] = str(self._engine_dir)
        return env

    async def start(self) -> bool:
        """Start the engine subprocess."""
        if self.is_running:
            logger.warning("Engine already running (pid=%d)", self._process.pid)
            return False

        self._stopping = False
        logger.info("Starting trading engine subprocess...")

        try:
            python = sys.executable
            self._process = await asyncio.create_subprocess_exec(
                python, str(self._run_py),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                cwd=str(self._engine_dir),
                env=self._build_env(),
            )

            self._start_time = datetime.now(timezone.utc)
            self._recent_logs.clear()

            # Start log reader
            self._log_reader_task = asyncio.create_task(
                self._read_logs(), name="engine_log_reader",
            )

            logger.info("Engine started (pid=%d)", self._process.pid)
            return True

        except Exception as e:
            logger.error("Failed to start engine: %s", e)
            return False

    async def stop(self, timeout: float = 15.0) -> bool:
        """Stop the engine subprocess gracefully."""
        self._stopping = True
        self._auto_restart = False

        if not self.is_running:
            return True

        pid = self._process.pid
        logger.info("Stopping engine (pid=%d)...", pid)

        try:
            self._process.send_signal(signal.SIGTERM)
        except ProcessLookupError:
            return True

        try:
            await asyncio.wait_for(self._process.wait(), timeout=timeout)
            logger.info("Engine stopped gracefully (pid=%d)", pid)
        except asyncio.TimeoutError:
            logger.warning("Engine did not stop in %ds, force-killing (pid=%d)", timeout, pid)
            try:
                self._process.kill()
                await self._process.wait()
            except ProcessLookupError:
                pass

        if self._log_reader_task and not self._log_reader_task.done():
            self._log_reader_task.cancel()
            try:
                await self._log_reader_task
            except asyncio.CancelledError:
                pass

        self._process = None
        return True

    async def restart(self) -> bool:
        """Restart the engine (stop then start)."""
        self._restart_count += 1
        await self.stop()
        self._auto_restart = True
        return await self.start()

    async def _read_logs(self) -> None:
        """Read stdout from the engine process and store recent lines."""
        try:
            while self.is_running:
                line = await self._process.stdout.readline()
                if not line:
                    break
                text = line.decode("utf-8", errors="replace").rstrip()
                if text:
                    self._recent_logs.append(
                        f"[{datetime.now().strftime('%H:%M:%S')}] {text}"
                    )
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.debug("Log reader error: %s", e)

        # Process ended — auto-restart if configured
        if self._process and self._process.returncode is not None:
            code = self._process.returncode
            self._recent_logs.append(
                f"[{datetime.now().strftime('%H:%M:%S')}] Engine exited with code {code}"
            )
            logger.warning("Engine process exited (code=%d)", code)

            if self._auto_restart and not self._stopping and code != 0:
                logger.info("Auto-restarting engine in 10 seconds...")
                self._recent_logs.append(
                    f"[{datetime.now().strftime('%H:%M:%S')}] Auto-restarting in 10s..."
                )
                await asyncio.sleep(10)
                if not self._stopping:
                    self._restart_count += 1
                    self._process = None
                    await self.start()

    def get_health(self) -> dict:
        """Read engine health from the health file + process state."""
        health = {
            "is_running": self.is_running,
            "pid": self._process.pid if self.is_running else None,
            "start_time": self._start_time.isoformat() if self._start_time else None,
            "restart_count": self._restart_count,
            "auto_restart": self._auto_restart,
            "uptime_seconds": None,
        }

        if self._start_time and self.is_running:
            health["uptime_seconds"] = int(
                (datetime.now(timezone.utc) - self._start_time).total_seconds()
            )

        # Read detailed health from engine's health file
        try:
            if self._health_file.exists():
                data = json.loads(self._health_file.read_text(encoding="utf-8"))
                health["engine_status"] = data.get("status", "unknown")
                health["engine_message"] = data.get("message", "")
                health["monitors"] = data.get("monitors", 0)
                health["queue_size"] = data.get("queue_size", 0)
                health["stats"] = data.get("stats")
                health["last_heartbeat"] = data.get("timestamp")
        except Exception:
            health["engine_status"] = "unknown"

        return health

    def get_recent_logs(self, lines: int = 100) -> list[str]:
        """Return the most recent log lines."""
        return list(self._recent_logs)[-lines:]

    async def enable_auto_restart(self) -> None:
        self._auto_restart = True

    async def disable_auto_restart(self) -> None:
        self._auto_restart = False
