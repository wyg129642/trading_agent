"""Orchestrates Playwright auto-login scripts as background subprocesses.

Why subprocess instead of an asyncio task in-process?

  1. Playwright spins up a Chromium child process anyway — running its driver
     in-tree would fight uvicorn's event loop and risk blocking (see
     memory: FutuOpenD caused uvicorn to stall every ~120s for this reason).
  2. A crashing Chromium would not take down the API server.
  3. Per-platform scripts stay runnable from the CLI for debugging.

Flow

  POST /api/data-sources/{key}/login    ──┐
                                          ▼
                               launch()  creates session_id, writes
                                         status=STARTING to Redis,
                                         spawns scraper/auto_login.py as a
                                         subprocess with credentials on
                                         the CLI (never in env: env leaks
                                         into child → crawler → logs).

  subprocess → writes status updates to Redis under
      login:{platform}:{session_id}    (hash: status, message, needs_otp …)

  GET /login-status polls that hash.

  If needs_otp=true, UI prompts user, POSTs the code which writes
  `otp:{session_id}` = <code>; the subprocess is blocking on BLPOP.

  Success: subprocess writes credentials.json + status=SUCCESS; API reads
  the updated creds on next status call and redacts.
"""

from __future__ import annotations

import asyncio
import json
import os
import secrets
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import redis.asyncio as aioredis


def _clean_env() -> dict[str, str]:
    """Strip proxy env so CN CDNs (Gangtise/Huawei WAF, AlphaPai, etc.) aren't
    routed through Clash. See the project's infra_proxy memory — local loopback
    and CN endpoints must bypass 127.0.0.1:7890.
    """
    blocked = {
        "http_proxy", "https_proxy", "all_proxy", "no_proxy",
        "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY",
    }
    return {k: v for k, v in os.environ.items() if k not in blocked}


_REPO_ROOT = Path(__file__).resolve().parents[3]
_CRAWL_DIR = _REPO_ROOT / "crawl"

# Map platform key → relative path of the Playwright script. Scripts not yet
# implemented simply aren't listed; the API surfaces that as "unsupported".
AUTO_LOGIN_SCRIPTS: dict[str, str] = {
    "alphapai": "alphapai_crawl/auto_login.py",
    "gangtise": "gangtise/auto_login.py",
    "jinmen": "jinmen/auto_login.py",
    "meritco": "meritco_crawl/auto_login.py",
    "acecamp": "AceCamp/auto_login.py",
    "alphaengine": "alphaengine/auto_login.py",
    "funda": "funda/auto_login.py",
    "thirdbridge": "third_bridge/auto_login.py",
    "wechat_mp": "wechat_mp/auto_login.py",
}

SESSION_TTL_SECONDS = 600  # keep status around for 10 min after completion
OTP_WAIT_SECONDS = 180  # how long the subprocess waits on BLPOP for an OTP


def _status_key(platform: str, session_id: str) -> str:
    return f"login:{platform}:{session_id}"


def _otp_key(session_id: str) -> str:
    return f"login_otp:{session_id}"


async def launch(
    redis: aioredis.Redis,
    platform: str,
    payload: dict[str, Any],
) -> str:
    """Spawn the platform's auto_login script. Returns a session_id.

    `payload` keys are platform-specific but typically {phone, email, password}.
    We pass them on the CLI as a single base64-ish JSON arg to avoid collisions
    with quoted characters in shell escaping.
    """
    if platform not in AUTO_LOGIN_SCRIPTS:
        raise ValueError(f"Auto-login not supported for platform: {platform}")

    script_rel = AUTO_LOGIN_SCRIPTS[platform]
    script_path = _CRAWL_DIR / script_rel
    if not script_path.exists():
        raise FileNotFoundError(f"Auto-login script missing: {script_path}")

    session_id = secrets.token_hex(8)
    status_key = _status_key(platform, session_id)

    await redis.hset(status_key, mapping={
        "platform": platform,
        "session_id": session_id,
        "status": "STARTING",
        "message": "正在启动浏览器…",
        "needs_otp": "0",
        "started_at": datetime.utcnow().isoformat() + "Z",
    })
    await redis.expire(status_key, SESSION_TTL_SECONDS)

    payload_json = json.dumps(payload, ensure_ascii=False)

    # Detached: we never await this coroutine; the subprocess lifecycle is
    # managed through Redis state, not the parent process.
    asyncio.create_task(
        _run_subprocess(redis, platform, session_id, script_path, payload_json)
    )
    return session_id


async def _run_subprocess(
    redis: aioredis.Redis,
    platform: str,
    session_id: str,
    script_path: Path,
    payload_json: str,
) -> None:
    status_key = _status_key(platform, session_id)

    try:
        proc = await asyncio.create_subprocess_exec(
            sys.executable,
            str(script_path),
            "--session-id",
            session_id,
            "--payload",
            payload_json,
            cwd=str(script_path.parent),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=_clean_env(),
        )
    except Exception as exc:
        await _mark_failed(redis, status_key, f"subprocess spawn failed: {exc}")
        return

    try:
        # Keep slack above the inner poll_timeout_s (≤300s) so the
        # subprocess has time to dump debug snapshots on timeout before we
        # SIGKILL it.
        stdout_raw, stderr_raw = await asyncio.wait_for(
            proc.communicate(), timeout=420.0
        )
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()
        await _mark_failed(redis, status_key, "登录超时 (7 分钟)")
        return

    current = await redis.hget(status_key, "status")
    if current in (None, b"STARTING", "STARTING", b"RUNNING", "RUNNING", b"OTP_NEEDED", "OTP_NEEDED"):
        # The child process exited without writing a terminal state — treat as failure.
        stderr_text = (stderr_raw or b"").decode("utf-8", errors="replace")[-800:]
        stdout_text = (stdout_raw or b"").decode("utf-8", errors="replace")[-400:]
        detail = stderr_text.strip() or stdout_text.strip() or f"exit={proc.returncode}"
        await _mark_failed(redis, status_key, detail)
        return

    # On SUCCESS: auto-start the scraper in --watch mode so a fresh token
    # immediately begins producing data. Best-effort — failure to boot the
    # crawler doesn't roll back the login.
    if current in (b"SUCCESS", "SUCCESS"):
        try:
            from backend.app.services import crawler_manager
            info = await crawler_manager.start(redis, platform, force=True)
            await redis.hset(status_key, "crawler_pid", str(info.get("pid") or ""))
            await redis.hset(status_key, "crawler_started", "1")
        except Exception as exc:
            # Surface the error into the session status so the UI can show it.
            await redis.hset(status_key, mapping={
                "crawler_started": "0",
                "crawler_error": str(exc)[:400],
            })


async def _mark_failed(redis: aioredis.Redis, status_key: str, message: str) -> None:
    await redis.hset(status_key, mapping={
        "status": "FAILED",
        "message": message[:400],
        "ended_at": datetime.utcnow().isoformat() + "Z",
    })
    await redis.expire(status_key, SESSION_TTL_SECONDS)


async def get_status(redis: aioredis.Redis, platform: str, session_id: str) -> dict[str, Any]:
    data = await redis.hgetall(_status_key(platform, session_id))
    if not data:
        return {"status": "NOT_FOUND", "message": "session expired or never existed"}
    # decode_responses=True is usually set on the app client, but be defensive.
    return {
        (k.decode() if isinstance(k, bytes) else k):
        (v.decode() if isinstance(v, bytes) else v)
        for k, v in data.items()
    }


async def submit_otp(redis: aioredis.Redis, session_id: str, code: str) -> None:
    """Unblock the subprocess waiting on BLPOP otp:{session_id}."""
    await redis.rpush(_otp_key(session_id), code)
    # Short TTL — the subprocess should pick it up almost immediately.
    await redis.expire(_otp_key(session_id), 60)


async def request_qr_refresh(redis: aioredis.Redis, session_id: str) -> None:
    """Ping the QR subprocess to re-capture the twocode now."""
    key = f"login_refresh:{session_id}"
    await redis.rpush(key, "refresh")
    await redis.expire(key, 60)
