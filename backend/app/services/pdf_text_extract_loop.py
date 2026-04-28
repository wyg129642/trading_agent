"""Lifespan loop that runs scripts/extract_pdf_texts.py incrementally.

Replaces the */30 min cron with a tighter cycle so freshly crawled PDFs reach
``pdf_text_md`` within minutes of ingestion. The script is spawned as a
subprocess under flock; if the cron (or another lifespan instance in the prod
worktree) already holds the lock, this cycle is a no-op rather than a JVM
double-spend.

Per-cycle ``--limit`` bounds wall time. Set ``pdf_text_extract_enabled=false``
to disable.
"""
from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path

from backend.app.config import Settings

log = logging.getLogger("pdf_text_extract_loop")

REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPT = REPO_ROOT / "scripts" / "extract_pdf_texts.py"
LOCK_FILE = f"/tmp/pdf_parse.{os.environ.get('USER','user')}.lock"


async def pdf_text_extract_loop(settings: Settings) -> None:
    if not getattr(settings, "pdf_text_extract_enabled", True):
        log.info("pdf_text_extract_loop: disabled via config")
        return

    interval = max(60, int(getattr(settings, "pdf_text_extract_interval_sec", 300)))
    limit = max(1, int(getattr(settings, "pdf_text_extract_limit_per_cycle", 100)))
    workers = max(1, int(getattr(settings, "pdf_text_extract_workers", 2)))
    batch_size = max(1, int(getattr(settings, "pdf_text_extract_batch_size", 8)))

    py = os.environ.get(
        "CRAWLER_PYTHON", "/home/ygwang/miniconda3/envs/agent/bin/python",
    )
    java_path = os.environ.get("PDF_PARSE_JAVA_PATH", "/home/ygwang/jdk17/bin")

    log.info(
        "pdf_text_extract_loop started "
        "(interval=%ds, limit=%d/cycle, workers=%d, batch=%d)",
        interval, limit, workers, batch_size,
    )

    # Stagger the first run a bit so we don't pile on with the heavier startup
    # tasks (Milvus client init, Futu connect, kb_vector_sync first poll).
    try:
        await asyncio.sleep(45)
    except asyncio.CancelledError:
        return

    while True:
        try:
            cmd = [
                "flock", "-n", LOCK_FILE,
                py, "-u", str(SCRIPT),
                "--workers", str(workers),
                "--batch-size", str(batch_size),
                "--limit", str(limit),
            ]
            env = os.environ.copy()
            env["PYTHONPATH"] = (
                str(REPO_ROOT) + os.pathsep + env.get("PYTHONPATH", "")
            ).rstrip(os.pathsep)
            env["PATH"] = java_path + os.pathsep + env.get("PATH", "")

            proc = await asyncio.create_subprocess_exec(
                *cmd,
                cwd=str(REPO_ROOT),
                env=env,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
            try:
                # Cap a single cycle to interval - 30s so a hung JVM doesn't
                # back-pressure the whole loop forever.
                await asyncio.wait_for(proc.wait(), timeout=max(interval - 30, 60))
            except asyncio.TimeoutError:
                log.warning("pdf_text_extract_loop: cycle timeout, killing pid=%s", proc.pid)
                try:
                    proc.kill()
                except ProcessLookupError:
                    pass
                await proc.wait()
            else:
                rc = proc.returncode
                # exit 1 from `flock -n` means lock was held by another runner —
                # expected when cron + lifespan overlap. Anything else logs.
                if rc and rc != 1:
                    log.warning(
                        "pdf_text_extract_loop: subprocess exited with rc=%d", rc,
                    )
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("pdf_text_extract_loop: cycle error (continuing)")
        try:
            await asyncio.sleep(interval)
        except asyncio.CancelledError:
            return
