#!/usr/bin/env python3
"""
One-time script: backfill title_zh into news_items.metadata JSONB.

Finds all rows where language != 'zh' and metadata->>'title_zh' is missing,
then batch-translates the English titles via MiniMax API and writes back.

Usage:
    python scripts/backfill_title_zh.py              # real run
    python scripts/backfill_title_zh.py --dry-run    # preview only
"""

import argparse
import asyncio
import json
import logging
import os
import re
import sys
from pathlib import Path

import asyncpg
import httpx
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

ENV_PATH = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(ENV_PATH)

DB_DSN = (
    f"postgresql://{os.getenv('POSTGRES_USER', 'trading_agent')}"
    f":{os.getenv('POSTGRES_PASSWORD', 'TradingAgent2025Secure')}"
    f"@{os.getenv('POSTGRES_HOST', 'localhost')}"
    f":{os.getenv('POSTGRES_PORT', '5432')}"
    f"/{os.getenv('POSTGRES_DB', 'trading_agent')}"
)

MINIMAX_API_KEY = os.getenv("MINIMAX_API_KEY", "")
MINIMAX_BASE_URL = os.getenv("MINIMAX_BASE_URL", "https://api.minimaxi.com/v1")
MINIMAX_MODEL = os.getenv("MINIMAX_MODEL", "MiniMax-M2")

# Fallback: OpenRouter (used when MiniMax is unavailable)
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "")
OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"
OPENROUTER_MODEL = "google/gemini-2.0-flash-001"

BATCH_SIZE = 20

SYSTEM_PROMPT = (
    "你是一个翻译助手。将以下英文新闻标题翻译为简洁的中文。"
    "每行一个标题，保持对应顺序，只返回翻译结果，每行一个。"
)

FETCH_SQL = """
    SELECT n.id, n.title
    FROM news_items n
    LEFT JOIN filter_results f ON n.id = f.news_item_id
    LEFT JOIN analysis_results a ON n.id = a.news_item_id
    WHERE n.language != 'zh'
      AND (n.metadata IS NULL OR n.metadata->>'title_zh' IS NULL)
    ORDER BY
      CASE WHEN f.is_relevant = true
                AND a.sentiment IS NOT NULL
                AND a.sentiment != 'neutral'
           THEN 0 ELSE 1 END,
      n.fetched_at DESC
"""

UPDATE_SQL = """
    UPDATE news_items
    SET metadata = jsonb_set(COALESCE(metadata, '{}'), '{title_zh}', to_jsonb($2::text))
    WHERE id = $1
"""

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("backfill_title_zh")


# ---------------------------------------------------------------------------
# Translation helper
# ---------------------------------------------------------------------------

async def _call_llm(
    client: httpx.AsyncClient,
    base_url: str,
    api_key: str,
    model: str,
    payload: dict,
    max_retries: int = 3,
) -> httpx.Response:
    """Call an OpenAI-compatible chat endpoint with retries."""
    for attempt in range(max_retries):
        resp = await client.post(
            f"{base_url}/chat/completions",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=60.0,
        )
        if resp.status_code in (429, 529, 500, 502, 503):
            wait = 2 ** (attempt + 1)
            log.warning("API returned %d, retrying in %ds (attempt %d/%d)", resp.status_code, wait, attempt + 1, max_retries)
            await asyncio.sleep(wait)
            continue
        resp.raise_for_status()
        return resp
    # Final attempt failed — raise
    resp.raise_for_status()
    return resp  # unreachable, for type-checker


async def translate_batch(
    client: httpx.AsyncClient,
    titles: list[str],
) -> list[str]:
    """Translate a batch of titles. Tries MiniMax first, falls back to OpenRouter."""
    user_content = "\n".join(titles)

    payload = {
        "model": MINIMAX_MODEL,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_content},
        ],
        "temperature": 0.1,
    }

    resp = None
    # Prefer OpenRouter (more stable); fall back to MiniMax
    if OPENROUTER_API_KEY:
        try:
            payload["model"] = OPENROUTER_MODEL
            resp = await _call_llm(client, OPENROUTER_BASE_URL, OPENROUTER_API_KEY, OPENROUTER_MODEL, payload)
        except Exception as e:
            log.warning("OpenRouter failed (%s), falling back to MiniMax", e)
            payload["model"] = MINIMAX_MODEL
            resp = await _call_llm(client, MINIMAX_BASE_URL, MINIMAX_API_KEY, MINIMAX_MODEL, payload)
    else:
        resp = await _call_llm(client, MINIMAX_BASE_URL, MINIMAX_API_KEY, MINIMAX_MODEL, payload)

    data = resp.json()
    reply = data["choices"][0]["message"]["content"].strip()
    # Strip <think>...</think> blocks from reasoning models
    reply = re.sub(r"<think>.*?</think>", "", reply, flags=re.DOTALL).strip()
    lines = [line.strip() for line in reply.splitlines() if line.strip()]

    # If the API returned fewer/more lines than expected, pad or truncate
    if len(lines) < len(titles):
        log.warning(
            "API returned %d lines but expected %d — padding with originals",
            len(lines), len(titles),
        )
        lines.extend(titles[len(lines):])
    elif len(lines) > len(titles):
        log.warning(
            "API returned %d lines but expected %d — truncating",
            len(lines), len(titles),
        )
        lines = lines[: len(titles)]

    return lines


# ---------------------------------------------------------------------------
# Main logic
# ---------------------------------------------------------------------------

async def main(dry_run: bool = False) -> None:
    if not MINIMAX_API_KEY:
        log.error("MINIMAX_API_KEY is not set. Aborting.")
        sys.exit(1)

    log.info("Connecting to database …")
    pool = await asyncpg.create_pool(DB_DSN, min_size=1, max_size=5)

    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(FETCH_SQL)

        total = len(rows)
        log.info("Found %d news items needing title_zh translation.", total)

        if total == 0:
            log.info("Nothing to do.")
            return

        # Split into batches of BATCH_SIZE
        batches = [rows[i : i + BATCH_SIZE] for i in range(0, total, BATCH_SIZE)]
        log.info("Will process %d batches of up to %d titles each.", len(batches), BATCH_SIZE)

        translated_count = 0
        failed_count = 0

        async with httpx.AsyncClient() as client:
            for batch_idx, batch in enumerate(batches, start=1):
                ids = [r["id"] for r in batch]
                titles = [r["title"] for r in batch]

                log.info(
                    "Batch %d/%d  (%d titles)  [progress: %d/%d]",
                    batch_idx, len(batches), len(titles),
                    translated_count, total,
                )

                if dry_run:
                    for title in titles:
                        log.info("  [dry-run] would translate: %s", title[:100])
                    translated_count += len(titles)
                    continue

                try:
                    zh_titles = await translate_batch(client, titles)
                except Exception:
                    log.exception("Batch %d failed — skipping %d titles", batch_idx, len(titles))
                    failed_count += len(titles)
                    continue

                # Write translations back to DB
                async with pool.acquire() as conn:
                    async with conn.transaction():
                        for row_id, zh_title in zip(ids, zh_titles):
                            await conn.execute(UPDATE_SQL, row_id, zh_title)

                translated_count += len(titles)
                log.info(
                    "Batch %d done.  Sample: \"%s\" → \"%s\"",
                    batch_idx,
                    titles[0][:60],
                    zh_titles[0][:60],
                )

                # Pace requests to avoid rate limits
                await asyncio.sleep(1)

        log.info("=== Finished ===")
        log.info("  Translated: %d", translated_count)
        log.info("  Failed:     %d", failed_count)
        log.info("  Total:      %d", total)

    finally:
        await pool.close()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill title_zh for news_items")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview items to translate without updating the database",
    )
    args = parser.parse_args()

    asyncio.run(main(dry_run=args.dry_run))
