"""Async HTTP client for all AlphaPai data APIs."""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime
from typing import Any

import aiohttp

logger = logging.getLogger(__name__)

# Time field used for sliding-window pagination per API
_TIME_FIELD: dict[str, str] = {
    "get_wechat_articles_yjh": "spider_time",
    "get_summary_roadshow_info_yjh": "stime",
    "get_summary_roadshow_info_us_yjh": "stime",
    "get_comment_info_yjh": "cmnt_date",
}

# Primary-key field per API (for dedup)
_PK_FIELD: dict[str, str] = {
    "get_wechat_articles_yjh": "arc_code",
    "get_summary_roadshow_info_yjh": "trans_id",
    "get_summary_roadshow_info_us_yjh": "trans_id",
    "get_comment_info_yjh": "cmnt_hcode",
}


class AlphaPaiClient:
    """Async client for all four AlphaPai data query APIs + file download."""

    QUERY_PATH = "/alpha/open-api/v1/data-manager/query"
    DOWNLOAD_PATH = "/alpha/open-api/v1/file/download"

    def __init__(self, base_url: str, app_agent: str):
        self.base_url = base_url.rstrip("/")
        self.app_agent = app_agent
        self._session: aiohttp.ClientSession | None = None

    # ------------------------------------------------------------------ #
    # Session management
    # ------------------------------------------------------------------ #
    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                headers={
                    "app-agent": self.app_agent,
                    "Content-Type": "application/json",
                },
                timeout=aiohttp.ClientTimeout(total=60),
                trust_env=False,
            )
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    # ------------------------------------------------------------------ #
    # Low-level query
    # ------------------------------------------------------------------ #
    async def _query_raw(
        self,
        api_name: str,
        start_time: str,
        end_time: str = "",
        size: int = 500,
    ) -> dict[str, Any]:
        """Single POST to the data-manager/query endpoint.

        Returns dict with keys: data (list), count (int), hasMore (bool).
        Raises on HTTP or API error after retries.
        """
        session = await self._ensure_session()
        payload = json.dumps({
            "apiName": api_name,
            "params": {
                "start_time": start_time,
                "end_time": end_time,
                "size": size,
            },
            "fields": [],
        })

        last_exc: Exception | None = None
        for attempt in range(3):
            try:
                async with session.post(
                    f"{self.base_url}{self.QUERY_PATH}",
                    data=payload,
                ) as resp:
                    body = await resp.json()
                    if body.get("code") != 200000:
                        msg = body.get("message", str(body))
                        raise RuntimeError(f"AlphaPai API error ({api_name}): {msg}")
                    return body["data"]  # {data: [...], count: N, hasMore: bool}
            except (aiohttp.ClientError, asyncio.TimeoutError, RuntimeError) as exc:
                last_exc = exc
                if attempt < 2:
                    await asyncio.sleep(2 ** attempt)
                    logger.warning(
                        "[AlphaPai] %s attempt %d failed: %s", api_name, attempt + 1, exc,
                    )

        raise RuntimeError(f"AlphaPai query failed after 3 retries: {last_exc}")

    # ------------------------------------------------------------------ #
    # Paginated fetch (time-window sliding)
    # ------------------------------------------------------------------ #
    async def fetch_all(
        self,
        api_name: str,
        start_time: str,
        end_time: str = "",
        batch_size: int = 500,
        max_items: int = 10000,
    ) -> list[dict]:
        """Fetch all records in [start_time, end_time] using time-window sliding.

        AlphaPai's offset/page params don't work, so we slide start_time
        forward to the last item's timestamp when hasMore is True.
        Deduplicates by primary key.
        """
        time_field = _TIME_FIELD.get(api_name, "stime")
        pk_field = _PK_FIELD.get(api_name, "id")

        all_items: list[dict] = []
        seen_pks: set[str] = set()
        current_start = start_time

        for _ in range(50):  # safety limit
            result = await self._query_raw(api_name, current_start, end_time, batch_size)
            items = result.get("data", [])

            for item in items:
                pk = item.get(pk_field)
                if pk and pk not in seen_pks:
                    seen_pks.add(pk)
                    all_items.append(item)

            if not result.get("hasMore", False) or not items:
                break

            if len(all_items) >= max_items:
                logger.warning(
                    "[AlphaPai] %s hit max_items=%d, stopping", api_name, max_items,
                )
                break

            # Slide forward: use last item's time
            last_time = items[-1].get(time_field, "")
            if not last_time or last_time <= current_start:
                # No progress — avoid infinite loop
                break
            current_start = last_time

        return all_items

    # ------------------------------------------------------------------ #
    # Convenience wrappers
    # ------------------------------------------------------------------ #
    async def fetch_wechat_articles(
        self, start_time: str, end_time: str = "", batch_size: int = 500,
    ) -> list[dict]:
        return await self.fetch_all(
            "get_wechat_articles_yjh", start_time, end_time, batch_size,
        )

    async def fetch_roadshows_cn(
        self, start_time: str, end_time: str = "", batch_size: int = 500,
    ) -> list[dict]:
        return await self.fetch_all(
            "get_summary_roadshow_info_yjh", start_time, end_time, batch_size,
        )

    async def fetch_roadshows_us(
        self, start_time: str, end_time: str = "", batch_size: int = 500,
    ) -> list[dict]:
        return await self.fetch_all(
            "get_summary_roadshow_info_us_yjh", start_time, end_time, batch_size,
        )

    async def fetch_comments(
        self, start_time: str, end_time: str = "", batch_size: int = 500,
    ) -> list[dict]:
        return await self.fetch_all(
            "get_comment_info_yjh", start_time, end_time, batch_size,
        )

    # ------------------------------------------------------------------ #
    # File download (for roadshow content, article HTML)
    # ------------------------------------------------------------------ #
    async def download_content(self, file_path: str, file_type: str = "2") -> str:
        """Download a file from AlphaPai storage.

        file_type: "2" = general, "3" = recording, "4" = research report
        Returns the file content as string.
        """
        session = await self._ensure_session()
        payload = json.dumps({"type": file_type, "filePath": file_path})

        for attempt in range(3):
            try:
                async with session.post(
                    f"{self.base_url}{self.DOWNLOAD_PATH}",
                    data=payload,
                ) as resp:
                    if resp.status != 200:
                        raise RuntimeError(f"Download failed: HTTP {resp.status}")
                    return await resp.text()
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                if attempt < 2:
                    await asyncio.sleep(2 ** attempt)
                    logger.warning("[AlphaPai] Download attempt %d failed: %s", attempt + 1, exc)

        raise RuntimeError(f"Download failed after 3 retries: {file_path}")
