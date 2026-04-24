"""Scraper → backend realtime push bridge.

When a scraper inserts a new crawled item (研报 / 纪要 / 观点 / 资讯), it can
call ``get_realtime_pusher().publish_new_item(...)`` to notify the backend via
Redis pub/sub on a platform-agnostic channel. The backend's ``ws/feed`` (and
anyone else that subscribes) then forwards the event to connected frontend
clients for "new item popped up" toasts / auto-refresh of lists.

Design
------

- **Single channel, single schema**: ``crawl:new-item``. Each message is a JSON
  blob with ``{platform, category, collection, doc_id, title, release_time,
  release_time_ms, organization, industry, has_pdf, at}``. Subscribers filter
  by platform/category.
- **Best-effort, never blocking**: if Redis is down or the URL is misconfigured,
  ``publish_new_item`` silently no-ops. Scrapers must not fail because the
  backend is unreachable.
- **Reconnects lazily**: first call connects; subsequent calls reuse. On a
  Redis error the cached client is dropped and the next call reconnects.
- **Process-local singleton**: returned by ``get_realtime_pusher()``. Thread-
  safe via the synchronous ``redis`` client (every scraper is single-thread).

Environment
-----------

``CRAWLER_PUSH_REDIS_URL``  – e.g. ``redis://localhost:6379/0``. Defaults to
that. If you set it to the empty string ``""``, pushing is disabled entirely
(useful for CI / test runs with no Redis).

Channel convention
------------------

Matches the backend ``CHANNEL_CRAWL_NEW`` constant in
``backend/app/core/events.py`` (both point to ``crawl:new-item``).
"""
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Optional

CHANNEL_CRAWL_NEW = "crawl:new-item"

# Module-level cache so multiple scrapers share one connection.
_pusher: Optional["RealtimePusher"] = None
_pusher_init_failed: bool = False


class RealtimePusher:
    """Fire-and-forget Redis publisher for "new crawl item" events.

    Every scraper platform has its own identifier (e.g. ``alphaengine``,
    ``gangtise``) that we attach to each event so the subscriber can route
    or filter. ``publish_new_item`` is safe to call on every insert.
    """

    def __init__(self, url: str, platform: str):
        self.url = url
        self.platform = platform
        self._client = None
        self._failed_at: float = 0.0      # last failure timestamp; use to debounce retries
        self._failed_warned: bool = False

    def _connect(self):
        if self._client is not None:
            return self._client
        # Rate-limit reconnect attempts — if Redis was down 5s ago, don't
        # retry until 30s later to avoid TCP thrash on every scrape.
        if self._failed_at and (time.time() - self._failed_at) < 30:
            return None
        try:
            import redis  # sync client; crawl scripts aren't async
            self._client = redis.Redis.from_url(self.url, decode_responses=True,
                                                socket_timeout=2.0,
                                                socket_connect_timeout=2.0)
            # No ping — let publish be the first command; any error will trip
            # the except below and we'll mark as failed.
            return self._client
        except Exception as exc:
            self._failed_at = time.time()
            if not self._failed_warned:
                print(f"[push] Redis connect failed ({self.url}): {exc}. "
                      f"Push disabled; scraping continues.")
                self._failed_warned = True
            return None

    def publish_new_item(self, *,
                         category: str,
                         collection: str,
                         doc_id: str,
                         title: str = "",
                         release_time: Optional[str] = None,
                         release_time_ms: Optional[int] = None,
                         organization: Optional[str] = None,
                         industry: Optional[str] = None,
                         has_pdf: bool = False,
                         extra: Optional[dict] = None) -> bool:
        """Publish a new-item event. Returns True if published, False on any failure."""
        client = self._connect()
        if client is None:
            return False
        event = {
            "platform": self.platform,
            "category": category,
            "collection": collection,
            "doc_id": str(doc_id),
            "title": title[:300] if title else "",
            "release_time": release_time,
            "release_time_ms": release_time_ms,
            "organization": organization,
            "industry": industry,
            "has_pdf": bool(has_pdf),
            "at": datetime.now(timezone.utc).isoformat(),
        }
        if extra:
            event["extra"] = extra
        try:
            client.publish(CHANNEL_CRAWL_NEW, json.dumps(event, ensure_ascii=False))
            return True
        except Exception as exc:
            self._failed_at = time.time()
            self._client = None  # force reconnect next time
            if not self._failed_warned:
                print(f"[push] Redis publish failed: {exc}. "
                      f"Subsequent pushes silently retry on backoff.")
                self._failed_warned = True
            return False


def get_realtime_pusher(platform: Optional[str] = None) -> Optional[RealtimePusher]:
    """Return a shared pusher instance, or None if pushing is disabled.

    If ``platform`` is not given, infer from the cwd basename (``alphaengine``,
    ``gangtise``, etc.). Users of this module never construct the pusher
    directly — always go through this helper so the singleton is shared.
    """
    global _pusher, _pusher_init_failed

    url = os.environ.get("CRAWLER_PUSH_REDIS_URL", "redis://localhost:6379/0")
    if not url:
        return None
    if _pusher_init_failed:
        return None

    if _pusher is not None:
        return _pusher

    if platform is None:
        # Infer from cwd — every scraper's cwd is its own directory
        # (e.g. crawl/alphaengine, crawl/gangtise).
        platform = os.path.basename(os.getcwd())
        # Map directory names that differ from canonical platform keys.
        _ALIAS = {
            "alphapai_crawl": "alphapai",
            "meritco_crawl":  "meritco",
            "third_bridge":   "thirdbridge",
            "AceCamp":        "acecamp",
        }
        platform = _ALIAS.get(platform, platform)

    try:
        _pusher = RealtimePusher(url=url, platform=platform)
    except Exception:
        _pusher_init_failed = True
        return None
    return _pusher


__all__ = ["RealtimePusher", "get_realtime_pusher", "CHANNEL_CRAWL_NEW"]
