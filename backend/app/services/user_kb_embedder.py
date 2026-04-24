"""OpenAI embedding client for the personal knowledge base.

We use OpenAI's ``text-embedding-3-small`` (1536 dim) as the dense
retrieval backbone. This model:

* handles Chinese, English, and code competitively with domain-specialist
  models 10× its size (top-5 on MTEB multilingual benchmarks);
* is **synonym-aware and cross-lingual by default** — "英伟达" and "NVDA"
  land in vectors that cosine ~0.70, closing the exact gap that broke
  jieba-only BM25 for our investment domain;
* has no local infrastructure burden (the previously configured TEI server
  at jumpbox:8080 is down, and re-provisioning it isn't our hill).

Design
------
* **Fail-open circuit breaker** — if OpenAI is temporarily unreachable,
  the embedder raises ``EmbedderUnavailable`` and the caller's hybrid
  search gracefully degrades to BM25 only. We never block chat latency
  on an embedding outage.
* **Batch API** — used during ingestion, ~100 chunks per HTTP round-trip
  for 50× throughput over one-at-a-time calls.
* **LRU-cached query path** — hot queries (e.g. the same ticker searched
  many times in one chat session) short-circuit to a cached vector.
* **Proxy via ``HTTPS_PROXY``** — OpenAI is blocked without a proxy from
  China; we reuse the same env-var-driven config as ``chat_llm.py``.

The module exposes module-level ``embed_query`` / ``embed_batch`` so
callers don't manage the client lifecycle.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Sequence

import httpx

from backend.app.config import get_settings

logger = logging.getLogger(__name__)


# ── Exceptions ─────────────────────────────────────────────────


class EmbedderError(RuntimeError):
    """Base for all embedder failures."""


class EmbedderUnavailable(EmbedderError):
    """Embedding service is unreachable / circuit open. Caller should
    degrade to lexical search only, not fail the user's request."""


# ── Circuit breaker ────────────────────────────────────────────


@dataclass
class _CircuitBreaker:
    """3-strike breaker with 60 s cooldown.

    States:
      closed    → (N consecutive failures) → open
      open      → (cooldown elapsed)       → half_open
      half_open → (1 success)              → closed
                → (1 failure)              → open
    """
    failure_threshold: int = 3
    cooldown_seconds: float = 60.0
    consecutive_failures: int = 0
    opened_at: float = 0.0
    state: str = "closed"  # closed | open | half_open

    def can_attempt(self) -> bool:
        now = time.monotonic()
        if self.state == "closed":
            return True
        if self.state == "open" and (now - self.opened_at) >= self.cooldown_seconds:
            self.state = "half_open"
            return True
        return self.state == "half_open"

    def record_success(self) -> None:
        self.consecutive_failures = 0
        if self.state != "closed":
            logger.info("user_kb embedder: circuit recovered → closed")
            self.state = "closed"

    def record_failure(self) -> None:
        self.consecutive_failures += 1
        if self.state == "half_open":
            # Single failure in half-open re-opens immediately.
            self.state = "open"
            self.opened_at = time.monotonic()
            logger.warning("user_kb embedder: circuit re-opened on half_open failure")
            return
        if self.consecutive_failures >= self.failure_threshold:
            self.state = "open"
            self.opened_at = time.monotonic()
            logger.warning(
                "user_kb embedder: circuit opened after %d consecutive failures",
                self.consecutive_failures,
            )


_breaker = _CircuitBreaker()


# ── HTTP client singleton ──────────────────────────────────────


_client_lock = asyncio.Lock()
_client: httpx.AsyncClient | None = None
# Per-event-loop client — same rationale as Motor's loop binding; if the loop
# we first touched is gone, the cached client's transport layer is dead.
_clients_by_loop: dict[int, httpx.AsyncClient] = {}


async def _get_client() -> httpx.AsyncClient:
    loop_id = id(asyncio.get_running_loop())
    c = _clients_by_loop.get(loop_id)
    if c is not None:
        return c
    async with _client_lock:
        c = _clients_by_loop.get(loop_id)
        if c is not None:
            return c
        proxy = os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY") or None
        c = httpx.AsyncClient(
            timeout=httpx.Timeout(30.0, connect=10.0),
            proxy=proxy,
            # OpenAI tolerates high concurrency; 20 keepalive conns is plenty.
            limits=httpx.Limits(max_keepalive_connections=20, max_connections=50),
        )
        _clients_by_loop[loop_id] = c
        return c


async def _aclose_all() -> None:
    """Test-teardown helper."""
    for c in list(_clients_by_loop.values()):
        try:
            await c.aclose()
        except Exception:
            pass
    _clients_by_loop.clear()


# ── LRU cache for query embeddings ─────────────────────────────


_CACHE_MAX = 512
_cache: OrderedDict[str, list[float]] = OrderedDict()
_cache_lock = asyncio.Lock()


async def _cache_get(key: str) -> list[float] | None:
    async with _cache_lock:
        v = _cache.get(key)
        if v is not None:
            # Touch-to-move-to-end for LRU.
            _cache.move_to_end(key)
        return v


async def _cache_set(key: str, value: list[float]) -> None:
    async with _cache_lock:
        _cache[key] = value
        _cache.move_to_end(key)
        while len(_cache) > _CACHE_MAX:
            _cache.popitem(last=False)


# ── Public API ─────────────────────────────────────────────────


async def embed_query(text: str) -> list[float]:
    """Embed a single query string. LRU-cached.

    Raises :class:`EmbedderUnavailable` if the service is down.
    """
    text = (text or "").strip()
    if not text:
        raise EmbedderError("empty text")
    cached = await _cache_get(text)
    if cached is not None:
        return cached
    vectors = await embed_batch([text])
    v = vectors[0]
    await _cache_set(text, v)
    return v


async def embed_batch(texts: Sequence[str]) -> list[list[float]]:
    """Embed many strings in one API round-trip.

    Raises :class:`EmbedderUnavailable` if the circuit is open or the call
    fails. Caller should catch and degrade gracefully.
    """
    if not texts:
        return []
    if not _breaker.can_attempt():
        raise EmbedderUnavailable(
            "embedder circuit open — falling back to lexical search"
        )
    settings = get_settings()
    api_key = settings.openai_api_key
    if not api_key:
        raise EmbedderError("OPENAI_API_KEY not configured")

    # OpenAI caps inputs at 8191 tokens per item; 1 token ≈ 3–4 chars for
    # mixed CJK/Latin, so clamp at 24 000 chars to stay safely under.
    clean = [t[:24_000] if t else "" for t in texts]
    # Drop empties — OpenAI rejects the batch otherwise.
    non_empty = [(i, t) for i, t in enumerate(clean) if t.strip()]
    if not non_empty:
        return [[] for _ in texts]

    payload = {
        "model": settings.user_kb_embedding_model,
        "input": [t for _, t in non_empty],
        "encoding_format": "float",
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    client = await _get_client()
    t0 = time.monotonic()
    try:
        resp = await client.post(
            f"{settings.user_kb_embedding_base_url.rstrip('/')}/embeddings",
            headers=headers,
            json=payload,
        )
    except httpx.HTTPError as e:
        _breaker.record_failure()
        raise EmbedderUnavailable(f"OpenAI embedding request failed: {e}") from e

    elapsed = time.monotonic() - t0
    if resp.status_code != 200:
        _breaker.record_failure()
        raise EmbedderUnavailable(
            f"OpenAI /embeddings HTTP {resp.status_code}: {resp.text[:200]}"
        )

    data = resp.json()
    # Sort by index to be defensive against out-of-order responses
    # (OpenAI preserves order but the schema permits reordering).
    items = sorted(data.get("data", []), key=lambda x: x.get("index", 0))
    if len(items) != len(non_empty):
        _breaker.record_failure()
        raise EmbedderError(
            f"embedding count mismatch: asked {len(non_empty)} got {len(items)}"
        )

    _breaker.record_success()
    logger.debug(
        "user_kb embedder: batch=%d elapsed=%.2fs model=%s",
        len(non_empty), elapsed, settings.user_kb_embedding_model,
    )

    # Re-expand with empty-vector placeholders so caller can zip with input.
    result: list[list[float]] = [[] for _ in texts]
    for (orig_idx, _), item in zip(non_empty, items):
        result[orig_idx] = item["embedding"]
    return result


# ── Introspection (tests, /health, etc.) ──────────────────────


def circuit_state() -> str:
    """Returns the current circuit state: 'closed', 'half_open', 'open'."""
    return _breaker.state


def _reset_for_tests() -> None:
    """Clear cache + circuit state. Tests only."""
    global _breaker
    _breaker = _CircuitBreaker()
    _cache.clear()
