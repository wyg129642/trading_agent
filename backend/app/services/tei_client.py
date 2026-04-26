"""HTTP client for the TEI (Text Embeddings Inference) server on jumpbox.

Design goals
------------
1. **Fail-fast, never fail-silent.** Jumpbox network hiccups must not starve
   the FastAPI event loop — strict connect/read timeouts + 3-strike circuit
   breaker (cf. infra_futu_opend_required memory).
2. **Async-native.** Works inside uvicorn workers and inside the sync script
   context (via a small ``run_sync`` helper).
3. **Two call paths:**
   - ``embed_query(text)``  — low-latency, LRU-cached, single string.
   - ``embed_batch(texts)`` — high-throughput, no cache, used by the
     ingestion worker to batch 32-64 chunks per HTTP round-trip.
4. **Idempotent module-level singleton.** A single shared ``httpx.AsyncClient``
   is created lazily; callers don't need to manage lifecycle.

Endpoint: OpenAI-compatible ``POST {base_url}/v1/embeddings`` with bearer auth.
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Sequence

import httpx

from backend.app.config import get_settings

logger = logging.getLogger(__name__)


# ── Exceptions ────────────────────────────────────────────────────


class TEIError(RuntimeError):
    """Base for all TEI client failures."""


class TEICircuitOpen(TEIError):
    """Raised when circuit breaker is open — caller should degrade."""


class TEIRequestError(TEIError):
    """Raised when a request fails after local retries."""


# ── Circuit breaker ───────────────────────────────────────────────


@dataclass
class _CircuitBreaker:
    """3-strike circuit breaker with 60s cooldown.

    State transitions:
        closed → (N consecutive failures) → open
        open   → (cooldown elapsed)       → half_open
        half_open → (1 success) → closed
        half_open → (1 failure) → open (reset cooldown)

    Half-open policy: only ONE caller is allowed through as a probe; others
    continue to see the circuit as open until the probe resolves. Without
    this, a burst of concurrent callers would all hit a downed backend at
    cooldown expiration, producing a thundering herd.
    """
    threshold: int = 3
    cooldown_s: float = 60.0

    consecutive_failures: int = 0
    opened_at: float = 0.0
    half_open_in_flight: bool = False
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def on_success(self) -> None:
        async with self._lock:
            self.consecutive_failures = 0
            self.opened_at = 0.0
            self.half_open_in_flight = False

    async def on_failure(self) -> None:
        async with self._lock:
            self.consecutive_failures += 1
            self.half_open_in_flight = False
            if self.consecutive_failures >= self.threshold:
                # Reset cooldown so half_open → open transitions get their
                # full backoff window.
                self.opened_at = time.monotonic()

    async def ready(self) -> bool:
        """Return True if a request may proceed.

        • Closed: always True.
        • Open (cooldown not elapsed): False.
        • Half-open: True for exactly one probe, False for everyone else
          until that probe resolves via on_success/on_failure.
        """
        async with self._lock:
            if self.opened_at == 0.0:
                return True
            if time.monotonic() - self.opened_at < self.cooldown_s:
                return False
            # Cooldown elapsed → half-open. Let one probe through.
            if self.half_open_in_flight:
                return False
            self.half_open_in_flight = True
            return True


# ── LRU cache for query embeddings ────────────────────────────────


class _LRU:
    """Thread-unsafe bounded LRU. Safe for single-event-loop async usage."""

    def __init__(self, maxsize: int = 5000):
        self.maxsize = maxsize
        self._data: OrderedDict[str, list[float]] = OrderedDict()

    def get(self, key: str) -> list[float] | None:
        try:
            val = self._data.pop(key)
        except KeyError:
            return None
        self._data[key] = val
        return val

    def set(self, key: str, val: list[float]) -> None:
        if key in self._data:
            self._data.pop(key)
        elif len(self._data) >= self.maxsize:
            self._data.popitem(last=False)
        self._data[key] = val

    def clear(self) -> None:
        self._data.clear()

    def __len__(self) -> int:
        return len(self._data)


# ── Module singletons ─────────────────────────────────────────────


_client_lock = asyncio.Lock()
_client: httpx.AsyncClient | None = None
_breaker = _CircuitBreaker()
_query_cache = _LRU(maxsize=5000)


async def _get_client() -> httpx.AsyncClient:
    """Lazy-init a shared AsyncClient. Single-instance per process."""
    global _client
    if _client is not None:
        return _client
    async with _client_lock:
        if _client is not None:
            return _client
        s = get_settings()
        base_url = s.tei_base_url.rstrip("/")
        api_key = s.tei_api_key
        if not api_key:
            raise TEIError("TEI_API_KEY is not configured — see .env.secrets")
        _client = httpx.AsyncClient(
            base_url=base_url,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            # Single-query path stays well under 1 s on warm GPU; 60 s read
            # is for the batch ingest path where a 32-chunk batch can bump
            # against cold-model loads or brief GPU preemption.
            timeout=httpx.Timeout(connect=2.0, read=60.0, write=5.0, pool=5.0),
            limits=httpx.Limits(max_connections=64, max_keepalive_connections=32),
            # TEI is on LAN — never send via any ambient http(s)_proxy.
            trust_env=False,
            proxy=None,
        )
        return _client


async def close() -> None:
    """Close the shared client. Call from lifespan shutdown."""
    global _client
    async with _client_lock:
        if _client is not None:
            await _client.aclose()
            _client = None


# ── Core embedding calls ──────────────────────────────────────────


async def _post_embeddings(texts: Sequence[str]) -> list[list[float]]:
    """One HTTP round-trip. Internal: no breaker check, no cache."""
    client = await _get_client()
    payload = {
        "input": list(texts),
        "model": get_settings().tei_model_name,
    }
    t0 = time.monotonic()
    resp = await client.post("/v1/embeddings", json=payload)
    latency_ms = int((time.monotonic() - t0) * 1000)
    if resp.status_code != 200:
        raise TEIRequestError(
            f"TEI /v1/embeddings → HTTP {resp.status_code}: {resp.text[:200]}"
        )
    body = resp.json()
    data = body.get("data") or []
    if len(data) != len(texts):
        raise TEIRequestError(
            f"TEI returned {len(data)} embeddings for {len(texts)} inputs"
        )
    out = [item["embedding"] for item in data]
    logger.debug("TEI embed batch=%d latency=%dms", len(texts), latency_ms)
    return out


async def embed_batch(texts: Sequence[str]) -> list[list[float]]:
    """Embed N strings in one call. No caching (ingestion path).

    Raises TEICircuitOpen if breaker is tripped; caller should back off
    and retry later (the ingestion worker respects this and requeues).
    """
    if not texts:
        return []
    if not await _breaker.ready():
        raise TEICircuitOpen("TEI circuit open — cooldown not elapsed")

    try:
        vecs = await _post_embeddings(texts)
    except (httpx.HTTPError, TEIRequestError) as e:
        await _breaker.on_failure()
        logger.warning("TEI embed_batch failed (%s). consecutive=%d",
                       type(e).__name__, _breaker.consecutive_failures)
        raise TEIRequestError(str(e)) from e

    await _breaker.on_success()
    return vecs


async def embed_query(text: str) -> list[float]:
    """Embed one query with LRU caching. Used by kb_search.

    Cache is in-memory per process; LRU 5000 entries. Keyed by raw text,
    trim + lower-case normalization only (don't over-normalize because
    CN/EN casing matters semantically).
    """
    if not text or not text.strip():
        raise TEIError("embed_query: empty text")

    key = text.strip()
    cached = _query_cache.get(key)
    if cached is not None:
        return cached

    vecs = await embed_batch([key])
    _query_cache.set(key, vecs[0])
    return vecs[0]


# ── Health probe ──────────────────────────────────────────────────


async def healthcheck(timeout_s: float = 3.0) -> bool:
    """GET /health. Returns True iff 200 OK. Never raises."""
    try:
        client = await _get_client()
        r = await client.get("/health", timeout=httpx.Timeout(timeout_s))
        return r.status_code == 200
    except Exception as e:
        logger.debug("TEI healthcheck failed: %s", e)
        return False


async def health_loop(interval_s: float = 30.0) -> None:
    """Long-running health probe. Start as a background task from lifespan.

    Trips the breaker on 3 consecutive misses even if no live request tripped
    it — catches silent outages during low traffic.
    """
    misses = 0
    while True:
        ok = await healthcheck()
        if ok:
            misses = 0
        else:
            misses += 1
            if misses >= 3:
                logger.error("TEI health probe %d consecutive failures", misses)
                await _breaker.on_failure()  # fold into existing breaker state
        await asyncio.sleep(interval_s)


# ── Observability ─────────────────────────────────────────────────


def cache_stats() -> dict:
    """Snapshot for /api/admin/vector/status."""
    return {
        "query_cache_size": len(_query_cache),
        "query_cache_maxsize": _query_cache.maxsize,
        "circuit_consecutive_failures": _breaker.consecutive_failures,
        "circuit_open": _breaker.opened_at > 0
        and (time.monotonic() - _breaker.opened_at) < _breaker.cooldown_s,
    }
