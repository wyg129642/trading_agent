"""Unit tests for tei_client — circuit breaker, LRU, batching.

Runs without a live TEI server by mocking httpx.AsyncClient.post.
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from backend.app.services import tei_client
from backend.app.services.tei_client import (
    TEICircuitOpen,
    TEIError,
    TEIRequestError,
    _LRU,
    _CircuitBreaker,
)


# ── Helpers ────────────────────────────────────────────────────────


def _fake_response(embeddings: list[list[float]], status: int = 200) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status
    resp.json = MagicMock(return_value={
        "data": [{"embedding": e, "index": i} for i, e in enumerate(embeddings)],
    })
    resp.text = "ok"
    return resp


@pytest.fixture(autouse=True)
def _reset_module_state():
    """Reset module singletons between tests."""
    tei_client._client = None
    tei_client._breaker = _CircuitBreaker()
    tei_client._query_cache = _LRU(maxsize=10)
    yield


@pytest.fixture
def mock_settings(monkeypatch):
    s = MagicMock()
    s.tei_base_url = "http://jumpbox:8080"
    s.tei_api_key = "test-key"
    s.tei_model_name = "qwen3-embed"
    monkeypatch.setattr(tei_client, "get_settings", lambda: s)
    return s


# ── LRU ────────────────────────────────────────────────────────────


def test_lru_eviction():
    lru = _LRU(maxsize=3)
    lru.set("a", [1.0])
    lru.set("b", [2.0])
    lru.set("c", [3.0])
    lru.set("d", [4.0])  # evicts "a"
    assert lru.get("a") is None
    assert lru.get("b") == [2.0]
    # "b" is now most recent
    lru.set("e", [5.0])  # evicts "c" (LRU), not "b"
    assert lru.get("c") is None
    assert lru.get("b") == [2.0]


def test_lru_update_preserves_recency():
    lru = _LRU(maxsize=2)
    lru.set("a", [1.0])
    lru.set("b", [2.0])
    lru.set("a", [10.0])  # refresh "a" to most recent
    lru.set("c", [3.0])   # evicts "b", not "a"
    assert lru.get("b") is None
    assert lru.get("a") == [10.0]


# ── Circuit breaker ────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_breaker_opens_after_threshold():
    br = _CircuitBreaker(threshold=3, cooldown_s=60)
    assert await br.ready() is True
    await br.on_failure()
    await br.on_failure()
    assert await br.ready() is True  # still under threshold
    await br.on_failure()
    assert await br.ready() is False  # tripped


@pytest.mark.asyncio
async def test_breaker_half_open_after_cooldown():
    br = _CircuitBreaker(threshold=2, cooldown_s=0.05)
    await br.on_failure()
    await br.on_failure()
    assert await br.ready() is False
    await asyncio.sleep(0.08)
    assert await br.ready() is True  # half-open


@pytest.mark.asyncio
async def test_breaker_resets_on_success():
    br = _CircuitBreaker(threshold=3, cooldown_s=60)
    await br.on_failure()
    await br.on_failure()
    await br.on_success()
    await br.on_failure()
    await br.on_failure()
    assert await br.ready() is True  # two new failures don't hit old count


# ── embed_batch + embed_query ──────────────────────────────────────


@pytest.mark.asyncio
async def test_embed_batch_returns_vectors(mock_settings):
    with patch.object(httpx, "AsyncClient") as MockClient:
        inst = MockClient.return_value
        inst.post = AsyncMock(return_value=_fake_response([[0.1, 0.2], [0.3, 0.4]]))
        inst.aclose = AsyncMock()
        out = await tei_client.embed_batch(["a", "b"])
        assert out == [[0.1, 0.2], [0.3, 0.4]]


@pytest.mark.asyncio
async def test_embed_batch_empty_returns_empty(mock_settings):
    assert await tei_client.embed_batch([]) == []


@pytest.mark.asyncio
async def test_embed_query_hits_cache(mock_settings):
    with patch.object(httpx, "AsyncClient") as MockClient:
        inst = MockClient.return_value
        inst.post = AsyncMock(return_value=_fake_response([[0.5] * 4]))
        inst.aclose = AsyncMock()

        v1 = await tei_client.embed_query("hello")
        v2 = await tei_client.embed_query("hello")
        assert v1 == v2 == [0.5] * 4
        # Only one POST — second call hit cache
        assert inst.post.call_count == 1


@pytest.mark.asyncio
async def test_embed_query_cache_is_trim_sensitive(mock_settings):
    with patch.object(httpx, "AsyncClient") as MockClient:
        inst = MockClient.return_value
        inst.post = AsyncMock(side_effect=[
            _fake_response([[1.0]]),
            _fake_response([[2.0]]),
        ])
        inst.aclose = AsyncMock()

        v1 = await tei_client.embed_query("  hello  ")
        v2 = await tei_client.embed_query("hello")  # same after trim → cache hit
        assert v1 == v2 == [1.0]
        assert inst.post.call_count == 1


@pytest.mark.asyncio
async def test_embed_query_rejects_empty(mock_settings):
    with pytest.raises(TEIError):
        await tei_client.embed_query("")
    with pytest.raises(TEIError):
        await tei_client.embed_query("   ")


@pytest.mark.asyncio
async def test_circuit_opens_after_consecutive_failures(mock_settings):
    with patch.object(httpx, "AsyncClient") as MockClient:
        inst = MockClient.return_value
        inst.post = AsyncMock(side_effect=httpx.ConnectError("refused"))
        inst.aclose = AsyncMock()

        for _ in range(3):
            with pytest.raises(TEIRequestError):
                await tei_client.embed_batch(["x"])

        # 4th call: breaker should be open → TEICircuitOpen
        with pytest.raises(TEICircuitOpen):
            await tei_client.embed_batch(["x"])


@pytest.mark.asyncio
async def test_healthcheck_never_raises(mock_settings):
    with patch.object(httpx, "AsyncClient") as MockClient:
        inst = MockClient.return_value
        inst.get = AsyncMock(side_effect=httpx.ConnectError("refused"))
        inst.aclose = AsyncMock()
        # Should return False, not raise
        assert await tei_client.healthcheck(timeout_s=0.1) is False


@pytest.mark.asyncio
async def test_missing_api_key_raises(monkeypatch):
    s = MagicMock()
    s.tei_base_url = "http://jumpbox:8080"
    s.tei_api_key = ""  # empty
    s.tei_model_name = "qwen3-embed"
    monkeypatch.setattr(tei_client, "get_settings", lambda: s)

    with pytest.raises(TEIError, match="TEI_API_KEY"):
        await tei_client.embed_batch(["x"])


@pytest.mark.asyncio
async def test_concurrent_embeds_share_client(mock_settings):
    """10 concurrent embed_query should use a single AsyncClient instance."""
    call_count = 0

    async def post_side_effect(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        await asyncio.sleep(0.01)
        # Unique vector per input so cache doesn't short-circuit
        input_texts = kwargs.get("json", {}).get("input", [])
        return _fake_response([[float(hash(t) % 1000)] for t in input_texts])

    with patch.object(httpx, "AsyncClient") as MockClient:
        inst = MockClient.return_value
        inst.post = AsyncMock(side_effect=post_side_effect)
        inst.aclose = AsyncMock()

        results = await asyncio.gather(*(
            tei_client.embed_query(f"q{i}") for i in range(10)
        ))

    # AsyncClient class instantiated at most once
    assert MockClient.call_count <= 1
    assert len(results) == 10
    assert call_count == 10  # 10 queries, none cached
