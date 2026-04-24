"""Shared pytest fixtures for KB tests.

Adds the repo root to sys.path so `from backend.app...` imports work when
pytest is invoked from anywhere inside the repo.

Also resets the Motor client singleton between async tests. Motor's
AsyncIOMotorClient binds to the event loop it first runs on; pytest-asyncio
creates a new loop per test by default, so a cached client from test A is
useless in test B ("Event loop is closed"). Clearing the ``lru_cache`` before
each test makes the next call bind to that test's current loop.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture(autouse=True)
def _reset_kb_mongo_client():
    """Drop the Motor client singletons before each test.

    Motor's AsyncIOMotorClient binds to the event loop it was first awaited
    on; pytest-asyncio gives a fresh loop per test, so we must clear the
    lru_cache or subsequent calls fail with "Event loop is closed".
    """
    def _clear():
        try:
            from backend.app.services import kb_service as _kb
            _kb._get_client.cache_clear()
        except Exception:
            pass
        try:
            from backend.app.services import user_kb_service as _ukb
            _ukb._clear_mongo_client_cache()
            _ukb._reset_index_init_for_retry()
        except Exception:
            pass
    _clear()
    yield
    _clear()
