"""Staging-only: auto-mirror user / auth / workspace tables from prod.

Why this exists
---------------
Staging has its own Postgres database (``trading_agent_staging``) so
experiments don't corrupt prod state. But employees expect to log in with
their existing credentials, which live only in prod's ``users`` table.
A manual ``sync-users-from-prod`` CLI would mean every new prod signup
requires someone to re-run it — easy to forget.

Instead, when ``APP_ENV=staging`` the backend runs this task on startup
and then every ``interval_seconds`` forever. Each pass is an UPSERT
(``INSERT ... ON CONFLICT DO UPDATE``) so:

  * existing staging rows refresh with the latest prod values,
  * new prod users appear in staging within one tick,
  * FK targets in dependent staging-only tables (chat_conversations,
    stock_predictions, user_chat_memories, ...) are preserved — we do
    NOT ``TRUNCATE CASCADE``, which would wipe staging test data.

Users deleted in prod linger in staging. For an internal tool this is
acceptable; a future enhancement could reconcile deletions by diffing
primary keys.

Tables mirrored (chosen to make login + workspace + dashboard work):

  users              — auth (required)
  user_preferences   — UI prefs, language, holdings-init flag
  user_sources       — subscribed news sources
  kb_folders         — personal/public KB folder tree (workspace page)
  watchlists         — portfolio definitions
  watchlist_items    — portfolio tickers

Tables explicitly NOT mirrored (staging must be free to diverge):
  chat_*, stock_predictions, user_news_read, user_favorites,
  user_chat_memories, user_feedback_events, signal_evaluations, and
  everything else.
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Iterable

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from backend.app.config import get_settings

logger = logging.getLogger(__name__)


# (table_name, primary-key columns). Order does not matter because we
# disable FK checks for the duration of one sync pass.
_TABLES: list[tuple[str, tuple[str, ...]]] = [
    ("users", ("id",)),
    ("user_preferences", ("user_id",)),
    ("user_sources", ("id",)),
    ("kb_folders", ("id",)),
    ("watchlists", ("id",)),
    ("watchlist_items", ("id",)),
]


def _prod_database_url() -> str:
    """Build a prod Postgres URL from staging settings.

    Staging's DB name is the prod name with ``_staging`` appended (by
    ``Settings.effective_postgres_db``). We strip that suffix to get the
    prod DB name while reusing the host/port/user/password. Same Postgres
    instance, different database.
    """
    s = get_settings()
    db = s.postgres_db or "trading_agent"
    if db.endswith("_staging"):
        db = db[: -len("_staging")]
    return (
        f"postgresql+asyncpg://{s.postgres_user}:{s.postgres_password}"
        f"@{s.postgres_host}:{s.postgres_port}/{db}"
    )


def _normalize_value(v: Any) -> Any:
    """Re-encode values that asyncpg's JSONB codec can't handle.

    asyncpg expects JSONB bindings to be pre-serialized strings (it
    prepends the `\\x01` wire-format byte). SQLAlchemy hands us already-
    parsed ``list`` / ``dict`` objects from the prod SELECT, so we
    re-serialize them here. ``tuple`` is also JSON-encoded because the
    prod side sometimes surfaces ARRAY columns as tuples.
    """
    if isinstance(v, (dict, list)):
        return json.dumps(v, ensure_ascii=False, default=str)
    return v


async def _upsert_table(
    prod_conn,
    stg_conn,
    table: str,
    pk_cols: tuple[str, ...],
) -> int:
    """Copy rows from prod.<table> into staging.<table> via upsert.

    Returns the number of rows pushed. Columns are auto-discovered from
    the prod row so schema additions don't require editing this file.
    """
    result = await prod_conn.execute(text(f'SELECT * FROM "{table}"'))
    rows = result.mappings().all()
    if not rows:
        return 0

    columns = list(rows[0].keys())
    cols_sql = ", ".join(f'"{c}"' for c in columns)
    placeholders = ", ".join(f":{c}" for c in columns)
    pk_sql = ", ".join(f'"{c}"' for c in pk_cols)
    set_cols = [c for c in columns if c not in pk_cols]
    if set_cols:
        update_clause = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in set_cols)
        conflict = f"ON CONFLICT ({pk_sql}) DO UPDATE SET {update_clause}"
    else:
        conflict = f"ON CONFLICT ({pk_sql}) DO NOTHING"

    sql = text(f'INSERT INTO "{table}" ({cols_sql}) VALUES ({placeholders}) {conflict}')
    payload = [
        {k: _normalize_value(v) for k, v in dict(r).items()}
        for r in rows
    ]
    await stg_conn.execute(sql, payload)
    return len(payload)


async def sync_users_from_prod_once() -> dict[str, int]:
    """Single sync pass. Returns per-table row counts upserted."""
    s = get_settings()
    if not s.is_staging:
        logger.debug("staging_user_sync skipped: APP_ENV=%s", s.app_env)
        return {}

    prod_url = _prod_database_url()
    # Smaller pool — this is a low-frequency background task.
    prod_engine = create_async_engine(prod_url, pool_size=1, max_overflow=0, pool_pre_ping=True)
    stg_engine = create_async_engine(s.database_url, pool_size=1, max_overflow=0, pool_pre_ping=True)
    counts: dict[str, int] = {}
    try:
        async with prod_engine.connect() as prod_conn, stg_engine.begin() as stg_conn:
            # Bypass FK checks for the duration of this transaction so
            # kb_folders' self-referential parent_id and cross-table FKs
            # don't care about insertion order.
            await stg_conn.execute(text("SET session_replication_role = 'replica'"))
            for table, pk in _TABLES:
                try:
                    counts[table] = await _upsert_table(prod_conn, stg_conn, table, pk)
                except Exception:
                    # Don't abort the whole sync for one bad table — log
                    # and keep going. A partial refresh beats no refresh.
                    logger.exception("staging_user_sync: failed on table %s", table)
                    counts[table] = -1
            await stg_conn.execute(text("SET session_replication_role = 'origin'"))
    finally:
        await prod_engine.dispose()
        await stg_engine.dispose()
    return counts


async def run_staging_user_sync_loop(interval_seconds: int = 900) -> None:
    """Run the sync on startup, then every ``interval_seconds`` (default 15 min).

    Designed to be launched with ``asyncio.create_task`` in the FastAPI
    lifespan. Cancellation is cooperative — cancel the task to stop.
    """
    s = get_settings()
    if not s.is_staging:
        logger.info("staging_user_sync: disabled (APP_ENV=%s)", s.app_env)
        return

    logger.info(
        "staging_user_sync: starting (interval=%ds, src DB=%s, dst DB=%s)",
        interval_seconds,
        _prod_database_url().rsplit("/", 1)[-1],
        s.effective_postgres_db,
    )
    while True:
        try:
            counts = await sync_users_from_prod_once()
            ok = sum(v for v in counts.values() if v >= 0)
            failed = [t for t, v in counts.items() if v < 0]
            if failed:
                logger.warning(
                    "staging_user_sync: pass done with errors — %d rows upserted, failed: %s",
                    ok, failed,
                )
            else:
                logger.info(
                    "staging_user_sync: pass done — %d rows upserted across %d tables (%s)",
                    ok, len(counts),
                    ", ".join(f"{t}={n}" for t, n in counts.items()),
                )
        except asyncio.CancelledError:
            logger.info("staging_user_sync: cancelled")
            raise
        except Exception:
            logger.exception("staging_user_sync: unexpected error in loop")

        try:
            await asyncio.sleep(interval_seconds)
        except asyncio.CancelledError:
            logger.info("staging_user_sync: cancelled during sleep")
            raise


def _sync_table_list() -> Iterable[str]:
    """Expose the mirrored table names for documentation / admin UIs."""
    return (t for t, _ in _TABLES)
