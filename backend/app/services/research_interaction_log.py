"""Persistent recorder for the AI research assistant's end-to-end workflow.

Captures the full lifecycle of every /api/chat request into MongoDB so the
admin "Research Logs" page (and Claude Code, while iterating on the chat
module) can replay exactly what each LLM did:

  user query
    ├── per-model timeline
    │     ├── round 1: llm_request (messages, tools) → reasoning → tool_calls[]
    │     │     └── per tool_call: full arguments + result
    │     │           └── web_search → engines called + urls returned
    │     │           └── read_webpage → full fetched content
    │     ├── round 2 ...
    │     └── final_content, tokens, latency, error
    └── aggregate summary (rounds, tool calls, urls_found, urls_read, citations)

Schema (one doc per trace_id, collection `research_sessions`):

  {
    trace_id,
    user_id, username, conversation_id,
    query, attachments, models_requested, mode,
    web_search, alphapai_enabled, jinmen_enabled, kb_enabled,
    tools_enabled, tool_names,
    system_prompt, system_prompt_len, history_len,
    initial_messages_preview,
    models: {
      <model_id>: {
        model_name, status, events[], rounds[],
        final_content, final_content_len, total_tokens, latency_ms, error,
      }
    },
    summary: { rounds_used, tool_calls_total, tool_call_names,
               search_queries, urls_found, urls_read, citations,
               final_content_len, total_tokens },
    total_elapsed_ms,
    status,
    created_at, updated_at,
  }

All writes are best-effort: connection / auth failures log a warning once and
then switch to a no-op mode so the chat pipeline is never blocked.
"""
from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Any

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

from backend.app.config import get_settings

logger = logging.getLogger(__name__)


_MAX_STR = 12000           # truncate very long strings (per value)
_MAX_MESSAGES_PREVIEW = 20000  # messages payload preview cap
_MAX_EVENT_DETAIL = 6000
_MAX_TOOL_RESULT = 20000   # per-tool result cap; webpage reads get their own larger cap
_MAX_WEBPAGE = 40000
# Env-scoped collection name: prod writes `research_sessions`, staging writes
# `stg_research_sessions`. Both co-inhabit the same `ti-user-knowledge-base`
# DB because u_spider cannot create new DBs on the remote cluster.
_DEFAULT_COLL_BASE = "research_sessions"


def _default_collection_name() -> str:
    # Prefer the env-scoped name from Settings when available so staging
    # never scribbles into prod's session log. Fallback is the raw base
    # name so test stubs / ad-hoc scripts that bypass Settings still work.
    try:
        return get_settings().research_sessions_collection
    except Exception:
        return _DEFAULT_COLL_BASE


def _safe_key(model_id: str) -> str:
    """MongoDB uses '.' as field path separator, so model IDs like
    'openai/gpt-5.4' would be split incorrectly when used as a dict key in a
    `$set`/`$push` update. Replace separators with '__' so paths stay atomic.
    We also map '/' → '__' to keep keys URL-safe when reading."""
    return (model_id or "").replace(".", "__DOT__").replace("/", "__SLASH__")


def _unsafe_key(safe: str) -> str:
    return (safe or "").replace("__SLASH__", "/").replace("__DOT__", ".")


def _truncate(val: Any, limit: int = _MAX_STR) -> Any:
    """Truncate strings for persistence; leaves non-strings untouched."""
    if val is None:
        return None
    if isinstance(val, str):
        if len(val) <= limit:
            return val
        half = limit // 2
        return val[:half] + f"\n... [truncated {len(val) - limit} chars] ...\n" + val[-half:]
    return val


def _truncate_message(msg: dict) -> dict:
    """Compact a single chat-style message for storage."""
    out = {"role": msg.get("role", "")}
    content = msg.get("content")
    if isinstance(content, list):
        # multimodal content
        parts = []
        for p in content:
            if isinstance(p, dict):
                if p.get("type") == "text":
                    parts.append({"type": "text", "text": _truncate(p.get("text", ""), 4000)})
                elif p.get("type") in ("image_url", "image"):
                    parts.append({"type": p.get("type"), "image_url": "[image]"})
                else:
                    parts.append({"type": p.get("type", "unknown")})
            else:
                parts.append({"type": "raw", "value": _truncate(str(p), 2000)})
        out["content"] = parts
    else:
        out["content"] = _truncate(content, _MAX_STR) if content is not None else None

    if msg.get("tool_calls"):
        out["tool_calls"] = [
            {
                "id": tc.get("id", ""),
                "function": {
                    "name": tc.get("function", {}).get("name", ""),
                    "arguments": _truncate(tc.get("function", {}).get("arguments", ""), 4000),
                },
            }
            for tc in msg.get("tool_calls", [])
        ]
    if msg.get("tool_call_id"):
        out["tool_call_id"] = msg["tool_call_id"]
    return out


def _truncate_messages(messages: list[dict]) -> list[dict]:
    if not messages:
        return []
    # Keep first 2 (system + first user) and tail — middle can balloon on long tool loops.
    if len(messages) <= 25:
        return [_truncate_message(m) for m in messages]
    head = [_truncate_message(m) for m in messages[:2]]
    tail = [_truncate_message(m) for m in messages[-22:]]
    return head + [{"role": "_elided", "content": f"[{len(messages) - 24} messages elided]"}] + tail


# ── Recorder singleton ───────────────────────────────────────────

class ResearchInteractionRecorder:
    """MongoDB-backed recorder. Fail-open — never raises to callers.

    All public methods are safe to call from any async context; they schedule
    actual Mongo writes as background tasks so chat latency is unaffected.
    """

    def __init__(
        self,
        mongo_uri: str,
        db_name: str,
        collection_name: str | None = None,
    ):
        # Resolve the default at call-time so staging starts writing to
        # `stg_research_sessions` after APP_ENV is set — a class-level
        # default would lock in the prod name at import time.
        if collection_name is None:
            collection_name = _default_collection_name()
        self._mongo_uri = mongo_uri
        self._db_name = db_name
        self._collection_name = collection_name
        self._client: AsyncIOMotorClient | None = None
        self._db: AsyncIOMotorDatabase | None = None
        self._enabled = False          # True once we've successfully pinged the server
        self._disabled_reason = ""     # reason if disabled
        self._init_lock = asyncio.Lock()
        self._init_attempted = False

    @property
    def enabled(self) -> bool:
        return self._enabled

    async def connect(self) -> bool:
        """Attempt to connect + create indexes. Returns True on success."""
        async with self._init_lock:
            if self._init_attempted:
                return self._enabled
            self._init_attempted = True
            try:
                self._client = AsyncIOMotorClient(
                    self._mongo_uri,
                    serverSelectionTimeoutMS=3000,
                    tz_aware=True,
                )
                # Ping forces auth / connectivity check
                await self._client.admin.command("ping")
                self._db = self._client[self._db_name]
                coll = self._db[self._collection_name]
                # Create helpful indexes (idempotent)
                await asyncio.gather(
                    coll.create_index("trace_id", unique=True),
                    coll.create_index("user_id"),
                    coll.create_index("username"),
                    coll.create_index([("created_at", -1)]),
                    coll.create_index("conversation_id"),
                    coll.create_index("status"),
                    return_exceptions=True,
                )
                self._enabled = True
                logger.info(
                    "ResearchInteractionRecorder: connected to %s (db=%s coll=%s)",
                    self._mongo_uri.split("@")[-1], self._db_name, self._collection_name,
                )
                return True
            except Exception as e:
                self._enabled = False
                self._disabled_reason = str(e)[:300]
                logger.warning(
                    "ResearchInteractionRecorder: disabled — connection/auth failed: %s",
                    self._disabled_reason,
                )
                return False

    async def close(self):
        if self._client:
            try:
                self._client.close()
            except Exception:
                pass

    def _coll(self):
        if not self._enabled or self._db is None:
            return None
        return self._db[self._collection_name]

    def _fire(self, coro):
        """Schedule a write as a background task (fire-and-forget)."""
        if not self._enabled:
            return
        try:
            asyncio.create_task(self._safe(coro))
        except RuntimeError:
            # No running loop — best effort, drop
            pass

    @staticmethod
    async def _safe(coro):
        try:
            await coro
        except Exception as e:
            logger.warning("ResearchInteractionRecorder write failed: %s", str(e)[:200])

    # ── High-level recording API ─────────────────────────────────

    async def start_request(
        self,
        *,
        trace_id: str,
        user_id: str,
        username: str,
        conversation_id: str,
        query: str,
        attachments: list | None,
        models_requested: list[str],
        mode: str,
        web_search: str,
        alphapai_enabled: bool,
        jinmen_enabled: bool,
        kb_enabled: bool,
        tools_enabled: list[str],
        system_prompt: str,
        initial_messages: list[dict],
        history_len: int,
    ):
        """Create the top-level session document."""
        if not self._enabled:
            return
        now = datetime.now(timezone.utc)
        doc = {
            "trace_id": trace_id,
            "user_id": user_id,
            "username": username,
            "conversation_id": conversation_id,
            "query": _truncate(query, 8000),
            "attachments": attachments or [],
            "models_requested": models_requested,
            "mode": mode,
            "web_search": web_search,
            "alphapai_enabled": alphapai_enabled,
            "jinmen_enabled": jinmen_enabled,
            "kb_enabled": kb_enabled,
            "tools_enabled": tools_enabled,
            "system_prompt_preview": _truncate(system_prompt, 6000),
            "system_prompt_len": len(system_prompt or ""),
            "history_len": history_len,
            "initial_messages": _truncate_messages(initial_messages),
            "models": {
                _safe_key(m): {"model_id": m, "status": "pending", "events": [], "rounds": []}
                for m in models_requested
            },
            "summary": {},
            "total_elapsed_ms": None,
            "status": "running",
            "created_at": now,
            "updated_at": now,
        }
        try:
            await self._coll().replace_one(
                {"trace_id": trace_id},
                doc,
                upsert=True,
            )
        except Exception as e:
            logger.warning("ResearchInteractionRecorder start_request failed: %s", str(e)[:200])

    def _push_event(self, trace_id: str, model_id: str, event: dict):
        """Append a timeline event under models.<model_id>.events."""
        coll = self._coll()
        if coll is None:
            return
        event = {**event, "ts": datetime.now(timezone.utc)}
        mk = _safe_key(model_id)
        field_events = f"models.{mk}.events"
        self._fire(coll.update_one(
            {"trace_id": trace_id},
            {
                "$push": {field_events: event},
                "$set": {"updated_at": datetime.now(timezone.utc)},
            },
        ))

    def record_event(self, trace_id: str, model_id: str, event_type: str, **data):
        """Generic timeline event (SSE-like) for a model."""
        self._push_event(trace_id, model_id, {"type": event_type, **data})

    def record_model_start(self, trace_id: str, model_id: str, model_name: str):
        coll = self._coll()
        if coll is None:
            return
        mk = _safe_key(model_id)
        self._fire(coll.update_one(
            {"trace_id": trace_id},
            {
                "$set": {
                    f"models.{mk}.model_name": model_name,
                    f"models.{mk}.status": "running",
                    f"models.{mk}.started_at": datetime.now(timezone.utc),
                    "updated_at": datetime.now(timezone.utc),
                },
            },
        ))
        self._push_event(trace_id, model_id, {"type": "model_start", "model_name": model_name})

    def record_round_start(
        self,
        trace_id: str,
        model_id: str,
        round_num: int,
        messages: list[dict],
        tools: list[dict] | None,
        mode: str,
    ):
        """Record the start of an LLM round (payload sent to the model)."""
        tool_names = [t.get("function", {}).get("name", "?") for t in (tools or [])]
        coll = self._coll()
        if coll is None:
            return
        round_doc = {
            "round": round_num,
            "mode": mode,
            "tool_names": tool_names,
            "messages_preview": _truncate_messages(messages),
            "started_at": datetime.now(timezone.utc),
            "tool_calls": [],
            "reasoning": "",
            "response_preview": "",
            "finish_reason": None,
            "latency_ms": None,
        }
        mk = _safe_key(model_id)
        self._fire(coll.update_one(
            {"trace_id": trace_id},
            {
                "$push": {f"models.{mk}.rounds": round_doc},
                "$set": {"updated_at": datetime.now(timezone.utc)},
            },
        ))
        self._push_event(trace_id, model_id, {
            "type": "round_start", "round": round_num, "mode": mode,
            "tool_names": tool_names, "message_count": len(messages),
        })

    def record_reasoning(self, trace_id: str, model_id: str, round_num: int, text: str):
        """Store the model's text emitted before tool calls in this round."""
        if not text:
            return
        coll = self._coll()
        if coll is None:
            return
        mk = _safe_key(model_id)
        self._fire(coll.update_one(
            {"trace_id": trace_id, f"models.{mk}.rounds.round": round_num},
            {"$set": {f"models.{mk}.rounds.$.reasoning": _truncate(text, _MAX_EVENT_DETAIL)}},
        ))
        self._push_event(trace_id, model_id, {
            "type": "reasoning", "round": round_num,
            "text": _truncate(text, 2000),
        })

    def record_tool_calls_detected(
        self,
        trace_id: str,
        model_id: str,
        round_num: int,
        tool_calls: list[dict],
    ):
        """Log the set of tool calls the model decided to make (with full args)."""
        calls = []
        for tc in tool_calls:
            fn = tc.get("function") or {}
            raw_args = tc.get("arguments") or fn.get("arguments") or "{}"
            if isinstance(raw_args, (dict, list)):
                try:
                    import json as _json
                    raw_args_str = _json.dumps(raw_args, ensure_ascii=False)
                except Exception:
                    raw_args_str = str(raw_args)
            else:
                raw_args_str = str(raw_args)
            calls.append({
                "id": tc.get("id", ""),
                "name": fn.get("name") or tc.get("name", ""),
                "arguments": _truncate(raw_args_str, 4000),
            })
        coll = self._coll()
        if coll is None:
            return
        mk = _safe_key(model_id)
        self._fire(coll.update_one(
            {"trace_id": trace_id, f"models.{mk}.rounds.round": round_num},
            {"$set": {f"models.{mk}.rounds.$.tool_calls_declared": calls}},
        ))
        self._push_event(trace_id, model_id, {
            "type": "tool_calls_detected", "round": round_num, "count": len(calls),
            "calls": calls,
        })

    def record_tool_result(
        self,
        trace_id: str,
        model_id: str,
        round_num: int,
        tool_name: str,
        arguments: dict,
        result: str,
        elapsed_ms: int,
        error: str | None = None,
        extra: dict | None = None,
    ):
        """Record a tool invocation's full arguments + result + any extra structured data
        (search engines / urls returned / webpage content, etc.)."""
        coll = self._coll()
        if coll is None:
            return
        # Tool-specific truncation: webpage reads get a bigger quota
        limit = _MAX_WEBPAGE if tool_name == "read_webpage" else _MAX_TOOL_RESULT
        entry = {
            "tool_name": tool_name,
            "round": round_num,
            "arguments": arguments,
            "result_preview": _truncate(result, limit),
            "result_len": len(result or ""),
            "elapsed_ms": elapsed_ms,
            "error": error,
            "ts": datetime.now(timezone.utc),
        }
        if extra:
            entry["extra"] = extra
        mk = _safe_key(model_id)
        self._fire(coll.update_one(
            {"trace_id": trace_id},
            {
                "$push": {f"models.{mk}.tool_results": entry},
                "$set": {"updated_at": datetime.now(timezone.utc)},
            },
        ))
        # Also attach to the round's tool_calls for side-by-side visibility.
        self._fire(coll.update_one(
            {"trace_id": trace_id, f"models.{mk}.rounds.round": round_num},
            {"$push": {f"models.{mk}.rounds.$.tool_calls": entry}},
        ))
        self._push_event(trace_id, model_id, {
            "type": "tool_result", "round": round_num, "tool_name": tool_name,
            "elapsed_ms": elapsed_ms, "result_len": len(result or ""),
            "error": error,
        })

    def record_search_engines(
        self,
        trace_id: str,
        model_id: str,
        query_cn: str,
        query_en: str,
        search_type: str,
        recency: str,
        engines_called: list[dict],
        top_results: list[dict],
    ):
        """Record the fan-out to Baidu/Tavily/Jina plus top merged results."""
        coll = self._coll()
        if coll is None:
            return
        doc = {
            "query_cn": _truncate(query_cn, 500),
            "query_en": _truncate(query_en or "", 500),
            "search_type": search_type,
            "recency": recency,
            "engines": engines_called,  # list of {engine, status, latency_ms, result_count, urls: [{title,url,date,score,...}]}
            "top_results": top_results,  # final merged results passed to the LLM
            "ts": datetime.now(timezone.utc),
        }
        mk = _safe_key(model_id)
        self._fire(coll.update_one(
            {"trace_id": trace_id},
            {
                "$push": {f"models.{mk}.search_calls": doc},
                "$set": {"updated_at": datetime.now(timezone.utc)},
            },
        ))
        self._push_event(trace_id, model_id, {
            "type": "search_fanout", "query_cn": _truncate(query_cn, 200),
            "engines": [e.get("engine", "?") for e in engines_called],
            "top_count": len(top_results),
        })

    def record_webpage_read(
        self,
        trace_id: str,
        model_id: str,
        url: str,
        status: str,
        content: str,
        latency_ms: int,
        error: str | None = None,
    ):
        """Record a full webpage fetch (body stored truncated to _MAX_WEBPAGE)."""
        coll = self._coll()
        if coll is None:
            return
        doc = {
            "url": url,
            "status": status,
            "content_len": len(content or ""),
            "content": _truncate(content, _MAX_WEBPAGE),
            "latency_ms": latency_ms,
            "error": error,
            "ts": datetime.now(timezone.utc),
        }
        mk = _safe_key(model_id)
        self._fire(coll.update_one(
            {"trace_id": trace_id},
            {
                "$push": {f"models.{mk}.webpage_reads": doc},
                "$set": {"updated_at": datetime.now(timezone.utc)},
            },
        ))
        self._push_event(trace_id, model_id, {
            "type": "webpage_read", "url": url, "status": status,
            "content_len": len(content or ""), "latency_ms": latency_ms,
        })

    def record_model_done(
        self,
        trace_id: str,
        model_id: str,
        final_content: str,
        tokens: int,
        latency_ms: int,
        error: str | None = None,
        citations: list[dict] | None = None,
        summary: dict | None = None,
    ):
        coll = self._coll()
        if coll is None:
            return
        mk = _safe_key(model_id)
        update = {
            "$set": {
                f"models.{mk}.status": "error" if error else "done",
                f"models.{mk}.final_content": _truncate(final_content, 80000),
                f"models.{mk}.final_content_len": len(final_content or ""),
                f"models.{mk}.total_tokens": tokens,
                f"models.{mk}.latency_ms": latency_ms,
                f"models.{mk}.error": error,
                f"models.{mk}.ended_at": datetime.now(timezone.utc),
                "updated_at": datetime.now(timezone.utc),
            },
        }
        if citations:
            update["$set"][f"models.{mk}.citations"] = citations
        if summary:
            update["$set"][f"models.{mk}.summary"] = summary
        self._fire(coll.update_one({"trace_id": trace_id}, update))
        self._push_event(trace_id, model_id, {
            "type": "model_done", "tokens": tokens, "latency_ms": latency_ms,
            "error": error, "final_content_len": len(final_content or ""),
        })

    async def finalize_request(
        self,
        trace_id: str,
        total_elapsed_ms: int,
    ):
        """Mark the whole request as complete and aggregate the per-model data."""
        coll = self._coll()
        if coll is None:
            return
        now = datetime.now(timezone.utc)
        doc = await coll.find_one({"trace_id": trace_id})
        if not doc:
            return

        # Aggregate across all models
        agg_tools: list[str] = []
        agg_queries: list[str] = []
        agg_urls_found: set[str] = set()
        agg_urls_read: set[str] = set()
        agg_citations: list[dict] = []
        tokens = 0
        final_len = 0

        for m_id, m_data in (doc.get("models") or {}).items():
            for r in (m_data.get("rounds") or []):
                for tc in (r.get("tool_calls") or []):
                    agg_tools.append(tc.get("tool_name", "?"))
                    if tc.get("tool_name") == "web_search":
                        q = (tc.get("arguments") or {}).get("query_cn", "")
                        if q:
                            agg_queries.append(q)
                    elif tc.get("tool_name") == "read_webpage":
                        u = (tc.get("arguments") or {}).get("url", "")
                        if u:
                            agg_urls_read.add(u)
            for s in (m_data.get("search_calls") or []):
                for r in (s.get("top_results") or []):
                    u = r.get("url", "")
                    if u:
                        agg_urls_found.add(u)
            for cit in (m_data.get("citations") or []):
                agg_citations.append(cit)
            tokens += m_data.get("total_tokens") or 0
            final_len += m_data.get("final_content_len") or 0

        summary = {
            "rounds_used": sum(len(m.get("rounds", [])) for m in (doc.get("models") or {}).values()),
            "tool_calls_total": len(agg_tools),
            "tool_call_names": agg_tools,
            "search_queries": agg_queries,
            "urls_found": sorted(agg_urls_found),
            "urls_read": sorted(agg_urls_read),
            "citations": agg_citations,
            "final_content_len": final_len,
            "total_tokens": tokens,
        }
        try:
            await coll.update_one(
                {"trace_id": trace_id},
                {"$set": {
                    "status": "done",
                    "total_elapsed_ms": total_elapsed_ms,
                    "updated_at": now,
                    "summary": summary,
                }},
            )
        except Exception as e:
            logger.warning("ResearchInteractionRecorder finalize failed: %s", str(e)[:200])


# ── Singleton wiring ────────────────────────────────────────────

_recorder: ResearchInteractionRecorder | None = None


def configure_recorder(
    mongo_uri: str | None = None,
    db_name: str | None = None,
    collection_name: str | None = None,
) -> ResearchInteractionRecorder:
    """Create (or rebuild) the singleton recorder instance."""
    global _recorder
    settings = get_settings()
    uri = mongo_uri or getattr(
        settings, "research_log_mongo_uri", None,
    ) or "mongodb://localhost:27017"
    db = db_name or getattr(
        settings, "research_log_mongo_db", None,
    ) or "research-agent-interaction-process-all-accounts"
    # Default collection name is env-scoped via Settings (prod: `research_sessions`,
    # staging: `stg_research_sessions`). Explicit override still wins.
    coll = collection_name or _default_collection_name()
    _recorder = ResearchInteractionRecorder(uri, db, coll)
    return _recorder


def get_recorder() -> ResearchInteractionRecorder:
    """Return the singleton recorder, configuring with defaults if needed."""
    global _recorder
    if _recorder is None:
        configure_recorder()
    return _recorder


async def init_recorder() -> ResearchInteractionRecorder:
    """Initialize the recorder (connect + create indexes). Safe to call at startup."""
    r = get_recorder()
    await r.connect()
    return r
