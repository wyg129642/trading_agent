"""Dedicated debug logging for the AI Chat pipeline.

Writes structured, high-detail logs to a separate file (logs/chat_debug.log)
so that LLM interactions, tool calls, and the full request lifecycle can be
diagnosed without wading through general application logs.

Every emitted log entry is also persisted into the chat_audit_run /
chat_audit_event Postgres tables via the async batched writer in
``chat_audit_writer``. The log file remains a parallel sink so a writer
outage never costs us observability.

Usage:
    from backend.app.services.chat_debug import chat_trace

    trace = chat_trace(user_id="abc", model_id="openai/gpt-5.4")
    trace.log_llm_request(messages, tools)
    trace.log_llm_chunk(delta_text)
    trace.log_tool_call("web_search", {"query_cn": "..."})
    trace.log_tool_result("web_search", result_text, elapsed_ms=1234)
    trace.log_llm_done(content, tokens, latency_ms)

Every log line includes the trace_id so a single user query can be followed
end-to-end across API → LLM → tools → response.
"""
from __future__ import annotations

import json
import logging
import time
import uuid
from contextvars import ContextVar
from pathlib import Path
from typing import Any

from backend.app.services import chat_audit_writer

# ── Logger setup ─────────────────────────────────────────────────

CHAT_DEBUG_LOGGER_NAME = "chat_debug"
_logger = logging.getLogger(CHAT_DEBUG_LOGGER_NAME)

# Context var for propagating trace_id across async call boundaries
_current_trace_id: ContextVar[str] = ContextVar("chat_trace_id", default="no-trace")

# Context var for the per-model scope. Set by chat_llm when a model's streaming
# entry creates its trace; downstream tool services (web_search_tool, kb_service,
# user_kb_service) pick it up automatically via chat_trace() so their events
# carry model_id instead of being mis-attributed to the request-level "shared"
# bucket. asyncio.create_task copies the parent context per task, so per-model
# values do not bleed between sibling tasks in the fan-out.
_current_model_id_var: ContextVar[str] = ContextVar("chat_model_id", default="")


def setup_chat_debug_logging(log_dir: str | Path = "logs") -> None:
    """Configure the chat_debug logger with a rotating file handler.

    Called once at startup (from main.py).
    """
    from logging.handlers import RotatingFileHandler

    log_dir = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "chat_debug.log"

    handler = RotatingFileHandler(
        log_file,
        maxBytes=50 * 1024 * 1024,  # 50 MB per file
        backupCount=10,
        encoding="utf-8",
    )
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-5s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)

    _logger.addHandler(handler)
    _logger.setLevel(logging.DEBUG)
    # Don't propagate to root logger (avoid duplicating in uvicorn output)
    _logger.propagate = False

    _logger.info("=== Chat debug logging initialized → %s ===", log_file)


def _truncate(text: str | None, max_len: int = 2000) -> str:
    """Truncate text for logging, preserving beginning and end."""
    if text is None:
        return "<None>"
    if len(text) <= max_len:
        return text
    half = max_len // 2
    return text[:half] + f"\n... [truncated {len(text) - max_len} chars] ...\n" + text[-half:]


def _safe_json(obj, max_len: int = 3000) -> str:
    """JSON-serialize with truncation, never raising."""
    try:
        s = json.dumps(obj, ensure_ascii=False, default=str)
        return _truncate(s, max_len)
    except Exception:
        return f"<json error: {type(obj).__name__}>"


# ── Per-trace shared state (run_id, sequence counter, rolling totals) ──

# trace_id → {
#   run_id: UUID, seq: int, master_started: bool,
#   tool_calls_total: int, tool_calls_by_name: dict,
#   urls_searched: int, urls_read: int, citations_count: int,
#   rounds_used: int, total_tokens: int, final_content_len: int,
# }
_run_state: dict[str, dict[str, Any]] = {}


def _get_or_create_state(trace_id: str) -> dict[str, Any]:
    state = _run_state.get(trace_id)
    if state is None:
        state = {
            "run_id": uuid.uuid4(),
            "seq": 0,
            "master_started": False,
            "tool_calls_total": 0,
            "tool_calls_by_name": {},
            "urls_searched": 0,
            "urls_read": 0,
            "citations_count": 0,
            "rounds_used": 0,
            "total_tokens": 0,
            "final_content_len": 0,
            "any_error": False,
            "first_error": None,
        }
        _run_state[trace_id] = state
    return state


def _pop_state(trace_id: str) -> dict[str, Any] | None:
    return _run_state.pop(trace_id, None)


class ChatTrace:
    """Scoped trace for one chat request (one user message → N model responses)."""

    def __init__(
        self,
        trace_id: str,
        user_id: str = "",
        username: str = "",
        conversation_id: str = "",
        message_id: str = "",
        model_id: str = "",
    ):
        self.trace_id = trace_id
        self.user_id = user_id
        self.username = username
        self.conversation_id = conversation_id
        self.message_id = message_id
        self.model_id = model_id
        self._start = time.monotonic()
        self._tool_round = 0
        # Shared state across master and per-model traces of the same trace_id.
        self._state = _get_or_create_state(trace_id)
        self.run_id: uuid.UUID = self._state["run_id"]

    # ── Internal helpers ─────────────────────────────────────────

    def _prefix(self) -> str:
        parts = [f"trace={self.trace_id}"]
        if self.model_id:
            parts.append(f"model={self.model_id}")
        return " | ".join(parts)

    def _next_seq(self) -> int:
        self._state["seq"] += 1
        return self._state["seq"]

    @staticmethod
    def _to_uuid(value: str) -> uuid.UUID | None:
        if not value:
            return None
        try:
            return uuid.UUID(str(value))
        except (ValueError, TypeError):
            return None

    def _emit(
        self,
        event_type: str,
        payload: dict[str, Any] | None = None,
        *,
        tool_name: str | None = None,
        round_num: int | None = None,
        latency_ms: int | None = None,
    ) -> None:
        """Persist one event to chat_audit_event via the async writer queue.

        Best-effort: any failure (writer not started, queue full) is logged
        and swallowed — the rotating log file is the parallel safety net.
        """
        try:
            chat_audit_writer.submit_event(
                run_id=self._state["run_id"],
                trace_id=self.trace_id,
                sequence=self._next_seq(),
                event_type=event_type,
                payload=payload,
                model_id=self.model_id or None,
                round_num=round_num,
                tool_name=tool_name,
                latency_ms=latency_ms,
            )
        except Exception:
            _logger.debug("audit submit_event failed", exc_info=True)

    # ── Request lifecycle ────────────────────────────────────────

    def log_request_start(
        self,
        content: str,
        models: list[str],
        mode: str,
        web_search: str,
        alphapai_enabled: bool,
        jinmen_enabled: bool,
        system_prompt_len: int,
        tools_count: int,
        tool_names: list[str],
        history_len: int,
    ):
        _logger.info(
            "%s | REQUEST_START | user=%s(%s) conv=%s\n"
            "  content: %s\n"
            "  models: %s | mode: %s\n"
            "  web_search: %s | alphapai: %s | jinmen: %s\n"
            "  tools(%d): %s\n"
            "  system_prompt_len: %d | history_messages: %d",
            self._prefix(), self.username, self.user_id, self.conversation_id,
            _truncate(content, 500),
            models, mode,
            web_search, alphapai_enabled, jinmen_enabled,
            tools_count, tool_names,
            system_prompt_len, history_len,
        )

        # First call wins: only the master trace ever invokes log_request_start
        # because chat.py drives it once, before the per-model fan-out.
        if not self._state["master_started"]:
            self._state["master_started"] = True
            try:
                chat_audit_writer.submit_run_start(
                    run_id=self._state["run_id"],
                    trace_id=self.trace_id,
                    user_id=self._to_uuid(self.user_id),
                    username=self.username,
                    conversation_id=self._to_uuid(self.conversation_id),
                    message_id=self._to_uuid(self.message_id),
                    user_content=content,
                    models_requested=list(models or []),
                    mode=mode or "standard",
                    web_search_mode=web_search or "off",
                    feature_flags={
                        "alphapai_enabled": bool(alphapai_enabled),
                        "jinmen_enabled": bool(jinmen_enabled),
                    },
                    system_prompt_len=int(system_prompt_len or 0),
                    history_messages=int(history_len or 0),
                    tools_offered=list(tool_names or []),
                )
            except Exception:
                _logger.debug("audit submit_run_start failed", exc_info=True)

        self._emit("REQUEST_START", {
            "content": content,
            "models": list(models or []),
            "mode": mode,
            "web_search": web_search,
            "alphapai_enabled": alphapai_enabled,
            "jinmen_enabled": jinmen_enabled,
            "system_prompt_len": system_prompt_len,
            "tools_count": tools_count,
            "tool_names": list(tool_names or []),
            "history_len": history_len,
        })

    def log_request_end(self, total_elapsed_ms: int):
        _logger.info(
            "%s | REQUEST_END | total_elapsed=%dms",
            self._prefix(), total_elapsed_ms,
        )
        self._emit("REQUEST_END", {"total_elapsed_ms": total_elapsed_ms})

        # Master trace owns finalize. Other in-flight sub-traces have already
        # contributed their events via the shared state dict.
        state = _pop_state(self.trace_id)
        if state is None:
            return
        try:
            chat_audit_writer.submit_run_finalize(
                run_id=state["run_id"],
                status="error" if state["any_error"] else "done",
                error_message=state["first_error"],
                rounds_used=int(state["rounds_used"]),
                tool_calls_total=int(state["tool_calls_total"]),
                tool_calls_by_name=dict(state["tool_calls_by_name"]),
                urls_searched=int(state["urls_searched"]),
                urls_read=int(state["urls_read"]),
                citations_count=int(state["citations_count"]),
                total_tokens=int(state["total_tokens"]),
                total_latency_ms=int(total_elapsed_ms or 0),
                final_content_len=int(state["final_content_len"]),
            )
        except Exception:
            _logger.debug("audit submit_run_finalize failed", exc_info=True)

    # ── LLM call lifecycle ───────────────────────────────────────

    def log_llm_request(
        self,
        messages: list[dict],
        tools: list[dict] | None = None,
        mode: str = "standard",
        round_num: int = 0,
    ):
        """Log the full payload being sent to the LLM."""
        self._tool_round = round_num
        tool_names = [t.get("function", {}).get("name", "?") for t in (tools or [])]

        # Log message roles and content summary
        msg_summary = []
        for m in messages:
            role = m.get("role", "?")
            content = m.get("content", "")
            if role == "system":
                msg_summary.append(f"  [{role}] len={len(str(content))}")
            elif role == "tool":
                tool_id = m.get("tool_call_id", "?")
                msg_summary.append(f"  [{role}] call_id={tool_id} result_len={len(str(content))}")
            elif role == "assistant":
                tc = m.get("tool_calls", [])
                if tc:
                    tc_names = [c.get("function", {}).get("name", "?") for c in tc]
                    msg_summary.append(f"  [{role}] tool_calls={tc_names} content_len={len(str(content or ''))}")
                else:
                    msg_summary.append(f"  [{role}] content_len={len(str(content or ''))}")
            else:
                msg_summary.append(f"  [{role}] content: {_truncate(str(content), 300)}")

        _logger.info(
            "%s | LLM_REQUEST | round=%d mode=%s tools=%s\n"
            "  messages(%d):\n%s",
            self._prefix(), round_num, mode, tool_names,
            len(messages), "\n".join(msg_summary),
        )

        if round_num > self._state["rounds_used"]:
            self._state["rounds_used"] = round_num

        self._emit("LLM_REQUEST", {
            "round_num": round_num,
            "mode": mode,
            "tools_offered": tool_names,
            "messages_count": len(messages),
            "messages_summary": [
                {
                    "role": m.get("role"),
                    "content_len": len(str(m.get("content", "") or "")),
                    "tool_call_id": m.get("tool_call_id"),
                    "has_tool_calls": bool(m.get("tool_calls")),
                }
                for m in messages
            ],
            "messages_full": messages,
        }, round_num=round_num)

    def log_llm_stream_start(self):
        _logger.debug(
            "%s | LLM_STREAM_START | round=%d",
            self._prefix(), self._tool_round,
        )

    def log_llm_error(self, error: str, round_num: int = 0):
        _logger.error(
            "%s | LLM_ERROR | round=%d error=%s",
            self._prefix(), round_num, error,
        )
        self._state["any_error"] = True
        if not self._state.get("first_error"):
            self._state["first_error"] = str(error)[:500]
        self._emit("LLM_ERROR", {"error": str(error)}, round_num=round_num)

    def log_llm_retry(self, attempt: int, reason: str):
        _logger.warning(
            "%s | LLM_RETRY | round=%d attempt=%d reason=%s",
            self._prefix(), self._tool_round, attempt, reason,
        )
        self._emit("LLM_RETRY", {"attempt": attempt, "reason": reason},
                   round_num=self._tool_round)

    def log_llm_done(
        self,
        content_len: int,
        tokens: int,
        latency_ms: int,
        finish_reason: str | None = None,
        error: str | None = None,
        round_num: int = 0,
    ):
        level = logging.ERROR if error else logging.INFO
        _logger.log(
            level,
            "%s | LLM_DONE | round=%d content_len=%d tokens=%d latency=%dms "
            "finish_reason=%s error=%s",
            self._prefix(), round_num, content_len, tokens, latency_ms,
            finish_reason, error,
        )
        if tokens:
            self._state["total_tokens"] += int(tokens)
        if error:
            self._state["any_error"] = True
            if not self._state.get("first_error"):
                self._state["first_error"] = str(error)[:500]
        if content_len:
            self._state["final_content_len"] = max(
                self._state["final_content_len"], int(content_len),
            )
        self._emit("LLM_DONE", {
            "content_len": content_len,
            "tokens": tokens,
            "finish_reason": finish_reason,
            "error": error,
        }, round_num=round_num, latency_ms=latency_ms)

    def log_llm_response_content(self, content: str, round_num: int = 0):
        """Log the actual LLM response content (truncated)."""
        _logger.debug(
            "%s | LLM_RESPONSE_CONTENT | round=%d\n%s",
            self._prefix(), round_num, _truncate(content, 1500),
        )
        self._emit("LLM_RESPONSE_CONTENT", {"content": content}, round_num=round_num)

    # ── Tool calling ─────────────────────────────────────────────

    def log_tool_calls_detected(self, tool_calls: list[dict], round_num: int = 0):
        """Log what tools the LLM decided to call."""
        summary = []
        normalised: list[dict[str, Any]] = []
        for tc in tool_calls:
            name = tc.get("name") or tc.get("function", {}).get("name", "?")
            args = tc.get("args") or tc.get("function", {}).get("arguments", "{}")
            if isinstance(args, str):
                args_display = _truncate(args, 500)
                try:
                    parsed_args = json.loads(args)
                except Exception:
                    parsed_args = {"_raw": args}
            else:
                args_display = _safe_json(args, 500)
                parsed_args = args
            tc_id = tc.get("id", "?")
            summary.append(f"  - {name}(id={tc_id}): {args_display}")
            normalised.append({"id": tc_id, "name": name, "arguments": parsed_args})

        _logger.info(
            "%s | TOOL_CALLS_DETECTED | round=%d count=%d\n%s",
            self._prefix(), round_num, len(tool_calls), "\n".join(summary),
        )

        self._emit("TOOL_CALLS_DETECTED", {
            "round_num": round_num,
            "count": len(tool_calls),
            "calls": normalised,
        }, round_num=round_num)

    def log_tool_exec_start(self, tool_name: str, arguments: dict):
        _logger.info(
            "%s | TOOL_EXEC_START | tool=%s args=%s",
            self._prefix(), tool_name, _safe_json(arguments, 800),
        )
        self._emit("TOOL_EXEC_START", {
            "tool_name": tool_name,
            "arguments": arguments,
        }, tool_name=tool_name)

    def log_tool_exec_done(self, tool_name: str, result: str, elapsed_ms: int, error: bool = False):
        level = logging.WARNING if error else logging.INFO
        _logger.log(
            level,
            "%s | TOOL_EXEC_DONE | tool=%s elapsed=%dms result_len=%d error=%s\n"
            "  result_preview: %s",
            self._prefix(), tool_name, elapsed_ms, len(result), error,
            _truncate(result, 1000),
        )
        # Count tool executions for run-level rollups.
        self._state["tool_calls_total"] += 1
        by_name = self._state["tool_calls_by_name"]
        by_name[tool_name] = by_name.get(tool_name, 0) + 1
        if error:
            self._state["any_error"] = True
            if not self._state.get("first_error"):
                self._state["first_error"] = f"{tool_name}: {_truncate(result, 200)}"
        self._emit("TOOL_EXEC_DONE", {
            "tool_name": tool_name,
            "result_len": len(result),
            "result": result,
            "error": error,
        }, tool_name=tool_name, latency_ms=elapsed_ms)

    def log_tool_timeout(self, tool_name: str, timeout_s: float):
        _logger.error(
            "%s | TOOL_TIMEOUT | tool=%s timeout=%.0fs",
            self._prefix(), tool_name, timeout_s,
        )
        self._state["any_error"] = True
        if not self._state.get("first_error"):
            self._state["first_error"] = f"{tool_name} timed out after {timeout_s:.0f}s"
        self._emit("TOOL_TIMEOUT", {
            "tool_name": tool_name,
            "timeout_s": timeout_s,
        }, tool_name=tool_name)

    # ── Gemini-specific ──────────────────────────────────────────

    def log_gemini_function_calls(self, function_calls: list[dict], round_num: int = 0):
        """Log Gemini native function call parts."""
        summary = []
        for fc in function_calls:
            summary.append(f"  - {fc.get('name', '?')}: {_safe_json(fc.get('args', {}), 500)}")
        _logger.info(
            "%s | GEMINI_FUNC_CALLS | round=%d count=%d\n%s",
            self._prefix(), round_num, len(function_calls), "\n".join(summary),
        )
        self._emit("GEMINI_FUNC_CALLS", {
            "round_num": round_num,
            "count": len(function_calls),
            "calls": function_calls,
        }, round_num=round_num)

    def log_gemini_grounding(self, has_grounding: bool, sources_count: int = 0):
        _logger.info(
            "%s | GEMINI_GROUNDING | has_grounding=%s sources=%d",
            self._prefix(), has_grounding, sources_count,
        )
        self._emit("GEMINI_GROUNDING", {
            "has_grounding": has_grounding,
            "sources_count": sources_count,
        })

    # ── SSE events ───────────────────────────────────────────────

    def log_sse_event(self, event_type: str, details: str = ""):
        _logger.debug(
            "%s | SSE_EVENT | type=%s %s",
            self._prefix(), event_type, details,
        )
        # SSE meta-events are too noisy / low-value for the audit timeline;
        # keep them in the log file only.

    # ── Messages payload inspection ──────────────────────────────

    def log_messages_payload(self, messages: list[dict]):
        """Log the full messages array sent to LLM (for deep debugging)."""
        _logger.debug(
            "%s | MESSAGES_PAYLOAD | count=%d\n%s",
            self._prefix(), len(messages), _safe_json(messages, 8000),
        )
        self._emit("MESSAGES_PAYLOAD", {
            "count": len(messages),
            "messages": messages,
        })

    # ── Web search deep logging ──────────────────────────────────

    def log_search_keywords(
        self,
        round_num: int,
        query_cn: str,
        query_en: str,
        search_type: str,
        recency: str,
        is_cn_stock: bool,
    ):
        """Highlight the search keywords the LLM chose for this round."""
        _logger.info(
            "%s | SEARCH_KEYWORDS | round=%d query_cn='%s' query_en='%s' "
            "type=%s recency=%s cn_stock=%s",
            self._prefix(), round_num,
            query_cn, query_en or "",
            search_type, recency, is_cn_stock,
        )
        self._emit("SEARCH_KEYWORDS", {
            "query_cn": query_cn,
            "query_en": query_en,
            "search_type": search_type,
            "recency": recency,
            "is_cn_stock": is_cn_stock,
        }, round_num=round_num, tool_name="web_search")

    def log_search_engine_call(
        self,
        engine: str,
        query: str,
        api_url: str,
        status: str,
        latency_ms: int,
        result_count: int,
        error: str = "",
    ):
        """Log a single search-engine API call: URL, status, timing, count."""
        _logger.info(
            "%s | SEARCH_ENGINE_CALL | engine=%s query='%s' api=%s "
            "status=%s latency=%dms results=%d error=%s",
            self._prefix(), engine, _truncate(query, 100), api_url,
            status, latency_ms, result_count, _truncate(error, 200) if error else "-",
        )
        self._emit("SEARCH_ENGINE_CALL", {
            "engine": engine,
            "query": query,
            "api_url": api_url,
            "status": status,
            "result_count": result_count,
            "error": error or "",
        }, tool_name="web_search", latency_ms=latency_ms)

    def log_search_urls_returned(
        self,
        engine: str,
        query: str,
        items: list[dict],
    ):
        """Log the URLs / titles / dates returned by a single search engine.

        items: list of dicts with title, url, website, date, score.
        """
        if not items:
            _logger.info(
                "%s | SEARCH_URLS_RETURNED | engine=%s query='%s' count=0",
                self._prefix(), engine, _truncate(query, 80),
            )
        else:
            lines = []
            for i, it in enumerate(items[:20], 1):
                title = _truncate(str(it.get("title", "")), 120)
                url = str(it.get("url", ""))
                website = str(it.get("website", ""))
                date = str(it.get("date", ""))
                score = it.get("score", 0)
                lines.append(f"  [{i}] {title} | {website} | {date} | score={score}\n      {url}")
            _logger.info(
                "%s | SEARCH_URLS_RETURNED | engine=%s query='%s' count=%d\n%s",
                self._prefix(), engine, _truncate(query, 80), len(items),
                "\n".join(lines),
            )
        self._state["urls_searched"] += len(items)
        self._emit("SEARCH_URLS_RETURNED", {
            "engine": engine,
            "query": query,
            "count": len(items),
            "items": items,
        }, tool_name="web_search")

    def log_search_top_results(
        self,
        round_num: int,
        results: list[dict],
    ):
        """Log the final top-N merged/ranked search results the LLM will actually read."""
        if not results:
            _logger.info(
                "%s | SEARCH_TOP_RESULTS | round=%d count=0",
                self._prefix(), round_num,
            )
        else:
            lines = []
            for i, r in enumerate(results, 1):
                idx = r.get("citation_index", i)
                title = _truncate(str(r.get("title", "")), 120)
                url = str(r.get("url", ""))
                website = str(r.get("website", ""))
                date = str(r.get("date", ""))
                source = str(r.get("source", ""))
                content_preview = _truncate(str(r.get("content", "")), 200)
                lines.append(
                    f"  [{idx}] {title} | {website} | {date} | via={source}\n"
                    f"      URL: {url}\n"
                    f"      preview: {content_preview}"
                )
            _logger.info(
                "%s | SEARCH_TOP_RESULTS | round=%d count=%d\n%s",
                self._prefix(), round_num, len(results), "\n".join(lines),
            )
        self._emit("SEARCH_TOP_RESULTS", {
            "round_num": round_num,
            "count": len(results),
            "results": results,
        }, round_num=round_num, tool_name="web_search")

    def log_search_cache_hit(self, cache_key: str, result_count: int):
        _logger.info(
            "%s | SEARCH_CACHE_HIT | key='%s' results=%d",
            self._prefix(), _truncate(cache_key, 100), result_count,
        )
        self._emit("SEARCH_CACHE_HIT", {
            "cache_key": cache_key,
            "result_count": result_count,
        }, tool_name="web_search")

    def log_webpage_read(
        self,
        url: str,
        status: str,
        latency_ms: int,
        content_len: int,
        content_preview: str = "",
        error: str = "",
    ):
        """Log a read_webpage / jina_reader call with URL, timing, content length and preview."""
        _logger.info(
            "%s | WEBPAGE_READ | url=%s status=%s latency=%dms content_len=%d error=%s\n"
            "  preview: %s",
            self._prefix(), url, status, latency_ms, content_len,
            _truncate(error, 200) if error else "-",
            _truncate(content_preview, 500),
        )
        self._state["urls_read"] += 1
        self._emit("WEBPAGE_READ", {
            "url": url,
            "status": status,
            "content_len": content_len,
            "content_preview": content_preview,
            "error": error or "",
        }, tool_name="read_webpage", latency_ms=latency_ms)

    # ── Inter-round reasoning (text emitted before tool calls) ──

    def log_model_reasoning(self, round_num: int, text: str):
        """Log any text the model emitted in this round (before or instead of tool calls).

        Useful for seeing the model's stated 'plan' before it picks tools.
        """
        if not text:
            return
        _logger.info(
            "%s | MODEL_REASONING | round=%d len=%d\n%s",
            self._prefix(), round_num, len(text),
            _truncate(text, 2000),
        )
        self._emit("MODEL_REASONING", {
            "round_num": round_num,
            "text": text,
        }, round_num=round_num)

    # ── AlphaPai / Jinmen result structure ─────────────────────

    def log_alphapai_results(
        self,
        query: str,
        recall_types: list,
        type_counts: dict,
        top_titles: list[str],
    ):
        """Log structured summary of an AlphaPai recall result."""
        _logger.info(
            "%s | ALPHAPAI_RESULTS | query='%s' types=%s counts=%s\n  top_titles:\n%s",
            self._prefix(), _truncate(query, 120), recall_types, type_counts,
            "\n".join(f"    - {_truncate(t, 200)}" for t in top_titles[:10]) or "    (none)",
        )
        self._emit("ALPHAPAI_RESULTS", {
            "query": query,
            "recall_types": list(recall_types or []),
            "type_counts": dict(type_counts or {}),
            "top_titles": list(top_titles or []),
        }, tool_name="alphapai_recall")

    def log_jinmen_results(
        self,
        tool_name: str,
        query: str,
        item_count: int,
        top_items: list[dict],
    ):
        """Log structured summary of a Jinmen MCP tool result."""
        lines = []
        for i, it in enumerate(top_items[:8], 1):
            title = _truncate(str(it.get("title", "")), 120)
            institution = str(it.get("institution", ""))
            date = str(it.get("date", ""))
            score = it.get("score", "")
            url = str(it.get("url", ""))
            lines.append(f"  [{i}] {title} | {institution} | {date} | score={score}\n      {url}")
        _logger.info(
            "%s | JINMEN_RESULTS | tool=%s query='%s' count=%d\n%s",
            self._prefix(), tool_name, _truncate(query, 120), item_count,
            "\n".join(lines) or "  (no items)",
        )
        self._emit("JINMEN_RESULTS", {
            "tool": tool_name,
            "query": query,
            "item_count": item_count,
            "top_items": top_items,
        }, tool_name=tool_name)

    def log_kb_request(
        self,
        query: str,
        tickers: list | None = None,
        doc_types: list | None = None,
        sources: list | None = None,
        date_range: dict | None = None,
        top_k: int = 0,
    ):
        """Log a KB tool invocation (query + full filter stack)."""
        _logger.info(
            "%s | KB_REQUEST | query='%s' tickers=%s doc_types=%s sources=%s "
            "date_range=%s top_k=%d",
            self._prefix(), _truncate(query, 200),
            tickers or [], doc_types or [], sources or [],
            date_range or {}, top_k,
        )
        self._emit("KB_REQUEST", {
            "query": query,
            "tickers": list(tickers or []),
            "doc_types": list(doc_types or []),
            "sources": list(sources or []),
            "date_range": dict(date_range or {}),
            "top_k": top_k,
        }, tool_name="kb_search")

    def log_user_kb_request(
        self,
        user_id: str,
        query: str,
        top_k: int = 0,
        document_ids: list[str] | None = None,
    ):
        """Log a user_kb_* tool invocation."""
        _logger.info(
            "%s | USER_KB_REQUEST | user=%s query='%s' top_k=%d doc_ids=%s",
            self._prefix(), user_id, _truncate(query, 200), top_k,
            document_ids or [],
        )
        self._emit("USER_KB_REQUEST", {
            "user_id": user_id,
            "query": query,
            "top_k": top_k,
            "document_ids": list(document_ids or []),
        }, tool_name="user_kb_search")

    def log_user_kb_results(
        self,
        query: str,
        result_count: int,
        top_titles: list[str],
    ):
        """Log user_kb search hit summary."""
        _logger.info(
            "%s | USER_KB_RESULTS | query='%s' count=%d\n  top_titles:\n%s",
            self._prefix(), _truncate(query, 120), result_count,
            "\n".join(f"    - {_truncate(t, 180)}" for t in top_titles[:10])
            or "    (none)",
        )
        self._emit("USER_KB_RESULTS", {
            "query": query,
            "result_count": result_count,
            "top_titles": list(top_titles or []),
        }, tool_name="user_kb_search")

    def log_kb_results(
        self,
        query: str,
        result_count: int,
        top_titles: list[str],
        sources: list[str] | None = None,
    ):
        """Log structured KB search results (titles + source distribution)."""
        src_counts: dict[str, int] = {}
        for s in (sources or []):
            if s:
                src_counts[s] = src_counts.get(s, 0) + 1
        _logger.info(
            "%s | KB_RESULTS | query='%s' count=%d src_dist=%s\n  top_titles:\n%s",
            self._prefix(), _truncate(query, 120), result_count, src_counts,
            "\n".join(f"    - {_truncate(t, 180)}" for t in top_titles[:10]) or "    (none)",
        )
        self._emit("KB_RESULTS", {
            "query": query,
            "result_count": result_count,
            "top_titles": list(top_titles or []),
            "src_distribution": src_counts,
        }, tool_name="kb_search")

    # ── Final summary at request end ─────────────────────────────

    def log_request_summary(
        self,
        rounds_used: int,
        tool_calls_total: int,
        tool_call_names: list[str],
        search_queries: list[str],
        urls_searched: list[str],
        urls_read: list[str],
        citations_count: int,
        final_content_len: int,
        total_tokens: int,
    ):
        """One-shot summary of everything that happened in this request."""
        _logger.info(
            "%s | REQUEST_SUMMARY |\n"
            "  rounds_used: %d\n"
            "  tool_calls: total=%d names=%s\n"
            "  search_queries(%d): %s\n"
            "  urls_found(%d): %s\n"
            "  urls_read(%d): %s\n"
            "  citations: %d\n"
            "  final_content_len: %d | total_tokens: %d",
            self._prefix(),
            rounds_used,
            tool_calls_total, tool_call_names,
            len(search_queries), [_truncate(q, 80) for q in search_queries[:20]],
            len(urls_searched), [_truncate(u, 120) for u in urls_searched[:30]],
            len(urls_read), [_truncate(u, 120) for u in urls_read[:15]],
            citations_count,
            final_content_len, total_tokens,
        )
        # Aggregate citation counts across models for the run-level rollup.
        self._state["citations_count"] += int(citations_count or 0)
        if final_content_len:
            self._state["final_content_len"] = max(
                self._state["final_content_len"], int(final_content_len),
            )
        self._emit("REQUEST_SUMMARY", {
            "rounds_used": rounds_used,
            "tool_calls_total": tool_calls_total,
            "tool_call_names": list(tool_call_names or []),
            "search_queries": list(search_queries or []),
            "urls_searched": list(urls_searched or []),
            "urls_read": list(urls_read or []),
            "citations_count": citations_count,
            "final_content_len": final_content_len,
            "total_tokens": total_tokens,
        })

    def log_full_response(self, content: str):
        """Log the complete LLM response content, with larger limit than preview."""
        _logger.info(
            "%s | LLM_FULL_RESPONSE | len=%d\n%s",
            self._prefix(), len(content), _truncate(content, 8000),
        )
        if content:
            self._state["final_content_len"] = max(
                self._state["final_content_len"], len(content),
            )
        self._emit("LLM_FULL_RESPONSE", {
            "content": content,
            "len": len(content or ""),
        })


def chat_trace(
    user_id: str = "",
    username: str = "",
    conversation_id: str = "",
    message_id: str = "",
    model_id: str = "",
    trace_id: str | None = None,
) -> ChatTrace:
    """Create a new ChatTrace for a request. Generates a trace_id if not provided.

    If model_id is omitted, falls back to the per-task model contextvar. This
    lets tool services (web_search_tool, kb_service, user_kb_service) build a
    trace from just the trace_id and still attribute their events to the model
    that triggered them — without threading model_id through every callsite.
    Conversely, when chat_llm passes model_id explicitly, this also seeds the
    contextvar for any nested chat_trace() in the same task.
    """
    tid = trace_id or uuid.uuid4().hex[:12]
    _current_trace_id.set(tid)
    if model_id:
        _current_model_id_var.set(model_id)
    else:
        model_id = _current_model_id_var.get()
    return ChatTrace(
        trace_id=tid,
        user_id=user_id,
        username=username,
        conversation_id=conversation_id,
        message_id=message_id,
        model_id=model_id,
    )


def get_current_trace_id() -> str:
    """Get the current trace_id from context (for use in tool services)."""
    return _current_trace_id.get()


def get_current_model_id() -> str:
    """Get the model_id of the current per-model scope, or "" at request level.

    Tool services rarely need this directly — chat_trace() picks it up
    automatically when model_id is not explicitly passed.
    """
    return _current_model_id_var.get()
