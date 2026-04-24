"""Dedicated debug logging for the AI Chat pipeline.

Writes structured, high-detail logs to a separate file (logs/chat_debug.log)
so that LLM interactions, tool calls, and the full request lifecycle can be
diagnosed without wading through general application logs.

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

# ── Logger setup ─────────────────────────────────────────────────

CHAT_DEBUG_LOGGER_NAME = "chat_debug"
_logger = logging.getLogger(CHAT_DEBUG_LOGGER_NAME)

# Context var for propagating trace_id across async call boundaries
_current_trace_id: ContextVar[str] = ContextVar("chat_trace_id", default="no-trace")


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


class ChatTrace:
    """Scoped trace for one chat request (one user message → N model responses)."""

    def __init__(
        self,
        trace_id: str,
        user_id: str = "",
        username: str = "",
        conversation_id: str = "",
        model_id: str = "",
    ):
        self.trace_id = trace_id
        self.user_id = user_id
        self.username = username
        self.conversation_id = conversation_id
        self.model_id = model_id
        self._start = time.monotonic()
        self._tool_round = 0

    def _prefix(self) -> str:
        parts = [f"trace={self.trace_id}"]
        if self.model_id:
            parts.append(f"model={self.model_id}")
        return " | ".join(parts)

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

    def log_request_end(self, total_elapsed_ms: int):
        _logger.info(
            "%s | REQUEST_END | total_elapsed=%dms",
            self._prefix(), total_elapsed_ms,
        )

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

    def log_llm_retry(self, attempt: int, reason: str):
        _logger.warning(
            "%s | LLM_RETRY | round=%d attempt=%d reason=%s",
            self._prefix(), self._tool_round, attempt, reason,
        )

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

    def log_llm_response_content(self, content: str, round_num: int = 0):
        """Log the actual LLM response content (truncated)."""
        _logger.debug(
            "%s | LLM_RESPONSE_CONTENT | round=%d\n%s",
            self._prefix(), round_num, _truncate(content, 1500),
        )

    # ── Tool calling ─────────────────────────────────────────────

    def log_tool_calls_detected(self, tool_calls: list[dict], round_num: int = 0):
        """Log what tools the LLM decided to call."""
        summary = []
        for tc in tool_calls:
            name = tc.get("name") or tc.get("function", {}).get("name", "?")
            args = tc.get("args") or tc.get("function", {}).get("arguments", "{}")
            if isinstance(args, str):
                args_display = _truncate(args, 500)
            else:
                args_display = _safe_json(args, 500)
            tc_id = tc.get("id", "?")
            summary.append(f"  - {name}(id={tc_id}): {args_display}")

        _logger.info(
            "%s | TOOL_CALLS_DETECTED | round=%d count=%d\n%s",
            self._prefix(), round_num, len(tool_calls), "\n".join(summary),
        )

    def log_tool_exec_start(self, tool_name: str, arguments: dict):
        _logger.info(
            "%s | TOOL_EXEC_START | tool=%s args=%s",
            self._prefix(), tool_name, _safe_json(arguments, 800),
        )

    def log_tool_exec_done(self, tool_name: str, result: str, elapsed_ms: int, error: bool = False):
        level = logging.WARNING if error else logging.INFO
        _logger.log(
            level,
            "%s | TOOL_EXEC_DONE | tool=%s elapsed=%dms result_len=%d error=%s\n"
            "  result_preview: %s",
            self._prefix(), tool_name, elapsed_ms, len(result), error,
            _truncate(result, 1000),
        )

    def log_tool_timeout(self, tool_name: str, timeout_s: float):
        _logger.error(
            "%s | TOOL_TIMEOUT | tool=%s timeout=%.0fs",
            self._prefix(), tool_name, timeout_s,
        )

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

    def log_gemini_grounding(self, has_grounding: bool, sources_count: int = 0):
        _logger.info(
            "%s | GEMINI_GROUNDING | has_grounding=%s sources=%d",
            self._prefix(), has_grounding, sources_count,
        )

    # ── SSE events ───────────────────────────────────────────────

    def log_sse_event(self, event_type: str, details: str = ""):
        _logger.debug(
            "%s | SSE_EVENT | type=%s %s",
            self._prefix(), event_type, details,
        )

    # ── Messages payload inspection ──────────────────────────────

    def log_messages_payload(self, messages: list[dict]):
        """Log the full messages array sent to LLM (for deep debugging)."""
        _logger.debug(
            "%s | MESSAGES_PAYLOAD | count=%d\n%s",
            self._prefix(), len(messages), _safe_json(messages, 8000),
        )

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
            return
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
            return
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

    def log_search_cache_hit(self, cache_key: str, result_count: int):
        _logger.info(
            "%s | SEARCH_CACHE_HIT | key='%s' results=%d",
            self._prefix(), _truncate(cache_key, 100), result_count,
        )

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

    def log_full_response(self, content: str):
        """Log the complete LLM response content, with larger limit than preview."""
        _logger.info(
            "%s | LLM_FULL_RESPONSE | len=%d\n%s",
            self._prefix(), len(content), _truncate(content, 8000),
        )


def chat_trace(
    user_id: str = "",
    username: str = "",
    conversation_id: str = "",
    model_id: str = "",
    trace_id: str | None = None,
) -> ChatTrace:
    """Create a new ChatTrace for a request. Generates a trace_id if not provided."""
    tid = trace_id or uuid.uuid4().hex[:12]
    _current_trace_id.set(tid)
    return ChatTrace(
        trace_id=tid,
        user_id=user_id,
        username=username,
        conversation_id=conversation_id,
        model_id=model_id,
    )


def get_current_trace_id() -> str:
    """Get the current trace_id from context (for use in tool services)."""
    return _current_trace_id.get()
