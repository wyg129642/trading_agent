"""Shared LLM/tool-call helpers for step executors.

These helpers wrap ``backend.app.services.chat_llm.call_model_stream_with_tools``
with conventions tailored to the modeling workflow:

  * Inject the Industry Pack's ``overview.md`` + path-prefix playbook snippets.
  * Wire the full tool registry (web_search, kb_search, alphapai_recall,
    jinmen_*, user_kb_search, consensus_forecast_query) so step executors
    actually call tools — not just hallucinate plausible citations.
  * Parse structured JSON output (schemas vary per step).
  * Convert chat-level citations into per-cell citation lists.

The helpers are the *only* place that knows about chat_llm. If we later
swap LLM SDKs, only this module changes.
"""
from __future__ import annotations

import json
import logging
import os
import re
import time
from typing import Any

logger = logging.getLogger(__name__)


# Regex to extract JSON blobs out of LLM prose (handles ```json fences + bare)
_JSON_BLOCK_RE = re.compile(r"```(?:json)?\s*(\{.*?\}|\[.*?\])\s*```", re.DOTALL)
_JSON_BARE_RE = re.compile(r"(?:\{.*?\}|\[.*?\])", re.DOTALL)


def parse_json_payload(text: str) -> Any:
    """Best-effort JSON extraction.

    Accepts:
      * pure JSON
      * ```json … ``` fenced
      * JSON embedded in prose (first matching {…} or […])

    Raises ValueError with helpful message on failure.
    """
    if not text:
        raise ValueError("empty text")
    text = text.strip()
    # direct parse
    try:
        return json.loads(text)
    except Exception:
        pass
    # fenced block
    m = _JSON_BLOCK_RE.search(text)
    if m:
        try:
            return json.loads(m.group(1))
        except Exception:
            pass
    # First balanced braces/brackets
    for m in _JSON_BARE_RE.finditer(text):
        try:
            return json.loads(m.group(0))
        except Exception:
            continue
    raise ValueError(f"Could not extract JSON from LLM output: {text[:400]}")


# ── System prompt construction ─────────────────────────────────

_BASE_SYSTEM_PROMPT = """You are a senior sell-side equity-research analyst operating a production revenue-modeling pipeline for a Chinese hedge fund.

You MUST obey these rules, which are not negotiable:

1. Every numeric assertion MUST be backed by at least one citation that was actually retrieved by the tools provided below. If a tool returned it, cite it with `[N]` inline. If no tool produced supporting evidence, mark the number confidence=LOW and set source_type=assumption in the JSON.

2. NEVER invent a citation. Never make up a title, URL, or snippet that was not in an actual tool response. If you did not call a tool for a value, say so and flag it as LOW confidence.

3. USE THE TOOLS. Your job is not to guess from your training data — the tools below let you read actual earnings-call transcripts, research reports, expert interview notes, and the web. Call them. Prefer `kb_search` (internal corpus, most reliable), then `alphapai_recall` / `jinmen_*` (Chinese broker research + roadshow notes), then `web_search` (last resort, for items not in the internal corpus).

4. When asked for structured output, produce ONLY valid JSON matching the requested schema. No commentary, no markdown fences outside the JSON.

5. Include citation markers `[N]` inside string fields of your JSON output where a number comes from a specific source, so the downstream parser can link cell → source.
"""


def build_system_prompt(pack, cell_path_hints: list[str] | None = None) -> str:
    """Compose the system prompt from the pack's overview + relevant playbook."""
    parts: list[str] = [_BASE_SYSTEM_PROMPT]
    if pack:
        parts += [
            "",
            "── Industry Overview ──",
            pack.overview_md() or "",
            "",
            "── Industry Rules ──",
            pack.rules_md() or "",
        ]
    if cell_path_hints and pack:
        snippets = []
        for h in cell_path_hints[:5]:
            s = pack.playbook_snippets(h, max_chars=600)
            if s:
                snippets.append(f"# Relevant to {h}\n\n{s}")
        if snippets:
            parts.append("")
            parts.append("── Relevant Lessons (from past research feedback) ──")
            parts.extend(snippets)
    return "\n".join(parts)


def format_template(template: str, variables: dict[str, Any]) -> str:
    """Apply ``{name}`` substitutions. Missing vars keep the placeholder literal.

    Rich templating (conditionals + loops) is handled in
    ``prompt_variables.render_prompt`` — this is the legacy simple path.
    """
    if not template:
        return ""

    def repl(m):
        key = m.group(1)
        if key in variables:
            v = variables[key]
            if isinstance(v, (list, dict)):
                return json.dumps(v, ensure_ascii=False)
            return str(v)
        return m.group(0)

    return re.sub(r"\{([A-Za-z_][A-Za-z0-9_]*)\}", repl, template)


async def render_step_prompt(ctx, template: str) -> str:
    """Build full variable context and render with conditionals/loops."""
    from .prompt_variables import build_variables, render_prompt
    vars_ = await build_variables(ctx)
    return render_prompt(template, vars_)


# ── Tool registry ─────────────────────────────────────────────

# Map symbolic names in recipe config → the concrete OpenAI-style tool dict
# plus a system-prompt addendum describing that tool. We build tools at
# call-time because each import pulls in heavy deps (httpx, Milvus, etc).

_DEFAULT_TOOL_SET = ("kb_search", "alphapai_recall", "jinmen_search", "web_search")


def _build_tools(tool_set: tuple[str, ...]) -> tuple[list[dict], str]:
    """Assemble OpenAI-compatible tool schemas + concatenated system prompt.

    Lazy-imports tool modules so unit tests that don't touch the LLM stack
    still work.
    """
    tools: list[dict] = []
    prompt_parts: list[str] = []
    seen: set[str] = set()

    def _add(new_tools: list[dict], prompt: str) -> None:
        for t in new_tools:
            fname = t.get("function", {}).get("name")
            if fname and fname not in seen:
                tools.append(t)
                seen.add(fname)
        if prompt and prompt not in prompt_parts:
            prompt_parts.append(prompt)

    for name in tool_set:
        try:
            if name in ("web_search", "read_webpage"):
                from backend.app.services.web_search_tool import (
                    WEB_SEARCH_TOOLS,
                    WEB_SEARCH_SYSTEM_PROMPT,
                )
                _add(WEB_SEARCH_TOOLS, WEB_SEARCH_SYSTEM_PROMPT)
            elif name in ("alphapai_recall", "alphapai"):
                from backend.app.services.alphapai_service import (
                    ALPHAPAI_TOOLS,
                    ALPHAPAI_SYSTEM_PROMPT,
                )
                _add(ALPHAPAI_TOOLS, ALPHAPAI_SYSTEM_PROMPT)
            elif name in ("jinmen_search", "jinmen"):
                from backend.app.services.jinmen_service import (
                    JINMEN_TOOLS,
                    JINMEN_SYSTEM_PROMPT,
                )
                _add(JINMEN_TOOLS, JINMEN_SYSTEM_PROMPT)
            elif name in ("kb_search", "kb"):
                from backend.app.services.kb_service import (
                    KB_TOOLS,
                    KB_SYSTEM_PROMPT,
                )
                _add(KB_TOOLS, KB_SYSTEM_PROMPT)
            elif name in ("user_kb_search", "user_kb"):
                from backend.app.services.user_kb_tools import (
                    USER_KB_TOOLS,
                    USER_KB_SYSTEM_PROMPT,
                )
                _add(USER_KB_TOOLS, USER_KB_SYSTEM_PROMPT)
            elif name == "consensus_forecast":
                t, p = _consensus_tool_schema()
                _add([t], p)
        except Exception:
            logger.exception("Failed to load tool set member %s", name)

    return tools, "\n\n".join(prompt_parts)


def _consensus_tool_schema() -> tuple[dict, str]:
    """Tool schema for Wind consensus lookup (A-share only)."""
    return (
        {
            "type": "function",
            "function": {
                "name": "consensus_forecast_query",
                "description": (
                    "查询 Wind 一致预期数据（A 股专用）。输入: ticker (例 '600519.SH' 或 '600519' + market 标签)。"
                    "返回: FY1/FY2/FY3 净利润/EPS/PE/营收 + 分析师数量 + 目标价 + 评级。"
                    "美股/港股 无返回, 请用 web_search 替代."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "ticker": {
                            "type": "string",
                            "description": "A 股代码，可以是 '600519' 或 '600519.SH'",
                        },
                        "market": {
                            "type": "string",
                            "enum": ["主板", "创业板", "科创板", "北交所"],
                            "description": "股票所在交易所板块（默认主板）",
                        },
                    },
                    "required": ["ticker"],
                },
            },
        },
        "\n\n## consensus_forecast_query\n"
        "当你建模 A 股公司的 FY1/FY2/FY3 盈利预测时, 必须先调用此工具查看 Wind 一致预期, "
        "并在你的预测显著偏离（>25%）时在 confidence_reason 里解释理由. 美股/港股不适用.",
    )


# ── Citation extraction ───────────────────────────────────────

def _collect_citation_tracker_sources(citation_tracker) -> list[dict]:
    """Return the list of citation dicts the tracker accumulated."""
    try:
        return list(getattr(citation_tracker, "_sources", []) or [])
    except Exception:
        return []


# ── LLM dispatch wrapper ─────────────────────────────────────────

_CHAT_LLM = None


def _load_llm():
    global _CHAT_LLM
    if _CHAT_LLM is None:
        from backend.app.services import chat_llm  # noqa
        _CHAT_LLM = chat_llm
    return _CHAT_LLM


class LLMStepError(Exception):
    """Raised when the LLM call fails in a way that should surface to the user
    (instead of silently falling back to dry-run stub values)."""


async def call_llm_for_json(
    ctx,
    *,
    user_prompt: str,
    path_hints: list[str] | None = None,
    model_id: str | None = None,
    temperature: float = 0.1,
    max_tool_rounds: int = 5,
    tool_set: tuple[str, ...] | list[str] | None = None,
) -> tuple[Any, list[dict[str, Any]], list[dict[str, Any]]]:
    """Call LLM with real tool use, expecting a JSON payload.

    Returns ``(parsed_json, citations, trace_steps)``.

    * ``citations`` — list of source dicts collected by ``CitationTracker``
      during tool execution. These are *real* — not hallucinated.
    * ``trace_steps`` — per-tool / per-round records suitable for
      persisting to ``ProvenanceTrace.steps``.

    In ``ctx.dry_run`` mode, returns a deterministic minimal stub so the
    engine runs end-to-end without network. Dry-run results are tagged so
    callers can mark cells as dry-run.
    """
    # Merge step-config tools with any defaults
    cfg_tools = (ctx.step_config or {}).get("tools")
    effective_tools = tuple(tool_set or cfg_tools or _DEFAULT_TOOL_SET)

    # Allow per-step model override from recipe config
    if not model_id:
        model_id = (ctx.step_config or {}).get("model_id") or "anthropic/claude-opus-4-7"

    if ctx.dry_run:
        stub = _dry_run_response(user_prompt)
        return stub, [], [{
            "step_type": "dry_run",
            "query_preview": user_prompt[:200],
            "model_id": model_id,
            "tokens": 0,
            "latency": 0,
            "dry_run": True,
        }]

    t0 = time.time()
    try:
        llm = _load_llm()
    except Exception as e:
        logger.exception("Cannot load chat_llm module")
        raise LLMStepError(f"chat_llm unavailable: {e}") from e

    # Build tools + enriched system prompt
    tools_schema, tools_prompt = _build_tools(effective_tools)
    system_prompt = build_system_prompt(ctx.pack, path_hints)
    if tools_prompt:
        system_prompt = system_prompt + "\n\n── Tool Usage Guide ──\n" + tools_prompt

    # Build citation tracker + shared tool dispatcher inline (so we get the
    # exact same CitationTracker semantics as the user chat path).
    from backend.app.services.web_search_tool import (
        CitationTracker,
        execute_tool as web_search_execute,
    )
    from backend.app.services.alphapai_service import execute_tool as alphapai_execute
    from backend.app.services.jinmen_service import execute_tool as jinmen_execute
    from backend.app.services.kb_service import execute_tool as kb_execute
    try:
        from backend.app.services.user_kb_tools import execute_tool as user_kb_execute
    except Exception:
        user_kb_execute = None

    citation_tracker = CitationTracker()
    trace_steps: list[dict[str, Any]] = []
    tokens_used = 0
    finish_reason: str | None = None

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]

    # Drive the existing streaming tool-calling loop but consume the stream
    # in-process rather than forwarding to SSE — we just want the final text
    # plus a citation trail.
    full_text = ""
    sources_from_stream: list[dict] = []
    search_queries: list[str] = []
    urls_read: list[str] = []
    tool_calls_log: list[dict] = []
    try:
        async for chunk in llm.call_model_stream_with_tools(
            model_id=model_id,
            messages=messages,
            mode="thinking" if temperature <= 0.2 else "standard",
            tools=tools_schema,
            max_tool_rounds=max_tool_rounds,
        ):
            if chunk.get("error"):
                raise LLMStepError(chunk["error"])
            if chunk.get("type") == "tool_status":
                tool_calls_log.append({
                    "tool": chunk.get("tool_name"),
                    "status": chunk.get("status"),
                })
                trace_steps.append({
                    "step_type": "tool_status",
                    "tool": chunk.get("tool_name"),
                    "status": chunk.get("status"),
                    "latency": 0,
                    "tokens": 0,
                })
                continue
            if chunk.get("type") == "search_status":
                q = chunk.get("query") or ""
                if q and q not in search_queries:
                    search_queries.append(q)
                continue
            if chunk.get("type") == "read_status":
                u = chunk.get("url") or ""
                if u and u not in urls_read:
                    urls_read.append(u)
                continue
            if chunk.get("type") == "heartbeat":
                continue
            if chunk.get("type") == "sources":
                # Final citation dump from the internal citation tracker —
                # THIS is how we get *real* citations rather than LLM-invented ones.
                new_sources = list(chunk.get("sources") or [])
                if new_sources:
                    sources_from_stream = new_sources
                continue
            delta = chunk.get("delta")
            if delta:
                full_text += delta
            if chunk.get("done"):
                tokens_used = int(chunk.get("tokens", 0) or 0)
                finish_reason = chunk.get("finish_reason")
                if chunk.get("content"):
                    # Prefer the content field — it's the full buffered text
                    # when the stream provides it.
                    if len(chunk["content"]) > len(full_text):
                        full_text = chunk["content"]
    except LLMStepError:
        raise
    except Exception as e:
        logger.exception("LLM streaming failure in call_llm_for_json")
        raise LLMStepError(f"LLM stream failed: {e}") from e

    latency_ms = int((time.time() - t0) * 1000)

    # REAL citations come from the stream's `sources` event (CitationTracker
    # inside call_model_stream_with_tools). These are keyed to the [N]
    # markers the model inserted in its output. These are *not* invented —
    # they were produced by actual tool calls.
    citations_out: list[dict[str, Any]] = [
        {
            "index": s.get("index"),
            "title": s.get("title") or "",
            "url": s.get("url") or "",
            "snippet": s.get("content") or s.get("snippet") or "",
            "date": s.get("date") or "",
            "tool": s.get("source_type") or "",
            "source_type": s.get("source_type") or "",
            "doc_type": s.get("doc_type") or "",
            "website": s.get("website") or "",
        }
        for s in sources_from_stream
    ]

    # Attempt to parse JSON
    parsed: Any = None
    parse_err: str | None = None
    try:
        parsed = parse_json_payload(full_text)
    except Exception as e:
        parse_err = str(e)
        logger.warning("LLM JSON parse failed (model=%s): %s | head=%s",
                       model_id, e, (full_text or "")[:400])

    trace_steps.append({
        "step_type": "llm_call",
        "model": model_id,
        "tool_set": list(effective_tools),
        "query_preview": user_prompt[:500],
        "response_preview": (full_text or "")[:500],
        "tokens": tokens_used,
        "latency": latency_ms,
        "finish_reason": finish_reason,
        "parse_error": parse_err,
        "has_tools": bool(tools_schema),
        "search_queries": search_queries,
        "urls_read": urls_read,
        "tool_calls": tool_calls_log,
        "citation_count": len(citations_out),
    })
    return parsed, citations_out, trace_steps


# ── Dry-run stub (used when ctx.dry_run=True) ───────────────────

def _dry_run_response(prompt: str) -> Any:
    """Synthetic response used when ``ctx.dry_run`` is True.

    Detects step intent from the most-distinctive keyword *first* and
    returns a plausible structure. Keep this ordered from most-specific to
    least-specific.
    """
    p = prompt.lower()
    # Margin cascade — most distinctive ("operating_margin" + "shares")
    if "operating_margin" in p or ("margin" in p and "tax_rate" in p):
        return {
            "operating_margin": {"FY25E": 0.18, "FY26E": 0.33, "FY27E": 0.40},
            "tax_rate": 0.15,
            "shares": {"FY25E": 0.835, "FY26E": 0.92, "FY27E": 0.95},
            "source": "guidance",
            "confidence": "MEDIUM",
        }
    # Verifier pass
    if "independently derive" in p or "you are the verifier" in p:
        return {
            "value": 0.0,
            "reasoning": "Verifier could not independently verify; defaulted.",
            "sources": [],
            "confidence": "LOW",
        }
    # Tiebreaker
    if "tiebreaker" in p:
        return {
            "value": 0.0, "reasoning": "Tiebreaker defaulted to zero in dry-run",
            "confidence": "LOW", "favors": "neither",
        }
    # Volume × price model step
    if "volume/price" in p or ("volume" in p and "asp" in p and ("expert" in p or "quote" in p)):
        return {
            "volume": {"FY25E": 80, "FY26E": 200, "FY27E": 350, "unit": "万块"},
            "asp":    {"FY25E": 450, "FY26E": 400, "FY27E": 350, "unit": "美元"},
            "confidence": "MEDIUM",
            "sources": [
                {"label": "管理层业绩会指引", "snippet": "800G 26 年出货 200-250 万块"},
                {"label": "专家访谈 2026-03", "snippet": "ASP 26 年降至 400 美元"},
            ],
        }
    # Apply-guidance step — "growth rate" + "segment" co-mention
    if "growth_rate" in p or ("growth rate" in p and "default" in p):
        return {
            "growth_rate": 0.03,
            "confidence": "MEDIUM",
            "source": "management_outlook",
            "notes": "Dry-run default 3% growth for stable segment",
        }
    # Classify growth profile — distinctive "growth profile" phrase
    if "growth profile" in p or "classify each segment" in p:
        return {
            "classifications": [
                {"segment": "module_400g", "profile": "stable"},
                {"segment": "module_800g", "profile": "high_growth"},
                {"segment": "module_1_6t", "profile": "new"},
                {"segment": "chip_eml_cw", "profile": "high_growth"},
                {"segment": "legacy_comms", "profile": "stable"},
                {"segment": "industrial", "profile": "stable"},
                {"segment": "ocs", "profile": "new"},
                {"segment": "cpo", "profile": "new"},
                {"segment": "dci", "profile": "high_growth"},
            ]
        }
    # Extract historical
    if "historical segment revenue" in p or "extract historical" in p:
        return {
            "historical": [
                {"segment": "module_400g", "period": "FY24", "rev": 0.3},
                {"segment": "module_800g", "period": "FY24", "rev": 0.8},
                {"segment": "module_1_6t", "period": "FY24", "rev": 0.0},
                {"segment": "chip_eml_cw", "period": "FY24", "rev": 5.2},
                {"segment": "legacy_comms", "period": "FY24", "rev": 0.7},
                {"segment": "industrial", "period": "FY24", "rev": 0.25},
                {"segment": "ocs", "period": "FY24", "rev": 0.05},
                {"segment": "cpo", "period": "FY24", "rev": 0.0},
                {"segment": "dci", "period": "FY24", "rev": 1.7},
            ]
        }
    # Decompose segments — specific "disclosed business segments" phrase
    if "disclosed business segments" in p or "decompose" in p:
        return {
            "segments": [
                {"slug": "module_400g", "label_zh": "400G光模块", "kind": "module",
                 "volume_unit": "万块", "asp_unit": "美元", "present_in_company": True},
                {"slug": "module_800g", "label_zh": "800G光模块", "kind": "module",
                 "volume_unit": "万块", "asp_unit": "美元", "present_in_company": True},
                {"slug": "module_1_6t", "label_zh": "1.6T光模块", "kind": "module",
                 "volume_unit": "万块", "asp_unit": "美元", "present_in_company": True},
                {"slug": "chip_eml_cw", "label_zh": "EML+CW光芯片", "kind": "chip",
                 "volume_unit": "万颗", "asp_unit": "美元", "present_in_company": True},
                {"slug": "ocs", "label_zh": "OCS", "kind": "product",
                 "revenue_directly": True, "present_in_company": True},
                {"slug": "cpo", "label_zh": "CPO", "kind": "product",
                 "revenue_directly": True, "present_in_company": True},
                {"slug": "dci", "label_zh": "DCI", "kind": "product",
                 "revenue_directly": True, "present_in_company": True},
                {"slug": "legacy_comms", "label_zh": "传统通信", "kind": "product",
                 "revenue_directly": True, "present_in_company": True},
                {"slug": "industrial", "label_zh": "工业", "kind": "product",
                 "revenue_directly": True, "present_in_company": True},
            ]
        }
    # Gather context — "earnings calls" + "10-K"
    if "earnings calls" in p and "10-k" in p:
        return {
            "summary": "Dry-run context: optical-modules maker with strong AI datacenter exposure.",
            "key_segments": [
                {"name": "Module 800G", "note": "Strong AI demand"},
                {"name": "EML+CW chips", "note": "High-margin product line"},
            ],
            "management_guidance": [],
        }
    # Growth decomposition
    if "growth_decomp" in p or "volume vs price" in p:
        return {
            "decomposition": {
                "FY26E": {"volume": 0.60, "price": 0.30, "mix": 0.10},
                "FY27E": {"volume": 0.75, "price": 0.15, "mix": 0.10},
            }
        }
    # Peer classification
    if "peer" in p and "margin" in p:
        return {
            "peers": [
                {"ticker": "COHR.US", "operating_margin": 0.22},
                {"ticker": "CIEN.US", "operating_margin": 0.16},
            ],
            "median_operating_margin": 0.19,
            "justification": "Dry-run default peer median",
        }
    # Multi-path cross-check
    if "four paths" in p or "cross-verification" in p or "multi-path" in p:
        return {
            "paths": [
                {"name": "volume_asp", "value": 80.0},
                {"name": "management_guidance", "value": 75.0},
                {"name": "peer_share_inferred", "value": 85.0},
                {"name": "tam_share", "value": 82.0},
            ],
            "aggregate": 80.5,
            "confidence": "MEDIUM",
        }
    return {}
