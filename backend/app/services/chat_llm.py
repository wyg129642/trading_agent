"""Multi-provider LLM service for chat with streaming support.

GPT → OpenAI native API; Claude → OpenRouter; Gemini → Google native API (with grounding).
"""
from __future__ import annotations

import asyncio
import base64
import json
import logging
import time
from pathlib import Path
from typing import AsyncIterator

import httpx
from google import genai
from google.genai import types as genai_types

from backend.app.config import get_settings

logger = logging.getLogger(__name__)


_ITER_SENTINEL = object()


async def _sync_iter_to_async(sync_iterable):
    """Convert a synchronous iterable to async, running each step in a thread.

    Prevents synchronous iterators (e.g. Google GenAI streaming) from blocking
    the asyncio event loop, which would starve other concurrent model streams.

    Uses a sentinel instead of catching StopIteration because StopIteration
    cannot be raised into a Future (Python 3.7+, PEP 479) — doing so causes
    'TypeError: StopIteration interacts badly with generators'.
    """
    it = iter(sync_iterable)
    loop = asyncio.get_running_loop()

    def _next():
        try:
            return next(it)
        except StopIteration:
            return _ITER_SENTINEL

    while True:
        item = await loop.run_in_executor(None, _next)
        if item is _ITER_SENTINEL:
            break
        yield item

# ── Available models ──────────────────────────────────────────

AVAILABLE_MODELS = [
    {
        "id": "anthropic/claude-opus-4-6",
        "name": "Claude Opus 4.6",
        "provider": "Anthropic",
        "supports_vision": True,
        "supports_thinking": True,
        "description": "Anthropic旗舰模型，超长上下文，深度分析与推理",
    },
    {
        "id": "google/gemini-3.1-pro-preview",
        "name": "Gemini 3.1 Pro",
        "provider": "Google",
        "supports_vision": True,
        "supports_thinking": True,
        "description": "Google最新旗舰，强大的多模态与推理能力，内置Google搜索",
    },
    {
        "id": "openai/gpt-5.4",
        "name": "GPT-5.4",
        "provider": "OpenAI",
        "supports_vision": True,
        "supports_thinking": True,
        "description": "OpenAI最强旗舰模型，顶尖推理与分析能力",
    },
]

# Model ID used in Google native API (without provider prefix)
GEMINI_NATIVE_MODEL = "gemini-3.1-pro-preview"

# ── Mode configurations ─────────────────────────────────────────

MODE_CONFIGS = {
    "standard": {
        "temperature": 0.7,
        "label": "标准模式",
    },
    "thinking": {
        "temperature": 0.3,
        "label": "深度思考",
    },
    "fast": {
        "temperature": 0.5,
        "label": "快速模式",
    },
}

MODEL_MAP = {m["id"]: m for m in AVAILABLE_MODELS}

OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"
OPENAI_BASE_URL = "https://api.openai.com/v1"

# Retry config for transient API failures
_RETRYABLE_STATUS = {429, 500, 502, 503, 504}
_MAX_RETRIES = 2
_RETRY_BACKOFF = [2.0, 5.0]

# Tool execution limits
_TOOL_EXEC_TIMEOUT = 120.0   # Per-tool execution timeout (seconds)
_OVERALL_TOOL_TIMEOUT = 300.0  # Max time for entire multi-round tool calling (seconds)
_GEMINI_ROUND_TIMEOUT = 180.0  # Per-round timeout for Gemini generate_content (seconds)


def _parse_api_error(status_code: int, raw_text: str) -> str:
    """Parse API error response and extract clean error message.

    API providers return JSON like {"error": {"message": "...", "type": "..."}}
    — show the human-readable message instead of raw JSON.
    """
    try:
        data = json.loads(raw_text)
        if isinstance(data, dict):
            err = data.get("error", {})
            if isinstance(err, dict):
                msg = err.get("message", "")
                if msg:
                    return f"API错误 ({status_code}): {msg[:300]}"
            elif isinstance(err, str):
                return f"API错误 ({status_code}): {err[:300]}"
    except (json.JSONDecodeError, KeyError, TypeError):
        pass
    # Fallback: truncated raw text
    return f"API错误 ({status_code}): {raw_text[:200]}"


_proxy_cache: str | None = ""  # sentinel "" = not yet resolved


def _http_proxy() -> str | None:
    """Get proxy URL from environment. Cached after first lookup."""
    global _proxy_cache
    if _proxy_cache == "":
        import os
        _proxy_cache = os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY") or None
        logger.info("HTTP proxy configured: %s", _proxy_cache)
    return _proxy_cache


def _proxy_for_api(model_id: str) -> str | None:
    """Return proxy for the given model's API endpoint.

    Connectivity test results (from China):
      - api.openai.com      → ConnectTimeout without proxy → needs proxy
      - openrouter.ai       → HTTP 200 without proxy      → direct OK
      - generativelanguage.googleapis.com → needs proxy (handled by genai SDK env vars)
      - Domestic APIs (Baidu, AlphaPai, Jinmen) → direct OK

    Using proxy only when necessary avoids the extra latency of routing
    through a proxy for APIs that are directly reachable.
    """
    if _is_openai(model_id):
        return _http_proxy()  # OpenAI blocked in China
    # OpenRouter is reachable directly — no proxy for better latency
    return None


def _is_gemini(model_id: str) -> bool:
    """Check if the model should be routed to Google native API."""
    return model_id.startswith("google/")


def _is_openai(model_id: str) -> bool:
    """Check if the model should be routed to OpenAI native API."""
    return model_id.startswith("openai/")


def get_api_key() -> str:
    """Get OpenRouter API key."""
    settings = get_settings()
    return settings.openrouter_api_key


def _get_openai_key() -> str:
    """Get OpenAI native API key."""
    return get_settings().openai_api_key


_gemini_client: genai.Client | None = None
_gemini_client_key: str | None = None


def _get_gemini_client() -> genai.Client:
    """Get or create cached Gemini client with explicit proxy for googleapis.com.

    The genai SDK uses httpx internally. Rather than relying on env vars
    (which affect ALL httpx clients in the process), we pass proxy
    explicitly via http_options so only Gemini traffic is proxied.
    """
    global _gemini_client, _gemini_client_key
    settings = get_settings()
    key = settings.gemini_api_key
    proxy = _http_proxy()
    if _gemini_client is None or _gemini_client_key != key:
        http_options = None
        if proxy:
            # Pass proxy to the SDK's internal httpx clients
            http_options = genai_types.HttpOptions(
                client_args={"proxy": proxy},
                async_client_args={"proxy": proxy},
            )
        _gemini_client = genai.Client(api_key=key, http_options=http_options)
        _gemini_client_key = key
    return _gemini_client


def _api_config(model_id: str) -> tuple[str, dict]:
    """Return (base_url, headers) for the given model.

    OpenAI models → OpenAI native API; others → OpenRouter.
    """
    if _is_openai(model_id):
        return OPENAI_BASE_URL, {
            "Authorization": f"Bearer {_get_openai_key()}",
            "Content-Type": "application/json",
        }
    return OPENROUTER_BASE_URL, {
        "Authorization": f"Bearer {get_api_key()}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://trading-intelligence.com",
        "X-Title": "Trading Intelligence Chat",
    }


def _build_messages(
    history: list[dict],
    user_content: str | list,
    system_prompt: str | None = None,
    search_context: str | None = None,
) -> list[dict]:
    """Build OpenAI-format message list from conversation history."""
    messages = []

    # System prompt (user-provided or default)
    sys_parts = []
    if system_prompt:
        sys_parts.append(system_prompt)
    if search_context:
        sys_parts.append(
            "以下是针对用户问题的实时网络搜索结果，请基于这些信息回答。"
            "引用信息时请注明来源。如果搜索结果不足以回答问题，请如实说明。\n\n"
            f"{search_context}"
        )
    if sys_parts:
        messages.append({"role": "system", "content": "\n\n".join(sys_parts)})

    # Add conversation history
    for msg in history:
        messages.append({"role": msg["role"], "content": msg["content"]})

    # Add current user message
    messages.append({"role": "user", "content": user_content})
    return messages


async def search_for_chat(query: str) -> str | None:
    """Run web search and return formatted context for the chat LLM.

    Uses Baidu (Chinese) + Tavily/Jina (English) in parallel.
    Returns None if no results or search is not needed.
    """
    settings = get_settings()
    baidu_key = settings.baidu_api_key
    tavily_key = settings.tavily_api_key
    jina_key = settings.jina_api_key

    if not baidu_key and not tavily_key and not jina_key:
        return None

    try:
        from src.tools.web_search import multi_search, format_search_results

        results = await multi_search(
            query,
            baidu_api_key=baidu_key,
            tavily_api_key=tavily_key,
            jina_api_key=jina_key,
            use_english_search=bool(tavily_key or jina_key),
            max_results=8,
        )
        if not results:
            return None
        return format_search_results(results, max_per_result=600)
    except Exception:
        logger.exception("Chat web search failed for: %s", query[:80])
        return None


def build_multimodal_content(text: str, attachments: list[dict]) -> str | list:
    """Build multimodal content with text, images, and native PDF file blocks.

    Modern LLMs (Claude, GPT, Gemini) natively understand PDF files —
    sending the raw PDF preserves layout, tables, charts, and figures
    that plain-text extraction would lose.
    """
    if not attachments:
        return text

    content_parts: list[dict] = []

    for att in attachments:
        file_type = att.get("file_type", "")
        file_path = att.get("file_path", "")

        if file_type.startswith("image/"):
            # Image → base64 image_url block
            if file_path and Path(file_path).exists():
                with open(file_path, "rb") as f:
                    b64 = base64.b64encode(f.read()).decode()
                content_parts.append({
                    "type": "image_url",
                    "image_url": {"url": f"data:{file_type};base64,{b64}"},
                })

        elif file_type == "application/pdf":
            # PDF → native file block (OpenRouter forwards to each provider)
            # This preserves tables, charts, formatting that text extraction loses
            if file_path and Path(file_path).exists():
                with open(file_path, "rb") as f:
                    b64 = base64.b64encode(f.read()).decode()
                fname = att.get("filename", "document.pdf")
                content_parts.append({
                    "type": "file",
                    "file": {
                        "filename": fname,
                        "file_data": f"data:application/pdf;base64,{b64}",
                    },
                })

    # No binary attachments → plain text
    if not content_parts:
        return text

    # Multimodal: text + file blocks
    content_parts.insert(0, {"type": "text", "text": text})
    return content_parts


# ── Gemini native API helpers ─────────────────────────────────

def _build_gemini_contents(messages: list[dict]) -> tuple[str | None, list[genai_types.Content]]:
    """Convert OpenAI-format messages to Gemini Contents.

    Returns (system_instruction, contents).
    """
    system_instruction = None
    contents: list[genai_types.Content] = []

    for msg in messages:
        role = msg["role"]
        raw = msg.get("content", "")

        if role == "system":
            system_instruction = raw if isinstance(raw, str) else str(raw)
            continue

        # Map roles: assistant → model, user → user
        gemini_role = "model" if role == "assistant" else "user"

        # Build parts
        parts: list[genai_types.Part] = []
        if isinstance(raw, str):
            if raw:
                parts.append(genai_types.Part(text=raw))
        elif isinstance(raw, list):
            for block in raw:
                if isinstance(block, str):
                    parts.append(genai_types.Part(text=block))
                elif isinstance(block, dict):
                    btype = block.get("type", "")
                    if btype == "text":
                        parts.append(genai_types.Part(text=block.get("text", "")))
                    elif btype == "image_url":
                        url = block.get("image_url", {}).get("url", "")
                        if url.startswith("data:"):
                            # data:image/png;base64,...
                            header, b64data = url.split(",", 1)
                            mime = header.split(":")[1].split(";")[0]
                            parts.append(genai_types.Part(
                                inline_data=genai_types.Blob(
                                    mime_type=mime,
                                    data=base64.b64decode(b64data),
                                )
                            ))
                    elif btype == "file":
                        file_info = block.get("file", {})
                        file_data_url = file_info.get("file_data", "")
                        if file_data_url.startswith("data:"):
                            header, b64data = file_data_url.split(",", 1)
                            mime = header.split(":")[1].split(";")[0]
                            parts.append(genai_types.Part(
                                inline_data=genai_types.Blob(
                                    mime_type=mime,
                                    data=base64.b64decode(b64data),
                                )
                            ))

        if parts:
            contents.append(genai_types.Content(role=gemini_role, parts=parts))

    return system_instruction, contents


def _openai_tool_to_gemini(tool_def: dict) -> genai_types.FunctionDeclaration:
    """Convert an OpenAI function calling tool definition to Gemini FunctionDeclaration."""
    func = tool_def.get("function", {})
    params = func.get("parameters", {})

    # Convert OpenAI JSON Schema to Gemini Schema
    def _convert_schema(schema: dict) -> dict | None:
        if not schema:
            return None
        result: dict = {}
        if "type" in schema:
            # Gemini uses uppercase type names
            result["type"] = schema["type"].upper()
        if "description" in schema:
            result["description"] = schema["description"]
        if "enum" in schema:
            # Gemini requires all enum values to be strings
            result["enum"] = [str(v) for v in schema["enum"]]
            # If the original type was integer but we converted enums to strings,
            # change the type to STRING for Gemini compatibility
            if schema.get("type") == "integer":
                result["type"] = "STRING"
        if "properties" in schema:
            result["properties"] = {
                k: _convert_schema(v) for k, v in schema["properties"].items()
            }
        if "required" in schema:
            result["required"] = schema["required"]
        if "items" in schema:
            result["items"] = _convert_schema(schema["items"])
        return result

    gemini_params = _convert_schema(params) if params else None

    return genai_types.FunctionDeclaration(
        name=func.get("name", ""),
        description=func.get("description", ""),
        parameters=gemini_params,
    )


def _gemini_config(
    mode: str = "standard",
    custom_tools: list[dict] | None = None,
) -> genai_types.GenerateContentConfig:
    """Build Gemini config with Google Search grounding + optional custom tools."""
    cfg = MODE_CONFIGS.get(mode, MODE_CONFIGS["standard"])
    tools = [genai_types.Tool(google_search=genai_types.GoogleSearch())]
    tool_config = None
    thinking_config = None

    if custom_tools:
        func_decls = [_openai_tool_to_gemini(t) for t in custom_tools]
        tools.append(genai_types.Tool(function_declarations=func_decls))
        # Required when combining built-in tools (Google Search) with function declarations
        tool_config = genai_types.ToolConfig(
            include_server_side_tool_invocations=True,
        )
        # Gemini 3.1 Pro requires thinking mode — include_thoughts=True ensures
        # thought_signature is populated on function_call parts, which is required
        # for multi-turn tool calling round-trips.
        thinking_config = genai_types.ThinkingConfig(include_thoughts=True)

    return genai_types.GenerateContentConfig(
        temperature=cfg["temperature"],
        max_output_tokens=128000,
        tools=tools,
        tool_config=tool_config,
        thinking_config=thinking_config,
    )


def _format_grounding_sources(metadata) -> str:
    """Format grounding metadata as Markdown footnotes."""
    if not metadata:
        return ""
    chunks = getattr(metadata, "grounding_chunks", None)
    if not chunks:
        return ""
    lines = ["\n\n---\n**搜索来源:**"]
    for i, chunk in enumerate(chunks, 1):
        web = getattr(chunk, "web", None)
        if web:
            title = getattr(web, "title", "") or ""
            uri = getattr(web, "uri", "") or ""
            lines.append(f"{i}. [{title}]({uri})" if uri else f"{i}. {title}")
    return "\n".join(lines) if len(lines) > 1 else ""


async def _gemini_stream(
    model_id: str,
    messages: list[dict],
    mode: str = "standard",
) -> AsyncIterator[dict]:
    """Stream response from Gemini via Google native API with grounding."""
    start = time.monotonic()
    full_content = ""
    usage_metadata = None

    last_error = None
    for attempt in range(_MAX_RETRIES + 1):
        if attempt > 0:
            backoff = _RETRY_BACKOFF[min(attempt - 1, len(_RETRY_BACKOFF) - 1)]
            logger.info("Gemini retry %d/%d after %.1fs", attempt, _MAX_RETRIES, backoff)
            await asyncio.sleep(backoff)
            full_content = ""  # reset on retry

        try:
            client = _get_gemini_client()
            system_instruction, contents = _build_gemini_contents(messages)
            config = _gemini_config(mode)
            if system_instruction:
                config.system_instruction = system_instruction

            response = client.models.generate_content_stream(
                model=GEMINI_NATIVE_MODEL,
                contents=contents,
                config=config,
            )

            grounding_metadata = None
            got_content = False
            async for chunk in _sync_iter_to_async(response):
                # Extract text (skip thought parts — internal reasoning)
                text = ""
                if chunk.candidates:
                    candidate = chunk.candidates[0]
                    if candidate.content and candidate.content.parts:
                        for part in candidate.content.parts:
                            if getattr(part, "thought", False):
                                continue
                            if hasattr(part, "text") and part.text:
                                text += part.text
                    # Capture grounding metadata from the last chunk
                    gm = getattr(candidate, "grounding_metadata", None)
                    if gm:
                        grounding_metadata = gm

                # Capture usage metadata (typically available in last chunk)
                um = getattr(chunk, "usage_metadata", None)
                if um:
                    usage_metadata = um

                if text:
                    got_content = True
                    full_content += text
                    yield {"delta": text, "done": False}

            # Append grounding sources
            sources = _format_grounding_sources(grounding_metadata)
            if sources:
                full_content += sources
                yield {"delta": sources, "done": False}

            # Success — break retry loop
            break

        except Exception as e:
            last_error = str(e)[:300]
            # Retry on transient errors if we haven't started yielding content
            is_transient = any(kw in last_error.lower() for kw in ("500", "503", "overloaded", "unavailable", "deadline", "timeout", "reset"))
            if attempt < _MAX_RETRIES and not full_content and is_transient:
                logger.warning("Gemini transient error (attempt %d): %s", attempt + 1, last_error)
                continue

            logger.exception("Error streaming from Gemini native API")
            total_tokens = 0
            if usage_metadata:
                total_tokens = getattr(usage_metadata, "total_token_count", 0) or 0
            yield {
                "delta": "",
                "done": True,
                "error": f"Gemini调用失败: {last_error}",
                "content": full_content,
                "tokens": total_tokens,
                "latency_ms": int((time.monotonic() - start) * 1000),
            }
            return

    latency_ms = int((time.monotonic() - start) * 1000)
    total_tokens = 0
    if usage_metadata:
        total_tokens = getattr(usage_metadata, "total_token_count", 0) or 0
    yield {
        "delta": "",
        "done": True,
        "content": full_content,
        "tokens": total_tokens,
        "latency_ms": latency_ms,
    }


async def _gemini_sync(
    model_id: str,
    messages: list[dict],
    mode: str = "standard",
) -> dict:
    """Non-streaming Gemini call via Google native API."""
    start = time.monotonic()
    try:
        client = _get_gemini_client()
        system_instruction, contents = _build_gemini_contents(messages)
        config = _gemini_config(mode)
        if system_instruction:
            config.system_instruction = system_instruction

        response = client.models.generate_content(
            model=GEMINI_NATIVE_MODEL,
            contents=contents,
            config=config,
        )

        content = response.text or ""

        # Append grounding sources
        if response.candidates:
            gm = getattr(response.candidates[0], "grounding_metadata", None)
            sources = _format_grounding_sources(gm)
            if sources:
                content += sources

        # Extract token usage
        total_tokens = 0
        um = getattr(response, "usage_metadata", None)
        if um:
            total_tokens = getattr(um, "total_token_count", 0) or 0

        latency_ms = int((time.monotonic() - start) * 1000)
        return {"content": content, "tokens": total_tokens, "latency_ms": latency_ms}
    except Exception as e:
        logger.exception("Error calling Gemini native API")
        return {
            "content": "",
            "error": f"Gemini调用失败: {str(e)[:300]}",
            "tokens": 0,
            "latency_ms": int((time.monotonic() - start) * 1000),
        }


def _build_request_body(
    model_id: str,
    messages: list[dict],
    mode: str = "standard",
    stream: bool = True,
    tools: list[dict] | None = None,
    max_tokens: int | None = None,
) -> dict:
    """Build API request body with mode-specific parameters.

    OpenAI native API differences:
    - Model name: strip 'openai/' prefix
    - Token limit: max_completion_tokens (not max_tokens)
    - Reasoning: reasoning_effort (flat string, not nested object)
    - temperature/top_p unavailable when reasoning_effort != "none"
    """
    cfg = MODE_CONFIGS.get(mode, MODE_CONFIGS["standard"])
    native_openai = _is_openai(model_id)
    api_model = model_id.removeprefix("openai/") if native_openai else model_id

    body: dict = {
        "model": api_model,
        "messages": messages,
        "stream": stream,
    }

    # Token limit parameter differs between APIs. Callers may pass max_tokens to
    # cap output for short-form tasks (e.g. title generation, recommendations) —
    # useful because mini/turbo models reject the default ceiling.
    limit = max_tokens if max_tokens is not None else 128000
    if native_openai:
        body["max_completion_tokens"] = limit
    else:
        body["max_tokens"] = limit

    if tools:
        body["tools"] = tools
        body["tool_choice"] = "auto"

    # Thinking mode: add extended thinking / reasoning parameters
    if mode == "thinking":
        model_info = MODEL_MAP.get(model_id, {})
        if model_info.get("supports_thinking"):
            if native_openai:
                # OpenAI Chat Completions API does NOT support function tools
                # with reasoning_effort for gpt-5.4 (returns 400).
                # Only set reasoning_effort when no tools are in the request.
                if not tools:
                    body["reasoning_effort"] = "high"
                    # temperature/top_p are unavailable when reasoning_effort is set
                else:
                    # With tools: use low temperature for analytical quality
                    body["temperature"] = cfg["temperature"]
            else:
                # OpenRouter: nested reasoning object.
                # Like OpenAI, skip extended thinking when tools are present
                # — OpenRouter/Anthropic can fail with reasoning + tools combined.
                if not tools:
                    body["reasoning"] = {"effort": "high"}
                    body["temperature"] = cfg["temperature"]
                    if "anthropic" in model_id:
                        body["transforms"] = ["middle-out"]
                        body["temperature"] = 1  # Anthropic thinking requires temperature=1
                else:
                    body["temperature"] = cfg["temperature"]
        else:
            body["temperature"] = cfg["temperature"]
    else:
        body["temperature"] = cfg["temperature"]

    # OpenAI streaming: request usage info in stream chunks
    if native_openai and stream:
        body["stream_options"] = {"include_usage": True}

    return body


async def call_model_stream(
    model_id: str,
    messages: list[dict],
    mode: str = "standard",
) -> AsyncIterator[dict]:
    """Stream responses from a model. Gemini → Google native API; others → OpenRouter.

    Yields dicts: {"delta": "...", "done": False} or
                  {"delta": "", "done": True, "tokens": N, "latency_ms": N, "content": "full text"}
    """
    # Route Gemini to native API
    if _is_gemini(model_id):
        async for chunk in _gemini_stream(model_id, messages, mode):
            yield chunk
        return

    start = time.monotonic()
    full_content = ""
    tokens_used = 0
    base_url, headers = _api_config(model_id)

    last_error = None
    for attempt in range(_MAX_RETRIES + 1):
        if attempt > 0:
            backoff = _RETRY_BACKOFF[min(attempt - 1, len(_RETRY_BACKOFF) - 1)]
            logger.info("Retry %d/%d for %s after %.1fs", attempt, _MAX_RETRIES, model_id, backoff)
            await asyncio.sleep(backoff)

        try:
            timeout_val = 300.0 if mode == "thinking" else 180.0
            async with httpx.AsyncClient(timeout=httpx.Timeout(timeout_val, connect=15.0), proxy=_proxy_for_api(model_id)) as client:
                async with client.stream(
                    "POST",
                    f"{base_url}/chat/completions",
                    headers=headers,
                    json=_build_request_body(model_id, messages, mode, stream=True),
                ) as response:
                    if response.status_code != 200:
                        body = await response.aread()
                        error_text = body.decode("utf-8", errors="replace")
                        parsed_error = _parse_api_error(response.status_code, error_text)
                        # Retry on transient errors
                        if response.status_code in _RETRYABLE_STATUS and attempt < _MAX_RETRIES:
                            last_error = parsed_error
                            logger.warning("Retryable error from %s (attempt %d): %s", model_id, attempt + 1, last_error)
                            continue
                        yield {
                            "delta": "",
                            "done": True,
                            "error": parsed_error,
                            "content": "",
                            "tokens": 0,
                            "latency_ms": int((time.monotonic() - start) * 1000),
                        }
                        return

                    # Connected successfully — no more retries after we start streaming
                    finish_reason = None
                    async for line in response.aiter_lines():
                        if not line.startswith("data: "):
                            continue
                        data = line[6:]
                        if data.strip() == "[DONE]":
                            break
                        try:
                            chunk = json.loads(data)
                            # OpenAI sends final chunk with choices:[] for usage
                            choices = chunk.get("choices") or [{}]
                            if choices:
                                choice = choices[0]
                                delta = choice.get("delta", {})
                                text = delta.get("content", "")
                                if text:
                                    full_content += text
                                    yield {"delta": text, "done": False}
                                fr = choice.get("finish_reason")
                                if fr:
                                    finish_reason = fr
                            # Check for usage info
                            if chunk.get("usage"):
                                tokens_used = chunk["usage"].get("total_tokens", 0)
                        except json.JSONDecodeError:
                            continue

                    if finish_reason == "length":
                        logger.warning("Model %s hit output token limit (finish_reason=length)", model_id)
                    # Success — break retry loop
                    break

        except (httpx.TimeoutException, httpx.ConnectError, httpx.RemoteProtocolError) as e:
            last_error = str(e)[:200]
            if attempt < _MAX_RETRIES:
                logger.warning("Retryable exception from %s (attempt %d): %s", model_id, attempt + 1, last_error)
                continue
            yield {
                "delta": "",
                "done": True,
                "error": "请求超时，请稍后重试" if isinstance(e, httpx.TimeoutException) else f"连接失败: {last_error}",
                "content": full_content,
                "tokens": tokens_used,
                "latency_ms": int((time.monotonic() - start) * 1000),
            }
            return
        except Exception as e:
            logger.exception("Error streaming from model %s", model_id)
            yield {
                "delta": "",
                "done": True,
                "error": f"调用失败: {str(e)[:200]}",
                "content": full_content,
                "tokens": tokens_used,
                "latency_ms": int((time.monotonic() - start) * 1000),
            }
            return

    latency_ms = int((time.monotonic() - start) * 1000)
    error_msg = None
    if finish_reason == "length":
        error_msg = "响应已达到输出token上限，内容可能不完整"
    yield {
        "delta": "",
        "done": True,
        "content": full_content,
        "tokens": tokens_used,
        "latency_ms": latency_ms,
        "error": error_msg,
    }


async def call_model_sync(
    model_id: str,
    messages: list[dict],
    mode: str = "standard",
    max_tokens: int | None = None,
) -> dict:
    """Non-streaming call to a model. Gemini → Google native API; others → OpenRouter."""
    # Route Gemini to native API
    if _is_gemini(model_id):
        return await _gemini_sync(model_id, messages, mode)

    start = time.monotonic()
    base_url, headers = _api_config(model_id)
    try:
        timeout_val = 300.0 if mode == "thinking" else 180.0
        async with httpx.AsyncClient(timeout=httpx.Timeout(timeout_val, connect=15.0), proxy=_proxy_for_api(model_id)) as client:
            resp = await client.post(
                f"{base_url}/chat/completions",
                headers=headers,
                json=_build_request_body(model_id, messages, mode, stream=False, max_tokens=max_tokens),
            )
            latency_ms = int((time.monotonic() - start) * 1000)
            if resp.status_code != 200:
                return {
                    "content": "",
                    "error": _parse_api_error(resp.status_code, resp.text),
                    "tokens": 0,
                    "latency_ms": latency_ms,
                }
            data = resp.json()
            content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
            tokens = data.get("usage", {}).get("total_tokens", 0)
            return {
                "content": content,
                "tokens": tokens,
                "latency_ms": latency_ms,
            }
    except Exception as e:
        return {
            "content": "",
            "error": f"调用失败: {str(e)[:200]}",
            "tokens": 0,
            "latency_ms": int((time.monotonic() - start) * 1000),
        }


async def call_model_stream_with_tools(
    model_id: str,
    messages: list[dict],
    mode: str = "standard",
    tools: list[dict] | None = None,
    max_tool_rounds: int = 5,
    trace_id: str | None = None,
) -> AsyncIterator[dict]:
    """Stream with multi-round tool calling support.

    Gemini → native API with Google Search grounding + custom function declarations.
    Others → OpenRouter with OpenAI-style function calling.

    Supports up to `max_tool_rounds` rounds of tool calling. Each round:
    the LLM may call tools or produce final text. If tools are called,
    they are executed (in parallel when possible) and results fed back.
    The loop continues until the LLM produces text without tool calls
    or the round limit is reached.
    """
    from backend.app.services.chat_debug import chat_trace
    from backend.app.services.research_interaction_log import get_recorder
    trace = chat_trace(model_id=model_id, trace_id=trace_id or "")
    recorder = get_recorder()

    # Route Gemini to native API with function calling
    if _is_gemini(model_id):
        trace.log_sse_event("ROUTE_GEMINI", "using native Gemini API with function calling")
        async for chunk in _gemini_stream_with_tools(model_id, messages, mode, tools, max_tool_rounds, trace_id=trace_id):
            yield chunk
        return

    if not tools:
        trace.log_sse_event("ROUTE_NO_TOOLS", "streaming without tools")
        async for chunk in call_model_stream(model_id, messages, mode):
            yield chunk
        return

    logger.info("Tool-stream: sending %d tools to %s (mode=%s, max_rounds=%d)",
                len(tools), model_id, mode, max_tool_rounds)
    trace.log_llm_request(messages, tools, mode, round_num=0)

    from backend.app.services.alphapai_service import execute_tool as alphapai_execute
    from backend.app.services.jinmen_service import execute_tool as jinmen_execute
    from backend.app.services.web_search_tool import (
        execute_tool as web_search_execute,
        CitationTracker,
    )

    citation_tracker = CitationTracker()

    # Per-request activity tracking for REQUEST_SUMMARY
    activity: dict = {
        "tool_call_names": [],      # all tool call names across rounds
        "search_queries": [],       # unique search queries (query_cn)
        "urls_read": [],            # URLs sent to read_webpage
        "rounds_used": 0,
    }

    async def dispatch_tool(name: str, arguments: dict, round_num: int = 0) -> str:
        """Execute a single tool with timeout protection."""
        trace.log_tool_exec_start(name, arguments)
        activity["tool_call_names"].append(name)
        if name == "web_search":
            q = arguments.get("query_cn", "")
            if q and q not in activity["search_queries"]:
                activity["search_queries"].append(q)
        elif name == "read_webpage":
            u = arguments.get("url", "")
            if u and u not in activity["urls_read"]:
                activity["urls_read"].append(u)

        tool_start = time.monotonic()
        try:
            coro = None
            if name in ("web_search", "read_webpage"):
                coro = web_search_execute(name, arguments, citation_tracker)
            elif name.startswith("jinmen_"):
                coro = jinmen_execute(name, arguments, citation_tracker)
            elif name.startswith("kb_"):
                from backend.app.services.kb_service import execute_tool as kb_execute
                coro = kb_execute(name, arguments, citation_tracker)
            elif name.startswith("user_kb_"):
                from backend.app.services.user_kb_tools import execute_tool as user_kb_execute
                coro = user_kb_execute(name, arguments, citation_tracker)
            elif name == "consensus_forecast_query":
                from backend.app.services.consensus_forecast_tool import (
                    execute_tool as consensus_execute,
                )
                coro = consensus_execute(name, arguments, citation_tracker)
            elif name == "trigger_revenue_model":
                from backend.app.services.revenue_model_chat_tool import (
                    execute_tool as trigger_execute,
                )
                coro = trigger_execute(name, arguments, citation_tracker)
            else:
                coro = alphapai_execute(name, arguments, citation_tracker)

            result = await asyncio.wait_for(coro, timeout=_TOOL_EXEC_TIMEOUT)
            elapsed_ms = int((time.monotonic() - tool_start) * 1000)
            if name in ("web_search", "read_webpage"):
                trace.log_tool_exec_done(name, result[0], elapsed_ms)
                result_text = result[0]
                if trace_id:
                    recorder.record_tool_result(
                        trace_id, model_id, round_num, name, arguments,
                        result_text, elapsed_ms,
                    )
                return result_text
            trace.log_tool_exec_done(name, result, elapsed_ms)
            if trace_id:
                recorder.record_tool_result(
                    trace_id, model_id, round_num, name, arguments,
                    result, elapsed_ms,
                )
            return result
        except asyncio.TimeoutError:
            elapsed_ms = int((time.monotonic() - tool_start) * 1000)
            trace.log_tool_timeout(name, _TOOL_EXEC_TIMEOUT)
            logger.warning("Tool %s timed out after %.0fs", name, _TOOL_EXEC_TIMEOUT)
            if trace_id:
                recorder.record_tool_result(
                    trace_id, model_id, round_num, name, arguments,
                    f"Tool timed out after {int(_TOOL_EXEC_TIMEOUT)}s",
                    elapsed_ms, error="timeout",
                )
            return f"Tool {name} timed out after {int(_TOOL_EXEC_TIMEOUT)} seconds."
        except Exception as e:
            elapsed_ms = int((time.monotonic() - tool_start) * 1000)
            trace.log_tool_exec_done(name, f"ERROR: {e}", elapsed_ms, error=True)
            if trace_id:
                recorder.record_tool_result(
                    trace_id, model_id, round_num, name, arguments,
                    f"ERROR: {e}", elapsed_ms, error=str(e)[:300],
                )
            raise

    start = time.monotonic()
    full_content = ""
    tokens_used = 0
    finish_reason = None
    current_messages = list(messages)
    exited_cleanly = False  # True if LLM produced text without tool calls

    for tool_round in range(1, max_tool_rounds + 1):
        activity["rounds_used"] = tool_round
        # Check overall timeout before each round
        elapsed = time.monotonic() - start
        if elapsed > _OVERALL_TOOL_TIMEOUT:
            logger.warning("Tool-stream overall timeout (%.0fs) for %s after %d rounds",
                           elapsed, model_id, tool_round - 1)
            break

        is_last_round = (tool_round == max_tool_rounds)
        round_content = ""
        tool_calls_acc: dict[int, dict] = {}
        has_tool_calls = False

        # For the synthesis round (last round): inject a user message telling
        # the model to synthesize instead of searching. Tools stay in the
        # request — removing tools from a conversation that has tool_calls in
        # history causes OpenRouter/Anthropic to return content:null.
        if is_last_round and tool_round > 1:
            current_messages.append({
                "role": "user",
                "content": (
                    "你已经进行了充分的搜索和信息收集。"
                    "现在请基于以上所有搜索结果，直接给出完整详细的分析报告。"
                    "不要再调用任何工具，直接输出你的分析。"
                ),
            })

        # API call with retry on transient errors (matching call_model_stream pattern)
        trace.log_llm_request(current_messages, tools, mode, round_num=tool_round)
        if trace_id:
            recorder.record_round_start(trace_id, model_id, tool_round, current_messages, tools, mode)
        base_url, api_headers = _api_config(model_id)
        round_success = False
        for attempt in range(_MAX_RETRIES + 1):
            if attempt > 0:
                backoff = _RETRY_BACKOFF[min(attempt - 1, len(_RETRY_BACKOFF) - 1)]
                logger.info("Tool-stream retry %d/%d for %s round %d after %.1fs",
                            attempt, _MAX_RETRIES, model_id, tool_round, backoff)
                trace.log_llm_retry(attempt, f"backoff={backoff}s round={tool_round}")
                await asyncio.sleep(backoff)
                # Reset accumulators on retry
                round_content = ""
                tool_calls_acc = {}
                has_tool_calls = False

            try:
                timeout_val = 300.0 if mode == "thinking" else 240.0
                # Always pass original tools to _build_request_body so it
                # suppresses extended thinking (thinking + tool message
                # history causes empty responses on OpenRouter/Anthropic).
                request_body = _build_request_body(
                    model_id, current_messages, mode,
                    stream=True, tools=tools,
                )
                async with httpx.AsyncClient(
                    timeout=httpx.Timeout(timeout_val, connect=15.0), proxy=_proxy_for_api(model_id),
                ) as client:
                    async with client.stream(
                        "POST",
                        f"{base_url}/chat/completions",
                        headers=api_headers,
                        json=request_body,
                    ) as response:
                        if response.status_code != 200:
                            resp_body = await response.aread()
                            error_text = resp_body.decode("utf-8", errors="replace")
                            parsed_error = _parse_api_error(response.status_code, error_text)
                            # Retry on transient status codes
                            if response.status_code in _RETRYABLE_STATUS and attempt < _MAX_RETRIES:
                                logger.warning("Retryable error in tool round %d for %s (attempt %d): %s",
                                               tool_round, model_id, attempt + 1, parsed_error)
                                trace.log_llm_retry(attempt + 1, parsed_error)
                                continue
                            trace.log_llm_error(parsed_error, round_num=tool_round)
                            yield {
                                "delta": "", "done": True,
                                "error": parsed_error,
                                "content": full_content, "tokens": tokens_used,
                                "latency_ms": int((time.monotonic() - start) * 1000),
                            }
                            return

                        finish_reason = None
                        async for line in response.aiter_lines():
                            if not line.startswith("data: "):
                                continue
                            data = line[6:]
                            if data.strip() == "[DONE]":
                                break
                            try:
                                chunk = json.loads(data)
                                choices = chunk.get("choices") or [{}]
                                if choices:
                                    choice = choices[0]
                                    delta = choice.get("delta", {})

                                    text = delta.get("content", "")
                                    if text:
                                        round_content += text
                                        full_content += text
                                        yield {"delta": text, "done": False}

                                    if delta.get("tool_calls"):
                                        has_tool_calls = True
                                        for tc in delta["tool_calls"]:
                                            idx = tc.get("index", 0)
                                            if idx not in tool_calls_acc:
                                                tool_calls_acc[idx] = {
                                                    "id": tc.get("id", ""),
                                                    "name": tc.get("function", {}).get("name", ""),
                                                    "arguments": "",
                                                }
                                            if tc.get("id"):
                                                tool_calls_acc[idx]["id"] = tc["id"]
                                            if tc.get("function", {}).get("name"):
                                                tool_calls_acc[idx]["name"] = tc["function"]["name"]
                                            if tc.get("function", {}).get("arguments"):
                                                tool_calls_acc[idx]["arguments"] += tc["function"]["arguments"]

                                    fr = choice.get("finish_reason")
                                    if fr:
                                        finish_reason = fr
                                if chunk.get("usage"):
                                    tokens_used = chunk["usage"].get("total_tokens", 0)
                            except json.JSONDecodeError:
                                continue

                        if finish_reason == "length":
                            logger.warning("Model %s hit output token limit in tool round %d", model_id, tool_round)

                round_success = True
                break  # Success — exit retry loop

            except (httpx.TimeoutException, httpx.ConnectError, httpx.RemoteProtocolError) as e:
                if attempt < _MAX_RETRIES:
                    logger.warning("Retryable exception in tool round %d for %s (attempt %d): %s",
                                   tool_round, model_id, attempt + 1, str(e)[:200])
                    continue
                yield {
                    "delta": "", "done": True,
                    "error": "请求超时，请稍后重试" if isinstance(e, httpx.TimeoutException) else f"连接失败: {str(e)[:200]}",
                    "content": full_content, "tokens": tokens_used,
                    "latency_ms": int((time.monotonic() - start) * 1000),
                }
                return
            except Exception as e:
                logger.exception("Error streaming round %d from model %s", tool_round, model_id)
                yield {
                    "delta": "", "done": True, "error": f"调用失败: {str(e)[:200]}",
                    "content": full_content, "tokens": tokens_used,
                    "latency_ms": int((time.monotonic() - start) * 1000),
                }
                return

        if not round_success:
            # All retries exhausted without yielding an error (shouldn't happen, but be safe)
            yield {
                "delta": "", "done": True, "error": "所有重试均失败",
                "content": full_content, "tokens": tokens_used,
                "latency_ms": int((time.monotonic() - start) * 1000),
            }
            return

        # No tool calls → LLM produced final text, we're done
        if not has_tool_calls:
            trace.log_llm_done(
                content_len=len(round_content), tokens=tokens_used,
                latency_ms=int((time.monotonic() - start) * 1000),
                finish_reason=finish_reason, round_num=tool_round,
            )
            trace.log_llm_response_content(round_content, round_num=tool_round)
            exited_cleanly = True
            break

        # Build assistant message with tool_calls
        assistant_msg: dict = {
            "role": "assistant",
            "content": round_content or None,
            "tool_calls": [],
        }
        for idx in sorted(tool_calls_acc.keys()):
            tc = tool_calls_acc[idx]
            assistant_msg["tool_calls"].append({
                "id": tc["id"],
                "type": "function",
                "function": {"name": tc["name"], "arguments": tc["arguments"]},
            })
        current_messages.append(assistant_msg)

        # Log the model's reasoning text (if any) emitted before these tool calls
        if round_content:
            trace.log_model_reasoning(round_num=tool_round, text=round_content)
            if trace_id:
                recorder.record_reasoning(trace_id, model_id, tool_round, round_content)
        # Log the tool calls the LLM decided to make
        trace.log_tool_calls_detected(assistant_msg["tool_calls"], round_num=tool_round)
        if trace_id:
            recorder.record_tool_calls_detected(trace_id, model_id, tool_round, assistant_msg["tool_calls"])

        # Execute tool calls (parallel when possible)
        tool_tasks = []
        for tc_data in assistant_msg["tool_calls"]:
            tool_name = tc_data["function"]["name"]
            try:
                args = json.loads(tc_data["function"]["arguments"])
            except json.JSONDecodeError:
                args = {}
            tool_tasks.append((tc_data, tool_name, args))

        # Yield search/read status events and execute
        for tc_data, tool_name, args in tool_tasks:
            # Emit search_status / read_status for web search tools
            if tool_name == "web_search":
                query_display = args.get("query_cn", "")[:50]
                yield {"type": "search_status", "query": query_display, "status": "searching"}
            elif tool_name == "read_webpage":
                yield {"type": "read_status", "url": args.get("url", "")[:100], "status": "reading"}
            else:
                yield {"type": "tool_status", "tool_name": tool_name, "status": "calling"}

        # Execute all tool calls in parallel with heartbeat to keep SSE alive
        tool_futures = asyncio.gather(
            *[dispatch_tool(name, args, round_num=tool_round) for _, name, args in tool_tasks],
            return_exceptions=True,
        )
        # Yield heartbeat every 15s while tools execute (keeps nginx/browser alive)
        results = None
        while results is None:
            try:
                results = await asyncio.wait_for(asyncio.shield(tool_futures), timeout=15.0)
            except asyncio.TimeoutError:
                yield {"type": "heartbeat"}
        if results is None:
            results = await tool_futures

        # Append tool results to messages and emit status done events
        for (tc_data, tool_name, args), result in zip(tool_tasks, results):
            if isinstance(result, Exception):
                logger.warning("Tool %s failed: %s", tool_name, result)
                result_str = f"Tool execution error: {str(result)[:200]}"
            else:
                result_str = result

            # Emit done status
            if tool_name == "web_search":
                yield {"type": "search_status", "query": args.get("query_cn", "")[:50], "status": "done"}
            elif tool_name == "read_webpage":
                yield {"type": "read_status", "url": args.get("url", "")[:100], "status": "done"}
            else:
                yield {"type": "tool_status", "tool_name": tool_name, "status": "done"}

            current_messages.append({
                "role": "tool",
                "tool_call_id": tc_data["id"],
                "content": result_str,
            })

        logger.info("Tool-stream round %d: executed %d tools for %s",
                     tool_round, len(tool_tasks), model_id)

    # ── Synthesis fallback ────────────────────────────────────────
    # Trigger when:
    # (a) Loop exhausted without LLM producing text (tool-loop exit, content too small), OR
    # (b) Stream ended abnormally (finish_reason=None) — OpenAI occasionally cuts
    #     the SSE connection mid-response when processing very long tool histories,
    #     producing visibly truncated output. finish_reason=='length' means we hit
    #     the token cap (can't salvage more), but finish_reason is None/missing is
    #     the "silent truncation" case we can recover from by doing a synthesis call.
    _tool_results_exist = any(m.get("role") == "tool" for m in current_messages)
    _stream_cut = (finish_reason is None and len(full_content) > 0 and _tool_results_exist)
    _no_content = (not exited_cleanly and len(full_content) < 500 and len(current_messages) > 2)
    if _stream_cut:
        trace.log_llm_error(
            f"Stream cut off abnormally (finish_reason=None, content_len={len(full_content)}); "
            "triggering synthesis fallback.",
            round_num=activity["rounds_used"],
        )
    if _no_content or _stream_cut:
        trace.log_sse_event(
            "SYNTHESIS_FALLBACK",
            f"reason={'stream_cut' if _stream_cut else 'no_content'} "
            f"full_content={len(full_content)} chars, rebuilding",
        )
        logger.info("Synthesis fallback for %s: full_content=%d chars after %d rounds (reason=%s)",
                     model_id, len(full_content), max_tool_rounds,
                     "stream_cut" if _stream_cut else "no_content")

        # When we already streamed partial content (stream_cut), emit a visible
        # separator so the user understands the remaining text is a restart and
        # not a continuation. Chunks already sent to the client cannot be retracted.
        if _stream_cut:
            sep = "\n\n---\n\n> ⚠️ 上一段回复因上游流式中断而被截断，以下为基于已收集数据的完整版本：\n\n"
            full_content += sep
            yield {"delta": sep, "done": False}

        try:
            tool_summary = _extract_tool_results_summary(current_messages)
            # Build clean messages without tool_call/tool history
            synthesis_msgs = [m for m in messages if m.get("role") in ("system", "user")]
            synthesis_msgs.append({
                "role": "assistant",
                "content": f"我已完成以下信息收集：\n\n{tool_summary}",
            })
            synthesis_msgs.append({
                "role": "user",
                "content": (
                    "请基于以上收集到的所有信息，输出完整详细的分析报告。"
                    "确保涵盖所有重要发现，提供深入分析和明确结论。"
                    "使用 [N] 行内引用来源（编号与上方工具结果中的 [N] 对齐）。"
                ),
            })

            synthesis_content = ""
            async for chunk in call_model_stream(model_id, synthesis_msgs, mode):
                if chunk.get("delta"):
                    synthesis_content += chunk["delta"]
                    full_content += chunk["delta"]
                    yield {"delta": chunk["delta"], "done": False}
                if chunk.get("done"):
                    tokens_used += chunk.get("tokens", 0)
                    if chunk.get("error"):
                        trace.log_llm_error(f"Synthesis error: {chunk['error']}")

            trace.log_sse_event("SYNTHESIS_DONE", f"added {len(synthesis_content)} chars")
        except Exception as e:
            logger.exception("Synthesis fallback failed for %s", model_id)
            trace.log_llm_error(f"Synthesis fallback exception: {e}")

    # Emit citation sources if any were tracked
    if citation_tracker.sources:
        yield {"type": "sources", "sources": citation_tracker.sources}

    # Collect URLs returned by search across the entire request
    urls_searched_all = [s.get("url", "") for s in citation_tracker.sources if s.get("url")]
    trace.log_request_summary(
        rounds_used=activity["rounds_used"],
        tool_calls_total=len(activity["tool_call_names"]),
        tool_call_names=activity["tool_call_names"],
        search_queries=activity["search_queries"],
        urls_searched=urls_searched_all,
        urls_read=activity["urls_read"],
        citations_count=len(citation_tracker.sources),
        final_content_len=len(full_content),
        total_tokens=tokens_used,
    )
    trace.log_full_response(full_content)

    error_msg = None
    if finish_reason == "length":
        error_msg = "响应已达到输出token上限，内容可能不完整"
    yield {
        "delta": "", "done": True,
        "content": full_content, "tokens": tokens_used,
        "latency_ms": int((time.monotonic() - start) * 1000),
        "error": error_msg,
    }


def _extract_tool_results_summary(messages: list[dict]) -> str:
    """Extract all tool results from message history as a readable text summary."""
    parts = []
    for msg in messages:
        if msg.get("role") == "tool":
            content = msg.get("content", "")
            if content and len(content) > 10:
                # Truncate very long individual results to keep context manageable
                if len(content) > 3000:
                    content = content[:2500] + f"\n... [截断，原文{len(content)}字符]"
                parts.append(content)
    return "\n\n---\n\n".join(parts) if parts else "未收集到有效信息。"


# ── Gemini native streaming with function calling ──────────────

async def _gemini_stream_with_tools(
    model_id: str,
    messages: list[dict],
    mode: str = "standard",
    tools: list[dict] | None = None,
    max_tool_rounds: int = 5,
    trace_id: str | None = None,
) -> AsyncIterator[dict]:
    """Stream from Gemini native API with Google Search grounding + custom function calling.

    Gemini gets both built-in Google Search grounding AND our custom tools
    (web_search, read_webpage, alphapai, jinmen) via native function declarations.

    Reliability features (industry best practices):
    - Per-round timeout on generate_content to prevent indefinite hangs
    - Retry with backoff on transient errors (500, 503, overloaded, timeout)
    - Per-tool execution timeout to prevent one hung tool from blocking all
    - Overall timeout to cap total multi-round time
    - Per-round try/except for granular error recovery
    """
    from backend.app.services.chat_debug import chat_trace
    from backend.app.services.research_interaction_log import get_recorder
    trace = chat_trace(model_id=model_id, trace_id=trace_id or "")
    recorder = get_recorder()

    from backend.app.services.alphapai_service import execute_tool as alphapai_execute
    from backend.app.services.jinmen_service import execute_tool as jinmen_execute
    from backend.app.services.web_search_tool import (
        execute_tool as web_search_execute,
        CitationTracker,
    )

    citation_tracker = CitationTracker()

    # Per-request activity tracking for REQUEST_SUMMARY
    activity: dict = {
        "tool_call_names": [],
        "search_queries": [],
        "urls_read": [],
        "rounds_used": 0,
    }

    async def dispatch_tool(name: str, arguments: dict, round_num: int = 0) -> str:
        """Execute a single tool with timeout protection."""
        trace.log_tool_exec_start(name, arguments)
        activity["tool_call_names"].append(name)
        if name == "web_search":
            q = arguments.get("query_cn", "")
            if q and q not in activity["search_queries"]:
                activity["search_queries"].append(q)
        elif name == "read_webpage":
            u = arguments.get("url", "")
            if u and u not in activity["urls_read"]:
                activity["urls_read"].append(u)

        tool_start = time.monotonic()
        try:
            coro = None
            if name in ("web_search", "read_webpage"):
                coro = web_search_execute(name, arguments, citation_tracker)
            elif name.startswith("jinmen_"):
                coro = jinmen_execute(name, arguments, citation_tracker)
            elif name.startswith("kb_"):
                from backend.app.services.kb_service import execute_tool as kb_execute
                coro = kb_execute(name, arguments, citation_tracker)
            elif name.startswith("user_kb_"):
                from backend.app.services.user_kb_tools import execute_tool as user_kb_execute
                coro = user_kb_execute(name, arguments, citation_tracker)
            elif name == "consensus_forecast_query":
                from backend.app.services.consensus_forecast_tool import (
                    execute_tool as consensus_execute,
                )
                coro = consensus_execute(name, arguments, citation_tracker)
            elif name == "trigger_revenue_model":
                from backend.app.services.revenue_model_chat_tool import (
                    execute_tool as trigger_execute,
                )
                coro = trigger_execute(name, arguments, citation_tracker)
            else:
                coro = alphapai_execute(name, arguments, citation_tracker)

            result = await asyncio.wait_for(coro, timeout=_TOOL_EXEC_TIMEOUT)
            elapsed_ms = int((time.monotonic() - tool_start) * 1000)
            if name in ("web_search", "read_webpage"):
                trace.log_tool_exec_done(name, result[0], elapsed_ms)
                result_text = result[0]
                if trace_id:
                    recorder.record_tool_result(
                        trace_id, model_id, round_num, name, arguments,
                        result_text, elapsed_ms,
                    )
                return result_text
            trace.log_tool_exec_done(name, result, elapsed_ms)
            if trace_id:
                recorder.record_tool_result(
                    trace_id, model_id, round_num, name, arguments,
                    result, elapsed_ms,
                )
            return result
        except asyncio.TimeoutError:
            trace.log_tool_timeout(name, _TOOL_EXEC_TIMEOUT)
            logger.warning("Tool %s timed out after %.0fs", name, _TOOL_EXEC_TIMEOUT)
            if trace_id:
                recorder.record_tool_result(
                    trace_id, model_id, round_num, name, arguments,
                    f"Tool timed out after {int(_TOOL_EXEC_TIMEOUT)}s",
                    int(_TOOL_EXEC_TIMEOUT * 1000), error="timeout",
                )
            return f"Tool {name} timed out after {int(_TOOL_EXEC_TIMEOUT)} seconds."
        except Exception as e:
            elapsed_ms = int((time.monotonic() - tool_start) * 1000)
            trace.log_tool_exec_done(name, f"ERROR: {e}", elapsed_ms, error=True)
            if trace_id:
                recorder.record_tool_result(
                    trace_id, model_id, round_num, name, arguments,
                    f"ERROR: {e}", elapsed_ms, error=str(e)[:300],
                )
            raise

    _TRANSIENT_KEYWORDS = ("500", "503", "overloaded", "unavailable", "deadline", "timeout", "reset", "rate")

    start = time.monotonic()
    full_content = ""
    usage_metadata = None

    trace.log_llm_request(messages, tools, mode, round_num=0)

    client = _get_gemini_client()
    system_instruction, contents = _build_gemini_contents(messages)
    config = _gemini_config(mode, custom_tools=tools)
    if system_instruction:
        config.system_instruction = system_instruction

    # Config without custom tools for the last round (force text output)
    config_no_tools = _gemini_config(mode, custom_tools=None)
    if system_instruction:
        config_no_tools.system_instruction = system_instruction

    for tool_round in range(1, max_tool_rounds + 1):
        activity["rounds_used"] = tool_round
        trace.log_sse_event("GEMINI_ROUND_START", f"round={tool_round}/{max_tool_rounds}")
        if trace_id:
            recorder.record_round_start(trace_id, model_id, tool_round, messages, tools, mode)
        # Check overall timeout before each round
        elapsed = time.monotonic() - start
        if elapsed > _OVERALL_TOOL_TIMEOUT:
            logger.warning("Gemini tool-stream overall timeout (%.0fs) after %d rounds",
                           elapsed, tool_round - 1)
            break

        is_last_round = (tool_round == max_tool_rounds)

        # Non-last rounds: use non-streaming to preserve thought_signature
        # on function call parts (streaming chunks don't include it).
        # Last round or text-only: use streaming for better UX.
        if not is_last_round:
            # API call with retry and timeout
            response = None
            last_error = None
            for attempt in range(_MAX_RETRIES + 1):
                if attempt > 0:
                    backoff = _RETRY_BACKOFF[min(attempt - 1, len(_RETRY_BACKOFF) - 1)]
                    logger.info("Gemini tool round %d retry %d/%d after %.1fs",
                                tool_round, attempt, _MAX_RETRIES, backoff)
                    await asyncio.sleep(backoff)

                try:
                    # Run generate_content with heartbeat to keep SSE alive.
                    # Frontend has a 120s read timeout; Gemini can take longer.
                    # Yield heartbeat every 15s so the connection stays open.
                    gen_future = asyncio.ensure_future(
                        asyncio.to_thread(
                            client.models.generate_content,
                            model=GEMINI_NATIVE_MODEL,
                            contents=contents,
                            config=config,
                        )
                    )
                    gen_start = time.monotonic()
                    while response is None:
                        remaining = _GEMINI_ROUND_TIMEOUT - (time.monotonic() - gen_start)
                        if remaining <= 0:
                            gen_future.cancel()
                            raise asyncio.TimeoutError()
                        try:
                            response = await asyncio.wait_for(
                                asyncio.shield(gen_future), timeout=min(15.0, remaining),
                            )
                        except asyncio.TimeoutError:
                            if gen_future.done():
                                # Future finished but raised — re-await to get exception
                                response = await gen_future
                            else:
                                yield {"type": "heartbeat"}
                    break  # Success
                except asyncio.TimeoutError:
                    last_error = f"generate_content timed out after {int(_GEMINI_ROUND_TIMEOUT)}s"
                    logger.warning("Gemini round %d timeout (attempt %d): %s",
                                   tool_round, attempt + 1, last_error)
                    if attempt < _MAX_RETRIES:
                        continue
                except Exception as e:
                    last_error = str(e)[:300]
                    is_transient = any(kw in last_error.lower() for kw in _TRANSIENT_KEYWORDS)
                    if is_transient and attempt < _MAX_RETRIES:
                        logger.warning("Gemini transient error round %d (attempt %d): %s",
                                       tool_round, attempt + 1, last_error)
                        continue
                    logger.exception("Gemini non-transient error in tool round %d", tool_round)

            if response is None:
                trace.log_llm_error(f"Gemini response=None: {last_error}", round_num=tool_round)
                total_tokens = 0
                if usage_metadata:
                    total_tokens = getattr(usage_metadata, "total_token_count", 0) or 0
                yield {
                    "delta": "", "done": True,
                    "error": f"Gemini调用失败: {last_error}",
                    "content": full_content, "tokens": total_tokens,
                    "latency_ms": int((time.monotonic() - start) * 1000),
                }
                return

            um = getattr(response, "usage_metadata", None)
            if um:
                usage_metadata = um

            function_calls: list[dict] = []
            round_text_emitted = ""

            if response.candidates:
                candidate = response.candidates[0]
                if candidate.content and candidate.content.parts:
                    for part in candidate.content.parts:
                        # Skip thought parts (internal reasoning, not for user)
                        if getattr(part, "thought", False):
                            continue
                        if hasattr(part, "text") and part.text:
                            full_content += part.text
                            round_text_emitted += part.text
                            yield {"delta": part.text, "done": False}
                        if hasattr(part, "function_call") and part.function_call:
                            fc = part.function_call
                            fc_args = dict(fc.args) if fc.args else {}
                            # Strip namespace prefix (Gemini may add "default_api:")
                            fc_name = fc.name
                            if ":" in fc_name:
                                fc_name = fc_name.split(":", 1)[1]
                            function_calls.append({"name": fc_name, "args": fc_args})

            if round_text_emitted and function_calls:
                # Text preceded the function calls — log it as model reasoning
                trace.log_model_reasoning(round_num=tool_round, text=round_text_emitted)
                if trace_id:
                    recorder.record_reasoning(trace_id, model_id, tool_round, round_text_emitted)

            if not function_calls:
                trace.log_sse_event("GEMINI_NO_FUNC_CALLS", f"round={tool_round} — LLM returned text only")
                trace.log_llm_response_content(full_content, round_num=tool_round)
                grounding_metadata = getattr(
                    response.candidates[0] if response.candidates else None,
                    "grounding_metadata", None,
                )
                trace.log_gemini_grounding(
                    grounding_metadata is not None,
                    len(getattr(grounding_metadata, "grounding_chunks", []) or []) if grounding_metadata else 0,
                )
                sources_text = _format_grounding_sources(grounding_metadata)
                if sources_text:
                    full_content += sources_text
                    yield {"delta": sources_text, "done": False}
                break

            # Log the function calls Gemini decided to make
            trace.log_gemini_function_calls(function_calls, round_num=tool_round)
            if trace_id:
                # Reshape to match record_tool_calls_detected format.
                tc_shaped = [
                    {"id": f"{tool_round}_{i}", "function": {"name": fc["name"], "arguments": fc["args"]}}
                    for i, fc in enumerate(function_calls)
                ]
                recorder.record_tool_calls_detected(trace_id, model_id, tool_round, tc_shaped)

            # Append the ORIGINAL content object to preserve thought_signature
            # (reconstructing Content loses thought_signature on function_call parts)
            contents.append(candidate.content)

        else:
            # Last round: stream WITHOUT tools to force text output
            # Inject synthesis instruction if we've done tool rounds
            if tool_round > 1:
                contents.append(genai_types.Content(
                    role="user",
                    parts=[genai_types.Part(text=(
                        "你已经进行了充分的搜索和信息收集。"
                        "现在请基于以上所有搜索结果和收集到的数据，给出完整详细的分析报告。"
                        "请确保涵盖所有重要发现，提供深入的分析和明确的结论。"
                    ))],
                ))
                trace.log_sse_event("GEMINI_SYNTHESIS_INJECTED", f"round={tool_round}")
            grounding_metadata = None
            try:
                response = client.models.generate_content_stream(
                    model=GEMINI_NATIVE_MODEL,
                    contents=contents,
                    config=config_no_tools,
                )
                async for chunk in _sync_iter_to_async(response):
                    if chunk.candidates:
                        candidate = chunk.candidates[0]
                        if candidate.content and candidate.content.parts:
                            for part in candidate.content.parts:
                                if getattr(part, "thought", False):
                                    continue
                                if hasattr(part, "text") and part.text:
                                    full_content += part.text
                                    yield {"delta": part.text, "done": False}
                        gm = getattr(candidate, "grounding_metadata", None)
                        if gm:
                            grounding_metadata = gm
                    um = getattr(chunk, "usage_metadata", None)
                    if um:
                        usage_metadata = um
            except Exception as e:
                logger.exception("Gemini streaming error in last round")
                total_tokens = 0
                if usage_metadata:
                    total_tokens = getattr(usage_metadata, "total_token_count", 0) or 0
                yield {
                    "delta": "", "done": True,
                    "error": f"Gemini调用失败: {str(e)[:300]}",
                    "content": full_content, "tokens": total_tokens,
                    "latency_ms": int((time.monotonic() - start) * 1000),
                }
                return

            sources_text = _format_grounding_sources(grounding_metadata)
            if sources_text:
                full_content += sources_text
                yield {"delta": sources_text, "done": False}
            break

        # Execute tools in parallel and build function responses
        # Emit status events for all tools
        for fc in function_calls:
            tn, ar = fc["name"], fc["args"]
            if tn == "web_search":
                yield {"type": "search_status", "query": ar.get("query_cn", "")[:50], "status": "searching"}
            elif tn == "read_webpage":
                yield {"type": "read_status", "url": ar.get("url", "")[:100], "status": "reading"}
            else:
                yield {"type": "tool_status", "tool_name": tn, "status": "calling"}

        # Execute all tools in parallel with heartbeat to keep SSE alive
        tool_futures = asyncio.gather(
            *[dispatch_tool(fc["name"], fc["args"], round_num=tool_round) for fc in function_calls],
            return_exceptions=True,
        )
        results = None
        while results is None:
            try:
                results = await asyncio.wait_for(asyncio.shield(tool_futures), timeout=15.0)
            except asyncio.TimeoutError:
                yield {"type": "heartbeat"}
        if results is None:
            results = await tool_futures

        # Build function response parts and emit done events
        func_response_parts = []
        for fc, result in zip(function_calls, results):
            tn, ar = fc["name"], fc["args"]
            if isinstance(result, Exception):
                logger.warning("Gemini tool %s failed: %s", tn, result)
                result_text = f"Tool error: {str(result)[:200]}"
            else:
                result_text = result

            if tn == "web_search":
                yield {"type": "search_status", "query": ar.get("query_cn", "")[:50], "status": "done"}
            elif tn == "read_webpage":
                yield {"type": "read_status", "url": ar.get("url", "")[:100], "status": "done"}
            else:
                yield {"type": "tool_status", "tool_name": tn, "status": "done"}

            func_response_parts.append(genai_types.Part(
                function_response=genai_types.FunctionResponse(
                    name=tn,
                    response={"result": result_text},
                )
            ))

        contents.append(genai_types.Content(role="user", parts=func_response_parts))
        logger.info("Gemini tool-stream round %d: executed %d tools in parallel", tool_round, len(function_calls))

    # ── Gemini synthesis fallback ─────────────────────────────────
    # Gemini 3.1 Pro with thinking_config occasionally returns an "all-thought"
    # response in a non-last round: it decides not to call more tools, outputs
    # internal reasoning (marked thought=True which we strip), and produces zero
    # visible text. If we exited the loop with empty content despite having
    # actually executed tool calls, force a streaming synthesis call without tools.
    _gemini_tools_ran = len(activity["tool_call_names"]) > 0
    if len(full_content) == 0 and _gemini_tools_ran:
        trace.log_sse_event(
            "GEMINI_SYNTHESIS_FALLBACK",
            f"full_content=0, rebuilding via streaming synthesis",
        )
        logger.info("Gemini synthesis fallback: empty content after %d rounds, rebuilding",
                    activity["rounds_used"])
        try:
            # Inject explicit synthesis prompt and switch to streaming (which
            # produces text reliably even when thinking_config is active).
            contents.append(genai_types.Content(
                role="user",
                parts=[genai_types.Part(text=(
                    "你已经进行了充分的搜索和信息收集。"
                    "现在请基于以上所有搜索结果和收集到的数据，给出完整详细的分析报告。"
                    "请确保涵盖所有重要发现，提供深入的分析和明确的结论。"
                    "引用时使用 [N] 行内引用（编号与上方工具结果中的 [N] 对齐）。"
                    "请直接输出报告正文，不要再调用任何工具。"
                ))],
            ))
            syn_response = client.models.generate_content_stream(
                model=GEMINI_NATIVE_MODEL,
                contents=contents,
                config=config_no_tools,
            )
            syn_added = 0
            async for chunk in _sync_iter_to_async(syn_response):
                if chunk.candidates:
                    cand = chunk.candidates[0]
                    if cand.content and cand.content.parts:
                        for part in cand.content.parts:
                            if getattr(part, "thought", False):
                                continue
                            if hasattr(part, "text") and part.text:
                                full_content += part.text
                                syn_added += len(part.text)
                                yield {"delta": part.text, "done": False}
                um = getattr(chunk, "usage_metadata", None)
                if um:
                    usage_metadata = um
            trace.log_sse_event("GEMINI_SYNTHESIS_DONE", f"added {syn_added} chars")
        except Exception as e:
            logger.exception("Gemini synthesis fallback failed")
            trace.log_llm_error(f"Gemini synthesis fallback exception: {e}")

    # Emit citation sources
    if citation_tracker.sources:
        yield {"type": "sources", "sources": citation_tracker.sources}

    latency_ms = int((time.monotonic() - start) * 1000)
    total_tokens = 0
    if usage_metadata:
        total_tokens = getattr(usage_metadata, "total_token_count", 0) or 0
    trace.log_llm_done(
        content_len=len(full_content), tokens=total_tokens,
        latency_ms=latency_ms,
    )
    trace.log_llm_response_content(full_content)

    urls_searched_all = [s.get("url", "") for s in citation_tracker.sources if s.get("url")]
    trace.log_request_summary(
        rounds_used=activity["rounds_used"],
        tool_calls_total=len(activity["tool_call_names"]),
        tool_call_names=activity["tool_call_names"],
        search_queries=activity["search_queries"],
        urls_searched=urls_searched_all,
        urls_read=activity["urls_read"],
        citations_count=len(citation_tracker.sources),
        final_content_len=len(full_content),
        total_tokens=total_tokens,
    )
    trace.log_full_response(full_content)

    yield {
        "delta": "", "done": True,
        "content": full_content, "tokens": total_tokens,
        "latency_ms": latency_ms,
    }


async def generate_title(first_message: str) -> str:
    """Auto-generate a conversation title from the first user message."""
    try:
        result = await call_model_sync(
            "openai/gpt-4o-mini",
            [
                {"role": "system", "content": "用10个字以内的中文短语总结用户的问题主题，不加标点。"},
                {"role": "user", "content": first_message[:500]},
            ],
            max_tokens=64,
        )
        title = result.get("content", "").strip()
        return title[:50] if title else "新对话"
    except Exception:
        return first_message[:30] + "..." if len(first_message) > 30 else first_message
