"""OpenRouter LLM service for multi-model chat with streaming support."""
from __future__ import annotations

import asyncio
import base64
import json
import logging
import time
from pathlib import Path
from typing import AsyncIterator

import httpx

from backend.app.config import get_settings

logger = logging.getLogger(__name__)

# ── Available models via OpenRouter ─────────────────────────────

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
        "description": "Google最新旗舰，强大的多模态与推理能力",
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

# ── Mode configurations ─────────────────────────────────────────

MODE_CONFIGS = {
    "standard": {
        "temperature": 0.7,
        "max_tokens": 4096,
        "label": "标准模式",
    },
    "thinking": {
        "temperature": 0.3,
        "max_tokens": 16384,
        "label": "深度思考",
    },
    "fast": {
        "temperature": 0.5,
        "max_tokens": 2048,
        "label": "快速模式",
    },
}

MODEL_MAP = {m["id"]: m for m in AVAILABLE_MODELS}

OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"


def get_api_key() -> str:
    settings = get_settings()
    return settings.openrouter_api_key


def _build_headers() -> dict:
    return {
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


def _build_request_body(model_id: str, messages: list[dict], mode: str = "standard", stream: bool = True) -> dict:
    """Build API request body with mode-specific parameters."""
    cfg = MODE_CONFIGS.get(mode, MODE_CONFIGS["standard"])
    body: dict = {
        "model": model_id,
        "messages": messages,
        "stream": stream,
        "max_tokens": cfg["max_tokens"],
        "temperature": cfg["temperature"],
    }

    # Thinking mode: add extended thinking / reasoning parameters
    if mode == "thinking":
        model_info = MODEL_MAP.get(model_id, {})
        if model_info.get("supports_thinking"):
            # OpenRouter supports provider-specific reasoning params
            body["reasoning"] = {"effort": "high"}
            # For Anthropic models, enable extended thinking via transforms
            if "anthropic" in model_id:
                body["transforms"] = ["middle-out"]
                body["temperature"] = 1  # Anthropic thinking requires temperature=1

    return body


async def call_model_stream(
    model_id: str,
    messages: list[dict],
    mode: str = "standard",
) -> AsyncIterator[dict]:
    """Stream responses from a single model via OpenRouter.

    Yields dicts: {"delta": "...", "done": False} or
                  {"delta": "", "done": True, "tokens": N, "latency_ms": N, "content": "full text"}
    """
    start = time.monotonic()
    full_content = ""
    tokens_used = 0

    try:
        timeout_val = 180.0 if mode == "thinking" else 120.0
        async with httpx.AsyncClient(timeout=httpx.Timeout(timeout_val, connect=15.0)) as client:
            async with client.stream(
                "POST",
                f"{OPENROUTER_BASE_URL}/chat/completions",
                headers=_build_headers(),
                json=_build_request_body(model_id, messages, mode, stream=True),
            ) as response:
                if response.status_code != 200:
                    body = await response.aread()
                    error_text = body.decode("utf-8", errors="replace")
                    yield {
                        "delta": "",
                        "done": True,
                        "error": f"API错误 ({response.status_code}): {error_text[:200]}",
                        "content": "",
                        "tokens": 0,
                        "latency_ms": int((time.monotonic() - start) * 1000),
                    }
                    return

                async for line in response.aiter_lines():
                    if not line.startswith("data: "):
                        continue
                    data = line[6:]
                    if data.strip() == "[DONE]":
                        break
                    try:
                        chunk = json.loads(data)
                        delta = chunk.get("choices", [{}])[0].get("delta", {})
                        text = delta.get("content", "")
                        if text:
                            full_content += text
                            yield {"delta": text, "done": False}
                        # Check for usage info
                        if "usage" in chunk:
                            tokens_used = chunk["usage"].get("total_tokens", 0)
                    except json.JSONDecodeError:
                        continue

    except httpx.TimeoutException:
        yield {
            "delta": "",
            "done": True,
            "error": "请求超时，请稍后重试",
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
    yield {
        "delta": "",
        "done": True,
        "content": full_content,
        "tokens": tokens_used,
        "latency_ms": latency_ms,
    }


async def call_model_sync(
    model_id: str,
    messages: list[dict],
    mode: str = "standard",
) -> dict:
    """Non-streaming call to a model. Returns full response."""
    start = time.monotonic()
    try:
        timeout_val = 180.0 if mode == "thinking" else 120.0
        async with httpx.AsyncClient(timeout=httpx.Timeout(timeout_val, connect=15.0)) as client:
            resp = await client.post(
                f"{OPENROUTER_BASE_URL}/chat/completions",
                headers=_build_headers(),
                json=_build_request_body(model_id, messages, mode, stream=False),
            )
            latency_ms = int((time.monotonic() - start) * 1000)
            if resp.status_code != 200:
                return {
                    "content": "",
                    "error": f"API错误 ({resp.status_code}): {resp.text[:200]}",
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


async def generate_title(first_message: str) -> str:
    """Auto-generate a conversation title from the first user message."""
    try:
        result = await call_model_sync(
            "openai/gpt-4o-mini",
            [
                {"role": "system", "content": "用10个字以内的中文短语总结用户的问题主题，不加标点。"},
                {"role": "user", "content": first_message[:500]},
            ],
        )
        title = result.get("content", "").strip()
        return title[:50] if title else "新对话"
    except Exception:
        return first_message[:30] + "..." if len(first_message) > 30 else first_message
