"""Multi-provider LLM client with token tracking and web search support.

Supported providers:
  - zhipu: Zhipu AI (GLM series) via zai-sdk
  - minimax: MiniMax via OpenAI-compatible endpoint
  - openrouter: OpenRouter via OpenAI-compatible endpoint
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from typing import Any

from openai import AsyncOpenAI

from engine.utils.token_tracker import TokenTracker, estimate_messages_tokens

logger = logging.getLogger(__name__)

# Retry configuration for transient API errors
_MAX_RETRIES = 3
_RETRY_BASE_DELAY = 2.0  # seconds (exponential backoff: 2, 4, 8)
_RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

# Providers that use OpenAI-compatible SDK
_OPENAI_COMPATIBLE_PROVIDERS = {"minimax", "openrouter"}


def _create_provider_client(llm_cfg: dict):
    """Create the appropriate client based on provider setting.

    Returns (client, provider_name, is_openai_compatible).
    """
    provider = llm_cfg.get("provider", "zhipu").lower()

    if provider == "zhipu":
        from zai import ZhipuAiClient
        client = ZhipuAiClient(api_key=llm_cfg["api_key"])
        return client, provider, False

    elif provider == "minimax":
        client = AsyncOpenAI(
            api_key=llm_cfg["api_key"],
            base_url=llm_cfg.get("base_url", "https://api.minimaxi.com/v1"),
            timeout=llm_cfg.get("timeout", 60),
        )
        return client, provider, True

    elif provider == "openrouter":
        client = AsyncOpenAI(
            api_key=llm_cfg["api_key"],
            base_url=llm_cfg.get("base_url", "https://openrouter.ai/api/v1"),
            timeout=llm_cfg.get("timeout", 120),
        )
        return client, provider, True

    else:
        # Treat unknown providers as OpenAI-compatible if base_url is given
        if llm_cfg.get("base_url"):
            client = AsyncOpenAI(
                api_key=llm_cfg["api_key"],
                base_url=llm_cfg["base_url"],
                timeout=llm_cfg.get("timeout", 60),
            )
            return client, provider, True
        raise ValueError(f"Unknown LLM provider: {provider}. "
                         f"Supported: zhipu, minimax, openrouter (or any with base_url)")


class LLMClient:
    """Async multi-provider LLM client with token tracking.

    Provider dispatch:
      - Zhipu: uses zai-sdk (synchronous, wrapped in asyncio.to_thread)
      - MiniMax/OpenRouter/others: uses openai AsyncOpenAI with custom base_url
    """

    def __init__(self, settings: dict, tracker: TokenTracker | None = None):
        llm_cfg = settings["llm"]
        self.client, self.provider, self._is_openai_compat = _create_provider_client(llm_cfg)

        self.model_filter = llm_cfg.get("model_filter", "glm-4-flash")
        self.model_analyzer = llm_cfg.get("model_analyzer", "glm-4-air")
        self.model_researcher = llm_cfg.get("model_researcher", "glm-4-air")
        self.max_tokens_filter = llm_cfg.get("max_tokens_filter", 300)
        self.max_tokens_analyzer = llm_cfg.get("max_tokens_analyzer", 2000)
        self.max_tokens_researcher = llm_cfg.get("max_tokens_researcher", 4000)
        self.temp_filter = llm_cfg.get("temperature_filter", 0.1)
        self.temp_analyzer = llm_cfg.get("temperature_analyzer", 0.3)
        self.temp_researcher = llm_cfg.get("temperature_researcher", 0.4)
        self.tracker = tracker

        logger.info("[LLM] Provider: %s | Filter: %s | Analyzer: %s | Researcher: %s",
                     self.provider, self.model_filter, self.model_analyzer, self.model_researcher)

    # ── Low-level API call dispatch ──────────────────────────────────

    async def _call_api(self, **kwargs) -> Any:
        """Dispatch API call to the right client with automatic retry.

        Retries up to _MAX_RETRIES times on transient errors (5xx, 429, timeouts).
        Uses exponential backoff between retries.
        """
        last_exc = None
        for attempt in range(_MAX_RETRIES):
            try:
                if self._is_openai_compat:
                    result = await self.client.chat.completions.create(**kwargs)
                else:
                    result = await asyncio.to_thread(
                        self.client.chat.completions.create, **kwargs
                    )
                return result
            except Exception as e:
                last_exc = e
                error_str = str(e)
                # Check if this is a retryable error
                is_retryable = False
                if "timeout" in error_str.lower() or "timed out" in error_str.lower():
                    is_retryable = True
                elif any(f"{code}" in error_str for code in _RETRYABLE_STATUS_CODES):
                    is_retryable = True
                elif "server_error" in error_str.lower():
                    is_retryable = True
                elif "connection" in error_str.lower():
                    is_retryable = True

                if is_retryable and attempt < _MAX_RETRIES - 1:
                    delay = _RETRY_BASE_DELAY * (2 ** attempt)
                    logger.warning(
                        "[LLM] Transient error (attempt %d/%d), retrying in %.1fs: %s",
                        attempt + 1, _MAX_RETRIES, delay, error_str[:120],
                    )
                    await asyncio.sleep(delay)
                else:
                    raise
        raise last_exc  # Should not reach here, but just in case

    # ── Main chat method ─────────────────────────────────────────────

    async def chat(
        self,
        system_prompt: str,
        user_prompt: str,
        model: str | None = None,
        max_tokens: int = 1024,
        temperature: float = 0.3,
        tools: list[dict] | None = None,
        stage: str = "other",
        source_name: str = "",
        web_search_config: dict | None = None,
        capture_prompts: bool = False,
    ) -> dict[str, Any]:
        """Send a chat completion request and return parsed JSON response.

        Returns a dict with 'content' (str), 'parsed' (dict or None),
        'tool_calls' (list or None), 'usage' (dict), and 'web_search' (list or None).

        Args:
            web_search_config: If provided, enables Zhipu's built-in web_search tool.
                Only effective for Zhipu provider. Ignored for other providers.
        """
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]

        # Pre-call token estimation
        estimated = estimate_messages_tokens(messages)

        # Rate limit check
        if self.tracker:
            if self.tracker.is_over_budget():
                logger.warning("[LLM] Daily budget exceeded — skipping call")
                return {"content": "", "parsed": None, "tool_calls": None, "usage": {}, "web_search": None}
            await self.tracker.wait_for_rate_limit()

        used_model = model or self.model_analyzer

        # Build tools list
        all_tools = []
        if tools:
            all_tools.extend(tools)
        # Zhipu-specific web_search tool (not supported by OpenAI-compat providers)
        if web_search_config and self.provider == "zhipu":
            all_tools.append({
                "type": "web_search",
                "web_search": web_search_config,
            })

        kwargs: dict[str, Any] = {
            "model": used_model,
            "messages": messages,
            "max_tokens": max_tokens,
            "temperature": temperature,
        }
        if all_tools:
            kwargs["tools"] = all_tools
            if tools:  # Only set tool_choice if there are function tools
                kwargs["tool_choice"] = "auto"

        t0 = time.monotonic()
        try:
            response = await self._call_api(**kwargs)
            duration_ms = int((time.monotonic() - t0) * 1000)

            if not response.choices:
                logger.warning("[LLM] API returned empty choices list")
                return {"content": "", "parsed": None, "tool_calls": None, "usage": {}, "web_search": None}
            choice = response.choices[0]
            content = choice.message.content or ""
            tool_calls = None

            if choice.message.tool_calls:
                tool_calls = []
                for tc in choice.message.tool_calls:
                    if tc.type == "function":
                        try:
                            args = json.loads(tc.function.arguments)
                        except (json.JSONDecodeError, TypeError):
                            logger.warning("[LLM] Failed to parse tool_call arguments: %s", tc.function.arguments[:200])
                            args = {}
                        tool_calls.append({
                            "id": tc.id,
                            "name": tc.function.name,
                            "arguments": args,
                        })

            parsed = self._extract_json(content)

            prompt_tok = response.usage.prompt_tokens if response.usage else 0
            completion_tok = response.usage.completion_tokens if response.usage else 0

            # Extract Zhipu web_search results (Zhipu-specific extra field)
            web_search_results = None
            if web_search_config and self.provider == "zhipu":
                web_search_results = self._extract_web_search(response, choice)

            # Record usage
            if self.tracker:
                await self.tracker.record(
                    model=used_model,
                    stage=stage,
                    prompt_tokens=prompt_tok,
                    completion_tokens=completion_tok,
                    estimated_prompt=estimated,
                    source_name=source_name,
                    duration_ms=duration_ms,
                )

            result = {
                "content": content,
                "parsed": parsed,
                "tool_calls": tool_calls,
                "usage": {
                    "prompt_tokens": prompt_tok,
                    "completion_tokens": completion_tok,
                },
                "web_search": web_search_results,
            }
            if capture_prompts:
                result["_system_prompt"] = system_prompt
                result["_user_prompt"] = user_prompt
            return result
        except Exception as e:
            logger.error("LLM API call failed: %s", e)
            return {"content": "", "parsed": None, "tool_calls": None, "usage": {}, "web_search": None}

    # ── Multi-turn tool-use loop (ReAct) ─────────────────────────────

    async def chat_with_tools_loop(
        self,
        system_prompt: str,
        user_prompt: str,
        tools_def: list[dict],
        tool_executor,
        model: str | None = None,
        max_tokens: int = 4000,
        temperature: float = 0.4,
        max_iterations: int = 5,
        stage: str = "researcher",
        source_name: str = "",
        web_search_config: dict | None = None,
        capture_prompts: bool = False,
    ) -> dict[str, Any]:
        """Run a multi-turn tool-use conversation loop (ReAct pattern).

        tool_executor: async callable(name, arguments) -> str
        Collects web_search results across iterations when web_search_config is provided (Zhipu only).
        """
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]

        # Build tools: combine function tools + web_search if Zhipu
        all_tools = list(tools_def)
        if web_search_config and self.provider == "zhipu":
            all_tools.append({
                "type": "web_search",
                "web_search": web_search_config,
            })

        used_model = model or self.model_researcher
        final_content = ""
        total_usage = {"prompt_tokens": 0, "completion_tokens": 0}
        total_t0 = time.monotonic()
        all_web_search_results = []

        for iteration in range(max_iterations):
            # Rate limit check each iteration
            if self.tracker:
                if self.tracker.is_over_budget():
                    logger.warning("[LLM] Daily budget exceeded — stopping research loop")
                    break
                await self.tracker.wait_for_rate_limit()

            try:
                kwargs: dict[str, Any] = {
                    "model": used_model,
                    "messages": messages,
                    "max_tokens": max_tokens,
                    "temperature": temperature,
                    "tools": all_tools,
                    "tool_choice": "auto",
                }
                response = await self._call_api(**kwargs)
                if not response.choices:
                    logger.warning("[LLM] API returned empty choices in tool loop iteration %d", iteration)
                    break
                choice = response.choices[0]

                if response.usage:
                    total_usage["prompt_tokens"] += response.usage.prompt_tokens
                    total_usage["completion_tokens"] += response.usage.completion_tokens

                # Collect web_search results from this iteration (Zhipu only)
                if web_search_config and self.provider == "zhipu":
                    ws = self._extract_web_search(response, choice)
                    if ws:
                        all_web_search_results.extend(ws)

                # Build assistant message dict
                assistant_msg = {
                    "role": "assistant",
                    "content": choice.message.content or "",
                }
                if choice.message.tool_calls:
                    if self._is_openai_compat:
                        # OpenAI SDK returns objects with model_dump()
                        assistant_msg["tool_calls"] = [
                            tc.model_dump() for tc in choice.message.tool_calls
                        ]
                    else:
                        # Zhipu — manual dict construction
                        assistant_msg["tool_calls"] = [
                            {
                                "id": tc.id,
                                "type": tc.type,
                                "function": {
                                    "name": tc.function.name,
                                    "arguments": tc.function.arguments,
                                },
                            }
                            for tc in choice.message.tool_calls
                        ]
                messages.append(assistant_msg)

                # Check for function tool calls (skip web_search type)
                func_tool_calls = [
                    tc for tc in (choice.message.tool_calls or [])
                    if tc.type == "function"
                ]

                if func_tool_calls:
                    for tc in func_tool_calls:
                        func_name = tc.function.name
                        func_args = json.loads(tc.function.arguments)
                        logger.info(
                            "[Research Agent] Calling tool: %s(%s)", func_name, func_args
                        )

                        try:
                            result = await tool_executor(func_name, func_args)
                        except Exception as e:
                            result = f"Tool execution error: {e}"

                        messages.append({
                            "role": "tool",
                            "tool_call_id": tc.id,
                            "content": str(result)[:3000],
                        })
                else:
                    final_content = choice.message.content or ""
                    break

                if choice.finish_reason == "stop":
                    final_content = choice.message.content or ""
                    break

            except Exception as e:
                logger.error("[Research Agent] Iteration %d failed: %s", iteration, e)
                break

        # If loop exhausted without a final answer, capture from last assistant msg
        if not final_content.strip():
            for msg in reversed(messages):
                if msg.get("role") == "assistant" and msg.get("content", "").strip():
                    final_content = msg["content"]
                    break

        # Check if final_content has valid JSON; if not, force one more call
        parsed_check = self._extract_json(final_content) if final_content.strip() else None
        if parsed_check is None:
            logger.info(
                "[Research Agent] No valid JSON after tool loop (content=%d chars), forcing final answer",
                len(final_content),
            )
            tool_results_summary = []
            for msg in messages:
                if msg.get("role") == "tool":
                    content = msg.get("content", "")
                    tool_results_summary.append(content[:500])

            tool_context = "\n---\n".join(tool_results_summary) if tool_results_summary else "All tool calls failed, no valid data obtained."

            force_messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": (
                    f"{user_prompt}\n\n"
                    f"【Tool call results summary】\n{tool_context}\n\n"
                    "Based on all available information (including tool results), output the final JSON research report. "
                    "If all tool calls failed, analyze based on your existing knowledge and the news content, "
                    "and reflect the lack of data support in the confidence field."
                )},
            ]
            try:
                if self.tracker:
                    await self.tracker.wait_for_rate_limit()
                final_resp = await self._call_api(
                    model=used_model,
                    messages=force_messages,
                    max_tokens=max_tokens,
                    temperature=temperature,
                )
                if not final_resp.choices:
                    logger.warning("[Research Agent] Forced final answer returned empty choices")
                    raise ValueError("Empty choices in forced final answer")
                final_choice = final_resp.choices[0]
                forced_content = final_choice.message.content or ""
                if final_resp.usage:
                    total_usage["prompt_tokens"] += final_resp.usage.prompt_tokens
                    total_usage["completion_tokens"] += final_resp.usage.completion_tokens
                if forced_content.strip():
                    final_content = forced_content
                    logger.info(
                        "[Research Agent] Forced final answer: %d chars", len(final_content)
                    )
                messages.append({"role": "user", "content": "[System] Force-generated final report"})
                messages.append({"role": "assistant", "content": final_content})
            except Exception as e:
                logger.error("[Research Agent] Final answer call failed: %s", e)

        # Record cumulative usage for the full research loop
        total_duration = int((time.monotonic() - total_t0) * 1000)
        if self.tracker:
            await self.tracker.record(
                model=used_model,
                stage=stage,
                prompt_tokens=total_usage["prompt_tokens"],
                completion_tokens=total_usage["completion_tokens"],
                estimated_prompt=estimate_messages_tokens(messages[:2]),
                source_name=source_name,
                duration_ms=total_duration,
            )

        parsed = self._extract_json(final_content)
        result = {
            "content": final_content,
            "parsed": parsed,
            "usage": total_usage,
            "web_search": all_web_search_results if all_web_search_results else None,
        }
        if capture_prompts:
            result["_system_prompt"] = system_prompt
            result["_user_prompt"] = user_prompt
            result["_full_messages"] = messages
        return result

    # ── Zhipu-specific helpers ───────────────────────────────────────

    def _extract_web_search(self, response, choice) -> list[dict] | None:
        """Extract Zhipu web_search results from the response.

        Zhipu returns web_search results in response.web_search as a list of dicts.
        Each item has: title, link, content, publish_date, media, icon, refer
        """
        results = []

        ws_data = getattr(response, "web_search", None)
        if ws_data and isinstance(ws_data, list):
            for item in ws_data:
                if isinstance(item, dict):
                    results.append(item)
                else:
                    try:
                        results.append(item.model_dump() if hasattr(item, "model_dump") else vars(item))
                    except Exception:
                        pass

        return results if results else None

    # ── JSON extraction ──────────────────────────────────────────────

    def _extract_json(self, text: str) -> dict | None:
        """Extract JSON from LLM response text, handling markdown code blocks."""
        if not text:
            return None

        # Try parsing the whole text as JSON
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass

        # Try extracting from ```json ... ``` code block
        json_match = re.search(r"```(?:json)?\s*\n?(.*?)\n?```", text, re.DOTALL)
        if json_match:
            try:
                return json.loads(json_match.group(1))
            except json.JSONDecodeError:
                pass

        # Try finding first { ... } block
        brace_match = re.search(r"\{.*\}", text, re.DOTALL)
        if brace_match:
            try:
                return json.loads(brace_match.group(0))
            except json.JSONDecodeError:
                pass

        return None
