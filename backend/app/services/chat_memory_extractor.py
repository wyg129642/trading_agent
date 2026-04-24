"""LLM-powered extraction of durable user memories from chat feedback.

Industry-standard "reflection" pattern (as in MemGPT / Letta / Mem0 / A-MEM):
a background LLM reads the user's message, the assistant's response, and the
user's feedback on that response, and produces a small set of typed,
deduplicable memories that the system should carry forward.

Design choices:
  * Uses the enrichment LLM (Qwen-Plus via `llm_enrichment_*` settings) so the
    user's own chat keys are not spent on background work.
  * Returns strict JSON with a constrained schema. We parse defensively —
    the extractor short-circuits and returns [] rather than letting a malformed
    LLM response crash the daemon.
  * memory_key is the dedup handle. The prompt forces short snake_case keys
    ≤120 chars. The caller upserts on (user_id, memory_key).
  * No extraction for responses with rating ≥4 AND no feedback_text AND no
    tags — there's nothing to learn that isn't already positive noise.
"""
from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from typing import Any

import httpx
from openai import AsyncOpenAI

from backend.app.config import Settings
from backend.app.models.chat_memory import (
    FEEDBACK_SENTIMENTS, MEMORY_TYPES,
)

logger = logging.getLogger(__name__)


# Max LLM-extractable memories per event. Limits cost + prevents hallucination
# runaway where a single feedback produces 20 "memories".
MAX_MEMORIES_PER_EVENT = 5

# Content trimming — LLM budget.
_MAX_USER_MSG_CHARS = 2000
_MAX_ASSISTANT_RESP_CHARS = 3000
_MAX_FEEDBACK_CHARS = 1000


EXTRACTION_SYSTEM_PROMPT = """你是"AI投研助手"的记忆提炼专家。用户会对助手的回答进行评价，你需要从这次交互中提炼出**对未来对话有长期价值**的用户画像/偏好/纠偏信号。

## 输出严格遵守 JSON 格式（只输出 JSON，不要任何额外文字、注释、代码块标记）：

```
{
  "memories": [
    {
      "memory_type": "preference | style | profile | topic_interest | domain_knowledge | correction",
      "memory_key": "短 snake_case 稳定标识（≤100 字符，用于去重，同一个事实永远同 key，如 prefers_concise_bullets / tracks_ai_capex_supply_chain / user_is_a_share_fundamental_analyst）",
      "content": "一句话自然语言陈述（20-120 字），面向未来助手第一人称可读，例如 '用户偏好结论在前、分条列表的简洁回答' ",
      "confidence": 0.0-1.0,
      "rationale": "为什么可以从本次反馈推出这条记忆（可选，≤80 字）"
    }
  ],
  "sentiment": "positive | negative | neutral | mixed"
}
```

## 提炼规则（非常重要）：

1. **只在确有信号时提炼**。如果用户打 5 星但无文字反馈，通常不产生新记忆（除非内容明显透露画像）。
2. **一次最多 %d 条**。宁缺毋滥，没有信号就返回 `"memories": []`。
3. **六类记忆的含义**：
   - `preference`: 用户喜欢的回答结构/格式（"prefers_tables_for_comparisons"）
   - `style`: 语气、篇幅、语言（"prefers_concise_chinese_answers"）
   - `profile`: 用户是谁（"is_fundamental_analyst_on_a_shares"）
   - `topic_interest`: 长期关注的主题（"actively_tracks_ai_capex"）
   - `domain_knowledge`: 用户的事实上下文（"holds_positions_in_nvda_smci"）
   - `correction`: 用户指出的错误模式，要避免（"avoid_wsj_for_china_macro"）
4. **不要提炼**：
   - 本次对话特定事实（"上周聊过英伟达"——这只是历史，不是记忆）
   - 过度泛化（"用户喜欢准确的答案"——所有人都如此）
   - 助手错误细节（"上次回答写错了 2023 年营收"——这是单次错误不是模式）
5. **memory_key 必须稳定**：同样的偏好应该永远产生同样的 key。
6. **置信度校准**：
   - 用户明确说"以后都这样"/"我不喜欢 X" → 0.85-0.95
   - 用户口气隐含 → 0.6-0.75
   - 只是打分推断 → 0.4-0.55
7. **sentiment**：基于反馈文本+评分综合判断。
"""

EXTRACTION_SYSTEM_PROMPT = EXTRACTION_SYSTEM_PROMPT % MAX_MEMORIES_PER_EVENT


@dataclass
class ExtractionInput:
    """Everything the extractor needs to distill a memory from one event."""
    user_message: str
    assistant_response: str
    model_name: str
    rating: int | None
    feedback_tags: list[str]
    feedback_text: str


@dataclass
class ExtractedMemory:
    memory_type: str
    memory_key: str
    content: str
    confidence: float
    rationale: str = ""


@dataclass
class ExtractionResult:
    memories: list[ExtractedMemory] = field(default_factory=list)
    sentiment: str = "neutral"
    raw_response: str = ""
    error: str | None = None


def _sentiment_from_signals(
    rating: int | None, feedback_text: str, tags: list[str],
) -> str:
    """Deterministic fallback sentiment when the LLM output is unusable."""
    negative_tags = {
        "too_long", "outdated", "off_topic", "wrong", "biased", "unclear",
        "missing_sources", "hallucinated",
    }
    positive_tags = {
        "accurate", "clear", "helpful", "concise", "relevant", "comprehensive",
        "well_sourced",
    }
    if tags:
        neg = len(set(tags) & negative_tags)
        pos = len(set(tags) & positive_tags)
        if neg and not pos:
            return "negative"
        if pos and not neg:
            return "positive"
        if neg and pos:
            return "mixed"
    if rating is not None:
        if rating >= 4:
            return "positive"
        if rating <= 2:
            return "negative"
    if feedback_text.strip():
        return "mixed"
    return "neutral"


def _strip_think_blocks(text: str) -> str:
    """Remove <think>...</think> blocks and markdown fences common on Qwen output."""
    text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL).strip()
    text = re.sub(r"<think>.*", "", text, flags=re.DOTALL).strip()
    m = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
    if m:
        return m.group(1)
    return text


def _extract_json_object(text: str) -> dict | None:
    """Best-effort JSON extraction from LLM output."""
    text = _strip_think_blocks(text)
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    depth = 0
    start = -1
    for i, ch in enumerate(text):
        if ch == "{":
            if depth == 0:
                start = i
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0 and start >= 0:
                try:
                    return json.loads(text[start:i + 1])
                except json.JSONDecodeError:
                    start = -1
    return None


def _coerce_str(value: Any, limit: int) -> str:
    if not isinstance(value, str):
        return ""
    return value.strip()[:limit]


def _coerce_float(value: Any, default: float = 0.5) -> float:
    try:
        f = float(value)
    except (TypeError, ValueError):
        return default
    return max(0.0, min(1.0, f))


def _parse_extraction_result(
    raw: str,
    fallback_sentiment: str,
) -> ExtractionResult:
    data = _extract_json_object(raw)
    if not data:
        return ExtractionResult(
            memories=[], sentiment=fallback_sentiment, raw_response=raw,
            error="parse_failed",
        )
    memories: list[ExtractedMemory] = []
    for item in (data.get("memories") or [])[:MAX_MEMORIES_PER_EVENT]:
        if not isinstance(item, dict):
            continue
        memory_type = _coerce_str(item.get("memory_type"), 40)
        memory_key = _coerce_str(item.get("memory_key"), 120)
        content = _coerce_str(item.get("content"), 600)
        if memory_type not in MEMORY_TYPES:
            continue
        if not memory_key or not content:
            continue
        # Normalize key: lowercase, alnum + underscores only, collapse runs
        memory_key = re.sub(r"[^a-z0-9_]+", "_", memory_key.lower()).strip("_")
        memory_key = re.sub(r"_+", "_", memory_key)
        if not memory_key:
            continue
        memory_key = memory_key[:120]
        memories.append(ExtractedMemory(
            memory_type=memory_type,
            memory_key=memory_key,
            content=content,
            confidence=_coerce_float(item.get("confidence"), 0.6),
            rationale=_coerce_str(item.get("rationale"), 240),
        ))
    sentiment = _coerce_str(data.get("sentiment"), 20) or fallback_sentiment
    if sentiment not in FEEDBACK_SENTIMENTS:
        sentiment = fallback_sentiment
    return ExtractionResult(
        memories=memories,
        sentiment=sentiment,
        raw_response=raw,
    )


def _build_user_prompt(inp: ExtractionInput) -> str:
    tag_line = ", ".join(inp.feedback_tags) if inp.feedback_tags else "(无)"
    rating_line = f"{inp.rating}/5" if inp.rating is not None else "(未打分)"
    feedback = inp.feedback_text.strip() or "(无详细文字反馈)"
    user_msg = (inp.user_message or "").strip()[:_MAX_USER_MSG_CHARS]
    assistant_resp = (inp.assistant_response or "").strip()[:_MAX_ASSISTANT_RESP_CHARS]
    feedback = feedback[:_MAX_FEEDBACK_CHARS]
    return (
        "## 本次交互\n\n"
        f"### 用户提问\n{user_msg}\n\n"
        f"### 助手回答（模型 {inp.model_name}）\n{assistant_resp}\n\n"
        f"## 用户反馈\n"
        f"- 评分: {rating_line}\n"
        f"- 标签: {tag_line}\n"
        f"- 文字反馈: {feedback}\n\n"
        "请严格按系统指令返回 JSON。"
    )


class ChatMemoryExtractor:
    """Thin wrapper around the enrichment LLM for memory extraction.

    Safe to construct many times; holds one pooled httpx client per instance.
    Caller should create one extractor per daemon instance and reuse.
    """

    def __init__(self, settings: Settings):
        self.settings = settings
        # Dashscope (Aliyun) uses a CN endpoint — the Clash HTTP_PROXY in env
        # would misroute. trust_env=False makes httpx ignore HTTP_PROXY.
        self._http = httpx.AsyncClient(trust_env=False, timeout=60.0)
        self.llm = AsyncOpenAI(
            api_key=settings.llm_enrichment_api_key,
            base_url=settings.llm_enrichment_base_url,
            timeout=60.0,
            http_client=self._http,
        )
        self.model = settings.llm_enrichment_model

    @property
    def is_configured(self) -> bool:
        return bool(self.settings.llm_enrichment_api_key)

    async def extract(self, inp: ExtractionInput) -> ExtractionResult:
        """Run one LLM call; never raise — always return an ExtractionResult."""
        fallback = _sentiment_from_signals(
            inp.rating, inp.feedback_text, inp.feedback_tags,
        )
        if not self.is_configured:
            return ExtractionResult(
                memories=[], sentiment=fallback, raw_response="",
                error="llm_not_configured",
            )
        try:
            resp = await self.llm.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": EXTRACTION_SYSTEM_PROMPT},
                    {"role": "user", "content": _build_user_prompt(inp)},
                ],
                temperature=0.2,
                max_tokens=800,
            )
            raw = resp.choices[0].message.content or ""
        except Exception as exc:
            logger.exception("chat_memory_extractor: LLM call failed")
            return ExtractionResult(
                memories=[], sentiment=fallback, raw_response="",
                error=f"llm_error: {exc!r}"[:500],
            )
        return _parse_extraction_result(raw, fallback)

    async def aclose(self) -> None:
        try:
            await self._http.aclose()
        except Exception:
            pass
