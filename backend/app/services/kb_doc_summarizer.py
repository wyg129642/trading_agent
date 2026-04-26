"""Long-document summarization subagent for kb_fetch_document.

When ``kb_fetch_document`` pulls back a long document (> ~5 pages of CJK text),
the chat LLM can get overwhelmed trying to skim it while also synthesising the
final answer. This module spins up a cheap summarization model (Claude Haiku
by default, fallback to OpenAI/Gemini) with the user's original query as the
pivot and prepends a tight bullet list to the full text.

Design principles
-----------------
- **Never block the chat path.** Any failure (network, rate-limit, parse error)
  degrades to returning the raw document without a summary.
- **Pivot on the user's intent, not the document's headline.** The summary
  prompt is always "given the user asked X, extract from this doc the facts
  that answer X" — a generic TLDR would waste the extra API call.
- **Stay cheap.** Haiku-class model, ~600-1500 char output, one round trip.
  At the default threshold (10k chars in → ~1k chars summary), expected cost
  is on the order of $0.001-$0.003 per fetch.
- **Single entry point.** ``maybe_summarize(res, mode, trace)`` takes the
  already-populated fetch_document result and mutates its ``text`` field to
  ``[摘要]\\n...\\n[原文]\\n...``; returns the same dict so callers can chain.
"""
from __future__ import annotations

import logging
import time
from typing import Any

logger = logging.getLogger(__name__)


# Heuristic: ~2000 CJK chars per page → 5 pages ≈ 10000 chars.
_LONG_DOC_THRESHOLD = 10_000

# Hard cap on the raw text we ship to the summarizer. Longer than this and we
# truncate head+tail to keep the prompt tokens under control.
_SUMMARIZER_INPUT_CAP = 40_000

# Which model to use for the summarization pass. Haiku is the cheapest +
# fastest capable model on the account; falls through to OpenRouter auto if
# that shortname isn't configured.
_SUMMARIZER_MODEL = "anthropic/claude-haiku-4-5"


_SYSTEM_PROMPT = (
    "你是一个投研文档摘要助手。基于用户的原始问题，从给定文档中提炼"
    "与该问题最相关的关键信息。\n\n"
    "要求：\n"
    "1. 直接列出与问题相关的事实、数字、观点（不要客套话，不要'本文介绍了...'）\n"
    "2. 保留关键数字、管理层原话、明确时间节点\n"
    "3. 按重要性降序排列；结构用 markdown 无序列表\n"
    "4. 每条摘要结尾用括号标注它在文档中的大致位置（如'（第3章 业务概览）'或"
    "'（电话会议 Q&A 环节）'），便于读者按图索骥\n"
    "5. 总长度 600-1500 字符\n"
    "6. 如果文档与问题不相关，输出一行：'文档与问题相关度低：...'（简述原因）\n"
    "7. 不要 fabricate；不出现文档里没有的数字/事实"
)


def _clip_input(text: str, cap: int = _SUMMARIZER_INPUT_CAP) -> str:
    """Clip to head + tail if the doc is too long for the summarizer."""
    if len(text) <= cap:
        return text
    half = cap // 2
    return (
        text[:half]
        + f"\n\n... [中间省略约 {len(text) - cap} 字符] ...\n\n"
        + text[-half:]
    )


async def _summarize_once(*, full_text: str, user_query: str, doc_meta: dict) -> str:
    """One LLM call. Returns a bullet list string on success, or '' on failure."""
    title = doc_meta.get("title") or "(未知标题)"
    date = doc_meta.get("date") or "(日期未知)"
    source = doc_meta.get("source") or ""
    doc_type = doc_meta.get("doc_type_cn") or doc_meta.get("doc_type") or ""
    institution = doc_meta.get("institution") or "—"

    clipped = _clip_input(full_text)

    user_prompt = (
        f"# 用户原始问题\n{user_query or '(未提供具体问题)'}\n\n"
        f"# 文档元信息\n"
        f"- 标题: {title}\n"
        f"- 日期: {date}\n"
        f"- 类型: {doc_type}\n"
        f"- 机构: {institution}\n"
        f"- 来源: {source}\n\n"
        f"# 文档全文\n{clipped}\n\n"
        f"请基于用户问题输出 600-1500 字符的针对性摘要。"
    )

    try:
        from backend.app.services.chat_llm import call_model_sync
        t0 = time.monotonic()
        resp = await call_model_sync(
            _SUMMARIZER_MODEL,
            [
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
            mode="fast",
            max_tokens=1200,
        )
        elapsed_ms = int((time.monotonic() - t0) * 1000)
        content = (resp or {}).get("content") or ""
        if resp and resp.get("error"):
            logger.warning(
                "kb_doc_summarizer LLM error: %s (elapsed=%dms, model=%s)",
                resp.get("error"), elapsed_ms, _SUMMARIZER_MODEL,
            )
            return ""
        logger.info(
            "kb_doc_summarizer ok: elapsed=%dms chars_in=%d chars_out=%d model=%s",
            elapsed_ms, len(full_text), len(content), _SUMMARIZER_MODEL,
        )
        return content.strip()
    except Exception as e:
        logger.warning("kb_doc_summarizer call failed: %s", e, exc_info=True)
        return ""


async def maybe_summarize(
    res: dict[str, Any],
    *,
    mode: str = "auto",
    trace: Any = None,
) -> dict[str, Any]:
    """Inline-summarize the fetch_document result if the doc is long enough.

    ``mode``:
        - ``auto`` (default): summarize if ``len(text) > 10000``.
        - ``always``: always summarize, even for short docs.
        - ``never``: short-circuit — return ``res`` untouched.

    On any failure (including missing user query, LLM error), returns ``res``
    with ``text`` untouched so the LLM still sees the raw document.
    """
    if mode == "never":
        return res
    if not res.get("found"):
        return res
    text = res.get("text") or ""
    if not text:
        return res
    if mode == "auto" and len(text) < _LONG_DOC_THRESHOLD:
        return res

    # Pull the user query from the request-scoped ContextVar. If the caller
    # didn't publish one (e.g. an external API path), we still summarize —
    # just with a generic TL;DR pivot instead of a query-targeted one.
    user_query = ""
    try:
        from backend.app.services.chat_debug import get_current_user_query
        user_query = get_current_user_query() or ""
    except Exception:
        user_query = ""

    summary = await _summarize_once(
        full_text=text,
        user_query=user_query,
        doc_meta=res,
    )
    if not summary:
        # Failure path: leave the original text alone.
        if trace and hasattr(trace, "log_sse_event"):
            try:
                trace.log_sse_event("KB_FETCH_SUMMARY_SKIPPED", "no summary produced")
            except Exception:
                pass
        return res

    # Splice: [摘要] block first, then a separator, then the full original text.
    header = (
        "## [摘要] (针对你的问题)\n\n"
        + summary
        + "\n\n---\n\n## [原文]\n\n"
    )
    res = dict(res)  # don't mutate the caller's dict
    res["text"] = header + text
    res["summary_applied"] = True
    res["summary_model"] = _SUMMARIZER_MODEL
    if trace and hasattr(trace, "log_sse_event"):
        try:
            trace.log_sse_event(
                "KB_FETCH_SUMMARY",
                f"chars_in={len(text)} chars_summary={len(summary)} model={_SUMMARIZER_MODEL}",
            )
        except Exception:
            pass
    return res
