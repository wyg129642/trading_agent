"""Alpha派投研数据检索 for AI Chat — RETIRED (2026-04-24).

All Alpha派 data (comment / roadShow / report / ann + wechat_articles) is now
mirrored into the local Mongo + Milvus knowledge base and served through
``kb_search`` (with wechat aggregator filtered out by default). Calling the
external AlphaPai API from the chat loop is deprecated:

* The remote API is slower and rate-limited.
* Field coverage is narrower than the local pipeline (no canonical ticker
  normalization, no cross-platform dedup).
* Citation tracking and time filtering are uniform across sources only when
  everything goes through ``kb_search``.

For backward compatibility we keep the module importable: ``ALPHAPAI_TOOLS`` is
now an empty list, the system prompt redirects the LLM to ``kb_search``, and
``execute_tool`` stays callable but returns a one-line deprecation notice if
a stale LLM still tries to call ``alphapai_recall``.
"""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

# Historical config kept so other modules that still import the dict do not
# crash; the live chat loop no longer hits the remote API.
ALPHAPAI_CONFIG = {
    "api_key": "wsuvsfa3fhfyfw3zrjlvhlx8",
    "base_url": "https://open-api.rabyte.cn",
}

_ALLOWED_RECALL_TYPES = {"comment", "roadShow", "report", "ann"}


# ── Retired tool list — chat loop no longer registers these ──────
#
# Historical shape:
#   [{"type": "function", "function": {"name": "alphapai_recall", ...}}]
#
# Anything that still imports ``ALPHAPAI_TOOLS`` (e.g. old scripts) gets an
# empty list so the LLM never sees the retired tool in its schema. Chat prompts
# now steer the model to ``kb_search`` instead.
ALPHAPAI_TOOLS: list[dict] = []

# Kept as empty string: chat.py concatenates this into the system prompt when
# ``alphapai_enabled=True``; emitting the legacy routing instructions would
# contradict the KB_SYSTEM_PROMPT. An empty prompt is a no-op.
ALPHAPAI_SYSTEM_PROMPT = ""


# ── Tool execution ────────────────────────────────────────────────

async def execute_tool(
    name: str,
    arguments: dict[str, Any],
    citation_tracker: Any = None,
) -> str:
    """Deprecated shim — redirect the LLM to kb_search.

    With the tool schema removed, a correctly-configured chat loop never reaches
    this function. We keep it in place to handle (1) stale conversation history
    where an older assistant turn still tries ``alphapai_recall``, and (2)
    direct callers from integration tests. Both cases get a clear redirect
    message rather than a silent failure.
    """
    logger.info(
        "alphapai.execute_tool deprecated call suppressed — name=%s args=%s",
        name, str(arguments)[:200],
    )
    query = str((arguments or {}).get("query", "")).strip()
    hint = (
        f"对应 query: '{query}'。" if query else ""
    )
    return (
        "[alphapai_recall 已停用] Alpha派的全部数据（点评/路演/研报/公告）已"
        "聚合到本地知识库。请改用 `kb_search` 并可选 `sources=['alphapai']` "
        f"筛选。{hint}"
    )


