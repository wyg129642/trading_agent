"""进门财经 MCP server integration for AI Chat — RETIRED (2026-04-24).

Every Jinmen (进门财经) collection — meetings, reports, oversea_reports — is
now mirrored into the local Mongo + Milvus knowledge base and served through
``kb_search``. Calling the remote MCP server from the chat loop is retired
because:

* MCP session management added latency (SSE handshake + session init per call).
* The server is rate-limited and occasionally returns truncated results.
* ``kb_search`` already runs vector + BM25 hybrid retrieval over the full
  1.5 M Jinmen oversea_reports corpus, which the live MCP tools could not
  match in coverage.

For backward compat we keep the module importable: ``JINMEN_TOOLS`` is an
empty list, ``JINMEN_SYSTEM_PROMPT`` is empty, and ``execute_tool`` is a
deprecation shim. Any stale conversation turn that still references a
``jinmen_*`` tool gets a clear redirect message rather than a hard failure.
"""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


# ── Historical config (kept for import-compat) ───────────────────
#
# The live chat loop never reaches the remote MCP server any more. The
# constants below are retained only so external scripts that still import
# ``JINMEN_API_KEY`` or ``JINMEN_BASE_URL`` don't crash on import.

JINMEN_BASE_URL = "https://mcp-server-global.comein.cn"
JINMEN_SSE_PATH = "/mcp-servers/mcp-server-brm/sse"
JINMEN_API_KEY = "cm_cf17e0751ce6457e9d80ee8cfa84d9a4"


# ── Retired tool list — chat loop no longer registers these ──────

JINMEN_TOOLS: list[dict] = []

JINMEN_SYSTEM_PROMPT = ""


# ── Deprecation shim ─────────────────────────────────────────────

_JINMEN_TOOL_MAP = {
    "jinmen_search": "进门综合投研（点评/路演/研报/海外研报）",
    "jinmen_analyst_comments": "进门分析师点评",
    "jinmen_roadshow": "进门路演纪要",
    "jinmen_announcements": "进门公告",
    "jinmen_foreign_reports": "进门外资研报",
    "jinmen_business_segments": "进门主营业务拆分",
}


async def execute_tool(
    name: str,
    arguments: dict[str, Any],
    citation_tracker: Any = None,
) -> str:
    """Deprecated shim — redirect the LLM to kb_search.

    With ``JINMEN_TOOLS`` emptied, a correctly-configured chat loop never
    reaches this function. We keep it so stale conversation history and
    integration callers get a clear redirect rather than a silent failure.
    """
    logger.info(
        "jinmen.execute_tool deprecated call suppressed — name=%s args=%s",
        name, str(arguments)[:200],
    )
    query = str((arguments or {}).get("query", "")).strip()
    topic = _JINMEN_TOOL_MAP.get(name, name)
    hint = f"对应 query: '{query}'。" if query else ""
    return (
        f"[{name} 已停用] 进门财经的全部内容（{topic}）已聚合到本地知识库。"
        "请改用 `kb_search` 并可选 `sources=['jinmen']` 筛选。"
        f"{hint}"
    )
