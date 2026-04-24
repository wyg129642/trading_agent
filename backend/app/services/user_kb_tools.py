"""Chat-tool surface for the personal knowledge base.

Two tools exposed to the LLM (peers to `kb_*`, `alphapai_*`, `web_search`):

* ``user_kb_search``  — BM25 search over the *current user's* uploaded
                        documents. Returns up to 20 chunk-level hits with
                        citation indices. Query is scoped by the ``user_id``
                        the chat endpoint pushes into a ContextVar before
                        dispatch.

* ``user_kb_fetch_document`` — pull the full text of a hit so the LLM can
                        quote / analyze the whole document when one chunk
                        isn't enough.

The dispatcher ``execute_tool`` is called from ``chat_llm.dispatch_tool``
through the ``name.startswith("user_kb_")`` branch. Failures never raise;
they come back as a short human-readable string the LLM can retry around.
"""

from __future__ import annotations

import logging
from typing import Any

from backend.app.services import user_kb_service as svc

logger = logging.getLogger(__name__)


# Shown to the LLM verbatim — keep it short, tell it *when* to reach for this
# over the other retrieval tools (alphapai_recall / kb_search / web_search).
USER_KB_SYSTEM_PROMPT = (
    "## 团队共享个人知识库\n\n"
    "团队成员上传的各类私人资料（PDF/Markdown/文本/Word 等）全部可检索，"
    "不限于当前用户。使用 `user_kb_search` 跨用户检索命中片段，"
    "使用 `user_kb_fetch_document` 读取任意命中文档的完整正文。\n\n"
    "**何时调用：**\n"
    "1. 用户提到自己或团队的笔记、上传文件、内部资料时；\n"
    "2. 当 `kb_search` / `alphapai_*` / `jinmen_*` / `web_search` 命中偏弱，"
    "团队私有上传可能相关时；\n"
    "3. 问题涉及公司内部接口、内部流程、内部研究等官方/公开库可能没有的内容。\n\n"
    "每条命中都是团队成员的私有上传，引用时插入行内 `[N]` 即可；不要额外罗列来源。"
    "结果列表里会标注 `uploader=<user_id>`，用于溯源但回答中无需显式提及。"
)


USER_KB_TOOLS: list[dict[str, Any]] = [
    {
        "type": "function",
        "function": {
            "name": "user_kb_search",
            "description": (
                "Search the team's shared personal knowledge base — files any member "
                "has uploaded (PDFs, markdown, text, docx, csv, json, html). This is "
                "*cross-user*: the current user can retrieve content uploaded by "
                "colleagues. Use this when the user references their own notes OR the "
                "team's internal uploads, or when public research databases (kb_search, "
                "alphapai_*, jinmen_*) return nothing useful. Returns up to `top_k` "
                "chunk-level hits with citation indices; each hit includes the "
                "uploader's user_id for traceability."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Natural-language query. Chinese or English.",
                    },
                    "top_k": {
                        "type": "integer",
                        "description": "Number of chunk hits to return (default 5, max 20).",
                    },
                    "document_ids": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": (
                            "Optional: restrict the search to these document ids "
                            "(as returned by earlier user_kb_search hits)."
                        ),
                    },
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "user_kb_fetch_document",
            "description": (
                "Read the full (or a larger window of the) text of any team member's "
                "uploaded document (cross-user). Use this after user_kb_search when a "
                "chunk hit looks highly relevant and more surrounding context is needed."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "document_id": {
                        "type": "string",
                        "description": "Document id as returned by user_kb_search.",
                    },
                    "max_chars": {
                        "type": "integer",
                        "description": "Ceiling on returned text length (default 8000, max 30000).",
                    },
                },
                "required": ["document_id"],
            },
        },
    },
]


# ── Formatters ────────────────────────────────────────────────


def _format_search_result(hits: list[svc.SearchHit], citation_tracker: Any) -> str:
    if not hits:
        return "在团队共享个人知识库中没有找到相关内容。"

    out: list[str] = ["共享知识库命中结果（按相关度降序）:"]
    for hit in hits:
        idx = None
        source_dict = {
            "title": hit.title,
            "url": "",
            "website": "团队个人库",
            "date": hit.created_at[:10] if hit.created_at else "",
            "source_type": "user_kb",
            "doc_type": "user_upload",
            "doc_id": hit.document_id,
            # Uploader id lets the UI link back to the original contributor
            # if desired. Chat renderer can ignore it.
            "uploader_user_id": hit.uploader_user_id,
        }
        if citation_tracker is not None:
            try:
                idx = citation_tracker.add_source(source_dict)
            except Exception:  # pragma: no cover - tracker shape drift
                idx = None
        prefix = f"[{idx}] " if idx is not None else "- "
        uploader = (hit.uploader_user_id or "?")[:8]  # short id for log compactness
        header = (
            f"{prefix}**{hit.title}** "
            f"(文件:{hit.original_filename} · 段 {hit.chunk_index} · "
            f"上传:{hit.created_at[:10] if hit.created_at else '?'} · "
            f"uploader={uploader} · score={hit.score:.2f})"
        )
        body = hit.text.strip()
        if len(body) > 1200:
            body = body[:1200] + "…"
        out.append(f"{header}\n{body}")
    out.append(
        "\n如需读取完整文档，请调用 `user_kb_fetch_document(document_id=...)`。"
    )
    return "\n\n".join(out)


def _format_fetch_result(meta: dict, text: str, max_chars: int) -> str:
    title = meta.get("title") or meta.get("original_filename") or "(untitled)"
    created = (meta.get("created_at") or "")[:10]
    filename = meta.get("original_filename") or ""
    ext = meta.get("file_extension") or ""
    total_chars = int(meta.get("extracted_char_count") or len(text))
    truncated = len(text) < total_chars
    header = (
        f"### {title}\n"
        f"- 文件: {filename} ({ext or '?'})\n"
        f"- 上传日期: {created}\n"
        f"- 总字符数: {total_chars}\n"
    )
    if truncated:
        header += f"- 注意: 已截取 {len(text)}/{total_chars} 字符，如需更多请调高 max_chars\n"
    return header + "\n---\n\n" + (text or "(no extractable text)")


# ── execute_tool ───────────────────────────────────────────────


async def execute_tool(
    name: str,
    arguments: dict[str, Any],
    citation_tracker: Any = None,
) -> str:
    """Dispatch entry point called from ``chat_llm.dispatch_tool``."""
    try:
        from backend.app.services.chat_debug import chat_trace, get_current_trace_id
        trace = chat_trace(get_current_trace_id())
    except Exception:  # pragma: no cover - chat_debug not wired
        trace = None

    # The caller's identity is recorded for observability only — the KB is
    # team-shared, so search/fetch are NOT scoped by user. If no context is
    # bound we still serve the tool (falls back to "unknown" in traces).
    caller_user_id = svc.get_current_user_id() or "(unknown)"

    try:
        if name == "user_kb_search":
            query = (arguments.get("query") or "").strip()
            if not query:
                return "user_kb_search 需要参数 query（自然语言查询字符串）。"
            top_k = int(arguments.get("top_k") or 5)
            document_ids_raw = arguments.get("document_ids") or []
            document_ids = [
                str(d) for d in document_ids_raw if isinstance(d, (str, int))
            ] if document_ids_raw else None

            if trace and hasattr(trace, "log_user_kb_request"):
                trace.log_user_kb_request(
                    user_id=caller_user_id, query=query, top_k=top_k,
                    document_ids=document_ids,
                )
            # Cross-user search — do NOT pass user_id.
            hits = await svc.search_chunks(
                query, top_k=top_k, document_ids=document_ids,
            )
            if trace and hasattr(trace, "log_user_kb_results"):
                trace.log_user_kb_results(
                    query=query,
                    result_count=len(hits),
                    top_titles=[h.title[:160] for h in hits[:10]],
                )
            return _format_search_result(hits, citation_tracker)

        if name == "user_kb_fetch_document":
            doc_id = (arguments.get("document_id") or "").strip()
            if not doc_id:
                return "user_kb_fetch_document 需要参数 document_id。"
            max_chars = int(arguments.get("max_chars") or 8000)
            max_chars = max(500, min(max_chars, 30_000))
            # Cross-user fetch — the chat tool is explicitly allowed to read
            # any team member's upload. HTTP management endpoints keep their
            # own user scoping.
            meta = await svc.get_any_document(doc_id)
            if meta is None:
                return f"未找到 document_id={doc_id}。"
            text = await svc.get_any_document_content(doc_id, max_chars=max_chars)
            text = text or ""
            if trace and hasattr(trace, "log_user_kb_request"):
                trace.log_user_kb_request(
                    user_id=caller_user_id, query=f"fetch:{doc_id}", top_k=1,
                )
            return _format_fetch_result(meta, text, max_chars)

        return f"未知的 user_kb 工具: {name}"

    except Exception as e:
        logger.exception("user_kb tool %s failed with args=%s", name, arguments)
        return f"user_kb 工具 `{name}` 执行失败: {e}"
