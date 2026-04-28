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
# over the other retrieval tools (kb_search / web_search).
USER_KB_SYSTEM_PROMPT = (
    "## 团队共享个人知识库（user_kb_search — 最高优先级检索工具）\n\n"
    "**团队全员上传的私有资料汇总**（PDF/Markdown/文本/Word/CSV/PPT/录音转写等）。"
    "本库**跨用户共享**——你能检索到团队任何成员上传的内容，不局限于当前提问用户的"
    "上传。包含：内部研究笔记、买方研报、专家访谈、调研纪要、内部数据表、会议录音等"
    "**官方/公开库无法获取的私有信息**，因此通常比 `kb_search` 和 `web_search` 更"
    "贴近团队实际研究脉络。\n\n"
    "### 检索优先级（严格执行）\n"
    "**研究类问题每轮工具调用必须并行发起 `user_kb_search` + `kb_search`**——"
    "二者是互补的（团队私藏 vs. 公开聚合）。优先级排序：\n"
    "1. **`user_kb_search`（最高）**——团队私有研究/纪要/数据，可能命中独家洞见\n"
    "2. **`kb_search`**——公司聚合的 8 个外部平台投研数据\n"
    "3. **`web_search`**——上述两者均未覆盖的公开新闻/宏观/最新事件\n\n"
    "**禁止**：跳过 user_kb_search 直接使用 kb_search 或 web_search。"
    "即使你认为问题更适合公开数据，也必须并行尝试 user_kb_search——团队可能"
    "正好上传过相关内部资料。\n\n"
    "### 工具用法\n"
    "- `user_kb_search(query, top_k)` — 跨用户 BM25+向量混合检索，命中片段 +"
    "  `uploader=<user_id>` 溯源。每条命中均使用全局编号 `[N]` 引用。\n"
    "- `user_kb_fetch_document(document_id, max_chars)` — 读取任意成员上传文档"
    "  的完整正文（包含 PDF 解析后的全文）。当摘要片段不足以回答问题时调用。"
    "  PDF/DOCX/录音转写均已离线解析为文本，无需手动转换。\n\n"
    "引用时插入行内 `[N]` 即可；不要在末尾罗列来源；不要在回答中暴露 uploader id。"
)


USER_KB_TOOLS: list[dict[str, Any]] = [
    {
        "type": "function",
        "function": {
            "name": "user_kb_search",
            "description": (
                "**最高优先级检索工具**——检索团队**全员共享**的个人知识库（任意"
                "成员上传的 PDF / Markdown / TXT / DOCX / CSV / PPT / 录音转写 / "
                "JSON / HTML）。\n\n"
                "**关键特性**：本库跨用户共享——当前用户能检索到团队任何成员上传的"
                "全部内容，**不局限于当前提问用户自己的上传**。包含买方内部研究、"
                "调研纪要、专家访谈、内部数据、会议录音等官方/公开库无法获取的"
                "私有信息。\n\n"
                "**调用规则（强制）**：研究类问题**每轮**工具调用必须并行发起 "
                "`user_kb_search` + `kb_search`，二者互补；不要跳过 user_kb_search "
                "直接用 kb_search/web_search——团队可能上传过更新更贴近的内部资料。\n\n"
                "返回最多 `top_k` 条带 `[N]` 编号的片段命中，含 uploader 元数据用于"
                "溯源。需要完整正文时调用 `user_kb_fetch_document`（PDF 已离线解析"
                "为文本）。"
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
                "读取**任意团队成员**上传文档的完整正文（跨用户）。在 user_kb_search "
                "命中关键片段后，调用本工具获取上下文。\n\n"
                "**PDF 支持**：上传时 PDF 已通过 opendataloader / pypdf 解析为 "
                "Markdown 文本，本工具直接返回解析后的正文；如解析为空（罕见情况，"
                "例如扫描件 OCR 失败），后端会**实时回退**到内联 PDF 解析并把结果"
                "回写。无需调用其它工具或下载 PDF。"
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
            f"uploader={uploader})"
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
        # Keyword args matter: chat_trace's first positional is user_id.
        # model_id is picked up from the per-model contextvar set by chat_llm.
        trace = chat_trace(trace_id=get_current_trace_id())
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
            max_chars = int(arguments.get("max_chars") or 8000)
            max_chars = max(500, min(max_chars, 30_000))
            if not doc_id:
                if trace and hasattr(trace, "log_user_kb_fetch"):
                    trace.log_user_kb_fetch(
                        document_id="", max_chars=max_chars,
                        result_len=0, error="missing document_id",
                    )
                return "user_kb_fetch_document 需要参数 document_id。"
            # Log the fetch request BEFORE doing IO so the audit timeline
            # captures every doc_id the LLM asked for, even if the fetch
            # itself fails. Pairs with a post-IO USER_KB_FETCH carrying
            # result_len / error, mirroring the kb_fetch_document pattern.
            if trace and hasattr(trace, "log_user_kb_fetch"):
                trace.log_user_kb_fetch(document_id=doc_id, max_chars=max_chars)
            # Cross-user fetch — the chat tool is explicitly allowed to read
            # any team member's upload. HTTP management endpoints keep their
            # own user scoping.
            meta = await svc.get_any_document(doc_id)
            if meta is None:
                if trace and hasattr(trace, "log_user_kb_fetch"):
                    trace.log_user_kb_fetch(
                        document_id=doc_id, max_chars=max_chars,
                        result_len=0, error="document not found",
                    )
                return f"未找到 document_id={doc_id}。"
            text = await svc.get_any_document_content(doc_id, max_chars=max_chars)
            text = text or ""
            formatted = _format_fetch_result(meta, text, max_chars)
            if trace and hasattr(trace, "log_user_kb_fetch"):
                trace.log_user_kb_fetch(
                    document_id=doc_id, max_chars=max_chars,
                    result_len=len(formatted or ""),
                    result_preview=(formatted or "")[:400],
                )
            return formatted

        return f"未知的 user_kb 工具: {name}"

    except Exception as e:
        logger.exception("user_kb tool %s failed with args=%s", name, arguments)
        return f"user_kb 工具 `{name}` 执行失败: {e}"
