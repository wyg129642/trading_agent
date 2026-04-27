"""Chat API: conversations, multi-model messages, ratings, templates, file upload."""
from __future__ import annotations

import asyncio
import json
import logging
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Query, Request, UploadFile, File
from fastapi.responses import StreamingResponse
from sqlalchemy import select, func, desc, delete, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from backend.app.config import get_settings
from backend.app.deps import get_db, get_current_user, get_current_admin
from backend.app.models.chat import (
    ChatConversation, ChatMessage, ChatModelResponse, ChatPromptTemplate,
    ChatTrackingTopic, ChatTrackingAlert,
)
from backend.app.models.user import User
from backend.app.schemas.chat import (
    ConversationCreate, ConversationUpdate, ConversationResponse,
    ConversationListResponse, ConversationDetailResponse,
    MessageResponse, ModelResponseData,
    SendMessageRequest, SendMessageResponse,
    RegenerateRequest, SavePartialRequest,
    RateRequest, RateResponse,
    TemplateCreate, TemplateUpdate, TemplateResponse,
    FileUploadResponse, ExportResponse,
    ModelInfo, ModelRanking, ModelRankingResponse,
    ChatStatsResponse,
    DebateRequest, DebateSummary,
    TrackingTopicCreate, TrackingTopicUpdate, TrackingTopicResponse,
    TrackingAlertResponse,
)
from backend.app.services.chat_llm import (
    AVAILABLE_MODELS, MODEL_MAP, MODE_CONFIGS,
    build_multimodal_content, call_model_stream, call_model_stream_with_tools,
    call_model_sync, generate_title, _build_messages, search_for_chat,
)
from backend.app.services.alphapai_service import (
    ALPHAPAI_TOOLS, ALPHAPAI_SYSTEM_PROMPT,
)
from backend.app.services.jinmen_service import (
    JINMEN_TOOLS, JINMEN_SYSTEM_PROMPT,
)
from backend.app.services.web_search_tool import (
    WEB_SEARCH_TOOLS, WEB_SEARCH_SYSTEM_PROMPT, WEB_SEARCH_FORCE_PROMPT,
)
from backend.app.services.kb_service import (
    KB_TOOLS, KB_SYSTEM_PROMPT,
)
from backend.app.services.user_kb_tools import (
    USER_KB_TOOLS, USER_KB_SYSTEM_PROMPT,
)
from backend.app.services import user_kb_service as _user_kb_svc
from backend.app.services.revenue_model_chat_tool import (
    TRIGGER_REVENUE_MODEL_TOOLS, TRIGGER_REVENUE_MODEL_SYSTEM_PROMPT,
)
from backend.app.services.chat_debug import chat_trace

logger = logging.getLogger(__name__)
router = APIRouter()

def _build_time_awareness_prompt() -> str:
    """Return a time-awareness system-prompt addition with today's date injected.

    Financial research data has strict time-tiered shelf life; without explicit
    guidance, models frequently mis-use stale numbers as current reality. This
    prompt teaches the model to read every result's publish-date field, anchor
    every claim to a dated timeframe, and distinguish realized / in-progress /
    forecasted information.
    """
    today = datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d")
    return f"""## 时间维度规范（关键，务必遵守）

工具返回的每条资料都带 `发布日期 / 发布时间 / businessTime / date` 字段 —— **引用前先看日期**，否则极易把旧数据当作当前事实。**今天的日期是 {today}**，请据此判断资料新旧。

### 数据时效性分层（不同数据的有效期差异极大）
- 股价、日内行情 → **天级**（距今 > 1 周的实时数据基本失效）
- 财务季报数据 → **季级**（新季报披露后，上一期数据自动降级为历史对比）
- 业务拆分、客户名单、产能数字 → **半年级**（以最新半年报/年报为准）
- 战略方向、产品路线、研发进展 → **半年级**（战略调整后旧规划不代表当前意图）
- 行业格局、市场份额 → **1–2 年**（AI/新能源等高速变化行业要更快刷新）
- 历史业绩趋势、业务变迁 → **稳定**（可随时引用作对比）

### 引用铁律
1. **每个关键数字必须带时间锚点**。示例：
   - ✅ "2025Q3 营收 33.87 亿，同比 +27.16% [5]"
   - ✅ "截至 2024 年年报，高功率服务器电源占数据中心电源 53.48% [27]"
   - ❌ "营收 15.94 亿"（无时间锚点——读者无法判断是哪一年的数据）
2. **同一事实有多条来源时，引用发布日期最近的那条**；旧版本可另外作为"历史对比 / 趋势佐证"引用。
3. **机构预测必须标注机构 + 研报日期**：
   - ✅ "光大证券 2025-04-27 研报预测 2026E 归母净利润 4.6 亿 [11]"
   - ❌ "市场预期 2026 年净利润 4.6 亿"（丢失了预测来源和预测发布时间）
4. **区分"已兑现 / 在研 / 预测"三种时态**，用词要能让读者立刻分辨：
   - 已兑现用完成态："2024 年**已实现**数据中心电源收入 14.59 亿 [27]"
   - 在研/导入中明确阶段："与谷歌合作**样品阶段**，预计 2026Q4 出份额判断 [11]"
   - 预测用未来时 + 来源："XX 证券 2025-xx-xx 研报**预测**..."
5. **禁止跨时间段混写**：
   - ❌ "营收持续增长 [A][B]"（[A]=2023 年数据，[B]=2025 年数据混为一谈）
   - ✅ "2023 年营收 28.70 亿 [A] → 2025 前三季度 33.87 亿，同比 +27.16% [B]"

### 过期数据的处理
- **发布日期距今 > 6 个月**的"现状"数据要警惕已被新数据替代；如是核心数字，优先用 web_search 或 jinmen_announcements 查最新季报/公告验证。
- **发布日期距今 > 12 个月**的"对未来的预测"基本失效，只能作为"当时视角"引用（例如 `"2024 年底中金曾预测 2025 全年增速 30%"`），不要当成对当下的预判。
- 如果工具返回的全是旧数据，**主动承认信息滞后**而不是硬编故事：
  - ✅ "目前可查到的业务拆分最新为 2024 年年报 [N]，2025 年年报尚未披露，以下基于最近可得数据推断"
  - ❌ 用 2023 年的产品结构写成"当前主要产品线是 XX"

### 结论层级要求
在得出任何"当前状况"、"现在怎么样"、"最新进展"的结论前，自问一句：**我引用的那条数据是几个月前发布的？它真的代表"当前"吗？** 如果不是最新，要么换一条新的资料，要么显式注明"截至 XXXX-XX"。"""


_RESEARCH_STRATEGY_PROMPT = """## 深度研究策略（务必遵守）

做个股/行业深度研究时，按"**并行多路召回 → 读原文 → 补强 → 写报告**"四步法，避免停留在摘要层面。

### 检索工具优先级（强制——每轮调用都要遵循）
研究类问题每一轮工具调用都必须按以下顺序覆盖三层信息源；前两层为**强制并行调用**，第三层为补充：

1. **`user_kb_search`（最高优先级·团队共享个人库）**——团队全员上传的私有研究/纪要/调研笔记/数据表/录音转写。跨用户共享，可能含独家内部资料。
2. **`kb_search`（公司聚合外部库）**——8 个外部投研平台聚合的公开研报/纪要/点评/专家访谈。
3. **`web_search`（最后补充）**——前两步均未覆盖的公开新闻/宏观/最新事件。

**强制规则**：
- **每一轮**工具调用都必须**同时**发起 `user_kb_search` + `kb_search`，二者互为补充（团队私藏 vs. 公开聚合），缺一不可。
- 即便你认为问题更适合公开数据，也不得跳过 user_kb_search——团队可能正好上传过最贴近的内部资料。
- 二者并行调用，总延迟 ≈ 慢者，对响应时间几乎无影响。

### 步骤1｜第一轮：并行多路召回（关键：同一轮内并发）
**必须在同一轮内同时发起多个工具调用**（现代 LLM 支持 parallel tool calling——多个 tool_calls 放在同一条 assistant 消息里），以压缩等待时间并保证多视角覆盖：

- `user_kb_search`（**最高优先**）：并发发起 1–2 次，覆盖团队所有成员上传的内部资料。中文 query + 关键词，必要时英文版本。
- `kb_search`（**外部聚合库**）：并发发起 2–4 次，组合不同策略：
  - 中文 query + tickers 筛选（覆盖国内券商、纪要、点评）
  - 英文 query + tickers 筛选（覆盖海外研报、外资视角）
  - 子议题 query 拆分（业务/财务/产能/客户各查一次）
  - 必须附带 `date_range` 以约束时效（涉及"最新/近期"时，gte 取最近 6–12 个月）
- `web_search`：并发发起 1–2 次，中英双语关键词，捕获最新新闻与宏观事件

> 并行的好处：总延迟约等于最慢工具的单次延迟，而非各工具之和。

### 步骤2｜必须进入"读原文"阶段（极其重要）
第一轮召回返回后，**不得直接写报告**。扫描命中列表，挑出 2–4 个最硬核的结果读原文：

- 对 `user_kb_search` 高相关命中 → 调 `user_kb_fetch_document(document_id=...)` 读完整原文（**自动包含 PDF 解析后的全文**）
- 对 `kb_search` 高相关命中 → 调 `kb_fetch_document(doc_id=...)` 读完整原文（**自动包含 PDF 解析后的全文**——研报 PDF 文本已离线提取，缺失时后端会实时回退到内联解析）
- 对 `web_search` 硬核 URL（公告详情页、pdf.dfcfw.com 研报、IR 页）→ 调 `read_webpage(url=...)`

优先级：团队内部纪要/调研 PDF > 官方公告 / 定期报告 PDF > 券商深度研报全文 > 业绩交流/路演纪要 > 权威财经新闻；避开百家号、SEO 聚合站、纯索引页。

### 步骤3｜第二轮：针对性补强（仍可并行；仍需 user_kb + kb 双召回）
基于第一轮 + 深读的结果，用更具体的 query 补漏，同样并行发起：
- 出现的子业务/客户/竞品名 → 同时发起 `user_kb_search` + `kb_search` 精准检索
- 发现关键数字但无上下文 → `kb_fetch_document` / `user_kb_fetch_document` 回到原文
- 跨时间对比缺数据 → `kb_list_facets(dimension='date_histogram')` 看分布再分时段检索
- 行业对标缺海外视角 → `web_search` 英文或 `kb_search` 限定 `sources=['jinmen']`（海外研报覆盖）

### 步骤4｜写作要求
- 2–3 轮召回 + 深读后停止工具调用，直接输出报告
- **行内 `[N]` 引用**：每个事实 / 数字 / 观点句末必须带引用；不要在末尾罗列来源列表——UI 自动渲染
- **时间锚点**：每个关键数字必须带具体日期（"2025Q3 营收 33.87 亿 [5]" 而不是裸的 "营收 33.87 亿"）
- 覆盖用户所问的所有维度（业务线 / 产能 / 客户 / 财务 / 估值 / 风险）；缺失的维度显式说明"数据不足"
- 字数 4000+ 字；不要为懒而压缩

### 禁止
- **跳过 `user_kb_search`**：每轮必须并行调用 user_kb_search + kb_search，二者缺一不可
- **在一轮里串行发起多个 kb_search**：必须把它们放进同一个 assistant 消息的 `tool_calls` 数组里并行执行
- 跳过 `kb_fetch_document` / `user_kb_fetch_document` / `read_webpage` 直接写报告（研究深度严重不足）
- 反复用近义 query 重复调用（浪费 round budget；应改变维度：不同 ticker / 不同日期段 / 不同子议题）
- 只调 `web_search` 不调 `kb_search` / `user_kb_search`（本地知识库质量远高于公网，必须优先）"""

UPLOAD_DIR = Path(__file__).resolve().parent.parent.parent.parent / "data" / "chat_uploads"
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)


# ── Personal-knowledge-base reference injection ──────────────────


# Per-doc and aggregate char caps. A single dropped PDF can extract to
# hundreds of KB; we give each doc a fair share and cap the total so the
# prompt doesn't blow past the model's context.
_KB_DOC_MAX_CHARS = 30_000
_KB_TOTAL_MAX_CHARS = 100_000


async def _build_kb_reference_prefix(
    user_id: str,
    doc_ids: list[str],
) -> tuple[str, list[dict]]:
    """Fetch accessible user-kb docs and format as a user-message prefix.

    Returns ``(prefix_text, doc_meta_list)``. ``doc_meta_list`` is a list
    of ``{"id","title","filename","chars","truncated"}`` for debug logging
    and client-side display.

    Docs the user can't access (wrong owner, not public, deleted) are
    silently skipped — a stale drag-dropped id shouldn't break the request.
    """
    if not doc_ids:
        return "", []
    meta_list: list[dict] = []
    parts: list[str] = []
    total_chars = 0
    dedup_ids: list[str] = []
    seen: set[str] = set()
    for d in doc_ids:
        if d and d not in seen:
            dedup_ids.append(d)
            seen.add(d)
    for idx, did in enumerate(dedup_ids, start=1):
        if total_chars >= _KB_TOTAL_MAX_CHARS:
            meta_list.append({
                "id": did, "title": "", "filename": "",
                "chars": 0, "truncated": True,
                "skipped": "total budget exhausted",
            })
            continue
        doc = await _user_kb_svc.get_accessible_document(user_id, did)
        if doc is None:
            meta_list.append({
                "id": did, "title": "", "filename": "",
                "chars": 0, "truncated": False,
                "skipped": "not accessible",
            })
            continue
        remaining = _KB_TOTAL_MAX_CHARS - total_chars
        cap = min(_KB_DOC_MAX_CHARS, max(1000, remaining))
        content = await _user_kb_svc.get_accessible_document_content(
            user_id, did, max_chars=cap,
        )
        content = (content or "").strip()
        if not content:
            meta_list.append({
                "id": did,
                "title": doc.get("title") or "",
                "filename": doc.get("original_filename") or "",
                "chars": 0, "truncated": False,
                "skipped": "empty content",
            })
            continue
        full_chars = int(doc.get("extracted_char_count") or len(content))
        truncated = len(content) < full_chars
        title = doc.get("title") or doc.get("original_filename") or "(untitled)"
        filename = doc.get("original_filename") or ""
        header = (
            f"\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"【参考文档 {idx}】: {title}"
            f"{f' ({filename})' if filename and filename != title else ''}"
            f"{' [已截断]' if truncated else ''}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        )
        parts.append(header + content)
        total_chars += len(content)
        meta_list.append({
            "id": did,
            "title": title,
            "filename": filename,
            "chars": len(content),
            "truncated": truncated,
        })
    if not parts:
        return "", meta_list
    prefix = (
        "以下是用户从「个人知识库」中附带的参考文档，"
        "请在回答中优先参考这些内容。"
        + "".join(parts)
        + "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "【参考文档结束】\n\n"
    )
    return prefix, meta_list


def _best_response(responses: list[ChatModelResponse]) -> ChatModelResponse | None:
    """Pick the best model response: highest rating, then lowest latency."""
    if not responses:
        return None
    return max(
        responses,
        key=lambda r: (r.rating or 0, -(r.latency_ms or 9999999)),
    )


# ── Models ──────────────────────────────────────────────────────

@router.get("/models", response_model=list[ModelInfo])
async def list_models():
    """List all available LLM models."""
    return [ModelInfo(**m) for m in AVAILABLE_MODELS]


@router.get("/modes")
async def list_modes():
    """List available chat modes."""
    return [
        {"id": k, "label": v["label"], "description": desc}
        for k, v, desc in [
            ("standard", MODE_CONFIGS["standard"], "平衡速度与质量，适合日常研究问答"),
            ("thinking", MODE_CONFIGS["thinking"], "深度推理，适合复杂分析和多步骤问题"),
            ("fast", MODE_CONFIGS["fast"], "快速响应，适合简单查询和头脑风暴"),
        ]
    ]


# ── Recommended questions (quick-start) ─────────────────────────

@router.get("/recommended-questions")
async def get_recommended_questions(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Return the current user's personalized quick-start questions.

    The scheduler refreshes these daily; if the cache is empty or stale on the
    first-ever load, we fall back to generating inline so the empty state is
    never truly blank.
    """
    from backend.app.services.recommendation_service import (
        get_for_user, DEFAULT_QUESTIONS,
    )
    try:
        questions = await get_for_user(db, user.id)
    except Exception:
        logger.exception("recommended-questions: fetch failed, using defaults")
        questions = list(DEFAULT_QUESTIONS)
    return {"questions": questions}


@router.post("/recommended-questions/refresh")
async def refresh_recommended_questions(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Force-regenerate the current user's recommendations now (manual refresh)."""
    from backend.app.services.recommendation_service import (
        generate_for_user, DEFAULT_QUESTIONS,
    )
    try:
        questions = await generate_for_user(db, user.id, force=True)
    except Exception:
        logger.exception("recommended-questions: manual refresh failed")
        questions = list(DEFAULT_QUESTIONS)
    return {"questions": questions}


# ── Conversations ───────────────────────────────────────────────

@router.get("/conversations", response_model=ConversationListResponse)
async def list_conversations(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
    search: str | None = None,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """List user's conversations, newest first."""
    q = select(ChatConversation).where(ChatConversation.user_id == user.id)
    if search:
        q = q.where(ChatConversation.title.ilike(f"%{search}%"))
    q = q.order_by(desc(ChatConversation.is_pinned), desc(ChatConversation.updated_at))

    # Count
    count_q = select(func.count()).select_from(q.subquery())
    total = await db.scalar(count_q) or 0

    # Paginate
    q = q.offset((page - 1) * page_size).limit(page_size)
    result = await db.execute(q)
    convs = result.scalars().all()

    items = []
    for c in convs:
        # Get last message preview
        last_msg = await db.scalar(
            select(ChatMessage)
            .where(ChatMessage.conversation_id == c.id, ChatMessage.role == "user")
            .order_by(desc(ChatMessage.created_at))
            .limit(1)
        )
        msg_count = await db.scalar(
            select(func.count()).where(ChatMessage.conversation_id == c.id)
        ) or 0

        items.append(ConversationResponse(
            id=str(c.id),
            title=c.title,
            tags=c.tags or [],
            is_pinned=c.is_pinned,
            created_at=c.created_at,
            updated_at=c.updated_at,
            message_count=msg_count,
            last_message_preview=last_msg.content[:80] if last_msg else "",
        ))

    return ConversationListResponse(conversations=items, total=total)


@router.post("/conversations", response_model=ConversationResponse, status_code=201)
async def create_conversation(
    body: ConversationCreate,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    conv = ChatConversation(
        user_id=user.id,
        title=body.title,
        tags=body.tags,
    )
    db.add(conv)
    await db.commit()
    await db.refresh(conv)
    return ConversationResponse(
        id=str(conv.id), title=conv.title, tags=conv.tags or [],
        is_pinned=conv.is_pinned, created_at=conv.created_at,
        updated_at=conv.updated_at, message_count=0, last_message_preview="",
    )


@router.get("/conversations/{conv_id}", response_model=ConversationDetailResponse)
async def get_conversation(
    conv_id: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    conv = await db.scalar(
        select(ChatConversation)
        .where(ChatConversation.id == conv_id, ChatConversation.user_id == user.id)
    )
    if not conv:
        raise HTTPException(404, "Conversation not found")

    msgs = await db.execute(
        select(ChatMessage)
        .where(ChatMessage.conversation_id == conv.id)
        .options(selectinload(ChatMessage.model_responses))
        .order_by(ChatMessage.created_at)
    )
    messages = msgs.scalars().all()

    return ConversationDetailResponse(
        id=str(conv.id), title=conv.title, tags=conv.tags or [],
        is_pinned=conv.is_pinned, created_at=conv.created_at,
        updated_at=conv.updated_at,
        messages=[
            MessageResponse(
                id=str(m.id), role=m.role, content=m.content,
                attachments=m.attachments or [],
                is_debate=getattr(m, "is_debate", False) or False,
                model_responses=[
                    ModelResponseData(
                        id=str(r.id), model_id=r.model_id, model_name=r.model_name,
                        content=r.content, tokens_used=r.tokens_used,
                        latency_ms=r.latency_ms, rating=r.rating,
                        rating_comment=r.rating_comment, error=r.error,
                        sources=r.sources,
                        debate_round=getattr(r, "debate_round", None),
                        created_at=r.created_at,
                    )
                    for r in m.model_responses
                ],
                created_at=m.created_at,
            )
            for m in messages
        ],
    )


@router.patch("/conversations/{conv_id}", response_model=ConversationResponse)
async def update_conversation(
    conv_id: str,
    body: ConversationUpdate,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    conv = await db.scalar(
        select(ChatConversation)
        .where(ChatConversation.id == conv_id, ChatConversation.user_id == user.id)
    )
    if not conv:
        raise HTTPException(404, "Conversation not found")

    if body.title is not None:
        conv.title = body.title
    if body.tags is not None:
        conv.tags = body.tags
    if body.is_pinned is not None:
        conv.is_pinned = body.is_pinned

    await db.commit()
    await db.refresh(conv)

    msg_count = await db.scalar(
        select(func.count()).where(ChatMessage.conversation_id == conv.id)
    ) or 0

    return ConversationResponse(
        id=str(conv.id), title=conv.title, tags=conv.tags or [],
        is_pinned=conv.is_pinned, created_at=conv.created_at,
        updated_at=conv.updated_at, message_count=msg_count,
        last_message_preview="",
    )


@router.delete("/conversations/{conv_id}", status_code=204)
async def delete_conversation(
    conv_id: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    conv = await db.scalar(
        select(ChatConversation)
        .where(ChatConversation.id == conv_id, ChatConversation.user_id == user.id)
    )
    if not conv:
        raise HTTPException(404, "Conversation not found")
    await db.delete(conv)
    await db.commit()


# ── Send message (non-streaming) ───────────────────────────────

@router.post("/conversations/{conv_id}/messages", response_model=SendMessageResponse)
async def send_message(
    conv_id: str,
    body: SendMessageRequest,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    conv = await db.scalar(
        select(ChatConversation)
        .where(ChatConversation.id == conv_id, ChatConversation.user_id == user.id)
    )
    if not conv:
        raise HTTPException(404, "Conversation not found")

    # Build user content (with attachments)
    user_content = build_multimodal_content(body.content, body.attachments)

    # Save user message
    msg = ChatMessage(
        conversation_id=conv.id,
        role="user",
        content=body.content,
        attachments=body.attachments,
    )
    db.add(msg)
    await db.flush()

    # Get conversation history for context
    history_result = await db.execute(
        select(ChatMessage)
        .where(ChatMessage.conversation_id == conv.id)
        .options(selectinload(ChatMessage.model_responses))
        .order_by(ChatMessage.created_at)
    )
    history_msgs = history_result.scalars().all()

    # Build history (user messages + first model response as assistant)
    history = []
    for hm in history_msgs:
        if hm.id == msg.id:
            continue  # skip current message
        if hm.role == "user":
            history.append({"role": "user", "content": hm.content})
            best = _best_response(hm.model_responses)
            if best and best.content:
                history.append({"role": "assistant", "content": best.content})

    # Call all models concurrently
    messages_payload = _build_messages(history, user_content, body.system_prompt)
    mode = body.mode if body.mode in MODE_CONFIGS else "standard"

    async def call_single(model_id: str) -> dict:
        result = await call_model_sync(model_id, messages_payload, mode=mode)
        return {"model_id": model_id, **result}

    tasks = [call_single(m) for m in body.models]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Save model responses
    response_data = []
    for r in results:
        if isinstance(r, Exception):
            r = {"model_id": "unknown", "content": "", "error": str(r), "tokens": 0, "latency_ms": 0}

        model_info = MODEL_MAP.get(r["model_id"], {"name": r["model_id"]})
        resp = ChatModelResponse(
            message_id=msg.id,
            model_id=r["model_id"],
            model_name=model_info.get("name", r["model_id"]),
            content=r.get("content", ""),
            tokens_used=r.get("tokens"),
            latency_ms=r.get("latency_ms"),
            error=r.get("error"),
        )
        db.add(resp)
        await db.flush()
        response_data.append(ModelResponseData(
            id=str(resp.id), model_id=resp.model_id, model_name=resp.model_name,
            content=resp.content, tokens_used=resp.tokens_used,
            latency_ms=resp.latency_ms, rating=resp.rating,
            rating_comment=resp.rating_comment, error=resp.error,
            sources=resp.sources,
            created_at=resp.created_at,
        ))

    # Auto-generate title in background for first message
    is_first = conv.title == "新对话"
    if is_first:
        conv.title = body.content[:20].strip() + ("..." if len(body.content) > 20 else "")

    conv.updated_at = datetime.now(timezone.utc)
    await db.commit()
    if is_first:
        asyncio.create_task(_generate_title_background(conv.id, body.content))

    return SendMessageResponse(message_id=str(msg.id), model_responses=response_data)


# ── Send message (SSE streaming) ───────────────────────────────

@router.post("/conversations/{conv_id}/messages/stream")
async def send_message_stream(
    conv_id: str,
    body: SendMessageRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Send message and stream responses from multiple models via SSE."""
    conv = await db.scalar(
        select(ChatConversation)
        .where(ChatConversation.id == conv_id, ChatConversation.user_id == user.id)
    )
    if not conv:
        raise HTTPException(404, "Conversation not found")

    # Prepend referenced personal-KB documents to the user message before
    # the LLM sees it. We do this BEFORE build_multimodal_content so text and
    # multimodal paths both pick up the prefix. The prefix is NOT saved into
    # the DB message — otherwise subsequent turns would reload the full file
    # bodies every time and blow past context.
    kb_prefix, kb_meta = await _build_kb_reference_prefix(
        str(user.id), list(body.kb_document_ids or []),
    )
    augmented_content = (kb_prefix + body.content) if kb_prefix else body.content
    user_content = build_multimodal_content(augmented_content, body.attachments)
    if kb_meta:
        logger.info(
            "chat: injected %d KB docs (chars=%d) into user message",
            len([m for m in kb_meta if not m.get("skipped")]),
            sum(int(m.get("chars", 0)) for m in kb_meta),
        )

    # Save user message (without the KB prefix — see above)
    msg = ChatMessage(
        conversation_id=conv.id,
        role="user",
        content=body.content,
        attachments=body.attachments,
    )
    db.add(msg)
    await db.flush()
    msg_id = msg.id

    # Get conversation history
    history_result = await db.execute(
        select(ChatMessage)
        .where(ChatMessage.conversation_id == conv.id)
        .options(selectinload(ChatMessage.model_responses))
        .order_by(ChatMessage.created_at)
    )
    history_msgs = history_result.scalars().all()

    history = []
    for hm in history_msgs:
        if hm.id == msg_id:
            continue
        if hm.role == "user":
            history.append({"role": "user", "content": hm.content})
            best = _best_response(hm.model_responses)
            if best and best.content:
                history.append({"role": "assistant", "content": best.content})

    # Build system prompt and tools based on enabled data sources
    system_prompt = body.system_prompt or ""
    all_tools: list[dict] = []

    # Web search as a tool (LLM decides when to search)
    if body.web_search in ("on", "auto"):
        system_prompt = (system_prompt + "\n\n" + WEB_SEARCH_SYSTEM_PROMPT).strip()
        all_tools.extend(WEB_SEARCH_TOOLS)
        if body.web_search == "on":
            system_prompt += WEB_SEARCH_FORCE_PROMPT

    # Backward-compat consolidation: alphapai_enabled and jinmen_enabled no
    # longer add the retired external tools; they now only act as a hint that
    # the user wants investment-research data, which means ``kb_search`` (the
    # local aggregated corpus) must be available. We therefore coerce kb_enabled
    # on whenever either legacy flag is set.
    kb_effective = bool(body.kb_enabled or body.alphapai_enabled or body.jinmen_enabled)

    # ALPHAPAI_TOOLS / JINMEN_TOOLS are now empty lists and their prompts are
    # empty strings — extending/concatenating them is a safe no-op, but we
    # intentionally do NOT call them any more to keep the system prompt clean
    # and avoid misleading the LLM about tool availability.

    # Knowledge bases are now a *paired* surface: per-round dual call is the
    # mandated research pattern (user_kb_search + kb_search in parallel). When
    # the user has either KB toggle on, we wire up BOTH tools so the LLM can
    # actually obey the prompt — toggling only one off would force the prompt
    # to lie about what's available. The user's explicit toggle still controls
    # whether the corresponding system-prompt section is injected, so an
    # all-off path stays clean.
    user_kb_effective = bool(body.user_kb_enabled or kb_effective)

    if kb_effective or user_kb_effective:
        system_prompt = (system_prompt + "\n\n" + KB_SYSTEM_PROMPT).strip()
        all_tools.extend(KB_TOOLS)
    if user_kb_effective:
        system_prompt = (system_prompt + "\n\n" + USER_KB_SYSTEM_PROMPT).strip()
        all_tools.extend(USER_KB_TOOLS)
        # Bind the user id for the tool dispatcher. Even though search/fetch
        # are team-shared (no user_id filter applied), the contextvar is read
        # by audit logging to attribute the call to the requesting user.
        _user_kb_svc.set_current_user_id(str(user.id))

    # Revenue-modeling trigger tool — always available so the chat can
    # bootstrap a structured model on explicit user ask.
    system_prompt = (system_prompt + "\n\n" + TRIGGER_REVENUE_MODEL_SYSTEM_PROMPT).strip()
    all_tools.extend(TRIGGER_REVENUE_MODEL_TOOLS)

    # Add research strategy guidance whenever any KB is wired in. The strategy
    # prompt mandates per-round dual KB calls, so it must be present whenever
    # *either* knowledge base is available.
    if kb_effective or user_kb_effective:
        system_prompt = (system_prompt + "\n\n" + _RESEARCH_STRATEGY_PROMPT).strip()

    # Time-awareness: any tool that returns dated content needs this guidance.
    # Without it, models frequently cite a 1-year-old analyst report as if it
    # describes "current" state, mixing realized/forecasted numbers freely.
    if (
        body.web_search in ("on", "auto")
        or kb_effective
        or user_kb_effective
    ):
        system_prompt = (system_prompt + "\n\n" + _build_time_awareness_prompt()).strip()

    # Long-term user memories (distilled from prior feedback). Prepend at the
    # very top so it is the first thing the model sees — critical for
    # "corrections" to take effect on the current turn.
    memory_prompt_block = ""
    memory_ids_used: list[uuid.UUID] = []
    try:
        from backend.app.services.chat_memory_service import build_user_memory_prompt
        memory_prompt_block, memory_ids_used = await build_user_memory_prompt(db, user.id)
    except Exception:
        logger.warning("build_user_memory_prompt failed — continuing without memory", exc_info=True)
    if memory_prompt_block:
        system_prompt = (memory_prompt_block + "\n" + system_prompt).strip()

    # Create debug trace for this request
    trace = chat_trace(
        user_id=str(user.id),
        username=getattr(user, "username", ""),
        conversation_id=str(conv.id),
        message_id=str(msg_id),
    )
    trace_id = trace.trace_id

    tool_names = [t.get("function", {}).get("name", "?") for t in all_tools]
    trace.log_request_start(
        content=body.content,
        models=body.models,
        mode=body.mode or "standard",
        web_search=body.web_search or "off",
        alphapai_enabled=body.alphapai_enabled,
        jinmen_enabled=body.jinmen_enabled,
        system_prompt_len=len(system_prompt),
        tools_count=len(all_tools),
        tool_names=tool_names,
        history_len=len(history),
    )

    logger.info(
        "Chat stream [%s]: web_search=%s alphapai=%s jinmen=%s tools=%d sys_prompt_len=%d models=%s mode=%s",
        trace_id, body.web_search, body.alphapai_enabled, body.jinmen_enabled, len(all_tools),
        len(system_prompt), body.models, body.mode,
    )

    messages_payload = _build_messages(history, user_content, system_prompt or None)
    mode = body.mode if body.mode in MODE_CONFIGS else "standard"

    # Log the full messages payload for deep debugging
    trace.log_messages_payload(messages_payload)

    # Auto-generate title in background (don't block the SSE stream)
    is_first = conv.title == "新对话"
    if is_first:
        conv.title = body.content[:20].strip() + ("..." if len(body.content) > 20 else "")
    conv.updated_at = datetime.now(timezone.utc)
    await db.commit()
    if is_first:
        asyncio.create_task(_generate_title_background(conv.id, body.content))

    # SSE event stream from all models concurrently
    async def event_stream():
        import time as _time
        _stream_start = _time.monotonic()
        queue: asyncio.Queue = asyncio.Queue()
        model_ids = body.models
        # Track partial content per model for saving on disconnect
        model_partial: dict[str, str] = {m: "" for m in model_ids}

        async def stream_one_model(model_id: str):
            # Create per-model trace
            from backend.app.services.chat_debug import chat_trace as _ct
            model_trace = _ct(
                user_id=str(user.id),
                username=getattr(user, "username", ""),
                conversation_id=str(conv.id),
                message_id=str(msg_id),
                model_id=model_id,
                trace_id=trace_id,
            )
            model_trace.log_sse_event("MODEL_STREAM_START", f"model={model_id}")
            try:
                async for chunk in call_model_stream_with_tools(
                    model_id, messages_payload, mode=mode, tools=all_tools or None,
                    trace_id=trace_id,
                ):
                    await queue.put({"model": model_id, **chunk})
            except asyncio.CancelledError:
                # Client disconnected — emit done so save logic runs
                await queue.put({
                    "model": model_id,
                    "delta": "",
                    "done": True,
                    "error": "[客户端断开连接]",
                    "content": model_partial.get(model_id, ""),
                    "tokens": 0,
                    "latency_ms": 0,
                })
            except Exception as e:
                logger.exception("stream_one_model crashed for %s", model_id)
                await queue.put({
                    "model": model_id,
                    "delta": "",
                    "done": True,
                    "error": f"模型调用异常: {str(e)[:200]}",
                    "content": model_partial.get(model_id, ""),
                    "tokens": 0,
                    "latency_ms": 0,
                })

        tasks = [asyncio.create_task(stream_one_model(m)) for m in model_ids]

        # Send message_id + enabled features status
        meta = {'type': 'meta', 'message_id': str(msg_id)}
        if body.web_search in ("on", "auto"):
            meta['web_search'] = body.web_search
        if body.alphapai_enabled:
            meta['alphapai_enabled'] = True
        if body.jinmen_enabled:
            meta['jinmen_enabled'] = True
        if memory_ids_used:
            meta['memory_ids'] = [str(mid) for mid in memory_ids_used]
        yield f"data: {json.dumps(meta)}\n\n"

        done_count = 0
        completed_models: set[str] = set()
        model_sources: dict[str, list] = {}  # track citation sources per model
        disconnected = False
        try:
            while done_count < len(model_ids):
                # Check client disconnect every queue poll
                if await request.is_disconnected():
                    logger.info("Client disconnected during SSE stream for msg %s", msg_id)
                    disconnected = True
                    break

                try:
                    item = await asyncio.wait_for(queue.get(), timeout=30.0)
                except asyncio.TimeoutError:
                    # Check if client is still connected
                    if await request.is_disconnected():
                        logger.info("Client disconnected (detected on timeout) for msg %s", msg_id)
                        disconnected = True
                        break
                    # Send keepalive comment to prevent proxy/browser timeout
                    yield ": keepalive\n\n"
                    continue

                model_id = item.get("model", "")
                model_info = MODEL_MAP.get(model_id, {"name": model_id})

                if item.get("type") == "heartbeat":
                    yield ": heartbeat\n\n"
                elif item.get("type") == "tool_status":
                    yield f"data: {json.dumps({'type': 'tool_status', 'model': model_id, 'tool_name': item.get('tool_name', ''), 'status': item.get('status', '')}, ensure_ascii=False)}\n\n"
                elif item.get("type") == "search_status":
                    yield f"data: {json.dumps({'type': 'search_status', 'model': model_id, 'query': item.get('query', ''), 'status': item.get('status', '')}, ensure_ascii=False)}\n\n"
                elif item.get("type") == "read_status":
                    yield f"data: {json.dumps({'type': 'read_status', 'model': model_id, 'url': item.get('url', ''), 'status': item.get('status', '')}, ensure_ascii=False)}\n\n"
                elif item.get("type") == "sources":
                    model_sources[model_id] = item.get("sources", [])
                    yield f"data: {json.dumps({'type': 'sources', 'model': model_id, 'sources': item.get('sources', [])}, ensure_ascii=False)}\n\n"
                elif item.get("done"):
                    done_count += 1
                    completed_models.add(model_id)

                    # Log model completion via debug trace
                    from backend.app.services.chat_debug import chat_trace as _ct2
                    done_trace = _ct2(
                        user_id=str(user.id), conversation_id=str(conv.id),
                        message_id=str(msg_id),
                        model_id=model_id, trace_id=trace_id,
                    )
                    done_trace.log_llm_done(
                        content_len=len(item.get("content", "")),
                        tokens=item.get("tokens", 0),
                        latency_ms=item.get("latency_ms", 0),
                        error=item.get("error"),
                    )
                    done_trace.log_llm_response_content(item.get("content", ""))

                    # Save to DB — wrapped in try/except so one model's save
                    # failure doesn't abort the entire SSE stream and lose
                    # other models' responses.
                    response_id = ""
                    save_error = item.get("error")
                    try:
                        async with (await _get_session()) as save_db:
                            resp = ChatModelResponse(
                                message_id=msg_id,
                                model_id=model_id,
                                model_name=model_info.get("name", model_id),
                                content=item.get("content", ""),
                                tokens_used=item.get("tokens"),
                                latency_ms=item.get("latency_ms"),
                                error=item.get("error"),
                                sources=model_sources.get(model_id),
                            )
                            save_db.add(resp)
                            await save_db.commit()
                            await save_db.refresh(resp)
                            response_id = str(resp.id)
                    except Exception as save_exc:
                        logger.exception("Failed to save response for model %s msg %s", model_id, msg_id)
                        if not save_error:
                            save_error = f"保存失败: {str(save_exc)[:100]}"

                    yield f"data: {json.dumps({'type': 'done', 'model': model_id, 'model_name': model_info.get('name', model_id), 'response_id': response_id, 'tokens': item.get('tokens', 0), 'latency_ms': item.get('latency_ms', 0), 'error': save_error}, ensure_ascii=False)}\n\n"
                else:
                    # Delta — track partial content
                    delta = item.get("delta", "")
                    if delta:
                        model_partial[model_id] = model_partial.get(model_id, "") + delta
                    yield f"data: {json.dumps({'type': 'delta', 'model': model_id, 'delta': delta}, ensure_ascii=False)}\n\n"

        except asyncio.CancelledError:
            logger.info("SSE generator cancelled (client disconnect) for msg %s", msg_id)
            disconnected = True
        except Exception as e:
            logger.exception("SSE stream error")
            yield f"data: {json.dumps({'type': 'error', 'error': str(e)[:200]})}\n\n"
            for mid in model_ids:
                if mid not in completed_models:
                    mi = MODEL_MAP.get(mid, {"name": mid})
                    yield f"data: {json.dumps({'type': 'done', 'model': mid, 'model_name': mi.get('name', mid), 'response_id': '', 'tokens': 0, 'latency_ms': 0, 'error': str(e)[:100]}, ensure_ascii=False)}\n\n"
        finally:
            # Cancel all model tasks
            for t in tasks:
                t.cancel()

            # Save partial responses for models that didn't complete (client disconnect or timeout)
            if disconnected:
                for mid in model_ids:
                    if mid not in completed_models and model_partial.get(mid):
                        try:
                            mi = MODEL_MAP.get(mid, {"name": mid})
                            async with (await _get_session()) as save_db:
                                resp = ChatModelResponse(
                                    message_id=msg_id,
                                    model_id=mid,
                                    model_name=mi.get("name", mid),
                                    content=model_partial[mid],
                                    error="[客户端断开连接，内容不完整]",
                                )
                                save_db.add(resp)
                                await save_db.commit()
                            logger.info("Saved partial response for %s (%d chars)", mid, len(model_partial[mid]))
                        except Exception:
                            logger.warning("Failed to save partial response for %s", mid)

            # Log request end
            total_ms = int((_time.monotonic() - _stream_start) * 1000)
            trace.log_request_end(total_ms)

            # Bump usage_count on memories that got injected into this turn
            # (best-effort; opens its own session — `db` is closed by now).
            if memory_ids_used:
                try:
                    from backend.app.services.chat_memory_service import mark_memories_used
                    async with (await _get_session()) as _mark_db:
                        await mark_memories_used(_mark_db, memory_ids_used)
                except Exception:
                    logger.debug("mark_memories_used failed", exc_info=True)

        yield "data: [DONE]\n\n"

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


async def _get_session():
    """Get a new async session for background DB writes."""
    from backend.app.core.database import async_session_factory
    return async_session_factory()


async def _generate_title_background(conv_id: str, content: str):
    """Generate a proper title in the background without blocking SSE stream."""
    try:
        title = await generate_title(content)
        if title and title != "新对话":
            async with (await _get_session()) as db:
                await db.execute(
                    update(ChatConversation)
                    .where(ChatConversation.id == conv_id)
                    .values(title=title)
                )
                await db.commit()
    except Exception:
        logger.warning("Background title generation failed for conv %s", conv_id)


# ── Cancel (save partial) ─────────────────────────────────────────

@router.post("/conversations/{conv_id}/messages/{msg_id}/save-partial")
async def save_partial_responses(
    conv_id: str,
    msg_id: str,
    body: SavePartialRequest,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Save partial model responses when user cancels a streaming request."""
    conv = await db.scalar(
        select(ChatConversation)
        .where(ChatConversation.id == conv_id, ChatConversation.user_id == user.id)
    )
    if not conv:
        raise HTTPException(404, "Conversation not found")

    msg = await db.scalar(
        select(ChatMessage)
        .where(ChatMessage.id == msg_id, ChatMessage.conversation_id == conv_id)
    )
    if not msg:
        raise HTTPException(404, "Message not found")

    saved = 0
    for model_id, content in body.partial_responses.items():
        if not content.strip():
            continue
        # Skip if a complete response already exists (saved by backend before abort)
        existing = await db.scalar(
            select(ChatModelResponse)
            .where(ChatModelResponse.message_id == msg.id, ChatModelResponse.model_id == model_id)
        )
        if existing:
            continue

        model_info = MODEL_MAP.get(model_id, {"name": model_id})
        resp = ChatModelResponse(
            message_id=msg.id,
            model_id=model_id,
            model_name=model_info.get("name", model_id),
            content=content,
            error="[已停止生成]",
        )
        db.add(resp)
        saved += 1

    if saved > 0:
        await db.commit()

    return {"saved": saved}


# ── Regenerate ────────────────────────────────────────────────────

@router.post("/conversations/{conv_id}/messages/{msg_id}/regenerate")
async def regenerate_message_stream(
    conv_id: str,
    msg_id: str,
    body: RegenerateRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Regenerate model responses for an existing user message (streaming SSE)."""
    conv = await db.scalar(
        select(ChatConversation)
        .where(ChatConversation.id == conv_id, ChatConversation.user_id == user.id)
    )
    if not conv:
        raise HTTPException(404, "Conversation not found")

    msg = await db.scalar(
        select(ChatMessage)
        .where(ChatMessage.id == msg_id, ChatMessage.conversation_id == conv_id, ChatMessage.role == "user")
    )
    if not msg:
        raise HTTPException(404, "User message not found")

    # Delete old model responses for this message
    await db.execute(
        delete(ChatModelResponse).where(ChatModelResponse.message_id == msg.id)
    )

    user_content = build_multimodal_content(msg.content, msg.attachments or [])

    # Build conversation history (excluding the message being regenerated)
    history_result = await db.execute(
        select(ChatMessage)
        .where(ChatMessage.conversation_id == conv.id)
        .options(selectinload(ChatMessage.model_responses))
        .order_by(ChatMessage.created_at)
    )
    history_msgs = history_result.scalars().all()

    history = []
    for hm in history_msgs:
        if hm.id == msg.id:
            continue
        if hm.role == "user":
            history.append({"role": "user", "content": hm.content})
            best = _best_response(hm.model_responses)
            if best and best.content:
                history.append({"role": "assistant", "content": best.content})

    # Build system prompt and tools (same logic as send_message_stream)
    system_prompt = body.system_prompt or ""
    all_tools: list[dict] = []

    if body.web_search in ("on", "auto"):
        system_prompt = (system_prompt + "\n\n" + WEB_SEARCH_SYSTEM_PROMPT).strip()
        all_tools.extend(WEB_SEARCH_TOOLS)
        if body.web_search == "on":
            system_prompt += WEB_SEARCH_FORCE_PROMPT

    if body.alphapai_enabled:
        system_prompt = (system_prompt + "\n\n" + ALPHAPAI_SYSTEM_PROMPT).strip()
        all_tools.extend(ALPHAPAI_TOOLS)
    if body.jinmen_enabled:
        system_prompt = (system_prompt + "\n\n" + JINMEN_SYSTEM_PROMPT).strip()
        all_tools.extend(JINMEN_TOOLS)

    # Same paired-KB wiring as send_message_stream — see the comment there for
    # why both KB toolsets get exposed when either toggle is on.
    kb_effective = bool(body.kb_enabled or body.alphapai_enabled or body.jinmen_enabled)
    user_kb_effective = bool(body.user_kb_enabled or kb_effective)
    if kb_effective or user_kb_effective:
        system_prompt = (system_prompt + "\n\n" + KB_SYSTEM_PROMPT).strip()
        all_tools.extend(KB_TOOLS)
    if user_kb_effective:
        system_prompt = (system_prompt + "\n\n" + USER_KB_SYSTEM_PROMPT).strip()
        all_tools.extend(USER_KB_TOOLS)
        _user_kb_svc.set_current_user_id(str(user.id))

    messages_payload = _build_messages(history, user_content, system_prompt or None)
    mode = body.mode if body.mode in MODE_CONFIGS else "standard"

    conv.updated_at = datetime.now(timezone.utc)
    await db.commit()

    async def event_stream():
        queue: asyncio.Queue = asyncio.Queue()
        model_ids = body.models
        model_partial: dict[str, str] = {m: "" for m in model_ids}

        async def stream_one_model(model_id: str):
            try:
                async for chunk in call_model_stream_with_tools(
                    model_id, messages_payload, mode=mode, tools=all_tools or None,
                ):
                    await queue.put({"model": model_id, **chunk})
            except asyncio.CancelledError:
                await queue.put({
                    "model": model_id, "delta": "", "done": True,
                    "error": "[客户端断开连接]",
                    "content": model_partial.get(model_id, ""),
                    "tokens": 0, "latency_ms": 0,
                })
            except Exception as e:
                logger.exception("stream_one_model crashed for %s (regenerate)", model_id)
                await queue.put({
                    "model": model_id, "delta": "", "done": True,
                    "error": f"模型调用异常: {str(e)[:200]}",
                    "content": model_partial.get(model_id, ""),
                    "tokens": 0, "latency_ms": 0,
                })

        tasks = [asyncio.create_task(stream_one_model(m)) for m in model_ids]

        yield f"data: {json.dumps({'type': 'meta', 'message_id': str(msg.id)})}\n\n"

        done_count = 0
        completed_models: set[str] = set()
        model_sources: dict[str, list] = {}
        disconnected = False
        try:
            while done_count < len(model_ids):
                if await request.is_disconnected():
                    logger.info("Client disconnected during regenerate SSE for msg %s", msg.id)
                    disconnected = True
                    break

                try:
                    item = await asyncio.wait_for(queue.get(), timeout=30.0)
                except asyncio.TimeoutError:
                    if await request.is_disconnected():
                        disconnected = True
                        break
                    yield ": keepalive\n\n"
                    continue

                model_id = item.get("model", "")
                model_info = MODEL_MAP.get(model_id, {"name": model_id})

                if item.get("type") == "heartbeat":
                    yield ": heartbeat\n\n"
                elif item.get("type") == "tool_status":
                    yield f"data: {json.dumps({'type': 'tool_status', 'model': model_id, 'tool_name': item.get('tool_name', ''), 'status': item.get('status', '')}, ensure_ascii=False)}\n\n"
                elif item.get("type") == "search_status":
                    yield f"data: {json.dumps({'type': 'search_status', 'model': model_id, 'query': item.get('query', ''), 'status': item.get('status', '')}, ensure_ascii=False)}\n\n"
                elif item.get("type") == "read_status":
                    yield f"data: {json.dumps({'type': 'read_status', 'model': model_id, 'url': item.get('url', ''), 'status': item.get('status', '')}, ensure_ascii=False)}\n\n"
                elif item.get("type") == "sources":
                    model_sources[model_id] = item.get("sources", [])
                    yield f"data: {json.dumps({'type': 'sources', 'model': model_id, 'sources': item.get('sources', [])}, ensure_ascii=False)}\n\n"
                elif item.get("done"):
                    done_count += 1
                    completed_models.add(model_id)
                    response_id = ""
                    save_error = item.get("error")
                    try:
                        async with (await _get_session()) as save_db:
                            resp = ChatModelResponse(
                                message_id=msg.id,
                                model_id=model_id,
                                model_name=model_info.get("name", model_id),
                                content=item.get("content", ""),
                                tokens_used=item.get("tokens"),
                                latency_ms=item.get("latency_ms"),
                                error=item.get("error"),
                                sources=model_sources.get(model_id),
                            )
                            save_db.add(resp)
                            await save_db.commit()
                            await save_db.refresh(resp)
                            response_id = str(resp.id)
                    except Exception as save_exc:
                        logger.exception("Failed to save regenerated response for model %s", model_id)
                        if not save_error:
                            save_error = f"保存失败: {str(save_exc)[:100]}"

                    yield f"data: {json.dumps({'type': 'done', 'model': model_id, 'model_name': model_info.get('name', model_id), 'response_id': response_id, 'tokens': item.get('tokens', 0), 'latency_ms': item.get('latency_ms', 0), 'error': save_error}, ensure_ascii=False)}\n\n"
                else:
                    delta = item.get("delta", "")
                    if delta:
                        model_partial[model_id] = model_partial.get(model_id, "") + delta
                    yield f"data: {json.dumps({'type': 'delta', 'model': model_id, 'delta': delta}, ensure_ascii=False)}\n\n"

        except asyncio.CancelledError:
            logger.info("SSE generator cancelled (regenerate) for msg %s", msg.id)
            disconnected = True
        except Exception as e:
            logger.exception("SSE stream error (regenerate)")
            yield f"data: {json.dumps({'type': 'error', 'error': str(e)[:200]})}\n\n"
            for mid in model_ids:
                if mid not in completed_models:
                    mi = MODEL_MAP.get(mid, {"name": mid})
                    yield f"data: {json.dumps({'type': 'done', 'model': mid, 'model_name': mi.get('name', mid), 'response_id': '', 'tokens': 0, 'latency_ms': 0, 'error': str(e)[:100]}, ensure_ascii=False)}\n\n"
        finally:
            for t in tasks:
                t.cancel()
            if disconnected:
                for mid in model_ids:
                    if mid not in completed_models and model_partial.get(mid):
                        try:
                            mi = MODEL_MAP.get(mid, {"name": mid})
                            async with (await _get_session()) as save_db:
                                resp = ChatModelResponse(
                                    message_id=msg.id,
                                    model_id=mid,
                                    model_name=mi.get("name", mid),
                                    content=model_partial[mid],
                                    error="[客户端断开连接，内容不完整]",
                                )
                                save_db.add(resp)
                                await save_db.commit()
                        except Exception:
                            logger.warning("Failed to save partial response for %s (regenerate)", mid)

        yield "data: [DONE]\n\n"

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


# ── Debate mode ─────────────────────────────────────────────────

from backend.app.services.debate_prompts import (
    detect_topic_type,
    get_bull_bear_prompts,
    get_multi_perspective_prompts,
    get_round_robin_prompt,
    MULTI_PERSPECTIVE_ROLES,
    DEBATE_SUMMARY_PROMPT,
)

DEBATE_ROLES = {1: "看多方", 2: "质疑方", 3: "综合判断"}


async def _save_debate_round(
    msg_id: str, model_id: str, model_name: str,
    content: str, tokens_used: int, latency_ms: int,
    error_text: str | None, debate_round: int,
) -> str:
    """Save a debate round response to DB. Returns response ID."""
    try:
        async with (await _get_session()) as save_db:
            resp = ChatModelResponse(
                message_id=msg_id,
                model_id=model_id,
                model_name=model_name,
                content=content,
                tokens_used=tokens_used,
                latency_ms=latency_ms,
                error=error_text,
                debate_round=debate_round,
            )
            save_db.add(resp)
            await save_db.flush()
            resp_id = str(resp.id)
            await save_db.commit()
            return resp_id
    except Exception:
        logger.exception("Failed to save debate round %d", debate_round)
        return ""


async def _stream_one_round(
    round_num: int, role: str, model_id: str, model_info: dict,
    messages_payload: list[dict], msg_id: str,
):
    """Stream a single debate round. Yields SSE events, then a _result dict."""
    yield f"data: {json.dumps({'type': 'round_start', 'round': round_num, 'role': role, 'model': model_id, 'model_name': model_info.get('name', model_id)})}\n\n"

    full_content = ""
    tokens_used = 0
    latency_ms = 0
    error_text = None

    async for chunk in call_model_stream(model_id, messages_payload, mode="thinking"):
        if chunk.get("done"):
            full_content = chunk.get("content", full_content)
            tokens_used = chunk.get("tokens", 0)
            latency_ms = chunk.get("latency_ms", 0)
            error_text = chunk.get("error")
        else:
            delta = chunk.get("delta", "")
            if delta:
                full_content += delta
                yield f"data: {json.dumps({'type': 'delta', 'model': model_id, 'delta': delta, 'debate_round': round_num})}\n\n"

    resp_id = await _save_debate_round(
        msg_id, model_id, model_info.get("name", model_id),
        full_content, tokens_used, latency_ms, error_text, round_num,
    )

    yield f"data: {json.dumps({'type': 'done', 'model': model_id, 'model_name': model_info.get('name', model_id), 'debate_round': round_num, 'response_id': resp_id, 'tokens': tokens_used, 'latency_ms': latency_ms, 'error': error_text})}\n\n"

    # Attach result metadata so the caller can access it
    yield {"_result": {"content": full_content, "error": error_text, "tokens": tokens_used, "latency_ms": latency_ms}}


@router.post("/conversations/{conv_id}/messages/debate")
async def send_debate_message(
    conv_id: str,
    body: DebateRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Multi-model debate with flexible formats: bull_bear, multi_perspective, round_robin."""
    conv = await db.get(ChatConversation, conv_id)
    if not conv or str(conv.user_id) != str(user.id):
        raise HTTPException(404, "Conversation not found")

    user_content = build_multimodal_content(body.content, body.attachments)

    # Save user message
    msg = ChatMessage(
        conversation_id=conv_id,
        role="user",
        content=body.content,
        attachments=body.attachments,
        is_debate=True,
    )
    db.add(msg)
    await db.flush()
    msg_id = msg.id

    # Load conversation history (last 5 exchanges for context)
    history_result = await db.execute(
        select(ChatMessage)
        .where(ChatMessage.conversation_id == conv.id)
        .options(selectinload(ChatMessage.model_responses))
        .order_by(ChatMessage.created_at)
    )
    history_msgs = history_result.scalars().all()
    history = []
    for hm in history_msgs:
        if hm.id == msg_id:
            continue
        if hm.role == "user":
            history.append({"role": "user", "content": hm.content})
            best = _best_response(hm.model_responses)
            if best and best.content:
                history.append({"role": "assistant", "content": best.content})
    history = history[-10:]  # last 5 exchanges

    # Auto-generate title in background
    is_first = conv.title == "新对话"
    if is_first:
        conv.title = body.content[:20].strip() + ("..." if len(body.content) > 20 else "")
    conv.updated_at = datetime.now(timezone.utc)
    await db.commit()
    if is_first:
        asyncio.create_task(_generate_title_background(conv.id, body.content))

    # Detect topic type for prompt customization
    topic_type = detect_topic_type(body.content)
    debate_format = body.debate_format if body.debate_format in ("bull_bear", "multi_perspective", "round_robin") else "bull_bear"

    async def event_stream():
        yield f"data: {json.dumps({'type': 'meta', 'message_id': str(msg_id), 'debate_format': debate_format, 'topic_type': topic_type})}\n\n"

        # Web search (if enabled)
        search_context = None
        if body.web_search:
            yield f"data: {json.dumps({'type': 'web_search_start'})}\n\n"
            search_context = await search_for_chat(body.content)
            yield f"data: {json.dumps({'type': 'web_search_done', 'has_results': bool(search_context)})}\n\n"

        if debate_format == "multi_perspective":
            async for event in _run_multi_perspective(body, msg_id, user_content, history, search_context, topic_type):
                yield event
        elif debate_format == "round_robin":
            async for event in _run_round_robin(body, msg_id, user_content, history, search_context):
                yield event
        else:
            async for event in _run_bull_bear(body, msg_id, user_content, history, search_context, topic_type):
                yield event

        yield f"data: {json.dumps({'type': 'all_done'})}\n\n"
        yield "data: [DONE]\n\n"

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


async def _run_bull_bear(body, msg_id, user_content, history, search_context, topic_type):
    """Bull/Bear debate format: sequential rounds with context chaining."""
    prompts = get_bull_bear_prompts(topic_type)
    roles = {1: "看多方", 2: "质疑方", 3: "综合判断"}
    num_rounds = len(body.debate_models)
    round_contents: dict[int, str] = {}

    for round_num in range(1, num_rounds + 1):
        model_id = body.debate_models[round_num - 1]
        model_info = MODEL_MAP.get(model_id, {"name": model_id})
        role = roles.get(round_num, f"Round {round_num}")

        # Build round-specific system prompt
        if round_num == 1:
            sys_prompt = prompts[1]
        elif round_num == 2:
            sys_prompt = prompts[2].format(prev_content=round_contents.get(1, ""))
        else:
            sys_prompt = prompts[3].format(
                round1_content=round_contents.get(1, ""),
                round2_content=round_contents.get(2, ""),
            )

        if body.system_prompt:
            sys_prompt = body.system_prompt + "\n\n" + sys_prompt

        sc = search_context if round_num == 1 else None
        messages_payload = _build_messages(history, user_content, sys_prompt, sc)

        result = None
        async for event in _stream_one_round(round_num, role, model_id, model_info, messages_payload, msg_id):
            if isinstance(event, dict) and "_result" in event:
                result = event["_result"]
            else:
                yield event

        if result:
            round_contents[round_num] = result["content"]
            if result["error"] and round_num < num_rounds:
                err_reason = f"Round {round_num} failed: {result['error']}"
                yield f"data: {json.dumps({'type': 'debate_aborted', 'reason': err_reason, 'completed_rounds': round_num})}\n\n"
                break


async def _run_multi_perspective(body, msg_id, user_content, history, search_context, topic_type):
    """Multi-perspective format: parallel analysts then sequential synthesis."""
    prompts = get_multi_perspective_prompts(topic_type)
    num_models = len(body.debate_models)
    num_analysts = min(num_models - 1, 3) if num_models > 1 else num_models
    has_synthesis = num_models > num_analysts

    # Phase 1: Run analyst models in parallel
    analyst_results: dict[int, str] = {}
    analyst_queues: dict[int, asyncio.Queue] = {}

    async def run_analyst(round_num: int, model_id: str):
        model_info = MODEL_MAP.get(model_id, {"name": model_id})
        role = MULTI_PERSPECTIVE_ROLES.get(round_num, f"分析师 {round_num}")
        sys_prompt = prompts.get(round_num, prompts[1])
        if body.system_prompt:
            sys_prompt = body.system_prompt + "\n\n" + sys_prompt
        sc = search_context if round_num == 1 else None
        messages_payload = _build_messages(history, user_content, sys_prompt, sc)

        q = analyst_queues[round_num]
        async for event in _stream_one_round(round_num, role, model_id, model_info, messages_payload, msg_id):
            if isinstance(event, dict) and "_result" in event:
                analyst_results[round_num] = event["_result"]["content"]
            else:
                await q.put(event)
        await q.put(None)  # signal done

    for i in range(1, num_analysts + 1):
        analyst_queues[i] = asyncio.Queue()

    tasks = [
        asyncio.create_task(run_analyst(i, body.debate_models[i - 1]))
        for i in range(1, num_analysts + 1)
    ]

    # Interleave parallel output from all analyst queues
    done_count = 0
    while done_count < num_analysts:
        for rn in range(1, num_analysts + 1):
            q = analyst_queues[rn]
            while not q.empty():
                event = q.get_nowait()
                if event is None:
                    done_count += 1
                else:
                    yield event
        await asyncio.sleep(0.05)

    # Drain remaining events
    for rn in range(1, num_analysts + 1):
        while not analyst_queues[rn].empty():
            event = analyst_queues[rn].get_nowait()
            if event is not None:
                yield event

    await asyncio.gather(*tasks, return_exceptions=True)

    # Phase 2: Synthesis round (sees all analyst outputs)
    if has_synthesis:
        synthesis_round = num_analysts + 1
        synthesis_model = body.debate_models[num_analysts]
        model_info = MODEL_MAP.get(synthesis_model, {"name": synthesis_model})
        role = MULTI_PERSPECTIVE_ROLES.get(4, "综合判断")

        sys_prompt = prompts.get(4, prompts[1]).format(
            round1_content=analyst_results.get(1, ""),
            round2_content=analyst_results.get(2, ""),
            round3_content=analyst_results.get(3, ""),
        )
        if body.system_prompt:
            sys_prompt = body.system_prompt + "\n\n" + sys_prompt

        messages_payload = _build_messages(history, user_content, sys_prompt)

        async for event in _stream_one_round(synthesis_round, role, synthesis_model, model_info, messages_payload, msg_id):
            if isinstance(event, dict) and "_result" in event:
                pass
            else:
                yield event


async def _run_round_robin(body, msg_id, user_content, history, search_context):
    """Round-robin format: models take turns, each sees all prior responses."""
    num_rounds = body.num_rounds or len(body.debate_models)
    num_models = len(body.debate_models)
    round_contents: dict[int, str] = {}

    for round_num in range(1, num_rounds + 1):
        model_idx = (round_num - 1) % num_models
        model_id = body.debate_models[model_idx]
        model_info = MODEL_MAP.get(model_id, {"name": model_id})
        role = f"Round {round_num}"

        sys_prompt = get_round_robin_prompt(round_num, round_contents)
        if body.system_prompt:
            sys_prompt = body.system_prompt + "\n\n" + sys_prompt

        sc = search_context if round_num == 1 else None
        messages_payload = _build_messages(history, user_content, sys_prompt, sc)

        result = None
        async for event in _stream_one_round(round_num, role, model_id, model_info, messages_payload, msg_id):
            if isinstance(event, dict) and "_result" in event:
                result = event["_result"]
            else:
                yield event

        if result:
            round_contents[round_num] = result["content"]
            if result["error"] and round_num < num_rounds:
                err_reason = f"Round {round_num} failed: {result['error']}"
                yield f"data: {json.dumps({'type': 'debate_aborted', 'reason': err_reason, 'completed_rounds': round_num})}\n\n"
                break


# ── Debate summary ────────────────────────────────────────────────

@router.post("/conversations/{conv_id}/messages/{msg_id}/debate-summary")
async def generate_debate_summary(
    conv_id: str,
    msg_id: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Extract structured summary from a completed debate."""
    conv = await db.get(ChatConversation, conv_id)
    if not conv or str(conv.user_id) != str(user.id):
        raise HTTPException(404, "Conversation not found")

    result = await db.execute(
        select(ChatModelResponse)
        .where(ChatModelResponse.message_id == msg_id, ChatModelResponse.debate_round > 0)
        .order_by(ChatModelResponse.debate_round)
    )
    responses = result.scalars().all()
    if not responses:
        raise HTTPException(404, "No debate responses found")

    debate_text = ""
    for resp in responses:
        round_label = DEBATE_ROLES.get(resp.debate_round, f"Round {resp.debate_round}")
        debate_text += f"\n\n【{round_label} - {resp.model_name}】\n{resp.content}"

    prompt = DEBATE_SUMMARY_PROMPT.format(debate_content=debate_text)

    summary_result = await call_model_sync(
        "openai/gpt-4o-mini",
        [{"role": "user", "content": prompt}],
        mode="fast",
    )

    summary_text = summary_result.get("content", "")
    try:
        import re as _re
        json_match = _re.search(r'\{[\s\S]*\}', summary_text)
        if json_match:
            summary_data = json.loads(json_match.group())
        else:
            summary_data = json.loads(summary_text)
        summary = DebateSummary(**summary_data)
    except (json.JSONDecodeError, Exception):
        logger.warning("Failed to parse debate summary JSON, returning raw text")
        summary = DebateSummary(conclusion=summary_text[:200])

    # Save summary as special model response (debate_round = -1)
    try:
        async with (await _get_session()) as save_db:
            summary_resp = ChatModelResponse(
                message_id=msg_id,
                model_id="system/debate-summary",
                model_name="辩论总结",
                content=json.dumps(summary.model_dump(), ensure_ascii=False),
                tokens_used=summary_result.get("tokens", 0),
                latency_ms=summary_result.get("latency_ms", 0),
                debate_round=-1,
            )
            save_db.add(summary_resp)
            await save_db.commit()
    except Exception:
        logger.exception("Failed to save debate summary")

    return summary.model_dump()


# ── Rating ──────────────────────────────────────────────────────

@router.post("/rate/{response_id}", response_model=RateResponse)
async def rate_response(
    response_id: str,
    body: RateRequest,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Record a star rating on a model response.

    Also emits a ChatFeedbackEvent so the background memory extractor sees
    this as a signal. Detailed qualitative feedback (tags + text) uses the
    separate /chat-memory/feedback/{response_id} endpoint.
    """
    resp = await db.scalar(select(ChatModelResponse).where(ChatModelResponse.id == response_id))
    if not resp:
        raise HTTPException(404, "Response not found")

    # Verify user owns this conversation
    msg = await db.scalar(select(ChatMessage).where(ChatMessage.id == resp.message_id))
    if not msg:
        raise HTTPException(404)
    conv = await db.scalar(
        select(ChatConversation)
        .where(ChatConversation.id == msg.conversation_id, ChatConversation.user_id == user.id)
    )
    if not conv:
        raise HTTPException(403, "Not your conversation")

    resp.rating = body.rating
    if body.comment is not None:
        resp.rating_comment = body.comment

    # Emit a feedback event for the background memory daemon to consume.
    # The extractor is already rate-limited and only produces memories when
    # there's a real signal (rating alone with no text rarely distills to a
    # memory, which is the intended behavior).
    from backend.app.models.chat_memory import ChatFeedbackEvent
    from backend.app.services.chat_memory_extractor import _sentiment_from_signals
    sentiment = _sentiment_from_signals(body.rating, body.comment or "", [])
    fb_event = ChatFeedbackEvent(
        response_id=resp.id,
        user_id=user.id,
        rating=body.rating,
        feedback_text=(body.comment or "").strip(),
        feedback_tags=[],
        sentiment=sentiment,
        processed=False,
    )
    db.add(fb_event)
    await db.commit()
    return RateResponse(id=str(resp.id), rating=resp.rating, rating_comment=resp.rating_comment)


# ── Model rankings ──────────────────────────────────────────────

@router.get("/model-rankings", response_model=ModelRankingResponse)
async def get_model_rankings(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Get model rankings based on user ratings."""
    result = await db.execute(
        select(
            ChatModelResponse.model_id,
            ChatModelResponse.model_name,
            func.avg(ChatModelResponse.rating).label("avg_rating"),
            func.count(ChatModelResponse.rating).label("total_ratings"),
            func.count(ChatModelResponse.id).label("total_uses"),
        )
        .group_by(ChatModelResponse.model_id, ChatModelResponse.model_name)
        .having(func.count(ChatModelResponse.id) > 0)
        .order_by(desc("avg_rating"))
    )
    rows = result.all()
    return ModelRankingResponse(
        rankings=[
            ModelRanking(
                model_id=r.model_id, model_name=r.model_name,
                avg_rating=round(float(r.avg_rating or 0), 2),
                total_ratings=r.total_ratings, total_uses=r.total_uses,
            )
            for r in rows
        ]
    )


# ── Prompt templates ────────────────────────────────────────────

@router.get("/templates", response_model=list[TemplateResponse])
async def list_templates(
    category: str | None = None,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """List templates (system-wide + user's own)."""
    q = select(ChatPromptTemplate).where(
        (ChatPromptTemplate.is_system == True) | (ChatPromptTemplate.user_id == user.id)
    )
    if category:
        q = q.where(ChatPromptTemplate.category == category)
    q = q.order_by(desc(ChatPromptTemplate.usage_count), ChatPromptTemplate.name)

    result = await db.execute(q)
    templates = result.scalars().all()
    return [
        TemplateResponse(
            id=str(t.id), name=t.name, content=t.content, category=t.category,
            is_system=t.is_system, usage_count=t.usage_count, created_at=t.created_at,
        )
        for t in templates
    ]


@router.post("/templates", response_model=TemplateResponse, status_code=201)
async def create_template(
    body: TemplateCreate,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    tpl = ChatPromptTemplate(
        user_id=user.id,
        name=body.name,
        content=body.content,
        category=body.category,
        is_system=False,
    )
    db.add(tpl)
    await db.commit()
    await db.refresh(tpl)
    return TemplateResponse(
        id=str(tpl.id), name=tpl.name, content=tpl.content, category=tpl.category,
        is_system=tpl.is_system, usage_count=tpl.usage_count, created_at=tpl.created_at,
    )


@router.put("/templates/{tpl_id}", response_model=TemplateResponse)
async def update_template(
    tpl_id: str,
    body: TemplateUpdate,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    tpl = await db.scalar(
        select(ChatPromptTemplate)
        .where(ChatPromptTemplate.id == tpl_id, ChatPromptTemplate.user_id == user.id)
    )
    if not tpl:
        raise HTTPException(404, "Template not found")
    if body.name is not None:
        tpl.name = body.name
    if body.content is not None:
        tpl.content = body.content
    if body.category is not None:
        tpl.category = body.category
    await db.commit()
    await db.refresh(tpl)
    return TemplateResponse(
        id=str(tpl.id), name=tpl.name, content=tpl.content, category=tpl.category,
        is_system=tpl.is_system, usage_count=tpl.usage_count, created_at=tpl.created_at,
    )


@router.delete("/templates/{tpl_id}", status_code=204)
async def delete_template(
    tpl_id: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    tpl = await db.scalar(
        select(ChatPromptTemplate)
        .where(ChatPromptTemplate.id == tpl_id, ChatPromptTemplate.user_id == user.id)
    )
    if not tpl:
        raise HTTPException(404, "Template not found")
    await db.delete(tpl)
    await db.commit()


@router.post("/templates/{tpl_id}/use")
async def use_template(
    tpl_id: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Increment template usage count."""
    tpl = await db.scalar(
        select(ChatPromptTemplate).where(ChatPromptTemplate.id == tpl_id)
    )
    if not tpl:
        raise HTTPException(404, "Template not found")
    tpl.usage_count += 1
    await db.commit()
    return {"ok": True}


# ── File upload ─────────────────────────────────────────────────

@router.post("/upload", response_model=FileUploadResponse)
async def upload_file(
    file: UploadFile = File(...),
    user: User = Depends(get_current_user),
):
    """Upload an image or PDF for chat attachments."""
    if not file.filename:
        raise HTTPException(400, "No file provided")

    # Validate file type
    content_type = file.content_type or ""
    allowed_types = {
        "image/png", "image/jpeg", "image/gif", "image/webp",
        "application/pdf",
    }
    if content_type not in allowed_types:
        raise HTTPException(400, f"不支持的文件类型: {content_type}，支持 PNG/JPEG/GIF/WebP/PDF")

    # Limit file size (20MB)
    content = await file.read()
    if len(content) > 20 * 1024 * 1024:
        raise HTTPException(400, "文件大小不能超过20MB")

    # Save file
    ext = Path(file.filename).suffix
    fname = f"{uuid.uuid4().hex}{ext}"
    user_dir = UPLOAD_DIR / str(user.id)
    user_dir.mkdir(exist_ok=True)
    fpath = user_dir / fname
    fpath.write_bytes(content)

    # Store the real server-side path so build_multimodal_content can read it
    server_path = str(fpath)

    return FileUploadResponse(
        filename=file.filename,
        file_type=content_type,
        file_url=f"/api/chat/files/{user.id}/{fname}",
        file_path=server_path,
    )


@router.get("/files/{user_id}/{filename}")
async def serve_file(user_id: str, filename: str):
    """Serve uploaded chat files."""
    file_path = UPLOAD_DIR / user_id / filename
    if not file_path.exists():
        raise HTTPException(404, "File not found")

    from fastapi.responses import FileResponse
    return FileResponse(file_path)


# ── Export conversation ─────────────────────────────────────────

@router.get("/export/{conv_id}", response_model=ExportResponse)
async def export_conversation(
    conv_id: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Export conversation as markdown."""
    conv = await db.scalar(
        select(ChatConversation)
        .where(ChatConversation.id == conv_id, ChatConversation.user_id == user.id)
    )
    if not conv:
        raise HTTPException(404, "Conversation not found")

    msgs = await db.execute(
        select(ChatMessage)
        .where(ChatMessage.conversation_id == conv.id)
        .options(selectinload(ChatMessage.model_responses))
        .order_by(ChatMessage.created_at)
    )
    messages = msgs.scalars().all()

    md_parts = [f"# {conv.title}\n"]
    md_parts.append(f"导出时间: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n")
    if conv.tags:
        md_parts.append(f"标签: {', '.join(conv.tags)}\n")
    md_parts.append("---\n")

    for m in messages:
        if m.role == "user":
            md_parts.append(f"## 👤 用户\n\n{m.content}\n")
        for r in m.model_responses:
            rating_str = f" (评分: {'⭐' * r.rating})" if r.rating else ""
            md_parts.append(f"### 🤖 {r.model_name}{rating_str}\n\n{r.content}\n")
            if r.error:
                md_parts.append(f"> ⚠️ 错误: {r.error}\n")

    return ExportResponse(markdown="\n".join(md_parts), title=conv.title)


# ── Summarize conversation ──────────────────────────────────────

@router.post("/summarize/{conv_id}")
async def summarize_conversation(
    conv_id: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Generate a summary of the conversation using LLM."""
    conv = await db.scalar(
        select(ChatConversation)
        .where(ChatConversation.id == conv_id, ChatConversation.user_id == user.id)
    )
    if not conv:
        raise HTTPException(404, "Conversation not found")

    msgs = await db.execute(
        select(ChatMessage)
        .where(ChatMessage.conversation_id == conv.id)
        .options(selectinload(ChatMessage.model_responses))
        .order_by(ChatMessage.created_at)
    )
    messages = msgs.scalars().all()

    # Build conversation text
    text_parts = []
    for m in messages:
        if m.role == "user":
            text_parts.append(f"用户: {m.content}")
        for r in m.model_responses:
            text_parts.append(f"AI({r.model_name}): {r.content[:500]}")

    conv_text = "\n".join(text_parts)[:8000]

    result = await call_model_sync(
        "openai/gpt-4o-mini",
        [
            {"role": "system", "content": "你是一位股票研究助手。请用中文总结以下对话的要点，包括讨论了哪些股票/行业、得出了什么结论、有哪些值得关注的观点。用markdown格式输出。"},
            {"role": "user", "content": conv_text},
        ],
    )
    return {"summary": result.get("content", "总结生成失败")}


# ── Admin: chat statistics ──────────────────────────────────────

@router.get("/admin/stats", response_model=ChatStatsResponse)
async def get_chat_stats(
    db: AsyncSession = Depends(get_db),
    admin: User = Depends(get_current_admin),
):
    total_convs = await db.scalar(select(func.count(ChatConversation.id))) or 0
    total_msgs = await db.scalar(select(func.count(ChatMessage.id))) or 0
    total_calls = await db.scalar(select(func.count(ChatModelResponse.id))) or 0
    active_users = await db.scalar(
        select(func.count(func.distinct(ChatConversation.user_id)))
    ) or 0

    # Top models by usage
    top_models_q = await db.execute(
        select(
            ChatModelResponse.model_id,
            ChatModelResponse.model_name,
            func.count().label("count"),
            func.avg(ChatModelResponse.rating).label("avg_rating"),
        )
        .group_by(ChatModelResponse.model_id, ChatModelResponse.model_name)
        .order_by(desc("count"))
        .limit(10)
    )
    top_models = [
        {"model_id": r.model_id, "model_name": r.model_name,
         "count": r.count, "avg_rating": round(float(r.avg_rating or 0), 2)}
        for r in top_models_q.all()
    ]

    # Daily usage for the past 30 days
    from sqlalchemy import text as sa_text
    daily_usage_q = await db.execute(
        select(
            func.date_trunc("day", ChatMessage.created_at).label("date"),
            func.count(ChatMessage.id).label("message_count"),
            func.count(func.distinct(ChatConversation.user_id)).label("active_users"),
        )
        .join(ChatConversation, ChatMessage.conversation_id == ChatConversation.id)
        .where(ChatMessage.created_at >= func.now() - sa_text("interval '30 days'"))
        .group_by(func.date_trunc("day", ChatMessage.created_at))
        .order_by(func.date_trunc("day", ChatMessage.created_at))
    )
    daily_usage = [
        {
            "date": row.date.strftime("%Y-%m-%d"),
            "message_count": row.message_count,
            "active_users": row.active_users,
        }
        for row in daily_usage_q.all()
    ]

    return ChatStatsResponse(
        total_conversations=total_convs,
        total_messages=total_msgs,
        total_model_calls=total_calls,
        active_users=active_users,
        top_models=top_models,
        daily_usage=daily_usage,
    )


# ── Tracking topics ─────────────────────────────────────────────

@router.post("/tracking", response_model=TrackingTopicResponse)
async def create_tracking_topic(
    body: TrackingTopicCreate,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Create a tracking topic with optional LLM keyword extraction."""
    keywords = body.keywords
    tickers = body.related_tickers
    sectors = body.related_sectors

    # Auto-extract keywords/tickers/sectors via LLM
    if body.auto_extract and not (keywords and tickers):
        try:
            result = await call_model_sync(
                "openai/gpt-4o-mini",
                [
                    {"role": "system", "content": (
                        "从用户的投资关注主题中提取关键词、相关股票代码和板块。"
                        "输出严格JSON格式: {\"keywords\":[...],\"tickers\":[...],\"sectors\":[...]}\n"
                        "股票代码格式：A股用6位数字(如600519)，港股用5位数字.HK，美股用英文代码。"
                        "板块用中文，如：新能源、半导体、消费。只输出JSON，不要其他文字。"
                    )},
                    {"role": "user", "content": body.topic},
                ],
            )
            import re
            content = result.get("content", "")
            # Extract JSON from response
            json_match = re.search(r'\{.*\}', content, re.DOTALL)
            if json_match:
                extracted = json.loads(json_match.group())
                if not keywords:
                    keywords = extracted.get("keywords", [])
                if not tickers:
                    tickers = extracted.get("tickers", [])
                if not sectors:
                    sectors = extracted.get("sectors", [])
        except Exception:
            logger.warning("Failed to auto-extract keywords for tracking topic")

    topic = ChatTrackingTopic(
        user_id=user.id,
        topic=body.topic,
        keywords=keywords,
        related_tickers=tickers,
        related_sectors=sectors,
        notify_channels=body.notify_channels,
    )
    db.add(topic)
    await db.commit()
    await db.refresh(topic)

    return TrackingTopicResponse(
        id=str(topic.id), topic=topic.topic, keywords=topic.keywords,
        related_tickers=topic.related_tickers, related_sectors=topic.related_sectors,
        notify_channels=topic.notify_channels, is_active=topic.is_active,
        created_at=topic.created_at, last_checked_at=topic.last_checked_at,
        last_triggered_at=topic.last_triggered_at, unread_count=0,
    )


@router.get("/tracking", response_model=list[TrackingTopicResponse])
async def list_tracking_topics(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """List user's tracking topics with unread alert counts."""
    result = await db.execute(
        select(ChatTrackingTopic)
        .where(ChatTrackingTopic.user_id == user.id)
        .order_by(desc(ChatTrackingTopic.created_at))
    )
    topics = result.scalars().all()

    responses = []
    for t in topics:
        unread_q = await db.scalar(
            select(func.count(ChatTrackingAlert.id))
            .where(ChatTrackingAlert.topic_id == t.id, ChatTrackingAlert.is_read == False)
        )
        responses.append(TrackingTopicResponse(
            id=str(t.id), topic=t.topic, keywords=t.keywords,
            related_tickers=t.related_tickers, related_sectors=t.related_sectors,
            notify_channels=t.notify_channels, is_active=t.is_active,
            created_at=t.created_at, last_checked_at=t.last_checked_at,
            last_triggered_at=t.last_triggered_at, unread_count=unread_q or 0,
        ))
    return responses


@router.patch("/tracking/{topic_id}", response_model=TrackingTopicResponse)
async def update_tracking_topic(
    topic_id: str,
    body: TrackingTopicUpdate,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Update a tracking topic."""
    topic = await db.get(ChatTrackingTopic, topic_id)
    if not topic or str(topic.user_id) != str(user.id):
        raise HTTPException(404, "Tracking topic not found")

    for field, value in body.model_dump(exclude_unset=True).items():
        setattr(topic, field, value)
    await db.commit()
    await db.refresh(topic)

    unread_q = await db.scalar(
        select(func.count(ChatTrackingAlert.id))
        .where(ChatTrackingAlert.topic_id == topic.id, ChatTrackingAlert.is_read == False)
    )
    return TrackingTopicResponse(
        id=str(topic.id), topic=topic.topic, keywords=topic.keywords,
        related_tickers=topic.related_tickers, related_sectors=topic.related_sectors,
        notify_channels=topic.notify_channels, is_active=topic.is_active,
        created_at=topic.created_at, last_checked_at=topic.last_checked_at,
        last_triggered_at=topic.last_triggered_at, unread_count=unread_q or 0,
    )


@router.delete("/tracking/{topic_id}")
async def delete_tracking_topic(
    topic_id: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Delete a tracking topic and its alerts."""
    topic = await db.get(ChatTrackingTopic, topic_id)
    if not topic or str(topic.user_id) != str(user.id):
        raise HTTPException(404, "Tracking topic not found")
    await db.delete(topic)
    await db.commit()
    return {"ok": True}


@router.get("/tracking/{topic_id}/alerts", response_model=list[TrackingAlertResponse])
async def list_tracking_alerts(
    topic_id: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """List alerts for a tracking topic."""
    topic = await db.get(ChatTrackingTopic, topic_id)
    if not topic or str(topic.user_id) != str(user.id):
        raise HTTPException(404, "Tracking topic not found")

    from backend.app.models.news import NewsItem
    result = await db.execute(
        select(ChatTrackingAlert, NewsItem)
        .join(NewsItem, ChatTrackingAlert.news_item_id == NewsItem.id)
        .where(ChatTrackingAlert.topic_id == topic_id)
        .order_by(desc(ChatTrackingAlert.created_at))
        .offset((page - 1) * page_size)
        .limit(page_size)
    )
    rows = result.all()
    return [
        TrackingAlertResponse(
            id=str(alert.id), topic_id=str(alert.topic_id),
            news_title=news.title, news_summary=news.content[:200] if news.content else "",
            match_score=alert.match_score, match_reason=alert.match_reason,
            is_read=alert.is_read, created_at=alert.created_at,
        )
        for alert, news in rows
    ]


@router.post("/tracking/{topic_id}/alerts/read")
async def mark_alerts_read(
    topic_id: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Mark all alerts for a topic as read."""
    topic = await db.get(ChatTrackingTopic, topic_id)
    if not topic or str(topic.user_id) != str(user.id):
        raise HTTPException(404, "Tracking topic not found")
    await db.execute(
        update(ChatTrackingAlert)
        .where(ChatTrackingAlert.topic_id == topic_id, ChatTrackingAlert.is_read == False)
        .values(is_read=True)
    )
    await db.commit()
    return {"ok": True}


# ── Admin: extract research experiences ─────────────────────────

@router.post("/admin/extract-experiences")
async def extract_experiences(
    db: AsyncSession = Depends(get_db),
    admin: User = Depends(get_current_admin),
):
    """Extract research experiences from high-rated conversations.

    Finds conversations with highly-rated responses and extracts
    research patterns/insights as markdown experiences.
    """
    # Find all highly rated responses (4-5 stars)
    result = await db.execute(
        select(ChatModelResponse)
        .where(ChatModelResponse.rating >= 4)
        .options(selectinload(ChatModelResponse.message))
        .order_by(desc(ChatModelResponse.created_at))
        .limit(100)
    )
    responses = result.scalars().all()

    if not responses:
        return {"experiences": [], "message": "暂无高分对话记录"}

    # Build context for experience extraction
    conversations_text = []
    for resp in responses:
        msg = resp.message
        conversations_text.append(
            f"## 用户问题\n{msg.content}\n\n"
            f"## AI回答 ({resp.model_name}, 评分{resp.rating}星)\n{resp.content[:1000]}\n\n---"
        )

    prompt_text = "\n".join(conversations_text[:20])  # Limit to 20 conversations

    result = await call_model_sync(
        "openai/gpt-4o",
        [
            {"role": "system", "content": (
                "你是一位资深股票研究经理。请从以下高质量研究对话中提取研究经验和方法论。"
                "输出为结构化的markdown格式，包含以下部分：\n"
                "1. **研究方法论** - 研究员如何分析股票的方法和思路\n"
                "2. **常用分析框架** - 识别出的分析模式和框架\n"
                "3. **关键洞察** - 值得学习的独特见解\n"
                "4. **常见问题类型** - 研究员最关心的问题\n"
                "5. **最佳实践** - 推荐的研究方法"
            )},
            {"role": "user", "content": prompt_text},
        ],
    )

    return {
        "experiences": result.get("content", "提取失败"),
        "source_count": len(responses),
    }
