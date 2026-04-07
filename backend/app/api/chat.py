"""Chat API: conversations, multi-model messages, ratings, templates, file upload."""
from __future__ import annotations

import asyncio
import json
import logging
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Query, UploadFile, File
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
    RateRequest, RateResponse,
    TemplateCreate, TemplateUpdate, TemplateResponse,
    FileUploadResponse, ExportResponse,
    ModelInfo, ModelRanking, ModelRankingResponse,
    ChatStatsResponse,
    DebateRequest,
    TrackingTopicCreate, TrackingTopicUpdate, TrackingTopicResponse,
    TrackingAlertResponse,
)
from backend.app.services.chat_llm import (
    AVAILABLE_MODELS, MODEL_MAP, MODE_CONFIGS,
    build_multimodal_content, call_model_stream, call_model_sync,
    generate_title, _build_messages, search_for_chat,
)

logger = logging.getLogger(__name__)
router = APIRouter()

UPLOAD_DIR = Path(__file__).resolve().parent.parent.parent.parent / "data" / "chat_uploads"
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)


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
            created_at=resp.created_at,
        ))

    # Auto-generate title for first message
    if conv.title == "新对话":
        conv.title = await generate_title(body.content)

    conv.updated_at = datetime.now(timezone.utc)
    await db.commit()

    return SendMessageResponse(message_id=str(msg.id), model_responses=response_data)


# ── Send message (SSE streaming) ───────────────────────────────

@router.post("/conversations/{conv_id}/messages/stream")
async def send_message_stream(
    conv_id: str,
    body: SendMessageRequest,
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

    # Web search (if enabled)
    search_context = None
    if body.web_search:
        search_context = await search_for_chat(body.content)

    messages_payload = _build_messages(history, user_content, body.system_prompt, search_context)
    mode = body.mode if body.mode in MODE_CONFIGS else "standard"

    # Auto-generate title
    is_first = conv.title == "新对话"
    if is_first:
        conv.title = await generate_title(body.content)
    conv.updated_at = datetime.now(timezone.utc)
    await db.commit()

    # SSE event stream from all models concurrently
    async def event_stream():
        queue: asyncio.Queue = asyncio.Queue()
        model_ids = body.models

        async def stream_one_model(model_id: str):
            async for chunk in call_model_stream(model_id, messages_payload, mode=mode):
                await queue.put({"model": model_id, **chunk})

        tasks = [asyncio.create_task(stream_one_model(m)) for m in model_ids]

        # Send message_id + search status
        meta = {'type': 'meta', 'message_id': str(msg_id)}
        if search_context:
            meta['web_search'] = True
        yield f"data: {json.dumps(meta)}\n\n"

        done_count = 0
        try:
            while done_count < len(model_ids):
                try:
                    item = await asyncio.wait_for(queue.get(), timeout=130.0)
                except asyncio.TimeoutError:
                    break

                model_id = item.get("model", "")
                model_info = MODEL_MAP.get(model_id, {"name": model_id})

                if item.get("done"):
                    done_count += 1
                    # Save to DB
                    async with (await _get_session()) as save_db:
                        resp = ChatModelResponse(
                            message_id=msg_id,
                            model_id=model_id,
                            model_name=model_info.get("name", model_id),
                            content=item.get("content", ""),
                            tokens_used=item.get("tokens"),
                            latency_ms=item.get("latency_ms"),
                            error=item.get("error"),
                        )
                        save_db.add(resp)
                        await save_db.commit()
                        await save_db.refresh(resp)

                    yield f"data: {json.dumps({'type': 'done', 'model': model_id, 'model_name': model_info.get('name', model_id), 'response_id': str(resp.id), 'tokens': item.get('tokens', 0), 'latency_ms': item.get('latency_ms', 0), 'error': item.get('error')}, ensure_ascii=False)}\n\n"
                else:
                    yield f"data: {json.dumps({'type': 'delta', 'model': model_id, 'delta': item.get('delta', '')}, ensure_ascii=False)}\n\n"

        except Exception as e:
            logger.exception("SSE stream error")
            yield f"data: {json.dumps({'type': 'error', 'error': str(e)[:200]})}\n\n"
        finally:
            for t in tasks:
                t.cancel()

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


# ── Debate mode ─────────────────────────────────────────────────

DEBATE_PROMPTS = {
    1: "你是一位资深投资分析师。请对以下问题给出你的深度分析和明确立场（看多或看空），包含具体论据、数据支撑和投资逻辑。",
    2: "你是一位风险评估专家和反方辩手。以下是另一位分析师的观点：\n\n{prev_content}\n\n请仔细审视其中的逻辑漏洞、被忽视的风险和过度乐观/悲观的假设，给出你的反驳和替代观点。",
    3: "你是一位独立的首席投资官。以下是看多方和质疑方的观点：\n\n【看多方】\n{round1_content}\n\n【质疑方】\n{round2_content}\n\n综合以上两方观点，给出你的独立判断、建议配置策略和关键风险指标。",
}

DEBATE_ROLES = {1: "看多方", 2: "质疑方", 3: "综合判断"}


@router.post("/conversations/{conv_id}/messages/debate")
async def send_debate_message(
    conv_id: str,
    body: DebateRequest,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Multi-model sequential debate: models argue in rounds."""
    # Validate conversation
    conv = await db.get(ChatConversation, conv_id)
    if not conv or str(conv.user_id) != str(user.id):
        raise HTTPException(404, "Conversation not found")

    # Build multimodal content
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

    # Auto-generate title
    if conv.title == "新对话":
        conv.title = await generate_title(body.content)
    conv.updated_at = datetime.now(timezone.utc)
    await db.commit()

    num_rounds = len(body.debate_models)
    round_contents: dict[int, str] = {}

    async def event_stream():
        yield f"data: {json.dumps({'type': 'meta', 'message_id': str(msg_id)})}\n\n"

        for round_num in range(1, num_rounds + 1):
            model_id = body.debate_models[round_num - 1]
            model_info = MODEL_MAP.get(model_id, {"name": model_id})
            role = DEBATE_ROLES[round_num]

            # Build round-specific system prompt
            if round_num == 1:
                sys_prompt = DEBATE_PROMPTS[1]
            elif round_num == 2:
                sys_prompt = DEBATE_PROMPTS[2].format(prev_content=round_contents.get(1, ""))
            else:
                sys_prompt = DEBATE_PROMPTS[3].format(
                    round1_content=round_contents.get(1, ""),
                    round2_content=round_contents.get(2, ""),
                )

            # Merge with user's custom system prompt if provided
            if body.system_prompt:
                sys_prompt = body.system_prompt + "\n\n" + sys_prompt

            messages_payload = _build_messages([], user_content, sys_prompt)

            # Emit round start
            yield f"data: {json.dumps({'type': 'round_start', 'round': round_num, 'role': role, 'model': model_id, 'model_name': model_info.get('name', model_id)})}\n\n"

            # Stream this round
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

            round_contents[round_num] = full_content

            # Save response to DB
            try:
                session = await _get_session()
                async with session.begin():
                    resp = ChatModelResponse(
                        message_id=msg_id,
                        model_id=model_id,
                        model_name=model_info.get("name", model_id),
                        content=full_content,
                        tokens_used=tokens_used,
                        latency_ms=latency_ms,
                        error=error_text,
                        debate_round=round_num,
                    )
                    session.add(resp)
                    await session.flush()
                    resp_id = str(resp.id)
                await session.close()
            except Exception:
                logger.exception("Failed to save debate round %d", round_num)
                resp_id = ""

            # Emit round done
            yield f"data: {json.dumps({'type': 'done', 'model': model_id, 'model_name': model_info.get('name', model_id), 'debate_round': round_num, 'response_id': resp_id, 'tokens': tokens_used, 'latency_ms': latency_ms, 'error': error_text})}\n\n"

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


# ── Rating ──────────────────────────────────────────────────────

@router.post("/rate/{response_id}", response_model=RateResponse)
async def rate_response(
    response_id: str,
    body: RateRequest,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
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
    resp.rating_comment = body.comment
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
