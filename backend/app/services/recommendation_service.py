"""Personalized quick-start question recommendations for the AI chat.

Reads a user's recent conversation titles, first user messages, and watchlist
tickers, then asks an LLM to produce 4 follow-up research questions tailored to
what that user actually works on. Results are cached for 24h in
``chat_recommended_questions`` and refreshed daily by ``RecommendationScheduler``.
"""
from __future__ import annotations

import hashlib
import json
import logging
import uuid
from datetime import datetime, timezone, timedelta

from sqlalchemy import select, desc
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert as pg_insert

from backend.app.models.chat import (
    ChatConversation, ChatMessage, ChatRecommendedQuestion,
)
from backend.app.models.watchlist import Watchlist, WatchlistItem

logger = logging.getLogger(__name__)

# Shown when the user has no history yet or LLM generation fails.
DEFAULT_QUESTIONS: list[str] = [
    "帮我分析贵州茅台(600519)的基本面，包括最近的财报表现",
    "当前A股市场的宏观环境如何？有哪些板块值得关注？",
    "比较宁德时代和比亚迪在新能源领域的竞争优势",
    "近期半导体板块大跌，分析一下原因和后续走势",
]

# Questions older than this are considered stale and regenerated on-demand.
CACHE_TTL_HOURS = 24

# LLM used for recommendation generation — cheap + fast.
RECOMMENDER_MODEL = "openai/gpt-4o-mini"

# How much history to consider per user.
RECENT_CONV_LIMIT = 15
RECENT_MESSAGE_CHARS_PER_CONV = 400
WATCHLIST_TICKER_LIMIT = 20


async def _collect_user_context(db: AsyncSession, user_id: uuid.UUID) -> dict:
    """Gather the minimal signals we'll feed the LLM.

    Returns a dict with: conversation_titles, recent_questions, watchlist_tickers.
    """
    convs = (
        await db.execute(
            select(ChatConversation)
            .where(ChatConversation.user_id == user_id)
            .order_by(desc(ChatConversation.updated_at))
            .limit(RECENT_CONV_LIMIT)
        )
    ).scalars().all()

    conv_titles: list[str] = []
    recent_questions: list[str] = []
    for c in convs:
        if c.title and c.title != "新对话":
            conv_titles.append(c.title)
        first_user_msg = (
            await db.execute(
                select(ChatMessage)
                .where(ChatMessage.conversation_id == c.id)
                .where(ChatMessage.role == "user")
                .order_by(ChatMessage.created_at)
                .limit(1)
            )
        ).scalar_one_or_none()
        if first_user_msg and first_user_msg.content:
            recent_questions.append(first_user_msg.content[:RECENT_MESSAGE_CHARS_PER_CONV])

    items = (
        await db.execute(
            select(WatchlistItem)
            .join(Watchlist, WatchlistItem.watchlist_id == Watchlist.id)
            .where(Watchlist.user_id == user_id)
            .where(WatchlistItem.item_type == "stock")
            .order_by(desc(WatchlistItem.added_at))
            .limit(WATCHLIST_TICKER_LIMIT)
        )
    ).scalars().all()
    tickers = [
        f"{it.value}{f' ({it.display_name})' if it.display_name else ''}"
        for it in items
    ]

    return {
        "conversation_titles": conv_titles[:10],
        "recent_questions": recent_questions[:10],
        "watchlist_tickers": tickers,
    }


def _build_prompt(context: dict) -> list[dict]:
    """Build the messages for the recommender LLM."""
    today = datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d")
    titles = "\n".join(f"- {t}" for t in context["conversation_titles"]) or "（暂无）"
    questions = "\n".join(f"- {q}" for q in context["recent_questions"]) or "（暂无）"
    tickers = ", ".join(context["watchlist_tickers"]) or "（暂无）"

    system = (
        "你是一位证券研究助手，正在为一位中国A股/港美股投资研究员推荐下一步可以问的问题。"
        "你将拿到该研究员最近的聊天主题、提过的问题、以及自选股列表，请据此生成 4 条"
        "个性化的研究问题，帮助他继续深入研究。"
    )
    user = f"""今天是 {today}。

## 用户最近的聊天主题
{titles}

## 用户最近问过的问题（节选）
{questions}

## 用户的自选股
{tickers}

## 输出要求
请生成 4 条面向该用户的研究问题，必须符合：
1. **贴近用户兴趣**：围绕他已经关注的公司、板块、议题展开；如果用户完全没有历史，可以用宽泛的当日热点。
2. **具体可回答**：每条问题都要有明确的研究对象（公司名/代码/板块/事件），不要问空泛的"市场走势如何"。
3. **不同角度**：4 条之间覆盖不同方向，例如基本面 / 催化剂 / 行业对比 / 风险点 / 近期新闻。
4. **中文输出**，每条 20~50 字，像自然提问，避免"你能否…"的套话。
5. **不要重复**用户最近已经问过的问题；要推进到下一步。

请严格按以下 JSON 返回，不要加任何解释或 markdown：
{{"questions": ["问题1", "问题2", "问题3", "问题4"]}}"""

    return [
        {"role": "system", "content": system},
        {"role": "user", "content": user},
    ]


def _parse_questions(content: str) -> list[str] | None:
    """Parse the LLM's JSON response. Returns None if unparseable."""
    if not content:
        return None
    text = content.strip()
    # Strip markdown code fences if present.
    if text.startswith("```"):
        lines = [ln for ln in text.splitlines() if not ln.strip().startswith("```")]
        text = "\n".join(lines).strip()
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}")
        if start == -1 or end == -1 or end <= start:
            return None
        try:
            data = json.loads(text[start : end + 1])
        except json.JSONDecodeError:
            return None
    qs = data.get("questions") if isinstance(data, dict) else None
    if not isinstance(qs, list):
        return None
    cleaned = [str(q).strip() for q in qs if str(q).strip()]
    return cleaned[:4] if cleaned else None


def _digest(context: dict) -> str:
    """Stable hash of the context — lets us skip regen when inputs are unchanged."""
    blob = json.dumps(context, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:32]


async def generate_for_user(
    db: AsyncSession, user_id: uuid.UUID, *, force: bool = False
) -> list[str]:
    """Generate (and persist) personalized questions for one user.

    Returns the 4 questions we end up showing — either freshly generated, the
    cached row (if digest unchanged and not forced), or DEFAULT_QUESTIONS on
    failure / when the user has no signals at all.
    """
    # Import lazily to avoid circular import at module load (chat_llm imports
    # from services that indirectly touch this module's area).
    from backend.app.services.chat_llm import call_model_sync

    context = await _collect_user_context(db, user_id)
    digest = _digest(context)
    now = datetime.now(timezone.utc)

    existing = (
        await db.execute(
            select(ChatRecommendedQuestion).where(
                ChatRecommendedQuestion.user_id == user_id
            )
        )
    ).scalar_one_or_none()

    if (
        existing
        and not force
        and existing.source_digest == digest
        and existing.generated_at
        and (now - existing.generated_at) < timedelta(hours=CACHE_TTL_HOURS)
        and existing.questions
    ):
        return list(existing.questions)

    # If the user has no signals at all, persist defaults so we don't hammer the LLM.
    has_signals = bool(
        context["conversation_titles"]
        or context["recent_questions"]
        or context["watchlist_tickers"]
    )
    if not has_signals:
        await _upsert(db, user_id, DEFAULT_QUESTIONS, digest)
        return list(DEFAULT_QUESTIONS)

    try:
        messages = _build_prompt(context)
        result = await call_model_sync(
            RECOMMENDER_MODEL, messages, mode="standard", max_tokens=800,
        )
        if result.get("error"):
            logger.warning(
                "recommendation: LLM error for user=%s: %s",
                user_id, result.get("error"),
            )
        questions = _parse_questions(result.get("content", ""))
        if not questions and result.get("content"):
            logger.warning(
                "recommendation: failed to parse LLM response for user=%s: %r",
                user_id, str(result.get("content"))[:300],
            )
    except Exception:
        logger.exception("recommendation: LLM call failed for user=%s", user_id)
        questions = None

    if not questions:
        # Keep whatever the user already had if regeneration failed — only fall back
        # to defaults if we have nothing.
        if existing and existing.questions:
            return list(existing.questions)
        await _upsert(db, user_id, DEFAULT_QUESTIONS, digest)
        return list(DEFAULT_QUESTIONS)

    # Pad with defaults if the LLM returned fewer than 4.
    while len(questions) < 4:
        for fallback in DEFAULT_QUESTIONS:
            if fallback not in questions:
                questions.append(fallback)
                break
        if len(questions) >= 4:
            break
    questions = questions[:4]

    await _upsert(db, user_id, questions, digest)
    logger.info("recommendation: generated %d questions for user=%s", len(questions), user_id)
    return questions


async def _upsert(
    db: AsyncSession, user_id: uuid.UUID, questions: list[str], digest: str
) -> None:
    stmt = pg_insert(ChatRecommendedQuestion).values(
        user_id=user_id,
        questions=questions,
        source_digest=digest,
        generated_at=datetime.now(timezone.utc),
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=["user_id"],
        set_={
            "questions": stmt.excluded.questions,
            "source_digest": stmt.excluded.source_digest,
            "generated_at": stmt.excluded.generated_at,
        },
    )
    await db.execute(stmt)
    await db.commit()


async def get_for_user(db: AsyncSession, user_id: uuid.UUID) -> list[str]:
    """Read-only fetch: returns cached questions if fresh, else regenerates.

    Used by the API endpoint — the scheduler is the primary producer, this is
    the on-demand fallback when a user hits the chat page before the scheduler
    has run (e.g. first login).
    """
    existing = (
        await db.execute(
            select(ChatRecommendedQuestion).where(
                ChatRecommendedQuestion.user_id == user_id
            )
        )
    ).scalar_one_or_none()

    if (
        existing
        and existing.questions
        and existing.generated_at
        and (datetime.now(timezone.utc) - existing.generated_at)
        < timedelta(hours=CACHE_TTL_HOURS)
    ):
        return list(existing.questions)

    return await generate_for_user(db, user_id)
