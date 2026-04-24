"""LLM enrichment pipeline for Jiuqian (久谦) data.

Quality gate strategy:
- Forum (expert calls): High-value content, already has summary/insight from source.
  Extract tickers/sectors/sentiment. Display threshold: 0.3 (nearly all pass).
- Minutes (research notes): Has summary. Enrich with tickers/sectors/tags.
  Display threshold: 0.4
- WeChat (public accounts): Highest volume, most noise. Strict two-pass triage.
  Display threshold: 0.6 (much stricter than alphapai's 0.55)

Only processes records where pub_time/meeting_time >= 2 days ago to avoid
processing massive historical backlog on initial sync.
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any

from openai import AsyncOpenAI
from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession

from backend.app.config import Settings
from backend.app.core.database import async_session_factory
from backend.app.services.stock_verifier import get_stock_verifier
from backend.app.models.jiuqian import (
    JiuqianForum,
    JiuqianMinutes,
    JiuqianWechat,
)

logger = logging.getLogger(__name__)


def _extract_json(text: str) -> dict | None:
    """Best-effort JSON extraction from LLM output."""
    text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL).strip()
    text = re.sub(r"<think>.*", "", text, flags=re.DOTALL).strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    m = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
    if m:
        try:
            return json.loads(m.group(1))
        except json.JSONDecodeError:
            pass
    depth = 0
    start = -1
    for i, ch in enumerate(text):
        if ch == '{':
            if depth == 0:
                start = i
            depth += 1
        elif ch == '}':
            depth -= 1
            if depth == 0 and start >= 0:
                try:
                    return json.loads(text[start:i + 1])
                except json.JSONDecodeError:
                    start = -1
    return None


def _html_to_text(html: str, max_len: int = 4000) -> str:
    """Convert HTML to readable text."""
    if not html:
        return ""
    text = re.sub(r"<br\s*/?>", "\n", html, flags=re.IGNORECASE)
    text = re.sub(r"</(?:p|div|h[1-6]|li|tr|ol|ul)>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<(?:h[1-6])[^>]*>", "\n## ", text, flags=re.IGNORECASE)
    text = re.sub(r"<li[^>]*>", "- ", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    text = text.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
    text = text.replace("&quot;", '"').replace("&#39;", "'").replace("&nbsp;", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()[:max_len]


def _normalize_tickers(tickers: list) -> list[str]:
    result = []
    for t in tickers:
        if isinstance(t, str):
            result.append(t)
        elif isinstance(t, dict):
            code = t.get("code", "")
            name = t.get("name", "")
            if code and name:
                result.append(f"{name}({code})")
            elif code:
                result.append(code)
            elif name:
                result.append(name)
    return result


class JiuqianProcessor:
    """Background LLM enrichment service for Jiuqian data."""

    def __init__(self, settings: Settings):
        self.settings = settings
        import httpx
        self.llm = AsyncOpenAI(
            api_key=settings.llm_enrichment_api_key,
            base_url=settings.llm_enrichment_base_url,
            timeout=90.0,
            http_client=httpx.AsyncClient(trust_env=False, timeout=90.0),
        )
        self.model = settings.llm_enrichment_model
        self._running = False
        self._task: asyncio.Task | None = None
        self._sem = asyncio.Semaphore(5)
        self._verifier = get_stock_verifier(
            baidu_api_key=settings.baidu_api_key,
            llm_client=self.llm,
            llm_model=self.model,
        )

    async def start(self) -> None:
        self._running = True
        self._task = asyncio.create_task(self._loop(), name="jiuqian_enrich")
        logger.info("[Jiuqian-Enrich] started (model=%s)", self.model)

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("[Jiuqian-Enrich] stopped")

    async def _loop(self) -> None:
        await asyncio.sleep(45)  # Let sync run first
        while self._running:
            try:
                await self._process_batch()
            except Exception:
                logger.exception("[Jiuqian-Enrich] batch error")

            # Run every 30 minutes
            for _ in range(1800):
                if not self._running:
                    return
                await asyncio.sleep(1)

    async def _llm_call(self, system: str, user: str, max_tokens: int = 800) -> dict | None:
        async with self._sem:
            try:
                resp = await self.llm.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": system},
                        {"role": "user", "content": user},
                    ],
                    max_tokens=max_tokens,
                    temperature=0.1,
                    response_format={"type": "json_object"},
                )
                text = resp.choices[0].message.content or ""
                parsed = _extract_json(text)
                if parsed:
                    return parsed
                logger.warning("[Jiuqian-Enrich] JSON parse failed: %s", text[:300])
            except Exception as exc:
                logger.warning("[Jiuqian-Enrich] LLM call failed: %s", exc)
        return None

    async def _process_batch(self) -> None:
        async with async_session_factory() as db:
            remaining = 60
            remaining = await self._enrich_forum(db, remaining)
            remaining = await self._enrich_minutes(db, remaining)
            remaining = await self._enrich_wechat(db, remaining)

    # ------------------------------------------------------------------ #
    # Forum enrichment — high-value, extract tickers/sectors/sentiment
    # These already have summary and insight from the source.
    # Only process recent items (meeting_time within 2 days).
    # ------------------------------------------------------------------ #
    async def _enrich_forum(self, db: AsyncSession, limit: int) -> int:
        if limit <= 0:
            return 0
        cutoff = datetime.now(timezone.utc) - timedelta(days=3)
        items = (await db.execute(
            select(JiuqianForum)
            .where(JiuqianForum.is_enriched == False)  # noqa: E712
            .where(JiuqianForum.meeting_time >= cutoff)
            .order_by(JiuqianForum.meeting_time.desc())
            .limit(limit)
        )).scalars().all()

        SYS = """你是专业A股/美股交易分析师。分析专家调研纪要，提取对交易有价值的信息。
输出严格JSON格式，不要输出其他文字。
重要规则：
- tickers：提取所有提及的股票，A股用"600519.SH"格式，美股用ticker如"NVDA"，附中文名
- sectors：使用申万一级行业分类
- sentiment：判断整体看法，bullish/bearish/neutral
- relevance_score：0-1，对交易决策的参考价值。专家调研通常>=0.5
- key_points：提取3-5个最核心的交易相关要点"""

        async def process_one(item):
            # Use existing summary + insight + related targets
            context_parts = []
            if item.summary:
                context_parts.append(f"摘要: {item.summary}")
            if item.insight:
                insight_text = _html_to_text(item.insight, 1500)
                context_parts.append(f"核心洞察:\n{insight_text}")
            if not context_parts:
                # Fall back to content
                content_text = _html_to_text(item.content, 2000)
                context_parts.append(f"正文:\n{content_text}")

            enrichment = await self._llm_call(
                system=SYS,
                user=f"""专家调研:
标题: {item.title}
行业: {item.industry or '未知'}
相关标的: {item.related_targets or '未知'}
专家: {item.expert_information or '未知'}
议题: {item.topic or ''}

{chr(10).join(context_parts)}

请分析并输出JSON:
{{"summary": "一句话核心观点(60-120字，包含方向性结论)",
"relevance_score": 0.0-1.0,
"tickers": [{{"code": "600519.SH", "name": "贵州茅台"}}],
"sectors": ["行业"],
"tags": ["主题标签"],
"sentiment": "bullish/bearish/neutral",
"key_points": ["要点1", "要点2", "要点3"]}}""",
                max_tokens=1500,
            )
            if enrichment is None:
                return
            raw_tickers = _normalize_tickers(enrichment.get("tickers", []))
            enrichment["tickers"] = await self._verifier.verify_tickers(raw_tickers)
            item.enrichment = enrichment
            item.is_enriched = True

        tasks = [process_one(item) for item in items]
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for i, r in enumerate(results):
                if isinstance(r, Exception):
                    logger.error("[Jiuqian-Enrich] forum task %d failed: %s", i, r)
            await db.commit()
            logger.info("[Jiuqian-Enrich] forum: %d processed", len(items))
        return limit - len(items)

    # ------------------------------------------------------------------ #
    # Minutes enrichment — moderate value, extract tickers/sectors
    # Only process recent items (pub_time within 2 days).
    # ------------------------------------------------------------------ #
    async def _enrich_minutes(self, db: AsyncSession, limit: int) -> int:
        if limit <= 0:
            return 0
        cutoff = datetime.now(timezone.utc) - timedelta(days=3)
        items = (await db.execute(
            select(JiuqianMinutes)
            .where(JiuqianMinutes.is_enriched == False)  # noqa: E712
            .where(JiuqianMinutes.pub_time >= cutoff)
            .order_by(JiuqianMinutes.pub_time.desc())
            .limit(limit)
        )).scalars().all()

        SYS = """你是专业券商研究纪要分析师。从纪要中提取对A股/美股交易有价值的信息。
输出严格JSON格式。
重要规则：
- tickers使用A股代码格式如"600519.SH"或美股ticker如"NVDA"，附中文名
- sectors使用申万一级行业分类
- sentiment必须给出明确判断
- relevance_score >= 0.5 表示有交易参考价值"""

        async def process_one(item):
            # Use existing summary, fall back to content
            content_text = ""
            if item.summary:
                content_text = item.summary[:1500]
            elif item.content:
                # Content might be a Q&A array stored as string
                raw = item.content
                if raw.startswith("[") and raw.endswith("]"):
                    try:
                        qa_list = json.loads(raw)
                        content_text = "\n\n".join(str(q)[:500] for q in qa_list[:5])
                    except json.JSONDecodeError:
                        content_text = raw[:2000]
                else:
                    content_text = raw[:2000]

            companies = ", ".join(item.company) if isinstance(item.company, list) else ""

            enrichment = await self._llm_call(
                system=SYS,
                user=f"""研究纪要:
标题: {item.title}
来源: {item.source or '未知'}
机构: {companies or '未知'}
作者: {item.author or '未知'}

内容:
{content_text if content_text else '(无内容)'}

请分析并输出JSON:
{{"summary": "核心要点一句话(60-120字)",
"relevance_score": 0.0-1.0,
"tickers": [{{"code": "600519.SH", "name": "贵州茅台"}}],
"sectors": ["行业"],
"tags": ["主题1", "主题2"],
"sentiment": "bullish/bearish/neutral"}}""",
                max_tokens=1200,
            )
            if enrichment is None:
                return
            raw_tickers = _normalize_tickers(enrichment.get("tickers", []))
            enrichment["tickers"] = await self._verifier.verify_tickers(raw_tickers)
            item.enrichment = enrichment
            item.is_enriched = True

        tasks = [process_one(item) for item in items]
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for i, r in enumerate(results):
                if isinstance(r, Exception):
                    logger.error("[Jiuqian-Enrich] minutes task %d failed: %s", i, r)
            await db.commit()
            logger.info("[Jiuqian-Enrich] minutes: %d processed", len(items))
        return limit - len(items)

    # ------------------------------------------------------------------ #
    # WeChat enrichment — strictest filter, two-pass triage
    # Display threshold: 0.6 (much higher than alphapai)
    # Only process recent items (pub_time within 2 days).
    # ------------------------------------------------------------------ #
    async def _enrich_wechat(self, db: AsyncSession, limit: int) -> int:
        if limit <= 0:
            return 0
        cutoff = datetime.now(timezone.utc) - timedelta(days=3)
        items = (await db.execute(
            select(JiuqianWechat)
            .where(JiuqianWechat.is_enriched == False)  # noqa: E712
            .where(JiuqianWechat.pub_time >= cutoff)
            .order_by(JiuqianWechat.pub_time.desc())
            .limit(limit)
        )).scalars().all()

        TRIAGE_SYS = """你是金融信息严格筛选系统。对微信公众号文章进行价值评估。
你的任务是从海量文章中筛选出真正对股票交易员有参考价值的内容。
评分标准非常严格——只有高质量的投资研究、行业深度分析、重大政策解读、关键数据点才能获得高分。

高分(>=0.6)：深度行业研究、重大公司公告解读、核心数据分析、政策影响解读、专家观点
中分(0.3-0.6)：一般行业新闻、常规市场评论、浅层分析
低分(<0.3)：广告、营销、通知、招聘、非金融内容、生活类、娱乐类、重复新闻、信息量极低

输出严格JSON格式。"""

        ENRICH_SYS = """你是资深A股/美股研究员。分析文章并提取交易相关信息。
输出严格JSON格式。
- tickers使用A股/美股代码格式附带中文名
- sectors使用申万一级行业分类
- summary要包含核心观点和方向性结论"""

        async def process_one(art):
            # Pass 1: Strict triage
            triage = await self._llm_call(
                system=TRIAGE_SYS,
                user=f"""标题: {art.title}
来源: {art.source or '未知'}
地区: {art.district or '未知'}
摘要: {(art.summary or '')[:200]}

输出JSON: {{"relevance_score": 0.0到1.0, "skip": true/false, "reason": "简短理由"}}
skip=true用于明确无交易价值的内容""",
                max_tokens=400,
            )
            if triage is None:
                return

            score = triage.get("relevance_score", 0)
            if triage.get("skip", False) or score < 0.35:
                art.enrichment = {
                    "relevance_score": score,
                    "skipped": True,
                    "summary": "",
                    "reason": triage.get("reason", ""),
                }
                art.is_enriched = True
                return

            # Pass 2: Full enrichment
            content_text = ""
            if art.summary:
                content_text = art.summary[:1500]
            elif art.content:
                content_text = art.content[:2000]

            enrichment = await self._llm_call(
                system=ENRICH_SYS,
                user=f"""文章标题: {art.title}
来源: {art.source or '未知'}
内容摘要: {content_text if content_text else '(无)'}

提取JSON:
{{"summary": "核心观点摘要(60-100字)",
"relevance_score": 0.0-1.0,
"tickers": [{{"code": "600519.SH", "name": "贵州茅台"}}],
"sectors": ["行业"],
"tags": ["主题标签"],
"sentiment": "bullish/bearish/neutral"}}""",
                max_tokens=1000,
            )
            if enrichment is None:
                return
            raw_tickers = _normalize_tickers(enrichment.get("tickers", []))
            enrichment["tickers"] = await self._verifier.verify_tickers(raw_tickers)
            art.enrichment = enrichment
            art.is_enriched = True

        tasks = [process_one(art) for art in items]
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for i, r in enumerate(results):
                if isinstance(r, Exception):
                    logger.error("[Jiuqian-Enrich] wechat task %d failed: %s", i, r)
            await db.commit()
            logger.info("[Jiuqian-Enrich] wechat: %d processed", len(items))
        return limit - len(items)
