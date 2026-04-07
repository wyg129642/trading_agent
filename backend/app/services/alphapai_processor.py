"""LLM enrichment pipeline for AlphaPai data using MiniMax M2 (or compatible).

Changes from v1:
- Better prompts with A-share stock name mapping
- Auto-digest generation on every cycle
- Improved HTML content extraction preserving structure
- Higher max_tokens for better responses
- Concurrent LLM calls for throughput
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Any

from openai import AsyncOpenAI
from sqlalchemy import select, func, Date, cast
from sqlalchemy.ext.asyncio import AsyncSession

from backend.app.config import Settings
from backend.app.core.database import async_session_factory
from backend.app.services.stock_verifier import get_stock_verifier
from backend.app.models.alphapai import (
    AlphaPaiArticle,
    AlphaPaiComment,
    AlphaPaiDigest,
    AlphaPaiRoadshowCN,
    AlphaPaiRoadshowUS,
)
from backend.app.services.alphapai_client import AlphaPaiClient

logger = logging.getLogger(__name__)

_EMPTY_ENRICHMENT: dict = {
    "summary": "",
    "relevance_score": 0.0,
    "tags": [],
    "tickers": [],
    "sectors": [],
    "sentiment": "",
}

# Sentiment display labels
SENTIMENT_LABELS = {
    "bullish": "看多",
    "bearish": "看空",
    "neutral": "中性",
}


def _extract_json(text: str) -> dict | None:
    """Best-effort JSON extraction from LLM output (handles <think> tags)."""
    # Remove closed <think> blocks
    text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL).strip()
    # Remove unclosed <think> block (model hit max_tokens mid-thinking)
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
    """Convert HTML to readable text preserving structure (headers, lists)."""
    if not html:
        return ""
    # Replace common block elements with newlines
    text = re.sub(r"<br\s*/?>", "\n", html, flags=re.IGNORECASE)
    text = re.sub(r"</(?:p|div|h[1-6]|li|tr)>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<(?:h[1-6])[^>]*>", "\n## ", text, flags=re.IGNORECASE)
    text = re.sub(r"<li[^>]*>", "- ", text, flags=re.IGNORECASE)
    # Remove all remaining HTML tags
    text = re.sub(r"<[^>]+>", "", text)
    # Decode HTML entities
    text = text.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
    text = text.replace("&quot;", '"').replace("&#39;", "'").replace("&nbsp;", " ")
    # Collapse whitespace
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()[:max_len]


class AlphaPaiProcessor:
    """Background LLM enrichment service for AlphaPai data."""

    def __init__(self, settings: Settings):
        self.settings = settings
        # MiniMax is a Chinese API — bypass proxy by using a no-proxy httpx client
        import httpx
        self.llm = AsyncOpenAI(
            api_key=settings.minimax_api_key,
            base_url=settings.minimax_base_url,
            timeout=90.0,
            http_client=httpx.AsyncClient(trust_env=False, timeout=90.0),
        )
        self.model = settings.minimax_model
        self._running = False
        self._task: asyncio.Task | None = None
        self._client: AlphaPaiClient | None = None
        self._sem = asyncio.Semaphore(5)  # limit concurrent LLM calls
        self._verifier = get_stock_verifier(
            baidu_api_key=settings.baidu_api_key,
            llm_client=self.llm,
            llm_model=self.model,
        )

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #
    async def start(self, client: AlphaPaiClient) -> None:
        self._client = client
        self._running = True
        self._task = asyncio.create_task(self._loop(), name="alphapai_enrich")
        logger.info("[AlphaPai-Enrich] started (model=%s)", self.model)

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("[AlphaPai-Enrich] stopped")

    async def _loop(self) -> None:
        await asyncio.sleep(30)  # Let sync run first
        while self._running:
            try:
                await self._process_batch()
            except Exception:
                logger.exception("[AlphaPai-Enrich] batch error")

            # Generate digest if needed (check every cycle)
            try:
                await self._maybe_generate_digest()
            except Exception:
                logger.exception("[AlphaPai-Enrich] digest error")

            # Run every 90 seconds
            for _ in range(90):
                if not self._running:
                    return
                await asyncio.sleep(1)

    # ------------------------------------------------------------------ #
    # Batch processing (priority: comments > roadshows > articles)
    # ------------------------------------------------------------------ #
    async def _process_batch(self) -> None:
        async with async_session_factory() as db:
            remaining = 80  # max items per cycle
            remaining = await self._enrich_comments(db, remaining)
            remaining = await self._enrich_roadshows_cn(db, remaining)
            remaining = await self._enrich_roadshows_us(db, remaining)
            remaining = await self._enrich_articles(db, remaining)

    # ------------------------------------------------------------------ #
    # LLM call helper with semaphore
    # ------------------------------------------------------------------ #
    async def _llm_call(self, system: str, user: str, max_tokens: int = 800) -> dict | None:
        """Call LLM and extract JSON. Returns enrichment dict or None on failure."""
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
                logger.warning(
                    "[AlphaPai-Enrich] JSON parse failed, raw (first 300): %s",
                    text[:300],
                )
            except Exception as exc:
                logger.warning("[AlphaPai-Enrich] LLM call failed: %s", exc)
        return None

    # ------------------------------------------------------------------ #
    # Comment enrichment
    # ------------------------------------------------------------------ #
    async def _enrich_comments(self, db: AsyncSession, limit: int) -> int:
        if limit <= 0:
            return 0
        items = (await db.execute(
            select(AlphaPaiComment)
            .where(AlphaPaiComment.is_enriched == False)  # noqa: E712
            .order_by(AlphaPaiComment.cmnt_date.desc())
            .limit(limit)
        )).scalars().all()

        SYS = """你是专业A股分析师助手。你需要分析券商点评，判断其对股票交易的价值。
输出严格JSON格式，不要输出其他文字。
重要规则：
- tickers必须使用A股代码格式如"600519.SH"或"000001.SZ"，同时附上股票中文名
- sectors使用申万一级行业分类
- sentiment只能是"bullish"、"bearish"或"neutral"
- relevance_score: 0-1，衡量对交易决策的价值。>=0.5表示有明确的投资参考价值"""

        async def process_one(cmt):
            content_text = (cmt.content or "")[:2000]
            enrichment = await self._llm_call(
                system=SYS,
                user=f"""券商点评:
标题: {cmt.title}
分析师: {cmt.psn_name or '未知'} ({cmt.team_cname or ''})
机构: {cmt.inst_cname or '未知'}
内容: {content_text}

请分析并输出JSON:
{{"summary": "一句话核心观点(50-100字，必须包含结论方向)",
"relevance_score": 0.0到1.0,
"tickers": [{{"code": "600519.SH", "name": "贵州茅台"}}],
"sectors": ["行业1"],
"tags": ["关键主题"],
"sentiment": "bullish/bearish/neutral"}}""",
            )
            if enrichment is None:
                return  # Skip — will retry next cycle
            raw_tickers = _normalize_tickers(enrichment.get("tickers", []))
            enrichment["tickers"] = await self._verifier.verify_tickers(raw_tickers)
            cmt.enrichment = enrichment
            cmt.is_enriched = True

        tasks = [process_one(cmt) for cmt in items]
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for i, r in enumerate(results):
                if isinstance(r, Exception):
                    logger.error("[AlphaPai-Enrich] comment task %d failed: %s", i, r)
            await db.commit()
            logger.info("[AlphaPai-Enrich] comments: %d", len(items))
        return limit - len(items)

    # ------------------------------------------------------------------ #
    # A-share roadshow enrichment
    # ------------------------------------------------------------------ #
    async def _enrich_roadshows_cn(self, db: AsyncSession, limit: int) -> int:
        if limit <= 0:
            return 0
        items = (await db.execute(
            select(AlphaPaiRoadshowCN)
            .where(AlphaPaiRoadshowCN.is_enriched == False)  # noqa: E712
            .order_by(AlphaPaiRoadshowCN.stime.desc())
            .limit(limit)
        )).scalars().all()

        SYS = """你是卖方路演纪要分析专家。从路演纪要中提取对A股交易有价值的信息。
输出严格JSON格式。
重要规则：
- tickers使用A股代码格式如"600519.SH"，附上中文名
- sectors使用申万一级行业分类
- 如果纪要涉及的股票/行业不明确，根据公司名和行业推断
- sentiment必须给出明确判断
- relevance_score >= 0.5 表示有交易参考价值
- key_points提取3-5个核心要点"""

        async def process_one(rs):
            # Download content if not cached
            if not rs.content_cached and rs.content_path and self._client:
                try:
                    rs.content_cached = await self._client.download_content(rs.content_path)
                except Exception as exc:
                    logger.debug("Download roadshow content failed: %s", exc)

            content_text = ""
            if rs.content_cached:
                content_text = _html_to_text(rs.content_cached, 2000)

            industries = ", ".join(
                i.get("name", "") for i in (rs.ind_json or []) if isinstance(i, dict)
            )

            enrichment = await self._llm_call(
                system=SYS,
                user=f"""路演纪要:
标题: {rs.show_title}
公司/券商: {rs.company or '未知'}
嘉宾: {rs.guest or '未知'}
行业: {industries or '未知'}
类型: {rs.trans_source} ({'AI摘要' if rs.trans_source == 'AI' else '完整速记'})

内容:
{content_text if content_text else '(内容未下载，请根据标题和行业分析)'}

请分析并输出JSON:
{{"summary": "核心要点一句话(60-120字，包含结论)",
"relevance_score": 0.0-1.0,
"tickers": [{{"code": "600519.SH", "name": "贵州茅台"}}],
"sectors": ["行业"],
"tags": ["主题1", "主题2"],
"sentiment": "bullish/bearish/neutral",
"key_points": ["要点1", "要点2", "要点3"]}}""",
                max_tokens=2500,
            )
            if enrichment is None:
                return  # Skip — will retry next cycle
            enrichment["tickers"] = _normalize_tickers(enrichment.get("tickers", []))
            rs.enrichment = enrichment
            rs.is_enriched = True

        tasks = [process_one(rs) for rs in items]
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for i, r in enumerate(results):
                if isinstance(r, Exception):
                    logger.error("[AlphaPai-Enrich] roadshow_cn task %d failed: %s", i, r)
            await db.commit()
            logger.info("[AlphaPai-Enrich] roadshows_cn: %d", len(items))
        return limit - len(items)

    # ------------------------------------------------------------------ #
    # US roadshow enrichment
    # ------------------------------------------------------------------ #
    async def _enrich_roadshows_us(self, db: AsyncSession, limit: int) -> int:
        if limit <= 0:
            return 0
        items = (await db.execute(
            select(AlphaPaiRoadshowUS)
            .where(AlphaPaiRoadshowUS.is_enriched == False)  # noqa: E712
            .order_by(AlphaPaiRoadshowUS.stime.desc())
            .limit(limit)
        )).scalars().all()

        for rs in items:
            aux = rs.ai_auxiliary_json or {}

            if aux.get("full_text_summary"):
                topic_bullets = aux.get("topic_bullets_v2") or aux.get("topic_bullets") or []
                key_points = []
                for topic in topic_bullets:
                    if isinstance(topic, dict):
                        title = topic.get("title", "")
                        bullets = topic.get("bullets", [])
                        for b in bullets[:2]:
                            text = b.get("text", "") if isinstance(b, dict) else str(b)
                            key_points.append(f"{title}: {text[:100]}")

                rs.enrichment = {
                    "summary": aux["full_text_summary"][:200],
                    "relevance_score": 0.6,
                    "tickers": [],
                    "sectors": [],
                    "tags": [t.get("title", "") for t in topic_bullets if isinstance(t, dict)][:5],
                    "sentiment": "neutral",
                    "key_points": key_points[:5],
                    "source": "ai_auxiliary",
                }
                rs.is_enriched = True
                continue

            enrichment = await self._llm_call(
                system="You are a US equity research assistant. Extract key info from earnings calls. Output JSON only.",
                user=f"""Roadshow:
Title: {rs.show_title}
Company: {rs.company or 'unknown'}
Quarter: {rs.quarter_year or 'N/A'}

Extract JSON:
{{"summary": "One-sentence summary (max 100 chars)",
"relevance_score": 0.0-1.0,
"tickers": [{{"code": "AAPL", "name": "Apple"}}],
"sectors": ["relevant sectors"],
"tags": ["key topics"],
"sentiment": "bullish/bearish/neutral"}}""",
                max_tokens=600,
            )
            if enrichment is None:
                continue  # Skip — will retry next cycle
            raw_tickers = _normalize_tickers(enrichment.get("tickers", []))
            enrichment["tickers"] = await self._verifier.verify_tickers(raw_tickers)
            rs.enrichment = enrichment
            rs.is_enriched = True

        if items:
            await db.commit()
            logger.info("[AlphaPai-Enrich] roadshows_us: %d", len(items))
        return limit - len(items)

    # ------------------------------------------------------------------ #
    # WeChat article enrichment (two-pass: triage then enrich)
    # ------------------------------------------------------------------ #
    async def _enrich_articles(self, db: AsyncSession, limit: int) -> int:
        if limit <= 0:
            return 0
        items = (await db.execute(
            select(AlphaPaiArticle)
            .where(AlphaPaiArticle.is_enriched == False)  # noqa: E712
            .order_by(AlphaPaiArticle.publish_time.desc())
            .limit(limit)
        )).scalars().all()

        TRIAGE_SYS = """你是金融内容分级系统。根据标题和作者判断微信公众号文章对A股交易员的价值。
输出严格JSON。
高价值文章特征：涉及具体行业/个股分析、政策解读、市场策略、盘前/盘后复盘、资金流向。
低价值文章特征：广告、营销软文、招聘、通知公告、个人生活、非股票类金融(保险/理财/区块链)。
注意：标题含有股票名、行业名、政策、宏观经济的文章通常有价值，不要轻易跳过。"""

        ENRICH_SYS = """你是资深A股研究员。分析微信公众号文章并提取交易相关信息。
输出严格JSON格式。
重要规则：
- tickers使用A股代码格式附带中文名
- sectors使用申万一级行业分类
- sentiment必须是bullish/bearish/neutral
- summary要包含核心观点和方向性结论
- market_impact_score是0-10的整数，衡量文章对市场/交易决策的实际影响力：
  9-10: 重大政策变动、行业颠覆性事件、核心个股重大利好/利空
  7-8: 明确的行业趋势分析、个股深度研究、重要数据解读
  5-6: 一般性行业/个股分析、常规政策解读
  3-4: 偏泛泛的市场评论、信息整合类文章
  1-2: 价值较低的内容、缺乏独立观点"""

        async def process_one(art):
            # Pass 1: Triage
            triage = await self._llm_call(
                system=TRIAGE_SYS,
                user=f"""标题: {art.arc_name}
作者: {art.author or '未知'}
字数: {art.text_count}
是否原创: {'是' if art.is_original else '否'}

输出JSON: {{"relevance_score": 0.0到1.0, "skip": true/false, "reason": "简短理由"}}
skip=true仅用于明确无价值的内容(营销/非金融)""",
                max_tokens=500,
            )
            if triage is None:
                return  # LLM failed — will retry next cycle

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

            # Pass 2: Download content + full enrichment
            if not art.content_cached and art.content_html_path and self._client:
                try:
                    art.content_cached = await self._client.download_content(
                        art.content_html_path
                    )
                except Exception:
                    pass

            content_text = _html_to_text(art.content_cached, 2000) if art.content_cached else ""

            enrichment = await self._llm_call(
                system=ENRICH_SYS,
                user=f"""文章标题: {art.arc_name}
作者: {art.author or '未知'}
正文: {content_text if content_text else '(无正文，请根据标题分析)'}

提取JSON:
{{"summary": "核心观点摘要(60-100字，包含方向性结论)",
"relevance_score": 0.0-1.0,
"market_impact_score": 0到10的整数,
"tickers": [{{"code": "600519.SH", "name": "贵州茅台"}}],
"sectors": ["行业"],
"tags": ["主题标签"],
"sentiment": "bullish/bearish/neutral"}}""",
                max_tokens=1200,
            )
            if enrichment is None:
                return  # LLM failed — will retry next cycle
            enrichment["tickers"] = _normalize_tickers(enrichment.get("tickers", []))
            art.enrichment = enrichment
            art.is_enriched = True

        tasks = [process_one(art) for art in items]
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for i, r in enumerate(results):
                if isinstance(r, Exception):
                    logger.error("[AlphaPai-Enrich] article task %d failed: %s", i, r)
            await db.commit()
            logger.info("[AlphaPai-Enrich] articles: %d", len(items))
        return limit - len(items)

    # ------------------------------------------------------------------ #
    # Digest: auto-generate when new enriched data is available
    # ------------------------------------------------------------------ #
    async def _maybe_generate_digest(self) -> None:
        """Generate digest if none exists for today or if enough time has passed."""
        async with async_session_factory() as db:
            now = datetime.now(timezone.utc)
            # Use CST (UTC+8) for the date since this is an A-share system
            cst_now = now + timedelta(hours=8)
            today = cst_now.date()

            # Check if already generated today
            existing = await db.scalar(
                select(AlphaPaiDigest).where(
                    cast(AlphaPaiDigest.digest_date, Date) == today
                )
            )

            # Generate once per day, or regenerate if the existing one is more than 4 hours old
            if existing:
                age = now - (existing.generated_at or now)
                if age < timedelta(hours=4):
                    return
                # Update existing digest
                await self._generate_digest_content(db, existing, today)
            else:
                # Create new digest
                digest = AlphaPaiDigest(digest_date=today)
                db.add(digest)
                await self._generate_digest_content(db, digest, today)

    async def _generate_digest_content(
        self, db: AsyncSession, digest: AlphaPaiDigest, today
    ) -> None:
        """Generate the actual digest content from enriched data."""
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(hours=24)

        # Gather enriched items from past 24h with decent relevance
        # Exclude neutral sentiment items — only include bullish/bearish
        from sqlalchemy import or_

        # Comments (high relevance, non-neutral)
        cmt_rows = (await db.execute(
            select(AlphaPaiComment)
            .where(AlphaPaiComment.cmnt_date >= cutoff)
            .where(AlphaPaiComment.is_enriched == True)  # noqa: E712
            .where(AlphaPaiComment.enrichment["relevance_score"].as_float() >= 0.5)
            .where(AlphaPaiComment.enrichment["sentiment"].as_string().in_(["bullish", "bearish"]))
            .order_by(AlphaPaiComment.enrichment["relevance_score"].as_float().desc())
            .limit(30)
        )).scalars().all()

        # Roadshows CN (AI only, non-neutral)
        rs_rows = (await db.execute(
            select(AlphaPaiRoadshowCN)
            .where(AlphaPaiRoadshowCN.stime >= cutoff)
            .where(AlphaPaiRoadshowCN.is_enriched == True)  # noqa: E712
            .where(AlphaPaiRoadshowCN.trans_source == "AI")
            .where(AlphaPaiRoadshowCN.enrichment["sentiment"].as_string().in_(["bullish", "bearish"]))
            .order_by(AlphaPaiRoadshowCN.stime.desc())
            .limit(20)
        )).scalars().all()

        # Articles (non-skipped, decent relevance, non-neutral)
        art_rows = (await db.execute(
            select(AlphaPaiArticle)
            .where(AlphaPaiArticle.publish_time >= cutoff)
            .where(AlphaPaiArticle.is_enriched == True)  # noqa: E712
            .where(or_(
                AlphaPaiArticle.enrichment["skipped"].as_boolean().is_(False),
                AlphaPaiArticle.enrichment["skipped"].is_(None),
            ))
            .where(AlphaPaiArticle.enrichment["relevance_score"].as_float() >= 0.4)
            .where(AlphaPaiArticle.enrichment["sentiment"].as_string().in_(["bullish", "bearish"]))
            .order_by(AlphaPaiArticle.enrichment["relevance_score"].as_float().desc())
            .limit(15)
        )).scalars().all()

        # Collect all tickers and sectors for hot-topic analysis
        ticker_counter: Counter = Counter()
        sector_counter: Counter = Counter()
        bullish_items: list[str] = []
        bearish_items: list[str] = []
        summaries: list[str] = []

        for c in cmt_rows:
            enr = c.enrichment or {}
            for t in enr.get("tickers", []):
                name = t if isinstance(t, str) else t.get("name", t.get("code", ""))
                if name:
                    ticker_counter[name] += 1
            for s in enr.get("sectors", []):
                sector_counter[s] += 1
            sent = enr.get("sentiment", "")
            summary_line = f"[点评|{c.inst_cname or ''}|{c.psn_name or ''}] {enr.get('summary', c.title)}"
            if sent == "bullish":
                bullish_items.append(summary_line)
            elif sent == "bearish":
                bearish_items.append(summary_line)
            summaries.append(summary_line)

        for r in rs_rows:
            enr = r.enrichment or {}
            for t in enr.get("tickers", []):
                name = t if isinstance(t, str) else t.get("name", t.get("code", ""))
                if name:
                    ticker_counter[name] += 1
            for s in enr.get("sectors", []):
                sector_counter[s] += 1
            industries = ", ".join(
                i.get("name", "") for i in (r.ind_json or []) if isinstance(i, dict)
            )
            summaries.append(
                f"[纪要|{r.company or ''}|{industries}] {enr.get('summary', r.show_title)}"
            )

        for a in art_rows:
            enr = a.enrichment or {}
            for t in enr.get("tickers", []):
                name = t if isinstance(t, str) else t.get("name", t.get("code", ""))
                if name:
                    ticker_counter[name] += 1
            for s in enr.get("sectors", []):
                sector_counter[s] += 1
            summaries.append(
                f"[文章|{a.author or ''}] {a.arc_name}: {enr.get('summary', '')}"
            )

        if not summaries:
            logger.info("[AlphaPai-Digest] No enriched data for digest")
            return

        # Count stats
        art_count = await db.scalar(
            select(func.count()).select_from(AlphaPaiArticle)
            .where(AlphaPaiArticle.synced_at >= cutoff)
        ) or 0
        rs_cn_count = await db.scalar(
            select(func.count()).select_from(AlphaPaiRoadshowCN)
            .where(AlphaPaiRoadshowCN.synced_at >= cutoff)
        ) or 0
        cmt_count = await db.scalar(
            select(func.count()).select_from(AlphaPaiComment)
            .where(AlphaPaiComment.synced_at >= cutoff)
        ) or 0

        # Build hot stocks/sectors summary for the prompt
        hot_tickers = ticker_counter.most_common(10)
        hot_sectors = sector_counter.most_common(8)
        hot_info = ""
        if hot_tickers:
            hot_info += f"被提及最多的股票: {', '.join(f'{t}({c}次)' for t, c in hot_tickers)}\n"
        if hot_sectors:
            hot_info += f"被提及最多的行业: {', '.join(f'{s}({c}次)' for s, c in hot_sectors)}\n"
        if bullish_items:
            hot_info += f"看多观点({len(bullish_items)}条):\n" + "\n".join(bullish_items[:8]) + "\n"
        if bearish_items:
            hot_info += f"看空观点({len(bearish_items)}条):\n" + "\n".join(bearish_items[:8]) + "\n"

        digest_prompt = f"""过去24小时的金融信息汇总 (共{art_count}篇公众号文章, {rs_cn_count}条路演纪要, {cmt_count}条券商点评):

{hot_info}

精选信息摘要:
{chr(10).join(summaries[:35])}

请生成一份交易员晨会简报(Markdown格式)，要求：

# 今日市场速览
(3-5句话总结今日关键趋势和热点，必须提及具体股票或板块名)

## 热门关注
(列出今天最被关注的3-5只股票/板块，说明原因，标注看多/看空/中性)

## 看多信号
(列出有明确看多逻辑的个股/行业，附信息来源)

## 看空/风险提示
(列出需要注意的风险点、看空信号)

## 重要纪要摘要
(精选3-5条最有价值的路演纪要核心观点)

## 值得深读
(推荐3-5篇值得仔细阅读的公众号文章/点评)

要求精炼有用，帮助交易员快速掌握市场脉搏。直接输出Markdown，不要额外解释。"""

        try:
            resp = await self.llm.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "你是一位资深A股研究主管，负责每日晨会简报。简报面向专业交易员，需要精准、简洁、有洞察力。"},
                    {"role": "user", "content": digest_prompt},
                ],
                max_tokens=2500,
                temperature=0.3,
            )
            content = resp.choices[0].message.content or ""
            # Strip thinking tags if any
            content = re.sub(r"<think>.*?</think>", "", content, flags=re.DOTALL).strip()
        except Exception as exc:
            logger.error("[AlphaPai-Digest] LLM call failed: %s", exc)
            content = "简报生成失败，请稍后重试"

        digest.content_markdown = content
        digest.stats = {
            "articles": art_count,
            "roadshows_cn": rs_cn_count,
            "comments": cmt_count,
            "hot_tickers": [{"name": t, "count": c} for t, c in hot_tickers],
            "hot_sectors": [{"name": s, "count": c} for s, c in hot_sectors],
            "bullish_count": len(bullish_items),
            "bearish_count": len(bearish_items),
        }
        digest.generated_at = datetime.now(timezone.utc)
        digest.model_used = self.model

        await db.commit()
        logger.info(
            "[AlphaPai-Digest] Generated for %s (%d chars, %d tickers, %d sectors)",
            today, len(content), len(hot_tickers), len(hot_sectors),
        )

    # ------------------------------------------------------------------ #
    # Public: manual trigger
    # ------------------------------------------------------------------ #
    async def generate_digest(self) -> None:
        """Force digest generation (called from admin API)."""
        async with async_session_factory() as db:
            now = datetime.now(timezone.utc)
            cst_now = now + timedelta(hours=8)
            today = cst_now.date()
            existing = await db.scalar(
                select(AlphaPaiDigest).where(
                    cast(AlphaPaiDigest.digest_date, Date) == today
                )
            )
            if existing:
                await self._generate_digest_content(db, existing, today)
            else:
                digest = AlphaPaiDigest(digest_date=today)
                db.add(digest)
                await self._generate_digest_content(db, digest, today)


def _normalize_tickers(tickers: list) -> list[str]:
    """Normalize tickers to flat string list: ['600519.SH', '贵州茅台'] etc."""
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
