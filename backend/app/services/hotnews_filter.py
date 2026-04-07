"""Two-stage hot news filter: first Y/N screening, then competitive Top-10 ranking.

Stage 1 (every 60s): LLM classifies new items as relevant/irrelevant (Y/N).
Stage 2 (every 90s): Takes all Y candidates in 24h window, LLM ranks them,
         keeps only the top 10 most valuable. New items compete with existing top 10.
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
from datetime import datetime, timedelta, timezone

import httpx
from openai import AsyncOpenAI
from sqlalchemy import text

from backend.app.config import Settings
from backend.app.core.database import async_session_factory

logger = logging.getLogger(__name__)

HOT_NEWS_SOURCES = ("华尔街见闻热点", "财联社热点", "雪球热榜", "微博热搜")
BATCH_SIZE = 15
TOP_N = 10  # Maximum items to show in radar

# --- Stage 1: Y/N screening prompt ---
SCREEN_PROMPT = """你是舆情雷达的个股筛选器。你的唯一任务：判断每条新闻是否**明确提及至少一只具体股票/上市公司，且包含该公司的重大事件**。

核心原则：舆情雷达只展示个股级别信息，宏观信息由其他模块处理。

【标记Y的严格条件——必须同时满足以下两点】
1. 新闻中**明确出现了具体公司名或股票名**（如"腾讯"、"比亚迪"、"阿里巴巴"、"华电辽能"、"源杰科技"等）
2. 该公司发生了以下任一重大事件：
   - 财报/业绩发布（季报、年报、业绩预告）
   - 重大并购/重组/借壳/分拆
   - 大额增减持/回购/股权变动
   - IPO/上市首日/退市
   - 涨停/跌停/涨跌幅>5%的异动
   - 重大产品发布/战略合作/订单
   - 高管重大变动（CEO/董事长更换）
   - 重大诉讼/处罚/监管调查
   - 停产/复产/产能重大变化
   - 被龙头企业采购/合作（如"特斯拉从XX采购"）

【板块异动的处理规则】
- 如果新闻列举了≥2只具体股票的异动（如"华电辽能5连板，韶能股份3连板"），标记Y
- 如果只说"XX板块拉升"但未提及任何具体股票名，标记N

【必须标记N——以下类型一律过滤】
- 宏观经济数据、指数行情、大宗商品价格、地缘政治/军事
- 外汇/汇率、政府官员任命、政策/规划（除非点名上市公司）
- 央行/监管层表态、北向/南向资金、分析师观点
- 基金/理财推荐、娱乐/体育/社会/生活、广告/软文
- 罗列大量个股但无实质信息的板块流水账（如"XX涨X%，YY涨X%，ZZ涨X%"仅仅是行情播报）

对每条标题输出Y或N，格式：
1:Y
2:N

不要思考，不要解释，直接输出。"""

# --- Stage 2: Competitive ranking prompt ---
RANK_PROMPT = """你是交易员的首席信息官。从以下候选新闻中精选出**最多{top_n}条**对交易员最有价值的信息。

【评分标准——按优先级排序】
1. **独家/首发重大事件**（10分）：财报超预期/暴雷、重大并购首次披露、突发停产/事故
2. **龙头公司重大动作**（9分）：市值Top50公司的战略级事件、里程碑式产品发布
3. **异常市场信号**（8分）：连板股（≥3连板）、千元股突破、上市首日暴涨/暴跌
4. **供应链/产业链重大变化**（7分）：大额采购订单、关键零部件短缺/停产
5. **普通个股涨跌停**（5分）：单日涨停但无特殊背景

【必须淘汰的低价值内容】
- 板块行情流水账：仅罗列"XX涨N%，YY涨N%"的播报式内容，无独立事件驱动
- 同质化内容：多条新闻说同一件事，只保留信息量最大的一条
- 转债行情播报
- 常规产品发布/降价（除非是战略级产品）

从候选列表中选出最多{top_n}条最有价值的，输出它们的编号和分数，格式：
编号:分数
例如：
3:9
7:8
1:7

按分数从高到低输出。如果候选中没有任何值得推送的内容，输出"无"。
不要思考，不要解释，直接输出。"""


class HotNewsFilter:
    """Background service: two-stage LLM filter (screen + rank) for hot news."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self.llm = AsyncOpenAI(
            api_key=settings.minimax_api_key,
            base_url=settings.minimax_base_url,
            timeout=30.0,
            http_client=httpx.AsyncClient(trust_env=False, timeout=30.0),
        )
        self.model = settings.minimax_model
        self._running = False
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        self._running = True
        self._task = asyncio.create_task(self._loop(), name="hotnews_filter")
        logger.info("[HotNewsFilter] Started (model=%s, batch=%d, top_n=%d)", self.model, BATCH_SIZE, TOP_N)

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("[HotNewsFilter] Stopped")

    async def _loop(self) -> None:
        await asyncio.sleep(10)
        cycle = 0
        while self._running:
            try:
                # Stage 1: screen new items every cycle
                processed = await self._filter_batch()
                if processed > 0:
                    logger.info("[HotNewsFilter] Screened %d items", processed)

                # Stage 2: re-rank after screening, or every 3 cycles (~3 min)
                cycle += 1
                if processed > 0 or cycle % 3 == 0:
                    await self._rank_top_n()
            except Exception:
                logger.exception("[HotNewsFilter] Error in filter cycle")
            for _ in range(60):
                if not self._running:
                    return
                await asyncio.sleep(1)

    # ---- Stage 1: Y/N screening (unchanged logic) ----

    async def _filter_batch(self) -> int:
        """Fetch unevaluated hot news items, classify via LLM, update metadata."""
        async with async_session_factory() as db:
            cutoff = datetime.now(timezone.utc) - timedelta(hours=48)
            result = await db.execute(text("""
                SELECT id, title, source_name
                FROM news_items
                WHERE source_name = ANY(:sources)
                  AND fetched_at >= :cutoff
                  AND (metadata IS NULL
                       OR metadata->>'llm_relevant' IS NULL)
                ORDER BY fetched_at DESC
                LIMIT :limit
            """), {
                "sources": list(HOT_NEWS_SOURCES),
                "cutoff": cutoff,
                "limit": BATCH_SIZE,
            })
            rows = result.fetchall()

        if not rows:
            return 0

        # Pre-LLM hard rules: auto-reject obvious junk
        auto_reject_ids = set()
        seen_titles: set[str] = set()
        for i, row in enumerate(rows):
            item_id, title, source = row[0], (row[1] or "").strip(), row[2]
            if len(title) < 8:
                auto_reject_ids.add(item_id)
                continue
            title_key = re.sub(r"[\s\d%,.，。、：:；;！!？?（）()\-—·]", "", title)
            if title_key in seen_titles:
                auto_reject_ids.add(item_id)
                continue
            seen_titles.add(title_key)

        if auto_reject_ids:
            async with async_session_factory() as db:
                for item_id in auto_reject_ids:
                    await db.execute(text("""
                        UPDATE news_items
                        SET metadata = COALESCE(metadata, '{}'::jsonb) || :patch
                        WHERE id = :id
                    """), {"id": item_id, "patch": json.dumps({"llm_relevant": False})})
                await db.commit()

        llm_rows = [row for row in rows if row[0] not in auto_reject_ids]
        if not llm_rows:
            return len(rows)

        titles_text = "\n".join(f"{i+1}. {row[1]}" for i, row in enumerate(llm_rows))
        user_msg = f"以下是{len(llm_rows)}条新闻标题，请严格判断哪些与具体个股直接相关：\n\n{titles_text}"

        try:
            response = await self.llm.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": SCREEN_PROMPT},
                    {"role": "user", "content": user_msg},
                ],
                max_tokens=2000,
                temperature=0.0,
            )
            content = response.choices[0].message.content or ""
        except Exception as e:
            logger.warning("[HotNewsFilter] LLM screen call failed: %s", e)
            return 0

        relevant_indices = set()
        for line in content.split("\n"):
            line = line.strip()
            m = re.match(r'^(\d+)\s*[:：]\s*([YyNn])', line)
            if m:
                idx = int(m.group(1))
                if m.group(2).upper() == "Y" and 1 <= idx <= len(llm_rows):
                    relevant_indices.add(idx)
                continue
            m = re.match(r'^(\d+)\.\s*.+?→\s*([YyNn])', line)
            if m:
                idx = int(m.group(1))
                if m.group(2).upper() == "Y" and 1 <= idx <= len(llm_rows):
                    relevant_indices.add(idx)

        async with async_session_factory() as db:
            for i, row in enumerate(llm_rows):
                item_id = row[0]
                is_relevant = (i + 1) in relevant_indices
                await db.execute(text("""
                    UPDATE news_items
                    SET metadata = COALESCE(metadata, '{}'::jsonb) || :patch
                    WHERE id = :id
                """), {
                    "id": item_id,
                    "patch": json.dumps({"llm_relevant": is_relevant}),
                })
            await db.commit()

        relevant_count = len(relevant_indices)
        logger.info(
            "[HotNewsFilter] Screen: %d total (%d auto-reject, %d LLM) → %d passed",
            len(rows), len(auto_reject_ids), len(llm_rows), relevant_count,
        )
        return len(rows)

    # ---- Stage 2: Competitive ranking — keep only Top N ----

    async def _rank_top_n(self) -> None:
        """Rank all Y-candidates in 24h window, keep only top N, demote the rest."""
        async with async_session_factory() as db:
            cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
            result = await db.execute(text("""
                SELECT DISTINCT ON (title) id, title, source_name, fetched_at
                FROM news_items
                WHERE source_name = ANY(:sources)
                  AND fetched_at >= :cutoff
                  AND (metadata->>'llm_relevant')::boolean = true
                  AND length(title) >= 8
                ORDER BY title, fetched_at DESC
            """), {"sources": list(HOT_NEWS_SOURCES), "cutoff": cutoff})
            candidates = result.fetchall()

        if not candidates:
            logger.debug("[HotNewsFilter] Rank: no candidates")
            return

        if len(candidates) <= TOP_N:
            # Few enough candidates — all stay as top, assign default score
            async with async_session_factory() as db:
                for row in candidates:
                    await db.execute(text("""
                        UPDATE news_items
                        SET metadata = COALESCE(metadata, '{}'::jsonb) || :patch
                        WHERE id = :id
                    """), {"id": row[0], "patch": json.dumps({"radar_score": 7, "radar_top": True})})
                await db.commit()
            logger.info("[HotNewsFilter] Rank: %d candidates ≤ %d, all kept", len(candidates), TOP_N)
            return

        # Build numbered list for LLM ranking
        titles_text = "\n".join(f"{i+1}. [{row[2]}] {row[1]}" for i, row in enumerate(candidates))
        user_msg = f"以下是{len(candidates)}条已通过初筛的个股新闻，请精选最多{TOP_N}条最有交易价值的：\n\n{titles_text}"

        try:
            response = await self.llm.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": RANK_PROMPT.format(top_n=TOP_N)},
                    {"role": "user", "content": user_msg},
                ],
                max_tokens=4000,
                temperature=0.0,
            )
            content = response.choices[0].message.content or ""
        except Exception as e:
            logger.warning("[HotNewsFilter] LLM rank call failed: %s", e)
            return

        # Strip <think>...</think> tags before parsing
        clean_content = re.sub(r'<think>.*?</think>', '', content, flags=re.DOTALL).strip()
        logger.debug("[HotNewsFilter] Rank raw=%d chars, clean=%d chars", len(content), len(clean_content))

        # Parse "idx:score" lines
        top_items: dict[int, int] = {}  # idx -> score
        for line in clean_content.split("\n"):
            line = line.strip()
            m = re.match(r'^(\d+)\s*[:：]\s*(\d+)', line)
            if m:
                idx, score = int(m.group(1)), int(m.group(2))
                if 1 <= idx <= len(candidates) and 1 <= score <= 10:
                    top_items[idx] = score

        # If LLM returned "无" (nothing worth pushing) and no scores parsed
        if not top_items and "无" in clean_content:
            logger.info("[HotNewsFilter] Rank: LLM says nothing worth pushing")
            async with async_session_factory() as db:
                for row in candidates:
                    await db.execute(text("""
                        UPDATE news_items
                        SET metadata = COALESCE(metadata, '{}'::jsonb) || :patch
                        WHERE id = :id
                    """), {"id": row[0], "patch": json.dumps({"radar_score": 0, "radar_top": False})})
                await db.commit()
            return

        # Keep only top N by score
        sorted_top = sorted(top_items.items(), key=lambda x: x[1], reverse=True)[:TOP_N]
        top_indices = {idx for idx, _ in sorted_top}

        async with async_session_factory() as db:
            for i, row in enumerate(candidates):
                idx = i + 1
                if idx in top_indices:
                    score = top_items[idx]
                    patch = {"radar_score": score, "radar_top": True}
                else:
                    patch = {"radar_score": 0, "radar_top": False}
                await db.execute(text("""
                    UPDATE news_items
                    SET metadata = COALESCE(metadata, '{}'::jsonb) || :patch
                    WHERE id = :id
                """), {"id": row[0], "patch": json.dumps(patch)})
            await db.commit()

        top_titles = [candidates[idx - 1][1] for idx, _ in sorted_top]
        logger.info(
            "[HotNewsFilter] Rank: %d candidates → %d top items: %s",
            len(candidates), len(sorted_top),
            "; ".join(t[:20] for t in top_titles),
        )
