"""Feishu (飞书) webhook alerting for trading signals.

Redesigned card format:
- News Title, Content Summary, Time, Link
- Citations from 3 categories (news coverage, stock info, historical impact)
- Stock Price Info with links
- Sentiment (bullish/bearish), Surprise degree, Timeliness degree
- Test mode: comprehensive debug messages with full LLM traces
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone

import httpx

from src.analysis.signal_scorer import SENTIMENT_WEIGHTS
from src.models import (
    AnalysisResult,
    DeepResearchResult,
    FinalAssessment,
    InitialEvaluation,
    NewsItem,
    ResearchReport,
    SearchVerification,
)

logger = logging.getLogger(__name__)

# Sentiment to emoji mapping
SENTIMENT_ICON = {
    "very_bullish": "🟢🟢",
    "bullish": "🟢",
    "neutral": "⚪",
    "bearish": "🔴",
    "very_bearish": "🔴🔴",
}

SENTIMENT_LABEL = {
    "very_bullish": "强烈看多",
    "bullish": "看多",
    "neutral": "中性",
    "bearish": "看空",
    "very_bearish": "强烈看空",
}

IMPACT_ICON = {
    "critical": "🔥🔥🔥",
    "high": "🔥🔥",
    "medium": "🔥",
    "low": "💧",
}

TIMELINESS_ICON = {
    "timely": "🟢",
    "medium": "🟡",
    "low": "🔴",
}

TIMELINESS_LABEL = {
    "timely": "时效 — 新闻新鲜，股价尚未反应",
    "medium": "一般 — 新闻新鲜，但股价已有反应",
    "low": "过时 — 旧闻，市场已充分消化",
}

# Stock price lookup URL templates
STOCK_PRICE_URLS = {
    # A-shares (6 digits)
    "a_share": "https://quote.eastmoney.com/{market}{ticker}.html",
    # US stocks
    "us": "https://finance.yahoo.com/quote/{ticker}",
    # HK stocks
    "hk": "https://finance.yahoo.com/quote/{ticker}",
}


def _stock_price_url(ticker: str) -> str:
    """Generate a stock price lookup URL for a given ticker."""
    if not ticker:
        return ""
    # A-share: 6 digits
    if len(ticker) == 6 and ticker.isdigit():
        # Shanghai: starts with 6, Shenzhen: starts with 0/3
        market = "sh" if ticker.startswith("6") else "sz"
        return f"https://quote.eastmoney.com/{market}{ticker}.html"
    # HK: digits.HK
    if ticker.upper().endswith(".HK"):
        return f"https://finance.yahoo.com/quote/{ticker.replace('.HK', '.HK')}"
    # US: letters
    if ticker.isalpha():
        return f"https://finance.yahoo.com/quote/{ticker}"
    return ""


class FeishuAlerter:
    """Send trading alerts via Feishu webhook bot."""

    def __init__(
        self,
        webhook_url: str,
        alert_levels: list[str] | None = None,
        max_alerts_per_ticker_per_hour: int = 3,
        dedup_window_minutes: int = 60,
    ):
        self.webhook_url = webhook_url
        self.alert_levels = set(alert_levels or ["critical", "high", "medium"])
        self._client = httpx.AsyncClient(timeout=15)
        self._max_per_ticker_hour = max_alerts_per_ticker_per_hour
        self._dedup_window = timedelta(minutes=dedup_window_minutes)
        self._alert_history: list[dict] = []
        self._pending_digest: list[dict] = []

    async def close(self):
        await self._client.aclose()

    def _should_suppress(self, tickers: list[str], category: str) -> str | None:
        """Check if this alert should be suppressed."""
        now = datetime.now()
        cutoff = now - self._dedup_window
        self._alert_history = [h for h in self._alert_history if h["time"] > cutoff]

        for h in self._alert_history:
            if h.get("category") == category and set(tickers) & set(h.get("tickers", [])):
                return f"dedup:{','.join(tickers)}/{category}"

        for ticker in tickers:
            count = sum(1 for h in self._alert_history if ticker in h.get("tickers", []))
            if count >= self._max_per_ticker_hour:
                return f"rate_limit:{ticker}({count})"

        return None

    def _record_alert(self, tickers: list[str], category: str) -> None:
        self._alert_history.append({
            "tickers": tickers,
            "category": category,
            "time": datetime.now(),
        })

    async def send_alert(
        self,
        news: NewsItem,
        analysis: AnalysisResult,
        research: ResearchReport | None = None,
        alert_level: str = "medium",
        signal_score: float = 0.0,
        search_verification: SearchVerification | None = None,
        signal_score_obj=None,
        # New pipeline data
        initial_evaluation: InitialEvaluation | None = None,
        deep_research: DeepResearchResult | None = None,
        final_assessment: FinalAssessment | None = None,
    ) -> bool:
        """Send a formatted alert to Feishu."""
        if alert_level not in self.alert_levels:
            return False

        tickers = analysis.affected_tickers or []
        category = analysis.category or "other"

        suppress_reason = self._should_suppress(tickers, category)
        if suppress_reason:
            logger.info("[Feishu] Suppressed (%s): %s", suppress_reason, news.title[:50])
            self._pending_digest.append({
                "title": news.title,
                "sentiment": analysis.sentiment,
                "tickers": tickers,
                "score": signal_score,
                "category": category,
            })
            return False

        try:
            payload = self._build_card(
                news=news,
                analysis=analysis,
                research=research,
                alert_level=alert_level,
                signal_score=signal_score,
                search_verification=search_verification,
                signal_score_obj=signal_score_obj,
                initial_evaluation=initial_evaluation,
                deep_research=deep_research,
                final_assessment=final_assessment,
            )

            resp = await self._client.post(
                self.webhook_url,
                json=payload,
                headers={"Content-Type": "application/json"},
            )

            if resp.status_code == 200:
                data = resp.json()
                if data.get("code") == 0 or data.get("StatusCode") == 0:
                    logger.info("[Feishu] Alert sent (impact=%s): %s", analysis.impact_magnitude, news.title[:50])
                    self._record_alert(tickers, category)
                    return True
                else:
                    logger.warning("[Feishu] API error: %s", data)
                    return False
            else:
                logger.warning("[Feishu] HTTP %d: %s", resp.status_code, resp.text)
                return False
        except Exception as e:
            logger.error("[Feishu] Failed to send alert: %s", e)
            return False

    async def send_digest(self) -> bool:
        """Send accumulated suppressed items as a single digest message."""
        if not self._pending_digest:
            return False

        items = sorted(self._pending_digest, key=lambda x: x.get("score", 0), reverse=True)[:20]
        self._pending_digest.clear()

        lines = [f"📋 行业动态汇总 ({len(items)} 条) | {datetime.now().strftime('%H:%M')}"]
        for item in items:
            icon = SENTIMENT_ICON.get(item.get("sentiment", ""), "⚪")
            tickers = ", ".join(item.get("tickers", [])[:3]) or "N/A"
            lines.append(f"  {icon} [{tickers}] {item['title'][:60]}")

        payload = {"msg_type": "text", "content": {"text": "\n".join(lines)}}
        try:
            resp = await self._client.post(
                self.webhook_url, json=payload,
                headers={"Content-Type": "application/json"},
            )
            return resp.status_code == 200
        except Exception as e:
            logger.error("[Feishu] Digest send failed: %s", e)
            return False

    def _format_timeliness(self, news: NewsItem) -> str:
        """Format timeliness info: publish time + relative time."""
        if not news.published_at:
            return "发布时间: 未知"

        pub = news.published_at
        now = datetime.now(timezone.utc)
        if pub.tzinfo is None:
            pub = pub.replace(tzinfo=timezone.utc)

        delta = now - pub
        total_seconds = int(delta.total_seconds())
        if total_seconds < 60:
            relative = "刚刚"
        elif total_seconds < 3600:
            relative = f"{total_seconds // 60}分钟前"
        elif total_seconds < 86400:
            relative = f"{total_seconds // 3600}小时前"
        else:
            relative = f"{total_seconds // 86400}天前"

        pub_str = pub.strftime("%Y-%m-%d %H:%M")
        if news.market == "us":
            try:
                eastern = pub - timedelta(hours=5)
                pub_str = f"{pub_str} UTC ({eastern.strftime('%H:%M')} ET)"
            except Exception:
                pub_str = f"{pub_str} UTC"
        elif news.market == "china":
            try:
                beijing = pub + timedelta(hours=8)
                pub_str = f"{beijing.strftime('%Y-%m-%d %H:%M')} 北京时间"
            except Exception:
                pass

        return f"{pub_str} ({relative})"

    def _build_card(
        self,
        news: NewsItem,
        analysis: AnalysisResult,
        research: ResearchReport | None,
        alert_level: str,
        signal_score: float = 0.0,
        search_verification: SearchVerification | None = None,
        signal_score_obj=None,
        initial_evaluation: InitialEvaluation | None = None,
        deep_research: DeepResearchResult | None = None,
        final_assessment: FinalAssessment | None = None,
    ) -> dict:
        """Build an interactive card message for Feishu.

        Layout:
        1. News Title + Link
        2. Content Summary + Time
        3. Sentiment / Surprise / Impact / Timeliness metrics
        4. Related Stocks (with price links)
        5. Related Sectors
        6. Citations — News Coverage
        7. Citations — Stock Information
        8. Citations — Historical Impact
        9. Stock Price Data
        10. Bull/Bear cases
        11. Recommended Action
        12. Footer links + timestamp
        """
        sentiment = analysis.sentiment
        sentiment_icon = SENTIMENT_ICON.get(sentiment, "⚪")
        sentiment_label = SENTIMENT_LABEL.get(sentiment, sentiment)
        impact_icon = IMPACT_ICON.get(analysis.impact_magnitude, "")

        # Use FinalAssessment data if available
        timeliness = "timely"
        surprise = analysis.surprise_factor
        if final_assessment:
            timeliness = final_assessment.timeliness
            surprise = final_assessment.surprise_factor
        elif signal_score_obj and hasattr(signal_score_obj, 'timeliness'):
            timeliness = signal_score_obj.timeliness
        timeliness_icon = TIMELINESS_ICON.get(timeliness, "⚪")
        timeliness_label = TIMELINESS_LABEL.get(timeliness, timeliness)

        header_color = "red" if alert_level == "critical" else ("orange" if alert_level == "high" else "blue")
        if alert_level == "critical":
            header_title = f"⚠️ 重大交易信号 | {sentiment_label}"
        elif alert_level == "high":
            header_title = f"📊 重要交易信号 | {sentiment_label}"
        else:
            header_title = f"📰 交易信号 | {sentiment_label}"

        elements = []

        # 1. News Title + Link (prominent)
        title_text = f"**{news.title}**"
        if news.url:
            title_text = f"**[{news.title}]({news.url})**"
        elements.append({"tag": "div", "text": {"content": title_text, "tag": "lark_md"}})

        # 2. Summary + Time
        timeliness_str = self._format_timeliness(news)
        summary = analysis.summary or (final_assessment.summary if final_assessment else "")
        elements.append({
            "tag": "div",
            "text": {
                "content": (
                    f"**来源**: {news.source_name}\n"
                    f"**时间**: {timeliness_str}\n"
                    f"**摘要**: {summary[:500]}"
                ),
                "tag": "lark_md",
            },
        })

        elements.append({"tag": "hr"})

        # 3. Core Metrics
        surprise_bar = "█" * int(surprise * 10) + "░" * (10 - int(surprise * 10))
        elements.append({
            "tag": "div",
            "text": {
                "content": (
                    f"**情绪**: {sentiment_icon} {sentiment_label}\n"
                    f"**意外度**: {surprise:.1f}/1.0 [{surprise_bar}]\n"
                    f"**影响量级**: {impact_icon} {analysis.impact_magnitude} | {analysis.impact_timeframe}\n"
                    f"**时效性**: {timeliness_icon} {timeliness_label}"
                ),
                "tag": "lark_md",
            },
        })

        elements.append({"tag": "hr"})

        # 4. Related Stocks (with price links)
        stocks_info = initial_evaluation or None
        if stocks_info and stocks_info.related_stocks:
            stock_lines = ["**🏢 相关标的**"]
            for s in stocks_info.related_stocks[:8]:
                name = s.get("name", "")
                ticker = s.get("ticker", "")
                price_url = _stock_price_url(ticker)
                if price_url:
                    stock_lines.append(f"- {name} ({ticker}) — [查看行情]({price_url})")
                else:
                    stock_lines.append(f"- {name} ({ticker})")
            elements.append({
                "tag": "div",
                "text": {"content": "\n".join(stock_lines), "tag": "lark_md"},
            })
        elif analysis.affected_tickers:
            tickers_str = ", ".join(analysis.affected_tickers)
            elements.append({
                "tag": "div",
                "text": {"content": f"**🏢 相关标的**: {tickers_str}", "tag": "lark_md"},
            })

        # 5. Related Sectors
        sectors = (stocks_info.related_sectors if stocks_info else None) or analysis.affected_sectors
        if sectors:
            elements.append({
                "tag": "div",
                "text": {"content": f"**📁 相关板块**: {', '.join(sectors)}", "tag": "lark_md"},
            })

        elements.append({"tag": "hr"})

        # 6-8. Citations by category (from deep research)
        if deep_research and deep_research.citations:
            self._add_citation_section(elements, "📰 新闻报道引用", deep_research.citations.get("news_coverage", []))
            self._add_citation_section(elements, "📈 个股信息引用", deep_research.citations.get("stock_info", []))
            self._add_citation_section(elements, "📜 历史影响引用", deep_research.citations.get("historical_impact", []))
            # Supplementary citations
            supp = deep_research.citations.get("supplementary", [])
            if supp:
                self._add_citation_section(elements, "🔍 补充研究", supp)
        elif search_verification and search_verification.related_news:
            # Fallback to legacy search verification
            related_lines = ["**🔍 相关搜索结果**"]
            for rn in search_verification.related_news[:5]:
                title = rn.get("title", "")[:60]
                url = rn.get("url", "")
                date = rn.get("date", "")
                if url:
                    related_lines.append(f"- [{title}]({url}) {f'({date})' if date else ''}")
                else:
                    related_lines.append(f"- {title} {f'({date})' if date else ''}")
            elements.append({
                "tag": "div",
                "text": {"content": "\n".join(related_lines), "tag": "lark_md"},
            })

        # 9. Stock Price Data
        price_data = (deep_research.price_data if deep_research else None) or \
                     (search_verification.price_data if search_verification else None)
        if price_data:
            elements.append({"tag": "hr"})
            price_lines = ["**💰 股价信息**"]
            for ticker, data in list(price_data.items())[:3]:
                data_str = data if isinstance(data, str) else str(data)
                price_url = _stock_price_url(ticker)
                # Extract period change if available
                if "Period Change:" in data_str:
                    change_line = [l for l in data_str.split("\n") if "Period Change:" in l]
                    if change_line:
                        change = change_line[0].strip()
                        if price_url:
                            price_lines.append(f"- **{ticker}**: {change} — [查看详情]({price_url})")
                        else:
                            price_lines.append(f"- **{ticker}**: {change}")
                    else:
                        price_lines.append(f"- **{ticker}**: 数据已获取")
                else:
                    price_lines.append(f"- **{ticker}**: {data_str[:100]}")
            elements.append({
                "tag": "div",
                "text": {"content": "\n".join(price_lines), "tag": "lark_md"},
            })

        elements.append({"tag": "hr"})

        # 10. Bull/Bear cases
        bull_case = (final_assessment.bull_case if final_assessment else None) or analysis.bull_case
        bear_case = (final_assessment.bear_case if final_assessment else None) or analysis.bear_case
        if bull_case or bear_case:
            elements.append({
                "tag": "div",
                "text": {
                    "content": (
                        f"**📈 多头逻辑**: {bull_case[:300]}\n"
                        f"**📉 空头逻辑**: {bear_case[:300]}"
                    ),
                    "tag": "lark_md",
                },
            })

        # 11. Market Expectation + Recommended Action
        mkt_exp = (final_assessment.market_expectation if final_assessment else None) or analysis.market_expectation
        rec_action = final_assessment.recommended_action if final_assessment else ""
        if mkt_exp or rec_action:
            parts = []
            if mkt_exp:
                parts.append(f"**市场预期**: {mkt_exp[:300]}")
            if rec_action:
                parts.append(f"**💡 交易建议**: {rec_action[:300]}")
            elements.append({
                "tag": "div",
                "text": {"content": "\n".join(parts), "tag": "lark_md"},
            })

        # 12. Confidence + Research depth
        confidence = final_assessment.confidence if final_assessment else 0.0
        research_depth = ""
        if deep_research:
            research_depth = (
                f"深度研究: {deep_research.total_iterations}轮迭代, "
                f"{len(deep_research.all_search_results)}条搜索结果, "
                f"{len(deep_research.all_fetched_pages)}个网页获取"
            )

        elements.append({"tag": "hr"})

        footer_parts = [f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"]
        if confidence > 0:
            footer_parts.append(f"信心: {confidence:.1f}/1.0")
        if research_depth:
            footer_parts.append(research_depth)
        if news.url:
            footer_parts.append(f"[原文]({news.url})")

        elements.append({
            "tag": "div",
            "text": {"content": " | ".join(footer_parts), "tag": "lark_md"},
        })

        return {
            "msg_type": "interactive",
            "card": {
                "config": {"wide_screen_mode": True},
                "header": {
                    "title": {"content": header_title, "tag": "plain_text"},
                    "template": header_color,
                },
                "elements": elements,
            },
        }

    def _add_citation_section(
        self,
        elements: list[dict],
        title: str,
        citations: list[dict],
    ) -> None:
        """Add a citation section to the card elements."""
        if not citations:
            return

        lines = [f"**{title}**"]
        seen_urls: set[str] = set()
        count = 0
        for c in citations:
            if count >= 4:
                break
            url = c.get("url", "")
            if url in seen_urls:
                continue
            seen_urls.add(url)

            c_title = c.get("title", "N/A")[:50]
            date = c.get("date", "")
            website = c.get("website", "")
            source = c.get("source", "")

            if url:
                line = f"- [{c_title}]({url})"
            else:
                line = f"- {c_title}"
            meta_parts = []
            if website:
                meta_parts.append(website)
            if date:
                meta_parts.append(date[:10])
            if source:
                meta_parts.append(f"via {source}")
            if meta_parts:
                line += f" ({', '.join(meta_parts)})"
            lines.append(line)
            count += 1

        if count > 0:
            elements.append({
                "tag": "div",
                "text": {"content": "\n".join(lines), "tag": "lark_md"},
            })

    # --- System alerts ---

    async def send_system_alert(self, message: str) -> bool:
        """Send a system-level alert."""
        payload = {
            "msg_type": "text",
            "content": {"text": f"⚙️ 系统告警 | {datetime.now().strftime('%H:%M:%S')}\n{message}"},
        }
        try:
            resp = await self._client.post(
                self.webhook_url, json=payload,
                headers={"Content-Type": "application/json"},
            )
            if resp.status_code == 200:
                logger.info("[Feishu] System alert sent: %s", message[:80])
            else:
                logger.warning("[Feishu] System alert HTTP %d: %s", resp.status_code, resp.text[:200])
            return resp.status_code == 200
        except Exception as e:
            logger.error("[Feishu] System alert failed: %s", e)
            return False

    # --- Test mode debug messages ---

    async def send_test_debug(self, news: NewsItem, pipeline_result: dict) -> bool:
        """Send comprehensive debug information for test mode.

        Sends multiple messages to Feishu with full LLM interaction details
        and the final card output.
        """
        messages = self._build_debug_messages(news, pipeline_result)
        success = True

        for i, msg in enumerate(messages):
            if isinstance(msg, dict) and msg.get("msg_type") == "interactive":
                # Send card message directly
                payload = msg
            else:
                payload = {"msg_type": "text", "content": {"text": str(msg)}}

            try:
                resp = await self._client.post(
                    self.webhook_url, json=payload,
                    headers={"Content-Type": "application/json"},
                )
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get("code") != 0 and data.get("StatusCode") != 0:
                        logger.warning("[Feishu] Debug msg %d/%d API error: %s", i + 1, len(messages), data)
                        success = False
                else:
                    logger.warning("[Feishu] Debug msg %d/%d HTTP %d", i + 1, len(messages), resp.status_code)
                    success = False
            except Exception as e:
                logger.error("[Feishu] Debug msg %d/%d failed: %s", i + 1, len(messages), e)
                success = False
            if i < len(messages) - 1:
                await asyncio.sleep(1.0)

        if success:
            logger.info("[Feishu] Test debug sent (%d messages): %s", len(messages), news.title[:50])
        return success

    @staticmethod
    def _truncate(text: str, max_len: int = 3000) -> str:
        if not text:
            return "(empty)"
        if len(text) <= max_len:
            return text
        return text[:max_len] + f"\n... [truncated, {len(text)} total chars]"

    def _build_debug_messages(self, news: NewsItem, pipeline_result: dict) -> list:
        """Build structured debug messages for test mode.

        Returns a list of text strings and/or card dicts.
        """
        MAX_MSG_LEN = 15000
        messages = []
        traces = pipeline_result.get("llm_traces", [])
        filter_res = pipeline_result.get("filter")
        analysis = pipeline_result.get("analysis")
        signal = pipeline_result.get("signal_score")
        research = pipeline_result.get("research")
        evaluation = pipeline_result.get("initial_evaluation")
        deep_research = pipeline_result.get("deep_research")
        final_assessment = pipeline_result.get("final_assessment")
        search_verification = pipeline_result.get("search_verification")

        # --- Message 1: Overview + Phase 1 ---
        msg1_parts = []
        msg1_parts.append(
            f"{'═' * 40}\n"
            f"🔧 [TEST] Pipeline Debug Report\n"
            f"{'═' * 40}\n"
            f"Source: {news.source_name}\n"
            f"Title: {news.title}\n"
            f"URL: {news.url}\n"
            f"Published: {news.published_at}\n"
            f"Content length: {len(news.content or '')} chars\n"
            f"Pipeline stage reached: {pipeline_result.get('stage')}\n"
            f"Alert level: {pipeline_result.get('alert_level') or 'NONE'}\n"
            f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"{'═' * 40}"
        )

        # Phase 1 results
        if evaluation:
            msg1_parts.append(
                f"\n{'─' * 30}\n"
                f"📋 PHASE 1: INITIAL EVALUATION\n"
                f"{'─' * 30}\n"
                f"Relevance score: {evaluation.relevance_score:.2f}\n"
                f"May affect market: {evaluation.may_affect_market}\n"
                f"Reason: {evaluation.reason}\n"
                f"Related stocks: {evaluation.related_stocks}\n"
                f"Related sectors: {evaluation.related_sectors}\n"
                f"Search queries: {json.dumps(evaluation.search_queries, ensure_ascii=False, indent=2)}"
            )
        elif filter_res:
            msg1_parts.append(
                f"\n{'─' * 30}\n"
                f"📋 PHASE 1: FILTER (legacy)\n"
                f"{'─' * 30}\n"
                f"Relevant: {filter_res.is_relevant}, Score: {filter_res.relevance_score:.2f}\n"
                f"Reason: {filter_res.reason}"
            )

        # Phase 1 LLM trace
        phase1_trace = next((t for t in traces if t.get("stage") == "phase1_evaluate"), None)
        if not phase1_trace:
            phase1_trace = next((t for t in traces if t.get("stage") == "filter"), None)
        if phase1_trace:
            msg1_parts.append(
                f"\n[Model] {phase1_trace.get('model', 'N/A')}\n"
                f"[Tokens] prompt={phase1_trace.get('usage', {}).get('prompt_tokens', 'N/A')}, "
                f"completion={phase1_trace.get('usage', {}).get('completion_tokens', 'N/A')}\n"
                f"\n[System Prompt]\n{self._truncate(phase1_trace.get('system_prompt', ''), 2000)}\n"
                f"\n[User Prompt]\n{self._truncate(phase1_trace.get('user_prompt', ''), 2000)}\n"
                f"\n[Raw Response]\n{self._truncate(phase1_trace.get('raw_response', ''), 2000)}"
            )

        msg1 = "\n".join(msg1_parts)
        if len(msg1) > MAX_MSG_LEN:
            msg1 = msg1[:MAX_MSG_LEN] + "\n... [truncated]"
        messages.append(msg1)

        # --- Message 2: Phase 2 Deep Research ---
        phase2_traces = [t for t in traces if t.get("stage", "").startswith("phase2_")]
        if phase2_traces or deep_research:
            msg2_parts = []
            msg2_parts.append(
                f"{'─' * 30}\n"
                f"🔍 PHASE 2: DEEP RESEARCH\n"
                f"{'─' * 30}"
            )

            if deep_research:
                msg2_parts.append(
                    f"\nTotal iterations: {deep_research.total_iterations}\n"
                    f"Total search results: {len(deep_research.all_search_results)}\n"
                    f"Total fetched pages: {len(deep_research.all_fetched_pages)}\n"
                    f"Price data tickers: {list(deep_research.price_data.keys())}\n"
                    f"Citations: {{{', '.join(f'{k}: {len(v)}' for k, v in deep_research.citations.items())}}}\n"
                    f"Research summary: {self._truncate(deep_research.research_summary, 1000)}"
                )

            for trace in phase2_traces:
                stage_name = trace.get("stage", "")
                model = trace.get("model", "N/A")
                usage = trace.get("usage", {})
                search_count = trace.get("search_count", 0)
                use_google = trace.get("use_google", False)

                msg2_parts.append(
                    f"\n--- {stage_name} (model: {model}) ---\n"
                    f"Tokens: prompt={usage.get('prompt_tokens', 'N/A')}, "
                    f"completion={usage.get('completion_tokens', 'N/A')}\n"
                    f"Search results: {search_count}, Google: {use_google}\n"
                    f"\n[User Prompt]\n{self._truncate(trace.get('user_prompt', ''), 2000)}\n"
                    f"\n[Raw Response]\n{self._truncate(trace.get('raw_response', ''), 2000)}"
                )

            msg2 = "\n".join(msg2_parts)
            if len(msg2) > MAX_MSG_LEN:
                msg2 = msg2[:MAX_MSG_LEN] + "\n... [truncated]"
            messages.append(msg2)

        # --- Message 3: Phase 3 Final Assessment ---
        phase3_trace = next((t for t in traces if t.get("stage") == "phase3_assess"), None)
        if phase3_trace or final_assessment:
            msg3_parts = []
            msg3_parts.append(
                f"{'─' * 30}\n"
                f"📊 PHASE 3: FINAL ASSESSMENT\n"
                f"{'─' * 30}"
            )

            if final_assessment:
                msg3_parts.append(
                    f"\nSentiment: {final_assessment.sentiment}\n"
                    f"Surprise factor: {final_assessment.surprise_factor:.2f}\n"
                    f"Impact: {final_assessment.impact_magnitude} | {final_assessment.impact_timeframe}\n"
                    f"Timeliness: {final_assessment.timeliness}\n"
                    f"Confidence: {final_assessment.confidence:.2f}\n"
                    f"Summary: {final_assessment.summary}\n"
                    f"Key findings: {final_assessment.key_findings}\n"
                    f"Market expectation: {final_assessment.market_expectation}\n"
                    f"Bull case: {final_assessment.bull_case[:200]}\n"
                    f"Bear case: {final_assessment.bear_case[:200]}\n"
                    f"Recommended action: {final_assessment.recommended_action}"
                )

            if phase3_trace:
                msg3_parts.append(
                    f"\n[Model] {phase3_trace.get('model', 'N/A')}\n"
                    f"[Tokens] prompt={phase3_trace.get('usage', {}).get('prompt_tokens', 'N/A')}, "
                    f"completion={phase3_trace.get('usage', {}).get('completion_tokens', 'N/A')}\n"
                    f"\n[System Prompt]\n{self._truncate(phase3_trace.get('system_prompt', ''), 2000)}\n"
                    f"\n[User Prompt]\n{self._truncate(phase3_trace.get('user_prompt', ''), 3000)}\n"
                    f"\n[Raw Response]\n{self._truncate(phase3_trace.get('raw_response', ''), 3000)}"
                )

            if signal:
                msg3_parts.append(
                    f"\n{'─' * 20}\n"
                    f"Signal: tier={signal.tier}, timeliness={signal.timeliness}, "
                    f"composite={signal.composite_score:.4f}"
                )

            msg3 = "\n".join(msg3_parts)
            if len(msg3) > MAX_MSG_LEN:
                msg3 = msg3[:MAX_MSG_LEN] + "\n... [truncated]"
            messages.append(msg3)

        # --- Message 4: Final Card (actual output) ---
        if analysis and pipeline_result.get("alert_level"):
            card = self._build_card(
                news=news,
                analysis=analysis,
                research=research,
                alert_level=pipeline_result.get("alert_level", "medium"),
                signal_score=signal.composite_score if signal else 0.0,
                search_verification=search_verification,
                signal_score_obj=signal,
                initial_evaluation=evaluation,
                deep_research=deep_research,
                final_assessment=final_assessment,
            )
            messages.append(card)

        return messages
