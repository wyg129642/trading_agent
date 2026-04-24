"""Feishu alert card builder for proactive breaking news monitoring.

v3: Event-driven breaking news cards with:
- Historical precedent price impact table
- News propagation timeline (earliest report → alert push)
- Current stock price in footer
- No cost information
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from engine.proactive.models import ProactiveScanResult

logger = logging.getLogger(__name__)

CST = ZoneInfo("Asia/Shanghai")

MAGNITUDE_ICONS = {
    "critical": "🔴🔴🔴",
    "high": "🔴🔴",
    "medium": "🟡",
    "low": "🟢",
}

SENTIMENT_ICONS = {
    "very_bullish": "📈📈",
    "bullish": "📈",
    "neutral": "➡️",
    "bearish": "📉",
    "very_bearish": "📉📉",
}

SENTIMENT_CN = {
    "very_bullish": "极度看多",
    "bullish": "看多",
    "neutral": "中性",
    "bearish": "看空",
    "very_bearish": "极度看空",
}

TIMEFRAME_CN = {
    "short_term": "短期",
    "medium_term": "中期",
    "long_term": "长期",
}


def _fmt_age(hours: float) -> str:
    """Format an age in hours as a compact human string.

    Rounding rules match the UI (Portfolio.tsx timeAgo): <60min shown as
    minutes, <24h as hours, else days.
    """
    if hours < 0:
        hours = 0.0
    if hours < 1:
        return f"{int(hours * 60)}分钟前"
    if hours < 24:
        return f"{int(hours)}小时前"
    return f"{int(hours / 24)}天前"


def _resolve_display_novelty(result, analysis: dict) -> str:
    """Pick the novelty label to render.

    The freshness gate sets `rejection_reason=event_too_old` in
    full_analysis and forces `novelty_status=stale` on the result, so we
    prefer the result's post-gate novelty_status over whatever the LLM
    originally said. If event_age_hours is known and large but the gate
    didn't rewrite novelty (e.g. corroborated story), we still downgrade
    a "verified_fresh" claim to "likely_fresh" to avoid overclaiming.
    """
    status = result.novelty_status or analysis.get("novelty_status") or ""
    if analysis.get("rejection_reason") == "event_too_old":
        return "stale"
    age = analysis.get("event_age_hours")
    if isinstance(age, (int, float)) and age >= 48 and status == "verified_fresh":
        return "likely_fresh"
    return status


def _price_link(ticker: str, market: str) -> str:
    if market == "china":
        code = ticker.split(".")[0] if "." in ticker else ticker
        return f"https://quote.eastmoney.com/{code}.html"
    if market in ("us",):
        return f"https://finance.yahoo.com/quote/{ticker}"
    if market == "hk":
        code = ticker.split(".")[0] if ".HK" in ticker.upper() else ticker
        return f"https://finance.yahoo.com/quote/{code}.HK"
    return ""


class ProactiveAlertFormatter:
    """Build Feishu interactive cards for proactive breaking news alerts."""

    def __init__(self, alerter):
        self._alerter = alerter
        self._proactive_alert_history: list[dict] = []

    def should_suppress(
        self, ticker: str, max_per_stock_4h: int = 1, max_per_hour_global: int = 5,
    ) -> str | None:
        """Check proactive-specific dedup rules."""
        now = datetime.now(timezone.utc)

        cutoff_4h = now - timedelta(hours=4)
        cutoff_1h = now - timedelta(hours=1)
        self._proactive_alert_history = [
            h for h in self._proactive_alert_history if h["time"] > cutoff_4h
        ]

        stock_count = sum(
            1 for h in self._proactive_alert_history
            if h["ticker"] == ticker and h["time"] > cutoff_4h
        )
        if stock_count >= max_per_stock_4h:
            return f"proactive_rate:{ticker}({stock_count}/{max_per_stock_4h} in 4h)"

        global_count = sum(
            1 for h in self._proactive_alert_history if h["time"] > cutoff_1h
        )
        if global_count >= max_per_hour_global:
            return f"proactive_global:{global_count}/{max_per_hour_global} in 1h"

        # Cross-system check
        cutoff_2h = now - timedelta(hours=2)
        for h in self._alerter._alert_history:
            h_time = h.get("time")
            if h_time and h_time.tzinfo is None:
                h_time = h_time.replace(tzinfo=timezone.utc)
            if h_time and h_time > cutoff_2h and ticker in h.get("tickers", []):
                return f"reactive_covered:{ticker}"

        return None

    def record_alert(self, ticker: str) -> None:
        self._proactive_alert_history.append({
            "ticker": ticker,
            "time": datetime.now(timezone.utc),
        })

    def build_card(self, result: ProactiveScanResult) -> dict:
        """Build a Feishu interactive card for a breaking news alert."""
        analysis = result.full_analysis or {}
        holding = result.holding
        magnitude = analysis.get("impact_magnitude", "medium")
        sentiment = analysis.get("sentiment", "neutral")
        confidence = analysis.get("alert_confidence", result.alert_confidence)
        summary = analysis.get("summary", result.news_summary)
        bull_case = analysis.get("bull_case", "")
        bear_case = analysis.get("bear_case", "")
        recommended = analysis.get("recommended_action", "")
        key_findings = analysis.get("key_findings", result.key_findings)
        sources = analysis.get("sources", [])
        timeline = result.news_timeline
        timeframe = analysis.get("impact_timeframe", "short_term")
        # Event-freshness gate (freshness_gate.py) overrides LLM novelty_status
        # when the underlying event is provably stale. We always prefer the
        # computed event age over the LLM's qualitative label.
        novelty_status = _resolve_display_novelty(result, analysis)
        event_age_hours = analysis.get("event_age_hours")

        mag_icon = MAGNITUDE_ICONS.get(magnitude, "")
        sent_icon = SENTIMENT_ICONS.get(sentiment, "")
        sent_cn = SENTIMENT_CN.get(sentiment, sentiment)
        tf_cn = TIMEFRAME_CN.get(timeframe, timeframe)

        # Title: 突发新闻 instead of 持仓预警
        mag_label = magnitude.upper() if magnitude else "ALERT"
        title = (
            f"🚨 突发新闻 | {mag_icon} {mag_label} | "
            f"{holding.name_cn} ({holding.ticker}) {holding.market_label}"
        )

        elements = []

        # Section 1: Breaking news summary
        elements.append({
            "tag": "div",
            "text": {"content": f"**📰 突发消息**\n{summary}", "tag": "lark_md"},
        })
        elements.append({"tag": "hr"})

        # Section 2: Key findings
        if key_findings:
            findings_text = "\n".join(f"• {f}" for f in key_findings[:6])
            elements.append({
                "tag": "div",
                "text": {"content": f"**📊 关键发现**\n{findings_text}", "tag": "lark_md"},
            })
            elements.append({"tag": "hr"})

        # Section 3: Historical precedent table (NEW in v3)
        precedent_text = self._build_precedent_section(result.historical_precedents)
        if precedent_text:
            elements.append({
                "tag": "div",
                "text": {"content": precedent_text, "tag": "lark_md"},
            })
            elements.append({"tag": "hr"})

        # Historical evidence summary from LLM
        hist_evidence = analysis.get("historical_evidence_summary", "")
        if hist_evidence:
            elements.append({
                "tag": "div",
                "text": {"content": f"**📝 历史分析**: {hist_evidence}", "tag": "lark_md"},
            })
            elements.append({"tag": "hr"})

        # Section 4: News propagation timeline
        if timeline:
            tl_text = self._build_news_propagation_timeline(timeline)
            elements.append({
                "tag": "div",
                "text": {"content": tl_text, "tag": "lark_md"},
            })
            elements.append({"tag": "hr"})

        # Section 5: Impact assessment
        novelty_label = {
            "verified_fresh": "✅ 确认新鲜",
            "likely_fresh": "🔵 可能新鲜",
            "stale": "⚠️ 可能旧闻",
            "repackaged": "❌ 旧闻重包装",
        }.get(novelty_status, novelty_status)

        if isinstance(event_age_hours, (int, float)):
            novelty_label = f"{novelty_label} (事件 {_fmt_age(event_age_hours)})"

        assess_lines = [
            f"方向: {sent_icon} {sent_cn} | 信心度: {confidence:.0%} | 新鲜度: {novelty_label}",
            f"影响级别: {mag_icon} {magnitude} | 时间窗口: {tf_cn}",
        ]
        elements.append({
            "tag": "div",
            "text": {"content": f"**📈 影响评估**\n" + "\n".join(assess_lines), "tag": "lark_md"},
        })

        # Bull/bear cases
        if bull_case or bear_case:
            cases = []
            if bull_case:
                cases.append(f"🐂 看多: {bull_case}")
            if bear_case:
                cases.append(f"🐻 看空: {bear_case}")
            elements.append({
                "tag": "div",
                "text": {"content": "\n".join(cases), "tag": "lark_md"},
            })

        # Recommended action
        if recommended:
            elements.append({
                "tag": "div",
                "text": {"content": f"**💡 建议**: {recommended}", "tag": "lark_md"},
            })

        elements.append({"tag": "hr"})

        # Source citations
        if sources:
            src_lines = [f"**📰 信息来源 (共{len(sources)}条)**"]
            for src in sources[:10]:
                src_type = "[内部]" if src.get("source_type") == "internal" else "[外部]"
                label = src.get("source_label", "")
                src_title = src.get("title", "")[:60]
                date = src.get("date", "")
                url = src.get("url", "")
                if url:
                    src_lines.append(f"{src_type} {label}: [{src_title}]({url}) ({date})")
                else:
                    src_lines.append(f"{src_type} {label}: {src_title} ({date})")
            elements.append({
                "tag": "div",
                "text": {"content": "\n".join(src_lines), "tag": "lark_md"},
            })
            elements.append({"tag": "hr"})

        # Footer: current price + research depth + price link
        price_url = _price_link(holding.ticker, holding.market)
        footer_parts = []

        # Extract current price from snapshot price_data if possible
        current_price = self._extract_current_price(result.snapshot.price_data)
        if current_price:
            footer_parts.append(current_price)

        footer_parts.append(f"研究深度: {result.research_iterations}轮 / {len(result.referenced_sources)}源")
        if price_url:
            footer_parts.append(f"[查看行情]({price_url})")

        elements.append({
            "tag": "div",
            "text": {"content": " | ".join(footer_parts), "tag": "lark_md"},
        })

        return {
            "msg_type": "interactive",
            "card": {
                "config": {"wide_screen_mode": True},
                "header": {
                    "title": {"content": title, "tag": "plain_text"},
                    "template": "red" if magnitude in ("critical", "high") else "orange",
                },
                "elements": elements,
            },
        }

    def _build_precedent_section(self, precedents: list[dict]) -> str:
        """Build the historical precedent comparison section."""
        if not precedents:
            return ""

        lines = ["**📈 历史先例对比**"]
        lines.append("`日期 | 事件 | 1日 | 3日 | 5日`")

        valid_1d = []
        for p in precedents[:5]:
            date = p.get("event_date", "?")
            desc = p.get("description", "?")[:25]
            r1 = p.get("return_1d")
            r3 = p.get("return_3d")
            r5 = p.get("return_5d")

            r1_str = f"{r1:+.1f}%" if r1 is not None else "N/A"
            r3_str = f"{r3:+.1f}%" if r3 is not None else "N/A"
            r5_str = f"{r5:+.1f}%" if r5 is not None else "N/A"

            lines.append(f"`{date} | {desc} | {r1_str} | {r3_str} | {r5_str}`")

            if r1 is not None:
                valid_1d.append(r1)

        # Average
        if len(valid_1d) >= 2:
            all_r1 = [p.get("return_1d") for p in precedents if p.get("return_1d") is not None]
            all_r3 = [p.get("return_3d") for p in precedents if p.get("return_3d") is not None]
            all_r5 = [p.get("return_5d") for p in precedents if p.get("return_5d") is not None]
            avg_1 = f"{sum(all_r1)/len(all_r1):+.1f}%" if all_r1 else "N/A"
            avg_3 = f"{sum(all_r3)/len(all_r3):+.1f}%" if all_r3 else "N/A"
            avg_5 = f"{sum(all_r5)/len(all_r5):+.1f}%" if all_r5 else "N/A"
            lines.append(f"`平均 | — | {avg_1} | {avg_3} | {avg_5}`")

        return "\n".join(lines)

    def _build_news_propagation_timeline(self, timeline: list[dict]) -> str:
        """Build the news propagation timeline showing when each source reported."""
        lines = ["**⏱ 新闻传播时间线**"]

        for entry in timeline[:8]:
            time_str = entry.get("time", "?")
            source = entry.get("source", "?")
            entry_title = entry.get("title", "?")[:55]
            lines.append(f"{time_str} → {source}: {entry_title}")

        # Alert push time (in CST for display)
        now_cst = datetime.now(timezone.utc).astimezone(CST)
        lines.append(f"{now_cst.strftime('%Y-%m-%d %H:%M')} → 🔔 本次检测推送")

        return "\n".join(lines)

    def _extract_current_price(self, price_data_text: str) -> str:
        """Try to extract the latest price from price data text."""
        if not price_data_text:
            return ""

        # Parse last line of the price table (format: date open high low close change% vol)
        lines = price_data_text.strip().split("\n")
        for line in reversed(lines):
            parts = line.split()
            if len(parts) >= 6 and parts[0][:4].isdigit():
                try:
                    close = float(parts[4])
                    change = parts[5] if len(parts) > 5 else ""
                    return f"当前: {close:.2f} ({change})"
                except (ValueError, IndexError):
                    continue
        return ""

    # ------------------------------------------------------------------
    # Morning briefing (updated for v3 field names)
    # ------------------------------------------------------------------

    def build_morning_briefing(self, scan_summaries: list[dict]) -> dict:
        """Build a Feishu message for the morning briefing."""
        now_cst = datetime.now(timezone.utc).astimezone(CST)
        date_str = now_cst.strftime("%Y-%m-%d %H:%M")

        green = []
        yellow = []
        red = []

        for s in scan_summaries:
            materiality = s.get("news_materiality", s.get("delta_magnitude", "none"))
            if materiality in ("material", "significant", "critical"):
                if materiality == "critical":
                    red.append(s)
                else:
                    yellow.append(s)
            elif materiality in ("routine", "minor"):
                yellow.append(s)
            else:
                green.append(s)

        lines = [f"📋 持仓晨报 | {date_str}\n"]

        lines.append(f"🟢 无重大变化 ({len(green)}/{len(scan_summaries)})")

        if yellow:
            lines.append(f"🟡 值得关注 ({len(yellow)}):")
            for s in yellow:
                narrative = s.get("narrative", s.get("news_summary", "无详情"))
                lines.append(f"  • {s['ticker']} {s['name']}: {narrative[:80]}")

        if red:
            lines.append(f"🔴 重点关注 ({len(red)}):")
            for s in red:
                narrative = s.get("narrative", s.get("news_summary", "无详情"))
                lines.append(f"  • {s['ticker']} {s['name']}: {narrative[:120]}")

        total_scans = sum(s.get("scan_count", 0) for s in scan_summaries)
        deep_count = sum(1 for s in scan_summaries if s.get("deep_research", False))
        alert_count = sum(1 for s in scan_summaries if s.get("alerted", False))
        lines.append(f"\n📊 昨日扫描统计: {total_scans}次 | 深度研究: {deep_count}次 | 预警: {alert_count}次")

        return {"msg_type": "text", "content": {"text": "\n".join(lines)}}
