#!/usr/bin/env python3
"""End-to-end test: run a sample news item through the full upgraded pipeline
and send the result to Feishu to verify the enhanced card format.
"""

import asyncio
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).resolve().parent
sys.path.insert(0, str(project_root))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)-25s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logging.getLogger("httpx").setLevel(logging.WARNING)

logger = logging.getLogger("e2e_test")


async def main():
    import yaml
    from src.models import NewsItem
    from src.database import Database
    from src.utils.token_tracker import TokenTracker
    from src.analysis.llm_client import LLMClient
    from src.analysis.pipeline import AnalysisPipeline
    from src.alerting.feishu import FeishuAlerter
    from src.tools.uqer_api import get_market_data
    from src.tools.web_search import web_search

    # Load config
    with open("config/settings.yaml", "r", encoding="utf-8") as f:
        settings = yaml.safe_load(f)

    # --- Initialize components ---
    db = Database(settings.get("database", {}).get("path", "data/news_monitor.db"))
    await db.initialize()

    budget_cfg = settings.get("token_budget", {})
    tracker = TokenTracker(
        daily_budget_cny=budget_cfg.get("daily_budget_cny", 50.0),
        rate_limit_tpm=budget_cfg.get("rate_limit_tpm", 1_000_000),
    )

    llm = LLMClient(settings, tracker=tracker)
    uqer_token = settings.get("uqer", {}).get("token", "")

    # Tool executor (for Stage 3)
    async def tool_executor(name: str, args: dict) -> str:
        if name == "web_search":
            return await web_search(args["query"], max_results=5)
        elif name == "get_market_data":
            return await get_market_data(
                args["ticker"], args["begin_date"], args["end_date"], token=uqer_token
            )
        else:
            return f"Unknown tool: {name}"

    pipeline = AnalysisPipeline(
        llm=llm, db=db, settings=settings,
        tool_executor=tool_executor, content_fetcher=None,
        uqer_token=uqer_token,
    )

    feishu_cfg = settings.get("feishu", {})
    alert_mgmt = settings.get("alert_management", {})
    alerter = FeishuAlerter(
        webhook_url=feishu_cfg["webhook_url"],
        alert_levels=feishu_cfg.get("alert_levels", ["critical", "high", "medium"]),
        max_alerts_per_ticker_per_hour=alert_mgmt.get("max_alerts_per_ticker_per_hour", 99),
        dedup_window_minutes=0,  # Disable dedup for testing
    )

    # --- Create a realistic test news item ---
    test_item = NewsItem(
        source_name="Reuters",
        title="NVIDIA reports Q4 revenue of $39.3 billion, beats estimates by 8%; data center revenue surges 93% YoY on AI chip demand",
        url="https://www.reuters.com/technology/nvidia-q4-2026-earnings/",
        content="""NVIDIA Corporation reported fourth-quarter revenue of $39.3 billion,
surpassing Wall Street consensus estimates of $36.4 billion by approximately 8%.
Data center revenue reached $35.1 billion, up 93% year-over-year, driven by
strong demand for H200 and Blackwell AI accelerators.

CEO Jensen Huang said: "The age of AI is in full steam. NVIDIA's data center
business has become the engine of the world's AI infrastructure buildout."

Key highlights:
- Q4 Revenue: $39.3B (vs. $36.4B consensus)
- Data Center Revenue: $35.1B (+93% YoY)
- Gaming Revenue: $3.1B (+5% YoY)
- Gross Margin: 73.5% (vs. 72.1% estimate)
- Q1 FY2027 Guidance: $42B ± 2% (vs. $38B consensus)
- Blackwell architecture chips now in full production
- China revenue declined 12% due to export controls

The results sent NVIDIA shares up 5.2% in after-hours trading.
Analysts at Morgan Stanley raised their price target to $180, citing
"structural demand tailwinds from AI infrastructure buildout that shows
no signs of slowing."

Impact on supply chain: 中际旭创(300308)作为NVIDIA光模块核心供应商，
预计将直接受益于数据中心需求的持续增长。寒武纪(688256)和海光信息(688041)
作为国产AI芯片替代厂商，可能因NVIDIA出口管制导致的供应缺口而间接受益。
浪潮信息(000977)作为AI服务器龙头也将受益。

市场分析师认为，NVIDIA持续超预期的业绩表明AI基础设施投资周期远未结束，
这对整个AI芯片供应链和数据中心产业链都是重大利好信号。""",
        published_at=datetime.now(timezone.utc) - timedelta(hours=2),
        language="en",
        market="us",
    )

    # Save to DB
    await db.save_news_item(test_item)

    # --- Run full pipeline ---
    print("\n" + "=" * 70)
    print("   E2E TEST: Running sample news through upgraded pipeline")
    print("=" * 70)

    print(f"\n📰 Test item: {test_item.title[:80]}...")
    print(f"   Source: {test_item.source_name}")
    print(f"   Market: {test_item.market}")
    print(f"   Published: {test_item.published_at}")

    result = await pipeline.process(test_item)

    # --- Print results ---
    filt = result.get("filter")
    if filt:
        print(f"\n--- Stage 1: Filter ---")
        print(f"   relevant={filt.is_relevant}, score={filt.relevance_score:.2f}")
        print(f"   reason: {filt.reason[:200]}")

    analysis = result.get("analysis")
    if analysis:
        print(f"\n--- Stage 2: Analysis ---")
        print(f"   Sentiment: {analysis.sentiment}")
        print(f"   Impact: {analysis.impact_magnitude}")
        print(f"   Surprise: {analysis.surprise_factor:.2f}")
        print(f"   Is routine: {analysis.is_routine}")
        print(f"   Tickers: {analysis.affected_tickers}")
        print(f"   Sectors: {analysis.affected_sectors}")
        print(f"   Category: {analysis.category}")
        print(f"   Summary: {analysis.summary[:200]}")
        print(f"   Search questions: {analysis.search_questions}")
        print(f"   Market expectation: {analysis.market_expectation[:150]}")
        print(f"   Requires deep research: {analysis.requires_deep_research}")

    signal = result.get("signal_score")
    if signal:
        print(f"\n--- Signal Score ---")
        print(f"   Tier (from impact): {signal.tier}, Timeliness: {signal.timeliness}")
        print(f"   Informational composite: {signal.composite_score:.4f}")

    sv = result.get("search_verification")
    if sv:
        print(f"\n--- Stage 2.5: Search Verification ---")
        print(f"   Related news: {len(sv.related_news)} items")
        for rn in sv.related_news[:3]:
            print(f"     - {rn.get('title', 'N/A')[:60]} ({rn.get('date', 'N/A')})")
        print(f"   Price data tickers: {list(sv.price_data.keys())}")
        print(f"   Timeliness: {sv.timeliness_info[:200]}")
        print(f"   Verification summary: {sv.verification_summary[:300]}")

    research = result.get("research")
    if research:
        print(f"\n--- Stage 3: Research ---")
        print(f"   Executive summary: {research.executive_summary[:200]}")
        print(f"   Confidence: {research.confidence:.2f}")

    alert_level = result.get("alert_level")
    print(f"\n--- Alert Decision ---")
    print(f"   Alert level: {alert_level or 'NONE (suppressed)'}")
    print(f"   Pipeline stage reached: {result.get('stage')}")

    # --- Send to Feishu ---
    if analysis and signal:
        # Force send regardless of alert_level for testing
        send_level = alert_level or "high"
        print(f"\n📤 Sending test alert to Feishu (level={send_level})...")
        sent = await alerter.send_alert(
            news=test_item,
            analysis=analysis,
            research=research,
            alert_level=send_level,
            signal_score=signal.composite_score,
            search_verification=sv,
            signal_score_obj=signal,
        )
        if sent:
            print("   ✅ Feishu alert sent successfully!")
        else:
            print("   ❌ Feishu alert failed to send")

        # Also send a medium-tier text alert for comparison
        print(f"\n📤 Sending text-format alert to Feishu (level=medium)...")
        sent2 = await alerter.send_alert(
            news=test_item,
            analysis=analysis,
            research=None,
            alert_level="medium",
            signal_score=signal.composite_score,
            search_verification=sv,
            signal_score_obj=signal,
        )
        if sent2:
            print("   ✅ Text alert sent successfully!")
        else:
            print("   ❌ Text alert failed to send")
    else:
        print("\n⚠️ No analysis result — cannot send Feishu alert")
        # Send a minimal test card anyway
        if filt:
            print("   (News was filtered out at Stage 1)")

    # --- Token report ---
    print(f"\n--- Token Usage ---")
    print(tracker.format_report(hours=1))

    # Cleanup
    await alerter.close()
    await db.close()
    print("\n✅ E2E test complete!")


if __name__ == "__main__":
    asyncio.run(main())
