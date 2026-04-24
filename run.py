#!/usr/bin/env python3
"""
AI Trading Agent — Entry Point
===============================

Monitor news sources and analyze their stock market impact.

Usage:
    python run.py              # Start the full monitoring system
    python run.py --test       # Run a quick test (single cycle, debug trace)
    python run.py --test-mode  # Test mode: bypass filter, force items, full debug to Feishu
    python run.py --status     # Show database stats
    python run.py --tokens     # Show token usage

Configuration:
    config/settings.yaml       # API keys, thresholds, intervals
    config/sources.yaml        # Data sources, company tickers
"""

import argparse
import asyncio
import logging
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).resolve().parent
sys.path.insert(0, str(project_root))


async def run_full():
    """Start the full monitoring system."""
    from src.main import main
    await main()


async def run_test(test_count: int = 1):
    """Run a single-cycle test with full debug output to Feishu.

    Args:
        test_count: Number of items to test. 0 means all sources.

    Pipeline runs with debug_trace=True (captures all LLM interactions).
    Normal thresholds apply — sends full debug info to Feishu.
    """
    import json as _json
    import traceback
    from datetime import datetime, timedelta
    from src.main import TradingAgent
    from src.models import NewsItem

    agent = TradingAgent()
    await agent.initialize()

    print("\n✅ System initialized successfully!")
    print(f"   Monitors: {len(agent.monitors)}")
    print(f"   Baidu API: {'configured' if agent.settings.get('baidu', {}).get('api_key') else 'NOT configured'}")
    for m in agent.monitors:
        print(f"   - [{m.priority}] {m.name}")

    # Warm-start dedup caches
    print("\n🔄 Loading known items from database...")
    for monitor in agent.monitors:
        existing = await agent.db.get_content_hashes_by_source(monitor.name)
        monitor._seen_hashes.update(existing)
        last_dt = await agent.db.get_latest_published_at(monitor.name)
        monitor._last_seen_dt = last_dt

    print("\n📡 Running one polling cycle for each monitor...")
    total_items = 0
    all_items = []
    for monitor in agent.monitors:
        try:
            items = await monitor.poll()
            total_items += len(items)
            if items:
                for item in items:
                    all_items.append((item, "new"))
                print(f"   [{monitor.name}]: ✅ {len(items)} new items")
                for item in items[:3]:
                    print(f"      → {item.title[:70]}")
            else:
                row = await agent.db.get_newest_item_by_source(monitor.name)
                if row:
                    item = NewsItem(
                        source_name=row["source_name"],
                        title=row["title"],
                        url=row["url"],
                        content=row["content"] or "",
                        published_at=(
                            datetime.fromisoformat(row["published_at"])
                            if row["published_at"] else None
                        ),
                        language=row["language"] or "zh",
                        market=row["market"] or "china",
                        metadata=(_json.loads(row["metadata"]) if row["metadata"] else {}),
                    )
                    item.id = row["id"]
                    all_items.append((item, "from_db"))
                    print(f"   [{monitor.name}]: ✅ 0 new (using DB: {item.title[:55]})")
                else:
                    print(f"   [{monitor.name}]: ✅ 0 items (no DB history)")
        except Exception as e:
            print(f"   [{monitor.name}]: ❌ Error: {e}")

    print(f"\n📊 Total new items: {total_items}")
    print(f"📦 Total candidates: {len(all_items)}")

    if not all_items:
        print("\n⚠️ No items available — cannot test")
        await agent.shutdown()
        return

    # Pick best candidates
    now = datetime.now()
    max_age = timedelta(hours=72)

    def _candidate_score(entry):
        item, origin = entry
        score = 0
        if origin == "new":
            score += 200
        if item.content and len(item.content) > 50:
            score += 100
        tlen = len(item.title)
        if 10 < tlen < 200:
            score += min(tlen, 80)
        if item.published_at:
            try:
                pub = item.published_at
                if pub.tzinfo is not None:
                    from datetime import timezone
                    age = datetime.now(timezone.utc) - pub
                else:
                    age = now - pub
                if age < max_age:
                    score += 50
            except Exception:
                pass
        if item.language == "en":
            score += 20
        return score

    best_per_source: dict[str, tuple] = {}
    for entry in all_items:
        item, origin = entry
        src = item.source_name
        if src not in best_per_source or _candidate_score(entry) > _candidate_score(best_per_source[src]):
            best_per_source[src] = entry

    sorted_sources = sorted(best_per_source.values(), key=_candidate_score, reverse=True)

    if test_count == 0:
        to_test = sorted_sources
    else:
        to_test = sorted_sources[:test_count]

    print(f"\n🎯 Will test {len(to_test)} source(s)")
    tested = 0

    for ci, (test_item, origin) in enumerate(to_test):
        tested += 1
        print(f"\n{'=' * 60}")
        print(f"🧠 [{tested}/{len(to_test)}] {test_item.title[:80]}")
        print(f"   Source: {test_item.source_name} | Origin: {origin}")
        print(f"   URL: {test_item.url}")
        print(f"   Content: {len(test_item.content or '')} chars")
        print(f"{'=' * 60}")

        try:
            result = await agent.pipeline.process(test_item, debug_trace=True)

            _print_pipeline_result(result)

            # Send debug info to Feishu
            print(f"\n   📤 Sending debug info to Feishu...")
            sent = await agent.alerter.send_test_debug(test_item, result)
            print(f"   Feishu: {'✅ sent OK' if sent else '⚠️ send had issues'}")

        except Exception as e:
            print(f"   ❌ Pipeline error: {e}")
            traceback.print_exc()

    await agent.shutdown()
    print(f"\n✅ Test complete! Processed {tested} item(s).")


async def run_test_mode():
    """Test mode: bypass filter, force all phases, send full debug to Feishu."""
    import json as _json
    import traceback
    from datetime import datetime
    from src.main import TradingAgent
    from src.models import NewsItem

    agent = TradingAgent()
    await agent.initialize()

    print(f"\n{'=' * 60}")
    print(f"  [TEST MODE] AI Trading Agent Debug Run")
    print(f"  Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Monitors: {len(agent.monitors)}")
    print(f"  Baidu API: {'configured' if agent.settings.get('baidu', {}).get('api_key') else 'NOT configured'}")
    print(f"{'=' * 60}")

    # Warm-start
    for monitor in agent.monitors:
        existing = await agent.db.get_content_hashes_by_source(monitor.name)
        monitor._seen_hashes.update(existing)
        last_dt = await agent.db.get_latest_published_at(monitor.name)
        monitor._last_seen_dt = last_dt

    test_items: list[tuple[str, NewsItem, str]] = []

    print("\nPolling sources...")
    for monitor in agent.monitors:
        try:
            items = await monitor.poll()
            if items:
                selected = items[0]
                await agent.db.save_news_item(selected)
                test_items.append((monitor.name, selected, "new"))
                print(f"  [{monitor.name}]: NEW → {selected.title[:60]}")
            else:
                row = await agent.db.get_newest_item_by_source(monitor.name)
                if row:
                    item = NewsItem(
                        source_name=row["source_name"],
                        title=row["title"],
                        url=row["url"],
                        content=row["content"] or "",
                        published_at=(
                            datetime.fromisoformat(row["published_at"])
                            if row["published_at"] else None
                        ),
                        language=row["language"] or "zh",
                        market=row["market"] or "china",
                        metadata=(_json.loads(row["metadata"]) if row["metadata"] else {}),
                    )
                    item.id = row["id"]
                    test_items.append((monitor.name, item, "from_db"))
                    print(f"  [{monitor.name}]: DB → {item.title[:60]}")
                else:
                    print(f"  [{monitor.name}]: No items, skipping")
        except Exception as e:
            print(f"  [{monitor.name}]: Error: {e}")

    print(f"\nProcessing {len(test_items)} items (test_mode=True)...")

    for idx, (source_name, item, origin) in enumerate(test_items, 1):
        print(f"\n{'=' * 60}")
        print(f"[{idx}/{len(test_items)}] {item.title[:70]}")
        print(f"  Source: {source_name} | Origin: {origin}")
        print(f"{'=' * 60}")

        try:
            result = await agent.pipeline.process(item, test_mode=True)

            _print_pipeline_result(result)

            # Send debug to Feishu
            print(f"  📤 Sending debug to Feishu...")
            sent = await agent.alerter.send_test_debug(item, result)
            print(f"  Feishu: {'✅ sent' if sent else '⚠️ issues'}")

        except Exception as e:
            print(f"  ❌ Error: {e}")
            traceback.print_exc()

    await agent.shutdown()
    print(f"\n{'=' * 60}")
    print(f"  [TEST MODE] Complete! Processed {len(test_items)} items.")
    print(f"{'=' * 60}")


def _print_pipeline_result(result: dict) -> None:
    """Print pipeline result to console."""
    evaluation = result.get("initial_evaluation")
    filt = result.get("filter")
    analysis = result.get("analysis")
    signal = result.get("signal_score")
    research = result.get("research")
    deep_research = result.get("deep_research")
    final_assessment = result.get("final_assessment")
    stage_reached = result.get("stage", 1)

    # Phase 1
    if evaluation:
        print(f"\n   --- Phase 1: Initial Evaluation ---")
        print(f"   Relevance: {evaluation.relevance_score:.2f}")
        print(f"   May affect market: {evaluation.may_affect_market}")
        print(f"   Reason: {evaluation.reason[:200]}")
        print(f"   Stocks: {evaluation.related_stocks}")
        print(f"   Sectors: {evaluation.related_sectors}")
        queries = evaluation.search_queries or {}
        for cat, qs in queries.items():
            print(f"   Queries [{cat}]: {qs}")
    elif filt:
        print(f"\n   --- Phase 1: Filter ---")
        print(f"   relevant={filt.is_relevant}, score={filt.relevance_score:.2f}")
        print(f"   reason: {filt.reason[:200]}")

    if stage_reached < 2:
        print(f"\n   ⚠️ Stopped at Phase 1 (relevance too low)")
        print(f"   LLM traces: {len(result.get('llm_traces', []))}")
        return

    # Phase 2
    if deep_research:
        print(f"\n   --- Phase 2: Deep Research ---")
        print(f"   Iterations: {deep_research.total_iterations}")
        print(f"   Search results: {len(deep_research.all_search_results)}")
        print(f"   Fetched pages: {len(deep_research.all_fetched_pages)}")
        print(f"   Price tickers: {list(deep_research.price_data.keys())}")
        for cat, cites in deep_research.citations.items():
            print(f"   Citations [{cat}]: {len(cites)}")
        if deep_research.research_summary:
            print(f"   Summary: {deep_research.research_summary[:200]}")

    # Phase 3
    if final_assessment:
        print(f"\n   --- Phase 3: Final Assessment ---")
        print(f"   Sentiment: {final_assessment.sentiment}")
        print(f"   Surprise: {final_assessment.surprise_factor:.2f}")
        print(f"   Impact: {final_assessment.impact_magnitude} | {final_assessment.impact_timeframe}")
        print(f"   Timeliness: {final_assessment.timeliness}")
        print(f"   Confidence: {final_assessment.confidence:.2f}")
        print(f"   Summary: {final_assessment.summary[:200]}")
        if final_assessment.bull_case:
            print(f"   Bull: {final_assessment.bull_case[:150]}")
        if final_assessment.bear_case:
            print(f"   Bear: {final_assessment.bear_case[:150]}")
        if final_assessment.recommended_action:
            print(f"   Action: {final_assessment.recommended_action[:150]}")
    elif analysis:
        print(f"\n   --- Analysis (legacy) ---")
        print(f"   Sentiment: {analysis.sentiment} | Impact: {analysis.impact_magnitude}")
        print(f"   Tickers: {analysis.affected_tickers}")
        print(f"   Summary: {analysis.summary[:200]}")

    if signal:
        print(f"\n   --- Signal ---")
        print(f"   Tier: {signal.tier} | Timeliness: {signal.timeliness}")
        print(f"   Composite: {signal.composite_score:.4f}")

    print(f"\n   Stage: {stage_reached} | Alert: {result.get('alert_level') or 'NONE'}")
    print(f"   LLM traces: {len(result.get('llm_traces', []))}")


async def show_status():
    """Show database statistics."""
    from src.database import Database
    from src.main import load_config

    settings, _ = load_config()
    db_path = settings.get("database", {}).get("path", "data/news_monitor.db")

    db = Database(db_path)
    await db.initialize()

    stats = await db.get_stats()
    print("\n📊 Database Statistics:")
    print(f"   News items:      {stats.get('news_items', 0)}")
    print(f"   Filter results:  {stats.get('filter_results', 0)}")
    print(f"   Analysis results: {stats.get('analysis_results', 0)}")
    print(f"   Research reports: {stats.get('research_reports', 0)}")

    print("\n📰 Recent items:")
    recent = await db.get_recent_news(limit=10)
    for item in recent:
        sentiment = item.get("sentiment") or "—"
        magnitude = item.get("impact_magnitude") or "—"
        title = item.get("title", "")[:60]
        print(f"   [{sentiment:>13}|{magnitude:>8}] {title}")

    await db.close()


async def show_tokens(days: int = 1):
    """Show token usage statistics."""
    from src.database import Database
    from src.main import load_config

    settings, _ = load_config()
    db_path = settings.get("database", {}).get("path", "data/news_monitor.db")

    db = Database(db_path)
    await db.initialize()

    token_stats = await db.get_token_stats(days=days)
    total = token_stats["total"]

    print(f"\n{'═' * 55}")
    print(f"  Token Usage Report (last {days} day{'s' if days > 1 else ''})")
    print(f"{'═' * 55}")
    print(f"  Total API calls:       {total['calls']}")
    print(f"  Prompt tokens:         {total['prompt_tokens']:>12,}")
    print(f"  Completion tokens:     {total['completion_tokens']:>12,}")
    print(f"  Total tokens:          {total['total_tokens']:>12,}")
    print(f"  Total cost (CNY):      ¥{total['cost_cny']:.4f}")

    by_stage = token_stats.get("by_stage", {})
    if by_stage:
        print(f"\n  {'Stage':<20} {'Calls':>6} {'Prompt':>10} {'Completion':>12} {'Cost':>10}")
        print(f"  {'-'*58}")
        for stage, data in by_stage.items():
            print(
                f"  {stage:<20} {data['calls']:>6} "
                f"{data['prompt_tokens']:>10,} "
                f"{data['completion_tokens']:>12,} "
                f"¥{data['cost_cny']:>8.4f}"
            )

    by_model = token_stats.get("by_model", {})
    if by_model:
        print(f"\n  {'Model':<20} {'Calls':>6} {'Tokens':>12} {'Cost':>10}")
        print(f"  {'-'*48}")
        for model, data in by_model.items():
            total_tok = data['prompt_tokens'] + data['completion_tokens']
            print(
                f"  {model:<20} {data['calls']:>6} "
                f"{total_tok:>12,} "
                f"¥{data['cost_cny']:>8.4f}"
            )

    print(f"{'═' * 55}")
    await db.close()


def main():
    parser = argparse.ArgumentParser(
        description="AI Trading Agent — Monitor news for stock market impact",
    )
    parser.add_argument("--test", nargs="?", const="1", default=None,
                        help="Run test: --test (1 source), --test 3, --test all")
    parser.add_argument("--test-mode", action="store_true",
                        help="Test mode: bypass filter, force all phases, full debug to Feishu")
    parser.add_argument("--status", action="store_true", help="Show database stats")
    parser.add_argument("--tokens", action="store_true", help="Show token usage")
    parser.add_argument("--days", type=int, default=1, help="Days for --tokens report")
    args = parser.parse_args()

    if args.test_mode:
        asyncio.run(run_test_mode())
    elif args.test is not None:
        if args.test.lower() == "all":
            count = 0
        else:
            try:
                count = int(args.test)
            except ValueError:
                print(f"Invalid test count: {args.test}")
                sys.exit(1)
        asyncio.run(run_test(test_count=count))
    elif args.status:
        asyncio.run(show_status())
    elif args.tokens:
        asyncio.run(show_tokens(days=args.days))
    else:
        asyncio.run(run_full())


if __name__ == "__main__":
    main()
