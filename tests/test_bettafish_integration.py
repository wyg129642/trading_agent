"""End-to-end tests for BettaFish integration (Phase 1, 2, 3).

Run: python tests/test_bettafish_integration.py
"""
from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from datetime import datetime

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


# ============================================================
# Phase 1: HotNewsMonitor — test API fetch
# ============================================================
async def test_hotnews_monitor():
    """Test that HotNewsMonitor can fetch real items from the newsnow API."""
    import aiohttp
    from engine.monitors.hotnews_monitor import HotNewsMonitor

    config = {
        "name": "Test-华尔街见闻",
        "type": "api",
        "api_type": "hotnews",
        "api_source_id": "wallstreetcn",
        "priority": "p1",
        "enabled": True,
        "market": "china",
    }

    async with aiohttp.ClientSession() as session:
        monitor = HotNewsMonitor(config, session)
        items = await monitor.fetch_items()

        print(f"\n[Phase 1] HotNewsMonitor test (wallstreetcn)")
        print(f"  Items fetched: {len(items)}")
        assert len(items) > 0, "Expected at least 1 item from wallstreetcn"

        item = items[0]
        print(f"  First item: {item.title[:60]}")
        print(f"  URL: {item.url[:80]}")
        print(f"  source_name: {item.source_name}")
        print(f"  language: {item.language}")
        print(f"  market: {item.market}")
        print(f"  content_hash: {item.content_hash}")
        print(f"  metadata: {item.metadata}")
        assert item.source_name == "Test-华尔街见闻"
        assert item.language == "zh"
        assert item.market == "china"
        assert item.metadata.get("source_id") == "wallstreetcn"
        assert item.content_hash  # Non-empty hash

    # Test other sources
    for source_id in ["cls-hot", "xueqiu", "weibo"]:
        config["api_source_id"] = source_id
        config["name"] = f"Test-{source_id}"
        async with aiohttp.ClientSession() as session:
            monitor = HotNewsMonitor(config, session)
            items = await monitor.fetch_items()
            print(f"  {source_id}: {len(items)} items")
            assert len(items) > 0, f"Expected items from {source_id}"

    # Test dedup via poll()
    config["api_source_id"] = "cls-hot"
    config["name"] = "Test-dedup"
    async with aiohttp.ClientSession() as session:
        monitor = HotNewsMonitor(config, session)
        items1 = await monitor.poll()
        items2 = await monitor.poll()
        print(f"  Dedup test: poll1={len(items1)}, poll2={len(items2)} (should be 0)")
        assert len(items2) == 0, "Second poll should return 0 items (all deduped)"

    print("  ✓ Phase 1 PASSED")


# ============================================================
# Phase 2: TrendingDetector — test matching logic
# ============================================================
async def test_trending_detector():
    """Test that TrendingDetector correctly matches portfolio holdings."""
    from engine.analysis.trending_detector import TrendingDetector
    from engine.models import NewsItem

    # Simulate portfolio sources
    portfolio_sources = [
        {"group": "portfolio", "stock_name": "中际旭创", "stock_ticker": "300308", "stock_market": "创业板"},
        {"group": "portfolio", "stock_name": "康宁", "stock_ticker": "GLW", "stock_market": "美股"},
        {"group": "portfolio", "stock_name": "天孚通信", "stock_ticker": "300394", "stock_market": "创业板"},
    ]

    companies = {
        "us": [{"name": "NVIDIA", "ticker": "NVDA"}],
        "china": [{"name": "寒武纪 (Cambricon)", "ticker": "688256", "exchange": "SSE"}],
        "private": [{"name": "OpenAI"}],
    }

    detector = TrendingDetector(portfolio_sources, companies)

    print(f"\n[Phase 2] TrendingDetector test")
    print(f"  Keywords registered: {len(detector.keywords)}")
    assert len(detector.keywords) > 0

    # Test 1: Match from social source
    item_match = NewsItem(
        source_name="雪球热榜",
        title="中际旭创发布最新季报，营收大幅增长",
        url="https://example.com/1",
        metadata={"source_id": "xueqiu"},
    )
    matches = detector.check_item(item_match)
    print(f"  Test 'xueqiu + 中际旭创': {len(matches)} match(es)")
    assert len(matches) >= 1, "Should match 中际旭创"
    assert matches[0]["ticker"] == "300308"

    # Test 2: No match from non-social source
    item_no_social = NewsItem(
        source_name="SEC EDGAR",
        title="中际旭创 in SEC filing",
        url="https://example.com/2",
        metadata={"source_id": "sec_edgar"},
    )
    matches = detector.check_item(item_no_social)
    print(f"  Test 'SEC + 中际旭创': {len(matches)} match(es) (should be 0)")
    assert len(matches) == 0, "Non-social source should not trigger"

    # Test 3: Match from companies section
    item_nvidia = NewsItem(
        source_name="微博热搜",
        title="NVIDIA发布新一代GPU芯片",
        url="https://example.com/3",
        metadata={"source_id": "weibo"},
    )
    matches = detector.check_item(item_nvidia)
    print(f"  Test 'weibo + NVIDIA': {len(matches)} match(es)")
    assert len(matches) >= 1, "Should match NVIDIA"

    # Test 4: Match Chinese name from companies (寒武纪)
    item_cambricon = NewsItem(
        source_name="华尔街见闻热点",
        title="寒武纪股价大涨20%",
        url="https://example.com/4",
        metadata={"source_id": "wallstreetcn"},
    )
    matches = detector.check_item(item_cambricon)
    print(f"  Test 'wallstreetcn + 寒武纪': {len(matches)} match(es)")
    assert len(matches) >= 1, "Should match 寒武纪"

    # Test 5: No match for unrelated title
    item_no_match = NewsItem(
        source_name="微博热搜",
        title="今天天气真好啊",
        url="https://example.com/5",
        metadata={"source_id": "weibo"},
    )
    matches = detector.check_item(item_no_match)
    print(f"  Test 'weibo + unrelated': {len(matches)} match(es) (should be 0)")
    assert len(matches) == 0, "Unrelated title should not match"

    # Test 6: Daily dedup
    detector._alerted_today.add("300308:雪球热榜")
    # Simulate alert — should skip because already alerted
    class FakeAlerter:
        def __init__(self):
            self.calls = 0
        async def send_system_alert(self, msg):
            self.calls += 1
    alerter = FakeAlerter()
    await detector.alert_if_trending(item_match, [{"name": "中际旭创", "ticker": "300308", "market": "创业板"}], alerter)
    print(f"  Test dedup: alerter calls={alerter.calls} (should be 0)")
    assert alerter.calls == 0, "Should be deduped"

    print("  ✓ Phase 2 PASSED")


# ============================================================
# Phase 3: TopicClusterService — test clustering logic
# ============================================================
async def test_topic_cluster():
    """Test the clustering logic with synthetic data."""
    from sklearn.cluster import KMeans
    from sentence_transformers import SentenceTransformer

    print(f"\n[Phase 3] TopicClusterService test")

    # Test 1: Model loads correctly
    print("  Loading SentenceTransformer model...")
    model = SentenceTransformer("paraphrase-multilingual-MiniLM-L12-v2")
    print("  ✓ Model loaded")

    # Test 2: Encoding works
    texts = [
        "中际旭创800G光模块出货量大增",
        "光模块行业景气度持续提升",
        "中际旭创与英伟达达成合作协议",
        "人工智能芯片需求推动光通信板块上涨",
        "寒武纪发布新一代AI芯片",
        "今天天气真好",
        "股市大盘走势分析",
        "美联储利率决议即将公布",
        "半导体行业迎来新一轮增长",
        "光通信产业链全面梳理",
    ]
    embeddings = model.encode(texts, show_progress_bar=False)
    print(f"  Encoded {len(texts)} texts → shape {embeddings.shape}")
    assert embeddings.shape[0] == len(texts)
    assert embeddings.shape[1] > 0

    # Test 3: KMeans clustering works
    n_clusters = 3
    kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
    labels = kmeans.fit_predict(embeddings)
    print(f"  KMeans: {n_clusters} clusters, labels={list(labels)}")
    assert len(labels) == len(texts)
    assert len(set(labels)) <= n_clusters

    # Test 4: Anomaly detection logic
    from collections import Counter
    cluster_sizes = Counter(int(l) for l in labels)
    total = len(texts)
    avg_size = total / n_clusters
    print(f"  Cluster sizes: {dict(cluster_sizes)}, avg={avg_size:.1f}")

    # Simulate anomaly with skewed data (many similar texts)
    anomaly_texts = ["中际旭创光模块技术突破"] * 20 + [
        "今天天气很好",
        "美联储加息",
        "A股大盘走势",
    ]
    anomaly_embeddings = model.encode(anomaly_texts, show_progress_bar=False)
    kmeans2 = KMeans(n_clusters=3, random_state=42, n_init=10)
    labels2 = kmeans2.fit_predict(anomaly_embeddings)
    sizes2 = Counter(int(l) for l in labels2)
    max_cluster_size = max(sizes2.values())
    avg2 = len(anomaly_texts) / 3
    print(f"  Anomaly test: sizes={dict(sizes2)}, max={max_cluster_size}, avg={avg2:.1f}")
    assert max_cluster_size > avg2 * 2, "Dominant cluster should be >2x average"

    # Test 5: Import the service module successfully
    from backend.app.services.topic_cluster import TopicClusterService
    print("  ✓ TopicClusterService import OK")

    # Test 6: Model import
    from backend.app.models.topic_cluster import TopicClusterResult
    print("  ✓ TopicClusterResult model import OK")

    print("  ✓ Phase 3 PASSED")


# ============================================================
# Run all tests
# ============================================================
async def main():
    print("=" * 60)
    print("BettaFish Integration — End-to-End Tests")
    print("=" * 60)

    await test_hotnews_monitor()
    await test_trending_detector()
    await test_topic_cluster()

    print("\n" + "=" * 60)
    print("ALL TESTS PASSED ✓")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
