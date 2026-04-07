"""API endpoints for Topic Radar — hot news, topic clusters, and trending alerts."""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone, date

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select, func, desc, text, cast, Date
from sqlalchemy.ext.asyncio import AsyncSession

from backend.app.core.database import async_session_factory
from backend.app.models.alphapai import AlphaPaiArticle, AlphaPaiComment
from backend.app.models.jiuqian import JiuqianForum, JiuqianMinutes, JiuqianWechat
from backend.app.models.topic_cluster import TopicClusterResult

logger = logging.getLogger(__name__)
router = APIRouter()

# Market-relevant keywords for filtering hot news titles
_MARKET_KEYWORDS = {
    # Market/Index
    "股", "A股", "港股", "美股", "上证", "深证", "创业板", "科创板", "恒生", "纳斯达克",
    "标普", "道琼斯", "大盘", "指数", "涨停", "跌停", "行情", "牛市", "熊市",
    # Trading
    "基金", "ETF", "期货", "期权", "债券", "国债", "利率", "汇率", "外汇",
    "交易", "券商", "证监", "银保监", "央行", "降息", "加息", "降准", "MLF", "LPR",
    # Sectors
    "芯片", "半导体", "光伏", "新能源", "锂电", "储能", "AI", "人工智能", "算力",
    "光模块", "光通信", "机器人", "无人机", "量子", "5G", "6G",
    "医药", "医疗", "生物", "疫苗", "创新药",
    "石油", "原油", "天然气", "煤炭", "有色", "黄金", "白银", "铜", "钢铁",
    "汽车", "电车", "新势力", "充电桩",
    "房地产", "地产", "楼市",
    "消费", "白酒", "零售", "电商",
    "军工", "国防", "航天", "航空",
    # Macro/Policy
    "GDP", "CPI", "PMI", "PPI", "出口", "进口", "贸易", "关税", "制裁", "出口管制",
    "财政", "货币", "通胀", "衰退", "经济",
    "美联储", "欧央行", "日央行",
    # Companies
    "特斯拉", "英伟达", "NVIDIA", "苹果", "谷歌", "微软", "台积电", "三星",
    "比亚迪", "宁德时代", "腾讯", "阿里", "华为", "小米", "字节",
    "茅台", "中石油", "中石化",
    # Events
    "IPO", "上市", "退市", "收购", "并购", "重组", "增发", "回购", "减持", "增持",
    "分红", "业绩", "财报", "营收", "净利", "暴雷", "违约", "爆仓",
    "停牌", "复牌", "ST",
    # Geopolitical
    "霍尔木兹", "台海", "中美", "俄乌", "伊朗", "朝鲜",
}


def _is_market_relevant(title: str) -> bool:
    """Check if a hot news title is market-relevant using keyword matching."""
    for kw in _MARKET_KEYWORDS:
        if kw in title:
            return True
    return False


def _safe_enrichment(val) -> dict:
    """Ensure enrichment is a dict (sometimes stored as JSON string)."""
    if isinstance(val, dict):
        return val
    if isinstance(val, str) and val:
        import json as _json
        try:
            return _json.loads(val)
        except (ValueError, _json.JSONDecodeError):
            pass
    return {}


async def _get_db():
    async with async_session_factory() as db:
        yield db


@router.get("/overview")
async def topic_radar_overview(db: AsyncSession = Depends(_get_db)):
    """Main overview: stats cards, latest hot news, recent cluster results."""
    now = datetime.now(timezone.utc)
    cutoff_24h = now - timedelta(hours=24)

    # 1. Fetch top-ranked hot news (radar_top = true, sorted by score desc)
    all_hot_result = await db.execute(text("""
        SELECT * FROM (
            SELECT DISTINCT ON (n.title)
                   n.id, n.title, n.source_name, n.url, n.fetched_at, n.published_at,
                   a.sentiment, a.impact_magnitude, a.affected_tickers,
                   COALESCE((n.metadata->>'radar_score')::int, 0) as score
            FROM news_items n
            LEFT JOIN analysis_results a ON a.news_item_id = n.id
            WHERE n.source_name IN ('华尔街见闻热点', '财联社热点', '雪球热榜', '微博热搜')
              AND n.fetched_at >= :cutoff
              AND (n.metadata->>'radar_top')::boolean = true
              AND length(n.title) >= 8
            ORDER BY n.title, n.fetched_at DESC
        ) deduped
        ORDER BY deduped.score DESC, deduped.fetched_at DESC
        LIMIT 10
    """), {"cutoff": cutoff_24h})

    hot_news_items = []
    hot_news_counts: dict[str, int] = {}
    for row in all_hot_result.fetchall():
        source = row[2] or ""
        hot_news_counts[source] = hot_news_counts.get(source, 0) + 1
        hot_news_items.append({
            "id": row[0],
            "title": row[1] or "",
            "source_name": source,
            "url": row[3],
            "fetched_at": row[4].isoformat() if row[4] else None,
            "published_at": row[5].isoformat() if row[5] else None,
            "sentiment": row[6],
            "impact": row[7],
            "tickers": row[8] if row[8] else [],
            "radar_score": row[9] if len(row) > 9 else 0,
        })

    # Count pending (not yet evaluated by LLM)
    pending_result = await db.execute(text("""
        SELECT count(*) FROM news_items
        WHERE source_name IN ('华尔街见闻热点', '财联社热点', '雪球热榜', '微博热搜')
          AND fetched_at >= :cutoff
          AND (metadata IS NULL OR metadata->>'llm_relevant' IS NULL)
    """), {"cutoff": cutoff_24h})
    pending_count = pending_result.scalar() or 0

    # 3. Enrichment sentiment stats (last 24h from AlphaPai/Jiuqian)
    enrichment_stats = {"bullish": 0, "bearish": 0, "neutral": 0}
    for Model, time_col in [
        (AlphaPaiArticle, AlphaPaiArticle.publish_time),
        (AlphaPaiComment, AlphaPaiComment.cmnt_date),
        (JiuqianMinutes, JiuqianMinutes.pub_time),
        (JiuqianWechat, JiuqianWechat.pub_time),
    ]:
        try:
            rows = (await db.execute(
                select(Model).where(Model.is_enriched.is_(True)).where(time_col >= cutoff_24h)
            )).scalars().all()
            for r in rows:
                enrichment = _safe_enrichment(r.enrichment)
                if enrichment.get("skipped"):
                    continue
                sentiment = enrichment.get("sentiment", "")
                if sentiment in ("bullish", "very_bullish"):
                    enrichment_stats["bullish"] += 1
                elif sentiment in ("bearish", "very_bearish"):
                    enrichment_stats["bearish"] += 1
                elif sentiment == "neutral":
                    enrichment_stats["neutral"] += 1
        except Exception:
            pass

    # 4. Latest cluster results (last 3 days)
    cluster_results = (await db.execute(
        select(TopicClusterResult)
        .order_by(desc(TopicClusterResult.run_time))
        .limit(10)
    )).scalars().all()

    clusters_data = []
    for c in cluster_results:
        clusters_data.append({
            "id": c.id,
            "cluster_date": c.cluster_date.isoformat() if c.cluster_date else None,
            "run_time": c.run_time.isoformat() if c.run_time else None,
            "total_items": c.total_items,
            "n_clusters": c.n_clusters,
            "anomalies": c.anomalies or [],
            "top_clusters": c.top_clusters or [],
            "summary": c.summary or "",
        })

    return {
        "hot_news_counts": hot_news_counts,
        "hot_news_items": hot_news_items,
        "pending_filter_count": pending_count,
        "enrichment_stats": enrichment_stats,
        "cluster_results": clusters_data,
    }


@router.get("/hot-news")
async def hot_news_feed(
    source: str = Query(None, description="Filter by source name"),
    limit: int = Query(100, le=500),
    hours: int = Query(24, le=168),
    db: AsyncSession = Depends(_get_db),
):
    """Hot news feed with optional source filter."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)

    query = """
        SELECT id, title, source_name, url, fetched_at, published_at
        FROM news_items
        WHERE source_name IN ('华尔街见闻热点', '财联社热点', '雪球热榜', '微博热搜')
          AND fetched_at >= :cutoff
    """
    params: dict = {"cutoff": cutoff}
    if source:
        query += " AND source_name = :source"
        params["source"] = source
    query += " ORDER BY fetched_at DESC LIMIT :limit"
    params["limit"] = limit

    result = await db.execute(text(query), params)
    items = []
    for row in result.fetchall():
        items.append({
            "id": row[0],
            "title": row[1],
            "source_name": row[2],
            "url": row[3],
            "fetched_at": row[4].isoformat() if row[4] else None,
            "published_at": row[5].isoformat() if row[5] else None,
        })

    return {"items": items, "total": len(items)}


@router.get("/top-tickers")
async def top_tickers(
    hours: int = Query(24, le=168),
    db: AsyncSession = Depends(_get_db),
):
    """Top mentioned tickers across all enriched data in the time window."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)

    ticker_counts: dict[str, dict] = {}  # ticker_code -> {name, code, count, sentiments}

    for Model, time_col in [
        (AlphaPaiArticle, AlphaPaiArticle.publish_time),
        (AlphaPaiComment, AlphaPaiComment.cmnt_date),
        (JiuqianMinutes, JiuqianMinutes.pub_time),
        (JiuqianWechat, JiuqianWechat.pub_time),
    ]:
        try:
            rows = (await db.execute(
                select(Model).where(Model.is_enriched.is_(True)).where(time_col >= cutoff)
            )).scalars().all()
            for r in rows:
                enrichment = _safe_enrichment(r.enrichment)
                if enrichment.get("skipped"):
                    continue
                sentiment = enrichment.get("sentiment", "neutral")
                for t in enrichment.get("tickers", []):
                    # Handle both dict format {name, code} and string format "名称(代码)"
                    if isinstance(t, dict):
                        code = t.get("code", "") or t.get("ticker", "")
                        name = t.get("name", "")
                    elif isinstance(t, str):
                        # Parse "恒瑞医药(600276.SH)" format
                        import re
                        m = re.match(r'^(.+?)\((.+?)\)$', t)
                        if m:
                            name, code = m.group(1), m.group(2)
                        else:
                            name, code = t, t
                    else:
                        continue
                    if not code:
                        continue
                    if code not in ticker_counts:
                        ticker_counts[code] = {"name": name, "code": code, "count": 0, "bullish": 0, "bearish": 0, "neutral": 0}
                    ticker_counts[code]["count"] += 1
                    if sentiment in ("bullish", "very_bullish"):
                        ticker_counts[code]["bullish"] += 1
                    elif sentiment in ("bearish", "very_bearish"):
                        ticker_counts[code]["bearish"] += 1
                    else:
                        ticker_counts[code]["neutral"] += 1
        except Exception:
            pass

    sorted_tickers = sorted(ticker_counts.values(), key=lambda x: x["count"], reverse=True)
    return {"tickers": sorted_tickers[:30], "total": len(ticker_counts)}
