#!/usr/bin/env python3
"""Reset is_enriched for articles mentioning portfolio stocks not in standard lists.

This covers Japanese (日股), Korean (韩股), and other non-standard stocks
whose tickers were previously dropped by the verifier during enrichment.
After running this, the enrichment pipeline will re-process these articles
with the updated verifier that now recognizes portfolio stocks.

Usage:
    cd /home/ygwang/trading_agent
    python -m scripts.reenrich_portfolio_stocks [--dry-run]
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

import yaml
from sqlalchemy import select, update, func, or_, text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

from backend.app.config import get_settings
from backend.app.models.alphapai import AlphaPaiArticle, AlphaPaiComment, AlphaPaiRoadshowCN
from backend.app.models.jiuqian import JiuqianForum, JiuqianMinutes, JiuqianWechat
from backend.app.services.stock_verifier import get_stock_verifier

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _get_custom_stock_names() -> list[str]:
    """Get stock names from portfolio that aren't in standard stock lists."""
    verifier = get_stock_verifier()

    portfolio_yaml = Path(__file__).resolve().parent.parent / "config" / "portfolio_sources.yaml"
    with open(portfolio_yaml, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    names = set()
    for s in data.get("sources", []):
        ticker = s.get("stock_ticker", "").strip()
        name = s.get("stock_name", "").strip()
        if not ticker or not name:
            continue
        # Only include stocks that are in the custom list (not in standard A/US/HK lists)
        if ticker in verifier._custom_code_to_name:
            names.add(name)
    return list(names)


async def main(dry_run: bool = False):
    settings = get_settings()
    engine = create_async_engine(settings.database_url, echo=False)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    stock_names = _get_custom_stock_names()
    if not stock_names:
        logger.info("No custom portfolio stocks found — nothing to do.")
        return

    logger.info("Custom portfolio stocks to re-enrich: %s", ", ".join(stock_names))

    # Tables and their title columns
    tables = [
        (AlphaPaiArticle, AlphaPaiArticle.arc_name, "alphapai_articles"),
        (AlphaPaiComment, AlphaPaiComment.title, "alphapai_comments"),
        (AlphaPaiRoadshowCN, AlphaPaiRoadshowCN.show_title, "alphapai_roadshows_cn"),
        (JiuqianForum, JiuqianForum.title, "jiuqian_forum"),
        (JiuqianMinutes, JiuqianMinutes.title, "jiuqian_minutes"),
        (JiuqianWechat, JiuqianWechat.title, "jiuqian_wechat"),
    ]

    total_reset = 0

    async with async_session() as db:
        for model, title_col, table_name in tables:
            # Find enriched items whose title mentions any custom stock name
            conditions = [title_col.ilike(f"%{name}%") for name in stock_names]
            stmt = (
                select(func.count())
                .select_from(model)
                .where(model.is_enriched == True)  # noqa
                .where(or_(*conditions))
            )
            count = (await db.execute(stmt)).scalar() or 0

            if count == 0:
                logger.info("[%s] No enriched items mention custom stocks — skipping", table_name)
                continue

            logger.info("[%s] Found %d enriched items mentioning custom stocks", table_name, count)

            if not dry_run:
                update_stmt = (
                    update(model)
                    .where(model.is_enriched == True)  # noqa
                    .where(or_(*conditions))
                    .values(is_enriched=False)
                )
                result = await db.execute(update_stmt)
                logger.info("[%s] Reset is_enriched for %d items", table_name, result.rowcount)
                total_reset += result.rowcount

        if not dry_run:
            await db.commit()
            logger.info("Total reset: %d items across all tables", total_reset)
            logger.info("These items will be re-enriched in the next enrichment cycle.")
        else:
            logger.info("[DRY-RUN] Would reset %d items total (no changes made)", total_reset or count)

    await engine.dispose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Re-enrich articles for portfolio stocks not in standard lists")
    parser.add_argument("--dry-run", action="store_true", help="Show counts without modifying DB")
    args = parser.parse_args()
    asyncio.run(main(dry_run=args.dry_run))
