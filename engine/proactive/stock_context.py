"""StockContextBuilder — orchestrates all data source plugins concurrently.

Assembles a complete StockSnapshot by running all registered data source
plugins in parallel and merging their results.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from engine.proactive.data_sources.base import DataSourcePlugin, DataSourceResult
from engine.proactive.models import (
    ExternalSearchContext,
    InternalDataContext,
    PortfolioHolding,
    StockBaseline,
    StockSnapshot,
)

logger = logging.getLogger(__name__)


class StockContextBuilder:
    """Orchestrate all data source plugins to build a comprehensive StockSnapshot."""

    def __init__(self, plugins: list[DataSourcePlugin], content_fetcher=None):
        self.plugins = plugins
        self._plugin_map = {p.name: p for p in plugins}
        self._content_fetcher = content_fetcher

    async def build_context(
        self,
        holding: PortfolioHolding,
        baseline: StockBaseline,
    ) -> StockSnapshot:
        """Run all plugins concurrently and merge results into a StockSnapshot."""

        # Run all plugins in parallel
        tasks = [
            self._safe_fetch(plugin, holding, baseline)
            for plugin in self.plugins
        ]
        results = await asyncio.gather(*tasks)

        # Merge results by plugin name
        result_map: dict[str, DataSourceResult] = {}
        for plugin, result in zip(self.plugins, results):
            result_map[plugin.name] = result

        # Build typed contexts
        internal_result = result_map.get("internal_db", DataSourceResult())
        external_result = result_map.get("web_search", DataSourceResult())
        market_result = result_map.get("market_data", DataSourceResult())

        internal_context = InternalDataContext(
            source_items=internal_result.metadata.get("source_items", {}),
            total_count=internal_result.item_count,
            new_count=internal_result.new_item_count,
            sentiment_distribution=internal_result.metadata.get("sentiment_distribution", {}),
            formatted_text=internal_result.formatted_text,
        )

        # Store raw items in search_results["all"] for the time gate
        external_search_results = external_result.metadata.get("search_results", {})
        if external_result.items:
            external_search_results["all"] = external_result.items

        external_context = ExternalSearchContext(
            search_results=external_search_results,
            fetched_pages=[],
            total_results=external_result.item_count,
            formatted_text=external_result.formatted_text,
        )

        return StockSnapshot(
            holding=holding,
            scan_time=datetime.now(timezone.utc),
            internal_context=internal_context,
            external_context=external_context,
            price_data=market_result.formatted_text,
            current_narrative="",  # Filled by LLM triage
        )

    async def _safe_fetch(
        self,
        plugin: DataSourcePlugin,
        holding: PortfolioHolding,
        baseline: StockBaseline,
    ) -> DataSourceResult:
        """Fetch from a plugin with error isolation."""
        try:
            # Pass content_fetcher to web_search plugin for undated item resolution
            kwargs = {}
            if plugin.name == "web_search" and self._content_fetcher:
                kwargs["content_fetcher"] = self._content_fetcher
            return await asyncio.wait_for(
                plugin.fetch(holding, baseline, **kwargs),
                timeout=60,  # 60s timeout per plugin
            )
        except asyncio.TimeoutError:
            logger.warning(
                "Plugin %s timed out for %s", plugin.name, holding.ticker,
            )
            return DataSourceResult(source_name=plugin.name)
        except Exception as e:
            logger.warning(
                "Plugin %s failed for %s: %s", plugin.name, holding.ticker, e,
            )
            return DataSourceResult(source_name=plugin.name)
