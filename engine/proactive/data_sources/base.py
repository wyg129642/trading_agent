"""Abstract base for pluggable data source plugins.

The plugin architecture makes it trivial to add new data sources
(social media, forums, new APIs) in the future — just implement
DataSourcePlugin and register it with StockContextBuilder.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

from engine.proactive.models import PortfolioHolding, StockBaseline


@dataclass
class DataSourceResult:
    """Standardized result from any data source plugin."""

    source_name: str = ""
    items: list[dict] = field(default_factory=list)
    formatted_text: str = ""
    item_count: int = 0
    new_item_count: int = 0  # Items not seen in baseline
    metadata: dict[str, Any] = field(default_factory=dict)


class DataSourcePlugin(ABC):
    """Abstract base for all data sources — internal, external, market data.

    Each plugin knows how to:
    1. Fetch data relevant to a given stock
    2. Format that data as text for LLM consumption
    """

    name: str = "base"

    @abstractmethod
    async def fetch(
        self,
        holding: PortfolioHolding,
        baseline: StockBaseline,
        **kwargs,
    ) -> DataSourceResult:
        """Fetch data for a stock. Returns structured results."""
        ...

    @abstractmethod
    def format_for_llm(self, result: DataSourceResult) -> str:
        """Format results into text for LLM consumption."""
        ...
