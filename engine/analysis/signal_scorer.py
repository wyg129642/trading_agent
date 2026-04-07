"""Signal scoring module — informational signal metrics for analyzed news.

The old multiplicative formula (impact * surprise * novelty * sentiment * routine)
has been replaced with a simpler approach:

  - Stage 1 relevance filter is the sole gate: relevant=True AND score >= threshold.
  - All items passing Stage 1 proceed through all stages and get reported to Feishu.
  - Signal metrics (impact, surprise, sentiment, timeliness) are displayed as
    informational fields in Feishu messages but do NOT suppress news.
  - Timeliness replaces novelty and is determined by LLM in Stage 2.5 based on
    web search results and price data. Three levels:
      * timely:  news is new and no evidence that stock price has reacted
      * medium:  news is new but stock price appears to have already reacted
      * low:     very old news / already widely known

Alert level is determined by impact_magnitude from Stage 2 analysis:
  critical → critical alert
  high     → high alert
  medium   → medium alert
  low      → low alert (still reported)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# Impact magnitude → weight (informational, for display)
IMPACT_WEIGHTS = {
    "critical": 1.0,
    "high": 0.8,
    "medium": 0.5,
    "low": 0.15,
}

# Sentiment → weight (informational, for display)
SENTIMENT_WEIGHTS = {
    "very_bullish": 1.0,
    "bullish": 0.85,
    "neutral": 0.4,
    "bearish": 0.85,
    "very_bearish": 1.0,
}

# Timeliness levels
TIMELINESS_LEVELS = ("timely", "medium", "low")


@dataclass
class SignalScore:
    """Informational signal score for a single analyzed news item.

    These metrics are displayed in Feishu but do NOT gate alerting.
    Alert gating is done solely by Stage 1 relevance filter.
    """
    news_item_id: str
    impact_weight: float
    surprise_factor: float
    timeliness: str           # "timely", "medium", or "low"
    sentiment_weight: float
    routine_penalty: float
    composite_score: float    # kept for backward compat (informational only)
    tier: str                 # based on impact_magnitude, not composite score


def score_signal(
    news_item_id: str,
    sentiment: str,
    impact_magnitude: str,
    surprise_factor: float,
    is_routine: bool,
    timeliness: str = "timely",
) -> SignalScore:
    """Compute informational signal metrics and assign tier based on impact_magnitude.

    Tier is now based directly on impact_magnitude (no multiplicative suppression):
      critical → "critical"
      high     → "high"
      medium   → "medium"
      low      → "low"
    """
    impact_w = IMPACT_WEIGHTS.get(impact_magnitude, 0.15)
    sentiment_w = SENTIMENT_WEIGHTS.get(sentiment, 0.4)
    surprise = max(0.0, min(1.0, surprise_factor))
    routine_pen = 0.5 if is_routine else 1.0

    # Informational composite score (for display only, does not gate anything)
    composite = impact_w * surprise * sentiment_w * routine_pen
    composite = round(composite, 4)

    # Tier is directly from impact_magnitude
    tier = impact_magnitude if impact_magnitude in ("critical", "high", "medium", "low") else "low"

    return SignalScore(
        news_item_id=news_item_id,
        impact_weight=impact_w,
        surprise_factor=surprise,
        timeliness=timeliness,
        sentiment_weight=sentiment_w,
        routine_penalty=routine_pen,
        composite_score=composite,
        tier=tier,
    )
