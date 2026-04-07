"""Token usage tracking, cost estimation, and rate limit monitoring.

Two modes of counting:
  1. Pre-call estimation: tiktoken (cl100k_base approximation) to check context limits
  2. Post-call actual:    API response `usage` field for accurate billing

Persists to SQLite for historical analysis.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

logger = logging.getLogger(__name__)

# ── Pre-call estimation via tiktoken ─────────────────────────────────
# MiniMax uses its own tokenizer but tiktoken cl100k_base is a reasonable
# approximation (~5-15 % error) for cost/limit estimation purposes.

_ENCODER = None


def _get_encoder():
    """Lazy-load tiktoken encoder (import is slow, only do it once)."""
    global _ENCODER
    if _ENCODER is None:
        try:
            import tiktoken
            _ENCODER = tiktoken.get_encoding("cl100k_base")
        except ImportError:
            logger.warning(
                "tiktoken not installed — pre-call token estimation disabled. "
                "Install with: pip install tiktoken"
            )
            _ENCODER = False  # sentinel: tried and failed
    return _ENCODER if _ENCODER else None


def estimate_tokens(text: str) -> int:
    """Estimate token count for a string using tiktoken cl100k_base.

    Returns 0 if tiktoken is not available (falls back to char heuristic).
    """
    enc = _get_encoder()
    if enc:
        return len(enc.encode(text))
    # Rough heuristic: ~1.5 tokens per Chinese character, ~0.75 per English word
    cn_chars = sum(1 for c in text if "\u4e00" <= c <= "\u9fff")
    en_words = len(text.split()) - cn_chars  # approximate
    return int(cn_chars * 1.5 + max(en_words, 0) * 1.3)


def estimate_messages_tokens(messages: list[dict]) -> int:
    """Estimate token count for a list of chat messages.

    Accounts for message formatting overhead (~4 tokens per message).
    """
    total = 0
    for msg in messages:
        total += 4  # role, content markers
        total += estimate_tokens(msg.get("content", ""))
    total += 2  # assistant reply priming
    return total


# ── Pricing configuration ────────────────────────────────────────────

@dataclass
class ModelPricing:
    """Cost per 1M tokens in CNY (Chinese Yuan)."""
    input_per_million: float
    output_per_million: float


# Pricing for all supported providers (CNY per 1M tokens)
DEFAULT_PRICING: dict[str, ModelPricing] = {
    # --- Zhipu GLM series ---
    "glm-4-flash": ModelPricing(input_per_million=0.0, output_per_million=0.0),   # free tier
    "glm-4-flash-250414": ModelPricing(input_per_million=0.0, output_per_million=0.0),
    "glm-4-air": ModelPricing(input_per_million=0.5, output_per_million=0.5),
    "glm-4-airx": ModelPricing(input_per_million=5.0, output_per_million=5.0),
    "glm-4-plus": ModelPricing(input_per_million=50.0, output_per_million=50.0),
    "glm-4-long": ModelPricing(input_per_million=1.0, output_per_million=1.0),
    "glm-4": ModelPricing(input_per_million=15.0, output_per_million=15.0),
    # --- MiniMax series (CNY pricing) ---
    "MiniMax-Text-01": ModelPricing(input_per_million=1.0, output_per_million=10.0),
    "MiniMax-M2": ModelPricing(input_per_million=1.0, output_per_million=10.0),
    "abab6.5s-chat": ModelPricing(input_per_million=1.0, output_per_million=1.0),
    "abab6.5-chat": ModelPricing(input_per_million=3.0, output_per_million=3.0),
    "abab7-chat": ModelPricing(input_per_million=5.0, output_per_million=5.0),
    # --- OpenRouter models (converted to CNY at ~7.2 CNY/USD) ---
    # Anthropic via OpenRouter
    "anthropic/claude-sonnet-4": ModelPricing(input_per_million=21.6, output_per_million=108.0),
    "anthropic/claude-haiku-4": ModelPricing(input_per_million=5.76, output_per_million=28.8),
    # Google via OpenRouter
    "google/gemini-2.0-flash-001": ModelPricing(input_per_million=0.72, output_per_million=2.88),
    "google/gemini-2.5-pro-preview": ModelPricing(input_per_million=9.0, output_per_million=36.0),
    # Meta via OpenRouter
    "meta-llama/llama-4-maverick": ModelPricing(input_per_million=1.44, output_per_million=5.76),
    # DeepSeek via OpenRouter
    "deepseek/deepseek-chat-v3-0324": ModelPricing(input_per_million=1.44, output_per_million=5.76),
    # Fallback for unknown models
    "_default": ModelPricing(input_per_million=1.0, output_per_million=5.0),
}


# ── Usage record ─────────────────────────────────────────────────────

@dataclass
class UsageRecord:
    """A single API call's token usage."""
    timestamp: datetime
    model: str
    stage: str            # "filter", "analyzer", "researcher", "other"
    prompt_tokens: int
    completion_tokens: int
    estimated_prompt: int  # pre-call tiktoken estimate (0 if skipped)
    source_name: str = ""  # which news source triggered this
    duration_ms: int = 0   # API call latency

    @property
    def total_tokens(self) -> int:
        return self.prompt_tokens + self.completion_tokens

    def cost_cny(self, pricing: dict[str, ModelPricing] | None = None) -> float:
        """Calculate cost in CNY."""
        p = (pricing or DEFAULT_PRICING).get(
            self.model, DEFAULT_PRICING.get("_default", ModelPricing(1.0, 10.0))
        )
        return (
            self.prompt_tokens * p.input_per_million / 1_000_000
            + self.completion_tokens * p.output_per_million / 1_000_000
        )


# ── Token Tracker ────────────────────────────────────────────────────

class TokenTracker:
    """Central token usage tracker with rate limit monitoring.

    Tracks:
      - Per-call usage (stored in memory + persisted to DB)
      - Rolling window for rate limits (tokens per minute)
      - Cumulative costs per stage, per model, per day
      - Budget alerts
    """

    def __init__(
        self,
        daily_budget_cny: float = 50.0,
        rate_limit_tpm: int = 1_000_000,
        pricing: dict[str, ModelPricing] | None = None,
    ):
        self.daily_budget_cny = daily_budget_cny
        self.rate_limit_tpm = rate_limit_tpm  # tokens per minute
        self.pricing = pricing or DEFAULT_PRICING

        # In-memory storage
        self._records: list[UsageRecord] = []
        self._persisted_count: int = 0  # index into _records: records[:this] already saved to DB
        self._lock = asyncio.Lock()

        # Rolling window for rate limiting (last 60 seconds)
        self._recent_tokens: list[tuple[float, int]] = []  # (timestamp, tokens)

        # Cumulative counters (reset daily)
        self._daily_reset_date: str = datetime.now().strftime("%Y-%m-%d")
        self._daily_totals: dict[str, int] = defaultdict(int)  # stage -> tokens

    async def record(
        self,
        model: str,
        stage: str,
        prompt_tokens: int,
        completion_tokens: int,
        estimated_prompt: int = 0,
        source_name: str = "",
        duration_ms: int = 0,
    ) -> UsageRecord:
        """Record a completed API call's token usage."""
        rec = UsageRecord(
            timestamp=datetime.now(),
            model=model,
            stage=stage,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            estimated_prompt=estimated_prompt,
            source_name=source_name,
            duration_ms=duration_ms,
        )

        async with self._lock:
            self._records.append(rec)

            # Update rolling window
            now = time.monotonic()
            self._recent_tokens.append((now, rec.total_tokens))
            # Prune entries older than 60 seconds
            cutoff = now - 60
            self._recent_tokens = [(t, n) for t, n in self._recent_tokens if t > cutoff]

            # Update daily totals
            today = datetime.now().strftime("%Y-%m-%d")
            if today != self._daily_reset_date:
                self._daily_totals.clear()
                self._daily_reset_date = today
            self._daily_totals[stage] += rec.total_tokens
            self._daily_totals["_all"] += rec.total_tokens

        cost = rec.cost_cny(self.pricing)
        logger.debug(
            "[Tokens] %s/%s: prompt=%d, completion=%d, total=%d, cost=¥%.4f",
            stage, model, prompt_tokens, completion_tokens, rec.total_tokens, cost,
        )
        return rec

    # ── Rate limit checks ─────────────────────────────────────────

    def tokens_last_minute(self) -> int:
        """Total tokens consumed in the last 60 seconds."""
        cutoff = time.monotonic() - 60
        return sum(n for t, n in self._recent_tokens if t > cutoff)

    def rate_limit_remaining(self) -> int:
        """How many tokens we can still use this minute."""
        return max(0, self.rate_limit_tpm - self.tokens_last_minute())

    def is_rate_limited(self) -> bool:
        """True if we've hit the rate limit."""
        return self.tokens_last_minute() >= self.rate_limit_tpm

    async def wait_for_rate_limit(self):
        """Sleep until we're below the rate limit."""
        while self.is_rate_limited():
            logger.warning("[Tokens] Rate limited — waiting 5s (used %d/%d TPM)",
                           self.tokens_last_minute(), self.rate_limit_tpm)
            await asyncio.sleep(5)

    # ── Budget tracking ───────────────────────────────────────────

    def daily_cost_cny(self) -> float:
        """Total cost today in CNY."""
        today = datetime.now().strftime("%Y-%m-%d")
        return sum(
            r.cost_cny(self.pricing)
            for r in self._records
            if r.timestamp.strftime("%Y-%m-%d") == today
        )

    def daily_budget_remaining_cny(self) -> float:
        return max(0, self.daily_budget_cny - self.daily_cost_cny())

    def is_over_budget(self) -> bool:
        return self.daily_cost_cny() >= self.daily_budget_cny

    # ── Reporting ─────────────────────────────────────────────────

    def summary(self, hours: int = 24) -> dict[str, Any]:
        """Generate a usage summary for the last N hours."""
        cutoff = datetime.now() - timedelta(hours=hours)
        recent = [r for r in self._records if r.timestamp > cutoff]

        by_stage: dict[str, dict] = defaultdict(lambda: {
            "calls": 0, "prompt_tokens": 0, "completion_tokens": 0, "cost_cny": 0.0
        })

        total_prompt = 0
        total_completion = 0
        total_cost = 0.0

        for r in recent:
            s = by_stage[r.stage]
            s["calls"] += 1
            s["prompt_tokens"] += r.prompt_tokens
            s["completion_tokens"] += r.completion_tokens
            s["cost_cny"] += r.cost_cny(self.pricing)
            total_prompt += r.prompt_tokens
            total_completion += r.completion_tokens
            total_cost += r.cost_cny(self.pricing)

        return {
            "period_hours": hours,
            "total_calls": len(recent),
            "total_prompt_tokens": total_prompt,
            "total_completion_tokens": total_completion,
            "total_tokens": total_prompt + total_completion,
            "total_cost_cny": round(total_cost, 4),
            "daily_budget_cny": self.daily_budget_cny,
            "budget_remaining_cny": round(self.daily_budget_remaining_cny(), 4),
            "rate_limit_tpm": self.rate_limit_tpm,
            "current_tpm": self.tokens_last_minute(),
            "by_stage": dict(by_stage),
        }

    def format_report(self, hours: int = 24) -> str:
        """Human-readable usage report."""
        s = self.summary(hours)
        lines = [
            f"═══ Token Usage Report (last {hours}h) ═══",
            f"  Total API calls:      {s['total_calls']}",
            f"  Prompt tokens:        {s['total_prompt_tokens']:,}",
            f"  Completion tokens:    {s['total_completion_tokens']:,}",
            f"  Total tokens:         {s['total_tokens']:,}",
            f"  Cost (CNY):           ¥{s['total_cost_cny']:.4f}",
            f"  Daily budget:         ¥{s['daily_budget_cny']:.2f}",
            f"  Budget remaining:     ¥{s['budget_remaining_cny']:.4f}",
            f"  Rate (current TPM):   {s['current_tpm']:,} / {s['rate_limit_tpm']:,}",
            "",
            "  By Stage:",
        ]
        for stage, data in s["by_stage"].items():
            lines.append(
                f"    {stage:>12}: {data['calls']:>4} calls | "
                f"{data['prompt_tokens'] + data['completion_tokens']:>8,} tok | "
                f"¥{data['cost_cny']:.4f}"
            )
        lines.append("═" * 42)
        return "\n".join(lines)

    # ── Persistence ───────────────────────────────────────────────

    async def persist_to_db(self, db) -> int:
        """Save only NEW (un-persisted) records to the database.

        Returns number of records saved.
        """
        async with self._lock:
            new_records = self._records[self._persisted_count:]

        if not new_records:
            return 0

        try:
            await db._db.executemany(
                """INSERT INTO token_usage
                   (timestamp, model, stage, prompt_tokens, completion_tokens,
                    estimated_prompt, source_name, duration_ms, cost_cny)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                [
                    (
                        r.timestamp.isoformat(),
                        r.model,
                        r.stage,
                        r.prompt_tokens,
                        r.completion_tokens,
                        r.estimated_prompt,
                        r.source_name,
                        r.duration_ms,
                        round(r.cost_cny(self.pricing), 6),
                    )
                    for r in new_records
                ],
            )
            await db._db.commit()
            async with self._lock:
                self._persisted_count += len(new_records)
            return len(new_records)
        except Exception as e:
            logger.error("Failed to persist token usage: %s", e)
            return 0

    async def load_today_from_db(self, db) -> None:
        """Load today's records from DB to restore state after restart."""
        today = datetime.now().strftime("%Y-%m-%d")
        try:
            async with db._db.execute(
                """SELECT timestamp, model, stage, prompt_tokens, completion_tokens,
                          estimated_prompt, source_name, duration_ms
                   FROM token_usage WHERE timestamp >= ?""",
                (f"{today}T00:00:00",),
            ) as cursor:
                rows = await cursor.fetchall()

            for row in rows:
                rec = UsageRecord(
                    timestamp=datetime.fromisoformat(row[0]),
                    model=row[1],
                    stage=row[2],
                    prompt_tokens=row[3],
                    completion_tokens=row[4],
                    estimated_prompt=row[5],
                    source_name=row[6],
                    duration_ms=row[7],
                )
                self._records.append(rec)
                self._daily_totals[rec.stage] += rec.total_tokens
                self._daily_totals["_all"] += rec.total_tokens

            # Mark all loaded records as already persisted so they don't get
            # written back to DB on the next persist_to_db() call.
            self._persisted_count = len(self._records)

            if rows:
                logger.info("[Tokens] Restored %d records from today's DB", len(rows))
        except Exception as e:
            logger.debug("Could not load token history: %s", e)
