"""Hallucination-rate circuit breaker + Feishu alert.

Top-trading-firm best practice: an agent system that can't self-monitor is a
liability. We sample citations daily, measure the mismatch rate weekly, and
if it crosses a red-line, we:

1. Pause any model currently in ``ready`` status (so analysts don't
   unknowingly use a hallucinating model's output).
2. Fire a Feishu alert to the oncall channel with the week's summary.
3. Write a ``SanityIssue(issue_type="hallucination_guard")`` on the top-k
   offending models so operators can triage.

Two entry points:

* :func:`daily_sample_pass` — called by the main.py lifespan loop; picks
  ``k`` models with citations, runs the sampler on each.
* :func:`weekly_review_and_alert` — aggregates the last 7 days and triggers
  the alert / auto-pause if the rate > ``HALLUCINATION_RED_LINE``.

Both are idempotent and best-effort — a failure here never blocks user
traffic.
"""
from __future__ import annotations

import json
import logging
import random
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.app.core.database import async_session_factory
from backend.app.models.revenue_model import RevenueModel, ModelCell, SanityIssue
from backend.app.services.citation_audit import (
    CitationAuditLog,
    audit_model,
    hallucination_summary,
)

logger = logging.getLogger(__name__)


# Red-line mismatch rate. Above this, the guard auto-pauses ready models.
HALLUCINATION_RED_LINE = 0.15
# Warn threshold (Feishu informational ping, no auto-pause).
HALLUCINATION_WARN_LINE = 0.08
# How many models to sample per daily pass. Budget-bounded.
DAILY_SAMPLE_SIZE = 8


async def _pickable_models(db: AsyncSession, limit: int) -> list[RevenueModel]:
    """Return up to ``limit`` models that have real citations and haven't been
    audited in the last 24h (LRU-ish)."""
    # Cheap query: prefer "ready" models first, then "running" — skip drafts.
    q = (
        select(RevenueModel)
        .where(RevenueModel.status.in_(("ready", "running")))
        .order_by(RevenueModel.updated_at.desc())
        .limit(limit * 4)  # overselect, we'll filter by recent-audit below
    )
    candidates = list((await db.execute(q)).scalars().all())
    if not candidates:
        return []
    # Skip any model that already has an audit log in the last 24h.
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    recent_audits = list((await db.execute(
        select(CitationAuditLog.model_id)
        .where(CitationAuditLog.created_at >= cutoff)
    )).scalars().all())
    recent_set = {mid for mid in recent_audits}
    eligible = [m for m in candidates if m.id not in recent_set]
    random.shuffle(eligible)
    return eligible[:limit]


async def daily_sample_pass(sample_rate: float = 0.05, max_samples: int = 15) -> dict[str, Any]:
    """Sample citations on up-to DAILY_SAMPLE_SIZE models once per day.

    Returns a per-model summary dict. Exceptions per-model are logged but
    don't abort the whole pass.
    """
    async with async_session_factory() as db:
        models = await _pickable_models(db, DAILY_SAMPLE_SIZE)
    if not models:
        return {"sampled_models": 0, "details": [], "message": "no_eligible_models"}

    details = []
    for m in models:
        try:
            res = await audit_model(m.id, sample_rate=sample_rate, max_samples=max_samples)
            res["model_id"] = str(m.id)
            res["ticker"] = m.ticker
            res["industry"] = m.industry
            details.append(res)
        except Exception as e:
            logger.exception("daily audit failed for model %s", m.id)
            details.append({"model_id": str(m.id), "error": str(e)})
    return {"sampled_models": len(models), "details": details}


async def _send_hallucination_feishu(summary: dict[str, Any], paused: list[dict]) -> None:
    """Push a Feishu card with the week's summary + any auto-paused models."""
    from backend.app.config import settings
    webhook = getattr(settings, "feishu_webhook_url", "")
    if not webhook:
        logger.debug("Feishu webhook not configured; skipping hallucination alert")
        return
    rate = summary.get("hallucination_rate", 0.0)
    template = "red" if rate >= HALLUCINATION_RED_LINE else "yellow"
    emoji = "🚨" if rate >= HALLUCINATION_RED_LINE else "⚠️"
    verdicts = summary.get("verdicts", {})
    verdict_lines = "\n".join(f"- {k}: {v}" for k, v in sorted(verdicts.items()))
    paused_section = ""
    if paused:
        rows = "\n".join(f"- {p['ticker']} ({p['industry']}) · {p['model_id']}" for p in paused[:10])
        paused_section = f"\n\n**自动暂停模型 ({len(paused)}):**\n{rows}"
    card = {
        "msg_type": "interactive",
        "card": {
            "header": {
                "title": {
                    "tag": "plain_text",
                    "content": f"{emoji} 收入模型幻觉率告警 · {rate:.1%}",
                },
                "template": template,
            },
            "elements": [
                {"tag": "div", "text": {"tag": "lark_md", "content": (
                    f"**窗口:** 最近 7 天\n"
                    f"**采样总量:** {summary.get('total_sampled', 0)}\n"
                    f"**mismatch 占比:** {rate:.2%}\n"
                    f"**红线:** {HALLUCINATION_RED_LINE:.0%}\n\n"
                    f"**verdict 分布:**\n{verdict_lines}"
                    f"{paused_section}"
                )}},
            ],
        },
    }
    try:
        async with httpx.AsyncClient(timeout=10.0, trust_env=False) as client:
            await client.post(webhook, json=card)
    except Exception:
        logger.warning("Failed to post Feishu hallucination alert", exc_info=True)


async def _auto_pause_offenders(
    db: AsyncSession, since_days: int, min_mismatch_per_model: int = 3,
) -> list[dict]:
    """Pause ready models whose mismatch count over the window is unusually high."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=since_days)
    rows = list((await db.execute(
        select(CitationAuditLog)
        .where(
            CitationAuditLog.created_at >= cutoff,
            CitationAuditLog.verdict == "mismatch",
        )
    )).scalars().all())
    per_model: dict[str, int] = {}
    for r in rows:
        per_model[str(r.model_id)] = per_model.get(str(r.model_id), 0) + 1

    paused: list[dict] = []
    for mid_str, count in per_model.items():
        if count < min_mismatch_per_model:
            continue
        try:
            import uuid as _uuid
            m = await db.get(RevenueModel, _uuid.UUID(mid_str))
        except Exception:
            continue
        if not m or m.status != "ready":
            continue
        m.paused_by_guard = True
        m.paused_reason = (
            f"hallucination_guard:{count}_mismatches_in_{since_days}d"
        )
        # Record a sanity issue describing why
        db.add(SanityIssue(
            model_id=m.id,
            issue_type="hallucination_guard",
            severity="error",
            cell_paths=[],
            message=(
                f"Auto-paused by hallucination guard: {count} citation "
                f"mismatches over the last {since_days} days."
            ),
            suggested_fix=(
                "Re-run verify_and_ask on flagged cells, or raise an expert "
                "call request to confirm the disputed sources."
            ),
            details={"mismatch_count": count, "window_days": since_days},
        ))
        paused.append({
            "model_id": mid_str, "ticker": m.ticker, "industry": m.industry,
            "mismatch_count": count,
        })
    if paused:
        await db.commit()
    return paused


async def weekly_review_and_alert(
    since_days: int = 7,
    auto_pause: bool = True,
) -> dict[str, Any]:
    """Aggregate last-week audit data, auto-pause offenders, push Feishu alert.

    Returns the summary + any paused models for test/telemetry visibility.
    """
    summary = await hallucination_summary(since_days=since_days)
    paused: list[dict] = []
    if auto_pause and summary.get("hallucination_rate", 0.0) >= HALLUCINATION_RED_LINE:
        async with async_session_factory() as db:
            paused = await _auto_pause_offenders(db, since_days=since_days)

    rate = summary.get("hallucination_rate", 0.0)
    # Always push at >= WARN; red cases (already auto-paused) include the
    # paused list in the card.
    if rate >= HALLUCINATION_WARN_LINE:
        await _send_hallucination_feishu(summary, paused)

    return {
        "summary": summary,
        "paused_models": paused,
        "red_line": HALLUCINATION_RED_LINE,
        "warn_line": HALLUCINATION_WARN_LINE,
        "auto_pause_triggered": bool(paused),
    }
